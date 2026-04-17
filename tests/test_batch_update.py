"""
batch_update.py 单元测试

覆盖：
1. Config 配置验证
2. get_active_stock / get_active_stock_with_date_range
3. fetch_daily_chunk / fetch_adj_factors
4. fill_suspended_gaps (核心逻辑)
5. upsert_daily_wide / refresh_stock_profile
6. run_batch_update (整合流程)
"""
import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from quant_engine.batch_update import (
    DAILY_WIDE_COLS,
    STOCK_PROFILE_COLS,
    fetch_adj_factors,
    fetch_daily_chunk,
    fill_suspended_gaps,
    get_active_stock,
    get_active_stock_with_date_range,
    refresh_stock_profile,
    run_batch_update,
    upsert_daily_wide,
)
from quant_engine.config import QuantConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeAcquire:
    """Proper async context manager for mocking pool.acquire()."""
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _make_pool_mock(**kwargs):
    """Create a mock asyncpg pool with configurable fetch/execute/acquire."""
    pool = AsyncMock()
    pool.fetch = kwargs.get("fetch", AsyncMock(return_value=[]))
    pool.execute = kwargs.get("execute", AsyncMock(return_value=""))
    conn = kwargs.get("conn", AsyncMock())
    pool.acquire = MagicMock(return_value=FakeAcquire(conn))
    return pool


def _make_row_dict(**fields):
    """Mimic asyncpg Row behavior (dict-like access)."""
    return fields


# ---------------------------------------------------------------------------
# Config Tests
# ---------------------------------------------------------------------------

class TestQuantConfig:
    def test_pg_dsn_format(self):
        QuantConfig.PG_HOST = "127.0.0.1"
        QuantConfig.PG_PORT = 5432
        QuantConfig.PG_USER = "postgres"
        QuantConfig.PG_PASSWORD = "secret"
        QuantConfig.PG_DATABASE = "quant_data"
        expected = "postgres://postgres:secret@127.0.0.1:5432/quant_data"
        assert QuantConfig.pg_dsn() == expected

    def test_pg_uri_asyncpg(self):
        QuantConfig.PG_HOST = "10.0.0.1"
        QuantConfig.PG_PORT = 5433
        QuantConfig.PG_USER = "admin"
        QuantConfig.PG_PASSWORD = "pwd"
        QuantConfig.PG_DATABASE = "test_db"
        expected = "postgresql+asyncpg://admin:pwd@10.0.0.1:5433/test_db"
        assert QuantConfig.pg_uri_asyncpg() == expected

    def test_redis_url_no_password(self):
        QuantConfig.REDIS_HOST = "127.0.0.1"
        QuantConfig.REDIS_PORT = 6379
        QuantConfig.REDIS_DB = 0
        QuantConfig.REDIS_PASSWORD = ""
        assert QuantConfig.redis_url() == "redis://127.0.0.1:6379/0"

    def test_redis_url_with_password(self):
        QuantConfig.REDIS_HOST = "10.0.0.1"
        QuantConfig.REDIS_PORT = 6380
        QuantConfig.REDIS_DB = 1
        QuantConfig.REDIS_PASSWORD = "rpass"
        assert QuantConfig.redis_url() == "redis://:rpass@10.0.0.1:6380/1"

    def test_validate_passes(self):
        QuantConfig.PG_HOST = "host"
        QuantConfig.PG_USER = "user"
        QuantConfig.PG_PASSWORD = "pass"
        QuantConfig.PG_DATABASE = "db"
        QuantConfig.validate()  # should not raise

    def test_validate_raises_on_missing(self):
        QuantConfig.PG_HOST = ""
        QuantConfig.PG_USER = "user"
        QuantConfig.PG_PASSWORD = "pass"
        QuantConfig.PG_DATABASE = "db"
        with pytest.raises(RuntimeError, match="缺少必填配置"):
            QuantConfig.validate()

    def test_batch_chunk_size_default(self):
        import os
        original = os.environ.get("BATCH_CHUNK_SIZE")
        if "BATCH_CHUNK_SIZE" in os.environ:
            del os.environ["BATCH_CHUNK_SIZE"]
        assert QuantConfig.BATCH_CHUNK_SIZE == 100
        if original is not None:
            os.environ["BATCH_CHUNK_SIZE"] = original
        else:
            os.environ.pop("BATCH_CHUNK_SIZE", None)


# ---------------------------------------------------------------------------
# get_active_stock Tests
# ---------------------------------------------------------------------------

class TestGetActiveStock:
    @pytest.mark.asyncio
    async def test_returns_sorted_codes(self):
        rows = [
            _make_row_dict(ts_code="000001.SZ"),
            _make_row_dict(ts_code="000002.SZ"),
            _make_row_dict(ts_code="600000.SH"),
        ]
        pool = _make_pool_mock(fetch=AsyncMock(return_value=rows))
        result = await get_active_stock(pool)
        assert result == ["000001.SZ", "000002.SZ", "600000.SH"]
        pool.fetch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_result(self):
        pool = _make_pool_mock(fetch=AsyncMock(return_value=[]))
        result = await get_active_stock(pool)
        assert result == []


# ---------------------------------------------------------------------------
# get_active_stock_with_date_range Tests
# ---------------------------------------------------------------------------

class TestGetActiveStockWithDateRange:
    @pytest.mark.asyncio
    async def test_returns_tuples_with_dates(self):
        rows = [
            _make_row_dict(ts_code="000001.SZ", list_date=date(1991, 4, 3), delist_date=None),
            _make_row_dict(ts_code="000003.SZ", list_date=date(1995, 1, 1), delist_date=date(2020, 6, 30)),
        ]
        pool = _make_pool_mock(fetch=AsyncMock(return_value=rows))
        result = await get_active_stock_with_date_range(pool)
        assert len(result) == 2
        assert result[0] == ("000001.SZ", date(1991, 4, 3), None)
        assert result[1] == ("000003.SZ", date(1995, 1, 1), date(2020, 6, 30))

    @pytest.mark.asyncio
    async def test_empty_result(self):
        pool = _make_pool_mock(fetch=AsyncMock(return_value=[]))
        result = await get_active_stock_with_date_range(pool)
        assert result == []


# ---------------------------------------------------------------------------
# fetch_daily_chunk Tests
# ---------------------------------------------------------------------------

class TestFetchDailyChunk:
    @pytest.mark.asyncio
    async def test_empty_codes_returns_empty_df(self):
        pool = AsyncMock()
        result = await fetch_daily_chunk(pool, [])
        assert result.height == 0
        pool.fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_data_with_rows(self):
        rows = [
            _make_row_dict(
                trade_date=date(2024, 1, 2), ts_code="000001.SZ",
                open=10.0, high=10.5, low=9.8, close=10.2,
                pre_close=10.0, change=0.2, pct_chg=2.0,
                vol=10000.0, amount=102000.0,
            ),
        ]
        pool = _make_pool_mock(fetch=AsyncMock(return_value=rows))
        result = await fetch_daily_chunk(pool, ["000001.SZ"])
        assert result.height == 1
        assert result["ts_code"][0] == "000001.SZ"
        assert result["close"][0] == 10.2

    @pytest.mark.asyncio
    async def test_date_filter_applied(self):
        rows = []
        pool = _make_pool_mock(fetch=AsyncMock(return_value=rows))
        await fetch_daily_chunk(pool, ["000001.SZ"], start_date="20240101", end_date="20240131")
        call_args = pool.fetch.call_args
        sql = call_args[0][0]
        assert "trade_date >=" in sql
        assert "trade_date <=" in sql

    @pytest.mark.asyncio
    async def test_no_rows_returns_empty_df(self):
        pool = _make_pool_mock(fetch=AsyncMock(return_value=[]))
        result = await fetch_daily_chunk(pool, ["000001.SZ"])
        assert result.height == 0


# ---------------------------------------------------------------------------
# fetch_adj_factors Tests
# ---------------------------------------------------------------------------

class TestFetchAdjFactors:
    @pytest.mark.asyncio
    async def test_empty_codes(self):
        pool = AsyncMock()
        result = await fetch_adj_factors(pool, [])
        assert result.height == 0

    @pytest.mark.asyncio
    async def test_returns_factors(self):
        rows = [
            _make_row_dict(ts_code="000001.SZ", trade_date=date(2024, 1, 2), adj_factor=1.5),
        ]
        pool = _make_pool_mock(fetch=AsyncMock(return_value=rows))
        result = await fetch_adj_factors(pool, ["000001.SZ"])
        assert result.height == 1
        assert result["adj_factor"][0] == 1.5

    @pytest.mark.asyncio
    async def test_no_rows_returns_empty(self):
        pool = _make_pool_mock(fetch=AsyncMock(return_value=[]))
        result = await fetch_adj_factors(pool, ["000001.SZ"])
        assert result.height == 0


# ---------------------------------------------------------------------------
# fill_suspended_gaps Tests (核心)
# ---------------------------------------------------------------------------

class TestFillSuspendedGaps:
    def _make_df(self, rows):
        """Helper to create test DataFrames with proper schema."""
        return pl.DataFrame(rows, schema={
            "trade_date": pl.Date,
            "ts_code": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "pre_close": pl.Float64,
            "change": pl.Float64,
            "pct_chg": pl.Float64,
            "vol": pl.Float64,
            "amount": pl.Float64,
        })

    def test_empty_df_returns_empty(self):
        df = self._make_df([])
        result = fill_suspended_gaps(df, {})
        assert result.height == 0

    def test_no_gaps_no_fills(self):
        """数据完整，无停牌 -> 所有 is_suspended=False"""
        df = self._make_df([
            (date(2024, 1, 2), "000001.SZ", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0),
            (date(2024, 1, 3), "000001.SZ", 10.2, 10.8, 10.0, 10.5, 10.2, 0.3, 3.0, 1100.0, 11000.0),
        ])
        result = fill_suspended_gaps(df, {})
        assert result.height == 2
        assert not result["is_suspended"].any()

    def test_suspended_fill_marks_correctly(self):
        """中间一天停牌 (close=null) -> 该行为 is_suspended=True"""
        df = self._make_df([
            (date(2024, 1, 2), "000001.SZ", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0),
            (date(2024, 1, 3), "000001.SZ", None, None, None, None, None, None, None, None, None),
            (date(2024, 1, 4), "000001.SZ", 10.5, 11.0, 10.3, 10.8, 10.5, 0.3, 3.0, 1200.0, 12000.0),
        ])
        result = fill_suspended_gaps(df, {})
        assert result.height == 3

        suspended_rows = result.filter(pl.col("is_suspended") == True)
        assert suspended_rows.height == 1
        assert suspended_rows["trade_date"][0] == date(2024, 1, 3)

        filled_close = suspended_rows["close"][0]
        assert filled_close == 10.2

    def test_multi_stock_independent_fill(self):
        """两只股票独立处理，停牌填充互不影响"""
        df = self._make_df([
            (date(2024, 1, 2), "000001.SZ", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0),
            (date(2024, 1, 3), "000001.SZ", 10.2, 10.8, 10.0, 10.5, 10.2, 0.3, 3.0, 1100.0, 11000.0),
            (date(2024, 1, 2), "000002.SZ", 20.0, 21.0, 19.5, 20.5, 20.0, 0.5, 2.5, 2000.0, 20000.0),
            (date(2024, 1, 3), "000002.SZ", None, None, None, None, None, None, None, None, None),
        ])
        result = fill_suspended_gaps(df, {})

        a_no_susp = result.filter(
            (pl.col("ts_code") == "000001.SZ") & (pl.col("is_suspended") == True)
        )
        assert a_no_susp.height == 0

        b_susp = result.filter(
            (pl.col("ts_code") == "000002.SZ") & (pl.col("is_suspended") == True)
        )
        assert b_susp.height == 1

    def test_fill_date_records_source(self):
        """fill_date 应记录填充行的数据来源日期"""
        df = self._make_df([
            (date(2024, 1, 2), "000001.SZ", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0),
            (date(2024, 1, 3), "000001.SZ", None, None, None, None, None, None, None, None, None),
            (date(2024, 1, 4), "000001.SZ", None, None, None, None, None, None, None, None, None),
        ])
        result = fill_suspended_gaps(df, {})

        suspended = result.filter(pl.col("is_suspended") == True)
        assert suspended.height == 2

        assert all(d == date(2024, 1, 2) for d in suspended["fill_date"].to_list())

    def test_consecutive_suspension_ffill_chain(self):
        """连续多天停牌，ffill 应正确链式填充"""
        df = self._make_df([
            (date(2024, 1, 2), "000001.SZ", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0),
            (date(2024, 1, 3), "000001.SZ", None, None, None, None, None, None, None, None, None),
            (date(2024, 1, 4), "000001.SZ", None, None, None, None, None, None, None, None, None),
            (date(2024, 1, 5), "000001.SZ", None, None, None, None, None, None, None, None, None),
            (date(2024, 1, 8), "000001.SZ", 10.3, 10.8, 10.0, 10.5, 10.2, 0.3, 3.0, 1100.0, 11000.0),
        ])
        result = fill_suspended_gaps(df, {})

        suspended = result.filter(pl.col("is_suspended") == True)
        assert suspended.height == 3

        assert all(c == 10.2 for c in suspended["close"].to_list())

        normal = result.filter(
            (pl.col("trade_date") == date(2024, 1, 8)) & (pl.col("is_suspended") == False)
        )
        assert normal.height == 1
        assert normal["close"][0] == 10.5

    def test_output_has_required_columns(self):
        """输出应包含所有关键字段"""
        df = self._make_df([
            (date(2024, 1, 2), "000001.SZ", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0),
        ])
        result = fill_suspended_gaps(df, {})
        cols = result.columns
        assert "is_suspended" in cols
        assert "fill_date" in cols
        assert "adj_factor" not in cols


# ---------------------------------------------------------------------------
# upsert_daily_wide Tests
# ---------------------------------------------------------------------------

class TestUpsertDailyWide:
    def _make_wide_df(self):
        return pl.DataFrame({
            "trade_date": [date(2024, 1, 2)],
            "ts_code": ["000001.SZ"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "pre_close": [10.0],
            "change": [0.2],
            "pct_chg": [2.0],
            "vol": [1000.0],
            "amount": [10000.0],
            "adj_factor": [1.5],
            "is_suspended": [False],
            "fill_date": [None],
        })

    @pytest.mark.asyncio
    async def test_empty_df_returns_zero(self):
        df = pl.DataFrame({c: [] for c in DAILY_WIDE_COLS})
        pool = AsyncMock()
        result = await upsert_daily_wide(pool, df)
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_records_via_temp_table(self):
        """验证通过临时表 + copy_records 的写入路径"""
        df = self._make_wide_df()

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 1")
        mock_conn.copy_records_to_table = AsyncMock()

        pool = AsyncMock()
        pool.acquire = MagicMock(return_value=FakeAcquire(mock_conn))

        result = await upsert_daily_wide(pool, df)
        assert result == 1

        mock_conn.copy_records_to_table.assert_awaited_once()
        call_args = mock_conn.copy_records_to_table.call_args
        # first positional arg is table name
        assert call_args.args[0] == "tmp_daily_wide"

    @pytest.mark.asyncio
    async def test_upsert_sql_executed(self):
        """验证 UPSERT SQL 被执行"""
        df = self._make_wide_df()

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 1")
        mock_conn.copy_records_to_table = AsyncMock()

        pool = AsyncMock()
        pool.acquire = MagicMock(return_value=FakeAcquire(mock_conn))

        await upsert_daily_wide(pool, df)

        last_call = mock_conn.execute.call_args
        sql = last_call[0][0]
        assert "ON CONFLICT" in sql
        assert "tmp_daily_wide" in sql


# ---------------------------------------------------------------------------
# refresh_stock_profile Tests
# ---------------------------------------------------------------------------

class TestRefreshStockProfile:
    @pytest.mark.asyncio
    async def test_returns_row_count(self):
        pool = AsyncMock()
        pool.execute = AsyncMock(return_value="INSERT 0 5000")
        result = await refresh_stock_profile(pool)
        assert result == 5000

    @pytest.mark.asyncio
    async def test_parses_empty_string(self):
        pool = AsyncMock()
        pool.execute = AsyncMock(return_value="")
        result = await refresh_stock_profile(pool)
        assert result == 0

    @pytest.mark.asyncio
    async def test_parses_non_numeric(self):
        pool = AsyncMock()
        pool.execute = AsyncMock(return_value="UPDATE 0")
        result = await refresh_stock_profile(pool)
        assert result == 0


# ---------------------------------------------------------------------------
# run_batch_update Tests (整合)
# ---------------------------------------------------------------------------

class TestRunBatchUpdate:
    @pytest.mark.asyncio
    async def test_empty_active_stocks_early_return(self):
        mock_pool = AsyncMock()
        mock_pool.fetch = AsyncMock(return_value=[])  # no active stocks
        mock_pool.close = AsyncMock()

        async def fake_create_pool(*args, **kwargs):
            return mock_pool

        with patch("quant_engine.batch_update.asyncpg.create_pool", fake_create_pool):
            result = await run_batch_update()
            assert result["active_stocks"] == 0
            assert result["daily_wide_rows"] == 0

    @pytest.mark.asyncio
    async def test_processes_single_chunk(self):
        """模拟处理一个分片的完整流程"""
        active_codes = ["000001.SZ", "000002.SZ"]

        daily_rows = [
            _make_row_dict(
                trade_date=date(2024, 1, 2), ts_code="000001.SZ",
                open=10.0, high=10.5, low=9.8, close=10.2,
                pre_close=10.0, change=0.2, pct_chg=2.0,
                vol=1000.0, amount=10000.0,
            ),
            _make_row_dict(
                trade_date=date(2024, 1, 2), ts_code="000002.SZ",
                open=20.0, high=21.0, low=19.5, close=20.5,
                pre_close=20.0, change=0.5, pct_chg=2.5,
                vol=2000.0, amount=20000.0,
            ),
        ]

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 2")
        mock_conn.copy_records_to_table = AsyncMock()

        mock_pool = AsyncMock()
        mock_pool.fetch = AsyncMock(side_effect=[
            [{"ts_code": c} for c in active_codes],  # get_active_stock (dict-like rows)
            daily_rows,         # fetch_daily_chunk
            [],                 # fetch_adj_factors (empty)
        ])
        mock_pool.execute = AsyncMock(return_value="INSERT 0 2")
        mock_pool.close = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=FakeAcquire(mock_conn))

        async def fake_create_pool(*args, **kwargs):
            return mock_pool

        with patch("quant_engine.batch_update.asyncpg.create_pool", fake_create_pool):
            result = await run_batch_update(chunk_size=100)

        assert result["active_stocks"] == 2
        assert result["chunks_processed"] >= 1
        assert result["daily_wide_rows"] >= 2
        assert result["stock_profile_rows"] == 2

    @pytest.mark.asyncio
    async def test_chunk_splitting(self):
        """验证分片逻辑正确拆分大批量股票"""
        active_codes = [f"{i:06d}.SZ" for i in range(250)]

        mock_pool = AsyncMock()
        mock_pool.fetch = AsyncMock(return_value=[])  # no daily data
        mock_pool.execute = AsyncMock(return_value="INSERT 0 0")
        mock_pool.close = AsyncMock()

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 0")
        mock_conn.copy_records_to_table = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=FakeAcquire(mock_conn))

        async def fake_create_pool(*args, **kwargs):
            return mock_pool

        with patch("quant_engine.batch_update.asyncpg.create_pool", fake_create_pool):
            with patch("quant_engine.batch_update.get_active_stock", return_value=active_codes):
                result = await run_batch_update(chunk_size=100)

        assert result["active_stocks"] == 250

    @pytest.mark.asyncio
    async def test_pool_closed_in_finally(self):
        """验证异常时 pool 被正确关闭"""
        mock_pool = AsyncMock()
        mock_pool.fetch = AsyncMock(side_effect=Exception("DB error"))
        mock_pool.close = AsyncMock()

        async def fake_create_pool(*args, **kwargs):
            return mock_pool

        with patch("quant_engine.batch_update.asyncpg.create_pool", fake_create_pool):
            with pytest.raises(Exception, match="DB error"):
                await run_batch_update()

        mock_pool.close.assert_awaited_once()
