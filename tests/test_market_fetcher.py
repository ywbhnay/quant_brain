"""
market 模块单元测试

覆盖：
1. RateLimiter.acquire — 限流逻辑
2. PGMarketReader — 从 asyncpg.Pool 读取日线 / 分钟线 / 批量
3. RealtimeQuoteClient — 实时快照 (Tushare realtime_quote)

历史行情读取现在走 PG (PGMarketReader)，不再直接调 Tushare HTTP API。
仅 realtime_quote 仍走 Tushare（5 档盘口不进 PG）。
"""

import asyncio
import time
from datetime import date
from datetime import time as dt_time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from quant_engine.market.fetcher import RateLimiter, RealtimeQuoteClient
from quant_engine.market.reader import PGMarketReader, _row_to_dict
from quant_engine.market.snapshot import MarketSnapshot, MinuteBar

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tushare_response(fields, items, code=0, msg=""):
    """构造 Tushare 标准返回格式"""
    return {
        "code": code,
        "msg": msg,
        "data": {"fields": fields, "items": items},
    }


class FakeRecord(dict):
    """
    模拟 asyncpg.Record —— dict 子类，但保持 dict-like 接口。
    _row_to_dict 用 dict(row) 转换，FakeRecord 直接兼容。
    """

    pass


def _row(ts_code="000001.SZ", trade_date=None, **ohlcv):
    """构造一条 PG daily 行（带 Decimal 类型以测试转换）"""
    if trade_date is None:
        trade_date = date(2026, 6, 18)
    data = {"ts_code": ts_code, "trade_date": trade_date}
    for col in ("open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"):
        val = ohlcv.get(col)
        data[col] = Decimal(str(val)) if val is not None else None
    return FakeRecord(**data)


# ---------------------------------------------------------------------------
# TestRateLimiter (未变，仍适用于 RealtimeQuoteClient)
# ---------------------------------------------------------------------------


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_acquire_returns_immediately_first_call(self):
        """首次调用应立即返回（无等待）"""
        limiter = RateLimiter(rate=100.0)
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05

    @pytest.mark.asyncio
    async def test_acquire_enforces_interval(self):
        """连续调用应受到间隔限制"""
        limiter = RateLimiter(rate=10.0)  # 0.1s 间隔
        await limiter.acquire()
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.08

    @pytest.mark.asyncio
    async def test_acquire_is_threadsafe_within_asyncio(self):
        """多次并发调用应序列化"""
        limiter = RateLimiter(rate=100.0)
        calls = await asyncio.gather(*[limiter.acquire() for _ in range(5)])
        assert len(calls) == 5


# ---------------------------------------------------------------------------
# Test_row_to_dict (reader.py 内部工具)
# ---------------------------------------------------------------------------


class TestRowToDict:
    def test_decimal_converted_to_float(self):
        """Numeric 列 (Decimal) 应转为 float"""
        row = FakeRecord(
            ts_code="000001.SZ",
            trade_date=date(2026, 6, 18),
            open=Decimal("10.5000"),
            close=Decimal("11.2500"),
            vol=Decimal("1000.0000"),
            amount=None,
            pre_close=Decimal("10.0000"),
            change=Decimal("0.7500"),
            pct_chg=Decimal("7.5000"),
            high=Decimal("11.5000"),
            low=Decimal("10.2500"),
        )
        d = _row_to_dict(row)
        assert d["ts_code"] == "000001.SZ"
        assert d["trade_date"] == "20260618"
        assert isinstance(d["open"], float) and d["open"] == 10.5
        assert isinstance(d["close"], float) and d["close"] == 11.25
        assert d["amount"] is None

    def test_trade_date_formatted_as_yyyymmdd(self):
        """datetime.date 应格式化为 YYYYMMDD 字符串"""
        row = FakeRecord(
            ts_code="000001.SZ",
            trade_date=date(2024, 1, 2),
            open=None,
            high=None,
            low=None,
            close=None,
            pre_close=None,
            change=None,
            pct_chg=None,
            vol=None,
            amount=None,
        )
        d = _row_to_dict(row)
        assert d["trade_date"] == "20240102"


# ---------------------------------------------------------------------------
# TestPGMarketReader
# ---------------------------------------------------------------------------


class TestPGMarketReader:
    def _make_reader(self, fetch_return=None, fetchval_return=True):
        """构造 PGMarketReader，pool.fetch 返回指定行"""
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=fetch_return or [])
        pool.fetchval = AsyncMock(return_value=fetchval_return)
        return PGMarketReader(pool), pool

    # -- get_daily -----------------------------------------------------

    @pytest.mark.asyncio
    async def test_get_daily_returns_list_of_dicts_with_float_ohlcv(self):
        """返回 list[dict]，OHLCV 为 float (不是 Decimal)"""
        reader, pool = self._make_reader(
            fetch_return=[
                _row(open=10.5, high=11.0, low=10.0, close=10.8, vol=1000.0, amount=10800.0),
            ]
        )
        result = await reader.get_daily("000001.SZ", "20260601", "20260618")

        assert len(result) == 1
        row = result[0]
        assert row["ts_code"] == "000001.SZ"
        assert row["trade_date"] == "20260618"
        assert isinstance(row["open"], float)
        assert row["close"] == 10.8
        # 验证 SQL 用了日期过滤
        sql = pool.fetch.call_args[0][0]
        assert "ts_code = $1" in sql
        assert "trade_date >= $2::date" in sql
        assert "trade_date <= $3::date" in sql

    @pytest.mark.asyncio
    async def test_get_daily_respects_date_range(self):
        """日期参数应转换为 PG DATE 字面量 (YYYY-MM-DD)"""
        reader, pool = self._make_reader(fetch_return=[])
        await reader.get_daily("000001.SZ", "20260101", "20260618")

        args = pool.fetch.call_args[0][1:]
        assert args[0] == "000001.SZ"
        assert args[1] == "2026-01-01"
        assert args[2] == "2026-06-18"

    @pytest.mark.asyncio
    async def test_get_daily_without_date_params(self):
        """不传日期时 WHERE 子句不带 trade_date 过滤"""
        reader, pool = self._make_reader(fetch_return=[])
        await reader.get_daily("000001.SZ")

        sql = pool.fetch.call_args[0][0]
        # 仅检查 WHERE 条件片段（排除 SELECT 列和 ORDER BY）
        where_fragment = sql.split("WHERE", 1)[1].split("ORDER BY", 1)[0]
        assert "trade_date" not in where_fragment
        args = pool.fetch.call_args[0][1:]
        assert args == ("000001.SZ",)

    @pytest.mark.asyncio
    async def test_get_daily_empty_result(self):
        """PG 返回空结果时返回空列表"""
        reader, _ = self._make_reader(fetch_return=[])
        result = await reader.get_daily("NOT_EXIST.SZ")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_daily_invalid_date_raises(self):
        """日期格式错误时应抛出 ValueError"""
        reader, _ = self._make_reader()
        with pytest.raises(ValueError, match="YYYYMMDD"):
            await reader.get_daily("000001.SZ", "2026-06-18")  # 错误格式

    # -- get_daily_batch ----------------------------------------------

    @pytest.mark.asyncio
    async def test_get_daily_batch_chunks_large_code_lists(self):
        """100 只股票 + chunk_size=50 → 2 次并发查询"""
        reader, pool = self._make_reader(
            fetch_return=[
                _row(ts_code="000001.SZ"),
            ]
        )
        codes = [f"{i:06d}.SZ" for i in range(100)]
        result = await reader.get_daily_batch(codes, chunk_size=50)

        # pool.fetch 应被调用 2 次
        assert pool.fetch.await_count == 2
        # 每次返回 1 行 → 共 2 行
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_daily_batch_empty_codes(self):
        """空代码列表直接返回空"""
        reader, pool = self._make_reader()
        result = await reader.get_daily_batch([])
        assert result == []
        pool.fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_daily_batch_handles_chunk_error(self):
        """单个 chunk 失败不影响其他 chunk"""
        reader, pool = self._make_reader()
        call_count = 0

        async def mock_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("PG connection lost")
            return [_row(ts_code="000002.SZ")]

        pool.fetch = mock_fetch
        result = await reader.get_daily_batch(["000001.SZ", "000002.SZ"], chunk_size=1)
        # 第一次失败，第二次成功 → 1 行
        assert len(result) == 1

    # -- get_minute_bars ----------------------------------------------

    @pytest.mark.asyncio
    async def test_get_minute_bars_splits_time_column(self):
        """PG 的 trade_date (DATE) + trade_time (TIME) 应拆分为字符串"""
        pool = AsyncMock()
        pool.fetchval = AsyncMock(return_value=True)  # 表存在
        pool.fetch = AsyncMock(
            return_value=[
                FakeRecord(
                    ts_code="000001.SZ",
                    trade_date=date(2026, 6, 18),
                    trade_time=dt_time(9, 31),
                    open=Decimal("10.0"),
                    high=Decimal("10.1"),
                    low=Decimal("9.9"),
                    close=Decimal("10.05"),
                    vol=Decimal("500.0"),
                    amount=Decimal("5000.0"),
                ),
            ]
        )
        reader = PGMarketReader(pool)

        bars = await reader.get_minute_bars("000001.SZ")

        assert len(bars) == 1
        assert isinstance(bars[0], MinuteBar)
        assert bars[0].ts_code == "000001.SZ"
        assert bars[0].trade_date == "20260618"
        assert bars[0].trade_time == "09:31"
        assert bars[0].close == 10.05

    @pytest.mark.asyncio
    async def test_get_minute_bars_returns_empty_when_table_missing(self):
        """minute_bar 表不存在时返回空列表 (不抛)"""
        reader, pool = self._make_reader(fetchval_return=False)
        bars = await reader.get_minute_bars("000001.SZ")
        assert bars == []
        pool.fetch.assert_not_awaited()  # 不应该去查询

    # -- connect/close -----------------------------------------------

    @pytest.mark.asyncio
    async def test_connect_and_close_are_noop(self):
        """connect/close 是 no-op，不抛异常"""
        reader, _ = self._make_reader()
        await reader.connect()  # should not raise
        await reader.close()  # should not raise


# ---------------------------------------------------------------------------
# TestRealtimeQuoteClient
# ---------------------------------------------------------------------------


class TestRealtimeQuoteClient:
    @pytest.mark.asyncio
    async def test_get_realtime_snapshot_success(self):
        """验证快照正确解析 5 档盘口"""
        client = RealtimeQuoteClient(token="test_token")

        response_data = _make_tushare_response(
            [
                "ts_code",
                "price",
                "change",
                "pct_chg",
                "vol",
                "amount",
                "b1_v",
                "b1_p",
                "b2_v",
                "b2_p",
                "b3_v",
                "b3_p",
                "b4_v",
                "b4_p",
                "b5_v",
                "b5_p",
                "a1_v",
                "a1_p",
                "a2_v",
                "a2_p",
                "a3_v",
                "a3_p",
                "a4_v",
                "a4_p",
                "a5_v",
                "a5_p",
            ],
            [
                [
                    "000001.SZ",
                    10.5,
                    0.5,
                    5.0,
                    1000.0,
                    10500.0,
                    100,
                    10.4,
                    200,
                    10.3,
                    300,
                    10.2,
                    400,
                    10.1,
                    500,
                    10.0,
                    150,
                    10.6,
                    250,
                    10.7,
                    350,
                    10.8,
                    450,
                    10.9,
                    550,
                    11.0,
                ],
            ],
        )

        mock_response = MagicMock()
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        mock_httpx_client = AsyncMock()
        mock_httpx_client.post.return_value = mock_response

        with patch.object(client, "_ensure_client", return_value=mock_httpx_client):
            with patch.object(client, "_limiter", AsyncMock(acquire=AsyncMock())):
                snap = await client.get_realtime_snapshot("000001.SZ")

        assert snap is not None
        assert isinstance(snap, MarketSnapshot)
        assert snap.ts_code == "000001.SZ"
        assert snap.price == 10.5
        assert snap.bids[0].price == 10.4
        assert snap.bids[0].vol == 100
        assert snap.asks[0].price == 10.6
        assert snap.asks[0].vol == 150

    @pytest.mark.asyncio
    async def test_get_realtime_snapshot_empty_returns_none(self):
        """Tushare 返回空数据时返回 None"""
        client = RealtimeQuoteClient(token="test_token")

        mock_response = MagicMock()
        mock_response.json.return_value = _make_tushare_response([], [])
        mock_response.raise_for_status = MagicMock()

        mock_httpx_client = AsyncMock()
        mock_httpx_client.post.return_value = mock_response

        with patch.object(client, "_ensure_client", return_value=mock_httpx_client):
            with patch.object(client, "_limiter", AsyncMock(acquire=AsyncMock())):
                result = await client.get_realtime_snapshot("000001.SZ")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_realtime_snapshot_raises_on_api_error(self):
        """API 错误码应抛出 RuntimeError"""
        client = RealtimeQuoteClient(token="test_token")

        mock_response = MagicMock()
        mock_response.json.return_value = _make_tushare_response([], [], code=-1, msg="bad token")
        mock_response.raise_for_status = MagicMock()

        mock_httpx_client = AsyncMock()
        mock_httpx_client.post.return_value = mock_response

        with patch.object(client, "_ensure_client", return_value=mock_httpx_client):
            with patch.object(client, "_limiter", AsyncMock(acquire=AsyncMock())):
                with pytest.raises(RuntimeError, match="bad token"):
                    await client.get_realtime_snapshot("000001.SZ")

    @pytest.mark.asyncio
    async def test_get_realtime_snapshot_raises_without_token(self):
        """未配置 TUSHARE_TOKEN 应抛 RuntimeError"""
        client = RealtimeQuoteClient(token="")
        with pytest.raises(RuntimeError, match="TUSHARE_TOKEN"):
            await client.get_realtime_snapshot("000001.SZ")

    @pytest.mark.asyncio
    async def test_close_closes_httpx_client(self):
        """close 应关闭底层 httpx 客户端"""
        client = RealtimeQuoteClient(token="test_token")
        mock_httpx = AsyncMock()
        mock_httpx.aclose = AsyncMock()
        client._client = mock_httpx

        await client.close()
        mock_httpx.aclose.assert_awaited_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_close_noop_when_no_client(self):
        """无客户端时 close 不报错"""
        client = RealtimeQuoteClient(token="test_token")
        await client.close()  # should not raise
