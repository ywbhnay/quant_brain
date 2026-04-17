"""
market/distributor.py 单元测试

覆盖：
1. distribute_daily_batch 日线跑批
2. distribute_minute_bars 分钟线分发
3. distribute_once 盘后跑批 (别名)
4. 无活跃代码时跳过
5. 单只股票分发失败不影响其他
"""
from unittest.mock import AsyncMock

import pytest

from quant_engine.market.distributor import (
    MarketDistributor,
    MARKET_STREAM,
)
from quant_engine.market.snapshot import MinuteBar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_distributor():
    """创建 distributor，使用 mock redis 和 mock fetcher"""
    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="1-0")

    mock_fetcher = AsyncMock()

    distributor = MarketDistributor(
        redis_client=mock_redis,
        fetcher=mock_fetcher,
        stream_maxlen=10000,
        active_codes=["000001.SZ", "000002.SZ"],
    )
    return distributor, mock_redis, mock_fetcher


# ---------------------------------------------------------------------------
class TestMarketDistributorDailyBatch:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_success(self):
        """验证日线跑批成功"""
        dist, mock_redis, mock_fetcher = _make_distributor()

        daily_data = [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": "10.5"},
        ]
        mock_fetcher.get_daily.return_value = daily_data

        result = await dist.distribute_daily_batch(
            start_date="20240101", end_date="20240103",
        )

        assert result == 2  # 2 只股票都成功
        assert mock_redis.xadd.await_count == 2

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_no_active_codes(self):
        """验证无活跃代码时返回 0"""
        dist, mock_redis, _ = _make_distributor()
        dist._active_codes = []

        result = await dist.distribute_daily_batch()
        assert result == 0

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_empty_daily(self):
        """验证无日线数据时跳过"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        mock_fetcher.get_daily.return_value = []

        result = await dist.distribute_daily_batch()
        assert result == 0

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_partial_failure(self):
        """验证单只股票分发失败不影响其他"""
        dist, mock_redis, mock_fetcher = _make_distributor()

        async def mock_get_daily(code, **kwargs):
            if code == "000001.SZ":
                raise RuntimeError("fetch error")
            return [{"ts_code": code, "trade_date": "20240102", "close": "20.0"}]

        mock_fetcher.get_daily.side_effect = mock_get_daily

        result = await dist.distribute_daily_batch()

        assert result == 1  # 只有 000002 成功
        assert mock_redis.xadd.await_count >= 1


# ---------------------------------------------------------------------------
class TestMarketDistributorMinuteBars:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_minute_bars(self):
        """验证分钟线分发到 Stream"""
        dist, mock_redis, _ = _make_distributor()

        bars = [
            MinuteBar(
                ts_code="000001.SZ", trade_date="20240102", trade_time="09:31",
                open=10.0, high=10.1, low=9.9, close=10.05,
                vol=500.0, amount=5000.0,
            ),
            MinuteBar(
                ts_code="000001.SZ", trade_date="20240102", trade_time="09:32",
                open=10.05, high=10.2, low=10.0, close=10.15,
                vol=600.0, amount=6060.0,
            ),
        ]

        await dist.distribute_minute_bars("000001.SZ", bars)

        assert mock_redis.xadd.await_count == 2
        # 验证写入的 stream 名称
        for call in mock_redis.xadd.call_args_list:
            assert call.args[0] == MARKET_STREAM

    @pytest.mark.asyncio
    async def test_distribute_minute_bars_empty(self):
        """验证空列表不写入"""
        dist, mock_redis, _ = _make_distributor()

        await dist.distribute_minute_bars("000001.SZ", [])

        mock_redis.xadd.assert_not_awaited()


# ---------------------------------------------------------------------------
class TestMarketDistributorDistributeOnce:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_once_delegates_to_daily_batch(self):
        """验证 distribute_once 是 distribute_daily_batch 的别名"""
        dist, mock_redis, mock_fetcher = _make_distributor()

        daily_data = [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": "10.5"},
        ]
        mock_fetcher.get_daily.return_value = daily_data

        result = await dist.distribute_once(
            start_date="20240101", end_date="20240103",
        )

        assert result == 2
        assert mock_redis.xadd.await_count == 2

    @pytest.mark.asyncio
    async def test_distribute_once_no_active_codes(self):
        """验证无活跃代码时返回 0"""
        dist, mock_redis, _ = _make_distributor()
        dist._active_codes = []

        result = await dist.distribute_once()
        assert result == 0
