"""
market/distributor.py 单元测试

覆盖：
1. distribute_daily_batch 日线跑批 (从 PG reader 读取)
2. distribute_minute_bars 分钟线分发
3. distribute_snapshot 实时快照 (可选 realtime_client)
4. distribute_once 盘后跑批 (别名)
5. 无活跃代码时跳过
6. 单只股票分发失败不影响其他
"""

from unittest.mock import AsyncMock

import pytest

from quant_engine.market.distributor import (
    MARKET_STREAM,
    SNAPSHOT_CHANNEL_PREFIX,
    MarketDistributor,
)
from quant_engine.market.snapshot import MarketSnapshot, MinuteBar

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_distributor(with_realtime: bool = False):
    """
    创建 distributor，使用 mock redis + mock reader + 可选 mock realtime_client
    """
    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value="1-0")
    mock_redis.publish = AsyncMock(return_value=1)

    mock_reader = AsyncMock()

    kwargs = dict(
        redis_client=mock_redis,
        reader=mock_reader,
        stream_maxlen=10000,
        active_codes=["000001.SZ", "000002.SZ"],
    )
    if with_realtime:
        mock_realtime = AsyncMock()
        kwargs["realtime_client"] = mock_realtime
    else:
        mock_realtime = None

    distributor = MarketDistributor(**kwargs)
    return distributor, mock_redis, mock_reader, mock_realtime


# ---------------------------------------------------------------------------
class TestMarketDistributorDailyBatch:
    # ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_success(self):
        """验证日线跑批成功"""
        dist, mock_redis, mock_reader, _ = _make_distributor()

        daily_data = [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": "10.5"},
        ]
        mock_reader.get_daily.return_value = daily_data

        result = await dist.distribute_daily_batch(
            start_date="20240101",
            end_date="20240103",
        )

        assert result == 2  # 2 只股票都成功
        assert mock_redis.xadd.await_count == 2

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_no_active_codes(self):
        """验证无活跃代码时返回 0"""
        dist, mock_redis, _, _ = _make_distributor()
        dist._active_codes = []

        result = await dist.distribute_daily_batch()
        assert result == 0

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_empty_daily(self):
        """验证无日线数据时跳过"""
        dist, mock_redis, mock_reader, _ = _make_distributor()
        mock_reader.get_daily.return_value = []

        result = await dist.distribute_daily_batch()
        assert result == 0

    @pytest.mark.asyncio
    async def test_distribute_daily_batch_partial_failure(self):
        """验证单只股票分发失败不影响其他"""
        dist, mock_redis, mock_reader, _ = _make_distributor()

        async def mock_get_daily(code, **kwargs):
            if code == "000001.SZ":
                raise RuntimeError("read error")
            return [{"ts_code": code, "trade_date": "20240102", "close": "20.0"}]

        mock_reader.get_daily.side_effect = mock_get_daily

        result = await dist.distribute_daily_batch()

        assert result == 1  # 只有 000002 成功
        assert mock_redis.xadd.await_count >= 1


# ---------------------------------------------------------------------------
class TestMarketDistributorMinuteBars:
    # ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_minute_bars(self):
        """验证分钟线分发到 Stream"""
        dist, mock_redis, _, _ = _make_distributor()

        bars = [
            MinuteBar(
                ts_code="000001.SZ",
                trade_date="20240102",
                trade_time="09:31",
                open=10.0,
                high=10.1,
                low=9.9,
                close=10.05,
                vol=500.0,
                amount=5000.0,
            ),
            MinuteBar(
                ts_code="000001.SZ",
                trade_date="20240102",
                trade_time="09:32",
                open=10.05,
                high=10.2,
                low=10.0,
                close=10.15,
                vol=600.0,
                amount=6060.0,
            ),
        ]

        await dist.distribute_minute_bars("000001.SZ", bars)

        assert mock_redis.xadd.await_count == 2
        for call in mock_redis.xadd.call_args_list:
            assert call.args[0] == MARKET_STREAM

    @pytest.mark.asyncio
    async def test_distribute_minute_bars_empty(self):
        """验证空列表不写入"""
        dist, mock_redis, _, _ = _make_distributor()

        await dist.distribute_minute_bars("000001.SZ", [])

        mock_redis.xadd.assert_not_awaited()


# ---------------------------------------------------------------------------
class TestMarketDistributorSnapshot:
    # ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_snapshot_success(self):
        """验证快照通过 Pub/Sub 发布"""
        dist, mock_redis, _, mock_realtime = _make_distributor(with_realtime=True)

        snap = MarketSnapshot(ts_code="000001.SZ", price=10.5)
        mock_realtime.get_realtime_snapshot.return_value = snap

        result = await dist.distribute_snapshot("000001.SZ")

        assert result is snap
        mock_realtime.get_realtime_snapshot.assert_awaited_once_with("000001.SZ")
        mock_redis.publish.assert_awaited_once()
        channel = mock_redis.publish.call_args[0][0]
        assert channel == f"{SNAPSHOT_CHANNEL_PREFIX}.000001.SZ"

    @pytest.mark.asyncio
    async def test_distribute_snapshot_empty_returns_none(self):
        """Tushare 返回空数据时返回 None，不发布"""
        dist, mock_redis, _, mock_realtime = _make_distributor(with_realtime=True)
        mock_realtime.get_realtime_snapshot.return_value = None

        result = await dist.distribute_snapshot("000001.SZ")

        assert result is None
        mock_redis.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_distribute_snapshot_raises_without_realtime_client(self):
        """未传 realtime_client 时抛 RuntimeError"""
        dist, _, _, _ = _make_distributor(with_realtime=False)

        with pytest.raises(RuntimeError, match="realtime_client"):
            await dist.distribute_snapshot("000001.SZ")


# ---------------------------------------------------------------------------
class TestMarketDistributorDistributeOnce:
    # ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_once_delegates_to_daily_batch(self):
        """验证 distribute_once 是 distribute_daily_batch 的别名"""
        dist, mock_redis, mock_reader, _ = _make_distributor()

        daily_data = [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": "10.5"},
        ]
        mock_reader.get_daily.return_value = daily_data

        result = await dist.distribute_once(
            start_date="20240101",
            end_date="20240103",
        )

        assert result == 2
        assert mock_redis.xadd.await_count == 2

    @pytest.mark.asyncio
    async def test_distribute_once_no_active_codes(self):
        """验证无活跃代码时返回 0"""
        dist, mock_redis, _, _ = _make_distributor()
        dist._active_codes = []

        result = await dist.distribute_once()
        assert result == 0
