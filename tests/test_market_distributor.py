"""
market/distributor.py 单元测试

覆盖：
1. MarketDistributor.start / stop
2. _snapshot_loop 周期性分发
3. distribute_minute_bars
4. distribute_once 盘后跑批
5. Callback 注册与调用
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from quant_engine.market.distributor import (
    MarketDistributor,
    MARKET_STREAM,
    MARKET_GROUP,
    SNAPSHOT_CHANNEL_PREFIX,
    MINUTE_BAR_CHANNEL,
)
from quant_engine.market.snapshot import MarketSnapshot, MinuteBar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_distributor():
    """创建 distributor，使用 mock redis 和 mock fetcher"""
    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock(return_value="OK")
    mock_redis.xadd = AsyncMock(return_value="1-0")
    mock_redis.publish = AsyncMock(return_value=1)

    mock_fetcher = AsyncMock()

    distributor = MarketDistributor(
        redis_client=mock_redis,
        fetcher=mock_fetcher,
        snapshot_interval=1,  # 1s 间隔（测试用）
        stream_maxlen=10000,
        active_codes=["000001.SZ", "000002.SZ"],
    )
    return distributor, mock_redis, mock_fetcher


# ---------------------------------------------------------------------------
class TestMarketDistributorLifecycle:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_start_creates_consumer_group(self):
        """验证 start 创建 Consumer Group"""
        dist, mock_redis, _ = _make_distributor()

        # 阻止真正的 snapshot_loop 运行
        with patch.object(dist, "_snapshot_loop", new_callable=AsyncMock):
            await dist.start()

        mock_redis.xgroup_create.assert_awaited_once_with(
            MARKET_STREAM, MARKET_GROUP, mkstream=True,
        )
        assert dist._running is True

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks(self):
        """验证 stop 取消后台任务"""
        dist, _, _ = _make_distributor()
        dist._running = True
        # 创建一个长期运行的后台任务
        async def long_running():
            while True:
                await asyncio.sleep(1)

        dist._tasks = [asyncio.create_task(long_running())]

        await dist.stop()

        assert dist._running is False
        assert len(dist._tasks) == 0

    @pytest.mark.asyncio
    async def test_stop_empty_tasks(self):
        """验证无任务时 stop 不报错"""
        dist, _, _ = _make_distributor()
        await dist.stop()
        assert dist._running is False


# ---------------------------------------------------------------------------
class TestMarketDistributorSnapshot:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_fetch_and_distribute_single_stock(self):
        """验证单只股票快照分发"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        dist._active_codes = ["000001.SZ"]  # 只测试一只

        snap = MarketSnapshot(ts_code="000001.SZ", price=10.5)
        mock_fetcher.get_realtime_snapshot.return_value = snap

        await dist._fetch_and_distribute()

        # 写入 Stream
        mock_redis.xadd.assert_awaited_once()
        call_args = mock_redis.xadd.call_args
        assert call_args.args[0] == MARKET_STREAM
        assert call_args.kwargs["maxlen"] == 10000

        # Pub/Sub 通知
        mock_redis.publish.assert_awaited_once()
        pub_call = mock_redis.publish.call_args
        assert pub_call.args[0] == f"{SNAPSHOT_CHANNEL_PREFIX}.000001.SZ"

    @pytest.mark.asyncio
    async def test_fetch_and_distribute_skips_none_snapshot(self):
        """验证 None 快照跳过分发"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        mock_fetcher.get_realtime_snapshot.return_value = None

        await dist._fetch_and_distribute()

        mock_redis.xadd.assert_not_awaited()
        mock_redis.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_distribute_no_active_codes(self):
        """验证无活跃代码时跳过"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        dist._active_codes = []

        await dist._fetch_and_distribute()

        mock_redis.xadd.assert_not_awaited()
        mock_fetcher.get_realtime_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_distribute_calls_callbacks(self):
        """验证快照回调被调用"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        dist._active_codes = ["000001.SZ"]  # 只测试一只

        snap = MarketSnapshot(ts_code="000001.SZ", price=10.5)
        mock_fetcher.get_realtime_snapshot.return_value = snap

        callback = AsyncMock()
        dist.register_callback(callback)

        await dist._fetch_and_distribute()

        callback.assert_awaited_once()
        snap_data = callback.await_args.args[0]
        assert snap_data["ts_code"] == "000001.SZ"
        assert snap_data["price"] == 10.5

    @pytest.mark.asyncio
    async def test_fetch_and_distribute_callback_error_doesnt_break_flow(self):
        """验证回调异常不影响后续流程"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        dist._active_codes = ["000001.SZ"]  # 只测试一只

        snap = MarketSnapshot(ts_code="000001.SZ", price=10.5)
        mock_fetcher.get_realtime_snapshot.return_value = snap

        failing_cb = AsyncMock(side_effect=RuntimeError("callback error"))
        ok_cb = AsyncMock()
        dist.register_callback(failing_cb)
        dist.register_callback(ok_cb)

        await dist._fetch_and_distribute()

        # 第二个回调仍应被调用
        ok_cb.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fetch_and_distribute_fetch_error_doesnt_break_flow(self):
        """验证单只股票获取失败不影响其他股票"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        dist._active_codes = ["000001.SZ", "000002.SZ"]

        call_count = 0

        async def mock_snapshot(code):
            nonlocal call_count
            call_count += 1
            if code == "000001.SZ":
                raise RuntimeError("fetch error")
            return MarketSnapshot(ts_code=code, price=20.0)

        mock_fetcher.get_realtime_snapshot.side_effect = mock_snapshot

        await dist._fetch_and_distribute()

        assert mock_redis.xadd.await_count >= 1  # 000002 应成功


# ---------------------------------------------------------------------------
class TestMarketDistributorMinuteBars:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_minute_bars(self):
        """验证分钟线分发到 Stream + Pub/Sub"""
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
        assert mock_redis.publish.await_count == 2

        # 验证 Pub/Sub channel
        pub_calls = mock_redis.publish.call_args_list
        assert pub_calls[0].args[0] == MINUTE_BAR_CHANNEL


# ---------------------------------------------------------------------------
class TestMarketDistributorDistributeOnce:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_distribute_once(self):
        """验证一次性分发"""
        dist, mock_redis, mock_fetcher = _make_distributor()

        daily_data = [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": "10.5"},
        ]
        mock_fetcher.get_daily.return_value = daily_data

        result = await dist.distribute_once(start_date="20240101", end_date="20240103")

        assert result == 2  # 2 只股票都成功
        # 每只股票 1 条数据 → 2 次 xadd
        assert mock_redis.xadd.await_count == 2

    @pytest.mark.asyncio
    async def test_distribute_once_no_active_codes(self):
        """验证无活跃代码时返回 0"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        dist._active_codes = []

        result = await dist.distribute_once()
        assert result == 0

    @pytest.mark.asyncio
    async def test_distribute_once_empty_daily(self):
        """验证无日线数据时跳过"""
        dist, mock_redis, mock_fetcher = _make_distributor()
        mock_fetcher.get_daily.return_value = []

        result = await dist.distribute_once()
        assert result == 0  # 无数据 = 0 只成功分发
