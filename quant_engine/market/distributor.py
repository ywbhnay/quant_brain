"""
行情分发器

职责：
1. 从 MarketFetcher 拉取行情数据
2. 写入 Redis Stream (持久化，带 MAXLEN 截断)
3. 通过 Redis Pub/Sub 分发实时快照
4. 定时任务: 周期性刷新快照

数据流:
  Tushare → MarketFetcher → Redis Stream + Pub/Sub → PostgreSQL
"""
import asyncio
import logging
from datetime import datetime, date
from typing import Callable, Awaitable

logger = logging.getLogger("market_distributor")

# ---------------------------------------------------------------------------
# Stream / Channel 常量
# ---------------------------------------------------------------------------
MARKET_STREAM = "market_data"
MARKET_GROUP = "market_consumers"
SNAPSHOT_CHANNEL_PREFIX = "market.snapshot"
MINUTE_BAR_CHANNEL = "market.minute_bar"

# ---------------------------------------------------------------------------
# 行情分发器
# ---------------------------------------------------------------------------


class MarketDistributor:
    """
    行情分发器

    将 MarketFetcher 获取的行情数据分发到:
    1. Redis Stream (market_data) — 持久化，支持断线重连后消费
    2. Redis Pub/Sub (market.snapshot.{ts_code}) — 实时通知

    使用方式:
        distributor = MarketDistributor(redis_client, fetcher)
        await distributor.start()
        # ... 后台自动分发
        await distributor.stop()
    """

    def __init__(
        self,
        redis_client,
        fetcher,
        snapshot_interval: int = 10,
        stream_maxlen: int = 10000,
        active_codes: list[str] | None = None,
    ):
        """
        Args:
            redis_client: RedisClient 实例
            fetcher: MarketFetcher 实例
            snapshot_interval: 快照刷新间隔 (秒)
            stream_maxlen: Stream 最大长度 (XTRIM)
            active_codes: 需要监听的股票代码列表
        """
        self._redis = redis_client
        self._fetcher = fetcher
        self._snapshot_interval = snapshot_interval
        self._stream_maxlen = stream_maxlen
        self._active_codes = active_codes or []
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._snapshot_callbacks: list[Callable[[dict], Awaitable[None]]] = []

    async def start(self) -> None:
        """启动行情分发"""
        self._running = True

        # 创建 Consumer Group
        await self._redis.xgroup_create(
            MARKET_STREAM, MARKET_GROUP, mkstream=True,
        )

        # 启动后台任务
        self._tasks.append(
            asyncio.create_task(self._snapshot_loop())
        )

        logger.info(
            f"行情分发器已启动: "
            f"interval={self._snapshot_interval}s, "
            f"stream_maxlen={self._stream_maxlen}, "
            f"stocks={len(self._active_codes)}"
        )

    async def stop(self) -> None:
        """停止行情分发"""
        self._running = False
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("行情分发器已停止")

    def register_callback(
        self, callback: Callable[[dict], Awaitable[None]]
    ) -> None:
        """注册快照回调 (用于通知下游消费者)"""
        self._snapshot_callbacks.append(callback)

    # ------------------------------------------------------------------
    # 快照循环
    # ------------------------------------------------------------------

    async def _snapshot_loop(self) -> None:
        """周期性获取快照并分发"""
        while self._running:
            try:
                await self._fetch_and_distribute()
            except Exception as e:
                logger.error(f"快照刷新失败: {e}")
            await asyncio.sleep(self._snapshot_interval)

    async def _fetch_and_distribute(self) -> None:
        """获取当前快照并分发到 Stream + Pub/Sub"""
        if not self._active_codes:
            logger.debug("无活跃股票代码，跳过快照分发")
            return

        for code in self._active_codes:
            try:
                snap = await self._fetcher.get_realtime_snapshot(code)
                if snap is None:
                    continue

                snap_data = snap.to_dict()

                # 1. 写入 Redis Stream
                await self._redis.xadd(
                    MARKET_STREAM,
                    data={k: str(v) for k, v in snap_data.items() if v is not None},
                    maxlen=self._stream_maxlen,
                )

                # 2. Pub/Sub 通知
                channel = f"{SNAPSHOT_CHANNEL_PREFIX}.{code}"
                import json
                await self._redis.publish(
                    channel, json.dumps(snap_data, ensure_ascii=False)
                )

                # 3. 回调通知
                for cb in self._snapshot_callbacks:
                    try:
                        await cb(snap_data)
                    except Exception as e:
                        logger.warning(f"快照回调失败: {e}")

            except Exception as e:
                logger.warning(f"股票 {code} 快照获取失败: {e}")

    # ------------------------------------------------------------------
    # 分钟线分发
    # ------------------------------------------------------------------

    async def distribute_minute_bars(
        self,
        ts_code: str,
        bars: list,
    ) -> None:
        """
        将分钟线数据分发到 Stream + 落盘。

        Args:
            ts_code: 股票代码
            bars: list[MinuteBar]
        """
        for bar in bars:
            bar_data = bar.to_dict()

            # 写入 Stream
            await self._redis.xadd(
                MARKET_STREAM,
                data={**bar_data, "type": "minute_bar"},
                maxlen=self._stream_maxlen,
            )

            # Pub/Sub 通知
            await self._redis.publish(
                MINUTE_BAR_CHANNEL,
                f"{ts_code}@{bar.trade_date}T{bar.trade_time}",
            )

    # ------------------------------------------------------------------
    # 一次性分发 (盘后跑批用)
    # ------------------------------------------------------------------

    async def distribute_once(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """
        一次性获取所有活跃股票的日线数据并分发。
        用于盘后跑批场景。

        Returns:
            成功分发的股票数量
        """
        if not self._active_codes:
            return 0

        count = 0
        for code in self._active_codes:
            try:
                daily = await self._fetcher.get_daily(
                    code, start_date=start_date, end_date=end_date,
                )
                if not daily:
                    continue

                for row in daily:
                    await self._redis.xadd(
                        MARKET_STREAM,
                        data={
                            k: str(v) for k, v in row.items() if v is not None
                        },
                        maxlen=self._stream_maxlen,
                    )
                count += 1
            except Exception as e:
                logger.warning(f"股票 {code} 分发失败: {e}")

        logger.info(f"一次性分发完成: {count}/{len(self._active_codes)} 只")
        return count
