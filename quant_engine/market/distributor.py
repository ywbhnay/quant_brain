"""
行情分发器

职责：
1. 盘后从 PostgreSQL 读取 ETL 已填充的行情数据，写入 Redis Stream
2. 盘中静默 — 行情由 Win10 QMT 网关推送至 Redis Stream (MARKET_STREAM)
3. 提供一次性分发接口供跑批任务调用
4. 可选：盘中调用 RealtimeQuoteClient 获取 5 档快照

数据流:
  夜间跑批: PG (quant_db, ETL-populated) → PGMarketReader → Redis Stream
  盘中实盘: Win10 QMT → Redis Stream (MARKET_STREAM) → Ubuntu 消费

盘中 distributor 不参与行情生产，仅作为夜间跑批工具使用（或可选实时快照）。
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quant_engine.market.fetcher import RealtimeQuoteClient
    from quant_engine.market.reader import PGMarketReader

logger = logging.getLogger("market_distributor")

# ---------------------------------------------------------------------------
# Stream / Channel 常量
# ---------------------------------------------------------------------------
MARKET_STREAM = "market_data"
MARKET_GROUP = "market_consumers"
SNAPSHOT_CHANNEL_PREFIX = "market.snapshot"
MINUTE_BAR_CHANNEL = "market.minute_bar"

# ---------------------------------------------------------------------------
# 行情分发器 (仅跑批)
# ---------------------------------------------------------------------------


class MarketDistributor:
    """
    行情分发器 — 仅用于夜间跑批场景。

    盘中行情由 Win10 miniQMT 网关主动推送至 Redis Stream，
    此组件在盘中不参与行情生产。

    使用方式:
        # 盘后跑批
        reader = PGMarketReader(pool)
        distributor = MarketDistributor(redis_client, reader=reader)
        await distributor.distribute_daily_batch(start_date="20240101")

        # 盘中实时快照 (可选)
        realtime = RealtimeQuoteClient()
        distributor = MarketDistributor(
            redis_client, reader=reader, realtime_client=realtime
        )
        snap = await distributor.distribute_snapshot("000001.SZ")
    """

    def __init__(
        self,
        redis_client,
        reader: PGMarketReader,
        realtime_client: RealtimeQuoteClient | None = None,
        stream_maxlen: int = 10000,
        active_codes: list[str] | None = None,
    ):
        """
        Args:
            redis_client: RedisClient 实例
            reader: PGMarketReader 实例（从 PG 读历史行情）
            realtime_client: RealtimeQuoteClient 实例（可选，用于实时快照）
            stream_maxlen: Stream 最大长度 (XTRIM)
            active_codes: 需要分发数据的股票代码列表
        """
        self._redis = redis_client
        self._reader = reader
        self._realtime_client = realtime_client
        self._stream_maxlen = stream_maxlen
        self._active_codes = active_codes or []

    # ------------------------------------------------------------------
    # 日线跑批
    # ------------------------------------------------------------------

    async def distribute_daily_batch(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """
        批量从 PG 读取活跃股票的日线数据并分发到 Redis Stream。
        用于盘后跑批场景。

        Returns:
            成功分发的股票数量
        """
        if not self._active_codes:
            logger.warning("无活跃股票代码，跳过日线跑批")
            return 0

        count = 0
        for code in self._active_codes:
            try:
                daily = await self._reader.get_daily(
                    code,
                    start_date=start_date,
                    end_date=end_date,
                )
                if not daily:
                    continue

                for row in daily:
                    await self._redis.xadd(
                        MARKET_STREAM,
                        data={k: str(v) for k, v in row.items() if v is not None},
                        maxlen=self._stream_maxlen,
                    )
                count += 1
            except Exception as e:
                logger.warning(f"股票 {code} 日线分发失败: {e}")

        logger.info(f"日线跑批完成: {count}/{len(self._active_codes)} 只")
        return count

    # ------------------------------------------------------------------
    # 分钟线分发 (跑批用)
    # ------------------------------------------------------------------

    async def distribute_minute_bars(
        self,
        ts_code: str,
        bars: list,
    ) -> None:
        """
        将分钟线数据分发到 Redis Stream。
        由跑批任务调用，非盘中实时推送。

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

        logger.debug(f"分钟线已分发: {ts_code}, {len(bars)} 条")

    # ------------------------------------------------------------------
    # 实时快照 (可选)
    # ------------------------------------------------------------------

    async def distribute_snapshot(self, ts_code: str):
        """
        获取实时快照并分发到 Redis Pub/Sub。

        需要初始化时传入 realtime_client；否则抛 RuntimeError。

        Returns:
            MarketSnapshot 或 None (无数据时)
        """
        if self._realtime_client is None:
            raise RuntimeError(
                "distribute_snapshot 需要 realtime_client；"
                "请在构造 MarketDistributor 时传入 RealtimeQuoteClient"
            )

        snapshot = await self._realtime_client.get_realtime_snapshot(ts_code)
        if snapshot is None:
            logger.warning(f"实时快照无数据: {ts_code}")
            return None

        # 发布到 Pub/Sub (RedisClient.publish 接受 str，JSON 序列化)
        await self._redis.publish(
            f"{SNAPSHOT_CHANNEL_PREFIX}.{ts_code}",
            json.dumps(snapshot.to_dict(), ensure_ascii=False, default=str),
        )
        return snapshot

    # ------------------------------------------------------------------
    # 通用一次性分发
    # ------------------------------------------------------------------

    async def distribute_once(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """
        一次性获取所有活跃股票的日线数据并分发。

        Returns:
            成功分发的股票数量
        """
        return await self.distribute_daily_batch(
            start_date=start_date,
            end_date=end_date,
        )
