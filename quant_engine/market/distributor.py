"""
行情分发器

职责：
1. 盘后从 Tushare 拉取历史数据，写入 Redis Stream + PostgreSQL
2. 盘中静默 — 行情由 Win10 QMT 网关推送至 Redis Stream (MARKET_STREAM)
3. 提供一次性分发接口供跑批任务调用

数据流:
  夜间跑批: Tushare → MarketFetcher → Redis Stream + PostgreSQL
  盘中实盘: Win10 QMT → Redis Stream (MARKET_STREAM) → Ubuntu 消费

盘中 distributor 不参与行情生产，仅作为夜间跑批工具使用。
"""

import logging

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
        distributor = MarketDistributor(redis_client, fetcher)
        await distributor.distribute_daily_batch(start_date="20240101")
        await distributor.distribute_minute_batch("000001.SZ", bars)
    """

    def __init__(
        self,
        redis_client,
        fetcher,
        stream_maxlen: int = 10000,
        active_codes: list[str] | None = None,
    ):
        """
        Args:
            redis_client: RedisClient 实例
            fetcher: MarketFetcher 实例
            stream_maxlen: Stream 最大长度 (XTRIM)
            active_codes: 需要分发数据的股票代码列表
        """
        self._redis = redis_client
        self._fetcher = fetcher
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
        批量获取活跃股票的日线数据并分发到 Redis Stream。
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
                daily = await self._fetcher.get_daily(
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
