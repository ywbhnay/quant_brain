"""
实时行情消费者

职责：
1. 监听 Win10 miniQMT 网关推送至 market_data Stream 的实时行情
2. 解码 msgpack + base64 消息
3. 将实时 K 线写入 Redis Hash (最新价缓存) + PostgreSQL (持久化)
4. 通过 Pub/Sub 通知策略引擎有新行情到达

数据流:
  Win10 miniQMT → Redis Stream (market_data)
    → MarketStreamConsumer 解码
    → Redis Hash (最新价缓存)
    → PostgreSQL realtime_bars 表
    → Pub/Sub 通知

消息格式 (payload):
  {
    "type": str,              # "minute_bar" | "daily" | "snapshot"
    "ts_code": str,
    "trade_date": str,        # YYYYMMDD
    "trade_time": str,        # HH:MM (分钟线)
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "vol": float,
    "amount": float,
  }
"""

import asyncio
import base64
import logging
from collections.abc import Callable
from typing import Any

import msgpack

from quant_engine.market.distributor import MARKET_GROUP, MARKET_STREAM

logger = logging.getLogger("market_stream_consumer")

# Pub/Sub 频道 — 行情到达通知
MARKET_DATA_CHANNEL = "market_data_notifications"

# Redis Hash key — 最新价缓存
LATEST_PRICE_HASH = "market:latest_prices"


# ---------------------------------------------------------------------------
# 实时行情消费者
# ---------------------------------------------------------------------------


class MarketStreamConsumer:
    """
    实时行情消费者

    后台长驻任务，从 Redis Stream 读取 Win10 网关推送的实时行情，
    写入缓存 + 持久化存储，并通知上游策略引擎。

    使用方式:
        consumer = MarketStreamConsumer(
            redis_client=redis_client,
            get_db_conn=get_db_conn,
            consumer_name="quant_engine_market",
            on_bar_callback=my_strategy_handler,
        )
        await consumer.start()
    """

    def __init__(
        self,
        redis_client,
        get_db_conn: Callable,
        consumer_name: str = "quant_engine_market",
        on_bar_callback: Callable | None = None,
    ):
        """
        Args:
            redis_client: RedisClient 实例
            get_db_conn: 获取 asyncpg 数据库连接的 callable
            consumer_name: Consumer Group 中的消费者名称
            on_bar_callback: 可选回调，每处理完一条 K 线后调用 (供策略引擎消费)
        """
        self._redis = redis_client
        self._get_db_conn = get_db_conn
        self._consumer_name = consumer_name
        self._on_bar_callback = on_bar_callback
        self._running = False

    # ------------------------------------------------------------------
    # 主消费循环
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        启动消费循环 (不会返回，除非手动 stop)。

        步骤：
        1. 确保 Consumer Group 已创建
        2. XREADGROUP 阻塞读取新消息
        3. 逐条解码、持久化、通知
        4. XACK 确认消费
        """
        self._running = True

        await self._redis.xgroup_create(
            MARKET_STREAM,
            MARKET_GROUP,
            mkstream=True,
        )

        logger.info(f"实时行情消费者启动: {self._consumer_name}")

        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    MARKET_GROUP,
                    self._consumer_name,
                    {MARKET_STREAM: ">"},
                    count=50,
                    block=5000,
                )

                if not messages:
                    continue

                for _stream_name, msg_list in messages:
                    for msg_id, fields in msg_list:
                        await self._handle_message(msg_id, fields)

            except asyncio.CancelledError:
                logger.info("实时行情消费者已停止")
                break
            except Exception as e:
                logger.error(f"实时行情消费异常: {e}", exc_info=True)
                await asyncio.sleep(1)

    def stop(self) -> None:
        """停止消费循环"""
        self._running = False

    # ------------------------------------------------------------------
    # 消息处理
    # ------------------------------------------------------------------

    async def _handle_message(self, msg_id: str, fields: dict) -> None:
        """
        处理单条行情消息。

        1. base64 解码 + msgpack 解包
        2. 更新 Redis Hash 最新价缓存
        3. 持久化到 PostgreSQL
        4. Pub/Sub 通知
        5. 可选回调
        6. XACK
        """
        try:
            # 1. 解码
            bar = self._decode_message(fields)
            if bar is None:
                await self._ack(msg_id)
                return

            bar_type = bar.get("type", "minute_bar")
            ts_code = bar.get("ts_code")

            if not ts_code:
                logger.warning(f"消息缺少 ts_code (msg_id={msg_id})")
                await self._ack(msg_id)
                return

            # 2. 更新最新价缓存
            close_price = bar.get("close")
            if close_price is not None:
                await self._redis.hset(
                    LATEST_PRICE_HASH,
                    mapping={ts_code: str(close_price)},
                )

            # 3. 持久化到 PostgreSQL
            await self._persist_bar(bar)

            # 4. Pub/Sub 通知
            await self._redis.publish(
                MARKET_DATA_CHANNEL,
                f"{ts_code}:{bar_type}",
            )

            # 5. 可选回调 (策略引擎消费)
            if self._on_bar_callback:
                try:
                    if asyncio.iscoroutinefunction(self._on_bar_callback):
                        await self._on_bar_callback(bar)
                    else:
                        self._on_bar_callback(bar)
                except Exception as e:
                    logger.error(f"on_bar_callback 异常: {e}")

        except Exception as e:
            logger.error(f"处理行情消息失败 (msg_id={msg_id}): {e}", exc_info=True)
            return

        # 6. 确认消费
        await self._ack(msg_id)

    def _decode_message(self, fields: dict) -> dict[str, Any] | None:
        """
        解码消息: base64 → msgpack → dict。

        与 order_status_consumer 一致，Ubuntu 端 Redis 使用 decode_responses=True。
        """
        raw = fields.get("payload")
        if raw is None:
            logger.warning("消息缺少 payload 字段")
            return None

        try:
            binary = base64.b64decode(raw)
            return msgpack.unpackb(binary, raw=False)
        except Exception as e:
            logger.error(f"行情消息解码失败: {e}")
            return None

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------

    async def _persist_bar(self, bar: dict[str, Any]) -> None:
        """
        将 K 线数据写入 PostgreSQL realtime_bars 表。

        使用 ON CONFLICT 做 upsert，防止重复插入。
        """
        ts_code = bar.get("ts_code")
        trade_date = bar.get("trade_date")
        trade_time = bar.get("trade_time", "")
        bar_type = bar.get("type", "minute_bar")

        if not ts_code or not trade_date:
            return

        try:
            conn = await self._get_conn()
            await conn.execute(
                """
                INSERT INTO realtime_bars (
                    ts_code, trade_date, trade_time, bar_type,
                    open, high, low, close, vol, amount
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (ts_code, trade_date, trade_time, bar_type)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    vol = EXCLUDED.vol,
                    amount = EXCLUDED.amount,
                    updated_at = now()
                """,
                ts_code,
                trade_date,
                trade_time,
                bar_type,
                bar.get("open"),
                bar.get("high"),
                bar.get("low"),
                bar.get("close"),
                bar.get("vol"),
                bar.get("amount"),
            )
        except Exception as e:
            logger.warning(f"持久化 K 线失败 ({ts_code}): {e}")

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    async def _ack(self, msg_id: str) -> None:
        """确认消费消息"""
        try:
            await self._redis.xack(MARKET_STREAM, MARKET_GROUP, msg_id)
        except Exception as e:
            logger.warning(f"XACK 失败 (msg_id={msg_id}): {e}")
