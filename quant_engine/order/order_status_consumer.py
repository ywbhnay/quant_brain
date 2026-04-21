"""
订单状态回报消费者

职责：
1. 监听 Win10 miniQMT 网关推送至 order_status_updates Stream 的状态回报
2. 解码 msgpack + base64 消息
3. 调用 OrderStateMachine.transition() 校验并执行状态转换
4. 更新 PostgreSQL 订单状态
5. 将超时未 ACK 的 SENT 订单推送至 dead_letter 或重发

数据流:
  Win10 miniQMT → Redis Stream (order_status_updates)
    → OrderStatusConsumer 解码
    → OrderStateMachine.transition() 校验
    → PostgreSQL orders 表状态更新

消息格式 (payload):
  {
    "order_id": str,          # Ubuntu 侧生成的 UUID
    "status": str,            # ACK / FILLED / PARTIAL / REJECTED / CANCELLED
    "qmt_order_id": str,      # 券商返回的委托编号
    "filled_price": float,    # 成交均价 (可选)
    "filled_volume": int,     # 成交数量 (可选)
    "reason": str,            # 拒绝/撤单原因 (可选)
  }
"""

import asyncio
import base64
import logging
from collections.abc import Callable
from typing import Any

import msgpack

from quant_engine.order.state_machine import (
    OrderStateMachine,
    OrderStatus,
    OrderTransitionError,
)

logger = logging.getLogger("order_status_consumer")

# ---------------------------------------------------------------------------
# Stream 名称常量
# ---------------------------------------------------------------------------
ORDER_STATUS_STREAM = "order_status_updates"
ORDER_STATUS_GROUP = "quant_engine"

# 兜底扫描间隔 (秒) — 超时未 ACK 的 SENT 订单触发重发或撤销
ORPHAN_SCAN_INTERVAL = 30
ORPHAN_TIMEOUT_SECONDS = 60


# ---------------------------------------------------------------------------
# 订单状态回报消费者
# ---------------------------------------------------------------------------


class OrderStatusConsumer:
    """
    订单状态回报消费者

    后台长驻任务，从 Redis Stream 读取 Win10 网关返回的订单状态变更，
    校验状态机后更新数据库。

    使用方式:
        consumer = OrderStatusConsumer(
            redis_client=redis_client,
            get_db_conn=get_db_conn,
            consumer_name="quant_engine_1",
        )
        # 启动消费循环 (不会返回)
        await consumer.start()

        # 或启动兜底扫描
        await consumer.start_orphan_checker()
    """

    def __init__(
        self,
        redis_client,
        get_db_conn: Callable,
        consumer_name: str = "quant_engine_1",
    ):
        """
        Args:
            redis_client: RedisClient 实例
            get_db_conn: 获取 asyncpg 数据库连接的 callable
            consumer_name: Consumer Group 中的消费者名称
        """
        self._redis = redis_client
        self._get_db_conn = get_db_conn
        self._consumer_name = consumer_name
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
        3. 逐条解码、校验、处理
        4. XACK 确认消费
        """
        self._running = True

        # 确保 Consumer Group 存在
        await self._redis.xgroup_create(
            ORDER_STATUS_STREAM,
            ORDER_STATUS_GROUP,
            mkstream=True,
        )

        logger.info(f"订单状态消费者启动: {self._consumer_name}")

        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    ORDER_STATUS_GROUP,
                    self._consumer_name,
                    {ORDER_STATUS_STREAM: ">"},
                    count=10,
                    block=5000,
                )

                if not messages:
                    continue

                for _stream_name, msg_list in messages:
                    for msg_id, fields in msg_list:
                        await self._handle_message(msg_id, fields)

            except asyncio.CancelledError:
                logger.info("订单状态消费者已停止")
                break
            except Exception as e:
                logger.error(f"订单状态消费异常: {e}", exc_info=True)
                await asyncio.sleep(1)

    def stop(self) -> None:
        """停止消费循环"""
        self._running = False

    # ------------------------------------------------------------------
    # 消息处理
    # ------------------------------------------------------------------

    async def _handle_message(self, msg_id: str, fields: dict) -> None:
        """
        处理单条状态回报消息。

        1. base64 解码 + msgpack 解包
        2. 从 DB 读取当前订单状态
        3. 状态机转换
        4. 更新 DB
        5. XACK
        """
        try:
            # 1. 解码
            cmd = self._decode_message(fields)
            if cmd is None:
                await self._ack(msg_id)
                return

            order_id = cmd.get("order_id")
            new_status_str = cmd.get("status")

            if not order_id or not new_status_str:
                logger.warning(f"消息格式无效 (msg_id={msg_id}): {cmd}")
                await self._ack(msg_id)
                return

            new_status = OrderStatus(new_status_str)

            # 2. 从 DB 读取当前状态
            conn = await self._get_conn()
            row = await conn.fetchrow(
                "SELECT status FROM orders WHERE id = $1",
                order_id,
            )

            if row is None:
                logger.warning(f"订单不存在: {order_id}")
                await self._ack(msg_id)
                return

            current_status = OrderStatus(row["status"])

            # 终态直接 ACK，不做重复处理
            sm = OrderStateMachine(current_status, order_id=order_id)
            if sm.is_terminal:
                logger.debug(f"订单已处于终态，跳过: {order_id} ({current_status.value})")
                await self._ack(msg_id)
                return

            # 3. 状态机转换
            try:
                sm.transition(new_status)
            except OrderTransitionError as e:
                logger.warning(f"非法状态转换: {e}")
                await self._ack(msg_id)
                return

            # 4. 更新 DB
            qmt_order_id = cmd.get("qmt_order_id")
            await self._update_order(
                order_id=order_id,
                status=new_status,
                qmt_order_id=qmt_order_id,
                filled_price=cmd.get("filled_price"),
                filled_volume=cmd.get("filled_volume"),
                reason=cmd.get("reason"),
            )

            logger.info(
                f"订单状态已更新: {order_id} "
                f"{current_status.value} → {new_status.value}"
            )

        except Exception as e:
            logger.error(f"处理消息失败 (msg_id={msg_id}): {e}", exc_info=True)
            return

        # 5. 确认消费
        await self._ack(msg_id)

    def _decode_message(self, fields: dict) -> dict[str, Any] | None:
        """
        解码消息: base64 → msgpack → dict。

        Ubuntu 端 Redis 客户端使用 decode_responses=True，
        所以 payload 字段是 base64 编码的 ASCII 字符串。
        """
        raw = fields.get("payload")
        if raw is None:
            logger.warning("消息缺少 payload 字段")
            return None

        try:
            binary = base64.b64decode(raw)
            return msgpack.unpackb(binary, raw=False)
        except Exception as e:
            logger.error(f"消息解码失败: {e}")
            return None

    # ------------------------------------------------------------------
    # 兜底扫描 — 超时未 ACK 订单
    # ------------------------------------------------------------------

    async def start_orphan_checker(self) -> None:
        """
        启动兜底扫描任务 (与 start() 并行运行)。

        定期扫描 DB 中超时未 ACK 的 SENT 订单:
        - 超时 < 3 次: 重发到 Redis Stream
        - 超时 >= 3 次: 标记为 REJECTED
        """
        self._running = True
        logger.info("兜底扫描任务启动")

        while self._running:
            try:
                await self._scan_orphan_orders()
            except asyncio.CancelledError:
                logger.info("兜底扫描任务已停止")
                break
            except Exception as e:
                logger.error(f"兜底扫描异常: {e}", exc_info=True)

            await asyncio.sleep(ORPHAN_SCAN_INTERVAL)

    async def _scan_orphan_orders(self) -> None:
        """扫描并处理超时未 ACK 订单"""
        conn = await self._get_conn()

        rows = await conn.fetch(
            f"""
            SELECT id, ts_code, price, volume, direction, order_type,
                   retry_count, updated_at
            FROM orders
            WHERE status = 'SENT'
              AND updated_at < now() - interval '{ORPHAN_TIMEOUT_SECONDS} seconds'
            ORDER BY updated_at
            """
        )

        if not rows:
            return

        logger.warning(f"发现 {len(rows)} 笔超时未 ACK 订单")

        from quant_engine.order.executor import TRADE_ORDERS_STREAM

        for row in rows:
            order_id = row["id"]
            retry_count = row["retry_count"]

            if retry_count >= 3:
                # 超过最大重试次数，标记为拒绝
                await conn.execute(
                    "UPDATE orders SET status = 'REJECTED', updated_at = now() WHERE id = $1",
                    order_id,
                )
                logger.warning(
                    f"订单 {order_id} 超过最大重试次数 ({retry_count})，"
                    f"已标记为 REJECTED"
                )
            else:
                # 重发到 Redis Stream
                payload = msgpack.packb(
                    {
                        "action": "place",
                        "order_id": order_id,
                        "ts_code": row["ts_code"],
                        "price": row["price"],
                        "volume": row["volume"],
                        "direction": row["direction"],
                        "order_type": row["order_type"],
                    },
                    use_bin_type=True,
                )
                await self._redis.xadd(
                    TRADE_ORDERS_STREAM,
                    data={"payload": base64.b64encode(payload).decode("ascii")},
                    maxlen=50000,
                )
                await conn.execute(
                    "UPDATE orders SET retry_count = retry_count + 1, "
                    "updated_at = now() WHERE id = $1",
                    order_id,
                )
                logger.info(f"订单 {order_id} 已重发 (retry={retry_count + 1})")

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    async def _ack(self, msg_id: str) -> None:
        """确认消费消息"""
        try:
            await self._redis.xack(
                ORDER_STATUS_STREAM, ORDER_STATUS_GROUP, msg_id
            )
        except Exception as e:
            logger.warning(f"XACK 失败 (msg_id={msg_id}): {e}")

    async def _update_order(
        self,
        order_id: str,
        status: OrderStatus,
        qmt_order_id: str | None = None,
        filled_price: float | None = None,
        filled_volume: int | None = None,
        reason: str | None = None,
    ) -> None:
        """更新订单状态及成交信息"""
        conn = await self._get_conn()

        if qmt_order_id:
            if filled_price is not None and filled_volume is not None:
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = $1,
                        qmt_order_id = $2,
                        filled_price = $3,
                        filled_volume = $4,
                        updated_at = now()
                    WHERE id = $5
                    """,
                    status.value,
                    qmt_order_id,
                    filled_price,
                    filled_volume,
                    order_id,
                )
            else:
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = $1,
                        qmt_order_id = $2,
                        updated_at = now()
                    WHERE id = $3
                    """,
                    status.value,
                    qmt_order_id,
                    order_id,
                )
        else:
            updates = ["status = $1", "updated_at = now()"]
            params: list = [status.value]
            param_idx = 2

            if reason:
                updates.append(f"reason = ${param_idx}")
                params.append(reason)
                param_idx += 1

            params.append(order_id)

            await conn.execute(
                f"UPDATE orders SET {', '.join(updates)} WHERE id = ${param_idx}",
                *params,
            )

    async def _get_conn(self):
        """获取数据库连接"""
        if self._get_db_conn is None:
            raise RuntimeError("未配置数据库连接")
        return await self._get_db_conn()
