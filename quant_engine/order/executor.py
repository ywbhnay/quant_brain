"""
订单执行器

职责：
1. 接收策略信号，执行风控检查
2. 创建订单记录 (PENDING)，写入 Redis Stream (SENT)
3. 封装 xtquant miniQMT 下单接口 (Windows 端调用)
4. 撤单指令下发

数据流 (Ubuntu quant-engine 侧):
  策略信号 → RiskChecker.check() → 通过则创建订单(PENDING)
                                  → 写入 Redis Stream trade_orders
                                  → 状态流转 PENDING → SENT

数据流 (Win10 miniQMT 侧):
  XREADGROUP trade_orders → xtquant 下单 → XACK + 推送状态回 Redis Stream

设计原则：
- xtquant 接口可替换 (真实/模拟)，通过 protocol 解耦
- 订单持久化在发送前完成，确保可追溯
- Redis Stream 作为跨平台通信骨干，不依赖 HTTP
"""
import json
import logging
import uuid
from typing import Any, Callable, Protocol

from quant_engine.order.state_machine import OrderStatus
from quant_engine.risk.checker import RiskChecker, RiskCheckResult

logger = logging.getLogger("order_executor")

# ---------------------------------------------------------------------------
# Redis Stream 名称常量
# ---------------------------------------------------------------------------
TRADE_ORDERS_STREAM = "trade_orders"
TRADE_ORDERS_GROUP = "quant_executor"

# 订单状态回调 Stream (miniQMT → quant-engine)
ORDER_STATUS_STREAM = "order_status_updates"


# ---------------------------------------------------------------------------
# xtquant 适配器接口
# ---------------------------------------------------------------------------

class XtquantAdapter(Protocol):
    """
    xtquant 适配器协议。

    Windows 端实现: 调用真实 xtquant API
    Linux 端实现: 模拟下单 (用于开发和测试)
    """

    async def submit_order(
        self,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        order_type: str = "LIMIT",
    ) -> dict[str, Any]:
        """
        提交订单到券商。

        Returns:
            {"qmt_order_id": str, "status": "ACK" | "REJECTED", "reason": str | None}
        """
        ...

    async def cancel_order(self, qmt_order_id: str) -> bool:
        """
        撤单。

        Returns:
            True 撤单成功，False 失败
        """
        ...


# ---------------------------------------------------------------------------
# 模拟 xtquant 适配器 (Linux 开发/测试用)
# ---------------------------------------------------------------------------

class MockXtquantAdapter:
    """
    模拟 xtquant 适配器。

    行为：
    - submit_order: 延迟 50ms 后返回 ACK + 模拟 qmt_order_id
    - cancel_order: 延迟 20ms 后返回 True
    """

    async def submit_order(
        self,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        order_type: str = "LIMIT",
    ) -> dict[str, Any]:
        import asyncio
        await asyncio.sleep(0.05)  # 模拟网络延迟
        return {
            "qmt_order_id": f"mock-{uuid.uuid4().hex[:8]}",
            "status": "ACK",
            "reason": None,
        }

    async def cancel_order(self, qmt_order_id: str) -> bool:
        import asyncio
        await asyncio.sleep(0.02)
        return True


# ---------------------------------------------------------------------------
# 订单执行器
# ---------------------------------------------------------------------------

class OrderExecutor:
    """
    订单执行器

    使用方式:
        executor = OrderExecutor(
            risk_checker=risk_checker,
            xtquant_adapter=xtquant_adapter,
            redis_client=redis_client,
            get_db_conn=get_db_conn,
        )
        order_id = await executor.place_order(
            ts_code="000001.SZ",
            price=10.5,
            volume=1000,
            direction="BUY",
            daily_filled_amount=200_000,
            daily_order_count=10,
        )
    """

    def __init__(
        self,
        risk_checker: RiskChecker,
        xtquant_adapter: XtquantAdapter | None = None,
        redis_client=None,
        get_db_conn: Callable | None = None,
    ):
        """
        Args:
            risk_checker: 风控检查器
            xtquant_adapter: xtquant 适配器 (默认 MockXtquantAdapter)
            redis_client: RedisClient 实例
            get_db_conn: 获取 asyncpg 数据库连接的 callable (无参 → Connection)
        """
        self._risk_checker = risk_checker
        self._xtquant = xtquant_adapter or MockXtquantAdapter()
        self._redis = redis_client
        self._get_db_conn = get_db_conn

    # ------------------------------------------------------------------
    # 下单流程 (Ubuntu quant-engine 侧)
    # ------------------------------------------------------------------

    async def place_order(
        self,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        order_type: str = "LIMIT",
        daily_filled_amount: float = 0.0,
        daily_order_count: int = 0,
    ) -> str:
        """
        执行下单流程

        1. 风控检查
        2. 生成订单 ID，创建状态机 (PENDING)
        3. 持久化到 PostgreSQL orders 表
        4. 状态转换 PENDING → SENT
        5. 写入 Redis Stream trade_orders

        Returns:
            order_id (UUID)

        Raises:
            RuntimeError: 风控拦截
        """
        # 1. 风控检查
        result = self._risk_checker.check(
            ts_code=ts_code,
            price=price,
            volume=volume,
            direction=direction,
            daily_filled_amount=daily_filled_amount,
            daily_order_count=daily_order_count,
        )

        order_id = str(uuid.uuid4())

        if not result.passed:
            logger.warning(f"风控拦截 (订单 {order_id}): {result.reason}")
            await self._persist_order(
                order_id=order_id,
                ts_code=ts_code,
                price=price,
                volume=volume,
                direction=direction,
                order_type=order_type,
                status=OrderStatus.REJECTED,
                reject_reason=result.reason,
            )
            raise RuntimeError(f"风控拦截: {result.reason}")

        # 2. 创建状态机 (PENDING)
        from quant_engine.order.state_machine import OrderStateMachine
        sm = OrderStateMachine(OrderStatus.PENDING, order_id=order_id)

        # 3. 持久化 (PENDING)
        await self._persist_order(
            order_id=order_id,
            ts_code=ts_code,
            price=price,
            volume=volume,
            direction=direction,
            order_type=order_type,
            status=OrderStatus.PENDING,
        )

        # 4. 状态转换 PENDING → SENT
        sm.transition(OrderStatus.SENT)

        # 5. 更新数据库状态
        await self._update_order_status(order_id, OrderStatus.SENT)

        # 6. 写入 Redis Stream
        await self._publish_to_stream(
            order_id=order_id,
            ts_code=ts_code,
            price=price,
            volume=volume,
            direction=direction,
            order_type=order_type,
        )

        logger.info(f"订单已发送到 Redis Stream: {order_id}")
        return order_id

    # ------------------------------------------------------------------
    # 撤单流程
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> bool:
        """
        撤单

        1. 查询当前订单状态
        2. 校验是否可撤 (PENDING / SENT / ACK / PARTIAL)
        3. 状态转换 → CANCELLED
        4. 推送撤单指令到 Redis Stream

        Returns:
            True 撤单指令已发送
        """
        conn = await self._get_conn()

        row = await conn.fetchrow(
            "SELECT status, qmt_order_id FROM orders WHERE id = $1",
            order_id,
        )
        if row is None:
            raise ValueError(f"订单不存在: {order_id}")

        current_status = row["status"]
        qmt_order_id = row["qmt_order_id"]

        # 检查是否可撤
        from quant_engine.order.state_machine import (
            OrderStateMachine,
            OrderTransitionError,
        )
        sm = OrderStateMachine(OrderStatus(current_status), order_id=order_id)
        try:
            sm.transition(OrderStatus.CANCELLED)
        except OrderTransitionError as e:
            logger.warning(f"撤单失败: {e}")
            raise

        # 更新数据库状态
        await self._update_order_status(order_id, OrderStatus.CANCELLED)

        # 推送撤单指令到 Redis Stream
        if self._redis:
            await self._redis.xadd(
                TRADE_ORDERS_STREAM,
                data={
                    "action": "cancel",
                    "order_id": order_id,
                    "qmt_order_id": qmt_order_id or "",
                },
            )
            logger.info(f"撤单指令已发送: {order_id}")

        return True

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    async def _persist_order(
        self,
        order_id: str,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        order_type: str,
        status: OrderStatus,
        reject_reason: str | None = None,
    ) -> None:
        """持久化订单到 PostgreSQL"""
        conn = await self._get_conn()
        await conn.execute(
            """
            INSERT INTO orders (
                id, ts_code, price, volume, direction, order_type,
                status, qmt_order_id, retry_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            order_id,
            ts_code,
            price,
            volume,
            direction,
            order_type,
            status.value,
            None,
            0,
        )

    async def _update_order_status(
        self,
        order_id: str,
        status: OrderStatus,
        qmt_order_id: str | None = None,
    ) -> None:
        """更新订单状态"""
        conn = await self._get_conn()
        if qmt_order_id:
            await conn.execute(
                """
                UPDATE orders
                SET status = $1, qmt_order_id = $2, updated_at = now()
                WHERE id = $3
                """,
                status.value,
                qmt_order_id,
                order_id,
            )
        else:
            await conn.execute(
                """
                UPDATE orders
                SET status = $1, updated_at = now()
                WHERE id = $2
                """,
                status.value,
                order_id,
            )

    async def _publish_to_stream(
        self,
        order_id: str,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        order_type: str,
    ) -> None:
        """写入 Redis Stream"""
        if self._redis is None:
            logger.warning("无 Redis 客户端，订单未写入 Stream")
            return

        await self._redis.xadd(
            TRADE_ORDERS_STREAM,
            data={
                "action": "place",
                "order_id": order_id,
                "ts_code": ts_code,
                "price": str(price),
                "volume": str(volume),
                "direction": direction,
                "order_type": order_type,
            },
            maxlen=50000,
        )

    async def _get_conn(self):
        """获取数据库连接"""
        if self._get_db_conn is None:
            raise RuntimeError("未配置数据库连接")
        return await self._get_db_conn()
