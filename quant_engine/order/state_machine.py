"""
订单状态机

职责：
1. 定义订单状态枚举 (OrderStatus)
2. 定义显式状态转换矩阵 (VALID_TRANSITIONS)
3. 提供 OrderStateMachine 类，校验并执行状态转换

状态流:
  PENDING → SENT → ACK → FILLED
                  ↳ PARTIAL → FILLED
          ↳ REJECTED
          ↳ CANCELLED

设计原则：
- 枚举类 + 显式转换矩阵，拒绝隐式字符串比较
- 非法转换抛出 OrderTransitionError
- 每次转换记录日志，便于死因追溯
"""
import logging
from enum import Enum

logger = logging.getLogger("order_state_machine")


class OrderStatus(str, Enum):
    """订单状态枚举"""
    PENDING = "PENDING"       # 订单已创建，待发送
    SENT = "SENT"             # 已发送给 xtquant，待券商确认
    ACK = "ACK"               # 券商已确认委托
    FILLED = "FILLED"         # 全部成交
    PARTIAL = "PARTIAL"       # 部分成交
    REJECTED = "REJECTED"     # 被拒绝 (风控拦截或券商拒绝)
    CANCELLED = "CANCELLED"   # 已撤销


class OrderTransitionError(ValueError):
    """非法状态转换异常"""

    def __init__(self, from_status: OrderStatus, to_status: OrderStatus, order_id: str | None = None):
        self.from_status = from_status
        self.to_status = to_status
        self.order_id = order_id
        msg = f"非法状态转换: {from_status.value} → {to_status.value}"
        if order_id:
            msg += f" (订单 {order_id})"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# 显式转换矩阵: 每个状态允许的下一个状态
# ---------------------------------------------------------------------------

VALID_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({
        OrderStatus.SENT,        # 正常发送
        OrderStatus.REJECTED,    # 风控拦截
        OrderStatus.CANCELLED,   # 用户主动撤单
    }),
    OrderStatus.SENT: frozenset({
        OrderStatus.ACK,         # 券商确认
        OrderStatus.REJECTED,    # 券商拒绝
        OrderStatus.CANCELLED,   # 撤单成功
    }),
    OrderStatus.ACK: frozenset({
        OrderStatus.FILLED,      # 全部成交
        OrderStatus.PARTIAL,     # 部分成交
        OrderStatus.CANCELLED,   # 撤单成功
    }),
    OrderStatus.PARTIAL: frozenset({
        OrderStatus.FILLED,      # 剩余部分成交完毕
        OrderStatus.CANCELLED,   # 撤单 (未成交部分)
    }),
    # 终态：不允许任何转换
    OrderStatus.FILLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
}


class OrderStateMachine:
    """
    订单状态机

    使用方式:
        sm = OrderStateMachine(OrderStatus.PENDING, order_id="xxx")
        sm.transition(OrderStatus.SENT)   # PENDING → SENT ✓
        sm.transition(OrderStatus.FILLED) # SENT → FILLED ✗ (raises OrderTransitionError)
    """

    def __init__(self, initial_status: OrderStatus, order_id: str | None = None):
        self._current = initial_status
        self._order_id = order_id

    @property
    def current(self) -> OrderStatus:
        return self._current

    @property
    def is_terminal(self) -> bool:
        """是否处于终态 (FILLED / REJECTED / CANCELLED)"""
        return self._current in (
            OrderStatus.FILLED,
            OrderStatus.REJECTED,
            OrderStatus.CANCELLED,
        )

    def can_transition_to(self, target: OrderStatus) -> bool:
        """检查是否允许转换到目标状态"""
        return target in VALID_TRANSITIONS.get(self._current, frozenset())

    def allowed_next(self) -> frozenset[OrderStatus]:
        """返回当前状态允许的所有下一个状态"""
        return VALID_TRANSITIONS.get(self._current, frozenset())

    def transition(self, target: OrderStatus) -> OrderStatus:
        """
        执行状态转换

        Args:
            target: 目标状态

        Returns:
            转换后的状态

        Raises:
            OrderTransitionError: 非法状态转换
        """
        if not self.can_transition_to(target):
            raise OrderTransitionError(self._current, target, self._order_id)

        logger.info(
            f"订单状态转换: {self._current.value} → {target.value}"
            f"{f' (订单 {self._order_id})' if self._order_id else ''}"
        )
        self._current = target
        return self._current

    def __repr__(self) -> str:
        return f"OrderStateMachine({self._current.value}, order_id={self._order_id!r})"
