"""
order/state_machine.py 单元测试

覆盖：
1. OrderStatus 枚举值
2. 合法状态转换 (所有正向路径)
3. 非法状态转换 (终态不可转换、跳跃转换)
4. OrderStateMachine 生命周期
5. is_terminal 终态判断
6. can_transition_to / allowed_next
"""
import pytest

from quant_engine.order.state_machine import (
    OrderStatus,
    OrderStateMachine,
    OrderTransitionError,
    VALID_TRANSITIONS,
)


# ---------------------------------------------------------------------------
class TestOrderStatus:
# ---------------------------------------------------------------------------

    def test_all_status_exist(self):
        """验证全部 7 个状态枚举存在"""
        expected = {"PENDING", "SENT", "ACK", "FILLED", "PARTIAL", "REJECTED", "CANCELLED"}
        actual = {s.value for s in OrderStatus}
        assert actual == expected

    def test_status_is_string(self):
        """验证 OrderStatus 继承 str (可序列化)"""
        assert isinstance(OrderStatus.PENDING, str)
        assert OrderStatus.PENDING.value == "PENDING"


# ---------------------------------------------------------------------------
class TestValidTransitions:
# ---------------------------------------------------------------------------

    def test_terminal_states_have_no_transitions(self):
        """验证终态不允许任何转换"""
        for status in (OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED):
            assert VALID_TRANSITIONS[status] == frozenset()

    def test_pending_allows_sent_rejected_cancelled(self):
        """验证 PENDING 允许 SENT / REJECTED / CANCELLED"""
        allowed = VALID_TRANSITIONS[OrderStatus.PENDING]
        assert OrderStatus.SENT in allowed
        assert OrderStatus.REJECTED in allowed
        assert OrderStatus.CANCELLED in allowed
        assert OrderStatus.FILLED not in allowed  # 不能直接到 FILLED

    def test_sent_allows_ack_rejected_cancelled(self):
        """验证 SENT 允许 ACK / REJECTED / CANCELLED"""
        allowed = VALID_TRANSITIONS[OrderStatus.SENT]
        assert OrderStatus.ACK in allowed
        assert OrderStatus.REJECTED in allowed
        assert OrderStatus.CANCELLED in allowed

    def test_ack_allows_filled_partial_cancelled(self):
        """验证 ACK 允许 FILLED / PARTIAL / CANCELLED"""
        allowed = VALID_TRANSITIONS[OrderStatus.ACK]
        assert OrderStatus.FILLED in allowed
        assert OrderStatus.PARTIAL in allowed
        assert OrderStatus.CANCELLED in allowed

    def test_partial_allows_filled_cancelled(self):
        """验证 PARTIAL 允许 FILLED / CANCELLED"""
        allowed = VALID_TRANSITIONS[OrderStatus.PARTIAL]
        assert OrderStatus.FILLED in allowed
        assert OrderStatus.CANCELLED in allowed


# ---------------------------------------------------------------------------
class TestOrderStateMachineValid:
# ---------------------------------------------------------------------------

    def test_happy_path(self):
        """验证正常路径: PENDING → SENT → ACK → FILLED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        assert sm.transition(OrderStatus.SENT) == OrderStatus.SENT
        assert sm.transition(OrderStatus.ACK) == OrderStatus.ACK
        assert sm.transition(OrderStatus.FILLED) == OrderStatus.FILLED

    def test_rejected_from_pending(self):
        """验证风控拦截: PENDING → REJECTED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.REJECTED)
        assert sm.current == OrderStatus.REJECTED

    def test_rejected_from_sent(self):
        """验证券商拒绝: SENT → REJECTED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.REJECTED)

    def test_cancel_from_pending(self):
        """验证主动撤单: PENDING → CANCELLED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.CANCELLED)
        assert sm.current == OrderStatus.CANCELLED

    def test_partial_then_filled(self):
        """验证部分成交后全部成交: ACK → PARTIAL → FILLED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.PARTIAL)
        sm.transition(OrderStatus.FILLED)

    def test_partial_then_cancelled(self):
        """验证部分成交后撤单: ACK → PARTIAL → CANCELLED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.PARTIAL)
        sm.transition(OrderStatus.CANCELLED)

    def test_cancel_from_ack(self):
        """验证已确认后撤单: ACK → CANCELLED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.CANCELLED)

    def test_cancel_from_sent(self):
        """验证已发送后撤单: SENT → CANCELLED"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.CANCELLED)


# ---------------------------------------------------------------------------
class TestOrderStateMachineInvalid:
# ---------------------------------------------------------------------------

    def test_terminal_state_no_transition(self):
        """验证终态不可转换"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.FILLED)

        with pytest.raises(OrderTransitionError, match="FILLED"):
            sm.transition(OrderStatus.PENDING)

    def test_skip_states_raises(self):
        """验证跳跃转换: PENDING → ACK (非法)"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        with pytest.raises(OrderTransitionError, match="PENDING.*ACK"):
            sm.transition(OrderStatus.ACK)

    def test_pending_to_filled_raises(self):
        """验证 PENDING → FILLED (非法)"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.FILLED)

    def test_sent_to_filled_raises(self):
        """验证 SENT → FILLED (非法，需先经过 ACK)"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.FILLED)

    def test_rejected_is_terminal(self):
        """验证 REJECTED 后不可转换"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.REJECTED)
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.PENDING)

    def test_cancelled_is_terminal(self):
        """验证 CANCELLED 后不可转换"""
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.CANCELLED)
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.SENT)


# ---------------------------------------------------------------------------
class TestOrderStateMachineProperties:
# ---------------------------------------------------------------------------

    def test_is_terminal_true_for_filled(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.FILLED)
        assert sm.is_terminal is True

    def test_is_terminal_false_for_pending(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        assert sm.is_terminal is False

    def test_is_terminal_false_for_sent(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        assert sm.is_terminal is False

    def test_can_transition_to(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        assert sm.can_transition_to(OrderStatus.SENT) is True
        assert sm.can_transition_to(OrderStatus.FILLED) is False

    def test_allowed_next(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        allowed = sm.allowed_next()
        assert OrderStatus.SENT in allowed
        assert OrderStatus.REJECTED in allowed
        assert OrderStatus.CANCELLED in allowed
        assert OrderStatus.FILLED not in allowed

    def test_allowed_next_empty_for_terminal(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.FILLED)
        assert sm.allowed_next() == frozenset()


# ---------------------------------------------------------------------------
class TestOrderStateMachineWithOrderId:
# ---------------------------------------------------------------------------

    def test_error_message_includes_order_id(self):
        """验证异常信息包含订单 ID"""
        sm = OrderStateMachine(OrderStatus.PENDING, order_id="ord-123")
        with pytest.raises(OrderTransitionError) as exc_info:
            sm.transition(OrderStatus.FILLED)

        assert "ord-123" in str(exc_info.value)

    def test_repr_includes_order_id(self):
        sm = OrderStateMachine(OrderStatus.PENDING, order_id="ord-456")
        assert "ord-456" in repr(sm)

    def test_repr_without_order_id(self):
        sm = OrderStateMachine(OrderStatus.PENDING)
        assert "order_id=None" in repr(sm)
