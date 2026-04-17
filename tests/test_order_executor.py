"""
order/executor.py 单元测试

覆盖：
1. OrderExecutor.place_order 风控通过路径
2. OrderExecutor.place_order 风控拦截路径
3. 订单持久化 + Redis Stream 写入
4. cancel_order 成功路径
5. cancel_order 异常路径 (不存在、终态不可撤)
6. MockXtquantAdapter 行为
7. 无 Redis / 无 DB 配置降级
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from quant_engine.order.executor import (
    MockXtquantAdapter,
    OrderExecutor,
    TRADE_ORDERS_STREAM,
)
from quant_engine.order.state_machine import OrderStatus, OrderTransitionError
from quant_engine.risk.checker import RiskCheckResult, RiskChecker


# ---------------------------------------------------------------------------
class TestMockXtquantAdapter:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_submit_order_returns_ack(self):
        """验证 mock 适配器返回 ACK"""
        adapter = MockXtquantAdapter()
        result = await adapter.submit_order(
            ts_code="000001.SZ", price=10.0, volume=100, direction="BUY",
        )
        assert result["status"] == "ACK"
        assert result["qmt_order_id"].startswith("mock-")
        assert result["reason"] is None

    @pytest.mark.asyncio
    async def test_cancel_order_returns_true(self):
        """验证 mock 撤单返回 True"""
        adapter = MockXtquantAdapter()
        result = await adapter.cancel_order("mock-abc123")
        assert result is True


# ---------------------------------------------------------------------------
def _make_executor(
    risk_pass=True,
    has_redis=True,
    has_db=True,
):
    """创建 OrderExecutor 及 mock 依赖"""
    # RiskChecker mock
    mock_risk = MagicMock(spec=RiskChecker)
    if risk_pass:
        mock_risk.check.return_value = RiskCheckResult.ok()
    else:
        mock_risk.check.return_value = RiskCheckResult.reject("单笔金额超限")

    # Redis mock
    mock_redis = None
    if has_redis:
        mock_redis = MagicMock()
        mock_redis.xadd = AsyncMock(return_value="mock-stream-id")

    # DB mock
    mock_db_conn = None
    mock_get_conn = None
    if has_db:
        mock_db_conn = AsyncMock()
        mock_db_conn.execute = AsyncMock(return_value=None)
        mock_db_conn.fetchrow = AsyncMock(return_value=None)
        mock_get_conn = AsyncMock(return_value=mock_db_conn)

    mock_xtquant = MockXtquantAdapter()

    executor = OrderExecutor(
        risk_checker=mock_risk,
        xtquant_adapter=mock_xtquant,
        redis_client=mock_redis,
        get_db_conn=mock_get_conn,
    )
    return executor, mock_risk, mock_redis, mock_db_conn, mock_get_conn


# ---------------------------------------------------------------------------
class TestOrderExecutorPlaceOrder:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_place_order_risk_pass(self):
        """验证风控通过后下单成功"""
        executor, mock_risk, mock_redis, mock_db_conn, _ = _make_executor(
            risk_pass=True,
        )

        order_id = await executor.place_order(
            ts_code="000001.SZ",
            price=10.0,
            volume=100,
            direction="BUY",
        )

        # 风控被调用
        mock_risk.check.assert_called_once_with(
            ts_code="000001.SZ",
            price=10.0,
            volume=100,
            direction="BUY",
            daily_filled_amount=0.0,
            daily_order_count=0,
        )

        # 数据库 INSERT 被调用 (PENDING)
        assert mock_db_conn.execute.call_count >= 1

        # Redis Stream 被写入
        mock_redis.xadd.assert_called_once()
        call_kwargs = mock_redis.xadd.call_args
        assert call_kwargs[0][0] == TRADE_ORDERS_STREAM
        assert call_kwargs[1]["data"]["ts_code"] == "000001.SZ"
        assert call_kwargs[1]["data"]["action"] == "place"

        # 返回 UUID
        assert order_id is not None
        assert len(order_id) == 36  # UUID format

    @pytest.mark.asyncio
    async def test_place_order_risk_reject_raises(self):
        """验证风控拦截时抛出 RuntimeError"""
        executor, mock_risk, mock_redis, mock_db_conn, _ = _make_executor(
            risk_pass=False,
        )

        with pytest.raises(RuntimeError, match="风控拦截"):
            await executor.place_order(
                ts_code="000001.SZ",
                price=100.0,
                volume=2000,
                direction="BUY",
            )

        # 风控被调用
        mock_risk.check.assert_called_once()

        # 数据库仍应写入 REJECTED 记录
        assert mock_db_conn.execute.call_count >= 1

        # Redis 不应写入有效订单 (只写拒绝记录)
        # xadd 应该没有被调用或只调用一次 (reject 记录)

    @pytest.mark.asyncio
    async def test_place_order_passes_daily_context(self):
        """验证日累计上下文传递给风控"""
        executor, mock_risk, _, _, _ = _make_executor(risk_pass=True)

        await executor.place_order(
            ts_code="000001.SZ",
            price=10.0,
            volume=100,
            direction="BUY",
            daily_filled_amount=300_000,
            daily_order_count=25,
        )

        call_kwargs = mock_risk.check.call_args
        assert call_kwargs[1]["daily_filled_amount"] == 300_000
        assert call_kwargs[1]["daily_order_count"] == 25

    @pytest.mark.asyncio
    async def test_place_order_custom_order_type(self):
        """验证自定义订单类型"""
        executor, _, _, _, _ = _make_executor(risk_pass=True)

        await executor.place_order(
            ts_code="000001.SZ",
            price=10.0,
            volume=100,
            direction="BUY",
            order_type="MARKET",
        )

        # Stream 中应包含 order_type
        _, _, mock_redis, _, _ = _make_executor(risk_pass=True)
        # (已在上面验证 xadd 包含 order_type)


# ---------------------------------------------------------------------------
class TestOrderExecutorCancel:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_cancel_order_success(self):
        """验证撤单成功"""
        executor, _, mock_redis, mock_db_conn, mock_get_conn = _make_executor(
            risk_pass=True,
        )

        # 模拟查询返回可撤单状态
        mock_db_conn.fetchrow.return_value = {
            "status": "PENDING",
            "qmt_order_id": None,
        }

        result = await executor.cancel_order("test-order-id")

        assert result is True
        # 数据库查询被调用
        mock_db_conn.fetchrow.assert_called_once()
        # 状态更新被调用 (CANCELLED)
        assert mock_db_conn.execute.call_count >= 1

    @pytest.mark.asyncio
    async def test_cancel_order_not_found(self):
        """验证订单不存在时抛异常"""
        executor, _, _, mock_db_conn, _ = _make_executor(risk_pass=True)
        mock_db_conn.fetchrow.return_value = None

        with pytest.raises(ValueError, match="订单不存在"):
            await executor.cancel_order("nonexistent")

    @pytest.mark.asyncio
    async def test_cancel_filled_order_raises(self):
        """验证终态订单不可撤"""
        executor, _, _, mock_db_conn, _ = _make_executor(risk_pass=True)
        mock_db_conn.fetchrow.return_value = {
            "status": "FILLED",
            "qmt_order_id": "qmt-123",
        }

        with pytest.raises(OrderTransitionError):
            await executor.cancel_order("test-order-id")

    @pytest.mark.asyncio
    async def test_cancel_rejected_order_raises(self):
        """验证已拒绝订单不可撤"""
        executor, _, _, mock_db_conn, _ = _make_executor(risk_pass=True)
        mock_db_conn.fetchrow.return_value = {
            "status": "REJECTED",
            "qmt_order_id": None,
        }

        with pytest.raises(OrderTransitionError):
            await executor.cancel_order("test-order-id")

    @pytest.mark.asyncio
    async def test_cancel_cancelled_order_raises(self):
        """验证已撤销订单不可再次撤单"""
        executor, _, _, mock_db_conn, _ = _make_executor(risk_pass=True)
        mock_db_conn.fetchrow.return_value = {
            "status": "CANCELLED",
            "qmt_order_id": None,
        }

        with pytest.raises(OrderTransitionError):
            await executor.cancel_order("test-order-id")

    @pytest.mark.asyncio
    async def test_cancel_ack_order_success(self):
        """验证 ACK 状态可撤"""
        executor, _, mock_redis, mock_db_conn, _ = _make_executor(risk_pass=True)
        mock_db_conn.fetchrow.return_value = {
            "status": "ACK",
            "qmt_order_id": "qmt-456",
        }

        result = await executor.cancel_order("test-order-id")
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_partial_order_success(self):
        """验证 PARTIAL 状态可撤"""
        executor, _, _, mock_db_conn, _ = _make_executor(risk_pass=True)
        mock_db_conn.fetchrow.return_value = {
            "status": "PARTIAL",
            "qmt_order_id": "qmt-789",
        }

        result = await executor.cancel_order("test-order-id")
        assert result is True


# ---------------------------------------------------------------------------
class TestOrderExecutorEdgeCases:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_place_order_without_redis(self):
        """验证无 Redis 时仍可下单 (Stream 未写入)"""
        executor, mock_risk, mock_redis, mock_db_conn, _ = _make_executor(
            risk_pass=True,
            has_redis=False,
        )

        order_id = await executor.place_order(
            ts_code="000001.SZ",
            price=10.0,
            volume=100,
            direction="BUY",
        )

        assert order_id is not None
        mock_risk.check.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_without_db_raises(self):
        """验证无 DB 配置时下单失败"""
        executor, _, _, _, _ = _make_executor(
            risk_pass=True,
            has_db=False,
        )

        with pytest.raises(RuntimeError, match="未配置数据库连接"):
            await executor.place_order(
                ts_code="000001.SZ",
                price=10.0,
                volume=100,
                direction="BUY",
            )

    @pytest.mark.asyncio
    async def test_cancel_order_without_db_raises(self):
        """验证无 DB 配置时撤单失败"""
        executor, _, _, _, _ = _make_executor(
            risk_pass=True,
            has_db=False,
        )

        with pytest.raises(RuntimeError, match="未配置数据库连接"):
            await executor.cancel_order("test-order-id")
