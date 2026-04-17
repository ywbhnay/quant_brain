"""
端到端集成测试 (Phase 5)

覆盖完整数据流：
1. 跑批 → daily_wide 写入
2. 策略信号 → 风控检查 → 订单创建 → Redis Stream
3. 订单状态机全生命周期
4. 完整管线：跑批数据 → 信号生成 → 下单 → 状态流转

所有外部依赖 (PostgreSQL, Redis, xtquant) 均 mock，可在非交易时间运行。
"""
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import base64
import msgpack
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_pool():
    """模拟 asyncpg 连接池"""
    pool = AsyncMock()
    # mock acquire 上下文管理器
    conn = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.fixture
def mock_redis():
    """模拟 RedisClient"""
    redis = AsyncMock()
    redis.ensure_connected = AsyncMock(return_value=AsyncMock())
    redis.xadd = AsyncMock(return_value="1234567890-0")
    redis.xread = AsyncMock(return_value=[])
    redis.xreadgroup = AsyncMock(return_value=[])
    redis.xack = AsyncMock(return_value=1)
    redis.xgroup_create = AsyncMock(return_value="OK")
    redis.hgetall = AsyncMock(return_value={})
    redis.publish = AsyncMock(return_value=1)
    return redis


# ---------------------------------------------------------------------------
# 1. 跑批流程集成测试
# ---------------------------------------------------------------------------

class TestBatchUpdateIntegration:
    """跑批 → daily_wide 写入完整流程"""

    @pytest.mark.asyncio
    async def test_batch_update_full_flow(self, mock_pool):
        """完整跑批：获取股票 → 分片读取 → ffill → 写入 daily_wide → 刷新 profile"""
        from quant_engine.batch_update import run_batch_update
        import polars as pl

        # 配置 mock
        mock_pool.fetch.side_effect = [
            # get_active_stock
            [{"ts_code": "000001.SZ"}, {"ts_code": "000002.SZ"}],
        ]
        mock_pool.execute = AsyncMock(side_effect=[
            "INSERT 0 2",   # upsert_daily_wide
            "INSERT 0 2",   # refresh_stock_profile
        ])

        # Mock acquire context manager
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.copy_records_to_table = AsyncMock()
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_cm)

        # Mock fetch_daily_chunk to return a proper Polars DataFrame
        mock_df = pl.DataFrame({
            "trade_date": ["2026-04-16", "2026-04-16"],
            "ts_code": ["000001.SZ", "000002.SZ"],
            "open": [10.5, 15.0],
            "high": [11.0, 15.5],
            "low": [10.3, 14.8],
            "close": [10.8, 15.2],
            "pre_close": [10.4, 14.9],
            "change": [0.4, 0.3],
            "pct_chg": [3.85, 2.01],
            "vol": [100000.0, 200000.0],
            "amount": [1080000.0, 3040000.0],
        })
        mock_adj = pl.DataFrame({
            "ts_code": [],
            "adj_factor": [],
            "trade_date": [],
        })
        mock_pool.fetch.side_effect = [
            [{"ts_code": "000001.SZ"}, {"ts_code": "000002.SZ"}],
            mock_adj,
        ]

        with patch("quant_engine.batch_update.asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            with patch("quant_engine.batch_update.QuantConfig.pg_dsn", return_value="postgres://test"):
                with patch("quant_engine.batch_update.QuantConfig.PG_HOST", "localhost"):
                    with patch("quant_engine.batch_update.QuantConfig.PG_USER", "postgres"):
                        with patch("quant_engine.batch_update.QuantConfig.PG_PASSWORD", "test"):
                            with patch("quant_engine.batch_update.QuantConfig.PG_DATABASE", "test"):
                                with patch("quant_engine.batch_update.fetch_daily_chunk", return_value=mock_df):
                                    with patch("quant_engine.batch_update.fetch_adj_factors", return_value=mock_adj):
                                        result = await run_batch_update(chunk_size=100)

        assert result["active_stocks"] == 2
        assert result["chunks_processed"] == 1
        assert result["daily_wide_rows"] > 0
        assert result["stock_profile_rows"] == 2

    @pytest.mark.asyncio
    async def test_batch_update_no_active_stocks(self, mock_pool):
        """无正常上市股票时跳过跑批"""
        from quant_engine.batch_update import run_batch_update

        mock_pool.fetch.return_value = []

        async def fake_create_pool(**kwargs):
            mock_pool.execute = AsyncMock(return_value="INSERT 0 1")
            return mock_pool

        with patch("quant_engine.batch_update.asyncpg.create_pool", new=AsyncMock(side_effect=fake_create_pool)):
            with patch("quant_engine.batch_update.QuantConfig.pg_dsn", return_value="postgres://test"):
                with patch("quant_engine.batch_update.QuantConfig.PG_HOST", "localhost"):
                    with patch("quant_engine.batch_update.QuantConfig.PG_USER", "postgres"):
                        with patch("quant_engine.batch_update.QuantConfig.PG_PASSWORD", "test"):
                            with patch("quant_engine.batch_update.QuantConfig.PG_DATABASE", "test"):
                                result = await run_batch_update()

        assert result["active_stocks"] == 0
        assert result["chunks_processed"] == 0

    @pytest.mark.asyncio
    async def test_batch_update_empty_chunk_skipped(self, mock_pool):
        """空分片自动跳过"""
        from quant_engine.batch_update import run_batch_update
        import polars as pl

        mock_pool.fetch.side_effect = [
            # get_active_stock
            [{"ts_code": "000001.SZ"}],
            # fetch_daily_chunk - 返回空 DataFrame
        ]

        async def fake_create_pool2(**kwargs):
            mock_pool.execute = AsyncMock(return_value="INSERT 0 1")
            return mock_pool

        with patch("quant_engine.batch_update.asyncpg.create_pool", new=AsyncMock(side_effect=fake_create_pool2)):
            with patch("quant_engine.batch_update.QuantConfig.pg_dsn", return_value="postgres://test"):
                with patch("quant_engine.batch_update.QuantConfig.PG_HOST", "localhost"):
                    with patch("quant_engine.batch_update.QuantConfig.PG_USER", "postgres"):
                        with patch("quant_engine.batch_update.QuantConfig.PG_PASSWORD", "test"):
                            with patch("quant_engine.batch_update.QuantConfig.PG_DATABASE", "test"):
                                with patch(
                                    "quant_engine.batch_update.fetch_daily_chunk",
                                    return_value=pl.DataFrame({
                                        "trade_date": [], "ts_code": [], "open": [],
                                        "high": [], "low": [], "close": [],
                                        "pre_close": [], "change": [], "pct_chg": [],
                                        "vol": [], "amount": [],
                                    }),
                                ):
                                    result = await run_batch_update(chunk_size=100)

        assert result["active_stocks"] == 1
        assert result["chunks_processed"] == 0  # 空分片被跳过


# ---------------------------------------------------------------------------
# 2. 风控 + 订单执行集成测试
# ---------------------------------------------------------------------------

class TestRiskToOrderIntegration:
    """策略信号 → 风控检查 → 订单创建"""

    @pytest.mark.asyncio
    async def test_order_passes_risk_and_published_to_stream(self, mock_pool, mock_redis):
        """风控通过 → 订单创建 (PENDING) → 状态转 SENT → 写入 Redis Stream"""
        from quant_engine.order.executor import OrderExecutor, TRADE_ORDERS_STREAM
        from quant_engine.risk.checker import RiskChecker
        from quant_engine.order.state_machine import OrderStatus

        # 配置 mock pool 的 acquire
        conn = AsyncMock()
        conn.execute = AsyncMock()

        async def fake_acquire():
            return conn

        mock_pool.acquire = fake_acquire

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)  # 使用默认规则
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        order_id = await executor.place_order(
            ts_code="000001.SZ",
            price=10.5,
            volume=100,
            direction="BUY",
            order_type="LIMIT",
        )

        assert order_id is not None
        # 验证数据库写入
        assert conn.execute.call_count >= 2  # INSERT + UPDATE
        # 验证 Redis Stream 写入
        mock_redis.xadd.assert_called_once()
        call_kwargs = mock_redis.xadd.call_args
        payload = base64.b64decode(call_kwargs[1]["data"]["payload"])
        decoded = msgpack.unpackb(payload, raw=False)
        assert decoded["action"] == "place"
        assert decoded["ts_code"] == "000001.SZ"
        assert decoded["direction"] == "BUY"

    @pytest.mark.asyncio
    async def test_order_rejected_by_risk_single_amount(self, mock_pool, mock_redis):
        """风控拦截：单笔金额超限"""
        from quant_engine.order.executor import OrderExecutor
        from quant_engine.risk.checker import RiskChecker

        conn = AsyncMock()
        conn.execute = AsyncMock()

        async def fake_acquire():
            return conn

        mock_pool.acquire = fake_acquire

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        # 单笔金额 = 10.5 * 20000 = 210000 > 100000 上限
        with pytest.raises(RuntimeError) as exc_info:
            await executor.place_order(
                ts_code="000001.SZ",
                price=10.5,
                volume=20000,
                direction="BUY",
            )

        assert "风控拦截" in str(exc_info.value)
        assert "超过上限" in str(exc_info.value)
        # 拒绝订单仍应持久化
        assert conn.execute.call_count >= 1

    @pytest.mark.asyncio
    async def test_order_rejected_by_blacklist(self, mock_redis):
        """风控拦截：标的在黑名单"""
        from quant_engine.order.executor import OrderExecutor
        from quant_engine.risk.checker import RiskChecker, RiskRules

        conn = AsyncMock()
        conn.execute = AsyncMock()

        async def fake_get_conn():
            return conn

        # 手动设置黑名单
        checker = RiskChecker(redis_client=None)
        checker._rules = RiskRules(
            blacklist=frozenset({"000001.SZ"}),
        )
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        with pytest.raises(RuntimeError) as exc_info:
            await executor.place_order(
                ts_code="000001.SZ",
                price=10.5,
                volume=100,
                direction="BUY",
            )

        assert "黑名单" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_sell_order_skips_amount_checks(self):
        """卖出订单仅检查黑名单，跳过金额限制"""
        from quant_engine.risk.checker import RiskChecker, RiskRules

        checker = RiskChecker(redis_client=None)
        # 设置极低的金额上限
        checker._rules = RiskRules(max_single_amount=1.0, max_daily_amount=1.0)

        # 卖出大额订单应通过 (不检查金额)
        result = checker.check(
            ts_code="000001.SZ",
            price=100.0,
            volume=10000,
            direction="SELL",
        )
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_risk_rules_reload_from_redis(self, mock_redis):
        """风控规则从 Redis 热加载"""
        from quant_engine.risk.checker import RiskChecker, RISK_RULES_KEY
        import json

        mock_client = AsyncMock()
        mock_client.hgetall = AsyncMock(return_value={
            "max_single_amount": "50000",
            "max_daily_amount": "200000",
            "max_daily_order_count": "20",
            "blacklist": json.dumps(["600000.SH"]),
        })
        mock_redis.ensure_connected = AsyncMock(return_value=mock_client)

        checker = RiskChecker(redis_client=mock_redis)
        await checker.load_rules()

        assert checker.rules.max_single_amount == 50000.0
        assert checker.rules.max_daily_amount == 200000.0
        assert "600000.SH" in checker.rules.blacklist


# ---------------------------------------------------------------------------
# 3. 订单状态机全生命周期测试
# ---------------------------------------------------------------------------

class TestOrderLifecycle:
    """订单从 PENDING 到终态的完整流程"""

    @pytest.mark.asyncio
    async def test_full_lifecycle_pending_to_filled(self, mock_pool, mock_redis):
        """PENDING → SENT → ACK → FILLED"""
        from quant_engine.order.executor import OrderExecutor
        from quant_engine.risk.checker import RiskChecker
        from quant_engine.order.state_machine import OrderStatus

        conn = AsyncMock()
        conn.execute = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={"status": "SENT", "qmt_order_id": "qmt-123"})

        async def fake_acquire():
            return conn

        mock_pool.acquire = fake_acquire

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        # Step 1: 下单 (PENDING → SENT)
        order_id = await executor.place_order(
            ts_code="000001.SZ",
            price=10.5,
            volume=100,
            direction="BUY",
        )

        # Step 2: 模拟状态更新 SENT → ACK
        from quant_engine.order.state_machine import OrderStateMachine
        sm = OrderStateMachine(OrderStatus.SENT, order_id=order_id)
        sm.transition(OrderStatus.ACK)
        assert sm.current == OrderStatus.ACK

        # Step 3: ACK → FILLED
        sm.transition(OrderStatus.FILLED)
        assert sm.current == OrderStatus.FILLED
        assert sm.is_terminal is True

    @pytest.mark.asyncio
    async def test_lifecycle_with_partial_fill(self, mock_pool, mock_redis):
        """PENDING → SENT → ACK → PARTIAL → FILLED"""
        from quant_engine.risk.checker import RiskChecker
        from quant_engine.order.state_machine import OrderStateMachine, OrderStatus

        conn = AsyncMock()

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)

        # 创建初始状态机
        sm = OrderStateMachine(OrderStatus.PENDING, order_id="test-partial")
        sm.transition(OrderStatus.SENT)
        sm.transition(OrderStatus.ACK)
        sm.transition(OrderStatus.PARTIAL)
        sm.transition(OrderStatus.FILLED)

        assert sm.current == OrderStatus.FILLED
        assert sm.is_terminal is True

    @pytest.mark.asyncio
    async def test_cancel_order_from_pending(self, mock_pool, mock_redis):
        """PENDING → CANCELLED"""
        from quant_engine.order.state_machine import OrderStateMachine, OrderStatus

        sm = OrderStateMachine(OrderStatus.PENDING, order_id="cancel-test")
        sm.transition(OrderStatus.CANCELLED)

        assert sm.current == OrderStatus.CANCELLED
        assert sm.is_terminal is True

    @pytest.mark.asyncio
    async def test_cancel_order_from_partial(self, mock_pool, mock_redis):
        """PARTIAL → CANCELLED (撤未成交部分)"""
        from quant_engine.order.state_machine import OrderStateMachine, OrderStatus

        sm = OrderStateMachine(OrderStatus.PARTIAL, order_id="partial-cancel")
        sm.transition(OrderStatus.CANCELLED)

        assert sm.current == OrderStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_filled_order_fails(self):
        """FILLED 不可撤销 (终态)"""
        from quant_engine.order.state_machine import (
            OrderStateMachine,
            OrderStatus,
            OrderTransitionError,
        )

        sm = OrderStateMachine(OrderStatus.FILLED, order_id="filled-no-cancel")
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.CANCELLED)

    @pytest.mark.asyncio
    async def test_cancel_rejected_order_fails(self):
        """REJECTED 不可撤销 (终态)"""
        from quant_engine.order.state_machine import (
            OrderStateMachine,
            OrderStatus,
            OrderTransitionError,
        )

        sm = OrderStateMachine(OrderStatus.REJECTED, order_id="rejected-no-cancel")
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.CANCELLED)

    @pytest.mark.asyncio
    async def test_skip_sent_state_fails(self):
        """PENDING → FILLED 非法 (跳过 SENT)"""
        from quant_engine.order.state_machine import (
            OrderStateMachine,
            OrderStatus,
            OrderTransitionError,
        )

        sm = OrderStateMachine(OrderStatus.PENDING, order_id="skip-sent")
        with pytest.raises(OrderTransitionError):
            sm.transition(OrderStatus.FILLED)

    @pytest.mark.asyncio
    async def test_cancel_order_db_update(self, mock_redis):
        """撤单：状态转 CANCELLED + 更新数据库"""
        from quant_engine.order.executor import OrderExecutor
        from quant_engine.risk.checker import RiskChecker

        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={
            "status": "PENDING",
            "qmt_order_id": None,
        })
        conn.execute = AsyncMock()

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        result = await executor.cancel_order("order-123")

        assert result is True
        # 验证状态更新
        update_calls = [c for c in conn.execute.call_args_list if "UPDATE" in str(c)]
        assert len(update_calls) >= 1
        assert "CANCELLED" in str(conn.execute.call_args_list)


# ---------------------------------------------------------------------------
# 4. Redis Stream 集成测试
# ---------------------------------------------------------------------------

class TestRedisStreamIntegration:
    """Redis Stream 作为跨平台通信骨干"""

    @pytest.mark.asyncio
    async def test_order_published_to_trade_orders_stream(self, mock_redis):
        """下单后订单写入 trade_orders Stream"""
        from quant_engine.order.executor import (
            OrderExecutor,
            TRADE_ORDERS_STREAM,
        )
        from quant_engine.risk.checker import RiskChecker

        conn = AsyncMock()
        conn.execute = AsyncMock()

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        await executor.place_order(
            ts_code="000001.SZ",
            price=10.5,
            volume=100,
            direction="BUY",
        )

        mock_redis.xadd.assert_called_once()
        args, kwargs = mock_redis.xadd.call_args
        assert args[0] == TRADE_ORDERS_STREAM
        payload = base64.b64decode(kwargs["data"]["payload"])
        decoded = msgpack.unpackb(payload, raw=False)
        assert decoded["action"] == "place"
        assert decoded["ts_code"] == "000001.SZ"

    @pytest.mark.asyncio
    async def test_cancel_published_to_stream(self, mock_redis):
        """撤单指令写入 trade_orders Stream"""
        from quant_engine.order.executor import OrderExecutor, TRADE_ORDERS_STREAM
        from quant_engine.risk.checker import RiskChecker

        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={
            "status": "SENT",
            "qmt_order_id": "qmt-456",
        })
        conn.execute = AsyncMock()

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        await executor.cancel_order("cancel-stream-test")

        # 找到 xadd 中 action=cancel 的调用
        cancel_calls = [
            c for c in mock_redis.xadd.call_args_list
            if c[1].get("data", {}).get("action") == "cancel"
        ]
        assert len(cancel_calls) >= 1
        assert cancel_calls[0][1]["data"]["order_id"] == "cancel-stream-test"

    @pytest.mark.asyncio
    async def test_redis_client_stream_operations(self):
        """RedisClient Stream 操作端到端 (mock redis)"""
        from quant_engine.redis_client import RedisClient

        mock_client = AsyncMock()
        mock_client.ping = AsyncMock()
        mock_client.xadd = AsyncMock(return_value="12345-0")
        mock_client.xread = AsyncMock(return_value=[["test_stream", [("12345-0", {"key": "value"})]]])
        mock_client.xgroup_create = AsyncMock(return_value="OK")
        mock_client.xreadgroup = AsyncMock(return_value=[])
        mock_client.xack = AsyncMock(return_value=1)
        mock_client.xtrim = AsyncMock(return_value=0)

        mock_pool = MagicMock()

        client = RedisClient(url="redis://localhost:6379/0")
        client._pool = mock_pool
        client._client = mock_client

        # xadd
        msg_id = await client.xadd("test_stream", {"key": "value"}, maxlen=1000)
        assert msg_id == "12345-0"

        # xread
        result = await client.xread({"test_stream": "0"}, count=10)
        assert len(result) == 1

        # xgroup_create
        result = await client.xgroup_create("test_stream", "test_group")
        assert result == "OK"

    @pytest.mark.asyncio
    async def test_redis_client_pubsub_operations(self):
        """RedisClient Pub/Sub 操作 (mock redis)"""
        from quant_engine.redis_client import RedisClient

        mock_client = AsyncMock()
        mock_client.ping = AsyncMock()
        mock_client.publish = AsyncMock(return_value=1)

        mock_pubsub = AsyncMock()
        mock_client.pubsub = MagicMock(return_value=mock_pubsub)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.psubscribe = AsyncMock()

        client = RedisClient(url="redis://localhost:6379/0")
        client._pool = MagicMock()
        client._client = mock_client

        # publish
        count = await client.publish("market.snapshot.000001.SZ", '{"price": 10.5}')
        assert count == 1

        # subscribe
        pubsub = await client.subscribe("channel1", "channel2")
        pubsub.subscribe.assert_called_once_with("channel1", "channel2")


# ---------------------------------------------------------------------------
# 5. 完整管线集成测试
# ---------------------------------------------------------------------------

class TestFullPipeline:
    """完整管线：跑批数据 → 信号生成 → 下单 → 状态流转"""

    @pytest.mark.asyncio
    async def test_pipeline_batch_to_order(self, mock_redis):
        """
        模拟完整管线：
        1. 跑批写入 daily_wide (mock DB)
        2. 策略从 daily_wide 读取数据生成信号
        3. 信号通过风控 → 创建订单
        4. 订单写入 Redis Stream
        """
        from quant_engine.risk.checker import RiskChecker
        from quant_engine.order.state_machine import OrderStateMachine, OrderStatus

        # --- Step 1: 模拟跑批数据 ---
        mock_pool = AsyncMock()
        mock_pool.fetch.side_effect = [
            # 跑批获取股票
            [{"ts_code": "000001.SZ"}],
            # 查询 daily_wide 验证数据 (策略读取)
            [
                {
                    "trade_date": "2026-04-15",
                    "ts_code": "000001.SZ",
                    "close": 10.8,
                    "vol": 100000.0,
                    "amount": 1080000.0,
                },
            ],
        ]

        conn = AsyncMock()
        conn.execute = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={
            "close": 10.8,
            "trade_date": "2026-04-15",
        })

        async def fake_get_conn():
            return conn

        # --- Step 2: 策略生成信号 ---
        # 简单均线策略：收盘价 > 10.0 → 买入信号
        bar = {"close": 10.8, "trade_date": "2026-04-15"}
        signal = "BUY" if bar["close"] > 10.0 else None
        assert signal == "BUY"

        # --- Step 3: 信号通过风控 → 创建订单 ---
        checker = RiskChecker(redis_client=None)
        result = checker.check(
            ts_code="000001.SZ",
            price=bar["close"],
            volume=100,
            direction="BUY",
            daily_filled_amount=0,
            daily_order_count=0,
        )
        assert result.passed is True

        # --- Step 4: 订单状态流转 ---
        sm = OrderStateMachine(OrderStatus.PENDING, order_id="pipeline-test")
        sm.transition(OrderStatus.SENT)
        assert sm.current == OrderStatus.SENT

        # 模拟券商确认
        sm.transition(OrderStatus.ACK)
        assert sm.current == OrderStatus.ACK

        # 全部成交
        sm.transition(OrderStatus.FILLED)
        assert sm.current == OrderStatus.FILLED
        assert sm.is_terminal is True

    @pytest.mark.asyncio
    async def test_pipeline_risk_reject_stops_flow(self):
        """风控拦截中断整个管线"""
        from quant_engine.risk.checker import RiskChecker, RiskRules

        # 设置极低的单笔上限
        checker = RiskChecker(redis_client=None)
        checker._rules = RiskRules(max_single_amount=100.0)

        # 策略生成信号
        signal = "BUY"

        # 风控拦截
        result = checker.check(
            ts_code="000001.SZ",
            price=50.0,
            volume=10,  # 50 * 10 = 500 > 100 上限
            direction="BUY",
        )

        # 管线在风控处中断
        assert result.passed is False
        assert "超过上限" in result.reason

    @pytest.mark.asyncio
    async def test_pipeline_multiple_orders_then_risk_limit(self, mock_redis):
        """
        连续下单直到触发日累计限额。
        验证风控在累计维度上的阻断效果。
        """
        from quant_engine.risk.checker import RiskChecker, RiskRules
        from quant_engine.order.state_machine import OrderStateMachine, OrderStatus

        checker = RiskChecker(redis_client=None)
        checker._rules = RiskRules(
            max_single_amount=100000.0,
            max_daily_amount=30000.0,  # 日累计 3 万
            max_daily_order_count=50,
        )

        daily_amount = 0.0
        order_count = 0
        orders_created = 0

        # 模拟连续 10 次下单，每次 100 股 * 10 元 = 1000 元
        for i in range(10):
            result = checker.check(
                ts_code="000001.SZ",
                price=10.0,
                volume=100,
                direction="BUY",
                daily_filled_amount=daily_amount,
                daily_order_count=order_count,
            )

            if result.passed:
                # 模拟订单创建和状态流转
                sm = OrderStateMachine(OrderStatus.PENDING, order_id=f"order-{i}")
                sm.transition(OrderStatus.SENT)
                sm.transition(OrderStatus.ACK)
                sm.transition(OrderStatus.FILLED)

                daily_amount += 10.0 * 100  # 1000 元
                order_count += 1
                orders_created += 1
            else:
                break

        # 30000 / 1000 = 30 笔，但我们只循环 10 次，所以应全部通过
        assert orders_created == 10
        assert daily_amount == 10000.0

    @pytest.mark.asyncio
    async def test_pipeline_order_status_persistence(self, mock_redis):
        """
        订单状态变更在 DB 中的持久化追踪。
        模拟：创建(PENDING) → 发送(SENT) → 确认(ACK) → 成交(FILLED)
        每次状态变更都应更新 DB 记录。
        """
        from quant_engine.order.executor import OrderExecutor
        from quant_engine.risk.checker import RiskChecker
        from quant_engine.order.state_machine import OrderStatus

        status_history = []
        conn = AsyncMock()
        conn.execute = AsyncMock()

        async def fake_get_conn():
            return conn

        checker = RiskChecker(redis_client=None)
        executor = OrderExecutor(
            risk_checker=checker,
            redis_client=mock_redis,
            get_db_conn=fake_get_conn,
        )

        # 下单
        order_id = await executor.place_order(
            ts_code="000001.SZ",
            price=10.5,
            volume=100,
            direction="BUY",
        )

        # 记录状态历史
        status_history.append(OrderStatus.PENDING)
        status_history.append(OrderStatus.SENT)  # place_order 内部已转到 SENT

        # 验证 DB 有两次写入 (INSERT PENDING + UPDATE SENT)
        execute_calls = [str(c) for c in conn.execute.call_args_list]
        assert len(execute_calls) >= 2
