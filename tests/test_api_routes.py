"""
API 路由测试

覆盖：
- 行情接口 (market)
- 交易接口 (trading)
- 账户接口 (account)
- 健康检查
- CORS 中间件
- 请求日志中间件
"""
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from web_backend.schemas import (
    CancelOrderRequest,
    PlaceOrderRequest,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_pool():
    """模拟 asyncpg 连接池"""
    pool = AsyncMock()
    return pool


@pytest.fixture
def mock_row():
    """模拟数据库行"""
    def _make_row(data: dict):
        row = MagicMock()
        row.__getitem__ = lambda self, key: data[key]
        row.__contains__ = lambda self, key: key in data
        return data
    return _make_row


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_endpoint(self):
        from web_backend.main import app
        # 简单验证路由存在
        routes = [r.path for r in app.routes]
        assert "/health" in routes


# ---------------------------------------------------------------------------
# 路由注册
# ---------------------------------------------------------------------------

class TestRouteRegistration:
    def test_market_routes_exist(self):
        from web_backend.main import app
        paths = [r.path for r in app.routes]
        assert "/api/market/daily/{ts_code}" in paths
        assert "/api/market/minute/{ts_code}" in paths
        assert "/api/market/stocks" in paths

    def test_trading_routes_exist(self):
        from web_backend.main import app
        paths = [r.path for r in app.routes]
        assert "/api/trading/order" in paths
        assert "/api/trading/cancel" in paths

    def test_account_routes_exist(self):
        from web_backend.main import app
        paths = [r.path for r in app.routes]
        assert "/api/account" in paths
        assert "/api/account/positions" in paths


# ---------------------------------------------------------------------------
# 行情接口测试
# ---------------------------------------------------------------------------

class TestMarketRoutes:
    @pytest.mark.asyncio
    async def test_get_daily_bars(self):
        from web_backend.routes.market import get_daily_bars

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = [
            {
                "trade_date": "20260416",
                "open": 10.5,
                "high": 11.0,
                "low": 10.3,
                "close": 10.8,
                "vol": 100000,
                "amount": 1080000.0,
            }
        ]

        with patch("web_backend.routes.market.get_pool", return_value=mock_pool):
            result = await get_daily_bars("000001.SZ")

        assert result.ts_code == "000001.SZ"
        assert len(result.bars) == 1
        assert result.bars[0][1] == 10.5  # open
        assert result.bars[0][4] == 10.8  # close

    @pytest.mark.asyncio
    async def test_get_daily_bars_with_adj(self):
        from web_backend.routes.market import get_daily_bars

        mock_pool = AsyncMock()
        mock_pool.fetch.side_effect = [
            [
                {"trade_date": "20260416", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 50000, "amount": 525000.0}
            ],
            [
                {"trade_date": "20260416", "adj_factor": 1.2}
            ],
        ]

        with patch("web_backend.routes.market.get_pool", return_value=mock_pool):
            result = await get_daily_bars("000001.SZ", with_adj=True)

        assert result.adj_factors is not None
        assert result.adj_factors == [1.2]

    @pytest.mark.asyncio
    async def test_get_daily_bars_with_date_range(self):
        from web_backend.routes.market import get_daily_bars

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = []

        with patch("web_backend.routes.market.get_pool", return_value=mock_pool):
            await get_daily_bars(
                "000001.SZ",
                start_date="20260101",
                end_date="20260416",
                with_adj=False,
            )

        # 验证查询包含日期范围条件
        call_args = mock_pool.fetch.call_args_list[0]
        query = call_args[0][0]
        assert "trade_date >= $" in query
        assert "trade_date <= $" in query

    @pytest.mark.asyncio
    async def test_get_minute_bars(self):
        from web_backend.routes.market import get_minute_bars

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = [
            {
                "time": "2026-04-16 09:30:00",
                "open": 10.5,
                "high": 10.6,
                "low": 10.4,
                "close": 10.55,
                "vol": 5000,
                "amount": 52750.0,
            }
        ]

        with patch("web_backend.routes.market.get_pool", return_value=mock_pool):
            result = await get_minute_bars("000001.SZ", limit=100)

        assert result.ts_code == "000001.SZ"
        assert len(result.bars) == 1

    @pytest.mark.asyncio
    async def test_get_stock_list(self):
        from web_backend.routes.market import get_stock_list

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = [
            {"ts_code": "000001.SZ", "name": "平安银行"},
            {"ts_code": "000002.SZ", "name": "万科A"},
        ]

        with patch("web_backend.routes.market.get_pool", return_value=mock_pool):
            result = await get_stock_list()

        assert len(result["stocks"]) == 2
        assert result["stocks"][0]["ts_code"] == "000001.SZ"

    @pytest.mark.asyncio
    async def test_get_stock_list_with_keyword(self):
        from web_backend.routes.market import get_stock_list

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = [{"ts_code": "000001.SZ", "name": "平安银行"}]

        with patch("web_backend.routes.market.get_pool", return_value=mock_pool):
            result = await get_stock_list(keyword="平安")

        call_args = mock_pool.fetch.call_args[0]
        assert "ILIKE" in call_args[0]


# ---------------------------------------------------------------------------
# 交易接口测试
# ---------------------------------------------------------------------------

class TestTradingRoutes:
    @pytest.mark.asyncio
    async def test_place_order_success(self):
        from web_backend.routes.trading import place_order

        mock_pool = AsyncMock()
        mock_pool.execute.return_value = None

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            with patch("uuid.uuid4", return_value="test-order-id"):
                result = await place_order(
                    PlaceOrderRequest(
                        ts_code="000001.SZ",
                        price=10.5,
                        volume=100,
                        direction="BUY",
                        order_type="LIMIT",
                    )
                )

        assert result.order_id == "test-order-id"
        assert result.status == "PENDING"

    @pytest.mark.asyncio
    async def test_place_order_invalid_direction(self):
        from web_backend.routes.trading import place_order
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await place_order(
                PlaceOrderRequest(
                    ts_code="000001.SZ",
                    price=10.5,
                    volume=100,
                    direction="INVALID",
                    order_type="LIMIT",
                )
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_place_order_invalid_order_type(self):
        from web_backend.routes.trading import place_order
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await place_order(
                PlaceOrderRequest(
                    ts_code="000001.SZ",
                    price=10.5,
                    volume=100,
                    direction="BUY",
                    order_type="INVALID",
                )
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_place_order_duplicate(self):
        from web_backend.routes.trading import place_order
        from fastapi import HTTPException

        mock_pool = AsyncMock()
        mock_pool.execute.side_effect = asyncpg.UniqueViolationError()

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            with patch("uuid.uuid4", return_value="test-order-id"):
                with pytest.raises(HTTPException) as exc_info:
                    await place_order(
                        PlaceOrderRequest(
                            ts_code="000001.SZ",
                            price=10.5,
                            volume=100,
                            direction="BUY",
                            order_type="LIMIT",
                        )
                    )

        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_cancel_order_success(self):
        from web_backend.routes.trading import cancel_order

        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = {"status": "PENDING"}
        mock_pool.execute.return_value = None

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            result = await cancel_order(CancelOrderRequest(order_id="order-123"))

        assert result.order_id == "order-123"
        assert result.status == "CANCELLED"

    @pytest.mark.asyncio
    async def test_cancel_order_not_found(self):
        from web_backend.routes.trading import cancel_order
        from fastapi import HTTPException

        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = None

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            with pytest.raises(HTTPException) as exc_info:
                await cancel_order(CancelOrderRequest(order_id="nonexistent"))

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_cancel_order_terminal_state(self):
        from web_backend.routes.trading import cancel_order
        from fastapi import HTTPException

        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = {"status": "FILLED"}

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            with pytest.raises(HTTPException) as exc_info:
                await cancel_order(CancelOrderRequest(order_id="filled-order"))

        assert exc_info.value.status_code == 400
        assert "不可撤销" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_order_status(self):
        from web_backend.routes.trading import get_order_status

        import datetime
        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = {
            "id": "order-123",
            "ts_code": "000001.SZ",
            "direction": "BUY",
            "price": 10.5,
            "volume": 100,
            "status": "PENDING",
            "created_at": datetime.datetime(2026, 4, 16, 9, 30, 0),
        }

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            result = await get_order_status("order-123")

        assert result.order_id == "order-123"
        assert result.ts_code == "000001.SZ"
        assert result.status == "PENDING"

    @pytest.mark.asyncio
    async def test_get_order_status_not_found(self):
        from web_backend.routes.trading import get_order_status
        from fastapi import HTTPException

        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = None

        with patch("web_backend.routes.trading.get_pool", return_value=mock_pool):
            with pytest.raises(HTTPException) as exc_info:
                await get_order_status("nonexistent")

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# 账户接口测试
# ---------------------------------------------------------------------------

class TestAccountRoutes:
    @pytest.mark.asyncio
    async def test_get_account(self):
        from web_backend.routes.account import get_account

        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = {
            "cash": 50000.0,
            "total_assets": 150000.0,
            "market_value": 100000.0,
        }

        with patch("web_backend.routes.account.get_pool", return_value=mock_pool):
            result = await get_account()

        assert result["cash"] == 50000.0
        assert result["total_assets"] == 150000.0
        assert result["market_value"] == 100000.0

    @pytest.mark.asyncio
    async def test_get_account_not_found(self):
        from web_backend.routes.account import get_account
        from fastapi import HTTPException

        mock_pool = AsyncMock()
        mock_pool.fetchrow.return_value = None

        with patch("web_backend.routes.account.get_pool", return_value=mock_pool):
            with pytest.raises(HTTPException) as exc_info:
                await get_account()

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_positions(self):
        from web_backend.routes.account import get_positions

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = [
            {
                "ts_code": "000001.SZ",
                "volume": 1000,
                "available_volume": 500,
                "cost_price": 10.0,
                "market_price": 10.5,
                "market_value": 10500.0,
            },
            {
                "ts_code": "000002.SZ",
                "volume": 2000,
                "available_volume": 2000,
                "cost_price": 15.0,
                "market_price": 14.5,
                "market_value": 29000.0,
            },
        ]

        with patch("web_backend.routes.account.get_pool", return_value=mock_pool):
            result = await get_positions()

        assert len(result["positions"]) == 2
        assert result["positions"][0]["ts_code"] == "000001.SZ"
        assert result["positions"][0]["volume"] == 1000
        assert result["positions"][0]["cost_price"] == 10.0

    @pytest.mark.asyncio
    async def test_get_positions_empty(self):
        from web_backend.routes.account import get_positions

        mock_pool = AsyncMock()
        mock_pool.fetch.return_value = []

        with patch("web_backend.routes.account.get_pool", return_value=mock_pool):
            result = await get_positions()

        assert result["positions"] == []


# ---------------------------------------------------------------------------
# CORS 配置测试
# ---------------------------------------------------------------------------

class TestCORS:
    def test_cors_middleware_configured(self):
        from web_backend.main import app
        middleware_types = [m.cls.__name__ for m in app.user_middleware]
        assert "CORSMiddleware" in middleware_types


# ---------------------------------------------------------------------------
# 配置验证测试
# ---------------------------------------------------------------------------

class TestConfig:
    def test_validate_missing_required(self):
        from web_backend.config import WebConfig

        original = WebConfig.PG_PASSWORD
        try:
            WebConfig.PG_PASSWORD = ""
            with pytest.raises(RuntimeError) as exc_info:
                WebConfig.validate()
            assert "PG_PASSWORD" in str(exc_info.value)
        finally:
            WebConfig.PG_PASSWORD = original

    def test_pg_dsn_format(self):
        from web_backend.config import WebConfig
        dsn = WebConfig.pg_dsn()
        assert "postgres://" in dsn
        assert WebConfig.PG_HOST in dsn
        assert WebConfig.PG_DATABASE in dsn

    def test_redis_url_with_password(self):
        from web_backend.config import WebConfig
        original = WebConfig.REDIS_PASSWORD
        try:
            WebConfig.REDIS_PASSWORD = "secret"
            url = WebConfig.redis_url()
            assert "secret@" in url
        finally:
            WebConfig.REDIS_PASSWORD = original

    def test_redis_url_without_password(self):
        from web_backend.config import WebConfig
        original = WebConfig.REDIS_PASSWORD
        try:
            WebConfig.REDIS_PASSWORD = ""
            url = WebConfig.redis_url()
            assert "@" not in url.split("://")[1].split("/")[0]
        finally:
            WebConfig.REDIS_PASSWORD = original


# ---------------------------------------------------------------------------
# 数据库连接池测试
# ---------------------------------------------------------------------------

class TestDB:
    @pytest.mark.asyncio
    async def test_create_pool_singleton(self):
        from web_backend import db

        original_pool = db._pool
        db._pool = None

        mock_pool_obj = AsyncMock()

        # asyncpg.create_pool 不是原生 coroutine，需要用 MagicMock 返回 awaitable
        async def fake_create_pool(**kwargs):
            return mock_pool_obj

        with patch("web_backend.db.asyncpg.create_pool", side_effect=fake_create_pool):
            result1 = await db.create_pool()
            result2 = await db.create_pool()

        assert result1 is mock_pool_obj
        assert result1 is result2
        assert db._pool is not None

        # 清理
        db._pool = original_pool

    @pytest.mark.asyncio
    async def test_get_pool_auto_create(self):
        from web_backend import db

        original_pool = db._pool
        db._pool = None
        mock_pool_obj = AsyncMock()

        async def fake_create_pool(**kwargs):
            return mock_pool_obj

        with patch("web_backend.db.asyncpg.create_pool", side_effect=fake_create_pool):
            result = await db.get_pool()

        assert result is mock_pool_obj
        db._pool = original_pool

    @pytest.mark.asyncio
    async def test_close_pool(self):
        from web_backend import db

        mock_pool = AsyncMock()
        original_pool = db._pool
        db._pool = mock_pool

        await db.close_pool()
        assert db._pool is None
        mock_pool.close.assert_called_once()

        # 清理
        db._pool = original_pool
