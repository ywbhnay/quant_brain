"""
Phase 6: OpenClaw 命令测试

测试所有命令处理器：
- openclaw/commands/market.py
- openclaw/commands/account.py
- openclaw/commands/trade.py
- openclaw/chat.py (完整对话流程)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ============================================================================
# 行情命令测试
# ============================================================================


class TestMarketCommands:
    """测试行情查询命令"""

    @pytest.fixture
    def mock_api(self):
        """模拟 ApiClient"""
        api = AsyncMock()
        api.get_daily_bars = AsyncMock()
        api.get_minute_bars = AsyncMock()
        api.get_stock_list = AsyncMock()
        return api

    @pytest.mark.asyncio
    async def test_daily_bars_basic(self, mock_api):
        """测试基本日线查询"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_daily_bars.return_value = {
            "bars": [
                [1712419200, 10.0, 10.5, 9.8, 10.2, 1000000],
                [1712505600, 10.2, 10.8, 10.0, 10.5, 1200000],
                [1712592000, 10.5, 11.0, 10.3, 10.8, 1100000],
                [1712678400, 10.8, 11.2, 10.6, 11.0, 1300000],
                [1712764800, 11.0, 11.5, 10.9, 11.3, 1400000],
            ]
        }

        result = await handle_market_command(mock_api, "查看 000001.SZ 日线")

        assert "000001.SZ" in result
        assert "日线" in result
        mock_api.get_daily_bars.assert_called_once()

    @pytest.mark.asyncio
    async def test_daily_bars_no_data(self, mock_api):
        """测试无日线数据"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_daily_bars.return_value = {"bars": []}

        result = await handle_market_command(mock_api, "查看 000001.SZ 日线")

        assert "无日线数据" in result

    @pytest.mark.asyncio
    async def test_daily_bars_with_adj(self, mock_api):
        """测试复权日线查询"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_daily_bars.return_value = {
            "bars": [
                [1712419200, 10.0, 10.5, 9.8, 10.2, 1000000],
            ],
            "adj_factors": [1.2, 1.25, 1.3, 1.35, 1.4],
        }

        result = await handle_market_command(mock_api, "查看 000001.SZ 日线 复权")

        assert "000001.SZ" in result

    @pytest.mark.asyncio
    async def test_minute_bars_basic(self, mock_api):
        """测试基本分钟线查询"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_minute_bars.return_value = {
            "bars": [
                [1712419200, 10.0, 10.5, 9.8, 10.2, 50000],
                [1712419260, 10.2, 10.8, 10.0, 10.5, 60000],
                [1712419320, 10.5, 11.0, 10.3, 10.8, 70000],
                [1712419380, 10.8, 11.2, 10.6, 11.0, 80000],
                [1712419440, 11.0, 11.5, 10.9, 11.3, 90000],
            ]
        }

        result = await handle_market_command(mock_api, "查看 000001.SZ 分钟线")

        assert "000001.SZ" in result
        assert "分钟线" in result
        mock_api.get_minute_bars.assert_called_once()

    @pytest.mark.asyncio
    async def test_minute_bars_no_data(self, mock_api):
        """测试无分钟线数据"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_minute_bars.return_value = {"bars": []}

        result = await handle_market_command(mock_api, "查看 000001.SZ 分钟线")

        assert "无分钟线数据" in result

    @pytest.mark.asyncio
    async def test_stock_list_search(self, mock_api):
        """测试股票列表搜索"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_stock_list.return_value = [
            {"ts_code": "000001.SZ", "name": "平安银行"},
            {"ts_code": "000002.SZ", "name": "万科A"},
        ]

        result = await handle_market_command(mock_api, "查看股票列表")

        assert "平安银行" in result
        assert "000001.SZ" in result
        mock_api.get_stock_list.assert_called_once()

    @pytest.mark.asyncio
    async def test_stock_search_keyword(self, mock_api):
        """测试关键词搜索股票"""
        from openclaw.commands.market import handle_market_command

        mock_api.get_stock_list.return_value = [
            {"ts_code": "000001.SZ", "name": "平安银行"},
        ]

        result = await handle_market_command(mock_api, "查看股票 平安")

        assert "000001.SZ" in result
        assert "平安银行" in result

    @pytest.mark.asyncio
    async def test_unrecognized_market_command(self, mock_api):
        """测试无法识别的行情命令"""
        from openclaw.commands.market import handle_market_command

        result = await handle_market_command(mock_api, "查看天气预报")

        assert "未识别行情命令" in result


# ============================================================================
# 账户命令测试
# ============================================================================


class TestAccountCommands:
    """测试账户查询命令"""

    @pytest.fixture
    def mock_api(self):
        """模拟 ApiClient"""
        api = AsyncMock()
        api.get_account = AsyncMock()
        api.get_positions = AsyncMock()
        return api

    @pytest.mark.asyncio
    async def test_account_query(self, mock_api):
        """测试账户总资产查询"""
        from openclaw.commands.account import handle_account_command

        mock_api.get_account.return_value = {
            "total": 100000.0,
            "available": 50000.0,
            "frozen": 10000.0,
            "pnl": 5000.0,
        }

        result = await handle_account_command(mock_api, "查看账户")

        assert "100,000.00" in result
        assert "50,000.00" in result
        assert "10,000.00" in result
        assert "盈利" in result
        mock_api.get_account.assert_called_once()

    @pytest.mark.asyncio
    async def test_account_with_loss(self, mock_api):
        """测试账户亏损情况"""
        from openclaw.commands.account import handle_account_command

        mock_api.get_account.return_value = {
            "total": 90000.0,
            "available": 40000.0,
            "frozen": 5000.0,
            "pnl": -10000.0,
        }

        result = await handle_account_command(mock_api, "查看资产")

        assert "亏损" in result

    @pytest.mark.asyncio
    async def test_positions_query(self, mock_api):
        """测试持仓列表查询"""
        from openclaw.commands.account import handle_account_command

        mock_api.get_positions.return_value = [
            {
                "ts_code": "000001.SZ",
                "name": "平安银行",
                "volume": 1000,
                "cost_price": 10.0,
                "current_price": 11.0,
                "pnl": 1000.0,
            },
        ]

        result = await handle_account_command(mock_api, "查看持仓")

        assert "000001.SZ" in result
        assert "平安银行" in result
        assert "盈" in result
        mock_api.get_positions.assert_called_once()

    @pytest.mark.asyncio
    async def test_positions_empty(self, mock_api):
        """测试无持仓情况"""
        from openclaw.commands.account import handle_account_command

        mock_api.get_positions.return_value = []

        result = await handle_account_command(mock_api, "查看持仓")

        assert "无持仓" in result


# ============================================================================
# 交易命令测试
# ============================================================================


class TestTradeCommands:
    """测试交易命令"""

    @pytest.fixture
    def mock_api(self):
        """模拟 ApiClient"""
        api = AsyncMock()
        api.cancel_order = AsyncMock()
        api.get_order_status = AsyncMock()
        return api

    @pytest.fixture
    def mock_ctx(self):
        """模拟对话上下文"""
        from dataclasses import dataclass

        @dataclass
        class MockChatContext:
            pending_trade = None
            last_order_id = None

        return MockChatContext()

    @pytest.mark.asyncio
    async def test_buy_order_parsing(self, mock_api, mock_ctx):
        """测试买入订单解析"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "买入 000001.SZ 100股 价格10.5", mock_ctx)

        assert "确认交易" in result
        assert "买入" in result
        assert "000001.SZ" in result
        assert "100" in result
        assert "10.50" in result
        assert mock_ctx.pending_trade is not None
        assert mock_ctx.pending_trade.direction == "BUY"
        assert mock_ctx.pending_trade.volume == 100
        assert mock_ctx.pending_trade.price == 10.5

    @pytest.mark.asyncio
    async def test_sell_order_parsing(self, mock_api, mock_ctx):
        """测试卖出订单解析"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "卖出 000002.SZ 200股 价格11.0", mock_ctx)

        assert "确认交易" in result
        assert "卖出" in result
        assert mock_ctx.pending_trade is not None
        assert mock_ctx.pending_trade.direction == "SELL"
        assert mock_ctx.pending_trade.volume == 200
        assert mock_ctx.pending_trade.price == 11.0

    @pytest.mark.asyncio
    async def test_buy_missing_stock_code(self, mock_api, mock_ctx):
        """测试买入缺少股票代码"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "买入 100股 价格10.5", mock_ctx)

        assert "未识别股票代码" in result

    @pytest.mark.asyncio
    async def test_buy_missing_volume(self, mock_api, mock_ctx):
        """测试买入缺少数量"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "买入 000001.SZ 价格10.5", mock_ctx)

        assert "未识别交易数量" in result

    @pytest.mark.asyncio
    async def test_buy_missing_price(self, mock_api, mock_ctx):
        """测试买入缺少价格"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "买入 000001.SZ 100股", mock_ctx)

        assert "未识别交易价格" in result

    @pytest.mark.asyncio
    async def test_cancel_order(self, mock_api, mock_ctx):
        """测试撤单"""
        from openclaw.commands.trade import handle_trade_command

        mock_api.cancel_order.return_value = {"order_id": "order-123", "status": "CANCELLED"}

        result = await handle_trade_command(mock_api, "撤单 order-123", mock_ctx)

        assert "已撤销" in result
        mock_api.cancel_order.assert_called_once_with("order-123")

    @pytest.mark.asyncio
    async def test_cancel_missing_order_id(self, mock_api, mock_ctx):
        """测试撤单缺少订单ID"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "撤单", mock_ctx)

        assert "未识别订单ID" in result

    @pytest.mark.asyncio
    async def test_order_query(self, mock_api, mock_ctx):
        """测试订单查询"""
        from openclaw.commands.trade import handle_trade_command

        mock_api.get_order_status.return_value = {
            "order_id": "order-123",
            "ts_code": "000001.SZ",
            "direction": "BUY",
            "price": 10.5,
            "volume": 100,
            "status": "FILLED",
        }

        result = await handle_trade_command(mock_api, "查询订单 order-123", mock_ctx)

        assert "order-123" in result
        assert "000001.SZ" in result
        assert "BUY" in result
        assert "10.50" in result
        mock_api.get_order_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_unrecognized_trade_command(self, mock_api, mock_ctx):
        """测试无法识别的交易命令"""
        from openclaw.commands.trade import handle_trade_command

        result = await handle_trade_command(mock_api, "查询天气", mock_ctx)

        assert "未识别交易命令" in result


# ============================================================================
# ChatHandler 对话流程测试
# ============================================================================


class TestChatHandler:
    """测试 ChatHandler 完整对话流程"""

    @pytest.fixture
    def handler(self):
        """创建 ChatHandler 实例"""
        from openclaw.agent_main import ApiClient
        from openclaw.chat import ChatHandler

        api = ApiClient()
        return ChatHandler(api)

    @pytest.mark.asyncio
    async def test_help_command(self, handler):
        """测试帮助命令"""
        result = await handler.handle("帮助")
        assert "可用命令" in result
        assert "行情" in result

    @pytest.mark.asyncio
    async def test_help_variants(self, handler):
        """测试帮助命令变体"""
        for cmd in ["help", "?", "h"]:
            result = await handler.handle(cmd)
            assert "可用命令" in result

    @pytest.mark.asyncio
    async def test_empty_input(self, handler):
        """测试空输入"""
        result = await handler.handle("")
        assert "请输入命令" in result

    @pytest.mark.asyncio
    async def test_cancel_trade(self, handler):
        """测试取消交易"""
        # 先设置一个待确认交易
        from openclaw.chat import TradeConfirmation

        handler.ctx.pending_trade = TradeConfirmation(
            ts_code="000001.SZ",
            price=10.5,
            volume=100,
            direction="BUY",
        )

        result = await handler.handle("取消")
        assert "交易已取消" in result
        assert handler.ctx.pending_trade is None

    @pytest.mark.asyncio
    async def test_confirm_no_pending(self, handler):
        """测试无待确认交易时确认"""
        handler.ctx.pending_trade = None
        result = await handler.handle("确认")
        assert "没有待确认的交易" in result

    @pytest.mark.asyncio
    async def test_stock_code_fallback(self, handler):
        """测试识别股票代码的回退处理"""
        result = await handler.handle("000001.SZ")
        assert "000001.SZ" in result
        assert "查看" in result

    @pytest.mark.asyncio
    async def test_unrecognized_fallback(self, handler):
        """测试无法识别命令的回退处理"""
        result = await handler.handle("今天天气怎么样")
        assert "未识别命令" in result
