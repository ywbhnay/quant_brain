"""
OpenClaw 对话逻辑

职责：
1. 解析用户输入，路由到对应命令处理器
2. 格式化命令结果为自然语言回复
3. 维护对话上下文（如交易确认状态）
"""
import logging
from dataclasses import dataclass, field

from openclaw.agent_main import ApiClient

logger = logging.getLogger("openclaw.chat")

# ---------------------------------------------------------------------------
# 对话上下文
# ---------------------------------------------------------------------------


@dataclass
class TradeConfirmation:
    """待确认的交易"""
    ts_code: str
    price: float
    volume: int
    direction: str
    order_type: str = "LIMIT"


@dataclass
class ChatContext:
    """对话上下文"""
    pending_trade: TradeConfirmation | None = None
    last_order_id: str | None = None


# ---------------------------------------------------------------------------
# 对话处理器
# ---------------------------------------------------------------------------


class ChatHandler:
    """对话处理器，解析输入并路由到命令"""

    def __init__(self, api_client: ApiClient):
        self.api = api_client
        self.ctx = ChatContext()

    async def handle(self, user_input: str) -> str:
        """
        处理用户输入，返回自然语言回复。

        命令路由规则：
        - "行情" / "日线" / "分钟线" / "股票" → market 命令
        - "账户" / "资产" / "持仓" → account 命令
        - "买入" / "卖出" / "撤单" / "订单" → trade 命令
        - "确认" / "cancel" → 交易确认/取消
        - "帮助" / "help" → 帮助信息
        """
        text = user_input.strip()

        if not text:
            return "请输入命令。输入「帮助」查看可用命令。"

        # 交易确认
        if text.lower() in ("确认", "confirm", "y", "yes"):
            return await self._handle_trade_confirm()

        # 交易取消
        if text.lower() in ("取消", "cancel", "n", "no"):
            self.ctx.pending_trade = None
            return "交易已取消。"

        # 帮助
        if text.lower() in ("帮助", "help", "?", "h"):
            return self._format_help()

        # 路由到对应命令
        lower = text.lower()

        # 行情命令
        if any(kw in lower for kw in ("行情", "日线", "分钟线", "股票", "bars", "quote")):
            return await self._handle_market(text)

        # 账户命令
        if any(kw in lower for kw in ("账户", "资产", "持仓", "account", "position")):
            return await self._handle_account(text)

        # 交易命令
        if any(kw in lower for kw in ("买入", "卖出", "撤单", "订单", "buy", "sell", "cancel order", "order")):
            return await self._handle_trade(text)

        # 默认：尝试智能解析
        return await self._handle_fallback(text)

    # --- 命令处理器 ---

    async def _handle_market(self, text: str) -> str:
        """处理行情查询命令"""
        from openclaw.commands.market import handle_market_command
        try:
            result = await handle_market_command(self.api, text)
            return result
        except Exception as e:
            logger.error(f"行情命令执行失败: {e}")
            return f"查询行情失败: {e}"

    async def _handle_account(self, text: str) -> str:
        """处理账户查询命令"""
        from openclaw.commands.account import handle_account_command
        try:
            result = await handle_account_command(self.api, text)
            return result
        except Exception as e:
            logger.error(f"账户命令执行失败: {e}")
            return f"查询账户失败: {e}"

    async def _handle_trade(self, text: str) -> str:
        """处理交易命令"""
        from openclaw.commands.trade import handle_trade_command
        try:
            result = await handle_trade_command(self.api, text, self.ctx)
            return result
        except Exception as e:
            logger.error(f"交易命令执行失败: {e}")
            return f"交易失败: {e}"

    async def _handle_trade_confirm(self) -> str:
        """确认待交易订单"""
        if self.ctx.pending_trade is None:
            return "没有待确认的交易。"

        trade = self.ctx.pending_trade
        self.ctx.pending_trade = None

        try:
            result = await self.api.place_order(
                ts_code=trade.ts_code,
                price=trade.price,
                volume=trade.volume,
                direction=trade.direction,
                order_type=trade.order_type,
            )
            self.ctx.last_order_id = result["order_id"]
            return (
                f"订单已提交！\n"
                f"  订单ID: {result['order_id']}\n"
                f"  状态: {result['status']}"
            )
        except Exception as e:
            logger.error(f"订单提交失败: {e}")
            return f"订单提交失败: {e}"

    async def _handle_fallback(self, text: str) -> str:
        """无法识别时的回退处理"""
        # 尝试解析股票代码
        import re
        match = re.search(r"\d{6}\.(SZ|SH)", text, re.IGNORECASE)
        if match:
            ts_code = match.group(0).upper()
            return (
                f"识别到股票代码 {ts_code}。\n"
                f"您可以说：\n"
                f"  - 「查看 {ts_code} 行情」\n"
                f"  - 「买入 {ts_code} 100股 价格10.5」\n"
                f"  - 「查看持仓」"
            )
        return (
            "未识别命令。可用命令：\n"
            "  - 行情：「查看 000001.SZ 日线」\n"
            "  - 账户：「查看账户」「查看持仓」\n"
            "  - 交易：「买入 000001.SZ 100股 价格10.5」\n"
            "  - 帮助：输入「帮助」"
        )

    def _format_help(self) -> str:
        """格式化帮助信息"""
        return (
            "OpenClaw 量化助手可用命令：\n\n"
            "行情查询：\n"
            "  查看 000001.SZ 日线\n"
            "  查看 000001.SZ 分钟线\n"
            "  查看股票列表\n\n"
            "账户查询：\n"
            "  查看账户\n"
            "  查看持仓\n\n"
            "交易操作：\n"
            "  买入 000001.SZ 100股 价格10.5\n"
            "  卖出 000001.SZ 100股 价格11.0\n"
            "  撤单 <订单ID>\n"
            "  查询订单 <订单ID>\n\n"
            "其他：\n"
            "  帮助 / help\n\n"
            "交易命令会要求二次确认，输入「确认」或「取消」继续。"
        )
