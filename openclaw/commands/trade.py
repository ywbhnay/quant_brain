"""
交易命令处理器

解析用户自然语言交易指令，提取股票代码、数量、价格、方向，
调用 ApiClient 执行交易或设置待确认状态。

支持的命令：
- "买入 000001.SZ 100股 价格10.5" → 设置待确认交易
- "卖出 000001.SZ 200股 价格11.0" → 设置待确认交易
- "撤单 <订单ID>" → 直接撤单
- "查询订单 <订单ID>" → 查询订单状态
"""
import logging
import re

from openclaw.chat import ChatContext

logger = logging.getLogger("openclaw.commands.trade")

# 股票代码正则
STOCK_CODE_RE = re.compile(r"(\d{6})\.(SZ|SH)", re.IGNORECASE)
# 数量正则：数字 + "股" (必须带"股"字以区分股票代码)
VOLUME_RE = re.compile(r"(\d+)\s*股")
# 价格正则："价格" + 数字 或 直接数字（小数）
PRICE_RE = re.compile(r"价格\s*([\d.]+)|[\@]\s*([\d.]+)|([\d]+\.[\d]+)")
# 订单ID正则
ORDER_ID_RE = re.compile(r"([\w-]+)", re.IGNORECASE)


async def handle_trade_command(api, text: str, ctx: ChatContext) -> str:
    """
    处理交易命令。

    参数：
        api: ApiClient 实例
        text: 用户输入文本
        ctx: 对话上下文

    返回：
        格式化后的交易信息字符串
    """
    lower = text.lower()

    # 撤单
    if "撤单" in lower or "cancel order" in lower:
        return await _handle_cancel(api, text)

    # 查询订单
    if "查询订单" in lower or "order" in lower or "订单" in lower:
        return await _handle_order_query(api, text)

    # 买入/卖出
    if "买入" in lower or "buy" in lower:
        return await _handle_buy(api, text, ctx)

    if "卖出" in lower or "sell" in lower:
        return await _handle_sell(api, text, ctx)

    return (
        "未识别交易命令。试试：\n"
        "  - 「买入 000001.SZ 100股 价格10.5」\n"
        "  - 「卖出 000001.SZ 100股 价格11.0」\n"
        "  - 「撤单 <订单ID>」\n"
        "  - 「查询订单 <订单ID>」"
    )


async def _handle_buy(api, text: str, ctx: ChatContext) -> str:
    """处理买入指令"""
    return await _parse_and_confirm_trade(api, text, ctx, direction="BUY")


async def _handle_sell(api, text: str, ctx: ChatContext) -> str:
    """处理卖出指令"""
    return await _parse_and_confirm_trade(api, text, ctx, direction="SELL")


async def _parse_and_confirm_trade(api, text: str, ctx: ChatContext, direction: str) -> str:
    """
    解析交易参数并设置待确认状态。

    需要用户二次确认后才会真正提交订单。
    """
    # 提取股票代码
    code_match = STOCK_CODE_RE.search(text)
    if not code_match:
        return "未识别股票代码。格式示例：000001.SZ"
    ts_code = code_match.group(0).upper()

    # 提取数量
    vol_match = VOLUME_RE.search(text)
    if not vol_match:
        return "未识别交易数量。格式示例：100股"
    volume = int(vol_match.group(1))

    # 提取价格
    price = _extract_price(text)
    if price is None:
        return "未识别交易价格。格式示例：价格10.5"

    # 设置待确认交易
    ctx.pending_trade = type(
        "TradeConfirmation",
        (),
        {
            "ts_code": ts_code,
            "price": price,
            "volume": volume,
            "direction": direction,
            "order_type": "LIMIT",
        },
    )()

    direction_cn = "买入" if direction == "BUY" else "卖出"
    return (
        f"请确认交易：\n"
        f"  {direction_cn} {ts_code}\n"
        f"  数量: {volume} 股\n"
        f"  价格: ¥{price:.2f}\n"
        f"  类型: 限价单\n\n"
        f"输入「确认」提交订单，或「取消」放弃。"
    )


async def _handle_cancel(api, text: str) -> str:
    """处理撤单指令"""
    order_id = _extract_order_id(text)
    if not order_id:
        return "未识别订单ID。格式示例：撤单 order-123"

    try:
        result = await api.cancel_order(order_id)
        return f"订单 {order_id} 已撤销。状态: {result.get('status', 'unknown')}"
    except Exception as e:
        logger.error(f"撤单失败: {e}")
        return f"撤单失败: {e}"


async def _handle_order_query(api, text: str) -> str:
    """处理订单查询指令"""
    order_id = _extract_order_id(text)
    if not order_id:
        return "未识别订单ID。格式示例：查询订单 order-123"

    try:
        result = await api.get_order_status(order_id)
        return (
            f"订单 {order_id} 状态：\n"
            f"  股票代码: {result.get('ts_code', 'N/A')}\n"
            f"  方向: {result.get('direction', 'N/A')}\n"
            f"  价格: ¥{result.get('price', 0):.2f}\n"
            f"  数量: {result.get('volume', 0)}\n"
            f"  状态: {result.get('status', 'N/A')}"
        )
    except Exception as e:
        logger.error(f"查询订单失败: {e}")
        return f"查询订单失败: {e}"


# --- 辅助函数 ---


def _extract_price(text: str) -> float | None:
    """从文本中提取价格"""
    # 尝试 "价格XX.XX" 格式
    match = re.search(r"价格\s*([\d.]+)", text)
    if match:
        return float(match.group(1))
    # 尝试 "@XX.XX" 格式
    match = re.search(r"@([\d.]+)", text)
    if match:
        return float(match.group(1))
    # 尝试最后一个小数
    matches = re.findall(r"(\d+\.\d+)", text)
    if matches:
        return float(matches[-1])
    return None


def _extract_order_id(text: str) -> str | None:
    """从文本中提取订单ID"""
    # 尝试匹配 UUID 格式
    match = re.search(r"([a-f0-9-]{8,})", text, re.IGNORECASE)
    if match:
        return match.group(1)
    # 尝试匹配 order-xxx 格式
    match = re.search(r"(order-\S+)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    return None
