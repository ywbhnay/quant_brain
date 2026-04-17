"""
账户查询命令处理器

解析用户输入，调用 ApiClient 获取账户信息，格式化为自然语言回复。

支持的命令：
- "查看账户" → 账户总资产
- "查看持仓" → 持仓列表
- "查看资产" → 同账户
"""
import logging

logger = logging.getLogger("openclaw.commands.account")


async def handle_account_command(api, text: str) -> str:
    """
    处理账户查询命令。

    参数：
        api: ApiClient 实例
        text: 用户输入文本

    返回：
        格式化后的账户信息字符串
    """
    lower = text.lower()

    if "持仓" in lower or "position" in lower:
        return await _handle_positions(api)

    if "账户" in lower or "资产" in lower or "account" in lower:
        return await _handle_account(api)

    # 默认返回账户信息
    return await _handle_account(api)


async def _handle_account(api) -> str:
    """处理账户总资产查询"""
    account = await api.get_account()

    total = account.get("total", 0)
    available = account.get("available", 0)
    frozen = account.get("frozen", 0)
    pnl = account.get("pnl", 0)

    pnl_sign = "+" if pnl >= 0 else ""
    pnl_color = "盈利" if pnl >= 0 else "亏损"

    return (
        f"账户信息：\n"
        f"  总资产: ¥{total:,.2f}\n"
        f"  可用资金: ¥{available:,.2f}\n"
        f"  冻结资金: ¥{frozen:,.2f}\n"
        f"  持仓盈亏: {pnl_sign}¥{abs(pnl):,.2f} ({pnl_color})"
    )


async def _handle_positions(api) -> str:
    """处理持仓列表查询"""
    positions = await api.get_positions()

    if not positions:
        return "当前无持仓。"

    lines = []
    for pos in positions:
        ts_code = pos.get("ts_code", "")
        name = pos.get("name", "")
        volume = pos.get("volume", 0)
        cost = pos.get("cost_price", 0)
        current = pos.get("current_price", 0)
        pnl = pos.get("pnl", 0)

        pnl_pct = 0
        if cost > 0:
            pnl_pct = ((current - cost) / cost) * 100

        pnl_sign = "+" if pnl >= 0 else ""
        direction = "盈" if pnl >= 0 else "亏"

        lines.append(
            f"  {ts_code} {name}\n"
            f"    持仓: {volume} 股  成本: ¥{cost:.2f}  现价: ¥{current:.2f}\n"
            f"    盈亏: {pnl_sign}¥{abs(pnl):,.2f} ({pnl_sign}{pnl_pct:.1f}%) {direction}"
        )

    header = f"持仓列表（共 {len(positions)} 只）："
    return f"{header}\n" + "\n".join(lines)
