"""
行情查询命令处理器

解析用户输入，调用 ApiClient 获取行情数据，格式化为自然语言回复。

支持的命令：
- "查看 000001.SZ 日线" → 日线数据
- "查看 000001.SZ 分钟线" → 分钟线数据
- "查看股票列表" / "查看股票 平安" → 股票列表搜索
"""
import logging
import re

logger = logging.getLogger("openclaw.commands.market")

# 股票代码正则：6位数字.SZ 或 .SH
STOCK_CODE_RE = re.compile(r"(\d{6})\.(SZ|SH)", re.IGNORECASE)


async def handle_market_command(api, text: str) -> str:
    """
    处理行情查询命令。

    参数：
        api: ApiClient 实例
        text: 用户输入文本

    返回：
        格式化后的行情信息字符串
    """
    lower = text.lower()

    # 股票列表搜索
    if "股票" in lower and ("列表" in lower or "搜索" in lower):
        return await _handle_stock_list(api, text)

    # 搜索特定股票
    if "股票" in lower:
        keyword = _extract_keyword_after(api, text, "股票")
        return await _handle_stock_search(api, keyword or text)

    # 日线数据
    if "日线" in lower or "bars" in lower:
        return await _handle_daily_bars(api, text)

    # 分钟线数据
    if "分钟" in lower or "minute" in lower:
        return await _handle_minute_bars(api, text)

    # 默认：尝试解析股票代码查日线
    match = STOCK_CODE_RE.search(text)
    if match:
        ts_code = match.group(0).upper()
        return await _handle_daily_bars(api, text)

    return (
        "未识别行情命令。试试：\n"
        "  - 「查看 000001.SZ 日线」\n"
        "  - 「查看 000001.SZ 分钟线」\n"
        "  - 「查看股票列表」\n"
        "  - 「查看股票 平安」"
    )


async def _handle_daily_bars(api, text: str) -> str:
    """处理日线数据查询"""
    match = STOCK_CODE_RE.search(text)
    if not match:
        return "未识别股票代码。格式示例：000001.SZ"

    ts_code = match.group(0).upper()
    lower = text.lower()

    # 提取日期范围
    start_date = _extract_date(text, "from|自|从")
    end_date = _extract_date(text, "to|到|至")

    # 日线默认返回最近 30 天
    bars_resp = await api.get_daily_bars(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        with_adj="复权" in lower,
    )

    bars = bars_resp.get("bars", [])
    if not bars:
        return f"{ts_code} 无日线数据。"

    # 取最近 5 条展示
    recent = bars[-5:]
    lines = [_format_bar_line(b, with_adj=bars_resp.get("adj_factors") is not None) for b in recent]

    header = f"{ts_code} 最近 5 日日线：\n日期        开盘     最高     最低     收盘     成交量"
    return f"{header}\n" + "\n".join(lines)


async def _handle_minute_bars(api, text: str) -> str:
    """处理分钟线数据查询"""
    match = STOCK_CODE_RE.search(text)
    if not match:
        return "未识别股票代码。格式示例：000001.SZ"

    ts_code = match.group(0).upper()

    # 提取 limit 参数
    limit_match = re.search(r"(\d+)\s*条", text)
    limit = int(limit_match.group(1)) if limit_match else 20
    limit = min(limit, 100)

    bars_resp = await api.get_minute_bars(ts_code=ts_code, limit=limit)
    bars = bars_resp.get("bars", [])

    if not bars:
        return f"{ts_code} 无分钟线数据。"

    recent = bars[-5:]
    lines = [_format_bar_line(b, is_minute=True) for b in recent]

    header = f"{ts_code} 最近 {len(recent)} 条分钟线：\n时间                  开盘     最高     最低     收盘     成交量"
    return f"{header}\n" + "\n".join(lines)


async def _handle_stock_list(api, text: str) -> str:
    """处理股票列表查询"""
    keyword = _extract_keyword_after(api, text, "列表")
    keyword = keyword or ""

    stocks = await api.get_stock_list(keyword=keyword, limit=20)

    if not stocks:
        return f"未找到匹配的股票。"

    lines = [f"  {s['ts_code']}  {s['name']}" for s in stocks[:20]]
    return f"搜索结果（共 {len(stocks)} 只）：\n" + "\n".join(lines)


async def _handle_stock_search(api, text: str) -> str:
    """处理股票搜索"""
    # 提取搜索关键词
    match = STOCK_CODE_RE.search(text)
    if match:
        keyword = match.group(1)
    else:
        # 尝试提取中文关键词
        chinese = re.findall(r"[\u4e00-\u9fff]+", text)
        keyword = "".join(chinese) if chinese else ""

    stocks = await api.get_stock_list(keyword=keyword, limit=10)

    if not stocks:
        return f"未找到匹配「{keyword}」的股票。"

    lines = [f"  {s['ts_code']}  {s['name']}" for s in stocks[:10]]
    return f"搜索结果：\n" + "\n".join(lines)


# --- 辅助函数 ---


def _extract_date(text: str, keyword: str) -> str | None:
    """从文本中提取 YYYYMMDD 格式的日期"""
    pattern = rf"(?:{keyword})\s*(\d{{8}})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    # 也支持 YYYY-MM-DD 格式
    pattern2 = rf"(?:{keyword})\s*(\d{{4}})-(\d{{2}})-(\d{{2}})"
    match2 = re.search(pattern2, text, re.IGNORECASE)
    if match2:
        return f"{match2.group(1)}{match2.group(2)}{match2.group(3)}"
    return None


def _extract_keyword_after(api, text: str, marker: str) -> str:
    """提取标记词后面的关键词"""
    lower = text.lower()
    idx = lower.find(marker)
    if idx == -1:
        return ""
    return text[idx + len(marker):].strip()


def _format_bar_line(bar: list, with_adj: bool = False, is_minute: bool = False) -> str:
    """格式化单根 K 线数据"""
    timestamp = str(bar[0])
    # 截断时间戳到合理长度
    if len(timestamp) > 19:
        timestamp = timestamp[:19]
    open_, high, low, close, vol = bar[1], bar[2], bar[3], bar[4], bar[5]
    return f"{timestamp}  {open_:>8.2f}  {high:>8.2f}  {low:>8.2f}  {close:>8.2f}  {vol:>10.0f}"
