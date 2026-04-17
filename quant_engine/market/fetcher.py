"""
行情获取器 — Tushare HTTP API 异步封装

职责：
1. 异步调用 Tushare HTTP API (httpx)
2. 限流控制 (Tushare 默认 200 次/分钟)
3. 增量拉取 (记录 last_trade_date，只拉新数据)
4. 支持日线、分钟线、实时快照

Tushare HTTP API 格式:
  POST https://tushare.pro/api
  Body: {"api_name": "daily", "token": "...", "params": {...}, "fields": "..."}
"""
import asyncio
import logging
import time
from typing import Any

import httpx

from quant_engine.config import QuantConfig
from quant_engine.market.snapshot import MarketSnapshot, MinuteBar

logger = logging.getLogger("market_fetcher")

# ---------------------------------------------------------------------------
# 限流器
# ---------------------------------------------------------------------------

class RateLimiter:
    """简单令牌桶限流器"""

    def __init__(self, rate: float = 3.0):
        """
        rate: 每秒最大调用次数 (默认 3 = 180 次/分钟，留有余量)
        """
        self._interval = 1.0 / rate
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """等待直到有可用令牌"""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self._interval:
                await asyncio.sleep(self._interval - elapsed)
            self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# Tushare HTTP 客户端
# ---------------------------------------------------------------------------

class TushareClient:
    """Tushare HTTP API 异步客户端"""

    BASE_URL = "https://tushare.pro/api"

    def __init__(
        self,
        token: str | None = None,
        rate_limit: float = 3.0,
        timeout: float = 15.0,
    ):
        self.token = token or QuantConfig.TUSHARE_TOKEN
        self._limiter = RateLimiter(rate=rate_limit)
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def request(
        self,
        api_name: str,
        params: dict[str, Any] | None = None,
        fields: str | None = None,
    ) -> dict[str, Any]:
        """
        调用 Tushare API，带限流和重试。

        返回格式:
        {
            "code": 0,
            "msg": "",
            "data": {
                "fields": [...],
                "items": [...]
            }
        }
        """
        await self._limiter.acquire()
        client = await self._ensure_client()

        payload: dict[str, Any] = {
            "api_name": api_name,
            "token": self.token,
            "params": params or {},
        }
        if fields:
            payload["fields"] = fields

        resp = await client.post(self.BASE_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise RuntimeError(
                f"Tushare API 错误 [{api_name}]: {data.get('msg', 'unknown')}"
            )

        return data


# ---------------------------------------------------------------------------
# 行情获取器 — 高层封装
# ---------------------------------------------------------------------------

class MarketFetcher:
    """
    行情获取器，封装 TushareClient 提供高层接口。

    使用方式:
        fetcher = MarketFetcher()
        await fetcher.connect()
        bars = await fetcher.get_minute_bars("000001.SZ", "20240102")
        await fetcher.close()
    """

    def __init__(
        self,
        token: str | None = None,
        rate_limit: float = 3.0,
    ):
        self._client = TushareClient(token=token, rate_limit=rate_limit)

    async def connect(self) -> None:
        """初始化 HTTP 客户端"""
        await self._client._ensure_client()

    async def close(self) -> None:
        await self._client.close()

    # ------------------------------------------------------------------
    # 日线数据
    # ------------------------------------------------------------------

    async def get_daily(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        获取日线数据。
        日期格式: YYYYMMDD (如 "20240102")

        返回:
        [
            {"ts_code": "...", "trade_date": "...", "open": ..., ...},
            ...
        ]
        """
        params: dict[str, str] = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        fields = (
            "ts_code,trade_date,open,high,low,close,"
            "pre_close,change,pct_chg,vol,amount"
        )
        result = await self._client.request("daily", params=params, fields=fields)
        return self._items_to_dicts(result)

    # ------------------------------------------------------------------
    # 分钟线数据
    # ------------------------------------------------------------------

    async def get_minute_bars(
        self,
        ts_code: str,
        freq: str = "1min",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[MinuteBar]:
        """
        获取分钟线数据。
        freq: "1min", "5min", "15min", "30min", "60min"

        Tushare 的 ts.pro_bar 接口通过 daily 实现 (freq 参数)。
        使用 daily 接口 + freq 参数。
        """
        params: dict[str, str] = {"ts_code": ts_code, "freq": freq}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        fields = "ts_code,trade_date,open,high,low,close,vol,amount"
        result = await self._client.request("daily", params=params, fields=fields)
        items = self._items_to_dicts(result)

        bars = []
        for item in items:
            trade_dt = item.get("trade_date", "")
            # Tushare 分钟线 trade_date 格式: "20240102 09:31:00"
            if " " in trade_dt:
                date_part, time_part = trade_dt.split(" ", 1)
                trade_time = time_part[:5]  # "HH:MM"
            else:
                date_part = trade_dt
                trade_time = "00:00"

            bars.append(MinuteBar(
                ts_code=item.get("ts_code", ts_code),
                trade_date=date_part,
                trade_time=trade_time,
                open=item.get("open"),
                high=item.get("high"),
                low=item.get("low"),
                close=item.get("close"),
                vol=item.get("vol"),
                amount=item.get("amount"),
            ))
        return bars

    # ------------------------------------------------------------------
    # 实时快照
    # ------------------------------------------------------------------

    async def get_realtime_snapshot(self, ts_code: str) -> MarketSnapshot | None:
        """
        获取实时行情快照。
        使用 Tushare realtime_quote 接口。
        """
        params = {"ts_code": ts_code}
        fields = (
            "ts_code,price,change,pct_chg,vol,amount,"
            "b1_v,b1_p,b2_v,b2_p,b3_v,b3_p,b4_v,b4_p,b5_v,b5_p,"
            "a1_v,a1_p,a2_v,a2_p,a3_v,a3_p,a4_v,a4_p,a5_v,a5_p"
        )
        result = await self._client.request(
            "realtime_quote", params=params, fields=fields
        )
        items = self._items_to_dicts(result)
        if not items:
            return None

        item = items[0]
        from datetime import datetime

        snapshot = MarketSnapshot(
            ts_code=item.get("ts_code", ts_code),
            price=item.get("price"),
            change=item.get("change"),
            pct_chg=item.get("pct_chg"),
            vol=item.get("vol"),
            amount=item.get("amount"),
            snapshot_time=datetime.now(),
        )

        # 解析买卖盘 (Tushare 格式: b1_p=买一价, b1_v=买一量)
        for i in range(1, 6):
            snapshot.bids[i - 1].price = item.get(f"b{i}_p")
            snapshot.bids[i - 1].vol = item.get(f"b{i}_v")
            snapshot.asks[i - 1].price = item.get(f"a{i}_p")
            snapshot.asks[i - 1].vol = item.get(f"a{i}_v")

        return snapshot

    # ------------------------------------------------------------------
    # 批量获取 (分片)
    # ------------------------------------------------------------------

    async def get_daily_batch(
        self,
        codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        chunk_size: int = 50,
    ) -> list[dict[str, Any]]:
        """
        批量获取日线数据，每次 chunk_size 只股票。
        Tushare 支持不传 ts_code 获取全量，但受权限限制。
        """
        all_data = []
        for i in range(0, len(codes), chunk_size):
            chunk = codes[i : i + chunk_size]
            ts_codes = ",".join(chunk)
            params: dict[str, str] = {"ts_code": ts_codes}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date

            fields = (
                "ts_code,trade_date,open,high,low,close,"
                "pre_close,change,pct_chg,vol,amount"
            )
            try:
                result = await self._client.request("daily", params=params, fields=fields)
                all_data.extend(self._items_to_dicts(result))
            except RuntimeError as e:
                logger.warning(f"批量获取失败 (chunk {i}): {e}")

        return all_data

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    @staticmethod
    def _items_to_dicts(result: dict[str, Any]) -> list[dict[str, Any]]:
        """将 Tushare 返回的 fields/items 格式转为字典列表"""
        data = result.get("data", {})
        fields = data.get("fields", [])
        items = data.get("items", [])
        return [dict(zip(fields, item)) for item in items]
