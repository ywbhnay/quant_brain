"""
实时行情快照客户端 — 仅保留 Tushare realtime_quote 调用

职责：
1. 调用 Tushare realtime_quote HTTP API 获取 5 档买卖盘
2. 限流控制 (默认 3 次/秒 = 180 次/分钟，Tushare 限制 200/分钟)

为什么仍然保留 Tushare HTTP：
- PG 库由 quant_data_pipeline 填充，只包含历史行情（日线/财务/宏观）
- 5 档盘口数据是实时数据，不进 PG，必须直接调 Tushare
- 历史日线 / 分钟线等 → 走 PGMarketReader (reader.py)

Tushare HTTP API 格式:
  POST https://tushare.pro/api
  Body: {"api_name": "realtime_quote", "token": "...", "params": {...}, "fields": "..."}
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any

import httpx

from quant_engine.config import QuantConfig
from quant_engine.market.snapshot import MarketSnapshot

logger = logging.getLogger("realtime_quote_client")


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
# 实时快照客户端
# ---------------------------------------------------------------------------


class RealtimeQuoteClient:
    """
    Tushare realtime_quote 实时快照客户端。

    仅用于获取 5 档盘口；其他历史行情请走 PGMarketReader。

    使用方式：
        client = RealtimeQuoteClient()
        snapshot = await client.get_realtime_snapshot("000001.SZ")
        await client.close()
    """

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
        """关闭底层 HTTP 连接。"""
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # 实时快照
    # ------------------------------------------------------------------

    async def get_realtime_snapshot(self, ts_code: str) -> MarketSnapshot | None:
        """
        获取实时行情快照（5 档盘口）。

        Args:
            ts_code: 股票代码 (e.g. "000001.SZ")

        Returns:
            MarketSnapshot 或 None（Tushare 返回空数据时）

        Raises:
            RuntimeError: Tushare 返回非零 code
            httpx.HTTPError: 网络层错误
        """
        if not self.token:
            raise RuntimeError(
                "TUSHARE_TOKEN 未配置；RealtimeQuoteClient 需要 token 才能获取实时快照"
            )

        await self._limiter.acquire()
        client = await self._ensure_client()

        payload: dict[str, Any] = {
            "api_name": "realtime_quote",
            "token": self.token,
            "params": {"ts_code": ts_code},
            "fields": (
                "ts_code,price,change,pct_chg,vol,amount,"
                "b1_v,b1_p,b2_v,b2_p,b3_v,b3_p,b4_v,b4_p,b5_v,b5_p,"
                "a1_v,a1_p,a2_v,a2_p,a3_v,a3_p,a4_v,a4_p,a5_v,a5_p"
            ),
        }

        resp = await client.post(self.BASE_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"Tushare API 错误 [realtime_quote]: {data.get('msg', 'unknown')}")

        items = self._items_to_dicts(data)
        if not items:
            return None

        item = items[0]
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
    # 工具
    # ------------------------------------------------------------------

    @staticmethod
    def _items_to_dicts(result: dict[str, Any]) -> list[dict[str, Any]]:
        """将 Tushare 返回的 fields/items 格式转为字典列表。"""
        data = result.get("data", {})
        fields = data.get("fields", [])
        items = data.get("items", [])
        return [dict(zip(fields, item)) for item in items]
