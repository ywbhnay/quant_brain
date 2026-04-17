"""
OpenClaw AI Agent — 入口 + HTTP 客户端

职责：
1. 封装 FastAPI 后端 HTTP 调用
2. 提供统一的 API 客户端接口
3. 连接配置管理
"""
import logging
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger("openclaw.agent")

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class OpenClawConfig:
    """OpenClaw 配置"""
    api_base_url: str = "http://127.0.0.1:8000"
    timeout: float = 10.0
    max_retries: int = 3


# ---------------------------------------------------------------------------
# HTTP 客户端
# ---------------------------------------------------------------------------


class ApiClient:
    """FastAPI 后端 HTTP 客户端，封装所有 API 调用"""

    def __init__(self, config: OpenClawConfig | None = None):
        self.config = config or OpenClawConfig()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.config.api_base_url,
                timeout=httpx.Timeout(self.config.timeout),
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            logger.info("ApiClient 连接已关闭")

    # --- 行情数据 ---

    async def get_daily_bars(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        with_adj: bool = False,
    ) -> dict:
        """获取日线数据"""
        client = await self._get_client()
        params: dict = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if with_adj:
            params["with_adj"] = "true"
        resp = await client.get(f"/api/market/daily/{ts_code}", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_minute_bars(
        self,
        ts_code: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
    ) -> dict:
        """获取分钟线数据"""
        client = await self._get_client()
        params: dict = {"limit": limit}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        resp = await client.get(f"/api/market/minute/{ts_code}", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_stock_list(
        self,
        keyword: str = "",
        limit: int = 50,
    ) -> list[dict]:
        """获取股票列表"""
        client = await self._get_client()
        params = {"keyword": keyword, "limit": limit}
        resp = await client.get("/api/market/stocks", params=params)
        resp.raise_for_status()
        return resp.json()["stocks"]

    # --- 交易指令 ---

    async def place_order(
        self,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        order_type: str = "LIMIT",
    ) -> dict:
        """提交交易指令"""
        client = await self._get_client()
        resp = await client.post(
            "/api/trading/order",
            json={
                "ts_code": ts_code,
                "price": price,
                "volume": volume,
                "direction": direction,
                "order_type": order_type,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def cancel_order(self, order_id: str) -> dict:
        """撤单"""
        client = await self._get_client()
        resp = await client.post(
            "/api/trading/cancel",
            json={"order_id": order_id},
        )
        resp.raise_for_status()
        return resp.json()

    async def get_order_status(self, order_id: str) -> dict:
        """查询订单状态"""
        client = await self._get_client()
        resp = await client.get(f"/api/trading/order/{order_id}")
        resp.raise_for_status()
        return resp.json()

    # --- 账户查询 ---

    async def get_account(self) -> dict:
        """查询账户总资产"""
        client = await self._get_client()
        resp = await client.get("/api/account")
        resp.raise_for_status()
        return resp.json()

    async def get_positions(self) -> list[dict]:
        """查询持仓列表"""
        client = await self._get_client()
        resp = await client.get("/api/account/positions")
        resp.raise_for_status()
        return resp.json()["positions"]
