"""
Web 后端 Pydantic 响应模型

设计原则：
- 紧凑数组返回：行情数据返回 [timestamp, open, high, low, close, volume]
- 复权因子单独返回 adj_factor
- 交易指令返回订单 ID
"""
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# 行情数据响应
# ---------------------------------------------------------------------------

class DailyBarsResponse(BaseModel):
    """日线数据响应"""
    ts_code: str
    bars: list[list]  # [[date, open, high, low, close, vol, amount], ...]
    adj_factors: list[float] | None = None


class MinuteBarsResponse(BaseModel):
    """分钟线数据响应"""
    ts_code: str
    bars: list[list]  # [[datetime, open, high, low, close, vol, amount], ...]


class StockListResponse(BaseModel):
    """股票列表响应"""
    stocks: list[dict]  # [{"ts_code": ..., "name": ...}, ...]


# ---------------------------------------------------------------------------
# 交易指令响应
# ---------------------------------------------------------------------------

class PlaceOrderRequest(BaseModel):
    """下单请求"""
    ts_code: str
    price: float
    volume: int
    direction: str  # BUY / SELL
    order_type: str = "LIMIT"  # LIMIT / MARKET


class PlaceOrderResponse(BaseModel):
    """下单响应"""
    order_id: str
    status: str  # PENDING / REJECTED


class CancelOrderRequest(BaseModel):
    """撤单请求"""
    order_id: str


class CancelOrderResponse(BaseModel):
    """撤单响应"""
    order_id: str
    status: str  # CANCELLED / FAILED


class OrderStatusResponse(BaseModel):
    """订单状态查询响应"""
    order_id: str
    ts_code: str
    direction: str
    price: float
    volume: int
    status: str
    created_at: str


# ---------------------------------------------------------------------------
# 账本查询响应
# ---------------------------------------------------------------------------

class AccountResponse(BaseModel):
    """账本响应"""
    total_assets: float
    cash: float
    market_value: float
    positions: list[dict]  # [{"ts_code": ..., "volume": ..., "price": ...}, ...]
