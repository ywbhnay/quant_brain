"""
实时行情快照模型

职责：
1. 定义实时快照数据结构 (dataclass)
2. from_dict / to_dict 序列化
3. 支持 5 档买卖盘
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Level5Quote:
    """单档买卖盘"""
    price: float | None = None
    vol: float | None = None


@dataclass
class MarketSnapshot:
    """
    实时行情快照

    对应 Tushare realtime_quote 接口返回数据。
    用于盘中快速获取最新行情，通过 Redis Pub/Sub 分发。
    """
    ts_code: str
    price: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    vol: float | None = None
    amount: float | None = None
    # 5 档买卖盘
    bids: list[Level5Quote] = field(default_factory=lambda: [Level5Quote() for _ in range(5)])
    asks: list[Level5Quote] = field(default_factory=lambda: [Level5Quote() for _ in range(5)])
    snapshot_time: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """序列化为字典，用于 Redis 存储"""
        data: dict[str, Any] = {
            "ts_code": self.ts_code,
            "price": self.price,
            "change": self.change,
            "pct_chg": self.pct_chg,
            "vol": self.vol,
            "amount": self.amount,
        }
        if self.snapshot_time:
            data["snapshot_time"] = self.snapshot_time.isoformat()

        for i, bid in enumerate(self.bids, 1):
            data[f"b{i}_price"] = bid.price
            data[f"b{i}_vol"] = bid.vol
        for i, ask in enumerate(self.asks, 1):
            data[f"a{i}_price"] = ask.price
            data[f"a{i}_vol"] = ask.vol

        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MarketSnapshot":
        """从字典反序列化"""
        bids = []
        asks = []
        for i in range(1, 6):
            bids.append(Level5Quote(
                price=data.get(f"b{i}_price"),
                vol=data.get(f"b{i}_vol"),
            ))
            asks.append(Level5Quote(
                price=data.get(f"a{i}_price"),
                vol=data.get(f"a{i}_vol"),
            ))

        snapshot_time = None
        if raw_time := data.get("snapshot_time"):
            snapshot_time = datetime.fromisoformat(raw_time)

        return cls(
            ts_code=data["ts_code"],
            price=data.get("price"),
            change=data.get("change"),
            pct_chg=data.get("pct_chg"),
            vol=data.get("vol"),
            amount=data.get("amount"),
            bids=bids,
            asks=asks,
            snapshot_time=snapshot_time,
        )


@dataclass
class MinuteBar:
    """
    分钟级 K 线数据

    对应 Tushare ts.pro_bar(freq='min') 返回数据。
    用于落盘到 PostgreSQL minute_bar 表。
    """
    ts_code: str
    trade_date: str
    trade_time: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    vol: float | None = None
    amount: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """序列化为字典，用于 Redis Stream"""
        return {
            "ts_code": self.ts_code,
            "trade_date": self.trade_date,
            "trade_time": self.trade_time,
            "open": str(self.open) if self.open is not None else "",
            "high": str(self.high) if self.high is not None else "",
            "low": str(self.low) if self.low is not None else "",
            "close": str(self.close) if self.close is not None else "",
            "vol": str(self.vol) if self.vol is not None else "",
            "amount": str(self.amount) if self.amount is not None else "",
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "MinuteBar":
        """从 Redis Stream 字符串数据反序列化"""
        def _float(val: str) -> float | None:
            return float(val) if val else None

        return cls(
            ts_code=data["ts_code"],
            trade_date=data["trade_date"],
            trade_time=data["trade_time"],
            open=_float(data.get("open", "")),
            high=_float(data.get("high", "")),
            low=_float(data.get("low", "")),
            close=_float(data.get("close", "")),
            vol=_float(data.get("vol", "")),
            amount=_float(data.get("amount", "")),
        )
