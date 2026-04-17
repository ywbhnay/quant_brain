"""
market/snapshot.py 单元测试

覆盖：
1. Level5Quote 默认值
2. MarketSnapshot.to_dict / from_dict
3. MinuteBar.to_dict / from_dict
4. 序列化往返一致性
"""
from datetime import datetime

import pytest

from quant_engine.market.snapshot import (
    Level5Quote,
    MarketSnapshot,
    MinuteBar,
)


# ---------------------------------------------------------------------------
class TestLevel5Quote:
# ---------------------------------------------------------------------------

    def test_default_values(self):
        """验证默认价格为 None，量为 None"""
        quote = Level5Quote()
        assert quote.price is None
        assert quote.vol is None

    def test_custom_values(self):
        """验证自定义值"""
        quote = Level5Quote(price=10.5, vol=100.0)
        assert quote.price == 10.5
        assert quote.vol == 100.0


# ---------------------------------------------------------------------------
class TestMarketSnapshot:
# ---------------------------------------------------------------------------

    def test_default_bids_asks(self):
        """验证默认生成 5 档买卖盘"""
        snap = MarketSnapshot(ts_code="000001.SZ", price=10.0)
        assert len(snap.bids) == 5
        assert len(snap.asks) == 5
        assert all(b.price is None for b in snap.bids)
        assert all(a.price is None for a in snap.asks)

    def test_to_dict(self):
        """验证 to_dict 序列化 (扁平格式 b1_price, b1_vol ...)"""
        snap = MarketSnapshot(
            ts_code="000001.SZ",
            price=10.5,
            change=0.5,
            pct_chg=5.0,
            vol=1000.0,
            amount=10500.0,
        )
        d = snap.to_dict()
        assert d["ts_code"] == "000001.SZ"
        assert d["price"] == 10.5
        assert d["change"] == 0.5
        assert d["pct_chg"] == 5.0
        assert d["vol"] == 1000.0
        assert d["amount"] == 10500.0
        # 买卖盘使用扁平 key
        assert "b1_price" in d
        assert "b1_vol" in d
        assert "a1_price" in d
        assert "a1_vol" in d

    def test_from_dict_roundtrip(self):
        """验证 from_dict(to_dict()) 往返一致"""
        snap = MarketSnapshot(
            ts_code="000001.SZ",
            price=10.5,
            change=0.5,
            pct_chg=5.0,
            vol=1000.0,
            amount=10500.0,
        )
        snap.bids[0].price = 10.4
        snap.bids[0].vol = 500.0
        snap.asks[0].price = 10.6
        snap.asks[0].vol = 300.0

        d = snap.to_dict()
        restored = MarketSnapshot.from_dict(d)

        assert restored.ts_code == snap.ts_code
        assert restored.price == snap.price
        assert restored.bids[0].price == 10.4
        assert restored.bids[0].vol == 500.0
        assert restored.asks[0].price == 10.6
        assert restored.asks[0].vol == 300.0

    def test_from_dict_missing_optional_fields(self):
        """验证缺失字段使用默认值"""
        d = {"ts_code": "000001.SZ"}
        snap = MarketSnapshot.from_dict(d)
        assert snap.ts_code == "000001.SZ"
        assert snap.price is None

    def test_bids_asks_flat_serialization(self):
        """验证买卖盘的扁平 key 格式序列化"""
        snap = MarketSnapshot(ts_code="000001.SZ")
        snap.bids[0].price = 10.0
        snap.bids[0].vol = 100.0
        snap.asks[0].price = 10.1
        snap.asks[0].vol = 200.0

        d = snap.to_dict()
        assert d["b1_price"] == 10.0
        assert d["b1_vol"] == 100.0
        assert d["a1_price"] == 10.1
        assert d["a1_vol"] == 200.0
        # 其余档位为 None
        assert d["b2_price"] is None
        assert d["a5_vol"] is None

    def test_snapshot_time_serialization(self):
        """验证 snapshot_time ISO 格式序列化"""
        now = datetime(2024, 1, 2, 9, 30, 0)
        snap = MarketSnapshot(ts_code="000001.SZ", snapshot_time=now)
        d = snap.to_dict()
        assert d["snapshot_time"] == "2024-01-02T09:30:00"

        restored = MarketSnapshot.from_dict(d)
        assert restored.snapshot_time == now


# ---------------------------------------------------------------------------
class TestMinuteBar:
# ---------------------------------------------------------------------------

    def test_to_dict(self):
        """验证 to_dict 序列化 (值转字符串)"""
        bar = MinuteBar(
            ts_code="000001.SZ",
            trade_date="20240102",
            trade_time="09:31",
            open=10.0,
            high=10.5,
            low=9.8,
            close=10.2,
            vol=1000.0,
            amount=10200.0,
        )
        d = bar.to_dict()
        assert d["ts_code"] == "000001.SZ"
        assert d["trade_date"] == "20240102"
        assert d["trade_time"] == "09:31"
        # MinuteBar.to_dict 将数值转为字符串 (Redis Stream 格式)
        assert d["open"] == "10.0"
        assert d["high"] == "10.5"
        assert d["low"] == "9.8"
        assert d["close"] == "10.2"
        assert d["vol"] == "1000.0"
        assert d["amount"] == "10200.0"

    def test_to_dict_none_values(self):
        """验证 None 值转为空字符串"""
        bar = MinuteBar(ts_code="000001.SZ", trade_date="20240102", trade_time="09:31")
        d = bar.to_dict()
        assert d["open"] == ""
        assert d["vol"] == ""

    def test_from_dict_roundtrip(self):
        """验证 from_dict(to_dict()) 往返一致"""
        bar = MinuteBar(
            ts_code="000001.SZ",
            trade_date="20240102",
            trade_time="09:31",
            open=10.0,
            high=10.5,
            low=9.8,
            close=10.2,
            vol=1000.0,
            amount=10200.0,
        )
        d = bar.to_dict()
        restored = MinuteBar.from_dict(d)
        assert restored.ts_code == bar.ts_code
        assert restored.trade_date == bar.trade_date
        assert restored.trade_time == bar.trade_time
        assert restored.open == bar.open
        assert restored.high == bar.high
        assert restored.low == bar.low
        assert restored.close == bar.close
        assert restored.vol == bar.vol
        assert restored.amount == bar.amount

    def test_from_dict_empty_string_as_none(self):
        """验证空字符串解析为 None"""
        d = {
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "trade_time": "09:31",
            "open": "",
            "vol": "",
        }
        bar = MinuteBar.from_dict(d)
        assert bar.open is None
        assert bar.vol is None
