"""
risk/checker.py 单元测试

覆盖：
1. RiskRules 默认值和 from_dict 解析
2. RiskChecker 基础检查 (单笔、日累计、黑名单、次数)
3. 卖出订单仅检查黑名单
4. Redis 规则加载和回退
5. 热更新重新加载
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from quant_engine.risk.checker import (
    RiskChecker,
    RiskRules,
    RiskCheckResult,
    RISK_RULES_KEY,
    DEFAULT_RISK_RULES,
)


# ---------------------------------------------------------------------------
class TestRiskRules:
# ---------------------------------------------------------------------------

    def test_default_values(self):
        """验证默认规则值"""
        rules = RiskRules.defaults()
        assert rules.max_single_amount == 100_000.0
        assert rules.max_daily_amount == 500_000.0
        assert rules.max_daily_order_count == 50
        assert rules.blacklist == frozenset()

    def test_from_dict_full(self):
        """验证完整解析"""
        raw = {
            "max_single_amount": "200000",
            "max_daily_amount": "1000000",
            "max_daily_order_count": "100",
            "blacklist": '["000001.SZ", "000002.SZ"]',
        }
        rules = RiskRules.from_dict(raw)
        assert rules.max_single_amount == 200_000.0
        assert rules.max_daily_amount == 1_000_000.0
        assert rules.max_daily_order_count == 100
        assert "000001.SZ" in rules.blacklist
        assert "000002.SZ" in rules.blacklist

    def test_from_dict_partial(self):
        """验证部分字段使用默认值"""
        raw = {"max_single_amount": "50000"}
        rules = RiskRules.from_dict(raw)
        assert rules.max_single_amount == 50_000.0
        assert rules.max_daily_amount == 500_000.0  # default
        assert rules.blacklist == frozenset()  # default

    def test_from_dict_empty_string_blacklist(self):
        """验证空字符串黑名单解析"""
        raw = {"blacklist": "[]"}
        rules = RiskRules.from_dict(raw)
        assert rules.blacklist == frozenset()

    def test_rules_frozen(self):
        """验证 RiskRules 不可变"""
        rules = RiskRules.defaults()
        with pytest.raises(Exception):  # frozen=True 阻止修改
            rules.max_single_amount = 0

    def test_result_ok(self):
        """验证 RiskCheckResult.ok()"""
        result = RiskCheckResult.ok()
        assert result.passed is True
        assert result.reason is None

    def test_result_reject(self):
        """验证 RiskCheckResult.reject()"""
        result = RiskCheckResult.reject("too much")
        assert result.passed is False
        assert result.reason == "too much"


# ---------------------------------------------------------------------------
class TestRiskCheckerBasic:
# ---------------------------------------------------------------------------

    def _make_checker(self) -> RiskChecker:
        return RiskChecker()

    def test_single_amount_pass(self):
        """验证单笔金额在限制内"""
        checker = self._make_checker()
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=1000, direction="BUY",
        )
        assert result.passed is True  # 10 * 1000 = 10,000 < 100,000

    def test_single_amount_exceed(self):
        """验证单笔金额超限"""
        checker = self._make_checker()
        result = checker.check(
            ts_code="000001.SZ", price=100.0, volume=2000, direction="BUY",
        )
        assert result.passed is False
        assert "单笔金额" in result.reason
        assert "200000.00" in result.reason

    def test_single_amount_boundary(self):
        """验证单笔金额正好等于上限"""
        checker = RiskChecker()
        # 手动设规则以精确测试边界
        checker._rules = RiskRules(max_single_amount=10_000.0)
        result = checker.check(
            ts_code="000001.SZ", price=100.0, volume=100, direction="BUY",
        )
        assert result.passed is True  # 10,000 == 上限 → 通过

    def test_single_amount_over_boundary(self):
        """验证单笔金额超过上限 1 元"""
        checker = RiskChecker()
        checker._rules = RiskRules(max_single_amount=10_000.0)
        result = checker.check(
            ts_code="000001.SZ", price=100.0, volume=101, direction="BUY",
        )
        assert result.passed is False  # 10,100 > 10,000

    def test_daily_amount_pass(self):
        """验证日累计金额在限制内"""
        checker = RiskChecker()
        checker._rules = RiskRules(max_daily_amount=100_000.0)
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=1000, direction="BUY",
            daily_filled_amount=80_000,
        )
        assert result.passed is True  # 80k + 10k = 90k < 100k

    def test_daily_amount_exceed(self):
        """验证日累计金额超限"""
        checker = RiskChecker()
        checker._rules = RiskRules(max_daily_amount=100_000.0)
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=5000, direction="BUY",
            daily_filled_amount=80_000,
        )
        assert result.passed is False
        assert "日累计金额" in result.reason

    def test_blacklist_rejects(self):
        """验证黑名单股票被拒绝"""
        checker = RiskChecker()
        checker._rules = RiskRules(blacklist=frozenset({"000001.SZ"}))
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=100, direction="BUY",
        )
        assert result.passed is False
        assert "黑名单" in result.reason

    def test_blacklist_passes(self):
        """验证非黑名单股票通过"""
        checker = RiskChecker()
        checker._rules = RiskRules(blacklist=frozenset({"000001.SZ"}))
        result = checker.check(
            ts_code="000002.SZ", price=10.0, volume=100, direction="BUY",
        )
        assert result.passed is True

    def test_daily_order_count_pass(self):
        """验证日累计次数在限制内"""
        checker = RiskChecker()
        checker._rules = RiskRules(max_daily_order_count=5)
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=100, direction="BUY",
            daily_order_count=3,
        )
        assert result.passed is True

    def test_daily_order_count_exceed(self):
        """验证日累计次数达上限"""
        checker = RiskChecker()
        checker._rules = RiskRules(max_daily_order_count=5)
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=100, direction="BUY",
            daily_order_count=5,
        )
        assert result.passed is False
        assert "开仓次数" in result.reason

    def test_sell_only_checks_blacklist(self):
        """验证卖出订单仅检查黑名单"""
        checker = RiskChecker()
        checker._rules = RiskRules(
            max_single_amount=1.0,  # 极低上限
            max_daily_amount=1.0,
            max_daily_order_count=0,
        )
        # 卖出应跳过金额/次数检查
        result = checker.check(
            ts_code="000001.SZ", price=100.0, volume=10000, direction="SELL",
            daily_filled_amount=999_999,
            daily_order_count=999,
        )
        assert result.passed is True

    def test_sell_blacklist_still_applies(self):
        """验证卖出订单仍检查黑名单"""
        checker = RiskChecker()
        checker._rules = RiskRules(blacklist=frozenset({"000001.SZ"}))
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=100, direction="SELL",
        )
        assert result.passed is False
        assert "黑名单" in result.reason


# ---------------------------------------------------------------------------
class TestRiskCheckerRedis:
# ---------------------------------------------------------------------------

    def _mock_redis_with_rules(self, raw_rules: dict):
        """创建带有指定规则的 mock Redis"""
        mock_client = AsyncMock()
        mock_client.hgetall = AsyncMock(return_value=raw_rules)
        mock_redis = MagicMock()
        mock_redis.ensure_connected = AsyncMock(return_value=mock_client)
        return mock_redis

    @pytest.mark.asyncio
    async def test_load_rules_from_redis(self):
        """验证从 Redis 加载规则"""
        raw = {
            "max_single_amount": "200000",
            "max_daily_amount": "800000",
            "max_daily_order_count": "30",
            "blacklist": '["600000.SH"]',
        }
        mock_redis = self._mock_redis_with_rules(raw)
        checker = RiskChecker(redis_client=mock_redis)
        await checker.load_rules()

        assert checker.rules.max_single_amount == 200_000.0
        assert "600000.SH" in checker.rules.blacklist

    @pytest.mark.asyncio
    async def test_load_rules_empty_redis(self):
        """验证 Redis 无规则时使用默认值"""
        mock_redis = self._mock_redis_with_rules({})
        checker = RiskChecker(redis_client=mock_redis)
        await checker.load_rules()
        assert checker.rules == RiskRules.defaults()

    @pytest.mark.asyncio
    async def test_load_rules_no_redis_client(self):
        """验证无 Redis 客户端时使用默认值"""
        checker = RiskChecker(redis_client=None)
        rules = await checker.load_rules()
        assert rules == RiskRules.defaults()

    @pytest.mark.asyncio
    async def test_load_rules_redis_error(self):
        """验证 Redis 异常时回退到默认值"""
        mock_redis = MagicMock()
        mock_redis.ensure_connected = AsyncMock(
            side_effect=ConnectionError("Redis down")
        )
        checker = RiskChecker(redis_client=mock_redis)
        rules = await checker.load_rules()
        # 不应抛出异常，应回退到默认值
        assert rules == RiskRules.defaults()

    @pytest.mark.asyncio
    async def test_reload_rules(self):
        """验证热更新后重新加载"""
        raw1 = {"max_single_amount": "100000"}
        raw2 = {"max_single_amount": "999999"}
        mock_client = AsyncMock()
        mock_redis = MagicMock()
        mock_redis.ensure_connected = AsyncMock(return_value=mock_client)
        mock_client.hgetall = AsyncMock(side_effect=[raw1, raw2])

        checker = RiskChecker(redis_client=mock_redis)
        await checker.load_rules()
        assert checker.rules.max_single_amount == 100_000.0

        await checker.reload_rules()
        assert checker.rules.max_single_amount == 999_999.0


# ---------------------------------------------------------------------------
class TestRiskCheckerIntegration:
# ---------------------------------------------------------------------------

    def test_multiple_violations_first_reported(self):
        """验证多项违规时返回第一个拦截原因"""
        checker = RiskChecker()
        checker._rules = RiskRules(
            max_single_amount=1.0,
            blacklist=frozenset({"000001.SZ"}),
        )
        result = checker.check(
            ts_code="000001.SZ",  # 在黑名单
            price=100.0,           # 单笔金额 100 * volume 超限
            volume=1000,
            direction="BUY",
        )
        # 黑名单先于单笔金额检查
        assert result.passed is False
        assert "黑名单" in result.reason

    def test_all_checks_pass(self):
        """验证全部检查通过"""
        checker = RiskChecker()
        checker._rules = RiskRules(
            max_single_amount=1_000_000.0,
            max_daily_amount=10_000_000.0,
            max_daily_order_count=1000,
            blacklist=frozenset(),
        )
        result = checker.check(
            ts_code="000001.SZ", price=10.0, volume=1000, direction="BUY",
            daily_filled_amount=0,
            daily_order_count=0,
        )
        assert result.passed is True
        assert result.reason is None
