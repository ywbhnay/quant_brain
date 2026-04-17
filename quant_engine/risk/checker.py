"""
风控检查器

职责：
1. 单笔上限检查 (price * volume)
2. 日累计上限检查 (当日已开仓金额 + 本次金额)
3. 标的黑名单检查 (禁止交易的股票)
4. 日累计开仓次数检查

规则来源:
- Redis Hash (risk_rules) — 盘中热更新，无需重启
- 回退到内存默认值 (Redis 不可用时)

数据流:
  策略信号 → RiskChecker.check() → 通过则创建订单(PENDING)
                                  → 拦截则返回 REJECTED + 原因
"""
import json
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("risk_checker")

# ---------------------------------------------------------------------------
# Redis Hash Key 常量
# ---------------------------------------------------------------------------
RISK_RULES_KEY = "risk_rules"

# ---------------------------------------------------------------------------
# 默认风控规则 (Redis 不可用时回退)
# ---------------------------------------------------------------------------
DEFAULT_RISK_RULES = {
    "max_single_amount": "100000",       # 单笔最大金额 (元)
    "max_daily_amount": "500000",        # 日累计最大金额 (元)
    "max_daily_order_count": "50",       # 日累计最大开仓次数
    "blacklist": "[]",                   # 标的黑名单 JSON 数组
}


@dataclass(frozen=True)
class RiskRules:
    """风控规则快照 (从 Redis 加载后解析为不可变结构)"""
    max_single_amount: float = 100_000.0
    max_daily_amount: float = 500_000.0
    max_daily_order_count: int = 50
    blacklist: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_dict(cls, raw: dict) -> "RiskRules":
        """从 Redis Hash 的字符串值解析为 RiskRules"""
        return cls(
            max_single_amount=float(raw.get("max_single_amount", DEFAULT_RISK_RULES["max_single_amount"])),
            max_daily_amount=float(raw.get("max_daily_amount", DEFAULT_RISK_RULES["max_daily_amount"])),
            max_daily_order_count=int(raw.get("max_daily_order_count", DEFAULT_RISK_RULES["max_daily_order_count"])),
            blacklist=frozenset(json.loads(raw.get("blacklist", DEFAULT_RISK_RULES["blacklist"]))),
        )

    @classmethod
    def defaults(cls) -> "RiskRules":
        """返回默认规则"""
        return cls()


@dataclass(frozen=True)
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    reason: str | None = None

    @classmethod
    def ok(cls) -> "RiskCheckResult":
        return cls(passed=True)

    @classmethod
    def reject(cls, reason: str) -> "RiskCheckResult":
        return cls(passed=False, reason=reason)


# ---------------------------------------------------------------------------
# 风控检查器
# ---------------------------------------------------------------------------

class RiskChecker:
    """
    风控检查器

    使用方式:
        checker = RiskChecker(redis_client)
        await checker.load_rules()  # 从 Redis 加载规则

        result = checker.check(
            ts_code="000001.SZ",
            price=10.5,
            volume=1000,
            direction="BUY",
            daily_filled_amount=200_000,
            daily_order_count=10,
        )
        if not result.passed:
            logger.warning(f"风控拦截: {result.reason}")
    """

    def __init__(self, redis_client=None):
        """
        Args:
            redis_client: RedisClient 实例 (可选，用于热加载规则)
        """
        self._redis = redis_client
        self._rules = RiskRules.defaults()

    @property
    def rules(self) -> RiskRules:
        return self._rules

    async def load_rules(self) -> RiskRules:
        """
        从 Redis Hash 加载风控规则。
        Redis 不可用时回退到默认值。
        """
        if self._redis is None:
            logger.warning("无 Redis 客户端，使用默认风控规则")
            return self._rules

        try:
            client = await self._redis.ensure_connected()
            raw = await client.hgetall(RISK_RULES_KEY)
            if raw:
                self._rules = RiskRules.from_dict(raw)
                logger.info(f"风控规则已从 Redis 加载: {len(self._rules.blacklist)} 只黑名单股票")
            else:
                logger.info("Redis 中无风控规则，使用默认值")
        except Exception as e:
            logger.warning(f"从 Redis 加载风控规则失败，使用默认值: {e}")

        return self._rules

    async def reload_rules(self) -> RiskRules:
        """重新加载规则 (用于盘中热更新后主动刷新)"""
        return await self.load_rules()

    def check(
        self,
        ts_code: str,
        price: float,
        volume: int,
        direction: str,
        daily_filled_amount: float = 0.0,
        daily_order_count: int = 0,
    ) -> RiskCheckResult:
        """
        执行风控检查

        Args:
            ts_code: 股票代码
            price: 委托价格
            volume: 委托数量
            direction: 方向 (BUY/SELL)
            daily_filled_amount: 当日已成交金额
            daily_order_count: 当日已开仓次数

        Returns:
            RiskCheckResult
        """
        # 卖出订单通常不做风控拦截 (仅检查黑名单)
        if direction == "SELL":
            return self._check_blacklist(ts_code)

        # 1. 标的黑名单
        result = self._check_blacklist(ts_code)
        if not result.passed:
            return result

        # 2. 单笔金额上限
        result = self._check_single_amount(price, volume)
        if not result.passed:
            return result

        # 3. 日累计金额上限
        result = self._check_daily_amount(price, volume, daily_filled_amount)
        if not result.passed:
            return result

        # 4. 日累计开仓次数
        result = self._check_daily_order_count(daily_order_count)
        if not result.passed:
            return result

        return RiskCheckResult.ok()

    # ------------------------------------------------------------------
    # 单项检查
    # ------------------------------------------------------------------

    def _check_blacklist(self, ts_code: str) -> RiskCheckResult:
        if ts_code in self._rules.blacklist:
            return RiskCheckResult.reject(f"标的 {ts_code} 在风控黑名单中")
        return RiskCheckResult.ok()

    def _check_single_amount(self, price: float, volume: int) -> RiskCheckResult:
        amount = price * volume
        if amount > self._rules.max_single_amount:
            return RiskCheckResult.reject(
                f"单笔金额 {amount:.2f} 超过上限 {self._rules.max_single_amount:.2f}"
            )
        return RiskCheckResult.ok()

    def _check_daily_amount(
        self, price: float, volume: int, daily_filled_amount: float
    ) -> RiskCheckResult:
        amount = price * volume
        total = daily_filled_amount + amount
        if total > self._rules.max_daily_amount:
            return RiskCheckResult.reject(
                f"日累计金额 {total:.2f} 超过上限 {self._rules.max_daily_amount:.2f}"
            )
        return RiskCheckResult.ok()

    def _check_daily_order_count(self, daily_order_count: int) -> RiskCheckResult:
        if daily_order_count >= self._rules.max_daily_order_count:
            return RiskCheckResult.reject(
                f"日累计开仓次数 {daily_order_count} 已达上限 {self._rules.max_daily_order_count}"
            )
        return RiskCheckResult.ok()
