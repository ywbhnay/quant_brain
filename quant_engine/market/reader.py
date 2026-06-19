"""
行情读取器 — 从 ETL 已填充的 PostgreSQL 读取行情

职责：
1. 从 quant_data_pipeline 已清洗好的 PG 库读取行情数据
2. 不再直接调用 Tushare HTTP API（实时快照除外，由 fetcher.py 的
   RealtimeQuoteClient 负责）
3. 使用 asyncpg 异步读取 + Polars 友好的 list[dict] 返回
4. 限流 / 重试由上游 ETL 负责，本模块不做

数据流：
  Tushare → quant_data_pipeline (ETL) → PostgreSQL (quant_db)
                                              ↓
                                       PGMarketReader (本模块)
                                              ↓
                                       MarketDistributor → Redis Stream
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

import asyncpg

from quant_engine.market.snapshot import MinuteBar

logger = logging.getLogger("market_reader")

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
# daily 表要查询的列（与 fetcher.py 旧 get_daily 字段保持一致）
DAILY_COLUMNS: tuple[str, ...] = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)

# 需要 Decimal → float 转换的列（Numeric 类型）
DECIMAL_COLUMNS: frozenset[str] = frozenset(
    {"open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"}
)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    """
    将 asyncpg.Record 转为 dict，其中 Numeric 列转为 float。

    asyncpg 对 PG Numeric 类型默认返回 Decimal；下游 (Redis Stream xadd /
    JSON 序列化) 需要 float，因此在此统一转换。
    """
    data: dict[str, Any] = dict(row)
    for col in DECIMAL_COLUMNS:
        val = data.get(col)
        if isinstance(val, Decimal):
            data[col] = float(val)
    # trade_date: PG 是 DATE 类型，asyncpg 返回 datetime.date；
    # 下游期望 YYYYMMDD 字符串，统一格式
    td = data.get("trade_date")
    if hasattr(td, "strftime"):
        data["trade_date"] = td.strftime("%Y%m%d")
    return data


# ---------------------------------------------------------------------------
# PGMarketReader
# ---------------------------------------------------------------------------


class PGMarketReader:
    """
    从 PostgreSQL 读取行情数据。

    使用方式：
        pool = await asyncpg.create_pool(QuantConfig.pg_dsn(), min_size=2, max_size=10)
        reader = PGMarketReader(pool)
        bars = await reader.get_daily("000001.SZ", "20260601", "20260618")
        # pool 的生命周期由调用方管理，reader 不负责关闭

    约定：
    - 日期参数格式均为 YYYYMMDD 字符串（如 "20260618"）
    - 返回 list[dict]，字段与旧 Tushare fetcher 兼容
    - Numeric 列返回 float（不是 Decimal）
    - trade_date 返回 YYYYMMDD 字符串（不是 datetime.date）
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        """
        Args:
            pool: asyncpg 连接池，由调用方创建并管理生命周期
        """
        self._pool = pool

    # ------------------------------------------------------------------
    # 连接管理 (no-op，保持与原 fetcher 接口兼容)
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """No-op。pool 由调用方创建，reader 不管理连接生命周期。"""
        return None

    async def close(self) -> None:
        """No-op。pool 由调用方关闭。"""
        return None

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
        读取单只股票的日线数据。

        Args:
            ts_code: 股票代码 (e.g. "000001.SZ")
            start_date: 起始日期 YYYYMMDD (包含)，可选
            end_date: 结束日期 YYYYMMDD (包含)，可选

        Returns:
            list[dict]，按 trade_date 升序，字段：
            ts_code, trade_date, open, high, low, close, pre_close,
            change, pct_chg, vol, amount
        """
        cols = ", ".join(DAILY_COLUMNS)
        conditions = ["ts_code = $1"]
        args: list[Any] = [ts_code]
        arg_idx = 2

        if start_date is not None:
            conditions.append(f"trade_date >= ${arg_idx}::date")
            args.append(self._to_pg_date(start_date))
            arg_idx += 1
        if end_date is not None:
            conditions.append(f"trade_date <= ${arg_idx}::date")
            args.append(self._to_pg_date(end_date))
            arg_idx += 1

        sql = f"SELECT {cols} FROM daily WHERE {' AND '.join(conditions)} ORDER BY trade_date ASC"

        rows = await self._pool.fetch(sql, *args)
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 批量日线
    # ------------------------------------------------------------------

    async def get_daily_batch(
        self,
        codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        chunk_size: int = 50,
    ) -> list[dict[str, Any]]:
        """
        批量读取多只股票的日线数据，按 chunk_size 分片查询。

        与旧 fetcher.get_daily_batch 行为一致：分片并发请求，失败记 warning
        但不抛异常，返回所有成功结果的并集。

        Args:
            codes: 股票代码列表
            start_date: 起始日期 YYYYMMDD (包含)
            end_date: 结束日期 YYYYMMDD (包含)
            chunk_size: 每次 IN 查询的股票数量（默认 50）

        Returns:
            list[dict]，多只股票的数据拼接，按 trade_date 升序（每只股票内）
        """
        if not codes:
            return []

        cols = ", ".join(DAILY_COLUMNS)

        # 构建日期过滤片段
        date_filters = ""
        date_args: list[Any] = []
        if start_date is not None:
            date_filters += " AND trade_date >= $2::date"
            date_args.append(self._to_pg_date(start_date))
        if end_date is not None:
            placeholder = "$3" if start_date is not None else "$2"
            date_filters += f" AND trade_date <= {placeholder}::date"
            date_args.append(self._to_pg_date(end_date))

        all_data: list[dict[str, Any]] = []

        async def _fetch_chunk(chunk: list[str]) -> list[dict[str, Any]]:
            # 动态构造 IN ($1, $2, ..., $N) + 日期条件
            placeholders = ", ".join(f"${i + 1}" for i in range(len(chunk)))
            # 日期占位符接着 IN 之后
            n_in = len(chunk)
            extra_filters = ""
            extra_args: list[Any] = []
            if start_date is not None:
                extra_filters += f" AND trade_date >= ${n_in + 1}::date"
                extra_args.append(self._to_pg_date(start_date))
            if end_date is not None:
                extra_filters += f" AND trade_date <= ${n_in + 1 + len(extra_args)}::date"
                extra_args.append(self._to_pg_date(end_date))

            sql = (
                f"SELECT {cols} FROM daily "
                f"WHERE ts_code IN ({placeholders}){extra_filters} "
                f"ORDER BY ts_code, trade_date ASC"
            )
            rows = await self._pool.fetch(sql, *chunk, *extra_args)
            return [_row_to_dict(r) for r in rows]

        # 分片并发（限制并发度避免打爆 PG 连接池）
        semaphore = asyncio.Semaphore(4)

        async def _bounded(chunk: list[str]) -> list[dict[str, Any]]:
            async with semaphore:
                return await _fetch_chunk(chunk)

        tasks = [_bounded(codes[i : i + chunk_size]) for i in range(0, len(codes), chunk_size)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for chunk_idx, res in enumerate(results):
            if isinstance(res, Exception):
                logger.warning(f"批量获取失败 (chunk {chunk_idx}): {res}")
                continue
            all_data.extend(res)

        return all_data

    # ------------------------------------------------------------------
    # 分钟线
    # ------------------------------------------------------------------

    async def get_minute_bars(
        self,
        ts_code: str,
        freq: str = "1min",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[MinuteBar]:
        """
        读取分钟线数据。

        注：quant_data_pipeline 的 ETL 主要覆盖日线；分钟线表 (minute_bar)
        由 quant_brain 自己的 infra/sql/003_market_tables.sql 定义，需要
        由盘中推送链路（Win10 miniQMT → Redis Stream → stream_consumer.py）
        填充。如果表未填充，本方法返回空列表。

        Args:
            ts_code: 股票代码
            freq: 周期（保留参数，PG 中只存 1min，其他周期需下游聚合）
            start_date: 起始日期 YYYYMMDD (包含)
            end_date: 结束日期 YYYYMMDD (包含)

        Returns:
            list[MinuteBar]，按 (trade_date, trade_time) 升序
        """
        # 先检查表是否存在（可能未建表）
        table_exists = await self._pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'minute_bar'
            )
            """
        )
        if not table_exists:
            logger.warning("minute_bar 表不存在，跳过分钟线读取 (需要由盘中推送链路填充)")
            return []

        conditions = ["ts_code = $1"]
        args: list[Any] = [ts_code]
        arg_idx = 2

        if start_date is not None:
            conditions.append(f"trade_date >= ${arg_idx}::date")
            args.append(self._to_pg_date(start_date))
            arg_idx += 1
        if end_date is not None:
            conditions.append(f"trade_date <= ${arg_idx}::date")
            args.append(self._to_pg_date(end_date))
            arg_idx += 1

        sql = (
            "SELECT ts_code, trade_date, trade_time, "
            "open, high, low, close, vol, amount "
            f"FROM minute_bar WHERE {' AND '.join(conditions)} "
            "ORDER BY trade_date ASC, trade_time ASC"
        )

        rows = await self._pool.fetch(sql, *args)

        bars: list[MinuteBar] = []
        for row in rows:
            td = row["trade_date"]
            tt = row["trade_time"]
            # trade_date: datetime.date → "YYYYMMDD"
            trade_date_str = td.strftime("%Y%m%d") if hasattr(td, "strftime") else str(td)
            # trade_time: datetime.time → "HH:MM"
            trade_time_str = tt.strftime("%H:%M") if hasattr(tt, "strftime") else str(tt)[:5]

            bars.append(
                MinuteBar(
                    ts_code=row["ts_code"],
                    trade_date=trade_date_str,
                    trade_time=trade_time_str,
                    open=self._to_float(row.get("open")),
                    high=self._to_float(row.get("high")),
                    low=self._to_float(row.get("low")),
                    close=self._to_float(row.get("close")),
                    vol=self._to_float(row.get("vol")),
                    amount=self._to_float(row.get("amount")),
                )
            )
        return bars

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _to_pg_date(yyyymmdd: str) -> str:
        """YYYYMMDD 字符串 → PG DATE 字面量 (YYYY-MM-DD)。"""
        if len(yyyymmdd) != 8 or not yyyymmdd.isdigit():
            raise ValueError(f"日期格式错误，期望 YYYYMMDD: {yyyymmdd}")
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

    @staticmethod
    def _to_float(val: Any) -> float | None:
        """Decimal / int / float → float；None 保持 None。"""
        if val is None:
            return None
        if isinstance(val, Decimal):
            return float(val)
        return float(val)
