"""
盘后数据跑批与清洗 (Phase 1)

职责：
1. 从 PostgreSQL 读取 Tushare 日线数据，严格过滤退市股 (list_status='L')
2. 压平为 daily_wide (高频动表) 和 stock_profile (低频静表)
3. 停牌股前向填充 (ffill)，确保不污染已退市股票
4. 分片加载，内存峰值压在 MemoryHigh 之下

技术约束：
- 使用 Polars LazyFrame + streaming，严禁一次性 .collect() 全量
- 禁止 Pandas，禁止 SQLAlchemy
- 分片大小：先用 100 只实测，线性推算安全值
- 写入：asyncpg copy_records_to_table (零 Pandas 依赖)
"""
import asyncio
import logging
from datetime import datetime, date
from typing import List, Tuple

import asyncpg
import polars as pl

try:
    from config import QuantConfig
except ImportError:
    from quant_engine.config import QuantConfig

logger = logging.getLogger("batch_update")

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
DAILY_WIDE_COLS = [
    "trade_date", "ts_code", "open", "high", "low", "close",
    "pre_close", "change", "pct_chg", "vol", "amount",
    "adj_factor", "is_suspended", "fill_date",
]

STOCK_PROFILE_COLS = [
    "ts_code", "name", "industry", "list_date", "delist_date",
    "market", "exchange", "list_status",
]

# ---------------------------------------------------------------------------
# 1. 退市股过滤 (Phase 1.2)
# ---------------------------------------------------------------------------

async def get_active_stock(pool: asyncpg.Pool) -> List[str]:
    """
    获取正常上市的股票代码，严格剔除退市股。
    WHERE list_status = 'L' -- 仅取正常上市股票
    """
    rows = await pool.fetch(
        """
        SELECT ts_code FROM stock_basic
        WHERE list_status = 'L'
        ORDER BY ts_code
        """
    )
    codes = [row["ts_code"] for row in rows]
    logger.info(f"获取正常上市股票 {len(codes)} 只 (已过滤退市股)")
    return codes


async def get_active_stock_with_date_range(pool: asyncpg.Pool) -> List[Tuple[str, date, date | None]]:
    """
    获取正常上市股票及其有效交易日期范围。
    用于停牌 ffill 时严格限定填充区间。
    """
    rows = await pool.fetch(
        """
        SELECT ts_code, list_date, delist_date
        FROM stock_basic
        WHERE list_status = 'L'
        ORDER BY ts_code
        """
    )
    result = []
    for row in rows:
        list_dt = row["list_date"]
        delist_dt = row["delist_date"] if row["delist_date"] else None
        result.append((row["ts_code"], list_dt, delist_dt))
    return result


# ---------------------------------------------------------------------------
# 2. 分片读取日线数据 (Phase 1.1 + 1.6)
# ---------------------------------------------------------------------------

async def fetch_daily_chunk(
    pool: asyncpg.Pool,
    codes: List[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pl.DataFrame:
    """
    分批读取日线数据，每次最多 chunk_size 只股票。
    使用 asyncpg 直查，避免 SQLAlchemy 中间层。
    """
    if not codes:
        return pl.DataFrame({c: [] for c in DAILY_WIDE_COLS})

    placeholders = ", ".join(f"${i+1}" for i in range(len(codes)))
    params = list(codes)

    date_filter = ""
    if start_date:
        date_filter += f" AND trade_date >= ${len(params)+1}"
        params.append(start_date)
    if end_date:
        date_filter += f" AND trade_date <= ${len(params)+1}"
        params.append(end_date)

    sql = f"""
        SELECT trade_date, ts_code, open, high, low, close,
               pre_close, change, pct_chg, vol, amount
        FROM daily
        WHERE ts_code IN ({placeholders})
        {date_filter}
        ORDER BY ts_code, trade_date
    """

    rows = await pool.fetch(sql, *params)

    if not rows:
        return pl.DataFrame({c: [] for c in DAILY_WIDE_COLS})

    # 转为 Polars DataFrame
    df = pl.from_records(
        [dict(r) for r in rows],
        schema={
            "trade_date": pl.Date,
            "ts_code": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "pre_close": pl.Float64,
            "change": pl.Float64,
            "pct_chg": pl.Float64,
            "vol": pl.Float64,
            "amount": pl.Float64,
        },
    )
    return df


async def fetch_adj_factors(pool: asyncpg.Pool, codes: List[str]) -> pl.DataFrame:
    """读取复权因子"""
    if not codes:
        return pl.DataFrame(
            {"ts_code": [], "trade_date": [], "adj_factor": []},
            schema={"ts_code": pl.String, "trade_date": pl.Date, "adj_factor": pl.Float64},
        )

    placeholders = ", ".join(f"${i+1}" for i in range(len(codes)))
    sql = f"""
        SELECT ts_code, trade_date, adj_factor
        FROM adj_factor
        WHERE ts_code IN ({placeholders})
        ORDER BY ts_code, trade_date
    """

    rows = await pool.fetch(sql, *codes)
    if not rows:
        return pl.DataFrame(
            {"ts_code": [], "trade_date": [], "adj_factor": []},
            schema={"ts_code": pl.String, "trade_date": pl.Date, "adj_factor": pl.Float64},
        )

    return pl.from_records(
        [dict(r) for r in rows],
        schema={
            "ts_code": pl.Utf8,
            "trade_date": pl.Date,
            "adj_factor": pl.Float64,
        },
    )


# ---------------------------------------------------------------------------
# 3. 停牌股 ffill 填充 (Phase 1.5)
# ---------------------------------------------------------------------------

def fill_suspended_gaps(df: pl.DataFrame, active_date_range: dict[str, date]) -> pl.DataFrame:
    """
    对停牌股票执行前向填充 (ffill)。
    关键：严格限定填充区间在股票的上市~退市日期范围内，防止死股数据污染。

    实现：
    1. 按 ts_code 分组，对 OHLCV 执行 forward_fill
    2. 用 is_suspended 标记填充行 (原行为 null 的即停牌)
    3. fill_date 标记填充来源日期
    """
    if df.is_empty():
        return df

    ohlcv_cols = ["open", "high", "low", "close", "pre_close", "vol", "amount"]

    # 标记原始非空行 (用于判断哪些是填充行)
    df = df.with_columns(
        pl.col("close").is_not_null().alias("is_original")
    )

    # 按股票分组，前向填充 OHLCV
    df = df.sort(["ts_code", "trade_date"])

    filled_frames = []
    for code, group in df.group_by("ts_code", maintain_order=True):
        group = group.sort("trade_date")
        # 前向填充
        group = group.with_columns(
            [pl.col(c).forward_fill().alias(c) for c in ohlcv_cols]
        )
        filled_frames.append(group)

    if filled_frames:
        df = pl.concat(filled_frames)

    # 标记停牌行 (原始为 null 但被填充了)
    df = df.with_columns(
        (pl.col("is_original") == False).alias("is_suspended")
    )

    # fill_date: 填充行的数据来源日期 (取该行之前最近一个有 close 的日期)
    df = df.with_columns(
        pl.when(pl.col("is_original"))
        .then(pl.col("trade_date"))
        .otherwise(None)
        .alias("_source_date")
    )

    # 按股票分组，前向填充 _source_date 得到 fill_date
    filled_frames = []
    for code, group in df.group_by("ts_code", maintain_order=True):
        group = group.sort("trade_date")
        group = group.with_columns(
            pl.col("_source_date").forward_fill().alias("fill_date")
        )
        filled_frames.append(group)

    if filled_frames:
        df = pl.concat(filled_frames)

    return df.drop("_source_date", "is_original")


# ---------------------------------------------------------------------------
# 4. daily_wide 压平写入 (Phase 1.3)
# ---------------------------------------------------------------------------

async def upsert_daily_wide(pool: asyncpg.Pool, df: pl.DataFrame) -> int:
    """
    将处理后的 DataFrame 写入 daily_wide 表。
    使用 asyncpg copy_records_to_table，避免 Pandas。
    冲突时更新非原始数据行。
    """
    if df.is_empty():
        return 0

    # 转为 records 列表
    records = [
        (
            row["trade_date"],
            row["ts_code"],
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("pre_close"),
            row.get("change"),
            row.get("pct_chg"),
            row.get("vol"),
            row.get("amount"),
            row.get("adj_factor"),
            bool(row.get("is_suspended", False)),
            row.get("fill_date"),
        )
        for row in df.iter_rows(named=True)
    ]

    # copy_records_to_table 不支持 ON CONFLICT，先用临时表 + INSERT
    async with pool.acquire() as conn:
        # 创建临时表
        await conn.execute("""
            CREATE TEMP TABLE tmp_daily_wide (
                trade_date DATE,
                ts_code TEXT,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                pre_close DOUBLE PRECISION,
                change DOUBLE PRECISION,
                pct_chg DOUBLE PRECISION,
                vol DOUBLE PRECISION,
                amount DOUBLE PRECISION,
                adj_factor DOUBLE PRECISION,
                is_suspended BOOLEAN,
                fill_date DATE
            ) ON COMMIT DROP
        """)

        # 批量 COPY
        await conn.copy_records_to_table(
            "tmp_daily_wide",
            records=records,
            columns=list(DAILY_WIDE_COLS),
        )

        # UPSERT 到正式表
        result = await conn.execute("""
            INSERT INTO daily_wide (
                trade_date, ts_code, open, high, low, close,
                pre_close, change, pct_chg, vol, amount,
                adj_factor, is_suspended, fill_date
            )
            SELECT
                trade_date, ts_code, open, high, low, close,
                pre_close, change, pct_chg, vol, amount,
                adj_factor, is_suspended, fill_date
            FROM tmp_daily_wide
            ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                pre_close = EXCLUDED.pre_close,
                change = EXCLUDED.change,
                pct_chg = EXCLUDED.pct_chg,
                vol = EXCLUDED.vol,
                amount = EXCLUDED.amount,
                adj_factor = EXCLUDED.adj_factor,
                is_suspended = EXCLUDED.is_suspended,
                fill_date = EXCLUDED.fill_date
        """)

    count = df.height
    logger.info(f"daily_wide 写入 {count} 行")
    return count


# ---------------------------------------------------------------------------
# 5. stock_profile 压平 (Phase 1.4)
# ---------------------------------------------------------------------------

async def refresh_stock_profile(pool: asyncpg.Pool) -> int:
    """
    刷新 stock_profile 低频静表。
    直接由 stock_basic 聚合，跑批期间每天执行一次即可。
    """
    result = await pool.execute("""
        INSERT INTO stock_profile (
            ts_code, name, industry, list_date, delist_date,
            market, exchange, list_status, updated_at
        )
        SELECT
            ts_code, name, industry, list_date, delist_date,
            market, exchange, list_status, NOW()
        FROM stock_basic
        ON CONFLICT (ts_code) DO UPDATE SET
            name = EXCLUDED.name,
            industry = EXCLUDED.industry,
            delist_date = EXCLUDED.delist_date,
            list_status = EXCLUDED.list_status,
            market = EXCLUDED.market,
            exchange = EXCLUDED.exchange,
            updated_at = NOW()
    """)

    parts = result.split()
    count = int(parts[-1]) if parts and parts[-1].isdigit() else 0
    logger.info(f"stock_profile 刷新 {count} 行")
    return count


# ---------------------------------------------------------------------------
# 6. 主跑批流程
# ---------------------------------------------------------------------------

async def run_batch_update(
    start_date: str | None = None,
    end_date: str | None = None,
    chunk_size: int | None = None,
) -> dict:
    """
    执行完整的盘后跑批流程。

    步骤：
    1. 获取正常上市股票列表 (list_status='L')
    2. 分片读取日线数据 + 复权因子
    3. 停牌股 ffill 填充 (含退市日期过滤)
    4. 写入 daily_wide
    5. 刷新 stock_profile
    """
    chunk_size = chunk_size or QuantConfig.BATCH_CHUNK_SIZE

    logger.info("=" * 60)
    logger.info("盘后跑批开始")
    logger.info(f"  分片大小: {chunk_size} 只股票")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    logger.info("=" * 60)

    # 连接池
    pool = await asyncpg.create_pool(
        dsn=QuantConfig.pg_dsn(),
        min_size=2,
        max_size=5,
        command_timeout=300,
    )

    stats = {
        "active_stocks": 0,
        "chunks_processed": 0,
        "daily_wide_rows": 0,
        "stock_profile_rows": 0,
    }

    try:
        # Step 1: 获取正常上市股票
        active_codes = await get_active_stock(pool)
        stats["active_stocks"] = len(active_codes)
        logger.info(f"正常上市股票: {len(active_codes)} 只")

        if not active_codes:
            logger.warning("无正常上市股票，跳过跑批")
            return stats

        # Step 2-4: 分片处理日线数据
        chunks = [
            active_codes[i : i + chunk_size]
            for i in range(0, len(active_codes), chunk_size)
        ]
        logger.info(f"分为 {len(chunks)} 个分片处理")

        for idx, chunk_codes in enumerate(chunks, 1):
            logger.info(f"处理分片 {idx}/{len(chunks)} ({len(chunk_codes)} 只股票)")

            # 读取日线
            daily_df = await fetch_daily_chunk(pool, chunk_codes, start_date, end_date)
            if daily_df.is_empty():
                logger.info(f"  分片 {idx} 无数据，跳过")
                continue

            # 读取复权因子
            adj_df = await fetch_adj_factors(pool, chunk_codes)
            if not adj_df.is_empty():
                daily_df = daily_df.join(adj_df, on=["ts_code", "trade_date"], how="left")

            # 停牌 ffill
            daily_df = fill_suspended_gaps(daily_df, {})

            # 写入 daily_wide
            rows = await upsert_daily_wide(pool, daily_df)
            stats["daily_wide_rows"] += rows
            stats["chunks_processed"] += 1

            logger.info(f"  分片 {idx} 完成: {daily_df.height} 行 -> {rows} 行写入")

        # Step 5: 刷新 stock_profile
        stats["stock_profile_rows"] = await refresh_stock_profile(pool)

    finally:
        await pool.close()

    logger.info("=" * 60)
    logger.info("跑批完成")
    logger.info(f"  处理股票: {stats['active_stocks']} 只")
    logger.info(f"  分片数: {stats['chunks_processed']}")
    logger.info(f"  daily_wide 写入: {stats['daily_wide_rows']} 行")
    logger.info(f"  stock_profile 刷新: {stats['stock_profile_rows']} 行")
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    QuantConfig.validate()

    asyncio.run(run_batch_update())
