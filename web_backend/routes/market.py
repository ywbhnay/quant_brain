"""
行情数据接口

返回格式：紧凑数组 [timestamp, open, high, low, close, vol, amount]
复权因子单独返回 adj_factor 字段
"""
import logging
from typing import Any

import asyncpg
from fastapi import APIRouter, HTTPException, Query

from web_backend.db import get_pool
from web_backend.schemas import DailyBarsResponse, MinuteBarsResponse

logger = logging.getLogger("routes.market")

router = APIRouter()


@router.get("/api/market/daily/{ts_code}")
async def get_daily_bars(
    ts_code: str,
    start_date: str = Query(default=None, description="开始日期 YYYYMMDD"),
    end_date: str = Query(default=None, description="结束日期 YYYYMMDD"),
    with_adj: bool = Query(default=False, description="是否返回复权因子"),
) -> DailyBarsResponse:
    """获取日线数据"""
    # FastAPI 会正确解析 Query 默认值，但直接调用时需显式处理
    if not isinstance(with_adj, bool):
        with_adj = False
    pool = await get_pool()

    query = """
        SELECT trade_date, open, high, low, close, vol, amount
        FROM daily_wide
        WHERE ts_code = $1
    """
    params: list = [ts_code]

    if start_date:
        param_idx = len(params) + 1
        query += f" AND trade_date >= ${param_idx}"
        params.append(start_date)

    if end_date:
        param_idx = len(params) + 1
        query += f" AND trade_date <= ${param_idx}"
        params.append(end_date)

    query += " ORDER BY trade_date ASC"

    rows = await pool.fetch(query, *params)

    bars = [
        [
            row["trade_date"].isoformat() if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"]),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["vol"]),
            float(row["amount"]),
        ]
        for row in rows
    ]

    adj_factors: list[float] | None = None
    if with_adj:
        adj_rows = await pool.fetch(
            "SELECT trade_date, adj_factor FROM adj_factor WHERE ts_code = $1 ORDER BY trade_date ASC",
            ts_code,
        )
        adj_factors = [float(r["adj_factor"]) for r in adj_rows]

    return DailyBarsResponse(ts_code=ts_code, bars=bars, adj_factors=adj_factors)


@router.get("/api/market/minute/{ts_code}")
async def get_minute_bars(
    ts_code: str,
    start_time: str = Query(default=None, description="开始时间 YYYY-MM-DD HH:MM:SS"),
    end_time: str = Query(default=None, description="结束时间 YYYY-MM-DD HH:MM:SS"),
    limit: int = Query(default=1000, ge=1, le=5000),
) -> MinuteBarsResponse:
    """获取分钟线数据"""
    pool = await get_pool()

    query = "SELECT time, open, high, low, close, vol, amount FROM minute_bar WHERE ts_code = $1"
    params: list = [ts_code]

    if start_time:
        param_idx = len(params) + 1
        query += f" AND time >= ${param_idx}"
        params.append(start_time)

    if end_time:
        param_idx = len(params) + 1
        query += f" AND time <= ${param_idx}"
        params.append(end_time)

    query += " ORDER BY time ASC LIMIT ${}".format(len(params) + 1)
    params.append(limit)

    rows = await pool.fetch(query, *params)

    bars = [
        [
            row["time"].isoformat() if hasattr(row["time"], "isoformat") else str(row["time"]),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["vol"]),
            float(row["amount"]),
        ]
        for row in rows
    ]

    return MinuteBarsResponse(ts_code=ts_code, bars=bars)


@router.get("/api/market/stocks")
async def get_stock_list(
    keyword: str = Query(default="", description="搜索关键词"),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, list[dict]]:
    """获取股票列表，支持关键词搜索"""
    pool = await get_pool()

    if keyword:
        rows = await pool.fetch(
            """
            SELECT ts_code, name FROM stock_profile
            WHERE ts_code ILIKE $1 OR name ILIKE $1
            ORDER BY ts_code ASC
            LIMIT $2
            """,
            f"%{keyword}%",
            limit,
        )
    else:
        rows = await pool.fetch(
            "SELECT ts_code, name FROM stock_profile ORDER BY ts_code ASC LIMIT $1",
            limit,
        )

    return {"stocks": [{"ts_code": r["ts_code"], "name": r["name"]} for r in rows]}
