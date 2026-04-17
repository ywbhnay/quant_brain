"""
账户查询接口

职责：
1. 查询账户总资产 (account)
2. 查询持仓列表 (positions)
"""
import logging

from fastapi import APIRouter, HTTPException

from web_backend.db import get_pool

logger = logging.getLogger("routes.account")

router = APIRouter()


@router.get("/api/account")
async def get_account() -> dict:
    """查询账户总资产"""
    pool = await get_pool()

    row = await pool.fetchrow(
        """
        SELECT cash, total_assets, market_value
        FROM account_summary
        LIMIT 1
        """,
    )

    if row is None:
        raise HTTPException(status_code=404, detail="账户信息不存在")

    return {
        "cash": float(row["cash"]),
        "total_assets": float(row["total_assets"]),
        "market_value": float(row["market_value"]),
    }


@router.get("/api/account/positions")
async def get_positions() -> dict:
    """查询持仓列表"""
    pool = await get_pool()

    rows = await pool.fetch(
        """
        SELECT ts_code, volume, available_volume, cost_price, market_price, market_value
        FROM positions
        WHERE volume > 0
        ORDER BY ts_code ASC
        """,
    )

    return {
        "positions": [
            {
                "ts_code": r["ts_code"],
                "volume": r["volume"],
                "available_volume": r["available_volume"],
                "cost_price": float(r["cost_price"]),
                "market_price": float(r["market_price"]),
                "market_value": float(r["market_value"]),
            }
            for r in rows
        ],
    }
