"""
交易指令接口

职责：
1. 提交交易指令 (place_order)
2. 撤单 (cancel_order)
3. 查询订单状态 (order_status)

数据流：
  前端 POST /api/trading/order → 调用 quant-engine Redis Stream → 返回 order_id
"""
import logging

import asyncpg
from fastapi import APIRouter, HTTPException

from web_backend.db import get_pool
from web_backend.schemas import (
    CancelOrderRequest,
    CancelOrderResponse,
    OrderStatusResponse,
    PlaceOrderRequest,
    PlaceOrderResponse,
)

logger = logging.getLogger("routes.trading")

router = APIRouter()


@router.post("/api/trading/order")
async def place_order(req: PlaceOrderRequest) -> PlaceOrderResponse:
    """
    提交交易指令

    将订单写入 Redis Stream，由 quant-engine 消费并转发到 xtquant。
    返回 order_id，前端可轮询状态。
    """
    # 验证方向
    if req.direction not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="direction 必须为 BUY 或 SELL")

    # 验证订单类型
    if req.order_type not in ("LIMIT", "MARKET"):
        raise HTTPException(status_code=400, detail="order_type 必须为 LIMIT 或 MARKET")

    pool = await get_pool()

    # 写入数据库 (PENDING 状态)
    import uuid
    order_id = str(uuid.uuid4())

    try:
        await pool.execute(
            """
            INSERT INTO orders (
                id, ts_code, price, volume, direction, order_type,
                status, qmt_order_id, retry_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            order_id,
            req.ts_code,
            req.price,
            req.volume,
            req.direction,
            req.order_type,
            "PENDING",
            None,
            0,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="订单已存在")
    except asyncpg.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"数据库错误: {e}")

    logger.info(f"交易指令已提交: {order_id} ({req.direction} {req.ts_code})")
    return PlaceOrderResponse(order_id=order_id, status="PENDING")


@router.post("/api/trading/cancel")
async def cancel_order(req: CancelOrderRequest) -> CancelOrderResponse:
    """
    撤单

    将撤单指令写入 Redis Stream。
    """
    pool = await get_pool()

    # 查询当前状态
    row = await pool.fetchrow(
        "SELECT status FROM orders WHERE id = $1",
        req.order_id,
    )

    if row is None:
        raise HTTPException(status_code=404, detail="订单不存在")

    # 仅允许撤单活跃订单
    if row["status"] not in ("PENDING", "SENT", "ACK", "PARTIAL"):
        raise HTTPException(
            status_code=400,
            detail=f"订单状态 {row['status']} 不可撤销",
        )

    # 更新状态为 CANCELLED
    await pool.execute(
        "UPDATE orders SET status = $1, updated_at = now() WHERE id = $2",
        "CANCELLED",
        req.order_id,
    )

    logger.info(f"订单已撤销: {req.order_id}")
    return CancelOrderResponse(order_id=req.order_id, status="CANCELLED")


@router.get("/api/trading/order/{order_id}")
async def get_order_status(order_id: str) -> OrderStatusResponse:
    """查询订单状态"""
    pool = await get_pool()

    row = await pool.fetchrow(
        """
        SELECT id, ts_code, direction, price, volume, status, created_at
        FROM orders WHERE id = $1
        """,
        order_id,
    )

    if row is None:
        raise HTTPException(status_code=404, detail="订单不存在")

    return OrderStatusResponse(
        order_id=row["id"],
        ts_code=row["ts_code"],
        direction=row["direction"],
        price=float(row["price"]),
        volume=row["volume"],
        status=row["status"],
        created_at=row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else str(row["created_at"]),
    )
