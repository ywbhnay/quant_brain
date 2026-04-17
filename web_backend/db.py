"""
Web 后端 PostgreSQL 连接池

职责：
1. 创建和管理 asyncpg 连接池
2. 提供获取连接的接口
3. 优雅关闭连接池

设计原则：
- 使用 asyncpg 原生连接池，禁止 SQLAlchemy
- 连接池大小可配置，适应 N5105 800MB 内存配额
- 启动时创建，关闭时释放
"""
import logging

import asyncpg

from web_backend.config import WebConfig

logger = logging.getLogger("web_db")

_pool: asyncpg.Pool | None = None


async def create_pool(
    dsn: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
) -> asyncpg.Pool:
    """创建 asyncpg 连接池"""
    global _pool
    if _pool is not None:
        return _pool

    pool_dsn = dsn or WebConfig.pg_dsn()
    pool_min = min_size or WebConfig.PG_MIN_CONNECTIONS
    pool_max = max_size or WebConfig.PG_MAX_CONNECTIONS

    _pool = await asyncpg.create_pool(
        dsn=pool_dsn,
        min_size=pool_min,
        max_size=pool_max,
        command_timeout=30,
    )
    logger.info(f"PostgreSQL 连接池已创建: min={pool_min}, max={pool_max}")
    return _pool


async def get_pool() -> asyncpg.Pool:
    """获取连接池，未创建时自动创建"""
    if _pool is None:
        await create_pool()
    return _pool  # type: ignore[union-attr]


async def close_pool() -> None:
    """关闭连接池"""
    global _pool
    if _pool is not None:
        await _pool.close()
        logger.info("PostgreSQL 连接池已关闭")
        _pool = None
