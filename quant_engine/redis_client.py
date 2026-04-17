"""
Redis 客户端封装 (redis.asyncio)

职责：
1. Stream 操作：xadd, xread, xack, xgroup_create, xtrim
2. Pub/Sub 操作：publish, subscribe
3. Hash 操作：hset, hgetall, hdel
4. 带 expire 的 write 方法 (防止缓存无限膨胀)
"""
import asyncio
import logging
from typing import Any, List, Mapping, Optional

import redis.asyncio as aioredis

from quant_engine.config import QuantConfig

logger = logging.getLogger("redis_client")

# ---------------------------------------------------------------------------
# Redis 客户端封装
# ---------------------------------------------------------------------------

class RedisClient:
    """异步 Redis 客户端，封装 Stream / Pub/Sub / Hash 操作"""

    def __init__(
        self,
        url: str | None = None,
        max_connections: int = 5,
        socket_timeout: float = 5.0,
    ):
        self.url = url or QuantConfig.redis_url()
        self._pool: aioredis.ConnectionPool | None = None
        self._client: aioredis.Redis | None = None
        self._max_connections = max_connections
        self._socket_timeout = socket_timeout

    async def connect(self) -> None:
        """创建连接池"""
        self._pool = aioredis.ConnectionPool.from_url(
            self.url,
            max_connections=self._max_connections,
            socket_timeout=self._socket_timeout,
            decode_responses=True,
        )
        self._client = aioredis.Redis(connection_pool=self._pool)
        await self._client.ping()
        logger.info(f"Redis 连接成功: {self.url}")

    async def close(self) -> None:
        """关闭连接池"""
        if self._client:
            await self._client.aclose()
        if self._pool:
            await self._pool.aclose()
        logger.info("Redis 连接已关闭")

    async def ensure_connected(self) -> aioredis.Redis:
        """确保已连接，未连接时自动连接"""
        if self._client is None:
            await self.connect()
        return self._client  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Stream 操作
    # ------------------------------------------------------------------

    async def xadd(
        self,
        stream: str,
        data: Mapping[str, str],
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        """
        向 Stream 追加消息。
        maxlen + approximate=True 使用 ~ 截断，性能更好。
        """
        client = await self.ensure_connected()
        kwargs: dict[str, Any] = {"data": data}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = approximate
        return await client.xadd(stream, **kwargs)

    async def xread(
        self,
        streams: dict[str, str],
        count: int = 100,
        block: int = 5000,
    ) -> list:
        """
        从多个 Stream 读取消息。
        block: 阻塞毫秒数，0 = 不阻塞，None = 永久阻塞。
        """
        client = await self.ensure_connected()
        return await client.xread(streams, count=count, block=block)

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int = 100,
        block: int = 5000,
    ) -> list:
        """
        Consumer Group 模式读取消息。
        streams: {"stream_name": ">"}  (">" = 新消息)
        """
        client = await self.ensure_connected()
        return await client.xreadgroup(
            group, consumer, streams, count=count, block=block,
        )

    async def xack(
        self,
        stream: str,
        group: str,
        *ids: str,
    ) -> int:
        """确认消费消息"""
        client = await self.ensure_connected()
        return await client.xack(stream, group, *ids)

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "0",
        mkstream: bool = True,
    ) -> str:
        """创建 Consumer Group"""
        client = await self.ensure_connected()
        try:
            return await client.xgroup_create(stream, group, id, mkstream=mkstream)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer Group 已存在: {stream}/{group}")
                return "OK"
            raise

    async def xtrim(
        self,
        stream: str,
        maxlen: int,
        approximate: bool = True,
    ) -> int:
        """截断 Stream，保留最近 maxlen 条消息"""
        client = await self.ensure_connected()
        return await client.xtrim(stream, maxlen, approximate=approximate)

    async def xpending(
        self,
        stream: str,
        group: str,
    ) -> dict:
        """查看待确认消息"""
        client = await self.ensure_connected()
        return await client.xpending(stream, group)

    async def xinfo_consumers(
        self,
        stream: str,
        group: str,
    ) -> list[dict]:
        """查看 Consumer Group 的消费者信息"""
        client = await self.ensure_connected()
        return await client.xinfo_consumers(stream, group)

    # ------------------------------------------------------------------
    # Pub/Sub 操作
    # ------------------------------------------------------------------

    async def publish(self, channel: str, message: str) -> int:
        """向频道发布消息"""
        client = await self.ensure_connected()
        return await client.publish(channel, message)

    async def subscribe(self, *channels: str):
        """订阅频道，返回 PubSub 对象"""
        client = await self.ensure_connected()
        pubsub = client.pubsub()
        await pubsub.subscribe(*channels)
        return pubsub

    async def psubscribe(self, *patterns: str):
        """通配符订阅"""
        client = await self.ensure_connected()
        pubsub = client.pubsub()
        await pubsub.psubscribe(*patterns)
        return pubsub

    # ------------------------------------------------------------------
    # Hash 操作
    # ------------------------------------------------------------------

    async def hset(
        self,
        key: str,
        mapping: dict[str, str] | None = None,
        **kwargs: str,
    ) -> int:
        """设置 Hash 字段"""
        client = await self.ensure_connected()
        return await client.hset(key, mapping=mapping, **kwargs)

    async def hgetall(self, key: str) -> dict:
        """获取 Hash 全部字段"""
        client = await self.ensure_connected()
        return await client.hgetall(key)

    async def hget(self, key: str, field: str) -> str | None:
        """获取 Hash 单个字段"""
        client = await self.ensure_connected()
        return await client.hget(key, field)

    async def hdel(self, key: str, *fields: str) -> int:
        """删除 Hash 字段"""
        client = await self.ensure_connected()
        return await client.hdel(key, *fields)

    async def hexpire(
        self,
        key: str,
        seconds: int,
    ) -> bool:
        """设置 Hash key 过期时间"""
        client = await self.ensure_connected()
        result = await client.expire(key, seconds)
        return bool(result)

    # ------------------------------------------------------------------
    # 通用操作
    # ------------------------------------------------------------------

    async def setex(self, key: str, seconds: int, value: str) -> bool:
        """设置带过期时间的字符串"""
        client = await self.ensure_connected()
        result = await client.setex(key, seconds, value)
        return bool(result)

    async def get(self, key: str) -> str | None:
        """获取字符串"""
        client = await self.ensure_connected()
        return await client.get(key)

    async def delete(self, *keys: str) -> int:
        """删除 key"""
        client = await self.ensure_connected()
        return await client.delete(*keys)

    async def flushdb(self) -> bool:
        """清空当前数据库 (谨慎使用)"""
        client = await self.ensure_connected()
        return await client.flushdb()
