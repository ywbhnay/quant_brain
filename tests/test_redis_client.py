"""
redis_client.py 单元测试

覆盖：
1. Stream 操作：xadd, xread, xack, xgroup_create, xtrim, xpending
2. Pub/Sub 操作：publish, subscribe
3. Hash 操作：hset, hgetall, hget, hdel, hexpire
4. 通用操作：setex, get, delete
5. 连接管理：connect, close, ensure_connected
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as aioredis

from quant_engine.redis_client import RedisClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client() -> RedisClient:
    return RedisClient(url="redis://127.0.0.1:6379/0")


def _mock_redis_pool():
    pool = AsyncMock()
    pool.aclose = AsyncMock()
    return pool


def _mock_redis_client():
    client = AsyncMock()
    client.ping = AsyncMock()
    client.aclose = AsyncMock()
    return client


# ---------------------------------------------------------------------------
class TestRedisClientConnect:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_connect_creates_pool(self):
        """验证 connect 创建连接池并 ping"""
        with patch("quant_engine.redis_client.aioredis.ConnectionPool") as mock_pool_cls:
            mock_pool = _mock_redis_pool()
            mock_pool_cls.from_url.return_value = mock_pool

            with patch("quant_engine.redis_client.aioredis.Redis") as mock_redis_cls:
                mock_client = _mock_redis_client()
                mock_redis_cls.return_value = mock_client

                client = _make_client()
                await client.connect()

                mock_pool_cls.from_url.assert_called_once()
                mock_redis_cls.assert_called_once_with(connection_pool=mock_pool)
                mock_client.ping.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_closes_pool_and_client(self):
        """验证 close 关闭连接池和客户端"""
        client = _make_client()
        client._client = _mock_redis_client()
        client._pool = _mock_redis_pool()

        await client.close()

        client._client.aclose.assert_awaited_once()
        client._pool.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ensure_connected_auto_connects(self):
        """验证 ensure_connected 在未连接时自动连接"""
        client = _make_client()

        with patch.object(client, "connect", new_callable=AsyncMock) as mock_connect:
            with patch("quant_engine.redis_client.aioredis.ConnectionPool") as mock_pool_cls:
                mock_pool = _mock_redis_pool()
                mock_pool_cls.from_url.return_value = mock_pool

                with patch("quant_engine.redis_client.aioredis.Redis") as mock_redis_cls:
                    mock_client = _mock_redis_client()
                    mock_redis_cls.return_value = mock_client
                    client._client = mock_client

                    result = await client.ensure_connected()
                    assert result is mock_client


# ---------------------------------------------------------------------------
class TestRedisClientStream:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_xadd_calls_xadd(self):
        """验证 xadd 写入 Stream"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xadd = AsyncMock(return_value="1234567890-0")
        client._client = mock_client

        result = await client.xadd("test_stream", {"key": "value"})

        mock_client.xadd.assert_awaited_once_with(
            "test_stream", data={"key": "value"}
        )
        assert result == "1234567890-0"

    @pytest.mark.asyncio
    async def test_xadd_with_maxlen(self):
        """验证 xadd 带 maxlen 截断"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xadd = AsyncMock(return_value="1-0")
        client._client = mock_client

        await client.xadd("test_stream", {"key": "val"}, maxlen=100)

        mock_client.xadd.assert_awaited_once_with(
            "test_stream", data={"key": "val"}, maxlen=100, approximate=True,
        )

    @pytest.mark.asyncio
    async def test_xread_calls_xread(self):
        """验证 xread 读取 Stream"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xread = AsyncMock(return_value=[("stream1", [("1-0", {"k": "v"})])])
        client._client = mock_client

        result = await client.xread({"stream1": "0"}, count=10, block=1000)

        mock_client.xread.assert_awaited_once_with(
            {"stream1": "0"}, count=10, block=1000,
        )

    @pytest.mark.asyncio
    async def test_xreadgroup_calls_xreadgroup(self):
        """验证 xreadgroup Consumer Group 模式"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xreadgroup = AsyncMock(return_value=[])
        client._client = mock_client

        result = await client.xreadgroup(
            "group1", "consumer1", {"stream1": ">"}, count=50, block=2000,
        )

        mock_client.xreadgroup.assert_awaited_once_with(
            "group1", "consumer1", {"stream1": ">"}, count=50, block=2000,
        )

    @pytest.mark.asyncio
    async def test_xack_calls_xack(self):
        """验证 xack 确认消息"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xack = AsyncMock(return_value=1)
        client._client = mock_client

        result = await client.xack("stream1", "group1", "1-0", "2-0")

        mock_client.xack.assert_awaited_once_with("stream1", "group1", "1-0", "2-0")
        assert result == 1

    @pytest.mark.asyncio
    async def test_xgroup_create_creates_group(self):
        """验证 xgroup_create 创建 Consumer Group"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xgroup_create = AsyncMock(return_value="OK")
        client._client = mock_client

        result = await client.xgroup_create("stream1", "group1")

        mock_client.xgroup_create.assert_awaited_once_with(
            "stream1", "group1", "0", mkstream=True,
        )
        assert result == "OK"

    @pytest.mark.asyncio
    async def test_xgroup_create_busygroup_returns_ok(self):
        """验证 Consumer Group 已存在时返回 OK"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xgroup_create = AsyncMock(
            side_effect=aioredis.ResponseError("BUSYGROUP Consumer Group name already exists")
        )
        client._client = mock_client

        result = await client.xgroup_create("stream1", "group1")
        assert result == "OK"

    @pytest.mark.asyncio
    async def test_xtrim_calls_xtrim(self):
        """验证 xtrim 截断 Stream"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xtrim = AsyncMock(return_value=50)
        client._client = mock_client

        result = await client.xtrim("stream1", maxlen=1000)

        mock_client.xtrim.assert_awaited_once_with("stream1", 1000, approximate=True)

    @pytest.mark.asyncio
    async def test_xpending_calls_xpending(self):
        """验证 xpending 查询待确认消息"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.xpending = AsyncMock(return_value={"pending": 5})
        client._client = mock_client

        result = await client.xpending("stream1", "group1")
        assert result == {"pending": 5}


# ---------------------------------------------------------------------------
class TestRedisClientPubSub:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_publish_calls_publish(self):
        """验证 publish 发布消息"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.publish = AsyncMock(return_value=2)
        client._client = mock_client

        result = await client.publish("channel1", '{"key": "value"}')

        mock_client.publish.assert_awaited_once_with("channel1", '{"key": "value"}')
        assert result == 2

    @pytest.mark.asyncio
    async def test_subscribe_returns_pubsub(self):
        """验证 subscribe 返回 PubSub 对象"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        # pubsub() 是同步方法，需用 MagicMock
        mock_client.pubsub = MagicMock(return_value=mock_pubsub)
        client._client = mock_client

        result = await client.subscribe("ch1", "ch2")

        mock_client.pubsub.assert_called_once()
        mock_pubsub.subscribe.assert_awaited_once_with("ch1", "ch2")
        assert result is mock_pubsub


# ---------------------------------------------------------------------------
class TestRedisClientHash:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_hset_calls_hset(self):
        """验证 hset 设置 Hash 字段"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.hset = AsyncMock(return_value=2)
        client._client = mock_client

        result = await client.hset("risk_rules", mapping={"max_loss": "1000", "max_trades": "10"})

        mock_client.hset.assert_awaited_once_with(
            "risk_rules", mapping={"max_loss": "1000", "max_trades": "10"}
        )
        assert result == 2

    @pytest.mark.asyncio
    async def test_hgetall_calls_hgetall(self):
        """验证 hgetall 获取全部字段"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.hgetall = AsyncMock(return_value={"key1": "val1", "key2": "val2"})
        client._client = mock_client

        result = await client.hgetall("risk_rules")

        assert result == {"key1": "val1", "key2": "val2"}

    @pytest.mark.asyncio
    async def test_hget_calls_hget(self):
        """验证 hget 获取单个字段"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.hget = AsyncMock(return_value="1000")
        client._client = mock_client

        result = await client.hget("risk_rules", "max_loss")
        assert result == "1000"

    @pytest.mark.asyncio
    async def test_hdel_calls_hdel(self):
        """验证 hdel 删除 Hash 字段"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.hdel = AsyncMock(return_value=1)
        client._client = mock_client

        result = await client.hdel("risk_rules", "max_loss")
        assert result == 1

    @pytest.mark.asyncio
    async def test_hexpire_calls_expire(self):
        """验证 hexpire 设置过期时间"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.expire = AsyncMock(return_value=True)
        client._client = mock_client

        result = await client.hexpire("risk_rules", 3600)
        assert result is True


# ---------------------------------------------------------------------------
class TestRedisClientGeneral:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_setex_calls_setex(self):
        """验证 setex 设置带过期的字符串"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.setex = AsyncMock(return_value=True)
        client._client = mock_client

        result = await client.setex("cache:key", 60, "value")
        assert result is True

    @pytest.mark.asyncio
    async def test_get_calls_get(self):
        """验证 get 获取字符串"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.get = AsyncMock(return_value="value")
        client._client = mock_client

        result = await client.get("cache:key")
        assert result == "value"

    @pytest.mark.asyncio
    async def test_delete_calls_delete(self):
        """验证 delete 删除 key"""
        client = _make_client()
        mock_client = _mock_redis_client()
        mock_client.delete = AsyncMock(return_value=1)
        client._client = mock_client

        result = await client.delete("key1", "key2")
        assert result == 1
