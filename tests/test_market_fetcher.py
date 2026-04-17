"""
market/fetcher.py 单元测试

覆盖：
1. RateLimiter.acquire — 限流逻辑
2. TushareClient.request — HTTP 请求、限流、错误处理
3. MarketFetcher.get_daily — 日线数据获取
4. MarketFetcher.get_minute_bars — 分钟线解析
5. MarketFetcher.get_realtime_snapshot — 快照解析
6. MarketFetcher.get_daily_batch — 批量分片获取
"""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from quant_engine.market.fetcher import RateLimiter, TushareClient, MarketFetcher
from quant_engine.market.snapshot import MarketSnapshot, MinuteBar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tushare_response(fields, items, code=0, msg=""):
    """构造 Tushare 标准返回格式"""
    return {
        "code": code,
        "msg": msg,
        "data": {"fields": fields, "items": items},
    }


# ---------------------------------------------------------------------------
class TestRateLimiter:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_acquire_returns_immediately_first_call(self):
        """首次调用应立即返回（无等待）"""
        limiter = RateLimiter(rate=100.0)  # 高速率 = 短间隔
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05  # 应该几乎立即返回

    @pytest.mark.asyncio
    async def test_acquire_enforces_interval(self):
        """连续调用应受到间隔限制"""
        limiter = RateLimiter(rate=10.0)  # 0.1s 间隔
        await limiter.acquire()
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.08  # 至少等待约 0.1s（允许误差）

    @pytest.mark.asyncio
    async def test_acquire_is_threadsafe_within_asyncio(self):
        """多次并发调用应序列化"""
        limiter = RateLimiter(rate=100.0)
        calls = await asyncio.gather(*[limiter.acquire() for _ in range(5)])
        assert len(calls) == 5


# ---------------------------------------------------------------------------
class TestTushareClient:
# ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_request_success(self):
        """验证请求成功返回"""
        client = TushareClient(token="test_token")

        mock_response = MagicMock()
        mock_response.json.return_value = _make_tushare_response(
            ["ts_code", "price"], [["000001.SZ", 10.5]]
        )
        mock_response.raise_for_status = MagicMock()

        mock_httpx_client = AsyncMock()
        mock_httpx_client.post.return_value = mock_response

        with patch.object(client, "_ensure_client", return_value=mock_httpx_client):
            with patch.object(client, "_limiter", AsyncMock(acquire=AsyncMock())):
                result = await client.request("realtime_quote", params={"ts_code": "000001.SZ"})

        assert result["code"] == 0
        assert result["data"]["items"][0][1] == 10.5

    @pytest.mark.asyncio
    async def test_request_raises_on_error_code(self):
        """验证 API 返回错误码时抛出 RuntimeError"""
        client = TushareClient(token="test_token")

        mock_response = MagicMock()
        mock_response.json.return_value = _make_tushare_response(
            [], [], code=-1, msg="Invalid token"
        )
        mock_response.raise_for_status = MagicMock()

        mock_httpx_client = AsyncMock()
        mock_httpx_client.post.return_value = mock_response

        with patch.object(client, "_ensure_client", return_value=mock_httpx_client):
            with patch.object(client, "_limiter", AsyncMock(acquire=AsyncMock())):
                with pytest.raises(RuntimeError, match="Invalid token"):
                    await client.request("daily")

    @pytest.mark.asyncio
    async def test_request_raises_on_http_error(self):
        """验证 HTTP 错误时抛出异常"""
        client = TushareClient(token="test_token")

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(side_effect=Exception("500 Server Error"))

        mock_httpx_client = AsyncMock()
        mock_httpx_client.post.return_value = mock_response

        with patch.object(client, "_ensure_client", return_value=mock_httpx_client):
            with patch.object(client, "_limiter", AsyncMock(acquire=AsyncMock())):
                with pytest.raises(Exception, match="500"):
                    await client.request("daily")

    @pytest.mark.asyncio
    async def test_close_closes_client(self):
        """验证 close 关闭 httpx 客户端"""
        client = TushareClient(token="test_token")
        mock_httpx = AsyncMock()
        mock_httpx.aclose = AsyncMock()
        client._client = mock_httpx

        await client.close()
        mock_httpx.aclose.assert_awaited_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_close_noop_when_no_client(self):
        """验证无客户端时 close 不报错"""
        client = TushareClient(token="test_token")
        await client.close()  # Should not raise


# ---------------------------------------------------------------------------
class TestMarketFetcher:
# ---------------------------------------------------------------------------

    def _make_fetcher_with_mock_client(self):
        """创建 MarketFetcher 并 mock 内部 TushareClient"""
        fetcher = MarketFetcher(token="test_token")
        mock_response_data = _make_tushare_response(
            ["ts_code", "trade_date", "open", "high", "low", "close",
             "pre_close", "change", "pct_chg", "vol", "amount"],
            [
                ["000001.SZ", "20240102", 10.0, 10.5, 9.8, 10.2,
                 9.7, 0.5, 5.15, 1000.0, 10200.0],
            ],
        )
        return fetcher, mock_response_data

    @pytest.mark.asyncio
    async def test_get_daily(self):
        """验证日线数据获取"""
        fetcher, response_data = self._make_fetcher_with_mock_client()

        async def mock_request(*args, **kwargs):
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            result = await fetcher.get_daily("000001.SZ", start_date="20240101", end_date="20240103")

        assert len(result) == 1
        assert result[0]["ts_code"] == "000001.SZ"
        assert result[0]["trade_date"] == "20240102"
        assert result[0]["close"] == 10.2

    @pytest.mark.asyncio
    async def test_get_daily_no_date_params(self):
        """验证不传日期参数时只传 ts_code"""
        fetcher, response_data = self._make_fetcher_with_mock_client()

        captured_params = {}

        async def mock_request(api_name, params=None, fields=None):
            captured_params["params"] = params
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            await fetcher.get_daily("000001.SZ")

        assert captured_params["params"] == {"ts_code": "000001.SZ"}

    @pytest.mark.asyncio
    async def test_get_minute_bars(self):
        """验证分钟线数据解析"""
        fetcher = MarketFetcher(token="test_token")

        response_data = _make_tushare_response(
            ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
            [
                ["000001.SZ", "20240102 09:31:00", 10.0, 10.1, 9.9, 10.05, 500.0, 5000.0],
                ["000001.SZ", "20240102 09:32:00", 10.05, 10.2, 10.0, 10.15, 600.0, 6060.0],
            ],
        )

        async def mock_request(*args, **kwargs):
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            bars = await fetcher.get_minute_bars("000001.SZ", freq="1min")

        assert len(bars) == 2
        assert isinstance(bars[0], MinuteBar)
        assert bars[0].ts_code == "000001.SZ"
        assert bars[0].trade_date == "20240102"
        assert bars[0].trade_time == "09:31"
        assert bars[0].close == 10.05

    @pytest.mark.asyncio
    async def test_get_minute_bars_no_time_in_trade_date(self):
        """验证 trade_date 无时间部分时默认 00:00"""
        fetcher = MarketFetcher(token="test_token")

        response_data = _make_tushare_response(
            ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
            [["000001.SZ", "20240102", 10.0, 10.1, 9.9, 10.05, 500.0, 5000.0]],
        )

        async def mock_request(*args, **kwargs):
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            bars = await fetcher.get_minute_bars("000001.SZ")

        assert len(bars) == 1
        assert bars[0].trade_time == "00:00"

    @pytest.mark.asyncio
    async def test_get_realtime_snapshot(self):
        """验证实时快照获取"""
        fetcher = MarketFetcher(token="test_token")

        response_data = _make_tushare_response(
            ["ts_code", "price", "change", "pct_chg", "vol", "amount",
             "b1_v", "b1_p", "b2_v", "b2_p", "b3_v", "b3_p", "b4_v", "b4_p", "b5_v", "b5_p",
             "a1_v", "a1_p", "a2_v", "a2_p", "a3_v", "a3_p", "a4_v", "a4_p", "a5_v", "a5_p"],
            [
                ["000001.SZ", 10.5, 0.5, 5.0, 1000.0, 10500.0,
                 100, 10.4, 200, 10.3, 300, 10.2, 400, 10.1, 500, 10.0,
                 150, 10.6, 250, 10.7, 350, 10.8, 450, 10.9, 550, 11.0],
            ],
        )

        async def mock_request(*args, **kwargs):
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            snap = await fetcher.get_realtime_snapshot("000001.SZ")

        assert snap is not None
        assert isinstance(snap, MarketSnapshot)
        assert snap.ts_code == "000001.SZ"
        assert snap.price == 10.5
        assert snap.bids[0].price == 10.4
        assert snap.bids[0].vol == 100
        assert snap.asks[0].price == 10.6
        assert snap.asks[0].vol == 150

    @pytest.mark.asyncio
    async def test_get_realtime_snapshot_empty(self):
        """验证无数据时返回 None"""
        fetcher = MarketFetcher(token="test_token")

        response_data = _make_tushare_response([], [])

        async def mock_request(*args, **kwargs):
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            result = await fetcher.get_realtime_snapshot("000001.SZ")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_daily_batch(self):
        """验证批量分片获取"""
        fetcher = MarketFetcher(token="test_token")

        response_data = _make_tushare_response(
            ["ts_code", "trade_date", "open", "high", "low", "close",
             "pre_close", "change", "pct_chg", "vol", "amount"],
            [["000001.SZ", "20240102", 10.0, 10.5, 9.8, 10.2, 9.7, 0.5, 5.15, 1000.0, 10200.0]],
        )

        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            codes = ["000001.SZ", "000002.SZ", "000003.SZ"]
            result = await fetcher.get_daily_batch(codes, chunk_size=2)

        # 3 只股票，chunk_size=2 → 2 次请求
        assert call_count == 2
        assert len(result) == 2  # 每次返回 1 条

    @pytest.mark.asyncio
    async def test_get_daily_batch_handles_error(self):
        """验证批量获取中单次失败不中断"""
        fetcher = MarketFetcher(token="test_token")

        response_data = _make_tushare_response(
            ["ts_code", "trade_date", "open", "high", "low", "close",
             "pre_close", "change", "pct_chg", "vol", "amount"],
            [["000001.SZ", "20240102", 10.0, 10.5, 9.8, 10.2, 9.7, 0.5, 5.15, 1000.0, 10200.0]],
        )

        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("API error")
            return response_data

        with patch.object(fetcher._client, "request", side_effect=mock_request):
            codes = ["000001.SZ", "000002.SZ"]
            result = await fetcher.get_daily_batch(codes, chunk_size=1)

        # 第一次失败，第二次成功
        assert call_count == 2
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_connect_and_close(self):
        """验证 connect/close 生命周期"""
        fetcher = MarketFetcher(token="test_token")
        with patch.object(fetcher._client, "_ensure_client", new_callable=AsyncMock):
            await fetcher.connect()
        await fetcher.close()  # Should not raise
