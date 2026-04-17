"""
FastAPI Data Gateway 入口

职责：
1. 注册路由 (行情、交易、账户)
2. CORS 中间件
3. 请求日志中间件
4. 数据库连接池生命周期管理
5. Uvicorn 启动配置
"""
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from web_backend.config import WebConfig
from web_backend.db import close_pool, create_pool
from web_backend.routes.account import router as account_router
from web_backend.routes.market import router as market_router
from web_backend.routes.trading import router as trading_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("web_backend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动
    WebConfig.validate()
    await create_pool()
    logger.info("Web 后端启动完成")
    yield
    # 关闭
    await close_pool()
    logger.info("Web 后端已关闭")


app = FastAPI(
    title="Quant Data Gateway",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """请求日志中间件"""
    start = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - start
    logger.info(
        f"{request.method} {request.url.path} -> {response.status_code} "
        f"({duration * 1000:.0f}ms)"
    )
    return response


# 注册路由
app.include_router(market_router)
app.include_router(trading_router)
app.include_router(account_router)


@app.get("/health")
async def health():
    """健康检查"""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "web_backend.main:app",
        host=WebConfig.HOST,
        port=WebConfig.PORT,
        workers=WebConfig.WORKERS,
        log_level="info",
    )
