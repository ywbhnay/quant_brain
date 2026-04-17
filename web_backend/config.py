"""
Web 后端配置管理
从环境变量读取，启动时验证必填项
"""
import os


class WebConfig:
    """Web 后端配置，全部从环境变量读取"""

    # FastAPI 服务配置
    HOST: str = os.getenv("WEB_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("WEB_PORT", "8000"))
    WORKERS: int = int(os.getenv("WEB_WORKERS", "2"))

    # PostgreSQL 连接
    PG_HOST: str = os.getenv("PG_HOST", "127.0.0.1")
    PG_PORT: int = int(os.getenv("PG_PORT", "5432"))
    PG_USER: str = os.getenv("PG_USER", "postgres")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "")
    PG_DATABASE: str = os.getenv("PG_DATABASE", "quant_data")

    # Redis 连接
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")

    # 连接池配置
    PG_MIN_CONNECTIONS: int = int(os.getenv("PG_MIN_CONNECTIONS", "2"))
    PG_MAX_CONNECTIONS: int = int(os.getenv("PG_MAX_CONNECTIONS", "10"))

    @classmethod
    def pg_dsn(cls) -> str:
        """asyncpg 直连 DSN"""
        return f"postgres://{cls.PG_USER}:{cls.PG_PASSWORD}@{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DATABASE}"

    @classmethod
    def redis_url(cls) -> str:
        if cls.REDIS_PASSWORD:
            return f"redis://:{cls.REDIS_PASSWORD}@{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"

    @classmethod
    def validate(cls) -> None:
        """启动时验证必填配置"""
        required = {
            "PG_HOST": cls.PG_HOST,
            "PG_USER": cls.PG_USER,
            "PG_PASSWORD": cls.PG_PASSWORD,
            "PG_DATABASE": cls.PG_DATABASE,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(f"缺少必填配置: {', '.join(missing)}")
