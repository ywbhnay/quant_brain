"""
量化引擎配置管理
从环境变量读取，启动时验证必填项
"""
import os


class QuantConfig:
    """量化引擎配置，全部从环境变量读取"""

    # PostgreSQL 连接
    PG_HOST: str = os.getenv("PG_HOST", "127.0.0.1")
    PG_PORT: int = int(os.getenv("PG_PORT", "5432"))
    PG_USER: str = os.getenv("PG_USER", "postgres")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "")
    PG_DATABASE: str = os.getenv("PG_DATABASE", "quant_data")

    # Tushare API Token
    TUSHARE_TOKEN: str = os.getenv("TUSHARE_TOKEN", "")

    # Redis 连接
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")

    # 跑批配置
    BATCH_CHUNK_SIZE: int = int(os.getenv("BATCH_CHUNK_SIZE", "100"))  # 先 100 只实测
    BATCH_MAX_MEMORY_MB: int = int(os.getenv("BATCH_MAX_MEMORY_MB", "1500"))  # MemoryHigh 软限

    @classmethod
    def pg_uri(cls) -> str:
        return f"postgresql+psycopg2://{cls.PG_USER}:{cls.PG_PASSWORD}@{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DATABASE}"

    @classmethod
    def pg_uri_asyncpg(cls) -> str:
        return f"postgresql+asyncpg://{cls.PG_USER}:{cls.PG_PASSWORD}@{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DATABASE}"

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
