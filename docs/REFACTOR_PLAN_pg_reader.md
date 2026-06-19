# 重构计划：删除 quant_brain 中冗余的 Tushare ETL，改为读取 quant_data_pipeline 的 PG 库

## Context（为什么做这件事）

当前 `/home/quant/cokyquant/quant_brain` 和 `/home/quant/cokyquant/quant_data_pipeline` 两个项目共存：

- `quant_data_pipeline`：**生产环境每日运行的 ETL**（cron `0 18 * * *`），把 Tushare 数据增量写入远程 PG `192.168.3.11:5432 / quant_db`，累计 23 张表 / ~5800 万行 / 9+ GB。技术栈 Pandas + SQLAlchemy。
- `quant_brain`：设计为量化交易系统的上层（分析 + 决策 + 执行），**从未部署过**。它的 `quant_engine/market/fetcher.py` 又自己实现了一遍 Tushare HTTP 拉取，**只覆盖了老管道 ~15% 的功能**（仅 daily），其他 85%（adj_factor、stk_limit、suspend_d、财务、宏观、全量回填、校验、告警）都没有。

CLAUDE.md 的架构意图是 quant_brain 应该消费 ETL 已清洗好的 PG 数据，专注做分析/交易——而不是重复造 ETL 轮子。`batch_update.py` 已经是这个模式的范例（纯 PG 读写 + Polars），但 `fetcher.py` 仍然走 Tushare HTTP，架构上不一致。

本次重构：删除 `fetcher.py` 中的 Tushare 直连逻辑，换成一个从 PG 读行情的 `PGMarketReader`；让 quant_brain 直接连老管道的库（`192.168.3.11 / quant_db`），复用已有 9GB 数据。

### 用户决策（已确认）

| 决策点 | 选择 |
|---|---|
| 实时快照 `get_realtime_snapshot`（5 档盘口） | **保留 Tushare 客户端**，仅用于此（PG 无 5 档数据）|
| 模块命名 | 新建 `market/reader.py`，导出 `PGMarketReader`；`fetcher.py` 删除 |
| 数据库 | 连老管道的 `192.168.3.11 / quant_db`（默认值改掉）|
| 分钟线数据类 | reader 内部做 `str ↔ datetime` 适配，`MinuteBar` 数据类不动 |

---

## 改动清单

### 1. 新建 `quant_engine/market/reader.py` — `PGMarketReader`

```python
class PGMarketReader:
    """从 ETL 已填充的 PostgreSQL 库读取行情。替代 fetcher.py 的 Tushare HTTP 调用。"""

    def __init__(self, pool: asyncpg.Pool) -> None: ...
    async def get_daily(self, ts_code: str, start_date: str | None = None,
                        end_date: str | None = None) -> list[dict[str, Any]]: ...
    async def get_daily_batch(self, codes: list[str], start_date: str | None = None,
                              end_date: str | None = None,
                              chunk_size: int = 50) -> list[dict[str, Any]]: ...
    async def get_minute_bars(self, ts_code: str, freq: str = "1min",
                              start_date: str | None = None,
                              end_date: str | None = None) -> list[MinuteBar]: ...
    async def connect(self) -> None: ...   # no-op，pool 外部注入
    async def close(self) -> None: ...     # no-op，pool 生命周期由调用方管理
```

**SQL 来源**：

- `get_daily` / `get_daily_batch`：`SELECT ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount FROM daily WHERE ts_code = $1 [AND trade_date >= $2] [AND trade_date <= $3] ORDER BY trade_date`
- `get_minute_bars`：先查 `infra/sql/003_market_tables.sql` 确认分钟线表结构（预期是 `minute_bar` 表，列 `time` 为 timestamp）；reader 内部把 `time` 拆成 `trade_date` (YYYYMMDD) + `trade_time` (HH:MM) 填进 `MinuteBar`。如果表不存在/列不存在，方法抛出 `NotImplementedError("minute_bar table not populated by ETL")` 而不是静默返回空。

**注意**：老管道的 `daily` 表 Numeric 列是 `(30, 4)`，asyncpg 默认返回 `Decimal`，reader 用 `float()` 转换，保持和原 fetcher 返回类型一致（`list[dict]` 中 OHLCV 为 float）。

### 2. 重构 `quant_engine/market/fetcher.py` — 瘦身为仅实时快照

原 fetcher.py 有 4 个方法：`get_daily` / `get_minute_bars` / `get_realtime_snapshot` / `get_daily_batch`。重构后：

- 删除 `MarketFetcher` 类（4 方法全删）
- 删除 `TushareClient.request` 中通用的 `api_name` 调度，**仅保留 `realtime_quote` 调用**
- 重命名 `TushareClient` → `RealtimeQuoteClient`（或保留名，仅删方法）
- 保留 `RateLimiter`（`realtime_quote` 仍需要限流）
- 导出：`RealtimeQuoteClient.get_realtime_snapshot(ts_code) -> MarketSnapshot | None`

### 3. 更新 `quant_engine/market/distributor.py`

`MarketDistributor` 构造函数原签名：
```python
def __init__(self, redis_client, fetcher, stream_maxlen, active_codes): ...
```

改为：
```python
def __init__(self, redis_client, reader: PGMarketReader,
             realtime_client: RealtimeQuoteClient | None = None,
             stream_maxlen=10000, active_codes=None): ...
```

- `distribute_daily_batch` 调 `self._reader.get_daily(...)`
- `distribute_snapshot`（若有）调 `self._realtime_client.get_realtime_snapshot(...)`；如果 `realtime_client` 为 None，该方法跳过并 log warning
- `distribute_minute_bars` 签名不变（bars 由调用方传入）

### 4. 更新 `quant_engine/market/__init__.py`

```python
from quant_engine.market.reader import PGMarketReader
from quant_engine.market.fetcher import RealtimeQuoteClient, RateLimiter
from quant_engine.market.distributor import MarketDistributor
from quant_engine.market.snapshot import MarketSnapshot, MinuteBar, Level5Quote

__all__ = [
    "PGMarketReader",
    "RealtimeQuoteClient", "RateLimiter",
    "MarketDistributor",
    "MarketSnapshot", "MinuteBar", "Level5Quote",
]
```

删掉 `MarketFetcher`, `TushareClient`。

### 5. 更新 `quant_engine/config.py`

```python
class QuantConfig:
    PG_HOST: str      = os.getenv("PG_HOST", "192.168.3.11")       # 改为老管道 DB
    PG_PORT: int      = int(os.getenv("PG_PORT", "5432"))
    PG_USER: str      = os.getenv("PG_USER", "quant_user")          # 改为老管道用户
    PG_PASSWORD: str  = os.getenv("PG_PASSWORD", "")
    PG_DATABASE: str  = os.getenv("PG_DATABASE", "quant_db")        # 改为老管道库名
    # TUSHARE_TOKEN 保留（RealtimeQuoteClient 仍需要）
    # validate() 中 TUSHARE_TOKEN 仍不强制（实时快照是可选功能）
```

同时更新 `.env.example`：
```
PG_HOST=192.168.3.11
PG_USER=quant_user
PG_DATABASE=quant_db
# ...
# TUSHARE_TOKEN 仅实时快照功能需要；纯 PG 读取模式可留空
TUSHARE_TOKEN=
```

### 6. 测试改动

| 文件 | 动作 |
|---|---|
| `tests/test_market_fetcher.py` | **拆分**：① 删除 `TestRateLimiter`（保留但简化，只测 acquire 时序）；② 删除 `TestTushareClient` 中 daily/minute/batch 的 case；③ 新增 `TestRealtimeQuoteClient`（mock `_ensure_client`，仅测 `realtime_quote` 路径）；④ 新增 `TestPGMarketReader`（mock `asyncpg.Pool.fetch` 返回行列表，断言返回类型/字段/空结果处理） |
| `tests/test_market_distributor.py` | 把 `mock_fetcher` 改成 `mock_reader: PGMarketReader`；realtime_client 为可选 |
| `tests/test_market_snapshot.py` | 不动 |
| `tests/test_batch_update.py` | 不动（已正确 mock asyncpg.Pool，可作新 reader 测试的模板） |

新 `TestPGMarketReader` 测试用例（至少）：
- `test_get_daily_returns_list_of_dicts_with_float_ohlcv`
- `test_get_daily_respects_date_range`
- `test_get_daily_batch_chunks_large_code_lists`
- `test_get_minute_bars_splits_time_column`
- `test_get_minute_bars_raises_when_table_missing`

### 7. 文档更新

**`CLAUDE.md`**：
- Tech Stack 表 "Data" 行从 `Tushare Pro API` 改为 `PostgreSQL (ETL-populated by quant_data_pipeline)`
- "Key Constraints" 新增一条：`NO direct Tushare HTTP for historical data — read from PG (ETL handles Tushare)`
- "Environment Variables" 段落：把 `TUSHARE_TOKEN` 从 "Required at startup" 移到 "Optional (only for realtime snapshot)"
- PG 默认值描述更新为 `192.168.3.11 / quant_db / quant_user`

**`README.md`**：
- 架构图加一层 `[quant_data_pipeline] → PG → [quant_brain]`
- "数据获取" 行从 `Tushare Pro HTTP API` 改为 `PostgreSQL (ETL by quant_data_pipeline)`
- "快速开始" 章节增加一节 "数据库准备"：说明需要先有 quant_data_pipeline 跑过的 PG 库，或提供只读账号配置示例

---

## 不在本次范围的事

为避免范围蔓延，**明确不做**：

1. ❌ 把 `batch_update.py` 合并进 `PGMarketReader` —— 二者职责不同（batch_update 做清洗/宽表，reader 只读）
2. ❌ 修改 `quant_data_pipeline` —— 老管道维持原状
3. ❌ 改 `web_backend/` 或 `openclaw/` —— 它们走 FastAPI 路由层，不直接碰 fetcher
4. ❌ 实现 systemd 部署 / .env 实际配置 / 真实连 PG 测试 —— 仅做代码重构
5. ❌ 引入双库连接 —— 默认直连老管道的库；quant_brain 的订单/持仓表如果和老管道库冲突，**留作后续问题**（infra/sql/ 的 4 个迁移脚本是否要在新库执行，需另议）
6. ❌ 删掉 `infra/sql/001-004` 迁移脚本 —— 它们定义了 quant_brain 自己的 orders/positions/trading 表，和 ETL 库不冲突

---

## 关键文件清单（按修改顺序）

### 新增
- `quant_engine/market/reader.py` （~150 行）

### 修改
- `quant_engine/market/fetcher.py` （瘦身到 ~80 行）
- `quant_engine/market/distributor.py` （构造函数改签名）
- `quant_engine/market/__init__.py` （导出表更新）
- `quant_engine/config.py` （PG 默认值）
- `.env.example` （PG 默认值 + TUSHARE_TOKEN 注释）
- `tests/test_market_fetcher.py` （拆分 + 重写）
- `tests/test_market_distributor.py` （mock 更新）
- `CLAUDE.md` （架构描述更新）
- `README.md` （架构图 + 数据获取章节）

### 删除
- （无整文件删除，仅 fetcher.py 内的类/方法删除）

---

## 验证（按顺序执行）

1. **静态检查**：`ruff check .` + `ruff format --check .`
2. **类型检查**（如有 mypy）：`mypy quant_engine/`
3. **单元测试**：
   ```bash
   python -m pytest tests/test_market_fetcher.py tests/test_market_distributor.py \
                    tests/test_market_snapshot.py tests/test_batch_update.py -v
   ```
   期望全部通过；覆盖率目标 ≥ 80%（reader.py 必须 100%）
4. **导入检查**：
   ```bash
   python -c "from quant_engine.market import PGMarketReader, RealtimeQuoteClient, MarketDistributor; print('ok')"
   ```
5. **不连真实 PG 的端到端 smoke**：用 mock pool 跑一次 `PGMarketReader.get_daily("000001.SZ", "20260601", "20260618")`，断言返回的 dict 列表包含 float OHLCV
6. **真实 PG 集成测试（可选，需 .env 配好）**：
   ```bash
   # 仅当 PG_HOST=192.168.3.11 可达时
   python -c "
   import asyncio, asyncpg
   from quant_engine.config import QuantConfig
   from quant_engine.market.reader import PGMarketReader
   async def main():
       pool = await asyncpg.create_pool(QuantConfig.pg_dsn(), min_size=1, max_size=2)
       r = PGMarketReader(pool)
       rows = await r.get_daily('000001.SZ', '20260601', '20260618')
       print(f'got {len(rows)} rows, first: {rows[0] if rows else None}')
       await pool.close()
   asyncio.run(main())
   "
   ```
   期望：返回 ~12 行（12 个交易日），OHLCV 为 float

---

## 风险与注意事项

1. **Numeric 精度**：老库 Numeric(30,4) 经 asyncpg 返回 `Decimal`。reader 用 `float()` 转，可能引入浮点误差。如果下游对精度敏感（例如订单金额计算），需要保持 Decimal 或改用整数分。当前 fetcher 返回的就是 float（Tushare JSON 解码后），所以行为一致。

2. **分钟线表可能不存在**：`quant_data_pipeline` 的 ETL 主要覆盖日线，`minute_bar` 表可能未填充。`get_minute_bars` 必须显式处理"表不存在/空"场景，抛清晰错误而不是返回空列表误导下游。

3. **`TUSHARE_TOKEN` 仍要保留**：`RealtimeQuoteClient` 需要它。CLAUDE.md 原"Required at startup"要改成"Optional"。

4. **老管道数据库权限**：quant_brain 用 `quant_user` 连老库，需要确认该账号对 `daily / adj_factor / minute_bar / stock_basic` 等表有 SELECT 权限。本次重构不动老管道的用户/权限，但若实际连不上，是部署问题不是代码问题。

5. **订单表位置**：quant_brain 的 `infra/sql/004_orders.sql` 定义 `orders / order_fills` 等交易表。这些表目前在 quant_brain 自己的 PG 中；改成连老库后，需要确保老库里没有同名冲突表（从 `DATABASE_SCHEMA_REPORT.md` 看，老库只有行情/财务/宏观表，无 orders，**不冲突**）。

6. **向后兼容**：`distributor.py` 构造函数签名变更会破坏现有调用方。搜索确认唯一调用方在 `quant_engine/main_quant.py`（如果存在）或测试；同步更新。

---

## 完成标准

- [ ] `fetcher.py` 中除 `RealtimeQuoteClient.get_realtime_snapshot` 外的所有 Tushare HTTP 调用已删除
- [ ] `PGMarketReader` 提供 `get_daily` / `get_daily_batch` / `get_minute_bars`，返回类型与原 fetcher 一致
- [ ] `PG 默认配置` 指向 `192.168.3.11 / quant_db / quant_user`
- [ ] `distributor.py` 使用 `reader`（不是 `fetcher`）
- [ ] 所有测试通过，`test_market_fetcher.py` 重写为针对 `PGMarketReader` + `RealtimeQuoteClient`
- [ ] `CLAUDE.md` 和 `README.md` 反映新架构
- [ ] `ruff check` 无错误
