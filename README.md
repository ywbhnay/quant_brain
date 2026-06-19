# Quant Brain — A 股量化交易系统

> 轻量级量化交易平台，运行于 N5105 (4C/8GB) 低功耗设备。由三个独立组件构成：**量化引擎**、**FastAPI 网关**、**AI Agent 交互层**。

## 架构

```
Tushare Pro API
       ↓ (每日 18:00 UTC, cron)
[quant_data_pipeline]  ← 老项目，生产运行中 (Pandas + SQLAlchemy)
       ↓ INSERT
PostgreSQL @ 192.168.3.11:5432 (quant_db, ~9GB, 23 tables)
       ↓ (asyncpg 读取)
[quant_brain.quant_engine]  ← 本项目 (Polars + asyncpg)
       ↓ (Redis Stream)
    [miniQMT (Win VM)] ← Redis Stream ← [web_backend] ← HTTP ← [openclaw agent] ← 用户
       ↓ (可选)
[RealtimeQuoteClient]  ← 仅 5 档盘口直接调 Tushare realtime_quote
```

### 技术栈

| 层次 | 技术选型 |
|------|---------|
| 数据源 | PostgreSQL (ETL-populated by quant_data_pipeline) |
| 数据处理 | Polars (LazyFrame + streaming) |
| 数据库 | PostgreSQL 15 + asyncpg |
| 消息队列 | Redis 7 (Stream + Pub/Sub) |
| Web API | FastAPI + uvicorn |
| 订单执行 | xtquant miniQMT 协议 |
| AI 交互 | OpenClaw Agent + httpx |
| 实时快照 | Tushare realtime_quote (5 档盘口) |
| 部署 | systemd + cgroup v2 |

## 快速开始

### 系统要求

- Linux (Ubuntu/Debian)
- Python 3.11+
- PostgreSQL 15+
- Redis 7+

### 安装

```bash
# 1. 前置：先确保 PG 数据库已就绪
#    推荐做法：先部署 quant_data_pipeline 跑一次全量回填，
#    让 PG 库 (192.168.3.11 / quant_db) 积累历史数据。
#    详见 https://github.com/ywbhnay/quant_data_pipeline (私有)

# 2. 一键初始化（创建用户、venv、Redis 配置、systemd 服务）
sudo bash scripts/setup.sh

# 3. 数据库迁移（在 quant_db 执行，添加 quant_brain 自己的 orders/trading 表）
psql -h 192.168.3.11 -U quant_user -d quant_db -f infra/sql/001_base_tables.sql
psql -h 192.168.3.11 -U quant_user -d quant_db -f infra/sql/002_derived_tables.sql
psql -h 192.168.3.11 -U quant_user -d quant_db -f infra/sql/003_market_tables.sql
psql -h 192.168.3.11 -U quant_user -d quant_db -f infra/sql/004_orders.sql

# 4. 配置环境变量
cp .env.example .env
# 编辑 .env，填入 PG_PASSWORD；TUSHARE_TOKEN 可选（仅实时快照需要）
```

### 启动

```bash
# 启动全部服务
sudo systemctl enable --now fastapi-web quant-engine openclaw-agent

# 查看状态
sudo systemctl status fastapi-web quant-engine openclaw-agent
```

### 运行测试

```bash
pip install pytest pytest-cov pytest-asyncio
python -m pytest tests/ -v --cov=quant_engine --cov=web_backend --cov=openclaw
```

## 核心模块

### 量化引擎 (`quant_engine/`)

- **行情读取**：`PGMarketReader` 从 ETL 已填充的 PG 库读取历史行情（日线/分钟线）；`RealtimeQuoteClient` 直连 Tushare 获取 5 档实时快照
- **跑批系统**：Polars streaming 处理，内存 < 1.5GB
- **订单状态机**：7 状态显式转换矩阵 (`PENDING → SENT → ACK → FILLED/PARTIAL/REJECTED/CANCELLED`)
- **风控检查器**：单笔上限、日累计上限、标的黑名单，规则存 Redis Hash 支持热更新
- **订单执行器**：生产环境对接 xtquant miniQMT，开发环境自动 Mock

### Web 网关 (`web_backend/`)

- FastAPI 异步 REST API，2 workers + uvloop
- 行情数据返回紧凑数组 `[timestamp, open, high, low, close, vol, amount]`
- 复权计算下放前端 JS，后端仅返回 `adj_factor`

### AI Agent (`openclaw/`)

- 自然语言交互：查行情、查账户、下订单
- 交易命令带二次确认流程
- 通过 HTTP 调用 FastAPI 获取数据

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/market/daily/{ts_code}` | 日线数据 |
| `GET` | `/api/market/minute/{ts_code}` | 分钟线数据 |
| `GET` | `/api/market/stocks` | 搜索股票 |
| `POST` | `/api/trading/order` | 提交订单 |
| `POST` | `/api/trading/cancel` | 撤单 |
| `GET` | `/api/trading/order/{order_id}` | 查询订单 |
| `GET` | `/api/account` | 账户总资产 |
| `GET` | `/api/account/positions` | 持仓列表 |

完整文档见 [`docs/API.md`](docs/API.md)。

## 项目结构

```
quant_brain/
├── infra/          # 基础设施 (SQL 迁移、systemd、Redis 配置)
├── quant_engine/   # 量化计算引擎
├── web_backend/    # FastAPI 数据网关
├── openclaw/       # OpenClaw AI Agent
├── tests/          # 测试 (80%+ 覆盖率)
├── scripts/        # 运维脚本
└── docs/           # 文档
```

## 关键决策

1. **asyncpg** — FastAPI + uvloop 下唯一原生异步 PG 驱动
2. **Polars streaming** — 严禁 `.collect()` 全量加载
3. **Redis Stream 直连** — Win10 miniQMT 直连 Ubuntu Redis，无额外 HTTP 服务
4. **前端复权** — 纯 JS 标量乘法，禁止 WASM
5. **Redis Hash 热配置** — 风控规则盘中热更新
6. **ETL 分离** — 历史行情由 `quant_data_pipeline` 拉取并填充 PG；quant_brain 只读取 PG，不直接调 Tushare（除实时 5 档快照）

详见 [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) 和 [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)。

## License

MIT
