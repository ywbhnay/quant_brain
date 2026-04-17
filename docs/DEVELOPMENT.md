# 量化中枢系统 — 开发文档

> 硬件底座：N5105 (4核) + 8GB 内存 PVE VM
> 数据中枢：远程 PostgreSQL 15 (~9GB Tushare A 股数据)
> 交易网关：远程 Win10 VM + miniQMT (xtquant)
> 核心原则：极简、轻量、金融级安全

---

## 一、项目概览

量化中枢系统是一个面向 A 股市场的轻量级量化交易平台，由三个独立组件构成：

```
Tushare API → [quant_engine] → PostgreSQL
                                   ↓
                              [web_backend] ← HTTP → [openclaw agent] ← 自然语言 → 用户
                                   ↓
                              miniQMT (Windows VM) ← Redis Stream
```

### 技术栈

| 层次 | 技术选型 | 理由 |
|------|---------|------|
| 数据获取 | Tushare Pro HTTP API | 中国 A 股数据源，支持日线/分钟线/复权因子 |
| 数据处理 | Polars (LazyFrame + streaming) | 内存高效，比 Pandas 快 5-10x |
| 数据库 | PostgreSQL 15 + asyncpg | 原生异步驱动，零 ORM 开销 |
| 消息队列 | Redis 7 (Stream + Pub/Sub) | 持久化消息队列，ACK 闭环 |
| Web API | FastAPI + uvicorn + asyncpg | 异步高性能，Pydantic v2 校验 |
| 订单执行 | xtquant miniQMT 协议 | 国盛 QMT 量化交易接口 |
| AI 交互 | OpenClaw Agent + httpx | 自然语言指令解析 |
| 部署 | systemd + cgroup v2 | 内存管控，进程守护 |

### 架构决策

1. **asyncpg** — FastAPI + uvloop 下唯一原生异步 PG 驱动，禁止 SQLAlchemy
2. **Polars streaming** — 严禁 `.collect()` 全量加载，必须 LazyFrame + `streaming=True`
3. **Redis Stream 直连** — Win10 miniQMT 直连 Ubuntu Redis 6379，禁止 TCP/HTTP 桥接
4. **前端复权** — 后端仅返回 `adj_factor`，复权计算由前端 JS 完成，禁止 WASM
5. **Redis Hash 热配置** — 风控规则/策略参数存 Redis，支持盘中热更新

---

## 二、目录结构

```
quant_brain/
├── infra/                          # 基础设施配置
│   ├── sql/
│   │   ├── 001_base_tables.sql     # stock_basic, daily, adj_factor
│   │   ├── 002_derived_tables.sql  # daily_wide, stock_profile
│   │   ├── 003_market_tables.sql   # minute_bar, market_snapshot
│   │   └── 004_orders.sql          # orders 表
│   ├── systemd/
│   │   ├── fastapi-web.service     # Web 网关服务
│   │   ├── quant-engine.service    # 量化引擎服务
│   │   └── openclaw-agent.service  # AI Agent 服务
│   ├── redis.conf                  # Redis 配置 (1GB maxmemory, AOF)
│   └── journald.conf               # 日志限制 (500M, 7天)
│
├── web_backend/                    # FastAPI 数据网关
│   ├── main.py                     # 入口 + lifespan + CORS + 路由注册
│   ├── config.py                   # WebConfig (PG/Redis 环境变量)
│   ├── db.py                       # asyncpg 连接池 (lazy init)
│   ├── schemas.py                  # Pydantic v2 请求/响应模型
│   └── routes/
│       ├── market.py               # GET /api/market/daily, /minute, /stocks
│       ├── trading.py              # POST /api/trading/order, /cancel
│       └── account.py              # GET /api/account, /positions
│
├── quant_engine/                   # 量化计算引擎
│   ├── config.py                   # WebConfig + TUSHARE/BATCH 配置
│   ├── redis_client.py             # Redis 封装 (Stream + Pub/Sub + Hash)
│   ├── batch_update.py             # 盘后跑批 (Polars streaming)
│   ├── market/
│   │   ├── fetcher.py              # Tushare 行情获取器 (异步 HTTP)
│   │   ├── distributor.py          # Redis pub/sub + Stream 分发器
│   │   └── snapshot.py             # 实时行情快照模型
│   ├── order/
│   │   ├── state_machine.py        # 7 状态订单机 + 转换矩阵
│   │   └── executor.py             # 订单执行器 (xtquant + mock)
│   └── risk/
│       └── checker.py              # 风控检查器 (Redis 热加载规则)
│
├── openclaw/                       # OpenClaw AI Agent
│   ├── agent_main.py               # ApiClient (httpx + FastAPI HTTP 封装)
│   ├── chat.py                     # ChatHandler (命令路由 + 对话逻辑)
│   └── commands/
│       ├── market.py               # 自然语言行情命令解析
│       ├── account.py              # 自然语言账户/持仓命令解析
│       └── trade.py                # 自然语言交易命令解析
│
├── tests/                          # 测试 (254 tests, 80%+ 覆盖率)
│   ├── test_batch_update.py        # 跑批逻辑测试
│   ├── test_redis_client.py        # Redis 操作测试
│   ├── test_market_fetcher.py      # Tushare API 调用测试
│   ├── test_market_distributor.py  # 分发流程测试
│   ├── test_market_snapshot.py     # 快照序列化测试
│   ├── test_order_state_machine.py # 状态机转换测试
│   ├── test_risk_checker.py        # 风控规则测试
│   └── test_openclaw_commands.py   # 自然语言命令测试
│
├── scripts/
│   ├── setup.sh                    # 一键环境初始化
│   └── backfill_retired_stocks.py  # 退市股清理脚本
│
└── docs/
    ├── IMPLEMENTATION_PLAN.md      # 分阶段实施计划
    ├── DEVELOPMENT.md              # 本文档
    └── API.md                      # REST API 接口文档
```

---

## 三、环境搭建

### 系统要求

- Linux (Ubuntu/Debian 推荐)
- Python 3.11+
- PostgreSQL 15+ (远程可达)
- Redis 7+
- systemd (服务管理)

### 一键安装

```bash
sudo bash scripts/setup.sh
```

该脚本完成：
1. 创建 `stock` 系统用户
2. 创建 `/opt/stock_sys` 目录结构
3. 创建 3 个 Python venv 并安装基础依赖
4. 配置 Redis (1GB maxmemory, AOF, volatile-lru)
5. 配置 journald 日志限制 (500M, 7天)
6. 部署 systemd service 文件

### 手动安装

#### 1. 数据库初始化

```bash
# 按顺序执行 SQL 迁移
psql -U stock -d quant_db -f infra/sql/001_base_tables.sql
psql -U stock -d quant_db -f infra/sql/002_derived_tables.sql
psql -U stock -d quant_db -f infra/sql/003_market_tables.sql
psql -U stock -d quant_db -f infra/sql/004_orders.sql
```

#### 2. Python 虚拟环境

```bash
# Web 后端
python3 -m venv venv_web
source venv_web/bin/activate
pip install fastapi uvicorn asyncpg httpx python-dotenv pydantic

# 量化引擎
python3 -m venv venv_quant
source venv_quant/bin/activate
pip install polars redis asyncio httpx python-dotenv asyncpg

# OpenClaw Agent
python3 -m venv venv_claw
source venv_claw/bin/activate
pip install httpx python-dotenv openclaw-sdk
```

#### 3. Redis 配置

```bash
# 使用项目配置启动
sudo cp infra/redis.conf /etc/redis/redis.conf
sudo systemctl restart redis
```

关键配置项：
- `maxmemory 1gb`
- `maxmemory-policy volatile-lru`
- `appendonly yes`
- `auto-aof-rewrite-min-size 128mb`
- `auto-aof-rewrite-percentage 100`

#### 4. 环境变量

创建 `.env` 文件（参考 `.env.example`）：

```bash
# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_USER=stock
PG_PASSWORD=your_password
PG_DATABASE=quant_db

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Tushare
TUSHARE_TOKEN=your_tushare_token

# 批量处理
BATCH_CHUNK_SIZE=100
BATCH_MAX_MEMORY_MB=1500
```

---

## 四、组件详解

### 4.1 Web 后端 (web_backend/)

FastAPI 异步 Web 网关，提供 REST API 给前端和 OpenClaw Agent。

#### 启动流程

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    WebConfig.validate()    # 验证环境变量
    await create_pool()     # 创建 asyncpg 连接池
    logger.info("Web 后端启动完成")
    yield
    await close_pool()      # 优雅关闭
```

#### 路由注册

- `market_router` — 行情数据查询 (`/api/market/*`)
- `trading_router` — 交易指令 (`/api/trading/*`)
- `account_router` — 账户信息 (`/api/account/*`)

#### 数据库连接池

```python
# lazy 初始化，首次请求时创建
pool = None

async def get_pool():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(dsn=WebConfig.pg_dsn(), min_size=2, max_size=10)
    return pool
```

### 4.2 量化引擎 (quant_engine/)

#### 盘后跑批 (batch_update.py)

核心流程：

```
获取活跃股票列表 (list_status='L')
  → 分片读取日线数据 (Polars LazyFrame + streaming)
    → 填充停牌缺口 (forward-fill OHLCV)
      → UPSERT 到 daily_wide (temp table + COPY + INSERT ON CONFLICT)
        → 刷新 stock_profile
```

关键设计：
- **分片读取**：按 `BATCH_CHUNK_SIZE` (默认 100) 分批，避免 OOM
- **停牌填充**：向前填充 OHLCV，但不会填充到退市日期之后
- **UPSERT 优化**：使用临时表 + COPY + INSERT ON CONFLICT，比逐行 upsert 快 10x

#### 订单状态机 (order/state_machine.py)

7 个状态 + 显式转换矩阵：

```
PENDING → SENT | REJECTED | CANCELLED
SENT    → ACK  | REJECTED | CANCELLED
ACK     → FILLED | PARTIAL | CANCELLED
PARTIAL → FILLED | CANCELLED
FILLED  → (终态)
REJECTED → (终态)
CANCELLED → (终态)
```

任何非法转换都会抛出 `InvalidTransitionError`。

#### 订单执行器 (order/executor.py)

下单流程：
```
风控检查 → 创建 PENDING 订单 → 持久化到 PostgreSQL
  → 状态转换为 SENT → 写入 Redis Stream `trade_orders`
    → miniQMT (Windows) XREADGROUP 消费 → 下单 → XACK
```

- 生产环境使用 `XtquantAdapter` (Windows miniQMT)
- 开发环境使用 `MockXtquantAdapter` (Linux 模拟)

#### 风控检查器 (risk/checker.py)

4 项风控检查：

| 检查项 | 默认值 | 说明 |
|-------|--------|------|
| 黑名单 | 空 | 禁止交易的标的 |
| 单笔上限 | 100,000 元 | 单笔订单最大金额 |
| 日累计上限 | 500,000 元 | 当日累计最大金额 |
| 日订单数上限 | 50 笔 | 当日最大订单数 |

规则从 Redis Hash `risk_rules` 加载，支持盘中热更新。

### 4.3 OpenClaw Agent (openclaw/)

AI 交互层，通过自然语言与系统交互。

#### 命令路由

| 意图 | 关键词 | 处理模块 |
|------|--------|---------|
| 查行情 | 行情/日线/分钟线/股票 | commands/market.py |
| 查账户 | 账户/资产/持仓 | commands/account.py |
| 交易 | 买入/卖出/撤单/订单 | commands/trade.py |

#### 自然语言解析

使用正则表达式提取结构化信息：
- 股票代码：`(\d{6})\.(SZ|SH)`
- 数量：`(\d+)\s*股`
- 价格：`价格\s*([\d.]+)`
- 日期：`(\d{4}-\d{2}-\d{2})`

#### 交易确认流程

用户下达交易指令 → 解析参数 → 生成 `TradeConfirmation` → 用户确认 → 调用 FastAPI 下单

---

## 五、数据库 Schema

### 5.1 基础表 (001_base_tables.sql)

| 表名 | 说明 | 主键 |
|------|------|------|
| `stock_basic` | 股票基本信息 | ts_code |
| `daily` | 日线行情 (Tushare 原始格式) | id |
| `adj_factor` | 复权因子 | ts_code, trade_date |

### 5.2 派生表 (002_derived_tables.sql)

| 表名 | 说明 | 主键 |
|------|------|------|
| `daily_wide` | 日线宽表 (OHLCV 平铺) | trade_date, ts_code |
| `stock_profile` | 股票档案 (最新状态快照) | ts_code |

### 5.3 行情表 (003_market_tables.sql)

| 表名 | 说明 | 主键 |
|------|------|------|
| `minute_bar` | 分钟级 K 线 | ts_code, trade_date, trade_time |
| `market_snapshot` | 实时快照 (5 档盘口) | ts_code (唯一) |

### 5.4 订单表 (004_orders.sql)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | UUID | 订单唯一标识 |
| ts_code | VARCHAR(10) | 标的代码 |
| direction | VARCHAR(4) | BUY / SELL |
| order_type | VARCHAR(10) | LIMIT / MARKET |
| price | DECIMAL(10,3) | 价格 |
| volume | INTEGER | 数量 |
| status | VARCHAR(10) | 状态 (CHECK 约束) |
| created_at | TIMESTAMPTZ | 创建时间 |
| updated_at | TIMESTAMPTZ | 更新时间 |
| qmt_order_id | VARCHAR(64) | miniQMT 返回的订单 ID |
| retry_count | INTEGER | 重试次数 |

约束：`status IN ('PENDING','SENT','ACK','FILLED','PARTIAL','REJECTED','CANCELLED')`
索引：4 个索引，含 1 个 active orders 局部索引

---

## 六、测试指南

### 运行全部测试

```bash
# 在对应 venv 中运行
python3 -m pytest tests/ -v --tb=short
```

### 按模块运行

```bash
# 跑批测试
python3 -m pytest tests/test_batch_update.py -v

# Redis 测试
python3 -m pytest tests/test_redis_client.py -v

# 行情模块测试
python3 -m pytest tests/test_market_*.py -v

# 订单状态机测试
python3 -m pytest tests/test_order_state_machine.py -v

# 风控测试
python3 -m pytest tests/test_risk_checker.py -v

# OpenClaw 命令测试
python3 -m pytest tests/test_openclaw_commands.py -v
```

### 覆盖率检查

```bash
python3 -m pytest tests/ --cov=quant_engine --cov=web_backend --cov=openclaw --cov-report=term-missing
```

### 测试策略

- **Mock 外部依赖**：Tushare API、Redis、PostgreSQL 全部 mock
- **AAA 模式**：Arrange → Act → Assert
- **不可变数据**：使用 frozen dataclass 确保测试隔离
- **覆盖率目标**：80%+

---

## 七、部署指南

### 7.1 systemd 服务

三个独立服务，均使用 `User=stock` 运行：

#### fastapi-web.service

```ini
[Unit]
Description=Quant FastAPI Web Backend
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=stock
WorkingDirectory=/opt/stock_sys/web_backend
ExecStart=/opt/stock_sys/venv_web/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=on-failure
MemoryHigh=800M
MemoryMax=1G
OOMScoreAdjust=-500

[Install]
WantedBy=multi-user.target
```

#### quant-engine.service

```ini
[Unit]
Description=Quant Batch Engine
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=stock
WorkingDirectory=/opt/stock_sys/quant_engine
ExecStart=/opt/stock_sys/venv_quant/bin/python main_quant.py
Restart=on-failure
RestartSec=30
MemoryHigh=1500M
MemoryMax=2G
OOMScoreAdjust=0

[Install]
WantedBy=multi-user.target
```

#### openclaw-agent.service

```ini
[Unit]
Description=OpenClaw AI Agent
After=network.target

[Service]
Type=simple
User=stock
WorkingDirectory=/opt/stock_sys/openclaw
ExecStart=/opt/stock_sys/venv_claw/bin/python agent_main.py
Restart=on-failure
RestartSec=10
MemoryHigh=500M
MemoryMax=800M
OOMScoreAdjust=0

[Install]
WantedBy=multi-user.target
```

### 7.2 cgroup v2 内存管控

| 服务 | MemoryHigh (软限) | MemoryMax (硬限) | OOMScoreAdjust |
|------|-------------------|-----------------|---------------|
| fastapi-web | 800M | 1G | -500 (优先保护) |
| quant-engine | 1500M | 2G | 0 |
| openclaw-agent | 500M | 800M | 0 |

- **MemoryHigh**：超过此值会触发内存回收和节流
- **MemoryMax**：超过此值会 OOM kill，但由 systemd 管理而非内核全局 OOM
- **OOMScoreAdjust**：负值降低被杀概率，Web 服务优先级最高

### 7.3 运维命令

```bash
# 启动/停止/重启服务
sudo systemctl start fastapi-web
sudo systemctl stop quant-engine
sudo systemctl restart openclaw-agent

# 查看状态
sudo systemctl status fastapi-web quant-engine openclaw-agent

# 查看日志
sudo journalctl -u fastapi-web -f
sudo journalctl -u quant-engine --since "1 hour ago"

# 查看内存使用
systemctl show fastapi-web --property=MemoryCurrent
```

---

## 八、开发工作流

### 功能开发流程

1. **研究与重用** — GitHub 代码搜索优先，确认 API 行为
2. **规划** — 使用 planner 代理创建实现计划
3. **TDD** — 先写测试 (RED) → 实现 (GREEN) → 重构 (IMPROVE)
4. **代码审查** — 使用 code-reviewer 代理
5. **提交** — 约定式提交格式

### 提交消息格式

```
<type>: <description>

<optional body>
```

类型：`feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `ci`

### 编码规范

- 遵循 **PEP 8**
- 所有函数签名使用**类型注解**
- 优先**不可变数据** (frozen dataclass, NamedTuple)
- **black** 格式化，**ruff** linting
- 错误显式处理，禁止静默吞错
- 系统边界验证输入

---

## 九、Redis 通信协议

### Stream 约定

| Stream | 用途 | Consumer Group |
|--------|------|---------------|
| `trade_orders` | 交易指令下发 | `quant_executor` |
| `market_data` | 行情数据分发 | 多个消费者 |
| `dead_letter` | 死信队列 (MAXLEN ~10000) | 无 |

### Pub/Sub Channel

| Channel | 用途 |
|---------|------|
| `market.snapshot.{ts_code}` | 实时行情快照推送 |

### Hash 热配置

| Hash | 用途 |
|------|------|
| `risk_rules` | 风控规则 (单笔上限/日累计/黑名单/订单数上限) |

---

## 十、常见问题

### Q: 跑批 OOM 怎么办？

A: 检查 `BATCH_CHUNK_SIZE`，先用 100 只股票实测，`htop` 看内存峰值，线性推算安全值。确保 Polars 使用 `streaming=True`。

### Q: Redis AOF 文件过大？

A: `auto-aof-rewrite-min-size 128mb` + `auto-aof-rewrite-percentage 100` 防止 rewrite 与跑批并发。定期 `BGREWRITEAOF`。

### Q: 订单状态卡住不流转？

A: 检查 Redis Stream 消费组是否正常，miniQMT 是否在线，死信队列是否有堆积。

### Q: 如何模拟交易环境？

A: 使用 `MockXtquantAdapter`，所有下单操作返回模拟成功响应，无需连接真实 miniQMT。

### Q: 如何热更新风控规则？

A: 直接修改 Redis Hash `risk_rules`：
```bash
redis-cli HSET risk_rules max_single_amount 200000
```
下次下单时自动加载新规则，无需重启服务。
