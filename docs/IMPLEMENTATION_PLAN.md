# 量化中枢系统 V5.0：实施计划

> 硬件底座：N5105 (4核) + 8GB 内存 PVE VM
> 数据中枢：远程 PostgreSQL 15 (~9GB Tushare A 股数据)
> 交易网关：远程 Win10 VM + miniQMT (xtquant)
> 核心原则：极简、轻量、金融级安全

---

## 一、项目目录结构

```
/home/quant/cokyquant/quant_brain/
│
├── infra/                          # 基础设施配置（系统级）
│   ├── systemd/
│   │   ├── fastapi-web.service     # Web 网关服务
│   │   ├── quant-engine.service    # 量化引擎服务
│   │   └── openclaw-agent.service  # AI Agent 服务
│   ├── redis.conf                  # Redis 配置
│   └── journald.conf               # 日志限制配置
│
├── venv_web/                       # Web 后端虚拟环境 (venv)
├── venv_quant/                     # 量化引擎虚拟环境 (venv)
├── venv_claw/                      # OpenClaw 虚拟环境 (venv)
│
├── web_backend/                    # FastAPI 数据网关
│   ├── main.py                     # 入口 + app 定义
│   ├── config.py                   # 配置管理 (env)
│   ├── db.py                       # PostgreSQL 连接池
│   ├── redis_client.py             # Redis 客户端
│   ├── routes/
│   │   ├── market.py               # 行情数据接口
│   │   ├── trading.py              # 交易指令接口
│   │   └── account.py              # 账本查询接口
│   └── schemas.py                  # Pydantic 响应模型
│
├── quant_engine/                   # 量化计算引擎
│   ├── main_quant.py               # 入口 + 调度循环
│   ├── config.py                   # 配置管理
│   ├── db.py                       # PostgreSQL 连接
│   ├── redis_client.py             # Redis Stream 客户端
│   ├── batch_update.py             # 盘后数据跑批 (Polars)
│   ├── strategy/
│   │   ├── base.py                 # 策略基类
│   │   └── momentum.py             # 动量策略示例
│   ├── risk/
│   │   └── checker.py              # 风控检查器
│   └── order/
│       ├── state_machine.py        # 订单状态机
│       └── executor.py             # 订单执行器 (xtquant)
│
├── openclaw/                       # OpenClaw AI Agent
│   ├── agent_main.py               # 入口
│   ├── chat.py                     # 对话逻辑
│   └── commands/
│       ├── market.py               # 查行情命令
│       ├── account.py              # 查账本命令
│       └── trade.py                # 交易命令
│
├── tests/                          # 测试 (80%+ 覆盖率)
│   ├── test_batch_update.py
│   ├── test_order_state_machine.py
│   ├── test_redis_stream.py
│   ├── test_risk_checker.py
│   ├── test_api_routes.py
│   └── test_openclaw_commands.py
│
├── scripts/                        # 运维脚本
│   ├── setup.sh                    # 环境初始化脚本
│   └── backfill_retired_stocks.py  # 退市股清理
│
└── docs/
    └── ARCHITECTURE.md             # 架构文档 (本文件)
```

---

## 二、分阶段实施计划

### Phase 0：地基搭建 (1 天)

**目标：** 系统环境准备，权限隔离，基础服务就绪

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 0.1 | 创建 stock 用户和目录结构 | `useradd` + `mkdir` 完成 | 无 |
| 0.2 | 创建 3 个 venv 并安装基础依赖 | `venv_web`, `venv_quant`, `venv_claw` | 0.1 |
| 0.3 | **验证 Polars PostgreSQL 驱动** (`connectorx` / `adbc`) | `python -c "import connectorx"` 无报错 | 0.2 |
| 0.4 | 配置 Redis (AOF + maxmemory 1gb + volatile-lru + **AOF rewrite 限制**) | `/etc/redis/redis.conf` 生效 | 0.1 |
| 0.5 | 配置 journald 日志限制 | `journald.conf` 生效 | 0.1 |
| 0.6 | 编写 `setup.sh` 一键初始化脚本 | `scripts/setup.sh` 可执行 | 0.1-0.5 |
| 0.7 | 验证 cgroup v2 可用性 | `stat -fc %T /sys/fs/cgroup/` 返回 `cgroup2fs` | 无 |

**验证标准：**
- `sudo -u stock whoami` 返回 `stock`
- `redis-cli ping` 返回 `PONG`
- 3 个 venv 中 `python -c "import fastapi"` / `import polars` / `import redis` / `import connectorx` 无报错
- **`sudo bash scripts/setup.sh` 必须以 sudo 运行；stock 用户仅用于 systemd 服务运行时身份 (`User=stock`)，不可用于系统配置**

**权限统一决策：** 所有 systemd 服务统一使用 `User=stock`，不使用 root。Phase 5 的 systemd 配置文件中必须包含 `User=stock`。开发期间可用当前用户，部署时切换到 stock。

**Redis AOF rewrite 保护：** 配置 `auto-aof-rewrite-min-size 128mb` 和 `auto-aof-rewrite-percentage 100`，防止 AOF rewrite 与跑批并发时触发 OOM。

---

### Phase 1：数据跑批与清洗 (2-3 天) ⭐ **推荐起点**

**目标：** 最脏最累的数据层先行，这是整个系统的基石

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 1.1 | 定义 `batch_update.py` 基础框架 | Polars 连接 PostgreSQL | Phase 0 |
| 1.2 | 实现退市股过滤 (`list_status='L'`) | 查询仅返回正常上市股票 | 1.1 |
| 1.3 | 实现 `daily_wide` 表压平逻辑 | 高频动表写入 | 1.2 |
| 1.4 | 实现 `stock_profile` 表压平逻辑 | 低频静表写入 | 1.2 |
| 1.5 | 实现停牌股 ffill 填充 (含退市过滤) | 无前向填充污染 | 1.3 |
| 1.6 | 分片大小实测反推 (先跑 100 只，htop 看内存，线性估算安全值) | 内存峰值 < 1.5GB | 1.3 |
| 1.7 | 编写 `test_batch_update.py` | 80%+ 覆盖率 | 1.2-1.6 |

**技术实现要点（已确认）：**
- 必须使用 Polars LazyFrame + `streaming=True`，严禁一次性 `.collect()` 全量数据
- 分片大小：先用 100 只股票实测，`htop` 监控内存峰值，线性推算安全分片数（禁止拍脑袋定 500）
- 禁止使用 Pandas，禁止使用 SQLAlchemy 中间层
- `daily_wide` 表结构：需要预先在 PostgreSQL 中设计（日期 + ts_code 联合主键）

**验证标准：**
- 跑批脚本在 N5105 上实际运行，内存峰值 < 1.5GB
- 跑批完成后，`daily_wide` 无退市股数据
- 停牌股 ffill 不会填充到退市日期之后的数据

---

### Phase 2：Redis Stream 通信骨架 (2 天)

**目标：** 交易指令的持久化消息队列，ACK 闭环

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 2.1 | 封装 Redis 客户端 (带 expire) | `redis_client.py` | Phase 0 |
| 2.2 | 实现 `XADD` 交易指令下发 | 量化引擎 -> Redis Stream | 2.1 |
| 2.3 | 实现 `XREADGROUP` 订单消费 | 订单执行器 <- Redis Stream | 2.1 |
| 2.4 | 实现 ACK 确认机制 | 消费后 `XACK` | 2.3 |
| 2.5 | 实现死信队列 (Dead Letter) + **MAXLEN 上限截断** | 超时未 ACK 的订单处理，dead_letter 设 `MAXLEN ~ 10000` | 2.4 |
| 2.6 | 实现断线重连逻辑 | 自动恢复消费组 | 2.3 |
| 2.7 | 编写 `test_redis_stream.py` | 80%+ 覆盖率 | 2.2-2.6 |

**技术实现要点（已确认）：**
- Stream 名称：`trade_orders`
- Consumer Group：`quant_executor`
- Win10 miniQMT 直连 Ubuntu Redis 6379，`XREADGROUP` 阻塞监听，禁止额外 HTTP 服务
- 死信阈值：3 次重试失败后转入 `dead_letter` Stream，设置 `MAXLEN ~ 10000` 防止无限膨胀
- dead_letter 消费策略：quant-engine 每 30 分钟扫描一次 dead_letter，记录日志并发送告警，超过 7 天的死信自动清除
- Web 接口写缓存必须强制带 `expire` 时间

**验证标准：**
- 量化引擎写入指令后，订单执行器能在 < 100ms 内收到
- 模拟断线重连后，消息不丢失、不重复消费
- 死信队列能正确捕获超时未 ACK 的订单

---

### Phase 3：订单状态机 + 风控 (2-3 天)

**目标：** PostgreSQL 中的订单状态追踪 + 交易前置风控

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 3.1 | 设计 `orders` 表 DDL | DDL 脚本 | Phase 0 |
| 3.2 | 实现订单状态机 (`PENDING -> SENT -> ACK -> FILLED/REJECTED`) | `order/state_machine.py` | 3.1 |
| 3.3 | 实现状态转换校验 (非法转换抛异常) | 状态机测试通过 | 3.2 |
| 3.4 | 实现风控检查器 (`risk/checker.py`) | 风控模块 | 3.1 |
| 3.5 | 风控规则：单笔上限、日累计上限、标的黑名单 | 可配置规则 | 3.4 |
| 3.6 | 集成 xtquant miniQMT 下单接口 | `order/executor.py` | Phase 0, 3.2, 3.4 |
| 3.7 | 编写 `test_order_state_machine.py` | 80%+ 覆盖率 | 3.2-3.6 |

**技术实现要点（已确认）：**
- `orders` 表字段：`id`, `ts_code`, `direction`, `price`, `volume`, `status`, `created_at`, `updated_at`, `qmt_order_id`, **`retry_count`** (记录重试次数，用于死因追溯)
- 状态转换使用枚举类 + 显式转换矩阵，拒绝隐式字符串比较
- 风控规则存 Redis Hash，支持盘中热更新，无需重启服务
- PostgreSQL 不参与高频配置读取

**验证标准：**
- 所有非法状态转换被拒绝并记录日志
- 风控规则触发时订单被拦截，状态变为 `REJECTED`
- xtquant 模拟下单成功，订单状态正确流转

---

### Phase 4：FastAPI 数据网关 (2-3 天)

**目标：** 纯数值紧凑数组返回，复权逻辑下放前端

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 4.1 | FastAPI 入口 + uvicorn 配置 | `web_backend/main.py` | Phase 0 |
| 4.2 | PostgreSQL 连接池 (asyncpg) | `web_backend/db.py` | 4.1 |
| 4.3 | 行情数据接口 (纯数值数组返回) | `routes/market.py` | 4.2 |
| 4.4 | 交易指令提交接口 | `routes/trading.py` | 4.2, Phase 2 |
| 4.5 | 账本查询接口 | `routes/account.py` | 4.2, Phase 3 |
| 4.6 | CORS + 请求日志 | 中间件 | 4.1 |
| 4.7 | 编写 `test_api_routes.py` | 80%+ 覆盖率 | 4.3-4.5 |

**技术实现要点（已确认）：**
- 返回格式：`[timestamp, open, high, low, close, volume]` 紧凑数组，复权因子单独返回 `adj_factor`
- 连接库：`asyncpg` 直连，禁止 SQLAlchemy 中间层
- 复权计算完全下放前端 Vue 3（纯 JS），后端不复权

**验证标准：**
- API 响应时间 < 200ms (P95)
- 并发 10 请求时内存 < 600M
- 紧凑数组返回格式正确，前端可解析

---

### Phase 5：Systemd 服务 + 集成测试 (1-2 天)

**目标：** 三组件 systemd 守护，内存管控，端到端验证

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 5.1 | 编写 `fastapi-web.service` | systemd 配置 | Phase 4 |
| 5.2 | 编写 `quant-engine.service` | systemd 配置 | Phase 3 |
| 5.3 | 编写 `openclaw-agent.service` | systemd 配置 | Phase 0 |
| 5.4 | 配置 MemoryHigh/MemoryMax/OOMScoreAdjust | 内存管控生效 | 5.1-5.3 |
| 5.5 | 端到端集成测试：跑批 -> 策略信号 -> 订单 -> 状态机 (**非交易时间需 mock executor**) | 全流程通过 | 5.1-5.4 |
| 5.6 | 压力测试：模拟内存高压场景 | OOMScoreAdjust 行为验证 | 5.4 |

**验证标准：**
- `systemctl status` 三组件均为 `active (running)`
- `MemoryMax` 硬限触发时服务重启而非系统 OOM
- 端到端跑批到下单全流程 < 30 分钟

---

### Phase 6：OpenClaw Agent (1-2 天)

**目标：** AI 交互枢纽，通过 HTTP 调用 FastAPI 获取数据

| # | 任务 | 产出 | 依赖 |
|---|------|------|------|
| 6.1 | OpenClaw 入口 + HTTP 客户端 | `openclaw/agent_main.py` | Phase 4 |
| 6.2 | 查行情命令 | `commands/market.py` | 6.1 |
| 6.3 | 查账本命令 | `commands/account.py` | 6.1 |
| 6.4 | 交易命令 (带确认) | `commands/trade.py` | 6.1, Phase 4 |
| 6.5 | 自然语言 -> 结构化指令解析 | 对话逻辑 | 6.2-6.4 |
| 6.6 | 编写 `test_openclaw_commands.py` | 对话逻辑 + 命令解析测试覆盖 | 6.5 |

---

## 三、已确认技术决策（架构师批复 ✅）

> 以下 5 项决策已确认，开发时必须严格遵守，不得更改。

1. **PostgreSQL 连接库：`asyncpg` ✅**
   - FastAPI + uvloop 必须搭配原生异步的 asyncpg
   - N5105 800MB 内存配额下，asyncpg 是唯一解
   - 禁止使用 psycopg2 或 SQLAlchemy 中间层

2. **跑批引擎：Polars（惰性求值 + streaming） ✅**
   - 坚决锁定 Polars，禁止使用 Pandas
   - 严禁一次性 `.collect()` 全量数据
   - 必须使用 LazyFrame + `streaming=True` + 分块读取
   - 内存峰值必须压在 1.5GB (MemoryHigh) 之下

3. **xtquant 远程通信：Redis Stream 直连 ✅**
   - 禁止 TCP/HTTP 桥接，禁止在 Win10 上额外启动 HTTP 服务
   - Win10 miniQMT 作为纯粹 Consumer，直连 Ubuntu Redis 6379
   - 执行方式：`XREADGROUP` 阻塞监听 `trade_orders` → 调用本地 xtquant 下单 → `XACK` + 推送状态机 Stream

4. **前端复权计算：纯 JS ✅**
   - 禁止使用 WebAssembly (WASM)
   - 复权是标量乘法，V8 JIT 处理 2000-3000 元素数组 < 1ms
   - 后端仅返回 `adj_factor` 字段 + 紧凑 OHLCV 数组

5. **策略配置存储：Redis Hash 为主 ✅**
   - 风控规则（单日最大开仓次数、止损线）和策略参数存 Redis Hash
   - 支持盘中热更新，无需重启 quant-engine.service
   - PostgreSQL 不参与高频配置读取

---

## 四、风险清单与缓解措施

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|----------|
| Polars 大表 OOM | 系统崩溃 | 高 | 分片加载 + MemoryHigh 软限流 |
| Redis AOF 膨胀 | 磁盘满 | 中 | 定期 BGREWRITEAOF + 磁盘监控 |
| **AOF rewrite 与跑批并发** | 瞬时 OOM | **高** | `auto-aof-rewrite-min-size 128mb` + `auto-aof-rewrite-percentage 100` |
| xtquant 断线 | 订单丢失 | 中 | Redis Stream ACK + 死信队列 |
| **dead_letter 无限膨胀** | Redis 内存泄露 | 中 | `MAXLEN ~ 10000` 截断 + 定期扫描清除 |
| 退市股 ffill 污染 | 回测失真 | 高 | 严格 `list_status='L'` 过滤 |
| N5105 网络 I/O 瓶颈 | API 超时 | 中 | 紧凑数组返回 + uvloop |
| systemd OOM 误杀 | Redis 被杀 | 低 | OOMScoreAdjust 保护 Redis |
| 并发请求击穿 | Web 宕机 | 中 | 2 workers + 连接池限制 |

---

## 五、推荐开发顺序

根据用户提供的建议，采用 **"先脏后精"** 的策略：

```
Phase 0 (地基) → Phase 1 (数据跑批) → Phase 2 (Redis Stream) → Phase 3 (订单状态机)
                                                                         ↓
Phase 6 (OpenClaw) ← Phase 5 (集成测试) ← Phase 4 (FastAPI) ←─────────┘
```

**理由：**
1. **Phase 1 先行：** 数据是系统的血液，跑批通了，后续所有模块才有真实数据可用
2. **Phase 2 紧随：** 消息队列是量化引擎和交易执行的解耦关键，先通通信再写业务
3. **Phase 3 核心：** 订单状态机和风控是金融级安全的底线
4. **Phase 4 对外：** 有数据有交易能力后，再暴露 API 给前端
5. **Phase 5 兜底：** 集成测试确保各组件协同工作
6. **Phase 6 锦上添花：** OpenClaw 是交互层，依赖前面所有模块

---

## 六、下一步行动

**立即执行：** Phase 0 - 地基搭建

具体任务：
1. 创建 stock 用户和目录结构
2. 创建 3 个 venv
3. 配置 Redis
4. 配置 journald
5. 编写 setup.sh

请确认是否开始执行 Phase 0，或者对计划有任何调整意见。
