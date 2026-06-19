# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

**Quant Brain** — A 股量化交易系统。三个独立服务进程：

1. **quant_engine** — Polars 跑批 + 订单状态机 + 风控 + Redis Stream 分发
2. **web_backend** — FastAPI REST API 网关 (asyncpg, 紧凑数组返回)
3. **openclaw** — OpenClaw AI Agent (自然语言交互)

## Commands

### Development
```bash
# Format
ruff format .
ruff check --fix .

# Run tests
python -m pytest tests/ -v

# Tests with coverage
python -m pytest tests/ --cov=quant_engine --cov=web_backend --cov=openclaw --cov-report=term-missing

# Run single module tests
python -m pytest tests/test_order_state_machine.py -v
```

### Running Services
```bash
# Web backend (dev)
WEB_PORT=8000 python -m web_backend.main

# Quant engine
python -m quant_engine.main_quant

# OpenClaw agent
python -m openclaw.agent_main
```

### Deploy
```bash
# One-click setup (requires sudo)
sudo bash scripts/setup.sh

# Manage services
sudo systemctl status fastapi-web quant-engine openclaw-agent
sudo journalctl -u quant-engine -f
```

## Architecture

### Tech Stack
| Layer | Technology |
|-------|------------|
| Data source | PostgreSQL (ETL-populated by `quant_data_pipeline`) |
| Processing | Polars (LazyFrame + streaming) |
| Database | PostgreSQL 15 + asyncpg (NO SQLAlchemy) |
| Queue | Redis 7 (Stream + Pub/Sub + Hash) |
| Web | FastAPI + uvicorn + asyncpg |
| Trading | xtquant miniQMT |
| Agent | OpenClaw + httpx |
| Realtime snapshot | Tushare realtime_quote (5-level bid/ask only) |
| Deploy | systemd + cgroup v2 |

### Key Constraints (DO NOT violate)
- **NO Pandas** — use Polars only
- **NO SQLAlchemy** — use asyncpg directly
- **NO `.collect()` full data** — always use LazyFrame + streaming + chunking
- **NO WASM** — frontend adj_factor calc only
- **NO TCP/HTTP bridge** for miniQMT — Redis Stream only
- **NO direct Tushare HTTP for historical data** — read from PG (ETL handles Tushare); only `realtime_quote` (5-level snapshot) may call Tushare directly

### Redis Streams
- `trade_orders` — order dispatch (consumer group: `quant_executor`)
- `market_data` — market data distribution
- `dead_letter` — failed orders (MAXLEN ~10000)

### Environment Variables
All config from env vars. See `.env.example` for reference. Required at startup:
- `PG_HOST`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE` (defaults point at the ETL-populated PG at `192.168.3.11 / quant_db / quant_user`)

Optional:
- `TUSHARE_TOKEN` — only needed for `RealtimeQuoteClient.get_realtime_snapshot` (5-level bid/ask). Pure PG-read mode does not need it.

### Order State Machine
7 states: `PENDING → SENT → ACK → FILLED | PARTIAL | REJECTED | CANCELLED`
State transitions enforced via explicit matrix. Illegal transitions raise `InvalidTransitionError`.

### Risk Rules (Redis Hash `risk_rules`)
- Blacklist (tickers)
- Single order limit (default: 100,000 RMB)
- Daily cumulative limit (default: 500,000 RMB)
- Daily order count limit (default: 50)

## Important Files
- `docs/IMPLEMENTATION_PLAN.md` — Full implementation plan with phases
- `docs/DEVELOPMENT.md` — Developer guide with architecture details
- `docs/API.md` — REST API documentation
- `infra/sql/` — Database migrations (execute in order 001-004)
- `scripts/setup.sh` — Production environment setup
