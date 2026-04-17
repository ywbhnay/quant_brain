#!/usr/bin/env bash
# ============================================================================
# 量化中枢系统 V5.0 — 一键初始化脚本
# ============================================================================
# ⚠️  必须以 sudo 运行：  sudo bash scripts/setup.sh
# stock 用户仅用于 systemd 服务运行时身份 (User=stock)，不可用于系统配置
# ============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 0. 前置检查
# ---------------------------------------------------------------------------
echo ""
echo "========================================="
echo "  量化中枢系统 V5.0 — 环境初始化"
echo "========================================="
echo ""

[[ $EUID -eq 0 ]] || fail "此脚本必须以 sudo 运行： sudo bash $0"

command -v python3  &>/dev/null || fail "python3 未安装"
command -v redis-server &>/dev/null || fail "redis-server 未安装"

# ---------------------------------------------------------------------------
# 1. 创建低权限业务用户
# ---------------------------------------------------------------------------
echo ""
echo "--- 1/7 创建 stock 用户 ---"

if id stock &>/dev/null; then
    warn "stock 用户已存在，跳过创建"
else
    useradd -r -s /usr/sbin/nologin -M stock
    log "stock 用户创建成功"
fi

# ---------------------------------------------------------------------------
# 2. 创建目录结构并授权
# ---------------------------------------------------------------------------
echo ""
echo "--- 2/7 创建目录结构 ---"

BASE=/opt/stock_sys

mkdir -p "$BASE"/{web_backend,quant_engine,openclaw}
chown -R stock:stock "$BASE"
log "目录结构已创建: $BASE"

# ---------------------------------------------------------------------------
# 3. 创建三个绝对隔离的 venv
# ---------------------------------------------------------------------------
echo ""
echo "--- 3/7 创建 Python 虚拟环境 ---"

create_venv() {
    local venv_path="$1"
    local pkg_list="$2"
    local label="$3"

    if [[ -f "$venv_path/bin/python" ]]; then
        warn "$label venv 已存在，跳过创建"
    else
        sudo -u stock python3 -m venv "$venv_path"
        log "$label venv 创建: $venv_path"
    fi

    "$venv_path/bin/pip" install --upgrade pip -q
    # shellcheck disable=SC2086
    "$venv_path/bin/pip" install -q $pkg_list
    log "$label 依赖安装完成: $pkg_list"
}

create_venv "$BASE/venv_web"   "fastapi uvicorn[standard] uvloop asyncpg pydantic" \
    "Web 后端"

create_venv "$BASE/venv_quant" "polars redis asyncpg httpx" \
    "量化引擎"

create_venv "$BASE/venv_claw"  "httpx redis pydantic" \
    "OpenClaw"

# ---------------------------------------------------------------------------
# 4. 部署 Redis 配置
# ---------------------------------------------------------------------------
echo ""
echo "--- 4/7 配置 Redis ---"

REDIS_CONF="/etc/redis/redis.conf"

if [[ -f "$REDIS_CONF" ]]; then
    cp "$REDIS_CONF" "${REDIS_CONF}.bak.$(date +%Y%m%d%H%M%S)"
    log "Redis 配置已备份: ${REDIS_CONF}.bak.*"
fi

# 写入覆盖关键参数（保留其他默认配置）
cat <<'EOF' | tee -a "$REDIS_CONF"

# === 量化中枢 V5.0 自定义配置 ===
maxmemory 1gb
maxmemory-policy volatile-lru
appendonly yes
appendfsync everysec
auto-aof-rewrite-min-size 128mb
auto-aof-rewrite-percentage 100
# === 量化中枢 V5.0 配置结束 ===
EOF

log "Redis 配置已更新 (maxmemory=1gb, AOF, volatile-lru, rewrite保护)"

systemctl restart redis-server || systemctl restart redis
log "Redis 已重启"
redis-cli ping | grep -q PONG && log "Redis 连接正常"

# ---------------------------------------------------------------------------
# 5. 配置 journald 日志限制
# ---------------------------------------------------------------------------
echo ""
echo "--- 5/7 配置 journald 日志限制 ---"

JOURNALD_CONF="/etc/systemd/journald.conf"

if [[ -f "$JOURNALD_CONF" ]]; then
    cp "$JOURNALD_CONF" "${JOURNALD_CONF}.bak.$(date +%Y%m%d%H%M%S)"
    log "journald 配置已备份"
fi

cat <<'EOF' | tee "$JOURNALD_CONF"
[Journal]
SystemMaxUse=500M
MaxRetentionSec=7day
EOF

log "journald 配置已更新 (500M 上限, 7天保留)"
systemctl restart systemd-journald
log "journald 已重启"

# ---------------------------------------------------------------------------
# 6. 验证 cgroup v2
# ---------------------------------------------------------------------------
echo ""
echo "--- 6/7 验证 cgroup v2 ---"

CGROUP_TYPE=$(stat -fc %T /sys/fs/cgroup/ 2>/dev/null || echo "unknown")

if [[ "$CGROUP_TYPE" == "cgroup2fs" ]]; then
    log "cgroup v2 已启用 (MemoryHigh/MemoryMax 可用)"
else
    warn "cgroup 类型: $CGROUP_TYPE — MemoryHigh/MemoryMax 可能不可用"
    warn "请在 Ubuntu 22.04+ 上运行，或确认 cgroup v2 已启用"
fi

# ---------------------------------------------------------------------------
# 7. 部署 systemd 服务文件（如果 infra/systemd/ 存在）
# ---------------------------------------------------------------------------
echo ""
echo "--- 7/7 部署 systemd 服务 ---"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SYSTEMD_DIR="${SCRIPT_DIR}/../infra/systemd"

if [[ -d "$SYSTEMD_DIR" ]]; then
    cp "$SYSTEMD_DIR"/*.service /etc/systemd/system/
    chmod 644 /etc/systemd/system/*.service
    systemctl daemon-reload
    log "systemd 服务文件已部署"

    systemctl enable --now fastapi-web quant-engine openclaw-agent 2>/dev/null || {
        warn "部分服务启动失败（可能因为代码尚未部署），这是预期的"
        warn "请先将 web_backend/ quant_engine/ openclaw/ 代码部署到 $BASE 后再启动服务"
    }
else
    warn "infra/systemd/ 目录不存在，跳过 systemd 服务部署"
    warn "请将 *.service 文件手动复制到 /etc/systemd/system/ 并执行 daemon-reload"
fi

# ---------------------------------------------------------------------------
# 完成
# ---------------------------------------------------------------------------
echo ""
echo "========================================="
echo "  初始化完成 ✅"
echo "========================================="
echo ""
echo "下一步："
echo "  1. 将 web_backend/ quant_engine/ openclaw/ 代码部署到 $BASE/"
echo "  2. 验证服务状态: systemctl status fastapi-web quant-engine openclaw-agent"
echo "  3. 查看内存限制: systemctl show fastapi-web -p MemoryHigh,MemoryMax"
echo ""
