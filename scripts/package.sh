#!/usr/bin/env bash
# 量化中枢系统打包脚本
# 用法: bash scripts/package.sh
#
# 将所有源代码 + 文档 + 配置文件打包为 tar.gz 归档文件，
# 自动排除 venv、__pycache__、.env 等不需要分发的内容。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DIST_DIR="${PROJECT_ROOT}/dist"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
PACKAGE_NAME="quant_brain_${TIMESTAMP}"
ARCHIVE_PATH="${DIST_DIR}/${PACKAGE_NAME}.tar.gz"

# 需要排除的路径/文件类型
EXCLUDES=(
    # Python 虚拟环境
    --exclude='venv_web'
    --exclude='venv_quant'
    --exclude='venv_claw'
    # Python 缓存
    --exclude='__pycache__'
    --exclude='*.pyc'
    --exclude='*.pyo'
    # 本地配置/敏感文件
    --exclude='.env'
    --exclude='.env.*'
    --exclude='*.key'
    # 构建产物
    --exclude='.pytest_cache'
    --exclude='.mypy_cache'
    --exclude='.ruff_cache'
    --exclude='htmlcov'
    --exclude='.coverage'
    # Git
    --exclude='.git'
    --exclude='.gitignore'
    # Claude Code 内部文件
    --exclude='.claude'
    # 打包产物自身
    --exclude='dist'
    # Node modules (如果有)
    --exclude='node_modules'
)

# 创建 dist 目录
mkdir -p "${DIST_DIR}"

echo ">>> 打包量化中枢系统..."
echo "    项目根目录: ${PROJECT_ROOT}"
echo "    归档文件:   ${ARCHIVE_PATH}"

# 打包
tar -czf "${ARCHIVE_PATH}" \
    -C "${PROJECT_ROOT}" \
    "${EXCLUDES[@]}" \
    web_backend/ \
    quant_engine/ \
    openclaw/ \
    infra/ \
    tests/ \
    scripts/ \
    docs/ \
    CLAUDE.md \
    2>/dev/null || true

# 显示结果
SIZE=$(du -sh "${ARCHIVE_PATH}" | cut -f1)
FILE_COUNT=$(tar -tzf "${ARCHIVE_PATH}" | wc -l)

echo ""
echo ">>> 打包完成!"
echo "    文件大小: ${SIZE}"
echo "    文件数量: ${FILE_COUNT}"
echo ""
echo ">>> 归档内容:"
echo ""
echo "  quant_brain/"
echo "  ├── web_backend/          # FastAPI Web 后端"
echo "  ├── quant_engine/         # 量化计算引擎"
echo "  ├── openclaw/             # AI Agent"
echo "  ├── infra/                # 基础设施 (SQL + systemd)"
echo "  ├── tests/                # 测试套件"
echo "  ├── scripts/              # 运维脚本"
echo "  ├── docs/                 # 文档"
echo "  │   ├── DEVELOPMENT.md    # 开发文档"
echo "  │   ├── API.md            # 接口文档"
echo "  │   └── IMPLEMENTATION_PLAN.md"
echo "  └── CLAUDE.md"
echo ""
echo ">>> 解压命令:"
echo "    tar -xzf ${ARCHIVE_PATH##*/} -C /path/to/destination"
