-- 订单表：orders
-- 用途：PostgreSQL 中的订单状态追踪，由订单状态机写入/更新
-- 数据流：策略信号 → 风控检查 → 状态机创建(PENDING) → xtquant 执行 → 状态流转
CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ts_code         TEXT NOT NULL,                    -- 股票代码 (如 000001.SZ)
    direction       TEXT NOT NULL CHECK (direction IN ('BUY', 'SELL')),
    order_type      TEXT NOT NULL CHECK (order_type IN ('LIMIT', 'MARKET')),
    price           DOUBLE PRECISION NOT NULL,        -- 委托价格 (市价单可为 0)
    volume          INT NOT NULL,                     -- 委托数量(股)
    status          TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'SENT', 'ACK', 'FILLED', 'PARTIAL', 'REJECTED', 'CANCELLED')),
    qmt_order_id    TEXT,                             -- miniQMT 返回的委托编号
    qmt_strategy    TEXT,                             -- miniQMT 策略名称
    qmt_remark      TEXT,                             -- miniQMT 备注
    retry_count     INT NOT NULL DEFAULT 0,           -- 重试次数 (用于死因追溯)
    rejected_reason TEXT,                             -- 拒绝原因 (风控拦截或券商拒绝)
    filled_price    DOUBLE PRECISION,                 -- 实际成交均价
    filled_volume   INT,                              -- 实际成交量
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 状态索引 (按状态查询待处理/执行中订单)
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);

-- 按股票+日期查询 (日常追溯)
CREATE INDEX IF NOT EXISTS idx_orders_ts_code_created ON orders (ts_code, created_at DESC);

-- 按 qmt_order_id 查询 (与 miniQMT 对账)
CREATE INDEX IF NOT EXISTS idx_orders_qmt_order_id ON orders (qmt_order_id) WHERE qmt_order_id IS NOT NULL;

-- 仅索引未完成订单 (高频查询，过滤掉终态)
CREATE INDEX IF NOT EXISTS idx_orders_active ON orders (created_at DESC)
    WHERE status IN ('PENDING', 'SENT', 'ACK', 'PARTIAL');
