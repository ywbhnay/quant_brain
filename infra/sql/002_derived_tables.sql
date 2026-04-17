-- 派生表：daily_wide (高频动表)
-- 由 batch_update.py 分片写入，含停牌填充标记
-- 数据频率：每天盘后更新
CREATE TABLE IF NOT EXISTS daily_wide (
    trade_date      DATE NOT NULL,
    ts_code         TEXT NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    pre_close       DOUBLE PRECISION,
    change          DOUBLE PRECISION,
    pct_chg         DOUBLE PRECISION,
    vol             DOUBLE PRECISION,
    amount          DOUBLE PRECISION,
    adj_factor      DOUBLE PRECISION,
    is_suspended    BOOLEAN DEFAULT FALSE,   -- 该行是否为停牌填充
    fill_date       DATE,                     -- 填充数据来源日期
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_daily_wide_ts_code ON daily_wide (ts_code);
CREATE INDEX IF NOT EXISTS idx_daily_wide_suspended ON daily_wide (is_suspended);
CREATE INDEX IF NOT EXISTS idx_daily_wide_fill_date ON daily_wide (fill_date);

-- 派生表：stock_profile (低频静表)
-- 由 batch_update.py 每天刷新一次
-- 数据频率：每天盘后更新
CREATE TABLE IF NOT EXISTS stock_profile (
    ts_code         TEXT PRIMARY KEY,
    name            TEXT,
    industry        TEXT,
    list_date       DATE,
    delist_date     DATE,
    market          TEXT,
    exchange        TEXT,
    list_status     TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_profile_industry ON stock_profile (industry);
CREATE INDEX IF NOT EXISTS idx_stock_profile_list_status ON stock_profile (list_status);
CREATE INDEX IF NOT EXISTS idx_stock_profile_market ON stock_profile (market);
