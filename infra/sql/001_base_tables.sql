-- 基础表：stock_basic (Tushare 股票基本信息)
-- 数据源：tushare stock_basic 接口
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code         TEXT PRIMARY KEY,
    symbol          TEXT,
    name            TEXT,
    area            TEXT,
    industry        TEXT,
    market          TEXT,
    list_date       DATE,
    delist_date     DATE,
    list_status     TEXT NOT NULL CHECK (list_status IN ('L', 'D', 'P')),
    exchange        TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_basic_list_status ON stock_basic (list_status);
CREATE INDEX IF NOT EXISTS idx_stock_basic_industry ON stock_basic (industry);
CREATE INDEX IF NOT EXISTS idx_stock_basic_market ON stock_basic (market);

-- 基础表：daily (Tushare 日线数据)
-- 数据源：tushare daily 接口
CREATE TABLE IF NOT EXISTS daily (
    ts_code         TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    pre_close       DOUBLE PRECISION,
    change          DOUBLE PRECISION,
    pct_chg         DOUBLE PRECISION,
    vol             DOUBLE PRECISION,   -- 成交量(手)
    amount          DOUBLE PRECISION,   -- 成交额(千元)
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON daily (trade_date);

-- 基础表：adj_factor (复权因子)
-- 数据源：tushare adj_factor 接口
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code         TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    adj_factor      DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_adj_factor_trade_date ON adj_factor (trade_date);
