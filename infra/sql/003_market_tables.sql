-- 行情表：minute_bar (分钟级 K 线)
-- 数据源：Tushare ts.pro_bar(freq='min')
-- 更新频率：盘中实时写入 (每根 K 线收盘后一条)
CREATE TABLE IF NOT EXISTS minute_bar (
    ts_code         TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    trade_time      TIME NOT NULL,     -- 分钟时间 (HH:MM:00)
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    vol             DOUBLE PRECISION,   -- 成交量(手)
    amount          DOUBLE PRECISION,   -- 成交额(千元)
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date, trade_time)
);

CREATE INDEX IF NOT EXISTS idx_minute_bar_trade_date ON minute_bar (trade_date);
CREATE INDEX IF NOT EXISTS idx_minute_bar_ts_code_date ON minute_bar (ts_code, trade_date);

-- 行情表：market_snapshot (实时快照缓存)
-- 由 distributor.py 每 N 秒刷新，交易时段外清空
-- 用于盘中快速获取最新行情，不参与历史查询
CREATE TABLE IF NOT EXISTS market_snapshot (
    ts_code         TEXT PRIMARY KEY,
    price           DOUBLE PRECISION,
    change          DOUBLE PRECISION,
    pct_chg         DOUBLE PRECISION,
    vol             DOUBLE PRECISION,
    amount          DOUBLE PRECISION,
    -- 5 档买卖盘
    bid_price_1     DOUBLE PRECISION,
    bid_vol_1       DOUBLE PRECISION,
    ask_price_1     DOUBLE PRECISION,
    ask_vol_1       DOUBLE PRECISION,
    bid_price_2     DOUBLE PRECISION,
    bid_vol_2       DOUBLE PRECISION,
    ask_price_2     DOUBLE PRECISION,
    ask_vol_2       DOUBLE PRECISION,
    bid_price_3     DOUBLE PRECISION,
    bid_vol_3       DOUBLE PRECISION,
    ask_price_3     DOUBLE PRECISION,
    ask_vol_3       DOUBLE PRECISION,
    bid_price_4     DOUBLE PRECISION,
    bid_vol_4       DOUBLE PRECISION,
    ask_price_4     DOUBLE PRECISION,
    ask_vol_4       DOUBLE PRECISION,
    bid_price_5     DOUBLE PRECISION,
    bid_vol_5       DOUBLE PRECISION,
    ask_price_5     DOUBLE PRECISION,
    ask_vol_5       DOUBLE PRECISION,
    -- 更新时间
    snapshot_time   TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_time ON market_snapshot (snapshot_time);
