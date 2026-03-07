-- ==============================================
-- HFT Options System - Database Schema
-- TimescaleDB (PostgreSQL with time-series ext)
-- ==============================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Price Bars (1-min OHLCV) ──
CREATE TABLE IF NOT EXISTS price_bars (
    time        TIMESTAMPTZ NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    open        DECIMAL(12,4),
    high        DECIMAL(12,4),
    low         DECIMAL(12,4),
    close       DECIMAL(12,4),
    volume      BIGINT,
    vwap        DECIMAL(12,4)
);
SELECT create_hypertable('price_bars', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_bars_ticker ON price_bars (ticker, time DESC);

-- ── Options Chain Snapshots ──
CREATE TABLE IF NOT EXISTS options_snapshots (
    time            TIMESTAMPTZ NOT NULL,
    ticker          VARCHAR(10) NOT NULL,
    expiry          DATE NOT NULL,
    strike          DECIMAL(10,2) NOT NULL,
    option_type     CHAR(1) NOT NULL,  -- C or P
    bid             DECIMAL(10,4),
    ask             DECIMAL(10,4),
    last            DECIMAL(10,4),
    volume          INT,
    open_interest   INT,
    implied_vol     DECIMAL(8,6),
    delta           DECIMAL(8,6),
    gamma           DECIMAL(8,6),
    theta           DECIMAL(8,6),
    vega            DECIMAL(8,6)
);
SELECT create_hypertable('options_snapshots', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_opts_ticker ON options_snapshots (ticker, expiry, strike, time DESC);

-- ── IV Surface History ──
CREATE TABLE IF NOT EXISTS iv_surface (
    time            TIMESTAMPTZ NOT NULL,
    ticker          VARCHAR(10) NOT NULL,
    iv_atm_30d      DECIMAL(8,6),   -- ATM IV for ~30 DTE
    iv_atm_7d       DECIMAL(8,6),   -- ATM IV for ~7 DTE
    iv_rank         DECIMAL(6,4),   -- 0-100 percentile vs 52wk
    iv_percentile   DECIMAL(6,4),   -- % of days below current
    skew_25d        DECIMAL(8,6),   -- 25-delta put/call skew
    term_slope      DECIMAL(8,6),   -- front vs back month spread
    rv_20d          DECIMAL(8,6),   -- 20-day realized vol
    iv_rv_ratio     DECIMAL(6,4)    -- IV / RV ratio
);
SELECT create_hypertable('iv_surface', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_iv_ticker ON iv_surface (ticker, time DESC);

-- ── Trade Log (Immutable Audit Trail) ──
CREATE TABLE IF NOT EXISTS trades (
    id              SERIAL,
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker          VARCHAR(10) NOT NULL,
    strategy_type   VARCHAR(30),       -- iron_condor, debit_spread, etc.
    direction       VARCHAR(10),       -- bullish, bearish, neutral
    agent_id        VARCHAR(30),
    signal_score    DECIMAL(5,4),
    
    -- Leg details (JSON for multi-leg)
    legs            JSONB NOT NULL,
    
    -- Execution
    fill_price      DECIMAL(10,4),     -- net credit/debit
    commission      DECIMAL(8,4),
    slippage        DECIMAL(8,4),
    
    -- Position tracking
    status          VARCHAR(15) DEFAULT 'open',  -- open, closed, expired, rolled
    close_time      TIMESTAMPTZ,
    close_price     DECIMAL(10,4),
    pnl             DECIMAL(10,4),
    pnl_pct         DECIMAL(8,4),
    
    -- Risk at entry
    max_risk        DECIMAL(10,4),
    max_reward      DECIMAL(10,4),
    
    -- Context
    market_regime   VARCHAR(15),
    iv_rank_entry   DECIMAL(6,4),
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades (time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status, ticker);

-- ── Agent Signals Log ──
CREATE TABLE IF NOT EXISTS signals (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent_id        VARCHAR(30) NOT NULL,
    ticker          VARCHAR(10) NOT NULL,
    direction       VARCHAR(10),
    confidence      DECIMAL(5,4),
    signal_data     JSONB,
    regime          VARCHAR(15),
    consumed        BOOLEAN DEFAULT FALSE
);
SELECT create_hypertable('signals', 'time', if_not_exists => TRUE);

-- ── Daily P&L Summary ──
CREATE TABLE IF NOT EXISTS daily_pnl (
    date            DATE NOT NULL PRIMARY KEY,
    gross_pnl       DECIMAL(10,4),
    commissions     DECIMAL(10,4),
    net_pnl         DECIMAL(10,4),
    trades_opened   INT,
    trades_closed   INT,
    win_rate        DECIMAL(5,4),
    max_drawdown    DECIMAL(10,4),
    portfolio_delta DECIMAL(10,4),
    portfolio_theta DECIMAL(10,4),
    portfolio_vega  DECIMAL(10,4),
    regime          VARCHAR(15)
);

-- ── Earnings Calendar ──
CREATE TABLE IF NOT EXISTS earnings_calendar (
    ticker          VARCHAR(10) NOT NULL,
    earnings_date   DATE NOT NULL,
    timing          VARCHAR(10),  -- BMO (before open), AMC (after close)
    expected_move   DECIMAL(6,4), -- expected % move from options pricing
    actual_move     DECIMAL(6,4), -- filled in after earnings
    iv_before       DECIMAL(8,6),
    iv_after        DECIMAL(8,6),
    PRIMARY KEY (ticker, earnings_date)
);

-- ── Compression Policy (keep raw data 30 days, compress older) ──
SELECT add_compression_policy('price_bars', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_compression_policy('options_snapshots', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('iv_surface', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_compression_policy('signals', INTERVAL '14 days', if_not_exists => TRUE);

-- ── Retention Policy (drop data older than 1 year) ──
SELECT add_retention_policy('price_bars', INTERVAL '1 year', if_not_exists => TRUE);
SELECT add_retention_policy('options_snapshots', INTERVAL '6 months', if_not_exists => TRUE);
SELECT add_retention_policy('signals', INTERVAL '3 months', if_not_exists => TRUE);
