"""
Database access layer for TimescaleDB (time-series)
and SQLite (operational state like PDT counter).
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import structlog

from config.settings import settings

logger = structlog.get_logger()

# SQLite for local operational state
SQLITE_PATH = Path("data/state.db")


class Database:
    """Async TimescaleDB connection pool + SQLite for state."""

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._sqlite: Optional[sqlite3.Connection] = None

    # ── Connection Management ──

    async def connect(self) -> None:
        """Initialize database connections."""
        # TimescaleDB pool
        self._pool = await asyncpg.create_pool(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password,
            database=settings.db_name,
            min_size=2,
            max_size=10,
        )
        logger.info("db.timescale_connected")

        # SQLite for operational state
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._sqlite = sqlite3.connect(str(SQLITE_PATH))
        self._sqlite.row_factory = sqlite3.Row
        self._init_sqlite()
        logger.info("db.sqlite_connected", path=str(SQLITE_PATH))

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
        if self._sqlite:
            self._sqlite.close()
        logger.info("db.disconnected")

    def _init_sqlite(self) -> None:
        """Create SQLite tables for operational state."""
        cur = self._sqlite.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS pdt_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE NOT NULL,
                ticker TEXT NOT NULL,
                strategy_type TEXT,
                is_day_trade BOOLEAN DEFAULT 0,
                opened_at TIMESTAMP,
                closed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS agent_config (
                agent_id TEXT PRIMARY KEY,
                weight REAL DEFAULT 1.0,
                enabled BOOLEAN DEFAULT 1,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                config_json TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS daily_state (
                date DATE PRIMARY KEY,
                watchlist_json TEXT,
                regime TEXT,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS iv_history (
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                iv_close REAL,
                PRIMARY KEY (ticker, date)
            );
        """)
        self._sqlite.commit()

    # ── TimescaleDB Operations ──

    async def execute(self, query: str, *args) -> str:
        """Execute a query and return status."""
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Fetch multiple rows."""
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Fetch a single row."""
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        """Fetch a single value."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    # ── Price Data ──

    async def insert_price_bar(
        self, ticker: str, bar: Dict[str, Any]
    ) -> None:
        await self.execute(
            """INSERT INTO price_bars (time, ticker, open, high, low, close, volume, vwap)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT DO NOTHING""",
            bar.get("time", datetime.utcnow()),
            ticker,
            bar["open"], bar["high"], bar["low"], bar["close"],
            bar.get("volume", 0),
            bar.get("vwap"),
        )

    async def get_price_bars(
        self, ticker: str, minutes: int = 60
    ) -> List[asyncpg.Record]:
        return await self.fetch(
            """SELECT * FROM price_bars
               WHERE ticker = $1 AND time > NOW() - $2::interval
               ORDER BY time ASC""",
            ticker,
            f"{minutes} minutes",
        )

    # ── Options Snapshots ──

    async def insert_options_snapshot(
        self, quotes: List[Dict[str, Any]]
    ) -> None:
        """Bulk insert options chain snapshot."""
        if not quotes:
            return
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """INSERT INTO options_snapshots
                   (time, ticker, expiry, strike, option_type,
                    bid, ask, last, volume, open_interest,
                    implied_vol, delta, gamma, theta, vega)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)""",
                [
                    (
                        q.get("time", datetime.utcnow()),
                        q["ticker"], q["expiry"], q["strike"],
                        q["option_type"],
                        q.get("bid", 0), q.get("ask", 0),
                        q.get("last", 0), q.get("volume", 0),
                        q.get("open_interest", 0),
                        q.get("implied_vol", 0),
                        q.get("delta", 0), q.get("gamma", 0),
                        q.get("theta", 0), q.get("vega", 0),
                    )
                    for q in quotes
                ],
            )

    # ── IV Surface History ──

    async def insert_iv_surface(self, data: Dict[str, Any]) -> None:
        await self.execute(
            """INSERT INTO iv_surface
               (time, ticker, iv_atm_30d, iv_atm_7d, iv_rank,
                iv_percentile, skew_25d, term_slope, rv_20d, iv_rv_ratio)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
            data.get("time", datetime.utcnow()),
            data["ticker"],
            data.get("iv_atm_30d", 0),
            data.get("iv_atm_7d", 0),
            data.get("iv_rank", 0),
            data.get("iv_percentile", 0),
            data.get("skew_25d", 0),
            data.get("term_slope", 0),
            data.get("rv_20d", 0),
            data.get("iv_rv_ratio", 0),
        )

    async def get_iv_history(
        self, ticker: str, days: int = 365
    ) -> List[asyncpg.Record]:
        return await self.fetch(
            """SELECT * FROM iv_surface
               WHERE ticker = $1 AND time > NOW() - $2::interval
               ORDER BY time ASC""",
            ticker,
            f"{days} days",
        )

    # ── Trade Log ──

    async def insert_trade(self, trade: Dict[str, Any]) -> int:
        return await self.fetchval(
            """INSERT INTO trades
               (ticker, strategy_type, direction, agent_id,
                signal_score, legs, fill_price, commission,
                slippage, max_risk, max_reward, market_regime,
                iv_rank_entry, notes)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
               RETURNING id""",
            trade["ticker"],
            trade.get("strategy_type", ""),
            trade.get("direction", ""),
            trade.get("agent_id", ""),
            trade.get("signal_score", 0),
            json.dumps(trade.get("legs", [])),
            trade.get("fill_price", 0),
            trade.get("commission", 0),
            trade.get("slippage", 0),
            trade.get("max_risk", 0),
            trade.get("max_reward", 0),
            trade.get("market_regime", ""),
            trade.get("iv_rank_entry", 0),
            trade.get("notes", ""),
        )

    async def close_trade(
        self, trade_id: int, close_price: float, pnl: float
    ) -> None:
        await self.execute(
            """UPDATE trades
               SET status = 'closed', close_time = NOW(),
                   close_price = $2, pnl = $3, pnl_pct = $4
               WHERE id = $1""",
            trade_id, close_price, pnl,
            pnl / max(abs(close_price), 0.01),
        )

    async def get_open_trades(self) -> List[asyncpg.Record]:
        return await self.fetch(
            "SELECT * FROM trades WHERE status = 'open' ORDER BY time DESC"
        )

    async def get_daily_pnl(self, d: date = None) -> float:
        d = d or date.today()
        result = await self.fetchval(
            """SELECT COALESCE(SUM(pnl), 0) FROM trades
               WHERE DATE(close_time) = $1 AND status = 'closed'""",
            d,
        )
        return float(result or 0)

    # ── Signals Log ──

    async def insert_signal(self, signal: Dict[str, Any]) -> None:
        await self.execute(
            """INSERT INTO signals
               (agent_id, ticker, direction, confidence, signal_data, regime)
               VALUES ($1,$2,$3,$4,$5,$6)""",
            signal["agent_id"],
            signal["ticker"],
            signal.get("direction", ""),
            signal.get("confidence", 0),
            json.dumps(signal.get("signal_data", {})),
            signal.get("regime", ""),
        )

    # ── Earnings Calendar ──

    async def get_upcoming_earnings(
        self, days_ahead: int = 7
    ) -> List[asyncpg.Record]:
        return await self.fetch(
            """SELECT * FROM earnings_calendar
               WHERE earnings_date BETWEEN CURRENT_DATE AND CURRENT_DATE + $1
               ORDER BY earnings_date""",
            days_ahead,
        )

    async def upsert_earnings(
        self, ticker: str, earnings_date: date, timing: str = "",
        expected_move: float = 0
    ) -> None:
        await self.execute(
            """INSERT INTO earnings_calendar
               (ticker, earnings_date, timing, expected_move)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (ticker, earnings_date) DO UPDATE
               SET timing = $3, expected_move = $4""",
            ticker, earnings_date, timing, expected_move,
        )

    # ── SQLite: PDT Tracking ──

    def record_day_trade(
        self, ticker: str, strategy_type: str = ""
    ) -> None:
        """Record a day trade for PDT tracking."""
        cur = self._sqlite.cursor()
        cur.execute(
            """INSERT INTO pdt_trades
               (trade_date, ticker, strategy_type, is_day_trade, opened_at)
               VALUES (?, ?, ?, 1, ?)""",
            (date.today().isoformat(), ticker, strategy_type,
             datetime.now().isoformat()),
        )
        self._sqlite.commit()

    def get_day_trade_count(self, rolling_days: int = 5) -> int:
        """Count day trades in the rolling window."""
        cutoff = (date.today() - timedelta(days=rolling_days)).isoformat()
        cur = self._sqlite.cursor()
        cur.execute(
            """SELECT COUNT(*) FROM pdt_trades
               WHERE is_day_trade = 1 AND trade_date >= ?""",
            (cutoff,),
        )
        return cur.fetchone()[0]

    # ── SQLite: IV History for IV Rank calculation ──

    def store_iv_close(self, ticker: str, iv: float, d: date = None) -> None:
        """Store end-of-day IV for IV rank calculation."""
        d = d or date.today()
        cur = self._sqlite.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO iv_history (ticker, date, iv_close)
               VALUES (?, ?, ?)""",
            (ticker, d.isoformat(), iv),
        )
        self._sqlite.commit()

    def get_iv_rank_data(
        self, ticker: str, lookback_days: int = 365
    ) -> List[float]:
        """Get historical IV closes for rank calculation."""
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        cur = self._sqlite.cursor()
        cur.execute(
            """SELECT iv_close FROM iv_history
               WHERE ticker = ? AND date >= ?
               ORDER BY date ASC""",
            (ticker, cutoff),
        )
        return [row[0] for row in cur.fetchall()]

    # ── SQLite: Agent Config ──

    def get_agent_weight(self, agent_id: str) -> float:
        cur = self._sqlite.cursor()
        cur.execute(
            "SELECT weight FROM agent_config WHERE agent_id = ?",
            (agent_id,),
        )
        row = cur.fetchone()
        return row[0] if row else settings.agent_weights.get(agent_id, 1.0)

    def set_agent_weight(self, agent_id: str, weight: float) -> None:
        cur = self._sqlite.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO agent_config
               (agent_id, weight, last_updated)
               VALUES (?, ?, ?)""",
            (agent_id, weight, datetime.now().isoformat()),
        )
        self._sqlite.commit()


# Singleton
db = Database()
