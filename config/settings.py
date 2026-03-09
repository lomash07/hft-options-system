"""
Central configuration for the HFT Options Trading System.
Loads from .env file with sensible defaults for development.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class TradingMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


class MarketRegime(str, Enum):
    BULL_TREND = "bull_trend"
    BEAR_TREND = "bear_trend"
    LOW_VOL = "low_vol"
    HIGH_VOL = "high_vol"
    CHOPPY = "choppy"


class SessionPhase(str, Enum):
    PRE_MARKET = "pre_market"       # 4:00 - 9:30 ET
    OPEN_STRIKE = "open_strike"     # 9:30 - 11:00 ET
    MIDDAY = "midday"               # 11:00 - 14:30 ET
    CLOSE_STRIKE = "close_strike"   # 14:30 - 16:00 ET
    POST_MARKET = "post_market"     # 16:00 - 18:00 ET
    CLOSED = "closed"               # 18:00 - 4:00 ET


# ── Top 20 Mega-Cap Universe ──
UNIVERSE: List[str] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "TSLA", "BRK-B", "LLY", "UNH",
    "AVGO", "JPM", "V", "XOM", "MA",
    "JNJ", "PG", "HD", "COST", "NFLX",
]

# SPY + VIX for regime detection
REGIME_TICKERS = ["SPY", "QQQ", "VIX"]

# All tickers to track
ALL_TICKERS = UNIVERSE + ["SPY", "QQQ"]


class Settings(BaseSettings):
    """Application settings loaded from environment / .env file."""

    # ── Mode ──
    trading_mode: TradingMode = TradingMode.PAPER
    log_level: str = "INFO"
    debug: bool = False

    # ── Database ──
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "hft_options"
    db_user: str = "hft"
    db_password: str = "hft_dev_2024"

    @property
    def db_url(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def async_db_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # ── Redis ──
    redis_host: str = "localhost"
    redis_port: int = 6379

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}"

    # ── Interactive Brokers ──
    ib_host: str = "127.0.0.1"
    ib_port: int = 4002  # 4002 = paper
    ib_client_id: int = 1

    # ── Telegram ──
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # ── Capital & Risk ──
    total_capital: float = 20000.0
    max_risk_per_trade: float = 400.0    # 2%
    max_risk_per_day: float = 800.0      # 4%
    max_concurrent_positions: int = 5
    options_capital_pct: float = 0.80
    stock_capital_pct: float = 0.20

    # ── Circuit Breakers ──
    cb_warning_loss: float = -400.0      # Halve position sizes
    cb_stop_loss: float = -600.0         # Block new trades
    cb_kill_loss: float = -800.0         # Close everything

    # ── PDT ──
    pdt_enabled: bool = True
    pdt_max_day_trades: int = 3

    # ── Options Defaults ──
    default_dte_premium: int = 35        # 30-45 DTE for theta
    default_dte_directional: int = 10    # 7-14 DTE for momentum
    min_iv_rank_theta: float = 40.0      # Min IV rank for premium selling
    target_delta_short: float = 0.30     # 30-delta for short strikes
    profit_target_pct: float = 0.50      # Close at 50% profit
    stop_loss_pct: float = 2.0           # Close at 2x credit received
    min_open_interest: int = 100
    max_bid_ask_spread_pct: float = 0.10 # 10% of mid

    # ── Agent Weights (Phase 2-3) ──
    agent_weights: dict = Field(default_factory=lambda: {
        "theta_harvester": 1.5,
        "vol_arb": 1.4,
        "unusual_flow": 1.5,
        "directional_momentum": 1.0,
        "earnings_vol": 1.3,
        "mean_reversion": 0.9,
        "news_catalyst": 1.3,
        "ml_ensemble": 1.2,
    })

    # ── Session Schedule (ET hours) ──
    pre_market_start: str = "04:00"
    open_strike_start: str = "09:30"
    midday_start: str = "11:00"
    close_strike_start: str = "14:30"
    market_close: str = "16:00"
    post_market_end: str = "18:00"

    # ── Feature Store TTL (seconds) ──
    feature_ttl: int = 60
    signal_staleness: int = 60

    # ── Metrics ──
    metrics_port: int = 8000

    # ── Data Source Keys (optional free tiers) ──
    tradier_api_key: str = ""
    tradier_sandbox: bool = True

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


# Singleton
settings = Settings()
