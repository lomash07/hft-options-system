"""
Core data models shared across all system components.
Uses Pydantic for validation and serialization.
"""

from __future__ import annotations

from datetime import datetime, date
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════
# Enums
# ═══════════════════════════════════════════════

class Direction(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class OptionType(str, Enum):
    CALL = "C"
    PUT = "P"


class StrategyType(str, Enum):
    # Credit strategies (premium selling)
    IRON_CONDOR = "iron_condor"
    IRON_BUTTERFLY = "iron_butterfly"
    PUT_CREDIT_SPREAD = "put_credit_spread"
    CALL_CREDIT_SPREAD = "call_credit_spread"
    SHORT_STRANGLE = "short_strangle"
    SHORT_STRADDLE = "short_straddle"

    # Debit strategies (directional)
    CALL_DEBIT_SPREAD = "call_debit_spread"
    PUT_DEBIT_SPREAD = "put_debit_spread"
    LONG_CALL = "long_call"
    LONG_PUT = "long_put"

    # Calendar / diagonal
    CALENDAR_SPREAD = "calendar_spread"
    DIAGONAL_SPREAD = "diagonal_spread"

    # Stock
    STOCK_LONG = "stock_long"
    STOCK_SHORT = "stock_short"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PositionStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    EXPIRED = "expired"
    ROLLED = "rolled"


# ═══════════════════════════════════════════════
# Market Data Models
# ═══════════════════════════════════════════════

class PriceBar(BaseModel):
    """1-minute OHLCV bar."""
    time: datetime
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None


class OptionQuote(BaseModel):
    """Single option contract quote."""
    ticker: str
    expiry: date
    strike: float
    option_type: OptionType
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    volume: int = 0
    open_interest: int = 0
    implied_vol: float = 0.0
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.last

    @property
    def spread_pct(self) -> float:
        if self.mid > 0:
            return (self.ask - self.bid) / self.mid
        return float("inf")


class OptionsChain(BaseModel):
    """Full options chain for a single ticker."""
    ticker: str
    timestamp: datetime
    underlying_price: float
    quotes: List[OptionQuote] = Field(default_factory=list)

    def get_expiries(self) -> List[date]:
        return sorted(set(q.expiry for q in self.quotes))

    def get_strikes(self, expiry: date) -> List[float]:
        return sorted(set(
            q.strike for q in self.quotes if q.expiry == expiry
        ))

    def get_quote(
        self, expiry: date, strike: float, opt_type: OptionType
    ) -> Optional[OptionQuote]:
        for q in self.quotes:
            if (q.expiry == expiry and q.strike == strike
                    and q.option_type == opt_type):
                return q
        return None

    def get_atm_strike(self, expiry: date) -> float:
        strikes = self.get_strikes(expiry)
        if not strikes:
            return self.underlying_price
        return min(strikes, key=lambda s: abs(s - self.underlying_price))

    def get_by_delta(
        self, expiry: date, target_delta: float, opt_type: OptionType
    ) -> Optional[OptionQuote]:
        """Find the strike closest to target delta."""
        candidates = [
            q for q in self.quotes
            if q.expiry == expiry and q.option_type == opt_type
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda q: abs(abs(q.delta) - abs(target_delta)))


# ═══════════════════════════════════════════════
# IV Surface & Analytics
# ═══════════════════════════════════════════════

class IVMetrics(BaseModel):
    """Implied volatility metrics for a single ticker."""
    ticker: str
    timestamp: datetime
    iv_atm_30d: float = 0.0      # ATM IV for ~30 DTE
    iv_atm_7d: float = 0.0       # ATM IV for ~7 DTE
    iv_rank: float = 0.0         # 0-100 vs 52-week range
    iv_percentile: float = 0.0   # % of days below current
    skew_25d: float = 0.0        # 25-delta put/call skew
    term_slope: float = 0.0      # front vs back month
    rv_20d: float = 0.0          # 20-day realized vol
    iv_rv_ratio: float = 0.0     # IV / RV


class TickerSnapshot(BaseModel):
    """Complete real-time state for a single ticker."""
    ticker: str
    timestamp: datetime
    price: float = 0.0
    vwap: float = 0.0
    volume: int = 0
    relative_volume: float = 0.0  # vs 20-day avg
    daily_change_pct: float = 0.0
    gap_pct: float = 0.0         # pre-market gap
    iv_metrics: Optional[IVMetrics] = None
    beta_spy: float = 1.0
    sector: str = ""
    next_earnings: Optional[date] = None
    days_to_earnings: Optional[int] = None


# ═══════════════════════════════════════════════
# Agent Signals
# ═══════════════════════════════════════════════

class Signal(BaseModel):
    """Trading signal emitted by an agent."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    agent_id: str
    ticker: str
    direction: Direction
    confidence: float = Field(ge=0.0, le=1.0)
    suggested_strategy: Optional[StrategyType] = None
    suggested_dte: Optional[int] = None
    target_delta: Optional[float] = None
    urgency: float = Field(default=0.5, ge=0.0, le=1.0)
    rationale: str = ""
    metadata: Dict = Field(default_factory=dict)

    @property
    def age_seconds(self) -> float:
        return (datetime.utcnow() - self.timestamp).total_seconds()


# ═══════════════════════════════════════════════
# Options Strategy & Legs
# ═══════════════════════════════════════════════

class OptionLeg(BaseModel):
    """Single leg of a multi-leg options strategy."""
    side: OrderSide
    quantity: int
    ticker: str
    expiry: date
    strike: float
    option_type: OptionType
    # Filled after execution
    fill_price: Optional[float] = None
    commission: Optional[float] = None


class StockLeg(BaseModel):
    """Stock position leg (for stock scalps)."""
    side: OrderSide
    quantity: int
    ticker: str
    fill_price: Optional[float] = None
    commission: Optional[float] = None


class StrategyOrder(BaseModel):
    """Complete options strategy order ready for execution."""
    id: str = ""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    ticker: str
    strategy_type: StrategyType
    direction: Direction
    option_legs: List[OptionLeg] = Field(default_factory=list)
    stock_leg: Optional[StockLeg] = None

    # Pricing
    net_credit_debit: float = 0.0    # Positive = credit received
    max_risk: float = 0.0
    max_reward: float = 0.0

    # Context
    agent_id: str = ""
    signal_score: float = 0.0
    market_regime: str = ""
    iv_rank_at_entry: float = 0.0

    # Management targets
    profit_target: float = 0.5       # % of max profit
    stop_loss: float = 2.0           # multiplier of credit
    dte_close: int = 21              # Close if DTE drops below


class Position(BaseModel):
    """Active tracked position."""
    id: str
    opened_at: datetime
    ticker: str
    strategy_type: StrategyType
    direction: Direction
    option_legs: List[OptionLeg] = Field(default_factory=list)
    stock_leg: Optional[StockLeg] = None
    status: PositionStatus = PositionStatus.OPEN

    # Entry pricing
    entry_credit_debit: float = 0.0
    max_risk: float = 0.0
    max_reward: float = 0.0

    # Current state
    current_value: float = 0.0
    unrealized_pnl: float = 0.0
    current_delta: float = 0.0
    current_theta: float = 0.0
    current_gamma: float = 0.0
    current_vega: float = 0.0
    days_to_expiry: int = 0

    # Context
    agent_id: str = ""
    market_regime: str = ""
    profit_target: float = 0.5
    stop_loss: float = 2.0

    # Closed
    closed_at: Optional[datetime] = None
    exit_credit_debit: float = 0.0
    realized_pnl: float = 0.0
    commission_total: float = 0.0

    @property
    def pnl_pct(self) -> float:
        if self.max_risk > 0:
            return self.unrealized_pnl / self.max_risk
        return 0.0

    @property
    def should_take_profit(self) -> bool:
        if self.entry_credit_debit > 0:  # Credit trade
            return self.unrealized_pnl >= (
                self.entry_credit_debit * self.profit_target
            )
        return False

    @property
    def should_stop_loss(self) -> bool:
        if self.entry_credit_debit > 0:
            return self.unrealized_pnl <= -(
                self.entry_credit_debit * self.stop_loss
            )
        elif self.max_risk > 0:
            return self.unrealized_pnl <= -(self.max_risk * 0.40)
        return False


# ═══════════════════════════════════════════════
# Portfolio State
# ═══════════════════════════════════════════════

class PortfolioGreeks(BaseModel):
    """Aggregate portfolio-level Greeks."""
    net_delta: float = 0.0
    net_gamma: float = 0.0
    net_theta: float = 0.0
    net_vega: float = 0.0
    total_risk: float = 0.0
    buying_power_used: float = 0.0
    buying_power_pct: float = 0.0


class PortfolioState(BaseModel):
    """Complete portfolio snapshot."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    positions: List[Position] = Field(default_factory=list)
    greeks: PortfolioGreeks = Field(default_factory=PortfolioGreeks)
    daily_pnl: float = 0.0
    open_pnl: float = 0.0
    day_trades_used: int = 0
    circuit_breaker_level: int = 0  # 0=normal, 1=warning, 2=stop, 3=kill


# ═══════════════════════════════════════════════
# Watchlist
# ═══════════════════════════════════════════════

class WatchlistItem(BaseModel):
    """Single ticker in the daily watchlist."""
    ticker: str
    score: float = 0.0
    iv_rank: float = 0.0
    gap_pct: float = 0.0
    has_earnings_soon: bool = False
    days_to_earnings: Optional[int] = None
    unusual_options_activity: bool = False
    suggested_strategies: List[StrategyType] = Field(default_factory=list)
    notes: str = ""


class DailyWatchlist(BaseModel):
    """Pre-market watchlist output."""
    date: date
    regime: str = "unknown"
    items: List[WatchlistItem] = Field(default_factory=list)
    vix_level: float = 0.0
    spy_gap_pct: float = 0.0
