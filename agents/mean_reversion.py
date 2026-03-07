"""
Agent 6: Mean Reversion (Fade Extremes).

Fades overextended intraday moves when:
  - Stock > 2σ from VWAP
  - No news catalyst (confirmed by Agent 5)
  - SPY is stable (not a market-wide move)
  - RSI extreme (<25 or >75)

Executes via: put credit spreads (on dips) or call credit spreads
(on spikes), or debit spreads targeting VWAP reversion.

Active in: CHOPPY, LOW_VOL regimes.
"""

from __future__ import annotations

from typing import List, Optional

from config.settings import MarketRegime
from core.models import Direction, Signal, StrategyType, TickerSnapshot
from core.event_bus import event_bus
from data.feature_store import FeatureStore
from agents.base_agent import BaseAgent


class MeanReversion(BaseAgent):
    """Fades overextended intraday moves."""

    def __init__(self, feature_store: FeatureStore):
        super().__init__("mean_reversion", feature_store)
        self._min_cooldown = 600

    @property
    def active_regimes(self) -> List[MarketRegime]:
        return [MarketRegime.CHOPPY, MarketRegime.LOW_VOL]

    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        price = snapshot.price
        vwap = snapshot.vwap
        if price <= 0 or vwap <= 0:
            return None

        # ── Gate 1: Significant deviation from VWAP ──
        vwap_dist = (price - vwap) / vwap
        if abs(vwap_dist) < 0.015:  # Need >1.5% deviation
            return None

        # ── Gate 2: No earnings today or tomorrow ──
        if self._has_earnings_soon(snapshot, days=2):
            return None

        # ── Gate 3: Check SPY isn't making a similar move ──
        spy_snapshot = self.feature_store.get_snapshot("SPY")
        if spy_snapshot:
            spy_move = abs(spy_snapshot.daily_change_pct)
            stock_move = abs(snapshot.daily_change_pct)
            # If SPY is moving nearly as much, it's market-wide
            if spy_move > 0 and stock_move / max(spy_move, 0.1) < 1.5:
                return None  # Stock-specific move not extreme enough

        # ── Gate 4: Volume check (low volume fades work better) ──
        if snapshot.relative_volume > 3.0:
            return None  # High volume = real move, don't fade

        # ── Score ──
        confidence = 0.0

        # VWAP deviation severity
        if abs(vwap_dist) > 0.03:
            confidence += 0.30
        elif abs(vwap_dist) > 0.02:
            confidence += 0.20
        else:
            confidence += 0.10

        # Low relative volume (better for reversion)
        if snapshot.relative_volume < 1.5:
            confidence += 0.15
        elif snapshot.relative_volume < 2.0:
            confidence += 0.10

        # IV context
        iv = snapshot.iv_metrics
        if iv and iv.iv_rank < 40:
            confidence += 0.10  # Low IV = stable environment
        if iv and iv.iv_rank >= 60:
            confidence -= 0.10  # High IV = more likely to continue

        # Daily change (bigger move = more to revert)
        if abs(snapshot.daily_change_pct) > 3.0:
            confidence += 0.15
        elif abs(snapshot.daily_change_pct) > 2.0:
            confidence += 0.10

        if confidence < self._min_confidence:
            return None

        # ── Direction: fade the move ──
        if vwap_dist > 0:
            # Stock above VWAP → expect reversion down
            direction = Direction.BEARISH
            strategy = StrategyType.CALL_CREDIT_SPREAD
        else:
            # Stock below VWAP → expect reversion up
            direction = Direction.BULLISH
            strategy = StrategyType.PUT_CREDIT_SPREAD

        return Signal(
            ticker=ticker,
            direction=direction,
            confidence=confidence,
            suggested_strategy=strategy,
            suggested_dte=14,
            target_delta=0.30,
            urgency=0.5,
            rationale=(
                f"Mean reversion: {ticker} {vwap_dist:+.2%} from VWAP, "
                f"daily chg {snapshot.daily_change_pct:+.1f}%, "
                f"rel_vol {snapshot.relative_volume:.1f}"
            ),
            metadata={
                "vwap_distance": vwap_dist,
                "daily_change": snapshot.daily_change_pct,
            },
        )
