"""
Agent 2: Volatility Arbitrage.

Trades the mismatch between implied and realized volatility.
  - IV >> RV: Sell vol (straddles, strangles, iron condors)
  - IV << RV: Buy vol (straddles — rare, only in strong trends)
  - Skew trades: sell rich side, buy cheap side
  - Calendar trades: sell expensive front, buy cheap back

Active in ALL regimes. This is a market-neutral strategy.
"""

from __future__ import annotations

from typing import List, Optional

from config.settings import MarketRegime, settings
from core.models import Direction, Signal, StrategyType, TickerSnapshot
from data.feature_store import FeatureStore
from agents.base_agent import BaseAgent


class VolArb(BaseAgent):
    """Exploits IV vs RV mismatch."""

    def __init__(self, feature_store: FeatureStore):
        super().__init__("vol_arb", feature_store)
        self._min_cooldown = 900  # 15 min cooldown

    @property
    def active_regimes(self) -> List[MarketRegime]:
        return list(MarketRegime)  # All regimes

    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        iv = snapshot.iv_metrics
        if not iv:
            return None

        # Need valid IV/RV ratio
        if iv.iv_rv_ratio <= 0 or iv.rv_20d <= 0:
            return None

        # Block near earnings (IV crush is Agent 4's domain)
        if self._has_earnings_soon(snapshot, days=10):
            return None

        signal = None

        # ── Case 1: IV significantly > RV → Sell volatility ──
        if iv.iv_rv_ratio > 1.3:
            confidence = self._score_sell_vol(iv, snapshot)
            if confidence >= self._min_confidence:
                strategy = self._pick_sell_structure(iv)
                signal = Signal(
                    ticker=ticker,
                    direction=Direction.NEUTRAL,
                    confidence=confidence,
                    suggested_strategy=strategy,
                    suggested_dte=settings.default_dte_premium,
                    target_delta=0.25,
                    urgency=0.4,
                    rationale=(
                        f"IV/RV={iv.iv_rv_ratio:.2f} "
                        f"(IV={iv.iv_atm_30d:.1%} vs RV={iv.rv_20d:.1%}), "
                        f"sell vol"
                    ),
                    metadata={
                        "iv_rv_ratio": iv.iv_rv_ratio,
                        "action": "sell_vol",
                    },
                )

        # ── Case 2: IV significantly < RV → Buy volatility (rare) ──
        elif iv.iv_rv_ratio < 0.8:
            confidence = self._score_buy_vol(iv, snapshot)
            if confidence >= 0.70:  # Higher threshold for buying vol
                signal = Signal(
                    ticker=ticker,
                    direction=Direction.NEUTRAL,
                    confidence=confidence,
                    suggested_strategy=StrategyType.CALENDAR_SPREAD,
                    suggested_dte=14,
                    urgency=0.5,
                    rationale=(
                        f"IV/RV={iv.iv_rv_ratio:.2f} "
                        f"(IV={iv.iv_atm_30d:.1%} vs RV={iv.rv_20d:.1%}), "
                        f"buy vol via calendar"
                    ),
                    metadata={
                        "iv_rv_ratio": iv.iv_rv_ratio,
                        "action": "buy_vol",
                    },
                )

        # ── Case 3: Term structure anomaly ──
        elif abs(iv.term_slope) > 0.10:
            confidence = self._score_term_trade(iv)
            if confidence >= self._min_confidence:
                signal = Signal(
                    ticker=ticker,
                    direction=Direction.NEUTRAL,
                    confidence=confidence,
                    suggested_strategy=StrategyType.CALENDAR_SPREAD,
                    suggested_dte=30,
                    urgency=0.3,
                    rationale=(
                        f"Term structure anomaly: slope={iv.term_slope:.3f}, "
                        f"front IV={iv.iv_atm_7d:.1%} vs back={iv.iv_atm_30d:.1%}"
                    ),
                    metadata={
                        "term_slope": iv.term_slope,
                        "action": "term_trade",
                    },
                )

        return signal

    def _score_sell_vol(self, iv, snapshot: TickerSnapshot) -> float:
        score = 0.0
        # IV/RV gap
        if iv.iv_rv_ratio >= 1.5:
            score += 0.35
        elif iv.iv_rv_ratio >= 1.3:
            score += 0.25
        # IV rank
        if iv.iv_rank >= 60:
            score += 0.20
        elif iv.iv_rank >= 40:
            score += 0.10
        # Stability (low relative volume = stable)
        if snapshot.relative_volume < 2.0:
            score += 0.15
        # Normal term structure
        if iv.term_slope >= 0:
            score += 0.10
        return min(score, 1.0)

    def _score_buy_vol(self, iv, snapshot: TickerSnapshot) -> float:
        score = 0.0
        if iv.iv_rv_ratio < 0.7:
            score += 0.35
        elif iv.iv_rv_ratio < 0.8:
            score += 0.25
        if iv.iv_rank < 20:
            score += 0.20
        if snapshot.relative_volume > 1.5:
            score += 0.15
        return min(score, 1.0)

    def _score_term_trade(self, iv) -> float:
        score = 0.0
        if abs(iv.term_slope) > 0.15:
            score += 0.30
        elif abs(iv.term_slope) > 0.10:
            score += 0.20
        if iv.iv_rank > 30:
            score += 0.15
        score += 0.15  # Base for any term anomaly
        return min(score, 1.0)

    def _pick_sell_structure(self, iv) -> StrategyType:
        """Pick best structure for selling vol."""
        if iv.iv_rank >= 70:
            return StrategyType.IRON_CONDOR
        elif iv.skew_25d > 0.05:
            # Puts rich → sell put credit spread
            return StrategyType.PUT_CREDIT_SPREAD
        else:
            return StrategyType.IRON_CONDOR
