"""
Agent 1: Theta Harvester (Premium Seller)

Core strategy: Sell premium when IV rank is elevated.
Selects structure based on market regime:
  🔵 LOW VOL:  Iron Condors (30Δ wings)
  🟡 HIGH VOL: Wide strangles / wide ICs
  🟢 BULL:     Put credit spreads
  🔴 BEAR:     Call credit spreads

Edge: Options are overpriced ~70% of the time (IV > RV).
Theta decay is a structural, repeatable edge.
"""

from __future__ import annotations

from typing import List, Optional

from config.settings import MarketRegime, settings
from core.models import (
    Direction, Signal, StrategyType, TickerSnapshot,
)
from data.feature_store import FeatureStore
from agents.base_agent import BaseAgent


class ThetaHarvester(BaseAgent):
    """Sells premium when IV rank is high."""

    def __init__(self, feature_store: FeatureStore):
        super().__init__("theta_harvester", feature_store)
        self._min_cooldown = 600  # 10 min cooldown per ticker

    @property
    def active_regimes(self) -> List[MarketRegime]:
        return [
            MarketRegime.LOW_VOL,
            MarketRegime.HIGH_VOL,
            MarketRegime.CHOPPY,
        ]

    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        """
        Look for premium selling opportunities.

        Triggers when:
          1. IV rank > 40 (elevated implied vol)
          2. No earnings within DTE window
          3. Bid-ask spread acceptable
        """
        iv = snapshot.iv_metrics
        if not iv:
            return None

        # ── Gate 1: IV rank must be elevated ──
        if iv.iv_rank < settings.min_iv_rank_theta:
            return None

        # ── Gate 2: No earnings in the next 45 days ──
        if self._has_earnings_soon(snapshot, days=45):
            return None

        # ── Gate 3: IV should exceed realized vol ──
        if iv.iv_rv_ratio > 0 and iv.iv_rv_ratio < 1.1:
            return None  # IV not sufficiently above RV

        # ── Determine Direction & Strategy by Regime ──
        regime_data = await self._get_regime()
        direction, strategy = self._select_strategy(regime_data, iv)

        # ── Confidence Scoring ──
        confidence = self._score_confidence(iv, snapshot)

        if confidence < self._min_confidence:
            return None

        return Signal(
            ticker=ticker,
            direction=direction,
            confidence=confidence,
            suggested_strategy=strategy,
            suggested_dte=settings.default_dte_premium,
            target_delta=settings.target_delta_short,
            urgency=0.3,  # Low urgency — theta strategies aren't time-critical
            rationale=(
                f"IV Rank {iv.iv_rank:.0f}%, "
                f"IV/RV {iv.iv_rv_ratio:.2f}, "
                f"30d IV {iv.iv_atm_30d:.1%}"
            ),
            metadata={
                "iv_rank": iv.iv_rank,
                "iv_rv_ratio": iv.iv_rv_ratio,
                "iv_atm_30d": iv.iv_atm_30d,
                "skew_25d": iv.skew_25d,
            },
        )

    def _select_strategy(
        self, regime: str, iv
    ) -> tuple:
        """Pick optimal strategy based on regime."""
        if regime == MarketRegime.LOW_VOL.value:
            # Low vol: iron condors (range-bound)
            return Direction.NEUTRAL, StrategyType.IRON_CONDOR

        elif regime == MarketRegime.HIGH_VOL.value:
            # High vol: wider structures, bigger credit
            if iv.skew_25d > 0.05:
                # Heavy put skew — sell puts
                return Direction.BULLISH, StrategyType.PUT_CREDIT_SPREAD
            return Direction.NEUTRAL, StrategyType.IRON_CONDOR

        elif regime == MarketRegime.BULL_TREND.value:
            return Direction.BULLISH, StrategyType.PUT_CREDIT_SPREAD

        elif regime == MarketRegime.BEAR_TREND.value:
            return Direction.BEARISH, StrategyType.CALL_CREDIT_SPREAD

        else:  # CHOPPY
            return Direction.NEUTRAL, StrategyType.IRON_CONDOR

    def _score_confidence(
        self, iv, snapshot: TickerSnapshot
    ) -> float:
        """
        Score confidence based on multiple factors.
        Range: 0.0 to 1.0
        """
        score = 0.0

        # IV Rank (higher = better for selling)
        if iv.iv_rank >= 80:
            score += 0.30
        elif iv.iv_rank >= 60:
            score += 0.20
        elif iv.iv_rank >= 40:
            score += 0.10

        # IV/RV ratio (higher = more overpriced)
        if iv.iv_rv_ratio >= 1.5:
            score += 0.25
        elif iv.iv_rv_ratio >= 1.3:
            score += 0.20
        elif iv.iv_rv_ratio >= 1.1:
            score += 0.10

        # Term structure (contango = normal, good for selling)
        if iv.term_slope > 0:  # Back > front (normal)
            score += 0.10
        elif iv.term_slope < -0.05:  # Backwardation (fear)
            score -= 0.10

        # Relative volume (high = more attention, wider spreads)
        if snapshot.relative_volume < 2.0:
            score += 0.10
        elif snapshot.relative_volume > 4.0:
            score -= 0.10  # Something unusual happening

        # Skew premium (puts expensive = sell them)
        if iv.skew_25d > 0.03:
            score += 0.10

        return min(max(score, 0.0), 1.0)

    async def _get_regime(self) -> str:
        data = await self.feature_store.feed.get_price("SPY")
        regime_info = await event_bus.get_feature_json("regime")
        if regime_info:
            return regime_info.get("regime", MarketRegime.CHOPPY.value)
        return MarketRegime.CHOPPY.value
