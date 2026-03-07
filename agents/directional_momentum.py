"""
Agent 3: Directional Momentum.

Rides breakouts using debit spreads for defined risk:
  - Opening range breakouts (first 30 min)
  - VWAP reclaim/reject patterns
  - Multi-timeframe confirmation (1min entry, 5min trend)

Active in: BULL_TREND, BEAR_TREND (trending regimes only)
Also triggers stock scalps for the sharpest setups.
"""

from __future__ import annotations

from typing import List, Optional

import numpy as np

from config.settings import MarketRegime, settings
from core.models import Direction, Signal, StrategyType, TickerSnapshot
from data.feature_store import FeatureStore
from agents.base_agent import BaseAgent


class DirectionalMomentum(BaseAgent):
    """Trades momentum breakouts with debit spreads."""

    def __init__(self, feature_store: FeatureStore):
        super().__init__("directional_momentum", feature_store)
        self._min_cooldown = 300
        self._opening_ranges: dict = {}  # ticker → (high, low)

    @property
    def active_regimes(self) -> List[MarketRegime]:
        return [MarketRegime.BULL_TREND, MarketRegime.BEAR_TREND]

    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        price = snapshot.price
        vwap = snapshot.vwap
        if price <= 0 or vwap <= 0:
            return None

        # ── VWAP Analysis ──
        vwap_distance = (price - vwap) / vwap
        above_vwap = price > vwap

        # ── Volume Confirmation ──
        strong_volume = snapshot.relative_volume >= 1.5

        # ── Gap Analysis ──
        gap = snapshot.gap_pct
        large_gap = abs(gap) > 2.0

        # ── Directional Scoring ──
        bull_score = 0.0
        bear_score = 0.0

        # VWAP reclaim (bullish: price crossed above VWAP)
        if above_vwap and vwap_distance > 0.002:
            bull_score += 0.20
        elif not above_vwap and abs(vwap_distance) > 0.002:
            bear_score += 0.20

        # Strong relative volume
        if strong_volume:
            bull_score += 0.15 if above_vwap else 0
            bear_score += 0.15 if not above_vwap else 0

        # Gap and go (gap + continuation)
        if large_gap:
            if gap > 0 and above_vwap:
                bull_score += 0.20
            elif gap < 0 and not above_vwap:
                bear_score += 0.20

        # Daily change momentum
        if snapshot.daily_change_pct > 1.5:
            bull_score += 0.15
        elif snapshot.daily_change_pct < -1.5:
            bear_score += 0.15

        # IV context (low IV = cheaper directional options)
        iv = snapshot.iv_metrics
        if iv and iv.iv_rank < 30:
            # Low IV = debit spreads are cheaper
            bull_score += 0.10
            bear_score += 0.10

        # ── Select Direction ──
        if bull_score > bear_score and bull_score >= 0.50:
            direction = Direction.BULLISH
            confidence = min(bull_score, 0.95)
            strategy = StrategyType.CALL_DEBIT_SPREAD
        elif bear_score > bull_score and bear_score >= 0.50:
            direction = Direction.BEARISH
            confidence = min(bear_score, 0.95)
            strategy = StrategyType.PUT_DEBIT_SPREAD
        else:
            return None

        # Check if stock scalp might be better
        # (sharp move, low IV → shares may be cleaner)
        use_stock = (
            iv and iv.iv_rank < 20
            and abs(snapshot.daily_change_pct) > 2.0
            and snapshot.relative_volume > 2.0
        )
        if use_stock:
            strategy = (
                StrategyType.STOCK_LONG if direction == Direction.BULLISH
                else StrategyType.STOCK_SHORT
            )

        return Signal(
            ticker=ticker,
            direction=direction,
            confidence=confidence,
            suggested_strategy=strategy,
            suggested_dte=settings.default_dte_directional,
            target_delta=0.60,  # ITM-ish for directional
            urgency=0.8,  # Momentum is time-sensitive
            rationale=(
                f"{'Bull' if direction == Direction.BULLISH else 'Bear'} "
                f"momentum: VWAP dist={vwap_distance:.3f}, "
                f"rel_vol={snapshot.relative_volume:.1f}, "
                f"gap={snapshot.gap_pct:.1f}%"
            ),
            metadata={
                "vwap_distance": vwap_distance,
                "above_vwap": above_vwap,
                "gap_pct": snapshot.gap_pct,
                "use_stock": use_stock,
            },
        )
