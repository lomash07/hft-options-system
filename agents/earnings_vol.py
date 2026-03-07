"""
Agent 4: Earnings Vol Player.

Exploits the systematic IV crush after earnings announcements:
  Pre-earnings (1-5 days): Sell premium via iron condors/butterflies
  Post-earnings (day of): Fade excessive gaps or follow momentum

IV typically drops 30-60% after earnings → selling premium is
systematically profitable if sized correctly.
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional

from config.settings import MarketRegime, settings
from core.models import Direction, Signal, StrategyType, TickerSnapshot
from data.feature_store import FeatureStore
from agents.base_agent import BaseAgent


class EarningsVol(BaseAgent):
    """Trades IV crush around earnings events."""

    def __init__(self, feature_store: FeatureStore):
        super().__init__("earnings_vol", feature_store)
        self._min_cooldown = 1800  # 30 min — earnings are event-driven

    @property
    def active_regimes(self) -> List[MarketRegime]:
        return list(MarketRegime)  # Earnings override regime

    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        iv = snapshot.iv_metrics
        if not iv:
            return None

        dte = snapshot.days_to_earnings
        if dte is None:
            return None

        # ── Pre-Earnings: 1-5 days before ──
        if 1 <= dte <= 5:
            return self._pre_earnings_signal(ticker, snapshot, iv, dte)

        # ── Post-Earnings: Day of or day after ──
        if dte == 0:
            return self._post_earnings_signal(ticker, snapshot, iv)

        return None

    def _pre_earnings_signal(
        self, ticker: str, snapshot: TickerSnapshot, iv, dte: int
    ) -> Optional[Signal]:
        """
        Sell premium before earnings to capture IV crush.
        Iron condor / iron butterfly with wings at expected move width.
        """
        # Only trade if IV is elevated (it should be pre-earnings)
        if iv.iv_rank < 50:
            return None

        # Higher confidence closer to earnings (more IV built in)
        confidence = 0.50
        if dte <= 2:
            confidence += 0.20  # 1-2 days = peak IV
        elif dte <= 3:
            confidence += 0.15
        else:
            confidence += 0.10

        # IV rank boost
        if iv.iv_rank >= 80:
            confidence += 0.15
        elif iv.iv_rank >= 60:
            confidence += 0.10

        strategy = (
            StrategyType.IRON_BUTTERFLY if iv.iv_rank >= 75
            else StrategyType.IRON_CONDOR
        )

        # Use the weekly that expires right after earnings
        # DTE for the trade should be the earnings DTE + 1-3 days
        trade_dte = dte + 2  # Expire shortly after

        return Signal(
            ticker=ticker,
            direction=Direction.NEUTRAL,
            confidence=min(confidence, 0.90),
            suggested_strategy=strategy,
            suggested_dte=trade_dte,
            target_delta=0.20,  # Tighter for earnings plays
            urgency=0.6,
            rationale=(
                f"Pre-earnings ({dte}d out): "
                f"IV Rank {iv.iv_rank:.0f}%, "
                f"selling IV crush via {strategy.value}"
            ),
            metadata={
                "earnings_dte": dte,
                "iv_rank": iv.iv_rank,
                "play_type": "pre_earnings",
            },
        )

    def _post_earnings_signal(
        self, ticker: str, snapshot: TickerSnapshot, iv
    ) -> Optional[Signal]:
        """
        Post-earnings: react to the gap.
        If gap > expected move → fade it (mean reversion)
        If gap < expected move → momentum follow
        """
        gap = abs(snapshot.gap_pct)

        if gap < 1.0:
            return None  # No significant move

        # Heuristic: expected move ≈ front-week ATM straddle price
        # For now use IV rank as proxy
        expected_move_pct = iv.iv_atm_7d * 100 / 4  # Rough approximation

        if gap > expected_move_pct * 1.5:
            # Gap exceeded expected → fade
            direction = (
                Direction.BEARISH if snapshot.gap_pct > 0
                else Direction.BULLISH
            )
            strategy = (
                StrategyType.PUT_DEBIT_SPREAD
                if direction == Direction.BEARISH
                else StrategyType.CALL_DEBIT_SPREAD
            )
            rationale = f"Post-ER fade: gap {snapshot.gap_pct:.1f}% > expected"
        else:
            # Gap within or below expected → momentum
            direction = (
                Direction.BULLISH if snapshot.gap_pct > 0
                else Direction.BEARISH
            )
            strategy = (
                StrategyType.CALL_DEBIT_SPREAD
                if direction == Direction.BULLISH
                else StrategyType.PUT_DEBIT_SPREAD
            )
            rationale = f"Post-ER momentum: gap {snapshot.gap_pct:.1f}%"

        return Signal(
            ticker=ticker,
            direction=direction,
            confidence=0.60,
            suggested_strategy=strategy,
            suggested_dte=14,  # 2-week for post-ER continuation
            target_delta=0.55,
            urgency=0.9,  # Time-critical post-earnings
            rationale=rationale,
            metadata={
                "gap_pct": snapshot.gap_pct,
                "play_type": "post_earnings",
            },
        )
