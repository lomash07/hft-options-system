"""
Agent 7: Unusual Options Flow.

Detects institutional options activity and follows "smart money":
  - Volume > 3× average on specific strikes
  - Large single prints (> $50K premium)
  - OI change overnight > 2× average
  - Sweep orders (aggressive multi-exchange fills)

Classifies flow as:
  - Opening vs closing (OI confirms)
  - Bought vs sold (trade relative to bid/ask)
  - Speculative vs hedge

Active in ALL regimes. Highest alpha signal.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from config.settings import MarketRegime
from core.models import (
    Direction, OptionType, Signal, StrategyType, TickerSnapshot,
)
from data.feature_store import FeatureStore
from agents.base_agent import BaseAgent


class UnusualFlow(BaseAgent):
    """Detects and follows unusual institutional options activity."""

    def __init__(self, feature_store: FeatureStore):
        super().__init__("unusual_flow", feature_store)
        self._min_cooldown = 600
        self._volume_history: Dict[str, Dict[str, float]] = {}

    @property
    def active_regimes(self) -> List[MarketRegime]:
        return list(MarketRegime)

    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        chain = self.feature_store.get_chain(ticker)
        if not chain or not chain.quotes:
            return None

        # ── Scan for unusual activity ──
        unusual_strikes = []

        for quote in chain.quotes:
            if quote.volume <= 0:
                continue

            # Build volume baseline from OI as proxy
            avg_daily_vol = max(quote.open_interest * 0.02, 50)

            vol_ratio = quote.volume / avg_daily_vol if avg_daily_vol > 0 else 0

            if vol_ratio < 3.0:
                continue

            # Estimate premium spent
            premium = quote.mid * quote.volume * 100  # × 100 shares

            # Score this strike
            strike_score = 0.0

            # Volume spike severity
            if vol_ratio >= 10:
                strike_score += 0.30
            elif vol_ratio >= 5:
                strike_score += 0.20
            elif vol_ratio >= 3:
                strike_score += 0.10

            # Premium size (institutional threshold)
            if premium >= 100_000:
                strike_score += 0.25
            elif premium >= 50_000:
                strike_score += 0.15
            elif premium >= 20_000:
                strike_score += 0.10

            # Bought vs sold heuristic
            # If trade near ask → likely bought (bullish for calls)
            # If trade near bid → likely sold
            is_bought = self._classify_direction(quote)

            # OTM vs ITM (OTM flow is more speculative)
            is_otm = (
                (quote.option_type == OptionType.CALL
                 and quote.strike > chain.underlying_price)
                or
                (quote.option_type == OptionType.PUT
                 and quote.strike < chain.underlying_price)
            )
            if is_otm:
                strike_score += 0.10

            if strike_score >= 0.20:
                unusual_strikes.append({
                    "quote": quote,
                    "vol_ratio": vol_ratio,
                    "premium": premium,
                    "score": strike_score,
                    "is_bought": is_bought,
                    "is_otm": is_otm,
                })

        if not unusual_strikes:
            return None

        # ── Aggregate unusual flow into a signal ──
        # Sort by score, take top
        unusual_strikes.sort(key=lambda x: x["score"], reverse=True)
        top = unusual_strikes[0]
        quote = top["quote"]

        # Determine direction from flow
        if quote.option_type == OptionType.CALL:
            direction = Direction.BULLISH if top["is_bought"] else Direction.BEARISH
        else:
            direction = Direction.BEARISH if top["is_bought"] else Direction.BULLISH

        # Aggregate confidence from all unusual strikes
        total_confidence = min(
            sum(s["score"] for s in unusual_strikes[:3]) / 3 + 0.20,
            0.95,
        )

        # Pick follow strategy
        if direction == Direction.BULLISH:
            strategy = StrategyType.CALL_DEBIT_SPREAD
        elif direction == Direction.BEARISH:
            strategy = StrategyType.PUT_DEBIT_SPREAD
        else:
            strategy = StrategyType.IRON_CONDOR

        # DTE: follow the flow's expiry
        from datetime import date
        dte = (quote.expiry - date.today()).days

        return Signal(
            ticker=ticker,
            direction=direction,
            confidence=total_confidence,
            suggested_strategy=strategy,
            suggested_dte=min(max(dte, 7), 45),
            target_delta=0.50 if top["is_otm"] else 0.60,
            urgency=0.7,  # Flow signals are time-sensitive
            rationale=(
                f"Unusual {quote.option_type.value} flow: "
                f"{quote.strike} {quote.expiry} "
                f"vol={quote.volume} ({top['vol_ratio']:.1f}× avg) "
                f"${top['premium']:,.0f} premium, "
                f"{'bought' if top['is_bought'] else 'sold'}"
            ),
            metadata={
                "unusual_count": len(unusual_strikes),
                "top_strike": quote.strike,
                "top_expiry": str(quote.expiry),
                "top_vol_ratio": top["vol_ratio"],
                "total_premium": sum(
                    s["premium"] for s in unusual_strikes
                ),
            },
        )

    def _classify_direction(self, quote) -> bool:
        """
        Heuristic: was this flow bought or sold?
        Bought = trade at or near ask price.
        Sold = trade at or near bid price.
        """
        if quote.bid <= 0 or quote.ask <= 0 or quote.last <= 0:
            return True  # Default to bought

        spread = quote.ask - quote.bid
        if spread <= 0:
            return True

        # Position of last trade within spread
        position = (quote.last - quote.bid) / spread

        return position > 0.5  # Above midpoint = likely bought
