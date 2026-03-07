"""
Pre-Market Scanner & Watchlist Builder.

Runs daily at 7:00 AM ET to produce the day's focus list.
Scores all 20 tickers on:
  - IV rank (elevated = theta opportunity)
  - Pre-market gap (breakout candidate)
  - Earnings proximity (earnings plays)
  - Unusual overnight OI changes
  - News catalysts

Output: 5-8 tickers for agents to focus on, with
suggested strategies per ticker.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import List, Optional

import structlog

from config.settings import settings, UNIVERSE
from core.event_bus import STREAM_WATCHLIST, event_bus
from core.models import (
    DailyWatchlist, Direction, StrategyType, WatchlistItem,
)
from core.database import db
from data.broker_feed import DataFeed
from data.options_analytics import options_analytics
from data.feature_store import FeatureStore

logger = structlog.get_logger()


class PreMarketScanner:
    """Builds the daily watchlist from pre-market analysis."""

    def __init__(self, feed: DataFeed, feature_store: FeatureStore):
        self.feed = feed
        self.feature_store = feature_store
        self._today_watchlist: Optional[DailyWatchlist] = None

    async def build_watchlist(self) -> DailyWatchlist:
        """
        Score all 20 tickers and select the top 5-8.
        Should be called at 7:00 AM ET.
        """
        logger.info("scanner.building_watchlist", tickers=len(UNIVERSE))
        today = date.today()
        items: List[WatchlistItem] = []

        # Get VIX and SPY for context
        spy_price = await self.feed.get_price("SPY")
        vix_level = 20.0
        try:
            import yfinance as yf
            vix_data = yf.Ticker("^VIX")
            vix_level = float(vix_data.fast_info.get("lastPrice", 20))
        except Exception:
            pass

        spy_gap = 0.0
        try:
            spy_df = await self.feed.get_historical_bars("SPY", days=5)
            if not spy_df.empty:
                prev_close = float(spy_df["Close"].iloc[-2])
                if spy_price and prev_close > 0:
                    spy_gap = ((spy_price - prev_close) / prev_close) * 100
        except Exception:
            pass

        for ticker in UNIVERSE:
            try:
                item = await self._score_ticker(ticker, today)
                if item:
                    items.append(item)
            except Exception as e:
                logger.warning(
                    "scanner.ticker_error", ticker=ticker, error=str(e)
                )
                continue

        # Sort by score, take top 5-8
        items.sort(key=lambda x: x.score, reverse=True)
        selected = items[:8]

        # Ensure minimum of 5 (add top by IV rank if needed)
        if len(selected) < 5:
            remaining = [i for i in items if i not in selected]
            remaining.sort(key=lambda x: x.iv_rank, reverse=True)
            selected.extend(remaining[: 5 - len(selected)])

        # Determine regime
        regime_info = await event_bus.get_feature_json("regime")
        regime = regime_info.get("regime", "unknown") if regime_info else "unknown"

        watchlist = DailyWatchlist(
            date=today,
            regime=regime,
            items=selected,
            vix_level=vix_level,
            spy_gap_pct=spy_gap,
        )

        self._today_watchlist = watchlist

        # Publish and log
        await event_bus.publish(STREAM_WATCHLIST, {
            "date": today.isoformat(),
            "tickers": [i.ticker for i in selected],
            "regime": regime,
            "vix": vix_level,
            "spy_gap": spy_gap,
        })

        logger.info(
            "scanner.watchlist_built",
            tickers=[i.ticker for i in selected],
            scores=[round(i.score, 2) for i in selected],
            regime=regime,
            vix=vix_level,
        )

        return watchlist

    async def _score_ticker(
        self, ticker: str, today: date
    ) -> Optional[WatchlistItem]:
        """Score a single ticker for watchlist inclusion."""
        score = 0.0
        notes_parts = []
        strategies: List[StrategyType] = []

        # ── Price and gap ──
        price = await self.feed.get_price(ticker)
        if not price or price <= 0:
            return None

        df = await self.feed.get_historical_bars(ticker, days=5)
        gap_pct = 0.0
        if not df.empty and len(df) >= 2:
            prev_close = float(df["Close"].iloc[-2])
            if prev_close > 0:
                gap_pct = ((price - prev_close) / prev_close) * 100

        # Gap score
        if abs(gap_pct) >= 5.0:
            score += 0.25
            notes_parts.append(f"Gap {gap_pct:+.1f}%")
            if gap_pct > 0:
                strategies.append(StrategyType.CALL_DEBIT_SPREAD)
            else:
                strategies.append(StrategyType.PUT_DEBIT_SPREAD)
        elif abs(gap_pct) >= 3.0:
            score += 0.15
            notes_parts.append(f"Gap {gap_pct:+.1f}%")

        # ── IV Rank ──
        iv_rank = 0.0
        chain = await self.feed.get_options_chain(ticker)
        if chain:
            iv_history = db.get_iv_rank_data(ticker)
            metrics = options_analytics.compute_iv_metrics(chain, iv_history)
            iv_rank = metrics.iv_rank

            if iv_rank >= 70:
                score += 0.25
                notes_parts.append(f"High IV rank {iv_rank:.0f}")
                strategies.append(StrategyType.IRON_CONDOR)
            elif iv_rank >= 50:
                score += 0.15
                notes_parts.append(f"IV rank {iv_rank:.0f}")
                strategies.append(StrategyType.PUT_CREDIT_SPREAD)
            elif iv_rank <= 15:
                score += 0.10
                notes_parts.append(f"Low IV rank {iv_rank:.0f}")
                strategies.append(StrategyType.CALENDAR_SPREAD)

            # IV/RV ratio
            if metrics.iv_rv_ratio > 1.3:
                score += 0.15
                notes_parts.append(f"IV/RV {metrics.iv_rv_ratio:.2f}")

        # ── Earnings proximity ──
        earnings_soon = False
        days_to_earn = None
        upcoming = await db.get_upcoming_earnings(days_ahead=14)
        for row in upcoming:
            if row["ticker"] == ticker:
                days_to_earn = (row["earnings_date"] - today).days
                if 0 <= days_to_earn <= 5:
                    earnings_soon = True
                    score += 0.30
                    notes_parts.append(f"Earnings in {days_to_earn}d")
                    strategies.append(StrategyType.IRON_BUTTERFLY)
                elif days_to_earn <= 14:
                    score += 0.10
                    notes_parts.append(f"Earnings {days_to_earn}d")
                break

        # ── Relative volume (use yesterday's as proxy for pre-mkt) ──
        if not df.empty:
            avg_vol = float(df["Volume"].rolling(20).mean().iloc[-1])
            last_vol = float(df["Volume"].iloc[-1])
            rel_vol = last_vol / avg_vol if avg_vol > 0 else 1.0
            if rel_vol > 2.0:
                score += 0.15
                notes_parts.append(f"Vol {rel_vol:.1f}×")

        # ── Unusual OI change (from chain data) ──
        unusual_oi = False
        if chain:
            high_vol_strikes = sum(
                1 for q in chain.quotes
                if q.volume > 0 and q.open_interest > 0
                and q.volume / max(q.open_interest * 0.02, 1) > 3
            )
            if high_vol_strikes >= 3:
                unusual_oi = True
                score += 0.20
                notes_parts.append(f"Unusual flow ({high_vol_strikes} strikes)")

        if score < 0.10:
            return None

        return WatchlistItem(
            ticker=ticker,
            score=round(score, 3),
            iv_rank=round(iv_rank, 1),
            gap_pct=round(gap_pct, 2),
            has_earnings_soon=earnings_soon,
            days_to_earnings=days_to_earn,
            unusual_options_activity=unusual_oi,
            suggested_strategies=strategies[:3],
            notes="; ".join(notes_parts),
        )

    def get_watchlist(self) -> Optional[DailyWatchlist]:
        return self._today_watchlist

    def get_watchlist_tickers(self) -> List[str]:
        if self._today_watchlist:
            return [i.ticker for i in self._today_watchlist.items]
        return UNIVERSE[:10]  # Default to top 10 by market cap
