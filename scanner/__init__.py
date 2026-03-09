"""
Pre-Market Scanner & Watchlist Builder.

FIX: Uses cached data from feature_store.initialize() instead of
     making individual API calls per ticker during scoring.
     Options chains fetched only for top candidates (not all 20).
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
from data.broker_feed import DataFeed, YFinanceFeed
from data.options_analytics import options_analytics
from data.feature_store import FeatureStore
from monitoring import iv_rank_gauge

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
        
        KEY FIX: Scores tickers using data already cached by
        feature_store.initialize(). Only fetches options chains
        for the top 10 candidates (not all 20).
        """
        logger.info("scanner.building_watchlist", tickers=len(UNIVERSE))
        today = date.today()
        items: List[WatchlistItem] = []

        # ── Get SPY/VIX from cache (no API call) ──
        spy_price = self.feature_store.get_cached_price("SPY")
        spy_prev = self.feature_store.get_prev_close("SPY")
        spy_gap = 0.0
        if spy_price and spy_prev and spy_prev > 0:
            spy_gap = ((spy_price - spy_prev) / spy_prev) * 100

        # VIX — try cache first, then one careful API call
        vix_level = 20.0
        try:
            import yfinance as yf
            vix_data = yf.download("^VIX", period="1d", progress=False)
            if not vix_data.empty:
                vix_level = float(vix_data["Close"].values[-1])
        except Exception:
            pass

        # ── Score each ticker using CACHED data (no API calls) ──
        for ticker in UNIVERSE:
            try:
                item = self._score_ticker_from_cache(ticker, today)
                if item:
                    items.append(item)
            except Exception as e:
                logger.warning(
                    "scanner.ticker_error", ticker=ticker, error=str(e)
                )
                continue

        # Sort by score
        items.sort(key=lambda x: x.score, reverse=True)

        # ── Fetch options chains ONLY for top 10 candidates ──
        # This is where we make targeted API calls (with delays)
        top_candidates = items[:10]
        logger.info(
            "scanner.fetching_chains",
            tickers=[i.ticker for i in top_candidates],
        )

        for item in top_candidates:
            try:
                chain = await self.feed.get_options_chain(item.ticker)
                if chain:
                    iv_history = db.get_iv_rank_data(item.ticker)
                    metrics = options_analytics.compute_iv_metrics(
                        chain, iv_history
                    )
                    item.iv_rank = round(metrics.iv_rank, 1)
                    iv_rank_gauge.labels(ticker=item.ticker).set(item.iv_rank)

                    # Boost score based on IV rank
                    if metrics.iv_rank >= 70:
                        item.score += 0.25
                        item.notes += f"; High IV rank {metrics.iv_rank:.0f}"
                        if StrategyType.IRON_CONDOR not in item.suggested_strategies:
                            item.suggested_strategies.append(StrategyType.IRON_CONDOR)
                    elif metrics.iv_rank >= 50:
                        item.score += 0.15
                        item.notes += f"; IV rank {metrics.iv_rank:.0f}"
                    elif metrics.iv_rank <= 15:
                        item.score += 0.10
                        item.notes += f"; Low IV = cheap debit spreads"

                    # IV/RV ratio
                    if metrics.iv_rv_ratio > 1.3:
                        item.score += 0.15
                        item.notes += f"; IV/RV {metrics.iv_rv_ratio:.2f}"

                    # Check for unusual options activity
                    high_vol_strikes = sum(
                        1 for q in chain.quotes
                        if q.volume > 0 and q.open_interest > 0
                        and q.volume / max(q.open_interest * 0.02, 1) > 3
                    )
                    if high_vol_strikes >= 3:
                        item.unusual_options_activity = True
                        item.score += 0.20
                        item.notes += f"; Unusual flow ({high_vol_strikes} strikes)"

                await asyncio.sleep(2)  # 2 second delay between chain fetches

            except Exception as e:
                logger.warning(
                    "scanner.chain_error",
                    ticker=item.ticker, error=str(e),
                )
                continue

        # Re-sort after IV rank updates
        items.sort(key=lambda x: x.score, reverse=True)
        selected = items[:8]

        # Ensure minimum of 5
        if len(selected) < 5:
            remaining = [i for i in items if i not in selected]
            selected.extend(remaining[: 5 - len(selected)])

        # Determine regime
        regime_info = await event_bus.get_feature_json("regime")
        regime = regime_info.get("regime", "unknown") if regime_info else "unknown"

        watchlist = DailyWatchlist(
            date=today,
            regime=regime,
            items=selected,
            vix_level=vix_level,
            spy_gap_pct=round(spy_gap, 2),
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

    def _score_ticker_from_cache(
        self, ticker: str, today: date
    ) -> Optional[WatchlistItem]:
        """
        Score a ticker using ONLY cached data from feature_store.
        No API calls here — this is the key fix.
        """
        price = self.feature_store.get_cached_price(ticker)
        if not price or price <= 0:
            return None

        prev_close = self.feature_store.get_prev_close(ticker)
        avg_volume = self.feature_store.get_avg_volume(ticker)

        score = 0.0
        notes_parts = []
        strategies: List[StrategyType] = []

        # ── Gap from previous close ──
        gap_pct = 0.0
        if prev_close and prev_close > 0:
            gap_pct = ((price - prev_close) / prev_close) * 100

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
        elif abs(gap_pct) >= 1.5:
            score += 0.05
            notes_parts.append(f"Gap {gap_pct:+.1f}%")

        # ── Earnings proximity (from database) ──
        earnings_soon = False
        days_to_earn = None
        # This is a sync call to SQLite — no API request
        try:
            # Check if we have earnings data in the DB
            # (Will be populated as we collect data over time)
            pass  # Earnings data will be sparse initially
        except Exception:
            pass

        # ── Base score for all tickers (so watchlist isn't empty) ──
        score += 0.05  # Every ticker gets a small base score

        # ── Price level scoring (higher priced = often better for options) ──
        if 50 <= price <= 500:
            score += 0.05  # Sweet spot for options spreads

        if score < 0.01:
            return None

        return WatchlistItem(
            ticker=ticker,
            score=round(score, 3),
            iv_rank=0.0,  # Will be updated after chain fetch
            gap_pct=round(gap_pct, 2),
            has_earnings_soon=earnings_soon,
            days_to_earnings=days_to_earn,
            unusual_options_activity=False,
            suggested_strategies=strategies[:3],
            notes="; ".join(notes_parts) if notes_parts else "",
        )

    def get_watchlist(self) -> Optional[DailyWatchlist]:
        return self._today_watchlist

    def get_watchlist_tickers(self) -> List[str]:
        if self._today_watchlist:
            return [i.ticker for i in self._today_watchlist.items]
        return UNIVERSE[:10]
