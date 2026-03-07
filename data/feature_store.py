"""
Feature Store.

Computes and caches real-time features in Redis for agent consumption.
Updates on every price bar or options chain refresh.

Features per ticker:
  - Price: vwap, spread, relative_volume, daily_change
  - IV: iv_rank, iv_percentile, iv_rv_ratio, skew, term_structure
  - Flow: put_call_ratio, unusual_volume_flags
  - Context: beta_spy, days_to_earnings, sector
"""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime
from typing import Dict, List, Optional

import numpy as np
import structlog

from config.settings import settings, ALL_TICKERS, UNIVERSE
from core.event_bus import event_bus
from core.models import (
    IVMetrics, OptionsChain, PriceBar, TickerSnapshot,
)
from core.database import db
from data.broker_feed import DataFeed
from data.options_analytics import OptionsAnalytics, options_analytics

logger = structlog.get_logger()

# Sector mapping for top 20
SECTOR_MAP = {
    "AAPL": "tech", "MSFT": "tech", "NVDA": "tech", "AMZN": "consumer",
    "GOOGL": "tech", "META": "tech", "TSLA": "consumer", "BRK-B": "finance",
    "LLY": "healthcare", "UNH": "healthcare", "AVGO": "tech",
    "JPM": "finance", "V": "finance", "XOM": "energy", "MA": "finance",
    "JNJ": "healthcare", "PG": "consumer", "HD": "consumer",
    "COST": "consumer", "NFLX": "tech",
    "SPY": "index", "QQQ": "index",
}


class FeatureStore:
    """
    Central feature computation and caching layer.
    Agents read from here rather than querying raw data.
    """

    def __init__(self, feed: DataFeed, analytics: OptionsAnalytics):
        self.feed = feed
        self.analytics = analytics
        self._snapshots: Dict[str, TickerSnapshot] = {}
        self._chains: Dict[str, OptionsChain] = {}
        self._avg_volumes: Dict[str, float] = {}
        self._prev_closes: Dict[str, float] = {}

    async def initialize(self) -> None:
        """Load historical data needed for feature computation."""
        logger.info("feature_store.initializing", tickers=len(ALL_TICKERS))

        for ticker in ALL_TICKERS:
            try:
                df = await self.feed.get_historical_bars(ticker, days=30)
                if not df.empty:
                    self._avg_volumes[ticker] = float(
                        df["Volume"].rolling(20).mean().iloc[-1]
                    )
                    self._prev_closes[ticker] = float(df["Close"].iloc[-2])

                    # Store IV history for rank calculation
                    # (uses daily close as proxy until we have chain data)
                    closes = df["Close"].tolist()
                    rv = options_analytics.compute_realized_vol(closes)
                    if rv > 0:
                        db.store_iv_close(ticker, rv)

            except Exception as e:
                logger.warning(
                    "feature_store.init_error",
                    ticker=ticker, error=str(e),
                )
                continue

        logger.info("feature_store.initialized")

    async def update_ticker(self, ticker: str) -> Optional[TickerSnapshot]:
        """
        Refresh all features for a single ticker.
        Called on every price bar update.
        """
        try:
            bar = await self.feed.get_price_bar(ticker)
            if not bar:
                return None

            price = bar.close
            volume = bar.volume

            # Relative volume
            avg_vol = self._avg_volumes.get(ticker, 0)
            rel_vol = volume / avg_vol if avg_vol > 0 else 1.0

            # Daily change
            prev_close = self._prev_closes.get(ticker, price)
            daily_change = (price - prev_close) / prev_close if prev_close > 0 else 0

            # Gap (pre-market only, use first bar)
            gap_pct = daily_change  # Simplified; refine with actual gap logic

            # IV metrics from cached chain
            iv_metrics = self.analytics.get_cached_metrics(ticker)

            # Days to earnings
            days_to_earn = None
            # Will be filled by scanner module

            snapshot = TickerSnapshot(
                ticker=ticker,
                timestamp=datetime.utcnow(),
                price=price,
                vwap=bar.vwap or price,
                volume=volume,
                relative_volume=round(rel_vol, 2),
                daily_change_pct=round(daily_change * 100, 2),
                gap_pct=round(gap_pct * 100, 2),
                iv_metrics=iv_metrics,
                sector=SECTOR_MAP.get(ticker, "unknown"),
            )

            self._snapshots[ticker] = snapshot

            # Cache in Redis for agents
            await event_bus.set_hash(f"ticker:{ticker}", {
                "price": price,
                "vwap": bar.vwap or price,
                "volume": volume,
                "rel_volume": round(rel_vol, 2),
                "daily_change_pct": round(daily_change * 100, 2),
                "gap_pct": round(gap_pct * 100, 2),
                "iv_rank": iv_metrics.iv_rank if iv_metrics else 0,
                "iv_percentile": (
                    iv_metrics.iv_percentile if iv_metrics else 0
                ),
                "iv_atm_30d": iv_metrics.iv_atm_30d if iv_metrics else 0,
                "iv_rv_ratio": iv_metrics.iv_rv_ratio if iv_metrics else 0,
                "skew_25d": iv_metrics.skew_25d if iv_metrics else 0,
                "sector": SECTOR_MAP.get(ticker, "unknown"),
            }, ttl=120)

            return snapshot

        except Exception as e:
            logger.error(
                "feature_store.update_error",
                ticker=ticker, error=str(e),
            )
            return None

    async def update_options_chain(
        self, ticker: str
    ) -> Optional[OptionsChain]:
        """
        Refresh options chain and IV metrics for a ticker.
        Called every 1-5 minutes.
        """
        try:
            chain = await self.feed.get_options_chain(ticker)
            if not chain:
                return None

            # Enrich with Greeks
            chain = self.analytics.enrich_greeks(chain)

            # Compute IV metrics
            iv_history = db.get_iv_rank_data(ticker)
            iv_metrics = self.analytics.compute_iv_metrics(
                chain, iv_history
            )

            # Store chain snapshot in DB (every 5 min)
            quotes_data = [
                {
                    "time": chain.timestamp,
                    "ticker": ticker,
                    "expiry": q.expiry,
                    "strike": q.strike,
                    "option_type": q.option_type.value,
                    "bid": q.bid, "ask": q.ask, "last": q.last,
                    "volume": q.volume,
                    "open_interest": q.open_interest,
                    "implied_vol": q.implied_vol,
                    "delta": q.delta, "gamma": q.gamma,
                    "theta": q.theta, "vega": q.vega,
                }
                for q in chain.quotes[:200]  # Limit to relevant strikes
            ]
            await db.insert_options_snapshot(quotes_data)

            # Store IV surface point
            await db.insert_iv_surface({
                "ticker": ticker,
                "iv_atm_30d": iv_metrics.iv_atm_30d,
                "iv_atm_7d": iv_metrics.iv_atm_7d,
                "iv_rank": iv_metrics.iv_rank,
                "iv_percentile": iv_metrics.iv_percentile,
                "skew_25d": iv_metrics.skew_25d,
                "term_slope": iv_metrics.term_slope,
                "rv_20d": iv_metrics.rv_20d,
                "iv_rv_ratio": iv_metrics.iv_rv_ratio,
            })

            self._chains[ticker] = chain

            logger.debug(
                "feature_store.chain_updated",
                ticker=ticker,
                iv_rank=iv_metrics.iv_rank,
                iv_atm_30d=iv_metrics.iv_atm_30d,
                quotes=len(chain.quotes),
            )

            return chain

        except Exception as e:
            logger.error(
                "feature_store.chain_error",
                ticker=ticker, error=str(e),
            )
            return None

    def get_snapshot(self, ticker: str) -> Optional[TickerSnapshot]:
        return self._snapshots.get(ticker)

    def get_chain(self, ticker: str) -> Optional[OptionsChain]:
        return self._chains.get(ticker)

    def get_all_snapshots(self) -> Dict[str, TickerSnapshot]:
        return dict(self._snapshots)

    async def run_update_loop(
        self, tickers: List[str], interval: int = 60
    ) -> None:
        """
        Continuous update loop for all tickers.
        Price: every `interval` seconds
        Options chains: every 5 * `interval` seconds
        """
        logger.info(
            "feature_store.loop_started",
            tickers=len(tickers), interval=interval,
        )

        chain_counter = 0

        while True:
            for ticker in tickers:
                await self.update_ticker(ticker)
                await asyncio.sleep(0.5)  # Rate limit

            # Update options chains less frequently
            chain_counter += 1
            if chain_counter >= 5:
                chain_counter = 0
                for ticker in tickers[:10]:  # Top 10 by priority
                    await self.update_options_chain(ticker)
                    await asyncio.sleep(1)

            await event_bus.send_heartbeat("feature_store")
            await asyncio.sleep(interval)
