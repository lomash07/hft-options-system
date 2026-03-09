"""
Feature Store.

Computes and caches real-time features in Redis for agent consumption.

FIX: initialize() now uses batch yf.download() — ONE request for all 22 tickers
     instead of 22 individual requests that trigger rate limits.
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
from data.broker_feed import DataFeed, YFinanceFeed
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
        self._prices: Dict[str, float] = {}

    async def initialize(self) -> None:
        """
        Load historical data needed for feature computation.
        
        KEY FIX: Uses batch download (1 API call) instead of
        22 individual requests that trigger Yahoo rate limits.
        """
        logger.info("feature_store.initializing", tickers=len(ALL_TICKERS))

        # ── BATCH download all history in ONE request ──
        if isinstance(self.feed, YFinanceFeed):
            history = await self.feed.batch_download_history(
                ALL_TICKERS, period="60d"
            )

            for ticker, df in history.items():
                try:
                    if df.empty or len(df) < 5:
                        continue

                    # Average volume (20-day)
                    vol_series = df["Volume"].rolling(20).mean()
                    valid_vol = vol_series.dropna()
                    if not valid_vol.empty:
                        self._avg_volumes[ticker] = float(valid_vol.iloc[-1])

                    # Previous close
                    if len(df) >= 2:
                        self._prev_closes[ticker] = float(df["Close"].iloc[-2])

                    # Current price
                    self._prices[ticker] = float(df["Close"].iloc[-1])

                    # Store IV history proxy (realized vol)
                    closes = df["Close"].tolist()
                    rv = options_analytics.compute_realized_vol(closes)
                    if rv > 0:
                        db.store_iv_close(ticker, rv)

                except Exception as e:
                    logger.warning(
                        "feature_store.init_ticker_error",
                        ticker=ticker, error=str(e),
                    )
                    continue

            logger.info(
                "feature_store.batch_loaded",
                loaded=len(history),
                prices=len(self._prices),
                volumes=len(self._avg_volumes),
            )

        else:
            # Non-yfinance feed: fetch individually (IB handles rate limits)
            for ticker in ALL_TICKERS:
                try:
                    df = await self.feed.get_historical_bars(ticker, days=60)
                    if not df.empty:
                        vol_series = df["Volume"].rolling(20).mean()
                        valid_vol = vol_series.dropna()
                        if not valid_vol.empty:
                            self._avg_volumes[ticker] = float(valid_vol.iloc[-1])
                        if len(df) >= 2:
                            self._prev_closes[ticker] = float(df["Close"].iloc[-2])
                        self._prices[ticker] = float(df["Close"].iloc[-1])
                except Exception as e:
                    logger.warning(
                        "feature_store.init_error",
                        ticker=ticker, error=str(e),
                    )
                    continue

        logger.info("feature_store.initialized")

    def get_cached_price(self, ticker: str) -> Optional[float]:
        """Get price from batch-loaded cache. No API call."""
        return self._prices.get(ticker)

    def get_prev_close(self, ticker: str) -> Optional[float]:
        """Get previous close from cache. No API call."""
        return self._prev_closes.get(ticker)

    def get_avg_volume(self, ticker: str) -> Optional[float]:
        """Get 20-day average volume from cache. No API call."""
        return self._avg_volumes.get(ticker)

    async def update_ticker(self, ticker: str) -> Optional[TickerSnapshot]:
        """
        Refresh all features for a single ticker.
        Called on every price bar update.
        """
        try:
            bar = await self.feed.get_price_bar(ticker)
            if not bar:
                # Fallback to cached price
                price = self._prices.get(ticker)
                if not price:
                    return None
                bar = PriceBar(
                    time=datetime.utcnow(), ticker=ticker,
                    open=price, high=price, low=price,
                    close=price, volume=0,
                )

            price = bar.close

            # Relative volume
            avg_vol = self._avg_volumes.get(ticker, 0)
            rel_vol = bar.volume / avg_vol if avg_vol > 0 else 1.0

            # Daily change
            prev_close = self._prev_closes.get(ticker, price)
            daily_change = (price - prev_close) / prev_close if prev_close > 0 else 0

            gap_pct = daily_change

            # IV metrics from cached chain
            iv_metrics = self.analytics.get_cached_metrics(ticker)

            snapshot = TickerSnapshot(
                ticker=ticker,
                timestamp=datetime.utcnow(),
                price=price,
                vwap=bar.vwap or price,
                volume=bar.volume,
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
                "volume": bar.volume,
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
        Called every 5 minutes.
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

            # Store chain snapshot in DB
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
                for q in chain.quotes[:200]
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
        """
        logger.info(
            "feature_store.loop_started",
            tickers=len(tickers), interval=interval,
        )

        chain_counter = 0

        while True:
            for ticker in tickers:
                await self.update_ticker(ticker)
                await asyncio.sleep(1)  # 1s between tickers

            # Update options chains less frequently
            chain_counter += 1
            if chain_counter >= 5:
                chain_counter = 0
                for ticker in tickers[:5]:  # Only top 5 to stay under rate limit
                    await self.update_options_chain(ticker)
                    await asyncio.sleep(3)  # 3s between chain fetches

            await event_bus.send_heartbeat("feature_store")
            await asyncio.sleep(interval)
