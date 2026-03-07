"""
Market Regime Detector.

Classifies the current market into one of 5 regimes:
  🟢 BULL_TREND  — SPY > 20DMA, uptrend
  🔴 BEAR_TREND  — SPY < 20DMA, downtrend
  🔵 LOW_VOL     — VIX < 16, compression
  🟡 HIGH_VOL    — VIX > 25, elevated fear
  ⚫ CHOPPY      — ADX < 20, range-bound

Regime determines:
  - Which agents are active
  - Position sizing multipliers
  - Strategy type preferences
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict, Optional

import numpy as np
import pandas as pd
import structlog

from config.settings import MarketRegime, settings
from core.event_bus import STREAM_REGIME, event_bus
from data.broker_feed import DataFeed

logger = structlog.get_logger()


class RegimeDetector:
    """Detects market regime from SPY, VIX, and breadth data."""

    def __init__(self, feed: DataFeed):
        self.feed = feed
        self.current_regime = MarketRegime.CHOPPY
        self.regime_data: Dict[str, float] = {}
        self._last_update: Optional[datetime] = None

    async def update(self) -> MarketRegime:
        """
        Compute current market regime.
        Should be called every 1-5 minutes during market hours.
        """
        try:
            # Get SPY data
            spy_df = await self.feed.get_historical_bars("SPY", days=60)
            if spy_df.empty:
                return self.current_regime

            spy_close = float(spy_df["Close"].iloc[-1])
            spy_20dma = float(spy_df["Close"].rolling(20).mean().iloc[-1])
            spy_50dma = float(spy_df["Close"].rolling(50).mean().iloc[-1])

            # ADX (Average Directional Index) - trend strength
            adx = self._compute_adx(spy_df)

            # Get VIX level
            vix_level = await self._get_vix()

            # VIX term structure (if we have QQQ as proxy for risk-on)
            qqq_df = await self.feed.get_historical_bars("QQQ", days=30)
            qqq_relative = 0.0
            if not qqq_df.empty:
                qqq_20dma = float(
                    qqq_df["Close"].rolling(20).mean().iloc[-1]
                )
                qqq_close = float(qqq_df["Close"].iloc[-1])
                qqq_relative = (qqq_close - qqq_20dma) / qqq_20dma

            # SPY relative to moving averages
            spy_above_20dma = spy_close > spy_20dma
            spy_above_50dma = spy_close > spy_50dma
            spy_distance_20dma = (spy_close - spy_20dma) / spy_20dma

            # Store raw data for dashboard
            self.regime_data = {
                "spy_close": spy_close,
                "spy_20dma": spy_20dma,
                "spy_50dma": spy_50dma,
                "spy_distance_20dma": round(spy_distance_20dma * 100, 2),
                "adx": round(adx, 2),
                "vix": vix_level,
                "qqq_relative": round(qqq_relative * 100, 2),
            }

            # ── Regime Classification Logic ──
            # Priority order matters: volatility regimes override trend

            if vix_level > 30:
                regime = MarketRegime.HIGH_VOL
            elif vix_level > 25:
                # High vol but check if it's trending
                if adx > 25 and not spy_above_20dma:
                    regime = MarketRegime.BEAR_TREND
                else:
                    regime = MarketRegime.HIGH_VOL
            elif vix_level < 14:
                regime = MarketRegime.LOW_VOL
            elif adx < 18:
                regime = MarketRegime.CHOPPY
            elif spy_above_20dma and spy_above_50dma:
                regime = MarketRegime.BULL_TREND
            elif not spy_above_20dma and not spy_above_50dma:
                regime = MarketRegime.BEAR_TREND
            elif adx > 25:
                # Strong trend in some direction
                if spy_distance_20dma > 0:
                    regime = MarketRegime.BULL_TREND
                else:
                    regime = MarketRegime.BEAR_TREND
            else:
                regime = MarketRegime.CHOPPY

            # Log regime change
            if regime != self.current_regime:
                logger.info(
                    "regime.changed",
                    old=self.current_regime.value,
                    new=regime.value,
                    data=self.regime_data,
                )
                await event_bus.publish(STREAM_REGIME, {
                    "regime": regime.value,
                    "previous": self.current_regime.value,
                    "data": self.regime_data,
                })

            self.current_regime = regime
            self._last_update = datetime.utcnow()

            # Cache in Redis
            await event_bus.set_feature("regime", {
                "regime": regime.value,
                "data": self.regime_data,
            }, ttl=300)

            return regime

        except Exception as e:
            logger.error("regime.update_error", error=str(e))
            return self.current_regime

    async def _get_vix(self) -> float:
        """Get current VIX level."""
        try:
            price = await self.feed.get_price("^VIX")
            if price:
                return price

            # Fallback: try VIX via yfinance directly
            import yfinance as yf
            vix = yf.Ticker("^VIX")
            info = vix.fast_info
            return float(
                info.get("lastPrice", info.get("previousClose", 20))
            )
        except Exception:
            return 20.0  # Default to neutral

    def _compute_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        """Compute ADX (Average Directional Index)."""
        try:
            high = df["High"].values
            low = df["Low"].values
            close = df["Close"].values

            if len(close) < period + 2:
                return 20.0

            # True Range
            tr = np.maximum(
                high[1:] - low[1:],
                np.maximum(
                    np.abs(high[1:] - close[:-1]),
                    np.abs(low[1:] - close[:-1]),
                ),
            )

            # +DM, -DM
            up = high[1:] - high[:-1]
            down = low[:-1] - low[1:]
            plus_dm = np.where((up > down) & (up > 0), up, 0)
            minus_dm = np.where((down > up) & (down > 0), down, 0)

            # Smoothed averages
            atr = self._ema(tr, period)
            plus_di = 100 * self._ema(plus_dm, period) / np.maximum(atr, 1e-10)
            minus_di = 100 * self._ema(minus_dm, period) / np.maximum(atr, 1e-10)

            # DX and ADX
            dx = 100 * np.abs(plus_di - minus_di) / np.maximum(
                plus_di + minus_di, 1e-10
            )
            adx = self._ema(dx, period)

            return float(adx[-1]) if len(adx) > 0 else 20.0

        except Exception:
            return 20.0

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        """Exponential moving average."""
        alpha = 2.0 / (period + 1)
        result = np.zeros_like(data, dtype=float)
        result[0] = data[0]
        for i in range(1, len(data)):
            result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
        return result

    def get_agent_activation(self) -> Dict[str, bool]:
        """
        Determine which agents should be active
        based on current regime.
        """
        regime = self.current_regime

        activation = {
            "theta_harvester": regime in (
                MarketRegime.LOW_VOL, MarketRegime.HIGH_VOL,
                MarketRegime.CHOPPY,
            ),
            "vol_arb": True,  # Active in all regimes
            "directional_momentum": regime in (
                MarketRegime.BULL_TREND, MarketRegime.BEAR_TREND,
            ),
            "earnings_vol": True,  # Active whenever earnings exist
            "mean_reversion": regime in (
                MarketRegime.CHOPPY, MarketRegime.LOW_VOL,
            ),
            "unusual_flow": True,  # Active in all regimes
            "news_catalyst": True,  # Event-driven
            "ml_ensemble": regime in (
                MarketRegime.BULL_TREND, MarketRegime.BEAR_TREND,
                MarketRegime.LOW_VOL,
            ),
        }

        return activation

    def get_sizing_multiplier(self) -> float:
        """
        Regime-based position sizing multiplier.
        Reduces size in high-vol, increases in low-vol.
        """
        multipliers = {
            MarketRegime.BULL_TREND: 1.0,
            MarketRegime.BEAR_TREND: 0.75,
            MarketRegime.LOW_VOL: 1.1,
            MarketRegime.HIGH_VOL: 0.6,
            MarketRegime.CHOPPY: 0.8,
        }
        return multipliers.get(self.current_regime, 0.8)
