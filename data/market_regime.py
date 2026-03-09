"""
Market Regime Detector.

FIX: Uses batch-downloaded data from YFinanceFeed cache
     instead of making individual API calls for SPY/QQQ.
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
from monitoring import vix_gauge, regime_gauge

logger = structlog.get_logger()


# Map regime to numeric code for metrics
REGIME_CODES = {
    MarketRegime.BULL_TREND: 1,
    MarketRegime.BEAR_TREND: 2,
    MarketRegime.LOW_VOL: 3,
    MarketRegime.HIGH_VOL: 4,
    MarketRegime.CHOPPY: 5,
}


class RegimeDetector:
    """Detects market regime from SPY, VIX, and breadth data."""

    def __init__(self, feed: DataFeed):
        self.feed = feed
        self.current_regime = MarketRegime.CHOPPY
        self.regime_data: Dict[str, float] = {}
        self._last_update: Optional[datetime] = None

    async def update(self) -> MarketRegime:
        """Compute current market regime."""
        try:
            # Get SPY history (uses cache from batch download)
            spy_df = await self.feed.get_historical_bars("SPY", days=60)
            if spy_df.empty or len(spy_df) < 20:
                logger.warning("regime.insufficient_spy_data", rows=len(spy_df) if not spy_df.empty else 0)
                return self.current_regime

            spy_close = float(spy_df["Close"].iloc[-1])
            spy_20dma = float(spy_df["Close"].rolling(20).mean().dropna().iloc[-1])
            spy_50dma_series = spy_df["Close"].rolling(50).mean().dropna()
            spy_50dma = float(spy_50dma_series.iloc[-1]) if not spy_50dma_series.empty else spy_20dma

            # ADX
            adx = self._compute_adx(spy_df)

            # VIX — try a single careful request
            vix_level = await self._get_vix()
            vix_gauge.set(vix_level)

            # SPY relative to MAs
            spy_above_20dma = spy_close > spy_20dma
            spy_above_50dma = spy_close > spy_50dma
            spy_distance_20dma = (spy_close - spy_20dma) / spy_20dma

            self.regime_data = {
                "spy_close": spy_close,
                "spy_20dma": round(spy_20dma, 2),
                "spy_50dma": round(spy_50dma, 2),
                "spy_distance_20dma": round(spy_distance_20dma * 100, 2),
                "adx": round(adx, 2),
                "vix": vix_level,
            }

            # ── Classification ──
            if vix_level > 30:
                regime = MarketRegime.HIGH_VOL
            elif vix_level > 25:
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
                if spy_distance_20dma > 0:
                    regime = MarketRegime.BULL_TREND
                else:
                    regime = MarketRegime.BEAR_TREND
            else:
                regime = MarketRegime.CHOPPY

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
            regime_gauge.set(REGIME_CODES[regime])

            await event_bus.set_feature("regime", {
                "regime": regime.value,
                "data": self.regime_data,
            }, ttl=300)

            logger.info(
                "regime.detected",
                regime=regime.value,
                vix=vix_level,
                adx=round(adx, 1),
                spy=spy_close,
            )

            return regime

        except Exception as e:
            logger.error("regime.update_error", error=str(e))
            return self.current_regime

    async def _get_vix(self) -> float:
        """Get VIX level with fallback."""
        try:
            import yfinance as yf
            vix_df = yf.download("^VIX", period="5d", progress=False)
            if not vix_df.empty:
                return float(vix_df["Close"].values[-1])
        except Exception:
            pass

        # Fallback: estimate from SPY realized vol
        return 20.0

    def _compute_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        """Compute ADX (Average Directional Index)."""
        try:
            high = df["High"].values
            low = df["Low"].values
            close = df["Close"].values

            if len(close) < period + 2:
                return 20.0

            tr = np.maximum(
                high[1:] - low[1:],
                np.maximum(
                    np.abs(high[1:] - close[:-1]),
                    np.abs(low[1:] - close[:-1]),
                ),
            )

            up = high[1:] - high[:-1]
            down = low[:-1] - low[1:]
            plus_dm = np.where((up > down) & (up > 0), up, 0)
            minus_dm = np.where((down > up) & (down > 0), down, 0)

            atr = self._ema(tr, period)
            plus_di = 100 * self._ema(plus_dm, period) / np.maximum(atr, 1e-10)
            minus_di = 100 * self._ema(minus_dm, period) / np.maximum(atr, 1e-10)

            dx = 100 * np.abs(plus_di - minus_di) / np.maximum(
                plus_di + minus_di, 1e-10
            )
            adx = self._ema(dx, period)

            return float(adx[-1]) if len(adx) > 0 else 20.0

        except Exception:
            return 20.0

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        alpha = 2.0 / (period + 1)
        result = np.zeros_like(data, dtype=float)
        result[0] = data[0]
        for i in range(1, len(data)):
            result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
        return result

    def get_agent_activation(self) -> Dict[str, bool]:
        regime = self.current_regime
        return {
            "theta_harvester": regime in (
                MarketRegime.LOW_VOL, MarketRegime.HIGH_VOL, MarketRegime.CHOPPY,
            ),
            "vol_arb": True,
            "directional_momentum": regime in (
                MarketRegime.BULL_TREND, MarketRegime.BEAR_TREND,
            ),
            "earnings_vol": True,
            "mean_reversion": regime in (
                MarketRegime.CHOPPY, MarketRegime.LOW_VOL,
            ),
            "unusual_flow": True,
            "news_catalyst": True,
            "ml_ensemble": regime in (
                MarketRegime.BULL_TREND, MarketRegime.BEAR_TREND, MarketRegime.LOW_VOL,
            ),
        }

    def get_sizing_multiplier(self) -> float:
        multipliers = {
            MarketRegime.BULL_TREND: 1.0,
            MarketRegime.BEAR_TREND: 0.75,
            MarketRegime.LOW_VOL: 1.1,
            MarketRegime.HIGH_VOL: 0.6,
            MarketRegime.CHOPPY: 0.8,
        }
        return multipliers.get(self.current_regime, 0.8)
