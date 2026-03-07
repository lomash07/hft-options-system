"""
Base Agent class.

All strategy agents inherit from this. Provides:
  - Lifecycle management (start, stop, heartbeat)
  - Feature store access
  - Signal emission
  - Regime-aware activation
  - Rate limiting and cooldowns
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

import structlog

from config.settings import MarketRegime, settings
from core.event_bus import STREAM_SIGNALS, event_bus
from core.models import Direction, Signal, StrategyType, TickerSnapshot
from core.database import db
from data.feature_store import FeatureStore

logger = structlog.get_logger()


class BaseAgent(ABC):
    """
    Abstract base class for all trading agents.

    Subclasses implement:
      - `analyze(ticker, snapshot)` → Optional[Signal]
      - `active_regimes` property → which regimes this agent runs in
    """

    def __init__(
        self,
        agent_id: str,
        feature_store: FeatureStore,
    ):
        self.agent_id = agent_id
        self.feature_store = feature_store
        self._running = False
        self._cooldowns: Dict[str, float] = {}  # ticker → last signal time
        self._min_cooldown = 300  # 5 minutes between signals per ticker
        self._signals_today: int = 0
        self._max_signals_per_day: int = 20

    @property
    @abstractmethod
    def active_regimes(self) -> List[MarketRegime]:
        """Regimes in which this agent is active."""
        ...

    @abstractmethod
    async def analyze(
        self, ticker: str, snapshot: TickerSnapshot
    ) -> Optional[Signal]:
        """
        Analyze a ticker and optionally produce a trading signal.

        Args:
            ticker: The stock ticker
            snapshot: Current real-time snapshot with all features

        Returns:
            Signal if a trade opportunity is detected, None otherwise
        """
        ...

    async def start(self, watchlist: List[str], regime: MarketRegime) -> None:
        """Start the agent's analysis loop."""
        self._running = True
        self._signals_today = 0

        if regime not in self.active_regimes:
            logger.info(
                "agent.skipped_regime",
                agent=self.agent_id,
                regime=regime.value,
                active_in=[r.value for r in self.active_regimes],
            )
            return

        logger.info(
            "agent.started",
            agent=self.agent_id,
            watchlist=watchlist,
            regime=regime.value,
        )

        while self._running:
            try:
                for ticker in watchlist:
                    snapshot = self.feature_store.get_snapshot(ticker)
                    if not snapshot:
                        continue

                    # Check cooldown
                    if self._in_cooldown(ticker):
                        continue

                    # Run analysis
                    signal = await self.analyze(ticker, snapshot)

                    if signal and signal.confidence >= self._min_confidence:
                        await self._emit_signal(signal)
                        self._cooldowns[ticker] = time.time()
                        self._signals_today += 1

                        if self._signals_today >= self._max_signals_per_day:
                            logger.warning(
                                "agent.daily_limit",
                                agent=self.agent_id,
                                count=self._signals_today,
                            )
                            break

                await event_bus.send_heartbeat(self.agent_id)
                await asyncio.sleep(self._scan_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "agent.error",
                    agent=self.agent_id, error=str(e),
                )
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        logger.info("agent.stopped", agent=self.agent_id)

    @property
    def _min_confidence(self) -> float:
        """Minimum confidence to emit a signal."""
        return 0.60

    @property
    def _scan_interval(self) -> float:
        """Seconds between full scan cycles."""
        return 30.0

    def _in_cooldown(self, ticker: str) -> bool:
        last = self._cooldowns.get(ticker, 0)
        return (time.time() - last) < self._min_cooldown

    async def _emit_signal(self, signal: Signal) -> None:
        """Publish signal to event bus and log to DB."""
        signal.agent_id = self.agent_id

        # Get current weight
        weight = db.get_agent_weight(self.agent_id)
        weighted_confidence = min(signal.confidence * weight, 1.0)

        signal_data = {
            "agent_id": self.agent_id,
            "ticker": signal.ticker,
            "direction": signal.direction.value,
            "confidence": weighted_confidence,
            "raw_confidence": signal.confidence,
            "suggested_strategy": (
                signal.suggested_strategy.value
                if signal.suggested_strategy else ""
            ),
            "suggested_dte": signal.suggested_dte or 0,
            "target_delta": signal.target_delta or 0.30,
            "urgency": signal.urgency,
            "rationale": signal.rationale,
            "metadata": signal.metadata,
        }

        await event_bus.publish_signal(signal_data)
        await db.insert_signal(signal_data)

        logger.info(
            "agent.signal",
            agent=self.agent_id,
            ticker=signal.ticker,
            direction=signal.direction.value,
            confidence=round(weighted_confidence, 3),
            strategy=signal.suggested_strategy.value if signal.suggested_strategy else "none",
            rationale=signal.rationale[:80],
        )

    def _has_earnings_soon(
        self, snapshot: TickerSnapshot, days: int = 5
    ) -> bool:
        """Check if earnings are within N days."""
        if snapshot.days_to_earnings is not None:
            return snapshot.days_to_earnings <= days
        return False
