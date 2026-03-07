"""
Consensus Engine.

Aggregates signals from multiple agents into trade decisions:
  - Weighted voting with regime-adjusted weights
  - Minimum 2 agents must agree for a trade
  - Combined score must exceed 0.70
  - Signal staleness filter (discard > 60s old)
  - Correlation guard (no double-counting correlated signals)
  - Fast-path: Unusual Flow + News combo skips full consensus
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import structlog

from config.settings import settings
from core.event_bus import (
    STREAM_SIGNALS, STREAM_ORDERS, GROUP_CONSENSUS, event_bus,
)
from core.models import Direction, Signal, StrategyOrder, StrategyType
from core.database import db
from data.feature_store import FeatureStore
from strategy.options_constructor import OptionsConstructor

logger = structlog.get_logger()

# Agents that produce correlated signals (don't double-count)
CORRELATED_PAIRS = {
    frozenset({"theta_harvester", "vol_arb"}),
    frozenset({"directional_momentum", "ml_ensemble"}),
}

# Fast-path pairs (skip full consensus)
FAST_PATH_PAIRS = {
    frozenset({"unusual_flow", "news_catalyst"}),
    frozenset({"unusual_flow", "directional_momentum"}),
}


class ConsensusEngine:
    """
    Aggregates agent signals and produces trade decisions.
    Runs as an async consumer on the signals stream.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        constructor: OptionsConstructor,
    ):
        self.feature_store = feature_store
        self.constructor = constructor
        self._signal_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self._buffer_window = 30  # seconds to accumulate signals
        self._last_flush: float = time.time()
        self._decisions_today: int = 0
        self._max_decisions_per_day: int = 10

    async def start(self) -> None:
        """Start consuming signals and producing orders."""
        logger.info("consensus.started")

        # Start signal consumer
        event_bus.start_consumer(
            STREAM_SIGNALS,
            GROUP_CONSENSUS,
            "consensus_worker",
            self._handle_signal,
        )

        # Periodic flush loop
        while True:
            await asyncio.sleep(self._buffer_window)
            await self._flush_buffer()
            await event_bus.send_heartbeat("consensus")

    async def _handle_signal(self, data: Dict) -> None:
        """Handle incoming signal from any agent."""
        ticker = data.get("ticker", "")
        if not ticker:
            return

        # Check staleness
        ts = data.get("_ts", "0")
        age = time.time() - float(ts) if ts != "0" else 0
        if age > settings.signal_staleness:
            logger.debug(
                "consensus.stale_signal",
                agent=data.get("agent_id"), ticker=ticker, age=age,
            )
            return

        # Check for fast-path (urgent combo)
        agent_id = data.get("agent_id", "")
        if await self._check_fast_path(ticker, agent_id, data):
            return

        # Add to buffer
        self._signal_buffer[ticker].append({
            **data,
            "received_at": time.time(),
        })

    async def _check_fast_path(
        self, ticker: str, agent_id: str, data: Dict
    ) -> bool:
        """
        Check if this signal combined with a buffered signal
        qualifies for fast-path execution.
        """
        buffered_agents = {
            s.get("agent_id") for s in self._signal_buffer.get(ticker, [])
        }

        for pair in FAST_PATH_PAIRS:
            if agent_id in pair and pair - {agent_id} <= buffered_agents:
                # Fast-path match!
                logger.info(
                    "consensus.fast_path",
                    ticker=ticker,
                    agents=list(pair),
                )
                # Use the highest-confidence signal
                all_signals = self._signal_buffer[ticker] + [data]
                best = max(
                    all_signals,
                    key=lambda s: float(s.get("confidence", 0)),
                )
                await self._produce_order(ticker, [best, data])
                # Clear buffer for this ticker
                self._signal_buffer[ticker] = []
                return True

        return False

    async def _flush_buffer(self) -> None:
        """Process accumulated signals for each ticker."""
        now = time.time()

        for ticker in list(self._signal_buffer.keys()):
            signals = self._signal_buffer[ticker]

            # Remove stale signals
            signals = [
                s for s in signals
                if now - s.get("received_at", 0) < self._buffer_window * 2
            ]

            if len(signals) < 2:
                # Need at least 2 agents
                self._signal_buffer[ticker] = signals
                continue

            # Check agreement
            agreed = self._check_agreement(signals)
            if agreed:
                direction, score, contributing = agreed
                logger.info(
                    "consensus.agreement",
                    ticker=ticker,
                    direction=direction,
                    score=round(score, 3),
                    agents=[s.get("agent_id") for s in contributing],
                )
                await self._produce_order(ticker, contributing)

            # Clear processed signals
            self._signal_buffer[ticker] = []

    def _check_agreement(
        self, signals: List[Dict]
    ) -> Optional[Tuple[str, float, List[Dict]]]:
        """
        Check if signals agree and meet minimum thresholds.
        Returns (direction, combined_score, contributing_signals) or None.
        """
        # Group by direction
        by_direction: Dict[str, List[Dict]] = defaultdict(list)
        for s in signals:
            d = s.get("direction", "")
            if d:
                by_direction[d].append(s)

        best_direction = None
        best_score = 0.0
        best_signals = []

        for direction, dir_signals in by_direction.items():
            if len(dir_signals) < 2:
                continue

            # Remove correlated signals (keep highest confidence)
            filtered = self._decorrelate(dir_signals)

            if len(filtered) < 2:
                continue

            # Weighted average confidence
            total_weight = 0.0
            weighted_sum = 0.0
            for s in filtered:
                conf = float(s.get("confidence", 0))
                weight = float(s.get("weight", 1.0))
                weighted_sum += conf * weight
                total_weight += weight

            score = weighted_sum / total_weight if total_weight > 0 else 0
            agent_count_bonus = min((len(filtered) - 2) * 0.05, 0.15)
            score = min(score + agent_count_bonus, 1.0)

            if score > best_score and score >= 0.70:
                best_direction = direction
                best_score = score
                best_signals = filtered

        if best_direction:
            return best_direction, best_score, best_signals
        return None

    def _decorrelate(self, signals: List[Dict]) -> List[Dict]:
        """
        Remove correlated signals — keep only the highest
        confidence from each correlated pair.
        """
        agents = {s.get("agent_id"): s for s in signals}
        to_remove = set()

        for pair in CORRELATED_PAIRS:
            pair_agents = pair & set(agents.keys())
            if len(pair_agents) >= 2:
                # Keep highest confidence, remove others
                pair_list = [(a, float(agents[a].get("confidence", 0)))
                             for a in pair_agents]
                pair_list.sort(key=lambda x: x[1], reverse=True)
                for a, _ in pair_list[1:]:
                    to_remove.add(a)

        return [s for s in signals if s.get("agent_id") not in to_remove]

    async def _produce_order(
        self, ticker: str, signals: List[Dict]
    ) -> None:
        """Convert agreed signals into an executable order."""
        if self._decisions_today >= self._max_decisions_per_day:
            logger.warning("consensus.daily_limit_reached")
            return

        # Pick best signal for strategy details
        best = max(signals, key=lambda s: float(s.get("confidence", 0)))
        direction = best.get("direction", "neutral")
        strategy_str = best.get("suggested_strategy", "")
        dte = int(best.get("suggested_dte", 35))
        delta = float(best.get("target_delta", 0.30))
        score = float(best.get("confidence", 0))

        # Need options chain
        chain = self.feature_store.get_chain(ticker)
        if not chain:
            logger.warning("consensus.no_chain", ticker=ticker)
            return

        # Map strings to enums
        try:
            dir_enum = Direction(direction)
            strat_enum = StrategyType(strategy_str) if strategy_str else None
        except ValueError:
            logger.warning(
                "consensus.invalid_enum",
                direction=direction, strategy=strategy_str,
            )
            return

        if not strat_enum:
            # Default strategy selection
            if dir_enum == Direction.NEUTRAL:
                strat_enum = StrategyType.IRON_CONDOR
            elif dir_enum == Direction.BULLISH:
                strat_enum = StrategyType.CALL_DEBIT_SPREAD
            else:
                strat_enum = StrategyType.PUT_DEBIT_SPREAD

        # Get regime
        regime_info = await event_bus.get_feature_json("regime")
        regime = regime_info.get("regime", "") if regime_info else ""

        # IV rank at entry
        iv_metrics = self.feature_store.feature_store if hasattr(self.feature_store, 'feature_store') else None
        snapshot = self.feature_store.get_snapshot(ticker)
        iv_rank = snapshot.iv_metrics.iv_rank if snapshot and snapshot.iv_metrics else 0

        # Build order
        order = self.constructor.construct(
            chain=chain,
            strategy_type=strat_enum,
            direction=dir_enum,
            target_dte=dte,
            target_delta=delta,
            agent_id=best.get("agent_id", "consensus"),
            signal_score=score,
            regime=regime,
        )

        if not order:
            logger.info(
                "consensus.construction_failed",
                ticker=ticker, strategy=strat_enum.value,
            )
            return

        order.iv_rank_at_entry = iv_rank

        # Publish to execution stream
        order_data = {
            "order_id": order.id,
            "ticker": order.ticker,
            "strategy_type": order.strategy_type.value,
            "direction": order.direction.value,
            "legs": [
                {
                    "side": leg.side.value,
                    "quantity": leg.quantity,
                    "strike": leg.strike,
                    "expiry": str(leg.expiry),
                    "option_type": leg.option_type.value,
                }
                for leg in order.option_legs
            ],
            "net_credit_debit": order.net_credit_debit,
            "max_risk": order.max_risk,
            "max_reward": order.max_reward,
            "agent_id": order.agent_id,
            "signal_score": order.signal_score,
            "regime": order.market_regime,
            "iv_rank": iv_rank,
        }

        await event_bus.publish(STREAM_ORDERS, order_data)
        self._decisions_today += 1

        logger.info(
            "consensus.order_produced",
            ticker=ticker,
            strategy=order.strategy_type.value,
            direction=order.direction.value,
            risk=order.max_risk,
            reward=order.max_reward,
            score=round(score, 3),
            agents=[s.get("agent_id") for s in signals],
        )
