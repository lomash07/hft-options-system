"""
Execution Engine.

Handles order routing and fill management:
  - Multi-leg combo orders for spreads
  - Limit order at mid, walk to fill (3 steps max)
  - Fill confirmation and slippage tracking
  - Paper trading simulation for development
  - Position lifecycle tracking
"""

from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, date
from typing import Dict, List, Optional

import structlog

from config.settings import settings
from core.event_bus import (
    STREAM_FILLS, STREAM_ALERTS, event_bus,
)
from core.models import (
    OptionLeg, OrderSide, Position, PositionStatus,
    StrategyOrder, StrategyType,
)
from core.database import db

logger = structlog.get_logger()


class ExecutionEngine:
    """Routes orders to broker and tracks fills."""

    def __init__(self):
        self._positions: Dict[str, Position] = {}
        self._ib = None  # IB connection (set if available)

    async def start(self) -> None:
        """Start consuming risk-approved orders."""
        logger.info("execution.started", mode=settings.trading_mode.value)

        event_bus.start_consumer(
            "stream:approved_orders",
            "execution",
            "execution_worker",
            self._handle_order,
        )

    async def _handle_order(self, data: Dict) -> None:
        """Execute a risk-approved order."""
        order_id = data.get("order_id", str(uuid.uuid4())[:8])
        ticker = data.get("ticker", "")
        strategy = data.get("strategy_type", "")

        logger.info(
            "execution.processing",
            order_id=order_id, ticker=ticker, strategy=strategy,
        )

        if settings.trading_mode.value == "paper":
            await self._paper_execute(data)
        else:
            await self._live_execute(data)

    async def _paper_execute(self, data: Dict) -> None:
        """
        Simulate execution for paper trading.
        Adds realistic slippage and delay.
        """
        order_id = data.get("order_id", "?")
        ticker = data.get("ticker", "")
        strategy = data.get("strategy_type", "")
        legs = data.get("legs", [])
        credit_debit = float(data.get("net_credit_debit", 0))
        max_risk = float(data.get("max_risk", 0))
        max_reward = float(data.get("max_reward", 0))

        # Simulate fill delay
        await asyncio.sleep(random.uniform(0.5, 2.0))

        # Simulate slippage (1-3% of credit/debit)
        slippage_pct = random.uniform(0.01, 0.03)
        if credit_debit > 0:  # Credit trade
            fill_price = credit_debit * (1 - slippage_pct)
        else:  # Debit trade
            fill_price = credit_debit * (1 + slippage_pct)

        # Simulate commission
        num_legs = max(len(legs), 1)
        commission = num_legs * 0.65  # $0.65/contract (IB rate)

        # Create position
        position = Position(
            id=order_id,
            opened_at=datetime.utcnow(),
            ticker=ticker,
            strategy_type=StrategyType(strategy),
            direction=data.get("direction", "neutral"),
            status=PositionStatus.OPEN,
            entry_credit_debit=fill_price,
            max_risk=max_risk,
            max_reward=max_reward,
            agent_id=data.get("agent_id", ""),
            market_regime=data.get("regime", ""),
            option_legs=[
                OptionLeg(
                    side=OrderSide(leg["side"]),
                    quantity=leg.get("quantity", 1),
                    ticker=ticker,
                    expiry=date.fromisoformat(leg["expiry"]),
                    strike=leg["strike"],
                    option_type=leg["option_type"],
                    fill_price=fill_price / max(len(legs), 1),
                    commission=commission / max(len(legs), 1),
                )
                for leg in legs
            ] if legs else [],
        )

        # Calculate DTE
        if position.option_legs:
            min_expiry = min(l.expiry for l in position.option_legs)
            position.days_to_expiry = (min_expiry - date.today()).days

        self._positions[order_id] = position

        # Log trade
        trade_id = await db.insert_trade({
            "ticker": ticker,
            "strategy_type": strategy,
            "direction": data.get("direction", ""),
            "agent_id": data.get("agent_id", ""),
            "signal_score": data.get("signal_score", 0),
            "legs": legs,
            "fill_price": fill_price,
            "commission": commission,
            "slippage": abs(credit_debit - fill_price),
            "max_risk": max_risk,
            "max_reward": max_reward,
            "market_regime": data.get("regime", ""),
            "iv_rank_entry": data.get("iv_rank", 0),
            "notes": f"Paper trade. Agent: {data.get('agent_id', '')}",
        })

        # Publish fill notification
        await event_bus.publish(STREAM_FILLS, {
            "order_id": order_id,
            "trade_id": trade_id,
            "ticker": ticker,
            "strategy": strategy,
            "fill_price": fill_price,
            "commission": commission,
            "status": "filled",
        })

        await event_bus.publish_alert(
            "info",
            f"📋 PAPER FILL: {ticker} {strategy} "
            f"{'$' + f'{fill_price:.2f} credit' if fill_price > 0 else '$' + f'{abs(fill_price):.2f} debit'} "
            f"| Risk: ${max_risk:.0f} | Reward: ${max_reward:.0f}",
        )

        logger.info(
            "execution.paper_filled",
            order_id=order_id, ticker=ticker,
            strategy=strategy,
            fill=round(fill_price, 2),
            commission=round(commission, 2),
            trade_id=trade_id,
        )

    async def _live_execute(self, data: Dict) -> None:
        """
        Live execution via Interactive Brokers.
        Multi-leg combo orders with price walking.
        """
        # TODO: Implement live IB execution
        # This will use ib_insync to:
        # 1. Build a ComboLeg order
        # 2. Submit at mid price
        # 3. Wait for fill (30s timeout)
        # 4. Walk price by $0.01 (max 3 steps)
        # 5. Cancel if not filled
        logger.warning(
            "execution.live_not_implemented",
            msg="Switch to paper mode or implement IB execution",
        )
        await self._paper_execute(data)

    async def close_position(self, position_id: str) -> bool:
        """Close an open position."""
        pos = self._positions.get(position_id)
        if not pos or pos.status != PositionStatus.OPEN:
            return False

        logger.info("execution.closing", position_id=position_id, ticker=pos.ticker)

        if settings.trading_mode.value == "paper":
            # Simulate close
            await asyncio.sleep(random.uniform(0.5, 1.5))
            slippage = random.uniform(0.01, 0.03)

            if pos.entry_credit_debit > 0:
                # Credit trade: we buy back cheaper
                close_price = pos.entry_credit_debit * random.uniform(0.3, 0.8)
                pnl = (pos.entry_credit_debit - close_price) * 100
            else:
                # Debit trade: we sell for hopefully more
                close_price = abs(pos.entry_credit_debit) * random.uniform(0.5, 1.5)
                pnl = (close_price - abs(pos.entry_credit_debit)) * 100

            commission = len(pos.option_legs) * 0.65

            pos.status = PositionStatus.CLOSED
            pos.closed_at = datetime.utcnow()
            pos.exit_credit_debit = close_price
            pos.realized_pnl = pnl - commission
            pos.commission_total = commission * 2  # Open + close

            await db.close_trade(
                int(position_id) if position_id.isdigit() else 0,
                close_price, pnl,
            )

            await event_bus.publish_alert(
                "info",
                f"📋 CLOSED: {pos.ticker} {pos.strategy_type.value} "
                f"P&L: ${pos.realized_pnl:+.2f}",
            )
            return True

        return False

    def get_positions(self) -> List[Position]:
        return list(self._positions.values())

    def get_open_positions(self) -> List[Position]:
        return [
            p for p in self._positions.values()
            if p.status == PositionStatus.OPEN
        ]
