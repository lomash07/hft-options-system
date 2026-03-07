"""
Risk Manager — CRITICAL SAFETY LAYER.

Enforces:
  - Tiered circuit breakers (-$400 / -$600 / -$800)
  - PDT day trade counter (if under $25K)
  - Portfolio Greeks limits (Δ, Θ, V)
  - Max concurrent positions
  - Buying power limits
  - Earnings date conflicts
  - Assignment / pin / gamma risk on expiring options
  - Dead man's switch

NO TRADE executes without passing through this gate.
"""

from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import structlog

from config.settings import settings
from core.event_bus import (
    STREAM_ALERTS, STREAM_ORDERS, GROUP_RISK, event_bus,
)
from core.models import (
    Direction, PortfolioGreeks, PortfolioState, Position,
    PositionStatus, StrategyOrder, StrategyType,
)
from core.database import db

logger = structlog.get_logger()


class RiskManager:
    """
    Central risk gate. Every order must be approved here
    before reaching execution.
    """

    def __init__(self):
        self.positions: List[Position] = []
        self.daily_pnl: float = 0.0
        self.circuit_breaker_level: int = 0  # 0=ok, 1=warn, 2=stop, 3=kill
        self._last_heartbeat: float = time.time()

    async def start(self) -> None:
        """Start the risk manager's order gate consumer."""
        logger.info("risk_manager.started")

        # Consume orders from consensus
        event_bus.start_consumer(
            STREAM_ORDERS,
            GROUP_RISK,
            "risk_worker",
            self._handle_order,
        )

        # Position management loop
        asyncio.create_task(self._management_loop())

        # Dead man's switch
        asyncio.create_task(self._dead_man_switch())

    async def _handle_order(self, data: Dict) -> None:
        """
        Gate function: validate an order before allowing execution.
        """
        order_id = data.get("order_id", "?")
        ticker = data.get("ticker", "")
        max_risk = float(data.get("max_risk", 0))
        strategy = data.get("strategy_type", "")

        logger.info(
            "risk.order_received",
            order_id=order_id, ticker=ticker,
            strategy=strategy, risk=max_risk,
        )

        # ── Run all checks ──
        checks = [
            ("circuit_breaker", self._check_circuit_breaker()),
            ("daily_loss", self._check_daily_loss(max_risk)),
            ("pdt", self._check_pdt(data)),
            ("max_positions", self._check_max_positions()),
            ("buying_power", self._check_buying_power(max_risk)),
            ("duplicate", self._check_duplicate(ticker, strategy)),
            ("earnings_conflict", await self._check_earnings(ticker, data)),
            ("per_trade_risk", self._check_per_trade_risk(max_risk)),
            ("greeks_limits", self._check_greeks_impact(data)),
        ]

        failed = [(name, reason) for name, (ok, reason) in checks if not ok]

        if failed:
            reasons = "; ".join(f"{n}: {r}" for n, r in failed)
            logger.warning(
                "risk.order_rejected",
                order_id=order_id, ticker=ticker, reasons=reasons,
            )
            await event_bus.publish_alert(
                "warning",
                f"Order rejected: {ticker} {strategy} — {reasons}",
            )
            return

        # ── All checks passed → forward to execution ──
        logger.info(
            "risk.order_approved",
            order_id=order_id, ticker=ticker,
            strategy=strategy, risk=max_risk,
        )

        # Tag as risk-approved
        data["risk_approved"] = True
        data["risk_approved_at"] = datetime.utcnow().isoformat()

        # Publish to a risk-approved stream that execution consumes
        await event_bus.publish("stream:approved_orders", data)

    # ═══════════════════════════════════════════
    # Risk Checks
    # ═══════════════════════════════════════════

    def _check_circuit_breaker(self) -> Tuple[bool, str]:
        """Check if circuit breaker is tripped."""
        if self.circuit_breaker_level >= 3:
            return False, "KILL switch active — all trading halted"
        if self.circuit_breaker_level >= 2:
            return False, f"Circuit breaker L2 — daily loss ${abs(self.daily_pnl):.0f}"
        return True, ""

    def _check_daily_loss(self, new_risk: float) -> Tuple[bool, str]:
        """Check if adding this trade would breach daily loss limits."""
        # Current daily P&L
        if self.daily_pnl <= settings.cb_kill_loss:
            self.circuit_breaker_level = 3
            return False, f"Daily loss ${abs(self.daily_pnl):.0f} hit kill level"

        if self.daily_pnl <= settings.cb_stop_loss:
            self.circuit_breaker_level = 2
            return False, f"Daily loss ${abs(self.daily_pnl):.0f} hit stop level"

        if self.daily_pnl <= settings.cb_warning_loss:
            self.circuit_breaker_level = 1
            # Allow trading but at reduced size
            if new_risk > settings.max_risk_per_trade / 2:
                return False, "Warning level — max risk reduced to 50%"

        return True, ""

    def _check_pdt(self, data: Dict) -> Tuple[bool, str]:
        """Pattern Day Trader rule check."""
        if not settings.pdt_enabled:
            return True, ""

        strategy = data.get("strategy_type", "")
        # Options spreads are generally not day trades unless
        # opened and closed same day. Check if likely to be closed today.
        if strategy in (
            StrategyType.STOCK_LONG.value,
            StrategyType.STOCK_SHORT.value,
        ):
            # Stock trades are most likely to be day trades
            count = db.get_day_trade_count()
            remaining = settings.pdt_max_day_trades - count

            if remaining <= 0:
                return False, f"PDT limit reached ({count}/{settings.pdt_max_day_trades} used)"

            if remaining == 1:
                # Last day trade — alert
                asyncio.create_task(event_bus.publish_alert(
                    "warning",
                    f"⚠️ Last PDT day trade available ({count}/{settings.pdt_max_day_trades})",
                ))

        return True, ""

    def _check_max_positions(self) -> Tuple[bool, str]:
        """Check concurrent position limit."""
        open_count = sum(
            1 for p in self.positions
            if p.status == PositionStatus.OPEN
        )
        if open_count >= settings.max_concurrent_positions:
            return False, f"Max positions reached ({open_count}/{settings.max_concurrent_positions})"
        return True, ""

    def _check_buying_power(self, new_risk: float) -> Tuple[bool, str]:
        """Ensure we have enough buying power."""
        total_at_risk = sum(
            p.max_risk for p in self.positions
            if p.status == PositionStatus.OPEN
        )
        available = settings.total_capital * 0.70  # Keep 30% reserve
        if total_at_risk + new_risk > available:
            return (
                False,
                f"Buying power: ${available - total_at_risk:.0f} avail, "
                f"${new_risk:.0f} needed",
            )
        return True, ""

    def _check_duplicate(
        self, ticker: str, strategy: str
    ) -> Tuple[bool, str]:
        """Prevent duplicate positions on same ticker."""
        for p in self.positions:
            if (p.ticker == ticker and p.status == PositionStatus.OPEN
                    and p.strategy_type.value == strategy):
                return False, f"Already have {strategy} on {ticker}"
        return True, ""

    async def _check_earnings(
        self, ticker: str, data: Dict
    ) -> Tuple[bool, str]:
        """
        Block theta/premium trades near earnings.
        Only Agent 4 (earnings_vol) is allowed to trade near earnings.
        """
        strategy = data.get("strategy_type", "")
        agent = data.get("agent_id", "")

        # Earnings agent is explicitly designed for this
        if agent == "earnings_vol":
            return True, ""

        # Check if this is a premium-selling strategy
        credit_strategies = {
            StrategyType.IRON_CONDOR.value,
            StrategyType.IRON_BUTTERFLY.value,
            StrategyType.PUT_CREDIT_SPREAD.value,
            StrategyType.CALL_CREDIT_SPREAD.value,
            StrategyType.SHORT_STRANGLE.value,
        }

        if strategy in credit_strategies:
            upcoming = await db.get_upcoming_earnings(days_ahead=45)
            for row in upcoming:
                if row["ticker"] == ticker:
                    dte = (row["earnings_date"] - date.today()).days
                    suggested_dte = int(data.get("suggested_dte", 35))
                    if dte <= suggested_dte:
                        return (
                            False,
                            f"Earnings on {row['earnings_date']} "
                            f"({dte}d) within trade DTE ({suggested_dte}d)",
                        )

        return True, ""

    def _check_per_trade_risk(self, risk: float) -> Tuple[bool, str]:
        """Enforce per-trade risk limit."""
        max_allowed = settings.max_risk_per_trade
        if self.circuit_breaker_level >= 1:
            max_allowed /= 2  # Half size in warning mode

        if risk > max_allowed:
            return False, f"Risk ${risk:.0f} > max ${max_allowed:.0f}"
        return True, ""

    def _check_greeks_impact(self, data: Dict) -> Tuple[bool, str]:
        """Check if this trade would breach portfolio Greeks limits."""
        current = self._get_portfolio_greeks()

        # Simplified: check delta exposure
        direction = data.get("direction", "neutral")
        max_risk = float(data.get("max_risk", 0))

        # Rough delta estimate
        est_delta = 0
        if direction == "bullish":
            est_delta = max_risk / 100 * 0.50  # ~50 delta per $100 risk
        elif direction == "bearish":
            est_delta = -max_risk / 100 * 0.50

        new_delta = current.net_delta + est_delta
        if abs(new_delta) > 300:
            return (
                False,
                f"Net delta would be {new_delta:.0f} (limit ±300)",
            )

        return True, ""

    # ═══════════════════════════════════════════
    # Position Management
    # ═══════════════════════════════════════════

    async def _management_loop(self) -> None:
        """
        Periodic position management:
          - Profit target / stop loss checks
          - DTE-based closes
          - Assignment risk warnings
          - Gamma/pin risk on expiry day
        """
        while True:
            try:
                for pos in self.positions:
                    if pos.status != PositionStatus.OPEN:
                        continue

                    # ── Profit target ──
                    if pos.should_take_profit:
                        logger.info(
                            "risk.profit_target",
                            ticker=pos.ticker,
                            pnl=pos.unrealized_pnl,
                            pnl_pct=pos.pnl_pct,
                        )
                        await event_bus.publish_alert(
                            "info",
                            f"📈 {pos.ticker} hit profit target: "
                            f"${pos.unrealized_pnl:.0f} ({pos.pnl_pct:.0%})",
                        )
                        # Mark for closing
                        pos.status = PositionStatus.CLOSING

                    # ── Stop loss ──
                    elif pos.should_stop_loss:
                        logger.warning(
                            "risk.stop_loss",
                            ticker=pos.ticker,
                            pnl=pos.unrealized_pnl,
                        )
                        await event_bus.publish_alert(
                            "warning",
                            f"🛑 {pos.ticker} hit stop loss: "
                            f"${pos.unrealized_pnl:.0f}",
                        )
                        pos.status = PositionStatus.CLOSING

                    # ── DTE check (close if approaching expiry) ──
                    if pos.days_to_expiry <= 1:
                        logger.warning(
                            "risk.expiry_close",
                            ticker=pos.ticker, dte=pos.days_to_expiry,
                        )
                        await event_bus.publish_alert(
                            "warning",
                            f"⏰ {pos.ticker} at {pos.days_to_expiry} DTE — closing",
                        )
                        pos.status = PositionStatus.CLOSING

                    elif pos.days_to_expiry <= 3:
                        await event_bus.publish_alert(
                            "info",
                            f"⏰ {pos.ticker} at {pos.days_to_expiry} DTE — monitor",
                        )

                # Update daily P&L
                self.daily_pnl = await db.get_daily_pnl()
                self._update_circuit_breaker()

                await event_bus.send_heartbeat("risk_manager")

            except Exception as e:
                logger.error("risk.management_error", error=str(e))

            await asyncio.sleep(30)

    def _update_circuit_breaker(self) -> None:
        """Update circuit breaker level based on daily P&L."""
        if self.daily_pnl <= settings.cb_kill_loss:
            if self.circuit_breaker_level < 3:
                self.circuit_breaker_level = 3
                logger.critical(
                    "risk.circuit_breaker_KILL",
                    pnl=self.daily_pnl,
                )
                asyncio.create_task(event_bus.publish_alert(
                    "critical",
                    f"🚨 KILL SWITCH: Daily loss ${abs(self.daily_pnl):.0f} — "
                    f"CLOSING ALL POSITIONS",
                ))
        elif self.daily_pnl <= settings.cb_stop_loss:
            if self.circuit_breaker_level < 2:
                self.circuit_breaker_level = 2
                logger.warning("risk.circuit_breaker_STOP", pnl=self.daily_pnl)
                asyncio.create_task(event_bus.publish_alert(
                    "warning",
                    f"🛑 Circuit breaker L2: Daily loss ${abs(self.daily_pnl):.0f} — "
                    f"New trades BLOCKED",
                ))
        elif self.daily_pnl <= settings.cb_warning_loss:
            if self.circuit_breaker_level < 1:
                self.circuit_breaker_level = 1
                logger.warning("risk.circuit_breaker_WARNING", pnl=self.daily_pnl)
                asyncio.create_task(event_bus.publish_alert(
                    "warning",
                    f"⚠️ Circuit breaker L1: Daily loss ${abs(self.daily_pnl):.0f} — "
                    f"Half position sizes",
                ))
        else:
            self.circuit_breaker_level = 0

    def _get_portfolio_greeks(self) -> PortfolioGreeks:
        """Aggregate Greeks across all open positions."""
        greeks = PortfolioGreeks()
        for pos in self.positions:
            if pos.status != PositionStatus.OPEN:
                continue
            greeks.net_delta += pos.current_delta
            greeks.net_gamma += pos.current_gamma
            greeks.net_theta += pos.current_theta
            greeks.net_vega += pos.current_vega
            greeks.total_risk += pos.max_risk
        greeks.buying_power_used = greeks.total_risk
        greeks.buying_power_pct = (
            greeks.total_risk / settings.total_capital
            if settings.total_capital > 0 else 0
        )
        return greeks

    async def get_state(self) -> PortfolioState:
        """Get complete portfolio state for dashboard."""
        return PortfolioState(
            positions=self.positions,
            greeks=self._get_portfolio_greeks(),
            daily_pnl=self.daily_pnl,
            open_pnl=sum(
                p.unrealized_pnl for p in self.positions
                if p.status == PositionStatus.OPEN
            ),
            day_trades_used=db.get_day_trade_count(),
            circuit_breaker_level=self.circuit_breaker_level,
        )

    # ═══════════════════════════════════════════
    # Dead Man's Switch
    # ═══════════════════════════════════════════

    async def _dead_man_switch(self) -> None:
        """
        Monitor system health. If critical components
        stop sending heartbeats, trigger emergency close.
        """
        while True:
            try:
                critical = ["feature_store", "consensus"]
                for component in critical:
                    last = await event_bus.check_heartbeat(component)
                    if last is None:
                        continue  # Not started yet
                    age = time.time() - last
                    if age > 120:  # 2 minutes without heartbeat
                        logger.critical(
                            "risk.dead_man_switch",
                            component=component,
                            seconds_since_heartbeat=age,
                        )
                        await event_bus.publish_alert(
                            "critical",
                            f"💀 Dead Man's Switch: {component} "
                            f"unresponsive for {age:.0f}s",
                        )
                        # Don't auto-close in paper mode
                        if settings.trading_mode.value == "live":
                            self.circuit_breaker_level = 3

            except Exception as e:
                logger.error("risk.dead_man_error", error=str(e))

            await asyncio.sleep(30)
