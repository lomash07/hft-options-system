"""
HFT Options Trading System — Main Entry Point.

Orchestrates all components:
  1. Infrastructure setup (Redis, DB, metrics)
  2. Data feed connection (IB or yfinance fallback)
  3. Session scheduling (pre-market → close)
  4. Agent lifecycle management
  5. Monitoring & alerting

Usage:
  python main.py              # Normal mode (follows market hours)
  python main.py --scan-only  # Pre-market scan only
  python main.py --backtest   # Run backtester (Phase 4)
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from datetime import datetime

import structlog

from config.settings import settings, UNIVERSE
from core.event_bus import event_bus
from core.database import db
from core.models import DailyWatchlist
from core.session import (
    get_current_phase, get_active_agents, is_market_open,
    SessionPhase,
)
from data.broker_feed import create_feed, DataFeed
from data.feature_store import FeatureStore
from data.options_analytics import options_analytics
from data.market_regime import RegimeDetector
from scanner import PreMarketScanner
from strategy.options_constructor import OptionsConstructor
from strategy.consensus import ConsensusEngine
from risk import RiskManager
from execution import ExecutionEngine
from monitoring import (
    configure_logging, start_metrics_server,
    TelegramAlerts, AlertManager,
    daily_pnl_gauge, open_positions_gauge, circuit_breaker_gauge,
    vix_gauge, iv_rank_gauge,
)

# Agents (Phase 2-3)
from agents.theta_harvester import ThetaHarvester
from agents.vol_arb import VolArb
from agents.unusual_flow import UnusualFlow
from agents.directional_momentum import DirectionalMomentum
from agents.earnings_vol import EarningsVol
from agents.mean_reversion import MeanReversion

logger = structlog.get_logger()


class TradingSystem:
    """
    Main orchestrator for the entire trading system.
    """

    def __init__(self):
        self.feed: DataFeed = None
        self.feature_store: FeatureStore = None
        self.regime_detector: RegimeDetector = None
        self.scanner: PreMarketScanner = None
        self.constructor: OptionsConstructor = None
        self.consensus: ConsensusEngine = None
        self.risk_manager: RiskManager = None
        self.execution: ExecutionEngine = None
        self.telegram: TelegramAlerts = None
        self.alert_manager: AlertManager = None

        self._agents: dict = {}
        self._agent_tasks: list = []
        self._running = False
        self._current_phase: SessionPhase = SessionPhase.CLOSED

    async def initialize(self) -> None:
        """Set up all components."""
        logger.info(
            "system.initializing",
            mode=settings.trading_mode.value,
            capital=settings.total_capital,
        )

        # ── Infrastructure ──
        await event_bus.connect()
        await db.connect()
        start_metrics_server()

        # ── Data ──
        self.feed = await create_feed()
        self.feature_store = FeatureStore(self.feed, options_analytics)
        self.regime_detector = RegimeDetector(self.feed)

        # ── Scanner ──
        self.scanner = PreMarketScanner(self.feed, self.feature_store)

        # ── Strategy ──
        self.constructor = OptionsConstructor()
        self.consensus = ConsensusEngine(self.feature_store, self.constructor)

        # ── Risk & Execution ──
        self.risk_manager = RiskManager()
        self.execution = ExecutionEngine()

        # ── Monitoring ──
        self.telegram = TelegramAlerts()
        await self.telegram.start()
        self.alert_manager = AlertManager(self.telegram)

        # ── Agents (Phase 2-3) ──
        self._agents = {
            "theta_harvester": ThetaHarvester(self.feature_store),
            "vol_arb": VolArb(self.feature_store),
            "unusual_flow": UnusualFlow(self.feature_store),
            "directional_momentum": DirectionalMomentum(self.feature_store),
            "earnings_vol": EarningsVol(self.feature_store),
            "mean_reversion": MeanReversion(self.feature_store),
        }

        logger.info("system.initialized", agents=list(self._agents.keys()))

    async def run(self) -> None:
        """Main run loop — follows market hours."""
        self._running = True
        logger.info("system.starting")

        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Start background services
        await self.risk_manager.start()
        await self.execution.start()
        await self.alert_manager.start()

        await self.telegram.send(
            f"🚀 <b>Trading System Started</b>\n"
            f"Mode: {settings.trading_mode.value}\n"
            f"Capital: ${settings.total_capital:,.0f}\n"
            f"Universe: {len(UNIVERSE)} tickers"
        )

        # ── Session Loop ──
        while self._running:
            try:
                phase = get_current_phase()

                if phase != self._current_phase:
                    await self._transition_phase(phase)
                    self._current_phase = phase

                # Update metrics
                self._update_metrics()

                await asyncio.sleep(30)  # Check phase every 30s

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("system.loop_error", error=str(e))
                await asyncio.sleep(10)

    async def run_scan_only(self) -> None:
        """Run pre-market scan and exit."""
        await self.feature_store.initialize()
        await self.regime_detector.update()
        watchlist = await self.scanner.build_watchlist()
        self._print_watchlist(watchlist)

    async def _transition_phase(self, new_phase: SessionPhase) -> None:
        """Handle phase transitions."""
        old = self._current_phase
        logger.info(
            "system.phase_transition",
            old=old.value, new=new_phase.value,
        )

        await self.telegram.send(
            f"⏰ Phase: <b>{new_phase.value}</b>"
        )

        # ── Stop current agents ──
        for task in self._agent_tasks:
            task.cancel()
        self._agent_tasks = []

        # ── Phase-specific actions ──

        if new_phase == SessionPhase.PRE_MARKET:
            # Initialize data, build watchlist
            await self.feature_store.initialize()
            await self.regime_detector.update()
            watchlist = await self.scanner.build_watchlist()
            self._print_watchlist(watchlist)

            # Start feature update loop for watchlist tickers
            tickers = self.scanner.get_watchlist_tickers()
            asyncio.create_task(
                self.feature_store.run_update_loop(tickers + ["SPY"], 60)
            )

        elif new_phase in (SessionPhase.OPEN_STRIKE, SessionPhase.CLOSE_STRIKE):
            # ALL agents active
            regime = await self.regime_detector.update()
            watchlist_tickers = self.scanner.get_watchlist_tickers()
            active_ids = get_active_agents(new_phase)

            # Start consensus engine
            asyncio.create_task(self.consensus.start())

            for agent_id in active_ids:
                agent = self._agents.get(agent_id)
                if agent:
                    task = asyncio.create_task(
                        agent.start(watchlist_tickers, regime)
                    )
                    self._agent_tasks.append(task)

            logger.info(
                "system.agents_activated",
                phase=new_phase.value,
                agents=active_ids,
                regime=regime.value,
            )

        elif new_phase == SessionPhase.MIDDAY:
            # Theta + Mean Rev only
            regime = await self.regime_detector.update()
            watchlist_tickers = self.scanner.get_watchlist_tickers()
            active_ids = get_active_agents(new_phase)

            asyncio.create_task(self.consensus.start())

            for agent_id in active_ids:
                agent = self._agents.get(agent_id)
                if agent:
                    task = asyncio.create_task(
                        agent.start(watchlist_tickers, regime)
                    )
                    self._agent_tasks.append(task)

        elif new_phase == SessionPhase.POST_MARKET:
            # Daily report + cleanup
            await self._post_market_tasks()

        elif new_phase == SessionPhase.CLOSED:
            logger.info("system.market_closed")

    async def _post_market_tasks(self) -> None:
        """End-of-day analysis and reporting."""
        daily_pnl = await db.get_daily_pnl()
        open_positions = self.execution.get_open_positions()

        # Store IV closes for IV rank history
        for ticker in UNIVERSE:
            metrics = options_analytics.get_cached_metrics(ticker)
            if metrics and metrics.iv_atm_30d > 0:
                db.store_iv_close(ticker, metrics.iv_atm_30d)

        # Get regime
        regime_info = await event_bus.get_feature_json("regime")
        regime = regime_info.get("regime", "unknown") if regime_info else "unknown"

        await self.telegram.send_daily_report(
            pnl=daily_pnl,
            trades=0,  # TODO: count from DB
            win_rate=0.0,  # TODO: compute from DB
            positions=len(open_positions),
            regime=regime,
        )

        logger.info(
            "system.daily_report",
            pnl=daily_pnl,
            open_positions=len(open_positions),
            regime=regime,
        )

    def _update_metrics(self) -> None:
        """Push current state to Prometheus."""
        try:
            daily_pnl_gauge.set(self.risk_manager.daily_pnl)
            circuit_breaker_gauge.set(self.risk_manager.circuit_breaker_level)
            open_positions_gauge.set(
                len(self.execution.get_open_positions())
            )
        except Exception:
            pass

    def _print_watchlist(self, watchlist: DailyWatchlist) -> None:
        """Pretty-print watchlist to console."""
        print("\n" + "=" * 70)
        print(f"  📋 DAILY WATCHLIST — {watchlist.date}")
        print(f"  Regime: {watchlist.regime}  |  VIX: {watchlist.vix_level:.1f}")
        print(f"  SPY Gap: {watchlist.spy_gap_pct:+.2f}%")
        print("=" * 70)

        for i, item in enumerate(watchlist.items, 1):
            strategies = ", ".join(s.value for s in item.suggested_strategies)
            earnings = f" 📊 ER in {item.days_to_earnings}d" if item.has_earnings_soon else ""
            flow = " 🔥 Unusual flow" if item.unusual_options_activity else ""
            print(
                f"  {i}. {item.ticker:6s} "
                f"Score: {item.score:.2f}  "
                f"IV Rank: {item.iv_rank:5.1f}%  "
                f"Gap: {item.gap_pct:+5.1f}%  "
                f"{earnings}{flow}"
            )
            if strategies:
                print(f"     └─ Strategies: {strategies}")
            if item.notes:
                print(f"     └─ {item.notes}")

        print("=" * 70 + "\n")

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("system.shutting_down")
        self._running = False

        # Stop agents
        for task in self._agent_tasks:
            task.cancel()

        # Close connections
        await self.feed.disconnect()
        await event_bus.disconnect()
        await db.disconnect()

        await self.telegram.send("🔴 <b>Trading System Stopped</b>")
        logger.info("system.shutdown_complete")


# ═══════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(
        description="HFT Options Trading System"
    )
    parser.add_argument(
        "--scan-only", action="store_true",
        help="Run pre-market scan and exit",
    )
    parser.add_argument(
        "--mode", choices=["paper", "live"],
        default=None,
        help="Override trading mode",
    )
    args = parser.parse_args()

    configure_logging()

    system = TradingSystem()

    try:
        await system.initialize()

        if args.scan_only:
            await system.run_scan_only()
        else:
            await system.run()
    except KeyboardInterrupt:
        pass
    finally:
        await system.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
