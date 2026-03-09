"""
Monitoring, Alerting & Observability.

Components:
  - Telegram bot: commands (/status, /pnl, /kill, /watchlist)
  - Prometheus metrics: for Grafana dashboards
  - Alert consumer: routes alerts to Telegram
  - Daily report generation
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Optional

import structlog
from prometheus_client import (
    Counter, Gauge, Histogram, start_http_server,
)

from config.settings import settings
from core.event_bus import (
    STREAM_ALERTS, STREAM_FILLS, GROUP_MONITOR, event_bus,
)

logger = structlog.get_logger()


# ═══════════════════════════════════════════════
# Prometheus Metrics
# ═══════════════════════════════════════════════

# P&L
daily_pnl_gauge = Gauge("hft_daily_pnl_dollars", "Daily P&L in dollars")
open_pnl_gauge = Gauge("hft_open_pnl_dollars", "Open P&L in dollars")
total_capital_gauge = Gauge("hft_total_capital", "Total capital")

# Positions
open_positions_gauge = Gauge("hft_open_positions", "Number of open positions")
day_trades_gauge = Gauge("hft_day_trades_used", "PDT day trades used (5d rolling)")
circuit_breaker_gauge = Gauge("hft_circuit_breaker_level", "Circuit breaker level 0-3")

# Greeks
portfolio_delta_gauge = Gauge("hft_portfolio_delta", "Net portfolio delta")
portfolio_theta_gauge = Gauge("hft_portfolio_theta", "Net portfolio theta")
portfolio_vega_gauge = Gauge("hft_portfolio_vega", "Net portfolio vega")

# IV
iv_rank_gauge = Gauge("hft_iv_rank", "IV rank per ticker", ["ticker"])
vix_gauge = Gauge("hft_vix_level", "VIX level")

# Trading activity
signals_counter = Counter("hft_signals_total", "Total signals emitted", ["agent"])
trades_counter = Counter("hft_trades_total", "Total trades executed", ["strategy"])
fills_counter = Counter("hft_fills_total", "Total fills", ["ticker"])
rejects_counter = Counter("hft_rejects_total", "Rejected orders", ["reason"])

# Regime
regime_gauge = Gauge("hft_market_regime", "Market regime code (1-5)")


def start_metrics_server() -> None:
    """Start Prometheus metrics HTTP endpoint."""
    start_http_server(settings.metrics_port)
    total_capital_gauge.set(settings.total_capital)
    logger.info("metrics.server_started", port=settings.metrics_port)


# ═══════════════════════════════════════════════
# Telegram Bot
# ═══════════════════════════════════════════════

class TelegramAlerts:
    """Telegram bot for alerts and control commands."""

    def __init__(self):
        self._bot = None
        self._enabled = bool(settings.telegram_bot_token)

    async def start(self) -> None:
        """Initialize Telegram bot."""
        if not self._enabled:
            logger.info("telegram.disabled", reason="No bot token configured")
            return

        try:
            from telegram import Bot
            self._bot = Bot(token=settings.telegram_bot_token)
            me = await self._bot.get_me()
            logger.info("telegram.connected", bot=me.username)
        except Exception as e:
            logger.error("telegram.connection_failed", error=str(e))
            self._enabled = False

    async def send(self, message: str, parse_mode: str = "HTML") -> None:
        """Send a message to the configured chat."""
        if not self._enabled or not self._bot:
            logger.debug("telegram.skipped", msg=message[:50])
            return

        try:
            await self._bot.send_message(
                chat_id=settings.telegram_chat_id,
                text=message,
                parse_mode=parse_mode,
            )
        except Exception as e:
            logger.error("telegram.send_error", error=str(e))

    async def send_daily_report(
        self, pnl: float, trades: int, win_rate: float,
        positions: int, regime: str,
    ) -> None:
        """Send end-of-day report."""
        emoji = "📈" if pnl >= 0 else "📉"
        msg = (
            f"{emoji} <b>Daily Report — {date.today()}</b>\n\n"
            f"P&L: <b>${pnl:+.2f}</b>\n"
            f"Trades: {trades}\n"
            f"Win Rate: {win_rate:.0%}\n"
            f"Open Positions: {positions}\n"
            f"Regime: {regime}\n"
        )
        await self.send(msg)


# ═══════════════════════════════════════════════
# Alert Consumer
# ═══════════════════════════════════════════════

class AlertManager:
    """Consumes alerts from event bus and routes to Telegram."""

    def __init__(self, telegram: TelegramAlerts):
        self.telegram = telegram

    async def start(self) -> None:
        """Start consuming alerts."""
        event_bus.start_consumer(
            STREAM_ALERTS,
            GROUP_MONITOR,
            "alert_worker",
            self._handle_alert,
        )

        event_bus.start_consumer(
            STREAM_FILLS,
            GROUP_MONITOR,
            "fill_alert_worker",
            self._handle_fill,
        )

        logger.info("alert_manager.started")

    async def _handle_alert(self, data: dict) -> None:
        level = data.get("level", "info")
        message = data.get("message", "")

        level_emoji = {
            "info": "ℹ️",
            "warning": "⚠️",
            "critical": "🚨",
        }

        emoji = level_emoji.get(level, "📌")
        await self.telegram.send(f"{emoji} {message}")

    async def _handle_fill(self, data: dict) -> None:
        ticker = data.get("ticker", "")
        strategy = data.get("strategy", "")
        fill = data.get("fill_price", 0)
        fills_counter.labels(ticker=ticker).inc()
        trades_counter.labels(strategy=strategy).inc()


# ═══════════════════════════════════════════════
# Logging Setup
# ═══════════════════════════════════════════════

def configure_logging() -> None:
    """Configure structured logging."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if settings.log_level == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(__import__("logging"), settings.log_level.upper(), 20)
        ),
    )
