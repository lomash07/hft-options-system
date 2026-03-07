"""
Session Scheduler.

Manages the daily trading lifecycle:
  PRE_MARKET  (04:00-09:30 ET) → Scanner + data loading
  OPEN_STRIKE (09:30-11:00 ET) → ALL agents active
  MIDDAY      (11:00-14:30 ET) → Theta + Mean Rev only
  CLOSE_STRIKE(14:30-16:00 ET) → ALL agents re-activated
  POST_MARKET (16:00-18:00 ET) → Analysis + retraining
  CLOSED      (18:00-04:00 ET) → Sleep
"""

from __future__ import annotations

from datetime import datetime, time
from typing import List

import pytz

from config.settings import SessionPhase

ET = pytz.timezone("US/Eastern")


def get_current_phase() -> SessionPhase:
    """Determine current session phase based on ET time."""
    now = datetime.now(ET).time()

    if time(4, 0) <= now < time(9, 30):
        return SessionPhase.PRE_MARKET
    elif time(9, 30) <= now < time(11, 0):
        return SessionPhase.OPEN_STRIKE
    elif time(11, 0) <= now < time(14, 30):
        return SessionPhase.MIDDAY
    elif time(14, 30) <= now < time(16, 0):
        return SessionPhase.CLOSE_STRIKE
    elif time(16, 0) <= now < time(18, 0):
        return SessionPhase.POST_MARKET
    else:
        return SessionPhase.CLOSED


def get_active_agents(phase: SessionPhase) -> List[str]:
    """
    Which agents should be active in each phase.
    Returns list of agent IDs.
    """
    ALL_AGENTS = [
        "theta_harvester", "vol_arb", "unusual_flow",
        "directional_momentum", "earnings_vol", "mean_reversion",
    ]

    THETA_ONLY = ["theta_harvester", "mean_reversion", "vol_arb"]

    if phase == SessionPhase.OPEN_STRIKE:
        return ALL_AGENTS
    elif phase == SessionPhase.CLOSE_STRIKE:
        return ALL_AGENTS
    elif phase == SessionPhase.MIDDAY:
        return THETA_ONLY
    else:
        return []  # No agents during pre/post/closed


def is_market_open() -> bool:
    """Check if US market is currently open."""
    now = datetime.now(ET)
    # Skip weekends
    if now.weekday() >= 5:
        return False
    t = now.time()
    return time(9, 30) <= t < time(16, 0)
