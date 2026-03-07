"""
Options Analytics Engine.

Computes:
  - Implied Volatility surface (strike × expiry)
  - IV Rank (vs 52-week range)
  - IV Percentile (% of days below current)
  - Put/Call skew
  - Term structure (front vs back month)
  - Realized volatility
  - Greeks aggregation for portfolio
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import structlog
from scipy import interpolate, stats

from core.models import (
    IVMetrics, OptionQuote, OptionType, OptionsChain,
)
from core.database import db

logger = structlog.get_logger()


# ═══════════════════════════════════════════════
# Black-Scholes Pricing
# ═══════════════════════════════════════════════

def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return (
        (math.log(S / K) + (r + 0.5 * sigma**2) * T)
        / (sigma * math.sqrt(T))
    )


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return _d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def bs_call_price(
    S: float, K: float, T: float, r: float, sigma: float
) -> float:
    """Black-Scholes call price."""
    if T <= 0:
        return max(S - K, 0)
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    return S * stats.norm.cdf(d1) - K * math.exp(-r * T) * stats.norm.cdf(d2)


def bs_put_price(
    S: float, K: float, T: float, r: float, sigma: float
) -> float:
    """Black-Scholes put price."""
    if T <= 0:
        return max(K - S, 0)
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)


def implied_volatility(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: str = "C",
    max_iter: int = 100,
    tol: float = 1e-6,
) -> float:
    """
    Compute implied volatility using Newton-Raphson method.
    Falls back to bisection if Newton fails.
    """
    if T <= 0 or market_price <= 0:
        return 0.0

    price_func = bs_call_price if option_type == "C" else bs_put_price

    # Initial guess from Brenner-Subrahmanyam approximation
    sigma = math.sqrt(2 * math.pi / T) * market_price / S

    # Newton-Raphson
    for _ in range(max_iter):
        try:
            price = price_func(S, K, T, r, sigma)
            d1 = _d1(S, K, T, r, sigma)
            vega = S * stats.norm.pdf(d1) * math.sqrt(T)

            if abs(vega) < 1e-10:
                break

            diff = market_price - price
            if abs(diff) < tol:
                return sigma

            sigma += diff / vega

            if sigma <= 0:
                sigma = 0.001

        except (ValueError, ZeroDivisionError, OverflowError):
            break

    # Bisection fallback
    lo, hi = 0.001, 5.0
    for _ in range(100):
        mid = (lo + hi) / 2
        price = price_func(S, K, T, r, mid)
        if abs(price - market_price) < tol:
            return mid
        if price < market_price:
            lo = mid
        else:
            hi = mid

    return (lo + hi) / 2


# ═══════════════════════════════════════════════
# Greeks Calculation
# ═══════════════════════════════════════════════

def compute_greeks(
    S: float, K: float, T: float, r: float, sigma: float,
    option_type: str = "C",
) -> Dict[str, float]:
    """Compute all Greeks for a single option."""
    if T <= 0 or sigma <= 0:
        return {
            "delta": 1.0 if option_type == "C" and S > K else (
                -1.0 if option_type == "P" and S < K else 0.0
            ),
            "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0,
        }

    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    sqrt_T = math.sqrt(T)
    pdf_d1 = stats.norm.pdf(d1)
    exp_rT = math.exp(-r * T)

    if option_type == "C":
        delta = stats.norm.cdf(d1)
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            - r * K * exp_rT * stats.norm.cdf(d2)
        ) / 365
        rho = K * T * exp_rT * stats.norm.cdf(d2) / 100
    else:
        delta = stats.norm.cdf(d1) - 1
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            + r * K * exp_rT * stats.norm.cdf(-d2)
        ) / 365
        rho = -K * T * exp_rT * stats.norm.cdf(-d2) / 100

    gamma = pdf_d1 / (S * sigma * sqrt_T)
    vega = S * pdf_d1 * sqrt_T / 100

    return {
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "theta": round(theta, 6),
        "vega": round(vega, 6),
        "rho": round(rho, 6),
    }


# ═══════════════════════════════════════════════
# Options Analytics Engine
# ═══════════════════════════════════════════════

class OptionsAnalytics:
    """
    Core analytics engine that processes options chains
    and computes IV surface, IV rank, skew, and more.
    """

    RISK_FREE_RATE = 0.05  # ~5% (update from Fed rate)

    def __init__(self):
        self._iv_cache: Dict[str, IVMetrics] = {}

    def compute_iv_metrics(
        self, chain: OptionsChain, iv_history: List[float] = None,
    ) -> IVMetrics:
        """
        Compute full IV metrics for a ticker given its options chain.

        Args:
            chain: Current options chain snapshot
            iv_history: List of historical daily IV closes (for rank calc)
        """
        ticker = chain.ticker
        price = chain.underlying_price
        now = datetime.utcnow()

        # Find ATM IV for different expiries
        expiries = chain.get_expiries()
        iv_atm_by_dte: Dict[int, float] = {}

        for exp in expiries:
            dte = (exp - date.today()).days
            if dte <= 0:
                continue
            atm_strike = chain.get_atm_strike(exp)
            # Use call ATM IV
            call = chain.get_quote(exp, atm_strike, OptionType.CALL)
            if call and call.implied_vol > 0:
                iv_atm_by_dte[dte] = call.implied_vol
            elif call and call.mid > 0:
                # Compute IV from price
                T = dte / 365.0
                iv = implied_volatility(
                    call.mid, price, atm_strike, T,
                    self.RISK_FREE_RATE, "C"
                )
                if iv > 0:
                    iv_atm_by_dte[dte] = iv

        # ATM IV for ~7 DTE and ~30 DTE
        iv_atm_7d = self._interpolate_iv(iv_atm_by_dte, 7)
        iv_atm_30d = self._interpolate_iv(iv_atm_by_dte, 30)

        # IV Rank & Percentile
        iv_rank = 0.0
        iv_percentile = 0.0
        current_iv = iv_atm_30d or iv_atm_7d or 0.0

        if iv_history and len(iv_history) >= 20 and current_iv > 0:
            iv_min = min(iv_history)
            iv_max = max(iv_history)
            if iv_max > iv_min:
                iv_rank = (
                    (current_iv - iv_min) / (iv_max - iv_min)
                ) * 100
            iv_percentile = (
                sum(1 for iv in iv_history if iv < current_iv)
                / len(iv_history)
            ) * 100

        # 25-delta skew (put IV - call IV at 25 delta)
        skew_25d = self._compute_skew(chain, 0.25)

        # Term structure slope
        term_slope = 0.0
        if iv_atm_30d and iv_atm_7d and iv_atm_7d > 0:
            term_slope = (iv_atm_30d - iv_atm_7d) / iv_atm_7d

        # Realized volatility (20-day)
        rv_20d = 0.0  # Computed externally from price data

        metrics = IVMetrics(
            ticker=ticker,
            timestamp=now,
            iv_atm_30d=round(iv_atm_30d or 0, 6),
            iv_atm_7d=round(iv_atm_7d or 0, 6),
            iv_rank=round(iv_rank, 2),
            iv_percentile=round(iv_percentile, 2),
            skew_25d=round(skew_25d, 6),
            term_slope=round(term_slope, 6),
            rv_20d=rv_20d,
            iv_rv_ratio=round(
                (iv_atm_30d / rv_20d) if rv_20d > 0 else 0, 4
            ),
        )

        self._iv_cache[ticker] = metrics
        return metrics

    def compute_realized_vol(
        self, closes: List[float], window: int = 20
    ) -> float:
        """
        Compute annualized realized volatility from close prices.
        Uses log returns × sqrt(252).
        """
        if len(closes) < window + 1:
            return 0.0

        arr = np.array(closes[-(window + 1):])
        log_returns = np.log(arr[1:] / arr[:-1])
        return float(np.std(log_returns) * math.sqrt(252))

    def enrich_greeks(self, chain: OptionsChain) -> OptionsChain:
        """
        Compute Greeks for all quotes in a chain
        that don't already have them.
        """
        price = chain.underlying_price
        for q in chain.quotes:
            if q.delta != 0:
                continue  # Already has Greeks

            dte = (q.expiry - date.today()).days
            if dte <= 0:
                continue

            T = dte / 365.0
            sigma = q.implied_vol

            # If no IV, compute from price
            if sigma <= 0 and q.mid > 0:
                sigma = implied_volatility(
                    q.mid, price, q.strike, T,
                    self.RISK_FREE_RATE, q.option_type.value,
                )
                q.implied_vol = sigma

            if sigma > 0:
                greeks = compute_greeks(
                    price, q.strike, T, self.RISK_FREE_RATE,
                    sigma, q.option_type.value,
                )
                q.delta = greeks["delta"]
                q.gamma = greeks["gamma"]
                q.theta = greeks["theta"]
                q.vega = greeks["vega"]

        return chain

    def get_cached_metrics(self, ticker: str) -> Optional[IVMetrics]:
        return self._iv_cache.get(ticker)

    # ── Private Helpers ──

    def _interpolate_iv(
        self, iv_by_dte: Dict[int, float], target_dte: int
    ) -> Optional[float]:
        """Interpolate IV for a specific DTE from known data points."""
        if not iv_by_dte:
            return None

        dtes = sorted(iv_by_dte.keys())
        ivs = [iv_by_dte[d] for d in dtes]

        if len(dtes) == 1:
            return ivs[0]

        # Find nearest
        if target_dte <= dtes[0]:
            return ivs[0]
        if target_dte >= dtes[-1]:
            return ivs[-1]

        # Linear interpolation
        for i in range(len(dtes) - 1):
            if dtes[i] <= target_dte <= dtes[i + 1]:
                frac = (
                    (target_dte - dtes[i]) / (dtes[i + 1] - dtes[i])
                )
                return ivs[i] + frac * (ivs[i + 1] - ivs[i])

        return ivs[0]

    def _compute_skew(
        self, chain: OptionsChain, target_delta: float = 0.25
    ) -> float:
        """
        Compute put-call skew at target delta level.
        Positive skew = puts are more expensive (normal).
        """
        expiries = chain.get_expiries()
        # Use nearest monthly expiry (closest to 30 DTE)
        target_exp = None
        min_diff = float("inf")
        for exp in expiries:
            dte = (exp - date.today()).days
            diff = abs(dte - 30)
            if diff < min_diff and dte > 5:
                min_diff = diff
                target_exp = exp

        if not target_exp:
            return 0.0

        put = chain.get_by_delta(
            target_exp, target_delta, OptionType.PUT
        )
        call = chain.get_by_delta(
            target_exp, target_delta, OptionType.CALL
        )

        if put and call and put.implied_vol > 0 and call.implied_vol > 0:
            return put.implied_vol - call.implied_vol
        return 0.0


# Singleton
options_analytics = OptionsAnalytics()
