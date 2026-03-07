"""
Options Strategy Constructor.

Translates consensus signals into concrete, executable multi-leg options orders.

Input:  Direction + IV State + Regime + Risk Budget
Output: Complete StrategyOrder with legs, strikes, expiries, sizing

Strike selection logic:
  - Short strikes: based on delta (30Δ standard)
  - Width: based on risk budget ($200-400)
  - Expiry: 30-45 DTE (premium) or 7-14 DTE (directional)

Liquidity filter:
  - Bid-ask spread < 10% of mid
  - Min OI > 100 on chosen strikes
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

import structlog

from config.settings import MarketRegime, settings
from core.models import (
    Direction, OptionLeg, OptionQuote, OptionType,
    OptionsChain, OrderSide, StrategyOrder, StrategyType, StockLeg,
)

logger = structlog.get_logger()


class OptionsConstructor:
    """Builds executable options strategy orders from signals."""

    def construct(
        self,
        chain: OptionsChain,
        strategy_type: StrategyType,
        direction: Direction,
        target_dte: int = 35,
        target_delta: float = 0.30,
        max_risk: float = None,
        agent_id: str = "",
        signal_score: float = 0.0,
        regime: str = "",
    ) -> Optional[StrategyOrder]:
        """
        Build a complete strategy order.

        Returns None if no suitable strikes found or liquidity insufficient.
        """
        max_risk = max_risk or settings.max_risk_per_trade

        builders = {
            StrategyType.IRON_CONDOR: self._build_iron_condor,
            StrategyType.IRON_BUTTERFLY: self._build_iron_butterfly,
            StrategyType.PUT_CREDIT_SPREAD: self._build_credit_spread,
            StrategyType.CALL_CREDIT_SPREAD: self._build_credit_spread,
            StrategyType.CALL_DEBIT_SPREAD: self._build_debit_spread,
            StrategyType.PUT_DEBIT_SPREAD: self._build_debit_spread,
            StrategyType.CALENDAR_SPREAD: self._build_calendar,
            StrategyType.STOCK_LONG: self._build_stock,
            StrategyType.STOCK_SHORT: self._build_stock,
        }

        builder = builders.get(strategy_type)
        if not builder:
            logger.warning(
                "constructor.unknown_strategy",
                strategy=strategy_type.value,
            )
            return None

        order = builder(
            chain, strategy_type, direction, target_dte,
            target_delta, max_risk,
        )

        if order:
            order.id = str(uuid.uuid4())[:8]
            order.agent_id = agent_id
            order.signal_score = signal_score
            order.market_regime = regime

        return order

    # ── Strategy Builders ──

    def _build_iron_condor(
        self, chain: OptionsChain, strategy_type: StrategyType,
        direction: Direction, target_dte: int, target_delta: float,
        max_risk: float,
    ) -> Optional[StrategyOrder]:
        """
        Iron Condor: sell OTM put + sell OTM call,
        buy further OTM put + buy further OTM call.
        """
        expiry = self._find_expiry(chain, target_dte)
        if not expiry:
            return None

        # Find short strikes at target delta
        short_put = chain.get_by_delta(expiry, target_delta, OptionType.PUT)
        short_call = chain.get_by_delta(expiry, target_delta, OptionType.CALL)

        if not short_put or not short_call:
            return None

        # Width: determine from max risk
        strikes = chain.get_strikes(expiry)
        strike_width = self._find_wing_width(
            strikes, short_put.strike, max_risk, "put"
        )

        long_put_strike = short_put.strike - strike_width
        long_call_strike = short_call.strike + strike_width

        long_put = chain.get_quote(expiry, long_put_strike, OptionType.PUT)
        long_call = chain.get_quote(expiry, long_call_strike, OptionType.CALL)

        if not long_put or not long_call:
            # Try nearest available strikes
            long_put = self._nearest_strike(chain, expiry, long_put_strike, OptionType.PUT)
            long_call = self._nearest_strike(chain, expiry, long_call_strike, OptionType.CALL)
            if not long_put or not long_call:
                return None

        # Liquidity check
        for q in [short_put, short_call, long_put, long_call]:
            if not self._check_liquidity(q):
                return None

        # Calculate credit and risk
        put_spread_credit = short_put.mid - long_put.mid
        call_spread_credit = short_call.mid - long_call.mid
        total_credit = put_spread_credit + call_spread_credit
        width = max(
            short_put.strike - long_put.strike,
            long_call.strike - short_call.strike,
        )
        risk = (width - total_credit) * 100  # × 100 shares per contract

        if risk > max_risk or total_credit <= 0:
            return None

        ticker = chain.ticker
        legs = [
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=expiry, strike=short_put.strike,
                      option_type=OptionType.PUT),
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=expiry, strike=long_put.strike,
                      option_type=OptionType.PUT),
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=expiry, strike=short_call.strike,
                      option_type=OptionType.CALL),
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=expiry, strike=long_call.strike,
                      option_type=OptionType.CALL),
        ]

        return StrategyOrder(
            ticker=ticker,
            strategy_type=StrategyType.IRON_CONDOR,
            direction=Direction.NEUTRAL,
            option_legs=legs,
            net_credit_debit=round(total_credit * 100, 2),
            max_risk=round(risk, 2),
            max_reward=round(total_credit * 100, 2),
            profit_target=0.50,
            stop_loss=2.0,
            dte_close=21,
        )

    def _build_iron_butterfly(
        self, chain, strategy_type, direction, target_dte,
        target_delta, max_risk,
    ) -> Optional[StrategyOrder]:
        """Iron Butterfly: sell ATM put + call, buy OTM wings."""
        expiry = self._find_expiry(chain, target_dte)
        if not expiry:
            return None

        atm = chain.get_atm_strike(expiry)
        strikes = chain.get_strikes(expiry)
        wing = self._find_wing_width(strikes, atm, max_risk, "put")

        short_put = chain.get_quote(expiry, atm, OptionType.PUT)
        short_call = chain.get_quote(expiry, atm, OptionType.CALL)
        long_put = self._nearest_strike(chain, expiry, atm - wing, OptionType.PUT)
        long_call = self._nearest_strike(chain, expiry, atm + wing, OptionType.CALL)

        if not all([short_put, short_call, long_put, long_call]):
            return None

        credit = (short_put.mid + short_call.mid - long_put.mid - long_call.mid)
        risk = (wing - credit) * 100

        if risk > max_risk or credit <= 0:
            return None

        ticker = chain.ticker
        legs = [
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=expiry, strike=atm, option_type=OptionType.PUT),
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=expiry, strike=atm, option_type=OptionType.CALL),
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=expiry, strike=long_put.strike, option_type=OptionType.PUT),
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=expiry, strike=long_call.strike, option_type=OptionType.CALL),
        ]

        return StrategyOrder(
            ticker=ticker, strategy_type=StrategyType.IRON_BUTTERFLY,
            direction=Direction.NEUTRAL, option_legs=legs,
            net_credit_debit=round(credit * 100, 2),
            max_risk=round(risk, 2),
            max_reward=round(credit * 100, 2),
        )

    def _build_credit_spread(
        self, chain, strategy_type, direction, target_dte,
        target_delta, max_risk,
    ) -> Optional[StrategyOrder]:
        """Put credit spread (bullish) or call credit spread (bearish)."""
        expiry = self._find_expiry(chain, target_dte)
        if not expiry:
            return None

        is_put = strategy_type == StrategyType.PUT_CREDIT_SPREAD
        opt_type = OptionType.PUT if is_put else OptionType.CALL

        short = chain.get_by_delta(expiry, target_delta, opt_type)
        if not short:
            return None

        strikes = chain.get_strikes(expiry)
        wing = self._find_wing_width(strikes, short.strike, max_risk, "put" if is_put else "call")
        long_strike = short.strike - wing if is_put else short.strike + wing
        long = self._nearest_strike(chain, expiry, long_strike, opt_type)
        if not long:
            return None

        if not self._check_liquidity(short) or not self._check_liquidity(long):
            return None

        credit = short.mid - long.mid
        width = abs(short.strike - long.strike)
        risk = (width - credit) * 100

        if risk > max_risk or credit <= 0:
            return None

        ticker = chain.ticker
        legs = [
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=expiry, strike=short.strike, option_type=opt_type),
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=expiry, strike=long.strike, option_type=opt_type),
        ]

        return StrategyOrder(
            ticker=ticker, strategy_type=strategy_type,
            direction=direction, option_legs=legs,
            net_credit_debit=round(credit * 100, 2),
            max_risk=round(risk, 2),
            max_reward=round(credit * 100, 2),
        )

    def _build_debit_spread(
        self, chain, strategy_type, direction, target_dte,
        target_delta, max_risk,
    ) -> Optional[StrategyOrder]:
        """Call debit spread (bullish) or put debit spread (bearish)."""
        expiry = self._find_expiry(chain, target_dte)
        if not expiry:
            return None

        is_call = strategy_type == StrategyType.CALL_DEBIT_SPREAD
        opt_type = OptionType.CALL if is_call else OptionType.PUT

        # Buy closer to ATM (higher delta)
        long = chain.get_by_delta(expiry, target_delta, opt_type)
        if not long:
            return None

        # Sell further OTM
        strikes = chain.get_strikes(expiry)
        wing = self._find_wing_width(strikes, long.strike, max_risk, "call" if is_call else "put")
        short_strike = long.strike + wing if is_call else long.strike - wing
        short = self._nearest_strike(chain, expiry, short_strike, opt_type)
        if not short:
            return None

        if not self._check_liquidity(long) or not self._check_liquidity(short):
            return None

        debit = long.mid - short.mid
        width = abs(long.strike - short.strike)
        risk = debit * 100
        reward = (width - debit) * 100

        if risk > max_risk or debit <= 0:
            return None

        ticker = chain.ticker
        legs = [
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=expiry, strike=long.strike, option_type=opt_type),
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=expiry, strike=short.strike, option_type=opt_type),
        ]

        return StrategyOrder(
            ticker=ticker, strategy_type=strategy_type,
            direction=direction, option_legs=legs,
            net_credit_debit=round(-debit * 100, 2),  # Negative = debit
            max_risk=round(risk, 2),
            max_reward=round(reward, 2),
            profit_target=0.50,
            stop_loss=0.40,  # 40% of debit paid
        )

    def _build_calendar(
        self, chain, strategy_type, direction, target_dte,
        target_delta, max_risk,
    ) -> Optional[StrategyOrder]:
        """Calendar spread: sell front, buy back month at same strike."""
        expiries = chain.get_expiries()
        if len(expiries) < 2:
            return None

        front_exp = self._find_expiry(chain, min(target_dte, 14))
        back_exp = self._find_expiry(chain, max(target_dte, 30))
        if not front_exp or not back_exp or front_exp == back_exp:
            return None

        atm = chain.get_atm_strike(front_exp)
        opt_type = OptionType.CALL  # Calls for neutral/bullish calendars

        front = chain.get_quote(front_exp, atm, opt_type)
        back = chain.get_quote(back_exp, atm, opt_type)
        if not front or not back:
            return None

        debit = back.mid - front.mid
        risk = debit * 100

        if risk > max_risk or debit <= 0:
            return None

        ticker = chain.ticker
        legs = [
            OptionLeg(side=OrderSide.SELL, quantity=1, ticker=ticker,
                      expiry=front_exp, strike=atm, option_type=opt_type),
            OptionLeg(side=OrderSide.BUY, quantity=1, ticker=ticker,
                      expiry=back_exp, strike=atm, option_type=opt_type),
        ]

        return StrategyOrder(
            ticker=ticker, strategy_type=StrategyType.CALENDAR_SPREAD,
            direction=Direction.NEUTRAL, option_legs=legs,
            net_credit_debit=round(-debit * 100, 2),
            max_risk=round(risk, 2),
            max_reward=round(risk * 0.5, 2),  # ~50% return typical
        )

    def _build_stock(
        self, chain, strategy_type, direction, target_dte,
        target_delta, max_risk,
    ) -> Optional[StrategyOrder]:
        """Stock scalp: simple long/short position."""
        price = chain.underlying_price
        if price <= 0:
            return None

        # Calculate shares from risk budget
        stop_pct = 0.01  # 1% stop loss
        stop_amount = price * stop_pct
        shares = int(max_risk / (stop_amount * 100) * 100)  # Round to 100s
        shares = max(min(shares, 200), 1)

        side = OrderSide.BUY if direction == Direction.BULLISH else OrderSide.SELL

        return StrategyOrder(
            ticker=chain.ticker,
            strategy_type=strategy_type,
            direction=direction,
            stock_leg=StockLeg(
                side=side, quantity=shares, ticker=chain.ticker,
            ),
            max_risk=round(shares * stop_amount, 2),
            max_reward=round(shares * stop_amount * 2, 2),
        )

    # ── Helpers ──

    def _find_expiry(self, chain: OptionsChain, target_dte: int) -> Optional[date]:
        """Find the expiry closest to target DTE."""
        expiries = chain.get_expiries()
        if not expiries:
            return None
        today = date.today()
        return min(
            expiries,
            key=lambda e: abs((e - today).days - target_dte),
        )

    def _find_wing_width(
        self, strikes: list, center: float, max_risk: float,
        side: str,
    ) -> float:
        """Calculate wing width to stay within risk budget."""
        # Target: max_risk = (width - credit) × 100
        # Start with $5 width and adjust
        sorted_strikes = sorted(strikes)
        widths = []
        for s in sorted_strikes:
            diff = abs(s - center)
            if 1 <= diff <= 20:
                widths.append(diff)
        if not widths:
            return 5.0
        # Pick width that gives ~$300-400 risk
        target_width = max_risk / 100 + 0.50  # Assume ~$0.50 credit
        return min(widths, key=lambda w: abs(w - target_width))

    def _nearest_strike(
        self, chain: OptionsChain, expiry: date,
        target_strike: float, opt_type: OptionType,
    ) -> Optional[OptionQuote]:
        """Find nearest available strike."""
        candidates = [
            q for q in chain.quotes
            if q.expiry == expiry and q.option_type == opt_type
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda q: abs(q.strike - target_strike))

    def _check_liquidity(self, quote: OptionQuote) -> bool:
        """Verify a quote meets liquidity requirements."""
        if quote.spread_pct > settings.max_bid_ask_spread_pct:
            return False
        if quote.open_interest < settings.min_open_interest:
            # Relaxed check for development (yfinance OI can be stale)
            if quote.volume < 10:
                return False
        return True
