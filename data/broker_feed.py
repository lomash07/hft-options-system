"""
Market data feed abstraction.

Supports:
  - Interactive Brokers (ib_insync) for live/paper trading
  - Yahoo Finance (yfinance) for free development data
  - Tradier sandbox for free real-time options data

The system auto-selects based on available connections.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional

import structlog
import numpy as np
import pandas as pd

from core.models import (
    OptionQuote, OptionType, OptionsChain, PriceBar,
)
from config.settings import settings, ALL_TICKERS

logger = structlog.get_logger()


class DataFeed(ABC):
    """Abstract base class for market data feeds."""

    @abstractmethod
    async def connect(self) -> bool: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def get_price(self, ticker: str) -> Optional[float]: ...

    @abstractmethod
    async def get_price_bar(self, ticker: str) -> Optional[PriceBar]: ...

    @abstractmethod
    async def get_options_chain(self, ticker: str) -> Optional[OptionsChain]: ...

    @abstractmethod
    async def get_historical_bars(
        self, ticker: str, days: int = 60
    ) -> pd.DataFrame: ...

    @abstractmethod
    async def subscribe_bars(
        self, tickers: List[str], callback: Callable
    ) -> None: ...


# ═══════════════════════════════════════════════
# Yahoo Finance Feed (Free — Development)
# ═══════════════════════════════════════════════

class YFinanceFeed(DataFeed):
    """
    Free data feed using yfinance.
    Suitable for development, backtesting, and pre-market scanning.
    Not suitable for live execution (slight delays, rate limits).
    """

    def __init__(self):
        self._connected = False
        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = 5  # seconds

    async def connect(self) -> bool:
        try:
            import yfinance as yf
            self._yf = yf
            # Test connection
            test = yf.Ticker("AAPL")
            info = test.fast_info
            self._connected = True
            logger.info("feed.yfinance.connected")
            return True
        except Exception as e:
            logger.error("feed.yfinance.connection_failed", error=str(e))
            return False

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("feed.yfinance.disconnected")

    async def get_price(self, ticker: str) -> Optional[float]:
        """Get current price (may be delayed)."""
        try:
            t = self._yf.Ticker(ticker)
            info = t.fast_info
            return float(info.get("lastPrice", info.get("previousClose", 0)))
        except Exception as e:
            logger.error("feed.get_price.error", ticker=ticker, error=str(e))
            return None

    async def get_price_bar(self, ticker: str) -> Optional[PriceBar]:
        """Get latest 1-min bar."""
        try:
            t = self._yf.Ticker(ticker)
            df = t.history(period="1d", interval="1m")
            if df.empty:
                return None
            last = df.iloc[-1]
            return PriceBar(
                time=df.index[-1].to_pydatetime(),
                ticker=ticker,
                open=float(last["Open"]),
                high=float(last["High"]),
                low=float(last["Low"]),
                close=float(last["Close"]),
                volume=int(last["Volume"]),
            )
        except Exception as e:
            logger.error("feed.get_bar.error", ticker=ticker, error=str(e))
            return None

    async def get_options_chain(self, ticker: str) -> Optional[OptionsChain]:
        """Fetch full options chain from Yahoo Finance (free)."""
        try:
            t = self._yf.Ticker(ticker)
            price = float(t.fast_info.get(
                "lastPrice", t.fast_info.get("previousClose", 0)
            ))

            expiries = t.options  # list of expiry date strings
            if not expiries:
                return None

            quotes = []
            # Fetch first 4 expiries to avoid rate limits
            for exp_str in expiries[:4]:
                try:
                    chain = t.option_chain(exp_str)
                    exp_date = date.fromisoformat(exp_str)

                    # Calls
                    for _, row in chain.calls.iterrows():
                        quotes.append(OptionQuote(
                            ticker=ticker,
                            expiry=exp_date,
                            strike=float(row.get("strike", 0)),
                            option_type=OptionType.CALL,
                            bid=float(row.get("bid", 0)),
                            ask=float(row.get("ask", 0)),
                            last=float(row.get("lastPrice", 0)),
                            volume=int(row.get("volume", 0) or 0),
                            open_interest=int(
                                row.get("openInterest", 0) or 0
                            ),
                            implied_vol=float(
                                row.get("impliedVolatility", 0) or 0
                            ),
                        ))

                    # Puts
                    for _, row in chain.puts.iterrows():
                        quotes.append(OptionQuote(
                            ticker=ticker,
                            expiry=exp_date,
                            strike=float(row.get("strike", 0)),
                            option_type=OptionType.PUT,
                            bid=float(row.get("bid", 0)),
                            ask=float(row.get("ask", 0)),
                            last=float(row.get("lastPrice", 0)),
                            volume=int(row.get("volume", 0) or 0),
                            open_interest=int(
                                row.get("openInterest", 0) or 0
                            ),
                            implied_vol=float(
                                row.get("impliedVolatility", 0) or 0
                            ),
                        ))

                except Exception as e:
                    logger.warning(
                        "feed.chain.expiry_error",
                        ticker=ticker, expiry=exp_str, error=str(e),
                    )
                    continue

            return OptionsChain(
                ticker=ticker,
                timestamp=datetime.utcnow(),
                underlying_price=price,
                quotes=quotes,
            )

        except Exception as e:
            logger.error(
                "feed.get_chain.error", ticker=ticker, error=str(e)
            )
            return None

    async def get_historical_bars(
        self, ticker: str, days: int = 60
    ) -> pd.DataFrame:
        """Get historical daily OHLCV."""
        try:
            t = self._yf.Ticker(ticker)
            df = t.history(period=f"{days}d")
            return df
        except Exception as e:
            logger.error(
                "feed.history.error", ticker=ticker, error=str(e)
            )
            return pd.DataFrame()

    async def get_earnings_dates(self, ticker: str) -> List[Dict]:
        """Get upcoming earnings dates."""
        try:
            t = self._yf.Ticker(ticker)
            cal = t.calendar
            if cal is not None and not cal.empty:
                return [{"date": d, "ticker": ticker} for d in cal.index]
            return []
        except Exception:
            return []

    async def subscribe_bars(
        self, tickers: List[str], callback: Callable
    ) -> None:
        """
        Simulate streaming by polling every 60 seconds.
        For development only — IB provides real streaming.
        """
        logger.info(
            "feed.yfinance.polling_mode",
            tickers=len(tickers), interval="60s",
        )
        while True:
            for ticker in tickers:
                try:
                    bar = await self.get_price_bar(ticker)
                    if bar:
                        await callback(bar)
                except Exception as e:
                    logger.error(
                        "feed.poll.error", ticker=ticker, error=str(e)
                    )
            await asyncio.sleep(60)


# ═══════════════════════════════════════════════
# Interactive Brokers Feed (Production)
# ═══════════════════════════════════════════════

class IBFeed(DataFeed):
    """
    Production feed using Interactive Brokers TWS API.
    Requires IB Gateway or TWS running.
    """

    def __init__(self):
        self._ib = None
        self._connected = False
        self._subscriptions: Dict[str, any] = {}

    async def connect(self) -> bool:
        try:
            from ib_insync import IB, util
            util.startLoop()  # Required for async

            self._ib = IB()
            await self._ib.connectAsync(
                host=settings.ib_host,
                port=settings.ib_port,
                clientId=settings.ib_client_id,
            )
            self._connected = True
            logger.info(
                "feed.ib.connected",
                host=settings.ib_host, port=settings.ib_port,
            )
            return True

        except Exception as e:
            logger.error("feed.ib.connection_failed", error=str(e))
            logger.info(
                "feed.ib.hint",
                msg="Ensure IB Gateway/TWS is running. "
                    "Falling back to yfinance."
            )
            return False

    async def disconnect(self) -> None:
        if self._ib and self._connected:
            self._ib.disconnect()
            self._connected = False
            logger.info("feed.ib.disconnected")

    async def get_price(self, ticker: str) -> Optional[float]:
        from ib_insync import Stock
        contract = Stock(ticker, "SMART", "USD")
        self._ib.qualifyContracts(contract)
        ticker_data = self._ib.reqMktData(contract, "", False, False)
        await asyncio.sleep(1)  # Wait for data
        price = ticker_data.marketPrice()
        self._ib.cancelMktData(contract)
        return float(price) if price and not np.isnan(price) else None

    async def get_price_bar(self, ticker: str) -> Optional[PriceBar]:
        from ib_insync import Stock
        contract = Stock(ticker, "SMART", "USD")
        self._ib.qualifyContracts(contract)
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="60 S",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=True,
        )
        if not bars:
            return None
        b = bars[-1]
        return PriceBar(
            time=b.date, ticker=ticker,
            open=b.open, high=b.high, low=b.low, close=b.close,
            volume=b.volume, vwap=b.average,
        )

    async def get_options_chain(self, ticker: str) -> Optional[OptionsChain]:
        from ib_insync import Stock, Option
        stock = Stock(ticker, "SMART", "USD")
        self._ib.qualifyContracts(stock)

        # Get underlying price
        ticker_data = self._ib.reqMktData(stock, "", False, False)
        await asyncio.sleep(1)
        price = ticker_data.marketPrice()
        self._ib.cancelMktData(stock)

        if not price or np.isnan(price):
            return None

        # Get option chains
        chains = self._ib.reqSecDefOptParams(
            stock.symbol, "", stock.secType, stock.conId
        )
        if not chains:
            return None

        chain = chains[0]  # Use first exchange
        quotes = []

        # Filter to nearby strikes and first 4 expiries
        all_expiries = sorted(chain.expirations)[:4]
        nearby_strikes = [
            s for s in chain.strikes
            if abs(s - price) / price < 0.15  # ±15% from current price
        ]

        for exp in all_expiries:
            for strike in nearby_strikes:
                for right in ["C", "P"]:
                    opt = Option(
                        ticker, exp, strike, right, "SMART"
                    )
                    try:
                        self._ib.qualifyContracts(opt)
                        md = self._ib.reqMktData(opt, "100", False, False)
                        await asyncio.sleep(0.1)

                        quotes.append(OptionQuote(
                            ticker=ticker,
                            expiry=date.fromisoformat(exp),
                            strike=strike,
                            option_type=(
                                OptionType.CALL if right == "C"
                                else OptionType.PUT
                            ),
                            bid=float(md.bid or 0),
                            ask=float(md.ask or 0),
                            last=float(md.last or 0),
                            volume=int(md.volume or 0),
                            open_interest=0,
                            implied_vol=float(
                                md.modelGreeks.impliedVol
                                if md.modelGreeks else 0
                            ),
                            delta=float(
                                md.modelGreeks.delta
                                if md.modelGreeks else 0
                            ),
                            gamma=float(
                                md.modelGreeks.gamma
                                if md.modelGreeks else 0
                            ),
                            theta=float(
                                md.modelGreeks.theta
                                if md.modelGreeks else 0
                            ),
                            vega=float(
                                md.modelGreeks.vega
                                if md.modelGreeks else 0
                            ),
                        ))
                        self._ib.cancelMktData(opt)

                    except Exception:
                        continue

        return OptionsChain(
            ticker=ticker,
            timestamp=datetime.utcnow(),
            underlying_price=float(price),
            quotes=quotes,
        )

    async def get_historical_bars(
        self, ticker: str, days: int = 60
    ) -> pd.DataFrame:
        from ib_insync import Stock
        contract = Stock(ticker, "SMART", "USD")
        self._ib.qualifyContracts(contract)
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=f"{days} D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
        )
        df = pd.DataFrame(
            [{"Date": b.date, "Open": b.open, "High": b.high,
              "Low": b.low, "Close": b.close, "Volume": b.volume}
             for b in bars]
        )
        if not df.empty:
            df.set_index("Date", inplace=True)
        return df

    async def subscribe_bars(
        self, tickers: List[str], callback: Callable
    ) -> None:
        from ib_insync import Stock
        for ticker in tickers:
            contract = Stock(ticker, "SMART", "USD")
            self._ib.qualifyContracts(contract)
            bars = self._ib.reqRealTimeBars(
                contract, 5, "TRADES", False
            )
            bars.updateEvent += lambda bars, t=ticker: asyncio.ensure_future(
                callback(PriceBar(
                    time=bars[-1].time, ticker=t,
                    open=bars[-1].open_, high=bars[-1].high,
                    low=bars[-1].low, close=bars[-1].close,
                    volume=bars[-1].volume,
                ))
            )
            self._subscriptions[ticker] = bars
        logger.info("feed.ib.subscribed", tickers=len(tickers))
        # Keep running
        while True:
            await asyncio.sleep(1)


# ═══════════════════════════════════════════════
# Feed Factory
# ═══════════════════════════════════════════════

async def create_feed() -> DataFeed:
    """
    Create the best available data feed.
    Tries IB first, falls back to yfinance.
    """
    # Try IB first
    ib = IBFeed()
    if await ib.connect():
        return ib

    # Fallback to yfinance
    logger.info("feed.fallback", to="yfinance")
    yf = YFinanceFeed()
    if await yf.connect():
        return yf

    raise RuntimeError("No data feed available")
