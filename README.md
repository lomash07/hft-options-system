# HFT Options Trading System v3.0

**Options-primary multi-agent trading system for US equities (Top 20 mega-cap).**

Capital: $20K | Options 80% / Stock Scalps 20% | 5-10 trades/day

---

## Architecture Overview

```
L0  Session Scheduler    ─ Pre-Market → Open Strike → Midday → Close Strike → Post-Market
L1  Data Ingestion       ─ Broker Feed + Options Analytics + News + Feature Store + Regime
L2  Strategy Agents (6)  ─ Theta | Vol Arb | Unusual Flow | Momentum | Earnings | Mean Rev
L3  Consensus + Constructor ─ Weighted voting → Options strategy construction → Risk gate
L4  Execution            ─ Multi-leg combo orders → IB/Paper → Position tracking
L5  Monitoring           ─ Grafana + Telegram + Prometheus + Kill switch
L6  Feedback             ─ Backtesting + Auto-tuning (Phase 4)
```

## Quick Start (Development — $0/month)

### 1. Prerequisites

- Python 3.11+
- Docker & Docker Compose
- (Optional) IB Gateway for live/paper trading

### 2. Setup

```bash
# Clone & enter
cd hft_options_system

# Create environment
python -m venv venv
source venv/bin/activate   # Linux/Mac
# venv\Scripts\activate    # Windows

# Install dependencies
pip install -r requirements.txt

# Copy env template
cp .env.example .env
# Edit .env with your settings (Telegram token, etc.)

# Start infrastructure
docker-compose up -d

# Verify services
docker-compose ps
# Should show: redis, timescaledb, grafana, prometheus all running
```

### 3. Run

```bash
# Pre-market scan only (test that everything works)
python main.py --scan-only

# Full system (follows market hours automatically)
python main.py

# Override mode
python main.py --mode paper
```

### 4. Access Dashboards

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Metrics**: http://localhost:8000/metrics

---

## Universe (Top 20 by Market Cap)

```
AAPL  MSFT  NVDA  AMZN  GOOGL  META  TSLA  BRK-B  LLY  UNH
AVGO  JPM   V     XOM   MA     JNJ   PG    HD     COST  NFLX
```

---

## Agents

| # | Agent | Strategy | Active Regimes | Phase |
|---|-------|----------|---------------|-------|
| 1 | Theta Harvester | Sell premium when IV elevated | Low/High Vol, Choppy | 2 |
| 2 | Vol Arb | Trade IV vs realized vol mismatch | All | 2 |
| 3 | Directional Momentum | Breakout trades with debit spreads | Bull/Bear trend | 3 |
| 4 | Earnings Vol | IV crush around earnings | All (event-driven) | 3 |
| 5 | News Catalyst | React to material news | All (event-driven) | 4 |
| 6 | Mean Reversion | Fade overextended moves | Choppy, Low Vol | 3 |
| 7 | Unusual Flow | Follow institutional options flow | All | 2 |
| 8 | ML Ensemble | Multi-model prediction | Trending, Low Vol | 4 |

---

## Session Schedule (Eastern Time)

| Time | Phase | Active Agents |
|------|-------|---------------|
| 04:00 - 09:30 | Pre-Market Scout | Scanner only — builds watchlist |
| 09:30 - 11:00 | Open Strike (PEAK) | ALL agents — ~50% daily P&L here |
| 11:00 - 14:30 | Midday Harvest | Theta + Mean Rev + Vol Arb only |
| 14:30 - 16:00 | Close Strike | ALL agents re-activated |
| 16:00 - 18:00 | Post-Market | Reports, retraining, analysis |
| 18:00 - 04:00 | Closed | Sleep |

---

## Risk Management

| Level | Trigger | Action |
|-------|---------|--------|
| ⚠️ Warning | -$400 daily | Half position sizes |
| 🛑 Stop | -$600 daily | Block new trades |
| 🚨 Kill | -$800 daily | Close ALL positions |

**Per-trade**: Max risk $400 (2% of capital)
**Portfolio**: Max 5 concurrent positions, Net Δ ±300, Keep 30% buying power reserve
**PDT**: Tracks day trades if under $25K

---

## Build Phases

### Phase 1 (Week 1-3): Infrastructure + Data ✅
- Docker Compose stack (Redis, TimescaleDB, Grafana)
- Data feed (yfinance free / IB)
- Options Analytics Engine (IV surface, Greeks, IV rank)
- Feature Store (real-time cache)
- Market Regime Detector
- Pre-Market Scanner & Watchlist Builder

### Phase 2 (Week 4-7): Core Agents + Execution ✅
- Agent 1: Theta Harvester
- Agent 2: Vol Arb
- Agent 7: Unusual Flow
- Options Strategy Constructor
- Consensus Engine
- Risk Manager (all checks)
- Execution Engine (paper mode)
- Telegram Alerts

### Phase 3 (Week 8-10): Full Agent Suite ✅
- Agent 3: Directional Momentum
- Agent 4: Earnings Vol
- Agent 6: Mean Reversion
- Portfolio Manager (Greeks limits)
- Session Scheduler
- Daily reporting

### Phase 4 (Week 11-14): Intelligence — NOT YET BUILT
- Agent 5: News Catalyst (FinBERT)
- Agent 8: ML Ensemble (XGBoost + LSTM)
- Backtesting Engine
- Agent Weight Auto-Tuner
- ML Model Retraining Pipeline

---

## Costs by Phase

| Phase | Monthly Cost |
|-------|-------------|
| Dev & Paper (Phase 1-3) | **$0** (local + yfinance) |
| Paper on cloud | **$0** (Oracle Cloud free tier) |
| Live with $5K | **$35-70** (Alpaca + optional Polygon) |
| Full production $20K | **$100-255** (IB + Hetzner + data) |

---

## Telegram Commands

| Command | Action |
|---------|--------|
| `/status` | Current positions, P&L, regime |
| `/pnl` | Today's P&L breakdown |
| `/watchlist` | Today's watchlist with scores |
| `/kill` | Emergency: close all positions |
| `/positions` | List all open positions |
| `/circuit` | Circuit breaker status |

---

## Project Structure

```
hft_options_system/
├── main.py                    # Entry point & orchestrator
├── docker-compose.yml         # Infrastructure services
├── requirements.txt           # Python dependencies
├── .env.example               # Environment template
│
├── config/
│   ├── settings.py            # Central configuration (Pydantic)
│   ├── init_db.sql            # TimescaleDB schema
│   └── prometheus.yml         # Prometheus config
│
├── core/
│   ├── models.py              # Data models (Pydantic)
│   ├── event_bus.py           # Redis Streams message bus
│   ├── database.py            # TimescaleDB + SQLite access
│   └── session.py             # Session scheduler
│
├── data/
│   ├── broker_feed.py         # Market data (IB / yfinance)
│   ├── options_analytics.py   # IV surface, Greeks, Black-Scholes
│   ├── market_regime.py       # Regime detection (SPY/VIX)
│   └── feature_store.py       # Real-time feature cache
│
├── agents/
│   ├── base_agent.py          # Abstract base agent
│   ├── theta_harvester.py     # Agent 1: Premium selling
│   ├── vol_arb.py             # Agent 2: Vol arbitrage
│   ├── directional_momentum.py# Agent 3: Breakouts
│   ├── earnings_vol.py        # Agent 4: Earnings plays
│   ├── mean_reversion.py      # Agent 6: Fade extremes
│   └── unusual_flow.py        # Agent 7: Unusual options flow
│
├── strategy/
│   ├── options_constructor.py # Signal → executable multi-leg order
│   └── consensus.py           # Agent signal aggregation
│
├── risk/
│   └── __init__.py            # Risk Manager (circuit breakers, PDT, Greeks)
│
├── execution/
│   └── __init__.py            # Order routing + paper/live execution
│
├── scanner/
│   └── __init__.py            # Pre-market watchlist builder
│
└── monitoring/
    └── __init__.py            # Telegram, Prometheus, alerts
```

---

## Important Notes

1. **PAPER TRADE FIRST** — Run in paper mode for at least 2-4 weeks before risking real money.
2. **PDT Rule** — If account is under $25K, you're limited to 3 day trades per 5 business days. The system tracks this automatically.
3. **Never leave unattended** — Always have Telegram alerts enabled and the kill switch accessible. Options can move fast.
4. **Earnings dates** — The system blocks theta trades near earnings, but always verify manually.
5. **IV Rank** — Needs 20+ days of IV history to be accurate. First 2-3 weeks will have limited IV rank data.
