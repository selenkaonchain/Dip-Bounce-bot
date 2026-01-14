

# Dip Bounce Bot (Polymarket CLOB)

A **fully automated dip-and-rebound trading bot** for **Polymarket CLOB** markets.
The bot monitors real-time order books via WebSocket, detects short-term price dips, places buy orders, and exits positions based on configurable rebound targets and time-based risk controls.

This repository contains a **patched and hardened version** with robust fill detection, improved sell logic, and race-condition protection.

---

## ⚠️ Disclaimer

This software is provided **for educational and experimental purposes only**.
Trading on prediction markets carries significant risk. You are fully responsible for any losses incurred. Use **DRY_RUN mode** before deploying with real funds.

---

## 🚀 Features

* 📉 **Dip Detection Strategy**

  * Tracks local highs within a configurable window
  * Buys when price drops beyond a defined dip threshold

* 📈 **Smart Exit Logic**

  * Main profit target
  * Fallback profit target
  * Breakeven exit
  * Forced exit after max hold time

* 🔄 **15-Minute Market Auto-Rotation**

  * Automatically refreshes markets every Polymarket 15-minute cycle
  * Prevents duplicate buys in the same window

* 🔐 **Robust Order Handling**

  * Accurate fill detection using `status` and `size_matched`
  * Correct average fill price calculation
  * Prevents double-processing of filled orders

* 🌐 **WebSocket Order Book Streaming**

  * Automatic reconnect with exponential backoff
  * Queue-based processing to avoid race conditions

* 🧪 **Dry-Run Mode**

  * Simulate trades without touching real funds

---

## 🧠 Strategy Overview

1. **Initialization**

   * Fetches Polymarket 15-minute up/down markets via Gamma API
   * Subscribes to order book updates for all tokens

2. **Dip Detection**

   * Tracks highest mid-price seen
   * Detects dips using percentage or absolute depth

3. **Buy Execution**

   * Places limit buy at best ask
   * Confirms fill via actual on-chain balance change
   * Corrects buy price using executed average fill price

4. **Sell Evaluation**

   * MAIN target (`R_MAIN`)
   * FALLBACK target (`R_FALLBACK`)
   * BREAKEVEN
   * Forced sell at `T_MAX` (maker-friendly aggressive limit)

5. **Cleanup**

   * Cancels stale orders
   * Prevents repeated sell attempts
   * Logs completed trade cycles

---

## 📦 Requirements

* Python **3.9+**
* Polymarket CLOB access
* Dependencies:

  ```bash
  pip install httpx websocket-client python-dotenv py-clob-client
  ```

---

## ⚙️ Environment Variables

Create a `.env` file in the project root:

```env
PRIVATE_KEY=0xYOUR_PRIVATE_KEY
FUNDER_ADDRESS=0xYOUR_WALLET_ADDRESS
HOST=https://clob.polymarket.com
CHAIN_ID=137

ORDER_SIZE=3.04
BUY_ORDER_SIZE=3.1
MAX_ORDER_SIZE=10

CURRENCIES=btc,eth
SAFE_MODE=1
DRY_RUN=1

LOG_LEVEL=INFO
```

### Important Flags

| Variable     | Description                       |
| ------------ | --------------------------------- |
| `DRY_RUN`    | Set to `1` to simulate trades     |
| `SAFE_MODE`  | Prevents oversized orders         |
| `CURRENCIES` | Assets to trade (comma-separated) |

---

## ⏱️ Strategy Parameters (Defaults)

| Parameter        | Description                      |
| ---------------- | -------------------------------- |
| `WINDOW_SECONDS` | Dip detection window             |
| `DIP_DEPTH`      | Required dip percentage          |
| `R_MAIN`         | Main profit target               |
| `R_FALLBACK`     | Backup profit target             |
| `T_MAIN`         | Time window for main target      |
| `T_FALLBACK`     | Time window for fallback         |
| `T_MAX`          | Max hold time before forced exit |

Late buys automatically extend `T_MAX` to avoid premature exits.

---

## ▶️ Running the Bot

```bash
python test.py
```

The bot runs continuously and handles:

* WebSocket reconnects
* Market refreshes
* Graceful shutdown on `CTRL+C`

---

## 🧾 Logs & Debugging

* **Console logs** for real-time monitoring
* **`bot_activity.log`** for persistent logs
* **`ws_events.ndjson`** (optional) for raw WebSocket event dumps

---

## 🛑 Graceful Shutdown

The bot:

* Cancels open orders
* Stops WebSocket connections
* Safely exits on `SIGINT` or `SIGTERM`

---

## 🧩 File Reference

Main trading logic is implemented in the uploaded script .

---

## ✅ Recommended Workflow

1. Run with `DRY_RUN=1`
2. Observe logs and WS dumps
3. Tune strategy parameters
4. Enable live trading cautiously
5. Monitor balance and order fills closely

---
