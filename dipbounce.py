# dip_bounce_bot (patched) - test_fixed.py
"""
Final patched version with:
- proper create_market_order usage (MarketOrderArgs + post_order)
- robust is_filled() checking `status` first, fallback to matched >= size
- processed_filled_order_ids to avoid repeated "Sell order ... filled" logs
- WS subscribes to token_ids, restart WS on refresh, queue clearing
- book worker uses task_done()
"""
import os
import sys
import time
import json
import signal
import logging
import threading
import traceback
import queue
from dataclasses import dataclass, field, asdict
from decimal import Decimal, InvalidOperation, getcontext
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone
from functools import wraps

import httpx
import websocket
from dotenv import load_dotenv

# Try to import py_clob_client types; if not available, code will fall back to dry-run
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs
except Exception:
    ClobClient = None
    OrderArgs = None
    OrderType = None
    MarketOrderArgs = None

# ---------- CONFIGURATION ----------
load_dotenv()

def parse_decimal(v, default=None):
    try:
        return Decimal(str(v))
    except Exception:
        return default

getcontext().prec = 18

@dataclass
class Config:
    PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")
    FUNDER_ADDRESS: str = os.getenv("FUNDER_ADDRESS", "")
    HOST: str = os.getenv("HOST", "https://clob.polymarket.com")
    CHAIN_ID: int = int(os.getenv("CHAIN_ID", "137"))

    ORDER_SIZE: Decimal = parse_decimal(os.getenv("ORDER_SIZE", "3.04"), Decimal("3.04"))
    BUY_ORDER_SIZE: Decimal = parse_decimal(os.getenv("BUY_ORDER_SIZE", "3.1"), Decimal("3.1"))

    CURRENCIES: List[str] = field(
        default_factory=lambda: [
            c.strip()
            for c in os.getenv("CURRENCIES", "btc").split(",")
            if c.strip()
        ]
    )

    # runtime params
    WINDOW_SECONDS: int = 120
    DIP_DEPTH: Decimal = Decimal("0.22")
    R_MAIN: Decimal = Decimal("0.04")
    R_FALLBACK: Decimal = Decimal("0.02")
    T_MAIN: int = 20
    T_FALLBACK: int = 40
    T_MAX: int = 50
    
    # LATE BUY ADJUSTMENT: If buy happens late (> LATE_BUY_THRESHOLD from market start),
    # add LATE_BUY_EXTENSION to T_MAX to give bot time to detect rebound
    LATE_BUY_THRESHOLD: int = 90  # seconds from market start to consider "late"
    LATE_BUY_EXTENSION: int = 30  # extra seconds added to T_MAX for late buys

    MAX_ORDER_SIZE: Decimal = parse_decimal(os.getenv("MAX_ORDER_SIZE", "10"), Decimal("10"))
    SAFE_MODE: bool = bool(int(os.getenv("SAFE_MODE", "1")))

    WS_URL: str = os.getenv(
        "WS_URL",
        "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    )
    GAMMA_EVENTS: str = os.getenv(
        "GAMMA_EVENTS",
        "https://gamma-api.polymarket.com/events"
    )

    DRY_RUN: bool = bool(int(os.getenv("DRY_RUN", "0")))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


cfg = Config()

WS_EVENTS_DUMP_PATH = os.getenv("WS_EVENTS_DUMP_PATH", os.path.join(os.getcwd(), "ws_events.ndjson"))
WS_EVENTS_DUMP_MAX = int(os.getenv("WS_EVENTS_DUMP_MAX", "200"))

# ---------- LOGGING ----------
numeric_level = getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO)
log_formatter = logging.Formatter("%(asctime)s | %(levelname)5s | %(name)s | %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(numeric_level)
console_handler.setFormatter(log_formatter)

log_file_path = os.getenv("BOT_LOG_FILE", os.path.join(os.getcwd(), "bot_activity.log"))
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
file_handler.setLevel(numeric_level)
file_handler.setFormatter(log_formatter)

log = logging.getLogger("dip-bounce")
log.setLevel(numeric_level)
log.handlers = []
log.addHandler(console_handler)
log.addHandler(file_handler)

# ---------- HELPERS ----------
def safe_decimal(x) -> Optional[Decimal]:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        return None

def now_ts() -> int:
    return int(time.time())

def get_15m_ts():
    n = datetime.now(timezone.utc)
    m = n.minute - n.minute % 15
    return int(n.replace(minute=m, second=0, microsecond=0).timestamp())

def backoff_retry(max_tries=5, base=0.5, factor=2.0, on_exception=(Exception,)):
    def deco(fn):
        @wraps(fn)
        def wrapper(*a, **kw):
            delay = base
            for attempt in range(1, max_tries + 1):
                try:
                    return fn(*a, **kw)
                except on_exception as e:
                    if attempt == max_tries:
                        raise
                    log.warning("Retry %s/%s for %s after exception: %s", attempt, max_tries, fn.__name__, e)
                    time.sleep(delay)
                    delay *= factor
        return wrapper
    return deco

# ---------- DATA CLASSES ----------
@dataclass
class AssetState:
    token_id: str
    start_time: int
    best_price: Optional[Decimal] = None
    buy_price: Optional[Decimal] = None
    buy_time: Optional[int] = None
    buy_order_id: Optional[str] = None
    bought_timestamp: Optional[int] = None  # Track when buy was first placed to prevent 2nd buy in same 15m window
    buy_filled: bool = False  # CRITICAL: Tracks actual fill confirmation (not just order placement)
    sell_order_id: Optional[str] = None
    sell_order_time: Optional[int] = None  # Track when sell order was placed
    done: bool = False
    buy_in_progress: bool = False
    bought: bool = False
    last_mid: Optional[Decimal] = None
    last_best_bid: Optional[Decimal] = None
    last_best_ask: Optional[Decimal] = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    # prevent concurrent post-buy evaluation for same asset
    processing_evaluation: bool = False
    # prevent concurrent order placement (buy or sell)
    processing_action: bool = False

    def to_dict(self):
        out = asdict(self)
        out.pop("lock", None)
        return out

# ---------- ORDER MANAGER ----------
class OrderManager:
    def __init__(self, client=None, dry_run=False):
        self.client = client
        self.dry_run = dry_run
        self._orders: Dict[str, Dict[str, Any]] = {}
        # track processed filled order ids to avoid repeated logging/processing
        self._processed_filled_order_ids = set()
        self._pf_lock = threading.Lock()

    def _log_action(self, action, *args):
        log.info("[ORDER] %s %s", action, " ".join(map(str, args)))

    def _fake_order_id(self, prefix):
        return f"dry_{prefix}_{int(time.time()*1000)}"

    def place_limit(self, token_id: str, price: Decimal, side: str, size: Decimal):
        if self.dry_run:
            oid = self._fake_order_id(side.lower())
            self._orders[oid] = dict(token_id=token_id, price=price, side=side, size=size, status="NEW")
            self._log_action("DRY_PLACE", token_id[:8], side, price, size, oid)
            return oid

        if not self.client:
            raise RuntimeError("No client configured for live trading")

        # Quantize for Polymarket precision: 
        # - Maker (BUY): max 4 decimals on size
        # - Taker (SELL): max 2 decimals on size
        quantized_size = size.quantize(Decimal('0.0001') if side == 'BUY' else Decimal('0.01'))
        quantized_price = price.quantize(Decimal('0.0001'))  # prices typically 4 decimals

        for attempt in range(1, 6):
            try:
                # build OrderArgs for limit order if available
                if OrderArgs:
                    args = OrderArgs(
                        token_id=token_id,
                        price=float(quantized_price),
                        size=float(quantized_size),
                        side=side
                    )
                    signed = self.client.create_order(args)
                    res = self.client.post_order(signed, OrderType.GTC)
                else:
                    # fallback: try a simpler create_post if client provides it
                    if hasattr(self.client, "create_and_post_order"):
                        res = self.client.create_and_post_order({
                            "token_id": token_id,
                            "price": float(quantized_price),
                            "size": float(quantized_size),
                            "side": side
                        })
                    else:
                        raise RuntimeError("No supported order creation method on client")
                oid = None
                if isinstance(res, dict):
                    for k in ("order_id", "orderId", "orderID", "id"):
                        if k in res and res[k]:
                            oid = res[k]
                            break
                    if not oid:
                        # try common fallback
                        oid = res.get("id") or res.get("orderID") or res.get("order_id")
                if not oid and isinstance(res, dict) and res.get("success", True):
                    oid = f"unknown_{res.get('status','ok')}_{int(time.time()*1000)}"
                    log.warning("place_limit: no explicit order id in response but success found; using fallback oid=%s", oid)
                if not oid:
                    log.warning("Order response: %s", res)
                    return None
                self._orders[oid] = dict(token_id=token_id, price=price, side=side, size=size, status="NEW", info=res)
                self._log_action("PLACE", token_id[:8], side, price, size, oid)
                log.debug("place_limit response for %s: %s", oid, res)
                return oid
            except Exception as e:
                error_str = str(e).lower()
                # Retry on network/timeout errors
                if any(x in error_str for x in ["request exception", "connection", "timeout", "temporary"]):
                    log.warning(f"Network error placing order (attempt {attempt}/5): {e}")
                    if attempt < 5:
                        time.sleep(1 + attempt)  # Exponential backoff: 2s, 3s, 4s, 5s
                        continue
                # For balance/allowance errors, don't retry
                elif "balance" in error_str or "allowance" in error_str:
                    log.error(f"Insufficient balance/allowance: {e}")
                    return None
                # For invalid signature, regenerate creds and retry
                elif "invalid signature" in error_str:
                    log.warning(f"Invalid signature (attempt {attempt}), regenerating creds")
                    try:
                        if hasattr(self.client, "create_or_derive_api_creds") and hasattr(self.client, "set_api_creds"):
                            creds = self.client.create_or_derive_api_creds()
                            self.client.set_api_creds(creds)
                    except Exception as cred_ex:
                        log.warning(f"Failed to refresh API creds: {cred_ex}")
                    if attempt < 5:
                        time.sleep(2)
                        continue
                # For other API errors, log and return None (don't retry)
                log.error(f"Failed to place order: {e}")
                return None
        log.error("Max retries exceeded placing order")
        return None

    def cancel(self, order_id: str):
        if not order_id:
            return
        if self.dry_run:
            if order_id in self._orders:
                self._orders[order_id]["status"] = "CANCELLED"
            self._log_action("DRY_CANCEL", order_id)
            return
        try:
            self.client.cancel(order_id)
            self._orders.get(order_id, {})["status"] = "CANCELLED"
            self._log_action("CANCEL", order_id)
            log.info(f"Cancelled order {order_id}")
        except Exception as e:
            log.warning(f"Cancel failed {order_id}: {e}")

    def get_order_status(self, order_id: str):
        if not order_id:
            return None
        if self.dry_run:
            return self._orders.get(order_id, {}).get("status", "UNKNOWN")
        try:
            info = self.client.get_order(order_id)
            status = info.get("status") or info.get("order_status") or "UNKNOWN"
            order = self._orders.get(order_id, {})
            order["info"] = info
            order["status"] = status
            self._orders[order_id] = order
            # BUGFIX: Log all fields in order info for debugging
            log.debug("Order %s info keys: %s", order_id[:8], list(info.keys()) if info else "empty")
            return info
        except Exception as e:
            log.warning(f"Failed to get order status {order_id}: {e}")
            return None

    def is_filled(self, order_id: str):
        """
        Robust is_filled:
        1) prefer explicit status field from get_order (FILLED/CLOSED)
        2) fallback to matched >= original size
        """
        if not order_id:
            return False
        if self.dry_run:
            s = self._orders.get(order_id, {}).get("status", "")
            return s.upper() in ("FILLED", "FILLED_PARTIAL", "FILLED_FULL", "CLOSED")
        info = self.get_order_status(order_id)
        if not info:
            # if we can't fetch status, be conservative: return False
            return False
        # prefer explicit status field
        order_status = (info.get("status") or info.get("order_status") or "").upper()
        if order_status in ("FILLED", "CLOSED"):
            return True
        # treat CANCELLED as not filled (unless matched >= size)
        if order_status in ("CANCELLED", "CANCELLED_BY_USER"):
            try:
                matched = Decimal(str(info.get("size_matched", "0")))
                original = Decimal(str(info.get("size") or info.get("original_size") or "0"))
                return original > 0 and matched >= original
            except Exception:
                return False
        # fallback numeric check: matched >= size
        try:
            matched = Decimal(str(info.get("size_matched", "0")))
            original = Decimal(str(info.get("size") or info.get("original_size") or "0"))
            if original > 0 and matched >= original:
                return True
        except Exception:
            pass
        return False

    def get_balance(self, token_id: str) -> Optional[Decimal]:
        """
        Get actual shares held by checking the filled order's size_matched.
        This is the most reliable way since size_matched represents actual executed shares.
        """
        if not token_id:
            return None
        if self.dry_run:
            return Decimal("100")  # Fake balance for dry-run
        
        try:
            # Use size_matched from order info - this is what actually got filled
            # Get most recent buy order for this token from internal tracking
            order_info = None
            for oid, order_data in self._orders.items():
                if order_data.get('token_id') == token_id and order_data.get('side') == 'BUY':
                    order_info = order_data.get('info') or self.client.get_order(oid)
                    if order_info:
                        break
            
            if order_info:
                size_matched = safe_decimal(order_info.get('size_matched') or order_info.get('sizeMatched'))
                if size_matched and size_matched > 0:
                    log.info("[BALANCE] ✓ Confirmed via size_matched: %s shares", size_matched)
                    return size_matched
        except Exception as e:
            log.warning("[BALANCE] Failed to get balance for %s: %s", token_id[:8], e)
        
        log.warning("[BALANCE] ✗ Could not determine balance for token_id=%s", token_id[:8])
        return None

    def get_filled_price(self, order_id: str) -> Optional[Decimal]:
        """
        Retrieve the actual fill price from the order.
        IMPORTANT: Polymarket fills can occur at better prices than requested!
        CRITICAL: API 'price' field = limit price (what you requested)
                  NOT the actual execution price (what you got)
        We must calculate: avg_price = matched_amount / size_matched
        """
        if not order_id:
            return None
        if self.dry_run:
            return self._orders.get(order_id, {}).get("price")
        try:
            info = self.get_order_status(order_id)
            if not info:
                log.warning("get_filled_price: No order info returned for %s", order_id[:8])
                return None
            
            # CRITICAL FIX: Calculate actual average fill price from matched_amount / size_matched
            # This is the ONLY reliable way to get execution price
            matched_amount = safe_decimal(info.get("matched_amount"))  # Total USDC spent/received
            size_matched = safe_decimal(info.get("size_matched"))      # Total shares filled
            
            if matched_amount and size_matched and size_matched > 0:
                avg_fill_price = matched_amount / size_matched
                log.info("[PRICE] ✓ ACTUAL fill price %s for %s (matched_amount=%s / size_matched=%s)", 
                        avg_fill_price, order_id[:8], matched_amount, size_matched)
                return avg_fill_price
            
            # FALLBACK: If matched_amount not available, check for outcome_prices field
            # Polymarket sometimes provides individual fill prices in outcome_prices array
            outcome_prices = info.get("outcome_prices")
            if outcome_prices and len(outcome_prices) > 0:
                # Average of all fill prices
                prices = [safe_decimal(p) for p in outcome_prices if safe_decimal(p)]
                if prices:
                    avg_price = sum(prices) / len(prices)
                    log.info("[PRICE] ✓ Calculated from outcome_prices: %s for %s", avg_price, order_id[:8])
                    return avg_price
            
            # FALLBACK: Try explicit fill price fields (if API provides them)
            possible_fields = [
                "average_price",            # Average fill price
                "avg_price",               # Short form
                "fill_price",              # Explicit fill price
                "filledPrice",             # Camel case
                "filled_price",            # Snake case
                "executionPrice",          # Execution price
                "execution_price",         # Snake case execution
                "matched_price",           # Matched price
                "matchedPrice",            # Camel case
            ]
            
            for field in possible_fields:
                if field in info and info[field]:
                    try:
                        p = safe_decimal(info[field])
                        if p is not None and p > 0:
                            log.info("[PRICE] Got fill price %s for %s from field '%s'", p, order_id[:8], field)
                            return p
                    except Exception as e:
                        log.debug("Could not parse %s from %s: %s", field, order_id[:8], e)
            
            # LAST RESORT: Use requested limit price (CONSERVATIVE but may miss better fills)
            # User reports: Bot shows 0.40 but actual fill was 0.42 per Polymarket profile
            # This means we're using the tentative limit price, not actual execution
            if self.is_filled(order_id):
                fallback_price = safe_decimal(info.get("price") or self._orders.get(order_id, {}).get("price"))
                log.warning("[PRICE] ⚠️ Using LIMIT price %s for %s (Polymarket API doesn't return matched_amount - actual fill may differ!)", 
                           fallback_price, order_id[:8])
                log.warning("[PRICE] API fields available: %s", list(info.keys())[:10])
                return fallback_price
                
            log.warning("[PRICE] Could not get fill price for %s: not filled yet", order_id[:8])
            return None
            
        except Exception as e:
            log.exception("Failed to get fill price for %s: %s", order_id[:8], e)
            return None

    def place_market_via_cancel_limit(self, token_id: str, side: str, size: Decimal, aggressive_price: Decimal):
        """
        Attempt:
         1) place an aggressive limit (place_limit) and wait short time for fill
         2) if not filled, cancel aggressive limit and POST a market order (create+post)
        Quantizes to Polymarket precision: BUY max 4 decimals, SELL max 2 decimals
        """
        # Quantize size for Polymarket API
        quantized_size = size.quantize(Decimal('0.0001') if side == 'BUY' else Decimal('0.01'))
        
        try:
            # first, try to place aggressive limit
            oid = self.place_limit(token_id, aggressive_price, side, quantized_size)
        except Exception as e:
            log.exception("place_limit aggressive failed, trying direct market: %s", e)
            oid = None

        if oid:
            for _ in range(5):
                time.sleep(0.2)
                if self.is_filled(oid):
                    return oid
            try:
                self.cancel(oid)
            except Exception:
                pass

        # if we reach here -> place direct market order via client
        if self.dry_run:
            return self._fake_order_id(side.lower())

        # Use py_clob_client flow: build MarketOrderArgs, create_market_order, post_order
        try:
            if MarketOrderArgs and hasattr(self.client, "create_market_order"):
                # MarketOrderArgs constructor may vary across versions, but official examples use token_id & size & side
                try:
                    order_args = MarketOrderArgs(
                        token_id=token_id,
                        size=float(quantized_size),
                        side=side
                    )
                except TypeError:
                    # try with amount instead of size (some versions use 'amount')
                    order_args = MarketOrderArgs(
                        token_id=token_id,
                        amount=float(quantized_size),
                        side=side
                    )
                signed = self.client.create_market_order(order_args)
                res = self.client.post_order(signed)
                mo_id = None
                if isinstance(res, dict):
                    mo_id = res.get("order_id") or res.get("id") or res.get("orderID")
                if not mo_id:
                    log.warning("place_market_via_cancel_limit: market order response without id: %s", res)
                    mo_id = f"market_{int(time.time()*1000)}"
                return mo_id
            else:
                # fallback: try client.create_and_post_order or similar convenience method
                if hasattr(self.client, "create_and_post_order"):
                    res = self.client.create_and_post_order({
                        "token_id": token_id,
                        "size": float(quantized_size),
                        "side": side
                    })
                    mo_id = None
                    if isinstance(res, dict):
                        mo_id = res.get("order_id") or res.get("id") or res.get("orderID")
                    if not mo_id:
                        mo_id = f"market_{int(time.time()*1000)}"
                    return mo_id
                # Last-resort: raise
                raise RuntimeError("Client does not support market order creation in expected ways")
        except Exception as e:
            log.exception("direct market order attempt failed: %s", e)
            raise

# ---------- WS CLIENT ----------
class WsClient:
    def __init__(self, url, on_message, on_open=None, on_close=None, on_error=None, ping_interval=60, ping_timeout=30):
        self.url = url
        self.on_message_cb = on_message
        self.on_open_cb = on_open
        self.on_close_cb = on_close
        self.on_error_cb = on_error
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.ws_app = None
        self._thread = None
        self._stop = threading.Event()

    def _run_forever(self, subscribe_payload):
        reconnect_delay = 2
        max_reconnect_delay = 30
        while not self._stop.is_set():
            try:
                self.ws_app = websocket.WebSocketApp(
                    self.url,
                    on_message=lambda ws, msg: self.on_message_cb(msg),
                    on_open=lambda ws: (log.info("WS on_open"), self.on_open_cb and self.on_open_cb(ws, subscribe_payload)),
                    on_close=lambda ws, code, reason: (log.info("WS closed %s %s", code, reason), self.on_close_cb and self.on_close_cb(ws)),
                    on_error=lambda ws, err: (log.exception("WS error %s", err), self.on_error_cb and self.on_error_cb(ws, err)),
                )
                self.ws_app.run_forever(ping_interval=self.ping_interval, ping_timeout=self.ping_timeout)
                # Connection closed normally, reset backoff
                reconnect_delay = 2
            except Exception as e:
                try:
                    error_str = str(e).lower()
                    if "ping/pong timed out" in error_str:
                        log.warning("WS ping/pong timeout, will reconnect")
                    elif hasattr(websocket, "_exceptions") and isinstance(e, websocket._exceptions.WebSocketConnectionClosedException):
                        log.info("WS connection closed by remote host")
                    else:
                        log.exception("Unexpected WS error: %s", e)
                except Exception:
                    log.exception("WS run_forever exception (fallback): %s", e)
            if not self._stop.is_set():
                log.info("WS disconnected — reconnecting in %ds", reconnect_delay)
                time.sleep(reconnect_delay)
                # Exponential backoff: 2s -> 4s -> 8s -> 16s -> 30s
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
        log.info("WS loop exiting")

    def start(self, subscribe_payload):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_forever, args=(subscribe_payload,), daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            if self.ws_app:
                self.ws_app.close()
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=3)

# ---------- MAIN CONTROLLER ----------
class DipBounceController:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.assets: Dict[str, AssetState] = {}
        self.lock = threading.Lock()
        self.running = threading.Event()
        self.running.set()
        self._refresh_thread = threading.Thread(target=self._refresh_assets_loop, daemon=True)
        self._ws_events_dumped = 0

        # queue + worker for book processing
        self.book_queue: "queue.Queue[tuple]" = queue.Queue(maxsize=10000)
        self._book_worker = threading.Thread(target=self._book_worker_loop, daemon=True)

        self.client = None
        if ClobClient and cfg.PRIVATE_KEY:
            try:
                self.client = ClobClient(cfg.HOST, key=cfg.PRIVATE_KEY, chain_id=cfg.CHAIN_ID, signature_type=2, funder=cfg.FUNDER_ADDRESS)
                creds = self.client.create_or_derive_api_creds()
                self.client.set_api_creds(creds)
                log.info("API credentials obtained and set")
            except Exception:
                log.exception("Failed to init ClobClient — proceeding with dry-run if configured")
                if not cfg.DRY_RUN:
                    pass
        self.order_mgr = OrderManager(client=self.client, dry_run=cfg.DRY_RUN)
        self.ws_client = WsClient(cfg.WS_URL, on_message=self._on_ws_message, on_open=self._on_ws_open)
        self._http = httpx.Client(timeout=10)
        self._monitor_thread = threading.Thread(target=self._background_monitor, daemon=True)

    def start(self):
        log.info("Starting DipBounceController")
        ts = get_15m_ts()
        ids = self._fetch_assets(ts)
        if not ids:
            log.warning("No token ids fetched for ts=%s; exiting", ts)
            return
        # build assets based on market -> token mapping
        with self.lock:
            for aid in ids:
                token_ids = self._fetch_market_tokens(aid)
                for token_id in token_ids:
                    self.assets[token_id] = AssetState(token_id=token_id, start_time=ts)
        log.info("Assets start_time set to market ts=%s", ts)

        # Subscribe WS on actual token_ids (asset ids)
        token_ids = list(self.assets.keys())
        subscribe_payload = {"type": "market", "operation": "subscribe", "assets_ids": token_ids}
        self.ws_client.start(subscribe_payload)
        log.info("WS subscribed MARKET for %d token_ids", len(token_ids))

        # start book worker BEFORE monitor
        self._book_worker.start()
        self._monitor_thread.start()
        self._refresh_thread.start()
        log.info("Started WS, book worker and monitor threads")

    def stop(self):
        log.info("Stopping controller — graceful shutdown begins")
        self.running.clear()
        try:
            self.ws_client.stop()
        except Exception:
            log.exception("Error stopping ws client")

        # send sentinel to book worker to exit
        try:
            for _ in range(5):
                try:
                    self.book_queue.put_nowait((None, None, None, None))
                    break
                except queue.Full:
                    time.sleep(0.1)
        except Exception:
            pass
        try:
            self._book_worker.join(timeout=1.0)
        except Exception:
            pass

        # gather order ids to cancel WITHOUT holding per-asset locks during network calls
        cancel_list = []
        with self.lock:
            assets_snapshot = list(self.assets.items())
        for aid, s in assets_snapshot:
            try:
                with s.lock:
                    if s.buy_order_id and not s.done:
                        cancel_list.append(s.buy_order_id)
                    if s.sell_order_id and not s.done:
                        cancel_list.append(s.sell_order_id)
            except Exception:
                log.exception("Error while snapshotting orders for %s during stop", aid[:8])

        for oid in cancel_list:
            try:
                self.order_mgr.cancel(oid)
            except Exception:
                log.exception("Failed to cancel order %s during stop", oid)

        log.info("Shutdown complete")

    # ---------- fetching assets ----------
    def _fetch_assets(self, ts: int) -> List[str]:
        out = []
        self._market_tokens = {}
        for c in self.cfg.CURRENCIES:
            slug = f"{c}-updown-15m-{ts}"
            url = f"{self.cfg.GAMMA_EVENTS}?slug={slug}"
            try:
                r = self._http.get(url)
                if r.status_code != 200:
                    log.warning("Gamma fetch non-200 %s: %s", url, r.status_code)
                    continue
                d = r.json()
                if not d:
                    continue
                for m in d[0].get("markets", []):
                    ids = json.loads(m.get("clobTokenIds", "[]"))
                    out.extend(ids)
                    tokens = m.get("tokens", [])
                    token_ids = [t["token_id"] for t in tokens if "token_id" in t]
                    for tid in token_ids:
                        self._market_tokens[tid] = token_ids
            except Exception:
                log.exception("Error fetching slug %s", slug)
        log.info("Fetched %d asset ids", len(out))
        return out

    def _fetch_market_tokens(self, market_id: str) -> list:
        return self._market_tokens.get(market_id, [market_id])

    # ---------- WS callbacks ----------
    def _on_ws_open(self, ws, subscribe_payload):
        try:
            ws.send(json.dumps(subscribe_payload))
            log.info("WS on_open (sent subscribe payload)")
        except Exception:
            log.exception("Failed to send WS subscribe payload")

    def _on_ws_message(self, raw_msg):
        try:
            if getattr(self, "_ws_events_dumped", 0) < WS_EVENTS_DUMP_MAX:
                try:
                    with open(WS_EVENTS_DUMP_PATH, "a", encoding="utf-8") as f:
                        f.write(raw_msg.replace("\n", " ") + "\n")
                    self._ws_events_dumped += 1
                    log.debug("Dumped WS event %d to %s", self._ws_events_dumped, WS_EVENTS_DUMP_PATH)
                except Exception:
                    log.debug("Failed to dump WS event")
        except Exception:
            pass

        try:
            data = json.loads(raw_msg)
        except Exception:
            log.debug("Non-JSON ws message: %s", raw_msg)
            return

        events = data if isinstance(data, list) else [data]

        for event in events:
            event_type = event.get("type") or event.get("event_type") or event.get("event")

            if event_type == "book":
                asset_id = event.get("asset_id") or event.get("token_id") or event.get("assetId")
                if not asset_id:
                    log.debug("Skipping book event: missing asset_id; keys=%s", list(event.keys()))
                    continue

                bids = event.get("buys") or event.get("bids") or []
                asks = event.get("sells") or event.get("asks") or []

                if not bids or not asks:
                    log.debug("Skipping book for %s: buys=%d asks=%d", asset_id if asset_id else "<unknown>", len(bids) if bids is not None else 0, len(asks) if asks is not None else 0)
                    continue

                def _level_price(level):
                    if level is None:
                        return None
                    if isinstance(level, dict):
                        return safe_decimal(level.get("price") or level.get("price_str") or level.get("p"))
                    if isinstance(level, (list, tuple)) and len(level) > 0:
                        return safe_decimal(level[0])
                    try:
                        return safe_decimal(level[0])
                    except Exception:
                        return None

                try:
                    bid_prices = [p for p in (_level_price(l) for l in bids) if p is not None]
                    ask_prices = [p for p in (_level_price(l) for l in asks) if p is not None]
                    best_bid = max(bid_prices) if bid_prices else None
                    best_ask = min(ask_prices) if ask_prices else None
                except Exception:
                    best_bid = _level_price(bids[0]) if bids else None
                    best_ask = _level_price(asks[0]) if asks else None

                if best_bid is None or best_ask is None:
                    log.debug("Skipping book for %s: best_bid=%s best_ask=%s first_bid=%s first_ask=%s", asset_id[:8] if asset_id else "<unknown>", best_bid, best_ask, bids[0] if bids else None, asks[0] if asks else None)
                    continue

                mid = (best_bid + best_ask) / 2

                s = self.assets.get(asset_id)
                if s:
                    with s.lock:
                        s.last_mid = mid
                        s.last_best_bid = best_bid
                        s.last_best_ask = best_ask

                # enqueue for worker (non-blocking); drop if queue full
                try:
                    self.book_queue.put_nowait((asset_id, best_bid, best_ask, mid))
                except queue.Full:
                    log.warning("Book queue overflow, dropping event for %s", asset_id[:8])

            elif event_type == "last_trade_price":
                asset_id = event.get("asset_id") or event.get("token_id")
                if not asset_id:
                    continue
                price = safe_decimal(event.get("price"))
                if price is not None:
                    log.info("[TRADE] %s price=%s", asset_id[:8], price)

    def _book_worker_loop(self):
        """Worker: take book events from queue and process using _handle_book."""
        while self.running.is_set():
            try:
                item = self.book_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                if not item:
                    continue
                asset_id, best_bid, best_ask, mid = item
                if asset_id is None:
                    # sentinel received -> exit
                    break
                try:
                    self._handle_book(asset_id, best_bid, best_ask, mid)
                except Exception:
                    log.exception("Error in book worker for %s", asset_id[:8] if asset_id else "<unk>")
            finally:
                try:
                    self.book_queue.task_done()
                except Exception:
                    pass
        log.info("Book worker exiting")

    # ---------- book handling + strategy ----------
    def _handle_book(self, asset_id: str, best_bid: Decimal, best_ask: Decimal, mid: Decimal):
        s = self.assets.get(asset_id)
        if not s:
            log.warning("No asset state found for token_id %s", asset_id)
            return

        action = None
        action_args = {}
        with s.lock:
            # CRITICAL: Block concurrent actions to prevent double-buys
            if s.processing_action:
                log.debug("Asset %s is already processing an action, skipping this book update", asset_id[:8])
                return
                
            if s.done:
                return
            elapsed = now_ts() - s.start_time

            if s.buy_price is None:
                # CRITICAL: Don't retry buy if already attempted (even if not filled yet)
                # Only allow new buy if: no pending order AND (buy_filled OR waiting for fill after order expired)
                if s.buy_order_id and not s.buy_filled:
                    # Buy order still pending (not filled, not cancelled yet) - wait for monitor thread
                    return
                if getattr(s, "bought", False) or s.buy_in_progress:
                    return

                # CRITICAL: Prevent multiple buys in same 15-minute market window
                # If a buy was already placed for this market, block new buys for 15 minutes
                if s.bought_timestamp is not None:
                    time_since_first_buy = now_ts() - s.bought_timestamp
                    if time_since_first_buy < 900:  # 15 minutes = 900 seconds
                        log.debug("Asset %s already bought at %s ago, blocking new buy (need to wait %.0fs more)", 
                                  asset_id[:8], time_since_first_buy, 900 - time_since_first_buy)
                        return
                    else:
                        # 15 minutes elapsed, reset for next cycle
                        log.info("Asset %s 15-min window expired, resetting for next buy cycle", asset_id[:8])
                        s.bought_timestamp = None
                        s.buy_filled = False
                        s.bought = False

                if s.best_price is None or mid > s.best_price:
                    s.best_price = mid
                    log.debug("Set best_price for %s -> %s (elapsed=%s)", asset_id[:8], s.best_price, elapsed)
                    return

                dip_threshold = self.cfg.DIP_DEPTH
                dip_hit = False
                try:
                    if dip_threshold <= 1:
                        if s.best_price and s.best_price > 0:
                            dip_hit = ((s.best_price - mid) / s.best_price) >= dip_threshold
                    else:
                        dip_hit = (s.best_price - mid) >= dip_threshold
                except Exception:
                    dip_hit = False

                log.debug("Dip check %s: best_price=%s mid=%s threshold=%s dip_hit=%s elapsed=%s window=%s", asset_id[:8], s.best_price, mid, dip_threshold, dip_hit, elapsed, self.cfg.WINDOW_SECONDS)

                if elapsed <= self.cfg.WINDOW_SECONDS and dip_hit:
                    s.buy_in_progress = True
                    tentative_buy_price = best_ask
                    log.info("[ACTION] DIP detected for %s: best_price=%s mid=%s diff=%s threshold=%s", asset_id[:8], s.best_price, mid, s.best_price - mid, dip_threshold)
                    action = "place_buy"
                    action_args = {"token_id": asset_id, "price": tentative_buy_price, "side": "BUY", "size": self.cfg.BUY_ORDER_SIZE}
                else:
                    return

            elif s.buy_filled and not s.sell_order_id:
                # CRITICAL: Only evaluate sell if BUY is ACTUALLY FILLED with correct price from monitor thread
                # VERIFY we still have shares before attempting sell
                try:
                    balance = self.order_mgr.get_balance(asset_id)
                    if not balance or balance <= 0:
                        log.warning("[SELL-ABORT] No balance for %s, marking as done to prevent sell attempts", asset_id[:8])
                        with s.lock:
                            s.sell_order_id = "NO_BALANCE"  # Prevent re-entry
                            s.done = True
                            s.bought = False
                            s.buy_filled = False
                        return
                    
                    if not action:  # Only evaluate sell if not already triggered aggressive buy
                        dt = now_ts() - (s.buy_time or now_ts())
                        target_main = s.buy_price + self.cfg.R_MAIN
                        target_fb = s.buy_price + self.cfg.R_FALLBACK

                        log.debug("[INFO] Evaluating sell for %s: dt=%s, mid=%s, targets=[%s,%s,%s], balance=%s", 
                                  asset_id[:8], dt, mid, target_main, target_fb, s.buy_price, balance)

                        if dt <= self.cfg.T_MAIN and mid >= target_main:
                            action = "place_sell"
                            action_args = {"token_id": asset_id, "price": best_bid, "side": "SELL", "size": self.cfg.ORDER_SIZE}
                            log.info("[SELL] Placing MAIN sell: dt=%s T_MAIN=%s, mid=%s target=%s", dt, self.cfg.T_MAIN, mid, target_main)
                        elif dt <= self.cfg.T_FALLBACK and mid >= target_fb:
                            action = "place_sell"
                            action_args = {"token_id": asset_id, "price": best_bid, "side": "SELL", "size": self.cfg.ORDER_SIZE}
                            log.info("[SELL] Placing FALLBACK sell: dt=%s T_FALLBACK=%s, mid=%s target=%s", dt, self.cfg.T_FALLBACK, mid, target_fb)
                        elif dt <= self.cfg.T_MAX and mid >= s.buy_price:
                            action = "place_sell"
                            action_args = {"token_id": asset_id, "price": best_bid, "side": "SELL", "size": self.cfg.ORDER_SIZE}
                            log.info("[SELL] Placing BREAKEVEN sell: dt=%s T_MAX=%s, mid=%s buy_price=%s", dt, self.cfg.T_MAX, mid, s.buy_price)
                        elif dt > self.cfg.T_MAX:
                            action = "force_sell"
                            action_args = {"token_id": asset_id, "side": "SELL", "size": self.cfg.ORDER_SIZE, "aggressive_price": best_bid}
                            log.warning("[SELL] T_MAX exceeded dt=%s, forcing market sell for %s", dt, asset_id[:8])
                        else:
                            log.debug("[SELL] No sell condition met yet: dt=%s mid=%s targets=[%s,%s], waiting...", dt, mid, target_main, target_fb)
                            return
                except Exception as e:
                    s.buy_in_progress = False
                    s.processing_action = False
                    log.exception("Error in buy/sell evaluation for %s: %s", asset_id[:8], e)
                    return

        # Perform network actions outside s.lock
        try:
            if action == "place_buy":
                # Mark that we're processing this action to prevent concurrent attempts
                with s.lock:
                    s.processing_action = True
                try:
                    order_id = self.order_mgr.place_limit(**action_args)
                    log.info("[ORDER] Placed BUY for %s at %s, order_id=%s", asset_id[:8], action_args["price"], order_id)
                except Exception as e:
                    log.exception("Failed to place buy limit for %s: %s", asset_id, e)
                    with s.lock:
                        s.buy_in_progress = False
                        s.processing_action = False
                    return
                
                # CRITICAL: Validate order_id was actually created before marking as bought
                if not order_id:
                    log.error("[ERROR] BUY order placement returned None for %s, aborting", asset_id[:8])
                    with s.lock:
                        s.buy_in_progress = False
                        s.processing_action = False
                    return
                
                with s.lock:
                    s.buy_price = action_args["price"]  # Tentative price; will be corrected by monitor thread on fill
                    s.buy_time = now_ts()
                    s.buy_order_id = order_id
                    s.buy_in_progress = True  # Still waiting for fill
                    s.buy_filled = False  # NOT filled yet - just placed
                    s.bought = False  # Don't mark bought until fill confirmed
                    s.bought_timestamp = now_ts()  # CRITICAL: Record when buy was first placed to prevent 2nd buy in 15m window
                    s.processing_action = False  # Release lock for other books
                    log.info("[STATE] Asset %s buy order placed %s (waiting for fill confirmation)", asset_id[:8], order_id)
                if not self.cfg.DRY_RUN:
                    try:
                        filled = self.order_mgr.is_filled(order_id)
                        log.info("[ORDER] Immediate is_filled check for %s -> %s", order_id, filled)
                        if filled:
                            log.info("[ORDER] Buy order %s for %s is already filled; evaluating sell immediately", order_id, asset_id[:8])
                            try:
                                self._evaluate_post_buy_and_maybe_sell(asset_id, s)
                            except Exception:
                                log.exception("Immediate post-buy evaluation failed for %s", asset_id[:8])
                    except Exception:
                        log.exception("is_filled check failed for %s", order_id)

            elif action == "aggressive_buy":
                with s.lock:
                    s.processing_action = True
                try:
                    midag = self.order_mgr.place_market_via_cancel_limit(**action_args)
                    # Validate market order was placed
                    if not midag:
                        log.error("[ERROR] Aggressive buy returned None for %s, skipping", asset_id[:8])
                        with s.lock:
                            s.processing_action = False
                        return
                    with s.lock:
                        s.buy_order_id = midag
                        s.buy_price = action_args["aggressive_price"]  # Tentative price; will be corrected by monitor thread
                        s.buy_time = now_ts()
                        s.buy_filled = False  # NOT filled yet - just placed
                        s.bought = False  # Don't mark bought until fill confirmed
                        s.bought_timestamp = now_ts()  # Record when buy was first placed
                        s.processing_action = False
                        log.info("Aggressive buy placed for token %s at %s, order_id %s (waiting for fill)", asset_id[:8], s.buy_price, midag)
                except Exception:
                    with s.lock:
                        s.processing_action = False
                    log.exception("Aggressive buy attempt failed")

            elif action == "place_sell":
                with s.lock:
                    s.processing_action = True
                try:
                    oid = self.order_mgr.place_limit(**action_args)
                    if not oid:
                        log.error("[ERROR] Sell order placement returned None for %s at %s", asset_id[:8], action_args.get("price"))
                        with s.lock:
                            s.processing_action = False
                        return
                    with s.lock:
                        s.sell_order_id = oid
                        s.sell_order_time = now_ts()  # BUGFIX: Track sell order placement time
                        s.done = True
                        s.bought = False
                        s.bought_timestamp = None  # Reset 15m window when sell is placed
                        s.processing_action = False
                    log.info("Placed sell %s -> %s; asset %s marked as not bought", asset_id[:8], action_args["price"], asset_id[:8])
                except Exception as e:
                    with s.lock:
                        s.processing_action = False
                    log.exception("Failed to place sell for %s at %s: %s", asset_id[:8], action_args.get("price"), e)

            elif action == "force_sell":
                try:
                    oid = self.order_mgr.place_market_via_cancel_limit(**action_args)
                    with s.lock:
                        s.sell_order_id = oid
                        s.done = True
                        s.bought = False
                        s.bought_timestamp = None  # Reset 15m window when market cycle completes
                    log.info("T_MAX expired, forced sell for %s; asset %s marked as not bought (15m window reset)", asset_id[:8], asset_id[:8])
                except Exception as e:
                    log.exception("Forced sell failed for %s: %s", asset_id[:8], e)
        except Exception as e:
            log.exception("Exception in _handle_book network phase for %s: %s", asset_id[:8], e)

    def _evaluate_post_buy_and_maybe_sell(self, asset_id: str, s: AssetState):
        # guard to prevent concurrent evaluations for same asset
        with s.lock:
            if s.processing_evaluation:
                log.debug("Post-buy eval already running for %s — skipping concurrent call", asset_id[:8])
                return
            # CRITICAL: If sell already attempted (including failures), don't retry
            if s.sell_order_id or s.buy_price is None:
                log.debug("[DEBUG] Skipping post-buy eval for %s: sell_order_id=%s, buy_price=%s", asset_id[:8], s.sell_order_id, s.buy_price)
                return
            s.processing_evaluation = True
        
        # VERIFY we have shares before attempting sell
        balance = self.order_mgr.get_balance(asset_id)
        if not balance or balance <= 0:
            log.warning("[POST-BUY] No balance for %s, aborting sell evaluation", asset_id[:8])
            with s.lock:
                s.sell_order_id = "NO_BALANCE"  # Prevent re-entry
                s.done = True
                s.bought = False
                s.buy_filled = False
                s.processing_evaluation = False
            return

        try:
            log.info("[DEBUG] Entering _evaluate_post_buy_and_maybe_sell for %s", asset_id[:8])
            with s.lock:
                mid = s.last_mid
                best_bid = s.last_best_bid
                buy_time_local = s.buy_time
                buy_price_local = s.buy_price
                market_start_time = s.start_time

            log.info("[DEBUG] Post-buy eval %s buy_price=%s buy_time=%s last_mid=%s last_best_bid=%s", asset_id[:8], buy_price_local, buy_time_local, mid, best_bid)

            if mid is None or best_bid is None:
                log.info("[DEBUG] No recent book snapshot for %s - cannot evaluate sell", asset_id[:8])
                return

            # STABILITY FIX: Only reject best_bid if it's stale (more than 2% below mid)
            # Don't cap it at buy_price - we need to sell even at a loss after T_MAX
            if mid and best_bid < (mid * Decimal("0.98")):
                log.warning("[STABILITY] best_bid (%.4f) stale relative to mid (%.4f), capping at mid-1%%", float(best_bid), float(mid))
                best_bid = mid * Decimal("0.99")

            dt = now_ts() - (buy_time_local or now_ts())
            
            # LATE BUY ADJUSTMENT: If buy happened late in market window, extend T_MAX
            # This prevents selling immediately after late buy without waiting for rebound
            time_from_market_start = (buy_time_local or now_ts()) - market_start_time
            t_max_adjusted = self.cfg.T_MAX
            if time_from_market_start > self.cfg.LATE_BUY_THRESHOLD:
                t_max_adjusted = self.cfg.T_MAX + self.cfg.LATE_BUY_EXTENSION
                log.info("[LATE-BUY] Buy at %ds from market start (>%ds threshold) - extending T_MAX from %ds to %ds", 
                        time_from_market_start, self.cfg.LATE_BUY_THRESHOLD, self.cfg.T_MAX, t_max_adjusted)
            
            target_main = buy_price_local + self.cfg.R_MAIN
            target_fb = buy_price_local + self.cfg.R_FALLBACK
            
            # Log detailed sell evaluation state
            log.info("[SELL-EVAL] %s dt=%ds mid=%.3f buy=%.3f best_bid=%.3f | MAIN:%.3f(ok=%s) FB:%.3f(ok=%s) BRK:%.3f(ok=%s) T_MAX:%ds", 
                     asset_id[:8], dt, mid, buy_price_local, best_bid, 
                     target_main, mid >= target_main,
                     target_fb, mid >= target_fb,
                     buy_price_local, mid >= buy_price_local,
                     t_max_adjusted)

            if dt <= self.cfg.T_MAIN and mid >= target_main:
                log.info("[DEBUG] Placing MAIN sell (post-buy) %s -> %s", asset_id[:8], best_bid)
                oid = self.order_mgr.place_limit(token_id=asset_id, price=best_bid, side="SELL", size=self.cfg.ORDER_SIZE)
                with s.lock:
                    s.sell_order_id = oid
                    s.sell_order_time = now_ts()  # BUGFIX: Track sell order placement time
                    s.done = True
                    s.bought = False
                log.info("Placed MAIN sell (post-buy) %s -> %s", asset_id[:8], best_bid)
                return

            if dt <= self.cfg.T_FALLBACK and mid >= target_fb:
                log.info("[DEBUG] Placing FALLBACK sell (post-buy) %s -> %s", asset_id[:8], best_bid)
                oid = self.order_mgr.place_limit(token_id=asset_id, price=best_bid, side="SELL", size=self.cfg.ORDER_SIZE)
                with s.lock:
                    s.sell_order_id = oid
                    s.sell_order_time = now_ts()  # BUGFIX: Track sell order placement time
                    s.done = True
                    s.bought = False
                log.info("Placed FALLBACK sell (post-buy) %s -> %s", asset_id[:8], best_bid)
                return

            if dt <= t_max_adjusted and mid >= buy_price_local:
                log.info("[DEBUG] Placing BREAKEVEN sell (post-buy) %s -> %s", asset_id[:8], best_bid)
                oid = self.order_mgr.place_limit(token_id=asset_id, price=best_bid, side="SELL", size=self.cfg.ORDER_SIZE)
                with s.lock:
                    s.sell_order_id = oid
                    s.sell_order_time = now_ts()  # BUGFIX: Track sell order placement time
                    s.done = True
                    s.bought = False
                log.info("Placed BREAKEVEN sell (post-buy) %s -> %s", asset_id[:8], best_bid)
                return

            if dt > t_max_adjusted:
                # CRITICAL: Check if we still have shares before attempting T_MAX sell
                # Sell may have already executed or balance gone
                balance = self.order_mgr.get_balance(asset_id)
                if not balance or balance <= 0:
                    log.warning("[T_MAX] No balance remaining for %s, marking as done without sell", asset_id[:8])
                    with s.lock:
                        s.sell_order_id = "NO_BALANCE"  # Prevent retry loop
                        s.done = True
                        s.bought = False
                        s.bought_timestamp = None
                    return
                
                # Use aggressive limit order (maker) instead of market order (taker) to avoid 1-1.5% fees
                # Price slightly below best_bid to ensure quick fill while staying as maker
                aggressive_sell_price = (best_bid or buy_price_local) * Decimal("0.99")  # 1% below to ensure fill
                log.info("[DEBUG] Placing T_MAX forced LIMIT sell (maker) for %s at %s (balance=%s)", asset_id[:8], aggressive_sell_price, balance)
                try:
                    oid = self.order_mgr.place_limit(token_id=asset_id, price=aggressive_sell_price, side="SELL", size=self.cfg.ORDER_SIZE)
                    with s.lock:
                        s.sell_order_id = oid
                        s.sell_order_time = now_ts()
                        s.done = True
                        s.bought = False
                        s.bought_timestamp = None
                    log.info("T_MAX expired, forced LIMIT sell (maker) for %s at %s", asset_id[:8], aggressive_sell_price)
                except Exception as e:
                    log.error("[T_MAX] Sell failed for %s: %s - marking as done to prevent retry", asset_id[:8], e)
                    with s.lock:
                        s.sell_order_id = "FAILED"  # Prevent infinite retry
                        s.done = True
                        s.bought = False
                        s.bought_timestamp = None
        except Exception as exc:
            log.exception("[FATAL] Exception in _evaluate_post_buy_and_maybe_sell for %s: %s", asset_id[:8], exc)
        finally:
            with s.lock:
                s.processing_evaluation = False

    # ---------- background monitor for order fills, stale orders ----------
    def _background_monitor(self):
        while self.running.is_set():
            try:
                with self.lock:
                    assets_items = list(self.assets.items())

                for aid, s in assets_items:
                    try:
                        with s.lock:
                            buy_oid = s.buy_order_id
                            sell_oid = s.sell_order_id
                            buy_time_local = s.buy_time

                        # If there is a buy order and no sell, check fill status (network call) OUTSIDE s.lock
                        # BUGFIX: Don't retry if sell already attempted (sell_oid includes "FAILED" or "NO_BALANCE")
                        if buy_oid and not sell_oid:
                            try:
                                filled = self.order_mgr.is_filled(buy_oid)
                            except Exception:
                                filled = False
                            if filled:
                                log.info("Buy order %s for %s FILLED on exchange", buy_oid, aid[:8])
                                # CRITICAL: Verify actual balance changed (real confirmation)
                                # Polymarket fills can appear "filled" but not actually settle
                                balance = self.order_mgr.get_balance(aid)
                                if not balance or balance <= 0:
                                    log.warning("[CRITICAL] Order reports FILLED but balance=0 for %s, treating as NOT FILLED", aid[:8])
                                    # Order is fake filled - don't mark buy_filled=True
                                    continue
                                
                                # ONLY NOW proceed - we have real shares
                                log.info("[CRITICAL] Balance confirmed for %s: %s shares actual", aid[:8], balance)
                                
                                # Fetch actual fill price from API BEFORE marking buy_filled=True
                                # This ensures sell logic uses correct price, not tentative placement price
                                actual_price = self.order_mgr.get_filled_price(buy_oid)
                                with s.lock:
                                    if actual_price:
                                        if actual_price != s.buy_price:
                                            log.warning("[CRITICAL] Buy price corrected for %s: actual=%s vs requested=%s", aid[:8], actual_price, s.buy_price)
                                            s.buy_price = actual_price
                                        else:
                                            log.info("Buy price confirmed: %s", actual_price)
                                    else:
                                        log.warning("Could not get actual fill price for %s, using tentative: %s", aid[:8], s.buy_price)
                                    # CRITICAL: Only mark buy_filled=True AFTER balance confirmed AND price updated
                                    # This signals to book handler that sell evaluation can now use correct price
                                    s.buy_filled = True
                                    s.buy_in_progress = False
                                    s.bought = True  # Now safe to mark as bought
                                try:
                                    self._evaluate_post_buy_and_maybe_sell(aid, s)
                                except Exception as exc:
                                    log.exception("[FATAL] Error during post-buy sell evaluation for %s: %s", aid[:8], exc)
                            else:
                                buy_age = now_ts() - (buy_time_local or now_ts())
                                if buy_age > 30 and not self.cfg.DRY_RUN:
                                    try:
                                        self.order_mgr.cancel(buy_oid)
                                        log.info("Cancelled stale buy order %s for %s after %ss", buy_oid, aid[:8], buy_age)
                                        with s.lock:
                                            if s.buy_order_id == buy_oid:
                                                s.buy_order_id = None
                                                s.buy_in_progress = False
                                    except Exception:
                                        log.warning("Failed to cancel buy order %s for %s", buy_oid, aid[:8])

                        # If buy is filled but no sell order after a long time, force a market sell
                        with s.lock:
                            buy_oid2 = s.buy_order_id
                            sell_oid2 = s.sell_order_id
                            buy_time_now = s.buy_time
                            # BUGFIX: Check if already done or processing evaluation to avoid race condition
                            if s.done or s.processing_evaluation:
                                continue_monitor = False
                            else:
                                continue_monitor = True
                        
                        if continue_monitor and buy_oid2 and not sell_oid2 and self.order_mgr.is_filled(buy_oid2):
                            time_since_buy = now_ts() - (buy_time_now or now_ts())
                            if time_since_buy > 120 and not self.cfg.DRY_RUN:  # BUGFIX: Increased from 60s to 120s to avoid race
                                log.warning("No sell order placed for %s after %ss since buy fill, forcing LIMIT sell (maker)", aid[:8], time_since_buy)
                                try:
                                    # Use aggressive limit order (maker) at 1c to avoid taker fees - essentially giving away position as maker
                                    oid = self.order_mgr.place_limit(token_id=aid, price=Decimal("0.01"), side="SELL", size=self.cfg.ORDER_SIZE)
                                    with s.lock:
                                        s.sell_order_id = oid
                                        s.done = True
                                        s.bought = False
                                    log.info("Forced LIMIT sell (maker) for %s after buy fill timeout at 0.01", aid[:8])
                                except Exception as exc:
                                    log.exception("[FATAL] Failed to force limit sell for %s: %s", aid[:8], exc)

                        # If there is a sell order, check fills and staleness
                        if sell_oid:
                            try:
                                sell_filled = False
                                try:
                                    sell_filled = self.order_mgr.is_filled(sell_oid)
                                except Exception:
                                    log.exception("is_filled() failed for sell_oid %s", sell_oid)

                                if sell_filled:
                                    # process filled only once using global processed set
                                    with self.order_mgr._pf_lock:
                                        already = sell_oid in self.order_mgr._processed_filled_order_ids
                                        if not already:
                                            self.order_mgr._processed_filled_order_ids.add(sell_oid)
                                            log.info("Sell order %s for %s filled", sell_oid, aid[:8])
                                            # CRITICAL FIX: DO NOT clear sell_order_id after fill
                                            # Keeping it set prevents re-entry into sell logic
                                            with s.lock:
                                                # Keep s.sell_order_id = sell_oid (DO NOT set to None)
                                                # This prevents infinite sell attempts
                                                s.done = True
                                                s.bought = False
                                                s.buy_filled = False  # Clear buy_filled to prevent re-evaluation
                                            log.info("[COMPLETE] Asset %s cycle complete - sold successfully", aid[:8])
                                        else:
                                            # already processed earlier; skip noisy log
                                            pass
                                else:
                                    with s.lock:
                                        # BUGFIX: Calculate sell_age from sell_order_time, not buy_time
                                        sell_age = now_ts() - (s.sell_order_time or now_ts())
                                    if sell_age > 120 and not self.cfg.DRY_RUN:
                                        try:
                                            self.order_mgr.cancel(sell_oid)
                                            log.info("Cancelled stale sell order %s for %s after %ss", sell_oid, aid[:8], sell_age)
                                            # Use aggressive limit order (maker) at 1c to avoid taker fees
                                            oid = self.order_mgr.place_limit(token_id=aid, price=Decimal("0.01"), side="SELL", size=self.cfg.ORDER_SIZE)
                                            with s.lock:
                                                s.sell_order_id = oid
                                                s.sell_order_time = now_ts()  # BUGFIX: Reset sell_order_time for new attempt
                                                s.done = True
                                                s.bought = False
                                            log.info("Forced LIMIT sell (maker) for %s after sell order timeout at 0.01", aid[:8])
                                        except Exception as exc:
                                            log.warning("[FATAL] Failed to cancel or force sell for %s: %s", aid[:8], exc)
                            except Exception as exc:
                                log.exception("Exception while handling sell_oid %s for %s: %s", sell_oid, aid[:8], exc)

                    except Exception as exc:
                        log.exception("[FATAL] Exception in monitor loop for asset %s: %s", aid[:8], exc)
                time.sleep(1.0)
            except Exception as exc:
                log.exception("[FATAL] Monitor loop exception: %s", exc)
        log.info("Monitor thread exiting")

    def _refresh_assets_loop(self):
        while self.running.is_set():
            try:
                now = now_ts()
                current_15 = get_15m_ts()
                next_boundary = current_15 + 15 * 60
                sleep_for = next_boundary - now
                if sleep_for > 0:
                    time.sleep(sleep_for + 1)
                if not self.running.is_set():
                    break
                ts = next_boundary
                log.info("Refreshing assets for next 15m ts=%s", ts)
                ids = self._fetch_assets(ts)
                if not ids:
                    log.warning("No token ids fetched for next ts=%s; skipping refresh", ts)
                    continue

                # Replace assets mapping
                with self.lock:
                    old_ids = list(self.assets.keys())
                    new_assets = {}
                    for aid in ids:
                        token_ids = self._fetch_market_tokens(aid)
                        for token_id in token_ids:
                            new_assets[token_id] = AssetState(token_id=token_id, start_time=ts)
                    self.assets = new_assets

                # Clear old queued book events (they belong to old market)
                try:
                    while not self.book_queue.empty():
                        try:
                            self.book_queue.get_nowait()
                            self.book_queue.task_done()
                        except Exception:
                            break
                except Exception:
                    pass

                # Subscribe WS to token_ids (full restart of WS client)
                try:
                    token_ids = list(self.assets.keys())
                    subscribe_payload = {"type": "market", "operation": "subscribe", "assets_ids": token_ids}

                    try:
                        self.ws_client.stop()
                    except Exception:
                        log.debug("WS stop during refresh failed (ignored)")

                    self.ws_client.start(subscribe_payload)
                    log.info("WS re-started and subscribed MARKET for %d token_ids (refresh)", len(token_ids))
                except Exception:
                    log.exception("WS restart & subscribe failed during refresh")
            except Exception:
                log.exception("Asset refresh loop exception")
                time.sleep(5)

# ---------- SIGNALS ----------
controller = None

def _signal_handler(signum, frame):
    log.info("Signal %s received — shutting down", signum)
    try:
        if controller:
            controller.stop()
    except Exception:
        log.exception("Error during controller.stop()")
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------- MAIN ----------
def main():
    global controller
    controller = DipBounceController(cfg)
    try:
        controller.start()
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt caught — stopping")
        controller.stop()
    except Exception:
        log.exception("Unhandled exception in main")
        try:
            controller.stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
