#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTC & PAXG ENGINE V14.6 — FIXED-STOP EDITION
MTF SMC + REGIME + PREDICTION + CLOSE-CONFIRM + LOW-DATA + FIXED-STOP
"""
from __future__ import annotations
import asyncio, hashlib, hmac, json, logging, math, os, signal, sqlite3, statistics
import threading, time, urllib.parse, urllib.request
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import websocket
except Exception:
    websocket = None
try:
    import requests
except Exception:
    requests = None

APP_NAME = "BTC_PAXG_ENGINE_V14_6"
VERSION = "V14.6"
MODE = os.getenv("MODE", "PAPER").upper()
MARKET_TYPE = os.getenv("MARKET_TYPE", "FUTURES").upper()
SYMBOLS = [x.strip().upper() for x in os.getenv("SYMBOLS", "BTCUSDT,PAXGUSDT").split(",") if x.strip()]
TIMEFRAMES = ["5m", "15m", "30m", "1h", "4h"]
EXECUTION_TF = os.getenv("EXECUTION_TF", "15m")
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "100"))
POI_LOOKBACK = int(os.getenv("POI_LOOKBACK", "200"))
SIZING_MODE = os.getenv("SIZING_MODE", "FIXED_NOTIONAL").upper()
POSITION_SIZE_PCT = float(os.getenv("POSITION_SIZE_PCT", "20.0"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
DAILY_LOSS_BREAKER_ENABLED = os.getenv("DAILY_LOSS_BREAKER_ENABLED", "false").lower() == "true"
MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", "2.0"))
CONSECUTIVE_LOSS_BREAKER_ENABLED = os.getenv("CONSECUTIVE_LOSS_BREAKER_ENABLED", "false").lower() == "true"
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
MIN_EQUITY_TO_TRADE = float(os.getenv("MIN_EQUITY_TO_TRADE", "0.01"))
ALLOW_LONG = os.getenv("ALLOW_LONG", "true").lower() == "true"
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "true").lower() == "true"
MIN_ENTRY_PROBABILITY = float(os.getenv("MIN_ENTRY_PROBABILITY", "50.0"))
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "2"))
MAX_ONE_PER_SYMBOL = os.getenv("MAX_ONE_PER_SYMBOL", "true").lower() == "true"
COOLDOWN_CANDLES = int(os.getenv("COOLDOWN_CANDLES", "2"))
DRAWDOWN_ADAPTIVE_ENABLED = os.getenv("DRAWDOWN_ADAPTIVE_ENABLED", "false").lower() == "true"
KELLY_ENABLED = os.getenv("KELLY_ENABLED", "false").lower() == "true"
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
KELLY_MIN_TRADES = int(os.getenv("KELLY_MIN_TRADES", "30"))
CORRELATION_FILTER_ENABLED = os.getenv("CORRELATION_FILTER_ENABLED", "true").lower() == "true"
CORRELATION_THRESHOLD = float(os.getenv("CORRELATION_THRESHOLD", "0.75"))
CORRELATION_LOOKBACK = int(os.getenv("CORRELATION_LOOKBACK", "50"))

# ✅ V14.6: Tighter, fixed stop and closer target
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "1.0"))
TP_R_MULT = float(os.getenv("TP_R_MULT", "1.5"))
BE_R_MULT = float(os.getenv("BE_R_MULT", "1.0"))
TRAIL_ATR_MULT = float(os.getenv("TRAIL_ATR_MULT", "1.5"))
CHANDELIER_MULT = float(os.getenv("CHANDELIER_MULT", "3.0"))
TARGET_PROTECTION_PROGRESS = float(os.getenv("TARGET_PROTECTION_PROGRESS", "0.85"))
TRAILING_STYLE = os.getenv("TRAILING_STYLE", "ATR").upper()
ATR_REFRESH_CANDLES = int(os.getenv("ATR_REFRESH_CANDLES", "4"))

# ✅ V14.6: Fixed stop and closer target controls
FIXED_STOP_LOSS = os.getenv("FIXED_STOP_LOSS", "true").lower() == "true"
DISABLE_BREAK_EVEN = os.getenv("DISABLE_BREAK_EVEN", "true").lower() == "true"
DISABLE_TRAILING = os.getenv("DISABLE_TRAILING", "true").lower() == "true"
DISABLE_TARGET_PROTECTION = os.getenv("DISABLE_TARGET_PROTECTION", "true").lower() == "true"

LEVERAGE = float(os.getenv("LEVERAGE", "3.0"))
MAX_MARGIN_UTILIZATION = float(os.getenv("MAX_MARGIN_UTILIZATION", "0.80"))
MAKER_FEE_RATE = float(os.getenv("MAKER_FEE_RATE", "0.0002"))
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.0005"))
PAPER_ENTRY_FEE = os.getenv("PAPER_ENTRY_FEE", "TAKER").upper()
PAPER_EXIT_FEE = os.getenv("PAPER_EXIT_FEE", "TAKER").upper()
MAX_SPREAD_ATR = float(os.getenv("MAX_SPREAD_ATR", "0.15"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.08"))
MAX_QUOTE_AGE_SECONDS = float(os.getenv("MAX_QUOTE_AGE_SECONDS", "5.0"))
LOOP_INTERVAL_SECONDS = float(os.getenv("LOOP_INTERVAL_SECONDS", "0.5"))
KLINE_REFRESH_SECONDS = float(os.getenv("KLINE_REFRESH_SECONDS", "120.0"))
BROADCAST_INTERVAL_SECONDS = int(os.getenv("BROADCAST_INTERVAL_SECONDS", "1800"))
ENTRY_CONFIRMATION_MODE = os.getenv("ENTRY_CONFIRMATION_MODE", "CLOSE_5M").upper()
CLOSE_CONFIRMATION_BODY_ATR = float(os.getenv("CLOSE_CONFIRMATION_BODY_ATR", "0.15"))
CLOSE_CONFIRMATION_BREAK_PREV = os.getenv("CLOSE_CONFIRMATION_BREAK_PREV", "true").lower() == "true"
EXIT_SLIPPAGE_PCT = float(os.getenv("EXIT_SLIPPAGE_PCT", "0.02"))
LIMIT_ENTRY_ENABLED = os.getenv("LIMIT_ENTRY_ENABLED", "true").lower() == "true"
LIMIT_ORDER_EXPIRY_CANDLES = int(os.getenv("LIMIT_ORDER_EXPIRY_CANDLES", "8"))
LIMIT_ORDER_EXPIRY_MINUTES = int(os.getenv("LIMIT_ORDER_EXPIRY_MINUTES", "180"))
LIMIT_POI_LOCATION = os.getenv("LIMIT_POI_LOCATION", "PROXIMAL").upper()
MIN_ENTRY_DISTANCE_PCT = float(os.getenv("MIN_ENTRY_DISTANCE_PCT", "0.05"))
FVG_MIN_ATR = float(os.getenv("FVG_MIN_ATR", "0.10"))
FVG_MAX_ATR = float(os.getenv("FVG_MAX_ATR", "2.5"))
SWING_LEFT = int(os.getenv("SWING_LEFT", "5"))
SWING_RIGHT = int(os.getenv("SWING_RIGHT", "5"))
LIQUIDITY_TOL_ATR = float(os.getenv("LIQUIDITY_TOL_ATR", "0.12"))
SWEEP_FRESHNESS_BARS = int(os.getenv("SWEEP_FRESHNESS_BARS", "20"))
SWEEP_INVALIDATION_BARS = int(os.getenv("SWEEP_INVALIDATION_BARS", "5"))
DISPLACEMENT_BODY_ATR = float(os.getenv("DISPLACEMENT_BODY_ATR", "0.80"))
DISPLACEMENT_VOLUME_RATIO = float(os.getenv("DISPLACEMENT_VOLUME_RATIO", "1.20"))
MIN_CLOSE_LOCATION = float(os.getenv("MIN_CLOSE_LOCATION", "0.65"))
REGIME_ADX_PERIOD = int(os.getenv("REGIME_ADX_PERIOD", "14"))
REGIME_TREND_THRESHOLD = float(os.getenv("REGIME_TREND_THRESHOLD", "25.0"))
REGIME_ATR_PCT_HIGH = float(os.getenv("REGIME_ATR_PCT_HIGH", "1.5"))
REGIME_ATR_PCT_LOW = float(os.getenv("REGIME_ATR_PCT_LOW", "0.5"))
ASIA_START = int(os.getenv("ASIA_START", "0"))
ASIA_END = int(os.getenv("ASIA_END", "8"))
LONDON_START = int(os.getenv("LONDON_START", "7"))
LONDON_END = int(os.getenv("LONDON_END", "16"))
NY_START = int(os.getenv("NY_START", "12"))
NY_END = int(os.getenv("NY_END", "21"))
SPOT_BASE_URL = os.getenv("SPOT_BASE_URL", "https://api.binance.com")
FUTURES_BASE_URL = os.getenv("FUTURES_BASE_URL", "https://fapi.binance.com")
BASE_URL = FUTURES_BASE_URL if MARKET_TYPE == "FUTURES" else SPOT_BASE_URL
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
RECV_WINDOW = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8839882725:AAGQFQg60GWKyC5AKiUgCPDZhrwkJqi8Sls")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1004388233604")
TELEGRAM_RATE_LIMIT_PER_MIN = int(os.getenv("TELEGRAM_RATE_LIMIT_PER_MIN", "20"))
NEWS_ENABLED = os.getenv("NEWS_ENABLED", "true").lower() == "true"
NEWS_FAIL_CLOSED = os.getenv("NEWS_FAIL_CLOSED", "false").lower() == "true"
NEWS_BEFORE_MINUTES = int(os.getenv("NEWS_BEFORE_MINUTES", "30"))
NEWS_AFTER_MINUTES = int(os.getenv("NEWS_AFTER_MINUTES", "30"))
NEWS_REFRESH_SECONDS = int(os.getenv("NEWS_REFRESH_SECONDS", "1800"))
NEWS_URL = os.getenv("NEWS_URL", "https://nfs.faireconomy.media/ff_calendar_thisweek.json")
DB_PATH = Path(os.getenv("DB_PATH", "bot_state_v14_6.db"))
SNAPSHOT_PATH = Path(os.getenv("SNAPSHOT_PATH", "bot_state_v14_6.json"))
JOURNAL_PATH = Path(os.getenv("JOURNAL_PATH", "trade_journal_v14_6.jsonl"))
CAPITAL_USDT = float(os.getenv("CAPITAL_USDT", "100.0"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "4"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "0.75"))
WS_ENABLED = os.getenv("WS_ENABLED", "true").lower() == "true"
WS_HEALTH_TIMEOUT = float(os.getenv("WS_HEALTH_TIMEOUT", "30.0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(APP_NAME)
UTC = timezone.utc

def now_ms(): return int(time.time() * 1000)
def utc_now_iso(): return datetime.now(UTC).isoformat()
def clamp(v, lo, hi): return max(lo, min(hi, v))
def safe_float(v, default=0.0):
    try: return float(v)
    except Exception: return default
def finite(v): return math.isfinite(v)
def mean(values):
    vals = [x for x in values if finite(x)]
    return statistics.fmean(vals) if vals else 0.0
def stdev(values):
    vals = [x for x in values if finite(x)]
    if len(vals) < 2: return 0.0
    return statistics.stdev(vals)
def pct(a, b): return 0.0 if b == 0 else (a / b) * 100.0
def timeframe_seconds(tf):
    n = int(tf[:-1]); u = tf[-1]
    return n * {"m": 60, "h": 3600, "d": 86400}[u]
def floor_step(value, step):
    if step <= 0: return value
    try:
        v = Decimal(str(value)); s = Decimal(str(step))
        return float((v / s).to_integral_value(rounding=ROUND_DOWN) * s)
    except (InvalidOperation, ValueError): return math.floor(value / step) * step
def ceil_step(value, step):
    if step <= 0: return value
    try:
        v = Decimal(str(value)); s = Decimal(str(step))
        return float((v / s).to_integral_value(rounding=ROUND_UP) * s)
    except (InvalidOperation, ValueError): return math.ceil(value / step) * step
def atomic_write_json(path, payload):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)
def json_dumps(data):
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), default=str)
def dir_ar(direction):
    if direction == "LONG": return "شراء 🟢"
    if direction == "SHORT": return "بيع 🔴"
    return "محايد ⚪"
def fmt_price(symbol, price):
    if price is None: return "-"
    if "BTC" in symbol: return f"{price:,.2f}"
    if "PAXG" in symbol: return f"{price:,.3f}"
    return f"{price:,.4f}"
def pearson_correlation(xs, ys):
    n = min(len(xs), len(ys))
    if n < 3: return 0.0
    xs, ys = xs[-n:], ys[-n:]
    mx, my = mean(xs), mean(ys)
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0: return 0.0
    return num / (dx * dy)
def current_session():
    h = datetime.now(UTC).hour
    in_london = LONDON_START <= h < LONDON_END
    in_ny = NY_START <= h < NY_END
    in_asia = ASIA_START <= h < ASIA_END
    if in_london and in_ny: return "LONDON+NY"
    if in_london: return "LONDON"
    if in_ny: return "NY"
    if in_asia: return "ASIA"
    return "OFF_HOURS"
def session_liquidity_score():
    s = current_session()
    if s == "LONDON+NY": return 1.0
    if s in {"LONDON", "NY"}: return 0.9
    if s == "ASIA": return 0.75
    return 0.5

@dataclass
class Quote:
    symbol: str
    bid: float
    ask: float
    ts_ms: int
    source: str = "REST"
    @property
    def mid(self): return (self.bid + self.ask) / 2.0
    @property
    def spread(self): return max(0.0, self.ask - self.bid)
    @property
    def spread_pct(self): return pct(self.spread, self.mid)

@dataclass
class ExchangeRules:
    symbol: str
    tick_size: float = 0.0
    min_price: float = 0.0
    max_price: float = 0.0
    step_size: float = 0.0
    min_qty: float = 0.0
    max_qty: float = 0.0
    market_step_size: float = 0.0
    market_min_qty: float = 0.0
    market_max_qty: float = 0.0
    min_notional: float = 0.0
    max_notional: float = 0.0

@dataclass
class Zone:
    kind: str
    direction: str
    low: float
    high: float
    created_index: int
    source_index: int
    mitigated: bool = False
    displacement: bool = False
    score: float = 0.0
    @property
    def midpoint(self): return (self.low + self.high) / 2.0

@dataclass
class MarketRegime:
    kind: str
    adx: float
    atr_pct: float
    trend_direction: str
    strength: float

@dataclass
class Signal:
    symbol: str
    direction: str
    score: float
    entry_probability: float
    entry_price: float
    stop_price: float
    target_price: float
    risk_distance: float
    rr: float
    atr: float
    spread: float
    spread_pct: float
    bias_4h: str
    structure_1h: str
    structure_15m: str
    confirmation_5m: str
    regime: str = "UNKNOWN"
    session: str = "UNKNOWN"
    bos: bool = False
    choch: bool = False
    mss: bool = False
    liquidity_sweep: bool = False
    fvg: bool = False
    order_block: bool = False
    premium_discount: str = "EQUILIBRIUM"
    poi_kind: str = ""
    poi_low: float = 0.0
    poi_high: float = 0.0
    reasons: List[str] = field(default_factory=list)
    required_passed: List[str] = field(default_factory=list)
    bonuses: List[str] = field(default_factory=list)
    prediction: Optional[Dict[str, Any]] = None

@dataclass
class PendingOrder:
    id: str
    symbol: str
    direction: str
    entry_price: float
    stop_price: float
    target_price: float
    risk_distance: float
    qty: float
    notional: float
    margin: float
    score: float
    probability: float
    atr: float
    created_ms: int
    created_candle_ms: int
    expiry_ms: int
    expiry_candle_ms: int
    poi_kind: str
    poi_low: float
    poi_high: float
    prediction: Optional[Dict[str, Any]] = None
    confirmation_mode: str = "LIMIT"
    last_5m_close_ms: int = 0
    last_15m_close_ms: int = 0
    status: str = "PENDING"

@dataclass
class Position:
    id: str
    symbol: str
    direction: str
    entry_price: float
    qty: float
    notional: float
    margin: float
    initial_risk: float
    stop_price: float
    original_stop: float
    target_price: float
    atr: float
    atr_candle_ms: int
    score: float
    probability: float
    opened_ms: int
    moved_to_be: bool = False
    target_protection: bool = False
    exit_price: float = 0.0
    exit_ms: int = 0
    exit_reason: str = ""
    gross_pnl: float = 0.0
    fees: float = 0.0
    net_pnl: float = 0.0
    prediction: Optional[Dict[str, Any]] = None
    status: str = "OPEN"

class HttpClient:
    def __init__(self):
        self.session = requests.Session() if requests else None
    def _urllib(self, method, url, params, headers):
        query = urllib.parse.urlencode([(k, v) for k, v in (params or {}).items() if v is not None])
        full = f"{url}?{query}" if query else url
        req = urllib.request.Request(full, method=method, headers=headers or {})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8"))
    def request(self, method, path, params=None, signed=False):
        url = BASE_URL.rstrip("/") + path
        p = dict(params or {})
        headers = {}
        if API_KEY: headers["X-MBX-APIKEY"] = API_KEY
        if signed:
            p["timestamp"] = now_ms() + SERVER_TIME_OFFSET_MS
            p["recvWindow"] = RECV_WINDOW
            qs = urllib.parse.urlencode([(k, p[k]) for k in sorted(p)])
            sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
            p["signature"] = sig
        last_exc = None
        for attempt in range(HTTP_RETRIES):
            try:
                if self.session:
                    if method == "GET":
                        r = self.session.get(url, params=p, headers=headers, timeout=HTTP_TIMEOUT)
                    else:
                        r = self.session.request(method, url, params=p, headers=headers, timeout=HTTP_TIMEOUT)
                    r.raise_for_status()
                    return r.json()
                return self._urllib(method, url, p, headers)
            except Exception as exc:
                last_exc = exc
                if attempt + 1 < HTTP_RETRIES:
                    time.sleep(HTTP_BACKOFF * (2 ** attempt))
        raise RuntimeError(f"HTTP failed {method} {path}: {last_exc}")

HTTP = HttpClient()
SERVER_TIME_OFFSET_MS = 0
SERVER_TIME_LOCK = threading.Lock()

def sync_server_time():
    global SERVER_TIME_OFFSET_MS
    path = "/fapi/v1/time" if MARKET_TYPE == "FUTURES" else "/api/v3/time"
    data = HTTP.request("GET", path)
    server = int(data["serverTime"])
    local = now_ms()
    with SERVER_TIME_LOCK:
        SERVER_TIME_OFFSET_MS = server - local
    log.info("Binance server time offset: %+d ms", SERVER_TIME_OFFSET_MS)

def klines_sync(symbol, interval, limit=KLINE_LIMIT):
    path = "/fapi/v1/klines" if MARKET_TYPE == "FUTURES" else "/api/v3/klines"
    raw = HTTP.request("GET", path, {"symbol": symbol, "interval": interval, "limit": limit})
    out = []
    for x in raw:
        out.append({"open_time": int(x[0]), "open": safe_float(x[1]), "high": safe_float(x[2]),
                    "low": safe_float(x[3]), "close": safe_float(x[4]), "volume": safe_float(x[5]),
                    "close_time": int(x[6]), "quote_volume": safe_float(x[7]), "trades": int(x[8])})
    return out

def book_ticker_sync(symbol):
    path = "/fapi/v1/ticker/bookTicker" if MARKET_TYPE == "FUTURES" else "/api/v3/ticker/bookTicker"
    d = HTTP.request("GET", path, {"symbol": symbol})
    return Quote(symbol=symbol, bid=safe_float(d["bidPrice"]),
                 ask=safe_float(d["askPrice"]), ts_ms=now_ms(), source="REST")

def exchange_info_sync():
    path = "/fapi/v1/exchangeInfo" if MARKET_TYPE == "FUTURES" else "/api/v3/exchangeInfo"
    data = HTTP.request("GET", path)
    result = {}
    for s in data.get("symbols", []):
        symbol = s.get("symbol")
        if symbol not in SYMBOLS: continue
        r = ExchangeRules(symbol=symbol)
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                r.tick_size = safe_float(f.get("tickSize"))
                r.min_price = safe_float(f.get("minPrice"))
                r.max_price = safe_float(f.get("maxPrice"))
            elif t == "LOT_SIZE":
                r.step_size = safe_float(f.get("stepSize"))
                r.min_qty = safe_float(f.get("minQty"))
                r.max_qty = safe_float(f.get("maxQty"))
            elif t == "MARKET_LOT_SIZE":
                r.market_step_size = safe_float(f.get("stepSize"))
                r.market_min_qty = safe_float(f.get("minQty"))
                r.market_max_qty = safe_float(f.get("maxQty"))
            elif t == "MIN_NOTIONAL":
                r.min_notional = safe_float(f.get("notional", f.get("minNotional", 0)))
            elif t == "NOTIONAL":
                r.min_notional = safe_float(f.get("minNotional", r.min_notional))
                r.max_notional = safe_float(f.get("maxNotional"))
        result[symbol] = r
    return result

class QuoteCache:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()
    def set(self, q):
        if q.bid <= 0 or q.ask <= 0 or q.ask < q.bid: return
        with self._lock:
            self._data[q.symbol] = q
    def get(self, symbol):
        with self._lock:
            q = self._data.get(symbol)
            if q is None: return None
            return Quote(**asdict(q))

QUOTES = QuoteCache()
SHUTDOWN = threading.Event()

class WebSocketHealth:
    def __init__(self):
        self.last_msg = {}
        self.connected = {}
        self.errors = {}
        self.lock = threading.Lock()
    def mark_msg(self, s):
        with self.lock: self.last_msg[s] = time.time()
    def mark_connected(self, s, ok):
        with self.lock: self.connected[s] = ok
    def mark_error(self, s):
        with self.lock: self.errors[s] = self.errors.get(s, 0) + 1
    def is_healthy(self, s):
        with self.lock:
            if not self.connected.get(s, False): return False
            return (time.time() - self.last_msg.get(s, 0)) < WS_HEALTH_TIMEOUT
    def status(self):
        with self.lock:
            return {"connected": dict(self.connected), "last_msg": dict(self.last_msg),
                    "errors": dict(self.errors)}

WS_HEALTH = WebSocketHealth()

def websocket_url():
    if MARKET_TYPE == "FUTURES": return "wss://fstream.binance.com/stream"
    return "wss://stream.binance.com:9443/stream"

def websocket_worker(symbol):
    if websocket is None or not WS_ENABLED: return
    stream = f"{symbol.lower()}@bookTicker"
    url = websocket_url() + "?streams=" + stream
    def on_message(_, message):
        try:
            d = json.loads(message)
            data = d.get("data", d)
            QUOTES.set(Quote(symbol=symbol, bid=safe_float(data.get("b")),
                             ask=safe_float(data.get("a")), ts_ms=now_ms(), source="WS"))
            WS_HEALTH.mark_msg(symbol)
        except Exception as exc:
            log.warning("[WS %s] parse: %s", symbol, exc)
            WS_HEALTH.mark_error(symbol)
    def on_open(_): WS_HEALTH.mark_connected(symbol, True)
    def on_error(_, error):
        WS_HEALTH.mark_connected(symbol, False)
        WS_HEALTH.mark_error(symbol)
    def on_close(_, code, msg): WS_HEALTH.mark_connected(symbol, False)
    while not SHUTDOWN.is_set():
        try:
            ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open,
                                        on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            log.warning("[WS %s] exception: %s", symbol, exc)
        WS_HEALTH.mark_connected(symbol, False)
        SHUTDOWN.wait(3.0)

def start_websockets():
    if websocket is None or not WS_ENABLED: return
    for symbol in SYMBOLS:
        t = threading.Thread(target=websocket_worker, args=(symbol,), daemon=True, name=f"ws-{symbol}")
        t.start()

async def refresh_quotes_if_needed():
    needs_rest = []
    threshold = int(MAX_QUOTE_AGE_SECONDS * 1000)
    for symbol in SYMBOLS:
        q = QUOTES.get(symbol)
        ws_ok = WS_HEALTH.is_healthy(symbol)
        if q is None or (now_ms() - q.ts_ms) > threshold or not ws_ok:
            needs_rest.append(symbol)
    if not needs_rest: return
    tasks = [asyncio.to_thread(book_ticker_sync, s) for s in needs_rest]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for symbol, result in zip(needs_rest, results):
        if isinstance(result, Exception): continue
        QUOTES.set(result)

def sma(values, period):
    if not values: return 0.0
    return mean(values[-period:])

def ema(values, period):
    if not values: return 0.0
    if len(values) < period: return mean(values)
    alpha = 2.0 / (period + 1.0)
    v = mean(values[:period])
    for x in values[period:]: v = alpha * x + (1.0 - alpha) * v
    return v

def atr(candles, period=14):
    if len(candles) < 2: return 0.0
    period = min(period, len(candles) - 1)
    window = candles[-(period + 1):]
    trs = []
    prev = window[0]["close"]
    for c in window[1:]:
        trs.append(max(c["high"] - c["low"], abs(c["high"] - prev), abs(c["low"] - prev)))
        prev = c["close"]
    return mean(trs) if trs else 0.0

def rsi(candles, period=14):
    if len(candles) <= period: return 50.0
    cs = [c["close"] for c in candles[-(period + 1):]]
    gains, losses = [], []
    for i in range(1, len(cs)):
        d = cs[i] - cs[i - 1]
        gains.append(max(0.0, d))
        losses.append(max(0.0, -d))
    ag, al = mean(gains), mean(losses)
    if al == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + ag / al))

def adx(candles, period=14):
    if len(candles) < period * 2 + 1: return 0.0
    window = candles[-(period * 2 + 1):]
    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(window)):
        up = window[i]["high"] - window[i - 1]["high"]
        dn = window[i - 1]["low"] - window[i]["low"]
        plus_dm.append(up if up > dn and up > 0 else 0.0)
        minus_dm.append(dn if dn > up and dn > 0 else 0.0)
        trs.append(max(window[i]["high"] - window[i]["low"],
                       abs(window[i]["high"] - window[i - 1]["close"]),
                       abs(window[i]["low"] - window[i - 1]["close"])))
    atr_val = mean(trs[-period:])
    if atr_val <= 0: return 0.0
    plus_di = 100.0 * mean(plus_dm[-period:]) / atr_val
    minus_di = 100.0 * mean(minus_dm[-period:]) / atr_val
    di_sum = plus_di + minus_di
    if di_sum <= 0: return 0.0
    return 100.0 * abs(plus_di - minus_di) / di_sum

def volume_ratio(candles, period=20):
    if len(candles) <= period: return 1.0
    avg = mean([x["volume"] for x in candles[-period-1:-1]])
    if avg <= 0: return 1.0
    return candles[-1]["volume"] / avg

def close_location(c):
    rng = c["high"] - c["low"]
    if rng <= 0: return 0.5
    return (c["close"] - c["low"]) / rng

def realized_volatility(candles, period=30):
    if len(candles) < period + 1: return 0.0
    closes = [c["close"] for c in candles[-(period + 1):]]
    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0: returns.append(math.log(closes[i] / closes[i - 1]))
    if len(returns) < 2: return 0.0
    return stdev(returns)

def detect_regime(candles, atr_val):
    if len(candles) < 30: return MarketRegime("UNKNOWN", 0.0, 0.0, "NEUTRAL", 0.0)
    adx_val = adx(candles, REGIME_ADX_PERIOD)
    price = candles[-1]["close"]
    atr_pct = pct(atr_val, price)
    closes_list = [c["close"] for c in candles]
    ema_fast = ema(closes_list, 20)
    ema_slow = ema(closes_list, 50)
    if ema_fast > ema_slow: trend_dir = "BULLISH"
    elif ema_fast < ema_slow: trend_dir = "BEARISH"
    else: trend_dir = "NEUTRAL"
    if adx_val >= REGIME_TREND_THRESHOLD:
        kind = "TRENDING"
        strength = clamp((adx_val - REGIME_TREND_THRESHOLD) / 25.0 + 0.5, 0.0, 1.0)
    elif atr_pct >= REGIME_ATR_PCT_HIGH:
        kind = "VOLATILE"
        strength = clamp(atr_pct / (REGIME_ATR_PCT_HIGH * 2), 0.0, 1.0)
    else:
        kind = "RANGING"
        strength = clamp(1.0 - adx_val / REGIME_TREND_THRESHOLD, 0.0, 1.0)
    return MarketRegime(kind, adx_val, atr_pct, trend_dir, strength)

def swing_highs(candles, left=SWING_LEFT, right=SWING_RIGHT):
    out = []
    for i in range(left, len(candles) - right):
        h = candles[i]["high"]
        if all(h > candles[j]["high"] for j in range(i-left, i)) and \
           all(h >= candles[j]["high"] for j in range(i+1, i+right+1)):
            out.append((i, h))
    return out

def swing_lows(candles, left=SWING_LEFT, right=SWING_RIGHT):
    out = []
    for i in range(left, len(candles) - right):
        lo = candles[i]["low"]
        if all(lo < candles[j]["low"] for j in range(i-left, i)) and \
           all(lo <= candles[j]["low"] for j in range(i+1, i+right+1)):
            out.append((i, lo))
    return out

def structure_state(candles):
    if len(candles) < 40:
        return {"trend": "NEUTRAL", "bos": False, "choch": False, "mss": False,
                "bos_up": False, "bos_down": False, "choch_up": False, "choch_down": False,
                "mss_up": False, "mss_down": False, "last_swing_high": 0.0, "last_swing_low": 0.0}
    sh = swing_highs(candles)
    sl = swing_lows(candles)
    last_high = sh[-1][1] if sh else max(c["high"] for c in candles[-20:])
    last_low = sl[-1][1] if sl else min(c["low"] for c in candles[-20:])
    prev_high = sh[-2][1] if len(sh) >= 2 else last_high
    prev_low = sl[-2][1] if len(sl) >= 2 else last_low
    close = candles[-1]["close"]
    hh_hl = last_high > prev_high and last_low > prev_low
    lh_ll = last_high < prev_high and last_low < prev_low
    bos_up = close > last_high
    bos_down = close < last_low
    if hh_hl: trend = "BULLISH"
    elif lh_ll: trend = "BEARISH"
    else: trend = "NEUTRAL"
    choch_up = trend == "BEARISH" and bos_up
    choch_down = trend == "BULLISH" and bos_down
    mss_up = bos_up or choch_up
    mss_down = bos_down or choch_down
    return {"trend": trend, "bos": bos_up or bos_down, "bos_up": bos_up, "bos_down": bos_down,
            "choch": choch_up or choch_down, "choch_up": choch_up, "choch_down": choch_down,
            "mss": mss_up or mss_down, "mss_up": mss_up, "mss_down": mss_down,
            "last_swing_high": last_high, "last_swing_low": last_low}

def liquidity_levels(candles):
    if len(candles) < 30: return {"eqh": [], "eql": []}
    a = atr(candles, 14)
    tol = max(a * LIQUIDITY_TOL_ATR, 1e-12)
    highs = swing_highs(candles)
    lows = swing_lows(candles)
    eqh, eql = [], []
    for i in range(max(0, len(highs) - 12), len(highs)):
        for j in range(i + 1, len(highs)):
            if abs(highs[i][1] - highs[j][1]) <= tol:
                eqh.append(mean([highs[i][1], highs[j][1]]))
    for i in range(max(0, len(lows) - 12), len(lows)):
        for j in range(i + 1, len(lows)):
            if abs(lows[i][1] - lows[j][1]) <= tol:
                eql.append(mean([lows[i][1], lows[j][1]]))
    return {"eqh": sorted(set(round(x, 10) for x in eqh)), "eql": sorted(set(round(x, 10) for x in eql))}

def detect_sweep(candles, direction):
    if len(candles) < 30: return False
    a = atr(candles, 14)
    if a <= 0: return False
    levels = liquidity_levels(candles)
    levels_list = levels["eql"] if direction == "LONG" else levels["eqh"]
    if not levels_list: return False
    start = max(1, len(candles) - SWEEP_FRESHNESS_BARS)
    for i in range(start, len(candles)):
        c = candles[i]
        for level in levels_list:
            if direction == "LONG":
                if c["low"] < level - a * 0.02 and c["close"] > level:
                    later = candles[i+1:i+1+SWEEP_INVALIDATION_BARS]
                    if any(x["close"] < level for x in later): continue
                    return True
            else:
                if c["high"] > level + a * 0.02 and c["close"] < level:
                    later = candles[i+1:i+1+SWEEP_INVALIDATION_BARS]
                    if any(x["close"] > level for x in later): continue
                    return True
    return False

def displacement_score(candles, direction):
    if len(candles) < 30: return 0.0
    c = candles[-1]
    a = atr(candles, 14)
    if a <= 0: return 0.0
    body = abs(c["close"] - c["open"])
    vr = volume_ratio(candles, 20)
    loc = close_location(c)
    if direction == "LONG":
        direction_ok = c["close"] > c["open"]
        loc_ok = loc >= MIN_CLOSE_LOCATION
    else:
        direction_ok = c["close"] < c["open"]
        loc_ok = loc <= (1.0 - MIN_CLOSE_LOCATION)
    if not direction_ok: return 0.0
    score = 0.0
    score += clamp(body / (a * DISPLACEMENT_BODY_ATR), 0, 1) * 0.55
    score += clamp(vr / DISPLACEMENT_VOLUME_RATIO, 0, 1) * 0.25
    score += (1.0 if loc_ok else 0.0) * 0.20
    return score

ZONE_CACHE = {}
ZONE_CACHE_LOCK = threading.Lock()

def _cache_key(symbol, last_candle_time, kind, direction):
    return (f"{symbol}|{kind}|{direction}", last_candle_time)

def find_fvgs(candles, direction, symbol_id=""):
    if len(candles) < 10: return []
    if symbol_id and len(candles) >= 1:
        key = _cache_key(symbol_id, candles[-1]["close_time"], "FVG", direction)
        with ZONE_CACHE_LOCK:
            if key in ZONE_CACHE: return ZONE_CACHE[key]
    a = atr(candles, 14)
    if a <= 0: return []
    start = max(2, len(candles) - POI_LOOKBACK)
    zones = []
    for i in range(start, len(candles)):
        c0, c1, c2 = candles[i-2], candles[i-1], candles[i]
        if direction == "LONG" and c2["low"] > c0["high"]:
            low, high = c0["high"], c2["low"]
            if FVG_MIN_ATR * a <= (high - low) <= FVG_MAX_ATR * a:
                disp = displacement_score(candles[:i+1], "LONG")
                if disp >= 0.20:
                    mitigated = any(x["low"] <= high and x["high"] >= low for x in candles[i+1:])
                    zones.append(Zone("FVG", "LONG", low, high, i, i-1, mitigated, disp >= 0.5, disp))
        elif direction == "SHORT" and c2["high"] < c0["low"]:
            low, high = c2["high"], c0["low"]
            if FVG_MIN_ATR * a <= (high - low) <= FVG_MAX_ATR * a:
                disp = displacement_score(candles[:i+1], "SHORT")
                if disp >= 0.20:
                    mitigated = any(x["low"] <= high and x["high"] >= low for x in candles[i+1:])
                    zones.append(Zone("FVG", "SHORT", low, high, i, i-1, mitigated, disp >= 0.5, disp))
    result = [z for z in zones if not z.mitigated]
    if symbol_id and len(candles) >= 1:
        key = _cache_key(symbol_id, candles[-1]["close_time"], "FVG", direction)
        with ZONE_CACHE_LOCK: ZONE_CACHE[key] = result
    return result

def find_order_blocks(candles, direction, symbol_id=""):
    if len(candles) < 20: return []
    if symbol_id and len(candles) >= 1:
        key = _cache_key(symbol_id, candles[-1]["close_time"], "OB", direction)
        with ZONE_CACHE_LOCK:
            if key in ZONE_CACHE: return ZONE_CACHE[key]
    a = atr(candles, 14)
    if a <= 0: return []
    start = max(2, len(candles) - POI_LOOKBACK)
    zones = []
    for i in range(start, len(candles) - 1):
        c = candles[i]
        future = candles[i+1:]
        if direction == "LONG":
            if c["close"] >= c["open"]: continue
            disp = displacement_score(candles[:i+2], "LONG")
            if disp < 0.35: continue
            if not any(x["close"] > c["high"] for x in future[:8]): continue
            low, high = c["low"], c["open"]
        else:
            if c["close"] <= c["open"]: continue
            disp = displacement_score(candles[:i+2], "SHORT")
            if disp < 0.35: continue
            if not any(x["close"] < c["low"] for x in future[:8]): continue
            low, high = c["open"], c["high"]
        mitigated = any(x["low"] <= high and x["high"] >= low for x in future[8:])
        if not mitigated:
            if direction == "LONG":
                if any(x["close"] < low for x in future): mitigated = True
            else:
                if any(x["close"] > high for x in future): mitigated = True
        zones.append(Zone("OB", direction, low, high, i, i, mitigated, disp >= 0.5, disp))
    result = [z for z in zones if not z.mitigated]
    if symbol_id and len(candles) >= 1:
        key = _cache_key(symbol_id, candles[-1]["close_time"], "OB", direction)
        with ZONE_CACHE_LOCK: ZONE_CACHE[key] = result
    return result

def premium_discount(candles):
    if len(candles) < 20: return "EQUILIBRIUM"
    window = candles[-100:]
    hi = max(x["high"] for x in window)
    lo = min(x["low"] for x in window)
    eq = (hi + lo) / 2.0
    price = window[-1]["close"]
    if price > eq: return "PREMIUM"
    if price < eq: return "DISCOUNT"
    return "EQUILIBRIUM"

def zone_distance(price, z):
    if z.low <= price <= z.high: return 0.0
    return min(abs(price - z.low), abs(price - z.high))

def select_poi(candles, direction, price, fvgs=None, obs=None):
    if fvgs is None: fvgs = find_fvgs(candles, direction)
    if obs is None: obs = find_order_blocks(candles, direction)
    candidates = fvgs + obs
    if not candidates: return None
    def key(z):
        age = len(candles) - z.created_index
        recency = clamp(1.0 - age / max(POI_LOOKBACK, 1), 0, 1)
        proximity = 1.0 / (1.0 + zone_distance(price, z))
        overlap_bonus = 0.0
        for other in candidates:
            if other is z: continue
            if z.low <= other.high and other.low <= z.high:
                overlap_bonus = 0.35
                break
        return recency * 0.30 + z.score * 0.25 + proximity * 0.20 + \
               (0.15 if z.displacement else 0.0) + overlap_bonus
    return max(candidates, key=key)

def normalize_price(price, rules, direction, order_role="ENTRY"):
    step = rules.tick_size
    if step <= 0: return price
    if order_role in {"ENTRY", "STOP", "TARGET"}:
        if direction == "LONG": return floor_step(price, step)
        return ceil_step(price, step)
    return floor_step(price, step)

def normalize_qty(qty, rules, market=False):
    step = rules.market_step_size if market and rules.market_step_size > 0 else rules.step_size
    min_q = rules.market_min_qty if market and rules.market_min_qty > 0 else rules.min_qty
    max_q = rules.market_max_qty if market and rules.market_max_qty > 0 else rules.max_qty
    if step > 0: qty = floor_step(qty, step)
    if max_q > 0: qty = min(qty, max_q)
    if min_q > 0 and qty < min_q: return 0.0
    return max(0.0, qty)

def fee_rate(side):
    return MAKER_FEE_RATE if (PAPER_ENTRY_FEE if side == "ENTRY" else PAPER_EXIT_FEE) == "MAKER" else TAKER_FEE_RATE

def estimate_fees(ne, nx):
    return ne * fee_rate("ENTRY") + nx * fee_rate("EXIT")

def spread_is_acceptable(quote, atr_value):
    if quote.mid <= 0: return False, "invalid quote"
    if quote.spread_pct > MAX_SPREAD_PCT: return False, "spread"
    if atr_value > 0 and quote.spread > atr_value * MAX_SPREAD_ATR: return False, "spread vs ATR"
    return True, "ok"

def effective_risk_pct(equity, initial_equity):
    base = RISK_PER_TRADE_PCT
    if not DRAWDOWN_ADAPTIVE_ENABLED or initial_equity <= 0: return base
    dd = max(0.0, (initial_equity - equity) / initial_equity * 100.0)
    if dd >= MAX_DRAWDOWN_FOR_REDUCTION_PCT: return base * DRAWDOWN_RISK_MULTIPLIER
    factor = 1.0 - (dd / MAX_DRAWDOWN_FOR_REDUCTION_PCT) * (1.0 - DRAWDOWN_RISK_MULTIPLIER)
    return base * max(DRAWDOWN_RISK_MULTIPLIER, min(1.0, factor))

def kelly_fraction(win_rate, avg_win_r, avg_loss_r):
    if avg_loss_r <= 0 or win_rate <= 0 or win_rate >= 1: return 0.0
    b = avg_win_r / avg_loss_r
    q = 1.0 - win_rate
    kelly = (b * win_rate - q) / b
    if kelly <= 0: return 0.0
    return kelly * KELLY_FRACTION

def closed_stats():
    try:
        rows = DB.conn.execute("SELECT net_pnl, initial_risk FROM trades WHERE status='CLOSED'").fetchall()
    except Exception:
        return 0, 0.0, 0.0, 0.0
    if len(rows) < KELLY_MIN_TRADES: return len(rows), 0.0, 0.0, 0.0
    wins, losses = [], []
    for r in rows:
        net = safe_float(r["net_pnl"])
        risk = safe_float(r["initial_risk"])
        if risk <= 0: continue
        r_mult = net / risk
        if r_mult > 0: wins.append(r_mult)
        elif r_mult < 0: losses.append(abs(r_mult))
    if not wins and not losses: return len(rows), 0.0, 0.0, 0.0
    total = len(wins) + len(losses)
    return (total, len(wins) / total if total else 0.0,
            mean(wins) if wins else 0.0, mean(losses) if losses else 1.0)

def calculate_qty(equity, entry, stop, direction, rules, initial_equity=None):
    if equity <= 0 or entry <= 0 or stop <= 0: return {"qty": 0.0, "risk": 0.0, "notional": 0.0, "margin": 0.0}
    if equity < MIN_EQUITY_TO_TRADE: return {"qty": 0.0, "risk": 0.0, "notional": 0.0, "margin": 0.0}
    distance = abs(entry - stop)
    if distance <= 0: return {"qty": 0.0, "risk": 0.0, "notional": 0.0, "margin": 0.0}
    if SIZING_MODE == "FIXED_NOTIONAL":
        target_notional = equity * (POSITION_SIZE_PCT / 100.0)
        raw_qty = target_notional / entry
    else:
        risk_pct = effective_risk_pct(equity, initial_equity or equity)
        if KELLY_ENABLED:
            n, wr, aw, al = closed_stats()
            if n >= KELLY_MIN_TRADES and wr > 0:
                kelly_pct = kelly_fraction(wr, aw, al) * 100.0
                risk_pct = min(risk_pct, kelly_pct) if kelly_pct > 0 else risk_pct * 0.5
        risk_budget = equity * (risk_pct / 100.0)
        raw_qty = risk_budget / distance
    qty = normalize_qty(raw_qty, rules)
    if qty <= 0: return {"qty": 0.0, "risk": 0.0, "notional": 0.0, "margin": 0.0}
    notional = qty * entry
    if rules.min_notional > 0 and notional < rules.min_notional:
        return {"qty": 0.0, "risk": 0.0, "notional": 0.0, "margin": 0.0}
    if rules.max_notional > 0 and notional > rules.max_notional:
        qty = normalize_qty(rules.max_notional / entry, rules)
        notional = qty * entry
    margin = notional / max(LEVERAGE, 1.0)
    if margin > equity * MAX_MARGIN_UTILIZATION:
        qty = normalize_qty(equity * MAX_MARGIN_UTILIZATION * max(LEVERAGE, 1.0) / entry, rules)
        notional = qty * entry
        margin = notional / max(LEVERAGE, 1.0)
    gross_risk = qty * distance
    fees = estimate_fees(notional, qty * stop)
    total_risk = gross_risk + fees
    if SIZING_MODE == "RISK":
        risk_budget = equity * (RISK_PER_TRADE_PCT / 100.0)
        if total_risk > risk_budget and qty > 0:
            target_qty = max(0.0, (risk_budget - fees) / distance)
            qty = normalize_qty(target_qty, rules)
            notional = qty * entry
            margin = notional / max(LEVERAGE, 1.0)
            gross_risk = qty * distance
            fees = estimate_fees(notional, qty * stop)
            total_risk = gross_risk + fees
    return {"qty": qty, "risk": total_risk, "notional": notional, "margin": margin}

SCHEMA_VERSION = 12

class Database:
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()
    def _init(self):
        with self.lock:
            c = self.conn.cursor()
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=FULL")
            c.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            c.execute("""CREATE TABLE IF NOT EXISTS risk_state (
                id INTEGER PRIMARY KEY CHECK(id=1), equity REAL NOT NULL,
                initial_equity REAL NOT NULL DEFAULT 0, day_start_equity REAL NOT NULL,
                day_key TEXT NOT NULL, consecutive_losses INTEGER NOT NULL DEFAULT 0,
                last_trade_ms INTEGER NOT NULL DEFAULT 0, updated_ms INTEGER NOT NULL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS symbols (symbol TEXT PRIMARY KEY,
                rules_json TEXT NOT NULL, updated_ms INTEGER NOT NULL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY, symbol TEXT NOT NULL, direction TEXT NOT NULL,
                status TEXT NOT NULL, entry_price REAL NOT NULL, qty REAL NOT NULL,
                notional REAL NOT NULL, margin REAL NOT NULL, initial_risk REAL NOT NULL,
                stop_price REAL NOT NULL, original_stop REAL NOT NULL, target_price REAL NOT NULL,
                atr REAL NOT NULL, atr_candle_ms INTEGER NOT NULL DEFAULT 0,
                score REAL NOT NULL, probability REAL NOT NULL, opened_ms INTEGER NOT NULL,
                moved_to_be INTEGER NOT NULL DEFAULT 0, target_protection INTEGER NOT NULL DEFAULT 0,
                exit_price REAL NOT NULL DEFAULT 0, exit_ms INTEGER NOT NULL DEFAULT 0,
                exit_reason TEXT NOT NULL DEFAULT '', gross_pnl REAL NOT NULL DEFAULT 0,
                fees REAL NOT NULL DEFAULT 0, net_pnl REAL NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}')""")
            c.execute("""CREATE TABLE IF NOT EXISTS pending_orders (
                id TEXT PRIMARY KEY, symbol TEXT NOT NULL, direction TEXT NOT NULL,
                status TEXT NOT NULL, created_ms INTEGER NOT NULL, expiry_ms INTEGER NOT NULL,
                expiry_candle_ms INTEGER NOT NULL, payload_json TEXT NOT NULL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, symbol TEXT,
                event_type TEXT NOT NULL, payload_json TEXT NOT NULL)""")
            row = c.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if row is None:
                c.execute("INSERT INTO meta(key,value) VALUES('schema_version',?)", (str(SCHEMA_VERSION),))
            else:
                c.execute("UPDATE meta SET value=? WHERE key='schema_version'", (str(SCHEMA_VERSION),))
            cols = {r[1] for r in c.execute("PRAGMA table_info(risk_state)").fetchall()}
            if "initial_equity" not in cols:
                c.execute("ALTER TABLE risk_state ADD COLUMN initial_equity REAL NOT NULL DEFAULT 0")
            cols = {r[1] for r in c.execute("PRAGMA table_info(trades)").fetchall()}
            if "atr_candle_ms" not in cols:
                c.execute("ALTER TABLE trades ADD COLUMN atr_candle_ms INTEGER NOT NULL DEFAULT 0")
            risk = c.execute("SELECT id FROM risk_state WHERE id=1").fetchone()
            if risk is None:
                key = datetime.now(UTC).strftime("%Y-%m-%d")
                c.execute("""INSERT INTO risk_state(id,equity,initial_equity,day_start_equity,day_key,
                    consecutive_losses,last_trade_ms,updated_ms) VALUES(1,?,?,?,?,?,?,?)""",
                    (CAPITAL_USDT, CAPITAL_USDT, CAPITAL_USDT, key, 0, 0, now_ms()))
            self.conn.commit()
    def get_risk(self):
        with self.lock:
            r = self.conn.execute("SELECT * FROM risk_state WHERE id=1").fetchone()
            return dict(r)
    def update_risk(self, **fields):
        if not fields: return
        fields["updated_ms"] = now_ms()
        with self.lock:
            sets = ", ".join(f"{k}=?" for k in fields)
            self.conn.execute(f"UPDATE risk_state SET {sets} WHERE id=1", list(fields.values()))
            self.conn.commit()
    def reset_day_if_needed(self):
        risk = self.get_risk()
        key = datetime.now(UTC).strftime("%Y-%m-%d")
        if risk["day_key"] != key:
            self.update_risk(day_key=key, day_start_equity=risk["equity"], consecutive_losses=0)
            return self.get_risk()
        return risk
    def log_event(self, event_type, payload, symbol=""):
        with self.lock:
            self.conn.execute("INSERT INTO events(ts_ms,symbol,event_type,payload_json) VALUES(?,?,?,?)",
                (now_ms(), symbol, event_type, json_dumps(payload)))
            self.conn.commit()
    def upsert_rules(self, rules):
        with self.lock:
            self.conn.execute("""INSERT INTO symbols(symbol,rules_json,updated_ms) VALUES(?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET rules_json=excluded.rules_json,
                updated_ms=excluded.updated_ms""", (rules.symbol, json_dumps(asdict(rules)), now_ms()))
            self.conn.commit()
    def save_pending(self, order):
        with self.lock:
            self.conn.execute("""INSERT OR REPLACE INTO pending_orders(
                id,symbol,direction,status,created_ms,expiry_ms,expiry_candle_ms,payload_json)
                VALUES(?,?,?,?,?,?,?,?)""",
                (order.id, order.symbol, order.direction, order.status, order.created_ms,
                 order.expiry_ms, order.expiry_candle_ms, json_dumps(asdict(order))))
            self.conn.commit()
    def pending(self):
        with self.lock:
            rows = self.conn.execute("SELECT payload_json FROM pending_orders WHERE status='PENDING'").fetchall()
        result = []
        for r in rows:
            try:
                d = json.loads(r["payload_json"])
                d.setdefault("confirmation_mode", "LIMIT")
                d.setdefault("last_5m_close_ms", 0)
                d.setdefault("last_15m_close_ms", 0)
                result.append(PendingOrder(**d))
            except Exception as e:
                log.warning("Bad pending JSON: %s", e)
        return result
    def update_pending_status(self, order_id, status):
        with self.lock:
            self.conn.execute("UPDATE pending_orders SET status=? WHERE id=?", (status, order_id))
            self.conn.commit()
    def save_position(self, p):
        with self.lock:
            payload = json_dumps(asdict(p))
            self.conn.execute("""INSERT OR REPLACE INTO trades(
                id,symbol,direction,status,entry_price,qty,notional,margin,initial_risk,
                stop_price,original_stop,target_price,atr,atr_candle_ms,score,probability,
                opened_ms,moved_to_be,target_protection,exit_price,exit_ms,exit_reason,
                gross_pnl,fees,net_pnl,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (p.id, p.symbol, p.direction, p.status, p.entry_price, p.qty, p.notional,
                 p.margin, p.initial_risk, p.stop_price, p.original_stop, p.target_price,
                 p.atr, p.atr_candle_ms, p.score, p.probability, p.opened_ms,
                 int(p.moved_to_be), int(p.target_protection), p.exit_price, p.exit_ms,
                 p.exit_reason, p.gross_pnl, p.fees, p.net_pnl, payload))
            self.conn.commit()
    def open_positions(self):
        with self.lock:
            rows = self.conn.execute("SELECT payload_json FROM trades WHERE status='OPEN'").fetchall()
        return [Position(**json.loads(r["payload_json"])) for r in rows]

DB = Database(DB_PATH)

def snapshot_state():
    risk = DB.get_risk()
    payload = {"version": VERSION, "schema_version": SCHEMA_VERSION, "updated": utc_now_iso(),
               "mode": MODE, "market_type": MARKET_TYPE, "capital": CAPITAL_USDT,
               "sizing_mode": SIZING_MODE, "position_size_pct": POSITION_SIZE_PCT,
               "entry_threshold": MIN_ENTRY_PROBABILITY, "entry_confirmation_mode": ENTRY_CONFIRMATION_MODE,
               "fixed_stop_loss": FIXED_STOP_LOSS, "disable_be": DISABLE_BREAK_EVEN,
               "disable_trailing": DISABLE_TRAILING, "disable_protection": DISABLE_TARGET_PROTECTION,
               "session": current_session(),
               "breakers": {"daily_loss_enabled": DAILY_LOSS_BREAKER_ENABLED,
                            "consecutive_loss_enabled": CONSECUTIVE_LOSS_BREAKER_ENABLED},
               "direction_filter": {"allow_long": ALLOW_LONG, "allow_short": ALLOW_SHORT},
               "ws_health": WS_HEALTH.status(), "risk_state": risk,
               "positions": [asdict(x) for x in DB.open_positions()],
               "pending_orders": [asdict(x) for x in DB.pending()],
               "symbols": SYMBOLS, "kline_limit": KLINE_LIMIT,
               "kline_refresh_seconds": KLINE_REFRESH_SECONDS}
    atomic_write_json(SNAPSHOT_PATH, payload)

class NewsManager:
    def __init__(self):
        self.items = []
        self.last_fetch = 0.0
        self.available = False
        self.lock = threading.Lock()
    def refresh_sync(self):
        if not NEWS_ENABLED: return
        if time.time() - self.last_fetch < NEWS_REFRESH_SECONDS: return
        try:
            req = urllib.request.Request(NEWS_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            if not isinstance(data, list): raise ValueError("unexpected format")
            parsed = []
            for x in data:
                title = str(x.get("title", "")).strip()
                country = str(x.get("country", "")).strip().upper()
                impact = str(x.get("impact", "")).strip().upper()
                date_raw = x.get("date") or x.get("datetime")
                if not date_raw: continue
                try:
                    dt = datetime.fromisoformat(str(date_raw).replace("Z", "+00:00"))
                    if dt.tzinfo is None: dt = dt.replace(tzinfo=UTC)
                    dt = dt.astimezone(UTC)
                except Exception: continue
                parsed.append({"title": title, "country": country, "impact": impact, "time": dt.timestamp()})
            with self.lock:
                self.items = parsed
                self.available = True
                self.last_fetch = time.time()
        except Exception as exc:
            with self.lock:
                self.available = False
                self.last_fetch = time.time()
                self.items = []
            log.warning("News refresh failed: %s", exc)
    async def refresh(self): await asyncio.to_thread(self.refresh_sync)
    def blocked(self):
        if not NEWS_ENABLED: return False, None
        with self.lock:
            if not self.available:
                if NEWS_FAIL_CLOSED: return True, {"title": "NEWS UNAVAILABLE"}
                return False, None
            t = time.time()
            before = NEWS_BEFORE_MINUTES * 60
            after = NEWS_AFTER_MINUTES * 60
            important = {"CPI", "FOMC", "FED", "NFP", "PCE", "GDP", "UNEMPLOYMENT", "POWELL"}
            for item in self.items:
                title = item["title"].upper()
                impact = item["impact"]
                event_t = item["time"]
                if not (impact in {"HIGH", "3"} or any(k in title for k in important)): continue
                if event_t - before <= t <= event_t + after: return True, item
        return False, None

NEWS = NewsManager()

class TelegramRateLimiter:
    def __init__(self, max_per_min):
        self.max_per_min = max_per_min
        self.timestamps = deque(maxlen=max_per_min)
        self.lock = threading.Lock()
    def acquire(self):
        now = time.time()
        with self.lock:
            while self.timestamps and now - self.timestamps[0] > 60.0:
                self.timestamps.popleft()
            if len(self.timestamps) >= self.max_per_min: return False
            self.timestamps.append(now)
            return True

TG_LIMITER = TelegramRateLimiter(TELEGRAM_RATE_LIMIT_PER_MIN)

def telegram_send_sync(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return False
    if not TG_LIMITER.acquire(): return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = urllib.parse.urlencode({"chat_id": TELEGRAM_CHAT_ID, "text": text,
                                       "disable_web_page_preview": "true"}).encode()
    req = urllib.request.Request(url, data=payload, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return r.status == 200
    except Exception as exc:
        log.warning("Telegram failed: %s", exc)
        return False

async def telegram_send(text): await asyncio.to_thread(telegram_send_sync, text)

def build_prediction(signal, quote, klines_15m):
    if signal.atr <= 0 or quote.mid <= 0: return None
    confidence = clamp(signal.entry_probability / 100.0, 0.0, 1.0)
    if confidence >= 0.75: horizon_hours, horizon_ar = 1.0, "ساعة إلى ساعتين"
    elif confidence >= 0.60: horizon_hours, horizon_ar = 2.0, "ساعتان إلى 4 ساعات"
    else: horizon_hours, horizon_ar = 4.0, "4 إلى 6 ساعات"
    bars_in_15m = max(1, int(horizon_hours * 60 / 15))
    scale = math.sqrt(bars_in_15m)
    rvol = realized_volatility(klines_15m, 30)
    rvol_factor = 1.0
    if rvol > 0: rvol_factor = clamp(rvol / 0.002, 0.5, 2.0)
    expected_move = signal.atr * scale * rvol_factor * (0.8 + confidence * 0.4)
    expected_pct = pct(expected_move, quote.mid)
    sigma = expected_move * 0.5
    price = quote.mid
    if signal.direction == "LONG":
        target_low = price + expected_move * 0.4
        target_high = price + expected_move
        s1_low, s1_high = price + expected_move - sigma, price + expected_move + sigma
        direction_ar = "صعود 📈"
    elif signal.direction == "SHORT":
        target_low = price - expected_move
        target_high = price - expected_move * 0.4
        s1_low, s1_high = price - expected_move - sigma, price - expected_move + sigma
        direction_ar = "هبوط 📉"
    else: return None
    return {"direction": signal.direction, "direction_ar": direction_ar,
            "confidence_pct": confidence * 100, "current_price": price,
            "horizon_hours": horizon_hours, "expected_move": expected_move,
            "expected_pct": expected_pct, "target_low": target_low, "target_high": target_high,
            "target_1sigma_low": s1_low, "target_1sigma_high": s1_high,
            "timeframe_ar": horizon_ar,
            "basis": f"ATR x sqrt({bars_in_15m}) x rvol({rvol_factor:.2f})"}

def score_signal_v2(states, direction, sweep, poi, has_fvg, has_ob, pd, momentum_ok, regime, session_score):
    required_passed = []
    bonuses = []
    want_trend = "BULLISH" if direction == "LONG" else "BEARISH"
    h4 = states["4h"]["trend"]
    if h4 == want_trend:
        required_passed.append("توافق 4H")
        base_probability = 58.0
    elif h4 == "NEUTRAL":
        required_passed.append("4H محايد")
        base_probability = 52.0
    else:
        required_passed.append("ضد 4H ⚠️")
        base_probability = 42.0
    if states["1h"]["trend"] == want_trend: required_passed.append("توافق 1H")
    if poi is None: return 0.0, [], []
    required_passed.append("POI")
    probability = base_probability
    if states["1h"]["trend"] == want_trend: probability += 5.0; bonuses.append("1H")
    if states["30m"]["trend"] == want_trend: probability += 3.0; bonuses.append("30m")
    if states["15m"]["trend"] == want_trend: probability += 3.0; bonuses.append("15m")
    if states["5m"]["trend"] == want_trend: probability += 2.0; bonuses.append("5m")
    st15 = states["15m"]
    structural_event = False
    if direction == "LONG":
        if st15.get("choch_up"): probability += 6.0; bonuses.append("CHOCH"); structural_event = True
        elif st15.get("bos_up"): probability += 5.0; bonuses.append("BOS"); structural_event = True
        if st15.get("mss_up") and not structural_event: probability += 3.0; bonuses.append("MSS")
    else:
        if st15.get("choch_down"): probability += 6.0; bonuses.append("CHOCH"); structural_event = True
        elif st15.get("bos_down"): probability += 5.0; bonuses.append("BOS"); structural_event = True
        if st15.get("mss_down") and not structural_event: probability += 3.0; bonuses.append("MSS")
    if sweep: probability += 5.0; bonuses.append("سويب")
    if has_fvg: probability += 3.0; bonuses.append("FVG")
    if has_ob: probability += 3.0; bonuses.append("OB")
    if direction == "LONG" and pd == "DISCOUNT": probability += 3.0; bonuses.append("خصم")
    elif direction == "SHORT" and pd == "PREMIUM": probability += 3.0; bonuses.append("علاوة")
    if momentum_ok: probability += 2.0; bonuses.append("زخم")
    if regime.kind == "TRENDING": probability += 4.0; bonuses.append("ترند")
    elif regime.kind == "VOLATILE": probability -= 3.0; bonuses.append("متقلب -3")
    if session_score >= 0.9: probability += 2.0; bonuses.append("جلسة قوية")
    return clamp(probability, 0.0, 100.0), required_passed, bonuses

def check_close_confirmation(symbol, direction, poi_low, poi_high, tf):
    candles = CANDLES.closed(symbol, tf)
    if len(candles) < 3: return False, 0, 0.0
    c = candles[-1]
    prev = candles[-2]
    atr_val = atr(candles, 14)
    min_body = atr_val * CLOSE_CONFIRMATION_BODY_ATR if atr_val > 0 else 0.0
    body = abs(c["close"] - c["open"])
    if body < min_body: return False, 0, 0.0
    if direction == "LONG":
        tapped = c["low"] <= poi_high
        closed_bullish = c["close"] > c["open"]
        above_zone = c["close"] >= poi_low
        confirmed = tapped and closed_bullish and above_zone
        if confirmed and CLOSE_CONFIRMATION_BREAK_PREV:
            confirmed = c["close"] > prev["close"]
        if confirmed: return True, c["close_time"], c["close"]
    else:
        tapped = c["high"] >= poi_low
        closed_bearish = c["close"] < c["open"]
        below_zone = c["close"] <= poi_high
        confirmed = tapped and closed_bearish and below_zone
        if confirmed and CLOSE_CONFIRMATION_BREAK_PREV:
            confirmed = c["close"] < prev["close"]
        if confirmed: return True, c["close_time"], c["close"]
    return False, 0, 0.0

def pairs_are_correlated():
    if not CORRELATION_FILTER_ENABLED: return False
    if len(SYMBOLS) < 2: return False
    try:
        a = CANDLES.closed(SYMBOLS[0], EXECUTION_TF)[-CORRELATION_LOOKBACK:]
        b = CANDLES.closed(SYMBOLS[1], EXECUTION_TF)[-CORRELATION_LOOKBACK:]
    except Exception: return False
    n = min(len(a), len(b))
    if n < 10: return False
    ra, rb = [], []
    for i in range(1, n):
        if a[i-1]["close"] > 0 and b[i-1]["close"] > 0:
            ra.append((a[i]["close"] - a[i-1]["close"]) / a[i-1]["close"])
            rb.append((b[i]["close"] - b[i-1]["close"]) / b[i-1]["close"])
    if len(ra) < 5: return False
    return abs(pearson_correlation(ra, rb)) >= CORRELATION_THRESHOLD

class CandleCache:
    def __init__(self):
        self.data = {}
        self.last_refresh = {}
        self.last_processed_close = {}
        self.lock = threading.Lock()
    def get(self, symbol, tf):
        with self.lock: return list(self.data.get(symbol, {}).get(tf, []))
    def set(self, symbol, tf, candles):
        with self.lock:
            self.data.setdefault(symbol, {})[tf] = candles
            self.last_refresh[(symbol, tf)] = time.time()
    def due(self, symbol, tf):
        return time.time() - self.last_refresh.get((symbol, tf), 0) >= KLINE_REFRESH_SECONDS
    def closed(self, symbol, tf):
        arr = self.get(symbol, tf)
        if len(arr) < 2: return []
        now_server = now_ms() + SERVER_TIME_OFFSET_MS
        return [c for c in arr if c["close_time"] <= now_server - 50]
    def new_closed_candle(self, symbol, tf):
        arr = self.closed(symbol, tf)
        if not arr: return False, None
        last = arr[-1]
        key = (symbol, tf)
        previous = self.last_processed_close.get(key, 0)
        if last["close_time"] > previous:
            self.last_processed_close[key] = last["close_time"]
            return True, last
        return False, last

CANDLES = CandleCache()

async def refresh_klines():
    tasks, meta = [], []
    needed_tfs = set(TIMEFRAMES) | {"5m", EXECUTION_TF}
    now_server = now_ms() + SERVER_TIME_OFFSET_MS
    for symbol in SYMBOLS:
        for tf in needed_tfs:
            tf_ms = timeframe_seconds(tf) * 1000
            last_closed_ms = (now_server // tf_ms) * tf_ms
            arr = CANDLES.get(symbol, tf)
            if arr and len(arr) > 0:
                latest_in_cache = arr[-1]["close_time"]
                if latest_in_cache >= last_closed_ms - 60_000: continue
            if not CANDLES.due(symbol, tf): continue
            tasks.append(asyncio.to_thread(klines_sync, symbol, tf))
            meta.append((symbol, tf))
    if not tasks: return
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for (symbol, tf), result in zip(meta, results):
        if isinstance(result, Exception): continue
        if len(result) >= 50: CANDLES.set(symbol, tf, result)

def build_signal(symbol):
    q = QUOTES.get(symbol)
    if q is None: return None
    data = {tf: CANDLES.closed(symbol, tf) for tf in TIMEFRAMES}
    if any(len(data[tf]) < 80 for tf in TIMEFRAMES): return None
    a15 = atr(data["15m"], 14)
    if a15 <= 0: return None
    spread_ok, _ = spread_is_acceptable(q, a15)
    if not spread_ok: return None
    news_blocked, news_item = NEWS.blocked()
    if news_blocked:
        DB.log_event("NEWS_BLOCK", news_item or {}, symbol)
        return None
    if pairs_are_correlated() and len(DB.open_positions()) > 0:
        DB.log_event("CORRELATION_BLOCK", {}, symbol)
        return None
    regime = detect_regime(data["15m"], a15)
    session_score = session_liquidity_score()
    session_name = current_session()
    states = {tf: structure_state(data[tf]) for tf in TIMEFRAMES}
    directions = []
    if ALLOW_LONG: directions.append("LONG")
    if ALLOW_SHORT: directions.append("SHORT")
    best = None
    for direction in directions:
        sweep = detect_sweep(data["15m"], direction)
        fvg_list = find_fvgs(data["15m"], direction, symbol_id=symbol)
        ob_list = find_order_blocks(data["15m"], direction, symbol_id=symbol)
        entry_reference = q.ask if direction == "LONG" else q.bid
        poi = select_poi(data["15m"], direction, entry_reference, fvgs=fvg_list, obs=ob_list)
        has_fvg = any(z.low <= poi.high and z.high >= poi.low for z in fvg_list) if poi else False
        has_ob = any(z.low <= poi.high and z.high >= poi.low for z in ob_list) if poi else False
        pd = premium_discount(data["15m"])
        rv = rsi(data["15m"], 14)
        vr = volume_ratio(data["15m"], 20)
        momentum_ok = False
        if direction == "LONG":
            if 48 <= rv <= 72 or vr >= 1.10: momentum_ok = True
        else:
            if 28 <= rv <= 52 or vr >= 1.10: momentum_ok = True
        probability, required_passed, bonuses = score_signal_v2(
            states, direction, sweep, poi, has_fvg, has_ob, pd, momentum_ok, regime, session_score)
        if probability < MIN_ENTRY_PROBABILITY: continue
        if poi is None: continue
        if LIMIT_ENTRY_ENABLED and ENTRY_CONFIRMATION_MODE == "LIMIT":
            if LIMIT_POI_LOCATION == "DISTAL": entry = poi.low if direction == "LONG" else poi.high
            elif LIMIT_POI_LOCATION == "MID": entry = poi.midpoint
            else: entry = poi.high if direction == "LONG" else poi.low
        else:
            entry = q.ask if direction == "LONG" else q.bid
        if direction == "LONG":
            stop = min(poi.low, entry - SL_ATR_MULT * a15)
            stop -= 0.05 * a15
            risk_distance = entry - stop
            target = entry + TP_R_MULT * risk_distance
        else:
            stop = max(poi.high, entry + SL_ATR_MULT * a15)
            stop += 0.05 * a15
            risk_distance = stop - entry
            target = entry - TP_R_MULT * risk_distance
        if risk_distance <= 0: continue
        rr = abs(target - entry) / risk_distance
        if rr < MIN_RR: continue
        sig = Signal(symbol=symbol, direction=direction, score=probability,
                     entry_probability=probability, entry_price=entry, stop_price=stop,
                     target_price=target, risk_distance=risk_distance, rr=rr, atr=a15,
                     spread=q.spread, spread_pct=q.spread_pct,
                     bias_4h=states["4h"]["trend"], structure_1h=states["1h"]["trend"],
                     structure_15m=states["15m"]["trend"], confirmation_5m=states["5m"]["trend"],
                     regime=regime.kind, session=session_name,
                     bos=bool(states["15m"]["bos"]), choch=bool(states["15m"]["choch"]),
                     mss=bool(states["15m"]["mss"]), liquidity_sweep=sweep,
                     fvg=has_fvg, order_block=has_ob, premium_discount=pd,
                     poi_kind=poi.kind, poi_low=poi.low, poi_high=poi.high,
                     reasons=required_passed + bonuses,
                     required_passed=required_passed, bonuses=bonuses)
        sig.prediction = build_prediction(sig, q, data["15m"])
        if best is None or sig.entry_probability > best.entry_probability: best = sig
    return best

def daily_loss_exceeded():
    if not DAILY_LOSS_BREAKER_ENABLED: return False
    risk = DB.reset_day_if_needed()
    if risk["day_start_equity"] <= 0: return False
    loss_pct = (risk["day_start_equity"] - risk["equity"]) / risk["day_start_equity"] * 100.0
    return loss_pct >= MAX_DAILY_LOSS_PCT

def risk_allows(symbol):
    risk = DB.get_risk()
    if risk["equity"] < MIN_EQUITY_TO_TRADE: return False, "الرصيد منخفض"
    if daily_loss_exceeded(): return False, "قاطع الخسارة اليومية"
    if CONSECUTIVE_LOSS_BREAKER_ENABLED and risk["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
        return False, "قاطع الخسائر المتتالية"
    positions = DB.open_positions()
    if len(positions) >= MAX_POSITIONS: return False, "الحد الأقصى للصفقات"
    if MAX_ONE_PER_SYMBOL and any(p.symbol == symbol for p in positions): return False, "صفقة موجودة"
    last = int(risk["last_trade_ms"])
    if last > 0:
        cooldown_ms = COOLDOWN_CANDLES * timeframe_seconds(EXECUTION_TF) * 1000
        if now_ms() - last < cooldown_ms: return False, "تهدئة"
    return True, "ok"

def pending_exists(symbol, direction):
    return any(x.symbol == symbol and x.direction == direction for x in DB.pending())

def create_pending_from_signal(signal, rules):
    allowed, reason = risk_allows(signal.symbol)
    if not allowed:
        DB.log_event("RISK_BLOCK", {"reason": reason}, signal.symbol)
        return None
    if pending_exists(signal.symbol, signal.direction): return None
    entry = normalize_price(signal.entry_price, rules, signal.direction, "ENTRY")
    stop = normalize_price(signal.stop_price, rules, signal.direction, "STOP")
    target = normalize_price(signal.target_price, rules, signal.direction, "TARGET")
    if signal.direction == "LONG" and not (stop < entry < target): return None
    if signal.direction == "SHORT" and not (target < entry < stop): return None
    if ENTRY_CONFIRMATION_MODE == "LIMIT":
        q = QUOTES.get(signal.symbol)
        if q is None: return None
        if signal.direction == "LONG":
            if entry >= q.ask:
                DB.log_event("ENTRY_INVALID", {"reason": "LONG wrong side"}, signal.symbol)
                return None
            dist_pct = (q.ask - entry) / q.ask * 100.0
            if dist_pct < MIN_ENTRY_DISTANCE_PCT: return None
        else:
            if entry <= q.bid:
                DB.log_event("ENTRY_INVALID", {"reason": "SHORT wrong side"}, signal.symbol)
                return None
            dist_pct = (entry - q.bid) / q.bid * 100.0
            if dist_pct < MIN_ENTRY_DISTANCE_PCT: return None
    risk = DB.get_risk()
    sizing = calculate_qty(risk["equity"], entry, stop, signal.direction, rules,
                            initial_equity=risk.get("initial_equity", risk["equity"]))
    if sizing["qty"] <= 0:
        DB.log_event("SIZE_BLOCK", {}, signal.symbol)
        return None
    arr = CANDLES.closed(signal.symbol, EXECUTION_TF)
    candle_ms = arr[-1]["close_time"] if arr else now_ms()
    created = now_ms()
    order = PendingOrder(id=f"PO-{signal.symbol}-{created}", symbol=signal.symbol,
                         direction=signal.direction, entry_price=entry, stop_price=stop,
                         target_price=target, risk_distance=abs(entry - stop), qty=sizing["qty"],
                         notional=sizing["notional"], margin=sizing["margin"], score=signal.score,
                         probability=signal.entry_probability, atr=signal.atr, created_ms=created,
                         created_candle_ms=candle_ms, expiry_ms=created + LIMIT_ORDER_EXPIRY_MINUTES * 60_000,
                         expiry_candle_ms=candle_ms + LIMIT_ORDER_EXPIRY_CANDLES * timeframe_seconds(EXECUTION_TF) * 1000,
                         poi_kind=signal.poi_kind, poi_low=signal.poi_low, poi_high=signal.poi_high,
                         prediction=signal.prediction, confirmation_mode=ENTRY_CONFIRMATION_MODE)
    DB.save_pending(order)
    DB.log_event("PENDING_CREATED", asdict(order), signal.symbol)
    return order

def pending_touched(order, q):
    if order.direction == "LONG": return q.ask <= order.entry_price
    return q.bid >= order.entry_price

def pending_expired(order):
    if now_ms() >= order.expiry_ms: return True
    arr = CANDLES.closed(order.symbol, EXECUTION_TF)
    if arr and arr[-1]["close_time"] >= order.expiry_candle_ms: return True
    return False

def activate_pending(order, q):
    if pending_expired(order):
        DB.update_pending_status(order.id, "EXPIRED")
        DB.log_event("PENDING_EXPIRED", asdict(order), order.symbol)
        return None
    if not pending_touched(order, q): return None
    fill = order.entry_price
    p = Position(id=f"POS-{order.symbol}-{now_ms()}", symbol=order.symbol,
                 direction=order.direction, entry_price=fill, qty=order.qty,
                 notional=order.qty * fill, margin=order.margin,
                 initial_risk=order.qty * order.risk_distance, stop_price=order.stop_price,
                 original_stop=order.stop_price, target_price=order.target_price, atr=order.atr,
                 atr_candle_ms=order.created_candle_ms, score=order.score,
                 probability=order.probability, opened_ms=now_ms(), prediction=order.prediction)
    DB.update_pending_status(order.id, "FILLED")
    DB.save_position(p)
    DB.update_risk(last_trade_ms=now_ms())
    DB.log_event("POSITION_OPENED", asdict(p), p.symbol)
    return p

def current_progress(p, q):
    if p.initial_risk <= 0: return 0.0
    if p.direction == "LONG":
        denom = p.target_price - p.entry_price
        if denom <= 0: return 0.0
        return (q.bid - p.entry_price) / denom
    else:
        denom = p.entry_price - p.target_price
        if denom <= 0: return 0.0
        return (p.entry_price - q.ask) / denom

def paper_exit_price(p, q):
    price = q.bid if p.direction == "LONG" else q.ask
    slip = EXIT_SLIPPAGE_PCT / 100.0
    if p.direction == "LONG":
        if price <= p.stop_price: return p.stop_price * (1.0 - slip), "وقف الخسارة"
        if price >= p.target_price: return p.target_price * (1.0 - slip), "الهدف"
    else:
        if price >= p.stop_price: return p.stop_price * (1.0 + slip), "وقف الخسارة"
        if price <= p.target_price: return p.target_price * (1.0 + slip), "الهدف"
    return None

def refresh_position_atr(p, klines_15m):
    if not klines_15m: return
    last_ct = klines_15m[-1]["close_time"]
    candles_elapsed = int((last_ct - p.atr_candle_ms) / (15 * 60 * 1000))
    if candles_elapsed >= ATR_REFRESH_CANDLES:
        new_atr = atr(klines_15m, 14)
        if new_atr > 0:
            p.atr = new_atr
            p.atr_candle_ms = last_ct

def update_position_levels(p, q, klines_15m=None):
    """
    ✅ V14.6: With FIXED_STOP_LOSS=true, stop NEVER moves.
    All dynamic adjustments (BE, trailing, protection) can be disabled.
    """
    if klines_15m:
        refresh_position_atr(p, klines_15m)
    progress = current_progress(p, q)

    if not DISABLE_BREAK_EVEN:
        if progress >= BE_R_MULT / max(TP_R_MULT, 1.0) and not p.moved_to_be:
            p.stop_price = p.entry_price
            p.moved_to_be = True
            DB.log_event("BREAK_EVEN", asdict(p), p.symbol)

    if not DISABLE_TARGET_PROTECTION:
        if progress >= TARGET_PROTECTION_PROGRESS and not p.target_protection:
            p.target_protection = True
            if p.direction == "LONG":
                protected = p.entry_price + 0.50 * (p.target_price - p.entry_price)
                p.stop_price = max(p.stop_price, protected)
            else:
                protected = p.entry_price - 0.50 * (p.entry_price - p.target_price)
                p.stop_price = min(p.stop_price, protected)
            DB.log_event("TARGET_PROTECTION", asdict(p), p.symbol)

    if not DISABLE_TRAILING and p.atr > 0:
        if TRAILING_STYLE == "CHANDELIER":
            if klines_15m and len(klines_15m) >= 20:
                window = klines_15m[-20:]
                if p.direction == "LONG":
                    hh = max(c["high"] for c in window)
                    candidate = hh - CHANDELIER_MULT * p.atr
                    if candidate > p.stop_price: p.stop_price = min(candidate, q.bid)
                else:
                    ll = min(c["low"] for c in window)
                    candidate = ll + CHANDELIER_MULT * p.atr
                    if candidate < p.stop_price: p.stop_price = max(candidate, q.ask)
        else:
            if p.direction == "LONG":
                candidate = q.bid - TRAIL_ATR_MULT * p.atr
                if candidate > p.stop_price: p.stop_price = min(candidate, q.bid)
            else:
                candidate = q.ask + TRAIL_ATR_MULT * p.atr
                if candidate < p.stop_price: p.stop_price = max(candidate, q.ask)

    # ✅ V14.6: Force stop back to original if FIXED_STOP_LOSS mode
    if FIXED_STOP_LOSS:
        p.stop_price = p.original_stop

def close_position(p, exit_price, reason):
    p.exit_price = exit_price
    p.exit_ms = now_ms()
    p.exit_reason = reason
    p.status = "CLOSED"
    if p.direction == "LONG": p.gross_pnl = (exit_price - p.entry_price) * p.qty
    else: p.gross_pnl = (p.entry_price - exit_price) * p.qty
    p.fees = estimate_fees(p.entry_price * p.qty, exit_price * p.qty)
    p.net_pnl = p.gross_pnl - p.fees
    DB.save_position(p)
    risk = DB.get_risk()
    new_equity = risk["equity"] + p.net_pnl
    streak = risk["consecutive_losses"] + 1 if p.net_pnl < 0 else 0
    DB.update_risk(equity=new_equity, consecutive_losses=streak, last_trade_ms=now_ms())
    DB.log_event("POSITION_CLOSED", asdict(p), p.symbol)
    try:
        with JOURNAL_PATH.open("a", encoding="utf-8") as f:
            f.write(json_dumps(asdict(p)) + "\n")
    except Exception: pass
    return new_equity

async def manage_positions():
    positions = DB.open_positions()
    if not positions: return
    for p in positions:
        q = QUOTES.get(p.symbol)
        if q is None or (now_ms() - q.ts_ms) > int(MAX_QUOTE_AGE_SECONDS * 1000):
            try:
                q = await asyncio.to_thread(book_ticker_sync, p.symbol)
                QUOTES.set(q)
            except Exception: continue
        klines_15m = CANDLES.closed(p.symbol, EXECUTION_TF)
        update_position_levels(p, q, klines_15m)
        exit_data = paper_exit_price(p, q)
        if exit_data:
            price, reason = exit_data
            new_equity = close_position(p, price, reason)
            emoji = "🟢" if p.net_pnl > 0 else "🔴"
            await telegram_send(
                f"{emoji} إغلاق الصفقة\n━━━━━━━━━━━━━━━━━━━━\n"
                f"العملة: {p.symbol}\nالاتجاه: {dir_ar(p.direction)}\n"
                f"السبب: {reason}\n━━━━━━━━━━━━━━━━━━━━\n"
                f"الدخول: {fmt_price(p.symbol, p.entry_price)}\n"
                f"الخروج: {fmt_price(p.symbol, price)}\n"
                f"صافي: {p.net_pnl:+.4f} USDT\nالرصيد: {new_equity:.4f} USDT")
        else:
            DB.save_position(p)

async def manage_pending():
    pending = DB.pending()
    if not pending: return
    for order in pending:
        q = QUOTES.get(order.symbol)
        if q is None or (now_ms() - q.ts_ms) > int(MAX_QUOTE_AGE_SECONDS * 1000):
            try:
                q = await asyncio.to_thread(book_ticker_sync, order.symbol)
                QUOTES.set(q)
            except Exception: continue
        if pending_expired(order):
            DB.update_pending_status(order.id, "EXPIRED")
            DB.log_event("PENDING_EXPIRED", asdict(order), order.symbol)
            continue
        mode = order.confirmation_mode
        if mode != "LIMIT":
            confirmed = False
            if mode in ("CLOSE_5M", "CLOSE_5M_OR_15M"):
                ok, ct, cp = check_close_confirmation(order.symbol, order.direction,
                                                       order.poi_low, order.poi_high, "5m")
                if ok and ct > order.last_5m_close_ms:
                    order.last_5m_close_ms = ct
                    confirmed = True
            if mode == "CLOSE_15M":
                ok, ct, cp = check_close_confirmation(order.symbol, order.direction,
                                                       order.poi_low, order.poi_high, "15m")
                if ok and ct > order.last_15m_close_ms:
                    order.last_15m_close_ms = ct
                    confirmed = True
            if mode == "CLOSE_5M_OR_15M" and not confirmed:
                ok, ct, cp = check_close_confirmation(order.symbol, order.direction,
                                                       order.poi_low, order.poi_high, "15m")
                if ok and ct > order.last_15m_close_ms:
                    order.last_15m_close_ms = ct
                    confirmed = True
            if mode == "CLOSE_5M_AND_15M":
                ok5, ct5, _ = check_close_confirmation(order.symbol, order.direction,
                                                        order.poi_low, order.poi_high, "5m")
                ok15, ct15, _ = check_close_confirmation(order.symbol, order.direction,
                                                          order.poi_low, order.poi_high, "15m")
                if ok5 and ok15:
                    latest_ct = max(ct5, ct15)
                    if latest_ct > max(order.last_5m_close_ms, order.last_15m_close_ms):
                        order.last_5m_close_ms = ct5
                        order.last_15m_close_ms = ct15
                        confirmed = True
            if confirmed:
                fill = q.ask if order.direction == "LONG" else q.bid
                new_risk = abs(fill - order.stop_price)
                if new_risk <= 0:
                    DB.update_pending_status(order.id, "CANCELED")
                    continue
                if order.direction == "LONG": new_target = fill + TP_R_MULT * new_risk
                else: new_target = fill - TP_R_MULT * new_risk
                p = Position(id=f"POS-{order.symbol}-{now_ms()}", symbol=order.symbol,
                             direction=order.direction, entry_price=fill, qty=order.qty,
                             notional=order.qty * fill, margin=order.notional / max(LEVERAGE, 1.0),
                             initial_risk=order.qty * new_risk, stop_price=order.stop_price,
                             original_stop=order.stop_price, target_price=new_target,
                             atr=order.atr, atr_candle_ms=order.created_candle_ms,
                             score=order.score, probability=order.probability,
                             opened_ms=now_ms(), prediction=order.prediction)
                DB.update_pending_status(order.id, "FILLED")
                DB.save_position(p)
                DB.update_risk(last_trade_ms=now_ms())
                DB.log_event("POSITION_OPENED_CLOSE_CONFIRM", asdict(p), order.symbol)
                msg = (f"🟢 دخول بتأكيد إغلاق الشمعة ({mode})\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"العملة: {p.symbol}\nالاتجاه: {dir_ar(p.direction)}\n"
                       f"وضع التأكيد: {mode}\nالدخول: {fmt_price(p.symbol, p.entry_price)}\n"
                       f"وقف الخسارة: {fmt_price(p.symbol, p.stop_price)}\n"
                       f"هدف الربح: {fmt_price(p.symbol, p.target_price)}\n"
                       f"الكمية: {p.qty:.8f}\nالـNotional: {p.notional:.4f} USDT\n"
                       f"التوافق: {p.score:.1f}/100")
                if p.prediction:
                    msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n🔮 التوقع:\n"
                            f"   {p.prediction['direction_ar']} | ثقة {p.prediction['confidence_pct']:.0f}%\n"
                            f"   الحركة: {p.prediction['expected_pct']:.2f}%")
                await telegram_send(msg)
            else:
                DB.save_pending(order)
            continue
        if pending_touched(order, q):
            p = activate_pending(order, q)
            if p:
                msg = ("🟢 تم تنفيذ الدخول (ورقي - LIMIT)\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"العملة: {p.symbol}\nالاتجاه: {dir_ar(p.direction)}\n"
                       f"الدخول: {fmt_price(p.symbol, p.entry_price)}\n"
                       f"وقف: {fmt_price(p.symbol, p.stop_price)}\n"
                       f"هدف: {fmt_price(p.symbol, p.target_price)}\n"
                       f"الكمية: {p.qty:.8f}\nالـNotional: {p.notional:.4f} USDT\n"
                       f"التوافق: {p.score:.1f}/100")
                if p.prediction:
                    msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n🔮 التوقع:\n"
                            f"   {p.prediction['direction_ar']} | ثقة {p.prediction['confidence_pct']:.0f}%\n"
                            f"   الحركة: {p.prediction['expected_pct']:.2f}%")
                await telegram_send(msg)

async def process_symbol(symbol):
    try:
        sig = build_signal(symbol)
        if sig is None: return
        rules = RULES.get(symbol)
        if rules is None: return
        order = create_pending_from_signal(sig, rules)
        if order:
            msg = ("🟡 أمر معلق ورقي\n━━━━━━━━━━━━━━━━━━━━\n"
                   f"العملة: {symbol}\nالاتجاه: {dir_ar(sig.direction)}\n"
                   f"النظام: {sig.regime} | الجلسة: {sig.session}\n"
                   f"وضع الدخول: {ENTRY_CONFIRMATION_MODE}\nPOI: {sig.poi_kind}\n"
                   f"الدخول المرجعي: {fmt_price(symbol, order.entry_price)}\n"
                   f"الوقف: {fmt_price(symbol, order.stop_price)}\n"
                   f"الهدف: {fmt_price(symbol, order.target_price)}\n"
                   f"RR: 1:{sig.rr:.2f}\nالكمية: {order.qty:.8f}\n"
                   f"الـNotional: {order.notional:.4f} USDT\n"
                   f"التوافق: {sig.score:.1f}/100\n"
                   f"لازم: {', '.join(sig.required_passed)}\n"
                   f"مكافآت: {', '.join(sig.bonuses[:6])}")
            if sig.prediction:
                msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n🔮 التوقع:\n"
                        f"   {sig.prediction['direction_ar']} | ثقة {sig.prediction['confidence_pct']:.0f}%\n"
                        f"   الحركة: {sig.prediction['expected_pct']:.2f}%\n"
                        f"   النطاق: {fmt_price(symbol, sig.prediction['target_low'])} → "
                        f"{fmt_price(symbol, sig.prediction['target_high'])}\n"
                        f"   الإطار: {sig.prediction['timeframe_ar']}")
            await telegram_send(msg)
    except Exception as exc:
        log.exception("Signal failed for %s: %s", symbol, exc)
        DB.log_event("SIGNAL_ERROR", {"error": str(exc)}, symbol)

LAST_BROADCAST = 0.0

async def broadcast_status():
    global LAST_BROADCAST
    if time.time() - LAST_BROADCAST < BROADCAST_INTERVAL_SECONDS: return
    LAST_BROADCAST = time.time()
    risk = DB.get_risk()
    positions = DB.open_positions()
    pending = DB.pending()
    trades_count, wr, aw, al = closed_stats()
    sizing_line = (f"حجم الصفقة: {POSITION_SIZE_PCT:.0f}% من الرصيد"
                   if SIZING_MODE == "FIXED_NOTIONAL"
                   else f"المخاطرة/صفقة: {RISK_PER_TRADE_PCT:.2f}%")
    daily_status = "مفعّل" if DAILY_LOSS_BREAKER_ENABLED else "معطّل ⛔"
    streak_status = "مفعّل" if CONSECUTIVE_LOSS_BREAKER_ENABLED else "معطّل ⛔"
    stop_status = "ثابت 🔒" if FIXED_STOP_LOSS else "متحرك 🔄"
    lines = ["📊 تقرير محرك BTC & PAXG V14.6", "━━━━━━━━━━━━━━━━━━━━",
             f"الرصيد: {risk['equity']:.4f} USDT", f"الجلسة: {current_session()}",
             sizing_line, f"عتبة الدخول: {MIN_ENTRY_PROBABILITY:.0f}%",
             f"وضع الدخول: {ENTRY_CONFIRMATION_MODE}",
             f"الوقف: {stop_status} ({SL_ATR_MULT:.1f} ATR)",
             f"الهدف: {TP_R_MULT:.1f}R",
             f"مفتوحة: {len(positions)}/{MAX_POSITIONS} | معلقة: {len(pending)}",
             f"خسائر متتالية: {risk['consecutive_losses']} (قاطع: {streak_status})",
             f"قاطع الخسارة اليومية: {daily_status}"]
    if trades_count >= 10:
        lines.append(f"📈 إحصائيات: {trades_count} صفقة | WR {wr*100:.1f}% | avgW {aw:.2f}R | avgL {al:.2f}R")
    for p in positions:
        q = QUOTES.get(p.symbol)
        if q:
            progress = current_progress(p, q) * 100
            side_price = q.bid if p.direction == "LONG" else q.ask
            lines.append(f"{p.symbol} {dir_ar(p.direction)} | "
                         f"{fmt_price(p.symbol, p.entry_price)} → {fmt_price(p.symbol, side_price)} | {progress:.1f}%")
    await telegram_send("\n".join(lines))

RULES = {}

async def initialize():
    global SYMBOLS
    log.info("%s starting...", APP_NAME)
    log.info("Mode=%s Market=%s Symbols=%s", MODE, MARKET_TYPE, SYMBOLS)
    log.info("Sizing=%s (%.1f%%) | Entry Mode=%s | Threshold=%.1f%%",
             SIZING_MODE, POSITION_SIZE_PCT, ENTRY_CONFIRMATION_MODE, MIN_ENTRY_PROBABILITY)
    log.info("KLINE_LIMIT=%d | KLINE_REFRESH_SECONDS=%.0f", KLINE_LIMIT, KLINE_REFRESH_SECONDS)
    log.info("SL=%.2f ATR | TP=%.2f R | FixedStop=%s | NoBE=%s | NoTrail=%s | NoProt=%s",
             SL_ATR_MULT, TP_R_MULT, FIXED_STOP_LOSS, DISABLE_BREAK_EVEN,
             DISABLE_TRAILING, DISABLE_TARGET_PROTECTION)
    log.info("Breakers: daily=%s, consecutive=%s", DAILY_LOSS_BREAKER_ENABLED, CONSECUTIVE_LOSS_BREAKER_ENABLED)
    log.info("Directions: LONG=%s, SHORT=%s", ALLOW_LONG, ALLOW_SHORT)
    sync_server_time()
    rules = await asyncio.to_thread(exchange_info_sync)
    available = []
    for symbol in SYMBOLS:
        if symbol in rules:
            available.append(symbol)
            RULES[symbol] = rules[symbol]
            DB.upsert_rules(rules[symbol])
        else:
            log.warning("%s unavailable on %s — skipping", symbol, MARKET_TYPE)
    if not available: raise RuntimeError(f"No tradable symbols on {MARKET_TYPE}")
    SYMBOLS = available
    await refresh_klines()
    await refresh_quotes_if_needed()
    await NEWS.refresh()
    start_websockets()
    snapshot_state()
    sizing_line = (f"حجم الصفقة: {POSITION_SIZE_PCT:.0f}% من الرصيد (ثابت)"
                   if SIZING_MODE == "FIXED_NOTIONAL"
                   else f"المخاطرة/صفقة: {RISK_PER_TRADE_PCT:.2f}%")
    breaker_lines = (f"قاطع الخسارة اليومية: {'مفعّل' if DAILY_LOSS_BREAKER_ENABLED else 'معطّل ⛔'}\n"
                     f"قاطع الخسائر المتتالية: {'مفعّل' if CONSECUTIVE_LOSS_BREAKER_ENABLED else 'معطّل ⛔'}")
    stop_line = "ثابت 🔒" if FIXED_STOP_LOSS else "متحرك 🔄"
    await telegram_send(
        "🚀 محرك BTC & PAXG V14.6 — FIXED-STOP\n━━━━━━━━━━━━━━━━━━━━\n"
        "MTF SMC + Regime + Prediction + Close-Confirm + Fixed Stop\n"
        f"السوق: {MARKET_TYPE}\nالرموز: {', '.join(SYMBOLS)}\n"
        f"الرصيد الابتدائي: {CAPITAL_USDT} USDT\n"
        f"عتبة الدخول: {MIN_ENTRY_PROBABILITY:.0f}%\n"
        f"وضع الدخول: {ENTRY_CONFIRMATION_MODE}\n{sizing_line}\n{breaker_lines}\n"
        f"الوقف: {stop_line} ({SL_ATR_MULT:.1f} ATR)\n"
        f"الهدف: {TP_R_MULT:.1f}R\n"
        f"Break-Even: {'معطّل ⛔' if DISABLE_BREAK_EVEN else 'مفعّل'}\n"
        f"Trailing: {'معطّل ⛔' if DISABLE_TRAILING else 'مفعّل'}\n"
        f"حماية 85%: {'معطّلة ⛔' if DISABLE_TARGET_PROTECTION else 'مفعّلة'}\n"
        f"الجلسة الحالية: {current_session()}\n"
        f"KLINE_LIMIT={KLINE_LIMIT} | Refresh={KLINE_REFRESH_SECONDS:.0f}s\n"
        "⚠️ استهلاك بيانات منخفض (~200MB/يوم)")

async def engine_loop():
    last_server_sync = 0.0
    while not SHUTDOWN.is_set():
        cycle_start = time.monotonic()
        try:
            if time.time() - last_server_sync > 300:
                await asyncio.to_thread(sync_server_time)
                last_server_sync = time.time()
            await asyncio.gather(refresh_quotes_if_needed(), manage_pending(), manage_positions())
            await asyncio.gather(refresh_klines(), NEWS.refresh())
            changed_5m = False
            changed_15m = False
            for symbol in SYMBOLS:
                is_new_5m, _ = CANDLES.new_closed_candle(symbol, "5m")
                is_new_15m, _ = CANDLES.new_closed_candle(symbol, EXECUTION_TF)
                changed_5m = changed_5m or is_new_5m
                changed_15m = changed_15m or is_new_15m
            if changed_5m or changed_15m:
                await asyncio.gather(*(process_symbol(s) for s in SYMBOLS))
            await broadcast_status()
            snapshot_state()
        except Exception as exc:
            log.exception("Engine cycle error: %s", exc)
        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0.05, LOOP_INTERVAL_SECONDS - elapsed))

def request_shutdown(*_): SHUTDOWN.set()

async def _bootstrap():
    await initialize()
    await engine_loop()

def main():
    try: signal.signal(signal.SIGINT, request_shutdown)
    except Exception: pass
    try: signal.signal(signal.SIGTERM, request_shutdown)
    except Exception: pass
    try:
        asyncio.run(_bootstrap())
    except KeyboardInterrupt: pass
    finally:
        snapshot_state()
        log.info("%s stopped.", APP_NAME)

# ✅ V14.6.1: Health check server for Back4App
def start_health_server():
    """Minimal HTTP server so Back4App/Render thinks we're a web app."""
    import http.server
    import socketserver
    
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"BTC PAXG Bot is running")
        
        def log_message(self, format, *args):
            pass  # silence logs
    
    port = int(os.getenv("PORT", "8080"))
    try:
        with socketserver.TCPServer(("0.0.0.0", port), Handler) as httpd:
            log.info(f"Health check server on port {port}")
            httpd.serve_forever()
    except Exception as exc:
        log.error(f"Health server failed: {exc}")


def main():
    # Start health server in background thread
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    try: 
        signal.signal(signal.SIGINT, request_shutdown)
    except Exception: 
        pass
    try: 
        signal.signal(signal.SIGTERM, request_shutdown)
    except Exception: 
        pass
    try:
        asyncio.run(_bootstrap())
    except KeyboardInterrupt: 
        pass
    finally:
        snapshot_state()
        log.info("%s stopped.", APP_NAME)


if __name__ == "__main__":
    main()
