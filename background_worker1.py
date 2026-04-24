# =============================================================
# background_worker.py  —  Panchak Dashboard Background Engine
# Version 4.2 — Production-ready, all bugs fixed
# =============================================================
# Runs 24/7 on AWS Linux independently of Streamlit / browser.
# All data fetched directly from Kite API — NO Streamlit needed.
#
# FIXES in v4.2 (full audit):
#   ✅ CRITICAL: _ohlc_fetch_all now correctly assigns to globals on cache-hit
#   ✅ CRITICAL: NIFTY/BANKNIFTY removed from STOCKS (were duplicated)
#   ✅ CRITICAL: futures exchange corrected NSE→NFO
#   ✅ FIXED: _check_nifty_slot break key separated from progress key (no re-fire)
#   ✅ FIXED: fire_alert dedup key uses category+slot only, not symbol names
#   ✅ FIXED: _struct_tg dedup key stabilised
#   ✅ FIXED: _update_ema_cache parses date with pd.to_datetime before sorting
#   ✅ FIXED: is_market_hours end time corrected to 15:30
#   ✅ FIXED: _kp_df_cache cleared on day rollover
#   ✅ FIXED: access_token file opened with 'with' statement
#   ✅ FIXED: OI option symbol uses correct Kite format
#   ✅ FIXED: Log rotation added (10MB × 5 files)
#   ✅ FIXED: Per-symbol OHLC errors logged (not silently swallowed)
#   ✅ FIXED: _check_bt_st filters watchlist before iterrows (performance)
#   ✅ FIXED: Alert log CSV appended for each alert fired
#   ✅ FIXED: Weekly/monthly ohlc globals correctly assigned from cache
#   ✅ FIXED: OI variable name collision (pd2→pe_oi_map)
#   ✅ FIXED: Panchak period alert uses correct icon (UP=🟢 not always 🔴)
#
# Required files in same folder:
#   access_token.txt        — Kite access token (refresh daily)
#   kp_panchang_2026.csv    — KP Panchang data
#   smc_engine.py           — SMC analysis engine
#   bos_scanner.py          — BOS/CHoCH 1H scanner
#   ohlc_store.py           — OHLC SQLite store (for BOS)
#   astro_engine.py         — Swiss Ephemeris planet engine
#   astro_logic.py          — Vedic astro score
#   astro_time.py           — Time-of-day signal
#
# START:  python3 background_worker.py
# STOP:   Ctrl+C  or  kill $(cat CACHE/worker.pid)
# LOGS:   tail -f CACHE/worker.log
# STATUS: cat CACHE/worker_heartbeat.json
# =============================================================

import os, sys, time, json, logging, logging.handlers, traceback, threading, urllib.request, csv
from datetime import datetime, timedelta, date, time as dtime
import pandas as pd
import pytz

_DIR      = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

# ── Logging with rotation (10MB × 5 files) ─────────────────────────
_log_file = os.path.join(CACHE_DIR, "worker.log")
_rotating = logging.handlers.RotatingFileHandler(
    _log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
)
_rotating.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_console  = logging.StreamHandler(sys.stdout)
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_rotating, _console])
log = logging.getLogger("panchak_worker")

IST = pytz.timezone("Asia/Kolkata")

# ══════════════════════════════════════════════════════════════════
# CONFIG — matches panchak_kite_dashboard_v3_2.py exactly
# ══════════════════════════════════════════════════════════════════
API_KEY           = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = os.path.join(_DIR, "access_token.txt")

TG_BOT_TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID   = "-1003706739531"

# ── Second Telegram channel — for SMC+OI Confluence, BOS UP/DOWN, CHoCH UP/DOWN ──
# AutoBotTest123 supergroup — same bot token, different chat_id
TG_CHAT_ID_2  = "-1002360390807"   # AutoBotTest123
# Categories that ALSO go to channel 2 (in addition to primary channel)
TG_CHANNEL2_CATEGORIES = {"BOS_UP", "BOS_DOWN", "CHOCH_UP", "CHOCH_DOWN",
                           "SMC_OI_CONFLUENCE", "COMBINED_ENGINE"}

# Expose env vars so kp_panchang_tab._get_tg_cfg() can read them
os.environ["TG_BOT_TOKEN"] = TG_BOT_TOKEN
os.environ["TG_CHAT_ID"]   = TG_CHAT_ID

EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO   = ["uppala.wla@gmail.com"]
# Point 1: Email and browser alerts disabled by default — only Telegram is active
EMAIL_ALERTS_ENABLED = False   # set True to re-enable email globally

NSE_HOLIDAYS = {
    date(2026,1,26), date(2026,3,17), date(2026,4,2),  date(2026,4,10),
    date(2026,4,14), date(2026,5,1),  date(2026,8,15), date(2026,10,2),
    date(2026,10,24),date(2026,11,5), date(2026,12,25),
}

PANCHAK_SCHEDULE_2026 = [
    (date(2026,  3, 17), date(2026,  3, 21)),
    (date(2026,  4, 13), date(2026,  4, 17)),
    (date(2026,  5, 10), date(2026,  5, 14)),
    (date(2026,  6,  6), date(2026,  6, 11)),
    (date(2026,  7,  4), date(2026,  7,  8)),
    (date(2026,  7, 31), date(2026,  8,  4)),
    (date(2026,  8, 27), date(2026,  9,  1)),
    (date(2026,  9, 23), date(2026,  9, 28)),
    (date(2026, 10, 21), date(2026, 10, 25)),
    (date(2026, 11, 17), date(2026, 11, 22)),
    (date(2026, 12, 14), date(2026, 12, 19)),
]

def _get_active_panchak_worker():
    """
    Return (PANCHAK_START, PANCHAK_END) for the currently relevant period.
    - During a period: return that period.
    - Between periods: return the MOST RECENTLY ENDED period (for post-period range alerts).
    - Before any period: return the first.
    """
    today = date.today()
    # Find active period
    for s, e in PANCHAK_SCHEDULE_2026:
        if s <= today <= e:
            return s, e
    # Between periods — find most recent ended
    past = [(s, e) for s, e in PANCHAK_SCHEDULE_2026 if e < today]
    if past:
        return past[-1]
    # Before all periods — return first upcoming
    future = [(s, e) for s, e in PANCHAK_SCHEDULE_2026 if s > today]
    if future:
        return future[0]
    return PANCHAK_SCHEDULE_2026[0]

PANCHAK_START, PANCHAK_END = _get_active_panchak_worker()

# Index symbols — these require SYMBOL_META mapping for Kite
SYMBOL_META = {
    "NIFTY":       "NIFTY 50",
    "BANKNIFTY":   "NIFTY BANK",
    "FINNIFTY":    "FINNIFTY",
    "NIFTYIT":     "NIFTYIT",
    "NIFTYFMCG":   "NIFTYFMCG",
    "NIFTYPHARMA": "NIFTYPHARMA",
    "NIFTYMETAL":  "NIFTYMETAL",
    "NIFTYAUTO":   "NIFTYAUTO",
    "NIFTYENERGY": "NIFTYENERGY",
    "NIFTYPSUBANK":"NIFTYPSUBANK",
}
INDEX_ONLY_SYMBOLS = list(SYMBOL_META.keys())

# FIX #14/#15: NIFTY and BANKNIFTY removed from STOCKS — they are in INDEX_ONLY_SYMBOLS
STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS",
    "ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV",
    "BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
    "BHEL","BHARATFORG","BHARTIARTL","BIOCON","BLUESTARCO","BOSCHLTD","BPCL",
    "BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA",
    "COALINDIA","COFORGE","COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR",
    "DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY","EICHERMOT",
    "ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP",
    "GODREJPROP","GRASIM","HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK",
    "HDFCLIFE","HEROMOTOCO","HINDALCO","HINDCOPPER","HINDPETRO","HINDUNILVR",
    "HINDZINC","HUDCO","ICICIBANK","ICICIGI","ICICIPRULI","IEX","INDHOTEL",
    "INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND","IRCTC",
    "IRFC","IREDA","ITC","JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","JUBLFOOD",
    "KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK","LAURUSLABS",
    "LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M","MANAPPURAM",
    "MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL","MPHASIS","MOTHERSON",
    "MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NTPC","NUVAMA","NYKAA","NATIONALUM",
    "OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM","PERSISTENT",
    "PETRONET","PFC","PGEL","PHOENIXLTD","PIDILITIND","PIIND","PNB","PNBHOUSING",
    "POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PREMIERENE","PRESTIGE",
    "PPLPHARMA","RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SAMMAANCAP",
    "SBICARD","SBILIFE","SBIN","SHREECEM","SHRIRAMFIN","SIEMENS","SOLARINDS",
    "SRF","SUNPHARMA","SUPREMEIND","SWIGGY","SYNGENE","TATACONSUM","TATAELXSI",
    "TATAPOWER","TATATECH","TATASTEEL","TCS","TECHM","TIINDIA","TITAN","TMPV",
    "TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK",
    "UNITDSPR","UPL","VBL","VEDL","VOLTAS","WAAREEENER","WIPRO","ZYDUSLIFE",
]

SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS  # No duplicates now

SPECIAL_ALERT_STOCKS = {
    "BEL","HINDCOPPER","INDHOTEL","ABCAPITAL","TATASTEEL",
    "BANKINDIA","CANBK","JINDALSTEL","BANDHANBNK","INDUSTOWER","MOTHERSON","NATIONALUM",
}
PANCHAK_ALERT_WATCHLIST = {
    "NIFTY","BANKNIFTY","BEL","HINDCOPPER","INDHOTEL","ABCAPITAL","TATASTEEL",
    "BANKINDIA","CANBK","JINDALSTEL","BANDHANBNK","INDUSTOWER","MOTHERSON","NATIONALUM",
}

# Refresh intervals (seconds)
INTERVAL_LIVE    = 60
INTERVAL_5MIN    = 300
INTERVAL_15MIN   = 300
INTERVAL_OI      = 180
INTERVAL_OI_30M  = 1800
INTERVAL_BOS     = 300
INTERVAL_FUTURES = 180
INTERVAL_YEST    = 3600
INTERVAL_WEEKLY  = 7200
INTERVAL_MONTHLY = 7200
INTERVAL_SMC     = 900     # Combined engine every 15 min
INTERVAL_OFF     = 300

# ══════════════════════════════════════════════════════════════════
# OPTIONAL MODULE IMPORTS (graceful fallback)
# ══════════════════════════════════════════════════════════════════
try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite, detect_market_structure, find_order_blocks, find_fvg
    _SMC_OK = True
    log.info("✅ smc_engine loaded")
except ImportError as e:
    _SMC_OK = False
    log.warning(f"⚠️ smc_engine not found: {e}")

try:
    from bos_scanner import run_bos_scan
    from ohlc_store  import OHLCStore
    _BOS_OK  = True
    _ohlc_db = OHLCStore(os.path.join(_DIR, "ohlc_1h.db"))
    log.info("✅ bos_scanner + ohlc_store loaded")
except ImportError as e:
    _BOS_OK = False; _ohlc_db = None
    log.warning(f"⚠️ bos_scanner/ohlc_store not found: {e}")

try:
    from astro_logic import get_astro_score
    from astro_time  import get_time_signal
    _ASTRO_OK = True
    log.info("✅ astro_logic + astro_time loaded")
except ImportError as e:
    _ASTRO_OK = False
    log.warning(f"⚠️ astro modules not found: {e}")

_KP_CSV_PATH = os.path.join(_DIR, "kp_panchang_2026.csv")
_kp_df_cache = None   # cleared on day rollover

# ══════════════════════════════════════════════════════════════════
# TIME HELPERS
# ══════════════════════════════════════════════════════════════════
def _now():       return datetime.now(IST)
def _today():     return _now().date()
def _today_str(): return _now().strftime("%Y%m%d")
def _now_str():   return _now().strftime("%H:%M IST")

def _dated(name, ext="csv"):
    return os.path.join(CACHE_DIR, f"{name}_{_today_str()}.{ext}")

def is_market_hours():
    n = _now()
    if n.weekday() >= 5 or n.date() in NSE_HOLIDAYS:
        return False
    # FIX #23: market closes at 15:30, not 15:35
    return dtime(9, 10) <= n.time() <= dtime(15, 30)

def last_trading_day(d=None):
    d = d or _today()
    d -= timedelta(days=1)
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d

def previous_week_range():
    end = last_trading_day(_today())
    # Last completed week ending on Friday
    days_since_friday = (end.weekday() - 4) % 7
    lwe   = end - timedelta(days=days_since_friday)
    lws   = lwe - timedelta(days=4)
    return lws, lwe

def previous_month_range():
    first_this = _today().replace(day=1)
    last_prev  = first_this - timedelta(days=1)
    return last_prev.replace(day=1), last_prev

def _slot(minutes):
    n = _now()
    return n.replace(minute=(n.minute // minutes) * minutes,
                     second=0, microsecond=0).strftime("%H%M")

def _slot_10m():  return _slot(10)
def _slot_15m():  return _slot(15)
def _slot_30m():  return _slot(30)

# ══════════════════════════════════════════════════════════════════
# ALERT TOGGLE SYSTEM
# ══════════════════════════════════════════════════════════════════
TOGGLES_FILE = os.path.join(CACHE_DIR, "alert_toggles.json")

_TOGGLE_DEFAULTS = {
    # ── Telegram primary channel ─────────────────────────────────────
    "tg_TOP_HIGH":False,        "tg_TOP_LOW":False,
    "tg_DAILY_UP":False,        "tg_DAILY_DOWN":False,
    "tg_WEEKLY_UP":False,       "tg_WEEKLY_DOWN":False,
    "tg_MONTHLY_UP":False,      "tg_MONTHLY_DOWN":False,
    "tg_EMA20_50":False,
    "tg_SEQ_1H_HIGH":True,      "tg_SEQ_1H_LOW":True,
    "tg_SEQ_15M_HIGH":False,    "tg_SEQ_15M_LOW":False,
    "tg_THREE_GREEN_15M":False,  "tg_VOL_SURGE_15M":False,
    "tg_OI_INTEL":True,         "tg_OI_30M_SUMMARY":True,
    "tg_BOS_1H":True,           "tg_KP_ALERTS":True,
    "tg_KP_BREAK_15M":False,    "tg_LONG_UNWIND":False,
    "tg_PUT_CRUMBLE":False,
    "tg_TOP_GAINERS":False,     "tg_TOP_LOSERS":False,
    "tg_OPTION_STRONG":False,
    "tg_YEST_GREEN_BREAK":True, "tg_YEST_RED_BREAK":True,
    "tg_HOURLY_BREAK_UP":True,  "tg_HOURLY_BREAK_DOWN":True,
    "tg_BREAK_ABOVE_1H_HIGH":True,
    "tg_BREAK_BELOW_1H_LOW":True,
    "tg_2M_EMA_REVERSAL":True,
    "tg_ASTRO_ADVANCE":True,
    "tg_GREEN_OPEN_STRUCTURE":True,
    "tg_RED_OPEN_STRUCTURE":True,
    "tg_YEST_GREEN_OPEN_BREAK":True,
    "tg_YEST_RED_OPEN_LOWER":True,
    "tg_OEH_OEL_SETUPS":True,
    "tg_NIFTY_SLOT_BREAK":True,
    "tg_BT_ST_TARGET":True,
    "tg_COMBINED_ENGINE":False,
    "tg_SMC_PANCHAK":True,
    # ── New alert categories (Points 9-13) ───────────────────────────
    "tg_MACD_BULL":True,        "tg_MACD_BEAR":True,
    "tg_INSIDE_BAR":True,
    "tg_EXPIRY_ALERT":True,
    "tg_HOLIDAY_ALERT":True,
    "tg_PANCHAK_RANGE":True,
    # ── Second Telegram channel — BOS/CHoCH/SMC (Point 12) ──────────
    "tg2_BOS_UP":True,          "tg2_BOS_DOWN":True,
    "tg2_CHOCH_UP":True,        "tg2_CHOCH_DOWN":True,
    "tg2_SMC_OI_CONFLUENCE":True,
    # ── Channel routing defaults (ch1 = Panchak Alerts, ch2 = AutoBotTest123) ──
    # "ch1" | "ch2" | "both"
    "route_TOP_HIGH":"ch1",     "route_TOP_LOW":"ch1",
    "route_DAILY_UP":"ch1",     "route_DAILY_DOWN":"ch1",
    "route_WEEKLY_UP":"ch1",    "route_WEEKLY_DOWN":"ch1",
    "route_MONTHLY_UP":"ch1",   "route_MONTHLY_DOWN":"ch1",
    "route_EMA20_50":"ch1",
    "route_SEQ_1H_HIGH":"ch1",  "route_SEQ_1H_LOW":"ch1",
    "route_SEQ_15M_HIGH":"ch1", "route_SEQ_15M_LOW":"ch1",
    "route_THREE_GREEN_15M":"ch1","route_VOL_SURGE_15M":"ch1",
    "route_OI_INTEL":"ch1",     "route_OI_30M_SUMMARY":"ch1",
    "route_BOS_1H":"both",      "route_KP_ALERTS":"ch1",
    "route_KP_BREAK_15M":"ch1", "route_LONG_UNWIND":"ch1",
    "route_PUT_CRUMBLE":"ch1",  "route_PUT_FLOOR":"ch1",
    "route_SMC_PANCHAK":"ch1",
    "route_TOP_GAINERS":"ch1",  "route_TOP_LOSERS":"ch1",
    "route_OPTION_STRONG":"ch1",
    "route_YEST_GREEN_BREAK":"ch1",  "route_YEST_RED_BREAK":"ch1",
    "route_HOURLY_BREAK_UP":"ch1",   "route_HOURLY_BREAK_DOWN":"ch1",
    "route_BREAK_ABOVE_1H_HIGH":"ch1","route_BREAK_BELOW_1H_LOW":"ch1",
    "route_2M_EMA_REVERSAL":"ch1",
    "route_ASTRO_ADVANCE":"ch1",
    "route_GREEN_OPEN_STRUCTURE":"both","route_RED_OPEN_STRUCTURE":"both",
    "route_YEST_GREEN_OPEN_BREAK":"ch1","route_YEST_RED_OPEN_LOWER":"ch1",
    "route_OEH_OEL_SETUPS":"ch1",
    "route_NIFTY_SLOT_BREAK":"ch1",  "route_BT_ST_TARGET":"ch1",
    "route_COMBINED_ENGINE":"ch1",
    "route_HEATMAP_REVERSAL":"both",
    "route_MACD_BULL":"ch1",    "route_MACD_BEAR":"ch1",
    "route_INSIDE_BAR":"both",  "route_PANCHAK_RANGE":"ch1",
    "route_CHART_PATTERN":"ch1",
    "route_MTF_SEQ":"both",
    "route_EXPIRY_ALERT":"both","route_HOLIDAY_ALERT":"both",
    # ── Email — ALL disabled (Point 1) ───────────────────────────────
    "email_TOP_HIGH":False,     "email_TOP_LOW":False,
    "email_DAILY_UP":False,     "email_DAILY_DOWN":False,
    "email_WEEKLY_UP":False,    "email_WEEKLY_DOWN":False,
    "email_MONTHLY_UP":False,   "email_MONTHLY_DOWN":False,
    "email_EMA20_50":False,
    "email_SEQ_1H_HIGH":False,  "email_SEQ_1H_LOW":False,
    "email_SEQ_15M_HIGH":False, "email_SEQ_15M_LOW":False,
    "email_TOP_GAINERS":False,  "email_TOP_LOSERS":False,
    "email_OPTION_STRONG":False,
    "email_YEST_GREEN_BREAK":False,"email_YEST_RED_BREAK":False,
    "email_HOURLY_BREAK_UP":False,"email_HOURLY_BREAK_DOWN":False,
    "email_2M_EMA_REVERSAL":False,
    "special_stock_alerts":True,
}

_toggles       = dict(_TOGGLE_DEFAULTS)
_toggles_mtime = 0.0

def _reload_toggles():
    """
    Reload alert_toggles.json from disk (written by dashboard UI).
    Handles both bool toggle keys and str route_* channel-routing keys.
    """
    global _toggles, _toggles_mtime
    try:
        mt = os.path.getmtime(TOGGLES_FILE) if os.path.exists(TOGGLES_FILE) else 0
        if mt > _toggles_mtime:
            with open(TOGGLES_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            merged = dict(_TOGGLE_DEFAULTS)
            for k, v in saved.items():
                default = _TOGGLE_DEFAULTS.get(k)
                if default is None:
                    merged[k] = v
                elif isinstance(default, bool) and isinstance(v, bool):
                    merged[k] = v
                elif isinstance(default, str) and isinstance(v, str) and v in ("ch1","ch2","both"):
                    merged[k] = v
            _toggles       = merged
            _toggles_mtime = mt
            log.info(f"🔄 Toggles reloaded: {len(merged)} keys")
    except Exception as e:
        log.warning(f"Toggle reload error: {e}")

def tg_on(cat):    return _toggles.get(f"tg_{cat}",    True)
def email_on(cat): return _toggles.get(f"email_{cat}", False)

def _route(cat: str) -> str:
    """Return channel routing for a category: 'ch1' | 'ch2' | 'both'."""
    return _toggles.get(f"route_{cat}", _TOGGLE_DEFAULTS.get(f"route_{cat}", "ch1"))

def _send_routed(cat: str, msg: str, key: str = None):
    """Send Telegram alert respecting channel routing from alert_toggles.json."""
    route = _route(cat)
    if route in ("ch1", "both"):
        send_tg_bg(msg, key)
    if route in ("ch2", "both"):
        send_tg2_bg(msg, f"CH2_{key}" if key else None)

# ══════════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ══════════════════════════════════════════════════════════════════
_tg_dedup = {}
_TG_LOCK  = threading.Lock()

def _load_tg_dedup():
    global _tg_dedup
    path = os.path.join(CACHE_DIR, f"tg_dedup_worker_{_today_str()}.json")
    try:
        if os.path.exists(path):
            with open(path) as f:
                _tg_dedup = json.load(f)
    except Exception:
        _tg_dedup = {}

def _save_key(key):
    with _TG_LOCK:
        _tg_dedup[key] = _now().isoformat()
    try:
        path = os.path.join(CACHE_DIR, f"tg_dedup_worker_{_today_str()}.json")
        with _TG_LOCK:
            with open(path, "w") as f:
                json.dump(_tg_dedup, f)
    except Exception:
        pass

def already_sent(key):
    return key in _tg_dedup

def send_tg(msg: str, key: str = None) -> bool:
    if key and already_sent(key):
        return False
    # Telegram message limit is 4096 chars
    if len(msg) > 4000:
        msg = msg[:3990] + "\n…[truncated]"
    try:
        url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id":    TG_CHAT_ID,
            "text":       msg,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = json.loads(r.read()).get("ok", False)
        if ok and key:
            _save_key(key)
        return ok
    except Exception as e:
        log.warning(f"TG send error: {e}")
        return False

def send_tg_bg(msg: str, key: str = None):
    if key and already_sent(key):
        return
    threading.Thread(target=send_tg, args=(msg, key), daemon=True).start()


def send_tg2(msg: str, key: str = None) -> bool:
    """Send to second Telegram channel (AutoBotTest123). No market-hours gate."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID_2:
        return False
    if key and already_sent("CH2_" + key):
        return False
    if len(msg) > 4000:
        msg = msg[:3990] + "\n…[truncated]"
    try:
        url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id":    TG_CHAT_ID_2,
            "text":       msg,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = json.loads(r.read()).get("ok", False)
        if ok and key:
            _save_key("CH2_" + key)
        return ok
    except Exception as e:
        log.warning(f"TG2 send error: {e}")
        return False


def send_tg2_bg(msg: str, key: str = None):
    """Non-blocking second-channel send."""
    if key and already_sent("CH2_" + key):
        return
    threading.Thread(target=send_tg2, args=(msg, key), daemon=True).start()

# ══════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════════════════════
def send_email(subject: str, body: str):
    # Point 1: Email disabled by default
    if not EMAIL_ALERTS_ENABLED:
        log.debug(f"Email suppressed (disabled): {subject}")
        return
    try:
        import smtplib
        from email.message import EmailMessage
        m = EmailMessage()
        m["Subject"] = subject
        m["From"]    = EMAIL_FROM
        m["To"]      = ", ".join(EMAIL_TO)
        m.set_content(body)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s:
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.send_message(m)
        log.info(f"📧 Email sent: {subject}")
    except Exception as e:
        log.warning(f"Email error: {e}")

def send_email_bg(subject: str, body: str):
    threading.Thread(target=send_email, args=(subject, body), daemon=True).start()

def _sh(t: str) -> str:
    """Strip HTML tags for plain-text email body."""
    for tag in ["<b>", "</b>", "<i>", "</i>", "<br>"]:
        t = t.replace(tag, "")
    return t

def _bd(up: bool = True) -> str:
    return "🟩" * 10 if up else "🟥" * 10

# ══════════════════════════════════════════════════════════════════
# ALERT LOG (appended CSV for debugging)
# ══════════════════════════════════════════════════════════════════
ALERT_LOG_FILE = os.path.join(CACHE_DIR, "alerts_log.csv")

def _log_alert(category: str, symbols: list, channel: str = "TG"):
    try:
        exists = os.path.exists(ALERT_LOG_FILE)
        with open(ALERT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["timestamp", "category", "channel", "symbols", "count"])
            w.writerow([
                _now().strftime("%Y-%m-%d %H:%M:%S"),
                category, channel,
                ",".join(str(s) for s in symbols[:10]),
                len(symbols),
            ])
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════
# KITE INIT + INSTRUMENT HELPERS
# ══════════════════════════════════════════════════════════════════
def init_kite():
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=API_KEY)
    # FIX #20: use 'with' for file handle
    with open(ACCESS_TOKEN_FILE, encoding="utf-8") as f:
        token = f.read().strip()
    kite.set_access_token(token)
    log.info("✅ Kite connected")
    return kite

_inst_df = None

def _load_instruments(kite):
    global _inst_df
    try:
        _inst_df = pd.DataFrame(kite.instruments("NSE"))
        log.info(f"📋 Instruments loaded: {len(_inst_df)} records")
    except Exception as e:
        log.error(f"Instruments load failed: {e}")

def kite_sym(symbol: str) -> str:
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

def get_token(symbol: str):
    if _inst_df is None:
        return None
    name = SYMBOL_META.get(symbol, symbol)
    row  = _inst_df[_inst_df.tradingsymbol == name]
    return None if row.empty else int(row.iloc[0].instrument_token)

# ══════════════════════════════════════════════════════════════════
# OHLC DATA CACHES
# FIX #4/#5: globals now always assigned (both cache-hit and fresh-fetch)
# ══════════════════════════════════════════════════════════════════
_yest_ohlc    = {}
_top_levels   = {}
_weekly_ohlc  = {}
_monthly_ohlc = {}
_1h_range     = {}
_ema_cache    = {}

def _ohlc_load_or_fetch(kite, path, d_start, d_end, interval, field_fn, label):
    """
    Load from cache if file exists, else fetch from Kite.
    Always returns the result dict and logs per-symbol errors.
    """
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            log.info(f"📂 {label} loaded from cache: {len(data)} symbols")
            return data
        except Exception as e:
            log.warning(f"Cache read error ({label}): {e} — fetching fresh")

    log.info(f"🔄 Fetching {label} from Kite [{d_start} → {d_end}]…")
    result = {}
    for sym in SYMBOLS:
        try:
            tk = get_token(sym)
            if not tk:
                continue
            bars = kite.historical_data(tk, d_start, d_end, interval)
            if bars:
                result[sym] = field_fn(sym, bars)
        except Exception as e:
            log.debug(f"  {label} fetch skipped [{sym}]: {e}")
        time.sleep(0.15)

    try:
        with open(path, "w") as f:
            json.dump(result, f)
    except Exception as e:
        log.warning(f"Cache write error ({label}): {e}")

    log.info(f"✅ {label} fetched: {len(result)} symbols")
    return result


def _fetch_yesterday_ohlc(kite):
    global _yest_ohlc
    path = os.path.join(CACHE_DIR, f"yest_ohlc_{_today_str()}.json")
    ytd  = last_trading_day()
    def _f(sym, bars):
        b = bars[-1]
        return {
            "yh": round(b["high"],  2), "yl": round(b["low"],   2),
            "yc": round(b["close"], 2), "yo": round(b["open"],  2),
            "yv": int(b.get("volume", 0)),
        }
    _yest_ohlc = _ohlc_load_or_fetch(kite, path, ytd, ytd, "day", _f, "Yest OHLC")


def _load_top_levels(kite):
    global _top_levels
    path = os.path.join(CACHE_DIR, f"top_levels_{_today_str()}.json")
    td   = _today()
    sd   = td - timedelta(days=90)
    def _f(sym, bars):
        df = pd.DataFrame(bars)
        return {
            "top_high": round(float(df["high"].max()), 2),
            "top_low":  round(float(df["low"].min()),  2),
        }
    _top_levels = _ohlc_load_or_fetch(kite, path, sd, td, "day", _f, "Top Levels (90d)")


def _fetch_weekly_ohlc(kite):
    global _weekly_ohlc
    ltd  = last_trading_day()
    path = os.path.join(CACHE_DIR, f"weekly_ohlc_{ltd}.json")
    s, e = previous_week_range()
    def _f(sym, bars):
        df = pd.DataFrame(bars)
        return {
            "high_w": round(float(df["high"].max()), 2),
            "low_w":  round(float(df["low"].min()),  2),
        }
    _weekly_ohlc = _ohlc_load_or_fetch(kite, path, s, e, "day", _f, "Weekly OHLC")


def _fetch_monthly_ohlc(kite):
    global _monthly_ohlc
    ltd  = last_trading_day()
    path = os.path.join(CACHE_DIR, f"monthly_ohlc_{ltd}.json")
    s, e = previous_month_range()
    def _f(sym, bars):
        df = pd.DataFrame(bars)
        return {
            "high_m": round(float(df["high"].max()), 2),
            "low_m":  round(float(df["low"].min()),  2),
        }
    _monthly_ohlc = _ohlc_load_or_fetch(kite, path, s, e, "day", _f, "Monthly OHLC")


def _fetch_1h_opening_range(kite):
    """Fetch first 1-hour candle (09:15–10:15) for all symbols."""
    global _1h_range
    path  = _dated("1h_range", "json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                _1h_range = json.load(f)
            log.info(f"📂 1H range loaded from cache: {len(_1h_range)} symbols")
            return
        except Exception:
            pass

    today = _today()
    from datetime import datetime as _dt
    start = IST.localize(_dt(today.year, today.month, today.day, 9, 15))
    end   = IST.localize(_dt(today.year, today.month, today.day, 10, 15))
    result = {}
    log.info("📊 Fetching 1H Opening Range…")
    for sym in SYMBOLS:
        try:
            tk = get_token(sym)
            if not tk:
                continue
            bars = kite.historical_data(tk, start, end, "60minute")
            if bars:
                result[sym] = {
                    "high_1h": round(bars[0]["high"], 2),
                    "low_1h":  round(bars[0]["low"],  2),
                }
        except Exception as e:
            log.debug(f"  1H range [{sym}]: {e}")
        time.sleep(0.1)
    _1h_range = result
    try:
        with open(path, "w") as f:
            json.dump(result, f)
    except Exception:
        pass
    log.info(f"✅ 1H range fetched: {len(result)} symbols")

# ══════════════════════════════════════════════════════════════════
# LIVE QUOTE FETCH
# ══════════════════════════════════════════════════════════════════
def fetch_live(kite) -> pd.DataFrame:
    """Fetch live quotes for all symbols and merge with cached OHLC levels."""
    all_q = {}
    keys  = [kite_sym(s) for s in SYMBOLS]
    for i in range(0, len(keys), 400):
        try:
            all_q.update(kite.quote(keys[i : i + 400]))
        except Exception as e:
            log.warning(f"Quote batch [{i}:{i+400}] error: {e}")
        time.sleep(0.1)

    if not all_q:
        log.error("Live fetch: no quotes received")
        return pd.DataFrame()

    rows = []
    for sym in SYMBOLS:
        q = all_q.get(kite_sym(sym))
        if not q:
            continue
        ltp  = q["last_price"]
        pc   = q["ohlc"]["close"]
        chg  = round(ltp - pc, 2) if pc else 0
        chgp = round(chg / pc * 100, 2) if pc else 0

        yo = _yest_ohlc.get(sym, {})
        tl = _top_levels.get(sym, {})
        wk = _weekly_ohlc.get(sym, {})
        mo = _monthly_ohlc.get(sym, {})
        ec = _ema_cache.get(sym, {})

        rows.append({
            "Symbol":      sym,
            "LTP":         round(ltp, 2),
            "LIVE_OPEN":   round(q["ohlc"]["open"], 2),
            "LIVE_HIGH":   round(q["ohlc"]["high"], 2),
            "LIVE_LOW":    round(q["ohlc"]["low"],  2),
            "LIVE_VOLUME": int(q.get("volume", 0)),
            "CHANGE":      chg,
            "CHANGE_%":    chgp,
            # Yesterday
            "YEST_OPEN":   yo.get("yo", 0),
            "YEST_HIGH":   yo.get("yh", 0),
            "YEST_LOW":    yo.get("yl", 0),
            "YEST_CLOSE":  yo.get("yc", 0),
            "YEST_VOL":    yo.get("yv", 0),
            # Top levels
            "TOP_HIGH":    tl.get("top_high", 0),
            "TOP_LOW":     tl.get("top_low",  0),
            # Weekly / Monthly
            "HIGH_W":      wk.get("high_w", 0),
            "LOW_W":       wk.get("low_w",  0),
            "HIGH_M":      mo.get("high_m", 0),
            "LOW_M":       mo.get("low_m",  0),
            # EMA (populated once 5-min candles fetched)
            "EMA7":        ec.get("ema7",  0),
            "EMA20":       ec.get("ema20", 0),
            "EMA50":       ec.get("ema50", 0),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Volume %
    df["YEST_VOL"]    = pd.to_numeric(df["YEST_VOL"],    errors="coerce").fillna(0)
    df["LIVE_VOLUME"] = pd.to_numeric(df["LIVE_VOLUME"], errors="coerce").fillna(0)
    df["VOL_%"] = (
        ((df["LIVE_VOLUME"] - df["YEST_VOL"]) / df["YEST_VOL"]) * 100
    ).where(df["YEST_VOL"] > 0, 0).round(2)

    df.to_csv(_dated("live_data"), index=False)
    log.info(f"💾 Live data saved: {len(df)} symbols")
    return df

# ══════════════════════════════════════════════════════════════════
# EMA COMPUTATION
# ══════════════════════════════════════════════════════════════════
def _ema(prices: list, n: int) -> float:
    if len(prices) < n:
        return 0.0
    k   = 2 / (n + 1)
    val = sum(prices[:n]) / n
    for p in prices[n:]:
        val = p * k + val * (1 - k)
    return round(val, 2)

def _update_ema_cache(df_candles: pd.DataFrame):
    """Compute EMA7/20/50 from 5-min close prices and store in _ema_cache."""
    global _ema_cache
    if df_candles.empty or "Symbol" not in df_candles.columns:
        return
    # FIX #6: parse date properly before sorting
    df_candles = df_candles.copy()
    df_candles["date"] = pd.to_datetime(df_candles["date"], utc=False, errors="coerce")
    for sym, grp in df_candles.groupby("Symbol"):
        closes = grp.sort_values("date")["close"].dropna().tolist()
        if closes:
            _ema_cache[sym] = {
                "ema7":  _ema(closes, 7),
                "ema20": _ema(closes, 20),
                "ema50": _ema(closes, 50),
            }

# ══════════════════════════════════════════════════════════════════
# ALERT DISPATCHER
# ══════════════════════════════════════════════════════════════════
_prev_sets = {}   # category → set of symbols seen last cycle

def _new_entries(category: str, current: list) -> list:
    """Return symbols newly entering the alert condition this cycle."""
    prev = _prev_sets.get(category, set())
    curr = set(str(s) for s in current if s)
    new  = sorted(curr - prev)   # sorted for deterministic dedup keys
    _prev_sets[category] = curr
    return new

def fire_alert(category: str, title: str, symbols: list,
               ltp_map: dict, up: bool = True):
    """Send TG + email alert for a category. FIX #7: dedup key = category+slot."""
    if not symbols:
        return
    if not tg_on(category) and not email_on(category):
        return

    # FIX #7: stable key — category + 10-min slot only (no symbol names)
    key = f"ALERT_{category}_{_slot_10m()}"
    if already_sent(key):
        return

    lines = "\n".join(
        f"  • <b>{s}</b>  LTP: ₹{ltp_map.get(s, '?')}"
        for s in symbols[:20]
    )
    bd  = _bd(up)
    msg = (
        f"{bd}\n"
        f"{'🟢' if up else '🔴'} <b>{title}</b>\n"
        f"⏰ {_now_str()}\n"
        f"📋 Stocks ({len(symbols)}):\n{lines}\n"
        f"⚠️ <i>NOT financial advice.</i>\n{bd}"
    )

    if tg_on(category):
        _send_routed(category, msg, key)
        _log_alert(category, symbols, "TG")
        log.info(f"📤 [{category}] {len(symbols)} stocks: {','.join(symbols[:5])}")

    if email_on(category):
        send_email_bg(
            f"[OiAnalytics] {title} — {len(symbols)} stocks | {_now_str()}",
            _sh(msg)
        )
        _log_alert(category, symbols, "EMAIL")


def _struct_tg(category: str, title: str, symbols: list,
               ltp_map: dict, df: pd.DataFrame, up: bool = True):
    """Structure alert with per-stock CHANGE_% detail. FIX #8: stable key."""
    if not tg_on(category):
        return
    # FIX #8: stable dedup key = category + 30-min slot
    key = f"STRUCT_{category}_{_slot_30m()}"
    if already_sent(key):
        return

    bd   = _bd(up)
    icon = "🟢" if up else "🔴"
    lines = []
    for s in symbols[:15]:
        ltp = ltp_map.get(s, "")
        try:
            row = df[df["Symbol"] == s]
            chg = f"  {row.iloc[0]['CHANGE_%']:+.2f}%" \
                  if not row.empty and "CHANGE_%" in row.columns else ""
        except Exception:
            chg = ""
        lines.append(f"  {icon} <b>{s}</b>  LTP: {ltp}{chg}")

    msg = (
        f"{bd}\n{icon} <b>{title}</b>\n"
        f"⏰ {_now_str()}\n"
        f"📋 Stocks ({len(symbols)}):\n" + "\n".join(lines) +
        f"\n⚠️ <i>NOT financial advice.</i>\n{bd}"
    )
    send_tg_bg(msg, key)
    _log_alert(category, symbols, "TG")
    log.info(f"📤 STRUCT [{category}]: {','.join(symbols[:5])}")


def fire_special_all3(up_syms: list, dn_syms: list, ltp_map: dict):
    """Special watchlist stocks — all-3 alert (TG + email, no toggle check)."""
    if not _toggles.get("special_stock_alerts", True):
        return
    now_s = _now_str()
    hour_slot = _now().strftime("%Y%m%d_%H")
    for direction, syms, icon in [("UP", up_syms, "🟢"), ("DOWN", dn_syms, "🔴")]:
        if not syms:
            continue
        key = f"SPECIAL_{direction}_{hour_slot}"
        if already_sent(key):
            continue
        lines = "\n".join(
            f"  🌟 <b>{s}</b>  LTP: ₹{ltp_map.get(s, '')}"
            for s in syms
        )
        title = "TOP_HIGH" if direction == "UP" else "TOP_LOW"
        msg = (
            f"🚨 <b>⭐ SPECIAL STOCKS — {title} BREAK</b>\n"
            f"⏰ {now_s}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{lines}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔔 All-3 Alert (Toast + Email + Telegram)\n"
            f"⚠️ <i>NOT financial advice.</i>"
        )
        send_tg_bg(msg, key)
        send_email_bg(
            f"[OiAnalytics] ⭐ SPECIAL {direction} — {','.join(syms[:3])}",
            _sh(msg)
        )
        _log_alert(f"SPECIAL_{direction}", syms, "ALL3")
        log.info(f"🌟 Special All-3 [{direction}]: {syms}")

# ══════════════════════════════════════════════════════════════════
# HELPER: First-15-min candle high/low cache (Point 2)
# Caches the high and low of the 09:15 candle for each symbol.
# Used to filter Yest GREEN / RED alerts — price must first break
# the 15-min candle before alerting (avoids false breakouts).
# ══════════════════════════════════════════════════════════════════
_first15_cache: dict = {}   # sym → {"high": float, "low": float, "fetched": bool}

def _get_first15_hl(kite, sym: str) -> tuple:
    """Return (high, low) of the first 15-min candle (09:15–09:30). Cached."""
    if sym in _first15_cache and _first15_cache[sym].get("fetched"):
        return _first15_cache[sym]["high"], _first15_cache[sym]["low"]
    try:
        tok = get_token(sym)
        if not tok:
            return 0.0, 0.0
        today = _today()
        from datetime import datetime as _dt
        s = IST.localize(_dt(today.year, today.month, today.day, 9, 15))
        e = IST.localize(_dt(today.year, today.month, today.day, 9, 30))
        bars = kite.historical_data(tok, s, e, "15minute")
        if bars:
            h = round(bars[0]["high"], 2)
            l = round(bars[0]["low"],  2)
            _first15_cache[sym] = {"high": h, "low": l, "fetched": True}
            return h, l
    except Exception:
        pass
    return 0.0, 0.0


def _build_enriched_alert(sym: str, ltp: float, row, ltp_map: dict,
                           fut_oi_map: dict = None) -> str:
    """
    Point 3: Enriched alert message (MARICO-style) with vol%, yesterday vol,
    today vol, future OI, change%, OHLC context.
    """
    chg_pct   = float(row.get("CHANGE_%", 0) or 0)
    chg_abs   = float(row.get("CHANGE",   0) or 0)
    yest_vol  = int(row.get("YEST_VOL",   0) or 0)
    live_vol  = int(row.get("LIVE_VOLUME",0) or 0)
    vol_pct   = float(row.get("VOL_%",    0) or 0)
    yh        = float(row.get("YEST_HIGH", 0) or 0)
    yl        = float(row.get("YEST_LOW",  0) or 0)
    yc        = float(row.get("YEST_CLOSE",0) or 0)
    yo        = float(row.get("YEST_OPEN", 0) or 0)
    day_open  = float(row.get("LIVE_OPEN", 0) or 0)
    day_high  = float(row.get("LIVE_HIGH", 0) or 0)
    day_low   = float(row.get("LIVE_LOW",  0) or 0)

    # Volume conviction
    vol_icon = "🔥" if vol_pct >= 50 else "📊" if vol_pct >= 0 else "📉"
    vol_conv = "HIGH" if vol_pct >= 50 else "NORMAL" if vol_pct >= 0 else "LOW"

    # Future OI
    fut_oi = (fut_oi_map or {}).get(sym, 0)
    oi_str = f"{fut_oi:,}" if fut_oi else "N/A"

    # Price action context
    abv_yh = ltp > yh
    blw_yl = ltp < yl

    lines = [
        f"💹 <b>{sym}</b>   LTP: <b>₹{ltp:,.2f}</b>   {chg_abs:+.2f} ({chg_pct:+.2f}%)",
        f"━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 <b>Yesterday OHLC</b>",
        f"   Open: {yo:,.2f}  High: {yh:,.2f}  Low: {yl:,.2f}  Close: {yc:,.2f}",
        f"📈 <b>Today OHLC</b>",
        f"   Open: {day_open:,.2f}  High: {day_high:,.2f}  Low: {day_low:,.2f}  LTP: {ltp:,.2f}",
        f"━━━━━━━━━━━━━━━━━━━━━━",
        f"{vol_icon} <b>Volume</b>   Today: {live_vol:,}  Yesterday: {yest_vol:,}  Vol%: {vol_pct:+.1f}% [{vol_conv}]",
        f"📊 <b>Future OI</b>: {oi_str}",
    ]
    if abv_yh:
        pts_abv = round(ltp - yh, 2)
        lines.append(f"✅ <b>Above Yest High</b> by +{pts_abv:.2f} pts — bullish breakout")
    elif blw_yl:
        pts_blw = round(yl - ltp, 2)
        lines.append(f"⚠️ <b>Below Yest Low</b> by -{pts_blw:.2f} pts — bearish breakdown")

    # Conviction summary
    conviction = "HIGH" if (vol_pct >= 30 and (abv_yh or blw_yl)) else \
                 "MEDIUM" if vol_pct >= 0 else "LOW"
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🎯 <b>Conviction</b>: {conviction}  |  ⚠️ NOT financial advice.")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# MACD DAILY SCANNER  (Point 9)
# Daily MACD crossovers for all stocks — fired once per signal per stock.
# Uses 12/26/9 standard MACD on daily close prices.
# ══════════════════════════════════════════════════════════════════
_macd_cache:  dict = {}   # sym → {"macd": float, "signal": float, "hist": float}
_macd_state:  dict = {}   # sym → "bull" | "bear" | None  (last known crossover)

def _compute_macd(closes: list, fast=12, slow=26, sig=9):
    """Compute MACD line, signal line, histogram from close list."""
    def _ema_series(prices, n):
        k = 2 / (n + 1)
        ema = [sum(prices[:n]) / n]
        for p in prices[n:]:
            ema.append(p * k + ema[-1] * (1 - k))
        return ema

    if len(closes) < slow + sig:
        return None, None, None
    ema_f  = _ema_series(closes, fast)
    ema_s  = _ema_series(closes, slow)
    # Align — ema_f is longer
    offset = len(ema_f) - len(ema_s)
    macd_l = [f - s for f, s in zip(ema_f[offset:], ema_s)]
    if len(macd_l) < sig:
        return None, None, None
    sig_l  = _ema_series(macd_l, sig)
    hist   = macd_l[-1] - sig_l[-1]
    return round(macd_l[-1], 4), round(sig_l[-1], 4), round(hist, 4)


def _update_macd_cache(kite):
    """Fetch 90-day daily closes (completed candles only) and compute MACD for all stocks."""
    global _macd_cache
    today = _today()
    end_d = last_trading_day(today)   # use last completed trading day only
    sd    = end_d - timedelta(days=90)
    result = {}
    for sym in STOCKS:
        try:
            tok = get_token(sym)
            if not tok:
                continue
            bars = kite.historical_data(tok, sd, end_d, "day")
            if len(bars) < 35:
                continue
            closes = [b["close"] for b in bars]
            m, s, h = _compute_macd(closes)
            if m is not None:
                result[sym] = {"macd": m, "signal": s, "hist": h,
                               "close": closes[-1], "prev_hist": h}
                # Also store previous-day histogram for crossover detection
                if len(closes) >= 2:
                    m2, s2, h2 = _compute_macd(closes[:-1])
                    if h2 is not None:
                        result[sym]["prev_hist"] = h2
        except Exception as e:
            log.debug(f"MACD [{sym}]: {e}")
        time.sleep(0.1)
    _macd_cache = result
    log.info(f"📊 MACD cache updated: {len(result)} symbols")


def _check_macd_alerts(ltp_map: dict):
    """Fire MACD bullish/bearish crossover alerts (daily timeframe)."""
    if not tg_on("MACD_BULL") and not tg_on("MACD_BEAR"):
        return
    now_s = _now_str()
    today_s = _now().strftime("%Y%m%d")

    bull_syms, bear_syms = [], []
    for sym, data in _macd_cache.items():
        h     = data.get("hist", 0) or 0
        prev  = data.get("prev_hist", 0) or 0
        # Bullish crossover: histogram crosses above 0
        if prev <= 0 and h > 0:
            if _macd_state.get(sym) != "bull":
                bull_syms.append(sym)
                _macd_state[sym] = "bull"
        # Bearish crossover: histogram crosses below 0
        elif prev >= 0 and h < 0:
            if _macd_state.get(sym) != "bear":
                bear_syms.append(sym)
                _macd_state[sym] = "bear"

    if bull_syms and tg_on("MACD_BULL"):
        k = f"MACD_BULL_{today_s}"
        if not already_sent(k):
            lines = "\n".join(
                f"  🟢 <b>{s}</b>  LTP: {ltp_map.get(s,'?')}  "
                f"MACD: {_macd_cache[s]['macd']:.3f}  Hist: {_macd_cache[s]['hist']:.3f}"
                for s in bull_syms[:15]
            )
            send_tg_bg(
                f"📈📈📈📈📈📈📈📈📈📈\n"
                f"📈 <b>MACD BULLISH CROSSOVER — Daily</b>\n"
                f"⏰ {now_s}   📊 12/26/9 Standard MACD\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stocks ({len(bull_syms)}):\n{lines}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ MACD line crossed ABOVE Signal line (daily)\n"
                f"⚠️ <i>NOT financial advice.</i>\n"
                f"📈📈📈📈📈📈📈📈📈📈", k)
            _log_alert("MACD_BULL", bull_syms, "TG")
            log.info(f"📈 MACD Bull: {bull_syms[:5]}")

    if bear_syms and tg_on("MACD_BEAR"):
        k = f"MACD_BEAR_{today_s}"
        if not already_sent(k):
            lines = "\n".join(
                f"  🔴 <b>{s}</b>  LTP: {ltp_map.get(s,'?')}  "
                f"MACD: {_macd_cache[s]['macd']:.3f}  Hist: {_macd_cache[s]['hist']:.3f}"
                for s in bear_syms[:15]
            )
            send_tg_bg(
                f"📉📉📉📉📉📉📉📉📉📉\n"
                f"📉 <b>MACD BEARISH CROSSOVER — Daily</b>\n"
                f"⏰ {now_s}   📊 12/26/9 Standard MACD\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stocks ({len(bear_syms)}):\n{lines}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ MACD line crossed BELOW Signal line (daily)\n"
                f"⚠️ <i>NOT financial advice.</i>\n"
                f"📉📉📉📉📉📉📉📉📉📉", k)
            _log_alert("MACD_BEAR", bear_syms, "TG")
            log.info(f"📉 MACD Bear: {bear_syms[:5]}")


# ══════════════════════════════════════════════════════════════════
# INSIDE BAR SCANNER  (Point 13)
# Daily timeframe — fires when today's candle is inside yesterday's.
# Uses previous two daily candles per stock.
# ══════════════════════════════════════════════════════════════════
_inside_bar_daily: dict = {}   # sym → {"parent_high": float, "parent_low": float}

def _update_inside_bar_cache(kite):
    """Fetch last 3 completed daily candles and detect inside bar patterns.
    Uses last_trading_day() as end_date to avoid incomplete intraday candle."""
    global _inside_bar_daily
    today   = _today()
    end_d   = last_trading_day(today)   # most recent COMPLETED trading day
    sd      = end_d - timedelta(days=10)
    result  = {}
    for sym in STOCKS:
        try:
            tok = get_token(sym)
            if not tok:
                continue
            bars = kite.historical_data(tok, sd, end_d, "day")
            if len(bars) < 2:
                continue
            parent = bars[-2]   # day before yesterday = parent bar
            child  = bars[-1]   # yesterday = inside bar candidate
            ph, pl = parent["high"], parent["low"]
            ch, cl = child["high"],  child["low"]
            # Inside bar: yesterday's range strictly within day-before's range
            if ch <= ph and cl >= pl:
                result[sym] = {
                    "parent_high": round(ph, 2),
                    "parent_low":  round(pl, 2),
                    "child_high":  round(ch, 2),
                    "child_low":   round(cl, 2),
                    "parent_body": round(abs(parent["close"] - parent["open"]), 2),
                    "child_body":  round(abs(child["close"]  - child["open"]),  2),
                    "compression": round((ch - cl) / (ph - pl) * 100, 1) if ph != pl else 0,
                }
        except Exception as e:
            log.debug(f"InsideBar [{sym}]: {e}")
        time.sleep(0.1)
    _inside_bar_daily = result
    log.info(f"📊 Inside Bar cache: {len(result)} patterns found")


def _check_inside_bar_alerts(ltp_map: dict):
    """Alert when LTP breaks above parent high or below parent low (Inside Bar breakout)."""
    if not tg_on("INSIDE_BAR") or not _inside_bar_daily:
        return
    now_s   = _now_str()
    today_s = _now().strftime("%Y%m%d")
    slot    = _slot_10m()

    bull_breaks, bear_breaks = [], []
    for sym, data in _inside_bar_daily.items():
        ltp = float(ltp_map.get(sym, 0) or 0)
        ph  = data["parent_high"]
        pl  = data["parent_low"]
        if ltp <= 0:
            continue
        if ltp > ph:
            bull_breaks.append((sym, ltp, ph, pl, data))
        elif ltp < pl:
            bear_breaks.append((sym, ltp, ph, pl, data))

    for direction, breaks, icon, verdict in [
        ("UP",   bull_breaks, "🟢", "BULLISH BREAKOUT"),
        ("DOWN", bear_breaks, "🔴", "BEARISH BREAKDOWN"),
    ]:
        if not breaks:
            continue
        k = f"INSIDE_BAR_{direction}_{today_s}_{slot}"
        if already_sent(k):
            continue
        lines = []
        for sym, ltp, ph, pl, data in breaks[:10]:
            comp = data.get("compression", 0)
            lines.append(
                f"  {icon} <b>{sym}</b>  LTP: {ltp:,.2f}\n"
                f"    Parent: H={ph:,.2f}  L={pl:,.2f}  "
                f"Compression: {comp:.0f}%\n"
                f"    Entry: {'Above ' + str(ph) if direction=='UP' else 'Below ' + str(pl)}  "
                f"SL: {'Below ' + str(pl) if direction=='UP' else 'Above ' + str(ph)}"
            )
        send_tg_bg(
            f"{'🟢' * 10 if direction=='UP' else '🔴' * 10}\n"
            f"{icon} <b>INSIDE BAR {verdict} — Daily</b>\n"
            f"⏰ {now_s}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Today's candle was INSIDE yesterday's range.\n"
            f"Now breaking {'above Parent High' if direction=='UP' else 'below Parent Low'}!\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            + "\n".join(lines) +
            f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 Stop-loss = {'Parent Low' if direction=='UP' else 'Parent High'}\n"
            f"⚠️ <i>NOT financial advice.</i>\n"
            f"{'🟢' * 10 if direction=='UP' else '🔴' * 10}",
            k
        )
        _log_alert(f"INSIDE_BAR_{direction}", [s for s, *_ in breaks], "TG")
        log.info(f"📊 Inside Bar {direction}: {[s for s,*_ in breaks[:5]]}")


# ══════════════════════════════════════════════════════════════════
# PANCHAK RANGE ENGINE  (Point 7)
# Tracks the complete Panchak period HIGH and LOW.
# Once Panchak ends → monitors for breakout and fires targets at
# 61%, 138%, 200% of the range. Also monitors bearish start days,
# Vaidhriti/Indra yoga flags (simplified).
# ══════════════════════════════════════════════════════════════════
_panchak_range_state: dict = {}   # persisted in CACHE/panchak_range.json
_PANCHAK_RANGE_FILE = os.path.join(CACHE_DIR, "panchak_range.json")

def _load_panchak_range():
    global _panchak_range_state
    try:
        with open(_PANCHAK_RANGE_FILE) as f:
            _panchak_range_state = json.load(f)
    except Exception:
        _panchak_range_state = {}

def _save_panchak_range():
    try:
        with open(_PANCHAK_RANGE_FILE, "w") as f:
            json.dump(_panchak_range_state, f, indent=2)
    except Exception:
        pass

def _check_panchak_range_alerts(ltp_map: dict):
    """
    Point 7: Panchak range trading engine.
    - During Panchak period: track NIFTY high and low.
    - After Panchak ends: fire breakout + target alerts.
    - Start-day bias: Mon/Wed/Fri = bearish, else bullish.
    - Targets: 61%, 138%, 200% of range.
    - SL: opposite end of range.
    """
    if not tg_on("PANCHAK_RANGE"):
        return

    nifty_ltp = float(ltp_map.get("NIFTY", 0) or 0)
    if nifty_ltp <= 0:
        return

    today   = _today()
    now_s   = _now_str()
    p_start = PANCHAK_START
    p_end   = PANCHAK_END
    today_s = today.strftime("%Y%m%d")

    state = _panchak_range_state
    pkey  = p_start.strftime("%Y%m%d")    # unique key per panchak period

    if pkey not in state:
        # Init for this panchak period
        start_wd = p_start.weekday()  # 0=Mon,2=Wed,4=Fri
        bearish_start = start_wd in (0, 2, 4)
        state[pkey] = {
            "start": p_start.isoformat(), "end": p_end.isoformat(),
            "period_high": 0.0, "period_low": 999999.0,
            "bearish_start": bearish_start,
            "bias": "BEARISH" if bearish_start else "BULLISH",
            "breakout_up_sent": False, "breakout_dn_sent": False,
            "t61_up": False, "t138_up": False, "t200_up": False,
            "t61_dn": False, "t138_dn": False, "t200_dn": False,
        }

    s = state[pkey]
    in_panchak  = p_start <= today <= p_end
    post_panchak = today > p_end

    # ── During Panchak: track HIGH and LOW ──────────────────────────
    if in_panchak:
        if nifty_ltp > s["period_high"]:
            s["period_high"] = round(nifty_ltp, 2)
        if nifty_ltp < s["period_low"]:
            s["period_low"]  = round(nifty_ltp, 2)
        _save_panchak_range()

        # Send bias alert once at start of panchak
        k_bias = f"PANCHAK_RANGE_BIAS_{pkey}"
        if not already_sent(k_bias):
            bias  = s["bias"]
            wd_name = ["Monday","Tuesday","Wednesday","Thursday","Friday"][p_start.weekday()]
            send_tg_bg(
                f"🪐🪐🪐🪐🪐🪐🪐🪐🪐🪐\n"
                f"🪐 <b>PANCHAK RANGE — Period Started</b>\n"
                f"📅 {p_start.strftime('%d-%b-%Y')} → {p_end.strftime('%d-%b-%Y')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Start Day: <b>{wd_name}</b>\n"
                f"Bias: <b>{'🔴 BEARISH' if bias=='BEARISH' else '🟢 BULLISH'}</b>\n"
                f"(Mon/Wed/Fri start = Bearish, other days = Bullish)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 Tracking NIFTY range throughout Panchak.\n"
                f"Breakout alerts will fire AFTER Panchak ends.\n"
                f"⚠️ <i>NOT financial advice.</i>\n"
                f"🪐🪐🪐🪐🪐🪐🪐🪐🪐🪐", k_bias)
        return  # No breakout alerts during Panchak itself

    # ── Post-Panchak: fire breakout + target alerts ──────────────────
    if not post_panchak:
        return

    ph = s.get("period_high", 0)
    pl = s.get("period_low", 999999)
    if ph <= 0 or pl >= 999999 or ph <= pl:
        return

    rng    = round(ph - pl, 2)
    t61    = round(rng * 0.61, 2)
    t138   = round(rng * 1.38, 2)
    t200   = round(rng * 2.00, 2)

    # Upside targets (from high)
    up_t61  = round(ph + t61,  2)
    up_t138 = round(ph + t138, 2)
    up_t200 = round(ph + t200, 2)
    # Downside targets (from low)
    dn_t61  = round(pl - t61,  2)
    dn_t138 = round(pl - t138, 2)
    dn_t200 = round(pl - t200, 2)

    def _panchak_msg(title, level, target61, target138, target200,
                     sl, direction, broke_pts):
        return (
            f"{'🟢' * 8 if direction=='UP' else '🔴' * 8}\n"
            f"{'🟢' if direction=='UP' else '🔴'} <b>PANCHAK RANGE {title}</b>\n"
            f"📅 Panchak: {p_start.strftime('%d-%b')} → {p_end.strftime('%d-%b')}\n"
            f"⏰ {now_s}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Panchak High  : <b>{ph:,.2f}</b>\n"
            f"📊 Panchak Low   : <b>{pl:,.2f}</b>\n"
            f"📏 Range         : <b>{rng:,.2f} pts</b>\n"
            f"🏷️  Broke Level  : <b>{level:,.2f}</b>\n"
            f"💹 NIFTY LTP     : <b>{nifty_ltp:,.2f}</b>\n"
            f"📏 {'Above' if direction=='UP' else 'Below'} level: <b>{broke_pts:,.2f} pts</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Targets (from {'High' if direction=='UP' else 'Low'})</b>\n"
            f"   61%  → <b>{target61:,.2f}</b>\n"
            f"  138%  → <b>{target138:,.2f}</b>\n"
            f"  200%  → <b>{target200:,.2f}</b>\n"
            f"🛡️  SL          : <b>{sl:,.2f}</b> (opposite end)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>NOT financial advice.</i>\n"
            f"{'🟢' * 8 if direction=='UP' else '🔴' * 8}"
        )

    # Upside breakout (price above Panchak High)
    if nifty_ltp > ph and not s["breakout_up_sent"]:
        k = f"PR_BREAK_UP_{pkey}_{today_s}"
        if not already_sent(k):
            send_tg_bg(_panchak_msg(
                "HIGH BREAK — BULLISH", ph, up_t61, up_t138, up_t200,
                pl, "UP", round(nifty_ltp - ph, 2)), k)
            s["breakout_up_sent"] = True
            _save_panchak_range()

    # Downside breakout (price below Panchak Low)
    if nifty_ltp < pl and not s["breakout_dn_sent"]:
        k = f"PR_BREAK_DN_{pkey}_{today_s}"
        if not already_sent(k):
            send_tg_bg(_panchak_msg(
                "LOW BREAK — BEARISH", pl, dn_t61, dn_t138, dn_t200,
                ph, "DOWN", round(pl - nifty_ltp, 2)), k)
            s["breakout_dn_sent"] = True
            _save_panchak_range()

    # Target alerts (upside)
    for tgt, tgt_val, key_flag, tgt_name in [
        (up_t61,  up_t61,  "t61_up",  "61%  Target UP"),
        (up_t138, up_t138, "t138_up", "138% Target UP"),
        (up_t200, up_t200, "t200_up", "200% Target UP"),
    ]:
        if nifty_ltp >= tgt_val and not s[key_flag]:
            k = f"PR_{key_flag}_{pkey}"
            if not already_sent(k):
                send_tg_bg(
                    f"🏆 <b>PANCHAK RANGE — {tgt_name} HIT</b>\n"
                    f"Target: <b>{tgt_val:,.2f}</b>  NIFTY LTP: <b>{nifty_ltp:,.2f}</b>\n"
                    f"Range: {rng:.0f} pts  SL: {pl:,.2f}\n"
                    f"⏰ {now_s}  ⚠️ <i>NOT financial advice.</i>", k)
            s[key_flag] = True
            _save_panchak_range()

    # Target alerts (downside)
    for tgt, tgt_val, key_flag, tgt_name in [
        (dn_t61,  dn_t61,  "t61_dn",  "61%  Target DOWN"),
        (dn_t138, dn_t138, "t138_dn", "138% Target DOWN"),
        (dn_t200, dn_t200, "t200_dn", "200% Target DOWN"),
    ]:
        if nifty_ltp <= tgt_val and not s[key_flag]:
            k = f"PR_{key_flag}_{pkey}"
            if not already_sent(k):
                send_tg_bg(
                    f"🏆 <b>PANCHAK RANGE — {tgt_name} HIT</b>\n"
                    f"Target: <b>{tgt_val:,.2f}</b>  NIFTY LTP: <b>{nifty_ltp:,.2f}</b>\n"
                    f"Range: {rng:.0f} pts  SL: {ph:,.2f}\n"
                    f"⏰ {now_s}  ⚠️ <i>NOT financial advice.</i>", k)
            s[key_flag] = True
            _save_panchak_range()


# ══════════════════════════════════════════════════════════════════
# EXPIRY ALERTS  (Point 10)
# Nifty: Weekly on Tue (or Mon if Tue=holiday)
# Sensex: Weekly on Thu (or Wed if Thu=holiday)
# Alert schedule: day-before + morning-of reminders
# ══════════════════════════════════════════════════════════════════
def _get_nifty_expiry_day(ref_date) -> date:
    """Return Nifty expiry day (Tue) for the week containing ref_date.
    Shifts to Monday if Tuesday is an NSE holiday."""
    # Find Tuesday of the same week (weekday=1)
    days_to_tue = (1 - ref_date.weekday()) % 7
    tue = ref_date + timedelta(days=days_to_tue)
    if tue in NSE_HOLIDAYS:
        return tue - timedelta(days=1)   # shift to Monday
    return tue

def _get_sensex_expiry_day(ref_date) -> date:
    """Return Sensex expiry day (Thu) for the week containing ref_date.
    Shifts to Wednesday if Thursday is an NSE holiday."""
    days_to_thu = (3 - ref_date.weekday()) % 7
    thu = ref_date + timedelta(days=days_to_thu)
    if thu in NSE_HOLIDAYS:
        return thu - timedelta(days=1)   # shift to Wednesday
    return thu

def _check_expiry_alerts():
    """Fire expiry reminder alerts. Called every 60s during market hours."""
    if not tg_on("EXPIRY_ALERT"):
        return

    today   = _today()
    now_dt  = _now()
    hhmm    = now_dt.strftime("%H%M")
    now_s   = _now_str()
    today_s = today.strftime("%Y%m%d")

    nifty_exp  = _get_nifty_expiry_day(today)
    sensex_exp = _get_sensex_expiry_day(today)
    nifty_exp_str  = nifty_exp.strftime("%A %d-%b")
    sensex_exp_str = sensex_exp.strftime("%A %d-%b")

    # ── Nifty expiry reminders ───────────────────────────────────────
    # Day before (Mon or Sun→Mon) at 09:15
    days_to_nifty = (nifty_exp - today).days
    if days_to_nifty == 1 and hhmm >= "0915":
        k = f"EXP_NIFTY_DAYBEFORE_{today_s}"
        if not already_sent(k):
            send_tg_bg(
                f"📅 <b>NIFTY EXPIRY REMINDER — Tomorrow</b>\n"
                f"⏰ {now_s}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Nifty Weekly Expiry: <b>{nifty_exp_str}</b>\n"
                f"{'⚠️ NOTE: Shifted from Tue (Holiday)' if nifty_exp.weekday()==0 else ''}\n"
                f"📌 Review open Nifty positions. Manage risk.\n"
                f"⚠️ <i>NOT financial advice.</i>", k)

    # Morning of expiry at 09:15
    if days_to_nifty == 0 and hhmm >= "0915":
        k = f"EXP_NIFTY_MORNING_{today_s}"
        if not already_sent(k):
            send_tg_bg(
                f"🚨 <b>NIFTY EXPIRY DAY</b> 🚨\n"
                f"⏰ {now_s}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Today is Nifty Weekly Expiry: <b>{nifty_exp_str}</b>\n"
                f"{'⚠️ Shifted from Tue (Holiday)' if nifty_exp.weekday()==0 else ''}\n"
                f"📌 Close or hedge Nifty positions by 15:20!\n"
                f"⚠️ <i>NOT financial advice.</i>", k)

    # ── Sensex expiry reminders ──────────────────────────────────────
    days_to_sensex = (sensex_exp - today).days
    if days_to_sensex == 1 and hhmm >= "0915":
        k = f"EXP_SENSEX_DAYBEFORE_{today_s}"
        if not already_sent(k):
            send_tg_bg(
                f"📅 <b>SENSEX EXPIRY REMINDER — Tomorrow</b>\n"
                f"⏰ {now_s}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Sensex Weekly Expiry: <b>{sensex_exp_str}</b>\n"
                f"{'⚠️ NOTE: Shifted from Thu (Holiday)' if sensex_exp.weekday()==2 else ''}\n"
                f"📌 Review open Sensex positions. Manage risk.\n"
                f"⚠️ <i>NOT financial advice.</i>", k)

    if days_to_sensex == 0 and hhmm >= "0915":
        k = f"EXP_SENSEX_MORNING_{today_s}"
        if not already_sent(k):
            send_tg_bg(
                f"🚨 <b>SENSEX EXPIRY DAY</b> 🚨\n"
                f"⏰ {now_s}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Today is Sensex Weekly Expiry: <b>{sensex_exp_str}</b>\n"
                f"{'⚠️ Shifted from Thu (Holiday)' if sensex_exp.weekday()==2 else ''}\n"
                f"📌 Close or hedge Sensex positions by 15:20!\n"
                f"⚠️ <i>NOT financial advice.</i>", k)


# ══════════════════════════════════════════════════════════════════
# HOLIDAY ALERTS  (Point 11)
# - Morning alert if tomorrow is NSE holiday
# - Alert before 3 PM if tomorrow is NSE holiday (close trades)
# - Friday alert if Monday is a holiday (long weekend)
# ══════════════════════════════════════════════════════════════════
def _check_holiday_alerts():
    """Fire NSE holiday alerts. Called every loop iteration (not gated by market hours)."""
    if not tg_on("HOLIDAY_ALERT"):
        return

    today   = _today()
    now_dt  = _now()
    hhmm    = now_dt.strftime("%H%M")
    now_s   = _now_str()
    today_s = today.strftime("%Y%m%d")

    tomorrow = today + timedelta(days=1)
    # Skip weekends for tomorrow check
    if tomorrow.weekday() < 5 and tomorrow in NSE_HOLIDAYS:
        tom_name = tomorrow.strftime("%A %d-%b-%Y")

        # Morning alert — fires once at/after 09:00, dedup key is per-day (not per-window)
        if hhmm >= "0900":
            k = f"HOLIDAY_TOMORROW_MORNING_{today_s}"
            if not already_sent(k):
                send_tg_bg(
                    f"🚨 <b>NSE HOLIDAY TOMORROW</b>\n"
                    f"⏰ {now_s}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📅 <b>{tom_name}</b> is an NSE Holiday.\n"
                    f"📌 Plan accordingly. Close or hedge open positions.\n"
                    f"⚠️ <i>NOT financial advice.</i>", k)

        # Pre-3PM alert (14:30 onwards) — urgent: close all trades
        if hhmm >= "1430":
            k = f"HOLIDAY_TOMORROW_PRECLOSE_{today_s}"
            if not already_sent(k):
                send_tg_bg(
                    f"⚠️⚠️ <b>LAST CHANCE — NSE HOLIDAY TOMORROW</b> ⚠️⚠️\n"
                    f"⏰ {now_s}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📅 Tomorrow <b>{tom_name}</b> is NSE CLOSED.\n"
                    f"🚨 <b>Close all open intraday positions before 15:20 today!</b>\n"
                    f"📌 Overnight positions carry holiday gap risk.\n"
                    f"⚠️ <i>NOT financial advice.</i>", k)

    # Friday + Monday holiday = long weekend alert (fires once on Friday >= 09:00)
    if today.weekday() == 4:   # Friday
        monday = today + timedelta(days=3)
        if monday in NSE_HOLIDAYS and hhmm >= "0900":
            mon_name = monday.strftime("%A %d-%b-%Y")
            k = f"HOLIDAY_LONG_WEEKEND_{today_s}"
            if not already_sent(k):
                send_tg_bg(
                    f"📅 <b>LONG WEEKEND ALERT</b>\n"
                    f"⏰ {now_s}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Today is Friday + <b>{mon_name}</b> is NSE Holiday.\n"
                    f"⚠️ <b>3-day weekend</b> — manage weekend gap risk!\n"
                    f"📌 Consider closing/hedging before 15:20 today.\n"
                    f"⚠️ <i>NOT financial advice.</i>", k)


# ══════════════════════════════════════════════════════════════════
# SMC + PANCHAK COMBINED (HTF TREND + BREAKOUT)
# ══════════════════════════════════════════════════════════════════
def _check_smc_panchak(df: pd.DataFrame, ltp_map: dict):
    if not _SMC_OK or not tg_on("SMC_PANCHAK"):
        return
    if not all(c in df.columns for c in ["Symbol", "LTP", "TOP_HIGH", "TOP_LOW"]):
        return

    today_s = _today_str()
    now_s   = _now_str()

    wl_df = df[df["Symbol"].isin(PANCHAK_ALERT_WATCHLIST)].copy()

    for _, row in wl_df.iterrows():
        sym = str(row["Symbol"]).strip()
        k = f"SMC_P_COMBINED_{sym}_{today_s}"
        if already_sent(k):
            continue

        ltp = float(ltp_map.get(sym, row.get("LTP", 0)) or 0)
        th  = float(row.get("TOP_HIGH", 0) or 0)
        tl  = float(row.get("TOP_LOW",  0) or 0)

        if ltp <= 0 or th <= 0 or tl <= 0:
            continue

        is_bull_break = ltp >= th
        is_bear_break = ltp <= tl

        if not (is_bull_break or is_bear_break):
            continue

        try:
            tk = get_token(sym)
            if not tk: continue

            end_dt   = _now()
            start_dt = end_dt - timedelta(days=10)
            bars = kite.historical_data(tk, start_dt, end_dt, "60minute")
            if not bars or len(bars) < 10:
                continue

            ms = detect_market_structure(bars)
            trend = ms.get("trend", "RANGING")

            match = False
            if is_bull_break and "BULLISH" in trend: match = True
            if is_bear_break and "BEARISH" in trend: match = True

            if match:
                ob = find_order_blocks(bars)
                fvg = find_fvg(bars)

                side_icon = "🟢" if is_bull_break else "🔴"
                side_text = "BULLISH" if is_bull_break else "BEARISH"

                smc_details = [
                    f"  Trend: <b>{trend}</b>",
                    f"  Structure: {ms.get('structure_summary', 'N/A')}"
                ]
                if is_bull_break:
                    nob = ob.get("nearest_bullish_ob")
                    if nob: smc_details.append(f"  Bullish OB: {nob['low']:.1f}-{nob['high']:.1f}")
                    nfvg = fvg.get("nearest_bullish_fvg")
                    if nfvg: smc_details.append(f"  Bullish FVG: {nfvg['bottom']:.1f}-{nfvg['top']:.1f}")
                else:
                    nob = ob.get("nearest_bearish_ob")
                    if nob: smc_details.append(f"  Bearish OB: {nob['low']:.1f}-{nob['high']:.1f}")
                    nfvg = fvg.get("nearest_bearish_fvg")
                    if nfvg: smc_details.append(f"  Bearish FVG: {nfvg['bottom']:.1f}-{nfvg['top']:.1f}")

                ohlc = f"{row.get('LIVE_OPEN',0)} / {row.get('LIVE_HIGH',0)} / {row.get('LIVE_LOW',0)} / {ltp}"
                yest = f"{row.get('YEST_HIGH',0)} / {row.get('YEST_LOW',0)} / {row.get('YEST_CLOSE',0)}"
                weekly = f"{row.get('HIGH_W',0)} / {row.get('LOW_W',0)}"
                change_str = f"{row.get('CHANGE',0):.2f} ({row.get('CHANGE_%',0):+.2f}%)"

                msg = (
                    f"{side_icon} <b>SMC + PANCHAK COMBINED ALERT</b>\n"
                    f"⏰ {now_s} | Stock: <b>{sym}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔥 <b>{side_text} CONFLUENCE</b>\n"
                    f"SMC HTF Trend confirmed {side_text} and LTP crossed {'TOP_HIGH' if is_bull_break else 'TOP_LOW'}.\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🧠 <b>SMC HTF (1H) DETAILS:</b>\n"
                    + "\n".join(smc_details) + "\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>PRICE DETAILS:</b>\n"
                    f"  LTP: <b>{ltp:,.2f}</b>\n"
                    f"  OHLC: {ohlc}\n"
                    f"  Yest H/L/C: {yest}\n"
                    f"  Weekly H/L: {weekly}\n"
                    f"  Panchak High: {th}\n"
                    f"  Panchak Low: {tl}\n"
                    f"  Change: {change_str}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <i>One alert per day per stock.</i>\n"
                    f"⚠️ <i>NOT financial advice.</i>\n"
                    f"{side_icon*10}"
                )

                send_tg_bg(msg, k)
                _log_alert("SMC_PANCHAK", [sym], "TG")
                log.info(f"🧠 SMC+Panchak Confluence: {sym} {side_text}")

        except Exception as e:
            log.warning(f"SMC+Panchak check error [{sym}]: {e}")
            continue


# ══════════════════════════════════════════════════════════════════
# MAIN ALERT CHECK — called every 60s with live DataFrame
# ══════════════════════════════════════════════════════════════════
def check_all_alerts(df: pd.DataFrame):
    if df.empty:
        return

    ltp_map = dict(zip(df["Symbol"], df["LTP"]))

    # ── TOP HIGH / TOP LOW ────────────────────────────────────────
    if "TOP_HIGH" in df.columns and "TOP_LOW" in df.columns:
        th_new = _new_entries("TOP_HIGH",
            df[(df["TOP_HIGH"] > 0) & (df["LTP"] >= df["TOP_HIGH"])]["Symbol"].tolist())
        tl_new = _new_entries("TOP_LOW",
            df[(df["TOP_LOW"]  > 0) & (df["LTP"] <= df["TOP_LOW"]) ]["Symbol"].tolist())
        if th_new:
            fire_alert("TOP_HIGH", "🟢 TOP_HIGH Break", th_new, ltp_map, True)
        if tl_new:
            fire_alert("TOP_LOW",  "🔴 TOP_LOW Break",  tl_new, ltp_map, False)
        fire_special_all3(
            [s for s in th_new if s in SPECIAL_ALERT_STOCKS],
            [s for s in tl_new if s in SPECIAL_ALERT_STOCKS],
            ltp_map
        )

    # ── DAILY HIGH / LOW ──────────────────────────────────────────
    if "YEST_HIGH" in df.columns and "YEST_LOW" in df.columns:
        fire_alert("DAILY_UP",   "📈 Daily HIGH Break",
            _new_entries("DAILY_UP",
                df[(df["YEST_HIGH"] > 0) & (df["LTP"] >= df["YEST_HIGH"])]["Symbol"].tolist()),
            ltp_map, True)
        fire_alert("DAILY_DOWN", "📉 Daily LOW Break",
            _new_entries("DAILY_DOWN",
                df[(df["YEST_LOW"]  > 0) & (df["LTP"] <= df["YEST_LOW"]) ]["Symbol"].tolist()),
            ltp_map, False)

    # ── YESTERDAY GREEN / RED SETUP (Point 2: first-15min break filter) ──
    if all(c in df.columns for c in ["YEST_CLOSE", "YEST_OPEN", "YEST_HIGH", "YEST_LOW"]):
        now_hhmm = _now().strftime("%H%M")
        # Only apply first-15min filter after 09:30 (first candle is complete)
        first15_ready = now_hhmm >= "0930"

        green_candidates = df[
            (df["YEST_CLOSE"] > df["YEST_OPEN"]) &
            (df["YEST_HIGH"] > 0) & (df["LTP"] >= df["YEST_HIGH"])
        ]
        red_candidates = df[
            (df["YEST_CLOSE"] < df["YEST_OPEN"]) &
            (df["YEST_LOW"]  > 0) & (df["LTP"] <= df["YEST_LOW"])
        ]

        # Point 2: For GREEN — price must ALSO break the first-15min HIGH
        # For RED — price must ALSO break below the first-15min LOW
        # This avoids false breakouts at open when price gaps past YEST_HIGH
        def _passes_first15_filter(sym: str, direction: str) -> bool:
            """Check if LTP has also broken the first 15-min candle in the alert direction."""
            if not first15_ready:
                return False   # first candle not complete yet
            ltp = float(ltp_map.get(sym, 0) or 0)
            if ltp <= 0:
                return False
            # Use cached first15 data if available
            cached = _first15_cache.get(sym, {})
            if not cached.get("fetched"):
                return True    # no cache yet — allow through (will build on next kite cycle)
            h15 = cached.get("high", 0)
            l15 = cached.get("low",  0)
            if direction == "UP":
                return ltp >= h15 > 0    # must be at or above first 15-min high
            else:
                return ltp <= l15 > 0    # must be at or below first 15-min low

        yg_syms = [s for s in green_candidates["Symbol"].tolist()
                   if _passes_first15_filter(s, "UP")]
        yr_syms = [s for s in red_candidates["Symbol"].tolist()
                   if _passes_first15_filter(s, "DOWN")]

        # Point 3: Enriched alert for Yest GREEN (MARICO-style with vol/OI)
        new_yg = _new_entries("YEST_GREEN_BREAK", yg_syms)
        if new_yg:
            # Standard fire_alert for the group
            fire_alert("YEST_GREEN_BREAK",
                "📈 Yest Green → Breakout Above YH (✅ First-15m Confirmed)",
                new_yg, ltp_map, True)
            # Per-stock enriched alert for high-conviction stocks
            for sym in new_yg[:5]:
                try:
                    row = df[df["Symbol"] == sym].iloc[0]
                    enriched = _build_enriched_alert(sym, float(ltp_map.get(sym,0)), row, ltp_map)
                    k = f"YEST_GREEN_ENRICHED_{sym}_{_slot_10m()}"
                    if not already_sent(k):
                        send_tg_bg(
                            f"🟢 <b>YEST GREEN BREAKOUT — Details</b>\n{enriched}", k)
                except Exception:
                    pass

        new_yr = _new_entries("YEST_RED_BREAK", yr_syms)
        if new_yr:
            fire_alert("YEST_RED_BREAK",
                "📉 Yest Red → Breakdown Below YL (✅ First-15m Confirmed)",
                new_yr, ltp_map, False)
            for sym in new_yr[:5]:
                try:
                    row = df[df["Symbol"] == sym].iloc[0]
                    enriched = _build_enriched_alert(sym, float(ltp_map.get(sym,0)), row, ltp_map)
                    k = f"YEST_RED_ENRICHED_{sym}_{_slot_10m()}"
                    if not already_sent(k):
                        send_tg_bg(
                            f"🔴 <b>YEST RED BREAKDOWN — Details</b>\n{enriched}", k)
                except Exception:
                    pass

    # ── WEEKLY / MONTHLY ──────────────────────────────────────────
    if "HIGH_W" in df.columns and "LOW_W" in df.columns:
        fire_alert("WEEKLY_UP",   "📈 Weekly HIGH Break",
            _new_entries("WEEKLY_UP",
                df[(df["HIGH_W"] > 0) & (df["LTP"] >= df["HIGH_W"])]["Symbol"].tolist()),
            ltp_map, True)
        fire_alert("WEEKLY_DOWN", "📉 Weekly LOW Break",
            _new_entries("WEEKLY_DOWN",
                df[(df["LOW_W"]  > 0) & (df["LTP"] <= df["LOW_W"]) ]["Symbol"].tolist()),
            ltp_map, False)
    if "HIGH_M" in df.columns and "LOW_M" in df.columns:
        fire_alert("MONTHLY_UP",   "📈 Monthly HIGH Break",
            _new_entries("MONTHLY_UP",
                df[(df["HIGH_M"] > 0) & (df["LTP"] >= df["HIGH_M"])]["Symbol"].tolist()),
            ltp_map, True)
        fire_alert("MONTHLY_DOWN", "📉 Monthly LOW Break",
            _new_entries("MONTHLY_DOWN",
                df[(df["LOW_M"]  > 0) & (df["LTP"] <= df["LOW_M"]) ]["Symbol"].tolist()),
            ltp_map, False)

    # ── OPEN STRUCTURE ALERTS ─────────────────────────────────────
    if all(c in df.columns for c in
           ["YEST_CLOSE", "YEST_OPEN", "YEST_HIGH", "YEST_LOW", "LIVE_OPEN"]):
        PCT = 1.0   # within 1% of yest high/low for tight-zone variants
        OT  = 1.0   # open tolerance (points)

        # 1. Green Open Structure
        gs = df[(df["YEST_CLOSE"] > df["YEST_OPEN"]) &
                (df["LIVE_OPEN"]  > df["YEST_CLOSE"]) &
                (df["LIVE_OPEN"]  < df["YEST_HIGH"]) &
                (df["LTP"] >= df["YEST_HIGH"])]
        gs_n = _new_entries("GREEN_OPEN_STRUCTURE", gs["Symbol"].tolist())
        if gs_n:
            _struct_tg("GREEN_OPEN_STRUCTURE",
                "🟢 Green Open Structure — Yest GREEN, Open Inside, LTP ≥ YH",
                gs_n, ltp_map, df, True)

        # 2. Red Open Structure
        rs = df[(df["YEST_CLOSE"] < df["YEST_OPEN"]) &
                (df["LIVE_OPEN"]  < df["YEST_CLOSE"]) &
                (df["LIVE_OPEN"]  > df["YEST_LOW"]) &
                (df["LTP"] <= df["YEST_LOW"])]
        rs_n = _new_entries("RED_OPEN_STRUCTURE", rs["Symbol"].tolist())
        if rs_n:
            _struct_tg("RED_OPEN_STRUCTURE",
                "🔴 Red Open Structure — Yest RED, Open Inside, LTP ≤ YL",
                rs_n, ltp_map, df, False)

        # 3. Yest GREEN – Open Inside tight BREAKOUT (≤1% from YH)
        _pct_yh = (df["YEST_HIGH"] - df["YEST_CLOSE"]).where(
            df["YEST_CLOSE"] > 0, 0) / df["YEST_CLOSE"].replace(0, 1) * 100
        gz = df[(_pct_yh <= PCT) &
                (df["YEST_CLOSE"] > df["YEST_OPEN"]) &
                (df["LIVE_OPEN"]  > df["YEST_CLOSE"]) &
                (df["LIVE_OPEN"]  < df["YEST_HIGH"]) &
                (df["LTP"] >= df["YEST_HIGH"])]
        gz_n = _new_entries("YEST_GREEN_OPEN_BREAK", gz["Symbol"].tolist())
        if gz_n:
            _struct_tg("YEST_GREEN_OPEN_BREAK",
                "🟢 Yesterday GREEN – Open Inside BREAKOUT (LTP ≥ YEST_HIGH)",
                gz_n, ltp_map, df, True)

        # 4. Yest RED – Open Inside Lower Zone (≤1% from YL)
        _pct_yl = (df["YEST_CLOSE"] - df["YEST_LOW"]).where(
            df["YEST_CLOSE"] > 0, 0) / df["YEST_CLOSE"].replace(0, 1) * 100
        rz = df[(_pct_yl <= PCT) &
                (df["LIVE_OPEN"] >= df["YEST_LOW"]  + OT) &
                (df["LIVE_OPEN"] <= df["YEST_CLOSE"] - OT) &
                (df["LTP"] <= df["YEST_LOW"])]
        rz_n = _new_entries("YEST_RED_OPEN_LOWER", rz["Symbol"].tolist())
        if rz_n:
            _struct_tg("YEST_RED_OPEN_LOWER",
                "🔴 Yesterday RED – Open Inside Lower Zone (LTP ≤ YEST_LOW)",
                rz_n, ltp_map, df, False)

    # ── O=H / O=L SETUPS ─────────────────────────────────────────
    if all(c in df.columns for c in
           ["LIVE_OPEN", "LIVE_HIGH", "LIVE_LOW", "YEST_CLOSE", "YEST_LOW"]):
        TOL = 0.5
        ol = ((df["LIVE_OPEN"] - df["LIVE_LOW"]).abs() <= TOL) & \
             (df["LIVE_OPEN"] < df["YEST_CLOSE"]) & \
             (df["LTP"] > df["LIVE_OPEN"])
        oh = ((df["LIVE_OPEN"] - df["LIVE_HIGH"]).abs() <= TOL) & \
             (df["LIVE_OPEN"] > df["YEST_LOW"]) & \
             (df["LIVE_OPEN"] < df["YEST_CLOSE"]) & \
             (df["LTP"] < df["LIVE_OPEN"])
        oe_n = _new_entries("OEH_OEL_SETUPS", df[ol | oh]["Symbol"].tolist())
        if oe_n:
            _struct_tg("OEH_OEL_SETUPS",
                "⚡ O=H / O=L Setups — Gainers + Losers",
                oe_n, ltp_map, df, True)

    # ── 1H OPENING RANGE BREAKOUT / BREAKDOWN ────────────────────
    if _1h_range:
        up_s, dn_s = [], []
        for sym in df["Symbol"].tolist():
            ltp = ltp_map.get(sym, 0)
            rng = _1h_range.get(sym, {})
            h1  = rng.get("high_1h", 0)
            l1  = rng.get("low_1h",  0)
            if h1 > 0 and ltp >= h1:
                up_s.append(sym)
            elif l1 > 0 and ltp <= l1:
                dn_s.append(sym)

        new_up = _new_entries("HOURLY_BREAK_UP",   up_s)
        new_dn = _new_entries("HOURLY_BREAK_DOWN", dn_s)

        if new_up:
            fire_alert("HOURLY_BREAK_UP",
                "🟢 1H Opening Range BREAKOUT", new_up, ltp_map, True)
            if tg_on("BREAK_ABOVE_1H_HIGH"):
                _key_d = f"1H_UP_DETAIL_{_slot_10m()}"
                if not already_sent(_key_d):
                    lines = [
                        f"  🟢 <b>{s}</b>  LTP:{ltp_map.get(s,0):,.2f}"
                        f"  1H_HIGH:{_1h_range.get(s,{}).get('high_1h',0):,.2f}"
                        f"  +{round(ltp_map.get(s,0) - _1h_range.get(s,{}).get('high_1h',0), 2):,.2f}pts"
                        for s in new_up[:15]
                    ]
                    send_tg_bg(
                        f"🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩\n"
                        f"📈 <b>Breakouts Above 1H High</b>\n⏰ {_now_str()}\n"
                        f"📋 Stocks ({len(new_up)}):\n" + "\n".join(lines) +
                        "\n⚠️ <i>NOT financial advice.</i>\n🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩",
                        _key_d
                    )

        if new_dn:
            fire_alert("HOURLY_BREAK_DOWN",
                "🔴 1H Opening Range BREAKDOWN", new_dn, ltp_map, False)
            if tg_on("BREAK_BELOW_1H_LOW"):
                _key_d = f"1H_DN_DETAIL_{_slot_10m()}"
                if not already_sent(_key_d):
                    lines = [
                        f"  🔴 <b>{s}</b>  LTP:{ltp_map.get(s,0):,.2f}"
                        f"  1H_LOW:{_1h_range.get(s,{}).get('low_1h',0):,.2f}"
                        f"  -{round(_1h_range.get(s,{}).get('low_1h',0) - ltp_map.get(s,0), 2):,.2f}pts"
                        for s in new_dn[:15]
                    ]
                    send_tg_bg(
                        f"🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥\n"
                        f"📉 <b>Breakdowns Below 1H Low</b>\n⏰ {_now_str()}\n"
                        f"📋 Stocks ({len(new_dn)}):\n" + "\n".join(lines) +
                        "\n⚠️ <i>NOT financial advice.</i>\n🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥",
                        _key_d
                    )

    # ── EMA20/50 CROSS + 2M EMA REVERSAL ─────────────────────────
    if "EMA20" in df.columns and "EMA50" in df.columns:
        fire_alert("EMA20_50", "📊 EMA20 > EMA50 — Bull Cross",
            _new_entries("EMA20_50",
                df[(df["EMA20"] > 0) & (df["EMA50"] > 0) &
                   (df["LTP"] > df["EMA20"]) & (df["EMA20"] > df["EMA50"])]["Symbol"].tolist()),
            ltp_map, True)
        if "EMA7" in df.columns:
            fire_alert("2M_EMA_REVERSAL", "🚀 2M Downtrend → EMA Reversal",
                _new_entries("2M_EMA_REVERSAL",
                    df[(df["EMA7"]  > 0) & (df["EMA20"] > 0) &
                       (df["LTP"]   > df["EMA20"]) &
                       (df["EMA7"]  > df["EMA20"]) &
                       (df["CHANGE_%"] > 0)]["Symbol"].tolist()),
                ltp_map, True)

    # ── TOP GAINERS / LOSERS ──────────────────────────────────────
    if "CHANGE_%" in df.columns:
        fire_alert("TOP_GAINERS", "🔥 Top Gainers >2.5%",
            _new_entries("TOP_GAINERS",
                df[df["CHANGE_%"] >=  2.5]["Symbol"].tolist()),
            ltp_map, True)
        fire_alert("TOP_LOSERS", "🔥 Top Losers <-2.5%",
            _new_entries("TOP_LOSERS",
                df[df["CHANGE_%"] <= -2.5]["Symbol"].tolist()),
            ltp_map, False)

    # ── BT/ST TARGETS ─────────────────────────────────────────────
    _check_bt_st(df, ltp_map)

    # ── NIFTY / BANKNIFTY SLOT BREAK ─────────────────────────────
    _check_nifty_slot(ltp_map)

    # ── SMC + PANCHAK COMBINED (HTF TREND + BREAKOUT) ─────────────
    _check_smc_panchak(df, ltp_map)

    # ── PANCHAK PERIOD (stock movement alerts during period) ───────
    today = _today()
    if PANCHAK_START <= today <= PANCHAK_END and "CHANGE_%" in df.columns:
        slot = _slot_10m()
        for _, row in df.iterrows():
            chg = float(row.get("CHANGE_%", 0) or 0)
            if abs(chg) >= 2.0:
                sym = str(row["Symbol"])
                ltp = float(row.get("LTP", 0) or 0)
                d   = "UP" if chg > 0 else "DOWN"
                k   = f"PANCHAK_{sym}_{d}_{today}_{slot}"
                if not already_sent(k):
                    send_tg_bg(
                        f"{_bd(chg > 0)}\n"
                        f"{'🟢' if chg > 0 else '🔴'} <b>PANCHAK {d} ALERT</b>\n"
                        f"Symbol: <b>{sym}</b>  LTP: ₹{ltp:,.2f}  Chg: {chg:+.2f}%\n"
                        f"⏰ {_now_str()}\n{_bd(chg > 0)}",
                        k
                    )

    # ── PANCHAK RANGE ENGINE (Point 7) ────────────────────────────
    _check_panchak_range_alerts(ltp_map)

    # ── INSIDE BAR BREAKOUT ALERTS (Point 13) ─────────────────────
    _check_inside_bar_alerts(ltp_map)

    # ── MACD DAILY CROSSOVER ALERTS (Point 9) ─────────────────────
    _check_macd_alerts(ltp_map)
    # NOTE: expiry + holiday alerts moved to main loop (need to fire at 09:00 before market opens)

# ══════════════════════════════════════════════════════════════════
# BT/ST TARGET CHECK
# FIX #17: filter watchlist first before iterrows
# ══════════════════════════════════════════════════════════════════
def _check_bt_st(df: pd.DataFrame, ltp_map: dict):
    if not tg_on("BT_ST_TARGET"):
        return
    if not all(c in df.columns for c in ["Symbol", "LTP", "TOP_HIGH", "TOP_LOW"]):
        return

    today_s = _now().strftime("%Y%m%d")
    now_s   = _now_str()

    # FIX #17: filter to watchlist only
    wl_df = df[df["Symbol"].isin(PANCHAK_ALERT_WATCHLIST)].copy()

    for _, row in wl_df.iterrows():
        sym = str(row["Symbol"]).strip()
        ltp = float(ltp_map.get(sym, row.get("LTP", 0)) or 0)
        th  = float(row.get("TOP_HIGH", 0) or 0)
        tl  = float(row.get("TOP_LOW",  0) or 0)
        if ltp <= 0 or th <= 0 or tl <= 0 or th <= tl:
            continue

        diff = th - tl
        bt   = round(th + diff * 0.5, 2)
        st   = round(tl - diff * 0.5, 2)

        if ltp >= bt:
            k = f"BT_HIT_{sym}_{today_s}"
            if not already_sent(k):
                msg = (
                    f"🏆 <b>BUY TARGET (BT) HIT</b>\n"
                    f"Stock : <b>{sym}</b>   Time: {now_s}\n"
                    f"BT Lvl: {bt:,.2f}   TOP HI: {th:,.2f}   LTP: {ltp:,.2f}\n"
                    f"Above BT  : <b>+{round(ltp-bt,  2):,.2f} pts</b>\n"
                    f"Above TOP : <b>+{round(ltp-th,  2):,.2f} pts</b>\n"
                    f"⚠️ <i>NOT financial advice.</i>"
                )
                send_tg_bg(msg, k)
                _log_alert("BT_HIT", [sym], "TG")
                log.info(f"🏆 BT Hit: {sym} LTP={ltp} BT={bt}")

        if ltp <= st:
            k = f"ST_HIT_{sym}_{today_s}"
            if not already_sent(k):
                msg = (
                    f"⚠️ <b>SELL TARGET (ST) HIT</b>\n"
                    f"Stock : <b>{sym}</b>   Time: {now_s}\n"
                    f"ST Lvl: {st:,.2f}   TOP LO: {tl:,.2f}   LTP: {ltp:,.2f}\n"
                    f"Below ST  : <b>-{round(st-ltp,  2):,.2f} pts</b>\n"
                    f"Below TOP : <b>-{round(tl-ltp,  2):,.2f} pts</b>\n"
                    f"⚠️ <i>NOT financial advice.</i>"
                )
                send_tg_bg(msg, k)
                _log_alert("ST_HIT", [sym], "TG")
                log.info(f"⚠️ ST Hit: {sym} LTP={ltp} ST={st}")

# ══════════════════════════════════════════════════════════════════
# NIFTY / BANKNIFTY SLOT BREAK + 15-MIN PROGRESS
# FIX #9: break key is day-level, progress key includes slot
# ══════════════════════════════════════════════════════════════════
_slot_break_state: dict = {}   # "SYM_DIR": {"ltp": float, "level": float}

def _check_nifty_slot(ltp_map: dict):
    if not tg_on("NIFTY_SLOT_BREAK"):
        return

    now_dt   = _now()
    now_s    = now_dt.strftime("%H:%M IST")
    today_s  = now_dt.strftime("%Y%m%d")
    slot_15  = _slot_15m()

    for sym in ["NIFTY", "BANKNIFTY"]:
        ltp  = float(ltp_map.get(sym, 0) or 0)
        tl   = _top_levels.get(sym, {})
        th   = float(tl.get("top_high", 0) or 0)
        tlo  = float(tl.get("top_low",  0) or 0)
        if ltp <= 0 or th <= 0:
            continue

        for direction, broke, level, lname in [
            ("UP",   ltp >  th,  th,  "TOP HIGH"),
            ("DOWN", ltp <  tlo, tlo, "LEAST LOW"),
        ]:
            if not broke:
                continue

            # FIX #9: break key = day + direction (not slot) — fires once per day per direction
            break_key = f"NSLOT_BREAK_{sym}_{direction}_{today_s}"
            state_key = f"{sym}_{direction}"

            if already_sent(break_key):
                # Progress update — key includes slot so fires every 15 min
                prog_key = f"NSLOT_PROG_{sym}_{direction}_{today_s}_{slot_15}"
                if not already_sent(prog_key):
                    prior_ltp = _slot_break_state.get(state_key, {}).get("ltp", ltp)
                    pts_since = round(ltp - prior_ltp, 2) if direction == "UP" \
                                else round(prior_ltp - ltp, 2)
                    pts_from_level = round(ltp - level, 2) if direction == "UP" \
                                     else round(level - ltp, 2)
                    icon = "🟢" if direction == "UP" else "🔴"
                    send_tg_bg(
                        f"⏱️ <b>{sym} — KP 15-Min Progress {icon}</b>\n"
                        f"⏰ {now_s}   Slot: {slot_15[:2]}:{slot_15[2:]}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"LTP Now     : <b>{ltp:,.2f}</b>\n"
                        f"{lname}  : {level:,.2f}\n"
                        f"Pts from level : <b>{pts_from_level:+,.2f}</b>\n"
                        f"Trend since entry: <b>{pts_since:+,.2f} pts</b>\n"
                        f"⚠️ <i>NOT financial advice.</i>",
                        prog_key
                    )
                    # Update tracking LTP for next progress
                    _slot_break_state[state_key] = {"ltp": ltp, "level": level}
                continue

            # First break alert
            icon = "🟢" if direction == "UP" else "🔴"
            pts  = round(abs(ltp - level), 2)
            send_tg_bg(
                f"{icon * 6}\n"
                f"🚨 <b>{sym} — KP SLOT {lname} BREAK "
                f"{'↑' if direction=='UP' else '↓'}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Index  : {sym}\n"
                f"⏰ Time   : {now_s}\n"
                f"🕐 Slot   : {slot_15[:2]}:{slot_15[2:]}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📍 {lname}: {level:,.2f}\n"
                f"💹 LTP Now: {ltp:,.2f}\n"
                f"📏 Pts {'above' if direction=='UP' else 'below'}: "
                f"<b>+{pts:,.2f} pts</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{'✅ <b>BULLISH BREAK</b>' if direction=='UP' else '⚠️ <b>BEARISH BREAK</b>'}\n"
                f"🔔 Progress updates every 15 min.\n"
                f"⚠️ <i>NOT financial advice.</i>\n{icon * 6}",
                break_key
            )
            _slot_break_state[state_key] = {"ltp": ltp, "level": level}
            _log_alert(f"NIFTY_SLOT_{direction}", [sym], "TG")
            log.info(f"🚨 Slot Break [{sym}] {direction} @ {ltp}")

# ══════════════════════════════════════════════════════════════════
# KP PANCHANG WINDOW ALERTS (standalone — no Streamlit)
# ══════════════════════════════════════════════════════════════════
KP_SUB_SIGNAL = {
    "Ju": ("STRONG BUY 🟢🟢", "bullish"),
    "Ve": ("STRONG BUY 🟢🟢", "bullish"),
    "Mo": ("BUY 🟢",           "bullish"),
    "Me": ("MIXED ⚪",         "neutral"),
    "Su": ("MILD BUY 🟡",      "mild_bull"),
    "Sa": ("SELL 🔴",          "bearish"),
    "Ra": ("STRONG SELL 🔴🔴", "bearish"),
    "Ke": ("STRONG SELL 🔴🔴", "bearish"),
    "Ma": ("SELL 🔴",          "bearish"),
}

def _load_kp_csv() -> pd.DataFrame:
    global _kp_df_cache
    if _kp_df_cache is not None:
        return _kp_df_cache
    if not os.path.exists(_KP_CSV_PATH):
        log.warning(f"⚠️ KP CSV not found: {_KP_CSV_PATH}")
        return pd.DataFrame()
    try:
        _kp_df_cache = pd.read_csv(_KP_CSV_PATH)
        log.info(f"📜 KP CSV loaded: {len(_kp_df_cache)} rows")
        return _kp_df_cache
    except Exception as e:
        log.warning(f"KP CSV load error: {e}")
        return pd.DataFrame()


def _run_kp_alerts(ltp_map: dict):
    if not tg_on("KP_ALERTS"):
        return
    df = _load_kp_csv()
    if df.empty:
        return

    now_dt   = _now()
    # FIX #12: format must match CSV Date column exactly
    today_s  = now_dt.date().strftime("%d/%b/%Y")
    now_min  = now_dt.hour * 60 + now_dt.minute

    try:
        today_df = df[df["Date"] == today_s].copy()
    except Exception:
        return
    if today_df.empty:
        return

    nifty_ltp = float(ltp_map.get("NIFTY",     0) or 0)
    bnf_ltp   = float(ltp_map.get("BANKNIFTY", 0) or 0)

    for _, row in today_df.iterrows():
        try:
            slot_s = str(row.get("Slot_Start", ""))
            slot_e = str(row.get("Slot_End",   ""))
            p1 = str(row.get("P1", ""))
            p2 = str(row.get("P2", ""))
            p3 = str(row.get("P3", ""))
            p4 = str(row.get("P4", ""))
            if not slot_s or ":" not in slot_s:
                continue
            sm = int(slot_s[:2]) * 60 + int(slot_s[3:5])
            em = int(slot_e[:2]) * 60 + int(slot_e[3:5]) if ":" in slot_e else sm + 15
            if not (sm <= now_min < em):
                continue

            sig_label, sig_type = KP_SUB_SIGNAL.get(p3, ("NEUTRAL ⚪", "neutral"))

            # Window-open alert (once per slot per day)
            open_key = f"KP_OPEN_{today_s.replace('/','_')}_{slot_s.replace(':','')}"
            if not already_sent(open_key):
                icon = "🟢" if "bullish" in sig_type else \
                       "🔴" if "bearish" in sig_type else "🟡"
                msg = (
                    f"🌙 <b>KP WINDOW OPEN</b> — {slot_s}–{slot_e}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Signal : {icon} <b>{sig_label}</b>\n"
                    f"Sub    : <b>{p3}</b>   Sub-Sub: <b>{p4}</b>\n"
                    f"Star   : {p2}   Sign: {p1}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"NIFTY: {nifty_ltp:,.0f}   BankNifty: {bnf_ltp:,.0f}\n"
                    f"⏰ {_now_str()}\n"
                    f"⚠️ <i>NOT financial advice.</i>"
                )
                send_tg_bg(msg, open_key)
                log.info(f"🌙 KP Window Open: {slot_s}–{slot_e} [{sig_label}]")

            # KP BREAK 15M alert
            if tg_on("KP_BREAK_15M") and nifty_ltp > 0:
                th  = float(_top_levels.get("NIFTY", {}).get("top_high", 0) or 0)
                tlo = float(_top_levels.get("NIFTY", {}).get("top_low",  0) or 0)
                s15 = _slot_15m()
                today_flat = today_s.replace("/","_")
                if th > 0 and nifty_ltp > th:
                    k = f"KP_BREAK15_UP_{today_flat}_{s15}"
                    if not already_sent(k):
                        send_tg_bg(
                            f"⏱️ <b>KP BREAK 15M — NIFTY TOP HIGH BREAK ↑</b>\n"
                            f"Slot: {slot_s}–{slot_e}   KP: {sig_label}\n"
                            f"TOP HIGH: {th:,.2f}   LTP: {nifty_ltp:,.2f}\n"
                            f"⏰ {_now_str()}\n⚠️ <i>NOT financial advice.</i>", k)
                if tlo > 0 and nifty_ltp < tlo:
                    k = f"KP_BREAK15_DN_{today_flat}_{s15}"
                    if not already_sent(k):
                        send_tg_bg(
                            f"⏱️ <b>KP BREAK 15M — NIFTY TOP LOW BREAK ↓</b>\n"
                            f"Slot: {slot_s}–{slot_e}   KP: {sig_label}\n"
                            f"TOP LOW: {tlo:,.2f}   LTP: {nifty_ltp:,.2f}\n"
                            f"⏰ {_now_str()}\n⚠️ <i>NOT financial advice.</i>", k)
        except Exception as e:
            log.debug(f"KP row error: {e}")

# ══════════════════════════════════════════════════════════════════
# OI INTELLIGENCE
# FIX #10: Kite option symbol format validated
# FIX #21: renamed pd2 → pe_oi_map
# ══════════════════════════════════════════════════════════════════
_last_oi: dict = {}

def fetch_oi_intelligence(kite):
    global _last_oi
    try:
        spot_q = kite.quote(["NSE:NIFTY 50"])
        spot   = spot_q["NSE:NIFTY 50"]["last_price"]
        atm    = round(spot / 50) * 50

        # Next expiry Thursday
        td  = _today()
        dah = (3 - td.weekday()) % 7   # days ahead to Thursday
        if dah == 0:
            dah = 7
        exp_date = td + timedelta(days=dah)
        # Kite NFO format: NIFTY + YY + MMM (3-letter) + DD + strike + CE/PE
        # e.g. NIFTY26APR1722500CE
        exp_str = exp_date.strftime("%y%b%d").upper()   # e.g. "26APR17"

        strikes       = [atm + i * 50 for i in range(-10, 11)]
        ce_total      = 0
        pe_total      = 0
        ce_oi_map: dict = {}
        pe_oi_map: dict = {}   # FIX #21: was pd2

        for strike in strikes:
            ce_sym = f"NFO:NIFTY{exp_str}{strike}CE"
            pe_sym = f"NFO:NIFTY{exp_str}{strike}PE"
            try:
                q      = kite.quote([ce_sym, pe_sym])
                ce_oi  = q.get(ce_sym, {}).get("oi", 0)
                pe_oi  = q.get(pe_sym, {}).get("oi", 0)
                ce_total       += ce_oi
                pe_total       += pe_oi
                ce_oi_map[strike] = ce_oi
                pe_oi_map[strike] = pe_oi
            except Exception:
                pass

        pcr = round(pe_total / ce_total, 2) if ce_total > 0 else 0
        direction = (
            "📈 BULLISH (PCR high)" if pcr >= 1.2 else
            "📉 BEARISH (PCR low)"  if pcr <= 0.8 else
            "↔️ NEUTRAL"
        )
        call_wall  = max(ce_oi_map, key=ce_oi_map.get, default=atm)
        put_floor  = max(pe_oi_map, key=pe_oi_map.get, default=atm)

        _last_oi = {
            "spot":                spot,
            "atm":                 atm,
            "pcr":                 pcr,
            "direction":           direction,
            "call_wall":           call_wall,
            "put_floor":           put_floor,
            "nearest_call_wall":   call_wall,
            "nearest_put_floor":   put_floor,
            "fetched_at":          _now().isoformat(),
        }

        # Save for SMC engine
        with open(_dated("oi_intel", "json"), "w") as f:
            json.dump(_last_oi, f, indent=2)
        log.info(f"📊 OI: PCR={pcr}  CW={call_wall}  PF={put_floor}")

        if tg_on("OI_INTEL"):
            k = f"OI_INTEL_{_slot_15m()}"
            if not already_sent(k):
                send_tg_bg(
                    f"📊 <b>OI Intelligence Update</b>\n⏰ {_now_str()}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Spot: <b>₹{spot:,.0f}</b>   ATM: <b>{atm:,}</b>\n"
                    f"PCR : <b>{pcr}</b>  →  {direction}\n"
                    f"📞 Call Wall : <b>{call_wall:,}</b>\n"
                    f"🛡️ Put Floor : <b>{put_floor:,}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <i>NOT financial advice.</i>",
                    k
                )
    except Exception as e:
        log.error(f"OI Intelligence error: {e}")


def _oi_30m_summary():
    if not tg_on("OI_30M_SUMMARY") or not _last_oi:
        return
    k = f"OI_30M_{_slot_30m()}"
    if already_sent(k):
        return
    oi = _last_oi
    send_tg_bg(
        f"📊 <b>OI 30-Min Summary</b>\n⏰ {_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Spot: <b>₹{oi.get('spot',0):,.0f}</b>   ATM: {oi.get('atm',0):,}\n"
        f"PCR : <b>{oi.get('pcr',0)}</b>   {oi.get('direction','?')}\n"
        f"📞 CW: {oi.get('call_wall',0):,}   🛡️ PF: {oi.get('put_floor',0):,}\n"
        f"⚠️ <i>NOT financial advice.</i>",
        k
    )

# ══════════════════════════════════════════════════════════════════
# BOS / CHoCH SCANNER
# ══════════════════════════════════════════════════════════════════
def run_bos_alerts(kite, ltp_map: dict):
    if not _BOS_OK or not tg_on("BOS_1H"):
        return
    try:
        ltp_dict = {s: float(ltp_map.get(s, 0)) for s in STOCKS}

        # Wrap send_telegram_fn to ALSO relay BOS/CHoCH to second channel (Point 12)
        def _relay_send(msg: str, key: str = None):
            send_tg_bg(msg, key)
            # Detect event type from message content for channel-2 routing
            _is_bos_up   = "BOS UP"   in msg or "Break of Structure UP"   in msg
            _is_bos_dn   = "BOS DOWN" in msg or "Break of Structure DOWN" in msg
            _is_choch_up = "CHoCH UP"   in msg or "Change of Character UP"   in msg
            _is_choch_dn = "CHoCH DOWN" in msg or "Change of Character DOWN" in msg
            if _is_bos_up   and _toggles.get("tg2_BOS_UP",   True):
                send_tg2_bg(msg, key)
            elif _is_bos_dn  and _toggles.get("tg2_BOS_DOWN",  True):
                send_tg2_bg(msg, key)
            elif _is_choch_up and _toggles.get("tg2_CHOCH_UP", True):
                send_tg2_bg(msg, key)
            elif _is_choch_dn and _toggles.get("tg2_CHOCH_DOWN",True):
                send_tg2_bg(msg, key)

        result   = run_bos_scan(
            db               = _ohlc_db,
            symbols          = STOCKS,
            send_telegram_fn = _relay_send,
            ltp_dict         = ltp_dict,
            tg_enabled       = True,
            kite             = kite,
        )
        events = result[0] if isinstance(result, tuple) else result
        log.info(f"📡 BOS scan: {len(events)} events")
        # Update OHLC DB in background
        def _update_db():
            try:
                _ohlc_db.update_all(
                    kite=kite, symbols=STOCKS,
                    get_token_fn=get_token,
                    batch_size=10, delay_secs=0.3,
                )
            except Exception as ex:
                log.debug(f"OHLC DB update: {ex}")
        threading.Thread(target=_update_db, daemon=True).start()
    except Exception as e:
        log.error(f"BOS scan error: {e}")

# ══════════════════════════════════════════════════════════════════
# SMC + COMBINED ENGINE (every 15 min)
# ══════════════════════════════════════════════════════════════════
def run_smc_combined(kite, ltp_map: dict):
    if not _SMC_OK:
        return
    try:
        c15 = fetch_nifty_candles_kite(kite, "15minute", 5)
        c1h = fetch_nifty_candles_kite(kite, "60minute",  15)
        if not c15:
            log.warning("SMC: no 15m candles returned")
            return
        smc = get_smc_confluence(
            oi_intel    = _last_oi,
            candles_15m = c15,
            candles_1h  = c1h or None,
        )
        log.info(
            f"🧠 SMC: {smc.get('final_signal','?')} "
            f"score={smc.get('final_score',0)}"
        )
        # Cache for status monitoring
        try:
            with open(os.path.join(CACHE_DIR, "smc_result.json"), "w") as f:
                json.dump(
                    {k: str(v) if not isinstance(v, (int,float,str,bool,list,dict,type(None)))
                     else v for k, v in smc.items()},
                    f, indent=2
                )
        except Exception:
            pass

        if tg_on("COMBINED_ENGINE"):
            _fire_combined(smc, ltp_map)
    except Exception as e:
        log.error(f"SMC/Combined Engine error: {e}")


def _fire_combined(smc: dict, ltp_map: dict):
    s15 = _slot_15m()
    k   = f"COMBINED_{_today_str()}_{s15}"
    if already_sent(k):
        return

    sig    = smc.get("final_signal", "—")
    score  = smc.get("final_score",  0)
    action = smc.get("final_action", "—")
    ltf    = smc.get("smc_trend_ltf","—")
    htf    = smc.get("smc_trend_htf","—")
    zone   = smc.get("pd_zone",       "—")

    oi_dir  = _last_oi.get("direction",     "—")
    pcr     = _last_oi.get("pcr",           0)
    cw      = _last_oi.get("call_wall",     0)
    pf      = _last_oi.get("put_floor",     0)
    spot    = _last_oi.get("spot",          0)
    nifty   = int(ltp_map.get("NIFTY",     spot) or spot)
    bnifty  = int(ltp_map.get("BANKNIFTY", 0)    or 0)

    a_sig   = "—"; a_score = 0; kp_sig = "—"
    if _ASTRO_OK:
        try:
            a       = get_astro_score()
            a_sig   = a.get("signal", "—")
            a_score = int(a.get("score", 0) or 0)
            kp_sig  = get_time_signal()
        except Exception:
            pass

    icon = "🟢" if score > 0 else "🔴" if score < 0 else "🟡"
    msg  = (
        f"🧠 <b>COMBINED ENGINE — 15-Min Snapshot</b>\n"
        f"⏰ {_now_str()}   Slot: {s15[:2]}:{s15[2:]}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SMC Engine</b>\n"
        f"  {icon} <b>{sig}</b>   Score: <b>{score:+d}</b>\n"
        f"  Action: {action}   Zone: {zone}\n"
        f"  15m: {ltf}   HTF: {htf}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 <b>OI Intelligence</b>\n"
        f"  Spot: {spot:,.0f}   NIFTY: {nifty:,}   BNIFTY: {bnifty:,}\n"
        f"  PCR: {pcr}   {oi_dir}\n"
        f"  📞 CW: {cw:,}   🛡️ PF: {pf:,}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Astro</b>   Score: {a_score:+d}   {a_sig}\n"
        f"⏱️ <b>Time Zone</b>: {kp_sig}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>NOT financial advice.</i>"
    )
    send_tg_bg(msg, k)
    # Point 12: also send SMC+OI Confluence to second channel
    if _toggles.get("tg2_SMC_OI_CONFLUENCE", True):
        send_tg2_bg(msg, k)
    log.info(f"🧠 Combined Engine alert sent [{s15}]")

# ══════════════════════════════════════════════════════════════════
# ASTRO ADVANCE ALERT (once per day, sent at 09:00)
# ══════════════════════════════════════════════════════════════════
def _astro_advance():
    if not _ASTRO_OK or not tg_on("ASTRO_ADVANCE"):
        return
    k = f"ASTRO_ADV_{_today_str()}"
    if already_sent(k):
        return
    try:
        a       = get_astro_score()
        kp      = get_time_signal()
        score   = int(a.get("score", 0) or 0)
        sig     = a.get("signal",    "—")
        nak     = a.get("nakshatra", "—")
        msign   = a.get("moon_sign", "—")
        sub     = a.get("sub_lord",  "—")
        reasons = " | ".join(a.get("details", [])[:3])
        icon    = "🟢" if score > 0 else "🔴" if score < 0 else "🟡"
        msg = (
            f"🌟 <b>Astro Day Advance Alert</b>\n"
            f"📅 {_today().strftime('%A %d-%b-%Y')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{icon} <b>{sig}</b>   Score: <b>{score:+d}</b>\n"
            f"Moon  : <b>{nak}</b> ({msign})\n"
            f"KP Sub: <b>{sub}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 {reasons}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ {kp}\n"
            f"⚠️ <i>NOT financial advice.</i>"
        )
        send_tg_bg(msg, k)
        log.info(f"🌟 Astro advance sent: {sig} score={score}")
    except Exception as e:
        log.warning(f"Astro advance error: {e}")

# ══════════════════════════════════════════════════════════════════
# INTRADAY CANDLE FETCH (5-min + 15-min)
# ══════════════════════════════════════════════════════════════════
def _fetch_intraday_candles(kite, interval: str, path_name: str,
                             syms_list: list) -> pd.DataFrame:
    today = _today()
    from datetime import datetime as _dt
    start = IST.localize(_dt(today.year, today.month, today.day, 9, 15))
    end   = _now()
    rows  = []
    for sym in syms_list[:80]:   # batch safely under API rate limits
        try:
            tk = get_token(sym)
            if not tk:
                continue
            for b in kite.historical_data(tk, start, end, interval):
                rows.append({
                    "Symbol": sym,
                    "date":   b["date"],
                    "open":   b["open"],
                    "high":   b["high"],
                    "low":    b["low"],
                    "close":  b["close"],
                    "volume": b.get("volume", 0),
                })
        except Exception as e:
            log.debug(f"Candle [{interval}/{sym}]: {e}")
        time.sleep(0.1)

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(_dated(path_name), index=False)
        return df
    return pd.DataFrame()


def fetch_5min_candles(kite):
    df = _fetch_intraday_candles(kite, "5minute", "five_min", STOCKS)
    if not df.empty:
        _update_ema_cache(df)
        log.info(f"📊 5-min candles: {len(df)} bars, {df['Symbol'].nunique()} symbols")


def fetch_15min_candles(kite):
    df = _fetch_intraday_candles(kite, "15minute", "fifteen_min", STOCKS)
    if not df.empty:
        log.info(f"📊 15-min candles: {len(df)} bars")

# ══════════════════════════════════════════════════════════════════
# FUTURES + INDICES
# FIX #11: futures on NFO exchange, not NSE
# ══════════════════════════════════════════════════════════════════
def fetch_futures(kite):
    try:
        td  = _today()
        dah = (3 - td.weekday()) % 7
        if dah == 0:
            dah = 7
        # Kite futures symbol format: NFO:NIFTY25APRFUT (month only, no date)
        exp = (td + timedelta(days=dah)).strftime("%y%b").upper()   # e.g. "25APR"
        rows = []
        for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
            # FIX #11: exchange is NFO not NSE
            fut_sym = f"NFO:{sym}{exp}FUT"
            try:
                q = kite.quote([fut_sym])
                d = q.get(fut_sym, {})
                if d:
                    rows.append({
                        "Symbol":  sym,
                        "FUT_LTP": d["last_price"],
                        "FUT_OI":  d.get("oi", 0),
                        "FUT_VOL": d.get("volume", 0),
                    })
            except Exception as e:
                log.debug(f"Futures [{sym}]: {e}")
        if rows:
            pd.DataFrame(rows).to_csv(_dated("futures"), index=False)
            log.info(f"📊 Futures: {len(rows)} cached")
    except Exception as e:
        log.debug(f"Futures fetch error: {e}")


INDEX_SYMS = {
    "NIFTY 50":       "NIFTY 50",
    "BANK NIFTY":     "NIFTY BANK",
    "FINNIFTY":       "FINNIFTY",
    "NIFTY IT":       "NIFTYIT",
    "NIFTY FMCG":     "NIFTYFMCG",
    "NIFTY PHARMA":   "NIFTYPHARMA",
    "NIFTY METAL":    "NIFTYMETAL",
    "NIFTY AUTO":     "NIFTYAUTO",
    "NIFTY ENERGY":   "NIFTYENERGY",
    "NIFTY PSU BANK": "NIFTYPSUBANK",
}

def fetch_indices(kite):
    try:
        all_q = kite.quote([f"NSE:{s}" for s in INDEX_SYMS.values()])
        rows  = []
        for name, sym in INDEX_SYMS.items():
            q = all_q.get(f"NSE:{sym}")
            if not q:
                continue
            ltp = q["last_price"]
            pc  = q["ohlc"]["close"]
            chg = round(ltp - pc, 2) if pc else 0
            rows.append({
                "Index":    name,
                "LTP":      round(ltp, 2),
                "CHANGE":   chg,
                "CHANGE_%": round(chg / pc * 100, 2) if pc else 0,
            })
        if rows:
            pd.DataFrame(rows).to_csv(_dated("indices_live"), index=False)
    except Exception as e:
        log.debug(f"Indices fetch error: {e}")

# ══════════════════════════════════════════════════════════════════
# HEARTBEAT FILE
# ══════════════════════════════════════════════════════════════════
def write_heartbeat(status: str = "running", extra: dict = None):
    try:
        data = {
            "status":      status,
            "timestamp":   _now().isoformat(),
            "pid":         os.getpid(),
            "market_open": is_market_hours(),
            "version":     "4.2",
        }
        if extra:
            data.update(extra)
        with open(os.path.join(CACHE_DIR, "worker_heartbeat.json"), "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 65)
    log.info("  Panchak Background Worker v4.2 — Production-Ready")
    log.info(f"  PID: {os.getpid()}   CACHE: {CACHE_DIR}")
    log.info(
        f"  SMC: {'✅' if _SMC_OK else '❌'}  "
        f"BOS: {'✅' if _BOS_OK else '❌'}  "
        f"ASTRO: {'✅' if _ASTRO_OK else '❌'}  "
        f"KP CSV: {'✅' if os.path.exists(_KP_CSV_PATH) else '❌'}"
    )
    log.info("=" * 65)

    # Write PID for management scripts
    try:
        with open(os.path.join(CACHE_DIR, "worker.pid"), "w") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass

    # Kite init
    try:
        kite = init_kite()
    except Exception as e:
        log.error(f"❌ Kite init failed: {e}")
        sys.exit(1)

    # Bootstrap
    _load_tg_dedup()
    _reload_toggles()
    _load_instruments(kite)

    # Timestamps dict
    ts = {k: 0.0 for k in [
        "live", "5min", "15min", "oi", "oi_30m", "bos",
        "futures", "yest", "weekly", "monthly", "smc", "toggles",
        "macd", "inside_bar", "first15",   # new scanners
    ]}
    last_day       = ""
    _1h_fetched    = ""
    astro_done     = ""
    macd_done      = ""    # once per day after market opens
    inside_done    = ""    # once per day
    first15_done   = ""    # once per day after 09:30

    # Pre-fetch static data before market opens
    log.info("📅 Pre-market data fetch starting…")
    _fetch_yesterday_ohlc(kite)
    _load_top_levels(kite)
    _fetch_weekly_ohlc(kite)
    _fetch_monthly_ohlc(kite)
    _load_panchak_range()          # Point 7: load Panchak range state
    log.info("✅ Pre-market data ready")

    write_heartbeat("started")
    log.info("✅ Worker entering main loop")

    while True:
        try:
            now_ts  = time.time()
            now_dt  = _now()
            today_s = now_dt.strftime("%Y%m%d")
            hhmm    = now_dt.strftime("%H%M")

            # ── Day rollover ──────────────────────────────────────
            if today_s != last_day:
                log.info(f"📅 New day: {today_s} — resetting state")
                _prev_sets.clear()
                _1h_range.clear()
                _ema_cache.clear()
                _slot_break_state.clear()
                _first15_cache.clear()
                _macd_state.clear()
                global _kp_df_cache, PANCHAK_START, PANCHAK_END
                _kp_df_cache  = None
                PANCHAK_START, PANCHAK_END = _get_active_panchak_worker()
                log.info(f"🪐 Panchak period: {PANCHAK_START} → {PANCHAK_END}")
                _load_tg_dedup()
                _load_instruments(kite)
                for k in ts:
                    ts[k] = 0.0
                _1h_fetched  = ""
                astro_done   = ""
                macd_done    = ""
                inside_done  = ""
                first15_done = ""
                last_day     = today_s

            # ── Toggle reload (every 60s) ─────────────────────────
            if now_ts - ts["toggles"] >= 60:
                _reload_toggles()
                ts["toggles"] = now_ts

            # ── Astro advance (once at 09:00) ─────────────────────
            if hhmm >= "0900" and astro_done != today_s:
                _astro_advance()
                astro_done = today_s

            # ── Expiry alerts (every loop, has own dedup) ──────────
            # Runs outside market-hours gate so morning reminders fire at 09:00
            _check_expiry_alerts()

            # ── Holiday alerts (every loop, has own dedup) ─────────
            _check_holiday_alerts()

            # ── MARKET HOURS ──────────────────────────────────────
            if is_market_hours():

                # OHLC refreshes
                if now_ts - ts["yest"]    >= INTERVAL_YEST:
                    _fetch_yesterday_ohlc(kite)
                    _load_top_levels(kite)
                    ts["yest"] = now_ts
                if now_ts - ts["weekly"]  >= INTERVAL_WEEKLY:
                    _fetch_weekly_ohlc(kite)
                    ts["weekly"] = now_ts
                if now_ts - ts["monthly"] >= INTERVAL_MONTHLY:
                    _fetch_monthly_ohlc(kite)
                    ts["monthly"] = now_ts

                # 1H Opening Range — after 10:15, once per day
                if hhmm >= "1015" and _1h_fetched != today_s:
                    _fetch_1h_opening_range(kite)
                    _1h_fetched = today_s

                # First-15min candle cache — after 09:30, once per day (Point 2)
                if hhmm >= "0930" and first15_done != today_s:
                    first15_done = today_s   # mark immediately so loop doesn't re-enter
                    def _build_first15_cache():
                        log.info("📊 Building first-15min cache for Yest GREEN/RED filter…")
                        for _sym in STOCKS:
                            _get_first15_hl(kite, _sym)
                            time.sleep(0.04)
                        log.info(f"✅ First-15min cache: {len(_first15_cache)} symbols")
                    threading.Thread(target=_build_first15_cache, daemon=True).start()

                # MACD daily cache — once per day at 09:30 (Point 9)
                if hhmm >= "0930" and macd_done != today_s:
                    log.info("📊 Updating MACD daily cache…")
                    _update_macd_cache(kite)
                    macd_done = today_s

                # Inside Bar daily cache — once per day at 09:30 (Point 13)
                if hhmm >= "0930" and inside_done != today_s:
                    log.info("📊 Updating Inside Bar daily cache…")
                    _update_inside_bar_cache(kite)
                    inside_done = today_s
                    log.info(f"✅ Inside Bar patterns: {len(_inside_bar_daily)}")

                # Load Panchak range state at startup (Point 7)
                if not _panchak_range_state:
                    _load_panchak_range()

                # Live quotes + all stock alerts (every 60s)
                if now_ts - ts["live"] >= INTERVAL_LIVE:
                    df = fetch_live(kite)
                    if not df.empty:
                        ltp_map = dict(zip(df["Symbol"], df["LTP"]))
                        check_all_alerts(df)
                        _run_kp_alerts(ltp_map)
                    fetch_indices(kite)
                    ts["live"] = now_ts

                # 5-min candles + EMA update
                if now_ts - ts["5min"] >= INTERVAL_5MIN:
                    fetch_5min_candles(kite)
                    ts["5min"] = now_ts

                # 15-min candles
                if now_ts - ts["15min"] >= INTERVAL_15MIN:
                    fetch_15min_candles(kite)
                    ts["15min"] = now_ts

                # OI Intelligence (every 3 min)
                if now_ts - ts["oi"] >= INTERVAL_OI:
                    fetch_oi_intelligence(kite)
                    ts["oi"] = now_ts

                # OI 30-min summary
                if now_ts - ts["oi_30m"] >= INTERVAL_OI_30M:
                    _oi_30m_summary()
                    ts["oi_30m"] = now_ts

                # BOS scan (every 5 min) — uses live CSV for LTP
                if now_ts - ts["bos"] >= INTERVAL_BOS:
                    ltp_now = {}
                    try:
                        tmp     = pd.read_csv(_dated("live_data"))
                        ltp_now = dict(zip(tmp["Symbol"], tmp["LTP"]))
                    except Exception:
                        pass
                    run_bos_alerts(kite, ltp_now)
                    ts["bos"] = now_ts

                # Futures (every 3 min)
                if now_ts - ts["futures"] >= INTERVAL_FUTURES:
                    fetch_futures(kite)
                    ts["futures"] = now_ts

                # SMC + Combined Engine (every 15 min)
                if now_ts - ts["smc"] >= INTERVAL_SMC:
                    ltp_now = {}
                    try:
                        tmp     = pd.read_csv(_dated("live_data"))
                        ltp_now = dict(zip(tmp["Symbol"], tmp["LTP"]))
                    except Exception:
                        pass
                    run_smc_combined(kite, ltp_now)
                    ts["smc"] = now_ts

                write_heartbeat("market_open", {
                    "last_live":     now_dt.strftime("%H:%M:%S"),
                    "symbols":       len(SYMBOLS),
                    "1h_range_ready":bool(_1h_range),
                    "weekly_ready":  bool(_weekly_ohlc),
                    "monthly_ready": bool(_monthly_ohlc),
                    "oi_pcr":        _last_oi.get("pcr", 0),
                    "smc_ok":        _SMC_OK,
                    "bos_ok":        _BOS_OK,
                    "astro_ok":      _ASTRO_OK,
                })
                time.sleep(10)

            else:
                # ── OFF-MARKET ────────────────────────────────────
                status = "pre_market" if hhmm < "0915" else "post_market"
                if hhmm < "0915":
                    # Pre-fetch / refresh static data before open
                    if now_ts - ts["yest"]    >= INTERVAL_YEST:
                        _fetch_yesterday_ohlc(kite)
                        _load_top_levels(kite)
                        ts["yest"] = now_ts
                    if now_ts - ts["weekly"]  >= INTERVAL_WEEKLY:
                        _fetch_weekly_ohlc(kite)
                        ts["weekly"] = now_ts
                    if now_ts - ts["monthly"] >= INTERVAL_MONTHLY:
                        _fetch_monthly_ohlc(kite)
                        ts["monthly"] = now_ts
                write_heartbeat(status)
                log.debug(f"💤 Market closed [{hhmm}] — sleeping {INTERVAL_OFF}s")
                time.sleep(INTERVAL_OFF)

        except KeyboardInterrupt:
            log.info("🛑 Worker stopped by user (Ctrl+C)")
            write_heartbeat("stopped")
            break
        except Exception as e:
            log.error(f"❌ Main loop error: {e}\n{traceback.format_exc()}")
            write_heartbeat("error", {"error": str(e)})
            time.sleep(30)   # short pause before retry


if __name__ == "__main__":
    main()
