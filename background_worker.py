# =============================================================
# background_worker.py  —  Panchak Dashboard Background Engine
# Version 4.4 — Synced with dashboard v4 (trigger-aware periods, unified logs)
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

# ── Windows UTF-8 console fix (emoji/₹ in print crashes on Windows cp1252) ──
try:
    import sys as _sys_enc
    if hasattr(_sys_enc.stdout, 'reconfigure'):
        _sys_enc.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(_sys_enc.stderr, 'reconfigure'):
        _sys_enc.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass
# ─────────────────────────────────────────────────────────────────────────────


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
TG_CHAT_ID   = "-1002360390807"    # ch1 = AutoBotTest123 (primary)

# ── Second Telegram channel — Panchak Alerts ──────────────────────────────────
# ch1 = AutoBotTest123  Chat ID: -1002360390807  (primary)
# ch2 = Panchak Alerts  Chat ID: -1003706739531  (secondary — "both" routes go here too)
TG_CHAT_ID_2  = "-1003706739531"   # ch2 = Panchak Alerts
# NOTE: uses same TG_BOT_TOKEN (main bot) — must be admin in both channels
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
    date(2026, 1, 26), date(2026, 3, 6), date(2026, 3, 20), date(2026, 3, 31),
    date(2026, 4, 1),  date(2026, 4, 2), date(2026, 5, 1),  date(2026, 10, 2),
    date(2026, 10, 21), date(2026, 11, 5), date(2026, 11, 6), date(2026, 12, 25),
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
    Return (PANCHAK_START, PANCHAK_END) based on dashboard's 'trigger' logic.
    Period N data is used from the day AFTER Period N ends until the day Period N+1 ends.
    """
    today = date.today()
    sched = PANCHAK_SCHEDULE_2026
    active_idx = 0  # Default to first period
    for i in range(1, len(sched)):
        # Period i data becomes the 'active' source on the morning of (Period i end + 1 day)
        trigger = sched[i][1] + timedelta(days=1)
        if today >= trigger:
            active_idx = i
    return sched[active_idx]

PANCHAK_START, PANCHAK_END = _get_active_panchak_worker()

# Index symbols — these require SYMBOL_META mapping for Kite
SYMBOL_META = {
    "NIFTY":       "NIFTY 50",
    "BANKNIFTY":   "NIFTY BANK",
    "SENSEX":      "SENSEX",     # BSE:SENSEX
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
    "tg_HEATMAP_REVERSAL":True,
    "tg_SMC_PANCHAK":True,
    # ── New alert categories (Points 9-13) ───────────────────────────
    "tg_MACD_BULL":True,        "tg_MACD_BEAR":True,
    "tg_INSIDE_BAR":True,
    "tg_CHART_PATTERN":True,
    "tg_EXPIRY_ALERT":True,
    "tg_HOLIDAY_ALERT":True,
    "tg_PANCHAK_RANGE":True,
    # ── Second Telegram channel — BOS/CHoCH/SMC (Point 12) ──────────
    "tg2_BOS_UP":True,          "tg2_BOS_DOWN":True,
    "tg2_CHOCH_UP":True,        "tg2_CHOCH_DOWN":True,
    "tg2_SMC_OI_CONFLUENCE":True,
    # ── New toggle keys (synced with dashboard v3.4) ─────────────────
    "tg_PUT_FLOOR":False,
    "tg_MTF_SEQ":True,          # PI-IND Style Scanner (3-Pillar)
                                # NOTE: MTF_SEQ alerts only fire from dashboard (needs live 5-min candles)
                                # Worker does not replicate this — toggle controls dashboard only
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
    Only reloads if file mtime has changed (cheap inode check).
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
                    # Accept new keys that postdate _TOGGLE_DEFAULTS (e.g. newly added routes)
                    merged[k] = v
                elif isinstance(default, bool) and isinstance(v, bool):
                    merged[k] = v
                elif isinstance(default, str) and isinstance(v, str) and v in ("ch1","ch2","both"):
                    merged[k] = v
                # else: silently keep default (protects against corrupt file)
            _toggles       = merged
            _toggles_mtime = mt
            log.info(f"🔄 Toggles reloaded: {len(merged)} keys, {mt:.0f}")
    except Exception as e:
        log.warning(f"Toggle reload error: {e}")

def tg_on(cat):    return _toggles.get(f"tg_{cat}",    True)
def email_on(cat): return _toggles.get(f"email_{cat}", False)

def _route(cat: str) -> str:
    """
    Return channel routing for a category: 'ch1' | 'ch2' | 'both'.
    Falls back to 'ch1' if no route_ key set.
    """
    return _toggles.get(f"route_{cat}", _TOGGLE_DEFAULTS.get(f"route_{cat}", "ch1"))

def _send_routed(cat: str, msg: str, key: str = None):
    """
    Send Telegram alert respecting channel routing from alert_toggles.json.
    ch1  → send_tg_bg only
    ch2  → send_tg2_bg only
    both → send_tg_bg + send_tg2_bg
    Dedup key prefixed with CH2_ for ch2 to avoid cross-channel dedup collision.
    """
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
    path = os.path.join(CACHE_DIR, f"tg_dedup_{_today_str()}.json")
    try:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                _tg_dedup = json.load(f)
    except Exception:
        _tg_dedup = {}

def _save_key(key):
    with _TG_LOCK:
        _tg_dedup[key] = _now().isoformat()
    try:
        path = os.path.join(CACHE_DIR, f"tg_dedup_{_today_str()}.json")
        with _TG_LOCK:
            with open(path, "w", encoding='utf-8') as f:
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
# ALERT LOG (appended CSV — unified with dashboard v4 schema)
# ══════════════════════════════════════════════════════════════════
ALERT_LOG_FILE = os.path.join(CACHE_DIR, "alerts_log.csv")

def _log_alert(category: str, symbols: list, title: str = "", ltp_map: dict = None, channel: str = "TG"):
    """Log per-symbol rows to match dashboard ALERTS_LOG_FILE schema."""
    try:
        now = _now()
        _ltp_map = ltp_map if ltp_map is not None else {}
        _title = str(title if title else category).replace(",", " ")
        exists = os.path.exists(ALERT_LOG_FILE)
        with open(ALERT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["DATE", "TIME", "SYMBOL", "CATEGORY", "DETAILS", "LTP", "SOURCE"])
            for sym in symbols:
                # Ensure scalar LTP and clean Symbol/Title
                ltp_val = _ltp_map.get(sym, "")
                if isinstance(ltp_val, dict): ltp_val = ltp_val.get("LTP", "")
                
                w.writerow([
                    now.strftime("%Y-%m-%d"),
                    now.strftime("%H:%M:%S"),
                    str(sym).replace(",", " ")[:20],
                    category,
                    _title,
                    ltp_val,
                    f"WORKER_{channel}"
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
kite      = None   # set by main() — module-level so helper fns can access

def _load_instruments(kite):
    global _inst_df, _bse_inst_df
    try:
        _inst_df = pd.DataFrame(kite.instruments("NSE"))
        log.info(f"📋 NSE Instruments loaded: {len(_inst_df)} records")
    except Exception as e:
        log.error(f"NSE Instruments load failed: {e}")
    try:
        _bse_inst_df = pd.DataFrame(kite.instruments("BSE"))
        log.info(f"📋 BSE Instruments loaded: {len(_bse_inst_df)} records (for SENSEX)")
    except Exception as e:
        log.warning(f"BSE Instruments load failed (SENSEX token unavailable): {e}")

def kite_sym(symbol: str) -> str:
    """Return Kite exchange:tradingsymbol. SENSEX trades on BSE."""
    if symbol == "SENSEX":
        return "BSE:SENSEX"
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

_bse_inst_df = None   # BSE instruments — loaded once for SENSEX

def get_token(symbol: str):
    """
    Get Kite instrument token.
    SENSEX (BSE index) uses _bse_inst_df loaded from BSE instruments.
    All other symbols use _inst_df loaded from NSE instruments.
    Returns None if token not found (caller must handle gracefully).
    """
    if symbol == "SENSEX":
        if _bse_inst_df is None:
            return None   # BSE instruments not loaded yet
        _row = _bse_inst_df[_bse_inst_df.tradingsymbol == "SENSEX"]
        return None if _row.empty else int(_row.iloc[0].instrument_token)
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
            with open(path, encoding="utf-8") as f:
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
        with open(path, "w", encoding='utf-8') as f:
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
            with open(path, encoding="utf-8") as f:
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
        with open(path, "w", encoding='utf-8') as f:
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
        _log_alert(category, symbols, title, ltp_map, "TG")
        log.info(f"📤 [{category}] {len(symbols)} stocks → route:{_route(category)}: {','.join(symbols[:5])}")

    if email_on(category):
        send_email_bg(
            f"[OiAnalytics] {title} — {len(symbols)} stocks | {_now_str()}",
            _sh(msg)
        )
        _log_alert(category, symbols, title, ltp_map, "EMAIL")


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
    _send_routed(category, msg, key)
    _log_alert(category, symbols, title, ltp_map, "TG")
    log.info(f"📤 STRUCT [{category}] → route:{_route(category)}: {','.join(symbols[:5])}")


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
        _log_alert(f"SPECIAL_{direction}", syms, f"SPECIAL {title} BREAK", ltp_map, "ALL3")
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
            _send_routed("MACD_BULL",
                f"📈📈📈📈📈📈📈📈📈📈\n"
                f"📈 <b>MACD BULLISH CROSSOVER — Daily</b>\n"
                f"⏰ {now_s}   📊 12/26/9 Standard MACD\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stocks ({len(bull_syms)}):\n{lines}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ MACD line crossed ABOVE Signal line (daily)\n"
                f"⚠️ <i>NOT financial advice.</i>\n"
                f"📈📈📈📈📈📈📈📈📈📈", k)
            _log_alert("MACD_BULL", bull_syms, "MACD BULLISH CROSSOVER", ltp_map, "TG")
            log.info(f"📈 MACD Bull → route:{_route('MACD_BULL')}: {bull_syms[:5]}")

    if bear_syms and tg_on("MACD_BEAR"):
        k = f"MACD_BEAR_{today_s}"
        if not already_sent(k):
            lines = "\n".join(
                f"  🔴 <b>{s}</b>  LTP: {ltp_map.get(s,'?')}  "
                f"MACD: {_macd_cache[s]['macd']:.3f}  Hist: {_macd_cache[s]['hist']:.3f}"
                for s in bear_syms[:15]
            )
            _send_routed("MACD_BEAR",
                f"📉📉📉📉📉📉📉📉📉📉\n"
                f"📉 <b>MACD BEARISH CROSSOVER — Daily</b>\n"
                f"⏰ {now_s}   📊 12/26/9 Standard MACD\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stocks ({len(bear_syms)}):\n{lines}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ MACD line crossed BELOW Signal line (daily)\n"
                f"⚠️ <i>NOT financial advice.</i>\n"
                f"📉📉📉📉📉📉📉📉📉📉", k)
            _log_alert("MACD_BEAR", bear_syms, "MACD BEARISH CROSSOVER", ltp_map, "TG")
            log.info(f"📉 MACD Bear → route:{_route('MACD_BEAR')}: {bear_syms[:5]}")


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
    """
    Alert once per stock per direction per day when LTP breaks inside bar.
    Two-stage alert system:
      ① PROXIMITY PRE-ALERT: fires when LTP is within 0.3% of level (advance warning)
      ② BREAKOUT ALERT: fires the moment LTP crosses the level (entry signal)
    Both use IB_UP_/IB_DN_ dedup prefix shared with dashboard tab22.
    Worker runs every 60s from live ltp_map — fires as soon as price crosses.
    """
    if not tg_on("INSIDE_BAR") or not _inside_bar_daily:
        return
    now_s    = _now_str()
    today_s  = _now().strftime("%Y%m%d")
    slot_10m = _slot_10m()
    PROX_PCT = 0.3   # proximity threshold in %

    for sym, data in _inside_bar_daily.items():
        ltp  = float(ltp_map.get(sym, 0) or 0)
        ph   = data["parent_high"]
        pl   = data["parent_low"]
        comp = data.get("compression", 0)
        ch   = data.get("child_high",  ph)
        cl   = data.get("child_low",   pl)
        if ltp <= 0 or ph <= 0 or pl <= 0:
            continue

        # ── ① BREAKOUT / BREAKDOWN (level crossed) ───────────────────────
        for direction, broke, icon, verdict, entry_str, sl_str, dedup_pre, level, away_pts in [
            ("UP",   ltp > ph, "🟢", "BREAKOUT ↑",  f"Long > ₹{ph:,.2f}", f"Below ₹{pl:,.2f}", "IB_UP",  ph, round(ltp - ph, 2)),
            ("DOWN", ltp < pl, "🔴", "BREAKDOWN ↓", f"Short < ₹{pl:,.2f}", f"Above ₹{ph:,.2f}", "IB_DN", pl, round(pl - ltp, 2)),
        ]:
            if not broke:
                continue
            k = f"{dedup_pre}_{sym}_{today_s}"
            if already_sent(k):
                continue
            msg = (
                f"🕯️ <b>INSIDE BAR {verdict} — {sym}</b>\n"
                f"⏰ {now_s}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 LTP: <b>₹{ltp:,.2f}</b>  ({'+' if direction=='UP' else '-'}{away_pts:.2f} pts {'above' if direction=='UP' else 'below'} level)\n"
                f"📊 Yest (Child): H={ch:,.2f}  L={cl:,.2f}\n"
                f"📐 Parent: H={ph:,.2f}  L={pl:,.2f}\n"
                f"📏 Compression: {comp:.0f}%\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 Entry: {entry_str}\n"
                f"🛡️ SL: {sl_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ <i>NOT financial advice.</i>"
            )
            _send_routed("INSIDE_BAR", msg, k)
            _log_alert(f"INSIDE_BAR_{direction}", [sym], f"INSIDE BAR {verdict}", ltp_map, "TG")
            log.info(f"🕯️ Inside Bar {direction} [{sym}] LTP={ltp} level={level} +{away_pts}pts")

        # ── ② PROXIMITY PRE-ALERT (within 0.3% of level, not yet crossed) ──
        # Only for High Conviction setups (compression ≤ 70%)
        if ltp <= ph and ltp >= pl and comp <= 70:
            dist_up = (ph - ltp) / ph * 100
            dist_dn = (ltp - pl) / pl * 100

            if dist_up <= PROX_PCT:
                prox_k = f"IB_PROX_UP_{sym}_{today_s}_{slot_10m}"
                if not already_sent(prox_k):
                    _pts_away = round(ph - ltp, 2)
                    prox_msg = (
                        f"⏰ <b>IB PRE-ALERT ↑ — {sym}</b>\n"
                        f"🔥 Approaching Parent High — just {_pts_away:.2f} pts away\n"
                        f"Parent High: <b>₹{ph:,.2f}</b>  |  LTP: <b>₹{ltp:,.2f}</b>\n"
                        f"📐 Compression: {comp:.0f}%  |  SL: ₹{pl:,.2f}\n"
                        f"⚠️ <i>NOT financial advice.</i>"
                    )
                    _send_routed("INSIDE_BAR", prox_msg, prox_k)
                    log.info(f"⏰ IB Pre-Alert UP [{sym}] {_pts_away:.2f}pts from ₹{ph}")

            elif dist_dn <= PROX_PCT:
                prox_k = f"IB_PROX_DN_{sym}_{today_s}_{slot_10m}"
                if not already_sent(prox_k):
                    _pts_away = round(ltp - pl, 2)
                    prox_msg = (
                        f"⏰ <b>IB PRE-ALERT ↓ — {sym}</b>\n"
                        f"🔥 Approaching Parent Low — just {_pts_away:.2f} pts away\n"
                        f"Parent Low: <b>₹{pl:,.2f}</b>  |  LTP: <b>₹{ltp:,.2f}</b>\n"
                        f"📐 Compression: {comp:.0f}%  |  SL: ₹{ph:,.2f}\n"
                        f"⚠️ <i>NOT financial advice.</i>"
                    )
                    _send_routed("INSIDE_BAR", prox_msg, prox_k)
                    log.info(f"⏰ IB Pre-Alert DN [{sym}] {_pts_away:.2f}pts from ₹{pl}")


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
        with open(_PANCHAK_RANGE_FILE, encoding="utf-8") as f:
            _panchak_range_state = json.load(f)
    except Exception:
        _panchak_range_state = {}

def _save_panchak_range():
    try:
        with open(_PANCHAK_RANGE_FILE, "w", encoding='utf-8') as f:
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
            _send_routed("PANCHAK_RANGE",
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
            _send_routed("PANCHAK_RANGE", _panchak_msg(
                "HIGH BREAK — BULLISH", ph, up_t61, up_t138, up_t200,
                pl, "UP", round(nifty_ltp - ph, 2)), k)
            s["breakout_up_sent"] = True
            _save_panchak_range()

    # Downside breakout (price below Panchak Low)
    if nifty_ltp < pl and not s["breakout_dn_sent"]:
        k = f"PR_BREAK_DN_{pkey}_{today_s}"
        if not already_sent(k):
            _send_routed("PANCHAK_RANGE", _panchak_msg(
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
                _send_routed("PANCHAK_RANGE",
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
                _send_routed("PANCHAK_RANGE",
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
# ══════════════════════════════════════════════════════════════════════════
# CHART PATTERN AUTO-SCAN — Worker side
# Runs once per hour during market hours.
# Same dedup as dashboard — whichever fires first marks the key.
# ══════════════════════════════════════════════════════════════════════════

_CPS_WK_LAST_HOUR  = -1    # last hour when worker scanned
_CPS_WK_DEDUP_DIR  = CACHE_DIR
_CPS_WK_MIN_SCORE  = 6
_CPS_WK_MIN_RR     = 1.5


def _cps_wk_dedup_key(symbol: str) -> str:
    """ONE alert per stock per day — same key format as dashboard."""
    return f"CPS_{symbol}_{date.today().isoformat()}"


def _cps_wk_already_sent(symbol: str) -> bool:
    """Check if this stock was already alerted today (any pattern, by dashboard or worker)."""
    key_file = os.path.join(_CPS_WK_DEDUP_DIR, f"cps_dedup_{date.today().isoformat()}.json")
    try:
        if os.path.exists(key_file):
            with open(key_file, "r", encoding="utf-8") as _f:
                d = json.load(_f)
        else:
            return False
        return _cps_wk_dedup_key(symbol) in d
    except Exception:
        pass
    return False


def _cps_wk_mark_sent(symbol: str, pattern: str):
    """Mark stock as alerted today — stores which pattern fired."""
    key_file = os.path.join(_CPS_WK_DEDUP_DIR, f"cps_dedup_{date.today().isoformat()}.json")
    try:
        d = {}
        if os.path.exists(key_file):
            with open(key_file, "r", encoding="utf-8") as _f:
                d = json.load(_f)
        d[_cps_wk_dedup_key(symbol)] = {"pattern": pattern, "time": _now_str()}
        with open(key_file, "w", encoding="utf-8") as _f:
            json.dump(d, _f)
    except Exception:
        pass


def _cps_wk_is_fresh(r: dict) -> tuple:
    """
    Freshness + validity filter — identical logic to dashboard _cps_is_fresh.
    BULL:  0 < (ltp - resist) / resist * 100 ≤ 5%  and  change_% ≥ -1.5%
    BEAR:  0 < (support - ltp) / support * 100 ≤ 5%  and  change_% ≤ +1.5%
    """
    direction = r.get("direction", "")
    ltp       = float(r.get("ltp", 0) or 0)
    resist    = float(r.get("resist_level", 0) or 0)
    support   = float(r.get("support_level", 0) or 0)
    chg_pct   = float(r.get("change_%", 0) or 0)

    if ltp <= 0:
        return False, "LTP is zero"

    if direction == "BULL":
        if resist <= 0:
            return False, "No resist level"
        overshoot = (ltp - resist) / resist * 100
        if overshoot <= 0:
            return False, "Not broken out"
        if overshoot > 5.0:
            return False, f"Stale +{overshoot:.1f}%"
        if chg_pct < -1.5:
            return False, f"Reversing {chg_pct:+.1f}%"
        return True, f"Fresh BULL +{overshoot:.1f}%"
    else:
        if support <= 0:
            return False, "No support level"
        overshoot = (support - ltp) / support * 100
        if overshoot <= 0:
            return False, "Not broken down"
        if overshoot > 5.0:
            return False, f"Stale -{overshoot:.1f}%"
        if chg_pct > 1.5:
            return False, f"Bouncing {chg_pct:+.1f}%"
        return True, f"Fresh BEAR -{overshoot:.1f}%"


def _cps_wk_best_per_stock(results: list) -> list:
    """Keep only the highest-scoring pattern per stock — one alert per stock."""
    by_sym = {}
    for r in results:
        sym   = r.get("symbol", "")
        score = r.get("score", 0)
        rr    = r.get("rr", 0)
        if sym not in by_sym:
            by_sym[sym] = r
        else:
            prev = by_sym[sym]
            if score > prev.get("score", 0) or (
                score == prev.get("score", 0) and rr > prev.get("rr", 0)
            ):
                by_sym[sym] = r
    return list(by_sym.values())


def _build_cps_tg_msg_wk(r: dict) -> str:
    """Worker-side TG message builder for chart pattern alerts."""
    sym       = r.get("symbol", "")
    pattern   = r.get("pattern", "")
    direction = r.get("direction", "")
    score     = r.get("score", 0)
    ltp       = r.get("ltp", 0)
    entry     = r.get("entry", 0)
    sl        = r.get("sl", 0)
    t1        = r.get("t1", 0)
    t2        = r.get("t2", 0)
    rr        = r.get("rr", 0)
    vol_ratio = r.get("vol_ratio", 0)
    pole_pct  = r.get("pole_pct", 0)
    post_pct  = r.get("post_break_%", 0)
    bars      = r.get("bars", 0)
    resist    = r.get("resist_level", 0)
    chg_pct   = r.get("change_%", 0)
    yh        = r.get("yest_high", 0)
    yl        = r.get("yest_low", 0)
    yc        = r.get("yest_close", 0)

    is_bull   = direction == "BULL"
    dir_icon  = "🟢" if is_bull else "🔴"
    dir_label = "BULLISH BREAKOUT ↑" if is_bull else "BEARISH BREAKDOWN ↓"
    vol_ic    = "🔥" if vol_ratio >= 2.0 else ("⚡" if vol_ratio >= 1.5 else "📊")
    hc_label  = "🔥🔥 HIGH CONVICTION" if score >= 8 else ("⚡ STRONG" if score >= 6 else "📊 MODERATE")
    post_line = f"📈 Post-break: <b>+{post_pct:.1f}%</b> confirmed\n" if post_pct > 1 else ""

    ctx_lines = {
        "Falling Wedge":       "Converging downtrend lines → explosive upside breakout",
        "Ascending Triangle":  "Flat resistance + rising lows → breakout above flat top",
        "Bull Flag":           f"Pole +{pole_pct}% then consolidation → measured move",
        "Bull Pennant":        f"Pole +{pole_pct}% then converging pennant → continuation",
        "Cup & Handle":        "U-shaped recovery + handle → classic bullish continuation",
        "Double Bottom":       "Two equal lows (W-pattern) → neckline break confirms reversal",
        "Rising Wedge":        "Converging uptrend lines → breakdown expected",
        "Descending Triangle": "Flat support + falling highs → breakdown below support",
        "Bear Flag":           f"Pole -{abs(pole_pct)}% then consolidation → continuation lower",
        "Bear Pennant":        f"Pole -{abs(pole_pct)}% then converging pennant → lower",
        "Head & Shoulders":    "Left shoulder + head + right shoulder → neckline break",
        "Double Top":          "Two equal highs (M-pattern) → neckline break confirms reversal",
    }
    ctx = ctx_lines.get(pattern, "")

    return (
        f"{dir_icon} <b>📐 CHART PATTERN — {sym}</b>\n"
        f"⏰ {_now_str()}  |  📅 Daily TF\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{hc_label}  [Score {score}/10]\n"
        f"<b>{pattern}</b> — {dir_label}\n"
        f"<i>{ctx}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 LTP: <b>₹{ltp:,.2f}</b>  ({chg_pct:+.2f}% today)\n"
        f"📊 Yest: H:{yh:,.0f}  L:{yl:,.0f}  C:{yc:,.0f}\n"
        f"🔓 Key level: <b>₹{resist:,.2f}</b>\n"
        f"{vol_ic} Volume: <b>{vol_ratio:.1f}x</b>  |  Pattern: <b>{bars} bars</b>\n"
        f"{post_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Entry <b>₹{entry:,.2f}</b>  |  SL <b>₹{sl:,.2f}</b>\n"
        f"   T1 <b>₹{t1:,.2f}</b>  |  T2 <b>₹{t2:,.2f}</b>  |  R:R <b>{rr}:1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Daily candle pattern. NOT financial advice.</i>"
    )


def _check_chart_pattern_scan():
    """
    Run chart pattern scan once per hour during market hours.
    Fires TG alerts for new patterns. Non-blocking (runs inline but rate-limited).
    """
    global _CPS_WK_LAST_HOUR
    if kite is None:
        return   # not yet initialized — skip
    if not tg_on("CHART_PATTERN"):
        return
    if not is_market_hours():
        return

    now_hour = _now().hour
    if now_hour == _CPS_WK_LAST_HOUR:
        return   # already ran this hour

    _CPS_WK_LAST_HOUR = now_hour
    log.info(f"📐 Chart pattern scan starting (hour {now_hour:02d}:xx)...")

    try:
        import sys as _sys
        _cps_path = os.path.dirname(os.path.abspath(__file__))
        if _cps_path not in _sys.path:
            _sys.path.insert(0, _cps_path)
        import chart_pattern_scanner as _cps_mod
    except ImportError as _ie:
        log.warning(f"📐 chart_pattern_scanner.py not found: {_ie}")
        return

    # Build a minimal live_df from ltp_map (worker doesn't have full live_df)
    # Pattern scanner uses live_df mainly for LTP — pass None if unavailable
    import pandas as _pd
    try:
        _ltp_rows = [{"Symbol": s, "LTP": v, "LIVE_OPEN": 0, "CHANGE_%": 0, "VOL_%": 0}
                     for s, v in ltp_map.items() if v > 0]
        _live_df_wk = _pd.DataFrame(_ltp_rows) if _ltp_rows else None
    except Exception:
        _live_df_wk = None

    try:
        results = _cps_mod.scan_chart_patterns(
            kite         = kite,
            symbols      = STOCKS,   # STOCKS only — indices have no chart patterns
            get_token_fn = get_token,
            live_df      = _live_df_wk,
            min_score    = _CPS_WK_MIN_SCORE,
            min_rr       = _CPS_WK_MIN_RR,
        )
    except Exception as _scan_err:
        log.error(f"📐 Pattern scan error: {_scan_err}")
        return

    import time as _tt

    # Step 1: Freshness filter
    fresh_results = []
    for r in results:
        is_valid, reason = _cps_wk_is_fresh(r)
        if is_valid:
            fresh_results.append(r)

    # Step 2: Best pattern per stock
    best_results = _cps_wk_best_per_stock(fresh_results)

    # Step 3: Alert — one per stock per day
    new_count  = 0
    skip_count = 0
    for r in best_results:
        sym     = r.get("symbol", "")
        pattern = r.get("pattern", "")
        if not sym or not pattern:
            continue
        if _cps_wk_already_sent(sym):      # stock-level dedup
            skip_count += 1
            continue
        msg     = _build_cps_tg_msg_wk(r)
        dedup_k = _cps_wk_dedup_key(sym)   # symbol-level key
        _send_routed("CHART_PATTERN", msg, dedup_k)
        _cps_wk_mark_sent(sym, pattern)
        new_count += 1
        _tt.sleep(0.3)

    log.info(
        f"📐 Pattern scan done: {len(results)} total, "
        f"{len(fresh_results)} fresh, {len(best_results)} best-per-stock, "
        f"{skip_count} already sent, {new_count} new alerts"
    )


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
            _send_routed("EXPIRY_ALERT",
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
            _send_routed("EXPIRY_ALERT",
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
            _send_routed("EXPIRY_ALERT",
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
            _send_routed("EXPIRY_ALERT",
                f"🚨 <b>SENSEX EXPIRY DAY</b> 🚨\n"
                f"⏰ {now_s}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Today is Sensex Weekly Expiry: <b>{sensex_exp_str}</b>\n"
                f"{'⚠️ Shifted from Thu (Holiday)' if sensex_exp.weekday()==2 else ''}\n"
                f"📌 Close or hedge Sensex positions by 15:20!\n"
                f"⚠️ <i>NOT financial advice.</i>", k)


# ══════════════════════════════════════════════════════════════════
# HEATMAP REVERSAL DETECTION  (Early Detection)
# ══════════════════════════════════════════════════════════════════
_N50_HEAVY_SYMS = ["RELIANCE", "HDFCBANK", "ICICIBANK", "BHARTIARTL", "SBIN", "TCS", "INFY", "ITC", "AXISBANK", "LT"]
_BNK_HEAVY_SYMS = ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK"]
_SNX_HEAVY_SYMS = ["HDFCBANK", "ICICIBANK", "RELIANCE", "INFY", "BHARTIARTL", "ITC", "LT", "TCS", "AXISBANK", "KOTAKBANK"]

N50_WTS = {
    "RELIANCE":9.30,"HDFCBANK":6.37,"BHARTIARTL":5.74,"SBIN":5.04,
    "ICICIBANK":4.93,"TCS":4.77,"LT":2.90,"BAJFINANCE":2.89,
    "INFY":2.74,"HINDUNILVR":2.57,"M&M":2.58,"ITC":2.71,
    "AXISBANK":3.26,"TATAMOTORS":1.62,"MARUTI":1.54,"NTPC":1.86,
    "POWERGRID":1.58,"HCLTECH":1.58,"SUNPHARMA":1.61,"KOTAKBANK":1.82,
    "ONGC":1.22,"COALINDIA":1.14,"TATASTEEL":1.18,"JSWSTEEL":1.04,
    "ULTRACEMCO":1.02,"WIPRO":1.05,"TITAN":1.51,"BAJAJFINSV":1.34,
    "BAJAJ-AUTO":1.10,"NESTLEIND":1.08,"HINDALCO":0.92,"GRASIM":0.84,
    "ADANIENT":0.81,"ASIANPAINT":0.79,"ADANIPORTS":0.84,"TATACONSUM":0.82,
    "APOLLOHOSP":0.68,"BEL":0.72,"CIPLA":0.75,"DRREDDY":0.71,
    "TRENT":0.74,"EICHERMOT":0.61,"HEROMOTOCO":0.58,"BPCL":0.65,
    "BRITANNIA":0.64,"SHRIRAMFIN":1.01,"HDFCLIFE":0.88,"SBILIFE":0.82,
    "LTIM":0.81,"DIVISLAB":0.56,
}
BNK_WTS = {
    "HDFCBANK":25.56,"SBIN":20.28,"ICICIBANK":19.79,"AXISBANK":8.64,
    "KOTAKBANK":7.80,"UNIONBANK":2.95,"BANKBARODA":2.95,"PNB":2.66,
    "CANBK":2.64,"AUBANK":1.51,"FEDERALBNK":1.45,
    "INDUSINDBK":1.34,"YESBANK":1.25,"IDFCFIRSTB":1.18,
}
SNX_WTS = {
    "HDFCBANK":15.66,"ICICIBANK":10.88,"RELIANCE":10.24,"INFY":5.75,
    "BHARTIARTL":5.37,"ITC":4.23,"LT":4.20,"TCS":3.74,
    "AXISBANK":3.63,"KOTAKBANK":3.49,"M&M":2.84,"HINDUNILVR":2.82,
    "TATASTEEL":2.58,"SBIN":2.34,"NTPC":2.16,"POWERGRID":1.84,
    "SUNPHARMA":1.76,"TITAN":1.68,"BAJFINANCE":1.65,"JSWSTEEL":1.48,
    "ULTRACEMCO":1.42,"ADANIENT":1.38,"MARUTI":1.32,"NESTLEIND":1.28,
    "HCLTECH":1.24,"ASIANPAINT":1.18,"BAJAJFINSV":1.14,"TATAMOTORS":1.12,
    "TECHM":0.98,"INDUSINDBK":0.91
}

_heatmap_history = []

def _check_heatmap_reversal(df_live):
    """
    Evaluates reversal criteria.
    Tracks history of Heatmap Bull % and monitors Core Heavyweights.
    """
    if not is_market_hours() or not tg_on("HEATMAP_REVERSAL"):
        return
    if df_live is None or df_live.empty:
        return

    try:
        live_lookup = {row.Symbol: row for _, row in df_live.iterrows()}
        
        # ── 1. Nifty 50 Bull % ──
        bull_wt = bear_wt = 0.0
        for sym, wt in N50_WTS.items():
            if sym in live_lookup:
                chg = float(live_lookup[sym].get("CHANGE_%", 0) or 0)
                if chg > 0.25:   bull_wt += wt
                elif chg < -0.25: bear_wt += wt
        curr_bull_pct = bull_wt / (bull_wt + bear_wt + 0.01) * 100
        
        # ── 1b. Bank Nifty Bull % ──
        bnk_bull_wt = bnk_bear_wt = 0.0
        for sym, wt in BNK_WTS.items():
            if sym in live_lookup:
                chg = float(live_lookup[sym].get("CHANGE_%", 0) or 0)
                if chg > 0.25:   bnk_bull_wt += wt
                elif chg < -0.25: bnk_bear_wt += wt
        curr_bnk_bull_pct = bnk_bull_wt / (bnk_bull_wt + bnk_bear_wt + 0.01) * 100

        # ── 1c. Sensex Bull % ──
        snx_bull_wt = snx_bear_wt = 0.0
        for sym, wt in SNX_WTS.items():
            if sym in live_lookup:
                chg = float(live_lookup[sym].get("CHANGE_%", 0) or 0)
                if chg > 0.25:   snx_bull_wt += wt
                elif chg < -0.25: snx_bear_wt += wt
        curr_snx_bull_pct = snx_bull_wt / (snx_bull_wt + snx_bear_wt + 0.01) * 100

        # Store in rolling history
        global _heatmap_history
        _heatmap_history.append({"n50": curr_bull_pct, "bnk": curr_bnk_bull_pct, "snx": curr_snx_bull_pct})
        if len(_heatmap_history) > 30: _heatmap_history = _heatmap_history[-30:]
        
        if len(_heatmap_history) < 5: return 
        
        max_n50 = max(h["n50"] for h in _heatmap_history[:-1])
        min_n50 = min(h["n50"] for h in _heatmap_history[:-1])
        max_snx = max(h["snx"] for h in _heatmap_history[:-1])
        min_snx = min(h["snx"] for h in _heatmap_history[:-1])
        
    except Exception as e:
        log.warning(f"Heatmap calc error: {e}")
        return

    def _validate_heavyweights(symbols, mode="BEAR"):
        total = len(symbols)
        match_count = 0
        details = []
        for sym in symbols:
            if sym not in live_lookup: continue
            row = live_lookup[sym]
            ltp    = float(row.get("LTP", 0))
            open_p = float(row.get("LIVE_OPEN", 0))
            chg    = float(row.get("CHANGE", 0))
            pc     = ltp - chg
            high   = float(row.get("LIVE_HIGH", 0))
            low    = float(row.get("LIVE_LOW", 0))
            yh     = float(row.get("YEST_HIGH", 0))
            yl     = float(row.get("YEST_LOW", 0))
            
            if mode == "BEAR":
                if ltp < open_p and ltp < pc and ltp < high and (yh == 0 or ltp < yh):
                    match_count += 1
                    details.append(f"{sym} (LTP {ltp} < Open {open_p})")
            else: # BULL
                if ltp > open_p and ltp > pc and ltp > low and (yl == 0 or ltp > yl):
                    match_count += 1
                    details.append(f"{sym} (LTP {ltp} > Open {open_p})")
        return match_count >= (total // 2), details

    now_s = _now_str()
    today_k = _today_str()

    # BULL -> BEAR
    if (max_n50 >= 65 and curr_bull_pct <= 50) or (max_snx >= 65 and curr_snx_bull_pct <= 50):
        index_name = "Nifty" if (max_n50 >= 65 and curr_bull_pct <= 50) else "Sensex"
        max_val = max_n50 if index_name == "Nifty" else max_snx
        curr_val = curr_bull_pct if index_name == "Nifty" else curr_snx_bull_pct
        heavy_syms = _N50_HEAVY_SYMS if index_name == "Nifty" else _SNX_HEAVY_SYMS
        
        is_confirmed, hv_details = _validate_heavyweights(heavy_syms, mode="BEAR")
        if is_confirmed:
            key = f"HEATMAP_REV_BEAR_{index_name}_{today_k}_{_now().hour:02d}"
            if not already_sent(key):
                hv_txt = "\n".join([f"  • {d}" for d in hv_details[:6]])
                msg = (
                    f"🔴🔴 <b>🔥 EARLY REVERSAL ALERT (BULL → BEAR)</b>\n"
                    f"⏰ {now_s} | {index_name} Heatmap Shift\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <b>Detected early reversal before a big fall!</b>\n"
                    f"📊 {index_name} Heatmap dropped from <b>{max_val:.0f}%</b> to <b>{curr_val:.0f}%</b>\n"
                    f"🏦 BNK Heatmap: <b>{curr_bnk_bull_pct:.0f}%</b> bull wt\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏗️ <b>Heavyweights confirming fall:</b>\n"
                    f"{hv_txt}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📍 <i>Heavyweights failing to break higher highs and dropping below Day Open & Yesterday Close.</i>\n"
                    f"⚠️ <i>NOT financial advice.</i>\n"
                    f"🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴"
                )
                _send_routed("HEATMAP_REVERSAL", msg, key)
                log.info(f"🔴 Heatmap Bear Rev [{index_name}]")

    # BEAR -> BULL
    if (min_n50 <= 35 and curr_bull_pct >= 50) or (min_snx <= 35 and curr_snx_bull_pct >= 50):
        index_name = "Nifty" if (min_n50 <= 35 and curr_bull_pct >= 50) else "Sensex"
        min_val = min_n50 if index_name == "Nifty" else min_snx
        curr_val = curr_bull_pct if index_name == "Nifty" else curr_snx_bull_pct
        heavy_syms = _N50_HEAVY_SYMS if index_name == "Nifty" else _SNX_HEAVY_SYMS

        is_confirmed, hv_details = _validate_heavyweights(heavy_syms, mode="BULL")
        if is_confirmed:
            key = f"HEATMAP_REV_BULL_{index_name}_{today_k}_{_now().hour:02d}"
            if not already_sent(key):
                hv_txt = "\n".join([f"  • {d}" for d in hv_details[:6]])
                msg = (
                    f"🟢🟢 <b>🔥 EARLY REVERSAL ALERT (BEAR → BULL)</b>\n"
                    f"⏰ {now_s} | {index_name} Heatmap Shift\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🚀 <b>Detected early reversal before a big rally!</b>\n"
                    f"📊 {index_name} Heatmap rose from <b>{min_val:.0f}%</b> to <b>{curr_val:.0f}%</b>\n"
                    f"🏦 BNK Heatmap: <b>{curr_bnk_bull_pct:.0f}%</b> bull wt\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏗️ <b>Heavyweights confirming rise:</b>\n"
                    f"{hv_txt}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📍 <i>Heavyweights failing to break lower lows and rising above Day Open & Yesterday Close.</i>\n"
                    f"⚠️ <i>NOT financial advice.</i>\n"
                    f"🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢"
                )
                _send_routed("HEATMAP_REVERSAL", msg, key)
                log.info(f"🟢 Heatmap Bull Rev [{index_name}]")


# ══════════════════════════════════════════════════════════════════
# PANCHAK SPECIAL RULES  (Fierce Reversal, Yoga & Planetary)
# ══════════════════════════════════════════════════════════════════
_panchak_tracking = {}   # pkey -> {"first_break": "UP"|"DN"|None, "alerted": bool}

def _check_panchak_special_rules(df_live):
    """Checks high-conviction panchak rules."""
    if not is_market_hours(): return
    if df_live is None or df_live.empty: return
    
    now_dt = _now()
    today_k = _today_str()
    
    # ── 1. Fierce Reversal Alert ──
    global _panchak_tracking
    for pkey, ps in _panchak_range_state.items():
        ph = ps.get("period_high", 0)
        pl = ps.get("period_low", 0)
        if ph == 0 or pl == 0: continue
        
        nifty_row = df_live[df_live["Symbol"] == "NIFTY"]
        if nifty_row.empty: continue
        ni_ltp = float(nifty_row.iloc[0]["LTP"])
        if ni_ltp == 0: continue
        
        state = _panchak_tracking.get(pkey, {"first_break": None, "alerted": False})
        
        # Detect first break
        if state["first_break"] is None:
            if ni_ltp > ph: state["first_break"] = "UP"
            elif ni_ltp < pl: state["first_break"] = "DN"
        
        # Detect fierce reversal
        elif not state["alerted"]:
            if state["first_break"] == "UP" and ni_ltp < pl:
                if not ps.get("t61_up", False):
                    msg = f"🚨 <b>PANCHAK FIERCE REVERSAL (FALL)</b>\nNifty broke High first, but now crashed below Low without hitting Target! 📉\nLTP: {ni_ltp:,.2f}"
                    _send_routed("KP_BREAK_15M", msg, f"PANCHAK_FIERCE_FALL_{pkey}_{today_k}")
                    state["alerted"] = True
            elif state["first_break"] == "DN" and ni_ltp > ph:
                if not ps.get("t61_dn", False):
                    msg = f"🚀 <b>PANCHAK FIERCE REVERSAL (RALLY)</b>\nNifty broke Low first, but now surged above High without hitting Target! 📈\nLTP: {ni_ltp:,.2f}"
                    _send_routed("KP_BREAK_15M", msg, f"PANCHAK_FIERCE_RALLY_{pkey}_{today_k}")
                    state["alerted"] = True
        
        _panchak_tracking[pkey] = state

    # ── 2. Yoga & Planetary Alerts (Once per day at 09:16) ──
    if now_dt.hour == 9 and now_dt.minute == 16:
        try:
            astro = _vedic_day_analysis(now_dt.date())
            yoga = astro.get("yoga_name", "")
            jr = astro.get("jupiter_rashi", "")
            sr = astro.get("saturn_rashi", "")
            
            if yoga in ["Indra", "Vaidhriti", "Vyatipata"]:
                msg = f"✨ <b>PANCHAK BULLISH YOGA</b>\nToday is a Panchak day with <b>{yoga} Yoga</b>. High conviction bullish bias! 🟢"
                _send_routed("KP_BREAK_15M", msg, f"PANCHAK_YOGA_{today_k}")
                
            if jr == sr and jr != "":
                msg = f"🪐 <b>PANCHAK PLANETARY SYNC</b>\nJupiter and Saturn are together in <b>{jr}</b>. Strong bullish cycle active! 🟢"
                _send_routed("KP_BREAK_15M", msg, f"PANCHAK_SYNC_{today_k}")
        except Exception as e:
            log.warning(f"Yoga alert error: {e}")


# ══════════════════════════════════════════════════════════════════
# VEDIC ASTRO ENGINE
# ══════════════════════════════════════════════════════════════════
import math

def _jd(dt):
    a = (14 - dt.month) // 12
    y = dt.year + 4800 - a
    m = dt.month + 12 * a - 3
    n = dt.day + (153*m+2)//5 + 365*y + y//4 - y//100 + y//400 - 32045
    return n + (dt.hour + dt.minute/60 + dt.second/3600 - 12) / 24

def _lahiri(jd_val):
    return 23.8506 + (jd_val - 2451545.0) * (50.27 / (3600 * 365.25))

def _sid(trop, jd_val):
    return (trop - _lahiri(jd_val)) % 360

def _sun_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L0 = 280.46646 + 36000.76983 * T
    M  = math.radians((357.52911 + 35999.05029*T) % 360)
    C  = (1.914602 - 0.004817*T)*math.sin(M) + 0.019993*math.sin(2*M) + 0.000289*math.sin(3*M)
    return (L0 + C) % 360

def _moon_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L0 = 218.3164477 + 481267.88123421*T - 0.0015786*T**2
    D  = math.radians((297.8501921 + 445267.1114034*T - 0.1851675*T**2) % 360)
    M  = math.radians((357.5291092 + 35999.0502909*T) % 360)
    Mp = math.radians((134.9633964 + 477198.8675055*T + 0.0087414*T**2) % 360)
    F  = math.radians((93.2720950  + 483202.0175233*T - 0.0036539*T**2) % 360)
    _terms = [
        (0,0,1,0,6288774),(2,0,-1,0,1274027),(2,0,0,0,658314),(0,0,2,0,213618),
        (0,1,0,0,-185116),(0,0,0,2,-114332),(2,0,-2,0,58793),(2,-1,-1,0,57066),
        (2,0,1,0,53322),(2,-1,0,0,45758),(0,1,-1,0,-40923),(1,0,0,0,-34720),
        (0,1,1,0,-30383),(2,0,0,-2,15327),(4,0,-1,0,10675),(0,0,3,0,10034),
        (4,0,-2,0,8548),(2,1,-1,0,-7888),(2,1,0,0,-6766),(1,0,-1,0,-5163),
        (1,1,0,0,4987),(2,-1,1,0,4036),(2,0,2,0,3994),(4,0,0,0,3861),
        (0,1,-2,0,-2689),(2,0,-3,0,3665),
    ]
    sl = sum(c[4]*math.sin(c[0]*D+c[1]*M+c[2]*Mp+c[3]*F) for c in _terms)
    return (L0 + sl/1_000_000) % 360

def _venus_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (181.979801 + 58517.8156760*T) % 360
    M = math.radians((212.106 + 58517.803*T) % 360)
    return (L + (0.007680*math.sin(M) + 0.000500*math.sin(2*M))*180/math.pi) % 360

def _mars_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (355.433 + 19140.2993*T) % 360
    M = math.radians((19.387 + 19140.300*T) % 360)
    return (L + 10.6912*math.sin(M) + 0.6228*math.sin(2*M)) % 360

def _jupiter_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (34.351519 + 3034.9056606*T) % 360
    M = math.radians((20.020 + 3034.906*T) % 360)
    return (L + 5.5549*math.sin(M) + 0.1683*math.sin(2*M)) % 360

def _saturn_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (50.077444 + 1222.1137943*T) % 360
    M = math.radians((316.967 + 1221.549*T) % 360)
    return (L + 6.3585*math.sin(M) + 0.2204*math.sin(2*M)) % 360

def _mercury_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (252.250906 + 149472.6746358*T) % 360
    M = math.radians((174.795 + 149472.515*T) % 360)
    return (L + 23.4400*math.sin(M) + 2.9818*math.sin(2*M) + 0.5255*math.sin(3*M)) % 360

def _rahu_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    return (125.04452 - 1934.136261*T + 0.0020708*T**2) % 360

_RASHIS = ["Aries","Taurus","Gemini","Cancer","Leo","Virgo",
           "Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"]
_NAKS = [
    "Ashwini","Bharani","Krittika","Rohini","Mrigasira","Ardra",
    "Punarvasu","Pushya","Ashlesha","Magha","Purva Phalguni","Uttara Phalguni",
    "Hasta","Chitra","Swati","Vishakha","Anuradha","Jyeshtha",
    "Mula","Purvashadha","Uttarashadha","Shravana","Dhanishtha","Shatabhisha",
    "Purva Bhadrapada","Uttara Bhadrapada","Revati",
]
_NAK_LORDS_ASTRO = ["Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury"] * 3
_YOGA_NAMES = [
    "Vishkumbha","Preeti","Ayushman","Saubhagya","Shobhana","Atiganda",
    "Sukarma","Dhriti","Shoola","Ganda","Vriddhi","Dhruva","Vyaghata",
    "Harshana","Vajra","Siddhi","Vyatipata","Variyana","Parigha",
    "Shiva","Siddha","Sadhya","Shubha","Shukla","Brahma","Indra","Vaidhriti"
]

def _nak(lon):
    idx = int(lon / (360/27))
    return _NAKS[idx], _NAK_LORDS_ASTRO[idx], idx

def _rashi(lon):
    return _RASHIS[int(lon/30)]

def _vedic_day_analysis(d_date):
    dt_utc = datetime(d_date.year, d_date.month, d_date.day, 3, 45, 0)
    jd_val = _jd(dt_utc)
    m_s  = _sid(_moon_trop(jd_val),    jd_val)
    s_s  = _sid(_sun_trop(jd_val),     jd_val)
    j_s  = _sid(_jupiter_trop(jd_val), j_s_val := jd_val)
    sa_s = _sid(_saturn_trop(jd_val),  sa_s_val := jd_val)
    
    yoga_raw  = (m_s + s_s) % 360
    yoga_idx  = int(yoga_raw / (360/27))
    yoga_name = _YOGA_NAMES[min(yoga_idx, 26)]
    
    return {
        "yoga_name": yoga_name,
        "jupiter_rashi": _rashi(j_s),
        "saturn_rashi": _rashi(sa_s),
    }


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
                _send_routed("HOLIDAY_ALERT",
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
                _send_routed("HOLIDAY_ALERT",
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
                _send_routed("HOLIDAY_ALERT",
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

    # Filter watchlist
    wl_df = df[df["Symbol"].isin(PANCHAK_ALERT_WATCHLIST)].copy()

    for _, row in wl_df.iterrows():
        sym = str(row["Symbol"]).strip()
        
        # Check dedup first
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

        # Get 1H candles for HTF analysis (worker has its own way of getting candles)
        # We can use the logic from run_bos_alerts or similar
        try:
            # In worker, we can use the OHLCStore or fetch fresh
            # The dashboard uses _get_live_candles_for_bos which combines hist + live
            # Worker also has _BOS_OK and _ohlc_db
            
            # Fetch 20 1H candles from Kite (consistent with dashboard logic)
            tk = get_token(sym)
            if not tk: continue
            
            # Use 5-day lookback for 1H candles to ensure we have enough for structure
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
                
                _send_routed("SMC_PANCHAK", msg, k)
                _log_alert("SMC_PANCHAK", [sym], "SMC+Panchak Confluence", ltp_now, "TG")
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
                        _send_routed("YEST_GREEN_BREAK",
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
                        _send_routed("YEST_RED_BREAK",
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
                    _send_routed("BREAK_ABOVE_1H_HIGH",
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
                    _send_routed("BREAK_BELOW_1H_LOW",
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
                    _send_routed("PANCHAK_RANGE",
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
                _send_routed("BT_ST_TARGET", msg, k)
                _log_alert("BT_HIT", [sym], "BUY TARGET (BT) HIT", ltp_map, "TG")
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
                _send_routed("BT_ST_TARGET", msg, k)
                _log_alert("ST_HIT", [sym], "SELL TARGET (ST) HIT", ltp_map, "TG")
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
                    _send_routed("NIFTY_SLOT_BREAK",
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
            _send_routed("NIFTY_SLOT_BREAK",
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
            _log_alert(f"NIFTY_SLOT_{direction}", [sym], f"{sym} KP SLOT BREAK", ltp_map, "TG")
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
        _kp_df_cache = pd.read_csv(_KP_CSV_PATH, encoding='utf-8')
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

    nifty_ltp  = float(ltp_map.get("NIFTY",     0) or 0)
    bnf_ltp    = float(ltp_map.get("BANKNIFTY", 0) or 0)
    sensex_ltp = float(ltp_map.get("SENSEX",    0) or 0)

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
                    f"NIFTY: {nifty_ltp:,.0f}  |  BankNifty: {bnf_ltp:,.0f}  |  SENSEX: {sensex_ltp:,.0f}\n"
                    f"⏰ {_now_str()}\n"
                    f"⚠️ <i>NOT financial advice.</i>"
                )
                _send_routed("KP_ALERTS", msg, open_key)
                log.info(f"🌙 KP Window Open: {slot_s}–{slot_e} [{sig_label}] → route:{_route('KP_ALERTS')}")

            # KP BREAK 15M alert
            if tg_on("KP_BREAK_15M") and nifty_ltp > 0:
                th  = float(_top_levels.get("NIFTY", {}).get("top_high", 0) or 0)
                tlo = float(_top_levels.get("NIFTY", {}).get("top_low",  0) or 0)
                s15 = _slot_15m()
                today_flat = today_s.replace("/","_")
                if th > 0 and nifty_ltp > th:
                    k = f"KP_BREAK15_UP_{today_flat}_{s15}"
                    if not already_sent(k):
                        _send_routed("KP_BREAK_15M",
                            f"⏱️ <b>KP BREAK 15M — NIFTY TOP HIGH BREAK ↑</b>\n"
                            f"Slot: {slot_s}–{slot_e}   KP: {sig_label}\n"
                            f"TOP HIGH: {th:,.2f}   LTP: {nifty_ltp:,.2f}\n"
                            f"⏰ {_now_str()}\n⚠️ <i>NOT financial advice.</i>", k)
                if tlo > 0 and nifty_ltp < tlo:
                    k = f"KP_BREAK15_DN_{today_flat}_{s15}"
                    if not already_sent(k):
                        _send_routed("KP_BREAK_15M",
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

def fetch_oi_intelligence(kite, symbol="NIFTY"):
    """
    Fetches OI data for Nifty or BankNifty and updates _last_oi.
    Saves results to CACHE/oi_intel_{SYMBOL}_YYYYMMDD.json.
    """
    global _last_oi
    try:
        # Resolve index name for Kite
        idx_full = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"
        spot_q = kite.quote([idx_full])
        spot   = spot_q[idx_full]["last_price"]
        step   = 50 if symbol == "NIFTY" else 100
        atm    = round(spot / step) * step

        # Next expiry Thursday (for Nifty/BNF)
        td  = _today()
        # Find next Thursday (weekday 3)
        days_ahead = (3 - td.weekday()) % 7
        if days_ahead == 0: days_ahead = 7
        exp_date = td + timedelta(days=days_ahead)
        exp_str = exp_date.strftime("%y%b%d").upper()

        # Build strike list
        strikes = [atm + i * step for i in range(-10, 11)]
        ce_total = pe_total = 0
        ce_oi_map = {}; pe_oi_map = {}

        # Fetch in one batch if possible, or 2 batches
        prefix = "NIFTY" if symbol == "NIFTY" else "BANKNIFTY"
        syms_to_fetch = []
        for strike in strikes:
            syms_to_fetch.append(f"NFO:{prefix}{exp_str}{strike}CE")
            syms_to_fetch.append(f"NFO:{prefix}{exp_str}{strike}PE")
        
        q_data = {}
        for i in range(0, len(syms_to_fetch), 50):
            q_data.update(kite.quote(syms_to_fetch[i:i+50]))

        for strike in strikes:
            ce_s = f"NFO:{prefix}{exp_str}{strike}CE"
            pe_s = f"NFO:{prefix}{exp_str}{strike}PE"
            coi = q_data.get(ce_s, {}).get("oi", 0)
            poi = q_data.get(pe_s, {}).get("oi", 0)
            ce_total += coi; pe_total += poi
            ce_oi_map[strike] = coi; pe_oi_map[strike] = poi

        pcr = round(pe_total / ce_total, 2) if ce_total > 0 else 0
        direction = (
            "📈 BULLISH" if pcr >= 1.2 else
            "📉 BEARISH" if pcr <= 0.8 else
            "↔️ NEUTRAL"
        )
        cw = max(ce_oi_map, key=ce_oi_map.get, default=atm)
        pf = max(pe_oi_map, key=pe_oi_map.get, default=atm)

        res = {
            "spot": spot, "atm": atm, "pcr": pcr,
            "direction": direction, "call_wall": cw, "put_floor": pf,
            "nearest_call_wall": cw, "nearest_put_floor": pf,
            "fetched_at": _now().isoformat(),
        }
        
        if symbol == "NIFTY":
            _last_oi = res

        # Save to cache
        path = os.path.join(CACHE_DIR, f"oi_intel_{symbol}_{_today_str()}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2)
        
        log.info(f"📊 OI {symbol}: PCR={pcr} CW={cw} PF={pf}")

        if tg_on("OI_INTEL"):
            k = f"OI_INTEL_{symbol}_{_slot_15m()}"
            if not already_sent(k):
                _send_routed("OI_INTEL",
                    f"📊 <b>OI Intelligence Update ({symbol})</b>\n⏰ {_now_str()}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Spot: <b>₹{spot:,.0f}</b>   ATM: <b>{atm:,}</b>\n"
                    f"PCR : <b>{pcr}</b>  →  {direction}\n"
                    f"📞 Call Wall : <b>{cw:,}</b>\n"
                    f"🛡️ Put Floor : <b>{pf:,}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <i>NOT financial advice.</i>",
                    k
                )
        return res

    except Exception as e:
        log.warning(f"OI fetch error [{symbol}]: {e}")
        # Try to load last valid from global or file
        if symbol == "NIFTY" and _last_oi:
            return _last_oi
        return {}


def _oi_30m_summary():
    if not tg_on("OI_30M_SUMMARY") or not _last_oi:
        return
    k = f"OI_30M_{_slot_30m()}"
    if already_sent(k):
        return
    oi = _last_oi
    _send_routed("OI_30M_SUMMARY",
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

        # Wrap send_telegram_fn to route via alert_toggles.json channel routing
        def _relay_send(msg: str, key: str = None, dedup_key: str = None):
            # Accept dedup_key kwarg from bos_scanner (alias for key)
            if dedup_key is not None and key is None:
                key = dedup_key
            # Primary channel routing from route_BOS_1H setting
            _send_routed("BOS_1H", msg, key)
            # Per-event-type ch2 routing based on message content
            _is_bos_up   = "BOS UP"   in msg or "BOS_UP"   in str(key or "")
            _is_bos_dn   = "BOS DOWN" in msg or "BOS_DOWN" in str(key or "")
            _is_choch_up = "CHoCH UP"   in msg or "CHOCH_UP"   in str(key or "")
            _is_choch_dn = "CHoCH DOWN" in msg or "CHOCH_DOWN" in str(key or "")
            # Only send to ch2 if not already routed there by route_BOS_1H=both
            _bos_route = _route("BOS_1H")
            if _bos_route != "both":
                if _is_bos_up   and _toggles.get("tg2_BOS_UP",   True):
                    send_tg2_bg(msg, f"CH2_{key}" if key else None)
                elif _is_bos_dn  and _toggles.get("tg2_BOS_DOWN",  True):
                    send_tg2_bg(msg, f"CH2_{key}" if key else None)
                elif _is_choch_up and _toggles.get("tg2_CHOCH_UP", True):
                    send_tg2_bg(msg, f"CH2_{key}" if key else None)
                elif _is_choch_dn and _toggles.get("tg2_CHOCH_DOWN", True):
                    send_tg2_bg(msg, f"CH2_{key}" if key else None)

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
def run_smc_combined(kite, ltp_map: dict, df_live=None):
    if not _SMC_OK:
        return
    global _last_oi
    try:
        # Fallback: load Nifty OI from cache if empty
        if not _last_oi:
            try:
                p = os.path.join(CACHE_DIR, f"oi_intel_NIFTY_{_today_str()}.json")
                if os.path.exists(p):
                    with open(p) as f: _last_oi = json.load(f)
            except: pass

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
            with open(os.path.join(CACHE_DIR, "smc_result.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {k: str(v) if not isinstance(v, (int,float,str,bool,list,dict,type(None)))
                     else v for k, v in smc.items()},
                    f, indent=2
                )
        except Exception:
            pass

        if tg_on("COMBINED_ENGINE"):
            _fire_combined(smc, ltp_map, df_live)
    except Exception as e:
        log.error(f"SMC/Combined Engine error: {e}")


def _fire_combined(smc: dict, ltp_map: dict, df_live=None):
    """
    Point 10: 15-Min Combined Confluence Alert.
    Cloned from dashboard v4.1 (_fire_heatmap_confluence_alert).
    """
    global _last_oi
    s15 = _slot_15m()
    k   = f"COMBINED_{_today_str()}_{s15}"
    if already_sent(k):
        return

    # 1. Heatmap Calculation (cloned logic)
    n50_bull = n50_bear = bnk_bull = bnk_bear = snx_bull = snx_bear = 0
    n50_bull_wt = n50_bear_wt = bnk_bull_wt = bnk_bear_wt = snx_bull_wt = snx_bear_wt = 0.0
    n50_impact = bnk_impact = snx_impact = 0.0
    
    # Load df_live if not provided (from latest CSV)
    if df_live is None or df_live.empty:
        try:
            df_live = pd.read_csv(_dated("live_data"))
        except Exception:
            df_live = pd.DataFrame()

    try:
        # Index LTPs for point conversion
        idx_path = _dated("indices_live")
        idx_ltps = {"NIFTY 50": 24315, "BANK NIFTY": 56086, "SENSEX": 80000}
        if os.path.exists(idx_path):
            try:
                idf = pd.read_csv(idx_path)
                idx_ltps.update(dict(zip(idf["Index"], idf["LTP"])))
            except: pass
        
        n50_ltp = idx_ltps.get("NIFTY 50", 24315)
        bnk_ltp = idx_ltps.get("BANK NIFTY", 56086)
        snx_ltp = idx_ltps.get("SENSEX", 80000)

        # Map CHANGE_% for all symbols
        chg_map = {}
        if not df_live.empty:
            chg_map = dict(zip(df_live["Symbol"], df_live["CHANGE_%"]))

        # Nifty 50
        for sym, wt in N50_WTS.items():
            c = float(chg_map.get(sym, 0) or 0)
            n50_impact += (wt * c / 100)
            if c > 0.25:   n50_bull_wt += wt; n50_bull += 1
            elif c < -0.25: n50_bear_wt += wt; n50_bear += 1
        n50_bull_pct = n50_bull_wt / (n50_bull_wt + n50_bear_wt + 0.01) * 100

        # Bank Nifty
        for sym, wt in BNK_WTS.items():
            c = float(chg_map.get(sym, 0) or 0)
            bnk_impact += (wt * c / 100)
            if c > 0.25:   bnk_bull_wt += wt; bnk_bull += 1
            elif c < -0.25: bnk_bear_wt += wt; bnk_bear += 1
        bnk_bull_pct = bnk_bull_wt / (bnk_bull_wt + bnk_bear_wt + 0.01) * 100

        # Sensex
        for sym, wt in SNX_WTS.items():
            c = float(chg_map.get(sym, 0) or 0)
            snx_impact += (wt * c / 100)
            if c > 0.25:   snx_bull_wt += wt; snx_bull += 1
            elif c < -0.25: snx_bear_wt += wt; snx_bear += 1
        snx_bull_pct = snx_bull_wt / (snx_bull_wt + snx_bear_wt + 0.01) * 100

        n50_pts = round(n50_impact / 100 * n50_ltp, 0)
        bnk_pts = round(bnk_impact / 100 * bnk_ltp, 0)
        snx_pts = round(snx_impact / 100 * snx_ltp, 0)

        n50_sig = "🟢 BULLISH" if n50_bull_pct > 55 else ("🔴 BEARISH" if n50_bull_pct < 45 else "🟡 NEUTRAL")
        bnk_sig = "🟢 BULLISH" if bnk_bull_pct > 55 else ("🔴 BEARISH" if bnk_bull_pct < 45 else "🟡 NEUTRAL")
        snx_sig = "🟢 BULLISH" if snx_bull_pct > 55 else ("🔴 BEARISH" if snx_bull_pct < 45 else "🟡 NEUTRAL")

    except Exception as e:
        log.warning(f"Heatmap calc error: {e}")
        n50_bull_pct = bnk_bull_pct = snx_bull_pct = 50.0
        n50_sig = bnk_sig = snx_sig = "—"
        n50_pts = bnk_pts = snx_pts = 0

    # 2. SMC + OI + Astro Signals
    smc_sig = str(smc.get("final_signal", "—"))
    smc_sc  = int(smc.get("final_score",  0) or 0)
    
    # Nifty OI fallback
    if not _last_oi:
        try:
            p = os.path.join(CACHE_DIR, f"oi_intel_NIFTY_{_today_str()}.json")
            if os.path.exists(p):
                with open(p) as f: _last_oi = json.load(f)
        except: pass

    oi_dir  = _last_oi.get("direction", "—")
    oi_pcr  = float(_last_oi.get("pcr", 0) or 0)
    
    # BankNifty OI (fetch fresh)
    bnk_oi_dir = "—"; bnk_oi_pcr = 0.0
    try:
        bnk_path = os.path.join(CACHE_DIR, f"oi_intel_BANKNIFTY_{_today_str()}.json")
        if os.path.exists(bnk_path):
            with open(bnk_path) as f:
                b_oi = json.load(f)
                bnk_oi_dir = b_oi.get("direction", "—")
                bnk_oi_pcr = float(b_oi.get("pcr", 0) or 0)
    except: pass

    a_sig = "—"; a_sc = 0; a_nak = "—"; a_tit = "—"; kp_sub = "—"
    if _ASTRO_OK:
        try:
            a = _vedic_day_analysis(_now().date())
            a_sig = a.get("overall", "—")
            a_sc  = int(a.get("net_score", 0) or 0)
            a_nak = a.get("moon_nak", "—")
            a_tit = a.get("tithi", "").split("(")[0].strip()
            kp_sub = a.get("kp_sub", "—")
        except: pass

    # 3. Conviction Scorer
    conv_score = 0
    reasons = []
    
    if n50_bull_pct > 60:
        conv_score += 2
        reasons.append(f"Heatmap N50: {n50_bull_pct:.0f}% wt BULL ({n50_pts:+.0f}pts)")
    elif n50_bull_pct > 52:
        conv_score += 1
        reasons.append(f"Heatmap N50: {n50_bull_pct:.0f}% wt mildly BULL")
    elif n50_bull_pct < 40:
        conv_score -= 2
        reasons.append(f"Heatmap N50: {n50_bull_pct:.0f}% wt BEAR ({n50_pts:+.0f}pts)")
    elif n50_bull_pct < 48:
        conv_score -= 1
        reasons.append(f"Heatmap N50: {n50_bull_pct:.0f}% wt mildly BEAR")

    if "BULLISH" in smc_sig.upper():
        conv_score += 2
        reasons.append(f"SMC: {smc_sig} (score {smc_sc:+d})")
    elif "BEARISH" in smc_sig.upper():
        conv_score -= 2
        reasons.append(f"SMC: {smc_sig} (score {smc_sc:+d})")

    if "BULLISH" in oi_dir.upper():
        conv_score += 1
        reasons.append(f"N50 OI: {oi_dir} (PCR {oi_pcr:.2f})")
    elif "BEARISH" in oi_dir.upper():
        conv_score -= 1
        reasons.append(f"N50 OI: {oi_dir} (PCR {oi_pcr:.2f})")

    if "BULLISH" in a_sig.upper():
        conv_score += 1
        reasons.append(f"Astro: {a_sig} ({a_nak}/{a_tit})")
    elif "BEARISH" in a_sig.upper():
        conv_score -= 1
        reasons.append(f"Astro: {a_sig} ({a_nak}/{a_tit})")

    # Verdict
    if conv_score >= 6:    verdict = "🔥🔥 EXTREME BULL — HIGH CONVICTION LONG"
    elif conv_score >= 3:  verdict = "🟢🟢 STRONG BULL — STAY LONG"
    elif conv_score >= 1:  verdict = "🟢 MILD BULL — Trail longs / avoid shorts"
    elif conv_score <= -6: verdict = "🔴🔴 EXTREME BEAR — HIGH CONVICTION SHORT"
    elif conv_score <= -3: verdict = "🔴🔴 STRONG BEAR — STAY SHORT"
    elif conv_score <= -1: verdict = "🔴 MILD BEAR — Trail shorts / avoid longs"
    else:                  verdict = "🟡 NEUTRAL / RANGING — No clear edge"

    max_conv = 9
    conv_pct = int(abs(conv_score) / max_conv * 100)
    reasons_txt = "\n".join([f"  • {r}" for r in reasons]) if reasons else "  • No strong signals"

    # 4. Message Assembly
    slot_str = f"{s15[:2]}:{s15[2:]}"
    now_str  = _now_str()
    msg = (
        f"📊📊📊📊📊📊📊📊📊📊\n"
        f"<b>🗺️ HEATMAP CONFLUENCE ALERT — {slot_str}</b>  ⏰ {now_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 VERDICT: {verdict}</b>\n"
        f"<b>Conviction: {conv_score:+d}/{max_conv} ({conv_pct}%)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Nifty 50:</b> {n50_sig} | <b>BankNifty:</b> {bnk_sig}\n"
        f"🏛️ <b>Sensex:</b> {snx_sig}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Nifty 50 Details:</b>\n"
        f"   {n50_bull}▲ ({n50_bull_wt:.0f}% wt)  {n50_bear}▼ ({n50_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{n50_impact:+.2f}% ≈ {n50_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 <b>BankNifty Details:</b>\n"
        f"   {bnk_bull}▲ ({bnk_bull_wt:.0f}% wt)  {bnk_bear}▼ ({bnk_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{bnk_impact:+.2f}% ≈ {bnk_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛️ <b>Sensex Details:</b>\n"
        f"   {snx_bull}▲ ({snx_bull_wt:.0f}% wt)  {snx_bear}▼ ({snx_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{snx_impact:+.2f}% ≈ {snx_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>SMC:</b> {smc_sig} (score {smc_sc:+d})\n"
        f"📉 <b>OI:</b> N50: {oi_dir} | BNK: {bnk_oi_dir}\n"
        f"📈 <b>PCR:</b> N50: {oi_pcr:.2f} | BNK: {bnk_oi_pcr:.2f}\n"
        f"🌙 <b>Astro:</b> {a_sig} | 🔑 <b>KP:</b> {kp_sub}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Why this verdict:</b>\n{reasons_txt}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>NOT financial advice. Auto-15min.</i>\n"
        f"📊📊📊📊📊📊📊📊📊📊"
    )

    _send_routed("COMBINED_ENGINE", msg, k)
    log.info(f"🧠 Confluence Engine sent [{verdict}] ({conv_score})")

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
        _send_routed("ASTRO_ADVANCE", msg, k)
        log.info(f"🌟 Astro advance → route:{_route('ASTRO_ADVANCE')}: {sig} score={score}")
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
    "NIFTY 50":       "NSE:NIFTY 50",
    "BANK NIFTY":     "NSE:NIFTY BANK",
    "FINNIFTY":       "NSE:NIFTY FIN SERVICE",
    "SENSEX":         "BSE:SENSEX",
    "NIFTY IT":       "NSE:NIFTYIT",
    "NIFTY FMCG":     "NSE:NIFTYFMCG",
    "NIFTY PHARMA":   "NSE:NIFTYPHARMA",
    "NIFTY METAL":    "NSE:NIFTYMETAL",
    "NIFTY AUTO":     "NSE:NIFTYAUTO",
    "NIFTY ENERGY":   "NSE:NIFTYENERGY",
    "NIFTY PSU BANK": "NSE:NIFTYPSUBANK",
}

def fetch_indices(kite):
    try:
        all_q = kite.quote(list(INDEX_SYMS.values()))
        rows  = []
        for name, full_sym in INDEX_SYMS.items():
            q = all_q.get(full_sym)
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
            "version":     "4.3",
        }
        if extra:
            data.update(extra)
        with open(os.path.join(CACHE_DIR, "worker_heartbeat.json"), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 65)
    log.info("  Panchak Background Worker v4.3 — Synced with dashboard v3.4")
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
        with open(os.path.join(CACHE_DIR, "worker.pid"), "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass

    # Kite init
    global kite
    try:
        kite = init_kite()
    except Exception as e:
        log.error(f"❌ Kite init failed: {e}")
        sys.exit(1)

    # Bootstrap
    _load_tg_dedup()
    _reload_toggles()
    _load_instruments(kite)

    # Initial OI load from cache
    try:
        p = os.path.join(CACHE_DIR, f"oi_intel_NIFTY_{_today_str()}.json")
        if os.path.exists(p):
            with open(p) as f: global _last_oi; _last_oi = json.load(f)
            log.info(f"📈 Loaded cached Nifty OI: {_last_oi.get('direction')}")
    except: pass

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

            # ── Chart Pattern Auto-Scan (once per hour during mkt hrs) ─
            _check_chart_pattern_scan()

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
                        _check_heatmap_reversal(df)
                        _check_panchak_special_rules(df)
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
                    fetch_oi_intelligence(kite, "NIFTY")
                    fetch_oi_intelligence(kite, "BANKNIFTY")
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
                    ltp_now = {}; df_tmp = None
                    try:
                        df_tmp  = pd.read_csv(_dated("live_data"))
                        ltp_now = dict(zip(df_tmp["Symbol"], df_tmp["LTP"]))
                    except Exception:
                        pass
                    run_smc_combined(kite, ltp_now, df_tmp)
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
