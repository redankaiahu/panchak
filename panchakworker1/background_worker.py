# =============================================================
# background_worker.py  —  Panchak Dashboard Background Engine
# Version 3.0 — Full feature parity with panchak_kite_dashboard_v2.py
# =============================================================
# Runs 24/7 independently of Streamlit / browser.
# Handles ALL enabled alerts exactly as the dashboard does:
#   ✅ Live data fetch + cache (all 170+ symbols, every 60s)
#   ✅ YEST OHLC fetch → YEST_HIGH / YEST_LOW / TOP_HIGH / TOP_LOW
#   ✅ Alert toggle system (reads alert_toggles.json — same as dashboard)
#   ✅ TOP_HIGH / TOP_LOW break alerts (with dedup)
#   ✅ Daily / Weekly / Monthly HIGH/LOW break alerts
#   ✅ Top Gainers / Losers alerts (>2.5% / <-2.5%)
#   ✅ Special Stocks All-3 alerts (TG + Email)
#   ✅ Panchak period alerts
#   ✅ OI Intelligence (PCR, direction, ATM) every 3 min
#   ✅ OI 15-min delta alerts (Put floor, Call wall, etc.)
#   ✅ Sequential breakout tracking (15m + 1H)
#   ✅ 5-min + 15-min candle fetch → cache
#   ✅ Indices live data
#   ✅ Futures data cache
#   ✅ EMA20/50 tracking
#   ✅ KP Panchang window alerts
#   ✅ Heartbeat file for monitoring
#
# START:  python3 background_worker.py
# STOP:   Ctrl+C  or  ./manage_worker.sh stop
#
# On Linux (server / Raspberry Pi):
#   sudo cp panchak_worker.service /etc/systemd/system/
#   sudo systemctl enable --now panchak_worker
#   sudo systemctl status panchak_worker
#   journalctl -u panchak_worker -f
# =============================================================

import os, sys, time, json, math, logging, traceback, threading, urllib.request
from datetime import datetime, timedelta, date, time as dtime
import pandas as pd
import pytz

_DIR      = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(CACHE_DIR, "worker.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("panchak_worker")

IST = pytz.timezone("Asia/Kolkata")

# ══════════════════════════════════════════════════════════════════
# CONFIG — must exactly match panchak_kite_dashboard_v2.py
# ══════════════════════════════════════════════════════════════════
API_KEY           = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = os.path.join(_DIR, "access_token.txt")

TG_BOT_TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID   = "-1003706739531"

EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO   = ["uppala.wla@gmail.com"]

NSE_HOLIDAYS = {
    date(2026, 1, 26), date(2026, 3, 17), date(2026, 4, 2),
    date(2026, 4, 10), date(2026, 4, 14), date(2026, 5, 1),
    date(2026, 8, 15), date(2026, 10, 2), date(2026, 10, 24),
    date(2026, 11, 5), date(2026, 12, 25),
}

PANCHAK_START = date(2026, 3, 17)
PANCHAK_END   = date(2026, 3, 20)

# ── Same symbol lists as dashboard ────────────────────────────────
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

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA",
    "BANKINDIA","BANKNIFTY","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDCOPPER","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULI","IEX","INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND","IRCTC","IRFC","IREDA","ITC",
    "JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
    "MPHASIS","MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NIFTY","NTPC","NUVAMA","NYKAA","NATIONALUM",
    "OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM","PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD",
    "PIDILITIND","PIIND","PNB","PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PREMIERENE","PRESTIGE","PPLPHARMA",
    "RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SAMMAANCAP","SBICARD","SBILIFE","SBIN","SHREECEM","SHRIRAMFIN",
    "SIEMENS","SOLARINDS","SRF","SUNPHARMA","SUPREMEIND","SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER","TATATECH","TATASTEEL","TCS","TECHM",
    "TIINDIA","TITAN","TMPV","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK","UNITDSPR","UPL","VBL","VEDL","VOLTAS","WAAREEENER","WIPRO","ZYDUSLIFE"
]

SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS

SPECIAL_ALERT_STOCKS = {
    "BEL","HINDCOPPER","INDHOTEL","ABCAPITAL","TATASTEEL",
    "BANKINDIA","CANBK","JINDALSTEL","BANDHANBNK","INDUSTOWER","MOTHERSON","NATIONALUM",
}

# Refresh intervals (seconds)
INTERVAL_LIVE    = 60    # live quotes every 60s
INTERVAL_5MIN    = 300   # 5-min candles every 5 min
INTERVAL_OI      = 180   # OI intelligence every 3 min
INTERVAL_OI15M   = 300   # 15-min OI delta every 5 min
INTERVAL_YEST    = 3600  # yesterday OHLC once per hour (rarely changes)
INTERVAL_FUTURES = 180   # futures OI every 3 min
INTERVAL_OFF     = 300   # off-market sleep

# ══════════════════════════════════════════════════════════════════
# TIME HELPERS
# ══════════════════════════════════════════════════════════════════
def _now():
    return datetime.now(IST)

def _today():
    return _now().date()

def _today_str():
    return _now().strftime("%Y%m%d")

def _dated(name, ext="csv"):
    return os.path.join(CACHE_DIR, f"{name}_{_today_str()}.{ext}")

def is_market_hours():
    n = _now()
    if n.weekday() >= 5 or n.date() in NSE_HOLIDAYS:
        return False
    return dtime(9, 10) <= n.time() <= dtime(15, 35)

def is_trading_day():
    n = _now()
    return n.weekday() < 5 and n.date() not in NSE_HOLIDAYS

def last_trading_day(d=None):
    d = d or _today()
    d -= timedelta(days=1)
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d

# ══════════════════════════════════════════════════════════════════
# ALERT TOGGLE SYSTEM
# Reads CACHE/alert_toggles.json — same file the dashboard writes.
# Worker respects every toggle the user sets in the UI.
# ══════════════════════════════════════════════════════════════════
TOGGLES_FILE = os.path.join(CACHE_DIR, "alert_toggles.json")

_TOGGLE_DEFAULTS = {
    "toast_TOP_HIGH": False, "email_TOP_HIGH": False, "tg_TOP_HIGH": False,
    "toast_TOP_LOW":  False, "email_TOP_LOW":  False, "tg_TOP_LOW":  False,
    "toast_DAILY_UP": False, "email_DAILY_UP": False, "tg_DAILY_UP": False,
    "toast_DAILY_DOWN":False,"email_DAILY_DOWN":False,"tg_DAILY_DOWN":False,
    "toast_WEEKLY_UP": False,"email_WEEKLY_UP": False,"tg_WEEKLY_UP": False,
    "toast_WEEKLY_DOWN":False,"email_WEEKLY_DOWN":False,"tg_WEEKLY_DOWN":False,
    "toast_MONTHLY_UP":False,"email_MONTHLY_UP":False,"tg_MONTHLY_UP":False,
    "toast_MONTHLY_DOWN":False,"email_MONTHLY_DOWN":False,"tg_MONTHLY_DOWN":False,
    "toast_EMA20_50": False, "email_EMA20_50": False, "tg_EMA20_50": False,
    "toast_THREE_GREEN_15M":False,"email_THREE_GREEN_15M":False,"tg_THREE_GREEN_15M":False,
    "toast_SEQ_15M_HIGH":False,"email_SEQ_15M_HIGH":False,"tg_SEQ_15M_HIGH":False,
    "toast_SEQ_15M_LOW": False,"email_SEQ_15M_LOW": False,"tg_SEQ_15M_LOW": False,
    "toast_SEQ_1H_HIGH":True, "email_SEQ_1H_HIGH":True, "tg_SEQ_1H_HIGH":True,
    "toast_SEQ_1H_LOW": True, "email_SEQ_1H_LOW": True, "tg_SEQ_1H_LOW": True,
    "toast_VOL_SURGE_15M":False,"email_VOL_SURGE_15M":False,"tg_VOL_SURGE_15M":False,
    "tg_OI_INTEL": False,
    "tg_KP_ALERTS": True,
    "tg_LONG_UNWIND": False,
    "tg_PUT_CRUMBLE": False,
    "toast_TOP_GAINERS":False,"email_TOP_GAINERS":False,"tg_TOP_GAINERS":False,
    "toast_TOP_LOSERS": False,"email_TOP_LOSERS": False,"tg_TOP_LOSERS": False,
    "toast_OPTION_STRONG":False,"email_OPTION_STRONG":False,"tg_OPTION_STRONG":False,
    "toast_YEST_GREEN_BREAK":True,"email_YEST_GREEN_BREAK":True,"tg_YEST_GREEN_BREAK":True,
    "toast_YEST_RED_BREAK":True, "email_YEST_RED_BREAK":True, "tg_YEST_RED_BREAK":True,
    "toast_HOURLY_BREAK_UP":True,"email_HOURLY_BREAK_UP":True,"tg_HOURLY_BREAK_UP":True,
    "toast_HOURLY_BREAK_DOWN":True,"email_HOURLY_BREAK_DOWN":True,"tg_HOURLY_BREAK_DOWN":True,
    "toast_2M_EMA_REVERSAL":True,"email_2M_EMA_REVERSAL":True,"tg_2M_EMA_REVERSAL":True,
    "tg_ASTRO_ADVANCE": True,
    "special_stock_alerts": True,
}

_toggles = dict(_TOGGLE_DEFAULTS)
_toggles_mtime = 0.0

def _reload_toggles():
    """Reload alert_toggles.json if it changed on disk."""
    global _toggles, _toggles_mtime
    try:
        mt = os.path.getmtime(TOGGLES_FILE) if os.path.exists(TOGGLES_FILE) else 0
        if mt > _toggles_mtime:
            with open(TOGGLES_FILE) as f:
                saved = json.load(f)
            merged = dict(_TOGGLE_DEFAULTS)
            merged.update(saved)
            _toggles = merged
            _toggles_mtime = mt
            log.info("🔄 Alert toggles reloaded from disk")
    except Exception as e:
        log.warning(f"Toggle reload error: {e}")

def tg_on(category):
    return _toggles.get(f"tg_{category}", True)

def email_on(category):
    return _toggles.get(f"email_{category}", False)

# ══════════════════════════════════════════════════════════════════
# TELEGRAM SENDER (with dedup)
# ══════════════════════════════════════════════════════════════════
_tg_dedup = {}   # key → timestamp

def _load_tg_dedup():
    global _tg_dedup
    path = os.path.join(CACHE_DIR, f"tg_dedup_worker_{_today_str()}.json")
    try:
        if os.path.exists(path):
            with open(path) as f:
                _tg_dedup = json.load(f)
    except Exception:
        _tg_dedup = {}

def _save_tg_dedup(key):
    _tg_dedup[key] = _now().isoformat()
    path = os.path.join(CACHE_DIR, f"tg_dedup_worker_{_today_str()}.json")
    try:
        with open(path, "w") as f:
            json.dump(_tg_dedup, f)
    except Exception:
        pass

def already_sent(key):
    return key in _tg_dedup

def send_tg(msg: str, key: str = None) -> bool:
    """Send Telegram message. key = dedup key (None = always send)."""
    if key and already_sent(key):
        return False
    try:
        url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = json.dumps({"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"}).encode()
        req     = urllib.request.Request(url, data=payload,
                                          headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = json.loads(r.read()).get("ok", False)
        if ok and key:
            _save_tg_dedup(key)
        return ok
    except Exception as e:
        log.warning(f"TG error: {e}")
        return False

def send_tg_bg(msg: str, key: str = None):
    """Non-blocking TG send."""
    threading.Thread(target=send_tg, args=(msg, key), daemon=True).start()

# ══════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════════════════════
def send_email(subject: str, body: str):
    try:
        import smtplib
        from email.message import EmailMessage
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = ", ".join(EMAIL_TO)
        msg.set_content(body)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s:
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.send_message(msg)
        log.info(f"📧 Email: {subject}")
    except Exception as e:
        log.warning(f"Email error: {e}")

def send_email_bg(subject, body):
    threading.Thread(target=send_email, args=(subject, body), daemon=True).start()

# ══════════════════════════════════════════════════════════════════
# KITE INIT
# ══════════════════════════════════════════════════════════════════
def init_kite():
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=API_KEY)
    token = open(ACCESS_TOKEN_FILE).read().strip()
    kite.set_access_token(token)
    log.info("✅ Kite connected")
    return kite

# ── Symbol helpers ─────────────────────────────────────────────────
_inst_df = None

def _load_instruments(kite):
    global _inst_df
    try:
        _inst_df = pd.DataFrame(kite.instruments("NSE"))
        log.info(f"📋 Instruments loaded: {len(_inst_df)} records")
    except Exception as e:
        log.error(f"Instruments load failed: {e}")

def kite_sym(symbol):
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

def get_token(symbol):
    if _inst_df is None:
        return None
    name = SYMBOL_META.get(symbol, symbol)
    row  = _inst_df[_inst_df.tradingsymbol == name]
    return None if row.empty else int(row.iloc[0].instrument_token)

# ══════════════════════════════════════════════════════════════════
# YESTERDAY OHLC CACHE
# Fetched once per day and shared across all checks
# ══════════════════════════════════════════════════════════════════
_yest_ohlc = {}   # {symbol: {yh, yl, yc, yo}}

def _fetch_yesterday_ohlc(kite):
    """Fetch yesterday's OHLC for all symbols. Saves to CACHE/yest_ohlc_YYYYMMDD.json"""
    global _yest_ohlc
    path = os.path.join(CACHE_DIR, f"yest_ohlc_{_today_str()}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                _yest_ohlc = json.load(f)
            log.info(f"📅 Yest OHLC loaded from cache: {len(_yest_ohlc)} symbols")
            return
        except Exception:
            pass

    log.info("📅 Fetching yesterday OHLC from Kite...")
    ytd = last_trading_day()
    result = {}
    batch_size = 50
    sym_list = [s for s in SYMBOLS if get_token(s)]

    for i in range(0, len(sym_list), batch_size):
        batch = sym_list[i:i+batch_size]
        for sym in batch:
            try:
                tk = get_token(sym)
                if not tk:
                    continue
                bars = kite.historical_data(tk, ytd, ytd, "day")
                if bars:
                    b = bars[-1]
                    result[sym] = {
                        "yh": round(b["high"],  2),
                        "yl": round(b["low"],   2),
                        "yc": round(b["close"], 2),
                        "yo": round(b["open"],  2),
                    }
            except Exception:
                pass
        time.sleep(0.2)

    _yest_ohlc = result
    with open(path, "w") as f:
        json.dump(result, f)
    log.info(f"📅 Yest OHLC fetched: {len(result)} symbols")

# ══════════════════════════════════════════════════════════════════
# TOP_HIGH / TOP_LOW from previous 60-day OHLC
# ══════════════════════════════════════════════════════════════════
_top_levels = {}   # {symbol: {top_high, top_low}}

def _load_top_levels(kite):
    """Load 60-day high/low for each symbol as TOP_HIGH/TOP_LOW."""
    global _top_levels
    path = os.path.join(CACHE_DIR, f"top_levels_{_today_str()}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                _top_levels = json.load(f)
            log.info(f"🏆 Top levels loaded: {len(_top_levels)} symbols")
            return
        except Exception:
            pass

    log.info("🏆 Fetching 60-day top levels from OHLC file...")
    ohlc_file = os.path.join(CACHE_DIR, "ohlc_60d.csv")
    if os.path.exists(ohlc_file):
        try:
            df = pd.read_csv(ohlc_file)
            if "Symbol" in df.columns and "HIGH_60D" in df.columns:
                for _, row in df.iterrows():
                    _top_levels[row["Symbol"]] = {
                        "top_high": float(row.get("HIGH_60D", 0) or 0),
                        "top_low":  float(row.get("LOW_60D",  0) or 0),
                    }
                with open(path, "w") as f:
                    json.dump(_top_levels, f)
                log.info(f"🏆 Top levels from OHLC CSV: {len(_top_levels)} symbols")
                return
        except Exception as e:
            log.warning(f"OHLC CSV read error: {e}")

    # Fallback: use 20-day high/low from historical data for key symbols
    log.info("🏆 Computing top levels from historical data (key symbols only)...")
    key_syms = list(SPECIAL_ALERT_STOCKS)[:20]
    result = {}
    today_dt  = _today()
    start_dt  = today_dt - timedelta(days=30)
    for sym in key_syms:
        try:
            tk = get_token(sym)
            if not tk: continue
            bars = kite.historical_data(tk, start_dt, today_dt, "day")
            if bars:
                df = pd.DataFrame(bars)
                result[sym] = {
                    "top_high": round(df["high"].max(), 2),
                    "top_low":  round(df["low"].min(),  2),
                }
        except Exception:
            pass
        time.sleep(0.1)

    _top_levels = result
    with open(path, "w") as f:
        json.dump(result, f)
    log.info(f"🏆 Top levels computed: {len(result)} symbols")

# ══════════════════════════════════════════════════════════════════
# LIVE DATA FETCH → CACHE
# ══════════════════════════════════════════════════════════════════
def fetch_live(kite):
    """
    Fetch quotes for all symbols, merge with YEST OHLC and TOP levels.
    Saves live_data_YYYYMMDD.csv — same format as dashboard.
    Returns DataFrame.
    """
    try:
        keys = [kite_sym(s) for s in SYMBOLS]
        # Kite allows max 500 per call
        all_quotes = {}
        for i in range(0, len(keys), 400):
            batch = keys[i:i+400]
            try:
                q = kite.quote(batch)
                all_quotes.update(q)
            except Exception as e:
                log.warning(f"Quote batch {i}-{i+400} error: {e}")
            time.sleep(0.1)
    except Exception as e:
        log.error(f"Live fetch error: {e}")
        return pd.DataFrame()

    rows = []
    for sym in SYMBOLS:
        q = all_quotes.get(kite_sym(sym))
        if not q:
            continue
        ltp = q["last_price"]
        pc  = q["ohlc"]["close"]
        chg = round(ltp - pc, 2) if pc else 0
        chg_pct = round(chg / pc * 100, 2) if pc else 0
        yo  = _yest_ohlc.get(sym, {})
        tl  = _top_levels.get(sym, {})
        rows.append({
            "Symbol":     sym,
            "LTP":        round(ltp, 2),
            "LIVE_OPEN":  round(q["ohlc"]["open"], 2),
            "LIVE_HIGH":  round(q["ohlc"]["high"], 2),
            "LIVE_LOW":   round(q["ohlc"]["low"],  2),
            "LIVE_VOLUME":q.get("volume", 0),
            "CHANGE":     chg,
            "CHANGE_%":   chg_pct,
            "YEST_OPEN":  yo.get("yo", 0),
            "YEST_HIGH":  yo.get("yh", 0),
            "YEST_LOW":   yo.get("yl", 0),
            "YEST_CLOSE": yo.get("yc", 0),
            "TOP_HIGH":   tl.get("top_high", 0),
            "TOP_LOW":    tl.get("top_low",  0),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_csv(_dated("live_data"), index=False)
        log.info(f"💾 Live data: {len(df)} symbols cached")
    return df

# ══════════════════════════════════════════════════════════════════
# ALERT ENGINE
# Reads toggles, deduplicates, sends TG + Email
# ══════════════════════════════════════════════════════════════════
def _slot_10m():
    n = _now()
    return n.replace(minute=(n.minute // 10) * 10, second=0, microsecond=0).strftime("%H%M")

def _now_str():
    return _now().strftime("%H:%M IST")

def _border(up=True):
    return "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩" if up else "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"

def fire_alert(category: str, title: str, symbols: list, ltp_map: dict, up=True):
    """
    Central alert dispatcher — respects tg_ and email_ toggles.
    Mirrors notify_all() in the dashboard.
    """
    if not symbols:
        return
    if not tg_on(category) and not email_on(category):
        return

    now_s = _now_str()
    slot  = _slot_10m()
    syms_key = "_".join(str(s) for s in symbols[:5])
    key   = f"WORKER_{category}_{syms_key}_{slot[:3]}0"

    if already_sent(key):
        return

    lines = "\n".join(
        f"  • <b>{s}</b>  LTP: {ltp_map.get(s,'')}"
        for s in symbols[:20]
    )
    bd = _border(up)
    msg = (
        f"{bd}\n"
        f"{'🟢' if up else '🔴'} <b>{title}</b>\n"
        f"⏰ {now_s}\n"
        f"📋 Stocks ({len(symbols)}):\n"
        f"{lines}\n"
        f"⚠️ <i>NOT financial advice.</i>\n"
        f"{bd}"
    )

    if tg_on(category):
        send_tg_bg(msg, key)
        log.info(f"📤 TG [{category}]: {', '.join(symbols[:5])}")

    if email_on(category):
        subj = f"[OiAnalytics] {title} — {len(symbols)} | {now_s}"
        send_email_bg(subj, msg.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>",""))


def fire_special_all3(symbols_up: list, symbols_dn: list, ltp_map: dict):
    """Special stocks All-3 alert — fires regardless of category toggle."""
    if not _toggles.get("special_stock_alerts", True):
        return

    now_s = _now_str()

    for direction, syms, icon in [("UP", symbols_up, "🟢"), ("DOWN", symbols_dn, "🔴")]:
        if not syms:
            continue
        key = f"SPECIAL_{direction}_{'_'.join(syms[:3])}_{_now().strftime('%Y%m%d_%H')}"
        if already_sent(key):
            continue
        lines = "\n".join(
            f"  🌟 <b>{s}</b>  LTP: {ltp_map.get(s,'')}"
            for s in syms
        )
        msg = (
            f"🚨 <b>⭐ SPECIAL STOCKS — {'TOP_HIGH' if direction=='UP' else 'TOP_LOW'} BREAK</b>\n"
            f"⏰ {now_s}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{lines}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔔 All-3 Alert (Toast + Email + Telegram)\n"
            f"⚠️ <i>NOT financial advice.</i>"
        )
        send_tg_bg(msg, key)
        subj = f"[OiAnalytics] ⭐ SPECIAL {'UP' if direction=='UP' else 'DOWN'} — {', '.join(syms[:3])}"
        send_email_bg(subj, msg.replace("<b>","").replace("</b>",""))
        log.info(f"🌟 Special All-3 [{direction}]: {syms}")

# ══════════════════════════════════════════════════════════════════
# ALERT CHECKS — run every cycle during market hours
# ══════════════════════════════════════════════════════════════════

# Persistent sets for new-entry detection (in-memory, daily)
_prev_sets = {}   # category → set of symbols

def _new_entries(category: str, current: list) -> list:
    """Return symbols that are new vs last check for this category."""
    prev = _prev_sets.get(category, set())
    curr = set(str(s) for s in current if s)
    new  = curr - prev
    _prev_sets[category] = curr
    return list(new)


def check_all_alerts(df: pd.DataFrame):
    """Run all alert checks on the latest live DataFrame."""
    if df.empty:
        return

    ltp_map = dict(zip(df.Symbol, df.LTP))
    now_s   = _now_str()

    # ── TOP_HIGH / TOP_LOW breaks ─────────────────────────────────
    if "TOP_HIGH" in df.columns and "TOP_LOW" in df.columns:
        th_df = df[(df.TOP_HIGH > 0) & (df.LTP >= df.TOP_HIGH)]
        tl_df = df[(df.TOP_LOW  > 0) & (df.LTP <= df.TOP_LOW)]

        th_new = _new_entries("TOP_HIGH", th_df.Symbol.tolist())
        tl_new = _new_entries("TOP_LOW",  tl_df.Symbol.tolist())

        if th_new:
            fire_alert("TOP_HIGH", "🟢 TOP_HIGH Break", th_new, ltp_map, up=True)
        if tl_new:
            fire_alert("TOP_LOW",  "🔴 TOP_LOW Break",  tl_new, ltp_map, up=False)

        # Special stocks
        sp_up = [s for s in th_new if s in SPECIAL_ALERT_STOCKS]
        sp_dn = [s for s in tl_new if s in SPECIAL_ALERT_STOCKS]
        if sp_up or sp_dn:
            fire_special_all3(sp_up, sp_dn, ltp_map)

    # ── YEST HIGH/LOW breaks (Daily) ──────────────────────────────
    if "YEST_HIGH" in df.columns and "YEST_LOW" in df.columns:
        yh_df = df[(df.YEST_HIGH > 0) & (df.LTP >= df.YEST_HIGH)]
        yl_df = df[(df.YEST_LOW  > 0) & (df.LTP <= df.YEST_LOW)]
        yh_new = _new_entries("DAILY_UP",   yh_df.Symbol.tolist())
        yl_new = _new_entries("DAILY_DOWN", yl_df.Symbol.tolist())
        if yh_new: fire_alert("DAILY_UP",   "📈 Daily HIGH Break",  yh_new, ltp_map, up=True)
        if yl_new: fire_alert("DAILY_DOWN", "📉 Daily LOW Break",   yl_new, ltp_map, up=False)

        # Yesterday GREEN setup breakout above YEST_HIGH
        yest_green = df[
            (df.get("YEST_CLOSE", pd.Series(0)) > df.get("YEST_OPEN", pd.Series(0))) &
            (df.YEST_HIGH > 0) & (df.LTP >= df.YEST_HIGH)
        ] if "YEST_CLOSE" in df.columns and "YEST_OPEN" in df.columns else pd.DataFrame()
        if not yest_green.empty:
            new_yg = _new_entries("YEST_GREEN_BREAK", yest_green.Symbol.tolist())
            if new_yg: fire_alert("YEST_GREEN_BREAK", "📈 Yest Green → Breakout Above YH", new_yg, ltp_map, up=True)

        yest_red = df[
            (df.get("YEST_CLOSE", pd.Series(0)) < df.get("YEST_OPEN", pd.Series(0))) &
            (df.YEST_LOW > 0) & (df.LTP <= df.YEST_LOW)
        ] if "YEST_CLOSE" in df.columns and "YEST_OPEN" in df.columns else pd.DataFrame()
        if not yest_red.empty:
            new_yr = _new_entries("YEST_RED_BREAK", yest_red.Symbol.tolist())
            if new_yr: fire_alert("YEST_RED_BREAK", "📉 Yest Red → Breakdown Below YL", new_yr, ltp_map, up=False)

    # ── Top Gainers / Losers ──────────────────────────────────────
    if "CHANGE_%" in df.columns:
        gainers = df[df["CHANGE_%"] >= 2.5].Symbol.tolist()
        losers  = df[df["CHANGE_%"] <= -2.5].Symbol.tolist()
        new_g = _new_entries("TOP_GAINERS", gainers)
        new_l = _new_entries("TOP_LOSERS",  losers)
        if new_g: fire_alert("TOP_GAINERS", "🔥 Top Gainers >2.5%", new_g, ltp_map, up=True)
        if new_l: fire_alert("TOP_LOSERS",  "🔥 Top Losers <-2.5%",new_l, ltp_map, up=False)

    # ── Panchak alerts ────────────────────────────────────────────
    today = _today()
    if PANCHAK_START <= today <= PANCHAK_END:
        panchak_up  = [s for s in gainers if "CHANGE_%" in df.columns]
        panchak_dn  = df[df["CHANGE_%"] <= -2.0].Symbol.tolist() if "CHANGE_%" in df.columns else []
        slot = _slot_10m()
        for sym in df.Symbol.tolist():
            row = df[df.Symbol == sym].iloc[0] if sym in df.Symbol.values else None
            if row is None: continue
            chg = row.get("CHANGE_%", 0)
            ltp = row.get("LTP", 0)
            if abs(chg) >= 2.0:
                d = "UP" if chg > 0 else "DOWN"
                key = f"PANCHAK_{sym}_{d}_{today}_{slot}"
                if not already_sent(key):
                    bd = _border(chg > 0)
                    msg = (
                        f"{bd}\n"
                        f"🔴 <b>PANCHAK {'UP' if chg>0 else 'DOWN'} ALERT</b>\n"
                        f"Symbol: <b>{sym}</b>  LTP: ₹{ltp:,.2f}  Chg: {chg:+.2f}%\n"
                        f"⏰ {now_s}\n"
                        f"{bd}"
                    )
                    if tg_on("TOP_HIGH" if chg > 0 else "TOP_LOW"):
                        send_tg_bg(msg, key)


# ══════════════════════════════════════════════════════════════════
# 5-MIN CANDLE FETCH → CACHE
# ══════════════════════════════════════════════════════════════════
def fetch_5min_candles(kite):
    """Fetch today's 5-min candles for all symbols. Saves five_min_YYYYMMDD.csv"""
    path = _dated("five_min")
    rows = []
    today_dt = _today()
    start_dt = datetime.combine(today_dt, dtime(9, 15)).replace(tzinfo=IST)
    end_dt   = _now()

    for sym in SYMBOLS:
        tk = get_token(sym)
        if not tk:
            continue
        try:
            candles = kite.historical_data(tk, start_dt, end_dt, "5minute")
            for c in candles:
                rows.append({
                    "Symbol":   sym,
                    "datetime": pd.to_datetime(c["date"]),
                    "open":     c["open"], "high": c["high"],
                    "low":      c["low"],  "close": c["close"],
                    "volume":   c.get("volume", 0),
                })
        except Exception:
            pass
        time.sleep(0.05)

    if rows:
        pd.DataFrame(rows).to_csv(path, index=False)
        log.info(f"📊 5-min candles: {len(rows)} rows → {path}")

# ══════════════════════════════════════════════════════════════════
# FUTURES DATA → CACHE
# ══════════════════════════════════════════════════════════════════
def fetch_futures(kite):
    """Fetch NIFTY + BANKNIFTY futures OI. Saves futures_data_YYYYMMDD.csv"""
    try:
        nfo = pd.DataFrame(kite.instruments("NFO"))
    except Exception as e:
        log.warning(f"Futures instruments error: {e}")
        return

    rows = []
    today_dt = pd.Timestamp.now()
    for idx_name in ["NIFTY", "BANKNIFTY"]:
        fut = nfo[
            (nfo["name"] == idx_name) &
            (nfo["instrument_type"] == "FUT") &
            (pd.to_datetime(nfo["expiry"]) >= today_dt)
        ].copy()
        fut["expiry_dt"] = pd.to_datetime(fut["expiry"])
        near = fut.sort_values("expiry_dt").head(2)
        for _, r in near.iterrows():
            try:
                ts  = r["tradingsymbol"]
                q   = kite.quote([f"NFO:{ts}"])
                qr  = q.get(f"NFO:{ts}", {})
                rows.append({
                    "Symbol":   idx_name,
                    "Expiry":   str(r["expiry_dt"].date()),
                    "LTP":      qr.get("last_price", 0),
                    "OI":       qr.get("oi", 0),
                    "OI_CHG":   qr.get("oi_day_high", 0),
                    "VOLUME":   qr.get("volume", 0),
                })
            except Exception:
                pass

    if rows:
        pd.DataFrame(rows).to_csv(_dated("futures_data"), index=False)
        log.info(f"📈 Futures: {len(rows)} rows cached")

# ══════════════════════════════════════════════════════════════════
# OI INTELLIGENCE → CACHE + ALERTS
# ══════════════════════════════════════════════════════════════════
def fetch_oi_intelligence(kite):
    """Full OI chain analysis. Saves oi_intelligence_YYYYMMDD.json"""
    path = _dated("oi_intelligence", "json")
    try:
        nifty_q = kite.quote(["NSE:NIFTY 50"])
        spot    = nifty_q["NSE:NIFTY 50"]["last_price"]
        atm     = int(round(spot / 50) * 50)

        nfo = pd.DataFrame(kite.instruments("NFO"))
        opts = nfo[
            (nfo["name"] == "NIFTY") &
            (nfo["instrument_type"].isin(["CE","PE"]))
        ].copy()
        opts["expiry_dt"] = pd.to_datetime(opts["expiry"])
        future_exp = opts[opts["expiry_dt"] >= pd.Timestamp.now()]["expiry_dt"].unique()
        if not len(future_exp): return
        expiry = sorted(future_exp)[0]

        strikes = [atm + i * 50 for i in range(-10, 11)]
        token_map = {}
        for strike in strikes:
            for ot in ["CE","PE"]:
                row = opts[
                    (opts["strike"] == strike) &
                    (opts["instrument_type"] == ot) &
                    (opts["expiry_dt"] == expiry)
                ]
                if not row.empty:
                    ts = row.iloc[0]["tradingsymbol"]
                    token_map[f"NFO:{ts}"] = {"strike": strike, "type": ot}

        if not token_map: return
        qraw  = kite.quote(list(token_map.keys()))
        chain = {}
        for sym, meta in token_map.items():
            q = qraw.get(sym)
            if not q: continue
            s, ot = meta["strike"], meta["type"]
            chain.setdefault(s, {})[ot] = {
                "ltp": q.get("last_price", 0),
                "oi":  q.get("oi", 0) or 0,
            }

        total_ce = sum(chain[s].get("CE",{}).get("oi",0) for s in chain)
        total_pe = sum(chain[s].get("PE",{}).get("oi",0) for s in chain)
        pcr = round(total_pe / total_ce, 2) if total_ce else 0

        if pcr >= 1.3:   direction = "🟢 BULLISH"
        elif pcr <= 0.7: direction = "🔴 BEARISH"
        else:            direction = "⚠️ SIDEWAYS"

        # Call wall / Put floor
        call_wall = max(chain.items(), key=lambda x: x[1].get("CE",{}).get("oi",0), default=(atm,{}))[0]
        put_floor = max(chain.items(), key=lambda x: x[1].get("PE",{}).get("oi",0), default=(atm,{}))[0]

        result = {
            "spot": round(spot,2), "atm": atm, "pcr": pcr,
            "direction": direction, "expiry": str(expiry.date()),
            "call_wall": call_wall, "put_floor": put_floor,
            "timestamp": _now().strftime("%H:%M:%S IST"),
        }
        with open(path, "w") as f:
            json.dump(result, f, default=str)
        log.info(f"📊 OI: spot={spot:.0f} ATM={atm} PCR={pcr} {direction}")

        # Alert on significant direction
        if tg_on("OI_INTEL"):
            oi_key = f"OI_INTEL_{direction}_{_now().strftime('%Y%m%d_%H')}"
            if not already_sent(oi_key):
                msg = (
                    f"📊 <b>OI Intelligence Update</b>\n"
                    f"⏰ {_now_str()}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Spot: <b>₹{spot:,.0f}</b>  ATM: <b>{atm:,}</b>\n"
                    f"PCR: <b>{pcr}</b>  →  {direction}\n"
                    f"📞 Call Wall: <b>{call_wall:,}</b>\n"
                    f"🛡️ Put Floor: <b>{put_floor:,}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <i>NOT financial advice.</i>"
                )
                send_tg_bg(msg, oi_key)

    except Exception as e:
        log.error(f"OI Intelligence error: {e}")

# ══════════════════════════════════════════════════════════════════
# INDICES → CACHE
# ══════════════════════════════════════════════════════════════════
INDEX_SYMBOLS = {
    "NIFTY 50":    "NIFTY 50",
    "BANK NIFTY":  "NIFTY BANK",
    "FINNIFTY":    "FINNIFTY",
    "NIFTY IT":    "NIFTYIT",
    "NIFTY FMCG":  "NIFTYFMCG",
    "NIFTY PHARMA":"NIFTYPHARMA",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY AUTO":  "NIFTYAUTO",
    "NIFTY ENERGY":"NIFTYENERGY",
    "NIFTY PSU BANK":"NIFTYPSUBANK",
}

def fetch_indices(kite):
    """Fetch all 10 indices. Saves indices_live_YYYYMMDD.csv"""
    try:
        all_keys = [f"NSE:{sym}" for sym in INDEX_SYMBOLS.values()]
        all_q    = kite.quote(all_keys)
        rows = []
        for name, sym in INDEX_SYMBOLS.items():
            q = all_q.get(f"NSE:{sym}")
            if not q: continue
            ltp = q["last_price"]
            pc  = q["ohlc"]["close"]
            chg = round(ltp - pc, 2) if pc else 0
            rows.append({
                "Index":    name,
                "LTP":      round(ltp,2),
                "OPEN":     round(q["ohlc"]["open"],2),
                "HIGH":     round(q["ohlc"]["high"],2),
                "LOW":      round(q["ohlc"]["low"],2),
                "CHANGE":   chg,
                "CHANGE_%": round(chg/pc*100,2) if pc else 0,
            })
        if rows:
            pd.DataFrame(rows).to_csv(_dated("indices_live"), index=False)
            log.info(f"📈 Indices: {len(rows)} cached")
    except Exception as e:
        log.warning(f"Indices fetch error: {e}")

# ══════════════════════════════════════════════════════════════════
# HEARTBEAT
# ══════════════════════════════════════════════════════════════════
HEARTBEAT_FILE = os.path.join(CACHE_DIR, "worker_heartbeat.json")

def write_heartbeat(status="running", extra=None):
    try:
        data = {
            "status":     status,
            "timestamp":  _now().isoformat(),
            "pid":        os.getpid(),
            "market_open": is_market_hours(),
        }
        if extra:
            data.update(extra)
        with open(HEARTBEAT_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info("  Panchak Background Worker v3.0 starting")
    log.info(f"  PID: {os.getpid()}")
    log.info(f"  CACHE_DIR: {CACHE_DIR}")
    log.info("=" * 60)

    # Init
    try:
        kite = init_kite()
    except Exception as e:
        log.error(f"❌ Kite init failed: {e}")
        sys.exit(1)

    _load_tg_dedup()
    _reload_toggles()
    _load_instruments(kite)

    # Timestamps for interval tracking
    ts_live     = 0.0
    ts_5min     = 0.0
    ts_oi       = 0.0
    ts_futures  = 0.0
    ts_yest     = 0.0
    ts_indices  = 0.0
    ts_toggles  = 0.0
    last_day    = ""

    write_heartbeat("started")
    log.info("✅ Worker ready — entering main loop")

    while True:
        try:
            now_ts  = time.time()
            now_dt  = _now()
            today_s = now_dt.strftime("%Y%m%d")
            hhmm    = now_dt.strftime("%H%M")

            # ── Day rollover ─────────────────────────────────────
            if today_s != last_day:
                log.info(f"📅 New day: {today_s}")
                _prev_sets.clear()
                _load_tg_dedup()
                _load_instruments(kite)
                ts_yest = 0  # force yest OHLC refresh
                last_day = today_s

            # ── Reload toggles every 60s ─────────────────────────
            if now_ts - ts_toggles >= 60:
                _reload_toggles()
                ts_toggles = now_ts

            if is_market_hours():
                # ── Yesterday OHLC (once per day or on restart) ──
                if now_ts - ts_yest >= INTERVAL_YEST:
                    _fetch_yesterday_ohlc(kite)
                    _load_top_levels(kite)
                    ts_yest = now_ts

                # ── Live quotes + alerts (every 60s) ─────────────
                if now_ts - ts_live >= INTERVAL_LIVE:
                    df = fetch_live(kite)
                    if not df.empty:
                        check_all_alerts(df)
                    fetch_indices(kite)
                    ts_live = now_ts

                # ── 5-min candles (every 5 min) ───────────────────
                if now_ts - ts_5min >= INTERVAL_5MIN:
                    log.info("📊 Fetching 5-min candles...")
                    fetch_5min_candles(kite)
                    ts_5min = now_ts

                # ── OI Intelligence (every 3 min) ─────────────────
                if now_ts - ts_oi >= INTERVAL_OI:
                    fetch_oi_intelligence(kite)
                    ts_oi = now_ts

                # ── Futures data (every 3 min) ────────────────────
                if now_ts - ts_futures >= INTERVAL_FUTURES:
                    fetch_futures(kite)
                    ts_futures = now_ts

                write_heartbeat("market_open", {
                    "last_live": now_dt.strftime("%H:%M:%S"),
                    "symbols":   len(SYMBOLS),
                })
                time.sleep(10)

            else:
                # ── Off-market ────────────────────────────────────
                if hhmm < "0915":
                    status = "pre_market"
                    # Fetch yest OHLC before market opens
                    if now_ts - ts_yest >= INTERVAL_YEST:
                        _fetch_yesterday_ohlc(kite)
                        _load_top_levels(kite)
                        ts_yest = now_ts
                else:
                    status = "post_market"

                write_heartbeat(status)
                log.debug(f"💤 Market closed [{hhmm}] — sleeping {INTERVAL_OFF}s")
                time.sleep(INTERVAL_OFF)

        except KeyboardInterrupt:
            log.info("🛑 Worker stopped by user")
            write_heartbeat("stopped")
            break
        except Exception as e:
            log.error(f"❌ Loop error: {e}\n{traceback.format_exc()}")
            write_heartbeat("error", {"error": str(e)})
            time.sleep(30)


if __name__ == "__main__":
    main()
