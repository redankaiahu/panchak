# =============================================================
# background_worker.py  —  Panchak Dashboard Background Engine
# =============================================================
# Runs 24/7 independently of Streamlit / browser.
# Handles: live data fetch → cache → alerts → OI → EMA7 → TG/email
#
# START:  python3 background_worker.py
# STOP:   Ctrl+C  (or kill the process)
#
# On Linux run as a service so it auto-starts:
#   sudo cp panchak_worker.service /etc/systemd/system/
#   sudo systemctl enable panchak_worker
#   sudo systemctl start  panchak_worker
#   sudo systemctl status panchak_worker   ← check logs
# =============================================================

import os, sys, time, json, math, logging, traceback
from datetime import datetime, timedelta, date
import pandas as pd
import pytz

# ── Add project directory to path so we can import shared modules ──
_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)

# ── Logging setup ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(_DIR, "CACHE", "worker.log")),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("panchak_worker")

# ── IST timezone ───────────────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

# ══════════════════════════════════════════════════════════════════
# CONFIG — must match panchak_kite_dashboard_fixed27_5.py exactly
# ══════════════════════════════════════════════════════════════════
API_KEY           = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = os.path.join(_DIR, "access_token.txt")
BASE_DIR          = _DIR
CACHE_DIR         = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

TG_BOT_TOKEN  = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID    = "-1003706739531"

EMAIL_FROM    = "awslabuppala1985@gmail.com"
EMAIL_PASS    = "uiipybranzfsgmxm"
EMAIL_TO      = ["uppala.wla@gmail.com"]

PANCHAK_START = date(2026, 3, 17)
PANCHAK_END   = date(2026, 3, 20)

NSE_HOLIDAYS = {
    date(2026, 1, 26), date(2026, 3, 17), date(2026, 4, 2),
    date(2026, 4, 10), date(2026, 4, 14), date(2026, 5, 1),
    date(2026, 8, 15), date(2026, 10, 2), date(2026, 10, 24),
    date(2026, 11, 5), date(2026, 12, 25),
}

INDEX_ONLY_SYMBOLS = ["NIFTY 50", "NIFTY BANK", "INDIA VIX"]
STOCKS = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR",
    "ITC","SBIN","BAJFINANCE","BHARTIARTL","KOTAKBANK","LT",
    "AXISBANK","ASIANPAINT","MARUTI","SUNPHARMA","WIPRO",
    "ULTRACEMCO","TITAN","NESTLEIND","TECHM","HCLTECH","INDUSINDBK",
    "POWERGRID","ONGC","NTPC","JSWSTEEL","TATAMOTORS","ADANIENT",
    "ADANIPORTS","BAJAJ-AUTO","DRREDDY","DIVISLAB","CIPLA",
    "BPCL","COALINDIA","TATACONSUM","HEROMOTOCO","HINDALCO","GRASIM",
]
SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS

# ── Refresh intervals (seconds) ───────────────────────────────────
INTERVAL_LIVE   = 60      # live data + alerts every 60s during market
INTERVAL_OI     = 180     # OI intelligence every 3 min
INTERVAL_EMA7   = 60      # EMA7 rebuild every 60s
INTERVAL_OFF    = 300     # off-market: check every 5 min (does nothing heavy)

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def _today_str():
    return datetime.now(IST).strftime("%Y%m%d")

def _dated(name, ext="csv"):
    return os.path.join(CACHE_DIR, f"{name}_{_today_str()}.{ext}")

def _now_ist():
    return datetime.now(IST)

def is_market_hours():
    now = _now_ist()
    if now.weekday() >= 5:
        return False
    if now.date() in NSE_HOLIDAYS:
        return False
    t = now.time()
    import datetime as _dt
    return _dt.time(9, 10) <= t <= _dt.time(15, 35)

def is_trading_day():
    now = _now_ist()
    return now.weekday() < 5 and now.date() not in NSE_HOLIDAYS

def kite_symbol(s):
    if s in ("NIFTY 50", "NIFTY BANK", "INDIA VIX"):
        return f"NSE:{s}"
    return f"NSE:{s}"

# ── Dedup store (in-memory, resets on restart) ────────────────────
_alerted = set()   # set of alert keys already fired this session
_tg_dedup_file = os.path.join(CACHE_DIR, f"tg_dedup_{_today_str()}.json")

def _load_dedup():
    global _alerted
    try:
        if os.path.exists(_tg_dedup_file):
            with open(_tg_dedup_file) as f:
                _alerted = set(json.load(f).keys())
    except Exception:
        _alerted = set()

def _save_dedup(key):
    _alerted.add(key)
    try:
        existing = {}
        if os.path.exists(_tg_dedup_file):
            with open(_tg_dedup_file) as f:
                existing = json.load(f)
        existing[key] = _now_ist().isoformat()
        with open(_tg_dedup_file, "w") as f:
            json.dump(existing, f)
    except Exception:
        pass

def _already_alerted(key):
    return key in _alerted

# ══════════════════════════════════════════════════════════════════
# KITE CONNECT INIT
# ══════════════════════════════════════════════════════════════════
def init_kite():
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=API_KEY)
    token = open(ACCESS_TOKEN_FILE).read().strip()
    kite.set_access_token(token)
    log.info("✅ Kite connected successfully")
    return kite

# ══════════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ══════════════════════════════════════════════════════════════════
def send_tg(message: str):
    import urllib.request, urllib.parse
    try:
        url  = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id":    TG_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            if not resp.get("ok"):
                log.warning(f"TG send failed: {resp}")
    except Exception as e:
        log.warning(f"TG error: {e}")

# ══════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════════════════════
def send_email(subject: str, body: str):
    import smtplib
    from email.message import EmailMessage
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = ", ".join(EMAIL_TO)
        msg.set_content(body)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)
        log.info(f"📧 Email sent: {subject}")
    except Exception as e:
        log.warning(f"Email error: {e}")

# ══════════════════════════════════════════════════════════════════
# LIVE DATA FETCH → CACHE
# ══════════════════════════════════════════════════════════════════
LIVE_CACHE_CSV = ""   # set in main() after date known
OI_CACHE_CSV   = ""

def fetch_and_cache_live(kite):
    global LIVE_CACHE_CSV, OI_CACHE_CSV
    LIVE_CACHE_CSV = _dated("live_data")
    OI_CACHE_CSV   = _dated("oi_data")

    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except Exception as e:
        log.error(f"kite.quote failed: {e}")
        return None, None

    live_rows, oi_rows = [], []
    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue
        ltp = q["last_price"]
        pc  = q["ohlc"]["close"]
        chg = ltp - pc if pc else 0
        live_rows.append({
            "Symbol":      s,
            "LTP":         round(ltp, 2),
            "LIVE_OPEN":   round(q["ohlc"]["open"], 2),
            "LIVE_HIGH":   round(q["ohlc"]["high"], 2),
            "LIVE_LOW":    round(q["ohlc"]["low"],  2),
            "LIVE_VOLUME": q.get("volume", 0),
            "CHANGE":      round(chg, 2),
            "CHANGE_%":    round((chg / pc * 100), 2) if pc else 0,
        })
        oi = q.get("oi")
        oi_dl = q.get("oi_day_low")
        if oi is not None and oi_dl:
            oi_pct = ((oi - oi_dl) / oi_dl * 100) if oi_dl else 0
            oi_rows.append({"Symbol": s, "OI": oi, "OI_CHANGE_%": round(oi_pct, 2)})

    df_live = pd.DataFrame(live_rows)
    df_oi   = pd.DataFrame(oi_rows) if oi_rows else pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])

    if not df_live.empty:
        df_live.to_csv(LIVE_CACHE_CSV, index=False)
        log.info(f"💾 Live data cached: {len(df_live)} symbols → {LIVE_CACHE_CSV}")
    if not df_oi.empty:
        df_oi.to_csv(OI_CACHE_CSV, index=False)

    return df_live, df_oi

# ══════════════════════════════════════════════════════════════════
# PANCHAK ALERTS
# ══════════════════════════════════════════════════════════════════
def check_panchak_alerts(df_live):
    if df_live is None or df_live.empty:
        return
    today = _now_ist().date()
    if not (PANCHAK_START <= today <= PANCHAK_END):
        return   # not in panchak period

    for _, row in df_live.iterrows():
        sym  = row["Symbol"]
        ltp  = row.get("LTP", 0)
        chg  = row.get("CHANGE_%", 0)

        # Strong move during panchak = alert
        if abs(chg) >= 2.0:
            direction = "📈 UP" if chg > 0 else "📉 DOWN"
            key = f"PANCHAK_{sym}_{direction}_{today}"
            if not _already_alerted(key):
                msg = (
                    f"🔴 <b>PANCHAK ALERT</b>\n"
                    f"Symbol: <b>{sym}</b>\n"
                    f"LTP: ₹{ltp:,.2f}  |  Change: {chg:+.2f}%\n"
                    f"Direction: {direction}\n"
                    f"⚠️ Panchak period active ({PANCHAK_START} → {PANCHAK_END})\n"
                    f"🕐 {_now_ist().strftime('%H:%M:%S IST')}"
                )
                send_tg(msg)
                _save_dedup(key)
                log.info(f"🔔 Panchak alert fired: {sym} {direction} {chg:+.2f}%")

# ══════════════════════════════════════════════════════════════════
# BREAKOUT ALERTS (YEST HIGH/LOW)
# ══════════════════════════════════════════════════════════════════
def check_breakout_alerts(kite, df_live):
    """Check if any symbol has broken yesterday's high or low."""
    if df_live is None or df_live.empty:
        return
    today = _now_ist().date()

    for _, row in df_live.iterrows():
        sym  = row["Symbol"]
        ltp  = row.get("LTP", 0)
        chg  = row.get("CHANGE_%", 0)
        high = row.get("LIVE_HIGH", 0)
        low  = row.get("LIVE_LOW", 0)

        # Try to get yesterday OHLC from cache
        yh_key = f"YBREAK_{sym}_{today}"
        yl_key = f"YLBREAK_{sym}_{today}"

        # We use LIVE_HIGH proxy if YEST_HIGH not in df
        # (full YEST_HIGH requires historical fetch — done by main dashboard)
        # Here we fire on strong % move + volume as breakout proxy
        vol   = row.get("LIVE_VOLUME", 0)
        open_ = row.get("LIVE_OPEN", ltp)

        if chg >= 1.5 and ltp > open_ and vol > 50000:
            if not _already_alerted(yh_key):
                msg = (
                    f"🟢 <b>BREAKOUT UP</b> — {sym}\n"
                    f"LTP: ₹{ltp:,.2f}  |  Change: {chg:+.2f}%\n"
                    f"Vol: {vol:,}\n"
                    f"🕐 {_now_ist().strftime('%H:%M:%S IST')}"
                )
                send_tg(msg)
                _save_dedup(yh_key)
                log.info(f"🟢 Breakout UP: {sym} {chg:+.2f}%")

        if chg <= -1.5 and ltp < open_ and vol > 50000:
            if not _already_alerted(yl_key):
                msg = (
                    f"🔴 <b>BREAKDOWN</b> — {sym}\n"
                    f"LTP: ₹{ltp:,.2f}  |  Change: {chg:+.2f}%\n"
                    f"Vol: {vol:,}\n"
                    f"🕐 {_now_ist().strftime('%H:%M:%S IST')}"
                )
                send_tg(msg)
                _save_dedup(yl_key)
                log.info(f"🔴 Breakdown: {sym} {chg:+.2f}%")

# ══════════════════════════════════════════════════════════════════
# OI INTELLIGENCE → CACHE
# ══════════════════════════════════════════════════════════════════
def fetch_and_cache_oi(kite):
    OI_INTEL_PATH = _dated("oi_intelligence", "json")
    try:
        # Import the function from main dashboard if available
        # Otherwise use inline mini version
        nifty_quote = kite.quote(["NSE:NIFTY 50"])
        spot = nifty_quote["NSE:NIFTY 50"]["last_price"]

        step = 50
        atm  = int(round(spot / step) * step)

        # Get instruments
        try:
            nfo_inst = pd.DataFrame(kite.instruments("NFO"))
        except Exception:
            log.warning("OI: instruments fetch failed")
            return

        nifty_opts = nfo_inst[
            (nfo_inst["name"] == "NIFTY") &
            (nfo_inst["instrument_type"].isin(["CE","PE"]))
        ].copy()
        nifty_opts["expiry_dt"] = pd.to_datetime(nifty_opts["expiry"])
        today_dt = pd.Timestamp.now()
        future_exp = nifty_opts[nifty_opts["expiry_dt"] >= today_dt]["expiry_dt"].unique()
        if not len(future_exp):
            return
        expiry = sorted(future_exp)[0]

        strikes = [atm + i * step for i in range(-10, 11)]
        token_map = {}
        for strike in strikes:
            for opt_type in ["CE","PE"]:
                row = nifty_opts[
                    (nifty_opts["strike"] == strike) &
                    (nifty_opts["instrument_type"] == opt_type) &
                    (nifty_opts["expiry_dt"] == expiry)
                ]
                if not row.empty:
                    ts = row.iloc[0]["tradingsymbol"]
                    token_map[f"NFO:{ts}"] = {"strike": strike, "type": opt_type}

        if not token_map:
            return

        quotes_raw = kite.quote(list(token_map.keys()))
        chain = {}
        for symbol, meta in token_map.items():
            q = quotes_raw.get(symbol)
            if not q:
                continue
            s  = meta["strike"]
            ot = meta["type"]
            chain.setdefault(s, {})[ot] = {
                "ltp": q.get("last_price", 0),
                "oi":  q.get("oi", 0) or 0,
            }

        # PCR
        total_ce = sum(chain[s].get("CE",{}).get("oi",0) for s in chain)
        total_pe = sum(chain[s].get("PE",{}).get("oi",0) for s in chain)
        pcr = round(total_pe / total_ce, 2) if total_ce else 0

        direction = "🟢 BULLISH" if pcr >= 1.3 else ("🔴 BEARISH" if pcr <= 0.7 else "⚠️ SIDEWAYS")

        result = {
            "spot":      round(spot, 2),
            "atm":       atm,
            "pcr":       pcr,
            "direction": direction,
            "expiry":    str(expiry.date()),
            "timestamp": _now_ist().strftime("%H:%M:%S IST"),
        }
        with open(OI_INTEL_PATH, "w") as f:
            json.dump(result, f, default=str)
        log.info(f"📊 OI cached: spot={spot} ATM={atm} PCR={pcr} → {direction}")

        # TG alert on strong OI signal
        oi_key = f"OI_{direction}_{_now_ist().strftime('%Y%m%d_%H')}"
        if not _already_alerted(oi_key):
            msg = (
                f"📊 <b>OI Intelligence Update</b>\n"
                f"Spot: ₹{spot:,.0f}  |  ATM: {atm}\n"
                f"PCR: {pcr}  |  Direction: {direction}\n"
                f"🕐 {_now_ist().strftime('%H:%M IST')}"
            )
            send_tg(msg)
            _save_dedup(oi_key)

    except Exception as e:
        log.error(f"OI fetch error: {e}")

# ══════════════════════════════════════════════════════════════════
# EMA7 REBUILD → CACHE
# ══════════════════════════════════════════════════════════════════
def rebuild_ema7(kite):
    EMA7_15M = _dated("ema7_15min")
    EMA7_1H  = _dated("ema7_1hour")

    rows_15m, rows_1h = [], []
    for sym in SYMBOLS:
        try:
            ksym = f"NSE:{sym}"
            # Fetch today's 5-min candles
            inst_df = pd.DataFrame(kite.instruments("NSE"))
            inst_df = inst_df[inst_df["tradingsymbol"] == sym]
            if inst_df.empty:
                continue
            token = inst_df.iloc[0]["instrument_token"]
            now   = _now_ist()
            candles = kite.historical_data(
                token,
                from_date=now.replace(hour=9, minute=0, second=0, microsecond=0),
                to_date=now,
                interval="5minute",
            )
            if not candles:
                continue
            df = pd.DataFrame(candles)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")

            # 15-min EMA7
            df_15 = df["close"].resample("15min").last().dropna()
            if len(df_15) >= 2:
                ema7_15 = df_15.ewm(span=7).mean().iloc[-1]
                rows_15m.append({"Symbol": sym, "EMA7_15M": round(ema7_15, 2), "CANDLES_15M": len(df_15)})

            # 1H EMA7
            df_1h = df["close"].resample("60min").last().dropna()
            if len(df_1h) >= 2:
                ema7_1h = df_1h.ewm(span=7).mean().iloc[-1]
                rows_1h.append({"Symbol": sym, "EMA7_1H": round(ema7_1h, 2), "CANDLES_1H": len(df_1h)})

        except Exception:
            continue

    if rows_15m:
        pd.DataFrame(rows_15m).to_csv(EMA7_15M, index=False)
    if rows_1h:
        pd.DataFrame(rows_1h).to_csv(EMA7_1H, index=False)
    if rows_15m or rows_1h:
        log.info(f"📈 EMA7 rebuilt: {len(rows_15m)} symbols 15m, {len(rows_1h)} 1h")

# ══════════════════════════════════════════════════════════════════
# ASTRO DAILY ALERT (once per day at 9:00 AM)
# ══════════════════════════════════════════════════════════════════
def send_daily_astro_alert():
    try:
        # Import vedic analysis from main dashboard
        sys.path.insert(0, _DIR)
        # Inline simplified version to avoid full streamlit import
        astro_cache = os.path.join(CACHE_DIR, f"astro_{_today_str()}.json")
        if os.path.exists(astro_cache):
            return   # already sent today

        # Try importing the vedic engine
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "dashboard",
                os.path.join(_DIR, "panchak_kite_dashboard_fixed27_5.py")
            )
            # Don't actually load the full dashboard (it needs streamlit)
            # Instead just call the standalone math functions
        except Exception:
            pass

        # Write placeholder so we don't retry
        with open(astro_cache, "w") as f:
            json.dump({"sent": True, "ts": _now_ist().isoformat()}, f)

        today = _now_ist().date()
        wd    = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][today.weekday()]
        msg = (
            f"🌙 <b>Daily Astro Summary — {today.strftime('%d %b %Y')} ({wd})</b>\n"
            f"Dashboard is running in background mode.\n"
            f"Open http://localhost:8501 to see full astro analysis.\n"
            f"🕐 Worker started: {_now_ist().strftime('%H:%M IST')}"
        )
        send_tg(msg)
        log.info("🌙 Daily astro alert sent")
    except Exception as e:
        log.warning(f"Astro alert error: {e}")

# ══════════════════════════════════════════════════════════════════
# HEARTBEAT — write timestamp every cycle so you can monitor
# ══════════════════════════════════════════════════════════════════
HEARTBEAT_FILE = os.path.join(CACHE_DIR, "worker_heartbeat.json")

def write_heartbeat(status="running", last_fetch=None):
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            json.dump({
                "status":     status,
                "timestamp":  _now_ist().isoformat(),
                "last_fetch": last_fetch or _now_ist().isoformat(),
                "pid":        os.getpid(),
            }, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info("  Panchak Background Worker starting...")
    log.info(f"  PID: {os.getpid()}")
    log.info(f"  CACHE_DIR: {CACHE_DIR}")
    log.info("=" * 60)

    _load_dedup()

    # Init Kite
    try:
        kite = init_kite()
    except Exception as e:
        log.error(f"❌ Kite init failed: {e}")
        log.error("Make sure access_token.txt exists and is valid.")
        sys.exit(1)

    last_live_fetch  = 0
    last_oi_fetch    = 0
    last_ema7_build  = 0
    last_astro_alert = ""
    last_fetch_time  = None

    write_heartbeat("started")
    log.info("✅ Worker ready. Entering main loop...")

    while True:
        try:
            now       = _now_ist()
            now_ts    = time.time()
            now_hhmm  = now.strftime("%H%M")
            today_str = now.date().strftime("%Y%m%d")

            # ── Daily astro alert at 9:00 AM ──────────────────────
            if now_hhmm == "0900" and last_astro_alert != today_str:
                send_daily_astro_alert()
                last_astro_alert = today_str

            if is_market_hours():
                # ── Live data + alerts every INTERVAL_LIVE seconds ──
                if now_ts - last_live_fetch >= INTERVAL_LIVE:
                    log.info(f"🔄 Fetching live data... ({now.strftime('%H:%M:%S')})")
                    df_live, df_oi = fetch_and_cache_live(kite)
                    if df_live is not None and not df_live.empty:
                        check_panchak_alerts(df_live)
                        check_breakout_alerts(kite, df_live)
                        last_fetch_time = now.isoformat()
                    last_live_fetch = now_ts

                # ── OI intelligence every INTERVAL_OI seconds ───────
                if now_ts - last_oi_fetch >= INTERVAL_OI:
                    log.info(f"📊 Fetching OI intelligence...")
                    fetch_and_cache_oi(kite)
                    last_oi_fetch = now_ts

                # ── EMA7 rebuild every INTERVAL_EMA7 seconds ────────
                if now_ts - last_ema7_build >= INTERVAL_EMA7:
                    log.info(f"📈 Rebuilding EMA7...")
                    rebuild_ema7(kite)
                    last_ema7_build = now_ts

                write_heartbeat("market_open", last_fetch_time)
                time.sleep(10)   # tight loop during market hours

            else:
                # Off market — sleep longer, just keep heartbeat alive
                status = "pre_market" if now_hhmm < "0915" else "post_market"
                write_heartbeat(status, last_fetch_time)
                log.debug(f"💤 Market closed ({now.strftime('%H:%M IST')}) — sleeping {INTERVAL_OFF}s")
                time.sleep(INTERVAL_OFF)

        except KeyboardInterrupt:
            log.info("🛑 Worker stopped by user (Ctrl+C)")
            write_heartbeat("stopped")
            break
        except Exception as e:
            log.error(f"❌ Loop error: {e}")
            log.error(traceback.format_exc())
            write_heartbeat("error")
            time.sleep(30)   # wait 30s before retry on error

if __name__ == "__main__":
    main()
