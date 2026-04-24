"""
alert_engine.py — Standalone Alert Engine for AWS Linux
=========================================================
Runs 24/7 on your AWS server alongside web_dashboard.py.
Fires Telegram alerts for ALL signal types — no desktop app needed.

What it does every 60 seconds (during market hours):
  1. Fetches live NIFTY Options Chain → OI direction + call/put wall alerts
  2. Scans all stocks → Panchak TOP_HIGH / TOP_LOW breaks
  3. Scans all stocks → OHL setups (O=Low bullish, O=High bearish)
  4. Scans OHLC DB  → BOS / CHoCH 1H breakouts + OB retest
  5. Scans OHLC DB  → Hourly high/low breaks with EMA confirmation
  6. Runs KP Panchang → astro signal summary at 9:15 AM

What it does every hour:
  7. Updates ohlc_1h.db with fresh 1H candles from Kite

Install once:
    pip install kiteconnect pandas pytz

Run in background (keeps running after SSH disconnect):
    screen -S alerts
    python3 alert_engine.py
    Ctrl+A  then  D   (detach)

Reattach:
    screen -r alerts

Or with nohup:
    nohup python3 alert_engine.py > alerts.log 2>&1 &

Kite token refresh (run this daily before 9 AM):
    echo "YOUR_NEW_TOKEN" > access_token.txt
"""

import os, sys, json, time, traceback
from datetime import datetime, date, timedelta

# ── SMC ENGINE ──────────────────────────────────────────────────────────────
try:
    from smc_engine import detect_market_structure, find_order_blocks, find_fvg
    SMC_OK = True
except ImportError:
    SMC_OK = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these to match your setup
# ─────────────────────────────────────────────────────────────────────────────

API_KEY       = "7am67kxijfsusk9i"
TG_BOT_TOKEN  = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID    = "-1003706739531"

# ── CACHE folder — shared with old Panchak dashboard ────────────────────────
_AE_BASE  = os.path.dirname(os.path.abspath(__file__))
_AE_CACHE = os.path.join(_AE_BASE, "CACHE")
os.makedirs(_AE_CACHE, exist_ok=True)

def _acp(f): return os.path.join(_AE_CACHE, f)

# Shared files (no _qt suffix — same file read by all dashboards)
PANCHAK_DATA_FILE = _acp("panchak_data.csv")

# Qt/alert-engine specific files
TG_DEDUP_FILE_AE  = _acp("tg_dedup_ae.json")    # alert engine dedup (separate from Qt)
ALERT_LOG_FILE_AE = _acp("alert_log_ae.json")    # alert engine history

# Access token — check CACHE/ first, then base dir
ACCESS_TOKEN_FILE = (
    _acp("access_token.txt")
    if os.path.exists(_acp("access_token.txt"))
    else os.path.join(_AE_BASE, "access_token.txt")
)
SCAN_INTERVAL     = 60        # seconds between each full scan cycle
OHLC_UPDATE_MINS  = 60        # minutes between OHLC DB updates
MARKET_OPEN_H     = 9
MARKET_OPEN_M     = 15
MARKET_CLOSE_H    = 15
MARKET_CLOSE_M    = 30

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS",
    "ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV",
    "BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
    "BHEL","BHARATFORG","BHARTIARTL","BIOCON","BLUESTARCO","BOSCHLTD","BPCL",
    "BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA",
    "COALINDIA","COFORGE","COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR",
    "DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK",
    "GODREJCP","GODREJPROP","GRASIM","HAL","HAVELLS","HCLTECH","HDFCAMC",
    "HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDCOPPER","HINDPETRO",
    "HINDUNILVR","HINDZINC","HUDCO","ICICIBANK","ICICIGI","ICICIPRULI","IEX",
    "INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND",
    "IRCTC","IRFC","IREDA","ITC","JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL",
    "JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M",
    "MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL","MPHASIS",
    "MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NTPC","NUVAMA","NYKAA",
    "NATIONALUM","OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM",
    "PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD","PIDILITIND","PIIND","PNB",
    "PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PRESTIGE",
    "RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SBICARD","SBILIFE","SBIN",
    "SHREECEM","SHRIRAMFIN","SIEMENS","SOLARINDS","SRF","SUNPHARMA","SUPREMEIND",
    "SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER","TATATECH","TATASTEEL",
    "TCS","TECHM","TIINDIA","TITAN","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR",
    "ULTRACEMCO","UNIONBANK","UNITDSPR","UPL","VBL","VEDL","VOLTAS","WIPRO","ZYDUSLIFE",
]

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

try:
    from kiteconnect import KiteConnect
    import pandas as pd
except ImportError:
    print("❌  pip install kiteconnect pandas"); sys.exit(1)

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
except ImportError:
    IST = None

try:
    from ohlc_store import OHLCStore
    OHLC_OK = True
except ImportError:
    OHLC_OK = False
    print("⚠️  ohlc_store.py not found — BOS scanner disabled")

try:
    from bos_scanner import (
        run_bos_scan, scan_hourly_breaks,
        _todays_bos_setups, check_ob_retest,
    )
    BOS_OK = True
except ImportError:
    BOS_OK = False
    print("⚠️  bos_scanner.py not found — BOS scanner disabled")

try:
    from astro_logic import get_astro_score
    ASTRO_OK = True
except ImportError:
    ASTRO_OK = False

try:
    from astro_time import get_time_signal_detail
    TIME_OK = True
except ImportError:
    TIME_OK = False

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────

import urllib.request

# Per-day dedup: {key: date_str}
_tg_dedup: dict = {}

def _ts() -> str:
    if IST:
        return datetime.now(IST).strftime("%H:%M:%S IST")
    return datetime.now().strftime("%H:%M:%S")

def tg_send(msg: str, dedup_key: str = None) -> bool:
    """Send Telegram message. If dedup_key given, only sends once per day."""
    today = date.today().isoformat()
    if dedup_key:
        if _tg_dedup.get(dedup_key) == today:
            return False   # already sent today

    try:
        url  = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id":    TG_CHAT_ID,
            "text":       msg,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = json.loads(r.read()).get("ok", False)
        if ok and dedup_key:
            _tg_dedup[dedup_key] = today
        return ok
    except Exception as e:
        print(f"[TG] Error: {e}")
        return False

def tg_bg(msg: str, dedup_key: str = None):
    """Non-blocking Telegram send."""
    import threading
    threading.Thread(target=tg_send, args=(msg, dedup_key), daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# KITE CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

_kite: KiteConnect = None

def init_kite() -> bool:
    global _kite
    try:
        k = KiteConnect(api_key=API_KEY)
        with open(ACCESS_TOKEN_FILE, encoding="utf-8") as f:
            k.set_access_token(f.read().strip())
        k.quote(["NSE:NIFTY 50"])   # validate
        _kite = k
        print(f"[{_ts()}] ✅  Kite connected")
        return True
    except FileNotFoundError:
        print(f"[{_ts()}] ❌  access_token.txt not found")
    except Exception as e:
        print(f"[{_ts()}] ❌  Kite error: {e}")
    _kite = None
    return False

# ─────────────────────────────────────────────────────────────────────────────
# MARKET HOURS
# ─────────────────────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    now = datetime.now(IST) if IST else datetime.now()
    if now.weekday() >= 5:   # Saturday / Sunday
        return False
    t = now.hour * 60 + now.minute
    open_t  = MARKET_OPEN_H  * 60 + MARKET_OPEN_M
    close_t = MARKET_CLOSE_H * 60 + MARKET_CLOSE_M
    return open_t <= t <= close_t

# ─────────────────────────────────────────────────────────────────────────────
# OHLC DB UPDATER
# ─────────────────────────────────────────────────────────────────────────────

_last_ohlc_update: datetime = None

def _get_token(sym: str, nse_df) -> int:
    """Get instrument token for a symbol from NSE instruments dataframe."""
    row = nse_df[nse_df["tradingsymbol"] == sym]
    return int(row.iloc[0]["instrument_token"]) if not row.empty else None

def update_ohlc_db():
    """Update ohlc_1h.db with latest 1H candles from Kite."""
    global _last_ohlc_update
    if not OHLC_OK or not _kite:
        return
    try:
        print(f"[{_ts()}] 📦  Updating OHLC DB…")
        nse_df = pd.DataFrame(_kite.instruments("NSE"))
        db     = OHLCStore()
        db.update_all(
            kite         = _kite,
            symbols      = STOCKS,
            get_token_fn = lambda s: _get_token(s, nse_df),
            batch_size   = 10,
            delay_secs   = 0.3,
            log_fn       = None,
        )
        _last_ohlc_update = datetime.now()
        print(f"[{_ts()}] ✅  OHLC DB updated")
    except Exception as e:
        print(f"[{_ts()}] ⚠️  OHLC update error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# SCAN 1 — PANCHAK TOP_HIGH / TOP_LOW BREAKS + OHL
# ─────────────────────────────────────────────────────────────────────────────

def scan_panchak():
    """Scan all stocks vs panchak levels. Fire alerts for breaks and OHL."""
    if not _kite:
        return
    try:
        # Batch quotes
        quotes = {}
        for i in range(0, len(STOCKS), 400):
            try:
                quotes.update(_kite.quote([f"NSE:{s}" for s in STOCKS[i:i+400]]))
            except Exception: pass

        # Load panchak CSV
        panchak = {}
        if os.path.exists(PANCHAK_DATA_FILE):
            pdf = pd.read_csv(PANCHAK_DATA_FILE)
            for _, r in pdf.iterrows():
                panchak[str(r["Symbol"])] = {
                    "TOP_HIGH": float(r.get("TOP_HIGH", 0) or 0),
                    "TOP_LOW":  float(r.get("TOP_LOW",  0) or 0),
                    "DIFF":     float(r.get("DIFF",     0) or 0),
                }

        th_breaks = []; tl_breaks = []
        ohl_bull  = []; ohl_bear  = []
        near_list = []

        for sym in STOCKS:
            q = quotes.get(f"NSE:{sym}")
            if not q: continue
            ltp  = q.get("last_price", 0)
            pc   = q["ohlc"]["close"]
            lo   = q["ohlc"]["open"]
            lh   = q["ohlc"]["high"]
            ll   = q["ohlc"]["low"]
            chgp = round((ltp - pc) / pc * 100, 2) if pc else 0

            pan = panchak.get(sym, {})
            th  = pan.get("TOP_HIGH", 0)
            tl  = pan.get("TOP_LOW",  0)
            diff= pan.get("DIFF",     0)

            # TOP_HIGH break
            if th > 0 and ltp >= th:
                th_breaks.append((sym, ltp, th, chgp))

            # TOP_LOW break
            if tl > 0 and ltp <= tl:
                tl_breaks.append((sym, ltp, tl, chgp))

            # Near levels (within 0.5% or 0.5×DIFF)
            if th > 0 and tl > 0:
                tol = max(diff * 0.5, ltp * 0.005)
                if abs(ltp - th) <= tol and ltp < th:
                    near_list.append((sym, ltp, th, "↑ near TOP_HIGH"))
                elif abs(ltp - tl) <= tol and ltp > tl:
                    near_list.append((sym, ltp, tl, "↓ near TOP_LOW"))

            # OHL — O=L bullish
            tol_pct = 0.001
            if lo > 0 and abs(ll - lo) / lo <= tol_pct:
                ohl_bull.append((sym, ltp, lo, chgp))

            # OHL — O=H bearish
            if lo > 0 and abs(lh - lo) / lo <= tol_pct and sym not in [r[0] for r in ohl_bull]:
                ohl_bear.append((sym, ltp, lo, chgp))

        # ── Fire TOP_HIGH alert ─────────────────────────────────────────
        if th_breaks:
            syms_str = ", ".join(f"<b>{s}</b> {l:.1f}" for s, l, _, _ in th_breaks[:8])
            msg = (
                f"🟢 <b>TOP_HIGH Breaks</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{syms_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>LTP ≥ Panchak TOP_HIGH — bullish breakout</i>\n"
                f"⚠️ Not financial advice."
            )
            tg_bg(msg, dedup_key=f"TH_{date.today().isoformat()}")
            print(f"[{_ts()}] 🟢  TOP_HIGH breaks: {len(th_breaks)}")

        # ── Fire TOP_LOW alert ──────────────────────────────────────────
        if tl_breaks:
            syms_str = ", ".join(f"<b>{s}</b> {l:.1f}" for s, l, _, _ in tl_breaks[:8])
            msg = (
                f"🔴 <b>TOP_LOW Breaks</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{syms_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>LTP ≤ Panchak TOP_LOW — bearish breakdown</i>\n"
                f"⚠️ Not financial advice."
            )
            tg_bg(msg, dedup_key=f"TL_{date.today().isoformat()}")
            print(f"[{_ts()}] 🔴  TOP_LOW breaks: {len(tl_breaks)}")

        # ── OHL Bull (once per day) ─────────────────────────────────────
        if ohl_bull:
            syms_str = ", ".join(
                f"<b>{s}</b> {l:.1f} ({c:+.1f}%)" for s, l, _, c in ohl_bull[:8])
            msg = (
                f"🟢 <b>OHL — Open = Low (Bullish)</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{syms_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Day opened at low = strong bullish bias</i>\n"
                f"⚠️ Not financial advice."
            )
            tg_bg(msg, dedup_key=f"OHL_BULL_{date.today().isoformat()}")

        # ── OHL Bear (once per day) ─────────────────────────────────────
        if ohl_bear:
            syms_str = ", ".join(
                f"<b>{s}</b> {l:.1f} ({c:+.1f}%)" for s, l, _, c in ohl_bear[:8])
            msg = (
                f"🔴 <b>OHL — Open = High (Bearish)</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{syms_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Day opened at high = strong bearish bias</i>\n"
                f"⚠️ Not financial advice."
            )
            tg_bg(msg, dedup_key=f"OHL_BEAR_{date.today().isoformat()}")

        # ── Near levels (once per day per symbol) ──────────────────────
        for sym, ltp, level, label in near_list:
            key = f"NEAR_{sym}_{round(level/5)*5}_{date.today().isoformat()}"
            msg = (
                f"⚠️ <b>Near Level — {sym}</b>  ⏰ {_ts()}\n"
                f"LTP: <b>{ltp:.1f}</b>  {label}  Level: <b>{level:.1f}</b>\n"
                f"<i>Approaching key panchak level</i>"
            )
            tg_bg(msg, dedup_key=key)

        # ── Yesterday + Weekly/Monthly breaks (uses same quotes batch) ──
        scan_yest_breaks(quotes)
        scan_weekly_monthly_breaks(quotes)

    except Exception as e:
        print(f"[{_ts()}] ⚠️  Panchak scan error: {e}")
        traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 2 — OI DIRECTION + CALL WALL / PUT FLOOR
# ─────────────────────────────────────────────────────────────────────────────

_last_oi_direction = ""

def scan_oi():
    """Check OI direction shift — alert when it changes."""
    global _last_oi_direction
    if not _kite:
        return
    try:
        spot = _kite.quote(["NSE:NIFTY 50"])["NSE:NIFTY 50"]["last_price"]
        nfo  = pd.DataFrame(_kite.instruments("NFO"))
        step = 50; atm = int(round(spot / step) * step)
        strikes = [atm + i * step for i in range(-5, 6)]

        opts = nfo[(nfo["name"] == "NIFTY") &
                   (nfo["instrument_type"].isin(["CE", "PE"]))].copy()
        opts["expiry_dt"] = pd.to_datetime(opts["expiry"])
        upcoming = opts[opts["expiry_dt"] >= pd.Timestamp.now()]["expiry_dt"].unique()
        if not len(upcoming): return
        expiry = sorted(upcoming)[0]

        tok_map = {}
        for s in strikes:
            for t in ["CE","PE"]:
                row = opts[(opts["strike"]==s) &
                           (opts["instrument_type"]==t) &
                           (opts["expiry_dt"]==expiry)]
                if not row.empty:
                    tok_map[f"NFO:{row.iloc[0]['tradingsymbol']}"] = {"strike": s, "type": t}

        raw   = _kite.quote(list(tok_map.keys()))
        ce_oi = {}; pe_oi = {}

        for sym, meta in tok_map.items():
            q = raw.get(sym)
            if not q: continue
            s = meta["strike"]; t = meta["type"]
            oi = q.get("oi", 0) or 0
            if t == "CE": ce_oi[s] = oi
            else:         pe_oi[s] = oi

        tot_ce = sum(ce_oi.values()) or 1
        tot_pe = sum(pe_oi.values()) or 1
        pcr    = round(tot_pe / tot_ce, 2)

        # Direction
        if   pcr >= 1.3: direction = "🟢 BULLISH"
        elif pcr <= 0.7: direction = "🔴 BEARISH"
        else:            direction = "⚠️ SIDEWAYS"

        # Call wall / Put floor
        cw_list = [s for s in ce_oi if s > spot]
        pf_list = [s for s in pe_oi if s < spot]
        cw = min(cw_list, key=lambda x: x - spot) if cw_list else None
        pf = max(pf_list, key=lambda x: spot - x) if pf_list else None

        # Only alert when direction changes
        if direction != _last_oi_direction:
            _last_oi_direction = direction
            msg = (
                f"📊 <b>OI Direction Changed</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"Direction: <b>{direction}</b>\n"
                f"NIFTY Spot: <b>{spot:,.1f}</b>  |  PCR: <b>{pcr}</b>\n"
                f"Call Wall: <b>{cw if cw else '—'}</b>  |  "
                f"Put Floor: <b>{pf if pf else '—'}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ Not financial advice."
            )
            tg_bg(msg)   # no dedup — fires on every direction change
            print(f"[{_ts()}] 📊  OI direction → {direction}  PCR={pcr}")

    except Exception as e:
        print(f"[{_ts()}] ⚠️  OI scan error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 3 — BOS / CHoCH + HOURLY BREAKS (uses bos_scanner.py)
# ─────────────────────────────────────────────────────────────────────────────

def scan_bos():
    """Run BOS scanner — alerts fired inside bos_scanner.run_bos_scan()."""
    if not BOS_OK or not OHLC_OK or not _kite:
        return
    try:
        db      = OHLCStore()
        symbols = db.get_all_symbols()
        if not symbols:
            return

        # Build live LTP dict
        ltp_dict = {}
        try:
            quotes = {}
            for i in range(0, len(symbols), 400):
                try:
                    quotes.update(_kite.quote([f"NSE:{s}" for s in symbols[i:i+400]]))
                except Exception: pass
            for sym in symbols:
                q = quotes.get(f"NSE:{sym}")
                if q: ltp_dict[sym] = q.get("last_price", 0)
        except Exception: pass

        bos_events, hourly_events = run_bos_scan(
            db            = db,
            symbols       = symbols,
            send_telegram_fn = tg_bg,
            ltp_dict      = ltp_dict,
            tg_enabled    = True,
            kite          = _kite,
        )

        if bos_events:
            print(f"[{_ts()}] 📐  BOS: {len(bos_events)} events | Hourly: {len(hourly_events)}")
        if hourly_events:
            print(f"[{_ts()}] ⚡  Hourly breaks: {len(hourly_events)}")

    except Exception as e:
        print(f"[{_ts()}] ⚠️  BOS scan error: {e}")
        traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 4 — ASTRO MORNING SUMMARY (fires once at 9:15)
# ─────────────────────────────────────────────────────────────────────────────

_astro_alerted_today = ""

def scan_astro_morning():
    """Send astro summary once at market open."""
    global _astro_alerted_today
    today = date.today().isoformat()
    if _astro_alerted_today == today:
        return

    now = datetime.now(IST) if IST else datetime.now()
    if not (9 <= now.hour <= 10 and now.minute >= 10):
        return   # only between 9:10 and 10:00

    _astro_alerted_today = today
    try:
        lines = ["🌙 <b>Morning Astro Summary</b>  " + _ts() + "\n━━━━━━━━━━━━━━━━━━━━━"]

        if ASTRO_OK:
            r = get_astro_score()
            score  = r.get("score", 0)
            signal = r.get("signal", "—")
            icon   = "🟢" if score > 0 else ("🔴" if score < 0 else "🟡")
            lines.append(f"{icon} Astro Signal: <b>{signal}</b>  (Score: {score:+d})")
            lines.append(f"🌙 Nakshatra: <b>{r.get('nakshatra','—')}</b>  |  KP Sub: <b>{r.get('sub_lord','—')}</b>")
            lines.append(f"📝 {r.get('reason','—')[:120]}")

        if TIME_OK:
            t = get_time_signal_detail()
            lines.append(f"\n⏰ Time Zone: <b>{t.get('signal','—')}</b>")
            lines.append(f"📋 {t.get('description','—')}")

        lines.append("\n━━━━━━━━━━━━━━━━━━━━━\n⚠️ Not financial advice.")
        tg_bg("\n".join(lines))
        print(f"[{_ts()}] 🌙  Morning astro summary sent")
    except Exception as e:
        print(f"[{_ts()}] ⚠️  Astro error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 5 — TOP MOVERS (fires once — when gainers/losers cross 2%)
# ─────────────────────────────────────────────────────────────────────────────

_movers_alerted: dict = {}   # {"TOP_GAINERS": date_str, "TOP_LOSERS": date_str}

def scan_movers():
    """Alert on top gainers/losers — once per day."""
    if not _kite: return
    today = date.today().isoformat()
    # Only run after 10:30 AM (enough time for real moves to establish)
    now = datetime.now(IST) if IST else datetime.now()
    if now.hour < 10 or (now.hour == 10 and now.minute < 30):
        return

    try:
        quotes = {}
        for i in range(0, len(STOCKS), 400):
            try: quotes.update(_kite.quote([f"NSE:{s}" for s in STOCKS[i:i+400]]))
            except Exception: pass

        rows = []
        for sym in STOCKS:
            q = quotes.get(f"NSE:{sym}")
            if not q: continue
            ltp  = q.get("last_price", 0)
            pc   = q["ohlc"]["close"]
            chgp = round((ltp - pc) / pc * 100, 2) if pc else 0
            rows.append({"s": sym, "l": ltp, "c": chgp})

        gainers = sorted([r for r in rows if r["c"] >  2.5], key=lambda x: -x["c"])[:10]
        losers  = sorted([r for r in rows if r["c"] < -2.5], key=lambda x:  x["c"])[:10]

        if gainers and _movers_alerted.get("TOP_GAINERS") != today:
            _movers_alerted["TOP_GAINERS"] = today
            lines = "\n".join(
                f"  • <b>{r['s']}</b>  {r['l']:.1f}  +{r['c']:.1f}%" for r in gainers)
            tg_bg(f"🔥 <b>Top Gainers >2.5%</b>  ⏰ {_ts()}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━\n{lines}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━\n⚠️ Not financial advice.")
            print(f"[{_ts()}] 🔥  Top gainers alert: {len(gainers)}")

        if losers and _movers_alerted.get("TOP_LOSERS") != today:
            _movers_alerted["TOP_LOSERS"] = today
            lines = "\n".join(
                f"  • <b>{r['s']}</b>  {r['l']:.1f}  {r['c']:.1f}%" for r in losers)
            tg_bg(f"💥 <b>Top Losers <-2.5%</b>  ⏰ {_ts()}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━\n{lines}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━\n⚠️ Not financial advice.")
            print(f"[{_ts()}] 💥  Top losers alert: {len(losers)}")

    except Exception as e:
        print(f"[{_ts()}] ⚠️  Movers error: {e}")



# ─────────────────────────────────────────────────────────────────────────────
# SCAN 6 — YESTERDAY HIGH/LOW BREAK (YEST_GREEN_BREAK / YEST_RED_BREAK)
# ─────────────────────────────────────────────────────────────────────────────

_yest_ohlc_cache: dict = {}   # {symbol: {high, low, close}}
_yest_cache_date: str = ""

def _load_yest_ohlc():
    """Load yesterday's OHLC for all stocks once per day."""
    global _yest_ohlc_cache, _yest_cache_date
    today = date.today().isoformat()
    if _yest_cache_date == today and _yest_ohlc_cache:
        return   # already loaded today

    if not _kite:
        return
    try:
        nse_df   = pd.DataFrame(_kite.instruments("NSE"))
        from_d   = date.today() - timedelta(days=5)
        to_d     = date.today() - timedelta(days=1)
        loaded   = 0
        for sym in STOCKS:
            try:
                row = nse_df[nse_df["tradingsymbol"] == sym]
                if row.empty: continue
                token = int(row.iloc[0]["instrument_token"])
                bars  = _kite.historical_data(token, from_d, to_d, "day")
                if bars:
                    last = bars[-1]
                    _yest_ohlc_cache[sym] = {
                        "high":  last["high"],
                        "low":   last["low"],
                        "close": last["close"],
                        "open":  last["open"],
                    }
                    loaded += 1
            except Exception:
                continue
        _yest_cache_date = today
        print(f"[{_ts()}] 📅  Yesterday OHLC loaded for {loaded} symbols")
    except Exception as e:
        print(f"[{_ts()}] ⚠️  Yest OHLC load error: {e}")


def scan_yest_breaks(quotes: dict):
    """
    YEST_GREEN_BREAK: LTP > yesterday's HIGH → bullish breakout
    YEST_RED_BREAK:   LTP < yesterday's LOW  → bearish breakdown
    Fires once per symbol per day.
    """
    if not _yest_ohlc_cache:
        return

    today = date.today().isoformat()
    green_breaks = []; red_breaks = []

    for sym in STOCKS:
        q = quotes.get(f"NSE:{sym}")
        if not q: continue
        ltp  = q.get("last_price", 0)
        yest = _yest_ohlc_cache.get(sym)
        if not yest: continue

        yh = yest["high"]; yl = yest["low"]

        if ltp > yh and yh > 0:
            key = f"YEST_GREEN_{sym}_{today}"
            if _tg_dedup.get(key) != today:
                green_breaks.append((sym, ltp, yh))
                _tg_dedup[key] = today

        elif ltp < yl and yl > 0:
            key = f"YEST_RED_{sym}_{today}"
            if _tg_dedup.get(key) != today:
                red_breaks.append((sym, ltp, yl))
                _tg_dedup[key] = today

    if green_breaks:
        lines = "\n".join(f"  • <b>{s}</b>  LTP {l:.1f}  YestH <b>{yh:.1f}</b>"
                           for s, l, yh in green_breaks[:8])
        tg_bg(
            f"🟢 <b>Yesterday High Break</b>  ⏰ {_ts()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{lines}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>LTP crossed above yesterday's high — momentum signal</i>\n"
            f"⚠️ Not financial advice."
        )
        print(f"[{_ts()}] 🟢  Yest High breaks: {len(green_breaks)}")

    if red_breaks:
        lines = "\n".join(f"  • <b>{s}</b>  LTP {l:.1f}  YestL <b>{yl:.1f}</b>"
                           for s, l, yl in red_breaks[:8])
        tg_bg(
            f"🔴 <b>Yesterday Low Break</b>  ⏰ {_ts()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{lines}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>LTP crossed below yesterday's low — breakdown signal</i>\n"
            f"⚠️ Not financial advice."
        )
        print(f"[{_ts()}] 🔴  Yest Low breaks: {len(red_breaks)}")


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 7 — WEEKLY + MONTHLY HIGH/LOW BREAKS
# ─────────────────────────────────────────────────────────────────────────────

_weekly_monthly_cache: dict = {}   # {sym: {week_high, week_low, month_high, month_low}}
_wm_cache_date: str = ""

def _load_weekly_monthly():
    """Load weekly and monthly OHLC once per day (at 9:15 AM)."""
    global _weekly_monthly_cache, _wm_cache_date
    today = date.today().isoformat()
    if _wm_cache_date == today and _weekly_monthly_cache:
        return

    if not _kite:
        return
    try:
        nse_df = pd.DataFrame(_kite.instruments("NSE"))
        # Week = last 7 days, Month = last 30 days
        from_w = date.today() - timedelta(days=7)
        from_m = date.today() - timedelta(days=30)
        to_d   = date.today() - timedelta(days=1)
        loaded = 0
        for sym in STOCKS:
            try:
                row = nse_df[nse_df["tradingsymbol"] == sym]
                if row.empty: continue
                token = int(row.iloc[0]["instrument_token"])
                week_bars  = _kite.historical_data(token, from_w, to_d, "day")
                month_bars = _kite.historical_data(token, from_m, to_d, "day")
                if week_bars and month_bars:
                    _weekly_monthly_cache[sym] = {
                        "week_high":  max(b["high"]  for b in week_bars),
                        "week_low":   min(b["low"]   for b in week_bars),
                        "month_high": max(b["high"]  for b in month_bars),
                        "month_low":  min(b["low"]   for b in month_bars),
                    }
                    loaded += 1
            except Exception:
                continue
        _wm_cache_date = today
        print(f"[{_ts()}] 📅  Weekly/Monthly OHLC loaded for {loaded} symbols")
    except Exception as e:
        print(f"[{_ts()}] ⚠️  Weekly/Monthly OHLC error: {e}")


def scan_weekly_monthly_breaks(quotes: dict):
    """Fire alerts when LTP breaks weekly or monthly high/low."""
    if not _weekly_monthly_cache: return
    today = date.today().isoformat()

    wk_up = []; wk_dn = []; mo_up = []; mo_dn = []

    for sym in STOCKS:
        q = quotes.get(f"NSE:{sym}")
        if not q: continue
        ltp = q.get("last_price", 0)
        wm  = _weekly_monthly_cache.get(sym)
        if not wm: continue

        if ltp > wm["week_high"] and _tg_dedup.get(f"WK_H_{sym}_{today}") != today:
            wk_up.append((sym, ltp, wm["week_high"]))
            _tg_dedup[f"WK_H_{sym}_{today}"] = today

        if ltp < wm["week_low"] and _tg_dedup.get(f"WK_L_{sym}_{today}") != today:
            wk_dn.append((sym, ltp, wm["week_low"]))
            _tg_dedup[f"WK_L_{sym}_{today}"] = today

        if ltp > wm["month_high"] and _tg_dedup.get(f"MO_H_{sym}_{today}") != today:
            mo_up.append((sym, ltp, wm["month_high"]))
            _tg_dedup[f"MO_H_{sym}_{today}"] = today

        if ltp < wm["month_low"] and _tg_dedup.get(f"MO_L_{sym}_{today}") != today:
            mo_dn.append((sym, ltp, wm["month_low"]))
            _tg_dedup[f"MO_L_{sym}_{today}"] = today

    for label, icon, items in [
        ("Weekly High Break",   "📈", wk_up),
        ("Weekly Low Break",    "📉", wk_dn),
        ("Monthly High Break",  "🚀", mo_up),
        ("Monthly Low Break",   "💣", mo_dn),
    ]:
        if items:
            lines = "\n".join(f"  • <b>{s}</b>  LTP {l:.1f}  Level <b>{lvl:.1f}</b>"
                               for s, l, lvl in items[:8])
            tg_bg(
                f"{icon} <b>{label}</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{lines}\n"
                f"⚠️ Not financial advice."
            )
            print(f"[{_ts()}] {icon}  {label}: {len(items)}")


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 8 — OI LONG UNWIND / PUT CRUMBLE (advanced OI patterns)
# ─────────────────────────────────────────────────────────────────────────────

def scan_oi_patterns(chain_data: dict):
    """
    LONG_UNWIND: CE OI rising sharply + PE OI falling → bears covering but calls piling = very bearish
    PUT_CRUMBLE: PE OI collapsing fast → put writers exiting = bearish (no support below)
    Uses raw chain data passed from scan_oi()
    """
    if not chain_data:
        return
    today = date.today().isoformat()
    spot  = chain_data.get("spot", 0)
    atm   = chain_data.get("atm", 0)

    ce_add = chain_data.get("ce_add_pct", 0)   # CE OI buildup %
    pe_drp = chain_data.get("pe_drp_pct", 0)   # PE OI drop %
    pe_add = chain_data.get("pe_add_pct", 0)
    ce_drp = chain_data.get("ce_drp_pct", 0)

    # Long Unwind: CE building ≥10% AND PE dropping ≥5% simultaneously
    if ce_add >= 10 and pe_drp <= -5:
        key = f"LONG_UNWIND_{today}"
        if _tg_dedup.get(key) != today:
            _tg_dedup[key] = today
            tg_bg(
                f"⚡ <b>OI Long Unwind Signal</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"CE OI Building: <b>+{ce_add:.1f}%</b>\n"
                f"PE OI Dropping: <b>{pe_drp:.1f}%</b>\n"
                f"NIFTY Spot: <b>{spot:,.0f}</b>  ATM: <b>{atm}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Longs exiting + Call wall building = strong bearish</i>\n"
                f"⚠️ Not financial advice."
            )
            print(f"[{_ts()}] ⚡  Long Unwind: CE +{ce_add:.1f}%  PE {pe_drp:.1f}%")

    # Put Crumble: PE OI dropping ≥8% with no CE buildup → pure bearish
    if pe_drp <= -8 and ce_add < 3:
        key = f"PUT_CRUMBLE_{today}"
        if _tg_dedup.get(key) != today:
            _tg_dedup[key] = today
            tg_bg(
                f"💥 <b>OI Put Crumble Signal</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"PE OI Collapsing: <b>{pe_drp:.1f}%</b>\n"
                f"NIFTY Spot: <b>{spot:,.0f}</b>  ATM: <b>{atm}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Put support crumbling = strong bearish. Watch for fast fall.</i>\n"
                f"⚠️ Not financial advice."
            )
            print(f"[{_ts()}] 💥  Put Crumble: PE {pe_drp:.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 9 — THREE CONSECUTIVE GREEN 15M CANDLES
# ─────────────────────────────────────────────────────────────────────────────

def scan_three_green_15m():
    """
    Fires when a stock has 3+ consecutive green 15M candles.
    Uses Kite historical_data for last few 15M candles.
    Only runs on a sample of high-volume stocks (too expensive for all 170).
    """
    if not _kite: return
    today = date.today().isoformat()

    # Only scan top 40 most-watched stocks to keep API calls manageable
    WATCHLIST = [
        "RELIANCE","TCS","HDFCBANK","ICICIBANK","INFY","SBIN","AXISBANK","BAJFINANCE",
        "KOTAKBANK","LT","HINDUNILVR","MARUTI","TITAN","BAJAJ-AUTO","M&M",
        "SUNPHARMA","WIPRO","HCLTECH","NTPC","POWERGRID","COALINDIA","ONGC",
        "ADANIPORTS","BPCL","INDUSINDBK","TATAPOWER","TATASTEEL","JSWSTEEL",
        "HINDALCO","BHARTIARTL","NAUKRI","DMART","EICHERMOT","DIVISLAB",
        "CIPLA","DRREDDY","GRASIM","ULTRACEMCO","HAL","BEL",
    ]

    try:
        nse_df = pd.DataFrame(_kite.instruments("NSE"))
        hits   = []

        for sym in WATCHLIST:
            try:
                key = f"THREE_GREEN_{sym}_{today}"
                if _tg_dedup.get(key) == today:
                    continue   # already alerted today

                row = nse_df[nse_df["tradingsymbol"] == sym]
                if row.empty: continue
                token = int(row.iloc[0]["instrument_token"])

                from_d = date.today() - timedelta(days=1)
                bars   = _kite.historical_data(token, from_d, date.today(), "15minute")
                if len(bars) < 3: continue

                last3 = bars[-3:]
                if all(b["close"] > b["open"] for b in last3):
                    ltp = bars[-1]["close"]
                    hits.append((sym, ltp))
                    _tg_dedup[key] = today

            except Exception:
                continue

        if hits:
            lines = "\n".join(f"  • <b>{s}</b>  {l:.1f}" for s, l in hits)
            tg_bg(
                f"🟢 <b>3 Consecutive Green 15M Candles</b>  ⏰ {_ts()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{lines}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Strong momentum — 3 green candles in a row on 15M</i>\n"
                f"⚠️ Not financial advice."
            )
            print(f"[{_ts()}] 🟢  Three-green 15M: {len(hits)}")

    except Exception as e:
        print(f"[{_ts()}] ⚠️  Three-green error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCAN 10 — ASTRO + KP TELEGRAM ALERT (once per day at open)
# ─────────────────────────────────────────────────────────────────────────────

_full_astro_alerted: str = ""

def scan_astro_full():
    """Send full astro + KP summary once at 9:15 AM."""
    global _full_astro_alerted
    today = date.today().isoformat()
    if _full_astro_alerted == today: return

    now = datetime.now(IST) if IST else datetime.now()
    if not (9 <= now.hour <= 9 and now.minute >= 14):
        return   # only at 9:14-9:30

    _full_astro_alerted = today
    lines = [f"🌙 <b>Daily Astro + KP Signal</b>  ⏰ {_ts()}",
             "━━━━━━━━━━━━━━━━━━━━━"]

    if ASTRO_OK:
        try:
            r      = get_astro_score()
            score  = r.get("score", 0)
            signal = r.get("signal", "—")
            nak    = r.get("nakshatra", "—")
            sub    = r.get("sub_lord", "—")
            reason = r.get("reason", "—")[:150]
            icon   = "🟢" if score > 0 else ("🔴" if score < 0 else "🟡")
            lines += [
                f"{icon} Signal: <b>{signal}</b>  Score: <b>{score:+d}</b>",
                f"🌙 Nakshatra: <b>{nak}</b>  |  KP Sub: <b>{sub}</b>",
                f"📝 {reason}",
            ]
        except Exception as e:
            lines.append(f"⚠️ Astro error: {e}")

    # KP panchang
    try:
        from astro_engine import get_all_planets, get_moon_data, get_dignities
        import pytz
        IST_tz = pytz.timezone("Asia/Kolkata")
        now_kp = datetime.now(IST_tz)
        moon = get_moon_data(now_kp)
        digs = get_dignities(now_kp)
        lines += [
            "━━━━━━━━━━━━━━━━━━━━━",
            f"🪐 Moon Sign: <b>{moon.get('sign','—')}</b>",
            f"🔮 Moon Nak: <b>{moon.get('nakshatra','—')}</b>",
        ]
        exalted = [p for p, d in digs.items() if d == "EXALTED"]
        debil   = [p for p, d in digs.items() if d == "DEBILITATED"]
        if exalted: lines.append(f"⬆️ Exalted: <b>{', '.join(exalted)}</b>")
        if debil:   lines.append(f"⬇️ Debilitated: <b>{', '.join(debil)}</b>")
    except Exception:
        pass

    lines += ["━━━━━━━━━━━━━━━━━━━━━", "⚠️ Not financial advice."]
    tg_bg("\n".join(lines))
    print(f"[{_ts()}] 🌙  Full astro alert sent")

# ─────────────────────────────────────────────────────────────────────────────
# SCAN 8 — SMC + PANCHAK COMBINED (HTF TREND + BREAKOUT)
# ─────────────────────────────────────────────────────────────────────────────

def scan_smc_panchak():
    if not SMC_OK or not _kite:
        return
    try:
        # Batch quotes
        quotes = {}
        for i in range(0, len(STOCKS), 400):
            try:
                quotes.update(_kite.quote([f"NSE:{s}" for s in STOCKS[i:i+400]]))
            except Exception: pass

        # Load panchak CSV
        if not os.path.exists(PANCHAK_DATA_FILE):
            return
            
        pdf = pd.read_csv(PANCHAK_DATA_FILE)
        
        # Only check a subset of symbols to avoid too many historical calls
        # We'll use the same watchlist as dashboard if possible
        WATCHLIST = {
            "NIFTY", "BANKNIFTY", "BEL", "HINDCOPPER", "INDHOTEL",
            "ABCAPITAL", "TATASTEEL", "BANKINDIA", "CANBK", "JINDALSTEL",
            "BANDHANBNK", "INDUSTOWER", "MOTHERSON", "NATIONALUM",
        }
        
        for _, row in pdf.iterrows():
            sym = str(row.get("Symbol", ""))
            if sym not in WATCHLIST:
                continue
                
            dedup_key = f"SMC_PANCHAK_COMBINED_{sym}_{date.today().isoformat()}"
            if _tg_dedup.get(dedup_key) == date.today().isoformat():
                continue

            q = quotes.get(f"NSE:{sym}")
            if not q: continue
            
            ltp = q.get("last_price", 0)
            th  = row.get("TOP_HIGH", 0)
            tl  = row.get("TOP_LOW",  0)
            
            if ltp <= 0 or th <= 0 or tl <= 0:
                continue
                
            is_bull_break = ltp >= th
            is_bear_break = ltp <= tl
            
            if not (is_bull_break or is_bear_break):
                continue

            # Fetch 1H candles for HTF analysis
            try:
                # Need instrument token
                # We can't use _get_token easily without pre-loading nse_df
                # Let's get it from the quote instrument_token
                tk = q.get("instrument_token")
                if not tk: continue
                
                end_dt   = datetime.now(IST) if IST else datetime.now()
                start_dt = end_dt - timedelta(days=10)
                bars = _kite.historical_data(tk, start_dt, end_dt, "60minute")
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

                    # Helper to get yesterday/weekly info (alert_engine has these in globals)
                    # Actually alert_engine doesn't store them in a way easily accessible here per symbol
                    # but we have the quote 'q' for OHLC
                    
                    msg = (
                        f"{side_icon} <b>SMC + PANCHAK COMBINED ALERT</b>\n"
                        f"⏰ {_ts()} | Stock: <b>{sym}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🔥 <b>{side_text} CONFLUENCE</b>\n"
                        f"SMC HTF Trend confirmed {side_text} and LTP crossed {'TOP_HIGH' if is_bull_break else 'TOP_LOW'}.\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🧠 <b>SMC HTF (1H) DETAILS:</b>\n"
                        + "\n".join(smc_details) + "\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 <b>PRICE DETAILS:</b>\n"
                        f"  LTP: <b>{ltp:,.2f}</b>\n"
                        f"  Panchak High: {th}\n"
                        f"  Panchak Low: {tl}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⚠️ <i>One alert per day per stock.</i>\n"
                        f"{side_icon*10}"
                    )
                    
                    tg_send(msg, dedup_key)
                    print(f"[{_ts()}] 🧠 SMC+Panchak Confluence: {sym} {side_text}")

            except Exception as ex:
                print(f"[{_ts()}] SMC HTF fetch error [{sym}]: {ex}")

    except Exception as e:
        print(f"[{_ts()}] ❌  scan_smc_panchak error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP HEALTH CHECK
# ─────────────────────────────────────────────────────────────────────────────

def send_startup_message():
    modules = []
    if BOS_OK:    modules.append("📐 BOS / CHoCH / OB Retest / Hourly Break")
    if OHLC_OK:   modules.append("📦 OHLC DB (1H candles)")
    if ASTRO_OK:  modules.append("🌙 Astro + KP Daily Alert")
    if TIME_OK:   modules.append("⏰ Time Signal")
    modules.append("📊 OI Direction + Long Unwind + Put Crumble")
    modules.append("📈 Panchak TOP_HIGH/LOW + Near Levels")
    modules.append("📅 Yesterday / Weekly / Monthly Breaks")
    modules.append("🟢 OHL Scanner (O=L / O=H)")
    modules.append("🟢 Three Green 15M Candles")
    modules.append("🔥 Top Gainers / Losers")

    msg = (
        f"✅ <b>Alert Engine Started</b>  ⏰ {_ts()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Active modules:\n" +
        "\n".join(f"  ✅ {m}" for m in modules) +
        f"\n━━━━━━━━━━━━━━━━━━━━━\n"
        f"Scan interval: every {SCAN_INTERVAL}s during market hours\n"
        f"OHLC update: every {OHLC_UPDATE_MINS} min\n"
        f"Alerts sent here: {TG_CHAT_ID}"
    )
    tg_send(msg)
    print(f"[{_ts()}] 📤  Startup message sent to Telegram")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Panchak Alert Engine")
    print(f"  Scan interval : {SCAN_INTERVAL}s")
    print(f"  OHLC update   : every {OHLC_UPDATE_MINS} min")
    print("=" * 60)

    # Connect Kite
    if not init_kite():
        print("Place access_token.txt in the same folder and restart.")
        sys.exit(1)

    send_startup_message()

    # Pre-load historical OHLC levels (yesterday + weekly + monthly)
    print(f"[{_ts()}] 📅  Loading historical OHLC levels…")
    _load_yest_ohlc()
    _load_weekly_monthly()

    # Initial OHLC update
    update_ohlc_db()
    _last_ohlc_update_time = datetime.now()

    cycle = 0
    while True:
        try:
            cycle += 1
            now = datetime.now(IST) if IST else datetime.now()

            if is_market_open():
                print(f"[{_ts()}] 🔄  Cycle {cycle} — market open")

                # ── Run all scans ──────────────────────────────────────
                scan_astro_full()      # full astro+KP once at 9:14 AM
                scan_astro_morning()   # brief summary once at 9:10–10:00
                scan_panchak()         # panchak + yest + weekly/monthly breaks
                scan_smc_panchak()     # SMC HTF Trend + Panchak Break
                scan_oi()              # OI direction + patterns
                scan_bos()             # BOS/CHoCH + OB retest + hourly break
                scan_movers()          # top gainers/losers once per day
                scan_three_green_15m() # 3 green 15M (watchlist)

                # ── OHLC DB update every N minutes ─────────────────────
                mins_since = (datetime.now() - _last_ohlc_update_time).seconds / 60
                if mins_since >= OHLC_UPDATE_MINS:
                    update_ohlc_db()
                    _last_ohlc_update_time = datetime.now()

            else:
                # Outside market hours — just update OHLC once per hour
                mins_since = (datetime.now() - _last_ohlc_update_time).seconds / 60
                if mins_since >= OHLC_UPDATE_MINS:
                    update_ohlc_db()
                    _last_ohlc_update_time = datetime.now()
                else:
                    print(f"[{_ts()}] 💤  Market closed — sleeping")

        except KeyboardInterrupt:
            print(f"\n[{_ts()}] Stopped by user.")
            tg_send(f"⚠️ Alert Engine stopped manually at {_ts()}")
            break

        except Exception as e:
            print(f"[{_ts()}] ❌  Main loop error: {e}")
            traceback.print_exc()

            # If Kite connection broke, try to reconnect
            if "Token" in str(e) or "session" in str(e).lower():
                print(f"[{_ts()}] 🔄  Trying Kite reconnect…")
                init_kite()

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
