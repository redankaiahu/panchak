# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – HOLIDAY AWARE)
# ==========================================================

import os
#import time
from datetime import datetime, timedelta, date, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz
from streamlit_autorefresh import st_autorefresh
from io import BytesIO
import smtplib
from email.message import EmailMessage
import numpy as np
import time as tm




EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO = ["uppala.wla@gmail.com"]
EMAIL_ENABLED = True
EMAIL_MAX_PER_DAY = 40        # safe under Gmail limit
EMAIL_COOLDOWN_MIN = 10       # minutes between emails

EMAIL_META_FILE = "CACHE/email_meta.json"
EMAIL_DEDUP_FILE = "CACHE/email_dedup.csv"
ALERTS_DEDUP_FILE = "CACHE/alerts_dedup.csv"





# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
st.markdown("""
<style>
/* Section headers */
.section-green {background:#e8f5e9;padding:8px;border-radius:6px;}
.section-red {background:#fdecea;padding:8px;border-radius:6px;}
.section-yelLIVE_LOW {background:#fff8e1;padding:8px;border-radius:6px;}
.section-blue {background:#e3f2fd;padding:8px;border-radius:6px;}
.section-purple {background:#f3e5f5;padding:8px;border-radius:6px;}
.section-orange {background:#fff3e0;padding:8px;border-radius:6px;}

/* Table tweaks */
thead tr th {
    background-color:#f5f7fa !important;
    font-weight:600 !important;
}
</style>
""", unsafe_allow_html=True)


IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

#OHLC_FILE = os.path.join(CACHE_DIR, "ohlc_60d.csv")
INST_FILE    = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE   = os.path.join(CACHE_DIR, "daily_ohlc.csv")
WEEKLY_FILE  = os.path.join(CACHE_DIR, "weekly_ohlc.csv")
MONTHLY_FILE = os.path.join(CACHE_DIR, "monthly_ohlc.csv")
#PANCHAK_FILE = os.path.join(CACHE_DIR, "panchak_static.csv")
#EMA_FILE = os.path.join(CACHE_DIR, "ema_20_50.csv")


API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"

# ================= NSE HOLIDAYS – 2026 =================
NSE_HOLIDAYS = {
    date(2026,1,15), date(2026,1,26), date(2026,3,3),
    date(2026,3,26), date(2026,3,31), date(2026,4,3),
    date(2026,4,14), date(2026,5,1), date(2026,5,28),
    date(2026,6,26), date(2026,9,14), date(2026,10,2),
    date(2026,10,20), date(2026,11,10), date(2026,11,24),
    date(2026,12,25)
}

# ================= SYMBOL MASTER =================
SYMBOL_META = {
    "NIFTY":     "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
}

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA",
    "BANKINDIA","BANKNIFTY","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
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


#SYMBOLS = ["NIFTY", "BANKNIFTY"] + STOCKS
INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "FINNIFTY",
    "NIFTYIT",
    "NIFTYFMCG",
    "NIFTYPHARMA",
    "NIFTYMETAL",
    "NIFTYAUTO",
    "NIFTYENERGY",
    "NIFTYPSUBANK",
]

SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS

SYMBOL_META.update({
    "FINNIFTY": "FINNIFTY",
    "NIFTYIT": "NIFTY IT",
    "NIFTYFMCG": "NIFTY FMCG",
    "NIFTYPHARMA": "NIFTY PHARMA",
    "NIFTYMETAL": "NIFTY METAL",
    "NIFTYAUTO": "NIFTY AUTO",
    "NIFTYENERGY": "NIFTY ENERGY",
    "NIFTYPSUBANK": "NIFTY PSU BANK",
})

# ================= PANCHAK STATIC DATES =================
PANCHAK_DATES = [
    date(2026,1,21),
    date(2026,1,22),
    date(2026,1,23),
    date(2026,1,24),
    date(2026,1,25),
]

# FEB 2026
#PANCHAK_DATES = [
 #   date(2026,2,17),
  #  date(2026,2,18),
   # date(2026,2,19),
    #date(2026,2,20),
    
#]

PANCHAK_START = date(2026, 1, 21)
PANCHAK_END   = date(2026, 1, 25)

PANCHAK_DATA_FILE = os.path.join(CACHE_DIR, "panchak_data.csv")
PANCHAK_META_FILE = os.path.join(CACHE_DIR, "panchak_meta.csv")
ALERTS_LOG_FILE = "CACHE/alerts_log.csv"



# ================= KITE INIT =================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(open(ACCESS_TOKEN_FILE).read().strip())




# ================= INSTRUMENTS =================
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def get_token(symbol):
    name = SYMBOL_META.get(symbol, symbol)
    row = inst[inst.tradingsymbol == name]
    return None if row.empty else int(row.iloc[0].instrument_token)

def kite_symbol(symbol):
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

# ================= TRADING DAY HELPERS =================
def last_trading_day(d):
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d

@st.cache_data(ttl=3600)
def fetch_yesterday_ohlc(token):
    d = last_trading_day(date.today() - timedelta(days=1))
    bars = kite.historical_data(token, d, d, "day")
    if not bars:
        return None, None
    b = bars[0]
    return round(b["high"],2), round(b["low"],2), round(b["close"],2)


# ================= OHLC BUILDERS (UNCHANGED) =================
# ==========================================================
# ✅ PREVIOUS PERIOD OHLC (DAILY / WEEKLY / MONTHLY)
# ==========================================================

def dated_file(name):
    d = last_trading_day(date.today())
    return os.path.join(CACHE_DIR, f"{name}_{d}.csv")


def previous_week_range():
    end = last_trading_day(date.today())
    last_week_end = end - timedelta(days=end.weekday() + 1)
    last_week_start = last_week_end - timedelta(days=4)
    return last_week_start, last_week_end


def previous_month_range():
    first_this_month = date.today().replace(day=1)
    last_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1)
    return first_prev_month, last_prev_month


def build_daily_ohlc():
    path = dated_file("daily_ohlc")
    if os.path.exists(path):
        return path

    d = last_trading_day(date.today() - timedelta(days=1))
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, d, d, "day")
        if not bars:
            continue

        b = bars[0]
        rows.append({
            "Symbol": s,
            "OPEN_D":  b["open"],
            "HIGH_D":  b["high"],
            "LOW_D":   b["low"],
            "CLOSE_D": b["close"],
            "VOLUME_D": b["volume"]
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def build_weekly_ohlc():
    path = dated_file("weekly_ohlc")
    if os.path.exists(path):
        return path

    start, end = previous_week_range()
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, start, end, "day")
        if not bars:
            continue

        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol":   s,
            "OPEN_W": dfb.iloc[0]["open"],
            "HIGH_W": dfb["high"].max(),
            "LOW_W": dfb["low"].min(),
            "CLOSE_W": dfb.iloc[-1]["close"],
            "VOLUME_W": dfb["volume"].sum()

        })

        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def build_monthly_ohlc():
    path = dated_file("monthly_ohlc")
    if os.path.exists(path):
        return path

    start, end = previous_month_range()
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, start, end, "day")
        if not bars:
            continue

        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol":   s,
            "OPEN_M":   dfb.iloc[0]["open"],
            "HIGH_M":   dfb["high"].max(),
            "LOW_M":    dfb["low"].min(),
            "CLOSE_M":  dfb.iloc[-1]["close"],
            "VOLUME_M": dfb["volume"].sum()
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
    return path


# -------- BUILD OR LOAD FILES (AUTO) --------
DAILY_FILE   = build_daily_ohlc()
WEEKLY_FILE  = build_weekly_ohlc()
MONTHLY_FILE = build_monthly_ohlc()


# ================= PANCHAK STATIC =================

# ================= PANCHAK STATIC (VALIDATED CACHE – OPTION A) =================

def is_panchak_cache_valid():
    """
    Cache is valid ONLY if Panchak dates in meta file
    exactly match script-defined PANCHAK_START / PANCHAK_END
    """
    if not os.path.exists(PANCHAK_META_FILE):
        return False

    try:
        meta = pd.read_csv(PANCHAK_META_FILE)

        file_start = pd.to_datetime(
            meta.loc[meta["key"] == "start_date", "value"].values[0]
        ).date()

        file_end = pd.to_datetime(
            meta.loc[meta["key"] == "end_date", "value"].values[0]
        ).date()

        return file_start == PANCHAK_START and file_end == PANCHAK_END

    except Exception:
        return False


def build_panchak_files():
    """
    Builds Panchak DATA file + META file
    Called ONLY when cache is invalid
    """
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(
            tk,
            PANCHAK_DATES[0],
            PANCHAK_DATES[-1],
            "day"
        )

        dfb = pd.DataFrame(bars)
        if dfb.empty:
            continue

        th = dfb["high"].max()
        tl = dfb["low"].min()
        diff = th - tl

        rows.append({
            "Symbol": s,
            "TOP_HIGH": round(th, 2),
            "TOP_LOW":  round(tl, 2),
            "DIFF":     round(diff, 2),
            "BT":       round(th + diff, 2),
            "ST":       round(tl - diff, 2),
        })

        tm.sleep(0.35)

    # 🔒 Safety: don’t write empty data
    if not rows:
        return

    # --- DATA FILE ---
    pd.DataFrame(rows).to_csv(PANCHAK_DATA_FILE, index=False)

    # --- META FILE ---
    meta_df = pd.DataFrame([
        {"key": "start_date", "value": PANCHAK_START.isoformat()},
        {"key": "end_date",   "value": PANCHAK_END.isoformat()},
    ])

    meta_df.to_csv(PANCHAK_META_FILE, index=False)


# ---------- PANCHAK LOAD (AUTO-VALIDATED) ----------
if not is_panchak_cache_valid():
    build_panchak_files()

panchak_df = pd.read_csv(PANCHAK_DATA_FILE)

panchak_df = (
    panchak_df
    .sort_values("Symbol")
    .drop_duplicates(subset=["Symbol"], keep="last")
)





# ================= LIVE DATA =================
# ================= LIVE DATA (SAFE, HOLIDAY-AWARE) =================
@st.cache_data(ttl=55)
def live_data():
    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except Exception as e:
        # Kite down / holiday / 503 / rate limit
        st.warning("⚠️ Live data not available (Market closed or Kite issue)")
        return pd.DataFrame(columns=[
            "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
            "YEST_HIGH","YEST_LOW","YEST_CLOSE",
            "CHANGE","CHANGE_%"
        ])

    rows = []

    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue

        tk = get_token(s)
        yh, yl, yc = fetch_yesterday_ohlc(tk)

        ltp = q["last_price"]
        pc = q["ohlc"]["close"]
        chg = ltp - pc if pc else 0

        rows.append({
            "Symbol": s,
            "LTP": round(ltp, 2),
            "LIVE_OPEN": round(q["ohlc"]["open"], 2),
            "LIVE_HIGH": round(q["ohlc"]["high"], 2),
            "LIVE_LOW": round(q["ohlc"]["low"], 2),
            "LIVE_VOLUME": q.get("volume", 0),   # ✅ LIVE DAY VOLUME
            "YEST_HIGH": yh,
            "YEST_LOW": yl,
            "YEST_CLOSE": yc,
            "CHANGE": round(chg, 2),
            "CHANGE_%": round((chg / pc) * 100, 2) if pc else 0
        })

        


    return pd.DataFrame(rows)



# ================= MERGE =================
df = (
    live_data()
    .merge(pd.read_csv(DAILY_FILE), on="Symbol", how="left")
    .merge(pd.read_csv(WEEKLY_FILE), on="Symbol", how="left", suffixes=("","_W"))
    .merge(pd.read_csv(MONTHLY_FILE), on="Symbol", how="left", suffixes=("","_M"))
    .merge(panchak_df, on="Symbol", how="left")
    #.merge(pd.read_csv(PANCHAK_FILE), on="Symbol", how="left")

)

# ================= FIX-2: REMOVE DUPLICATE SYMBOLS =================
df = (
    df
    .sort_values("Symbol")
    .drop_duplicates(subset=["Symbol"], keep="last")
    .reset_index(drop=True)
)
# ==================================================================


if "LIVE_VOLUME" in df.columns:
    df["LIVE_VOLUME"] = pd.to_numeric(df["LIVE_VOLUME"], errors="coerce").fillna(0)

# ================= SAFETY NET: REQUIRED LIVE COLUMNS =================

REQUIRED_COLS = [
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE"
]

missing = [c for c in REQUIRED_COLS if c not in df.columns]
if missing:
    st.error(f"❌ Missing required columns: {missing}")
    st.stop()

#st.write("DEBUG DF COLUMNS:", df.columns.tolist())


# ================= NEAR / GAIN =================
def near(r):
    if r.LTP >= r.TOP_HIGH: return "🟢 ↑ BREAK"
    if r.LTP <= r.TOP_LOW:  return "🔴 ↓ BREAK"
    return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}" if (r.TOP_HIGH-r.LTP) <= (r.LTP-r.TOP_LOW) else f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

df["NEAR"] = df.apply(near, axis=1)

# ================= GAIN (SAFE, NUMERIC, ARROW-COMPATIBLE) =================

def gain(r):
    if pd.notna(r.TOP_HIGH) and r.LTP > r.TOP_HIGH:
        return round(r.LTP - r.TOP_HIGH, 2)
    if pd.notna(r.TOP_LOW) and r.LTP < r.TOP_LOW:
        return round(r.LTP - r.TOP_LOW, 2)
    return None  # IMPORTANT: None, not ""

df["GAIN"] = df.apply(gain, axis=1)
df["GAIN"] = pd.to_numeric(df["GAIN"], errors="coerce")




# =========================================================
# ROLLING OHLC (60 DAYS) + EMA20 / EMA50 (BULLETPROOF)
# =========================================================

OHLC_FILE = os.path.join(CACHE_DIR, "ohlc_60d.csv")
EMA_FILE  = os.path.join(CACHE_DIR, "ema_20_50.csv")


def last_trading_day(d):
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d


def build_or_update_ohlc_60d():
    """
    1️⃣ If file NOT present → build last 60 trading days
    2️⃣ If file present → append only latest trading day
    """

    today = date.today()
    end_day = last_trading_day(today)

    # ---------- CASE 1: FILE NOT EXISTS → FULL BUILD ----------
    if not os.path.exists(OHLC_FILE):
        rows = []

        start_day = end_day - timedelta(days=180)  # buffer to get 180 trading days

        for s in SYMBOLS:
            tk = get_token(s)
            if not tk:
                continue

            try:
                bars = kite.historical_data(
                    tk,
                    start_day,
                    end_day,
                    "day"
                )
            except:
                continue

            dfb = pd.DataFrame(bars)
            if dfb.empty:
                continue

            dfb["date"] = pd.to_datetime(dfb["date"]).dt.date
            dfb = dfb.sort_values("date").tail(60)

            for _, r in dfb.iterrows():
                rows.append({
                    "Symbol": s,
                    "date": r["date"],
                    "open":  r["open"],
                    "high":  r["high"],
                    "low":   r["low"],
                    "close": r["close"],
                    "volume": r["volume"],

                })

            tm.sleep(0.3)

        if rows:
            pd.DataFrame(rows).to_csv(OHLC_FILE, index=False)

        return

    # ---------- CASE 2: FILE EXISTS → DAILY APPEND ----------
    ohlc_df = pd.read_csv(OHLC_FILE)
    ohlc_df["date"] = pd.to_datetime(ohlc_df["date"]).dt.date

    if end_day in ohlc_df["date"].values:
        return  # already updated

    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        try:
            bars = kite.historical_data(
                tk,
                end_day,
                end_day,
                "day"
            )
        except:
            continue

        if not bars:
            continue

        b = bars[0]
        rows.append({
            "Symbol": s,
            "date": end_day,
            "open":  b["open"],
            "high":  b["high"],
            "low":   b["low"],
            "close": b["close"],
            "volume": b["volume"],

        })

        tm.sleep(0.25)

    if not rows:
        return

    new_df = pd.DataFrame(rows)

    final_df = (
        pd.concat([ohlc_df, new_df], ignore_index=True)
        .drop_duplicates(subset=["Symbol", "date"])
        .sort_values(["Symbol", "date"])
        .groupby("Symbol", as_index=False)
        .tail(60)
    )

    final_df.to_csv(OHLC_FILE, index=False)


def build_ema_from_ohlc():
    if not os.path.exists(OHLC_FILE):
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    df = pd.read_csv(OHLC_FILE)

    rows = []

    for s, g in df.groupby("Symbol"):
        if len(g) < 50:
            continue

        g = g.sort_values("date")
        g["EMA20"] = g["close"].ewm(span=20).mean()
        g["EMA50"] = g["close"].ewm(span=50).mean()

        rows.append({
            "Symbol": s,
            "EMA20": round(g.iloc[-1]["EMA20"], 2),
            "EMA50": round(g.iloc[-1]["EMA50"], 2)
        })

    if not rows:
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    ema_df = pd.DataFrame(rows)
    ema_df.to_csv(EMA_FILE, index=False)
    return ema_df


# AFTER main df is built
build_or_update_ohlc_60d()
ema_df = build_ema_from_ohlc()

df = df.merge(ema_df, on="Symbol", how="left")



# =========================================================
# 📊 INTRADAY 15-MIN OHLC DATA (SINGLE SOURCE)
# =========================================================

@st.cache_data(ttl=300)
def fetch_intraday_15m():
    rows = []

    start = datetime.combine(date.today(), time(9, 15))
    end   = datetime.now(IST)

    for sym in SYMBOLS:
        token = get_token(sym)
        if not token:
            continue

        try:
            bars = kite.historical_data(
                token,
                start,
                end,
                interval="15minute"
            )
        except:
            continue

        if not bars:
            continue

        for b in bars:
            rows.append({
                "Symbol": sym,
                "datetime": pd.to_datetime(b["date"]),
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b["volume"]
            })

        tm.sleep(0.25)

    return pd.DataFrame(rows)


# 🔹 BUILD INTRADAY DF (GLOBAL)
intraday_15m_df = fetch_intraday_15m()

# ================= SAFETY: intraday_15m_df =================
if intraday_15m_df.empty or "Symbol" not in intraday_15m_df.columns:
    intraday_15m_df = pd.DataFrame(
        columns=["Symbol", "datetime", "open", "high", "low", "close", "volume"]
    )
# ===========================================================




# =========================================================
# EMA20–EMA50 + TOP LIVE_HIGH / TOP LIVE_LOW (SINGLE SOURCE OF TRUTH)
# =========================================================

ema_signal_df = df.dropna(
    subset=["EMA20", "EMA50", "TOP_HIGH", "TOP_LOW"]
).loc[
    (
        (df["LTP"] > df["EMA20"]) &
        (df["EMA20"] > df["EMA50"]) &
        (df["LTP"] > df["TOP_HIGH"])
    ) |
    (
        (df["LTP"] < df["EMA20"]) &
        (df["EMA20"] < df["EMA50"]) &
        (df["LTP"] < df["TOP_LOW"])
    ),
    [
        "Symbol",
        "TOP_HIGH",
        "TOP_LOW",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "GAIN",
        "EMA20",
        "EMA50"
        
        
    ]
].copy()

ema_signal_df["SIGNAL"] = ema_signal_df.apply(
    lambda r: "BUY" if r["LTP"] > r["EMA20"] else "SELL",
    axis=1
)

# ================= SPLIT BUY / SELL =================

ema_buy_df = ema_signal_df[ema_signal_df["SIGNAL"] == "BUY"].copy()
ema_sell_df = ema_signal_df[ema_signal_df["SIGNAL"] == "SELL"].copy()







# ================= UI =================
#st.title("📊 Panchak + Breakout Dashboard")
st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")
st_autorefresh(interval=1780_000, key="refresh")
#st_autorefresh(interval=300_000, key="refresh")

PANCHAK_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "GAIN",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

panchak_view = df[PANCHAK_COLUMNS]

TOP_HIGH_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "DIFF",
    "BT",
    "ST",
    "YEST_HIGH",
    "YEST_LOW"
]

TOP_HIGH_view = df[TOP_HIGH_COLUMNS]

TOP_LOW_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "DIFF",
    "BT",
    "ST",
    "YEST_HIGH",
    "YEST_LOW"
]

TOP_LOW_view = df[TOP_LOW_COLUMNS]

NEAR_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

NEAR_view = df[NEAR_COLUMNS]

DAILY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

DAILY_view = df[DAILY_COLUMNS]

WEEKLY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

WEEKLY_view = df[WEEKLY_COLUMNS]

MONTHLY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

MONTHLY_view = df[MONTHLY_COLUMNS]


#import pandas as pd

def export_all_tabs_to_excel():
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # ================= CORE VIEWS =================
        if not panchak_view.empty:
            panchak_view.to_excel(writer, sheet_name="PANCHAK", index=False)

        if not TOP_HIGH_df.empty:
            TOP_HIGH_df.to_excel(writer, sheet_name="TOP_HIGH", index=False)

        if not TOP_LOW_df.empty:
            TOP_LOW_df.to_excel(writer, sheet_name="TOP_LOW", index=False)

        # ================= DAILY =================
        if not daily_up.empty:
            daily_up.to_excel(writer, sheet_name="DAILY_UP", index=False)

        if not daily_down.empty:
            daily_down.to_excel(writer, sheet_name="DAILY_DOWN", index=False)

        # ================= WEEKLY =================
        if not weekly_up.empty:
            weekly_up.to_excel(writer, sheet_name="WEEKLY_UP", index=False)

        if not weekly_down.empty:
            weekly_down.to_excel(writer, sheet_name="WEEKLY_DOWN", index=False)

        # ================= MONTHLY =================
        if not monthly_up.empty:
            monthly_up.to_excel(writer, sheet_name="MONTHLY_UP", index=False)

        if not monthly_down.empty:
            monthly_down.to_excel(writer, sheet_name="MONTHLY_DOWN", index=False)

        # ================= O=H / O=L =================
        if "ol_oh_df" in globals() and not ol_oh_df.empty:
            ol_oh_df.to_excel(writer, sheet_name="O_EQUALS_HL", index=False)

        # ================= 4 BAR =================
        if "four_bar_df" in globals() and not four_bar_df.empty:
            four_bar_df.to_excel(writer, sheet_name="FOUR_BAR", index=False)

        # ================= ALERTS =================
        if "alerts" in globals() and len(alerts) > 0:
            alerts_df = pd.DataFrame(alerts)
            alerts_df.to_excel(writer, sheet_name="ALERTS", index=False)

        # ================= OPTIONAL: COMBINED G/L =================
        if "gainers_losers_df" in globals() and not gainers_losers_df.empty:
            gainers_losers_df.to_excel(writer, sheet_name="GAINERS_LOSERS", index=False)

        if "fake_yh15_df" in globals() and not fake_yh15_df.empty:
            fake_yh15_df.to_excel(writer, sheet_name="FAKE_YH1.5", index=False)


    output.seek(0)
    return output





# ================= BREAKOUT DATA (GLOBAL) =================

TOP_HIGH_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
TOP_LOW_df  = df.loc[df.LTP <= df.TOP_LOW,  TOP_LOW_COLUMNS]

near_df = df.loc[
    (df.LTP > df.TOP_LOW) & (df.LTP < df.TOP_HIGH),
    [
        "Symbol","LTP","TOP_HIGH","TOP_LOW","NEAR",
        "LIVE_HIGH","LIVE_LOW","CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW"
    ]
]

daily_up   = df.loc[df.LTP >= df.YEST_HIGH, DAILY_COLUMNS]
daily_down = df.loc[df.LTP <= df.YEST_LOW,  DAILY_COLUMNS]

weekly_up   = df.loc[df.LTP >= df.HIGH_W, WEEKLY_COLUMNS]
weekly_down = df.loc[df.LTP <= df.LOW_W,  WEEKLY_COLUMNS]

monthly_up   = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
monthly_down = df.loc[df.LTP <= df.LOW_M,  MONTHLY_COLUMNS]

def notify_browser(title, symbols):
    if not symbols:
        return
    st.toast(f"{title}: {', '.join(symbols)}", icon="🚨")

def detect_new_entries(name, current_symbols):
    path = f"CACHE/{name}_prev.txt"
    prev = set(open(path).read().split(",")) if os.path.exists(path) else set()
    curr = set(current_symbols)

    new = curr - prev

    with open(path, "w") as f:
        f.write(",".join(curr))

    return list(new)

new_TOP_HIGH = detect_new_entries(
    "TOP_HIGH",
    TOP_HIGH_df.Symbol.tolist()
)

notify_browser("🟢 New TOP LIVE_HIGH", new_TOP_HIGH)

new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

notify_browser("🔴 New TOP LIVE_LOW", new_TOP_LOW)

import json
from datetime import datetime, timedelta

def can_send_email():
    if not EMAIL_ENABLED:
        return False

    os.makedirs("CACHE", exist_ok=True)

    now = datetime.now()

    data = {
        "last_sent": None,
        "count_today": 0,
        "date": now.strftime("%Y-%m-%d")
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    # Reset daily counter
    if data["date"] != now.strftime("%Y-%m-%d"):
        data["date"] = now.strftime("%Y-%m-%d")
        data["count_today"] = 0
        data["last_sent"] = None

    # Daily limit
    if data["count_today"] >= EMAIL_MAX_PER_DAY:
        return False

    # Cooldown
    if data["last_sent"]:
        last = datetime.fromisoformat(data["last_sent"])
        if now - last < timedelta(minutes=EMAIL_COOLDOWN_MIN):
            return False

    return True


def record_email_sent():
    now = datetime.now()

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "last_sent": now.isoformat(),
        "count_today": 0
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    data["count_today"] += 1
    data["last_sent"] = now.isoformat()

    with open(EMAIL_META_FILE, "w") as f:
        json.dump(data, f)
import json
from datetime import datetime, timedelta

def can_send_email():
    if not EMAIL_ENABLED:
        return False

    os.makedirs("CACHE", exist_ok=True)

    now = datetime.now()

    data = {
        "last_sent": None,
        "count_today": 0,
        "date": now.strftime("%Y-%m-%d")
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    # Reset daily counter
    if data["date"] != now.strftime("%Y-%m-%d"):
        data["date"] = now.strftime("%Y-%m-%d")
        data["count_today"] = 0
        data["last_sent"] = None

    # Daily limit
    if data["count_today"] >= EMAIL_MAX_PER_DAY:
        return False

    # Cooldown
    if data["last_sent"]:
        last = datetime.fromisoformat(data["last_sent"])
        if now - last < timedelta(minutes=EMAIL_COOLDOWN_MIN):
            return False

    return True


def record_email_sent():
    now = datetime.now()

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "last_sent": now.isoformat(),
        "count_today": 0
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    data["count_today"] += 1
    data["last_sent"] = now.isoformat()

    with open(EMAIL_META_FILE, "w") as f:
        json.dump(data, f)


EMAIL_ENABLED = True  # 🔁 set False to fully disable emails

def send_email(subject, body):
    if not can_send_email():
        print("EMAIL SKIPPED: limit or cooldown active")
        return

    try:
        msg = EmailMessage()
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)

        record_email_sent()

    except Exception as e:
        print("EMAIL ERROR:", e)
        #st.warning("📧 Email alert blocked (Gmail limit). Alerts still logged.")


def email_already_sent(symbol, category):
    os.makedirs("CACHE", exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(EMAIL_DEDUP_FILE):
        return False

    df = pd.read_csv(EMAIL_DEDUP_FILE)

    return (
        (df["DATE"] == today) &
        (df["SYMBOL"] == symbol) &
        (df["CATEGORY"] == category)
    ).any()


def mark_email_sent(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    row = {
        "DATE": today,
        "SYMBOL": symbol,
        "CATEGORY": category
    }

    if os.path.exists(EMAIL_DEDUP_FILE):
        df = pd.read_csv(EMAIL_DEDUP_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(EMAIL_DEDUP_FILE, index=False)




if new_TOP_HIGH:
    send_email(
        "TOP LIVE_HIGH Breakout Alert",
        "New TOP LIVE_HIGH:\n" + "\n".join(new_TOP_HIGH)
    )



from datetime import datetime

def log_alert(symbol, category, details, ltp, source):
    if alert_already_logged(symbol, category):
        return  # 🚫 Prevent duplicate static alerts

    now = datetime.now()

    row = {
        "DATE": now.strftime("%Y-%m-%d"),
        "TIME": now.strftime("%H:%M:%S"),
        "SYMBOL": symbol,
        "CATEGORY": category,
        "DETAILS": details,
        "LTP": ltp,
        "SOURCE": source
    }

    if os.path.exists(ALERTS_LOG_FILE):
        df = pd.read_csv(ALERTS_LOG_FILE)
        df = pd.concat([pd.DataFrame([row]), df], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_LOG_FILE, index=False)

    # Mark this alert as logged
    mark_alert_logged(symbol, category)


def alert_already_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(ALERTS_DEDUP_FILE):
        return False

    df = pd.read_csv(ALERTS_DEDUP_FILE)

    return (
        (df["DATE"] == today) &
        (df["SYMBOL"] == symbol) &
        (df["CATEGORY"] == category)
    ).any()


def mark_alert_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    row = {
        "DATE": today,
        "SYMBOL": symbol,
        "CATEGORY": category
    }

    if os.path.exists(ALERTS_DEDUP_FILE):
        df = pd.read_csv(ALERTS_DEDUP_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_DEDUP_FILE, index=False)

####################################################################################

def notify_all(category, title, symbols, ltp_map=None):
    if not symbols:
        return

    email_symbols = []

    for sym in symbols:
        # Dedup email per symbol per category per day
        if not email_already_sent(sym, category):
            email_symbols.append(sym)
            mark_email_sent(sym, category)

        # Always log alert (static journal)
        ltp = ltp_map.get(sym) if ltp_map else ""
        log_alert(
            symbol=sym,
            category=category,
            details=title,
            ltp=ltp,
            source="EMAIL + BROWSER"
        )

    # Send ONE email only if new symbols exist
    if email_symbols:
        send_email(
            title,
            title + "\n\n" + "\n".join(email_symbols)
        )

    # Browser toast (safe, non-flooding)
    st.toast(f"{title}: {', '.join(symbols)}")



################################################################################
ltp_map = dict(zip(df.Symbol, df.LTP))

new_TOP_HIGH = detect_new_entries(
    "TOP_HIGH",
    TOP_HIGH_df.Symbol.tolist()
)

notify_all(
    "TOP_HIGH",
    "🟢 TOP LIVE_HIGH Breakout",
    new_TOP_HIGH,
    ltp_map
)
new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

notify_all(
    "TOP_LOW",
    "🔴 TOP LIVE_LOW Breakdown",
    new_TOP_LOW,
    ltp_map
)
new_ema = detect_new_entries(
    "EMA20_50",
    ema_signal_df.Symbol.tolist()
)

notify_all(
    "EMA20_50",
    "⚡ EMA20–EMA50 Signal",
    new_ema,
    ltp_map
)
new_daily_up = detect_new_entries(
    "DAILY_UP",
    daily_up.Symbol.tolist()
)

notify_all(
    "DAILY_UP",
    "📈 DAILY LIVE_HIGH Break",
    new_daily_up,
    ltp_map
)

new_daily_down = detect_new_entries(
    "DAILY_DOWN",
    daily_down.Symbol.tolist()
)

notify_all(
    "DAILY_DOWN",
    "📉 DAILY LIVE_LOW Break",
    new_daily_down,
    ltp_map
)
new_weekly_up = detect_new_entries(
    "WEEKLY_UP",
    weekly_up.Symbol.tolist()
)

notify_all(
    "WEEKLY_UP",
    "📊 WEEKLY LIVE_HIGH Break",
    new_weekly_up,
    ltp_map
)

new_weekly_down = detect_new_entries(
    "WEEKLY_DOWN",
    weekly_down.Symbol.tolist()
)

notify_all(
    "WEEKLY_DOWN",
    "📉 WEEKLY LIVE_LOW Break",
    new_weekly_down,
    ltp_map
)
new_monthly_up = detect_new_entries(
    "MONTHLY_UP",
    monthly_up.Symbol.tolist()
)

notify_all(
    "MONTHLY_UP",
    "📅 MONTHLY LIVE_HIGH Break",
    new_monthly_up,
    ltp_map
)

new_monthly_down = detect_new_entries(
    "MONTHLY_DOWN",
    monthly_down.Symbol.tolist()
)

notify_all(
    "MONTHLY_DOWN",
    "📉 MONTHLY LIVE_LOW Break",
    new_monthly_down,
    ltp_map
)

ohl_df = df.loc[
    (df["LIVE_OPEN"] == df["LIVE_HIGH"]) | (df["LIVE_OPEN"] == df["LIVE_LOW"]),
    [
        "Symbol",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "YEST_HIGH",
        "YEST_LOW"
    ]
].copy()


ohl_df["TYPE"] = ohl_df.apply(
    lambda r: "🔴 LIVE_OPEN = LIVE_HIGH" if r.LIVE_OPEN == r.LIVE_HIGH else "🟢 LIVE_OPEN = LIVE_LOW",
    axis=1
)

# ================= LIVE_OPEN = LIVE_HIGH / LIVE_LOW SPLIT =================

LIVE_OPEN_LIVE_LOW_df = pd.DataFrame()
LIVE_OPEN_LIVE_HIGH_df = pd.DataFrame()

if not ohl_df.empty and "TYPE" in ohl_df.columns:
    LIVE_OPEN_LIVE_LOW_df = ohl_df[ohl_df["TYPE"] == "🟢 LIVE_OPEN = LIVE_LOW"]
    LIVE_OPEN_LIVE_HIGH_df = ohl_df[ohl_df["TYPE"] == "🔴 LIVE_OPEN = LIVE_HIGH"]




NUM_COLS = [
    "EMA20","EMA50","TOP_HIGH","TOP_LOW",
    "YEST_HIGH","YEST_LOW",
    "HIGH_W","LOW_W","HIGH_M","LOW_M"
]

for c in NUM_COLS:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")


ema_signal_df = df.dropna(subset=["EMA20","EMA50"]).loc[
    (
        (df.LTP > df.EMA20) &
        (df.EMA20 > df.EMA50) &
        (df.LTP > df.TOP_HIGH)
    ) |
    (
        (df.LTP < df.EMA20) &
        (df.EMA20 < df.EMA50) &
        (df.LTP < df.TOP_LOW)
    ),
    [
        "Symbol",
        "LTP",
        "EMA20",
        "EMA50",
        "TOP_HIGH",
        "TOP_LOW",
        "CHANGE",
        "CHANGE_%"
    ]
].copy()

ema_signal_df["SIGNAL"] = ema_signal_df.apply(
    lambda r: "🟢 BUY" if r.LTP > r.EMA20 else "🔴 SELL",
    axis=1
)

# ================= TOP GAINERS / LOSERS =================

#gainers_df = (
 #   df[df["CHANGE_%"] >= 2.5]
  #  .sort_values("CHANGE_%", ascending=False)
   # .loc[:, [
    #    "Symbol",
     #   "LTP",
      #  "CHANGE",
       # "CHANGE_%",
        #"LIVE_HIGH",
        #"LIVE_LOW"
    #]]
#)

losers_df = (
    df[df["CHANGE_%"] <= -2.5]
    .sort_values("CHANGE_%")
    .loc[:, [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_HIGH",
        "LIVE_LOW"
    ]]
)

# =========================================================
# TOP GAINERS / LOSERS — COLUMN ORDER FIX
# =========================================================

TOP_GL_COLUMNS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "LIVE_VOLUME",
]

# --- TOP GAINERS ---
gainers_df = (
    df[df["CHANGE_%"] > 0]
    .sort_values("CHANGE_%", ascending=False)
    [TOP_GL_COLUMNS]
)

# --- TOP LOSERS ---
losers_df = (
    df[df["CHANGE_%"] < 0]
    .sort_values("CHANGE_%")
    [TOP_GL_COLUMNS]
)

TOP_GL_COLUMNS = [c for c in TOP_GL_COLUMNS if c in df.columns]

new_gainers = detect_new_entries(
    "TOP_GAINERS",
    gainers_df.Symbol.tolist()
)

notify_all(
    "TOP_GAINERS",
    "🔥 Top Gainers > 2.5%",
    new_gainers,
    ltp_map
)

new_losers = detect_new_entries(
    "TOP_LOSERS",
    losers_df.Symbol.tolist()
)

notify_all(
    "TOP_LOSERS",
    "🔥 Top LOSERS < -2.5%",
    new_losers,
    ltp_map
)

        ############## TOP GAINERS NEW ADDITION ##############################

# ================= O=H / O=L FILTERED SETUPS =================

ol_condition = (
    (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
    (df["LIVE_OPEN"] < df["YEST_HIGH"])
)

oh_condition = (
    (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
    (df["LIVE_OPEN"] > df["YEST_LOW"])
)

ol_oh_df = df.loc[
    ol_condition | oh_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
    ]
].copy()

ol_oh_df["SETUP"] = np.where(
    ol_condition.loc[ol_oh_df.index],
    "🟢 O = L",
    "🔴 O = H"
)

ol_oh_df["SIDE"] = np.where(
    ol_condition.loc[ol_oh_df.index],
    "BULLISH",
    "BEARISH"
)

ol_oh_df = ol_oh_df.sort_values(
    by=["SIDE", "CHANGE_%"],
    ascending=[True, False]
)





# ================= DAILY + EMA CONFIRMATION =================
daily_ema_buy = df.loc[
    (df.LTP > df.YEST_HIGH) &
    (df.LTP > df.EMA20) &
    (df.EMA20 > df.EMA50),
    DAILY_COLUMNS
]

daily_ema_sell = df.loc[
    (df.LTP < df.YEST_LOW) &
    (df.LTP < df.EMA20) &
    (df.EMA20 < df.EMA50),
    DAILY_COLUMNS
]

# ================= WEEKLY + EMA CONFIRMATION =================

weekly_ema_buy = df.loc[
    (df.LTP > df.HIGH_W) &
    (df.LTP > df.EMA20) &
    (df.EMA20 > df.EMA50),
    WEEKLY_COLUMNS
]

weekly_ema_sell = df.loc[
    (df.LTP < df.LOW_W) &
    (df.LTP < df.EMA20) &
    (df.EMA20 < df.EMA50),
    WEEKLY_COLUMNS
]



# =========================================================
# SUPERTREND (DAILY) + VWAP  — CLEAN VERSION
# (NO ZERO VALUES | BUY / SELL SEPARATE)
# =========================================================

def compute_supertrend(df, period=10, multiplier=3):
    df = df.copy()

    df["H-L"] = df["high"] - df["low"]
    df["H-PC"] = (df["high"] - df["close"].shift()).abs()
    df["L-PC"] = (df["low"] - df["close"].shift()).abs()

    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)

    # 🔥 Wilder ATR (Kite match)
    df["ATR"] = df["TR"].rolling(period).mean()
    df["ATR"] = df["ATR"].combine_first(
        df["ATR"].shift().ewm(alpha=1/period, adjust=False).mean()
    )

    mid = (df["high"] + df["low"]) / 2
    df["UpperBand"] = mid + multiplier * df["ATR"]
    df["LowerBand"] = mid - multiplier * df["ATR"]

    df["SuperTrend"] = np.nan
    df["ST_DIR"] = None

    # Seed
    df.loc[period, "SuperTrend"] = df.loc[period, "UpperBand"]
    df.loc[period, "ST_DIR"] = "SELL"

    for i in range(period + 1, len(df)):
        prev = i - 1

        if df.loc[prev, "close"] > df.loc[prev, "UpperBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "LowerBand"]
            df.loc[i, "ST_DIR"] = "BUY"

        elif df.loc[prev, "close"] < df.loc[prev, "LowerBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "UpperBand"]
            df.loc[i, "ST_DIR"] = "SELL"

        else:
            df.loc[i, "SuperTrend"] = df.loc[prev, "SuperTrend"]
            df.loc[i, "ST_DIR"] = df.loc[prev, "ST_DIR"]

        # 🔒 Band carry-forward (Kite logic)
        if df.loc[i, "ST_DIR"] == "BUY":
            df.loc[i, "LowerBand"] = max(
                df.loc[i, "LowerBand"], df.loc[prev, "LowerBand"]
            )
        else:
            df.loc[i, "UpperBand"] = min(
                df.loc[i, "UpperBand"], df.loc[prev, "UpperBand"]
            )

    return df



# -----------------------------
# BUILD SUPERTREND PER SYMBOL
# -----------------------------
supertrend_rows = []

ohlc_full = pd.read_csv(OHLC_FILE)
ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

for sym, g in ohlc_full.groupby("Symbol"):
    g = g.sort_values("date").reset_index(drop=True)

    if len(g) < 20:
        continue

    st_df = compute_supertrend(g)
    last = st_df.iloc[-1]

    # 🔴 SKIP IF NOT INITIALIZED
    if pd.isna(last["SuperTrend"]):
        continue

    supertrend_rows.append({
        "Symbol": sym,
        "SUPERTREND": round(last["SuperTrend"], 2),
        "ST_SIGNAL": last["ST_DIR"]
    })

supertrend_df = pd.DataFrame(supertrend_rows)

df = df.merge(supertrend_df, on="Symbol", how="left")


# -----------------------------
# VWAP (INTRADAY APPROX)
# -----------------------------
def compute_vwap(row):
    price = (row["LIVE_HIGH"] + row["LIVE_LOW"] + row["LTP"]) / 3
    return round(price, 2)

df["VWAP"] = df.apply(compute_vwap, axis=1)
df["VWAP_DIFF"] = (df["LTP"] - df["VWAP"]).round(2)


# -----------------------------
# CLEAN + ACTIONABLE VIEW
# -----------------------------
supertrend_view = df[
    (df["SUPERTREND"].notna())
].copy()

# Remove invalid / zero SuperTrend values
supertrend_view = supertrend_view[
    supertrend_view["SUPERTREND"] > 0
].copy()


supertrend_view["ST_BUY"] = supertrend_view.apply(
    lambda x: "BUY" if x["ST_SIGNAL"] == "BUY" else "",
    axis=1
)

supertrend_view["ST_SELL"] = supertrend_view.apply(
    lambda x: "SELL" if x["ST_SIGNAL"] == "SELL" else "",
    axis=1
)

supertrend_view = supertrend_view[
    [
        "Symbol",
        "LTP",
        "SUPERTREND",
        "ST_BUY",
        "ST_SELL",
        "VWAP",
        "VWAP_DIFF",
        "EMA20",
        "EMA50",
        "TOP_HIGH",
        "TOP_LOW",
        "CHANGE_%"
    ]
].copy()

DISPLAY_ROUND_COLS = [
    "LTP",
    "SUPERTREND",
    "VWAP",
    "VWAP_DIFF",
    "EMA20",
    "EMA50",
    "TOP_HIGH",
    "TOP_LOW",
    "CHANGE_%"
]

for col in DISPLAY_ROUND_COLS:
    if col in supertrend_view.columns:
        supertrend_view[col] = supertrend_view[col].round(2)



# =========================================================
# SUPERTREND — PREPARE DATAFRAME + STYLER (MUST BE BEFORE TABS)
# =========================================================

def highlight_supertrend(row):
    styles = []
    for col in row.index:
        if col == "ST_BUY" and row[col] == "BUY":
            styles.append("background-color:#d4f8d4;color:#006400;font-weight:bold;")
        elif col == "ST_SELL" and row[col] == "SELL":
            styles.append("background-color:#ffd6d6;color:#8b0000;font-weight:bold;")
        else:
            styles.append("")
    return styles



# DataFrame used for logic + empty checks
supertrend_df = supertrend_view.copy()

# Create Styler ONLY for UI
supertrend_styled = supertrend_df.style.format({
    "LTP": "{:.2f}",
    "SUPERTREND": "{:.2f}",
    "VWAP": "{:.2f}",
    "VWAP_DIFF": "{:.2f}",
    "EMA20": "{:.2f}",
    "EMA50": "{:.2f}",
    "TOP_HIGH": "{:.2f}",
    "TOP_LOW": "{:.2f}",
    "CHANGE_%": "{:.2f}"
}).apply(highlight_supertrend, axis=1)


#####################################           SUPERTREND – NEAR LTP ZONE

supertrend_view = df[
    (df["SUPERTREND"].notna())
].copy()


# =========================================================
# 🎯 SUPERTREND – NEAR LTP OPPORTUNITY TABLE
# =========================================================

# Distance in % between LTP and Supertrend
supertrend_view["ST_DIST_%"] = (
    (supertrend_view["LTP"] - supertrend_view["SUPERTREND"])
    / supertrend_view["SUPERTREND"] * 100
).round(2)

# Absolute distance for sorting
supertrend_view["ST_DIST_ABS"] = supertrend_view["ST_DIST_%"].abs()

# 🔎 Filter: only NEAR opportunities (adjust threshold if needed)
ST_NEAR_THRESHOLD = 2.0   # 2% near zone

st_near_df = supertrend_view[
    supertrend_view["ST_DIST_ABS"] <= ST_NEAR_THRESHOLD
].copy()

# Direction clarity
st_near_df["SIDE"] = np.where(
    st_near_df["ST_SIGNAL"] == "BUY", "🟢 LONG",
    "🔴 SHORT"
)

# Sort by nearest first
st_near_df = st_near_df.sort_values("ST_DIST_ABS")

# Final columns (clean & actionable)
ST_NEAR_COLUMNS = [
    "Symbol",
    "SIDE",
    "LTP",
    "CHANGE",
    "CHANGE_%"
    "SUPERTREND",
    "ST_DIST_%",
    "VWAP",
    "EMA20",
    "EMA50",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "NEAR"
    
]

ST_NEAR_COLUMNS = [c for c in ST_NEAR_COLUMNS if c in st_near_df.columns]

st_near_view = st_near_df[ST_NEAR_COLUMNS]


##########################################          EXACT FIX — CREATE INTRADAY SUPERTREND
#📍 Step 1: Build 15-min Supertrend
def supertrend(df, period=10, multiplier=3):
    hl2 = (df["high"] + df["low"]) / 2
    atr = df["tr"].rolling(period).mean()

    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr

    st = []
    direction = []

    for i in range(len(df)):
        if i == 0:
            st.append(upperband.iloc[i])
            direction.append("SELL")
            continue

        if df["close"].iloc[i] > st[i-1]:
            st.append(max(lowerband.iloc[i], st[i-1]))
            direction.append("BUY")
        else:
            st.append(min(upperband.iloc[i], st[i-1]))
            direction.append("SELL")

    df["ST"] = st
    df["ST_DIR"] = direction
    return df

########################            Step 2: Apply on 15-min data (THIS IS KEY)

intraday_st_rows = []

for sym in SYMBOLS:
    df15 = intraday_15m_df[intraday_15m_df["Symbol"] == sym].copy()
    if len(df15) < 20:
        continue

    df15["tr"] = np.maximum(
        df15["high"] - df15["low"],
        np.maximum(
            abs(df15["high"] - df15["close"].shift()),
            abs(df15["low"] - df15["close"].shift())
        )
    )

    df15 = supertrend(df15, period=10, multiplier=3)

    last = df15.iloc[-1]

    intraday_st_rows.append({
        "Symbol": sym,
        "ST_INTRADAY": round(last["ST"], 2),
        "ST_DIR_INTRADAY": last["ST_DIR"]
    })

intraday_st_df = pd.DataFrame(intraday_st_rows)

#########           Step 3: Merge with live df
df = df.merge(intraday_st_df, on="Symbol", how="left")

#######################         NOW BUILD THE CORRECT “NEAR LTP” TABLE

df["ST_INTRADAY_DIST_%"] = (
    (df["LTP"] - df["ST_INTRADAY"]) / df["ST_INTRADAY"] * 100
).round(2)

st_near_intraday = df[
    df["ST_INTRADAY_DIST_%"].abs() <= 0.7
].sort_values("ST_INTRADAY_DIST_%")





# ================= EMA20–EMA50 SPLIT (BUY / SELL) =================

#ema_buy_df = ema_signal_df[ema_signal_df["SIGNAL"] == "BUY"].copy()
#ema_sell_df = ema_signal_df[ema_signal_df["SIGNAL"] == "SELL"].copy()

# =========================================================
# TOP GAINERS / LOSERS (LEAST EXTREME ON TOP)
# =========================================================

gainers_df = (
    df[df["CHANGE_%"] > 2.5]
    .sort_values(by="CHANGE_%", ascending=True)   # least positive first
    .copy()
)

losers_df = (
    df[df["CHANGE_%"] < -2.5]
    .sort_values(by="CHANGE_%", ascending=True)   # least negative first
    .copy()
)

# =========================================================
# EMA20–EMA50 SORTING (BASED ON GAIN)
# =========================================================

ema_buy_df = (
    ema_buy_df
    .sort_values(by="GAIN", ascending=True)   # least positive gain first
)

ema_sell_df = (
    ema_sell_df
    .sort_values(by="GAIN", ascending=False)   # least negative gain first
)



####################     OPTIONS SCORING ENGINE    ####################################







def option_score(row):
    score = 0
    reasons = []

    # -------- SPOT TREND --------
    if row["EMA20"] > row["EMA50"] and row["SUPERTREND"] == "BUY":
        score += 2
        reasons.append("Trend bullish")
    elif row["EMA20"] < row["EMA50"] and row["SUPERTREND"] == "SELL":
        score += 2
        reasons.append("Trend bearish")
    else:
        return 0, "Spot not aligned"

    # -------- BREAKOUT / NEAR --------
    if row["LTP"] > row["TOP_HIGH"] or "↑" in str(row.get("NEAR", "")):
        score += 2
        reasons.append("Upside breakout / near")
    elif row["LTP"] < row["TOP_LOW"] or "↓" in str(row.get("NEAR", "")):
        score += 2
        reasons.append("Downside breakout / near")

    # -------- VWAP --------
    if row["LTP"] > row["VWAP"]:
        score += 1
        reasons.append("Above VWAP")
    else:
        score -= 1

    # -------- ATR / MOMENTUM --------
    if row.get("ATR_PCT", 0) > 1.2:
        score += 1
        reasons.append("Good momentum")

    return score, ", ".join(reasons)


df[["OPTION_SCORE", "OPTION_REASON"]] = df.apply(
    lambda r: pd.Series(option_score(r)), axis=1
)

def recommend_strike(row):
    if row["OPTION_SCORE"] < 6:
        return "AVOID"

    # Expiry safety
    if row["OPTION_SCORE"] >= 8:
        return "ATM"

    if row["OPTION_SCORE"] == 7:
        return "ITM"

    return "OTM"

df["STRIKE_PREF"] = df.apply(recommend_strike, axis=1)

def option_verdict(row):
    if row["OPTION_SCORE"] >= 8:
        if row["EMA20"] > row["EMA50"]:
            return "STRONG CE BUY"
        else:
            return "STRONG PE BUY"
    return "AVOID"

df["OPTION_SIGNAL"] = df.apply(option_verdict, axis=1)

#ALERT FILTER (NO MORE EMAIL FLOOD)
STRONG_BUY_DF = df[df["OPTION_SIGNAL"].str.contains("STRONG")]
new_strong = detect_new_entries(
    "OPTION_STRONG",
    STRONG_BUY_DF["Symbol"].tolist()
)
if new_strong:
    notify_all(
        "OPTION_STRONG",
        "🔥 STRONG OPTIONS BUY",
        [
            f"{s} | {df.loc[df.Symbol==s,'OPTION_SIGNAL'].values[0]} | "
            f"Strike: {df.loc[df.Symbol==s,'STRIKE_PREF'].values[0]}"
            for s in new_strong
        ]
    )


def backtest_options(df, ohlc_df):
    results = []

    for sym in df["Symbol"].unique():
        spot = df[df.Symbol == sym].iloc[0]
        hist = ohlc_df[ohlc_df.Symbol == sym].sort_values("date").tail(30)

        for i in range(len(hist)-1):
            r = hist.iloc[i]
            next_day = hist.iloc[i+1]

            if spot["OPTION_SIGNAL"] == "STRONG CE BUY":
                pnl = next_day["close"] - r["close"]
            elif spot["OPTION_SIGNAL"] == "STRONG PE BUY":
                pnl = r["close"] - next_day["close"]
            else:
                continue

            results.append({
                "Symbol": sym,
                "Signal": spot["OPTION_SIGNAL"],
                "Day": r["date"],
                "PnL": round(pnl, 2)
            })

    return pd.DataFrame(results)

backtest_df = backtest_options(df, ohlc_full)


# =========================================================
# LIVE MAP (Symbol → Live Values) for quick access
# =========================================================
live_map = {}

required_cols = [
    "Symbol", "LTP", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "CHANGE", "CHANGE_%"
]

available_cols = [c for c in required_cols if c in df.columns]

for _, r in df[available_cols].iterrows():
    live_map[r["Symbol"]] = r.to_dict()


#####   SETUP 1: 4 BAR REVERSAL + Breakouts     ####################################################

# =========================================================
# 4-BAR SETUP (EXACT SCREENER MATCH)
# =========================================================

# =========================================================
# 4 BAR REVERSAL (STRICT)
# =========================================================


four_bar_rows = []

ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

for sym, g in ohlc_full.groupby("Symbol"):
    if sym not in live_map:
        continue
    g = g.sort_values("date").reset_index(drop=True)

    if len(g) < 5:
        continue

    d0  = g.iloc[-1]   # today
    d1  = g.iloc[-2]
    d2  = g.iloc[-3]
    d3  = g.iloc[-4]
    d4  = g.iloc[-5]

    # --- Last 4 RED candles (strict)
    red_4 = (
        (d1.close <= d1.open) and
        (d2.close <= d2.open) and
        (d3.close <= d3.open) and
        (d4.close <  d4.open)
    )

    if not red_4:
        continue

    # --- Today reversal conditions
    today_reversal = (
        (d0.open  > d1.low) and
        (d0.open  > d1.close) and
        (d0.high  > d1.high) and
        (d0.close > d0.open)
    )

    if not today_reversal:
        continue

    
    live = live_map[sym]

    four_bar_rows.append({
        "Symbol": sym,

        # 🔴 LIVE DATA (single source of truth)
        "LTP": round(live["LTP"], 2),
        "CHANGE": round(live["CHANGE"], 2),
        "CHANGE_%": round(live["CHANGE_%"], 2),

        # 🟢 Candle structure (from OHLC)
        "LIVE_OPEN": round(d0.open, 2),
        "LIVE_HIGH": round(d0.high, 2),
        "LIVE_LOW": round(d0.low, 2),

        # 🟡 Yesterday reference
        "YEST_HIGH": round(d1.high, 2),
        "YEST_LOW": round(d1.low, 2),
        "YEST_CLOSE": round(d1.close, 2),
    })

    


#four_bar_df = pd.DataFrame(four_bar_rows)
four_bar_df = pd.DataFrame(four_bar_rows)

# 🔒 SAFETY: ensure required columns exist
REQUIRED_4BAR_COLS = [
    "Symbol", "LTP", "CHANGE", "CHANGE_%",
    "LIVE_OPEN", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "YEST_CLOSE"
]

for col in REQUIRED_4BAR_COLS:
    if col not in four_bar_df.columns:
        four_bar_df[col] = np.nan






# =========================================================
# 🚨 FAKE BREAKOUTS : BULL TRAP & BEAR TRAP
# =========================================================

fake_bull_rows = []
fake_bear_rows = []

for _, r in df.iterrows():

    y_close = r["YEST_CLOSE"]
    if pd.isna(y_close) or y_close == 0:
        continue

    # -------------------------
    # % calculations
    # -------------------------
    high_pct = (r["LIVE_HIGH"] - y_close) / y_close * 100
    low_pct  = (r["LIVE_LOW"]  - y_close) / y_close * 100
    ltp_pct  = (r["LTP"] - y_close) / y_close * 100

    # =========================
    # 🟡 FAKE BULL TRAP
    # =========================
    if (
        high_pct >= 2.5 and          # broke +2.5%
        ltp_pct < 2.5 and            # failed to hold
        r["LIVE_OPEN"] < r["YEST_HIGH"]
    ):
        fake_bull_rows.append({
            "Symbol": r["Symbol"],
            "YEST_CLOSE": round(y_close, 2),
            "LIVE_HIGH": round(r["LIVE_HIGH"], 2),
            "LIVE_OPEN": round(r["LIVE_OPEN"], 2),
            "LTP": round(r["LTP"], 2),
            "CHANGE_%": round(ltp_pct, 2),
            "FAIL_%": round(high_pct - ltp_pct, 2)
        })

    # =========================
    # 🔵 FAKE BEAR TRAP
    # =========================
    if (
        low_pct <= -2.5 and          # broke −2.5%
        ltp_pct > -2.5 and           # recovered
        r["LIVE_OPEN"] > r["YEST_LOW"]
    ):
        fake_bear_rows.append({
            "Symbol": r["Symbol"],
            "YEST_CLOSE": round(y_close, 2),
            "LIVE_LOW": round(r["LIVE_LOW"], 2),
            "LIVE_OPEN": round(r["LIVE_OPEN"], 2),
            "LTP": round(r["LTP"], 2),
            "CHANGE_%": round(ltp_pct, 2),
            "FAIL_%": round(abs(low_pct - ltp_pct), 2)
        })

fake_bull_df = pd.DataFrame(fake_bull_rows)
fake_bear_df = pd.DataFrame(fake_bear_rows)

# =========================================================
# 🎨 Styling
# =========================================================
def style_ltp_relative(row):
    """
    Row-wise styling:
    - Green if LTP >= YEST_CLOSE
    - Orange if LTP < YEST_CLOSE
    """
    if row["LTP"] >= row["YEST_CLOSE"]:
        return ["background-color:#e8f5e9"] * len(row)
    else:
        return ["background-color:#fff3e0"] * len(row)

def style_bear_trap(_):
    return ["background-color:#fff3e0"] * len(fake_bear_df.columns)

def style_ltp_only(row):
    styles = [""] * len(row)

    ltp_idx = row.index.get_loc("LTP")

    if row["LTP"] >= row["YEST_CLOSE"]:
        styles[ltp_idx] = "background-color:#e8f5e9"  # light green
    else:
        styles[ltp_idx] = "background-color:#fff3e0"  # light orange

    return styles

def style_ltp_bear_only(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")
    styles[ltp_idx] = "background-color:#fff3e0"
    return styles
############ for 15 mins table style
def style_ltp_15min(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")

    if row["BREAK_TYPE"] == "UP":
        styles[ltp_idx] = "background-color:#e8f5e9; color:#1b5e20"  # green
    elif row["BREAK_TYPE"] == "DOWN":
        styles[ltp_idx] = "background-color:#ffebee; color:#b71c1c"  # red

    return styles







############    OPTION 2 (Fallback): Build 15-min from 5-min candles    ================
def fetch_5min_candles(symbols):
    rows = []

    today = datetime.now(IST).date()
    start = datetime.combine(today, time(9, 15))
    end   = datetime.combine(today, time(15, 30))

    for sym in symbols:
        tk = get_token(sym)
        if not tk:
            continue

        try:
            candles = kite.historical_data(
                tk,
                start,
                end,
                interval="5minute"
            )
        except Exception:
            continue

        for c in candles:
            rows.append({
                "Symbol": sym,
                "datetime": pd.to_datetime(c["date"]),
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"]
            })

    return pd.DataFrame(rows)

five_df = fetch_5min_candles(SYMBOLS)

# ================= SAFETY: 5-MIN DATA AVAILABILITY =================
if five_df.empty or "datetime" not in five_df.columns:
    intraday_15m_df = pd.DataFrame(columns=[
        "Symbol", "datetime", "open", "high", "low", "close"
    ])
else:
    intraday_15m_df = (
        five_df
        .set_index("datetime")
        .groupby("Symbol")
        .resample("15T")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last"
        })
        .dropna()
        .reset_index()
    )




############    SETUP 3: 15-MIN INSIDE RANGE BREAK    #######################

inside_break_rows = []

for sym in SYMBOLS:
    df15 = intraday_15m_df[intraday_15m_df["Symbol"] == sym].sort_values("datetime")

    if len(df15) < 4 or sym not in live_map:
        continue

    first = df15.iloc[0]
    later = df15.iloc[1:4]

    # 1️⃣ Inside range condition
    if not (
        later["high"].max() <= first["high"] and
        later["low"].min() >= first["low"]
    ):
        continue

    live = live_map[sym]
    ltp = live["LTP"]

    break_type = None
    chg_15m_pct = None

    # 2️⃣ Break detection + 15-min % calc
    if ltp > first["high"]:
        break_type = "UP"
        chg_15m_pct = round(
            ((ltp - first["high"]) / first["high"]) * 100, 2
        )

    elif ltp < first["low"]:
        break_type = "DOWN"
        chg_15m_pct = round(
            ((ltp - first["low"]) / first["low"]) * 100, 2
        )

    else:
        continue

    inside_break_rows.append({
        "Symbol": sym,
        "LTP": ltp,
        "CHG_15M_%": chg_15m_pct,
        "CHANGE": live["CHANGE"],
        "CHANGE_%": live["CHANGE_%"],
        "DAY_HIGH": live["LIVE_HIGH"],
        "YEST_HIGH": live["YEST_HIGH"],
        "DAY_LOW": live["LIVE_LOW"],
        "YEST_LOW": live["YEST_LOW"],
        "BREAK_TYPE": break_type
    })

inside_15m_df = pd.DataFrame(inside_break_rows)


# =========================================================
# YH1.5 STRONG BREAKOUT (SCREENER LOGIC) — FINAL & CORRECT
# Rules:
# 1. OPEN < YEST_HIGH  (no gap-up)
# 2. LTP >= YEST_HIGH * 1.015
# =========================================================

yh15_rows = []

for _, r in df.iterrows():

    yh = r["YEST_HIGH"]
    ltp = r["LTP"]
    live_open = r["LIVE_OPEN"]

    if pd.isna(yh) or yh <= 0:
        continue
    if pd.isna(ltp) or pd.isna(live_open):
        continue

    # 🚫 GAP-UP FILTER (ABSOLUTE)
    if live_open >= yh:
        continue

    level_15 = yh * 1.015

    # ✅ TRUE BREAKOUT
    if ltp >= level_15:

        break_pct = ((ltp - yh) / yh) * 100
        after_break_pct = ((ltp - level_15) / level_15) * 100

        yh15_rows.append({
            "Symbol": r["Symbol"],
            "LIVE_OPEN": round(live_open, 2),
            "LTP": round(ltp, 2),
            "YEST_HIGH": round(yh, 2),
            "BREAK_%": round(break_pct, 2),
            "AFTER_BREAK_%": round(after_break_pct, 2),
            "CHANGE": round(r["CHANGE"], 2),
            "CHANGE_%": round(r["CHANGE_%"], 2),
        })

yh15_df = pd.DataFrame(yh15_rows)

if not yh15_df.empty:
    yh15_df = yh15_df.sort_values(
        ["AFTER_BREAK_%", "BREAK_%"],
        ascending=False
    )




# =========================================================
# FAKE / FAILED YH1.5 BREAKOUTS
# =========================================================

fake_yh15_rows = []

for sym, live in live_map.items():

    yh = live.get("YEST_HIGH")
    ltp = live.get("LTP")
    high = live.get("LIVE_HIGH")

    if not yh or yh == 0:
        continue

    level_15 = yh * 1.015

    # 🔴 Attempted but failed breakout
    if high >= level_15 and ltp < level_15:

        break_pct = ((high - yh) / yh) * 100
        retrace_pct = ((ltp - high) / high) * 100 if high else 0

        fake_yh15_rows.append({
            "Symbol": sym,
            "LTP": round(ltp, 2),
            "LIVE_HIGH": round(high, 2),
            "YEST_HIGH": round(yh, 2),
            "BREAK_%": round(break_pct, 2),      # how much it broke
            "RETRACE_%": round(retrace_pct, 2),  # how much it failed
            "CHANGE": round(live.get("CHANGE", 0), 2),
            "CHANGE_%": round(live.get("CHANGE_%", 0), 2),
        })

fake_yh15_df = pd.DataFrame(fake_yh15_rows)

if not fake_yh15_df.empty:
    fake_yh15_df = fake_yh15_df.sort_values("RETRACE_%")




alerts = []
alert_keys = set()   # prevents duplicate alerts per refresh
##########      ALERT-1 IMPLEMENTATION ######## YH 1.5% Strong Breakout
yh15_df = df.loc[
    (df["LTP"] >= df["YEST_HIGH"] * 1.015) &
    (df["CHANGE_%"] >= 1),
    [
        "Symbol","LTP","CHANGE","CHANGE_%",
        "YEST_HIGH","LIVE_HIGH","LIVE_LOW","LIVE_OPEN","YEST_LOW","YEST_CLOSE"
    ]
].copy()

for _, r in yh15_df.iterrows():
    key = f"YH15_{r['Symbol']}"
    if key not in alert_keys:
        alerts.append({
            "TYPE": "🚀 YH 1.5% ",
            "Symbol": r["Symbol"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"],
            "DAY_OPEN": r["LIVE_OPEN"],
            "DAY_HIGH": r["LIVE_HIGH"],
            "DAY_LOW": r["LIVE_LOW"],
            "YEST_HIGH": r["YEST_HIGH"],
            "YEST_LOW": r["YEST_LOW"],
            "YEST_CLOSE": r["YEST_CLOSE"],
            

        })
        alert_keys.add(key)


###############     ALERT-2 IMPLEMENTATION      ## 4-BAR Reversal + Breakout

for _, r in four_bar_df.iterrows():

    if r["Symbol"] not in df["Symbol"].values:
        continue

    base = df.loc[df.Symbol == r.Symbol].iloc[0]

    # Bullish
    if (
        r["CHANGE_%"] > 0 and
        base["LTP"] > base["YEST_HIGH"]
    ):
        key = f"4BAR_BUY_{r['Symbol']}"
        if key not in alert_keys:
            alerts.append({
                "TYPE": "🔁 4-BAR REVERSAL BUY",
                "Symbol": r["Symbol"],
                "LTP": base["LTP"],
                "CHANGE_%": base["CHANGE_%"]
            })
            alert_keys.add(key)

    # Bearish
    if (
        r["CHANGE_%"] < 0 and
        base["LTP"] < base["YEST_LOW"]
    ):
        key = f"4BAR_SELL_{r['Symbol']}"
        if key not in alert_keys:
            alerts.append({
                "TYPE": "🔁 4-BAR REVERSAL SELL",
                "Symbol": r["Symbol"],
                "LTP": base["LTP"],
                "CHANGE_%": base["CHANGE_%"]
            })
            alert_keys.add(key)





# ================= YH 1.5 STRONG BREAKOUT ALERT =================

new_YH15 = detect_new_entries(
    "YH15",
    yh15_df.Symbol.tolist()
)

notify_browser("🚀 YH 1.5 STRONG BREAKOUT", new_YH15)

# ================= 4-BAR REVERSAL BUY ALERT =================

# =========================================================
# FIX-2 : SAFE 4-BAR BUY FILTER (BUY ONLY)
# =========================================================

if not four_bar_df.empty and "CHANGE_%" in four_bar_df.columns:

    four_bar_buy = four_bar_df.loc[
        four_bar_df["CHANGE_%"] > 0, "Symbol"
    ].tolist()

else:
    four_bar_buy = []


new_4BAR_BUY = detect_new_entries(
    "FOUR_BAR_BUY",
    four_bar_buy
)

notify_browser("🟢 4-BAR REVERSAL BUY", new_4BAR_BUY)


def load_index_symbols(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line.strip()]

INDEX_FILES = {
    "NIFTY 50": "NIFTY 50.txt",
    "BANK NIFTY": "BANK NIFTY.txt",
    "FINNIFTY": "FINNIFTY.txt",
    "NIFTY IT": "NIFTY IT.txt",
    "NIFTY FMCG": "NIFTY FMCG.txt",
    "NIFTY PHARMA": "NIFTY PHARMA.txt",
    "NIFTY METAL": "NIFTY METAL.txt",
    "NIFTY AUTO": "NIFTY AUTO.txt",
    "NIFTY ENERGY": "NIFTY ENERGY.txt",
    "NIFTY PSU BANK": "NIFTY PSU BANK.txt",
}

index_symbols = {
    name: load_index_symbols(file)
    for name, file in INDEX_FILES.items()
}

# =========================================================
# INDICES – LIVE OHLC (SINGLE SOURCE OF TRUTH)
# =========================================================

INDEX_SYMBOLS = {
    "NIFTY 50": "NIFTY",
    "BANK NIFTY": "BANKNIFTY",
    "FINNIFTY": "FINNIFTY",
    "NIFTY IT": "NIFTYIT",
    "NIFTY FMCG": "NIFTYFMCG",
    "NIFTY PHARMA": "NIFTYPHARMA",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY AUTO": "NIFTYAUTO",
    "NIFTY ENERGY": "NIFTYENERGY",
    "NIFTY PSU BANK": "NIFTYPSUBANK",
}

index_rows = []

for index_name, sym in INDEX_SYMBOLS.items():

    try:
        q = kite.quote(f"NSE:{sym}").get(f"NSE:{sym}")
        if not q:
            continue

        o = q["ohlc"]["open"]
        h = q["ohlc"]["high"]
        l = q["ohlc"]["low"]
        pc = q["ohlc"]["close"]   # prev close (index)
        ltp = q["last_price"]

        chg = ltp - pc if pc else 0
        chg_pct = (chg / pc * 100) if pc else 0

        tk = get_token(sym)
        yh, yl, yc = fetch_yesterday_ohlc(tk)

        index_rows.append({
            "Index": index_name,
            "OPEN": round(o, 2),
            "HIGH": round(h, 2),
            "LOW": round(l, 2),
            "LTP": round(ltp, 2),
            "CHANGE": round(chg, 2),
            "CHANGE_%": round(chg_pct, 2),
            "YEST_HIGH": yh,
            "YEST_LOW": yl,
            "YEST_CLOSE": yc,
        })

    except Exception as e:
        print("INDEX ERROR:", index_name, e)

indices_df = pd.DataFrame(index_rows)

# ================= INDICES TAB (FIXED) =================

INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "FINNIFTY",
    "NIFTYIT",
    "NIFTYFMCG",
    "NIFTYPHARMA",
    "NIFTYMETAL",
    "NIFTYAUTO",
    "NIFTYENERGY",
    "NIFTYPSUBANK",
]

indices_df = (
    df[df["Symbol"].isin(INDEX_ONLY_SYMBOLS)]
    .loc[:, [
        "Symbol",
        "LTP",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "CHANGE",
        "CHANGE_%",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE",
    ]]
    .sort_values("Symbol")
    .reset_index(drop=True)
)


# 🔒 SAFETY NET – ENSURE ALL REQUIRED COLUMNS EXIST
REQUIRED_INDEX_COLS = [
    "Index","OPEN","HIGH","LOW","LTP",
    "CHANGE","CHANGE_%",
    "YEST_HIGH","YEST_LOW","YEST_CLOSE"
]

for c in REQUIRED_INDEX_COLS:
    if c not in indices_df.columns:
        indices_df[c] = None


indices_df = indices_df.sort_values("CHANGE_%", ascending=False)

# =========================================================
# 🚨 ALERT: 3 CONSECUTIVE 15-MIN GREEN CANDLES
# (Valid until a RED candle appears)
# =========================================================

three_green_rows = []

for sym in SYMBOLS:

    # --- intraday candles (signal source) ---
    df15 = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(df15) < 3:
        continue

    # --- master df row (OHLC source) ---
    row = df[df["Symbol"] == sym]
    if row.empty:
        continue

    row = row.iloc[0]

    # Take last 3 COMPLETED 15-min candles
    last3 = df15.iloc[-3:]

    # Condition: all 3 green
    if not all(last3["close"] > last3["open"]):
        continue

    candle_time = last3.iloc[-1]["datetime"]

    three_green_rows.append({
        "TYPE": "🟢 3×15m GREEN",
        "Symbol": sym,

        # 🔴 LIVE
        "LTP": round(row["LTP"], 2),
        "CHANGE_%": round(row["CHANGE_%"], 2),

        # 🟡 DAY OHLC
        "DAY_OPEN": round(row["LIVE_OPEN"], 2),
        "DAY_HIGH": round(row["LIVE_HIGH"], 2),
        "DAY_LOW": round(row["LIVE_LOW"], 2),

        # 🟠 YESTERDAY
        "YEST_HIGH": row["YEST_HIGH"],
        "YEST_LOW": row["YEST_LOW"],
        "YEST_CLOSE": row["YEST_CLOSE"],

        # ⏱️ SIGNAL TIME
        "CANDLE_TIME": candle_time.strftime("%H:%M"),
    })

three_green_df = pd.DataFrame(three_green_rows)

# 🔒 SAFETY CHECK — INTRADAY DF
if intraday_15m_df.empty:
    st.warning("⚠️ Intraday 15m data empty (market closed or Kite issue)")
else:
    st.write("DEBUG intraday_15m_df columns:", intraday_15m_df.columns.tolist())


# =========================================================
# ADD 3×15m GREEN TO LIVE ALERTS (DEDUP SAFE)
# =========================================================

if not three_green_df.empty and "Symbol" in three_green_df.columns:

    for _, r in three_green_df.iterrows():
        sym = r["Symbol"]
        key = f"3GREEN_{sym}"

        if key in alert_keys:
            continue

        alerts.append(r.to_dict())
        alert_keys.add(key)

    new_3green = detect_new_entries(
        "THREE_GREEN_15M",
        three_green_df["Symbol"].tolist()   # ✅ SAFE ACCESS
    )

    notify_all(
        "THREE_GREEN_15M",
        "🟢 3×15-Min Green Candles",
        new_3green,
        ltp_map
    )



def detect_new_15m_signals(name, rows):
    """
    rows: list of dicts with Symbol + SCANDLE_TIME
    """
    path = f"CACHE/{name}_15m_prev.txt"

    prev = set(open(path).read().split(",")) if os.path.exists(path) else set()
    curr = set(f"{r['Symbol']}|{r['CANDLE_TIME']}" for r in rows)

    new = curr - prev

    with open(path, "w") as f:
        f.write(",".join(curr))

    return [x.split("|")[0] for x in new]

three_green_15m_df = pd.DataFrame(three_green_rows)

# =========================================================
# 🔔 LIVE ALERT — 3rd 15-min GREEN candle completed
# (integrated with LIVE ALERTS table)
# =========================================================

# ================== ALERT: 3 × 15m GREEN (WITH OHLC) ==================

if not three_green_15m_df.empty:

    for _, r in three_green_15m_df.iterrows():
        sym = r["Symbol"]

        # 🔒 pull OHLC from main df (single source of truth)
        live_row = df[df["Symbol"] == sym]
        if live_row.empty:
            continue

        live_row = live_row.iloc[0]

        key = f"3X15_{sym}_{r['CANDLE_TIME']}"
        if key in alert_keys:
            continue

        alerts.append({
            "TYPE": "🟢 3×15m GREEN",
            "Symbol": sym,

            # 🔴 LIVE
            "LTP": round(live_row["LTP"], 2),
            "CHANGE_%": round(live_row["CHANGE_%"], 2),

            # 🟡 DAY OHLC
            "DAY_OPEN": round(live_row["LIVE_OPEN"], 2),
            "DAY_HIGH": round(live_row["LIVE_HIGH"], 2),
            "DAY_LOW": round(live_row["LIVE_LOW"], 2),

            # 🟠 YESTERDAY
            "YEST_HIGH": live_row["YEST_HIGH"],
            "YEST_LOW": live_row["YEST_LOW"],
            "YEST_CLOSE": live_row["YEST_CLOSE"],

            # ⏱️ SIGNAL
            "CANDLE_TIME": r["CANDLE_TIME"],
        })

        alert_keys.add(key)


# ================== LIVE ALERTS ==================
if alerts:
    st.subheader("⚡ LIVE ALERTS")
    st.dataframe(pd.DataFrame(alerts), width='content')
# =================================================



tabs = st.tabs([
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    "🟡 NEAR",
    "📈 D-BREAKS",
    "📊 W-BREAKS",
    "📅 M-BREAKS",
    "⚡ O=H=L",  
    "📉 EMA20-50",
    "📈 SUPERTREND",
    "🔥 TOP G/L",
    " 4-BAR",
    "🧠 OPTIONS",
    "INDICES",
    "15-MIN-3",
    "⚡ Alerts",    
    "ℹ️ INFO"
])

with tabs[0]:
    #st.dataframe(df, width="stretch")
    #st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    st.subheader("🪐 Panchak – Full View")
    st.dataframe(panchak_view, width="stretch",height=7800)

with tabs[1]:
    
    st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    TOP_HIGH_df = (
    df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
    .sort_values(by="GAIN", ascending=True)   # least positive gain on top
    )

    st.dataframe(TOP_HIGH_df, width="stretch", height=7000)


with tabs[2]:
    #st.dataframe(df[df.LTP <= df.TOP_LOW])
    st.markdown('<div class="section-red"><b>🔴 TOP LIVE_LOW – Breakdowns</b></div>', unsafe_allow_html=True)
    TOP_LOW_df = (
    df.loc[df.LTP <= df.TOP_LOW, TOP_LOW_COLUMNS]
    .sort_values(by="GAIN", ascending=False)   # least negative gain on top
    )

    st.dataframe(TOP_LOW_df, width="stretch", height=7000)


with tabs[3]:
    st.markdown(
        '<div class="section-yelLIVE_LOW"><b>🟡 NEAR – Watch Zone</b></div>',
        unsafe_allow_html=True
    )

    # only stocks inside range
    near_base = df.loc[
        (df.LTP > df.TOP_LOW) & (df.LTP < df.TOP_HIGH),
        ["Symbol", "TOP_HIGH", "TOP_LOW", "LTP","CHANGE","CHANGE_%"]
    ].copy()

    if near_base.empty:
        st.info("No stocks currently between TOP_HIGH and TOP_LOW")
    else:
        # distances
        near_base["DIST_LIVE_HIGH"] = (near_base["TOP_HIGH"] - near_base["LTP"]).round(2)
        near_base["DIST_LIVE_LOW"]  = (near_base["LTP"] - near_base["TOP_LOW"]).round(2)

        # split based on *nearest*
        near_buy_df = near_base[
            near_base["DIST_LIVE_HIGH"] <= near_base["DIST_LIVE_LOW"]
        ].copy()

        near_sell_df = near_base[
            near_base["DIST_LIVE_HIGH"] > near_base["DIST_LIVE_LOW"]
        ].copy()

        # build NEAR column (arrow + value)
        near_buy_df["NEAR"]  = "🟢 ↑ " + near_buy_df["DIST_LIVE_HIGH"].astype(str)
        near_sell_df["NEAR"] = "🔴 ↓ " + near_sell_df["DIST_LIVE_LOW"].astype(str)

        # sorting: closest first
        near_buy_df  = near_buy_df.sort_values("DIST_LIVE_HIGH")
        near_sell_df = near_sell_df.sort_values("DIST_LIVE_LOW")

        # final columns
        near_buy_df = near_buy_df[
            ["Symbol", "TOP_HIGH", "TOP_LOW", "LTP", "NEAR","CHANGE","CHANGE_%"]
        ]
        near_sell_df = near_sell_df[
            ["Symbol", "TOP_HIGH", "TOP_LOW", "LTP", "NEAR","CHANGE","CHANGE_%"]
        ]

        # layout: BUY LEFT, SELL RIGHT
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🟢 NEAR BUY (Closer to TOP_HIGH)")
            if near_buy_df.empty:
                st.info("No BUY-side NEAR stocks")
            else:
                st.dataframe(
                    near_buy_df,
                    width="stretch",
                    height=min(4200, 60 + len(near_buy_df) * 35)
                )

        with col2:
            st.markdown("### 🔴 NEAR SELL (Closer to TOP_LOW)")
            if near_sell_df.empty:
                st.info("No SELL-side NEAR stocks")
            else:
                st.dataframe(
                    near_sell_df,
                    width="stretch",
                    height=min(4200, 60 + len(near_sell_df) * 35)
                )




    
with tabs[4]:
    st.subheader("📈 DAILY BREAKS – Above YEST HIGH")
    st.dataframe(daily_up, width="stretch")

    st.subheader("📉 DAILY BREAKS – Below YEST LOW")
    st.dataframe(daily_down, width="stretch")

    st.divider()

    st.markdown("### ✅ DAILY + EMA CONFIRMATION (HIGH Probability)")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("🟢 BUY : YEST HIGH + EMA20 > EMA50")
        if daily_ema_buy.empty:
            st.info("No Daily EMA BUY confirmations")
        else:
            st.dataframe(daily_ema_buy, width="stretch")

    with col2:
        st.markdown("🔴 SELL : YEST LOW + EMA20 < EMA50")
        if daily_ema_sell.empty:
            st.info("No Daily EMA SELL confirmations")
        else:
            st.dataframe(daily_ema_sell, width="stretch")


with tabs[5]:
    st.subheader("📊 WEEKLY BREAKS – Above WEEK HIGH")
    st.dataframe(weekly_up, width="stretch")

    st.subheader("📊 WEEKLY BREAKS – BeLIVE_LOW WEEK LOW")
    st.dataframe(weekly_down, width="stretch")

    st.divider()

    st.markdown("### ✅ WEEKLY + EMA CONFIRMATION (Strong Trend)")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("🟢 BUY : WEEK HIGH + EMA20 > EMA50")
        if weekly_ema_buy.empty:
            st.info("No Weekly EMA BUY confirmations")
        else:
            st.dataframe(weekly_ema_buy, width="stretch")

    with col2:
        st.markdown("🔴 SELL : WEEK LOW + EMA20 < EMA50")
        if weekly_ema_sell.empty:
            st.info("No Weekly EMA SELL confirmations")
        else:
            st.dataframe(weekly_ema_sell, width="stretch")


with tabs[6]:
    #st.dataframe(df[df.LTP >= df.HIGH_M])
    #st.dataframe(df[df.LTP <= df.LOW_M])
    st.subheader("📅 MONTHLY BREAKS – Above MONTH HIGH")
    monthly_up = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_up, width="stretch")

    st.subheader("📅 MONTHLY BREAKS – Below MONTH LOW")
    monthly_down = df.loc[df.LTP <= df.LOW_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_down, width="stretch")


with tabs[7]:
    #st.markdown('<div class="section-yelLIVE_LOW"><b>⚡ LIVE_OPEN = LIVE_HIGH / LIVE_LOW (Trend Day)</b></div>', unsafe_allow_html=True )
    st.subheader("🔥 O=H / O=L Setups (Gainers + Losers)")

    st.dataframe(
        ol_oh_df,
        #width='content'
        width="stretch",
        height=min(3200, 60 + len(ol_oh_df) * 35)
    )
    col1, col2 = st.columns(2)

    # -------- LIVE_OPEN = LIVE_LOW (Bullish) --------
    with col1:
        st.markdown("### 🟢 OPEN==LOW ")
        if LIVE_OPEN_LIVE_LOW_df.empty:
            st.info("No OPEN==LOW stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_LOW_df,
                width="stretch",
                height=min(3000, 60 + len(LIVE_OPEN_LIVE_LOW_df) * 35)
            )

    # -------- LIVE_OPEN = LIVE_HIGH (Bearish) --------
    with col2:
        st.markdown("### 🔴 OPEN==HIGH ")
        if LIVE_OPEN_LIVE_HIGH_df.empty:
            st.info("No OPEN==HIGH stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_HIGH_df,
                width="stretch",
                height=min(3000, 60 + len(LIVE_OPEN_LIVE_HIGH_df) * 35)
            )


#with tabs[8]:
 #   st.markdown('<div class="section-purple"><b>📉 EMA20–EMA50 + Breakout</b></div>', unsafe_allow_html=True)

  #  if ema_signal_df.empty:
   #     st.info("No EMA20–EMA50 signals currently")
   # else:
    #    st.dataframe(
     #       ema_signal_df,
      #      width="stretch",
       #     height=min(1200, 60 + len(ema_signal_df) * 35)
       # )

with tabs[8]:
    st.markdown(
        '<div class="section-green"><b>🟢 EMA20–EMA50 BUY (Breakout)</b></div>',
        unsafe_allow_html=True
    )

    if ema_buy_df.empty:
        st.info("No EMA20–EMA50 BUY signals")
    else:
        st.dataframe(
            ema_buy_df,
            width="stretch",
            height=min(2000, 60 + len(ema_buy_df) * 35)
        )

    st.markdown(
        '<div class="section-red"><b>🔴 EMA20–EMA50 SELL (Breakdown)</b></div>',
        unsafe_allow_html=True
    )

    if ema_sell_df.empty:
        st.info("No EMA20–EMA50 SELL signals")
    else:
        st.dataframe(
            ema_sell_df,
            width="stretch",
            height=min(2000, 60 + len(ema_sell_df) * 35)
        )


with tabs[9]:

    st.subheader("🎯 Intraday Supertrend – Near LTP Zone")
    #st.dataframe(st_near_intraday, use_container_width=True)
    st.dataframe(
        st_near_intraday.style.background_gradient(
            subset=["ST_DIST_%"],
            cmap="RdYlGn"
        ),
        use_container_width=True
    )
    threshold = st.slider(
        "Max distance from Supertrend (%)",
        0.2, 2.0, 0.7, 0.1
    )

    st_near_intraday = st_near_intraday[
        st_near_intraday["ST_DIST_%"].abs() <= threshold
    ]

    st.markdown("---")
    st.subheader("📈 Daily Supertrend (Trend Filter)")
    st.dataframe(daily_st_view, use_container_width=True)
    




with tabs[10]:  # assuming INFO is last tab

    #st.markdown("### 🟢 Top Gainers ( > +2.5% )")

    #if gainers_df.empty:
     #   st.info("No gainers above 2.5%")
    #else:
     #   st.dataframe(gainers_df, use_container_width=True)

    #st.markdown("---")  # separator line

    #st.markdown("### 🔴 Top Losers ( < -2.5% )")

    #if losers_df.empty:
     #   st.info("No losers below -2.5%")
    #else:
     #   st.dataframe(losers_df, use_container_width=True)
    TOP_GAINER_COLS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "LIVE_VOLUME",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "NEAR",
    "GAIN",
    "TOP_HIGH",
    "TOP_LOW",

    ]
    TOP_GAINER_COLS = [c for c in TOP_GAINER_COLS if c in gainers_df.columns]
    with st.expander("🟢 Top Gainers ( > +2.5% )", expanded=True):
        #st.dataframe(gainers_df, use_container_width=True)
        st.dataframe(
            gainers_df[TOP_GAINER_COLS],
            #use_container_width=True
            width="stretch",
            height=min(2200, 60 + len(gainers_df) * 35)
        )

    TOP_LOSER_COLS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "LIVE_VOLUME",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "NEAR",
    "GAIN",
    "TOP_HIGH",
    "TOP_LOW",

    ]
    TOP_LOSER_COLS = [c for c in TOP_LOSER_COLS if c in losers_df.columns]
    with st.expander("🔴 Top Losers ( < -2.5% )", expanded=True):
        #st.dataframe(losers_df, use_container_width=True)
        st.dataframe(
            losers_df[TOP_LOSER_COLS],
            #use_container_width=True
            width="stretch",
            height=min(2200, 60 + len(losers_df) * 35)
        )


with tabs[11]:  # 4 BAR
    st.markdown("### 🔁 4 BAR Reversal + Breakout")

    if four_bar_df.empty:
        st.info("No 4-bar setups today")
    else:
        st.dataframe(four_bar_df, width="stretch")

    ###############################
    st.markdown("### ⚠️ Fake 2.5% Breakouts")

    col1, col2 = st.columns(2)

    # -------- BULL TRAP --------
    with col1:
        st.markdown("#### 🟡 Fake Bull Trap")
        if fake_bull_df.empty:
            st.info("No Fake Bull Traps")
        else:
            st.dataframe(
                fake_bull_df
                .sort_values("FAIL_%", ascending=False)
                .style.apply(style_ltp_only, axis=1),
                width="stretch",
                height=min(2000, 60 + len(fake_bull_df) * 35)
        )


    # -------- BEAR TRAP --------
    with col2:
        st.markdown("#### 🔵 Fake Bear Trap")
        if fake_bear_df.empty:
            st.info("No Fake Bear Traps")
        else:
            st.dataframe(
                fake_bear_df
                .sort_values("FAIL_%", ascending=False)
                .style.apply(style_ltp_bear_only, axis=1),
                width="stretch",
                height=min(2000, 60 + len(fake_bear_df) * 35)
        )

    ##############################################
    st.markdown("### ⏱️ 15-Min Inside Range Break")

    if inside_15m_df.empty:
        st.info("No 15-min inside range breaks yet")
    else:
        st.dataframe(
            inside_15m_df
                .sort_values("CHANGE_%", ascending=False)
                .style.apply(style_ltp_15min, axis=1),
            width="stretch",
            height=min(2000, 60 + len(inside_15m_df) * 35)
        )

    #######################################################
    st.markdown(
        '<div class="section-blue"><b>🚀 YH1.5 Strong Breakout (Screener)</b></div>',
        unsafe_allow_html=True
    )

    if yh15_df.empty:
        st.info("No valid YH1.5 breakouts (gap-ups filtered)")
    else:
        st.dataframe(
            yh15_df,
            width='content'
        )
    ##################################################################
    st.subheader("⚠️ Fake / Failed YH1.5 Breakouts")

    if fake_yh15_df.empty:
        st.info("No failed YH1.5 breakouts currently")
    else:
        st.dataframe(fake_yh15_df, width='content')




with tabs[12]:  # adjust index if needed
    st.markdown(
        '<div class="section-purple"><b>🧠 OPTIONS – Strong Buy Signals Only</b></div>',
        unsafe_allow_html=True
    )
    st.markdown("### 🔎 Option Scan Summary")

    st.write(
        df[["Symbol","OPTION_SCORE","OPTION_SIGNAL"]]
        .sort_values("OPTION_SCORE", ascending=False)
        .head(10)
    )

    # ---- FILTER ONLY STRONG OPTION SETUPS ----
    options_df = df[
        df["OPTION_SIGNAL"].str.contains("STRONG", na=False)
    ].copy()

    if options_df.empty:
        st.info("No STRONG option setups at the moment")
    else:
        # ---- SORT: BEST QUALITY FIRST ----
        options_df = options_df.sort_values(
            by=["OPTION_SCORE", "GAIN"],
            ascending=[False, True]
        )

        # ---- SPLIT CE / PE ----
        ce_df = options_df[
            options_df["OPTION_SIGNAL"] == "STRONG CE BUY"
        ].copy()

        pe_df = options_df[
            options_df["OPTION_SIGNAL"] == "STRONG PE BUY"
        ].copy()

        col1, col2 = st.columns(2)

        # ================= CE BUY =================
        with col1:
            st.markdown("### 🟢 STRONG CE BUY")

            if ce_df.empty:
                st.info("No STRONG CE setups")
            else:
                st.dataframe(
                    ce_df[
                        [
                            "Symbol",
                            "LTP",
                            "TOP_HIGH",
                            "EMA20",
                            "EMA50",
                            "SUPERTREND",
                            "VWAP",
                            "GAIN",
                            "OPTION_SCORE",
                            "STRIKE_PREF",
                            "OPTION_REASON"
                        ]
                    ],
                    width="stretch",
                    height=min(3200, 60 + len(ce_df) * 35)
                )

        # ================= PE BUY =================
        with col2:
            st.markdown("### 🔴 STRONG PE BUY")

            if pe_df.empty:
                st.info("No STRONG PE setups")
            else:
                st.dataframe(
                    pe_df[
                        [
                            "Symbol",
                            "LTP",
                            "TOP_LOW",
                            "EMA20",
                            "EMA50",
                            "SUPERTREND",
                            "VWAP",
                            "GAIN",
                            "OPTION_SCORE",
                            "STRIKE_PREF",
                            "OPTION_REASON"
                        ]
                    ],
                    width="stretch",
                    height=min(3200, 60 + len(pe_df) * 35)
                )

    st.caption(
        "🧠 Logic: Spot trend + breakout/near + VWAP + EMA + Supertrend. "
        "Only score ≥ 8 shown. Designed to avoid option decay & heavy writing zones."
    )


with tabs[13]:
    st.subheader("📊 NSE INDICES – LIVE")

    st.dataframe(
        indices_df[
            [
                "Symbol",
                "LTP",
                "OPEN",
                "HIGH",
                "LOW",
                "CHANGE",
                "CHANGE_%",
                "YEST_HIGH",
                "YEST_LOW",
                "YEST_CLOSE",
            ]
        ],
         width='content'
    )

    st.subheader("📊 Index-wise Top Gainers (Live)")

    for index_name, symbols in index_symbols.items():

        idx_df = df[df["Symbol"].isin(symbols)].copy()

        if idx_df.empty:
            continue

        idx_df = idx_df[
            [
                "Symbol",
                "LTP",
                "LIVE_OPEN",
                "LIVE_HIGH",
                "LIVE_LOW",
                "CHANGE",
                "CHANGE_%",
                "YEST_HIGH",
                "YEST_LOW",
                "YEST_CLOSE",
            ]
        ].sort_values("CHANGE_%", ascending=False)

        st.markdown(f"### 🔹 {index_name}")

        st.dataframe(
            idx_df,
            width='content'
        )

with tabs[14]:  # adjust index as per your tabs list

    st.subheader("🟢 3 × 15-Min Green Candles (Still Valid)")

    if three_green_15m_df.empty:
        st.info("No symbols currently maintaining 3 consecutive green 15-min candles")
    else:
        st.dataframe(
            three_green_15m_df.sort_values("CANDLE_TIME"),
            use_container_width=True
        )

with tabs[15]:
    st.subheader("🚨 Alerts Log (Static)")

    if not os.path.exists(ALERTS_LOG_FILE):
        st.info("No alerts logged yet.")
    else:
        #alerts_df = pd.read_csv(ALERTS_LOG_FILE)
        alerts_df = pd.read_csv(ALERTS_LOG_FILE)
        alerts_df = alerts_df.sort_values(
            by=["DATE", "TIME"],
            ascending=False
        )

        st.dataframe(
            alerts_df,
            width="stretch",
            height=min(4200, 60 + len(alerts_df) * 32)
        )

        st.caption("📌 Latest alerts appear at the top. Data is static and will not change.")


with tabs[16]:
    #st.write("✔ Holiday aware yesterday logic")
    #st.write("✔ Excel matched")
    #st.write("✔ No features removed")

    st.divider()
    st.subheader("📤 Export Dashboard Data")

    excel_file = export_all_tabs_to_excel()

    st.download_button(
        label="📥 Download Full Dashboard (Excel)",
        data=excel_file,
        file_name=f"Panchak_Dashboard_{datetime.now(IST).strftime('%d_%b_%Y_%H%M')}.xlsx",
        mime="application/vnd.LIVE_OPENxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.write(" I . when it is TOP LIVE_HIGH breaks or NEAR LIVE_HIGH value - just check the Entity is DAILY and WEEKLY UPTREND AND SUPER TREND SHOULD IN BUY MODE in DAILY TREND- THEN ONLY ENTER - same as SELL ViceVersa")
    st.write(" II. when you take position - always decide your SL and immediatly put StopLoss - IF STOP LOSS HITS - Dont touch for that day or reenter : we will get multiple Chances in coming days and we have lot of entities")
    st.write(" III. whenever Entity breaks TOP LIVE_HIGH (in buy entry) and returns and SL hits - There is a possibility to REVERSE and you will get sell side opportunity- some wiered cases both sides SL hits. dont touch that time")
    st.write(" IV. Take only one or two lots and keep some money with you, other wise when sudden dips you will have a chance to average in worst cases, other wise losses will be huge, if we average at least  can exit with minimal losses")
    st.write(" V. when prices beLIVE_LOW of panchak LIVE_LOW and not moved punchak up side, dont carry longs until it comes above TOP LIVE_HIGH , same for sell side as well")
    st.write(" VI. take stock positions based on INDICES TOP_HIGH and TOP_LOW as NIFTY controls most movements. ")

    
