# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – HOLIDAY AWARE)
# ==========================================================

import os, time
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


EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO = ["uppala.wla@gmail.com"]


# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
st.markdown("""
<style>
/* Section headers */
.section-green {background:#e8f5e9;padding:8px;border-radius:6px;}
.section-red {background:#fdecea;padding:8px;border-radius:6px;}
.section-yellow {background:#fff8e1;padding:8px;border-radius:6px;}
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
    "RELIANCE","INFY","HCLTECH","TVSMOTOR","BHARATFORG","JUBLFOOD",
    "LAURUSLABS","SUNPHARMA","TATACONSUM","COFORGE","ASIANPAINT",
    "MUTHOOTFIN","CHOLAFIN","BSE","GRASIM","ACC","ADANIENT","BHARTIARTL",
    "BIOCON","BRITANNIA","DIVISLAB","ESCORTS","JSWSTEEL","M&M","PAGEIND",
    "SHREECEM","BOSCHLTD","DIXON","MARUTI","ULTRACEMCO","APOLLOHOSP","MCX",
    "POLYCAB","PERSISTENT","TRENT","EICHERMOT","HAL","TIINDIA","SIEMENS",
    "GAIL","NATIONALUM","TATASTEEL","MOTHERSON","SHRIRAMFIN","VEDL","VBL",
    "GRANULES","LICHSGFIN","UPL","ANGELONE","INDHOTEL","APLAPOLLO","CAMS",
    "CUMMINSIND","MAXHEALTH","POLICYBZR","HAVELLS","GLENMARK","ADANIPORTS",
    "SRF","CDSL","TITAN","SBILIFE","COLPAL","HDFCLIFE","VOLTAS","NAUKRI",
    "TATACHEM","KALYANKJIL"
]

SYMBOLS = ["NIFTY", "BANKNIFTY"] + STOCKS

# ================= PANCHAK STATIC DATES =================
PANCHAK_DATES = [
    date(2026,1,21),
    date(2026,1,22),
    date(2026,1,23),
    date(2026,1,24),
    date(2026,1,25),
]

#PANCHAK_DATES = [
 #   date(2025,12,24),
  #  date(2025,12,25),
   # date(2025,12,26),
    #date(2025,12,27),
    #date(2025,12,29),
#]

PANCHAK_START = date(2026, 1, 21)
PANCHAK_END   = date(2026, 1, 25)

PANCHAK_DATA_FILE = os.path.join(CACHE_DIR, "panchak_data.csv")
PANCHAK_META_FILE = os.path.join(CACHE_DIR, "panchak_meta.csv")


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
    return round(b["high"],2), round(b["low"],2), round(b["close"],2)   # yest close added here

# ================= OHLC BUILDERS (UNCHANGED) =================
def build_period_file(path, days):
    if os.path.exists(path):
        return
    rows = []
    today = date.today()
    start = today - timedelta(days=days)
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        bars = kite.historical_data(tk, start, today, "day")
        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol": s,
            "HIGH": round(dfb["high"].max(),2),
            "LOW":  round(dfb["low"].min(),2)
        })
        time.sleep(0.35)
    pd.DataFrame(rows).to_csv(path, index=False)

build_period_file(DAILY_FILE, 7)
build_period_file(WEEKLY_FILE, 14)
build_period_file(MONTHLY_FILE, 40)

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

        time.sleep(0.35)

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
            "Symbol","LTP","OPEN","LIVE_HIGH","LIVE_LOW",
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
            "OPEN": round(q["ohlc"]["open"], 2),
            "LIVE_HIGH": round(q["ohlc"]["high"], 2),
            "LIVE_LOW": round(q["ohlc"]["low"], 2),
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

        start_day = end_day - timedelta(days=90)  # buffer to get 60 trading days

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
                    "open": r["open"],
                    "high": r["high"],
                    "low":  r["low"],
                    "close": r["close"]
                })

            time.sleep(0.3)

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
            "open": b["open"],
            "high": b["high"],
            "low":  b["low"],
            "close": b["close"]
        })

        time.sleep(0.25)

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



ema_signal_df = df.loc[
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



# ================= UI =================
#st.title("📊 Panchak + Breakout Dashboard")
st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")
st_autorefresh(interval=60_000, key="refresh")

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

def export_all_tabs_to_excel():
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        panchak_view.to_excel(writer, sheet_name="PANCHAK", index=False)
        top_high_df.to_excel(writer, sheet_name="TOP_HIGH", index=False)
        top_low_df.to_excel(writer, sheet_name="TOP_LOW", index=False)

        daily_up.to_excel(writer, sheet_name="DAILY_UP", index=False)
        daily_down.to_excel(writer, sheet_name="DAILY_DOWN", index=False)

        weekly_up.to_excel(writer, sheet_name="WEEKLY_UP", index=False)
        weekly_down.to_excel(writer, sheet_name="WEEKLY_DOWN", index=False)

        monthly_up.to_excel(writer, sheet_name="MONTHLY_UP", index=False)
        monthly_down.to_excel(writer, sheet_name="MONTHLY_DOWN", index=False)

    output.seek(0)
    return output



# ================= BREAKOUT DATA (GLOBAL) =================

top_high_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
top_low_df  = df.loc[df.LTP <= df.TOP_LOW,  TOP_LOW_COLUMNS]

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

new_top_high = detect_new_entries(
    "TOP_HIGH",
    top_high_df.Symbol.tolist()
)

notify_browser("🟢 New TOP HIGH", new_top_high)

new_top_low = detect_new_entries(
    "TOP_LOW",
    top_low_df.Symbol.tolist()
)

notify_browser("🔴 New TOP LOW", new_top_low)

def send_email(subject, body):
    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_TO)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_FROM, EMAIL_PASS)
        smtp.send_message(msg)
if new_top_high:
    send_email(
        "TOP HIGH Breakout Alert",
        "New TOP HIGH:\n" + "\n".join(new_top_high)
    )
  
def notify_all(name, title, symbols):
    """
    name   : unique key (used for cache file)
    title  : message title
    symbols: list of symbols
    """
    if not symbols:
        return

    # 📢 Browser notification
    st.toast(f"{title}: {', '.join(symbols)}", icon="🚨")

    # 📧 Email notification
    send_email(
        subject=f"{title} Alert",
        body=f"{title}\n\n" + "\n".join(symbols)
    )
new_top_high = detect_new_entries(
    "TOP_HIGH",
    top_high_df.Symbol.tolist()
)

notify_all(
    "TOP_HIGH",
    "🟢 TOP HIGH Breakout",
    new_top_high
)
new_top_low = detect_new_entries(
    "TOP_LOW",
    top_low_df.Symbol.tolist()
)

notify_all(
    "TOP_LOW",
    "🔴 TOP LOW Breakdown",
    new_top_low
)
new_ema = detect_new_entries(
    "EMA20_50",
    ema_signal_df.Symbol.tolist()
)

notify_all(
    "EMA20_50",
    "⚡ EMA20–EMA50 Signal",
    new_ema
)
new_daily_up = detect_new_entries(
    "DAILY_UP",
    daily_up.Symbol.tolist()
)

notify_all(
    "DAILY_UP",
    "📈 DAILY High Break",
    new_daily_up
)

new_daily_down = detect_new_entries(
    "DAILY_DOWN",
    daily_down.Symbol.tolist()
)

notify_all(
    "DAILY_DOWN",
    "📉 DAILY Low Break",
    new_daily_down
)
new_weekly_up = detect_new_entries(
    "WEEKLY_UP",
    weekly_up.Symbol.tolist()
)

notify_all(
    "WEEKLY_UP",
    "📊 WEEKLY High Break",
    new_weekly_up
)

new_weekly_down = detect_new_entries(
    "WEEKLY_DOWN",
    weekly_down.Symbol.tolist()
)

notify_all(
    "WEEKLY_DOWN",
    "📉 WEEKLY Low Break",
    new_weekly_down
)
new_monthly_up = detect_new_entries(
    "MONTHLY_UP",
    monthly_up.Symbol.tolist()
)

notify_all(
    "MONTHLY_UP",
    "📅 MONTHLY High Break",
    new_monthly_up
)

new_monthly_down = detect_new_entries(
    "MONTHLY_DOWN",
    monthly_down.Symbol.tolist()
)

notify_all(
    "MONTHLY_DOWN",
    "📉 MONTHLY Low Break",
    new_monthly_down
)


ohl_df = df.loc[
    (df.OPEN == df.LIVE_HIGH) | (df.OPEN == df.LIVE_LOW),
    [
        "Symbol",
        "OPEN",
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
    lambda r: "🔴 OPEN = HIGH" if r.OPEN == r.LIVE_HIGH else "🟢 OPEN = LOW",
    axis=1
)

# ================= OPEN = HIGH / LOW SPLIT =================

open_low_df = pd.DataFrame()
open_high_df = pd.DataFrame()

if not ohl_df.empty and "TYPE" in ohl_df.columns:
    open_low_df = ohl_df[ohl_df["TYPE"] == "OPEN=LOW"]
    open_high_df = ohl_df[ohl_df["TYPE"] == "OPEN=HIGH"]



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

gainers_df = (
    df[df["CHANGE_%"] >= 2.5]
    .sort_values("CHANGE_%", ascending=False)
    .loc[:, [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_HIGH",
        "LIVE_LOW"
    ]]
)

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

new_gainers = detect_new_entries(
    "TOP_GAINERS",
    gainers_df.Symbol.tolist()
)

notify_all(
    "TOP_GAINERS",
    "🔥 Top Gainers > 2.5%",
    new_gainers
)

new_losers = detect_new_entries(
    "TOP_LOSERS",
    losers_df.Symbol.tolist()
)

notify_all(
    "TOP_LOSERS",
    "🔥 Top LOSERS < -2.5%",
    new_losers
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

# ================= DAILY + EMA ALERTS =================

new_daily_ema_buy = detect_new_entries(
    "DAILY_EMA_BUY",
    daily_ema_buy.Symbol.tolist()
)

notify_all(
    "DAILY_EMA_BUY",
    "📈 DAILY EMA BUY (YH + EMA20>EMA50)",
    new_daily_ema_buy
)


new_daily_ema_sell = detect_new_entries(
    "DAILY_EMA_SELL",
    daily_ema_sell.Symbol.tolist()
)

notify_all(
    "DAILY_EMA_SELL",
    "📉 DAILY EMA SELL (YL + EMA20<EMA50)",
    new_daily_ema_sell
)

# ================= WEEKLY + EMA ALERTS =================

new_weekly_ema_buy = detect_new_entries(
    "WEEKLY_EMA_BUY",
    weekly_ema_buy.Symbol.tolist()
)

notify_all(
    "WEEKLY_EMA_BUY",
    "📊 WEEKLY EMA BUY (WH + EMA20>EMA50)",
    new_weekly_ema_buy
)


new_weekly_ema_sell = detect_new_entries(
    "WEEKLY_EMA_SELL",
    weekly_ema_sell.Symbol.tolist()
)

notify_all(
    "WEEKLY_EMA_SELL",
    "📉 WEEKLY EMA SELL (WL + EMA20<EMA50)",
    new_weekly_ema_sell
)

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
    df["ATR"] = df["TR"].ewm(span=period, adjust=False).mean()

    mid = (df["high"] + df["low"]) / 2
    df["UpperBand"] = mid + multiplier * df["ATR"]
    df["LowerBand"] = mid - multiplier * df["ATR"]

    df["SuperTrend"] = np.nan
    df["ST_DIR"] = None

    for i in range(1, len(df)):
        if df.loc[i, "close"] > df.loc[i - 1, "UpperBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "LowerBand"]
            df.loc[i, "ST_DIR"] = "BUY"

        elif df.loc[i, "close"] < df.loc[i - 1, "LowerBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "UpperBand"]
            df.loc[i, "ST_DIR"] = "SELL"

        else:
            df.loc[i, "SuperTrend"] = df.loc[i - 1, "SuperTrend"]
            df.loc[i, "ST_DIR"] = df.loc[i - 1, "ST_DIR"]

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


# =========================================================
# SUPERTREND ALERTS (NEW BUY / SELL ENTRIES)
# =========================================================

st_buy_symbols = supertrend_df[
    supertrend_df["ST_BUY"] == "BUY"
]["Symbol"].tolist()

st_sell_symbols = supertrend_df[
    supertrend_df["ST_SELL"] == "SELL"
]["Symbol"].tolist()

# Detect new BUY signals
new_st_buy = detect_new_entries(
    "SUPERTREND_BUY",
    st_buy_symbols
)

# Detect new SELL signals
new_st_sell = detect_new_entries(
    "SUPERTREND_SELL",
    st_sell_symbols
)

# Notify
notify_all(
    "SUPERTREND_BUY",
    "🟢 SuperTrend BUY Signal",
    new_st_buy
)

notify_all(
    "SUPERTREND_SELL",
    "🔴 SuperTrend SELL Signal",
    new_st_sell
)




tabs = st.tabs([
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    "🟡 NEAR",
    "📈 DAILY BREAKS",
    "📊 WEEKLY BREAKS",
    "📅 MONTHLY BREAKS",
    "⚡ OPEN=HIGH=LOW",  
    "📉 EMA20-50",
    "📈 SUPERTREND",
    "🔥 TOP GAINERS/LOSERS",     
    "ℹ️ INFO"
])

with tabs[0]:
    #st.dataframe(df, width="stretch")
    #st.markdown('<div class="section-green"><b>🟢 TOP HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    st.subheader("🪐 Panchak – Full View")
    st.dataframe(panchak_view, width="stretch",height=2800)

with tabs[1]:
    #st.dataframe(df[df.LTP >= df.TOP_HIGH])
    ##st.subheader(" TOP_HIGH_view – Full View")
    #st.dataframe(TOP_HIGH_view, width="stretch")
    #1
    #top_high_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]

    #if top_high_df.empty:
     #   st.info("No TOP HIGH breakouts yet")
    #else:
     #   st.subheader("🟢 TOP HIGH – Breakouts")
      #  st.dataframe(top_high_df, width="stretch")

    #2
    st.markdown('<div class="section-green"><b>🟢 TOP HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    top_high_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]

    #st.subheader("🟢 TOP HIGH – Breakouts")
    st.dataframe(top_high_df, width="stretch",height=2000)

with tabs[2]:
    #st.dataframe(df[df.LTP <= df.TOP_LOW])
    st.markdown('<div class="section-red"><b>🔴 TOP LOW – Breakdowns</b></div>', unsafe_allow_html=True)
    top_low_df = df.loc[df.LTP <= df.TOP_LOW, TOP_LOW_COLUMNS]

    #st.subheader("🟢 TOP LOW – Breakouts")
    st.dataframe(top_low_df, width="stretch",height=2000)

with tabs[3]:
    #st.subheader("📈 NEAR (Between TOP_LOW & TOP_HIGH)")
    st.markdown('<div class="section-yellow"><b>🟡 NEAR – Watch Zone</b></div>', unsafe_allow_html=True)
    near_df = df.loc[
        (df.LTP > df.TOP_LOW) & (df.LTP < df.TOP_HIGH),
        [
            "Symbol",
            "LTP",
            "TOP_HIGH",
            "TOP_LOW",
            "NEAR",
            "LIVE_HIGH",
            "LIVE_LOW",
            "CHANGE",
            "CHANGE_%",
            "YEST_HIGH",
            "YEST_LOW"
        ]
    ]

    if near_df.empty:
        st.info("No stocks currently between TOP_HIGH and TOP_LOW")
    else:
        row_height = 35
        table_height = min(2000, 60 + len(near_df) * row_height)

        st.dataframe(
            near_df,
            width="stretch",
            height=table_height
        )
    near_df = near_df.sort_values(by="NEAR")
    near_df["DIST_%"] = ((near_df.TOP_HIGH - near_df.LTP) / near_df.LTP * 100).round(2)

    
with tabs[4]:
    st.subheader("📈 DAILY BREAKS – Above YEST HIGH")
    st.dataframe(daily_up, width="stretch")

    st.subheader("📉 DAILY BREAKS – Below YEST LOW")
    st.dataframe(daily_down, width="stretch")

    st.divider()

    st.markdown("### ✅ DAILY + EMA CONFIRMATION (High Probability)")

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

    st.subheader("📊 WEEKLY BREAKS – Below WEEK LOW")
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
    #st.markdown('<div class="section-yellow"><b>⚡ OPEN = HIGH / LOW (Trend Day)</b></div>', unsafe_allow_html=True )

    col1, col2 = st.columns(2)

    # -------- OPEN = LOW (Bullish) --------
    with col1:
        st.markdown("### 🟢 OPEN = LOW ")
        if open_low_df.empty:
            st.info("No OPEN = LOW stocks today")
        else:
            st.dataframe(
                open_low_df,
                width="stretch",
                height=min(900, 60 + len(open_low_df) * 35)
            )

    # -------- OPEN = HIGH (Bearish) --------
    with col2:
        st.markdown("### 🔴 OPEN = HIGH ")
        if open_high_df.empty:
            st.info("No OPEN = HIGH stocks today")
        else:
            st.dataframe(
                open_high_df,
                width="stretch",
                height=min(900, 60 + len(open_high_df) * 35)
            )


with tabs[8]:
    st.markdown('<div class="section-purple"><b>📉 EMA20–EMA50 + Breakout</b></div>', unsafe_allow_html=True)

    if ema_signal_df.empty:
        st.info("No EMA20–EMA50 signals currently")
    else:
        st.dataframe(
            ema_signal_df,
            width="stretch",
            height=min(1200, 60 + len(ema_signal_df) * 35)
        )


with tabs[9]:
    st.markdown(
        '<div class="section-blue"><b>📈 SUPERTREND</b></div>',
        unsafe_allow_html=True
    )

    if supertrend_df.empty:
        st.info("No SuperTrend data available")
    else:
        st.dataframe(
            supertrend_styled,
            width="stretch",
            height=min(1200, 60 + len(supertrend_df) * 35)
        )



with tabs[10]:  # assuming INFO is last tab
    #st.subheader("🔥 TOP GAINERS & LOSERS (±2.5%)")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🟢 Top Gainers ( > +2.5% )")
        if gainers_df.empty:
            st.info("No gainers above 2.5%")
        else:
            st.dataframe(gainers_df, width="stretch")

    with col2:
        st.markdown("### 🔴 Top Losers ( < -2.5% )")
        if losers_df.empty:
            st.info("No losers below -2.5%")
        else:
            st.dataframe(losers_df, width="stretch")


with tabs[11]:
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
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.write(" I . when it is TOP HIGH breaks or NEAR high value - just check the Entity is DAILY and WEEKLY UPTREND AND SUPER TREND SHOULD IN BUY MODE in DAILY TREND- THEN ONLY ENTER - same as SELL ViceVersa")
    st.write(" II. when you take position - always decide your SL and immediatly put StopLoss - IF STOP LOSS HITS - Dont touch for that day or reenter : we will get multiple Chances in coming days and we have lot of entities")
    st.write(" III. whenever Entity breaks TOP HIGH (in buy entry) and returns and SL hits - There is a possibility to REVERSE and you will get sell side opportunity- some wiered cases both sides SL hits. dont touch that time")
    st.write(" IV. Take only one or two lots and keep some money with you, other wise when sudden dips you will have a chance to average in worst cases, other wise losses will be huge, if we average at least  can exit with minimal losses")
    st.write(" V. when prices below of panchak low and not moved punchak up side, dont carry longs until it comes above TOP HIGH , same for sell side as well")
    st.write(" VI. take stock positions based on INDICES TOP_HIGH and TOP_LOW as NIFTY controls most movements. ")

    
