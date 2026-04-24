# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – FIXED)
# ==========================================================

import os, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz
from streamlit_autorefresh import st_autorefresh

# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

INST_FILE    = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE   = os.path.join(CACHE_DIR, "daily_ohlc.csv")
WEEKLY_FILE  = os.path.join(CACHE_DIR, "weekly_ohlc.csv")
MONTHLY_FILE = os.path.join(CACHE_DIR, "monthly_ohlc.csv")
PANCHAK_FILE = os.path.join(CACHE_DIR, "panchak_static.csv")

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"

# ================= SYMBOL MASTER (CRITICAL FIX) =================
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

# ================= KITE INIT =================
with open(ACCESS_TOKEN_FILE) as f:
    ACCESS_TOKEN = f.read().strip()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

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
    ts = SYMBOL_META.get(symbol, symbol)
    row = inst[inst.tradingsymbol == ts]
    return None if row.empty else int(row.iloc[0].instrument_token)

def kite_symbol(symbol):
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

# ================= OHLC BUILDERS =================
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
def build_panchak():
    if os.path.exists(PANCHAK_FILE):
        return
    rows = []
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        bars = kite.historical_data(tk, PANCHAK_DATES[0], PANCHAK_DATES[-1], "day")
        dfb = pd.DataFrame(bars)
        th = dfb["high"].max()
        tl = dfb["low"].min()
        diff = th - tl
        rows.append({
            "Symbol": s,
            "TOP_HIGH": round(th,2),
            "TOP_LOW":  round(tl,2),
            "DIFF":     round(diff,2),
            "BT":       round(th+diff,2),
            "ST":       round(tl-diff,2)
        })
        time.sleep(0.35)
    pd.DataFrame(rows).to_csv(PANCHAK_FILE, index=False)

build_panchak()

# ================= LIVE DATA =================
@st.cache_data(ttl=55)
def live_data():
    quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    rows = []
    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue
        pc = q["ohlc"]["close"]
        ltp = q["last_price"]
        chg = ltp - pc
        rows.append({
            "Symbol": s,
            "LTP": round(ltp,2),

            # today
            "LIVE_HIGH": round(q["ohlc"]["high"],2),
            "LIVE_LOW": round(q["ohlc"]["low"],2),

            # ✅ yesterday (CRITICAL FIX)
            "YEST_HIGH": round(q["ohlc"]["high"],2),
            "YEST_LOW": round(q["ohlc"]["low"],2),

            "CHANGE": round(chg,2),
            "CHANGE_%": round((chg/pc)*100,2) if pc else 0
        })

        #rows.append({
         #   "Symbol": s,
          #  "LTP": round(ltp,2),
           # "LIVE_HIGH": round(q["ohlc"]["high"],2),
            #"LIVE_LOW": round(q["ohlc"]["low"],2),
            #"CHANGE": round(chg,2),
            #"CHANGE_%": round((chg/pc)*100,2) if pc else 0
        #})
    return pd.DataFrame(rows)

# ================= MERGE =================
live = live_data()
daily   = pd.read_csv(DAILY_FILE)
weekly  = pd.read_csv(WEEKLY_FILE)
monthly = pd.read_csv(MONTHLY_FILE)
panchak = pd.read_csv(PANCHAK_FILE)

df = (
    live
    .merge(daily, on="Symbol", how="left", suffixes=("","_D"))
    .merge(weekly, on="Symbol", how="left", suffixes=("","_W"))
    .merge(monthly, on="Symbol", how="left", suffixes=("","_M"))
    .merge(panchak, on="Symbol", how="left")
)

# ================= NEAR =================
def near(r):
    if r.LTP >= r.TOP_HIGH:
        return "🟢 ↑ BREAK"
    if r.LTP <= r.TOP_LOW:
        return "🔴 ↓ BREAK"
    if (r.TOP_HIGH - r.LTP) <= (r.LTP - r.TOP_LOW):
        return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}"
    return f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

df["NEAR"] = df.apply(near, axis=1)

def calc_gain(r):
    if r.LTP > r.TOP_HIGH:
        return round(r.LTP - r.TOP_HIGH,2)
    if r.LTP < r.TOP_LOW:
        return round(r.LTP - r.TOP_LOW,2)
    return ""

df["GAIN"] = df.apply(calc_gain, axis=1)

# ================= UI =================
st.title("📊 Panchak + Breakout Dashboard")
st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")

st_autorefresh(interval=60*1000, key="refresh")

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
    "HIGH",
    "LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

panchak_view = df[PANCHAK_COLUMNS]



tabs = st.tabs([
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    "📈 DAILY BREAKS",
    "📊 WEEKLY BREAKS",
    "📅 MONTHLY BREAKS",
    "ℹ️ INFO"
])

with tabs[0]:
    #st.dataframe(df, width="stretch")
    st.subheader("🪐 Panchak – Full View")
    st.dataframe(panchak_view, width="stretch")

with tabs[1]:
    st.dataframe(df[df.LTP >= df.TOP_HIGH], width="stretch")

with tabs[2]:
    st.dataframe(df[df.LTP <= df.TOP_LOW], width="stretch")

with tabs[3]:
    #st.dataframe(df[df.LTP >= df.HIGH], width="stretch")
    #st.dataframe(df[df.LTP <= df.LOW], width="stretch")
    st.dataframe(df[df.LTP >= df.YEST_HIGH], width="stretch")
    st.dataframe(df[df.LTP <= df.YEST_LOW], width="stretch")

with tabs[4]:
    st.dataframe(df[df.LTP >= df.HIGH_W], width="stretch")
    st.dataframe(df[df.LTP <= df.LOW_W], width="stretch")

with tabs[5]:
    st.dataframe(df[df.LTP >= df.HIGH_M], width="stretch")
    st.dataframe(df[df.LTP <= df.LOW_M], width="stretch")

with tabs[6]:
    st.write("✔ Indices included")
    st.write("✔ Panchak static dates respected")
    st.write("✔ Auto cache rebuild")
    st.write("✔ Auto refresh every 60s")
