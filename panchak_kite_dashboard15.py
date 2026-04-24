# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (FINAL – STABLE)
# ==========================================================

import os, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz
from streamlit_autorefresh import st_autorefresh

# ================= CONFIG =================
st.set_page_config("Panchak Dashboard", layout="wide")
IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = os.getcwd()
CACHE = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE, exist_ok=True)

INST_FILE = f"{CACHE}/instruments_NSE.csv"
PANCHAK_FILE = f"{CACHE}/panchak_static.csv"
DAILY_FILE = f"{CACHE}/daily_ohlc.csv"

ACCESS_TOKEN_FILE = "access_token.txt"
API_KEY = "7am67kxijfsusk9i"

# ================= SYMBOL MASTER =================
SYMBOL_META = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK"
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

# ================= PANCHAK DATES =================
STATIC_DATES = [
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
@st.cache_data
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def get_token(sym):
    ts = SYMBOL_META.get(sym, sym)
    row = inst[inst.tradingsymbol == ts]
    return None if row.empty else int(row.iloc[0].instrument_token)

def kite_symbol(sym):
    return f"NSE:{SYMBOL_META.get(sym, sym)}"

# ================= BUILD PANCHAK =================
def build_panchak():
    if os.path.exists(PANCHAK_FILE):
        return
    rows = []
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        bars = kite.historical_data(
            tk, STATIC_DATES[0], STATIC_DATES[-1], "day"
        )
        df = pd.DataFrame(bars)
        th = df.high.max()
        tl = df.low.min()
        diff = th - tl
        rows.append({
            "Symbol": s,
            "TOP_HIGH": round(th,2),
            "TOP_LOW": round(tl,2),
            "DIFF": round(diff,2),
            "BT": round(th+diff,2),
            "ST": round(tl-diff,2)
        })
        time.sleep(0.35)
    pd.DataFrame(rows).to_csv(PANCHAK_FILE, index=False)

build_panchak()

# ================= LIVE DATA =================
@st.cache_data(ttl=55)
def fetch_live():
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
            "HIGH": round(q["ohlc"]["high"],2),
            "LOW": round(q["ohlc"]["low"],2),
            "CHANGE": round(chg,2),
            "CHANGE_%": round((chg/pc)*100,2) if pc else 0,
            "YH": round(q["ohlc"]["high"],2),
            "YL": round(q["ohlc"]["low"],2),
        })
    return pd.DataFrame(rows)

live = fetch_live()
panchak = pd.read_csv(PANCHAK_FILE)

df = live.merge(panchak, on="Symbol", how="left")

# ================= NEAR & GAIN =================
def calc_near(r):
    if r.LTP >= r.TOP_HIGH:
        return "🟢 ↑ BREAK"
    if r.LTP <= r.TOP_LOW:
        return "🔴 ↓ BREAK"
    if (r.TOP_HIGH - r.LTP) <= (r.LTP - r.TOP_LOW):
        return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}"
    return f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

def calc_gain(r):
    if r.LTP > r.TOP_HIGH:
        return round(r.LTP - r.TOP_HIGH,2)
    if r.LTP < r.TOP_LOW:
        return round(r.LTP - r.TOP_LOW,2)
    return ""

df["NEAR"] = df.apply(calc_near, axis=1)
df["GAIN"] = df.apply(calc_gain, axis=1)

# ================= FINAL PANCHAK VIEW =================
panchak_view = df[[
    "Symbol","TOP_HIGH","TOP_LOW","DIFF","BT","ST",
    "NEAR","GAIN","LTP","HIGH","LOW","CHANGE","CHANGE_%",
    "YH","YL"
]]

# ================= UI =================
st.title("📊 Panchak Dashboard")
st.caption(f"Last updated: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')}")

st_autorefresh(interval=60*1000, key="refresh")

def style_gain(v):
    if isinstance(v,(int,float)) and v>0:
        return "color:green;font-weight:bold"
    if isinstance(v,(int,float)) and v<0:
        return "color:red;font-weight:bold"
    return ""

def style_near(v):
    if "↑" in str(v):
        return "color:green;font-weight:bold"
    if "↓" in str(v):
        return "color:red;font-weight:bold"
    return ""

styled = (
    panchak_view
    .style
    .applymap(style_gain, subset=["GAIN"])
    .applymap(style_near, subset=["NEAR"])
)

st.dataframe(styled, width="stretch")
