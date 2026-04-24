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
BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

INST_FILE     = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE    = os.path.join(CACHE_DIR, "daily_ohlc.csv")
WEEKLY_FILE   = os.path.join(CACHE_DIR, "weekly_ohlc.csv")
MONTHLY_FILE  = os.path.join(CACHE_DIR, "monthly_ohlc.csv")
PANCHAK_FILE  = os.path.join(CACHE_DIR, "panchak_static.csv")

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"
IST = pytz.timezone("Asia/Kolkata")

REFRESH_SEC = 60

SYMBOL_ALIAS = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK"
}
INDEX_MAP = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK"
}

SYMBOLS = [
    "NIFTY","BANKNIFTY","RELIANCE","INFY","HCLTECH","TVSMOTOR","BHARATFORG",
    "JUBLFOOD","LAURUSLABS","SUNPHARMA","TATACONSUM","COFORGE","ASIANPAINT",
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

# Panchak static dates (EXACT)
PANCHAK_DATES = [
    date(2026,1,21),
    date(2026,1,22),
    date(2026,1,23),
    date(2026,1,24),
    date(2026,1,25),
]

# ================= KITE =================
with open(ACCESS_TOKEN_FILE) as f:
    ACCESS_TOKEN = f.read().strip()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ================= INSTRUMENTS =================
def kite_symbol(sym):
    return f"NSE:{INDEX_MAP.get(sym, sym)}"
    
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

#def token(sym):
 #   r = inst[inst.tradingsymbol == sym]
  #  return None if r.empty else int(r.iloc[0].instrument_token)

def token(symbol):
    lookup = SYMBOL_ALIAS.get(symbol, symbol)

    row = inst[
        (inst.tradingsymbol == lookup) &
        ((inst.instrument_type == "EQ") | (inst.instrument_type == "INDICES"))
    ]

    if row.empty:
        return None

    return int(row.iloc[0].instrument_token)

def token(sym):
    name = INDEX_MAP.get(sym, sym)
    row = inst[inst.tradingsymbol == name]
    return int(row.iloc[0].instrument_token)

# ================= OHLC BUILDERS =================
def build_period_ohlc(path, days):
    if os.path.exists(path):
        return
    rows = []
    today = date.today()
    start = today - timedelta(days=days)
    for s in SYMBOLS:
        tk = token(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(tk, start, today, "day")
            dfb = pd.DataFrame(bars)
            rows.append({
                "Symbol": s,
                "HIGH": dfb["high"].max(),
                "LOW": dfb["low"].min()
            })
            time.sleep(0.35)
        except:
            time.sleep(1)
    pd.DataFrame(rows).to_csv(path, index=False)

build_period_ohlc(DAILY_FILE, 7)
build_period_ohlc(WEEKLY_FILE, 14)
build_period_ohlc(MONTHLY_FILE, 40)

# ================= PANCHAK STATIC =================
def build_panchak():
    if os.path.exists(PANCHAK_FILE):
        return
    rows = []
    for s in SYMBOLS:
        tk = token(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(tk, PANCHAK_DATES[0], PANCHAK_DATES[-1], "day")
            dfb = pd.DataFrame(bars)
            th = dfb["high"].max()
            tl = dfb["low"].min()
            diff = th - tl
            rows.append({
                "Symbol": s,
                "TOP_HIGH": th,
                "TOP_LOW": tl,
                "DIFF": diff,
                "BT": th + diff,
                "ST": tl - diff
            })
            time.sleep(0.35)
        except:
            time.sleep(1)
    pd.DataFrame(rows).to_csv(PANCHAK_FILE, index=False)

build_panchak()

# ================= LIVE =================
@st.cache_data(ttl=55)
def live_data():
    q = kite.quote([f"NSE:{s}" for s in SYMBOLS])
    rows = []
    for s in SYMBOLS:
        d = q.get(f"NSE:{s}")
        if not d:
            continue
        pc = d["ohlc"]["close"]
        ltp = d["last_price"]
        chg = ltp - pc
        rows.append({
            "Symbol": s,
            "LTP": round(ltp,2),
            "LIVE_HIGH": round(d["ohlc"]["high"],2),
            "LIVE_LOW": round(d["ohlc"]["low"],2),
            "CHANGE": round(chg,2),
            "CHANGE_%": round((chg/pc)*100,2) if pc else 0
        })
    return pd.DataFrame(rows)

# ================= MERGE =================
live = live_data()
daily = pd.read_csv(DAILY_FILE)
weekly = pd.read_csv(WEEKLY_FILE)
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
        return "🚀"
    if r.LTP <= r.TOP_LOW:
        return "🔻"
    if (r.TOP_HIGH - r.LTP) <= (r.LTP - r.TOP_LOW):
        return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}"
    return f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

df["NEAR"] = df.apply(near, axis=1)

# ================= UI =================
st.set_page_config("Panchak Dashboard", layout="wide")
st.title("📊 Panchak + Breakout Dashboard")
st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")

# === AUTO REFRESH EVERY 60 SECONDS ===
st_autorefresh(
    interval=60 * 1000,   # 60 seconds
    key="auto_refresh"
)



tabs = st.tabs([
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    "📈 DAILY BREAKS",
    "📊 WEEKLY BREAKS",
    "📅 MONTHLY BREAKS",
    "ℹ️ INFO"
])

# PANCHAK
with tabs[0]:
    st.dataframe(df, width="stretch")

# TOP HIGH
with tabs[1]:
    st.dataframe(df[df.LTP >= df.TOP_HIGH], width="stretch")

# TOP LOW
with tabs[2]:
    st.dataframe(df[df.LTP <= df.TOP_LOW], width="stretch")

# DAILY
with tabs[3]:
    st.dataframe(df[df.LTP >= df.HIGH], width="stretch")
    st.dataframe(df[df.LTP <= df.LOW], width="stretch")

# WEEKLY
with tabs[4]:
    st.dataframe(df[df.LTP >= df.HIGH_W], width="stretch")
    st.dataframe(df[df.LTP <= df.LOW_W], width="stretch")

# MONTHLY
with tabs[5]:
    st.dataframe(df[df.LTP >= df.HIGH_M], width="stretch")
    st.dataframe(df[df.LTP <= df.LOW_M], width="stretch")

# INFO
with tabs[6]:
    st.write("✔ Cached OHLC")
    st.write("✔ Static Panchak dates respected")
    st.write("✔ Indices included")
