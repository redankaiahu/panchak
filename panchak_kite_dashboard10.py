# ==============================
# PANCHAK + BREAKOUT DASHBOARD
# FINAL – STABLE PRODUCTION VERSION
# ==============================

import streamlit as st
import pandas as pd
import os, time
from datetime import date, timedelta
from kiteconnect import KiteConnect

# ==============================
# CONFIG
# ==============================
BASE_DIR = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN = open(os.path.join(BASE_DIR, "access_token.txt")).read().strip()

REFRESH_SEC = 60

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

STATIC_DATES = [
    date(2026,1,21),
    date(2026,1,22),
    date(2026,1,23),
    date(2026,1,24),
    date(2026,1,25)
]

# ==============================
# KITE INIT
# ==============================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ==============================
# LOAD INSTRUMENTS
# ==============================
@st.cache_data
def load_instruments():
    return pd.read_csv(os.path.join(BASE_DIR, "instruments_NSE.csv"))

INST = load_instruments()

def get_token(sym):
    row = INST[
        (INST.tradingsymbol == sym) &
        (INST.instrument_type.isin(["EQ", "INDICES"]))
    ]
    if row.empty:
        return None
    return int(row.iloc[0].instrument_token)

# ==============================
# BUILD PERIOD OHLC (CACHED)
# ==============================
def build_period_ohlc(fname, start, end):
    path = os.path.join(CACHE_DIR, fname)
    if os.path.exists(path):
        return pd.read_csv(path)

    rows = []
    for s in SYMBOLS:
        tok = get_token(s)
        if not tok:
            continue
        try:
            bars = kite.historical_data(tok, start, end, "day")
            df = pd.DataFrame(bars)
            rows.append({
                "Symbol": s,
                "HIGH": df["high"].max(),
                "LOW": df["low"].min()
            })
            time.sleep(0.3)
        except:
            continue

    out = pd.DataFrame(rows)
    out.to_csv(path, index=False)
    return out

# ==============================
# PANCHAK STATIC
# ==============================
def build_panchak():
    path = os.path.join(CACHE_DIR, "panchak.csv")
    if os.path.exists(path):
        return pd.read_csv(path)

    rows = []
    for s in SYMBOLS:
        tok = get_token(s)
        if not tok:
            continue

        highs, lows = [], []
        for d in STATIC_DATES:
            try:
                bars = kite.historical_data(tok, d, d + timedelta(days=1), "day")
                highs.append(bars[0]["high"])
                lows.append(bars[0]["low"])
                time.sleep(0.25)
            except:
                pass

        if highs and lows:
            th, tl = max(highs), min(lows)
            diff = th - tl
            rows.append({
                "Symbol": s,
                "TOP_HIGH": th,
                "TOP_LOW": tl,
                "DIFF": diff,
                "BT": th + diff,
                "ST": tl - diff
            })

    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    return df

# ==============================
# LIVE DATA
# ==============================
@st.cache_data(ttl=REFRESH_SEC)
def fetch_live():
    quotes = kite.quote([f"NSE:{s}" for s in SYMBOLS])
    rows = []
    for s in SYMBOLS:
        q = quotes.get(f"NSE:{s}")
        if not q:
            continue
        rows.append({
            "Symbol": s,
            "LTP": q["last_price"],
            "LIVE_HIGH": q["ohlc"]["high"],
            "LIVE_LOW": q["ohlc"]["low"]
        })
    return pd.DataFrame(rows)

# ==============================
# LOAD ALL DATA
# ==============================
today = date.today()
live = fetch_live()

daily = build_period_ohlc("daily.csv", today - timedelta(days=1), today)
weekly = build_period_ohlc("weekly.csv", today - timedelta(days=7), today)
monthly = build_period_ohlc("monthly.csv", today - timedelta(days=30), today)
panchak = build_panchak()

daily.rename(columns={"HIGH":"Y_HIGH","LOW":"Y_LOW"}, inplace=True)
weekly.rename(columns={"HIGH":"W_HIGH","LOW":"W_LOW"}, inplace=True)
monthly.rename(columns={"HIGH":"M_HIGH","LOW":"M_LOW"}, inplace=True)

df = (
    live.merge(daily, on="Symbol", how="left")
        .merge(weekly, on="Symbol", how="left")
        .merge(monthly, on="Symbol", how="left")
        .merge(panchak, on="Symbol", how="left")
)

# ==============================
# CALCULATIONS
# ==============================
for c in df.columns:
    if c != "Symbol":
        df[c] = df[c].round(2)

def calc_near(r):
    if pd.isna(r.TOP_HIGH) or pd.isna(r.TOP_LOW):
        return ""
    if r.LTP >= r.TOP_HIGH or r.LTP <= r.TOP_LOW:
        return ""
    up = r.TOP_HIGH - r.LTP
    dn = r.LTP - r.TOP_LOW
    return f"↑ {up:.1f}" if up <= dn else f"↓ {dn:.1f}"

df["NEAR"] = df.apply(calc_near, axis=1)

# ==============================
# STYLING
# ==============================
def row_style(r):
    if r.LTP >= r.TOP_HIGH:
        return ["background-color:#c6f6d5"] * len(r)
    if r.LTP <= r.TOP_LOW:
        return ["background-color:#fed7d7"] * len(r)
    return [""] * len(r)

def near_style(v):
    if isinstance(v,str) and v.startswith("↑"):
        return "color:green;font-weight:bold"
    if isinstance(v,str) and v.startswith("↓"):
        return "color:red;font-weight:bold"
    return ""

# ==============================
# STREAMLIT UI
# ==============================
st.set_page_config(layout="wide")
st.title("📊 Panchak + Breakout Dashboard")
st.caption(f"Auto refresh every {REFRESH_SEC}s")

tabs = st.tabs([
    "📌 Panchak",
    "📈 Daily Break",
    "📊 Weekly Break",
    "📆 Monthly Break",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW"
])

with tabs[0]:
    styled = df.style.apply(row_style, axis=1).map(near_style, subset=["NEAR"])
    st.dataframe(styled, width="stretch")

with tabs[1]:
    st.dataframe(df[df.LTP >= df.Y_HIGH], width="stretch")
    st.dataframe(df[df.LTP <= df.Y_LOW], width="stretch")

with tabs[2]:
    st.dataframe(df[df.LTP >= df.W_HIGH], width="stretch")
    st.dataframe(df[df.LTP <= df.W_LOW], width="stretch")

with tabs[3]:
    st.dataframe(df[df.LTP >= df.M_HIGH], width="stretch")
    st.dataframe(df[df.LTP <= df.M_LOW], width="stretch")

with tabs[4]:
    st.dataframe(df[df.LTP >= df.TOP_HIGH], width="stretch")

with tabs[5]:
    st.dataframe(df[df.LTP <= df.TOP_LOW], width="stretch")
