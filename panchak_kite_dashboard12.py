# =====================================================
# PANCHAK + BREAKOUT DASHBOARD (INDEX + FORMAT FIXED)
# =====================================================

import streamlit as st
import pandas as pd
import os, time
from datetime import date, timedelta
from kiteconnect import KiteConnect

# ---------------- CONFIG ----------------
BASE_DIR = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN = open(os.path.join(BASE_DIR, "access_token.txt")).read().strip()

REFRESH_SEC = 60

# ---------------- SYMBOLS ----------------
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

# 🔑 INDEX → ZERODHA SYMBOL MAP (CRITICAL FIX)
INDEX_SYMBOL_MAP = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK"
}

STATIC_DATES = [
    date(2026,1,21),
    date(2026,1,22),
    date(2026,1,23),
    date(2026,1,24),
    date(2026,1,25)
]

# ---------------- KITE ----------------
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ---------------- INSTRUMENTS ----------------
@st.cache_data
def load_instruments():
    return pd.read_csv(os.path.join(BASE_DIR, "instruments_NSE.csv"))

INST = load_instruments()

def get_token(symbol):
    row = INST[(INST.tradingsymbol == symbol) & (INST.instrument_type == "EQ")]
    if not row.empty:
        return int(row.iloc[0].instrument_token)

    row = INST[(INST.tradingsymbol == symbol) & (INST.instrument_type == "INDICES")]
    if not row.empty:
        return int(row.iloc[0].instrument_token)

    return None

# ---------------- SAFE QUOTES (INDEX FIX) ----------------
@st.cache_data(ttl=REFRESH_SEC)
def fetch_live():
    rows = []
    quote_keys = []

    for s in SYMBOLS:
        if s in INDEX_SYMBOL_MAP:
            quote_keys.append(f"NSE:{INDEX_SYMBOL_MAP[s]}")
        else:
            quote_keys.append(f"NSE:{s}")

    quotes = kite.quote(quote_keys)

    for s in SYMBOLS:
        key = f"NSE:{INDEX_SYMBOL_MAP[s]}" if s in INDEX_SYMBOL_MAP else f"NSE:{s}"
        q = quotes.get(key)
        if not q:
            continue

        rows.append({
            "Symbol": s,
            "LTP": q["last_price"],
            "LIVE_HIGH": q["ohlc"]["high"],
            "LIVE_LOW": q["ohlc"]["low"]
        })

    return pd.DataFrame(rows)

# ---------------- PANCHAK STATIC ----------------
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
                bars = kite.historical_data(tok, d, d+timedelta(days=1), "day")
                if bars:
                    highs.append(bars[0]["high"])
                    lows.append(bars[0]["low"])
                time.sleep(0.3)
            except:
                pass

        if highs and lows:
            th, tl = max(highs), min(lows)
            diff = th - tl
            rows.append({
                "Symbol": s,
                "TOP_HIGH": th,
                "TOP_LOW": tl,
                "BT": th + diff,
                "ST": tl - diff
            })

    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    return df

# ---------------- NEAR LOGIC ----------------
def calc_near(r):
    if pd.isna(r.TOP_HIGH) or pd.isna(r.TOP_LOW):
        return ""
    if r.LTP >= r.TOP_HIGH or r.LTP <= r.TOP_LOW:
        return ""
    up = r.TOP_HIGH - r.LTP
    dn = r.LTP - r.TOP_LOW
    return f"↑ {up:.1f}" if up <= dn else f"↓ {dn:.1f}"

# ---------------- LOAD DATA ----------------
live = fetch_live()
panchak = build_panchak()

df = live.merge(panchak, on="Symbol", how="left")
df["NEAR"] = df.apply(calc_near, axis=1)

# ---------------- STYLING ----------------
def row_color(r):
    if r.LTP >= r.TOP_HIGH:
        return ["background-color:#c6f6d5"] * len(r)
    if r.LTP <= r.TOP_LOW:
        return ["background-color:#fed7d7"] * len(r)
    return [""] * len(r)

def near_color(v):
    if isinstance(v,str) and v.startswith("↑"):
        return "color:green;font-weight:bold"
    if isinstance(v,str) and v.startswith("↓"):
        return "color:red;font-weight:bold"
    return ""

# 🔢 FORMAT FIX (NO MORE ZEROS)
FORMAT_MAP = {
    c: "{:.2f}" for c in df.columns if c not in ["Symbol","NEAR"]
}

# ---------------- STREAMLIT ----------------
st.set_page_config(layout="wide")
st.title("📊 Panchak + Breakout Dashboard")
st.caption("Auto refresh every 60s")

tabs = st.tabs(["📌 Panchak","🟢 TOP_HIGH","🔴 TOP_LOW"])

with tabs[0]:
    styled = (
        df.style
        .apply(row_color, axis=1)
        .map(near_color, subset=["NEAR"])
        .format(FORMAT_MAP)
    )
    st.dataframe(styled, width="stretch")

with tabs[1]:
    st.dataframe(df[df.LTP >= df.TOP_HIGH].round(2), width="stretch")

with tabs[2]:
    st.dataframe(df[df.LTP <= df.TOP_LOW].round(2), width="stretch")
