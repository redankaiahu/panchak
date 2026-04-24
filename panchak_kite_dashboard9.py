# ==============================
# PANCHAK + BREAKOUT DASHBOARD
# FINAL PRODUCTION VERSION
# ==============================

import streamlit as st
import pandas as pd
import os
import time
from datetime import date, datetime, timedelta
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
CACHE_DIR = "CACHE"
os.makedirs(CACHE_DIR, exist_ok=True)

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
# KITE LOGIN
# ==============================
kite = KiteConnect(api_key="7am67kxijfsusk9i")
kite.set_access_token(ACCESS_TOKEN)
#kite.set_access_token(open("AccessToken.txt").read().strip())

# ==============================
# LOAD INSTRUMENTS
# ==============================
@st.cache_data
def load_instruments():
    return pd.read_csv("instruments_NSE.csv")

INST = load_instruments()

#def token(sym):
 #   row = INST[INST.tradingsymbol == sym]
  #  return int(row.iloc[0].instrument_token)
def token(sym):
    row = INST[
        (INST.tradingsymbol == sym) &
        (INST.exchange == "NSE")
    ]
    if row.empty:
        return None
    return int(row.iloc[0].instrument_token)

# ==============================
# BUILD STATIC OHLC
# ==============================
def build_static_ohlc(filename, from_date, to_date):
    path = f"{CACHE_DIR}/{filename}"
    if os.path.exists(path):
        return pd.read_csv(path)

    rows = []
    for s in SYMBOLS:
        try:
            bars = kite.historical_data(token(s), from_date, to_date, "day")
            df = pd.DataFrame(bars)
            rows.append({
                "Symbol": s,
                "HIGH": df.high.max(),
                "LOW": df.low.min()
            })
            time.sleep(0.25)
        except:
            pass

    out = pd.DataFrame(rows)
    out.to_csv(path, index=False)
    return out

# ==============================
# PANCHAK STATIC
# ==============================
def build_panchak():
    path = f"{CACHE_DIR}/panchak.csv"
    if os.path.exists(path):
        return pd.read_csv(path)

    rows = []
    for s in SYMBOLS:
        highs, lows = [], []
        for d in STATIC_DATES:
            try:
                bars = kite.historical_data(token(s), d, d+timedelta(days=1), "day")
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
        if not q: continue
        rows.append({
            "Symbol": s,
            "LTP": q["last_price"],
            "LIVE_HIGH": q["ohlc"]["high"],
            "LIVE_LOW": q["ohlc"]["low"]
        })
    return pd.DataFrame(rows)

# ==============================
# STYLE
# ==============================
def row_style(row):
    if row.LTP >= row.TOP_HIGH:
        return ["background-color:#b6f2c2"]*len(row)
    if row.LTP <= row.TOP_LOW:
        return ["background-color:#f7b2b0"]*len(row)
    return [""]*len(row)

def calc_near(row):
    try:
        if row["LTP"] >= row["TOP_HIGH"] or row["LTP"] <= row["TOP_LOW"]:
            return ""
        up = row["TOP_HIGH"] - row["LTP"]
        down = row["LTP"] - row["TOP_LOW"]
        if up <= down:
            return f"↑ {up:.1f}"
        else:
            return f"↓ {down:.1f}"
    except:
        return ""


# ==============================
# DATA LOAD
# ==============================
live = fetch_live()

daily = build_static_ohlc("daily.csv", date.today()-timedelta(days=1), date.today())
weekly = build_static_ohlc("weekly.csv", date.today()-timedelta(days=7), date.today())
monthly = build_static_ohlc("monthly.csv", date.today()-timedelta(days=30), date.today())
panchak = build_panchak()

# STANDARDIZE
daily.rename(columns={"HIGH":"Y_HIGH","LOW":"Y_LOW"}, inplace=True)
weekly.rename(columns={"HIGH":"W_HIGH","LOW":"W_LOW"}, inplace=True)
monthly.rename(columns={"HIGH":"M_HIGH","LOW":"M_LOW"}, inplace=True)

# MERGE ALL
df = live.merge(daily,on="Symbol",how="left") \
         .merge(weekly,on="Symbol",how="left") \
         .merge(monthly,on="Symbol",how="left") \
         .merge(panchak,on="Symbol",how="left")

PRICE_COLS = [
    "LTP","LIVE_HIGH","LIVE_LOW",
    "Y_HIGH","Y_LOW","W_HIGH","W_LOW",
    "M_HIGH","M_LOW","TOP_HIGH","TOP_LOW",
    "BT","ST"
]

for c in PRICE_COLS:
    if c in df.columns:
        df[c] = df[c].round(2)

df["NEAR"] = df.apply(calc_near, axis=1)
def near_style(val):
    if isinstance(val, str) and val.startswith("↑"):
        return "color: green; font-weight: bold"
    if isinstance(val, str) and val.startswith("↓"):
        return "color: red; font-weight: bold"
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
    st.dataframe(df.style.apply(row_style, axis=1), width="stretch")
    styled = df.style.applymap(near_style, subset=["NEAR"]).apply(row_style, axis=1)
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
