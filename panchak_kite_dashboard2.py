# ============================================================
# PANCHAK + YH/YL + LIVE DASHBOARD (PRODUCTION VERSION)
# Zerodha Kite Connect
# ============================================================

import os
import time
import datetime as dt
import pandas as pd
import streamlit as st
from kiteconnect import KiteConnect

# ==============================
# CONFIG
# ==============================
BASE_DIR = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
INST_FILE = os.path.join(CACHE_DIR, "instruments_NSE.csv")

DAILY_FILE = os.path.join(CACHE_DIR, "STATIC_OHLC.xlsx")
LIVE_FILE  = os.path.join(CACHE_DIR, "LIVE_DATA.xlsx")

API_KEY_FILE = os.path.join(BASE_DIR, "API_KEY.txt")
ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")

REFRESH_SECONDS = 60

PANCHAK_DATES = [
    "2026-01-21",
    "2026-01-22",
    "2026-01-23",
    "2026-01-24",
    "2026-01-25"
]

SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "RELIANCE",
    "INFY",
    "HCLTECH",
    "TVSMOTOR",
    "BHARATFORG",
    "JUBLFOOD",
    "LAURUSLABS",
    "SUNPHARMA",
    "TATACONSUM",
    "COFORGE",
    "ASIANPAINT",
    "MUTHOOTFIN",
    "CHOLAFIN",
    "BSE",
    "GRASIM",
    "ACC",
    "ADANIENT",
    "BHARTIARTL",
    "BIOCON",
    "BRITANNIA",
    "DIVISLAB",
    "ESCORTS",
    "JSWSTEEL",
    "M&M",
    "PAGEIND",
    "SHREECEM",
    "BOSCHLTD",
    "DIXON",
    "MARUTI",
    "ULTRACEMCO",
    "APOLLOHOSP",
    "MCX",
    "POLYCAB",
    "PERSISTENT",
    "TRENT",
    "EICHERMOT",
    "HAL",
    "TIINDIA",
    "SIEMENS",
    "GAIL",
    "NATIONALUM",
    "TATASTEEL",
    "MOTHERSON",
    "SHRIRAMFIN",
    "VEDL",
    "VBL",
    "GRANULES",
    "LICHSGFIN",
    "UPL",
    "ANGELONE",
    "INDHOTEL",
    "APLAPOLLO",
    "CAMS",
    "CUMMINSIND",
    "MAXHEALTH",
    "POLICYBZR",
    "HAVELLS",
    "GLENMARK",
    "ADANIPORTS",
    "SRF",
    "CDSL",
    "TITAN",
    "SBILIFE",
    "COLPAL",
    "HDFCLIFE",
    "VOLTAS",
    "NAUKRI",
    "TATACHEM",
    "KALYANKJIL"
]


# ==============================
# INIT
# ==============================
os.makedirs(CACHE_DIR, exist_ok=True)

st.set_page_config(
    page_title="Panchak + YH/YL Live Dashboard",
    layout="wide"
)

# ==============================
# KITE LOGIN
# ==============================
with open(ACCESS_TOKEN_FILE) as f:
    ACCESS_TOKEN = f.read().strip()
with open(API_KEY_FILE) as f:
    API_KEY = f.read().strip()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ==============================
# LOAD INSTRUMENTS (ONCE)
# ==============================
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)

    data = kite.instruments("NSE")
    df = pd.DataFrame(data)
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

# ==============================
# TOKEN RESOLVER (SAFE)
# ==============================
def get_token(symbol):
    df = inst[inst.tradingsymbol == symbol]
    if df.empty:
        return None
    return int(df.iloc[0].instrument_token)

# ==============================
# BUILD STATIC OHLC (ONCE PER DAY)
# ==============================
def build_static_ohlc():
    if os.path.exists(DAILY_FILE):
        return

    rows = []
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)

    for sym in SYMBOLS:
        token = get_token(sym)
        if not token:
            continue

        # Panchak dates
        for d in PANCHAK_DATES:
            d0 = dt.datetime.strptime(d, "%Y-%m-%d").date()
            d1 = d0 + dt.timedelta(days=1)

            bars = kite.historical_data(token, d0, d1, "day")
            if not bars:
                continue

            bar = bars[0]
            rows.append({
                "Symbol": sym,
                "Date": d0,
                "Open": bar["open"],
                "High": bar["high"],
                "Low": bar["low"],
                "Close": bar["close"],
                "Type": "PANCHAK"
            })

        # Yesterday
        bars = kite.historical_data(token, yesterday, today, "day")
        if bars:
            bar = bars[0]
            rows.append({
                "Symbol": sym,
                "Date": yesterday,
                "Open": bar["open"],
                "High": bar["high"],
                "Low": bar["low"],
                "Close": bar["close"],
                "Type": "YESTERDAY"
            })

        time.sleep(0.25)  # rate-limit safety

    df = pd.DataFrame(rows)

    # 🔒 FIX: REMOVE TIMEZONE SAFELY
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    df.to_excel(DAILY_FILE, index=False)

# ==============================
# BUILD STATIC FILE
# ==============================
build_static_ohlc()

# ==============================
# LIVE DATA FETCH
# ==============================
def fetch_live():
    quotes = {}
    for sym in SYMBOLS:
        token = get_token(sym)
        if not token:
            continue

        q = kite.quote([f"NSE:{sym}"])[f"NSE:{sym}"]
        quotes[sym] = {
            "LTP": q["last_price"],
            "DayHigh": q["ohlc"]["high"],
            "DayLow": q["ohlc"]["low"]
        }
        time.sleep(0.05)

    df = pd.DataFrame.from_dict(quotes, orient="index").reset_index()
    df.columns = ["Symbol", "LTP", "TodayHigh", "TodayLow"]
    df["Time"] = dt.datetime.now()

    # timezone-safe
    df["Time"] = df["Time"].dt.tz_localize(None)

    df.to_excel(LIVE_FILE, index=False)
    return df

# ==============================
# STREAMLIT UI
# ==============================
st.title("📊 Panchak + YH/YL Live Dashboard")
st.caption(f"Auto refresh every {REFRESH_SECONDS} seconds")

tabs = st.tabs([
    "🚀 Yesterday High Break",
    "🔻 Yesterday Low Break",
    "🪐 Panchak",
])

static_df = pd.read_excel(DAILY_FILE)
live_df = fetch_live()

merged = live_df.merge(
    static_df[static_df.Type == "YESTERDAY"][["Symbol", "High", "Low"]],
    on="Symbol",
    how="left"
)

merged.rename(columns={
    "High": "Y_High",
    "Low": "Y_Low"
}, inplace=True)

# ==============================
# TAB 1 – Y HIGH BREAK
# ==============================
with tabs[0]:
    df = merged[merged.LTP > merged.Y_High]
    st.dataframe(df, use_container_width=True)

# ==============================
# TAB 2 – Y LOW BREAK
# ==============================
with tabs[1]:
    df = merged[merged.LTP < merged.Y_Low]
    st.dataframe(df, use_container_width=True)

# ==============================
# TAB 3 – PANCHAK
# ==============================
with tabs[2]:
    p = static_df[static_df.Type == "PANCHAK"]

    summary = (
        p.groupby("Symbol")
        .agg(
            TopHigh=("High", "max"),
            TopLow=("Low", "min")
        )
        .reset_index()
    )

    summary["Diff"] = summary.TopHigh - summary.TopLow
    summary["BT"] = summary.TopHigh + summary.Diff
    summary["ST"] = summary.TopLow - summary.Diff

    final = summary.merge(live_df, on="Symbol", how="left")

    def status(r):
        if r.LTP >= r.BT:
            return "🎯 HIGH TARGET HIT"
        if r.LTP <= r.ST:
            return "🎯 LOW TARGET HIT"
        return "⏳ WAIT"

    final["Status"] = final.apply(status, axis=1)

    st.dataframe(final, use_container_width=True)

# ==============================
# AUTO REFRESH
# ==============================
time.sleep(REFRESH_SECONDS)
st.rerun()
