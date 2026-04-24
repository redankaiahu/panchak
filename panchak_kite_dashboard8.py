# ==============================
# DEC PANCHAK – FINAL STABLE DASHBOARD
# ==============================

import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect

# ================= CONFIG ================= #
BASE_DIR = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")

REFRESH_SEC = 60

STATIC_DATES = [
    date(2026, 1, 21),
    date(2026, 1, 22),
    date(2026, 1, 23),
    date(2026, 1, 24),
    date(2026, 1, 25),
]

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

INDEX_MAP = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK"
}

# ================= STREAMLIT ================= #
st.set_page_config("DEC PANCHAK – LIVE", layout="wide")

# ================= AUTH ================= #
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(open(ACCESS_TOKEN_FILE).read().strip())

def ks(sym):
    return f"NSE:{INDEX_MAP.get(sym, sym)}"

# ================= CACHE FILES ================= #
DAILY_FILE   = os.path.join(BASE_DIR, "daily_ohlc.csv")
WEEKLY_FILE  = os.path.join(BASE_DIR, "weekly_ohlc.csv")
MONTHLY_FILE = os.path.join(BASE_DIR, "monthly_ohlc.csv")
PANCHAK_FILE = os.path.join(BASE_DIR, "panchak_static.csv")

# ================= BUILD STATIC OHLC (ONCE/DAY) ================= #
@st.cache_data(ttl=86400)
def build_static_files():
    quotes = kite.quote([ks(s) for s in SYMBOLS])

    rows = []
    for s in SYMBOLS:
        q = quotes.get(ks(s))
        if not q:
            continue

        rows.append({
            "Symbol": s,
            "Y_Close": q["ohlc"]["close"],
            "Y_High": q["ohlc"]["high"],
            "Y_Low": q["ohlc"]["low"],
        })

    df = pd.DataFrame(rows)

    df.to_csv(DAILY_FILE, index=False)
    df.to_csv(WEEKLY_FILE, index=False)
    df.to_csv(MONTHLY_FILE, index=False)

    # Panchak static
    p_rows = []
    for s in SYMBOLS:
        p_rows.append({
            "Symbol": s,
            "TOP_HIGH": df[df.Symbol == s].Y_High.max(),
            "TOP_LOW": df[df.Symbol == s].Y_Low.min(),
        })

    p = pd.DataFrame(p_rows)
    p["DIFF"] = (p.TOP_HIGH - p.TOP_LOW).round(2)
    p["BT"] = (p.TOP_HIGH + p.DIFF).round(2)
    p["ST"] = (p.TOP_LOW - p.DIFF).round(2)

    p.to_csv(PANCHAK_FILE, index=False)

if not os.path.exists(PANCHAK_FILE):
    build_static_files()

# ================= LOAD STATIC ================= #
daily = pd.read_csv(DAILY_FILE)
weekly = pd.read_csv(WEEKLY_FILE)
monthly = pd.read_csv(MONTHLY_FILE)
panchak = pd.read_csv(PANCHAK_FILE)

# ================= LIVE DATA ================= #
@st.cache_data(ttl=30)
def live_data():
    rows = []
    quotes = kite.quote([ks(s) for s in SYMBOLS])

    for s in SYMBOLS:
        q = quotes.get(ks(s))
        if not q:
            continue

        ltp = q["last_price"]
        yc = q["ohlc"]["close"]
        change = round(ltp - yc, 2)
        pct = round((change / yc) * 100, 2)

        rows.append({
            "Symbol": s,
            "LTP": round(ltp, 2),
            "Today_High": round(q["ohlc"]["high"], 2),
            "Today_Low": round(q["ohlc"]["low"], 2),
            "Change": change,
            "Change_%": pct
        })
    return pd.DataFrame(rows)

live = live_data()

# ================= PANCHAK MERGE ================= #
df = live.merge(panchak, on="Symbol", how="left")

# ================= NEAR LOGIC ================= #
def calc_near(r):
    if r.LTP >= r.TOP_HIGH or r.LTP <= r.TOP_LOW:
        return ""
    if (r.TOP_HIGH - r.LTP) <= (r.LTP - r.TOP_LOW):
        return f"↑ {round(r.TOP_HIGH - r.LTP,1)}"
    return f"↓ {round(r.LTP - r.TOP_LOW,1)}"

df["NEAR"] = df.apply(calc_near, axis=1)

# ================= STYLING ================= #
def arrow_style(v):
    if isinstance(v, str):
        if v.startswith("↑"):
            return "color:green;font-weight:bold"
        if v.startswith("↓"):
            return "color:red;font-weight:bold"
    return ""

def row_bg(r):
    if r.LTP >= r.TOP_HIGH:
        return ["background-color:#d4f7d4"] * len(r)
    if r.LTP <= r.TOP_LOW:
        return ["background-color:#f7d4d4"] * len(r)
    return [""] * len(r)

styled = (
    df.style
    .applymap(arrow_style, subset=["NEAR"])
    .apply(row_bg, axis=1)
    .format("{:.2f}")
)

# ================= UI ================= #
st.title("📊 DEC PANCHAK – LIVE KITE DASHBOARD")
st.caption("Live LTP | Daily / Weekly / Monthly | Panchak | Auto refresh 60s")

st.info(f"⏱ Last refresh: {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")

tabs = st.tabs([
    "🔥 Daily Breaks",
    "📅 Weekly Breaks",
    "🗓 Monthly Breaks",
    "🧮 Panchak",
    "ℹ INFO"
])

with tabs[0]:
    st.dataframe(df[df.LTP > df.TOP_HIGH], use_container_width=True)
    st.dataframe(df[df.LTP < df.TOP_LOW], use_container_width=True)

with tabs[1]:
    st.dataframe(df[df.LTP > df.TOP_HIGH], use_container_width=True)
    st.dataframe(df[df.LTP < df.TOP_LOW], use_container_width=True)

with tabs[2]:
    st.dataframe(df[df.LTP > df.TOP_HIGH], use_container_width=True)
    st.dataframe(df[df.LTP < df.TOP_LOW], use_container_width=True)

with tabs[3]:
    st.dataframe(styled, use_container_width=True)

with tabs[4]:
    st.markdown("""
**Logic Summary**
- Daily / Weekly / Monthly cached once per day
- Live = LTP + Today High/Low
- Panchak = TOP HIGH / TOP LOW / DIFF / BT / ST
- NEAR = nearest side exactly like Excel
""")

st.markdown(f"<meta http-equiv='refresh' content='{REFRESH_SEC}'>", unsafe_allow_html=True)
