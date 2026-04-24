# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION)
# Zerodha Kite | Streamlit | Cached | Stable
# ==========================================================

import os, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz

# ==========================================================
# CONFIG
# ==========================================================
BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

INST_FILE   = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE  = os.path.join(CACHE_DIR, "DAILY_OHLC.xlsx")
PANCHAK_FILE = os.path.join(CACHE_DIR, "PANCHAK_OHLC.xlsx")

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"
IST = pytz.timezone("Asia/Kolkata")

REFRESH_SECONDS = 60

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

PANCHAK_DATES = [
    date(2026,1,21), date(2026,1,22),
    date(2026,1,23), date(2026,1,24),
    date(2026,1,25)
]

# ==========================================================
# KITE CONNECT
# ==========================================================
with open(ACCESS_TOKEN_FILE) as f:
    ACCESS_TOKEN = f.read().strip()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ==========================================================
# LOAD INSTRUMENTS (ONCE)
# ==========================================================
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def get_token(symbol):
    r = inst[inst.tradingsymbol == symbol]
    if r.empty:
        return None
    return int(r.iloc[0].instrument_token)

# ==========================================================
# BUILD YESTERDAY OHLC (ONCE PER DAY)
# ==========================================================
def build_daily_ohlc():
    if os.path.exists(DAILY_FILE):
        return

    rows = []
    today = date.today()
    start = today - timedelta(days=7)

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(tk, start, today, "day")
            if len(bars) < 2:
                continue
            y = bars[-2]
            rows.append({
                "Symbol": s,
                "Y_Close": y["close"],
                "Y_High": y["high"],
                "Y_Low": y["low"]
            })
            time.sleep(0.35)
        except:
            time.sleep(1)

    pd.DataFrame(rows).to_excel(DAILY_FILE, index=False)

build_daily_ohlc()
daily_df = pd.read_excel(DAILY_FILE)

# ==========================================================
# BUILD PANCHAK STATIC OHLC (ONCE PER DAY)
# ==========================================================
def build_panchak_ohlc():
    if os.path.exists(PANCHAK_FILE):
        return

    rows = []
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(
                tk,
                PANCHAK_DATES[0],
                PANCHAK_DATES[-1],
                "day"
            )
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

    pd.DataFrame(rows).to_excel(PANCHAK_FILE, index=False)

build_panchak_ohlc()
panchak_static = pd.read_excel(PANCHAK_FILE)

# ==========================================================
# LIVE DATA (LTP + TODAY HIGH/LOW)
# ==========================================================
@st.cache_data(ttl=55)

def fetch_live():
    quotes = kite.quote([f"NSE:{s}" for s in SYMBOLS])
    rows = []

    for s in SYMBOLS:
        q = quotes.get(f"NSE:{s}")
        if not q:
            continue

        ltp = q["last_price"]
        prev_close = q["ohlc"]["close"]
        change = ltp - prev_close
        change_pct = (change / prev_close) * 100 if prev_close else 0

        rows.append({
            "Symbol": s,
            "LTP": ltp,
            "Today_High": q["ohlc"]["high"],
            "Today_Low": q["ohlc"]["low"],
            "Change": round(change, 2),
            "Change_%": round(change_pct, 2)
        })

    return pd.DataFrame(rows)


live_df = fetch_live()

# ==========================================================
# MERGE ALL
# ==========================================================
df = (
    live_df
    .merge(daily_df, on="Symbol", how="left")
    .merge(panchak_static, on="Symbol", how="left")
)

# ==========================================================
# PANCHAK NEAR LOGIC (EXCEL MATCH)
# ==========================================================
def calc_near(row):
    if pd.isna(row.TOP_HIGH) or pd.isna(row.TOP_LOW):
        return ""
    if row.LTP >= row.TOP_HIGH or row.LTP <= row.TOP_LOW:
        return ""
    if (row.TOP_HIGH - row.LTP) <= (row.LTP - row.TOP_LOW):
        return f"↑ {row.TOP_HIGH - row.LTP:.1f}"
    return f"↓ {row.LTP - row.TOP_LOW:.1f}"

df["NEAR"] = df.apply(calc_near, axis=1)

# ==========================================================
# STREAMLIT UI
# ==========================================================
st.set_page_config("Panchak Dashboard", layout="wide")
st.title("📊 LIVE PANCHAK + BREAKOUT DASHBOARD")

st.caption(
    f"🕒 Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto-refresh {REFRESH_SECONDS}s"
)

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Y-High Break",
    "📉 Y-Low Break",
    "🪐 Panchak",
    "ℹ️ INFO"
])

# ==========================================================
# TAB 1 – YEST HIGH BREAK
# ==========================================================
with tab1:
    t = df[df.LTP >= df.Y_High]
    st.dataframe(
        t.style.apply(
            lambda x: ["background-color: #C6EFCE"] * len(x), axis=1
        ),
        use_container_width=True
    )

# ==========================================================
# TAB 2 – YEST LOW BREAK
# ==========================================================
with tab2:
    t = df[df.LTP <= df.Y_Low]
    st.dataframe(
        t.style.apply(
            lambda x: ["background-color: #F4CCCC"] * len(x), axis=1
        ),
        use_container_width=True
    )

# ==========================================================
# TAB 3 – PANCHAK
# ==========================================================
with tab3:
    st.dataframe(
        df[[
            "Symbol","LTP","TOP_HIGH","TOP_LOW","DIFF","BT","ST",
            "NEAR","Change","Change_%"
        ]].sort_values("NEAR"),
        use_container_width=True
    )

# ==========================================================
# TAB 4 – INFO
# ==========================================================
with tab4:
    st.dataframe(
        df[[
            "Symbol","LTP","Today_High","Today_Low",
            "Y_High","Y_Low","Change","Change_%"
        ]],
        use_container_width=True
    )

# ==========================================================
# AUTO REFRESH
# ==========================================================
time.sleep(REFRESH_SECONDS)
st.rerun()
