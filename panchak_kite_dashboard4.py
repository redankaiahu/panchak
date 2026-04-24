import os, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz

# ===============================
# CONFIG
# ===============================
BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

INST_FILE = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE = os.path.join(CACHE_DIR, "DAILY_OHLC.xlsx")

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"
IST = pytz.timezone("Asia/Kolkata")

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

# ===============================
# KITE LOGIN
# ===============================
with open(ACCESS_TOKEN_FILE) as f:
    access_token = f.read().strip()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(access_token)

# ===============================
# LOAD INSTRUMENTS (CACHED)
# ===============================
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def token(symbol):
    r = inst[inst.tradingsymbol == symbol]
    return None if r.empty else int(r.iloc[0].instrument_token)

# ===============================
# BUILD DAILY OHLC (ONCE/DAY)
# ===============================
def build_daily_ohlc():
    if os.path.exists(DAILY_FILE):
        return

    rows = []
    today = date.today()
    start = today - timedelta(days=10)

    for s in SYMBOLS:
        tk = token(s)
        if not tk: continue

        try:
            bars = kite.historical_data(tk, start, today, "day")
            if len(bars) < 2: continue

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

# ===============================
# LIVE DATA
# ===============================
@st.cache_data(ttl=55)
def live_data():
    quotes = kite.quote([f"NSE:{s}" for s in SYMBOLS])
    rows = []
    for s in SYMBOLS:
        q = quotes.get(f"NSE:{s}")
        if not q: continue
        rows.append({
            "Symbol": s,
            "LTP": q["last_price"],
            "Today_High": q["ohlc"]["high"],
            "Today_Low": q["ohlc"]["low"],
        })
    return pd.DataFrame(rows)

live_df = live_data()
df = live_df.merge(daily_df, on="Symbol", how="left")

# ===============================
# PANCHAK CALCULATION
# ===============================
def panchak_calc(symbol):
    tk = token(symbol)
    if not tk: return None

    bars = kite.historical_data(
        tk, PANCHAK_DATES[0], PANCHAK_DATES[-1], "day"
    )
    df = pd.DataFrame(bars)
    th = df["high"].max()
    tl = df["low"].min()
    diff = th - tl

    return th, tl, diff, th + diff, tl - diff

panchak_rows = []
for s in SYMBOLS:
    try:
        r = panchak_calc(s)
        if not r: continue
        th, tl, diff, bt, stv = r
        ltp = df[df.Symbol == s]["LTP"].values[0]

        panchak_rows.append({
            "Symbol": s,
            "LTP": ltp,
            "TOP_HIGH": th,
            "TOP_LOW": tl,
            "DIFF": diff,
            "BT": bt,
            "ST": stv,
            "NEAR": min(abs(th-ltp), abs(ltp-tl))
        })
        time.sleep(0.25)
    except:
        pass

panchak_df = pd.DataFrame(panchak_rows)

# ===============================
# STREAMLIT UI
# ===============================
st.set_page_config("Panchak Kite Dashboard", layout="wide")
st.title("📊 Panchak Live Dashboard (Kite)")

st.caption(f"🕒 Last refresh: {datetime.now(IST).strftime('%H:%M:%S')} | Auto-refresh 60s")

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Y-High Broken",
    "📉 Y-Low Broken",
    "🪐 Panchak",
    "ℹ️ INFO"
])

with tab1:
    st.dataframe(
        df[df.LTP >= df.Y_High][["Symbol","LTP","Y_High"]],
        width="content"
    )

with tab2:
    st.dataframe(
        df[df.LTP <= df.Y_Low][["Symbol","LTP","Y_Low"]],
        width="content"
    )

with tab3:
    st.dataframe(
        panchak_df.sort_values("NEAR"),
        width="content"
    )

with tab4:
    st.dataframe(
        df[["Symbol","LTP","Today_High","Today_Low","Y_High","Y_Low"]],
        width="content"
    )
