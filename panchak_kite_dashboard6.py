import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect

# ================= CONFIG ================= #
BASE_DIR = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")
API_KEY = "7am67kxijfsusk9i"

REFRESH_SEC = 60

STATIC_DATES = [
    date(2026, 1, 21),
    date(2026, 1, 22),
    date(2026, 1, 23),
    date(2026, 1, 24),
    date(2026, 1, 25),
]

SYMBOLS = [
    "NIFTY", "BANKNIFTY", "RELIANCE", "INFY", "HCLTECH", "TVSMOTOR",
    "BHARATFORG", "JUBLFOOD", "LAURUSLABS", "SUNPHARMA", "TATACONSUM",
    "COFORGE", "ASIANPAINT", "MUTHOOTFIN", "CHOLAFIN", "BSE", "GRASIM",
    "ACC", "ADANIENT", "BHARTIARTL", "BIOCON", "BRITANNIA", "DIVISLAB",
    "ESCORTS", "JSWSTEEL", "M&M", "PAGEIND", "SHREECEM", "BOSCHLTD",
    "DIXON", "MARUTI", "ULTRACEMCO", "APOLLOHOSP", "MCX", "POLYCAB",
    "PERSISTENT", "TRENT", "EICHERMOT", "HAL", "TIINDIA", "SIEMENS",
    "GAIL", "NATIONALUM", "TATASTEEL", "MOTHERSON", "SHRIRAMFIN",
    "VEDL", "VBL", "GRANULES", "LICHSGFIN", "UPL", "ANGELONE",
    "INDHOTEL", "APLAPOLLO", "CAMS", "CUMMINSIND", "MAXHEALTH",
    "POLICYBZR", "HAVELLS", "GLENMARK", "ADANIPORTS", "SRF",
    "CDSL", "TITAN", "SBILIFE", "COLPAL", "HDFCLIFE", "VOLTAS",
    "NAUKRI", "TATACHEM", "KALYANKJIL"
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

# ================= HELPERS ================= #
def kite_symbol(sym):
    return f"NSE:{INDEX_MAP.get(sym, sym)}"

@st.cache_data(ttl=86400)
def load_instruments():
    return pd.DataFrame(kite.instruments("NSE"))

inst = load_instruments()

def token(sym):
    name = INDEX_MAP.get(sym, sym)
    row = inst[inst.tradingsymbol == name]
    return int(row.iloc[0].instrument_token)

# ================= STATIC OHLC ================= #
@st.cache_data(ttl=86400)
def build_static_ohlc():
    rows = []
    for s in SYMBOLS:
        try:
            t = token(s)
            bars = kite.historical_data(
                t,
                STATIC_DATES[0],
                STATIC_DATES[-1],
                "day"
            )
            for b in bars:
                rows.append({
                    "Symbol": s,
                    "Date": b["date"].date(),
                    "Open": b["open"],
                    "High": b["high"],
                    "Low": b["low"],
                    "Close": b["close"],
                })
        except:
            pass
    return pd.DataFrame(rows)

static_df = build_static_ohlc()

# ================= PANCHAK LOGIC ================= #
def compute_panchak(df):
    out = []
    for s in SYMBOLS:
        d = df[df.Symbol == s]
        if d.empty:
            continue

        top_high = d.High.max()
        top_low = d.Low.min()
        diff = round(top_high - top_low, 2)
        bt = round(top_high + diff, 2)
        stp = round(top_low - diff, 2)

        out.append({
            "Symbol": s,
            "TOP_HIGH": round(top_high, 2),
            "TOP_LOW": round(top_low, 2),
            "DIFF": diff,
            "BT": bt,
            "ST": stp
        })
    return pd.DataFrame(out)

panchak_df = compute_panchak(static_df)

# ================= LIVE DATA ================= #
@st.cache_data(ttl=30)
def fetch_live():
    rows = []
    quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])

    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue

        ltp = q["last_price"]
        yclose = q["ohlc"]["close"]
        change = round(ltp - yclose, 2)
        pct = round((change / yclose) * 100, 2)

        rows.append({
            "Symbol": s,
            "LTP": round(ltp, 2),
            "Today_High": round(q["ohlc"]["high"], 2),
            "Today_Low": round(q["ohlc"]["low"], 2),
            "Change": change,
            "Change_%": pct,
            "Y_Close": round(yclose, 2),
            "Y_High": round(q["ohlc"]["high"], 2),
            "Y_Low": round(q["ohlc"]["low"], 2),
        })
    return pd.DataFrame(rows)

live_df = fetch_live()

# ================= MERGE ================= #
df = live_df.merge(panchak_df, on="Symbol", how="left")

# ================= NEAR LOGIC ================= #
def calc_near(r):
    if r.LTP >= r.TOP_HIGH or r.LTP <= r.TOP_LOW:
        return ""
    if (r.TOP_HIGH - r.LTP) <= (r.LTP - r.TOP_LOW):
        return f"↑ {round(r.TOP_HIGH - r.LTP,1)}"
    return f"↓ {round(r.LTP - r.TOP_LOW,1)}"

df["NEAR"] = df.apply(calc_near, axis=1)

# ================= STYLING ================= #
def near_style(v):
    if isinstance(v, str):
        if v.startswith("↑"):
            return "color: green; font-weight:bold"
        if v.startswith("↓"):
            return "color: red; font-weight:bold"
    return ""

def row_highlight(r):
    if r.LTP >= r.TOP_HIGH:
        return ["background-color:#d4f7d4"] * len(r)
    if r.LTP <= r.TOP_LOW:
        return ["background-color:#f7d4d4"] * len(r)
    return [""] * len(r)

styled = (
    df.style
    .applymap(near_style, subset=["NEAR"])
    .apply(row_highlight, axis=1)
    .format("{:.2f}", subset=[
        "LTP","Today_High","Today_Low","Change","Change_%",
        "Y_Close","Y_High","Y_Low","TOP_HIGH","TOP_LOW","DIFF","BT","ST"
    ])
)

# ================= UI ================= #
st.title("📊 DEC PANCHAK – LIVE KITE DASHBOARD")
st.caption("Live LTP | Panchak | Auto refresh every 60 seconds")

st.info(f"⏱ Last refresh: {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")

tabs = st.tabs(["🔥 Panchak", "ℹ INFO"])

with tabs[0]:
    st.dataframe(styled, use_container_width=True)

with tabs[1]:
    st.markdown("""
    **Panchak Logic**
    - TOP HIGH = Max High of given dates  
    - TOP LOW = Min Low of given dates  
    - DIFF = TOP HIGH - TOP LOW  
    - BT = TOP HIGH + DIFF  
    - ST = TOP LOW - DIFF  
    - NEAR shows nearest side (↑ / ↓)  
    """)

st.markdown(
    f"""
    <meta http-equiv="refresh" content="{REFRESH_SEC}">
    """,
    unsafe_allow_html=True
)
