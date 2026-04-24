# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – HOLIDAY AWARE)
# ==========================================================

import os, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz
from streamlit_autorefresh import st_autorefresh
from io import BytesIO


# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
#st.markdown(
 #   """
  #  <style>
   #     .block-container {
    #        padding-top: 1rem;
     #       padding-bottom: 0rem;
      #  }
   # </style>
  #  """,
 #   unsafe_allow_html=True
#)

IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

INST_FILE    = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE   = os.path.join(CACHE_DIR, "daily_ohlc.csv")
WEEKLY_FILE  = os.path.join(CACHE_DIR, "weekly_ohlc.csv")
MONTHLY_FILE = os.path.join(CACHE_DIR, "monthly_ohlc.csv")
PANCHAK_FILE = os.path.join(CACHE_DIR, "panchak_static.csv")

API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"

# ================= NSE HOLIDAYS – 2026 =================
NSE_HOLIDAYS = {
    date(2026,1,15), date(2026,1,26), date(2026,3,3),
    date(2026,3,26), date(2026,3,31), date(2026,4,3),
    date(2026,4,14), date(2026,5,1), date(2026,5,28),
    date(2026,6,26), date(2026,9,14), date(2026,10,2),
    date(2026,10,20), date(2026,11,10), date(2026,11,24),
    date(2026,12,25)
}

# ================= SYMBOL MASTER =================
SYMBOL_META = {
    "NIFTY":     "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
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

# ================= PANCHAK STATIC DATES =================
#PANCHAK_DATES = [
 #   date(2026,1,21),
  #  date(2026,1,22),
   # date(2026,1,23),
    #date(2026,1,24),
    #date(2026,1,25),
#]

PANCHAK_DATES = [
    date(2025,12,24),
    date(2025,12,25),
    date(2025,12,26),
    date(2025,12,27),
    date(2025,12,29),
]

# ================= KITE INIT =================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(open(ACCESS_TOKEN_FILE).read().strip())

# ================= INSTRUMENTS =================
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def get_token(symbol):
    name = SYMBOL_META.get(symbol, symbol)
    row = inst[inst.tradingsymbol == name]
    return None if row.empty else int(row.iloc[0].instrument_token)

def kite_symbol(symbol):
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

# ================= TRADING DAY HELPERS =================
def last_trading_day(d):
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d

@st.cache_data(ttl=3600)
def fetch_yesterday_ohlc(token):
    d = last_trading_day(date.today() - timedelta(days=1))
    bars = kite.historical_data(token, d, d, "day")
    if not bars:
        return None, None
    b = bars[0]
    return round(b["high"],2), round(b["low"],2), round(b["close"],2)   # yest close added here

# ================= OHLC BUILDERS (UNCHANGED) =================
def build_period_file(path, days):
    if os.path.exists(path):
        return
    rows = []
    today = date.today()
    start = today - timedelta(days=days)
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        bars = kite.historical_data(tk, start, today, "day")
        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol": s,
            "HIGH": round(dfb["high"].max(),2),
            "LOW":  round(dfb["low"].min(),2)
        })
        time.sleep(0.35)
    pd.DataFrame(rows).to_csv(path, index=False)

build_period_file(DAILY_FILE, 7)
build_period_file(WEEKLY_FILE, 14)
build_period_file(MONTHLY_FILE, 40)

# ================= PANCHAK STATIC =================
def build_panchak():
    if os.path.exists(PANCHAK_FILE):
        return
    rows = []
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        bars = kite.historical_data(tk, PANCHAK_DATES[0], PANCHAK_DATES[-1], "day")
        dfb = pd.DataFrame(bars)
        th = dfb["high"].max()
        tl = dfb["low"].min()
        diff = th - tl
        rows.append({
            "Symbol": s,
            "TOP_HIGH": round(th,2),
            "TOP_LOW":  round(tl,2),
            "DIFF":     round(diff,2),
            "BT":       round(th+diff,2),
            "ST":       round(tl-diff,2)
        })
        time.sleep(0.35)
    pd.DataFrame(rows).to_csv(PANCHAK_FILE, index=False)

build_panchak()

# ================= LIVE DATA =================
@st.cache_data(ttl=55)
def live_data():
    quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    rows = []

    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue

        tk = get_token(s)
        #yh, yl = fetch_yesterday_ohlc(tk)
        yh, yl, yc = fetch_yesterday_ohlc(tk)


        ltp = q["last_price"]
        pc = q["ohlc"]["close"]
        chg = ltp - pc

        rows.append({
            "Symbol": s,
            "LTP": round(ltp,2),
            "LIVE_HIGH": round(q["ohlc"]["high"],2),
            "LIVE_LOW": round(q["ohlc"]["low"],2),
            "YEST_HIGH": yh,
            "YEST_LOW": yl,
            "YEST_CLOSE": yc,
            "CHANGE": round(chg,2),
            "CHANGE_%": round((chg/pc)*100,2) if pc else 0
        })
    return pd.DataFrame(rows)

# ================= MERGE =================
df = (
    live_data()
    .merge(pd.read_csv(DAILY_FILE), on="Symbol", how="left")
    .merge(pd.read_csv(WEEKLY_FILE), on="Symbol", how="left", suffixes=("","_W"))
    .merge(pd.read_csv(MONTHLY_FILE), on="Symbol", how="left", suffixes=("","_M"))
    .merge(pd.read_csv(PANCHAK_FILE), on="Symbol", how="left")
)

# ================= NEAR / GAIN =================
def near(r):
    if r.LTP >= r.TOP_HIGH: return "🟢 ↑ BREAK"
    if r.LTP <= r.TOP_LOW:  return "🔴 ↓ BREAK"
    return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}" if (r.TOP_HIGH-r.LTP) <= (r.LTP-r.TOP_LOW) else f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

df["NEAR"] = df.apply(near, axis=1)

def gain(r):
    if r.LTP > r.TOP_HIGH: return round(r.LTP-r.TOP_HIGH,2)
    if r.LTP < r.TOP_LOW:  return round(r.LTP-r.TOP_LOW,2)
    return ""

df["GAIN"] = df.apply(gain, axis=1)

# ================= UI =================
#st.title("📊 Panchak + Breakout Dashboard")
st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")
st_autorefresh(interval=60_000, key="refresh")

PANCHAK_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "GAIN",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

panchak_view = df[PANCHAK_COLUMNS]

TOP_HIGH_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "DIFF",
    "BT",
    "ST",
    "YEST_HIGH",
    "YEST_LOW"
]

TOP_HIGH_view = df[TOP_HIGH_COLUMNS]

TOP_LOW_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "DIFF",
    "BT",
    "ST",
    "YEST_HIGH",
    "YEST_LOW"
]

TOP_LOW_view = df[TOP_LOW_COLUMNS]

NEAR_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

NEAR_view = df[NEAR_COLUMNS]

DAILY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

DAILY_view = df[DAILY_COLUMNS]

WEEKLY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

WEEKLY_view = df[WEEKLY_COLUMNS]

MONTHLY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

MONTHLY_view = df[MONTHLY_COLUMNS]

def export_all_tabs_to_excel():
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        panchak_view.to_excel(writer, sheet_name="PANCHAK", index=False)
        top_high_df.to_excel(writer, sheet_name="TOP_HIGH", index=False)
        top_low_df.to_excel(writer, sheet_name="TOP_LOW", index=False)

        daily_up.to_excel(writer, sheet_name="DAILY_UP", index=False)
        daily_down.to_excel(writer, sheet_name="DAILY_DOWN", index=False)

        weekly_up.to_excel(writer, sheet_name="WEEKLY_UP", index=False)
        weekly_down.to_excel(writer, sheet_name="WEEKLY_DOWN", index=False)

        monthly_up.to_excel(writer, sheet_name="MONTHLY_UP", index=False)
        monthly_down.to_excel(writer, sheet_name="MONTHLY_DOWN", index=False)

    output.seek(0)
    return output


tabs = st.tabs([
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    " NEAR",
    "📈 DAILY BREAKS",
    "📊 WEEKLY BREAKS",
    "📅 MONTHLY BREAKS",
    "ℹ️ INFO"
])

with tabs[0]:
    #st.dataframe(df, use_container_width=True)
    st.subheader("🪐 Panchak – Full View")
    st.dataframe(panchak_view, width="stretch",height=2800)

with tabs[1]:
    #st.dataframe(df[df.LTP >= df.TOP_HIGH])
    ##st.subheader(" TOP_HIGH_view – Full View")
    #st.dataframe(TOP_HIGH_view, width="stretch")
    #1
    #top_high_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]

    #if top_high_df.empty:
     #   st.info("No TOP HIGH breakouts yet")
    #else:
     #   st.subheader("🟢 TOP HIGH – Breakouts")
      #  st.dataframe(top_high_df, use_container_width=True)

    #2
    top_high_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]

    st.subheader("🟢 TOP HIGH – Breakouts")
    st.dataframe(top_high_df, use_container_width=True,height=2000)

with tabs[2]:
    #st.dataframe(df[df.LTP <= df.TOP_LOW])
    top_low_df = df.loc[df.LTP <= df.TOP_LOW, TOP_LOW_COLUMNS]

    st.subheader("🟢 TOP LOW – Breakouts")
    st.dataframe(top_low_df, use_container_width=True,height=2000)

with tabs[3]:
    st.subheader("📈 NEAR (Between TOP_LOW & TOP_HIGH)")

    near_df = df.loc[
        (df.LTP > df.TOP_LOW) & (df.LTP < df.TOP_HIGH),
        [
            "Symbol",
            "LTP",
            "TOP_HIGH",
            "TOP_LOW",
            "NEAR",
            "LIVE_HIGH",
            "LIVE_LOW",
            "CHANGE",
            "CHANGE_%",
            "YEST_HIGH",
            "YEST_LOW"
        ]
    ]

    if near_df.empty:
        st.info("No stocks currently between TOP_HIGH and TOP_LOW")
    else:
        row_height = 35
        table_height = min(1200, 60 + len(near_df) * row_height)

        st.dataframe(
            near_df,
            use_container_width=True,
            height=table_height
        )
    near_df = near_df.sort_values(by="NEAR")
    near_df["DIST_%"] = ((near_df.TOP_HIGH - near_df.LTP) / near_df.LTP * 100).round(2)

    


with tabs[4]:
    #st.dataframe(df[df.LTP >= df.YEST_HIGH])
    #st.dataframe(df[df.LTP <= df.YEST_LOW])
    st.subheader("📈 DAILY BREAKS – Above YEST HIGH")
    daily_up = df.loc[df.LTP >= df.YEST_HIGH, DAILY_COLUMNS]
    st.dataframe(daily_up, use_container_width=True)

    st.subheader("📉 DAILY BREAKS – Below YEST LOW")
    daily_down = df.loc[df.LTP <= df.YEST_LOW, DAILY_COLUMNS]
    st.dataframe(daily_down, use_container_width=True)

with tabs[5]:
    #st.dataframe(df[df.LTP >= df.HIGH_W])
    #st.dataframe(df[df.LTP <= df.LOW_W])
    st.subheader("📊 WEEKLY BREAKS – Above WEEK HIGH")
    weekly_up = df.loc[df.LTP >= df.HIGH_W, WEEKLY_COLUMNS]
    st.dataframe(weekly_up, use_container_width=True)

    st.subheader("📊 WEEKLY BREAKS – Below WEEK LOW")
    weekly_down = df.loc[df.LTP <= df.LOW_W, WEEKLY_COLUMNS]
    st.dataframe(weekly_down, use_container_width=True)

with tabs[6]:
    #st.dataframe(df[df.LTP >= df.HIGH_M])
    #st.dataframe(df[df.LTP <= df.LOW_M])
    st.subheader("📅 MONTHLY BREAKS – Above MONTH HIGH")
    monthly_up = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_up, use_container_width=True)

    st.subheader("📅 MONTHLY BREAKS – Below MONTH LOW")
    monthly_down = df.loc[df.LTP <= df.LOW_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_down, use_container_width=True)

with tabs[7]:
    #st.write("✔ Holiday aware yesterday logic")
    #st.write("✔ Excel matched")
    #st.write("✔ No features removed")

    st.divider()
    st.subheader("📤 Export Dashboard Data")

    excel_file = export_all_tabs_to_excel()

    st.download_button(
        label="📥 Download Full Dashboard (Excel)",
        data=excel_file,
        file_name=f"Panchak_Dashboard_{datetime.now(IST).strftime('%d_%b_%Y_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

