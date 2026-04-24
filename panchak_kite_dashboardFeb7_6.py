# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – HOLIDAY AWARE)
# ==========================================================

import os
#import time
from datetime import datetime, timedelta, date, time
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz
from streamlit_autorefresh import st_autorefresh
from io import BytesIO
import smtplib
from email.message import EmailMessage
import numpy as np
import time as tm
#from astro_logic import get_astro_score
from astro_time import get_time_signal
from astro_logic import get_astro_score, get_future_astro



EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO = ["uppala.wla@gmail.com"]
EMAIL_ENABLED = True
EMAIL_MAX_PER_DAY = 40        # safe under Gmail limit
EMAIL_COOLDOWN_MIN = 10       # minutes between emails

EMAIL_META_FILE = "CACHE/email_meta.json"
EMAIL_DEDUP_FILE = "CACHE/email_dedup.csv"
ALERTS_DEDUP_FILE = "CACHE/alerts_dedup.csv"

if "alerts" not in st.session_state:
    st.session_state.alerts = []

if "alert_keys" not in st.session_state:
    st.session_state.alert_keys = set()




# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
st.markdown("""
<style>
/* Section headers */
.section-green {background:#e8f5e9;padding:8px;border-radius:6px;}
.section-red {background:#fdecea;padding:8px;border-radius:6px;}
.section-yelLIVE_LOW {background:#fff8e1;padding:8px;border-radius:6px;}
.section-blue {background:#e3f2fd;padding:8px;border-radius:6px;}
.section-purple {background:#f3e5f5;padding:8px;border-radius:6px;}
.section-orange {background:#fff3e0;padding:8px;border-radius:6px;}

/* Table tweaks */
thead tr th {
    background-color:#f5f7fa !important;
    font-weight:600 !important;
}
</style>
""", unsafe_allow_html=True)


IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

#OHLC_FILE = os.path.join(CACHE_DIR, "ohlc_60d.csv")
INST_FILE    = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE   = os.path.join(CACHE_DIR, "daily_ohlc.csv")
WEEKLY_FILE  = os.path.join(CACHE_DIR, "weekly_ohlc.csv")
MONTHLY_FILE = os.path.join(CACHE_DIR, "monthly_ohlc.csv")
#PANCHAK_FILE = os.path.join(CACHE_DIR, "panchak_static.csv")
#EMA_FILE = os.path.join(CACHE_DIR, "ema_20_50.csv")


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
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA",
    "BANKINDIA","BANKNIFTY","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULI","IEX","INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND","IRCTC","IRFC","IREDA","ITC",
    "JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
    "MPHASIS","MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NIFTY","NTPC","NUVAMA","NYKAA","NATIONALUM",
    "OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM","PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD",
    "PIDILITIND","PIIND","PNB","PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PREMIERENE","PRESTIGE","PPLPHARMA",
    "RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SAMMAANCAP","SBICARD","SBILIFE","SBIN","SHREECEM","SHRIRAMFIN",
    "SIEMENS","SOLARINDS","SRF","SUNPHARMA","SUPREMEIND","SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER","TATATECH","TATASTEEL","TCS","TECHM",
    "TIINDIA","TITAN","TMPV","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK","UNITDSPR","UPL","VBL","VEDL","VOLTAS","WAAREEENER","WIPRO","ZYDUSLIFE"
]


#SYMBOLS = ["NIFTY", "BANKNIFTY"] + STOCKS
INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "FINNIFTY",
    "NIFTYIT",
    "NIFTYFMCG",
    "NIFTYPHARMA",
    "NIFTYMETAL",
    "NIFTYAUTO",
    "NIFTYENERGY",
    "NIFTYPSUBANK",
]

SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS

SYMBOL_META.update({
    "FINNIFTY": "FINNIFTY",
    "NIFTYIT": "NIFTY IT",
    "NIFTYFMCG": "NIFTY FMCG",
    "NIFTYPHARMA": "NIFTY PHARMA",
    "NIFTYMETAL": "NIFTY METAL",
    "NIFTYAUTO": "NIFTY AUTO",
    "NIFTYENERGY": "NIFTY ENERGY",
    "NIFTYPSUBANK": "NIFTY PSU BANK",
})

# ================= PANCHAK STATIC DATES =================
PANCHAK_DATES = [
    date(2026,2,17),
    date(2026,2,18),
    date(2026,2,19),
    date(2026,2,20),
    #date(2026,2,25),
]

# FEB 2026
#PANCHAK_DATES = [
 #   date(2026,2,17),
  #  date(2026,2,18),
   # date(2026,2,19),
    #date(2026,2,20),
    
#]

PANCHAK_START = date(2026, 2, 17)
PANCHAK_END   = date(2026, 2, 20)

PANCHAK_DATA_FILE = os.path.join(CACHE_DIR, "panchak_data.csv")
PANCHAK_META_FILE = os.path.join(CACHE_DIR, "panchak_meta.csv")
ALERTS_LOG_FILE = "CACHE/alerts_log.csv"



# ================= KITE INIT =================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(open(ACCESS_TOKEN_FILE).read().strip())


########################            UI Controls To Add

st.sidebar.header("🎥 Backtest Replay")

replay_mode = st.sidebar.toggle("Enable Replay Mode")

selected_date = st.sidebar.date_input("Select Date")

speed = st.sidebar.selectbox("Speed", ["Real 5 Min", "Fast (1 sec)"])

play = st.sidebar.button("▶ Start")
pause = st.sidebar.button("⏸ Pause")
col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button("⏮ Back"):
        st.session_state.replay_index = max(
            0, st.session_state.replay_index - 1
        )

with col2:
    if st.button("⏭ Next"):
        st.session_state.replay_index += 1


###############     Core Replay Engine
if "replay_index" not in st.session_state:
    st.session_state.replay_index = 0

if "replay_running" not in st.session_state:
    st.session_state.replay_running = False




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

    try:
        d = last_trading_day(date.today() - timedelta(days=1))

        bars = kite.historical_data(token, d, d, "day")

        if not bars:
            return None, None, None, None

        b = bars[0]

        return (
            round(b["open"], 2),
            round(b["high"], 2),
            round(b["low"], 2),
            round(b["close"], 2),
            round(b["volume"], 2)
        )

    except Exception as e:
        # 🔒 Prevent app crash
        return None, None, None, None, None



# ================= OHLC BUILDERS (UNCHANGED) =================
# ==========================================================
# ✅ PREVIOUS PERIOD OHLC (DAILY / WEEKLY / MONTHLY)
# ==========================================================

def dated_file(name):
    d = last_trading_day(date.today())
    return os.path.join(CACHE_DIR, f"{name}_{d}.csv")


def previous_week_range():
    end = last_trading_day(date.today())
    last_week_end = end - timedelta(days=end.weekday() + 1)
    last_week_start = last_week_end - timedelta(days=4)
    return last_week_start, last_week_end


def previous_month_range():
    first_this_month = date.today().replace(day=1)
    last_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1)
    return first_prev_month, last_prev_month


def build_daily_ohlc():
    path = dated_file("daily_ohlc")
    if os.path.exists(path):
        return path

    d = last_trading_day(date.today() - timedelta(days=1))
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, d, d, "day")
        if not bars:
            continue

        b = bars[0]
        rows.append({
            "Symbol": s,
            "OPEN_D":  b["open"],
            "HIGH_D":  b["high"],
            "LOW_D":   b["low"],
            "CLOSE_D": b["close"],
            "VOLUME_D": b["volume"]
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def build_weekly_ohlc():
    path = dated_file("weekly_ohlc")
    if os.path.exists(path):
        return path

    start, end = previous_week_range()
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, start, end, "day")
        if not bars:
            continue

        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol":   s,
            "OPEN_W": dfb.iloc[0]["open"],
            "HIGH_W": dfb["high"].max(),
            "LOW_W": dfb["low"].min(),
            "CLOSE_W": dfb.iloc[-1]["close"],
            "VOLUME_W": dfb["volume"].sum()

        })

        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def build_monthly_ohlc():
    path = dated_file("monthly_ohlc")
    if os.path.exists(path):
        return path

    start, end = previous_month_range()
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, start, end, "day")
        if not bars:
            continue

        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol":   s,
            "OPEN_M":   dfb.iloc[0]["open"],
            "HIGH_M":   dfb["high"].max(),
            "LOW_M":    dfb["low"].min(),
            "CLOSE_M":  dfb.iloc[-1]["close"],
            "VOLUME_M": dfb["volume"].sum()
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
    return path


# -------- BUILD OR LOAD FILES (AUTO) --------
DAILY_FILE   = build_daily_ohlc()
WEEKLY_FILE  = build_weekly_ohlc()
MONTHLY_FILE = build_monthly_ohlc()


# ================= PANCHAK STATIC =================

# ================= PANCHAK STATIC (VALIDATED CACHE – OPTION A) =================

def is_panchak_cache_valid():
    """
    Cache is valid ONLY if Panchak dates in meta file
    exactly match script-defined PANCHAK_START / PANCHAK_END
    """
    if not os.path.exists(PANCHAK_META_FILE):
        return False

    try:
        meta = pd.read_csv(PANCHAK_META_FILE)

        file_start = pd.to_datetime(
            meta.loc[meta["key"] == "start_date", "value"].values[0]
        ).date()

        file_end = pd.to_datetime(
            meta.loc[meta["key"] == "end_date", "value"].values[0]
        ).date()

        return file_start == PANCHAK_START and file_end == PANCHAK_END

    except Exception:
        return False


def build_panchak_files():
    """
    Builds Panchak DATA file + META file
    Called ONLY when cache is invalid
    """
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(
            tk,
            PANCHAK_DATES[0],
            PANCHAK_DATES[-1],
            "day"
        )

        dfb = pd.DataFrame(bars)
        if dfb.empty:
            continue

        th = dfb["high"].max()
        tl = dfb["low"].min()
        diff = th - tl

        rows.append({
            "Symbol": s,
            "TOP_HIGH": round(th, 2),
            "TOP_LOW":  round(tl, 2),
            "DIFF":     round(diff, 2),
            "BT":       round(th + diff, 2),
            "ST":       round(tl - diff, 2),
        })

        tm.sleep(0.35)

    # 🔒 Safety: don’t write empty data
    if not rows:
        return

    # --- DATA FILE ---
    pd.DataFrame(rows).to_csv(PANCHAK_DATA_FILE, index=False)

    # --- META FILE ---
    meta_df = pd.DataFrame([
        {"key": "start_date", "value": PANCHAK_START.isoformat()},
        {"key": "end_date",   "value": PANCHAK_END.isoformat()},
    ])

    meta_df.to_csv(PANCHAK_META_FILE, index=False)


# ---------- PANCHAK LOAD (AUTO-VALIDATED) ----------
if not is_panchak_cache_valid():
    build_panchak_files()

panchak_df = pd.read_csv(PANCHAK_DATA_FILE)

panchak_df = (
    panchak_df
    .sort_values("Symbol")
    .drop_duplicates(subset=["Symbol"], keep="last")
)





# ================= LIVE DATA =================
# ================= LIVE DATA (SAFE, HOLIDAY-AWARE) =================
@st.cache_data(ttl=55)
def live_data():
    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except Exception as e:
        # Kite down / holiday / 503 / rate limit
        st.warning("⚠️ Live data not available (Market closed or Kite issue)")
        return pd.DataFrame(columns=[
            "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
            "YEST_OPEN","YEST_HIGH","YEST_LOW","YEST_CLOSE",
            "CHANGE","CHANGE_%"
        ])

    rows = []

    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue

        tk = get_token(s)
        yo, yh, yl, yc, yv = fetch_yesterday_ohlc(tk)

        ltp = q["last_price"]
        pc = q["ohlc"]["close"]
        chg = ltp - pc if pc else 0
        live_volume = q.get("volume", 0)

        rows.append({
            "Symbol": s,
            "LTP": round(ltp, 2),
            "LIVE_OPEN": round(q["ohlc"]["open"], 2),
            "LIVE_HIGH": round(q["ohlc"]["high"], 2),
            "LIVE_LOW": round(q["ohlc"]["low"], 2),
            "LIVE_VOLUME": live_volume,   # ✅ LIVE DAY VOLUME
            #"live_vol": live_volume,
            "YEST_OPEN": yo,
            "YEST_HIGH": yh,
            "YEST_LOW": yl,
            "YEST_CLOSE": yc,
            "YEST_VOL": yv,
            "CHANGE": round(chg, 2),
            "CHANGE_%": round((chg / pc) * 100, 2) if pc else 0
        })

        


    return pd.DataFrame(rows)


##########################################################          BAR REPLAY -- BACKETEST DATA



###################         Example Code – Download 1 Month 5-Min Data

def download_intraday_data(symbol, interval="5minute", days=30):

    tk = get_token(symbol)
    if not tk:
        return None

    end = datetime.now()
    start = end - timedelta(days=days)

    try:
        bars = kite.historical_data(
            tk,
            start,
            end,
            interval
        )
    except Exception as e:
        print("Error:", e)
        return None

    df = pd.DataFrame(bars)
    df["date"] = pd.to_datetime(df["date"])

    # Save to CACHE
    path = f"CACHE/{symbol}_{interval}.csv"
    df.to_csv(path, index=False)

    return df

############        Download for All Symbols
def build_intraday_database(interval="5minute", days=30):

    for s in SYMBOLS:
        print("Downloading:", s)
        download_intraday_data(s, interval, days)
        tm.sleep(0.35)   # prevent rate limit

if st.sidebar.button("Download 5-Min Data (30 Days)"):
    build_intraday_database("5minute", 30)

#build_intraday_database("5minute", 30)

#build_intraday_database("1minute", 15)

#####################           Load intraday database ONCE and cache it

@st.cache_data(show_spinner=False)
def load_all_intraday_data():
    data_dict = {}
    for s in SYMBOLS:
        path = os.path.join(CACHE_DIR, f"{s}_5minute.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            df["date"] = pd.to_datetime(df["date"])
            data_dict[s] = df
    return data_dict


###############         Load 5-Min Data For Selected Date

def preload_backtest_ohlc(date_selected):

    if "backtest_ohlc_cache" not in st.session_state:
        st.session_state.backtest_ohlc_cache = {}

    if date_selected in st.session_state.backtest_ohlc_cache:
        return st.session_state.backtest_ohlc_cache[date_selected]

    intraday_db = load_all_intraday_data()

    daily_rows = []
    weekly_rows = []
    monthly_rows = []

    prev_day = last_trading_day(date_selected - timedelta(days=1))

    for s, data in intraday_db.items():

        # --- Get selected day ---
        day_data = data[data["date"].dt.date == date_selected]
        prev_data = data[data["date"].dt.date == prev_day]

        if day_data.empty or prev_data.empty:
            continue

        # === YEST OHLC ===
        y_open = prev_data.iloc[0]["open"]
        y_high = prev_data["high"].max()
        y_low  = prev_data["low"].min()
        y_close = prev_data.iloc[-1]["close"]
        y_vol = prev_data["volume"].sum()

        daily_rows.append({
            "Symbol": s,
            "OPEN_D": y_open,
            "HIGH_D": y_high,
            "LOW_D": y_low,
            "CLOSE_D": y_close,
            "VOLUME_D": y_vol
        })

        # === WEEK (last 5 trading days before selected date) ===
        #week_start = prev_day - timedelta(days=7)
        #week_start = prev_day - timedelta(days=10)  # buffer for weekend
        # === EXACT LAST 5 TRADING DAYS ===
        # === WEEK (last 5 trading days before selected date) ===
        trading_days = sorted(data["date"].dt.date.unique())

        past_days = [d for d in trading_days if d < date_selected]

        last_5_days = past_days[-5:]

        week_data = data[data["date"].dt.date.isin(last_5_days)]

        if not week_data.empty:
            weekly_rows.append({
                "Symbol": s,
                "OPEN_W": week_data.iloc[0]["open"],
                "HIGH_W": week_data["high"].max(),
                "LOW_W": week_data["low"].min(),
                "CLOSE_W": week_data.iloc[-1]["close"],
                "VOLUME_W": week_data["volume"].sum()
            })

        if not week_data.empty:
            weekly_rows.append({
                "Symbol": s,
                "OPEN_W": week_data.iloc[0]["open"],
                "HIGH_W": week_data["high"].max(),
                "LOW_W": week_data["low"].min(),
                "CLOSE_W": week_data.iloc[-1]["close"],
                "VOLUME_W": week_data["volume"].sum()
            })

        # === MONTH (30 days before selected date) ===
        month_start = prev_day - timedelta(days=30)
        month_data = data[
            (data["date"].dt.date >= month_start) &
            (data["date"].dt.date <= prev_day)
        ]

        if not month_data.empty:
            monthly_rows.append({
                "Symbol": s,
                "OPEN_M": month_data.iloc[0]["open"],
                "HIGH_M": month_data["high"].max(),
                "LOW_M": month_data["low"].min(),
                "CLOSE_M": month_data.iloc[-1]["close"],
                "VOLUME_M": month_data["volume"].sum()
            })

    #daily_df = pd.DataFrame(daily_rows)
    #weekly_df = pd.DataFrame(weekly_rows)
    #monthly_df = pd.DataFrame(monthly_rows)
    daily_df = pd.DataFrame(daily_rows)
    if daily_df.empty:
        daily_df = pd.DataFrame(columns=[
            "Symbol","OPEN_D","HIGH_D","LOW_D","CLOSE_D","VOLUME_D"
        ])

    weekly_df = pd.DataFrame(weekly_rows)
    if weekly_df.empty:
        weekly_df = pd.DataFrame(columns=[
            "Symbol","OPEN_W","HIGH_W","LOW_W","CLOSE_W","VOLUME_W"
        ])

    monthly_df = pd.DataFrame(monthly_rows)
    if monthly_df.empty:
        monthly_df = pd.DataFrame(columns=[
            "Symbol","OPEN_M","HIGH_M","LOW_M","CLOSE_M","VOLUME_M"
        ])

    st.session_state.backtest_ohlc_cache[date_selected] = {
        "daily": daily_df,
        "weekly": weekly_df,
        "monthly": monthly_df
    }

    return st.session_state.backtest_ohlc_cache[date_selected]


if "replay_day_cache" not in st.session_state:
    st.session_state.replay_day_cache = {}

def preload_selected_day(date_selected):

    if date_selected in st.session_state.replay_day_cache:
        return st.session_state.replay_day_cache[date_selected]

    intraday_db = load_all_intraday_data()
    day_dict = {}

    for s, data in intraday_db.items():
        day_data = data[data["date"].dt.date == date_selected]
        day_data = day_data.sort_values("date")

        if not day_data.empty:
            day_dict[s] = day_data.reset_index(drop=True)

    st.session_state.replay_day_cache[date_selected] = day_dict
    return day_dict


def load_replay_data(date_selected):

    rows = []
    #intraday_db = load_all_intraday_data()
    intraday_db = preload_selected_day(date_selected)
    #for s in SYMBOLS:
    for s, data in intraday_db.items():

        # Selected day data
        day_data = data[data["date"].dt.date == date_selected]
        day_data = day_data.sort_values("date")

        if day_data.empty:
            continue

        # Candle pointer
        idx = min(
            st.session_state.replay_index,
            len(day_data)
        )

        if idx >= len(day_data):
            st.session_state.replay_running = False

        if idx == 0:
            idx = 1

        current_candle = day_data.iloc[idx - 1]
        st.session_state.current_replay_time = current_candle["date"]
        # 🔹 Previous day data
        #prev_day = date_selected - timedelta(days=1)
        #def previous_trading_day(d):
         #   while d.weekday() >= 5:
          #      d -= timedelta(days=1)
           # return d

        #prev_day = previous_trading_day(date_selected - timedelta(days=1))
        prev_day = last_trading_day(date_selected - timedelta(days=1))
        prev_data = data[data["date"].dt.date == prev_day]

        if prev_data.empty:
            continue

        y_high = prev_data["high"].max()
        y_low  = prev_data["low"].min()
        y_close = prev_data.iloc[-1]["close"]
        y_open = prev_data.iloc[0]["open"]
        y_vol = prev_data["volume"].sum()

        # 🔹 Current change calculation
        ltp = current_candle["close"]
        change = ltp - y_close
        change_pct = (change / y_close) * 100 if y_close else 0

        if idx >= len(day_data):
            st.session_state.replay_running = False
            idx = len(day_data)

        rows.append({
            "Symbol": s,
            "LTP": round(ltp,2),
            "LIVE_OPEN": current_candle["open"],
            "LIVE_HIGH": current_candle["high"],
            "LIVE_LOW": current_candle["low"],
            "LIVE_VOLUME": current_candle["volume"],

            "YEST_OPEN": y_open,
            "YEST_HIGH": y_high,
            "YEST_LOW": y_low,
            "YEST_CLOSE": y_close,
            "YEST_VOL": y_vol,

            "CHANGE": round(change,2),
            "CHANGE_%": round(change_pct,2)
        })

    df = pd.DataFrame(rows)

    # 🔥 FORCE all required columns to exist
    required_cols = [
        "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
        "LIVE_VOLUME","YEST_OPEN","YEST_HIGH","YEST_LOW",
        "YEST_CLOSE","YEST_VOL","CHANGE","CHANGE_%"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = 0

    return df


if "last_selected_date" not in st.session_state:
    st.session_state.last_selected_date = selected_date

if selected_date != st.session_state.last_selected_date:
    # 🔥 PRELOAD EVERYTHING WHEN DATE CHANGES
    if replay_mode:
        preload_selected_day(selected_date)
        preload_backtest_ohlc(selected_date)
    st.session_state.replay_index = 0
    st.session_state.last_selected_date = selected_date


##########      Play Logic
if play:
    st.session_state.replay_running = True

if pause:
    st.session_state.replay_running = False
###################                 Candle Advancement Logic
if replay_mode and st.session_state.replay_running:

    st.session_state.replay_index += 1

    #interval = 1000 if speed == "Fast (1 sec)" else 300000  # 5 mintues
    #interval = 1000 if speed == "Fast (1 sec)" else 100000   # 30000=30 seconds
    interval = 100 if speed == "Fast (1 sec)" else 30000

    st_autorefresh(interval=interval, key="replay_refresh")




# ================= MERGE =================
if replay_mode:
    base_df = load_replay_data(selected_date)
else:
    base_df = live_data()

# 🔥 FORCE required columns even if empty
REPLAY_REQUIRED = [
    "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
    "LIVE_VOLUME","YEST_OPEN","YEST_HIGH","YEST_LOW",
    "YEST_CLOSE","YEST_VOL","CHANGE","CHANGE_%"
]

for col in REPLAY_REQUIRED:
    if col not in base_df.columns:
        base_df[col] = 0

if replay_mode and "current_replay_time" in st.session_state:
    st.markdown(
        f"## 🕒 Replay Date: {selected_date} | Candle Time: "
        f"{st.session_state.current_replay_time.strftime('%H:%M')}"
    )
    # ===== Replay Progress Bar =====
    # ---- Slider Navigation ----
    #max_candles = 75  # approx full day
    intraday_db = preload_selected_day(selected_date)

    # get max candle length among all symbols
    max_candles = max(
        [len(v) for v in intraday_db.values()],
        default=75
    )

    slider_index = st.slider(
        "Manual Candle Navigation",
        min_value=0,
        max_value=max_candles,
        value=st.session_state.replay_index
    )

    st.session_state.replay_index = slider_index


if replay_mode:

    ohlc_cache = preload_backtest_ohlc(selected_date)
    # 🔥 SAFETY FIX
        # 🔥 HARD SAFETY
    for key in ["daily","weekly","monthly"]:
        if key not in ohlc_cache or ohlc_cache[key] is None:
            ohlc_cache[key] = pd.DataFrame(columns=["Symbol"])

        if "Symbol" not in ohlc_cache[key].columns:
            ohlc_cache[key]["Symbol"] = []

    df = (
        base_df
        .merge(ohlc_cache["daily"], on="Symbol", how="left")
        .merge(ohlc_cache["weekly"], on="Symbol", how="left")
        .merge(ohlc_cache["monthly"], on="Symbol", how="left")
        .merge(panchak_df, on="Symbol", how="left")
    )


else:
    df = (
        base_df
        .merge(pd.read_csv(DAILY_FILE), on="Symbol", how="left")
        .merge(pd.read_csv(WEEKLY_FILE), on="Symbol", how="left")
        .merge(pd.read_csv(MONTHLY_FILE), on="Symbol", how="left")
        .merge(panchak_df, on="Symbol", how="left")
    )
# 🔥 REMOVE duplicate suffix columns
df = df.loc[:, ~df.columns.duplicated()]

# ================= FIX-2: REMOVE DUPLICATE SYMBOLS =================
df = (
    df
    .sort_values("Symbol")
    .drop_duplicates(subset=["Symbol"], keep="last")
    .reset_index(drop=True)
)
# ==================================================================


if "LIVE_VOLUME" in df.columns:
    df["LIVE_VOLUME"] = pd.to_numeric(df["LIVE_VOLUME"], errors="coerce").fillna(0)


# ================= VOLUME % CALCULATION =================

# Ensure numeric
#df["LIVE_VOLUME"] = pd.to_numeric(df.get("LIVE_VOLUME", 0), errors="coerce").fillna(0)
#df["YEST_VOL"] = pd.to_numeric(df.get("YEST_VOL", 0), errors="coerce").fillna(0)
if "LIVE_VOLUME" not in df.columns:
    df["LIVE_VOLUME"] = 0

if "YEST_VOL" not in df.columns:
    df["YEST_VOL"] = 0

df["LIVE_VOLUME"] = pd.to_numeric(df["LIVE_VOLUME"], errors="coerce").fillna(0)
df["YEST_VOL"] = pd.to_numeric(df["YEST_VOL"], errors="coerce").fillna(0)


# Safe division
df["VOL_%"] = np.where(
    df["YEST_VOL"] > 0,
    ((df["LIVE_VOLUME"] - df["YEST_VOL"]) / df["YEST_VOL"]) * 100,
    0
).round(2)


# ================= SAFETY NET: REQUIRED LIVE COLUMNS =================

REQUIRED_COLS = [
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE"
]

if not df.empty:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        st.error(f"❌ Missing required columns: {missing}")
        st.stop()

#st.write("DEBUG DF COLUMNS:", df.columns.tolist())


# ================= NEAR / GAIN =================
def near(r):
    if r.LTP >= r.TOP_HIGH: return "🟢 ↑ BREAK"
    if r.LTP <= r.TOP_LOW:  return "🔴 ↓ BREAK"
    return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}" if (r.TOP_HIGH-r.LTP) <= (r.LTP-r.TOP_LOW) else f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

#df["NEAR"] = df.apply(near, axis=1)
if not df.empty:
    df["NEAR"] = df.apply(near, axis=1)
else:
    df["NEAR"] = ""

# ================= GAIN (SAFE, NUMERIC, ARROW-COMPATIBLE) =================

def gain(r):
    if pd.notna(r.TOP_HIGH) and r.LTP > r.TOP_HIGH:
        return round(r.LTP - r.TOP_HIGH, 2)
    if pd.notna(r.TOP_LOW) and r.LTP < r.TOP_LOW:
        return round(r.LTP - r.TOP_LOW, 2)
    return None  # IMPORTANT: None, not ""

df["GAIN"] = df.apply(gain, axis=1)
df["GAIN"] = pd.to_numeric(df["GAIN"], errors="coerce")




# =========================================================
# ROLLING OHLC (60 DAYS) + EMA20 / EMA50 (BULLETPROOF)
# =========================================================

OHLC_FILE = os.path.join(CACHE_DIR, "ohlc_60d.csv")
EMA_FILE  = os.path.join(CACHE_DIR, "ema_20_50.csv")


def last_trading_day(d):
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d


def build_or_update_ohlc_60d():
    """
    1️⃣ If file NOT present → build last 60 trading days
    2️⃣ If file present → append only latest trading day
    """

    today = date.today()
    end_day = last_trading_day(today)

    # ---------- CASE 1: FILE NOT EXISTS → FULL BUILD ----------
    if not os.path.exists(OHLC_FILE):
        rows = []

        start_day = end_day - timedelta(days=360)  # buffer to get 180 trading days

        for s in SYMBOLS:
            tk = get_token(s)
            if not tk:
                continue

            try:
                bars = kite.historical_data(
                    tk,
                    start_day,
                    end_day,
                    "day"
                )
            except:
                continue

            dfb = pd.DataFrame(bars)
            if dfb.empty:
                continue

            dfb["date"] = pd.to_datetime(dfb["date"]).dt.date
            dfb = dfb.sort_values("date").tail(60)

            for _, r in dfb.iterrows():
                rows.append({
                    "Symbol": s,
                    "date": r["date"],
                    "open":  r["open"],
                    "high":  r["high"],
                    "low":   r["low"],
                    "close": r["close"],
                    "volume": r["volume"],

                })

            tm.sleep(0.3)

        if rows:
            pd.DataFrame(rows).to_csv(OHLC_FILE, index=False)

        return

    # ---------- CASE 2: FILE EXISTS → DAILY APPEND ----------
    ohlc_df = pd.read_csv(OHLC_FILE)
    ohlc_df["date"] = pd.to_datetime(ohlc_df["date"]).dt.date

    if end_day in ohlc_df["date"].values:
        return  # already updated

    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        try:
            bars = kite.historical_data(
                tk,
                end_day,
                end_day,
                "day"
            )
        except:
            continue

        if not bars:
            continue

        b = bars[0]
        rows.append({
            "Symbol": s,
            "date": end_day,
            "open":  b["open"],
            "high":  b["high"],
            "low":   b["low"],
            "close": b["close"],
            "volume": b["volume"],

        })

        tm.sleep(0.25)

    if not rows:
        return

    new_df = pd.DataFrame(rows)

    final_df = (
        pd.concat([ohlc_df, new_df], ignore_index=True)
        .drop_duplicates(subset=["Symbol", "date"])
        .sort_values(["Symbol", "date"])
        .groupby("Symbol", as_index=False)
        .tail(60)
    )

    final_df.to_csv(OHLC_FILE, index=False)


def build_ema_from_ohlc():
    if not os.path.exists(OHLC_FILE):
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    df = pd.read_csv(OHLC_FILE)

    rows = []

    for s, g in df.groupby("Symbol"):
        if len(g) < 50:
            continue

        g = g.sort_values("date")
        g["EMA7"]  = g["close"].ewm(span=7).mean()
        g["EMA20"] = g["close"].ewm(span=20).mean()
        g["EMA50"] = g["close"].ewm(span=50).mean()

        rows.append({
            "Symbol": s,
            "EMA7": round(g.iloc[-1]["EMA7"], 2),
            "EMA20": round(g.iloc[-1]["EMA20"], 2),
            "EMA50": round(g.iloc[-1]["EMA50"], 2)
        })

    if not rows:
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    ema_df = pd.DataFrame(rows)
    ema_df.to_csv(EMA_FILE, index=False)
    return ema_df


# ONLY update OHLC in LIVE MODE
if not replay_mode:
    build_or_update_ohlc_60d()

ema_df = build_ema_from_ohlc()
df = df.merge(ema_df, on="Symbol", how="left")

# =========================================================
# MULTI-PERIOD DOWNTREND → EMA REVERSAL SCREENER
# =========================================================

ohlc_full = pd.read_csv(OHLC_FILE)
ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

def build_downtrend_reversal(days):
    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        past_close = g.iloc[-days]["close"]
        current_close = g.iloc[-1]["close"]

        # Downtrend condition
        if past_close <= current_close:
            continue

        # Get live row
        live_row = df[df["Symbol"] == s]

        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # EMA Reversal Confirmation
        if (
            r["LTP"] > r.get("EMA7", 0) and
            r["LTP"] > r.get("EMA20", 0) and
            r["LTP"] > r.get("EMA50", 0) and
            r.get("EMA20", 0) > r.get("EMA50", 0)
        ):
            rows.append({
                "Symbol": s,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                "EMA7": r.get("EMA7"),
                "EMA20": r.get("EMA20"),
                "EMA50": r.get("EMA50"),
                "Past_Close": past_close,
                "Current_Close": current_close,
                "Drop_%": round(((current_close - past_close) / past_close) * 100, 2)
            })

    return pd.DataFrame(rows)

down_1m_df = build_downtrend_reversal(22)
down_2m_df = build_downtrend_reversal(44)
down_3m_df = build_downtrend_reversal(66)
down_6m_df = build_downtrend_reversal(132)

# =========================================================
# MULTI-PERIOD DOWNTREND → EMA REVERSAL SCREENER
# =========================================================

#ohlc_full = pd.read_csv(OHLC_FILE)
#ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

def build_downtrend_reversal1(days):
    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        past_close = g.iloc[-days]["close"]
        current_close = g.iloc[-1]["close"]

        # Downtrend condition
        if past_close <= current_close:
            continue

        # Get live row
        live_row = df[df["Symbol"] == s]

        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # EMA Reversal Confirmation
        if (
            r["LTP"] > r.get("EMA7", 0) and
            r["LTP"] > r.get("EMA20", 0) and
            r["LTP"] > r.get("EMA50", 0) #and
            #r.get("EMA20", 0) > r.get("EMA50", 0)
        ):
            rows.append({
                "Symbol": s,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                "EMA7": r.get("EMA7"),
                "EMA20": r.get("EMA20"),
                "EMA50": r.get("EMA50"),
                "Past_Close": past_close,
                "Current_Close": current_close,
                "Drop_%": round(((current_close - past_close) / past_close) * 100, 2)
            })

    return pd.DataFrame(rows)

down_1m_df1 = build_downtrend_reversal1(22)
down_2m_df2 = build_downtrend_reversal1(44)
down_3m_df3 = build_downtrend_reversal1(66)
down_6m_df4 = build_downtrend_reversal1(132)


# =========================================================
# MULTI-PERIOD DOWNTREND → EMA REVERSAL AFTER UPTREND
# =========================================================

def build_uptrend_reversal2(days):
    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        past_close = g.iloc[-days]["close"]
        current_close = g.iloc[-1]["close"]

        # ✅ UPtrend condition
        if past_close >= current_close:
            continue

        live_row = df[df["Symbol"] == s]
        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # ✅ Bearish EMA breakdown
        if (
            r["LTP"] < r.get("EMA7", 0) and
            r["LTP"] < r.get("EMA20", 0) and
            r["LTP"] < r.get("EMA50", 0) and
            r.get("EMA7", 0) < r.get("EMA20", 0) < r.get("EMA50", 0)
        ):
            rows.append({
                "Symbol": s,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                "EMA7": r.get("EMA7"),
                "EMA20": r.get("EMA20"),
                "EMA50": r.get("EMA50"),
                "Past_Close": past_close,
                "Current_Close": current_close,
                "Rise_%": round(((current_close - past_close) / past_close) * 100, 2)
            })

    return pd.DataFrame(rows)

#up_1m_df1 = build_uptrend_reversal2(22)
#up_2m_df2 = build_uptrend_reversal2(44)
#up_3m_df3 = build_uptrend_reversal2(66)
#up_6m_df4 = build_uptrend_reversal2(132)

#def build_exhaustion_top(days=30):
# =========================================================
# EARLY WEAKNESS BEFORE BIG FALL (SMART EXIT ZONE)
# =========================================================

def build_early_top_weakness(days):

    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        recent = g.tail(days)

        highest_close = recent["close"].max()
        last_row = g.iloc[-1]
        prev_row = g.iloc[-2]

        # 🔹 Was in uptrend
        if g.iloc[-days]["close"] >= last_row["close"]:
            continue

        # 🔹 Near recent high (within 5%)
        if last_row["close"] < highest_close * 0.95:
            continue

        live_row = df[df["Symbol"] == s]
        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # 🔹 KEY CONDITION: LTP BELOW EMA7 (first weakness)
        if r["LTP"] >= r.get("EMA7", 0):
            continue

        # 🔹 But still above EMA20 (not full breakdown yet)
        if r["LTP"] <= r.get("EMA20", 0):
            continue

        # 🔹 EMA7 flattening or turning down
        ema7_series = g["close"].ewm(span=7).mean()
        if ema7_series.iloc[-1] >= ema7_series.iloc[-2]:
            continue

        rows.append({
            "Symbol": s,
            "LTP": r["LTP"],
            "EMA7": r.get("EMA7"),
            "EMA20": r.get("EMA20"),
            "CHANGE_%": r["CHANGE_%"],
            "Near_30D_High": highest_close,
            "Weakness": "LTP < EMA7"
        })

    return pd.DataFrame(rows)

#up_3m_df3 = build_early_top_weakness(30)
#up_6m_df4 = build_early_top_weakness(60)

up_1m_df1 = build_early_top_weakness(30)
up_2m_df2 = build_early_top_weakness(60)

# =========================================================
# STRONG EARLY TOP BREAKDOWN (INSTITUTIONAL EXIT)
# =========================================================

def build_refined_top_breakdown(days):

    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        recent = g.tail(days)

        highest_close = recent["close"].max()
        last_close = g.iloc[-1]["close"]

        # 🔹 Prior Uptrend (structure condition)
        if g.iloc[-days]["close"] >= last_close:
            continue

        # 🔹 Near recent high (within 7%)
        if last_close < highest_close * 0.93:
            continue

        live_row = df[df["Symbol"] == s]
        if live_row.empty:
            continue

        r = live_row.iloc[0]

        ema7 = r.get("EMA7", 0)
        ema20 = r.get("EMA20", 0)
        ema50 = r.get("EMA50", 0)

        # 🔴 Core Conditions You Requested
        if not (
            ema7 < ema20 and            # EMA7 below EMA20
            r["LTP"] < ema20 and        # Price below EMA20
            r["LTP"] > ema50            # Still above EMA50 (early stage)
        ):
            continue

        rows.append({
            "Symbol": s,
            "LTP": r["LTP"],
            "EMA7": ema7,
            "EMA20": ema20,
            "EMA50": ema50,
            "CHANGE_%": r["CHANGE_%"],
            "Distance_from_High_%": round(
                ((r["LTP"] - highest_close) / highest_close) * 100, 2
            )
        })

    return pd.DataFrame(rows)

up_3m_df3 = build_refined_top_breakdown(30)
up_6m_df4 = build_refined_top_breakdown(60)

# =========================================================
# EMA20–EMA50 + TOP LIVE_HIGH / TOP LIVE_LOW (SINGLE SOURCE OF TRUTH)
# =========================================================

ema_signal_df = df.dropna(
    subset=["EMA20", "EMA50", "TOP_HIGH", "TOP_LOW"]
).loc[
    (
        (df["LTP"] > df["EMA20"]) &
        (df["EMA20"] > df["EMA50"]) &
        (df["LTP"] > df["TOP_HIGH"])
    ) |
    (
        (df["LTP"] < df["EMA20"]) &
        (df["EMA20"] < df["EMA50"]) &
        (df["LTP"] < df["TOP_LOW"])
    ),
    [
        "Symbol",
        "TOP_HIGH",
        "TOP_LOW",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "GAIN",
        "EMA20",
        "EMA50"
        
        
    ]
].copy()

ema_signal_df["SIGNAL"] = ema_signal_df.apply(
    lambda r: "BUY" if r["LTP"] > r["EMA20"] else "SELL",
    axis=1
)

# ================= SPLIT BUY / SELL =================

ema_buy_df = ema_signal_df[ema_signal_df["SIGNAL"] == "BUY"].copy()
ema_sell_df = ema_signal_df[ema_signal_df["SIGNAL"] == "SELL"].copy()







# ================= UI =================
#st.title("📊 Panchak + Breakout Dashboard")
#st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")
#st_autorefresh(interval=10000_000, key="refresh")
#st_autorefresh(interval=300_000, key="refresh")
# ================= UI =================
from datetime import datetime, time

now = datetime.now(IST).time()

market_start = time(9, 0)
market_end   = time(15, 30)

st.caption(
    f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')}"
)

# Auto refresh only during market hours
if market_start <= now <= market_end:
    #st.caption("🔄 Auto refresh active (Market Hours)")
    st_autorefresh(interval=100 * 1000, key="refresh")   # 100 seconds
else:
    st.caption("⏸ Auto refresh paused (Market Closed)")


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


#import pandas as pd

def export_all_tabs_to_excel():
    output = BytesIO()

    def safe_export(writer, df_obj, sheet):
        if df_obj is not None and isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
            df_obj.to_excel(writer, sheet_name=sheet[:31], index=False)

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # ========= CORE =========
        safe_export(writer, panchak_view, "PANCHAK")
        safe_export(writer, TOP_HIGH_df, "TOP_HIGH")
        safe_export(writer, TOP_LOW_df, "TOP_LOW")
        safe_export(writer, near_df, "NEAR_ZONE")

        # ========= DAILY / WEEKLY / MONTHLY =========
        safe_export(writer, daily_up, "DAILY_UP")
        safe_export(writer, daily_down, "DAILY_DOWN")
        safe_export(writer, weekly_up, "WEEKLY_UP")
        safe_export(writer, weekly_down, "WEEKLY_DOWN")
        safe_export(writer, monthly_up, "MONTHLY_UP")
        safe_export(writer, monthly_down, "MONTHLY_DOWN")

        # ========= EMA =========
        safe_export(writer, ema_buy_df, "EMA_BUY")
        safe_export(writer, ema_sell_df, "EMA_SELL")
        safe_export(writer, daily_ema_buy, "DAILY_EMA_BUY")
        safe_export(writer, daily_ema_sell, "DAILY_EMA_SELL")
        safe_export(writer, weekly_ema_buy, "WEEKLY_EMA_BUY")
        safe_export(writer, weekly_ema_sell, "WEEKLY_EMA_SELL")

        # ========= SUPERTREND =========
        safe_export(writer, supertrend_df, "SUPERTREND")
        safe_export(writer, st_near_view, "ST_NEAR")

        # ========= OPTIONS =========
        safe_export(writer, STRONG_BUY_DF, "OPTIONS_STRONG")
        safe_export(writer, backtest_df, "OPTIONS_BACKTEST")

        # ========= PATTERNS =========
        safe_export(writer, ol_oh_df, "O_EQUALS_HL")
        safe_export(writer, LIVE_OPEN_LIVE_LOW_df, "O_EQ_L")
        safe_export(writer, LIVE_OPEN_LIVE_HIGH_df, "O_EQ_H")
        safe_export(writer, four_bar_df, "FOUR_BAR")

        # ========= FAKE BREAKOUTS =========
        safe_export(writer, fake_bull_df, "FAKE_BULL")
        safe_export(writer, fake_bear_df, "FAKE_BEAR")

        # ========= ALERTS =========
        if "alerts" in globals() and alerts:
            safe_export(writer, pd.DataFrame(alerts), "ALERTS")

        # ========= GAINERS / LOSERS =========
        safe_export(writer, gainers_df, "GAINERS")
        safe_export(writer, losers_df, "LOSERS")

    output.seek(0)
    return output
    safe_export(writer, ema_buy_df, f"EMA_BUY_{len(ema_buy_df)}")



from datetime import time

def is_market_hours():
    now_dt = datetime.now(IST)
    now = now_dt.time()

    if now_dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False

    return time(9,15) <= now <= time(15,30)


def is_email_allowed():
    now = datetime.now(IST).time()
    return time(9,15) <= now <= time(15,15)


# ================= BREAKOUT DATA (GLOBAL) =================

TOP_HIGH_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
TOP_LOW_df  = df.loc[df.LTP <= df.TOP_LOW,  TOP_LOW_COLUMNS]

near_df = df.loc[
    (df.LTP > df.TOP_LOW) & (df.LTP < df.TOP_HIGH),
    [
        "Symbol","LTP","TOP_HIGH","TOP_LOW","NEAR",
        "LIVE_HIGH","LIVE_LOW","CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW"
    ]
]

#daily_up   = df.loc[df.LTP >= df.YEST_HIGH, DAILY_COLUMNS]
#daily_down = df.loc[df.LTP <= df.YEST_LOW,  DAILY_COLUMNS]

# =========================================================
# DAILY UP – Clean Break Above YH (No Gap-Up)
# =========================================================
daily_up = df.loc[
    (df["LIVE_OPEN"] <= df["YEST_HIGH"]) & (df["LTP"] > df["YEST_HIGH"]), DAILY_COLUMNS ]

# =========================================================
# DAILY DOWN – Clean Break Below YL (No Gap-Down)
# =========================================================
daily_down = df.loc[(df["LIVE_OPEN"] >= df["YEST_LOW"]) & (df["LTP"] < df["YEST_LOW"]),DAILY_COLUMNS]


weekly_up   = df.loc[df.LTP >= df.HIGH_W, WEEKLY_COLUMNS]
weekly_down = df.loc[df.LTP <= df.LOW_W,  WEEKLY_COLUMNS]

monthly_up   = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
monthly_down = df.loc[df.LTP <= df.LOW_M,  MONTHLY_COLUMNS]

def notify_browser(title, symbols):
    if not symbols:
        return
    st.toast(f"{title}: {', '.join(symbols)}", icon="🚨")

def detect_new_entries(name, current_symbols):
    path = f"CACHE/{name}_prev.txt"

    # Convert everything to string safely
    current_symbols = [str(x) for x in current_symbols if pd.notna(x)]

    prev = set()
    if os.path.exists(path):
        with open(path, "r") as f:
            content = f.read().strip()
            if content:
                prev = set(content.split(","))

    curr = set(current_symbols)
    new = curr - prev

    # Write back safely
    with open(path, "w") as f:
        f.write(",".join(curr))

    return list(new)


new_TOP_HIGH = detect_new_entries(
    "TOP_HIGH",
    TOP_HIGH_df.Symbol.tolist()
)

notify_browser("🟢 New TOP LIVE_HIGH", new_TOP_HIGH)

new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

notify_browser("🔴 New TOP LIVE_LOW", new_TOP_LOW)

import json
from datetime import datetime, timedelta

def can_send_email():
    if not EMAIL_ENABLED:
        return False

    os.makedirs("CACHE", exist_ok=True)

    now = datetime.now()

    data = {
        "last_sent": None,
        "count_today": 0,
        "date": now.strftime("%Y-%m-%d")
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    # Reset daily counter
    if data["date"] != now.strftime("%Y-%m-%d"):
        data["date"] = now.strftime("%Y-%m-%d")
        data["count_today"] = 0
        data["last_sent"] = None

    # Daily limit
    if data["count_today"] >= EMAIL_MAX_PER_DAY:
        return False

    # Cooldown
    if data["last_sent"]:
        last = datetime.fromisoformat(data["last_sent"])
        if now - last < timedelta(minutes=EMAIL_COOLDOWN_MIN):
            return False

    return True


def record_email_sent():
    now = datetime.now()

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "last_sent": now.isoformat(),
        "count_today": 0
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    data["count_today"] += 1
    data["last_sent"] = now.isoformat()

    with open(EMAIL_META_FILE, "w") as f:
        json.dump(data, f)
import json
from datetime import datetime, timedelta

def can_send_email():
    if not EMAIL_ENABLED:
        return False

    os.makedirs("CACHE", exist_ok=True)

    now = datetime.now()

    data = {
        "last_sent": None,
        "count_today": 0,
        "date": now.strftime("%Y-%m-%d")
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    # Reset daily counter
    if data["date"] != now.strftime("%Y-%m-%d"):
        data["date"] = now.strftime("%Y-%m-%d")
        data["count_today"] = 0
        data["last_sent"] = None

    # Daily limit
    if data["count_today"] >= EMAIL_MAX_PER_DAY:
        return False

    # Cooldown
    if data["last_sent"]:
        last = datetime.fromisoformat(data["last_sent"])
        if now - last < timedelta(minutes=EMAIL_COOLDOWN_MIN):
            return False

    return True


def record_email_sent():
    now = datetime.now()

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "last_sent": now.isoformat(),
        "count_today": 0
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    data["count_today"] += 1
    data["last_sent"] = now.isoformat()

    with open(EMAIL_META_FILE, "w") as f:
        json.dump(data, f)


EMAIL_ENABLED = True  # 🔁 set False to fully disable emails

def send_email(subject, body):
    if not can_send_email():
        print("EMAIL SKIPPED: limit or cooldown active")
        return

    try:
        msg = EmailMessage()
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)

        record_email_sent()

    except Exception as e:
        print("EMAIL ERROR:", e)
        #st.warning("📧 Email alert blocked (Gmail limit). Alerts still logged.")


def email_already_sent(symbol, category):
    os.makedirs("CACHE", exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(EMAIL_DEDUP_FILE):
        return False

    df = pd.read_csv(EMAIL_DEDUP_FILE)

    return (
        (df["DATE"] == today) &
        (df["SYMBOL"] == symbol) &
        (df["CATEGORY"] == category)
    ).any()


def mark_email_sent(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    row = {
        "DATE": today,
        "SYMBOL": symbol,
        "CATEGORY": category
    }

    if os.path.exists(EMAIL_DEDUP_FILE):
        df = pd.read_csv(EMAIL_DEDUP_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(EMAIL_DEDUP_FILE, index=False)




if new_TOP_HIGH:
    send_email(
        "TOP LIVE_HIGH Breakout Alert",
        "New TOP LIVE_HIGH:\n" + "\n".join(new_TOP_HIGH)
    )



from datetime import datetime

def log_alert(symbol, category, details, ltp, source):
    if alert_already_logged(symbol, category):
        return  # 🚫 Prevent duplicate static alerts

    now = datetime.now()

    row = {
        "DATE": now.strftime("%Y-%m-%d"),
        "TIME": now.strftime("%H:%M:%S"),
        "SYMBOL": symbol,
        "CATEGORY": category,
        "DETAILS": details,
        "LTP": ltp,
        "SOURCE": source
    }

    if os.path.exists(ALERTS_LOG_FILE):
        df = pd.read_csv(ALERTS_LOG_FILE)
        df = pd.concat([pd.DataFrame([row]), df], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_LOG_FILE, index=False)

    # Mark this alert as logged
    mark_alert_logged(symbol, category)


def alert_already_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(ALERTS_DEDUP_FILE):
        return False

    df = pd.read_csv(ALERTS_DEDUP_FILE)

    return (
        (df["DATE"] == today) &
        (df["SYMBOL"] == symbol) &
        (df["CATEGORY"] == category)
    ).any()


def mark_alert_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    row = {
        "DATE": today,
        "SYMBOL": symbol,
        "CATEGORY": category
    }

    if os.path.exists(ALERTS_DEDUP_FILE):
        df = pd.read_csv(ALERTS_DEDUP_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_DEDUP_FILE, index=False)

####################################################################################

# =========================================================
# 🔔 MASTER ALERT ENGINE (Market Time Protected)
# =========================================================
def notify_all(category, title, symbols, ltp_map=None):

    # 🚫 Stop outside market hours
    if not is_market_hours():
        return

    # 🚫 Nothing new
    if not symbols:
        return

    email_symbols = []

    for sym in symbols:

        # ----- EMAIL DEDUP -----
        if not email_already_sent(sym, category):
            email_symbols.append(sym)
            mark_email_sent(sym, category)

        # ----- ALERT LOGGING -----
        ltp = ltp_map.get(sym) if ltp_map else ""
        log_alert(
            symbol=sym,
            category=category,
            details=title,
            ltp=ltp,
            source="EMAIL + BROWSER"
        )

    # ----- SEND ONE EMAIL (if allowed) -----
    if email_symbols and is_email_allowed():
        send_email(
            title,
            title + "\n\n" + "\n".join(email_symbols)
        )

    # ----- BROWSER TOAST -----
    st.toast(f"{title}: {', '.join(symbols)}", icon="🚨")




################################################################################
ltp_map = dict(zip(df.Symbol, df.LTP))

new_TOP_HIGH = detect_new_entries(
    "TOP_HIGH",
    TOP_HIGH_df.Symbol.tolist()
)

notify_all(
    "TOP_HIGH",
    "🟢TOP LIVE_HIGH Breakout",
    new_TOP_HIGH,
    ltp_map
)
new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

notify_all(
    "TOP_LOW",
    "🔴TOP LIVE_LOW Breakdown",
    new_TOP_LOW,
    ltp_map
)
new_ema = detect_new_entries(
    "EMA20_50",
    ema_signal_df.Symbol.tolist()
)

notify_all(
    "EMA20_50",
    "⚡ EMA20–EMA50 Signal",
    new_ema,
    ltp_map
)
new_daily_up = detect_new_entries(
    "DAILY_UP",
    daily_up.Symbol.tolist()
)

notify_all(
    "DAILY_UP",
    "📈🟢DAILY LIVE_HIGH Break",
    new_daily_up,
    ltp_map
)

new_daily_down = detect_new_entries(
    "DAILY_DOWN",
    daily_down.Symbol.tolist()
)

notify_all(
    "DAILY_DOWN",
    "📉🔴DAILY LIVE_LOW Break",
    new_daily_down,
    ltp_map
)
new_weekly_up = detect_new_entries(
    "WEEKLY_UP",
    weekly_up.Symbol.tolist()
)

notify_all(
    "WEEKLY_UP",
    "📊🟢WEEKLY LIVE_HIGH Break",
    new_weekly_up,
    ltp_map
)

new_weekly_down = detect_new_entries(
    "WEEKLY_DOWN",
    weekly_down.Symbol.tolist()
)

notify_all(
    "WEEKLY_DOWN",
    "📉🔴WEEKLY LIVE_LOW Break",
    new_weekly_down,
    ltp_map
)
new_monthly_up = detect_new_entries(
    "MONTHLY_UP",
    monthly_up.Symbol.tolist()
)

notify_all(
    "MONTHLY_UP",
    "📅🟢MONTHLY LIVE_HIGH Break",
    new_monthly_up,
    ltp_map
)

new_monthly_down = detect_new_entries(
    "MONTHLY_DOWN",
    monthly_down.Symbol.tolist()
)

notify_all(
    "MONTHLY_DOWN",
    "📉🔴MONTHLY LIVE_LOW Break",
    new_monthly_down,
    ltp_map
)

ohl_df = df.loc[
    (df["LIVE_OPEN"] == df["LIVE_HIGH"]) | (df["LIVE_OPEN"] == df["LIVE_LOW"]),
    [
        "Symbol",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "YEST_HIGH",
        "YEST_LOW"
    ]
].copy()


ohl_df["TYPE"] = ohl_df.apply(
    lambda r: "🔴 LIVE_OPEN = LIVE_HIGH" if r.LIVE_OPEN == r.LIVE_HIGH else "🟢 LIVE_OPEN = LIVE_LOW",
    axis=1
)

# ================= LIVE_OPEN = LIVE_HIGH / LIVE_LOW SPLIT =================

LIVE_OPEN_LIVE_LOW_df = pd.DataFrame()
LIVE_OPEN_LIVE_HIGH_df = pd.DataFrame()

if not ohl_df.empty and "TYPE" in ohl_df.columns:
    LIVE_OPEN_LIVE_LOW_df = ohl_df[ohl_df["TYPE"] == "🟢 LIVE_OPEN = LIVE_LOW"]
    LIVE_OPEN_LIVE_HIGH_df = ohl_df[ohl_df["TYPE"] == "🔴 LIVE_OPEN = LIVE_HIGH"]




NUM_COLS = [
    "EMA20","EMA50","TOP_HIGH","TOP_LOW",
    "YEST_HIGH","YEST_LOW",
    "HIGH_W","LOW_W","HIGH_M","LOW_M"
]

for c in NUM_COLS:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")


ema_signal_df = df.dropna(subset=["EMA20","EMA50"]).loc[
    (
        (df.LTP > df.EMA20) &
        (df.EMA20 > df.EMA50) &
        (df.LTP > df.TOP_HIGH)
    ) |
    (
        (df.LTP < df.EMA20) &
        (df.EMA20 < df.EMA50) &
        (df.LTP < df.TOP_LOW)
    ),
    [
        "Symbol",
        "LTP",
        "EMA20",
        "EMA50",
        "TOP_HIGH",
        "TOP_LOW",
        "CHANGE",
        "CHANGE_%"
    ]
].copy()

ema_signal_df["SIGNAL"] = ema_signal_df.apply(
    lambda r: "🟢 BUY" if r.LTP > r.EMA20 else "🔴 SELL",
    axis=1
)

# ================= TOP GAINERS / LOSERS =================

#gainers_df = (
 #   df[df["CHANGE_%"] >= 2.5]
  #  .sort_values("CHANGE_%", ascending=False)
   # .loc[:, [
    #    "Symbol",
     #   "LTP",
      #  "CHANGE",
       # "CHANGE_%",
        #"LIVE_HIGH",
        #"LIVE_LOW"
    #]]
#)

losers_df = (
    df[df["CHANGE_%"] <= -2.5]
    .sort_values("CHANGE_%")
    .loc[:, [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_HIGH",
        "LIVE_LOW"
    ]]
)

# =========================================================
# TOP GAINERS / LOSERS — COLUMN ORDER FIX
# =========================================================

TOP_GL_COLUMNS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "LIVE_VOLUME",
]

# --- TOP GAINERS ---
gainers_df = (
    df[df["CHANGE_%"] > 2]
    .sort_values("CHANGE_%", ascending=False)
    [TOP_GL_COLUMNS]
)

# --- TOP LOSERS ---
losers_df = (
    df[df["CHANGE_%"] < -2]
    .sort_values("CHANGE_%")
    [TOP_GL_COLUMNS]
)

TOP_GL_COLUMNS = [c for c in TOP_GL_COLUMNS if c in df.columns]

new_gainers = detect_new_entries(
    "TOP_GAINERS",
    gainers_df.Symbol.tolist()
)

notify_all(
    "TOP_GAINERS",
    "🔥🟢Top Gainers > 2.5%",
    new_gainers,
    ltp_map
)

new_losers = detect_new_entries(
    "TOP_LOSERS",
    losers_df.Symbol.tolist()
)

notify_all(
    "TOP_LOSERS",
    "🔥🔴Top LOSERS < -2.5%",
    new_losers,
    ltp_map
)

        ############## TOP GAINERS NEW ADDITION ##############################

# ================= O=H / O=L FILTERED SETUPS =================
TOL = 0.05   # 5 paise tolerance

ol_condition = (
    (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
    (df["LIVE_OPEN"] < df["YEST_HIGH"]) &
    (df["LIVE_OPEN"] > df["YEST_LOW"])
)

#oh_condition = (
 #   (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
  #  (df["LIVE_OPEN"] > df["YEST_LOW"])
#)

oh_condition = (
    (abs(df["LIVE_OPEN"] - df["LIVE_HIGH"]) <= TOL) &
    (df["LIVE_OPEN"] > df["YEST_LOW"]) &
    (df["LIVE_OPEN"] < df["YEST_CLOSE"]) &
    (df["LTP"] < df["LIVE_OPEN"])    # price below open
)


ol_oh_df = df.loc[
    ol_condition | oh_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE",
    ]
].copy()

ol_oh_df["SETUP"] = np.where(
    ol_condition.loc[ol_oh_df.index],
    "🟢 O = L",
    "🔴 O = H"
)

ol_oh_df["SIDE"] = np.where(
    ol_condition.loc[ol_oh_df.index],
    "BULLISH",
    "BEARISH"
)

ol_oh_df = ol_oh_df.sort_values(
    by=["SIDE", "CHANGE_%"],
    ascending=[True, False]
)





# ================= DAILY + EMA CONFIRMATION =================
daily_ema_buy = df.loc[
    (df.LTP > df.YEST_HIGH) &
    (df.LTP > df.EMA20) &
    (df.EMA20 > df.EMA50),
    DAILY_COLUMNS
]

daily_ema_sell = df.loc[
    (df.LTP < df.YEST_LOW) &
    (df.LTP < df.EMA20) &
    (df.EMA20 < df.EMA50),
    DAILY_COLUMNS
]

# ================= WEEKLY + EMA CONFIRMATION =================

weekly_ema_buy = df.loc[
    (df.LTP > df.HIGH_W) &
    (df.LTP > df.EMA20) &
    (df.EMA20 > df.EMA50),
    WEEKLY_COLUMNS
]

weekly_ema_sell = df.loc[
    (df.LTP < df.LOW_W) &
    (df.LTP < df.EMA20) &
    (df.EMA20 < df.EMA50),
    WEEKLY_COLUMNS
]



# =========================================================
# SUPERTREND (DAILY) + VWAP  — CLEAN VERSION
# (NO ZERO VALUES | BUY / SELL SEPARATE)
# =========================================================

def compute_supertrend(df, period=10, multiplier=3):
    df = df.copy()

    df["H-L"] = df["high"] - df["low"]
    df["H-PC"] = (df["high"] - df["close"].shift()).abs()
    df["L-PC"] = (df["low"] - df["close"].shift()).abs()

    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)

    # 🔥 Wilder ATR (Kite match)
    df["ATR"] = df["TR"].rolling(period).mean()
    df["ATR"] = df["ATR"].combine_first(
        df["ATR"].shift().ewm(alpha=1/period, adjust=False).mean()
    )

    mid = (df["high"] + df["low"]) / 2
    df["UpperBand"] = mid + multiplier * df["ATR"]
    df["LowerBand"] = mid - multiplier * df["ATR"]

    df["SuperTrend"] = np.nan
    df["ST_DIR"] = None

    # Seed
    df.loc[period, "SuperTrend"] = df.loc[period, "UpperBand"]
    df.loc[period, "ST_DIR"] = "SELL"

    for i in range(period + 1, len(df)):
        prev = i - 1

        if df.loc[prev, "close"] > df.loc[prev, "UpperBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "LowerBand"]
            df.loc[i, "ST_DIR"] = "BUY"

        elif df.loc[prev, "close"] < df.loc[prev, "LowerBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "UpperBand"]
            df.loc[i, "ST_DIR"] = "SELL"

        else:
            df.loc[i, "SuperTrend"] = df.loc[prev, "SuperTrend"]
            df.loc[i, "ST_DIR"] = df.loc[prev, "ST_DIR"]

        # 🔒 Band carry-forward (Kite logic)
        if df.loc[i, "ST_DIR"] == "BUY":
            df.loc[i, "LowerBand"] = max(
                df.loc[i, "LowerBand"], df.loc[prev, "LowerBand"]
            )
        else:
            df.loc[i, "UpperBand"] = min(
                df.loc[i, "UpperBand"], df.loc[prev, "UpperBand"]
            )

    return df



# -----------------------------
# BUILD SUPERTREND PER SYMBOL
# -----------------------------
supertrend_rows = []

ohlc_full = pd.read_csv(OHLC_FILE)
ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

for sym, g in ohlc_full.groupby("Symbol"):
    g = g.sort_values("date").reset_index(drop=True)

    if len(g) < 20:
        continue

    st_df = compute_supertrend(g)
    last = st_df.iloc[-1]

    # 🔴 SKIP IF NOT INITIALIZED
    if pd.isna(last["SuperTrend"]):
        continue

    supertrend_rows.append({
        "Symbol": sym,
        "SUPERTREND": round(last["SuperTrend"], 2),
        "ST_SIGNAL": last["ST_DIR"]
    })

supertrend_df = pd.DataFrame(supertrend_rows)

df = df.merge(supertrend_df, on="Symbol", how="left")


# -----------------------------
# VWAP (INTRADAY APPROX)
# -----------------------------
def compute_vwap(row):
    price = (row["LIVE_HIGH"] + row["LIVE_LOW"] + row["LTP"]) / 3
    return round(price, 2)

df["VWAP"] = df.apply(compute_vwap, axis=1)
df["VWAP_DIFF"] = (df["LTP"] - df["VWAP"]).round(2)


# -----------------------------
# CLEAN + ACTIONABLE VIEW
# -----------------------------
supertrend_view = df[
    (df["SUPERTREND"].notna())
].copy()

# Remove invalid / zero SuperTrend values
supertrend_view = supertrend_view[
    supertrend_view["SUPERTREND"] > 0
].copy()


supertrend_view["ST_BUY"] = supertrend_view.apply(
    lambda x: "BUY" if x["ST_SIGNAL"] == "BUY" else "",
    axis=1
)

supertrend_view["ST_SELL"] = supertrend_view.apply(
    lambda x: "SELL" if x["ST_SIGNAL"] == "SELL" else "",
    axis=1
)

supertrend_view = supertrend_view[
    [
        "Symbol",
        "LTP",
        "SUPERTREND",
        "ST_BUY",
        "ST_SELL",
        "VWAP",
        "VWAP_DIFF",
        "EMA20",
        "EMA50",
        "TOP_HIGH",
        "TOP_LOW",
        "CHANGE_%"
    ]
].copy()

DISPLAY_ROUND_COLS = [
    "LTP",
    "SUPERTREND",
    "VWAP",
    "VWAP_DIFF",
    "EMA20",
    "EMA50",
    "TOP_HIGH",
    "TOP_LOW",
    "CHANGE_%"
]

for col in DISPLAY_ROUND_COLS:
    if col in supertrend_view.columns:
        supertrend_view[col] = supertrend_view[col].round(2)



# =========================================================
# SUPERTREND — PREPARE DATAFRAME + STYLER (MUST BE BEFORE TABS)
# =========================================================

def highlight_supertrend(row):
    styles = []
    for col in row.index:
        if col == "ST_BUY" and row[col] == "BUY":
            styles.append("background-color:#d4f8d4;color:#006400;font-weight:bold;")
        elif col == "ST_SELL" and row[col] == "SELL":
            styles.append("background-color:#ffd6d6;color:#8b0000;font-weight:bold;")
        else:
            styles.append("")
    return styles



# DataFrame used for logic + empty checks
supertrend_df = supertrend_view.copy()

# Create Styler ONLY for UI
supertrend_styled = supertrend_df.style.format({
    "LTP": "{:.2f}",
    "SUPERTREND": "{:.2f}",
    "VWAP": "{:.2f}",
    "VWAP_DIFF": "{:.2f}",
    "EMA20": "{:.2f}",
    "EMA50": "{:.2f}",
    "TOP_HIGH": "{:.2f}",
    "TOP_LOW": "{:.2f}",
    "CHANGE_%": "{:.2f}"
}).apply(highlight_supertrend, axis=1)


#####################################           SUPERTREND – NEAR LTP ZONE

supertrend_view = df[
    (df["SUPERTREND"].notna())
].copy()


# =========================================================
# 🎯 SUPERTREND – NEAR LTP OPPORTUNITY TABLE
# =========================================================

# Distance in % between LTP and Supertrend
supertrend_view["ST_DIST_%"] = (
    (supertrend_view["LTP"] - supertrend_view["SUPERTREND"])
    / supertrend_view["SUPERTREND"] * 100
).round(2)

# Absolute distance for sorting
supertrend_view["ST_DIST_ABS"] = supertrend_view["ST_DIST_%"].abs()

# 🔎 Filter: only NEAR opportunities (adjust threshold if needed)
ST_NEAR_THRESHOLD = 2.0   # 2% near zone

st_near_df = supertrend_view[
    supertrend_view["ST_DIST_ABS"] <= ST_NEAR_THRESHOLD
].copy()

# Direction clarity
st_near_df["SIDE"] = np.where(
    st_near_df["ST_SIGNAL"] == "BUY", "🟢 LONG",
    "🔴 SHORT"
)

# Sort by nearest first
st_near_df = st_near_df.sort_values("ST_DIST_ABS")

# Final columns (clean & actionable)
ST_NEAR_COLUMNS = [
    "Symbol",
    #"SIDE",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "SUPERTREND",
    "ST_DIST_%",
    "VWAP",
    "EMA20",
    "EMA50",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "NEAR"
    
]

ST_NEAR_COLUMNS = [c for c in ST_NEAR_COLUMNS if c in st_near_df.columns]

st_near_view = st_near_df[ST_NEAR_COLUMNS]







# ================= EMA20–EMA50 SPLIT (BUY / SELL) =================

#ema_buy_df = ema_signal_df[ema_signal_df["SIGNAL"] == "BUY"].copy()
#ema_sell_df = ema_signal_df[ema_signal_df["SIGNAL"] == "SELL"].copy()

# =========================================================
# TOP GAINERS / LOSERS (LEAST EXTREME ON TOP)
# =========================================================

gainers_df = (
    df[df["CHANGE_%"] > 2.5]
    .sort_values(by="CHANGE_%", ascending=True)   # least positive first
    .copy()
)

losers_df = (
    df[df["CHANGE_%"] < -2.5]
    .sort_values(by="CHANGE_%", ascending=True)   # least negative first
    .copy()
)

# =========================================================
# EMA20–EMA50 SORTING (BASED ON GAIN)
# =========================================================

ema_buy_df = (
    ema_buy_df
    .sort_values(by="GAIN", ascending=True)   # least positive gain first
)

ema_sell_df = (
    ema_sell_df
    .sort_values(by="GAIN", ascending=False)   # least negative gain first
)



####################     OPTIONS SCORING ENGINE    ####################################







def option_score(row):
    score = 0
    reasons = []

    # -------- SPOT TREND --------
    if row["EMA20"] > row["EMA50"] and row["SUPERTREND"] == "BUY":
        score += 2
        reasons.append("Trend bullish")
    elif row["EMA20"] < row["EMA50"] and row["SUPERTREND"] == "SELL":
        score += 2
        reasons.append("Trend bearish")
    else:
        return 0, "Spot not aligned"

    # -------- BREAKOUT / NEAR --------
    if row["LTP"] > row["TOP_HIGH"] or "↑" in str(row.get("NEAR", "")):
        score += 2
        reasons.append("Upside breakout / near")
    elif row["LTP"] < row["TOP_LOW"] or "↓" in str(row.get("NEAR", "")):
        score += 2
        reasons.append("Downside breakout / near")

    # -------- VWAP --------
    if row["LTP"] > row["VWAP"]:
        score += 1
        reasons.append("Above VWAP")
    else:
        score -= 1

    # -------- ATR / MOMENTUM --------
    if row.get("ATR_PCT", 0) > 1.2:
        score += 1
        reasons.append("Good momentum")

    return score, ", ".join(reasons)


df[["OPTION_SCORE", "OPTION_REASON"]] = df.apply(
    lambda r: pd.Series(option_score(r)), axis=1
)

def recommend_strike(row):
    if row["OPTION_SCORE"] < 3:
        return "AVOID"

    # Expiry safety
    if row["OPTION_SCORE"] >= 3:
        return "ATM"

    if row["OPTION_SCORE"] == 3:
        return "ITM"

    return "OTM"

df["STRIKE_PREF"] = df.apply(recommend_strike, axis=1)

def option_verdict(row):
    if row["OPTION_SCORE"] >= 3:
        if row["EMA20"] > row["EMA50"]:
            return "STRONG CE BUY"
        else:
            return "STRONG PE BUY"
    return "AVOID"

df["OPTION_SIGNAL"] = df.apply(option_verdict, axis=1)

#ALERT FILTER (NO MORE EMAIL FLOOD)
STRONG_BUY_DF = df[df["OPTION_SIGNAL"].str.contains("STRONG")]
new_strong = detect_new_entries(
    "OPTION_STRONG",
    STRONG_BUY_DF["Symbol"].tolist()
)
if new_strong:
    notify_all(
        "OPTION_STRONG",
        "🔥 STRONG OPTIONS BUY",
        [
            f"{s} | {df.loc[df.Symbol==s,'OPTION_SIGNAL'].values[0]} | "
            f"Strike: {df.loc[df.Symbol==s,'STRIKE_PREF'].values[0]}"
            for s in new_strong
        ]
    )


def backtest_options(df, ohlc_df):
    results = []

    for sym in df["Symbol"].unique():
        spot = df[df.Symbol == sym].iloc[0]
        hist = ohlc_df[ohlc_df.Symbol == sym].sort_values("date").tail(30)

        for i in range(len(hist)-1):
            r = hist.iloc[i]
            next_day = hist.iloc[i+1]

            if spot["OPTION_SIGNAL"] == "STRONG CE BUY":
                pnl = next_day["close"] - r["close"]
            elif spot["OPTION_SIGNAL"] == "STRONG PE BUY":
                pnl = r["close"] - next_day["close"]
            else:
                continue

            results.append({
                "Symbol": sym,
                "Signal": spot["OPTION_SIGNAL"],
                "Day": r["date"],
                "PnL": round(pnl, 2)
            })

    return pd.DataFrame(results)

backtest_df = backtest_options(df, ohlc_full)


# =========================================================
# LIVE MAP (Symbol → Live Values) for quick access
# =========================================================
live_map = {}

required_cols = [
    "Symbol", "LTP", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "CHANGE", "CHANGE_%"
]

available_cols = [c for c in required_cols if c in df.columns]

for _, r in df[available_cols].iterrows():
    live_map[r["Symbol"]] = r.to_dict()


#####   SETUP 1: 4 BAR REVERSAL + Breakouts     ####################################################

# =========================================================
# 4-BAR SETUP (EXACT SCREENER MATCH)
# =========================================================

# =========================================================
# 4 BAR REVERSAL (STRICT)
# =========================================================


four_bar_rows = []

ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

for sym, g in ohlc_full.groupby("Symbol"):
    if sym not in live_map:
        continue
    g = g.sort_values("date").reset_index(drop=True)

    if len(g) < 5:
        continue

    d0  = g.iloc[-1]   # today
    d1  = g.iloc[-2]
    d2  = g.iloc[-3]
    d3  = g.iloc[-4]
    d4  = g.iloc[-5]

    # --- Last 4 RED candles (strict)
    red_4 = (
        (d1.close <= d1.open) and
        (d2.close <= d2.open) and
        (d3.close <= d3.open) and
        (d4.close <  d4.open)
    )

    if not red_4:
        continue

    # --- Today reversal conditions
    today_reversal = (
        (d0.open  > d1.low) and
        (d0.open  > d1.close) and
        (d0.high  > d1.high) and
        (d0.close > d0.open)
    )

    if not today_reversal:
        continue

    
    live = live_map[sym]

    four_bar_rows.append({
        "Symbol": sym,

        # 🔴 LIVE DATA (single source of truth)
        "LTP": round(live["LTP"], 2),
        "CHANGE": round(live["CHANGE"], 2),
        "CHANGE_%": round(live["CHANGE_%"], 2),

        # 🟢 Candle structure (from OHLC)
        "LIVE_OPEN": round(d0.open, 2),
        "LIVE_HIGH": round(d0.high, 2),
        "LIVE_LOW": round(d0.low, 2),

        # 🟡 Yesterday reference
        "YEST_HIGH": round(d1.high, 2),
        "YEST_LOW": round(d1.low, 2),
        "YEST_CLOSE": round(d1.close, 2),
    })

    


#four_bar_df = pd.DataFrame(four_bar_rows)
four_bar_df = pd.DataFrame(four_bar_rows)

# 🔒 SAFETY: ensure required columns exist
REQUIRED_4BAR_COLS = [
    "Symbol", "LTP", "CHANGE", "CHANGE_%",
    "LIVE_OPEN", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "YEST_CLOSE"
]

for col in REQUIRED_4BAR_COLS:
    if col not in four_bar_df.columns:
        four_bar_df[col] = np.nan






# =========================================================
# 🚨 FAKE BREAKOUTS : BULL TRAP & BEAR TRAP
# =========================================================

fake_bull_rows = []
fake_bear_rows = []

for _, r in df.iterrows():

    y_close = r["YEST_CLOSE"]
    if pd.isna(y_close) or y_close == 0:
        continue

    # -------------------------
    # % calculations
    # -------------------------
    high_pct = (r["LIVE_HIGH"] - y_close) / y_close * 100
    low_pct  = (r["LIVE_LOW"]  - y_close) / y_close * 100
    ltp_pct  = (r["LTP"] - y_close) / y_close * 100

    # =========================
    # 🟡 FAKE BULL TRAP
    # =========================
    if (
        high_pct >= 2.5 and          # broke +2.5%
        ltp_pct < 2.5 and            # failed to hold
        r["LIVE_OPEN"] < r["YEST_HIGH"]
    ):
        fake_bull_rows.append({
            "Symbol": r["Symbol"],
            "YEST_CLOSE": round(y_close, 2),
            "LIVE_HIGH": round(r["LIVE_HIGH"], 2),
            "LIVE_OPEN": round(r["LIVE_OPEN"], 2),
            "LTP": round(r["LTP"], 2),
            "CHANGE_%": round(ltp_pct, 2),
            "FAIL_%": round(high_pct - ltp_pct, 2)
        })

    # =========================
    # 🔵 FAKE BEAR TRAP
    # =========================
    if (
        low_pct <= -2.5 and          # broke −2.5%
        ltp_pct > -2.5 and           # recovered
        r["LIVE_OPEN"] > r["YEST_LOW"]
    ):
        fake_bear_rows.append({
            "Symbol": r["Symbol"],
            "YEST_CLOSE": round(y_close, 2),
            "LIVE_LOW": round(r["LIVE_LOW"], 2),
            "LIVE_OPEN": round(r["LIVE_OPEN"], 2),
            "LTP": round(r["LTP"], 2),
            "CHANGE_%": round(ltp_pct, 2),
            "FAIL_%": round(abs(low_pct - ltp_pct), 2)
        })

fake_bull_df = pd.DataFrame(fake_bull_rows)
fake_bear_df = pd.DataFrame(fake_bear_rows)

# =========================================================
# 🎨 Styling
# =========================================================
def style_ltp_relative(row):
    """
    Row-wise styling:
    - Green if LTP >= YEST_CLOSE
    - Orange if LTP < YEST_CLOSE
    """
    if row["LTP"] >= row["YEST_CLOSE"]:
        return ["background-color:#e8f5e9"] * len(row)
    else:
        return ["background-color:#fff3e0"] * len(row)

def style_bear_trap(_):
    return ["background-color:#fff3e0"] * len(fake_bear_df.columns)

def style_ltp_only(row):
    styles = [""] * len(row)

    ltp_idx = row.index.get_loc("LTP")

    if row["LTP"] >= row["YEST_CLOSE"]:
        styles[ltp_idx] = "background-color:#e8f5e9"  # light green
    else:
        styles[ltp_idx] = "background-color:#fff3e0"  # light orange

    return styles

def style_ltp_bear_only(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")
    styles[ltp_idx] = "background-color:#fff3e0"
    return styles
############ for 15 mins table style
def style_ltp_15min(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")

    if row["BREAK_TYPE"] == "UP":
        styles[ltp_idx] = "background-color:#e8f5e9; color:#1b5e20"  # green
    elif row["BREAK_TYPE"] == "DOWN":
        styles[ltp_idx] = "background-color:#ffebee; color:#b71c1c"  # red

    return styles







############    OPTION 2 (Fallback): Build 15-min from 5-min candles    ================
def fetch_5min_candles(symbols):
    rows = []

    today = datetime.now(IST).date()
    start = datetime.combine(today, time(9, 15))
    end   = datetime.combine(today, time(15, 30))

    for sym in symbols:
        tk = get_token(sym)
        if not tk:
            continue

        try:
            candles = kite.historical_data(
                tk,
                start,
                end,
                interval="5minute"
            )
        except Exception:
            continue

        for c in candles:
            rows.append({
                "Symbol": sym,
                "datetime": pd.to_datetime(c["date"]),
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"]
            })

    return pd.DataFrame(rows)

five_df = fetch_5min_candles(SYMBOLS)

# ================= SAFETY: 5-MIN DATA AVAILABILITY =================
if five_df.empty or "datetime" not in five_df.columns:
    intraday_15m_df = pd.DataFrame(columns=[
        "Symbol", "datetime", "open", "high", "low", "close"
    ])
else:
    intraday_15m_df = (
        five_df
        .set_index("datetime")
        .groupby("Symbol")
        .resample("15T")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last"
        })
        .dropna()
        .reset_index()
    )




############    SETUP 3: 15-MIN INSIDE RANGE BREAK    #######################

inside_break_rows = []

for sym in SYMBOLS:
    df15 = intraday_15m_df[intraday_15m_df["Symbol"] == sym].sort_values("datetime")

    if len(df15) < 4 or sym not in live_map:
        continue

    first = df15.iloc[0]
    later = df15.iloc[1:4]

    # 1️⃣ Inside range condition
    if not (
        later["high"].max() <= first["high"] and
        later["low"].min() >= first["low"]
    ):
        continue

    live = live_map[sym]
    ltp = live["LTP"]

    break_type = None
    chg_15m_pct = None

    # 2️⃣ Break detection + 15-min % calc
    if ltp > first["high"]:
        break_type = "UP"
        chg_15m_pct = round(
            ((ltp - first["high"]) / first["high"]) * 100, 2
        )

    elif ltp < first["low"]:
        break_type = "DOWN"
        chg_15m_pct = round(
            ((ltp - first["low"]) / first["low"]) * 100, 2
        )

    else:
        continue

    inside_break_rows.append({
        "Symbol": sym,
        "LTP": ltp,
        "CHG_15M_%": chg_15m_pct,
        "CHANGE": live["CHANGE"],
        "CHANGE_%": live["CHANGE_%"],
        "DAY_HIGH": live["LIVE_HIGH"],
        "YEST_HIGH": live["YEST_HIGH"],
        "DAY_LOW": live["LIVE_LOW"],
        "YEST_LOW": live["YEST_LOW"],
        "BREAK_TYPE": break_type
    })

inside_15m_df = pd.DataFrame(inside_break_rows)


# =========================================================
# YH1.5 STRONG BREAKOUT (SCREENER LOGIC) — FINAL & CORRECT
# Rules:
# 1. OPEN < YEST_HIGH  (no gap-up)
# 2. LTP >= YEST_HIGH * 1.015
# =========================================================

yh15_rows = []

for _, r in df.iterrows():

    yh = r["YEST_HIGH"]
    ltp = r["LTP"]
    live_open = r["LIVE_OPEN"]

    if pd.isna(yh) or yh <= 0:
        continue
    if pd.isna(ltp) or pd.isna(live_open):
        continue

    # 🚫 GAP-UP FILTER (ABSOLUTE)
    if live_open >= yh:
        continue

    level_15 = yh * 1.015

    # ✅ TRUE BREAKOUT
    if ltp >= level_15:

        break_pct = ((ltp - yh) / yh) * 100
        after_break_pct = ((ltp - level_15) / level_15) * 100

        yh15_rows.append({
            "Symbol": r["Symbol"],
            "LIVE_OPEN": round(live_open, 2),
            "LTP": round(ltp, 2),
            "YEST_HIGH": round(yh, 2),
            "BREAK_%": round(break_pct, 2),
            "AFTER_BREAK_%": round(after_break_pct, 2),
            "CHANGE": round(r["CHANGE"], 2),
            "CHANGE_%": round(r["CHANGE_%"], 2),
        })

yh15_df = pd.DataFrame(yh15_rows)

if not yh15_df.empty:
    yh15_df = yh15_df.sort_values(
        ["AFTER_BREAK_%", "BREAK_%"],
        ascending=False
    )




# =========================================================
# FAKE / FAILED YH1.5 BREAKOUTS
# =========================================================

fake_yh15_rows = []

for sym, live in live_map.items():

    yh = live.get("YEST_HIGH")
    ltp = live.get("LTP")
    high = live.get("LIVE_HIGH")

    if not yh or yh == 0:
        continue

    level_15 = yh * 1.015

    # 🔴 Attempted but failed breakout
    if high >= level_15 and ltp < level_15:

        break_pct = ((high - yh) / yh) * 100
        retrace_pct = ((ltp - high) / high) * 100 if high else 0

        fake_yh15_rows.append({
            "Symbol": sym,
            "LTP": round(ltp, 2),
            "LIVE_HIGH": round(high, 2),
            "YEST_HIGH": round(yh, 2),
            "BREAK_%": round(break_pct, 2),      # how much it broke
            "RETRACE_%": round(retrace_pct, 2),  # how much it failed
            "CHANGE": round(live.get("CHANGE", 0), 2),
            "CHANGE_%": round(live.get("CHANGE_%", 0), 2),
        })

fake_yh15_df = pd.DataFrame(fake_yh15_rows)

if not fake_yh15_df.empty:
    fake_yh15_df = fake_yh15_df.sort_values("RETRACE_%")




alerts = []
alert_keys = set()   # prevents duplicate alerts per refresh
alert_time = datetime.now(IST)

##########      ALERT-1 IMPLEMENTATION ######## YH 1.5% Strong Breakout
df["FROM_YH_%"] = np.where(
    df["LTP"] >= df["YEST_HIGH"],
    ((df["LTP"] - df["YEST_HIGH"]) / df["YEST_HIGH"] * 100),
    0
).round(2)

df["FROM_YL_%"] = np.where(
    df["LTP"] <= df["YEST_LOW"],
    ((df["YEST_LOW"] - df["LTP"]) / df["YEST_LOW"] * 100),
    0
).round(2)


yh15_df = df.loc[
    (
        # 🔹 Breakout strength (0.5% above YH)
        (df["LTP"] >= df["YEST_HIGH"] * 1.005) & (df["LTP"] <= df["YEST_HIGH"] * 1.02) &
        (df["CHANGE_%"] >= 0.5) &

        # 🔹 Previous day bullish
        (df["YEST_CLOSE"] > df["YEST_OPEN"]) &

        # 🔹 Gap continuation structure
        (df["LIVE_OPEN"] >= df["YEST_CLOSE"]) &
        (df["LIVE_OPEN"] <= df["YEST_HIGH"]) & (df["VOL_%"] >= -50)
    ),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_VOLUME",
        "YEST_VOL",
        "VOL_%",
        "FROM_YH_%",   # 👈 added
        "FROM_YL_%",   # 👈 added
        "YEST_HIGH",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LIVE_OPEN",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

for _, r in yh15_df.iterrows():
    key = f"YH15_{r['Symbol']}"

    if key not in st.session_state.alert_keys:
        alert_time = datetime.now(IST).replace(tzinfo=None)   # 🔒 ONLY HERE

        st.session_state.alerts.append({
            "TIME": alert_time.strftime("%H:%M:%S"),
            "TS": alert_time,             # sortable
            "TYPE": "🚀 YH 1.5%",
            "Symbol": r["Symbol"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"],
            "VOL_%": r["VOL_%"],
            "FROM_YH_%": r["FROM_YH_%"],
            "FROM_YL_%": r["FROM_YL_%"],
            "DAY_OPEN": r["LIVE_OPEN"],
            "DAY_HIGH": r["LIVE_HIGH"],
            "DAY_LOW": r["LIVE_LOW"],
            "YEST_HIGH": r["YEST_HIGH"],
            "YEST_LOW": r["YEST_LOW"],
            "YEST_CLOSE": r["YEST_CLOSE"],
        })

        st.session_state.alert_keys.add(key)

yl15_df = df.loc[
    (
        # 🔻 Breakdown strength (0.5% below YL)
        (df["LTP"] <= df["YEST_LOW"] * 0.995) & (df["LTP"] >= df["YEST_LOW"] * 0.98) &
        (df["CHANGE_%"] <= -0.5) &

        # 🔻 Previous day bearish
        (df["YEST_CLOSE"] < df["YEST_OPEN"]) &

        # 🔻 Gap continuation structure
        (df["LIVE_OPEN"] <= df["YEST_CLOSE"]) &
        (df["LIVE_OPEN"] >= df["YEST_LOW"]) & (df["VOL_%"] >= -50)
    ),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_VOLUME",
        "YEST_VOL",
        "VOL_%",
        "FROM_YH_%",   # 👈 added
        "FROM_YL_%",   # 👈 added
        "YEST_HIGH",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LIVE_OPEN",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

for _, r in yl15_df.iterrows():
    key = f"YL15_{r['Symbol']}"

    if key not in st.session_state.alert_keys:
        alert_time = datetime.now(IST).replace(tzinfo=None)   # 🔒 ONLY HERE

        st.session_state.alerts.append({
            "TIME": alert_time.strftime("%H:%M:%S"),
            "TS": alert_time,             # sortable
            "TYPE": " 🔴 YL 1.5%",
            "Symbol": r["Symbol"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"],
            "VOL_%": r["VOL_%"],
            "FROM_YH_%": r["FROM_YH_%"],
            "FROM_YL_%": r["FROM_YL_%"],
            "DAY_OPEN": r["LIVE_OPEN"],
            "DAY_HIGH": r["LIVE_HIGH"],
            "DAY_LOW": r["LIVE_LOW"],
            "YEST_HIGH": r["YEST_HIGH"],
            "YEST_LOW": r["YEST_LOW"],
            "YEST_CLOSE": r["YEST_CLOSE"],
        })

        st.session_state.alert_keys.add(key)

###############     ALERT-2 IMPLEMENTATION      ## 4-BAR Reversal + Breakout

for _, r in four_bar_df.iterrows():

    if r["Symbol"] not in df["Symbol"].values:
        continue

    base = df.loc[df.Symbol == r.Symbol].iloc[0]

    # Bullish
    if (
        r["CHANGE_%"] > 0 and
        base["LTP"] > base["YEST_HIGH"]
    ):
        key = f"4BAR_BUY_{r['Symbol']}"
        if key not in alert_keys:
            alerts.append({
                "TIME": alert_time.strftime("%H:%M:%S"),   # 🔥 readable
                "TYPE": "🔁 4-BAR REVERSAL BUY",
                "Symbol": r["Symbol"],
                "LTP": base["LTP"],
                "CHANGE_%": base["CHANGE_%"]
            })
            alert_keys.add(key)

    # Bearish
    if (
        r["CHANGE_%"] < 0 and
        base["LTP"] < base["YEST_LOW"]
    ):
        key = f"4BAR_SELL_{r['Symbol']}"
        if key not in alert_keys:
            alerts.append({
                "TYPE": "🔁 4-BAR REVERSAL SELL",
                "Symbol": r["Symbol"],
                "LTP": base["LTP"],
                "CHANGE_%": base["CHANGE_%"]
            })
            alert_keys.add(key)





# ================= YH 1.5 STRONG BREAKOUT ALERT =================

new_YH15 = detect_new_entries(
    "YH15",
    yh15_df.Symbol.tolist()
)

notify_browser("🚀 YH 1.5 STRONG BREAKOUT", new_YH15)

# ================= 4-BAR REVERSAL BUY ALERT =================

# =========================================================
# FIX-2 : SAFE 4-BAR BUY FILTER (BUY ONLY)
# =========================================================

if not four_bar_df.empty and "CHANGE_%" in four_bar_df.columns:

    four_bar_buy = four_bar_df.loc[
        four_bar_df["CHANGE_%"] > 0, "Symbol"
    ].tolist()

else:
    four_bar_buy = []


new_4BAR_BUY = detect_new_entries(
    "FOUR_BAR_BUY",
    four_bar_buy
)

notify_browser("🟢 4-BAR REVERSAL BUY", new_4BAR_BUY)


def load_index_symbols(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line.strip()]

INDEX_FILES = {
    "NIFTY 50": "NIFTY 50.txt",
    "BANK NIFTY": "BANK NIFTY.txt",
    "FINNIFTY": "FINNIFTY.txt",
    "NIFTY IT": "NIFTY IT.txt",
    "NIFTY FMCG": "NIFTY FMCG.txt",
    "NIFTY PHARMA": "NIFTY PHARMA.txt",
    "NIFTY METAL": "NIFTY METAL.txt",
    "NIFTY AUTO": "NIFTY AUTO.txt",
    "NIFTY ENERGY": "NIFTY ENERGY.txt",
    "NIFTY PSU BANK": "NIFTY PSU BANK.txt",
}

index_symbols = {
    name: load_index_symbols(file)
    for name, file in INDEX_FILES.items()
}

# =========================================================
# INDICES – LIVE OHLC (SINGLE SOURCE OF TRUTH)
# =========================================================

INDEX_SYMBOLS = {
    "NIFTY 50": "NIFTY",
    "BANK NIFTY": "BANKNIFTY",
    "FINNIFTY": "FINNIFTY",
    "NIFTY IT": "NIFTYIT",
    "NIFTY FMCG": "NIFTYFMCG",
    "NIFTY PHARMA": "NIFTYPHARMA",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY AUTO": "NIFTYAUTO",
    "NIFTY ENERGY": "NIFTYENERGY",
    "NIFTY PSU BANK": "NIFTYPSUBANK",
}

index_rows = []

for index_name, sym in INDEX_SYMBOLS.items():

    try:
        q = kite.quote(f"NSE:{sym}").get(f"NSE:{sym}")
        if not q:
            continue

        o = q["ohlc"]["open"]
        h = q["ohlc"]["high"]
        l = q["ohlc"]["low"]
        pc = q["ohlc"]["close"]   # prev close (index)
        ltp = q["last_price"]

        chg = ltp - pc if pc else 0
        chg_pct = (chg / pc * 100) if pc else 0

        tk = get_token(sym)
        yh, yl, yc = fetch_yesterday_ohlc(tk)

        index_rows.append({
            "Index": index_name,
            "OPEN": round(o, 2),
            "HIGH": round(h, 2),
            "LOW": round(l, 2),
            "LTP": round(ltp, 2),
            "CHANGE": round(chg, 2),
            "CHANGE_%": round(chg_pct, 2),
            "YEST_HIGH": yh,
            "YEST_LOW": yl,
            "YEST_CLOSE": yc,
        })

    except Exception as e:
        print("INDEX ERROR:", index_name, e)

indices_df = pd.DataFrame(index_rows)

# ================= INDICES TAB (FIXED) =================

INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "FINNIFTY",
    "NIFTYIT",
    "NIFTYFMCG",
    "NIFTYPHARMA",
    "NIFTYMETAL",
    "NIFTYAUTO",
    "NIFTYENERGY",
    "NIFTYPSUBANK",
]

indices_df = (
    df[df["Symbol"].isin(INDEX_ONLY_SYMBOLS)]
    .loc[:, [
        "Symbol",
        "LTP",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "CHANGE",
        "CHANGE_%",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE",
    ]]
    .sort_values("Symbol")
    .reset_index(drop=True)
)


# 🔒 SAFETY NET – ENSURE ALL REQUIRED COLUMNS EXIST
REQUIRED_INDEX_COLS = [
    "Index","OPEN","HIGH","LOW","LTP",
    "CHANGE","CHANGE_%",
    "YEST_HIGH","YEST_LOW","YEST_CLOSE"
]

for c in REQUIRED_INDEX_COLS:
    if c not in indices_df.columns:
        indices_df[c] = None


indices_df = indices_df.sort_values("CHANGE_%", ascending=False)

# =========================================================
# 🚨 ALERT: 3 CONSECUTIVE 15-MIN GREEN CANDLES
# (Valid until a RED candle appears)
# =========================================================
alert_time = datetime.now(IST)
three_green_rows = []

for sym in SYMBOLS:

    # --- intraday candles (signal source) ---
    df15 = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(df15) < 3:
        continue

    # --- master df row (OHLC source) ---
    row = df[df["Symbol"] == sym]
    if row.empty:
        continue

    row = row.iloc[0]

    # Take last 3 COMPLETED 15-min candles
    last3 = df15.iloc[-3:]

    # Condition: all 3 green
    if not all(last3["close"] > last3["open"]):
        continue
    # 🔒 NEW CONDITION: price strength confirmation
    if not (
        row["LTP"] > row["LIVE_OPEN"] and
        row["LTP"] > row["YEST_CLOSE"]
    ):
        continue

    candle_time = last3.iloc[-1]["datetime"]

    three_green_rows.append({
        "TIME": alert_time.strftime("%H:%M:%S"),   # 🔥 readable
        #"TS": alert_time,                           # 🔒 sortable
        "TYPE": "🟢 3×15m GREEN",
        "Symbol": sym,

        # 🔴 LIVE
        "LTP": round(row["LTP"], 2),
        "CHANGE_%": round(row["CHANGE_%"], 2),

        # 🟡 DAY OHLC
        "DAY_OPEN": round(row["LIVE_OPEN"], 2),
        "DAY_HIGH": round(row["LIVE_HIGH"], 2),
        "DAY_LOW": round(row["LIVE_LOW"], 2),

        # 🟠 YESTERDAY
        "YEST_HIGH": row["YEST_HIGH"],
        "YEST_LOW": row["YEST_LOW"],
        "YEST_CLOSE": row["YEST_CLOSE"],

        # ⏱️ SIGNAL TIME
        "CANDLE_TIME": candle_time.strftime("%H:%M"),
    })

three_green_df = pd.DataFrame(three_green_rows)


# =========================================================
# ADD 3×15m GREEN TO LIVE ALERTS (DEDUP SAFE)
# =========================================================

if not three_green_df.empty and "Symbol" in three_green_df.columns:

    for _, r in three_green_df.iterrows():
        sym = r["Symbol"]
        key = f"3GREEN_{sym}"

        if key in alert_keys:
            continue

        alerts.append(r.to_dict())
        alert_keys.add(key)

    new_3green = detect_new_entries(
        "THREE_GREEN_15M",
        three_green_df["Symbol"].tolist()   # ✅ SAFE ACCESS
    )

    notify_all(
        "THREE_GREEN_15M",
        "🟢3×15-Min Green Candles",
        new_3green,
        ltp_map
    )



def detect_new_15m_signals(name, rows):
    """
    rows: list of dicts with Symbol + SCANDLE_TIME
    """
    path = f"CACHE/{name}_15m_prev.txt"

    prev = set(open(path).read().split(",")) if os.path.exists(path) else set()
    curr = set(f"{r['Symbol']}|{r['CANDLE_TIME']}" for r in rows)

    new = curr - prev

    with open(path, "w") as f:
        f.write(",".join(curr))

    return [x.split("|")[0] for x in new]

three_green_15m_df = pd.DataFrame(three_green_rows)

# =========================================================
# 🔔 LIVE ALERT — 3rd 15-min GREEN candle completed
# (integrated with LIVE ALERTS table)
# =========================================================

# ================== ALERT: 3 × 15m GREEN (WITH OHLC) ==================

if not three_green_15m_df.empty:

    for _, r in three_green_15m_df.iterrows():
        sym = r["Symbol"]

        # 🔒 pull OHLC from main df (single source of truth)
        live_row = df[df["Symbol"] == sym]
        if live_row.empty:
            continue

        live_row = live_row.iloc[0]

        key = f"3X15_{sym}_{r['CANDLE_TIME']}"
        if key in alert_keys:
            continue

        alerts.append({
            "TYPE": "🟢 3×15m GREEN",
            "Symbol": sym,

            # 🔴 LIVE
            "LTP": round(live_row["LTP"], 2),
            "CHANGE_%": round(live_row["CHANGE_%"], 2),

            # 🟡 DAY OHLC
            "DAY_OPEN": round(live_row["LIVE_OPEN"], 2),
            "DAY_HIGH": round(live_row["LIVE_HIGH"], 2),
            "DAY_LOW": round(live_row["LIVE_LOW"], 2),

            # 🟠 YESTERDAY
            "YEST_HIGH": live_row["YEST_HIGH"],
            "YEST_LOW": live_row["YEST_LOW"],
            "YEST_CLOSE": live_row["YEST_CLOSE"],

            # ⏱️ SIGNAL
            "CANDLE_TIME": r["CANDLE_TIME"],
        })

        alert_keys.add(key)


############                SCENARIO 1 — Yesterday GREEN candle, tight body near high
# =========================================================
# 🟢 YEST GREEN + OPEN BETWEEN YH & YC (~1%)
# =========================================================
PCT_TOL = 1.0      # around 1%
OPEN_TOL = 0.05    # price tolerance
df["YH_MOVE"] = (df["LTP"] - df["YEST_HIGH"]).round(2)

df["YH_MOVE_%"] = ((df["LTP"] - df["YEST_HIGH"]) / df["YEST_HIGH"] * 100).round(2)


green_zone_condition = (
    ((df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100 <= PCT_TOL) &
    (df["LIVE_OPEN"] >= df["YEST_CLOSE"] + OPEN_TOL) &
    (df["LIVE_OPEN"] <= df["YEST_HIGH"] - OPEN_TOL)  &
    (df["LTP"] > 500 ) &
    (df["LTP"] >= df["YEST_HIGH"])
    #(df["LTP"] >= df["YEST_HIGH"] - OPEN_TOL)
)

green_zone_df = df.loc[
    green_zone_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        # 🆕 YH breakout strength
        "YH_MOVE",
        "YH_MOVE_%",
        "VOL_%",
        "LIVE_OPEN",   
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

green_zone_df["ZONE_%"] = (
    (df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100
).round(2)

green_zone_df = green_zone_df.sort_values("ZONE_%")

            ################################################################

green_zone_condition1 = (
    ((df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100 <= PCT_TOL) &
    (df["YEST_CLOSE"] > df["YEST_OPEN"] ) &
    (df["LIVE_OPEN"] > df["YEST_CLOSE"] ) &
    (df["LIVE_OPEN"] < df["YEST_HIGH"] )  &
    (df["LTP"] > 500 ) &
    (df["LTP"] <= df["YEST_HIGH"])
    #(df["LTP"] >= df["YEST_HIGH"] - OPEN_TOL)
)

green_zone_df1 = df.loc[
    green_zone_condition1,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",   
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

green_zone_df1["ZONE_%"] = (
    (df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100
).round(2)

green_zone_df1 = green_zone_df1.sort_values("ZONE_%")


#########       SCENARIO 2 — Yesterday RED candle, tight body near low
# =========================================================
# 🔴 YEST RED + OPEN BETWEEN YL & YC (~1%)
# =========================================================

red_zone_condition = (
    ((df["YEST_CLOSE"] - df["YEST_LOW"]) / df["YEST_CLOSE"] * 100 <= PCT_TOL) &
    (df["LIVE_OPEN"] >= df["YEST_LOW"] + OPEN_TOL) &
    (df["LIVE_OPEN"] <= df["YEST_CLOSE"] - OPEN_TOL) &
    (df["LTP"] <= df["YEST_LOW"])
    #(df["LTP"] <= df["YEST_LOW"] + OPEN_TOL)
)

red_zone_df = df.loc[
    red_zone_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",   
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

red_zone_df["ZONE_%"] = (
    (df["YEST_CLOSE"] - df["YEST_LOW"]) / df["YEST_CLOSE"] * 100
).round(2)

red_zone_df = red_zone_df.sort_values("ZONE_%")




################                STEP 1: UNIVERSAL OPTION DOWNLOADER (INDEX + STOCK)

#SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS
# ================= OPTION SYMBOLS (STOCKS + INDICES) =================

OPTION_SYMBOLS = [
    s for s in SYMBOLS
    if s not in ["BSE"]   # exclude non-option symbols if needed
]

# Safety check
if not OPTION_SYMBOLS:
    st.error("❌ OPTION_SYMBOLS is empty")
    st.stop()


def get_strike_step(symbol):
    if symbol in ["NIFTY"]:
        return 50
    if symbol in ["BANKNIFTY", "SENSEX"]:
        return 100
    return 50   # stocks

def download_option_chain(symbol):
    spot = live_map[symbol]["LTP"]
    step = get_strike_step(symbol)
    atm = int(round(spot / step) * step)

    strikes = [atm + i * step for i in range(-3, 4)]
    expiry = get_monthly_expiry(symbol)

    inst = pd.DataFrame(kite.instruments("NFO"))
    rows = []

    for strike in strikes:
        for opt_type in ["CE", "PE"]:
            row = inst[
                (inst["name"] == symbol) &
                (inst["strike"] == strike) &
                (inst["instrument_type"] == opt_type) &
                (pd.to_datetime(inst["expiry"]).dt.date == expiry)
            ]

            if row.empty:
                continue

            ts = row.iloc[0]["tradingsymbol"]
            token = int(row.iloc[0]["instrument_token"])

            try:
                q = kite.quote([f"NFO:{ts}"])[f"NFO:{ts}"]
            except:
                continue

            rows.append({
                "SYMBOL": symbol,
                "STRIKE": strike,
                "TYPE": opt_type,
                "SPOT": spot,
                "ATM": atm,
                "MONEYNESS": (
                    "ATM" if strike == atm else
                    "ITM" if (opt_type == "CE" and strike < atm) or
                              (opt_type == "PE" and strike > atm)
                    else "OTM"
                ),
                "LTP": q["last_price"],
                "OI": q.get("oi", 0),
                "OI_DAY_HIGH": q.get("oi_day_high", 0),
                "OI_DAY_LOW": q.get("oi_day_low", 0),
                "VOLUME": q.get("volume", 0),
                "IV": q.get("implied_volatility", None),
                "TIME": datetime.now(IST)
            })

    if rows:
        folder = "INDEX" if symbol in ["NIFTY", "BANKNIFTY", "SENSEX"] else "STOCK"
        path = f"{CACHE_DIR}/OPTIONS/{folder}/{symbol}.csv"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame(rows).to_csv(path, index=False)


#           STEP 2: OI BUILDUP CLASSIFICATION (KEY PART)
def classify_oi_buildup(df):
    df = df.copy()

    df["OI_CHANGE"] = df["OI_DAY_HIGH"] - df["OI_DAY_LOW"]
    df["PRICE_CHANGE"] = df["LTP"] - df.groupby("STRIKE")["LTP"].transform("first")

    def label(row):
        if row["PRICE_CHANGE"] > 0 and row["OI_CHANGE"] > 0:
            return "LONG BUILDUP"
        if row["PRICE_CHANGE"] < 0 and row["OI_CHANGE"] > 0:
            return "SHORT BUILDUP"
        if row["PRICE_CHANGE"] > 0 and row["OI_CHANGE"] < 0:
            return "SHORT COVERING"
        if row["PRICE_CHANGE"] < 0 and row["OI_CHANGE"] < 0:
            return "LONG UNWINDING"
        return "NEUTRAL"

    df["OI_BUILDUP"] = df.apply(label, axis=1)
    return df


# =========================================================
# STEP 3: ATM vs ITM HEATMAP (ULTRA SAFE VERSION)
# =========================================================

df = df.copy()

# ---- Ensure numeric base columns exist ----
for col in ["OI", "VOLUME", "IV"]:
    if col not in df.columns:
        df[col] = 0
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ---- Rank-based component scores ----
oi_score  = df["OI"].rank(pct=True) if df["OI"].sum() > 0 else 0
vol_score = df["VOLUME"].rank(pct=True) if df["VOLUME"].sum() > 0 else 0
iv_score  = df["IV"].rank(pct=True) if df["IV"].sum() > 0 else 0

# ---- Final HEAT SCORE ----
df["HEAT_SCORE"] = (
    oi_score * 0.5 +
    vol_score * 0.3 +
    iv_score * 0.2
)

df["HEAT_SCORE"] = df["HEAT_SCORE"].round(2)




############        STEP 4: OPTIONS TAB — STOCKS + INDICES
def load_option_csv(symbol):
    for folder in ["INDEX", "STOCK"]:
        path = f"{CACHE_DIR}/OPTIONS/{folder}/{symbol}.csv"
        if os.path.exists(path):
            return pd.read_csv(path)
    return pd.DataFrame()


# =========================================================
# 🚀 EARLY HIGH-GAIN RUNNERS (YH MOMENTUM ENGINE)
# =========================================================

runner_df = df.copy()

# -------------------------
# 1️⃣ Range expansion after YH
# -------------------------
runner_df["YH_RANGE_EXP_%"] = (
    (runner_df["LIVE_HIGH"] - runner_df["YEST_HIGH"])
    / runner_df["YEST_HIGH"] * 100
).round(2)

# -------------------------
# 2️⃣ Open distance from YH
# -------------------------
runner_df["OPEN_DIST_YH_%"] = (
    abs(runner_df["LIVE_OPEN"] - runner_df["YEST_HIGH"])
    / runner_df["YEST_HIGH"] * 100
).round(2)

# -------------------------
# 3️⃣ Acceptance above YH
# -------------------------
runner_df["YH_ACCEPTED"] = runner_df["LIVE_LOW"] >= runner_df["YEST_HIGH"]

# -------------------------
# 4️⃣ Momentum score (core logic)
# -------------------------
runner_df["MOMO_SCORE"] = 0

runner_df.loc[runner_df["LTP"] > runner_df["YEST_HIGH"], "MOMO_SCORE"] += 2
runner_df.loc[runner_df["CHANGE_%"] >= 1.5, "MOMO_SCORE"] += 2
runner_df.loc[runner_df["YH_RANGE_EXP_%"] >= 1.0, "MOMO_SCORE"] += 2
runner_df.loc[runner_df["YH_ACCEPTED"], "MOMO_SCORE"] += 2
runner_df.loc[runner_df["LTP"] > runner_df["LIVE_OPEN"], "MOMO_SCORE"] += 1

# -------------------------
# 5️⃣ Final EARLY RUNNER FILTER
# -------------------------
early_runner_df = runner_df.loc[
    (runner_df["LTP"] > runner_df["YEST_HIGH"]) &
    (runner_df["MOMO_SCORE"] >= 6),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "MOMO_SCORE",

        # Strength diagnostics
        "YH_RANGE_EXP_%",
        "OPEN_DIST_YH_%",
        "YH_ACCEPTED",

        # Price context
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE",
    ]
].copy()

# -------------------------
# 6️⃣ Sort → BEST early runners on TOP
# -------------------------
early_runner_df = early_runner_df.sort_values(
    by=["MOMO_SCORE", "CHANGE_%"],
    ascending=[False, False]
)


# =========================================================
# 🚨 YESTERDAY GREEN → BREAKOUT ALERT
# =========================================================
new_green_break = detect_new_entries(
    "YEST_GREEN_BREAK",
    green_zone_df.Symbol.tolist()
)

notify_all(
    "YEST_GREEN_BREAK",
    "🟢Yesterday GREEN → Breakout Above YH",
    new_green_break,
    ltp_map
)


# =========================================================
# 🚨 YESTERDAY RED → BREAKDOWN ALERT
# =========================================================
new_red_break = detect_new_entries(
    "YEST_RED_BREAK",
    red_zone_df.Symbol.tolist()
)

notify_all(
    "YEST_RED_BREAK",
    "🔴Yesterday RED → Breakdown Below YL",
    new_red_break,
    ltp_map
)



###################################################################################
#🔴 RED SETUP =  LIVE_OPEN > YEST_LOW & LIVE_OPEN < YEST_CLOSE && First 15-min LOW should NOT break yesterday LOW
#🟢 GREEN SETUP =  LIVE_OPEN > YEST_CLOSE  & LIVE_OPEN < YEST_HIGH  & First 15-min HIGH should NOT break yesterday HIGH
###########################################################################################

# =========================================================
# FIRST 15-MIN CANDLE FETCHER
# =========================================================
@st.cache_data(ttl=120)
def get_today_15m(token):
    today = date.today()
    start_dt = datetime.combine(today, time(9,15))
    end_dt   = datetime.combine(today, time(15,30))

    try:
        return kite.historical_data(
            token,
            start_dt,
            end_dt,
            "15minute"
        )
    except:
        return []


# =========================================================
# 🔴 / 🟢 DELAYED BREAK STRUCTURE
# =========================================================

# =========================================================
# 🔴 / 🟢 YESTERDAY STRUCTURE CONTINUATION (FINAL VERSION)
# =========================================================

red_rows = []
green_rows = []

for _, r in df.iterrows():

    tk = get_token(r["Symbol"])
    if not tk:
        continue

    candles = get_today_15m(tk)
    if not candles or len(candles) < 2:
        continue

    first = candles[0]
    rest  = candles[1:]

    # =====================================================
    # 🟢 GREEN STRUCTURE (Yesterday GREEN → Break Above YH)
    # =====================================================
    if (
        r["YEST_CLOSE"] > r["YEST_OPEN"] and   # Yesterday GREEN
        r["LIVE_OPEN"] > r["YEST_CLOSE"] and
        r["LIVE_OPEN"] < r["YEST_HIGH"] and
        first["high"] <= r["YEST_HIGH"]        # First 15m did not break
    ):

        breakout_candle = next(
            (c for c in rest if c["high"] > r["YEST_HIGH"]),
            None
        )

        if breakout_candle and r["LTP"] > r["YEST_HIGH"]:

            post_high = max(
                c["high"] for c in candles
                if c["date"] >= breakout_candle["date"]
            )

            gain_value = round(post_high - r["YEST_HIGH"], 2)
            gain_pct   = round((gain_value / r["YEST_HIGH"]) * 100, 2)

            green_rows.append({
                "Symbol": r["Symbol"],
                "LTP": r["LTP"],
                "CHANGE": r["CHANGE"],
                "LIVE_OPEN": r["LIVE_OPEN"],
                "YEST_OPEN": r["YEST_OPEN"],
                "YEST_CLOSE": r["YEST_CLOSE"],
                "YEST_HIGH": r["YEST_HIGH"],
                "BREAK_TIME": breakout_candle["date"].strftime("%H:%M"),
                "POST_BREAK_GAIN": gain_value,
                "POST_BREAK_GAIN_%": gain_pct,
                "CHANGE_%": r["CHANGE_%"]
            })

    # =====================================================
    # 🔴 RED STRUCTURE (Yesterday RED → Break Below YL)
    # =====================================================
    if (
        r["YEST_CLOSE"] < r["YEST_OPEN"] and   # Yesterday RED
        r["LIVE_OPEN"] > r["YEST_LOW"] and
        r["LIVE_OPEN"] < r["YEST_CLOSE"] and
        first["low"] >= r["YEST_LOW"]          # First 15m did not break
    ):

        breakdown_candle = next(
            (c for c in rest if c["low"] < r["YEST_LOW"]),
            None
        )

        if breakdown_candle and r["LTP"] < r["YEST_LOW"]:

            post_low = min(
                c["low"] for c in candles
                if c["date"] >= breakdown_candle["date"]
            )

            drop_value = round(r["YEST_LOW"] - post_low, 2)
            drop_pct   = round((drop_value / r["YEST_LOW"]) * 100, 2)

            red_rows.append({
                "Symbol": r["Symbol"],
                "LTP": r["LTP"],
                "CHANGE": r["CHANGE"],
                "LIVE_OPEN": r["LIVE_OPEN"],
                "YEST_OPEN": r["YEST_OPEN"],
                "YEST_HIGH": r["YEST_HIGH"],
                "YEST_CLOSE": r["YEST_CLOSE"],
                "YEST_LOW": r["YEST_LOW"],
                "BREAK_TIME": breakdown_candle["date"].strftime("%H:%M"),
                "POST_BREAK_DROP": drop_value,
                "POST_BREAK_DROP_%": drop_pct,
                "CHANGE_%": r["CHANGE_%"]
            })


# =========================================================
# CREATE DATAFRAMES (SAFE COLUMNS)
# =========================================================

green_structure_df = pd.DataFrame(
    green_rows,
    columns=[
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "YEST_HIGH",
        "BREAK_TIME",
        "POST_BREAK_GAIN",
        "POST_BREAK_GAIN_%"
        
    ]
)

red_structure_df = pd.DataFrame(
    red_rows,
    columns=[
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "YEST_HIGH",
        "BREAK_TIME",
        "POST_BREAK_DROP",
        "POST_BREAK_DROP_%"
    ]
)

#red_structure_df   = pd.DataFrame(red_rows)
##green_structure_df = green_structure_df.sort_values("POST_BREAK_GAIN_%", ascending=False)
#red_structure_df = red_structure_df.sort_values("POST_BREAK_DROP_%", ascending=False)
if not green_structure_df.empty and "POST_BREAK_GAIN_%" in green_structure_df.columns:
    green_structure_df = green_structure_df.sort_values(
        "POST_BREAK_GAIN_%", ascending=False
    )

if not red_structure_df.empty and "POST_BREAK_DROP_%" in red_structure_df.columns:
    red_structure_df = red_structure_df.sort_values(
        "POST_BREAK_DROP_%", ascending=False
    )







######################################################################################

############        STEP 1 — Add 1H Candle Fetch Function

# =========================================================
# 1-HOUR OPENING RANGE (9:15–10:15)
# =========================================================
@st.cache_data(ttl=60)
def get_hourly_opening_range(token):

    today = date.today()

    start = datetime.combine(today, time(9,15))
    end   = datetime.combine(today, time(15,30))

    try:
        bars = kite.historical_data(
            token,
            start,
            end,
            "15minute"
        )
    except:
        return None

    if not bars or len(bars) < 4:
        return None

    df15 = pd.DataFrame(bars)
    df15["date"] = pd.to_datetime(df15["date"])

    # First 4 candles = 9:15–10:15
    first_hour = df15.iloc[:4]

    hour_high = first_hour["high"].max()
    hour_low  = first_hour["low"].min()

    # Remaining candles
    rest = df15.iloc[4:]

    break_high = None
    break_low  = None

    for _, r in rest.iterrows():
        if break_high is None and r["high"] > hour_high:
            break_high = r["date"]
        if break_low is None and r["low"] < hour_low:
            break_low = r["date"]

    return {
        "1H_HIGH": round(hour_high, 2),
        "1H_LOW": round(hour_low, 2),
        "BREAK_HIGH_TIME": break_high,
        "BREAK_LOW_TIME": break_low
    }


###################     STEP 2 — Build Hourly Breakout Screener

# =========================================================
# ⏰ 1H OPENING RANGE BREAKOUT (WITH EMA20 FILTER)
# =========================================================

hourly_rows = []

for sym in SYMBOLS:

    # ---- Get 1H candle (9:15–10:15)
    df1h = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(df1h) < 4:
        continue

    first_hour = df1h.iloc[:4]

    h1_high = first_hour["high"].max()
    h1_low  = first_hour["low"].min()
    # 1️⃣ First Hour Range %
    range_pct = round(
        ((h1_high - h1_low) / h1_low) * 100,
        2
    )
    current_type = None
    break_time = ""
    post_break_pct = 0

    # ---- Get live row
    row = df[df["Symbol"] == sym]
    if row.empty:
        continue

    row = row.iloc[0]

    # 🔥 REQUIRE EMA20
    if pd.isna(row["EMA20"]):
        continue

    # =====================================================
    # 🟢 BREAKOUT (Above 1H High + Above EMA20)
    # =====================================================
    if (
        row["LTP"] > h1_high and
        row["LTP"] > row["EMA20"]
    ):
        post_break_pct = round(
            ((row["LTP"] - h1_high) / h1_high) * 100,
            2
        )

        hourly_rows.append({
            "Symbol": sym,
            "TYPE": "🟢 1H BREAKOUT",
            "1H_HIGH": round(h1_high, 2),
            "1H_LOW": round(h1_low, 2),
            "1H_RANGE_%": range_pct,
            "LTP": round(row["LTP"], 2),
            "CHANGE": row["CHANGE"],
            "CHANGE_%": row["CHANGE_%"],
            "POST_BREAK_MOVE_%": post_break_pct,
            "EMA20": round(row["EMA20"], 2),
            "LIVE_HIGH": row["LIVE_HIGH"],
            "LIVE_LOW": row["LIVE_LOW"],
            "YEST_HIGH": row["YEST_HIGH"],
            "YEST_LOW": row["YEST_LOW"],
            #"CHANGE_%": row["CHANGE_%"],
            "BREAK_TIME": datetime.now(IST).strftime("%H:%M:%S")
        })

    # =====================================================
    # 🔴 BREAKDOWN (Below 1H Low + Below EMA20)
    # =====================================================
    elif (
        row["LTP"] < h1_low and
        row["LTP"] < row["EMA20"]
    ):
        post_break_pct = round(
            ((h1_low - row["LTP"]) / h1_low) * 100,
            2
        )
        hourly_rows.append({
            "Symbol": sym,
            "TYPE": "🔴 1H BREAKDOWN",
            "1H_HIGH": round(h1_high, 2),
            "1H_LOW": round(h1_low, 2),
            "1H_RANGE_%": range_pct,
            "LTP": round(row["LTP"], 2),
            "CHANGE": row["CHANGE"],
            "CHANGE_%": row["CHANGE_%"],
            "POST_BREAK_MOVE_%": post_break_pct,
            "EMA20": round(row["EMA20"], 2),
            "LIVE_HIGH": row["LIVE_HIGH"],
            "LIVE_LOW": row["LIVE_LOW"],
            "YEST_HIGH": row["YEST_HIGH"],
            "YEST_LOW": row["YEST_LOW"],
            "BREAK_TIME": datetime.now(IST).strftime("%H:%M:%S")
        })


hourly_break_df = pd.DataFrame(hourly_rows)





#########   STEP 3 — Add Alerts

if not hourly_break_df.empty:

    # ===============================
    # 🟢 UPSIDE BREAKOUT ALERT
    # ===============================
    up_df = hourly_break_df[
        hourly_break_df["TYPE"].str.contains("BREAKOUT", na=False)
    ]

    if not up_df.empty:
        new_up = detect_new_entries(
            "HOURLY_BREAK_UP",
            up_df["Symbol"].tolist()
        )

        notify_all(
            "HOURLY_BREAK_UP",
            "🟢1H Opening Range BREAKOUT",
            new_up,
            ltp_map
        )

    # ===============================
    # 🔴 DOWNSIDE BREAKDOWN ALERT
    # ===============================
    down_df = hourly_break_df[
        hourly_break_df["TYPE"].str.contains("BREAKDOWN", na=False)
    ]

    if not down_df.empty:
        new_down = detect_new_entries(
            "HOURLY_BREAK_DOWN",
            down_df["Symbol"].tolist()
        )

        notify_all(
            "HOURLY_BREAK_DOWN",
            "🔴1H Opening Range BREAKDOWN",
            new_down,
            ltp_map
        )


# =========================================================
# 📅 WEEKLY BREAKS – WITH POST BREAK MOVE
# =========================================================

weekly_rows = []

for _, r in df.iterrows():

    week_high = r["HIGH_W"]
    week_low  = r["LOW_W"]

    # 🟢 Weekly Breakout
    if r["LIVE_HIGH"] > week_high:

        move = round(r["LIVE_HIGH"] - week_high, 2)
        move_pct = round((move / week_high) * 100, 2)

        weekly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🟢 WEEK BREAKOUT",
            "WEEK_HIGH": week_high,
            "WEEK_LOW": week_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

    # 🔴 Weekly Breakdown
    elif r["LIVE_LOW"] < week_low:

        move = round(week_low - r["LIVE_LOW"], 2)
        move_pct = round((move / week_low) * 100, 2)

        weekly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🔴 WEEK BREAKDOWN",
            "WEEK_HIGH": week_high,
            "WEEK_LOW": week_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

weekly_break_df = pd.DataFrame(weekly_rows)


# =========================================================
# 📆 MONTHLY BREAKS – WITH POST BREAK MOVE
# =========================================================

monthly_rows = []

for _, r in df.iterrows():

    month_high = r["HIGH_M"]
    month_low  = r["LOW_M"]

    # 🟢 Monthly Breakout
    if r["LIVE_HIGH"] > month_high:

        move = round(r["LIVE_HIGH"] - month_high, 2)
        move_pct = round((move / month_high) * 100, 2)

        monthly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🟢 MONTH BREAKOUT",
            "MONTH_HIGH": month_high,
            "MONTH_LOW": month_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

    # 🔴 Monthly Breakdown
    elif r["LIVE_LOW"] < month_low:

        move = round(month_low - r["LIVE_LOW"], 2)
        move_pct = round((move / month_low) * 100, 2)

        monthly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🔴 MONTH BREAKDOWN",
            "MONTH_HIGH": month_high,
            "MONTH_LOW": month_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

monthly_break_df = pd.DataFrame(monthly_rows)


#weekly_break_df = weekly_break_df.sort_values("POST_BREAK_MOVE_%", ascending=False)

#monthly_break_df = monthly_break_df.sort_values("POST_BREAK_MOVE_%", ascending=False)
if "POST_BREAK_MOVE_%" in weekly_break_df.columns:
    weekly_break_df = weekly_break_df.sort_values("POST_BREAK_MOVE_%")

if "POST_BREAK_MOVE_%" in monthly_break_df.columns:
    monthly_break_df = monthly_break_df.sort_values("POST_BREAK_MOVE_%")


##############      STEP 1 — ADD THIS BLOCK AFTER EMA BUILD (After df = df.merge(ema_df...))

# =========================================================
# SUPPORT / RESISTANCE ENGINE (DAILY / WEEKLY / MONTHLY)
# =========================================================

def find_pivots(df, left=3, right=3):
    highs = []
    lows = []

    for i in range(left, len(df)-right):
        window = df.iloc[i-left:i+right+1]

        if df.iloc[i]["high"] == window["high"].max():
            highs.append(df.iloc[i]["high"])

        if df.iloc[i]["low"] == window["low"].min():
            lows.append(df.iloc[i]["low"])

    return highs, lows


def cluster_levels(levels, threshold=0.005):
    levels = sorted(levels)
    clusters = []

    for lvl in levels:
        placed = False
        for cluster in clusters:
            if abs(lvl - cluster[0]) / cluster[0] < threshold:
                cluster.append(lvl)
                placed = True
                break
        if not placed:
            clusters.append([lvl])

    return clusters


def classify_cluster(cluster):
    touches = len(cluster)
    if touches >= 3:
        return "STRONG"
    elif touches == 2:
        return "WEAK"
    return None


###################         STEP 2 — BUILD S/R FROM 180-DAY DATA

# =========================================================
# BUILD DAILY SUPPORT / RESISTANCE
# =========================================================

sr_rows = []

for sym, g in ohlc_full.groupby("Symbol"):

    g = g.sort_values("date")

    if len(g) < 50:
        continue

    highs, lows = find_pivots(g)

    res_clusters = cluster_levels(highs)
    sup_clusters = cluster_levels(lows)

    for cluster in res_clusters:
        strength = classify_cluster(cluster)
        if not strength:
            continue

        level = round(np.mean(cluster), 2)

        sr_rows.append({
            "Symbol": sym,
            "TF": "DAILY",
            "TYPE": "RESISTANCE",
            "LEVEL": level,
            "STRENGTH": strength,
            "TOUCHES": len(cluster)
        })

    for cluster in sup_clusters:
        strength = classify_cluster(cluster)
        if not strength:
            continue

        level = round(np.mean(cluster), 2)

        sr_rows.append({
            "Symbol": sym,
            "TF": "DAILY",
            "TYPE": "SUPPORT",
            "LEVEL": level,
            "STRENGTH": strength,
            "TOUCHES": len(cluster)
        })

sr_df = pd.DataFrame(sr_rows)


########################            STEP 3 — MERGE NEAREST LEVEL INTO MAIN DF

# =========================================================
# FIND NEAREST STRONG SUPPORT / RESISTANCE
# =========================================================

nearest_rows = []

for sym in df["Symbol"].unique():

    price = df.loc[df.Symbol == sym, "LTP"].values[0]

    sym_levels = sr_df[
        (sr_df.Symbol == sym) &
        (sr_df.STRENGTH == "STRONG")
    ]

    if sym_levels.empty:
        continue

    supports = sym_levels[
        (sym_levels.TYPE == "SUPPORT") &
        (sym_levels.LEVEL < price)
    ]

    resistances = sym_levels[
        (sym_levels.TYPE == "RESISTANCE") &
        (sym_levels.LEVEL > price)
    ]

    nearest_sup = supports.sort_values("LEVEL", ascending=False).head(1)
    nearest_res = resistances.sort_values("LEVEL", ascending=True).head(1)

    nearest_rows.append({
        "Symbol": sym,
        "STRONG_SUPPORT": nearest_sup.LEVEL.values[0] if not nearest_sup.empty else None,
        "STRONG_RESISTANCE": nearest_res.LEVEL.values[0] if not nearest_res.empty else None
    })

nearest_df = pd.DataFrame(nearest_rows)

df = df.merge(nearest_df, on="Symbol", how="left")

# Distance %
df["SS_DIST_%"] = ((df["LTP"] - df["STRONG_SUPPORT"]) / df["STRONG_SUPPORT"] * 100).round(2)
df["SR_DIST_%"] = ((df["STRONG_RESISTANCE"] - df["LTP"]) / df["STRONG_RESISTANCE"] * 100).round(2)


# =========================================================
# BREAKOUT STRENGTH FROM DAILY / WEEKLY / MONTHLY LEVELS
# =========================================================

def breakout_strength(row):

    score = 0
    reasons = []

    # ----- DAILY -----
    if row["LTP"] > row["HIGH_D"]:
        move = (row["LTP"] - row["HIGH_D"]) / row["HIGH_D"] * 100
        score += 1
        reasons.append(f"Daily +{round(move,2)}%")

    if row["LTP"] < row["LOW_D"]:
        move = (row["LOW_D"] - row["LTP"]) / row["LOW_D"] * 100
        score += 1
        reasons.append(f"Daily -{round(move,2)}%")

    # ----- WEEKLY -----
    if row["LTP"] > row["HIGH_W"]:
        move = (row["LTP"] - row["HIGH_W"]) / row["HIGH_W"] * 100
        score += 2
        reasons.append(f"Weekly +{round(move,2)}%")

    if row["LTP"] < row["LOW_W"]:
        move = (row["LOW_W"] - row["LTP"]) / row["LOW_W"] * 100
        score += 2
        reasons.append(f"Weekly -{round(move,2)}%")

    # ----- MONTHLY -----
    if row["LTP"] > row["HIGH_M"]:
        move = (row["LTP"] - row["HIGH_M"]) / row["HIGH_M"] * 100
        score += 3
        reasons.append(f"Monthly +{round(move,2)}%")

    if row["LTP"] < row["LOW_M"]:
        move = (row["LOW_M"] - row["LTP"]) / row["LOW_M"] * 100
        score += 3
        reasons.append(f"Monthly -{round(move,2)}%")

    return score, " | ".join(reasons)


df[["BREAK_SCORE", "BREAK_DETAILS"]] = df.apply(
    lambda r: pd.Series(breakout_strength(r)),
    axis=1
)

####################        ADD THIS FUNCTION (After S/R engine)

# =========================================================
# CONFIRMED CONTINUATION FROM S/R
# =========================================================

def continuation_confirmation(row, level, direction, last_candle):

    if level is None or pd.isna(level):
        return False, 0

    distance_pct = abs((row["LTP"] - level) / level) * 100

    # Require minimum expansion
    if distance_pct < 0.3:
        return False, 0

    # Require EMA alignment
    if direction == "UP" and row["LTP"] <= row["EMA20"]:
        return False, 0

    if direction == "DOWN" and row["LTP"] >= row["EMA20"]:
        return False, 0

    # Require candle confirmation
    if last_candle is not None:
        if direction == "UP" and last_candle["close"] <= level:
            return False, 0
        if direction == "DOWN" and last_candle["close"] >= level:
            return False, 0

    return True, round(distance_pct, 2)

#############       STEP 2 — APPLY TO DAILY STRONG S/R
# =========================================================
# APPLY CONTINUATION CONFIRMATION
# =========================================================

# =========================================================
# CORRECTED CONTINUATION CONFIRMATION ENGINE
# =========================================================

conf_rows = []

for _, r in df.iterrows():

    sym = r["Symbol"]

    # Skip if no S/R
    if pd.isna(r["STRONG_SUPPORT"]) and pd.isna(r["STRONG_RESISTANCE"]):
        continue

    # Get last 15m candle
    last15 = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(last15) == 0:
        continue

    last_candle = last15.iloc[-1]

    # =====================================================
    # 🟢 RESISTANCE BREAK CONFIRMATION
    # =====================================================
    level = r["STRONG_RESISTANCE"]

    if not pd.isna(level):

        distance_pct = ((r["LTP"] - level) / level) * 100

        if (
            r["LTP"] > level and                       # Must be above level
            last_candle["close"] > level and           # Candle close above
            r["LTP"] > r["EMA20"] and                  # EMA alignment
            distance_pct > 0.3                         # Minimum expansion
        ):

            conf_rows.append({
                "Symbol": sym,
                "TYPE": "🟢 CONFIRMED RESISTANCE BREAK",
                "LEVEL": round(level,2),
                "DIST_%": round(distance_pct,2),
                "LTP": round(r["LTP"],2),
                "EMA20": round(r["EMA20"],2),
                "CHANGE_%": r["CHANGE_%"]
            })

    # =====================================================
    # 🔴 SUPPORT BREAK CONFIRMATION
    # =====================================================
    level = r["STRONG_SUPPORT"]

    if not pd.isna(level):

        distance_pct = ((level - r["LTP"]) / level) * 100

        if (
            r["LTP"] < level and                       # Must be below level
            last_candle["close"] < level and           # Candle close below
            r["LTP"] < r["EMA20"] and                  # EMA alignment
            distance_pct > 0.3                         # Minimum expansion
        ):

            conf_rows.append({
                "Symbol": sym,
                "TYPE": "🔴 CONFIRMED SUPPORT BREAK",
                "LEVEL": round(level,2),
                "DIST_%": round(distance_pct,2),
                "LTP": round(r["LTP"],2),
                "EMA20": round(r["EMA20"],2),
                "CHANGE_%": r["CHANGE_%"]
            })


continuation_df = pd.DataFrame(conf_rows)


############################################################################# FUTURES 

##############      STEP 1 — AUTO FETCH ALL FUTURES
# ================= AUTO FUTURES FETCHER =================

@st.cache_data(ttl=3600)
def get_nearest_month_futures():

    try:
        instruments = kite.instruments("NFO")
    except:
        return []

    df_inst = pd.DataFrame(instruments)

    # Only Futures
    fut_df = df_inst[df_inst["instrument_type"] == "FUT"].copy()

    if fut_df.empty:
        return []

    # Convert expiry to datetime
    fut_df["expiry"] = pd.to_datetime(fut_df["expiry"])

    # Get nearest expiry
    nearest_expiry = fut_df["expiry"].min()

    fut_df = fut_df[fut_df["expiry"] == nearest_expiry]

    # Keep only stock + index futures
    fut_df = fut_df[fut_df["segment"] == "NFO-FUT"]

    # Build trading symbols
    futures_list = [
        f"NFO:{ts}"
        for ts in fut_df["tradingsymbol"].unique()
    ]

    return futures_list

########################        STEP 2 — USE AUTO FUTURE LIST
FUTURES_LIST = get_nearest_month_futures()

##########################      STEP 3 — REAL OI TRACKING ENGINE
OI_SNAPSHOT_FILE = "oi_snapshot_auto.csv"

def load_prev_oi():
    try:
        return pd.read_csv(OI_SNAPSHOT_FILE)
    except:
        return pd.DataFrame(columns=["FUT_SYMBOL","OI"])

def save_snapshot(df):
    df[["FUT_SYMBOL","OI"]].to_csv(OI_SNAPSHOT_FILE, index=False)

######################      STEP 4 — FETCH ALL FUTURES OI
@st.cache_data(ttl=5)
def fetch_all_futures_data(futures_list):

    rows = []

    if not futures_list:
        return pd.DataFrame()

    try:
        quotes = kite.quote(futures_list)
    except:
        return pd.DataFrame()

    for sym in futures_list:

        q = quotes.get(sym)
        if not q:
            continue

        ltp = q.get("last_price", 0)
        prev_close = q.get("ohlc", {}).get("close", 0)

        price_pct = 0
        if prev_close:
            price_pct = ((ltp - prev_close) / prev_close) * 100

        rows.append({
            "FUT_SYMBOL": sym.replace("NFO:",""),
            "LTP": round(ltp,2),
            "PRICE_%": round(price_pct,2),
            "OI": q.get("oi",0)
        })

    return pd.DataFrame(rows)

################################        STEP 5 — BUILD FULL DERIVATIVE ENGINE
fut_df = fetch_all_futures_data(FUTURES_LIST)

if not fut_df.empty:

    # Load previous first
    prev_df = load_prev_oi()

    fut_df = fut_df.merge(
        prev_df,
        on="FUT_SYMBOL",
        how="left",
        suffixes=("","_PREV")
    )

    fut_df["OI_PREV"] = fut_df["OI_PREV"].fillna(0)

    # If previous is zero (first run), set equal to current
    fut_df["OI_PREV"] = np.where(
        fut_df["OI_PREV"] == 0,
        fut_df["OI"],
        fut_df["OI_PREV"]
    )

    # Now calculate change
    fut_df["REAL_OI_%"] = np.where(
        fut_df["OI_PREV"] > 0,
        ((fut_df["OI"] - fut_df["OI_PREV"]) / fut_df["OI_PREV"]) * 100,
        0
    ).round(2)

    # Save snapshot AFTER calculation
    save_snapshot(fut_df)


    def classify(row):
        if row["PRICE_%"] > 0 and row["REAL_OI_%"] > 0:
            return "🟢 LONG BUILDUP"
        elif row["PRICE_%"] < 0 and row["REAL_OI_%"] > 0:
            return "🔴 SHORT BUILDUP"
        elif row["PRICE_%"] > 0 and row["REAL_OI_%"] < 0:
            return "⚠ SHORT COVERING"
        elif row["PRICE_%"] < 0 and row["REAL_OI_%"] < 0:
            return "⚠ LONG UNWINDING"
        return "NEUTRAL"

    fut_df["POSITION_TYPE"] = fut_df.apply(classify, axis=1)

else:
    fut_df = pd.DataFrame()


###################     STEP 6 — OI HEATMAP SCORE
# Create strength score
fut_df["OI_SCORE"] = (fut_df["REAL_OI_%"].abs() + fut_df["PRICE_%"].abs()).round(2)

##########################          SNIPPET — STRONG CLOSING FILTER

# ================= STRONG CLOSING FILTER =================

# Add distance from day high / low
fut_df["DIST_HIGH_%"] = (
    (fut_df["LTP"] - fut_df["LTP"])  # placeholder, replace below
)

# We need Day High & Day Low from quote
quotes = kite.quote(FUTURES_LIST)

high_low_rows = []

for sym in FUTURES_LIST:
    q = quotes.get(sym)
    if not q:
        continue

    high_low_rows.append({
        "FUT_SYMBOL": sym.replace("NFO:",""),
        "DAY_HIGH": q.get("ohlc", {}).get("high", 0),
        "DAY_LOW": q.get("ohlc", {}).get("low", 0),
    })

hl_df = pd.DataFrame(high_low_rows)

fut_df = fut_df.merge(hl_df, on="FUT_SYMBOL", how="left")

# Distance calculations
fut_df["DIST_FROM_HIGH_%"] = (
    (fut_df["DAY_HIGH"] - fut_df["LTP"]) / fut_df["DAY_HIGH"] * 100
).round(2)

fut_df["DIST_FROM_LOW_%"] = (
    (fut_df["LTP"] - fut_df["DAY_LOW"]) / fut_df["DAY_LOW"] * 100
).round(2)

# ================= STRONG LONG CLOSING =================
strong_long_close = fut_df[
    (fut_df["PRICE_%"] > 1) &
    (fut_df["REAL_OI_%"] > 1) &
    (fut_df["DIST_FROM_HIGH_%"] <= 0.5)
]

# ================= STRONG SHORT CLOSING =================
strong_short_close = fut_df[
    (fut_df["PRICE_%"] < -1) &
    (fut_df["REAL_OI_%"] > 1) &
    (fut_df["DIST_FROM_LOW_%"] <= 0.5)
]




################        STEP 1 — ADD OI FETCH FUNCTION
# =========================================================
# 📊 OI DATA FETCH
# =========================================================

@st.cache_data(ttl=60)
def fetch_oi_data():
    oi_rows = []

    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except:
        return pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])

    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue

        oi = q.get("oi", None)
        oi_day_high = q.get("oi_day_high", None)
        oi_day_low = q.get("oi_day_low", None)

        if oi is None or oi_day_high is None:
            continue

        # Approximate OI change %
        oi_change_pct = 0
        if oi_day_high and oi_day_high != 0:
            oi_change_pct = ((oi - oi_day_low) / oi_day_low) * 100 if oi_day_low else 0

        oi_rows.append({
            "Symbol": s,
            "OI": oi,
            "OI_CHANGE_%": round(oi_change_pct, 2)
        })

    return pd.DataFrame(oi_rows)

###################     STEP 2 — MERGE OI INTO MAIN DF
# ================= OI MERGE =================
oi_df = fetch_oi_data()
df = df.merge(oi_df, on="Symbol", how="left")

#df["OI"] = pd.to_numeric(df.get("OI", 0), errors="coerce").fillna(0)
# ================= SAFE OI HANDLING =================

if "OI" not in df.columns:
    df["OI"] = 0

df["OI"] = pd.to_numeric(df["OI"], errors="coerce").fillna(0)
df["OI_CHANGE_%"] = pd.to_numeric(df.get("OI_CHANGE_%", 0), errors="coerce").fillna(0)

###################     STEP 3 — OI STRENGTH ENGINE
# =========================================================
# 🔥 OI BASED STRENGTH METER
# =========================================================

def oi_strength_logic(row):
    price_change = row.get("CHANGE_%", 0)
    oi_change = row.get("OI_CHANGE_%", 0)

    if price_change > 0 and oi_change > 0:
        return "🟢 LONG BUILDUP", 3

    elif price_change < 0 and oi_change > 0:
        return "🔴 SHORT BUILDUP", 3

    elif price_change > 0 and oi_change < 0:
        return "⚠ SHORT COVERING", 1

    elif price_change < 0 and oi_change < 0:
        return "⚠ LONG UNWINDING", 1

    return "NEUTRAL", 0


df[["OI_SIGNAL","OI_SCORE"]] = df.apply(
    lambda r: pd.Series(oi_strength_logic(r)), axis=1
)


################        STEP 4 — STRONG OI FILTER TABLE

# Strong OI moves only
oi_strong_df = df[df["OI_SCORE"] >= 3].copy()

OI_COLUMNS = [
    "Symbol",
    "LTP",
    "CHANGE_%",
    "OI",
    "OI_CHANGE_%",
    "OI_SIGNAL",
    "EMA20",
    "EMA50",
    "SUPERTREND"
]

OI_COLUMNS = [c for c in OI_COLUMNS if c in oi_strong_df.columns]
oi_strong_df = oi_strong_df[OI_COLUMNS].sort_values("CHANGE_%", ascending=False)


########################        🔥 SNIPPET – Strong Closing Near Day High

# =========================================================
# DISTANCE FROM DAY HIGH / LOW (%)
# =========================================================

# Ensure numeric
df["LIVE_HIGH"] = pd.to_numeric(df["LIVE_HIGH"], errors="coerce")
df["LIVE_LOW"] = pd.to_numeric(df["LIVE_LOW"], errors="coerce")
df["LTP"] = pd.to_numeric(df["LTP"], errors="coerce")

# Distance from day high %
df["DIST_FROM_DAY_HIGH_%"] = (
    (df["LIVE_HIGH"] - df["LTP"]) / df["LIVE_HIGH"] * 100
).round(2)

# Distance from day low %
df["DIST_FROM_DAY_LOW_%"] = (
    (df["LTP"] - df["LIVE_LOW"]) / df["LIVE_LOW"] * 100
).round(2)


# ================= STRONG STOCK CLOSING =================

# =========================================================
# 🔥 CLEAN YH BREAK + STRONG CLOSE NEAR DAY HIGH
# =========================================================

strong_high_df = df.loc[
    (df["LIVE_OPEN"] <= df["YEST_HIGH"]) &      # No gap-up
    (df["LTP"] >= df["YEST_HIGH"]) &           # Breakout above YH
    (df["LIVE_HIGH"] >= df["YEST_HIGH"]) &
    (df["DIST_FROM_DAY_HIGH_%"] <= 0.5) &        # Strong close near day high
    (df["CHANGE_%"] >= 0.5) &
    (df["CHANGE_%"] <= 2.5)
].copy()

if not strong_high_df.empty:

    strong_high_df = strong_high_df[
        [
            "Symbol",
            "LTP",
            "CHANGE",
            "CHANGE_%",
            "LIVE_HIGH",
            "DIST_FROM_DAY_HIGH_%",
            "YEST_HIGH",
            "HIGH_W",
            "HIGH_M",
            "EMA20",
            "TOP_HIGH"
        ]
    ]

    
else:
    st.info("No strong bullish closing stocks near day high.")


#############################       LOGIC – Strong Red Closing (Near Day Low)
# ================= STRONG RED CLOSING =================

# =========================================================
# 🔴 CLEAN YL BREAK + STRONG CLOSE NEAR DAY LOW
# =========================================================

strong_low_df = df.loc[
    (df["LIVE_OPEN"] >= df["YEST_LOW"]) &        # No gap-down
    (df["LTP"] <= df["YEST_LOW"]) &             # Breakdown below YL
    (df["DIST_FROM_DAY_LOW_%"] <= 0.5) &        # Closing near day low
    (df["CHANGE_%"] <= -0.5)  &                     # Bearish momentum
    (df["CHANGE_%"] >= -2.2)
].copy()

if not strong_low_df.empty:

    strong_low_df = strong_low_df[
        [
            "Symbol",
            "LTP",
            "CHANGE",
            "CHANGE_%",
            "LIVE_LOW",
            "DIST_FROM_DAY_LOW_%",
            "YEST_LOW",
            "LOW_W",
            "LOW_M",
            "EMA20",
            "TOP_LOW"
        ]
    ]

    

else:
    st.info("No strong bearish closing stocks near day low.")



################################    Bullish Version (Reclaim After Gap Up Failure)
###############################     Bearish Version (Reclaim After Gap Down Failure)
# ================= GAP RECLAIM / FAILURE SCREENER =================

# 🟢 Bullish Reclaim After Gap Up Failure
bull_reclaim_df = df[
    (df["LIVE_OPEN"] > df["YEST_HIGH"]) &   # gap above YH
    (df["LIVE_LOW"] < df["YEST_HIGH"]) &    # dipped below YH
    (df["LTP"] > df["YEST_HIGH"])   &        # reclaimed & holding
    (df["LTP"] > df["EMA20"]) &
    (df["VOL_%"] > 10)#(df["VOL_%"] > 30)
].copy()

bull_reclaim_df["STRENGTH_%"] = (
    (df["LTP"] - df["YEST_HIGH"]) / df["YEST_HIGH"] * 100
).round(2)

bull_reclaim_df = bull_reclaim_df.sort_values("STRENGTH_%", ascending=False)


# 🔴 Bearish Reclaim After Gap Down Failure
bear_reclaim_df = df[
    (df["LIVE_OPEN"] < df["YEST_LOW"]) &    # gap below YL
    (df["LIVE_HIGH"] > df["YEST_LOW"]) &    # moved above YL intraday
    (df["LTP"] < df["YEST_LOW"])  &          # rejected & holding below
    (df["LTP"] < df["EMA20"]) &
    (df["VOL_%"] > 10)#(df["VOL_%"] > 30)
].copy()

bear_reclaim_df["STRENGTH_%"] = (
    (df["YEST_LOW"] - df["LTP"]) / df["YEST_LOW"] * 100
).round(2)

bear_reclaim_df = bear_reclaim_df.sort_values("STRENGTH_%", ascending=False)



# ================= 0.5% BREAK ZONES =================

# Ensure numeric
df["YEST_HIGH"] = pd.to_numeric(df["YEST_HIGH"], errors="coerce")
df["YEST_LOW"]  = pd.to_numeric(df["YEST_LOW"], errors="coerce")

# 0.5% above yesterday high
df["YH_05"] = (df["YEST_HIGH"] * 1.005).round(2)

# 0.5% below yesterday low
df["YL_05"] = (df["YEST_LOW"] * 0.995).round(2)


####################            Step 2: Define “Last 2 Months Downtrend”
def get_2m_downtrend_symbols():
    if not os.path.exists(OHLC_FILE):
        return []

    ohlc = pd.read_csv(OHLC_FILE)
    ohlc["date"] = pd.to_datetime(ohlc["date"])

    down_symbols = []

    for sym, g in ohlc.groupby("Symbol"):
        g = g.sort_values("date")

        if len(g) < 40:
            continue

        first_close = g.iloc[0]["close"]
        last_close  = g.iloc[-1]["close"]

        if last_close < first_close:
            down_symbols.append(sym)

    return down_symbols

########################        Step 3: Build Your New Table

downtrend_symbols = get_2m_downtrend_symbols()

reversal_df = df.loc[
    (df["Symbol"].isin(downtrend_symbols)) &
    (df["LTP"] >= df["EMA7"]) &
    (df["LTP"] >= df["EMA20"]) &
    (df["LTP"] >= df["EMA50"]) &
    (df["EMA20"] >= df["EMA50"]),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",        
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "DIST_FROM_DAY_HIGH_%",
        "YH_05",
        "YL_05",
        "DIST_FROM_DAY_LOW_%", 
        "EMA7",
        "EMA20",
        "EMA50"
        
    ]
].copy()

reversal_df = reversal_df.sort_values("CHANGE_%", ascending=False)
new_reversal = detect_new_entries(
    "2M_EMA_REVERSAL",
    reversal_df.Symbol.tolist()
)

notify_all(
    "2M_EMA_REVERSAL",
    "🚀 2M Downtrend → EMA Reversal",
    new_reversal,
    ltp_map
)


astro = get_astro_score()

def allow_trade(row):

    # 🚫 Block bad astro days
    if astro["score"] <= -2:
        return "BLOCK"

    # 🔥 Strong day → full trade
    if astro["score"] >= 1:
        return "FULL"

    # ⚠️ Neutral → only breakout trades
    if astro["score"] == 0:
        if row["LTP"] > row["TOP_HIGH"] or row["LTP"] < row["TOP_LOW"]:
            return "BREAKOUT ONLY"
        else:
            return "WAIT"

    return "WAIT"

df["ASTRO_PERMISSION"] = df.apply(allow_trade, axis=1)




############################################################################################################
############################################################################################################
############################################################################################################
############################################################################################################




# ================== LIVE ALERTS ==================
if "alerts" not in st.session_state:
    st.session_state.alerts = []

if "alerted_symbols" not in st.session_state:
    st.session_state.alerted_symbols = set()

# ================== LIVE ALERTS ENGINE ==================

# ================== LIVE ALERTS ENGINE (NO DUPLICATES) ==================

df["EMA7"] = pd.to_numeric(df["EMA7"], errors="coerce")
df["EMA20"] = pd.to_numeric(df["EMA20"], errors="coerce")
df["EMA50"] = pd.to_numeric(df["EMA50"], errors="coerce")


from datetime import datetime

new_alerts = []

for _, r in df.iterrows():

    sym = r["Symbol"]
    now_time = datetime.now(IST).replace(tzinfo=None)

    # =====================================================
    # 🟢 UPWARD BREAK
    # =====================================================
    #if (r["LTP"] > r["YEST_HIGH"] and  r["CHANGE_%"] > 1 ):
    #if (r["LTP"] >= r.get("YEST_HIGH", 0) and r["CHANGE_%"] >= 0.5 and r.get("VOL_%", 0) >= -30 ):
    if (
        r["LTP"] >= r.get("YEST_HIGH", 0) and
        r["CHANGE_%"] >= 0.5 and
        r.get("VOL_%", 0) >= -30 and

        # 🔥 EMA Structure Confirmation
        r.get("LTP", 0) >= r.get("EMA7", 0) and
        r.get("LTP", 0) >= r.get("EMA20", 0) and
        r.get("LTP", 0) >= r.get("EMA50", 0) and
        r.get("EMA20", 0) >= r.get("EMA50", 0)
    ):

        key = f"{sym}_UP"

        if key not in st.session_state.alerted_symbols:

            new_alerts.append({
                "TS": now_time,
                "TIME": now_time.strftime("%H:%M:%S"),
                "TYPE": "🟢 YH BREAK",
                "Symbol": sym,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                #"LIVE_VOLUME": r.get("LIVE_VOLUME", 0),
                #"YEST_VOL": r.get("YEST_VOL", 0),
                "VOL_%": r.get("VOL_%", 0),
                "FROM_YH_%": r["FROM_YH_%"],
                "FROM_YL_%": r["FROM_YL_%"],
                "DAY_OPEN": r["LIVE_OPEN"],
                "DAY_HIGH": r["LIVE_HIGH"],
                "DAY_LOW": r["LIVE_LOW"],
                "YEST_HIGH": r["YEST_HIGH"],
                "YEST_LOW": r["YEST_LOW"],
                "YEST_CLOSE": r["YEST_CLOSE"],
            })

            st.session_state.alerted_symbols.add(key)

    # =====================================================
    # 🔴 DOWNWARD BREAK
    # =====================================================
    #if (r["LTP"] < r["YEST_LOW"] and r["CHANGE_%"] < -1 ):
    #if (r["LTP"] <= r.get("YEST_LOW", 0) and r["CHANGE_%"] >= -0.5 and r.get("VOL_%", 0) >= -30 ):
    if (
        r["LTP"] <= r.get("YEST_LOW", 0) and
        r["CHANGE_%"] <= -0.5 and
        r.get("VOL_%", 0) >= -30 and

        r.get("LTP", 0) <= r.get("EMA7", 0) and
        r.get("LTP", 0) <= r.get("EMA20", 0) and
        r.get("LTP", 0) <= r.get("EMA50", 0) and
        r.get("EMA20", 0) <= r.get("EMA50", 0)
    ):

        key = f"{sym}_DOWN"

        if key not in st.session_state.alerted_symbols:

            new_alerts.append({
                "TS": now_time,
                "TIME": now_time.strftime("%H:%M:%S"),
                "TYPE": "🔴 YL BREAK",
                "Symbol": sym,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                #"LIVE_VOLUME": r.get("LIVE_VOLUME", 0),
                #"YEST_VOL": r.get("YEST_VOL", 0),
                "VOL_%": r.get("VOL_%", 0),
                "FROM_YH_%": r["FROM_YH_%"],
                "FROM_YL_%": r["FROM_YL_%"],
                "DAY_OPEN": r["LIVE_OPEN"],
                "DAY_HIGH": r["LIVE_HIGH"],
                "DAY_LOW": r["LIVE_LOW"],
                "YEST_HIGH": r["YEST_HIGH"],
                "YEST_LOW": r["YEST_LOW"],
                "YEST_CLOSE": r["YEST_CLOSE"],
            })

            st.session_state.alerted_symbols.add(key)


# Append only new alerts
if new_alerts:
    st.session_state.alerts.extend(new_alerts)

if datetime.now(IST).hour == 9 and datetime.now(IST).minute < 16:
    st.session_state.alerted_symbols.clear()


# ================== DISPLAY SECTION ==================

alerts_df = pd.DataFrame(st.session_state.alerts)

st.subheader("⚡ LIVE ALERTS")

if alerts_df.empty:
    st.info("No live alerts yet.")
else:

    #alerts_df = alerts_df.sort_values("TS", ascending=False)
    if "TS" in alerts_df.columns:
        alerts_df["TS"] = pd.to_datetime(alerts_df["TS"], errors="coerce")
        alerts_df = alerts_df.sort_values("TS", ascending=False)


    display_cols = [
        "TIME",
        "TYPE",
        "Symbol",
        "LTP",
        "CHANGE_%",
        "LIVE_VOLUME",
        "YEST_VOL",
        "VOL_%",
        "DAY_OPEN",
        "DAY_HIGH",
        "DAY_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]

    existing_cols = [c for c in display_cols if c in alerts_df.columns]
    alerts_df = alerts_df[existing_cols]

    st.dataframe(
        alerts_df,
        use_container_width=True
    )



# =================================================
# ================= CLEAN OLD ALERTS =================

# Normalize TS
for a in st.session_state.alerts:
    if "TS" in a and a["TS"] is not None:
        a["TS"] = pd.Timestamp(a["TS"]).tz_localize(None)

# Remove alerts older than 30 mins
cutoff = pd.Timestamp.now(tz=IST).tz_localize(None) - timedelta(minutes=30)

st.session_state.alerts = [
    a for a in st.session_state.alerts
    if "TS" in a and a["TS"] >= cutoff
]


            ######################      Clear alerts button
if st.button("🧹 Clear Alerts"):
    st.session_state.alerts = []
    st.session_state.alert_keys = set()


tabs = st.tabs([
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    "🟡 NEAR",
    "WATCHLIST",
    "BREAKOUT",
    "📈 D-BREAKS",
    "📊 W-BREAKS",
    "📅 M-BREAKS",
    "⚡ O=H=L",  
    "📉 EMA20-50",
    "🔥 TOP G/L",
    " 4-BAR",
    "🧠 OPTIONS",
    "INDICES",
    "15-MIN-3",
    "⚡ Alerts",    
    "ℹ️ INFO"
])

with tabs[0]:
    #st.dataframe(df, width="stretch")
    #st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    st.subheader("🪐 Panchak – Full View")
    st.dataframe(panchak_view, width="stretch",height=7800)

with tabs[1]:
    
    st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    TOP_HIGH_df = (
    df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
    .sort_values(by="GAIN", ascending=True)   # least positive gain on top
    )

    st.dataframe(TOP_HIGH_df, width="stretch", height=7000)


with tabs[2]:
    #st.dataframe(df[df.LTP <= df.TOP_LOW])
    st.markdown('<div class="section-red"><b>🔴 TOP LIVE_LOW – Breakdowns</b></div>', unsafe_allow_html=True)
    TOP_LOW_df = (
    df.loc[df.LTP <= df.TOP_LOW, TOP_LOW_COLUMNS]
    .sort_values(by="GAIN", ascending=False)   # least negative gain on top
    )

    st.dataframe(TOP_LOW_df, width="stretch", height=7000)


with tabs[3]:
    st.markdown(
        '<div class="section-yelLIVE_LOW"><b>🟡 NEAR – Watch Zone</b></div>',
        unsafe_allow_html=True
    )

    # only stocks inside Panchak range
    near_base = df.loc[
        (df.LTP > df.TOP_LOW) & 
        (df.LTP < df.TOP_HIGH),
        [
            "Symbol",
            "TOP_HIGH",
            "TOP_LOW",
            "LTP",
            "CHANGE",
            "CHANGE_%",
            "EMA20"
        ]
    ].copy()

    if near_base.empty:
        st.info("No stocks currently between TOP_HIGH and TOP_LOW")
    else:

        # Distance calculation
        near_base["DIST_LIVE_HIGH"] = (near_base["TOP_HIGH"] - near_base["LTP"]).round(2)
        near_base["DIST_LIVE_LOW"]  = (near_base["LTP"] - near_base["TOP_LOW"]).round(2)

        # =========================
        # 🟢 NEAR BUY (Closer to TOP_HIGH + Above EMA20)
        # =========================
        near_buy_df = near_base[
            (near_base["DIST_LIVE_HIGH"] <= near_base["DIST_LIVE_LOW"]) &
            (near_base["LTP"] > near_base["EMA20"])
        ].copy()

        # =========================
        # 🔴 NEAR SELL (Closer to TOP_LOW + Below EMA20)
        # =========================
        near_sell_df = near_base[
            (near_base["DIST_LIVE_HIGH"] > near_base["DIST_LIVE_LOW"]) &
            (near_base["LTP"] < near_base["EMA20"])
        ].copy()

        # Arrow display
        near_buy_df["NEAR"]  = "🟢 ↑ " + near_buy_df["DIST_LIVE_HIGH"].astype(str)
        near_sell_df["NEAR"] = "🔴 ↓ " + near_sell_df["DIST_LIVE_LOW"].astype(str)

        # Sorting
        near_buy_df  = near_buy_df.sort_values("DIST_LIVE_HIGH")
        near_sell_df = near_sell_df.sort_values("DIST_LIVE_LOW")

        # Final columns
        display_cols = [
            "Symbol",
            "TOP_HIGH",
            "TOP_LOW",
            "LTP",
            "EMA20",
            "NEAR",
            "CHANGE",
            "CHANGE_%"
        ]

        near_buy_df  = near_buy_df[display_cols]
        near_sell_df = near_sell_df[display_cols]

        # Layout
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🟢 NEAR BUY (Above EMA20)")
            if near_buy_df.empty:
                st.info("No BUY-side NEAR stocks")
            else:
                st.dataframe(
                    near_buy_df,
                    width="stretch",
                    height=min(4200, 60 + len(near_buy_df) * 35)
                )

        with col2:
            st.markdown("### 🔴 NEAR SELL (Below EMA20)")
            if near_sell_df.empty:
                st.info("No SELL-side NEAR stocks")
            else:
                st.dataframe(
                    near_sell_df,
                    width="stretch",
                    height=min(4200, 60 + len(near_buy_df) * 35)
                )

# =========================================================
# 📌 WATCHLIST TAB
# =========================================================

with tabs[4]:   # 👈 replace with correct index
    #st.markdown("## 📌 Intraday Watchlist")

    # -------------------------------
    # Common Columns
    # -------------------------------
    WATCH_COLS = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",        
        "DIST_FROM_DAY_HIGH_%",
        "DIST_FROM_DAY_LOW_%",
        "YEST_HIGH",
        "YEST_LOW",        
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS = [c for c in WATCH_COLS if c in df.columns]

    WATCH_COLS_YH = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "YEST_HIGH",
        "DIST_FROM_DAY_HIGH_%",
        "YL_05",        
        "LIVE_LOW",        
        "DIST_FROM_DAY_LOW_%",        
        "YEST_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YH = [c for c in WATCH_COLS_YH if c in df.columns]

    WATCH_COLS_YL = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_LOW",
        "YEST_LOW",
        "DIST_FROM_DAY_LOW_%",
        "LIVE_HIGH",    
        "DIST_FROM_DAY_HIGH_%",
        "YH_05",
        "YEST_HIGH",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YL = [c for c in WATCH_COLS_YL if c in df.columns]


    # =====================================================
    # 1️⃣ STRUCTURE – Inside YH Reclaim + Above EMA
    # open < yest.high and open >yest.close
    # LTP >yest.close and LTP>EMA20 and LTP<yest.high
    # =====================================================

    table1 = df[
        (df["LIVE_OPEN"] < df["YEST_HIGH"]) &
        (df["LIVE_OPEN"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] < df["YEST_HIGH"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YH].copy()
    #table1 = table1.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table1 = table1.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 Above Close + Below YH + EMA")

    if table1.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table1.sort_values("VOL_%", ascending=False), width="stretch")
        st.dataframe(table1, width="stretch")

    # =====================================================
    # 2️⃣ OPEN = LOW + Above EMA
    # open==low and ltp >ema20 and ltp <= yest.high
    # =====================================================

    table2 = df[
        (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] <= df["YEST_HIGH"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YH].copy()
    table2 = table2.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)

    st.markdown("### 🟢 Open = Low + Below YH + EMA Support")

    if table2.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table2.sort_values("VOL_%", ascending=False), use_container_width=True)

    # =====================================================
    # 3️⃣ Weak Bounce Structure (Below Close + Below EMA)
    # open > yest.low and open <=yest.close
    # LTP <=yest.close and LTP<=EMA20 and LTP>yest.low
    # =====================================================

    table3 = df[
        (df["LIVE_OPEN"] > df["YEST_LOW"]) &
        (df["LIVE_OPEN"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] > df["YEST_LOW"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YL].copy()
    table3 = table3.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Below Close + ABOVE YEST.LOW + EMA ")

    if table3.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table3.sort_values("VOL_%"), use_container_width=True)

    # =====================================================
    # 4️⃣ OPEN = HIGH + Below EMA
    # open==high and ltp <=ema20 and ltp >= yest.low
    # =====================================================

    table4 = df[
        (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] >= df["YEST_LOW"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YL].copy()
    table4 = table4.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Open = High + ABOVE YEST.LOW + EMA ")

    if table4.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table4.sort_values("VOL_%"), use_container_width=True)


# =========================================================
# 📌 BREAKOUT TAB
# =========================================================

with tabs[5]:   # 👈 replace with correct index
    #st.markdown("## 📌 Intraday Watchlist")

    # -------------------------------
    # Common Columns
    # -------------------------------
    WATCH_COLS = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "DIST_FROM_DAY_HIGH_%",
        "DIST_FROM_DAY_LOW_%",
        "YEST_HIGH",
        "YEST_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS = [c for c in WATCH_COLS if c in df.columns]

    WATCH_COLS_YH = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "YEST_HIGH",
        "DIST_FROM_DAY_HIGH_%",
        "YL_05",        
        "LIVE_LOW",        
        "DIST_FROM_DAY_LOW_%",        
        "YEST_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YH = [c for c in WATCH_COLS_YH if c in df.columns]

    WATCH_COLS_YL = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_LOW",
        "YEST_LOW",
        "DIST_FROM_DAY_LOW_%",
        "LIVE_HIGH",    
        "DIST_FROM_DAY_HIGH_%",
        "YH_05",
        "YEST_HIGH",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YL = [c for c in WATCH_COLS_YL if c in df.columns]

    # =====================================================
    # 1️⃣ STRUCTURE – Inside YH Reclaim + Above EMA
    # open < yest.high and open >yest.close
    # LTP >yest.close and LTP>EMA20 and LTP<yest.high
    # =====================================================

    table1 = df[
        (df["LIVE_OPEN"] < df["YEST_HIGH"]) &
        (df["LIVE_OPEN"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= -50) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    ##table1 = table1.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table1 = table1.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 Above YH + EMA ")

    if table1.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table1.sort_values("VOL_%", ascending=False), use_container_width=True)
        st.dataframe(table1, width="stretch")

    table10 = df[
        (df["LIVE_OPEN"] >= df["YEST_HIGH"]) &
        (df["LIVE_LOW"] <= df["YEST_HIGH"]) &
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= -50) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    #table10 = table10.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table10 = table10.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 OPEN ABOVE YH ")

    if table10.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table10.sort_values("VOL_%", ascending=False), use_container_width=True)
        st.dataframe(table10, width="stretch")

    table11 = df[
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= 0) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    #table11 = table11.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table11 = table11.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 VOL% ")

    if table11.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table11.sort_values("VOL_%", ascending=False), use_container_width=True)
        st.dataframe(table11, width="stretch")



    # =====================================================
    # 2️⃣ OPEN = LOW + Above EMA
    # open==low and ltp >ema20 and ltp <= yest.high
    # =====================================================

    table2 = df[
        (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= -50) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    table2 = table2.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)

    st.markdown("### 🟢 Open = Low + YH + EMA Support")

    if table2.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table2.sort_values("VOL_%", ascending=False), use_container_width=True)

    # =====================================================
    # 3️⃣ Weak Bounce Structure (Below Close + Below EMA)
    # open > yest.low and open <=yest.close
    # LTP <=yest.close and LTP<=EMA20 and LTP>yest.low
    # =====================================================

    table3 = df[
        (df["LIVE_OPEN"] > df["YEST_LOW"]) &
        (df["LIVE_OPEN"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] <= df["YEST_LOW"]) & (df["VOL_%"] >= -50) & (df["LTP"] >= df["YEST_LOW"] * 0.995)
    ][WATCH_COLS_YL].copy()
    table3 = table3.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Below YEST_LOW ")

    if table3.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table3.sort_values("VOL_%"), use_container_width=True)

    # =====================================================
    # 4️⃣ OPEN = HIGH + Below EMA
    # open==high and ltp <=ema20 and ltp >= yest.low
    # =====================================================

    table4 = df[
        (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] <= df["YEST_LOW"]) & (df["VOL_%"] >= -50)  & (df["LTP"] >= df["YEST_LOW"] * 0.995)
    ][WATCH_COLS_YL].copy()
    table4 = table4.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Open = High + YEST_LOW")

    if table4.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table4.sort_values("VOL_%"), use_container_width=True)

    table5 = df[
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] <= df["YEST_LOW"]) & (df["VOL_%"] >= 0)  & (df["LTP"] >= df["YEST_LOW"] * 0.995)
    ][WATCH_COLS_YL].copy()
    table5 = table5.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 VOL_% + YEST_LOW")

    if table5.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table5.sort_values("VOL_%"), use_container_width=True)




# =========================================================
# 🔥 STRONG CLOSING TAB (VERTICAL LAYOUT)
# =========================================================

with tabs[6]:   # change tab index if required

    st.subheader("🟢 Green Open Structure")
    st.dataframe(green_structure_df, use_container_width=True)
    #st.divider()

    st.subheader("🔴 Red Open Structure")
    st.dataframe(red_structure_df, use_container_width=True)

    st.markdown("## 🔥 Strong Closing Stocks")

    # -----------------------------------------------------
    # Ensure numeric fields
    # -----------------------------------------------------
    df["LIVE_VOLUME"] = pd.to_numeric(df.get("LIVE_VOLUME", 0), errors="coerce")
    df["YEST_VOL"] = pd.to_numeric(df.get("YEST_VOL", 0), errors="coerce")
    df["VOL_%"] = pd.to_numeric(df.get("VOL_%", 0), errors="coerce")

    df["LIVE_HIGH"] = pd.to_numeric(df.get("LIVE_HIGH", 0), errors="coerce")
    df["LIVE_LOW"] = pd.to_numeric(df.get("LIVE_LOW", 0), errors="coerce")
    df["LTP"] = pd.to_numeric(df.get("LTP", 0), errors="coerce")

    # -----------------------------------------------------
    # Distance calculation (safe)
    # -----------------------------------------------------
    df["DIST_FROM_DAY_HIGH_%"] = (
        (df["LIVE_HIGH"] - df["LTP"]) / df["LIVE_HIGH"] * 100
    ).round(2)

    df["DIST_FROM_DAY_LOW_%"] = (
        (df["LTP"] - df["LIVE_LOW"]) / df["LIVE_LOW"] * 100
    ).round(2)

    # =====================================================
    # 🟢 STRONG NEAR DAY HIGH
    # =====================================================
    #strong_high_df = df.loc[(df["CHANGE_%"] > 1.5) & (df["DIST_FROM_DAY_HIGH_%"] <= 0.5)].copy()
    strong_high_df = df.loc[
    (df["LIVE_OPEN"] <= df["YEST_HIGH"]) &      # No gap-up
    (df["LTP"] >= df["YEST_HIGH"]) &           # Breakout above YH
    (df["LIVE_HIGH"] >= df["YEST_HIGH"]) &
    (df["DIST_FROM_DAY_HIGH_%"] <= 0.5) &        # Strong close near day high
    (df["CHANGE_%"] >= 0.5) &
    (df["CHANGE_%"] <= 2.5) & (df["LTP"] >= df["EMA20"]) 
    ].copy()

    # =====================================================
    # 🔴 STRONG NEAR DAY LOW
    # =====================================================
    #strong_low_df = df.loc[(df["CHANGE_%"] < -1.5) & (df["DIST_FROM_DAY_LOW_%"] <= 0.5)].copy()
    strong_low_df = df.loc[
    (df["LIVE_OPEN"] >= df["YEST_LOW"]) &        # No gap-down
    (df["LTP"] <= df["YEST_LOW"]) &             # Breakdown below YL
    (df["DIST_FROM_DAY_LOW_%"] <= 0.5) &        # Closing near day low
    (df["CHANGE_%"] <= -0.5)  &                     # Bearish momentum
    (df["CHANGE_%"] >= -2.2) & (df["LTP"] <= df["EMA20"]) & (df["LTP"] <= df["TOP_LOW"]) & (df["LTP"] <= df["LOW_W"])
    ].copy()

    # =====================================================
    # STYLE FUNCTIONS
    # =====================================================

    def highlight_high_breaks(row):
        styles = []
        for col in row.index:
            if col in ["YEST_HIGH","HIGH_W","HIGH_M","EMA20","TOP_HIGH"]:
                if pd.notna(row[col]) and row["LTP"] > row[col]:
                    styles.append(
                        "background-color:#d4f8d4; color:#006400; font-weight:bold;"
                    )
                else:
                    styles.append("")
            else:
                styles.append("")
        return styles


    def highlight_low_breaks(row):
        styles = []
        for col in row.index:
            if col in ["YEST_LOW","LOW_W","LOW_M","EMA20","TOP_LOW"]:
                if pd.notna(row[col]) and row["LTP"] < row[col]:
                    styles.append(
                        "background-color:#ffd6d6; color:#8b0000; font-weight:bold;"
                    )
                else:
                    styles.append("")
            else:
                styles.append("")
        return styles


    # =====================================================
    # DISPLAY – STRONG HIGH
    # =====================================================

    st.markdown("### 🟢 Strong Close – Near Day High")

    if strong_high_df.empty:
        st.info("No strong bullish closing stocks.")
    else:

        strong_high_df = strong_high_df[
            [
                "Symbol",
                "LTP",
                "CHANGE",
                "CHANGE_%",
                "LIVE_VOLUME",
                "YEST_VOL",
                "VOL_%",
                "LIVE_HIGH",
                "DIST_FROM_DAY_HIGH_%",
                "YEST_HIGH",
                "HIGH_W",
                "HIGH_M",
                "EMA20",
                "TOP_HIGH"
            ]
        ]

        styled_high = strong_high_df.style.apply(
            highlight_high_breaks,
            axis=1
        ).format({
            "LTP": "{:.2f}",
            "CHANGE_%": "{:.2f}",
            "VOL_%": "{:.2f}",
            "DIST_FROM_DAY_HIGH_%": "{:.2f}",
        })

        st.dataframe(
            styled_high,
            use_container_width=True,
            height=450
        )

    # Spacer
    st.markdown("---")

    # =====================================================
    # DISPLAY – STRONG LOW
    # =====================================================

    st.markdown("### 🔴 Strong Close – Near Day Low")

    if strong_low_df.empty:
        st.info("No strong bearish closing stocks.")
    else:

        strong_low_df = strong_low_df[
            [
                "Symbol",
                "LTP",
                "CHANGE",
                "CHANGE_%",
                "LIVE_VOLUME",
                "YEST_VOL",
                "VOL_%",
                "LIVE_LOW",
                "DIST_FROM_DAY_LOW_%",
                "YEST_LOW",
                "LOW_W",
                "LOW_M",
                "EMA20",
                "TOP_LOW"
            ]
        ]

        styled_low = strong_low_df.style.apply(
            highlight_low_breaks,
            axis=1
        ).format({
            "LTP": "{:.2f}",
            "CHANGE_%": "{:.2f}",
            "VOL_%": "{:.2f}",
            "DIST_FROM_DAY_LOW_%": "{:.2f}",
        })

        st.dataframe(
            styled_low,
            use_container_width=True,
            height=450
        )

    st.subheader("📈 DAILY BREAKS – Above YEST HIGH")
    st.dataframe(daily_up, width="stretch")

    st.subheader("📉 DAILY BREAKS – Below YEST LOW")
    st.dataframe(daily_down, width="stretch")



with tabs[7]:
    st.subheader("🕘 1H Opening Range Breakouts")
    st.dataframe(hourly_break_df, use_container_width=True)

    st.markdown("## 🔥 Gap Failure Reclaim Scanner")

    # Bullish
    if bull_reclaim_df.empty:
        st.info("No bullish reclaim setups")
    else:
        st.markdown("### 🟢 Bullish Reclaim (Gap Up Failure Recovery)")
        st.dataframe(
            bull_reclaim_df[
                ["Symbol","LTP","LIVE_OPEN","YEST_HIGH","LIVE_LOW","STRENGTH_%"]
            ],
            use_container_width=True
        )

    # Bearish
    if bear_reclaim_df.empty:
        st.info("No bearish reclaim setups")
    else:
        st.markdown("### 🔴 Bearish Reclaim (Gap Down Failure Continuation)")
        st.dataframe(
            bear_reclaim_df[
                ["Symbol","LTP","LIVE_OPEN","YEST_LOW","LIVE_HIGH","STRENGTH_%"]
            ],
            use_container_width=True
        )


    st.subheader("📊 WEEKLY BREAKS – Above WEEK HIGH")
    st.dataframe(weekly_up, width="stretch")

    st.subheader("📊 WEEKLY BREAKS – BeLIVE_LOW WEEK LOW")
    st.dataframe(weekly_down, width="stretch")

    #st.divider()

    #st.markdown("### ✅ WEEKLY + EMA CONFIRMATION (Strong Trend)")

    #col1, col2 = st.columns(2)

    #with col1:
     #   st.markdown("🟢 BUY : WEEK HIGH + EMA20 > EMA50")
      #  if weekly_ema_buy.empty:
       #     st.info("No Weekly EMA BUY confirmations")
        #else:
         #   st.dataframe(weekly_ema_buy, width="stretch")

    #with col2:
     #   st.markdown("🔴 SELL : WEEK LOW + EMA20 < EMA50")
      #  if weekly_ema_sell.empty:
       #     st.info("No Weekly EMA SELL confirmations")
        #else:
         #   st.dataframe(weekly_ema_sell, width="stretch")


with tabs[8]:
    st.markdown("## 🔄 EMA Reversal After Downtrend and Uptrend")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12 = st.tabs([
        "1M Downtrend",
        "2M Downtrend",
        "3M Downtrend",
        "6M Downtrend",
        "1M Downtrend1",
        "2M Downtrend2",
        "3M Downtrend3",
        "6M Downtrend4",
        "1M Uptrend",
        "2M Uptrend",
        "3M Uptrend",
        "6M Uptrend"
    ])

    with tab1:
        st.dataframe(down_1m_df, use_container_width=True)

    with tab2:
        st.dataframe(down_2m_df, use_container_width=True)

    with tab3:
        st.dataframe(down_3m_df, use_container_width=True)

    with tab4:
        st.dataframe(down_6m_df, use_container_width=True)

    with tab5:
        st.dataframe(down_1m_df1, use_container_width=True)

    with tab6:
        st.dataframe(down_2m_df2, use_container_width=True)

    with tab7:
        st.dataframe(down_3m_df3, use_container_width=True)

    with tab8:
        st.dataframe(down_6m_df4, use_container_width=True)

    with tab9:
        st.dataframe(up_1m_df1, use_container_width=True)

    with tab10:
        st.dataframe(up_2m_df2, use_container_width=True)

    with tab11:
        st.dataframe(up_3m_df3, use_container_width=True)

    with tab12:
        st.dataframe(up_6m_df4, use_container_width=True)


    st.subheader("📈 2M Downtrend → EMA Reversal Setup")

    if reversal_df.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(reversal_df, use_container_width=True)

    #st.dataframe(df[df.LTP >= df.HIGH_M])
    #st.dataframe(df[df.LTP <= df.LOW_M])
    st.subheader("📅 WEEKLY %")
    st.dataframe(weekly_break_df, width="stretch")

    st.subheader("📅 MONTHLY %")
    st.dataframe(monthly_break_df, width="stretch")

    st.subheader("📅 MONTHLY BREAKS – Above MONTH HIGH")
    monthly_up = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_up, width="stretch")

    st.subheader("📅 MONTHLY BREAKS – Below MONTH LOW")
    monthly_down = df.loc[df.LTP <= df.LOW_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_down, width="stretch")


with tabs[9]:
    
    st.subheader("🟢 Yesterday GREEN – Open Inside Upper Zone")
    if green_zone_df1.empty:
        st.info("No green-zone setups today")
    else:
        st.dataframe(green_zone_df1, use_container_width=True)

    st.markdown("---")

    st.subheader("🟢 Yesterday GREEN – Open Inside BREAKOUT")
    if green_zone_df.empty:
        st.info("No green-zone setups today")
    else:
        st.dataframe(green_zone_df, use_container_width=True)

    st.markdown("---")


    st.subheader("🔴 Yesterday RED – Open Inside Lower Zone")
    if red_zone_df.empty:
        st.info("No red-zone setups today")
    else:
        st.dataframe(red_zone_df, use_container_width=True)

    #st.markdown('<div class="section-yelLIVE_LOW"><b>⚡ LIVE_OPEN = LIVE_HIGH / LIVE_LOW (Trend Day)</b></div>', unsafe_allow_html=True )
    st.subheader("🔥 O=H / O=L Setups (Gainers + Losers)")

    st.dataframe(
        ol_oh_df,
        #width='content'
        width="stretch",
        height=min(3200, 60 + len(ol_oh_df) * 35)
    )
    col1, col2 = st.columns(2)

    # -------- LIVE_OPEN = LIVE_LOW (Bullish) --------
    with col1:
        st.markdown("### 🟢 OPEN==LOW ")
        if LIVE_OPEN_LIVE_LOW_df.empty:
            st.info("No OPEN==LOW stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_LOW_df,
                width="stretch",
                height=min(3000, 60 + len(LIVE_OPEN_LIVE_LOW_df) * 35)
            )

    # -------- LIVE_OPEN = LIVE_HIGH (Bearish) --------
    with col2:
        st.markdown("### 🔴 OPEN==HIGH ")
        if LIVE_OPEN_LIVE_HIGH_df.empty:
            st.info("No OPEN==HIGH stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_HIGH_df,
                width="stretch",
                height=min(3000, 60 + len(LIVE_OPEN_LIVE_HIGH_df) * 35)
            )


#with tabs[8]:
 #   st.markdown('<div class="section-purple"><b>📉 EMA20–EMA50 + Breakout</b></div>', unsafe_allow_html=True)

  #  if ema_signal_df.empty:
   #     st.info("No EMA20–EMA50 signals currently")
   # else:
    #    st.dataframe(
     #       ema_signal_df,
      #      width="stretch",
       #     height=min(1200, 60 + len(ema_signal_df) * 35)
       # )

with tabs[10]:
    st.markdown(
        '<div class="section-green"><b>🟢 EMA20–EMA50 BUY (Breakout)</b></div>',
        unsafe_allow_html=True
    )

    if ema_buy_df.empty:
        st.info("No EMA20–EMA50 BUY signals")
    else:
        st.dataframe(
            ema_buy_df,
            width="stretch",
            height=min(2000, 60 + len(ema_buy_df) * 35)
        )

    st.markdown(
        '<div class="section-red"><b>🔴 EMA20–EMA50 SELL (Breakdown)</b></div>',
        unsafe_allow_html=True
    )

    if ema_sell_df.empty:
        st.info("No EMA20–EMA50 SELL signals")
    else:
        st.dataframe(
            ema_sell_df,
            width="stretch",
            height=min(2000, 60 + len(ema_sell_df) * 35)
        )

with tabs[11]:  # assuming INFO is last tab

    #st.markdown("### 🟢 Top Gainers ( > +2.5% )")

    #if gainers_df.empty:
     #   st.info("No gainers above 2.5%")
    #else:
     #   st.dataframe(gainers_df, use_container_width=True)

    #st.markdown("---")  # separator line

    #st.markdown("### 🔴 Top Losers ( < -2.5% )")

    #if losers_df.empty:
     #   st.info("No losers below -2.5%")
    #else:
     #   st.dataframe(losers_df, use_container_width=True)
    TOP_GAINER_COLS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "VOL_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "LIVE_VOLUME",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "NEAR",
    "GAIN",
    "TOP_HIGH",
    "TOP_LOW",

    ]
    TOP_GAINER_COLS = [c for c in TOP_GAINER_COLS if c in gainers_df.columns]
    with st.expander("🟢 Top Gainers ( > +2.5% )", expanded=True):
        #st.dataframe(gainers_df, use_container_width=True)
        st.dataframe(
            gainers_df[TOP_GAINER_COLS],
            #use_container_width=True
            width="stretch",
            height=min(2200, 60 + len(gainers_df) * 35)
        )

    TOP_LOSER_COLS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "VOL_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "LIVE_VOLUME",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "NEAR",
    "GAIN",
    "TOP_HIGH",
    "TOP_LOW",

    ]
    TOP_LOSER_COLS = [c for c in TOP_LOSER_COLS if c in losers_df.columns]
    with st.expander("🔴 Top Losers ( < -2.5% )", expanded=True):
        #st.dataframe(losers_df, use_container_width=True)
        st.dataframe(
            losers_df[TOP_LOSER_COLS],
            #use_container_width=True
            width="stretch",
            height=min(2200, 60 + len(losers_df) * 35)
        )


with tabs[12]:  # 4 BAR
    st.markdown("### 🔁 4 BAR Reversal + Breakout")

    if four_bar_df.empty:
        st.info("No 4-bar setups today")
    else:
        st.dataframe(four_bar_df, width="stretch")

    ###############################
    st.markdown("### ⚠️ Fake 2.5% Breakouts")

    col1, col2 = st.columns(2)

    # -------- BULL TRAP --------
    with col1:
        st.markdown("#### 🟡 Fake Bull Trap")
        if fake_bull_df.empty:
            st.info("No Fake Bull Traps")
        else:
            st.dataframe(
                fake_bull_df
                .sort_values("FAIL_%", ascending=False)
                .style.apply(style_ltp_only, axis=1),
                width="stretch",
                height=min(2000, 60 + len(fake_bull_df) * 35)
        )


    # -------- BEAR TRAP --------
    with col2:
        st.markdown("#### 🔵 Fake Bear Trap")
        if fake_bear_df.empty:
            st.info("No Fake Bear Traps")
        else:
            st.dataframe(
                fake_bear_df
                .sort_values("FAIL_%", ascending=False)
                .style.apply(style_ltp_bear_only, axis=1),
                width="stretch",
                height=min(2000, 60 + len(fake_bear_df) * 35)
        )

    ##############################################
    st.markdown("### ⏱️ 15-Min Inside Range Break")

    if inside_15m_df.empty:
        st.info("No 15-min inside range breaks yet")
    else:
        st.dataframe(
            inside_15m_df
                .sort_values("CHANGE_%", ascending=False)
                .style.apply(style_ltp_15min, axis=1),
            width="stretch",
            height=min(2000, 60 + len(inside_15m_df) * 35)
        )

    #######################################################
    st.markdown(
        '<div class="section-blue"><b>🚀 YH1.5 Strong Breakout (Screener)</b></div>',
        unsafe_allow_html=True
    )

    if yh15_df.empty:
        st.info("No valid YH1.5 breakouts (gap-ups filtered)")
    else:
        st.dataframe(
            yh15_df,
            width='content'
        )
    ##################################################################
    st.subheader("⚠️ Fake / Failed YH1.5 Breakouts")

    if fake_yh15_df.empty:
        st.info("No failed YH1.5 breakouts currently")
    else:
        st.dataframe(fake_yh15_df, width='content')




with tabs[13]:

    
    st.subheader("🚀 EARLY HIGH-GAIN RUNNERS (YH MOMENTUM)")

    if early_runner_df.empty:
        st.info("No strong early runners yet")
    else:
        st.dataframe(
            early_runner_df,
            use_container_width=True
    )
    
    st.markdown("## 🔥 Full Futures OI Heatmap")
    #filtered_df = fut_df[(fut_df["REAL_OI_%"].abs() > 0.8) & (fut_df["PRICE_%"].abs() > 0.8)]

    if fut_df.empty:
    #if filtered_df.empty:
        st.info("No F&O data available")
    else:

        display_cols = [
            "FUT_SYMBOL",
            "LTP",
            "PRICE_%",
            "REAL_OI_%",
            "OI_SCORE",
            "POSITION_TYPE"
        ]

        #st.dataframe(fut_df[display_cols].sort_values("OI_SCORE", ascending=False),use_container_width=True)
        # Remove NEUTRAL positions
        filtered_fut_df = fut_df[
            fut_df["POSITION_TYPE"] != "NEUTRAL"
        ].copy()

        # Optional: also remove weak OI_SCORE
        filtered_fut_df = filtered_fut_df[
            filtered_fut_df["OI_SCORE"].abs() > 1
        ]

        st.dataframe(
            filtered_fut_df.sort_values("OI_SCORE", ascending=False),
            width="stretch"
        )


    st.markdown("## 🔥 Strong Futures Closing")

    if strong_long_close.empty and strong_short_close.empty:
        st.info("No strong closing futures detected")

    if not strong_long_close.empty:
        st.markdown("### 🟢 Strong Long Closing (Near Day High)")
        st.dataframe(
            strong_long_close[
                ["FUT_SYMBOL","LTP","PRICE_%","REAL_OI_%","DIST_FROM_HIGH_%"]
            ],
            use_container_width=True
        )

    if not strong_short_close.empty:
        st.markdown("### 🔴 Strong Short Closing (Near Day Low)")
        st.dataframe(
            strong_short_close[
                ["FUT_SYMBOL","LTP","PRICE_%","REAL_OI_%","DIST_FROM_LOW_%"]
            ],
            use_container_width=True
        )



with tabs[14]:
    st.subheader("📊 NSE INDICES – LIVE")

    st.dataframe(
        indices_df[
            [
                "Symbol",
                "LTP",
                "OPEN",
                "HIGH",
                "LOW",
                "CHANGE",
                "CHANGE_%",
                "YEST_HIGH",
                "YEST_LOW",
                "YEST_CLOSE",
            ]
        ],
         width='content'
    )

    st.subheader("📊 Index-wise Top Gainers (Live)")

    for index_name, symbols in index_symbols.items():

        idx_df = df[df["Symbol"].isin(symbols)].copy()

        if idx_df.empty:
            continue

        idx_df = idx_df[
            [
                "Symbol",
                "LTP",
                "LIVE_OPEN",
                "LIVE_HIGH",
                "LIVE_LOW",
                "CHANGE",
                "CHANGE_%",
                "YEST_HIGH",
                "YEST_LOW",
                "YEST_CLOSE",
            ]
        ].sort_values("CHANGE_%", ascending=False)

        st.markdown(f"### 🔹 {index_name}")

        st.dataframe(
            idx_df,
            width='content'
        )

with tabs[15]:
    st.markdown("## 🔥 Confirmed Continuation Breaks")

    if continuation_df.empty:
        st.info("No confirmed continuation setups currently")
    else:
        st.dataframe(
            continuation_df.sort_values("DIST_%", ascending=False),
            use_container_width=True
        )


    st.markdown("## 🏗 Support / Resistance – Strong Levels")

    sr_cols = [
        "Symbol",
        "LTP",
        "STRONG_SUPPORT",
        "SS_DIST_%",
        "STRONG_RESISTANCE",
        "SR_DIST_%"
    ]

    if all(col in df.columns for col in sr_cols):
        sr_view = df[sr_cols].copy()
        sr_view = sr_view.sort_values("SR_DIST_%", na_position="last")
        st.dataframe(sr_view, use_container_width=True)
    else:
        st.info("S/R levels not available yet.")

    st.markdown("## 🚀 Breakout Strength Scanner")

    if "BREAK_SCORE" in df.columns:
        break_view = df[
            df["BREAK_SCORE"] > 0
        ][[
            "Symbol",
            "LTP",
            "CHANGE_%",
            "BREAK_SCORE",
            "BREAK_DETAILS",
            "HIGH_D","HIGH_W","HIGH_M",
            "LOW_D","LOW_W","LOW_M"
        ]].sort_values("BREAK_SCORE", ascending=False)

        st.dataframe(break_view, use_container_width=True)
    else:
        st.info("Breakout strength not calculated.")

    st.subheader("🟢 3 × 15-Min Green Candles (Still Valid)")

    if three_green_15m_df.empty:
        st.info("No symbols currently maintaining 3 consecutive green 15-min candles")
    else:
        st.dataframe(
            three_green_15m_df.sort_values("CANDLE_TIME", ascending=False),
            use_container_width=True
        )


with tabs[16]:
    st.subheader("🚨 Alerts Log (Static)")

    if not os.path.exists(ALERTS_LOG_FILE):
        st.info("No alerts logged yet.")
    else:
        #alerts_df = pd.read_csv(ALERTS_LOG_FILE)
        alerts_df = pd.read_csv(ALERTS_LOG_FILE)
        alerts_df = alerts_df.sort_values(
            by=["DATE", "TIME"],
            ascending=False
        )

        st.dataframe(
            alerts_df,
            width="stretch",
            height=min(4200, 60 + len(alerts_df) * 32)
        )

        st.caption("📌 Latest alerts appear at the top. Data is static and will not change.")


with tabs[17]:
    st.header("🪐 ASTRO HEATMAP")

    astro = get_astro_score()
    time_signal = get_time_signal()

    col1, col2, col3 = st.columns(3)

    col1.metric("Signal", astro["signal"])
    col2.metric("Score", astro["score"])
    col3.metric("Time Zone", time_signal)

    st.write("Moon Sign:", astro["moon_sign"])
    st.write("Nakshatra:", astro["nakshatra"])
    st.write("Reason:", astro["reason"])

    st.subheader("🔮 Next 3 Days Astro Forecast")

    #d1 = get_future_astro(1)
    #d2 = get_future_astro(2)
    #d3 = get_future_astro(3)
    from datetime import datetime, timedelta
    import pytz

    IST = pytz.timezone("Asia/Kolkata")

    future_days = []
    days_added = 0
    i = 1

    while days_added < 10:
        dt = datetime.now(IST) + timedelta(days=i)

        # Skip weekends + NSE holidays
        if dt.weekday() < 5 and dt.date() not in NSE_HOLIDAYS:
            future_days.append(get_future_astro(i))
            days_added += 1

        i += 1

    future_df = pd.DataFrame(future_days)

    #st.dataframe(future_df, use_container_width=True)

    #future_df = pd.DataFrame([d1, d2, d3])
    def color_signal(val):
        if "❌" in val:
            return "background-color: #f8d7da"   # soft red ###return "background-color: #ffcccc"   # light red
        elif "🔥" in val:
            return "background-color: #28a745; color: white;"   # rich green  ####return "background-color: #00ff00"   # ✅ BRIGHT GREEN
        elif "🟢" in val:
            return "background-color: #d4edda"   # light green ########return "background-color: #ccffcc"   # normal green
        elif "⚠️" in val:
            return "background-color: #fff3cd"   # yellow
        else:
            return ""

    styled_df = future_df.style.applymap(color_signal, subset=["signal"])

    st.dataframe(styled_df, use_container_width=True)

    #st.dataframe(future_df, use_container_width=True)
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
        mime="application/vnd.LIVE_OPENxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.write(" I . when it is TOP LIVE_HIGH breaks or NEAR LIVE_HIGH value - just check the Entity is DAILY and WEEKLY UPTREND AND SUPER TREND SHOULD IN BUY MODE in DAILY TREND- THEN ONLY ENTER - same as SELL ViceVersa")
    st.write(" II. when you take position - always decide your SL and immediatly put StopLoss - IF STOP LOSS HITS - Dont touch for that day or reenter : we will get multiple Chances in coming days and we have lot of entities")
    st.write(" III. whenever Entity breaks TOP LIVE_HIGH (in buy entry) and returns and SL hits - There is a possibility to REVERSE and you will get sell side opportunity- some wiered cases both sides SL hits. dont touch that time")
    st.write(" IV. Take only one or two lots and keep some money with you, other wise when sudden dips you will have a chance to average in worst cases, other wise losses will be huge, if we average at least  can exit with minimal losses")
    st.write(" V. when prices below of panchak low and not moved punchak up side, dont carry longs until it comes above TOP_HIGH , same for sell side as well")
    st.write(" VI. take stock positions based on INDICES TOP_HIGH and TOP_LOW as NIFTY controls most movements. ")
    st.subheader("📌 MOMENTUM SCORE – COLUMN MEANING")

    st.write(pd.DataFrame({
        "Column": [
            "MOMO_SCORE",
            "YH_RANGE_EXP_%",
            "YH_ACCEPTED",
            "OPEN_DIST_YH_%",
            "CHANGE_%"
        ],
        "Meaning": [
            "🔥 Higher score = stronger chance of 2%–5% run",
            "Fast expansion above YH = real momentum",
            "Holding above Yesterday High (no pullback)",
            "Open near Yesterday High = clean structure",
            "Actual intraday momentum"
        ]
    }))

