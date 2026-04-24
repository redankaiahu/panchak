import streamlit as st
import pandas as pd
import datetime as dt
import time
from kiteconnect import KiteConnect

# ===============================
# CONFIG
# ===============================
def load_access_token():
    with open(r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24\access_token.txt") as f:
        return f.read().strip()

# ===============================
# ZERODHA CREDENTIALS
# ===============================
API_KEY = "7am67kxijfsusk9i"
#ACCESS_TOKEN = "BI70C5DhZwgADwJ6A1YzPpRR2S0qob3a"

# ===============================
# CONNECT
# ===============================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(load_access_token())
print("✅ Zerodha connected")
REFRESH_SECONDS = 60  # 5 minutes

STATIC_DATES = [
    dt.date(2026, 1, 21),
    dt.date(2026, 1, 22),
    dt.date(2026, 1, 23),
    dt.date(2026, 1, 24),
    dt.date(2026, 1, 25),
]

SYMBOLS = [
    "NIFTY","BANKNIFTY","RELIANCE","INFY","TCS","HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK",
    "LT","ITC","HINDUNILVR","BHARTIARTL","ASIANPAINT","MARUTI","SUNPHARMA","TITAN",
    "ULTRACEMCO","ONGC","POWERGRID","NTPC","COALINDIA","TATAMOTORS","TATASTEEL",
    "JSWSTEEL","HINDALCO","ADANIPORTS","ADANIENT","BAJFINANCE","BAJAJFINSV",
    "HDFCLIFE","SBILIFE","ICICIPRULI","DIVISLAB","DRREDDY","CIPLA","APOLLOHOSP",
    "GRASIM","UPL","BPCL","IOC","GAIL","PNB","BANKBARODA","CANBK","IDFCFIRSTB",
    "FEDERALBNK","INDUSINDBK","AUBANK","M&M","HEROMOTOCO","EICHERMOT","TVSMOTOR",
    "BAJAJ-AUTO","ASHOKLEY","AMBUJACEM","ACC","DALBHARAT","RAMCOCEM","SHREECEM",
    "DLF","GODREJPROP","OBEROIRLTY","PRESTIGE","LODHA","TRENT","DMART","NAUKRI",
    "TECHM","WIPRO","LTIM","MPHASIS","COFORGE","PERSISTENT","HCLTECH","OFSS",
    "BEL","HAL","BHEL","IRCTC","IRFC","RVNL","NBCC","RECLTD","PFC","SIEMENS",
    "ABB","CGPOWER","CUMMINSIND","SRF","PIDILITIND","BERGEPAINT","BRITANNIA",
    "COLPAL","DABUR","MARICO","MCDOWELL-N","UBL","INDIGO","SPICEJET","MFSL",
    "CHOLAFIN","MUTHOOTFIN","MANAPPURAM","LTF","ABCAPITAL","TATAPOWER",
    "TORNTPHARM","ALKEM","AUROPHARMA","BIOCON","LUPIN","ZYDUSLIFE","ICICIGI",
    "SBICARD","HDFCAMC","LICHSGFIN","BANDHANBNK","RBLBANK","YESBANK","SAIL",
    "NMDC","VEDL","HAVELLS","DIXON","VOLTAS","CROMPTON","INDHOTEL","CONCOR",
    "CANFINHOME","SUNTV","ZEEL","IDEA","PAYTM","NYKAA","ZOMATO","DELHIVERY"
]


instruments = pd.DataFrame(kite.instruments())

def get_token(symbol):
    if symbol in ["NIFTY", "BANKNIFTY"]:
        return instruments[
            (instruments.tradingsymbol == symbol) &
            (instruments.exchange == "NSE")
        ].iloc[0]["instrument_token"]
    return instruments[
        (instruments.tradingsymbol == symbol) &
        (instruments.exchange == "NSE") &
        (instruments.instrument_type == "EQ")
    ].iloc[0]["instrument_token"]

# ===============================
# DATA FETCHERS
# ===============================
@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_data():
    data = []
    yhigh_break = []
    ylow_break = []

    now = dt.datetime.now().time()

    for sym in SYMBOLS:
        try:
            token = get_token(sym)

            # Yesterday OHLC
            yday = dt.date.today() - dt.timedelta(days=1)
            yohlc = kite.historical_data(
                token,
                yday,
                yday,
                "day"
            )[0]

            # Today OHLC
            today_ohlc = kite.historical_data(
                token,
                dt.date.today(),
                dt.date.today(),
                "day"
            )[0]

            # LTP
            ltp = kite.ltp(f"NSE:{sym}")[f"NSE:{sym}"]["last_price"]

            # Breakout checks
            if ltp >= yohlc["high"]:
                yhigh_break.append({
                    "Symbol": sym,
                    "LTP": ltp,
                    "Y_High": yohlc["high"],
                    "Today_High": today_ohlc["high"],
                    "Break_Time": now
                })

            if ltp <= yohlc["low"]:
                ylow_break.append({
                    "Symbol": sym,
                    "LTP": ltp,
                    "Y_Low": yohlc["low"],
                    "Today_Low": today_ohlc["low"],
                    "Break_Time": now
                })

            # PANCHAK DATA
            hist_rows = []
            for d in STATIC_DATES:
                hd = kite.historical_data(token, d, d, "day")
                if hd:
                    hist_rows.append(hd[0])

            if not hist_rows:
                continue

            dfh = pd.DataFrame(hist_rows)
            top_high = dfh["high"].max()
            top_low = dfh["low"].min()
            diff = top_high - top_low

            bt = top_high + diff
            stp = top_low - diff

            data.append({
                "Symbol": sym,
                "TOP_HIGH": round(top_high, 2),
                "TOP_LOW": round(top_low, 2),
                "DIFF": round(diff, 2),
                "BT": round(bt, 2),
                "ST": round(stp, 2),
                "LTP": round(ltp, 2),
                "Y_HIGH": round(yohlc["high"], 2),
                "Y_LOW": round(yohlc["low"], 2),
                "Today_High": round(today_ohlc["high"], 2),
                "Today_Low": round(today_ohlc["low"], 2)
            })

        except Exception:
            pass

    return (
        pd.DataFrame(yhigh_break),
        pd.DataFrame(ylow_break),
        pd.DataFrame(data)
    )

# ===============================
# UI
# ===============================
st.set_page_config(layout="wide")
st.title("📊 LIVE BREAKOUT DASHBOARD – ZERODHA")
st.caption("Live LTP | Yesterday OHLC | Auto refresh every 5 minutes")

st.info(f"🕒 Last refresh: {dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["🔺 Y-High Break", "🔻 Y-Low Break", "🧿 Panchak", "🧠 AIO", "ℹ INFO"]
)

yhigh_df, ylow_df, panchak_df = fetch_data()

# ===============================
# TAB 1
# ===============================
with tab1:
    st.subheader("Yesterday HIGH Breakouts")
    if not yhigh_df.empty:
        st.dataframe(yhigh_df, use_container_width=True)
    else:
        st.warning("No Y-High breakouts yet")

# ===============================
# TAB 2
# ===============================
with tab2:
    st.subheader("Yesterday LOW Breakdowns")
    if not ylow_df.empty:
        st.dataframe(ylow_df, use_container_width=True)
    else:
        st.warning("No Y-Low breakdowns yet")

# ===============================
# TAB 3 – PANCHAK
# ===============================
with tab3:
    st.subheader("DEC PANCHAK (LIVE)")

    if not panchak_df.empty:
        def target_hit(row):
            if row["LTP"] >= row["BT"]:
                return "🎯 HIGH TARGET HIT"
            elif row["LTP"] <= row["ST"]:
                return "🎯 LOW TARGET HIT"
            return "⏳ WAIT"

        def yhyl(row):
            if row["LTP"] >= row["Y_HIGH"]:
                return "🚀 YH BROKEN"
            elif row["LTP"] <= row["Y_LOW"]:
                return "🔻 YL BROKEN"
            return ""

        panchak_df["TARGET"] = panchak_df.apply(target_hit, axis=1)
        panchak_df["YH/YL"] = panchak_df.apply(yhyl, axis=1)

        st.dataframe(
            panchak_df.sort_values("Symbol"),
            use_container_width=True
        )
    else:
        st.warning("Panchak data not available")

# ===============================
# TAB 4
# ===============================
with tab4:
    st.info("AIO logic will be merged here next")

# ===============================
# TAB 5
# ===============================
with tab5:
    st.markdown("""
    **INFO**
    - Data source: Zerodha Kite Connect
    - Logic: Live + Historical OHLC
    - Refresh: Every 5 minutes
    - Engine: Unified (no Excel dependency)
    """)

# ===============================
# AUTO REFRESH
# ===============================
time.sleep(REFRESH_SECONDS)
#st.experimental_rerun()
st.rerun()

