import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
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


REFRESH_SECONDS = 100  # 5 minutes

SYMBOLS = [
    "RELIANCE", "INFY", "TCS", "HDFCBANK", "ICICIBANK",
    "BHARTIARTL", "ITC", "LT", "AXISBANK", "SBIN"
]

EXCHANGE = "NSE"



# ===============================
# AUTO REFRESH
# ===============================
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > REFRESH_SECONDS:
    st.session_state.last_refresh = time.time()
    st.rerun()

# ===============================
# HEADER
# ===============================
st.set_page_config(layout="wide")
st.title("📊 LIVE BREAKOUT DASHBOARD – ZERODHA")
st.caption("Live LTP | Yesterday OHLC | Auto refresh every 5 minutes")

st.info(f"🕒 Last refresh: {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")

# ===============================
# FETCH DATA
# ===============================
rows_high = []
rows_low = []

for sym in SYMBOLS:
    try:
        token = kite.ltp(f"{EXCHANGE}:{sym}")[f"{EXCHANGE}:{sym}"]["instrument_token"]

        # Yesterday OHLC
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=5)

        hist = kite.historical_data(
            token,
            from_date,
            to_date,
            interval="day"
        )

        y = hist[-2]  # yesterday candle

        y_high = y["high"]
        y_low = y["low"]

        # Live data
        quote = kite.quote(f"{EXCHANGE}:{sym}")[f"{EXCHANGE}:{sym}"]
        ltp = quote["last_price"]
        today_high = quote["ohlc"]["high"]
        today_low = quote["ohlc"]["low"]

        # Breakout logic
        if ltp > y_high:
            rows_high.append({
                "Symbol": sym,
                "LTP": ltp,
                "Y_High": y_high,
                "Today_High": today_high,
                "Break_Time": datetime.now().strftime("%H:%M:%S")
            })

        if ltp < y_low:
            rows_low.append({
                "Symbol": sym,
                "LTP": ltp,
                "Y_Low": y_low,
                "Today_Low": today_low,
                "Break_Time": datetime.now().strftime("%H:%M:%S")
            })

    except Exception as e:
        pass

df_high = pd.DataFrame(rows_high)
df_low = pd.DataFrame(rows_low)

# ===============================
# TABS
# ===============================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🔼 Y-High Break",
    "🔽 Y-Low Break",
    "🧭 Panchak",
    "🧠 AIO",
    "ℹ INFO"
])

# -------------------------------
with tab1:
    st.subheader("🔼 Yesterday HIGH Breakouts")
    st.dataframe(df_high, use_container_width=True)

# -------------------------------
with tab2:
    st.subheader("🔽 Yesterday LOW Breakdowns")
    st.dataframe(df_low, use_container_width=True)

# -------------------------------
with tab3:
    st.info("DEC Panchak logic will be merged here (your existing engine)")

# -------------------------------
with tab4:
    st.info("AIO = Breakout + OI + Strength (next phase)")

# -------------------------------
with tab5:
    st.metric("Symbols monitored", len(SYMBOLS))
    st.metric("High breakouts", len(df_high))
    st.metric("Low breakdowns", len(df_low))

time.sleep(REFRESH_SECONDS)
st.rerun()
