import streamlit as st
import pandas as pd
import os
from datetime import datetime
#from streamlit_extras.st_autorefresh import st_autorefresh
import time


# ===============================
# CONFIG
# ===============================
DATA_FOLDER = "ZERODHA_OPTION_DATA"
REFRESH_SECONDS = 60
MIN_STRENGTH = 60

# ===============================
# PAGE SETUP
# ===============================
st.set_page_config(
    page_title="Options Strength Dashboard",
    layout="wide",
)

st.title("📊 OPTIONS STRENGTH DASHBOARD")
st.caption("Price + OI based | Auto refreshed every 60 seconds")

# ✅ PROPER AUTO REFRESH
#st_autorefresh(interval=REFRESH_SECONDS * 1000, key="refresh")

# ===============================
# LOAD LATEST EXCEL
# ===============================
def get_latest_excel(folder):
    files = [f for f in os.listdir(folder) if f.endswith(".xlsx")]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join(folder, files[0])

latest_file = get_latest_excel(DATA_FOLDER)

if not latest_file:
    st.error("❌ No Excel files found")
    st.stop()

try:
    df = pd.read_excel(latest_file)
except PermissionError:
    st.error("❌ Excel file is open. Close it and refresh.")
    st.stop()

st.success(f"📂 Loaded: {os.path.basename(latest_file)}")

# ===============================
# LAST FETCHED TIME
# ===============================
file_time = datetime.fromtimestamp(os.path.getmtime(latest_file))
st.info(f"🕒 Last data fetched at: {file_time.strftime('%d-%b-%Y %H:%M:%S')}")

# ===============================
# CLEAN COLUMNS
# ===============================
df.columns = [c.strip() for c in df.columns]

# ===============================
# COMBINED STRENGTH
# ===============================
if {"Strength_15m", "Strength_30m", "Strength_60m"}.issubset(df.columns):
    df["Combined Strength"] = (
        0.3 * df["Strength_15m"] +
        0.3 * df["Strength_30m"] +
        0.4 * df["Strength_60m"]
    ).round(0)
else:
    df["Combined Strength"] = df["Strength"]

# ===============================
# 🔥 FIXED SIGNAL LOGIC
# ===============================
def generate_signal(row):
    if row["Spot %"] > 0 and row["CE OI Δ"] > 0 and row["CE/PE OI Ratio"] > 1:
        return "🟢 BUY CE"
    elif row["Spot %"] < 0 and row["PE OI Δ"] > 0 and row["CE/PE OI Ratio"] < 1:
        return "🔴 BUY PE"
    else:
        return "⚠ IGNORE"

df["Signal"] = df.apply(generate_signal, axis=1)

# ===============================
# BIAS
# ===============================
df["Bias"] = df["Signal"].apply(
    lambda x: "Bullish" if "CE" in x else "Bearish" if "PE" in x else "Neutral"
)

# ===============================
# FILTER VALID SETUPS
# ===============================
df = df[
    (df["Combined Strength"] >= MIN_STRENGTH) &
    (df["Signal"] != "⚠ IGNORE")
]

df = df.sort_values("Combined Strength", ascending=False)

# ===============================
# TOP PICKS
# ===============================
df["Top Pick"] = ""
df.iloc[:3, df.columns.get_loc("Top Pick")] = "🔥 TOP 3"

# ===============================
# STRIKE TYPE
# ===============================
def strike_type(sig):
    if "CE" in sig:
        return "ATM / ITM CE"
    if "PE" in sig:
        return "ATM / ITM PE"
    return "-"

df["Strike Type"] = df["Signal"].apply(strike_type)

# ===============================
# DISPLAY
# ===============================
st.subheader("🔥 TOP OPTION SETUPS")

display_cols = [
    "Symbol",
    "Bias",
    "Signal",
    "Combined Strength",
    "Strike Type",
    "CE/PE OI Ratio",
    "Entry Price",
    "Top Pick"
]

st.dataframe(
    df[display_cols],
    use_container_width=True,
    hide_index=True
)

st.caption("🔁 Strategy: Price + OI confirmation | Intraday / Positional")

# ===============================
# AUTO REFRESH (Native & Stable)
# ===============================
REFRESH_SECONDS = 60
time.sleep(REFRESH_SECONDS)
st.rerun()

