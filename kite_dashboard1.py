import streamlit as st
import pandas as pd
import os
from datetime import datetime
import time

# ===============================
# CONFIG
# ===============================
DATA_FOLDER = "ZERODHA_OPTION_DATA"
REFRESH_SECONDS = 60
MIN_STRENGTH = 60

# ===============================
# STREAMLIT PAGE SETUP
# ===============================
st.set_page_config(
    page_title="Options Strength Dashboard",
    layout="wide",
)

st.title("📊 OPTIONS STRENGTH DASHBOARD")
st.caption("Price + OI based | Auto refreshed every 60 seconds")

# ===============================
# AUTO REFRESH
# ===============================
time.sleep(REFRESH_SECONDS)
st.query_params["refresh"] = datetime.now().strftime("%H:%M:%S")

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
    st.error("❌ No Excel file found in ZERODHA_OPTION_DATA folder")
    st.stop()

try:
    df = pd.read_excel(latest_file)
except PermissionError:
    st.error("❌ Excel file is open. Please close it and refresh.")
    st.stop()

st.success(f"📂 Loaded: {os.path.basename(latest_file)}")

# ===============================
# LAST DATA FETCHED TIME
# ===============================
file_time = datetime.fromtimestamp(os.path.getmtime(latest_file))
st.info(f"🕒 Last data fetched at: {file_time.strftime('%d-%b-%Y %H:%M:%S')}")


# ===============================
# NORMALIZE COLUMNS
# ===============================
df.columns = [c.strip() for c in df.columns]

# Temporary mapping (until 15m/30m/60m added)
if "Combined Strength" not in df.columns:
    df["Combined Strength"] = df["Strength"]

# ===============================
# FILTER LOGIC
# ===============================
df = df[df["Combined Strength"] >= MIN_STRENGTH]
df = df.sort_values("Combined Strength", ascending=False)

# ===============================
# TOP PICKS TAG
# ===============================
df["Top Pick"] = ""
df.iloc[:3, df.columns.get_loc("Top Pick")] = "🔥 TOP 3"

# ===============================
# STRIKE TYPE LOGIC (BASIC)
# ===============================
def strike_type(signal):
    if "CE" in signal:
        return "ATM / ITM CE"
    if "PE" in signal:
        return "ATM / ITM PE"
    return "-"

df["Strike Type"] = df["Signal"].apply(strike_type)

# ===============================
# DISPLAY DASHBOARD
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

# ===============================
# FOOTER
# ===============================
st.caption("🔁 Auto refresh enabled | Strategy: Price + OI confirmation")
