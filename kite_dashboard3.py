import streamlit as st
import pandas as pd
import os
from datetime import datetime
import time

# ===============================
# CONFIG
# ===============================
BASE_DIR = "ZERODHA_OPTION_DATA"
REFRESH_SECONDS = 60
MIN_STRENGTH = 60

# ===============================
# PAGE SETUP
# ===============================
st.set_page_config(
    page_title="Options Strength Dashboard",
    layout="wide"
)

st.title("📊 OPTIONS STRENGTH DASHBOARD")
st.caption("Price + OI derived | Rolling 15m / 30m / 60m | Auto refresh 60s")

# ===============================
# LOAD TODAY DASHBOARD
# ===============================
today = datetime.now().strftime("%Y-%m-%d")
folder = os.path.join(BASE_DIR, today)

if not os.path.exists(folder):
    st.error("❌ Today data folder not found")
    st.stop()

dash_files = [
    f for f in os.listdir(folder)
    if f.startswith("DASHBOARD_") and f.endswith("_FINAL.xlsx")
]

if not dash_files:
    st.error("❌ Final dashboard not generated yet")
    st.stop()

dash_path = os.path.join(folder, dash_files[0])

try:
    df = pd.read_excel(dash_path)
except PermissionError:
    st.error("❌ Dashboard Excel is open. Close it and refresh.")
    st.stop()

# ===============================
# META INFO
# ===============================
file_time = datetime.fromtimestamp(os.path.getmtime(dash_path))
st.info(f"🕒 Last updated: {file_time.strftime('%d-%b-%Y %H:%M:%S')}")

# ===============================
# SIGNAL LOGIC (FIXED)
# ===============================
def signal_logic(row):
    # Strong bullish
    if (
        row["Combined_Strength"] >= 70 and
        row["Spot"] > row["Y_Close"] and
        row["Spot"] >= row["Today_High"]
    ):
        return "🟢 BUY CE"

    # Strong bearish
    if (
        row["Combined_Strength"] <= 30 and
        row["Spot"] < row["Y_Close"] and
        row["Spot"] <= row["Today_Low"]
    ):
        return "🔴 BUY PE"

    return "⚠ IGNORE"

df["Signal"] = df.apply(signal_logic, axis=1)

# ===============================
# FILTER HIGH QUALITY SETUPS
# ===============================
df = df[df["Combined_Strength"] >= MIN_STRENGTH]
df = df[df["Signal"] != "⚠ IGNORE"]

df = df.sort_values("Combined_Strength", ascending=False)

# ===============================
# TOP PICKS
# ===============================
df["Top Pick"] = ""
if len(df) >= 3:
    df.iloc[:3, df.columns.get_loc("Top Pick")] = "🔥 TOP 3"

# ===============================
# DISPLAY
# ===============================
st.subheader("🔥 HIGH-PROBABILITY OPTION SETUPS")

display_cols = [
    "Symbol",
    "Signal",
    "Combined_Strength",
    "Strength_15m",
    "Strength_30m",
    "Strength_60m",
    "Spot",
    "Today_High",
    "Today_Low",
    "Y_Close",
    "Y_High",
    "Y_Low",
    "Top Pick"
]

st.dataframe(
    df[display_cols],
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Logic: Rolling strength + price position vs yesterday & today levels. "
    "OI already embedded in strength."
)

# ===============================
# AUTO REFRESH
# ===============================
time.sleep(REFRESH_SECONDS)
st.rerun()
