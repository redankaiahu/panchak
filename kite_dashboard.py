import streamlit as st
import pandas as pd
import os
from datetime import datetime

# ===============================
# CONFIG
# ===============================
DATA_DIR = "ZERODHA_OPTION_DATA"
REFRESH_SEC = 60

st.set_page_config(
    page_title="Options Strength Dashboard",
    layout="wide"
)

st.title("📊 OPTIONS STRENGTH DASHBOARD")
st.caption("Price + OI based | Auto refreshed")

# ===============================
# AUTO REFRESH
# ===============================
st.experimental_set_query_params(refresh=str(datetime.now()))
st.markdown(
    f"<meta http-equiv='refresh' content='{REFRESH_SEC}'>",
    unsafe_allow_html=True
)

# ===============================
# LOAD LATEST EXCEL
# ===============================
files = [f for f in os.listdir(DATA_DIR) if f.endswith(".xlsx")]

if not files:
    st.error("❌ No Excel files found")
    st.stop()

latest_file = sorted(files)[-1]
file_path = os.path.join(DATA_DIR, latest_file)

df = pd.read_excel(file_path)

# ===============================
# SPLIT WINDOWS
# ===============================
df_15 = df[df["Window"] == "15m"]
df_30 = df[df["Window"] == "30m"]
df_60 = df[df["Window"] == "60m"]

# ===============================
# MERGE & COMBINE STRENGTH
# ===============================
combined = df_60.merge(
    df_30[["Symbol", "Strength"]],
    on="Symbol",
    how="left",
    suffixes=("_60", "_30")
).merge(
    df_15[["Symbol", "Strength"]],
    on="Symbol",
    how="left"
)

combined.rename(columns={"Strength": "Strength_15"}, inplace=True)

combined["Combined Strength"] = (
    combined["Strength_15"] * 0.4 +
    combined["Strength_30"] * 0.35 +
    combined["Strength_60"] * 0.25
).round(1)

# ===============================
# FINAL SIGNAL FILTER
# ===============================
final_df = combined[
    (combined["Combined Strength"] >= 60)
]

final_df = final_df.sort_values("Combined Strength", ascending=False)

# ===============================
# ENTRY STRIKE LOGIC
# ===============================
def strike_logic(row):
    if row["Combined Strength"] >= 75:
        return "ATM"
    elif row["Combined Strength"] >= 65:
        return "1 ITM"
    else:
        return "WAIT"

final_df["Strike Type"] = final_df.apply(strike_logic, axis=1)

# ===============================
# DISPLAY
# ===============================
st.subheader("🔥 TOP OPTION SETUPS")

st.dataframe(
    final_df[[
        "Symbol",
        "Bias",
        "Signal",
        "Combined Strength",
        "Strike Type",
        "CE/PE OI Ratio",
        "Entry Price"
    ]],
    use_container_width=True,
    height=600
)

# ===============================
# TOP 3 PICKS
# ===============================
st.subheader("🏆 TOP 3 PICKS")

top3 = final_df.head(3)

for _, row in top3.iterrows():
    st.success(
        f"🔥 {row['Symbol']} | {row['Signal']} | "
        f"Strength {row['Combined Strength']} | {row['Strike Type']}"
    )

st.caption(f"📅 Last Updated: {latest_file}")
