# =============================================================
# PATCH FOR kp_panchang_tab.py
# Add this block AFTER you compute TOP HIGH and LEAST LOW for
# the current KP slot (inside the render_kp_tab function or
# wherever you calculate slot_top_high / slot_least_low).
#
# This writes slot data to session_state so that the main
# dashboard file can read it for Telegram break alerts.
# =============================================================
#
# STEP 1 — Find where you compute slot TOP HIGH / LEAST LOW.
#           It will look something like:
#
#   nifty_top_high   = df_nifty["High"].max()     ← your variable name
#   nifty_least_low  = df_nifty["Low"].min()      ← your variable name
#   slot_label       = "09:45–10:00"              ← your slot label string
#
# STEP 2 — Immediately after those lines, add:

import streamlit as st

# ── Push NIFTY slot OHLC to session_state for break alerts ──
if "kp_slot_ohlc" not in st.session_state:
    st.session_state["kp_slot_ohlc"] = {}

# Replace the right-hand side values with YOUR variable names:
st.session_state["kp_slot_ohlc"]["NIFTY"] = {
    "top_high":  nifty_top_high,     # ← replace with your variable
    "least_low": nifty_least_low,    # ← replace with your variable
    "slot":      slot_label,         # ← replace with your slot label string
}
st.session_state["kp_slot_ohlc"]["BANKNIFTY"] = {
    "top_high":  banknifty_top_high,  # ← replace with your variable
    "least_low": banknifty_least_low, # ← replace with your variable
    "slot":      slot_label,          # ← replace with your slot label string
}

# =============================================================
# WHAT THIS DOES:
#   The main dashboard reads st.session_state["kp_slot_ohlc"]
#   in fire_nifty_slot_break_alert() every refresh cycle.
#   It compares NIFTY / BANKNIFTY LTP to these values and:
#     • Sends an INSTANT Telegram alert when LTP breaks TOP HIGH
#       or LEAST LOW (once per slot, resets each new slot)
#     • Sends a PROGRESS Telegram alert every 15 min showing
#       how many points gained/lost since the break
#
# NOTE: Until you add this patch, the main file automatically
#   falls back to df["TOP_HIGH"] / df["TOP_LOW"] for NIFTY and
#   BANKNIFTY — so alerts WILL work even without this patch,
#   just with Panchak TOP_HIGH/TOP_LOW instead of KP slot levels.
# =============================================================
