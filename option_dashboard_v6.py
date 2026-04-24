import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

# ===============================
# CONFIG
# ===============================
BASE_DIR = "ZERODHA_OPTION_DATA"
REFRESH_SECONDS = 300  # 5 minutes
ROLLING_BARS = 4       # last 3–4 snapshots
OUTPUT_NAME = "DASHBOARD_FINAL.xlsx"

REQUIRED_COLS = {
    "Timestamp", "Spot", "Type", "OI",
    "Y_Close", "Y_High", "Y_Low",
    "D_High", "D_Low"
}

# ===============================
# HELPERS
# ===============================
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def today_folder():
    return os.path.join(BASE_DIR, datetime.now().strftime("%Y-%m-%d"))

def strength_from_window(dfw):
    if len(dfw) < 2:
        return np.nan

    # Price score
    price_delta = dfw["Spot"].iloc[-1] - dfw["Spot"].iloc[0]
    price_score = 50 if price_delta > 0 else -50

    # OI score
    ce_oi = dfw[dfw["Type"] == "CE"]["OI"].diff().sum()
    pe_oi = dfw[dfw["Type"] == "PE"]["OI"].diff().sum()

    if ce_oi > 0 and pe_oi <= 0:
        oi_score = 50
    elif pe_oi > 0 and ce_oi <= 0:
        oi_score = -50
    else:
        oi_score = 0

    return max(0, min(100, price_score + oi_score))

# ===============================
# SYMBOL PROCESSOR
# ===============================
def process_symbol(symbol, path):
    try:
        df = pd.read_excel(path)
    except Exception as e:
        log(f"⚠ {symbol} read error: {e}")
        return None

    if not REQUIRED_COLS.issubset(df.columns):
        log(f"⚠ {symbol} skipped: missing columns")
        return None

    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp")

    if len(df) < ROLLING_BARS:
        return None

    df_last = df.tail(ROLLING_BARS)
    last = df_last.iloc[-1]

    s15 = strength_from_window(df_last.tail(3))
    s30 = strength_from_window(df_last.tail(6)) if len(df) >= 6 else np.nan
    s60 = strength_from_window(df_last.tail(12)) if len(df) >= 12 else np.nan

    combined = np.nanmean([
        s15 * 0.3 if not np.isnan(s15) else np.nan,
        s30 * 0.3 if not np.isnan(s30) else np.nan,
        s60 * 0.4 if not np.isnan(s60) else np.nan,
    ])

    return {
        "Symbol": symbol,
        "Spot": round(last["Spot"], 2),
        "Y_Close": last["Y_Close"],
        "Y_High": last["Y_High"],
        "Y_Low": last["Y_Low"],
        "Today_High": last["D_High"],
        "Today_Low": last["D_Low"],
        "Strength_15m": round(s15, 1) if not np.isnan(s15) else np.nan,
        "Strength_30m": round(s30, 1) if not np.isnan(s30) else np.nan,
        "Strength_60m": round(s60, 1) if not np.isnan(s60) else np.nan,
        "Combined_Strength": round(combined, 1) if not np.isnan(combined) else np.nan,
        "Last_Update": last["Timestamp"]
    }

# ===============================
# MAIN LOOP
# ===============================
def run_dashboard():
    log("📊 OPTION DASHBOARD v6 – AUTO REFRESH MODE STARTED")

    while True:
        folder = today_folder()

        if not os.path.exists(folder):
            log("⚠ Today folder not found, waiting...")
            time.sleep(REFRESH_SECONDS)
            continue

        rows = []

        for file in os.listdir(folder):
            if not file.endswith(".xlsx") or file.startswith("DASHBOARD"):
                continue

            symbol = file.replace(".xlsx", "")
            path = os.path.join(folder, file)

            result = process_symbol(symbol, path)
            if result:
                rows.append(result)

        if rows:
            dash = pd.DataFrame(rows)
            dash = dash.sort_values("Combined_Strength", ascending=False)

            out_path = os.path.join(folder, OUTPUT_NAME)
            dash.to_excel(out_path, index=False)

            log(f"✅ Dashboard updated ({len(rows)} stocks)")
        else:
            log("⚠ No valid symbols processed")

        log("⏳ Waiting 5 minutes...\n")
        time.sleep(REFRESH_SECONDS)

# ===============================
# START
# ===============================
if __name__ == "__main__":
    run_dashboard()
