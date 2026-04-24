import os
import time
import pandas as pd
from datetime import datetime

# ===============================
# CONFIG
# ===============================
BASE_DIR = "ZERODHA_OPTION_DATA"
REFRESH_SECONDS = 300          # 5 minutes
ROLLING_BARS = 4               # last 3–4 x 15min logic
OUTPUT_NAME =  f"{DATA_FOLDER}/{TODAY}/DASHBOARD_{TODAY}_FINAL.xlsx"

REQUIRED_COLS = [
    "Timestamp", "Spot", "Y_Close", "Y_High", "Y_Low",
    "D_High", "D_Low", "Strike", "Type", "OI"
]
ROLLING_WINDOWS = {
    "15m": 3,
    "30m": 6,
    "60m": 12
}
# ===============================
# UTILS
# ===============================
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def today_folder():
    return os.path.join(BASE_DIR, datetime.now().strftime("%Y-%m-%d"))


def strength_from_window(dfw):
    if len(dfw) < 2:
        return np.nan

    # Price movement
    price_delta = dfw["Spot"].iloc[-1] - dfw["Spot"].iloc[0]
    price_score = 50 if price_delta > 0 else -50

    # OI movement
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
# CORE LOGIC
# ===============================
def process_symbol(symbol, file_path):
    df = pd.read_excel(file_path)

    # Validate schema
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        log(f"⚠ {symbol} skipped: missing {missing}")
        return None

    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp")

    # Rolling window
    df_last = df.tail(ROLLING_BARS)

    # OI change
    ce_oi_change = (
        df_last[df_last["Type"] == "CE"]["OI"].diff().sum()
    )
    pe_oi_change = (
        df_last[df_last["Type"] == "PE"]["OI"].diff().sum()
    )

    # Spot & levels
    spot = df_last["Spot"].iloc[-1]
    y_close = df_last["Y_Close"].iloc[-1]
    y_high = df_last["Y_High"].iloc[-1]
    y_low = df_last["Y_Low"].iloc[-1]
    d_high = df_last["D_High"].iloc[-1]
    d_low = df_last["D_Low"].iloc[-1]

    # OI Bias
    if ce_oi_change > pe_oi_change:
        oi_bias = "BULLISH"
    elif pe_oi_change > ce_oi_change:
        oi_bias = "BEARISH"
    else:
        oi_bias = "NEUTRAL"

    # Price Position
    if spot > y_high:
        price_pos = "ABOVE_Y_HIGH"
    elif spot < y_low:
        price_pos = "BELOW_Y_LOW"
    elif spot > y_close:
        price_pos = "ABOVE_Y_CLOSE"
    else:
        price_pos = "BELOW_Y_CLOSE"

    # Strength Score (0–100)
    score = 0

    # Price strength
    if spot > y_close:
        score += 25
    if spot > y_high:
        score += 25
    if spot < y_low:
        score -= 25

    # OI strength
    if oi_bias == "BULLISH":
        score += 25
    elif oi_bias == "BEARISH":
        score -= 25

    score = max(0, min(100, score))

    # Signal logic
    if score >= 65 and oi_bias == "BULLISH":
        signal = "BUY CE"
    elif score <= 35 and oi_bias == "BEARISH":
        signal = "BUY PE"
    else:
        signal = "IGNORE"

    return {
        "Symbol": symbol,
        "Spot": round(spot, 2),
        "Y_Close": y_close,
        "Y_High": y_high,
        "Y_Low": y_low,
        "D_High": d_high,
        "D_Low": d_low,
        "CE_OI_Change": int(ce_oi_change),
        "PE_OI_Change": int(pe_oi_change),
        "OI_Bias": oi_bias,
        "Price_Position": price_pos,
        "Strength_Score": score,
        "Signal": signal,
        "Last_Updated": datetime.now().strftime("%H:%M:%S")
    }

# ===============================
# MAIN LOOP
# ===============================
def run_dashboard():
    log("📊 OPTION DASHBOARD v6 – PRODUCTION MODE STARTED")

    while True:
        try:
            folder = today_folder()
            if not os.path.exists(folder):
                log("⚠ Today folder not found, waiting...")
                time.sleep(REFRESH_SECONDS)
                continue

            rows = []

            for file in os.listdir(folder):
                if not file.endswith(".xlsx"):
                    continue

                symbol = file.replace(".xlsx", "")
                path = os.path.join(folder, file)

                result = process_symbol(symbol, path)
                if result:
                    rows.append(result)

            if rows:
                dashboard_df = pd.DataFrame(rows)
                out_path = os.path.join(folder, OUTPUT_NAME)
                dashboard_df.to_excel(out_path, index=False)
                log(f"✅ Dashboard updated ({len(rows)} stocks)")
            else:
                log("⚠ No valid symbols processed")

        except Exception as e:
            log(f"❌ Error: {e}")

        time.sleep(REFRESH_SECONDS)

# ===============================
# START
# ===============================
if __name__ == "__main__":
    run_dashboard()
