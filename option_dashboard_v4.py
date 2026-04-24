import os
import pandas as pd
from datetime import datetime
import numpy as np

print("📊 OPTION DASHBOARD v5 STARTED")

# ===============================
# CONFIG
# ===============================
BASE_DIR = "ZERODHA_OPTION_DATA"
TODAY = datetime.now().strftime("%Y-%m-%d")
DATA_DIR = os.path.join(BASE_DIR, TODAY)

TODAY = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(DATA_DIR, f"DASHBOARD_{TODAY}_V5.xlsx")

ROLLING = {
    "15m": 3,    # 3 x 5min
    "30m": 6,
    "60m": 12
}

# ===============================
# HELPERS
# ===============================
def strength_score(price_change, ce_oi_chg, pe_oi_chg):
    score = (price_change * 0.6) + ((ce_oi_chg - pe_oi_chg) * 0.4)
    return round(max(min(score, 100), -100), 2)

# ===============================
# MAIN
# ===============================
dashboard = []

for file in os.listdir(DATA_DIR):

    if not file.endswith(".xlsx"):
        continue

    if file.upper().startswith("DASHBOARD"):
        continue

    file_path = os.path.join(DATA_DIR, file)
    df = pd.read_excel(file_path)

    # process df safely



    symbol = file.replace(".xlsx", "")
    path = os.path.join(DATA_DIR, file)

    try:
        df = pd.read_excel(path)
        df.columns = [c.strip() for c in df.columns]

        REQUIRED = [
            "Time", "Spot", "Type", "OI",
            "Y_Close", "Y_High", "Y_Low",
            "D_High", "D_Low"
        ]

        if any(c not in df.columns for c in REQUIRED):
            print(f"⚠ {symbol} skipped: required columns missing")
            continue

        # Timestamp
        df["Time"] = pd.to_datetime(df["Time"])
        df = df.sort_values("Time")

        latest = df.iloc[-1]

        # Split CE / PE
        ce = df[df["Type"] == "CE"]
        pe = df[df["Type"] == "PE"]

        if ce.empty or pe.empty:
            continue

        row = {
            "Symbol": symbol,
            "Spot": latest["Spot"],
            "Y_Close": latest["Y_Close"],
            "Y_High": latest["Y_High"],
            "Y_Low": latest["Y_Low"],
            "D_High": latest["D_High"],
            "D_Low": latest["D_Low"],
            "CE_OI": ce["OI"].iloc[-1],
            "PE_OI": pe["OI"].iloc[-1],
        }

        # ===============================
        # ROLLING STRENGTH
        # ===============================
        for label, bars in ROLLING.items():
            if len(df) < bars:
                row[f"Strength_{label}"] = 0
                continue

            block = df.tail(bars)
            price_chg = block["Spot"].iloc[-1] - block["Spot"].iloc[0]

            ce_oi_chg = (
                block[block["Type"] == "CE"]["OI"].iloc[-1]
                - block[block["Type"] == "CE"]["OI"].iloc[0]
            )
            pe_oi_chg = (
                block[block["Type"] == "PE"]["OI"].iloc[-1]
                - block[block["Type"] == "PE"]["OI"].iloc[0]
            )

            row[f"Strength_{label}"] = strength_score(
                price_chg, ce_oi_chg, pe_oi_chg
            )

        # ===============================
        # COMBINED SCORE
        # ===============================
        row["Combined Strength"] = round(
            0.3 * row["Strength_15m"] +
            0.3 * row["Strength_30m"] +
            0.4 * row["Strength_60m"], 2
        )

        # CE / PE Ratio
        row["CE/PE OI Ratio"] = round(
            row["CE_OI"] / row["PE_OI"] if row["PE_OI"] > 0 else 0, 2
        )

        # ===============================
        # SIGNAL (FIXED & STRICT)
        # ===============================
        if (
            row["Spot"] > row["Y_Close"]
            and row["Spot"] > row["D_Low"]
            and row["CE/PE OI Ratio"] > 1.2
            and row["Combined Strength"] > 15
        ):
            row["Signal"] = "🟢 BUY CE"

        elif (
            row["Spot"] < row["Y_Close"]
            and row["Spot"] < row["D_High"]
            and row["CE/PE OI Ratio"] < 0.8
            and row["Combined Strength"] < -15
        ):
            row["Signal"] = "🔴 BUY PE"

        else:
            row["Signal"] = "⚠ IGNORE"

        dashboard.append(row)

    except Exception as e:
        print(f"⚠ {symbol} error: {e}")

# ===============================
# SAVE
# ===============================
if dashboard:
    out = pd.DataFrame(dashboard)
    out = out.sort_values("Combined Strength", ascending=False)
    out.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Dashboard saved: {OUTPUT_FILE}")
else:
    print("❌ No valid data to save dashboard")
