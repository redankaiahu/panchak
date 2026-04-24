import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# ===============================
# CONFIG
# ===============================
BASE_DIR = Path("ZERODHA_OPTION_DATA")
TODAY = datetime.now().strftime("%Y-%m-%d")
DATA_DIR = BASE_DIR / TODAY

DASHBOARD_FILE = BASE_DIR / f"DASHBOARD_{TODAY}_V3.xlsx"

ROLLING_WINDOWS = {
    "15m": 3,
    "30m": 6,
    "60m": 12
}

REFRESH_SECONDS = 300  # 5 mins

REQUIRED_COLS = [
    "Time", "Spot", "Type", "OI", "LTP",
    "Y_Close", "Y_High", "Y_Low"
]

print("📊 OPTION DASHBOARD v3 STARTED")

# ===============================
# SCORING
# ===============================
def strength_score(spot_pct, dom_oi, opp_oi, ratio):
    score = 0
    score += min(abs(spot_pct) * 25, 40)          # price
    score += min(abs(dom_oi) / 50000 * 25, 30)    # OI
    score += min(abs(dom_oi - opp_oi) / 50000 * 20, 20)
    score += min(ratio * 10, 10)                  # dominance
    return int(min(score, 100))

# ===============================
# WINDOW ANALYSIS
# ===============================
def analyze_window(df, rows):
    if len(df) < rows:
        return None

    recent = df.tail(rows)

    spot_start = recent.iloc[0]["Spot"]
    spot_end = recent.iloc[-1]["Spot"]
    spot_pct = ((spot_end - spot_start) / spot_start) * 100

    ce = recent[recent["Type"] == "CE"]
    pe = recent[recent["Type"] == "PE"]

    if ce.empty or pe.empty:
        return None

    ce_oi = ce.iloc[-1]["OI"] - ce.iloc[0]["OI"]
    pe_oi = pe.iloc[-1]["OI"] - pe.iloc[0]["OI"]

    ratio = round(abs(ce_oi) / abs(pe_oi), 2) if pe_oi != 0 else 0

    y_close = df.iloc[-1]["Y_Close"]
    y_high = df.iloc[-1]["Y_High"]
    y_low = df.iloc[-1]["Y_Low"]

    t_high = df["Spot"].max()
    t_low = df["Spot"].min()

    signal = "⚪ IGNORE"
    bias = "Neutral"
    entry = None
    score = 0

    # ===== BUY CE =====
    if (
        spot_end > y_close and
        spot_pct > 0 and
        ce_oi > 0 and
        pe_oi <= 0 and
        ratio > 1.2
    ):
        signal = "🟢 BUY CE"
        bias = "Bullish"
        entry = round(ce.iloc[-1]["LTP"], 2)
        score = strength_score(spot_pct, ce_oi, pe_oi, ratio)

    # ===== BUY PE =====
    elif (
        spot_end < y_close and
        spot_pct < 0 and
        pe_oi > 0 and
        ce_oi <= 0 and
        ratio < 0.8
    ):
        signal = "🔴 BUY PE"
        bias = "Bearish"
        entry = round(pe.iloc[-1]["LTP"], 2)
        score = strength_score(spot_pct, pe_oi, ce_oi, ratio)

    return {
        "Spot": round(spot_end, 2),
        "Spot %": round(spot_pct, 2),
        "Y Close": y_close,
        "Y High": y_high,
        "Y Low": y_low,
        "T High": round(t_high, 2),
        "T Low": round(t_low, 2),
        "CE OI Δ": int(ce_oi),
        "PE OI Δ": int(pe_oi),
        "CE/PE OI Ratio": ratio,
        "Strength": score,
        "Bias": bias,
        "Signal": signal,
        "Entry Price": entry
    }

# ===============================
# MAIN LOOP
# ===============================
while True:
    dashboard_rows = []

    for file in DATA_DIR.glob("*.xlsx"):
        symbol = file.stem

        try:
            df = pd.read_excel(file)

            # Validate columns
            missing = [c for c in REQUIRED_COLS if c not in df.columns]
            if missing:
                print(f"⚠ {symbol} skipped – missing {missing}")
                continue

            df["Time"] = pd.to_datetime(df["Time"])
            df = df.sort_values("Time")

            for win, rows in ROLLING_WINDOWS.items():
                result = analyze_window(df, rows)
                if not result:
                    continue

                dashboard_rows.append({
                    "Symbol": symbol,
                    "Window": win,
                    **result
                })

        except Exception as e:
            print(f"⚠ {symbol} error:", e)

    if dashboard_rows:
        dash = pd.DataFrame(dashboard_rows)
        dash = dash.sort_values("Strength", ascending=False)

        dash["Top Pick"] = ""
        dash.loc[dash.head(3).index, "Top Pick"] = "🔥 TOP 3"

        dash["Last Updated"] = datetime.now().strftime("%H:%M:%S")

        dash.to_excel(DASHBOARD_FILE, index=False)

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Dashboard updated | Best Strength: {dash.iloc[0]['Strength']}"
        )
    else:
        print("⚠ Waiting for sufficient data...")

    time.sleep(REFRESH_SECONDS)
