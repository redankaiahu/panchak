import pandas as pd
from pathlib import Path
from datetime import datetime
import time

BASE_DIR = Path("ZERODHA_OPTION_DATA")
TODAY = datetime.now().strftime("%Y-%m-%d")
DATA_DIR = BASE_DIR / TODAY

DASHBOARD_FILE = BASE_DIR / f"DASHBOARD_{TODAY}.xlsx"

WINDOWS = {
    "15m": 3,
    "30m": 6,
    "60m": 12
}

print("📊 OPTION DASHBOARD v1 STARTED")

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

    # ---------------- SIGNAL LOGIC ----------------
    if spot_pct > 0 and ce_oi > 0 and pe_oi <= 0:
        signal = "🟢 BUY CE"
        bias = "Bullish"
    elif spot_pct < 0 and pe_oi > 0 and ce_oi <= 0:
        signal = "🔴 BUY PE"
        bias = "Bearish"
    else:
        signal = "⚪ IGNORE"
        bias = "Neutral"

    return {
        "Spot %": round(spot_pct, 2),
        "CE OI Δ": int(ce_oi),
        "PE OI Δ": int(pe_oi),
        "Bias": bias,
        "Signal": signal
    }

while True:
    dashboard_rows = []

    for file in DATA_DIR.glob("*.xlsx"):
        symbol = file.stem

        try:
            df = pd.read_excel(file)
            df["Time"] = pd.to_datetime(df["Time"])
            df = df.sort_values("Time")

            for win, rows in WINDOWS.items():
                res = analyze_window(df, rows)
                if not res:
                    continue

                dashboard_rows.append({
                    "Symbol": symbol,
                    "Window": win,
                    **res
                })

        except Exception as e:
            print(f"⚠ {symbol} error:", e)

    if dashboard_rows:
        dash_df = pd.DataFrame(dashboard_rows)
        dash_df.to_excel(DASHBOARD_FILE, index=False)

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Dashboard updated | Signals: {len(dashboard_rows)}"
        )
    else:
        print("⚠ No sufficient data yet")

    time.sleep(300)   # refresh every 5 mins
