
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta
from feb2025_v8_two import alice, fetch_and_filter_symbol

# Load the NSE symbol list (from IPO)
nse_df = pd.read_csv("NSE.csv")
symbols = nse_df["Symbol"].tolist()

# Filter only EQ if needed
# symbols = [s for s in symbols if s.endswith("-EQ")]

# Time range
to_datetime = datetime.now()
from_datetime = to_datetime - timedelta(days=180)

# Output storage
results = []

def plot_chart(symbol, hist_df):
    plt.figure(figsize=(10, 4))
    plt.plot(hist_df['close'], label="Close Price", color='blue')
    plt.title(f"{symbol} - Last 6 Months")
    plt.xlabel("Days")
    plt.ylabel("Price")
    plt.legend()
    chart_path = f"charts/{symbol}.png"
    plt.savefig(chart_path)
    plt.close()
    return chart_path

# Ensure charts folder
os.makedirs("charts", exist_ok=True)

for symbol in symbols:
    try:
        data = fetch_and_filter_symbol(symbol)
        if not data:
            continue

        latest_close = data["yesterday_close"]
        max_close = data["latest_max_120"]

        breakout_threshold = 0.8 * max_close
        if latest_close >= breakout_threshold and latest_close <= max_close:
            results.append({
                "Symbol": symbol,
                "Latest Close": latest_close,
                "Max IPO High": max_close,
                "Volume": data["yesterday_vol"],
                "Breakout?": "YES",
                "Chart": plot_chart(symbol, alice.get_historical(
                    instrument=alice.get_instrument_by_symbol("NSE", symbol),
                    from_datetime=from_datetime,
                    to_datetime=to_datetime,
                    interval="day",
                    indices=False
                ))
            })
    except Exception as e:
        print(f"❌ Error with {symbol}: {e}")

# Save result to Excel
if results:
    df_result = pd.DataFrame(results)
    out_file = f"IPO_Breakout_Candidates_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    df_result.to_excel(out_file, index=False)
    print(f"✅ Report saved to {out_file}")
else:
    print("❌ No breakout candidates found.")
