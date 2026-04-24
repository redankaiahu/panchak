import pandas as pd
import requests
from datetime import datetime
from io import StringIO

# ================= USER INPUT =================
stock_urls = {
    "NIFTY": "https://in.investing.com/indices/s-p-cnx-nifty-historical-data",
    "BANKNIFTY": "https://in.investing.com/indices/bank-nifty-historical-data",
    "RELIANCE": "https://in.investing.com/equities/reliance-industries-historical-data",
    "INFY": "https://in.investing.com/equities/infosys-historical-data",
    "HCLTECH": "https://in.investing.com/equities/hcl-technologies-historical-data",
    "TVSMOTO": "https://in.investing.com/equities/tvs-motor-company-historical-data",
    "BHARATFC": "https://in.investing.com/equities/bharat-forge-historical-data"
}

from_date = "01-12-2025"
to_date   = "02-01-2026"

output_file = "ALL_STOCKS_OHLC.xlsx"
# ==============================================

headers = {"User-Agent": "Mozilla/5.0"}
final_rows = []

for symbol, url in stock_urls.items():
    print(f"Fetching {symbol}")

    r = requests.get(url, headers=headers)
    ##df = pd.read_html(r.text)[0]
    from io import StringIO
    df = pd.read_html(StringIO(r.text))[0]


    df = df.rename(columns={
        "Price": "close",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Date": "date"
    })

    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    df = df[(df["date"] >= from_date) & (df["date"] <= to_date)]
    df = df.sort_values("date", ascending=False).head(3)

    top_high = df["high"].max()
    top_low  = df["low"].min()
    diff = round(top_high - top_low, 2)

    bt = round(top_high + diff, 2)
    st = round(top_low - diff, 2)

    for i, row in df.iterrows():
        final_rows.append({
            "symbol": symbol,
            "date": row["date"].date(),
            "close": round(row["close"], 2),
            "open": round(row["open"], 2),
            "high": round(row["high"], 2),
            "low": round(row["low"], 2),
            "TOP HIGH": round(top_high, 2),
            "TOP LOW": round(top_low, 2),
            "DIFF": diff,
            "BT": bt,
            "ST": st
        })

final_df = pd.DataFrame(final_rows)

final_df.to_excel(output_file, index=False)
print(f"\n✅ Excel generated: {output_file}")
