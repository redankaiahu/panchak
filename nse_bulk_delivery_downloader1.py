import requests
import pandas as pd
from datetime import datetime

# 🗓️ Change this to the date you want
date = datetime.strptime("02-06-2025", "%d-%m-%Y")
date_str = date.strftime("%d%m%Y")

url = f"https://www1.nseindia.com/archives/equities/mto/MTO_{date_str}.DAT"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www1.nseindia.com/"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    text = response.text.splitlines()[4:-2]  # skip header/footer
    data = [line.strip().split(",") for line in text]

    df = pd.DataFrame(data, columns=[
        "Symbol", "Series", "Date", "Prev Close", "Open Price", "High Price", "Low Price",
        "Last Price", "Close Price", "VWAP", "Traded Qty", "Turnover", "No. of Trades",
        "Deliverable Qty", "Delivery %"
    ])

    df.to_csv(f"delivery_data_{date_str}.csv", index=False)
    print(f"✅ Saved data for {date_str} to CSV.")
else:
    print(f"❌ Failed to fetch data for {date_str}. HTTP Status: {response.status_code}")
