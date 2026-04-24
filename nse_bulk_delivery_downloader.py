import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_nse_delivery_data(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%d-%m-%Y")
    end = datetime.strptime(end_date, "%d-%m-%Y")
    
    all_data = []

    for day in (start + timedelta(n) for n in range((end - start).days + 1)):
        date_str = day.strftime("%d%m%Y")
        readable_date = day.strftime("%d-%m-%Y")
        url = f"https://www1.nseindia.com/archives/equities/mto/MTO_{date_str}.DAT"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www1.nseindia.com/"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                lines = response.text.splitlines()[4:-2]  # skip header/footer
                for line in lines:
                    row = line.strip().split(",")
                    row.insert(2, readable_date)  # Insert date manually
                    all_data.append(row)
                print(f"✅ {readable_date} downloaded.")
            else:
                print(f"⚠️ {readable_date} skipped. HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ Error on {readable_date}: {e}")

    if not all_data:
        print("No data retrieved.")
        return

    columns = [
        "Symbol", "Series", "Date", "Prev Close", "Open Price", "High Price", "Low Price",
        "Last Price", "Close Price", "VWAP", "Traded Qty", "Turnover", "No. of Trades",
        "Deliverable Qty", "Delivery %"
    ]
    
    df = pd.DataFrame(all_data, columns=columns)
    df.to_csv("NSE_Delivery_Data.csv", index=False)
    print("📁 Saved as NSE_Delivery_Data.csv")

# 📅 Change date range as needed
fetch_nse_delivery_data("27-05-2025", "02-06-2025")
