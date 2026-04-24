import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

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

chrome_options = Options()
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

final_rows = []

for symbol, url in stock_urls.items():
    print(f"Fetching {symbol}")
    driver.get(url)
    time.sleep(5)

    # 🔹 Accept cookies if popup appears
    try:
        driver.find_element(By.ID, "onetrust-accept-btn-handler").click()
        time.sleep(2)
    except:
        pass

    # 🔹 Scroll to force table load
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)

    html = driver.page_source
    tables = pd.read_html(StringIO(html))
    df = tables[0]

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
    top_low = df["low"].min()
    diff = round(top_high - top_low, 2)

    bt = round(top_high + diff, 2)
    st = round(top_low - diff, 2)

    for _, row in df.iterrows():
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

driver.quit()

final_df = pd.DataFrame(final_rows)
final_df.to_excel(output_file, index=False)

print(f"\n✅ Excel generated successfully: {output_file}")
