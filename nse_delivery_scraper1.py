import csv
import time
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ✅ List of correct NSE symbols
stock_symbols = [
    "SIGNETIND",
    "RELIANCE",
    "TCS",
    "INFY"
]

output_file = "delivery_data.csv"

options = Options()
# Comment for visible browser
# options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0")

driver = webdriver.Chrome(service=Service('./chromedriver.exe'), options=options)
wait = WebDriverWait(driver, 15)

with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["Symbol", "Date", "Series", "Prev Close", "Open", "High", "Low", "Last", "Close", "VWAP",
                     "Total Qty", "Turnover", "Trades", "Delivery Qty", "Delivery %"])

    for symbol in stock_symbols:
        try:
            driver.get("https://www.nseindia.com/report-detail/eq_security")
            time.sleep(3)

            # Type symbol
            input_box = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Symbol']")))
            input_box.clear()
            input_box.send_keys(symbol)
            time.sleep(2)

            # Wait and find suggestion dropdown entries
            suggestions = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul[role='listbox'] li")))

            # Match exact NSE symbol (on the right side of each suggestion)
            matched = False
            for item in suggestions:
                if item.text.endswith(symbol):
                    item.click()
                    matched = True
                    break

            if not matched:
                raise Exception(f"No exact dropdown match found for symbol: {symbol}")

            time.sleep(2)

            # Click 1W
            one_week_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '1W')]")))
            one_week_btn.click()
            time.sleep(2)

            # Read table
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            rows = table.find_elements(By.TAG_NAME, "tr")

            if len(rows) <= 1:
                raise Exception("No data found")

            for row in rows[1:]:
                cols = [td.text for td in row.find_elements(By.TAG_NAME, "td")]
                if cols:
                    writer.writerow([symbol] + cols)

            print(f"✅ Success: {symbol}")

        except Exception as e:
            print(f"❌ Failed for {symbol}: {e}")
            traceback.print_exc()

driver.quit()
print(f"\n📁 Done! Data saved to {output_file}")
