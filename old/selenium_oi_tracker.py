
import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def init_browser():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def fetch_index_price(driver, symbol):
    url = "https://www.nseindia.com/market-data/live-equity-market"
    driver.get(url)
    time.sleep(2)
    try:
        index_xpath = "//table[contains(@id, 'equityStockIndicesTable')]//td[contains(text(), '{}')]/following-sibling::td[1]".format("NIFTY 50" if symbol == "NIFTY" else "NIFTY BANK")
        ltp = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, index_xpath))
        ).text.replace(',', '')
        return float(ltp)
    except Exception as e:
        print(f"⚠️ Could not fetch LTP for {symbol}: {e}")
        return None

def fetch_option_chain(driver, symbol):
    url = f"https://www.nseindia.com/option-chain"
    driver.get(url)
    time.sleep(3)
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "equity_symbol"))
        )
        dropdown.clear()
        dropdown.send_keys(symbol)
        time.sleep(2)
        driver.find_element(By.ID, "equity_symbol").submit()
        time.sleep(3)
        
        table = driver.find_element(By.ID, "optionChainTable-indices")
        rows = table.find_elements(By.TAG_NAME, "tr")
        data = []
        for row in rows[2:]:  # Skip headers
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 21:
                continue
            try:
                strike = float(cols[11].text.replace(',', ''))
                ce_oi = int(cols[1].text.replace(',', ''))
                ce_chg = int(cols[2].text.replace(',', ''))
                pe_oi = int(cols[19].text.replace(',', ''))
                pe_chg = int(cols[18].text.replace(',', ''))
                data.append({
                    'Strike Price': strike,
                    'Call OI': ce_oi,
                    'Put OI': pe_oi,
                    'Chg Call OI': ce_chg,
                    'Chg Put OI': pe_chg
                })
            except:
                continue
        return pd.DataFrame(data).sort_values(by='Strike Price')
    except Exception as e:
        print(f"⚠️ Failed to fetch option chain: {e}")
        return pd.DataFrame()

def analyze_and_export(symbol, df, ltp, timestamp):
    total_call_oi = df['Call OI'].sum()
    total_put_oi = df['Put OI'].sum()
    call_chg = df['Chg Call OI'].sum()
    put_chg = df['Chg Put OI'].sum()
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0
    sentiment = 'Bullish' if pcr >= 1 else 'Bearish'

    top_calls = df.sort_values(by='Call OI', ascending=False).head(2)
    top_puts = df.sort_values(by='Put OI', ascending=False).head(2)

    res1 = top_calls.iloc[0]['Strike Price'] if len(top_calls) > 0 else None
    res2 = top_calls.iloc[1]['Strike Price'] if len(top_calls) > 1 else None
    sup1 = top_puts.iloc[0]['Strike Price'] if len(top_puts) > 0 else None
    sup2 = top_puts.iloc[1]['Strike Price'] if len(top_puts) > 1 else None

    export_df = pd.DataFrame({
        'Date': [datetime.now().strftime('%Y-%m-%d')],
        'Time': [datetime.now().strftime('%H:%M:%S')],
        'Symbol': [symbol],
        'LTP': [ltp],
        'Total Call OI': [total_call_oi],
        'Total Put OI': [total_put_oi],
        'Change Call OI': [call_chg],
        'Change Put OI': [put_chg],
        'PCR': [pcr],
        'Sentiment': [sentiment],
        'Top Resistance 1': [res1],
        'Top Resistance 2': [res2],
        'Top Support 1': [sup1],
        'Top Support 2': [sup2],
    })

    save_path = "C:/Users/aarya/OneDrive/Documents/PythonAlgo/DEC24/OI_Summary"
    os.makedirs(save_path, exist_ok=True)
    filename = os.path.join(save_path, f"oi_summary_{symbol}.csv")
    file_exists = os.path.isfile(filename)
    export_df.to_csv(filename, mode='a', header=not file_exists, index=False)

if __name__ == "__main__":
    while True:
        driver = init_browser()
        for symbol in ["NIFTY", "BANKNIFTY"]:
            print(f"🔄 Fetching data for {symbol}...")
            ltp = fetch_index_price(driver, symbol)
            df = fetch_option_chain(driver, symbol)
            if not df.empty:
                analyze_and_export(symbol, df, ltp, datetime.now().strftime('%Y%m%d_%H%M%S'))
            else:
                print(f"⚠️ No data for {symbol}")
        driver.quit()
        print("✅ Data saved. Next update in 10 minutes...")
        time.sleep(600)
