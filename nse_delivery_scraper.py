import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from openpyxl import Workbook

# Read company names from Excel
input_excel = 'stocks.xlsx'
companies = pd.read_excel(input_excel)['Company Name'].dropna().tolist()

# Output Excel writer
output_excel = 'nse_delivery_data.xlsx'
writer = pd.ExcelWriter(output_excel, engine='openpyxl')

# Setup Selenium Chrome
chrome_options = Options()
chrome_options.add_argument('--start-maximized')
chrome_options.add_experimental_option("detach", True)  # Keeps window open

driver = webdriver.Chrome(service=Service('./chromedriver.exe'), options=chrome_options)
#driver = webdriver.Chrome(service=Service('./chromedriver-win64/chromedriver-win64'), options=chrome_options)


base_url = "https://www.nseindia.com/report-detail/eq_security"

for company in companies:
    print(f"\nProcessing: {company}")
    driver.get(base_url)

    try:
        # Wait for symbol input
        wait = WebDriverWait(driver, 15)
        symbol_input = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@placeholder="Symbol"]')))
        symbol_input.clear()
        symbol_input.send_keys(company)
        time.sleep(2)  # Allow dropdown to populate
        symbol_input.send_keys(Keys.RETURN)

        # Click on 1W
        one_week_btn = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"1W")]')))
        one_week_btn.click()

        # Wait for table to load
        wait.until(EC.presence_of_element_located((By.XPATH, '//table')))

        time.sleep(2)  # Ensure all data is loaded

        # Extract table
        rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
        data = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            if len(cols) >= 11:
                data.append({
                    'Date': cols[2].text,
                    'Symbol': cols[0].text,
                    'Total Traded Qty': cols[8].text,
                    'Deliverable Qty': cols[10].text,
                    '% Dly Qt to Traded Qty': cols[11].text
                })

        df = pd.DataFrame(data)
        df.to_excel(writer, sheet_name=company[:31], index=False)
        print(f"✅ Data saved for {company}")

    except Exception as e:
        print(f"❌ Error for {company}: {e}")

# Save output file
writer.close()
driver.quit()
print(f"\n🎉 All data saved to {output_excel}")
