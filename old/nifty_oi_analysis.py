import requests
import pandas as pd
from bs4 import BeautifulSoup

def fetch_nifty_option_chain():
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9"
    }

    with requests.Session() as s:
        s.get("https://www.nseindia.com", headers=headers)
        response = s.get(url, headers=headers)
        data = response.json()

    return data['records']['data']

def extract_oi_levels(option_data):
    strikes = []
    call_oi = []
    put_oi = []

    for entry in option_data:
        strike = entry.get('strikePrice')
        ce_data = entry.get('CE', {})
        pe_data = entry.get('PE', {})

        if ce_data and pe_data:
            strikes.append(strike)
            call_oi.append(ce_data.get('openInterest', 0))
            put_oi.append(pe_data.get('openInterest', 0))

    df = pd.DataFrame({
        'Strike Price': strikes,
        'Call OI': call_oi,
        'Put OI': put_oi
    }).sort_values(by='Strike Price')

    return df

def show_support_resistance(df):
    top_call_oi = df.sort_values(by='Call OI', ascending=False).head(5)
    top_put_oi = df.sort_values(by='Put OI', ascending=False).head(5)

    print("\n🔴 Top 5 Resistance Levels (Call OI):")
    print(top_call_oi[['Strike Price', 'Call OI']])

    print("\n🟢 Top 5 Support Levels (Put OI):")
    print(top_put_oi[['Strike Price', 'Put OI']])

    print("\n🔐 Key Resistance Level:", top_call_oi.iloc[0]['Strike Price'])
    print("🛡️ Key Support Level:", top_put_oi.iloc[0]['Strike Price'])

# Run everything
if __name__ == "__main__":
    print("Fetching real-time Nifty Option Chain data...")
    option_data = fetch_nifty_option_chain()
    df = extract_oi_levels(option_data)
    show_support_resistance(df)
