import requests
import pandas as pd

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}

def fetch_index_price(symbol):
    url = f"https://www.nseindia.com/api/equity-stockIndices?index=NIFTY {symbol}"
    with requests.Session() as s:
        s.get("https://www.nseindia.com", headers=HEADERS)
        r = s.get(url, headers=HEADERS)
        data = r.json()
    return data["data"][0]["last"]

def fetch_option_chain(symbol):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    with requests.Session() as s:
        s.get("https://www.nseindia.com", headers=HEADERS)
        r = s.get(url, headers=HEADERS)
        data = r.json()
    return data['records']['data']

def extract_oi_levels(option_data):
    strikes, call_oi, put_oi = [], [], []
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

def show_support_resistance(df, index_name, current_price):
    top_call_oi = df.sort_values(by='Call OI', ascending=False).head(5)
    top_put_oi = df.sort_values(by='Put OI', ascending=False).head(5)

    resistance = top_call_oi.iloc[0]['Strike Price']
    support = top_put_oi.iloc[0]['Strike Price']

    print(f"\n📊 {index_name} Option Chain Analysis:")
    print(f"📈 Current Price: {current_price}")
    print("\n🔴 Top 5 Resistance Levels (Call OI):")
    print(top_call_oi[['Strike Price', 'Call OI']].to_string(index=False))

    print("\n🟢 Top 5 Support Levels (Put OI):")
    print(top_put_oi[['Strike Price', 'Put OI']].to_string(index=False))

    print(f"\n🔐 Key Resistance: {resistance} ({'ABOVE' if resistance > current_price else 'BELOW'} current price)")
    print(f"🛡️ Key Support: {support} ({'BELOW' if support < current_price else 'ABOVE'} current price)")
    print("-" * 60)

if __name__ == "__main__":
    print("📡 Fetching real-time Option Chain + LTP from NSE...\n")

    try:
        for symbol in ["NIFTY", "BANKNIFTY"]:
            index_price = fetch_index_price(symbol)
            option_data = fetch_option_chain(symbol)
            df = extract_oi_levels(option_data)
            show_support_resistance(df, symbol, index_price)
    except Exception as e:
        print(f"⚠️ Error: {e}")
        print("Please retry after a few seconds. NSE might have rate-limited the request.")
