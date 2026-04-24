import requests
import pandas as pd
import time
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}

def fetch_index_price(symbol):
    index_map = {
        "NIFTY": "NIFTY 50",
        "BANKNIFTY": "NIFTY BANK"
    }
    index_name = index_map.get(symbol, "NIFTY 50")
    url = f"https://www.nseindia.com/api/equity-stockIndices?index={index_name}"

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=5)
        response = session.get(url, headers=HEADERS, timeout=5)
        if response.ok:
            data = response.json()
            return float(data['data'][0]['last'])
        else:
            print(f"⚠️ NSE response not OK: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Exception while fetching index price: {e}")
    
    return None  # fallback if it fails



def fetch_option_chain(symbol):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=5)
        response = session.get(url, headers=HEADERS, timeout=5)
        if response.ok:
            return response.json()['records']['data']
        else:
            print(f"⚠️ NSE Option Chain response not OK: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Exception while fetching option chain: {e}")
    
    return []


def analyze_oi(option_data):
    strikes, call_oi, put_oi, call_chg_oi, put_chg_oi = [], [], [], [], []
    for entry in option_data:
        strike = entry.get('strikePrice')
        ce = entry.get('CE', {})
        pe = entry.get('PE', {})
        if ce and pe:
            strikes.append(strike)
            call_oi.append(ce.get('openInterest', 0))
            put_oi.append(pe.get('openInterest', 0))
            call_chg_oi.append(ce.get('changeinOpenInterest', 0))
            put_chg_oi.append(pe.get('changeinOpenInterest', 0))

    df = pd.DataFrame({
        'Strike Price': strikes,
        'Call OI': call_oi,
        'Put OI': put_oi,
        'Chg Call OI': call_chg_oi,
        'Chg Put OI': put_chg_oi
    }).sort_values(by='Strike Price')

    total_call_oi = sum(call_oi)
    total_put_oi = sum(put_oi)
    total_call_chg = sum(call_chg_oi)
    total_put_chg = sum(put_chg_oi)
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi != 0 else 0

    return df, total_call_oi, total_put_oi, total_call_chg, total_put_chg, pcr

def show_and_export(symbol, df, ltp, call_oi, put_oi, call_chg, put_chg, pcr, timestamp):
    sentiment = "🐂" if pcr >= 1 else "🐻"
    print(f"\n【 {symbol} 】")
    print(f"📈 LTP        : {ltp}")
    print(f"Call OI       : {call_oi:,}")
    print(f"Put OI        : {put_oi:,}")
    print(f"PCR           : {pcr} {sentiment}")
    print(f"Chg OI Call   : {'📈' if call_chg >= 0 else '📉'} {call_chg:+,}")
    print(f"Chg OI Put    : {'📈' if put_chg >= 0 else '📉'} {put_chg:+,}")

    top_calls = df.sort_values(by='Call OI', ascending=False).head(2)
    top_puts = df.sort_values(by='Put OI', ascending=False).head(2)

    print("\n🔴 Top 5 Resistance:")
    print(top_calls[['Strike Price', 'Call OI']].to_string(index=False))
    print("\n🟢 Top 5 Support:")
    print(top_puts[['Strike Price', 'Put OI']].to_string(index=False))
    print("-" * 60)

    # Save to CSV
    export_df = pd.DataFrame({
        'Symbol': [symbol],
        'LTP': [ltp],
        'Total Call OI': [call_oi],
        'Total Put OI': [put_oi],
        'Change Call OI': [call_chg],
        'Change Put OI': [put_chg],
        'PCR': [pcr],
        'Sentiment': ['Bullish' if pcr >= 1 else 'Bearish']
    })
    #filename = f"oi_summary_{symbol}_{timestamp}.csv"
    filename = f"C:/Users/aarya/OneDrive/Documents/PythonAlgo/DEC24/OI_Summary/oi_summary_{symbol}_{timestamp}.csv"
    
    export_df.to_csv(filename, index=False)

if __name__ == "__main__":
    while True:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Fetching real-time data...\n")

        try:
            for symbol in ["NIFTY", "BANKNIFTY"]:
                ltp = fetch_index_price(symbol)
                option_data = fetch_option_chain(symbol)
                df, call_oi, put_oi, call_chg, put_chg, pcr = analyze_oi(option_data)
                show_and_export(symbol, df, ltp, call_oi, put_oi, call_chg, put_chg, pcr, timestamp)

        except Exception as e:
            print(f"⚠️ Error: {e}\nRetrying in 6 minutes...")

        print("\n🔄 Next update in 6 minutes...\n")
        time.sleep(360)  # 6 minutes
