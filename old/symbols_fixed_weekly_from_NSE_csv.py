
import pandas as pd
import os
import time
import json
from datetime import datetime, timedelta
from alice_blue import AliceBlue
from pya3 import *

# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

websocket_ready = False

# Load NSE.csv containing all instruments (including index)
nse_master_df = pd.read_csv("NSE.csv")

def get_instrument_from_csv(symbol):
    row = nse_master_df[nse_master_df["Symbol"].str.upper() == symbol.upper()]
    if row.empty:
        return None
    return {
        "symbol": symbol,
        "exchange": row.iloc[0]["Exchange"],
        "token": int(row.iloc[0]["Token"]),
        "instrument_type": row.iloc[0]["Instrument Type"]
    }

def fetch_historical_data(symbol, from_datetime, to_datetime):
    try:
        instrument_info = get_instrument_from_csv(symbol)
        if not instrument_info:
            print(f"⚠️ Symbol {symbol} not found in NSE.csv")
            return None

        instrument = alice.get_instrument_by_token(
            exchange=instrument_info["exchange"],
            token=instrument_info["token"]
        )

        historical_data = alice.get_historical(
            instrument=instrument,
            from_datetime=from_datetime - timedelta(days=14),
            to_datetime=to_datetime,
            interval='Day',
            indices='NIFTY' in symbol or 'BANK' in symbol
        )

        if isinstance(historical_data, dict):
            print(f"⚠️ API Error for {symbol}: {historical_data}")
            return None

        df = pd.DataFrame(historical_data)
        if df.empty:
            print(f"No data for {symbol}")
            return None

        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

        today = datetime.now()
        last_friday = today - timedelta(days=today.weekday() + 3)
        last_monday = last_friday - timedelta(days=4)

        mask = (df.index >= last_monday) & (df.index <= last_friday)
        weekly_df = df.loc[mask]

        if weekly_df.empty:
            print(f"No data for last week for {symbol}")
            return None

        open_price = weekly_df.iloc[0]['open']
        high_price = weekly_df['high'].max()
        low_price = weekly_df['low'].min()
        close_price = weekly_df.iloc[-1]['close']
        volume = weekly_df['volume'].sum()

        print(f"📊 Weekly OHLC for {symbol} (Prev Week {last_monday.date()} to {last_friday.date()}):")
        print(f"Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {close_price}, Volume: {volume}")

        return {
            "symbol": symbol,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume
        }

    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return None

def socket_open():
    global websocket_ready
    print("✅ WebSocket connection opened.")
    websocket_ready = True

def socket_close():
    print("WebSocket connection closed.")

def socket_error(message):
    print(f"WebSocket error: {message}")

def feed_data(message):
    feed_message = json.loads(message)
    if feed_message.get("t") == "tk":
        symbol = feed_message.get("tk")
        open_price = feed_message.get("o", "N/A")
        high_price = feed_message.get("h", "N/A")
        low_price = feed_message.get("l", "N/A")
        close_price = feed_message.get("c", "N/A")
        volume = feed_message.get("v", "N/A")

        print(f"Symbol: {symbol}")
        print(f"Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {close_price}, Volume: {volume}")

def main():
    file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24\nifty_index_test.txt'
    with open(file_path, 'r') as file:
        symbols = [s.strip().strip('"') for s in file.read().splitlines() if s.strip()]

    symbols = [s.replace('NIFTY 50', 'NIFTY').replace('NIFTY BANK', 'BANKNIFTY') for s in symbols]

    today = datetime.now()
    last_friday = today - timedelta(days=today.weekday() + 3)
    last_monday = last_friday - timedelta(days=4)

    from_datetime = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    to_datetime = last_friday.replace(hour=23, minute=59, second=59, microsecond=0)

    for symbol in symbols:
        fetch_historical_data(symbol, from_datetime, to_datetime)

if __name__ == "__main__":
    main()
