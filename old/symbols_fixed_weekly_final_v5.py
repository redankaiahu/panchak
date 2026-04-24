
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from alice_blue import AliceBlue
from pya3 import *
import pandas as pd
import os
import time
import json
import re

# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

websocket_ready = False

def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    while date in holidays:
        date -= timedelta(days=1)
    return date

def load_symbols_from_txt(file_path):
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            symbols = re.findall(r'"(.*?)"', content)
            return symbols
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []

def fetch_historical_data(symbol, from_datetime, to_datetime):
    try:
        exchange = 'NSE'
        if symbol in ['NIFTY', 'BANKNIFTY']:
            exchange = 'INDEX'

        instrument = alice.get_instrument_by_symbol(exchange, symbol)
        if isinstance(instrument, dict):
            print(f"⚠️ Skipping {symbol} due to instrument fetch error: {instrument}")
            return None
        if not instrument:
            print(f"Instrument not found for symbol: {symbol}")
            return None

        # Always fetch daily data, even for weekly needs
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

        if isinstance(historical_data, list):
            df = pd.DataFrame(historical_data)
            if df.empty:
                print(f"No data for {symbol}")
                return None

            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)

            # Filter previous week's Monday to Friday
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

        else:
            print(f"Unexpected data type received for {symbol}")
            return None

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
    symbols = load_symbols_from_txt(file_path)
    symbols = [s.replace('NIFTY 50', 'NIFTY').replace('NIFTY BANK', 'BANKNIFTY') for s in symbols]

    if not symbols:
        print("No symbols to process.")
        return

    today = datetime.now()
    last_friday = today - timedelta(days=today.weekday() + 3)
    last_monday = last_friday - timedelta(days=4)

    from_datetime = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    to_datetime = last_friday.replace(hour=23, minute=59, second=59, microsecond=0)

    for symbol in symbols:
        fetch_historical_data(symbol, from_datetime, to_datetime)

    instruments = []
    for symbol in symbols:
        try:
            exchange = 'NSE'
            if symbol in ['NIFTY', 'BANKNIFTY']:
                exchange = 'INDEX'

            inst = alice.get_instrument_by_symbol(exchange, symbol)
            if hasattr(inst, 'token') and hasattr(inst, 'exchange'):
                instruments.append(inst)
            else:
                print(f"⚠️ Invalid instrument for {symbol}: {inst}")
        except Exception as e:
            print(f"❌ Error getting instrument for {symbol}: {e}")

    subscribe_list = instruments

    alice.start_websocket(
        socket_open_callback=socket_open,
        socket_close_callback=socket_close,
        socket_error_callback=socket_error,
        subscription_callback=feed_data,
        run_in_background=True,
        market_depth=False
    )

    timeout = 10
    start_time = time.time()
    while not websocket_ready and time.time() - start_time < timeout:
        time.sleep(0.5)

    if websocket_ready:
        alice.subscribe(subscribe_list)
        print("✅ Subscribed to symbols for live data.")
    else:
        print("❌ WebSocket not ready. Skipping subscription.")

    time.sleep(30)
    alice.stop_websocket()
    print("WebSocket connection stopped.")

if __name__ == "__main__":
    main()
