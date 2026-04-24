from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from alice_blue import AliceBlue
from pya3 import *
import pandas as pd
import os
import time
import time as time_module  # renamed to avoid conflict with datetime
from datetime import datetime
import json
import re



# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()
# Adjust dates to skip weekends and holidays
def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:  # Skip weekends
        date -= timedelta(days=1)
    while date in holidays:  # Skip holidays
        date -= timedelta(days=1)
    return date

# Load symbols from the nifty_500_symbols.txt file
def load_symbols_from_txt(file_path):
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            # Extract symbols from the formatted text
            symbols = re.findall(r'"(.*?)"', content)
            return symbols
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []

# Fetch historical data for a symbol
def fetch_historical_data(symbol, from_datetime, to_datetime):
    try:
        instrument = alice.get_instrument_by_symbol('NSE', symbol)
        if not instrument:
            print(f"Instrument not found for symbol: {symbol}")
            return None

        # Fetch historical data
        historical_data = alice.get_historical(
            instrument=instrument,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            interval='D',  # Daily interval
            indices='NIFTY' in symbol or 'BANK' in symbol  # Index flag for NIFTY symbols
        )

        # Handle different return types
        if isinstance(historical_data, list):
            historical_data = pd.DataFrame(historical_data)
        elif isinstance(historical_data, dict):
            print(f"Error fetching historical data for {symbol}: Received dict response instead of data.")
            return None

        if not historical_data.empty:
            print(f"Data for {symbol}:")
            print(historical_data)
            return historical_data
        else:
            print(f"No historical data available for {symbol}.")
            return None

    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return None

# WebSocket callbacks
def socket_open():
    print("WebSocket connection opened.")

def socket_close():
    print("WebSocket connection closed.")

def socket_error(message):
    print(f"WebSocket error: {message}")

def feed_data(message):
    feed_message = json.loads(message)
    if feed_message.get("t") == "tk":  # Tick feed
        symbol = feed_message.get("tk")
        open_price = feed_message.get("o", "N/A")
        high_price = feed_message.get("h", "N/A")
        low_price = feed_message.get("l", "N/A")
        close_price = feed_message.get("c", "N/A")
        volume = feed_message.get("v", "N/A")

        print(f"Symbol: {symbol}")
        print(f"Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {close_price}, Volume: {volume}")

# Main function
def main():
    # Load symbols from the text file
    #file_path = r'C:\Users\Administrator\Documents\algo\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
    #file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
    #file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24\nifty_500_symbols.txt'
    file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24\nifty_index_test.txt'
    
    symbols = load_symbols_from_txt(file_path)

    if not symbols:
        print("No symbols to process.")
        return

    # Adjust dates for fetching historical data
    holidays = [datetime(2024, 12, 25), datetime(2024, 1, 1)]
    yesterday = adjust_for_weekends_and_holidays(datetime.now() - timedelta(days=1), holidays)
    from_datetime = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    to_datetime = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

    # Fetch historical data for each symbol
    for symbol in symbols:
        fetch_historical_data(symbol, from_datetime, to_datetime)

    # Subscribe to live data for symbols
    instruments = [alice.get_instrument_by_symbol('NSE', symbol) for symbol in symbols if symbol]
    subscribe_list = [instrument for instrument in instruments if instrument]

    alice.start_websocket(
        socket_open_callback=socket_open,
        socket_close_callback=socket_close,
        socket_error_callback=socket_error,
        subscription_callback=feed_data,
        run_in_background=True,
        market_depth=False
    )

    # Subscribe to instruments
    alice.subscribe(subscribe_list)
    print("Subscribed to symbols for live data.")

    # Allow streaming for a while
    time.sleep(30)

    # Stop WebSocket
    alice.stop_websocket()
    print("WebSocket connection stopped.")

# Entry point
if __name__ == "__main__":
    main()

