import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from alice_blue import AliceBlue, TransactionType, OrderType, ProductType
import logging
import time as time_module  # renamed to avoid conflict with datetime
from pya3 import *
from tabulate import tabulate
import json
import re
import threading
import math
import time
import requests
import pickle
from datetime import time as datetime_time

# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

# Global variables
socket_opened = False
terminate_websocket = False
subscribe_list = []
tick_data = {}
orbh_values = {}  # Store ORBH values
orbl_values = {}  # Store ORBL values
orders_placed = set()  # To track orders and prevent duplicates
orders_hit = set()  # To track completed orders (target/SL hit)
square_off_done = False  # To track if square-off is already performed
order_tracking = {}
order_count = 0
MAX_ORDERS = 20  # Maximum orders allowed per day
terminate_square_off = False
sq_start_time = datetime_time(14, 59)
sq_end_time = datetime_time(15, 10)
current_time1 = datetime.now().time()
current_time2 = datetime.now().time()
low_values = []
high_values = []
low_values15 = []
high_values15 = []
#orders_file_path = "orders_placed.pkl"


# Define holidays list
holidays = [
    datetime(2024, 11, 15), datetime(2024, 11, 20), datetime(2024, 12, 25),
    datetime(2025, 1, 1), datetime(2025, 2, 28), datetime(2025, 3, 17),
    datetime(2025, 4, 14), datetime(2025, 4, 18), datetime(2025, 4, 29)
]

# Adjust date to skip weekends and holidays
def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:  # Skip weekends
        date -= timedelta(days=1)
    while date in holidays:  # Skip holidays
        date -= timedelta(days=1)
    return date
    
today = datetime.today()
yesterday = adjust_for_weekends_and_holidays(datetime.now() - timedelta(days=1), holidays)
from_datetime = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
to_datetime = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

from_datetime1 = today.replace(hour=9, minute=15, second=0, microsecond=0)
to_datetime1 = today.replace(hour=15, minute=30, second=59, microsecond=0)

interval = "D"  # Daily interval

# Adjust date to skip weekends and holidays
def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:  # Skip weekends
        date -= timedelta(days=1)
    while date in holidays:  # Skip holidays
        date -= timedelta(days=1)
    return date



# Fetch historical data for a symbol
def fetch_and_filter_symbol(symbol):
    try:
        instrument = alice.get_instrument_by_symbol("NSE", symbol)
        if not instrument or not hasattr(instrument, 'token'):
            return None

        # Fetch historical data for the last few days
        historical_data = alice.get_historical(
            instrument=instrument,
            from_datetime=from_datetime - timedelta(days=300),  # To calculate EMA
            to_datetime=to_datetime,
            interval=interval,
            indices=False
        )

        # Check if data is returned properly
        if isinstance(historical_data, pd.DataFrame) and not historical_data.empty:
            # Ensure all relevant columns are numeric
            numeric_columns = ["open", "close", "high", "low", "volume"]
            for col in numeric_columns:
                historical_data[col] = pd.to_numeric(historical_data[col], errors="coerce")
            
            # Drop rows with any missing or invalid values
            historical_data.dropna(subset=numeric_columns, inplace=True)

            # Ensure enough data is available for analysis
            if historical_data.shape[0] >= 3:
                # Calculate EMA values
                historical_data['EMA_200'] = historical_data['close'].ewm(span=200, adjust=False).mean()
                historical_data['EMA_50'] = historical_data['close'].ewm(span=50, adjust=False).mean()
                historical_data['EMA_20'] = historical_data['close'].ewm(span=20, adjust=False).mean()
                historical_data['EMA_13'] = historical_data['close'].ewm(span=13, adjust=False).mean()

                yesterday_data = historical_data.iloc[-1]
                day_before_data = historical_data.iloc[-2]
                three_days_ago = historical_data.iloc[-3]
                four_days_ago = historical_data.iloc[-4]
                five_days_ago = historical_data.iloc[-5]

                return {
                    "yesterday_open": float(yesterday_data['open']),
                    "yesterday_close": float(yesterday_data['close']),
                    "yesterday_high": float(yesterday_data['high']),
                    "yesterday_low": float(yesterday_data['low']),
                    "yesterday_vol": float(yesterday_data['volume']),
                    "daybefore_vol": float(day_before_data['volume']),
                    "daybefore_open": float(day_before_data['open']),
                    "daybefore_low": float(day_before_data['low']),
                    "daybefore_high": float(day_before_data['high']),
                    "daybefore_close": float(day_before_data['close']),
                    "three_days_ago_open": float(three_days_ago['open']),
                    "three_days_ago_low": float(three_days_ago['low']),
                    "three_days_ago_high": float(three_days_ago['high']),
                    "three_days_ago_close": float(three_days_ago['close']),
                    "four_days_ago_open": float(four_days_ago['open']),
                    "four_days_ago_low": float(four_days_ago['low']),
                    "four_days_ago_high": float(four_days_ago['high']),
                    "four_days_ago_close": float(four_days_ago['close']),
                    "five_days_ago_close": float(five_days_ago['close']),
                    "ema_200": float(yesterday_data['EMA_200']),
                    "ema_50": float(yesterday_data['EMA_50']),
                    "ema_20": float(yesterday_data['EMA_20']),
                    "ema_13": float(yesterday_data['EMA_13']),
                    "symbol_name": symbol
                }
            else:
                print(f"Not enough historical data for {symbol}.")
                return None
        else:
            print(f"No historical data found for {symbol}.")
            return None
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None

# Evaluate buy condition 1
def evaluate_buy_condition_1(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name):
    try:
        if (
            volume > data["yesterday_vol"] and
            #data["daybefore_close"] > data["three_days_ago_close"] and
            #data["yesterday_close"] > data["daybefore_close"] and
            ltp > data["yesterday_close"] and
            open_price > data["three_days_ago_low"] and
            ltp > data["three_days_ago_high"] and
            data["daybefore_vol"] > 300000 and
            ltp > open_price and
            ltp > ORBH15 and          
            ltp > ema_200 and
            ltp > ema_50 and 
            ltp > ema_20 and
            ltp > ema_13 and 
            ema_13 > ema_20 and 
            ema_20 > ema_50 and
            ema_50 > ema_200 
                     
         ):
            print(f"BUY cond 1 -- {symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
            print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def Intraday_Magic(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name):
    try:
        if (

            data["yesterday_close"] <= data["yesterday_open"] and 
            data["daybefore_close"] <= data["daybefore_open"] and 
            data["three_days_ago_close"] <= data["three_days_ago_open"] and
            data["four_days_ago_close"] <= data["four_days_ago_open"] and
            data["daybefore_vol"] > 300000 and
            high_price > SMA_5 and 
            data["yesterday_high"] >= ORBH15 and 
            ltp > ORBH15 and
            ltp > open_price     

         ):
            print(f"Intraday_Magic--{symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
            print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def evaluate_sell_condition(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name):
    try:
        if (

            open_price == high_price and
            #ltp > ORBL5Con and
            ltp < data["ema_200"] and
            ltp < data["ema_50"] and
            ltp < data["ema_20"] and
            ltp < data["ema_13"] and
            data["daybefore_vol"] > 300000 and
            data["yesterday_low"] < data["daybefore_low"] and
            ltp < ORBL5   #data["yesterday_low"]
            
        ):
            print(f"Sell--{symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
            print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            return True
    except KeyError as e:
        print(f"Missing data for sell condition: {e}")
    return False

# Feed data callback
def feed_data(message):
    global tick_data, orbh_values, orbl_values, orders_placed
    feed_message = json.loads(message)

    if feed_message.get("t") == "tk":  # Token data for OHLC
        symbol_token = feed_message.get("tk")

        # Match token with subscription list
        instrument = next((inst for inst in subscribe_list if int(inst.token) == int(symbol_token)), None)
        if instrument:
            symbol_name = instrument.symbol
            ltp = float(feed_message.get("lp", 0))  # Last Traded Price
            open_price = float(feed_message.get("o", 0))
            high_price = float(feed_message.get("h", 0))
            low_price = float(feed_message.get("l", 0))
            volume = float(feed_message.get("v", 0))
            open = open_price
            high = high_price
            low = low_price
            stop_loss = ltp - 3.3

            
            historical_data = fetch_and_filter_symbol(symbol_name)
            #print("historical_data:", historical_data,"\n")
            if historical_data:
                logging.info(f"Historical data fetched for {symbol_name}.")
                yesterday_open = historical_data.get("yesterday_open")
                yesterday_high = historical_data.get("yesterday_high")
                yesterday_low = historical_data.get("yesterday_low")
                yesterday_close = historical_data.get("yesterday_close")                
                yesterday_vol = historical_data.get("yesterday_vol")
                daybefore_open = historical_data.get("daybefore_open")
                daybefore_high = historical_data.get("daybefore_high")
                daybefore_low = historical_data.get("daybefore_low")
                daybefore_close = historical_data.get("daybefore_close")                
                daybefore_vol = historical_data.get("daybefore_vol")
                three_days_ago_open = historical_data.get("three_days_ago_open")
                three_days_ago_high = historical_data.get("three_days_ago_high")
                three_days_ago_low = historical_data.get("three_days_ago_low")
                three_days_ago_close = historical_data.get("three_days_ago_close")                
                three_days_ago_vol = historical_data.get("three_days_ago_vol")
                four_days_ago_open = historical_data.get("four_days_ago_open")
                four_days_ago_high = historical_data.get("four_days_ago_high")
                four_days_ago_low = historical_data.get("four_days_ago_low")
                four_days_ago_close = historical_data.get("four_days_ago_close")            
                four_days_ago_vol = historical_data.get("four_days_ago_vol")
                five_days_ago_close = historical_data.get("five_days_ago_close")
                condition1    = "BUY Condition 1"
                condition2    = "BUY OPEN == LOW"
                condition3    = "SELL OPEN == HIGH"
                condition4    = "BUY ORBH15"
                condition5    = "SELL ORBH15"

                latest_max_5 = max( float(yesterday_close),
                                    float(daybefore_close),
                                    float(three_days_ago_close),
                                    float(four_days_ago_close),
                                    float(five_days_ago_close)
                                    )

            historical_data_one = alice.get_historical(instrument=instrument, from_datetime=from_datetime1, to_datetime=to_datetime1, interval="1", indices=False )
            # Create a list to store the first 60 rows
            if isinstance(historical_data_one, pd.DataFrame) and not historical_data_one.empty:
            #if not historical_data_one.empty:
             
                try:
                    # Extract 'low' and 'high' values for specific ranges
                    if len(historical_data_one) >= 15:
                        low_values = historical_data_one.iloc[:5]['low'].tolist()
                        high_values = historical_data_one.iloc[:5]['high'].tolist()
                        low_values15 = historical_data_one.iloc[:15]['low'].tolist()
                        high_values15 = historical_data_one.iloc[:15]['high'].tolist()
                    else:
                        print("Insufficient data for extracting low and high values")

                    historical_data_one['SMA_5'] = historical_data_one['high'].rolling(window=5).mean()
                    SMA_5 = float(historical_data_one['SMA_5'].iloc[-1])
                    historical_data_one['SMA_Volume_5'] = historical_data_one['volume'].rolling(window=5).mean()
                    SMA_Volume_5 = float(historical_data_one['SMA_Volume_5'].iloc[-1])

                    historical_data_one['EMA_200'] = historical_data_one['close'].ewm(span=200, adjust=False).mean()
                    historical_data_one['EMA_50'] = historical_data_one['close'].ewm(span=50, adjust=False).mean()
                    historical_data_one['EMA_20'] = historical_data_one['close'].ewm(span=20, adjust=False).mean()
                    historical_data_one['EMA_13'] = historical_data_one['close'].ewm(span=13, adjust=False).mean()

                    if not historical_data_one['EMA_200'].empty:
                        ema_200 = round(float(historical_data_one['EMA_200'].iloc[-1]), 2)
                        ema_50 = round(float(historical_data_one['EMA_50'].iloc[-1]), 2)
                        ema_20 = round(float(historical_data_one['EMA_20'].iloc[-1]), 2)
                        ema_13 = round(float(historical_data_one['EMA_13'].iloc[-1]), 2)
                    else:
                        print("EMA 200 is not available. Ensure sufficient data points.")
                except KeyError as e:
                    print(f"KeyError: Missing column in historical data - {e}")        
            else:
                print("Historical data is empty.")    
                        

            # FIRST 15 MINS
            data1 = historical_data_one.iloc[0]
            data2 = historical_data_one.iloc[1]
            data3 = historical_data_one.iloc[2]
            data4 = historical_data_one.iloc[3]
            data5 = historical_data_one.iloc[4]

            # giving none values for below as this program will stop at 10:15 and another will start at 10:15
            ORBL15_one = ORBH15_one = ORBL15_two = ORBH15_two = ORBL15_thr = ORBH15_thr = ORBL15_four = ORBH15_four = ORBL15Con = ORBH15Con = None
            
            # First 5 minutes ORB values (low and high)
            low_values_5_one = low_values[:5]
            high_values_5_one = high_values[:5]
            ORBL5 = min(low_values_5_one)
            ORBH5 = max(high_values_5_one)

            low_values_15_one = low_values[:15]
            high_values_15_one = high_values[:15]
            ORBL15 = min(low_values_15_one)
            ORBH15 = max(high_values_15_one)

            gapup = ((ORBH5 - yesterday_close)/yesterday_close) * 100
            gapdown =  ((yesterday_close - ORBL5)/yesterday_close) * 100
            ORBHP = round(gapup , 2)
            ORBLP = round(gapdown ,2)

            ORBH5Con = float(ORBH5) + 5
            ORBL5Con = float(ORBL5) - 5
            ORBH15Con = float(ORBH15) + 5
            ORBL15Con = float(ORBL15) - 5
            


            #if evaluate_buy_condition_1(historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13):
             #   print(f"BUY cond 1 -- {symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
              #  print(f"Y_Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}, volume: {volume}, yesterday_vol: {yesterday_vol}, daybefore_vol: {daybefore_vol},ema_200:{ema_200},ema_50:{ema_50},ema_20:{ema_20},ema_13:{ema_13}\n")
            #elif Intraday_Magic(historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13):
             #   print(f"Intraday_Magic--{symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
              #  print(f"Y_Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}, volume: {volume}, yesterday_vol:{yesterday_vol}, daybefore_vol: {daybefore_vol}\n")
            try:
                with ThreadPoolExecutor(max_workers=60) as executor:
                    futures = [
                        executor.submit(evaluate_buy_condition_1,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name),
                        executor.submit(Intraday_Magic,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name),
                        executor.submit(evaluate_sell_condition,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name)
                        ]

                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                logging.info(f"Condition met for {symbol_name}")
                        except Exception as exc:
                            logging.error(f"Error during evaluation for {symbol_name}: {exc}")

            except Exception as e:
                logging.error(f"Error processing symbol {symbol_name}: {e}")   
        
                

# Function to keep the WebSocket open for 1 hour
def keep_websocket_open_for_one_hour():
    global terminate_websocket
    print("WebSocket is open. Staying for 1 hour...")
    time.sleep(21000)  # Wait for 1 hour
    terminate_websocket = True
    print("WebSocket stopped. Sleeping 30 seconds...")
    alice.stop_websocket()  # Stop the WebSocket after 1 hour
    time.sleep(30)  # Sleep for 30 seconds



# Create subscription list
def create_subscription_list(symbols):
    global subscribe_list
    subscribe_list = []
    for symbol in symbols:
        instrument = alice.get_instrument_by_symbol("NSE", symbol)
        if instrument:
            subscribe_list.append(instrument)

# Socket open callback
def socket_open():
    global socket_opened
    socket_opened = True
    print("WebSocket connection opened")
    if subscribe_list:
        alice.subscribe(subscribe_list)

# Socket close callback
def socket_close():
    global socket_opened
    socket_opened = False
    print("WebSocket connection closed")
    threading.Thread(target=main_loop).start()  # Restart the WebSocket



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

# Main loop
def main_loop():
    
    #load_orders_placed()
    global socket_opened, terminate_websocket
    #file_path = r'C:\Users\Administrator\Documents\algo\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
    file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
    symbols = load_symbols_from_txt(file_path)
  
    create_subscription_list(symbols)

    while True:
        now = datetime.now()
        current_time = datetime.now().time()
        
        print("Starting new WebSocket session...")
        threading.Thread(target=lambda: alice.start_websocket(
            socket_open_callback=socket_open,
            socket_close_callback=socket_close,
            socket_error_callback=lambda msg: print(f"Error: {msg}"),
            subscription_callback=feed_data,
            run_in_background=True,
            market_depth=False
        )).start()

        while not socket_opened:
            time.sleep(1)
        print("WebSocket is open. Staying active for 6 hour...")
        start_time = time.time()

        # Keep the WebSocket open for 1 hour
        while time.time() - start_time < 21000:
            if terminate_websocket:
                break
            time.sleep(1)  # Sleep for a short period to avoid high CPU usage

        # Stop WebSocket after 1 hour
        print("Stopping WebSocket after 6 hour...")
        alice.stop_websocket()

        # Reset termination flag
        terminate_websocket = False

        # Wait for 30 seconds before restarting the WebSocket
        print("Sleeping for 30 seconds before restarting...")
        time.sleep(30)  # Sleep before restarting the WebSocket session

        
# Entry point
if __name__ == "__main__":
    main_loop()
