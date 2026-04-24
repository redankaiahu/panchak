import os
import pandas as pd
import numpy as np
import threading
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
import sys
import signal

holidays = [
    datetime(2025, 5, 1),  # Maharashtra Day
    datetime(2025, 4, 14), # Ambedkar Jayanti
    datetime(2025, 4, 18),
    datetime(2025, 5, 1),
    datetime(2025, 8, 15),
    datetime(2025, 8, 27),
    datetime(2025, 10, 2),
    datetime(2025, 10, 21),
    datetime(2025, 10, 22),
    datetime(2025, 11, 5),
    datetime(2025, 12, 25),


]
#candle = get_last_week_candle(historical_data, holidays)


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
order_count = 1
MAX_ORDERS = 20  # Maximum orders allowed per day
terminate_square_off = False
cancel_unnecessary = False
sq_start_time = datetime_time(15, 10)
sq_end_time = datetime_time(15, 15)
sq_start_time1 = datetime_time(9, 15)
sq_end_time1 = datetime_time(15, 20)
place_order_time = datetime_time(9, 30)
place_order_start = datetime_time(9, 30)
place_order_end = datetime_time(23, 40)
current_time1 = datetime.now().time()
current_time3 = datetime.now().strftime('%H:%M:%S')  #Get the current time as a string in HH:MM:SS format
#current_time1 = current_time3.strftime('%H:%M:%S')
current_time2 = datetime.now().time()
low_values = []
high_values = []
low_values15 = []
high_values15 = []
sl_modify_tracking = []
websocket_lock = threading.Lock()
ltp_by_symbol = {}
printed_symbols = {}
modified_symbols = {}
executor1 = ThreadPoolExecutor(max_workers=50)  # Initialize globally
terminate_program = False              # Global flag to control program termination


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

def get_orders_file_path():
    today = datetime.now()
    file_name = today.strftime("may_orders_placed_%d-%m-%Y.pkl")
    return file_name

def load_orders_placed():
    global orders_placed
    file_path = get_orders_file_path()
    
    if os.path.exists(file_path):
        try:
            with open(file_path, 'rb') as f:
                orders_placed = pickle.load(f)
                if not orders_placed:
                    print(f"Warning: '{file_path}' is empty. Initializing with an empty set.")
                    orders_placed = set()
        except (EOFError, pickle.UnpicklingError):
            print(f"Error reading '{file_path}'. Initializing with an empty set.")
            orders_placed = set()
    else:
        print(f"Warning: '{file_path}' does not exist. Initializing with an empty set.")
        orders_placed = set()

def save_orders_placed():
    global orders_placed
    file_path = get_orders_file_path()
    
    try:
        with open(file_path, 'wb') as f:
            pickle.dump(orders_placed, f)
            print(f"Orders saved to '{file_path}'.")
    except Exception as e:
        print(f"Error saving orders to '{file_path}': {e}")
       

def cancel_unnecessary_orders():
    
    print(f"=================>>>>>>>>>>>>  Calling cancel_unnecessary_orders... [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    try:
        # Fetch all open orders
        open_orders = alice.get_order_history('')
        # Ensure open_orders is a list
        if not open_orders:  # Handles None, empty string, or empty list
            #print("No open orders found.")
            return
        if isinstance(open_orders, str):
            import json
            try:
                open_orders = json.loads(open_orders)
            except json.JSONDecodeError:
                #print("Error parsing order history response.")
                return
        if not isinstance(open_orders, list):
            #print("Unexpected data format for open_orders:", open_orders)
            return
        if len(open_orders) == 0:
            #print("No open orders to process.")
            return

        # Fetch netwise positions
        net_position = alice.get_netwise_positions()
        open_positions = Alice_Wrapper.open_net_position(net_position)
        # Extract symbols with open positions
        open_position_symbols = {pos['Symbol'] for pos in open_positions}

        # Track symbols with valid 'open' or 'trigger pending' orders
        valid_orders = {}

        for order in open_orders:
            if not isinstance(order, dict):
                #print("Skipping invalid order format:", order)
                continue
            symbol = order.get('Sym')
            status = order.get('Status', '').lower()
            order_id = order.get('Nstordno')
            #print(symbol,status,order_id)

            if not symbol or not status or not order_id:
                continue

            # Identify symbols with active orders
            if status in ['open', 'trigger pending']:
                if symbol not in valid_orders:
                    valid_orders[symbol] = {'open': [], 'trigger_pending': []}

                valid_orders[symbol][status.replace(' ', '_')].append(order_id)

        # Process cancellation of orders with incomplete matching
        for symbol, orders in valid_orders.items():
            open_orders = orders['open']
            trigger_pending_orders = orders['trigger_pending']

            # Skip cancellation if the symbol has an open position
            if symbol in open_position_symbols:
                #print(f"Symbol {symbol} has an open position. Skipping cancellation.")
                continue

            if not (open_orders and trigger_pending_orders):
                # Cancel any incomplete orders for the symbol
                for order_id in open_orders + trigger_pending_orders:
                    cancel_response = alice.cancel_order(order_id)
                    print(f"=========>>>>>>>>>  Cancelled order {order_id} for symbol {symbol}: {cancel_response} TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")

        #print("Order cleanup complete.")
        time.sleep(200)

    except Exception as e:
        print(f"Error while canceling orders: {e}")



from datetime import datetime, timedelta, date
import pandas as pd
import calendar


def get_last_week_candle(historical_data, holidays):
    # Step 1: Ensure proper datetime index
    if 'datetime' in historical_data.columns:
        historical_data['datetime'] = pd.to_datetime(historical_data['datetime'])
        historical_data.set_index('datetime', inplace=True)

    historical_data = historical_data.sort_index()

    # Step 2: Normalize holiday dates
    holiday_dates = set(h.date() for h in holidays) if holidays else set()

    # Step 3: Get today's date
    today = datetime.today().date()

    # Step 4: Get all weekdays of the current week
    curr_weekday = today.weekday()  # 0 = Monday
    curr_week_start = today - timedelta(days=curr_weekday)
    curr_week_dates = {curr_week_start + timedelta(days=i) for i in range(5)}  # Mon to Fri

    # Step 5: Get last week's Mon to Fri dates
    last_week_start = curr_week_start - timedelta(days=7)
    last_week_dates = [last_week_start + timedelta(days=i) for i in range(5)]
    last_week_dates_set = set(last_week_dates)

    #print("\n🗓️ Last week dates:", last_week_dates)

    # Step 6: Filter last 20 days and match normalized dates
    last_20 = historical_data.tail(30).copy()
    last_20['date_only'] = last_20.index.normalize().date

    #print("✅ Dates in DataFrame:", set(last_20['date_only']))
    #print("✅ Looking for matches in:", last_week_dates)

    filtered = last_20[last_20['date_only'].isin(last_week_dates)]

    #print("\n📉 Filtered Data (matching actual last week's Mon-Fri):")
    #print(filtered)

    if filtered.empty or len(filtered) < 3:
        print("❌ Not enough data for weekly candle.")
        return None

    # Step 7: Build the weekly candle
    weekly_open = filtered['open'].iloc[0]
    weekly_high = filtered['high'].max()
    weekly_low = filtered['low'].min()
    weekly_close = filtered['close'].iloc[-1]

    #print("\n✅ Weekly Candle Dates Used:", list(filtered['date_only']))

    return {
        "weekly_open": float(weekly_open),
        "weekly_high": float(weekly_high),
        "weekly_low": float(weekly_low),
        "weekly_close": float(weekly_close)
    }





# Fetch historical data for a symbol
def fetch_and_filter_symbol(symbol):
    #print("in fetch_and_filter_symbol loop")
    try:
        instrument = alice.get_instrument_by_symbol("NSE", symbol)
        if not instrument or not hasattr(instrument, 'token'):
        #if not instrument or not isinstance(instrument, dict) or 'token' not in instrument:
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
        #if isinstance(historical_data, pd.DataFrame) and not historical_data.empty and historical_data.shape[0] >= 300:
        if isinstance(historical_data, pd.DataFrame) and not historical_data.empty:
            # Ensure all relevant columns are numeric
            numeric_columns = ["open", "close", "high", "low", "volume"]
            for col in numeric_columns:
                historical_data[col] = pd.to_numeric(historical_data[col], errors="coerce")
            
            # Drop rows with any missing or invalid values
            historical_data.dropna(subset=numeric_columns, inplace=True)
            if historical_data.shape[0] >= 3:
                
                yesterday_data = historical_data.iloc[-1]
                day_before_data = historical_data.iloc[-2]
                three_days_ago = historical_data.iloc[-3]
                four_days_ago = historical_data.iloc[-4]
                five_days_ago = historical_data.iloc[-5]
                days_6_ago = historical_data.iloc[-6]
                days_6_ago_close = float(days_6_ago['close'])
                days_7_ago = historical_data.iloc[-7]
                days_7_ago_close = float(days_7_ago['close'])
                days_8_ago = historical_data.iloc[-8]
                days_8_ago_close = float(days_8_ago['close'])
                days_9_ago = historical_data.iloc[-9]
                days_9_ago_close = float(days_9_ago['close'])
                days_10_ago = historical_data.iloc[-10]
                days_10_ago_close = float(days_10_ago['close'])
                #print("days_10_ago_close:",days_10_ago_close)
                close_values120 = historical_data.iloc[-126:-6]['close'].tolist()
                max_close_120 = max(close_values120)
                latest_max_120 = round(float(max_close_120 * 1.05),2)
                historical_data['SMA_5'] = historical_data['high'].rolling(window=5).mean()
                SMA_5 = float(historical_data['SMA_5'].iloc[-1])
                historical_data['SMA_Volume_5'] = historical_data['volume'].rolling(window=5).mean()
                SMA_Volume_5 = float(historical_data['SMA_Volume_5'].iloc[-1])

                historical_data['EMA_200'] = historical_data['close'].ewm(span=200, adjust=False).mean()
                historical_data['EMA_50'] = historical_data['close'].ewm(span=50, adjust=False).mean()
                historical_data['EMA_20'] = historical_data['close'].ewm(span=20, adjust=False).mean()
                historical_data['EMA_13'] = historical_data['close'].ewm(span=13, adjust=False).mean()
                EMA_200 = float(historical_data['EMA_200'].iloc[-1])
                EMA_50 = float(historical_data['EMA_50'].iloc[-1])
                EMA_20 = float(historical_data['EMA_20'].iloc[-1])
                EMA_13 = float(historical_data['EMA_13'].iloc[-1])

                # --- Weekly Candle Calculation ---
                weekly_candle = get_last_week_candle(historical_data, holidays)
                if not weekly_candle:
                    return None

                weekly_open = weekly_candle['weekly_open']
                weekly_high = weekly_candle['weekly_high']
                weekly_low = weekly_candle['weekly_low']
                weekly_close = weekly_candle['weekly_close']

                #print(f"\n📊 Custom Weekly Candle for {symbol}:")
                #print(f"Open: {weekly_open}, High: {weekly_high}, Low: {weekly_low}, Close: {weekly_close}")



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
                    "latest_max_120": latest_max_120,
                    "SMA_Volume_5": SMA_Volume_5,
                    "ema_200": EMA_200,
                    "ema_50": EMA_50,
                    "ema_20": EMA_20,
                    "ema_13": EMA_13,
                    "weekly_open": weekly_open,
                    "weekly_high": weekly_high,
                    "weekly_low": weekly_low,
                    "weekly_close": weekly_close,
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
def evaluate_buy_condition_1(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,YH15,YH15Con,OL,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60):
    try:
        if (
            ltp > weekly_high and
            open_price == low_price and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            #data["daybefore_high"] < data["yesterday_high"] and
            #data["yesterday_close"] > data["yesterday_open"]  and 
            ltp > open_price and
            #ltp >= YH15 and
            high_price < YHCon and
            ltp < YHCon

        ) or (
            ltp > weekly_high and
            Y_H_C_d < 1.5 and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            data["daybefore_high"] < data["yesterday_high"] and
            data["yesterday_close"] > data["yesterday_open"]  and 
            ltp > open_price and
            ltp >= YH15 and
            high_price < YH15Con and
            ltp < YH15Con

        ) or (
            ltp > weekly_high and
            Y_H_C_d < 1.5 and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            ltp > data["yesterday_close"] and
            data["daybefore_close"] < data["daybefore_open"] and
            data["daybefore_low"] < data["yesterday_low"] and
            ltp > open_price and
            ltp >= YH15 and
            high_price < YH15Con and
            ltp < YH15Con

        ) or (
            ltp > weekly_high and
            Y_H_C_d < 1.5 and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            ltp > data["yesterday_close"] and
            data["daybefore_high"] > data["yesterday_high"] and
            ltp > open_price and
            ltp >= YH15 and
            high_price < YH15Con and
            ltp < YH15Con
        )  or (
            
            ltp > weekly_high and
            open_price > OL and
            open_price == low_price and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            ltp > data["yesterday_close"] and
            data["yesterday_high"] > data["daybefore_high"] and
            ltp > open_price and
            ltp < YHCon
        ):
            #place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB1, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_buy_condition_1: {e}")
    return False

def evaluate_buy_condition_2(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YH3Con,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp > weekly_high and
            open_price > data["yesterday_close"] and
            data["yesterday_close"] > data["daybefore_close"] and
            data["three_days_ago_high"] > data["daybefore_high"] and
            data["three_days_ago_high"] > data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            ltp > data["three_days_ago_high"] and
            open_price < YH3Con and
            high_price < YH3Con and
            ltp < YH3Con
        ):
            #place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB2, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_buy_condition_2: {e}")
    return False

def evaluate_buy_condition_3(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp > weekly_high and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            data["yesterday_close"] > data["yesterday_open"] and
            data["yesterday_close"] > data["daybefore_open"] and
            data["daybefore_close"] < data["three_days_ago_close"] and            
            #data["yesterday_vol"] > 100000 and
            ltp > data["yesterday_high"] and
            high_price < YHCon and
            ltp < YHCon and 
            ltp > ema_200 and
            ltp > ema_20 and
            ema_20 > ema_50  and
            ltp > ema_50 and 
            ema_50 > ema_200 
        ):
            #place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB3, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_buy_condition_3: {e}")
    return False

def evaluate_buy_condition_4(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if ( 
            ltp > weekly_high and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            data["daybefore_high"] < data["yesterday_high"] and
            data["yesterday_close"] > data["yesterday_open"] and         
            #data["yesterday_vol"] > 100000 and
            high_price < YHCon and
            ltp < YHCon and
            ltp > data["yesterday_high"] #and #ORBH15              
            
            ):
            #place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB4, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_buy_condition_4: {e}")
    return False

def Intraday_Magic1(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp > open_price and
            data["yesterday_close"] <= data["yesterday_open"] and 
            data["daybefore_close"] <= data["daybefore_open"] and 
            data["three_days_ago_close"] <= data["three_days_ago_open"] and
            data["four_days_ago_close"] <= data["four_days_ago_open"] and
            #volume > data["yesterday_vol"] and
            open_price > data["yesterday_low"] and
            open_price > data["yesterday_close"] and
            ltp > data["yesterday_high"] and
            high_price < YHCon and
            ltp < YHCon
        ):            
            #place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionI1, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for Intraday_Magic1: {e}")
    return False

                

def Intraday_Magic2(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp > weekly_high and
            latest_max_5 >= latest_max_120 and 
            volume > SMA_Volume_5 and
            #volume > data["yesterday_vol"] and
            ltp > data["yesterday_close"] and
            ltp > data["yesterday_high"] and
            ltp > data["daybefore_high"] and
            ltp > data["three_days_ago_high"] and
            ltp > data["yesterday_high"] and #ORBH15 and
            ltp > open_price     

        ):            
            #place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionI2, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for Intraday_Magic2: {e}")
    return False

def evaluate_sell_condition1(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,OH,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp < weekly_low and
            Y_C_L_d < 1.5 and
            data["yesterday_close"] < data["yesterday_open"] and 
            open_price >= data["yesterday_low"] and
            open_price <= data["yesterday_close"] and
            high_price <= data["yesterday_close"] and 
            ltp < data["yesterday_low"]  and
            low_price > YLCon and
            ltp > YLCon
        ) or (
            ltp < weekly_low and
            Y_H_C_d < 1.5 and
            open_price == high_price and
            data["yesterday_close"] > data["yesterday_open"] and
            open_price <= data["yesterday_high"] and
            open_price >= data["yesterday_close"] and
            ltp < data["yesterday_close"] and 
            ltp > YCCon
        ) or (
            ltp < weekly_low and
            Y_C_L_d < 1.5 and
            open_price == high_price and
            data["yesterday_close"] < data["yesterday_open"] and 
            open_price >= data["yesterday_low"] and
            open_price <= data["yesterday_close"] and
            ltp < data["yesterday_low"]  and
            low_price > YLCon and
            ltp > YLCon
        )  or (
            ltp < weekly_low and
            open_price == high_price and
            open_price <= OH and
            #data["yesterday_close"] < data["yesterday_open"] and 
            open_price >= data["yesterday_low"] and
            #open_price <= data["yesterday_close"] and
            ltp < data["yesterday_low"]  and
            low_price > YLCon and
            ltp > YLCon
        ):
            #place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS1, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_sell_condition1: {e}")
    return False
  

def evaluate_sell_condition2(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp < weekly_low and
            open_price == high_price and
            data["daybefore_close"] < data["daybefore_open"] and
            data["yesterday_close"] < data["yesterday_open"] and
            data["yesterday_low"] < data["daybefore_low"] and
            open_price > YLCon and
            ltp < data["yesterday_low"] and
            low_price > YLCon and         
            ltp > YLCon 
        ) or (
            ltp < weekly_low and
            open_price > data["yesterday_low"] and
            data["daybefore_close"] < data["daybefore_open"] and
            data["yesterday_close"] < data["yesterday_open"] and
            data["yesterday_low"] < data["daybefore_low"] and
            open_price > YLCon and
            ltp < data["yesterday_low"] and
            low_price > YLCon and         
            ltp > YLCon 
        ):
            #place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS2, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_sell_condition2: {e}")
    return False
  
            
def evaluate_sell_condition3(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp < weekly_low and
            open_price == high_price and
            data["daybefore_close"] > data["daybefore_open"] and
            data["three_days_ago_close"] > data["three_days_ago_open"] and
            data["yesterday_close"] <= data["yesterday_open"] and
            data["yesterday_close"] > data["daybefore_close"] and
            ltp < data["yesterday_low"] and
            open_price > YLCon and
            ltp > YLCon and
            low_price > YLCon
             
        ):
            #place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS3, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_sell_condition13: {e}")
    return False
    
            
def evaluate_sell_condition4(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp < weekly_low and
            open_price >= data["yesterday_open"] and
            open_price <= data["yesterday_high"] and
            data["yesterday_close"] < data["yesterday_open"] and
            data["yesterday_close"] < data["daybefore_close"] and
            data["daybefore_close"] < data["three_days_ago_close"] and
            ltp < data["yesterday_low"] and
            ltp > YLCon
        ):
            #place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS4, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close )
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_sell_condition4: {e}")
    return False
  

def evaluate_buy_condition_ORB60(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            #Y_H_C_d < 1.5 and
            open_price > data["yesterday_close"] and  #june 1
            open_price < data["yesterday_high"] and   #june 1
            ltp > data["yesterday_close"] and
            ltp > data["yesterday_high"] and 
            ltp > weekly_high and
            ltp > open_price and
            ltp > high_values60  and 
            ltp < high_values60Con 

        ):            
            #place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionI2, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60 )
            #print("-> -> -> -> -> -> ->          evaluate_buy_condition_ORB60        -> -> -> -> -> -> -> -> -> -> -> -> ->-> -> -> -> -> -> -> ->-> -> -> -> -> -> -> ->")
            #print(f" {symbol_name}: high_values60: {high_values60}, low_values60: {low_values60}, day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp},Volume:{volume} ")
            #print(f"Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close},yesterday_vol:{yesterday_vol}")
            #print(f"weekly_open: {weekly_open}, weekly_high: {weekly_high}, weekly_low: {weekly_low}, weekly_close: {weekly_close}")
        
            return True
    except KeyError as e:
        print(f"Missing data for Intraday_Magic2: {e}")
    return False

def evaluate_Sell_condition_ORB60(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YCCon,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60,high_values60Con,low_values60Con):
    try:
        if (
            ltp < weekly_low and
            #Y_C_L_d < 1.5 and
            open_price >= data["yesterday_low"] and
            open_price <= data["yesterday_close"] and
            data["yesterday_close"] < data["yesterday_open"] and
            data["yesterday_close"] < data["daybefore_close"] and
            data["daybefore_close"] < data["three_days_ago_close"] and
            ltp < data["yesterday_low"] and
            ltp < low_values60 and
            ltp > low_values60Con 
        ):
            #place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS4, yesterday_vol,weekly_open,weekly_high,weekly_low,weekly_close,low_values60,high_values60 )
            #print("-> -> -> -> -> -> -> -> -> ->         evaluate_Sell_condition_ORB60              -> -> -> -> -> -> -> -> -> ->-> -> -> -> -> -> -> ->-> -> -> -> -> -> -> ->")
            #print(f" {symbol_name}: high_values60: {high_values60}, low_values60: {low_values60}, day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp},Volume:{volume} ")
            #print(f"Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close},yesterday_vol:{yesterday_vol}")
            #print(f"weekly_open: {weekly_open}, weekly_high: {weekly_high}, weekly_low: {weekly_low}, weekly_close: {weekly_close}")
        
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_sell_condition4: {e}")
    return False


# Feed data callback
def feed_data(message):
    global tick_data, orbh_values, orbl_values, orders_placed, ltp_by_symbol
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
            ltp_by_symbol[symbol_name] = ltp # Store the latest LTP for the symbol

            
            historical_data = fetch_and_filter_symbol(symbol_name)
            #print("historical_data:", historical_data,"\n")
            if historical_data:
            #if isinstance(historical_data, pd.DataFrame) and not historical_data.empty:
                
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
                latest_max_120 = historical_data.get("latest_max_120")
                SMA_Volume_5 = historical_data.get("SMA_Volume_5")

                weekly_open = historical_data.get("weekly_open", 0.0)
                weekly_high = historical_data.get("weekly_high", 0.0)
                weekly_low = historical_data.get("weekly_low", 0.0)
                weekly_close = historical_data.get("weekly_close", 0.0)

                ema_200 = round(historical_data.get("ema_200"),2)
                ema_50 = round(historical_data.get("ema_50"),2)
                ema_20 = round(historical_data.get("ema_20"),2)
                ema_13 = round(historical_data.get("ema_13"),2)
                conditionB1    = "BUY Condition 1"
                conditionB2    = "BUY Condition 2"
                conditionB3    = "BUY Condition 3"
                conditionB4    = "BUY Condition 4"
                conditionI1   = "Intraday_Magic 1"
                #conditionI2   = "Intraday_Magic 2"
                conditionI2   = "BUY ORB60"
                conditionSB    = "Shortterm_Breakout"
                conditionS1    = "SELL Condition 1"
                conditionS2    = "SELL Condition 2"
                conditionS3    = "SELL Condition 3"
                #conditionS4    = "SELL Condition 4"
                conditionS4    = "SELL ORB60"
                latest_max5 = max( float(yesterday_close),
                                    float(daybefore_close),
                                    float(three_days_ago_close),
                                    float(four_days_ago_close),
                                    float(five_days_ago_close)
                                    )
                latest_max_5 = round(float(latest_max5),2)
               
                historical_data_one = alice.get_historical(instrument=instrument, from_datetime=from_datetime1, to_datetime=to_datetime1, interval="1", indices=False )
                if isinstance(historical_data_one, pd.DataFrame) and not historical_data_one.empty: 
                    low_values = historical_data_one.iloc[:60]['low'].tolist()
                    high_values = historical_data_one.iloc[:60]['high'].tolist()

                    if low_values and high_values:  # Ensure lists are not empty 
                        low_values60 = min(low_values)
                        high_values60 = max(high_values)
                    else:
                        print(f"[ERROR] Empty high/low values list for {symbol_name}")
                        return
                else:
                    print(f"[ERROR] Invalid or empty historical data for {symbol_name}")
                    return

                gap_percent = ((high_values60 - low_values60) / low_values60) * 100
                gap_percent1 = float(gap_percent)

                #print(f" {symbol_name} : {gap_percent1}")
    
                Y_H_C_d = round(float(((yesterday_high - yesterday_close) / yesterday_high) * 100),2)  # yest high and yest close gap percentage
                Y_H_L_d = round(float(((yesterday_high - yesterday_low) / yesterday_high) * 100),2)    # yest high and yest low  candle percentage
                Y_C_L_d = round(float(((yesterday_close - yesterday_low) / yesterday_close) * 100),2)  # yest low and yest close gap percentage

                #YHCon = yesterday_high + 5
                YLCon = yesterday_low - (yesterday_low * 0.0045)
                YHCon = yesterday_high + (yesterday_high * 0.0045)

                low_values60Con = low_values60 - (low_values60 * 0.0035)
                high_values60Con = high_values60 + (high_values60 * 0.0035)

                YCCon = yesterday_close - (yesterday_close * 0.0045)             # yesterday close for sellcondition1
                #YH3Con = three_days_ago_high + 5
                YH3Con = three_days_ago_high + (three_days_ago_high * 0.0045)
                #YH15 = yesterday_high + (yesterday_high * 0.011)
                YH15 = yesterday_high + (yesterday_high * 0.00001)
                YH15Con = YH15 + (YH15 * 0.0045)
                OL = yesterday_close - (yesterday_close * 0.0045)
                OH = yesterday_close + (yesterday_close * 0.0045)

                if (
                    open_price > yesterday_close and  #june 1
                    open_price < yesterday_high and   #june 1
                    ltp > yesterday_close and
                    ltp > yesterday_high and 
                    ltp > weekly_high and
                    ltp > open_price and
                    ltp > high_values60  #and 
                    #ltp < high_values60Con 
                    ):
                    print(f" {symbol_name} : {gap_percent1}")   

            
            

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
    now = datetime.now()
    current_time = datetime.now().time()
    global socket_opened
    socket_opened = False
    print(f"WebSocket connection closed, restarting...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    time.sleep(5)  # Short delay before restarting
    

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

def should_place_order():
    current_time = datetime.now().time()
    cutoff_time_start = datetime.strptime('10:05:55', '%H:%M:%S').time()  # After 9:21 AM
    cutoff_time_end = datetime.strptime('14:45:00', '%H:%M:%S').time()    # Before 2:50 PM

    # Check if current time is within the allowed time window
    if current_time >= cutoff_time_start and current_time <= cutoff_time_end:

        return True
    else:
        return False

# Main loop
def main_loop():
    #cancel_unnecessary_orders_thread1 = threading.Thread(target=cancel_unnecessary_orders1, name="cancel_unnecessary_orders")
    #cancel_unnecessary_orders_thread1.start()
    
    global socket_opened, terminate_websocket, terminate_square_off

    with websocket_lock:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        file_name = 'nifty_500_symbols.txt'
        file_path = os.path.join(current_directory, file_name)
        symbols = load_symbols_from_txt(file_path)
        create_subscription_list(symbols)

        try:
            while not terminate_program:
                now = datetime.now()
                current_time = datetime.now().time()
                market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)

                if now >= market_close_time:
                    print(f"Market hours over. Exiting...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                    terminate_square_off = True  # Signal threads to terminate
                    time.sleep(2)  # Allow threads to exit
                    sys.exit(0)  # Exit cleanly
                
                if not should_place_order():
                    print("It's NOT a Shopping zone before 10:05:55 AM and after 14:45 PM. Sleeping for 60 seconds.")
                    if not terminate_square_off:
                        square_off_thread_instance = threading.Thread(target=square_off_thread, daemon=True, name="square_off_thread")
                        square_off_thread_instance.start()
                        cancel_unnecessary_orders_thread1 = threading.Thread(target=cancel_unnecessary_orders, name="cancel_unnecessary_orders")
                        cancel_unnecessary_orders_thread1.start()
                    time.sleep(60)  # Sleep for 60 seconds before rechecking
                    continue  # Recheck after sleep

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

                print(f"WebSocket is open. Staying active for 3 mins...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                start_time = time.time()

                # Keep the WebSocket open for 4 minutes
                while time.time() - start_time < 240:
                    if terminate_websocket or terminate_program:
                        break
                    time.sleep(1)  # Sleep to avoid high CPU usage

                print(f"Finished processing all symbols. Restarting...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]\n")
                time.sleep(2)

                print("Stopping WebSocket after 3 mins...")
                alice.stop_websocket()

                # Reset termination flag
                terminate_websocket = False

                # Wait before restarting
                print(f"Sleeping for 30 seconds before restarting...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                time.sleep(30)  

        except KeyboardInterrupt:
            signal_handler(None, None)  # Call signal handler manually

        finally:
            print("Cleaning up resources before exit...")
            os._exit(0)  # Force exit in case of hanging threads
            


# Function to handle Ctrl+C
def signal_handler(sig, frame):
    global terminate_program, terminate_websocket, terminate_square_off, cancel_unnecessary
    print("\nReceived Ctrl+C. Exiting program...")
    terminate_program = True
    terminate_websocket = True
    terminate_square_off = True
    cancel_unnecessary = True
    os._exit(0)  # Use os._exit(0) for an immediate exit
    sys.exit(0)  # Ensure the script exits completely

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# Entry point
if __name__ == "__main__":
    while True:
        try:
            main_loop()
        except Exception as e:
            print(f"Critical error in main loop: {e}, restarting...")
            time.sleep(5)  # Pause before restarting