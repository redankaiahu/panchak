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
sq_start_time1 = datetime_time(11, 5)
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
executor1 = ThreadPoolExecutor(max_workers=10)  # Initialize globally

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
    file_name = today.strftime("orders_placed_%d-%m-%Y.pkl")
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
    current_time1 = datetime.now().time()
    try:
        # Fetch all open orders
        open_orders = alice.get_order_history('')
        #print("Open Orders:", open_orders)
        # Fetch netwise positions
        net_position = alice.get_netwise_positions()
        open_positions = Alice_Wrapper.open_net_position(net_position)
        # Extract symbols with open positions
        open_position_symbols = {pos['Symbol'] for pos in open_positions}

        # Track symbols with valid 'open' or 'trigger pending' orders
        valid_orders = {}

        for order in open_orders:
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
                    print(f"=================>>>>>>>>>>>>  Cancelled order {order_id} for symbol {symbol}: {cancel_response} TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")

        #print("Order cleanup complete.")
        time.sleep(600)

    except Exception as e:
        print(f"Error while canceling orders: {e}")

def cancel_unnecessary_orders1():
    current_time1 = datetime.now().time()
    global cancel_unnecessary
    try:
        while not cancel_unnecessary:
            #current_time1 = datetime.now().time()

            if sq_start_time1 <= current_time1 <= sq_end_time1:
                #print(f"Calling cancel_unnecessary_orders function TIME:{current_time1}")
                #print(f" Calling cancel_unnecessary_orders... [{datetime.now()}]")
                print(f"=================>>>>>>>>>>>>  Calling cancel_unnecessary_orders... [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")

                cancel_unnecessary_orders()

            # Sleep for a short period to avoid constant checking
            time.sleep(60)
    except Exception as e:
        print(f"Error in square_off_thread: {e}")


cancel_unnecessary_orders_thread = threading.Thread(target=cancel_unnecessary_orders1, daemon=True, name=cancel_unnecessary_orders1)
cancel_unnecessary_orders_thread.start()


# Square-Off Positions
def square_off_positions():
    global order_count
    try:
        # Get netwise positions
        net_position = alice.get_netwise_positions()
        open_positions = Alice_Wrapper.open_net_position(net_position)
        #MIS = None

        if open_positions:
            for pos in open_positions:
                symbol = pos['Symbol']
                qty = int(pos['Netqty'])
                product_code = pos['Pcode']
                abs_qty = abs(qty)
                # Map the product code to the ProductType enum
                product_type = None
                if product_code == "MIS":
                    product_type = ProductType.Intraday
                elif product_code == "CO":
                    product_type = ProductType.CoverOrder
                elif product_code == "BO":
                    product_type = ProductType.BracketOrder

                if product_type:  # Ensure product_type is valid
                    #print("ProductType:", product_type)
                    #transaction_type = (TransactionType.Sell if qty > 0 else TransactionType.Buy)
                    transaction_type = (TransactionType.Sell if -100 <= qty <= 100 and qty > 0 else (TransactionType.Buy if -100 <= qty <= 100 else ValueError("Quantity must be between -100 and 100")))


                    #transaction_type = TransactionType.Sell if qty > 0 else TransactionType.Buy                  

                    try:
                        square_off_response = alice.place_order(
                        transaction_type=transaction_type,
                        instrument=alice.get_instrument_by_symbol('NSE', symbol),
                        quantity=abs_qty,
                        order_type=OrderType.Market,
                        product_type=product_type,#ProductType.Intraday,
                        price=0.0,
                        trigger_price=0.0,
                        stop_loss=None,
                        square_off=None,
                        trailing_sl=None,
                        is_amo=False,
                        order_tag='square_off'
                        )
                        print(f"=================>>>>>>>>>>>>  Square-off order placed for {symbol}: {abs_qty} shares")
                        print("=================>>>>>>>>>>>>  Order Response:", square_off_response)
                    except Exception as e:
                        print(f"Error placing square-off order for {symbol}: {e}")                
                        continue

                try:
                    # Cancel open or pending orders for the same symbol
                    open_orders = alice.get_order_history('')
                    for order in open_orders:
                        if order['Sym'] == symbol and order['Status'].lower() in ['open', 'trigger pending']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            cancel_response = alice.cancel_order(order_id)
                            print(f"=================>>>>>>>>>>>>  Cancelled order {order_id} for {symbol}. Response: {cancel_response}")
                            
                        if order['Sym'] == symbol and order['Status'].lower() in ['open']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            cancel_response = alice.cancel_order(order_id)

                        if order['Sym'] == symbol and order['Status'].lower() in ['trigger pending']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            cancel_response = alice.cancel_order(order_id)
                        #print("\n")    
                except Exception as e:
                    print(f"Error canceling orders for {symbol}: {e}")
        else:
            print("=================>>>>>>>>>>>>  No open positions to square off.")
    except Exception as e:
        print(f"Error in square_off_positions: {e}")

# Square-Off Thread
def square_off_thread():
    current_time1 = datetime.now().time()
    global terminate_square_off
    try:
        while not terminate_square_off:
            #current_time1 = datetime.now().time()

            if sq_start_time <= current_time1 <= sq_end_time:
                print("=================>>>>>>>>>>>>  Calling square off positions function")
                square_off_positions()

            # Sleep for a short period to avoid constant checking
            time.sleep(60)
    except Exception as e:
        print(f"Error in square_off_thread: {e}")

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
def evaluate_buy_condition_1(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
            Y_H_C_d < 0.76 and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            data["daybefore_high"] < data["yesterday_high"] and
            data["yesterday_close"] > data["yesterday_open"]  and 
            ltp > open_price and
            high_price < YHCon and
            ltp < YHCon
        ):
            place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB1, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def evaluate_buy_condition_2(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YH3Con):
    try:
        if (
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
            place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB2, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def evaluate_buy_condition_3(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            data["yesterday_close"] > data["yesterday_open"] and
            data["yesterday_close"] > data["daybefore_open"] and
            data["daybefore_close"] < data["three_days_ago_close"] and            
            data["yesterday_vol"] > 100000 and
            ltp > data["yesterday_high"] and
            high_price < YHCon and
            ltp < YHCon and 
            ltp > ema_200 and
            ltp > ema_20 and
            ema_20 > ema_50  and
            ltp > ema_50 and 
            ema_50 > ema_200 
        ):
            place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB3, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def evaluate_buy_condition_4(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if ( 
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            data["daybefore_high"] < data["yesterday_high"] and
            data["yesterday_close"] > data["yesterday_open"] and         
            data["yesterday_vol"] > 100000 and
            high_price < YHCon and
            ltp < YHCon and
            ltp > data["yesterday_high"] #and #ORBH15              
            
            ):
            place_order_with_prevention( instrument, TransactionType.Buy,  ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionB4, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def Intraday_Magic1(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
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
            place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionI1, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

                

def Intraday_Magic2(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
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
            place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionI2, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def evaluate_sell_condition1(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
            Y_C_L_d < 0.75 and
            data["yesterday_close"] < data["yesterday_open"] and 
            open_price > data["yesterday_low"] and
            open_price < data["yesterday_close"] and
            high_price <= data["yesterday_close"] and 
            ltp < data["yesterday_low"]  and
            low_price > YLCon and
            ltp > YLCon
        ):
            place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS1, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for sell condition: {e}")
    return False
  

def evaluate_sell_condition2(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
            data["daybefore_close"] < data["daybefore_open"] and
            data["yesterday_close"] < data["yesterday_open"] and
            open_price == high_price and
            data["yesterday_low"] < data["daybefore_low"] and
            open_price > YLCon and
            ltp < data["yesterday_low"] and
            low_price > YLCon and         
            ltp > YLCon 
        ):
            place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS2, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for sell condition: {e}")
    return False
  
            
def evaluate_sell_condition3(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
            open_price == high_price and
            data["daybefore_close"] > data["daybefore_open"] and
            data["three_days_ago_close"] > data["three_days_ago_open"] and
            data["yesterday_close"] <= data["yesterday_open"] and
            data["yesterday_close"] > data["daybefore_close"] and
            open_price > YLCon and
            ltp > YLCon and
            low_price > YLCon and
            ltp < data["yesterday_low"] 
        ):
            place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS3, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for sell condition: {e}")
    return False
    
            
def evaluate_sell_condition4(data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4):
    try:
        if (
            open_price > data["yesterday_open"] and
            open_price < data["yesterday_high"] and
            data["yesterday_close"] < data["yesterday_open"] and
            data["yesterday_close"] < data["daybefore_close"] and
            data["daybefore_close"] < data["three_days_ago_close"] and            
            data["yesterday_vol"] > 100000 and
            open_price > YLCon and
            ltp > YLCon and
            low_price > YLCon and
            ltp < data["yesterday_low"]  
            
        ):
            place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,conditionS4, yesterday_vol )
            return True
    except KeyError as e:
        print(f"Missing data for sell condition: {e}")
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

                ema_200 = round(historical_data.get("ema_200"),2)
                ema_50 = round(historical_data.get("ema_50"),2)
                ema_20 = round(historical_data.get("ema_20"),2)
                ema_13 = round(historical_data.get("ema_13"),2)
                conditionB1    = "BUY Condition 1"
                conditionB2    = "BUY Condition 2"
                conditionB3    = "BUY Condition 3"
                conditionB4    = "BUY Condition 4"
                conditionI1   = "Intraday_Magic 1"
                conditionI2   = "Intraday_Magic 2"
                conditionSB    = "Shortterm_Breakout"
                conditionS1    = "SELL Condition 1"
                conditionS2    = "SELL Condition 2"
                conditionS3    = "SELL Condition 3"
                conditionS4    = "SELL Condition 4"
                latest_max5 = max( float(yesterday_close),
                                    float(daybefore_close),
                                    float(three_days_ago_close),
                                    float(four_days_ago_close),
                                    float(five_days_ago_close)
                                    )
                latest_max_5 = round(float(latest_max5),2)
               
                historical_data_one = alice.get_historical(instrument=instrument, from_datetime=from_datetime1, to_datetime=to_datetime1, interval="1", indices=False )
                

                Y_H_C_d = round(float(((yesterday_high - yesterday_close) / yesterday_high) * 100),2)  # yest high and yest close gap percentage
                Y_H_L_d = round(float(((yesterday_high - yesterday_low) / yesterday_high) * 100),2)    # yest high and yest low  candle percentage
                Y_C_L_d = round(float(((yesterday_close - yesterday_low) / yesterday_close) * 100),2)  # yest low and yest close gap percentage

                #YHCon = yesterday_high + 5
                YLCon = yesterday_low - 5
                YHCon = yesterday_high + (yesterday_high * 0.0075)
                YH3Con = three_days_ago_high + 5
            
            open_orders = alice.get_order_history('')
            #print(f" in alice.get_order_history")
            if not open_orders:
                print("No orders found. Skipping processing.")
            else:
                orders_by_symbol = {}
                # Filter and organize orders
                for order in open_orders:
                    #print(f" in order in open_orders")
                    if not isinstance(order, dict):  # Validate individual orders
                        #print(f"Invalid order format: {order}")
                        continue
                    symbol = order.get('Sym')
                    status = order.get('Status', '').lower()
                    sl_order_id = order.get('OrderID')
                    qty = order.get('Qty')

                    # Track only relevant statuses
                    if status in ['open', 'trigger pending', 'complete']:
                        if symbol not in orders_by_symbol:
                            orders_by_symbol[symbol] = {
                                'open': None,
                                'trigger_pending': None,
                                'complete': []
                            }

                        # Assign to correct status
                        if status == 'open' and not orders_by_symbol[symbol]['open']:
                            orders_by_symbol[symbol]['open'] = {
                                'Prc': order.get('Prc'),
                                'OrderID': order.get('Nstordno'),
                                'Qty': qty
                            }
                        elif status == 'trigger pending' and not orders_by_symbol[symbol]['trigger_pending']:
                            orders_by_symbol[symbol]['trigger_pending'] = {
                                'Trgprc': order.get('Trgprc'),
                                'Prc': order.get('Prc'),
                                'OrderID': order.get('Nstordno'),
                                'Qty': qty
                            }
                        elif status == 'complete':
                            orders_by_symbol[symbol]['complete'].append({
                                'Avgprc': order.get('Avgprc'),
                                'Trantype': order.get('Trantype')
                            })

            
                # Pass extracted data to another function
                for symbol, details in orders_by_symbol.items():
                    #print(f" in symbol, details in orders_by_symbol.items")
                    if details['open'] and details['trigger_pending'] and details['complete']:
                        target_price = details['open']['Prc']
                        stoploss_price = details['trigger_pending']['Prc']
                        trigger_price = details['trigger_pending']['Trgprc']
                        sl_order_id = details['trigger_pending']['OrderID']
                        qty = details['trigger_pending']['Qty']

                        executed_price = details['complete'][0]['Avgprc']
                        trantype = details['complete'][0]['Trantype']
                        if symbol in ltp_by_symbol:
                            ltp1 = ltp_by_symbol[symbol]
                            if printed_symbols.get(symbol) != ltp1:
                                #print(f"Symbol: {symbol},ltp:{ltp1},trantype:{trantype},executed_price:{executed_price},target:{target_price},stoploss:{stoploss_price},trigger:{trigger_price}\n")
                                executor1.submit( modify_stoploss_orders_data,symbol, ltp1, stoploss_price, target_price, trigger_price,executed_price, trantype, sl_order_id, qty )
                                #modify_stoploss_orders_data(symbol, ltp1, stoploss_price, target_price, trigger_price,executed_price, trantype, sl_order_id, qty )
                                printed_symbols[symbol] = ltp1

            try:
                with ThreadPoolExecutor(max_workers=350) as executor:
                    #print("in  ThreadPoolExecutor loop")
                    futures = [
                        executor.submit(evaluate_buy_condition_1,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(evaluate_buy_condition_2,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4,YH3Con),
                        executor.submit(evaluate_buy_condition_3,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(Intraday_Magic1,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(Intraday_Magic2,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(evaluate_sell_condition1,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(evaluate_sell_condition2,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(evaluate_sell_condition3,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4),
                        executor.submit(evaluate_sell_condition4,historical_data,ltp,open_price,high_price,low_price,volume, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,Y_H_C_d,Y_H_L_d,Y_C_L_d,YHCon,YLCon,latest_max_5,latest_max_120,SMA_Volume_5, ema_200, ema_50, ema_20, ema_13,conditionB1,conditionB2,conditionB3,conditionB4,conditionI1,conditionI2,conditionSB,conditionS1,conditionS2,conditionS3,conditionS4)
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
            
def modify_stoploss_orders_data(symbol, ltp1, stoploss_price, target_price, trigger_price,executed_price, trantype, sl_order_id, qty ):
    global modified_symbols
    # Check if the symbol's SL is already modified
    if symbol in modified_symbols:
     #   #print(f"SL already modified for {symbol}. Skipping further modifications.")
      #  return
        last_modified_threshold = modified_symbols[symbol]
        if (trantype == "S" and ltp > last_modified_threshold) or (trantype == "B" and ltp < last_modified_threshold):
        # Skip if no higher threshold is reached
            return
    try:
        # Convert values to float to avoid type issues
        executed_price = round(float(executed_price),2)
        ltp = round(float(ltp1),2)
        target_price = round(float(target_price),2)
        stoploss_price = round(float(stoploss_price),2)
        trigger_price = round(float(trigger_price),2)
        symbol = symbol
        qty = int(qty)
        sl_order_id = str(sl_order_id)
        instrument = alice.get_instrument_by_symbol("NSE", symbol)
        if not instrument:
            raise ValueError(f"Invalid instrument fetched for symbol {symbol}.")
        transaction_type = TransactionType.Sell if trantype == "S" else TransactionType.Buy
        move_1_percent = executed_price * 0.01
        move_2_percent = executed_price * 0.02
        move_3_percent = executed_price * 0.03
        if trantype == "S":  # Sell Case
            # Adjust stop-loss for 2% and 3% downward movement
            if ltp <= executed_price - move_1_percent:
                new_stop_loss = executed_price - (executed_price * 0.005)  # Move to 1% above executed price
                new_trigger_price = new_stop_loss - 0.30
            elif ltp <= executed_price - move_2_percent:
                new_stop_loss = executed_price - (executed_price * 0.01)  # Move to 1% above executed price
                new_trigger_price = new_stop_loss - 0.30
            elif ltp <= executed_price - move_3_percent:
                new_stop_loss = executed_price - (executed_price * 0.02)  # Move to 2% above executed price
                new_trigger_price = new_stop_loss - 0.30
            else:
                return  # No change required for current LTP
        else:  # Buy Case
            # Adjust stop-loss for 2% and 3% upward movement
            if ltp >= executed_price + move_1_percent:
                new_stop_loss = executed_price + (executed_price * 0.005)  # Move to 1% above executed price
                new_trigger_price = new_stop_loss + 0.30
            elif ltp >= executed_price + move_2_percent:
                new_stop_loss = executed_price + (executed_price * 0.01)  # Move to 1% above executed price
                new_trigger_price = new_stop_loss + 0.30
            elif ltp >= executed_price + move_3_percent:
                new_stop_loss = executed_price + (executed_price * 0.02)  # Move to 2% above executed price
                new_trigger_price = new_stop_loss + 0.30
            else:
                return  # No change required for current LTP

        # Round to two decimals for price consistency
        #new_stop_loss = round(new_stop_loss, 2)
        #new_trigger_price = round(new_trigger_price, 2)
        TICK_SIZE = 0.05
        new_stop_loss = round_to_tick(new_stop_loss, TICK_SIZE)
        new_trigger_price = round_to_tick(new_trigger_price, TICK_SIZE)
    
        if (trantype == "S" and ltp < executed_price) or (trantype == "B" and ltp > executed_price):
            try:
                modify_order_response = alice.modify_order(
                transaction_type=transaction_type,
                instrument=instrument,
                order_id=sl_order_id,
                quantity=qty,
                order_type=OrderType.StopLossLimit,
                product_type=ProductType.Intraday,
                price=new_stop_loss,
                trigger_price=new_trigger_price
                )

                print(f"======>> SL order modified successfully for {symbol}: {modify_order_response}\n")
                if modify_order_response.get('stat') == 'Ok':
                    #modified_symbols[symbol] = sl_order_id
                    if trantype == "S":
                        modified_symbols[symbol] = executed_price - move_2_percent  # Use the latest threshold
                    else:
                        modified_symbols[symbol] = executed_price + move_2_percent
                    #print(f"Stop-loss successfully modified for {symbol}. Updated threshold in modified_symbols.")
            except Exception as e:
                print(f"Error while processing orders: {e}")    
    except Exception as e:
            print(f"Error while processing orders: {e}") 



# Monitor SL and Target Orders
def monitor_orders(sl_order_id, target_order_id):
    current_time1 = datetime.now().time()
    try:
        while True:
        # Monitor Stop Loss Order
            sl_order_status = alice.get_order_history(sl_order_id)
            symbol = sl_order_status.get('Sym')
            if sl_order_status and sl_order_status['Status'].lower() == 'complete':
                print(f"=================>>>>>>>>>>>>  {symbol} Stop Loss order executed. Canceling Target order: {target_order_id} - TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                cancel_response = alice.cancel_order(target_order_id)
                print(f"=================>>>>>>>>>>>>  Target order cancellation response: {cancel_response}")
                break

            # Monitor Target Order
            target_order_status = alice.get_order_history(target_order_id)
            symbol = target_order_status.get('Sym')
            if target_order_status and target_order_status['Status'].lower() == 'complete':
                print(f"=================>>>>>>>>>>>>  {symbol} Target order executed. Canceling Stop Loss order: {sl_order_id} - TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                cancel_response = alice.cancel_order(sl_order_id)
                print(f"=================>>>>>>>>>>>>  Stop Loss order cancellation response: {cancel_response}")
                break

        # Pause briefly before rechecking
            time.sleep(2)
    except Exception as e:
        print(f"Error while monitoring orders (SL ID: {sl_order_id}, Target ID: {target_order_id}): {e}")



def round_to_tick(price, tick_size):
    if price % tick_size == 0:
        return price  # Already aligned with the tick size
    
    # Round up (ceil) or down (floor) based on the decimal part
    if price > 0:
        return math.ceil(price / tick_size) * tick_size
    else:
        return math.floor(price / tick_size) * tick_size
def round_to_two_decimals(value):
    return round(value, 2)

def place_order_with_prevention( instrument, transaction_type, price,  symbol_name,open_price,high_price,low_price,volume,yesterday_open,yesterday_high,yesterday_low,yesterday_close,condition, yesterday_vol ):   
    global orders_placed, order_tracking, order_count, sl_modify_tracking
    current_time1 = datetime.now().time()
    order_id = f"{transaction_type}_{symbol_name}"
    # Load the persisted orders on each function call (optional - can be loaded once at startup)
    load_orders_placed()
    
    if order_id in orders_placed:
        #print(f"=================>>>>>>>>>>>>  Duplicate order prevented for {symbol_name} TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        return
    if order_count >= 12:
       #print(f" orders are reached to {order_count} level")
       print(f"=======>>>>Order count limit reached -- not placing order -- {condition} - {transaction_type} - {symbol_name} at {price} -  Time: [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] \n")
       return


    try:
        # Calculate stop_loss based on transaction type
        price = float(price)  # Convert to float
        ltp = float(price)
        symbol = symbol_name
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Determine quantity based on price
        if price < 500:
            quantity = 1
        elif 500 <= price <= 1000:
            quantity = 1
        elif 1000 < price <= 2000:
            quantity = 1
        else:  # price > 2000
            quantity = 1

        #print(f"\nPlacing  {transaction_type} order for {symbol_name} at LTP:{ltp} - TIME: [{current_time}]")
        #print("-> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> ->")
        origOrderId = alice.place_order(
                    transaction_type=transaction_type,
                    instrument=instrument,
                    quantity=quantity,
                    order_type=OrderType.Market,#OrderType.StopLossMarket,
                    product_type=ProductType.Intraday,  # ProductType.CoverOrder,
                    price=0.0,
                    trigger_price=None,#float(5),  # 5.0,
                    stop_loss=None,
                    square_off=None,
                    trailing_sl=None,
                    is_amo=False,
                    order_tag='order_id'  # 'BUY_ORDER'
        )
        
        time.sleep(5)  # Add a delay if required for execution to complete
        order_number = origOrderId.get('NOrdNo')
        print("-> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> -> ->-> -> -> -> -> -> -> ->-> -> -> -> -> -> -> ->")
        print(f" - {condition} - Order placed for {symbol_name}: {transaction_type} at {price} - origOrderId: {order_number} at Time: ... [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ")
        print(f" day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp},Volume:{volume} ")
        print(f"Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close},yesterday_vol:{yesterday_vol}")
        
        if not order_number:
            print("=======>>>> Order placement failed, no order number found.\n")
            return
        #print(f"Order placed for {symbol_name}: {transaction_type} at {price}")
        orders_placed.add(order_id)  # Record the placed order
        save_orders_placed()  # Save the updated orders_placed set to the file
        #order_count += 1
        order_history = alice.get_order_history(order_number)  # Get the order history using the order number
        executed_price = float(order_history['Avgprc'])  # Extract and convert average price to float
        qty = int(order_history['Fillshares'])  # Extract and convert filled shares to integer
        TICK_SIZE = 0.05
        # Determine SL and target based on transaction type
        if transaction_type == TransactionType.Buy:
            #stop_loss_price_call = executed_price * (1 - 0.0075)
            stop_loss_price_call = executed_price * (1 - 0.017)
            stop_loss_price_cal = math.floor(stop_loss_price_call * 10) / 10
            stop_loss_price = round_to_two_decimals(stop_loss_price_cal)
            trigger_price = stop_loss_price + 0.30
            
            target_price_call = executed_price * (1 + 0.03)
            target_price_cal = math.floor(target_price_call * 10) / 10
            target_price = round_to_two_decimals(target_price_cal)

            sl_transaction_type = TransactionType.Sell
        else:  # Sell order
            #stop_loss_price_call = executed_price * (1 + 0.0075)
            stop_loss_price_call = executed_price * (1 + 0.017)
            stop_loss_price_cal = math.floor(stop_loss_price_call * 10) / 10
            stop_loss_price = round_to_two_decimals(stop_loss_price_cal)
            trigger_price = stop_loss_price - 0.30

            target_price_call = executed_price * (1 - 0.03)
            target_price_cal = math.floor(target_price_call * 10) / 10
            target_price = round_to_two_decimals(target_price_cal)

            sl_transaction_type = TransactionType.Buy
                            
        sl_order_response = alice.place_order(
                        transaction_type=sl_transaction_type,  # Opposite of the market order
                        instrument=instrument,
                        quantity=qty,
                        order_type=OrderType.StopLossLimit,#OrderType.Limit,#OrderType.SL,  # Stop Loss Order
                        product_type=ProductType.Intraday,
                        price=stop_loss_price,  # SL price
                        trigger_price=trigger_price,  # Trigger price
                        stop_loss=None,
                        square_off=None,
                        trailing_sl=None,
                        is_amo=False,
                        order_tag='sl_order'  # Custom tag for the SL order
        )
        print("Stop Loss order placed:", sl_order_response)
        sl_order_id = sl_order_response.get('NOrdNo')
                

        target_order_response = alice.place_order(
                        transaction_type=sl_transaction_type,  # Opposite of the market order
                        instrument=instrument,
                        quantity=qty,
                        order_type=OrderType.Limit,#OrderType.StopLossLimit,#OrderType.Limit,#OrderType.SL,  # Stop Loss Order
                        product_type=ProductType.Intraday,
                        price=target_price,  # SL price
                        trigger_price=None,  # Trigger price
                        stop_loss=None,
                        square_off=None,
                        trailing_sl=None,
                        is_amo=False,
                        order_tag='tgt_order'  # Custom tag for the SL order
        )
        print("Target order placed:", target_order_response)
        print(f"<- <- <- <- <- <- <-    ###################     order_count: {order_count}      <- <- <- <- <- <- <- <- <- <- <- <- <-\n")
        target_order_id = target_order_response.get('NOrdNo')
                # Track these orders in the dictionary
        order_tracking[symbol_name] = {
                    'sl_order_id': sl_order_response['NOrdNo'],
                    'target_order_id': target_order_response['NOrdNo']
        }
        
        if sl_order_id:
            order_count += 1

                # Start monitoring orders
        if sl_order_id and target_order_id:
            monitor_thread = threading.Thread(target=monitor_orders, args=(sl_order_id, target_order_id))
            monitor_thread.start()
        else:
            print(f"=============>>>>{symbol_name}: No valid conditions met for placing orders.\n")
            
    except Exception as e:
        print(f"===================>>>>> Error placing order for {symbol_name}: {e}\n")

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
    cutoff_time_start = datetime.strptime('09:15:05', '%H:%M:%S').time()  # After 9:21 AM
    cutoff_time_end = datetime.strptime('14:45:00', '%H:%M:%S').time()    # Before 2:50 PM

    # Check if current time is within the allowed time window
    if current_time >= cutoff_time_start and current_time <= cutoff_time_end:

        return True
    else:
        return False

# Main loop
def main_loop():
    global socket_opened, terminate_websocket, terminate_square_off
    with websocket_lock:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        file_name = 'nifty_500_symbols.txt'
        file_path = os.path.join(current_directory, file_name)
        #file_path = r'C:\Users\Administrator\Documents\algo\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
        #file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
        symbols = load_symbols_from_txt(file_path)
  
        create_subscription_list(symbols)

        while True:
            now = datetime.now()
            current_time = datetime.now().time()
            market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
            if now >= market_close_time:
                    print(f"Market hours over. Exiting...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                    terminate_square_off = True  # Signal threads to terminate
                    time.sleep(2)  # Give threads time to exit
                    sys.exit(0)  # Exit cleanly with status 0
                    #os._exit(0)  # Forcefully exit with status 0
                    #break
            if not should_place_order():
                print("It's NOT a Shopping zone before 09:15 AM and after 14:45 PM . Sleeping for 60 seconds to recheck time stamps .","current_time:",current_time)
                if not terminate_square_off:
                    square_off_thread_instance = threading.Thread(target=square_off_thread, daemon=True, name=square_off_thread)
                    square_off_thread_instance.start()
                    cancel_unnecessary_orders_thread = threading.Thread(target=cancel_unnecessary_orders1, daemon=True, name=cancel_unnecessary_orders)
                    cancel_unnecessary_orders_thread.start()
                time.sleep(60)  # Sleep for 60 seconds before rechecking
                continue  # Recheck again after the sleep
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

            # Keep the WebSocket open for 1 hour
            while time.time() - start_time < 240:
                if terminate_websocket:
                    break
                time.sleep(1)  # Sleep for a short period to avoid high CPU usage
            # Stop WebSocket after 1 hour
            # After processing all symbols, loop back to start
            print(f"Finished processing all symbols. Restarting from the first symbol.TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]\n")
            time.sleep(2)  # Optional pause before restarting the loop

            print("Stopping WebSocket after 3 mins...")
            alice.stop_websocket()

            # Reset termination flag
            terminate_websocket = False

            # Wait for 30 seconds before restarting the WebSocket
            print(f"Sleeping for 30 seconds before restarting...TIME:[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
            time.sleep(30)  # Sleep before restarting the WebSocket session

        
# Entry point
if __name__ == "__main__":
    while True:
        try:
            main_loop()
        except Exception as e:
            print(f"Critical error in main loop: {e}, restarting...")
            time.sleep(5)  # Pause before restarting