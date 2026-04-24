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
sq_start_time = datetime_time(15, 5)
sq_end_time = datetime_time(15, 10)
current_time1 = datetime.now().time()
current_time2 = datetime.now().time()
low_values = []
high_values = []
low_values15 = []
high_values15 = []
sl_modify_tracking = {}

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
    #global orders_placed
    #orders_placed = set()  # Initialize as an empty set
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
       
def open_count_calculations():
    global order_count
    try:
        local_order_count = 0  # Local variable to calculate the count
        current_time6 = datetime.now().time()
        # Get netwise positions
        net_position = alice.get_netwise_positions()
        open_positions = Alice_Wrapper.open_net_position(net_position)
        symbols = [pos['Symbol'] for pos in open_positions] if open_positions else []
        #if open_positions:
         #   for pos in open_positions:
          #      #print(pos)
           #     symbol = pos['Symbol']
        # Cancel open or pending orders for the same symbol
        open_orders = alice.get_order_history('')
        # Parse JSON if open_orders is a string
        if isinstance(open_orders, str):
            import json
            try:
                open_orders = json.loads(open_orders)
                print("Successfully parsed open_orders.")
            except json.JSONDecodeError as e:
                print(f"Error parsing open_orders: {e}")
                return 0

        # Ensure open_orders is a list
        if not isinstance(open_orders, list):
            print(f"Unexpected open_orders type: {type(open_orders)}")
            return 0

        # Process each order
        for order in open_orders:
            if not isinstance(order, dict):
                print(f"Invalid order format: {order}")
                continue
            if 'Sym' not in order or 'Status' not in order:
                print(f"Missing keys in order: {order}")
                continue

            # Check for 'trigger pending' status
            if (
                order.get('Sym') in symbols and 
                order.get('Status', '').lower() == 'trigger pending'
            ):
                local_order_count += 1  # Increment the count for each valid order

        #print(f"Trigger Pending Orders Count: {local_order_count} - TIME: {current_time6}")
        return local_order_count

    except Exception as e:
        print(f"Error in open_count_thread: {e}")
        return 0

def periodic_open_count_calculations():
    """
    Periodically run the open_count_calculations function in a loop.
    """
    global order_count
    while True:
        order_count = open_count_calculations()
        #print(f"Open orders count: {open_count}")
        time.sleep(10)  # Run every 60 seconds

# Start the thread
thread = threading.Thread(target=periodic_open_count_calculations, daemon=True, name=periodic_open_count_calculations)
thread.start()
#monitor_thread(thread)


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
                if product_code in ["MIS", "CO", "BO"]:
                    if product_code == "MIS":
                        product_type = ProductType.Intraday
                    elif product_code == "CO":
                        product_type = ProductType.CoverOrder
                    elif product_code == "BO":
                        product_type = ProductType.BracketOrder

                if product_type:  # Ensure product_type is valid
                    print("ProductType:", product_type)
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
                        print(f"Square-off order placed for {symbol}: {abs_qty} shares")
                        print("Order Response:", square_off_response, "\n")
                    except Exception as e:
                        print(f"Error placing square-off order for {symbol}: {e}")                
                        continue

                try:
                    # Cancel open or pending orders for the same symbol
                    open_orders = alice.get_order_history('')
                    print(f"cancelling open positions after square off function ")
                    for order in open_orders:
                        if order['Sym'] == symbol and order['Status'].lower() in ['open', 'trigger pending']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            if order_id:
                                cancel_response = alice.cancel_order(order_id)
                                print(f"Cancelled order {order_id} for {symbol}. Response: {cancel_response}")
                            else:
                                print(f"No valid order ID found for {symbol} during cancellation.")
                            #cancel_response = alice.cancel_order(order_id)
                            #print(f"Cancelled order {order_id} for {symbol}. Response: {cancel_response}")

                except Exception as e:
                    print(f"Error canceling orders for {symbol}: {e}")
        else:
            print("No open positions to square off.")
    except Exception as e:
        print(f"Error in square_off_positions: {e}")

# Square-Off Thread
def square_off_thread():
    global terminate_square_off
    try:
        while not terminate_square_off:
            current_time1 = datetime.now().time()

            if sq_start_time <= current_time1 <= sq_end_time:
                print("Calling square off positions function")
                square_off_positions()

            # Sleep for a short period to avoid constant checking
            time.sleep(60)
    except Exception as e:
        print(f"Error in square_off_thread: {e}")

# Start the square-off thread in the background
square_off_thread = threading.Thread(target=square_off_thread, daemon=True, name=square_off_thread)
square_off_thread.start()
#monitor_thread(square_off_thread)

def cancel_open_trigger_pending_orders(symbol):
    print("in the function--- cancel_open_trigger_pending_orders")
    try:
        # Retrieve order history
        open_orders = alice.get_order_history('')
        
        # Track order IDs by status
        open_order_ids = []
        trigger_pending_order_ids = []
        
        # Group orders based on status
        for order in open_orders:
            if order['Sym'] == symbol:
                status = order['Status'].lower()
                order_id = order.get('Nstordno')
                if status == 'open' and order_id:
                    open_order_ids.append(order_id)
                elif status == 'trigger pending' and order_id:
                    trigger_pending_order_ids.append(order_id)
        
        # Cancel orders based on conditions
        if open_order_ids and not trigger_pending_order_ids:
            for order_id in open_order_ids:
                cancel_response = alice.cancel_order(order_id)
                print(f"Cancelled open order with ID {order_id}: {cancel_response}")
        
        if trigger_pending_order_ids and not open_order_ids:
            for order_id in trigger_pending_order_ids:
                cancel_response = alice.cancel_order(order_id)
                print(f"Cancelled trigger pending order with ID {order_id}: {cancel_response}")
        
        # If both exist, assume one is hit and cancel the other
        if open_order_ids and trigger_pending_order_ids:
            print(f"Both open and trigger pending orders exist for {symbol}. Consider manual review.")
    
    except Exception as e:
        print(f"Error processing orders for {symbol}: {e}")

def cancel_open_trigger_pending_orders1(symbol):
    cancel_open_trigger_pending_orders_thread = threading.Thread(target=cancel_open_trigger_pending_orders, args=(symbol,),  daemon=True)
    cancel_open_trigger_pending_orders_thread.start()


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
        #if isinstance(historical_data, pd.DataFrame) and not historical_data.empty and historical_data.shape[0] >= 300:
        if isinstance(historical_data, pd.DataFrame) and not historical_data.empty:
            # Ensure all relevant columns are numeric
            numeric_columns = ["open", "close", "high", "low", "volume"]
            for col in numeric_columns:
                historical_data[col] = pd.to_numeric(historical_data[col], errors="coerce")
            
            # Drop rows with any missing or invalid values
            historical_data.dropna(subset=numeric_columns, inplace=True)
            #latest_max_120 = None
            #if len(historical_data) >= 300:
                # Calculate the maximum closing price from 6 days ago over the last 120 days
             #   close_values120 = historical_data.iloc[:10]['close'].tolist()#historical_data.iloc[6:126]['close'].tolist()
              #  max_close_120 = max(close_values120)

                # Multiply by 1.05
               # latest_max_120 = max_close_120 * 1.05
            #print("latest_max_120:",latest_max_120)    

            # Ensure enough data is available for analysis
            if historical_data.shape[0] >= 3:
                # Calculate EMA values
                historical_data['EMA_200'] = historical_data['close'].ewm(span=200, adjust=False).mean()
                historical_data['EMA_50'] = historical_data['close'].ewm(span=50, adjust=False).mean()
                historical_data['EMA_20'] = historical_data['close'].ewm(span=20, adjust=False).mean()
                historical_data['EMA_13'] = historical_data['close'].ewm(span=13, adjust=False).mean()

                close_values120 = historical_data.iloc[-126:-6]['close'].tolist()
                max_close_120 = max(close_values120)
                latest_max_120 = round(float(max_close_120 * 1.05),2)
                #print("latest_max_120:",latest_max_120)

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
                    "latest_max_120": latest_max_120,
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

# open==high
def open_high(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1):
    try:
        if (
            open_price == high_price and
            ltp < ORBL1
                     
         ):
            #print(f"BUY cond 1 -- {symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
            #print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            place_order_with_prevention( instrument, TransactionType.Sell, ltp, symbol_name,condition4,open_price,high_price,low_price,volume,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ORBH1, ORBL1 )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

# open==low
def open_low(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1):
    try:
        if (
            open_price == low_price and
            ltp > ORBH1
                     
         ):
            #print(f"BUY cond 1 -- {symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
            #print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,condition5,open_price,high_price,low_price,volume,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ORBH1, ORBL1 )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False


# Evaluate buy condition 1
def evaluate_buy_condition_1(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1):
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
            #print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            #place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,condition1,open_price,high_price,low_price,volume,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ORBH1, ORBL1 )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def Intraday_Magic(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1):
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
            #print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            #place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,condition2,open_price,high_price,low_price,volume,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ORBH1, ORBL1 )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

def Shortterm_Breakout(data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1):
    try:
        if (
            latest_max_5 >= latest_max_120 and 
            volume > SMA_Volume_5 and
            ltp > data["yesterday_close"] and
            ltp > open_price     

         ):
            print(f"Shortterm_Breakout--{symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, latest_max_5: {latest_max_5}, latest_max_120: {latest_max_120}, SMA_Volume_5: {SMA_Volume_5}")
            #print(f"Y_Open: {data['yesterday_open']}, High: {data['yesterday_high']}, Low: {data['yesterday_low']}, Close: {data['yesterday_close']}, volume: {volume}, yesterday_vol:{data['yesterday_vol']}, daybefore_vol: {data['daybefore_vol']}\n")
            #place_order_with_prevention( instrument, TransactionType.Buy, ltp, symbol_name,condition3,open_price,high_price,low_price,volume,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ORBH1, ORBL1 )
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
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
            #if isinstance(historical_data, pd.DataFrame) and not historical_data.empty:
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
                latest_max_120 = historical_data.get("latest_max_120")
                condition1    = "BUY Condition 1"
                condition2    = "Intraday_Magic"
                condition3    = "Shortterm_Breakout"
                condition4    = "OPEN==HIGH"   #"BUY ORBH15"
                condition5    = "OPEN==LOW"   #"SELL ORBH15"

                latest_max5 = max( float(yesterday_close),
                                    float(daybefore_close),
                                    float(three_days_ago_close),
                                    float(four_days_ago_close),
                                    float(five_days_ago_close)
                                    )
                latest_max_5 = round(float(latest_max5),2)
               
            historical_data_one = alice.get_historical(instrument=instrument, from_datetime=from_datetime1, to_datetime=to_datetime1, interval="1", indices=False )
            # Create a list to store the first 60 rows
            #if isinstance(historical_data_one, pd.DataFrame) and not historical_data_one.empty:
            #if historical_data_one and isinstance(historical_data_one, pd.DataFrame) and not historical_data_one.empty:

            if not historical_data_one.empty:
             
                try:
                    # Extract 'low' and 'high' values for specific ranges
                    if len(historical_data_one) >= 1:
                        low_values1 = historical_data_one.iloc[:1]['low'].tolist()
                        high_values1 = historical_data_one.iloc[:1]['high'].tolist()
                        low_values = historical_data_one.iloc[:5]['low'].tolist() if len(historical_data_one) >= 5 else []
                        high_values = historical_data_one.iloc[:5]['high'].tolist() if len(historical_data_one) >= 5 else []
                        low_values15 = historical_data_one.iloc[:15]['low'].tolist() if len(historical_data_one) >= 15 else []
                        high_values15 = historical_data_one.iloc[:15]['high'].tolist() if len(historical_data_one) >= 15 else []
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
            low_values_1_one = low_values1[:1]
            high_values_1_one = high_values1[:1]
            ORBL1 = min(low_values_1_one)
            ORBH1 = max(high_values_1_one)

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

            ORBH5Con = float(ORBH5) + 3
            ORBL5Con = float(ORBL5) - 3
            


            #if evaluate_buy_condition_1(historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13):
             #   print(f"BUY cond 1 -- {symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
              #  print(f"Y_Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}, volume: {volume}, yesterday_vol: {yesterday_vol}, daybefore_vol: {daybefore_vol},ema_200:{ema_200},ema_50:{ema_50},ema_20:{ema_20},ema_13:{ema_13}\n")
            #elif Intraday_Magic(historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13):
             #   print(f"Intraday_Magic--{symbol_name}: day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
              #  print(f"Y_Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}, volume: {volume}, yesterday_vol:{yesterday_vol}, daybefore_vol: {daybefore_vol}\n")
            try:
                with ThreadPoolExecutor(max_workers=60) as executor:
                    futures = [
                        executor.submit(open_high,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1),
                        executor.submit(open_low,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1), 
                        executor.submit(evaluate_buy_condition_1,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1),
                        executor.submit(Intraday_Magic,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1),
                        executor.submit(Shortterm_Breakout,historical_data, ltp,open_price,high_price,low_price,volume,ORBL5,ORBH5,ORBL15,ORBH15, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, SMA_5, ema_200, ema_50, ema_20, ema_13, symbol_name, instrument, yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol, daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol, three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol, five_days_ago_close,latest_max_5,latest_max_120,SMA_Volume_5, ORBH1, ORBL1)
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
        



# Monitor SL and Target Orders
def monitor_orders(sl_order_id, target_order_id, timeout=20):
    current_time6 = datetime.now().time()
    today = datetime.now().time()
    current_time5 = today.strftime('%Y-%m-%d %H:%M')
    start_time = datetime.now()
    #current_time5 = datetime.now().time()
    """
    Monitor SL and Target orders. Cancel the opposite order if one gets executed.
    """
    try:
        while True:
            current_time = datetime.now()
            elapsed_time = (current_time - start_time).total_seconds()
            # Check for timeout
            if elapsed_time > timeout:
                print(f"Timeout reached while monitoring orders (SL ID: {sl_order_id}, Target ID: {target_order_id}). Exiting loop.")
                break
        # Monitor Stop Loss Order
            sl_order_status = alice.get_order_history(sl_order_id)
            target_order_status = alice.get_order_history(target_order_id)
            symbol_sl = sl_order_status.get('Sym') if sl_order_status else 'Unknown'
            symbol_tg = target_order_status.get('Sym') if target_order_status else 'Unknown'

            if sl_order_status and sl_order_status.get('Status', '').lower() == 'complete':
                print(f"{symbol_sl} Stop Loss order executed. Canceling Target order: {target_order_id} - TIME: {current_time5}\n")
                cancel_response = alice.cancel_order(target_order_id)
                cancel_open_trigger_pending_orders1(symbol_sl)
                break
            elif target_order_status and target_order_status.get('Status', '').lower() == 'complete':
                print(f"{symbol_tg} Target order executed. Canceling Stop Loss order: {sl_order_id} - TIME: {current_time5}\n")
                cancel_response = alice.cancel_order(sl_order_id)
                cancel_open_trigger_pending_orders1(symbol_tg)
                break

        # Pause briefly before rechecking
            time.sleep(2)
    except Exception as e:
        print(f"Error while monitoring orders (SL ID: {sl_order_id}, Target ID: {target_order_id}): {e}")

'''
def stoploss_modify_orders(sl_modify_tracking):
    
    try:
        while True:
            for symbol, order_data in list(sl_modify_tracking.items()):
                try:
                    sl_order_id = order_data['sl_order_id']
                    target_order_id = order_data['target_order_id']
                    buy_order_id = order_data['buy_order_id']
                    executed_price = order_data['executed_price']
                    qty = order_data['qty']
                    symbol = order_data['symbol']
                    ltp = order_data['ltp']
                    # Fetch SL Order status
                    sl_order_status = alice.get_order_history(sl_order_id)
                    if not sl_order_status:
                        print(f"Order ID {sl_order_id} not found for {symbol}")
                        continue

                    stop_loss_price = float(sl_order_status.get('Prc', 0.0))
                    trigger_price = float(sl_order_status.get('Trgprc', 0.0))
                    trantype = sl_order_status.get('Trantype')
                    transaction_type = TransactionType.Sell if trantype == "S" else TransactionType.Buy

                    # Check if LTP exceeds trailing threshold
                    new_stop_loss = round(stop_loss_price * (1 + 0.01), 2)
                    new_trigger_price = round(trigger_price * (1 + 0.01), 2)

                    if ltp >= executed_price * (1 + 0.01) and new_stop_loss > stop_loss_price:
                        print(f"Updating SL for {symbol}: Old SL: {stop_loss_price}, New SL: {new_stop_loss}")
                        modify_sl_orders(
                            transaction_type=transaction_type,
                            symbol=symbol,
                            sl_order_id=sl_order_id,
                            qty=qty,
                            new_stop_loss=new_stop_loss,
                            new_trigger_price=new_trigger_price
                        )
                
                except Exception as e:
                    print(f"Error while modifying SL for {symbol}: {e}")

            # Pause briefly before the next monitoring cycle
            time.sleep(60)

    except Exception as e:
        print(f"Critical error in SL modification for all symbols: {e}")
        '''

def modify_sl_orders(transaction_type, symbol, sl_order_id, qty, new_stop_loss, new_trigger_price):
    """
    Modify Stop Loss Order with new SL and Trigger Price.
    """
    try:
        instrument = alice.get_instrument_by_token('NSE', symbol)
        modify_order_response = alice.modify_order(
            transaction_type=transaction_type,
            instrument=instrument,
            order_id=sl_order_id,
            quantity=qty,
            order_type=OrderType.StopLossLimit,
            product_type=ProductType.Delivery,
            price=new_stop_loss,
            trigger_price=new_trigger_price
        )
        print(f"SL order modified successfully for {symbol}: {modify_order_response}")
    except Exception as e:
        print(f"Error while modifying SL order (ID: {sl_order_id}, Symbol: {symbol}): {e}")

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

def place_order_with_prevention( instrument, transaction_type, price,  symbol_name,condition,open_price,high_price,low_price,volume,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,SMA_5, ORBH1, ORBL1 ):   
    global orders_placed, order_tracking, order_count, sl_modify_tracking
    #order_count = 0
    if order_count >= 200:
       print(f" orders are reached to {order_count} level")
       return
    
    order_id = f"{transaction_type}_{symbol_name}"
    # Load the persisted orders on each function call (optional - can be loaded once at startup)
    load_orders_placed()
    
    if order_id in orders_placed:
        print(f"Duplicate order prevented for {symbol_name}")
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
        print(f"{condition} = Order placed for {symbol_name}: {transaction_type} at {price} - origOrderId: {order_number} at Time: [{current_time}] ")
        print(f" day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH1: {ORBH1}, ORBL1: {ORBL1}")
        print(f"Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}")
        print(f"ORBHP: {ORBHP}, ORBLP: {ORBLP}, ORBH5Con: {ORBH5Con}, Close: {ORBL5Con}")

        #print(f"origOrderId: {origOrderId}")
        
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
            stop_loss_price_call = executed_price * (1 - 0.005)
            stop_loss_price_cal = math.floor(stop_loss_price_call * 10) / 10
            stop_loss_price = ORBL1      #round_to_two_decimals(stop_loss_price_cal)
            trigger_price = ORBL1 + 0.30     #stop_loss_price + 0.30
            
            target_price_call = executed_price * (1 + 0.02)
            target_price_cal = math.floor(target_price_call * 10) / 10
            target_price = round_to_two_decimals(target_price_cal)

            sl_transaction_type = TransactionType.Sell
        else:  # Sell order
            stop_loss_price_call = executed_price * (1 + 0.005)
            stop_loss_price_cal = math.floor(stop_loss_price_call * 10) / 10
            stop_loss_price = ORBH1     #round_to_two_decimals(stop_loss_price_cal)
            trigger_price = ORBH1 - 0.30     #stop_loss_price - 0.30

            target_price_call = executed_price * (1 - 0.02)
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
                # Start monitoring orders
        if sl_order_id and target_order_id:
            monitor_orders_thread = threading.Thread(target=monitor_orders, args=(sl_order_id, target_order_id), name=monitor_orders.__name__)
            monitor_orders_thread.start()
            cancel_open_trigger_pending_orders1(symbol_name)
            #monitor_thread(monitor_orders_thread)
        else:
            print(f"=============>>>>{symbol_name}: No valid conditions met for placing orders.\n")
        if symbol not in sl_modify_tracking:    
            sl_modify_tracking[symbol_name] = {
                    'sl_order_id': sl_order_response['NOrdNo'],
                    'target_order_id': target_order_response['NOrdNo'],
                    'buy_order_id'   : origOrderId['NOrdNo'],
                    'executed_price' : float(order_history['Avgprc']),
                    'qty'            : int(order_history['Fillshares']),
                    'ltp'            : ltp,
                    'symbol'         : symbol
                }
                # Start monitoring orders
        #if sl_order_id and target_order_id:
         #   stoploss_orders_thread = threading.Thread(target=stoploss_modify_orders, args=(sl_modify_tracking,), name=stoploss_modify_orders.__name__)
          #  stoploss_orders_thread.start()
            #monitor_thread(stoploss_modify_orders)
        #else:
         #   print(f"=============>>>>{symbol_name}: No valid conditions met for placing orders.\n")
            

    except Exception as e:
        print(f"===================>>>>> Error placing order for {symbol_name}: {e}\n")

                

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

def should_place_order():
    # Get current time
    current_time = datetime.now().time()

    # Define 9:20 AM as the cutoff time
    #cutoff_time = datetime.strptime('09:21:00', '%H:%M:%S').time()
    # Define cutoff times
    cutoff_time_start = datetime.strptime('09:15:50', '%H:%M:%S').time()  # After 9:21 AM
    cutoff_time_end = datetime.strptime('23:45:00', '%H:%M:%S').time()    # Before 2:50 PM

    # Check if current time is within the allowed time window
    if current_time >= cutoff_time_start and current_time <= cutoff_time_end:

        return True
    else:
        return False

# Main loop
def main_loop():
    
    global socket_opened, terminate_websocket
    #file_path = r'C:\Users\Administrator\Documents\algo\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
    file_path = r'C:\Users\aarya\OneDrive\Documents\PythonAlgo\AliceBlue\nifty_500_symbols.txt'
    symbols = load_symbols_from_txt(file_path)
  
    create_subscription_list(symbols)

    while True:
        now = datetime.now()
        current_time = datetime.now().time()
        if not should_place_order():
            print("It's NOT a Shopping zone before 09:16 AM and after 14:45 PM . Sleeping for 60 seconds to recheck time stamps .","current_time:",current_time)
            time.sleep(60)  # Sleep for 60 seconds before rechecking
            continue  # Recheck again after the sleep
       
        market_close_time = now.replace(hour=23, minute=30, second=0, microsecond=0)

        if now >= market_close_time:
            print("Market hours over. Exiting...")
            break
        
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
