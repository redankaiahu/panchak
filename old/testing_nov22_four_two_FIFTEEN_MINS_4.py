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
import threading
import math
import time
import requests
import pickle
from datetime import time as datetime_time

# ANSI escape codes for colors and styles
class ConsoleColors:
    HEADER = '\033[95m'  # Purple
    OKBLUE = '\033[94m'  # Blue
    OKCYAN = '\033[96m'  # Cyan
    OKGREEN = '\033[92m'  # Green
    WARNING = '\033[93m'  # Yellow
    FAIL = '\033[91m'     # Red
    BOLD = '\033[1m'      # Bold
    UNDERLINE = '\033[4m' # Underline
    ENDC = '\033[0m'      # Reset



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
sl_modify_tracking = {}
order_count = 0
MAX_ORDERS = 20  # Maximum orders allowed per day
terminate_square_off = False
sq_start_time = datetime_time(14, 59)
sq_end_time = datetime_time(15, 10)
current_time1 = datetime.now().time()
current_time2 = datetime.now().time()
low_values = []
high_values = []
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
                if product_code == "MIS":
                    product_type = ProductType.Intraday
                elif product_code == "CNC":
                    product_type = ProductType.Delivery
                elif product_code == "NRML":
                    product_type = ProductType.Normal
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
                    for order in open_orders:
                        if order['Sym'] == symbol and order['Status'].lower() in ['open', 'trigger pending']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            cancel_response = alice.cancel_order(order_id)
                            print(f"Cancelled order {order_id} for {symbol}. Response: {cancel_response}")

                        if order['Sym'] == symbol and order['Status'].lower() in ['open']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            cancel_response = alice.cancel_order(order_id)

                        if order['Sym'] == symbol and order['Status'].lower() in ['trigger pending']:
                            #order_count += 1
                            order_id = order.get('Nstordno')
                            cancel_response = alice.cancel_order(order_id)

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
'''
def cancel_open_trigger_pending_orders(symbol):
    # Retrieve order history
    open_orders = alice.get_order_history('')

    # Separate orders by type
    open_order_ids = []
    trigger_pending_order_ids = []

        # Group orders based on status
    for order in open_orders:
        if order['Sym'] == symbol:
            status = order['Status'].lower()
            order_id = order.get('Nstordno')
            if status == 'open':
                open_order_ids.append(order_id)
            elif status == 'trigger pending':
                trigger_pending_order_ids.append(order_id)

        # Cancel 'open' orders without 'trigger pending'
    if open_order_ids and not trigger_pending_order_ids:
        for order_id in open_order_ids:
            cancel_response = alice.cancel_order(order_id)
            print(f"Cancelled open order with ID {order_id}: {cancel_response}")

        # Cancel 'trigger pending' orders without 'open'
    if trigger_pending_order_ids and not open_order_ids:
        for order_id in trigger_pending_order_ids:
                cancel_response = alice.cancel_order(order_id)
        print(f"Cancelled trigger pending order with ID {order_id}: {cancel_response}")

def cancel_open_trigger_pending_orders1(symbols):
    for symbol in symbols:
        cancel_open_trigger_pending_orders_thread = threading.Thread(target=cancel_open_trigger_pending_orders, args=(symbol), daemon=True)
        cancel_open_trigger_pending_orders_thread.start()
'''
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

                return {
                    "yesterday_open": float(yesterday_data['open']),
                    "yesterday_close": float(yesterday_data['close']),
                    "yesterday_high": float(yesterday_data['high']),
                    "yesterday_low": float(yesterday_data['low']),
                    "yesterday_vol": float(yesterday_data['volume']),
                    "daybefore_vol": float(day_before_data['volume']),
                    "daybefore_low": float(day_before_data['low']),
                    "daybefore_high": float(day_before_data['high']),
                    "daybefore_close": float(day_before_data['close']),
                    "three_days_ago_low": float(three_days_ago['low']),
                    "three_days_ago_high": float(three_days_ago['high']),
                    "three_days_ago_close": float(three_days_ago['close']),
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
def evaluate_buy_condition_1(data, ltp,open_price,high_price,low_price,ORBL5,ORBH5,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
    try:
        if (
            data["daybefore_close"] > data["three_days_ago_close"] and
            data["yesterday_close"] > data["daybefore_close"] and
            ltp > data["yesterday_close"]  and
            open_price > data["three_days_ago_low"] and
            open_price > data["three_days_ago_high"] and
            data["daybefore_vol"] > 300000 and
            ltp > data["three_days_ago_high"] and
            ltp > open_price and            
            ltp > data["ema_200"] and
            ltp > data["ema_50"] and 
            ltp > data["ema_20"] and
            ltp > data["ema_13"] and
            ORBHP < 6 and
            ltp < ORBH5Con and 
            ltp > ORBH5
            
         )  or (
            data["daybefore_close"] > data["three_days_ago_close"] and
            data["yesterday_close"] > data["daybefore_close"] and
            ltp > data["yesterday_close"]  and
            open_price > data["three_days_ago_low"] and
            ltp > data["three_days_ago_high"] and
            data["daybefore_vol"] > 300000 and
            ltp > open_price and            
            ltp > data["ema_200"] and
            ltp > data["ema_50"] and 
            ltp > data["ema_20"] and
            ltp > data["ema_13"] and
            ORBHP < 6 and
            ltp < ORBH5Con and 
            ltp > ORBH5
            
         ):

            return True
    except KeyError as e:
        print(f"Missing data for buy condition 1: {e}")
    return False

# Evaluate buy condition 2
def evaluate_buy_condition_2(data, ltp,open_price,high_price,low_price,ORBL5,ORBH5,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
    try:
        if (

            ORBHP < 6 and
            open_price == low_price and
            ltp < ORBH5Con and
            ltp > data["ema_200"] and
            ltp > data["ema_50"] and
            ltp > data["ema_20"] and
            ltp > data["ema_13"] and
            data["daybefore_vol"] > 300000 and
            ltp > ORBH5
            #ltp > data["yesterday_high"]
            
        ):
            return True
    except KeyError as e:
        print(f"Missing data for buy condition 2: {e}")
    return False

# Evaluate sell condition
def evaluate_sell_condition(data, ltp,open_price,high_price,low_price,ORBL5,ORBH5,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
    try:
        if (

            ORBLP < 6 and
            open_price == high_price and
            ltp > ORBL5Con and
            ltp < data["ema_200"] and
            ltp < data["ema_50"] and
            ltp < data["ema_20"] and
            ltp < data["ema_13"] and
            data["daybefore_vol"] > 300000 and
            data["yesterday_low"] < data["daybefore_low"] and
            ltp < ORBL5   #data["yesterday_low"]
            
        ):
            return True
    except KeyError as e:
        print(f"Missing data for sell condition: {e}")
    return False

def evaluate_buy_condition_ORB15(data, ltp,open_price,high_price,low_price,ORBL5,ORBH5,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
    try:
        if (

            ORBL15_one <= ORBL15_two and ORBL15_one <= ORBL15_thr and ORBL15_one <= ORBL15_four and
            ORBH15_one >= ORBH15_two and ORBH15_one >= ORBH15_thr and ORBH15_one >= ORBH15_four and 
            ltp < ORBH15Con and        
            ltp > data["ema_200"] and
            ltp > data["ema_50"] and
            ltp > data["ema_20"] and
            ltp > data["ema_13"] and            
            data["daybefore_vol"] > 300000 and            
            ltp > ORBH15_one
            
        ):
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_buy_condition_ORB15: {e}")
    return False

# Evaluate sell condition
def evaluate_sell_condition_ORB15(data, ltp,open_price,high_price,low_price,ORBL5,ORBH5,condition1,condition2,condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
    try:
        if (
            
            ORBL15_one <= ORBL15_two and ORBL15_one <= ORBL15_thr and ORBL15_one <= ORBL15_four and
            ORBH15_one >= ORBH15_two and ORBH15_one >= ORBH15_thr and ORBH15_one >= ORBH15_four and
            ltp > ORBL15Con and        
            ltp < data["ema_200"] and
            ltp < data["ema_50"] and
            ltp < data["ema_20"] and
            ltp < data["ema_13"] and
            data["daybefore_vol"] > 300000 and            
            ltp < ORBL15_one
            
        ):
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_sell_condition_ORB15: {e}")
    return False



# Monitor SL and Target Orders
def monitor_orders(sl_order_id, target_order_id):
    current_time6 = datetime.now().time()
    today = datetime.now().time()
    current_time5 = today.strftime('%Y-%m-%d %H:%M')
    #current_time5 = datetime.now().time()
    """
    Monitor SL and Target orders. Cancel the opposite order if one gets executed.
    """
    try:
        while True:
        # Monitor Stop Loss Order
            sl_order_status = alice.get_order_history(sl_order_id)
            symbol = sl_order_status.get('Sym')
            if sl_order_status and sl_order_status['Status'].lower() == 'complete':
                print(f"{symbol} Stop Loss order executed. Canceling Target order: {target_order_id} - TIME: {current_time5}\n")
                cancel_response = alice.cancel_order(target_order_id)
                #print(f"Target order cancellation response: {cancel_response}")
                break

            # Monitor Target Order
            target_order_status = alice.get_order_history(target_order_id)
            symbol = target_order_status.get('Sym')
            if target_order_status and target_order_status['Status'].lower() == 'complete':
                print(f"{symbol} Target order executed. Canceling Stop Loss order: {sl_order_id} - TIME: {current_time5}\n")
                cancel_response = alice.cancel_order(sl_order_id)
                #print(f"Stop Loss order cancellation response: {cancel_response}")
                break

        # Pause briefly before rechecking
            time.sleep(2)
    except Exception as e:
        print(f"Error while monitoring orders (SL ID: {sl_order_id}, Target ID: {target_order_id}): {e}")
'''
# Monitor SL and Target Orders
def stoploss_modify_orders(sl_order_id, target_order_id, buy_order_id, executed_price, qty, ltp):
    current_time6 = datetime.now().time()
    today = datetime.now().time()
    current_time5 = today.strftime('%Y-%m-%d %H:%M')
    #current_time5 = datetime.now().time()
    """
    Monitor SL and Target orders. Cancel the opposite order if one gets executed.
    """
    try:
        while True:
        # Monitor Stop Loss Order
            sl_order_status = alice.get_order_history(sl_order_id)
            symbol = sl_order_status.get('Sym')
            qty = sl_order_status.get('Qty')
            stop_loss_price = sl_order_status.get('Prc')   # stop loss price
            trigger_price = sl_order_status.get('Trgprc')  # Trigger price
            Trantype = sl_order_status.get('Trantype') 
            transaction_type = TransactionType.Sell if Trantype == "S" else TransactionType.Buy
            

            if ltp >= executed_price * (1 + 0.01):
                new_stop_loss = stop_loss_price  * (1 + 0.01)
                new_trigger_price = trigger_price   * (1 + 0.01)
                modify_sl_orders( transaction_type, symbol, sl_order_id, qty, new_stop_loss, new_trigger_price )
                
            open_orders = alice.get_order_history('')
                for order in open_orders:
                    if order['Status'].lower() == 'trigger pending':
                        symbol = str(order.get('Sym'))
                        order_id = order.get('Nstordno')
                        qty = order.get('Qty')
                        Prc = float(order.get('Prc'))
                        Trgprc = float(order.get('Trgprc'))
                        Prctype = order.get('Prctype')
                        Trantype = order.get('Trantype')
                        executed_price = Prc  * (1 + 0.01)
                        transaction_type = TransactionType.Sell if Trantype == "S" else TransactionType.Buy
                        if ltp >= executed_price * (1 + 0.01):
                            new_stop_loss = stop_loss_price  * (1 + 0.01)
                            new_trigger_price = trigger_price   * (1 + 0.01)
                            modify_sl_orders( transaction_type, symbol, sl_order_id, qty, new_stop_loss, new_trigger_price )
              


        # Pause briefly before rechecking
            time.sleep(2)
    except Exception as e:
        print(f"Error while monitoring orders (SL ID: {sl_order_id}, Target ID: {target_order_id}): {e}")

def modify_sl_orders( transaction_type, symbol, sl_order_id, qty, new_stop_loss, new_trigger_price ):

    try:
        modify_order_response = alice.modify_order(
                    transaction_type = transaction_type,
                    instrument = alice.get_instrument_by_token('NSE', symbol),
                    order_id="sl_order_id",
                    quantity = qty,
                    order_type = OrderType.Limit,
                    product_type = ProductType.Delivery,
                    price=new_stop_loss,
                    trigger_price = new_trigger_price
                    )
'''
'''
def EVALUATE_BUY_COND_1( data, ltp, open_price, high_price, low_price, ORBL5, ORBH5, condition1, condition2, condition3, condition4, condition5, ORBL15_one, ORBH15_one, ORBL15_two, ORBH15_two, ORBL15_thr, ORBH15_thr, ORBL15_four, ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP, instrument,  stop_loss, symbol_name, yesterday_open, yesterday_high, yesterday_low, yesterday_close):
    current_time6 = datetime.now().time()
    today = datetime.now().time()
    current_time5 = today.strftime('%Y-%m-%d %H:%M')
    
    try:
        while True:
        
            if (
                open_price > data["three_days_ago_low"] and
                open_price > data["three_days_ago_high"] and
                data["daybefore_vol"] > 300000 and
                ltp > data["three_days_ago_high"] and
                ltp > open_price and            
                ltp > data["ema_200"] and
                ltp > data["ema_50"] and 
                ltp > data["ema_20"] and
                ltp > data["ema_13"] and
                ORBHP < 6 and
                ltp < ORBH5Con and 
                ltp > ORBH5
            
                )  or (
                open_price > data["three_days_ago_low"] and
                ltp > data["three_days_ago_high"] and
                data["daybefore_vol"] > 300000 and
                ltp > open_price and            
                ltp > data["ema_200"] and
                ltp > data["ema_50"] and 
                ltp > data["ema_20"] and
                ltp > data["ema_13"] and
                ORBHP < 6 and
                ltp < ORBH5Con and 
                ltp > ORBH5
            
                ):

                place_order_with_prevention( instrument, TransactionType.Buy, ltp,  stop_loss, symbol_name,condition1,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP  )

        # Pause briefly before rechecking
            time.sleep(10)
    except Exception as e:
        print(f"issue in function: {e}")

'''

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

def place_order_with_prevention( instrument, transaction_type, price,  stop_loss, symbol_name,condition,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP ):   
    global orders_placed, order_tracking, order_count, sl_modify_tracking
    #order_count = 0
    if order_count >= 20:
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
            quantity = 10
        elif 500 <= price <= 1000:
            quantity = 5
        elif 1000 < price <= 2000:
            quantity = 2
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
        print(f" day's Open: {open_price}, High: {high_price}, Low: {low_price}, LTP: {ltp}, ORBL5: {ORBL5}, ORBH5: {ORBH5}, ORBH15: {ORBH15_one}, ORBL15: {ORBL15_one}")
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
            stop_loss_price = round_to_two_decimals(stop_loss_price_cal)
            trigger_price = stop_loss_price + 0.30
            
            target_price_call = executed_price * (1 + 0.01)
            target_price_cal = math.floor(target_price_call * 10) / 10
            target_price = round_to_two_decimals(target_price_cal)

            sl_transaction_type = TransactionType.Sell
        else:  # Sell order
            stop_loss_price_call = executed_price * (1 + 0.005)
            stop_loss_price_cal = math.floor(stop_loss_price_call * 10) / 10
            stop_loss_price = round_to_two_decimals(stop_loss_price_cal)
            trigger_price = stop_loss_price - 0.30

            target_price_call = executed_price * (1 - 0.01)
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
            monitor_orders_thread = threading.Thread(target=monitor_orders, args=(sl_order_id, target_order_id), name=monitor_orders)
            monitor_orders_thread.start()
            monitor_thread(monitor_orders_thread)
        else:
            print(f"=============>>>>{symbol_name}: No valid conditions met for placing orders.\n")
            '''
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
        if sl_order_id and target_order_id:
            stoploss_orders_thread = threading.Thread(target=stoploss_modify_orders, args=(sl_order_id, target_order_id, buy_order_id, executed_price, qty, ltp, symbol), name=stoploss_modify_orders)
            stoploss_orders_thread.start()
            monitor_thread(stoploss_modify_orders)
        else:
            print(f"=============>>>>{symbol_name}: No valid conditions met for placing orders.\n")
            '''

    except Exception as e:
        print(f"===================>>>>> Error placing order for {symbol_name}: {e}\n")




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
                yesterday_close = historical_data.get("yesterday_close")
                yesterday_high = historical_data.get("yesterday_high")
                yesterday_low = historical_data.get("yesterday_low")
                condition1    = "BUY Condition 1"
                condition2    = "BUY OPEN == LOW"
                condition3    = "SELL OPEN == HIGH"
                condition4    = "BUY ORBH15"
                condition5    = "SELL ORBH15"

            historical_data_one = alice.get_historical(instrument=instrument, from_datetime=from_datetime1, to_datetime=to_datetime1, interval="1", indices=False )
            # Create a list to store the first 60 rows
            if not historical_data_one.empty:
             #   # Extract 'low' and 'high' columns as lists
                low_values = historical_data_one.iloc[:60]['low'].tolist()
                high_values = historical_data_one.iloc[:60]['high'].tolist()
            # FIRST 15 MINS
            data1 = historical_data_one.iloc[0]
            data2 = historical_data_one.iloc[1]
            data3 = historical_data_one.iloc[2]
            data4 = historical_data_one.iloc[3]
            data5 = historical_data_one.iloc[4]

            data6 = historical_data_one.iloc[5]
            data7 = historical_data_one.iloc[6]
            data8 = historical_data_one.iloc[7]
            data9 = historical_data_one.iloc[8]
            data10 = historical_data_one.iloc[9]

            data11 = historical_data_one.iloc[10]
            data12 = historical_data_one.iloc[11]
            data13 = historical_data_one.iloc[12]
            data14 = historical_data_one.iloc[13]
            data15 = historical_data_one.iloc[14]

            # FIRST 15 MINS ORB VALUES
            low1, high1 = historical_data_one.iloc[0]['low'], historical_data_one.iloc[0]['high']
            low2, high2 = historical_data_one.iloc[1]['low'], historical_data_one.iloc[1]['high']
            low3, high3 = historical_data_one.iloc[2]['low'], historical_data_one.iloc[2]['high']
            low4, high4 = historical_data_one.iloc[3]['low'], historical_data_one.iloc[3]['high']
            low5, high5 = historical_data_one.iloc[4]['low'], historical_data_one.iloc[4]['high']

            low6, high6 = historical_data_one.iloc[5]['low'], historical_data_one.iloc[5]['high']
            low7, high7 = historical_data_one.iloc[6]['low'], historical_data_one.iloc[6]['high']
            low8, high8 = historical_data_one.iloc[7]['low'], historical_data_one.iloc[7]['high']
            low9, high9 = historical_data_one.iloc[8]['low'], historical_data_one.iloc[8]['high']
            low10, high10 = historical_data_one.iloc[9]['low'], historical_data_one.iloc[9]['high']

            low11, high11 = historical_data_one.iloc[10]['low'], historical_data_one.iloc[10]['high']
            low12, high12 = historical_data_one.iloc[11]['low'], historical_data_one.iloc[11]['high']
            low13, high13 = historical_data_one.iloc[12]['low'], historical_data_one.iloc[12]['high']
            low14, high14 = historical_data_one.iloc[13]['low'], historical_data_one.iloc[13]['high']
            low15, high15 = historical_data_one.iloc[14]['low'], historical_data_one.iloc[14]['high']

            
            low_values5 = [low1, low2, low3, low4, low5]
            high_values5 = [high1, high2, high3, high4, high5]

            # Find the lowest low and highest high
            #ORBL5 = min(low_values5)
            #ORBH5 = max(high_values5)
    
            
            # First 5 minutes ORB values (low and high)
            low_values_5_one = low_values[:5]
            high_values_5_one = high_values[:5]
            ORBL5 = min(low_values_5_one)
            ORBH5 = max(high_values_5_one)

            # First 15 minutes ORB values (low and high)
            low_values_15_one = low_values[:15]
            high_values_15_one = high_values[:15]
            ORBL15_one = min(low_values_15_one)
            ORBH15_one = max(high_values_15_one)

            # Second 15 minutes ORB values (low and high)
            low_values_15_two = low_values[15:30]
            high_values_15_two = high_values[15:30]
            ORBL15_two = min(low_values_15_two)
            ORBH15_two = max(high_values_15_two)

            # Third 15 minutes ORB values (low and high)
            low_values_15_thr = low_values[30:45]
            high_values_15_thr = high_values[30:45]
            ORBL15_thr = min(low_values_15_thr)
            ORBH15_thr = max(high_values_15_thr)

            # Fourth 15 minutes ORB values (low and high)
            low_values_15_four = low_values[45:60]
            high_values_15_four = high_values[45:60]
            ORBL15_four = min(low_values_15_four)
            ORBH15_four = max(high_values_15_four)

            gapup = ((ORBH5 - yesterday_close)/yesterday_close) * 100
            gapdown =  ((yesterday_close - ORBL5)/yesterday_close) * 100
            ORBHP = round(gapup , 2)
            ORBLP = round(gapdown ,2)

            ORBH5Con = float(ORBH5) + 3
            ORBL5Con = float(ORBL5) - 3
            ORBH15Con = float(ORBH15_one) + 3
            ORBL15Con = float(ORBL15_one) - 3

            
            if evaluate_buy_condition_1(historical_data, ltp,open_price,high_price,low_price,ORBL5,ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
                place_order_with_prevention( instrument, TransactionType.Buy, ltp,  stop_loss, symbol_name,condition1,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP )
                thread = threading.Thread(target=evaluate_buy_condition_1, args=( historical_data, ltp, open_price, high_price, low_price, ORBL5, ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one, ORBH15_one, ORBL15_two, ORBH15_two, ORBL15_thr, ORBH15_thr, ORBL15_four, ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP), name=evaluate_buy_condition_1)
                thread.start()
                monitor_thread(thread)
                
            elif evaluate_buy_condition_2(historical_data, ltp,open_price,high_price,low_price,ORBL5,ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
                place_order_with_prevention(instrument, TransactionType.Buy, ltp,  stop_loss, symbol_name,condition2,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP )
                thread = threading.Thread(target=evaluate_buy_condition_2, args=( historical_data, ltp, open_price, high_price, low_price, ORBL5, ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one, ORBH15_one, ORBL15_two, ORBH15_two, ORBL15_thr, ORBH15_thr, ORBL15_four, ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP), name=evaluate_buy_condition_2)
                thread.start()
                monitor_thread(thread)

            elif evaluate_sell_condition(historical_data, ltp,open_price,high_price,low_price,ORBL5,ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
                place_order_with_prevention( instrument, TransactionType.Sell, ltp,  stop_loss, symbol_name,condition3,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP  )
                thread = threading.Thread(target=evaluate_sell_condition, args=( historical_data, ltp, open_price, high_price, low_price, ORBL5, ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one, ORBH15_one, ORBL15_two, ORBH15_two, ORBL15_thr, ORBH15_thr, ORBL15_four, ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP), name=evaluate_sell_condition)
                thread.start()
                monitor_thread(thread)

            elif should_evaluate_condition() and evaluate_buy_condition_ORB15(historical_data, ltp,open_price,high_price,low_price,ORBL5,ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
                 
                 #place_order_with_prevention( instrument, TransactionType.Buy, ltp,  stop_loss, symbol_name, condition4,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four )
                thread = threading.Thread(target=place_order_with_prevention, args=( instrument, TransactionType.Buy, ltp,  stop_loss, symbol_name, condition4,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP ), name=evaluate_buy_condition_ORB15)
                thread.start()
                monitor_thread(thread)
            elif should_evaluate_condition() and evaluate_sell_condition_ORB15(historical_data, ltp,open_price,high_price,low_price,ORBL5,ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP):
                 
                 #place_order_with_prevention( instrument, TransactionType.Sell, ltp,  stop_loss, symbol_name, condition5,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four )
                thread = threading.Thread(target=place_order_with_prevention, args=(  instrument, TransactionType.Sell, ltp,  stop_loss, symbol_name, condition5,open_price,high_price,low_price,ORBL5,ORBH5,yesterday_open,yesterday_high,yesterday_low,yesterday_close,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP ), name=evaluate_sell_condition_ORB15)
                thread.start()
                monitor_thread(thread)
       # while True:
        #    EVALUATE_BUY_COND_1_thread = threading.Thread(target=EVALUATE_BUY_COND_1, args=(historical_data, ltp,open_price,high_price,low_price,ORBL5,ORBH5, condition1, condition2, condition3, condition4, condition5,ORBL15_one,ORBH15_one,ORBL15_two,ORBH15_two,ORBL15_thr,ORBH15_thr,ORBL15_four,ORBH15_four,ORBH5Con, ORBL5Con, ORBH15Con, ORBL15Con, ORBHP, ORBLP,instrument,  stop_loss,symbol_name,yesterday_open,yesterday_high,yesterday_low,yesterday_close), name=EVALUATE_BUY_COND_1)
         #   EVALUATE_BUY_COND_1_thread.start()
                 

def monitor_thread(thread):
    current_time6 = datetime.now().time()
    """
    Monitors and prints the activity of a thread.
    """
    while thread.is_alive():
        print(f"{ConsoleColors.WARNING}Thread {thread.name} is running...{ConsoleColors.ENDC}- TIME: {current_time6}")
        time.sleep(1)  # Poll every second

# Function to keep the WebSocket open for 1 hour
def keep_websocket_open_for_one_hour():
    global terminate_websocket
    print("WebSocket is open. Staying for 6 hour...")
    time.sleep(21000)  # Wait for 1 hour
    terminate_websocket = True
    print("WebSocket stopped. Sleeping 30 seconds...")
    alice.stop_websocket()  # Stop the WebSocket after 1 hour
    time.sleep(15)  # Sleep for 30 seconds


def should_evaluate_condition():        ### FOR ORB15 EVALUATE FUNCTIONS
    # Get the current time
    current_time = datetime.now().time()

    # Define the cutoff time as 10:15 AM
    cutoff_time = datetime.strptime('10:15:30', '%H:%M:%S').time()

    # Return True if the current time is after 10:15 AM
    if current_time >= cutoff_time:
        return True
    else:
        return False

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

# Set up the logging configuration
def setup_logging():
    # Get current date and time to create a unique log file name
    log_filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + "_log.txt"
    
    # Configure the logging module
    logging.basicConfig(
        filename=log_filename,  # Create a new log file each time with timestamp
        level=logging.DEBUG,     # Set the log level to capture everything from DEBUG and above
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log format (timestamp, log level, message)
        filemode='w'  # 'w' means the file will be overwritten each time the code runs
    )

def should_place_order():
    # Get current time
    current_time = datetime.now().time()

    # Define 9:20 AM as the cutoff time
    #cutoff_time = datetime.strptime('09:21:00', '%H:%M:%S').time()
    # Define cutoff times
    cutoff_time_start = datetime.strptime('10:15:10', '%H:%M:%S').time()  # After 9:21 AM
    cutoff_time_end = datetime.strptime('23:45:00', '%H:%M:%S').time()    # Before 2:50 PM

    # Check if current time is within the allowed time window
    if current_time >= cutoff_time_start and current_time <= cutoff_time_end:

        return True
    else:
        return False

# Main loop
def main_loop():
    setup_logging()
    #load_orders_placed()
    global socket_opened, terminate_websocket
    symbols = [
                "360ONE", "ACC", "AIAENG", "APLAPOLLO", "AUBANK", "AADHARHFC", "AARTIIND", "AAVAS", "ADANIENT", "ADANIGREEN", "ADANIPORTS",
                "ADANIPOWER",  "CENTURYPLY", "ABSLAMC", "ACCELYA", "ACE", "AETHER", "AFFLE", "AGI", "AJANTPHARM", "AKZOINDIA", "CARTRADE",  "ALKYLAMINE", "ARE&M", "AMBER", "ANGELONE", "ANURAS", 
                "APLLTD", "APOLLOHOSP", "APOLLOTYRE", "ARVINDFASN", "ASAHIINDIA", "ASAL", "ASIANPAINT", "ASTERDM", "ASTRAL", "ATGL", "ATUL", "AUBANK", "AVANTIFEED", 
                "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BALKRISIND", "BALRAMCHIN", "BATAINDIA", "BAYERCROP", "BBTC", "BDL", "BEL",
                "BERGEPAINT", "BFINVEST", "BHARATFORG", "BHEL",  "BIOCON", "BIRLACORPN", "BLUESTARCO", "AVALON","CMSINFO", "POLICYBZR",  "BORORENEW", "BPCL", "BRITANNIA", "BSOFT", "CAMS",  "CANFINHOME", 
                "CARBORUNIV", "CEATLTD",  "CERA", "CHALET", "CHAMBLFERT","TEJASNET",  "CHOLAFIN", "CIPLA", "CLEAN", "COALINDIA",  "COLPAL", "CONCOR", 
                "COROMANDEL", "CREDITACC", "CRISIL", "CROMPTON", "CSBBANK", "CUMMINSIND", "CYIENT", "DCMSHRIRAM", "DEEPAKNTR", "DHANUKA",  "DIVISLAB", "DMART", 
                "DRREDDY",  "EICHERMOT", "EIDPARRY", "EIHOTEL", "ELGIEQUIP", "EMAMILTD", "ADANIENSOL", "ERIS", "ESCORTS",  "EXIDEIND", "FDC", "FINEORG",  
                "FLUOROCHEM", "FMGOETZE", "FORTIS", "FSL", "GABRIEL", "GALAXYSURF", "GARFIBRES", "GESHIP", "GHCL", "GICRE", "GLAND", "GLAXO", "GLENMARK", "GMDCLTD",
                "JYOTHYLAB", "TATATECH", "GNFC", "GOCOLORS", "GODFRYPHLP", "GODREJAGRO", "GODREJCP", "GODREJIND", "GODREJPROP", "GRANULES", "GRAPHITE", "GRASIM", 
                "GREENLAM", "GREENPANEL","CGPOWER", "GRINDWELL", "GRSE", "GSPL", "GUJGASLTD", "HAL", "HAPPSTMNDS","MAXHEALTH", "HAVELLS", "HBLPOWER", "HCLTECH", "HDFCAMC",
                "HDFCBANK", "HDFCLIFE","JINDWORLD", "HESTERBIO", "HGINFRA", "HIKAL", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HOMEFIRST", "ICICIBANK", "ICICIGI",
                "RAILTEL", "ICICIPRULI", "IFBIND", "IGL", "JPOLYINVST", "STERTOOLS","PSPPROJECT", "INDHOTEL", "INDIACEM", "INDIAGLYCO", "INDIAMART", "INDIANB", "INDIGO",
                "INDUSINDBK","INDUSTOWER", "INFOBEAN", "INFY", "INGERRAND",  "INTELLECT", "IPCALAB", "IRCTC", "ITC", "ITI",  "JAICORPLTD",   "JBCHEPHARM",
                "JINDALPOLY", "JINDALSTEL", "JKCEMENT", "JKLAKSHMI", "JKPAPER", "JKTYRE", "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "KAJARIACER",  "TITAGARH", 
                "KEI", "KIRLOSBROS", "KIRLOSENG", "KIRLOSIND", "KNRCON", "KOTAKBANK", "KRBL", "LAOPALA", "LICHSGFIN", "LICI", "LINDEINDIA", "LT", "LTIM",
                "ARTEMISMED", "JGCHEM","ATGL",  "LTTS", "LUMAXIND","LUXIND", "M&M",  "MAHLIFE", "MAPMYINDIA","IIFL",  "MARICO", "MARUTI", "MASTEK",  "MCX",
                "METROBRAND", "MFSL", "MIDHANI", "WEBELSOLAR",   "MINDSPACE",   "MOIL","STARHEALTH", "KIRIINDUS", "JASH", "KIMS", "KRN",  "MPHASIS", "MTARTECH",
                "MUTHOOTFIN", "NATIONALUM", "NAUKRI", "NAVINFLUOR", "WAAREEENER", "NCC", "NDRAUTO","NEOGEN", "NESCO", "NH", "MOTILALOFS",  "NTPC", "NUCLEUS", "OIL",
                "ORIENTCEM","RVNL", "PCBL", "PEL", "PERSISTENT", "BRIGADE","SYRMA", "PETRONET", "PFC", "PFIZER",  "PHOENIXLTD", "PIDILITIND", "PIIND", "POLYCAB",
                "TIMETECHNO","ITDCEM", "POONAWALLA", "POWERGRID", "PRESTIGE", "PRINCEPIPE", "QUESS", "RADICO", "RALLIS", "RAMCOCEM", "RATNAMANI", "RAYMOND",
                "RECLTD", "RELAXO", "RELIANCE", "RHIM", "RITES", "ROUTE", "SANOFI", "SBICARD", "SBILIFE",  "SBIN",  "SCHAEFFLER", "SHARDACROP",  "SHILPAMED",
                "SHOPERSTOP","MARKSANS",  "SHRIRAMFIN", "SIEMENS", "SIS", "SKFINDIA", "SOBHA", "SONACOMS", 
                "SRF", "STAR", "SUMICHEM",  "SUNDARMFIN", "SUNDRMFAST","PREMIERENE", "SUNPHARMA", "SUNTECK", "SUPRAJIT", "SURYAROSNI", "SWANENERGY", "SYMPHONY",
                "SYNGENE", "TATACHEM", "TATACOMM", "TATACONSUM", "TATAELXSI", "TATAINVEST",  "TATAMOTORS","TRITURBINE", "TATAPOWER", "TCI", "TCS", "TEAMLEASE",
                "TECHM", "THERMAX", "THYROCARE", "TIINDIA",  "TIMKEN", "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT", "TTKPRESTIG", "ORIENTTECH","PGEL","TVSMOTOR",
                "UBL", "UFLEX", "UNOMINDA", "UPL", "UTIAMC","DEEPAKFERT", "VBL", "VEDL", "VENKEYS", "VGUARD", "VINATIORGA", "VIPIND", "VMART", "VOLTAS", "VSTIND",
                "WABAG", "WELCORP",  "WESTLIFE", "WHIRLPOOL", "WIPRO",  "WOCKPHARMA","OBEROIRLTY", "ZENSARTECH", "KALYANKJIL", "BSE", "LAURUSLABS", "CDSL", "SONACOMS", "BBOX"
    ]
    create_subscription_list(symbols)
    #cancel_open_trigger_pending_orders1(symbols)

    while True:
        now = datetime.now()
        current_time = datetime.now().time()
        if not should_place_order():
            print("It's NOT a Shopping zone before 10:14 AM and after 14:50 PM . Sleeping for 60 seconds to recheck time stamps .","current_time:",current_time)
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
        print("Stopping WebSocket after 1 hour...")
        alice.stop_websocket()

        # Reset termination flag
        terminate_websocket = False

        # Wait for 30 seconds before restarting the WebSocket
        print("Sleeping for 30 seconds before restarting...")
        time.sleep(30)  # Sleep before restarting the WebSocket session

        
# Entry point
if __name__ == "__main__":
    main_loop()
