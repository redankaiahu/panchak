from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from alice_blue import AliceBlue
from pya3 import *
import pandas as pd
import os
import time
import time as time_module  # renamed to avoid conflict with datetime
from datetime import datetime
import math


# Adjust date to skip weekends and holidays
def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:  # Skip weekends
        date -= timedelta(days=1)
    while date in holidays:  # Skip holidays
        date -= timedelta(days=1)
    return date

# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

socket_opened = False


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
to_datetime1 = today.replace(hour=9, minute=19, second=59, microsecond=0)

net_position = alice.get_netwise_positions()
open_positions = Alice_Wrapper.open_net_position(net_position)
open_orders = alice.get_order_history('')
#print("open_orders",open_orders)
                    
for order in open_orders:
    symbol = order['Sym']
    if order['Sym'] == symbol and order['Status'].lower() in ['open', 'trigger pending']:
        order_id = order.get('Nstordno')
        #print("order id:",symbol,order_id)
        
def modify_stoploss_orders_data(symbol, open_price, target_price, trigger_price, avg_price, trantype):
    symbol = symbol
    executed_price = avg_price
    target = open_price
    stoploss = target_price
    trigger = trigger_price

    print(f"Symbol: {symbol},trantype:{trantype},executed_price:{executed_price},target:{target},stoploss:{stoploss},trigger:{trigger}\n")

    #print(f"\nSymbol: {symbol}")
    #print(f"  Open Order Price: {open_price}")
    #print(f"  Target Price: {target_price}")
    #print(f"  Trigger Price: {trigger_price}")
    #print(f"  Completed Avg Price: {avg_price}")
    #print(f"  Transaction Type: {trantype}")


# Function to extract and pass relevant data
def modify_stoploss_orders():
    try:
        # Fetch all orders
        open_orders = alice.get_order_history('')

        # Group orders dynamically by symbol
        orders_by_symbol = {}

        # Filter and organize orders
        for order in open_orders:
            symbol = order.get('Sym')
            status = order.get('Status', '').lower()

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
                        'Prc': order.get('Prc')
                    }
                elif status == 'trigger pending' and not orders_by_symbol[symbol]['trigger_pending']:
                    orders_by_symbol[symbol]['trigger_pending'] = {
                        'Trgprc': order.get('Trgprc'),
                        'Prc': order.get('Prc')
                    }
                elif status == 'complete':
                    orders_by_symbol[symbol]['complete'].append({
                        'Avgprc': order.get('Avgprc'),
                        'Trantype': order.get('Trantype')
                    })

        # Pass extracted data to another function
        for symbol, details in orders_by_symbol.items():
            if details['open'] and details['trigger_pending']:
                open_price = details['open']['Prc']
                target_price = details['trigger_pending']['Prc']
                trigger_price = details['trigger_pending']['Trgprc']

                # Print completed orders if available
                avg_price = "N/A"
                trantype = "N/A"

                if details['complete']:
                    completed = details['complete'][0]  # Take the first completed order
                    avg_price = completed['Avgprc']
                    trantype = completed['Trantype']

                # Call the secondary function
                modify_stoploss_orders_data(
                    symbol, open_price, target_price, trigger_price, avg_price, trantype
                )

    except Exception as e:
        print(f"Error while processing orders: {e}")


# Execute the main function
modify_stoploss_orders()