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
        print("order id:",symbol,order_id)

def cancel_unnecessary_orders():
    try:
        # Fetch all open orders
        open_orders = alice.get_order_history('')
        print("Open Orders:", open_orders)

        # Track symbols with valid 'open' or 'trigger pending' orders
        valid_orders = {}

        for order in open_orders:
            symbol = order.get('Sym')
            status = order.get('Status', '').lower()
            order_id = order.get('Nstordno')
            print(symbol,status,order_id)

            # Identify symbols with active orders
            if status in ['open', 'trigger pending']:
                if symbol not in valid_orders:
                    valid_orders[symbol] = {'open': [], 'trigger_pending': []}

                valid_orders[symbol][status.replace(' ', '_')].append(order_id)

        # Process cancellation of orders with incomplete matching
        for symbol, orders in valid_orders.items():
            open_orders = orders['open']
            trigger_pending_orders = orders['trigger_pending']

            if not (open_orders and trigger_pending_orders):
                # Cancel any incomplete orders for the symbol
                for order_id in open_orders + trigger_pending_orders:
                    cancel_response = alice.cancel_order(order_id)
                    print(f"Cancelled order {order_id} for symbol {symbol}: {cancel_response}")

        print("Order cleanup complete.")

    except Exception as e:
        print(f"Error while canceling orders: {e}")
# Execute the cleanup function
cancel_unnecessary_orders()