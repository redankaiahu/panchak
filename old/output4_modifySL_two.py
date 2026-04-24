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
'''
def modify_stoploss_orders():
    try:
        # Fetch all open orders
        open_orders = alice.get_order_history('')
        #print("Open Orders:", open_orders)

        # Track symbols with valid 'open' or 'trigger pending' orders
        valid_orders = {}

        for order in open_orders:
            symbol = order.get('Sym')
            status = order.get('Status', '').lower()
            order_id = order.get('Nstordno')
            print(symbol,status,order_id)

            

    except Exception as e:
        print(f"Error while canceling orders: {e}")
'''
'''
def modify_stoploss_orders():
    try:
        # Fetch all orders
        open_orders = alice.get_order_history('')
        print(alice.get_order_history('24121900038004'))
        print(alice.get_order_history('24121900038000'))
        print(alice.get_order_history('24121900037918'))
        # Filter and display only relevant orders
        filtered_orders = [order for order in open_orders 
                           if order.get('Status', '').lower() in ['open', 'trigger pending', 'complete']]

        # Display filtered orders
        for order in filtered_orders:
            symbol = order.get('Sym')
            status = order.get('Status', '').lower()
            order_id = order.get('Nstordno')
            print(f"{symbol} {status} {order_id}")
            

    except Exception as e:
        print(f"Error while canceling orders: {e}")

'''
'''
def modify_stoploss_orders():
    try:
        # Fetch all orders
        open_orders = alice.get_order_history('')

        # Group orders by symbol
        orders_by_symbol = {}

        for order in open_orders:
            symbol = order.get('Sym')
            status = order.get('Status', '').lower()

            # Filter only relevant statuses
            if status in ['open', 'trigger pending', 'complete']:
                if symbol not in orders_by_symbol:
                    orders_by_symbol[symbol] = {
                        'open': None,
                        'trigger_pending': None,
                        'complete': []
                    }

                # Assign orders to corresponding categories
                if status == 'open' and not orders_by_symbol[symbol]['open']:
                    orders_by_symbol[symbol]['open'] = {
                        'Prc': order.get('Prc'),
                        'Pcode': order.get('Pcode'),
                        'Qty': order.get('Qty'),
                        'Trantype': order.get('Trantype'),
                        'Nstordno': order.get('Nstordno')
                    }
                elif status == 'trigger pending' and not orders_by_symbol[symbol]['trigger_pending']:
                    orders_by_symbol[symbol]['trigger_pending'] = {
                        'Trgprc': order.get('Trgprc'),
                        'Prc': order.get('Prc'),
                        'Qty': order.get('Qty'),
                        'Prctype': order.get('Prctype'),
                        'Pcode': order.get('Pcode'),
                        'Trantype': order.get('Trantype'),
                        'Nstordno': order.get('Nstordno')
                    }
                elif status == 'complete':
                    orders_by_symbol[symbol]['complete'].append({
                        'Qty': order.get('Qty'),
                        'Avgprc': order.get('Avgprc'),
                        'Trantype': order.get('Trantype'),
                        'Nstordno': order.get('Nstordno')
                    })

        # Print only symbols with both 'open' and 'trigger pending' orders
        for symbol, details in orders_by_symbol.items():
            if details['open'] and details['trigger_pending']:
                print(f"\nSymbol: {symbol}")

                print(f"  Open Order: {details['open']}")
                print(f"  Trigger Pending Order: {details['trigger_pending']}")

                if details['complete']:
                    print("  Completed Orders:")
                    for completed in details['complete']:
                        print(f"    {completed}")

    except Exception as e:
        print(f"Error while processing orders: {e}")

'''

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
                        'Prc': order.get('Prc'),
                        'Pcode': order.get('Pcode'),
                        'Qty': order.get('Qty'),
                        'Trantype': order.get('Trantype'),
                        'Nstordno': order.get('Nstordno')
                    }
                elif status == 'trigger pending' and not orders_by_symbol[symbol]['trigger_pending']:
                    orders_by_symbol[symbol]['trigger_pending'] = {
                        'Trgprc': order.get('Trgprc'),
                        'Prc': order.get('Prc'),
                        'Qty': order.get('Qty'),
                        'Prctype': order.get('Prctype'),
                        'Pcode': order.get('Pcode'),
                        'Trantype': order.get('Trantype'),
                        'Nstordno': order.get('Nstordno')
                    }
                elif status == 'complete':
                    orders_by_symbol[symbol]['complete'].append({
                        'Qty': order.get('Qty'),
                        'Avgprc': order.get('Avgprc'),
                        'Trantype': order.get('Trantype'),
                        'Nstordno': order.get('Nstordno')
                    })

        # Print results for symbols with both 'open' and 'trigger pending' orders
        for symbol, details in orders_by_symbol.items():
            if details['open'] and details['trigger_pending']:
                print(f"\nSymbol: {symbol}")

                # Print Open Order Details
                open_order = details['open']
                print(f"  Open Order: Price: {open_order['Prc']}, "
                      f"Pcode: {open_order['Pcode']}, Qty: {open_order['Qty']}, "
                      f"Trantype: {open_order['Trantype']}, Order ID: {open_order['Nstordno']}")

                # Print Trigger Pending Order Details
                tp_order = details['trigger_pending']
                print(f"  Trigger Pending Order: Trigger Price: {tp_order['Trgprc']}, "
                      f"Price: {tp_order['Prc']}, Qty: {tp_order['Qty']}, "
                      f"Prctype: {tp_order['Prctype']}, Pcode: {tp_order['Pcode']}, "
                      f"Trantype: {tp_order['Trantype']}, Order ID: {tp_order['Nstordno']}")

                # Print Completed Orders if available
                if details['complete']:
                    print("  Completed Orders:")
                    for completed in details['complete']:
                        print(f"    Qty: {completed['Qty']}, Avg Price: {completed['Avgprc']}, "
                              f"Transaction Type: {completed['Trantype']}, Order ID: {completed['Nstordno']}")

    except Exception as e:
        print(f"Error while processing orders: {e}")



# Execute the cleanup function
modify_stoploss_orders()
