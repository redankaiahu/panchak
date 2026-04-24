from telethon import TelegramClient, events
import json
from datetime import datetime
import os
import pandas as pd
import numpy as np
import threading
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging
import time as time_module  # renamed to avoid conflict with datetime
from pya3 import *
from pya3 import Aliceblue
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


# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

# Load NFO.csv once at the start
nfo_df = pd.read_csv('NFO.csv')
# Convert expiry dates correctly
nfo_df['Expiry Date'] = pd.to_datetime(nfo_df['Expiry Date'], errors='coerce', dayfirst=True)

def correct_symbol_from_nfo(symbol, nfo_df):
    symbol = symbol.upper().strip()

    # Direct match
    if symbol in nfo_df['Symbol'].unique():
        return symbol

    # Try cleaning and matching (remove non-letters)
    cleaned_symbol = ''.join(filter(str.isalpha, symbol))
    possible_matches = nfo_df['Symbol'].unique()

    for possible in possible_matches:
        if cleaned_symbol.startswith(possible):
            return possible

    # Fallback if nothing found
    return symbol


def get_trading_symbol_from_nfo(symbol, strike, option_type, nfo_df):
    today = datetime.now().date()

    # Filter matching symbol, strike, option_type
    filtered = nfo_df[
        (nfo_df['Symbol'].str.upper() == symbol.upper()) &
        (nfo_df['Strike Price'].astype(float) == float(strike)) &
        (nfo_df['Option Type'].str.upper() == option_type.upper())
    ]

    if filtered.empty:
        print(f"⚠️ No matching contract found for {symbol} {strike} {option_type}.")
        return None, None

    # Filter only future expiry contracts
    future_contracts = filtered[filtered['Expiry Date'].dt.date >= today]

    if future_contracts.empty:
        print(f"⚠️ No future expiry available for {symbol} {strike} {option_type}.")
        return None, None

    # Pick nearest expiry
    contract = future_contracts.sort_values('Expiry Date').iloc[0]

    expiry_date = contract['Expiry Date']
    day = expiry_date.strftime('%d')  # 2 digit day
    month = expiry_date.strftime('%b').upper()  # 3-letter month (JAN, FEB, etc.)
    year = expiry_date.strftime('%y')  # Last two digits of year

    # 🔥 Important Correction
    if option_type.upper() == "CE":
        option_code = "C"
    elif option_type.upper() == "PE":
        option_code = "P"
    else:
        option_code = option_type.upper()

    # Build AliceBlue format trading symbol correctly
    trading_symbol = f"{symbol.upper()}{day}{month}{year}{option_code}{int(strike)}"
    lot_size = int(contract['Lot Size'])

    return trading_symbol, lot_size




# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

USER_ID = config['user_id']
API_KEY = config['api_key']
ACCESS_TOKEN = config['access_token']
MODE = config['mode']
TELEGRAM_API_ID = config['telegram_api_id']
TELEGRAM_API_HASH = config['telegram_api_hash']
TELEGRAM_SESSION = config['telegram_session']
ORDER_WAIT_TIME_MINUTES = config['order_wait_time_minutes']

client = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)


placed_orders = {}
lot_sizes = {}

# Load saved lot sizes at startup
if os.path.exists("lot_sizes.json"):
    try:
        with open("lot_sizes.json", "r") as f:
            lot_sizes = json.load(f)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Loaded saved lot sizes.")
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Failed to load lot_sizes.json: {e}")
        lot_sizes = {}

def get_lot_size(symbol_name):
    if symbol_name in lot_sizes:
        return lot_sizes[symbol_name]
    try:
        instrument = alice.get_instrument_by_symbol('NFO', symbol_name)
        if isinstance(instrument, dict):
            print(f"Error: Symbol {symbol_name} not found in Contract Master.")
            return 25  # fallback
        lot_size = int(instrument.lot_size)
        lot_sizes[symbol_name] = lot_size
        with open("lot_sizes.json", "w") as f:
            json.dump(lot_sizes, f)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Fetched & saved lot size for {symbol_name}: {lot_size}")
        return lot_size
    except Exception as e:
        print(f"Error fetching lot size for {symbol_name}: {e}")
        return 25  # fallback


def auto_switch_to_market(order_id, full_symbol):
    try:
        orderbook = alice.order_data()
        for order in orderbook:
            if order['Nstordno'] == order_id and order['Status'].lower() == 'open':
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ⏳ Limit Order {order_id} still open after 1 minute. Switching to MARKET...")
                try:
                    instrument = alice.get_instrument_by_symbol('NFO', full_symbol)
                    modified = alice.modify_order(
                        transaction_type=AliceBlue.TRANSACTION_TYPE_BUY,
                        instrument=instrument,
                        product_type=AliceBlue.PRODUCT_MIS,
                        order_id=order_id,
                        order_type=AliceBlue.ORDER_TYPE_MARKET,
                        quantity=order['Qty'],
                        price=0.0,
                        trigger_price=0.0
                    )
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔥 Order switched to MARKET successfully: {modified}")
                except Exception as mod_err:
                    print(f"Error while switching to MARKET: {mod_err}")
                break
    except Exception as check_error:
        print(f"Error checking order status: {check_error}")



def process_signal(message_text):
    try:
        # First split into multiple signals (if any)
        signals = []
        current_signal = ""

        for line in message_text.split('\n'):
            line_upper = line.upper()
            if ("BUY ABOVE" in line_upper or "LOOKS GOOD" in line_upper or (line.startswith("#") and ("CE" in line_upper or "PE" in line_upper))):
                if current_signal:
                    signals.append(current_signal.strip())
                    current_signal = ""
            current_signal += line + '\n'

        if current_signal:
            signals.append(current_signal.strip())

        for signal_text in signals:
            parse_single_signal(signal_text)

    except Exception as e:
        print(f"Error processing multiple signals: {e}")

def parse_single_signal(signal_text):
    try:
        lines = signal_text.split('\n')
        symbol = None
        strike = None
        option_type = None
        entry_price = None
        sl_price = None
        target_price = None

        for line in lines:
            line = line.strip().upper()

            # Skip disclaimers
            if any(word in line for word in ["DISCLAIMER", "DO YOUR OWN RESEARCH", "F&O IS RISKY", "HERO ZERO", "RISKY"]):
                continue

            # Detect CMP (Entry Price)
            if "CMP" in line:
                try:
                    after_cmp = line.split("CMP")[-1].strip()
                    entry_text = after_cmp.split()[0].replace("+", "").replace(",", "").split("-")[0]
                    entry_price = float(entry_text)
                except:
                    pass

            # Detect SL
            if "SL" in line and "TARGET" not in line:
                try:
                    after_sl = line.split("SL")[-1].strip()
                    sl_text = after_sl.split()[0].replace("+", "").replace(",", "")
                    sl_price = float(sl_text)
                except:
                    pass

            # Detect Target
            if "TARGET" in line:
                try:
                    after_target = line.split("TARGET")[-1].strip()
                    target_text = after_target.replace("-", "").replace(":", "").split()[0]
                    if "/" in target_text:
                        target_text = target_text.split("/")[0]
                    if "," in target_text:
                        target_text = target_text.split(",")[0]
                    target_price = float(target_text)
                except:
                    pass

            # Detect Symbol, Strike, Option
            if "CE" in line or "PE" in line:
                words = line.replace("#", "").replace(".", "").split()
                for idx, word in enumerate(words):
                    if word.endswith("CE") or word.endswith("PE"):
                        option_type = "CE" if "CE" in word else "PE"
                        # Search leftwards for Strike and Symbol
                        if idx >= 1:
                            try:
                                strike = ''.join(filter(str.isdigit, words[idx - 1]))
                            except:
                                pass
                        if idx >= 2:
                            symbol = words[idx - 2]
                        break

        if symbol and strike and option_type and entry_price:
            trade_data = {
                "symbol": symbol,
                "strike": strike,
                "option_type": option_type,
                "entry": entry_price,
                "sl": sl_price,
                "target": target_price
            }
            place_trade(trade_data)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Incomplete signal, skipped.")
            print(f"[DEBUG] symbol={symbol}, strike={strike}, option_type={option_type}, entry={entry_price}, sl={sl_price}, target={target_price}")

    except Exception as e:
        print(f"Error parsing single signal: {e}")



def clean_message(msg):
    # Remove unnecessary lines like Note, Disclaimer, etc
    cleaned = []
    for line in msg.split('\n'):
        if "NOTE" in line.upper() or "DISCLAIMER" in line.upper() or "SEBI" in line.upper():
            continue
        cleaned.append(line)
    return ' '.join(cleaned).replace('*', '').replace(':', '').upper()

def parse_signal(message_text):
    symbol = None
    strike = None
    option_type = None
    entry = None
    sl = None
    target = None

    # Clean the message first
    msg = clean_message(message_text)

    # Extract Stock Name (more carefully)
    stock_match = re.search(r'STOCK NAME\s*-?\s*([A-Z\s]+)', msg)
    if stock_match:
        symbol = stock_match.group(1).strip().replace(' ', '')  # Remove spaces

    # Extract Strike and Option Type
    strike_option_match = re.search(r'STRIKE\s*-?\s*(\d+)\s*(CALL|PUT)', msg)
    if strike_option_match:
        strike = strike_option_match.group(1)
        opt_type = strike_option_match.group(2)
        option_type = 'CE' if opt_type == 'CALL' else 'PE'

    # Extract Range level (Entry price)
    entry_match = re.search(r'(\d+)[-/](\d+)\s*RANGE', msg)
    if entry_match:
        low = float(entry_match.group(1))
        high = float(entry_match.group(2))
        entry = min(low, high)  # 🛠 Take lower value as entry

    # Extract Stop Loss
    sl_match = re.search(r'SL\s*-?\s*(\d+)', msg)
    if sl_match:
        sl = float(sl_match.group(1))

    # Extract Target (biggest target)
    target_match = re.search(r'TARGETS?-?\s*([\d/]+)', msg)
    if target_match:
        targets = list(map(int, target_match.group(1).split('/')))
        target = max(targets)  # 🛠 Take highest value as final target

    # DEBUG Print
    print(f"[DEBUG] symbol={symbol}, strike={strike}, option_type={option_type}, entry={entry}, sl={sl}, target={target}")

    if all([symbol, strike, option_type, entry]):
        return {
            "symbol": symbol,
            "strike": strike,
            "option_type": option_type,
            "entry": entry,
            "sl": sl,
            "target": target
        }
    else:
        return None




def place_trade(trade_data):
    try:
        key = f"{trade_data['symbol']}_{trade_data['strike']}_{trade_data['option_type']}"
        today = datetime.now().strftime("%Y-%m-%d")
        if placed_orders.get(key) == today:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Already traded {key} today, skipping.")
            return

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Preparing to place order: {trade_data}")

        # 🛠 Correct the symbol from NFO before placing
        corrected_symbol = correct_symbol_from_nfo(trade_data['symbol'], nfo_df)
        if corrected_symbol != trade_data['symbol']:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Corrected symbol: {trade_data['symbol']} -> {corrected_symbol}")
            trade_data['symbol'] = corrected_symbol

        # 🔥 Use corrected expiry-based trading symbol fetch
        trading_symbol, quantity = get_trading_symbol_from_nfo(
            trade_data['symbol'], trade_data['strike'], trade_data['option_type'], nfo_df
        )

        if not trading_symbol:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Cannot place order: Trading Symbol not found.")
            return

        if MODE == "paper":
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PAPER MODE] Simulating Buy {trading_symbol} at {trade_data['entry']} Qty={quantity}")
        else:
            try:
                instrument = alice.get_instrument_by_symbol('NFO', trading_symbol)
                if isinstance(instrument, dict):
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Cannot place LIVE order: {trading_symbol} not found in contract master.")
                    return

                order = alice.place_order(
                    transaction_type=TransactionType.Buy,
                    instrument=instrument,
                    quantity=quantity,
                    order_type=OrderType.Limit,
                    product_type=ProductType.Intraday,
                    price=trade_data['entry'],
                    trigger_price=None,
                    stop_loss=trade_data.get('sl'),
                    square_off=trade_data.get('target'),
                    trailing_sl=None,
                    is_amo=False
                )
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ LIVE Order placed successfully: {order}")

# Correct way
order_id = order['NOrdNo']
threading.Timer(60, auto_switch_to_market, args=[order_id, trading_symbol]).start()

            except Exception as order_error:
                print(f"Error placing LIVE order: {order_error}")

        placed_orders[key] = today

    except Exception as e:
        print(f"Error placing trade: {e}")





@client.on(events.NewMessage)
async def my_event_handler(event):
    try:
        chat = await event.get_chat()
        chat_title = chat.title.upper() if hasattr(chat, 'title') and chat.title else ""

        # Only monitor allowed groups
        if not any(group.upper() in chat_title for group in config["groups_to_monitor"]):
            return  # Ignore message if not from allowed groups

        message_text = event.message.message.strip()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Incoming message from {chat_title}: {message_text}")

        trade_data = parse_signal(message_text)
        if trade_data:
            place_trade(trade_data)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Incomplete signal, skipped.")

    except Exception as e:
        print(f"Error in Telegram handler: {e}")


def main():
    try:
        print(f"=============================================")
        print(f"✅ AutoTradeBot Started - Mode: {MODE.upper()}")
        print(f"✅ Orders will switch to MARKET after {ORDER_WAIT_TIME_MINUTES} minutes if unfilled")
        print(f"=============================================")

        client.start()
        client.run_until_disconnected()

    except KeyboardInterrupt:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Emergency STOP detected! Closing bot safely.")
    except Exception as e:
        print(f"Fatal Error: {e}")

if __name__ == "__main__":
    main()
