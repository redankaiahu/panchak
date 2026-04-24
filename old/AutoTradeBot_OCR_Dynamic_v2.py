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

from PIL import Image
import pytesseract
import io
import cv2


# Set tesseract path for Windows (update as needed)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"



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
    symbol = symbol.upper().replace('&', 'AND').replace(' ', '')
    available_symbols = nfo_df['Symbol'].unique()

    # Direct match
    if symbol in available_symbols:
        return symbol

    # Partial match (contains)
    partial_matches = [s for s in available_symbols if symbol in s or s in symbol]
    if len(partial_matches) == 1:
        return partial_matches[0]  # Only one match, return it

    # Try startswith match
    startswith_matches = [s for s in available_symbols if s.startswith(symbol)]
    if len(startswith_matches) == 1:
        return startswith_matches[0]

    # Fallback
    return symbol


def extract_symbol_from_message(msg, nfo_df):
    # Priority 1: Hashtag based symbol extraction
    hashtag_match = re.search(r'#([A-Z&]+)', msg)
    if hashtag_match:
        raw_symbol = hashtag_match.group(1).replace('&', 'AND')
        corrected_symbol = correct_symbol_from_nfo(raw_symbol, nfo_df)
        return corrected_symbol

    # Priority 2: Direct pattern e.g. NIFTY 24000 CE
    direct_match = re.search(r'([A-Z]+)\s*(\d+)\s*(CE|PE)', msg)
    if direct_match:
        raw_symbol = direct_match.group(1)
        corrected_symbol = correct_symbol_from_nfo(raw_symbol, nfo_df)
        return corrected_symbol

    # Priority 3: Stock Name style (OPTION TRADE format)
    stock_match = re.search(r'STOCK NAME\s*-?\s*([A-Z\s]+)', msg)
    if stock_match:
        raw_symbol = stock_match.group(1).strip().replace(' ', '')
        corrected_symbol = correct_symbol_from_nfo(raw_symbol, nfo_df)
        return corrected_symbol

    return None  # not found


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
ORDER_BUFFER = config.get("order_price_buffer", 1.0)  # Default to 1.0 if not defined

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








WATCHLIST_KEYWORDS = [
    "watchlist only", "this one also looking good", "looking good",
    "just watchlist", "good above day high only", "good above day high",
    "good above", "watchlist", "looking good now"
]

def message_has_watchlist_keywords(msg):
    msg = msg.lower()
    return any(kw in msg for kw in WATCHLIST_KEYWORDS)



def parse_ocr_image_for_trade(img_bytes):
    try:
        # Preprocess the image using OpenCV
        import cv2
        import numpy as np

        image = Image.open(io.BytesIO(img_bytes))
        image_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(image_cv, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
        processed_image = Image.fromarray(thresh)

        # OCR
        text = pytesseract.image_to_string(processed_image)
        lines = text.splitlines()
        symbol = None
        strike = None
        expiry = None
        high = None
        option_type = None

        for line in lines:
            if not symbol and "NSE FO" in line.upper():
                parts = line.upper().split("NSE FO")[0].strip().split()
                if parts:
                    symbol = parts[-1].replace("₹", "").strip()

            if not strike or not option_type:
                match = re.search(r"(\d{3,5})\s*(CE|PE)", line.upper())
                if match:
                    strike = match.group(1)
                    option_type = match.group(2)

            if not expiry:
                match = re.search(r"(\d{2}-[A-Z]{3}-\d{2})", line.upper())
                if match:
                    expiry = match.group(1)

            if not high:
                if "HIGH" in line.upper():
                    # Try inline and next line both
                    inline = re.search(r"HIGH\s*(\d+(?:\.\d+)?)", line.upper())
                    if inline:
                        high = float(inline.group(1))
                    else:
                        idx = lines.index(line)
                        if idx + 1 < len(lines):
                            try:
                                high = float(lines[idx + 1])
                            except:
                                continue
                # Fallback: scan for 3 numbers in a row (like High/Low/PrevClose line)
                if not high and re.search(r"\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+", line):
                    nums = [float(n) for n in re.findall(r"\d+\.\d+", line)]
                    if len(nums) == 3:
                        high = nums[0]

        if symbol and strike and expiry and high and option_type:
            return {
                "symbol": symbol,
                "strike": strike,
                "option_type": option_type,
                "entry": high,
                "expiry": expiry,
                "buy_above": True
            }
    except Exception as e:
        print(f"[OCR ERROR] {e}")
    return None


def clean_message(msg):
    # Remove unnecessary lines like Note, Disclaimer, Educational, etc
    cleaned = []
    for line in msg.split('\n'):
        if any(x in line.upper() for x in ["NOTE", "DISCLAIMER", "SEBI", "SCAN", "GTT ORDER", "EDUCATIONAL", "CONSULT"]):
            continue
        cleaned.append(line)
    return ' '.join(cleaned).replace('*', '').replace(':', '').upper()


def parse_signal(message_text, nfo_df):
    symbol = None
    strike = None
    option_type = None
    entry = None
    sl = None
    target = None
    buy_above = False
    cmp_value = None

    msg = clean_message(message_text)

    # ✅ Get correct symbol
    symbol = extract_symbol_from_message(msg, nfo_df)

    # ✅ NEW: Parse Strike - 320CALL Option or 5000PUT Option format
    alt_strike_match = re.search(r'STRIKE\s*-\s*(\d+)(CALL|PUT)', msg)
    if alt_strike_match:
        strike = alt_strike_match.group(1)
        opt_word = alt_strike_match.group(2)
        option_type = 'CE' if opt_word == 'CALL' else 'PE'

    # Fallback: classic format e.g. 3700 CE
    if not strike or not option_type:
        strike_option_match = re.search(r'(\d+)\s*(CE|PE)', msg)
        if strike_option_match:
            strike = strike_option_match.group(1)
            option_type = strike_option_match.group(2).strip().upper()

    # Strike + Option type parsing
    strike_option_match = re.search(r'(\d+)\s*(CE|PE)', msg)
    if strike_option_match:
        strike = strike_option_match.group(1)
        option_type = strike_option_match.group(2).strip().upper()

    # Parse BUY ABOVE first
    buy_above_match = re.search(r'BUY ABOVE\s*(\d+(?:\.\d+)?)', msg)
    if buy_above_match:
        entry_from_buy_above = float(buy_above_match.group(1))
        buy_above = True
    else:
        entry_from_buy_above = None

    # Parse Range level
    range_match = re.search(r'(\d+(?:\.\d+)?)[-/](\d+(?:\.\d+)?)\s*RANGE', msg)
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        entry_from_range = min(low, high)
    else:
        entry_from_range = None

    # Parse CMP
    cmp_match = re.search(r'CMP\s*-?\s*(\d+(?:\.\d+)?)', msg)
    if cmp_match:
        cmp_value = float(cmp_match.group(1))

    # Parse @ price (eg. NIFTY 24250 CE @ 106)
    if not cmp_value:
        at_price_match = re.search(r'@\s*(\d+(?:\.\d+)?)', msg)
        if at_price_match:
            cmp_value = float(at_price_match.group(1))

    # Parse SL
    sl_match = re.search(r'SL\s*-?\s*(\d+(?:\.\d+)?)', msg)
    if sl_match:
        sl = float(sl_match.group(1))

    # Parse Target
    target_match = re.search(r'TARGETS?-?\s*([\d/,+.]+)', msg)
    if target_match:
        target_text = target_match.group(1)
        target_numbers = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', target_text)]
        if target_numbers:
            target = max(target_numbers)

    # Final Entry Decision Logic
    if entry_from_buy_above:
        entry = entry_from_buy_above
    elif entry_from_range:
        entry = entry_from_range
    elif cmp_value and strike:
        try:
            strike_val = float(strike)
            if abs(strike_val - cmp_value) / strike_val > 0.05:
                # CMP far → CMP is option LTP
                entry = cmp_value
            else:
                # CMP near strike → stock price
                pass
        except:
            pass
    elif cmp_value:
        entry = cmp_value

    # Final Clean-up
    if symbol and symbol.endswith('CMP'):
        symbol = symbol.replace('CMP', '')

    print(f"[DEBUG] symbol={symbol}, strike={strike}, option_type={option_type}, entry={entry}, sl={sl}, target={target}, buy_above={buy_above}")

    if all([symbol, strike, option_type, entry]):
        return {
            "symbol": symbol,
            "strike": strike,
            "option_type": option_type,
            "entry": entry,
            "sl": sl,
            "target": target,
            "buy_above": buy_above
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

        if MODE == "paper":
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PAPER MODE] Simulating {key} at {trade_data['entry']}")
        else:
            try:
                #trading_symbol = get_trading_symbol(trade_data['symbol'], trade_data['strike'], trade_data['option_type'])
                trading_symbol, lot_size = get_trading_symbol_from_nfo(trade_data['symbol'], trade_data['strike'], trade_data['option_type'], nfo_df)
                instrument = alice.get_instrument_by_symbol('NFO', trading_symbol)

                if instrument is None:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Cannot place LIVE order: {trading_symbol} not found in contract master.")
                    return

                #quantity = get_lot_size(trading_symbol)
                quantity = lot_size

                if trade_data.get('buy_above'):
                    # ✅ Place GTT order (StopLossMarket Buy)
                    trigger = trade_data['entry']
                    limit_price = round(trigger + ORDER_BUFFER, 1)  # Buffer can be adjusted

                    order = alice.place_order(
                        transaction_type=TransactionType.Buy,
                        instrument=instrument,
                        quantity=quantity,
                        order_type=OrderType.StopLossLimit,  # ✅ Replaced SL-M with SL-L
                        product_type=ProductType.Delivery,      #ProductType.Intraday,
                        price=limit_price,
                        trigger_price=trigger,
                        stop_loss=trade_data.get('sl'),
                        square_off=trade_data.get('target'),
                        trailing_sl=None,
                        is_amo=False
                    )
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ GTT Trigger Order placed at {trade_data['entry']}")
                    print(f"[DEBUG] Order Response: {order}")
                    print(f"[DEBUG] Using trading symbol: {trading_symbol} with exchange: NFO")


                else:
                    # Normal Limit Order
                    order = alice.place_order(
                        transaction_type=TransactionType.Buy,
                        instrument=instrument,
                        quantity=quantity,
                        order_type=OrderType.Limit,
                        product_type=ProductType.Delivery,      #ProductType.Intraday,      # ProductType.Delivery
                        price=trade_data['entry'],
                        trigger_price=None,
                        stop_loss=trade_data.get('sl'),
                        square_off=trade_data.get('target'),
                        trailing_sl=None,
                        is_amo=False
                    )
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Limit Order placed at {trade_data['entry']}")
                    print(f"[DEBUG] Order Response: {order}")
                    print(f"[DEBUG] Using trading symbol: {trading_symbol} with exchange: NFO")

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

        if not any(group.upper() in chat_title for group in config["groups_to_monitor"]):
            return

        message_text = event.message.message.strip()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Incoming message from {chat_title}: {message_text}")

        if message_has_watchlist_keywords(message_text) and event.message.media:
            img_bytes = await event.message.download_media(bytes)
            trade_data = parse_ocr_image_for_trade(img_bytes)
            if trade_data:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🧠 OCR Trade Data Extracted: {trade_data}")
                place_trade(trade_data)
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Failed to extract trade info from OCR.")
        else:
            trade_data = parse_signal(message_text, nfo_df)
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
