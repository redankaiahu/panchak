
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
import difflib

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

NFO_SYMBOLS = ['011NSETEST', '021NSETEST', '031NSETEST', '041NSETEST', '051NSETEST', '061NSETEST', '071NSETEST', '081NSETEST', '091NSETEST', '101NSETEST', '111NSETEST', '121NSETEST', '131NSETEST', '141NSETEST', '151NSETEST', '161NSETEST', '171NSETEST', '181NSETEST', 'AARTIIND', 'ABB', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENSOL', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ALKEM', 'AMBUJACEM', 'ANGELONE', 'APLAPOLLO', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'ATGL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BANDHANBNK', 'BANKBARODA', 'BANKINDIA', 'BANKNIFTY', 'BEL', 'BHARATFORG', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSE', 'BSOFT', 'CAMS', 'CANBK', 'CDSL', 'CESC', 'CGPOWER', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'CROMPTON', 'CUMMINSIND', 'CYIENT', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELHIVERY', 'DIVISLAB', 'DIXON', 'DLF', 'DMART', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'ETERNAL', 'EXIDEIND', 'FEDERALBNK', 'FINNIFTY', 'GAIL', 'GLENMARK', 'GMRAIRPORT', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HFCL', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HINDZINC', 'HUDCO', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFCFIRSTB', 'IEX', 'IGL', 'IIFL', 'INDHOTEL', 'INDIANB', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INOXWIND', 'IOC', 'IRB', 'IRCTC', 'IREDA', 'IRFC', 'ITC', 'JINDALSTEL', 'JIOFIN', 'JSL', 'JSWENERGY', 'JSWSTEEL', 'JUBLFOOD', 'KALYANKJIL', 'KEI', 'KOTAKBANK', 'KPITTECH', 'LAURUSLABS', 'LICHSGFIN', 'LICI', 'LODHA', 'LT', 'LTF', 'LTIM', 'LUPIN', 'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MAXHEALTH', 'MCX', 'MFSL', 'MGL', 'MIDCPNIFTY', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NBCC', 'NCC', 'NESTLEIND', 'NHPC', 'NIFTY', 'NIFTYNXT50', 'NMDC', 'NTPC', 'NYKAA', 'OBEROIRLTY', 'OFSS', 'OIL', 'ONGC', 'PAGEIND', 'PATANJALI', 'PAYTM', 'PEL', 'PERSISTENT', 'PETRONET', 'PFC', 'PHOENIXLTD', 'PIDILITIND', 'PIIND', 'PNB', 'PNBHOUSING', 'POLICYBZR', 'POLYCAB', 'POONAWALLA', 'POWERGRID', 'PRESTIGE', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SHRIRAMFIN', 'SIEMENS', 'SJVN', 'SOLARINDS', 'SONACOMS', 'SRF', 'SUNPHARMA', 'SUPREMEIND', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAELXSI', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TATATECH', 'TCS', 'TECHM', 'TIINDIA', 'TITAGARH', 'TITAN', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'ULTRACEMCO', 'UNIONBANK', 'UNITDSPR', 'UPL', 'VBL', 'VEDL', 'VOLTAS', 'WIPRO', 'YESBANK', 'ZYDUSLIFE']


# Set tesseract path for Windows (update as needed)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# === Manual Symbol Corrections and Expiry Parser ===
MANUAL_SYMBOL_CORRECTIONS = {
    "THNDIA": "TIINDIA",
    "ICBC": "ICICIBANK",
    "HEROMOTO": "HEROMOTOCO"
}

def correct_symbol(symbol):
    if symbol in MANUAL_SYMBOL_CORRECTIONS:
        return MANUAL_SYMBOL_CORRECTIONS[symbol]
    matches = difflib.get_close_matches(symbol.upper(), NFO_SYMBOLS, n=1, cutoff=0.7)
    return matches[0] if matches else symbol

def normalize_expiry(text):
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?[ \-\./]?([A-Z]{3})[ \-\./]?(\d{2,4})?", text.upper())
    if match:
        day, mon, year = match.groups()
        year = year if year else "25"
        return f"{day.zfill(2)}-{mon.upper()}-{year[-2:]}"
    return None


def correct_symbol(ocr_symbol):
    if not ocr_symbol:
        return None
    matches = difflib.get_close_matches(ocr_symbol.upper(), NFO_SYMBOLS, n=1, cutoff=0.8)
    return matches[0] if matches else ocr_symbol.upper()


#

MANUAL_SYMBOL_CORRECTIONS = {
    "THNDIA": "TIINDIA",
    "HEROMOTO": "HEROMOTOCO",
    "ICBC": "ICICIBANK"
}

NFO_SYMBOLS = set(nfo_df[nfo_df.columns[3]].dropna().astype(str).str.upper().unique())

def correct_symbol(symbol):
    symbol = symbol.upper()
    if symbol in MANUAL_SYMBOL_CORRECTIONS:
        return MANUAL_SYMBOL_CORRECTIONS[symbol]
    matches = difflib.get_close_matches(symbol, NFO_SYMBOLS, n=1, cutoff=0.7)
    return matches[0] if matches else symbol

def normalize_expiry(text):
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?[ \-\./]?([A-Z]{3})[ \-\./]?(\d{2,4})?", text.upper())
    if match:
        day, mon, year = match.groups()
        year = year if year else "25"
        return f"{day.zfill(2)}-{mon.upper()}-{year[-2:]}"
    return None

def correct_strike(symbol, strike, option_type, expiry):
    try:
        df = nfo_df[nfo_df[nfo_df.columns[3]].astype(str).str.upper() == symbol.upper()]
        df = df[df[nfo_df.columns[4]].str.upper().str.contains(option_type)]
        df = df[df[nfo_df.columns[2]].str.contains(expiry[:2])]
        df['STRIKE'] = pd.to_numeric(df[nfo_df.columns[5]], errors='coerce')
        valid_strikes = df['STRIKE'].dropna().tolist()
        if valid_strikes:
            closest = min(valid_strikes, key=lambda x: abs(x - int(strike)))
            return str(int(closest))
    except:
        pass
    return strike


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
        import cv2
        import numpy as np

        image = Image.open(io.BytesIO(img_bytes))
        image_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(image_cv, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
        processed_image = Image.fromarray(thresh)

        text = pytesseract.image_to_string(processed_image)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        print("🔍 OCR Lines:\n" + "\n".join(lines))

        symbol = None
        strike = None
        expiry = None
        high = None
        option_type = None
        all_floats = []

        for line in lines:
            all_floats.extend([float(x) for x in re.findall(r"\d+\.\d+", line)])

            # Fuzzy NSE FO line detection
            if not symbol and re.search(r"(NSE|NSF|NST|NSÉ|NSEF)", line.upper()) and len(line.split()) <= 5:
                parts = line.upper().split()
                if len(parts) >= 1:
                    candidate = parts[0].strip("₹")
                    if candidate.isalpha() and 2 <= len(candidate) <= 10:
                        symbol = correct_symbol(candidate)

            # Strike and Option Type (with fuzzy match)
            if not strike or not option_type:
                match = re.search(r"(\d{3,5})\s*(C\w|P\w)", line.upper())
                if match:
                    strike = match.group(1)
                    raw_opt = match.group(2)
                    if raw_opt.startswith("C"):
                        option_type = "CE"
                    elif raw_opt.startswith("P"):
                        option_type = "PE"

            # Expiry extraction
            if not expiry:
                match = re.search(r"(\d{2})[\-\.]([A-Z]{3})[\-\.](\d{2})", line.upper())
                if match:
                    expiry = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

            # Try line-by-line HIGH parsing
            if not high:
                if "HIGH" in line.upper():
                    inline = re.search(r"HIGH\s*(\d+(?:\.\d+)?)", line.upper())
                    if inline:
                        high = float(inline.group(1))
                    else:
                        idx = lines.index(line)
                        if idx + 1 < len(lines):
                            try:
                                high = float(lines[idx + 1])
                            except:
                                pass

        # OHLC 4-float fallback
        for line in lines:
            matches = re.findall(r"\d+\.\d+", line)
            if len(matches) == 4:
                try:
                    high_candidate = float(matches[1])
                    if 1 <= high_candidate <= 300:
                        high = high_candidate
                        print(f"✅ Fallback OHLC-based HIGH detected: {high}")
                        break
                except:
                    continue

        # Reasonable max float fallback
        if not high and all_floats:
            reasonable = [x for x in all_floats if 1 <= x <= 300]
            if reasonable:
                high = max(reasonable)
                print(f"⚠️ Using fallback max float as HIGH: {high}")

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

                    origOrderId = alice.place_order(
                        transaction_type=TransactionType.Buy,
                        instrument=instrument,
                        quantity=quantity,
                        order_type=OrderType.StopLossLimit,  # ✅ Replaced SL-M with SL-L
                        product_type=ProductType.Intraday,#ProductType.Delivery,      #ProductType.Intraday,
                        price=limit_price,
                        trigger_price=trigger,
                        stop_loss=trade_data.get('sl'),
                        square_off=trade_data.get('target'),
                        trailing_sl=None,
                        is_amo=False
                    )
                    print(" ONE \n")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ GTT Trigger Order placed at {trade_data['entry']}")
                    print(f"[DEBUG] Order Response: {origOrderId}")
                    print(f"[DEBUG] Using trading symbol: {trading_symbol} with exchange: NFO")
                    time.sleep(5)  # Add a delay if required for execution to complete
                    order_number = origOrderId.get('NOrdNo')
                    if not order_number:
                        print("=======>>>> Order placement failed, no order number found.\n")
                        return
                    order_history = alice.get_order_history(order_number)  # Get the order history using the order number
                    executed_price = float(order_history['Avgprc'])  # Extract and convert average price to float
                    qty = int(order_history['Fillshares'])  # Extract and convert filled shares to integer
                    TICK_SIZE = 0.05
                    # Determine SL and target based on transaction type
                    entry_price = trade_data['entry']
                    stop_loss_price = round(entry_price * 0.90, 2)  # 10% below entry
                    trigger_price = round(stop_loss_price + 0.30, 2)
                    target_price = round(entry_price * 2.0, 2)      # 100% profit    #round_to_two_decimals(target_price_cal)

                    
                    
                            
                    sl_order_response = alice.place_order(
                            transaction_type=TransactionType.Sell,  # Opposite of the market order
                            instrument=instrument,
                            quantity=quantity,
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
                            transaction_type=TransactionType.Buy,  # Opposite of the market order
                            instrument=instrument,
                            quantity=quantity,
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
                    #print(f"<- <- <- <- <- <- <-    ###################     order_count: {order_count}      <- <- <- <- <- <- <- <- <- <- <- <- <-\n")
                    target_order_id = target_order_response.get('NOrdNo')




                else:
                    # Normal Limit Order
                    origOrderId = alice.place_order(
                        transaction_type=TransactionType.Buy,
                        instrument=instrument,
                        quantity=quantity,
                        order_type=OrderType.Limit,
                        product_type=ProductType.Intraday,   #ProductType.Delivery,      #ProductType.Intraday,      # ProductType.Delivery
                        price=trade_data['entry'],
                        trigger_price=None,
                        stop_loss=trade_data.get('sl'),
                        square_off=trade_data.get('target'),
                        trailing_sl=None,
                        is_amo=False
                    )
                    print(" TWO \n")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Limit Order placed at {trade_data['entry']}")
                    print(f"[DEBUG] Order Response: {origOrderId}")
                    print(f"[DEBUG] Using trading symbol: {trading_symbol} with exchange: NFO")
                    time.sleep(5)  # Add a delay if required for execution to complete
                    order_number = origOrderId.get('NOrdNo')
                    if not order_number:
                        print("=======>>>> Order placement failed, no order number found.\n")
                        return
                    order_history = alice.get_order_history(order_number)  # Get the order history using the order number
                    executed_price = float(order_history['Avgprc'])  # Extract and convert average price to float
                    qty = int(order_history['Fillshares'])  # Extract and convert filled shares to integer
                    TICK_SIZE = 0.05
                    entry_price = trade_data['entry']
                    stop_loss_price = round(entry_price * 0.90, 2)  # 10% below entry
                    trigger_price = round(stop_loss_price + 0.30, 2)
                    target_price = round(entry_price * 2.0, 2)      # 100% profit   
                            
                    sl_order_response = alice.place_order(
                            transaction_type=TransactionType.Sell,  # Opposite of the market order
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
                            transaction_type=TransactionType.Buy,  # Opposite of the market order
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
                    target_order_id = target_order_response.get('NOrdNo')



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


# --- Manual Message Parsing + Full Trade Logic ---
from telethon import TelegramClient, events
import json
from datetime import datetime
import os
import pandas as pd


# === Enhanced OCR Fixes ===
import difflib

MANUAL_SYMBOL_CORRECTIONS = {
    "THNDIA": "TIINDIA",
    "HEROMOTO": "HEROMOTOCO",
    "ICBC": "ICICIBANK"
}

NFO_SYMBOLS = set(nfo_df[nfo_df.columns[3]].dropna().astype(str).str.upper().unique())

def correct_symbol(symbol):
    symbol = symbol.upper()
    if symbol in MANUAL_SYMBOL_CORRECTIONS:
        return MANUAL_SYMBOL_CORRECTIONS[symbol]
    matches = difflib.get_close_matches(symbol, NFO_SYMBOLS, n=1, cutoff=0.7)
    return matches[0] if matches else symbol

def normalize_expiry(text):
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?[ \-\./]?([A-Z]{3})[ \-\./]?(\d{2,4})?", text.upper())
    if match:
        day, mon, year = match.groups()
        year = year if year else "25"
        return f"{day.zfill(2)}-{mon.upper()}-{year[-2:]}"
    return None

def correct_strike(symbol, strike, option_type, expiry):
    try:
        df = nfo_df[nfo_df[nfo_df.columns[3]].astype(str).str.upper() == symbol.upper()]
        df = df[df[nfo_df.columns[4]].str.upper().str.contains(option_type)]
        df = df[df[nfo_df.columns[2]].str.contains(expiry[:2])]
        df['STRIKE'] = pd.to_numeric(df[nfo_df.columns[5]], errors='coerce')
        valid_strikes = df['STRIKE'].dropna().tolist()
        if valid_strikes:
            closest = min(valid_strikes, key=lambda x: abs(x - int(strike)))
            return str(int(closest))
    except:
        pass
    return strike
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
                    origOrderId = alice.place_order(
                        transaction_type=TransactionType.Buy,
                        instrument=instrument,
                        quantity=quantity,
                        order_type=OrderType.StopLossMarket,  # Important
                        product_type=ProductType.Intraday,
                        price=0.0,
                        trigger_price=trade_data['entry'],
                        stop_loss=trade_data.get('sl'),
                        square_off=trade_data.get('target'),
                        trailing_sl=None,
                        is_amo=False
                    )
                    print(" THREE \n")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ GTT Trigger Order placed at {trade_data['entry']}")
                    print(f"[DEBUG] Order Response: {order}")
                    print(f"[DEBUG] Using trading symbol: {trading_symbol} with exchange: NFO")
                    time.sleep(5)  # Add a delay if required for execution to complete
                    order_number = origOrderId.get('NOrdNo')
                    if not order_number:
                        print("=======>>>> Order placement failed, no order number found.\n")
                        return
                    order_history = alice.get_order_history(order_number)  # Get the order history using the order number
                    executed_price = float(order_history['Avgprc'])  # Extract and convert average price to float
                    qty = int(order_history['Fillshares'])  # Extract and convert filled shares to integer
                    TICK_SIZE = 0.05
                    entry_price = trade_data['entry']
                    stop_loss_price = round(entry_price * 0.90, 2)  # 10% below entry
                    trigger_price = round(stop_loss_price + 0.30, 2)
                    target_price = round(entry_price * 2.0, 2)      # 100% profit  
                    sl_order_response = alice.place_order(
                            transaction_type=TransactionType.Sell,  # Opposite of the market order
                            instrument=instrument,
                            quantity=quantity,
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
                            transaction_type=TransactionType.Buy,  # Opposite of the market order
                            instrument=instrument,
                            quantity=quantity,
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
                    target_order_id = target_order_response.get('NOrdNo')




                else:
                    # Normal Limit Order
                    origOrderId = alice.place_order(
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
                    print(" FOUR \n")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Limit Order placed at {trade_data['entry']}")
                    print(f"[DEBUG] Order Response: {order}")
                    print(f"[DEBUG] Using trading symbol: {trading_symbol} with exchange: NFO")

                    time.sleep(5)  # Add a delay if required for execution to complete
                    order_number = origOrderId.get('NOrdNo')
                    if not order_number:
                        print("=======>>>> Order placement failed, no order number found.\n")
                        return
                    order_history = alice.get_order_history(order_number)  # Get the order history using the order number
                    executed_price = float(order_history['Avgprc'])  # Extract and convert average price to float
                    qty = int(order_history['Fillshares'])  # Extract and convert filled shares to integer
                    TICK_SIZE = 0.05
                    entry_price = trade_data['entry']
                    stop_loss_price = round(entry_price * 0.90, 2)  # 10% below entry
                    trigger_price = round(stop_loss_price + 0.30, 2)
                    target_price = round(entry_price * 2.0, 2)      # 100% profit  
                            
                    sl_order_response = alice.place_order(
                            transaction_type=TransactionType.Sell,  # Opposite of the market order
                            instrument=instrument,
                            quantity=quantity,
                            order_type=OrderType.StopLossLimit,#OrderType.Limit,#OrderType.SL,  # Stop Loss Order
                            product_type=ProductType.Intraday,
                            price=stop_loss_price,  # SL price
                            trigger_price=trigger_price,  # Trigger priTransactionType.Sellce
                            stop_loss=None,
                            square_off=None,
                            trailing_sl=None,
                            is_amo=False,
                            order_tag='sl_order'  # Custom tag for the SL order
                    )
                    print("Stop Loss order placed:", sl_order_response)
                    sl_order_id = sl_order_response.get('NOrdNo')
                

                    target_order_response = alice.place_order(
                            transaction_type=TransactionType.Buy,  # Opposite of the market order
                            instrument=instrument,
                            quantity=quantity,
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
                    target_order_id = target_order_response.get('NOrdNo')


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

        #trade_data = parse_signal(message_text)
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

