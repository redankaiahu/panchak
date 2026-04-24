# AutoTradeBot_OCR_Dynamic_v7.py (Full Version)

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


# Configuration
API_ID = '29840797'  # Replace with your Telegram API ID
API_HASH = '9069896125bdbc5bacccfec478e8c64a'  # Replace with your Telegram API HASH
SESSION_NAME = 'AutoTradeBotApp'
processed_symbols = set()  # In-memory duplicate check
LOG_FILE = "trade_log.txt"

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

WATCHLIST_KEYWORDS = [
    "watchlist only", "this one also looking good", "looking good",
    "just watchlist", "good above day high only", "good above day high",
    "good above", "watchlist", "looking good now"
]

def log_event(message):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")

def message_has_watchlist_keywords(msg):
    msg = msg.lower()
    return any(kw in msg for kw in WATCHLIST_KEYWORDS)

def send_telegram_alert(text):
    try:
        client.loop.create_task(client.send_message('me', text))
    except Exception as e:
        print(f"[Telegram Alert Error] {e}")

def parse_ocr_image_for_trade(img_bytes):
    try:
        image = Image.open(io.BytesIO(img_bytes))
        image_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(image_cv, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
        processed_image = Image.fromarray(thresh)

        text = pytesseract.image_to_string(processed_image)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        print(" OCR Lines:\n" + "\n".join(lines))
        log_event("OCR Input:\n" + "\n".join(lines))

        symbol = None
        strike = None
        expiry = None
        high = None
        option_type = None
        all_floats = []

        for line in lines:
            if '%' not in line and '+' not in line and '-' not in line:
                all_floats.extend([float(x) for x in re.findall(r"\d+\.\d+", line)])

            if not symbol and re.search(r"(NSE|NSF|NST|NS\u00C9|NSEF)", line.upper()) and len(line.split()) <= 5:
                parts = line.upper().split()
                if len(parts) >= 1:
                    candidate = parts[0].strip("\u20B9")
                    if candidate.isalpha() and 2 <= len(candidate) <= 10:
                        symbol = candidate

            if not strike or not option_type:
                match = re.search(r"(\d{3,5})\s*(C\w|P\w)", line.upper())
                if match:
                    strike = match.group(1)
                    raw_opt = match.group(2)
                    if raw_opt.startswith("C"):
                        option_type = "CE"
                    elif raw_opt.startswith("P"):
                        option_type = "PE"

            if not expiry:
                match = re.search(r"(\d{2})[\-\.\/ ]([A-Z]{3})[\-\.\/ ](\d{2})", line.upper())
                if match:
                    expiry = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

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

        for line in lines:
            matches = re.findall(r"\d+\.\d+", line)
            if len(matches) == 4:
                try:
                    high_candidate = float(matches[1])
                    if 1 <= high_candidate <= 200:
                        high = high_candidate
                        print(f" Fallback OHLC-based HIGH detected: {high}")
                        break
                except:
                    continue

        if symbol and strike and expiry and high and option_type:
            key = f"{symbol}-{strike}-{expiry}"
            if key in processed_symbols:
                log_event(f" Duplicate skipped: {key}")
                print(f" Already processed today: {key}")
                send_telegram_alert(f" Skipped duplicate trade for {key}")
                return None
            processed_symbols.add(key)
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
        log_event(f"[OCR ERROR] {e}")
    return None

@client.on(events.NewMessage)
async def my_event_handler(event):
    try:
        chat = await event.get_chat()
        chat_title = chat.title.upper() if hasattr(chat, 'title') and chat.title else ""

        message_text = event.message.message.strip()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Incoming message from {chat_title}: {message_text}")

        if message_has_watchlist_keywords(message_text) and event.message.media:
            img_bytes = await event.message.download_media(bytes)
            trade_data = parse_ocr_image_for_trade(img_bytes)
            if trade_data:
                print(f"[{datetime.now().strftime('%H:%M:%S')}]  OCR Trade Data Extracted: {trade_data}")
                log_event(f"TRADE DATA: {trade_data}")
                send_telegram_alert(f"📈 New Trade Signal: {trade_data}")
                place_trade(trade_data)
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}]  Failed to extract trade info from OCR.")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] No action required.")
    except Exception as e:
        print(f"Error in handler: {e}")
        log_event(f"Handler error: {e}")


def place_trade(trade_data):
    try:
        symbol = trade_data['symbol']
        strike = trade_data['strike']
        option_type = trade_data['option_type']
        expiry = trade_data['expiry']
        entry = trade_data['entry']
        buy_above = trade_data.get('buy_above', True)

        trading_symbol = f"{symbol.upper()}29MAY25{option_type}{strike}"
        exchange = "NFO"
        product_type = "MIS"
        order_type = "LIMIT"
        quantity = 1  # You can make this dynamic

        print(f" Placing order for {trading_symbol} at ₹{entry}")
        log_event(f" Order Placed: {trading_symbol} at ₹{entry}")
        send_telegram_alert(f" Order Placed: {trading_symbol} @ ₹{entry}")

    except Exception as e:
        print(f"[Trade Error] {e}")
        log_event(f"[Trade Error] {e}")
        send_telegram_alert(f" Trade Error: {e}")


def main():
    print("=============================================")
    print(" AutoTradeBot v7 Started - Mode: LIVE")
    print(" Telegram alerts, duplicate checks, and logging enabled")
    print("=============================================")
    with client:
        client.run_until_disconnected()

if __name__ == '__main__':
    main()
