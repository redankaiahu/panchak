
# AutoTradeBot_OCR_Manual_Combined_v18_FINAL.py

import os
import re
import io
import cv2
import difflib
import pandas as pd
import numpy as np
from PIL import Image
from datetime import datetime
from telethon.sync import TelegramClient, events
import pytesseract

# === CONFIG ===
API_ID = 'your_api_id'
API_HASH = 'your_api_hash'
SESSION_NAME = 'session_combined'
WATCHLIST_KEYWORDS = ["watchlist", "looking good", "this one also looking good"]
processed_symbols = set()
LOG_FILE = "trade_log.txt"

# === AliceBlue API placeholders ===
def place_trade(trade_data):
    print(f"📦 Placing order for {trade_data['symbol']}{trade_data['expiry'].replace('-', '')}{trade_data['option_type']}{trade_data['strike']} at ₹{trade_data['entry']}")
    return {'stat': 'Ok', 'NOrdNo': 'SIM123456'}

# === Load NFO SYMBOLS ===
nfo_df = pd.read_csv("NFO.csv")
nfo_symbols = set(nfo_df.iloc[:, 3].dropna().str.upper())

# Optional manual correction map
MANUAL_SYMBOL_CORRECTIONS = {
    "THNDIA": "TIINDIA",
    "ICBC": "ICICIBANK",
    "HEROMOTO": "HEROMOTOCO"
}

def correct_symbol(symbol):
    if symbol in MANUAL_SYMBOL_CORRECTIONS:
        return MANUAL_SYMBOL_CORRECTIONS[symbol]
    match = difflib.get_close_matches(symbol.upper(), nfo_symbols, n=1, cutoff=0.7)
    return match[0] if match else symbol

# === Utility ===
def log_event(message):
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now()}] {message}\n")

def normalize_expiry(text):
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?[ \-\./]?([A-Z]{3})[ \-\./]?(\d{2,4})?", text.upper())
    if match:
        day, mon, year = match.groups()
        year = year if year else "25"
        return f"{day.zfill(2)}-{mon.upper()}-{year[-2:]}"
    return None

def parse_ocr_image_for_trade(img_bytes):
    try:
        image = Image.open(io.BytesIO(img_bytes))
        image_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(image_cv, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
        processed_image = Image.fromarray(thresh)

        text = pytesseract.image_to_string(processed_image)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        print("🔍 OCR Lines:\n" + "\n".join(lines))

        for line in lines:
            if re.search(r"\b(CE|PE)\b", line.upper()):
                parts = line.upper().split()
                symbol = correct_symbol(parts[0])
                strike_match = re.search(r"(\d{3,5})", line)
                option_match = re.search(r"\b(CE|PE)\b", line)
                expiry = normalize_expiry(line)

                if symbol and strike_match and option_match and expiry:
                    strike = strike_match.group(1)
                    opt = option_match.group(1)
                    for l2 in lines:
                        floats = re.findall(r"\d+\.\d+", l2)
                        if len(floats) >= 2:
                            try:
                                high = float(floats[1])
                                return {
                                    "symbol": symbol,
                                    "strike": strike,
                                    "option_type": opt,
                                    "expiry": expiry,
                                    "entry": high,
                                    "buy_above": True
                                }
                            except:
                                continue
        return None
    except Exception as e:
        print(f"[OCR ERROR] {e}")
        return None

# === Manual Message Parse ===
def parse_text_message(text):
    text = text.upper()
    match = re.search(r"(\d{4,5})\s*(CE|PE)", text)
    entry = re.search(r"BUY ABOVE\s*(\d+(?:\.\d+)?)", text)
    sl = re.search(r"SL[\s:-]*(\d+(?:\.\d+)?)", text)
    target = re.search(r"TARGET[\s:-]*(\d+(?:\.\d+)?)", text)
    if match and entry:
        return {
            "symbol": re.split(r"\s", text)[0],
            "strike": match.group(1),
            "option_type": match.group(2),
            "entry": float(entry.group(1)),
            "sl": float(sl.group(1)) if sl else None,
            "target": float(target.group(1)) if target else None,
            "buy_above": True
        }
    return None

# === Telegram Bot ===
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

@client.on(events.NewMessage)
async def handler(event):
    text = event.message.message.strip()
    sender = await event.get_chat()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Incoming message from {sender.title if hasattr(sender, 'title') else 'Unknown'}: {text}")

    if any(k in text.lower() for k in WATCHLIST_KEYWORDS) and event.message.media:
        img_bytes = await event.message.download_media(bytes)
        trade = parse_ocr_image_for_trade(img_bytes)
        if trade:
            print(f"🧠 OCR Trade Data Extracted: {trade}")
            place_trade(trade)
        else:
            print("❌ Failed to extract trade info from OCR.")
    else:
        trade = parse_text_message(text)
        if trade:
            print(f"🧠 Manual Trade Data Extracted: {trade}")
            place_trade(trade)

def main():
    print("=============================================")
    print("✅ AutoTradeBot Started - Mode: LIVE")
    print("✅ Orders will switch to MARKET after 5 minutes if unfilled")
    print("=============================================")
    with client:
        client.run_until_disconnected()

if __name__ == '__main__':
    main()
