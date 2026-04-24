#AutoTradeBot.py
import json
import time
import pytesseract
import schedule
from telethon import TelegramClient, events
from alice_blue import AliceBlue
import threading
import os
from datetime import datetime

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
GROUPS_TO_MONITOR = config['groups_to_monitor']
ALERT_CHANNEL = config['private_alert_channel']
ORDER_WAIT_TIME_MINUTES = config['order_wait_time_minutes']

# Prepare Alice Blue object
alice = None

# Global variables
placed_orders = {}
lock = threading.Lock()


# Initialize Telegram Client
client = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Telegram Event Handler
@client.on(events.NewMessage)
async def my_event_handler(event):
    try:
        sender = await event.get_sender()
        group_title = event.chat.title if event.chat else ""
        group_username = sender.username if (sender and sender.username) else ""

        match_name = group_username or group_title

        message_text = event.message.message
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Incoming message from {match_name}: {message_text}")

        if match_name and match_name.lower() not in [g.lower() for g in GROUPS_TO_MONITOR]:
            return  # Ignore if not from monitored groups

        await process_signal(message_text)

    except Exception as e:
        print(f"Error in Telegram handler: {e}")





def process_signal(message_text):
    try:
        lines = message_text.split('\n')
        symbol = None
        strike = None
        option_type = None
        entry_price = None
        sl_price = None
        target_price = None

        for line in lines:
            line = line.strip().upper()
            if "BUY ABOVE" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "BUY" and parts[i+1] == "ABOVE":
                        entry_price = float(parts[i+2])
            elif "CMP" in line:
                try:
                    entry_price = float(line.split('CMP')[-1].strip())
                except:
                    pass
            elif "SL" in line and "TARGET" not in line:
                try:
                    sl_price = float(line.split('SL')[-1].strip())
                except:
                    pass
            elif "TARGET" in line:
                try:
                    target_price = float(line.split('TARGET')[-1].strip().split("/")[0])
                except:
                    pass
            elif "CE" in line or "PE" in line:
                # Try to parse symbol, strike and option type
                parts = line.replace("#", "").split()
                for part in parts:
                    if part.endswith("CE") or part.endswith("PE"):
                        option_type = "CE" if "CE" in part else "PE"
                        numbers = ''.join(filter(str.isdigit, part))
                        if numbers:
                            strike = numbers
                    else:
                        if not part.isdigit():
                            symbol = part

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

    except Exception as e:
        print(f"Error parsing signal: {e}")


def place_trade(trade_data):
    try:
        key = f"{trade_data['symbol']}_{trade_data['strike']}_{trade_data['option_type']}"

        # Check if already traded today
        today = datetime.now().strftime("%Y-%m-%d")
        if placed_orders.get(key) == today:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Already traded {key} today, skipping.")
            return

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Preparing to place order: {trade_data}")

        if MODE == "paper": ##Live
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PAPER MODE] Simulating buy {key} at {trade_data['entry']}")
        else:
            # Place LIMIT order
            order_id = alice.place_order(transaction_type=AliceBlue.TRANSACTION_TYPE_BUY,
                                         instrument=alice.get_instrument_for_fno(symbol=trade_data['symbol'], expiry_date=None,
                                                                                 is_fut=False, strike_price=float(trade_data['strike']),
                                                                                 option_type=trade_data['option_type']),
                                         quantity=1,
                                         order_type=AliceBlue.ORDER_TYPE_LIMIT,
                                         product_type=AliceBlue.PRODUCT_MIS,
                                         price=trade_data['entry'],
                                         trigger_price=None,
                                         stop_loss=trade_data['sl'],
                                         square_off=trade_data['target'],
                                         trailing_sl=None,
                                         is_amo=False)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Order placed, ID: {order_id}")

            # Start timer to switch to Market Order
            threading.Timer(ORDER_WAIT_TIME_MINUTES * 60, switch_to_market_order, args=(order_id, trade_data)).start()

        # Mark this symbol as traded today
        with lock:
            placed_orders[key] = today

    except Exception as e:
        print(f"Error placing trade: {e}")


def switch_to_market_order(order_id, trade_data):
    try:
        if MODE == "paper":
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PAPER MODE] Would have switched {trade_data['symbol']} to MARKET order.")
            return

        order_history = alice.get_order_history(order_id)
        if order_history and order_history['status'] != 'complete':
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Limit order not filled, cancelling and switching to MARKET.")

            # Cancel old order
            alice.cancel_order(order_id)

            # Place Market Order
            alice.place_order(transaction_type=AliceBlue.TRANSACTION_TYPE_BUY,
                              instrument=alice.get_instrument_for_fno(symbol=trade_data['symbol'], expiry_date=None,
                                                                      is_fut=False, strike_price=float(trade_data['strike']),
                                                                      option_type=trade_data['option_type']),
                              quantity=1,
                              order_type=AliceBlue.ORDER_TYPE_MARKET,
                              product_type=AliceBlue.PRODUCT_MIS,
                              price=None,
                              trigger_price=None,
                              stop_loss=trade_data['sl'],
                              square_off=trade_data['target'],
                              trailing_sl=None,
                              is_amo=False)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] MARKET order placed successfully for {trade_data['symbol']}!")

    except Exception as e:
        print(f"Error in switching to market order: {e}")


def connect_alice_blue():
    global alice
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to Alice Blue...")
    session_id = AliceBlue.login_and_get_access_token(username=USER_ID, password=API_KEY, twoFA="1234")
    alice = AliceBlue(username=USER_ID, password=API_KEY, access_token=session_id)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connected to Alice Blue successfully.")

def main():
    try:
        # Connect to AliceBlue only if Live Mode
        if MODE != "paper":
            connect_alice_blue()

        print(f"=============================================")
        print(f"✅ AutoTradeBot Started - Mode: {MODE.upper()}")
        print(f"✅ Monitoring Groups: {GROUPS_TO_MONITOR}")
        print(f"✅ Orders will switch to MARKET after {ORDER_WAIT_TIME_MINUTES} minutes if unfilled")
        print(f"=============================================")

        # Start Telegram
        client.start()
        client.run_until_disconnected()

    except KeyboardInterrupt:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Emergency STOP detected! Closing bot safely.")
    except Exception as e:
        print(f"Fatal Error: {e}")

if __name__ == "__main__":
    main()


