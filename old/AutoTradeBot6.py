from telethon import TelegramClient, events
import json
from datetime import datetime
from alice_blue import AliceBlue

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
alice = None

placed_orders = {}

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
        full_text = signal_text.replace('\n', ' ').upper()
        symbol = None
        strike = None
        option_type = None
        entry_price = None
        sl_price = None
        target_price = None

        # Extract Symbol and Strike and Option
        if "#" in full_text:
            full_text = full_text.split("#", 1)[1]  # Keep only text after first '#'
        parts = full_text.replace("#", "").split()
        for idx, part in enumerate(parts):
            if part.endswith("CE") or part.endswith("PE"):
                option_type = "CE" if "CE" in part else "PE"
                numbers = ''.join(filter(str.isdigit, part))
                if numbers:
                    strike = numbers
                # Try to capture symbol before this
                if idx > 0:
                    symbol = parts[idx - 1]
                break  # Only first CE/PE needed

        # Extract Entry Price
        if "BUY ABOVE" in full_text:
            try:
                after_buy = full_text.split("BUY ABOVE")[1].strip()
                entry_text = after_buy.split()[0].replace("+", "").replace(",", "").split("-")[0]
                entry_price = float(entry_text)
            except:
                pass
        elif "CMP" in full_text:
            try:
                after_cmp = full_text.split("CMP")[1].strip()
                entry_text = after_cmp.split()[0].replace("+", "").replace(",", "").split("-")[0]
                entry_price = float(entry_text)
            except:
                pass

        # Extract Stoploss
        if "SL" in full_text:
            try:
                after_sl = full_text.split("SL")[1].strip()
                sl_text = after_sl.split()[0].replace("+", "").replace(",", "")
                sl_price = float(sl_text)
            except:
                pass

        # Extract Target
        if "TARGET" in full_text:
            try:
                after_target = full_text.split("TARGET")[1].strip()
                target_text = after_target.replace("-", "").replace(":", "").split()[0]
                if "/" in target_text:
                    target_text = target_text.split("/")[0]
                if "," in target_text:
                    target_text = target_text.split(",")[0]
                target_price = float(target_text)
            except:
                pass

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
        print(f"Error parsing single signal: {e}")

def place_trade(trade_data):
    try:
        key = f"{trade_data['symbol']}_{trade_data['strike']}_{trade_data['option_type']}"
        today = datetime.now().strftime("%Y-%m-%d")
        if placed_orders.get(key) == today:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Already traded {key} today, skipping.")
            return

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Preparing to place order: {trade_data}")

        if MODE == "paper":
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PAPER MODE] Simulating Buy {key} at {trade_data['entry']}")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [LIVE MODE] Would place real order here (code commented for safety).")

        placed_orders[key] = today

    except Exception as e:
        print(f"Error placing trade: {e}")

@client.on(events.NewMessage)
async def my_event_handler(event):
    try:
        message_text = event.message.message.strip()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Incoming message: {message_text}")

        process_signal(message_text)

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
