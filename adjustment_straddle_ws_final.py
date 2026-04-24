
"""
adjustment_straddle_ws_final.py
Real-time straddle discount bot using Alice Blue WebSocket (via token feed)

• Uses token-based LTP from feed_data()
• Monitors NIFTY/SENSEX index + CE/PE every second
• On undervaluation (30–50%), executes 1-lot market buy with SL & Target
"""

import time, json
from datetime import datetime, timedelta
import pytz
from pya3 import Aliceblue, TransactionType, OrderType, ProductType

USER_ID = "1660575"
API_KEY = "WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB"

DISCOUNT = 0.70
TARGET_MULT = 1.50
SL_MULT = 0.70
START_IST = (10, 0)
CLOSE_IST = (15, 25)

ltp_by_token = {}
order_placed = False
ce, pe = None, None


def ist_now():
    return datetime.now(pytz.timezone("Asia/Kolkata"))


def wait_until(hour, minute):
    while (now := ist_now()) < now.replace(hour=hour, minute=minute, second=0):
        print(f"⏳ Waiting for {hour}:{minute} IST...")
        time.sleep(30)


def get_expiry(today, weekday_target):
    shift = weekday_target - today.weekday()
    if shift < 0:
        shift += 7
    return (today + timedelta(days=shift)).strftime("%Y-%m-%d")


def feed_data(msg):
    global ltp_by_token, order_placed, ce, pe

    data = json.loads(msg)
    if data.get("t") != "tk":
        return

    token = int(data["tk"])
    ltp = float(data.get("lp", 0))
    ltp_by_token[token] = ltp

    if order_placed or not ce or not pe:
        return

    now = ist_now()
    if (now.hour, now.minute) < START_IST or (now.hour, now.minute) >= CLOSE_IST:
        return

    ce_ltp = ltp_by_token.get(ce.token, 0)
    pe_ltp = ltp_by_token.get(pe.token, 0)

    if ce_ltp == 0 or pe_ltp == 0:
        return

    undervalued = None
    if ce_ltp <= DISCOUNT * pe_ltp:
        undervalued = ("CE", ce, ce_ltp)
    elif pe_ltp <= DISCOUNT * ce_ltp:
        undervalued = ("PE", pe, pe_ltp)

    if undervalued:
        leg, inst, entry = undervalued
        target = round(entry * TARGET_MULT, 1)
        sl = round(entry * SL_MULT, 1)

        print(f"🚀 BUY {leg} undervalued at ₹{entry} → Target ₹{target}, SL ₹{sl}")

        res = alice.place_order(
            transaction_type=TransactionType.Buy,
            instrument=inst,
            quantity=1,
            order_type=OrderType.Market,
            product_type=ProductType.Intraday,
            price=0.0,
            trigger_price=None,
            stop_loss=sl,
            square_off=target,
            trailing_sl=None,
            is_amo=False,
        )
        print("✅ Order placed:", res)
        order_placed = True


# --- Main script ---
alice = Aliceblue(user_id=USER_ID, api_key=API_KEY)
if not alice.get_session_id():
    print("❌ Login failed.")
    exit()

# Choose index
day = ist_now().weekday()
if day in [4, 0, 1]:
    index_sym, expiry_wd = "SENSEX", 1
elif day in [2, 3]:
    index_sym, expiry_wd = "NIFTY", 3
else:
    print("Market holiday/weekend.")
    exit()

wait_until(*START_IST)

expiry = get_expiry(ist_now().date(), expiry_wd)
spot_inst = alice.get_instrument_by_symbol("INDICES", index_sym)
spot_token = spot_inst.token

# Let LTP update first
print(f"📡 Waiting for spot price of {index_sym}...")
alice.start_websocket([spot_inst], feed_callback=lambda m: ltp_by_token.update({spot_token: float(json.loads(m).get("lp", 0))}))
time.sleep(3)

spot_price = ltp_by_token.get(spot_token, 0)
if spot_price == 0:
    print("❌ Failed to receive spot LTP.")
    exit()

atm = round(spot_price / 100) * 100
print(f"🔍 Spot={spot_price:.2f} → ATM strike={atm}")

# Prepare CE/PE
ce = alice.get_instrument_for_fno("NFO", index_sym, expiry, False, atm, True)
pe = alice.get_instrument_for_fno("NFO", index_sym, expiry, False, atm, False)

# Subscribe all 3
alice.start_websocket([spot_inst, ce, pe], feed_callback=feed_data)
