"""
adjustment_straddle_monitor.py
Auto‑straddle value‑adjustment bot (v2)

• Index decided by day:
    – Fri / Mon / Tue  → SENSEX  (weekly expiry Tuesday)
    – Wed / Thu        → NIFTY   (weekly expiry Thursday)

• After 10 :00 IST, picks ATM CE+PE.
• Monitors every second; if one premium ≤ 70 % of the other, treat it as a
  30 – 50 % discount → buy the undervalued leg (1 lot) with
      target  = +50 %, stop‑loss = –30 %.
"""

import os
import pandas as pd
import numpy as np
import threading
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from alice_blue import AliceBlue, TransactionType, OrderType, ProductType
import logging
import time as time_module  # renamed to avoid conflict with datetime
from pya3 import *
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

holidays = [
    datetime(2025, 5, 1),  # Maharashtra Day
    datetime(2025, 4, 14), # Ambedkar Jayanti
    datetime(2025, 4, 18),
    datetime(2025, 5, 1),
    datetime(2025, 8, 15),
    datetime(2025, 8, 27),
    datetime(2025, 10, 2),
    datetime(2025, 10, 21),
    datetime(2025, 10, 22),
    datetime(2025, 11, 5),
    datetime(2025, 12, 25),


]
# ───────────── USER CONFIG ──────────────
USER_ID   = "1660575"
API_KEY   = "WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB"

POLL_INTERVAL_SEC  = 1          # tick frequency
DISCOUNT_RATIO     = 0.70       # 30 – 50 % discount threshold (price ≤ 70 % of other)
TARGET_MULTIPLIER  = 1.50       # +50 % target
SL_MULTIPLIER      = 0.70       # –30 % stop‑loss
START_TIME_IST     = (10, 0)    # start logic after 10 :00 IST
MARKET_CLOSE_IST   = (15, 25)
# ─────────────────────────────────────────


def ist_now():
    """Return current time in Asia/Kolkata."""
    import pytz
    return datetime.now(pytz.timezone("Asia/Kolkata"))


def get_weekly_expiry(today, weekday_target):
    """
    Return this week’s expiry date (YYYY‑MM‑DD) falling on weekday_target
    (0=Mon … 6=Sun). If today is past it, pick next week.
    """
    days_ahead = weekday_target - today.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")


def choose_segment():
    wd = ist_now().weekday()
    if wd in (4, 0, 1):                 # Fri / Mon / Tue
        return "SENSEX", 1              # Tuesday expiry (weekday=1)
    elif wd in (2, 3):                  # Wed / Thu
        return "NIFTY", 3               # Thursday expiry (weekday=3)
    return None, None                   # Weekend


def wait_until(hour, minute):
    while True:
        now = ist_now()
        if (now.hour, now.minute) >= (hour, minute):
            break
        time.sleep(30)


def main():
    segment, expiry_wd = choose_segment()
    if not segment:
        print("Weekend – nothing to do.")
        return

    print(f"▶ Selected segment: {segment} (weekly expiry weekday={expiry_wd})")

    alice = Aliceblue(user_id=USER_ID, api_key=API_KEY)
    if not alice.get_session_id():
        print("❌ Login to Alice Blue failed.")
        return

    # 1️⃣ Wait until after 10 :00 IST
    wait_until(*START_TIME_IST)

    today  = ist_now().date()
    expiry = get_weekly_expiry(today, expiry_wd)
    print(f"Weekly expiry picked: {expiry}")

    # 2️⃣ Determine ATM strike
    index_symbol = segment
    spot_inst  = alice.get_instrument_by_symbol("NSE", index_symbol)
    spot_price = float(alice.get_scrip_info(spot_inst)["LTP"])
    strike     = round(spot_price / 100) * 100
    print(f"Spot = {spot_price:.2f}  →  ATM strike = {strike}")

    # 3️⃣ Fetch CE / PE instruments
    ce_inst = alice.get_instrument_for_fno(
        "NFO", index_symbol, expiry, is_fut=False, strike=strike, is_CE=True
    )
    pe_inst = alice.get_instrument_for_fno(
        "NFO", index_symbol, expiry, is_fut=False, strike=strike, is_CE=False
    )

    order_sent = False
    print("Polling premiums every second…")
    while True:
        now = ist_now()
        if (now.hour, now.minute) >= MARKET_CLOSE_IST:
            print("Market close reached – exiting.")
            break

        ce_price = float(alice.get_scrip_info(ce_inst)["LTP"])
        pe_price = float(alice.get_scrip_info(pe_inst)["LTP"])

        undervalued = None
        if ce_price <= DISCOUNT_RATIO * pe_price:
            undervalued = ("CE", ce_inst, ce_price, pe_price)
        elif pe_price <= DISCOUNT_RATIO * ce_price:
            undervalued = ("PE", pe_inst, pe_price, ce_price)

        if undervalued and not order_sent:
            leg, inst, entry, other = undervalued
            target   = round(entry * TARGET_MULTIPLIER, 1)
            stoploss = round(entry * SL_MULTIPLIER,   1)

            print(
                f"🔔 Discount detected – {leg} undervalued:"
                f" entry ₹{entry} vs other ₹{other}."
            )
            print(f"→ Placing BUY (1 lot)  target ₹{target}  SL ₹{stoploss}")

            res = alice.place_order(
                transaction_type=TransactionType.Buy,
                instrument=inst,
                quantity=1,
                order_type=OrderType.Market,
                product_type=ProductType.Intraday,
                price=0.0,
                trigger_price=None,
                stop_loss=stoploss,
                square_off=target,
                trailing_sl=None,
                is_amo=False,
            )
            print("✔ Order response:", res)
            order_sent = True
            break  # stop monitoring after first trade; remove to continue

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
