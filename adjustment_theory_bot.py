
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


print(f"Today is for segment: {segment}")

# Get ATM strike based on spot
spot_symbol = segment
spot_instrument = alice.get_instrument_by_symbol("NSE", spot_symbol)
spot_info = alice.get_scrip_info(spot_instrument)
spot_price = float(spot_info.get("LTP", 0))
spot_strike = round(spot_price / 100) * 100

# Get CE and PE options
expiry_date = '2025-06-19'  # adjust to actual expiry
ce_inst = alice.get_instrument_for_fno('NFO', spot_symbol, expiry_date, False, spot_strike, True)
pe_inst = alice.get_instrument_for_fno('NFO', spot_symbol, expiry_date, False, spot_strike, False)

ce_price = float(alice.get_scrip_info(ce_inst).get("LTP", 0))
pe_price = float(alice.get_scrip_info(pe_inst).get("LTP", 0))

print(f"Spot: {spot_price}, {spot_strike}CE: {ce_price}, {spot_strike}PE: {pe_price}")

# Decide which one is undervalued
undervalued = None
if ce_price < 30:
    undervalued = ("CE", ce_inst, ce_price)
elif pe_price < 30:
    undervalued = ("PE", pe_inst, pe_price)

if not undervalued:
    print("No undervalued option found.")
    exit()

entry = undervalued[2]
target = round(entry * 1.5, 1)
stoploss = round(entry * 0.7, 1)

print(f"Placing BUY for 1 lot {undervalued[0]} at ₹{entry} → Target: ₹{target}, SL: ₹{stoploss}")

# Place order
order = alice.place_order(
    transaction_type=TransactionType.Buy,
    instrument=undervalued[1],
    quantity=1,
    order_type=OrderType.Market,
    product_type=ProductType.Intraday,
    price=0.0,
    trigger_price=None,
    stop_loss=stoploss,
    square_off=target,
    trailing_sl=None,
    is_amo=False
)

print("✅ Order placed:", order)
