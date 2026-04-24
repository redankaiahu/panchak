#pip install optiondata-ab
from pya3 import Aliceblue, LiveFeedType
from Optionchain import *
import os
import pandas as pd
import numpy as np
import threading
#from datetime import datetime, timedelta, time
import datetime
#from pya3 import Aliceblue
from datetime import datetime, date
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
from websocket import WebSocketApp

# ======================================================
# USER CONFIG
# ======================================================
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
Optionchainlogin(
    user_id='1660575',
    session_id=str(session_id)  # IMPORTANT: must be string
)

if not session_id:
    print("Failed to establish session.")
    exit()
expiry = date(2026, 1, 27)
#alice.get_contract_master("NFO")
#alice.get_contract_master("NSE")
#alice.get_contract_master("INDICES")
#print(alice.get_scrip_info(alice.get_instrument_by_symbol("NSE", "INFY-EQ")))
print(alice.get_instrument_by_symbol("NFO", "RELIANCE"))
print(alice.get_instrument_by_symbol("NSE", "RELIANCE"))
print(alice.get_scrip_info(alice.get_instrument_by_symbol("NSE", "RELIANCE")))
print("/n")
#print(alice.get_scrip_info(alice.get_instrument_by_symbol("NFO", "RELIANCE27JAN26F")))
print(alice.get_scrip_info(alice.get_instrument_by_symbol("NFO", "RELIANCE27JAN26P1500")))
#print(alice.get_scrip_info(alice.get_instrument_by_symbol("NFO", "BANKNIFTY27JAN26F")))

from datetime import date

spot_info = alice.get_scrip_info(
    alice.get_instrument_by_symbol("NSE", "RELIANCE")
)

spot_price = float(spot_info["LTP"])
print("RELIANCE Spot:", spot_price)

expiry = "2026-01-27"   # same as RELIANCE27JAN26

chain = PremiumDashboard(
    tab_name="bullish",        # or bearish / neutral
    name="RELIANCE",
    expiry=expiry,
    spot_price=str(spot_price),
    exchange="NSE"
)

print(chain)
df = pd.DataFrame(chain)

# Keep only what we need
df = df[[
    "strike_price",
    "call_ltp",
    "call_oi",
    "put_ltp",
    "put_oi"
]]

print(df.head())


oi_data = OpenInterest(
    date=datetime.now().strftime("%Y-%m-%d"),
    cm="true",
    cm_1="true",
    cm_2="true",
    short=1,
    long=5
)

print(oi_data)



#print(alice.get_instrument_for_fno(exch="NFO",symbol='BANKNIFTY', expiry_date="2026-01-27", is_fut=True,strike=None, is_CE=False))
#print(alice.get_instrument_for_fno(exch="NFO",symbol='BANKNIFTY', expiry_date="2026-01-27", is_fut=False,strike=60000, is_CE=False))
#print(alice.get_instrument_for_fno(exch="NFO",symbol='BANKNIFTY', expiry_date="2026-01-27", is_fut=False,strike=60000, is_CE=True))

#print(alice.get_instrument_by_symbol("NFO",'NIFTY 50'))
#print(alice.get_instrument_by_symbol("NFO",'NIFTY BANK'))

#print(alice.get_instrument_by_token('INDICES',26000)) # Nifty Indices
#print(alice.get_instrument_by_token('INDICES',26009)) # Bank Nifty

#bnf_fut=alice.get_instrument_for_fno(exch="NFO", symbol = "BANKNIFTY", expiry_date="2026-01-27", is_fut=True, strike=None, is_CE = False)
#print("bnf_fut:",bnf_fut)
#nifty_fut=alice.get_instrument_for_fno(exch="NFO", symbol = 'NIFTY', expiry_date="2026-01-27", is_fut=True, strike=None, is_CE = False)
#print("nifty_fut:",nifty_fut)
#nifty_spot = alice.get_instrument_by_symbol('NSE', 'NIFTY 50')
#print("nifty_spot:",nifty_spot)
#bnf_spot = alice.get_instrument_by_symbol('NSE', 'NIFTY BANK')
#print("bnf_spot:",bnf_spot)


#print("Banknifty future/n")
#print(alice.get_instrument_for_fno(symbol = 'BANKNIFTY', expiry_date=date(2026, 1, 27), is_fut=True, strike=None, is_CE = False))
#print("Banknifty CE /n")
#print(alice.get_instrument_for_fno(exch="NFO", symbol = 'BANKNIFTY', expiry_date="2026-01-27", is_fut=False, strike=60000, is_CE = True))
#print("Banknifty PE/n")
#print(alice.get_instrument_for_fno(exch="NFO", symbol = 'BANKNIFTY', expiry_date="2026-01-27", is_fut=False, strike=60000, is_CE = False))


STOCKS = ["RELIANCE", "INFY", "AXISBANK"]

STRIKE_GAP = 10
STRIKE_RANGE = [-1, 0, 1]

SNAPSHOT_INTERVAL = 300  # 5 minutes

BASE_DIR = "nfo_engine_output"
RAW_DIR = f"{BASE_DIR}/raw"
LOG_DIR = f"{BASE_DIR}/logs"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ======================================================
# LOG
# ======================================================
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(f"{LOG_DIR}/engine.log", "a") as f:
        f.write(line + "\n")

# ======================================================
# LOGIN
# ======================================================
#alice = Aliceblue(
 #   username=USER_ID,
  #  api_key=API_KEY,
   # session_id=SESSION_ID,
    #master_contracts_to_download=["NFO", "NSE"]
#)

log("Alice login successful")

# ======================================================
# HELPERS
# ======================================================
def round_strike(price):
    return int(round(price / STRIKE_GAP) * STRIKE_GAP)

def get_spot_ltp(symbol):
    inst = alice.get_instrument_by_symbol("NSE", f"{symbol}-EQ")
    info = alice.get_scrip_info(inst)
    return float(info.get("Ltp") or info.get("LTP") or 0)

def get_nearest_expiry(symbol):
    df = pd.read_csv("NFO.csv")
    df = df[df["Symbol"] == symbol]
    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], dayfirst=True, errors="coerce")
    df = df[df["Expiry Date"] >= pd.Timestamp.today()]
    return df.sort_values("Expiry Date").iloc[0]["Expiry Date"].date()

# ======================================================
# GLOBAL STATE
# ======================================================
tick_data = {}

# ======================================================
# WEBSOCKET HANDLERS
# ======================================================
def on_tick(message):
    token = message.get("token")
    if not token:
        return

    tick_data[token] = {
        "ltp": float(message.get("ltp", 0)),
        "oi": int(message.get("oi", 0)),
        "time": time.time()
    }

def on_open():
    log("WebSocket connected")

def on_close():
    log("WebSocket closed")

def on_error(msg):
    log(f"WebSocket error: {msg}")

alice.start_websocket(
    subscribe_callback=on_tick,
    socket_open_callback=on_open,
    socket_close_callback=on_close,
    socket_error_callback=on_error
)

time.sleep(2)

# ======================================================
# SUBSCRIBE OPTIONS
# ======================================================
option_map = {}

for sym in STOCKS:
    log(f"Preparing options for {sym}")

    expiry = get_nearest_expiry(sym)
    spot = get_spot_ltp(sym)
    atm = round_strike(spot)

    option_map[sym] = {}

    for off in STRIKE_RANGE:
        strike = atm + off * STRIKE_GAP

        ce = alice.get_instrument_for_fno(
            symbol=sym,
            expiry_date=expiry,
            is_fut=False,
            strike=strike,
            is_CE=True
        )

        pe = alice.get_instrument_for_fno(
            symbol=sym,
            expiry_date=expiry,
            is_fut=False,
            strike=strike,
            is_CE=False
        )

        option_map[sym][f"{strike}CE"] = ce
        option_map[sym][f"{strike}PE"] = pe

        alice.subscribe(ce, LiveFeedType.DEPTH_DATA)
        alice.subscribe(pe, LiveFeedType.DEPTH_DATA)

log("All option subscriptions done")

# ======================================================
# SNAPSHOT WORKER
# ======================================================
def snapshot_worker():
    while True:
        now = datetime.now()
        if now.minute % 5 == 0 and now.second < 2:
            rows = []

            for sym, opts in option_map.items():
                spot = get_spot_ltp(sym)

                for name, inst in opts.items():
                    d = tick_data.get(inst.token)
                    if not d:
                        continue

                    rows.append({
                        "time": now.strftime("%H:%M"),
                        "symbol": sym,
                        "spot": spot,
                        "option": name,
                        "ltp": d["ltp"],
                        "oi": d["oi"]
                    })

            if rows:
                df = pd.DataFrame(rows)
                fname = f"snapshot_{now.strftime('%H_%M')}.xlsx"
                df.to_excel(f"{RAW_DIR}/{fname}", index=False)
                log(f"Snapshot saved → {fname}")

        time.sleep(1)

# ======================================================
# START SNAPSHOT THREAD
# ======================================================
threading.Thread(target=snapshot_worker, daemon=True).start()

log("ENGINE RUNNING")

while True:
    time.sleep(1)
