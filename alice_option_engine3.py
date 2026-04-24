# ===============================
# RELIANCE OPTION ENGINE (STABLE)
# ===============================

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
#Optionchainlogin(user_id='1660575', session_id=str(session_id) ) # IMPORTANT: must be string


if not session_id:
    print("Failed to establish session.")
    exit()
# -------------------------------
# USER CONFIG
# -------------------------------


SYMBOL = "RELIANCE"
STRIKE_GAP = 10
STRIKE_RANGE = [-2, -1, 0, 1, 2]   # ATM ±2
INTERVAL = 300  # 5 minutes
EXPIRY = "2026-01-27"  # working RELIANCE expiry

BASE_DIR = "reliance_engine"
SNAP_DIR = f"{BASE_DIR}/snapshots"
ANA_DIR = f"{BASE_DIR}/analysis"

os.makedirs(SNAP_DIR, exist_ok=True)
os.makedirs(ANA_DIR, exist_ok=True)

# -------------------------------
# LOGIN
# -------------------------------


print("[OK] Alice login successful")

# -------------------------------
# HELPERS
# -------------------------------
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(f"{BASE_DIR}/engine.log", "a") as f:
        f.write(line + "\n")

def get_spot_info():
    inst = alice.get_instrument_by_symbol("NSE", "RELIANCE")
    return alice.get_scrip_info(inst)

def round_strike(price):
    return int(round(price / STRIKE_GAP) * STRIKE_GAP)

def get_option_snapshot(strike, is_ce):
    inst = alice.get_instrument_for_fno(
        exch="NFO",
        symbol="RELIANCE",
        expiry_date=EXPIRY,
        is_fut=False,
        strike=strike,
        is_CE=is_ce
    )
    return alice.get_scrip_info(inst)

# -------------------------------
# SNAPSHOT LOOP
# -------------------------------
while True:
    try:
        spot = get_spot_info()
        spot_ltp = float(spot["LTP"])
        atm = round_strike(spot_ltp)

        rows = []

        for off in STRIKE_RANGE:
            strike = atm + off * STRIKE_GAP

            ce = get_option_snapshot(strike, True)
            pe = get_option_snapshot(strike, False)

            rows.append({
                "time": datetime.now().strftime("%H:%M"),
                "spot": spot_ltp,
                "strike": strike,
                "ce_ltp": float(ce.get("LTP", 0)),
                "ce_oi": int(ce.get("OI", 0)),
                "ce_vol": int(ce.get("TradeVolume", 0)),
                "pe_ltp": float(pe.get("LTP", 0)),
                "pe_oi": int(pe.get("OI", 0)),
                "pe_vol": int(pe.get("TradeVolume", 0))
            })

        df = pd.DataFrame(rows)
        fname = f"{SNAP_DIR}/snapshot_{datetime.now().strftime('%H_%M')}.xlsx"
        df.to_excel(fname, index=False)
        log(f"Snapshot saved → {fname}")

        # ---------------------------
        # STRENGTH ANALYSIS
        # ---------------------------
        files = sorted(os.listdir(SNAP_DIR))[-12:]  # last 60 mins (12×5min)
        hist = pd.concat(
            [pd.read_excel(f"{SNAP_DIR}/{f}") for f in files],
            ignore_index=True
        )

        latest = hist.groupby("strike").last()
        earliest = hist.groupby("strike").first()

        score = 0
        score += (latest["ce_oi"] - earliest["ce_oi"]).sum()
        score -= (latest["pe_oi"] - earliest["pe_oi"]).sum()

        trend = "STRONG BUY" if score > 0 else "STRONG SELL" if score < 0 else "NEUTRAL"

        out = pd.DataFrame([{
            "time": datetime.now().strftime("%H:%M"),
            "spot": spot_ltp,
            "score": score,
            "trend": trend
        }])

        out.to_excel(
            f"{ANA_DIR}/strength_{datetime.now().strftime('%H_%M')}.xlsx",
            index=False
        )

        log(f"Trend → {trend} | Score → {score}")

    except Exception as e:
        log(f"ERROR: {e}")

    time.sleep(INTERVAL)
