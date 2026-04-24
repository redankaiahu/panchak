
# ======================================================
# USER CONFIG
# ======================================================
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
from websocket import WebSocketApp

alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

#alice.get_contract_master("NFO")
#alice.get_contract_master("NSE")
#alice.get_contract_master("BFO")

#alice = AliceBlue(username='1660575',  access_token='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB', master_contracts_to_download=['NSE', 'BSE'])
#alice = AliceBlue(username = '1660575', session_id = session_id, master_contracts_to_download=['NSE', 'BSE'])
#multiple_underlying = ['BANKNIFTY','NIFTY','INFY','BHEL']
#all_scripts = alice.search_instruments('NFO', multiple_underlying)
#all_sensex_scrips = alice.search_instruments('BSE', 'sEnSeX')
#print(all_sensex_scrips)
#bn_fut = alice.get_instrument_for_fno(symbol = 'BANKNIFTY', expiry_date=datetime.date(2019, 6, 27), is_fut=True, strike=None, is_CE = False)
#bn_call = alice.get_instrument_for_fno(symbol = 'BANKNIFTY', expiry_date=datetime.date(2019, 6, 27), is_fut=False, strike=30000, is_CE = True)
#bn_put = alice.get_instrument_for_fno(symbol = 'BANKNIFTY', expiry_date=datetime.date(2019, 6, 27), is_fut=False, strike=30000, is_CE = False)

FUTURE_SYMBOLS = [
    
    "INFY",
    "HCLTECH",
    "TCS",
    "ICICIBANK"
]

STRIKE_GAP = 10        # 10 for stocks, 50 for BANKNIFTY
STRIKE_RANGE = [-2, -1, 0, 1, 2]

SNAPSHOT_INTERVAL = 300  # 5 min
ANALYSIS_MINUTES = [16, 31, 46, 61]

BASE_DIR = "alice_output"
RAW_DIR = f"{BASE_DIR}/raw"
SIG_DIR = f"{BASE_DIR}/signals"
LOG_DIR = f"{BASE_DIR}/logs"

for d in [RAW_DIR, SIG_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# ======================================================
# LOGGING
# ======================================================
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(f"{LOG_DIR}/engine.log", "a") as f:
        f.write(line + "\n")

# ======================================================
# ALICE LOGIN
# ======================================================
#alice = AliceBlue(
 #   user_id=USER_ID,
  #  api_key=API_KEY,
   # access_token=ACCESS_TOKEN
#)

log("Alice Blue session established")


def get_nearest_expiry(symbol, exch="NFO"):
    df = pd.read_csv(f"{exch}.csv")

    df = df[
        (df["Symbol"] == symbol) &
        (df["Option Type"] == "XX")
    ]

    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], errors="coerce")
    df = df.dropna(subset=["Expiry Date"])

    today = pd.Timestamp.today()
    future_exp = df[df["Expiry Date"] >= today].sort_values("Expiry Date")

    if future_exp.empty:
        raise Exception(f"No future expiry found for {symbol}")

    return future_exp.iloc[0]["Expiry Date"].strftime("%Y-%m-%d")


def round_strike(price):
    return int(round(price / STRIKE_GAP) * STRIKE_GAP)

# ======================================================
# TOKEN BUILDER
# ======================================================
def round_strike(price):
    return int(round(price / STRIKE_GAP) * STRIKE_GAP)

token_map = {}
subscribe_tokens = []

for sym in FUTURE_SYMBOLS:
    log(f"Building instruments for {sym}")

    expiry = get_nearest_expiry(sym)
    log(f"{sym} expiry → {expiry}")

    fut = alice.get_instrument_for_fno(
        exch="NFO",
        symbol=sym,
        expiry_date=expiry,
        is_fut=True,
        strike=None,
        is_CE=False
    )

    fut_ltp = alice.get_ltp(fut)["ltp"]
    atm = round_strike(fut_ltp)

    token_map[sym] = {
        "FUT": f"NFO|{fut.token}",
        "OPTIONS": {}
    }

    subscribe_tokens.append(f"NFO|{fut.token}")

    for off in STRIKE_RANGE:
        strike = atm + off * STRIKE_GAP

        ce = alice.get_instrument_for_fno(
            exch="NFO",
            symbol=sym,
            expiry_date=expiry,
            is_fut=False,
            strike=strike,
            is_CE=True
        )

        pe = alice.get_instrument_for_fno(
            exch="NFO",
            symbol=sym,
            expiry_date=expiry,
            is_fut=False,
            strike=strike,
            is_CE=False
        )

        token_map[sym]["OPTIONS"][strike] = {
            "CE": f"NFO|{ce.token}",
            "PE": f"NFO|{pe.token}"
        }

        subscribe_tokens.append(f"NFO|{ce.token}")
        subscribe_tokens.append(f"NFO|{pe.token}")



log(f"Total tokens subscribed: {len(subscribe_tokens)}")

# ======================================================
# MARKET STATE
# ======================================================
market_state = {}

# ======================================================
# WEBSOCKET HANDLERS
# ======================================================
def on_open(ws):
    log("WebSocket connected")

    payload = {
        "k": "#".join(subscribe_tokens),
        "t": "d"
    }
    ws.send(json.dumps(payload))
    log("Market data subscription sent")

def on_message(ws, msg):
    data = json.loads(msg)
    if data.get("t") in ("dk", "df"):
        tk = data["tk"]
        market_state[tk] = {
            "ltp": float(data.get("lp", 0)),
            "oi": int(data.get("oi", 0)),
            "time": time.time()
        }

def on_error(ws, err):
    log(f"WS ERROR: {err}")

def on_close(ws):
    log("WebSocket closed, reconnecting in 5s")
    time.sleep(5)
    start_ws()

# ======================================================
# SNAPSHOT WORKER
# ======================================================
def snapshot_worker():
    while True:
        now = datetime.now()
        if now.minute % 5 == 0 and now.second < 2:
            rows = []
            for sym in token_map:
                fut_token = token_map[sym]["FUT"].split("|")[1]
                fut_data = market_state.get(fut_token)

                if not fut_data:
                    continue

                rows.append({
                    "time": now.strftime("%H:%M"),
                    "symbol": sym,
                    "type": "FUT",
                    "ltp": fut_data["ltp"],
                    "oi": fut_data["oi"]
                })

                for strike, opt in token_map[sym]["OPTIONS"].items():
                    for opt_type in ["CE", "PE"]:
                        token = opt[opt_type].split("|")[1]
                        d = market_state.get(token)
                        if not d:
                            continue

                        rows.append({
                            "time": now.strftime("%H:%M"),
                            "symbol": sym,
                            "type": f"{strike}{opt_type}",
                            "ltp": d["ltp"],
                            "oi": d["oi"]
                        })

            if rows:
                df = pd.DataFrame(rows)
                fname = f"raw_{now.strftime('%H_%M')}.xlsx"
                df.to_excel(f"{RAW_DIR}/{fname}", index=False)
                log(f"Snapshot saved → {fname}")

        time.sleep(1)

# ======================================================
# ANALYSIS WORKER
# ======================================================
def analyze_worker():
    while True:
        now = datetime.now()
        if now.minute in ANALYSIS_MINUTES and now.second < 2:
            log(f"Analysis triggered @ {now.strftime('%H:%M')}")

            files = sorted(os.listdir(RAW_DIR))[-12:]
            if len(files) < 3:
                time.sleep(5)
                continue

            df = pd.concat(pd.read_excel(f"{RAW_DIR}/{f}") for f in files)

            signals = []

            for sym in df["symbol"].unique():
                sdf = df[df["symbol"] == sym]

                fut = sdf[sdf["type"] == "FUT"]
                if len(fut) < 2:
                    continue

                fut_move = (fut.iloc[-1].ltp - fut.iloc[0].ltp) / fut.iloc[0].ltp * 100

                ce = sdf[sdf["type"].str.endswith("CE")]
                pe = sdf[sdf["type"].str.endswith("PE")]

                ce_move = (ce.iloc[-1].ltp - ce.iloc[0].ltp) / max(ce.iloc[0].ltp,1) * 100
                pe_move = (pe.iloc[-1].ltp - pe.iloc[0].ltp) / max(pe.iloc[0].ltp,1) * 100

                ce_oi = (ce.iloc[-1].oi - ce.iloc[0].oi) / max(ce.iloc[0].oi,1) * 100
                pe_oi = (pe.iloc[-1].oi - pe.iloc[0].oi) / max(pe.iloc[0].oi,1) * 100

                if fut_move > 0.4 and ce_move > 15 and ce_oi <= 2 and pe_move < 0:
                    sig = "STRONG_BULL"
                elif fut_move < -0.4 and pe_move > 15 and pe_oi <= 2 and ce_move < 0:
                    sig = "STRONG_BEAR"
                else:
                    sig = "IGNORE"

                signals.append({
                    "symbol": sym,
                    "signal": sig,
                    "fut_%": round(fut_move,2),
                    "ce_%": round(ce_move,2),
                    "pe_%": round(pe_move,2)
                })

            out = pd.DataFrame(signals)
            fname = f"signal_{now.strftime('%H_%M')}.xlsx"
            out.to_excel(f"{SIG_DIR}/{fname}", index=False)
            log(f"Signal file generated → {fname}")

        time.sleep(1)




# ======================================================
# START ENGINE
# ======================================================
def start_ws():
    ws = WebSocketApp(
        "wss://v2api.aliceblueonline.com/websocket",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        header={
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "uid": USER_ID
        }
    )
    ws.run_forever()

log("ENGINE STARTED")

threading.Thread(target=start_ws, daemon=True).start()
threading.Thread(target=snapshot_worker, daemon=True).start()
threading.Thread(target=analyze_worker, daemon=True).start()

while True:
    time.sleep(1)
