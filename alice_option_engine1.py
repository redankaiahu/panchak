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

# ======================================================
# USER CONFIG
# ======================================================
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

#alice.get_contract_master("NFO")
#alice.get_contract_master("NSE")
print(alice.get_scrip_info(alice.get_instrument_by_symbol("NSE", "INFY-EQ")))

STOCKS = [
    "RELIANCE",
    "INFY",
    "HCLTECH",
    "AXISBANK",
    "ICICIBANK"
]

STRIKE_GAP = 10          # 10 for stocks, 50 for BANKNIFTY
STRIKE_RANGE = [-1, 0, 1]  # ATM ±1

SNAPSHOT_INTERVAL = 300   # 5 minutes
ANALYSIS_MINUTES = [16, 31, 46, 61]

BASE_DIR = "nfo_output"
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




# ======================================================
# HELPERS
# ======================================================
def round_strike(price):
    return int(round(price / STRIKE_GAP) * STRIKE_GAP)

def get_spot_ltp(symbol):
    inst = alice.get_instrument_by_symbol("NSE", f"{symbol}-EQ")
    info = alice.get_scrip_info(inst)

    # Alice sometimes returns Ltp or LTP
    ltp = info.get("Ltp") or info.get("LTP")
    return float(ltp)


def get_nearest_nfo_expiry(symbol):
    df = pd.read_csv("NFO.csv")

    df = df[
        (df["Symbol"] == symbol) &
        (df["Option Type"].isin(["CE", "PE"]))
    ]

    df["Expiry Date"] = pd.to_datetime(
        df["Expiry Date"], dayfirst=True, errors="coerce"
    )
    today = pd.Timestamp.today().normalize()

    df = df[df["Expiry Date"] >= today].sort_values("Expiry Date")

    if df.empty:
        raise Exception(f"No NFO options found for {symbol}")

    return df.iloc[0]["Expiry Date"].date()

# ======================================================
# TOKEN BUILDER (NFO ONLY)
# ======================================================
token_map = {}
subscribe_tokens = []

for sym in STOCKS:
    log(f"Preparing NFO options for {sym}")

    expiry = get_nearest_nfo_expiry(sym)
    spot = get_spot_ltp(sym)
    atm = round_strike(spot)

    token_map[sym] = {
        "spot": spot,
        "expiry": expiry,
        "options": {}
    }

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

        token_map[sym]["options"][strike] = {
            "CE": f"NFO|{ce.token}",
            "PE": f"NFO|{pe.token}"
        }

        subscribe_tokens.append(f"NFO|{ce.token}")
        subscribe_tokens.append(f"NFO|{pe.token}")

log(f"Subscribed tokens count: {len(subscribe_tokens)}")

# ======================================================
# MARKET STATE
# ======================================================
market_state = {}

# ======================================================
# WEBSOCKET HANDLERS
# ======================================================
def on_open(ws):
    log("WebSocket connected")
    ws.send(json.dumps({
        "k": "#".join(subscribe_tokens),
        "t": "d"
    }))
    log("Subscription sent")

def on_message(ws, msg):
    data = json.loads(msg)
    if data.get("t") in ("df", "dk"):
        market_state[data["tk"]] = {
            "ltp": float(data.get("lp", 0)),
            "oi": int(data.get("oi", 0)),
            "time": time.time()
        }

def on_error(ws, err):
    log(f"WS ERROR: {err}")

def on_close(ws):
    log("WebSocket closed, reconnecting in 5 sec")
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
                spot = get_spot_ltp(sym)

                for strike, opt in token_map[sym]["options"].items():
                    for side in ["CE", "PE"]:
                        tk = opt[side].split("|")[1]
                        d = market_state.get(tk)
                        if not d:
                            continue

                        rows.append({
                            "time": now.strftime("%H:%M"),
                            "symbol": sym,
                            "spot": spot,
                            "strike": strike,
                            "type": side,
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

            df = pd.concat(
                pd.read_excel(f"{RAW_DIR}/{f}") for f in files
            )

            signals = []

            for sym in df["symbol"].unique():
                sdf = df[df["symbol"] == sym]

                ce = sdf[sdf["type"] == "CE"]
                pe = sdf[sdf["type"] == "PE"]

                if len(ce) < 2 or len(pe) < 2:
                    continue

                ce_move = (ce.iloc[-1].ltp - ce.iloc[0].ltp) / max(ce.iloc[0].ltp, 1) * 100
                pe_move = (pe.iloc[-1].ltp - pe.iloc[0].ltp) / max(pe.iloc[0].ltp, 1) * 100

                ce_oi = (ce.iloc[-1].oi - ce.iloc[0].oi) / max(ce.iloc[0].oi, 1) * 100
                pe_oi = (pe.iloc[-1].oi - pe.iloc[0].oi) / max(pe.iloc[0].oi, 1) * 100

                if ce_move > 15 and ce_oi <= 2 and pe_oi > 3:
                    sig = "STRONG_CALL"
                elif pe_move > 15 and pe_oi <= 2 and ce_oi > 3:
                    sig = "STRONG_PUT"
                else:
                    sig = "IGNORE"

                signals.append({
                    "symbol": sym,
                    "signal": sig,
                    "ce_%": round(ce_move, 2),
                    "pe_%": round(pe_move, 2)
                })

            out = pd.DataFrame(signals)
            fname = f"signal_{now.strftime('%H_%M')}.xlsx"
            out.to_excel(f"{SIG_DIR}/{fname}", index=False)
            log(f"Signal file saved → {fname}")

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
