
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

# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

# Load recent stocks (IPO list)
nse_df = pd.read_csv("NSE.csv")
recent_stocks = nse_df.copy()
recent_stocks = recent_stocks[recent_stocks["Instrument Name"].str.contains("LIMITED|LTD", na=False)]
symbols = recent_stocks["Symbol"].unique().tolist()

def is_round_bottom_breakout(df, ipo_high):
    if len(df) < 180:
        return False

    lowest = df['low'].min()
    last_close = df.iloc[-1]['close']

    near_breakout = last_close >= 0.8 * ipo_high
    gradual_recovery = last_close > df['close'].rolling(10).mean().iloc[-1]

    return near_breakout and gradual_recovery

def scan_ipos():
    results = []
    for sym in symbols:
        try:
            instrument = alice.get_instrument_by_symbol("NSE", sym)
            if not instrument: continue

            hist = alice.get_historical_data(instrument, 
                                             datetime.now() - timedelta(days=180),
                                             datetime.now(), 
                                             interval=Interval.DAY)

            df = pd.DataFrame(hist)
            if df.empty: continue

            ipo_high = df.head(3)['high'].max()

            if is_round_bottom_breakout(df, ipo_high):
                results.append(sym)

                # Plot and save chart
                plt.figure(figsize=(10, 4))
                plt.plot(df['date'], df['close'], label=sym)
                plt.axhline(y=ipo_high, color='red', linestyle='--', label='IPO High')
                plt.title(f"{sym} - Round Bottom Breakout Candidate")
                plt.legend()
                plt.grid()
                plt.savefig(f"{sym}_breakout_chart.png")
                plt.close()

            time.sleep(1)

        except Exception as e:
            print(f"Error processing {sym}: {e}")
            continue

    pd.DataFrame(results, columns=["Symbol"]).to_excel("IPO_Breakout_Candidates.xlsx", index=False)
    print("✅ IPO breakout candidates saved.")

# Run
scan_ipos()
