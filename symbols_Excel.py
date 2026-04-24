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

symbols = ['RELIANCE', 'TCS', 'INFY']  # Add more symbols as required

# ✅ Specific dates for which OHLC is needed
specific_dates = ['23-04-2025', '24-04-2025', '25-04-2025']
specific_dates = [datetime.strptime(date, "%d-%m-%Y").date() for date in specific_dates]


def fetch_ohlc(symbol, from_date, to_date):
    try:
        instrument = alice.get_instrument_by_symbol('NSE', symbol)
        data = alice.get_historical(
            instrument=instrument,
            from_datetime=datetime.combine(from_date, datetime.min.time()),
            to_datetime=datetime.combine(to_date, datetime.max.time()),
            interval='D',
            indices=False
        )
        df = pd.DataFrame(data)
        df['symbol'] = symbol
        return df
    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

# ✅ Fetch OHLC for all dates and symbols
final_df = pd.DataFrame()
for symbol in symbols:
    df_symbol = fetch_ohlc(symbol, min(specific_dates), max(specific_dates))
    if not df_symbol.empty:
        df_symbol['date'] = pd.to_datetime(df_symbol['datetime']).dt.date
        df_filtered = df_symbol[df_symbol['date'].isin(specific_dates)]
        final_df = pd.concat([final_df, df_filtered], ignore_index=True)

# ✅ Save to Excel
output_filename = f"OHLC_Data_{datetime.now().strftime('%d%m%Y_%H%M%S')}.xlsx"
output_path = os.path.join(os.getcwd(), output_filename)
final_df.to_excel(output_path, index=False)

print(f"✅ OHLC data saved to: {output_path}")
