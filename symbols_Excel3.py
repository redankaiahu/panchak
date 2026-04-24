
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

alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()
#symbols = ['RELIANCE', 'TCS', 'INFY']  # Modify as needed
symbols = [
    'NIFTY',
    'BANKNIFTY',
    'RELIANCE',
    'INFY',
    'HCLTECH',
    'TVSMOTOR',
    'BHARATFORG',
    'JUBLFOOD',
    'LAURUSLABS',
    'SUNPHARMA',
    'TATACONSUM',
    'COFORGE',
    'ASIANPAINT',
    'MUTHOOTFIN',
    'CHOLAFIN',
    'BSE',
    'GRASIM',
    'ACC',
    'ADANIENT',
    'BHARTIARTL',
    'BIOCON',
    'BRITANNIA',
    'DIVISLAB',
    'ESCORTS',
    'JSWSTEEL',
    'M&M',
    'PAGEIND',         # PAGE Industries
    'SHREECEM',        # Shree Cement
    'BOSCHLTD',        # BOSCH
    'DIXON',           # Dixon Technologies
    'MARUTI',          # Maruti Suzuki
    'ULTRACEMCO',      # UltraTech Cement
    'APOLLOHOSP',      # Apollo Hospitals
    'MCX',             # Multi Commodity Exchange
    'POLYCAB',
    'PERSISTENT',
    'TRENT',
    'EICHERMOT',       # Eicher Motors
    'HAL',             # Hindustan Aeronautics
    'TIINDIA',         # Tube Investments
    'SIEMENS',
    'GAIL',
    'NATIONALUM',
    'TATASTEEL',
    'MOTHERSON',
    'JSL',
    'VEDL',
    'VBL',
    'GRANULES',
    'LICHSGFIN',
    'UPL'
]


# ✅ Dates for OHLC
specific_dates = ['12-05-2025', '13-05-2025', '14-05-2025', '15-05-2025', '16-05-2025']
specific_dates = [datetime.strptime(date, "%d-%m-%Y").date() for date in specific_dates]


# ✅ Fetch function
def fetch_ohlc(symbol, from_date, to_date):
    try:
        if symbol in ['NIFTY', 'BANKNIFTY']:
            instrument = alice.get_instrument_by_symbol('NSE', symbol)#, exchange_segment='INDEX')
        else:
            instrument = alice.get_instrument_by_symbol('NSE', symbol)
        
        if not instrument or not hasattr(instrument, 'token'):
            print(f"⚠️ Invalid instrument for {symbol}")
            return pd.DataFrame()

        data = alice.get_historical(
            instrument=instrument,
            from_datetime=datetime.combine(from_date, datetime.min.time()),
            to_datetime=datetime.combine(to_date, datetime.max.time()),
            interval='D',
            #indices=symbol in ['NIFTY', 'BANKNIFTY']
            indices='NIFTY' in symbol or 'BANK' in symbol  # Index flag for NIFTY symbols
        )

        df = pd.DataFrame(data)
        df['symbol'] = symbol
        return df
    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return pd.DataFrame()


# ✅ Fetch and filter
final_df = pd.DataFrame()
for symbol in symbols:
    df_symbol = fetch_ohlc(symbol, min(specific_dates), max(specific_dates))
    if not df_symbol.empty:
        df_symbol['date'] = pd.to_datetime(df_symbol['datetime']).dt.date
        df_filtered = df_symbol[df_symbol['date'].isin(specific_dates)]
        final_df = pd.concat([final_df, df_filtered], ignore_index=True)

# ✅ Format output
if not final_df.empty:
    grouped = []
    for symbol in final_df['symbol'].unique():
        sub_df = final_df[final_df['symbol'] == symbol].copy()
        sub_df = sub_df[['symbol','date','close', 'open', 'high', 'low' ]]
        grouped.append(sub_df)
        grouped.append(pd.DataFrame([['', '', '', '', '', '']], columns=sub_df.columns))  # blank row
    formatted_df = pd.concat(grouped, ignore_index=True)

    # ✅ Save to Excel
    output_filename = f"OHLC_Data_{datetime.now().strftime('%d%m%Y_%H%M%S')}.xlsx"
    output_path = os.path.join(os.getcwd(), output_filename)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        formatted_df.to_excel(writer, index=False, sheet_name='OHLC')

    print(f"✅ OHLC data saved to: {output_path}")
else:
    print("⚠️ No OHLC data found for specified symbols and dates.")
