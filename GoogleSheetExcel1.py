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
    'SHRIRAMFIN',
    'VEDL',
    'VBL',
    'GRANULES',
    'LICHSGFIN',
    'UPL',
    'ANGELONE',
    'INDHOTEL',
    'APLAPOLLO',
    'CAMS',
    'CUMMINSIND',
    'MAXHEALTH',
    'POLICYBZR',
    'HAVELLS',
    'GLENMARK',
    'ADANIPORTS',
    'SRF',
    'CDSL',
    'TITAN',
    'SBILIFE',
    'COLPAL',
    'HDFCLIFE',
    'VOLTAS',
    'NAUKRI',
    'TATACHEM',
    'KALYANKJIL'
]


# ✅ Dates for OHLC PANCHAK DATES
#specific_dates = ['13-07-2025', '14-07-2025', '15-07-2025', '16-07-2025', '17-07-2025']
specific_dates = ['21-01-2026', '22-01-2026', '23-01-2026','24-01-2026','25-01-2026']
specific_dates = [datetime.strptime(date, "%d-%m-%Y").date() for date in specific_dates]


def fetch_ohlc(symbol, from_date, to_date):
    try:
        if symbol == 'NIFTY':
            instrument = alice.get_instrument_by_token('NSE', 26000)
        elif symbol == 'BANKNIFTY':
            instrument = alice.get_instrument_by_token('NSE', 26009)
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
            indices=symbol in ['NIFTY', 'BANKNIFTY']
        )

        df = pd.DataFrame(data)
        df['symbol'] = symbol
        return df

    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

final_df = pd.DataFrame()
for symbol in symbols:
    df_symbol = fetch_ohlc(symbol, min(specific_dates), max(specific_dates))
    if not df_symbol.empty:
        df_symbol['date'] = pd.to_datetime(df_symbol['datetime']).dt.date
        df_filtered = df_symbol[df_symbol['date'].isin(specific_dates)]
        final_df = pd.concat([final_df, df_filtered], ignore_index=True)

if not final_df.empty:
    final_output = []

    for symbol in final_df['symbol'].unique():
        symbol_df = final_df[final_df['symbol'] == symbol].copy()

        rows = []
        for dt in specific_dates:
            row = symbol_df[symbol_df['date'] == dt]
            if not row.empty:
                rows.append(row[['symbol', 'date', 'close', 'open', 'high', 'low']].iloc[0])
            else:
                rows.append(pd.Series({
                    'symbol': symbol,
                    'date': dt,
                    'close': '',
                    'open': '',
                    'high': '',
                    'low': ''
                }))

        block_df = pd.DataFrame(rows)

        # ---- Calculations only on available data ----
        valid_data = block_df[block_df['high'] != '']
        if not valid_data.empty:
            top_high = valid_data['high'].astype(float).max()
            top_low = valid_data['low'].astype(float).min()
            diff = top_high - top_low
            bt = top_high + diff
            st = top_low - diff
        else:
            top_high = top_low = diff = bt = st = ''

        # ---- Add calculation columns ----
        block_df['TOP HIGH'] = ''
        block_df['TOP LOW'] = ''
        block_df['DIFF'] = ''
        block_df['BT'] = ''
        block_df['ST'] = ''

        block_df.loc[block_df.index[0], 'TOP HIGH'] = round(top_high, 2) if top_high != '' else ''
        block_df.loc[block_df.index[0], 'TOP LOW'] = round(top_low, 2) if top_low != '' else ''
        block_df.loc[block_df.index[0], 'DIFF'] = round(diff, 2) if diff != '' else ''
        block_df.loc[block_df.index[0], 'BT'] = round(bt, 2) if bt != '' else ''
        block_df.loc[block_df.index[0], 'ST'] = round(st, 2) if st != '' else ''

        final_output.append(block_df)

        # ---- Extra empty row after each symbol ----
        final_output.append(pd.DataFrame([[''] * len(block_df.columns)], columns=block_df.columns))

    formatted_df = pd.concat(final_output, ignore_index=True)

    output_filename = f"OHLC_Data_{datetime.now().strftime('%d%m%Y_%H%M%S')}.xlsx"
    output_path = os.path.join(os.getcwd(), output_filename)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        formatted_df.to_excel(writer, index=False, sheet_name='OHLC')

    print(f"✅ OHLC data saved to: {output_path}")
else:
    print("⚠️ No OHLC data found for specified symbols and dates.")
