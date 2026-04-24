import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from alice_blue import AliceBlue, TransactionType, OrderType, ProductType
import logging
import time as time_module  # renamed to avoid conflict with datetime
from pya3 import *
from tabulate import tabulate
import json
import threading
import math
import time
import requests
import pickle
from datetime import time as datetime_time

# Initialize AliceBlue
alice = Aliceblue(user_id='1660575', api_key='WaXdEOLI4VWzn5bUfDpmBMDSmBhN91v7nQjEx8WPBV4iQQHB2MIE6XWFuTNMxFcstK6z4QTVVgksD24KEUi0fZhuLAJYCVXAuqWluNdSke02Ssz9a37u9nyf2aiAk3eB')
session_id = alice.get_session_id()
if not session_id:
    print("Failed to establish session.")
    exit()

# Global variables
socket_opened = False
terminate_websocket = False
subscribe_list = []
tick_data = {}
orbh_values = {}  # Store ORBH values
orbl_values = {}  # Store ORBL values
orders_placed = set()  # To track orders and prevent duplicates
orders_hit = set()  # To track completed orders (target/SL hit)
square_off_done = False  # To track if square-off is already performed
order_tracking = {}
order_count = 0
MAX_ORDERS = 20  # Maximum orders allowed per day
terminate_square_off = False
sq_start_time = datetime_time(14, 59)
sq_end_time = datetime_time(15, 10)
current_time1 = datetime.now().time()
current_time2 = datetime.now().time()
low_values = []
high_values = []
ORB15_IN_UP_SIDE = []
ORB15_IN_RANGE = []
ORB15_IN_DOWN_SIDE = []
#orders_file_path = "orders_placed.pkl"


# Define holidays list
holidays = [
    datetime(2024, 11, 15), datetime(2024, 11, 20), datetime(2024, 12, 25),
    datetime(2025, 1, 1), datetime(2025, 2, 28), datetime(2025, 3, 17),
    datetime(2025, 4, 14), datetime(2025, 4, 18), datetime(2025, 4, 29)
]

# Adjust date to skip weekends and holidays
def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:  # Skip weekends
        date -= timedelta(days=1)
    while date in holidays:  # Skip holidays
        date -= timedelta(days=1)
    return date
    
today = datetime.today()
yesterday = adjust_for_weekends_and_holidays(datetime.now() - timedelta(days=1), holidays)
from_datetime = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
to_datetime = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

#from_datetime1 = today.replace(hour=9, minute=15, second=0, microsecond=0)
#to_datetime1 = today.replace(hour=15, minute=30, second=59, microsecond=0)

from_datetime1 = yesterday.replace(hour=9, minute=15, second=0, microsecond=0)
to_datetime1 = yesterday.replace(hour=15, minute=30, second=59, microsecond=0)

interval = "D"  # Daily interval

# Adjust date to skip weekends and holidays
def adjust_for_weekends_and_holidays(date, holidays=[]):
    while date.weekday() >= 5:  # Skip weekends
        date -= timedelta(days=1)
    while date in holidays:  # Skip holidays
        date -= timedelta(days=1)
    return date


# Create subscription list
def create_subscription_list(symbols):
    global subscribe_list
    subscribe_list = []
    for symbol in symbols:
        instrument = alice.get_instrument_by_symbol("NSE", symbol)
        if instrument:
            subscribe_list.append(instrument)


# Fetch historical data for a symbol
def fetch_and_filter_symbol(symbol):
    try:
        #print("in fetch_and_filter_symbol")
        instrument = alice.get_instrument_by_symbol("NSE", symbol)
        if not instrument or not hasattr(instrument, 'token'):
            return None

        # Fetch historical data for the last few days
        historical_data = alice.get_historical(
            instrument=instrument,
            from_datetime=from_datetime - timedelta(days=300),  # To calculate EMA
            to_datetime=to_datetime,
            interval=interval,
            indices=False
        )

        # Check if data is returned properly
        if isinstance(historical_data, pd.DataFrame) and not historical_data.empty:
            # Ensure all relevant columns are numeric
            numeric_columns = ["open", "close", "high", "low", "volume"]
            for col in numeric_columns:
                historical_data[col] = pd.to_numeric(historical_data[col], errors="coerce")
            
            # Drop rows with any missing or invalid values
            historical_data.dropna(subset=numeric_columns, inplace=True)

            # Ensure enough data is available for analysis
            if historical_data.shape[0] >= 3:
                # Calculate EMA values
                historical_data['EMA_200'] = historical_data['close'].ewm(span=200, adjust=False).mean()
                historical_data['EMA_50'] = historical_data['close'].ewm(span=50, adjust=False).mean()
                historical_data['EMA_20'] = historical_data['close'].ewm(span=20, adjust=False).mean()

                yesterday_data = historical_data.iloc[-1]
                day_before_data = historical_data.iloc[-2]
                three_days_ago = historical_data.iloc[-3]
                four_days_ago = historical_data.iloc[-4]
                day2_high = float(day_before_data['high'])  # daybefore_high
                day2_close = float(day_before_data['close'])  # daybefore_close
                day2_low = float(day_before_data['low']) 
                high_close_diff = ((day2_high - day2_close) / day2_high) * 100
                high_low_diff = ((day2_high - day2_low) / day2_high) * 100


                return {
                    "yesterday_open": float(yesterday_data['open']),
                    "yesterday_close": float(yesterday_data['close']),
                    "yesterday_high": float(yesterday_data['high']),
                    "yesterday_low": float(yesterday_data['low']),
                    "yesterday_vol": float(yesterday_data['volume']),

                    "daybefore_open": float(day_before_data['open']),
                    "daybefore_high": float(day_before_data['high']),
                    "daybefore_low": float(day_before_data['low']),
                    "daybefore_close": float(day_before_data['close']),
                    "daybefore_vol": float(day_before_data['volume']),

                    "three_days_ago_low": float(three_days_ago['low']),
                    "three_days_ago_high": float(three_days_ago['high']),
                    "three_days_ago_open": float(three_days_ago['open']),
                    "three_days_ago_close": float(three_days_ago['close']),
                    "three_days_ago_vol": float(three_days_ago['volume']),

                    "four_days_ago_low": float(four_days_ago['low']),
                    "four_days_ago_high": float(four_days_ago['high']),
                    "four_days_ago_open": float(four_days_ago['open']),
                    "four_days_ago_close": float(four_days_ago['close']),
                    "four_days_ago_vol": float(four_days_ago['volume']),

                    "ema_200": float(yesterday_data['EMA_200']),
                    "ema_50": float(yesterday_data['EMA_50']),
                    "ema_20": float(yesterday_data['EMA_20']),
                    "symbol_name": symbol,
                    "high_close_diff": high_close_diff,
                    "high_low_diff": high_low_diff
                }
                
            else:
                print(f"Not enough historical data for {symbol}.")
                return None
        else:
            print(f"No historical data found for {symbol}.")
            return None
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None



def evaluate_buy_condition_1(historical_data, open_low_list,buy_cond2_list,sell_cond2_list,high_orbh15_list, open_high_list,symbol,ORB15_IN_UP_SIDE,ORB15_IN_DOWN_SIDE,ORB15_IN_RANGE):
    #historical_data = fetch_and_filter_symbol(symbol)
    #print("historical_data:", historical_data,"\n")
    instrument = alice.get_instrument_by_symbol("NSE", symbol)
    if not instrument or not hasattr(instrument, 'token'):
        return None
    if historical_data:
                
        yesterday_open = round(float(historical_data.get("yesterday_open")),2)
        yesterday_close = round(float(historical_data.get("yesterday_close")),2)
        yesterday_high = round(float(historical_data.get("yesterday_high")),2)
        yesterday_low = round(float(historical_data.get("yesterday_low")),2)
        yesterday_vol = round(float(historical_data.get("yesterday_vol")),2)
        daybefore_open = round(float(historical_data.get("daybefore_open")),2)
        daybefore_close = round(float(historical_data.get("daybefore_close")),2)
        daybefore_high = round(float(historical_data.get("daybefore_high")),2)
        daybefore_low = round(float(historical_data.get("daybefore_low")),2)
        daybefore_vol = round(float(historical_data.get("daybefore_vol")),2)
        three_days_ago_open = round(float(historical_data.get("three_days_ago_open")),2)
        three_days_ago_close = round(float(historical_data.get("three_days_ago_close")),2)
        three_days_ago_high = round(float(historical_data.get("three_days_ago_high")),2)
        three_days_ago_low = round(float(historical_data.get("three_days_ago_low")),2)
        three_days_ago_vol = round(float(historical_data.get("three_days_ago_vol")),2)
        four_days_ago_open = round(float(historical_data.get("four_days_ago_open")),2)
        four_days_ago_close = round(float(historical_data.get("four_days_ago_close")),2)
        four_days_ago_high = round(float(historical_data.get("four_days_ago_high")),2)
        four_days_ago_low = round(float(historical_data.get("four_days_ago_low")),2)
        four_days_ago_vol = round(float(historical_data.get("four_days_ago_vol")),2)
        high_close_diff = round(float(historical_data.get("high_close_diff")),2)
        high_low_diff = round(float(historical_data.get("high_low_diff")),2)
        symbol_name = historical_data.get("symbol_name")
        #daybefore_vol = float(historical_data.get("daybefore_vol"))
        ema_200_value = float(historical_data.get("ema_200"))
        ema_50_value = float(historical_data.get("ema_50"))
        ema_20_value = float(historical_data.get("ema_20"))
        three_days_ago_low = historical_data.get("three_days_ago_low")
        three_days_ago_high = historical_data.get("three_days_ago_high")
        four_days_ago_low = historical_data.get("four_days_ago_low")
        four_days_ago_high = historical_data.get("four_days_ago_high")
        #instrument = historical_data.get("instrument")
        ema_200 = round(ema_200_value, 2)
        ema_50 = round(ema_50_value, 2)
        ema_20 = round(ema_20_value, 2)
        
        historical_data_one = alice.get_historical(instrument=instrument, from_datetime=from_datetime1, to_datetime=to_datetime1, interval="1", indices=False )

        low_values = historical_data_one.iloc[:60]['low'].tolist()
        high_values = historical_data_one.iloc[:60]['high'].tolist()

        low_values_5_one = low_values[:5]
        high_values_5_one = high_values[:5]
        ORBL5 = min(low_values_5_one)
        ORBH5 = max(high_values_5_one)

            # First 15 minutes ORB values (low and high)
        low_values_15_one = low_values[:15]
        high_values_15_one = high_values[:15]
        ORBL15_one = min(low_values_15_one)
        ORBH15_one = max(high_values_15_one)

            # Second 15 minutes ORB values (low and high)
        low_values_15_two = low_values[15:30]
        high_values_15_two = high_values[15:30]
        ORBL15_two = min(low_values_15_two)
        ORBH15_two = max(high_values_15_two)

            # Third 15 minutes ORB values (low and high)
        low_values_15_thr = low_values[30:45]
        high_values_15_thr = high_values[30:45]
        ORBL15_thr = min(low_values_15_thr)
        ORBH15_thr = max(high_values_15_thr)

            # Fourth 15 minutes ORB values (low and high)
        low_values_15_four = low_values[45:60]
        high_values_15_four = high_values[45:60]
        ORBL15_four = min(low_values_15_four)
        ORBH15_four = max(high_values_15_four)

        gapup = ((ORBH5 - yesterday_close)/yesterday_close) * 100
        gapdown =  ((yesterday_close - ORBL5)/yesterday_close) * 100
        ORBHP = round(gapup , 2)
        ORBLP = round(gapdown ,2)

        ORBH5Con = float(ORBH5) + 5
        ORBL5Con = float(ORBL5) - 5
        ORBH15Con = float(ORBH15_one) + 5
        ORBL15Con = float(ORBL15_one) - 5
         


        #print(f"{symbol_name}:Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}\n")
        #if yesterday_open == yesterday_low:
        #   print("OPEN == LOW") 
         #  print(f"{symbol_name}:Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}\n")
        #if yesterday_open == yesterday_high:
         #  print("OPEN == HIGH")
          # print(f"{symbol_name}:Yesterday's Open: {yesterday_open}, High: {yesterday_high}, Low: {yesterday_low}, Close: {yesterday_close}\n")

        if yesterday_open > daybefore_close and yesterday_open < daybefore_high and  daybefore_close > daybefore_open and yesterday_close > ORBH15_one:
            buy_cond2_list.append({
                "symbol": symbol_name,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "day2_open": daybefore_open,
                "day2_high": daybefore_high,
                "day2_low": daybefore_low,
                "day2_close": daybefore_close,
                "ORBH15_one": ORBH15_one,
                "ORBL15_one": ORBL15_one,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol,
                "high_close_diff": high_close_diff,
                "high_low_diff": high_low_diff
            })

        if yesterday_open > daybefore_close and yesterday_open < daybefore_high and  daybefore_close > daybefore_open and yesterday_close < ORBL15_one:
            sell_cond2_list.append({
                "symbol": symbol_name,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "day2_open": daybefore_open,
                "day2_high": daybefore_high,
                "day2_low": daybefore_low,
                "day2_close": daybefore_close,
                "ORBH15_one": ORBH15_one,
                "ORBL15_one": ORBL15_one,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol,
                "high_close_diff": high_close_diff,
                "high_low_diff": high_low_diff
            })

        if yesterday_high == ORBH15_one and yesterday_open > daybefore_close and yesterday_open < daybefore_high and  daybefore_close > daybefore_open and yesterday_close < ORBL15_one:
            high_orbh15_list.append({
                "symbol": symbol_name,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "day2_open": daybefore_open,
                "day2_high": daybefore_high,
                "day2_low": daybefore_low,
                "day2_close": daybefore_close,
                "ORBH15_one": ORBH15_one,
                "ORBL15_one": ORBL15_one,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol,
                "high_close_diff": high_close_diff,
                "high_low_diff": high_low_diff
                
            })

        if yesterday_open == yesterday_low:
            open_low_list.append({
                "symbol": symbol_name,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol
            })
        if yesterday_open == yesterday_high:
            open_high_list.append({
                "symbol": symbol_name,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol
            })

        #if  (yesterday_open > four_days_ago_low and  yesterday_open > four_days_ago_high and
         #   yesterday_close > four_days_ago_high and  daybefore_vol > 300000 and #yesterday_close > yesterday_open
          #  yesterday_close > ema_200 and  yesterday_close > ema_50 and yesterday_close > ema_20):
            #print(f"{symbol_name}:Y_Open: {yesterday_open}, Y_High: {yesterday_high}, Y_Low: {yesterday_low}, Y_Close: {yesterday_close},4_d_high: {four_days_ago_high}, 4_d_low: {four_days_ago_low},ema_200:{ema_200},ema_50:{ema_50}\n")
            #print(f"{symbol_name}: four_days_ago_high: {four_days_ago_high}, four_days_ago_low: {four_days_ago_low}\n")
        
        if (ORBL15_one <= ORBL15_two and ORBL15_one <= ORBL15_thr and ORBL15_one <= ORBL15_four and
            ORBH15_one >= ORBH15_two and ORBH15_one >= ORBH15_thr and ORBH15_one >= ORBH15_four       ):
            ORB15_IN_RANGE.append({
                "symbol": symbol_name,
                "ORBH15_one": ORBH15_one,
                "ORBL15_one": ORBL15_one,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol,
                "high_close_diff": high_close_diff,
                "high_low_diff": high_low_diff
                
            })
            #print("ORB15 IN RANGE")
            #print(f"{symbol_name}:ORBH15_one: {ORBH15_one}, ORBL15_one: {ORBL15_one}\n")

        if (ORBL15_one <= ORBL15_two and ORBL15_one <= ORBL15_thr and ORBL15_one <= ORBL15_four and  ORBH15_one >= ORBH15_two and ORBH15_one >= ORBH15_thr and ORBH15_one >= ORBH15_four and yesterday_close > ORBH15_one  and yesterday_close > ema_200 and  yesterday_close > ema_50 and yesterday_close > ema_20    ):
            ORB15_IN_UP_SIDE.append({
                "symbol": symbol_name,
                "ORBH15_one": ORBH15_one,
                "ORBL15_one": ORBL15_one,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "ema_200": ema_200,
                "ema_50": ema_50,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol,
                "high_close_diff": high_close_diff,
                "high_low_diff": high_low_diff
                
            })
            #print("ORB15 IN UP SIDE")
            #print(f"{symbol_name}:ORBH15_one: {ORBH15_one}, ORBL15_one: {ORBL15_one}\n")

        if (ORBL15_one <= ORBL15_two and ORBL15_one <= ORBL15_thr and ORBL15_one <= ORBL15_four and ORBH15_one >= ORBH15_two and ORBH15_one >= ORBH15_thr and ORBH15_one >= ORBH15_four and yesterday_close < ORBL15_one  and yesterday_close < ema_200 and  yesterday_close < ema_50 and yesterday_close < ema_20    ):
            ORB15_IN_DOWN_SIDE.append({
                "symbol": symbol_name,
                "ORBH15_one": ORBH15_one,
                "ORBL15_one": ORBL15_one,
                "open": yesterday_open,
                "high": yesterday_high,
                "low": yesterday_low,
                "close": yesterday_close,
                "ema_200": ema_200,
                "ema_50": ema_50,
                "yesterday_vol": yesterday_vol,
                "daybefore_vol": daybefore_vol
                
            })
            #print("ORB15 IN DOWN SIDE")
            #print(f"{symbol_name}:ORBH15_one: {ORBH15_one}, ORBL15_one: {ORBL15_one}\n")
        

            


    

# Main loop
def main_loop():
    
    global socket_opened, terminate_websocket
    #print("in main loop")
    current_time = datetime.now().time()
    print("current_time:",current_time)
    symbols = [
                "360ONE", "ACC", "AIAENG", "APLAPOLLO", "AUBANK", "AADHARHFC", "AARTIIND", "AAVAS", "ADANIENT", "ADANIGREEN", "ADANIPORTS",
                "ADANIPOWER",  "CENTURYPLY", "ABSLAMC", "ACCELYA", "ACE", "AETHER", "AFFLE", "AGI", "AJANTPHARM", "AKZOINDIA", "CARTRADE",  "ALKYLAMINE", "ARE&M", "AMBER", "ANGELONE", "ANURAS", 
                "APLLTD", "APOLLOHOSP", "APOLLOTYRE", "ARVINDFASN", "ASAHIINDIA", "ASAL", "ASIANPAINT", "ASTERDM", "ASTRAL", "ATGL", "ATUL", "AUBANK", "AVANTIFEED", 
                "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BALKRISIND", "BALRAMCHIN", "BATAINDIA", "BAYERCROP", "BBTC", "BDL", "BEL",
                "BERGEPAINT", "BFINVEST", "BHARATFORG", "BHEL",  "BIOCON", "BIRLACORPN", "BLUESTARCO", "AVALON","CMSINFO", "POLICYBZR",  "BORORENEW", "BPCL", "BRITANNIA", "BSOFT", "CAMS",  "CANFINHOME", 
                "CARBORUNIV", "CEATLTD",  "CERA", "CHALET", "CHAMBLFERT","TEJASNET",  "CHOLAFIN", "CIPLA", "CLEAN", "COALINDIA", "COCHINSHIP", "COLPAL", "CONCOR", 
                "COROMANDEL", "CREDITACC", "CRISIL", "CROMPTON", "CSBBANK", "CUMMINSIND", "CYIENT", "DCMSHRIRAM", "DEEPAKNTR", "DHANUKA",  "DIVISLAB", "DMART", 
                "DRREDDY",  "EICHERMOT", "EIDPARRY", "EIHOTEL", "ELGIEQUIP", "EMAMILTD", "ADANIENSOL", "ERIS", "ESCORTS",  "EXIDEIND", "FDC", "FINEORG", "BLS", 
                "FLUOROCHEM", "FMGOETZE", "FORTIS", "FSL", "GABRIEL", "GALAXYSURF", "GARFIBRES", "GESHIP", "GHCL", "GICRE", "GLAND", "GLAXO", "GLENMARK", "GMDCLTD",
                "JYOTHYLAB", "TATATECH", "GNFC", "GOCOLORS", "GODFRYPHLP", "GODREJAGRO", "GODREJCP", "GODREJIND", "GODREJPROP", "GRANULES", "GRAPHITE", "GRASIM", 
                "GREENLAM", "GREENPANEL","CGPOWER", "GRINDWELL", "GRSE", "GSPL", "GUJGASLTD", "HAL", "HAPPSTMNDS","MAXHEALTH", "HAVELLS", "HBLPOWER", "HCLTECH", "HDFCAMC",
                "HDFCBANK", "HDFCLIFE","JINDWORLD", "HESTERBIO", "HGINFRA", "HIKAL", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HOMEFIRST", "ICICIBANK", "ICICIGI",
                "RAILTEL", "ICICIPRULI", "IFBIND", "IGL", "JPOLYINVST", "STERTOOLS","PSPPROJECT", "INDHOTEL", "INDIACEM", "INDIAGLYCO", "INDIAMART", "INDIANB", "INDIGO",
                "INDUSINDBK","INDUSTOWER", "INFOBEAN", "INFY", "INGERRAND",  "INTELLECT", "IPCALAB", "IRCTC", "ITC", "ITI",  "JAICORPLTD", "EPACK",  "JBCHEPHARM",
                "JINDALPOLY", "JINDALSTEL", "JKCEMENT", "JKLAKSHMI", "JKPAPER", "JKTYRE", "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "KAJARIACER",  "KITEX","TITAGARH", 
                "KEI", "KIRLOSBROS", "KIRLOSENG", "KIRLOSIND", "KNRCON", "KOTAKBANK", "KRBL", "LAOPALA", "LICHSGFIN", "LICI", "LINDEINDIA", "LT", "LTIM",
                "ARTEMISMED", "JGCHEM","ATGL",  "LTTS", "LUMAXIND","LUXIND", "M&M",  "MAHLIFE", "MAPMYINDIA","IIFL",  "MARICO", "MARUTI", "MASTEK",  "MCX",
                "METROBRAND", "MFSL", "MIDHANI", "WEBELSOLAR",   "MINDSPACE",   "MOIL","STARHEALTH", "KIRIINDUS", "JASH", "KIMS", "KRN",  "MPHASIS", "MTARTECH",
                "MUTHOOTFIN", "NATIONALUM", "NAUKRI", "NAVINFLUOR", "WAAREEENER", "NCC", "NDRAUTO","NEOGEN", "NESCO", "NH", "MOTILALOFS",  "NTPC", "NUCLEUS", "OIL",
                "ORIENTCEM","RVNL", "PCBL", "PEL", "PERSISTENT", "BRIGADE","SYRMA", "PETRONET", "PFC", "PFIZER",  "PHOENIXLTD", "PIDILITIND", "PIIND", "POLYCAB",
                "TIMETECHNO","ITDCEM", "POONAWALLA", "POWERGRID", "PRESTIGE", "PRINCEPIPE", "QUESS", "RADICO", "RALLIS", "RAMCOCEM", "RATNAMANI", "RAYMOND",
                "RECLTD", "RELAXO", "RELIANCE", "RHIM", "RITES", "ROUTE", "SANOFI", "SBICARD", "SBILIFE",  "SBIN",  "SCHAEFFLER", "SHARDACROP",  "SHILPAMED",
                "SHOPERSTOP","MARKSANS",  "SHRIRAMFIN", "SIEMENS", "SIS", "SKFINDIA", "SOBHA", "SONACOMS", 
                "SRF", "STAR", "SUMICHEM",  "SUNDARMFIN", "SUNDRMFAST","PREMIERENE", "SUNPHARMA", "SUNTECK", "SUPRAJIT", "SURYAROSNI", "SWANENERGY", "SYMPHONY",
                "SYNGENE", "TATACHEM", "TATACOMM", "TATACONSUM", "TATAELXSI", "TATAINVEST",  "TATAMOTORS","TRITURBINE", "TATAPOWER", "TCI", "TCS", "TEAMLEASE",
                "TECHM", "THERMAX", "THYROCARE", "TIINDIA",  "TIMKEN", "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT", "TTKPRESTIG", "ORIENTTECH","PGEL","TVSMOTOR",
                "UBL", "UFLEX", "UNOMINDA", "UPL", "UTIAMC","DEEPAKFERT", "VBL", "VEDL", "VENKEYS", "VGUARD", "VINATIORGA", "VIPIND", "VMART", "VOLTAS", "VSTIND",
                "WABAG", "WELCORP",  "WESTLIFE", "WHIRLPOOL", "WIPRO",  "WOCKPHARMA","OBEROIRLTY", "ZENSARTECH", "KALYANKJIL", "BSE", "LAURUSLABS", "CDSL", "SONACOMS", "BBOX"
    
]

    create_subscription_list(symbols)
    open_low_list = []
    open_high_list = []
    ORB15_IN_UP_SIDE = []
    ORB15_IN_RANGE = []
    ORB15_IN_DOWN_SIDE = []
    buy_cond2_list = []
    sell_cond2_list = []
    high_orbh15_list = []

    for symbol in symbols:
        historical_data = fetch_and_filter_symbol(symbol)
        if historical_data:
            evaluate_buy_condition_1(historical_data, open_low_list,buy_cond2_list,sell_cond2_list,high_orbh15_list, open_high_list,symbol,ORB15_IN_UP_SIDE,ORB15_IN_DOWN_SIDE,ORB15_IN_RANGE)
    #evaluate_buy_condition_1()
    # Print results for OPEN == LOW
    if open_low_list:
        print("\nSymbols with OPEN == LOW:")
        for item in open_low_list:
            print(f"{item['symbol']}: Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}")

    if buy_cond2_list:
        print("\nSymbols with buy_cond2_list:")
        for item in buy_cond2_list:
            print(f"{item['symbol']}: Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}, day2_open: {item['day2_open']}, day2_high: {item['day2_high']}, day2_low: {item['day2_low']}, day2_close: {item['day2_close']},ORBH15:{item['ORBH15_one']},ORBL15:{item['ORBL15_one']},day1_V: {item['yesterday_vol']},day2_V:{item['daybefore_vol']}")
    #if sell_cond2_list:
     #   print("\nSymbols with sell_cond2_list:")
      #  for item in sell_cond2_list:
       #     print(f"{item['symbol']}: Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}, day2_open: {item['day2_open']}, day2_high: {item['day2_high']}, day2_low: {item['day2_low']}, day2_close: {item['day2_close']},ORBH15_one: {item['ORBH15_one']}, ORBL15_one: {item['ORBL15_one']}")

    if high_orbh15_list:
        print("\nSymbols with high == orbh15_list:")
        for item in high_orbh15_list:
            print(f"{item['symbol']}:Open:{item['open']},High:{item['high']},Low:{item['low']},Close:{item['close']},d2_O:{item['day2_open']},d2_H:{item['day2_high']},d2_L:{item['day2_low']},d2_C:{item['day2_close']},ORBH15:{item['ORBH15_one']},ORBL15:{item['ORBL15_one']},\nday1_V: {item['yesterday_vol']},day2_V:{item['daybefore_vol']},high_close_diff: {item['high_close_diff']},high_low_diff: {item['high_low_diff']}")

    # Print results for OPEN == HIGH
    if open_high_list:
        print("\nSymbols with OPEN == HIGH:")
        for item in open_high_list:
            print(f"{item['symbol']}: Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}")

    if ORB15_IN_UP_SIDE:
        print("\nORB15_IN_UP_SIDE:")
        for item in ORB15_IN_UP_SIDE:
            print(f"{item['symbol']}: ORBH15_one: {item['ORBH15_one']}, ORBL15_one: {item['ORBL15_one']},Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}, ema_200: {item['ema_200']}, ema_50: {item['ema_50']}")
    if ORB15_IN_DOWN_SIDE:
        print("\nORB15_IN_DOWN_SIDE:")
        for item in ORB15_IN_DOWN_SIDE:
            print(f"{item['symbol']}: ORBH15_one: {item['ORBH15_one']}, ORBL15_one: {item['ORBL15_one']},Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}, ema_200: {item['ema_200']}, ema_50: {item['ema_50']}")
    #if ORB15_IN_RANGE:
     #   print("\nORB15_IN_RANGE:")
      #  for item in ORB15_IN_RANGE:
       #     print(f"{item['symbol']}: ORBH15_one: {item['ORBH15_one']}, ORBL15_one: {item['ORBL15_one']},Open: {item['open']}, High: {item['high']}, Low: {item['low']}, Close: {item['close']}")

            
# Entry point
if __name__ == "__main__":
    main_loop()