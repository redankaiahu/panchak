import time
import threading
import csv
import os
from datetime import datetime
from datetime import timedelta
from kiteconnect import KiteConnect, KiteTicker
from colorama import init, Fore, Style
import pandas as pd

init(autoreset=True)

# ==========================================
# TRADING MODE
# ==========================================
TRADING_MODE = "PAPER"   # "LIVE" or "PAPER"

FIXED_QTY = 10
STOP_LOSS_PERCENT = 1        # 1%
TARGET_PERCENT = 2.5         # 2.5%
TRAIL_STEP_PERCENT = 1       # trail every 1% move
BROKERAGE_PER_ORDER = 20     # approx MIS brokerage

HIST_DIR = "historical_data"
MINUTE_DIR = os.path.join(HIST_DIR, "minute")
DAILY_DIR = os.path.join(HIST_DIR, "daily")

os.makedirs(MINUTE_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)

REPLAY_DURATION_MINUTES = 15  # change 10 / 15 as needed


# ==========================================
# REALTIME LTP CACHE (WebSocket Based)
# ==========================================
latest_prices = {}


# ==========================================
# PAPER TRADE STORAGE
# ==========================================
today_str = datetime.now().strftime("%d-%m-%Y")
LOG_TXT_FILE = f"execution_log_{today_str}.txt"

paper_positions = {}
paper_trade_log_file = f"paper_trades_{today_str}.csv"

if TRADING_MODE == "PAPER":
    if not os.path.exists(paper_trade_log_file):
        with open(paper_trade_log_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Time", "Symbol", "Side", "Entry",
                "Exit", "Qty", "PnL", "Reason"
            ])



# ==========================================
# CONFIG
# ==========================================
API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"
# Use low priced stocks for small capital
SYMBOLS = [
    "360ONE", "ACC", "AIAENG", "APLAPOLLO", "AUBANK", "AAVAS", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "CENTURYPLY", "ABSLAMC", "ACCELYA", "ACE",
"AETHER", "AFFLE", "AGI", "AJANTPHARM", "AKZOINDIA", "CARTRADE",  "ALKYLAMINE", "ARE&M",  "ANGELONE", "ANURAS", "APLLTD", "ASAHIINDIA", "ASIANPAINT", "ASTERDM",
"ASTRAL", "ATGL", "AUBANK", "AVANTIFEED", "AXISBANK", "BAJAJFINSV", "BALKRISIND", "BATAINDIA", "BBTC", "BDL","BHARATFORG", "BIOCON", "BIRLACORPN", "BLUESTARCO",
"AVALON", "POLICYBZR",  "BRITANNIA",  "CAMS",  "CANFINHOME", "BAJAJHCARE","HCG","CARBORUNIV", "CEATLTD", "CHALET", "CHAMBLFERT","TEJASNET", "CHOLAFIN", "CIPLA",
"CLEAN", "COLPAL", "CONCOR","CHENNPETRO", "COROMANDEL", "CREDITACC", "CUMMINSIND", "CYIENT", "DCMSHRIRAM", "DEEPAKNTR", "DHANUKA", "DMART","KFINTECH", "SALZERELEC",
"DRREDDY", "EIDPARRY",  "ELGIEQUIP", "EMAMILTD", "ADANIENSOL", "ERIS", "ESCORTS", "DIAMONDYD","FLUOROCHEM",  "FORTIS",  "GABRIEL", "GALAXYSURF", "GARFIBRES",
"GESHIP", "GHCL", "GLAND", "GLAXO", "GLENMARK",   "TATATECH", "GNFC", "GOCOLORS", "GODREJAGRO", "GODREJCP", "GODREJIND", "GODREJPROP", "GRANULES",
"GRAPHITE", "GRASIM",  "CGPOWER", "GRINDWELL", "GRSE", "HAL", "HAPPSTMNDS","MAXHEALTH", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE",
"HGINFRA", "HINDALCO"
]


RISK_PER_TRADE_PERCENT = 1
DAILY_MAX_LOSS_PERCENT = 5
SQUARE_OFF_TIME = "15:15"
COOLDOWN_SECONDS = 60

TRAIL_START_PERCENT = 0.5

# ==========================================
# LOGGING SYSTEM
# ==========================================


trade_statistics = {
    "total_trades": 0,
    "buy_trades": 0,
    "sell_trades": 0,
    "sl_hits": 0,
    "target_hits": 0
}

order_count = 0
daily_pnl = 0
trading_enabled = True

def write_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_TXT_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

# ==========================================
# INITIALIZATION
# ==========================================
kite = KiteConnect(api_key=API_KEY)
ACCESS_TOKEN = open(ACCESS_TOKEN_FILE).read().strip()
kite.set_access_token(ACCESS_TOKEN)
kws = KiteTicker(API_KEY, ACCESS_TOKEN)

instrument_tokens = {}
trades_taken = {}
symbol_last_trade_time = {}

# ==========================================
# LOAD INSTRUMENTS
# ==========================================
instruments = kite.instruments("NSE")
for inst in instruments:
    if inst["tradingsymbol"] in SYMBOLS:
        instrument_tokens[inst["instrument_token"]] = inst["tradingsymbol"]

tokens = list(instrument_tokens.keys())

# ==========================================
# QTY CALCULATION
# ==========================================
def calculate_qty(entry, sl):
    return 1

# ==========================================
# PLACE TRADE
# ==========================================
# ==========================================
# PLACE TRADE
# ==========================================
def place_trade(symbol, side, ltp, tick, condition_name):

    global order_count

    if symbol in trades_taken:
        return

    qty = FIXED_QTY

    sl = round(
        ltp * (1 - STOP_LOSS_PERCENT/100), 2
    ) if side == "BUY" else round(
        ltp * (1 + STOP_LOSS_PERCENT/100), 2
    )

    target = round(
        ltp * (1 + TARGET_PERCENT/100), 2
    ) if side == "BUY" else round(
        ltp * (1 - TARGET_PERCENT/100), 2
    )

    color = Fore.GREEN if side == "BUY" else Fore.RED

    print(color + "-> " * 20)
    print(color +
          f"{TRADING_MODE} {side} | {symbol} | "
          f"Entry:{ltp} SL:{sl} Target:{target} Qty:{qty}"
    )

    write_log(
        f"{TRADING_MODE} {side} {symbol} "
        f"Entry:{ltp} SL:{sl} Target:{target}"
    )

    # =============================
    # STATISTICS UPDATE
    # =============================
    order_count += 1
    trade_statistics["total_trades"] += 1

    if side == "BUY":
        trade_statistics["buy_trades"] += 1
    else:
        trade_statistics["sell_trades"] += 1

    # =============================
    # LIVE ORDER EXECUTION
    # =============================
    if TRADING_MODE == "LIVE":
        try:
            transaction = (
                kite.TRANSACTION_TYPE_BUY
                if side == "BUY"
                else kite.TRANSACTION_TYPE_SELL
            )

            kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=transaction,
                quantity=qty,
                order_type=kite.ORDER_TYPE_MARKET,
                product=kite.PRODUCT_MIS
            )

        except Exception as e:
            print(Fore.RED + f"Live order failed: {e}")
            return

    # =============================
    # STORE POSITION (PAPER & LIVE TRACKING)
    # =============================
    paper_positions[symbol] = {
        "side": side,
        "entry": ltp,
        "sl": sl,
        "target": target,
        "qty": qty,
        "trail_level": 0,
        "status": "OPEN"
    }

    trades_taken[symbol] = True



# ==========================================
# OCO MONITOR
# ==========================================
def oco_monitor(symbol):
    while True:
        try:
            orders = kite.orders()
            sl_id = trades_taken[symbol]["sl"]
            tgt_id = trades_taken[symbol]["target"]

            sl_order = next(o for o in orders if o["order_id"] == sl_id)
            tgt_order = next(o for o in orders if o["order_id"] == tgt_id)

            if sl_order["status"] == "COMPLETE":
                trade_statistics["sl_hits"] += 1
                kite.cancel_order(variety="regular", order_id=tgt_id)
                msg = f"{symbol} STOP LOSS HIT. Target Cancelled."
                print(Fore.YELLOW + msg)
                write_log(msg)
                break

            if tgt_order["status"] == "COMPLETE":
                trade_statistics["target_hits"] += 1
                kite.cancel_order(variety="regular", order_id=sl_id)
                msg = f"{symbol} TARGET HIT. Stop Loss Cancelled."
                print(Fore.BLUE + msg)
                write_log(msg)
                break

            time.sleep(2)

        except:
            break

# ==========================================
# YESTERDAY DATA CACHE
# ==========================================
yesterday_data = {}
live_day_extremes = {}

# ==========================================
# YESTERDAY DATA CACHE WITH LOCAL STORAGE
# ==========================================
YESTERDAY_CACHE_FILE = "yesterday_ohlc_cache.csv"

yesterday_data = {}
live_day_extremes = {}

def load_yesterday_from_local(replay_date):

    print(f"📂 Loading Yesterday OHLC for {replay_date.date()}")

    # Make replay_date timezone-naive
    replay_date = pd.to_datetime(replay_date).tz_localize(None)

    for symbol in SYMBOLS:

        file_path = os.path.join(DAILY_DIR, f"{symbol}_daily.csv")

        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)

        # Convert to datetime and REMOVE timezone
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

        df = df.sort_values("date")

        df_before = df[df["date"] < replay_date]

        if df_before.empty:
            continue

        yest = df_before.iloc[-1]

        yesterday_data[symbol] = {
            "high": yest["high"],
            "low": yest["low"],
            "close": yest["close"],
            "volume": yest["volume"]
        }

        live_day_extremes[symbol] = {
            "high": 0,
            "low": 999999
        }

###################################         ADD EMA7 / EMA20 / EMA50 CACHE

# ==========================================
# EMA CACHE
# ==========================================
ema_cache = {}

def load_ema_data_from_local(replay_date=None):

    print("📊 Loading EMA from local daily data...")

    for symbol in SYMBOLS:

        file_path = os.path.join(DAILY_DIR, f"{symbol}_daily.csv")

        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        df = df.sort_values("date")

        # If replay mode → cut data till replay date
        if replay_date:
            df = df[df["date"] < replay_date]

        if len(df) < 50:
            continue

        df["ema7"] = df["close"].ewm(span=7, adjust=False).mean()
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

        ema_cache[symbol] = {
            "ema7": df["ema7"].iloc[-1],
            "ema20": df["ema20"].iloc[-1],
            "ema50": df["ema50"].iloc[-1]
        }

    print("✅ EMA Loaded from Local")


#######################     ADD REALTIME PAPER MONITOR (VERY IMPORTANT)
# ==========================================
# REALTIME PAPER POSITION MANAGER (NO REST CALLS)
# ==========================================
def paper_position_manager():

    global daily_pnl

    while True:

        for symbol in list(paper_positions.keys()):

            pos = paper_positions[symbol]

            if pos["status"] != "OPEN":
                continue

            ltp = latest_prices.get(symbol)

            if not ltp:
                continue

            entry = pos["entry"]
            side = pos["side"]
            qty = pos["qty"]

            points = (ltp - entry) if side == "BUY" else (entry - ltp)
            pnl = points * qty

            # =============================
            # TRAILING LOGIC
            # =============================
            move_percent = abs((ltp - entry) / entry) * 100

            if move_percent >= (pos["trail_level"] + TRAIL_STEP_PERCENT):

                pos["trail_level"] += TRAIL_STEP_PERCENT

                if side == "BUY":
                    pos["sl"] = round(ltp * (1 - STOP_LOSS_PERCENT/100), 2)
                else:
                    pos["sl"] = round(ltp * (1 + STOP_LOSS_PERCENT/100), 2)

                print(Fore.YELLOW + f"TRAIL UPDATED | {symbol} | New SL: {pos['sl']}")

            # =============================
            # EXIT CONDITIONS
            # =============================
            if (side == "BUY" and ltp <= pos["sl"]) or \
               (side == "SELL" and ltp >= pos["sl"]):

                exit_reason = "SL HIT"

            elif (side == "BUY" and ltp >= pos["target"]) or \
                 (side == "SELL" and ltp <= pos["target"]):

                exit_reason = "TARGET HIT"

            else:
                continue

            pos["status"] = "CLOSED"

            gross_pnl = pnl
            net_pnl = gross_pnl - (BROKERAGE_PER_ORDER * 2)
            daily_pnl += net_pnl

            trade_statistics["sl_hits" if exit_reason=="SL HIT" else "target_hits"] += 1

            print(Fore.CYAN +
                f"{symbol} CLOSED | {exit_reason} | "
                f"{side} | Entry:{entry} | Exit:{ltp} | "
                f"Points:{points:.2f} | NetPnL:{net_pnl:.2f}"
            )

            write_log(
                f"{symbol} {exit_reason} {side} Entry:{entry} Exit:{ltp} NetPnL:{net_pnl:.2f}"
            )

            with open(paper_trade_log_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.now(),
                    symbol,
                    side,
                    entry,
                    ltp,
                    qty,
                    net_pnl,
                    exit_reason
                ])

        time.sleep(1)




# ==========================================
# INSTITUTIONAL GRADE STRATEGY (DEBUG SAFE)
# ==========================================
def strategy(token, tick):

    symbol = instrument_tokens.get(token)
    if not symbol:
        return

    if symbol not in yesterday_data:
        return

    if symbol in trades_taken:
        return

    # ===============================
    # EXTRACT DATA FIRST (IMPORTANT)
    # ===============================
    ltp = tick["last_price"]
    live_open = tick["ohlc"]["open"]
    live_high = tick["ohlc"]["high"]
    live_low = tick["ohlc"]["low"]
    live_volume = tick.get("volume", 0)

    yest_high = yesterday_data[symbol]["high"]
    yest_low = yesterday_data[symbol]["low"]
    yest_close = yesterday_data[symbol]["close"]
    yest_volume = yesterday_data[symbol]["volume"]

    # Update live extremes
    live_day_extremes[symbol]["high"] = max(
        live_day_extremes[symbol]["high"], ltp
    )
    live_day_extremes[symbol]["low"] = min(
        live_day_extremes[symbol]["low"], ltp
    )

    day_high = live_day_extremes[symbol]["high"]
    day_low = live_day_extremes[symbol]["low"]

    change_percent = ((ltp - yest_close) / yest_close) * 100
    vol_percent = (live_volume / yest_volume) * 100 if yest_volume else 0

    # ================= DEBUG PRINT =================
    #print(
     #   f"{symbol} | LTP:{ltp:.2f} | YH:{yest_high:.2f} | "
      #  f"YL:{yest_low:.2f} | Change%:{change_percent:.2f}"
    #)
    # ================= PRE-CALCULATIONS =================

    ema7 = ema_cache.get(symbol, {}).get("ema7")
    ema20 = ema_cache.get(symbol, {}).get("ema20")
    ema50 = ema_cache.get(symbol, {}).get("ema50")

    if ema7 is None or ema20 is None:
        return

    dist_from_day_high = (
        ((day_high - ltp) / day_high) * 100
        if day_high != 0 else 999
    )
    # Distance from today's low
    dist_from_day_low = (
        ((ltp - day_low) / day_low) * 100
        if day_low != 0 else 999
    )

    # ================= BUY CONDITION =================
    if (
        live_open <= yest_high and
        ltp >= yest_high and
        dist_from_day_high <= 0.4 and
        0.6 <= change_percent <= 1.5 and
        ltp >= ema7 >= ema20 and
        vol_percent >= -50
    ):

        print(Fore.GREEN +
            f"BUY SIGNAL TRIGGERED | {symbol} | LTP:{ltp} >= YH:{yest_high}")

        place_trade(symbol, "BUY", ltp, tick, "Simple YH Break")


    

    # ================= SELL CONDITION =================
    if (
        live_open >= yest_low and
        ltp <= yest_low and
        dist_from_day_low <= 0.4 and
        -1.5 <= change_percent <= -0.6 and
        ltp <= ema7 <= ema20 and #< ema50 and
        vol_percent >= -50
    ):

        print(Fore.RED +
              f"SELL SIGNAL TRIGGERED | {symbol} | LTP:{ltp} <= YL:{yest_low}")

        place_trade(symbol, "SELL", ltp, tick, "Simple YL Break")



# ==========================================
# POSITION MONITOR
# ==========================================
def position_manager():
    global daily_pnl
    while True:
        try:
            positions = kite.positions()["net"]
            daily_pnl = sum([p["pnl"] for p in positions if p["quantity"] != 0])
        except:
            pass
        time.sleep(5)

# ==========================================
# DAILY SUMMARY
# ==========================================
def daily_summary():
    print("\n" + "="*60)
    print(Fore.CYAN + "📊 DAILY TRADING SUMMARY")
    print("="*60)

    summary = (
        f"Total Trades: {trade_statistics['total_trades']}\n"
        f"Buy Trades: {trade_statistics['buy_trades']}\n"
        f"Sell Trades: {trade_statistics['sell_trades']}\n"
        f"Stop Loss Hits: {trade_statistics['sl_hits']}\n"
        f"Target Hits: {trade_statistics['target_hits']}\n"
        f"Final Live PnL: {daily_pnl}\n"
    )

    print(summary)
    write_log("===== DAILY SUMMARY =====")
    write_log(summary)

# ==========================================
# AUTO SQUARE OFF
# ==========================================
def auto_square_off():

    while True:

        if datetime.now().strftime("%H:%M") >= SQUARE_OFF_TIME:

            print(Fore.MAGENTA + "\n🔔 AUTO SQUARE OFF TRIGGERED")

            # =====================================
            # LIVE MODE SQUARE OFF
            # =====================================
            if TRADING_MODE == "LIVE":

                try:
                    positions = kite.positions()["net"]

                    for pos in positions:
                        if pos["quantity"] != 0:

                            side = kite.TRANSACTION_TYPE_SELL if pos["quantity"] > 0 else kite.TRANSACTION_TYPE_BUY

                            kite.place_order(
                                variety=kite.VARIETY_REGULAR,
                                exchange=pos["exchange"],
                                tradingsymbol=pos["tradingsymbol"],
                                transaction_type=side,
                                quantity=abs(pos["quantity"]),
                                order_type=kite.ORDER_TYPE_MARKET,
                                product=kite.PRODUCT_MIS
                            )

                            print(Fore.YELLOW + f"LIVE SQUARE OFF: {pos['tradingsymbol']}")

                except Exception as e:
                    print(Fore.RED + f"LIVE Square off error: {e}")

            # =====================================
            # PAPER MODE SQUARE OFF
            # =====================================
            elif TRADING_MODE == "PAPER":

                for symbol in list(paper_positions.keys()):

                    pos = paper_positions[symbol]

                    if pos["status"] != "OPEN":
                        continue

                    ltp = latest_prices.get(symbol)

                    if not ltp:
                        continue

                    entry = pos["entry"]
                    side = pos["side"]
                    qty = pos["qty"]

                    points = (ltp - entry) if side == "BUY" else (entry - ltp)
                    gross_pnl = points * qty
                    net_pnl = gross_pnl - (BROKERAGE_PER_ORDER * 2)

                    pos["status"] = "CLOSED"

                    print(Fore.MAGENTA +
                        f"PAPER SQUARE OFF | {symbol} | {side} | "
                        f"Entry:{entry} | Exit:{ltp} | "
                        f"Points:{points:.2f} | Net PnL:{net_pnl:.2f}")

                    write_log(
                        #f"PAPER SQUARE OFF {symbol} Exit:{ltp} NetPnL:{net_pnl:.2f}"
                        f"PAPER SQUARE OFF {symbol} {side} Entry:{entry} Exit:{ltp} NetPnL:{net_pnl:.2f}"
                    )

            daily_summary()
            break

        time.sleep(20)




################        Add 15-Minute Position Summary Monitor
# ==========================================
# 15 MIN POSITION DISPLAY
# ==========================================
# ==========================================
# 15 MIN POSITION DISPLAY (WebSocket Based)
# ==========================================
def paper_position_summary():

    while True:

        time.sleep(900)  # 15 minutes

        if TRADING_MODE != "PAPER":
            continue

        print("\n" + "="*80)
        print(Fore.CYAN + "📊 15 MIN POSITION SUMMARY")
        print("="*80)

        for symbol in paper_positions:

            pos = paper_positions[symbol]

            if pos["status"] != "OPEN":
                continue

            ltp = latest_prices.get(symbol)

            if not ltp:
                continue

            entry = pos["entry"]
            side = pos["side"]
            qty = pos["qty"]

            points = (ltp - entry) if side == "BUY" else (entry - ltp)
            gross_pnl = points * qty
            net_pnl = gross_pnl - (BROKERAGE_PER_ORDER * 2)

            color = Fore.GREEN if net_pnl >= 0 else Fore.RED

            print(color +
                  f"{symbol} | {side} | Entry:{entry} | "
                  f"LTP:{ltp} | Points:{points:.2f} | "
                  f"NetPnL:{net_pnl:.2f}")

        print("="*80)




##############          START PAPER ENGINE THREAD
if TRADING_MODE == "PAPER":
    threading.Thread(target=paper_position_manager, daemon=True).start()

if TRADING_MODE == "PAPER":
    threading.Thread(target=paper_position_summary, daemon=True).start()


#########################               REPLAY ENGINE BLOCK

# ==========================================
# CLEAN REPLAY ENGINE (DEBUG VERSION)
# ==========================================
def run_market_replay_proper(replay_date):

    #print(f"\n🔁 REPLAY STARTED FOR {replay_date.date()}")
    #print("=" * 80)

    replay_date = pd.to_datetime(replay_date).date()

    # -------------------------------
    # STEP 1: LOAD YESTERDAY OHLC
    # -------------------------------
    load_yesterday_from_local(pd.to_datetime(replay_date))
    #print(f"✅ Yesterday data loaded for {len(yesterday_data)} symbols")

    # -------------------------------
    # STEP 2: LOAD EMA CACHE
    # -------------------------------
    load_ema_data_from_local(pd.to_datetime(replay_date))
    #print(f"✅ EMA cache loaded for {len(ema_cache)} symbols")

    # -------------------------------
    # STEP 3: LOAD MINUTE DATA
    # -------------------------------
    symbol_minute_data = {}

    for symbol in SYMBOLS:

        file_path = os.path.join(MINUTE_DIR, f"{symbol}.csv")

        #print(f"\n📂 Checking minute file for {symbol}")

        if not os.path.exists(file_path):
            print("❌ File not found")
            continue

        df = pd.read_csv(file_path)

        if df.empty:
            print("❌ CSV empty")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["date_only"] = df["date"].dt.date

        available_dates = df["date_only"].unique()

        #print(f"Available Dates Range: {min(available_dates)} → {max(available_dates)}")

        day_df = df[df["date_only"] == replay_date]

        if day_df.empty:
            print(f"❌ No minute data for {replay_date}")
            continue

        #print(f"✅ Loaded {len(day_df)} candles for {symbol}")

        symbol_minute_data[symbol] = day_df.sort_values("date")

    #print("\n" + "=" * 80)
    #print(f"Symbols with minute data: {len(symbol_minute_data)}")

    if len(symbol_minute_data) == 0:
        print("❌ No data available for replay date. Exiting replay.")
        return

    # -------------------------------
    # STEP 4: COLLECT ALL TIMESTAMPS
    # -------------------------------
    all_times = sorted(
        set(
            t
            for df in symbol_minute_data.values()
            for t in df["date"]
        )
    )

    print(f"Total timestamps found: {len(all_times)}")
    print("=" * 80)

    # -------------------------------
    # STEP 5: REPLAY LOOP
    # -------------------------------
    total_market_minutes = 375
    total_replay_seconds = REPLAY_DURATION_MINUTES * 60
    sleep_per_candle = total_replay_seconds / total_market_minutes

    for current_time in all_times:

        #print(f"\n⏱ Replay Time: {current_time}")

        for symbol, df in symbol_minute_data.items():

            row = df[df["date"] == current_time]

            if row.empty:
                continue

            row = row.iloc[0]

            token = next(
                k for k, v in instrument_tokens.items()
                if v == symbol
            )

            fake_tick = {
                "instrument_token": token,
                "last_price": row["close"],
                "ohlc": {
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"]
                },
                "volume": row["volume"]
            }

            latest_prices[symbol] = row["close"]

            #print(f"   {symbol} | LTP:{row['close']}")

            strategy(token, fake_tick)

        time.sleep(sleep_per_candle)

    print("\n✅ Replay Finished")
    daily_summary()



# ==========================================
# DOWNLOAD & STORE 6 MONTH HISTORICAL DATA
# ==========================================

# ==========================================
# DOWNLOAD & STORE 6 MONTH HISTORICAL DATA (FIXED)
# ==========================================
def download_6_month_data():

    print("📥 Downloading 6 Months Historical Data (Chunked 60 Days)...")

    to_date = datetime.now()
    from_date = to_date - timedelta(days=180)

    for token, symbol in instrument_tokens.items():

        try:
            print(f"\nDownloading {symbol}...")

            # ==========================
            # DOWNLOAD MINUTE DATA IN 60 DAY CHUNKS
            # ==========================
            all_minute_data = []

            chunk_start = from_date

            while chunk_start < to_date:

                chunk_end = min(chunk_start + timedelta(days=60), to_date)

                print(f"  ⏳ Fetching {chunk_start.date()} → {chunk_end.date()}")

                minute_data = kite.historical_data(
                    token,
                    chunk_start,
                    chunk_end,
                    "minute"
                )

                if minute_data:
                    all_minute_data.extend(minute_data)

                chunk_start = chunk_end + timedelta(days=1)

                time.sleep(0.3)  # avoid rate limits

            if all_minute_data:
                minute_df = pd.DataFrame(all_minute_data)
                minute_df.drop_duplicates(subset=["date"], inplace=True)
                minute_df.sort_values("date", inplace=True)

                minute_df.to_csv(
                    os.path.join(MINUTE_DIR, f"{symbol}.csv"),
                    index=False
                )

            # ==========================
            # DOWNLOAD DAILY DATA (No 60 Day Limit)
            # ==========================
            daily_data = kite.historical_data(
                token,
                from_date,
                to_date,
                "day"
            )

            if daily_data:
                daily_df = pd.DataFrame(daily_data)
                daily_df.drop_duplicates(subset=["date"], inplace=True)
                daily_df.sort_values("date", inplace=True)

                daily_df.to_csv(
                    os.path.join(DAILY_DIR, f"{symbol}_daily.csv"),
                    index=False
                )

            print(f"  ✅ {symbol} Done")

        except Exception as e:
            print(f"❌ Error downloading {symbol}: {e}")

    print("\n✅ 6 Months Historical Data Download Complete")



# ==========================================
# WEBSOCKET
# ==========================================
# ==========================================
# WEBSOCKET TICKS HANDLER
# ==========================================
def on_ticks(ws, ticks):

    for tick in ticks:

        token = tick["instrument_token"]
        symbol = instrument_tokens.get(token)

        if not symbol:
            continue

        # Store realtime LTP from websocket
        latest_prices[symbol] = tick["last_price"]

        # Run strategy
        strategy(token, tick)


def on_connect(ws, response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    print(Fore.GREEN + "🚀 Connected to Kite WebSocket")

kws.on_ticks = on_ticks
kws.on_connect = on_connect

if TRADING_MODE == "LIVE":
    threading.Thread(target=position_manager, daemon=True).start()
    threading.Thread(target=auto_square_off, daemon=True).start()

if __name__ == "__main__":

    print("\n" + "="*70)

    mode_color = Fore.GREEN if TRADING_MODE == "PAPER" else Fore.RED

    print(mode_color + f"🚀 EXECUTION ENGINE STARTED | MODE: {TRADING_MODE}")
    print("="*70)

    print(Fore.CYAN + f"Symbols Loaded: {len(tokens)}")
    print(Fore.CYAN + f"Fixed Qty: {FIXED_QTY}")
    print(Fore.CYAN + f"Stop Loss %: {STOP_LOSS_PERCENT}%")
    print(Fore.CYAN + f"Target %: {TARGET_PERCENT}%")
    print(Fore.CYAN + f"Trail Step %: {TRAIL_STEP_PERCENT}%")
    print(Fore.CYAN + f"Square Off Time: {SQUARE_OFF_TIME}")
    print(Fore.CYAN + f"Daily Max Loss %: {DAILY_MAX_LOSS_PERCENT}%")

    print("="*70 + "\n")

    write_log(f"ENGINE STARTED | MODE: {TRADING_MODE}")
    if input("Download 6 months data? (y/n): ") == "y":
        download_6_month_data()

    if TRADING_MODE == "PAPER" and input("Replay Mode? (y/n): ") == "y":

        from datetime import datetime
        replay_date = datetime(2026, 2, 27)   # Change date here
        run_market_replay_proper(replay_date)

    else:
        kws.connect()