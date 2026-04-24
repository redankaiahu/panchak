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
STOP_LOSS_PERCENT = 0.75        # 1%
TARGET_PERCENT = 2.5         # 2.5%
TRAIL_STEP_PERCENT = 0.75       # trail every 1% move
BROKERAGE_PER_ORDER = 20     # approx MIS brokerage

HIST_DIR = "historical_data"
MINUTE_DIR = os.path.join(HIST_DIR, "minute")
DAILY_DIR = os.path.join(HIST_DIR, "daily")

os.makedirs(MINUTE_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)

REPLAY_DURATION_MINUTES = 5  # change 10 / 15 as needed
ENTRY_START_TIME = "09:31"
ENTRY_END_TIME = "14:16"
ENABLE_ORB = True
ENABLE_TEST = True


# ==========================================
# REALTIME LTP CACHE (WebSocket Based)
# ==========================================
latest_prices = {}
latest_highs = {}
latest_lows = {}
range_results = {}

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
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA",
    "BANKINDIA","BANKNIFTY","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULI","IEX","INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND","IRCTC","IRFC","IREDA","ITC",
    "JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
    "MPHASIS","MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NIFTY","NTPC","NUVAMA","NYKAA","NATIONALUM",
    "OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM","PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD",
    "PIDILITIND","PIIND","PNB","PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PREMIERENE","PRESTIGE","PPLPHARMA",
    "RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SAMMAANCAP","SBICARD","SBILIFE","SBIN","SHREECEM","SHRIRAMFIN",
    "SIEMENS","SOLARINDS","SRF","SUNPHARMA","SUPREMEIND","SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER","TATATECH","TATASTEEL","TCS","TECHM",
    "TIINDIA","TITAN","TMPV","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK","UNITDSPR","UPL","VBL","VEDL","VOLTAS","WAAREEENER","WIPRO","ZYDUSLIFE"
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
    "trailing_sl_hits": 0,
    "target_hits": 0
}

strategy_stats = {}
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
#def place_trade(symbol, side, ltp, tick, condition_name):
def place_trade(symbol, side, ltp, tick, condition_name, extra_info=None):

    global order_count
    entry_time = tick.get("date", datetime.now()).strftime("%H:%M")

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

    #print(color + "-> " * 20)
    structure_text = ""
    if extra_info:
        structure_text = (
            f"\n   ORB H/L: {extra_info['orb_high']} / {extra_info['orb_low']}"
            f"\n   15m H/L: {extra_info['f_high']} / {extra_info['f_low']}"
            f"\n   YH/YL: {extra_info['y_high']} / {extra_info['y_low']}"
            f"\n   YVol: {extra_info['y_vol']} | TVol: {extra_info['t_vol']}"
            f" | Vol%: {extra_info['vol_pct']}%"
        )

    print(color +
        f"{entry_time} | {condition_name} | {side} | {symbol} | "
        f"Entry:{ltp} SL:{sl} Target:{target} Qty:{qty}"
        f"{structure_text}\n"
    )

    write_log(
        f"{TRADING_MODE} {side} {symbol} "
        f"Entry:{ltp} SL:{sl} Target:{target}\n"
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

    if condition_name not in strategy_stats:
        strategy_stats[condition_name] = {
            "trades": 0,
            "wins": 0,
            "loss": 0,
            "pnl": 0
        }

    strategy_stats[condition_name]["trades"] += 1

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
    entry_time = tick.get("date", datetime.now()).strftime("%H:%M")

    paper_positions[symbol] = {
        "side": side,
        "entry": ltp,
        "sl": sl,
        "target": target,
        "qty": qty,
        "trail_level": 0,
        "status": "OPEN",
        "strategy": condition_name,
        "entry_time": entry_time
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
            current_high = latest_highs.get(symbol)
            current_low = latest_lows.get(symbol)

            if current_high is None or current_low is None:
                continue

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

            #if move_percent >= (pos["trail_level"] + TRAIL_STEP_PERCENT):
            while move_percent >= (pos["trail_level"] + TRAIL_STEP_PERCENT):

                pos["trail_level"] += TRAIL_STEP_PERCENT

                if side == "BUY":
                    #pos["sl"] = round(ltp * (1 - STOP_LOSS_PERCENT/100), 2)
                    new_sl = round(ltp * (1 - STOP_LOSS_PERCENT/100), 2)
                    if new_sl > pos["sl"]:
                        pos["sl"] = new_sl
                else:
                    #pos["sl"] = round(ltp * (1 + STOP_LOSS_PERCENT/100), 2)
                    new_sl = round(ltp * (1 + STOP_LOSS_PERCENT/100), 2)
                    if new_sl < pos["sl"]:
                        pos["sl"] = new_sl

                #gross_pnl = points * qty
                gross_pnl = points * qty
                net_pnl = gross_pnl - (BROKERAGE_PER_ORDER * 2)

                print(
                    Fore.YELLOW +
                    f"=============>>>>>>>>   TRAIL UPDATED | {symbol} | {side} | "
                    f"Entry:{entry} | LTP:{ltp} | "
                    f"New SL:{pos['sl']} | Target:{pos['target']} | "
                    f"Points:{points:.2f} | NetPnL:{net_pnl:.2f}\n"
                )

            # =============================
            # EXIT CONDITIONS (HIGH/LOW BASED)
            # =============================

            if side == "BUY":
                if ltp <= pos["sl"]:
                    exit_price = pos["sl"]
                    exit_reason = "SL HIT"
                elif ltp >= pos["target"]:
                    exit_price = pos["target"]
                    exit_reason = "TARGET HIT"
                else:
                    continue
            elif side == "SELL":
                if ltp >= pos["sl"]:
                    exit_price = pos["sl"]
                    exit_reason = "SL HIT"
                elif ltp <= pos["target"]:
                    exit_price = pos["target"]
                    exit_reason = "TARGET HIT"
                else:
                    continue

            points = (exit_price - entry) if side == "BUY" else (entry - exit_price)
            pos["status"] = "CLOSED"

            #gross_pnl = pnl
            gross_pnl = points * qty
            net_pnl = gross_pnl - (BROKERAGE_PER_ORDER * 2)
            daily_pnl += net_pnl

            if exit_reason == "TARGET HIT":

                trade_statistics["target_hits"] += 1
                display_reason = "TARGET HIT"
                color = Fore.BLUE

            elif exit_reason == "SL HIT":

                if pos["trail_level"] > 0:
                    trade_statistics["trailing_sl_hits"] += 1
                    if net_pnl > 0:
                        display_reason = "TSL PROFIT"
                    elif net_pnl == 0:
                        display_reason = "TSL BE"
                    else:
                        display_reason = "TSL LOSS"
                    color = Fore.MAGENTA
                else:
                    trade_statistics["sl_hits"] += 1
                    display_reason = "SL HIT"
                    color = Fore.RED

            print(color +
                f"=========>>>>     {symbol} CLOSED | {display_reason} | "
                f"{side} | Entry:{entry} | Exit:{exit_price} | "
                f"Points:{points:.2f} | NetPnL:{net_pnl:.2f}\n"
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

        #time.sleep(1)
        time.sleep(0.1)



# ==========================================
# ORB + 15 MIN STRUCTURE CACHE
# ==========================================
orb_data = {}
first15_data = {}


def build_context(token, tick):

    symbol = instrument_tokens.get(token)
    if not symbol:
        return None

    if symbol not in yesterday_data:
        return None

    if symbol in trades_taken:
        return None

    candle_time = tick.get("date", datetime.now())
    current_time = candle_time.strftime("%H:%M")

    if not (ENTRY_START_TIME <= current_time <= ENTRY_END_TIME):
        return None

    ltp = tick["last_price"]
    ohlc = tick["ohlc"]

    yest = yesterday_data[symbol]
    ema = ema_cache.get(symbol, {})

    if not ema.get("ema7") or not ema.get("ema20"):
        return None

    change_percent = ((ltp - yest["close"]) / yest["close"]) * 100
    gap_percent = ((ohlc["open"] - yest["close"]) / yest["close"]) * 100

    vol_percent = 0
    if yest["volume"] > 0:
        vol_percent = round((tick.get("volume", 0) / yest["volume"]) * 100, 2)

    return {
        "symbol": symbol,
        "tick": tick,
        "ltp": ltp,
        "current_time": current_time,
        "ohlc": ohlc,
        "yest": yest,
        "ema": ema,
        "change_percent": change_percent,
        "gap_percent": gap_percent,
        "vol_percent": vol_percent
    }

################################            Create Modular ORB Strategy
def strategy_orb(ctx):

    symbol = ctx["symbol"]
    ltp = ctx["ltp"]
    ohlc = ctx["ohlc"]
    yest = ctx["yest"]
    ema = ctx["ema"]
    current_time = ctx["current_time"]

    # Build ORB range
    if symbol not in orb_data:
        orb_data[symbol] = {"high": ohlc["high"], "low": ohlc["low"], "ready": False}

    if current_time <= "10:15":
        orb_data[symbol]["high"] = max(orb_data[symbol]["high"], ohlc["high"])
        orb_data[symbol]["low"] = min(orb_data[symbol]["low"], ohlc["low"])
        return

    orb_data[symbol]["ready"] = True

    if (
        399 <= ltp <= 3999 and
        ltp >= orb_data[symbol]["high"] and
        ltp >= ema["ema7"] and
        ltp >= ema["ema20"]
    ):
        extra = {
            "orb_high": 0,
            "orb_low": 0,
            "f_high": 0,
            "f_low": 0,
            "y_high": ctx["yest"]["high"],
            "y_low": ctx["yest"]["low"],
            "y_vol": ctx["yest"]["volume"],
            "t_vol": ctx["tick"].get("volume", 0),
            "vol_pct": ctx["vol_percent"]
        }

        place_trade(symbol, "BUY", ltp, ctx["tick"], "ORB Break",extra)

############        Create TEST Strategy (Independent)
def strategy_test(ctx):

    symbol = ctx["symbol"]
    ltp = ctx["ltp"]
    change_percent = ctx["change_percent"]

    if (
        399 <= ltp <= 5999 and
        change_percent >= 2.5
    ):
        #place_trade(symbol, "BUY", ltp, ctx["tick"], "TEST >=2%")
        extra = {
            "orb_high": 0,
            "orb_low": 0,
            "f_high": 0,
            "f_low": 0,
            "y_high": ctx["yest"]["high"],
            "y_low": ctx["yest"]["low"],
            "y_vol": ctx["yest"]["volume"],
            "t_vol": ctx["tick"].get("volume", 0),
            "vol_pct": ctx["vol_percent"]
        }

        place_trade(symbol, "BUY", ltp, ctx["tick"], "TEST >=2%", extra)


def strategy_open_low(ctx):

    symbol = ctx["symbol"]
    ltp = ctx["ltp"]
    ohlc = ctx["ohlc"]
    yest = ctx["yest"]
    ema = ctx["ema"]
    current_time = ctx["current_time"]
    vol_percent = ctx["vol_percent"]

    # -----------------------------------
    # Build First 15 Minute Structure
    # -----------------------------------
    if symbol not in first15_data:
        first15_data[symbol] = {
            "open": ohlc["open"],
            "high": ohlc["high"],
            "low": ohlc["low"],
            "close": ltp,
            "range_ready": False
        }

    if current_time <= "09:30":
        first15_data[symbol]["high"] = max(first15_data[symbol]["high"], ohlc["high"])
        first15_data[symbol]["low"] = min(first15_data[symbol]["low"], ohlc["low"])
        first15_data[symbol]["close"] = ltp
        return

    first15_data[symbol]["range_ready"] = True

    f = first15_data[symbol]

    range_percent = (
        ((f["high"] - f["low"]) / f["open"]) * 100
        if f["open"] != 0 else 999
    )

    # -----------------------------------
    # BUY: Open = Low Break
    # -----------------------------------
    if (
        399 <= ltp <= 3999 and
        f["open"] == f["low"] and
        f["close"] > f["open"] and
        range_percent < 1 and
        ltp > f["high"] and
        ltp >= ema["ema7"] and
        ltp >= ema["ema20"]
    ):
        extra = {
            "orb_high": 0,
            "orb_low": 0,
            "f_high": 0,
            "f_low": 0,
            "y_high": ctx["yest"]["high"],
            "y_low": ctx["yest"]["low"],
            "y_vol": ctx["yest"]["volume"],
            "t_vol": ctx["tick"].get("volume", 0),
            "vol_pct": ctx["vol_percent"]
        }

        place_trade(symbol, "BUY", ltp, ctx["tick"], "OPEN==LOW BREAK",extra)

    # -----------------------------------
    # SELL: Open = High Break
    # -----------------------------------
    if (
        399 <= ltp <= 3999 and
        f["open"] == f["high"] and
        f["close"] < f["open"] and
        range_percent < 1 and
        ltp < f["low"] and
        ltp <= ema["ema7"] and
        ltp <= ema["ema20"]
    ):
        extra = {
            "orb_high": 0,
            "orb_low": 0,
            "f_high": 0,
            "f_low": 0,
            "y_high": ctx["yest"]["high"],
            "y_low": ctx["yest"]["low"],
            "y_vol": ctx["yest"]["volume"],
            "t_vol": ctx["tick"].get("volume", 0),
            "vol_pct": ctx["vol_percent"]
        }

        place_trade(symbol, "SELL", ltp, ctx["tick"], "OPEN==HIGH BREAK",extra)

def strategy_yh_break(ctx):

    symbol = ctx["symbol"]
    ltp = ctx["ltp"]
    ohlc = ctx["ohlc"]
    yest = ctx["yest"]
    ema = ctx["ema"]
    change_percent = ctx["change_percent"]
    gap_percent = ctx["gap_percent"]

    yest_high = yest["high"]
    yest_low = yest["low"]

    # -----------------------------------
    # BUY: Yesterday High Break
    # -----------------------------------
    if (
        399 <= ltp <= 3999 and
        gap_percent <= 1 and            # gap protection
        ltp >= yest_high and
        1 <= change_percent <= 4 and
        ltp >= ema["ema7"] and
        ltp >= ema["ema20"]
    ):
        extra = {
            "orb_high": 0,
            "orb_low": 0,
            "f_high": 0,
            "f_low": 0,
            "y_high": ctx["yest"]["high"],
            "y_low": ctx["yest"]["low"],
            "y_vol": ctx["yest"]["volume"],
            "t_vol": ctx["tick"].get("volume", 0),
            "vol_pct": ctx["vol_percent"]
        }

        place_trade(symbol, "BUY", ltp, ctx["tick"], "YH BREAK",extra)

    # -----------------------------------
    # SELL: Yesterday Low Break
    # -----------------------------------
    if (
        399 <= ltp <= 3999 and
        gap_percent >= -1 and           # gap protection
        ltp <= yest_low and
        -4 <= change_percent <= -1 and
        ltp <= ema["ema7"] and
        ltp <= ema["ema20"]
    ):
        extra = {
            "orb_high": 0,
            "orb_low": 0,
            "f_high": 0,
            "f_low": 0,
            "y_high": ctx["yest"]["high"],
            "y_low": ctx["yest"]["low"],
            "y_vol": ctx["yest"]["volume"],
            "t_vol": ctx["tick"].get("volume", 0),
            "vol_pct": ctx["vol_percent"]
        }

        place_trade(symbol, "SELL", ltp, ctx["tick"], "YL BREAK",extra)


# ==========================================
# INSTITUTIONAL GRADE STRATEGY (DEBUG SAFE)
# ==========================================
def strategy(token, tick):

    ctx = build_context(token, tick)
    if not ctx:
        return

    strategy_test(ctx)
    strategy_orb(ctx)
    strategy_open_low(ctx)
    strategy_yh_break(ctx)



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
        f"Trailing SL Hits: {trade_statistics['trailing_sl_hits']}\n"
        f"Target Hits: {trade_statistics['target_hits']}\n"
        f"Final Live PnL: {daily_pnl}\n"
    )

    print(summary)
    write_log("===== DAILY SUMMARY =====")
    write_log(summary)
    print("\n" + "="*60)
    print(Fore.CYAN + "📊 STRATEGY WISE SUMMARY")
    print("="*60)

    for strat, data in strategy_stats.items():

        win_rate = 0
        if data["trades"] > 0:
            win_rate = round((data["wins"] / data["trades"]) * 100, 2)

        color = Fore.GREEN if data["pnl"] >= 0 else Fore.RED

        print(color +
            f"{strat}\n"
            f"  Trades: {data['trades']}\n"
            f"  Wins: {data['wins']}\n"
            f"  Loss: {data['loss']}\n"
            f"  Win%: {win_rate}%\n"
            f"  PnL: {round(data['pnl'],2)}\n"
        )

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

        time.sleep(300)  # 15 minutes

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
                f"LTP:{ltp} | SL:{pos['sl']} | "
                f"Target:{pos['target']} | "
                f"Points:{points:.2f} | "
                f"NetPnL:{net_pnl:.2f}")

        print("="*80)




##############          START PAPER ENGINE THREAD

if TRADING_MODE == "PAPER":
    threading.Thread(target=paper_position_summary, daemon=True).start()


#########################               REPLAY ENGINE BLOCK

# ==========================================
# CLEAN REPLAY ENGINE (DEBUG VERSION)
# ==========================================
def run_market_replay_proper(replay_date):

    global daily_pnl
    global trade_statistics, trades_taken, paper_positions

    # ================= RESET =================
    trade_statistics = {
        "total_trades": 0,
        "buy_trades": 0,
        "sell_trades": 0,
        "sl_hits": 0,
        "trailing_sl_hits": 0,
        "target_hits": 0
    }

    

    daily_pnl = 0
    trades_taken = {}
    paper_positions = {}
    orb_data.clear()
    first15_data.clear()

    replay_date = pd.to_datetime(replay_date).date()

    # ================= LOAD DATA =================
    load_yesterday_from_local(pd.to_datetime(replay_date))
    load_ema_data_from_local(pd.to_datetime(replay_date))

    symbol_minute_data = {}

    for symbol in SYMBOLS:
        file_path = os.path.join(MINUTE_DIR, f"{symbol}.csv")
        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)
        if df.empty:
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["date_only"] = df["date"].dt.date
        day_df = df[df["date_only"] == replay_date]

        if not day_df.empty:
            symbol_minute_data[symbol] = day_df.sort_values("date")

    if len(symbol_minute_data) == 0:
        print("❌ No data for replay date")
        return

    # Collect all timestamps
    all_times = sorted(
        set(t for df in symbol_minute_data.values() for t in df["date"])
    )

    print(f"Total timestamps found: {len(all_times)}")
    print("=" * 80)

    # ================= REPLAY LOOP =================
    for current_time in all_times:

        candle_time_str = current_time.strftime("%H:%M")

        for symbol, df in symbol_minute_data.items():

            row = df[df["date"] == current_time]
            if row.empty:
                continue

            row = row.iloc[0]

            token = next(k for k, v in instrument_tokens.items() if v == symbol)

            ltp = row["close"]
            high = row["high"]
            low = row["low"]

            latest_prices[symbol] = ltp

            # ================= EXIT CHECK FIRST =================
            if symbol in paper_positions:

                pos = paper_positions[symbol]
                if pos["status"] == "OPEN":

                    entry = pos["entry"]
                    side = pos["side"]
                    qty = pos["qty"]

                    exit_price = None
                    exit_reason = None

                    if side == "BUY":
                        if low <= pos["sl"]:
                            exit_price = pos["sl"]
                            exit_reason = "SL HIT"
                        elif high >= pos["target"]:
                            exit_price = pos["target"]
                            exit_reason = "TARGET HIT"

                    elif side == "SELL":
                        if high >= pos["sl"]:
                            exit_price = pos["sl"]
                            exit_reason = "SL HIT"
                        elif low <= pos["target"]:
                            exit_price = pos["target"]
                            exit_reason = "TARGET HIT"

                    if exit_price is not None:

                        points = (exit_price - entry) if side == "BUY" else (entry - exit_price)
                        gross = points * qty
                        net = gross - (BROKERAGE_PER_ORDER * 2)
                        strategy_name = pos.get("strategy", "Unknown")

                        strategy_stats[strategy_name]["pnl"] += net

                        if net > 0:
                            strategy_stats[strategy_name]["wins"] += 1
                        else:
                            strategy_stats[strategy_name]["loss"] += 1

                        daily_pnl += net
                        pos["status"] = "CLOSED"

                        if exit_reason == "TARGET HIT":
                            trade_statistics["target_hits"] += 1
                            color = Fore.BLUE
                        else:
                            if pos["trail_level"] > 0:
                                trade_statistics["trailing_sl_hits"] += 1
                                color = Fore.MAGENTA
                            else:
                                trade_statistics["sl_hits"] += 1
                                color = Fore.RED

                        print(color +
                              f"{candle_time_str} | {symbol} CLOSED | {exit_reason} | "
                              f"{side} | Entry:{entry} | Exit:{exit_price} | "
                              f"Points:{points:.2f} | NetPnL:{net:.2f}")

                        continue

                    # ================= TRAILING =================
                    move_percent = abs((ltp - entry) / entry) * 100

                    while move_percent >= (pos["trail_level"] + TRAIL_STEP_PERCENT):

                        pos["trail_level"] += TRAIL_STEP_PERCENT

                        if side == "BUY":
                            new_sl = round(ltp * (1 - STOP_LOSS_PERCENT/100), 2)
                            if new_sl > pos["sl"]:
                                pos["sl"] = new_sl
                        else:
                            new_sl = round(ltp * (1 + STOP_LOSS_PERCENT/100), 2)
                            if new_sl < pos["sl"]:
                                pos["sl"] = new_sl

                        print(Fore.YELLOW +
                              f"{candle_time_str} | TRAIL UPDATED | {symbol} | "
                              f"{side} | Entry:{entry} | LTP:{ltp} | "
                              f"New SL:{pos['sl']}")

            # ================= ENTRY AFTER EXIT CHECK =================
            fake_tick = {
                "instrument_token": token,
                "last_price": ltp,
                "ohlc": {
                    "open": row["open"],
                    "high": high,
                    "low": low,
                    "close": ltp
                },
                "volume": row["volume"],
                "date": current_time
            }

            strategy(token, fake_tick)

        time.sleep(0.01)

    # ================= EOD CLOSE =================
    print("\n🔔 End of Day – Closing Open Positions")

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
        gross = points * qty
        net = gross - (BROKERAGE_PER_ORDER * 2)

        strategy_name = pos.get("strategy", "Unknown")

        strategy_stats[strategy_name]["pnl"] += net

        if net > 0:
            strategy_stats[strategy_name]["wins"] += 1
        else:
            strategy_stats[strategy_name]["loss"] += 1

        daily_pnl += net
        pos["status"] = "CLOSED"

        print(Fore.MAGENTA +
              f"EOD | {symbol} | {side} | Entry:{entry} | Exit:{ltp} | NetPnL:{net:.2f}")

    daily_pnl = round(daily_pnl, 2)
    print("\n✅ Replay Finished")
    daily_summary()
    range_results[str(replay_date)] = daily_pnl



# ==========================================
# DOWNLOAD & STORE 6 MONTH HISTORICAL DATA
# ==========================================

# ==========================================
# DOWNLOAD & STORE 6 MONTH HISTORICAL DATA (FIXED)
# ==========================================
def download_6_month_data():

    print("📥 Downloading 6 Months Historical Data (Chunked 60 Days)...")

    to_date = datetime.now()
    from_date = to_date - timedelta(days=360)

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

        latest_highs[symbol] = tick["ohlc"]["high"]
        latest_lows[symbol] = tick["ohlc"]["low"]

        strategy(token, tick)
        #if symbol == "RELIANCE":
         #   print("Tick received:", tick["last_price"])


def on_connect(ws, response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    print(Fore.GREEN + "🚀 Connected to Kite WebSocket")

kws.on_ticks = on_ticks
kws.on_connect = on_connect

if TRADING_MODE == "LIVE":
    threading.Thread(target=position_manager, daemon=True).start()
    threading.Thread(target=auto_square_off, daemon=True).start()

if TRADING_MODE == "PAPER":
    threading.Thread(target=paper_position_manager, daemon=True).start()
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

        print("\nSelect Replay Mode:")
        print("1 → Single Day")
        print("2 → 1 Week")
        print("3 → 1 Month")

        choice = input("Enter choice (1/2/3): ")

        if choice == "1":

            date_input = input("Enter date (YYYY-MM-DD): ")
            replay_date = pd.to_datetime(date_input)

            run_market_replay_proper(replay_date)

        else:

            start_input = input("Start Date (YYYY-MM-DD): ")
            end_input = input("End Date (YYYY-MM-DD): ")

            start_date = pd.to_datetime(start_input)
            end_date = pd.to_datetime(end_input)

            range_results.clear()
            current = start_date

            while current <= end_date:

                # Skip weekends
                if current.weekday() < 5:

                    print("\n" + "="*80)
                    print(f"🔁 STARTING REPLAY FOR {current.date()}")
                    print("="*80)

                    # Reset daily stats
                    trades_taken.clear()
                    paper_positions.clear()
                    for key in trade_statistics:
                        trade_statistics[key] = 0

                    #global daily_pnl
                    daily_pnl = 0

                    run_market_replay_proper(current)

                    print(f"✅ COMPLETED {current.date()}")

                current += timedelta(days=1)

            #print("\n✅ RANGE REPLAY COMPLETED")
            print("\n" + "="*60)
            print("📊 RANGE SUMMARY")
            print("="*60)

            total_range_pnl = 0
            total_days = 0

            for date, pnl in range_results.items():

                total_days += 1
                total_range_pnl += pnl

                color = Fore.GREEN if pnl >= 0 else Fore.RED
                print(color + f"{date} → {pnl}")

            print("-"*60)

            final_color = Fore.GREEN if total_range_pnl >= 0 else Fore.RED
            print(final_color + f"TOTAL ({total_days} Days) → {round(total_range_pnl,2)}")

            print("="*60)
            print("\n✅ RANGE REPLAY COMPLETED")

    else:
        # Load required data for live paper mode
        today = pd.to_datetime(datetime.now().date())

        load_yesterday_from_local(today)
        load_ema_data_from_local(today)
        kws.connect()