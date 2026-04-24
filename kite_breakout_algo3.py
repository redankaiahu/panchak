import time
import threading
import csv
import os
from datetime import datetime
from kiteconnect import KiteConnect, KiteTicker
from colorama import init, Fore, Style

init(autoreset=True)

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
"HGINFRA", "HINDALCO",  "HINDUNILVR", "HINDZINC", "HOMEFIRST", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IFBIND", "PSPPROJECT", "INDHOTEL", "INDIAGLYCO",
"INDIAMART", "INDIANB", "INDIGO", "INDUSINDBK", "INFY", "INGERRAND", "INTELLECT", "IPCALAB", "IRCTC", "JBCHEPHARM", "JINDALPOLY", "JINDALSTEL", "JKCEMENT", "JKLAKSHMI",
"JSWSTEEL", "JUBLFOOD", "KAJARIACER",  "TITAGARH", "KEI", "KIRLOSBROS", "KIRLOSENG", "KIRLOSIND", "KOTAKBANK", "LICHSGFIN", "LICI", "LT", "ATGL", "LUMAXIND","LUXIND", "M&M",
"MAPMYINDIA", "MARICO", "MASTEK", "METROBRAND", "MFSL",  "KIRIINDUS", "JASH", "KIMS", "KRN", "MPHASIS", "MTARTECH", "MUTHOOTFIN",  "NAVINFLUOR", "WAAREEENER",  "NDRAUTO",
"NEOGEN", "NESCO", "NH", "MOTILALOFS", "PEL", "BRIGADE", "SYRMA",  "PFIZER", "PHOENIXLTD", "PIDILITIND", "PIIND",  "PRESTIGE", "RADICO",
"RAMCOCEM", "RATNAMANI", "RAYMOND", "RECLTD", "RELAXO", "RELIANCE", "RHIM", "ROUTE", "SBICARD", "SBILIFE", "SBIN", "SCHAEFFLER", "SHARDACROP", "SHILPAMED", "SHOPERSTOP",
"SHRIRAMFIN", "SOBHA", "SONACOMS","NEWGEN", "SRF", "SUMICHEM", "SUNDARMFIN", "SUNDRMFAST","PREMIERENE", "SUNPHARMA", "SUNTECK", "SWANENERGY", "SYMPHONY", "SYNGENE", "TATACHEM",
"TATACOMM", "TATACONSUM", "TATAMOTORS","TRITURBINE", "TCI", "TCS", "TEAMLEASE", "TECHM", "THERMAX", "THYROCARE", "TIINDIA", "TIMKEN", "TITAN", "TORNTPHARM", "TORNTPOWER",
"TTKPRESTIG", "PGEL","TVSMOTOR", "UBL", "UFLEX", "UNOMINDA", "UPL", "UTIAMC","DEEPAKFERT", "VBL", "VEDL", "VENKEYS",  "VINATIORGA",  "VMART", "VOLTAS", "WABAG", "WELCORP",
"WESTLIFE", "WHIRLPOOL", "WOCKPHARMA","OBEROIRLTY", "ZENSARTECH", "LAURUSLABS", "CDSL", "SONACOMS", "BBOX", "INTERARCH", "FINCABLES",  "CREATIVE",
"AMIORG", "SFL", "WINDLAS",  "HSCL"

]


RISK_PER_TRADE_PERCENT = 1
DAILY_MAX_LOSS_PERCENT = 5
SQUARE_OFF_TIME = "15:15"
COOLDOWN_SECONDS = 60

TRAIL_START_PERCENT = 0.5

# ==========================================
# LOGGING SYSTEM
# ==========================================
today_str = datetime.now().strftime("%d-%m-%Y")
LOG_TXT_FILE = f"execution_log_{today_str}.txt"

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
    try:
        margins = kite.margins()
        available_cash = margins["equity"]["available"]["cash"]

        risk_amount = available_cash * (RISK_PER_TRADE_PERCENT / 100)
        sl_distance = abs(entry - sl)

        if sl_distance == 0:
            return 0

        qty = int(risk_amount / sl_distance)
        max_affordable = int(available_cash / entry)

        return max(min(qty, max_affordable), 0)

    except:
        return 0

# ==========================================
# PLACE TRADE
# ==========================================
def place_trade(symbol, side, ltp, tick, condition_name):

    global order_count

    if not trading_enabled:
        return

    now = datetime.now()

    if symbol in symbol_last_trade_time:
        if (now - symbol_last_trade_time[symbol]).seconds < COOLDOWN_SECONDS:
            return

    symbol_last_trade_time[symbol] = now

    sl = round(ltp * 0.995, 2) if side == "BUY" else round(ltp * 1.005, 2)
    target = round(ltp * 1.01, 2) if side == "BUY" else round(ltp * 0.99, 2)

    qty = calculate_qty(ltp, sl)
    if qty <= 0:
        return

    transaction = kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL
    opposite = kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY

    try:
        entry_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=transaction,
            quantity=1,
            order_type=kite.ORDER_TYPE_MARKET,
            product=kite.PRODUCT_MIS
        )
    except:
        return

    order_count += 1
    trade_statistics["total_trades"] += 1
    if side == "BUY":
        trade_statistics["buy_trades"] += 1
    else:
        trade_statistics["sell_trades"] += 1

    color = Fore.GREEN if side == "BUY" else Fore.RED

    print(color + "-> " * 20)
    msg = (f"{side} {condition_name} | Symbol: {symbol} | Entry: {ltp} | "
           f"SL: {sl} | Target: {target} | Qty: {qty} | OrderID: {entry_id}")
    print(color + msg)
    write_log(msg)

    print(Fore.CYAN +
          f"Day O:{tick['ohlc']['open']} "
          f"H:{tick['ohlc']['high']} "
          f"L:{tick['ohlc']['low']} "
          f"LTP:{ltp} "
          f"Vol:{tick.get('volume',0)}")

    try:
        sl_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=1,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=sl,
            product=kite.PRODUCT_MIS
        )

        tgt_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=1,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=target,
            product=kite.PRODUCT_MIS
        )

    except:
        return

    trades_taken[symbol] = {"sl": sl_id, "target": tgt_id}

    print("<- " * 10 + f" ORDER COUNT: {order_count} " + "<- " * 10)

    threading.Thread(target=oco_monitor, args=(symbol,), daemon=True).start()

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

def load_yesterday_levels():
    print("Loading Yesterday OHLC levels...")
    for token, symbol in instrument_tokens.items():
        try:
            to_date = datetime.now()
            from_date = to_date.replace(day=to_date.day-5)

            data = kite.historical_data(
                token,
                from_date,
                to_date,
                "day"
            )

            if len(data) >= 2:
                yest = data[-2]
                yesterday_data[symbol] = {
                    "high": yest["high"],
                    "low": yest["low"],
                    "close": yest["close"]
                }

                live_day_extremes[symbol] = {
                    "high": 0,
                    "low": 999999
                }

        except:
            pass

load_yesterday_levels()



# ==========================================
# ADVANCED YH / YL STRATEGY
# ==========================================
def strategy(token, tick):

    symbol = instrument_tokens[token]

    if symbol not in yesterday_data:
        return

    ltp = tick["last_price"]
    live_open = tick["ohlc"]["open"]
    live_high = tick["ohlc"]["high"]
    live_low = tick["ohlc"]["low"]

    yest_high = yesterday_data[symbol]["high"]
    yest_low = yesterday_data[symbol]["low"]
    yest_close = yesterday_data[symbol]["close"]

    # Update live extremes
    live_day_extremes[symbol]["high"] = max(
        live_day_extremes[symbol]["high"], ltp
    )
    live_day_extremes[symbol]["low"] = min(
        live_day_extremes[symbol]["low"], ltp
    )

    day_high = live_day_extremes[symbol]["high"]
    day_low = live_day_extremes[symbol]["low"]

    # Change %
    change_percent = ((ltp - yest_close) / yest_close) * 100

    # Distance from day high %
    dist_from_day_high = ((day_high - ltp) / day_high) * 100 if day_high != 0 else 0
    dist_from_day_low = ((ltp - day_low) / day_low) * 100 if day_low != 0 else 0

    # ================= BUY CONDITION =================
    if (
        live_open <= yest_high and
        ltp >= yest_high and
        live_high >= yest_high and
        dist_from_day_high <= 0.5 and
        0.5 <= change_percent <= 2.5
    ):
        place_trade(symbol, "BUY", ltp, tick, "Strong YH Break")

    # ================= SELL CONDITION =================
    if (
        live_open >= yest_low and
        ltp <= yest_low and
        dist_from_day_low <= 0.5 and
        -2.2 <= change_percent <= -0.5
    ):
        place_trade(symbol, "SELL", ltp, tick, "Strong YL Break")



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
            except:
                pass

            daily_summary()
            break

        time.sleep(20)

# ==========================================
# WEBSOCKET
# ==========================================
def on_ticks(ws, ticks):
    for tick in ticks:
        strategy(tick["instrument_token"], tick)

def on_connect(ws, response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    print(Fore.GREEN + "🚀 Connected to Kite WebSocket")

kws.on_ticks = on_ticks
kws.on_connect = on_connect

threading.Thread(target=position_manager, daemon=True).start()
threading.Thread(target=auto_square_off, daemon=True).start()

if __name__ == "__main__":
    print(Fore.CYAN + "🔥 Execution Engine Started")
    kws.connect()