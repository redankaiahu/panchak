import time
import threading
import csv
import os
from datetime import datetime
from kiteconnect import KiteConnect, KiteTicker
from colorama import init, Fore, Style
init(autoreset=True)


#############       Add Logging System

# ==========================================
# ADVANCED LOG SYSTEM
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

def write_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] {message}"
    
    with open(LOG_TXT_FILE, "a", encoding="utf-8") as f:
        f.write(formatted + "\n")



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
TRAIL_STEP_PERCENT = 0.3

LOG_FILE = "trade_log.csv"

order_count = 0


# ==========================================
# INITIALIZATION
# ==========================================
kite = KiteConnect(api_key=API_KEY)
ACCESS_TOKEN = open(ACCESS_TOKEN_FILE).read().strip()
kite.set_access_token(ACCESS_TOKEN)

kws = KiteTicker(API_KEY, ACCESS_TOKEN)

instrument_tokens = {}
token_symbol_map = {}

trades_taken = {}
symbol_last_trade_time = {}

daily_pnl = 0
trading_enabled = True

# ==========================================
# LOAD INSTRUMENTS
# ==========================================
instruments = kite.instruments("NSE")
for inst in instruments:
    if inst["tradingsymbol"] in SYMBOLS:
        instrument_tokens[inst["instrument_token"]] = inst["tradingsymbol"]
        token_symbol_map[inst["tradingsymbol"]] = inst["instrument_token"]

tokens = list(instrument_tokens.keys())

# ==========================================
# CALCULATE QTY USING REAL MARGIN
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

        qty = min(qty, max_affordable)

        return max(qty, 0)

    except Exception as e:
        print("Margin error:", e)
        return 0

# ==========================================
# LOGGING
# ==========================================
def log_trade(data):
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

# ==========================================
# RISK CHECK
# ==========================================
def risk_check():
    global trading_enabled
    try:
        margins = kite.margins()
        starting_cash = margins["equity"]["available"]["cash"]
        max_loss = starting_cash * DAILY_MAX_LOSS_PERCENT / 100

        if daily_pnl <= -max_loss:
            trading_enabled = False
            print("🚨 Daily Max Loss Hit. Trading Disabled.")
    except:
        pass

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
                print(f"\n============>>>>>>>>>>>>  {symbol} Stop Loss executed. "
                      f"Canceling Target order: {tgt_id} - TIME:[{datetime.now()}]")
                kite.cancel_order(variety="regular", order_id=tgt_id)
                break

            if tgt_order["status"] == "COMPLETE":
                print(f"\n============>>>>>>>>>>>>  {symbol} Target executed. "
                      f"Canceling Stop Loss order: {sl_id} - TIME:[{datetime.now()}]")
                kite.cancel_order(variety="regular", order_id=sl_id)
                break

            time.sleep(2)

        except:
            break

# ==========================================
# PLACE TRADE
# ==========================================
def place_trade(symbol, side, ltp, tick=None, condition_name="Condition 1"):

    global trading_enabled, order_count

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
        print(f"{symbol} skipped - insufficient margin")
        return

    transaction = kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL
    opposite = kite.TRANSACTION_TYPE_SELL if side == "BUY" else kite.TRANSACTION_TYPE_BUY

    # ================= ENTRY =================
    try:
        entry_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=transaction,
            quantity=qty,
            order_type=kite.ORDER_TYPE_MARKET,
            product=kite.PRODUCT_MIS
        )
    except Exception as e:
        print("Entry order failed:", e)
        return

    order_count += 1

    print("-> " * 20)
    print(f" - {side} {condition_name} - Order placed for {symbol}: {transaction} at {ltp} "
          f"- origOrderId: {entry_id} at Time: [{now.strftime('%Y-%m-%d %H:%M:%S')}]")

    if tick:
        print(f" day's Open: {tick['ohlc']['open']}, "
              f"High: {tick['ohlc']['high']}, "
              f"Low: {tick['ohlc']['low']}, "
              f"LTP: {ltp}, "
              f"Volume: {tick.get('volume', 0)}")

    # ================= SL =================
    try:
        sl_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=qty,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=sl,
            product=kite.PRODUCT_MIS
        )
        print(f"Stop Loss order placed: {sl_id}")
    except Exception as e:
        print("SL order failed:", e)
        return

    # ================= TARGET =================
    try:
        tgt_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=opposite,
            quantity=qty,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=target,
            product=kite.PRODUCT_MIS
        )
        print(f"Target order placed: {tgt_id}")
    except Exception as e:
        print("Target order failed:", e)
        return

    print("<- " * 10 + f"   ###################     order_count: {order_count}      " + "<- " * 10)

    trades_taken[symbol] = {
        "sl": sl_id,
        "target": tgt_id
    }

    threading.Thread(target=oco_monitor, args=(symbol,), daemon=True).start()

# ==========================================
# SIMPLE BREAKOUT STRATEGY
# ==========================================
def strategy(token, tick):
    try:
        symbol = instrument_tokens[token]
        ltp = tick["last_price"]
        high = tick["ohlc"]["high"]
        low = tick["ohlc"]["low"]

        if ltp >= high * 0.999:
            place_trade(symbol, "BUY", ltp, tick, "Breakout High")

        if ltp <= low * 1.001:
            place_trade(symbol, "SELL", ltp, tick, "Breakdown Low")

    except:
        pass

# ==========================================
# POSITION MONITOR
# ==========================================
def position_manager():
    global daily_pnl
    while True:
        try:
            positions = kite.positions()["net"]
            daily_pnl = sum([p["pnl"] for p in positions if p["quantity"] != 0])
            print("Live PnL:", daily_pnl)
            risk_check()
        except:
            pass
        time.sleep(300)

# ==========================================
# TRAILING SL
# ==========================================
def trailing_sl_manager():
    while True:
        try:
            positions = kite.positions()["net"]

            for pos in positions:
                if pos["quantity"] == 0:
                    continue

                symbol = pos["tradingsymbol"]
                entry_price = pos["average_price"]
                ltp = pos["last_price"]

                if symbol not in trades_taken:
                    continue

                sl_id = trades_taken[symbol]["sl"]

                orders = kite.orders()
                sl_order = next((o for o in orders if o["order_id"] == sl_id), None)

                if not sl_order or sl_order["status"] != "TRIGGER PENDING":
                    continue

                if pos["quantity"] > 0:
                    move_percent = ((ltp - entry_price) / entry_price) * 100
                    if move_percent >= TRAIL_START_PERCENT:
                        new_sl = round(ltp * 0.995, 2)
                        if new_sl > float(sl_order["trigger_price"]):
                            kite.modify_order(
                                variety="regular",
                                order_id=sl_id,
                                trigger_price=new_sl
                            )
                            #print("Trailing SL updated", symbol)
                            print(f"======>> SL order modified successfully for {symbol} "
                                f"Order {sl_id}: New SL: {new_sl}, LTP: {ltp}, "
                                f"TIME:[{datetime.now()}]")

                if pos["quantity"] < 0:
                    move_percent = ((entry_price - ltp) / entry_price) * 100
                    if move_percent >= TRAIL_START_PERCENT:
                        new_sl = round(ltp * 1.005, 2)
                        if new_sl < float(sl_order["trigger_price"]):
                            kite.modify_order(
                                variety="regular",
                                order_id=sl_id,
                                trigger_price=new_sl
                            )
                            print("Trailing SL updated", symbol)

        except:
            pass

        time.sleep(5)

# ==========================================
# AUTO SQUARE OFF
# ==========================================
def auto_square_off():
    while True:
        if datetime.now().strftime("%H:%M") >= SQUARE_OFF_TIME:
            print("Auto Square-Off")
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
            break
        time.sleep(20)
        daily_summary()


#############################           Add End Of Day Summary

# ==========================================
# DAILY SUMMARY REPORT
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
    write_log("========== DAILY SUMMARY ==========")
    write_log(summary)



# ==========================================
# WEBSOCKET
# ==========================================
def on_ticks(ws, ticks):
    for tick in ticks:
        strategy(tick["instrument_token"], tick)

def on_connect(ws, response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    print("🚀 Connected")

kws.on_ticks = on_ticks
kws.on_connect = on_connect

threading.Thread(target=position_manager, daemon=True).start()
threading.Thread(target=auto_square_off, daemon=True).start()
threading.Thread(target=trailing_sl_manager, daemon=True).start()

if __name__ == "__main__":
    print("🔥 Execution Engine Started")
    kws.connect()