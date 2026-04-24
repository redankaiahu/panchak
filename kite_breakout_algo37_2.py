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
# TIMESTAMP HELPER
# ==========================================
def ts():
    """Return current date + time string for console output."""
    return datetime.now().strftime("  |  %Y-%m-%d  %H:%M:%S")


# ==========================================
# TRADING MODE
# ==========================================
TRADING_MODE = "PAPER"   # "LIVE" or "PAPER"

FIXED_QTY = 10               # ← 1 qty for live test (change to 10 after validation)
LIVE_MAX_TRADES = 13         # ← max simultaneous live trades (safety cap for testing)
STOP_LOSS_PERCENT = 0.75        # 0.75% SL → 1:2 ratio with 1.5% target
TARGET_PERCENT = 1.5         # 1.5% target → 2x the SL distance
TRAIL_STEP_PERCENT = 0.75       # trail locks in profit every 0.75% move
BROKERAGE_PER_ORDER = 10     # approx MIS brokerage

# Maximum % a stock can have already moved PAST yesterday's high/low before entry.
# If stock is already >1.5% below YL → oversold, reversal risk high → skip SELL.
# If stock is already >1.5% above YH → overbought, reversal risk high → skip BUY.
# RELIANCE case: was 2.5% below YL → would have been blocked by this filter.
MAX_OVEREXTENSION_PCT = 1.5

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
minute_candles = {}

# ==========================================
# PAPER TRADE STORAGE
# ==========================================
today_str = datetime.now().strftime("%d-%m-%Y")
LOG_TXT_FILE = f"execution_log_{today_str}.txt"

paper_positions = {}
paper_trade_log_file = f"paper_trades_{today_str}.csv"

# ==========================================
# TRADES CACHE  (persist trades_taken across restarts)
# ==========================================
# File: trades_cache_DD-MM-YYYY.csv
# Named by date so yesterday's file is never loaded.
# Saved every time a new symbol is locked (entry attempted).
# Loaded at startup — so if you restart mid-session, already-traded
# symbols are immediately blocked and won't re-enter.
# ==========================================
TRADES_CACHE_FILE   = f"trades_cache_{today_str}.csv"
POSITIONS_CACHE_FILE = f"positions_cache_{today_str}.csv"   # paper positions + daily_pnl

def save_trades_cache():
    """Write all locked symbols to today's trades cache CSV."""
    try:
        rows = []
        for symbol, value in trades_taken.items():
            if isinstance(value, dict):
                status = "LIVE"
                entry_oid  = value.get("entry", "")
                sl_oid     = value.get("sl", "")
                target_oid = value.get("target", "")
                side       = value.get("side", "")
            elif value == "PENDING":
                status = "PENDING"
                entry_oid = sl_oid = target_oid = side = ""
            else:
                status = "PAPER"
                entry_oid = sl_oid = target_oid = side = ""
            rows.append({
                "symbol":     symbol,
                "status":     status,
                "side":       side,
                "entry_oid":  entry_oid,
                "sl_oid":     sl_oid,
                "target_oid": target_oid,
            })
        pd.DataFrame(rows).to_csv(TRADES_CACHE_FILE, index=False)
    except Exception as e:
        print(Fore.YELLOW + f"⚠️  Could not save trades cache: {e}" + ts())


def save_paper_positions_cache():
    """Persist all paper_positions + current daily_pnl so restarts can resume."""
    try:
        rows = []
        for symbol, pos in paper_positions.items():
            # Use stored net_pnl for closed positions (exact, not recalculated from live price)
            saved_pnl = round(float(pos.get("net_pnl", 0)), 2)
            rows.append({
                "symbol":      symbol,
                "side":        pos["side"],
                "entry":       pos["entry"],
                "sl":          pos["sl"],
                "target":      pos["target"],
                "qty":         pos["qty"],
                "trail_level": pos["trail_level"],
                "status":      pos["status"],
                "strategy":    pos["strategy"],
                "entry_time":  pos["entry_time"],
                "net_pnl":     saved_pnl,
            })
        df = pd.DataFrame(rows)
        # Store daily_pnl (closed PnL) in a separate header row using a comment column
        df["daily_pnl_snapshot"] = daily_pnl   # same value on every row — restored on load
        df.to_csv(POSITIONS_CACHE_FILE, index=False)
    except Exception as e:
        print(Fore.YELLOW + f"⚠️  Could not save positions cache: {e}" + ts())


def load_paper_positions_cache():
    """
    Reload paper_positions and daily_pnl from today's cache on restart.
    For each OPEN position:
      • If live price available → verify SL/target not already hit → keep OPEN
      • If price not yet available → keep OPEN (WebSocket will catch up)
    For CLOSED positions → restore into paper_positions so daily_pnl is intact.
    """
    global daily_pnl

    if not os.path.exists(POSITIONS_CACHE_FILE):
        return

    try:
        df = pd.read_csv(POSITIONS_CACHE_FILE)
        if df.empty:
            return

        # Restore daily_pnl from the snapshot column (all rows have the same value)
        daily_pnl = float(df["daily_pnl_snapshot"].iloc[-1])

        # Restore trade counts
        closed_df = df[df["status"] == "CLOSED"]
        trade_statistics["total_trades"] = len(closed_df)
        trade_statistics["buy_trades"]   = len(closed_df[closed_df["side"] == "BUY"])
        trade_statistics["sell_trades"]  = len(closed_df[closed_df["side"] == "SELL"])

        # Rebuild strategy_stats from closed rows
        # net_pnl column may be missing from older cache files — handle gracefully
        for _, row in closed_df.iterrows():
            sname   = str(row.get("strategy", "Unknown"))
            # net_pnl column present in new cache; absent in old — use 0 as placeholder
            pnl_val = float(row["net_pnl"]) if "net_pnl" in row and pd.notna(row["net_pnl"]) else 0.0
            if sname not in strategy_stats:
                strategy_stats[sname] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}
            strategy_stats[sname]["trades"] += 1
            strategy_stats[sname]["pnl"]    += pnl_val
            if pnl_val > 0:   strategy_stats[sname]["wins"] += 1
            elif pnl_val < 0: strategy_stats[sname]["loss"] += 1
            # pnl_val == 0 means old cache with no net_pnl — counted as trade but not win/loss

        restored_open   = 0
        restored_closed = 0

        for _, row in df.iterrows():
            symbol = row["symbol"]
            status = row["status"]

            pos = {
                "side":        row["side"],
                "entry":       float(row["entry"]),
                "sl":          float(row["sl"]),
                "target":      float(row["target"]),
                "qty":         int(row["qty"]),
                "trail_level": float(row["trail_level"]),
                "status":      status,
                "strategy":    row["strategy"],
                "entry_time":  row["entry_time"],
            }
            paper_positions[symbol] = pos

            if status == "OPEN":
                # Block re-entry for this symbol
                trades_taken[symbol] = True
                restored_open += 1
            else:
                # CLOSED — also block re-entry. One trade per symbol per day,
                # regardless of whether position is open or already closed.
                trades_taken[symbol] = True
                restored_closed += 1

        print(Fore.CYAN +
            f"♻️  Positions restored — {restored_open} OPEN, {restored_closed} CLOSED "
            f"| Closed PnL so far: {daily_pnl:.2f}" + ts())

        if restored_open > 0:
            print(Fore.YELLOW +
                "⏳ Open positions will be re-checked against live prices once WebSocket connects..." + ts())

    except Exception as e:
        print(Fore.YELLOW + f"⚠️  Could not load positions cache: {e}" + ts())


def verify_restored_positions():
    """
    Called once after WebSocket connects and prices start flowing.
    For each restored OPEN position, check if SL or target was already
    hit while the program was offline. If yes → close it at current price
    with reason 'OFFLINE HIT' and update daily_pnl.
    Also handles LIVE mode — queries kite.positions() to cross-check.
    """
    global daily_pnl

    if TRADING_MODE == "LIVE":
        # ── LIVE: cross-check with actual broker positions ───────────────
        try:
            broker_positions = kite.positions()["net"]
            broker_open = {
                p["tradingsymbol"]: p for p in broker_positions
                if p.get("exchange") == "NSE" and p["quantity"] != 0
            }
            for symbol, trade in list(trades_taken.items()):
                if not isinstance(trade, dict):
                    continue
                if symbol not in broker_open:
                    # Position no longer open at broker → already closed offline
                    if symbol in paper_positions:
                        paper_positions[symbol]["status"] = "CLOSED"
                    trades_taken[symbol] = "CLOSED"
                    print(Fore.MAGENTA +
                        f"🔄 {symbol}: position no longer at broker — marked CLOSED" + ts())
                else:
                    print(Fore.GREEN +
                        f"✅ {symbol}: confirmed OPEN at broker qty={broker_open[symbol]['quantity']}" + ts())
        except Exception as e:
            print(Fore.YELLOW + f"⚠️  Live position verify failed: {e}" + ts())

    else:
        # ── PAPER: check SL/target against current live prices ───────────
        hit_count = 0
        for symbol, pos in list(paper_positions.items()):
            if pos["status"] != "OPEN":
                continue
            ltp = latest_prices.get(symbol)
            if not ltp:
                continue   # price not yet received, monitor will handle it

            entry = pos["entry"]
            side  = pos["side"]
            qty   = pos["qty"]

            # Check if offline price moved past SL or target
            if side == "BUY":
                if ltp <= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT (offline)"
                elif ltp >= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT (offline)"
                else:
                    continue
            else:  # SELL
                if ltp >= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT (offline)"
                elif ltp <= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT (offline)"
                else:
                    continue

            points  = (exit_price - entry) if side == "BUY" else (entry - exit_price)
            net_pnl = points * qty - (BROKERAGE_PER_ORDER * 2)
            daily_pnl += net_pnl
            pos["status"] = "CLOSED"
            hit_count += 1

            print(Fore.MAGENTA +
                f"🔄 {symbol} {exit_reason} | {side} | Entry:{entry} → Exit:{exit_price} "
                f"| NetPnL:{net_pnl:.2f}" + ts())
            write_log(f"OFFLINE CLOSE {symbol} {exit_reason} {side} Entry:{entry} Exit:{exit_price} NetPnL:{net_pnl:.2f}")

            with open(paper_trade_log_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([datetime.now(), symbol, side, entry, exit_price, qty, net_pnl, exit_reason])

        save_paper_positions_cache()

        if hit_count:
            print(Fore.CYAN + f"♻️  {hit_count} position(s) auto-closed after offline SL/target check" + ts())
        else:
            print(Fore.GREEN + "✅ All restored OPEN positions still valid (SL/target not hit)" + ts())


def load_live_positions_cache():
    """
    LIVE mode only — called at startup to reload trades_taken from
    today's trades_cache file and restart OCO monitors.
    This is a wrapper that also triggers broker verification once
    the WebSocket is connected (via verify_restored_positions).
    """
    load_trades_cache()   # already exists — restores trades_taken + starts OCO monitors


def load_trades_cache():
    """
    Load today's trades cache at startup.
    Restores trades_taken so already-traded symbols are immediately blocked.
    Also restarts OCO monitors for any LIVE positions that were active.
    """
    if not os.path.exists(TRADES_CACHE_FILE):
        return

    try:
        df = pd.read_csv(TRADES_CACHE_FILE)
        if df.empty:
            return

        restored = 0
        for _, row in df.iterrows():
            symbol = row["symbol"]
            status = row["status"]

            if status == "LIVE":
                # Restore full order dict and restart OCO monitor
                trades_taken[symbol] = {
                    "entry":  str(row["entry_oid"]),
                    "sl":     str(row["sl_oid"]),
                    "target": str(row["target_oid"]),
                    "side":   str(row["side"]),
                    "qty":    FIXED_QTY
                }
                # Restart OCO monitor thread so exits are still watched
                threading.Thread(
                    target=oco_monitor, args=(symbol,), daemon=True
                ).start()
                print(Fore.YELLOW + f"♻️  Restored LIVE position: {symbol} — OCO monitor restarted" + ts())
            else:
                # PENDING or PAPER — just block re-entry
                trades_taken[symbol] = status

            restored += 1

        print(Fore.CYAN + f"📂 Trades cache loaded — {restored} symbols blocked from re-entry" + ts())

    except Exception as e:
        print(Fore.YELLOW + f"⚠️  Could not load trades cache: {e}" + ts())

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
    "BANKINDIA","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULI","IEX","INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND","IRCTC","IRFC","IREDA","ITC",
    "JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
    "MPHASIS","MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NTPC","NUVAMA","NYKAA","NATIONALUM",
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

WARMUP_SECONDS  = 60
ws_connect_time = None
ws_ready        = False

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
tick_sizes = {}          # symbol → tick_size (e.g. 0.05 or 0.10)
trades_taken = {}
symbol_last_trade_time = {}

# ==========================================
# LOAD INSTRUMENTS
# ==========================================
instruments = kite.instruments("NSE")
for inst in instruments:
    if inst["tradingsymbol"] in SYMBOLS:
        instrument_tokens[inst["instrument_token"]] = inst["tradingsymbol"]
        tick_sizes[inst["tradingsymbol"]] = inst.get("tick_size", 0.05)

tokens = list(instrument_tokens.keys())

# ==========================================
# QTY CALCULATION
# ==========================================
def calculate_qty(entry, sl):
    return 1

# ── Day-change % helper ──────────────────────────────────────────────────────
def _chg(price, y_close):
    """Returns '(+1.23%)' or '(-0.45%)' — price vs yesterday close."""
    if not y_close or y_close == 0:
        return ""
    pct  = (price - y_close) / y_close * 100
    sign = "+" if pct >= 0 else ""
    return f"({sign}{pct:.2f}%)"

# ==========================================
# PLACE TRADE
# ==========================================
# ==========================================
# PLACE TRADE
# ==========================================
#def place_trade(symbol, side, ltp, tick, condition_name):
def place_trade(symbol, side, ltp, tick, condition_name, extra_info=None):

    global order_count

    # ── Guards ──────────────────────────────────────────────────────────
    if not trading_enabled:
        return

    # One trade per symbol per day — once a symbol enters trades_taken
    # (as "PENDING", True, or a dict), it is NEVER re-entered that session.
    # This covers: successful trades, failed entries, and emergency exits.
    if symbol in trades_taken:
        return

    # LIVE mode: cap simultaneous open trades
    if TRADING_MODE == "LIVE":
        # Count both confirmed open positions AND pending (entry placed, SL failed)
        open_count = sum(
            1 for p in paper_positions.values() if p["status"] == "OPEN"
        )
        pending_count = sum(
            1 for v in trades_taken.values() if v == "PENDING"
        )
        if (open_count + pending_count) >= LIVE_MAX_TRADES:
            print(Fore.YELLOW + f"⚠️  Max live trades ({LIVE_MAX_TRADES}) reached — skipping {symbol}" + ts())
            return

    entry_time = tick.get("date", datetime.now()).strftime("%H:%M")
    qty = FIXED_QTY

    # ── Tick-size aware rounding ─────────────────────────────────────────
    # Kite rejects SL/target orders if price is not a multiple of tick_size.
    # We round UP for SL on SELL (higher = safer) and DOWN for SL on BUY,
    # and snap target to the nearest valid tick as well.
    tick_size = tick_sizes.get(symbol, 0.05)

    def round_to_tick(price, tick):
        """Round price to nearest tick_size multiple."""
        return round(round(price / tick) * tick, 10)

    raw_sl = (
        ltp * (1 - STOP_LOSS_PERCENT / 100) if side == "BUY"
        else ltp * (1 + STOP_LOSS_PERCENT / 100)
    )
    raw_target = (
        ltp * (1 + TARGET_PERCENT / 100) if side == "BUY"
        else ltp * (1 - TARGET_PERCENT / 100)
    )

    sl     = round_to_tick(raw_sl,     tick_size)
    target = round_to_tick(raw_target, tick_size)

    # ── SL sanity check BEFORE placing any order ────────────────────────
    if side == "BUY" and sl >= ltp:
        print(Fore.RED + f"⚠️  Invalid SL for BUY {symbol}: SL={sl} >= LTP={ltp}" + ts())
        return
    if side == "SELL" and sl <= ltp:
        print(Fore.RED + f"⚠️  Invalid SL for SELL {symbol}: SL={sl} <= LTP={ltp}" + ts())
        return

    color = Fore.GREEN if side == "BUY" else Fore.RED

    structure_text = ""
    if extra_info:
        structure_text = (
            f"\n   ORB H/L: {extra_info['orb_high']} / {extra_info['orb_low']}"
            f"\n   F15 H/L: {extra_info['f_high']} / {extra_info['f_low']}"
            f"\n   C2 H/L:  {extra_info['c2_high']} / {extra_info['c2_low']}"
            f"\n   C3 H/L:  {extra_info['c3_high']} / {extra_info['c3_low']}"
            f"\n   C4 H/L:  {extra_info['c4_high']} / {extra_info['c4_low']}"
            f"\n   Today O/H/L: {extra_info['t_open']} / {extra_info['t_high']} / {extra_info['t_low']}"
            f"\n   YH/YL/YClose: {extra_info['y_high']} / {extra_info['y_low']} / {extra_info['y_close']}"
            f"\n   YVol: {extra_info['y_vol']} | CumVol: {extra_info['t_vol']} | Vol%: {extra_info['vol_pct']}%"
        )

    y_close_entry = extra_info.get("y_close", 0) if extra_info else 0

    print(color +
        f"{entry_time} | {condition_name} | {side} | {symbol} | "
        f"Entry:{ltp} SL:{sl} Target:{target} Qty:{qty} | "
        f"LTP:{ltp} {_chg(ltp, y_close_entry)}"
        f"{structure_text}\n"
     + ts())

    write_log(
        f"{TRADING_MODE} {side} {symbol} "
        f"Entry:{ltp} SL:{sl} Target:{target}\n"
    )

    # ── Statistics ──────────────────────────────────────────────────────
    order_count += 1
    trade_statistics["total_trades"] += 1
    if side == "BUY":
        trade_statistics["buy_trades"] += 1
    else:
        trade_statistics["sell_trades"] += 1

    if condition_name not in strategy_stats:
        strategy_stats[condition_name] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}
    strategy_stats[condition_name]["trades"] += 1

    # ── LIVE Order Execution ─────────────────────────────────────────────
    if TRADING_MODE == "LIVE":

        # 🔒 LOCK the symbol IMMEDIATELY — before any API call.
        # This is the critical fix: even if SL/target placement fails,
        # the symbol is blocked from re-entering on the next tick.
        trades_taken[symbol] = "PENDING"
        save_trades_cache()   # persist immediately so restart won't re-enter

        entry_order_id = None
        sl_order_id    = None
        target_order_id = None

        try:
            transaction = (
                kite.TRANSACTION_TYPE_BUY if side == "BUY"
                else kite.TRANSACTION_TYPE_SELL
            )
            exit_side = (
                kite.TRANSACTION_TYPE_SELL if side == "BUY"
                else kite.TRANSACTION_TYPE_BUY
            )

            # 1️⃣ Entry Market Order
            entry_order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=transaction,
                quantity=qty,
                order_type=kite.ORDER_TYPE_MARKET,
                product=kite.PRODUCT_MIS
            )
            print(Fore.YELLOW + f"✅ Entry order placed | ID:{entry_order_id}" + ts())

            # Small delay to let entry fill before placing SL/target
            time.sleep(0.5)

            # 2️⃣ Stop Loss Order (SL-M)
            sl_order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=exit_side,
                quantity=qty,
                order_type=kite.ORDER_TYPE_SLM,
                trigger_price=sl,
                product=kite.PRODUCT_MIS
            )
            print(Fore.YELLOW + f"✅ SL order placed | ID:{sl_order_id} | SL:{sl}" + ts())

            # 3️⃣ Target Limit Order
            target_order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=exit_side,
                quantity=qty,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=target,
                product=kite.PRODUCT_MIS
            )
            print(Fore.YELLOW + f"✅ Target order placed | ID:{target_order_id} | Target:{target}" + ts())

            # All 3 orders succeeded — store full order dict
            trades_taken[symbol] = {
                "entry":  entry_order_id,
                "sl":     sl_order_id,
                "target": target_order_id,
                "side":   side,
                "qty":    qty
            }
            save_trades_cache()   # update cache with full order IDs

            threading.Thread(target=oco_monitor, args=(symbol,), daemon=True).start()

        except Exception as e:
            print(Fore.RED + f"❌ Live order failed for {symbol}: {e}" + ts())
            write_log(f"LIVE ORDER FAILED {symbol}: {e}")

            # ── Partial failure recovery ─────────────────────────────────
            # Entry filled but SL/target failed → we have an unprotected
            # live position. Cancel what we can and market-exit immediately.
            if entry_order_id and (sl_order_id is None or target_order_id is None):
                print(Fore.RED + f"⚠️  PARTIAL ORDER — emergency exit for {symbol}" + ts())
                write_log(f"PARTIAL ORDER EMERGENCY EXIT {symbol}")

                # Cancel whichever of SL/target did get placed
                for oid in [sl_order_id, target_order_id]:
                    if oid:
                        try:
                            kite.cancel_order(variety="regular", order_id=oid)
                        except Exception:
                            pass

                # Market exit to flatten the position
                try:
                    emergency_side = (
                        kite.TRANSACTION_TYPE_SELL if side == "BUY"
                        else kite.TRANSACTION_TYPE_BUY
                    )
                    kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange=kite.EXCHANGE_NSE,
                        tradingsymbol=symbol,
                        transaction_type=emergency_side,
                        quantity=qty,
                        order_type=kite.ORDER_TYPE_MARKET,
                        product=kite.PRODUCT_MIS
                    )
                    print(Fore.RED + f"🚨 Emergency exit placed for {symbol}" + ts())
                    write_log(f"EMERGENCY EXIT PLACED {symbol}")
                except Exception as ex:
                    print(Fore.RED + f"🚨 EMERGENCY EXIT ALSO FAILED for {symbol}: {ex}" + ts())
                    write_log(f"EMERGENCY EXIT FAILED {symbol}: {ex}")

            # Symbol stays in trades_taken as "PENDING" — blocks re-entry
            # regardless of whether recovery succeeded or not.
            return

    # ── Store Position (PAPER tracking + LIVE local tracking) ────────────
    paper_positions[symbol] = {
        "side":        side,
        "entry":       ltp,
        "sl":          sl,
        "target":      target,
        "qty":         qty,
        "trail_level": 0,
        "status":      "OPEN",
        "strategy":    condition_name,
        "entry_time":  entry_time,
        "y_close":     extra_info.get("y_close", 0) if extra_info else 0
    }

    # For PAPER mode, trades_taken is just a flag
    if TRADING_MODE == "PAPER":
        trades_taken[symbol] = True
        save_paper_positions_cache()   # persist immediately on every new trade



# ==========================================
# OCO MONITOR  (LIVE MODE)
# ==========================================
def oco_monitor(symbol):
    """
    Watches SL and Target orders for a live position.
    When one fills, cancels the other and marks the position closed.
    Retries on transient network errors — does NOT silently die.
    """
    consecutive_errors = 0

    while True:
        try:
            # trades_taken[symbol] may be True (paper) or a dict (live)
            trade = trades_taken.get(symbol)
            if not isinstance(trade, dict):
                break   # position already closed / paper mode

            orders = kite.orders()
            sl_id  = trade["sl"]
            tgt_id = trade["target"]

            sl_order  = next((o for o in orders if o["order_id"] == sl_id),  None)
            tgt_order = next((o for o in orders if o["order_id"] == tgt_id), None)

            if sl_order is None or tgt_order is None:
                # Orders vanished — unusual, log and exit
                print(Fore.RED + f"⚠️  OCO: orders not found for {symbol}, exiting monitor" + ts())
                write_log(f"OCO orders not found for {symbol}")
                break

            # ── SL Hit ──────────────────────────────────────────────────
            if sl_order["status"] == "COMPLETE":
                trade_statistics["sl_hits"] += 1
                try:
                    kite.cancel_order(variety="regular", order_id=tgt_id)
                except Exception as ce:
                    print(Fore.YELLOW + f"⚠️  Could not cancel target for {symbol}: {ce}" + ts())
                msg = f"🔴 {symbol} STOP LOSS HIT | Target order cancelled"
                print(Fore.YELLOW + msg + ts())
                write_log(msg)
                if symbol in paper_positions:
                    paper_positions[symbol]["status"] = "CLOSED"
                break

            # ── Target Hit ──────────────────────────────────────────────
            if tgt_order["status"] == "COMPLETE":
                trade_statistics["target_hits"] += 1
                try:
                    kite.cancel_order(variety="regular", order_id=sl_id)
                except Exception as ce:
                    print(Fore.YELLOW + f"⚠️  Could not cancel SL for {symbol}: {ce}" + ts())
                msg = f"🎯 {symbol} TARGET HIT | SL order cancelled"
                print(Fore.BLUE + msg + ts())
                write_log(msg)
                if symbol in paper_positions:
                    paper_positions[symbol]["status"] = "CLOSED"
                break

            consecutive_errors = 0   # reset on successful poll
            time.sleep(2)

        except Exception as e:
            consecutive_errors += 1
            print(Fore.RED + f"⚠️  OCO poll error for {symbol} (attempt {consecutive_errors}): {e}" + ts())
            write_log(f"OCO error {symbol}: {e}")
            if consecutive_errors >= 10:
                print(Fore.RED + f"❌ OCO monitor giving up on {symbol} after 10 errors" + ts())
                write_log(f"OCO MONITOR ABANDONED {symbol}")
                break
            time.sleep(5)   # back off before retry

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

def _safe_to_naive(series):
    """Convert datetime series to timezone-naive safely."""
    parsed = pd.to_datetime(series, errors="coerce")
    try:
        if parsed.dt.tz is not None:
            return parsed.dt.tz_convert(None)
        return parsed
    except Exception:
        return parsed


def load_yesterday_from_local(replay_date):

    print(f"📂 Loading Yesterday OHLC for {replay_date.date()}" + ts())

    # Make replay_date timezone-naive
    replay_date = _safe_to_naive(pd.Series([pd.to_datetime(replay_date)])).iloc[0]

    # ── Zerodha daily candle timestamp fix ──────────────────────────────
    # Kite stores each session with the PREVIOUS calendar day's date at 18:30.
    #   "2026-03-09 18:30" = today's (10-Mar) session  ← must exclude
    #   "2026-03-08 18:30" = yesterday's (09-Mar) data ← this is what we need
    # Without fix: df[date < 10-Mar] picks today's row as "yesterday".
    # Fix: subtract 1 day from cutoff so today's row is excluded.
    cutoff_date = replay_date - pd.Timedelta(days=1)

    for symbol in SYMBOLS:

        file_path = os.path.join(DAILY_DIR, f"{symbol}_daily.csv")

        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)
        df["date"] = _safe_to_naive(df["date"])
        df = df.sort_values("date")

        df_before = df[df["date"] < cutoff_date]

        if df_before.empty:
            continue

        yest = df_before.iloc[-1]

        yesterday_data[symbol] = {
            "high":   yest["high"],
            "low":    yest["low"],
            "close":  yest["close"],
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

    print("📊 Calculating and saving EMA from local daily data..." + ts())

    for symbol in SYMBOLS:

        file_path = os.path.join(DAILY_DIR, f"{symbol}_daily.csv")

        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)
        df["date"] = _safe_to_naive(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        # If replay mode → cut data till replay date (apply same -1 day Zerodha offset)
        if replay_date:
            cutoff = _safe_to_naive(pd.Series([pd.to_datetime(replay_date)])).iloc[0] - pd.Timedelta(days=1)
            df = df[df["date"] < cutoff]

        # Need at least 150 rows for EMA50 to be meaningful
        if len(df) < 50:
            continue

        # Calculate EMA on all rows (full history = accurate values)
        df["ema7"]  = df["close"].ewm(span=7,  adjust=False).mean()
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

        # Save EMA columns back to CSV — persists across restarts
        # Next startup reads pre-calculated values directly
        if not replay_date:   # don't overwrite CSV during replay
            df.to_csv(file_path, index=False)

        # Store only the last row's EMA in memory for live use
        ema_cache[symbol] = {
            "ema7":  round(df["ema7"].iloc[-1],  2),
            "ema20": round(df["ema20"].iloc[-1], 2),
            "ema50": round(df["ema50"].iloc[-1], 2)
        }

    loaded  = len(ema_cache)
    missing = [s for s in SYMBOLS if s not in ema_cache]
    print(f"✅ EMA loaded for {loaded}/{len(SYMBOLS)} symbols" + ts())
    if missing:
        print(Fore.YELLOW + f"⚠️  EMA missing for {len(missing)} symbols: "
              + ", ".join(missing[:15]) + ("…" if len(missing) > 15 else "") + ts())


# ==========================================
# PRE-LOAD ORB + FIRST 15 MIN  (CACHE-AWARE, LIVE MODE)
# ==========================================
# Cache file holds today's ORB & First-15 so restarts skip the API calls.
# File is named by date (e.g.  orb_cache_2026-03-02.csv) so yesterday's
# file is never accidentally used.
# ==========================================

def _orb_cache_file():
    return f"orb_cache_{datetime.now().strftime('%Y-%m-%d')}.csv"

def _save_orb_cache():
    """Write current orb_data + first15_data to today's cache CSV."""
    rows = []
    for symbol in SYMBOLS:
        orb  = orb_data.get(symbol)
        f15  = first15_data.get(symbol)
        if orb and f15:
            rows.append({
                "symbol":       symbol,
                "orb_high":     orb["high"],
                "orb_low":      orb["low"],
                "orb_ready":    orb["ready"],
                "f15_open":     f15["open"],
                "f15_high":     f15["high"],
                "f15_low":      f15["low"],
                "f15_close":    f15["close"],
                "f15_ready":    f15["ready"],
            })
    if rows:
        pd.DataFrame(rows).to_csv(_orb_cache_file(), index=False)
        print(f"💾 ORB cache saved → {_orb_cache_file()}  ({len(rows)} symbols)" + ts())

def _seed_all_from_api():
    """
    Fetches today's minute bars (09:15–now) for all symbols and seeds:
      - latest_highs / latest_lows  (full day range)
      - second15_data  C2  (09:30–09:44)
      - third15_data   C3  (09:45–09:59)
      - fourth15_data  C4  (10:00–10:14)
    Called after cache load so a 14:00 restart gets all candle data correctly.
    Silently skips symbols that fail — WS ticks will fill the gaps.
    """
    now_time   = datetime.now().strftime("%H:%M")
    today      = datetime.now().date()
    from_dt    = datetime.combine(today, datetime.min.time().replace(hour=9, minute=15, second=0))
    to_dt      = datetime.now()
    symbol_to_token = {v: k for k, v in instrument_tokens.items()}

    for symbol in SYMBOLS:
        token = symbol_to_token.get(symbol)
        if not token:
            continue
        try:
            bars = kite.historical_data(token, from_dt, to_dt, "minute")
            if not bars:
                continue
            df = pd.DataFrame(bars)
            df["time_str"] = pd.to_datetime(df["date"]).dt.strftime("%H:%M")

            # ── Full day high/low ────────────────────────────────────────
            full_df = df[df["time_str"] >= "09:15"]
            if not full_df.empty:
                latest_highs[symbol] = max(latest_highs.get(symbol, 0),      float(full_df["high"].max()))
                latest_lows[symbol]  = min(latest_lows.get(symbol,  999999), float(full_df["low"].min()))

            # ── C2: 09:30–09:44 ─────────────────────────────────────────
            c2_df = df[(df["time_str"] >= "09:30") & (df["time_str"] < "09:45")]
            if not c2_df.empty and now_time >= "09:45":
                second15_data[symbol] = {
                    "open":  float(c2_df.iloc[0]["open"]),
                    "high":  float(c2_df["high"].max()),
                    "low":   float(c2_df["low"].min()),
                    "close": float(c2_df.iloc[-1]["close"]),
                    "ready": True
                }

            # ── C3: 09:45–09:59 ─────────────────────────────────────────
            c3_df = df[(df["time_str"] >= "09:45") & (df["time_str"] < "10:00")]
            if not c3_df.empty and now_time >= "10:00":
                third15_data[symbol] = {
                    "high":  float(c3_df["high"].max()),
                    "low":   float(c3_df["low"].min()),
                    "ready": True
                }

            # ── C4: 10:00–10:14 ─────────────────────────────────────────
            c4_df = df[(df["time_str"] >= "10:00") & (df["time_str"] < "10:15")]
            if not c4_df.empty and now_time >= "10:15":
                fourth15_data[symbol] = {
                    "high":  float(c4_df["high"].max()),
                    "low":   float(c4_df["low"].min()),
                    "ready": True
                }

        except Exception:
            pass   # WS ticks will build from here

    print(f"📈 Day H/L + C2/C3/C4 seeded from full-day minute data" + ts())


def _load_orb_cache():
    """
    Try loading today's cache file.
    Returns True if loaded successfully, False if file missing or stale.
    """
    path = _orb_cache_file()
    if not os.path.exists(path):
        return False

    try:
        df = pd.read_csv(path)
        if df.empty:
            return False

        for _, row in df.iterrows():
            symbol = row["symbol"]
            orb_data[symbol] = {
                "high":  float(row["orb_high"]),
                "low":   float(row["orb_low"]),
                "ready": str(row["orb_ready"]).lower() == "true"
            }
            first15_data[symbol] = {
                "open":  float(row["f15_open"]),
                "high":  float(row["f15_high"]),
                "low":   float(row["f15_low"]),
                "close": float(row["f15_close"]),
                "ready": str(row["f15_ready"]).lower() == "true"
            }

            # Seed latest_highs/lows from the best known values so far.
            # ORB and F15 both cover early morning — take the wider range.
            # WS ticks will extend these further via max/min as they arrive.
            known_high = max(float(row["orb_high"]), float(row["f15_high"]))
            known_low  = min(float(row["orb_low"]),  float(row["f15_low"]))
            latest_highs[symbol] = max(latest_highs.get(symbol, 0),      known_high)
            latest_lows[symbol]  = min(latest_lows.get(symbol,  999999), known_low)

        print(f"📂 ORB cache loaded from {path}  ({len(df)} symbols) — skipping API fetch" + ts())

        # ── Top-up latest_highs/lows from full day minute data ───────────
        # Cache only has ORB/F15 range (morning). If restarting mid-day,
        # the true day high/low may be much wider. Fetch minute bars quickly
        # so Today H/L displayed in trade prints is accurate.
        _seed_all_from_api()

        return True

    except Exception as e:
        print(f"⚠️  ORB cache read failed ({e}), will fetch from API" + ts())
        return False


def load_orb_and_first15_from_kite():
    """
    Smart loader — uses today's local cache when available, otherwise
    fetches from Kite API and saves a fresh cache for subsequent restarts.

    Flow:
      1. Check for  orb_cache_YYYY-MM-DD.csv  (today only)
      2a. Found  → load it directly  (fast, no API calls)
      2b. Missing → fetch from Kite API → save cache for next restart

    During the live ORB / First-15 build windows (before 10:15 / 09:30),
    build_context() keeps extending the ranges tick-by-tick, and
    _save_orb_cache() is called again once each window freezes so the
    final complete values are persisted.
    """

    # ── Step 1: try cache ──────────────────────────────────────────────
    if _load_orb_cache():
        return   # done — no API calls needed

    # ── Step 2: fetch from Kite API ────────────────────────────────────
    today    = datetime.now().date()
    now_time = datetime.now().strftime("%H:%M")

    # ── Pre-market guard: market opens at 09:15 ────────────────────────
    # If script is started before 09:15, the Kite API rejects the request
    # because from_dt (09:15) would be after to_dt (now).
    # We skip silently and let build_context() populate ORB/First-15 live
    # from WebSocket ticks once the market opens.
    if now_time < "09:15":
        print(f"⏳ Pre-market ({now_time}) — ORB fetch skipped. Will build live from 09:15 ticks." + ts())
        return

    print(f"📡 No cache found — fetching ORB & First-15 from Kite API for {today}..." + ts())

    symbol_to_token = {v: k for k, v in instrument_tokens.items()}
    loaded = 0

    for symbol in SYMBOLS:

        token = symbol_to_token.get(symbol)
        if not token:
            continue

        try:
            from_dt = datetime(today.year, today.month, today.day, 9, 15)
            # Use market open (09:15) as floor for to_dt to avoid Kite API
            # rejecting requests where from_dt > to_dt during early morning restarts.
            to_dt = max(datetime.now(), from_dt)
            minute_data = kite.historical_data(
                token,
                from_dt,
                to_dt,
                "minute"
            )

            if not minute_data:
                continue

            df = pd.DataFrame(minute_data)
            df["time_str"] = pd.to_datetime(df["date"]).dt.strftime("%H:%M")

            # ── ORB: 09:15 – 10:14 ──
            orb_df = df[(df["time_str"] >= "09:15") & (df["time_str"] < "10:15")]
            if not orb_df.empty:
                orb_data[symbol] = {
                    "high":  float(orb_df["high"].max()),
                    "low":   float(orb_df["low"].min()),
                    "ready": now_time >= "10:15"
                }

            # ── First 15 (C1): 09:15 – 09:29 ──
            f15_df = df[(df["time_str"] >= "09:15") & (df["time_str"] < "09:30")]
            if not f15_df.empty:
                first15_data[symbol] = {
                    "open":  float(f15_df.iloc[0]["open"]),
                    "high":  float(f15_df["high"].max()),
                    "low":   float(f15_df["low"].min()),
                    "close": float(f15_df.iloc[-1]["close"]),
                    "ready": now_time >= "09:30"
                }

            # ── C2 (09:30–09:44), C3 (09:45–09:59), C4 (10:00–10:14) ──────
            # CRITICAL: must be seeded from API when starting after each window
            # closes. Without this, the builder functions init with high=0 /
            # low=999999 which makes every inside-bar check trivially pass —
            # causing phantom trades on completely fake candle data.
            c2_df = df[(df["time_str"] >= "09:30") & (df["time_str"] < "09:45")]
            if not c2_df.empty and now_time >= "09:45":
                second15_data[symbol] = {
                    "open":  float(c2_df.iloc[0]["open"]),
                    "high":  float(c2_df["high"].max()),
                    "low":   float(c2_df["low"].min()),
                    "close": float(c2_df.iloc[-1]["close"]),
                    "ready": True
                }

            c3_df = df[(df["time_str"] >= "09:45") & (df["time_str"] < "10:00")]
            if not c3_df.empty and now_time >= "10:00":
                third15_data[symbol] = {
                    "high":  float(c3_df["high"].max()),
                    "low":   float(c3_df["low"].min()),
                    "ready": True
                }

            c4_df = df[(df["time_str"] >= "10:00") & (df["time_str"] < "10:15")]
            if not c4_df.empty and now_time >= "10:15":
                fourth15_data[symbol] = {
                    "high":  float(c4_df["high"].max()),
                    "low":   float(c4_df["low"].min()),
                    "ready": True
                }

            # ── Seed latest_highs/lows from all available candles ────────
            full_day_df = df[df["time_str"] >= "09:15"]
            if not full_day_df.empty:
                api_high = float(full_day_df["high"].max())
                api_low  = float(full_day_df["low"].min())
                latest_highs[symbol] = max(latest_highs.get(symbol, 0),      api_high)
                latest_lows[symbol]  = min(latest_lows.get(symbol, 999999),  api_low)

            loaded += 1
            time.sleep(0.1)   # respect rate limits

        except Exception as e:
            print(f"⚠️  {symbol}: ORB fetch failed — {e}" + ts())
            continue

    print(f"✅ ORB & First-15 fetched from Kite API for {loaded} symbols" + ts())

    # ── Step 3: save cache for next restart ────────────────────────────
    # Only save when both windows are fully closed (complete data).
    # If we're still mid-window, build_context() will keep updating, and
    # the freeze blocks below will call _save_orb_cache() once finalised.
    if now_time >= "10:15":
        _save_orb_cache()


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
            side  = pos["side"]
            qty   = pos["qty"]

            points = (ltp - entry) if side == "BUY" else (entry - ltp)
            pnl    = points * qty

            # =============================
            # TRAILING LOGIC
            # =============================
            move_percent = abs((ltp - entry) / entry) * 100

            #if move_percent >= (pos["trail_level"] + TRAIL_STEP_PERCENT):
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

                gross_pnl = points * qty
                net_pnl   = gross_pnl - (BROKERAGE_PER_ORDER * 2)

                print(
                    Fore.YELLOW +
                    f"=============>>>>>>>>   TRAIL UPDATED | {symbol} | {side} | "
                    f"Entry:{entry} | LTP:{ltp} {_chg(ltp, pos.get('y_close', 0))} | "
                    f"New SL:{pos['sl']} | Target:{pos['target']} | "
                    f"Points:{points:.2f} | NetPnL:{net_pnl:.2f}\n"
                 + ts())

            # =============================
            # EXIT CONDITIONS — compare LTP directly against stored SL/target.
            # SL and target are calculated at entry time and stored in the position.
            # They are independent of intraday high/low — just compare current price.
            # =============================
            if side == "BUY":
                if ltp <= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT"
                elif ltp >= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT"
                else:
                    continue
            elif side == "SELL":
                if ltp >= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT"
                elif ltp <= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT"
                else:
                    continue

            points    = (exit_price - entry) if side == "BUY" else (entry - exit_price)
            pos["status"] = "CLOSED"
            gross_pnl = points * qty
            net_pnl   = gross_pnl - (BROKERAGE_PER_ORDER * 2)
            pos["net_pnl"] = net_pnl   # store for cache rebuild on restart
            daily_pnl += net_pnl
            save_paper_positions_cache()   # persist closed state + updated daily_pnl

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
                f"LTP:{ltp} {_chg(ltp, pos.get('y_close', 0))} | "
                f"Points:{points:.2f} | NetPnL:{net_pnl:.2f}\n"
             + ts())

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




# ==========================================
# REPLAY INTRADAY STATE UPDATER
# ==========================================
# Called once per candle in the replay loop BEFORE strategy() fires.
# Updates cumulative volume, rolling 15-min candle, rolling 1-hour candle.
# In live mode these values come naturally from the WebSocket tick stream.
# ==========================================
def replay_update_intraday_state(symbol, candle_time, open_p, high, low, close_p, volume):
    """
    Update replay intraday state for one 1-min bar.
    Returns a dict of context overrides to be injected into build_context results.
    """
    # ── Cumulative volume (mimics live volume_traded) ─────────────────────
    replay_cum_volume[symbol] = replay_cum_volume.get(symbol, 0) + int(volume)
    cum_vol = replay_cum_volume[symbol]

    # ── 15-minute rolling candle ──────────────────────────────────────────
    # Kite 15-min bars start at :00/:15/:30/:45 (e.g. 09:15, 09:30 …)
    # We identify the candle's 15-min slot by flooring minutes to nearest 15.
    minute_of_day = candle_time.hour * 60 + candle_time.minute
    slot_15 = (minute_of_day // 15) * 15          # e.g. 09:16 → slot 9*60+15=555
    slot_15_str = f"{slot_15 // 60:02d}:{slot_15 % 60:02d}"

    if symbol not in replay_15m_data or replay_15m_data[symbol]["start_min"] != slot_15:
        replay_15m_data[symbol] = {
            "open":       open_p,
            "high":       high,
            "low":        low,
            "close":      close_p,
            "start_min":  slot_15,
            "slot_str":   slot_15_str
        }
    else:
        c = replay_15m_data[symbol]
        c["high"]  = max(c["high"],  high)
        c["low"]   = min(c["low"],   low)
        c["close"] = close_p

    # ── 1-hour rolling candle ─────────────────────────────────────────────
    hour_slot = candle_time.hour          # 9, 10, 11 …
    if symbol not in replay_1h_data or replay_1h_data[symbol]["start_hour"] != hour_slot:
        replay_1h_data[symbol] = {
            "open":       open_p,
            "high":       high,
            "low":        low,
            "close":      close_p,
            "start_hour": hour_slot
        }
    else:
        c = replay_1h_data[symbol]
        c["high"]  = max(c["high"],  high)
        c["low"]   = min(c["low"],   low)
        c["close"] = close_p

    return {
        "cum_volume":   cum_vol,
        "c15":          dict(replay_15m_data[symbol]),
        "c1h":          dict(replay_1h_data[symbol]),
    }


def build_context(token, tick):

    symbol = instrument_tokens.get(token)
    if not symbol:
        return None

    if symbol not in yesterday_data:
        return None

    ltp = tick["last_price"]
    ohlc = tick["ohlc"]

    candle_time = tick.get("date", datetime.now())
    current_time = candle_time.strftime("%H:%M")

    # Ignore pre-market
    if current_time < "09:15":
        return None

    # ==========================================================
    # ================= ORB BUILD (STRICT FREEZE) ===============
    # ==========================================================
    # Build using ltp (every individual price tick) so we never miss
    # intra-candle highs/lows. ohlc["high/low"] is only the current
    # 1-min candle range — using ltp gives true tick-by-tick ORB.
    if symbol not in orb_data:
        if "09:15" <= current_time < "10:15":
            orb_data[symbol] = {
                "high": ltp,
                "low": ltp,
                "ready": False
            }
        else:
            orb_data[symbol] = {
                "high": 0,
                "low": 999999,
                "ready": False
            }

    # Build ONLY between 09:15 and 10:14
    if "09:15" <= current_time < "10:15":
        orb_data[symbol]["high"] = max(orb_data[symbol]["high"], ltp)
        orb_data[symbol]["low"]  = min(orb_data[symbol]["low"],  ltp)

    # Freeze after 10:15 — save cache once when window closes
    if current_time >= "10:15":
        was_ready = orb_data[symbol]["ready"]
        orb_data[symbol]["ready"] = True
        if not was_ready:
            if all(orb_data.get(s, {}).get("ready", False) for s in SYMBOLS if s in orb_data):
                _save_orb_cache()

    # ==========================================================
    # ============== FIRST 15 MIN BUILD (STRICT FREEZE) =========
    # ==========================================================
    # Same fix: use ltp for high/low so every tick is captured.
    # open is set only once (first tick of the day at 09:15).
    if symbol not in first15_data:
        if "09:15" <= current_time < "09:30":
            first15_data[symbol] = {
                "open": ltp,   # first tick = day open
                "high": ltp,
                "low":  ltp,
                "close": ltp,
                "ready": False
            }
        else:
            first15_data[symbol] = {
                "open": 0,
                "high": 0,
                "low": 999999,
                "close": 0,
                "ready": False
            }

    # Build ONLY between 09:15 and 09:29
    if "09:15" <= current_time < "09:30":
        first15_data[symbol]["high"]  = max(first15_data[symbol]["high"], ltp)
        first15_data[symbol]["low"]   = min(first15_data[symbol]["low"],  ltp)
        first15_data[symbol]["close"] = ltp

    # Freeze after 09:30
    if current_time >= "09:30":
        was_ready = first15_data[symbol]["ready"]
        first15_data[symbol]["ready"] = True
        if not was_ready:
            if all(first15_data.get(s, {}).get("ready", False) for s in SYMBOLS if s in first15_data):
                _save_orb_cache()

    # ==========================================================
    # ================= ENTRY TIME FILTER =======================
    # ==========================================================
    if not (ENTRY_START_TIME <= current_time <= ENTRY_END_TIME):
        return None

    # ==========================================================
    # ================= CONTEXT VALUES ==========================
    # ==========================================================
    yest = yesterday_data[symbol]
    ema = ema_cache.get(symbol, {})

    if not ema.get("ema7") or not ema.get("ema20"):
        return None

    change_percent = ((ltp - yest["close"]) / yest["close"]) * 100

    # gap_percent: use first15_data open (actual 09:15 market open, frozen) if available.
    # ohlc["open"] is the current 1-min candle open — changes every minute — wrong for gap calc.
    # Fall back to ohlc["open"] only before first15 is ready (i.e. during 09:15–09:29 itself).
    _f15_open = first15_data.get(symbol, {}).get("open", 0)
    _gap_base  = _f15_open if _f15_open > 0 else ohlc["open"]
    gap_percent = ((_gap_base - yest["close"]) / yest["close"]) * 100

    live_volume = tick.get("volume_traded", tick.get("volume", 0))

    # ── Replay: use cumulative day volume instead of single candle volume ──
    # In live mode, volume_traded from WebSocket is already cumulative.
    # In replay, each tick carries only the 1-min bar volume.
    # replay_cum_volume[symbol] is built bar-by-bar before strategy() is called.
    if symbol in replay_cum_volume:
        live_volume = replay_cum_volume[symbol]

    vol_percent = 0
    if yest["volume"] > 0:
        vol_percent = round((live_volume / yest["volume"]) * 100, 2)

    # ── 15-min and 1-hour candle context (replay builds on-the-fly) ───────
    c15  = replay_15m_data.get(symbol, {})
    c1h  = replay_1h_data.get(symbol, {})

    # How far has price already moved past yesterday's high/low?
    # Positive = below YL (overextended SELL), Negative = still above YL
    # Used by all strategies to reject entries where the move is exhausted.
    dist_from_yl_pct = ((yest["low"] - ltp) / yest["low"]) * 100   # +ve = below YL
    dist_from_yh_pct = ((ltp - yest["high"]) / yest["high"]) * 100  # +ve = above YH

    return {
        "symbol":           symbol,
        "tick":             tick,
        "ltp":              ltp,
        "current_time":     current_time,
        "ohlc":             ohlc,
        "yest":             yest,
        "ema":              ema,
        "change_percent":   change_percent,
        "gap_percent":      gap_percent,
        "live_volume":      live_volume,
        "vol_percent":      vol_percent,
        "dist_from_yl_pct": dist_from_yl_pct,   # how far below YL (SELL overextension)
        "dist_from_yh_pct": dist_from_yh_pct,   # how far above YH (BUY overextension)
        "c15":              c15,    # current rolling 15-min candle {open,high,low,close}
        "c1h":              c1h,    # current rolling 1-hour candle {open,high,low,close}
    }




# ==========================================
# STRATEGY SCAN DIAGNOSTIC SYSTEM
# ==========================================
# Prints a per-minute summary for every strategy showing:
#   • Which symbols were scanned
#   • Why each was SKIPPED (first failing condition)
#   • Which ones hit SIGNAL (would place_trade)
#
# Fires ONCE per minute per strategy — not on every tick.
# Format:
#   ── Strategy Name ── scanning at HH:MM
#   SYMBOL   SKIP REASON  (or)  ✅ SIGNAL BUY/SELL
#   ...
#   completed at HH:MM:SS
# ==========================================

_scan_lock         = threading.Lock()
_scan_last_minute  = {}   # strategy_name → "HH:MM" currently being collected
_scan_buffer       = {}   # strategy_name → list of (symbol, result_str)
_scan_printed      = {}   # strategy_name → last "HH:MM" already printed

def _scan_start(strategy_name, current_time):
    """Called once per tick per strategy. Rotates buffer when minute changes."""
    with _scan_lock:
        last = _scan_last_minute.get(strategy_name)
        if last != current_time:
            _scan_last_minute[strategy_name] = current_time
            _scan_buffer[strategy_name] = {}   # dict: symbol→result, deduplicates ticks

def _scan_record(strategy_name, symbol, result):
    """Record one symbol scan result for the current minute."""
    with _scan_lock:
        buf = _scan_buffer.get(strategy_name)
        if buf is not None:
            buf[symbol] = result   # overwrites — one result per symbol per minute

def _flush_scan(strategy_name, minute_str):
    """Print the buffered scan summary for a completed minute."""
    with _scan_lock:
        if _scan_printed.get(strategy_name) == minute_str:
            return   # already printed this minute
        buf_raw = _scan_buffer.get(strategy_name, {})
        buf = list(buf_raw.items()) if isinstance(buf_raw, dict) else list(buf_raw)
        _scan_printed[strategy_name] = minute_str

    if not buf:
        return

    signals   = [(s, r) for s, r in buf if r.startswith("\u2705")]
    skipped   = [(s, r) for s, r in buf if not r.startswith("\u2705")]
    total     = len(buf)
    sig_count = len(signals)

    print(Fore.CYAN +
        f"\n\u2500\u2500 {strategy_name} \u2500\u2500 scanning at {minute_str}  "
        f"({total} symbols | {sig_count} signal{'s' if sig_count != 1 else ''})" + ts())

    if signals:
        for sym, res in signals:
            print(Fore.GREEN + f"   {sym:<14} {res}" + ts())
    else:
        print(Fore.YELLOW + "   (no signals this minute)" + ts())

    reason_groups = {}
    for sym, res in skipped:
        reason_groups.setdefault(res, []).append(sym)

    if reason_groups:
        print(Fore.WHITE + "   Skip reasons:" + ts())
        for reason, syms in sorted(reason_groups.items()):
            sym_list = ", ".join(syms[:8]) + ("\u2026" if len(syms) > 8 else "")
            print(Fore.WHITE + f"     {reason:<40} \u2192 {sym_list}" + ts())

    print(Fore.CYAN +
        f"\u2500\u2500 {strategy_name} \u2500\u2500 completed at {datetime.now().strftime('%H:%M:%S')}" + ts())


_STRATEGY_NAMES = [
    "ORB Breakout",
    "Open=Low/High Break",
    "EMA Pullback",
    "Inside Bar Breakout",
    "VWAP Reclaim",
    "YL Breakdown / YH Breakout",
    "Gap+First15 Breakout",
    "15m Inside Range Break",
]

def _scan_flush_worker():
    """
    Background thread — wakes at :02 of every new minute and prints the
    PREVIOUS minute's scan buffer for all strategies.
    Guarantees every minute is printed even if no new-minute tick arrives.
    """
    last_flushed = ""
    while True:
        now = datetime.now()
        # Sleep until 2 seconds into the next minute
        secs_to_wait = (62 - now.second) % 60 or 62
        time.sleep(secs_to_wait)

        prev_minute = (datetime.now() - timedelta(minutes=1)).strftime("%H:%M")
        if prev_minute == last_flushed:
            continue
        last_flushed = prev_minute

        for sname in _STRATEGY_NAMES:
            with _scan_lock:
                buf_minute = _scan_last_minute.get(sname)
                has_data   = bool(_scan_buffer.get(sname))
            if buf_minute == prev_minute and has_data:
                _flush_scan(sname, prev_minute)

# Start flush thread at module level
threading.Thread(target=_scan_flush_worker, daemon=True).start()



# ==========================================
# HELPER: build extra info dict for place_trade
# ==========================================
def _extra(ctx):
    symbol = ctx["symbol"]
    f   = first15_data.get(symbol, {})
    orb = orb_data.get(symbol, {})
    c2  = second15_data.get(symbol, {})
    c3  = third15_data.get(symbol, {})
    c4  = fourth15_data.get(symbol, {})
    return {
        "orb_high":   orb.get("high", 0),
        "orb_low":    orb.get("low", 0),
        "f_high":     f.get("high", 0),
        "f_low":      f.get("low", 0),
        "c2_high":    c2.get("high", "-"),
        "c2_low":     c2.get("low",  "-"),
        "c3_high":    c3.get("high", "-"),
        "c3_low":     c3.get("low",  "-"),
        "c4_high":    c4.get("high", "-"),
        "c4_low":     c4.get("low",  "-"),
        "y_high":     ctx["yest"]["high"],
        "y_low":      ctx["yest"]["low"],
        "y_close":    ctx["yest"]["close"],
        "y_vol":      ctx["yest"]["volume"],
        "t_vol":      ctx["live_volume"],
        "vol_pct":    ctx["vol_percent"],
        "t_open":     f.get("open", 0),
        "t_high":     latest_highs.get(symbol, orb.get("high", 0)),
        "t_low":      latest_lows.get(symbol,  orb.get("low", 0)),
    }


# ==========================================
# STRATEGY 1 — ORB BREAKOUT (FIXED + BOTH SIDES)
# ==========================================
# Original had: no volume filter, no ORB range size check, no SELL side, no gap filter.
# Fixed:
#   • Volume must be >= 40% of yesterday's full-day volume by entry time (real demand)
#   • ORB range must be 0.3%–2.5% (not a flat day, not a wild gap day)
#   • Gap filter: stock must not have already gapped past the ORB (chasing avoidance)
#   • SELL side added: ORB low breakdown with same symmetric filters
#   • Both sides require EMA7 > EMA20 alignment
# ==========================================
def strategy_orb(ctx):

    _SNAME = "ORB Breakout"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    vol_percent      = ctx["vol_percent"]
    gap_percent      = ctx["gap_percent"]
    current_time     = ctx["current_time"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)

    if not orb_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "ORB not ready yet")
        return

    orb_high = orb_data[symbol]["high"]
    orb_low  = orb_data[symbol]["low"]

    if orb_high == 0 or orb_low == 999999:
        _scan_record(_SNAME, symbol, "ORB high/low invalid")
        return

    orb_range_pct = ((orb_high - orb_low) / orb_low) * 100

    if not (0.3 <= orb_range_pct <= 2.5):
        _scan_record(_SNAME, symbol, f"ORB range {orb_range_pct:.2f}% outside 0.3–2.5%")
        return

    # Volume filter: cumulative today volume must be >= 15% of yesterday's total
    # (works in both live mode and replay now that cum volume is properly tracked)
    if vol_percent < 15:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 15% of yesterday")
        return

    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 13:00 cutoff")
        return

    # Max slippage: reject entry if LTP is already too far past the breakout level.
    # This prevents chasing — e.g. ORB high=6272 but LTP=6301 is 0.46% above,
    # which means the move already happened within a 1-min candle.
    # Allow up to 0.3% above ORB high for BUY, 0.3% below ORB low for SELL.
    MAX_ENTRY_SLIPPAGE_PCT = 0.3

    # ── BUY: ORB High Breakout ──────────────────────────────────────────
    buy_slippage_pct = ((ltp - orb_high) / orb_high * 100) if orb_high > 0 else 999

    # Stale breakout guard: if day_high is already well past ORB high,
    # the breakout happened hours ago — entering now is chasing.
    t_high = latest_highs.get(symbol, ltp)
    t_low  = latest_lows.get(symbol,  ltp)
    MAX_STALE_PCT = 0.5
    day_high_past_orb_pct = ((t_high  - orb_high) / orb_high * 100) if orb_high > 0 else 0
    day_low_past_orb_pct  = ((orb_low - t_low)    / orb_low  * 100) if orb_low  > 0 else 0

    buy_signal = (
        299 <= ltp <= 6999 and
        -0.5 <= gap_percent <= 1.5 and
        ltp >= orb_high and
        buy_slippage_pct <= MAX_ENTRY_SLIPPAGE_PCT and
        day_high_past_orb_pct <= MAX_STALE_PCT and
        ema["ema7"] >= ema["ema20"] and
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT
    )
    # ── SELL: ORB Low Breakdown ─────────────────────────────────────────
    sell_slippage_pct = ((orb_low - ltp) / orb_low * 100) if orb_low > 0 else 999
    sell_signal = (
        299 <= ltp <= 6999 and
        -1.5 <= gap_percent <= 0.5 and
        ltp <= orb_low and
        sell_slippage_pct <= MAX_ENTRY_SLIPPAGE_PCT and
        day_low_past_orb_pct <= MAX_STALE_PCT and
        ema["ema7"] <= ema["ema20"] and
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
    )

    if buy_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL BUY  ltp={ltp} orb_high={orb_high:.2f}")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "ORB Break", _extra(ctx))
    elif sell_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL SELL ltp={ltp} orb_low={orb_low:.2f}")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "ORB Break SELL", _extra(ctx))
    else:
        # Identify first failing condition for debug
        if not (299 <= ltp <= 6999):
            reason = f"LTP {ltp} out of 299–6999"
        elif ltp >= orb_high and buy_slippage_pct > MAX_ENTRY_SLIPPAGE_PCT:
            reason = f"BUY slippage {buy_slippage_pct:.2f}% > {MAX_ENTRY_SLIPPAGE_PCT}% — too far past ORB high {orb_high:.2f}"
        elif ltp <= orb_low and sell_slippage_pct > MAX_ENTRY_SLIPPAGE_PCT:
            reason = f"SELL slippage {sell_slippage_pct:.2f}% > {MAX_ENTRY_SLIPPAGE_PCT}% — too far past ORB low {orb_low:.2f}"
        elif ltp >= orb_high and not (-0.5 <= gap_percent <= 1.5):
            reason = f"BUY gap {gap_percent:.2f}% outside -0.5–1.5%"
        elif ltp <= orb_low and not (-1.5 <= gap_percent <= 0.5):
            reason = f"SELL gap {gap_percent:.2f}% outside -1.5–0.5%"
        elif ltp >= orb_high and ema["ema7"] < ema["ema20"]:
            reason = f"BUY EMA7({ema['ema7']:.1f})<EMA20({ema['ema20']:.1f})"
        elif ltp <= orb_low and ema["ema7"] > ema["ema20"]:
            reason = f"SELL EMA7({ema['ema7']:.1f})>EMA20({ema['ema20']:.1f})"
        elif orb_low < ltp < orb_high:
            reason = f"LTP {ltp} inside ORB {orb_low:.2f}–{orb_high:.2f}"
        else:
            reason = f"No breakout: ltp={ltp} orb={orb_low:.2f}–{orb_high:.2f}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY 2 — OPEN = LOW / OPEN = HIGH (FIXED)
# ==========================================
# Original had: exact float equality (almost never triggered), range too tight (<1%),
#               no volume filter.
# Fixed:
#   • Float equality replaced with 0.2% tolerance
#   • Range widened to 0.3%–2.0% (catches real setups)
#   • Volume confirmation added (>= 30%)
#   • close > open confirmation retained (bullish close within 15m)
#   • Only valid in morning session
# ==========================================
def strategy_open_low(ctx):

    _SNAME = "Open=Low/High Break"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    vol_percent      = ctx["vol_percent"]
    current_time     = ctx["current_time"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)

    if not first15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "First-15 not ready")
        return

    f = first15_data[symbol]
    if f["open"] == 0:
        _scan_record(_SNAME, symbol, "First-15 open=0")
        return

    range_pct = ((f["high"] - f["low"]) / f["open"]) * 100

    if not (0.3 <= range_pct <= 2.0):
        _scan_record(_SNAME, symbol, f"15m range {range_pct:.2f}% outside 0.3–2.0%")
        return

    if vol_percent < 15:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 15% of yesterday")
        return

    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 13:00 cutoff")
        return

    tol = 0.001   # tightened from 0.2% → 0.1%: open must be within 0.1% of low/high

    open_is_low  = abs(f["open"] - f["low"])  / f["open"] < tol
    open_is_high = abs(f["open"] - f["high"]) / f["open"] < tol

    yest = ctx["yest"]

    today_open = f["open"]
    gap_from_yl = ((today_open - yest["low"]) / yest["low"]) * 100
    gap_from_yh = ((yest["high"] - today_open) / yest["high"]) * 100

    today_opened_below_yl = today_open < yest["low"]

    # ── F15 range sanity: if the F15 candle range itself is >1.5%, the stock
    #    had a violent open swing — not a clean consolidation. Skip both sides.
    f15_range_pct = ((f["high"] - f["low"]) / f["open"]) * 100
    if f15_range_pct > 1.5:
        _scan_record(_SNAME, symbol,
            f"F15 range {f15_range_pct:.2f}% > 1.5% — violent open swing, not consolidation")
        return

    # ── Intraday crash/spike filter ───────────────────────────────────────
    # If the day's low is already >1% below F15 open, the stock had a violent
    # drop and recovered — this is a bounce, not a clean Open=Low setup.
    # Same logic for SELL: if day's high is >1% above F15 open, stock spiked
    # and faded — this is a pullback, not a clean Open=High setup.
    day_high = latest_highs.get(symbol, ltp)
    day_low  = latest_lows.get(symbol,  ltp)
    MAX_INTRADAY_CRASH_PCT = 1.0

    day_crashed = ((today_open - day_low)  / today_open * 100) > MAX_INTRADAY_CRASH_PCT
    day_spiked  = ((day_high  - today_open) / today_open * 100) > MAX_INTRADAY_CRASH_PCT

    if day_crashed:
        _scan_record(_SNAME, symbol,
            f"BUY blocked: day low {day_low:.2f} is already "
            f"{((today_open - day_low)/today_open*100):.2f}% below open — crash+bounce, not consolidation")
        return
    # Note: day_spiked check only blocks SELL (handled in sell_signal conditions below)

    gap_vs_yclose = ((today_open - yest["close"]) / yest["close"]) * 100

    buy_signal = (
        299 <= ltp <= 6999 and
        open_is_low and
        f["close"] > f["open"] and
        ltp > f["high"] and
        ema["ema7"] >= ema["ema20"] and
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT and
        gap_from_yl >= -1.0 and
        gap_vs_yclose >= -0.5 and
        not day_crashed                            # no violent intraday crash before this signal
    )
    sell_signal = (
        299 <= ltp <= 6999 and
        open_is_high and
        f["close"] < f["open"] and
        ltp < f["low"] and
        ema["ema7"] <= ema["ema20"] and
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT and
        gap_from_yh >= -1.0 and
        not today_opened_below_yl and
        gap_vs_yclose <= 0.5 and
        not day_spiked                             # no violent intraday spike before this signal
    )

    if buy_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL BUY  ltp={ltp} f15_high={f['high']:.2f}")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "OPEN==LOW Break", _extra(ctx))
    elif sell_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL SELL ltp={ltp} f15_low={f['low']:.2f}")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "OPEN==HIGH Break", _extra(ctx))
    else:
        if not open_is_low and not open_is_high:
            reason = f"Open not≈Low/High (open={f['open']:.2f} lo={f['low']:.2f} hi={f['high']:.2f})"
        elif open_is_low and gap_vs_yclose < -0.5:
            reason = f"BUY: gap-down open {gap_vs_yclose:.2f}% vs YClose {yest['close']:.2f} — bearish context"
        elif open_is_high and gap_vs_yclose > 0.5:
            reason = f"SELL: gap-up open {gap_vs_yclose:.2f}% vs YClose {yest['close']:.2f} — bullish context"
        elif open_is_low and gap_from_yl < -1.0:
            reason = f"BUY: gap-down context — open {today_open:.2f} is {abs(gap_from_yl):.2f}% below YL {yest['low']:.2f}"
        elif open_is_high and today_opened_below_yl:
            reason = f"SELL: today opened {today_open:.2f} already below YL {yest['low']:.2f}"
        elif open_is_high and gap_from_yh < -1.0:
            reason = f"SELL: gap-up context — open {today_open:.2f} is {abs(gap_from_yh):.2f}% above YH {yest['high']:.2f}"
        elif open_is_low and not (ltp > f["high"]):
            reason = f"BUY: ltp {ltp} not above f15_high {f['high']:.2f}"
        elif open_is_high and not (ltp < f["low"]):
            reason = f"SELL: ltp {ltp} not below f15_low {f['low']:.2f}"
        elif open_is_low and ema["ema7"] < ema["ema20"]:
            reason = f"BUY EMA7<EMA20 ({ema['ema7']:.1f}<{ema['ema20']:.1f})"
        elif open_is_high and ema["ema7"] > ema["ema20"]:
            reason = f"SELL EMA7>EMA20 ({ema['ema7']:.1f}>{ema['ema20']:.1f})"
        else:
            reason = f"No setup: open={f['open']:.2f} hi={f['high']:.2f} lo={f['low']:.2f} ltp={ltp}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY 3 — EMA PULLBACK ON TREND (NEW)
# ==========================================
# Logic: Stock in confirmed uptrend (EMA7 > EMA20 > EMA50). Price pulls back
#        to touch EMA20, then resumes — current price is back above EMA7.
#        Enter on resumption. This is "buy at value in a trend" — not chasing.
#
# Why high win rate: You enter WITH the trend, at a support level (EMA20),
#   after confirmation of resumption (price back above EMA7). Three confluences.
#
# Pullback detection: we track whether price was at or below EMA20 within
#   the last few ticks using a per-symbol pullback state cache.
# ==========================================

ema_pullback_state = {}   # symbol → {"touched_ema20": bool, "touched_time": str}

# ==========================================
# REPLAY INTRADAY STATE  (built bar-by-bar during replay)
# ==========================================
# These mirror what live mode gets from WebSocket ticks:
#   replay_cum_volume  — cumulative day volume per symbol (for vol_percent filter)
#   replay_15m_data    — rolling 15-min candle  H/L/O/C (resets every :15/:30/:45/:00)
#   replay_1h_data     — rolling 1-hour candle  H/L/O/C (resets every full hour)
# All are cleared at the top of run_market_replay_proper() before each day.
# ==========================================
replay_cum_volume  = {}   # symbol → int  (sum of 1-min volumes so far today)
replay_15m_data    = {}   # symbol → {"open","high","low","close","start_min"}
replay_1h_data     = {}   # symbol → {"open","high","low","close","start_hour"}

def strategy_ema_pullback(ctx):

    _SNAME = "EMA Pullback"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    vol_percent      = ctx["vol_percent"]
    current_time     = ctx["current_time"]
    change_pct       = ctx["change_percent"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)

    ema7  = ema.get("ema7", 0)
    ema20 = ema.get("ema20", 0)
    ema50 = ema.get("ema50", 0)

    if not (ema7 and ema20 and ema50):
        _scan_record(_SNAME, symbol, "Missing EMA7/20/50")
        return

    if current_time > "14:30":
        _scan_record(_SNAME, symbol, "After 13:30 cutoff")
        return

    if vol_percent < 15:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 15% of yesterday")
        return
    if symbol not in ema_pullback_state:
        ema_pullback_state[symbol] = {"touched_ema20": False, "touched_time": ""}

    state = ema_pullback_state[symbol]

    if ema7 > ema20 > ema50:  # confirmed uptrend
        near_ema20 = abs(ltp - ema20) / ema20 < 0.003
        if near_ema20 or ltp <= ema20:
            state["touched_ema20"] = True
            state["touched_time"]  = current_time

        if (
            state["touched_ema20"] and
            ltp > ema7 and
            299 <= ltp <= 6999 and
            0.2 <= change_pct <= 4.0 and
            state["touched_time"] < current_time and
            dist_from_yh_pct <= MAX_OVEREXTENSION_PCT
        ):
            _scan_record(_SNAME, symbol, f"✅ SIGNAL BUY  ltp={ltp} ema7={ema7:.1f}")
            place_trade(symbol, "BUY", ltp, ctx["tick"], "EMA Pullback BUY", _extra(ctx))
            state["touched_ema20"] = False
        else:
            if not state["touched_ema20"]:
                reason = f"UP: no EMA20 touch yet (ltp={ltp:.1f} ema20={ema20:.1f})"
            elif not (ltp > ema7):
                reason = f"UP: ltp {ltp:.1f} not above EMA7 {ema7:.1f}"
            elif not (0.2 <= change_pct <= 4.0):
                reason = f"UP: change {change_pct:.2f}% outside 0.2–4.0%"
            else:
                reason = f"UP: waiting (touched={state['touched_ema20']} ltp={ltp:.1f})"
            _scan_record(_SNAME, symbol, reason)

    elif ema7 < ema20 < ema50:  # confirmed downtrend
        near_ema20 = abs(ltp - ema20) / ema20 < 0.003
        if near_ema20 or ltp >= ema20:
            state["touched_ema20"] = True
            state["touched_time"]  = current_time

        if (
            state["touched_ema20"] and
            ltp < ema7 and
            299 <= ltp <= 6999 and
            -4.0 <= change_pct <= -0.2 and
            state["touched_time"] < current_time and
            dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
        ):
            _scan_record(_SNAME, symbol, f"✅ SIGNAL SELL ltp={ltp} ema7={ema7:.1f}")
            place_trade(symbol, "SELL", ltp, ctx["tick"], "EMA Pullback SELL", _extra(ctx))
            state["touched_ema20"] = False
        else:
            if not state["touched_ema20"]:
                reason = f"DN: no EMA20 touch yet (ltp={ltp:.1f} ema20={ema20:.1f})"
            elif not (ltp < ema7):
                reason = f"DN: ltp {ltp:.1f} not below EMA7 {ema7:.1f}"
            elif not (-4.0 <= change_pct <= -0.2):
                reason = f"DN: change {change_pct:.2f}% outside -4.0–-0.2%"
            else:
                reason = f"DN: waiting (touched={state['touched_ema20']} ltp={ltp:.1f})"
            _scan_record(_SNAME, symbol, reason)
    else:
        _scan_record(_SNAME, symbol, f"EMAs not aligned (e7={ema7:.1f} e20={ema20:.1f} e50={ema50:.1f})")


# ==========================================
# STRATEGY 4 — 15-MIN INSIDE BAR BREAKOUT (NEW)
# ==========================================
# Logic: When the second 15-min candle is completely inside the first
#        (compression), a breakout from the first candle's range is explosive.
#        Compression → expansion is one of the most reliable price action patterns.
#
# Why high win rate: Tight range = market participants undecided = energy building.
#   When it breaks, stops from both sides fuel the move. Best in morning session.
#
# Implementation: We use first15_data as candle 1, and build a "second15_data"
#   cache for the 09:30–09:44 window as candle 2.
# ==========================================

second15_data = {}  # symbol → {open, high, low, close, ready}

def _build_second15(symbol, ohlc, ltp, current_time):
    """Build the 09:30–09:44 candle (second 15-min candle) using ltp for accuracy."""

    if symbol not in second15_data:
        if "09:30" <= current_time < "09:45":
            second15_data[symbol] = {
                "open":  ltp, "high": ltp, "low": ltp, "close": ltp, "ready": False
            }
        else:
            # Started after window — will be seeded from API; don't mark ready yet
            second15_data[symbol] = {
                "open": 0, "high": 0, "low": 999999, "close": 0, "ready": False
            }

    if "09:30" <= current_time < "09:45":
        c = second15_data[symbol]
        c["high"]  = max(c["high"], ltp)
        c["low"]   = min(c["low"],  ltp)
        c["close"] = ltp

    if current_time >= "09:45":
        # Only mark ready if we have real data (not the 0/999999 placeholder)
        if second15_data[symbol]["high"] != 0 and second15_data[symbol]["low"] != 999999:
            second15_data[symbol]["ready"] = True


def strategy_inside_bar(ctx):

    _SNAME = "Inside Bar Breakout"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    ohlc             = ctx["ohlc"]
    vol_percent      = ctx["vol_percent"]
    current_time     = ctx["current_time"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)
    _build_second15(symbol, ohlc, ltp, current_time)

    if not first15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "First-15 not ready")
        return
    if not second15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "Second-15 not ready (<09:45)")
        return

    c1 = first15_data[symbol]
    c2 = second15_data[symbol]

    if c1["high"] == 0 or c2["high"] == 0:
        _scan_record(_SNAME, symbol, "Candle data invalid")
        return

    yest = ctx["yest"]

    # ── True Inside Bar: today must open AND trade within yesterday's range ──
    # Yesterday is the "mother bar". If today's high > yest high OR
    # today's low < yest low, it has already broken out — not an inside bar.
    # Use latest_highs/lows (true running day high/low) — NOT ORB which freezes
    # at 10:15 and misses any price action after that.
    today_high = latest_highs.get(symbol, 0)
    today_low  = latest_lows.get(symbol, 999999)

    if today_high == 0 or today_low == 999999:
        _scan_record(_SNAME, symbol, "Today high/low not yet available")
        return

    if not (today_high <= yest["high"] and today_low >= yest["low"]):
        _scan_record(_SNAME, symbol,
            f"Today broke yesterday range: today={today_low:.2f}–{today_high:.2f} "
            f"yest={yest['low']:.2f}–{yest['high']:.2f}")
        return

    # ── Second15 must be inside First15 (compression confirmation) ──────
    if not (c2["high"] < c1["high"] and c2["low"] > c1["low"]):
        _scan_record(_SNAME, symbol,
            f"Not inside bar (c2={c2['low']:.2f}–{c2['high']:.2f} c1={c1['low']:.2f}–{c1['high']:.2f})")
        return

    c2_range_pct = ((c2["high"] - c2["low"]) / ltp) * 100
    if c2_range_pct >= 0.6:
        _scan_record(_SNAME, symbol, f"c2 range {c2_range_pct:.2f}% >= 0.6% (not tight)")
        return

    #if vol_percent < 35:
     #   _scan_record(_SNAME, symbol, f"Vol {vol_percent:.0f}% < 35%")
      #  return

    if current_time > "14:00":
        _scan_record(_SNAME, symbol, "After 12:00 cutoff")
        return

    buy_signal = (
        299 <= ltp <= 6999 and
        ltp > c1["high"] and
        ema["ema7"] >= ema["ema20"] and
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT
    )
    sell_signal = (
        299 <= ltp <= 6999 and
        ltp < c1["low"] and
        ema["ema7"] <= ema["ema20"] and
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
    )

    if buy_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL BUY  ltp={ltp} c1_high={c1['high']:.2f}")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "Inside Bar BUY", _extra(ctx))
    elif sell_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL SELL ltp={ltp} c1_low={c1['low']:.2f}")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "Inside Bar SELL", _extra(ctx))
    else:
        if not (today_high <= yest["high"] and today_low >= yest["low"]):
            reason = (f"Today broke yest range: "
                      f"today={today_low:.2f}–{today_high:.2f} "
                      f"yest={yest['low']:.2f}–{yest['high']:.2f}")
        elif not (ltp > c1["high"]) and not (ltp < c1["low"]):
            reason = f"ltp {ltp} inside c1 range {c1['low']:.2f}–{c1['high']:.2f}"
        elif ltp > c1["high"] and ema["ema7"] < ema["ema20"]:
            reason = f"BUY EMA7<EMA20 ({ema['ema7']:.1f}<{ema['ema20']:.1f})"
        elif ltp < c1["low"] and ema["ema7"] > ema["ema20"]:
            reason = f"SELL EMA7>EMA20 ({ema['ema7']:.1f}>{ema['ema20']:.1f})"
        else:
            reason = f"No breakout yet ltp={ltp}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY 5 — VWAP RECLAIM (NEW)
# ==========================================
# Logic: Stock drops below VWAP, consolidates, then reclaims it with strong
#        volume. VWAP is the average price paid by all participants today.
#        A reclaim means institutional buyers are defending / accumulating.
#
# Why high win rate: VWAP is self-fulfilling — institutions use it as benchmark.
#   Reclaims with volume mean they are actively buying, not just drifting up.
#
# VWAP calculation: cumulative (price × volume) / cumulative volume,
#   reset at 09:15 each day. Updated every tick.
# ==========================================

vwap_state = {}
# symbol → {cum_pv: float, cum_vol: float, vwap: float,
#            below_count: int,   # consecutive candles below VWAP
#            above_count: int}   # consecutive candles above VWAP (for SELL)

def _update_vwap(symbol, ltp, volume):
    """Update VWAP for symbol with latest tick price and volume."""
    if symbol not in vwap_state:
        vwap_state[symbol] = {
            "cum_pv":      0.0,
            "cum_vol":     0.0,
            "vwap":        0.0,
            "below_count": 0,
            "above_count": 0
        }
    v = vwap_state[symbol]
    if volume > 0:
        v["cum_pv"]  += ltp * volume
        v["cum_vol"] += volume
        v["vwap"]     = v["cum_pv"] / v["cum_vol"]
    return v["vwap"]


def strategy_vwap_reclaim(ctx):

    _SNAME = "VWAP Reclaim"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    vol_percent      = ctx["vol_percent"]
    current_time     = ctx["current_time"]
    live_volume      = ctx["live_volume"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)

    if current_time < "09:45":
        _scan_record(_SNAME, symbol, "Before 09:45 (VWAP building)")
        return
    if current_time > "14:30":
        _scan_record(_SNAME, symbol, "After 13:30 cutoff")
        return
    #if vol_percent < 20:
     #   _scan_record(_SNAME, symbol, f"Vol {vol_percent:.0f}% < 20%")
      #  return

    vwap = _update_vwap(symbol, ltp, live_volume)
    if vwap == 0:
        _scan_record(_SNAME, symbol, "VWAP=0 (no volume yet)")
        return

    v = vwap_state[symbol]

    if ltp < vwap:
        v["below_count"] += 1
        v["above_count"]  = 0
    elif ltp > vwap:
        v["above_count"] += 1
        v["below_count"]  = 0

    buy_signal = (
        299 <= ltp <= 6999 and
        v["below_count"] >= 3 and
        ltp > vwap and
        ema["ema7"] >= ema["ema20"] and
        vol_percent >= 35 and
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT
    )
    sell_signal = (
        299 <= ltp <= 6999 and
        v["above_count"] >= 3 and
        ltp < vwap and
        ema["ema7"] <= ema["ema20"] and
        vol_percent >= 35 and
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
    )

    if buy_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL BUY  ltp={ltp} vwap={vwap:.2f}")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "VWAP Reclaim BUY", _extra(ctx))
        v["below_count"] = 0
    elif sell_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL SELL ltp={ltp} vwap={vwap:.2f}")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "VWAP Rejection SELL", _extra(ctx))
        v["above_count"] = 0
    else:
        if ltp > vwap and v["below_count"] < 3:
            reason = f"BUY: only {v['below_count']} below-ticks < 3 needed"
        elif ltp < vwap and v["above_count"] < 3:
            reason = f"SELL: only {v['above_count']} above-ticks < 3 needed"
        elif ltp > vwap and ema["ema7"] < ema["ema20"]:
            reason = f"BUY EMA7<EMA20 ({ema['ema7']:.1f}<{ema['ema20']:.1f})"
        elif ltp < vwap and ema["ema7"] > ema["ema20"]:
            reason = f"SELL EMA7>EMA20 ({ema['ema7']:.1f}>{ema['ema20']:.1f})"
        elif vol_percent < 35:
            reason = f"Vol {vol_percent:.0f}% < 35% for signal"
        else:
            reason = f"ltp={ltp:.1f} vwap={vwap:.1f} below={v['below_count']} above={v['above_count']}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY 6 — YESTERDAY LEVEL BREAKDOWN / BREAKOUT
# ==========================================
# SELL Setup (Strong Low / YL Breakdown):
#   • No gap-down open  → stock opened at or above YL (not already broken)
#   • LTP has now broken below YL  → fresh breakdown happening live
#   • LTP is close to today's intraday low (≤ 0.5% above day low) → price
#     is hugging the low, not bouncing — confirms sustained selling pressure
#   • Change% is -0.5% to -1.2% → bearish momentum but not exhausted
#   • LTP ≤ EMA20 → price is below medium-term average (downtrend context)
#
# BUY Setup (Strong High / YH Breakout) — mirror image:
#   • No gap-up open  → stock opened at or below YH (not already broken)
#   • LTP has now broken above YH  → fresh breakout happening live
#   • LTP is close to today's intraday high (≤ 0.5% below day high) → price
#     is hugging the high, not fading — confirms sustained buying pressure
#   • Change% is +0.5% to +1.2% → bullish momentum but not exhausted
#   • LTP ≥ EMA20 → price is above medium-term average (uptrend context)
# ==========================================

def strategy_yl_breakdown(ctx):
    #print(" In strategy_yl_breakdown")

    _SNAME = "YL Breakdown / YH Breakout"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ohlc             = ctx["ohlc"]
    yest             = ctx["yest"]
    ema              = ctx["ema"]
    change_pct       = ctx["change_percent"]
    vol_percent      = ctx["vol_percent"]
    current_time     = ctx["current_time"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)

    ema20 = ema.get("ema20", 0)
    #if not ema20:
     #   _scan_record(_SNAME, symbol, "Missing EMA20")
      #  return

    if current_time < "09:30":
        _scan_record(_SNAME, symbol, "Before 09:30")
        return
    if current_time > "14:30":
        _scan_record(_SNAME, symbol, "After 13:30 cutoff")
        return

    #if vol_percent < 30:
     #   _scan_record(_SNAME, symbol, f"Vol {vol_percent:.0f}% < 30%")
      #  return

    live_open = first15_data.get(symbol, {}).get("open", 0)  # actual 09:15 open
    day_high  = latest_highs.get(symbol, 0)                   # true intraday high
    day_low   = latest_lows.get(symbol, 999999)               # true intraday low

    if live_open == 0 or day_high == 0 or day_low == 999999:
        _scan_record(_SNAME, symbol, "OHLC data missing")
        return

    # ── Gap + Recovery filter ────────────────────────────────────────────
    # If stock gapped DOWN below YL at open AND has since recovered back UP
    # to/near YL → this is a recovery attempt, NOT a fresh breakdown.
    # Block SELL in this case — the breakdown energy is exhausted.
    # Criteria: open < YL (gap-down) AND day_high > YL (recovered above YL at some point)
    already_recovered_from_gap_down = (
        live_open < yest["low"] and day_high > yest["low"]
    )
    # Similarly for BUY: if stock gapped UP above YH then fell back below YH → faded
    already_faded_from_gap_up = (
        live_open > yest["high"] and day_low < yest["high"]
    )

    # ── SELL: YL Breakdown ──────────────────────────────────────────────
    # Stale guard: if day_low already went more than 0.5% below YL,
    # the breakdown happened earlier and this is a late stale entry.
    yl_already_broken_pct = ((yest["low"] - day_low) / yest["low"] * 100) if yest["low"] > 0 else 0
    yh_already_broken_pct = ((day_high - yest["high"]) / yest["high"] * 100) if yest["high"] > 0 else 0
    MAX_YL_STALE_PCT = 0.5

    sell_signal = (
        299 <= ltp <= 6999 and
        live_open >= yest["low"] and           # must have opened AT or ABOVE YL
        ltp <= yest["low"] and
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT and
        yl_already_broken_pct <= MAX_YL_STALE_PCT and
        day_low > 0 and ((ltp - day_low) / day_low * 100) <= 0.5 and
        -1.2 <= change_pct <= -0.5 and
        ltp <= ema20 and
        not already_recovered_from_gap_down    # not a gap-down recovery
    )
    # ── BUY: YH Breakout ────────────────────────────────────────────────
    buy_signal = (
        299 <= ltp <= 6999 and
        live_open <= yest["high"] and          # must have opened AT or BELOW YH
        ltp >= yest["high"] and
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT and
        yh_already_broken_pct <= MAX_YL_STALE_PCT and
        day_high > 0 and ((day_high - ltp) / day_high * 100) <= 0.5 and
        0.5 <= change_pct <= 1.2 and
        ltp >= ema20 and
        not already_faded_from_gap_up          # not a gap-up fade
    )

    if sell_signal:
        _scan_record(_SNAME, symbol,
            f"✅ SIGNAL SELL ltp={ltp} yl={yest['low']:.2f} chg={change_pct:.2f}%")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "YL Breakdown SELL", _extra(ctx))
    elif buy_signal:
        _scan_record(_SNAME, symbol,
            f"✅ SIGNAL BUY  ltp={ltp} yh={yest['high']:.2f} chg={change_pct:.2f}%")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "YH Breakout BUY", _extra(ctx))
    else:
        if not (ltp <= yest["low"] or ltp >= yest["high"]):
            reason = f"ltp {ltp:.1f} inside YL={yest['low']:.1f}–YH={yest['high']:.1f}"
        elif ltp <= yest["low"] and already_recovered_from_gap_down:
            reason = f"SELL blocked: gap-down open {live_open:.1f} < YL but recovered to {day_high:.1f}"
        elif ltp >= yest["high"] and already_faded_from_gap_up:
            reason = f"BUY blocked: gap-up open {live_open:.1f} > YH but faded to {day_low:.1f}"
        elif ltp <= yest["low"] and live_open < yest["low"]:
            reason = f"SELL: gap-down open {live_open:.1f} < YL {yest['low']:.1f}"
        elif ltp >= yest["high"] and live_open > yest["high"]:
            reason = f"BUY: gap-up open {live_open:.1f} > YH {yest['high']:.1f}"
        elif ltp <= yest["low"] and not (-1.2 <= change_pct <= -0.5):
            reason = f"SELL: change {change_pct:.2f}% outside -1.2–-0.5%"
        elif ltp >= yest["high"] and not (0.5 <= change_pct <= 1.2):
            reason = f"BUY: change {change_pct:.2f}% outside 0.5–1.2%"
        elif ltp <= yest["low"] and ltp > ema20:
            reason = f"SELL: ltp {ltp:.1f} > EMA20 {ema20:.1f}"
        elif ltp >= yest["high"] and ltp < ema20:
            reason = f"BUY: ltp {ltp:.1f} < EMA20 {ema20:.1f}"
        elif ltp <= yest["low"] and dist_from_yl_pct > MAX_OVEREXTENSION_PCT:
            reason = f"SELL: overextended {dist_from_yl_pct:.2f}% > {MAX_OVEREXTENSION_PCT}%"
        elif ltp >= yest["high"] and dist_from_yh_pct > MAX_OVEREXTENSION_PCT:
            reason = f"BUY: overextended {dist_from_yh_pct:.2f}% > {MAX_OVEREXTENSION_PCT}%"
        else:
            reason = f"ltp={ltp:.1f} yl={yest['low']:.1f} yh={yest['high']:.1f} chg={change_pct:.2f}%"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY 7 — GAP + FIRST 15 MIN BREAKOUT
# ==========================================
# Logic:
#   Gap Up  (<= +1.5%): first 15min candle must be GREEN (close > open)
#            → once price breaks ABOVE first15 high  → BUY
#
#   Gap Down (<= -1.5%): first 15min candle must be RED (close < open)
#            → once price breaks BELOW first15 low   → SELL
#
# Filters:
#   • Gap must be 0.1%–1.5% in magnitude (not flat, not overextended)
#   • First 15min candle range must be < 2% (tight consolidation — not volatile)
#   • First 15min candle must confirm gap direction (green for gap-up, red for gap-down)
#   • Price must actually break out of the first15 range on current tick
#   • LTP price range: 299–6999
#   • Entry allowed only after 09:30 (first15 window closed) until 13:00
# ==========================================
def strategy_gap_first15(ctx):

    _SNAME = "Gap+First15 Breakout"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    current_time     = ctx["current_time"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]

    _scan_start(_SNAME, current_time)

    # ── Time gate: only after First-15 is complete ──────────────────────
    if current_time < "09:30":
        _scan_record(_SNAME, symbol, "First-15 window not closed yet")
        return

    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 13:00 cutoff")
        return

    # ── First-15 data must be ready ─────────────────────────────────────
    if not first15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "First-15 not ready")
        return

    f = first15_data[symbol]

    if f["open"] == 0 or f["high"] == 0 or f["low"] == 999999:
        _scan_record(_SNAME, symbol, "First-15 OHLC invalid")
        return

    # ── Use first15 open as the TRUE day open (09:15 candle open) ───────
    # ctx["gap_percent"] uses the current 1-min candle's ohlc["open"] which
    # changes every minute — NOT the actual market open price.
    # first15_data["open"] is always the 09:15 opening tick → correct gap.
    yest_close = ctx["yest"]["close"]
    if yest_close == 0:
        _scan_record(_SNAME, symbol, "Yesterday close = 0")
        return

    true_day_open = f["open"]   # actual 09:15 market open
    true_gap_pct  = ((true_day_open - yest_close) / yest_close) * 100

    # ── Gap filter: must be between 0.3% and 1.5% (both directions) ─────
    abs_gap = abs(true_gap_pct)
    if abs_gap < 0.3 or abs_gap > 1.5:
        _scan_record(_SNAME, symbol,
            f"Gap {true_gap_pct:.2f}% outside 0.3–1.5% range")
        return

    # ── F15 must be a CONSOLIDATION after the gap, not a reversal ─────────
    # If price gapped DOWN but the F15 high is well above the gap-open,
    # it means price rallied back up during F15 — that's a recovery attempt,
    # not consolidation. We want a tight F15 range that stays near the gap level.
    #
    # Consolidation rule: F15 range (high-low) must be <= 60% of the gap size.
    # Example: gap of -1.5% → F15 range must be <= 0.9% of price.
    # GRASIM: gap = 1.49%, F15 range = (2685-2674.8)/2677.7 = 0.38% ✅ passes
    # But F15 high (2685) is 0.27% above F15 open (2677.7) — price bounced up.
    #
    # Stronger check: for SELL, F15 high must be within 0.3% of F15 open.
    # This ensures the 15-min candle stayed flat/down, not a spike-and-fail.
    f15_range_pct = ((f["high"] - f["low"]) / f["open"]) * 100
    gap_size_pct  = abs_gap

    if f15_range_pct > gap_size_pct * 0.8:
        _scan_record(_SNAME, symbol,
            f"F15 range {f15_range_pct:.2f}% too wide vs gap {gap_size_pct:.2f}% — not consolidation")
        return

    # For SELL: F15 must not have bounced significantly above F15 open
    # (bounce > 0.3% above open = recovery attempt, not consolidation)
    f15_bounce_pct = ((f["high"] - f["open"]) / f["open"]) * 100
    f15_drop_pct   = ((f["open"] - f["low"])  / f["open"]) * 100

    if true_gap_pct < 0 and f15_bounce_pct > 0.3:
        _scan_record(_SNAME, symbol,
            f"SELL: F15 bounced {f15_bounce_pct:.2f}% above open — recovery, not consolidation")
        return

    if true_gap_pct > 0 and f15_drop_pct > 0.3:
        _scan_record(_SNAME, symbol,
            f"BUY: F15 dropped {f15_drop_pct:.2f}% below open — fading, not consolidation")
        return

    # ── Move from yesterday close to first15 extreme must be < 2.5% ──────
    move_to_f15_high = ((f["high"] - yest_close) / yest_close) * 100
    move_to_f15_low  = ((yest_close - f["low"])  / yest_close) * 100

    if true_gap_pct > 0 and move_to_f15_high >= 2.5:
        _scan_record(_SNAME, symbol,
            f"Gap-up: yclose→f15_high {move_to_f15_high:.2f}% >= 2.5%")
        return
    if true_gap_pct < 0 and move_to_f15_low >= 2.5:
        _scan_record(_SNAME, symbol,
            f"Gap-down: yclose→f15_low {move_to_f15_low:.2f}% >= 2.5%")
        return

    # ── Price range filter ───────────────────────────────────────────────
    if not (299 <= ltp <= 6999):
        _scan_record(_SNAME, symbol, f"LTP {ltp} out of 299–6999 range")
        return

    # ── Candle direction confirmation + breakout ─────────────────────────
    candle_is_green = f["close"] > f["open"]   # gap-up confirmation
    candle_is_red   = f["close"] < f["open"]   # gap-down confirmation

    # ── BUY: Gap Up + Green First15 + Break above First15 High ──────────
    # ── Max entry slippage: reject if price already ran >0.5% past F15 level ──
    MAX_F15_CHASE_PCT = 0.5
    buy_chase_pct  = ((ltp - f["high"]) / f["high"] * 100) if ltp > f["high"] else 0
    sell_chase_pct = ((f["low"] - ltp)  / f["low"]  * 100) if ltp < f["low"]  else 0

    # ── Day-range proximity filter ────────────────────────────────────────
    # SELL: if the day's intraday low is already >1% below F15 low, the
    #       initial breakdown already happened and price has since bounced
    #       back — this is a re-test, not a fresh breakdown. Block entry.
    # BUY:  same logic — if day high is >1% above F15 high, initial breakout
    #       already ran and price pulled back to F15 high level again.
    day_high = latest_highs.get(symbol, ltp)
    day_low  = latest_lows.get(symbol,  ltp)

    f15_low_already_broken_by = ((f["low"] - day_low) / f["low"] * 100)   # +ve = day went lower
    f15_high_already_broken_by = ((day_high - f["high"]) / f["high"] * 100)  # +ve = day went higher

    MAX_PRIOR_BREAK_PCT = 0.75   # if price already broke >0.75% past F15 level and bounced back → skip

    buy_signal = (
        true_gap_pct > 0 and
        candle_is_green and
        ltp > f["high"] and
        buy_chase_pct <= MAX_F15_CHASE_PCT and
        f15_high_already_broken_by <= MAX_PRIOR_BREAK_PCT and
        ((day_high - f["high"]) / f["high"] * 100) <= MAX_PRIOR_BREAK_PCT and  # day hasn't already ran >0.75% past F15 high on a spike
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT
    )

    # ── SELL: Gap Down + Red First15 + Break below First15 Low ──────────
    sell_signal = (
        true_gap_pct < 0 and
        candle_is_red and
        ltp < f["low"] and
        sell_chase_pct <= MAX_F15_CHASE_PCT and
        f15_low_already_broken_by <= MAX_PRIOR_BREAK_PCT and    # breakdown not already done+recovered
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
    )

    if buy_signal:
        _scan_record(_SNAME, symbol,
            f"✅ SIGNAL BUY  ltp={ltp} f15_high={f['high']:.2f} gap={true_gap_pct:.2f}%")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "Gap+F15 BUY", _extra(ctx))

    elif sell_signal:
        _scan_record(_SNAME, symbol,
            f"✅ SIGNAL SELL ltp={ltp} f15_low={f['low']:.2f} gap={true_gap_pct:.2f}%")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "Gap+F15 SELL", _extra(ctx))

    else:
        # Detailed skip reason for diagnostics
        if abs_gap < 0.1 or abs_gap > 1.5:
            reason = f"Gap {true_gap_pct:.2f}% outside 0.1–1.5%"
        elif true_gap_pct > 0 and move_to_f15_high >= 2.0:
            reason = f"Gap-up: yclose→f15_high {move_to_f15_high:.2f}% >= 2%"
        elif true_gap_pct < 0 and move_to_f15_low >= 2.0:
            reason = f"Gap-down: yclose→f15_low {move_to_f15_low:.2f}% >= 2%"
        elif true_gap_pct > 0 and not candle_is_green:
            reason = f"Gap-up but F15 candle RED (open={f['open']:.2f} close={f['close']:.2f})"
        elif true_gap_pct < 0 and not candle_is_red:
            reason = f"Gap-down but F15 candle GREEN (open={f['open']:.2f} close={f['close']:.2f})"
        elif true_gap_pct > 0 and candle_is_green and ltp <= f["high"]:
            reason = f"BUY: ltp {ltp} not above f15_high {f['high']:.2f}"
        elif true_gap_pct < 0 and candle_is_red and ltp >= f["low"]:
            reason = f"SELL: ltp {ltp} not below f15_low {f['low']:.2f}"
        elif true_gap_pct > 0 and buy_chase_pct > MAX_F15_CHASE_PCT:
            reason = f"BUY: already {buy_chase_pct:.2f}% past F15 high {f['high']:.2f} — chasing"
        elif true_gap_pct < 0 and sell_chase_pct > MAX_F15_CHASE_PCT:
            reason = f"SELL: already {sell_chase_pct:.2f}% past F15 low {f['low']:.2f} — chasing"
        elif true_gap_pct > 0 and f15_high_already_broken_by > MAX_PRIOR_BREAK_PCT:
            reason = f"BUY: day high already {f15_high_already_broken_by:.2f}% above F15 high — breakout done, now a pullback re-test"
        elif true_gap_pct < 0 and f15_low_already_broken_by > MAX_PRIOR_BREAK_PCT:
            reason = f"SELL: day low already {f15_low_already_broken_by:.2f}% below F15 low — breakdown done, now a bounce re-test"
        elif true_gap_pct > 0 and dist_from_yh_pct > MAX_OVEREXTENSION_PCT:
            reason = f"BUY: overextended {dist_from_yh_pct:.2f}% > {MAX_OVEREXTENSION_PCT}% above YH"
        elif true_gap_pct < 0 and dist_from_yl_pct > MAX_OVEREXTENSION_PCT:
            reason = f"SELL: overextended {dist_from_yl_pct:.2f}% > {MAX_OVEREXTENSION_PCT}% below YL"
        else:
            reason = f"No signal: gap={true_gap_pct:.2f}% ltp={ltp} f15={f['low']:.2f}–{f['high']:.2f}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY 8 — 15-MIN INSIDE RANGE BREAKOUT
# ==========================================
# Candle 1 (09:15–09:29) = mother bar / reference range
# Candles 2, 3, 4 (09:30, 09:45, 10:00) must ALL stay strictly inside C1
#   → 45 minutes of compression/consolidation before breakout
# BUY when LTP breaks above C1 high | SELL when LTP breaks below C1 low
#
# Tight filters to cut noise:
#   1. C1 range 0.3%–1.5%        → not flat, not a wild gap open
#   2. Gap ≤ 1.5%                → exclude gap-and-go stocks
#   3. All 3 inner candles strictly inside C1 (no edge touching)
#   4. Compression ratio < 60%   → inner H-L span < 60% of C1 — truly coiled
#   5. Freshness ≤ 0.4%          → no chasing past the breakout level
#   6. Volume ≥ 40% of yesterday → real participation at breakout time
#   7. EMA7 ≥ EMA20 for BUY, EMA7 ≤ EMA20 for SELL
#   8. Valid only 10:15–13:00
#   9. Overextension guard
# ==========================================

# ── Third 15-min candle builder: 09:45–09:59 ────────────────────────────
third15_data = {}

def _build_third15(symbol, ltp, current_time):
    if symbol not in third15_data:
        if "09:45" <= current_time < "10:00":
            third15_data[symbol] = {"high": ltp, "low": ltp, "ready": False}
        else:
            # Started after window — will be seeded from API; don't mark ready yet
            third15_data[symbol] = {"high": 0, "low": 999999, "ready": False}
    if "09:45" <= current_time < "10:00":
        third15_data[symbol]["high"] = max(third15_data[symbol]["high"], ltp)
        third15_data[symbol]["low"]  = min(third15_data[symbol]["low"],  ltp)
    if current_time >= "10:00":
        # Only mark ready if we have real data (not the 0/999999 placeholder)
        if third15_data[symbol]["high"] != 0 and third15_data[symbol]["low"] != 999999:
            third15_data[symbol]["ready"] = True

# ── Fourth 15-min candle builder: 10:00–10:14 ───────────────────────────
fourth15_data = {}

def _build_fourth15(symbol, ltp, current_time):
    if symbol not in fourth15_data:
        if "10:00" <= current_time < "10:15":
            fourth15_data[symbol] = {"high": ltp, "low": ltp, "ready": False}
        else:
            # Started after window — will be seeded from API; don't mark ready yet
            fourth15_data[symbol] = {"high": 0, "low": 999999, "ready": False}
    if "10:00" <= current_time < "10:15":
        fourth15_data[symbol]["high"] = max(fourth15_data[symbol]["high"], ltp)
        fourth15_data[symbol]["low"]  = min(fourth15_data[symbol]["low"],  ltp)
    if current_time >= "10:15":
        # Only mark ready if we have real data (not the 0/999999 placeholder)
        if fourth15_data[symbol]["high"] != 0 and fourth15_data[symbol]["low"] != 999999:
            fourth15_data[symbol]["ready"] = True


def strategy_15m_inside_break(ctx):

    _SNAME           = "15m Inside Range Break"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    current_time     = ctx["current_time"]
    vol_percent      = ctx["vol_percent"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]

    _scan_start(_SNAME, current_time)

    # Build C3 and C4 on every tick
    _build_third15(symbol,  ltp, current_time)
    _build_fourth15(symbol, ltp, current_time)

    # Time gate: all 4 candles must be fully formed
    if current_time < "10:15":
        _scan_record(_SNAME, symbol, "Waiting — 4 candles not yet complete")
        return
    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 13:00 cutoff")
        return

    # All candle data must be ready
    if not first15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "C1 not ready"); return
    if not second15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "C2 not ready"); return
    if not third15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "C3 not ready"); return
    if not fourth15_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "C4 not ready"); return

    c1 = first15_data[symbol]
    c2 = second15_data[symbol]
    c3 = third15_data[symbol]
    c4 = fourth15_data[symbol]

    c1_high = c1["high"]
    c1_low  = c1["low"]

    if c1_high == 0 or c1_low == 999999:
        _scan_record(_SNAME, symbol, "C1 data invalid"); return

    # ── Filter 1: C1 range 0.3%–1.5% ────────────────────────────────────
    c1_range_pct = ((c1_high - c1_low) / c1_low) * 100
    if not (0.3 <= c1_range_pct <= 1.5):
        _scan_record(_SNAME, symbol, f"C1 range {c1_range_pct:.2f}% outside 0.3–1.5%")
        return

    # ── Filter 2: Gap must be mild (use true day open from first15) ───────
    yest = ctx["yest"]
    true_gap_pct = ((c1["open"] - yest["close"]) / yest["close"]) * 100
    if abs(true_gap_pct) > 1.5:
        _scan_record(_SNAME, symbol, f"Gap {true_gap_pct:.2f}% > 1.5% — excluded")
        return

    # ── Filter 3: All 3 inner candles strictly inside C1 ─────────────────
    if not (c2["high"] < c1_high and c2["low"] > c1_low):
        _scan_record(_SNAME, symbol,
            f"C2 broke C1: {c2['low']:.1f}–{c2['high']:.1f} vs {c1_low:.1f}–{c1_high:.1f}")
        return
    if not (c3["high"] < c1_high and c3["low"] > c1_low):
        _scan_record(_SNAME, symbol,
            f"C3 broke C1: {c3['low']:.1f}–{c3['high']:.1f} vs {c1_low:.1f}–{c1_high:.1f}")
        return
    if not (c4["high"] < c1_high and c4["low"] > c1_low):
        _scan_record(_SNAME, symbol,
            f"C4 broke C1: {c4['low']:.1f}–{c4['high']:.1f} vs {c1_low:.1f}–{c1_high:.1f}")
        return

    # ── Filter 4: Compression quality — inner span < 60% of C1 ──────────
    inner_high     = max(c2["high"], c3["high"], c4["high"])
    inner_low      = min(c2["low"],  c3["low"],  c4["low"])
    inner_range    = inner_high - inner_low
    c1_range       = c1_high - c1_low
    compress_ratio = inner_range / c1_range

    if compress_ratio >= 0.60:
        _scan_record(_SNAME, symbol,
            f"Weak compression: inner={compress_ratio*100:.0f}% of C1 (need <60%)")
        return


    if vol_percent < 15:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 15% of yesterday")
        return
    MAX_CHASE_PCT  = 0.4
    buy_chase_pct  = ((ltp - c1_high) / c1_high * 100) if ltp > c1_high else 0
    sell_chase_pct = ((c1_low - ltp)  / c1_low  * 100) if ltp < c1_low  else 0

    # ── BUY signal ────────────────────────────────────────────────────────
    buy_signal = (
        299 <= ltp <= 6999               and
        ltp > c1_high                    and
        buy_chase_pct <= MAX_CHASE_PCT   and
        ema["ema7"] >= ema["ema20"]      and
        dist_from_yh_pct <= MAX_OVEREXTENSION_PCT
    )

    # ── SELL signal ───────────────────────────────────────────────────────
    sell_signal = (
        299 <= ltp <= 6999               and
        ltp < c1_low                     and
        sell_chase_pct <= MAX_CHASE_PCT  and
        ema["ema7"] <= ema["ema20"]      and
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
    )

    if buy_signal:
        _scan_record(_SNAME, symbol,
            f"✅ BUY  ltp={ltp} c1_high={c1_high:.2f} "
            f"chase={buy_chase_pct:.2f}% compress={compress_ratio*100:.0f}%")
        place_trade(symbol, "BUY",  ltp, ctx["tick"], "15m InsideBreak BUY",  _extra(ctx))

    elif sell_signal:
        _scan_record(_SNAME, symbol,
            f"✅ SELL ltp={ltp} c1_low={c1_low:.2f} "
            f"chase={sell_chase_pct:.2f}% compress={compress_ratio*100:.0f}%")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "15m InsideBreak SELL", _extra(ctx))

    else:
        if not (299 <= ltp <= 6999):
            reason = f"LTP {ltp} out of range"
        elif ltp > c1_high and buy_chase_pct > MAX_CHASE_PCT:
            reason = f"BUY: chasing {buy_chase_pct:.2f}% past C1H {c1_high:.2f}"
        elif ltp < c1_low and sell_chase_pct > MAX_CHASE_PCT:
            reason = f"SELL: chasing {sell_chase_pct:.2f}% past C1L {c1_low:.2f}"
        elif ltp > c1_high and ema["ema7"] < ema["ema20"]:
            reason = f"BUY: EMA7({ema['ema7']:.1f}) < EMA20({ema['ema20']:.1f})"
        elif ltp < c1_low and ema["ema7"] > ema["ema20"]:
            reason = f"SELL: EMA7({ema['ema7']:.1f}) > EMA20({ema['ema20']:.1f})"
        elif c1_low <= ltp <= c1_high:
            reason = f"ltp {ltp:.1f} still inside C1 {c1_low:.1f}–{c1_high:.1f}"
        else:
            reason = f"No breakout: ltp={ltp:.1f} C1={c1_low:.1f}–{c1_high:.1f}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY DISPATCHER
# ==========================================
def strategy(token, tick):

    ctx = build_context(token, tick)
    if not ctx:
        return

    # Strategy 1: ORB Breakout (fixed — volume + range filter + both sides)
    strategy_orb(ctx)

    # Strategy 2: Open=Low / Open=High first-15m break (fixed — float tolerance)
    strategy_open_low(ctx)

    # Strategy 3: EMA Pullback in trend (new — buy value in uptrend)
    strategy_ema_pullback(ctx)

    # Strategy 4: Inside Bar Breakout (new — compression → expansion)
    strategy_inside_bar(ctx)

    # Strategy 5: VWAP Reclaim / Rejection (new — institutional level)
    strategy_vwap_reclaim(ctx)

    # Strategy 6: Yesterday Level Breakdown / Breakout (YL break SELL, YH break BUY)
    strategy_yl_breakdown(ctx)

    # Strategy 7: Gap Up/Down + First 15 Min Candle Direction + Breakout
    strategy_gap_first15(ctx)

    # Strategy 8: 15-Min Inside Range Breakout (3-candle compression → expansion)
    strategy_15m_inside_break(ctx)



# ==========================================
# POSITION MONITOR  (LIVE MODE)
# ==========================================
def position_manager():
    global daily_pnl, trading_enabled

    while True:
        try:
            positions = kite.positions()["net"]
            algo_symbols = set(trades_taken.keys())

            # Only sum PnL for positions this algo opened on NSE equity.
            # Do NOT include NFO/BFO positions — they are your manual trades.
            daily_pnl = sum(
                p["pnl"] for p in positions
                if p.get("exchange") == "NSE"
                and p["tradingsymbol"] in algo_symbols
                and p["quantity"] != 0
            )

            # ── Daily Max Loss Circuit Breaker ───────────────────────────
            if trading_enabled:
                # Approximate capital at risk: avg entry * qty * open positions
                open_positions = [p for p in positions if p["quantity"] != 0]
                if open_positions:
                    avg_value = sum(
                        abs(p["quantity"]) * p["average_price"]
                        for p in open_positions
                    ) / len(open_positions)
                else:
                    avg_value = 50000   # fallback reference capital

                max_loss_rupees = avg_value * (DAILY_MAX_LOSS_PERCENT / 100)

                if daily_pnl <= -abs(max_loss_rupees):
                    trading_enabled = False
                    msg = (
                        f"🚨 DAILY MAX LOSS REACHED | PnL: {daily_pnl:.2f} | "
                        f"Limit: -{abs(max_loss_rupees):.2f} | Trading HALTED"
                    )
                    print(Fore.RED + msg + ts())
                    write_log(msg)

        except Exception as e:
            print(Fore.RED + f"position_manager error: {e}" + ts())

        time.sleep(5)

# ==========================================
# DAILY SUMMARY
# ==========================================
def daily_summary():
    print("\n" + "="*60)
    print(Fore.CYAN + "📊 DAILY TRADING SUMMARY" + ts())
    print("="*60)

    total_t    = trade_statistics["total_trades"]
    buy_t      = trade_statistics["buy_trades"]
    sell_t     = trade_statistics["sell_trades"]
    sl_h       = trade_statistics["sl_hits"]
    tsl_h      = trade_statistics["trailing_sl_hits"]
    tgt_h      = trade_statistics["target_hits"]
    total_wins = sum(d["wins"] for d in strategy_stats.values())
    total_loss = sum(d["loss"] for d in strategy_stats.values())
    win_rate   = round(total_wins / max(total_t, 1) * 100, 1)
    pnl_clean  = round(daily_pnl, 2)

    summary = (
        f"Total Trades   : {total_t}  (Buy: {buy_t}  Sell: {sell_t})\n"
        f"Wins / Losses  : {total_wins} W / {total_loss} L  |  Win Rate: {win_rate}%\n"
        f"Stop Loss Hits : {sl_h}\n"
        f"Trailing SL    : {tsl_h}\n"
        f"Target Hits    : {tgt_h}\n"
        f"Final PnL      : {pnl_clean:+.2f}\n"
    )

    print(summary + ts())
    write_log("===== DAILY SUMMARY =====")
    write_log(summary)
    print("\n" + "="*60)
    print(Fore.CYAN + "📊 STRATEGY WISE SUMMARY" + ts())
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
         + ts())

# ==========================================
# AUTO SQUARE OFF
# ==========================================
def auto_square_off():
    """Waits 60s after startup, then monitors for square off time."""
    global daily_pnl
    time.sleep(60)   # wait for WebSocket + positions to load

    while True:

        if datetime.now().strftime("%H:%M") >= SQUARE_OFF_TIME:

            open_count = sum(1 for p in paper_positions.values() if p.get("status") == "OPEN")
            if TRADING_MODE == "PAPER" and open_count == 0:
                print(Fore.YELLOW + "⏭️  Square off: no open positions — skipping" + ts())
                daily_summary()
                break

            print(Fore.MAGENTA + "\n🔔 AUTO SQUARE OFF TRIGGERED" + ts())

            # =====================================
            # LIVE MODE SQUARE OFF
            # =====================================
            if TRADING_MODE == "LIVE":

                try:
                    # Step 1: Cancel all pending SL and Target orders first
                    # to avoid double-position when market exit fills
                    open_orders = kite.orders()
                    for symbol, trade in list(trades_taken.items()):
                        if not isinstance(trade, dict):
                            continue
                        for oid_key in ("sl", "target"):
                            oid = trade.get(oid_key)
                            if not oid:
                                continue
                            order = next(
                                (o for o in open_orders if o["order_id"] == oid), None
                            )
                            if order and order["status"] in ("OPEN", "TRIGGER PENDING"):
                                try:
                                    kite.cancel_order(variety="regular", order_id=oid)
                                    print(Fore.YELLOW + f"Cancelled pending {oid_key} order for {symbol}" + ts())
                                except Exception as ce:
                                    print(Fore.RED + f"Could not cancel {oid_key} for {symbol}: {ce}" + ts())

                    time.sleep(1)   # let cancellations process

                    # Step 2: Square off ONLY the NSE equity positions this algo opened.
                    # kite.positions()["net"] returns ALL exchanges including NFO.
                    # We filter strictly to:
                    #   a) exchange == NSE  (never touch NFO/BFO/CDS)
                    #   b) tradingsymbol is in trades_taken  (only algo's own trades)
                    # This guarantees your manual NFO positions are never touched.
                    positions = kite.positions()["net"]
                    algo_symbols = set(trades_taken.keys())

                    for pos in positions:
                        # ── Safety filter 1: NSE equity only ──────────────
                        if pos.get("exchange") != "NSE":
                            print(Fore.CYAN +
                                f"⏭️  Skipping {pos['tradingsymbol']} "
                                f"(exchange={pos.get('exchange')} — not touched by algo)"
                             + ts())
                            continue

                        # ── Safety filter 2: only algo's own symbols ───────
                        if pos["tradingsymbol"] not in algo_symbols:
                            print(Fore.CYAN +
                                f"⏭️  Skipping {pos['tradingsymbol']} "
                                f"(not opened by this algo)"
                             + ts())
                            continue

                        if pos["quantity"] == 0:
                            continue

                        sq_side = (
                            kite.TRANSACTION_TYPE_SELL if pos["quantity"] > 0
                            else kite.TRANSACTION_TYPE_BUY
                        )
                        kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange="NSE",
                            tradingsymbol=pos["tradingsymbol"],
                            transaction_type=sq_side,
                            quantity=abs(pos["quantity"]),
                            order_type=kite.ORDER_TYPE_MARKET,
                            product=kite.PRODUCT_MIS
                        )
                        msg = f"✅ LIVE SQUARE OFF: {pos['tradingsymbol']} qty={abs(pos['quantity'])}"
                        print(Fore.YELLOW + msg + ts())
                        write_log(msg)

                except Exception as e:
                    print(Fore.RED + f"LIVE Square off error: {e}" + ts())
                    write_log(f"LIVE SQUARE OFF ERROR: {e}")

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

                    pos["status"]  = "CLOSED"
                    pos["net_pnl"] = net_pnl   # store for cache rebuild on restart
                    daily_pnl += net_pnl
                    trade_statistics["total_trades"] += 1
                    if side == "BUY": trade_statistics["buy_trades"]  += 1
                    else:             trade_statistics["sell_trades"] += 1
                    strat_sq = pos.get("strategy", "Unknown")
                    if strat_sq not in strategy_stats:
                        strategy_stats[strat_sq] = {"trades":0,"wins":0,"loss":0,"pnl":0}
                    strategy_stats[strat_sq]["trades"] += 1
                    strategy_stats[strat_sq]["pnl"]    += net_pnl
                    if net_pnl > 0: strategy_stats[strat_sq]["wins"] += 1
                    else:           strategy_stats[strat_sq]["loss"] += 1
                    save_paper_positions_cache()

                    print(Fore.MAGENTA +
                        f"PAPER SQUARE OFF | {symbol} | {side} | "
                        f"Entry:{entry} | Exit:{ltp} | "
                        f"LTP:{ltp} {_chg(ltp, pos.get('y_close', 0))} | "
                        f"Points:{points:.2f} | Net PnL:{net_pnl:.2f}" + ts())

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

        if datetime.now().strftime("%H:%M") > SQUARE_OFF_TIME:
            break   # all positions closed — stop printing

        print("\n" + "="*80)
        print(Fore.CYAN + "📊 15 MIN POSITION SUMMARY" + ts())
        print("="*80)

        open_pnl   = 0.0
        open_count = 0

        for symbol in paper_positions:

            pos = paper_positions[symbol]

            if pos["status"] != "OPEN":
                continue

            ltp = latest_prices.get(symbol)

            if not ltp:
                continue

            entry = pos["entry"]
            side  = pos["side"]
            qty   = pos["qty"]
            strat = pos.get("strategy", "")

            points    = (ltp - entry) if side == "BUY" else (entry - ltp)
            gross_pnl = points * qty
            net_pnl   = gross_pnl - (BROKERAGE_PER_ORDER * 2)

            open_pnl   += net_pnl
            open_count += 1

            color = Fore.GREEN if net_pnl >= 0 else Fore.RED

            print(color +
                f"{symbol} | {strat} | {side} | Entry:{entry} | "
                f"LTP:{ltp} | SL:{pos['sl']} | "
                f"Target:{pos['target']} | "
                f"Points:{points:.2f} | "
                f"NetPnL:{net_pnl:.2f}" + ts())

        closed_pnl   = daily_pnl
        total_pnl    = open_pnl + closed_pnl
        closed_count = sum(1 for p in paper_positions.values() if p["status"] == "CLOSED")
        print("="*80)
        print(Fore.YELLOW + f"   Open  : {open_count:>3}  |  Open PnL   : {open_pnl:>+10.2f}" + ts())
        print(Fore.YELLOW + f"   Closed: {closed_count:>3}  |  Closed PnL : {closed_pnl:>+10.2f}" + ts())
        total_color = Fore.GREEN if total_pnl >= 0 else Fore.RED
        print(total_color  + f"   ── TOTAL PnL ──────────────────────────  {total_pnl:>+10.2f}" + ts())
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

    strategy_stats.clear()   # ← FIXED: reset per-day so multi-day replay doesn't accumulate
    replay_cum_volume.clear()
    replay_15m_data.clear()
    replay_1h_data.clear()

    daily_pnl = 0
    trades_taken = {}
    paper_positions = {}
    orb_data.clear()
    first15_data.clear()
    second15_data.clear()
    third15_data.clear()
    fourth15_data.clear()
    latest_prices.clear()
    latest_highs.clear()
    latest_lows.clear()
    ema_pullback_state.clear()
    vwap_state.clear()

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
        print("❌ No data for replay date" + ts())
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

            open_p  = row["open"]
            high    = row["high"]
            low     = row["low"]
            close_p = row["close"]
            volume  = row["volume"]

            # =========================================================
            # REPLAY TICK SIMULATION — matches live WebSocket behaviour
            # ---------------------------------------------------------
            # Live WebSocket sends many ticks per minute. Each tick can
            # hit intraday highs or lows that trigger entries AND exits.
            # A single close-only tick misses SELL entries (which need
            # price to touch the candle low) and SL/target hits that
            # occur intra-candle.
            #
            # Solution: simulate 3 ordered ticks per candle —
            #   Tick 1 → open  (sets direction context)
            #   Tick 2 → high  (triggers BUY entries + BUY targets + SELL SLs)
            #   Tick 3 → low   (triggers SELL entries + SELL targets + BUY SLs)
            #   Tick 4 → close (final price for trailing + next-candle context)
            #
            # latest_highs / latest_lows are updated cumulatively so
            # strategies like YL Breakdown that need day_high / day_low
            # work exactly as they do in live mode.
            # =========================================================
            if symbol not in latest_highs:
                latest_highs[symbol] = high
                latest_lows[symbol]  = low
            else:
                latest_highs[symbol] = max(latest_highs[symbol], high)
                latest_lows[symbol]  = min(latest_lows[symbol],  low)

            # ── Update cumulative volume + 15m/1h candles BEFORE ticks fire ──
            # This ensures build_context() sees the correct values when
            # strategy() is called on the high/low/close ticks below.
            replay_update_intraday_state(
                symbol, current_time, open_p, high, low, close_p, volume
            )

            def _make_tick(price):
                return {
                    "instrument_token": token,
                    "last_price": price,
                    "ohlc": {
                        "open":  open_p,
                        "high":  high,
                        "low":   low,
                        "close": close_p
                    },
                    "volume_traded": volume,
                    "volume":        volume,
                    "date": current_time
                }

            def _replay_exit_check(price_for_trail):
                """Check SL/target for open position using candle high+low."""
                global daily_pnl   # daily_pnl is module-level global; nested functions must declare it
                if symbol not in paper_positions:
                    return False
                pos = paper_positions[symbol]
                if pos["status"] != "OPEN":
                    return False

                entry     = pos["entry"]
                side      = pos["side"]
                qty       = pos["qty"]
                exit_price  = None
                exit_reason = None

                if side == "BUY":
                    if low <= pos["sl"]:
                        exit_price  = pos["sl"]
                        exit_reason = "SL HIT"
                    elif high >= pos["target"]:
                        exit_price  = pos["target"]
                        exit_reason = "TARGET HIT"
                elif side == "SELL":
                    if high >= pos["sl"]:
                        exit_price  = pos["sl"]
                        exit_reason = "SL HIT"
                    elif low <= pos["target"]:
                        exit_price  = pos["target"]
                        exit_reason = "TARGET HIT"

                if exit_price is not None:
                    points = (exit_price - entry) if side == "BUY" else (entry - exit_price)
                    gross  = points * qty
                    net    = gross - (BROKERAGE_PER_ORDER * 2)
                    strategy_name = pos.get("strategy", "Unknown")
                    if strategy_name not in strategy_stats:
                        strategy_stats[strategy_name] = {"trades":0,"wins":0,"loss":0,"pnl":0}
                    strategy_stats[strategy_name]["pnl"] += net
                    if net > 0:
                        strategy_stats[strategy_name]["wins"] += 1
                    else:
                        strategy_stats[strategy_name]["loss"] += 1

                    daily_pnl += net
                    pos["status"]  = "CLOSED"
                    pos["net_pnl"] = net

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
                          f"LTP:{exit_price} {_chg(exit_price, pos.get('y_close', 0))} | "
                          f"Points:{points:.2f} | NetPnL:{net:.2f}" + ts())
                    return True   # closed — skip entry

                # ── Trailing on close price ──────────────────────────────
                move_percent = abs((price_for_trail - entry) / entry) * 100
                while move_percent >= (pos["trail_level"] + TRAIL_STEP_PERCENT):
                    pos["trail_level"] += TRAIL_STEP_PERCENT
                    if side == "BUY":
                        new_sl = round(price_for_trail * (1 - STOP_LOSS_PERCENT/100), 2)
                        if new_sl > pos["sl"]:
                            pos["sl"] = new_sl
                    else:
                        new_sl = round(price_for_trail * (1 + STOP_LOSS_PERCENT/100), 2)
                        if new_sl < pos["sl"]:
                            pos["sl"] = new_sl
                    print(Fore.YELLOW +
                          f"{candle_time_str} | TRAIL UPDATED | {symbol} | "
                          f"Entry:{entry} | LTP:{price_for_trail} {_chg(price_for_trail, pos.get('y_close', 0))} | "
                          f"New SL:{pos['sl']}" + ts())
                return False   # still open

            # ── Pre-seed first15_data open with the actual bar open price ──
            # In replay, Tick 2 (high) is the first tick that calls strategy().
            # Without this, build_context() initialises first15_data["open"]
            # with ltp=high — so F15 open becomes the bar HIGH, not the real
            # open price. Pre-seeding here before any tick fires fixes it.
            candle_time_str_pre = current_time.strftime("%H:%M")
            if "09:15" <= candle_time_str_pre < "09:30":
                if symbol not in first15_data:
                    first15_data[symbol] = {
                        "open":  open_p,   # ← actual bar open, not first tick price
                        "high":  high,
                        "low":   low,
                        "close": close_p,
                        "ready": False
                    }
                else:
                    # update running H/L/C but never overwrite the open
                    first15_data[symbol]["high"]  = max(first15_data[symbol]["high"],  high)
                    first15_data[symbol]["low"]   = min(first15_data[symbol]["low"],   low)
                    first15_data[symbol]["close"] = close_p

            # ── Pre-seed orb_data with correct bar high/low ───────────────
            if "09:15" <= candle_time_str_pre < "10:15":
                if symbol not in orb_data:
                    orb_data[symbol] = {"high": high, "low": low, "ready": False}
                else:
                    orb_data[symbol]["high"] = max(orb_data[symbol]["high"], high)
                    orb_data[symbol]["low"]  = min(orb_data[symbol]["low"],  low)

            # ── Tick 1: open — update price, no strategy signal ──────────
            latest_prices[symbol] = open_p

            # ── Tick 2: high — BUY entries fire here (ltp = high) ────────
            latest_prices[symbol] = high
            closed = _replay_exit_check(high)
            if not closed:
                strategy(token, _make_tick(high))

            # ── Tick 3: low — SELL entries fire here (ltp = low) ─────────
            latest_prices[symbol] = low
            closed = _replay_exit_check(low)
            if not closed:
                strategy(token, _make_tick(low))

            # ── Tick 4: close — trailing update + context for next candle ─
            latest_prices[symbol] = close_p
            _replay_exit_check(close_p)

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
              f"EOD | {symbol} | {side} | Entry:{entry} | Exit:{ltp} | "
              f"LTP:{ltp} {_chg(ltp, pos.get('y_close', 0))} | NetPnL:{net:.2f}" + ts())

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
# ==========================================
# FRESH DAILY DOWNLOAD  (auto-runs every morning)
# ==========================================
# Downloads last 150 calendar days of daily OHLC for all 195 symbols.
# Overwrites the daily CSV completely — no stale data, no merge confusion.
# 150 days is enough for:
#   • EMA7  (needs ~21 days to stabilize)
#   • EMA20 (needs ~60 days to stabilize)
#   • EMA50 (needs ~150 days to fully stabilize) ✅
#   • Yesterday OHLC (always the actual last trading day)
# Takes ~30-40 seconds for 195 symbols.
# ==========================================
def fresh_daily_download():

    print("⚡ Downloading last 300 days of daily data for all symbols..." + ts())

    to_date   = datetime.now()
    from_date = to_date - timedelta(days=300)

    success = 0
    failed  = 0

    for token, symbol in instrument_tokens.items():

        try:
            daily_data = kite.historical_data(token, from_date, to_date, "day")

            if not daily_data:
                print(Fore.YELLOW + f"⚠️  {symbol}: no data returned" + ts())
                failed += 1
                continue

            df = pd.DataFrame(daily_data)
            df["date"] = _safe_to_naive(df["date"])
            df.drop_duplicates(subset=["date"], inplace=True)
            df.sort_values("date", inplace=True)
            df.reset_index(drop=True, inplace=True)

            file_path = os.path.join(DAILY_DIR, f"{symbol}_daily.csv")
            df.to_csv(file_path, index=False)

            success += 1
            time.sleep(0.1)   # gentle rate limiting — avoid Kite API throttle

        except Exception as e:
            print(Fore.RED + f"❌ {symbol}: {e}" + ts())
            failed += 1

    print(Fore.GREEN + f"✅ Daily download done — {success} symbols updated, {failed} failed" + ts())


# ==========================================
# DOWNLOAD TODAY'S MINUTE DATA  (run after market close ~3:45 PM)
# ==========================================
# Downloads today's 1-min OHLC for all symbols from Kite API.
# Merges with existing minute CSVs so replay works for today's date.
# Kite historical API makes today's data available ~15-30 min after close.
# Run this after 3:45 PM and then use Replay Mode → Single Day → today's date.
# ==========================================
def download_today_minute_data():

    today      = datetime.now().date()
    now_time   = datetime.now().strftime("%H:%M")

    # Guard: market must be closed (data available after ~15:30)
    if now_time < "15:30":
        print(Fore.YELLOW +
            f"⚠️  Market not yet closed ({now_time}). "
            f"Run this after 15:30 for complete today's data." + ts())
        # Allow continue — partial data is still useful for debugging

    print(Fore.CYAN +
        f"📥 Downloading today's ({today}) 1-min data for all symbols..." + ts())

    from_dt = datetime(today.year, today.month, today.day, 9, 15)
    to_dt   = datetime(today.year, today.month, today.day, 15, 30)

    success = 0
    failed  = 0
    skipped = 0

    for token, symbol in instrument_tokens.items():

        try:
            minute_data = kite.historical_data(token, from_dt, to_dt, "minute")

            if not minute_data:
                print(Fore.YELLOW + f"⚠️  {symbol}: no data returned" + ts())
                failed += 1
                continue

            new_df = pd.DataFrame(minute_data)
            new_df["date"] = _safe_to_naive(new_df["date"])

            file_path = os.path.join(MINUTE_DIR, f"{symbol}.csv")

            if os.path.exists(file_path):
                # Merge: load existing, remove any stale today rows, append fresh
                existing_df = pd.read_csv(file_path)
                existing_df["date"] = _safe_to_naive(existing_df["date"])

                # Remove any existing rows for today (to avoid duplicates)
                today_ts   = pd.Timestamp(today)
                existing_df = existing_df[
                    existing_df["date"].dt.date != today
                ]
                merged_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                merged_df = new_df

            merged_df.drop_duplicates(subset=["date"], inplace=True)
            merged_df.sort_values("date", inplace=True)
            merged_df.reset_index(drop=True, inplace=True)
            merged_df.to_csv(file_path, index=False)

            success += 1
            time.sleep(0.1)   # gentle rate limiting

        except Exception as e:
            print(Fore.RED + f"❌ {symbol}: {e}" + ts())
            failed += 1

    print(Fore.GREEN +
        f"✅ Today's minute data done — "
        f"{success} updated, {failed} failed | "
        f"Now use Replay Mode → Single Day → {today}" + ts())


def download_6_month_data():

    print("📥 Downloading 6 Months Historical Data (Chunked 60 Days)..." + ts())

    to_date = datetime.now()
    from_date = to_date - timedelta(days=360)

    for token, symbol in instrument_tokens.items():

        try:
            print(f"\nDownloading {symbol}..." + ts())

            # ==========================
            # DOWNLOAD MINUTE DATA IN 60 DAY CHUNKS
            # ==========================
            all_minute_data = []

            chunk_start = from_date

            while chunk_start < to_date:

                chunk_end = min(chunk_start + timedelta(days=60), to_date)

                print(f"  ⏳ Fetching {chunk_start.date()} → {chunk_end.date()}" + ts())

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

            print(f"  ✅ {symbol} Done" + ts())

        except Exception as e:
            print(f"❌ Error downloading {symbol}: {e}" + ts())

    print("\n✅ 6 Months Historical Data Download Complete" + ts())



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

        ltp = tick["last_price"]
        tick_time = datetime.now().replace(second=0, microsecond=0)

        latest_prices[symbol] = ltp

        # ===============================
        # TRACK TRUE INTRADAY HIGH / LOW
        # ===============================
        # latest_highs / latest_lows = running day high/low from 09:15 onwards.
        # Used by strategies instead of ohlc["high"/"low"] which is only the
        # current 1-min candle — NOT the full day range.
        if symbol not in latest_highs:
            latest_highs[symbol] = ltp
            latest_lows[symbol]  = ltp
        else:
            latest_highs[symbol] = max(latest_highs[symbol], ltp)
            latest_lows[symbol]  = min(latest_lows[symbol],  ltp)

        # ===============================
        # BUILD REAL 1-MIN CANDLE
        # ===============================
        if symbol not in minute_candles:
            minute_candles[symbol] = {}

        if tick_time not in minute_candles[symbol]:
            minute_candles[symbol][tick_time] = {
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            }
        else:
            candle = minute_candles[symbol][tick_time]
            candle["high"] = max(candle["high"], ltp)
            candle["low"] = min(candle["low"], ltp)
            candle["close"] = ltp

        # Use this candle instead of tick["ohlc"]
        candle = minute_candles[symbol][tick_time]

        # ===============================
        # BUILD LIVE 15-MIN + 1-HOUR CANDLES
        # ===============================
        # Mirrors what replay_update_intraday_state() does in replay mode.
        # Uses ltp as proxy for current candle prices (true open/high/low/close
        # maintained below via the rolling candle dict).
        now = tick_time
        slot_15 = (now.hour * 60 + now.minute) // 15 * 15
        if symbol not in replay_15m_data or replay_15m_data[symbol]["start_min"] != slot_15:
            replay_15m_data[symbol] = {
                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "start_min": slot_15
            }
        else:
            c = replay_15m_data[symbol]
            c["high"]  = max(c["high"],  ltp)
            c["low"]   = min(c["low"],   ltp)
            c["close"] = ltp

        if symbol not in replay_1h_data or replay_1h_data[symbol]["start_hour"] != now.hour:
            replay_1h_data[symbol] = {
                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "start_hour": now.hour
            }
        else:
            c = replay_1h_data[symbol]
            c["high"]  = max(c["high"],  ltp)
            c["low"]   = min(c["low"],   ltp)
            c["close"] = ltp

        fake_tick = {
            "instrument_token": token,
            "last_price": ltp,
            "ohlc": candle,
            "volume_traded": tick.get("volume_traded", 0),
            "date": tick_time
        }

        global ws_ready
        if not ws_ready:
            if ws_connect_time and (datetime.now() - ws_connect_time).total_seconds() >= WARMUP_SECONDS:
                ws_ready = True
                print(Fore.GREEN + "✅ Warmup complete — strategies now active" + ts())
            else:
                continue

        strategy(token, fake_tick)

def on_connect(ws, response):
    global ws_connect_time, ws_ready
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    ws_connect_time = datetime.now()
    ws_ready        = False
    ready_at = (ws_connect_time + pd.Timedelta(seconds=WARMUP_SECONDS)).strftime("%H:%M:%S")
    print(Fore.GREEN  + "🚀 Connected to Kite WebSocket" + ts())
    print(Fore.YELLOW + f"⏳ Warmup: strategies active after {WARMUP_SECONDS}s (at {ready_at})" + ts())

    # After WebSocket connects, wait a few seconds for prices to flow in,
    # then verify any restored OPEN positions against live prices.
    def _delayed_verify():
        time.sleep(5)   # wait for first batch of ticks to arrive
        verify_restored_positions()
    threading.Thread(target=_delayed_verify, daemon=True).start()

def on_close(ws, code, reason):
    print(Fore.YELLOW + f"⚠️  WebSocket closed: {code} - {reason}" + ts())

def on_error(ws, code, reason):
    print(Fore.RED + f"❌ WebSocket error: {code} - {reason}" + ts())

def on_reconnect(ws, attempt):
    print(Fore.YELLOW + f"🔄 WebSocket reconnecting... attempt {attempt}" + ts())

def on_noreconnect(ws):
    print(Fore.RED + "🚨 WebSocket max reconnects reached — restart required" + ts())

kws.on_ticks      = on_ticks
kws.on_connect    = on_connect
kws.on_close      = on_close
kws.on_error      = on_error
kws.on_reconnect  = on_reconnect
kws.on_noreconnect = on_noreconnect

# Threads started inside __main__ after all prompts complete — see below

if __name__ == "__main__":

    print("\n" + "="*70)

    mode_color = Fore.GREEN if TRADING_MODE == "PAPER" else Fore.RED

    print(mode_color + f"🚀 EXECUTION ENGINE STARTED | MODE: {TRADING_MODE}" + ts())
    print("="*70)

    print(Fore.CYAN + f"Symbols Loaded: {len(tokens)}" + ts())
    print(Fore.CYAN + f"Fixed Qty: {FIXED_QTY}" + ts())
    print(Fore.CYAN + f"Stop Loss %: {STOP_LOSS_PERCENT}%" + ts())
    print(Fore.CYAN + f"Target %: {TARGET_PERCENT}%" + ts())
    print(Fore.CYAN + f"Trail Step %: {TRAIL_STEP_PERCENT}%" + ts())
    print(Fore.CYAN + f"Square Off Time: {SQUARE_OFF_TIME}" + ts())
    print(Fore.CYAN + f"Daily Max Loss %: {DAILY_MAX_LOSS_PERCENT}%" + ts())

    print("="*70 + "\n")

    write_log(f"ENGINE STARTED | MODE: {TRADING_MODE}")

    # ── Daily data — always download fresh 100 days at startup ──────────
    # Runs automatically every morning. No prompt needed.
    # Downloads 100 days of daily OHLC → overwrites local CSVs → fresh
    # yesterday OHLC and accurate EMA7/20/50 guaranteed every session.
    # Skip only if you want to use existing local data (not recommended).
    # ────────────────────────────────────────────────────────────────────
    skip = input("Skip daily data download? (y = skip, Enter = download): ").strip().lower()
    if skip != "y":
        fresh_daily_download()
    else:
        print(Fore.YELLOW + "⚠️  Using existing local daily data — EMA/Yesterday may be stale" + ts())

    if TRADING_MODE == "PAPER" and input("Replay Mode? (y/n): ") == "y":

        print("\nSelect Replay Mode:" + ts())
        print("1 → Single Day")
        print("2 → 1 Week")
        print("3 → 1 Month")
        print("T → Download Today's Data (run after 15:30)")

        choice = input("Enter choice (1/2/3/T): ")

        if choice == "T" or choice.lower() == "t":
            today_str_check = datetime.now().strftime("%Y-%m-%d")
            sample_symbol   = SYMBOLS[0]
            sample_file     = os.path.join(MINUTE_DIR, f"{sample_symbol}.csv")
            already_have_today = False
            if os.path.exists(sample_file):
                try:
                    _chk = pd.read_csv(sample_file, usecols=["date"])
                    _chk["date"] = pd.to_datetime(_chk["date"])
                    already_have_today = (_chk["date"].dt.strftime("%Y-%m-%d") == today_str_check).any()
                except Exception:
                    pass

            if already_have_today:
                skip_dl = input(
                    f"⚠️  Today's ({today_str_check}) data already downloaded. "
                    f"Re-download? (y = yes, Enter = skip): "
                ).strip().lower()
                if skip_dl == "y":
                    download_today_minute_data()
                else:
                    print(Fore.CYAN + f"⏭️  Skipping download — using existing data for {today_str_check}" + ts())
            else:
                download_today_minute_data()

            date_input = input("Replay today's date now? (Enter date YYYY-MM-DD or skip): ").strip()
            if date_input:
                replay_date = pd.to_datetime(date_input)
                run_market_replay_proper(replay_date)

        elif choice == "1":

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
                    print(f"🔁 STARTING REPLAY FOR {current.date()}" + ts())
                    print("="*80)

                    # Reset daily stats
                    trades_taken.clear()
                    paper_positions.clear()
                    for key in trade_statistics:
                        trade_statistics[key] = 0

                    #global daily_pnl
                    daily_pnl = 0

                    run_market_replay_proper(current)

                    print(f"✅ COMPLETED {current.date()}" + ts())

                current += timedelta(days=1)

            #print("\n✅ RANGE REPLAY COMPLETED")
            print("\n" + "="*60)
            print("📊 RANGE SUMMARY" + ts())
            print("="*60)

            total_range_pnl = 0
            total_days = 0

            for date, pnl in range_results.items():

                total_days += 1
                total_range_pnl += pnl

                color = Fore.GREEN if pnl >= 0 else Fore.RED
                print(color + f"{date} → {pnl}" + ts())

            print("-"*60)

            final_color = Fore.GREEN if total_range_pnl >= 0 else Fore.RED
            print(final_color + f"TOTAL ({total_days} Days) → {round(total_range_pnl,2)}" + ts())

            print("="*60)
            print("\n✅ RANGE REPLAY COMPLETED")

    else:
        # Load required data for live paper mode
        today = pd.to_datetime(datetime.now().date())

        load_yesterday_from_local(today)
        load_ema_data_from_local(today)
        load_orb_and_first15_from_kite()

        # ── Restore state from previous session (if any) ─────────────────
        if TRADING_MODE == "PAPER":
            load_paper_positions_cache()   # restore paper_positions + daily_pnl
        if TRADING_MODE == "LIVE":
            load_live_positions_cache()    # restore trades_taken + restart OCO monitors

        # ── Start background threads (after prompts complete) ─────────────
        if TRADING_MODE == "LIVE":
            threading.Thread(target=position_manager,       daemon=True).start()
        if TRADING_MODE == "PAPER":
            threading.Thread(target=paper_position_manager, daemon=True).start()
        threading.Thread(target=auto_square_off,            daemon=True).start()

        # ── Start WebSocket ───────────────────────────────────────────────
        kws.connect(threaded=False, disable_ssl_verification=False)