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
TRADING_MODE = "LIVE"   # ⚠️  MUST change to "LIVE" manually before live trading

FIXED_QTY = 10                # ⚠️  SET TO 1 FOR LIVE TEST — change to 10 only after full validation
LIVE_MAX_TRADES = 20         # max simultaneous live trades (safety cap)
STOP_LOSS_PERCENT = 0.75        # 0.75% SL → 1:2 ratio with 1.5% target
TARGET_PERCENT = 1.5         # 1.5% target → 2x the SL distance
TRAIL_STEP_PERCENT = 0.75       # trail locks in profit every 0.75% move
BROKERAGE_PER_ORDER = 0     # approx MIS brokerage 

# Maximum % a stock can have already moved PAST yesterday's high/low before entry.
# If stock is already >1.5% below YL → oversold, reversal risk high → skip SELL.
# If stock is already >1.5% above YH → overbought, reversal risk high → skip BUY.
# RELIANCE case: was 2.5% below YL → would have been blocked by this filter.
MAX_OVEREXTENSION_PCT = 1.5

# ==========================================
# PYRAMIDING CONFIGURATION
# ==========================================
# S1–S8: add a unit every PYRAMID_STEP_PCT move, up to PYRAMID_MAX_PCT
#         SL for each pyramid leg = 0.75% from that leg's entry
#         Target for ALL legs     = 1.5% from the FIRST entry
# S9:     add a unit every S9_PYRAMID_STEP_PCT move, no limit except first-
#         entry SL/TSL. No target — TSL-only (same as the base S9 position).
PYRAMID_STEP_PCT     = 0.30   # S1–S8: add leg every 0.30% price move
PYRAMID_MAX_PCT      = 1.20   # S1–S8: stop pyramiding after 1.20% move
S9_PYRAMID_STEP_PCT  = 0.50   # S9: add leg every 0.50% price move
PYRAMID_SL_PCT       = 0.75   # SL for every pyramid leg (same as base SL)

# pyramid_state[symbol] = {
#   "base_entry":    float,   # first-entry price
#   "base_sl":       float,   # first-entry SL (used as sentinel for S9 pyramid stop)
#   "side":          "BUY"|"SELL",
#   "strategy":      str,     # strategy name — used to detect S9 vs S1–S8
#   "legs":          int,     # number of pyramid legs placed so far (0 = base only)
#   "next_trigger":  float,   # price level that will trigger the next pyramid leg
#   "tsl_only":      bool,    # True for S9
#   "base_target":   float|None,  # 1.5% target from base entry (None for S9)
# }
pyramid_state = {}

HIST_DIR = "historical_data"
MINUTE_DIR = os.path.join(HIST_DIR, "minute")
DAILY_DIR = os.path.join(HIST_DIR, "daily")

os.makedirs(MINUTE_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)

REPLAY_DURATION_MINUTES = 5  # change 10 / 15 as needed
ENTRY_START_TIME = "09:19"
ENTRY_END_TIME   = "14:20"   # aligned with all strategy time gates (S1/S8/S9 all check > "14:20")


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
POSITIONS_CACHE_FILE = f"positions_cache_{today_str}.csv"
_max_trades_warned = set()   # paper positions + daily_pnl

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
        if not paper_positions:
            return   # nothing to save on first run
        rows = []
        for symbol, pos in paper_positions.items():
            saved_pnl = round(float(pos.get("net_pnl", 0)), 2)
            rows.append({
                "symbol":        symbol,
                "side":          pos["side"],
                "entry":         pos["entry"],
                "sl":            pos["sl"],
                "target":        pos["target"],
                "qty":           pos["qty"],
                "trail_level":   pos["trail_level"],
                "status":        pos["status"],
                "strategy":      pos["strategy"],
                "entry_time":    pos["entry_time"],
                "net_pnl":       saved_pnl,
                "tsl_only":      pos.get("tsl_only", False),
                "tsl_step":      pos.get("tsl_step", TRAIL_STEP_PERCENT),
                "pyramid_base":  pos.get("pyramid_base", ""),
                "pyramid_leg":   pos.get("pyramid_leg", ""),
                # BUG FIX: persist LIVE pyramid leg order IDs so cascade-cancel
                # works correctly after a crash/restart. Without these, a restart
                # after a pyramid leg fires leaves dangling SL-M orders at broker
                # that never get cancelled when the base position closes.
                "live_sl_oid":   pos.get("_live_sl_oid", ""),
                "live_tgt_oid":  pos.get("_live_tgt_oid", ""),
            })
        df = pd.DataFrame(rows)
        df["daily_pnl_snapshot"] = daily_pnl
        df.to_csv(POSITIONS_CACHE_FILE, index=False)
    except Exception as e:
        print(Fore.YELLOW + f"⚠️  Could not save positions cache: {e}" + ts())


def load_paper_positions_cache():
    """
    Reload paper_positions and daily_pnl from today's cache on restart.
    daily_pnl is recomputed from closed position net_pnls — NOT from the
    snapshot — to avoid double-counting when the crash happened mid-save.
    """
    global daily_pnl

    if not os.path.exists(POSITIONS_CACHE_FILE):
        return

    try:
        df = pd.read_csv(POSITIONS_CACHE_FILE)
        if df.empty:
            return

        # ── Guard: old cache file may be missing newer columns ────────────
        required_cols = {"symbol", "side", "entry", "sl", "target", "qty",
                         "trail_level", "status", "strategy", "entry_time"}
        if not required_cols.issubset(df.columns):
            print(Fore.YELLOW + "⚠️  Positions cache missing required columns — ignoring" + ts())
            return

        # ── Recompute daily_pnl from closed net_pnls (source-of-truth) ────
        # Using the snapshot column risks double-count if crash happened
        # between daily_pnl update and save.  Summing closed net_pnl is exact.
        if "net_pnl" in df.columns:
            closed_mask = df["status"] == "CLOSED"
            daily_pnl   = float(df.loc[closed_mask, "net_pnl"].fillna(0).sum())
        elif "daily_pnl_snapshot" in df.columns:
            daily_pnl = float(df["daily_pnl_snapshot"].iloc[-1])

        # ── Restore trade counts ──────────────────────────────────────────
        closed_df = df[df["status"] == "CLOSED"]
        # Count only base positions (not pyramid legs) to avoid inflating counts
        base_closed = closed_df[~closed_df["symbol"].astype(str).str.contains("__PYR")]
        trade_statistics["total_trades"] = len(base_closed)
        trade_statistics["buy_trades"]   = len(base_closed[base_closed["side"] == "BUY"])
        trade_statistics["sell_trades"]  = len(base_closed[base_closed["side"] == "SELL"])

        # ── Rebuild strategy_stats from closed base rows ──────────────────
        # BUG FIX 19: strip " [PYRn]" suffix from strategy names on load so
        # old cached rows (written before the fix) don't create phantom entries.
        for _, row in base_closed.iterrows():
            sname   = str(row.get("strategy", "Unknown")).split(" [PYR")[0]
            pnl_val = float(row["net_pnl"]) if "net_pnl" in row and pd.notna(row["net_pnl"]) else 0.0
            if sname not in strategy_stats:
                strategy_stats[sname] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}
            strategy_stats[sname]["trades"] += 1
            strategy_stats[sname]["pnl"]    += pnl_val
            if pnl_val > 0:   strategy_stats[sname]["wins"] += 1
            elif pnl_val < 0: strategy_stats[sname]["loss"] += 1

        restored_open   = 0
        restored_closed = 0

        for _, row in df.iterrows():
            symbol = str(row["symbol"])
            status = row["status"]

            pos = {
                "side":        row["side"],
                "entry":       float(row["entry"]),
                "sl":          float(row["sl"]),
                "target":      float(row["target"]) if pd.notna(row["target"]) else None,
                "qty":         int(row["qty"]),
                "trail_level": float(row["trail_level"]),
                "status":      status,
                "strategy":    str(row.get("strategy", "")),
                "entry_time":  str(row.get("entry_time", "")),
                "net_pnl":     float(row["net_pnl"]) if "net_pnl" in row and pd.notna(row.get("net_pnl")) else 0.0,
                "tsl_only":    str(row.get("tsl_only", "False")).lower() == "true" if "tsl_only" in row else False,
                "tsl_step":    float(row["tsl_step"]) if "tsl_step" in row and pd.notna(row.get("tsl_step")) else TRAIL_STEP_PERCENT,
            }
            # Restore pyramid linkage — only for real pyramid legs (non-empty base)
            raw_pb = str(row.get("pyramid_base", "")) if "pyramid_base" in row else ""
            if raw_pb and raw_pb not in ("", "nan", "None"):
                pos["pyramid_base"] = raw_pb
            raw_pl = row.get("pyramid_leg", "")
            if pd.notna(raw_pl) and str(raw_pl) not in ("", "nan", "None"):
                pos["pyramid_leg"] = int(float(raw_pl))

            # BUG FIX: restore LIVE pyramid leg order IDs from CSV so
            # cascade-cancel (_cascade_cancel_live_pyramid_legs) can cancel
            # dangling broker SL-M/target orders after a restart.
            raw_sl_oid  = str(row.get("live_sl_oid",  "")) if "live_sl_oid"  in row else ""
            raw_tgt_oid = str(row.get("live_tgt_oid", "")) if "live_tgt_oid" in row else ""
            if raw_sl_oid  and raw_sl_oid  not in ("", "nan", "None"):
                pos["_live_sl_oid"]  = raw_sl_oid
            if raw_tgt_oid and raw_tgt_oid not in ("", "nan", "None"):
                pos["_live_tgt_oid"] = raw_tgt_oid

            paper_positions[symbol] = pos

            if status == "OPEN":
                if "__PYR" in symbol:
                    restored_open += 1
                    continue   # pyramid legs don't get trades_taken entries
                trades_taken[symbol] = True
                restored_open += 1
            else:
                if "__PYR" not in symbol:
                    trades_taken[symbol] = True
                restored_closed += 1

        print(Fore.CYAN +
            f"♻️  Positions restored — {restored_open} OPEN, {restored_closed} CLOSED "
            f"| Closed PnL so far: ₹{daily_pnl:.2f}" + ts())

        if restored_open > 0:
            print(Fore.YELLOW +
                "⏳ Open positions will be re-checked against live prices once WebSocket connects..." + ts())

        # ── Rebuild pyramid_state for every restored OPEN base position ──────
        pyr_rebuilt = 0
        for sym_key, pos in paper_positions.items():
            if pos.get("status") != "OPEN":
                continue
            if "__PYR" in str(sym_key):
                continue

            is_s9    = pos.get("tsl_only", False)
            side_r   = pos["side"]
            entry_r  = pos["entry"]
            step_r   = S9_PYRAMID_STEP_PCT if is_s9 else PYRAMID_STEP_PCT
            max_legs = 999 if is_s9 else int(round(PYRAMID_MAX_PCT / PYRAMID_STEP_PCT))

            existing_legs = sum(
                1 for k, p in paper_positions.items()
                if p.get("pyramid_base") == sym_key and p.get("status") == "OPEN"
            )

            if existing_legs >= max_legs and not is_s9:
                continue

            next_trig = round(
                entry_r * (1 + (existing_legs + 1) * step_r / 100) if side_r == "BUY"
                else entry_r * (1 - (existing_legs + 1) * step_r / 100), 4
            )

            base_tgt = (
                round(entry_r * (1 + TARGET_PERCENT / 100), 4) if (not is_s9 and side_r == "BUY")
                else round(entry_r * (1 - TARGET_PERCENT / 100), 4) if not is_s9
                else None
            )

            pyramid_state[sym_key] = {
                "base_entry":   entry_r,
                "base_sl":      pos["sl"],
                "side":         side_r,
                "strategy":     pos.get("strategy", ""),
                "legs":         existing_legs,
                "next_trigger": next_trig,
                "tsl_only":     is_s9,
                "base_target":  base_tgt,
            }
            pyr_rebuilt += 1

        if pyr_rebuilt:
            print(Fore.CYAN +
                  f"🔺 Pyramid state rebuilt for {pyr_rebuilt} open base position(s)" + ts())

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

                # ── Confirmed LIVE position (has order dict) ─────────────
                if isinstance(trade, dict):
                    if symbol not in broker_open:
                        if symbol in paper_positions:
                            paper_positions[symbol]["status"] = "CLOSED"
                        trades_taken[symbol] = "CLOSED"
                        print(Fore.MAGENTA +
                            f"🔄 {symbol}: position no longer at broker — marked CLOSED" + ts())
                    else:
                        print(Fore.GREEN +
                            f"✅ {symbol}: confirmed OPEN at broker qty={broker_open[symbol]['quantity']}" + ts())

                # ── PENDING entry: check if entry actually filled ─────────
                # If entry filled but SL failed, the position is at the broker
                # with no OCO monitor. We must detect it and start monitoring.
                elif trade == "PENDING":
                    if symbol in broker_open:
                        broker_pos = broker_open[symbol]
                        qty_at_broker = abs(broker_pos["quantity"])
                        side_at_broker = "BUY" if broker_pos["quantity"] > 0 else "SELL"
                        avg_price = broker_pos.get("average_price", 0)

                        # Try to find any existing open SL-M order for this symbol
                        try:
                            all_orders  = kite.orders()
                            open_sl     = next((
                                o for o in all_orders
                                if o["tradingsymbol"] == symbol
                                and o["order_type"] in ("SL-M", "SL", "SLM")
                                and o["status"] in ("OPEN", "TRIGGER PENDING")
                                and o["transaction_type"] != ("BUY" if side_at_broker == "BUY" else "SELL")
                            ), None)
                            open_tgt    = next((
                                o for o in all_orders
                                if o["tradingsymbol"] == symbol
                                and o["order_type"] == "LIMIT"
                                and o["status"] == "OPEN"
                                and o["transaction_type"] != ("BUY" if side_at_broker == "BUY" else "SELL")
                            ), None)

                            if open_sl:
                                # Restore full trade dict and start OCO
                                trades_taken[symbol] = {
                                    "entry":  "",
                                    "sl":     str(open_sl["order_id"]),
                                    "target": str(open_tgt["order_id"]) if open_tgt else None,
                                    "side":   side_at_broker,
                                    "qty":    qty_at_broker,
                                }
                                # Ensure paper_positions entry exists
                                if symbol not in paper_positions or paper_positions[symbol].get("status") != "OPEN":
                                    tick_s  = tick_sizes.get(symbol, 0.05)
                                    raw_sl  = open_sl.get("trigger_price", avg_price * 0.9925)
                                    raw_tgt = open_tgt.get("price") if open_tgt else None
                                    paper_positions[symbol] = {
                                        "side":        side_at_broker,
                                        "entry":       float(avg_price),
                                        "sl":          float(raw_sl),
                                        "target":      float(raw_tgt) if raw_tgt else None,
                                        "qty":         qty_at_broker,
                                        "trail_level": 0,
                                        "status":      "OPEN",
                                        "strategy":    "Restored(PENDING)",
                                        "entry_time":  datetime.now().strftime("%H:%M"),
                                        "y_close":     0,
                                        "tsl_only":    open_tgt is None,
                                        "tsl_step":    TRAIL_STEP_PERCENT,
                                    }
                                threading.Thread(
                                    target=oco_monitor, args=(symbol,), daemon=True
                                ).start()
                                # BUG FIX: arm pyramid_state for this recovered position
                                # so legs can fire as price moves. Without this, a crash
                                # right after entry (before pyramid_state was set) leaves
                                # the position permanently unable to pyramid.
                                if symbol not in pyramid_state:
                                    _rp = paper_positions[symbol]
                                    _is_s9_r  = _rp.get("tsl_only", False)
                                    _step_r   = S9_PYRAMID_STEP_PCT if _is_s9_r else PYRAMID_STEP_PCT
                                    _entry_r  = float(avg_price)
                                    _side_r   = side_at_broker
                                    _trig_r   = round(
                                        _entry_r * (1 + _step_r / 100) if _side_r == "BUY"
                                        else _entry_r * (1 - _step_r / 100), 4
                                    )
                                    pyramid_state[symbol] = {
                                        "base_entry":   _entry_r,
                                        "base_sl":      float(raw_sl),
                                        "side":         _side_r,
                                        "strategy":     "Restored(PENDING)",
                                        "legs":         0,
                                        "next_trigger": _trig_r,
                                        "tsl_only":     _is_s9_r,
                                        "base_target":  (
                                            None if _is_s9_r
                                            else round(_entry_r * (1 + TARGET_PERCENT / 100), 4)
                                            if _side_r == "BUY"
                                            else round(_entry_r * (1 - TARGET_PERCENT / 100), 4)
                                        ),
                                    }
                                    print(Fore.CYAN +
                                          f"🔺 Pyramid armed (PENDING recovery) | {symbol} | "
                                          f"{_side_r} | trigger:{_trig_r:.2f}" + ts())
                                print(Fore.YELLOW +
                                    f"♻️  PENDING {symbol} recovered: "
                                    f"qty={qty_at_broker} SL={open_sl['trigger_price']} "
                                    f"— OCO monitor started" + ts())
                                write_log(
                                    f"PENDING RECOVERY {symbol} qty={qty_at_broker} "
                                    f"sl_oid={open_sl['order_id']}"
                                )
                            else:
                                # Entry filled but NO SL order exists → emergency exit
                                print(Fore.RED +
                                    f"🚨 PENDING {symbol}: filled at broker, NO SL found "
                                    f"— placing emergency market exit" + ts())
                                write_log(f"PENDING RECOVERY EMERGENCY EXIT {symbol}")
                                try:
                                    exit_txn = (
                                        kite.TRANSACTION_TYPE_SELL if side_at_broker == "BUY"
                                        else kite.TRANSACTION_TYPE_BUY
                                    )
                                    kite.place_order(
                                        variety=kite.VARIETY_REGULAR,
                                        exchange=kite.EXCHANGE_NSE,
                                        tradingsymbol=symbol,
                                        transaction_type=exit_txn,
                                        quantity=qty_at_broker,
                                        order_type=kite.ORDER_TYPE_MARKET,
                                        product=kite.PRODUCT_MIS
                                    )
                                    print(Fore.RED +
                                          f"🚨 Emergency exit placed for PENDING {symbol}" + ts())
                                except Exception as ee:
                                    print(Fore.RED +
                                          f"🚨 Emergency exit FAILED for {symbol}: {ee}" + ts())
                                    write_log(f"PENDING EMERGENCY EXIT FAILED {symbol}: {ee}")

                        except Exception as oe:
                            write_log(f"PENDING order fetch failed {symbol}: {oe}")
                    else:
                        # PENDING but not at broker — entry never filled or was cancelled
                        print(Fore.CYAN +
                            f"ℹ️  PENDING {symbol}: not found at broker — entry never filled, keeping blocked" + ts())

        except Exception as e:
            print(Fore.YELLOW + f"⚠️  Live position verify failed: {e}" + ts())

        # Persist any status changes (CLOSED markings) to disk
        save_paper_positions_cache()

    else:
        # ── PAPER: check SL/target against current live prices ───────────
        # daily_pnl is already recomputed from closed net_pnls in
        # load_paper_positions_cache — no double-counting here.
        hit_count = 0
        for symbol, pos in list(paper_positions.items()):
            if pos["status"] != "OPEN":
                continue

            real_sym = pos.get("pyramid_base", symbol) if "__PYR" in symbol else symbol
            ltp = latest_prices.get(real_sym)
            if not ltp:
                continue

            entry = pos["entry"]
            side  = pos["side"]
            qty   = pos["qty"]

            if side == "BUY":
                if ltp <= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT (offline)"
                elif not pos.get("tsl_only") and pos.get("target") is not None and ltp >= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT (offline)"
                else:
                    continue
            else:
                if ltp >= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT (offline)"
                elif not pos.get("tsl_only") and pos.get("target") is not None and ltp <= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT (offline)"
                else:
                    continue

            points  = (exit_price - entry) if side == "BUY" else (entry - exit_price)
            net_pnl = round(points * qty - BROKERAGE_PER_ORDER * 2, 2)
            daily_pnl += net_pnl
            pos["status"]  = "CLOSED"
            pos["net_pnl"] = net_pnl
            hit_count += 1

            print(Fore.MAGENTA +
                f"🔄 {symbol} {exit_reason} | {side} | Entry:{entry} → Exit:{exit_price} "
                f"| NetPnL:{net_pnl:.2f}" + ts())
            write_log(f"OFFLINE CLOSE {symbol} {exit_reason} {side} Entry:{entry} Exit:{exit_price} NetPnL:{net_pnl:.2f}")
            with open(paper_trade_log_file, "a", newline="") as f_out:
                csv.writer(f_out).writerow(
                    [datetime.now(), symbol, side, entry, exit_price, qty, net_pnl, exit_reason]
                )

            # ── Cascade-close pyramid legs if base was hit offline ────────
            if "__PYR" not in symbol:
                for leg_key, leg_pos in list(paper_positions.items()):
                    if (leg_pos.get("pyramid_base") == symbol and
                            leg_pos.get("status") == "OPEN"):
                        l_pts = (exit_price - leg_pos["entry"]) if side == "BUY" else (leg_pos["entry"] - exit_price)
                        l_net = round(l_pts * leg_pos["qty"] - BROKERAGE_PER_ORDER * 2, 2)
                        leg_pos["status"]  = "CLOSED"
                        leg_pos["net_pnl"] = l_net
                        # BUG FIX 20: do NOT add l_net to daily_pnl here.
                        # load_paper_positions_cache() already recomputes daily_pnl
                        # as sum of all CLOSED net_pnls after save. Adding here
                        # AND having it summed again on next restart = double count.
                        # The base position is correctly counted above (daily_pnl += net_pnl).
                        # Leg PnL flows in on next load via the closed mask sum.
                        hit_count         += 1
                        with open(paper_trade_log_file, "a", newline="") as _f_out2:
                            csv.writer(_f_out2).writerow(
                                [datetime.now(), leg_key, side, leg_pos["entry"], exit_price, leg_pos["qty"], l_net, exit_reason + " (cascade)"]
                            )
                        print(Fore.MAGENTA +
                            f"🔄 {leg_key} {exit_reason} (cascade) | "
                            f"Entry:{leg_pos['entry']} → Exit:{exit_price} | NetPnL:{l_net:.2f}" + ts())
                        write_log(f"OFFLINE CASCADE {leg_key} {exit_reason} Entry:{leg_pos['entry']} Exit:{exit_price} NetPnL:{l_net:.2f}")
                pyramid_state.pop(symbol, None)

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
        live_restored = 0
        for _, row in df.iterrows():
            symbol = row["symbol"]
            status = row["status"]

            if status == "LIVE":
                # Restore full order dict and restart OCO monitor
                _raw_tgt = row["target_oid"]
                _tgt_restored = (
                    None if (pd.isna(_raw_tgt) or str(_raw_tgt).strip() in ("", "nan", "None"))
                    else str(_raw_tgt)
                )
                trades_taken[symbol] = {
                    "entry":  str(row["entry_oid"]),
                    "sl":     str(row["sl_oid"]),
                    "target": _tgt_restored,
                    "side":   str(row["side"]),
                    "qty":    FIXED_QTY
                }
                # FIX 2: stagger starts so all 6 monitors don't hammer
                # kite.orders() simultaneously at restart.
                time.sleep(0.5 * live_restored)
                threading.Thread(
                    target=oco_monitor, args=(symbol,), daemon=True
                ).start()
                print(Fore.YELLOW + f"♻️  Restored LIVE position: {symbol} — OCO monitor restarted" + ts())
                live_restored += 1
            else:
                # PENDING or PAPER — just block re-entry
                trades_taken[symbol] = status

            restored += 1

        # Do NOT overwrite trade_statistics["total_trades"] here — that counter
        # is rebuilt from the positions cache (load_paper_positions_cache).
        # Overwriting it with len(trades_cache) would count CLOSED + PENDING + LIVE
        # and corrupt the dashboard trade count on every restart.
        print(Fore.CYAN +
              f"📂 Trades cache loaded — {restored} symbols blocked | "
              f"{live_restored} LIVE positions with OCO monitors restarted" + ts())

        # BUG FIX: rebuild pyramid_state for every OPEN LIVE base position.
        # load_paper_positions_cache() already does this for PAPER mode, but
        # in LIVE mode load_trades_cache() runs AFTER load_paper_positions_cache()
        # and the two are separate code paths. Without this block, pyramid_state
        # stays empty after any restart and no legs ever fire in LIVE mode.
        pyr_live_rebuilt = 0
        for sym_key, pos in paper_positions.items():
            if pos.get("status") != "OPEN":
                continue
            if "__PYR" in str(sym_key):
                continue
            if sym_key in pyramid_state:
                continue  # already built by load_paper_positions_cache

            is_s9_r  = pos.get("tsl_only", False)
            side_r   = pos["side"]
            entry_r  = pos["entry"]
            step_r   = S9_PYRAMID_STEP_PCT if is_s9_r else PYRAMID_STEP_PCT
            max_legs = 999 if is_s9_r else int(round(PYRAMID_MAX_PCT / PYRAMID_STEP_PCT))

            existing_legs = sum(
                1 for k, p in paper_positions.items()
                if p.get("pyramid_base") == sym_key and p.get("status") == "OPEN"
            )
            if existing_legs >= max_legs and not is_s9_r:
                continue

            next_trig = round(
                entry_r * (1 + (existing_legs + 1) * step_r / 100) if side_r == "BUY"
                else entry_r * (1 - (existing_legs + 1) * step_r / 100), 4
            )
            base_tgt = (
                None if is_s9_r
                else round(entry_r * (1 + TARGET_PERCENT / 100), 4) if side_r == "BUY"
                else round(entry_r * (1 - TARGET_PERCENT / 100), 4)
            )
            pyramid_state[sym_key] = {
                "base_entry":   entry_r,
                "base_sl":      pos["sl"],
                "side":         side_r,
                "strategy":     pos.get("strategy", ""),
                "legs":         existing_legs,
                "next_trigger": next_trig,
                "tsl_only":     is_s9_r,
                "base_target":  base_tgt,
            }
            pyr_live_rebuilt += 1

        if pyr_live_rebuilt:
            print(Fore.CYAN +
                  f"🔺 LIVE pyramid state rebuilt for {pyr_live_rebuilt} open position(s)" + ts())

    except Exception as e:
        print(Fore.YELLOW + f"⚠️  Could not load trades cache: {e}" + ts())

if not os.path.exists(paper_trade_log_file):
    with open(paper_trade_log_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Time","Symbol","Side","Entry","Exit","Qty","PnL","Reason"])



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
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
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

# ==========================================
# LOAD INSTRUMENTS
# ==========================================
instruments = kite.instruments("NSE")
for inst in instruments:
    if inst["tradingsymbol"] in SYMBOLS:
        instrument_tokens[inst["instrument_token"]] = inst["tradingsymbol"]
        tick_sizes[inst["tradingsymbol"]] = inst.get("tick_size", 0.05)

tokens = list(instrument_tokens.keys())

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
def place_trade(symbol, side, ltp, tick, condition_name, extra_info=None, custom_sl=None, tsl_only=False, tsl_step=None):

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
        # FIX C: only count BASE positions (not __PYR legs) toward LIVE_MAX_TRADES.
        # Pyramid legs are sub-units of existing trades; counting them would
        # wrongly block new base entries when pyramiding is active.
        open_count = sum(
            1 for k, p in paper_positions.items()
            if p["status"] == "OPEN" and "__PYR" not in str(k)
        )
        pending_count = sum(
            1 for v in trades_taken.values() if v == "PENDING"
        )
        if (open_count + pending_count) >= LIVE_MAX_TRADES:
            if symbol not in _max_trades_warned:
                _max_trades_warned.add(symbol)
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
        custom_sl if custom_sl is not None
        else (ltp * (1 - STOP_LOSS_PERCENT / 100) if side == "BUY"
              else ltp * (1 + STOP_LOSS_PERCENT / 100))
    )
    raw_target = (
        None if tsl_only
        else (ltp * (1 + TARGET_PERCENT / 100) if side == "BUY"
              else ltp * (1 - TARGET_PERCENT / 100))
    )

    sl     = round_to_tick(raw_sl, tick_size)
    target = round_to_tick(raw_target, tick_size) if raw_target is not None else None

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
            f"\n   C1 H/L:  {extra_info['f_high']} / {extra_info['f_low']}"
            f"\n   C2 H/L:  {extra_info['c2_high']} / {extra_info['c2_low']}"
            f"\n   C3 H/L:  {extra_info['c3_high']} / {extra_info['c3_low']}"
            f"\n   C4 H/L:  {extra_info['c4_high']} / {extra_info['c4_low']}"
            f"\n   Today  O/H/L: {extra_info['t_open']} / {extra_info['t_high']} / {extra_info['t_low']}"
            f"\n   Yest   O/H/L: {extra_info['y_open']} / {extra_info['y_high']} / {extra_info['y_low']} / Close:{extra_info['y_close']}"
            f"\n   YVol: {extra_info['y_vol']} | CumVol: {extra_info['t_vol']} | Vol%: {extra_info['vol_pct']}%"
        )

    y_close_entry = extra_info.get("y_close", 0) if extra_info else 0

    print(color +
        f"{entry_time} | {condition_name} | {side} | {symbol} | "
        f"Entry:{ltp} SL:{sl} {'TSL-Only(1.5%)' if tsl_only else f'Target:{target}'} Qty:{qty} | "
        f"LTP:{ltp} {_chg(ltp, y_close_entry)}"
        f"{structure_text}\n"
     + ts())

    write_log(
        f"{TRADING_MODE} {side} {symbol} "
        f"Entry:{ltp} SL:{sl} {'TSL-Only' if tsl_only else f'Target:{target}'}\n"
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

            # ── Write local position tracking immediately after entry fills ──
            # This ensures we always have a paper_positions entry even if
            # SL/target placement fails and we return early via exception.
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
                "y_close":     extra_info.get("y_close", 0) if extra_info else 0,
                "tsl_only":    tsl_only,
                "tsl_step":    tsl_step if tsl_step is not None else TRAIL_STEP_PERCENT,
            }

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

            # 3️⃣ Target Limit Order — skip for TSL-only strategies
            if not tsl_only:
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

            # ── Initialise pyramiding state immediately after LIVE entry ──────
            # BUG FIX: previously pyramid_state was only populated AFTER the
            # if/else PAPER block below — which is never reached in LIVE mode
            # because the try block succeeds and falls through to the PAPER check.
            # Now we initialise it here so LIVE pyramiding fires correctly.
            _is_s9_live = tsl_only
            _step_live  = S9_PYRAMID_STEP_PCT if _is_s9_live else PYRAMID_STEP_PCT
            _trig_live  = round(
                ltp * (1 + _step_live / 100) if side == "BUY"
                else ltp * (1 - _step_live / 100), 4
            )
            _base_tgt_live = (
                None if _is_s9_live
                else round(ltp * (1 + TARGET_PERCENT / 100), 4) if side == "BUY"
                else round(ltp * (1 - TARGET_PERCENT / 100), 4)
            )
            pyramid_state[symbol] = {
                "base_entry":   ltp,
                "base_sl":      sl,
                "side":         side,
                "strategy":     condition_name,
                "legs":         0,
                "next_trigger": _trig_live,
                "tsl_only":     _is_s9_live,
                "base_target":  _base_tgt_live,
            }
            print(Fore.CYAN +
                  f"🔺 Pyramid armed (LIVE) | {symbol} | {side} | "
                  f"Base:{ltp} | Next trigger:{_trig_live:.2f} | Step:{_step_live}%" + ts())
            write_log(f"PYRAMID ARMED {symbol} {side} base={ltp} trigger={_trig_live:.2f}")

        except Exception as e:
            print(Fore.RED + f"❌ Live order failed for {symbol}: {e}" + ts())
            write_log(f"LIVE ORDER FAILED {symbol}: {e}")

            # ── Partial failure recovery ─────────────────────────────────
            # Entry filled but SL/target failed → we have an unprotected
            # live position. Cancel what we can and market-exit immediately.
            # For tsl_only trades, target_order_id is intentionally None — that is
            # NOT a failure, so exclude it from the partial-failure condition.
            sl_failed     = sl_order_id is None
            target_failed = (not tsl_only) and (target_order_id is None)
            if entry_order_id and (sl_failed or target_failed):
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
            # Save positions cache so restart can see this entry even if we return now.
            save_paper_positions_cache()
            return

    # ── Store Position ────────────────────────────────────────────────────
    # LIVE: paper_positions was already written inside the try block above
    #       (immediately after entry fills). We only need the PAPER path here.
    # PAPER: first and only write.
    if TRADING_MODE == "PAPER":
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
            "y_close":     extra_info.get("y_close", 0) if extra_info else 0,
            "tsl_only":    tsl_only,
            "tsl_step":    tsl_step if tsl_step is not None else TRAIL_STEP_PERCENT,
        }
        trades_taken[symbol] = True
        save_paper_positions_cache()   # persist immediately on every new trade
    else:
        # LIVE: persist positions cache so restart has correct entry price
        save_paper_positions_cache()

    # ── Initialise pyramiding state for this position ─────────────────────
    # Applies to S1–S8 (fixed target) and S9 (TSL-only, no cap).
    # We store the base entry and compute the first trigger level.
    is_s9 = tsl_only   # S9 is the only strategy that uses tsl_only=True
    step_pct = S9_PYRAMID_STEP_PCT if is_s9 else PYRAMID_STEP_PCT
    if side == "BUY":
        first_trigger = round(ltp * (1 + step_pct / 100), 4)
    else:
        first_trigger = round(ltp * (1 - step_pct / 100), 4)

    # base_target: 1.5% from base entry (used by ALL S1–S8 pyramid legs)
    if not is_s9:
        base_target = (
            round(ltp * (1 + TARGET_PERCENT / 100), 4) if side == "BUY"
            else round(ltp * (1 - TARGET_PERCENT / 100), 4)
        )
    else:
        base_target = None

    pyramid_state[symbol] = {
        "base_entry":   ltp,
        "base_sl":      sl,      # first-entry SL — used as S9 pyramid stop sentinel
        "side":         side,
        "strategy":     condition_name,
        "legs":         0,       # 0 = only base position placed so far
        "next_trigger": first_trigger,
        "tsl_only":     is_s9,
        "base_target":  base_target,
    }


# ==========================================
# PYRAMIDING ENGINE
# ==========================================
# Called on every price tick for open positions.
# Checks if price has moved enough to add the next pyramid leg.
# ──────────────────────────────────────────────────────────────
# S1–S8:
#   • Add a leg every PYRAMID_STEP_PCT (0.30%) from the base entry.
#   • Maximum move for pyramiding: PYRAMID_MAX_PCT (1.20%).
#     → 4 legs possible (at +0.30%, +0.60%, +0.90%, +1.20%).
#   • Each leg gets its own SL = 0.75% from THAT leg's entry price.
#   • ALL legs share the base target = 1.5% above/below base entry.
#
# S9:
#   • Add a leg every S9_PYRAMID_STEP_PCT (0.50%) from the base entry.
#   • No cap — continues as long as position is OPEN.
#   • Each leg: SL = 0.75% from that leg's entry, TSL-only (no target).
#   • Stops adding when the FIRST entry's SL/TSL has been hit (position CLOSED).
# ──────────────────────────────────────────────────────────────
def _pyramid_leg_key(symbol, leg_num):
    """Generate a unique position key for pyramid leg tracking."""
    return f"{symbol}__PYR{leg_num}"

def check_and_add_pyramid(symbol, ltp):
    """
    Called from paper_position_manager and replay exit checks on every price update.
    Adds pyramid legs whenever price crosses the next trigger.

    Uses a WHILE loop so a single large-gap tick (e.g. replay bar jumping
    +0.80% in one candle) correctly fires multiple legs in sequence.

    PAPER: only paper_positions is updated.
    LIVE:  real Kite orders are placed AND paper_positions is updated.
    """
    pyr = pyramid_state.get(symbol)
    if pyr is None:
        return

    # Base position must still be OPEN
    base_pos = paper_positions.get(symbol)
    if base_pos is None or base_pos.get("status") != "OPEN":
        pyramid_state.pop(symbol, None)
        return

    side        = pyr["side"]
    is_s9       = pyr["tsl_only"]
    step_pct    = S9_PYRAMID_STEP_PCT if is_s9 else PYRAMID_STEP_PCT
    max_legs    = 999 if is_s9 else int(round(PYRAMID_MAX_PCT / PYRAMID_STEP_PCT))  # 4 for S1–S8
    base_entry  = pyr["base_entry"]
    base_target = pyr["base_target"]
    tick_size   = tick_sizes.get(symbol, 0.05)

    def _rt(price):
        return round(round(price / tick_size) * tick_size, 10)

    # ── Time guard: no new pyramid legs after ENTRY_END_TIME ───────────
    # Matches the base-entry cutoff so no short-lived legs are created
    # near squareoff. Also prevents any pyramid firing after squareoff starts.
    _now_hhmm = datetime.now().strftime("%H:%M")
    if _now_hhmm >= ENTRY_END_TIME:
        return   # too late in the day to add legs

    # ── WHILE: fire every trigger crossed in one tick (handles gap jumps) ──
    while True:
        current_legs = pyr["legs"]
        if current_legs >= max_legs:
            break   # cap reached

        # ── Cooldown guard: skip if a transient error set a retry-after ──
        _retry_after = pyr.get("_retry_after", 0)
        if _retry_after and time.time() < _retry_after:
            break   # still in cooldown window — don't attempt

        next_trigger = pyr["next_trigger"]
        trigger_hit = (
            (side == "BUY"  and ltp >= next_trigger) or
            (side == "SELL" and ltp <= next_trigger)
        )
        if not trigger_hit:
            break

        leg_num  = current_legs + 1
        leg_key  = _pyramid_leg_key(symbol, leg_num)
        leg_entry = next_trigger

        if side == "BUY":
            leg_sl     = _rt(leg_entry * (1 - PYRAMID_SL_PCT / 100))
            leg_target = _rt(base_target) if not is_s9 else None
        else:
            leg_sl     = _rt(leg_entry * (1 + PYRAMID_SL_PCT / 100))
            leg_target = _rt(base_target) if not is_s9 else None

        # BUG FIX 25: use base position qty not global FIXED_QTY.
        # If FIXED_QTY is changed mid-session the leg qty must match the base.
        qty = base_pos.get("qty", FIXED_QTY)

        # SL sanity guard
        if side == "BUY" and leg_sl >= leg_entry:
            break
        if side == "SELL" and leg_sl <= leg_entry:
            break

        # Dedup guard: don't re-add a leg that already exists (e.g. after restart)
        if leg_key in paper_positions and paper_positions[leg_key].get("status") == "OPEN":
            # Leg already live — advance state and continue checking next trigger
            pyr["legs"] = leg_num
            pyr["next_trigger"] = round(
                base_entry * (1 + (leg_num + 1) * step_pct / 100) if side == "BUY"
                else base_entry * (1 - (leg_num + 1) * step_pct / 100), 4
            )
            continue

        # ── LIVE: place real orders for this pyramid leg ─────────────────
        pyr_sl_oid  = None
        pyr_tgt_oid = None
        if TRADING_MODE == "LIVE":
            try:
                transaction = (kite.TRANSACTION_TYPE_BUY  if side == "BUY"
                               else kite.TRANSACTION_TYPE_SELL)
                exit_txn    = (kite.TRANSACTION_TYPE_SELL if side == "BUY"
                               else kite.TRANSACTION_TYPE_BUY)

                pyr_entry_oid = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol,
                    transaction_type=transaction,
                    quantity=qty,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_MIS
                )
                time.sleep(0.3)

                pyr_sl_oid = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol,
                    transaction_type=exit_txn,
                    quantity=qty,
                    order_type=kite.ORDER_TYPE_SLM,
                    trigger_price=leg_sl,
                    product=kite.PRODUCT_MIS
                )

                if not is_s9:
                    pyr_tgt_oid = kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange=kite.EXCHANGE_NSE,
                        tradingsymbol=symbol,
                        transaction_type=exit_txn,
                        quantity=qty,
                        order_type=kite.ORDER_TYPE_LIMIT,
                        price=leg_target,
                        product=kite.PRODUCT_MIS
                    )

                print(Fore.CYAN +
                    f"🔺 PYRAMID LEG {leg_num} (LIVE) | {symbol} | {side} | "
                    f"Entry:{leg_entry:.2f} SL:{leg_sl:.2f} "
                    f"{'TSL-Only' if is_s9 else f'Target:{leg_target:.2f}'} | "
                    f"EntryOID:{pyr_entry_oid} SLOID:{pyr_sl_oid}" + ts())
                write_log(
                    f"PYRAMID LIVE Leg{leg_num} {symbol} {side} "
                    f"Entry:{leg_entry:.2f} SL:{leg_sl:.2f} "
                    f"{'TSL-Only' if is_s9 else f'Target:{leg_target:.2f}'}"
                )

            except Exception as e:
                err_str = str(e).lower()
                print(Fore.RED + f"❌ Pyramid LIVE order failed {symbol} Leg{leg_num}: {e}" + ts())
                write_log(f"PYRAMID LIVE ORDER FAILED {symbol} Leg{leg_num}: {e}")

                # ── Classify error: permanent vs transient ────────────────
                # Permanent = no point retrying this tick or next tick.
                # Advance pyramid_state past this leg so we stop hammering
                # the API every 0.1s. For insufficient funds, skip ALL
                # remaining legs too (cap reached for this position).
                _permanent_errors = (
                    "insufficient" in err_str or
                    "margin"       in err_str or
                    "funds"        in err_str or
                    "rejected"     in err_str or
                    "not allowed"  in err_str or
                    "invalid"      in err_str
                )
                if _permanent_errors:
                    # Advance state past this leg — prevents infinite retry loop.
                    # Set next_trigger to an unreachable level (beyond max_legs cap)
                    # so the while loop exits naturally on next check.
                    pyr["legs"] = max_legs   # mark as capped — no more legs
                    print(Fore.YELLOW +
                          f"⛔ Pyramid CAPPED for {symbol} (permanent error: funds/margin) "
                          f"— no more legs will be attempted" + ts())
                    write_log(f"PYRAMID CAPPED {symbol} permanent error")
                else:
                    # Transient error (network, timeout) — set a 30s cooldown
                    # so we don't spam the API every 0.1s.
                    pyr["_retry_after"] = time.time() + 30
                    write_log(f"PYRAMID RETRY COOLDOWN {symbol} 30s")
                break   # stop adding more legs this cycle

        # ── Store leg in paper_positions ─────────────────────────────────
        entry_time_str = datetime.now().strftime("%H:%M")
        paper_positions[leg_key] = {
            "side":         side,
            "entry":        leg_entry,
            "sl":           leg_sl,
            "target":       leg_target,
            "qty":          qty,
            "trail_level":  0,
            "status":       "OPEN",
            "strategy":     base_pos.get("strategy", "") + f" [PYR{leg_num}]",
            "entry_time":   entry_time_str,
            "y_close":      base_pos.get("y_close", 0),
            "tsl_only":     is_s9,
            "tsl_step":     base_pos.get("tsl_step", TRAIL_STEP_PERCENT),
            "pyramid_base": symbol,
            "pyramid_leg":  leg_num,
            # LIVE order IDs stored so oco_cascade can cancel them
            "_live_sl_oid":  pyr_sl_oid,
            "_live_tgt_oid": pyr_tgt_oid,
        }

        color = Fore.CYAN if side == "BUY" else Fore.MAGENTA
        print(color +
            f"🔺 PYRAMID LEG {leg_num} | {symbol} | {side} | "
            f"Entry:{leg_entry:.2f} | SL:{leg_sl:.2f} | "
            f"{'TSL-Only' if is_s9 else f'Target:{leg_target:.2f} (base)'} | "
            f"Qty:{qty}" + ts())
        write_log(
            f"PYRAMID Leg{leg_num} {symbol} {side} "
            f"Entry:{leg_entry:.2f} SL:{leg_sl:.2f} "
            f"{'TSL-Only' if is_s9 else f'Target:{leg_target:.2f}'}"
        )

        # ── Advance pyramid state ────────────────────────────────────────
        pyr["legs"] = leg_num
        pyr["next_trigger"] = round(
            base_entry * (1 + (leg_num + 1) * step_pct / 100) if side == "BUY"
            else base_entry * (1 - (leg_num + 1) * step_pct / 100), 4
        )

        # BUG FIX: persist positions cache immediately after each pyramid leg
        # so a crash/restart doesn't lose the leg. Without this, legs exist only
        # in memory — a crash between two legs means the restart sees 0 legs
        # and fires them all again, placing duplicate LIVE orders.
        save_paper_positions_cache()


# ==========================================
# OCO MONITOR  (LIVE MODE)
# ==========================================
def _cancel_live_order_safe(order_id, label=""):
    """Cancel a Kite order silently — used for pyramid leg cleanup."""
    if not order_id:
        return
    try:
        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=order_id)
    except Exception as ce:
        write_log(f"Cancel {label} OID:{order_id} failed: {ce}")

def _cascade_cancel_live_pyramid_legs(base_symbol, exit_price=None):
    """
    LIVE mode: cancel all open SL-M and target orders for pyramid legs
    tied to base_symbol, then mark those leg positions as CLOSED.
    Called when the base position's SL or target fires at the broker.
    exit_price: the base fill price — used to compute leg PnL.
    If not supplied, falls back to latest_prices for the symbol.
    """
    base_side = paper_positions.get(base_symbol, {}).get("side", "BUY")
    exit_txn  = "BUY" if base_side == "SELL" else "SELL"

    # ── Broad cancel: fetch all open orders and cancel any that belong to
    # this symbol's exit direction (catches dangling orders whose IDs were
    # not saved in paper_positions, e.g. from a previous crashed session).
    try:
        all_open = kite.orders()
        for _o in all_open:
            if (_o.get("tradingsymbol") == base_symbol and
                    _o.get("transaction_type") == exit_txn and
                    _o.get("status") in ("OPEN", "TRIGGER PENDING") and
                    _o.get("product") == "MIS"):
                _cancel_live_order_safe(str(_o["order_id"]), f"broad cascade {base_symbol}")
    except Exception as _ce:
        write_log(f"Broad cascade cancel failed {base_symbol}: {_ce}")

    # ── Close all local pyramid leg entries ───────────────────────────────
    for leg_key, leg_pos in list(paper_positions.items()):
        if leg_pos.get("pyramid_base") != base_symbol:
            continue
        if leg_pos.get("status") != "OPEN":
            continue
        leg_pos["status"] = "CLOSED"
        _ep = exit_price or latest_prices.get(base_symbol, leg_pos["entry"])
        _pts = (_ep - leg_pos["entry"]) if base_side == "BUY" else (leg_pos["entry"] - _ep)
        leg_pos["net_pnl"] = round(_pts * leg_pos["qty"] - BROKERAGE_PER_ORDER * 2, 2)
        print(Fore.YELLOW + f"🔺 PYRAMID LEG CANCELLED (cascade) | {leg_key} | NetPnL:{leg_pos['net_pnl']:.2f}" + ts())
        write_log(f"PYRAMID CASCADE CANCEL {leg_key} NetPnL:{leg_pos['net_pnl']:.2f}")
    pyramid_state.pop(base_symbol, None)


def oco_monitor(symbol):
    """
    Watches SL and Target orders for a live BASE position.
    - SL or target fill → cancel the other, cascade-cancel pyramid legs, record PnL.
    - TSL-only (S9): trails base SL-M AND all pyramid leg SL-Ms on every poll.
    - Transient "order not found": retries up to 5 times before giving up.
    - Network errors: retries up to 10 times with 5s back-off.
    """
    consecutive_errors = 0
    sl_not_found_count  = 0
    tgt_not_found_count = 0
    # FIX 1: Increased from 5→30 retries (60s tolerance).
    # At restart, 6+ oco_monitors start simultaneously and pyramid may fire
    # within seconds, all hammering kite.orders() at once. The original 10s
    # (5 retries × 2s) was far too short — API rate-limit or propagation
    # delay caused all monitors to give up and leave positions unprotected.
    MAX_NOT_FOUND = 30

    # FIX 1b: Initial startup delay — let the Kite order book fully populate
    # before first poll. Prevents false "not found" on the very first call
    # when the session has just connected and orders haven't propagated yet.
    time.sleep(3)

    while True:
        try:
            trade = trades_taken.get(symbol)
            if not isinstance(trade, dict):
                break   # position already closed or paper mode

            base_pos = paper_positions.get(symbol, {})
            if base_pos.get("status") == "CLOSED":
                break   # already closed locally — stop monitoring

            orders = kite.orders()
            sl_id  = trade["sl"]
            tgt_id = trade.get("target")   # None for tsl_only (S9)

            sl_order = next((o for o in orders if str(o["order_id"]) == str(sl_id)), None)

            # ── SL not found by ID: try fallback scan by symbol+type ─────
            # Covers the case where the stored order ID was corrupted or
            # the order was re-placed (e.g. after partial failure recovery).
            if sl_order is None:
                base_side = base_pos.get("side", "BUY")
                exit_txn  = "BUY" if base_side == "SELL" else "SELL"
                fallback  = next((
                    o for o in orders
                    if o.get("tradingsymbol") == symbol
                    and o.get("transaction_type") == exit_txn
                    and o.get("order_type") in ("SL-M", "SL", "SLM")
                    and o.get("status") in ("TRIGGER PENDING", "OPEN")
                ), None)
                if fallback:
                    new_sl_id = str(fallback["order_id"])
                    write_log(f"OCO: SL ID mismatch {symbol} — fallback to {new_sl_id} (was {sl_id})")
                    print(Fore.YELLOW + f"⚠️  OCO: SL ID updated by fallback | {symbol} | {new_sl_id}" + ts())
                    trade["sl"] = new_sl_id
                    sl_id = new_sl_id
                    sl_order = fallback
                    sl_not_found_count = 0
                    save_trades_cache()   # persist corrected ID so next restart uses it directly

            # ── SL order not found: transient or permanent ────────────────
            if sl_order is None:
                sl_not_found_count += 1
                if sl_not_found_count <= MAX_NOT_FOUND:
                    write_log(f"OCO: SL not found {symbol} (attempt {sl_not_found_count}/{MAX_NOT_FOUND})")
                    time.sleep(2)
                    continue
                print(Fore.RED +
                      f"⚠️  OCO: SL permanently missing for {symbol} after {MAX_NOT_FOUND} retries — stopping monitor" + ts())
                write_log(f"OCO SL permanently missing {symbol}")
                break
            sl_not_found_count = 0   # reset on successful find

            # ── SL Hit ───────────────────────────────────────────────────
            if sl_order["status"] == "COMPLETE":
                trade_statistics["sl_hits"] += 1
                if tgt_id:
                    _cancel_live_order_safe(tgt_id, f"{symbol} TARGET")

                fill_price = float(sl_order.get("average_price") or
                                   sl_order.get("trigger_price") or
                                   base_pos.get("sl", 0))
                entry_p  = float(base_pos.get("entry", 0))
                side_p   = base_pos.get("side", "BUY")
                qty_p    = int(base_pos.get("qty", FIXED_QTY))
                points_p = (fill_price - entry_p) if side_p == "BUY" else (entry_p - fill_price)
                net_p    = round(points_p * qty_p - BROKERAGE_PER_ORDER * 2, 2)
                base_pos["status"]  = "CLOSED"
                base_pos["net_pnl"] = net_p

                msg = (f"🔴 {symbol} STOP LOSS HIT | Fill:{fill_price:.2f} NetPnL:{net_p:.2f}" +
                       (" | Target cancelled" if tgt_id else " | TSL-Only closed"))
                print(Fore.YELLOW + msg + ts())
                write_log(msg)
                with open(paper_trade_log_file, "a", newline="") as _pf:
                    csv.writer(_pf).writerow([
                        datetime.now(), symbol, side_p,
                        entry_p, fill_price, qty_p, net_p, "SL HIT"
                    ])
                # BUG FIX 14: compute PnL for every open pyramid leg at the SL fill
                # price, write to trade log, update daily_pnl, then cancel orders.
                for _lk, _lp in list(paper_positions.items()):
                    if _lp.get("pyramid_base") != symbol or _lp.get("status") != "OPEN":
                        continue
                    _l_pts = (fill_price - _lp["entry"]) if side_p == "BUY" else (_lp["entry"] - fill_price)
                    _l_net = round(_l_pts * _lp["qty"] - BROKERAGE_PER_ORDER * 2, 2)
                    _lp["net_pnl"] = _l_net
                    with open(paper_trade_log_file, "a", newline="") as _pf2:
                        csv.writer(_pf2).writerow([
                            datetime.now(), _lk, side_p,
                            _lp["entry"], fill_price, _lp["qty"], _l_net, "SL HIT (cascade)"
                        ])
                    write_log(f"PYR LEG SL HIT {_lk} Entry:{_lp['entry']} Exit:{fill_price} NetPnL:{_l_net:.2f}")
                _cascade_cancel_live_pyramid_legs(symbol, exit_price=fill_price)
                # BUG FIX 14b: save positions cache after base+legs are closed
                save_paper_positions_cache()
                break

            # ── Target Hit ───────────────────────────────────────────────
            if tgt_id:
                tgt_order = next((o for o in orders if str(o["order_id"]) == str(tgt_id)), None)

                # ── Target not found by ID: try fallback scan by symbol+type ──
                if tgt_order is None:
                    base_side_t = base_pos.get("side", "BUY")
                    exit_txn_t  = "BUY" if base_side_t == "SELL" else "SELL"
                    tgt_price   = base_pos.get("target")
                    fallback_t  = next((
                        o for o in orders
                        if o.get("tradingsymbol") == symbol
                        and o.get("transaction_type") == exit_txn_t
                        and o.get("order_type") == "LIMIT"
                        and o.get("status") in ("OPEN",)
                        and (tgt_price is None or abs(float(o.get("price", 0)) - float(tgt_price)) < 1.0)
                    ), None)
                    if fallback_t:
                        new_tgt_id = str(fallback_t["order_id"])
                        write_log(f"OCO: Target ID mismatch {symbol} — fallback to {new_tgt_id} (was {tgt_id})")
                        print(Fore.YELLOW + f"⚠️  OCO: Target ID updated by fallback | {symbol} | {new_tgt_id}" + ts())
                        trade["target"] = new_tgt_id
                        tgt_id   = new_tgt_id
                        tgt_order = fallback_t
                        tgt_not_found_count = 0
                        save_trades_cache()   # persist corrected ID so next restart uses it directly

                if tgt_order is None:
                    tgt_not_found_count += 1
                    if tgt_not_found_count <= MAX_NOT_FOUND:
                        write_log(f"OCO: target not found {symbol} (attempt {tgt_not_found_count}/{MAX_NOT_FOUND})")
                        time.sleep(2)
                        continue
                    # Target permanently missing — log warning but DO NOT stop monitoring.
                    # The position still has a valid SL order protecting it.
                    # Stopping the monitor here would leave the position completely unprotected.
                    # Instead: mark tgt_id=None so we skip target checks going forward,
                    # and keep the SL monitor running (TSL will protect the position).
                    print(Fore.YELLOW +
                          f"⚠️  OCO: target order missing for {symbol} after {MAX_NOT_FOUND} retries "
                          f"— continuing SL-only monitoring (position still protected)" + ts())
                    write_log(f"OCO target missing {symbol} — switching to SL-only mode")
                    trade["target"] = None   # stop checking target; SL still active
                    tgt_id = None
                    tgt_not_found_count = 0
                    continue
                tgt_not_found_count = 0

                if tgt_order["status"] == "COMPLETE":
                    trade_statistics["target_hits"] += 1
                    _cancel_live_order_safe(sl_id, f"{symbol} SL")

                    fill_price = float(tgt_order.get("average_price") or
                                       base_pos.get("target", 0))
                    entry_p  = float(base_pos.get("entry", 0))
                    side_p   = base_pos.get("side", "BUY")
                    qty_p    = int(base_pos.get("qty", FIXED_QTY))
                    points_p = (fill_price - entry_p) if side_p == "BUY" else (entry_p - fill_price)
                    net_p    = round(points_p * qty_p - BROKERAGE_PER_ORDER * 2, 2)
                    base_pos["status"]  = "CLOSED"
                    base_pos["net_pnl"] = net_p

                    msg = (f"🎯 {symbol} TARGET HIT | Fill:{fill_price:.2f} "
                           f"NetPnL:{net_p:.2f} | SL cancelled")
                    print(Fore.BLUE + msg + ts())
                    write_log(msg)
                    with open(paper_trade_log_file, "a", newline="") as _pf:
                        csv.writer(_pf).writerow([
                            datetime.now(), symbol, side_p,
                            entry_p, fill_price, qty_p, net_p, "TARGET HIT"
                        ])
                    # BUG FIX 15: compute PnL for every open pyramid leg at the
                    # target fill price, write to trade log, update daily_pnl.
                    for _lk, _lp in list(paper_positions.items()):
                        if _lp.get("pyramid_base") != symbol or _lp.get("status") != "OPEN":
                            continue
                        _l_pts = (fill_price - _lp["entry"]) if side_p == "BUY" else (_lp["entry"] - fill_price)
                        _l_net = round(_l_pts * _lp["qty"] - BROKERAGE_PER_ORDER * 2, 2)
                        _lp["net_pnl"] = _l_net
                        with open(paper_trade_log_file, "a", newline="") as _pf2:
                            csv.writer(_pf2).writerow([
                                datetime.now(), _lk, side_p,
                                _lp["entry"], fill_price, _lp["qty"], _l_net, "TARGET HIT (cascade)"
                            ])
                        write_log(f"PYR LEG TGT HIT {_lk} Entry:{_lp['entry']} Exit:{fill_price} NetPnL:{_l_net:.2f}")
                    _cascade_cancel_live_pyramid_legs(symbol, exit_price=fill_price)
                    # BUG FIX 15b: save positions cache after base+legs are closed
                    save_paper_positions_cache()
                    break

            # ── Live TSL: only for tsl_only positions (S9) ───────────────
            # Using pos.get("tsl_only") instead of tgt_id is None avoids
            # accidentally trailing SL on non-TSL positions that somehow
            # lost their target OID (e.g. after a partial-failure recovery).
            if base_pos.get("tsl_only") and base_pos.get("status") == "OPEN":
                current_sl = base_pos.get("sl")
                if current_sl and current_sl != trade.get("_last_live_sl"):
                    try:
                        kite.modify_order(
                            variety=kite.VARIETY_REGULAR,
                            order_id=sl_id,
                            trigger_price=current_sl
                        )
                        trade["_last_live_sl"] = current_sl
                        print(Fore.YELLOW +
                              f"🔄 LIVE TSL UPDATED | {symbol} | New SL:{current_sl}" + ts())
                        write_log(f"LIVE TSL UPDATED {symbol} SL:{current_sl}")
                    except Exception as te:
                        print(Fore.YELLOW + f"⚠️  TSL modify failed {symbol}: {te}" + ts())

                # Trail each S9 pyramid leg SL-M
                for leg_key, leg_pos in list(paper_positions.items()):
                    if leg_pos.get("pyramid_base") != symbol:
                        continue
                    if leg_pos.get("status") != "OPEN":
                        continue
                    leg_sl_oid = leg_pos.get("_live_sl_oid")
                    if not leg_sl_oid:
                        continue
                    new_leg_sl  = leg_pos.get("sl")
                    last_leg_sl = leg_pos.get("_last_live_sl")
                    if new_leg_sl and new_leg_sl != last_leg_sl:
                        try:
                            kite.modify_order(
                                variety=kite.VARIETY_REGULAR,
                                order_id=leg_sl_oid,
                                trigger_price=new_leg_sl
                            )
                            leg_pos["_last_live_sl"] = new_leg_sl
                            print(Fore.YELLOW +
                                  f"🔄 LIVE TSL UPDATED | {leg_key} | New SL:{new_leg_sl}" + ts())
                            write_log(f"LIVE TSL UPDATED {leg_key} SL:{new_leg_sl}")
                        except Exception as te:
                            print(Fore.YELLOW +
                                  f"⚠️  Pyramid TSL modify failed {leg_key}: {te}" + ts())

            # ── Check individual pyramid leg SL/target fills ─────────────
            # Each PYR leg has its own broker SL-M and target order.
            # When they fill, close the leg locally and log PnL.
            # This runs every poll cycle regardless of base position state.
            _orders_by_id = {str(o["order_id"]): o for o in orders}
            for _lk, _lp in list(paper_positions.items()):
                if _lp.get("pyramid_base") != symbol:
                    continue
                if _lp.get("status") != "OPEN":
                    continue
                _leg_sl_oid  = str(_lp.get("_live_sl_oid",  "") or "")
                _leg_tgt_oid = str(_lp.get("_live_tgt_oid", "") or "")
                _leg_exit_price = None
                _leg_exit_reason = None

                if _leg_sl_oid and _leg_sl_oid in _orders_by_id:
                    _lo = _orders_by_id[_leg_sl_oid]
                    if _lo["status"] == "COMPLETE":
                        _leg_exit_price  = float(_lo.get("average_price") or _lo.get("trigger_price") or _lp["sl"])
                        _leg_exit_reason = "SL HIT"
                        # Cancel the target order for this leg
                        if _leg_tgt_oid:
                            _cancel_live_order_safe(_leg_tgt_oid, f"leg tgt {_lk}")

                if _leg_exit_price is None and _leg_tgt_oid and _leg_tgt_oid in _orders_by_id:
                    _to = _orders_by_id[_leg_tgt_oid]
                    if _to["status"] == "COMPLETE":
                        _leg_exit_price  = float(_to.get("average_price") or _lp.get("target") or 0)
                        _leg_exit_reason = "TARGET HIT"
                        # Cancel the SL order for this leg
                        if _leg_sl_oid:
                            _cancel_live_order_safe(_leg_sl_oid, f"leg sl {_lk}")

                if _leg_exit_price is not None:
                    _base_side = base_pos.get("side", "BUY")
                    _l_pts = (_leg_exit_price - _lp["entry"]) if _base_side == "BUY" else (_lp["entry"] - _leg_exit_price)
                    _l_net = round(_l_pts * _lp["qty"] - BROKERAGE_PER_ORDER * 2, 2)
                    _lp["status"]  = "CLOSED"
                    _lp["net_pnl"] = _l_net
                    print(Fore.YELLOW +
                          f"🔺 PYR LEG {_leg_exit_reason} | {_lk} | "
                          f"Entry:{_lp['entry']:.2f} Exit:{_leg_exit_price:.2f} | "
                          f"NetPnL:{_l_net:.2f}" + ts())
                    write_log(f"PYR LEG {_leg_exit_reason} {_lk} Entry:{_lp['entry']:.2f} "
                              f"Exit:{_leg_exit_price:.2f} NetPnL:{_l_net:.2f}")
                    with open(paper_trade_log_file, "a", newline="") as _pf3:
                        csv.writer(_pf3).writerow([
                            datetime.now(), _lk, _base_side,
                            _lp["entry"], _leg_exit_price, _lp["qty"], _l_net,
                            _leg_exit_reason + " (leg)"
                        ])
                    save_paper_positions_cache()

            consecutive_errors = 0
            time.sleep(2)

        except Exception as e:
            consecutive_errors += 1
            print(Fore.RED +
                  f"⚠️  OCO poll error {symbol} (attempt {consecutive_errors}): {e}" + ts())
            write_log(f"OCO error {symbol}: {e}")
            if consecutive_errors >= 10:
                print(Fore.RED + f"❌ OCO monitor giving up on {symbol} after 10 errors" + ts())
                write_log(f"OCO MONITOR ABANDONED {symbol}")
                break
            time.sleep(5)


# ==========================================
# YESTERDAY DATA CACHE WITH LOCAL STORAGE
# ==========================================
YESTERDAY_CACHE_FILE = "yesterday_ohlc_cache.csv"

yesterday_data    = {}   # symbol → {open, high, low, close, volume}
live_day_extremes = {}   # symbol → {high, low} (intraday, not used by current strategies)

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
            "open":   yest["open"],
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

_save_orb_cache_silent = False   # set True to suppress console output

def _orb_cache_file():
    return f"orb_cache_{datetime.now().strftime('%Y-%m-%d')}.csv"

def _save_orb_cache(silent=False):
    global _save_orb_cache_silent
    _save_orb_cache_silent = silent
    """Write ORB, F15, and C2/C3/C4 to today's cache CSV.
    Called at each window freeze (09:30, 09:45, 10:00, 10:15) and by the
    5-min refresh loop — so the file always reflects the latest settled values.
    """
    rows = []
    for symbol in SYMBOLS:
        orb = orb_data.get(symbol)
        f15 = first15_data.get(symbol)
        if orb and f15:
            c2 = second15_data.get(symbol, {})
            c3 = third15_data.get(symbol,  {})
            c4 = fourth15_data.get(symbol, {})
            f5 = first5_data.get(symbol,   {})
            rows.append({
                "symbol":    symbol,
                "orb_high":  orb["high"],
                "orb_low":   orb["low"],
                "orb_ready": orb["ready"],
                "f15_open":  f15["open"],
                "f15_high":  f15["high"],
                "f15_low":   f15["low"],
                "f15_close": f15["close"],
                "f15_ready": f15["ready"],
                # C2 (09:30–09:44)
                "c2_open":  c2.get("open",  0),
                "c2_high":  c2.get("high",  0),
                "c2_low":   c2.get("low",   999999),
                "c2_close": c2.get("close", 0),
                "c2_ready": c2.get("ready", False),
                # C3 (09:45–09:59)
                "c3_high":  c3.get("high",  0),
                "c3_low":   c3.get("low",   999999),
                "c3_ready": c3.get("ready", False),
                # C4 (10:00–10:14)
                "c4_high":  c4.get("high",  0),
                "c4_low":   c4.get("low",   999999),
                "c4_ready": c4.get("ready", False),
                # F5 (09:15–09:19) — S9 Open=High Breakdown
                "f5_open":  f5.get("open",  0),
                "f5_high":  f5.get("high",  0),
                "f5_low":   f5.get("low",   999999),
                "f5_close": f5.get("close", 0),
                "f5_ready": f5.get("ready", False),
                # O==H / O==L flags (0.1% tolerance, same as strategy_open_high_breakdown)
                # TRUE only when f5 is ready and condition met; empty string otherwise.
                "f5_o_eq_h": (
                    True if (
                        f5.get("ready", False) and f5.get("open", 0) > 0 and
                        abs(f5.get("open", 0) - f5.get("high", 0)) / f5.get("open", 1) < 0.001
                    ) else ""
                ),
                "f5_o_eq_l": (
                    True if (
                        f5.get("ready", False) and f5.get("open", 0) > 0 and
                        abs(f5.get("open", 0) - f5.get("low", 999999)) / f5.get("open", 1) < 0.001
                    ) else ""
                ),
            })
    if rows:
        pd.DataFrame(rows).to_csv(_orb_cache_file(), index=False)
        if not _save_orb_cache_silent:
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

            # ── Restore C2/C3/C4 if present (backward compat: old cache has no c2/c3/c4 cols) ──
            # Only restore if the saved high is real (> 0), not a 0/999999 placeholder.
            if "c2_high" in row and pd.notna(row["c2_high"]) and float(row["c2_high"]) > 0:
                second15_data[symbol] = {
                    "open":  float(row["c2_open"]),
                    "high":  float(row["c2_high"]),
                    "low":   float(row["c2_low"]),
                    "close": float(row["c2_close"]),
                    "ready": str(row["c2_ready"]).lower() == "true"
                }
            if "c3_high" in row and pd.notna(row["c3_high"]) and float(row["c3_high"]) > 0:
                third15_data[symbol] = {
                    "high":  float(row["c3_high"]),
                    "low":   float(row["c3_low"]),
                    "ready": str(row["c3_ready"]).lower() == "true"
                }
            if "c4_high" in row and pd.notna(row["c4_high"]) and float(row["c4_high"]) > 0:
                fourth15_data[symbol] = {
                    "high":  float(row["c4_high"]),
                    "low":   float(row["c4_low"]),
                    "ready": str(row["c4_ready"]).lower() == "true"
                }
            # F5 (09:15–09:19) — S9 Open=High Breakdown
            # Only restore if high is real (> 0) and low is valid (< 999999)
            if ("f5_high" in row and pd.notna(row["f5_high"]) and float(row["f5_high"]) > 0
                    and float(row.get("f5_low", 999999)) < 999999):
                first5_data[symbol] = {
                    "open":  float(row["f5_open"]),
                    "high":  float(row["f5_high"]),
                    "low":   float(row["f5_low"]),
                    "close": float(row["f5_close"]),
                    "ready": str(row["f5_ready"]).lower() == "true"
                }

            # Seed latest_highs/lows from the best known values so far.
            # ORB and F15 both cover early morning — take the wider range.
            # WS ticks will extend these further via max/min as they arrive.
            known_high = max(float(row["orb_high"]), float(row["f15_high"]))
            known_low  = min(float(row["orb_low"]),  float(row["f15_low"]))
            latest_highs[symbol] = max(latest_highs.get(symbol, 0),      known_high)
            latest_lows[symbol]  = min(latest_lows.get(symbol,  999999), known_low)

        print(f"📂 ORB cache loaded from {path}  ({len(df)} symbols) — skipping API fetch" + ts())
        # _refresh_orb_cache_loop runs immediately at startup and will seed
        # Day H/L + C2/C3/C4 from the API within seconds — no separate seed needed.
        return True

    except Exception as e:
        print(f"⚠️  ORB cache read failed ({e}), will fetch from API" + ts())
        return False


ORB_REFRESH_INTERVAL = 300   # seconds — refresh every 5 minutes

def _refresh_orb_cache_loop():
    """
    Background thread: runs immediately at startup, then every ORB_REFRESH_INTERVAL
    seconds. Re-fetches today's minute bars and refreshes all candle structures +
    latest_highs/lows in memory, then re-saves the cache CSV.

    Replaces the one-shot _seed_all_from_api() — this loop handles both the
    initial seed on startup AND the periodic 5-min corrections thereafter.
    """
    first_run = True

    while True:
        try:
            now_time = datetime.now().strftime("%H:%M")

            # Stop permanently after square-off — market is closed, no more data needed
            if now_time >= SQUARE_OFF_TIME:
                print(Fore.CYAN + f"🛑 ORB refresh loop stopped (after {SQUARE_OFF_TIME} square-off)" + ts())
                return

            if now_time < "09:15":
                time.sleep(ORB_REFRESH_INTERVAL)
                continue

            today           = datetime.now().date()
            from_dt         = datetime(today.year, today.month, today.day, 9, 15)
            to_dt           = datetime.now()
            symbol_to_token = {v: k for k, v in instrument_tokens.items()}
            refreshed       = 0

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

                    # ── Full day H/L — always update ────────────────────────
                    full_df = df[df["time_str"] >= "09:15"]
                    if not full_df.empty:
                        latest_highs[symbol] = max(latest_highs.get(symbol, 0),      float(full_df["high"].max()))
                        latest_lows[symbol]  = min(latest_lows.get(symbol,  999999), float(full_df["low"].min()))

                    # ── ORB (09:15–10:14): only extend while open ───────────
                    if now_time < "10:15":
                        orb_df = df[(df["time_str"] >= "09:15") & (df["time_str"] < "10:15")]
                        if not orb_df.empty:
                            if symbol in orb_data:
                                orb_data[symbol]["high"] = max(orb_data[symbol]["high"], float(orb_df["high"].max()))
                                orb_data[symbol]["low"]  = min(orb_data[symbol]["low"],  float(orb_df["low"].min()))
                            else:
                                orb_data[symbol] = {"high": float(orb_df["high"].max()), "low": float(orb_df["low"].min()), "ready": False}

                    # ── F15/C1 (09:15–09:29): only extend while open ────────
                    if now_time < "09:30":
                        f15_df = df[(df["time_str"] >= "09:15") & (df["time_str"] < "09:30")]
                        if not f15_df.empty:
                            if symbol in first15_data:
                                first15_data[symbol]["high"]  = max(first15_data[symbol]["high"],  float(f15_df["high"].max()))
                                first15_data[symbol]["low"]   = min(first15_data[symbol]["low"],   float(f15_df["low"].min()))
                                first15_data[symbol]["close"] = float(f15_df.iloc[-1]["close"])
                            else:
                                first15_data[symbol] = {"open": float(f15_df.iloc[0]["open"]), "high": float(f15_df["high"].max()), "low": float(f15_df["low"].min()), "close": float(f15_df.iloc[-1]["close"]), "ready": False}


                    # ── F5 (09:15–09:19): seed/correct first5_data from API ─
                    # S9 (Open=High Breakdown) needs this candle. _build_first5()
                    # only runs via WebSocket ticks, so on restart after 09:20
                    # first5_data is empty. Seed it here so S9 works post-restart
                    # and so the ORB cache CSV always stores valid f5 values.
                    f5_df = df[(df["time_str"] >= "09:15") & (df["time_str"] < "09:20")]
                    if not f5_df.empty:
                        existing_f5 = first5_data.get(symbol, {})
                        first5_data[symbol] = {
                            "open":  existing_f5.get("open") or float(f5_df.iloc[0]["open"]),
                            "high":  max(existing_f5.get("high", 0),      float(f5_df["high"].max())),
                            "low":   min(existing_f5.get("low",  999999), float(f5_df["low"].min())),
                            "close": float(f5_df.iloc[-1]["close"]),
                            "ready": now_time >= "09:20",
                        }
                    # ── C2 (09:30–09:44): extend if open, correct if closed ─
                    c2_df = df[(df["time_str"] >= "09:30") & (df["time_str"] < "09:45")]
                    if not c2_df.empty:
                        api_c2_high = float(c2_df["high"].max())
                        api_c2_low  = float(c2_df["low"].min())
                        existing    = second15_data.get(symbol, {})
                        second15_data[symbol] = {
                            "open":  existing.get("open") or float(c2_df.iloc[0]["open"]),
                            "high":  max(existing.get("high", 0),      api_c2_high),
                            "low":   min(existing.get("low",  999999), api_c2_low),
                            "close": float(c2_df.iloc[-1]["close"]) if now_time < "09:45" else existing.get("close", float(c2_df.iloc[-1]["close"])),
                            "ready": now_time >= "09:45"
                        }

                    # ── C3 (09:45–09:59) ────────────────────────────────────
                    c3_df = df[(df["time_str"] >= "09:45") & (df["time_str"] < "10:00")]
                    if not c3_df.empty:
                        api_c3_high = float(c3_df["high"].max())
                        api_c3_low  = float(c3_df["low"].min())
                        existing    = third15_data.get(symbol, {})
                        third15_data[symbol] = {
                            "high":  max(existing.get("high", 0),      api_c3_high),
                            "low":   min(existing.get("low",  999999), api_c3_low),
                            "ready": now_time >= "10:00"
                        }

                    # ── C4 (10:00–10:14) ────────────────────────────────────
                    c4_df = df[(df["time_str"] >= "10:00") & (df["time_str"] < "10:15")]
                    if not c4_df.empty:
                        api_c4_high = float(c4_df["high"].max())
                        api_c4_low  = float(c4_df["low"].min())
                        existing    = fourth15_data.get(symbol, {})
                        fourth15_data[symbol] = {
                            "high":  max(existing.get("high", 0),      api_c4_high),
                            "low":   min(existing.get("low",  999999), api_c4_low),
                            "ready": now_time >= "10:15"
                        }

                    refreshed += 1
                    time.sleep(0.05)

                except Exception:
                    pass

            _save_orb_cache()
            label = "seeded at startup" if first_run else "refreshed from API"
            print(Fore.CYAN + f"🔄 ORB+C2/C3/C4 {label} ({refreshed} symbols)" + ts())
            first_run = False

        except Exception as e:
            print(Fore.YELLOW + f"⚠️  ORB refresh loop error: {e}" + ts())
            first_run = False

        # ── Smart sleep: wake at 09:20 if we're still pre-F5-freeze ────────
        # The first5_data candle freezes at 09:20. If the current cycle
        # finished before 09:20, don't wait a full 5 minutes — sleep only
        # until 09:20:05 so we immediately seed F5 and save a fresh cache.
        _now = datetime.now()
        _now_time = _now.strftime("%H:%M")
        if _now_time < "09:20":
            _target = _now.replace(hour=9, minute=20, second=5, microsecond=0)
            _sleep_secs = max(5, (_target - _now).total_seconds())
            time.sleep(_sleep_secs)
        else:
            time.sleep(ORB_REFRESH_INTERVAL)


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

    def _close_position(sym, p, exit_price, exit_reason, ltp_display):
        """Mark one paper position CLOSED, update PnL, stats, log, and CSV."""
        global daily_pnl
        entry_p  = p["entry"]
        side_p   = p["side"]
        qty_p    = p["qty"]
        points_p = (exit_price - entry_p) if side_p == "BUY" else (entry_p - exit_price)
        net_p    = points_p * qty_p - BROKERAGE_PER_ORDER * 2
        p["status"]  = "CLOSED"
        p["net_pnl"] = net_p
        daily_pnl   += net_p

        is_pyr_leg = "__PYR" in str(sym)

        # BUG FIX: strip [PYRn] suffix so PYR leg PnL rolls up under the parent
        # strategy, not a phantom "ORB Break [PYR1]" entry in strategy_stats.
        strat_n = p.get("strategy", "Unknown")
        if is_pyr_leg:
            strat_n = strat_n.split(" [PYR")[0]
        if strat_n not in strategy_stats:
            strategy_stats[strat_n] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}
        strategy_stats[strat_n]["pnl"] += net_p
        # BUG FIX: only count wins/losses for BASE positions.
        # PYR legs are sub-units of one trade; counting them inflates stats.
        if not is_pyr_leg:
            if net_p > 0:   strategy_stats[strat_n]["wins"] += 1
            elif net_p < 0: strategy_stats[strat_n]["loss"] += 1

        # BUG FIX: only increment exit-reason counters for BASE positions.
        if exit_reason == "TARGET HIT":
            if not is_pyr_leg:
                trade_statistics["target_hits"] += 1
            display_r = "TARGET HIT"
            color_p   = Fore.BLUE
        else:
            if p["trail_level"] > 0:
                if not is_pyr_leg:
                    trade_statistics["trailing_sl_hits"] += 1
                display_r = "TSL PROFIT" if net_p > 0 else ("TSL BE" if net_p == 0 else "TSL LOSS")
                color_p   = Fore.MAGENTA
            else:
                if not is_pyr_leg:
                    trade_statistics["sl_hits"] += 1
                display_r = "SL HIT"
                color_p   = Fore.RED

        print(color_p +
            f"=========>>>>     {sym} CLOSED | {display_r} | "
            f"{side_p} | Entry:{entry_p} | Exit:{exit_price} | "
            f"LTP:{ltp_display} {_chg(ltp_display, p.get('y_close', 0))} | "
            f"Points:{points_p:.2f} | NetPnL:{net_p:.2f}\n" + ts())
        write_log(
            f"{sym} {exit_reason} {side_p} Entry:{entry_p} Exit:{exit_price} NetPnL:{net_p:.2f}"
        )
        with open(paper_trade_log_file, "a", newline="") as _f:
            csv.writer(_f).writerow(
                [datetime.now(), sym, side_p, entry_p, exit_price, qty_p, net_p, exit_reason]
            )

    def _cascade_close_pyramid_legs(base_sym, exit_price, exit_reason, ltp_display):
        """
        Close all open pyramid legs for base_sym at exit_price.
        In LIVE mode, also cancels any open SL-M / target orders at the broker.
        """
        for leg_key, leg_pos in list(paper_positions.items()):
            if (leg_pos.get("pyramid_base") == base_sym and
                    leg_pos.get("status") == "OPEN"):
                # LIVE: cancel the leg's broker orders before marking closed
                if TRADING_MODE == "LIVE":
                    _cancel_live_order_safe(
                        leg_pos.get("_live_sl_oid"),  f"cascade SL  {leg_key}")
                    _cancel_live_order_safe(
                        leg_pos.get("_live_tgt_oid"), f"cascade TGT {leg_key}")
                _close_position(leg_key, leg_pos, exit_price, exit_reason, ltp_display)
        # BUG FIX 18: always pop pyramid_state here so stale state never lingers.
        # Previously this was done in PAPER path only; LIVE relied on
        # _cascade_cancel_live_pyramid_legs which was called separately.
        pyramid_state.pop(base_sym, None)

    while True:

        for symbol in list(paper_positions.keys()):

            pos = paper_positions[symbol]

            if pos["status"] != "OPEN":
                continue

            # Pyramid legs use the base symbol for price lookup
            real_sym = pos.get("pyramid_base", symbol) if "__PYR" in symbol else symbol
            ltp = latest_prices.get(real_sym)

            if not ltp:
                continue

            entry = pos["entry"]
            side  = pos["side"]
            qty   = pos["qty"]

            points = (ltp - entry) if side == "BUY" else (entry - ltp)

            # ── Trailing SL ──────────────────────────────────────────────
            pos_tsl_step = pos.get("tsl_step", TRAIL_STEP_PERCENT)
            move_percent = abs((ltp - entry) / entry) * 100

            while move_percent >= (pos["trail_level"] + pos_tsl_step):
                pos["trail_level"] += pos_tsl_step

                if side == "BUY":
                    new_sl = round(ltp * (1 - STOP_LOSS_PERCENT / 100), 2)
                    if new_sl > pos["sl"]:
                        pos["sl"] = new_sl
                else:
                    new_sl = round(ltp * (1 + STOP_LOSS_PERCENT / 100), 2)
                    if new_sl < pos["sl"]:
                        pos["sl"] = new_sl

                net_trail = round(points * qty - BROKERAGE_PER_ORDER * 2, 2)
                tgt_label = "TSL-Only" if pos.get("tsl_only") else f"Target:{pos['target']}"
                print(Fore.YELLOW +
                    f"=============>>>>>>>>   TRAIL UPDATED | {symbol} | {side} | "
                    f"Entry:{entry} | LTP:{ltp} {_chg(ltp, pos.get('y_close', 0))} | "
                    f"New SL:{pos['sl']} | {tgt_label} | "
                    f"Points:{points:.2f} | NetPnL:{net_trail:.2f}\n" + ts())

            # ── Pyramid trigger check (base positions only) ───────────────
            # FIX 3: respect ws_ready warmup guard for pyramid in LIVE mode.
            # Without this, all accumulated pyramid triggers fire within 2s of
            # restart, placing 5-10 orders simultaneously, hammering the API,
            # and causing oco_monitors to fail finding their SL orders.
            _pyramid_ok = (TRADING_MODE == "PAPER") or ws_ready
            if "__PYR" not in symbol and _pyramid_ok:
                try:
                    check_and_add_pyramid(symbol, ltp)
                except Exception as _pyr_e:
                    write_log(f"Pyramid check error {symbol}: {_pyr_e}")

            # ── Exit conditions ───────────────────────────────────────────
            # LIVE mode: exits are handled exclusively by oco_monitor + broker fills.
            # paper_position_manager must NOT close positions or update daily_pnl
            # in LIVE mode — doing so causes double-close and daily_pnl corruption
            # (position_manager() already owns daily_pnl from broker PnL sync).
            # We only run trailing SL logic and pyramid triggering here for LIVE.
            # BUG FIX: save positions cache periodically in LIVE mode so that
            # trail_level and updated sl values survive a crash/restart.
            # We throttle this to once per symbol per trail event (trail_level > 0)
            # to avoid hammering disk on every tick.
            if TRADING_MODE == "LIVE":
                # BUG FIX 21: only save when SL actually changed this iteration,
                # not on every tick (which fires 5-10x/sec and hammers disk).
                if pos.get("sl") != pos.get("_last_saved_sl"):
                    pos["_last_saved_sl"] = pos.get("sl")
                    save_paper_positions_cache()
                continue

            tsl_only_pos = pos.get("tsl_only", False)
            if side == "BUY":
                if ltp <= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT"
                elif not tsl_only_pos and pos.get("target") and ltp >= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT"
                else:
                    continue
            else:  # SELL
                if ltp >= pos["sl"]:
                    exit_price  = pos["sl"]
                    exit_reason = "SL HIT"
                elif not tsl_only_pos and pos.get("target") and ltp <= pos["target"]:
                    exit_price  = pos["target"]
                    exit_reason = "TARGET HIT"
                else:
                    continue

            # ── Close this position via shared helper ─────────────────────
            _close_position(symbol, pos, exit_price, exit_reason, ltp)

            # ── If it's a BASE position, cascade-close all its pyramid legs ─
            if "__PYR" not in symbol:
                _cascade_close_pyramid_legs(symbol, exit_price, exit_reason, ltp)

            _max_trades_warned.clear()
            # Single save after base + all cascade legs are closed
            save_paper_positions_cache()

            # ── Daily Max Loss Circuit Breaker ────────────────────────────
            if trading_enabled:
                ref_capital = sum(
                    p2["entry"] * p2["qty"]
                    for p2 in paper_positions.values()
                    if p2.get("status") == "OPEN"
                ) or 50000
                max_loss_rupees = ref_capital * (DAILY_MAX_LOSS_PERCENT / 100)
                if daily_pnl <= -abs(max_loss_rupees):
                    trading_enabled = False
                    msg = (
                        f"🚨 PAPER DAILY MAX LOSS REACHED | PnL:{daily_pnl:.2f} | "
                        f"Limit:-{abs(max_loss_rupees):.2f} | Trading HALTED"
                    )
                    print(Fore.RED + msg + ts())
                    write_log(msg)

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
    "Open=High Breakdown",
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
        "y_open":     ctx["yest"].get("open", 0),
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
        _scan_record(_SNAME, symbol, "After 14:20 cutoff")
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

    # Single range check — 0.3–1.5% consolidation (not wide violent open)
    range_pct = ((f["high"] - f["low"]) / f["open"]) * 100
    if not (0.3 <= range_pct <= 1.5):
        _scan_record(_SNAME, symbol, f"15m range {range_pct:.2f}% outside 0.3–1.5%")
        return

    if vol_percent < 15:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 15% of yesterday")
        return

    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 14:20 cutoff")
        return

    tol = 0.001   # open must be within 0.1% of low/high

    open_is_low  = abs(f["open"] - f["low"])  / f["open"] < tol
    open_is_high = abs(f["open"] - f["high"]) / f["open"] < tol

    yest = ctx["yest"]

    today_open = f["open"]
    gap_from_yl = ((today_open - yest["low"])  / yest["low"])  * 100
    gap_from_yh = ((yest["high"] - today_open) / yest["high"]) * 100

    today_opened_below_yl = today_open < yest["low"]

    # ── Intraday crash/spike filter ───────────────────────────────────────
    day_high = latest_highs.get(symbol, ltp)
    day_low  = latest_lows.get(symbol,  ltp)
    MAX_INTRADAY_CRASH_PCT = 1.0

    day_crashed = ((today_open - day_low)  / today_open * 100) > MAX_INTRADAY_CRASH_PCT
    day_spiked  = ((day_high  - today_open) / today_open * 100) > MAX_INTRADAY_CRASH_PCT

    if day_crashed:
        _scan_record(_SNAME, symbol,
            f"BUY blocked: day low {day_low:.2f} is already "
            f"{((today_open - day_low)/today_open*100):.2f}% below open — crash+bounce")
        return

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
        not day_crashed
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
        not day_spiked
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
            reason = f"BUY: gap-down open {gap_vs_yclose:.2f}% vs YClose {yest['close']:.2f}"
        elif open_is_high and gap_vs_yclose > 0.5:
            reason = f"SELL: gap-up open {gap_vs_yclose:.2f}% vs YClose {yest['close']:.2f}"
        elif open_is_low and gap_from_yl < -1.0:
            reason = f"BUY: open {today_open:.2f} is {abs(gap_from_yl):.2f}% below YL {yest['low']:.2f}"
        elif open_is_high and today_opened_below_yl:
            reason = f"SELL: today opened {today_open:.2f} below YL {yest['low']:.2f}"
        elif open_is_high and gap_from_yh < -1.0:
            reason = f"SELL: open {today_open:.2f} is {abs(gap_from_yh):.2f}% above YH {yest['high']:.2f}"
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

    if current_time < "09:30":
        _scan_record(_SNAME, symbol, "Before 09:30 (opening volatility window)")
        return
    if current_time > "14:30":
        _scan_record(_SNAME, symbol, "After 14:30 cutoff")
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
            was_ready = second15_data[symbol]["ready"]
            second15_data[symbol]["ready"] = True
            # Save cache once when C2 window freezes — so the file has real C2 values,
            # not the 0/999999 placeholder written at the earlier 09:30 F15 freeze.
            if not was_ready:
                if all(second15_data.get(s, {}).get("ready", False) for s in SYMBOLS if s in second15_data):
                    _save_orb_cache()


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

    if vol_percent < 20:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 20% of yesterday")
        return

    if current_time > "14:00":
        _scan_record(_SNAME, symbol, "After 14:00 cutoff")
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
#            below_count: int,   # consecutive MINUTES with close below VWAP
#            above_count: int,   # consecutive MINUTES with close above VWAP
#            last_minute: str}   # last HH:MM processed — so counts advance once/min

def _update_vwap(symbol, ltp, volume):
    """Update VWAP for symbol with latest tick price and volume."""
    if symbol not in vwap_state:
        vwap_state[symbol] = {
            "cum_pv":      0.0,
            "cum_vol":     0.0,
            "vwap":        0.0,
            "below_count": 0,
            "above_count": 0,
            "last_minute": ""
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
        _scan_record(_SNAME, symbol, "After 14:30 cutoff")
        return
    if vol_percent < 20:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 20% of yesterday")
        return

    vwap = _update_vwap(symbol, ltp, live_volume)
    if vwap == 0:
        _scan_record(_SNAME, symbol, "VWAP=0 (no volume yet)")
        return

    v = vwap_state[symbol]

    # ── Advance minute-level counters only once per minute ──────────────
    # below_count / above_count represent consecutive MINUTES closing on
    # one side of VWAP — not individual ticks (which fire 5–10x/second).
    # We use the last tick of each minute (first tick of next minute triggers
    # the count advance) so the count is based on settled 1-min close price.
    if v["last_minute"] != current_time:
        v["last_minute"] = current_time
        if ltp < vwap:
            v["below_count"] += 1
            v["above_count"]  = 0
        elif ltp > vwap:
            v["above_count"] += 1
            v["below_count"]  = 0
        # ltp == vwap exactly → don't advance either counter

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
        _scan_record(_SNAME, symbol, f"✅ SIGNAL BUY  ltp={ltp} vwap={vwap:.2f} below_mins={v['below_count']}")
        place_trade(symbol, "BUY", ltp, ctx["tick"], "VWAP Reclaim BUY", _extra(ctx))
        v["below_count"] = 0
    elif sell_signal:
        _scan_record(_SNAME, symbol, f"✅ SIGNAL SELL ltp={ltp} vwap={vwap:.2f} above_mins={v['above_count']}")
        place_trade(symbol, "SELL", ltp, ctx["tick"], "VWAP Rejection SELL", _extra(ctx))
        v["above_count"] = 0
    else:
        if ltp > vwap and v["below_count"] < 3:
            reason = f"BUY: only {v['below_count']} below-mins < 3 needed"
        elif ltp < vwap and v["above_count"] < 3:
            reason = f"SELL: only {v['above_count']} above-mins < 3 needed"
        elif ltp > vwap and ema["ema7"] < ema["ema20"]:
            reason = f"BUY EMA7<EMA20 ({ema['ema7']:.1f}<{ema['ema20']:.1f})"
        elif ltp < vwap and ema["ema7"] > ema["ema20"]:
            reason = f"SELL EMA7>EMA20 ({ema['ema7']:.1f}>{ema['ema20']:.1f})"
        elif vol_percent < 35:
            reason = f"Vol {vol_percent:.0f}% < 35% for signal"
        else:
            reason = f"ltp={ltp:.1f} vwap={vwap:.1f} below_mins={v['below_count']} above_mins={v['above_count']}"
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
    if not ema20:
        _scan_record(_SNAME, symbol, "Missing EMA20")
        return

    if current_time < "09:30":
        _scan_record(_SNAME, symbol, "Before 09:30")
        return
    if current_time > "14:30":
        _scan_record(_SNAME, symbol, "After 14:30 cutoff")
        return

    if vol_percent < 20:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 20% of yesterday")
        return

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
        -3.0 <= change_pct <= -0.3 and
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
        0.3 <= change_pct <= 3.0 and
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
        elif ltp <= yest["low"] and not (-3.0 <= change_pct <= -0.3):
            reason = f"SELL: change {change_pct:.2f}% outside -3.0–-0.3%"
        elif ltp >= yest["high"] and not (0.3 <= change_pct <= 3.0):
            reason = f"BUY: change {change_pct:.2f}% outside 0.3–3.0%"
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
        _scan_record(_SNAME, symbol, "After 14:20 cutoff")
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
            was_ready = third15_data[symbol]["ready"]
            third15_data[symbol]["ready"] = True
            if not was_ready:
                if all(third15_data.get(s, {}).get("ready", False) for s in SYMBOLS if s in third15_data):
                    _save_orb_cache()

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
            was_ready = fourth15_data[symbol]["ready"]
            fourth15_data[symbol]["ready"] = True
            # At this point all 4 candles are complete — save with full accurate data.
            if not was_ready:
                if all(fourth15_data.get(s, {}).get("ready", False) for s in SYMBOLS if s in fourth15_data):
                    _save_orb_cache()


def strategy_15m_inside_break(ctx):

    _SNAME           = "15m Inside Range Break"
    symbol           = ctx["symbol"]
    ltp              = ctx["ltp"]
    ema              = ctx["ema"]
    ohlc             = ctx["ohlc"]
    current_time     = ctx["current_time"]
    vol_percent      = ctx["vol_percent"]
    dist_from_yh_pct = ctx["dist_from_yh_pct"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]

    _scan_start(_SNAME, current_time)

    # Build all inner candles on every tick — S8 owns its own build calls
    # so it never depends on S4 (strategy_inside_bar) having run first.
    _build_second15(symbol, ohlc, ltp, current_time)
    _build_third15(symbol,  ltp, current_time)
    _build_fourth15(symbol, ltp, current_time)

    # Time gate: all 4 candles must be fully formed
    if current_time < "10:15":
        _scan_record(_SNAME, symbol, "Waiting — 4 candles not yet complete")
        return
    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 14:20 cutoff")
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
# STRATEGY 9 — OPEN = HIGH  5-MIN BREAKDOWN (SELL)
# ==========================================
# Setup:
#   • Today's market open == day high (within 0.1% tolerance)
#     → stock opened at its highest point = immediate seller rejection
#   • First 5-min candle (09:15–09:19) range < 1% of open price
#     → tight consolidation at open, not a wide volatile spike
#   • First 5-min candle range is bearish context (no big bounce)
#   • LTP breaks below first-5-min candle low → SELL entry
#   • SL = first-5-min candle high + 1 tick  (the open/high level)
#   • Previous day must be a GREEN candle (close > open)
#     → breakdown of a bullish day's high = stronger reversal signal
#
# Example (COLPAL chart):
#   Day opened at 2198.3 = day high → open==high confirmed
#   First 5-min candle tight → then breakdown through 2176
#   → Strong SELL signal with SL just above 2198
# ==========================================

first5_data = {}   # symbol → {open, high, low, close, ready}

def _build_first5(symbol, ltp, current_time):
    """Build the 09:15–09:19 candle (first 5-min candle) using ltp."""
    if symbol not in first5_data:
        if "09:15" <= current_time < "09:20":
            first5_data[symbol] = {
                "open": ltp, "high": ltp, "low": ltp, "close": ltp, "ready": False
            }
        else:
            first5_data[symbol] = {
                "open": 0, "high": 0, "low": 999999, "close": 0, "ready": False
            }

    if "09:15" <= current_time < "09:20":
        c = first5_data[symbol]
        c["high"]  = max(c["high"], ltp)
        c["low"]   = min(c["low"],  ltp)
        c["close"] = ltp

    if current_time >= "09:20":
        c = first5_data[symbol]
        if c["high"] != 0 and c["low"] != 999999:
            was_ready = c.get("ready", False)
            c["ready"] = True
            # ── Print F5 candle summary ONCE when window freezes at 09:20 ──
            if not was_ready:
                # Suppressed per-symbol console print — too noisy (195 lines at 09:20).
                # Still written to log file for diagnostics.
                write_log(
                    f"F5 FROZEN {symbol} "
                    f"O:{c['open']:.2f} H:{c['high']:.2f} "
                    f"L:{c['low']:.2f} C:{c['close']:.2f} "
                    f"Range:{((c['high']-c['low'])/c['open']*100):.2f}%"
                )
                # Persist to ORB cache so F5 survives a restart
                _save_orb_cache(silent=True)


def strategy_open_high_breakdown(ctx):

    _SNAME       = "Open=High Breakdown"
    symbol       = ctx["symbol"]
    ltp          = ctx["ltp"]
    vol_percent  = ctx["vol_percent"]
    current_time = ctx["current_time"]
    yest         = ctx["yest"]
    dist_from_yl_pct = ctx["dist_from_yl_pct"]

    _scan_start(_SNAME, current_time)

    # Already traded this symbol today — stop all scanning immediately
    if symbol in trades_taken:
        return

    # Build first-5 candle on every tick
    _build_first5(symbol, ltp, current_time)

    # ── Time gate: only after first-5 candle is complete ───────────────
    if current_time < "09:20":
        _scan_record(_SNAME, symbol, "Before 09:20 (first-5 building)")
        return
    if current_time > "14:20":
        _scan_record(_SNAME, symbol, "After 14:20 cutoff")
        return

    # ── First-5 candle must be ready ─────────────────────────────────────
    if not first5_data.get(symbol, {}).get("ready"):
        _scan_record(_SNAME, symbol, "First-5 not ready")
        return

    f5 = first5_data[symbol]

    if f5["open"] == 0 or f5["high"] == 0 or f5["low"] == 999999:
        _scan_record(_SNAME, symbol, "First-5 data invalid")
        return

    # ── Volume filter ────────────────────────────────────────────────────
    if vol_percent < 15:
        _scan_record(_SNAME, symbol, f"Vol {vol_percent:.1f}% < 15% of yesterday")
        return

    # ── Condition A: Open == High (within 1 tick tolerance) ─────────────
    # F5 open must be the day high — meaning sellers rejected every attempt
    # to go above the open price. Allow at most 1 tick above open (spread
    # noise), NOT a percentage band — 0.1% was too loose and let through
    # stocks where price genuinely rallied above open (e.g. HDFCLIFE
    # open=635.00, high=635.50, 0.079% diff → wrongly passed).
    day_open = f5["open"]
    day_high = latest_highs.get(symbol, f5["high"])
    tick      = tick_sizes.get(symbol, 0.05)

    # day_high must not exceed day_open by more than 1 tick
    if day_high > day_open + tick:
        _scan_record(_SNAME, symbol,
            f"Open≠High: open={day_open:.2f} high={day_high:.2f} "
            f"diff={day_high-day_open:.2f} > 1tick({tick})")
        return

    # ── Condition B: First-5 candle range < 1% ──────────────────────────
    f5_range_pct = ((f5["high"] - f5["low"]) / f5["open"]) * 100
    if f5_range_pct >= 1.8:
        _scan_record(_SNAME, symbol,
            f"First-5 range {f5_range_pct:.2f}% >= 1% (too wide)")
        return

    # ── Condition C: removed — yest green/red both valid for O=H SELL ───
    yest_open  = yest.get("open", 0)
    yest_close = yest.get("close", 0)

    # ── Entry: LTP breaks below first-5 candle low ──────────────────────
    entry_level = f5["low"]
    # SL = 0.3% above day high. Flat +1 point is meaningless for high-price stocks.
    sl_level    = round(day_high * 1.003, 2)   # 0.3% above open

    # Max slippage: don't chase more than 0.3% below the breakout level
    MAX_SLIPPAGE_PCT = 0.3
    slippage_pct = ((entry_level - ltp) / entry_level * 100) if ltp < entry_level else 0

    # Stale guard: if price EVER broke below F5 low before this signal,
    # it is a re-test — the fresh breakdown already happened. Skip it.
    # MAX_STALE_PCT = 0.0 means zero tolerance: the first break must be
    # the signal tick itself (or within 0.3% slippage from it).
    # Previously 0.5% — that allowed HDFCLIFE at 09:49 (price had already
    # been 0.46% below F5 low and bounced back before the signal fired).
    stale_pct = ((f5["low"] - latest_lows.get(symbol, ltp)) / f5["low"] * 100)
    MAX_STALE_PCT = 0.0

    sell_signal = (
        299 <= ltp <= 6999              and
        ltp < entry_level               and   # broke below first-5 low
        slippage_pct <= MAX_SLIPPAGE_PCT and   # not chasing
        stale_pct <= MAX_STALE_PCT      and   # fresh breakdown
        dist_from_yl_pct <= MAX_OVEREXTENSION_PCT
    )

    if sell_signal:
        _scan_record(_SNAME, symbol,
            f"✅ SIGNAL SELL ltp={ltp} f5_low={entry_level:.2f} "
            f"sl={sl_level:.2f} range={f5_range_pct:.2f}% TSL-only")
        # Only print trigger once — skip if already traded this symbol today
        if symbol not in trades_taken:
            print(Fore.RED +
                f"🔴 S9 SELL TRIGGER | {symbol} | "
                f"LTP:{ltp:.2f} broke F5-Low:{entry_level:.2f} | "
                f"F5 O:{f5['open']:.2f} H:{f5['high']:.2f} L:{f5['low']:.2f} | "
                f"Open==High:{day_open:.2f} | SL:{sl_level:.2f} | TSL-step:1.5%" + ts())
        place_trade(symbol, "SELL", ltp, ctx["tick"], "Open=High Breakdown SELL",
                    _extra(ctx), custom_sl=sl_level, tsl_only=True, tsl_step=1.5)
    else:
        if symbol in trades_taken:
            return   # already traded — stop scanning and printing skip reasons
        if ltp >= entry_level:
            reason = f"ltp {ltp:.2f} not below first-5 low {entry_level:.2f}"
        elif slippage_pct > MAX_SLIPPAGE_PCT:
            reason = f"Slippage {slippage_pct:.2f}% > {MAX_SLIPPAGE_PCT}% — chasing"
        elif stale_pct > MAX_STALE_PCT:
            reason = f"Stale: day low already {stale_pct:.2f}% below f5_low — re-test"
        elif dist_from_yl_pct > MAX_OVEREXTENSION_PCT:
            reason = f"Overextended {dist_from_yl_pct:.2f}% below YL"
        else:
            reason = f"No breakdown: ltp={ltp:.2f} f5={f5['low']:.2f}–{f5['high']:.2f}"
        _scan_record(_SNAME, symbol, reason)


# ==========================================
# STRATEGY DISPATCHER
# ==========================================
# Active strategies:
#   ✅ S1  ORB Breakout          (60-min ORB, fires after 10:15)
#   ✅ S8  15m Inside Range Break (C1/C2/C3/C4 compression, fires after 10:15)
#   ✅ S9  Open=High 5-min Breakdown (SELL only, fires from 09:20 — early ctx bypass active)
#
# Disabled (commented — re-enable individually once validated):
#   ❌ S2  Open=Low / Open=High Break
#   ❌ S3  EMA Pullback
#   ❌ S4  Inside Bar Breakout
#   ❌ S5  VWAP Reclaim
#   ❌ S6  YL Breakdown / YH Breakout
#   ❌ S7  Gap + First15 Breakout
# ==========================================
def strategy(token, tick):

    ctx = build_context(token, tick)

    # ==========================================
    # S9 EARLY WINDOW: 09:20 – 09:30
    # ==========================================
    # build_context() blocks before ENTRY_START_TIME (09:20), so S9 would miss
    # the 09:20–09:30 window without this bypass. We build a minimal ctx directly
    # from tick data for S9 only. All other strategies require full build_context().
    if ctx is None:
        symbol = instrument_tokens.get(token)
        if symbol and symbol in yesterday_data:
            candle_time = tick.get("date", datetime.now())
            current_time = candle_time.strftime("%H:%M")
            if "09:20" <= current_time < "09:31":
                ltp = tick["last_price"]
                yest = yesterday_data[symbol]
                live_volume = tick.get("volume_traded", tick.get("volume", 0))
                if symbol in replay_cum_volume:
                    live_volume = replay_cum_volume[symbol]
                vol_pct = round((live_volume / yest["volume"]) * 100, 2) if yest.get("volume", 0) > 0 else 0
                s9_ctx = {
                    "symbol":           symbol,
                    "tick":             tick,
                    "ltp":              ltp,
                    "current_time":     current_time,
                    "ohlc":             tick.get("ohlc", {}),
                    "yest":             yest,
                    "ema":              ema_cache.get(symbol, {}),
                    "change_percent":   ((ltp - yest["close"]) / yest["close"]) * 100 if yest.get("close") else 0,
                    "gap_percent":      0,
                    "live_volume":      live_volume,
                    "vol_percent":      vol_pct,
                    "dist_from_yl_pct": ((yest["low"] - ltp) / yest["low"]) * 100 if yest.get("low") else 0,
                    "dist_from_yh_pct": ((ltp - yest["high"]) / yest["high"]) * 100 if yest.get("high") else 0,
                    "c15": {},
                    "c1h": {},
                }
                strategy_open_high_breakdown(s9_ctx)
        return

    # ✅ Strategy 1: ORB Breakout (60-min ORB 09:15–10:14, fires after 10:15)
    strategy_orb(ctx)

    # ❌ Strategy 2: Open=Low / Open=High first-15m break
    # strategy_open_low(ctx)

    # ❌ Strategy 3: EMA Pullback in trend
    # strategy_ema_pullback(ctx)

    # ❌ Strategy 4: Inside Bar Breakout (C1 vs C2 compression)
    # strategy_inside_bar(ctx)

    # ❌ Strategy 5: VWAP Reclaim / Rejection
    # strategy_vwap_reclaim(ctx)

    # ❌ Strategy 6: Yesterday Level Breakdown / Breakout
    # strategy_yl_breakdown(ctx)

    # ❌ Strategy 7: Gap Up/Down + First 15 Min Candle Breakout
    # strategy_gap_first15(ctx)

    # ✅ Strategy 8: 15-Min Inside Range Breakout (C1/C2/C3/C4 → 45-min compression)
    strategy_15m_inside_break(ctx)

    # ✅ Strategy 9: Open=High 5-min Breakdown (SELL — yest green, open==high, break f5 low)
    #    Early window 09:20–09:30 handled above via s9_ctx bypass.
    strategy_open_high_breakdown(ctx)



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
            # Include BOTH open (qty!=0) and closed (qty==0) positions — Kite's
            # p["pnl"] reflects realized PnL even after a position is fully closed.
            # Excluding qty==0 rows was causing closed trade PnL to disappear from
            # the daily_pnl counter after each exit.
            daily_pnl = sum(
                p["pnl"] for p in positions
                if p.get("exchange") == "NSE"
                and p["tradingsymbol"] in algo_symbols
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
    """
    Monitors for SQUARE_OFF_TIME.  When reached:

    LIVE
    ────
    1. Cancel ALL open SL-M / target orders for every algo position
       (base orders from trades_taken + pyramid leg orders from paper_positions).
    2. Cancel any PENDING base entry orders (entry placed but SL failed).
    3. Fetch live net positions from broker and market-exit every open qty.
    4. Each symbol in its own try/except — one failure never blocks others.
    5. Retry the entire sequence once after 5 s if any symbol still has qty.

    PAPER
    ─────
    1. Close all open positions (base + pyramid legs) at latest LTP.
    2. Clear pyramid_state so the position manager stops adding legs.
    """
    global daily_pnl
    time.sleep(60)   # wait for WebSocket + positions to load

    while True:

        now_hhmm = datetime.now().strftime("%H:%M")
        if not ("09:15" <= now_hhmm <= "15:30"):
            time.sleep(30)
            continue

        if now_hhmm < SQUARE_OFF_TIME:
            time.sleep(20)
            continue

        # ── Count open positions ──────────────────────────────────────────
        open_count = sum(1 for p in paper_positions.values() if p.get("status") == "OPEN")
        if TRADING_MODE == "PAPER" and open_count == 0:
            print(Fore.YELLOW + "⏭️  Square off: no open positions — skipping" + ts())
            daily_summary()
            break

        print(Fore.MAGENTA + "\n🔔 AUTO SQUARE OFF TRIGGERED" + ts())

        # Stop pyramid engine immediately — no new legs during squareoff sequence
        pyramid_state.clear()
        write_log("SQUAREOFF: pyramid_state cleared — no new legs will fire")

        # =================================================================
        # LIVE MODE SQUARE OFF
        # =================================================================
        if TRADING_MODE == "LIVE":

            def _sq_cancel_all_orders():
                """
                Step 1: Cancel every open SL-M / target / pending entry order
                this algo placed — base orders AND pyramid leg orders.
                Returns count of cancellations attempted.
                """
                cancelled = 0
                try:
                    open_orders  = kite.orders()
                    orders_by_id = {str(o["order_id"]): o for o in open_orders}

                    # ── Cancel base-position SL and target orders ─────────
                    for symbol, trade in list(trades_taken.items()):
                        if not isinstance(trade, dict):
                            # PENDING: the entry order itself may still be open
                            if trade == "PENDING":
                                # We don't have the entry OID here — skip
                                # (broker will auto-expire MIS at 15:20)
                                pass
                            continue
                        for oid_key in ("sl", "target"):
                            oid = trade.get(oid_key)
                            if not oid:
                                continue
                            order = orders_by_id.get(str(oid))
                            if order and order["status"] in ("OPEN", "TRIGGER PENDING"):
                                try:
                                    kite.cancel_order(
                                        variety=kite.VARIETY_REGULAR,
                                        order_id=str(oid)
                                    )
                                    print(Fore.YELLOW +
                                          f"  Cancelled base {oid_key} | {symbol}" + ts())
                                    cancelled += 1
                                    time.sleep(0.05)
                                except Exception as ce:
                                    print(Fore.RED +
                                          f"  Could not cancel base {oid_key} {symbol}: {ce}" + ts())
                                    write_log(f"SQ CANCEL FAIL base {oid_key} {symbol}: {ce}")

                    # ── Cancel pyramid leg SL and target orders (known IDs) ──
                    for leg_key, leg_pos in list(paper_positions.items()):
                        if "__PYR" not in leg_key:
                            continue
                        if leg_pos.get("status") != "OPEN":
                            continue
                        for oid_field in ("_live_sl_oid", "_live_tgt_oid"):
                            oid = leg_pos.get(oid_field)
                            if not oid:
                                continue
                            order = orders_by_id.get(str(oid))
                            if order and order["status"] in ("OPEN", "TRIGGER PENDING"):
                                try:
                                    kite.cancel_order(
                                        variety=kite.VARIETY_REGULAR,
                                        order_id=str(oid)
                                    )
                                    print(Fore.YELLOW +
                                          f"  Cancelled pyramid {oid_field} | {leg_key}" + ts())
                                    cancelled += 1
                                    time.sleep(0.05)
                                except Exception as ce:
                                    print(Fore.RED +
                                          f"  Could not cancel {oid_field} {leg_key}: {ce}" + ts())
                                    write_log(f"SQ CANCEL FAIL {oid_field} {leg_key}: {ce}")

                    # ── BROAD SWEEP: cancel ALL remaining open MIS orders for
                    # every algo symbol — catches dangling orders from crashed
                    # sessions whose IDs were never saved in paper_positions.
                    # This is the safety net that ensures a clean slate at squareoff.
                    algo_syms = set(trades_taken.keys())
                    already_cancelled = set()
                    for o in open_orders:
                        sym = o.get("tradingsymbol", "")
                        # Extract base symbol from PYR keys (e.g. "JUBLFOOD__PYR1" → "JUBLFOOD")
                        base_sym = sym.split("__PYR")[0] if "__PYR" in sym else sym
                        if base_sym not in algo_syms:
                            continue
                        if o.get("product") != "MIS":
                            continue
                        if o.get("status") not in ("OPEN", "TRIGGER PENDING"):
                            continue
                        oid_str = str(o["order_id"])
                        if oid_str in already_cancelled:
                            continue
                        already_cancelled.add(oid_str)
                        try:
                            kite.cancel_order(
                                variety=kite.VARIETY_REGULAR,
                                order_id=oid_str
                            )
                            print(Fore.YELLOW +
                                  f"  Cancelled dangling MIS order | {sym} | OID:{oid_str}" + ts())
                            write_log(f"SQ BROAD CANCEL {sym} OID:{oid_str}")
                            cancelled += 1
                            time.sleep(0.05)
                        except Exception as ce:
                            # Already cancelled by the loop above — ignore
                            pass

                except Exception as e:
                    print(Fore.RED + f"  Order fetch for cancellation failed: {e}" + ts())
                    write_log(f"SQ ORDER FETCH FAIL: {e}")

                return cancelled

            def _sq_exit_live_positions():
                """
                Step 2: Market-exit every open NSE MIS position this algo opened.
                Each symbol is wrapped in its own try/except.
                Returns list of symbols that failed.
                """
                failed = []
                try:
                    positions    = kite.positions()["net"]
                    algo_symbols = set(trades_taken.keys())

                    for pos in positions:
                        if pos.get("exchange") != "NSE":
                            continue
                        if pos["tradingsymbol"] not in algo_symbols:
                            continue
                        qty = pos["quantity"]
                        if qty == 0:
                            continue

                        sq_side = (
                            kite.TRANSACTION_TYPE_SELL if qty > 0
                            else kite.TRANSACTION_TYPE_BUY
                        )
                        sym = pos["tradingsymbol"]
                        try:
                            kite.place_order(
                                variety=kite.VARIETY_REGULAR,
                                exchange="NSE",
                                tradingsymbol=sym,
                                transaction_type=sq_side,
                                quantity=abs(qty),
                                order_type=kite.ORDER_TYPE_MARKET,
                                product=kite.PRODUCT_MIS
                            )
                            time.sleep(0.3)
                            msg = (f"✅ LIVE SQ OFF: {sym} qty={abs(qty)} "
                                   f"({'SELL' if qty > 0 else 'BUY'})")
                            print(Fore.YELLOW + msg + ts())
                            write_log(msg)
                            # Mark base position closed
                            sq_ltp = latest_prices.get(sym, 0)
                            if sym in paper_positions:
                                base_p = paper_positions[sym]
                                base_p["status"] = "CLOSED"
                                if sq_ltp:
                                    pts = (sq_ltp - base_p["entry"]) if base_p["side"] == "BUY" else (base_p["entry"] - sq_ltp)
                                    base_p["net_pnl"] = round(pts * base_p["qty"] - BROKERAGE_PER_ORDER * 2, 2)
                            # BUG FIX: cascade-close all open pyramid legs in paper_positions
                            # Without this, PYR legs stay OPEN in the cache after squareoff,
                            # causing ghost positions to appear on the next day's restart.
                            for leg_key, leg_pos in list(paper_positions.items()):
                                if (leg_pos.get("pyramid_base") == sym and
                                        leg_pos.get("status") == "OPEN"):
                                    leg_pos["status"] = "CLOSED"
                                    if sq_ltp:
                                        l_pts = (sq_ltp - leg_pos["entry"]) if leg_pos["side"] == "BUY" else (leg_pos["entry"] - sq_ltp)
                                        leg_pos["net_pnl"] = round(l_pts * leg_pos["qty"] - BROKERAGE_PER_ORDER * 2, 2)
                                    print(Fore.YELLOW + f"  ✅ LIVE SQ OFF (PYR leg): {leg_key}" + ts())
                                    write_log(f"LIVE SQ OFF PYR LEG {leg_key}")
                            pyramid_state.pop(sym, None)
                        except Exception as se:
                            failed.append(sym)
                            print(Fore.RED +
                                  f"  ❌ SQ exit FAILED {sym} qty={abs(qty)}: {se}" + ts())
                            write_log(f"SQ EXIT FAIL {sym}: {se}")

                except Exception as e:
                    print(Fore.RED + f"  Position fetch failed: {e}" + ts())
                    write_log(f"SQ POSITION FETCH FAIL: {e}")

                return failed

            # ── Attempt 1 ────────────────────────────────────────────────
            cancelled = _sq_cancel_all_orders()
            wait_secs = min(4, 1.0 + cancelled * 0.15)
            print(Fore.CYAN +
                  f"  {cancelled} orders cancelled — waiting {wait_secs:.1f}s before exit" + ts())
            time.sleep(wait_secs)

            failed_syms = _sq_exit_live_positions()

            # ── Retry once for any failed symbols ─────────────────────────
            if failed_syms:
                print(Fore.YELLOW +
                      f"⚠️  {len(failed_syms)} symbol(s) failed first exit attempt — "
                      f"retrying in 5s: {failed_syms}" + ts())
                write_log(f"SQ RETRY NEEDED: {failed_syms}")
                time.sleep(5)

                # Re-cancel orders that may have appeared in the gap
                _sq_cancel_all_orders()
                time.sleep(1)
                failed2 = _sq_exit_live_positions()
                if failed2:
                    print(Fore.RED +
                          f"🚨 SQUARE OFF STILL FAILED after retry: {failed2}\n"
                          f"   These positions may need MANUAL EXIT in Kite app!" + ts())
                    write_log(f"SQ RETRY FAIL (MANUAL ACTION NEEDED): {failed2}")
                else:
                    print(Fore.GREEN + "✅ Retry successful — all positions exited" + ts())

            # BUG FIX: persist positions cache after LIVE squareoff so that
            # on next day's restart no ghost OPEN positions are loaded from CSV.
            save_paper_positions_cache()
            write_log("LIVE SQUAREOFF COMPLETE — positions cache saved")

        # =================================================================
        # PAPER MODE SQUARE OFF
        # =================================================================
        elif TRADING_MODE == "PAPER":

            # Clear pyramid_state first so manager stops adding legs
            pyramid_state.clear()

            for sym_key in list(paper_positions.keys()):

                pos = paper_positions[sym_key]

                if pos["status"] != "OPEN":
                    continue

                # Pyramid legs use base symbol for LTP
                real_sym = pos.get("pyramid_base", sym_key) if "__PYR" in sym_key else sym_key
                ltp = latest_prices.get(real_sym)

                if not ltp:
                    continue

                entry  = pos["entry"]
                side   = pos["side"]
                qty    = pos["qty"]
                points = (ltp - entry) if side == "BUY" else (entry - ltp)
                net_pnl = points * qty - (BROKERAGE_PER_ORDER * 2)

                pos["status"]  = "CLOSED"
                pos["net_pnl"] = net_pnl
                daily_pnl     += net_pnl

                # BUG FIX: only count BASE positions in trade_statistics.
                # PYR legs are sub-units of a trade — counting them inflates
                # total_trades and corrupts win rate in the daily summary.
                if "__PYR" not in sym_key:
                    trade_statistics["total_trades"] += 1
                    if side == "BUY": trade_statistics["buy_trades"]  += 1
                    else:             trade_statistics["sell_trades"] += 1

                # Strip [PYRn] suffix so PYR leg PnL rolls up under parent strategy
                strat_sq = pos.get("strategy", "Unknown")
                if "__PYR" in sym_key:
                    strat_sq = strat_sq.split(" [PYR")[0]
                if strat_sq not in strategy_stats:
                    strategy_stats[strat_sq] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}
                strategy_stats[strat_sq]["pnl"] += net_pnl
                if "__PYR" not in sym_key:
                    strategy_stats[strat_sq]["trades"] += 1
                    if net_pnl > 0: strategy_stats[strat_sq]["wins"] += 1
                    else:           strategy_stats[strat_sq]["loss"] += 1

                print(Fore.MAGENTA +
                    f"PAPER SQ OFF | {sym_key} | {side} | "
                    f"Entry:{entry} | Exit:{ltp} | "
                    f"LTP:{ltp} {_chg(ltp, pos.get('y_close', 0))} | "
                    f"Points:{points:.2f} | NetPnL:{net_pnl:.2f}" + ts())
                write_log(
                    f"PAPER SQ OFF {sym_key} {side} Entry:{entry} Exit:{ltp} NetPnL:{net_pnl:.2f}"
                )

            # Single cache save after all positions are closed
            save_paper_positions_cache()

        daily_summary()
        break




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

            # Pyramid legs use base symbol for LTP
            real_sym = pos.get("pyramid_base", symbol) if "__PYR" in symbol else symbol
            ltp = latest_prices.get(real_sym)

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
            # BUG FIX 24: only count BASE positions in open_count so the
            # 15-min summary shows "2 open" not "6 open" with pyramid legs.
            if "__PYR" not in symbol:
                open_count += 1

            color = Fore.GREEN if net_pnl >= 0 else Fore.RED
            tgt_label2 = "TSL-Only" if pos.get("tsl_only") else f"Target:{pos['target']}"
            print(color +
                f"{symbol} | {strat} | {side} | Entry:{entry} | "
                f"LTP:{ltp} | SL:{pos['sl']} | "
                f"{tgt_label2} | "
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
    pyramid_state.clear()   # ← reset pyramiding state for each replay day

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

            def _replay_close_one(sym_key, p, exit_p, reason, display_ltp):
                """Close one paper position in replay, update PnL + stats."""
                global daily_pnl
                e_p   = p["entry"]
                s_p   = p["side"]
                q_p   = p["qty"]
                pts_p = (exit_p - e_p) if s_p == "BUY" else (e_p - exit_p)
                net_p = pts_p * q_p - BROKERAGE_PER_ORDER * 2
                p["status"]  = "CLOSED"
                p["net_pnl"] = net_p
                daily_pnl   += net_p
                is_pyr = "__PYR" in str(sym_key)
                # BUG FIX: strip [PYRn] suffix — PYR leg PnL rolls up under
                # parent strategy, not phantom "ORB Break [PYR1]" entries.
                strat_n = p.get("strategy", "Unknown")
                if is_pyr:
                    strat_n = strat_n.split(" [PYR")[0]
                if strat_n not in strategy_stats:
                    strategy_stats[strat_n] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}
                strategy_stats[strat_n]["pnl"] += net_p
                # BUG FIX: only count wins/losses and exit counters for BASE positions.
                if not is_pyr:
                    if net_p > 0:   strategy_stats[strat_n]["wins"] += 1
                    elif net_p < 0: strategy_stats[strat_n]["loss"] += 1
                if reason == "TARGET HIT":
                    if not is_pyr:
                        trade_statistics["target_hits"] += 1
                    clr = Fore.BLUE
                else:
                    if p["trail_level"] > 0:
                        if not is_pyr:
                            trade_statistics["trailing_sl_hits"] += 1
                        clr = Fore.MAGENTA
                    else:
                        if not is_pyr:
                            trade_statistics["sl_hits"] += 1
                        clr = Fore.RED
                print(clr +
                      f"{candle_time_str} | {sym_key} CLOSED | {reason} | "
                      f"{s_p} | Entry:{e_p} | Exit:{exit_p} | "
                      f"LTP:{display_ltp} {_chg(display_ltp, p.get('y_close', 0))} | "
                      f"Points:{pts_p:.2f} | NetPnL:{net_p:.2f}" + ts())

            def _replay_exit_check(price_for_trail):
                """
                Check SL/target for the BASE position and all open pyramid legs.
                Trails all open positions. Adds pyramid legs via while-loop (handles gap jumps).
                Returns True if the BASE position was closed (caller skips new entry signal).
                """
                global daily_pnl
                if symbol not in paper_positions:
                    return False
                pos = paper_positions[symbol]
                if pos["status"] != "OPEN":
                    return False

                side_b = pos["side"]

                # ── Check BASE exit ──────────────────────────────────────
                base_exit_price  = None
                base_exit_reason = None
                if side_b == "BUY":
                    if low <= pos["sl"]:
                        base_exit_price  = pos["sl"];  base_exit_reason = "SL HIT"
                    elif not pos.get("tsl_only") and pos.get("target") and high >= pos["target"]:
                        base_exit_price  = pos["target"]; base_exit_reason = "TARGET HIT"
                else:
                    if high >= pos["sl"]:
                        base_exit_price  = pos["sl"];  base_exit_reason = "SL HIT"
                    elif not pos.get("tsl_only") and pos.get("target") and low <= pos["target"]:
                        base_exit_price  = pos["target"]; base_exit_reason = "TARGET HIT"

                if base_exit_price is not None:
                    _replay_close_one(symbol, pos, base_exit_price, base_exit_reason, base_exit_price)
                    for leg_key, leg_pos in list(paper_positions.items()):
                        if (leg_pos.get("pyramid_base") == symbol and
                                leg_pos.get("status") == "OPEN"):
                            _replay_close_one(leg_key, leg_pos,
                                              base_exit_price, base_exit_reason, base_exit_price)
                    pyramid_state.pop(symbol, None)
                    return True   # BASE closed → caller skips new entry

                # ── Check each pyramid leg's own SL / target ─────────────
                for leg_key, leg_pos in list(paper_positions.items()):
                    if (leg_pos.get("pyramid_base") != symbol or
                            leg_pos.get("status") != "OPEN"):
                        continue
                    leg_side = leg_pos["side"]
                    leg_exit = None;  leg_rsn = None
                    if leg_side == "BUY":
                        if low <= leg_pos["sl"]:
                            leg_exit = leg_pos["sl"];  leg_rsn = "SL HIT"
                        elif not leg_pos.get("tsl_only") and leg_pos.get("target") and high >= leg_pos["target"]:
                            leg_exit = leg_pos["target"]; leg_rsn = "TARGET HIT"
                    else:
                        if high >= leg_pos["sl"]:
                            leg_exit = leg_pos["sl"];  leg_rsn = "SL HIT"
                        elif not leg_pos.get("tsl_only") and leg_pos.get("target") and low <= leg_pos["target"]:
                            leg_exit = leg_pos["target"]; leg_rsn = "TARGET HIT"
                    if leg_exit is not None:
                        _replay_close_one(leg_key, leg_pos, leg_exit, leg_rsn, leg_exit)

                # ── Trail BASE position ──────────────────────────────────
                p_tsl_step = pos.get("tsl_step", TRAIL_STEP_PERCENT)
                move_base  = abs((price_for_trail - pos["entry"]) / pos["entry"]) * 100
                while move_base >= (pos["trail_level"] + p_tsl_step):
                    pos["trail_level"] += p_tsl_step
                    if side_b == "BUY":
                        new_sl = round(price_for_trail * (1 - STOP_LOSS_PERCENT / 100), 2)
                        if new_sl > pos["sl"]: pos["sl"] = new_sl
                    else:
                        new_sl = round(price_for_trail * (1 + STOP_LOSS_PERCENT / 100), 2)
                        if new_sl < pos["sl"]: pos["sl"] = new_sl
                    print(Fore.YELLOW +
                          f"{candle_time_str} | TRAIL UPDATED | {symbol} | "
                          f"Entry:{pos['entry']} | LTP:{price_for_trail} "
                          f"{_chg(price_for_trail, pos.get('y_close', 0))} | "
                          f"New SL:{pos['sl']}" + ts())

                # ── Trail each open pyramid leg ──────────────────────────
                for leg_key, leg_pos in list(paper_positions.items()):
                    if (leg_pos.get("pyramid_base") != symbol or
                            leg_pos.get("status") != "OPEN"):
                        continue
                    leg_tsl  = leg_pos.get("tsl_step", TRAIL_STEP_PERCENT)
                    leg_side = leg_pos["side"]
                    move_leg = abs((price_for_trail - leg_pos["entry"]) / leg_pos["entry"]) * 100
                    while move_leg >= (leg_pos["trail_level"] + leg_tsl):
                        leg_pos["trail_level"] += leg_tsl
                        if leg_side == "BUY":
                            new_sl = round(price_for_trail * (1 - STOP_LOSS_PERCENT / 100), 2)
                            if new_sl > leg_pos["sl"]: leg_pos["sl"] = new_sl
                        else:
                            new_sl = round(price_for_trail * (1 + STOP_LOSS_PERCENT / 100), 2)
                            if new_sl < leg_pos["sl"]: leg_pos["sl"] = new_sl
                        print(Fore.YELLOW +
                              f"{candle_time_str} | TRAIL UPDATED | {leg_key} | "
                              f"Entry:{leg_pos['entry']} | LTP:{price_for_trail} "
                              f"{_chg(price_for_trail, leg_pos.get('y_close', 0))} | "
                              f"New SL:{leg_pos['sl']}" + ts())

                # ── Pyramid trigger check (while-loop handles gap jumps) ──
                try:
                    check_and_add_pyramid(symbol, price_for_trail)
                except Exception:
                    pass

                return False   # BASE still open

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

    for sym_key in list(paper_positions.keys()):

        pos = paper_positions[sym_key]
        if pos["status"] != "OPEN":
            continue

        # For pyramid legs, price comes from the base symbol
        real_sym = pos.get("pyramid_base", sym_key) if "__PYR" in sym_key else sym_key
        ltp = latest_prices.get(real_sym)
        if not ltp:
            continue

        entry = pos["entry"]
        side = pos["side"]
        qty = pos["qty"]

        points = (ltp - entry) if side == "BUY" else (entry - ltp)
        gross = points * qty
        net = gross - (BROKERAGE_PER_ORDER * 2)

        is_pyr_eod = "__PYR" in str(sym_key)
        # BUG FIX: strip [PYRn] suffix — roll PYR PnL up to parent strategy.
        strategy_name = pos.get("strategy", "Unknown")
        if is_pyr_eod:
            strategy_name = strategy_name.split(" [PYR")[0]
        if strategy_name not in strategy_stats:
            strategy_stats[strategy_name] = {"trades": 0, "wins": 0, "loss": 0, "pnl": 0}

        strategy_stats[strategy_name]["pnl"] += net
        # BUG FIX: only count wins/losses for BASE positions in EOD close.
        if not is_pyr_eod:
            if net > 0:
                strategy_stats[strategy_name]["wins"] += 1
            else:
                strategy_stats[strategy_name]["loss"] += 1

        daily_pnl += net
        pos["status"] = "CLOSED"

        print(Fore.MAGENTA +
              f"EOD | {sym_key} | {side} | Entry:{entry} | Exit:{ltp} | "
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
        # Guard: ignore pre-market ticks (before 09:15) — pre-open prices
        # can be far from market price and would corrupt stale/overextension checks.
        current_tick_time = tick_time.strftime("%H:%M")
        if current_tick_time >= "09:15":
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

        # ── Prune candles older than 2 minutes to prevent memory growth ──
        cutoff = tick_time - timedelta(minutes=2)
        stale  = [t for t in minute_candles[symbol] if t < cutoff]
        for t in stale:
            del minute_candles[symbol][t]

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

            # ── S9 first-5 candle: must build during 09:15-09:19 even during warmup ──
            # build_context() returns None before ENTRY_START_TIME (09:20), so
            # _build_first5 inside strategy_open_high_breakdown never runs during the
            # critical 09:15-09:19 window. We call it here directly, before the gate.
            current_time_str = tick_time.strftime("%H:%M")
            if "09:15" <= current_time_str < "09:20":
                _build_first5(symbol, ltp, current_time_str)

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


# ==========================================
# TRADING DASHBOARD  (built-in HTTP server)
# ==========================================
# Opens automatically on http://localhost:8765
# Open trading_dashboard.html in your browser and point it at
# http://<this-machine-ip>:8765  (open port 8765 in AWS security group).
# Serves live JSON every poll: positions, trades, log, summary.
# Starts as a daemon thread — stops when the algo process exits.
# ==========================================
import glob
from http.server import HTTPServer, BaseHTTPRequestHandler

DASHBOARD_PORT = 8765

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Algo Dashboard</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
:root{--bg:#0a0c10;--bg2:#0f1218;--bg3:#161b24;--border:#1e2530;--border2:#252d3a;--text:#c8d0dc;--dim:#5a6577;--bright:#e8edf5;--green:#00d68f;--gdim:#00512f;--red:#ff4757;--rdim:#5c0f1a;--yellow:#ffd600;--ydim:#3d3300;--cyan:#00c8ff;--cdim:#003d4d;--purple:#8b5cf6;--orange:#ff8c42;--mono:'IBM Plex Mono',monospace;--sans:'IBM Plex Sans',sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;min-height:100vh}
.hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 20px;background:var(--bg2);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100}
.logo{font-family:var(--mono);font-size:15px;font-weight:600;color:var(--cyan);letter-spacing:.08em}
.logo span{color:var(--dim);font-weight:300}
.badge{font-family:var(--mono);font-size:10px;padding:3px 8px;border-radius:3px;font-weight:600;letter-spacing:.1em;background:var(--ydim);color:var(--yellow);border:1px solid var(--yellow)}
.badge.live{background:var(--rdim);color:var(--red);border-color:var(--red)}
.hdr-r{display:flex;align-items:center;gap:14px}
.clk{font-family:var(--mono);font-size:14px;color:var(--bright);font-weight:500}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 2s infinite}
.dot.off{background:var(--rdim);box-shadow:none;animation:none}
.conn{font-family:var(--mono);font-size:10px;color:var(--dim)}
.rbtn{background:var(--bg3);border:1px solid var(--border2);color:var(--text);padding:5px 12px;border-radius:4px;cursor:pointer;font-family:var(--mono);font-size:11px}
.rbtn:hover{border-color:var(--cyan);color:var(--cyan)}
.cards{display:grid;grid-template-columns:repeat(7,1fr);gap:1px;background:var(--border);border-bottom:1px solid var(--border)}
.card{background:var(--bg2);padding:13px 15px;position:relative;overflow:hidden}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.card.cp::before{background:var(--green)}.card.cn::before{background:var(--red)}.card.cy::before{background:var(--cyan)}.card.cg::before{background:var(--dim)}.card.cyl::before{background:var(--yellow)}.card.cpr::before{background:var(--purple)}.card.cor::before{background:var(--orange)}
.clbl{font-size:10px;color:var(--dim);font-family:var(--mono);text-transform:uppercase;letter-spacing:.1em;margin-bottom:5px}
.cval{font-family:var(--mono);font-size:21px;font-weight:600;color:var(--bright);line-height:1}
.cval.pos{color:var(--green)}.cval.neg{color:var(--red)}
.csub{font-family:var(--mono);font-size:10px;color:var(--dim);margin-top:4px}
.tabs{display:flex;background:var(--bg2);border-bottom:1px solid var(--border);padding:0 20px}
.tab{font-family:var(--mono);font-size:11px;font-weight:500;padding:10px 16px;cursor:pointer;color:var(--dim);border-bottom:2px solid transparent;letter-spacing:.06em;transition:all .15s;user-select:none}
.tab:hover{color:var(--text)}.tab.a{color:var(--cyan);border-bottom-color:var(--cyan)}
.tc{display:inline-flex;align-items:center;justify-content:center;background:var(--bg3);border:1px solid var(--border2);color:var(--dim);font-size:9px;width:16px;height:16px;border-radius:3px;margin-left:5px;font-weight:600}
.tab.a .tc{background:var(--cdim);border-color:var(--cyan);color:var(--cyan)}
.main{padding:14px 20px}
.sh{font-family:var(--mono);font-size:10px;font-weight:600;color:var(--dim);text-transform:uppercase;letter-spacing:.12em;margin-bottom:8px}
.tw{background:var(--bg2);border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:14px}
table{width:100%;border-collapse:collapse}
thead th{background:var(--bg3);font-family:var(--mono);font-size:9px;font-weight:600;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;padding:7px 10px;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:var(--bg3)}
tbody td{padding:8px 10px;font-family:var(--mono);font-size:12px;color:var(--text);white-space:nowrap}
.er td{text-align:center;color:var(--dim);padding:22px;font-size:12px}
.sym{color:var(--bright);font-weight:600}
.buy{color:var(--cyan);font-weight:600}.sell{color:var(--orange);font-weight:600}
.pp{color:var(--green)}.pn{color:var(--red)}.pz{color:var(--dim)}
.so{color:var(--yellow);font-weight:600}.sc{color:var(--dim)}
.stag{font-size:9px;padding:2px 6px;border-radius:2px;background:var(--bg3);border:1px solid var(--border2);color:var(--dim)}
.ttag{font-size:9px;padding:2px 6px;border-radius:2px;background:var(--purple);color:#fff;font-weight:600;opacity:.85}
.er-tag{font-size:10px;padding:2px 6px;border-radius:2px;font-weight:600}
.et{background:var(--cdim);color:var(--cyan);border:1px solid var(--cyan)}
.es{background:var(--rdim);color:var(--red);border:1px solid var(--red)}
.etsl{background:#2a1a40;color:var(--purple);border:1px solid var(--purple)}
.esq{background:var(--ydim);color:var(--yellow);border:1px solid var(--yellow)}
.trl{display:inline-flex;align-items:center;gap:4px;font-size:10px;color:var(--purple)}
.trldot{width:5px;height:5px;border-radius:50%;background:var(--purple)}
.lw{background:var(--bg2);border:1px solid var(--border);border-radius:4px;overflow:hidden}
.li{height:300px;overflow-y:auto;padding:10px 14px;font-family:var(--mono);font-size:11px;line-height:1.6}
.li::-webkit-scrollbar{width:4px}.li::-webkit-scrollbar-track{background:var(--bg3)}.li::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
.ll{color:var(--dim);white-space:pre-wrap;word-break:break-all}
.ll.lt{color:var(--green)}.ll.le{color:var(--red)}.ll.lw{color:var(--yellow)}.ll.lc{color:var(--cyan)}.ll.ltsl{color:var(--purple)}.ll.ls{color:var(--bright)}
.pane{display:none}.pane.a{display:block}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
@keyframes fi{from{opacity:0;transform:translateY(3px)}to{opacity:1;transform:translateY(0)}}
.fi{animation:fi .18s ease}
</style>
</head>
<body>
<div class="hdr">
  <div style="display:flex;align-items:center;gap:12px">
    <div class="logo">KITE<span>/</span>ALGO</div>
    <div class="badge" id="mbadge">PAPER</div>
    <div class="conn" id="conn">● connecting…</div>
  </div>
  <div class="hdr-r">
    <div class="clk" id="clk">--:--:--</div>
    <div class="dot off" id="dot"></div>
    <button class="rbtn" onclick="fd()">↻ REFRESH</button>
  </div>
</div>
<div class="cards">
  <div class="card cp" id="cpnl"><div class="clbl">Total P&amp;L</div><div class="cval" id="vpnl">₹0.00</div><div class="csub" id="vpnl-sub">closed ₹0.00  |  open ₹0.00</div></div>
  <div class="card cy"><div class="clbl">Open</div><div class="cval" id="vopen">0</div><div class="csub">positions</div></div>
  <div class="card cg"><div class="clbl">Closed</div><div class="cval" id="vclosed">0</div><div class="csub">trades today</div></div>
  <div class="card cp"><div class="clbl">Wins</div><div class="cval pos" id="vwins">0</div><div class="csub">profitable</div></div>
  <div class="card cn"><div class="clbl">Losses</div><div class="cval neg" id="vloss">0</div><div class="csub">stop loss</div></div>
  <div class="card cyl"><div class="clbl">Win Rate</div><div class="cval" id="vwr">--%</div><div class="csub" id="swt">0 trades</div></div>
  <div class="card cpr"><div class="clbl">Updated</div><div class="cval" style="font-size:14px;padding-top:4px" id="vtime">--:--:--</div><div class="csub">auto 60s</div></div>
</div>
<div class="tabs">
  <div class="tab a" data-tab="op" onclick="st('op')">OPEN POSITIONS <span class="tc" id="tc-op">0</span></div>
  <div class="tab" data-tab="cl" onclick="st('cl')">CLOSED TRADES <span class="tc" id="tc-cl">0</span></div>
  <div class="tab" data-tab="al" onclick="st('al')">ALL POSITIONS <span class="tc" id="tc-al">0</span></div>
  <div class="tab" data-tab="lg" onclick="st('lg')">EXECUTION LOG <span class="tc" id="tc-lg">0</span></div>
</div>
<div class="main">
  <div class="pane a" id="pane-op">
    <div class="sh">Open Positions</div>
    <div class="tw"><table><thead><tr><th>Symbol</th><th>Strategy</th><th>Side</th><th>Entry</th><th>SL</th><th>Target / TSL</th><th>Qty</th><th>Trail Level</th><th>Entry Time</th><th>LTP</th><th>Live P&amp;L</th></tr></thead><tbody id="tbo"></tbody></table></div>
  </div>
  <div class="pane" id="pane-cl">
    <div class="sh">Closed Trades</div>
    <div class="tw"><table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>Qty</th><th>Points</th><th>Net P&amp;L</th><th>Exit Reason</th></tr></thead><tbody id="tbc"></tbody></table></div>
  </div>
  <div class="pane" id="pane-al">
    <div class="sh">All Positions</div>
    <div class="tw"><table><thead><tr><th>Symbol</th><th>Strategy</th><th>Side</th><th>Status</th><th>Entry</th><th>SL</th><th>Target / TSL</th><th>Trail Lvl</th><th>Qty</th><th>Time</th><th>Net P&amp;L</th></tr></thead><tbody id="tba"></tbody></table></div>
  </div>
  <div class="pane" id="pane-lg">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
      <div class="sh" style="margin:0">Execution Log</div>
      <label style="font-family:var(--mono);font-size:10px;color:var(--dim);display:flex;align-items:center;gap:4px;cursor:pointer"><input type="checkbox" id="asc" checked> AUTO-SCROLL</label>
    </div>
    <div class="lw"><div class="li" id="li"></div></div>
  </div>
</div>
<script>
const SRV='';
let ok=false;
function uc(){document.getElementById('clk').textContent=new Date().toTimeString().slice(0,8)}
setInterval(uc,1000);uc();
function st(n){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('a'));
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('a'));
  document.querySelector('[data-tab="'+n+'"]').classList.add('a');
  document.getElementById('pane-'+n).classList.add('a');
  if(n==='lg')sl();
}
function fn(v){if(v===null||v===undefined||v===''||v==='None')return '—';const n=parseFloat(v);return isNaN(n)?v:n.toFixed(2)}
function pc(v){const n=parseFloat(v);if(isNaN(n))return '';return n>0?'pp':n<0?'pn':'pz'}
function et(r){if(!r)return '—';const u=r.toUpperCase();if(u.includes('TARGET'))return '<span class="er-tag et">'+r+'</span>';if(u.includes('TSL')||u.includes('TRAIL'))return '<span class="er-tag etsl">'+r+'</span>';if(u.includes('SQUARE')||u.includes('SOF'))return '<span class="er-tag esq">'+r+'</span>';return '<span class="er-tag es">'+r+'</span>'}
function lc(l){const lo=l.toLowerCase();if(lo.includes('signal')||lo.includes('entry')||lo.includes(' buy ')||lo.includes(' sell '))return 'lt';if(lo.includes('error')||lo.includes('failed')||lo.includes('\u274c')||lo.includes('\ud83d\udea8'))return 'le';if(lo.includes('warn')||lo.includes('\u26a0')||lo.includes('could not'))return 'lw';if(lo.includes('closed')||lo.includes('target hit')||lo.includes('sl hit')||lo.includes('square'))return 'lc';if(lo.includes('trail')||lo.includes('tsl'))return 'ltsl';if(lo.includes('\u2705')||lo.includes('\ud83d\ude80')||lo.includes('connected')||lo.includes('engine'))return 'ls';return ''}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function sl(){const e=document.getElementById('li');if(document.getElementById('asc').checked)e.scrollTop=e.scrollHeight}

async function fd(){
  try{
    const r=await fetch(SRV+'/data?'+Date.now(),{cache:'no-store'});
    if(!r.ok)throw new Error(r.status);
    const d=await r.json();
    if(!ok){ok=true;document.getElementById('dot').classList.remove('off');document.getElementById('conn').textContent='● live';document.getElementById('conn').style.color='var(--green)'}
    render(d);
  }catch(e){
    ok=false;document.getElementById('dot').classList.add('off');
    document.getElementById('conn').textContent='● server offline';document.getElementById('conn').style.color='var(--red)';
  }
}

function render(d){
  const s=d.summary;
  const pnl=parseFloat(s.daily_pnl)||0;
  const opnl=parseFloat(s.open_pnl)||0;
  const cpnl=parseFloat(s.closed_pnl)||0;
  document.getElementById('vpnl').textContent='₹'+pnl.toFixed(2);
  document.getElementById('vpnl').className='cval '+(pnl>0?'pos':pnl<0?'neg':'');
  document.getElementById('cpnl').className='card '+(pnl>0?'cp':pnl<0?'cn':'cg');
  const sub=document.getElementById('vpnl-sub');
  if(sub){sub.textContent='closed ₹'+cpnl.toFixed(2)+'  |  open ₹'+opnl.toFixed(2);}
  const badge=document.getElementById('mbadge');
  badge.textContent=s.mode||'PAPER';
  if((s.mode||'').toUpperCase()==='LIVE'){badge.classList.add('live');}else{badge.classList.remove('live');}
  document.getElementById('vopen').textContent=s.open_count;
  document.getElementById('vclosed').textContent=s.closed_count;
  document.getElementById('vwins').textContent=s.wins;
  document.getElementById('vloss').textContent=s.losses;
  document.getElementById('vwr').textContent=s.win_rate+'%';
  document.getElementById('swt').textContent=s.total_trades+' trades';
  document.getElementById('vtime').textContent=s.time;
  document.getElementById('tc-op').textContent=s.open_count;
  document.getElementById('tc-cl').textContent=s.closed_count;
  document.getElementById('tc-al').textContent=s.open_count+s.closed_count;

  // Open positions
  const op=d.positions.filter(p=>p.status==='OPEN');
  const tbo=document.getElementById('tbo');
  tbo.innerHTML=op.length?op.map(p=>{
    const tsl=p.tsl_only==='True'||p.tsl_only===true;
    const tgt=tsl?'<span class="ttag">TSL '+( p.tsl_step||1.5)+'%</span>':fn(p.target);
    const trl=parseFloat(p.trail_level)||0;
    const trlc=trl>0?'<span class="trl"><span class="trldot"></span>'+trl.toFixed(2)+'%</span>':'<span style="color:var(--dim)">—</span>';
    const ltp=p.ltp!==''&&p.ltp!==undefined?parseFloat(p.ltp):null;
    const lpnl=p.live_pnl!==''&&p.live_pnl!==undefined?parseFloat(p.live_pnl):null;
    const ltpc=ltp!==null?fn(ltp):'<span style="color:var(--dim)">—</span>';
    const lpnlc=lpnl!==null?'<span style="color:'+(lpnl>=0?'var(--green)':'var(--red)')+';font-weight:600">'+(lpnl>=0?'+':'')+'₹'+lpnl.toFixed(2)+'</span>':'<span style="color:var(--dim)">—</span>';
    return '<tr class="fi"><td class="sym">'+p.symbol+'</td><td><span class="stag">'+(p.strategy||'—')+'</span></td><td class="'+(p.side==='BUY'?'buy':'sell')+'">'+p.side+'</td><td>'+fn(p.entry)+'</td><td style="color:var(--red)">'+fn(p.sl)+'</td><td>'+tgt+'</td><td>'+p.qty+'</td><td>'+trlc+'</td><td style="color:var(--dim)">'+(p.entry_time||'—')+'</td><td>'+ltpc+'</td><td>'+lpnlc+'</td></tr>'
  }).join(''):'<tr class="er"><td colspan="11">No open positions</td></tr>';

  // Closed trades
  const cl=[...d.trades].reverse();
  const tbc=document.getElementById('tbc');
  tbc.innerHTML=cl.length?cl.map(t=>{
    const pnl=parseFloat(t.PnL)||0;
    const entry=parseFloat(t.Entry)||0;
    const exit=parseFloat(t.Exit)||0;
    const pts=t.Side==='BUY'?(exit-entry):(entry-exit);
    const tm=t.Time?String(t.Time).slice(11,19):'—';
    return '<tr class="fi"><td style="color:var(--dim)">'+tm+'</td><td class="sym">'+t.Symbol+'</td><td class="'+(t.Side==='BUY'?'buy':'sell')+'">'+t.Side+'</td><td>'+fn(t.Entry)+'</td><td>'+fn(t.Exit)+'</td><td>'+t.Qty+'</td><td class="'+(pts>=0?'pp':'pn')+'">'+pts.toFixed(2)+'</td><td class="'+pc(t.PnL)+'">₹'+pnl.toFixed(2)+'</td><td>'+et(t.Reason)+'</td></tr>'
  }).join(''):'<tr class="er"><td colspan="9">No closed trades yet</td></tr>';

  // All positions
  const tba=document.getElementById('tba');
  tba.innerHTML=d.positions.length?d.positions.map(p=>{
    const tsl=p.tsl_only==='True'||p.tsl_only===true;
    const tgt=tsl?'<span class="ttag">TSL '+(p.tsl_step||1.5)+'%</span>':fn(p.target);
    const trl=parseFloat(p.trail_level)||0;
    const trlc=trl>0?'<span class="trl"><span class="trldot"></span>'+trl.toFixed(2)+'%</span>':'—';
    const net=parseFloat(p.net_pnl)||0;
    const sc=p.status==='OPEN'?'so':'sc';
    return '<tr class="fi"><td class="sym">'+p.symbol+'</td><td><span class="stag">'+(p.strategy||'—')+'</span></td><td class="'+(p.side==='BUY'?'buy':'sell')+'">'+p.side+'</td><td class="'+sc+'">'+p.status+'</td><td>'+fn(p.entry)+'</td><td style="color:var(--red)">'+fn(p.sl)+'</td><td>'+tgt+'</td><td>'+trlc+'</td><td>'+p.qty+'</td><td style="color:var(--dim)">'+(p.entry_time||'—')+'</td><td class="'+pc(p.net_pnl)+'">'+( net!==0?'₹'+net.toFixed(2):'—')+'</td></tr>'
  }).join(''):'<tr class="er"><td colspan="11">No positions yet</td></tr>';

  // Log
  const li=document.getElementById('li');
  li.innerHTML=d.log.map(l=>'<div class="ll '+lc(l)+'">'+esc(l)+'</div>').join('');
  document.getElementById('tc-lg').textContent=d.log.length;
  sl();
}
fd();
setInterval(fd,60000);
</script>
</body>
</html>"""

def _dashboard_get_json():
    import json as _json, csv as _csv, os as _os
    today_d = datetime.now().strftime("%d-%m-%Y")

    def _read_csv(path):
        rows = []
        try:
            if _os.path.exists(path):
                with open(path, newline='', encoding='utf-8', errors='replace') as fh:
                    rows = list(_csv.DictReader(fh))
        except Exception:
            pass
        return rows

    def _read_log(path, n=80):
        try:
            if _os.path.exists(path):
                with open(path, 'r', encoding='utf-8', errors='replace') as fh:
                    lines = fh.readlines()
                return [l.rstrip() for l in lines[-n:]]
        except Exception:
            pass
        return []

    trades    = _read_csv(paper_trade_log_file)
    log_lines = _read_log(LOG_TXT_FILE)

    # ── Build positions list from in-memory paper_positions ──────────────
    # Always current — works for both PAPER and LIVE, survives restarts.
    # CSV (POSITIONS_CACHE_FILE) can be stale; paper_positions is the source of truth.
    positions  = []
    open_pnl_snap  = 0.0
    open_c  = 0
    closed_c = 0

    try:
        for sym, pos in paper_positions.items():
            status = pos.get("status", "OPEN")
            # Pyramid legs use the base symbol for LTP
            real_sym = pos.get("pyramid_base", sym) if "__PYR" in sym else sym
            ltp    = latest_prices.get(real_sym)

            if status == "OPEN" and ltp:
                entry  = float(pos.get("entry", 0))
                qty    = int(pos.get("qty", 1))
                side   = pos.get("side", "BUY")
                pts    = (ltp - entry) if side == "BUY" else (entry - ltp)
                live_pnl_val = round(pts * qty - BROKERAGE_PER_ORDER * 2, 2)
                open_pnl_snap += pts * qty
                # BUG FIX 23: count only BASE positions so dashboard "Open: N"
                # shows number of trades, not base+legs combined.
                if "__PYR" not in str(sym):
                    open_c += 1
            else:
                live_pnl_val = ""
                ltp = ltp or ""
                if status != "OPEN":
                    # BUG FIX 23b: count only base closed positions
                    if "__PYR" not in str(sym):
                        closed_c += 1

            positions.append({
                "symbol":      sym,
                "side":        pos.get("side", ""),
                "strategy":    pos.get("strategy", ""),
                "entry":       pos.get("entry", ""),
                "sl":          pos.get("sl", ""),
                "target":      pos.get("target", ""),
                "qty":         pos.get("qty", ""),
                "trail_level": pos.get("trail_level", 0),
                "tsl_only":    pos.get("tsl_only", False),
                "tsl_step":    pos.get("tsl_step", TRAIL_STEP_PERCENT),
                "entry_time":  pos.get("entry_time", ""),
                "status":      status,
                "net_pnl":     pos.get("net_pnl", ""),
                "ltp":         ltp,
                "live_pnl":    live_pnl_val,
            })
    except Exception:
        pass

    # ── PnL summary ───────────────────────────────────────────────────────
    try:    closed_pnl_snap = float(daily_pnl)
    except Exception: closed_pnl_snap = 0.0
    total_pnl_snap = round(closed_pnl_snap + open_pnl_snap, 2)

    wins = losses = 0
    for t in trades:
        try:
            # BUG FIX 22: skip PYR leg rows — only count base positions
            # in dashboard wins/losses so win rate is not inflated.
            if "__PYR" in str(t.get("Symbol", "")):
                continue
            pv = float(t.get("PnL", 0) or 0)
            if pv > 0: wins  += 1
            else:      losses += 1
        except Exception: pass

    total = wins + losses
    summary = {
        "daily_pnl":    total_pnl_snap,
        "open_pnl":     round(open_pnl_snap, 2),
        "closed_pnl":   round(closed_pnl_snap, 2),
        "open_count":   open_c,
        "closed_count": closed_c,
        "wins": wins, "losses": losses,
        "win_rate":     round(wins / total * 100, 1) if total else 0,
        "total_trades": total,
        "time": datetime.now().strftime("%H:%M:%S"),
        "mode": TRADING_MODE,
    }
    return _json.dumps({"summary": summary, "positions": positions,
                        "trades": trades, "log": log_lines}).encode()


class _DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        pass   # suppress all dashboard HTTP request logs

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        try:
            if self.path.startswith("/data"):
                body = _dashboard_get_json()
                ct   = "application/json"
            else:
                body = DASHBOARD_HTML.encode("utf-8", errors="replace")
                ct   = "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self._cors()
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as _de:
            import traceback
            print(f"[Dashboard] do_GET error: {_de}")
            traceback.print_exc()
            try:
                err = b"Server error"
                self.send_response(500)
                self.send_header("Content-Length", str(len(err)))
                self.end_headers()
                self.wfile.write(err)
            except Exception:
                pass


def _start_dashboard():
    """Start the built-in dashboard HTTP server as a daemon thread."""
    try:
        server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), _DashboardHandler)
        print(Fore.CYAN + f"📊 Dashboard running → http://localhost:{DASHBOARD_PORT}  (same machine)"
              f"  |  AWS: http://<this-server-ip>:{DASHBOARD_PORT}" + ts())
        server.serve_forever()
    except OSError as e:
        print(Fore.YELLOW + f"⚠️  Dashboard could not start on port {DASHBOARD_PORT}: {e}" + ts())

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

    print(Fore.CYAN + f"Symbols Loaded: {len(tokens)}")
    print(Fore.CYAN + f"Fixed Qty: {FIXED_QTY}")
    print(Fore.CYAN + f"Stop Loss %: {STOP_LOSS_PERCENT}%")
    print(Fore.CYAN + f"Target %: {TARGET_PERCENT}%")
    print(Fore.CYAN + f"Trail Step %: {TRAIL_STEP_PERCENT}%")
    print(Fore.CYAN + f"Square Off Time: {SQUARE_OFF_TIME}")
    print(Fore.CYAN + f"Daily Max Loss %: {DAILY_MAX_LOSS_PERCENT}%")

    print("="*70 + "\n")

    write_log(f"ENGINE STARTED | MODE: {TRADING_MODE}")

    # ── Daily data — smart download ──────────────────────────────────────
    # First run (no files): downloads automatically, no prompt.
    # Subsequent runs same day: files already exist → skips silently.
    # New day: today's date not in file → downloads fresh automatically.
    # User can still force-skip by setting FORCE_SKIP_DAILY_DOWNLOAD = True.
    # ─────────────────────────────────────────────────────────────────────
    def _daily_data_is_fresh():
        """Return True if today's daily data already exists for most symbols."""
        today_str_check = datetime.now().strftime("%Y-%m-%d")
        found = 0
        for sym in SYMBOLS[:10]:   # check first 10 as a sample
            fp = os.path.join(DAILY_DIR, f"{sym}_daily.csv")
            if not os.path.exists(fp):
                return False        # file missing → need download
            try:
                df_chk = pd.read_csv(fp, usecols=["date"])
                df_chk["date"] = pd.to_datetime(df_chk["date"])
                # Zerodha daily candles are timestamped at prev day 18:30,
                # so today's session appears as yesterday+1. We just check
                # whether the file was modified today as a proxy.
                if os.path.getmtime(fp) > (datetime.now() - timedelta(hours=20)).timestamp():
                    found += 1
            except Exception:
                return False
        return found >= 8   # 8/10 sample symbols have fresh files

    if _daily_data_is_fresh():
        print(Fore.CYAN + "📂 Daily data already fresh for today — skipping download" + ts())
    else:
        print(Fore.CYAN + "📡 Downloading daily data (first run or new day)..." + ts())
        fresh_daily_download()

    # Load required data for live/paper mode
    today = pd.to_datetime(datetime.now().date())

    load_yesterday_from_local(today)
    load_ema_data_from_local(today)
    load_orb_and_first15_from_kite()

    # ── Restore state from previous session (if any) ─────────────────
    if TRADING_MODE == "PAPER":
        load_paper_positions_cache()   # restore paper_positions + daily_pnl
    if TRADING_MODE == "LIVE":
        load_paper_positions_cache()   # restore paper_positions (entry prices, SL, trail_level)
        load_live_positions_cache()    # restore trades_taken + restart OCO monitors

    # ── Start background threads (after prompts complete) ─────────────
    if TRADING_MODE == "LIVE":
        threading.Thread(target=position_manager,       daemon=True).start()
    # FIX B: paper_position_manager runs in BOTH modes.
    # In LIVE mode it handles: TSL updates, pyramid triggering, and
    # cache saves after trail events. Exits via "continue" before any
    # position close (those are handled exclusively by oco_monitor).
    # Previously only started in PAPER mode → pyramiding NEVER fired in LIVE.
    threading.Thread(target=paper_position_manager, daemon=True).start()
    threading.Thread(target=auto_square_off,           daemon=True).start()
    threading.Thread(target=_refresh_orb_cache_loop,  daemon=True).start()  # ← 5-min refresh: ORB/F15/C2/C3/C4/H/L
    threading.Thread(target=_start_dashboard,          daemon=True).start()  # ← built-in dashboard on port 8765

    # ── Start WebSocket ───────────────────────────────────────────────
    kws.connect(threaded=False, disable_ssl_verification=False)