# ==========================================
# 🤖 Telegram Trading Agent
# Version: 4.3 (Automatic PnL Auditor + Fixes)
# ==========================================
import os
import sys
import json
import time
import asyncio
import re
import threading
import pandas as pd
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from kiteconnect import KiteConnect, exceptions as kite_exc
from colorama import init, Fore, Style

# Reuse your existing logic modules
sys.path.append(os.getcwd())
try:
    from smc_engine import detect_market_structure
    from astro_time import get_time_signal, is_good_entry_time
    SMC_ENABLED = True
except ImportError:
    SMC_ENABLED = False

init(autoreset=True)

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_ID   = 29840797
API_HASH = "9069896125bdbc5bacccfec478e8c64a"
PHONE    = "+919164575142"
TARGET_CHANNEL_ID = -1002360390807  # AutoBotTest123

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")
API_KEY = "7am67kxijfsusk9i"

# Paths
AGENT_TRADES_FILE = os.path.join(BASE_DIR, f"agent_paper_trades_{datetime.now().strftime('%d-%m-%Y')}.csv")
PERSISTENCE_FILE  = os.path.join(BASE_DIR, f"agent_active_trades_{datetime.now().strftime('%d-%m-%Y')}.json")
CACHE_DIR         = os.path.join(BASE_DIR, "CACHE")

# Execution Rules
PAPER_MODE       = True        
MAX_CONCURRENT   = 5           
MAX_DAILY_TRADES = 30          
ITM_STRIKE_OFFSET_INDEX = 100  
ITM_STRIKE_OFFSET_STOCK = 50   

# --- ENTRY TIME WINDOW ---
START_TIME       = "09:30"     
END_TIME         = "14:45"     

# --- STOP LOSS / TARGET LOGIC ---
INITIAL_SL_PCT   = 15.0        
TARGET_PCT       = 50.0        
T1_HIT_PCT       = 20.0        
T2_HIT_PCT       = 30.0        
SQUARE_OFF_TIME  = "15:10"     
MARKET_BUFFER_PCT = 3.0        # 3% buffer for market-to-limit simulation
MIN_OPTION_VOLUME = 50000      # Minimum daily volume to enter a contract
MAX_BID_ASK_SPREAD_PCT = 2.0   # Maximum allowed Bid-Ask spread %

CHASE_LIMIT_PCT  = 0.3         
CONVICTION_THRESHOLD = 6       

PRODUCT_TYPE     = "MIS"       
AGENT_TAG        = "TG_AGENT"  

# ==========================================
# 🔌 KITE INITIALIZATION
# ==========================================
kite = KiteConnect(api_key=API_KEY)
try:
    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE, "r") as f:
            kite.set_access_token(f.read().strip())
        print(Fore.GREEN + f"✅ Kite Session Connected (Version 4.3)")
    else:
        print(Fore.RED + f"❌ Kite Access Token file missing.")
        sys.exit(1)
except Exception as e:
    print(Fore.RED + f"❌ Kite Session Error: {e}")
    sys.exit(1)

# ==========================================
# 🧠 AGENT STATE & UTILS
# ==========================================
active_positions = {} 
trade_counter    = 0
_NFO_INSTRUMENTS = None
_NSE_INSTRUMENTS = None

SYMBOL_BLACKLIST = {"AND", "THE", "FOR", "IST", "ZONE", "SIDE", "HIGH", "LOW", "PANCHAK", "SMC", "BOS", "CHOCH", "RANGE", "OPEN", "GREEN", "RED", "BREAK", "INSIDE", "STYLE", "SETUP", "LIST", "STOCKS", "DAILY", "WEEKLY", "STRENGTH", "CONFIRMED", "ENTRIES", "SHORT", "LONG", "ALERT", "COMBINED", "DASHBOARD", "BREAKDOWN", "BREAKOUT", "ABOVE", "BELOW", "NIFTY50", "BANK", "NIFTYBANK", "ENTRY", "LOWER", "UPPER", "TREND", "OHLC", "PRICE", "MATRIX", "DETAILS", "CHANGE", "CHG", "YEST", "CLOSE", "TIME", "LEVEL", "CONFLUENCE", "STOCK", "SYMBOL", "SYMBOLS", "INDICATORS", "HTF", "STRUCTURE", "BULLISH", "BEARISH", "REJECTED", "APPROVED", "BREAKS", "YES", "NO", "STRENGTH"}

def _round_tick(price):
    """Ensure price is compliant with 0.05 tick size for options."""
    return round(float(price) * 20) / 20

def place_market_order_safe(exch, sym, transaction_type, qty, ltp):
    """Simulate a market order using a Limit order with a price buffer to bypass Kite restrictions."""
    try:
        if transaction_type == kite.TRANSACTION_TYPE_BUY:
            price = _round_tick(ltp * (1 + MARKET_BUFFER_PCT / 100))
        else:
            price = _round_tick(ltp * (1 - MARKET_BUFFER_PCT / 100))
            
        return kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exch,
            tradingsymbol=sym,
            transaction_type=transaction_type,
            quantity=qty,
            product=PRODUCT_TYPE,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=price,
            tag=AGENT_TAG
        )
    except Exception as e:
        print(Fore.RED + f"❌ Order Placement Error: {e}")
        return None

def save_active_state():
    try:
        with open(PERSISTENCE_FILE, "w") as f:
            json.dump(active_positions, f, indent=4)
    except Exception: pass

def audit_journal_math():
    """Startup fix: Ensures net_pnl = (exit_price - entry_price) * qty for all closed trades."""
    if not os.path.exists(AGENT_TRADES_FILE): return
    try:
        df = pd.read_csv(AGENT_TRADES_FILE)
        if df.empty: return
        
        fixed_count = 0
        for i, row in df.iterrows():
            if row['status'] == 'CLOSED':
                # Re-calculate PnL
                entry = float(row['entry_price'])
                exit  = float(row.get('exit_price', entry))
                qty   = float(row['qty'])
                actual_pnl = round((exit - entry) * qty, 2)
                
                if abs(float(row.get('net_pnl', 0)) - actual_pnl) > 0.1:
                    df.at[i, 'net_pnl'] = actual_pnl
                    fixed_count += 1
        
        if fixed_count > 0:
            df.to_csv(AGENT_TRADES_FILE, index=False)
            print(Fore.YELLOW + f"🩹 Audited Journal: {fixed_count} records corrected.")
    except Exception as e:
        print(f"Audit Error: {e}")

def load_active_state():
    global active_positions, trade_counter
    
    # Run Auditor first to fix history
    audit_journal_math()

    if os.path.exists(PERSISTENCE_FILE):
        try:
            with open(PERSISTENCE_FILE, "r") as f:
                data = json.load(f)
                active_positions.clear()
                for k, v in data.items():
                    # Correction: if spot_entry is suspicious, try to refetch
                    if 'spot_entry' not in v or v['spot_entry'] < 100:
                        v['spot_entry'] = get_spot_ltp(v.get('symbol', '')) or 0
                    if 'active_sl' not in v: v['active_sl'] = v.get('sl', 0)
                    active_positions[k] = v
            print(Fore.CYAN + f"🔄 Resumed {len(active_positions)} active trades.")
        except Exception: pass
    
    stats = get_daily_stats()
    trade_counter = stats['total_attempted']
    print(Fore.CYAN + f"📊 Today's stats: {stats['trades']} Closed | {len(active_positions)} Open | Total: {trade_counter}/{MAX_DAILY_TRADES}")

def get_daily_stats():
    stats = {"pnl": 0.0, "trades": 0, "wins": 0, "losses": 0, "total_attempted": 0, "closed_trades": []}
    if not os.path.exists(AGENT_TRADES_FILE): return stats
    try:
        df = pd.read_csv(AGENT_TRADES_FILE)
        if df.empty: return stats
        stats['total_attempted'] = len(df)
        closed = df[df['status'] == 'CLOSED'].copy()
        if not closed.empty:
            closed['net_pnl'] = pd.to_numeric(closed['net_pnl'], errors='coerce').fillna(0)
            stats['pnl'] = float(closed['net_pnl'].sum())
            stats['trades'] = len(closed)
            stats['wins'] = len(closed[closed['net_pnl'] > 0])
            stats['losses'] = len(closed[closed['net_pnl'] < 0])
            stats['closed_trades'] = closed.tail(10).to_dict('records')
    except Exception: pass
    return stats

def map_index_symbol(symbol):
    mapping = {"NIFTY": "NSE:NIFTY 50", "BANKNIFTY": "NSE:NIFTY BANK", "FINNIFTY": "NSE:NIFTY FIN SERVICE", "SENSEX": "BSE:SENSEX"}
    return mapping.get(symbol.upper(), f"NSE:{symbol}")

def get_spot_ltp(symbol):
    if not symbol: return 0
    try:
        k = map_index_symbol(symbol); d = kite.ltp(k)
        return d[k]["last_price"] if k in d else 0
    except Exception: return 0

# --- Strict Schema for CSV ---
AGENT_COLUMNS = [
    "timestamp", "symbol", "tradingsymbol", "side", "qty", "entry_price", "ltp", 
    "sl", "active_sl", "target", "status", "net_pnl", "strategy", "score", 
    "spot_entry", "spot_ltp", "spot_chg", "exit_time", "exit_price", "exit_reason"
]

def log_trade_to_csv(data):
    try:
        # Ensure all columns exist in the data dict
        for col in AGENT_COLUMNS:
            if col not in data:
                data[col] = ""
        
        # Create DataFrame with explicit column order
        df = pd.DataFrame([data])[AGENT_COLUMNS]
        ex = os.path.exists(AGENT_TRADES_FILE)
        df.to_csv(AGENT_TRADES_FILE, mode='a', header=not ex, index=False)
    except Exception as e:
        print(f"Log CSV Error: {e}")

def update_trade_csv(opt_symbol, updates):
    """Robust CSV Updater with math verification and schema enforcement."""
    try:
        if not os.path.exists(AGENT_TRADES_FILE): return
        df = pd.read_csv(AGENT_TRADES_FILE)
        
        # Ensure schema is intact in the loaded DF
        for col in AGENT_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        sym = opt_symbol.split(':')[-1]
        
        # Only update the OPEN row for this symbol
        mask = (df['tradingsymbol'].str.endswith(sym)) & (df['status'] == 'OPEN')
        if not df.loc[mask].empty:
            # Audit Math: If this is an exit update, force correct PnL calculation
            if 'exit_price' in updates:
                # Use current row's data for calculation
                _row = df.loc[mask].iloc[0]
                entry = float(_row['entry_price'])
                exit  = float(updates['exit_price'])
                qty   = float(_row['qty'])
                updates['net_pnl'] = round((exit - entry) * qty, 2)
            
            for k, v in updates.items():
                if k in df.columns:
                    df.loc[mask, k] = v
            
            # Save with explicit column order
            df[AGENT_COLUMNS].to_csv(AGENT_TRADES_FILE, index=False)
    except Exception as e:
        print(f"CSV Audit Error: {e}")

def get_itm_strike(symbol, ltp, side):
    is_idx = any(x in symbol for x in ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"])
    off = ITM_STRIKE_OFFSET_INDEX if is_idx else ITM_STRIKE_OFFSET_STOCK
    step = 100 if "BANKNIFTY" in symbol or "SENSEX" in symbol else (50 if "NIFTY" in symbol else 5)
    if side == "CE": return ((ltp - off) // step) * step
    else: return ((ltp + off) // step + 1) * step

def get_real_price(symbol, side="BUY"):
    """Fetch realistic execution price from Market Depth. side='BUY' -> Ask, side='SELL' -> Bid."""
    try:
        q = kite.quote(symbol)[symbol]
        depth = q.get('depth', {})
        if side == "BUY":
            # We buy at the first Ask (Offer)
            price = depth.get('sell', [{'price': 0}])[0].get('price', 0)
            return price if price > 0 else q.get('last_price', 0)
        else:
            # We sell at the first Bid
            price = depth.get('buy', [{'price': 0}])[0].get('price', 0)
            return price if price > 0 else q.get('last_price', 0)
    except Exception: return 0

def find_best_option(symbol, strike, side):
    """Finds the best liquid option strike near the target level."""
    try:
        df = pd.DataFrame(kite.instruments("NFO"))
        opts = df[(df['name'] == symbol) & (df['instrument_type'] == side)]
        if opts.empty: return None, 0
        
        opts = opts.copy(); opts['sd'] = (opts['strike'] - strike).abs()
        # Evaluate top 5 closest strikes for liquidity
        candidates = opts.sort_values(['sd', 'expiry']).head(5)
        
        for _, row in candidates.iterrows():
            tsym = row['tradingsymbol']; full_s = f"NFO:{tsym}"
            try:
                q = kite.quote(full_s)[full_s]
                vol = q.get('volume', 0)
                d = q.get('depth', {})
                bid = d.get('buy', [{'price': 0}])[0].get('price', 0)
                ask = d.get('sell', [{'price': 0}])[0].get('price', 0)
                
                # Check liquidity constraints
                if vol >= MIN_OPTION_VOLUME and bid > 0 and ask > 0:
                    spread = ((ask - bid) / bid) * 100
                    if spread <= MAX_BID_ASK_SPREAD_PCT:
                        print(Fore.GREEN + f"✅ Found Liquid Contract: {tsym} (Vol: {vol} | Spread: {spread:.2f}%)")
                        return tsym, int(row['lot_size'])
                else:
                    print(Fore.YELLOW + f"⚠️ Skipping {tsym}: Low Liquidity (Vol: {vol} | Bid: {bid} | Ask: {ask})")
            except Exception: continue
            
    except Exception as e: print(f"Strike Error: {e}")
    
    print(Fore.RED + f"❌ No liquid {side} contracts found for {symbol} meeting constraints.")
    return None, 0

async def validate_conviction(symbol, direction, strategy, ref_level):
    score = 0; reasons = []
    if SMC_ENABLED:
        try:
            from ohlc_store import OHLCStore
            candles = OHLCStore().get(symbol, n=50)
            if not candles or len(candles) < 10:
                tk = get_token_helper(symbol)
                if tk: candles = kite.historical_data(tk, datetime.now()-timedelta(days=10), datetime.now(), "60minute")
            if candles:
                ms = detect_market_structure(candles); trend = ms.get('trend', 'RANGING')
                if direction in trend: score += 3; reasons.append(f"SMC HTF ✅ ({trend})")
                elif trend == "RANGING": score += 1; reasons.append("SMC HTF 🟡")
        except Exception: pass
    oi = get_oi_bias()
    if direction == oi: score += 2; reasons.append(f"OI Bias ✅ ({oi})")
    elif oi == "NEUTRAL": score += 1; reasons.append("OI Bias 🟡")
    if is_good_entry_time(): score += 1; reasons.append("Astro Time ✅")
    if any(x in strategy for x in ["SMC", "BOS", "PI-IND", "YESTERDAY"]): score += 2; reasons.append("High-Conv Strat ✅")
    else: score += 1
    spot = get_spot_ltp(symbol)
    if spot > 0 and ref_level > 0:
        ch = abs(spot - ref_level) / ref_level * 100
        if ch <= 0.1: score += 2; reasons.append(f"Perfect Entry ✅ ({ch:.2f}%)")
        elif ch <= CHASE_LIMIT_PCT: score += 1; reasons.append(f"Good Entry ✅ ({ch:.2f}%)")
    return score, reasons, spot

def get_oi_bias():
    try:
        today_k = datetime.now().strftime("%Y%m%d")
        p = os.path.join(CACHE_DIR, f"oi_intelligence_{today_k}.json")
        if os.path.exists(p):
            with open(p, "r") as f: data = json.load(f)
            return data.get("market_direction", "NEUTRAL")
    except Exception: pass
    return "NEUTRAL"

def get_token_helper(symbol):
    df = pd.DataFrame(kite.instruments("NSE"))
    k_name = map_index_symbol(symbol).split(':')[-1]
    row = df[df.tradingsymbol == k_name]
    return int(row.iloc[0].instrument_token) if not row.empty else None

async def handle_new_alert(event):
    global trade_counter
    msg = event.message.message; msg_upper = msg.upper()
    now_str = datetime.now().strftime("%H:%M")
    if now_str < START_TIME or now_str > END_TIME: return 
    STRATS = ["SMC + PANCHAK", "BOS", "CHOCH", "INSIDE BAR", "OPEN STRUCTURE", "OPEN GREEN", "OPEN RED", "15-MIN RANGE", "1H RANGE", "RANGE BREAK", "YESTERDAY RED", "YESTERDAY GREEN", "OPEN INSIDE", "PI-IND STYLE"]
    found_strat = next((s for s in STRATS if s in msg_upper), None)
    if not found_strat: return 
    try:
        raw_symbols = re.findall(r"(?:•|🔴|🟢|📌|Stock:|Symbols:|📋 Stocks:)\s*([A-Z0-9]{3,})", msg) + \
                      re.findall(r"([A-Z0-9]{3,})\s+LTP:", msg)
        symbols = list(dict.fromkeys([s.upper() for s in raw_symbols if s.upper() not in SYMBOL_BLACKLIST]))
        if not symbols: return
        for symbol in symbols:
            is_bull = any(x in msg_upper for x in ["🟢", "BULLISH", "UP", "GREEN", "BREAKOUT", "↑", "LONG"])
            is_bear = any(x in msg_upper for x in ["🔴", "BEARISH", "DOWN", "RED", "BREAKDOWN", "↓", "SHORT", "🔻"])
            if "RED" in msg_upper or "SHORT" in msg_upper or "🔻" in msg_upper: is_bull = False; is_bear = True
            elif "GREEN" in msg_upper or "LONG" in msg_upper: is_bear = False; is_bull = True
            if not (is_bull or is_bear): continue
            direction = "BULLISH" if is_bull else "BEARISH"; side = "CE" if is_bull else "PE"
            if trade_counter >= MAX_DAILY_TRADES or len(active_positions) >= MAX_CONCURRENT: return
            if symbol in active_positions: continue
            ltp_m = re.search(r"LTP:(?:Rs\.)?\s*([0-9,.]+)", msg, re.I)
            lev_m = re.search(r"(?:at|Level|Entry|High|Low|Range|Parent High):\s*(?:Below|Above|Rs\.)?\s*([0-9,.]+)", msg, re.I)
            ltp_val = float(ltp_m.group(1).replace(',', '')) if ltp_m else 0
            ref_level = float(lev_m.group(1).replace(',', '')) if lev_m else 0
            score_full, reasons, live_spot = await validate_conviction(symbol, direction, found_strat, ref_level)
            if score_full < CONVICTION_THRESHOLD: continue
            print(Fore.YELLOW + "\n" + "="*65); print(Fore.YELLOW + f"📥 {found_strat} ALERT ACCEPTED")
            print(Fore.WHITE + Style.BRIGHT + f"🧐 Evaluating: {symbol} ({direction})")
            print(Fore.CYAN + f"   Spot LTP: {live_spot} | Score: {score_full}/10")
            for r in reasons: print(f"   • {r}")
            strike = get_itm_strike(symbol, live_spot or ltp_val or ref_level, side)
            opt_sym, lot = find_best_option(symbol, strike, side)
            if not opt_sym: continue
            await execute_trade(symbol, opt_sym, side, lot, found_strat, score_full, live_spot); break 
    except Exception: pass

async def execute_trade(spot_symbol, opt_symbol, side, lot, strategy, score, spot_entry):
    global trade_counter
    try:
        exch = "BFO" if spot_symbol == "SENSEX" else "NFO"; full_sym = f"{exch}:{opt_symbol}"
        # Use realistic Ask price for entry
        opt_buy_price = get_real_price(full_sym, side="BUY")
        if opt_buy_price <= 0: return
        
        if not PAPER_MODE:
            order_id = place_market_order_safe(exch, opt_symbol, kite.TRANSACTION_TYPE_BUY, lot, opt_buy_price)
        else: order_id = f"PAPER_{int(time.time())}"
        
        trade_counter += 1
        pos_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            "symbol": spot_symbol, 
            "tradingsymbol": full_sym, 
            "side": side, 
            "qty": lot, 
            "entry_price": opt_buy_price, 
            "ltp": opt_buy_price, 
            "sl": round(opt_buy_price * (1 - INITIAL_SL_PCT/100), 2), 
            "active_sl": round(opt_buy_price * (1 - INITIAL_SL_PCT/100), 2),
            "target": round(opt_buy_price * (1 + TARGET_PCT/100), 2), 
            "status": "OPEN", 
            "net_pnl": 0.0, 
            "strategy": strategy, 
            "score": score, 
            "spot_entry": spot_entry, 
            "spot_ltp": spot_entry, 
            "spot_chg": 0.0
        }
        active_positions[spot_symbol] = pos_data; save_active_state(); log_trade_to_csv(pos_data)
        print(Fore.GREEN + Style.BRIGHT + f"🚀 ENTERED {'PAPER' if PAPER_MODE else 'LIVE'} {side} TRADE: {full_sym} (Entry: {opt_buy_price} | Spot: {spot_entry})")
    except Exception as e: print(f"Trade Error: {e}")

async def monitor_loop():
    print(Fore.BLUE + "🔄 Maintainer Thread Active (30s Polling)...")
    last_summary_time = 0
    while True:
        try:
            now_str = datetime.now().strftime("%H:%M")
            stats = get_daily_stats()
            if now_str >= SQUARE_OFF_TIME and active_positions:
                print(Fore.RED + Style.BRIGHT + f"⏰ {SQUARE_OFF_TIME} REACHED! Closing agent trades...")
                for s in list(active_positions.keys()):
                    p = active_positions[s]; sym_full = p['tradingsymbol']; exch, sym_only = sym_full.split(':')
                    if not PAPER_MODE:
                        place_market_order_safe(exch, sym_only, kite.TRANSACTION_TYPE_SELL, p['qty'], p['ltp'])
                    
                    # Audit final PnL before closing
                    final_pnl = round((p['ltp'] - p['entry_price']) * p['qty'], 2)
                    update_trade_csv(sym_only, {"status": "CLOSED", "exit_time": datetime.now().strftime("%H:%M:%S"), "exit_price": p['ltp'], "exit_reason": "SQUARE_OFF", "net_pnl": final_pnl})
                    del active_positions[s]
                save_active_state(); continue

            to_remove = []
            if active_positions:
                opt_symbols = [p['tradingsymbol'] for p in active_positions.values()]
                spot_symbols = [map_index_symbol(p['symbol']) for p in active_positions.values()]
                
                # Fetch full quotes for options (depth/volume) and ltp for spots
                all_opt_quotes = kite.quote(opt_symbols)
                all_spot_quotes = kite.ltp(spot_symbols)

                for spot_sym_key, pos in active_positions.items():
                    opt_sym_full = pos['tradingsymbol']; spot_kite_key = map_index_symbol(pos['symbol'])
                    
                    # EXTRACT REAL BID (Price you can sell at)
                    q = all_opt_quotes.get(opt_sym_full, {})
                    depth = q.get('depth', {})
                    bid_price = depth.get('buy', [{'price': 0}])[0].get('price', 0)
                    curr_bid = bid_price if bid_price > 0 else q.get('last_price', 0)
                    
                    curr_spot = all_spot_quotes.get(spot_kite_key, {}).get("last_price", 0)
                    
                    if curr_bid > 0:
                        ep = pos['entry_price']; pnl = (curr_bid - ep) * pos['qty']
                        pos['ltp'] = curr_bid; pos['net_pnl'] = round(pnl, 2)
                        p_pct = (curr_bid - ep) / ep * 100
                        
                        if p_pct >= T2_HIT_PCT:
                            lsl = round(ep * 1.10, 2)
                            if pos['active_sl'] < lsl: pos['active_sl'] = lsl; print(Fore.MAGENTA + f"🛡️  LOCK: {opt_sym_full} hit {T2_HIT_PCT}%. SL locked at +10% Profit.")
                        elif p_pct >= T1_HIT_PCT:
                            if pos['active_sl'] < ep: pos['active_sl'] = ep; print(Fore.MAGENTA + f"🛡️  COST: {opt_sym_full} hit {T1_HIT_PCT}%. SL moved to Cost.")
                        
                        if curr_spot > 0:
                            pos['spot_ltp'] = curr_spot; pos['spot_chg'] = round(((curr_spot - pos['spot_entry']) / pos['spot_entry'] * 100), 2)
                        
                        update_trade_csv(opt_sym_full.split(':')[-1], {"ltp": curr_bid, "net_pnl": round(pnl, 2), "spot_ltp": curr_spot, "spot_chg": pos['spot_chg']})
                    
                    # TRIGGER EXIT (Evaluated against REAL SELL PRICE)
                    if curr_bid > 0 and (curr_bid <= pos['active_sl'] or curr_bid >= pos['target']):
                        tag = "SL" if curr_bid <= pos['active_sl'] else "TARGET"
                        if tag == "SL" and pos['active_sl'] >= ep: tag = "TSL_LOCKED"
                        exch, sym_only = opt_sym_full.split(':')
                        
                        if not PAPER_MODE:
                            place_market_order_safe(exch, sym_only, kite.TRANSACTION_TYPE_SELL, pos['qty'], curr_bid)
                        
                        final_pnl = round((curr_bid - ep) * pos['qty'], 2)
                        print((Fore.RED if "SL" in tag else Fore.GREEN) + Style.BRIGHT + f"📉 EXIT ({tag}): {opt_sym_full} @ {curr_bid} | Final PnL: ₹{final_pnl:.2f}")
                        update_trade_csv(sym_only, {"status": "CLOSED", "exit_time": datetime.now().strftime("%H:%M:%S"), "exit_price": curr_bid, "exit_reason": tag, "net_pnl": final_pnl})
                        to_remove.append(spot_sym_key)
                if to_remove:
                    for s in to_remove: del active_positions[s]
                    save_active_state()

            if time.time() - last_summary_time > 60:
                stats = get_daily_stats(); unrealized = sum(p.get('net_pnl', 0) for p in active_positions.values())
                print(Fore.CYAN + "\n" + "╔" + "═"*162 + "╗")
                print(Fore.CYAN + "║" + Fore.WHITE + Style.BRIGHT + f" 💼 ACTIVE POSITIONS ({len(active_positions)}/5)".center(162) + Fore.CYAN + "║")
                print(Fore.CYAN + "║" + f" 💰 TOTAL P&L: ₹{stats['pnl']+unrealized:,.2f} (Realized: ₹{stats['pnl']:,.2f} | Open: ₹{unrealized:,.2f} | Trades: {stats['trades']} | W/L: {stats['wins']}/{stats['losses']}) ".center(162) + "║")
                print(Fore.CYAN + "╠" + "═"*26 + "╦" + "═"*7 + "╦" + "═"*7 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*12 + "╦" + "═"*13 + "╦" + "═"*10 + "╣")
                header = f"║ {'Symbol':<24} ║ {'Ent':<5} ║ {'Qty':<5} ║ {'Opt Buy':<8} ║ {'Active SL':<8} ║ {'Opt Cur':<8} ║ {'Pts +/-':<8} ║ {'Spot Ent':<8} ║ {'Spot Cur':<8} ║ {'Opt PnL':<10} ║ {'Progress':<11} ║ {'Chg%':<8} ║"
                print(Fore.CYAN + header); print(Fore.CYAN + "╠" + "═"*26 + "╬" + "═"*7 + "╬" + "═"*7 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*12 + "╬" + "═"*13 + "╬" + "═"*10 + "╣")
                if not active_positions: print(Fore.CYAN + "║" + "NO ACTIVE TRADES".center(162) + "║")
                else:
                    for s, p in active_positions.items():
                        ltp = p.get('ltp', 0); ep = p['entry_price']; tgt = p['target']; prog = (ltp - ep) / (tgt - ep) * 100 if ltp > ep else (ltp - ep) / (ep - p['sl']) * 100
                        sl_t = "COST" if p['active_sl'] == ep else (f"+10%" if p['active_sl'] > ep else f"-{INITIAL_SL_PCT}%")
                        p_col = Fore.GREEN if p['net_pnl'] >= 0 else Fore.RED
                        pts = ltp - ep; pts_col = Fore.GREEN if pts >= 0 else Fore.RED; ent_t = p['timestamp'].split(' ')[1][:5]
                        print(Fore.CYAN + f"║ {p['tradingsymbol'][:24]:<24} " + Fore.CYAN + "║ " + Fore.WHITE + f"{ent_t:<5}" + Fore.CYAN + " ║ " + Fore.WHITE + f"{p['qty']:<5} " + Fore.CYAN + "║ " + Fore.WHITE + f"{ep:<8.1f} " + Fore.CYAN + "║ " + Fore.WHITE + f"{sl_t:<8} " + Fore.CYAN + "║ " + Fore.WHITE + f"{ltp:<8.1f} " + Fore.CYAN + "║ " + pts_col + f"{pts:<+8.1f}" + Fore.CYAN + " ║ " + Fore.WHITE + f"{p['spot_entry']:<8.1f} " + Fore.CYAN + "║ " + Fore.WHITE + f"{p['spot_ltp']:<8.1f} " + Fore.CYAN + "║ " + p_col + f"{'₹'+str(round(p['net_pnl'],1)):<10}" + Fore.CYAN + " ║ " + p_col + f"{round(prog,1):>9}% goal" + Fore.CYAN + " ║ " + p_col + f"{p['spot_chg']:>+8.2f}%" + Fore.CYAN + " ║")
                print(Fore.CYAN + "╚" + "═"*26 + "╩" + "═"*7 + "╩" + "═"*7 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*12 + "╩" + "═"*13 + "╩" + "═"*10 + "╝")
                if stats['closed_trades']:
                    print(Fore.YELLOW + "╔" + "═"*122 + "╗")
                    print(Fore.YELLOW + "║" + Fore.WHITE + Style.BRIGHT + f" 📜 TODAY'S TRADE HISTORY (Last 10 Closed)".center(122) + Fore.YELLOW + "║")
                    print(Fore.YELLOW + "╠" + "═"*26 + "╦" + "═"*7 + "╦" + "═"*7 + "╦" + "═"*7 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*12 + "╦" + "═"*15 + "╣")
                    print(Fore.YELLOW + f"║ {'Symbol':<24} ║ {'Ent':<5} ║ {'Ext':<5} ║ {'Qty':<5} ║ {'Buy':<8} ║ {'Sell':<8} ║ {'Pts +/-':<8} ║ {'PnL':<10} ║ {'Reason':<13} ║")
                    print(Fore.YELLOW + "╠" + "═"*26 + "╬" + "═"*7 + "╬" + "═"*7 + "╬" + "═"*7 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*12 + "╬" + "═"*15 + "╣")
                    for tr in stats['closed_trades']:
                        t_e = str(tr.get('timestamp', '0 00:00')).split(' ')[1][:5]; t_x = str(tr.get('exit_time', '00:00'))[:5]; p_col = Fore.GREEN if tr['net_pnl'] > 0 else Fore.RED
                        pts = float(tr.get('exit_price', 0)) - float(tr['entry_price']); pts_col = Fore.GREEN if pts >= 0 else Fore.RED
                        print(Fore.YELLOW + f"║ {tr['tradingsymbol'][:24]:<24} " + Fore.YELLOW + "║ " + Fore.WHITE + f"{t_e:<5}" + Fore.YELLOW + " ║ " + Fore.WHITE + f"{t_x:<5}" + Fore.YELLOW + " ║ " + Fore.WHITE + f"{tr['qty']:<5}" + Fore.YELLOW + " ║ " + Fore.WHITE + f"{tr['entry_price']:<8.1f} " + Fore.YELLOW + "║ " + Fore.WHITE + f"{tr.get('exit_price',0):<8.1f} " + Fore.YELLOW + "║ " + pts_col + f"{pts:<+8.1f}" + Fore.YELLOW + " ║ " + p_col + f"{'₹'+str(round(tr['net_pnl'],1)):<10}" + Fore.YELLOW + " ║ " + Fore.WHITE + f"{tr.get('exit_reason','N/A'):<13} " + Fore.YELLOW + "║")
                    print(Fore.YELLOW + "╚" + "═"*26 + "╩" + "═"*7 + "╩" + "═"*7 + "╩" + "═"*7 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*12 + "╩" + "═"*15 + "╝\n")
                last_summary_time = time.time()
            await asyncio.sleep(30)
        except Exception as e: print(f"Monitor error: {e}"); await asyncio.sleep(5)

async def main():
    print(Fore.CYAN + Style.BRIGHT + "="*60); print(Fore.CYAN + Style.BRIGHT + "  🤖 TELEGRAM OPTION AGENT (V4.3)"); print(Fore.CYAN + f"  Math Fix: ACTIVE | PnL Audit: ON | SquareOff: {SQUARE_OFF_TIME}"); print(Fore.CYAN + Style.BRIGHT + "="*60)
    load_active_state(); client = TelegramClient('agent_session', API_ID, API_HASH)
    @client.on(events.NewMessage(chats=TARGET_CHANNEL_ID))
    async def listener(event): await handle_new_alert(event)
    await client.start(phone=PHONE); print(Fore.GREEN + "📡 Telegram Listener Active. Listening to alerts..."); asyncio.create_task(monitor_loop()); await client.run_until_disconnected()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("\nAgent stopped.")
