import subprocess
import time
import os
import sys
import pandas as pd
import re
import signal
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# --- CONFIGURATION ---
STRATEGY_A_FILE = "kite_breakout_algo42_3.py"
STRATEGY_B_FILE = "kite_Heatmap_Direction.py"

A_HARD_LIMIT = -1000
B_HARD_LIMIT = -800
DASHBOARD_PORT = 8666
SQUARE_OFF_TIME = "15:15"

# Filenames for PnL monitoring
today_str = datetime.now().strftime("%d-%m-%Y")
A_CACHE_FILE = f"multi_positions_cache_{today_str}.csv"
B_CACHE_FILE = f"multi_heatmap_nifty_positions_cache_{today_str}.csv"

def ts():
    return datetime.now().strftime("  |  %Y-%m-%d  %H:%M:%S")

def get_pnl_from_csv(cache_file):
    """Read the latest daily_pnl_snapshot from the given CSV cache file."""
    if not os.path.exists(cache_file):
        return 0
    try:
        df = pd.read_csv(cache_file)
        if df.empty: return 0
        
        if "daily_pnl_snapshot" in df.columns:
            val = df["daily_pnl_snapshot"].iloc[-1]
            if pd.notna(val): return float(val)
            
        if "status" in df.columns and "net_pnl" in df.columns:
            return float(df[df["status"] == "CLOSED"]["net_pnl"].sum())
    except Exception:
        pass
    return 0

def patch_script(file_path, limit, port, instance_id):
    """
    Creates a temporary version of the strategy script with modified 
    hard limits, ports, and cache files to avoid conflicts.
    """
    if not os.path.exists(file_path):
        print(Fore.RED + f"❌ Error: {file_path} not found!")
        return None

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Modify HARD_LOSS_LIMIT
    content = re.sub(r"(HARD_LOSS_LIMIT\s*=\s*)-?\d+(\.\d+)?", rf"\g<1>{limit}", content)

    # 2. Modify DASHBOARD_PORT
    content = re.sub(r"(DASHBOARD_PORT\s*=\s*)\d+", rf"\g<1>{port}", content)

    # 3. Modify SQUARE_OFF_TIME
    content = re.sub(r'(SQUARE_OFF_TIME\s*=\s*)"[^"]+"', rf'\g<1>"{SQUARE_OFF_TIME}"', content)

    # 4. Modify Cache Files to avoid conflict with standalone runs
    # Strategy A
    content = content.replace('POSITIONS_CACHE_FILE = f"positions_cache_', 'POSITIONS_CACHE_FILE = f"multi_positions_cache_')
    content = content.replace('paper_trade_log_file = f"paper_trades_', 'paper_trade_log_file = f"multi_paper_trades_')
    content = content.replace('LOG_TXT_FILE = f"execution_log_', 'LOG_TXT_FILE = f"multi_execution_log_')
    content = content.replace('TRADES_CACHE_FILE   = f"trades_cache_', 'TRADES_CACHE_FILE   = f"multi_trades_cache_')
    
    # Strategy B
    content = content.replace('POSITIONS_CACHE_FILE = f"heatmap_nifty_positions_cache_', 'POSITIONS_CACHE_FILE = f"multi_heatmap_nifty_positions_cache_')
    content = content.replace('paper_trade_log_file = f"heatmap_nifty_paper_trades_', 'paper_trade_log_file = f"multi_heatmap_nifty_paper_trades_')
    content = content.replace('LOG_TXT_FILE = f"heatmap_nifty_execution_log_', 'LOG_TXT_FILE = f"multi_heatmap_nifty_execution_log_')
    content = content.replace('TRADES_CACHE_FILE   = f"heatmap_nifty_trades_cache_', 'TRADES_CACHE_FILE   = f"multi_heatmap_nifty_trades_cache_')

    temp_name = f"multi_run_{instance_id}.py"
    with open(temp_name, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return temp_name

def run_multi_algo():
    print(Fore.CYAN + Style.BRIGHT + "="*75)
    print(Fore.CYAN + Style.BRIGHT + "  🚀 KITE MULTI-STRATEGY SUPERVISOR")
    print(Fore.CYAN + f"  🔹 Phase 1: {STRATEGY_A_FILE} (Limit: {A_HARD_LIMIT})")
    print(Fore.CYAN + f"  🔹 Phase 2: {STRATEGY_B_FILE} (Limit: {B_HARD_LIMIT})")
    print(Fore.CYAN + f"  📊 Dashboard Port: {DASHBOARD_PORT} | SquareOff: {SQUARE_OFF_TIME}")
    print(Fore.CYAN + Style.BRIGHT + "="*75 + ts())

    # --- PHASE 1: Strategy A ---
    temp_a = patch_script(STRATEGY_A_FILE, A_HARD_LIMIT, DASHBOARD_PORT, "A")
    if not temp_a: return

    print(Fore.GREEN + f"▶️  Starting Phase 1: {STRATEGY_A_FILE}..." + ts())
    proc_a = subprocess.Popen([sys.executable, temp_a], env=os.environ)

    a_hit_limit = False
    try:
        while True:
            if proc_a.poll() is not None:
                exit_code = proc_a.poll()
                print(Fore.YELLOW + f"ℹ️  Strategy A exited (Code: {exit_code})." + ts())
                pnl = get_pnl_from_csv(A_CACHE_FILE)
                if pnl <= A_HARD_LIMIT:
                    a_hit_limit = True
                break

            pnl = get_pnl_from_csv(A_CACHE_FILE)
            if pnl <= A_HARD_LIMIT:
                print(Fore.RED + Style.BRIGHT + f"🛑 Strategy A hit Hard Loss Limit: ₹{pnl:.2f}")
                print(Fore.YELLOW + "   Terminating Strategy A and moving to Phase 2...")
                proc_a.terminate()
                try: proc_a.wait(timeout=15)
                except: proc_a.kill()
                a_hit_limit = True
                break
            
            time.sleep(10)
    except KeyboardInterrupt:
        print(Fore.RED + "⏹  User interrupted. Stopping Strategy A...")
        proc_a.terminate()
        return
    finally:
        if os.path.exists(temp_a): 
            try: os.remove(temp_a)
            except: pass

    # Only move to Phase 2 if Phase 1 hit the limit
    if not a_hit_limit:
        print(Fore.YELLOW + "✅ Strategy A ended without hitting hard limit. No need for Phase 2.")
        return

    # --- PHASE 2: Strategy B ---
    print("\n" + Fore.MAGENTA + Style.BRIGHT + "="*75)
    print(Fore.MAGENTA + Style.BRIGHT + f"🚀 PHASE 2: Starting {STRATEGY_B_FILE}")
    print(Fore.MAGENTA + f"  🔹 Hard Limit: {B_HARD_LIMIT} strictly")
    print(Fore.MAGENTA + Style.BRIGHT + "="*75 + ts())

    temp_b = patch_script(STRATEGY_B_FILE, B_HARD_LIMIT, DASHBOARD_PORT, "B")
    if not temp_b: return

    proc_b = subprocess.Popen([sys.executable, temp_b], env=os.environ)

    try:
        while True:
            if proc_b.poll() is not None:
                exit_code = proc_b.poll()
                print(Fore.YELLOW + f"ℹ️  Strategy B exited (Code: {exit_code})." + ts())
                break

            pnl_b = get_pnl_from_csv(B_CACHE_FILE)
            if pnl_b <= B_HARD_LIMIT:
                print(Fore.RED + Style.BRIGHT + f"🚨 Strategy B hit STRICT Hard Loss Limit: ₹{pnl_b:.2f}")
                print(Fore.RED + "   Trading HALTED for the day.")
                proc_b.terminate()
                try: proc_b.wait(timeout=15)
                except: proc_b.kill()
                break
            
            time.sleep(10)
    except KeyboardInterrupt:
        print(Fore.RED + "⏹  User interrupted. Stopping Strategy B...")
        proc_b.terminate()
    finally:
        if os.path.exists(temp_b):
            try: os.remove(temp_b)
            except: pass

if __name__ == "__main__":
    run_multi_algo()
