import subprocess
import time
import sys
import signal
import os
import threading
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# Configuration
SCRIPTS = {
    "A": {"name": "Strategy A (Breakout)", "file": "kite_breakout_algo42_3.py", "color": Fore.CYAN},
    "B": {"name": "Strategy B (NIFTY)",   "file": "kite_breakout_algo42_NIFTY3.py", "color": Fore.GREEN},
    "C": {"name": "Strategy C (Heatmap)",  "file": "kite_Heatmap_Direction.py", "color": Fore.YELLOW},
    "D": {"name": "Combined Dashboard",    "file": "dashboard_combined2.py", "color": Fore.MAGENTA}
}

# Flag files directory
FLAGS_DIR = ".run_flags"
if not os.path.exists(FLAGS_DIR):
    os.makedirs(FLAGS_DIR)

def get_flag_path(key):
    return os.path.join(FLAGS_DIR, f"{key}.flag")

def set_flag(key, state):
    with open(get_flag_path(key), "w") as f:
        f.write(state)

def get_flag(key):
    path = get_flag_path(key)
    if not os.path.exists(path):
        return "stop" # Default to stop if no flag exists
    with open(path, "r") as f:
        return f.read().strip().lower()

# --- Supervisor Logic ---

running_processes = {}
shutdown_event = threading.Event()
# Global lock to ensure only one script performs daily download at a time
download_lock = threading.Lock()
download_completed = False

def log_with_prefix(name, color, line):
    timestamp = time.strftime("%H:%M:%S")
    try:
        print(f"{color}[{timestamp}] [{name}]{Style.RESET_ALL} {line.strip()}")
    except UnicodeEncodeError:
        try:
            print(f"{color}[{timestamp}] [{name}]{Style.RESET_ALL} {line.strip().encode('ascii', 'replace').decode()}")
        except:
            pass

def monitor_output(name, color, process, key):
    global download_completed
    try:
        for line in iter(process.stdout.readline, ""):
            if line:
                log_with_prefix(name, color, line)
                # If this is the "Master" downloader and it finishes its job
                if key == "A" and "Warmup complete" in line:
                    log_with_prefix(name, Fore.GREEN, "✅ MASTER INITIALIZATION COMPLETE. Unlocking other strategies...")
                    download_completed = True
            if shutdown_event.is_set():
                break
    except Exception:
        pass

def supervisor_loop(key):
    global download_completed
    config = SCRIPTS[key]
    name = config["name"]
    script_file = config["file"]
    color = config["color"]

    while not shutdown_event.is_set():
        desired_state = get_flag(key)

        if desired_state == "stop":
            if key in running_processes:
                log_with_prefix(name, Fore.RED, "🛑 Stop command received. Terminating process...")
                running_processes[key].terminate()
                del running_processes[key]
            time.sleep(2)
            continue

        if desired_state == "start":
            if key not in running_processes:
                
                # Logic: D (Dashboard) starts anytime.
                # A (Master) starts first.
                # B and C MUST wait until A says "Download Complete".
                if key in ["B", "C"]:
                    if not download_completed:
                        # Periodically check if another script already created the files today
                        # to avoid infinite waiting if A was restarted or didn't run.
                        log_with_prefix(name, Fore.WHITE, f"⏳ Waiting for Master (Strategy A) to finish daily downloads...")
                        time.sleep(10)
                        continue

                if not os.path.exists(script_file):
                    log_with_prefix(name, Fore.RED, f"❌ Error: {script_file} not found!")
                    set_flag(key, "stop")
                    continue

                log_with_prefix(name, color, f"🚀 Starting {script_file}...")
                
                env = os.environ.copy()
                env["PYTHONUTF8"] = "1"
                
                try:
                    process = subprocess.Popen(
                        [sys.executable, script_file],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        encoding="utf-8",
                        errors="replace",
                        env=env
                    )
                    running_processes[key] = process
                    threading.Thread(target=monitor_output, args=(name, color, process, key), daemon=True).start()
                except Exception as e:
                    log_with_prefix(name, Fore.RED, f"❌ Failed to start: {e}")
                    time.sleep(5)
                    continue
            
            if running_processes[key].poll() is not None:
                exit_code = running_processes[key].poll()
                log_with_prefix(name, Fore.RED, f"⚠️  Process exited (Code: {exit_code}). Restarting in 10s...")
                del running_processes[key]
                time.sleep(10)
                continue

        time.sleep(2)

def run_supervisor():
    print("=" * 75)
    print(f"{Fore.CYAN}{Style.BRIGHT}  🚀  TRADING SUITE MASTER SUPERVISOR")
    print(f"  📂  PID: {os.getpid()} | Order: Dashboard -> Master (A) -> Slaves (B,C)")
    print(f"  🛑  Press Ctrl+C to close this monitor and stop ALL")
    print("=" * 75)
    
    # Start order: Dashboard and Master start immediately.
    # B and C will wait for 'download_completed' flag.
    keys = ["D", "A", "B", "C"]
    for key in keys:
        t = threading.Thread(target=supervisor_loop, args=(key,), daemon=True)
        t.start()
        time.sleep(1) # Small gap between thread creations

    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown_event.set()
        print("\n🛑 Shutting down supervisor and all scripts...")
        for k, p in list(running_processes.items()):
            try:
                p.terminate()
            except:
                pass
        print("✅ Clean shutdown complete.")

if __name__ == "__main__":
    args = [a.lower() for a in sys.argv[1:]]
    
    if len(args) < 2:
        print(f"{Fore.YELLOW}Usage Examples:")
        print("  python run_all.py start all")
        print("  python run_all.py stop all")
        sys.exit(0)

    command = args[0]
    target = args[1].upper()

    if command not in ["start", "stop"]:
        sys.exit(1)

    if target == "ALL":
        for k in SCRIPTS:
            set_flag(k, command)
    elif target in SCRIPTS:
        set_flag(target, command)

    if command == "start":
        lock_file = os.path.join(FLAGS_DIR, "supervisor.lock")
        is_running = False
        if os.path.exists(lock_file):
            try:
                with open(lock_file, "r") as f:
                    old_pid = int(f.read())
                os.kill(old_pid, 0)
                is_running = True
            except (ValueError, OSError):
                pass
        
        if not is_running:
            with open(lock_file, "w") as f:
                f.write(str(os.getpid()))
            try:
                run_supervisor()
            finally:
                if os.path.exists(lock_file):
                    os.remove(lock_file)
        else:
            print(f"{Fore.BLUE}ℹ️  Supervisor already running. Command sent to the active terminal.")
