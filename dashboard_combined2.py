"""
dashboard_combined2.py
──────────────────────
Unified dashboard for three strategy engines (Breakout, NIFTY Bias, Heatmap).
Enhanced with high-level summaries and combined history tracking.
Fallback support: Loads today's data from CSV/logs if engines are offline.

ENGINES:
  A: Breakout (kite_breakout_algo42_3.py)        → http://localhost:8765
  B: Multi-Engine (Kite_Multi_algo.py)           → http://localhost:8666
  C: Heatmap (kite_Heatmap_Direction.py)         → http://localhost:8555

RUNNING:
  1. python Kite_Multi_algo.py (Starts A then B on port 8666)
  2. python kite_Heatmap_Direction.py (Port 8555)
  3. python dashboard_combined2.py

Dashboard URL: http://localhost:9000
"""

import csv
import glob
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from urllib.request import urlopen
from urllib.error import URLError

COMBINED_PORT  = 9000
ENGINE_A_URL   = "http://localhost:8765/data"
ENGINE_B_URL   = "http://localhost:8666/data" # Port for Kite_Multi_algo.py
ENGINE_C_URL   = "http://localhost:8555/data"
POLL_INTERVAL  = 60

# ── Live-data cache ──────────────────────────────────────────────────────────
_lock  = threading.Lock()
_cache = {
    "A": None, "B": None, "C": None, 
    "A_err": None, "B_err": None, "C_err": None
}

def _fetch_fallback(key):
    """Attempt to reconstruct today's engine state from local cache/log files."""
    try:
        today = datetime.now().strftime("%d-%m-%Y")
        
        # Filename patterns based on engine key
        if key == "A":
            f_pos    = next((f for f in [f"multi_positions_cache_{today}.csv", f"positions_cache_{today}.csv"] if os.path.exists(f)), f"positions_cache_{today}.csv")
            f_trades = next((f for f in [f"multi_paper_trades_{today}.csv", f"paper_trades_{today}.csv"] if os.path.exists(f)), f"paper_trades_{today}.csv")
            f_log    = next((f for f in [f"multi_execution_log_{today}.txt", f"execution_log_{today}.txt"] if os.path.exists(f)), f"execution_log_{today}.txt")
        elif key == "B":
            f_pos    = next((f for f in [f"multi_heatmap_nifty_positions_cache_{today}.csv", f"multi_positions_cache_{today}.csv", f"nifty_positions_cache_{today}.csv"] if os.path.exists(f)), f"nifty_positions_cache_{today}.csv")
            f_trades = next((f for f in [f"multi_heatmap_nifty_paper_trades_{today}.csv", f"multi_paper_trades_{today}.csv", f"nifty_paper_trades_{today}.csv"] if os.path.exists(f)), f"nifty_paper_trades_{today}.csv")
            f_log    = next((f for f in [f"multi_heatmap_nifty_execution_log_{today}.txt", f"multi_execution_log_{today}.txt", f"nifty_execution_log_{today}.txt"] if os.path.exists(f)), f"nifty_execution_log_{today}.txt")
        elif key == "C":
            f_pos    = f"heatmap_nifty_positions_cache_{today}.csv"
            f_trades = f"heatmap_nifty_paper_trades_{today}.csv"
            f_log    = f"heatmap_nifty_execution_log_{today}.txt"
        else:
            return None

        # 1. Load Positions
        positions = []
        daily_pnl = 0.0
        wins = 0
        losses = 0
        if os.path.exists(f_pos):
            try:
                with open(f_pos, newline="", encoding="utf-8") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        pnl = float(row.get("net_pnl", 0))
                        status = row.get("status", "CLOSED")
                        positions.append({
                            "symbol":     row.get("symbol", ""),
                            "side":       row.get("side", ""),
                            "entry":      row.get("entry", ""),
                            "ltp":        row.get("entry", ""), # Fallback LTP to entry
                            "live_pnl":   pnl,
                            "status":     status,
                            "entry_time": row.get("entry_time", ""),
                            "strategy":   row.get("strategy", ""),
                        })
                        if status == "CLOSED":
                            daily_pnl += pnl
                            if pnl > 0: wins += 1
                            else: losses += 1
            except Exception: pass

        # 2. Load Closed Trades (Recent)
        trades = []
        if os.path.exists(f_trades):
            trades = _read_trades_csv(f_trades)

        # 3. Load Log
        log_lines = []
        if os.path.exists(f_log):
            try:
                with open(f_log, "r", encoding="utf-8") as fh:
                    log_lines = [line.strip() for line in fh.readlines() if line.strip()]
            except Exception: pass

        # 4. Construct Payload
        total = wins + losses
        win_rate = round((wins/total*100),1) if total > 0 else 0
        
        return {
            "summary": {
                "daily_pnl": round(daily_pnl, 2),
                "open_pnl": 0.0,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "open_count": sum(1 for p in positions if p["status"] == "OPEN"),
                "time": datetime.now().strftime("%H:%M:%S"),
                "is_fallback": True
            },
            "positions": positions,
            "trades": trades,
            "log": log_lines[-100:] # Last 100 lines
        }
    except Exception as e:
        print(f"Fallback error for {key}: {e}")
        return None

def _fetch(key, url):
    try:
        with urlopen(url, timeout=5) as r:
            data = json.loads(r.read().decode())
        with _lock:
            _cache[key]          = data
            _cache[f"{key}_err"] = None
    except URLError as e:
        # Engine offline -> Try fallback
        fallback_data = _fetch_fallback(key)
        with _lock:
            if fallback_data:
                _cache[key] = fallback_data
                _cache[f"{key}_err"] = f"Engine offline (Loaded from cache: {e.reason})"
            else:
                _cache[f"{key}_err"] = f"Engine offline ({e.reason})"
    except Exception as e:
        with _lock:
            _cache[f"{key}_err"] = str(e)

def _poll_loop():
    while True:
        # Start fetches in parallel
        threads = [
            threading.Thread(target=_fetch, args=("A", ENGINE_A_URL), daemon=True),
            threading.Thread(target=_fetch, args=("B", ENGINE_B_URL), daemon=True),
            threading.Thread(target=_fetch, args=("C", ENGINE_C_URL), daemon=True)
        ]
        for t in threads: t.start()
        
        # Wait for all to finish or timeout
        for t in threads: t.join(timeout=15)
        
        time.sleep(POLL_INTERVAL)

# ── History helpers ──────────────────────────────────────────────────────────

def _is_float(v):
    try:
        float(v); return True
    except (TypeError, ValueError):
        return False

def _scan_files(glob_pat, date_re):
    results = []
    for path in glob.glob(glob_pat):
        m = date_re.search(os.path.basename(path))
        if m:
            results.append((m.group(1), path))
    results.sort(key=lambda x: datetime.strptime(x[0], "%d-%m-%Y"), reverse=True)
    return results

def _read_trades_csv(path):
    trades = []
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Support multiple naming conventions for headers
                trades.append({
                    "time":     row.get("Time", row.get("ExitTime", row.get("time", ""))),
                    "symbol":   row.get("Symbol", row.get("symbol", "")),
                    "side":     row.get("Side", row.get("side", "")),
                    "entry":    row.get("Entry", row.get("EntryPrice", row.get("entry", ""))),
                    "exit":     row.get("Exit", row.get("ExitPrice", row.get("exit", ""))),
                    "qty":      row.get("Qty", row.get("qty", "")),
                    "pnl":      row.get("PnL", row.get("pnl", "0")),
                    "reason":   row.get("Reason", row.get("ExitReason", row.get("reason", ""))),
                    "strategy": row.get("Strategy", row.get("strategy", "")),
                })
    except Exception:
        pass
    return trades

def _build_history_payload(engine_key, live_data=None):
    if engine_key == "A":
        glob_pat = "*paper_trades_*.csv"
        date_re  = re.compile(r"paper_trades_(\d{2}-\d{2}-\d{4})\.csv")
    elif engine_key == "B":
        glob_pat = "*paper_trades_*.csv"
        date_re  = re.compile(r"paper_trades_(\d{2}-\d{2}-\d{4})\.csv")
    elif engine_key == "C":
        glob_pat = "heatmap_nifty_paper_trades_*.csv"
        date_re  = re.compile(r"heatmap_nifty_paper_trades_(\d{2}-\d{2}-\d{4})\.csv")
    else:
        return {"daily": [], "weekly": [], "monthly": []}

    files = _scan_files(glob_pat, date_re)
    daily   = []
    weekly  = {}
    monthly = {}

    today_str = datetime.now().strftime("%d-%m-%Y")

    for date_str, path in files:
        # Deduplicate by date for engine B if multiple prefixes exist
        if engine_key == "B" and any(d["date"] == date_str for d in daily):
            continue
            
        all_trades = _read_trades_csv(path)
        # Total day PnL (sum of ALL rows in CSV)
        day_pnl = round(sum(float(t["pnl"]) for t in all_trades if _is_float(t["pnl"])), 2)
        
        # Base trades (for counting)
        base_trades = [t for t in all_trades if "__PYR" not in t.get("symbol", "")]
        wins   = sum(1 for t in base_trades if _is_float(t["pnl"]) and float(t["pnl"]) > 0)
        losses = sum(1 for t in base_trades if _is_float(t["pnl"]) and float(t["pnl"]) <= 0)

        daily.append({
            "date":   date_str,
            "pnl":    day_pnl,
            "wins":   wins,
            "losses": losses,
            "trades": all_trades
        })

    # ── Inject Live Data for Today ───────────────────────────────────────────
    if live_data and engine_key in live_data and live_data[engine_key]:
        engine_snap = live_data[engine_key]
        summary = engine_snap.get("summary", {})
        live_pnl = round(float(summary.get("daily_pnl", 0)), 2)
        
        # Find today's entry or create it
        today_entry = next((d for d in daily if d["date"] == today_str), None)
        if not today_entry:
            today_entry = {
                "date":   today_str,
                "pnl":    live_pnl,
                "wins":   int(summary.get("wins", 0)),
                "losses": int(summary.get("losses", 0)),
                "trades": []
            }
            daily.insert(0, today_entry)
        else:
            # Today exists in CSV, but we want the LIVE PnL (which includes OPEN positions)
            today_entry["pnl"]    = live_pnl
            today_entry["wins"]   = int(summary.get("wins", 0))
            today_entry["losses"] = int(summary.get("losses", 0))

        # Add open positions to today's trades list for the detail view
        open_pos = [p for p in engine_snap.get("positions", []) if p.get("status") == "OPEN"]
        for p in open_pos:
            today_entry["trades"].append({
                "time":     p.get("entry_time", ""),
                "symbol":   p.get("symbol", ""),
                "side":     p.get("side", ""),
                "entry":    p.get("entry", ""),
                "exit":     p.get("ltp", ""),
                "qty":      p.get("qty", ""),
                "pnl":      p.get("live_pnl", 0),
                "reason":   "OPEN",
                "strategy": p.get("strategy", ""),
            })

    # ── Aggregate Weekly/Monthly ─────────────────────────────────────────────
    for d in daily:
        day_pnl = d["pnl"]
        date_str = d["date"]
        dt  = datetime.strptime(date_str, "%d-%m-%Y")
        
        mon = dt - timedelta(days=dt.weekday())
        fri = mon + timedelta(days=4)
        wk  = mon.strftime("%d-%m-%Y")
        if wk not in weekly:
            weekly[wk] = {
                "label":  f"{mon.strftime('%d %b')} – {fri.strftime('%d %b %Y')}",
                "pnl": 0.0, "trades": [], "_sort": mon
            }
        weekly[wk]["pnl"]    = round(weekly[wk]["pnl"] + day_pnl, 2)
        weekly[wk]["trades"] += d["trades"]

        mk = dt.strftime("%Y-%m")
        if mk not in monthly:
            monthly[mk] = {
                "label":  dt.strftime("%B %Y"),
                "pnl": 0.0, "trades": [], "_sort": dt.replace(day=1)
            }
        monthly[mk]["pnl"]    = round(monthly[mk]["pnl"] + day_pnl, 2)
        monthly[mk]["trades"] += d["trades"]

    def _strip(lst):
        for d in lst:
            d.pop("_sort", None)
        return lst

    return {
        "daily":   daily,
        "weekly":  _strip(sorted(weekly.values(),  key=lambda x: x["_sort"])),
        "monthly": _strip(sorted(monthly.values(), key=lambda x: x["_sort"])),
    }

# ── Dashboard HTML ───────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ultimate Strategy Dashboard</title>
<style>
  :root{
    --bg:#0b0e14;--card:#151921;--bdr:#262c36;
    --txt:#e6edf3;--muted:#8b949e;
    --grn:#3fb950;--red:#f85149;--blu:#58a6ff;--ylw:#d29922;
    --purp:#7c3aed;--teal:#0d9488;--gold:#f59e0b;
    --tab-a:#7c3aed;--tab-b:#0d9488;--tab-c:#f59e0b;
    --side-a:#1e1535;--side-b:#0d2520;--side-c:#251e0d;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--txt);font:13px/1.5 'Inter','Segoe UI',system-ui,sans-serif; -webkit-font-smoothing: antialiased}

  /* header */
  header{display:flex;align-items:center;justify-content:space-between;
    padding:12px 20px;border-bottom:1px solid var(--bdr);background:var(--card);
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);position:sticky;top:0;z-index:100}
  header h1{font-size:16px;font-weight:700;letter-spacing:-0.2px;display:flex;align-items:center;gap:10px}
  header h1 span{color:var(--blu)}
  #clock{font-family:'Cascadia Code',monospace;font-size:13px;color:var(--muted);background:#000;padding:4px 10px;border-radius:6px;border:1px solid var(--bdr)}
  #mode-badge{font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px;
    background:#1a3a1a;color:var(--grn);border:1px solid #2d5a2d;text-transform:uppercase}

  /* summary bar */
  .global-sum{display:flex;gap:12px;padding:16px 20px;background:linear-gradient(180deg, var(--card) 0%, var(--bg) 100%)}
  .gsum-card{flex:1;background:rgba(255,255,255,0.03);border:1px solid var(--bdr);border-radius:12px;padding:12px 16px;
    transition:transform 0.2s, background 0.2s; cursor:default}
  .gsum-card:hover{background:rgba(255,255,255,0.06);transform:translateY(-2px)}
  .gsum-card .lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px}
  .gsum-card .val{font-size:22px;font-weight:800;letter-spacing:-0.5px}

  /* tab bar */
  .tab-bar{display:flex;gap:4px;padding:4px 20px;border-bottom:1px solid var(--bdr);background:var(--card);overflow-x:auto}
  .tab-btn{padding:10px 20px;font-size:13px;font-weight:600;cursor:pointer;
    border:none;background:transparent;color:var(--muted);border-radius:8px;
    transition:all 0.2s; white-space:nowrap}
  .tab-btn:hover{background:rgba(255,255,255,0.05);color:var(--txt)}
  .tab-btn.active-a{color:#a78bfa;background:rgba(124,58,237,0.1)}
  .tab-btn.active-b{color:#5eead4;background:rgba(13,148,136,0.1)}
  .tab-btn.active-c{color:#fbbf24;background:rgba(245,158,11,0.1)}
  .tab-btn.active-all{color:var(--blu);background:rgba(88,166,255,0.1)}
  .tab-btn.active-h{color:#f472b6;background:rgba(244,114,182,0.1)}

  /* panels */
  .panel{display:none;padding:20px}
  .panel.active{display:block; animation: fadeIn 0.3s ease-out}
  @keyframes fadeIn { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:translateY(0)} }

  /* engine status lights */
  .status-light{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
  .light-on{background:var(--grn);box-shadow:0 0 8px var(--grn)}
  .light-off{background:var(--red);box-shadow:0 0 8px var(--red)}
  .light-wait{background:var(--ylw);box-shadow:0 0 8px var(--ylw)}

  /* components */
  .card{background:var(--card);border:1px solid var(--bdr);border-radius:12px;overflow:hidden;margin-bottom:20px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.2)}
  .card-header{padding:12px 16px;border-bottom:1px solid var(--bdr);display:flex;align-items:center;justify-content:space-between;
    background:rgba(255,255,255,0.02)}
  .card-title{font-size:12px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.8px}
  
  .strip{display:grid;grid-template-columns:repeat(auto-fit, minmax(140px, 1fr));gap:12px;margin-bottom:20px}
  .mini-card{background:var(--card);border:1px solid var(--bdr);border-radius:10px;padding:12px 16px}
  .mini-card .l{font-size:10px;color:var(--muted);text-transform:uppercase;margin-bottom:2px}
  .mini-card .v{font-size:18px;font-weight:700}

  /* tables */
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{padding:10px 16px;text-align:left;color:var(--muted);font-weight:500;background:rgba(0,0,0,0.2);border-bottom:1px solid var(--bdr)}
  td{padding:10px 16px;border-bottom:1px solid rgba(255,255,255,0.03);white-space:nowrap}
  tr:last-child td{border-bottom:none}
  tr:hover td{background:rgba(255,255,255,0.03)}
  .badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:800;text-transform:uppercase}
  .badge-buy{background:rgba(63,185,80,0.15);color:var(--grn)}
  .badge-sell{background:rgba(248,81,73,0.15);color:var(--red)}

  /* side grid */
  .side-grid{display:grid;grid-template-columns:repeat(3, 1fr);gap:16px}
  .side-col{display:flex;flex-direction:column;gap:12px}
  .side-box{background:var(--card);border:1px solid var(--bdr);border-radius:12px;overflow:hidden}
  .side-head{padding:10px 14px;font-weight:800;font-size:12px;display:flex;justify-content:space-between;align-items:center}
  .sh-a{background:var(--side-a);color:#c4b5fd;border-bottom:1px solid #4c1d95}
  .sh-b{background:var(--side-b);color:#99f6e4;border-bottom:1px solid #0f766e}
  .sh-c{background:var(--side-c);color:#fde68a;border-bottom:1px solid #92400e}

  /* log */
  .log-box{background:#07090e;padding:12px;font-family:'Cascadia Code',monospace;font-size:11px;
    height:250px;overflow-y:auto;color:#cbd5e1;border-radius:0 0 12px 12px}
  .log-line{margin-bottom:4px;border-bottom:1px solid rgba(255,255,255,0.02);padding-bottom:2px}

  /* helpers */
  .pos{color:var(--grn)}.neg{color:var(--red)}.neu{color:var(--txt)}.blu{color:var(--blu)}.ylw{color:var(--ylw)}
  .offline-msg{padding:40px;text-align:center;color:var(--muted);background:rgba(248,81,73,0.05);border-radius:12px;border:1px dashed var(--red)}
  
  /* history specific */
  .h-eng-bar{display:flex;gap:6px;margin-bottom:12px}
  .h-eng-btn{padding:8px 16px;border:1px solid var(--bdr);background:var(--card);color:var(--muted);border-radius:8px;cursor:pointer;font-weight:600}
  .h-eng-btn.active{background:rgba(244,114,182,0.1);color:#f472b6;border-color:#f472b6}
  .h-day-row{cursor:pointer; user-select:none}
  .h-day-row:hover{background:rgba(255,255,255,0.05) !important}
  .h-details{background:rgba(0,0,0,0.3);display:none}
  .h-details td{padding:0}
  .h-inner-table{width:100%;font-size:11px;border-top:1px solid var(--bdr)}
  .h-inner-table th{background:rgba(0,0,0,0.4);color:#555;font-size:10px;text-transform:uppercase}
  .h-inner-table tr:last-child td{border-bottom:none}
  .exp-btn{font-size:10px;color:var(--muted);margin-right:8px}

  @media(max-width:1100px){ .side-grid{grid-template-columns:1fr 1fr} }
  @media(max-width:750px){ .side-grid{grid-template-columns:1fr} .global-sum{flex-wrap:wrap} }
</style>
</head>
<body>

<header>
  <h1>⚡ <span>Strategy</span> Control Center</h1>
  <div style="display:flex;align-items:center;gap:15px">
    <div style="text-align:right">
      <div id="mode-badge">PAPER</div>
      <div id="last-update" style="font-size:10px;color:var(--muted);margin-top:4px">Waiting for update...</div>
    </div>
    <span id="clock">--:--:--</span>
  </div>
</header>

<div class="global-sum">
  <div class="gsum-card">
    <div class="lbl">Total Combined PnL</div>
    <div class="val" id="total_pnl">₹ 0.00</div>
  </div>
  <div class="gsum-card">
    <div class="lbl">Total Wins / Losses</div>
    <div class="val neu" id="total_wl"><span class="pos">0</span> / <span class="neg">0</span></div>
  </div>
  <div class="gsum-card">
    <div class="lbl">Aggregate Win Rate</div>
    <div class="val blu" id="total_wr">0%</div>
  </div>
  <div class="gsum-card">
    <div class="lbl">Open Trades</div>
    <div class="val ylw" id="total_open">0</div>
  </div>
</div>

<div class="tab-bar">
  <button class="tab-btn active-a" onclick="showTab('tabA',this,'active-a')"><span id="lightA" class="status-light light-wait"></span>💎 Strategy A</button>
  <button class="tab-btn" onclick="showTab('tabB',this,'active-b')"><span id="lightB" class="status-light light-wait"></span>🎯 Strategy B</button>
  <button class="tab-btn" onclick="showTab('tabC',this,'active-c')"><span id="lightC" class="status-light light-wait"></span>🔥 Strategy C</button>
  <button class="tab-btn" onclick="showTab('tabAll',this,'active-all')">🔳 All Engines</button>
  <button class="tab-btn" id="histBtn" onclick="showTab('tabH',this,'active-h');loadHistory()">📅 History</button>
</div>

<!-- Tab A -->
<div id="tabA" class="panel active">
  <div id="uiA"></div>
</div>

<!-- Tab B -->
<div id="tabB" class="panel">
  <div id="uiB"></div>
</div>

<!-- Tab C -->
<div id="tabC" class="panel">
  <div id="uiC"></div>
</div>

<!-- Tab All (Side by Side) -->
<div id="tabAll" class="panel">
  <div class="side-grid">
    <div class="side-col">
      <div class="side-box">
        <div class="side-head sh-a"><span><span id="lightA_side" class="status-light light-wait"></span>A: BREAKOUT</span><span id="pnlA_side">₹ 0.00</span></div>
        <div id="sideA_mini" style="padding:12px"></div>
      </div>
    </div>
    <div class="side-col">
      <div class="side-box">
        <div class="side-head sh-b"><span><span id="lightB_side" class="status-light light-wait"></span>B: MULTI-ENGINE</span><span id="pnlB_side">₹ 0.00</span></div>
        <div id="sideB_mini" style="padding:12px"></div>
      </div>
    </div>
    <div class="side-col">
      <div class="side-box">
        <div class="side-head sh-c"><span><span id="lightC_side" class="status-light light-wait"></span>C: HEATMAP</span><span id="pnlC_side">₹ 0.00</span></div>
        <div id="sideC_mini" style="padding:12px"></div>
      </div>
    </div>
  </div>
</div>

<!-- History Tab -->
<div id="tabH" class="panel">
  <div class="h-eng-bar">
    <button class="h-eng-btn active" id="hE_A" onclick="setHistEng('A')">Strategy A</button>
    <button class="h-eng-btn" id="hE_B" onclick="setHistEng('B')">Strategy B</button>
    <button class="h-eng-btn" id="hE_C" onclick="setHistEng('C')">Strategy C</button>
    <button class="h-eng-btn" id="hE_Combined" onclick="setHistEng('Combined')">⊕ Combined</button>
  </div>
  <div id="history_content"></div>
</div>

<script>
let activeTab='tabA', activeClass='active-a';
let lastCache = { A:null, B:null, C:null };

function showTab(id,btn,cls){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active-a','active-b','active-c','active-all','active-h'));
  document.getElementById(id).classList.add('active');
  btn.classList.add(cls); activeTab=id; activeClass=cls;
}

setInterval(()=>document.getElementById('clock').textContent=new Date().toLocaleTimeString('en-IN',{hour12:false}),1000);

function fmt(v){
  const n=parseFloat(v); if(isNaN(n)) return '—';
  return `<span class="${n>=0?'pos':'neg'}">₹ ${n>=0?'+':''}${n.toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}</span>`;
}

function updateLight(key, status){
  const l = document.getElementById('light'+key);
  const ls = document.getElementById('light'+key+'_side');
  const cls = status==='on' ? 'light-on' : (status==='off' ? 'light-off' : 'light-wait');
  if(l) { l.className = 'status-light ' + cls; }
  if(ls) { ls.className = 'status-light ' + cls; }
}

async function poll(){
  const updateEl = document.getElementById('last-update');
  try{
    updateEl.style.opacity = '0.5';
    const r=await fetch('/combined_data');
    const d=await r.json();
    
    // Set Last Updated Timestamp
    updateEl.textContent = 'Last Updated: ' + new Date().toLocaleTimeString('en-IN', {hour12:false});
    updateEl.style.color = 'var(--muted)';
    updateEl.style.opacity = '1';

    // Update Global Summary
    let totalPnl = 0, totalW = 0, totalL = 0, totalO = 0;
    ['A','B','C'].forEach(k=>{
      const engine = d[k] || (lastCache[k]);
      updateLight(k, d[k] && !d[k].summary.is_fallback ? 'on' : (d[k+'_err'] ? 'off' : 'wait'));
      if(engine && engine.summary){
        totalPnl += parseFloat(engine.summary.daily_pnl || 0);
        totalW += parseInt(engine.summary.wins || 0);
        totalL += parseInt(engine.summary.losses || 0);
        totalO += parseInt(engine.summary.open_count || 0);
      }
    });
    
    document.getElementById('total_pnl').innerHTML = fmt(totalPnl);
    document.getElementById('total_wl').innerHTML = `<span class="pos">${totalW}</span> / <span class="neg">${totalL}</span>`;
    document.getElementById('total_wr').textContent = (totalW+totalL > 0) ? Math.round(totalW/(totalW+totalL)*100)+'%' : '0%';
    document.getElementById('total_open').textContent = totalO;

    // Render Engines
    renderEngine('A', d.A, d.A_err);
    renderEngine('B', d.B, d.B_err);
    renderEngine('C', d.C, d.C_err);

    // Render Side-by-Side
    renderMini('A', d.A, d.A_err);
    renderMini('B', d.B, d.B_err);
    renderMini('C', d.C, d.C_err);

    if(d.A) lastCache.A = d.A;
    if(d.B) lastCache.B = d.B;
    if(d.C) lastCache.C = d.C;

  }catch(e){
    console.error(e);
    updateEl.textContent = 'Last Sync Failed (Retrying...)';
    updateEl.style.color = 'var(--red)';
    updateEl.style.opacity = '1';
  }
}

function renderEngine(key, data, err){
  const target = document.getElementById('ui'+key);
  if(!data && !lastCache[key] && err) {
    target.innerHTML = `<div class="offline-msg"><h2>Engine ${key} Offline</h2><p>${err}</p></div>`;
    return;
  }
  const display = data || lastCache[key];
  if(!display) return;
  
  const s = display.summary || {};
  let stale = '';
  if(s.is_fallback) {
    stale = `<div style="background:#442;color:#fb8;padding:8px;border-radius:8px;margin-bottom:12px;font-size:11px">⚠️ ENGINE OFFLINE: Loading static data from today's cache files</div>`;
  } else if(!data && err) {
    stale = `<div style="background:#422;color:#f88;padding:8px;border-radius:8px;margin-bottom:12px;font-size:11px">⚠️ DISCONNECTED: Showing last known data (${err})</div>`;
  }
  
  let biasHtml = '';
  if(key === 'B') {
    biasHtml = `<div class="mini-card"><div class="l">Status</div><div class="v blu">MULTI-SUPERVISOR</div></div>`;
  } else if(key === 'C') {
    const b = s.heatmap_bias || '—';
    const c = b.includes('BULL') ? 'pos' : (b.includes('BEAR') ? 'neg' : 'ylw');
    biasHtml = `<div class="mini-card"><div class="l">Heatmap Bias</div><div class="v ${c}">${b} (${s.heatmap_pct || 0}%)</div></div>`;
  }

  let html = stale + `
    <div class="strip">
      <div class="mini-card"><div class="l">Total PnL ${s.is_fallback?'(Closed)':''}</div><div class="v">${fmt(s.daily_pnl)}</div></div>
      ${biasHtml}
      <div class="mini-card"><div class="l">Open PnL</div><div class="v">${fmt(s.open_pnl)}</div></div>
      <div class="mini-card"><div class="l">Wins/Losses</div><div class="v"><span class="pos">${s.wins}</span> / <span class="neg">${s.losses}</span></div></div>
      <div class="mini-card"><div class="l">Win Rate</div><div class="v blu">${s.win_rate}%</div></div>
      <div class="mini-card"><div class="l">Open Trades</div><div class="v ylw">${s.open_count}</div></div>
    </div>
    <div class="card">
      <div class="card-header"><span class="card-title">Open Positions</span></div>
      ${buildPositionsTable(display.positions)}
    </div>
    <div class="card">
      <div class="card-header"><span class="card-title">Recent Trades</span></div>
      ${buildTradesTable(display.trades)}
    </div>
    <div class="card">
      <div class="card-header"><span class="card-title">Execution Log</span></div>
      <div class="log-box">${(display.log || []).slice(-50).reverse().map(l=>`<div class="log-line">${l}</div>`).join('')}</div>
    </div>
  `;
  target.innerHTML = html;
}

function renderMini(key, data, err){
  const pnlEl = document.getElementById(`pnl${key}_side`);
  const miniEl = document.getElementById(`side${key}_mini`);
  const display = data || lastCache[key];
  
  if(!display){
    pnlEl.textContent = 'OFFLINE';
    miniEl.innerHTML = `<div style="color:var(--muted);font-size:11px">Waiting for engine...</div>`;
    return;
  }
  
  const s = display.summary || {};
  pnlEl.innerHTML = fmt(s.daily_pnl);
  
  let biasMini = '';
  if(key === 'B') {
    biasMini = `<div style="font-size:10px;color:var(--muted)">Mode: <span class="blu">SUPERVISOR</span></div>`;
  } else if(key === 'C') {
    const b = s.heatmap_bias || '—';
    const c = b.includes('BULL') ? 'pos' : (b.includes('BEAR') ? 'neg' : 'ylw');
    biasMini = `<div style="font-size:10px;color:var(--muted)">Heatmap: <span class="${c}">${s.heatmap_pct||0}%</span></div>`;
  }

  const open = (display.positions || []).filter(p=>p.status==='OPEN');
  let openRows = open.slice(0, 5).map(p=>`
    <div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.02)">
      <span style="font-weight:600">${p.symbol}</span>
      <span class="${p.live_pnl>=0?'pos':'neg'}">${parseFloat(p.live_pnl||0).toFixed(0)}</span>
    </div>
  `).join('');
  
  miniEl.innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px">
      <div style="font-size:10px;color:var(--muted)">W/L: <span class="neu">${s.wins}/${s.losses}</span></div>
      <div style="font-size:10px;color:var(--muted);text-align:right">Open: <span class="ylw">${s.open_count}</span></div>
      ${biasMini}
    </div>
    ${openRows || '<div style="color:var(--muted);font-size:11px;padding:10px 0;text-align:center">No active positions</div>'}
    <div style="margin-top:10px;font-size:9px;color:var(--muted);text-align:right">Last update: ${s.time || '--:--'} ${s.is_fallback?'(OFF)':''}</div>
  `;
}

function buildPositionsTable(pos){
  const open = (pos||[]).filter(p=>p.status==='OPEN');
  if(!open.length) return '<div style="padding:20px;text-align:center;color:var(--muted)">No open positions</div>';
  return `<table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>LTP</th><th>PnL</th><th>Time</th></tr></thead>
    <tbody>${open.map(p=>`<tr>
      <td>${p.symbol}</td>
      <td><span class="badge badge-${(p.side||'').toLowerCase()}">${p.side}</span></td>
      <td>${p.entry}</td><td>${p.ltp}</td>
      <td class="${p.live_pnl>=0?'pos':'neg'}">${parseFloat(p.live_pnl||0).toFixed(2)}</td>
      <td>${p.entry_time}</td>
    </tr>`).join('')}</tbody></table>`;
}

function buildTradesTable(trades){
  const closed = (trades||[]).slice(-50).reverse(); // Increased to 50
  if(!closed.length) return '<div style="padding:20px;text-align:center;color:var(--muted)">No closed trades yet</div>';
  return `<table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Reason</th></tr></thead>
    <tbody>${closed.map(t=>`<tr>
      <td>${t.symbol}</td>
      <td><span class="badge badge-${(t.side||'').toLowerCase()}">${t.side}</span></td>
      <td>${t.entry}</td><td>${t.exit}</td>
      <td class="${parseFloat(t.pnl||0)>=0?'pos':'neg'}">${parseFloat(t.pnl||0).toFixed(2)}</td>
      <td style="font-size:11px;color:var(--muted)">${t.reason}</td>
    </tr>`).join('')}</tbody></table>`;
}

// ── HISTORY ──
let hData = null, hEng = 'A';

async function loadHistory(){
  const target = document.getElementById('history_content');
  target.innerHTML = `<div style="padding:40px;text-align:center;color:var(--muted)">⏳ Loading trade history...</div>`;
  try {
    const r = await fetch('/history_data');
    hData = await r.json();
    renderHistory();
  } catch(e) { target.innerHTML = `<div class="offline-msg">Error loading history: ${e}</div>`; }
}

function setHistEng(e){
  hEng = e;
  document.querySelectorAll('.h-eng-btn').forEach(b=>b.classList.toggle('active', b.id==='hE_'+e));
  renderHistory();
}

function renderHistory(){
  if(!hData) return;
  const target = document.getElementById('history_content');
  let data = [];
  
  if(hEng === 'Combined'){
    // Merge A, B, C by date
    let merged = {};
    ['A','B','C'].forEach(k=>{
      (hData[k].daily || []).forEach(d=>{
        if(!merged[d.date]) merged[d.date] = { date:d.date, pnl:0, wins:0, losses:0, trades:[] };
        merged[d.date].pnl += d.pnl;
        merged[d.date].wins += (d.wins || 0);
        merged[d.date].losses += (d.losses || 0);
        merged[d.date].trades = merged[d.date].trades.concat(d.trades.map(t=>({...t, eng:k})));
      });
    });
    // Sort descending (newest first)
    data = Object.values(merged).sort((a,b)=>datetime_sort(b.date, a.date));
  } else {
    // Single engine data is already sorted descending from server
    data = hData[hEng].daily || [];
  }

  if(!data.length){
    target.innerHTML = `<div style="padding:40px;text-align:center;color:var(--muted)">No history records found for this strategy.</div>`;
    return;
  }

  let html = `<div class="card"><div class="card-header"><span class="card-title">${hEng==='Combined'?'Combined':hEng} Performance History</span></div>`;
  html += `<table><thead><tr><th>Date</th><th>Trades</th><th>W / L</th><th>Win Rate</th><th>Net PnL</th></tr></thead><tbody>`;
  
  data.forEach((d, idx)=>{
    const total = d.wins + d.losses;
    const wr = total > 0 ? Math.round(d.wins/total*100) : 0;
    const rowId = `hrow_${idx}`;
    
    html += `<tr class="h-day-row" onclick="toggleHistory('${rowId}')">
      <td style="font-weight:600"><span class="exp-btn" id="btn_${rowId}">▶</span>${d.date}</td>
      <td>${d.trades.length}</td>
      <td><span class="pos">${d.wins}</span> / <span class="neg">${d.losses}</span></td>
      <td class="blu">${wr}%</td>
      <td class="${d.pnl>=0?'pos':'neg'}">₹ ${d.pnl.toFixed(2)}</td>
    </tr>
    <tr class="h-details" id="${rowId}"><td colspan="5">
      <table class="h-inner-table">
        <thead><tr><th>Time</th>${hEng==='Combined'?'<th>Eng</th>':''}<th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Reason</th></tr></thead>
        <tbody>
          ${d.trades.map(t=>{
            const is_open = t.reason === 'OPEN';
            const pnl_val = parseFloat(t.pnl||0);
            return `<tr>
              <td style="color:#666">${(t.time||'').split(' ')[1] || (t.time||'').slice(0,8)}</td>
              ${hEng==='Combined'?`<td><span style="font-size:9px;opacity:0.7">${t.eng}</span></td>`:''}
              <td class="sym">${t.symbol}</td>
              <td><span class="badge badge-${(t.side||'').toLowerCase()}">${t.side}</span></td>
              <td>${parseFloat(t.entry||0).toFixed(2)}</td>
              <td>${t.exit ? parseFloat(t.exit).toFixed(2) : '—'}</td>
              <td class="${pnl_val>=0?'pos':'neg'}">${pnl_val.toFixed(2)}</td>
              <td>${is_open ? '<span class="badge" style="background:rgba(210,153,34,0.15);color:var(--ylw)">OPEN</span>' : 
                   `<span style="font-size:10px;color:#777;white-space:normal">${t.reason||t.strategy||'—'}</span>`}</td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>
    </td></tr>`;
  });
  
  html += `</tbody></table></div>`;
  target.innerHTML = html;
}

function toggleHistory(id){
  const el = document.getElementById(id);
  const btn = document.getElementById('btn_'+id);
  if(el.style.display === 'table-row'){
    el.style.display = 'none';
    btn.textContent = '▶';
  } else {
    el.style.display = 'table-row';
    btn.textContent = '▼';
  }
}

function datetime_sort(a,b){
  const [da,ma,ya] = a.split('-');
  const [db,mb,yb] = b.split('-');
  return new Date(ya,ma-1,da) - new Date(yb,mb-1,db);
}

poll();
setInterval(poll, 60000); // Refresh every 60 seconds
</script>
</body>
</html>"""


# ── HTTP server ──────────────────────────────────────────────────────────────
class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path.startswith('/combined_data'):
            with _lock:
                payload = json.dumps({
                    "A":     _cache["A"],
                    "B":     _cache["B"],
                    "C":     _cache["C"],
                    "A_err": _cache["A_err"],
                    "B_err": _cache["B_err"],
                    "C_err": _cache["C_err"],
                }).encode()
            ct = "application/json"

        elif parsed.path.startswith('/history_data'):
            with _lock:
                live_snap = {k: v for k, v in _cache.items() if k in ("A", "B", "C")}
            hist_a = _build_history_payload("A", live_snap)
            hist_b = _build_history_payload("B", live_snap)
            hist_c = _build_history_payload("C", live_snap)
            payload = json.dumps({"A": hist_a, "B": hist_b, "C": hist_c}, default=str).encode()
            ct = "application/json"

        else:
            payload = DASHBOARD_HTML.encode("utf-8")
            ct = "text/html; charset=utf-8"

        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    print("=" * 60)
    print("  Ultimate Strategy Dashboard (A + B + C)")
    print(f"  A (Breakout)  -> {ENGINE_A_URL}")
    print(f"  B (Multi)     -> {ENGINE_B_URL}")
    print(f"  C (Heatmap)   -> {ENGINE_C_URL}")
    print(f"  Dashboard     -> http://localhost:{COMBINED_PORT}")
    print("=" * 60)

    threading.Thread(target=_poll_loop, daemon=True).start()

    server = HTTPServer(("0.0.0.0", COMBINED_PORT), _Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Shutting down dashboard...")
        server.shutdown()
        print("  Stopped.")
