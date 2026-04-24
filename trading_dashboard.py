"""
Trading Dashboard Server
Serves JSON endpoints for the React GUI to poll.
Reads: paper_trades_*.csv, positions_cache_*.csv, execution_log_*.txt
Run alongside the algo on the same machine.
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json, csv, os, glob
from datetime import datetime

PORT = 8765
TODAY = datetime.now().strftime("%d-%m-%Y")

def _find_file(pattern):
    files = glob.glob(pattern)
    return files[0] if files else None

def get_positions():
    f = _find_file(f"positions_cache_{TODAY}.csv")
    if not f or not os.path.exists(f):
        return []
    rows = []
    try:
        with open(f, newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(row)
    except:
        pass
    return rows

def get_trades():
    f = _find_file(f"paper_trades_{TODAY}.csv")
    if not f or not os.path.exists(f):
        return []
    rows = []
    try:
        with open(f, newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(row)
    except:
        pass
    return rows

def get_log_tail(n=80):
    f = _find_file(f"execution_log_{TODAY}.txt")
    if not f or not os.path.exists(f):
        return []
    try:
        with open(f, 'r', encoding='utf-8', errors='replace') as fh:
            lines = fh.readlines()
        return [l.rstrip() for l in lines[-n:]]
    except:
        return []

def build_summary(positions, trades):
    daily_pnl_snapshot = 0.0
    open_pnl = 0.0
    open_count = 0
    closed_count = 0
    wins = 0
    losses = 0

    for p in positions:
        try:
            daily_pnl_snapshot = float(p.get("daily_pnl_snapshot", 0) or 0)
        except:
            pass
        if p.get("status") == "CLOSED":
            closed_count += 1
        elif p.get("status") == "OPEN":
            open_count += 1

    for t in trades:
        try:
            pnl = float(t.get("PnL", 0) or 0)
            if pnl > 0:
                wins += 1
            else:
                losses += 1
        except:
            pass

    total_trades = wins + losses
    win_rate = round(wins / total_trades * 100, 1) if total_trades > 0 else 0

    return {
        "daily_pnl": round(daily_pnl_snapshot, 2),
        "open_count": open_count,
        "closed_count": closed_count,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "total_trades": total_trades,
        "time": datetime.now().strftime("%H:%M:%S"),
    }

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default logging

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET")
        self.end_headers()

    def do_GET(self):
        positions = get_positions()
        trades = get_trades()
        log = get_log_tail()
        summary = build_summary(positions, trades)

        data = {
            "summary": summary,
            "positions": positions,
            "trades": trades,
            "log": log,
        }

        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Dashboard server running on http://0.0.0.0:{PORT}")
    print(f"Reading files for date: {TODAY}")
    server.serve_forever()
