"""
dashboard_combined.py
─────────────────────
Unified dashboard for running both strategy engines side-by-side.

SETUP (one-time):
  1. In kite_breakout_algo42_NIFTY.py change:
       DASHBOARD_PORT = 8765  →  DASHBOARD_PORT = 8766
     Also add "nifty_" prefix to these file names to avoid collision:
       TRADES_CACHE_FILE    = f"nifty_trades_cache_{today_str}.csv"
       POSITIONS_CACHE_FILE = f"nifty_positions_cache_{today_str}.csv"
       paper_trade_log_file = f"nifty_paper_trades_{today_str}.csv"
       LOG_TXT_FILE         = f"nifty_execution_log_{today_str}.txt"

RUNNING:
  Terminal 1:  python kite_breakout_algo42_2.py
  Terminal 2:  python kite_breakout_algo42_NIFTY.py
  Terminal 3:  python dashboard_combined.py

  Open browser:  http://localhost:9000
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import urlopen
from urllib.error import URLError

COMBINED_PORT  = 9000
ENGINE_A_URL   = "http://localhost:8765/data"   # kite_breakout_algo42_2.py
ENGINE_B_URL   = "http://localhost:8766/data"   # kite_breakout_algo42_NIFTY.py
POLL_INTERVAL  = 3   # seconds between fetches

# ── Cached data from each engine ────────────────────────────────────────────
_lock   = threading.Lock()
_cache  = {"A": None, "B": None, "A_err": None, "B_err": None}


def _fetch(key, url):
    try:
        with urlopen(url, timeout=4) as r:
            data = json.loads(r.read().decode())
        with _lock:
            _cache[key]           = data
            _cache[f"{key}_err"]  = None
    except URLError as e:
        with _lock:
            _cache[f"{key}_err"] = f"Engine offline or not yet started ({e.reason})"
    except Exception as e:
        with _lock:
            _cache[f"{key}_err"] = str(e)


def _poll_loop():
    while True:
        t1 = threading.Thread(target=_fetch, args=("A", ENGINE_A_URL), daemon=True)
        t2 = threading.Thread(target=_fetch, args=("B", ENGINE_B_URL), daemon=True)
        t1.start(); t2.start()
        t1.join();  t2.join()
        time.sleep(POLL_INTERVAL)


# ── HTML page served at / ────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Combined Strategy Dashboard</title>
<style>
  :root {
    --bg:   #0e1117;
    --card: #161b22;
    --bdr:  #30363d;
    --txt:  #e6edf3;
    --muted:#8b949e;
    --grn:  #3fb950;
    --red:  #f85149;
    --blu:  #58a6ff;
    --ylw:  #d29922;
    --tab-a:#7c3aed;
    --tab-b:#0d9488;
    --tab-bg-a:#1e1535;
    --tab-bg-b:#0d2520;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--txt); font: 13px/1.5 'Segoe UI', system-ui, sans-serif; }

  /* ── Header ── */
  header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 16px; border-bottom: 1px solid var(--bdr);
    background: var(--card);
  }
  header h1 { font-size: 15px; font-weight: 600; letter-spacing: .3px; }
  #clock { font-size: 12px; color: var(--muted); }
  #mode-badge {
    font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 4px;
    background: #1a3a1a; color: var(--grn); border: 1px solid #2d5a2d;
  }

  /* ── Tab bar ── */
  .tab-bar {
    display: flex; gap: 0; border-bottom: 1px solid var(--bdr);
    background: var(--card);
  }
  .tab-btn {
    padding: 9px 22px; font-size: 13px; font-weight: 500; cursor: pointer;
    border: none; background: transparent; color: var(--muted);
    border-bottom: 2px solid transparent; transition: color .15s, border-color .15s;
  }
  .tab-btn.active-a { color: #a78bfa; border-bottom-color: var(--tab-a); }
  .tab-btn.active-b { color: #5eead4; border-bottom-color: var(--tab-b); }
  .tab-btn.active-c { color: var(--txt); border-bottom-color: var(--blu); }

  /* ── Panels ── */
  .panel { display: none; padding: 14px 16px; }
  .panel.active { display: block; }

  /* ── Summary strip ── */
  .summary-strip {
    display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 14px;
  }
  .sum-card {
    flex: 1 1 120px; background: var(--card); border: 1px solid var(--bdr);
    border-radius: 8px; padding: 10px 14px;
  }
  .sum-card .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; }
  .sum-card .value { font-size: 20px; font-weight: 600; margin-top: 2px; }
  .pos  { color: var(--grn); }
  .neg  { color: var(--red); }
  .neu  { color: var(--txt); }
  .info { color: var(--blu); }

  /* ── Table ── */
  .tbl-wrap { background: var(--card); border: 1px solid var(--bdr); border-radius: 8px; overflow-x: auto; margin-bottom: 14px; }
  .tbl-title { padding: 9px 14px; font-size: 12px; font-weight: 600; color: var(--muted);
               text-transform: uppercase; letter-spacing: .4px; border-bottom: 1px solid var(--bdr); }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { padding: 7px 10px; text-align: left; color: var(--muted); font-weight: 500;
       border-bottom: 1px solid var(--bdr); white-space: nowrap; }
  td { padding: 6px 10px; border-bottom: 1px solid #1c2026; white-space: nowrap; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #1c2430; }
  .badge {
    display: inline-block; padding: 1px 7px; border-radius: 4px;
    font-size: 11px; font-weight: 600;
  }
  .badge-buy  { background: #0e2a1a; color: var(--grn); }
  .badge-sell { background: #2a0e0e; color: var(--red); }
  .badge-open { background: #0e1f2a; color: var(--blu); }
  .badge-closed { background: #1a1a1a; color: var(--muted); }

  /* ── Side-by-side view ── */
  .side-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  .side-panel { min-width: 0; }
  .side-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 8px 12px; border-radius: 8px 8px 0 0;
    border: 1px solid var(--bdr); border-bottom: none;
    font-size: 13px; font-weight: 600;
  }
  .side-a { background: var(--tab-bg-a); border-color: #4c1d95; color: #c4b5fd; }
  .side-b { background: var(--tab-bg-b); border-color: #0f766e; color: #99f6e4; }

  /* ── Log box ── */
  .log-box {
    background: #090d13; border: 1px solid var(--bdr); border-radius: 8px;
    padding: 10px 12px; font-family: 'Cascadia Code','Consolas',monospace;
    font-size: 11px; line-height: 1.6; color: #8b949e;
    max-height: 200px; overflow-y: auto;
  }
  .log-box .log-line { white-space: pre-wrap; word-break: break-all; }

  /* ── Error state ── */
  .offline-card {
    background: #1a0e0e; border: 1px solid #5a2020; border-radius: 8px;
    padding: 24px; text-align: center; color: var(--red); font-size: 13px;
  }
  .offline-card .icon { font-size: 28px; margin-bottom: 8px; }
  .offline-card .hint { color: var(--muted); font-size: 11px; margin-top: 6px; }

  /* ── Responsive ── */
  @media (max-width: 900px) { .side-grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>

<header>
  <h1>⚡ Combined Strategy Dashboard</h1>
  <div style="display:flex;align-items:center;gap:12px">
    <span id="mode-badge">PAPER</span>
    <span id="clock">--:--:--</span>
  </div>
</header>

<div class="tab-bar">
  <button class="tab-btn active-a" onclick="showTab('tabA',this,'active-a')">
    Strategy A — Breakout
  </button>
  <button class="tab-btn" onclick="showTab('tabB',this,'active-b')">
    Strategy B — NIFTY Bias
  </button>
  <button class="tab-btn" onclick="showTab('tabC',this,'active-c')">
    ◈ Side by Side
  </button>
</div>

<!-- ── Tab A ── -->
<div id="tabA" class="panel active">
  <div id="summaryA"></div>
  <div id="positionsA"></div>
  <div id="tradesA"></div>
  <div id="logA"></div>
</div>

<!-- ── Tab B ── -->
<div id="tabB" class="panel">
  <div id="summaryB"></div>
  <div id="positionsB"></div>
  <div id="tradesB"></div>
  <div id="logB"></div>
</div>

<!-- ── Tab C: Side by side ── -->
<div id="tabC" class="panel">
  <div class="side-grid">
    <div class="side-panel">
      <div class="side-header side-a">
        <span>Strategy A — Breakout</span>
        <span id="pnlA_side" style="font-size:15px">₹ —</span>
      </div>
      <div id="sideA_content" style="border:1px solid #4c1d95;border-top:none;border-radius:0 0 8px 8px;padding:10px;"></div>
    </div>
    <div class="side-panel">
      <div class="side-header side-b">
        <span>Strategy B — NIFTY Bias</span>
        <span id="pnlB_side" style="font-size:15px">₹ —</span>
      </div>
      <div id="sideB_content" style="border:1px solid #0f766e;border-top:none;border-radius:0 0 8px 8px;padding:10px;"></div>
    </div>
  </div>
</div>

<script>
let activeTab = 'tabA';
let activeClass = 'active-a';

function showTab(id, btn, cls) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => {
    b.classList.remove('active-a','active-b','active-c');
  });
  document.getElementById(id).classList.add('active');
  btn.classList.add(cls);
  activeTab = id; activeClass = cls;
}

// ── Clock ──
setInterval(() => {
  document.getElementById('clock').textContent =
    new Date().toLocaleTimeString('en-IN', {hour12: false});
}, 1000);

// ── Helpers ──
function fmt(v, digits=2) {
  if (v === null || v === undefined || v === '') return '—';
  const n = parseFloat(v);
  if (isNaN(n)) return v;
  return (n >= 0 ? '+' : '') + n.toFixed(digits);
}
function fmtRs(v) {
  if (v === null || v === undefined || v === '') return '—';
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  const cls = n >= 0 ? 'pos' : 'neg';
  return `<span class="${cls}">₹ ${n >= 0 ? '+' : ''}${n.toFixed(2)}</span>`;
}
function pnlClass(v) {
  const n = parseFloat(v);
  if (isNaN(n) || v === '') return 'neu';
  return n > 0 ? 'pos' : n < 0 ? 'neg' : 'neu';
}

function buildSummary(s) {
  if (!s) return '<div class="offline-card"><div class="icon">⚠</div>Waiting for engine data…<div class="hint">Make sure the strategy engine is running</div></div>';
  const pnlCls = s.daily_pnl >= 0 ? 'pos' : 'neg';
  const openCls = s.open_pnl >= 0 ? 'pos' : 'neg';
  return `
  <div class="summary-strip">
    <div class="sum-card"><div class="label">Total PnL</div><div class="value ${pnlCls}">₹ ${s.daily_pnl >= 0 ? '+' : ''}${(+s.daily_pnl).toFixed(2)}</div></div>
    <div class="sum-card"><div class="label">Open PnL</div><div class="value ${openCls}">₹ ${s.open_pnl >= 0 ? '+' : ''}${(+s.open_pnl).toFixed(2)}</div></div>
    <div class="sum-card"><div class="label">Closed PnL</div><div class="value ${(+s.closed_pnl)>=0?'pos':'neg'}">₹ ${(+s.closed_pnl)>=0?'+':''}${(+s.closed_pnl).toFixed(2)}</div></div>
    <div class="sum-card"><div class="label">Open Positions</div><div class="value info">${s.open_count}</div></div>
    <div class="sum-card"><div class="label">Closed Today</div><div class="value neu">${s.closed_count}</div></div>
    <div class="sum-card"><div class="label">Win Rate</div><div class="value ${s.win_rate>=50?'pos':'neg'}">${s.win_rate}%</div></div>
    <div class="sum-card"><div class="label">Wins / Losses</div><div class="value neu"><span class="pos">${s.wins}</span> / <span class="neg">${s.losses}</span></div></div>
    <div class="sum-card"><div class="label">Mode</div><div class="value neu">${s.mode}</div></div>
  </div>`;
}

function buildPositionsTable(positions) {
  if (!positions || !positions.length)
    return '<div class="tbl-wrap"><div class="tbl-title">Open Positions</div><div style="padding:14px;color:var(--muted);font-size:12px;">No open positions</div></div>';

  const open = positions.filter(p => p.status === 'OPEN');
  if (!open.length) return '';
  let rows = '';
  for (const p of open) {
    const side = p.side === 'BUY'
      ? '<span class="badge badge-buy">BUY</span>'
      : '<span class="badge badge-sell">SELL</span>';
    const pnlHtml = p.live_pnl !== '' ? fmtRs(p.live_pnl) : '—';
    const sym = p.symbol.includes('__PYR')
      ? `<span style="color:var(--ylw)">${p.symbol}</span>` : p.symbol;
    rows += `<tr>
      <td>${sym}</td><td>${side}</td>
      <td>${p.strategy}</td>
      <td>${p.entry}</td><td>${p.sl}</td>
      <td>${p.target || '—'}</td>
      <td>${p.ltp || '—'}</td>
      <td>${pnlHtml}</td>
      <td>${p.entry_time}</td>
    </tr>`;
  }
  return `<div class="tbl-wrap">
    <div class="tbl-title">Open Positions (${open.length})</div>
    <table>
      <thead><tr>
        <th>Symbol</th><th>Side</th><th>Strategy</th>
        <th>Entry</th><th>SL</th><th>Target</th>
        <th>LTP</th><th>Live PnL</th><th>Time</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function buildTradesTable(trades) {
  if (!trades || !trades.length)
    return '<div class="tbl-wrap"><div class="tbl-title">Closed Trades</div><div style="padding:14px;color:var(--muted);font-size:12px;">No closed trades yet</div></div>';

  const closed = trades.filter(t => !t.Symbol?.includes('__PYR')).slice(-30).reverse();
  if (!closed.length) return '';
  let rows = '';
  for (const t of closed) {
    const pv = parseFloat(t.PnL);
    const pnlCls = isNaN(pv) ? 'neu' : (pv > 0 ? 'pos' : 'neg');
    const side = (t.Side || '').toUpperCase() === 'BUY'
      ? '<span class="badge badge-buy">BUY</span>'
      : '<span class="badge badge-sell">SELL</span>';
    rows += `<tr>
      <td>${t.Symbol}</td><td>${side}</td>
      <td>${t.Strategy || '—'}</td>
      <td>${t.EntryPrice || t.Entry || '—'}</td>
      <td>${t.ExitPrice || t.Exit || '—'}</td>
      <td class="${pnlCls}">₹ ${isNaN(pv)?'—':(pv>=0?'+':'')+pv.toFixed(2)}</td>
      <td>${t.ExitReason || t.Reason || '—'}</td>
      <td>${t.ExitTime || t.Time || '—'}</td>
    </tr>`;
  }
  return `<div class="tbl-wrap">
    <div class="tbl-title">Closed Trades (last 30)</div>
    <table>
      <thead><tr>
        <th>Symbol</th><th>Side</th><th>Strategy</th>
        <th>Entry</th><th>Exit</th><th>PnL</th>
        <th>Reason</th><th>Time</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function buildLog(logLines) {
  if (!logLines || !logLines.length) return '';
  const lines = logLines.slice(-50).reverse();
  const html = lines.map(l => `<div class="log-line">${l}</div>`).join('');
  return `<div class="tbl-wrap">
    <div class="tbl-title">Execution Log (last 50)</div>
    <div class="log-box">${html}</div>
  </div>`;
}

function buildOffline(err) {
  return `<div class="offline-card">
    <div class="icon">⚠</div>
    Engine not reachable<br>
    <div class="hint">${err}</div>
  </div>`;
}

function renderEngine(prefix, data, errMsg) {
  const sumEl  = document.getElementById(`summary${prefix}`);
  const posEl  = document.getElementById(`positions${prefix}`);
  const trdEl  = document.getElementById(`trades${prefix}`);
  const logEl  = document.getElementById(`log${prefix}`);
  if (!sumEl) return;   // panel not in DOM yet

  if (errMsg) {
    sumEl.innerHTML = buildOffline(errMsg);
    posEl.innerHTML = ''; trdEl.innerHTML = ''; logEl.innerHTML = '';
    return;
  }
  if (!data) {
    sumEl.innerHTML = '<div class="offline-card"><div class="icon">⏳</div>Waiting for first data…</div>';
    posEl.innerHTML = ''; trdEl.innerHTML = ''; logEl.innerHTML = '';
    return;
  }
  const s = data.summary || {};
  document.getElementById('mode-badge').textContent = s.mode || 'PAPER';
  sumEl.innerHTML = buildSummary(s);
  posEl.innerHTML = buildPositionsTable(data.positions);
  trdEl.innerHTML = buildTradesTable(data.trades);
  logEl.innerHTML = buildLog(data.log);
}

// ── Side-by-side mini render ──
function renderSide(id, pnlId, data, errMsg) {
  const el = document.getElementById(id);
  const pnlEl = document.getElementById(pnlId);
  if (!el) return;

  if (errMsg || !data) {
    el.innerHTML = `<div style="color:var(--red);font-size:12px;">Engine offline</div>`;
    pnlEl.innerHTML = '—';
    return;
  }
  const s = data.summary || {};
  const pos = (data.positions || []).filter(p => p.status === 'OPEN');
  const pv = parseFloat(s.daily_pnl);
  const pnlCls = isNaN(pv) || pv === 0 ? 'neu' : pv > 0 ? 'pos' : 'neg';
  pnlEl.innerHTML = `<span class="${pnlCls}">₹ ${isNaN(pv)?'—':(pv>=0?'+':'')+pv.toFixed(2)}</span>`;

  let rows = '';
  if (!pos.length) {
    rows = '<div style="color:var(--muted);font-size:12px;padding:6px 0;">No open positions</div>';
  } else {
    let tbl = '<table style="width:100%;font-size:11px;border-collapse:collapse">';
    tbl += '<thead><tr style="color:var(--muted)"><th style="padding:4px 6px;text-align:left">Symbol</th><th>Side</th><th>Strategy</th><th>Entry</th><th>LTP</th><th>Live PnL</th></tr></thead><tbody>';
    for (const p of pos.slice(0,15)) {
      const side = p.side === 'BUY'
        ? '<span class="badge badge-buy">BUY</span>'
        : '<span class="badge badge-sell">SELL</span>';
      const sym = p.symbol.includes('__PYR')
        ? `<span style="color:var(--ylw)">${p.symbol}</span>` : p.symbol;
      const lp = p.live_pnl !== '' ? parseFloat(p.live_pnl) : NaN;
      const lpCls = isNaN(lp) ? 'neu' : lp >= 0 ? 'pos' : 'neg';
      tbl += `<tr style="border-top:1px solid #1c2026">
        <td style="padding:4px 6px">${sym}</td>
        <td style="padding:4px 6px">${side}</td>
        <td style="padding:4px 6px">${p.strategy}</td>
        <td style="padding:4px 6px">${p.entry}</td>
        <td style="padding:4px 6px">${p.ltp || '—'}</td>
        <td style="padding:4px 6px" class="${lpCls}">₹ ${isNaN(lp)?'—':(lp>=0?'+':'')+lp.toFixed(2)}</td>
      </tr>`;
    }
    tbl += '</tbody></table>';
    rows = tbl;
  }

  // summary mini bar
  el.innerHTML = `
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">
      <span style="font-size:11px;color:var(--muted)">Open: <strong style="color:var(--blu)">${s.open_count ?? '—'}</strong></span>
      <span style="font-size:11px;color:var(--muted)">Closed: <strong style="color:var(--txt)">${s.closed_count ?? '—'}</strong></span>
      <span style="font-size:11px;color:var(--muted)">Win rate: <strong class="${(s.win_rate??0)>=50?'pos':'neg'}">${s.win_rate ?? '—'}%</strong></span>
      <span style="font-size:11px;color:var(--muted)">Open PnL: <strong class="${(+s.open_pnl)>=0?'pos':'neg'}">₹ ${(+s.open_pnl||0).toFixed(2)}</strong></span>
    </div>
    ${rows}
  `;
}

// ── Main poll loop ──
async function poll() {
  try {
    const resp = await fetch('/combined_data');
    const d = await resp.json();

    renderEngine('A', d.A, d.A_err);
    renderEngine('B', d.B, d.B_err);
    renderSide('sideA_content', 'pnlA_side', d.A, d.A_err);
    renderSide('sideB_content', 'pnlB_side', d.B, d.B_err);
  } catch(e) {
    console.error('Dashboard fetch failed', e);
  }
}

poll();
setInterval(poll, 3000);
</script>
</body>
</html>"""


# ── HTTP server ──────────────────────────────────────────────────────────────
class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        pass

    def do_GET(self):
        if self.path.startswith('/combined_data'):
            with _lock:
                payload = json.dumps({
                    "A":     _cache["A"],
                    "B":     _cache["B"],
                    "A_err": _cache["A_err"],
                    "B_err": _cache["B_err"],
                }).encode()
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
    print("  Combined Strategy Dashboard")
    print(f"  Engine A  → {ENGINE_A_URL}")
    print(f"  Engine B  → {ENGINE_B_URL}")
    print(f"  Dashboard → http://localhost:{COMBINED_PORT}")
    print("=" * 60)

    threading.Thread(target=_poll_loop, daemon=True).start()

    server = HTTPServer(("0.0.0.0", COMBINED_PORT), _Handler)
    server.serve_forever()
