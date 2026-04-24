"""
qt_screener.py — Phase 3: Stock Screeners + Alert Controls
============================================================
Imported by qt_dashboard.py. Adds two new tabs:

Tab: 📈 Screeners
  • Panchak Levels (TOP_HIGH / TOP_LOW)
  • Panchak Breakouts (LTP ≥ TOP_HIGH or ≤ TOP_LOW)
  • OHL Scanner (Open = Low = bullish, Open = High = bearish)
  • Daily Breakout (LTP > Yesterday High/Low + EMA confirmation)
  • Near Levels (stocks within 0.5% of TOP_HIGH or TOP_LOW)

Tab: 🔔 Alerts
  • Telegram ON/OFF toggles for every alert category
  • Send test Telegram message
  • Alert log (last 50 fired alerts, persisted to JSON)

All data comes from the same Kite session KiteSession.get()
and uses the same SYMBOLS list from the dashboard.
"""

from __future__ import annotations
import os, json, threading, urllib.request
from datetime import datetime, date, timedelta

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTabWidget,
    QTableWidget, QTableWidgetItem, QHeaderView, QPushButton,
    QFrame, QScrollArea, QCheckBox, QGridLayout, QGroupBox,
    QSizePolicy, QComboBox, QLineEdit, QTextEdit, QSplitter,
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QColor, QFont, QBrush

# ── Matched to qt_dashboard.py premium palette ─────────────────────────────────
C_GREEN  = "#276749"   # forest green — bull / buy
C_RED    = "#C53030"   # strong red — bear / sell
C_YELLOW = "#B7791F"   # amber — warning / neutral key
C_BLUE   = "#2C5282"   # navy blue — accent (matches tabs)
C_CYAN   = "#2B6CB0"   # medium blue — secondary
C_PURPLE = "#553C9A"   # purple — special
C_ORANGE = "#C05621"   # orange — near-level alerts
C_DIM    = "#718096"   # dim text
C_MUTED  = "#4A5568"   # muted labels
C_TEXT   = "#1A1D23"   # primary text
C_TEXT2  = "#2D3748"   # headings
C_BG     = "#F0F2F5"   # page bg
C_BG_CARD= "#FFFFFF"   # card bg
C_BORDER = "#DDE1E8"   # border
C_BG_BULL= "#F0FFF4"   # light green row tint
C_BG_BEAR= "#FFF5F5"   # light red row tint

TG_BOT_TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID   = "-1003706739531"
# ── CACHE folder paths (_qt suffix = Qt-specific, won't clash with Streamlit) ──
_BASE_DIR_SCR   = os.path.dirname(os.path.abspath(__file__))
_CACHE_DIR_SCR  = os.path.join(_BASE_DIR_SCR, "CACHE")
os.makedirs(_CACHE_DIR_SCR, exist_ok=True)

def _scr_cp(f): return os.path.join(_CACHE_DIR_SCR, f)

PANCHAK_DATA_FILE   = _scr_cp("panchak_data.csv")      # shared with old dashboard
TG_DEDUP_FILE       = _scr_cp("tg_dedup_qt.json")      # Telegram dedup
ALERT_LOG_FILE      = _scr_cp("alert_log_qt.json")      # alert history
ALERT_SETTINGS_FILE = _scr_cp("alert_settings_qt.json") # toggle settings

# ── Panchak period dates ──────────────────────────────────────────────────────
# Read from CACHE/panchak_meta.csv (written by old Panchak dashboard).
# Falls back to hardcoded defaults if not found.
def _load_panchak_dates():
    """
    Load PANCHAK_START and PANCHAK_END from CACHE/panchak_meta.csv.
    The old Panchak dashboard writes this file every time it rebuilds.
    Falls back to hardcoded defaults if file is missing.
    """
    try:
        if os.path.exists(PANCHAK_META_FILE := _scr_cp("panchak_meta.csv")):
            import pandas as _pd
            meta = _pd.read_csv(PANCHAK_META_FILE)
            meta = meta.set_index("key")["value"].to_dict()
            start = date.fromisoformat(meta.get("start_date", "2026-03-17"))
            end   = date.fromisoformat(meta.get("end_date",   "2026-03-20"))
            return start, end
    except Exception as e:
        print(f"[Panchak] Could not read panchak_meta.csv: {e}")
    # Hardcoded fallback — update when new panchak starts
    return date(2026, 3, 17), date(2026, 3, 20)

PANCHAK_START, PANCHAK_END = _load_panchak_dates()
print(f"[Screener] Panchak period: {PANCHAK_START} → {PANCHAK_END}")


# Alert toggle defaults
ALERT_TOGGLE_DEFAULTS = {
    "tg_TOP_HIGH":      True,
    "tg_TOP_LOW":       True,
    "tg_OHL_BULL":      True,
    "tg_OHL_BEAR":      True,
    "tg_DAILY_BREAK_UP":True,
    "tg_DAILY_BREAK_DN":True,
    "tg_BOS_1H":        True,
    "tg_OI_INTEL":      True,
    "tg_ASTRO":         True,
    "tg_KP_ALERTS":     True,
}
_alert_state: dict = dict(ALERT_TOGGLE_DEFAULTS)
_alert_log:   list = []  # list of dicts {time, category, message}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _make_label(text, size=12, bold=False, color=None,
                align=Qt.AlignmentFlag.AlignLeft):
    lbl = QLabel(text)
    f = QFont("Segoe UI", size)
    f.setBold(bold)
    lbl.setFont(f)
    style = "background:transparent;"
    if color: style += f"color:{color};"
    lbl.setStyleSheet(style)
    lbl.setAlignment(align)
    return lbl

def _card_frame(layout_cls=QVBoxLayout, pad=(12, 9, 12, 9)):
    frame = QFrame()
    frame.setStyleSheet(f"""
        QFrame {{
            background: {C_BG_CARD};
            border: 1px solid {C_BORDER};
            border-radius: 8px;
        }}
        QLabel {{ background: transparent; color: #2D3748; }}
    """)
    lay = layout_cls()
    lay.setContentsMargins(*pad)
    lay.setSpacing(6)
    frame.setLayout(lay)
    return frame, lay

def _section_title(text):
    """Accent left-border section header — matches dashboard style."""
    lbl = QLabel(f"  {text}")
    lbl.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
    lbl.setFixedHeight(30)
    lbl.setStyleSheet(f"""
        color: {C_BLUE};
        background: #F8F9FB;
        border-left: 4px solid {C_BLUE};
        border-bottom: 1px solid {C_BORDER};
        padding-left: 10px;
        font-weight: 700;
    """)
    return lbl

def _stat_pill(label, value, color, width=120):
    """Consistent stat pill — matches dashboard stat_pill."""
    frame = QFrame()
    frame.setFixedWidth(width)
    frame.setStyleSheet(f"""
        QFrame {{
            background: #FFFFFF;
            border: 1px solid #DDE1E8;
            border-top: 3px solid {color};
            border-radius: 6px;
        }}
        QLabel {{ background: transparent; color: #2D3748; }}
    """)
    lay = QVBoxLayout(frame)
    lay.setContentsMargins(8, 6, 8, 6)
    lay.setSpacing(2)
    cap = QLabel(label.upper())
    cap.setFont(QFont("Segoe UI", 8, QFont.Weight.Bold))
    cap.setStyleSheet("color:#718096; letter-spacing:0.8px; background:transparent;")
    cap.setAlignment(Qt.AlignmentFlag.AlignCenter)
    val = QLabel(value)
    val.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
    val.setStyleSheet(f"color:{color}; background:transparent;")
    val.setAlignment(Qt.AlignmentFlag.AlignCenter)
    lay.addWidget(cap); lay.addWidget(val)
    frame._v = val; frame._cap = cap
    return frame

def _make_table(cols):
    """Consistent table — matches dashboard table style."""
    t = QTableWidget(0, len(cols))
    t.setHorizontalHeaderLabels(cols)
    t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
    t.horizontalHeader().setStretchLastSection(True)
    t.verticalHeader().setVisible(False)
    t.setAlternatingRowColors(True)
    t.setShowGrid(False)
    t.setStyleSheet("""
        QTableWidget {
            background: #FFFFFF;
            alternate-background-color: #F8F9FA;
            border: 1px solid #DDE1E8;
            border-radius: 4px;
            font-size: 12px;
            selection-background-color: #BEE3F8;
            gridline-color: #EEF1F6;
            color: #1A1D23;
        }
        QTableWidget::item { padding: 6px 12px; border-bottom: 1px solid #EEF1F6; color: #1A1D23; }
        QHeaderView::section {
            background: #F8F9FA;
            color: #4A5568;
            font-size: 11px; font-weight: 700;
            letter-spacing: 0.3px;
            padding: 8px 12px;
            border: none;
            border-right: 1px solid #E2E8F0;
            border-bottom: 2px solid #CBD5E0;
        }
        QHeaderView { background: #F8F9FA; }
    """)
    return t

def _fmt_chg(v):
    try:
        v = float(v)
        return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"
    except Exception:
        return str(v)

def _chg_color(v):
    try:
        return C_GREEN if float(v) >= 0 else C_RED
    except Exception:
        return C_DIM

def _tg_send(message: str, dedup_key: str = None) -> bool:
    """Send Telegram message (blocking — call from thread)."""
    if not TG_BOT_TOKEN: return False
    if dedup_key:
        try:
            if os.path.exists(TG_DEDUP_FILE):
                d = json.loads(open(TG_DEDUP_FILE).read())
                slot = datetime.now().strftime("%H%M")[:-1]  # 10-min slot
                if d.get(f"{dedup_key}_{slot}"): return False
        except Exception: pass
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = json.loads(resp.read()).get("ok", False)
        if ok and dedup_key:
            slot = datetime.now().strftime("%H%M")[:-1]
            try:
                d = json.loads(open(TG_DEDUP_FILE).read()) if os.path.exists(TG_DEDUP_FILE) else {}
                d[f"{dedup_key}_{slot}"] = datetime.now().isoformat()
                open(TG_DEDUP_FILE, "w").write(json.dumps(d))
            except Exception: pass
        return ok
    except Exception as e:
        print(f"TG error: {e}"); return False

def _tg_bg(message: str, dedup_key: str = None):
    threading.Thread(target=_tg_send, args=(message, dedup_key), daemon=True).start()

def _log_alert(category: str, message: str):
    global _alert_log
    _alert_log.insert(0, {
        "time": datetime.now().strftime("%H:%M:%S"),
        "category": category,
        "message": message[:120],
    })
    _alert_log = _alert_log[:100]
    try:
        open(ALERT_LOG_FILE, "w").write(json.dumps(_alert_log))
    except Exception: pass

def _load_alert_log():
    global _alert_log
    try:
        if os.path.exists(ALERT_LOG_FILE):
            _alert_log = json.loads(open(ALERT_LOG_FILE).read())
    except Exception: pass

def is_alert_on(key: str) -> bool:
    return _alert_state.get(key, False)


# ─────────────────────────────────────────────────────────────────────────────
# SCREENER DATA WORKER
# ─────────────────────────────────────────────────────────────────────────────

class ScreenerWorker(QThread):
    done  = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, symbols: list, kite=None):
        super().__init__()
        self.symbols = symbols
        self._kite   = kite   # passed directly — no circular import needed

    def run(self):
        try:
            import pandas as pd
            kite = self._kite

            # If no kite passed directly, try fallback
            if kite is None:
                try:
                    import sys
                    qt_dash = sys.modules.get("qt_dashboard") or sys.modules.get("__main__")
                    if qt_dash and hasattr(qt_dash, "KiteSession"):
                        ks = qt_dash.KiteSession.get()
                        if ks.ok:
                            kite = ks.kite
                except Exception as _e:
                    print(f"[ScreenerWorker] KiteSession fallback failed: {_e}")

            if kite is None:
                self.error.emit("Kite not connected — go to 🔌 Session tab and connect first")
                return
            # ── 1. Batch quote all symbols ────────────────────
            def kite_sym(s): return f"NSE:{s}"
            syms_nse = [kite_sym(s) for s in self.symbols]
            # Kite quote limit 500 per call
            quotes = {}
            for i in range(0, len(syms_nse), 400):
                try:
                    q = kite.quote(syms_nse[i:i+400])
                    quotes.update(q)
                except Exception: pass

            # ── 2. Load panchak data ──────────────────────────
            panchak = {}
            if os.path.exists(PANCHAK_DATA_FILE):
                try:
                    import pandas as pd
                    pdf = pd.read_csv(PANCHAK_DATA_FILE)
                    for _, r in pdf.iterrows():
                        panchak[r["Symbol"]] = {
                            "TOP_HIGH": float(r.get("TOP_HIGH", 0)),
                            "TOP_LOW":  float(r.get("TOP_LOW", 0)),
                            "DIFF":     float(r.get("DIFF", 0)),
                            "BT":       float(r.get("BT", 0)),
                            "ST":       float(r.get("ST", 0)),
                        }
                except Exception: pass

            # ── 3. EMA data from local OHLC DB ────────────────
            def _calc_ema(data, span):
                """Pure exponential moving average — defined once, not per-loop."""
                k = 2 / (span + 1); v = data[0]
                for x in data[1:]: v = x * k + v * (1 - k)
                return v

            ema_data = {}
            try:
                from ohlc_store import OHLCStore
                db = OHLCStore()
                for sym in self.symbols:
                    candles = db.get(sym, n=60)
                    if len(candles) >= 20:
                        closes = [c["close"] for c in candles]
                        ema_data[sym] = {
                            "EMA20": _calc_ema(closes, 20),
                            "EMA50": _calc_ema(closes, 50) if len(closes) >= 50 else 0,
                        }
            except Exception: pass

            # ── 4. Build screener rows ──────────────────────────
            rows = []
            for sym in self.symbols:
                q = quotes.get(kite_sym(sym))
                if not q: continue
                ltp   = q.get("last_price", 0)
                pc    = q["ohlc"]["close"]
                chg   = round(ltp - pc, 2)
                chg_p = round((chg/pc*100), 2) if pc else 0
                lo    = round(q["ohlc"]["open"], 2)
                lh    = round(q["ohlc"]["high"], 2)
                ll    = round(q["ohlc"]["low"],  2)
                vol   = q.get("volume", 0)

                pan = panchak.get(sym, {})
                th  = pan.get("TOP_HIGH", 0)
                tl  = pan.get("TOP_LOW",  0)
                diff= pan.get("DIFF", 0)
                bt  = pan.get("BT", 0)
                st  = pan.get("ST", 0)

                em  = ema_data.get(sym, {})
                ema20 = em.get("EMA20", 0)
                ema50 = em.get("EMA50", 0)

                # NEAR field
                if th > 0 and tl > 0:
                    if ltp >= th:   near = "🟢 BREAK ↑"
                    elif ltp <= tl: near = "🔴 BREAK ↓"
                    elif (th - ltp) <= (ltp - tl): near = f"🟢 ↑ {th-ltp:.1f}"
                    else:           near = f"🔴 ↓ {ltp-tl:.1f}"
                else:
                    near = "—"

                rows.append({
                    "Symbol":    sym,
                    "LTP":       round(ltp, 2),
                    "CHANGE_%":  chg_p,
                    "DAY_OPEN":  lo,
                    "DAY_HIGH":  lh,
                    "DAY_LOW":   ll,
                    "VOLUME":    vol,
                    "TOP_HIGH":  th,
                    "TOP_LOW":   tl,
                    "DIFF":      round(diff, 2),
                    "BT":        round(bt, 2),
                    "ST":        round(st, 2),
                    "NEAR":      near,
                    "EMA20":     round(ema20, 2),
                    "EMA50":     round(ema50, 2),
                    "PREV_CLOSE": round(pc, 2),
                })

            if not rows:
                if not quotes:
                    self.error.emit("No quotes received — check Kite connection"); return
                self.error.emit(f"Quotes returned but no valid rows — check STOCKS list ({len(quotes)} raw quotes)"); return

            import pandas as pd
            df = pd.DataFrame(rows)

            # ── Screener sub-frames ───────────────────────────
            result = {}

            # Panchak all (full table)
            result["panchak_all"] = df[[
                "Symbol","LTP","CHANGE_%","TOP_HIGH","TOP_LOW",
                "DIFF","BT","ST","NEAR","DAY_HIGH","DAY_LOW",
            ]].to_dict("records")

            # TOP_HIGH breakers
            if "TOP_HIGH" in df.columns and "LTP" in df.columns and df["TOP_HIGH"].any():
                thr = df[df["LTP"] >= df["TOP_HIGH"]].copy()
                thr["GAIN"] = (thr["LTP"] - thr["TOP_HIGH"]).round(2)
                result["top_high"] = thr[[
                    "Symbol","LTP","TOP_HIGH","GAIN","CHANGE_%","DAY_HIGH","DAY_LOW"
                ]].to_dict("records")
            else:
                result["top_high"] = []

            # TOP_LOW breakers
            if "TOP_LOW" in df.columns and df["TOP_LOW"].any():
                tlr = df[df["LTP"] <= df["TOP_LOW"]].copy()
                tlr["LOSS"] = (tlr["LTP"] - tlr["TOP_LOW"]).round(2)
                result["top_low"] = tlr[[
                    "Symbol","LTP","TOP_LOW","LOSS","CHANGE_%","DAY_HIGH","DAY_LOW"
                ]].to_dict("records")
            else:
                result["top_low"] = []

            # OHL Scanner: O=L (bullish) and O=H (bearish)
            # 0.1% tolerance — tighter than 0.999/1.001 to avoid false signals
            tol = 0.001
            ol = df[((df["DAY_LOW"] - df["DAY_OPEN"]).abs() / df["DAY_OPEN"].clip(lower=1) <= tol)].copy()
            ol["SETUP"] = "🟢 O=L"
            oh = df[((df["DAY_HIGH"] - df["DAY_OPEN"]).abs() / df["DAY_OPEN"].clip(lower=1) <= tol)].copy()
            oh["SETUP"] = "🔴 O=H"
            # Remove doji overlaps (can't be both)
            oh = oh[~oh["Symbol"].isin(ol["Symbol"])]
            ohl_df = pd.concat([ol, oh]).drop_duplicates("Symbol")
            result["ohl"] = ohl_df[["Symbol","LTP","CHANGE_%","DAY_OPEN","DAY_HIGH","DAY_LOW","SETUP"]].to_dict("records")

            # Daily breakout (LTP > yesterday — requires yesterday data which is in PREV_CLOSE)
            # We use LIVE_HIGH vs prev_close as proxy
            day_buy = df[(df["LTP"] > df["PREV_CLOSE"] * 1.01) &
                         (df["EMA20"] > 0) & (df["LTP"] > df["EMA20"])]
            day_sell= df[(df["LTP"] < df["PREV_CLOSE"] * 0.99) &
                         (df["EMA20"] > 0) & (df["LTP"] < df["EMA20"])]
            result["daily_buy"]  = day_buy[["Symbol","LTP","CHANGE_%","EMA20","EMA50","DAY_HIGH","DAY_LOW"]].to_dict("records")
            result["daily_sell"] = day_sell[["Symbol","LTP","CHANGE_%","EMA20","EMA50","DAY_HIGH","DAY_LOW"]].to_dict("records")

            # Near TOP_HIGH (within 0.5% or 0.5×DIFF)
            def is_near(row):
                th = row.get("TOP_HIGH", 0); tl = row.get("TOP_LOW", 0)
                ltp = row.get("LTP", 0); diff = row.get("DIFF", 0) or (th - tl)
                if th == 0 or tl == 0: return False
                tol = max(diff * 0.5, ltp * 0.005)
                return (abs(ltp - th) <= tol or abs(ltp - tl) <= tol) and not (ltp >= th or ltp <= tl)
            near_df = df[df.apply(is_near, axis=1)]
            result["near"] = near_df[["Symbol","LTP","CHANGE_%","TOP_HIGH","TOP_LOW","NEAR"]].to_dict("records")

            result["timestamp"] = datetime.now().strftime("%H:%M:%S")

            # ── Fire Telegram alerts ───────────────────────────
            self._fire_alerts(result)
            self.done.emit(result)

        except Exception as e:
            import traceback; traceback.print_exc()
            self.error.emit(str(e))

    def _fire_alerts(self, result):
        ts = datetime.now().strftime("%H:%M IST")
        pstart = PANCHAK_START.strftime("%d-%b-%Y")
        pend   = PANCHAK_END.strftime("%d-%b-%Y")

        # TOP_HIGH breaks
        if is_alert_on("tg_TOP_HIGH") and result.get("top_high"):
            rows = result["top_high"]
            lines = "\n".join(
                f"  • <b>{r['Symbol']}</b>  LTP: {r['LTP']}  TH: {r['TOP_HIGH']}  +{r['GAIN']}"
                for r in rows[:10]
            )
            msg = (f"🚀 <b>PANCHAK TOP_HIGH BREAK</b>  ⏰ {ts}\n"
                   f"Period: {pstart} → {pend}\n"
                   f"━━━━━━━━━━━━━━━━━━━\n{lines}\n"
                   f"━━━━━━━━━━━━━━━━━━━\n"
                   f"<i>✅ Valid till panchak ends ({pend})</i>")
            syms = "_".join(r["Symbol"] for r in rows[:3])
            _tg_bg(msg, dedup_key=f"TOP_HIGH_{syms}_{date.today().isoformat()}")
            _log_alert("TOP_HIGH", f"{len(rows)} stocks: {', '.join(r['Symbol'] for r in rows[:5])}")

        # TOP_LOW breaks
        if is_alert_on("tg_TOP_LOW") and result.get("top_low"):
            rows = result["top_low"]
            lines = "\n".join(
                f"  • <b>{r['Symbol']}</b>  LTP: {r['LTP']}  TL: {r['TOP_LOW']}  {r['LOSS']}"
                for r in rows[:10]
            )
            msg = (f"🔻 <b>PANCHAK TOP_LOW BREAK</b>  ⏰ {ts}\n"
                   f"Period: {pstart} → {pend}\n"
                   f"━━━━━━━━━━━━━━━━━━━\n{lines}\n"
                   f"━━━━━━━━━━━━━━━━━━━\n"
                   f"<i>⚠️ Below panchak low — avoid longs</i>")
            syms = "_".join(r["Symbol"] for r in rows[:3])
            _tg_bg(msg, dedup_key=f"TOP_LOW_{syms}_{date.today().isoformat()}")
            _log_alert("TOP_LOW", f"{len(rows)} stocks: {', '.join(r['Symbol'] for r in rows[:5])}")

        # OHL Bull
        bull_ohl = [r for r in result.get("ohl",[]) if r.get("SETUP","").startswith("🟢")]
        if is_alert_on("tg_OHL_BULL") and bull_ohl:
            syms = ", ".join(r["Symbol"] for r in bull_ohl[:8])
            msg  = (f"🟢 <b>OHL Scanner — Open = Low (Bullish)</b>  ⏰ {ts}\n"
                    f"<b>{syms}</b>\n<i>Price opened at day low — potential reversal up</i>")
            _tg_bg(msg, dedup_key=f"OHL_BULL_{date.today().isoformat()}")
            _log_alert("OHL_BULL", syms)

        # OHL Bear
        bear_ohl = [r for r in result.get("ohl",[]) if r.get("SETUP","").startswith("🔴")]
        if is_alert_on("tg_OHL_BEAR") and bear_ohl:
            syms = ", ".join(r["Symbol"] for r in bear_ohl[:8])
            msg  = (f"🔴 <b>OHL Scanner — Open = High (Bearish)</b>  ⏰ {ts}\n"
                    f"<b>{syms}</b>\n<i>Price opened at day high — potential reversal down</i>")
            _tg_bg(msg, dedup_key=f"OHL_BEAR_{date.today().isoformat()}")
            _log_alert("OHL_BEAR", syms)


# ─────────────────────────────────────────────────────────────────────────────
# SCREENER PANEL WIDGET
# ─────────────────────────────────────────────────────────────────────────────

class ScreenerPanel(QWidget):
    def __init__(self, symbols: list, kite_getter=None):
        """
        symbols     : list of NSE symbol strings
        kite_getter : callable that returns the live KiteConnect object,
                      e.g. lambda: KiteSession.get().kite
                      Pass this from MainWindow after Kite connects.
        """
        super().__init__()
        self.symbols     = symbols
        self._data       = {}
        self._worker     = None
        self._kite_getter = kite_getter   # no circular import needed
        _load_alert_log()
        self._build_ui()

    def set_kite_getter(self, getter):
        """Set the kite getter after construction (called by MainWindow)."""
        self._kite_getter = getter

    def _get_kite(self):
        """Safely get the live Kite object — tries kite_getter first, then KiteSession."""
        # Primary: use the getter lambda set by MainWindow (no circular import)
        if self._kite_getter:
            try:
                kite = self._kite_getter()
                if kite is not None:
                    return kite
            except Exception:
                pass

        # Fallback: try direct import of KiteSession
        # This works because at runtime qt_dashboard is already loaded
        try:
            import sys
            qt_dash = sys.modules.get("qt_dashboard") or sys.modules.get("__main__")
            if qt_dash and hasattr(qt_dash, "KiteSession"):
                ks = qt_dash.KiteSession.get()
                if ks.ok:
                    return ks.kite
        except Exception:
            pass

        return None

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12); root.setSpacing(12)

        # ── Header ────────────────────────────────────────────
        hdr = QHBoxLayout()
        hdr.addWidget(_section_title("Stock Screeners — Panchak Levels"))
        hdr.addStretch()
        self.lbl_ts = QLabel("Last scan: —")
        self.lbl_ts.setFont(QFont("Segoe UI", 9))
        self.lbl_ts.setStyleSheet(f"color:{C_DIM};background:transparent;")
        hdr.addWidget(self.lbl_ts)
        hdr.addSpacing(16)
        self.btn_scan = QPushButton("⟳  Run Screeners")
        self.btn_scan.setFixedSize(140, 30)
        self.btn_scan.clicked.connect(self._scan)
        hdr.addWidget(self.btn_scan)
        root.addLayout(hdr)

        # ── Panchak info strip ────────────────────────────────
        pf, pl = _card_frame(QHBoxLayout, (12, 7, 12, 7))
        period_lbl = QLabel("Panchak Period:")
        period_lbl.setFont(QFont("Segoe UI", 10)); period_lbl.setStyleSheet(f"color:{C_DIM};background:transparent;")
        date_lbl = QLabel(f"{PANCHAK_START.strftime('%d-%b-%Y')}  →  {PANCHAK_END.strftime('%d-%b-%Y')}")
        date_lbl.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        date_lbl.setStyleSheet(f"color:{C_YELLOW};background:transparent;")
        info_lbl = QLabel("  ·  TOP_HIGH = panchak HIGH  ·  TOP_LOW = panchak LOW")
        info_lbl.setFont(QFont("Segoe UI", 9)); info_lbl.setStyleSheet(f"color:{C_DIM};background:transparent;")
        pl.addWidget(period_lbl); pl.addWidget(date_lbl); pl.addWidget(info_lbl); pl.addStretch()
        root.addWidget(pf)

        # ── Summary pills ─────────────────────────────────────
        stats = QHBoxLayout(); stats.setSpacing(10)
        self.p_th   = _stat_pill("TOP_HIGH Breaks", "0", C_GREEN,  148)
        self.p_tl   = _stat_pill("TOP_LOW Breaks",  "0", C_RED,    148)
        self.p_ohl  = _stat_pill("OHL Setups",      "0", C_YELLOW, 120)
        self.p_dbuy = _stat_pill("Daily Buy",        "0", C_GREEN,  110)
        self.p_dsel = _stat_pill("Daily Sell",       "0", C_RED,    110)
        self.p_near = _stat_pill("Near Levels",      "0", C_ORANGE, 120)
        for p in [self.p_th,self.p_tl,self.p_ohl,self.p_dbuy,self.p_dsel,self.p_near]:
            stats.addWidget(p)
        stats.addStretch()
        root.addLayout(stats)

        # ── Sub-tabs ──────────────────────────────────────────
        self.sub_tabs = QTabWidget()
        # sub-tab styling inherits from dashboard QSS — no override needed

        self.tbl_panchak = _make_table(
            ["Symbol","LTP","Chg%","TOP_HIGH","TOP_LOW","DIFF","BT","ST","NEAR","Day_H","Day_L"])
        self.tbl_th      = _make_table(["Symbol","LTP","TOP_HIGH","GAIN","Chg%","Day_H","Day_L"])
        self.tbl_tl      = _make_table(["Symbol","LTP","TOP_LOW","LOSS","Chg%","Day_H","Day_L"])
        self.tbl_ohl     = _make_table(["Symbol","LTP","Chg%","Day_Open","Day_H","Day_L","SETUP"])
        self.tbl_dbuy    = _make_table(["Symbol","LTP","Chg%","EMA20","EMA50","Day_H","Day_L"])
        self.tbl_dsell   = _make_table(["Symbol","LTP","Chg%","EMA20","EMA50","Day_H","Day_L"])
        self.tbl_near    = _make_table(["Symbol","LTP","Chg%","TOP_HIGH","TOP_LOW","NEAR"])

        self.sub_tabs.addTab(self.tbl_panchak, "📊  All Panchak")
        self.sub_tabs.addTab(self.tbl_th,      "🟢  TOP_HIGH Breaks")
        self.sub_tabs.addTab(self.tbl_tl,      "🔴  TOP_LOW Breaks")
        self.sub_tabs.addTab(self.tbl_ohl,     "📐  OHL Scanner")
        self.sub_tabs.addTab(self.tbl_dbuy,    "⬆  Daily Buy")
        self.sub_tabs.addTab(self.tbl_dsell,   "⬇  Daily Sell")
        self.sub_tabs.addTab(self.tbl_near,    "🎯  Near Levels")
        root.addWidget(self.sub_tabs)

        self.status_lbl = QLabel("Click Run Screeners to fetch live data.")
        self.status_lbl.setFont(QFont("Segoe UI", 9))
        self.status_lbl.setStyleSheet("color:#718096;background:transparent;")
        root.addWidget(self.status_lbl)

    def _wrap(self, widget):
        return widget   # no wrapping needed — addTab accepts QTableWidget directly

    def _pill(self, label, value, color, width=120):
        return _stat_pill(label, value, color, width)

    def _scan(self):
        if self._worker and self._worker.isRunning():
            self.status_lbl.setText("⏳ Scan already running…")
            return
        kite = self._get_kite()
        if kite is None:
            self.status_lbl.setText(
                "❌  Kite not connected.  "
                "Go to the 🔌 Session tab → enter token → Connect.")
            self.status_lbl.setStyleSheet(f"color:{C_RED};background:transparent;font-size:11px;")
            return
        self.btn_scan.setEnabled(False); self.btn_scan.setText("Scanning…")
        self.status_lbl.setStyleSheet("color:#718096;background:transparent;")
        self.status_lbl.setText(f"✅  Kite connected — fetching {len(self.symbols)} symbols…")
        self._worker = ScreenerWorker(self.symbols, kite=kite)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_done(self, data):
        self.btn_scan.setEnabled(True); self.btn_scan.setText("⟳  Run Screeners")
        self._data = data
        ts = data.get("timestamp","")
        self.lbl_ts.setText(f"Last scan: {ts}")
        self.status_lbl.setStyleSheet(f"color:{C_GREEN};background:transparent;font-size:11px;")
        n = len(data.get("panchak_all", []))
        self.status_lbl.setText(
            f"✅  Scan complete — {n} symbols  |  "
            f"TH:{len(data.get('top_high',[]))}  "
            f"TL:{len(data.get('top_low',[]))}  "
            f"OHL:{len(data.get('ohl',[]))}  "
            f"Near:{len(data.get('near',[]))}"
        )
        self._populate(data)

    def _on_error(self, err):
        self.btn_scan.setEnabled(True); self.btn_scan.setText("⟳  Run Screeners")
        self.status_lbl.setText(f"❌  Error: {err}")
        self.status_lbl.setStyleSheet(f"color:{C_RED};background:transparent;font-size:11px;")

    def _populate(self, d):
        th = d.get("top_high", []);  tl = d.get("top_low", [])
        ohl= d.get("ohl", []);       dbuy= d.get("daily_buy", [])
        dsel= d.get("daily_sell",[]); near= d.get("near", [])
        all_p = d.get("panchak_all",[])

        self.p_th._v.setText(str(len(th)));   self.p_tl._v.setText(str(len(tl)))
        self.p_ohl._v.setText(str(len(ohl)));  self.p_dbuy._v.setText(str(len(dbuy)))
        self.p_dsel._v.setText(str(len(dsel)));self.p_near._v.setText(str(len(near)))

        # ── Panchak All ───────────────────────────────────────
        self._fill_panchak(self.tbl_panchak, all_p)

        # ── TOP_HIGH breaks ───────────────────────────────────
        self.tbl_th.setRowCount(0)
        for row in sorted(th, key=lambda r: r.get("GAIN",0), reverse=True):
            r = self.tbl_th.rowCount(); self.tbl_th.insertRow(r); self.tbl_th.setRowHeight(r,24)
            bg = QColor(C_BG_BULL)
            for col, (text, color) in enumerate([
                (row["Symbol"], C_BLUE), (str(row["LTP"]), C_TEXT),
                (str(row["TOP_HIGH"]), C_YELLOW),
                (f"+{row.get('GAIN',0):.2f}", C_GREEN),
                (_fmt_chg(row["CHANGE_%"]), _chg_color(row["CHANGE_%"])),
                (str(row.get("DAY_HIGH","")), C_DIM), (str(row.get("DAY_LOW","")), C_DIM),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                it.setBackground(QBrush(bg)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tbl_th.setItem(r, col, it)

        # ── TOP_LOW breaks ────────────────────────────────────
        self.tbl_tl.setRowCount(0)
        for row in sorted(tl, key=lambda r: r.get("LOSS",0)):
            r = self.tbl_tl.rowCount(); self.tbl_tl.insertRow(r); self.tbl_tl.setRowHeight(r,24)
            bg = QColor(C_BG_BEAR)
            for col, (text, color) in enumerate([
                (row["Symbol"], C_BLUE), (str(row["LTP"]), C_TEXT),
                (str(row["TOP_LOW"]), C_YELLOW),
                (f"{row.get('LOSS',0):.2f}", C_RED),
                (_fmt_chg(row["CHANGE_%"]), _chg_color(row["CHANGE_%"])),
                (str(row.get("DAY_HIGH","")), C_DIM), (str(row.get("DAY_LOW","")), C_DIM),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                it.setBackground(QBrush(bg)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tbl_tl.setItem(r, col, it)

        # ── OHL ───────────────────────────────────────────────
        self.tbl_ohl.setRowCount(0)
        for row in ohl:
            is_bull = "🟢" in row.get("SETUP","")
            r = self.tbl_ohl.rowCount(); self.tbl_ohl.insertRow(r); self.tbl_ohl.setRowHeight(r,24)
            bg = QColor(C_BG_BULL if is_bull else C_BG_BEAR)
            for col, (text, color) in enumerate([
                (row["Symbol"], C_BLUE), (str(row["LTP"]), C_TEXT),
                (_fmt_chg(row["CHANGE_%"]), _chg_color(row["CHANGE_%"])),
                (str(row.get("DAY_OPEN","")), C_DIM),
                (str(row.get("DAY_HIGH","")), C_DIM), (str(row.get("DAY_LOW","")), C_DIM),
                (row.get("SETUP",""), C_GREEN if is_bull else C_RED),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                it.setBackground(QBrush(bg)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tbl_ohl.setItem(r, col, it)

        # ── Daily Buy / Sell ──────────────────────────────────
        self._fill_daily(self.tbl_dbuy, dbuy, C_BG_BULL, C_GREEN)
        self._fill_daily(self.tbl_dsell, dsel, C_BG_BEAR, C_RED)

        # ── Near levels ───────────────────────────────────────
        self.tbl_near.setRowCount(0)
        for row in near:
            r = self.tbl_near.rowCount(); self.tbl_near.insertRow(r); self.tbl_near.setRowHeight(r,24)
            is_near_up = "↑" in row.get("NEAR","")
            bg = QColor(C_BG_BULL if is_near_up else C_BG_BEAR)
            for col, (text, color) in enumerate([
                (row["Symbol"], C_BLUE), (str(row["LTP"]), C_TEXT),
                (_fmt_chg(row["CHANGE_%"]), _chg_color(row["CHANGE_%"])),
                (str(row.get("TOP_HIGH","")), C_GREEN), (str(row.get("TOP_LOW","")), C_RED),
                (row.get("NEAR",""), C_YELLOW),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                it.setBackground(QBrush(bg)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tbl_near.setItem(r, col, it)

        self.status_lbl.setText(
            f"Screeners complete | {len(all_p)} symbols | "
            f"TH:{len(th)} TL:{len(tl)} OHL:{len(ohl)} "
            f"BUY:{len(dbuy)} SELL:{len(dsel)} NEAR:{len(near)}")

        for t in [self.tbl_panchak, self.tbl_th, self.tbl_tl,
                  self.tbl_ohl, self.tbl_dbuy, self.tbl_dsell, self.tbl_near]:
            t.resizeColumnsToContents()

    def _fill_panchak(self, tbl, rows):
        tbl.setRowCount(0)
        for row in rows:
            # row height set below
            near = row.get("NEAR","")
            is_bull_break = "BREAK ↑" in near
            is_bear_break = "BREAK ↓" in near
            is_near_up    = "↑" in near and "BREAK" not in near

            if is_bull_break: bg = QColor(C_BG_BULL)
            elif is_bear_break: bg = QColor(C_BG_BEAR)
            elif is_near_up: bg = QColor("#131d13")
            else: bg = QColor("#1a1215")

            r = tbl.rowCount(); tbl.insertRow(r); tbl.setRowHeight(r, 22)
            near_color = (C_GREEN if is_bull_break else C_RED if is_bear_break else
                          C_GREEN if is_near_up else C_YELLOW)
            for col, (text, color) in enumerate([
                (row["Symbol"],             C_BLUE),
                (str(row.get("LTP","")),    C_TEXT),
                (_fmt_chg(row.get("CHANGE_%",0)), _chg_color(row.get("CHANGE_%",0))),
                (str(row.get("TOP_HIGH","")), C_GREEN),
                (str(row.get("TOP_LOW","")), C_RED),
                (str(row.get("DIFF","")),    C_DIM),
                (str(row.get("BT","")),      C_DIM),
                (str(row.get("ST","")),      C_DIM),
                (near,                       near_color),
                (str(row.get("DAY_HIGH","")),C_DIM),
                (str(row.get("DAY_LOW","")), C_DIM),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                it.setBackground(QBrush(bg)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                tbl.setItem(r, col, it)

    def _fill_daily(self, tbl, rows, bg_color, accent):
        tbl.setRowCount(0)
        for row in rows:
            r = tbl.rowCount(); tbl.insertRow(r)
            bg = QColor(bg_color)
            for col, (text, color) in enumerate([
                (row["Symbol"], C_BLUE), (str(row["LTP"]), C_TEXT),
                (_fmt_chg(row["CHANGE_%"]), _chg_color(row["CHANGE_%"])),
                (str(round(row.get("EMA20",0),2)), accent),
                (str(round(row.get("EMA50",0),2)), C_DIM),
                (str(row.get("DAY_HIGH","")), C_DIM),
                (str(row.get("DAY_LOW","")), C_DIM),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                it.setBackground(QBrush(bg)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                tbl.setItem(r, col, it)

    def update_symbols(self, symbols: list, kite_getter=None):
        self.symbols = symbols
        if kite_getter:
            self._kite_getter = kite_getter

    def trigger_scan(self):
        """Called by MainWindow auto-refresh. Calls _scan which handles Kite check."""
        self._scan()


# ─────────────────────────────────────────────────────────────────────────────
# ALERTS PANEL WIDGET
# ─────────────────────────────────────────────────────────────────────────────

class AlertsPanel(QWidget):
    def __init__(self):
        super().__init__()
        _load_alert_log()
        self._build_ui()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12); root.setSpacing(12)

        root.addWidget(_section_title("Alert Controls"))

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setStyleSheet("QSplitter::handle{background:#1a2540;width:1px;}")

        # ══════════════════════════════════════════════════════
        # LEFT — Toggle controls
        # ══════════════════════════════════════════════════════
        left = QWidget()
        left.setStyleSheet("background:transparent;")
        left_lay = QVBoxLayout(left)
        left_lay.setContentsMargins(0, 0, 8, 0); left_lay.setSpacing(10)

        # Telegram info strip
        tg_frame, tg_lay = _card_frame(QHBoxLayout, (12, 8, 12, 8))
        bot_lbl = QLabel("Telegram Bot:")
        bot_lbl.setFont(QFont("Segoe UI",10,QFont.Weight.Bold))
        bot_lbl.setStyleSheet(f"color:{C_CYAN};background:transparent;")
        token_lbl = QLabel(f"{TG_BOT_TOKEN[:16]}…")
        token_lbl.setFont(QFont("Segoe UI",10))
        token_lbl.setStyleSheet(f"color:{C_DIM};background:transparent;")
        chat_lbl = QLabel(f"Chat: {TG_CHAT_ID}")
        chat_lbl.setFont(QFont("Segoe UI",10))
        chat_lbl.setStyleSheet(f"color:{C_DIM};background:transparent;")
        tg_lay.addWidget(bot_lbl); tg_lay.addWidget(token_lbl)
        tg_lay.addWidget(chat_lbl); tg_lay.addStretch()
        test_btn = QPushButton("📤  Test")
        test_btn.setFixedSize(90, 28)
        test_btn.clicked.connect(self._send_test)
        tg_lay.addWidget(test_btn)
        self.tg_status = QLabel("")
        self.tg_status.setFont(QFont("Segoe UI",10))
        self.tg_status.setStyleSheet(f"color:{C_GREEN};background:transparent;")
        tg_lay.addWidget(self.tg_status)
        left_lay.addWidget(tg_frame)

        # Toggle groups
        self._checkboxes = {}
        groups = [
            ("Panchak Levels", [
                ("tg_TOP_HIGH",       "🟢  TOP_HIGH Breaks"),
                ("tg_TOP_LOW",        "🔴  TOP_LOW Breaks"),
            ]),
            ("Scanners", [
                ("tg_OHL_BULL",       "🟢  OHL — Open = Low (Bullish)"),
                ("tg_OHL_BEAR",       "🔴  OHL — Open = High (Bearish)"),
                ("tg_DAILY_BREAK_UP", "⬆  Daily Breakout Up"),
                ("tg_DAILY_BREAK_DN", "⬇  Daily Breakout Down"),
            ]),
            ("SMC / OI", [
                ("tg_BOS_1H",         "📐  1H BOS / CHoCH Breakouts"),
                ("tg_OI_INTEL",       "📊  OI Intelligence Signals"),
            ]),
            ("Astro / KP", [
                ("tg_ASTRO",          "🌙  Astro Score Alerts"),
                ("tg_KP_ALERTS",      "🪐  KP Panchang Windows"),
            ]),
        ]

        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea{border:none;background:transparent;}")
        scroll_w = QWidget(); scroll_w.setStyleSheet("background:transparent;")
        scroll_lay = QVBoxLayout(scroll_w)
        scroll_lay.setSpacing(10); scroll_lay.setContentsMargins(0,0,4,0)

        for group_name, toggles in groups:
            gb = QGroupBox(group_name)
            gb.setStyleSheet(f"""
                QGroupBox {{
                    border: 1px solid {C_BORDER};
                    border-radius: 8px;
                    margin-top: 10px;
                    color: {C_BLUE};
                    font-size: 10px;
                    font-weight: 700;
                    letter-spacing: 0.5px;
                    background: {C_BG_CARD};
                    padding: 6px 4px 4px 4px;
                }}
                QGroupBox::title {{
                    subcontrol-origin: margin;
                    left: 10px;
                    padding: 0 6px;
                    color: {C_CYAN};
                }}
                QLabel {{ background: transparent; }}
                QCheckBox {{ background: transparent; }}
            """)
            gb_lay = QVBoxLayout(gb); gb_lay.setSpacing(6)
            for key, label in toggles:
                row = QHBoxLayout(); row.setSpacing(10)
                cb = QCheckBox(label)
                cb.setChecked(_alert_state.get(key, False))
                cb.setFont(QFont("Segoe UI", 11))
                cb.setStyleSheet(f"""
                    QCheckBox {{ color: {C_TEXT}; spacing: 8px; }}
                    QCheckBox::indicator {{
                        width:15px; height:15px;
                        border:1px solid {C_BORDER};
                        border-radius:3px;
                        background:#111826;
                    }}
                    QCheckBox::indicator:checked {{
                        background: {C_GREEN};
                        border-color: {C_GREEN};
                    }}
                """)
                cb.stateChanged.connect(lambda state, k=key: self._toggle(k, state))
                row.addWidget(cb)
                dot = QLabel("●")
                dot.setFont(QFont("Segoe UI", 12))
                dot.setStyleSheet(f"color:{C_GREEN if _alert_state.get(key,False) else C_DIM};background:transparent;")
                row.addWidget(dot); row.addStretch()
                self._checkboxes[key] = (cb, dot)
                gb_lay.addLayout(row)
            scroll_lay.addWidget(gb)

        scroll_lay.addStretch()
        scroll.setWidget(scroll_w)
        left_lay.addWidget(scroll)

        # Save/Reset
        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        save_btn  = QPushButton("💾  Save")
        reset_btn = QPushButton("↩  Defaults")
        save_btn.setFixedHeight(30); reset_btn.setFixedHeight(30)
        save_btn.clicked.connect(self._save); reset_btn.clicked.connect(self._reset)
        btn_row.addWidget(save_btn); btn_row.addWidget(reset_btn); btn_row.addStretch()
        left_lay.addLayout(btn_row)
        splitter.addWidget(left)

        # ══════════════════════════════════════════════════════
        # RIGHT — Alert log
        # ══════════════════════════════════════════════════════
        right = QWidget(); right.setStyleSheet("background:transparent;")
        right_lay = QVBoxLayout(right)
        right_lay.setContentsMargins(8, 0, 0, 0); right_lay.setSpacing(8)

        log_hdr = QHBoxLayout()
        log_hdr.addWidget(_section_title("Alert Log"))
        log_hdr.addStretch()
        clear_btn = QPushButton("Clear")
        clear_btn.setFixedSize(70, 26)
        clear_btn.clicked.connect(self._clear_log)
        log_hdr.addWidget(clear_btn)
        right_lay.addLayout(log_hdr)

        self.log_tbl = _make_table(["Time", "Category", "Details"])
        right_lay.addWidget(self.log_tbl)

        splitter.addWidget(right)
        splitter.setSizes([380, 600])
        root.addWidget(splitter)

        self._refresh_log()
        t = QTimer(self); t.timeout.connect(self._refresh_log); t.start(10000)

    def _toggle(self, key: str, state: int):
        enabled = bool(state)
        _alert_state[key] = enabled
        cb_pair = self._checkboxes.get(key)
        if cb_pair:
            _, dot = cb_pair
            dot.setText("●")
            dot.setStyleSheet(f"color:{C_GREEN if enabled else C_DIM};background:transparent;")

    def _save(self):
        try:
            open(ALERT_SETTINGS_FILE,"w").write(json.dumps(_alert_state))
            self.tg_status.setText("✅ Settings saved")
        except Exception as e:
            self.tg_status.setText(f"❌ Save failed: {e}")

    def _reset(self):
        global _alert_state
        _alert_state = dict(ALERT_TOGGLE_DEFAULTS)
        for key, (cb, dot) in self._checkboxes.items():
            cb.setChecked(_alert_state.get(key, False))
            dot.setStyleSheet(f"color: {C_GREEN if _alert_state.get(key,False) else C_DIM};")

    def _send_test(self):
        self.tg_status.setText("Sending…")
        def _do():
            ok = _tg_send(
                f"✅ <b>Test Message — Panchak Qt Dashboard</b>\n"
                f"⏰ {datetime.now().strftime('%H:%M:%S IST')}\n"
                f"All alert channels are working correctly."
            )
            self.tg_status.setText("✅ Sent!" if ok else "❌ Failed — check token/chat ID")
            _log_alert("TEST", "Test Telegram message sent")
            self._refresh_log()
        threading.Thread(target=_do, daemon=True).start()

    def _clear_log(self):
        global _alert_log
        _alert_log = []
        try: open(ALERT_LOG_FILE,"w").write("[]")
        except Exception: pass
        self._refresh_log()

    def _refresh_log(self):
        self.log_tbl.setRowCount(0)
        cat_colors = {
            "TOP_HIGH": C_GREEN, "TOP_LOW": C_RED,
            "OHL_BULL": C_GREEN, "OHL_BEAR": C_RED,
            "TEST":     C_BLUE,  "BOS":      C_PURPLE,
        }
        for entry in _alert_log[:80]:
            r = self.log_tbl.rowCount(); self.log_tbl.insertRow(r)
            cat = entry.get("category","")
            cc  = cat_colors.get(cat.upper().split("_")[0], C_DIM)
            for col, (text, color) in enumerate([
                (entry.get("time",""),     C_DIM),
                (cat,                      cc),
                (entry.get("message",""),  C_TEXT),
            ]):
                it = QTableWidgetItem(text); it.setForeground(QColor(color))
                self.log_tbl.setItem(r, col, it)
        self.log_tbl.resizeColumnsToContents()

    def load_settings(self):
        """Load saved alert settings on startup."""
        global _alert_state
        try:
            if os.path.exists(ALERT_SETTINGS_FILE):
                saved = json.loads(open(ALERT_SETTINGS_FILE).read())
                _alert_state.update(saved)
                for key, (cb, dot) in self._checkboxes.items():
                    cb.setChecked(_alert_state.get(key, False))
                    dot.setStyleSheet(f"color: {C_GREEN if _alert_state.get(key,False) else C_DIM};")
        except Exception: pass
