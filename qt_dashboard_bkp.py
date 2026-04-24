"""
qt_dashboard.py — Phase 1: PyQt6 Trading Dashboard
====================================================
Panels: BOS Scanner | Astro Score | Time Signal | DB Status
Reads from ohlc_1h.db (same folder). No Kite connection needed to view data.
Run: python qt_dashboard.py

Install deps once:
    pip install PyQt6 pytz

Optional (for full astro):
    pip install pyswisseph
"""

import sys
import os
import json
from datetime import datetime, date

# ── PyQt6 ────────────────────────────────────────────────────────────────────
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QComboBox, QDoubleSpinBox, QSplitter, QPushButton, QFrame,
    QScrollArea, QGridLayout, QStatusBar, QSizePolicy,
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QColor, QFont, QPalette, QBrush

# ── Your existing modules ────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

try:
    from ohlc_store import OHLCStore, is_market_hours
    DB_OK = True
except ImportError:
    DB_OK = False
    print("WARNING: ohlc_store.py not found in same folder")

try:
    from bos_scanner import detect_bos, build_bos_setup, load_scan_cache, run_bos_scan, BOS_LOOKBACK
    BOS_OK = True
except ImportError:
    BOS_OK = False
    print("WARNING: bos_scanner.py not found")

try:
    from astro_logic import get_astro_score, get_week_forecast
    ASTRO_OK = True
except ImportError:
    ASTRO_OK = False
    print("WARNING: astro_logic.py not found (astro panel will be limited)")

try:
    from astro_time import get_time_signal_detail, is_good_entry_time
    TIME_OK = True
except ImportError:
    TIME_OK = False
    print("WARNING: astro_time.py not found")


# ─────────────────────────────────────────────────────────────────────────────
# DARK THEME PALETTE
# ─────────────────────────────────────────────────────────────────────────────

DARK_QSS = """
QMainWindow, QWidget {
    background-color: #0d1117;
    color: #e6edf3;
    font-family: "Segoe UI", "Consolas", monospace;
}
QTabWidget::pane {
    border: 1px solid #30363d;
    background: #0d1117;
}
QTabBar::tab {
    background: #161b22;
    color: #8b949e;
    padding: 8px 18px;
    border: 1px solid #30363d;
    border-bottom: none;
    font-size: 12px;
    min-width: 100px;
}
QTabBar::tab:selected {
    background: #0d1117;
    color: #58a6ff;
    border-top: 2px solid #58a6ff;
}
QTabBar::tab:hover {
    color: #e6edf3;
    background: #21262d;
}
QTableWidget {
    background-color: #0d1117;
    gridline-color: #21262d;
    color: #e6edf3;
    border: none;
    font-size: 12px;
    selection-background-color: #1f6feb33;
}
QTableWidget::item {
    padding: 4px 8px;
    border-bottom: 1px solid #21262d;
}
QHeaderView::section {
    background-color: #161b22;
    color: #8b949e;
    padding: 6px 8px;
    border: none;
    border-bottom: 2px solid #30363d;
    font-size: 11px;
    font-weight: bold;
    text-transform: uppercase;
}
QComboBox {
    background: #21262d;
    color: #e6edf3;
    border: 1px solid #30363d;
    padding: 4px 8px;
    border-radius: 4px;
    min-width: 120px;
}
QComboBox::drop-down { border: none; }
QComboBox QAbstractItemView {
    background: #21262d;
    color: #e6edf3;
    selection-background-color: #1f6feb;
}
QDoubleSpinBox {
    background: #21262d;
    color: #e6edf3;
    border: 1px solid #30363d;
    padding: 4px 8px;
    border-radius: 4px;
}
QPushButton {
    background: #21262d;
    color: #58a6ff;
    border: 1px solid #30363d;
    padding: 6px 14px;
    border-radius: 5px;
    font-size: 12px;
}
QPushButton:hover {
    background: #1f6feb;
    color: #ffffff;
    border-color: #1f6feb;
}
QPushButton:pressed { background: #1158c7; }
QScrollArea { border: none; }
QLabel { color: #e6edf3; }
QStatusBar {
    background: #161b22;
    color: #8b949e;
    border-top: 1px solid #30363d;
    font-size: 11px;
}
QFrame[frameShape="4"] { color: #30363d; }
QSplitter::handle { background: #30363d; width: 1px; }
"""

# Color constants
C_GREEN      = "#3fb950"
C_RED        = "#f85149"
C_YELLOW     = "#d29922"
C_BLUE       = "#58a6ff"
C_PURPLE     = "#bc8cff"
C_DIM        = "#8b949e"
C_BG_CARD    = "#161b22"
C_BORDER     = "#30363d"
C_BG_BULL    = "#0d2819"
C_BG_BEAR    = "#2d0f0f"
C_BG_NEUTRAL = "#1c1c2e"


# ─────────────────────────────────────────────────────────────────────────────
# HELPER WIDGETS
# ─────────────────────────────────────────────────────────────────────────────

def make_label(text, size=12, bold=False, color=None, align=Qt.AlignmentFlag.AlignLeft):
    lbl = QLabel(text)
    font = QFont("Segoe UI", size)
    font.setBold(bold)
    lbl.setFont(font)
    if color:
        lbl.setStyleSheet(f"color: {color};")
    lbl.setAlignment(align)
    return lbl


def card_frame(layout_cls=QVBoxLayout):
    frame = QFrame()
    frame.setStyleSheet(f"""
        QFrame {{
            background: {C_BG_CARD};
            border: 1px solid {C_BORDER};
            border-radius: 6px;
        }}
    """)
    lay = layout_cls()
    lay.setContentsMargins(12, 10, 12, 10)
    lay.setSpacing(6)
    frame.setLayout(lay)
    return frame, lay


def color_item(text, color):
    item = QTableWidgetItem(str(text))
    item.setForeground(QColor(color))
    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
    return item


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND SCAN WORKER
# ─────────────────────────────────────────────────────────────────────────────

class BosWorker(QThread):
    """Runs BOS scan in background so UI stays responsive."""
    done = pyqtSignal(list)

    def __init__(self, db, symbols):
        super().__init__()
        self.db      = db
        self.symbols = symbols

    def run(self):
        results = []
        if not BOS_OK or not self.db:
            self.done.emit(results)
            return
        for symbol in self.symbols:
            try:
                candles = self.db.get(symbol, n=BOS_LOOKBACK)
                if not candles or len(candles) < 10:
                    continue
                try:
                    last_dt = datetime.strptime(candles[-1]["datetime"][:10], "%Y-%m-%d").date()
                    if last_dt < date.today():
                        continue
                except Exception:
                    pass
                bos = detect_bos(candles)
                if not bos["bos_type"]:
                    continue
                ltp   = candles[-1]["close"]
                setup = build_bos_setup(bos, symbol, ltp)
                results.append({"setup": setup, "bos": bos})
            except Exception:
                continue
        self.done.emit(results)


# ─────────────────────────────────────────────────────────────────────────────
# BOS SCANNER PANEL
# ─────────────────────────────────────────────────────────────────────────────

class BosPanel(QWidget):
    def __init__(self, db):
        super().__init__()
        self.db      = db
        self.symbols = []
        self.events  = []
        self.worker  = None
        self._build_ui()
        self._load_symbols()
        self._try_cache()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        # ── Header row ─────────────────────────────────────────
        hdr = QHBoxLayout()
        hdr.addWidget(make_label("📐  1-Hour BOS / CHoCH Scanner", 14, bold=True, color=C_BLUE))
        hdr.addStretch()
        self.scan_btn = QPushButton("⟳  Scan Now")
        self.scan_btn.setFixedWidth(120)
        self.scan_btn.clicked.connect(self.run_scan)
        hdr.addWidget(self.scan_btn)
        root.addLayout(hdr)

        # ── Stat pills ─────────────────────────────────────────
        stats_row = QHBoxLayout()
        self.lbl_total = self._pill("Events", "0", C_BLUE)
        self.lbl_bull  = self._pill("Bull 🚀", "0", C_GREEN)
        self.lbl_bear  = self._pill("Bear 💥", "0", C_RED)
        self.lbl_choch = self._pill("CHoCH 🔄", "0", C_PURPLE)
        self.lbl_scan_ts = make_label("Last scan: —", 10, color=C_DIM)
        for w in [self.lbl_total, self.lbl_bull, self.lbl_bear, self.lbl_choch]:
            stats_row.addWidget(w)
        stats_row.addStretch()
        stats_row.addWidget(self.lbl_scan_ts)
        root.addLayout(stats_row)

        # ── Filters ────────────────────────────────────────────
        filt_frame, filt_lay = card_frame(QHBoxLayout)
        filt_lay.setContentsMargins(8, 6, 8, 6)

        filt_lay.addWidget(make_label("Type:", 11, color=C_DIM))
        self.cmb_type = QComboBox()
        self.cmb_type.addItems(["All", "BOS only", "CHoCH only", "Bullish", "Bearish"])
        self.cmb_type.currentTextChanged.connect(self._apply_filters)
        filt_lay.addWidget(self.cmb_type)

        filt_lay.addWidget(make_label("  Min R:R:", 11, color=C_DIM))
        self.spin_rr = QDoubleSpinBox()
        self.spin_rr.setRange(0, 10); self.spin_rr.setValue(1.5); self.spin_rr.setSingleStep(0.5)
        self.spin_rr.valueChanged.connect(self._apply_filters)
        filt_lay.addWidget(self.spin_rr)

        filt_lay.addWidget(make_label("  Min Vol Ratio:", 11, color=C_DIM))
        self.spin_vol = QDoubleSpinBox()
        self.spin_vol.setRange(0, 10); self.spin_vol.setValue(1.2); self.spin_vol.setSingleStep(0.1)
        self.spin_vol.valueChanged.connect(self._apply_filters)
        filt_lay.addWidget(self.spin_vol)

        filt_lay.addStretch()
        root.addWidget(filt_frame)

        # ── Table ──────────────────────────────────────────────
        cols = ["Symbol", "Type", "LTP", "Broke", "Strength", "Vol×",
                "OB Zone", "Next Liq", "SL", "T1", "T2", "R:R now", "R:R retest", "Prior Trend"]
        self.table = QTableWidget(0, len(cols))
        self.table.setHorizontalHeaderLabels(cols)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(self.table.styleSheet() +
            "QTableWidget { alternate-background-color: #111820; }")
        root.addWidget(self.table)

        self.status_lbl = make_label("Loading scan cache…", 10, color=C_DIM)
        root.addWidget(self.status_lbl)

    def _pill(self, label, value, color):
        frame = QFrame()
        frame.setStyleSheet(f"""
            QFrame {{
                background: {color}22;
                border: 1px solid {color}66;
                border-radius: 4px;
                padding: 2px 8px;
            }}
        """)
        lay = QHBoxLayout(frame)
        lay.setContentsMargins(6, 2, 6, 2)
        lay.setSpacing(4)
        lbl_label = make_label(label, 10, color=C_DIM)
        lbl_value = make_label(value, 13, bold=True, color=color)
        lay.addWidget(lbl_label)
        lay.addWidget(lbl_value)
        frame._value_lbl = lbl_value
        return frame

    def _set_pill(self, pill, value):
        pill._value_lbl.setText(str(value))

    def _load_symbols(self):
        if not DB_OK or not self.db:
            self.status_lbl.setText("DB not available")
            return
        self.symbols = self.db.get_all_symbols()
        self.status_lbl.setText(f"{len(self.symbols)} symbols in DB")

    def _try_cache(self):
        """Load from bos_scan_cache.json first (instant)."""
        if not BOS_OK:
            self.status_lbl.setText("bos_scanner.py not found")
            return
        cache = load_scan_cache()
        if cache and cache.get("events"):
            self._load_from_cache(cache)
        else:
            self.status_lbl.setText("No cache — click Scan Now")

    def _load_from_cache(self, cache):
        events = cache.get("events", [])
        ts     = cache.get("time", "")
        self.events = events
        self.lbl_scan_ts.setText(f"Last scan: {ts}")
        self._apply_filters()
        self._update_stats(events)

    def _update_stats(self, events):
        bull  = sum(1 for e in events if "UP"    in e.get("bos_type", ""))
        bear  = sum(1 for e in events if "DOWN"  in e.get("bos_type", ""))
        choch = sum(1 for e in events if "CHOCH" in e.get("bos_type", ""))
        self._set_pill(self.lbl_total, len(events))
        self._set_pill(self.lbl_bull,  bull)
        self._set_pill(self.lbl_bear,  bear)
        self._set_pill(self.lbl_choch, choch)

    def _apply_filters(self):
        ftype   = self.cmb_type.currentText()
        min_rr  = self.spin_rr.value()
        min_vol = self.spin_vol.value()

        filtered = []
        for ev in self.events:
            bt  = ev.get("bos_type", "")
            rr  = max(ev.get("rr_now") or 0, ev.get("rr_retest") or 0)
            vol = ev.get("vol_ratio", 1.0)
            if ftype == "BOS only"   and "CHOCH" in bt: continue
            if ftype == "CHoCH only" and "CHOCH" not in bt: continue
            if ftype == "Bullish"    and "DOWN"  in bt: continue
            if ftype == "Bearish"    and "UP"    in bt: continue
            if rr < min_rr or vol < min_vol: continue
            filtered.append(ev)

        self._fill_table(filtered)

    def _fill_table(self, events):
        self.table.setRowCount(0)
        for ev in events:
            bt      = ev.get("bos_type", "")
            sym     = ev.get("symbol", "")
            ltp     = ev.get("ltp",    0)
            broken  = ev.get("broken", 0)
            sl      = ev.get("sl",     0)
            t1      = ev.get("t1",     0)
            t2      = ev.get("t2",     0)
            rr_now  = ev.get("rr_now", 0)
            rr_ret  = ev.get("rr_retest", 0)
            strength= ev.get("strength",  0)
            vol_r   = ev.get("vol_ratio", 1.0)
            ob_low  = ev.get("ob_low",  0)
            ob_high = ev.get("ob_high", 0)
            nxt_liq = ev.get("next_liq", 0)
            pt      = ev.get("prev_trend", "")

            is_bull = "UP" in bt
            is_choch= "CHOCH" in bt
            row_bg  = QColor(C_BG_BULL) if is_bull else QColor(C_BG_BEAR)

            icon = "🚀" if is_bull and not is_choch else ("🔄" if is_choch else "💥")
            r = self.table.rowCount()
            self.table.insertRow(r)

            type_color = C_GREEN if is_bull else C_RED
            if is_choch: type_color = C_PURPLE

            data = [
                (sym,                 C_BLUE),
                (f"{icon} {bt}",      type_color),
                (f"{ltp:.1f}",        "#e6edf3"),
                (f"{broken:.1f}",     C_YELLOW),
                (f"{strength:.2f}%",  C_DIM),
                (f"{vol_r:.1f}×",     C_YELLOW if vol_r >= 2 else "#e6edf3"),
                (f"{ob_low:.1f}–{ob_high:.1f}", C_DIM),
                (f"{nxt_liq:.1f}",    C_DIM),
                (f"{sl:.1f}",         C_RED),
                (f"{t1:.1f}",         C_GREEN),
                (f"{t2:.1f}",         C_GREEN),
                (f"{rr_now}:1",       C_GREEN if rr_now >= 2 else C_DIM),
                (f"{rr_ret}:1",       C_GREEN if rr_ret >= 2 else C_DIM),
                (pt,                  C_DIM),
            ]
            for col, (text, color) in enumerate(data):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item.setBackground(QBrush(row_bg))
                self.table.setItem(r, col, item)

        n = self.table.rowCount()
        self.status_lbl.setText(f"Showing {n} events (filtered)")
        self.table.resizeColumnsToContents()

    def run_scan(self):
        if not self.symbols:
            self.status_lbl.setText("No symbols in DB — load data first")
            return
        if self.worker and self.worker.isRunning():
            return
        self.scan_btn.setEnabled(False)
        self.scan_btn.setText("Scanning…")
        self.status_lbl.setText(f"Scanning {len(self.symbols)} symbols…")
        self.worker = BosWorker(self.db, self.symbols)
        self.worker.done.connect(self._on_scan_done)
        self.worker.start()

    def _on_scan_done(self, results):
        self.scan_btn.setEnabled(True)
        self.scan_btn.setText("⟳  Scan Now")
        ts = datetime.now().strftime("%H:%M:%S")
        self.lbl_scan_ts.setText(f"Last scan: {ts}")

        # Convert to the flat cache format that _apply_filters expects
        self.events = []
        for r in results:
            s = r["setup"]
            self.events.append({
                "symbol":     s["symbol"],
                "bos_type":   s["bos_type"],
                "ltp":        s["ltp"],
                "broken":     s["broken_level"],
                "sl":         s["sl"],
                "t1":         s["t1"],
                "t2":         s["t2"],
                "rr_now":     s["rr_now"],
                "rr_retest":  s["rr_retest"],
                "strength":   s["strength"],
                "vol_ratio":  s["volume_ratio"],
                "ob_low":     s["ob_low"],
                "ob_high":    s["ob_high"],
                "next_liq":   s["next_liq"],
                "prev_trend": s.get("prev_trend", ""),
                "alerted":    not s.get("already_alerted", False),
            })
        self._update_stats(self.events)
        self._apply_filters()


# ─────────────────────────────────────────────────────────────────────────────
# ASTRO PANEL
# ─────────────────────────────────────────────────────────────────────────────

class AstroPanel(QWidget):
    def __init__(self):
        super().__init__()
        self._build_ui()
        self._refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(10)

        # ── Header ────────────────────────────────────────────
        hdr = QHBoxLayout()
        hdr.addWidget(make_label("🌙  Vedic / KP Astro Score", 14, bold=True, color=C_BLUE))
        hdr.addStretch()
        btn = QPushButton("⟳  Refresh")
        btn.clicked.connect(self._refresh)
        btn.setFixedWidth(100)
        hdr.addWidget(btn)
        root.addLayout(hdr)

        # ── Today card ────────────────────────────────────────
        today_frame, today_lay = card_frame(QGridLayout)
        today_lay.setSpacing(10)

        self.lbl_signal  = make_label("—", 22, bold=True, color=C_YELLOW,
                                      align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_score   = make_label("Score: —", 16, bold=True, color=C_DIM,
                                      align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_nak     = make_label("Nakshatra: —", 12, color=C_DIM)
        self.lbl_sign    = make_label("Moon Sign: —", 12, color=C_DIM)
        self.lbl_lord    = make_label("Nak Lord: —", 12, color=C_DIM)
        self.lbl_sub     = make_label("KP Sub-lord: —", 12, color=C_DIM)
        self.lbl_reason  = make_label("—", 11, color=C_DIM)
        self.lbl_reason.setWordWrap(True)

        today_lay.addWidget(self.lbl_signal, 0, 0, 1, 2)
        today_lay.addWidget(self.lbl_score,  1, 0, 1, 2)
        today_lay.addWidget(self.lbl_nak,    2, 0)
        today_lay.addWidget(self.lbl_sign,   2, 1)
        today_lay.addWidget(self.lbl_lord,   3, 0)
        today_lay.addWidget(self.lbl_sub,    3, 1)
        today_lay.addWidget(self.lbl_reason, 4, 0, 1, 2)
        root.addWidget(today_frame)

        # ── Week forecast table ────────────────────────────────
        root.addWidget(make_label("📅  7-Day Forecast", 12, bold=True, color=C_BLUE))
        cols = ["Date", "Nakshatra", "Moon Sign", "Score", "Signal", "Reason"]
        self.week_table = QTableWidget(7, len(cols))
        self.week_table.setHorizontalHeaderLabels(cols)
        self.week_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.week_table.horizontalHeader().setStretchLastSection(True)
        self.week_table.verticalHeader().setVisible(False)
        self.week_table.setMaximumHeight(220)
        self.week_table.setAlternatingRowColors(True)
        self.week_table.setStyleSheet(self.week_table.styleSheet() +
            "QTableWidget { alternate-background-color: #111820; }")
        root.addWidget(self.week_table)
        root.addStretch()

    def _refresh(self):
        if not ASTRO_OK:
            self.lbl_signal.setText("⚠️ astro_logic.py not found")
            return
        try:
            r = get_astro_score()
            score   = r.get("score", 0)
            signal  = r.get("signal", "—")
            nak     = r.get("nakshatra", "—")
            sign    = r.get("moon_sign", "—")
            lord    = r.get("nak_lord", "—")
            sub     = r.get("sub_lord", "—")
            reason  = r.get("reason", "—")

            score_color = C_GREEN if score > 0 else (C_RED if score < 0 else C_YELLOW)
            sig_color   = C_GREEN if "UP" in signal or "BUY" in signal or "TREND" in signal else \
                          (C_RED if "SELL" in signal or "BEAR" in signal or "NO" in signal else C_YELLOW)

            self.lbl_signal.setText(signal)
            self.lbl_signal.setStyleSheet(f"color: {sig_color}; font-size: 20px; font-weight: bold;")
            self.lbl_score.setText(f"Score: {score:+d}")
            self.lbl_score.setStyleSheet(f"color: {score_color}; font-size: 15px; font-weight: bold;")
            self.lbl_nak.setText(f"🌙 Nakshatra:  {nak}")
            self.lbl_sign.setText(f"♈ Moon Sign: {sign}")
            self.lbl_lord.setText(f"👁 Nak Lord:   {lord}")
            self.lbl_sub.setText(f"🔮 KP Sub:     {sub}")
            self.lbl_reason.setText(f"📝 {reason}")

            self._fill_week()
        except Exception as e:
            self.lbl_signal.setText(f"Error: {e}")

    def _fill_week(self):
        try:
            forecast = get_week_forecast()
        except Exception:
            return
        self.week_table.setRowCount(len(forecast))
        for r_idx, day in enumerate(forecast):
            score  = day.get("score", 0)
            signal = day.get("signal", "")
            s_col  = C_GREEN if score > 0 else (C_RED if score < 0 else C_YELLOW)
            data   = [
                (day.get("date", ""), "#e6edf3"),
                (day.get("nakshatra", ""), C_DIM),
                (day.get("sign", ""), C_DIM),
                (f"{score:+d}", s_col),
                (signal, s_col),
                (day.get("reason", ""), C_DIM),
            ]
            for col, (text, color) in enumerate(data):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.week_table.setItem(r_idx, col, item)
        self.week_table.resizeColumnsToContents()


# ─────────────────────────────────────────────────────────────────────────────
# TIME SIGNAL PANEL (live clock)
# ─────────────────────────────────────────────────────────────────────────────

class TimePanel(QWidget):
    def __init__(self):
        super().__init__()
        self._build_ui()
        self._tick()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self._timer.start(5000)   # update every 5s

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(12)
        root.addWidget(make_label("⏰  Time-of-Day Trading Signal", 14, bold=True, color=C_BLUE))

        frame, lay = card_frame(QVBoxLayout)
        self.lbl_time    = make_label("—", 32, bold=True, color=C_YELLOW,
                                      align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_zone    = make_label("—", 20, bold=True,
                                      align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_desc    = make_label("—", 13, color=C_DIM,
                                      align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_mins    = make_label("—", 11, color=C_DIM,
                                      align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_good    = make_label("—", 12, bold=True,
                                      align=Qt.AlignmentFlag.AlignCenter)
        for w in [self.lbl_time, self.lbl_zone, self.lbl_desc, self.lbl_mins, self.lbl_good]:
            lay.addWidget(w)
        root.addWidget(frame)

        # Session timeline
        root.addWidget(make_label("📋  Session Guide", 12, bold=True, color=C_BLUE))
        sessions = [
            ("09:08–09:25", "🚧 Opening Risk",   "Gap opens — avoid immediate trades",         C_YELLOW),
            ("09:25–09:45", "⚡ Opening Range",  "First candle forming — observe only",        C_YELLOW),
            ("09:45–10:30", "🔥 Strong Trend",   "Best trending hour — trade with candle",     C_GREEN),
            ("10:30–11:30", "🟢 Momentum",       "Trail existing trades",                      C_GREEN),
            ("11:30–12:00", "🟡 Slow Zone",      "Reduce size, tighten stops",                 C_YELLOW),
            ("12:00–13:00", "🟡 Consolidation",  "Wait for breakout",                          C_YELLOW),
            ("13:00–13:45", "🚀 Afternoon Push", "Institutional accumulation",                 C_BLUE),
            ("13:45–14:30", "🚀 Breakout Zone",  "F&O expiry pressure — best breakouts",       C_BLUE),
            ("14:30–15:00", "⚡ Closing Rush",   "Hedging + squaring off",                     C_YELLOW),
            ("15:00–15:30", "⛔ Avoid Entry",    "Last 30 min — no new positions",             C_RED),
        ]
        guide_table = QTableWidget(len(sessions), 3)
        guide_table.setHorizontalHeaderLabels(["Time", "Zone", "Note"])
        guide_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        guide_table.horizontalHeader().setStretchLastSection(True)
        guide_table.verticalHeader().setVisible(False)
        guide_table.setMaximumHeight(300)
        for i, (t, z, n, c) in enumerate(sessions):
            for col, (text, clr) in enumerate([(t, C_DIM), (z, c), (n, C_DIM)]):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(clr))
                guide_table.setItem(i, col, item)
        root.addWidget(guide_table)
        root.addStretch()

    def _tick(self):
        if not TIME_OK:
            self.lbl_zone.setText("astro_time.py not found")
            return
        try:
            detail = get_time_signal_detail()
            sig    = detail.get("signal", "—")
            desc   = detail.get("description", "")
            mins   = detail.get("mins_left", 0)
            t_str  = detail.get("time_str", "")
            good   = is_good_entry_time()

            zone_color = C_GREEN if good else (C_RED if "AVOID" in sig or "CLOSED" in sig else C_YELLOW)
            good_text  = "✅ GOOD ENTRY TIME" if good else "⏸ WAIT / NO ENTRY"
            good_color = C_GREEN if good else C_RED

            self.lbl_time.setText(t_str)
            self.lbl_zone.setText(sig)
            self.lbl_zone.setStyleSheet(f"color: {zone_color}; font-size: 20px; font-weight: bold;")
            self.lbl_desc.setText(desc)
            self.lbl_mins.setText(f"{mins} min remaining in zone")
            self.lbl_good.setText(good_text)
            self.lbl_good.setStyleSheet(f"color: {good_color}; font-size: 13px; font-weight: bold;")
        except Exception as e:
            self.lbl_zone.setText(f"Error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# DB STATUS PANEL
# ─────────────────────────────────────────────────────────────────────────────

class DbPanel(QWidget):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self._build_ui()
        self._refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        hdr = QHBoxLayout()
        hdr.addWidget(make_label("📦  OHLC Database Status", 14, bold=True, color=C_BLUE))
        hdr.addStretch()
        btn = QPushButton("⟳  Refresh")
        btn.clicked.connect(self._refresh)
        btn.setFixedWidth(100)
        hdr.addWidget(btn)
        root.addLayout(hdr)

        # Summary pills
        pill_row = QHBoxLayout()
        self.pill_syms    = self._metric_card("Symbols", "—", C_BLUE)
        self.pill_candles = self._metric_card("Total Candles", "—", C_GREEN)
        self.pill_today   = self._metric_card("Updated Today", "—", C_YELLOW)
        self.pill_size    = self._metric_card("DB Size", "—", C_PURPLE)
        for p in [self.pill_syms, self.pill_candles, self.pill_today, self.pill_size]:
            pill_row.addWidget(p)
        root.addLayout(pill_row)

        # Per-symbol table
        root.addWidget(make_label("📋  Per-Symbol Status", 12, bold=True, color=C_BLUE))
        cols = ["Symbol", "Last Updated", "Last Candle", "Candles"]
        self.sym_table = QTableWidget(0, len(cols))
        self.sym_table.setHorizontalHeaderLabels(cols)
        self.sym_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.sym_table.horizontalHeader().setStretchLastSection(True)
        self.sym_table.verticalHeader().setVisible(False)
        self.sym_table.setAlternatingRowColors(True)
        self.sym_table.setStyleSheet(self.sym_table.styleSheet() +
            "QTableWidget { alternate-background-color: #111820; }")
        root.addWidget(self.sym_table)

    def _metric_card(self, label, value, color):
        frame, lay = card_frame(QVBoxLayout)
        frame.setFixedWidth(160)
        lbl_l = make_label(label, 10, color=C_DIM, align=Qt.AlignmentFlag.AlignCenter)
        lbl_v = make_label(value, 18, bold=True, color=color, align=Qt.AlignmentFlag.AlignCenter)
        lay.addWidget(lbl_l)
        lay.addWidget(lbl_v)
        frame._value = lbl_v
        return frame

    def _refresh(self):
        if not DB_OK or not self.db:
            return
        status = self.db.get_status()
        size   = self.db.get_db_size_mb()
        total  = len(status)
        n_can  = sum(s["total_candles"] for s in status)
        today  = date.today().isoformat()
        today_count = sum(1 for s in status if (s["last_updated"] or "")[:10] == today)

        self.pill_syms._value.setText(str(total))
        self.pill_candles._value.setText(f"{n_can:,}")
        self.pill_today._value.setText(str(today_count))
        self.pill_size._value.setText(f"{size} MB")

        self.sym_table.setRowCount(0)
        for row in status:
            r = self.sym_table.rowCount()
            self.sym_table.insertRow(r)
            lu = row["last_updated"] or "—"
            lc = row["last_candle_dt"] or "—"
            today_ok = lu[:10] == today
            sym_color = C_GREEN if today_ok else C_YELLOW
            data = [(row["symbol"], sym_color), (lu, C_DIM), (lc, C_DIM), (str(row["total_candles"]), "#e6edf3")]
            for col, (text, color) in enumerate(data):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.sym_table.setItem(r, col, item)
        self.sym_table.resizeColumnsToContents()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN WINDOW
# ─────────────────────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("📈  Panchak Trading Dashboard  — Phase 1")
        self.setMinimumSize(QSize(1200, 750))
        self.resize(1400, 850)
        self.setStyleSheet(DARK_QSS)

        # DB
        self.db = OHLCStore() if DB_OK else None

        # ── Header bar ────────────────────────────────────────
        header = QFrame()
        header.setStyleSheet(f"background: {C_BG_CARD}; border-bottom: 1px solid {C_BORDER};")
        hlay = QHBoxLayout(header)
        hlay.setContentsMargins(16, 8, 16, 8)

        self.lbl_title = make_label("📈  PANCHAK DASHBOARD", 15, bold=True, color=C_BLUE)
        self.lbl_clock = make_label("—", 13, bold=True, color=C_YELLOW)
        self.lbl_market= make_label("—", 12, color=C_DIM)

        hlay.addWidget(self.lbl_title)
        hlay.addStretch()
        hlay.addWidget(self.lbl_market)
        hlay.addWidget(make_label("  |  ", 12, color=C_BORDER))
        hlay.addWidget(self.lbl_clock)

        # ── Tabs ──────────────────────────────────────────────
        tabs = QTabWidget()
        self.bos_panel  = BosPanel(self.db)
        self.astro_panel= AstroPanel()
        self.time_panel = TimePanel()
        self.db_panel   = DbPanel(self.db)

        tabs.addTab(self.bos_panel,   "📐  BOS Scanner")
        tabs.addTab(self.astro_panel, "🌙  Astro Score")
        tabs.addTab(self.time_panel,  "⏰  Time Signal")
        tabs.addTab(self.db_panel,    "📦  DB Status")

        # ── Root layout ───────────────────────────────────────
        central = QWidget()
        root    = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        root.addWidget(header)
        root.addWidget(tabs)
        self.setCentralWidget(central)

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Phase 1 loaded — BOS Scanner | Astro | Time Signal | DB Status")

        # Clock timer
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._tick_clock)
        self._clock_timer.start(1000)
        self._tick_clock()

    def _tick_clock(self):
        import pytz
        try:
            IST = pytz.timezone("Asia/Kolkata")
            now = datetime.now(IST)
        except Exception:
            now = datetime.now()
        self.lbl_clock.setText(now.strftime("%H:%M:%S  IST  |  %a %d-%b-%Y"))
        market_open = is_market_hours() if DB_OK else False
        if market_open:
            self.lbl_market.setText("🟢 MARKET OPEN")
            self.lbl_market.setStyleSheet(f"color: {C_GREEN}; font-weight: bold;")
        else:
            self.lbl_market.setText("🔴 MARKET CLOSED")
            self.lbl_market.setStyleSheet(f"color: {C_RED};")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Panchak Trading Dashboard")

    # High-DPI scaling (PyQt6 handles this automatically, but be explicit)
    app.setStyle("Fusion")

    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
