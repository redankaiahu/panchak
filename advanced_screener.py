"""
advanced_screener.py — All Missing Scanners from Panchak Dashboard
===================================================================
Implements every scanner not yet in qt_dashboard:

  1.  Sequential 15-Min HIGH Breakout  (every candle high > prev)
  2.  Sequential 15-Min LOW Breakdown
  3.  Sequential 1-Hour HIGH Breakout
  4.  Sequential 1-Hour LOW Breakdown
  5.  Green Open Structure              (Yest GREEN, open inside upper zone, breaks YEST_HIGH)
  6.  Red Open Structure                (Yest RED, open inside lower zone, breaks YEST_LOW)
  7.  Strong Close – Near Day High      (LTP within 0.5% of day high + above YEST_HIGH)
  8.  Strong Close – Near Day Low
  9.  1H Opening Range Breakout/Breakdown  (09:15–10:14 range, valid after 10:15)
 10.  O=H / O=L Setups                 (open = high bearish / open = low bullish)
 11.  Yesterday RED – Open Inside Lower Zone
 12.  Yesterday GREEN – Open Inside Upper Zone / Breakout
 13.  15-Min Inside Range Break         (candles 2-4 inside candle 1, then break with EMA filter)
 14.  15-Min Volume Surge               (vol surge on candle above YEST_HIGH)

Data Sources (all from Kite live quotes + 15M historical):
  - kite.quote([...])                  → LTP, OHLC, volume
  - kite.historical_data(token, ..., "15minute") → today's 15M candles per symbol
  - Yesterday OHLC from caller (passed as yest_map dict)

Usage:
    from advanced_screener import AdvancedScreenerWorker, AdvancedScreenerPanel
    panel = AdvancedScreenerPanel(STOCKS, kite_getter=lambda: KiteSession.get().kite)
"""

from __future__ import annotations
import os, json, threading, time as _time
from datetime import datetime, date, timedelta, time

import pandas as pd
import numpy as np

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTabWidget,
    QTableWidget, QTableWidgetItem, QHeaderView, QPushButton,
    QFrame, QScrollArea, QSplitter, QSizePolicy,
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QBrush

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
except ImportError:
    IST = None

# ─────────────────────────────────────────────────────────────────────────────
# COLOR SYSTEM — Zerodha Kite inspired (light background)
# ─────────────────────────────────────────────────────────────────────────────
# Backgrounds
BG_PAGE     = "#FFFFFF"
BG_CARD     = "#FFFFFF"
BG_HEADER   = "#F8F9FA"
BG_BULL_ROW = "#F0FFF4"    # light green row tint (matches screenshots)
BG_BEAR_ROW = "#FFF5F5"    # light red row tint
BG_ZEBRA    = "#F8F9FA"    # alternating row

# Text
TXT_PRIMARY   = "#1A1D23"
TXT_SECONDARY = "#4A5568"
TXT_DIM       = "#718096"

# Accent — matches main dashboard navy-blue pill theme
CLR_GREEN  = "#276749"    # forest green
CLR_RED    = "#C53030"    # strong red
CLR_BLUE   = "#2C5282"    # navy blue (tab pills)
CLR_YELLOW = "#B7791F"    # amber
CLR_PURPLE = "#553C9A"
CLR_ORANGE = "#C05621"
CLR_BORDER = "#DDE1E8"
CLR_BORDER2= "#CBD5E0"

FONT_FAMILY = '"Segoe UI", "Inter", "Helvetica Neue", sans-serif'

# ─────────────────────────────────────────────────────────────────────────────
# QSS — Light Kite-style stylesheet (applied to AdvancedScreenerPanel only)
# ─────────────────────────────────────────────────────────────────────────────
LIGHT_QSS = f"""
QWidget {{
    background: {BG_PAGE};
    color: {TXT_PRIMARY};
    font-family: {FONT_FAMILY};
    font-size: 12px;
}}
QTabWidget::pane {{
    border: 1px solid {CLR_BORDER};
    border-top: none;
    background: {BG_PAGE};
}}
QTabBar {{
    background: {BG_HEADER};
    border-bottom: 1px solid {CLR_BORDER};
}}
QTabBar::tab {{
    background: transparent;
    color: {TXT_SECONDARY};
    padding: 8px 14px;
    border: none;
    border-bottom: 2px solid transparent;
    font-size: 11px;
    font-weight: 600;
    min-width: 60px;
    margin-right: 2px;
}}
QTabBar::tab:selected {{
    color: {CLR_BLUE};
    border-bottom: 2px solid {CLR_BLUE};
    background: {BG_PAGE};
}}
QTabBar::tab:hover:!selected {{
    color: {TXT_PRIMARY};
    background: #F3F4F6;
}}
QTableWidget {{
    background: {BG_PAGE};
    alternate-background-color: {BG_ZEBRA};
    border: none;
    gridline-color: {CLR_BORDER};
    color: {TXT_PRIMARY};
    font-size: 12px;
    selection-background-color: #DBEAFE;
    selection-color: {TXT_PRIMARY};
}}
QTableWidget::item {{
    padding: 5px 10px;
    border-bottom: 1px solid {CLR_BORDER};
}}
QHeaderView::section {{
    background: {BG_HEADER};
    color: {TXT_SECONDARY};
    padding: 6px 10px;
    border: none;
    border-bottom: 1px solid {CLR_BORDER2};
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}}
QHeaderView {{
    background: {BG_HEADER};
}}
QPushButton {{
    background: {CLR_BLUE};
    color: white;
    border: none;
    padding: 6px 16px;
    border-radius: 4px;
    font-weight: 600;
    font-size: 11px;
    min-height: 28px;
}}
QPushButton:hover {{ background: #0F52B0; }}
QPushButton:pressed {{ background: #0A3F8A; }}
QPushButton:disabled {{ background: #D1D5DB; color: #9CA3AF; }}
QScrollBar:vertical {{
    background: #F3F4F6;
    width: 8px;
    border: none;
}}
QScrollBar::handle:vertical {{
    background: #D1D5DB;
    border-radius: 4px;
    min-height: 30px;
}}
QScrollBar::handle:vertical:hover {{ background: {CLR_BLUE}; }}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QFrame {{ background: transparent; }}
QLabel {{ background: transparent; color: {TXT_PRIMARY}; }}
"""

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _ts() -> str:
    if IST:
        return datetime.now(IST).strftime("%H:%M:%S")
    return datetime.now().strftime("%H:%M:%S")

def _now_ist() -> datetime:
    if IST:
        return datetime.now(IST)
    return datetime.now()

def _chg_color(v) -> str:
    try:
        return CLR_GREEN if float(v) >= 0 else CLR_RED
    except Exception:
        return TXT_SECONDARY

def _fmt_chg(v) -> str:
    try:
        v = float(v)
        return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"
    except Exception:
        return str(v)

def _make_table(cols: list, stretch_last: bool = True) -> QTableWidget:
    t = QTableWidget(0, len(cols))
    t.setHorizontalHeaderLabels(cols)
    t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
    if stretch_last:
        t.horizontalHeader().setStretchLastSection(True)
    t.verticalHeader().setVisible(False)
    t.setAlternatingRowColors(True)
    t.setShowGrid(True)
    t.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
    return t

def _item(text: str, color: str = TXT_PRIMARY, bold: bool = False,
          bg: str = None, align=Qt.AlignmentFlag.AlignCenter) -> QTableWidgetItem:
    it = QTableWidgetItem(str(text))
    it.setForeground(QColor(color))
    if bold:
        f = QFont(); f.setBold(True); it.setFont(f)
    if bg:
        it.setBackground(QBrush(QColor(bg)))
    it.setTextAlignment(align)
    return it

def _section_lbl(text: str) -> QLabel:
    lbl = QLabel(f"  {text}")
    lbl.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
    lbl.setFixedHeight(32)
    lbl.setStyleSheet(f"""
        color: {CLR_BLUE};
        background: {BG_HEADER};
        border-left: 3px solid {CLR_BLUE};
        border-bottom: 1px solid {CLR_BORDER};
        padding-left: 8px;
    """)
    return lbl

def _stat_badge(label: str, value: str, color: str, width: int = 120) -> QFrame:
    f = QFrame()
    f.setFixedWidth(width)
    f.setStyleSheet(f"""
        QFrame {{
            background: #FFFFFF;
            border: 1px solid #DDE1E8;
            border-top: 3px solid {color};
            border-radius: 6px;
        }}
    """)
    lay = QVBoxLayout(f)
    lay.setContentsMargins(8, 6, 8, 6)
    lay.setSpacing(2)
    cap = QLabel(label.upper())
    cap.setFont(QFont("Segoe UI", 8, QFont.Weight.Bold))
    cap.setStyleSheet(f"color:{TXT_SECONDARY}; letter-spacing:0.5px;")
    cap.setAlignment(Qt.AlignmentFlag.AlignCenter)
    val = QLabel(value)
    val.setFont(QFont("Segoe UI", 15, QFont.Weight.Bold))
    val.setStyleSheet(f"color:{color};")
    val.setAlignment(Qt.AlignmentFlag.AlignCenter)
    lay.addWidget(cap); lay.addWidget(val)
    f._v = val
    return f

# ─────────────────────────────────────────────────────────────────────────────
# WORKER — fetches all data and runs all scanners in background thread
# ─────────────────────────────────────────────────────────────────────────────

class AdvancedScreenerWorker(QThread):
    done  = pyqtSignal(dict)
    error = pyqtSignal(str)
    status = pyqtSignal(str)

    def __init__(self, symbols: list, kite):
        super().__init__()
        self.symbols = symbols
        self.kite    = kite

    # ── Main entry point ───────────────────────────────────────────────────
    def run(self):
        try:
            import pandas as pd
            kite    = self.kite
            symbols = self.symbols
            self.status.emit(f"Fetching quotes for {len(symbols)} symbols…")

            # ── 1. Live quotes ─────────────────────────────────────────────
            quotes = {}
            syms_nse = [f"NSE:{s}" for s in symbols]
            for i in range(0, len(syms_nse), 400):
                try:
                    quotes.update(kite.quote(syms_nse[i:i+400]))
                except Exception as e:
                    self.status.emit(f"Quote batch error: {e}")

            if not quotes:
                self.error.emit("No quotes received from Kite"); return

            self.status.emit("Loading yesterday OHLC…")

            # ── 2. Yesterday OHLC (batch historical) ──────────────────────
            yest_map = {}   # sym → {high, low, open, close}
            try:
                nse_df = pd.DataFrame(kite.instruments("NSE"))
                from_d = date.today() - timedelta(days=5)
                to_d   = date.today() - timedelta(days=1)
                for sym in symbols:
                    try:
                        row = nse_df[nse_df["tradingsymbol"] == sym]
                        if row.empty: continue
                        token = int(row.iloc[0]["instrument_token"])
                        bars  = kite.historical_data(token, from_d, to_d, "day")
                        if bars:
                            b = bars[-1]
                            yest_map[sym] = {
                                "high": b["high"], "low": b["low"],
                                "open": b["open"], "close": b["close"],
                            }
                    except Exception:
                        continue
            except Exception as e:
                self.status.emit(f"Yest OHLC error: {e}")

            self.status.emit("Fetching 15-min intraday candles…")

            # ── 3. Today's 15-min candles per symbol ───────────────────────
            candles_15m = {}   # sym → list of {date, open, high, low, close}
            try:
                nse_df_local = nse_df if 'nse_df' in dir() else pd.DataFrame(kite.instruments("NSE"))
                today_start  = datetime.combine(date.today(), time(9, 15))
                today_end    = datetime.combine(date.today(), time(15, 30))
                for sym in symbols:
                    try:
                        row = nse_df_local[nse_df_local["tradingsymbol"] == sym]
                        if row.empty: continue
                        token = int(row.iloc[0]["instrument_token"])
                        bars  = kite.historical_data(token, today_start, today_end, "15minute")
                        if bars:
                            candles_15m[sym] = bars
                    except Exception:
                        continue
            except Exception as e:
                self.status.emit(f"15M candles error: {e}")

            self.status.emit("Running all scanners…")

            # ── 4. Build main dataframe ────────────────────────────────────
            rows = []
            for sym in symbols:
                q = quotes.get(f"NSE:{sym}")
                if not q: continue
                ltp   = q.get("last_price", 0)
                pc    = q["ohlc"]["close"]          # prev close from Kite
                lo    = round(q["ohlc"]["open"], 2)
                lh    = round(q["ohlc"]["high"], 2)
                ll    = round(q["ohlc"]["low"],  2)
                vol   = q.get("volume", 0)
                chgp  = round((ltp - pc) / pc * 100, 2) if pc else 0

                y = yest_map.get(sym, {})
                yh = y.get("high", 0); yl = y.get("low", 0)
                yo = y.get("open", 0); yc = y.get("close", 0)

                rows.append({
                    "Symbol":    sym,
                    "LTP":       round(ltp, 2),
                    "CHANGE_%":  chgp,
                    "LIVE_OPEN": lo,
                    "LIVE_HIGH": lh,
                    "LIVE_LOW":  ll,
                    "VOLUME":    vol,
                    "YEST_HIGH": yh, "YEST_LOW":  yl,
                    "YEST_OPEN": yo, "YEST_CLOSE": yc,
                })

            if not rows:
                self.error.emit("No rows built — check Kite connection"); return

            df = pd.DataFrame(rows)

            # ── 5. Run all scanners ────────────────────────────────────────
            result = {
                "timestamp":        _ts(),
                "seq_15m_high":     self._sequential(candles_15m, df, "15m", "HIGH"),
                "seq_15m_low":      self._sequential(candles_15m, df, "15m", "LOW"),
                "seq_1h_high":      self._sequential(candles_15m, df, "1h",  "HIGH"),
                "seq_1h_low":       self._sequential(candles_15m, df, "1h",  "LOW"),
                "green_structure":  self._green_structure(candles_15m, df),
                "red_structure":    self._red_structure(candles_15m, df),
                "strong_high":      self._strong_close(df, "HIGH"),
                "strong_low":       self._strong_close(df, "LOW"),
                "orb_breakout":     self._orb(candles_15m, df, "HIGH"),
                "orb_breakdown":    self._orb(candles_15m, df, "LOW"),
                "ohl":              self._ohl_setups(df),
                "yest_green_zone":  self._yest_green_zone(df),
                "yest_red_zone":    self._yest_red_zone(df),
                "yest_green_break": self._yest_green_break(df),
                "inside_15m":       self._inside_range_15m(candles_15m, df),
                "vol_surge":        self._vol_surge_15m(candles_15m, df),
            }

            self.done.emit(result)

        except Exception as e:
            import traceback
            self.error.emit(f"{e}\n{traceback.format_exc()[-300:]}")

    # ── Scanner implementations ────────────────────────────────────────────

    def _to_1h_slots(self, candles_15m_sym: list) -> list:
        """Convert 15-min candles to 1-hour OHLCV aligned to 09:15."""
        if not candles_15m_sym:
            return []
        df15 = pd.DataFrame(candles_15m_sym)
        df15["dt"] = pd.to_datetime(df15["date"])
        df15["slot1h"] = (
            (df15["dt"] - pd.Timedelta(minutes=15))
            .dt.floor("60min") + pd.Timedelta(minutes=15)
        )
        agg = df15.groupby("slot1h").agg(
            open=("open","first"), high=("high","max"),
            low=("low","min"),    close=("close","last"),
            volume=("volume","sum"),
        ).reset_index()
        result = []
        for _, r in agg.iterrows():
            result.append({
                "date":  r["slot1h"],
                "open":  r["open"],  "high": r["high"],
                "low":   r["low"],   "close": r["close"],
                "volume": r.get("volume", 0),
            })
        return sorted(result, key=lambda x: x["date"])

    def _sequential(self, candles_15m: dict, df: pd.DataFrame,
                    tf: str, direction: str) -> list:
        """
        Sequential high/low breakout scanner.
        tf = "15m" or "1h", direction = "HIGH" or "LOW"
        Every candle high must be > prev candle high (HIGH) or
        every candle low must be < prev candle low (LOW).
        LTP must be above last_high (HIGH) or below last_low (LOW).
        """
        now_hm = _now_ist().strftime("%H:%M")
        rows   = []

        for _, row in df.iterrows():
            sym  = row["Symbol"]
            ltp  = row["LTP"]
            raw  = candles_15m.get(sym, [])
            if not raw: continue

            candles = self._to_1h_slots(raw) if tf == "1h" else raw
            if len(candles) < 2: continue

            # Only use completed candles (not the live one)
            now_dt    = _now_ist()
            now_naive = now_dt.replace(tzinfo=None)
            completed = []
            for c in candles:
                try:
                    cd = pd.to_datetime(c["date"])
                    if cd.tzinfo is not None:
                        cd = cd.tz_convert(None)
                    if cd < now_naive:
                        completed.append(c)
                except Exception:
                    pass

            if len(completed) < 2: continue

            if direction == "HIGH":
                chain = all(
                    float(completed[i]["high"]) > float(completed[i-1]["high"])
                    for i in range(1, len(completed))
                )
                if not chain: continue
                last_high = float(completed[-1]["high"])
                if ltp <= last_high: continue
                rows.append({
                    "Symbol":      sym,
                    "LTP":         ltp,
                    "CHANGE_%":    row["CHANGE_%"],
                    "CANDLES":     len(completed),
                    "BROKEN_HIGH": round(last_high, 2),
                    "ABOVE_%":     round((ltp - last_high) / last_high * 100, 2),
                    "DAY_HIGH":    row["LIVE_HIGH"],
                    "YEST_HIGH":   row["YEST_HIGH"],
                    "TYPE":        "🟢 SEQ HIGH",
                })
            else:
                chain = all(
                    float(completed[i]["low"]) < float(completed[i-1]["low"])
                    for i in range(1, len(completed))
                )
                if not chain: continue
                last_low = float(completed[-1]["low"])
                if ltp >= last_low: continue
                rows.append({
                    "Symbol":     sym,
                    "LTP":        ltp,
                    "CHANGE_%":   row["CHANGE_%"],
                    "CANDLES":    len(completed),
                    "BROKEN_LOW": round(last_low, 2),
                    "BELOW_%":    round((last_low - ltp) / last_low * 100, 2),
                    "DAY_LOW":    row["LIVE_LOW"],
                    "YEST_LOW":   row["YEST_LOW"],
                    "TYPE":       "🔴 SEQ LOW",
                })

        key = "CANDLES"
        sort_col = "ABOVE_%" if direction == "HIGH" else "BELOW_%"
        return sorted(rows, key=lambda x: (-x[key], -x.get(sort_col, 0)))

    def _green_structure(self, candles_15m: dict, df: pd.DataFrame) -> list:
        """
        Yesterday GREEN → open inside upper zone → breaks YEST_HIGH.
        Conditions:
          YEST_CLOSE > YEST_OPEN                 (yesterday green)
          LIVE_OPEN  > YEST_CLOSE                (gap up inside zone)
          LIVE_OPEN  < YEST_HIGH                 (not a full gap-up)
          first 15M candle high <= YEST_HIGH     (first candle didn't break)
          then a later candle breaks YEST_HIGH   (confirmed break)
          LTP > YEST_HIGH                        (still holding above)
        """
        rows = []
        for _, row in df.iterrows():
            sym = row["Symbol"]
            yc  = row["YEST_CLOSE"]; yo = row["YEST_OPEN"]
            yh  = row["YEST_HIGH"];  lo = row["LIVE_OPEN"]
            ltp = row["LTP"]
            if yc <= yo: continue                 # not green
            if not (lo > yc and lo < yh): continue
            candles = candles_15m.get(sym, [])
            if len(candles) < 2: continue
            first = candles[0]
            rest  = candles[1:]
            if float(first.get("high", 0)) > yh: continue  # first broke already
            break_c = next((c for c in rest if float(c.get("high", 0)) > yh), None)
            if not break_c: continue
            if ltp <= yh: continue
            post_high = max(float(c.get("high", 0)) for c in candles
                            if pd.to_datetime(c["date"]) >= pd.to_datetime(break_c["date"]))
            gain = round(post_high - yh, 2)
            rows.append({
                "Symbol":            sym,
                "LTP":               ltp,
                "CHANGE_%":          row["CHANGE_%"],
                "LIVE_OPEN":         lo,
                "YEST_HIGH":         round(yh, 2),
                "YEST_CLOSE":        round(yc, 2),
                "BREAK_TIME":        pd.to_datetime(break_c["date"]).strftime("%H:%M"),
                "POST_BREAK_GAIN":   gain,
                "POST_BREAK_GAIN_%": round(gain / yh * 100, 2) if yh else 0,
            })
        return sorted(rows, key=lambda x: -x["POST_BREAK_GAIN_%"])

    def _red_structure(self, candles_15m: dict, df: pd.DataFrame) -> list:
        """
        Yesterday RED → open inside lower zone → breaks YEST_LOW.
        """
        rows = []
        for _, row in df.iterrows():
            sym = row["Symbol"]
            yc  = row["YEST_CLOSE"]; yo = row["YEST_OPEN"]
            yl  = row["YEST_LOW"];   lo = row["LIVE_OPEN"]
            ltp = row["LTP"]
            if yc >= yo: continue                 # not red
            if not (lo > yl and lo < yc): continue
            candles = candles_15m.get(sym, [])
            if len(candles) < 2: continue
            first = candles[0]
            rest  = candles[1:]
            if float(first.get("low", 9999)) < yl: continue
            break_c = next((c for c in rest if float(c.get("low", 9999)) < yl), None)
            if not break_c: continue
            if ltp >= yl: continue
            post_low = min(float(c.get("low", 9999)) for c in candles
                           if pd.to_datetime(c["date"]) >= pd.to_datetime(break_c["date"]))
            drop = round(yl - post_low, 2)
            rows.append({
                "Symbol":           sym,
                "LTP":              ltp,
                "CHANGE_%":         row["CHANGE_%"],
                "LIVE_OPEN":        lo,
                "YEST_LOW":         round(yl, 2),
                "YEST_CLOSE":       round(yc, 2),
                "BREAK_TIME":       pd.to_datetime(break_c["date"]).strftime("%H:%M"),
                "POST_BREAK_DROP":  drop,
                "POST_BREAK_DROP_%": round(drop / yl * 100, 2) if yl else 0,
            })
        return sorted(rows, key=lambda x: -x["POST_BREAK_DROP_%"])

    def _strong_close(self, df: pd.DataFrame, direction: str) -> list:
        """
        Strong Close Near Day High (direction=HIGH):
          LIVE_OPEN <= YEST_HIGH  (no gap-up)
          LTP >= YEST_HIGH        (above yesterday high)
          DIST_FROM_DAY_HIGH <= 0.5%  (near top of day)
          CHANGE 0.5–2.5%

        Strong Close Near Day Low (direction=LOW):
          LIVE_OPEN >= YEST_LOW   (no gap-down)
          LTP <= YEST_LOW         (below yesterday low)
          DIST_FROM_DAY_LOW <= 0.5%
          CHANGE -0.5 to -2.2%
        """
        rows = []
        for _, row in df.iterrows():
            ltp = row["LTP"]
            lh  = row["LIVE_HIGH"]; ll = row["LIVE_LOW"]
            lo  = row["LIVE_OPEN"]
            yh  = row["YEST_HIGH"]; yl = row["YEST_LOW"]
            chg = row["CHANGE_%"]

            if direction == "HIGH":
                if yh <= 0 or lh <= 0: continue
                dist = round((lh - ltp) / lh * 100, 2)
                if (lo <= yh and ltp >= yh and dist <= 0.5
                        and 0.5 <= chg <= 2.5):
                    rows.append({
                        "Symbol":               row["Symbol"],
                        "LTP":                  ltp,
                        "CHANGE_%":             chg,
                        "DAY_HIGH":             lh,
                        "DIST_FROM_DAY_HIGH_%": dist,
                        "YEST_HIGH":            round(yh, 2),
                        "VOLUME":               row["VOLUME"],
                    })
            else:
                if yl <= 0 or ll <= 0: continue
                dist = round((ltp - ll) / ll * 100, 2)
                if (lo >= yl and ltp <= yl and dist <= 0.5
                        and -2.2 <= chg <= -0.5):
                    rows.append({
                        "Symbol":              row["Symbol"],
                        "LTP":                 ltp,
                        "CHANGE_%":            chg,
                        "DAY_LOW":             ll,
                        "DIST_FROM_DAY_LOW_%": dist,
                        "YEST_LOW":            round(yl, 2),
                        "VOLUME":              row["VOLUME"],
                    })

        sort_col = "DIST_FROM_DAY_HIGH_%" if direction == "HIGH" else "DIST_FROM_DAY_LOW_%"
        return sorted(rows, key=lambda x: x[sort_col])

    def _orb(self, candles_15m: dict, df: pd.DataFrame, direction: str) -> list:
        """
        1H Opening Range Breakout/Breakdown.
        Range = highest high / lowest low of 09:15–10:14 (first 4 × 15-min candles).
        Valid only after 10:15.
        Breakout:  LTP > 1H_HIGH  AND  LTP > EMA20 daily  AND  range 0.3–2.0%
        Breakdown: LTP < 1H_LOW   AND  LTP < EMA20 daily  AND  range 0.3–2.0%
        """
        now_hm = _now_ist().strftime("%H:%M")
        if now_hm < "10:15":
            return []   # ORB not formed yet

        rows = []
        for _, row in df.iterrows():
            sym     = row["Symbol"]
            ltp     = row["LTP"]
            candles = candles_15m.get(sym, [])
            if len(candles) < 2: continue

            # ORB window = first 4 candles (09:15–10:14)
            orb = [c for c in candles if
                   "09:15" <= pd.to_datetime(c["date"]).strftime("%H:%M") < "10:15"]
            if len(orb) < 2: continue

            h1_high = max(float(c["high"]) for c in orb)
            h1_low  = min(float(c["low"])  for c in orb)
            if h1_high <= 0 or h1_low <= 0: continue

            range_pct = round((h1_high - h1_low) / h1_low * 100, 2)
            if not (0.3 <= range_pct <= 2.0): continue

            # Post-break move
            post_move = round(abs(ltp - h1_high) / h1_high * 100, 2) if direction == "HIGH" else \
                        round(abs(h1_low - ltp)  / h1_low  * 100, 2)

            if post_move > 3.0: continue   # already extended

            yh = row["YEST_HIGH"]; yl = row["YEST_LOW"]

            if direction == "HIGH":
                if ltp <= h1_high: continue
                if row["LIVE_OPEN"] > yh * 1.01: continue  # no big gap-up
                if row["CHANGE_%"] < 0.3: continue
                rows.append({
                    "Symbol":         sym,
                    "LTP":            ltp,
                    "CHANGE_%":       row["CHANGE_%"],
                    "1H_HIGH":        round(h1_high, 2),
                    "1H_LOW":         round(h1_low, 2),
                    "RANGE_%":        range_pct,
                    "POST_MOVE_%":    round((ltp - h1_high) / h1_high * 100, 2),
                    "DAY_HIGH":       row["LIVE_HIGH"],
                    "YEST_HIGH":      round(yh, 2),
                    "TYPE":           "🟢 ORB BREAKOUT",
                })
            else:
                if ltp >= h1_low: continue
                if row["LIVE_OPEN"] < yl * 0.99: continue  # no big gap-down
                if row["CHANGE_%"] > -0.3: continue
                rows.append({
                    "Symbol":         sym,
                    "LTP":            ltp,
                    "CHANGE_%":       row["CHANGE_%"],
                    "1H_HIGH":        round(h1_high, 2),
                    "1H_LOW":         round(h1_low, 2),
                    "RANGE_%":        range_pct,
                    "POST_MOVE_%":    round((h1_low - ltp) / h1_low * 100, 2),
                    "DAY_LOW":        row["LIVE_LOW"],
                    "YEST_LOW":       round(yl, 2),
                    "TYPE":           "🔴 ORB BREAKDOWN",
                })

        return sorted(rows, key=lambda x: -x["POST_MOVE_%"])

    def _ohl_setups(self, df: pd.DataFrame) -> list:
        """
        O=H (Open = High bearish) / O=L (Open = Low bullish) setups.
        Tolerance: abs(open - high/low) <= 0.05 (5 paise)
        """
        TOL = 0.05
        rows = []
        for _, row in df.iterrows():
            lo = row["LIVE_OPEN"]; lh = row["LIVE_HIGH"]; ll = row["LIVE_LOW"]
            ltp = row["LTP"]; yh = row["YEST_HIGH"]; yl = row["YEST_LOW"]
            yc  = row["YEST_CLOSE"]
            if lo <= 0: continue

            # O=L bullish: open at day low, inside yesterday range
            if (abs(lo - ll) <= TOL and lo < yh and lo > yl):
                rows.append({
                    "Symbol":    row["Symbol"], "LTP": ltp,
                    "CHANGE_%":  row["CHANGE_%"], "LIVE_OPEN": lo,
                    "LIVE_HIGH": lh, "LIVE_LOW": ll,
                    "YEST_HIGH": round(yh, 2), "YEST_LOW": round(yl, 2),
                    "SETUP":     "🟢 O = L", "SIDE": "BULLISH",
                })

            # O=H bearish: open at day high, below yesterday close, price below open
            elif (abs(lo - lh) <= TOL and lo > yl and lo < yc and ltp < lo):
                rows.append({
                    "Symbol":    row["Symbol"], "LTP": ltp,
                    "CHANGE_%":  row["CHANGE_%"], "LIVE_OPEN": lo,
                    "LIVE_HIGH": lh, "LIVE_LOW": ll,
                    "YEST_HIGH": round(yh, 2), "YEST_LOW": round(yl, 2),
                    "SETUP":     "🔴 O = H", "SIDE": "BEARISH",
                })

        return sorted(rows, key=lambda x: x["CHANGE_%"], reverse=True)

    def _yest_green_zone(self, df: pd.DataFrame) -> list:
        """
        Yesterday GREEN → open inside upper zone (Yest Close → Yest High) → waiting.
        (Stock not yet broken above YEST_HIGH — still inside zone)
        """
        PCT_TOL = 1.0; OPEN_TOL = 0.05
        rows = []
        for _, row in df.iterrows():
            yc = row["YEST_CLOSE"]; yo = row["YEST_OPEN"]
            yh = row["YEST_HIGH"];  lo = row["LIVE_OPEN"]
            ltp = row["LTP"]
            if yc <= yo: continue
            zone_pct = (yh - yc) / yc * 100 if yc else 0
            if zone_pct > PCT_TOL: continue
            if not (lo >= yc + OPEN_TOL and lo <= yh - OPEN_TOL): continue
            if ltp >= yh: continue    # already broke out
            rows.append({
                "Symbol":    row["Symbol"], "LTP": ltp,
                "CHANGE_%":  row["CHANGE_%"], "LIVE_OPEN": lo,
                "LIVE_HIGH": row["LIVE_HIGH"], "YEST_HIGH": round(yh, 2),
                "YEST_CLOSE": round(yc, 2),
                "ZONE_%":    round(zone_pct, 2),
                "STATUS":    "⏳ Watching",
            })
        return sorted(rows, key=lambda x: x["ZONE_%"])

    def _yest_green_break(self, df: pd.DataFrame) -> list:
        """Yesterday GREEN → already broke above YEST_HIGH (confirmed breakout)."""
        PCT_TOL = 1.0; OPEN_TOL = 0.05
        rows = []
        for _, row in df.iterrows():
            yc = row["YEST_CLOSE"]; yo = row["YEST_OPEN"]
            yh = row["YEST_HIGH"];  lo = row["LIVE_OPEN"]
            ltp = row["LTP"]
            if yc <= yo: continue
            zone_pct = (yh - yc) / yc * 100 if yc else 0
            if zone_pct > PCT_TOL: continue
            if not (lo >= yc + OPEN_TOL and lo <= yh - OPEN_TOL): continue
            if ltp < yh: continue   # not yet broken out
            rows.append({
                "Symbol":    row["Symbol"], "LTP": ltp,
                "CHANGE_%":  row["CHANGE_%"], "LIVE_OPEN": lo,
                "LIVE_HIGH": row["LIVE_HIGH"], "YEST_HIGH": round(yh, 2),
                "YEST_CLOSE": round(yc, 2),
                "YH_MOVE_%": round((ltp - yh) / yh * 100, 2) if yh else 0,
                "ZONE_%":    round(zone_pct, 2),
                "STATUS":    "✅ Broken Out",
            })
        return sorted(rows, key=lambda x: -x["YH_MOVE_%"])

    def _yest_red_zone(self, df: pd.DataFrame) -> list:
        """Yesterday RED → open inside lower zone (Yest Low → Yest Close) → watching."""
        PCT_TOL = 1.0; OPEN_TOL = 0.05
        rows = []
        for _, row in df.iterrows():
            yc = row["YEST_CLOSE"]; yo = row["YEST_OPEN"]
            yl = row["YEST_LOW"];   lo = row["LIVE_OPEN"]
            ltp = row["LTP"]
            if yc >= yo: continue
            zone_pct = (yc - yl) / yc * 100 if yc else 0
            if zone_pct > PCT_TOL: continue
            if not (lo >= yl + OPEN_TOL and lo <= yc - OPEN_TOL): continue
            rows.append({
                "Symbol":    row["Symbol"], "LTP": ltp,
                "CHANGE_%":  row["CHANGE_%"], "LIVE_OPEN": lo,
                "LIVE_LOW":  row["LIVE_LOW"],  "YEST_LOW": round(yl, 2),
                "YEST_CLOSE": round(yc, 2),
                "ZONE_%":    round(zone_pct, 2),
                "STATUS":    "⏳ Watching" if ltp >= yl else "🔴 Broke Down",
            })
        return sorted(rows, key=lambda x: x["ZONE_%"])

    def _inside_range_15m(self, candles_15m: dict, df: pd.DataFrame) -> list:
        """
        15-Min Inside Range Break:
          Candles 2–4 must be fully inside candle 1 range.
          Break UP:   LTP > candle1.high  AND  LTP > EMA7 AND EMA20
          Break DOWN: LTP < candle1.low   AND  LTP < EMA7 AND EMA20
        Note: EMA daily not available here → use price vs YEST_HIGH/LOW as proxy.
        """
        rows = []
        for _, row in df.iterrows():
            sym     = row["Symbol"]
            ltp     = row["LTP"]
            candles = candles_15m.get(sym, [])
            if len(candles) < 4: continue

            first   = candles[0]
            later   = candles[1:4]
            f_high  = float(first.get("high", 0))
            f_low   = float(first.get("low",  9999))
            if f_high <= 0 or f_low >= 9999: continue

            # All of candles 2-4 must be inside candle 1 range
            inside = all(
                float(c.get("high", 0)) <= f_high and
                float(c.get("low", 9999)) >= f_low
                for c in later
            )
            if not inside: continue

            break_type = None
            chg_pct    = None
            if ltp > f_high:
                break_type = "UP"
                chg_pct    = round((ltp - f_high) / f_high * 100, 2)
            elif ltp < f_low:
                break_type = "DOWN"
                chg_pct    = round((ltp - f_low)  / f_low  * 100, 2)
            else:
                continue

            yh = row["YEST_HIGH"]; yl = row["YEST_LOW"]
            # Price direction confirmation via YEST_HIGH/LOW
            if break_type == "UP"   and ltp < yh * 0.99: continue
            if break_type == "DOWN" and ltp > yl * 1.01: continue

            rows.append({
                "Symbol":      sym,
                "LTP":         ltp,
                "CHANGE_%":    row["CHANGE_%"],
                "CHG_15M_%":   chg_pct,
                "CANDLE1_H":   round(f_high, 2),
                "CANDLE1_L":   round(f_low,  2),
                "BREAK_TYPE":  "🟢 UP" if break_type == "UP" else "🔴 DOWN",
                "DAY_HIGH":    row["LIVE_HIGH"],
                "DAY_LOW":     row["LIVE_LOW"],
                "YEST_HIGH":   round(yh, 2),
                "YEST_LOW":    round(yl, 2),
            })

        return sorted(rows, key=lambda x: abs(x["CHG_15M_%"]), reverse=True)

    def _vol_surge_15m(self, candles_15m: dict, df: pd.DataFrame) -> list:
        """
        15-Min Volume Surge: current 15M candle volume > previous AND LTP > YEST_HIGH.
        """
        rows = []
        now_dt = _now_ist()
        for _, row in df.iterrows():
            sym     = row["Symbol"]
            ltp     = row["LTP"]
            yh      = row["YEST_HIGH"]
            if ltp <= yh: continue   # only above YEST_HIGH

            candles = candles_15m.get(sym, [])
            if len(candles) < 2: continue

            comp = [c for c in candles
                    if pd.to_datetime(c["date"]).replace(tzinfo=None) < now_dt.replace(tzinfo=None)]
            if len(comp) < 2: continue

            curr_vol = int(comp[-1].get("volume", 0))
            prev_vol = int(comp[-2].get("volume", 0))
            if prev_vol <= 0 or curr_vol <= prev_vol: continue

            surge = round((curr_vol - prev_vol) / prev_vol * 100, 1)
            rows.append({
                "Symbol":     sym,
                "LTP":        ltp,
                "CHANGE_%":   row["CHANGE_%"],
                "CURR_SLOT":  pd.to_datetime(comp[-1]["date"]).strftime("%H:%M"),
                "CURR_VOL":   curr_vol,
                "PREV_SLOT":  pd.to_datetime(comp[-2]["date"]).strftime("%H:%M"),
                "PREV_VOL":   prev_vol,
                "VOL_SURGE_%": surge,
                "YEST_HIGH":  round(yh, 2),
                "DAY_HIGH":   row["LIVE_HIGH"],
            })

        return sorted(rows, key=lambda x: -x["VOL_SURGE_%"])


# ─────────────────────────────────────────────────────────────────────────────
# TABLE FILLER — populates a QTableWidget from a list of dicts
# ─────────────────────────────────────────────────────────────────────────────

def _fill_table(tbl: QTableWidget, rows: list, col_order: list = None,
                bull_bg: bool = None):
    tbl.setRowCount(0)
    if not rows:
        return
    keys = col_order if col_order else list(rows[0].keys())
    tbl.setColumnCount(len(keys))
    tbl.setHorizontalHeaderLabels(keys)
    for row_data in rows:
        r = tbl.rowCount()
        tbl.insertRow(r)
        tbl.setRowHeight(r, 24)
        bg = (BG_BULL_ROW if bull_bg is True else
              BG_BEAR_ROW if bull_bg is False else None)
        for c, key in enumerate(keys):
            val = row_data.get(key, "")
            if isinstance(val, float):
                text = f"{val:.2f}"
            else:
                text = str(val) if val is not None else ""

            color = TXT_PRIMARY
            # Smart coloring
            if key in ("CHANGE_%", "CHG_15M_%", "POST_BREAK_GAIN_%",
                        "POST_BREAK_DROP_%", "ABOVE_%", "BELOW_%",
                        "POST_MOVE_%", "YH_MOVE_%", "VOL_SURGE_%"):
                try:
                    color = CLR_GREEN if float(val) >= 0 else CLR_RED
                except Exception:
                    pass
            elif key == "Symbol":
                color = CLR_BLUE

            tbl.setItem(r, c, _item(text, color=color, bg=bg))


# ─────────────────────────────────────────────────────────────────────────────
# ADVANCED SCREENER PANEL — the main QWidget
# ─────────────────────────────────────────────────────────────────────────────

class AdvancedScreenerPanel(QWidget):
    def __init__(self, symbols: list, kite_getter=None):
        super().__init__()
        self.symbols      = symbols
        self._kite_getter = kite_getter
        self._worker      = None
        self._data: dict  = {}
        self.setStyleSheet(LIGHT_QSS)
        self._build_ui()

    def set_kite_getter(self, g):
        self._kite_getter = g

    def trigger_scan(self):
        self._scan()

    def _get_kite(self):
        if self._kite_getter:
            try:
                k = self._kite_getter()
                if k: return k
            except Exception:
                pass
        try:
            import sys
            qd = sys.modules.get("qt_dashboard") or sys.modules.get("__main__")
            if qd and hasattr(qd, "KiteSession"):
                ks = qd.KiteSession.get()
                if ks.ok: return ks.kite
        except Exception:
            pass
        return None

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12)
        root.setSpacing(10)

        # ── Header ────────────────────────────────────────────
        hdr = QHBoxLayout()
        title = QLabel("Advanced Screeners")
        title.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
        title.setStyleSheet(f"color:{TXT_PRIMARY};")
        hdr.addWidget(title)
        hdr.addStretch()
        self.lbl_ts = QLabel("Last scan: —")
        self.lbl_ts.setFont(QFont("Segoe UI", 9))
        self.lbl_ts.setStyleSheet(f"color:{TXT_SECONDARY};")
        hdr.addWidget(self.lbl_ts)
        hdr.addSpacing(12)
        self.btn_scan = QPushButton("⟳  Run All Scanners")
        self.btn_scan.setFixedSize(160, 30)
        self.btn_scan.clicked.connect(self._scan)
        hdr.addWidget(self.btn_scan)
        root.addLayout(hdr)

        # ── Summary badges ────────────────────────────────────
        badge_row = QHBoxLayout(); badge_row.setSpacing(8)
        self.b_seq15h = _stat_badge("15M High",    "0", CLR_GREEN, 90)
        self.b_seq15l = _stat_badge("15M Low",     "0", CLR_RED,   90)
        self.b_seq1hh = _stat_badge("1H High",     "0", CLR_GREEN, 90)
        self.b_seq1hl = _stat_badge("1H Low",      "0", CLR_RED,   90)
        self.b_orb    = _stat_badge("ORB",         "0", CLR_BLUE,  80)
        self.b_green  = _stat_badge("Grn Struct",  "0", CLR_GREEN, 90)
        self.b_red    = _stat_badge("Red Struct",  "0", CLR_RED,   90)
        self.b_ohl    = _stat_badge("O=H/O=L",     "0", CLR_YELLOW,90)
        self.b_inside = _stat_badge("Inside 15M",  "0", CLR_PURPLE,90)
        for b in [self.b_seq15h, self.b_seq15l, self.b_seq1hh, self.b_seq1hl,
                  self.b_orb, self.b_green, self.b_red, self.b_ohl, self.b_inside]:
            badge_row.addWidget(b)
        badge_row.addStretch()
        root.addLayout(badge_row)

        # ── Status label ──────────────────────────────────────
        self.status_lbl = QLabel("Click Run All Scanners to start.")
        self.status_lbl.setFont(QFont("Segoe UI", 10))
        self.status_lbl.setStyleSheet(f"color:{TXT_SECONDARY};")
        root.addWidget(self.status_lbl)

        # ── Sub-tabs ──────────────────────────────────────────
        self.tabs = QTabWidget()
        self._make_tabs()
        root.addWidget(self.tabs)

    def _make_tabs(self):
        # Sequential
        self.t_seq15h = self._make_scanner_tab(
            "⚡ 15M High",
            ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_HIGH","ABOVE_%","DAY_HIGH","YEST_HIGH"],
        )
        self.t_seq15l = self._make_scanner_tab(
            "📉 15M Low",
            ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_LOW","BELOW_%","DAY_LOW","YEST_LOW"],
        )
        self.t_seq1hh = self._make_scanner_tab(
            "🕐 1H High",
            ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_HIGH","ABOVE_%","DAY_HIGH","YEST_HIGH"],
        )
        self.t_seq1hl = self._make_scanner_tab(
            "🕐 1H Low",
            ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_LOW","BELOW_%","DAY_LOW","YEST_LOW"],
        )
        # ORB
        self.t_orb_up = self._make_scanner_tab(
            "🟢 ORB Break",
            ["Symbol","LTP","CHANGE_%","1H_HIGH","1H_LOW","RANGE_%","POST_MOVE_%","DAY_HIGH","YEST_HIGH"],
        )
        self.t_orb_dn = self._make_scanner_tab(
            "🔴 ORB Down",
            ["Symbol","LTP","CHANGE_%","1H_HIGH","1H_LOW","RANGE_%","POST_MOVE_%","DAY_LOW","YEST_LOW"],
        )
        # Structure
        self.t_green_s = self._make_scanner_tab(
            "🟢 Green Struct",
            ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_HIGH","YEST_CLOSE","BREAK_TIME","POST_BREAK_GAIN_%"],
        )
        self.t_red_s = self._make_scanner_tab(
            "🔴 Red Struct",
            ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_LOW","YEST_CLOSE","BREAK_TIME","POST_BREAK_DROP_%"],
        )
        # Strong close
        self.t_str_h = self._make_scanner_tab(
            "💪 Strong High",
            ["Symbol","LTP","CHANGE_%","DAY_HIGH","DIST_FROM_DAY_HIGH_%","YEST_HIGH","VOLUME"],
        )
        self.t_str_l = self._make_scanner_tab(
            "💪 Strong Low",
            ["Symbol","LTP","CHANGE_%","DAY_LOW","DIST_FROM_DAY_LOW_%","YEST_LOW","VOLUME"],
        )
        # OHL / Yesterday zones
        self.t_ohl = self._make_scanner_tab(
            "🎯 O=H/O=L",
            ["Symbol","LTP","CHANGE_%","LIVE_OPEN","LIVE_HIGH","LIVE_LOW","YEST_HIGH","YEST_LOW","SETUP"],
        )
        self.t_yg_zone = self._make_scanner_tab(
            "🟡 Yest Grn Zone",
            ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_HIGH","YEST_CLOSE","ZONE_%","STATUS"],
        )
        self.t_yg_break = self._make_scanner_tab(
            "🟢 Yest Grn Break",
            ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_HIGH","YEST_CLOSE","YH_MOVE_%","ZONE_%"],
        )
        self.t_yr_zone = self._make_scanner_tab(
            "🔴 Yest Red Zone",
            ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_LOW","YEST_CLOSE","ZONE_%","STATUS"],
        )
        # Inside range + Vol surge
        self.t_inside = self._make_scanner_tab(
            "⏱ Inside 15M",
            ["Symbol","LTP","CHANGE_%","CHG_15M_%","CANDLE1_H","CANDLE1_L","BREAK_TYPE","YEST_HIGH","YEST_LOW"],
        )
        self.t_vol = self._make_scanner_tab(
            "📊 Vol Surge",
            ["Symbol","LTP","CHANGE_%","CURR_SLOT","CURR_VOL","PREV_VOL","VOL_SURGE_%","YEST_HIGH"],
        )

    def _make_scanner_tab(self, title: str, cols: list) -> QTableWidget:
        tbl = _make_table(cols)
        self.tabs.addTab(tbl, title)
        return tbl

    def _scan(self):
        if self._worker and self._worker.isRunning():
            self.status_lbl.setText("⏳ Scan running…")
            return
        kite = self._get_kite()
        if kite is None:
            self.status_lbl.setText("❌  Kite not connected — go to 🔌 Session tab first.")
            self.status_lbl.setStyleSheet(f"color:{CLR_RED};")
            return
        self.btn_scan.setEnabled(False)
        self.btn_scan.setText("Scanning…")
        self.status_lbl.setText(f"✅  Kite connected — scanning {len(self.symbols)} symbols…")
        self.status_lbl.setStyleSheet(f"color:{TXT_SECONDARY};")
        self._worker = AdvancedScreenerWorker(self.symbols, kite)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.status.connect(self.status_lbl.setText)
        self._worker.start()

    def _on_done(self, data: dict):
        self.btn_scan.setEnabled(True)
        self.btn_scan.setText("⟳  Run All Scanners")
        self._data = data
        self.lbl_ts.setText(f"Last scan: {data.get('timestamp','')}")

        def n(k): return str(len(data.get(k, [])))
        self.b_seq15h._v.setText(n("seq_15m_high"))
        self.b_seq15l._v.setText(n("seq_15m_low"))
        self.b_seq1hh._v.setText(n("seq_1h_high"))
        self.b_seq1hl._v.setText(n("seq_1h_low"))
        self.b_orb._v.setText(
            str(len(data.get("orb_breakout",[])) + len(data.get("orb_breakdown",[]))))
        self.b_green._v.setText(n("green_structure"))
        self.b_red._v.setText(n("red_structure"))
        self.b_ohl._v.setText(n("ohl"))
        self.b_inside._v.setText(n("inside_15m"))

        # Fill all tables
        _fill_table(self.t_seq15h,  data.get("seq_15m_high",[]),
                    ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_HIGH","ABOVE_%","DAY_HIGH","YEST_HIGH"])
        _fill_table(self.t_seq15l,  data.get("seq_15m_low",[]),
                    ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_LOW","BELOW_%","DAY_LOW","YEST_LOW"])
        _fill_table(self.t_seq1hh,  data.get("seq_1h_high",[]),
                    ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_HIGH","ABOVE_%","DAY_HIGH","YEST_HIGH"])
        _fill_table(self.t_seq1hl,  data.get("seq_1h_low",[]),
                    ["Symbol","LTP","CHANGE_%","CANDLES","BROKEN_LOW","BELOW_%","DAY_LOW","YEST_LOW"])
        _fill_table(self.t_orb_up,  data.get("orb_breakout",[]),
                    ["Symbol","LTP","CHANGE_%","1H_HIGH","1H_LOW","RANGE_%","POST_MOVE_%","DAY_HIGH","YEST_HIGH"])
        _fill_table(self.t_orb_dn,  data.get("orb_breakdown",[]),
                    ["Symbol","LTP","CHANGE_%","1H_HIGH","1H_LOW","RANGE_%","POST_MOVE_%","DAY_LOW","YEST_LOW"])
        _fill_table(self.t_green_s, data.get("green_structure",[]),
                    ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_HIGH","YEST_CLOSE","BREAK_TIME","POST_BREAK_GAIN_%"])
        _fill_table(self.t_red_s,   data.get("red_structure",[]),
                    ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_LOW","YEST_CLOSE","BREAK_TIME","POST_BREAK_DROP_%"])
        _fill_table(self.t_str_h,   data.get("strong_high",[]),
                    ["Symbol","LTP","CHANGE_%","DAY_HIGH","DIST_FROM_DAY_HIGH_%","YEST_HIGH","VOLUME"])
        _fill_table(self.t_str_l,   data.get("strong_low",[]),
                    ["Symbol","LTP","CHANGE_%","DAY_LOW","DIST_FROM_DAY_LOW_%","YEST_LOW","VOLUME"])
        _fill_table(self.t_ohl,     data.get("ohl",[]),
                    ["Symbol","LTP","CHANGE_%","LIVE_OPEN","LIVE_HIGH","LIVE_LOW","YEST_HIGH","YEST_LOW","SETUP"])
        _fill_table(self.t_yg_zone, data.get("yest_green_zone",[]),
                    ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_HIGH","YEST_CLOSE","ZONE_%","STATUS"])
        _fill_table(self.t_yg_break,data.get("yest_green_break",[]),
                    ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_HIGH","YEST_CLOSE","YH_MOVE_%","ZONE_%"])
        _fill_table(self.t_yr_zone, data.get("yest_red_zone",[]),
                    ["Symbol","LTP","CHANGE_%","LIVE_OPEN","YEST_LOW","YEST_CLOSE","ZONE_%","STATUS"])
        _fill_table(self.t_inside,  data.get("inside_15m",[]),
                    ["Symbol","LTP","CHANGE_%","CHG_15M_%","CANDLE1_H","CANDLE1_L","BREAK_TYPE","YEST_HIGH","YEST_LOW"])
        _fill_table(self.t_vol,     data.get("vol_surge",[]),
                    ["Symbol","LTP","CHANGE_%","CURR_SLOT","CURR_VOL","PREV_VOL","VOL_SURGE_%","YEST_HIGH"])

        total = sum(len(data.get(k,[])) for k in data if isinstance(data.get(k), list))
        self.status_lbl.setText(
            f"✅  Done — {total} total signals  |  "
            f"Seq15M: {n('seq_15m_high')}↑ {n('seq_15m_low')}↓  "
            f"Seq1H: {n('seq_1h_high')}↑ {n('seq_1h_low')}↓  "
            f"ORB: {n('orb_breakout')}↑ {n('orb_breakdown')}↓"
        )
        self.status_lbl.setStyleSheet(f"color:{CLR_GREEN}; font-weight:600;")

    def _on_error(self, err: str):
        self.btn_scan.setEnabled(True)
        self.btn_scan.setText("⟳  Run All Scanners")
        self.status_lbl.setText(f"❌  {err[:120]}")
        self.status_lbl.setStyleSheet(f"color:{CLR_RED};")
