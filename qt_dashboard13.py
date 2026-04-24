"""
qt_dashboard.py — Phase 2: PyQt6 Trading Dashboard
====================================================
Phase 1: BOS Scanner | Astro Score | Time Signal | DB Status
Phase 2: OI Intelligence | SMC + OI Confluence | Kite Live Session

Drop this file in the same folder as all your existing modules and run:
    python qt_dashboard.py

Install deps once:
    pip install PyQt6 pytz kiteconnect pandas

Optional (full astro):
    pip install pyswisseph
"""

import sys
import os
import json
from datetime import datetime, date, timedelta

# ── PyQt6 ─────────────────────────────────────────────────────────────────────
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QComboBox, QDoubleSpinBox, QSplitter, QPushButton, QFrame,
    QScrollArea, QGridLayout, QStatusBar, QSizePolicy, QLineEdit,
    QGroupBox, QProgressBar,
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QColor, QFont, QPalette, QBrush

# ── Your existing modules ─────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from ohlc_store import OHLCStore, is_market_hours
    DB_OK = True
except ImportError:
    DB_OK = False

try:
    from bos_scanner import (
        detect_bos, build_bos_setup, load_scan_cache, BOS_LOOKBACK,
        scan_hourly_breaks, check_ob_retest, get_daily_ema,
        _todays_bos_setups,
    )
    BOS_OK = True
except ImportError:
    BOS_OK = False

try:
    from astro_logic import get_astro_score, get_week_forecast
    ASTRO_OK = True
except ImportError:
    ASTRO_OK = False

try:
    from astro_time import get_time_signal_detail, is_good_entry_time
    TIME_OK = True
except ImportError:
    TIME_OK = False

try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
    SMC_OK = True
except ImportError:
    SMC_OK = False

try:
    from kiteconnect import KiteConnect
    import pandas as pd
    KITE_LIB_OK = True
except ImportError:
    KITE_LIB_OK = False

try:
    from qt_screener import ScreenerPanel, AlertsPanel
    from advanced_screener import AdvancedScreenerPanel
    ADV_SCREENER_OK = True
    SCREENER_OK = True
except ImportError:
    SCREENER_OK = False
    ADV_SCREENER_OK = False
    print("WARNING: qt_screener.py not found — Screener + Alerts tabs disabled")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY           = "7am67kxijfsusk9i"
_BASE_DIR         = os.path.dirname(os.path.abspath(__file__))

# ── CACHE folder — all qt support files live here ────────────────────────────
# Shared with old panchak dashboard (which also uses CACHE/ subfolder)
# _qt suffix distinguishes Qt-specific files from Streamlit ones
_CACHE_DIR = os.path.join(_BASE_DIR, "CACHE")
os.makedirs(_CACHE_DIR, exist_ok=True)

def _cp(filename: str) -> str:
    """Shorthand: return full path inside CACHE/ folder."""
    return os.path.join(_CACHE_DIR, filename)

# ── File paths — all under CACHE/ ────────────────────────────────────────────
# Shared with old dashboard (no _qt suffix — same file, both read it)
PANCHAK_DATA_FILE = _cp("panchak_data.csv")     # panchak HIGH/LOW levels
PANCHAK_META_FILE = _cp("panchak_meta.csv")     # panchak start/end dates

# Qt-specific files (_qt suffix so they don't clash with Streamlit versions)
OI_CACHE_FILE       = _cp("oi_intel_cache_qt.json")   # last OI snapshot
BOS_CACHE_FILE      = _cp("bos_scan_cache_qt.json")   # last BOS scan results
TG_DEDUP_FILE_QT    = _cp("tg_dedup_qt.json")         # Telegram dedup (Qt)
ALERT_LOG_FILE_QT   = _cp("alert_log_qt.json")        # alert history (Qt)
ALERT_SETTINGS_FILE = _cp("alert_settings_qt.json")   # alert toggle settings

# Access token — check CACHE/ first, then same folder as fallback
ACCESS_TOKEN_FILE = (
    _cp("access_token.txt")
    if os.path.exists(_cp("access_token.txt"))
    else os.path.join(_BASE_DIR, "access_token.txt")
)

AUTO_REFRESH_SECS = 60

print(f"[Init] CACHE folder: {_CACHE_DIR}")
if os.path.exists(PANCHAK_DATA_FILE):
    print(f"[Init] panchak_data.csv found ✅")
else:
    print(f"[Init] panchak_data.csv NOT FOUND — run panchak dashboard once to build it")

# ─────────────────────────────────────────────────────────────────────────────
# DARK THEME
# ─────────────────────────────────────────────────────────────────────────────

DARK_QSS = """
/* ═══════════════════════════════════════════════════════════════════
   PANCHAK TRADING DASHBOARD — CLEAN PROFESSIONAL LIGHT THEME
   Inspired by: clean white tables, pill-shaped tab buttons,
   readable typography, green/red row tinting for bull/bear data
   ═══════════════════════════════════════════════════════════════════ */

* {
    font-family: "Segoe UI", "Inter", "Helvetica Neue", Arial, sans-serif;
    font-size: 13px;
}

/* ── App base ───────────────────────────────────────────────────── */
QMainWindow, QDialog {
    background: #F0F2F5;
}
QWidget {
    background: #FFFFFF;
    color: #1A1D23;
}

/* ── TAB BAR — pill buttons style ──────────────────────────────── */
QTabWidget::pane {
    border: none;
    background: #FFFFFF;
    border-top: 1px solid #DDE1E8;
}
QTabBar {
    background: #EEF1F6;
    qproperty-drawBase: 0;
    border-bottom: 1px solid #DDE1E8;
    padding: 6px 8px 0px 8px;
}
QTabBar::tab {
    background: #FFFFFF;
    color: #2C5282;
    padding: 7px 16px;
    border: 1px solid #CBD5E0;
    border-bottom: none;
    border-radius: 18px 18px 0px 0px;
    font-size: 11px;
    font-weight: 700;
    min-width: 70px;
    margin-right: 4px;
    margin-bottom: 0px;
}
QTabBar::tab:selected {
    background: #2C5282;
    color: #FFFFFF;
    border-color: #2C5282;
}
QTabBar::tab:hover:!selected {
    background: #EBF4FF;
    color: #2C5282;
    border-color: #90CDF4;
}

/* ── TABLES — clean with green/red row tinting ──────────────────── */
QTableWidget {
    background: #FFFFFF;
    alternate-background-color: #F8F9FA;
    border: 1px solid #DDE1E8;
    border-radius: 4px;
    gridline-color: #E8ECF0;
    color: #1A1D23;
    font-size: 12px;
    selection-background-color: #BEE3F8;
    selection-color: #1A1D23;
    outline: none;
}
QTableWidget::item {
    padding: 6px 12px;
    border-bottom: 1px solid #EEF1F6;
    color: #1A1D23;
}
QTableWidget::item:selected {
    background: #BEE3F8;
    color: #1A1D23;
}
QHeaderView {
    background: #F8F9FA;
    border-bottom: 2px solid #CBD5E0;
}
QHeaderView::section {
    background: #F8F9FA;
    color: #4A5568;
    padding: 8px 12px;
    border: none;
    border-right: 1px solid #E2E8F0;
    border-bottom: 2px solid #CBD5E0;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.3px;
    text-transform: uppercase;
}
QHeaderView::section:last { border-right: none; }

/* ── INPUTS ─────────────────────────────────────────────────────── */
QComboBox {
    background: #FFFFFF;
    color: #1A1D23;
    border: 1px solid #CBD5E0;
    padding: 5px 12px;
    border-radius: 6px;
    min-width: 110px;
    font-size: 12px;
}
QComboBox:hover  { border-color: #4299E1; }
QComboBox:focus  { border-color: #4299E1; border-width: 2px; }
QComboBox::drop-down { border: none; width: 22px; }
QComboBox QAbstractItemView {
    background: #FFFFFF;
    color: #1A1D23;
    border: 1px solid #CBD5E0;
    selection-background-color: #EBF8FF;
    selection-color: #2B6CB0;
    outline: none;
}
QDoubleSpinBox, QLineEdit {
    background: #FFFFFF;
    color: #1A1D23;
    border: 1px solid #CBD5E0;
    padding: 5px 12px;
    border-radius: 6px;
    font-size: 12px;
}
QDoubleSpinBox:hover, QLineEdit:hover { border-color: #4299E1; }
QDoubleSpinBox:focus, QLineEdit:focus { border-color: #4299E1; border-width: 2px; }

/* ── BUTTONS ────────────────────────────────────────────────────── */
QPushButton {
    background: #2C5282;
    color: #FFFFFF;
    border: none;
    padding: 7px 18px;
    border-radius: 18px;
    font-weight: 700;
    font-size: 11px;
    min-height: 30px;
    letter-spacing: 0.3px;
}
QPushButton:hover   { background: #2A4A8A; }
QPushButton:pressed { background: #1E3A6E; }
QPushButton:disabled { background: #CBD5E0; color: #A0AEC0; }

/* ── SCROLLBARS ─────────────────────────────────────────────────── */
QScrollBar:vertical {
    background: #F8F9FA;
    width: 8px;
    border: none;
    border-radius: 4px;
}
QScrollBar::handle:vertical {
    background: #CBD5E0;
    border-radius: 4px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover { background: #4299E1; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar:horizontal {
    background: #F8F9FA;
    height: 8px;
    border: none;
}
QScrollBar::handle:horizontal { background: #CBD5E0; border-radius: 4px; }
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }

/* ── MISC ───────────────────────────────────────────────────────── */
QScrollArea  { border: none; background: transparent; }
QFrame       { background: transparent; }
QLabel       { color: #1A1D23; background: transparent; font-size: 13px; }
QStatusBar {
    background: #EEF1F6;
    color: #718096;
    border-top: 1px solid #DDE1E8;
    font-size: 11px;
    padding: 2px 12px;
}
QProgressBar {
    background: #E2E8F0;
    border: none;
    border-radius: 3px;
    max-height: 4px;
}
QProgressBar::chunk {
    background: qlineargradient(x1:0,y1:0,x2:1,y2:0,
        stop:0 #2C5282, stop:1 #4299E1);
    border-radius: 3px;
}
QGroupBox {
    border: 1px solid #DDE1E8;
    border-radius: 8px;
    margin-top: 10px;
    padding-top: 8px;
    font-size: 11px;
    font-weight: 700;
    color: #4A5568;
    background: #FFFFFF;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
    color: #2C5282;
}
QCheckBox { color: #2D3748; font-size: 12px; spacing: 6px; }
QCheckBox::indicator {
    width: 16px; height: 16px;
    border: 2px solid #CBD5E0;
    border-radius: 4px;
    background: #FFFFFF;
}
QCheckBox::indicator:checked {
    background: #2C5282;
    border-color: #2C5282;
}
QSplitter::handle { background: #DDE1E8; width: 1px; height: 1px; }
QToolTip {
    background: #2D3748;
    color: #FFFFFF;
    border: none;
    padding: 6px 10px;
    border-radius: 6px;
    font-size: 11px;
}
"""





# ── Color system: Professional Blue-Pill Light Theme ─────────────────────────
C_GREEN   = "#276749"   # forest green — bull / gain
C_GREEN2  = "#2F855A"   # medium green
C_RED     = "#C53030"   # strong red — bear / loss
C_RED2    = "#9B2C2C"   # deeper red
C_YELLOW  = "#B7791F"   # amber — warning / ATM
C_BLUE    = "#2C5282"   # navy blue — main accent (matches tab pills)
C_CYAN    = "#2B6CB0"   # medium blue — info
C_PURPLE  = "#553C9A"   # purple — CHoCH / special
C_ORANGE  = "#C05621"   # near levels
# Backgrounds
C_BG      = "#F0F2F5"   # page / app bg
C_BG_CARD = "#FFFFFF"   # card bg
C_BG_ROW  = "#F8F9FA"   # alternate row
C_BG_BULL = "#F0FFF4"   # light green row — bull
C_BG_BEAR = "#FFF5F5"   # light red row — bear
C_BG_ATM  = "#EBF8FF"   # ATM highlight
# Borders & text
C_BORDER  = "#DDE1E8"
C_BORDER2 = "#CBD5E0"
C_DIM     = "#A0AEC0"   # dim secondary
C_MUTED   = "#718096"   # muted labels
C_TEXT    = "#1A1D23"   # primary text
C_TEXT2   = "#2D3748"   # headings
# Tints
C_GLOW_G  = "#27674920"
C_GLOW_R  = "#C5303020"
C_GLOW_B  = "#2C528220"
C_BG_CARD_OLD = "#FFFFFF"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def make_label(text, size=12, bold=False, color=None,
               align=Qt.AlignmentFlag.AlignLeft, letter_spacing=False):
    lbl = QLabel(text)
    f = QFont("Segoe UI", size)
    f.setBold(bold)
    lbl.setFont(f)
    style = "background:transparent;"
    if color: style += f"color:{color};"
    if letter_spacing: style += "letter-spacing:1px;"
    lbl.setStyleSheet(style)
    lbl.setAlignment(align)
    return lbl


def section_title(text):
    """Consistent section header with accent left-border — Kite light style."""
    lbl = QLabel(f"  {text}")
    lbl.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
    lbl.setFixedHeight(32)
    lbl.setStyleSheet(f"""
        color: {C_BLUE};
        background: #F8F9FB;
        border-left: 4px solid {C_BLUE};
        border-bottom: 1px solid {C_BORDER};
        padding-left: 10px;
        letter-spacing: 0.3px;
        font-weight: 700;
    """)
    return lbl


def card_frame(layout_cls=QVBoxLayout, pad=(14, 12, 14, 12), glow=None):
    frame = QFrame()
    border = glow if glow else C_BORDER
    frame.setStyleSheet(f"""
        QFrame {{
            background: {C_BG_CARD};
            border: 1px solid {border if border != C_BORDER else "#DDE1E8"};
            border-radius: 8px;
        }}
    """)
    lay = layout_cls()
    lay.setContentsMargins(*pad)
    lay.setSpacing(8)
    frame.setLayout(lay)
    return frame, lay


def stat_pill(label: str, value: str, color: str, width: int = 150) -> QFrame:
    """
    Stat pill — Kite light style: white card, colored top border.
    """
    frame = QFrame()
    frame.setFixedWidth(width)
    frame.setStyleSheet(f"""
        QFrame {{
            background: #FFFFFF;
            border: 1px solid #DDE1E8;
            border-top: 3px solid {color};
            border-radius: 6px;
        }}
        QFrame:hover {{
            border-top-color: {color};
            border-color: {color}80;
            background: #FAFBFC;
        }}
    """)
    lay = QVBoxLayout(frame)
    lay.setContentsMargins(10, 7, 10, 7)
    lay.setSpacing(2)

    lbl_cap = QLabel(label.upper())
    lbl_cap.setFont(QFont("Segoe UI", 8, QFont.Weight.Bold))
    lbl_cap.setStyleSheet(f"color:#718096; background:transparent; letter-spacing:0.8px;")
    lbl_cap.setAlignment(Qt.AlignmentFlag.AlignCenter)

    lbl_val = QLabel(value)
    lbl_val.setFont(QFont("Segoe UI", 15, QFont.Weight.Bold))
    lbl_val.setStyleSheet(f"color:{color}; background:transparent;")
    lbl_val.setAlignment(Qt.AlignmentFlag.AlignCenter)

    lay.addWidget(lbl_cap)
    lay.addWidget(lbl_val)
    frame._cap = lbl_cap
    frame._v   = lbl_val
    return frame


def mini_badge(label: str, value: str, color: str) -> QFrame:
    """Horizontal badge: 'Label: VALUE' — used in levels row."""
    frame = QFrame()
    frame.setStyleSheet(f"""
        QFrame {{
            background: #FFFFFF;
            border: 1px solid #DDE1E8;
            border-left: 3px solid {color};
            border-radius: 6px;
        }}
    """)
    lay = QHBoxLayout(frame)
    lay.setContentsMargins(10, 5, 10, 5)
    lay.setSpacing(6)

    cap = QLabel(label + ":")
    cap.setFont(QFont("Segoe UI", 9))
    cap.setStyleSheet("color:#718096; background:transparent; letter-spacing:0.3px;")

    val = QLabel(value)
    val.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
    val.setStyleSheet(f"color:{color}; background:transparent;")

    lay.addWidget(cap)
    lay.addWidget(val)
    frame._v = val
    return frame


def separator_line(vertical=False) -> QFrame:
    """Thin separator line."""
    sep = QFrame()
    sep.setFrameShape(QFrame.Shape.VLine if vertical else QFrame.Shape.HLine)
    sep.setStyleSheet(f"color:{C_BORDER}; background:{C_BORDER};")
    if vertical:
        sep.setFixedWidth(1)
    else:
        sep.setFixedHeight(1)
    return sep


def fmt_oi(n):
    n = int(n or 0)
    if n >= 10_000_000: return f"{n/10_000_000:.1f}Cr"
    if n >= 100_000:    return f"{n/100_000:.1f}L"
    return str(n)



# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM ALERTER  (sends all trading alerts — OI, SMC, BOS, Screeners)
# ─────────────────────────────────────────────────────────────────────────────

import urllib.request as _urllib_req

TG_BOT_TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID   = "-1003706739531"

# ── Rate limiting + dedup ─────────────────────────────────────────────────────
import time as _time_mod
import threading as _threading_mod

_tg_dedup: dict     = {}   # {key: date_str}  — per-day dedup
_tg_cooldown: dict  = {}   # {category: last_sent_epoch}  — per-category cooldown
_tg_lock            = _threading_mod.Lock()
_tg_last_sent: float = 0   # global rate limiter — epoch of last successful send

# Minimum seconds between sends PER CATEGORY
_TG_COOLDOWN = {
    "OI":        900,   # 15 min — OI direction alert
    "SMC":       600,   # 10 min — SMC signal
    "BOS":       0,     # no category cooldown (dedup key handles it)
    "SCREENER":  300,   # 5 min
    "DEFAULT":   60,    # 1 min fallback
}
_TG_GLOBAL_GAP = 2.0   # seconds between ANY two sends (avoids 429)


def _tg_send(msg: str, dedup_key: str = None, category: str = "DEFAULT") -> bool:
    """
    Blocking Telegram send with:
    - Global rate limit: min _TG_GLOBAL_GAP seconds between sends
    - Per-day dedup: same dedup_key never fires twice in one day
    - Per-category cooldown: each category has a minimum repeat interval
    """
    global _tg_last_sent
    today = date.today().isoformat()

    with _tg_lock:
        # 1. Per-day dedup
        if dedup_key and _tg_dedup.get(dedup_key) == today:
            return False

        # 2. Per-category cooldown
        cooldown = _TG_COOLDOWN.get(category, _TG_COOLDOWN["DEFAULT"])
        if cooldown > 0:
            last = _tg_cooldown.get(category, 0)
            if _time_mod.time() - last < cooldown:
                return False

        # 3. Global rate limit — wait if needed
        since_last = _time_mod.time() - _tg_last_sent
        if since_last < _TG_GLOBAL_GAP:
            _time_mod.sleep(_TG_GLOBAL_GAP - since_last)

    try:
        url  = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT_ID, "text": msg,
                           "parse_mode": "HTML"}).encode()
        req  = _urllib_req.Request(url, data=data,
                                   headers={"Content-Type": "application/json"})
        with _urllib_req.urlopen(req, timeout=12) as r:
            resp = json.loads(r.read())
            ok   = resp.get("ok", False)

        with _tg_lock:
            _tg_last_sent = _time_mod.time()
            if ok:
                if dedup_key:
                    _tg_dedup[dedup_key] = today
                if cooldown > 0:
                    _tg_cooldown[category] = _time_mod.time()

        return ok

    except Exception as e:
        err = str(e)
        if "429" in err:
            print(f"[TG] Rate limited (429) — category={category}, backing off 30s")
            _time_mod.sleep(30)
        else:
            print(f"[TG] Error: {e}")
        return False


def _tg_bg(msg: str, dedup_key: str = None, category: str = "DEFAULT"):
    """Non-blocking send — daemon thread, UI never freezes."""
    _threading_mod.Thread(
        target=_tg_send,
        args=(msg, dedup_key, category),
        daemon=True
    ).start()


def _tg_ts():
    try:
        import pytz; IST = pytz.timezone("Asia/Kolkata")
        return datetime.now(IST).strftime("%H:%M IST")
    except Exception:
        return datetime.now().strftime("%H:%M")


# ── OI direction alert ────────────────────────────────────────────────────────
_last_oi_direction = ""

def _alert_oi(data: dict):
    global _last_oi_direction
    direction = data.get("direction", "")
    if not direction or direction == _last_oi_direction:
        return
    _last_oi_direction = direction
    spot   = data.get("spot", 0)
    pcr    = data.get("pcr", 0)
    ncw    = data.get("nearest_call_wall", "—")
    npf    = data.get("nearest_put_floor", "—")
    dr     = data.get("direction_reason", "")
    advice = data.get("advice", "")
    setup  = data.get("setup", "—")
    mp     = data.get("max_pain", "—")
    icon   = "🟢" if "BULL" in direction else ("🔴" if "BEAR" in direction else "⚠️")
    msg = (
        f"{icon} <b>OI Direction: {direction}</b>  ⏰ {_tg_ts()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"NIFTY: <b>{spot:,.1f}</b>  PCR: <b>{pcr}</b>\n"
        f"{dr}\n"
        f"Call Wall: <b>{ncw}</b>  |  Put Floor: <b>{npf}</b>\n"
        f"Max Pain: <b>{mp}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 {setup}\n"
        f"⚠️ <i>NOT financial advice.</i>"
    )
    # Use OI category — 15 min cooldown prevents startup spam
    _tg_bg(msg, dedup_key=None, category="OI")
    print(f"[TG] OI alert → {direction}")


# ── SMC confluence alert ──────────────────────────────────────────────────────
_last_smc_signal = ""

def _alert_smc(data: dict):
    global _last_smc_signal
    signal = data.get("signal", "")
    score  = data.get("score", 0)
    if not signal: return
    today  = date.today().isoformat()
    key    = f"SMC_{signal}_{today}"
    action = data.get("action", "")
    conf   = data.get("confluence_pct", 0)
    lk     = data.get("smc_trend_ltf", "—")
    hk     = data.get("smc_trend_htf", "—")
    reasons= data.get("reasons", [])[:4]
    icon   = "🟢" if "BUY" in signal or "BULL" in signal else (
             "🔴" if "SELL" in signal or "BEAR" in signal else "⚠️")
    reason_lines = "\n".join(f"  • {r}" for r in reasons) if reasons else "  —"
    msg = (
        f"{icon} <b>SMC+OI: {signal}</b>  ⏰ {_tg_ts()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Score: <b>{score:+d}</b>  Confluence: <b>{conf}%</b>\n"
        f"LTF: <b>{lk}</b>  HTF: <b>{hk}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{reason_lines}\n"
        f"🎯 {action}\n"
        f"⚠️ <i>NOT financial advice.</i>"
    )
    _tg_bg(msg, dedup_key=key, category="SMC")
    print(f"[TG] SMC alert → {signal} ({score:+d})")

# ─────────────────────────────────────────────────────────────────────────────
# KITE SESSION (singleton)
# ─────────────────────────────────────────────────────────────────────────────

class KiteSession:
    _inst = None

    def __init__(self):
        self.kite = None; self.ok = False; self.error = ""; self.status = "Not connected"
        self._nfo_cache = None

    @classmethod
    def get(cls):
        if cls._inst is None:
            cls._inst = KiteSession()
        return cls._inst

    def connect(self, token_file=ACCESS_TOKEN_FILE):
        if not KITE_LIB_OK:
            self.error = "kiteconnect not installed"; self.status = "❌ Library missing"; return False
        try:
            self.kite = KiteConnect(api_key=API_KEY)
            with open(token_file, encoding="utf-8") as f:
                self.kite.set_access_token(f.read().strip())
            self.kite.quote(["NSE:NIFTY 50"])   # validate
            self.ok = True; self.status = "✅ Connected"; return True
        except FileNotFoundError:
            self.error = f"access_token.txt not found: {token_file}"
        except Exception as e:
            self.error = str(e)
        self.ok = False; self.status = f"❌ {self.error[:60]}"; return False

    def nfo_instruments(self):
        if self._nfo_cache is not None:
            return self._nfo_cache
        if not self.ok:
            return None
        try:
            self._nfo_cache = pd.DataFrame(self.kite.instruments("NFO"))
            return self._nfo_cache
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND WORKERS
# ─────────────────────────────────────────────────────────────────────────────

class OIWorker(QThread):
    done  = pyqtSignal(dict)
    error = pyqtSignal(str)

    def run(self):
        ks = KiteSession.get()
        if not ks.ok: self.error.emit("Kite not connected"); return
        try:
            result = self._fetch(ks.kite)
            self.done.emit(result)
            try: _alert_oi(result)
            except Exception: pass
        except Exception as e:
            self.error.emit(str(e))

    def _fetch(self, kite):
        # Spot
        spot = kite.quote(["NSE:NIFTY 50"])["NSE:NIFTY 50"]["last_price"]
        # NFO instruments
        nfo = pd.DataFrame(kite.instruments("NFO"))
        KiteSession.get()._nfo_cache = nfo
        # Futures
        fut_ltp = spot; expiry_label = ""
        try:
            nf = nfo[(nfo["name"] == "NIFTY") & (nfo["instrument_type"] == "FUT")].copy()
            nf["expiry_dt"] = pd.to_datetime(nf["expiry"])
            nf = nf[nf["expiry_dt"] >= pd.Timestamp.now()].sort_values("expiry_dt")
            if not nf.empty:
                fts = nf.iloc[0]["tradingsymbol"]
                expiry_label = str(nf.iloc[0]["expiry_dt"].date())
                fq = kite.quote([f"NFO:{fts}"])
                fut_ltp = fq[f"NFO:{fts}"]["last_price"]
        except Exception:
            pass
        # ATM
        step = 50; atm = int(round(spot / step) * step)
        strikes = [atm + i * step for i in range(-10, 11)]
        # Options
        opts = nfo[(nfo["name"] == "NIFTY") & (nfo["instrument_type"].isin(["CE", "PE"]))].copy()
        opts["expiry_dt"] = pd.to_datetime(opts["expiry"])
        fut_exp = opts[opts["expiry_dt"] >= pd.Timestamp.now()]["expiry_dt"].unique()
        if len(fut_exp) == 0:
            raise RuntimeError("No NIFTY options expiry found")
        expiry = sorted(fut_exp)[0]
        if not expiry_label:
            expiry_label = str(expiry.date())
        # Token map
        tok_map = {}
        for s in strikes:
            for t in ["CE", "PE"]:
                row = opts[(opts["strike"] == s) & (opts["instrument_type"] == t) & (opts["expiry_dt"] == expiry)]
                if not row.empty:
                    ts = row.iloc[0]["tradingsymbol"]
                    tok_map[f"NFO:{ts}"] = {"strike": s, "type": t}
        if not tok_map:
            raise RuntimeError("No option instruments found")
        # Batch quote
        q_raw = kite.quote(list(tok_map.keys()))
        # Chain
        chain = {}
        for sym, meta in tok_map.items():
            q = q_raw.get(sym)
            if not q: continue
            s = meta["strike"]; t = meta["type"]
            chain.setdefault(s, {})[t] = {
                "ltp":     q.get("last_price", 0),
                "oi":      q.get("oi", 0) or 0,
                "oi_high": q.get("oi_day_high", 0) or 0,
                "oi_low":  q.get("oi_day_low", 0) or 0,
                "volume":  q.get("volume", 0) or 0,
                "iv":      q.get("implied_volatility", 0) or 0,
            }
        if not chain:
            raise RuntimeError("Empty options chain")
        # Max Pain
        pain = {}
        for ps in strikes:
            t = 0
            for s, d in chain.items():
                if s < ps: t += d.get("CE", {}).get("oi", 0) * (ps - s)
                elif s > ps: t += d.get("PE", {}).get("oi", 0) * (s - ps)
            pain[ps] = t
        max_pain = min(pain, key=pain.get) if pain else atm
        # OI maps
        ce_oi  = {s: chain[s].get("CE", {}).get("oi", 0) for s in chain}
        pe_oi  = {s: chain[s].get("PE", {}).get("oi", 0) for s in chain}
        ce_add = {s: chain[s].get("CE", {}).get("oi", 0) - chain[s].get("CE", {}).get("oi_low", 0) for s in chain}
        pe_add = {s: chain[s].get("PE", {}).get("oi", 0) - chain[s].get("PE", {}).get("oi_low", 0) for s in chain}
        pe_drp = {s: chain[s].get("PE", {}).get("oi", 0) - chain[s].get("PE", {}).get("oi_high", 0) for s in chain}
        str_ce = max(ce_oi, key=ce_oi.get) if ce_oi else atm
        str_pe = max(pe_oi, key=pe_oi.get) if pe_oi else atm
        tot_ce = sum(ce_oi.values()) or 1
        tot_pe = sum(pe_oi.values()) or 1
        pcr = round(tot_pe / tot_ce, 2)
        # Near-ATM shifts
        near = [s for s in chain if abs(s - atm) <= step * 4]
        _nce = sum(chain[s].get("CE", {}).get("oi", 0) for s in near) or 1
        _npe = sum(chain[s].get("PE", {}).get("oi", 0) for s in near) or 1
        _ce_bld = round(sum(ce_add.get(s, 0) for s in near) / _nce * 100, 1)
        _pe_bld = round(sum(pe_add.get(s, 0) for s in near) / _npe * 100, 1)
        _pe_drp = round(sum(pe_drp.get(s, 0) for s in near) / _npe * 100, 1)
        _ce_drp = round(sum(chain[s].get("CE", {}).get("oi", 0) - chain[s].get("CE", {}).get("oi_high", 0) for s in near) / _nce * 100, 1)
        # Walls
        cw = [s for s in ce_oi if s > spot]; pf = [s for s in pe_oi if s < spot]
        ncw = min(cw, key=lambda x: x-spot) if cw else None
        npf = min(pf, key=lambda x: spot-x) if pf else None
        d2c = round(ncw - spot, 0) if ncw else 9999
        d2p = round(spot - npf, 0) if npf else 9999
        # Direction
        if _ce_bld >= 5 and _pe_drp <= -3:
            direction = "🔴 BEARISH"; dr = f"WATERFALL: CE +{_ce_bld}% / PE {_pe_drp}%"
        elif _pe_bld >= 5 and _ce_drp <= -3:
            direction = "🟢 BULLISH"; dr = f"SHORT SQUEEZE: PE +{_pe_bld}% / CE {_ce_drp}%"
        elif _ce_bld >= 5:
            direction = "🔴 BEARISH BIAS"; dr = f"Call wall building +{_ce_bld}%"
        elif _pe_bld >= 5:
            direction = "🟢 BULLISH BIAS"; dr = f"Put floor building +{_pe_bld}%"
        elif _ce_drp <= -3:
            direction = "🟢 BULLISH"; dr = f"Call wall unwinding {_ce_drp}%"
        elif _pe_drp <= -3:
            direction = "🔴 BEARISH"; dr = f"Put floor unwinding {_pe_drp}%"
        elif pcr >= 1.3:
            direction = "🟢 BULLISH"; dr = f"PCR={pcr} — strong put support"
        elif pcr <= 0.7:
            direction = "🔴 BEARISH"; dr = f"PCR={pcr} — strong call pressure"
        elif d2c < d2p and ncw:
            direction = "🔴 BEARISH BIAS"; dr = f"Call wall {ncw} only {int(d2c)} pts away"
        elif d2p < d2c and npf:
            direction = "🟢 BULLISH BIAS"; dr = f"Put floor {npf} only {int(d2p)} pts away"
        else:
            direction = "⚠️ SIDEWAYS/NEUTRAL"; dr = f"PCR={pcr}, balanced OI"
        # Spot vs Max Pain
        svp = spot - max_pain
        pain_sig = ("AT MAX PAIN" if abs(svp) < 50 else
                    f"Spot ↑{round(svp)} pts above Max Pain ({max_pain})" if svp > 0 else
                    f"Spot ↓{round(abs(svp))} pts below Max Pain ({max_pain})")
        # Advice
        if "BEARISH" in direction and "BIAS" not in direction:
            advice = "⚠️ Strong BEARISH. Don't buy calls."
            setup  = f"Buy {atm} PE | SL above {ncw or str_ce}"
        elif "BULLISH" in direction and "BIAS" not in direction:
            advice = "✅ Strong BULLISH. Don't sell calls."
            setup  = f"Buy {atm} CE | SL below {npf or str_pe}"
        elif "BEARISH BIAS" in direction:
            advice = "Mild BEARISH. Sell rallies near call wall."
            setup  = f"Sell/Hedge near {ncw or str_ce} CE"
        elif "BULLISH BIAS" in direction:
            advice = "Mild BULLISH. Buy dips near put floor."
            setup  = f"Buy dips near {npf or str_pe} PE"
        else:
            advice = "Range-bound. Trade breakouts only."
            setup  = f"Wait: break above {ncw} OR below {npf}"
        # Chain rows
        rows = []
        for s in sorted(chain.keys()):
            ce = chain[s].get("CE", {}); pe = chain[s].get("PE", {})
            rows.append({
                "STRIKE": s,
                "STATUS": "ATM" if s == atm else ("ITM CE" if s < atm else "OTM CE"),
                "CE_LTP":    round(ce.get("ltp", 0), 1),
                "CE_OI":     int(ce.get("oi", 0)),
                "CE_OI_ADD": int(ce.get("oi", 0) - ce.get("oi_low", 0)),
                "CE_IV":     round(ce.get("iv", 0), 1),
                "PE_LTP":    round(pe.get("ltp", 0), 1),
                "PE_OI":     int(pe.get("oi", 0)),
                "PE_OI_ADD": int(pe.get("oi", 0) - pe.get("oi_low", 0)),
                "PE_IV":     round(pe.get("iv", 0), 1),
            })
        result = {
            "spot": round(spot, 2), "fut_ltp": round(fut_ltp, 2),
            "atm": atm, "step": step, "max_pain": max_pain, "pcr": pcr,
            "expiry": expiry_label,
            "strongest_ce": str_ce, "ce_oi": ce_oi.get(str_ce, 0),
            "strongest_pe": str_pe, "pe_oi": pe_oi.get(str_pe, 0),
            "nearest_call_wall": ncw, "nearest_put_floor": npf,
            "dist_to_call": d2c, "dist_to_put": d2p,
            "near_ce_pct": _ce_bld, "near_pe_pct": _pe_bld, "near_pe_drop_pct": _pe_drp,
            "direction": direction, "direction_reason": dr,
            "pain_signal": pain_sig, "advice": advice, "setup": setup,
            "total_ce_oi": tot_ce, "total_pe_oi": tot_pe,
            "chain_rows": rows,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }
        try:
            with open(OI_CACHE_FILE, "w") as f:
                json.dump(result, f, default=str)
        except Exception:
            pass
        return result


class SMCWorker(QThread):
    done  = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, oi_intel):
        super().__init__()
        self.oi_intel = oi_intel

    def run(self):
        ks = KiteSession.get()
        if not ks.ok or not SMC_OK:
            self.error.emit("Kite not connected or smc_engine.py missing"); return
        try:
            c15 = fetch_nifty_candles_kite(ks.kite, interval="15minute", days=5)
            c1h = fetch_nifty_candles_kite(ks.kite, interval="60minute", days=15)
            if not c15: self.error.emit("Could not fetch 15m candles"); return
            r = get_smc_confluence(oi_intel=self.oi_intel, candles_15m=c15, candles_1h=c1h or None)
            r["_fetched_at"] = datetime.now().strftime("%H:%M:%S")
            self.done.emit(r)
            try: _alert_smc(r)
            except Exception: pass
        except Exception as e:
            self.error.emit(str(e))


class BosWorker(QThread):
    """
    v2 BOS Worker — runs the full upgraded scan:
      • BOS / CHoCH detection with candle quality + volume filter
      • Hourly high/low break scan (separate pass)
      • OB retest check for today's prior BOS setups
      • Daily EMA7/20 confirmation
    Emits: done(bos_events: list, hourly_events: list)
    """
    done = pyqtSignal(list, list)   # (bos_events, hourly_events)

    def __init__(self, db, symbols, ltp_dict=None):
        super().__init__()
        self.db = db
        self.symbols = symbols
        self.ltp_dict = ltp_dict or {}

    def run(self):
        bos_events    = []
        hourly_events = []
        if not BOS_OK or not self.db:
            self.done.emit(bos_events, hourly_events)
            return

        kite    = KiteSession.get().kite if KiteSession.get().ok else None
        tg_func = _tg_bg   # fires real Telegram alerts from this worker

        # ── BOS scan ──────────────────────────────────────────
        for symbol in self.symbols:
            try:
                candles = self.db.get(symbol, n=BOS_LOOKBACK)
                if not candles or len(candles) < 10: continue
                try:
                    if datetime.strptime(candles[-1]["datetime"][:10],
                                         "%Y-%m-%d").date() < date.today():
                        continue
                except Exception:
                    pass
                bos = detect_bos(candles)
                if not bos["bos_type"]: continue
                ltp   = self.ltp_dict.get(symbol, candles[-1]["close"])
                setup = build_bos_setup(bos, symbol, ltp)

                # Daily EMA check (non-blocking — returns unknown if no Kite)
                direction = "UP" if bos["bos_type"] in ("BOS_UP","CHOCH_UP") else "DOWN"
                try:
                    ema = get_daily_ema(symbol, kite)
                    ltp_vs = ema.get("ltp_vs_ema","unknown")
                    if direction == "UP":
                        ema_ok = ltp_vs in ("above","unknown")
                        ema_note = f"Daily EMA7:{ema.get('ema7',0):.1f} EMA20:{ema.get('ema20',0):.1f}"
                    else:
                        ema_ok = ltp_vs in ("below","unknown")
                        ema_note = f"Daily EMA7:{ema.get('ema7',0):.1f} EMA20:{ema.get('ema20',0):.1f}"
                except Exception:
                    ema_ok = True; ema_note = ""

                setup["ema_ok"]       = ema_ok
                setup["ema_note"]     = ema_note
                setup["ema_filtered"] = not ema_ok

                # ── Fire BOS Telegram alert ──────────────────────────
                if ema_ok:
                    try:
                        from bos_scanner import (build_bos_telegram, _already_alerted,
                                                  _mark_alerted)
                        already = _already_alerted(symbol, bos["bos_type"], bos["broken_level"])
                        if not already:
                            tg_func(
                                build_bos_telegram(setup, bos, ema_note),
                                dedup_key=f"BOS_1H_{symbol}_{bos['bos_type']}_{round(bos['broken_level']/5)*5}"
                            )
                            _mark_alerted(symbol, bos["bos_type"], bos["broken_level"])
                    except Exception:
                        pass

                bos_events.append({"setup": setup, "bos": bos})

                # OB retest — fires Telegram when price returns to OB zone
                try:
                    prior = _todays_bos_setups.get(symbol)
                    if prior:
                        check_ob_retest(symbol, prior, ltp,
                                        tg_func, tg_enabled=True)
                except Exception:
                    pass
            except Exception:
                continue

        # ── Hourly break scan — fires live Telegram alerts ───────
        try:
            hourly_events = scan_hourly_breaks(
                self.db, self.symbols,
                tg_func,
                ltp_dict=self.ltp_dict,
                kite=kite,
                tg_enabled=True,
            )
        except Exception:
            hourly_events = []

        self.done.emit(bos_events, hourly_events)


# ─────────────────────────────────────────────────────────────────────────────
# SESSION PANEL
# ─────────────────────────────────────────────────────────────────────────────

class SessionPanel(QWidget):
    connected = pyqtSignal()

    def __init__(self):
        super().__init__()
        root = QVBoxLayout(self)
        root.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.setSpacing(16)

        root.addWidget(make_label("🔌  Kite Session", 18, bold=True, color=C_BLUE,
                                  align=Qt.AlignmentFlag.AlignCenter))
        info = make_label(
            "Place your access_token.txt in the same folder as qt_dashboard.py, "
            "then click Connect.\nThe dashboard will read live OI + SMC data from Kite.",
            12, color=C_DIM, align=Qt.AlignmentFlag.AlignCenter)
        info.setWordWrap(True)
        root.addWidget(info)

        path_frame, path_lay = card_frame(QHBoxLayout)
        path_lay.addWidget(make_label("Token file:", 11, color=C_DIM))
        self.path_edit = QLineEdit(ACCESS_TOKEN_FILE)
        self.path_edit.setMinimumWidth(360)
        path_lay.addWidget(self.path_edit)
        root.addWidget(path_frame)

        btn = QPushButton("🔌  Connect to Kite")
        btn.setFixedWidth(220)
        btn.setStyleSheet(
            "QPushButton{background:#1f6feb;color:#fff;font-size:14px;"
            "border:none;padding:10px 20px;border-radius:6px;}"
            "QPushButton:hover{background:#1158c7;}")
        btn.clicked.connect(self._connect)
        root.addWidget(btn, alignment=Qt.AlignmentFlag.AlignCenter)

        self.status_lbl = make_label("", 11, color=C_RED, align=Qt.AlignmentFlag.AlignCenter)
        self.status_lbl.setWordWrap(True)
        root.addWidget(self.status_lbl)
        root.addStretch()

    def _connect(self):
        ks = KiteSession.get()
        self.status_lbl.setText("Connecting…")
        self.status_lbl.setStyleSheet(f"color:{C_YELLOW};")
        QApplication.processEvents()
        if ks.connect(token_file=self.path_edit.text().strip() or ACCESS_TOKEN_FILE):
            self.status_lbl.setText(ks.status)
            self.status_lbl.setStyleSheet(f"color:{C_GREEN};")
            self.connected.emit()
        else:
            self.status_lbl.setText(f"❌ {ks.error}")
            self.status_lbl.setStyleSheet(f"color:{C_RED};")


# ─────────────────────────────────────────────────────────────────────────────
# OI INTELLIGENCE PANEL
# ─────────────────────────────────────────────────────────────────────────────

class OIPanel(QWidget):
    oi_updated = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        self._oi_data     = {}
        self._worker      = None
        self._snapshots   = []   # list of (timestamp_str, oi_dict) for 30-min summary
        self._last_snap_slot = ""  # "HH:M0" — slot key
        self._build_ui()
        self._try_cache()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12)
        root.setSpacing(12)

        # ── Header ────────────────────────────────────────────
        hdr = QHBoxLayout()
        hdr.addWidget(section_title("OI Intelligence — NIFTY Options Chain"))
        hdr.addStretch()
        self.btn_refresh = QPushButton("⟳  Refresh OI")
        self.btn_refresh.setFixedSize(126, 30)
        self.btn_refresh.clicked.connect(self._fetch)
        hdr.addWidget(self.btn_refresh)
        root.addLayout(hdr)

        # ── Stat pills ────────────────────────────────────────
        pills = QHBoxLayout(); pills.setSpacing(10)
        self.p_spot   = stat_pill("NIFTY Spot", "—", C_CYAN,   165)
        self.p_atm    = stat_pill("ATM Strike", "—", C_YELLOW, 148)
        self.p_pain   = stat_pill("Max Pain",   "—", C_PURPLE, 148)
        self.p_pcr    = stat_pill("PCR",        "—", C_MUTED,  110)
        self.p_expiry = stat_pill("Expiry",     "—", C_DIM,    138)
        self.p_ts     = stat_pill("Last Fetch", "—", C_DIM,    110)
        for p in [self.p_spot,self.p_atm,self.p_pain,self.p_pcr,self.p_expiry,self.p_ts]:
            pills.addWidget(p)
        pills.addStretch()
        root.addLayout(pills)

        # ── Direction banner ──────────────────────────────────
        df, dl = card_frame(QVBoxLayout, (14, 11, 14, 11))
        self._dir_card = df
        r1 = QHBoxLayout(); r1.setSpacing(12)
        self.lbl_dir = QLabel("—")
        self.lbl_dir.setFont(QFont("Segoe UI", 17, QFont.Weight.Bold))
        self.lbl_dir.setStyleSheet(f"color:{C_YELLOW};background:transparent;")
        self.lbl_dr = QLabel("—")
        self.lbl_dr.setFont(QFont("Segoe UI", 11))
        self.lbl_dr.setStyleSheet(f"color:{C_MUTED};background:transparent;")
        sv = separator_line(True); sv.setFixedHeight(22)
        r1.addWidget(self.lbl_dir); r1.addWidget(sv); r1.addWidget(self.lbl_dr); r1.addStretch()
        dl.addLayout(r1)
        r2 = QHBoxLayout(); r2.setSpacing(8)
        self.lbl_advice = QLabel("—")
        self.lbl_advice.setFont(QFont("Segoe UI", 10))
        self.lbl_advice.setStyleSheet(f"color:{C_DIM};background:transparent;")
        sc_lbl = QLabel("Setup:")
        sc_lbl.setFont(QFont("Segoe UI", 10))
        sc_lbl.setStyleSheet(f"color:{C_DIM};background:transparent;")
        self.lbl_setup = QLabel("—")
        self.lbl_setup.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self.lbl_setup.setStyleSheet(f"color:{C_GREEN};background:transparent;")
        r2.addWidget(self.lbl_advice); r2.addWidget(sc_lbl); r2.addWidget(self.lbl_setup); r2.addStretch()
        dl.addLayout(r2)
        root.addWidget(df)

        # ── Key levels ────────────────────────────────────────
        lf, ll = card_frame(QHBoxLayout, (12, 8, 12, 8)); ll.setSpacing(12)
        self.b_cw  = mini_badge("Call Wall",  "—", C_RED)
        self.b_pf  = mini_badge("Put Floor",  "—", C_GREEN)
        self.b_sce = mini_badge("Str CE OI",  "—", "#ff8a80")
        self.b_spe = mini_badge("Str PE OI",  "—", "#69f0ae")
        self.lbl_pain_sig = QLabel("—")
        self.lbl_pain_sig.setFont(QFont("Segoe UI", 10))
        self.lbl_pain_sig.setStyleSheet(f"color:{C_MUTED};background:transparent;")
        for w in [self.b_cw,self.b_pf,self.b_sce,self.b_spe]: ll.addWidget(w)
        ll.addStretch(); ll.addWidget(self.lbl_pain_sig)
        root.addWidget(lf)

        # ── Chain header ──────────────────────────────────────
        ch_hdr = QHBoxLayout()
        ch_hdr.addWidget(section_title("Options Chain  (ATM ±10 strikes)"))
        ch_hdr.addStretch()
        guide = QLabel("← CE side   STRIKE   PE side →")
        guide.setFont(QFont("Segoe UI", 9))
        guide.setStyleSheet(f"color:{C_DIM};background:transparent;")
        ch_hdr.addWidget(guide)
        root.addLayout(ch_hdr)

        cols = ["CE IV", "CE OI Δ", "CE OI", "CE LTP",
                "STRIKE", "STATUS",
                "PE LTP", "PE OI", "PE OI Δ", "PE IV"]
        self.chain_tbl = QTableWidget(0, len(cols))
        self.chain_tbl.setHorizontalHeaderLabels(cols)
        self.chain_tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.chain_tbl.horizontalHeader().setMinimumSectionSize(60)
        self.chain_tbl.verticalHeader().setVisible(False)
        self.chain_tbl.setAlternatingRowColors(True)
        self.chain_tbl.setShowGrid(False)
        self.chain_tbl.setStyleSheet("""
            QTableWidget {
                alternate-background-color: #F8F9FA;
                background-color: #FFFFFF;
                border: 1px solid #DDE1E8;
                border-radius: 4px;
                font-size: 12px;
                selection-background-color: #BEE3F8;
                gridline-color: #EEF1F6;
            }
            QTableWidget::item {
                padding: 6px 12px;
                border-bottom: 1px solid #EEF1F6;
                color: #1A1D23;
            }
            QHeaderView::section {
                background: #F8F9FA;
                color: #4A5568;
                font-size: 11px;
                font-weight: 700;
                letter-spacing: 0.3px;
                padding: 8px 12px;
                border: none;
                border-right: 1px solid #E2E8F0;
                border-bottom: 2px solid #CBD5E0;
            }
            QHeaderView { background: #F8F9FA; }
        """)
        root.addWidget(self.chain_tbl)

        # ── 30-Min OI Summary ────────────────────────────────────
        sum_hdr = QHBoxLayout()
        sum_hdr.addWidget(section_title("OI Summary (30-min snapshots)"))
        sum_hdr.addStretch()
        self.btn_clear_snap = QPushButton("Clear")
        self.btn_clear_snap.setFixedSize(60, 26)
        self.btn_clear_snap.clicked.connect(self._clear_snapshots)
        sum_hdr.addWidget(self.btn_clear_snap)
        root.addLayout(sum_hdr)

        self.lbl_oi_summary = QLabel("No snapshots yet — summary builds every 30 min.")
        self.lbl_oi_summary.setWordWrap(True)
        self.lbl_oi_summary.setStyleSheet("""
            QLabel {
                background: #F8FAFC;
                border: 1px solid #DDE1E8;
                border-left: 4px solid #2C5282;
                border-radius: 0px 6px 6px 0px;
                padding: 12px 16px;
                color: #2D3748;
                font-size: 12px;
                font-family: "Consolas", "Courier New", monospace;
                line-height: 1.8;
            }
        """)
        self.lbl_oi_summary.setMinimumHeight(80)
        self.lbl_oi_summary.setMaximumHeight(220)
        root.addWidget(self.lbl_oi_summary)

        self.status_lbl = make_label("Loading cache…", 10, color=C_DIM)
        root.addWidget(self.status_lbl)

    # stat_pill / mini_badge are module-level helpers — no instance methods needed

    def _sp(self, p, v):
        if p and hasattr(p,'_v'): p._v.setText(str(v))
    def _sb(self, b, v):
        if b and hasattr(b,'_v'): b._v.setText(str(v))

    def _try_cache(self):
        try:
            if os.path.exists(OI_CACHE_FILE):
                with open(OI_CACHE_FILE) as f:
                    cached = json.load(f)
                self._oi_data = cached
                self._populate(cached)
                self.status_lbl.setText(f"Cache loaded ({cached.get('timestamp','')}) — click Refresh for live data")
                return
        except Exception:
            pass
        self.status_lbl.setText("No cache — connect Kite and click Refresh")

    def _fetch(self):
        ks = KiteSession.get()
        if not ks.ok:
            self.status_lbl.setText("❌ Kite not connected — use Session tab")
            return
        if self._worker and self._worker.isRunning():
            return
        self.btn_refresh.setEnabled(False); self.btn_refresh.setText("Fetching…")
        self.status_lbl.setText("Fetching live NIFTY OI from Kite…")
        self._worker = OIWorker()
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_done(self, data):
        self.btn_refresh.setEnabled(True); self.btn_refresh.setText("⟳  Refresh OI")
        self._oi_data = data
        self._populate(data)
        self._maybe_snapshot(data)
        self.oi_updated.emit(data)

    def _maybe_snapshot(self, data):
        """Store a snapshot every 30 minutes and rebuild the summary."""
        now     = datetime.now()
        # slot = "HH:00" or "HH:30"
        minute  = (now.minute // 30) * 30
        slot    = now.strftime(f"%H:{minute:02d}")
        if slot == self._last_snap_slot:
            return   # already snapped this 30-min window
        self._last_snap_slot = slot
        ts = now.strftime("%H:%M")
        self._snapshots.append((ts, data))
        # Keep last 8 snapshots (4 hours)
        self._snapshots = self._snapshots[-8:]
        self._rebuild_summary()

    def _clear_snapshots(self):
        self._snapshots = []; self._last_snap_slot = ""
        self.lbl_oi_summary.setText("Snapshots cleared.")

    def _rebuild_summary(self):
        """Build a formatted OI delta summary across snapshots."""
        if not self._snapshots:
            self.lbl_oi_summary.setText("No snapshots yet."); return
        if len(self._snapshots) == 1:
            ts, d = self._snapshots[0]
            spot  = d.get("spot", 0); atm = d.get("atm", 0)
            pcr   = d.get("pcr", 0); dirc = d.get("direction","—")
            tot_ce= d.get("total_ce_oi", 0); tot_pe = d.get("total_pe_oi", 0)
            self.lbl_oi_summary.setText(
                "\U0001f4f8 " + ts + f"  Spot: {spot:,.0f}  ATM: {atm}  PCR: {pcr}\n"
                + f"OI Direction: {dirc}\n"
                + f"Total CE OI: {tot_ce/100000:.1f}L  |  Total PE OI: {tot_pe/100000:.1f}L"
            )  # single snapshot displayed
        ts0, d0 = self._snapshots[0]
        ts1, d1 = self._snapshots[-1]
        spot0    = d0.get("spot", 0);     spot1    = d1.get("spot", 0)
        pcr0     = d0.get("pcr", 0);      pcr1     = d1.get("pcr", 0)
        ce0      = d0.get("total_ce_oi",0); ce1    = d1.get("total_ce_oi",0)
        pe0      = d0.get("total_pe_oi",0); pe1    = d1.get("total_pe_oi",0)
        dirc     = d1.get("direction","—")
        cw       = d1.get("nearest_call_wall","—")
        pf       = d1.get("nearest_put_floor","—")
        dr       = d1.get("direction_reason","")

        spot_chg = spot1 - spot0
        pcr_chg  = round(pcr1 - pcr0, 2)
        ce_chg   = ce1 - ce0
        pe_chg   = pe1 - pe0

        # Interpret PCR shift
        if pcr_chg > 0.05:   pcr_note = "↑ PEs being written (put support building)"
        elif pcr_chg < -0.05: pcr_note = "↓ CEs being written (call pressure rising)"
        else:                  pcr_note = "→ PCR stable"

        # Interpret OI shift
        ce_arrow = "▲" if ce_chg > 0 else "▼"
        pe_arrow = "▲" if pe_chg > 0 else "▼"

        # Overall bias from latest direction
        bull_pct = 100 if "BULLISH" in dirc and "BIAS" not in dirc else                    70  if "BULLISH BIAS" in dirc else                    0   if "BEARISH" in dirc and "BIAS" not in dirc else                    30  if "BEARISH BIAS" in dirc else 50
        bear_pct = 100 - bull_pct

        lines = [
            f"OI SUMMARY — {ts0}→{ts1}",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Spot: {spot1:,.0f}  ({'+' if spot_chg>=0 else ''}{spot_chg:.0f} pts)  |  ATM: {d1.get('atm','—')}",
            f"PCR: {pcr1}  ({'+' if pcr_chg>=0 else ''}{pcr_chg})  |  {pcr_note}",
            f"Signals:  Bull {bull_pct}%  |  Bear {bear_pct}%",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"CE OI:  {ce1/100000:.1f}L  ({ce_arrow}{abs(ce_chg/100000):.1f}L)  |  "
            f"PE OI: {pe1/100000:.1f}L  ({pe_arrow}{abs(pe_chg/100000):.1f}L)",
            f"Call Wall: {cw}  |  Put Floor: {pf}",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"OI Direction: {dirc}",
            f"Reason: {dr}",
            f"Overall: {'BULLISH — Market likely targeting ' + str(cw) if bull_pct>=70 else 'BEARISH — Watch ' + str(pf) + ' support' if bear_pct>=70 else 'SIDEWAYS/MIXED — Trade breakouts only'}",
            f"NOT financial advice.",
        ]
        self.lbl_oi_summary.setText("\n".join(lines))

    def _on_error(self, err):
        self.btn_refresh.setEnabled(True); self.btn_refresh.setText("⟳  Refresh OI")
        self.status_lbl.setText(f"❌ {err}")

    def _populate(self, d):
        if not d: return
        spot = d.get("spot", 0); atm = d.get("atm", 0)
        self._sp(self.p_spot, f"{spot:,.1f}"); self._sp(self.p_atm, str(atm))
        self._sp(self.p_pain, str(d.get("max_pain", "—"))); self._sp(self.p_pcr, str(d.get("pcr", "—")))
        self._sp(self.p_expiry, d.get("expiry", "—")); self._sp(self.p_ts, d.get("timestamp", "—"))

        dirc = d.get("direction", "—")
        dc = C_GREEN if "BULL" in dirc else (C_RED if "BEAR" in dirc else C_YELLOW)
        glow = C_GREEN if "BULL" in dirc else (C_RED if "BEAR" in dirc else C_YELLOW)
        bg   = "#002a15" if "BULL" in dirc else ("#2a0008" if "BEAR" in dirc else "#1a1500")
        self.lbl_dir.setText(dirc)
        self.lbl_dir.setStyleSheet(f"color:{dc};font-size:16px;font-weight:bold;")
        self.lbl_dr.setText(d.get("direction_reason", ""))
        self.lbl_advice.setText(d.get("advice", ""))
        setup = d.get("setup", "—")
        sc = C_GREEN if "CE" in setup else C_RED
        self.lbl_setup.setText(setup); self.lbl_setup.setStyleSheet(f"color:{sc};font-weight:700;")
        # Dynamically update direction card background+border
        try:
            bg_tint = "#F0FFF4" if "BULL" in dirc else ("#FFF5F5" if "BEAR" in dirc else "#FFFDE7")
            border_color = glow
            self._dir_card.setStyleSheet(f"""
                QFrame {{
                    background: {bg_tint};
                    border: 1px solid {border_color}50;
                    border-left: 5px solid {border_color};
                    border-radius: 0px 8px 8px 0px;
                }}
            """)
        except Exception: pass

        ncw = d.get("nearest_call_wall"); npf = d.get("nearest_put_floor")
        self._sb(self.b_cw,  str(ncw) if ncw else "—")
        self._sb(self.b_pf,  str(npf) if npf else "—")
        self._sb(self.b_sce, f"{d.get('strongest_ce','—')} ({fmt_oi(d.get('ce_oi',0))})")
        self._sb(self.b_spe, f"{d.get('strongest_pe','—')} ({fmt_oi(d.get('pe_oi',0))})")
        self.lbl_pain_sig.setText(f"📌 {d.get('pain_signal','')}")

        rows = d.get("chain_rows", [])
        self.chain_tbl.setRowCount(0)
        for row in rows:
            s = row["STRIKE"]; is_atm = (s == atm); is_itm = (s < atm)
            r = self.chain_tbl.rowCount(); self.chain_tbl.insertRow(r)
            self.chain_tbl.setRowHeight(r, 26)

            # ATM gets a special highlight background
            if is_atm:
                bg = QColor("#0d1f40")
            elif is_itm:
                bg = QColor("#080f1c")
            else:
                bg = QColor("#090c14")

            def _it(text, color, bold=False):
                item = QTableWidgetItem(str(text))
                item.setForeground(QColor(color))
                item.setBackground(QBrush(bg))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if bold:
                    f = QFont("Segoe UI", 12); f.setBold(True); item.setFont(f)
                return item

            oi_add_c = lambda v: C_GREEN if v > 0 else (C_RED if v < 0 else C_DIM)
            self.chain_tbl.setItem(r, 0, _it(f"{row['CE_IV']:.1f}%",  C_DIM))
            self.chain_tbl.setItem(r, 1, _it(fmt_oi(row["CE_OI_ADD"]), oi_add_c(row["CE_OI_ADD"])))
            self.chain_tbl.setItem(r, 2, _it(fmt_oi(row["CE_OI"]),     "#ff8a80"))
            self.chain_tbl.setItem(r, 3, _it(f"{row['CE_LTP']:.1f}",   "#ff5252"))

            # Strike — bold + yellow for ATM
            si = QTableWidgetItem("◆ " + str(s) if is_atm else str(s))
            si.setForeground(QColor(C_YELLOW if is_atm else C_TEXT))
            si.setBackground(QBrush(bg))
            si.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            sf = QFont("Segoe UI", 13 if is_atm else 12)
            sf.setBold(is_atm); si.setFont(sf)
            self.chain_tbl.setItem(r, 4, si)

            status_color = C_YELLOW if is_atm else ("#69f0ae" if is_itm else "#ef9a9a")
            self.chain_tbl.setItem(r, 5, _it(row["STATUS"], status_color))
            self.chain_tbl.setItem(r, 6, _it(f"{row['PE_LTP']:.1f}",   "#69f0ae"))
            self.chain_tbl.setItem(r, 7, _it(fmt_oi(row["PE_OI"]),     "#00e676"))
            self.chain_tbl.setItem(r, 8, _it(fmt_oi(row["PE_OI_ADD"]), oi_add_c(row["PE_OI_ADD"])))
            self.chain_tbl.setItem(r, 9, _it(f"{row['PE_IV']:.1f}%",   C_DIM))

        self.chain_tbl.resizeColumnsToContents()
        self.status_lbl.setText(
            f"{len(rows)} strikes | Expiry {d.get('expiry','—')} | "
            f"Total CE: {fmt_oi(d.get('total_ce_oi',0))} | Total PE: {fmt_oi(d.get('total_pe_oi',0))}")

    def get_oi_data(self): return self._oi_data
    def trigger_refresh(self): self._fetch()


# ─────────────────────────────────────────────────────────────────────────────
# SMC + OI CONFLUENCE PANEL
# ─────────────────────────────────────────────────────────────────────────────

class SMCPanel(QWidget):
    def __init__(self, oi_panel):
        super().__init__()
        self._oi_panel = oi_panel; self._worker = None
        self._build_ui()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12); root.setSpacing(12)

        hdr = QHBoxLayout()
        hdr.addWidget(section_title("SMC + OI Confluence Engine"))
        hdr.addStretch()
        self.btn_run = QPushButton("⟳  Run Analysis")
        self.btn_run.setFixedSize(140, 30)
        self.btn_run.clicked.connect(self._run)
        hdr.addWidget(self.btn_run); root.addLayout(hdr)

        # ── Signal banner ─────────────────────────────────────
        sf, sl = card_frame(QVBoxLayout, (16, 14, 16, 14))
        inner = QHBoxLayout(); inner.setSpacing(20)
        left = QVBoxLayout(); left.setSpacing(6)
        self.lbl_signal = QLabel("— Run SMC Analysis —")
        self.lbl_signal.setFont(QFont("Segoe UI", 18, QFont.Weight.Bold))
        self.lbl_signal.setStyleSheet(f"color:{C_YELLOW};background:transparent;")
        self.lbl_signal.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_action = QLabel("")
        self.lbl_action.setFont(QFont("Segoe UI", 11))
        self.lbl_action.setStyleSheet(f"color:{C_DIM};background:transparent;")
        self.lbl_action.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_score = QLabel("Score: —")
        self.lbl_score.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
        self.lbl_score.setStyleSheet(f"color:{C_DIM};background:transparent;")
        self.lbl_score.setAlignment(Qt.AlignmentFlag.AlignCenter)
        left.addWidget(self.lbl_signal); left.addWidget(self.lbl_action); left.addWidget(self.lbl_score)
        inner.addLayout(left, stretch=2)
        inner.addWidget(separator_line(True))

        # Metrics mini-grid
        mg = QGridLayout(); mg.setSpacing(12); mg.setContentsMargins(8,4,8,4)
        self._m = {}
        for i,(label,key) in enumerate([
            ("OI Direction","oi_direction"),("SMC LTF","smc_trend_ltf"),
            ("SMC HTF","smc_trend_htf"),("P/D Zone","pd_zone"),
            ("PCR","oi_pcr"),("Conflict","conflict"),
        ]):
            col=i%3; row=i//3; vb=QVBoxLayout(); vb.setSpacing(2)
            cap=QLabel(label); cap.setFont(QFont("Segoe UI",8,QFont.Weight.Bold))
            cap.setStyleSheet(f"color:{C_DIM};background:transparent;letter-spacing:0.8px;")
            lbl=QLabel("—"); lbl.setFont(QFont("Segoe UI",12,QFont.Weight.Bold))
            lbl.setStyleSheet(f"color:{C_YELLOW};background:transparent;")
            vb.addWidget(cap); vb.addWidget(lbl)
            mg.addLayout(vb, row, col); self._m[key] = lbl
        mf=QFrame(); mf.setStyleSheet("QFrame{background:transparent;border:none;}")
        mf.setLayout(mg)
        inner.addWidget(mf, stretch=1)
        sl.addLayout(inner); root.addWidget(sf)

        # ── Key levels ────────────────────────────────────────
        klf, kll = card_frame(QHBoxLayout, (12, 8, 12, 8)); kll.setSpacing(10)
        self.kl = {}
        for label,color in [
            ("Call Wall",C_RED),("Put Floor",C_GREEN),
            ("Bull OB",C_GREEN),("Bear OB",C_RED),
            ("Bull FVG",C_GREEN),("Bear FVG",C_RED),
            ("Buy Liq",C_YELLOW),("Sell Liq",C_YELLOW),
        ]:
            vb=QVBoxLayout(); vb.setSpacing(1)
            cap=QLabel(label); cap.setFont(QFont("Segoe UI",8)); cap.setStyleSheet(f"color:{C_DIM};background:transparent;")
            lbl=QLabel("—"); lbl.setFont(QFont("Segoe UI",12,QFont.Weight.Bold))
            lbl.setStyleSheet(f"color:{color};background:transparent;")
            vb.addWidget(cap); vb.addWidget(lbl); kll.addLayout(vb); self.kl[label]=lbl
        kll.addStretch(); root.addWidget(klf)

        # ── Reasons table ─────────────────────────────────────
        root.addWidget(section_title("Analysis Reasons"))
        self.reasons_tbl = QTableWidget(0, 1)
        self.reasons_tbl.horizontalHeader().setVisible(False)
        self.reasons_tbl.verticalHeader().setVisible(False)
        self.reasons_tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.reasons_tbl.setMaximumHeight(175)
        self.reasons_tbl.setAlternatingRowColors(True)
        self.reasons_tbl.setShowGrid(False)
        self.reasons_tbl.setStyleSheet("QTableWidget{alternate-background-color:#FAFAFA;}")
        root.addWidget(self.reasons_tbl)

        # ── Trade setup ───────────────────────────────────────
        root.addWidget(section_title("Trade Setup"))
        tsf, tsl = card_frame(QVBoxLayout, (12, 10, 12, 10))
        self.lbl_setup = QLabel("—")
        self.lbl_setup.setFont(QFont("Segoe UI", 11))
        self.lbl_setup.setStyleSheet(f"color:{C_MUTED};background:transparent;")
        self.lbl_setup.setWordWrap(True)
        tsl.addWidget(self.lbl_setup); root.addWidget(tsf)

        self.status_lbl = QLabel("Fetch OI first, then run SMC analysis.")
        self.status_lbl.setFont(QFont("Segoe UI",10))
        self.status_lbl.setStyleSheet(f"color:{C_DIM};background:transparent;")
        root.addWidget(self.status_lbl)

    def _run(self):
        ks = KiteSession.get()
        if not ks.ok: self.status_lbl.setText("❌ Kite not connected"); return
        oi = self._oi_panel.get_oi_data()
        if not oi: self.status_lbl.setText("⚠️ Fetch OI data first"); return
        if self._worker and self._worker.isRunning(): return
        self.btn_run.setEnabled(False); self.btn_run.setText("Analysing…")
        self.status_lbl.setText("Fetching 15m + 1h candles and running SMC analysis…")
        self._worker = SMCWorker(oi)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_done(self, data):
        self.btn_run.setEnabled(True); self.btn_run.setText("⟳  Run SMC Analysis")
        self._populate(data)

    def _on_error(self, err):
        self.btn_run.setEnabled(True); self.btn_run.setText("⟳  Run SMC Analysis")
        self.status_lbl.setText(f"❌ {err}")

    def _populate(self, d):
        score   = d.get("final_score", 0); signal = d.get("final_signal", "—")
        action  = d.get("final_action", "—"); color = d.get("signal_color", "grey")
        conflict= d.get("conflict_detected", False); ts = d.get("_fetched_at", "")
        cm = {"green": C_GREEN, "red": C_RED, "yellow": C_YELLOW, "grey": C_DIM}
        sc = cm.get(color, C_DIM)

        self.lbl_signal.setText(signal)
        self.lbl_signal.setStyleSheet(f"color:{sc};font-size:17px;font-weight:bold;")
        self.lbl_action.setText(action)
        self.lbl_score.setText(f"Score: {score:+d}")
        self.lbl_score.setStyleSheet(f"color:{C_GREEN if score>0 else (C_RED if score<0 else C_YELLOW)};font-size:13px;font-weight:bold;")

        tc = lambda v: C_GREEN if "BULL" in str(v) else (C_RED if "BEAR" in str(v) else C_YELLOW)
        for k, v in [("oi_direction",d.get("oi_direction","—")),("smc_trend_ltf",d.get("smc_trend_ltf","—")),
                     ("smc_trend_htf",d.get("smc_trend_htf","—")),("pd_zone",d.get("pd_zone","—")),
                     ("oi_pcr",f"{d.get('oi_pcr',0):.2f}")]:
            w = self._m.get(k)
            if w: w.setText(str(v)); w.setStyleSheet(f"color:{tc(v)};font-weight:bold;")
        cw = self._m.get("conflict")
        if cw:
            cw.setText("⚠️ YES" if conflict else "✅ NO")
            cw.setStyleSheet(f"color:{C_YELLOW if conflict else C_GREEN};font-weight:bold;")

        # Key levels
        def _kl(key, val):
            w = self.kl.get(key)
            if not w: return
            w.setText(f"{val:.0f}" if val else "—")
        _kl("Call Wall", d.get("oi_call_wall")); _kl("Put Floor", d.get("oi_put_floor"))
        ob_b = d.get("nearest_bullish_ob"); ob_br = d.get("nearest_bearish_ob")
        fvg_b= d.get("nearest_bullish_fvg"); fvg_br= d.get("nearest_bearish_fvg")
        self.kl["Bull OB"].setText(f"{ob_b.get('low',0):.0f}–{ob_b.get('high',0):.0f}" if ob_b else "—")
        self.kl["Bear OB"].setText(f"{ob_br.get('low',0):.0f}–{ob_br.get('high',0):.0f}" if ob_br else "—")
        self.kl["Bull FVG"].setText(f"{fvg_b.get('bottom',0):.0f}–{fvg_b.get('top',0):.0f}" if fvg_b else "—")
        self.kl["Bear FVG"].setText(f"{fvg_br.get('bottom',0):.0f}–{fvg_br.get('top',0):.0f}" if fvg_br else "—")
        bl = d.get("nearest_buy_liq"); sl = d.get("nearest_sell_liq")
        self.kl["Buy Liq"].setText(f"{bl:.0f}" if bl else "—")
        self.kl["Sell Liq"].setText(f"{sl:.0f}" if sl else "—")

        # Reasons
        reasons = d.get("reasons", [])
        self.reasons_tbl.setRowCount(0)
        for reason in reasons:
            r = self.reasons_tbl.rowCount(); self.reasons_tbl.insertRow(r)
            item = QTableWidgetItem(reason)
            rc = C_GREEN if any(x in reason for x in ["✅","🟢","BUY","BULL"]) else \
                 (C_RED  if any(x in reason for x in ["❌","🔴","SELL","BEAR"]) else C_DIM)
            item.setForeground(QColor(rc)); self.reasons_tbl.setItem(r, 0, item)

        setup = d.get("setup", {})
        if isinstance(setup, dict):
            self.lbl_setup.setText("  |  ".join(f"{k}: {v}" for k, v in setup.items()))
        else:
            self.lbl_setup.setText(str(setup))

        self.status_lbl.setText(
            f"SMC @ {ts} | Score {score:+d} | LTF:{d.get('smc_trend_ltf','?')} "
            f"HTF:{d.get('smc_trend_htf','?')} | P/D:{d.get('pd_zone','?')} "
            f"({d.get('pd_zone_pct',0):.0f}%) | {d.get('oi_smc_interpretation','')}")

    def on_oi_updated(self, oi_data):
        ks = KiteSession.get()
        if ks.ok and SMC_OK and oi_data:
            self._run()


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 PANELS (BOS, Astro, Time, DB) — unchanged from Phase 1
# ─────────────────────────────────────────────────────────────────────────────

class BosPanel(QWidget):
    """
    v2 BOS Panel — three sub-tabs:
      1. BOS / CHoCH Events (with EMA filter badge + OB retest tracking)
      2. Hourly High/Low Breaks
      3. OB Retest Watch (entries where price returned to OB)
    """
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.symbols  = []
        self.bos_events     = []
        self.hourly_events  = []
        self.worker   = None
        self._build_ui()
        self._load_symbols()
        self._try_cache()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12); root.setSpacing(12)

        # ── Header ────────────────────────────────────────────────
        hdr = QHBoxLayout()
        hdr.addWidget(section_title("BOS / CHoCH + Hourly Break Scanner  v2"))
        hdr.addStretch()
        self.scan_btn = QPushButton("⟳  Scan Now")
        self.scan_btn.setFixedSize(120, 30)
        self.scan_btn.clicked.connect(self.run_scan)
        hdr.addWidget(self.scan_btn)
        root.addLayout(hdr)

        # ── Caption ───────────────────────────────────────────────
        cap = QLabel("BOS/CHoCH: candle quality + volume + daily EMA  |  Hourly: prev-1H break  |  OB Retest: B-entry zone")
        cap.setFont(QFont("Segoe UI", 9)); cap.setWordWrap(True)
        cap.setStyleSheet(f"color:{C_DIM};background:transparent;")
        root.addWidget(cap)

        # ── Summary pills ─────────────────────────────────────────
        sr = QHBoxLayout(); sr.setSpacing(8)
        self.p_total  = stat_pill("BOS Events",    "0", C_BLUE,   110)
        self.p_bull   = stat_pill("Bull",          "0", C_GREEN,  90)
        self.p_bear   = stat_pill("Bear",          "0", C_RED,    90)
        self.p_choch  = stat_pill("CHoCH",         "0", C_PURPLE, 90)
        self.p_hrly   = stat_pill("Hrly Breaks",   "0", C_YELLOW, 110)
        self.p_ema_f  = stat_pill("EMA Filtered",  "0", C_DIM,    110)
        self.lbl_ts   = QLabel("Last scan: —")
        self.lbl_ts.setFont(QFont("Segoe UI",9))
        self.lbl_ts.setStyleSheet(f"color:{C_DIM};background:transparent;")
        for p in [self.p_total,self.p_bull,self.p_bear,self.p_choch,self.p_hrly,self.p_ema_f]:
            sr.addWidget(p)
        sr.addStretch(); sr.addWidget(self.lbl_ts)
        root.addLayout(sr)

        # ── Filters row ───────────────────────────────────────────
        ff, fl = card_frame(QHBoxLayout, (10,7,10,7))
        fl.addWidget(make_label("Type:", 10, color=C_DIM))
        self.cmb = QComboBox()
        self.cmb.addItems(["All","BOS only","CHoCH only","Bullish","Bearish"])
        self.cmb.currentTextChanged.connect(self._apply); fl.addWidget(self.cmb)
        fl.addWidget(make_label("  Min R:R:", 10, color=C_DIM))
        self.srr = QDoubleSpinBox(); self.srr.setRange(0,10); self.srr.setValue(1.5)
        self.srr.setSingleStep(0.5); self.srr.valueChanged.connect(self._apply); fl.addWidget(self.srr)
        fl.addWidget(make_label("  Min Vol:", 10, color=C_DIM))
        self.svol = QDoubleSpinBox(); self.svol.setRange(0,10); self.svol.setValue(1.2)
        self.svol.setSingleStep(0.1); self.svol.valueChanged.connect(self._apply); fl.addWidget(self.svol)
        from PyQt6.QtWidgets import QCheckBox
        self.chk_ema = QCheckBox("Show EMA-filtered")
        self.chk_ema.setStyleSheet(f"color:{C_DIM}; font-size:10px;")
        self.chk_ema.stateChanged.connect(self._apply); fl.addWidget(self.chk_ema)
        fl.addStretch(); root.addWidget(ff)

        # ── Sub-tabs: BOS | Hourly Breaks | OB Retest Watch ──────
        self.sub_tabs = QTabWidget()
        self.sub_tabs.setStyleSheet(
            "QTabBar::tab { min-width:120px; padding:5px 10px; font-size:11px; }")

        # Tab 1 — BOS / CHoCH
        bos_cols = ["Symbol","Type","LTP","Broke","Strength","Vol×",
                    "OB Zone","Next Liq","SL","T1","T2","R:R now","R:R ret","Prior","EMA"]
        self.bos_tbl = self._make_tbl(bos_cols)
        self.sub_tabs.addTab(self.bos_tbl, "📐  BOS / CHoCH")

        # Tab 2 — Hourly Breaks
        hrly_cols = ["Symbol","Direction","LTP","Prev High","Prev Low",
                     "SL","T1","T2","R:R","Vol×","EMA Note"]
        self.hrly_tbl = self._make_tbl(hrly_cols)
        self.sub_tabs.addTab(self.hrly_tbl, "⚡  Hourly Breaks")

        # Tab 3 — OB Retest Watch
        retest_cols = ["Symbol","Type","OB Zone","Entry","SL","T1","T2","R:R","Status"]
        self.retest_tbl = self._make_tbl(retest_cols)
        self.sub_tabs.addTab(self.retest_tbl, "🎯  OB Retest Watch")

        root.addWidget(self.sub_tabs)
        self.status_lbl = make_label("Loading…", 10, color=C_DIM)
        root.addWidget(self.status_lbl)

    # ── Helpers ───────────────────────────────────────────────────

    def _pill(self, label, value, color):
        f = QFrame()
        f.setStyleSheet(f"QFrame{{background:{color}22;border:1px solid {color}66;border-radius:4px;}}")
        l = QHBoxLayout(f); l.setContentsMargins(6,2,6,2); l.setSpacing(4)
        l.addWidget(make_label(label, 9, color=C_DIM))
        lv = make_label(value, 13, bold=True, color=color)
        l.addWidget(lv); f._v = lv; return f

    def _make_tbl(self, cols):
        t = QTableWidget(0, len(cols))
        t.setHorizontalHeaderLabels(cols)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        t.horizontalHeader().setStretchLastSection(True)
        t.verticalHeader().setVisible(False)
        t.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        t.setAlternatingRowColors(True)
        t.setShowGrid(False)
        t.setStyleSheet("QTableWidget{alternate-background-color:#FAFAFA;background:#0b0f1a;} QTableWidget::item{padding:5px 10px;border-bottom:1px solid #0f1828;}")
        return t

    def _load_symbols(self):
        if not DB_OK or not self.db: return
        self.symbols = self.db.get_all_symbols()
        self.status_lbl.setText(f"{len(self.symbols)} symbols in DB")

    def _try_cache(self):
        if not BOS_OK: return
        c = load_scan_cache()
        if c and c.get("events"):
            self.bos_events = c["events"]
            self.lbl_ts.setText(f"Last scan: {c.get('time','')}")
            self._apply(); self._stats()
        else:
            self.status_lbl.setText("No cache — click Scan Now")

    def _stats(self):
        ev = self.bos_events
        bull  = sum(1 for e in ev if "UP"    in e.get("bos_type",""))
        bear  = sum(1 for e in ev if "DOWN"  in e.get("bos_type",""))
        choch = sum(1 for e in ev if "CHOCH" in e.get("bos_type",""))
        filt  = sum(1 for e in ev if e.get("ema_filtered", False))
        self.p_total._v.setText(str(len(ev)))
        self.p_bull._v.setText(str(bull))
        self.p_bear._v.setText(str(bear))
        self.p_choch._v.setText(str(choch))
        self.p_hrly._v.setText(str(len(self.hourly_events)))
        self.p_ema_f._v.setText(str(filt))

    def _apply(self):
        ft        = self.cmb.currentText()
        mr        = self.srr.value()
        mv        = self.svol.value()
        show_filt = self.chk_ema.isChecked()

        fil = []
        for ev in self.bos_events:
            bt    = ev.get("bos_type","")
            rr    = max(ev.get("rr_now") or 0, ev.get("rr_retest") or 0)
            vol   = ev.get("vol_ratio", 1.0)
            ema_f = ev.get("ema_filtered", False)
            if ft == "BOS only"   and "CHOCH" in bt: continue
            if ft == "CHoCH only" and "CHOCH" not in bt: continue
            if ft == "Bullish"    and "DOWN"  in bt: continue
            if ft == "Bearish"    and "UP"    in bt: continue
            if rr < mr or vol < mv: continue
            if ema_f and not show_filt: continue
            fil.append(ev)
        self._fill_bos(fil)
        self._fill_hourly(self.hourly_events)
        self._fill_retest()

    # ── BOS table ─────────────────────────────────────────────────

    def _fill_bos(self, events):
        self.bos_tbl.setRowCount(0)
        for ev in events:
            bt      = ev.get("bos_type","")
            sym     = ev.get("symbol","")
            ltp     = ev.get("ltp", 0)
            broken  = ev.get("broken", 0)
            sl      = ev.get("sl", 0)
            t1      = ev.get("t1", 0)
            t2      = ev.get("t2", 0)
            rr_now  = ev.get("rr_now", 0)
            rr_ret  = ev.get("rr_retest", 0)
            strength= ev.get("strength", 0)
            vol_r   = ev.get("vol_ratio", 1.0)
            obl     = ev.get("ob_low", 0)
            obh     = ev.get("ob_high", 0)
            nxt     = ev.get("next_liq", 0)
            pt      = ev.get("prev_trend","")
            ema_ok  = not ev.get("ema_filtered", False)
            ema_note= ev.get("ema_note","")

            ib = "UP" in bt; ic = "CHOCH" in bt
            icon  = "🚀" if ib and not ic else ("🔄" if ic else "💥")
            tc    = C_GREEN if ib else C_RED
            if ic: tc = C_PURPLE
            if not ema_ok: tc = C_YELLOW   # orange-ish for filtered

            # Row bg: dimmer if EMA filtered
            rb = QColor(C_BG_BULL if ib and ema_ok else
                        ("#002200" if ib else C_BG_BEAR) if not ema_ok else C_BG_BEAR)

            r = self.bos_tbl.rowCount(); self.bos_tbl.insertRow(r)
            self.bos_tbl.setRowHeight(r, 24)

            ema_badge = "✅ EMA" if ema_ok else "⚠️ filt"
            ema_c     = C_GREEN if ema_ok else C_YELLOW

            for col, (text, color) in enumerate([
                (sym,                                   C_BLUE),
                (f"{icon} {bt}",                        tc),
                (f"{ltp:.1f}",                          C_TEXT),
                (f"{broken:.1f}",                       C_YELLOW),
                (f"{strength:.2f}%",                    C_DIM),
                (f"{vol_r:.1f}×",    C_YELLOW if vol_r>=2 else C_TEXT),
                (f"{obl:.1f}–{obh:.1f}",               C_DIM),
                (f"{nxt:.1f}",                          C_DIM),
                (f"{sl:.1f}",                           C_RED),
                (f"{t1:.1f}",                           C_GREEN),
                (f"{t2:.1f}",                           C_GREEN),
                (f"{rr_now}:1",   C_GREEN if rr_now>=2 else C_DIM),
                (f"{rr_ret}:1",   C_GREEN if rr_ret>=2 else C_DIM),
                (pt,                                    C_DIM),
                (ema_badge,                             ema_c),
            ]):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setBackground(QBrush(rb))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.bos_tbl.setItem(r, col, item)

        self.bos_tbl.resizeColumnsToContents()
        self.status_lbl.setText(
            f"BOS: {self.bos_tbl.rowCount()} shown | "
            f"Hourly: {len(self.hourly_events)} | "
            f"Scan: {self.lbl_ts.text()}")

    # ── Hourly break table ────────────────────────────────────────

    def _fill_hourly(self, events):
        self.hrly_tbl.setRowCount(0)
        for ev in events:
            sym   = ev.get("symbol","")
            dirn  = ev.get("direction","")
            ltp   = ev.get("ltp", 0)
            ph    = ev.get("prev_high", 0)
            pl    = ev.get("prev_low", 0)
            sl    = ev.get("sl", 0)
            t1    = ev.get("t1", 0)
            t2    = ev.get("t2", 0)
            rr    = ev.get("rr", 0)
            vol_r = ev.get("vol_ratio", 1.0)
            ema_n = ev.get("ema_note","")

            ib  = dirn == "UP"
            rb  = QColor(C_BG_BULL if ib else C_BG_BEAR)
            dc  = C_GREEN if ib else C_RED
            icon= "⚡↑" if ib else "⚡↓"

            r = self.hrly_tbl.rowCount(); self.hrly_tbl.insertRow(r)
            self.hrly_tbl.setRowHeight(r, 24)

            for col, (text, color) in enumerate([
                (sym,               C_BLUE),
                (f"{icon} {dirn}",  dc),
                (f"{ltp:.1f}",      C_TEXT),
                (f"{ph:.1f}",       C_GREEN if ib else C_DIM),
                (f"{pl:.1f}",       C_RED   if not ib else C_DIM),
                (f"{sl:.1f}",       C_RED),
                (f"{t1:.1f}",       C_GREEN),
                (f"{t2:.1f}",       C_GREEN),
                (f"{rr}:1",         C_GREEN if rr >= 1.5 else C_DIM),
                (f"{vol_r:.1f}×",   C_YELLOW if vol_r>=2 else C_TEXT),
                (ema_n[:30] if ema_n else "—", C_DIM),
            ]):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setBackground(QBrush(rb))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.hrly_tbl.setItem(r, col, item)

        self.hrly_tbl.resizeColumnsToContents()

    # ── OB Retest watch table ─────────────────────────────────────

    def _fill_retest(self):
        """Show all today's BOS setups that have an OB zone — watch for retest."""
        self.retest_tbl.setRowCount(0)
        for ev in self.bos_events:
            obl = ev.get("ob_low", 0)
            obh = ev.get("ob_high", 0)
            if not obl or not obh: continue   # no OB zone defined

            sym    = ev.get("symbol","")
            bt     = ev.get("bos_type","")
            ltp    = ev.get("ltp",0)
            e_ret  = ev.get("entry_retest", obh if "UP" in bt else obl)
            sl     = ev.get("sl",0)
            t1     = ev.get("t1",0)
            t2     = ev.get("t2",0)
            rr_ret = ev.get("rr_retest",0)

            ib = "UP" in bt

            # Determine status relative to OB zone
            if ib:
                if ltp <= obh * 1.003:      status = "🎯 AT OB — ENTRY NOW"; sc = C_GREEN
                elif ltp <= obh * 1.02:     status = f"⬇ Approaching OB ({ltp-obh:.1f} away)"; sc = C_YELLOW
                else:                        status = f"⬆ Above OB ({ltp-obh:.1f} pts)"; sc = C_DIM
            else:
                if ltp >= obl * 0.997:      status = "🎯 AT OB — ENTRY NOW"; sc = C_RED
                elif ltp >= obl * 0.98:     status = f"⬆ Approaching OB ({obl-ltp:.1f} away)"; sc = C_YELLOW
                else:                        status = f"⬇ Below OB ({obl-ltp:.1f} pts)"; sc = C_DIM

            rb = QColor(C_BG_BULL if ib else C_BG_BEAR)
            r  = self.retest_tbl.rowCount(); self.retest_tbl.insertRow(r)
            self.retest_tbl.setRowHeight(r, 24)

            for col, (text, color) in enumerate([
                (sym,                       C_BLUE),
                (bt.replace("_"," "),       C_GREEN if ib else C_RED),
                (f"{obl:.1f}–{obh:.1f}",   C_YELLOW),
                (f"{e_ret:.1f}",            C_GREEN if ib else C_RED),
                (f"{sl:.1f}",               C_RED),
                (f"{t1:.1f}",               C_GREEN),
                (f"{t2:.1f}",               C_GREEN),
                (f"{rr_ret}:1",             C_GREEN if rr_ret>=2 else C_DIM),
                (status,                    sc),
            ]):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setBackground(QBrush(rb))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.retest_tbl.setItem(r, col, item)

        self.retest_tbl.resizeColumnsToContents()

    # ── Scan trigger ──────────────────────────────────────────────

    def run_scan(self):
        if not self.symbols:
            self.status_lbl.setText("No symbols in DB"); return
        if self.worker and self.worker.isRunning(): return
        self.scan_btn.setEnabled(False); self.scan_btn.setText("Scanning…")
        self.status_lbl.setText(f"Scanning {len(self.symbols)} symbols…")
        self.worker = BosWorker(self.db, self.symbols)
        self.worker.done.connect(self._on_scan)
        self.worker.start()

    def _on_scan(self, bos_results, hourly_results):
        self.scan_btn.setEnabled(True); self.scan_btn.setText("⟳  Scan Now")
        self.lbl_ts.setText(f"Last scan: {datetime.now().strftime('%H:%M:%S')}")

        # Convert BOS results to flat dicts for table
        self.bos_events = []
        for item in bos_results:
            s = item["setup"]
            self.bos_events.append({
                "symbol":       s["symbol"],
                "bos_type":     s["bos_type"],
                "ltp":          s["ltp"],
                "broken":       s["broken_level"],
                "sl":           s["sl"],
                "t1":           s["t1"],
                "t2":           s["t2"],
                "rr_now":       s["rr_now"],
                "rr_retest":    s["rr_retest"],
                "strength":     s["strength"],
                "vol_ratio":    s["volume_ratio"],
                "ob_low":       s["ob_low"],
                "ob_high":      s["ob_high"],
                "next_liq":     s["next_liq"],
                "entry_retest": s["entry_retest"],
                "prev_trend":   s.get("prev_trend",""),
                "ema_ok":       s.get("ema_ok", True),
                "ema_note":     s.get("ema_note",""),
                "ema_filtered": s.get("ema_filtered", False),
                "alerted":      not s.get("already_alerted", False),
            })

        self.hourly_events = hourly_results
        self._stats()
        self._apply()


class AstroPanel(QWidget):
    def __init__(self):
        super().__init__(); self._build_ui(); self._refresh()

    def _build_ui(self):
        root=QVBoxLayout(self); root.setContentsMargins(16,14,16,12); root.setSpacing(12)
        hdr=QHBoxLayout()
        hdr.addWidget(section_title("Vedic / KP Astro Score")); hdr.addStretch()
        btn=QPushButton("⟳  Refresh"); btn.setFixedSize(100,30); btn.clicked.connect(self._refresh); hdr.addWidget(btn)
        root.addLayout(hdr)
        frame,lay=card_frame(QGridLayout); lay.setSpacing(10)
        self.lbl_signal=make_label("—",22,bold=True,color=C_YELLOW,align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_score=make_label("Score: —",16,bold=True,color=C_DIM,align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_nak=make_label("Nakshatra: —",12,color=C_DIM); self.lbl_sign=make_label("Moon Sign: —",12,color=C_DIM)
        self.lbl_lord=make_label("Nak Lord: —",12,color=C_DIM); self.lbl_sub=make_label("KP Sub-lord: —",12,color=C_DIM)
        self.lbl_reason=make_label("—",11,color=C_DIM); self.lbl_reason.setWordWrap(True)
        lay.addWidget(self.lbl_signal,0,0,1,2); lay.addWidget(self.lbl_score,1,0,1,2)
        lay.addWidget(self.lbl_nak,2,0); lay.addWidget(self.lbl_sign,2,1)
        lay.addWidget(self.lbl_lord,3,0); lay.addWidget(self.lbl_sub,3,1)
        lay.addWidget(self.lbl_reason,4,0,1,2); root.addWidget(frame)
        root.addWidget(make_label("📅  7-Day Forecast",12,bold=True,color=C_BLUE))
        cols=["Date","Nakshatra","Moon Sign","Score","Signal","Reason"]
        self.wt=QTableWidget(7,len(cols)); self.wt.setHorizontalHeaderLabels(cols)
        self.wt.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.wt.horizontalHeader().setStretchLastSection(True)
        self.wt.verticalHeader().setVisible(False); self.wt.setMaximumHeight(230); self.wt.setAlternatingRowColors(True)
        root.addWidget(self.wt); root.addStretch()

    def _refresh(self):
        if not ASTRO_OK: self.lbl_signal.setText("⚠️ astro_logic.py not found"); return
        try:
            r=get_astro_score(); score=r.get("score",0); signal=r.get("signal","—")
            sc=C_GREEN if score>0 else (C_RED if score<0 else C_YELLOW)
            sgc=C_GREEN if any(x in signal for x in ["UP","BUY","TREND"]) else (C_RED if any(x in signal for x in ["SELL","BEAR","NO"]) else C_YELLOW)
            self.lbl_signal.setText(signal); self.lbl_signal.setStyleSheet(f"color:{sgc};font-size:20px;font-weight:bold;")
            self.lbl_score.setText(f"Score: {score:+d}"); self.lbl_score.setStyleSheet(f"color:{sc};font-size:15px;font-weight:bold;")
            self.lbl_nak.setText(f"🌙 Nakshatra: {r.get('nakshatra','—')}"); self.lbl_sign.setText(f"♈ Moon Sign: {r.get('moon_sign','—')}")
            self.lbl_lord.setText(f"👁 Nak Lord: {r.get('nak_lord','—')}"); self.lbl_sub.setText(f"🔮 KP Sub: {r.get('sub_lord','—')}")
            self.lbl_reason.setText(f"📝 {r.get('reason','—')}")
            try:
                fc=get_week_forecast(); self.wt.setRowCount(len(fc))
                for ri,d in enumerate(fc):
                    sc2=d.get("score",0); sc_c=C_GREEN if sc2>0 else (C_RED if sc2<0 else C_YELLOW)
                    for col,(text,clr) in enumerate([(d.get("date",""),"#e6edf3"),(d.get("nakshatra",""),C_DIM),(d.get("sign",""),C_DIM),(f"{sc2:+d}",sc_c),(d.get("signal",""),sc_c),(d.get("reason",""),C_DIM)]):
                        it=QTableWidgetItem(text); it.setForeground(QColor(clr)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.wt.setItem(ri,col,it)
                self.wt.resizeColumnsToContents()
            except Exception: pass
        except Exception as e: self.lbl_signal.setText(f"Error: {e}")


class TimePanel(QWidget):
    def __init__(self):
        super().__init__(); self._build_ui(); self._tick()
        t=QTimer(self); t.timeout.connect(self._tick); t.start(5000)

    def _build_ui(self):
        root=QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(12)
        root.addWidget(make_label("⏰  Time-of-Day Trading Signal",14,bold=True,color=C_BLUE))
        f,l=card_frame(QVBoxLayout)
        self.lbl_time=make_label("—",32,bold=True,color=C_YELLOW,align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_zone=make_label("—",20,bold=True,align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_desc=make_label("—",13,color=C_DIM,align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_mins=make_label("—",11,color=C_DIM,align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_good=make_label("—",12,bold=True,align=Qt.AlignmentFlag.AlignCenter)
        for w in [self.lbl_time,self.lbl_zone,self.lbl_desc,self.lbl_mins,self.lbl_good]: l.addWidget(w)
        root.addWidget(f)
        root.addWidget(make_label("📋  Session Guide",12,bold=True,color=C_BLUE))
        sessions=[("09:08–09:25","🚧 Opening Risk","Avoid",C_YELLOW),("09:25–09:45","⚡ Opening Range","Observe only",C_YELLOW),
                  ("09:45–10:30","🔥 Strong Trend","Best trending hour",C_GREEN),("10:30–11:30","🟢 Momentum","Trail trades",C_GREEN),
                  ("11:30–12:00","🟡 Slow Zone","Reduce size",C_YELLOW),("12:00–13:00","🟡 Consolidation","Wait breakout",C_YELLOW),
                  ("13:00–13:45","🚀 Afternoon Push","Institutional accum.",C_BLUE),("13:45–14:30","🚀 Breakout Zone","F&O expiry",C_BLUE),
                  ("14:30–15:00","⚡ Closing Rush","Sharp moves",C_YELLOW),("15:00–15:30","⛔ Avoid Entry","No new positions",C_RED)]
        g=QTableWidget(len(sessions),3); g.setHorizontalHeaderLabels(["Time","Zone","Note"])
        g.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        g.horizontalHeader().setStretchLastSection(True)
        g.verticalHeader().setVisible(False); g.setMaximumHeight(280)
        for i,(t,z,n,c) in enumerate(sessions):
            for col,(text,clr) in enumerate([(t,C_DIM),(z,c),(n,C_DIM)]):
                it=QTableWidgetItem(text); it.setForeground(QColor(clr)); g.setItem(i,col,it)
        root.addWidget(g); root.addStretch()

    def _tick(self):
        if not TIME_OK: self.lbl_zone.setText("astro_time.py not found"); return
        try:
            d=get_time_signal_detail(); good=is_good_entry_time()
            zc=C_GREEN if good else (C_RED if any(x in d.get("signal","") for x in ["AVOID","CLOSED"]) else C_YELLOW)
            self.lbl_time.setText(d.get("time_str","—")); self.lbl_zone.setText(d.get("signal","—"))
            self.lbl_zone.setStyleSheet(f"color:{zc};font-size:20px;font-weight:bold;")
            self.lbl_desc.setText(d.get("description","—")); self.lbl_mins.setText(f"{d.get('mins_left',0)} min remaining")
            gt="✅ GOOD ENTRY TIME" if good else "⏸ WAIT / NO ENTRY"
            self.lbl_good.setText(gt); self.lbl_good.setStyleSheet(f"color:{C_GREEN if good else C_RED};font-size:13px;font-weight:bold;")
        except Exception as e: self.lbl_zone.setText(f"Error: {e}")


class DbPanel(QWidget):
    def __init__(self,db):
        super().__init__(); self.db=db; self._build_ui(); self._refresh()

    def _build_ui(self):
        root=QVBoxLayout(self); root.setContentsMargins(16,14,16,12); root.setSpacing(12)
        hdr=QHBoxLayout()
        hdr.addWidget(section_title("Local OHLC Database")); hdr.addStretch()
        btn=QPushButton("⟳  Refresh"); btn.setFixedSize(100,30); btn.clicked.connect(self._refresh); hdr.addWidget(btn)
        root.addLayout(hdr)
        pr=QHBoxLayout(); pr.setSpacing(10)
        self.p_syms=stat_pill("Symbols","—",C_CYAN,155); self.p_can=stat_pill("Candles","—",C_GREEN,155)
        self.p_today=stat_pill("Updated Today","—",C_YELLOW,155); self.p_sz=stat_pill("DB Size","—",C_PURPLE,155)
        for p in [self.p_syms,self.p_can,self.p_today,self.p_sz]: pr.addWidget(p)
        pr.addStretch(); root.addLayout(pr)
        root.addWidget(section_title("Per-Symbol Status"))
        cols=["Symbol","Last Updated","Last Candle","Candles"]
        self.sym_tbl=QTableWidget(0,len(cols)); self.sym_tbl.setHorizontalHeaderLabels(cols)
        self.sym_tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.sym_tbl.horizontalHeader().setStretchLastSection(True)
        self.sym_tbl.verticalHeader().setVisible(False)
        self.sym_tbl.setAlternatingRowColors(True); self.sym_tbl.setShowGrid(False)
        self.sym_tbl.setStyleSheet("QTableWidget{alternate-background-color:#F8F9FA;background:#FFFFFF;border:1px solid #DDE1E8;}")
        root.addWidget(self.sym_tbl)

    def _card(self,label,value,color):
        return stat_pill(label, value, color, 160)

    def _refresh(self):
        if not DB_OK or not self.db: return
        status=self.db.get_status(); size=self.db.get_db_size_mb(); today=date.today().isoformat()
        tc=sum(1 for s in status if (s["last_updated"] or "")[:10]==today)
        self.p_syms._v.setText(str(len(status))); self.p_can._v.setText(f"{sum(s['total_candles'] for s in status):,}")
        self.p_today._v.setText(str(tc)); self.p_sz._v.setText(f"{size} MB")
        self.sym_tbl.setRowCount(0)
        for row in status:
            r=self.sym_tbl.rowCount(); self.sym_tbl.insertRow(r)
            lu=row["last_updated"] or "—"; ok=lu[:10]==today
            for col,(text,color) in enumerate([(row["symbol"],C_GREEN if ok else C_YELLOW),(lu,C_DIM),(row["last_candle_dt"] or "—",C_DIM),(str(row["total_candles"]),"#e6edf3")]):
                it=QTableWidgetItem(text); it.setForeground(QColor(color)); it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.sym_tbl.setItem(r,col,it)
        self.sym_tbl.resizeColumnsToContents()


# ─────────────────────────────────────────────────────────────────────────────
# AUTO-REFRESH BAR
# ─────────────────────────────────────────────────────────────────────────────

class AutoRefreshBar(QWidget):
    """
    Auto-refresh bar — starts automatically once Kite connects.
    Ticks every second regardless of market hours (no gate).
    Shows live countdown. User can pause/resume.
    """
    trigger = pyqtSignal()

    def __init__(self, interval_secs=AUTO_REFRESH_SECS):
        super().__init__()
        self._interval  = interval_secs
        self._remaining = interval_secs
        self._running   = False
        self.setFixedHeight(34)
        self.setStyleSheet("background:#EEF1F6; border-top:1px solid #DDE1E8;")

        lay = QHBoxLayout(self)
        lay.setContentsMargins(12, 4, 12, 4)
        lay.setSpacing(10)

        self.dot = make_label("⏸", 12, color=C_DIM)
        self.lbl = make_label("Auto-refresh: paused", 10, color=C_DIM)
        self.bar = QProgressBar()
        self.bar.setRange(0, self._interval)
        self.bar.setValue(self._interval)
        self.bar.setFixedHeight(4)
        self.bar.setTextVisible(False)
        self.bar.setStyleSheet("""
            QProgressBar { background:#0a1525; border:none; border-radius:2px; }
            QProgressBar::chunk { background:qlineargradient(x1:0,y1:0,x2:1,y2:0,stop:0 #1565c0,stop:1 #4fc3f7); }
        """)
        self.btn = QPushButton("▶  Resume")
        self.btn.setFixedWidth(100)
        self.btn.setStyleSheet("""
            QPushButton { background:#0f1f35; color:#4fc3f7; border:1px solid #1e3550;
                          padding:3px 10px; border-radius:4px; font-size:10px; }
            QPushButton:hover { background:#1a4a72; color:#fff; }
        """)
        self.btn.clicked.connect(self._toggle)

        # "Last refreshed" time inside the bar itself
        self.lbl_last = make_label("—", 10, color=C_DIM)
        self.lbl_last.setStyleSheet("color:#2a5a8a; font-size:10px; min-width:110px;")

        lay.addWidget(self.dot)
        lay.addWidget(self.lbl)
        lay.addWidget(self.bar, stretch=1)
        lay.addWidget(make_label("Last:", 9, color=C_DIM))
        lay.addWidget(self.lbl_last)
        lay.addWidget(self.btn)

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self._timer.start(1000)

    def start_auto(self):
        """Called by MainWindow after Kite connects."""
        if not self._running:
            self._running   = True
            self._remaining = self._interval
            self.btn.setText("⏹  Pause")
            self.dot.setText("●")
            self.dot.setStyleSheet(f"color:{C_GREEN};")

    def mark_refreshed(self, ts: str = ""):
        """Call this after every successful data refresh to update the timestamp."""
        if not ts:
            ts = datetime.now().strftime("%H:%M:%S")
        self.lbl_last.setText(ts)
        self.lbl_last.setStyleSheet(f"color:{C_GREEN}; font-size:10px; font-weight:bold;")
        # Fade back to dim after 3 seconds
        QTimer.singleShot(3000, lambda: self.lbl_last.setStyleSheet(
            "color:#2a5a8a; font-size:10px;"))

    def _toggle(self):
        self._running = not self._running
        if self._running:
            self._remaining = self._interval
            self.btn.setText("⏹  Pause")
            self.dot.setText("●")
            self.dot.setStyleSheet(f"color:{C_GREEN};")
        else:
            self.btn.setText("▶  Resume")
            self.lbl.setText("Auto-refresh: paused")
            self.bar.setValue(self._interval)
            self.dot.setText("⏸")
            self.dot.setStyleSheet(f"color:{C_DIM};")

    def _tick(self):
        if not self._running:
            return
        self._remaining -= 1
        self.bar.setValue(self._remaining)
        # Color shifts red as countdown nears zero
        if self._remaining <= 10:
            self.lbl.setStyleSheet(f"color:{C_RED};")
            self.lbl.setText(f"⚡ Refreshing in {self._remaining}s…")
        else:
            self.lbl.setStyleSheet(f"color:{C_MUTED};")
            self.lbl.setText(f"Next refresh: {self._remaining}s")
        if self._remaining <= 0:
            self._remaining = self._interval
            self.bar.setValue(self._interval)
            self.trigger.emit()



# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — TOP MOVERS WORKER
# ─────────────────────────────────────────────────────────────────────────────

class MoversWorker(QThread):
    """Fetches top gainers, losers and volume surge in background."""
    done  = pyqtSignal(dict)
    error = pyqtSignal(str)

    def run(self):
        ks = KiteSession.get()
        if not ks.ok:
            self.error.emit("Kite not connected"); return
        try:
            kite   = ks.kite
            quotes = {}
            syms   = [f"NSE:{s}" for s in STOCKS]
            for i in range(0, len(syms), 400):
                try: quotes.update(kite.quote(syms[i:i+400]))
                except Exception: pass

            rows = []
            for sym in STOCKS:
                q = quotes.get(f"NSE:{sym}")
                if not q: continue
                ltp  = q.get("last_price", 0)
                pc   = q["ohlc"]["close"]
                chgp = round((ltp - pc) / pc * 100, 2) if pc else 0
                vol  = q.get("volume", 0)
                rows.append({
                    "symbol":     sym,
                    "ltp":        round(ltp, 2),
                    "chg_p":      chgp,
                    "volume":     vol,
                    "day_high":   round(q["ohlc"]["high"], 2),
                    "day_low":    round(q["ohlc"]["low"],  2),
                    "day_open":   round(q["ohlc"]["open"], 2),
                    "prev_close": round(pc, 2),
                })

            if not rows:
                self.error.emit("No quotes returned"); return

            rows.sort(key=lambda r: r["chg_p"], reverse=True)
            gainers   = [r for r in rows if r["chg_p"] >  2][:20]
            losers    = sorted([r for r in rows if r["chg_p"] < -2], key=lambda r: r["chg_p"])[:20]
            vol_surge = sorted(rows, key=lambda r: r["volume"], reverse=True)[:20]

            self.done.emit({
                "gainers":   gainers,
                "losers":    losers,
                "vol_surge": vol_surge,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })
        except Exception as e:
            self.error.emit(str(e))


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — TOP MOVERS PANEL
# ─────────────────────────────────────────────────────────────────────────────

class MoversPanel(QWidget):
    def __init__(self):
        super().__init__()
        self._worker = None
        self._build_ui()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12); root.setSpacing(12)

        # Header
        hdr = QHBoxLayout()
        hdr.addWidget(section_title("Top Movers — Live"))
        hdr.addStretch()
        self.btn = QPushButton("⟳  Fetch Movers")
        self.btn.setFixedSize(130, 30); self.btn.clicked.connect(self._fetch)
        hdr.addWidget(self.btn)
        root.addLayout(hdr)

        # Summary pills
        pills = QHBoxLayout(); pills.setSpacing(10)
        self.p_g = stat_pill("Gainers >2%",     "0", C_GREEN, 130)
        self.p_l = stat_pill("Losers <-2%",     "0", C_RED,   130)
        self.p_v = stat_pill("Vol Surge Top20", "0", C_CYAN,  150)
        self.p_t = stat_pill("Updated",         "—", C_DIM,   120)
        for p in [self.p_g, self.p_l, self.p_v, self.p_t]: pills.addWidget(p)
        pills.addStretch(); root.addLayout(pills)

        # Sub-tabs
        self.sub_tabs = QTabWidget()
        self.sub_tabs.setStyleSheet("QTabBar::tab { min-width:100px; padding:5px 10px; }")

        self.tbl_g = self._make_tbl(["#","Symbol","LTP","Chg%","Day High","Day Low","Open","Prev Close"])
        self.tbl_l = self._make_tbl(["#","Symbol","LTP","Chg%","Day High","Day Low","Open","Prev Close"])
        self.tbl_v = self._make_tbl(["#","Symbol","LTP","Chg%","Volume","Day High","Day Low"])

        self.sub_tabs.addTab(self.tbl_g, "🟢  Top Gainers")
        self.sub_tabs.addTab(self.tbl_l, "🔴  Top Losers")
        self.sub_tabs.addTab(self.tbl_v, "📊  Volume Surge")
        root.addWidget(self.sub_tabs)

        self.status_lbl = make_label("Connect Kite and click Fetch Movers.", 10, color=C_DIM)
        root.addWidget(self.status_lbl)

    def _pill(self, label, value, color, width=130):
        return stat_pill(label, value, color, width)

    def _make_tbl(self, cols):
        t = QTableWidget(0, len(cols))
        t.setHorizontalHeaderLabels(cols)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        t.horizontalHeader().setStretchLastSection(True)
        t.verticalHeader().setVisible(False)
        t.setAlternatingRowColors(True)
        t.setShowGrid(False)
        t.setStyleSheet("QTableWidget{alternate-background-color:#FAFAFA;background:#0b0f1a;} QTableWidget::item{padding:5px 10px;border-bottom:1px solid #0f1828;}")
        return t

    def _fetch(self):
        ks = KiteSession.get()
        if not ks.ok:
            self.status_lbl.setText("❌ Kite not connected — use Session tab"); return
        if self._worker and self._worker.isRunning(): return
        self.btn.setEnabled(False); self.btn.setText("Fetching…")
        self.status_lbl.setText(f"Fetching quotes for {len(STOCKS)} symbols…")
        self._worker = MoversWorker()
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_error(self, err):
        self.btn.setEnabled(True); self.btn.setText("⟳  Fetch Movers")
        self.status_lbl.setText(f"❌ {err}")

    def _on_done(self, data):
        self.btn.setEnabled(True); self.btn.setText("⟳  Fetch Movers")
        g = data.get("gainers", []); l = data.get("losers", [])
        v = data.get("vol_surge", []); ts = data.get("timestamp", "")
        self.p_g._v.setText(str(len(g)))
        self.p_l._v.setText(str(len(l)))
        self.p_v._v.setText(str(len(v)))
        self.p_t._v.setText(ts)
        self._fill_gl(self.tbl_g, g, is_vol=False)
        self._fill_gl(self.tbl_l, l, is_vol=False)
        self._fill_gl(self.tbl_v, v, is_vol=True)
        self.status_lbl.setText(
            f"Gainers: {len(g)} | Losers: {len(l)} | Updated: {ts}")

    def _fill_gl(self, tbl, rows, is_vol=False):
        tbl.setRowCount(0)
        for i, row in enumerate(rows):
            chg  = row.get("chg_p", 0)
            bull = chg >= 0
            bg   = QColor(C_BG_BULL if bull else C_BG_BEAR)
            cc   = C_GREEN if bull else C_RED
            r    = tbl.rowCount(); tbl.insertRow(r); tbl.setRowHeight(r, 22)

            def it(text, color="#e6edf3"):
                item = QTableWidgetItem(str(text))
                item.setForeground(QColor(color))
                item.setBackground(QBrush(bg))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                return item

            chg_txt = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"

            if is_vol:
                vol = row.get("volume", 0)
                vol_txt = f"{vol/100000:.1f}L" if vol >= 100000 else str(vol)
                tbl.setItem(r, 0, it(i+1, C_DIM))
                tbl.setItem(r, 1, it(row["symbol"], C_BLUE))
                tbl.setItem(r, 2, it(row["ltp"]))
                tbl.setItem(r, 3, it(chg_txt, cc))
                tbl.setItem(r, 4, it(vol_txt, C_YELLOW))
                tbl.setItem(r, 5, it(row.get("day_high",""), C_DIM))
                tbl.setItem(r, 6, it(row.get("day_low",""),  C_DIM))
            else:
                tbl.setItem(r, 0, it(i+1, C_DIM))
                tbl.setItem(r, 1, it(row["symbol"], C_BLUE))
                tbl.setItem(r, 2, it(row["ltp"]))
                tbl.setItem(r, 3, it(chg_txt, cc))
                tbl.setItem(r, 4, it(row.get("day_high",""),   C_DIM))
                tbl.setItem(r, 5, it(row.get("day_low",""),    C_DIM))
                tbl.setItem(r, 6, it(row.get("day_open",""),   C_DIM))
                tbl.setItem(r, 7, it(row.get("prev_close",""), C_DIM))

        tbl.resizeColumnsToContents()

    def trigger_fetch(self):
        self._fetch()


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — KP PANCHANG WORKER
# ─────────────────────────────────────────────────────────────────────────────

class KPWorker(QThread):
    done  = pyqtSignal(dict)
    error = pyqtSignal(str)

    def run(self):
        try:
            import pytz
            from astro_engine import get_all_planets, get_moon_data, get_dignities, get_angular_distance
            IST = pytz.timezone("Asia/Kolkata")
            now = datetime.now(IST)

            planets = get_all_planets(now)
            moon    = get_moon_data(now)
            digs    = get_dignities(now)

            # KP Sub-lord
            nak_span   = 360 / 27
            SUB_SEQ    = ["Ke","Ve","Su","Mo","Ma","Ra","Ju","Sa","Me"]
            pos_in_nak = moon["degree"] % nak_span
            kp_sub     = SUB_SEQ[min(int(pos_in_nak / nak_span * 9), 8)]

            KP_BULL = {"Ju","Ve","Mo","Su"}
            KP_BEAR = {"Sa","Ra","Ke","Ma"}

            # Hora lord at 9:15 IST
            DAY_LORDS  = ["Su","Mo","Ma","Me","Ju","Ve","Sa"]
            HORA_SEQ   = ["Su","Ve","Me","Mo","Sa","Ju","Ma"]
            day_lord   = DAY_LORDS[now.weekday()]
            HORA_START = {"Su":0,"Ve":1,"Me":2,"Mo":3,"Sa":4,"Ju":5,"Ma":6}
            hora_idx   = (HORA_START.get(day_lord, 0) + 2) % 7
            hora_open  = HORA_SEQ[hora_idx]

            # Next 6 hora windows (9:15–15:15)
            horas = []
            for i in range(6):
                idx    = (hora_idx + i) % 7
                planet = HORA_SEQ[idx]
                sh     = 9 + i
                bias   = "🟢 Bull" if planet in {"Su","Ju","Ve","Mo"} else "🔴 Bear"
                horas.append({"time": f"{sh:02d}:15–{sh+1:02d}:15", "planet": planet, "bias": bias})

            # Key angular distances
            def ang(pa, pb):
                try: return round(get_angular_distance(pa, pb, now), 1)
                except: return 0.0

            mj = ang("Moon","Jupiter")
            mk = ang("Moon","Ketu")
            ms = ang("Moon","Saturn")

            bull_sigs = []
            bear_sigs = []
            if kp_sub in KP_BULL:                 bull_sigs.append(f"KP Sub-lord {kp_sub} → BULLISH")
            if kp_sub in KP_BEAR:                 bear_sigs.append(f"KP Sub-lord {kp_sub} → BEARISH")
            if digs.get("Jupiter") == "EXALTED":  bull_sigs.append("Jupiter EXALTED — market floor")
            if digs.get("Moon")    == "EXALTED":  bull_sigs.append("Moon EXALTED in Taurus — recovery")
            if mj < 10:  bull_sigs.append(f"Moon–Jupiter {mj}° conjunct (Gajakesari Yoga)")
            if mk < 12:  bear_sigs.append(f"Moon–Ketu {mk}° — BankNifty weakness")
            if ms < 10:  bear_sigs.append(f"Moon–Saturn {ms}° (Vish Yoga) — fall risk")
            if digs.get("Venus")   == "EXALTED":  bull_sigs.append("Venus EXALTED — FMCG/Banks strong")
            if digs.get("Saturn")  == "DEBILITATED": bear_sigs.append("Saturn debilitated — selling pressure")

            if   len(bull_sigs) > len(bear_sigs): kp_signal = "🟢 KP BULLISH"
            elif len(bear_sigs) > len(bull_sigs): kp_signal = "🔴 KP BEARISH"
            else:                                  kp_signal = "⚠️ KP MIXED"

            planet_rows = []
            DIG_ORDER = ["Sun","Moon","Mars","Mercury","Jupiter","Venus","Saturn","Rahu","Ketu"]
            for p in DIG_ORDER:
                pdata = planets.get(p, {})
                if not pdata: continue
                planet_rows.append({
                    "planet":    p,
                    "sign":      pdata.get("sign","—"),
                    "nakshatra": pdata.get("nakshatra","—"),
                    "degree":    round(pdata.get("degree",0), 2),
                    "dignity":   digs.get(p, "NEUTRAL"),
                })

            self.done.emit({
                "moon_nak":     moon.get("nakshatra","—"),
                "moon_sign":    moon.get("sign","—"),
                "moon_lord":    moon.get("nak_lord","—"),
                "kp_sub":       kp_sub,
                "hora_open":    hora_open,
                "hora_bias":    "🟢 Bull" if hora_open in {"Su","Ju","Ve","Mo"} else "🔴 Bear",
                "kp_signal":    kp_signal,
                "bull_signals": bull_sigs,
                "bear_signals": bear_sigs,
                "horas":        horas,
                "planets":      planet_rows,
                "moon_jupiter": mj,
                "moon_ketu":    mk,
                "moon_saturn":  ms,
                "timestamp":    now.strftime("%H:%M:%S IST"),
            })
        except Exception as e:
            self.error.emit(str(e))


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — KP PANCHANG PANEL
# ─────────────────────────────────────────────────────────────────────────────

class KPPanel(QWidget):
    def __init__(self):
        super().__init__()
        self._worker = None
        self._build_ui()
        self._fetch()   # load immediately on startup (no Kite needed)

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 12); root.setSpacing(12)

        # Header
        hdr = QHBoxLayout()
        hdr.addWidget(section_title("KP Panchang — Trading Windows"))
        hdr.addStretch()
        self.btn = QPushButton("⟳  Refresh")
        self.btn.setFixedSize(100, 30); self.btn.clicked.connect(self._fetch)
        hdr.addWidget(self.btn)
        root.addLayout(hdr)

        # Top signal banner
        banner_frame, banner_lay = _card_frame(QHBoxLayout, (12, 8, 12, 8))
        left = QVBoxLayout(); left.setSpacing(4)
        self.lbl_kp_signal = make_label("—", 20, bold=True, color=C_YELLOW,
                                         align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_hora      = make_label("Opening Hora: —", 13, color=C_DIM,
                                         align=Qt.AlignmentFlag.AlignCenter)
        left.addWidget(self.lbl_kp_signal)
        left.addWidget(self.lbl_hora)
        banner_lay.addLayout(left, stretch=2)

        # Pills
        right = QVBoxLayout(); right.setSpacing(4)
        pills1 = QHBoxLayout()
        self.p_nak  = self._pill("Moon Nakshatra", "—", C_BLUE)
        self.p_sign = self._pill("Moon Sign",      "—", C_YELLOW)
        pills1.addWidget(self.p_nak); pills1.addWidget(self.p_sign)
        pills2 = QHBoxLayout()
        self.p_sub  = self._pill("KP Sub-lord",    "—", C_PURPLE)
        self.p_ts   = self._pill("Updated",        "—", C_DIM)
        pills2.addWidget(self.p_sub); pills2.addWidget(self.p_ts)
        right.addLayout(pills1); right.addLayout(pills2)
        banner_lay.addLayout(right, stretch=3)
        root.addWidget(banner_frame)

        # Signals + Angles row
        signals_row = QHBoxLayout(); signals_row.setSpacing(8)

        # Bull signals
        bf, bl = _card_frame(QVBoxLayout, (10, 8, 10, 8))
        bl.addWidget(make_label("🟢  Bullish Signals", 11, bold=True, color=C_GREEN))
        self.lbl_bull = make_label("—", 11, color=C_GREEN)
        self.lbl_bull.setWordWrap(True)
        bl.addWidget(self.lbl_bull)
        signals_row.addWidget(bf)

        # Bear signals
        rf, rl = _card_frame(QVBoxLayout, (10, 8, 10, 8))
        rl.addWidget(make_label("🔴  Bearish Signals", 11, bold=True, color=C_RED))
        self.lbl_bear = make_label("—", 11, color=C_RED)
        self.lbl_bear.setWordWrap(True)
        rl.addWidget(self.lbl_bear)
        signals_row.addWidget(rf)

        # Key angles
        af, al = _card_frame(QVBoxLayout, (10, 8, 10, 8))
        al.addWidget(make_label("🔑  Key Aspects", 11, bold=True, color=C_BLUE))
        self.lbl_angles = make_label("—", 11, color=C_DIM)
        al.addWidget(self.lbl_angles)
        signals_row.addWidget(af)
        root.addLayout(signals_row)

        # Hora windows table
        root.addWidget(make_label("⏰  Intraday Hora Windows", 12, bold=True, color=C_BLUE))
        self.hora_tbl = self._make_tbl(["Time", "Hora Planet", "Bias"])
        self.hora_tbl.setMaximumHeight(180)
        root.addWidget(self.hora_tbl)

        # Planet positions table
        root.addWidget(make_label("🌍  Planet Positions (Sidereal / Lahiri)", 12, bold=True, color=C_BLUE))
        self.planet_tbl = self._make_tbl(["Planet", "Sign", "Nakshatra", "Degree", "Dignity"])
        root.addWidget(self.planet_tbl)

    def _pill(self, label, value, color, width=130):
        return stat_pill(label, value, color, width)

    def _make_tbl(self, cols):
        t = QTableWidget(0, len(cols))
        t.setHorizontalHeaderLabels(cols)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        t.horizontalHeader().setStretchLastSection(True)
        t.verticalHeader().setVisible(False)
        t.setAlternatingRowColors(True)
        t.setShowGrid(False)
        t.setStyleSheet("QTableWidget{alternate-background-color:#FAFAFA;background:#0b0f1a;} QTableWidget::item{padding:5px 10px;border-bottom:1px solid #0f1828;}")
        return t

    def _fetch(self):
        if self._worker and self._worker.isRunning(): return
        self.btn.setEnabled(False); self.btn.setText("Loading…")
        self._worker = KPWorker()
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_error(self, err):
        self.btn.setEnabled(True); self.btn.setText("⟳  Refresh")
        self.lbl_kp_signal.setText(f"⚠️ {err[:60]}")
        self.lbl_kp_signal.setStyleSheet(f"color:{C_RED};")

    def _on_done(self, d):
        self.btn.setEnabled(True); self.btn.setText("⟳  Refresh")

        sig = d.get("kp_signal","—")
        sc  = C_GREEN if "BULL" in sig else (C_RED if "BEAR" in sig else C_YELLOW)
        self.lbl_kp_signal.setText(sig)
        self.lbl_kp_signal.setStyleSheet(f"color:{sc};font-size:19px;font-weight:bold;")

        hora      = d.get("hora_open","—")
        hora_bias = d.get("hora_bias","—")
        hora_c    = C_GREEN if "Bull" in hora_bias else C_RED
        self.lbl_hora.setText(f"Opening Hora: {hora}  |  {hora_bias}")
        self.lbl_hora.setStyleSheet(f"color:{hora_c};font-size:12px;")

        self.p_nak._v.setText(d.get("moon_nak","—"))
        self.p_sign._v.setText(d.get("moon_sign","—"))
        self.p_sub._v.setText(d.get("kp_sub","—"))
        self.p_ts._v.setText(d.get("timestamp","—"))

        bull_text = "\n".join(f"✅ {s}" for s in d.get("bull_signals",[])) or "None"
        bear_text = "\n".join(f"⚠️ {s}" for s in d.get("bear_signals",[])) or "None"
        mj = d.get("moon_jupiter",0); mk = d.get("moon_ketu",0); ms = d.get("moon_saturn",0)
        self.lbl_bull.setText(bull_text)
        self.lbl_bear.setText(bear_text)
        self.lbl_angles.setText(
            f"Moon–Jupiter: {mj}°  {'🟢' if mj<10 else '⚪'}\n"
            f"Moon–Ketu:    {mk}°  {'🔴' if mk<12 else '⚪'}\n"
            f"Moon–Saturn:  {ms}°  {'🔴' if ms<10 else '⚪'}"
        )

        # Hora table
        self.hora_tbl.setRowCount(0)
        HORA_BULL = {"Su","Ju","Ve","Mo"}
        for h in d.get("horas",[]):
            planet = h["planet"]
            is_bull= planet in HORA_BULL
            r = self.hora_tbl.rowCount(); self.hora_tbl.insertRow(r)
            bg = QColor(C_BG_BULL if is_bull else C_BG_BEAR)
            for col,(text,color) in enumerate([
                (h["time"],   C_DIM),
                (planet,      C_GREEN if is_bull else C_RED),
                (h["bias"],   C_GREEN if is_bull else C_RED),
            ]):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setBackground(QBrush(bg))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.hora_tbl.setItem(r, col, item)

        # Planet table
        self.planet_tbl.setRowCount(0)
        DIG_COLORS = {"EXALTED": C_GREEN, "OWN": C_BLUE, "DEBILITATED": C_RED, "NEUTRAL": C_DIM}
        for p in d.get("planets",[]):
            dig_c = DIG_COLORS.get(p.get("dignity","NEUTRAL"), C_DIM)
            r = self.planet_tbl.rowCount(); self.planet_tbl.insertRow(r)
            for col,(text,color) in enumerate([
                (p.get("planet",""),    "#e6edf3"),
                (p.get("sign",""),      C_YELLOW),
                (p.get("nakshatra",""), C_DIM),
                (f"{p.get('degree',0):.2f}°", C_DIM),
                (p.get("dignity",""),   dig_c),
            ]):
                item = QTableWidgetItem(text)
                item.setForeground(QColor(color))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.planet_tbl.setItem(r, col, item)

        self.hora_tbl.resizeColumnsToContents()
        self.planet_tbl.resizeColumnsToContents()

    def trigger_refresh(self):
        self._fetch()


# helper _card_frame used by KPPanel — duplicate from shared scope, defined locally
def _card_frame(layout_cls=QVBoxLayout, pad=(12,10,12,10)):
    """Local alias so KPPanel can call it without shadowing the module-level one."""
    return card_frame(layout_cls, pad)

# ─────────────────────────────────────────────────────────────────────────────
# SYMBOL LIST  (mirrors STOCKS in panchak_kite_dashboard_v2.py)
# ─────────────────────────────────────────────────────────────────────────────

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS",
    "ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV",
    "BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
    "BHEL","BHARATFORG","BHARTIARTL","BIOCON","BLUESTARCO","BOSCHLTD","BPCL",
    "BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA",
    "COALINDIA","COFORGE","COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR",
    "DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK",
    "GODREJCP","GODREJPROP","GRASIM","HAL","HAVELLS","HCLTECH","HDFCAMC",
    "HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDCOPPER","HINDPETRO",
    "HINDUNILVR","HINDZINC","HUDCO","ICICIBANK","ICICIGI","ICICIPRULI","IEX",
    "INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND",
    "IRCTC","IRFC","IREDA","ITC","JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL",
    "JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M",
    "MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL","MPHASIS",
    "MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NTPC","NUVAMA","NYKAA",
    "NATIONALUM","OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM",
    "PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD","PIDILITIND","PIIND","PNB",
    "PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PREMIERENE",
    "PRESTIGE","PPLPHARMA","RBLBANK","RECLTD","RELIANCE","RVNL","SAIL",
    "SAMMAANCAP","SBICARD","SBILIFE","SBIN","SHREECEM","SHRIRAMFIN","SIEMENS",
    "SOLARINDS","SRF","SUNPHARMA","SUPREMEIND","SWIGGY","SYNGENE","TATACONSUM",
    "TATAELXSI","TATAPOWER","TATATECH","TATASTEEL","TCS","TECHM","TIINDIA",
    "TITAN","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK",
    "UNITDSPR","UPL","VBL","VEDL","VOLTAS","WAAREEENER","WIPRO","ZYDUSLIFE",
]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN WINDOW
# ─────────────────────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("📈  Panchak Dashboard  v4.0  |  OI · SMC · BOS · KP")
        self.setMinimumSize(QSize(1280, 800))
        self.resize(1500, 900)
        self.setStyleSheet(DARK_QSS)
        self.db = OHLCStore() if DB_OK else None

        # ══════════════════════════════════════════════════════
        # PREMIUM HEADER — Dark professional trading terminal
        # ══════════════════════════════════════════════════════
        header = QFrame()
        header.setFixedHeight(56)
        header.setStyleSheet("""
            QFrame {
                background: #2C5282;
            }
            QLabel { background: transparent; }
        """)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(20, 0, 20, 0)
        hl.setSpacing(0)

        # ── Brand ──────────────────────────────────────────────
        brand = QLabel("PANCHAK")
        brand.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        brand.setStyleSheet("color:#FFFFFF; letter-spacing:3px; background:transparent;")
        hl.addWidget(brand)

        dot = QLabel("  ●")
        dot.setFont(QFont("Segoe UI", 8))
        dot.setStyleSheet("color:#90CDF4; background:transparent;")
        hl.addWidget(dot)

        sub = QLabel("  TRADING DASHBOARD")
        sub.setFont(QFont("Segoe UI", 9))
        sub.setStyleSheet("color:#BEE3F8; letter-spacing:2px; background:transparent;")
        hl.addWidget(sub)

        hl.addSpacing(28)
        sep_v1 = QFrame(); sep_v1.setFrameShape(QFrame.Shape.VLine)
        sep_v1.setFixedWidth(1); sep_v1.setStyleSheet("background:#FFFFFF40; color:#FFFFFF40;")
        hl.addWidget(sep_v1)
        hl.addSpacing(20)

        # ── NIFTY spot ─────────────────────────────────────────
        self.lbl_spot = QLabel("NIFTY  —")
        self.lbl_spot.setFont(QFont("Segoe UI", 17, QFont.Weight.Bold))
        self.lbl_spot.setStyleSheet("color:#FFFFFF; background:transparent;")
        hl.addWidget(self.lbl_spot)

        hl.addSpacing(12)

        self.lbl_chg = QLabel("")
        self.lbl_chg.setFont(QFont("Segoe UI", 11))
        self.lbl_chg.setStyleSheet("color:#90CDF4; background:transparent;")
        hl.addWidget(self.lbl_chg)

        hl.addStretch()

        # ── Kite status badge ──────────────────────────────────
        self.lbl_kite = QLabel("  ●  Kite: disconnected  ")
        self.lbl_kite.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.lbl_kite.setStyleSheet("""
            color: #FC8181;
            background: #FFFFFF20;
            border: 1px solid #FC818160;
            border-radius: 12px;
            padding: 3px 14px;
        """)
        hl.addWidget(self.lbl_kite)
        hl.addSpacing(10)

        # ── Market status badge ────────────────────────────────
        self.lbl_market = QLabel("  ●  MARKET CLOSED  ")
        self.lbl_market.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.lbl_market.setStyleSheet(f"""
            color: {C_DIM};
            background: {C_DIM}14;
            border: 1px solid {C_DIM}30;
            border-radius: 12px;
            padding: 3px 14px;
        """)
        hl.addWidget(self.lbl_market)
        hl.addSpacing(20)

        hl.addWidget(separator_line(vertical=True))
        hl.addSpacing(20)

        # ── Clock ──────────────────────────────────────────────
        self.lbl_clock = QLabel("—")
        self.lbl_clock.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self.lbl_clock.setStyleSheet("color:#E2E8F0; background:transparent; letter-spacing:0.5px;")
        hl.addWidget(self.lbl_clock)

        hl.addSpacing(20)
        hl.addWidget(separator_line(vertical=True))
        hl.addSpacing(14)

        # ── Last refresh ───────────────────────────────────────
        self.lbl_last_refresh = QLabel("Last refresh: —")
        self.lbl_last_refresh.setFont(QFont("Segoe UI", 10))
        self.lbl_last_refresh.setStyleSheet("""
            color: #BEE3F8;
            background: transparent;
            padding: 2px 4px;
        """)
        hl.addWidget(self.lbl_last_refresh)

        # Core panels (Phase 1 + 2)
        self.oi_panel    = OIPanel()
        self.smc_panel   = SMCPanel(self.oi_panel)
        self.bos_panel   = BosPanel(self.db)
        self.astro_panel = AstroPanel()
        self.time_panel  = TimePanel()
        self.db_panel    = DbPanel(self.db)
        self.sess_panel  = SessionPanel()

        # Phase 3 panels
        _kite_lam = lambda: (
            KiteSession.get().kite
            if (KiteSession.get().ok and KiteSession.get().kite is not None)
            else None
        )
        if SCREENER_OK:
            self.screener_panel = ScreenerPanel(STOCKS, kite_getter=_kite_lam)
            self.alerts_panel   = AlertsPanel()
            self.alerts_panel.load_settings()
        else:
            _ph = QWidget()
            _lbl = make_label("qt_screener.py not found",
                               13, color=C_RED, align=Qt.AlignmentFlag.AlignCenter)
            _lay = QVBoxLayout(_ph); _lay.addWidget(_lbl)
            self.screener_panel = _ph
            self.alerts_panel   = QWidget()

        # Advanced screeners panel (all missing scanners)
        if ADV_SCREENER_OK:
            self.adv_panel = AdvancedScreenerPanel(STOCKS, kite_getter=_kite_lam)
        else:
            _adv = QWidget()
            _al  = make_label("advanced_screener.py not found",
                               13, color=C_RED, align=Qt.AlignmentFlag.AlignCenter)
            _all = QVBoxLayout(_adv); _all.addWidget(_al)
            self.adv_panel = _adv

        # Phase 4 panels
        self.movers_panel = MoversPanel()
        self.kp_panel     = KPPanel()

        # Cross-panel wiring
        self.oi_panel.oi_updated.connect(self.smc_panel.on_oi_updated)
        self.oi_panel.oi_updated.connect(self._on_spot)
        self.sess_panel.connected.connect(self._on_kite_connected)

        # Tabs — full suite
        self.tabs = QTabWidget()
        self.tabs.addTab(self.oi_panel,       "📊  OI Intel")
        self.tabs.addTab(self.smc_panel,      "🧠  SMC + OI")
        self.tabs.addTab(self.bos_panel,      "📐  BOS")
        self.tabs.addTab(self.screener_panel, "📈  Screeners")
        self.tabs.addTab(self.adv_panel,      "🔬  Advanced")
        self.tabs.addTab(self.movers_panel,   "🔥  Movers")
        self.tabs.addTab(self.kp_panel,       "🪐  KP Panchang")
        self.tabs.addTab(self.alerts_panel,   "🔔  Alerts")
        self.tabs.addTab(self.astro_panel,    "🌙  Astro")
        self.tabs.addTab(self.time_panel,     "⏰  Time")
        self.tabs.addTab(self.db_panel,       "📦  DB")
        self.tabs.addTab(self.sess_panel,     "🔌  Session")

        # Auto-refresh bar
        self.auto_bar = AutoRefreshBar()
        self.auto_bar.trigger.connect(self._auto_refresh)

        # Root layout
        central = QWidget(); root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0); root.setSpacing(0)
        root.addWidget(header); root.addWidget(self.tabs); root.addWidget(self.auto_bar)
        self.setCentralWidget(central)

        self.setStatusBar(QStatusBar())
        self.statusBar().showMessage(
            "OI | SMC | BOS | Screeners | Advanced | Movers | KP | Alerts | Astro | Time | DB | Session")

        t = QTimer(self); t.timeout.connect(self._tick_clock); t.start(1000); self._tick_clock()
        self._try_auto_connect()

    def _try_auto_connect(self):
        if os.path.exists(ACCESS_TOKEN_FILE):
            ks = KiteSession.get()
            if ks.connect():
                self._on_kite_connected()
            else:
                self.statusBar().showMessage(f"Auto-connect failed: {ks.error} — use Session tab")

    def _on_kite_connected(self):
        self.lbl_kite.setText("  ●  Kite: connected  ")
        self.lbl_kite.setStyleSheet("""
            color:#9AE6B4;
            background:#FFFFFF22;
            border:1px solid #9AE6B460;
            border-radius:12px; padding:3px 14px;
            font-size:10px; font-weight:700;
        """)
        self.statusBar().showMessage("Kite connected — starting auto-refresh…")
        self.tabs.setCurrentIndex(0)

        # Ensure screener has fresh kite reference
        _kl = lambda: (KiteSession.get().kite
                       if (KiteSession.get().ok and KiteSession.get().kite is not None)
                       else None)
        if SCREENER_OK and hasattr(self.screener_panel, 'set_kite_getter'):
            self.screener_panel.set_kite_getter(_kl)
        if ADV_SCREENER_OK and hasattr(self.adv_panel, 'set_kite_getter'):
            self.adv_panel.set_kite_getter(_kl)

        # Start auto-refresh automatically — no manual button click needed
        self.auto_bar.start_auto()
        # Kick off immediate fetch of all live panels
        self.oi_panel.trigger_refresh()
        self.movers_panel.trigger_fetch()
        self.kp_panel.trigger_refresh()
        # Delay scans slightly so Kite session is fully ready
        if SCREENER_OK and hasattr(self.screener_panel, 'trigger_scan'):
            QTimer.singleShot(1500, self.screener_panel.trigger_scan)
        if ADV_SCREENER_OK and hasattr(self.adv_panel, 'trigger_scan'):
            QTimer.singleShot(3000, self.adv_panel.trigger_scan)
        # Mark initial connection time as first refresh
        self._mark_refreshed()

    def _on_spot(self, data):
        spot = data.get("spot", 0)
        if not spot: return
        self.lbl_spot.setText(f"NIFTY: {spot:,.1f}")
        # Show PCR as quick context in header
        pcr = data.get("pcr", 0)
        dirc= data.get("direction","")
        if "BULL" in dirc:
            chg_color = C_GREEN; chg_icon = "▲"
        elif "BEAR" in dirc:
            chg_color = C_RED;   chg_icon = "▼"
        else:
            chg_color = C_DIM;   chg_icon = "◆"
        self.lbl_chg.setText(f"{chg_icon}  PCR {pcr}")
        self.lbl_chg.setStyleSheet(f"color:{chg_color}; font-size:11px; background:transparent;")

    def _auto_refresh(self):
        """Fires every AUTO_REFRESH_SECS. Refreshes all live panels."""
        ts = datetime.now().strftime("%H:%M:%S")
        self.oi_panel.trigger_refresh()
        if SCREENER_OK:
            self.screener_panel.trigger_scan()
        if ADV_SCREENER_OK and hasattr(self.adv_panel, 'trigger_scan'):
            self.adv_panel.trigger_scan()
        self.movers_panel.trigger_fetch()
        self.kp_panel.trigger_refresh()
        self._mark_refreshed(ts)
        self.statusBar().showMessage(f"Auto-refreshed at {ts}")

    def _mark_refreshed(self, ts: str = ""):
        """Update the last-refresh label in header + AutoRefreshBar."""
        if not ts:
            ts = datetime.now().strftime("%H:%M:%S")
        # Header label — bright flash
        self.lbl_last_refresh.setText(f"Last refresh:  {ts}")
        self.lbl_last_refresh.setStyleSheet("color:#9AE6B4;background:transparent;font-size:10px;font-weight:700;")
        QTimer.singleShot(4000, lambda: self.lbl_last_refresh.setStyleSheet(
            "color:#BEE3F8;background:transparent;font-size:10px;"))
        # AutoRefreshBar
        self.auto_bar.mark_refreshed(ts)

    def _tick_clock(self):
        try:
            import pytz; IST = pytz.timezone("Asia/Kolkata"); now = datetime.now(IST)
        except Exception:
            now = datetime.now()
        self.lbl_clock.setText(now.strftime("%H:%M:%S  |  %a %d %b %Y"))
        mo = is_market_hours() if DB_OK else False
        if mo:
            self.lbl_market.setText("  ●  MARKET OPEN  ")
            self.lbl_market.setStyleSheet("""
                color:#9AE6B4;
                background:#FFFFFF22;
                border:1px solid #9AE6B460;
                border-radius:12px; padding:3px 14px;
                font-size:10px; font-weight:700;
            """)
        else:
            self.lbl_market.setText("  ●  MARKET CLOSED  ")
            self.lbl_market.setStyleSheet("""
                color:#CBD5E0;
                background:#FFFFFF18;
                border:1px solid #FFFFFF30;
                border-radius:12px; padding:3px 14px;
                font-size:10px; font-weight:700;
            """)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Panchak Trading Dashboard")
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
