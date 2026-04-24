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
    from bos_scanner import detect_bos, build_bos_setup, load_scan_cache, BOS_LOOKBACK
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
    SCREENER_OK = True
except ImportError:
    SCREENER_OK = False
    print("WARNING: qt_screener.py not found — Screener + Alerts tabs disabled")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY           = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "access_token.txt")
OI_CACHE_FILE     = "oi_intel_cache.json"
AUTO_REFRESH_SECS = 60

# ─────────────────────────────────────────────────────────────────────────────
# DARK THEME
# ─────────────────────────────────────────────────────────────────────────────

DARK_QSS = """
QMainWindow, QWidget {
    background-color: #0d1117;
    color: #e6edf3;
    font-family: "Segoe UI", "Consolas", monospace;
    font-size: 12px;
}
QTabWidget::pane { border: 1px solid #30363d; background: #0d1117; }
QTabBar::tab {
    background: #161b22; color: #8b949e;
    padding: 8px 14px; border: 1px solid #30363d;
    border-bottom: none; min-width: 80px;
}
QTabBar::tab:selected { background: #0d1117; color: #58a6ff; border-top: 2px solid #58a6ff; }
QTabBar::tab:hover    { color: #e6edf3; background: #21262d; }
QTableWidget {
    background-color: #0d1117; gridline-color: #21262d;
    color: #e6edf3; border: none; font-size: 12px;
    selection-background-color: #1f6feb33;
}
QTableWidget::item { padding: 3px 6px; border-bottom: 1px solid #21262d; }
QHeaderView::section {
    background-color: #161b22; color: #8b949e;
    padding: 5px 6px; border: none;
    border-bottom: 2px solid #30363d; font-size: 11px; font-weight: bold;
}
QComboBox {
    background: #21262d; color: #e6edf3;
    border: 1px solid #30363d; padding: 4px 8px; border-radius: 4px; min-width: 110px;
}
QComboBox QAbstractItemView { background: #21262d; color: #e6edf3; selection-background-color: #1f6feb; }
QDoubleSpinBox, QLineEdit {
    background: #21262d; color: #e6edf3;
    border: 1px solid #30363d; padding: 4px 8px; border-radius: 4px;
}
QPushButton {
    background: #21262d; color: #58a6ff;
    border: 1px solid #30363d; padding: 6px 14px; border-radius: 5px;
}
QPushButton:hover   { background: #1f6feb; color: #ffffff; border-color: #1f6feb; }
QPushButton:pressed { background: #1158c7; }
QPushButton:disabled { color: #555; border-color: #333; }
QScrollArea { border: none; }
QLabel { color: #e6edf3; }
QStatusBar { background: #161b22; color: #8b949e; border-top: 1px solid #30363d; font-size: 11px; }
QProgressBar {
    background: #21262d; border: 1px solid #30363d; border-radius: 3px;
    text-align: center;
}
QProgressBar::chunk { background: #1f6feb; border-radius: 3px; }
"""

C_GREEN  = "#3fb950"; C_RED    = "#f85149"; C_YELLOW = "#d29922"
C_BLUE   = "#58a6ff"; C_PURPLE = "#bc8cff"; C_DIM    = "#8b949e"
C_BG_CARD= "#161b22"; C_BORDER = "#30363d"
C_BG_BULL= "#0d2819"; C_BG_BEAR= "#2d0f0f"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def make_label(text, size=12, bold=False, color=None,
               align=Qt.AlignmentFlag.AlignLeft):
    lbl = QLabel(text)
    f = QFont("Segoe UI", size); f.setBold(bold)
    lbl.setFont(f)
    if color: lbl.setStyleSheet(f"color: {color};")
    lbl.setAlignment(align)
    return lbl


def card_frame(layout_cls=QVBoxLayout, pad=(12, 10, 12, 10)):
    frame = QFrame()
    frame.setStyleSheet(
        f"QFrame {{ background:{C_BG_CARD}; border:1px solid {C_BORDER}; border-radius:6px; }}")
    lay = layout_cls()
    lay.setContentsMargins(*pad)
    lay.setSpacing(6)
    frame.setLayout(lay)
    return frame, lay


def fmt_oi(n):
    n = int(n or 0)
    if n >= 10_000_000: return f"{n/10_000_000:.1f}Cr"
    if n >= 100_000:    return f"{n/100_000:.1f}L"
    return str(n)


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
            self.done.emit(self._fetch(ks.kite))
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
        except Exception as e:
            self.error.emit(str(e))


class BosWorker(QThread):
    done = pyqtSignal(list)

    def __init__(self, db, symbols):
        super().__init__()
        self.db = db; self.symbols = symbols

    def run(self):
        results = []
        if not BOS_OK or not self.db: self.done.emit(results); return
        for symbol in self.symbols:
            try:
                candles = self.db.get(symbol, n=BOS_LOOKBACK)
                if not candles or len(candles) < 10: continue
                try:
                    if datetime.strptime(candles[-1]["datetime"][:10], "%Y-%m-%d").date() < date.today():
                        continue
                except Exception:
                    pass
                bos = detect_bos(candles)
                if not bos["bos_type"]: continue
                setup = build_bos_setup(bos, symbol, candles[-1]["close"])
                results.append({"setup": setup, "bos": bos})
            except Exception:
                continue
        self.done.emit(results)


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
        self._oi_data = {}; self._worker = None
        self._build_ui(); self._try_cache()

    def _build_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(8, 8, 8, 8); root.setSpacing(8)

        # Header
        hdr = QHBoxLayout()
        hdr.addWidget(make_label("📊  OI Intelligence — NIFTY Options Chain", 14, bold=True, color=C_BLUE))
        hdr.addStretch()
        self.btn_refresh = QPushButton("⟳  Refresh OI")
        self.btn_refresh.setFixedWidth(130)
        self.btn_refresh.clicked.connect(self._fetch)
        hdr.addWidget(self.btn_refresh)
        root.addLayout(hdr)

        # Pill row
        pills = QHBoxLayout()
        self.p_spot   = self._pill("NIFTY Spot", "—",   C_BLUE,   180)
        self.p_atm    = self._pill("ATM Strike", "—",   C_YELLOW, 160)
        self.p_pain   = self._pill("Max Pain",   "—",   C_PURPLE, 160)
        self.p_pcr    = self._pill("PCR",        "—",   C_DIM,    120)
        self.p_expiry = self._pill("Expiry",     "—",   C_DIM,    140)
        self.p_ts     = self._pill("Last Fetch", "—",   C_DIM,    120)
        for p in [self.p_spot, self.p_atm, self.p_pain, self.p_pcr, self.p_expiry, self.p_ts]:
            pills.addWidget(p)
        pills.addStretch()
        root.addLayout(pills)

        # Direction card
        df, dl = card_frame(QVBoxLayout, (10, 8, 10, 8))
        row1 = QHBoxLayout()
        self.lbl_dir    = make_label("—", 16, bold=True, color=C_YELLOW)
        self.lbl_dr     = make_label("—", 11, color=C_DIM)
        row1.addWidget(self.lbl_dir); row1.addWidget(make_label("|", 12, color=C_BORDER))
        row1.addWidget(self.lbl_dr); row1.addStretch()
        dl.addLayout(row1)
        row2 = QHBoxLayout()
        self.lbl_advice = make_label("—", 11, color=C_DIM)
        self.lbl_setup  = make_label("—", 11, bold=True, color=C_GREEN)
        row2.addWidget(self.lbl_advice)
        row2.addWidget(make_label("  |  Setup: ", 11, color=C_DIM))
        row2.addWidget(self.lbl_setup); row2.addStretch()
        dl.addLayout(row2)
        root.addWidget(df)

        # Levels row
        lf, ll = card_frame(QHBoxLayout, (10, 6, 10, 6))
        self.b_cw  = self._badge("Call Wall",   "—", C_RED)
        self.b_pf  = self._badge("Put Floor",   "—", C_GREEN)
        self.b_sce = self._badge("Str CE OI",   "—", C_RED)
        self.b_spe = self._badge("Str PE OI",   "—", C_GREEN)
        self.lbl_pain_sig = make_label("—", 11, color=C_DIM)
        for w in [self.b_cw, self.b_pf, self.b_sce, self.b_spe]:
            ll.addWidget(w)
        ll.addStretch(); ll.addWidget(self.lbl_pain_sig)
        root.addWidget(lf)

        # Chain table
        ch_hdr = QHBoxLayout()
        ch_hdr.addWidget(make_label("Options Chain  (ATM ±10 strikes)", 12, bold=True, color=C_BLUE))
        ch_hdr.addStretch()
        ch_hdr.addWidget(make_label("← CE side   |   STRIKE   |   PE side →", 10, color=C_DIM))
        root.addLayout(ch_hdr)

        cols = ["CE IV", "CE OI Δ", "CE OI", "CE LTP",
                "STRIKE", "STATUS",
                "PE LTP", "PE OI", "PE OI Δ", "PE IV"]
        self.chain_tbl = QTableWidget(0, len(cols))
        self.chain_tbl.setHorizontalHeaderLabels(cols)
        self.chain_tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.chain_tbl.verticalHeader().setVisible(False)
        self.chain_tbl.setAlternatingRowColors(True)
        self.chain_tbl.setStyleSheet(self.chain_tbl.styleSheet() +
            "QTableWidget{alternate-background-color:#111820;}")
        root.addWidget(self.chain_tbl)

        self.status_lbl = make_label("Loading cache…", 10, color=C_DIM)
        root.addWidget(self.status_lbl)

    def _pill(self, label, value, color, width=150):
        frame = QFrame(); frame.setFixedWidth(width)
        frame.setStyleSheet(f"QFrame{{background:{color}18;border:1px solid {color}55;border-radius:5px;}}")
        lay = QVBoxLayout(frame); lay.setContentsMargins(6, 3, 6, 3); lay.setSpacing(1)
        lay.addWidget(make_label(label, 9, color=C_DIM, align=Qt.AlignmentFlag.AlignCenter))
        lbl = make_label(value, 14, bold=True, color=color, align=Qt.AlignmentFlag.AlignCenter)
        lay.addWidget(lbl); frame._v = lbl; return frame

    def _badge(self, label, value, color):
        frame = QFrame()
        frame.setStyleSheet(f"QFrame{{background:{color}15;border:1px solid {color}44;border-radius:4px;}}")
        lay = QHBoxLayout(frame); lay.setContentsMargins(8, 3, 8, 3); lay.setSpacing(4)
        lay.addWidget(make_label(label + ":", 10, color=C_DIM))
        lbl = make_label(value, 13, bold=True, color=color)
        lay.addWidget(lbl); frame._v = lbl; return frame

    def _sp(self, p, v): p._v.setText(str(v))
    def _sb(self, b, v): b._v.setText(str(v))

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
        self.oi_updated.emit(data)

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
        self.lbl_dir.setText(dirc); self.lbl_dir.setStyleSheet(f"color:{dc};font-size:15px;font-weight:bold;")
        self.lbl_dr.setText(d.get("direction_reason", ""))
        self.lbl_advice.setText(d.get("advice", ""))
        setup = d.get("setup", "—")
        sc = C_GREEN if "CE" in setup else C_RED
        self.lbl_setup.setText(setup); self.lbl_setup.setStyleSheet(f"color:{sc};font-weight:bold;")

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
            self.chain_tbl.setRowHeight(r, 22)
            bg = QColor("#1c1c3a" if is_atm else ("#151f15" if is_itm else "#1f1515"))

            def _it(text, color):
                item = QTableWidgetItem(str(text))
                item.setForeground(QColor(color)); item.setBackground(QBrush(bg))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                return item

            oi_add_c = lambda v: C_GREEN if v > 0 else (C_RED if v < 0 else C_DIM)
            self.chain_tbl.setItem(r, 0, _it(f"{row['CE_IV']:.1f}%",  C_DIM))
            self.chain_tbl.setItem(r, 1, _it(fmt_oi(row["CE_OI_ADD"]), oi_add_c(row["CE_OI_ADD"])))
            self.chain_tbl.setItem(r, 2, _it(fmt_oi(row["CE_OI"]),     C_RED))
            self.chain_tbl.setItem(r, 3, _it(f"{row['CE_LTP']:.1f}",   C_RED))
            # Strike (bold if ATM)
            si = QTableWidgetItem(str(s))
            si.setForeground(QColor(C_YELLOW if is_atm else "#e6edf3"))
            si.setBackground(QBrush(bg))
            si.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            sf = QFont("Segoe UI", 12); sf.setBold(is_atm); si.setFont(sf)
            self.chain_tbl.setItem(r, 4, si)
            self.chain_tbl.setItem(r, 5, _it(row["STATUS"], C_YELLOW if is_atm else C_DIM))
            self.chain_tbl.setItem(r, 6, _it(f"{row['PE_LTP']:.1f}",   C_GREEN))
            self.chain_tbl.setItem(r, 7, _it(fmt_oi(row["PE_OI"]),     C_GREEN))
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
        root = QVBoxLayout(self); root.setContentsMargins(8, 8, 8, 8); root.setSpacing(8)

        hdr = QHBoxLayout()
        hdr.addWidget(make_label("🧠  SMC + OI Confluence Engine", 14, bold=True, color=C_BLUE))
        hdr.addStretch()
        self.btn_run = QPushButton("⟳  Run SMC Analysis")
        self.btn_run.setFixedWidth(170); self.btn_run.clicked.connect(self._run)
        hdr.addWidget(self.btn_run); root.addLayout(hdr)

        # Main signal banner
        sf, sl = card_frame(QVBoxLayout, (14, 12, 14, 12))
        inner = QHBoxLayout()
        left = QVBoxLayout(); left.setSpacing(4)
        self.lbl_signal = make_label("— Run SMC Analysis —", 18, bold=True, color=C_YELLOW,
                                     align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_action = make_label("", 12, color=C_DIM, align=Qt.AlignmentFlag.AlignCenter)
        self.lbl_score  = make_label("Score: —", 13, bold=True, color=C_DIM,
                                     align=Qt.AlignmentFlag.AlignCenter)
        left.addWidget(self.lbl_signal); left.addWidget(self.lbl_action); left.addWidget(self.lbl_score)
        inner.addLayout(left, stretch=2)

        # Metrics grid
        mg = QGridLayout(); mg.setSpacing(8); mg.setContentsMargins(8, 6, 8, 6)
        self._m = {}
        for i, (label, key) in enumerate([
            ("OI Direction", "oi_direction"), ("SMC LTF", "smc_trend_ltf"),
            ("SMC HTF", "smc_trend_htf"),    ("P/D Zone", "pd_zone"),
            ("PCR", "oi_pcr"),               ("Conflict", "conflict"),
        ]):
            col = i % 3; row = i // 3
            vb = QVBoxLayout()
            vb.addWidget(make_label(label, 9, color=C_DIM))
            lbl = make_label("—", 12, bold=True, color=C_YELLOW); vb.addWidget(lbl)
            mg.addLayout(vb, row, col)
            self._m[key] = lbl
        mf = QFrame(); mf.setStyleSheet("QFrame{background:#0d1117;border:none;}")
        mf.setLayout(mg)
        inner.addWidget(mf, stretch=1)
        sl.addLayout(inner); root.addWidget(sf)

        # Key levels row
        klf, kll = card_frame(QHBoxLayout, (10, 6, 10, 6)); kll.setSpacing(14)
        self.kl = {}
        for label, color in [
            ("Call Wall", C_RED), ("Put Floor", C_GREEN),
            ("Bull OB", C_GREEN), ("Bear OB", C_RED),
            ("Bull FVG", C_GREEN), ("Bear FVG", C_RED),
            ("Buy Liq", C_YELLOW), ("Sell Liq", C_YELLOW),
        ]:
            vb = QVBoxLayout()
            vb.addWidget(make_label(label, 9, color=C_DIM))
            lbl = make_label("—", 12, bold=True, color=color); vb.addWidget(lbl)
            kll.addLayout(vb); self.kl[label] = lbl
        kll.addStretch(); root.addWidget(klf)

        # Reasons
        root.addWidget(make_label("📋  Analysis Reasons", 12, bold=True, color=C_BLUE))
        self.reasons_tbl = QTableWidget(0, 1)
        self.reasons_tbl.horizontalHeader().setVisible(False)
        self.reasons_tbl.verticalHeader().setVisible(False)
        self.reasons_tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.reasons_tbl.setMaximumHeight(180)
        self.reasons_tbl.setAlternatingRowColors(True)
        self.reasons_tbl.setStyleSheet(self.reasons_tbl.styleSheet() +
            "QTableWidget{alternate-background-color:#111820;}")
        root.addWidget(self.reasons_tbl)

        # Setup card
        root.addWidget(make_label("🎯  Trade Setup", 12, bold=True, color=C_BLUE))
        tsf, tsl = card_frame(QVBoxLayout, (10, 8, 10, 8))
        self.lbl_setup = make_label("—", 12, color=C_DIM); self.lbl_setup.setWordWrap(True)
        tsl.addWidget(self.lbl_setup); root.addWidget(tsf)

        self.status_lbl = make_label("Fetch OI first, then run SMC analysis.", 10, color=C_DIM)
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
    def __init__(self, db):
        super().__init__()
        self.db=db; self.symbols=[]; self.events=[]; self.worker=None
        self._build_ui(); self._load_symbols(); self._try_cache()

    def _build_ui(self):
        root=QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(8)
        hdr=QHBoxLayout()
        hdr.addWidget(make_label("📐  1-Hour BOS / CHoCH Scanner",14,bold=True,color=C_BLUE)); hdr.addStretch()
        self.scan_btn=QPushButton("⟳  Scan Now"); self.scan_btn.setFixedWidth(120)
        self.scan_btn.clicked.connect(self.run_scan); hdr.addWidget(self.scan_btn); root.addLayout(hdr)
        sr=QHBoxLayout()
        self.lbl_total=self._pill("Events","0",C_BLUE); self.lbl_bull=self._pill("Bull 🚀","0",C_GREEN)
        self.lbl_bear=self._pill("Bear 💥","0",C_RED); self.lbl_choch=self._pill("CHoCH 🔄","0",C_PURPLE)
        self.lbl_ts=make_label("Last scan: —",10,color=C_DIM)
        for w in [self.lbl_total,self.lbl_bull,self.lbl_bear,self.lbl_choch]: sr.addWidget(w)
        sr.addStretch(); sr.addWidget(self.lbl_ts); root.addLayout(sr)
        ff,fl=card_frame(QHBoxLayout); fl.setContentsMargins(8,6,8,6)
        fl.addWidget(make_label("Type:",11,color=C_DIM))
        self.cmb=QComboBox(); self.cmb.addItems(["All","BOS only","CHoCH only","Bullish","Bearish"])
        self.cmb.currentTextChanged.connect(self._apply); fl.addWidget(self.cmb)
        fl.addWidget(make_label("  Min R:R:",11,color=C_DIM))
        self.srr=QDoubleSpinBox(); self.srr.setRange(0,10); self.srr.setValue(1.5)
        self.srr.setSingleStep(0.5); self.srr.valueChanged.connect(self._apply); fl.addWidget(self.srr)
        fl.addWidget(make_label("  Min Vol:",11,color=C_DIM))
        self.svol=QDoubleSpinBox(); self.svol.setRange(0,10); self.svol.setValue(1.2)
        self.svol.setSingleStep(0.1); self.svol.valueChanged.connect(self._apply); fl.addWidget(self.svol)
        fl.addStretch(); root.addWidget(ff)
        cols=["Symbol","Type","LTP","Broke","Strength","Vol×","OB Zone","Next Liq","SL","T1","T2","R:R now","R:R ret","Prior"]
        self.tbl=QTableWidget(0,len(cols)); self.tbl.setHorizontalHeaderLabels(cols)
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.verticalHeader().setVisible(False)
        self.tbl.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.tbl.setAlternatingRowColors(True)
        self.tbl.setStyleSheet(self.tbl.styleSheet()+"QTableWidget{alternate-background-color:#111820;}")
        root.addWidget(self.tbl)
        self.status_lbl=make_label("Loading…",10,color=C_DIM); root.addWidget(self.status_lbl)

    def _pill(self,label,value,color):
        f=QFrame(); f.setStyleSheet(f"QFrame{{background:{color}22;border:1px solid {color}66;border-radius:4px;}}")
        l=QHBoxLayout(f); l.setContentsMargins(6,2,6,2); l.setSpacing(4)
        l.addWidget(make_label(label,10,color=C_DIM)); lv=make_label(value,13,bold=True,color=color)
        l.addWidget(lv); f._v=lv; return f

    def _load_symbols(self):
        if not DB_OK or not self.db: return
        self.symbols=self.db.get_all_symbols(); self.status_lbl.setText(f"{len(self.symbols)} symbols in DB")

    def _try_cache(self):
        if not BOS_OK: return
        c=load_scan_cache()
        if c and c.get("events"):
            self.events=c["events"]; self.lbl_ts.setText(f"Last scan: {c.get('time','')}")
            self._apply(); self._stats(self.events)
        else: self.status_lbl.setText("No cache — click Scan Now")

    def _stats(self,events):
        bull=sum(1 for e in events if "UP" in e.get("bos_type",""))
        bear=sum(1 for e in events if "DOWN" in e.get("bos_type",""))
        choch=sum(1 for e in events if "CHOCH" in e.get("bos_type",""))
        self.lbl_total._v.setText(str(len(events))); self.lbl_bull._v.setText(str(bull))
        self.lbl_bear._v.setText(str(bear)); self.lbl_choch._v.setText(str(choch))

    def _apply(self):
        ft=self.cmb.currentText(); mr=self.srr.value(); mv=self.svol.value()
        fil=[]
        for ev in self.events:
            bt=ev.get("bos_type",""); rr=max(ev.get("rr_now") or 0,ev.get("rr_retest") or 0); vol=ev.get("vol_ratio",1.0)
            if ft=="BOS only" and "CHOCH" in bt: continue
            if ft=="CHoCH only" and "CHOCH" not in bt: continue
            if ft=="Bullish" and "DOWN" in bt: continue
            if ft=="Bearish" and "UP" in bt: continue
            if rr<mr or vol<mv: continue
            fil.append(ev)
        self._fill(fil)

    def _fill(self,events):
        self.tbl.setRowCount(0)
        for ev in events:
            bt=ev.get("bos_type",""); sym=ev.get("symbol",""); ltp=ev.get("ltp",0)
            broken=ev.get("broken",0); sl=ev.get("sl",0); t1=ev.get("t1",0); t2=ev.get("t2",0)
            rr_now=ev.get("rr_now",0); rr_ret=ev.get("rr_retest",0); strength=ev.get("strength",0)
            vol_r=ev.get("vol_ratio",1.0); obl=ev.get("ob_low",0); obh=ev.get("ob_high",0)
            nxt=ev.get("next_liq",0); pt=ev.get("prev_trend","")
            ib="UP" in bt; ic="CHOCH" in bt; icon="🚀" if ib and not ic else ("🔄" if ic else "💥")
            tc=C_GREEN if ib else C_RED
            if ic: tc=C_PURPLE
            rb=QColor(C_BG_BULL if ib else C_BG_BEAR)
            r=self.tbl.rowCount(); self.tbl.insertRow(r)
            for col,(text,color) in enumerate([
                (sym,C_BLUE),(f"{icon} {bt}",tc),(f"{ltp:.1f}","#e6edf3"),
                (f"{broken:.1f}",C_YELLOW),(f"{strength:.2f}%",C_DIM),(f"{vol_r:.1f}×",C_YELLOW if vol_r>=2 else "#e6edf3"),
                (f"{obl:.1f}–{obh:.1f}",C_DIM),(f"{nxt:.1f}",C_DIM),
                (f"{sl:.1f}",C_RED),(f"{t1:.1f}",C_GREEN),(f"{t2:.1f}",C_GREEN),
                (f"{rr_now}:1",C_GREEN if rr_now>=2 else C_DIM),(f"{rr_ret}:1",C_GREEN if rr_ret>=2 else C_DIM),(pt,C_DIM)
            ]):
                item=QTableWidgetItem(text); item.setForeground(QColor(color))
                item.setBackground(QBrush(rb)); item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tbl.setItem(r,col,item)
        self.status_lbl.setText(f"Showing {self.tbl.rowCount()} events")
        self.tbl.resizeColumnsToContents()

    def run_scan(self):
        if not self.symbols: self.status_lbl.setText("No symbols in DB"); return
        if self.worker and self.worker.isRunning(): return
        self.scan_btn.setEnabled(False); self.scan_btn.setText("Scanning…")
        self.status_lbl.setText(f"Scanning {len(self.symbols)} symbols…")
        self.worker=BosWorker(self.db,self.symbols); self.worker.done.connect(self._on_scan); self.worker.start()

    def _on_scan(self,results):
        self.scan_btn.setEnabled(True); self.scan_btn.setText("⟳  Scan Now")
        self.lbl_ts.setText(f"Last scan: {datetime.now().strftime('%H:%M:%S')}")
        self.events=[]
        for r in results:
            s=r["setup"]
            self.events.append({"symbol":s["symbol"],"bos_type":s["bos_type"],"ltp":s["ltp"],
                "broken":s["broken_level"],"sl":s["sl"],"t1":s["t1"],"t2":s["t2"],
                "rr_now":s["rr_now"],"rr_retest":s["rr_retest"],"strength":s["strength"],
                "vol_ratio":s["volume_ratio"],"ob_low":s["ob_low"],"ob_high":s["ob_high"],
                "next_liq":s["next_liq"],"prev_trend":s.get("prev_trend","")})
        self._stats(self.events); self._apply()


class AstroPanel(QWidget):
    def __init__(self):
        super().__init__(); self._build_ui(); self._refresh()

    def _build_ui(self):
        root=QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(10)
        hdr=QHBoxLayout()
        hdr.addWidget(make_label("🌙  Vedic / KP Astro Score",14,bold=True,color=C_BLUE)); hdr.addStretch()
        btn=QPushButton("⟳  Refresh"); btn.clicked.connect(self._refresh); btn.setFixedWidth(100); hdr.addWidget(btn)
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
        root=QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(8)
        hdr=QHBoxLayout()
        hdr.addWidget(make_label("📦  OHLC Database Status",14,bold=True,color=C_BLUE)); hdr.addStretch()
        btn=QPushButton("⟳  Refresh"); btn.clicked.connect(self._refresh); btn.setFixedWidth(100); hdr.addWidget(btn)
        root.addLayout(hdr)
        pr=QHBoxLayout()
        self.p_syms=self._card("Symbols","—",C_BLUE); self.p_can=self._card("Candles","—",C_GREEN)
        self.p_today=self._card("Updated Today","—",C_YELLOW); self.p_sz=self._card("DB Size","—",C_PURPLE)
        for p in [self.p_syms,self.p_can,self.p_today,self.p_sz]: pr.addWidget(p)
        root.addLayout(pr)
        root.addWidget(make_label("📋  Per-Symbol Status",12,bold=True,color=C_BLUE))
        cols=["Symbol","Last Updated","Last Candle","Candles"]
        self.sym_tbl=QTableWidget(0,len(cols)); self.sym_tbl.setHorizontalHeaderLabels(cols)
        self.sym_tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.sym_tbl.horizontalHeader().setStretchLastSection(True)
        self.sym_tbl.verticalHeader().setVisible(False); self.sym_tbl.setAlternatingRowColors(True)
        root.addWidget(self.sym_tbl)

    def _card(self,label,value,color):
        f,l=card_frame(QVBoxLayout); f.setFixedWidth(160)
        l.addWidget(make_label(label,10,color=C_DIM,align=Qt.AlignmentFlag.AlignCenter))
        lv=make_label(value,18,bold=True,color=color,align=Qt.AlignmentFlag.AlignCenter); l.addWidget(lv); f._v=lv; return f

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
    trigger = pyqtSignal()

    def __init__(self, interval_secs=AUTO_REFRESH_SECS):
        super().__init__()
        self._interval = interval_secs; self._remaining = interval_secs; self._running = False
        lay = QHBoxLayout(self); lay.setContentsMargins(8, 2, 8, 2); lay.setSpacing(8)
        self.lbl = make_label("Auto-refresh: OFF", 10, color=C_DIM)
        self.bar = QProgressBar(); self.bar.setRange(0, self._interval)
        self.bar.setValue(self._interval); self.bar.setFixedHeight(4); self.bar.setTextVisible(False)
        self.btn = QPushButton("▶  Start Auto"); self.btn.setFixedWidth(120); self.btn.clicked.connect(self._toggle)
        lay.addWidget(self.lbl); lay.addWidget(self.bar, stretch=1); lay.addWidget(self.btn)
        t = QTimer(self); t.timeout.connect(self._tick); t.start(1000)

    def _toggle(self):
        self._running = not self._running
        if self._running:
            self._remaining = self._interval
            self.btn.setText("⏹  Stop Auto")
        else:
            self.btn.setText("▶  Start Auto")
            self.lbl.setText("Auto-refresh: OFF")
            self.bar.setValue(self._interval)

    def _tick(self):
        if not self._running: return
        if not (DB_OK and is_market_hours()): return
        self._remaining -= 1
        self.bar.setValue(self._remaining)
        self.lbl.setText(f"Next OI refresh: {self._remaining}s")
        if self._remaining <= 0:
            self._remaining = self._interval; self.bar.setValue(self._interval)
            self.trigger.emit()


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
        self.setWindowTitle("📈  Panchak Trading Dashboard  — Phase 3")
        self.setMinimumSize(QSize(1280, 800))
        self.resize(1500, 900)
        self.setStyleSheet(DARK_QSS)
        self.db = OHLCStore() if DB_OK else None

        # Header
        header = QFrame()
        header.setStyleSheet(f"background:{C_BG_CARD};border-bottom:1px solid {C_BORDER};")
        hl = QHBoxLayout(header); hl.setContentsMargins(16, 8, 16, 8); hl.setSpacing(12)
        hl.addWidget(make_label("📈  PANCHAK DASHBOARD", 15, bold=True, color=C_BLUE))
        hl.addWidget(make_label("|", 14, color=C_BORDER))
        self.lbl_spot = make_label("NIFTY: —", 14, bold=True, color=C_YELLOW)
        hl.addWidget(self.lbl_spot); hl.addStretch()
        self.lbl_kite = make_label("🔴 Kite: disconnected", 11, color=C_RED)
        hl.addWidget(self.lbl_kite); hl.addWidget(make_label("|", 12, color=C_BORDER))
        self.lbl_market = make_label("—", 12, color=C_DIM)
        hl.addWidget(self.lbl_market); hl.addWidget(make_label("|", 12, color=C_BORDER))
        self.lbl_clock = make_label("—", 13, bold=True, color=C_YELLOW)
        hl.addWidget(self.lbl_clock)

        # Core panels (Phase 1 + 2)
        self.oi_panel    = OIPanel()
        self.smc_panel   = SMCPanel(self.oi_panel)
        self.bos_panel   = BosPanel(self.db)
        self.astro_panel = AstroPanel()
        self.time_panel  = TimePanel()
        self.db_panel    = DbPanel(self.db)
        self.sess_panel  = SessionPanel()

        # Phase 3 panels
        if SCREENER_OK:
            self.screener_panel = ScreenerPanel(STOCKS)
            self.alerts_panel   = AlertsPanel()
            self.alerts_panel.load_settings()
        else:
            _ph = QWidget()
            _lbl = make_label("qt_screener.py not found — place it in the same folder",
                               13, color=C_RED, align=Qt.AlignmentFlag.AlignCenter)
            _lay = QVBoxLayout(_ph); _lay.addWidget(_lbl)
            self.screener_panel = _ph
            self.alerts_panel   = QWidget()

        # Cross-panel wiring
        self.oi_panel.oi_updated.connect(self.smc_panel.on_oi_updated)
        self.oi_panel.oi_updated.connect(self._on_spot)
        self.sess_panel.connected.connect(self._on_kite_connected)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.addTab(self.oi_panel,       "📊  OI Intel")
        self.tabs.addTab(self.smc_panel,      "🧠  SMC + OI")
        self.tabs.addTab(self.bos_panel,      "📐  BOS")
        self.tabs.addTab(self.screener_panel, "📈  Screeners")
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
            "Phase 3 — OI | SMC | BOS | Screeners | Alerts | Astro | Time | DB | Session")

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
        self.lbl_kite.setText("🟢 Kite: connected")
        self.lbl_kite.setStyleSheet(f"color:{C_GREEN};font-size:11px;")
        self.statusBar().showMessage("Kite connected — fetching OI…")
        self.tabs.setCurrentIndex(0)
        self.oi_panel.trigger_refresh()

    def _on_spot(self, data):
        spot = data.get("spot", 0)
        if spot: self.lbl_spot.setText(f"NIFTY: {spot:,.1f}")

    def _auto_refresh(self):
        """Fires every AUTO_REFRESH_SECS during market hours. Refreshes OI + Screeners."""
        self.oi_panel.trigger_refresh()
        if SCREENER_OK:
            self.screener_panel.trigger_scan()
        self.statusBar().showMessage(
            f"Auto-refreshed at {datetime.now().strftime('%H:%M:%S')}")

    def _tick_clock(self):
        try:
            import pytz; IST = pytz.timezone("Asia/Kolkata"); now = datetime.now(IST)
        except Exception:
            now = datetime.now()
        self.lbl_clock.setText(now.strftime("%H:%M:%S IST | %a %d-%b-%Y"))
        mo = is_market_hours() if DB_OK else False
        self.lbl_market.setText("🟢 MARKET OPEN" if mo else "🔴 MARKET CLOSED")
        self.lbl_market.setStyleSheet(f"color:{C_GREEN if mo else C_RED};")


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
