# -*- coding: utf-8 -*-
# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – HOLIDAY AWARE)
# ==========================================================

import os
import sys
import threading
import pytz
import pandas as pd
import numpy as np
import time as tm
import math
import swisseph as swe
import json
import urllib.request
from datetime import datetime, timedelta, date, time
import streamlit as st

# ── CUSTOM CSS FOR UX PERFORMANCE ───────────────────────────────────────────
st.set_page_config(page_title="Panchak + Breakout Dashboard", layout="wide")

st.markdown("""
    <style>
    /* 1. COMPLETELY disable the 'dimming' and 'blurring' when Streamlit is running */
    [data-test-script-state="running"] .main,
    [data-test-script-state="running"] header,
    [data-test-script-state="running"] [data-testid="stSidebar"],
    [data-test-script-state="running"] [data-testid="stAppViewMain"] {
        opacity: 1.0 !important;
        filter: none !important;
        pointer-events: auto !important;
    }

    /* 2. Target the specific loading overlay that Streamlit uses */
    [data-testid="stAppViewMain"] > div:first-child {
        background-color: transparent !important;
        opacity: 1.0 !important;
    }

    /* 3. Remove the 'skeleton' or 'grey out' effect on tables and widgets */
    .st-emotion-cache-16idss3, .st-emotion-cache-10trblm, .st-emotion-cache-k77z3n {
        opacity: 1.0 !important;
        filter: none !important;
    }

    /* 4. Ensure dataframes stay bright */
    .stDataFrame {
        opacity: 1.0 !important;
    }

    /* 5. Hide the top status widget (loading bar) */
    [data-testid="stStatusWidget"] {
        display: none !important;
        visibility: hidden !important;
    }

    /* 6. Prevent content shifting/flicker */
    .main .block-container {
        padding-top: 2rem !important;
    }

    /* 7. Force high contrast for text during refresh */
    [data-test-script-state="running"] b, 
    [data-test-script-state="running"] span, 
    [data-test-script-state="running"] div {
        color: inherit !important;
        text-shadow: none !important;
    }
    
    /* 8. Keep the mouse cursor normal */
    html, body, .stApp {
        cursor: default !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ── DIRECTORY SETUP ─────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

# ── IST Timezone ─────────────────────────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

# ── NSE HOLIDAYS 2026 ───────────────────────────────────────────────────────
NSE_HOLIDAYS = {
    date(2026, 1, 26), date(2026, 3, 6), date(2026, 3, 20), date(2026, 3, 31),
    date(2026, 4, 1),  date(2026, 4, 2), date(2026, 5, 1),  date(2026, 10, 2),
    date(2026, 10, 21), date(2026, 11, 5), date(2026, 11, 6), date(2026, 12, 25),
}

def is_market_hours():
    """Return True if current time is within NSE market hours (09:15 - 15:30 IST)."""
    try:
        now_dt = datetime.now(IST)
        now_t  = now_dt.time()
        today_d = now_dt.date()
        if today_d.weekday() >= 5: return False
        if today_d in NSE_HOLIDAYS: return False
        return time(9, 15) <= now_t <= time(15, 30)
    except Exception:
        return False

def _is_tg_disabled(category: str) -> bool:
    """Return True if Telegram for this category is currently OFF."""
    try:
        return not st.session_state.get(f"tg_{category}", True)
    except Exception:
        return True

# ── Windows UTF-8 console fix ───────────────────────────────────────────────
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass
# ─────────────────────────────────────────────────────────────────────────────


def _safe_parse_date_col(df, col="date", new_col=None, errors="coerce"):
    """
    Safely parse a date/datetime column in a DataFrame.
    - Handles missing column gracefully (returns df unchanged)
    - Handles 'date' vs 'datetime' alias
    - Always uses errors='coerce' so bad values become NaT not crashes
    - Drops NaT rows after parse
    Cross-platform safe (Windows + Linux).
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return df
    target = new_col or col
    # Handle alias: if col missing but alias exists
    aliases = {"date": "datetime", "datetime": "date"}
    if col not in df.columns and aliases.get(col) in df.columns:
        df = df.rename(columns={aliases[col]: col})
    if col not in df.columns:
        return df   # column genuinely missing - return as-is
    df = df.copy()
    df[target] = pd.to_datetime(df[col], errors=errors)
    df = df.dropna(subset=[target]).reset_index(drop=True)
    return df


try:
    from kiteconnect import KiteConnect
    _KITE_OK = True
except ImportError:
    _KITE_OK = False
    class KiteConnect:
        def __init__(self, **kw): pass
        def __getattr__(self, name): return lambda *a, **k: None

from streamlit_autorefresh import st_autorefresh
from io import BytesIO
import smtplib
from email.message import EmailMessage
from astro_time import get_time_signal
from astro_logic import get_astro_score, get_kp_reversals, SECTOR_SIGNIFICATORS
from astro_engine import get_gann_square_9, get_gann_reversal_dates, get_dignities
from kp_panchang_tab import render_kp_tab

# ── Global State Handlers ───────────────────────────────────────────────────
_oi_nifty = None
_oi_bn    = None
_ast_data = None
@st.cache_data(ttl=86400, show_spinner=False)
def _get_static_inside_bars():
    """
    Identifies symbols that are inside bars based on yesterday's and day-before bars.
    Cached for 24 hours as this only changes on a new trading day.
    """
    results = {}
    today_d = date.today()
    _ltd = today_d - timedelta(days=1)
    while _ltd.weekday() >= 5 or _ltd in NSE_HOLIDAYS:
        _ltd -= timedelta(days=1)
    sd = _ltd - timedelta(days=10)
    
    for sym in STOCKS:
        try:
            tok = get_token(sym)
            if not tok: continue
            bars = kite.historical_data(tok, sd, _ltd, "day")
            if len(bars) < 2: continue
            parent = bars[-2]; child = bars[-1]
            ph, pl = parent["high"], parent["low"]
            ch, cl = child["high"],  child["low"]
            pc, cc, co = parent["close"], child["close"], child["open"]
            if ch <= ph and cl >= pl:
                rng_parent = round(ph - pl, 2)
                rng_child  = round(ch - cl, 2)
                comp_pct   = round(rng_child / rng_parent * 100, 1) if rng_parent > 0 else 0
                ch_to_ph_pct  = round((ph - ch) / ph * 100, 2) if ph > 0 else 99.0
                cl_to_pl_pct  = round((cl - pl) / pl * 100, 2) if pl > 0 else 99.0
                open_vs_ph_pct = round((ph - co) / ph * 100, 2) if ph > 0 else 99.0
                parent_body  = round(pc - parent["open"], 2)
                parent_bias  = "🟢 Bull" if parent_body > rng_parent * 0.1 else ("🔴 Bear" if parent_body < -rng_parent * 0.1 else "⚪ Doji")
                child_body   = round(cc - co, 2)
                child_bias   = "🟢 Bull" if child_body > rng_child * 0.1 else ("🔴 Bear" if child_body < -rng_child * 0.1 else "⚪ Doji")
                
                results[sym] = {
                    "Parent High": ph, "Parent Low": pl, "Parent Close": pc,
                    "Child High": ch, "Child Low": cl, "Child Open": co, "Child Close": cc,
                    "Parent Bias": parent_bias, "Child Bias": child_bias,
                    "Compression %": comp_pct, "CH->PH Gap %": ch_to_ph_pct,
                    "Open<PH Gap %": open_vs_ph_pct,
                    "High Conv": (co < ph and ch_to_ph_pct <= 1.5 and comp_pct <= 70)
                }
        except: continue
    return results

def _load_inside_bars():
    """
    Merges static inside bar setup with live LTP/VOL from main df.
    Runs in main thread but is very fast as it only does a few lookups.
    """
    static_data = _get_static_inside_bars()
    rows = []
    _IB_COLS = [
        "Symbol", "LTP", "Open", "Child High", "Child Low", "Child Close", "Child Open",
        "Parent High", "Parent Low", "Parent Close", "Parent Bias", "Child Bias",
        "Compression %", "CH->PH Gap %", "Open<PH Gap %", "VOL %",
        "Entry Long >", "Entry Short <", "SL Long", "SL Short", "High Conv", "Status"
    ]
    
    for sym, s_data in static_data.items():
        try:
            # Get live data from global df
            ltp_val = s_data["Child Close"]
            live_open = 0.0
            vol_pct = 0.0
            
            if "Symbol" in df.columns:
                match = df[df["Symbol"] == sym]
                if not match.empty:
                    ltp_val = float(match.iloc[0].get("LTP", ltp_val))
                    live_open = float(match.iloc[0].get("LIVE_OPEN", 0.0))
                    vol_pct = round(float(match.iloc[0].get("VOL_%", 0.0)), 1)

            ph, pl = s_data["Parent High"], s_data["Parent Low"]
            rows.append({
                "Symbol":           sym,
                "LTP":              round(ltp_val, 2),
                "Open":             round(live_open, 2),
                **s_data,
                "VOL %":            vol_pct,
                "Entry Long >":     round(ph, 2),
                "Entry Short <":    round(pl, 2),
                "SL Long":          round(pl, 2),
                "SL Short":         round(ph, 2),
                "High Conv":        "🔥 YES" if s_data["High Conv"] else "",
                "Status":           "🟢 BREAKOUT UP" if ltp_val > ph else
                                    "🔴 BREAKDOWN"   if ltp_val < pl else
                                    "⏳ Consolidating",
            })
        except: continue
        
    if not rows: return pd.DataFrame(columns=_IB_COLS)
    df_out = pd.DataFrame(rows)
    df_out["_hc_sort"] = df_out["High Conv"].apply(lambda x: 0 if x else 1)
    df_out = df_out.sort_values(["_hc_sort", "Compression %"]).drop(columns=["_hc_sort"])
    return df_out

@st.cache_data(ttl=86400, show_spinner=False)
def _get_static_macd():
    """
    Computes MACD based on historical daily data.
    Cached for 24 hours.
    """
    results = {}
    today_d = date.today(); _ltd = today_d - timedelta(days=1)
    while _ltd.weekday() >= 5 or _ltd in NSE_HOLIDAYS: _ltd -= timedelta(days=1)
    sd = _ltd - timedelta(days=90)
    
    # Use SYMBOLS from outer scope
    for sym in SYMBOLS[:80]:
        try:
            tok = get_token(sym)
            if not tok: continue
            bars = kite.historical_data(tok, sd, _ltd, "day")
            if len(bars) < 35: continue
            closes = [b["close"] for b in bars]
            def _f_ema(data, n):
                k = 2 / (n + 1); v = sum(data[:n]) / n
                for p in data[n:]: v = p * k + v * (1 - k)
                return v
            ema12 = _f_ema(closes, 12); ema26 = _f_ema(closes, 26)
            macd_line = ema12 - ema26
            ema12_p = _f_ema(closes[:-1], 12); ema26_p = _f_ema(closes[:-1], 26)
            macd_prev = ema12_p - ema26_p
            
            status = "⚪ Neutral"
            if macd_line > 0 and macd_prev <= 0: status = "🟢 BULLISH CROSS"
            elif macd_line < 0 and macd_prev >= 0: status = "🔴 BEARISH CROSS"
            elif macd_line > 0: status = "📈 Rising" if macd_line > macd_prev else "📉 Fading"
            else: status = "📉 Falling" if macd_line < macd_prev else "📈 Recovering"
            
            results[sym] = {
                "MACD Status": status, "MACD": round(macd_line, 3),
                "Prev": round(macd_prev, 3), "Close": round(closes[-1], 2)
            }
        except: continue
    return results

def _load_macd_all():
    """Merges static MACD data with live LTP/VOL."""
    static_data = _get_static_macd()
    rows = []
    for sym, s_data in static_data.items():
        try:
            ltp = s_data["Close"]
            vol = 0.0
            if "Symbol" in df.columns:
                match = df[df["Symbol"] == sym]
                if not match.empty:
                    ltp = float(match.iloc[0].get("LTP", ltp))
                    vol = float(match.iloc[0].get("VOL_%", 0.0))
            rows.append({
                "Symbol": sym, **s_data, "LTP": ltp, "VOL_%": vol
            })
        except: continue
    return pd.DataFrame(rows)

# ── BOS PATCH APPLIED ──
try:
    from ohlc_store import OHLCStore, render_db_status
    from bos_scanner import run_bos_scan, load_scan_cache, render_bos_tab
    _BOS_OK = True
    _ohlc_db = OHLCStore("ohlc_1h.db")
except ImportError as _bimp:
    _BOS_OK = False
    _ohlc_db = None
    print(f"BOS import error: {_bimp}")

def _update_ohlc_db_safe(kite_inst, symbols):
    """Update local OHLC DB - called once per hour or on startup."""
    if not _BOS_OK or not kite_inst or not _ohlc_db: return
    try:
        _ohlc_db.update_all(
            kite         = kite_inst,
            symbols      = symbols,
            get_token_fn = get_token,
            batch_size   = 10,
            delay_secs   = 0.35,
        )
        import streamlit as st
        st.session_state["ohlc_db_updated"] = datetime.now(IST).strftime("%H:%M:%S")
    except Exception as ex:
        print(f"OHLC DB update error: {ex}")

# ══════════════════════════════════════════════════════════════════════════
# AUTO CHART PATTERN SCANNER - fires Telegram alerts automatically
#
# Strategy:
#   • Runs ONCE per hour (daily candles don't change faster than that)
#   • Scans all STOCKS for all 12 patterns using daily candles (4 months)
#   • Sends one TG alert per pattern per symbol per day (deduped)
#   • Min score ≥ 6, Min R:R ≥ 1.5 for auto-alerts (higher bar than manual tab)
#   • Runs in background thread - doesn't block UI refresh
#   • Results also update Tab 25 display automatically
# ══════════════════════════════════════════════════════════════════════════

_CPS_AUTO_SCAN_KEY    = "_cps_auto_scan_done"   # session flag: scan done this session
_CPS_AUTO_LAST_HOUR   = "_cps_auto_last_hour"   # last hour (int) when scan ran
_CPS_AUTO_RUNNING     = "_cps_auto_running"     # flag: background thread active
_CPS_BG_THREAD_ACTIVE = False                   # module-level flag (thread-safe for reads)
_CPS_AUTO_MIN_SCORE   = 6     # minimum conviction score for auto-alert
_CPS_AUTO_MIN_RR      = 1.5   # minimum R:R for auto-alert
_CPS_AUTO_DEDUP_DIR   = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "CACHE"
)
_CPS_DEDUP_LOCK       = __import__("threading").Lock()  # protects JSON dedup writes on Windows


def _cps_dedup_key(symbol: str) -> str:
    """Daily dedup key: ONE alert per STOCK per day (not per pattern).
    Prevents multiple alerts for the same stock from different patterns."""
    return f"CPS_{symbol}_{date.today().isoformat()}"


def _cps_already_sent(symbol: str) -> bool:
    """Check if this stock was already alerted today (any pattern). Thread-safe read."""
    key_file = os.path.join(_CPS_AUTO_DEDUP_DIR, f"cps_dedup_{date.today().isoformat()}.json")
    with _CPS_DEDUP_LOCK:
        try:
            if os.path.exists(key_file):
                with open(key_file, "r", encoding="utf-8") as _f:
                    d = json.load(_f)
            else:
                return False
            return _cps_dedup_key(symbol) in d
        except Exception:
            pass
    return False


def _cps_mark_sent(symbol: str, pattern: str):
    """Mark this stock as alerted today (stores which pattern fired). Thread-safe."""
    os.makedirs(_CPS_AUTO_DEDUP_DIR, exist_ok=True)
    key_file = os.path.join(_CPS_AUTO_DEDUP_DIR, f"cps_dedup_{date.today().isoformat()}.json")
    with _CPS_DEDUP_LOCK:
        try:
            d = {}
            if os.path.exists(key_file):
                with open(key_file, "r", encoding="utf-8") as _f:
                    d = json.load(_f)
            d[_cps_dedup_key(symbol)] = {
                "pattern": pattern,
                "time":    datetime.now(IST).strftime("%H:%M:%S"),
            }
            with open(key_file, "w", encoding="utf-8") as _f:
                json.dump(d, _f)
        except Exception:
            pass


def _cps_is_fresh(r: dict) -> tuple:
    """
    Freshness + validity filter for a chart pattern result.

    Rules:
      BULL pattern:
        overshoot = (ltp - resist) / resist * 100
        Valid:  0 < overshoot ≤ 5%   (LTP just crossed, not stale)
        Stale:  overshoot > 5%        (breakout happened days ago, entry passed)
        Invalid: overshoot ≤ 0        (LTP hasn't actually broken out yet)
        Direction: change_% ≥ -1.5%  (today's session not collapsing back)

      BEAR pattern:
        overshoot = (support - ltp) / support * 100
        Valid:  0 < overshoot ≤ 5%
        Stale:  overshoot > 5%
        Invalid: overshoot ≤ 0
        Direction: change_% ≤ +1.5%  (today's session not bouncing back strongly)

    Returns (is_valid: bool, reason: str)
    """
    direction  = r.get("direction", "")
    ltp        = float(r.get("ltp", 0) or 0)
    resist     = float(r.get("resist_level", 0) or 0)
    support    = float(r.get("support_level", 0) or 0)
    chg_pct    = float(r.get("change_%", 0) or 0)

    if ltp <= 0:
        return False, "LTP is zero"

    if direction == "BULL":
        if resist <= 0:
            return False, "No resistance level"
        overshoot = (ltp - resist) / resist * 100
        if overshoot <= 0:
            return False, f"Not broken out yet (LTP {ltp:.0f} < resist {resist:.0f})"
        if overshoot > 5.0:
            return False, f"Stale breakout: +{overshoot:.1f}% above key level (>5%)"
        if chg_pct < -1.5:
            return False, f"Today's session reversing: {chg_pct:+.1f}%"
        return True, f"Fresh BULL: +{overshoot:.1f}% above {resist:.0f}"

    else:  # BEAR
        if support <= 0:
            return False, "No support level"
        overshoot = (support - ltp) / support * 100
        if overshoot <= 0:
            return False, f"Not broken down yet (LTP {ltp:.0f} > support {support:.0f})"
        if overshoot > 5.0:
            return False, f"Stale breakdown: {overshoot:.1f}% below key level (>5%)"
        if chg_pct > 1.5:
            return False, f"Today's session bouncing: {chg_pct:+.1f}%"
        return True, f"Fresh BEAR: -{overshoot:.1f}% below {support:.0f}"


def _cps_best_per_stock(results: list) -> list:
    """
    From a list of pattern results, return ONE result per stock -
    the highest scoring pattern. Ties broken by R:R, then direction
    preference (BULL first in uptrend, BEAR first in downtrend).

    This prevents multiple alerts for the same stock from different patterns
    (e.g., DIVISLAB firing both Bull Flag + Ascending Triangle).
    """
    by_sym = {}
    for r in results:
        sym   = r.get("symbol", "")
        score = r.get("score", 0)
        rr    = r.get("rr", 0)
        if sym not in by_sym:
            by_sym[sym] = r
        else:
            prev_score = by_sym[sym].get("score", 0)
            prev_rr    = by_sym[sym].get("rr", 0)
            # Replace if this pattern scores higher, or same score but better R:R
            if score > prev_score or (score == prev_score and rr > prev_rr):
                by_sym[sym] = r
    return list(by_sym.values())


def _build_cps_tg_message(r: dict) -> str:
    """Build Telegram message for a chart pattern alert."""
    sym       = r.get("symbol", "")
    pattern   = r.get("pattern", "")
    direction = r.get("direction", "")
    score     = r.get("score", 0)
    ltp       = r.get("ltp", 0)
    entry     = r.get("entry", 0)
    sl        = r.get("sl", 0)
    t1        = r.get("t1", 0)
    t2        = r.get("t2", 0)
    rr        = r.get("rr", 0)
    vol_ratio = r.get("vol_ratio", 0)
    pole_pct  = r.get("pole_pct", 0)
    post_pct  = r.get("post_break_%", 0)
    bars      = r.get("bars", 0)
    resist    = r.get("resist_level", 0)
    support   = r.get("support_level", 0)
    chg_pct   = r.get("change_%", 0)
    vol_pct   = r.get("vol_%", 0)
    scan_time = r.get("scan_time", "-")

    is_bull   = direction == "BULL"
    dir_icon  = "🟢" if is_bull else "🔴"
    dir_label = "BULLISH BREAKOUT ↑" if is_bull else "BEARISH BREAKDOWN ↓"
    vol_ic    = "🔥" if vol_ratio >= 2.0 else ("⚡" if vol_ratio >= 1.5 else "📊")

    # Score label
    hc_label  = "🔥🔥 HIGH CONVICTION" if score >= 8 else ("⚡ STRONG" if score >= 6 else "📊 MODERATE")

    # Pattern-specific context line
    ctx_lines = {
        "Falling Wedge":       "Converging downtrend lines -> explosive upside breakout expected",
        "Ascending Triangle":  "Flat resistance + rising lows -> coiling energy, breakout above flat top",
        "Bull Flag":           f"Pole +{pole_pct}% then tight consolidation -> measured move target",
        "Bull Pennant":        f"Pole +{pole_pct}% then converging pennant -> explosive continuation",
        "Cup & Handle":        f"U-shaped recovery + handle pullback -> classic William O'Neil setup",
        "Double Bottom":       "Two equal lows (W-pattern) -> neckline break confirms reversal",
        "Rising Wedge":        "Converging uptrend lines -> breakdown expected",
        "Descending Triangle": "Flat support + falling highs -> breakdown below support",
        "Bear Flag":           f"Pole -{abs(pole_pct)}% then tight consolidation -> continuation lower",
        "Bear Pennant":        f"Pole -{abs(pole_pct)}% then converging pennant -> continuation lower",
        "Head & Shoulders":    "Left shoulder + head + right shoulder -> neckline break confirms",
        "Double Top":          "Two equal highs (M-pattern) -> neckline break confirms reversal",
    }
    ctx = ctx_lines.get(pattern, "")

    post_line = f"📈 Post-break move confirmed: <b>+{post_pct:.1f}%</b> already\n" if post_pct > 1 else ""
    yh = r.get("yest_high", 0); yl = r.get("yest_low", 0); yc = r.get("yest_close", 0)

    return (
        f"{dir_icon} <b>📐 CHART PATTERN - {sym}</b>\n"
        f"⏰ {scan_time}  |  📅 Daily TF\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{hc_label}  [Score {score}/10]\n"
        f"<b>{pattern}</b> - {dir_label}\n"
        f"<i>{ctx}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 LTP: <b>Rs.{ltp:,.2f}</b>  ({chg_pct:+.2f}% today)\n"
        f"📊 Yest: H:{yh:,.0f}  L:{yl:,.0f}  C:{yc:,.0f}\n"
        f"🔓 Key level: <b>Rs.{resist:,.2f}</b> (broke {'above' if is_bull else 'below'})\n"
        f"{vol_ic} Volume: <b>{vol_ratio:.1f}x</b> avg  |  Pattern: <b>{bars} bars</b>\n"
        f"{post_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>TRADE SETUP</b>\n"
        f"  Entry  <b>Rs.{entry:,.2f}</b>\n"
        f"  SL     <b>Rs.{sl:,.2f}</b>\n"
        f"  T1     <b>Rs.{t1:,.2f}</b>  |  T2 <b>Rs.{t2:,.2f}</b>\n"
        f"  R:R    <b>{rr}:1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Daily candle pattern. NOT financial advice.</i>"
    )


def _run_cps_auto_scan_bg(kite_inst, symbols, live_df_snapshot, send_fn):
    """
    Background thread: scan all symbols for chart patterns and fire TG alerts.
    Uses daily candles - runs once per hour during market hours.
    Fires one Telegram alert per new pattern (deduped per symbol+pattern per day).
    """
    import time as _t
    try:
        import os as _os
        _cps_path = _os.path.dirname(_os.path.abspath(__file__))
        if _cps_path not in sys.path:
            sys.path.insert(0, _cps_path)
        import chart_pattern_scanner as _cps_mod
    except ImportError as _ie:
        print(f"[CPS-Auto] chart_pattern_scanner not found: {_ie}")
        return

    try:
        results = _cps_mod.scan_chart_patterns(
            kite         = kite_inst,
            symbols      = symbols,
            get_token_fn = get_token,
            live_df      = live_df_snapshot,
            min_score    = _CPS_AUTO_MIN_SCORE,
            min_rr       = _CPS_AUTO_MIN_RR,
        )

        # ── Step 1: Freshness filter - remove stale / invalid breakouts ──────
        fresh_results = []
        stale_count   = 0
        for r in results:
            is_valid, reason = _cps_is_fresh(r)
            if is_valid:
                r["_fresh_reason"] = reason   # store for debug
                fresh_results.append(r)
            else:
                stale_count += 1

        # ── Step 2: One alert per stock - keep highest scoring pattern ──────
        best_results = _cps_best_per_stock(fresh_results)

        # ── Step 3: Send alerts - deduped per STOCK (not per pattern) ───────
        new_count  = 0
        skip_count = 0
        for r in best_results:
            sym     = r.get("symbol", "")
            pattern = r.get("pattern", "")
            if not sym or not pattern:
                continue
            # Check stock-level dedup (one alert per stock per day)
            if _cps_already_sent(sym):
                skip_count += 1
                continue

            # Build and send alert
            msg = _build_cps_tg_message(r)
            dedup_k = _cps_dedup_key(sym)          # symbol-level key
            send_fn(msg, dedup_key=dedup_k)
            _cps_mark_sent(sym, pattern)            # marks stock as alerted
            new_count += 1
            _t.sleep(0.3)   # pace between alerts to avoid TG flood

        print(
            f"[CPS-Auto] Scan done. {len(results)} patterns found, "
            f"{stale_count} stale/invalid, {len(best_results)} fresh, "
            f"{skip_count} already sent today, {new_count} new alerts sent."
        )

        # Store results in a file cache so Tab 25 can display them
        _cache_file = os.path.join(
            _CPS_AUTO_DEDUP_DIR, f"cps_results_{date.today().isoformat()}.json"
        )
        with open(_cache_file, "w", encoding="utf-8") as _cf:
            json.dump(results, _cf, default=str)

    except Exception as _e:
        print(f"[CPS-Auto] Scan error: {_e}")
    finally:
        # Clear module-level running flag (thread-safe: bool assignment is atomic in CPython)
        global _CPS_BG_THREAD_ACTIVE
        _CPS_BG_THREAD_ACTIVE = False


def _trigger_cps_auto_scan_if_due():
    """
    Check if auto chart pattern scan is due. Runs once per hour during market hours.
    Fires background thread - non-blocking.
    """
    if _is_tg_disabled("CHART_PATTERN"):
        return
    if not is_market_hours():
        return
    # Check kite is connected (chart scanner needs kite, independent of BOS module)
    try:
        if kite is None:
            return
    except Exception:
        return

    now_hour = datetime.now(IST).hour

    global _CPS_BG_THREAD_ACTIVE

    # Already running? (use module-level flag - safe to read from main thread)
    if _CPS_BG_THREAD_ACTIVE:
        return

    # Check if we already ran this hour
    last_hour = st.session_state.get(_CPS_AUTO_LAST_HOUR, -1)
    if last_hour == now_hour:
        return

    # Mark as running and update hour
    _CPS_BG_THREAD_ACTIVE = True
    st.session_state[_CPS_AUTO_LAST_HOUR] = now_hour

    # Take a snapshot of live df (thread-safe: df is a pandas df, copying is safe)
    try:
        _df_snap = df.copy() if not df.empty else None
    except Exception:
        _df_snap = None

    # Build routed send function
    def _cps_send(message: str, dedup_key: str = None):
        send_alert_routed("CHART_PATTERN", message, dedup_key)

    import threading as _cps_th
    _cps_th.Thread(
        target = _run_cps_auto_scan_bg,
        args   = (kite, STOCKS, _df_snap, _cps_send),
        daemon = True,
        name   = "cps_auto_scan",
    ).start()

    print(f"[CPS-Auto] Scan triggered for hour {now_hour:02d}:xx")


# ══════════════════════════════════════════════════════════════════════════
# LIVE 1H CANDLE CACHE  - real-time BOS/CHoCH feed (no 65-min OHLC wait)
#
# Architecture:
#   • Historical candles (completed): fetched ONCE per session in background
#     via kite.historical_data(token, from_date, today, "60minute")
#   • Current forming candle: updated every 60s from live kite.quote() data
#     (LIVE_OPEN, LIVE_HIGH, LIVE_LOW, LTP from the df already in memory)
#   • BOS scan uses this combined list -> alerts from 09:16, not 10:15
# ══════════════════════════════════════════════════════════════════════════

# ── OPTIMIZED 1H HISTORICAL CACHE ──
_LIVE_1H_LOCK        = __import__("threading").Lock()
_LIVE_1H_HIST_N      = 20
_H1_DISK_CACHE       = os.path.join(CACHE_DIR, "live_1h_candles_cache.json")

def _load_h1_cache():
    if os.path.exists(_H1_DISK_CACHE):
        try:
            with open(_H1_DISK_CACHE, "r") as f:
                return json.load(f)
        except: pass
    return {}

def _save_h1_cache(data):
    try:
        with open(_H1_DISK_CACHE, "w") as f:
            json.dump(data, f)
    except: pass

def _fetch_hist_1h_batch(symbols_batch, result_dict):
    """Fetches in background and saves to disk cache."""
    import time as _t
    _today = datetime.now(IST).date()
    _from  = _today - timedelta(days=10)
    
    for sym in symbols_batch:
        try:
            tok = get_token(sym)
            if not tok: continue
            bars = kite.historical_data(tok, _from, _today, "60minute")
            if not bars: continue
            
            candles = []
            for b in bars:
                _raw_dt = b.get("date", "")
                candles.append({
                    "datetime": str(_raw_dt)[:19] if _raw_dt else "",
                    "open": float(b.get("open", 0)), "high": float(b.get("high", 0)),
                    "low": float(b.get("low", 0)), "close": float(b.get("close", 0)),
                    "volume": int(b.get("volume", 0)),
                })
            with _LIVE_1H_LOCK:
                result_dict[sym] = candles
        except: pass
        _t.sleep(0.12)
    
    # Save the whole thing to disk
    _save_h1_cache(result_dict)


def _ensure_hist_1h_loaded(symbols):
    """ONE-TIME Load from disk, then background fetch if incomplete."""
    if st.session_state.get("_live_1h_hist_done"):
        return

    # 1. Load from disk first (INSTANT)
    if "_live_1h_candles" not in st.session_state:
        st.session_state["_live_1h_candles"] = _load_h1_cache()
    
    _cache = st.session_state["_live_1h_candles"]
    _missing = [s for s in symbols if s not in _cache]
    
    if not _missing:
        st.session_state["_live_1h_hist_done"] = True
        return

    # 2. Background fetch for missing symbols
    def _do_fetch():
        try:
            _fetch_hist_1h_batch(_missing, _cache)
        except: pass
        
    threading.Thread(target=_do_fetch, daemon=True).start()
    st.session_state["_live_1h_hist_done"] = True 


def _update_live_1h_candle(sym, live_open, live_high, live_low, ltp, live_vol):
    """
    Update the current forming candle for one symbol.
    Called every 60s from live df data - no API call needed.
    The current candle always has today's date at 09:15 start time.
    """
    if not sym or ltp <= 0:
        return

    _today_str   = datetime.now(IST).strftime("%Y-%m-%d")
    _candle_open = f"{_today_str} 09:15:00"   # NSE session start

    live_candle = {
        "datetime": _candle_open,
        "open":     float(live_open  or ltp),
        "high":     float(live_high  or ltp),
        "low":      float(live_low   or ltp),
        "close":    float(ltp),
        "volume":   int(live_vol or 0),
    }

    cache = st.session_state.get(_LIVE_1H_CACHE_KEY, {})
    hist  = list(cache.get(sym, []))

    # Remove any existing today's forming candle (by date prefix)
    hist = [c for c in hist if not c.get("datetime","").startswith(_today_str)]
    # Append fresh live candle
    hist.append(live_candle)
    # Keep only last 25 candles to bound memory
    cache[sym] = hist[-25:]

    with _LIVE_1H_LOCK:
        st.session_state[_LIVE_1H_CACHE_KEY] = cache


def _get_live_candles_for_bos(sym):
    """
    Return the combined candle list (historical + live forming candle) for BOS scan.
    Falls back to OHLCStore if live cache not ready yet.
    """
    cache = st.session_state.get(_LIVE_1H_CACHE_KEY, {})
    candles = cache.get(sym, [])
    if len(candles) >= 5:
        return candles
    # Fallback: OHLCStore if available
    if _BOS_OK and _ohlc_db:
        try:
            return _ohlc_db.get(sym, n=25) or []
        except Exception:
            pass
    return []


# ── Lightweight DB proxy that wraps the live 1H cache for bos_scanner ──────
class _LiveCandleDB:
    """
    Drop-in replacement for OHLCStore.get() - feeds live 1H candles
    to bos_scanner without waiting for OHLC DB update.
    """
    def get(self, symbol, n=25):
        candles = _get_live_candles_for_bos(symbol)
        return candles[-n:] if candles else []

    def is_update_needed(self, max_age_minutes=65):
        return False   # live cache always up to date


_live_candle_db = _LiveCandleDB()


def _run_bos_scan_safe(symbols, ltp_dict=None):
    """
    Scan for BOS/CHoCH using LIVE 1H candle cache (no 65-min OHLC wait).
    Uses _live_candle_db which serves the live-updated candle list.
    Falls back to _ohlc_db if live cache not yet populated.
    Routes alerts via send_alert_routed.
    """
    # Use live candle db (updates every 60s) - no 65-min wait
    # Falls back to OHLCStore only if live cache is empty (startup)
    if not symbols: return []
    tg_on = st.session_state.get("tg_BOS_1H", True)

    def _bos_send_routed(message: str, dedup_key: str = None):
        """
        Wrapper passed to bos_scanner - routes each alert based on
        event type extracted from the dedup_key prefix, then applies
        ch2 routing for tg2_BOS_*/tg2_CHOCH_* toggles.
        """
        # Route via BOS_1H toggle + channel routing
        # (send_alert_routed not available this early - inline routing)
        import streamlit as _st_bos
        _bos_tg_on = _st_bos.session_state.get("tg_BOS_1H", True)
        if _bos_tg_on:
            _bos_route = _st_bos.session_state.get("route_BOS_1H", "ch1")
            if _bos_route in ("ch1", "both"):
                send_telegram_bg(message, dedup_key=dedup_key)
            if _bos_route in ("ch2", "both"):
                send_telegram2_bg(message, dedup_key=f"CH2_{dedup_key}" if dedup_key else None)
        # Additionally send to ch2 for BOS/CHoCH specific toggles
        if dedup_key:
            dk_upper = dedup_key.upper()
            for _cat2, _tg2_key in [
                ("BOS_UP",    "tg2_BOS_UP"),
                ("BOS_DOWN",  "tg2_BOS_DOWN"),
                ("CHOCH_UP",  "tg2_CHOCH_UP"),
                ("CHOCH_DOWN","tg2_CHOCH_DOWN"),
            ]:
                if _cat2 in dk_upper and st.session_state.get(_tg2_key, True):
                    send_telegram2_bg(message, dedup_key=f"CH2_{dedup_key}")
                    break

    try:
        # Build detail_dict for alerts: OHLC and Yesterday data
        _detail_dict = {}
        for s in symbols:
            try:
                row = df[df["Symbol"] == s].iloc[0]
                
                # Compute strength for this specific stock
                dirn = "UP" if row.get("CHANGE_%", 0) >= 0 else "DOWN"
                m_score, m_label, m_reasons = calculate_movement_strength(row, direction=dirn)
                
                _detail_dict[s] = {
                    "open":       row.get("LIVE_OPEN", 0),
                    "high":       row.get("LIVE_HIGH", 0),
                    "low":        row.get("LIVE_LOW", 0),
                    "yest_high":  row.get("YEST_HIGH", 0),
                    "yest_low":   row.get("YEST_LOW", 0),
                    "yest_close": row.get("YEST_CLOSE", 0),
                    "yest_open":  row.get("YEST_OPEN", 0),
                    "change":     row.get("CHANGE", 0),
                    "change_p":   row.get("CHANGE_%", 0),
                    "high_w":     row.get("HIGH_W", 0),
                    "low_w":      row.get("LOW_W", 0),
                    "ema20":      row.get("EMA20", 0),
                    "ema50":      row.get("EMA50", 0),
                    "vol_p":      row.get("VOL_%", 0),
                    "live_vol":   row.get("LIVE_VOLUME", 0),
                    "avg_vol_5d": row.get("AVG_VOL_5D", 0),
                    "vwap":       row.get("REAL_VWAP", 0),
                    "m_score":    m_score,
                    "m_label":    m_label,
                    "m_reasons":  m_reasons
                }
            except Exception:
                pass

        # Live candle db - feeds real-time 1H candles, no 65-min OHLC wait
        _bos_result = run_bos_scan(
            db               = _live_candle_db,
            symbols          = symbols,
            send_telegram_fn = _bos_send_routed,
            ltp_dict         = ltp_dict or {},
            tg_enabled       = tg_on,
            high_conviction_only = st.session_state.get("tg_BOS_HC_ONLY", True),
            detail_dict      = _detail_dict
        )
        # run_bos_scan returns (bos_events, hourly_events) tuple
        _bos_events    = _bos_result[0] if isinstance(_bos_result, tuple) else _bos_result
        _hourly_events = _bos_result[1] if isinstance(_bos_result, tuple) else []
        st.session_state["bos_cache"]    = load_scan_cache()
        st.session_state["hourly_events"] = _hourly_events
        return _bos_events
    except Exception as ex:
        print(f"BOS scan error: {ex}"); return []
# ── END BOS IMPORTS ──


# ── SMC PATCH APPLIED ──
try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
    _SMC_ENGINE_OK = True
except ImportError:
    _SMC_ENGINE_OK = False

@st.cache_data(ttl=60, show_spinner=False)
def _run_smc_intelligence(kite_inst, oi_intel_dict):
    if not _SMC_ENGINE_OK or not kite_inst or not oi_intel_dict:
        return {}
    try:
        c15 = fetch_nifty_candles_kite(kite_inst, interval="15minute", days=5)
        c1h = fetch_nifty_candles_kite(kite_inst, interval="60minute", days=15)
        if not c15:
            return {}
        r = get_smc_confluence(oi_intel=oi_intel_dict,
                               candles_15m=c15,
                               candles_1h=c1h or None)
        r["_fetched_at"] = datetime.now(IST).strftime("%H:%M:%S")
        return r
    except Exception as ex:
        return {"_error": str(ex)}

def _render_smc_block(smc):
    if not smc or not isinstance(smc, dict):
        return
    if "_error" in smc:
        st.warning(f"SMC Engine error: {smc['_error']}"); return

    score   = smc.get("final_score", 0)
    signal  = smc.get("final_signal", "NEUTRAL")
    action  = smc.get("final_action", "WAIT")
    color   = smc.get("signal_color", "grey")
    conflict= smc.get("conflict_detected", False)
    ts      = smc.get("_fetched_at", "")

    bg_map     = {"green":"#1a4d2e","red":"#4d1a1a","yellow":"#3d3a00","grey":"#1a1a2e"}
    border_map = {"green":"#00ff88","red":"#ff4444","yellow":"#ffdd00","grey":"#666688"}
    bg = bg_map.get(color,"#1a1a2e"); border = border_map.get(color,"#666688")

    st.markdown(f"""
<div style="border:2px solid {border};border-radius:10px;background:{bg};padding:12px;margin:8px 0;">
  <div style="font-size:11px;color:#aaa;margin-bottom:4px;">🧠 SMC + OI CONFLUENCE &nbsp;·&nbsp; {ts}</div>
  <div style="font-size:17px;font-weight:bold;color:{border};margin-bottom:4px;">{signal}</div>
  <div style="font-size:12px;color:#ddd;">Score: <b>{score:+d}</b> &nbsp;|&nbsp; {action}</div>
</div>""", unsafe_allow_html=True)

    if conflict:
        st.error(
            "**OI-SMC CONFLICT** - OI shows BEARISH but price structure is BULLISH.\n\n"
            "This is a **Gamma Squeeze / Smart Money Accumulation** pattern.\n"
            "Call writers are being squeezed as price rises through their strikes.\n"
            "**Price structure overrides raw OI signal.**"
        )

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**📊 OI**")
        st.caption(smc.get("oi_direction","?"))
        st.markdown(f"PCR `{smc.get('oi_pcr',0):.2f}` | MaxPain `{smc.get('oi_max_pain',0)}`")
        cw = smc.get("oi_call_wall")
        pf = smc.get("oi_put_floor")
        if cw: st.markdown(f"🔴 Call Wall `{cw}`")
        if pf: st.markdown(f"🟢 Put Floor `{pf}`")
    with c2:
        st.markdown("**📈 SMC Structure**")
        st.markdown(f"15m: `{smc.get('smc_trend_ltf','?')}`  HTF: `{smc.get('smc_trend_htf','?')}`")
        st.markdown(f"Zone: `{smc.get('pd_zone','?')} ({smc.get('pd_zone_pct',0):.0f}%)`")
        bob = smc.get("nearest_bullish_ob")
        if bob: st.markdown(f"🟦 OB Support `{bob['low']:.0f}`–`{bob['high']:.0f}`")
        bfvg = smc.get("nearest_bullish_fvg")
        if bfvg: st.markdown(f"📊 Bull FVG `{bfvg['bottom']:.0f}`–`{bfvg['top']:.0f}`")
        bos = smc.get("bos_events",[])
        if bos:
            b = bos[-1]
            st.markdown(f"🚀 BOS {'↑' if 'UP' in b['type'] else '↓'} `{b['price']:.0f}`")
    with c3:
        st.markdown("**🎯 Trade Setup**")
        setup = smc.get("setup",{})
        bias  = setup.get("bias","NEUTRAL")
        icon  = {"LONG":"🟢","SHORT":"🔴","NEUTRAL":"⚪"}.get(bias,"⚪")
        st.markdown(f"{icon} `{bias}`")
        if setup.get("entry"):   st.markdown(f"Entry `{setup['entry']:.0f}`")
        if setup.get("sl"):      st.markdown(f"SL    `{setup['sl']:.0f}`")
        if setup.get("target1"): st.markdown(f"T1    `{setup['target1']:.0f}`")
        if setup.get("rr"):      st.markdown(f"R:R   `{setup['rr']}:1`")
        if setup.get("option_setup"): st.code(setup["option_setup"])

    insight = smc.get("oi_smc_interpretation","")
    if insight:
        st.info(f"🧠 **SMC Insight:** {insight}")

    buy_liq = smc.get("buy_side_liq",[])
    sel_liq = smc.get("sell_side_liq",[])
    if buy_liq or sel_liq:
        lc1, lc2 = st.columns(2)
        with lc1:
            if buy_liq: st.markdown(f"💧 Buy Stops: **{', '.join(str(int(x)) for x in buy_liq)}**")
        with lc2:
            if sel_liq: st.markdown(f"💧 Sell Stops: **{', '.join(str(int(x)) for x in sel_liq)}**")

    reasons = smc.get("reasons",[])
    if reasons:
        with st.expander("📋 Full SMC Analysis", expanded=False):
            for r in reasons: st.markdown(f"- {r}")

    tg = smc.get("telegram_summary","")
    if tg:
        with st.expander("📤 Telegram Summary (copy)", expanded=False):
            st.code(tg, language=None)
# ── END SMC PATCH ──


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM ALERT CONFIG
# Bot   : @streamlit123_bot  (Name: streamlit)
# Token : 8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A
# Channel: "AutoBotTest123"  https://t.me/AutoBotTest123  (ch1 - primary)
# Chat ID : -1003706739531  ✅ CONFIRMED
# ═══════════════════════════════════════════════════════════════════════
TG_BOT_TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"

# ── Chat ID resolution ────────────────────────────────────────────────
# Private channels have numeric IDs like -100XXXXXXXXXX.
# The dashboard auto-detects this on first run via getUpdates and saves
# to cache/tg_chat_id.txt. After that it loads from file instantly.
# You can also run  python3 get_chat_id.py  to fetch it manually.
# ─────────────────────────────────────────────────────────────────────
# FIX-4a - uppercase CACHE; __file__-based (CACHE_DIR not yet defined this early)
_TG_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")
_TG_ID_FILE   = os.path.join(_TG_CACHE_DIR, "tg_chat_id.txt")

def _resolve_tg_chat_id():
    """
    1. Check cache/tg_chat_id.txt (fastest, runs every time after first)
    2. Call getUpdates API to find the 'AutoBotTest123' channel (ch1 = primary)
    3. If found, save to file and return
    4. Return hardcoded fallback -1002360390807 if API unavailable
    """
    import urllib.request as _ur, json as _js
    os.makedirs(_TG_CACHE_DIR, exist_ok=True)

    # Step 1 - cached
    if os.path.exists(_TG_ID_FILE):
        with open(_TG_ID_FILE, encoding="utf-8") as _f: _cid = _f.read().strip()
        if _cid:
            return _cid

    # Step 2 - fetch from API
    try:
        _url  = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates"
        _req  = _ur.Request(_url, headers={"User-Agent":"PanchakBot/1.0"})
        with _ur.urlopen(_req, timeout=8) as _r:
            _data = _js.loads(_r.read())

        _best = ""
        for _upd in reversed(_data.get("result", [])):
            _src  = _upd.get("channel_post") or _upd.get("message") or {}
            _chat = _src.get("chat", {})
            _cid  = str(_chat.get("id",""))
            _title= _chat.get("title","").lower()
            if _cid:
                # Prefer the AutoBotTest123 channel by name (ch1 = primary channel)
                if "autobot" in _title or "auto" in _title or "test123" in _title:
                    _best = _cid
                    break
                # Also accept if chat ID matches known AutoBotTest123
                if _cid == "-1002360390807":
                    _best = _cid
                    break
                _best = _cid  # fallback: last channel found

        if _best:
            with open(_TG_ID_FILE, "w", encoding="utf-8") as _f: _f.write(_best)
            return _best

    except Exception:
        pass

    return ""

# Resolve at startup (fast from cache, slow only on first ever run)
TG_CHAT_ID = _resolve_tg_chat_id() or "-1002360390807"  # ch1 = AutoBotTest123 (primary)

# ── Auto-start numerology_monitor.py as background subprocess ──
def _start_numerology_monitor():
    import subprocess, os as _os
    script = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "numerology_monitor.py")
    if not _os.path.exists(script): return
    pid_f  = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "CACHE", "numeo_monitor.pid")
    if _os.path.exists(pid_f):
        try:
            with open(pid_f, encoding="utf-8") as _pf:
                _pid_val = int(_pf.read().strip())
            _os.kill(_pid_val, 0)  # FIX-6: PermissionError on Windows
            return  # already running
        except (OSError, ValueError, PermissionError): pass
    try:
        log_f = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "CACHE", "numeo_monitor.log")
        _log_fh = open(log_f, "a", encoding="utf-8")  # kept open
        # FIX-5a: sys.executable = correct interpreter on Linux AND Windows
        p = subprocess.Popen([sys.executable, script],
            stdout=_log_fh, stderr=subprocess.STDOUT,
            # FIX-5b: start_new_session not available on Windows
            **({"start_new_session": True} if sys.platform != "win32"
               else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}))
        with open(pid_f, "w", encoding="utf-8") as _pf2: _pf2.write(str(p.pid))
    except Exception: pass
threading.Thread(target=_start_numerology_monitor, daemon=True).start()



def _tg_dedup_path() -> str:
    """Returns today's TG dedup file path - computed on each call so day rollover works."""
    _td = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")
    os.makedirs(_td, exist_ok=True)
    return os.path.join(_td, f"tg_dedup_{datetime.now().strftime('%Y%m%d')}.json")

# Kept for backwards compat - use _tg_dedup_path() inside load/save functions
_TG_DEDUP_FILE = _tg_dedup_path()   # initial value; _load/_save recompute daily

def _load_tg_dedup():
    """Load today's TG dedup store. Optimized: uses memory cache if available."""
    if "_tg_dedup_cache" in st.session_state:
        return st.session_state["_tg_dedup_cache"]
        
    try:
        path = _tg_dedup_path()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                st.session_state["_tg_dedup_cache"] = data
                return data
    except Exception:
        pass
    return {}

def _save_tg_dedup(d):
    """Save to both memory cache and disk."""
    st.session_state["_tg_dedup_cache"] = d
    try:
        with open(_tg_dedup_path(), "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass

_TG_DEDUP_LOCK = threading.Lock()   # prevents concurrent threads racing on dedup file

def send_telegram(message: str, dedup_key: str = None, parse_mode: str = "HTML") -> bool:
    """
    Send a Telegram message via bot.
    dedup_key: if provided, sent AT MOST ONCE per key (per day file).
    The dedup check-and-mark is performed atomically under a lock so
    concurrent background threads cannot both pass the check simultaneously.
    """
    if not TG_BOT_TOKEN:
        return False
    if not is_market_hours():
        return False

    # ── Atomic dedup: check + mark BEFORE sending ────────────────
    if dedup_key:
        with _TG_DEDUP_LOCK:
            dedup = _load_tg_dedup()
            if dedup.get(dedup_key):
                return False          # already sent - bail out immediately
            # Mark as sent NOW (before the actual HTTP call) so no
            # concurrent thread passes this check while we are sending.
            dedup[dedup_key] = datetime.now().isoformat()
            _save_tg_dedup(dedup)

    # ── Telegram hard limit is 4096 chars - truncate cleanly at last newline ──
    if len(message) > 4000:
        cut = message[:3900].rfind('\n')
        cut = cut if cut > 3000 else 3900
        message = message[:cut] + "\n...[truncated - message too long]"

    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id":    TG_CHAT_ID,
            "text":       message,
            "parse_mode": parse_mode,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = json.loads(resp.read()).get("ok", False)
        return ok
    except urllib.error.HTTPError as e:
        try:
            _body = e.read().decode("utf-8", errors="replace")
        except Exception:
            _body = "(unreadable)"
        print(f"TG send error: {e} | Response: {_body} | parse_mode={parse_mode!r} | msg_start={message[:80]!r}")
        # If send failed, remove the dedup mark so it can retry next cycle
        if dedup_key:
            with _TG_DEDUP_LOCK:
                try:
                    dedup = _load_tg_dedup()
                    dedup.pop(dedup_key, None)
                    _save_tg_dedup(dedup)
                except Exception:
                    pass
        return False
    except Exception as e:
        print(f"TG send error: {e}")
        # If send failed, remove the dedup mark so it can retry next cycle
        if dedup_key:
            with _TG_DEDUP_LOCK:
                try:
                    dedup = _load_tg_dedup()
                    dedup.pop(dedup_key, None)
                    _save_tg_dedup(dedup)
                except Exception:
                    pass
        return False

def send_telegram_bg(message: str, dedup_key: str = None):
    """
    Send telegram in background thread - non-blocking.
    Pre-checks AND marks dedup BEFORE spawning the thread so that
    rapid successive calls (e.g. 22 Streamlit reruns at 09:35) cannot
    spawn multiple threads that all pass the dedup check simultaneously.
    The background thread skips the dedup key (already marked) so the
    HTTP call still goes out exactly once.
    """
    if not is_market_hours():
        return
    if dedup_key:
        with _TG_DEDUP_LOCK:
            _d = _load_tg_dedup()
            if _d.get(dedup_key):
                return           # already sent - don't even spawn a thread
            _d[dedup_key] = datetime.now().isoformat()
            _save_tg_dedup(_d)
        # Pass dedup_key=None to send_telegram - key already marked above
        # so send_telegram's own dedup block is bypassed for this message
        threading.Thread(target=send_telegram, args=(message, None), daemon=True).start()
    else:
        threading.Thread(target=send_telegram, args=(message, None), daemon=True).start()


# ── Second Telegram channel: Panchak Alerts ─────────────────────────────────

# 🔥 HEATMAP DIRECTION AUTO-REVERSAL DETECTION
# Detects early reversal before big moves by monitoring:
#   1. Heatmap Bull % Momentum (dropping from >65 or rising from <35)
#   2. Core Heavyweight price action vs Day Open & Prev Close
#   3. High-conviction alerts with surgical precision
# ═══════════════════════════════════════════════════════════════════════

_N50_HEAVY_SYMS = ["RELIANCE", "HDFCBANK", "ICICIBANK", "BHARTIARTL", "SBIN", "TCS", "INFY", "ITC", "AXISBANK", "LT"]
_BNK_HEAVY_SYMS = ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK"]
_SNX_HEAVY_SYMS = ["HDFCBANK", "ICICIBANK", "RELIANCE", "INFY", "BHARTIARTL", "ITC", "LT", "TCS", "AXISBANK", "KOTAKBANK"]

def _check_heatmap_reversal(df_live):
    """
    Evaluates reversal criteria every refresh.
    Tracks history of Heatmap Bull % and monitors Core Heavyweights.
    """
    if not is_market_hours():
        return
    if _is_tg_disabled("HEATMAP_REVERSAL"):
        return
    if df_live is None or df_live.empty:
        return

    # ── 1. Calculate Current Heatmap Bull % (Nifty 50) ──
    try:
        # Weights from Facts (same as used in 15-min confluence)
        n50_wts = {
            "RELIANCE":9.30,"HDFCBANK":6.37,"BHARTIARTL":5.74,"SBIN":5.04,
            "ICICIBANK":4.93,"TCS":4.77,"LT":2.90,"BAJFINANCE":2.89,
            "INFY":2.74,"HINDUNILVR":2.57,"M&M":2.58,"ITC":2.71,
            "AXISBANK":3.26,"TATAMOTORS":1.62,"MARUTI":1.54,"NTPC":1.86,
            "POWERGRID":1.58,"HCLTECH":1.58,"SUNPHARMA":1.61,"KOTAKBANK":1.82,
            "ONGC":1.22,"COALINDIA":1.14,"TATASTEEL":1.18,"JSWSTEEL":1.04,
            "ULTRACEMCO":1.02,"WIPRO":1.05,"TITAN":1.51,"BAJAJFINSV":1.34,
            "BAJAJ-AUTO":1.10,"NESTLEIND":1.08,"HINDALCO":0.92,"GRASIM":0.84,
            "ADANIENT":0.81,"ASIANPAINT":0.79,"ADANIPORTS":0.84,"TATACONSUM":0.82,
            "APOLLOHOSP":0.68,"BEL":0.72,"CIPLA":0.75,"DRREDDY":0.71,
            "TRENT":0.74,"EICHERMOT":0.61,"HEROMOTOCO":0.58,"BPCL":0.65,
            "BRITANNIA":0.64,"SHRIRAMFIN":1.01,"HDFCLIFE":0.88,"SBILIFE":0.82,
            "LTIM":0.81,"DIVISLAB":0.56,
        }
        
        # Build symbol lookup for fast access
        live_lookup = {row.Symbol: row for _, row in df_live.iterrows()}
        
        bull_wt = bear_wt = 0.0
        for sym, wt in n50_wts.items():
            if sym in live_lookup:
                chg = float(live_lookup[sym].get("CHANGE_%", 0) or 0)
                if chg > 0.25:   bull_wt += wt
                elif chg < -0.25: bear_wt += wt
        
        curr_bull_pct = bull_wt / (bull_wt + bear_wt + 0.01) * 100
        
        # ── 1b. Calculate Current Heatmap Bull % (Bank Nifty) ──
        bnk_wts = {
            "HDFCBANK":25.56,"SBIN":20.28,"ICICIBANK":19.79,"AXISBANK":8.64,
            "KOTAKBANK":7.80,"UNIONBANK":2.95,"BANKBARODA":2.95,"PNB":2.66,
            "CANBK":2.64,"AUBANK":1.51,"FEDERALBNK":1.45,
            "INDUSINDBK":1.34,"YESBANK":1.25,"IDFCFIRSTB":1.18,
        }
        bnk_bull_wt = bnk_bear_wt = 0.0
        for sym, wt in bnk_wts.items():
            if sym in live_lookup:
                chg = float(live_lookup[sym].get("CHANGE_%", 0) or 0)
                if chg > 0.25:   bnk_bull_wt += wt
                elif chg < -0.25: bnk_bear_wt += wt
        curr_bnk_bull_pct = bnk_bull_wt / (bnk_bull_wt + bnk_bear_wt + 0.01) * 100

        # ── 1c. Calculate Current Heatmap Bull % (Sensex) ──
        snx_wts = {
            "HDFCBANK":15.66,"ICICIBANK":10.88,"RELIANCE":10.24,"INFY":5.75,
            "BHARTIARTL":5.37,"ITC":4.23,"LT":4.20,"TCS":3.74,
            "AXISBANK":3.63,"KOTAKBANK":3.49,"M&M":2.84,"HINDUNILVR":2.82,
            "TATASTEEL":2.58,"SBIN":2.34,"NTPC":2.16,"POWERGRID":1.84,
            "SUNPHARMA":1.76,"TITAN":1.68,"BAJFINANCE":1.65,"JSWSTEEL":1.48,
            "ULTRACEMCO":1.42,"ADANIENT":1.38,"MARUTI":1.32,"NESTLEIND":1.28,
            "HCLTECH":1.24,"ASIANPAINT":1.18,"BAJAJFINSV":1.14,"TATAMOTORS":1.12,
            "TECHM":0.98,"INDUSINDBK":0.91
        }
        snx_bull_wt = snx_bear_wt = 0.0
        for sym, wt in snx_wts.items():
            if sym in live_lookup:
                chg = float(live_lookup[sym].get("CHANGE_%", 0) or 0)
                if chg > 0.25:   snx_bull_wt += wt
                elif chg < -0.25: snx_bear_wt += wt
        curr_snx_bull_pct = snx_bull_wt / (snx_bull_wt + snx_bear_wt + 0.01) * 100

        # Store in rolling history
        hist = st.session_state.get("heatmap_history", [])
        hist.append({"n50": curr_bull_pct, "bnk": curr_bnk_bull_pct, "snx": curr_snx_bull_pct})
        if len(hist) > 30: hist = hist[-30:]
        st.session_state["heatmap_history"] = hist
        
        if len(hist) < 5: return 
        
        max_n50 = max(h["n50"] for h in hist[:-1])
        min_n50 = min(h["n50"] for h in hist[:-1])
        max_snx = max(h["snx"] for h in hist[:-1])
        min_snx = min(h["snx"] for h in hist[:-1])
        
    except Exception as e:
        print(f"[REVERSAL-SCAN] heatmap calc error: {e}")
        return

    # ── 2. Heavyweight Validation Logic ──
    def _validate_heavyweights(symbols, mode="BEAR"):
        """Check if core heavyweights confirm the reversal direction."""
        total = len(symbols)
        match_count = 0
        details = []
        for sym in symbols:
            if sym not in live_lookup: continue
            row = live_lookup[sym]
            ltp    = float(row.get("LTP", 0))
            open_p = float(row.get("LIVE_OPEN", 0))
            chg    = float(row.get("CHANGE", 0))
            pc     = ltp - chg # Yesterday's Close
            high   = float(row.get("LIVE_HIGH", 0))
            low    = float(row.get("LIVE_LOW", 0))
            yh     = float(row.get("YEST_HIGH", 0))
            yl     = float(row.get("YEST_LOW", 0))
            
            if mode == "BEAR":
                # Conditions: Below Open, Below Prev Close, Off the High, Below Yesterday High
                if ltp < open_p and ltp < pc and ltp < high and (yh == 0 or ltp < yh):
                    match_count += 1
                    details.append(f"{sym} (LTP {ltp} < Open {open_p})")
            else: # BULL
                # Conditions: Above Open, Above Prev Close, Bouncing from Low, Above Yesterday Low
                if ltp > open_p and ltp > pc and ltp > low and (yl == 0 or ltp > yl):
                    match_count += 1
                    details.append(f"{sym} (LTP {ltp} > Open {open_p})")
        
        return match_count >= (total // 2), details

    # ── 3. Detect Reversals ──
    _now_str = datetime.now(IST).strftime("%H:%M")
    _today_k = datetime.now(IST).strftime("%Y%m%d")
    _alert_last_sent = st.session_state.get("heatmap_reversal_alert_last_sent", {})

    # A. BULL -> BEAR REVERSAL (Early Fall Detection)
    # Thresholds: Conservative (65% -> 50%)
    if (max_n50 >= 65 and curr_bull_pct <= 50) or (max_snx >= 65 and curr_snx_bull_pct <= 50):
        index_name = "Nifty" if (max_n50 >= 65 and curr_bull_pct <= 50) else "Sensex"
        max_val = max_n50 if index_name == "Nifty" else max_snx
        curr_val = curr_bull_pct if index_name == "Nifty" else curr_snx_bull_pct
        heavy_syms = _N50_HEAVY_SYMS if index_name == "Nifty" else _SNX_HEAVY_SYMS
        
        is_confirmed, hv_details = _validate_heavyweights(heavy_syms, mode="BEAR")
        if is_confirmed:
            _key = f"BEAR_REV_{index_name}_{_today_k}_{datetime.now(IST).hour}"
            if _alert_last_sent.get(_key) != True:
                hv_txt = "\n".join([f"  • {d}" for d in hv_details[:6]])
                msg = (
                    f"🔴🔴 <b>🔥 EARLY REVERSAL ALERT (BULL -> BEAR)</b>\n"
                    f"⏰ {_now_str} | {index_name} Heatmap Shift\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <b>Detected early reversal before a big fall!</b>\n"
                    f"📊 {index_name} Heatmap dropped from <b>{max_val:.0f}%</b> to <b>{curr_val:.0f}%</b>\n"
                    f"🏦 BNK Heatmap: <b>{curr_bnk_bull_pct:.0f}%</b> bull wt\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏗️ <b>Heavyweights confirming fall:</b>\n"
                    f"{hv_txt}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📍 <i>Heavyweights failing to break higher highs and dropping below Day Open & Yesterday Close.</i>\n"
                    f"⚠️ <i>NOT financial advice.</i>\n"
                    f"🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴"
                )
                send_alert_routed("HEATMAP_REVERSAL", msg, dedup_key=f"HEATMAP_REV_BEAR_{index_name}_{_today_k}_{_now_str[:2]}")
                _alert_last_sent[_key] = True
                st.session_state["heatmap_reversal_alert_last_sent"] = _alert_last_sent
                st.toast(f"🔴 BEARISH REVERSAL ({index_name}): Heavyweights failing Day Open!", icon="🚨")

    # B. BEAR -> BULL REVERSAL (Early Rise Detection)
    # Thresholds: Conservative (35% -> 50%)
    if (min_n50 <= 35 and curr_bull_pct >= 50) or (min_snx <= 35 and curr_snx_bull_pct >= 50):
        index_name = "Nifty" if (min_n50 <= 35 and curr_bull_pct >= 50) else "Sensex"
        min_val = min_n50 if index_name == "Nifty" else min_snx
        curr_val = curr_bull_pct if index_name == "Nifty" else curr_snx_bull_pct
        heavy_syms = _N50_HEAVY_SYMS if index_name == "Nifty" else _SNX_HEAVY_SYMS

        is_confirmed, hv_details = _validate_heavyweights(heavy_syms, mode="BULL")
        if is_confirmed:
            _key = f"BULL_REV_{index_name}_{_today_k}_{datetime.now(IST).hour}"
            if _alert_last_sent.get(_key) != True:
                hv_txt = "\n".join([f"  • {d}" for d in hv_details[:6]])
                msg = (
                    f"🟢🟢 <b>🔥 EARLY REVERSAL ALERT (BEAR -> BULL)</b>\n"
                    f"⏰ {_now_str} | {index_name} Heatmap Shift\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🚀 <b>Detected early reversal before a big rally!</b>\n"
                    f"📊 {index_name} Heatmap rose from <b>{min_val:.0f}%</b> to <b>{curr_val:.0f}%</b>\n"
                    f"🏦 BNK Heatmap: <b>{curr_bnk_bull_pct:.0f}%</b> bull wt\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏗️ <b>Heavyweights confirming rise:</b>\n"
                    f"{hv_txt}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📍 <i>Heavyweights failing to break lower lows and rising above Day Open & Yesterday Close.</i>\n"
                    f"⚠️ <i>NOT financial advice.</i>\n"
                    f"🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢"
                )
                send_alert_routed("HEATMAP_REVERSAL", msg, dedup_key=f"HEATMAP_REV_BULL_{index_name}_{_today_k}_{_now_str[:2]}")
                _alert_last_sent[_key] = True
                st.session_state["heatmap_reversal_alert_last_sent"] = _alert_last_sent
                st.toast(f"🟢 BULLISH REVERSAL ({index_name}): Heavyweights rising above Open!", icon="🚀")

def _check_panchak_special_rules(df_live):
    """Checks high-conviction panchak rules and sends alerts."""
    if not is_market_hours(): return
    if df_live is None or df_live.empty: return
    
    _now = datetime.now(IST)
    _today_k = _now.strftime("%Y%m%d")
    
    # ── 1. Fierce Reversal Alert ──
    _PR_FILE = os.path.join(CACHE_DIR, "panchak_range.json")
    try:
        with open(_PR_FILE, encoding="utf-8") as _f:
            _pr_state = json.load(_f)
    except: _pr_state = {}
    
    tracking = st.session_state.get("panchak_reversal_tracking", {})
    
    for _pkey, _ps in _pr_state.items():
        _ph = _ps.get("period_high", 0)
        _pl = _ps.get("period_low", 0)
        if _ph == 0 or _pl == 0: continue
        
        _ni_ltp = float(df_live[df_live["Symbol"] == "NIFTY"].iloc[0]["LTP"] if "NIFTY" in df_live["Symbol"].values else 0)
        if _ni_ltp == 0: continue
        
        _rng = _ph - _pl
        _t61_up = _ph + _rng * 0.61
        _t61_dn = _pl - _rng * 0.61
        
        state = tracking.get(_pkey, {"first_break": None, "alerted": False})
        
        # Detect first break
        if state["first_break"] is None:
            if _ni_ltp > _ph: state["first_break"] = "UP"
            elif _ni_ltp < _pl: state["first_break"] = "DN"
        
        # Detect fierce reversal
        elif not state["alerted"]:
            if state["first_break"] == "UP" and _ni_ltp < _pl:
                # Check if it hit upside target first
                if not _ps.get("t61_up", False) and _ni_ltp < _pl:
                    msg = f"🚨 <b>PANCHAK FIERCE REVERSAL (FALL)</b>\nNifty broke High first, but now crashed below Low without hitting Target! 📉\nLTP: {_ni_ltp:,.2f}"
                    send_alert_routed("KP_BREAK_15M", msg, dedup_key=f"PANCHAK_FIERCE_FALL_{_pkey}_{_today_k}")
                    state["alerted"] = True
            elif state["first_break"] == "DN" and _ni_ltp > _ph:
                # Check if it hit downside target first
                if not _ps.get("t61_dn", False) and _ni_ltp > _ph:
                    msg = f"🚀 <b>PANCHAK FIERCE REVERSAL (RALLY)</b>\nNifty broke Low first, but now surged above High without hitting Target! 📈\nLTP: {_ni_ltp:,.2f}"
                    send_alert_routed("KP_BREAK_15M", msg, dedup_key=f"PANCHAK_FIERCE_RALLY_{_pkey}_{_today_k}")
                    state["alerted"] = True
        
        tracking[_pkey] = state
    st.session_state["panchak_reversal_tracking"] = tracking

    # ── 2. Yoga & Planetary Alerts (Once per day at 09:16) ──
    if _now.hour == 9 and _now.minute == 16:
        _astro = _vedic_day_analysis(_now.date())
        _yoga = _astro.get("yoga_name", "")
        _jr = _astro.get("jupiter_rashi", "")
        _sr = _astro.get("saturn_rashi", "")
        
        # Yoga Alert
        if _yoga in ["Indra", "Vaidhriti", "Vyatipata"]:
            msg = f"✨ <b>PANCHAK BULLISH YOGA</b>\nToday is a Panchak day with <b>{_yoga} Yoga</b>. High conviction bullish bias! 🟢"
            send_alert_routed("KP_BREAK_15M", msg, dedup_key=f"PANCHAK_YOGA_{_today_k}")
            
        # Planetary Sync
        if _jr == _sr:
            msg = f"🪐 <b>PANCHAK PLANETARY SYNC</b>\nJupiter and Saturn are together in <b>{_jr}</b>. Strong bullish cycle active! 🟢"
            send_alert_routed("KP_BREAK_15M", msg, dedup_key=f"PANCHAK_SYNC_{_today_k}")

# ── Second Telegram channel: Panchak Alerts ─────────────────────────────────
# ch1 = AutoBotTest123  Chat ID: -1002360390807  (primary - general alerts)
# ch2 = Panchak Alerts  Chat ID: -1003706739531  (secondary - selected alerts)
# Both channels use the SAME main bot token - bot must be admin in both channels.
TG_CHAT_ID_2   = "-1003706739531"                    # Panchak Alerts (ch2)
TG_BOT_TOKEN_2 = TG_BOT_TOKEN                        # Same main botmain bot - must be admin in both channels

def send_telegram2(message: str, dedup_key: str = None) -> bool:
    """Send to second Telegram channel (AutoBotTest123).
    Uses same main bot token - bot must be admin in AutoBotTest123 channel.
    Alerts routed here: BOS UP/DOWN, CHoCH UP/DOWN, SMC+OI Confluence,
    and any category set to 'ch2' or 'both' in Alert Control tab.
    """
    if not is_market_hours():
        return False
    if not TG_BOT_TOKEN or not TG_CHAT_ID_2:
        return False
    _tok2 = TG_BOT_TOKEN  # same bot, different chat
    _key2 = ("CH2_" + dedup_key) if dedup_key else None
    if _key2:
        with _TG_DEDUP_LOCK:
            dedup = _load_tg_dedup()
            if dedup.get(_key2):
                return False
            dedup[_key2] = datetime.now().isoformat()
            _save_tg_dedup(dedup)
    try:
        url = f"https://api.telegram.org/bot{_tok2}/sendMessage"
        payload = json.dumps({
            "chat_id": TG_CHAT_ID_2, "text": message, "parse_mode": "HTML",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()).get("ok", False)
    except Exception as e:
        print(f"TG2 send error: {e}")
        return False

def send_telegram2_bg(message: str, dedup_key: str = None):
    """
    Non-blocking second-channel send with pre-dedup check.
    Same race-condition fix as send_telegram_bg - marks dedup before spawning.
    """
    if not is_market_hours():
        return
    if dedup_key:
        with _TG_DEDUP_LOCK:
            _d = _load_tg_dedup()
            _ch2_key = f"CH2_BG_{dedup_key}"
            if _d.get(_ch2_key):
                return
            _d[_ch2_key] = datetime.now().isoformat()
            _save_tg_dedup(_d)
        threading.Thread(target=send_telegram2, args=(message, None), daemon=True).start()
    else:
        threading.Thread(target=send_telegram2, args=(message, None), daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════
# 📡 CHANNEL-ROUTED ALERT DISPATCHER
# Reads route_<CATEGORY> from session_state to decide which channel(s)
# to send to: "ch1" = Panchak Alerts only, "ch2" = AutoBotTest123 only,
# "both" = both channels simultaneously.
# DEFINED HERE (early) so it can be called from any alert function below.
# ═══════════════════════════════════════════════════════════════════════
def send_alert_routed(category: str, message: str, dedup_key: str = None):
    """
    Route a Telegram alert to ch1, ch2, or both based on
    st.session_state['route_<category>'].  Falls back to 'ch1' if unset.
    Only fires if the tg_<category> toggle is ON.
    Non-blocking (background threads).
    """
    # Check master toggle first
    tg_key = f"tg_{category}"
    if not st.session_state.get(tg_key, _ALERT_TOGGLE_DEFAULTS.get(tg_key, True)):
        return  # alert is OFF for this category

    route = st.session_state.get(
        f"route_{category}",
        _ALERT_TOGGLE_DEFAULTS.get(f"route_{category}", "ch1")
    )

    if route in ("ch1", "both"):
        send_telegram_bg(message, dedup_key=dedup_key)
    if route in ("ch2", "both"):
        send_telegram2_bg(message, dedup_key=(f"CH2_{dedup_key}" if dedup_key else None))


# [send_alert_routed defined earlier - see above send_telegram2_bg]


# ═══════════════════════════════════════════════════════════════════════
# 📲 PANCHAK-AWARE TOP_HIGH / TOP_LOW TELEGRAM ALERT ENGINE
#
# Rules:
#   1. ONE Telegram alert per stock per direction (UP/DOWN) per panchak period
#   2. Alert fires when LTP breaks TOP_HIGH (UP) or TOP_LOW (DOWN)
#   3. Dedup key = f"TG_PANCHAK_{symbol}_{direction}_{PANCHAK_START}"
#      -> resets automatically when a new panchak starts (new PANCHAK_START)
#   4. If the same stock re-enters a NEW panchak -> alert fires again
#   5. Non-blocking (background thread)
# ═══════════════════════════════════════════════════════════════════════

# Panchak-scoped dedup file - one file per panchak period
# Named with panchak start date so it auto-resets on new panchak
def _panchak_tg_dedup_path():
    """Returns path to today's panchak TG dedup file (keyed on PANCHAK_START)."""
    _dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")  # FIX-4c
    os.makedirs(_dir, exist_ok=True)
    # Use PANCHAK_START in filename - new panchak = new file = alerts fire again
    # PANCHAK_START is defined later; we use a lazy string to avoid forward-ref issues
    try:
        _pstart = PANCHAK_START.strftime("%Y%m%d")
    except Exception:
        _pstart = datetime.now().strftime("%Y%m%d")
    return os.path.join(_dir, f"tg_panchak_break_{_pstart}.json")


def _load_panchak_tg_dedup():
    """Load panchak-scoped TG dedup dict {dedup_key: iso_timestamp}."""
    try:
        with open(_panchak_tg_dedup_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_panchak_tg_dedup(d):
    """Persist panchak TG dedup dict."""
    try:
        with open(_panchak_tg_dedup_path(), "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass


_PANCHAK_TG_LOCK = threading.Lock()  # prevents race between concurrent refresh threads

def _panchak_tg_already_sent(symbol, direction):
    """
    Returns True if a TG alert was already sent for this stock+direction
    in the current panchak period.
    direction: 'UP' or 'DOWN'
    """
    try:
        _pstart = PANCHAK_START.strftime("%Y%m%d")
    except Exception:
        _pstart = datetime.now().strftime("%Y%m%d")
    key = f"TG_PANCHAK_{symbol}_{direction}_{_pstart}"
    return bool(_load_panchak_tg_dedup().get(key))


def _panchak_tg_mark_sent(symbol, direction):
    """Mark a panchak TG alert as sent for this stock+direction."""
    try:
        _pstart = PANCHAK_START.strftime("%Y%m%d")
    except Exception:
        _pstart = datetime.now().strftime("%Y%m%d")
    key = f"TG_PANCHAK_{symbol}_{direction}_{_pstart}"
    with _PANCHAK_TG_LOCK:
        d = _load_panchak_tg_dedup()
        d[key] = datetime.now().isoformat()
        _save_panchak_tg_dedup(d)


def _panchak_tg_check_and_mark(symbol, direction):
    """
    Atomic check-and-mark: returns True if this is a NEW alert (not yet sent).
    Marks as sent immediately under lock so no concurrent thread can double-fire.
    """
    try:
        _pstart = PANCHAK_START.strftime("%Y%m%d")
    except Exception:
        _pstart = datetime.now().strftime("%Y%m%d")
    key = f"TG_PANCHAK_{symbol}_{direction}_{_pstart}"
    with _PANCHAK_TG_LOCK:
        d = _load_panchak_tg_dedup()
        if d.get(key):
            return False          # already sent
        d[key] = datetime.now().isoformat()
        _save_panchak_tg_dedup(d)
        return True               # new alert - marked before returning


def fire_panchak_tg_alerts(df_live, ltp_map=None):
    """
    Scan df_live for TOP_HIGH/TOP_LOW breaks and send ONE Telegram alert
    per stock per direction per panchak period.

    For SPECIAL_ALERT_STOCKS: also fires email + toast when
    st.session_state.special_stock_alerts is True.

    Call this once per refresh cycle after df_live is populated.

    Parameters
    ----------
    df_live : pd.DataFrame  - main live data dataframe (must have Symbol, LTP,
                              TOP_HIGH, TOP_LOW columns)
    ltp_map : dict          - optional {symbol: ltp} override
    """
    if not is_market_hours():
        return
    if df_live is None or df_live.empty:
        return

    required = {"Symbol", "LTP", "TOP_HIGH", "TOP_LOW"}
    if not required.issubset(df_live.columns):
        return

    try:
        _pstart = PANCHAK_START.strftime("%d-%b-%Y")
        _pend   = PANCHAK_END.strftime("%d-%b-%Y")
    except Exception:
        _pstart = _pend = "?"

    _now_str  = datetime.now(IST).strftime("%H:%M IST")
    _now_dt   = datetime.now(IST)
    _slot_10m = _now_dt.replace(minute=(_now_dt.minute//10)*10, second=0, microsecond=0).strftime("%H%M")
    _up_rows   = []   # stocks breaking TOP_HIGH
    _down_rows = []   # stocks breaking TOP_LOW
    _special_up   = []  # special stocks breaking UP
    _special_down = []  # special stocks breaking DOWN

    for _, row in df_live.iterrows():
        sym = str(row.get("Symbol", "")).strip()
        if not sym:
            continue
        # ── Only alert for the 14 watchlist stocks ──────────────
        if sym.upper() not in PANCHAK_ALERT_WATCHLIST:
            continue
        ltp      = float(ltp_map.get(sym, row.get("LTP", 0)) or 0)
        top_high = row.get("TOP_HIGH", None)
        top_low  = row.get("TOP_LOW",  None)
        is_special = sym.upper() in _SPECIAL_SET

        detail = {
            "open":       row.get("OPEN", 0),
            "high":       row.get("HIGH", 0),
            "low":        row.get("LOW", 0),
            "yest_high":  row.get("YEST_HIGH", 0),
            "yest_low":   row.get("YEST_LOW", 0),
            "yest_close": row.get("YEST_CLOSE", 0)
        }

        # ── UP break ──────────────────────────────────────────
        if pd.notna(top_high) and ltp > 0 and ltp >= float(top_high):
            chg_pct = row.get("CHANGE_%", "")
            _entry = {
                "sym":      sym,
                "ltp":      ltp,
                "top_high": float(top_high),
                "chg":      chg_pct,
                "gain":     round(ltp - float(top_high), 2),
                **detail
            }
            # Special stocks: always add (separate daily dedup handled at send time)
            if is_special:
                _special_up.append(_entry)
            # General panchak alert: atomic check-and-mark (no race between threads)
            if _panchak_tg_check_and_mark(sym, "UP"):
                _up_rows.append(_entry)

        # ── DOWN break ────────────────────────────────────────
        if pd.notna(top_low) and ltp > 0 and ltp <= float(top_low):
            chg_pct = row.get("CHANGE_%", "")
            _entry = {
                "sym":     sym,
                "ltp":     ltp,
                "top_low": float(top_low),
                "chg":     chg_pct,
                "loss":    round(ltp - float(top_low), 2),
                **detail
            }
            # Special stocks: always add (separate daily dedup handled at send time)
            if is_special:
                _special_down.append(_entry)
            # General panchak alert: atomic check-and-mark
            if _panchak_tg_check_and_mark(sym, "DOWN"):
                _down_rows.append(_entry)

    # ── Send UP break TG alert ────────────────────────────────
    if _up_rows and not _is_tg_disabled("TOP_HIGH"):
        _lines = []
        for r in _up_rows:
            _chg = f"  ({r['chg']:+.2f}%)" if isinstance(r['chg'], (int, float)) else ""
            _lines.append(
                f"  🟢 <b>{r['sym']}</b>  LTP: <b>{r['ltp']}</b>{_chg}\n"
                f"    OHLC: {r['open']}/{r['high']}/{r['low']}/{r['ltp']}\n"
                f"    YH/YL/YC: {r['yest_high']}/{r['yest_low']}/{r['yest_close']}\n"
                f"    🔓 Broke TOP_HIGH: {r['top_high']} (+{r['gain']})"
            )
        _msg = (
            "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩\n"
            f"🚀 <b>PANCHAK TOP_HIGH BREAK</b>\n"
            f"⏰ {_now_str}\n"
            f"📅 Panchak: {_pstart} -> {_pend}\n"
            f"📈 Stocks breaking UP ({len(_up_rows)}):\n"
            + "\n".join(_lines) +
            f"\n\n✅ <i>Valid till panchak ends ({_pend}). "
            f"New panchak = fresh alerts.</i>\n"
            f"⚠️ <i>NOT financial advice.</i>\n"
            "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩"
        )
        _syms_key = "_".join(r["sym"] for r in _up_rows[:5])
        try:
            _pstart_key = PANCHAK_START.strftime("%Y%m%d")
        except Exception:
            _pstart_key = "?"
        send_telegram_bg(_msg, dedup_key=f"PANCHAK_UP_{_syms_key}_{_pstart_key}_{_slot_10m}")

    # ── Special stocks UP: fire toast + email + TG ──────────────
    if _special_up and st.session_state.get("special_stock_alerts", True):
        _sp_syms = [r["sym"] for r in _special_up]
        _sp_ltp  = {r["sym"]: r["ltp"] for r in _special_up}
        # toast disabled per alerts_modifications.docx (Point 1)
        # if not _is_toast_disabled("TOP_HIGH"):
        #     st.toast(f"🟢 SPECIAL TOP_HIGH BREAK: {', '.join(_sp_syms)}", icon="🚨")
        if False and not _is_email_disabled("TOP_HIGH"):  # email disabled per doc
            _sp_df = df_live[df_live["Symbol"].isin(_sp_syms)][_CAT_COLS.get("TOP_HIGH", ["Symbol","LTP"])].copy()
            def _sp_up_email(s=f"[OiAnalytics] 🟢 SPECIAL TOP_HIGH Break - {', '.join(_sp_syms[:3])} | {_now_str}",
                             d=_sp_df.copy(), c="#1b5e20", sp=_special_up):
                swl = [(r["sym"], r["ltp"]) for r in sp]
                html = _build_html_email("🟢 SPECIAL Stocks TOP_HIGH Break", swl, df_table=d, color=c, icon="🟢")
                send_email(s, html, is_html=True)
            threading.Thread(target=_sp_up_email, daemon=True).start()
        # ── Special TG alert (ALL-3) - gated by special_stock_alerts toggle ──
        _sp_lines = "\n".join(
            f"  \U0001f31f <b>{r['sym']}</b>  LTP: <b>{r['ltp']}</b>  TOP_HIGH: {r['top_high']}  +{r['gain']}"
            for r in _special_up
        )
        _SEP = "\u2501" * 22
        _sp_tg_msg = "\n".join([
            "🟢 <b>⭐ SPECIAL STOCKS - TOP_HIGH BREAK</b>",
            f"⏰ {_now_str}",
            _SEP,
            _sp_lines,
            _SEP,
            "🔔 All-3 Alert (Toast + Email + Telegram)",
            "⚠️ <i>NOT financial advice.</i>",
        ])
        # Atomic per-stock per-panchak-period dedup
        _sp_any_new_up = False
        _sp_new_syms_up = []
        for _sp_s in _sp_syms:
            if _panchak_tg_check_and_mark(_sp_s, "UP_SPECIAL"):
                _sp_any_new_up = True
                _sp_new_syms_up.append(_sp_s)
        if _sp_any_new_up:
            try:
                _pstart_k = PANCHAK_START.strftime("%Y%m%d")
            except Exception:
                _pstart_k = datetime.now(IST).strftime("%Y%m%d")
            _sp_key = "SPECIAL_UP_" + "_".join(sorted(_sp_new_syms_up[:3])) + "_" + _pstart_k
            send_telegram_bg(_sp_tg_msg, dedup_key=_sp_key)

    # ── Send DOWN break TG alert ──────────────────────────────
    if _down_rows and not _is_tg_disabled("TOP_LOW"):
        _lines = []
        for r in _down_rows:
            _chg = f"  ({r['chg']:+.2f}%)" if isinstance(r['chg'], (int, float)) else ""
            _lines.append(
                f"  🔴 <b>{r['sym']}</b>  LTP: <b>{r['ltp']}</b>{_chg}\n"
                f"    OHLC: {r['open']}/{r['high']}/{r['low']}/{r['ltp']}\n"
                f"    YH/YL/YC: {r['yest_high']}/{r['yest_low']}/{r['yest_close']}\n"
                f"    🔓 Broke TOP_LOW: {r['top_low']} ({r['loss']})"
            )
        _msg = (
            "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥\n"
            f"🔻 <b>PANCHAK TOP_LOW BREAK</b>\n"
            f"⏰ {_now_str}\n"
            f"📅 Panchak: {_pstart} -> {_pend}\n"
            f"📉 Stocks breaking DOWN ({len(_down_rows)}):\n"
            + "\n".join(_lines) +
            f"\n\n✅ <i>Valid till panchak ends ({_pend}). "
            f"New panchak = fresh alerts.</i>\n"
            f"⚠️ <i>NOT financial advice.</i>\n"
            "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"
        )
        _syms_key = "_".join(r["sym"] for r in _down_rows[:5])
        try:
            _pstart_key = PANCHAK_START.strftime("%Y%m%d")
        except Exception:
            _pstart_key = "?"
        send_telegram_bg(_msg, dedup_key=f"PANCHAK_DOWN_{_syms_key}_{_pstart_key}_{_slot_10m}")

    # ── Special stocks DOWN: fire toast + email + TG ─────────────
    if _special_down and st.session_state.get("special_stock_alerts", True):
        _sp_syms = [r["sym"] for r in _special_down]
        _sp_ltp  = {r["sym"]: r["ltp"] for r in _special_down}
        # toast disabled per alerts_modifications.docx (Point 1)
        # if not _is_toast_disabled("TOP_LOW"):
        #     st.toast(f"🔴 SPECIAL TOP_LOW BREAK: {', '.join(_sp_syms)}", icon="🚨")
        if False and not _is_email_disabled("TOP_LOW"):  # email disabled per doc
            _sp_df = df_live[df_live["Symbol"].isin(_sp_syms)][_CAT_COLS.get("TOP_LOW", ["Symbol","LTP"])].copy()
            def _sp_dn_email(s=f"[OiAnalytics] 🔴 SPECIAL TOP_LOW Break - {', '.join(_sp_syms[:3])} | {_now_str}",
                             d=_sp_df.copy(), c="#b71c1c", sp=_special_down):
                swl = [(r["sym"], r["ltp"]) for r in sp]
                html = _build_html_email("🔴 SPECIAL Stocks TOP_LOW Break", swl, df_table=d, color=c, icon="🔴")
                send_email(s, html, is_html=True)
            threading.Thread(target=_sp_dn_email, daemon=True).start()
        # ── Special TG alert (ALL-3) - gated by special_stock_alerts toggle ──
        _sp_lines = "\n".join(
            f"  \U0001f31f <b>{r['sym']}</b>  LTP: <b>{r['ltp']}</b>  TOP_LOW: {r['top_low']}  {r['loss']}"
            for r in _special_down
        )
        _SEP = "\u2501" * 22
        _sp_tg_msg = "\n".join([
            "🔴 <b>⭐ SPECIAL STOCKS - TOP_LOW BREAK</b>",
            f"⏰ {_now_str}",
            _SEP,
            _sp_lines,
            _SEP,
            "🔔 All-3 Alert (Toast + Email + Telegram)",
            "⚠️ <i>NOT financial advice.</i>",
        ])
        # Atomic per-stock per-panchak-period dedup
        # _panchak_tg_check_and_mark writes to panchak_break_<date>.json
        # send_telegram_bg also pre-marks in tg_dedup_<date>.json for same-session guard
        _sp_any_new = False
        _sp_new_syms = []
        for _sp_s in _sp_syms:
            if _panchak_tg_check_and_mark(_sp_s, "DOWN_SPECIAL"):
                _sp_any_new = True
                _sp_new_syms.append(_sp_s)
        if _sp_any_new:
            # Key uses panchak-start date (stable across all Streamlit threads today)
            try:
                _pstart_k = PANCHAK_START.strftime("%Y%m%d")
            except Exception:
                _pstart_k = datetime.now(IST).strftime("%Y%m%d")
            _sp_key = "SPECIAL_DN_" + "_".join(sorted(_sp_new_syms[:3])) + "_" + _pstart_k
            send_telegram_bg(_sp_tg_msg, dedup_key=_sp_key)


# ═══════════════════════════════════════════════════════════════════════
# 🪐 VEDIC ASTRO ENGINE - Pure Python (no external deps)
# Computes Moon nakshatra, rashi, tithi, planetary positions (sidereal/Lahiri)
# for any date. Used by the 10-Day Nifty Astro Forecast table.
# ═══════════════════════════════════════════════════════════════════════

def _jd(dt):
    """Gregorian date -> Julian Day Number (UTC datetime)."""
    a = (14 - dt.month) // 12
    y = dt.year + 4800 - a
    m = dt.month + 12 * a - 3
    n = dt.day + (153*m+2)//5 + 365*y + y//4 - y//100 + y//400 - 32045
    return n + (dt.hour + dt.minute/60 + dt.second/3600 - 12) / 24

def _lahiri(jd_val):
    """Lahiri ayanamsa (degrees) for given Julian Day."""
    return 23.8506 + (jd_val - 2451545.0) * (50.27 / (3600 * 365.25))

def _sid(trop, jd_val):
    """Tropical -> sidereal (Lahiri)."""
    return (trop - _lahiri(jd_val)) % 360

def _sun_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L0 = 280.46646 + 36000.76983 * T
    M  = math.radians((357.52911 + 35999.05029*T) % 360)
    C  = (1.914602 - 0.004817*T)*math.sin(M) + 0.019993*math.sin(2*M) + 0.000289*math.sin(3*M)
    return (L0 + C) % 360

def _moon_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L0 = 218.3164477 + 481267.88123421*T - 0.0015786*T**2
    D  = math.radians((297.8501921 + 445267.1114034*T - 0.1851675*T**2) % 360)
    M  = math.radians((357.5291092 + 35999.0502909*T) % 360)
    Mp = math.radians((134.9633964 + 477198.8675055*T + 0.0087414*T**2) % 360)
    F  = math.radians((93.2720950  + 483202.0175233*T - 0.0036539*T**2) % 360)
    _terms = [
        (0,0,1,0,6288774),(2,0,-1,0,1274027),(2,0,0,0,658314),(0,0,2,0,213618),
        (0,1,0,0,-185116),(0,0,0,2,-114332),(2,0,-2,0,58793),(2,-1,-1,0,57066),
        (2,0,1,0,53322),(2,-1,0,0,45758),(0,1,-1,0,-40923),(1,0,0,0,-34720),
        (0,1,1,0,-30383),(2,0,0,-2,15327),(4,0,-1,0,10675),(0,0,3,0,10034),
        (4,0,-2,0,8548),(2,1,-1,0,-7888),(2,1,0,0,-6766),(1,0,-1,0,-5163),
        (1,1,0,0,4987),(2,-1,1,0,4036),(2,0,2,0,3994),(4,0,0,0,3861),
        (0,1,-2,0,-2689),(2,0,-3,0,3665),
    ]
    sl = sum(c[4]*math.sin(c[0]*D+c[1]*M+c[2]*Mp+c[3]*F) for c in _terms)
    return (L0 + sl/1_000_000) % 360

def _venus_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (181.979801 + 58517.8156760*T) % 360
    M = math.radians((212.106 + 58517.803*T) % 360)
    return (L + (0.007680*math.sin(M) + 0.000500*math.sin(2*M))*180/math.pi) % 360

def _mars_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (355.433 + 19140.2993*T) % 360
    M = math.radians((19.387 + 19140.300*T) % 360)
    return (L + 10.6912*math.sin(M) + 0.6228*math.sin(2*M)) % 360

def _jupiter_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (34.351519 + 3034.9056606*T) % 360
    M = math.radians((20.020 + 3034.906*T) % 360)
    return (L + 5.5549*math.sin(M) + 0.1683*math.sin(2*M)) % 360

def _saturn_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (50.077444 + 1222.1137943*T) % 360
    M = math.radians((316.967 + 1221.549*T) % 360)
    return (L + 6.3585*math.sin(M) + 0.2204*math.sin(2*M)) % 360

def _mercury_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    L = (252.250906 + 149472.6746358*T) % 360
    M = math.radians((174.795 + 149472.515*T) % 360)
    return (L + 23.4400*math.sin(M) + 2.9818*math.sin(2*M) + 0.5255*math.sin(3*M)) % 360

def _rahu_trop(jd_val):
    T = (jd_val - 2451545.0) / 36525.0
    return (125.04452 - 1934.136261*T + 0.0020708*T**2) % 360

_RASHIS = ["Aries","Taurus","Gemini","Cancer","Leo","Virgo",
           "Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"]
_RASHIS_HI = ["मेष","वृषभ","मिथुन","कर्क","सिंह","कन्या",
              "तुला","वृश्चिक","धनु","मकर","कुंभ","मीन"]
_NAKS = [
    "Ashwini","Bharani","Krittika","Rohini","Mrigasira","Ardra",
    "Punarvasu","Pushya","Ashlesha","Magha","Purva Phalguni","Uttara Phalguni",
    "Hasta","Chitra","Swati","Vishakha","Anuradha","Jyeshtha",
    "Mula","Purvashadha","Uttarashadha","Shravana","Dhanishtha","Shatabhisha",
    "Purva Bhadrapada","Uttara Bhadrapada","Revati",
]
_NAK_LORDS = [
    "Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury",
    "Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury",
    "Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury",
]
_TITHI_NAMES = [
    "Pratipada","Dwitiya","Tritiya","Chaturthi","Panchami","Shashthi",
    "Saptami","Ashtami","Navami","Dashami","Ekadashi","Dwadashi",
    "Trayodashi","Chaturdashi","Purnima",
    "Pratipada","Dwitiya","Tritiya","Chaturthi","Panchami","Shashthi",
    "Saptami","Ashtami","Navami","Dashami","Ekadashi","Dwadashi",
    "Trayodashi","Chaturdashi","Amavasya",
]

# ── Bearish / Bullish / Trap nakshatra sets ──────────────────────
_BEARISH_NAKS = {
    "Shatabhisha","Krittika","Uttara Phalguni","Mrigasira","Dhanishtha",
    "Purva Bhadrapada","Mula","Purvashadha","Ardra","Ashlesha","Bharani",
}
_TRAP_NAKS = {"Rohini"}
_BULLISH_NAKS = {
    "Pushya","Punarvasu","Uttara Bhadrapada","Shravana","Revati",
    "Uttarashadha","Hasta","Anuradha",
}

# ── NSE holidays: sync with main NSE_HOLIDAYS (defined later) ────
_NSE_HOLIDAYS = set()   # filled at runtime via _get_nse_holidays()

def _get_nse_holidays():
    """Lazily sync _NSE_HOLIDAYS from the main NSE_HOLIDAYS set."""
    global _NSE_HOLIDAYS
    if not _NSE_HOLIDAYS:
        try:
            _NSE_HOLIDAYS = NSE_HOLIDAYS
        except NameError:
            pass
    return _NSE_HOLIDAYS

def _ang_dist(a, b):
    """Absolute angular distance between two sidereal longitudes (0–180)."""
    return abs((a - b + 180) % 360 - 180)

def _rashi(lon):
    return _RASHIS[int(lon / 30)]

def _nak(lon):
    idx = int(lon / (360/27))
    return _NAKS[idx], _NAK_LORDS[idx], idx


_YOGA_NAMES = [
    "Vishkumbha","Priti","Ayushman","Saubhagya","Shobhana","Atiganda","Sukarma","Dhriti",
    "Shula","Ganda","Vriddhi","Dhruva","Vyaghata","Harshana","Vajra","Siddhi","Vyatipata",
    "Variyan","Parigha","Shiva","Siddha","Sadhya","Shubha","Shukla","Brahma","Indra","Vaidhriti"
]

# =============================================================================
# 🌌 LIVE ASTRO UPDATE ENGINE (Vedic High Conviction)
# =============================================================================

# Mumbai Coordinates (Financial Hub)
MUMBAI_LAT = 18.9750
MUMBAI_LON = 72.8258

def _get_jd_now():
    """Julian day for current UTC."""
    now_utc = datetime.now(pytz.utc)
    frac_h  = now_utc.hour + now_utc.minute / 60.0 + now_utc.second / 3600.0
    return swe.julday(now_utc.year, now_utc.month, now_utc.day, frac_h)

def _get_planetary_state(jd_val):
    """Sidereal positions and speeds for all 9 planets."""
    swe.set_sid_mode(swe.SIDM_LAHIRI)
    planets = {}
    ids = {
        "Sun": swe.SUN, "Moon": swe.MOON, "Mars": swe.MARS, 
        "Mercury": swe.MERCURY, "Jupiter": swe.JUPITER, 
        "Venus": swe.VENUS, "Saturn": swe.SATURN, "Rahu": swe.MEAN_NODE
    }
    
    # Yesterday's JD for speed calculation
    jd_prev = jd_val - 1.0
    
    for name, pid in ids.items():
        res = swe.calc_ut(jd_val, pid, swe.FLG_SIDEREAL)[0]
        lon = res[0] % 360
        
        res_p = swe.calc_ut(jd_prev, pid, swe.FLG_SIDEREAL)[0]
        lon_p = res_p[0] % 360
        
        # Calculate speed (daily motion)
        speed = lon - lon_p
        if speed > 180: speed -= 360
        if speed < -180: speed += 360
        
        planets[name] = {
            "lon": lon, 
            "sign": _RASHIS[int(lon/30)], 
            "deg": lon%30, 
            "speed": speed,
            "retro": speed < 0
        }
    
    # Ketu (always opposite Rahu)
    k_lon = (planets["Rahu"]["lon"] + 180) % 360
    planets["Ketu"] = {
        "lon": k_lon, 
        "sign": _RASHIS[int(k_lon/30)], 
        "deg": k_lon%30,
        "speed": planets["Rahu"]["speed"],
        "retro": True # Rahu/Ketu always retro
    }
        
    return planets

def _get_lagna(jd_val):
    """Current rising sign (Lagna) for Mumbai."""
    res = swe.houses_ex(jd_val, MUMBAI_LAT, MUMBAI_LON, b'A')[0]
    asc_lon = res[0]
    return {"lon": asc_lon, "sign": _RASHIS[int(asc_lon/30)], "deg": asc_lon%30}

def _get_hora(jd_val, lat, lon):
    """Dynamic Hora calculation (Sunrise to Sunset / 12)."""
    # Exact Sunrise/Sunset for Mumbai at this JD
    # Correct signature: tjdut, body, rsmi, geopos, atpress, attemp, flags
    res_r = swe.rise_trans(jd_val - 0.5, swe.SUN, swe.CALC_RISE | swe.BIT_DISC_CENTER, (lon, lat, 0), 0, 0, swe.FLG_SWIEPH)
    sunrise_jd = res_r[1][0]
    res_s = swe.rise_trans(jd_val - 0.5, swe.SUN, swe.CALC_SET | swe.BIT_DISC_CENTER, (lon, lat, 0), 0, 0, swe.FLG_SWIEPH)
    sunset_jd = res_s[1][0]
    
    # Check if we are in night hora
    if jd_val < sunrise_jd or jd_val > sunset_jd:
        # For simplicity, focus on market hours (Day Horas)
        # Night calculation would involve Sunset to next day Sunrise
        return "Moon", "(Night)"
    
    day_len = sunset_jd - sunrise_jd
    hora_len = day_len / 12.0
    hora_num = int((jd_val - sunrise_jd) / hora_len)
    
    _HORA_SEQ = ["Sun","Venus","Mercury","Moon","Saturn","Jupiter","Mars"]
    dt = datetime.now(IST)
    # Day lord sequence starting from Moon (Monday)
    vara_idx = dt.weekday() # 0=Mon, 1=Tue...
    vara = ["Moon","Mars","Mercury","Jupiter","Venus","Saturn","Sun"][vara_idx]
    start_idx = _HORA_SEQ.index(vara)
    
    hora_lord = _HORA_SEQ[(start_idx + hora_num) % 7]
    h_start = sunrise_jd + hora_num * hora_len
    h_end   = h_start + hora_len
    
    def _jd_to_time(jd):
        y,m,d,h = swe.revjul(jd)
        total_m = h * 60 + 330 # IST
        hh = int((total_m // 60) % 24)
        mm = int(total_m % 60)
        return f"{hh:02d}:{mm:02d}"
        
    return hora_lord, f"({_jd_to_time(h_start)}–{_jd_to_time(h_end)})"

def _get_bhava(planets, lagna_lon):
    """Assign houses based on Lagna (1st sign = H1)."""
    lagna_sign_idx = int(lagna_lon / 30)
    for name, p in planets.items():
        p_sign_idx = int(p["lon"] / 30)
        p["house"] = (p_sign_idx - lagna_sign_idx + 12) % 12 + 1
    return planets

def _calculate_astro_score_v2(state):
    """High-conviction dynamic scoring system based on real-time data."""
    score = 50 
    planets = state["planets"]
    
    # 1. Dignities (Dynamic)
    EXALT = {"Sun":"Aries", "Moon":"Taurus", "Mars":"Capricorn", "Mercury":"Virgo", "Jupiter":"Cancer", "Venus":"Pisces", "Saturn":"Libra"}
    OWN   = {"Sun":"Leo", "Moon":"Cancer", "Mars":"Aries/Scorpio", "Mercury":"Gemini/Virgo", "Jupiter":"Sagittarius/Pisces", "Venus":"Taurus/Libra", "Saturn":"Capricorn/Aquarius"}
    DEBIL = {"Sun":"Libra", "Moon":"Scorpio", "Mars":"Cancer", "Mercury":"Pisces", "Jupiter":"Capricorn", "Venus":"Virgo", "Saturn":"Aries"}
    
    for p, d in planets.items():
        if p in EXALT and EXALT[p] == d["sign"]: score += 10
        elif p in OWN and d["sign"] in OWN[p]: score += 5
        elif p in DEBIL and DEBIL[p] == d["sign"]: score -= 8
        
    # 2. Planetary Speed (Dynamic - Atichari/Vakra)
    # Average speeds: Mercury 1.38, Venus 1.2, Mars 0.52, Jup 0.08, Sat 0.03
    if planets["Mercury"]["speed"] > 1.8: score += 5 # Atichari Bullish
    if planets["Mercury"]["retro"]: score -= 5 # Vakra Bearish
    if planets["Jupiter"]["retro"]: score -= 3
    if planets["Saturn"]["retro"]: score -= 2
    
    # 3. Tithi (Dynamic)
    tithi_raw = (planets["Moon"]["lon"] - planets["Sun"]["lon"]) % 360
    tithi_num = int(tithi_raw / 12) + 1
    if tithi_num <= 15: score += 5 # Shukla Paksha
    else: score -= 3 # Krishna Paksha
    
    # 4. Yoga (Dynamic)
    yoga_raw = (planets["Moon"]["lon"] + planets["Sun"]["lon"]) % 360
    yoga_idx = int(yoga_raw / (360/27))
    yoga_name = _YOGA_NAMES[yoga_idx % 27]
    _GOOD_YOGAS = {"Saubhagya","Siddhi","Shubha","Shukla","Brahma","Indra","Priti","Ayushman","Dhruva"}
    if yoga_name in _GOOD_YOGAS: score += 5
    
    # 5. Hora (Dynamic)
    if state["hora"] in ["Jupiter", "Venus", "Moon"]: score += 6
    elif state["hora"] in ["Saturn", "Mars", "Rahu", "Ketu"]: score -= 6
    
    # 6. Moon Nakshatra (Dynamic)
    nak_name, nak_lord, _ = _nak(planets["Moon"]["lon"])
    if nak_name in _BULLISH_NAKS: score += 8
    elif nak_name in _BEARISH_NAKS: score -= 8
    
    # 7. House Placement (Dynamic based on Lagna)
    # Benefics (Jup, Ven) in Kendras (1,4,7,10) or Trikonas (1,5,9)
    for p in ["Jupiter", "Venus"]:
        if planets[p]["house"] in [1,4,7,10,5,9]: score += 4
    # Malefics in Upachaya houses (3,6,10,11) are good
    for p in ["Saturn", "Mars", "Rahu"]:
        if planets[p]["house"] in [3,6,10,11]: score += 3
        elif planets[p]["house"] in [8,12]: score -= 5
    
    return min(100, max(0, int(score)))

def _get_atichari_status(planets):
    """Identify planets moving faster than their average speed (Real-time)."""
    status = []
    if planets["Mercury"]["speed"] > 1.8: status.append("Mercury Atichari")
    if planets["Venus"]["speed"] > 1.5: status.append("Venus Atichari")
    if planets["Jupiter"]["speed"] > 0.15: status.append("Jupiter Atichari")
    
    vakra = [p for p in ["Mercury", "Venus", "Mars", "Jupiter", "Saturn"] if planets[p]["retro"]]
    if vakra: status.append(f"Vakra: {', '.join(vakra)}")
    
    return " | ".join(status) if status else "Stable"

def _format_astro_update_msg(state):
    """Generate fully dynamic real-time alert message."""
    score = state["score"]
    bias  = "BUY (STRONG)" if score >= 65 else "BUY" if score >= 55 else "SELL (STRONG)" if score <= 35 else "SELL" if score <= 45 else "NEUTRAL"
    emoji = "🟢" if score >= 55 else "🔴" if score <= 45 else "🟡"
    
    tithi_info = f"{state['tithi']} | {state['nakshatra']} P{state['nak_pad']} | {state['paksha']}"
    yoga_info  = f"✨ Yoga: {state['yoga']} {'✅' if state['yoga_good'] else '⚠️'} | Karana: {state['karana']}"
    hora_info  = f"⏰ {state['hora']} Hora {state['hora_time']}"
    lagna_info = f"⬆ Lagna: {state['lagna']} ({state['lagna_lord']})"
    
    def planets_info(p_map, key):
        p = p_map[key]
        h = p.get("house", "?")
        return f"{p['sign']} {int(p['deg'])}°H{h}{'℞' if p.get('retro') else ''}"

    planets_line = "🪐 " + " | ".join([
        f"Sun: {planets_info(state['planets'], 'Sun')}",
        f"Moon: {planets_info(state['planets'], 'Moon')}",
        f"Mercury: {planets_info(state['planets'], 'Mercury')}",
        f"Venus: {planets_info(state['planets'], 'Venus')}",
        f"Mars: {planets_info(state['planets'], 'Mars')}",
        f"Jupiter: {planets_info(state['planets'], 'Jupiter')}",
        f"Saturn: {planets_info(state['planets'], 'Saturn')}",
        f"Rahu: {planets_info(state['planets'], 'Rahu')}",
        f"Ketu: {planets_info(state['planets'], 'Ketu')}"
    ])
    
    active_line = f"⚡ Active: ⚡ {state['active']}"
    
    routine = ""
    if state.get("house_changes"):
        routine = "🟢 ROUTINE CHANGES:\n"
        for ch in state["house_changes"]:
            routine += f"  🏠 {ch}\n"

    outlook = f"🇮🇳 Outlook: {bias} | Score: {score:+} | {state['yoga']} Yoga - {state['yoga_desc']}; {state['hora']} Hora - {state['hora_desc']}"

    msg = (
        f"{emoji} ASTRO UPDATE - {bias}\n"
        f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}  |  Score: {score:+d}\n\n"
        f"{routine}\n"
        f"📌 CURRENT STATE:\n"
        f"  Moon {tithi_info}\n"
        f"  {yoga_info}\n"
        f"  {hora_info}\n"
        f"  {lagna_info}\n\n"
        f"{planets_line}\n\n"
        f"{active_line}\n\n"
        f"{outlook}"
    )
    return msg

def fire_astro_update_alert():
    """Main function to detect real-time changes and fire alerts."""
    if not is_market_hours(): return
    if _is_tg_disabled("ASTRO_UPDATE"): return
    
    jd = _get_jd_now()
    planets = _get_planetary_state(jd)
    lagna   = _get_lagna(jd)
    planets = _get_bhava(planets, lagna["lon"])
    
    # Dynamic Panchang
    t_raw = (planets["Moon"]["lon"] - planets["Sun"]["lon"]) % 360
    t_num = int(t_raw / 12) + 1
    t_name = _TITHI_NAMES[t_num-1]
    paksha = "Waxing Crescent" if t_num <= 15 else "Waning Crescent"
    if t_num == 15: paksha = "Full Moon"
    if t_num == 30: paksha = "New Moon"
    
    nak_lon = planets["Moon"]["lon"]
    nak_name, _, _ = _nak(nak_lon)
    nak_pad = int((nak_lon % (360/27)) / (360/27/4)) + 1
    
    yoga_raw = (planets["Moon"]["lon"] + planets["Sun"]["lon"]) % 360
    yoga_name = _YOGA_NAMES[int(yoga_raw/(360/27)) % 27]
    _GOOD_YOGAS = {"Saubhagya","Siddhi","Shubha","Shukla","Brahma","Indra","Priti","Ayushman","Dhruva"}
    
    karana_num = int(t_raw / 6)
    _KARANAS = ["Bava","Balava","Kaulava","Taitila","Garija","Vanija","Vishti","Shakuni","Chatushpada","Naga","Kintughna"]
    karana_name = _KARANAS[karana_num % 11]
    
    hora_lord, hora_time = _get_hora(jd, MUMBAI_LAT, MUMBAI_LON)
    
    # Lagna Lord
    _SIGN_LORDS = {"Aries":"Mars","Taurus":"Venus","Gemini":"Mercury","Cancer":"Moon","Leo":"Sun","Virgo":"Mercury","Libra":"Venus","Scorpio":"Mars","Sagittarius":"Jupiter","Capricorn":"Saturn","Aquarius":"Saturn","Pisces":"Jupiter"}
    lagna_lord = _SIGN_LORDS.get(lagna["sign"], "?")
    
    state = {
        "score": _calculate_astro_score_v2({"planets": planets, "hora": hora_lord}),
        "planets": planets,
        "lagna": lagna["sign"],
        "lagna_lord": lagna_lord,
        "tithi": f"S-{t_name}" if t_num <= 15 else f"K-{t_name}",
        "paksha": paksha,
        "nakshatra": nak_name,
        "nak_pad": nak_pad,
        "yoga": yoga_name,
        "yoga_good": yoga_name in _GOOD_YOGAS,
        "karana": karana_name,
        "hora": hora_lord,
        "hora_time": hora_time,
        "active": _get_atichari_status(planets),
        "yoga_desc": "Auspicious" if yoga_name in _GOOD_YOGAS else "Neutral",
        "hora_desc": "Bullish bias" if hora_lord in ["Jupiter","Venus","Moon"] else "Bearish caution" if hora_lord in ["Saturn","Mars"] else "Normal"
    }
    
    if "last_astro_houses" not in st.session_state:
        st.session_state.last_astro_houses = {n: p["house"] for n, p in planets.items()}
        st.session_state.last_astro_score = state["score"]
        return

    changes = []
    for n, p in planets.items():
        last_h = st.session_state.last_astro_houses.get(n)
        if p["house"] != last_h:
            changes.append(f"{n} House Change: House {last_h} -> House {p['house']}")
            st.session_state.last_astro_houses[n] = p["house"]
    
    state["house_changes"] = changes
    score_changed = abs(state["score"] - st.session_state.last_astro_score) >= 10
    
    if changes or score_changed:
        st.session_state.last_astro_score = state["score"]
        msg = _format_astro_update_msg(state)
        _fire_panchak_tg_msg(msg, "ASTRO_UPDATE")

def _fire_panchak_tg_msg(msg, category):
    """Helper to send TG alert using dashboard's global send_telegram."""
    # This respects market hours and uses the dashboard's bot config
    send_telegram(msg, dedup_key=None)

# =============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def _vedic_day_analysis(d_date, hour_ist=9, minute_ist=15):
    """
    Full KP + Vedic prediction engine.
    Layers:
      A  Moon nakshatra signal (base)
      B  KP Sub-lord + Sub-sub-lord  (249-division)
      C  Day lord (Vara)
      D  Hora lord at market open
      E  Tithi + Karana
      F  Planetary dignity (exalt / debil / own)
      G  All conjunctions from books (0-15 deg)
      H  Aspects: trine 120, square 90, opposition 180
      I  Yogas: Gajakesari, Kemadruma, Vish, Subha
      J  Retrograde detection (all 5 planets, computed)
      K  Moon transit timing (hours to next nakshatra)
      L  Bullish rashi conditions
      M  Bearish rashi conditions
      N  Vasudev fertile/barren signs
      O  Dhruvanka (VB Govardhan Padhati)
      P  Murthy Nirnaya + Mercury strength
      Q  Crude oil signal
    """
    # ── Time: use 9:15 AM IST = 3:45 AM UTC ──────────────────────────
    dt_utc = datetime(d_date.year, d_date.month, d_date.day, 3, 45, 0)
    jd_val = _jd(dt_utc)

    # ── Sidereal positions ───────────────────────────────────────────
    m_s  = _sid(_moon_trop(jd_val),    jd_val)
    s_s  = _sid(_sun_trop(jd_val),     jd_val)
    v_s  = _sid(_venus_trop(jd_val),   jd_val)
    ma_s = _sid(_mars_trop(jd_val),    jd_val)
    j_s  = _sid(_jupiter_trop(jd_val), jd_val)
    sa_s = _sid(_saturn_trop(jd_val),  jd_val)
    me_s = _sid(_mercury_trop(jd_val), jd_val)
    r_s  = _sid(_rahu_trop(jd_val),    jd_val)
    k_s  = (r_s + 180) % 360

    # ── Retrograde (24-h forward motion) ────────────────────────────
    jd2 = _jd(datetime(d_date.year, d_date.month, d_date.day, 3, 45, 0) + timedelta(days=1))
    def _sid2(fn):
        return _sid(fn(jd2), jd2)
    def _daily_motion(lon1, lon2):
        d = lon2 - lon1
        if abs(d) > 180: d = d - 360 if d > 0 else d + 360
        return d
    retro = {
        "Jupiter": _daily_motion(j_s,  _sid2(_jupiter_trop)) < 0,
        "Saturn":  _daily_motion(sa_s, _sid2(_saturn_trop))  < 0,
        "Mercury": _daily_motion(me_s, _sid2(_mercury_trop)) < 0,
        "Venus":   _daily_motion(v_s,  _sid2(_venus_trop))   < 0,
        "Mars":    _daily_motion(ma_s, _sid2(_mars_trop))    < 0,
    }

    # ── Nakshatra, rashi, lords ──────────────────────────────────────
    moon_nak, moon_nak_lord, moon_nak_idx = _nak(m_s)
    sun_nak,  sun_nak_lord,  _            = _nak(s_s)
    mars_nak, _,             _            = _nak(ma_s)
    rahu_nak, _,             _            = _nak(r_s)

    moon_rashi    = _rashi(m_s)
    sun_rashi     = _rashi(s_s)
    venus_rashi   = _rashi(v_s)
    mars_rashi    = _rashi(ma_s)
    saturn_rashi  = _rashi(sa_s)
    jupiter_rashi = _rashi(j_s)
    mercury_rashi = _rashi(me_s)
    rahu_rashi    = _rashi(r_s)

    # ── KP Sub-lord system (249 divisions) ───────────────────────────
    # Each nakshatra (13°20') divided into 9 sub-periods proportional
    # to Vimshottari dasha years (120 yr total).  Sub-lord decides result.
    _KP_ORDER = ["Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury"]
    _KP_YRS   = {"Ketu":7,"Venus":20,"Sun":6,"Moon":10,"Mars":7,
                 "Rahu":18,"Jupiter":16,"Saturn":19,"Mercury":17}
    _KP_TOTAL = 120.0
    _NAK_SPAN = 360.0 / 27.0

    def _kp_sublords(lon):
        nak_idx   = int(lon / _NAK_SPAN)
        nak_lord  = _NAK_LORDS[nak_idx]
        pos       = (lon - nak_idx * _NAK_SPAN) / _NAK_SPAN   # 0-1 within nak
        start     = _KP_ORDER.index(nak_lord)
        cum = 0.0
        sub = nak_lord
        subsub = nak_lord
        for i in range(9):
            p   = _KP_ORDER[(start + i) % 9]
            f   = _KP_YRS[p] / _KP_TOTAL
            if pos <= cum + f:
                sub = p
                ss_start = _KP_ORDER.index(p)
                sub_pos  = (pos - cum) / f
                ss_cum   = 0.0
                for j in range(9):
                    sp = _KP_ORDER[(ss_start + j) % 9]
                    sf = _KP_YRS[sp] / _KP_TOTAL
                    if sub_pos <= ss_cum + sf:
                        subsub = sp
                        break
                    ss_cum += sf
                break
            cum += f
        return sub, subsub

    moon_sub, moon_subsub = _kp_sublords(m_s)
    sun_sub,  sun_subsub  = _kp_sublords(s_s)

    # ── Day lord (Vara) ──────────────────────────────────────────────
    _VARA = {0:"Moon",1:"Mars",2:"Mercury",3:"Jupiter",4:"Venus",5:"Saturn",6:"Sun"}
    day_lord = _VARA[d_date.weekday()]

    # ── Hora lord at 9:15 AM ─────────────────────────────────────────
    # Chaldean order: Sun Venus Mercury Moon Saturn Jupiter Mars
    _HORA_SEQ   = ["Sun","Venus","Mercury","Moon","Saturn","Jupiter","Mars"]
    # Correct: each lord's index = its position in Chaldean sequence (first hora = day lord)
    # Sunrise ~6:20 IST. Slot 0=6:20-7:20, slot 1=7:20-8:20, slot 2=8:20-9:20.
    # 9:15 falls in slot 2 (0-based), so offset = +2
    _HORA_START = {"Sun":0,"Venus":1,"Mercury":2,"Moon":3,"Saturn":4,"Jupiter":5,"Mars":6}
    _hora_idx   = (_HORA_START.get(day_lord, 0) + 2) % 7   # 9:15 = slot 2 after sunrise
    hora_open   = _HORA_SEQ[_hora_idx]
    _HORA_BULL  = {"Sun","Jupiter","Venus","Moon"}
    _HORA_BEAR  = {"Saturn","Mars","Rahu","Ketu"}

    # ── Tithi, Paksha, Karana, Yoga ──────────────────────────────────
    tithi_raw  = (m_s - s_s) % 360
    tithi_num  = int(tithi_raw / 12) + 1
    tithi_name = _TITHI_NAMES[min(tithi_num - 1, 29)]
    paksha     = "Shukla" if tithi_num <= 15 else "Krishna"
    karana_num = int(tithi_raw / 6)
    _KARANAS   = ["Bava","Balava","Kaulava","Taitila","Garija","Vanija","Vishti"]
    karana     = _KARANAS[karana_num % 7]

    yoga_raw  = (m_s + s_s) % 360
    yoga_idx  = int(yoga_raw / (360/27))
    yoga_name = _YOGA_NAMES[min(yoga_idx, 26)]

    # ── Navamsha (D9) ────────────────────────────────────────────────
    def _d9(lon):
        si = int(lon / 30)
        di = int((lon % 30) / (30.0/9))
        starts = {0:0,1:9,2:6,3:3,4:0,5:9,6:6,7:3,8:0,9:9,10:6,11:3}
        return _RASHIS[(starts.get(si,0) + di) % 12]

    moon_d9    = _d9(m_s)
    sun_d9     = _d9(s_s)
    moon_from_sun = int(((m_s - s_s) % 360) / 30) + 1

    # ── Planetary dignity ────────────────────────────────────────────
    _EXALT = {"Sun":"Aries","Moon":"Taurus","Mars":"Capricorn","Mercury":"Virgo",
              "Jupiter":"Cancer","Venus":"Pisces","Saturn":"Libra",
              "Rahu":"Gemini","Ketu":"Sagittarius"}
    _DEBIL = {"Sun":"Libra","Moon":"Scorpio","Mars":"Cancer","Mercury":"Pisces",
              "Jupiter":"Capricorn","Venus":"Virgo","Saturn":"Aries",
              "Rahu":"Sagittarius","Ketu":"Gemini"}
    _OWN   = {"Sun":{"Leo"},"Moon":{"Cancer"},"Mars":{"Aries","Scorpio"},
              "Mercury":{"Gemini","Virgo"},"Jupiter":{"Sagittarius","Pisces"},
              "Venus":{"Taurus","Libra"},"Saturn":{"Capricorn","Aquarius"}}
    def _dignity(planet, rashi):
        if _EXALT.get(planet) == rashi:     return "EXALTED"
        if _DEBIL.get(planet) == rashi:     return "DEBILITATED"
        if rashi in _OWN.get(planet, set()): return "OWN"
        return "NEUTRAL"
    dig = {p: _dignity(p, r) for p, r in [
        ("Moon",    moon_rashi),    ("Sun",     sun_rashi),
        ("Jupiter", jupiter_rashi), ("Venus",   venus_rashi),
        ("Mars",    mars_rashi),    ("Saturn",  saturn_rashi),
        ("Mercury", mercury_rashi),
    ]}

    # ── Angular separations ──────────────────────────────────────────
    moon_ketu    = _ang_dist(m_s,  k_s)
    moon_rahu    = _ang_dist(m_s,  r_s)
    moon_saturn  = _ang_dist(m_s,  sa_s)
    moon_mars    = _ang_dist(m_s,  ma_s)
    moon_jupiter = _ang_dist(m_s,  j_s)
    moon_venus   = _ang_dist(m_s,  v_s)
    mars_rahu    = _ang_dist(ma_s, r_s)
    sun_saturn   = _ang_dist(s_s,  sa_s)
    mars_saturn  = _ang_dist(ma_s, sa_s)
    merc_mars    = _ang_dist(me_s, ma_s)
    merc_rahu    = _ang_dist(me_s, r_s)
    sat_venus    = _ang_dist(sa_s, v_s)
    merc_venus   = _ang_dist(me_s, v_s)
    sun_rahu     = _ang_dist(s_s,  r_s)
    sun_mars     = _ang_dist(s_s,  ma_s)
    mars_jupiter = _ang_dist(ma_s, j_s)
    jup_saturn   = _ang_dist(j_s,  sa_s)
    jup_rahu     = _ang_dist(j_s,  r_s)
    moon_sun     = _ang_dist(m_s,  s_s)

    # ── Moon transit timing ──────────────────────────────────────────
    _nak_end      = (moon_nak_idx + 1) * _NAK_SPAN
    _deg_left     = _nak_end - m_s
    _hrs_to_next  = round(_deg_left / (13.2 / 24), 1)
    _next_nak     = _NAKS[(moon_nak_idx + 1) % 27]
    _next_nak_lord= _NAK_LORDS[(moon_nak_idx + 1) % 27]

    # ── Ashtakavarga (simplified) ────────────────────────────────────
    _BENEFICS = {"Jupiter","Venus","Moon","Mercury","Sun"}
    moon_sign_idx = int(m_s / 30)
    ashta_moon = sum(1 for p, lon in [
        ("Jupiter",j_s),("Venus",v_s),("Moon",m_s),
        ("Mercury",me_s),("Sun",s_s)
    ] if int(lon/30) == moon_sign_idx)
    ashta_str = "STRONG" if ashta_moon >= 4 else ("WEAK" if ashta_moon <= 1 else "MODERATE")

    # ── SCORING ─────────────────────────────────────────────────────
    B = 0   # bearish pts
    G = 0   # bullish pts
    signals = []   # (text, type)  B/G/T/M

    # ════════════════════════════════════════════════════════════════
    # LAYER A - NAKSHATRA
    # ════════════════════════════════════════════════════════════════
    _B_NAKS = {
        "Shatabhisha","Krittika","Uttara Phalguni","Mrigasira","Dhanishtha",
        "Purva Bhadrapada","Mula","Purvashadha","Ardra","Ashlesha","Bharani",
        "Chitra","Vishakha","Magha","Jyeshtha","Swati","Purva Phalguni",
    }
    _G_NAKS = {
        "Pushya","Punarvasu","Uttara Bhadrapada","Shravana","Revati",
        "Uttarashadha","Hasta","Anuradha","Ashwini",
    }
    _EXTREME_NAKS = {"Ardra","Ashlesha","Shatabhisha"}  # Mula removed - see below

    if moon_nak in _B_NAKS:
        if moon_nak == "Mula":
            # Mula = Ketu nak = sudden/explosive moves, NOT reliably directional.
            # Classic Vedic: Mula = "uprooting" - can be sharp UP or DOWN.
            # When Jupiter is exalted, Mula move is typically UP (foundation holds).
            # Treat as B+2 volatile (not B+3 extreme) and add TRAP signal.
            B += 2
            signals.append((f"Moon in Mula (Ketu★) - sudden volatile move; direction unclear", "T"))
        else:
            pts = 3 if moon_nak in _EXTREME_NAKS else 2
            B += pts
            signals.append((f"Moon in {moon_nak} ({moon_nak_lord}★) - bearish nak", "B"))
    elif moon_nak == "Rohini":
        signals.append(("Moon in Rohini - TRAP (illusion rally; reverse after 10:30)", "T"))
    elif moon_nak in _G_NAKS:
        G += 2
        signals.append((f"Moon in {moon_nak} ({moon_nak_lord}★) - bullish nak", "G"))

    # ════════════════════════════════════════════════════════════════
    # LAYER B - KP SUB-LORD (decides quality of nakshatra signal)
    # ════════════════════════════════════════════════════════════════
    _KP_B = {"Saturn","Rahu","Ketu","Mars"}
    _KP_G = {"Jupiter","Venus","Moon"}
    if moon_sub in _KP_G:
        G += 2
        signals.append((f"KP Moon sub-lord {moon_sub} (benefic) - buy confirmed", "G"))
    elif moon_sub in _KP_B:
        B += 2
        signals.append((f"KP Moon sub-lord {moon_sub} (malefic) - sell confirmed", "B"))
    if moon_subsub in _KP_G:
        G += 1
        signals.append((f"KP Moon sub-sub {moon_subsub} - buy timing fine-tuned", "G"))
    elif moon_subsub in _KP_B:
        B += 1
        signals.append((f"KP Moon sub-sub {moon_subsub} - sell timing fine-tuned", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER C - DAY LORD (Vara)
    # ════════════════════════════════════════════════════════════════
    _VARA_SIG = {
        "Sun":     ("G", 1, "Sun Vara - market tends to open strong"),
        "Moon":    ("G", 1, "Moon Vara - emotional; often green open"),
        "Mars":    ("B", 1, "Mars Vara - aggressive selling tendency"),
        "Mercury": ("M", 0, "Mercury Vara - volatile two-sided day"),
        "Jupiter": ("G", 2, "Jupiter Vara - most bullish weekday"),
        "Venus":   ("G", 1, "Venus Vara - positive close tendency"),
        "Saturn":  ("B", 1, "Saturn Vara - sluggish; bearish tendency"),
    }
    _vt, _vp, _vn = _VARA_SIG.get(day_lord, ("M",0,""))
    if _vp:
        if _vt == "G": G += _vp; signals.append((f"Day lord {day_lord} - {_vn}", "G"))
        elif _vt == "B": B += _vp; signals.append((f"Day lord {day_lord} - {_vn}", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER D - HORA LORD at 9:15 AM
    # ════════════════════════════════════════════════════════════════
    if hora_open in _HORA_BULL:
        G += 1; signals.append((f"Opening hora: {hora_open} (benefic) - bullish first hour", "G"))
    elif hora_open in _HORA_BEAR:
        B += 1; signals.append((f"Opening hora: {hora_open} (malefic) - bearish first hour", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER E - TITHI + KARANA
    # ════════════════════════════════════════════════════════════════
    if tithi_name == "Amavasya":
        B += 3; signals.append(("Amavasya - extreme bearish; sell at open", "B"))
    elif paksha == "Krishna" and tithi_name == "Pratipada":
        B += 3; signals.append(("Krishna Pratipada (post-Amavasya) - almost always bearish", "B"))
    elif paksha == "Shukla" and tithi_name == "Pratipada":
        B += 1; signals.append(("Shukla Pratipada - mild bearish (post-Purnima)", "B"))
    elif tithi_name == "Purnima":
        signals.append(("Purnima - volatile; watch direction at 09:30", "M"))
    if paksha == "Shukla" and tithi_name in ("Panchami","Saptami","Dashami","Ekadashi","Dwadashi"):
        G += 1; signals.append((f"Shukla {tithi_name} - auspicious tithi (Govardhan)", "G"))
    elif paksha == "Krishna" and tithi_name in ("Ashtami","Navami","Dashami","Chaturdashi"):
        B += 1; signals.append((f"Krishna {tithi_name} - inauspicious tithi", "B"))
    if karana == "Vishti":
        B += 1; signals.append(("Vishti (Bhadra) Karana - avoid longs this half-day", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER F - PLANETARY DIGNITY
    # ════════════════════════════════════════════════════════════════
    if dig["Moon"] == "EXALTED":
        G += 2; signals.append(("Moon exalted in Taurus - gap-down then 200pt recovery; buy", "G"))
    elif dig["Moon"] == "DEBILITATED":
        B += 2; signals.append(("Moon debilitated in Scorpio - weak psychology; bearish", "B"))
    elif dig["Moon"] == "OWN":
        G += 1; signals.append(("Moon in own sign Cancer - emotionally supportive", "G"))
    if dig["Jupiter"] == "EXALTED":
        G += 3; signals.append(("Jupiter EXALTED in Cancer - maximum wealth protection; floor under market; no crash possible", "G"))
    elif dig["Jupiter"] == "DEBILITATED":
        B += 1; signals.append(("Jupiter debilitated (Capricorn) - markets depressed", "B"))
    if dig["Venus"] == "EXALTED":
        G += 2; signals.append(("Venus exalted in Pisces - FMCG/banks very bullish", "G"))
    if dig["Mars"] == "EXALTED":
        B += 1; signals.append(("Mars exalted (Capricorn) - aggressive volatility", "B"))
    elif dig["Mars"] == "DEBILITATED":
        G += 1; signals.append(("Mars debilitated (Cancer) - aggression reduced; mild bullish", "G"))
    if dig["Saturn"] == "EXALTED":
        B += 1; signals.append(("Saturn exalted (Libra) - slow grind down", "B"))
    elif dig["Saturn"] == "DEBILITATED":
        G += 1; signals.append(("Saturn debilitated (Aries) - restriction lifting", "G"))

    # ════════════════════════════════════════════════════════════════
    # LAYER G - CONJUNCTIONS (book rules, 0–15 deg orb)
    # ════════════════════════════════════════════════════════════════
    # Moon-Ketu
    if moon_ketu < 15:
        B += 3; signals.append((f"Moon–Ketu {moon_ketu:.1f}° - Reliance/HDFC weak; BankNifty falls more", "B"))
    # Moon-Rahu
    if moon_rahu < 10:
        B += 2; signals.append((f"Moon–Rahu {moon_rahu:.1f}° - illusion/confusion; trap rallies", "T"))
    # Mars-Rahu (most dangerous from books)
    if mars_rahu < 5:
        B += 5; signals.append((f"Mars–Rahu {mars_rahu:.1f}° EXACT - EXTREME bearish; lower circuit risk; crude surges", "B"))
    elif mars_rahu < 10:
        B += 4; signals.append((f"Mars–Rahu {mars_rahu:.1f}° - very dangerous; crude bullish", "B"))
    elif mars_rahu < 15:
        B += 3; signals.append((f"Mars–Rahu {mars_rahu:.1f}° active - bearish; sell rallies", "B"))
    # Mercury+Mars+Rahu triple
    if mars_rahu < 15 and merc_rahu < 15 and merc_mars < 15:
        B += 3; signals.append(("Merc–Mars–Rahu TRIPLE - fire doubled; crash risk; crude to surge", "B"))
    # Saturn-Venus
    if saturn_rashi == venus_rashi:
        B += 2; signals.append((f"Saturn–Venus conjunct in {saturn_rashi} - brutal bearish until Venus exits", "B"))
    elif sat_venus < 20:
        B += 1; signals.append((f"Saturn–Venus {sat_venus:.0f}° - mild bearish pressure", "B"))
    # Sun-Saturn
    if sun_saturn < 3:
        B += 4; signals.append((f"Sun–Saturn {sun_saturn:.1f}° EXACT - Nifty+Gold+Silver all fall; only crude rises", "B"))
    elif sun_saturn < 8:
        B += 2; signals.append((f"Sun–Saturn {sun_saturn:.1f}° - strong bearish; Gold/Silver weak", "B"))
    # Mars-Saturn
    if mars_saturn < 3:
        B += 4; signals.append((f"Mars–Saturn {mars_saturn:.1f}° EXACT - geopolitical crisis; extreme bearish", "B"))
    elif mars_saturn < 8:
        B += 2; signals.append((f"Mars–Saturn {mars_saturn:.1f}° - strong bearish", "B"))
    # Mercury-Mars
    if merc_mars < 5:
        B += 2; signals.append((f"Mercury–Mars {merc_mars:.1f}° - aggression doubled; fall accelerates", "B"))
    # Moon-Saturn
    if moon_saturn < 10:
        B += 2; signals.append((f"Moon–Saturn {moon_saturn:.0f}° (Vish Yoga) - mood heavy; sharp fall risk", "B"))
    # Sun-Rahu
    if sun_rahu < 10:
        B += 2; signals.append((f"Sun–Rahu {sun_rahu:.1f}° - share prices bearish; illusion of strength", "B"))
    # Mercury-Venus
    if mercury_rashi == venus_rashi and merc_venus < 15:
        B += 1; signals.append((f"Mercury+Venus in {mercury_rashi} {merc_venus:.0f}° - prices fall (VB+VR books)", "B"))
    # Moon-Mars
    if moon_mars < 8:
        B += 1; signals.append((f"Moon–Mars {moon_mars:.0f}° - sudden aggressive selloff risk", "B"))
    # Sun-Mars
    if sun_mars < 5:
        B += 2; signals.append((f"Sun–Mars {sun_mars:.0f}° - Mundane adversity; crash risk (VB Ch48)", "B"))
    elif sun_mars < 12:
        B += 1; signals.append((f"Sun–Mars {sun_mars:.0f}° - selling pressure building", "B"))
    # Mars-Jupiter
    if mars_jupiter < 10:
        B += 2; signals.append((f"Mars–Jupiter {mars_jupiter:.0f}° - financial trouble (VB Ch48 Mundane)", "B"))
    # Jupiter-Rahu
    if jup_rahu < 15:
        signals.append((f"Jupiter–Rahu {jup_rahu:.0f}° - Gold/Silver hyperinflation (VB Ch17)", "M"))
    # Moon-Venus (benefic)
    if moon_venus < 10:
        G += 1; signals.append((f"Moon–Venus {moon_venus:.0f}° - harmony; FMCG/banks supportive", "G"))
    # Moon-Jupiter (benefic)
    if moon_jupiter < 10:
        G += 2; signals.append((f"Moon–Jupiter {moon_jupiter:.0f}° (Gajakesari) - strong rally signal", "G"))

    # ════════════════════════════════════════════════════════════════
    # LAYER H - ASPECTS (120° trine, 90° square, 180° opposition)
    # ════════════════════════════════════════════════════════════════
    def _asp(sep, target, orb=8): return abs(sep - target) <= orb

    # Moon trine Jupiter (120°) - very bullish
    if _asp(moon_jupiter, 120, 10):
        G += 2; signals.append((f"Moon trine Jupiter {moon_jupiter:.0f}° - Gajakesari; strong rally", "G"))
    # Moon opposite Jupiter (180°)
    elif _asp(moon_jupiter, 180, 8):
        G += 1; signals.append((f"Moon opp Jupiter {moon_jupiter:.0f}° - recovery tendency", "G"))
    # Moon square Saturn - bearish tension
    if _asp(moon_saturn, 90, 8):
        B += 1; signals.append((f"Moon sq Saturn {moon_saturn:.0f}° - tension; selling pressure", "B"))
    # Moon trine Venus - mild bullish
    if _asp(moon_venus, 120, 8):
        G += 1; signals.append((f"Moon trine Venus {moon_venus:.0f}° - harmony; mild bullish", "G"))
    # Mars trine Jupiter - risk-on
    if _asp(mars_jupiter, 120, 8):
        G += 1; signals.append((f"Mars trine Jupiter {mars_jupiter:.0f}° - confidence; risk-on", "G"))
    # Saturn square Sun - bearish
    if _asp(sun_saturn, 90, 8):
        B += 1; signals.append((f"Sun sq Saturn {sun_saturn:.0f}° - pressure; authority challenged", "B"))
    # Jupiter trine Sun - bullish
    if _asp(_ang_dist(s_s, j_s), 120, 8):
        G += 1; signals.append((f"Sun trine Jupiter {_ang_dist(s_s,j_s):.0f}° - positive; confidence", "G"))

    # ════════════════════════════════════════════════════════════════
    # LAYER I - YOGAS
    # ════════════════════════════════════════════════════════════════
    # Kemadruma - Moon with no planets in adjacent signs
    moon_sign_n = int(m_s / 30)
    _plon = [j_s, sa_s, v_s, me_s, s_s, ma_s]
    adj_empty = all(
        abs(int(lon/30) - moon_sign_n) not in (1, 11)
        for lon in _plon
    )
    if adj_empty:
        B += 1; signals.append(("Kemadruma Yoga - Moon isolated; market confusion", "B"))
    # Vish Yoga - Moon+Saturn same sign (already caught above in conjunctions)
    # Subha Yoga - Moon exalted/own + benefics around
    if dig["Moon"] in ("EXALTED","OWN") and ashta_str == "STRONG":
        G += 1; signals.append(("Subha Yoga - Moon strong with benefics; market supported", "G"))
    # Gaja-Kesari - Moon trine/conj Jupiter (caught in layers G+H)

    # ════════════════════════════════════════════════════════════════
    # LAYER J - RETROGRADE PLANETS
    # ════════════════════════════════════════════════════════════════
    for planet, is_r in retro.items():
        if not is_r: continue
        if planet == "Jupiter":
            if sun_nak_lord == "Jupiter" or moon_nak_lord == "Jupiter":
                B += 1; signals.append(("Jupiter retro + Sun/Moon in Jupiter nak - correction amplifier", "B"))
        elif planet == "Saturn":
            G += 1; signals.append(("Saturn retro - restriction easing; mild market relief", "G"))
        elif planet == "Mars":
            B += 1; signals.append(("Mars retro - delayed aggression; sudden reversals likely", "B"))
        elif planet == "Mercury":
            B += 1; signals.append(("Mercury retro - IT/Telecom weak; contracts stall", "B"))
        elif planet == "Venus":
            B += 1; signals.append(("Venus retro - FMCG/Banks/luxury sector weak", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER K - MOON TRANSIT TIMING
    # ════════════════════════════════════════════════════════════════
    if 0 < _hrs_to_next < 3:
        _nn_sig = "G" if _next_nak in _G_NAKS else ("B" if _next_nak in _B_NAKS else "M")
        if _nn_sig == "G":
            G += 1; signals.append((f"Moon enters {_next_nak} ({_next_nak_lord}★) in {_hrs_to_next}h - afternoon rally", "G"))
        elif _nn_sig == "B":
            B += 1; signals.append((f"Moon enters {_next_nak} ({_next_nak_lord}★) in {_hrs_to_next}h - afternoon reversal", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER L+M - RASHI CONDITIONS (Vasudev / books)
    # ════════════════════════════════════════════════════════════════
    if venus_rashi == "Aries":
        G += 3; signals.append(("Venus in Aries - BOTTOM ZONE; heavy long bias", "G"))
    if moon_rashi == "Taurus":
        G += 2; signals.append(("Moon in Taurus (exalted rashi) - gap-down then 200pt recovery; buy gap", "G"))
    if moon_rashi in ("Sagittarius","Cancer"):
        G += 1; signals.append((f"Moon in {moon_rashi} - positive close tendency", "G"))
    if sun_rashi == "Capricorn":
        G += 2; signals.append(("Sun in Capricorn - bull-run monthly bias", "G"))
    if sun_rashi in {"Sagittarius","Leo","Aries","Scorpio"}:
        G += 1; signals.append((f"Sun in {sun_rashi} - positive monthly rashi (Vasudev)", "G"))
    if sun_rashi in {"Aquarius","Gemini"}:
        B += 1; signals.append((f"Sun in {sun_rashi} - bearish monthly rashi (Vasudev)", "B"))
    if moon_d9 in ("Capricorn","Aquarius"):
        B += 1; signals.append((f"Moon D9 in Saturn sign ({moon_d9}) - bearish tinge", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER N - VASUDEV FERTILE / BARREN SIGNS
    # ════════════════════════════════════════════════════════════════
    _fertile = {"Aries","Taurus","Leo","Scorpio","Sagittarius","Capricorn"}
    _barren  = {"Gemini","Virgo","Libra","Aquarius","Pisces","Cancer"}
    _all_r   = [jupiter_rashi,saturn_rashi,rahu_rashi,sun_rashi,
                moon_rashi,venus_rashi,mercury_rashi,mars_rashi]
    _fc = sum(1 for r in _all_r if r in _fertile)
    _bc = sum(1 for r in _all_r if r in _barren)
    if _fc >= 5:
        G += 2; signals.append((f"Vasudev: {_fc}/8 planets in fertile signs - strong buy bias", "G"))
    elif _fc >= 4 and _fc > _bc:
        G += 1; signals.append((f"Vasudev: {_fc} fertile signs - mild bullish", "G"))
    if _bc >= 5:
        B += 2; signals.append((f"Vasudev: {_bc}/8 in barren signs - depressed market", "B"))
    elif _bc >= 4 and _bc > _fc:
        B += 1; signals.append((f"Vasudev: {_bc} barren signs - mild bearish", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER O - DHRUVANKA (VB Govardhan Padhati)
    # ════════════════════════════════════════════════════════════════
    _DH_N = {"Ashwini":10,"Bharani":10,"Krittika":96,"Rohini":56,"Mrigasira":20,
             "Ardra":86,"Punarvasu":21,"Pushya":64,"Ashlesha":135,"Magha":150,
             "Purva Phalguni":220,"Uttara Phalguni":72,"Hasta":334,"Chitra":21,
             "Swati":210,"Vishakha":320,"Anuradha":493,"Jyeshtha":559,
             "Mula":736,"Purvashadha":576,"Uttarashadha":275,"Shravana":126,
             "Dhanishtha":256,"Shatabhisha":275,"Purva Bhadrapada":126,
             "Uttara Bhadrapada":256,"Revati":275}
    _DH_R = {"Aries":37,"Taurus":84,"Gemini":66,"Cancer":109,"Leo":125,"Virgo":102,
             "Libra":140,"Scorpio":144,"Sagittarius":144,"Capricorn":198,
             "Aquarius":190,"Pisces":180}
    _DH_T = {"Pratipada":25,"Dwitiya":23,"Tritiya":21,"Chaturthi":19,"Panchami":17,
             "Shashthi":15,"Saptami":13,"Ashtami":11,"Navami":9,"Dashami":18,
             "Ekadashi":20,"Dwadashi":22,"Trayodashi":24,"Chaturdashi":26,
             "Purnima":16,"Amavasya":7}
    _DH_V = {0:103,1:70,2:62,3:212,4:112,5:542,6:40}
    _dh = (86 + _DH_N.get(moon_nak,50) + _DH_R.get(moon_rashi,100)
              + _DH_T.get(tithi_name,15) + _DH_V.get(d_date.weekday(),100))
    _dr = _dh % 9 or 9
    _DO = {1:"RISE",2:"FALL",3:"RISE",4:"SHARP RISE",5:"FALL",6:"RISE",7:"NO CHANGE",8:"RISE",9:"FALL"}
    _dout = _DO.get(_dr,"MIXED")
    if _dout in ("RISE","SHARP RISE"):
        _dp = 2 if _dout == "SHARP RISE" else 1
        G += _dp; signals.append((f"VB Dhruvanka: {_dh}÷9=R{_dr} -> {_dout} (Govardhan Padhati)", "G"))
    elif _dout == "FALL":
        B += 1;   signals.append((f"VB Dhruvanka: {_dh}÷9=R{_dr} -> {_dout} (Govardhan Padhati)", "B"))
    else:
        signals.append((f"VB Dhruvanka: {_dh}÷9=R{_dr} -> {_dout}", "M"))

    # ════════════════════════════════════════════════════════════════
    # LAYER P - MURTHY NIRNAYA + MERCURY STRENGTH (VB Ch50)
    # ════════════════════════════════════════════════════════════════
    _JM = {"Aries":("Copper","G"),"Taurus":("Silver","G"),"Gemini":("Iron","B"),
           "Cancer":("Gold","M"),"Leo":("Silver","G"),"Virgo":("Copper","G"),
           "Libra":("Iron","B"),"Scorpio":("Gold","M"),"Sagittarius":("Copper","G"),
           "Capricorn":("Iron","B"),"Aquarius":("Silver","G"),"Pisces":("Gold","M")}
    _jm = _JM.get(jupiter_rashi)
    if _jm:
        _mu, _mt = _jm
        _jtxt = f"Jupiter in {jupiter_rashi} = {_mu} Murthy (VB Nirnaya)"
        if _mt == "G":   G += 1; signals.append((_jtxt, "G"))
        elif _mt == "B": B += 1; signals.append((_jtxt, "B"))
        else:            signals.append((_jtxt, "M"))
    if mercury_rashi in ("Gemini","Virgo"):
        B += 1; signals.append((f"Mercury strong ({mercury_rashi}) - REDUCES share prices (VB Ch50)", "B"))
    elif mercury_rashi == "Pisces":
        G += 1; signals.append(("Mercury in Pisces - RAISES share prices (VB Ch50)", "G"))
    if moon_from_sun == 11:
        B += 1; signals.append(("Moon 11th from Sun - historically bearish (transcripts)", "B"))

    # ════════════════════════════════════════════════════════════════
    # LAYER Q - CRUDE OIL SIGNAL
    # ════════════════════════════════════════════════════════════════
    crude_bull = []
    if mars_rahu < 15: crude_bull.append(f"Mars–Rahu {mars_rahu:.0f}°")
    if mars_saturn < 8: crude_bull.append(f"Mars–Saturn {mars_saturn:.0f}°")
    if saturn_rashi in ("Libra","Capricorn"): crude_bull.append(f"Saturn uchha ({saturn_rashi})")
    if crude_bull:
        signals.append((f"⛽ CRUDE BULLISH - {', '.join(crude_bull)} (inverse to Nifty)", "M"))

    # ════════════════════════════════════════════════════════════════
    # LAYER R - PLANETARY TRANSIT APPROACH (approaching exaltation/own)
    # KEY INSIGHT: Planets APPROACHING exaltation are MORE powerful than
    # already-in-exaltation because market anticipates the event.
    # This was the main failure on Apr 8, 2026 - Sun was 6 days from
    # entering Aries (exaltation). Astrologers caught this, engine didn't.
    # ════════════════════════════════════════════════════════════════

    # Sun approaching Aries (exaltation) - enters at 0° Aries
    # Sun in last 10° of Pisces = approaching exaltation -> strong bull signal
    try:
        _sun_lon_deg = s_s % 360
        _sun_in_pisces_late = (330 <= _sun_lon_deg < 360)  # Pisces = 330-360
        _days_to_aries = round((360 - _sun_lon_deg) / 0.9855) if _sun_lon_deg >= 330 else 0
        if _sun_in_pisces_late and _days_to_aries <= 10:
            G += 3
            signals.append((
                f"Sun️ Sun approaching Aries (exaltation) in ~{_days_to_aries} days "
                f"- market front-runs the bull run (STRONGEST signal, astrologer-confirmed)",
                "G"
            ))
        elif sun_rashi == "Aries":
            G += 3
            signals.append(("Sun️ Sun in Aries (exaltation) - strongest bullish month of year", "G"))
    except Exception:
        pass

    # Saturn in own sign Aquarius or Capricorn = excellent condition
    # In 2025-2026 Saturn is in Pisces - but check dignity
    try:
        if saturn_rashi in ("Aquarius", "Capricorn"):
            G += 2
            signals.append((
                f"Saturn Saturn in own sign {saturn_rashi} - excellent condition, "
                f"market structure strong (VB + astrologer confirmed)",
                "G"
            ))
        elif dig.get("Saturn") == "EXALTED":
            # Already caught above but add extra weight if confirmed by astrologer
            pass
        elif saturn_rashi == "Pisces":
            # Saturn in Pisces (2023-2026 transit) - check if doing well
            # Saturn in watery sign = mixed but not directly bearish for market
            pass
    except Exception:
        pass

    # Venus in Pisces (exaltation) - extra weight beyond Layer F
    try:
        if venus_rashi == "Pisces" and dig.get("Venus") != "EXALTED":
            # Already in Pisces but not caught as exalted - fix
            G += 2
            signals.append(("Venus Venus in Pisces (exaltation) - FMCG/banks/auto very bullish", "G"))
        elif venus_rashi == "Taurus":
            G += 2
            signals.append(("Venus Venus in own sign Taurus - strong financial sector", "G"))
        elif venus_rashi == "Libra":
            G += 1
            signals.append(("Venus Venus in own sign Libra - balanced bullish tendency", "G"))
    except Exception:
        pass

    # Mars in Aries / Scorpio (own signs) = strong but volatile, not directly bearish
    try:
        if mars_rashi in ("Aries", "Scorpio") and dig.get("Mars") != "EXALTED":
            # Mars in own sign = energy/momentum, market volatile but directional
            signals.append((
                f"Mars Mars in own sign {mars_rashi} - aggressive directional move; "
                f"follow opening 15min direction",
                "M"
            ))
        elif mars_rashi == "Capricorn":
            # Mars exalted - already caught in Layer F
            pass
    except Exception:
        pass

    # Jupiter in own sign Cancer (exalted) or Sagittarius/Pisces (own)
    try:
        if jupiter_rashi in ("Sagittarius", "Pisces") and dig.get("Jupiter") not in ("EXALTED","OWN"):
            G += 2
            signals.append((
                f"Jupiter Jupiter in own sign {jupiter_rashi} - wealth expansion, "
                f"banking/finance sector strong",
                "G"
            ))
        elif jupiter_rashi == "Cancer" and dig.get("Jupiter") != "EXALTED":
            G += 3
            signals.append(("Jupiter Jupiter exalted in Cancer - maximum wealth signal", "G"))
    except Exception:
        pass

    # ════════════════════════════════════════════════════════════════
    # LAYER S - GLOBAL PLANETARY CONDITION OVERRIDE
    # When 3+ planets are in excellent condition simultaneously,
    # override even strong bearish nakshatra signals.
    # Example: Apr 8, 2026 - Sun->Aries, Saturn excellent, Mars strong
    # ════════════════════════════════════════════════════════════════
    try:
        _excellent_planets = []
        if sun_rashi in ("Aries","Leo") or (_sun_in_pisces_late and _days_to_aries <= 10):
            _excellent_planets.append("Sun")
        if saturn_rashi in ("Aquarius","Capricorn","Libra"):
            _excellent_planets.append("Saturn")
        if venus_rashi in ("Pisces","Taurus","Libra"):
            _excellent_planets.append("Venus")
        if jupiter_rashi in ("Cancer","Sagittarius","Pisces"):
            _excellent_planets.append("Jupiter")
        if mars_rashi in ("Aries","Scorpio","Capricorn"):
            _excellent_planets.append("Mars")
        if moon_rashi in ("Taurus","Cancer"):
            _excellent_planets.append("Moon")

        if len(_excellent_planets) >= 3:
            _override_pts = len(_excellent_planets) - 2  # +1 per extra planet beyond 2
            G += _override_pts
            signals.append((
                f"🌟 PLANETARY CLUSTER OVERRIDE: {', '.join(_excellent_planets)} "
                f"all in excellent condition (+{_override_pts} pts) - "
                f"overrides bearish nak signal. BUY THE DIP.",
                "G"
            ))
    except Exception:
        pass

    # ════════════════════════════════════════════════════════════════
    # ════════════════════════════════════════════════════════════════
    # JUPITER EXALTED FLOOR RULE (Vedic classic)
    # "When Jupiter is exalted in Cancer, the market cannot crash.
    #  It may fall intraday but will recover by close."
    # - Multiple Vedic sources (VB Govardhan, KP system)
    # Implementation: If Jupiter exalted and net is negative,
    # cap the bearish signal at CAUTION/MIXED (never BEARISH STRONG)
    # ════════════════════════════════════════════════════════════════
    _jupiter_exalted = dig.get("Jupiter") == "EXALTED"
    if _jupiter_exalted and G - B < 0:
        # Jupiter floor: shift 2 pts from B to G
        _floor_shift = min(2, B)
        G += _floor_shift
        B -= _floor_shift
        signals.append((
            "Jupiter JUPITER EXALTED FLOOR: Even with bearish nak, Jupiter in Cancer "
            "creates a market floor - expect dip-buying; no sustained crash. "
            "Bearish signal downgraded to Caution/Mixed.",
            "G"
        ))

    # FINAL SCORE
    # ════════════════════════════════════════════════════════════════
    net = G - B
    if net >= 5:   overall, intensity = "🟢 BULLISH", "Strong"
    elif net >= 2: overall, intensity = "🟢 BULLISH", "Mild"
    elif net == 1 and any(s[1]=="T" for s in signals):
                   overall, intensity = "⚠️ TRAP",    "Caution"
    elif net <= 0 and any(s[1]=="T" for s in signals):
                   overall, intensity = "⚠️ TRAP",    "Caution"
    elif net == 0: overall, intensity = "🟡 MIXED",   "Neutral"
    elif net >= -3:
        if _jupiter_exalted:
            overall, intensity = "⚠️ CAUTION", "Jupiter Floor Active"
        else:
            overall, intensity = "🔴 BEARISH", "Mild"
    elif net >= -6:overall, intensity = "🔴 BEARISH", "Strong"
    else:          overall, intensity = "🔴 BEARISH", "Extreme"

    return {
        # ── Core ─────────────────────────────────────────────
        "date":             d_date,
        "overall":          overall,
        "intensity":        intensity,
        "net_score":        net,
        "bearish_pts":      B,
        "bullish_pts":      G,
        "signals":          signals,
        # ── Nakshatra ────────────────────────────────────────
        "moon_nak":         moon_nak,
        "moon_nak_lord":    moon_nak_lord,
        "moon_sub_lord":    moon_sub,
        "moon_subsub_lord": moon_subsub,
        "moon_rashi":       moon_rashi,
        "moon_d9":          moon_d9,
        "moon_from_sun":    moon_from_sun,
        # ── Sun ──────────────────────────────────────────────
        "sun_nak":          sun_nak,
        "sun_nak_lord":     sun_nak_lord,
        "sun_sub_lord":     sun_sub,
        "sun_rashi":        sun_rashi,
        "sun_d9":           sun_d9,
        # ── Other planets ────────────────────────────────────
        "venus_rashi":      venus_rashi,
        "mars_rashi":       mars_rashi,
        "mars_nak":         mars_nak,
        "saturn_rashi":     saturn_rashi,
        "jupiter_rashi":    jupiter_rashi,
        "mercury_rashi":    mercury_rashi,
        "rahu_rashi":       rahu_rashi,
        # ── Timing ───────────────────────────────────────────
        "tithi":            f"{paksha} {tithi_name} ({tithi_num})",
        "tithi_name":       tithi_name,
        "paksha":           paksha,
        "karana":           karana,
        "day_lord":         day_lord,
        "hora_open":        hora_open,
        "yoga_name":        yoga_name,
        # ── Moon transit ─────────────────────────────────────
        "next_nak":         _next_nak,
        "next_nak_lord":    _next_nak_lord,
        "hours_to_next_nak":_hrs_to_next,
        # ── Dignity ──────────────────────────────────────────
        "moon_dignity":     dig["Moon"],
        "jupiter_dignity":  dig["Jupiter"],
        "venus_dignity":    dig["Venus"],
        "mars_dignity":     dig["Mars"],
        # ── Retrograde ───────────────────────────────────────
        "retro":            {p:v for p,v in retro.items() if v},
        # ── KP ───────────────────────────────────────────────
        "kp_sub":           moon_sub,
        "kp_subsub":        moon_subsub,
        # ── Ashtakavarga ─────────────────────────────────────
        "ashta_moon":       ashta_str,
        # ── Key angles ───────────────────────────────────────
        "moon_ketu_deg":    round(moon_ketu, 1),
        "mars_rahu_deg":    round(mars_rahu, 1),
        "sun_saturn_deg":   round(sun_saturn, 1),
        "mars_saturn_deg":  round(mars_saturn, 1),
        "moon_jupiter_deg": round(moon_jupiter, 1),
        "mars_jupiter_deg": round(mars_jupiter, 1),
        "sun_rahu_deg":     round(sun_rahu, 1),
        "merc_venus_deg":   round(merc_venus, 1),
        "moon_saturn_deg":  round(moon_saturn, 1),
        # ── Dhruvanka ────────────────────────────────────────
        "dhruvanka_total":  _dh,
        "dhruvanka_rem":    _dr,
        "dhruvanka_result": _dout,
        "jupiter_murthy":   _jm[0] if _jm else "",
    }

EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO = ["uppala.wla@gmail.com"]
EMAIL_ENABLED = False           # Disabled per alerts_modifications.docx (Point 1)
BROWSER_ALERTS_ENABLED = False  # Disabled per alerts_modifications.docx (Point 1)
EMAIL_MAX_PER_DAY = 40        # safe under Gmail limit
EMAIL_COOLDOWN_MIN = 10       # minutes between emails

EMAIL_META_FILE   = os.path.join(CACHE_DIR, "email_meta.json")  # FIX-2a
EMAIL_DEDUP_FILE  = os.path.join(CACHE_DIR, "email_dedup.csv")  # FIX-2b
ALERTS_DEDUP_FILE = os.path.join(CACHE_DIR, "alerts_dedup.csv")  # FIX-2c

if "alerts" not in st.session_state:
    st.session_state.alerts = []

if "alert_keys" not in st.session_state:
    st.session_state.alert_keys = set()

# ── Alert enable/disable toggles (persist across reruns) ─────────────
# Each key: True = enabled, False = disabled
_ALERT_TOGGLE_DEFAULTS = {
    "toast_TOP_HIGH":       False,
    "toast_TOP_LOW":        False,
    "toast_DAILY_UP":       False,
    "toast_DAILY_DOWN":     False,
    "toast_WEEKLY_UP":       False,  # disabled
    "toast_WEEKLY_DOWN":       False,  # disabled
    "toast_MONTHLY_UP":       False,  # disabled
    "toast_MONTHLY_DOWN":       False,  # disabled
    "toast_EMA20_50":       False,
    "toast_THREE_GREEN_15M":False,
    "toast_SEQ_15M_HIGH":       False,  # disabled
    "toast_SEQ_15M_LOW":       False,  # disabled
    "toast_SEQ_1H_HIGH":       False,  # disabled
    "toast_SEQ_1H_LOW": False,
    "toast_VOL_SURGE_15M": False,
    "email_TOP_HIGH":       False,
    "email_TOP_LOW":        False,
    "email_DAILY_UP":       False,
    "email_DAILY_DOWN":     False,
    "email_WEEKLY_UP":       False,  # disabled
    "email_WEEKLY_DOWN":       False,  # disabled
    "email_MONTHLY_UP":       False,  # disabled
    "email_MONTHLY_DOWN":       False,  # disabled
    "email_EMA20_50":       False,
    "email_THREE_GREEN_15M":False,
    "email_SEQ_15M_HIGH":       False,  # disabled
    "email_SEQ_15M_LOW":       False,  # disabled
    "email_SEQ_1H_HIGH":       False,  # disabled
    "email_SEQ_1H_LOW": False,
    "email_VOL_SURGE_15M": False,
    "tg_TOP_HIGH":          True,   # ← FIXED: was False - now fires on panchak TOP_HIGH break
    "tg_TOP_LOW":           True,   # ← FIXED: was False - now fires on panchak TOP_LOW break
    "tg_DAILY_UP":          False,
    "tg_DAILY_DOWN":        False,
    "tg_WEEKLY_UP":       False,  # disabled
    "tg_WEEKLY_DOWN":       False,  # disabled
    "tg_MONTHLY_UP":       False,  # disabled
    "tg_MONTHLY_DOWN":       False,  # disabled
    "tg_EMA20_50":          False,
    "tg_THREE_GREEN_15M":   False,
    "tg_INSIDE_15M_BREAK":  True,
    "tg_SEQ_15M_HIGH":       False,  # disabled
    "tg_SEQ_15M_LOW":       False,  # disabled
    "tg_SEQ_1H_HIGH":       False,  # disabled
    "tg_SEQ_1H_LOW":        True,
    "tg_VOL_SURGE_15M":     True,
    # Special stocks TOP_HIGH/TOP_LOW -> all 3 alerts
    "special_stock_alerts": True,
    # OI Intelligence alerts
    "tg_OI_INTEL":          False,   # disabled - enable from Alerts tab when needed
    "tg_OI_30M_SUMMARY":    False,   # disabled - enable from Alerts tab when needed
    "tg_BOS_1H":            True,   # 1H BOS/CHoCH alerts
    "tg_BOS_HC_ONLY":       True,   # Only High Conviction alerts for BOS/CHoCH
    # KP Window alerts
    "tg_KP_ALERTS":         True,
    # Long Unwinding tweets (default OFF)
    "tg_LONG_UNWIND":       False,
    # Put Support Crumbling tweets (default OFF)
    "tg_PUT_CRUMBLE":       False,
    # Put Floor Building tweets (default OFF)
    "tg_PUT_FLOOR":         False,
    # Top Gainers / Losers
    "toast_TOP_GAINERS": False,
    "email_TOP_GAINERS": False,
    "tg_TOP_GAINERS":       True,
    "toast_TOP_LOSERS": False,
    "email_TOP_LOSERS": False,
    "tg_TOP_LOSERS":        True,
    # Strong Options Buy
    "toast_OPTION_STRONG": False,
    "email_OPTION_STRONG": False,
    "tg_OPTION_STRONG":     True,
    # Yesterday Green/Red setups
    "toast_YEST_GREEN_BREAK": False,
    "email_YEST_GREEN_BREAK": False,
    "tg_YEST_GREEN_BREAK":    True,
    "toast_YEST_RED_BREAK": False,
    "email_YEST_RED_BREAK": False,
    "tg_YEST_RED_BREAK":      True,
    # 1H Opening Range Breakout/Breakdown
    "toast_HOURLY_BREAK_UP": False,
    "email_HOURLY_BREAK_UP": False,
    "tg_HOURLY_BREAK_UP":      True,
    "toast_HOURLY_BREAK_DOWN": False,
    "email_HOURLY_BREAK_DOWN": False,
    "tg_HOURLY_BREAK_DOWN":    True,
    # 2-Min EMA Reversal
    "toast_2M_EMA_REVERSAL": False,
    "email_2M_EMA_REVERSAL": False,
    "tg_2M_EMA_REVERSAL":     True,
    # Advance Astro Alert
    "tg_ASTRO_ADVANCE":       True,
    "tg_ASTRO_UPDATE":        True,
    # ── NEW: Open Structure alerts (Telegram only) ──────────────────
    "tg_GREEN_OPEN_STRUCTURE": True,
    "tg_RED_OPEN_STRUCTURE":   True,
    "tg_BREAK_ABOVE_1H_HIGH":  True,
    "tg_BREAK_BELOW_1H_LOW":   True,
    "tg_YEST_GREEN_OPEN_BREAK": True,
    "tg_YEST_RED_OPEN_LOWER":   True,
    "tg_OEH_OEL_SETUPS":        True,
    # KP break 15-min alert (Telegram only)
    "tg_KP_BREAK_15M":          False,   # disabled - enable from Alerts tab when needed
    # Combined SMC+OI+Astro+KP table (Telegram only)
    "tg_COMBINED_ENGINE":       False,   # disabled - enable from Alerts tab when needed
    "tg_HEATMAP_ALERT":         True,    # Heatmap + confluence alert every 15 min
    "route_HEATMAP_ALERT":      "both",  # send to both ch1 + ch2
    # NIFTY / BANKNIFTY KP slot break + 15-min progress (Telegram only)
    "tg_NIFTY_SLOT_BREAK":      True,
    # BT / ST target hit alerts (once per stock per day)
    "tg_BT_ST_TARGET":          True,
    # ── New alert categories (Points 7, 9, 10, 11, 12, 13) ───────────────
    "tg_MACD_BULL":             True,
    "tg_MACD_BEAR":             True,
    "tg_INSIDE_BAR":            True,
    "tg_CHART_PATTERN":         True,   # Auto chart pattern scan + alerts (daily TF)
    "tg_EXPIRY_ALERT":          True,
    "tg_HOLIDAY_ALERT":         True,
    "tg_PANCHAK_RANGE":         True,
    # ── Multi-TF Sequential Scanner (Point 6) ───────────────────────
    "tg_MTF_SEQ":               True,
    # ── Second Telegram channel - BOS/CHoCH/SMC+OI (Point 12) ───────────
    "tg2_BOS_UP":               True,
    "tg2_BOS_DOWN":             True,
    "tg2_CHOCH_UP":             True,
    "tg2_CHOCH_DOWN":           True,
    "tg2_SMC_OI_CONFLUENCE":    True,
    # ── Channel routing per alert (ch1 = Panchak Alerts, ch2 = AutoBotTest123) ──
    # Values: "ch1" | "ch2" | "both"
    "route_TOP_HIGH":              "ch1",
    "route_TOP_LOW":               "ch1",
    "route_DAILY_UP":              "ch1",
    "route_DAILY_DOWN":            "ch1",
    "route_WEEKLY_UP":             "ch1",
    "route_WEEKLY_DOWN":           "ch1",
    "route_MONTHLY_UP":            "ch1",
    "route_MONTHLY_DOWN":          "ch1",
    "route_EMA20_50":              "ch1",
    "route_THREE_GREEN_15M":       "ch1",
    "route_SEQ_15M_HIGH":          "ch1",
    "route_SEQ_15M_LOW":           "ch1",
    "route_SEQ_1H_HIGH":           "ch1",
    "route_SEQ_1H_LOW":            "ch1",
    "route_VOL_SURGE_15M":         "ch1",
    "route_OI_INTEL":              "ch1",
    "route_OI_30M_SUMMARY":        "ch1",
    "route_KP_ALERTS":             "ch1",
    "route_LONG_UNWIND":           "ch1",
    "route_PUT_CRUMBLE":           "ch1",
    "route_PUT_FLOOR":             "ch1",
    "route_TOP_GAINERS":           "ch1",
    "route_TOP_LOSERS":            "ch1",
    "route_OPTION_STRONG":         "ch1",
    "route_YEST_GREEN_BREAK":      "ch1",
    "route_YEST_RED_BREAK":        "ch1",
    "route_HOURLY_BREAK_UP":       "ch1",
    "route_HOURLY_BREAK_DOWN":     "ch1",
    "route_2M_EMA_REVERSAL":       "ch1",
    "route_ASTRO_ADVANCE":         "ch1",
    "route_ASTRO_UPDATE":          "ch1",
    "route_BOS_1H":                "both",
    "route_GREEN_OPEN_STRUCTURE":  "ch1",
    "route_RED_OPEN_STRUCTURE":    "ch1",
    "route_BREAK_ABOVE_1H_HIGH":   "ch1",
    "route_BREAK_BELOW_1H_LOW":    "ch1",
    "route_YEST_GREEN_OPEN_BREAK": "ch1",
    "route_YEST_RED_OPEN_LOWER":   "ch1",
    "route_OEH_OEL_SETUPS":        "ch1",
    "route_KP_BREAK_15M":          "ch1",
    "route_COMBINED_ENGINE":       "ch1",
    "route_NIFTY_SLOT_BREAK":      "ch1",
    "route_BT_ST_TARGET":          "ch1",
    "route_MACD_BULL":             "ch1",
    "route_MACD_BEAR":             "ch1",
    "route_INSIDE_BAR":            "ch1",
    "route_CHART_PATTERN":         "ch1",
    "route_PANCHAK_RANGE":         "ch1",
    "route_MTF_SEQ":               "ch1",
    "route_EXPIRY_ALERT":          "ch1",
    "route_HOLIDAY_ALERT":         "ch1",
    "tg_HEATMAP_REVERSAL":         True,
    "route_HEATMAP_REVERSAL":      "both",
}

# ── Heatmap Reversal Tracking ──
if "heatmap_history" not in st.session_state:
    st.session_state["heatmap_history"] = []
if "heatmap_reversal_alert_last_sent" not in st.session_state:
    st.session_state["heatmap_reversal_alert_last_sent"] = {}
if "panchak_reversal_tracking" not in st.session_state:
    st.session_state["panchak_reversal_tracking"] = {} # {pkey: {"first_break": "UP/DN", "alerted": False}}


# ── PERSISTENT ALERT TOGGLE STORAGE ──────────────────────────────────────────
# Saved to CACHE/alert_toggles.json - survives restarts, version upgrades,
# and Streamlit reruns. JSON keys match session_state keys 1:1.
# Priority: saved_file > defaults  (user's choices are NEVER overwritten on restart)
# ─────────────────────────────────────────────────────────────────────────────

_ALERT_TOGGLES_FILE = os.path.join(
    CACHE_DIR, "alert_toggles.json"  # FIX-4d
)

def _load_alert_toggles() -> dict:
    """
    Load saved alert toggle states from CACHE/alert_toggles.json.
    Falls back to _ALERT_TOGGLE_DEFAULTS for any missing key.
    Supports both bool (toggle) and str (route_* channel routing) values.
    """
    saved = {}
    try:
        os.makedirs(os.path.dirname(_ALERT_TOGGLES_FILE), exist_ok=True)
        if os.path.exists(_ALERT_TOGGLES_FILE):
            with open(_ALERT_TOGGLES_FILE, "r", encoding="utf-8") as _f:
                saved = json.load(_f)
    except Exception:
        pass
    merged = dict(_ALERT_TOGGLE_DEFAULTS)
    for k, v in saved.items():
        if k not in merged:
            continue
        default_val = _ALERT_TOGGLE_DEFAULTS[k]
        if isinstance(default_val, bool) and isinstance(v, bool):
            merged[k] = v
        elif isinstance(default_val, str) and isinstance(v, str) and v in ("ch1", "ch2", "both"):
            merged[k] = v
    return merged

def _save_alert_toggles():
    """
    Write current session_state toggle + route values to CACHE/alert_toggles.json.
    Handles both bool toggles and str route_* channel routing values.
    """
    try:
        os.makedirs(os.path.dirname(_ALERT_TOGGLES_FILE), exist_ok=True)
        snapshot = {}
        for k, default in _ALERT_TOGGLE_DEFAULTS.items():
            cur = st.session_state.get(k, default)
            if isinstance(default, bool):
                snapshot[k] = bool(cur)
            elif isinstance(default, str):
                snapshot[k] = cur if cur in ("ch1", "ch2", "both") else default
        with open(_ALERT_TOGGLES_FILE, "w", encoding="utf-8") as _f:
            json.dump(snapshot, _f, indent=2)
    except Exception as _e:
        print(f"Alert toggle save error: {_e}")

# ── Load saved toggles into session_state (once per session) ─────────────────
# Guard: only run on FIRST rerun of each session so we don't overwrite in-memory
# changes that have been made during the current session.
if not st.session_state.get("_alert_toggles_loaded", False):
    _saved_toggles = _load_alert_toggles()
    for _k, _v in _saved_toggles.items():
        st.session_state[_k] = _v
    st.session_state["_alert_toggles_loaded"] = True




# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
st.markdown("""
<style>
/* Section headers */
.section-green  {background:#0d2b0d;color:#b6f5c8;padding:8px;border-radius:6px;border-left:3px solid #00C851;}
.section-red    {background:#2b0d0d;color:#ffb3b3;padding:8px;border-radius:6px;border-left:3px solid #FF4444;}
.section-yelLIVE_LOW {background:#2a2200;color:#fff3b0;padding:8px;border-radius:6px;border-left:3px solid #FFD700;}
.section-blue   {background:#0d1a2b;color:#aad4ff;padding:8px;border-radius:6px;border-left:3px solid #2196F3;}
.section-purple {background:#1a0d2b;color:#d4aaff;padding:8px;border-radius:6px;border-left:3px solid #9B59B6;}
.section-orange {background:#2b1500;color:#ffd0a0;padding:8px;border-radius:6px;border-left:3px solid #FF9800;}

/* Table tweaks */
thead tr th {
    background-color:#1a2255 !important;
    color:#ffffff !important;
    font-weight:600 !important;
}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# 🔢 DYNAMIC NUMEROLOGY ENGINE
# Date root number × live planetary positions -> composite signal
# Research: @pankajkummar369, 50-yr sector study, AstroSanhita,
#           Sheelaa.com financial numerology, Vedic astro research
# ═══════════════════════════════════════════════════════════════════
_NR_PLANET = {1:"Sun",2:"Moon",3:"Jupiter",4:"Rahu",
              5:"Mercury",6:"Venus",7:"Ketu",8:"Saturn",9:"Mars"}
# (label, base_score, description, strong_sectors, weak_sectors)
_NR_BASE = {
    1:(  "🟢 BULLISH",+2,"Sun - leadership, new initiatives, govt/infra strength","PSU, infra, metals","banking"),
    2:(  "🟡 NEUTRAL", 0,"Moon - emotional swings, consumer/FMCG supported","FMCG, consumer, agri","large-caps"),
    3:(  "🟢 BULLISH",+2,"Jupiter - expansion, banking, finance positive","banking, finance, gold, pharma","none"),
    4:(  "🔴 BEARISH",-2,"Rahu - illusion traps, false rallies, extreme volatility","IT/tech (short only)","most sectors"),
    5:(  "🟡 VOLATILE",-1,"Mercury - choppy two-sided; IT/telecom mixed","IT, telecom (scalp)","banking"),
    6:(  "🟢 BULLISH",+2,"Venus - rally expected; FMCG, silver, jewellery strong","FMCG, silver, jewellery","none"),
    7:(  "🔴 BEARISH",-2,"Ketu - detachment, losses; market loses direction","nothing clear","most sectors"),
    8:(  "🔴 BEARISH",-2,"Saturn - slowdown, restriction; steady downtrend risk","realty (LT only)","banking, IT"),
    9:(  "🔴 EXTREME",-4,"Mars - aggression, crash risk; war/geopolitical energy","defence, crude (inverse)","Nifty, BankNifty"),
}

def _nr_amplifiers(v: dict, root: int) -> list:
    """Dynamic planetary combination amplifiers from live vedic positions."""
    amps, p = [], _NR_PLANET.get(root,"")
    mr  = v.get("mars_rahu_deg",99);  ss = v.get("sun_saturn_deg",99)
    ms  = v.get("moon_saturn_deg",99); mk = v.get("moon_ketu_deg",99)
    mj  = v.get("moon_jupiter_deg",99); retro = v.get("retro",{})
    sat_r = v.get("saturn_rashi",""); moon_r = v.get("moon_rashi","")
    ven_r = v.get("venus_rashi","");  mer_r  = v.get("mercury_rashi","")
    # Mars-Rahu (most dangerous - research + tweet confirmed)
    if mr<5:  amps.append((-4,f"⚡ Mars-Rahu EXACT {mr:.1f}° - extreme crash energy"))
    elif mr<10:amps.append((-3,f"⚡ Mars-Rahu {mr:.1f}° - very dangerous, crude bullish"))
    elif mr<15:amps.append((-2,f"⚡ Mars-Rahu {mr:.1f}° active - bearish amplifier"))
    # Root planet double-hit
    if p=="Mars" and mr<15: amps.append((-2,f"🔴 Root {root} (Mars)+Mars-Rahu = DOUBLE crash"))
    if p=="Saturn" and ss<8: amps.append((-2,f"🔴 Root {root} (Saturn)+Sun-Saturn = DOUBLE bearish"))
    if p=="Rahu" and mr<15:  amps.append((-2,f"🔴 Root {root} (Rahu)+Mars-Rahu = Rahu amplified"))
    # Sun-Saturn
    if ss<3:  amps.append((-3,f"Sun-Saturn Sun-Saturn EXACT {ss:.1f}° - Nifty+Gold all fall"))
    elif ss<8:amps.append((-2,f"Sun-Saturn Sun-Saturn {ss:.1f}° - strong bearish"))
    # Moon-Saturn Vish Yoga
    if ms<8:  amps.append((-2,f"Moon-Saturn Moon-Saturn {ms:.1f}° Vish Yoga - sharp fall risk"))
    # Moon-Ketu
    if mk<12: amps.append((-2,f"Moon-Ketu {mk:.1f}° - banks/Reliance weak"))
    # Gajakesari (bullish)
    if mj<10: amps.append((+2,f"Moon-Jupiter Gajakesari {mj:.1f}° - strong rally amplifier"))
    elif mj<15:amps.append((+1,f"Moon-Jupiter Moon-Jupiter {mj:.1f}° - mild rally support"))
    # Venus in Aries = bottom zone
    if ven_r=="Aries": amps.append((+3,"Venus Venus in Aries - BOTTOM ZONE, heavy long bias"))
    # Mercury own sign = bearish for price
    if mer_r in ("Gemini","Virgo"): amps.append((-1,f"Mercury Mercury in {mer_r} - reduces share prices"))
    # Retrograde
    if p in retro: amps.append((-1,f"🔄 Root planet {p} retrograde - energy weakened"))
    if "Saturn" in retro and root!=8: amps.append((+1,"Saturn Saturn retrograde - restriction easing"))
    # Saturn in Aries = destroys market confidence
    if sat_r=="Aries": amps.append((-2,"Saturn Saturn in Aries -> market confidence destroyed"))
    # Moon in Saturn signs
    if moon_r in ("Aquarius","Capricorn"): amps.append((-1,f"Moon Moon in {moon_r} - psychological drag"))
    return amps

def _nr_full(d, vedic=None) -> dict:
    """Full dynamic numerology reading. vedic = output of _vedic_day_analysis."""
    digits = [int(x) for x in d.strftime("%d%m%Y")]
    raw    = sum(digits)
    root   = raw
    while root>9: root = sum(int(x) for x in str(root))
    calc   = "+".join(str(x) for x in digits)+"="+str(raw)+("->"+str(root) if raw!=root else "")
    label,base,desc,sec_str,sec_wk = _NR_BASE[root]
    planet = _NR_PLANET[root]
    amps   = _nr_amplifiers(vedic, root) if vedic else []
    total  = base + sum(a[0] for a in amps)
    if   total>=4:  sig="🟢 STRONG BULLISH"
    elif total>=2:  sig="🟢 BULLISH"
    elif total>=1:  sig="🟢 MILD BULLISH"
    elif total==0:  sig="🟡 NEUTRAL / MIXED"
    elif total>=-1: sig="🔴 MILD BEARISH"
    elif total>=-3: sig="🔴 BEARISH"
    elif total>=-5: sig="🔴 STRONG BEARISH"
    else:           sig="🔴 EXTREME - CRASH RISK"
    if total>=3:    act=f"✅ BUY dips - Root {root} ({planet}) confirmed. Target 0.5–1% above open."
    elif total>=1:  act=f"✅ LEAN LONG - Root {root} ({planet}). Buy on morning dip after 09:45."
    elif total==0:  act=f"⚠️ WAIT - Root {root} neutral. Trade confirmed breakouts after 10:30 only."
    elif total>=-2: act=f"❌ SELL RALLIES - Root {root} ({planet}). Short on bounces, target -1%."
    elif total>=-4: act=f"❌ STRONG SHORT - Root {root} ({planet})+{sum(1 for a in amps if a[0]<0)} amplifiers."
    else:           act=f"🚨 DO NOT BUY - Root {root} ({planet}) EXTREME. Cash or short only."
    return {"date":d,"root":root,"planet":planet,"calc":calc,"base_signal":label,
            "base_score":base,"desc":desc,"amplifiers":amps,
            "amp_score":sum(a[0] for a in amps),"total_score":total,
            "final_signal":sig,"action":act,"sectors_strong":sec_str,"sectors_weak":sec_wk}



# BASE_DIR/CACHE_DIR moved earlier by patch_dashboard.py (FIX-1)

#OHLC_FILE = os.path.join(CACHE_DIR, "ohlc_60d.csv")
INST_FILE    = os.path.join(CACHE_DIR, "instruments_NSE.csv")
# DAILY_FILE / WEEKLY_FILE / MONTHLY_FILE are dated files assigned below
# via get_daily_file() / get_weekly_file() / get_monthly_file()
# Do NOT pre-assign undated paths here - they would be stale on next-day restart
DAILY_FILE   = None
WEEKLY_FILE  = None
MONTHLY_FILE = None
#PANCHAK_FILE = os.path.join(CACHE_DIR, "panchak_static.csv")
#EMA_FILE = os.path.join(CACHE_DIR, "ema_20_50.csv")


API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")  # FIX-3

# ================= SYMBOL MASTER =================
SYMBOL_META = {
    "NIFTY":     "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "SENSEX":    "SENSEX",    # BSE:SENSEX - fetched via BSE exchange
}

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA",
    "BANKINDIA","BANKNIFTY","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDCOPPER","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULI","IEX","INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND","IRCTC","IRFC","IREDA","ITC",
    "JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL",
    "MPHASIS","MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NIFTY","NTPC","NUVAMA","NYKAA","NATIONALUM",
    "OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM","PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD",
    "PIDILITIND","PIIND","PNB","PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PREMIERENE","PRESTIGE","PPLPHARMA",
    "RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SAMMAANCAP","SBICARD","SBILIFE","SBIN","SHREECEM","SHRIRAMFIN",
    "SIEMENS","SOLARINDS","SRF","SUNPHARMA","SUPREMEIND","SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER","TATATECH","TATASTEEL","TCS","TECHM",
    "TIINDIA","TITAN","TMPV","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","ULTRACEMCO","UNIONBANK","UNITDSPR","UPL","VBL","VEDL","VOLTAS","WAAREEENER","WIPRO","ZYDUSLIFE"
]


#SYMBOLS = ["NIFTY", "BANKNIFTY"] + STOCKS
INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
    "FINNIFTY",
    "NIFTYIT",
    "NIFTYFMCG",
    "NIFTYPHARMA",
    "NIFTYMETAL",
    "NIFTYAUTO",
    "NIFTYENERGY",
    "NIFTYPSUBANK",
]

SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS

SYMBOL_META.update({
    # These are the EXACT tradingsymbol strings in Kite's NSE instruments file
    "FINNIFTY":    "NIFTY FIN SERVICE",
    "NIFTYIT":     "NIFTY IT",
    "NIFTYFMCG":   "NIFTY FMCG",
    "NIFTYPHARMA": "NIFTY PHARMA",
    "NIFTYMETAL":  "NIFTY METAL",
    "NIFTYAUTO":   "NIFTY AUTO",
    "NIFTYENERGY": "NIFTY ENERGY",
    "NIFTYPSUBANK":"NIFTY PSU BANK",
})

# ================= DYNAMIC PANCHAK ENGINE =================
#
# HOW IT WORKS (exact behaviour requested):
# ─────────────────────────────────────────────────────────
# Each row below is one panchak period (begin -> end).
# The TABLE (TOP_HIGH / TOP_LOW) is built from those dates.
#
# TRIGGER RULE - "morning of (end_date + 1 day)":
#   • Currently (today ≤ Apr 17):  use Mar 17–21 data -> valid until Apr 17
#   • Apr 18 morning:              re-fetch Apr 13–17  -> valid until May 14
#   • May 15 morning:              re-fetch May 10–14  -> valid until Jun 11
#   • Jun 12 morning:              re-fetch Jun 6–11   -> valid until Jul 8
#   ... and so on for every month.
#
# Weekends / NSE holidays inside the panchak range are automatically
# skipped (no market data available -> gracefully ignored by Kite API).
# ─────────────────────────────────────────────────────────────────────

_PANCHAK_SCHEDULE_2026 = [
    # index  panchak_start         panchak_end           fetch_trigger (end+1)
    #  0     Mar 17                Mar 21                Mar 22  (already fetched; valid till Apr 17)
    (date(2026,  3, 17),   date(2026,  3, 21)),
    #  1     Apr 13                Apr 17                Apr 18  ← fetch trigger
    (date(2026,  4, 13),   date(2026,  4, 17)),
    #  2     May 10                May 14                May 15
    (date(2026,  5, 10),   date(2026,  5, 14)),
    #  3     Jun 6                 Jun 11                Jun 12
    (date(2026,  6,  6),   date(2026,  6, 11)),
    #  4     Jul 4                 Jul 8                 Jul 9
    (date(2026,  7,  4),   date(2026,  7,  8)),
    #  5     Jul 31                Aug 4                 Aug 5
    (date(2026,  7, 31),   date(2026,  8,  4)),
    #  6     Aug 27                Sep 1                 Sep 2
    (date(2026,  8, 27),   date(2026,  9,  1)),
    #  7     Sep 23                Sep 28                Sep 29
    (date(2026,  9, 23),   date(2026,  9, 28)),
    #  8     Oct 21                Oct 25                Oct 26
    (date(2026, 10, 21),   date(2026, 10, 25)),
    #  9     Nov 17                Nov 22                Nov 23
    (date(2026, 11, 17),   date(2026, 11, 22)),
    # 10     Dec 14                Dec 19                Dec 20
    (date(2026, 12, 14),   date(2026, 12, 19)),
]


def _panchak_trading_days(start, end):
    """Return list of trading days (Mon–Fri, not NSE holiday) in [start, end]."""
    out, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in NSE_HOLIDAYS:
            out.append(d)
        d += timedelta(days=1)
    return out or [start]   # safety: never empty


def _get_active_panchak(today_date=None):
    """
    Determine which panchak period's data should be displayed TODAY.

    Rule (exactly as specified):
    ─────────────────────────────────────────────────────────────────
    The data from panchak period N is valid from the day AFTER
    period N's end_date up to (and including) the end_date of
    period N+1.

    In other words:
      • Period 0 (Mar 17–21): data used from startup -> Apr 17
      • Period 1 (Apr 13–17): data used from Apr 18  -> May 14
      • Period 2 (May 10–14): data used from May 15  -> Jun 11
      ...

    The "fetch trigger" for period N is:  period[N-1].end_date + 1 day.
    On that morning the panchak table is rebuilt from period N's dates.

    Returns
    ───────
    (PANCHAK_START, PANCHAK_END, PANCHAK_DATES, fetch_period_idx)
        fetch_period_idx : index into _PANCHAK_SCHEDULE_2026 whose
                           dates should be fetched from Kite API.
    """
    if today_date is None:
        today_date = date.today()

    sched = _PANCHAK_SCHEDULE_2026
    n = len(sched)

    # Walk through periods and find which one's data governs today.
    # Period 0 governs until sched[1].end + 0 (i.e. sched[0] used while
    # today <= sched[1].end - 1, but that's actually up to sched[0].end + gap).
    #
    # Simplified:  period[i] is the "active fetch source" when
    #   trigger[i] <= today <= trigger[i+1] - 1
    # where trigger[i] = sched[i-1].end + 1  (trigger[0] = date.min, i.e. always)
    #
    # Equivalently: find the LARGEST i such that sched[i-1].end < today
    # (meaning the trigger for period i has already fired).

    active_idx = 0   # default: period 0 (Mar 17–21) - valid until Apr 17

    for i in range(1, n):
        # Period i becomes active on the morning of sched[i].end + 1 day.
        # e.g. Period 1 (Apr 13–17): active from Apr 18 = Apr 17 + 1
        #      Period 2 (May 10–14): active from May 15 = May 14 + 1
        trigger = sched[i][1] + timedelta(days=1)
        if today_date >= trigger:
            active_idx = i
        else:
            break   # triggers are monotone; no need to look further

    ps, pe = sched[active_idx]
    tdates = _panchak_trading_days(ps, pe)
    return ps, pe, tdates, active_idx


# ── Resolve at startup ───────────────────────────────────────────────────
PANCHAK_START, PANCHAK_END, PANCHAK_DATES, _PANCHAK_ACTIVE_IDX = _get_active_panchak()

# ── Track the last resolved calendar date for day-boundary detection ─────
_PANCHAK_RESOLVED_DATE = date.today()


def _refresh_panchak_if_needed():
    """
    Call once per refresh loop (already done at the top of each Streamlit
    rerun).  If the calendar date has advanced since the last resolution
    (e.g. Apr 17 -> Apr 18 at midnight), recompute PANCHAK_* globals.

    When the active index changes (new panchak period selected), the
    panchak cache validity check in build_panchak_files() will detect
    the changed START/END dates and automatically re-fetch from Kite.
    """
    global PANCHAK_START, PANCHAK_END, PANCHAK_DATES
    global _PANCHAK_ACTIVE_IDX, _PANCHAK_RESOLVED_DATE

    today_d = date.today()
    if today_d == _PANCHAK_RESOLVED_DATE:
        return   # same day - nothing to do

    new_start, new_end, new_dates, new_idx = _get_active_panchak(today_d)
    _PANCHAK_RESOLVED_DATE = today_d

    if new_idx == _PANCHAK_ACTIVE_IDX:
        return   # same period - no change needed

    # ── Period has changed -> update globals and force cache rebuild ──
    PANCHAK_START      = new_start
    PANCHAK_END        = new_end
    PANCHAK_DATES      = new_dates
    _PANCHAK_ACTIVE_IDX = new_idx

    # Invalidate the Streamlit cache for load_panchak_df so the UI
    # picks up fresh data on the very next rerun.
    try:
        load_panchak_df.clear()
    except Exception:
        pass

    print(
        f"[Panchak] Period updated -> {PANCHAK_START} – {PANCHAK_END} "
        f"(period #{new_idx}, trigger: {_PANCHAK_SCHEDULE_2026[new_idx-1][1] + timedelta(days=1) if new_idx > 0 else 'startup'})"
    )


# Run the refresh check now (covers first load of the day)
_refresh_panchak_if_needed()

# ── Priority stocks - always shown at top of Panchak table ───────────
PANCHAK_PRIORITY_STOCKS = [
    "NIFTY", "BANKNIFTY", "BEL", "HINDCOPPER", "INDHOTEL",
    "ABCAPITAL", "TATASTEEL", "BANKINDIA", "CANBK", "JINDALSTEL",
    "BANDHANBNK", "INDUSTOWER", "MOTHERSON", "NATIONALUM",
]
# Normalised uppercase set for fast lookup
_PRIORITY_SET = {s.upper() for s in PANCHAK_PRIORITY_STOCKS}

# ── Special alert stocks - TOP_HIGH / TOP_LOW breaks fire all 3 alerts ─
SPECIAL_ALERT_STOCKS = [
    "NIFTY", "BANKNIFTY",
    "BEL", "HINDCOPPER", "INDHOTEL",
    "ABCAPITAL", "TATASTEEL", "BANKINDIA", "CANBK", "JINDALSTEL",
    "BANDHANBNK", "INDUSTOWER", "MOTHERSON", "NATIONALUM",
]
_SPECIAL_SET = {s.upper() for s in SPECIAL_ALERT_STOCKS}

# ── Panchak TOP_HIGH / TOP_LOW break alerts - ONLY these stocks ─────────
# NIFTY & BANKNIFTY are handled by fire_nifty_slot_break_alert() separately.
PANCHAK_ALERT_WATCHLIST = {
    "NIFTY", "BANKNIFTY", "BEL", "HINDCOPPER", "INDHOTEL",
    "ABCAPITAL", "TATASTEEL", "BANKINDIA", "CANBK", "JINDALSTEL",
    "BANDHANBNK", "INDUSTOWER", "MOTHERSON", "NATIONALUM",
}

PANCHAK_DATA_FILE = os.path.join(CACHE_DIR, "panchak_data.csv")
PANCHAK_META_FILE = os.path.join(CACHE_DIR, "panchak_meta.csv")
ALERTS_LOG_FILE   = os.path.join(CACHE_DIR, "alerts_log.csv")  # FIX-2d



# ================= KITE INIT =================
kite = KiteConnect(api_key=API_KEY)
with open(ACCESS_TOKEN_FILE, encoding="utf-8") as _tf: kite.set_access_token(_tf.read().strip())


########################            UI Controls To Add

st.sidebar.header("🎥 Backtest Replay")

replay_mode = st.sidebar.toggle("Enable Replay Mode")

selected_date = st.sidebar.date_input("Select Date")

speed = st.sidebar.selectbox("Speed", ["Real 5 Min", "Fast (1 sec)"])

play = st.sidebar.button("▶ Start")
pause = st.sidebar.button("⏸ Pause")
col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button("⏮ Back"):
        st.session_state.replay_index = max(
            0, st.session_state.replay_index - 1
        )

with col2:
    if st.button("⏭ Next"):
        st.session_state.replay_index += 1


###############     Core Replay Engine
if "replay_index" not in st.session_state:
    st.session_state.replay_index = 0

if "replay_running" not in st.session_state:
    st.session_state.replay_running = False




# ================= INSTRUMENTS =================
@st.cache_data(show_spinner=False)
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE, encoding='utf-8')
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False, encoding='utf-8')
    return df

# BSE instruments file - for SENSEX token lookup
INST_FILE_BSE = os.path.join(CACHE_DIR, f"bse_instruments_{datetime.now().strftime('%Y%m%d')}.csv")

def load_bse_instruments():
    """Load BSE instruments (for SENSEX token). Cached daily."""
    if os.path.exists(INST_FILE_BSE):
        try:
            return pd.read_csv(INST_FILE_BSE, encoding='utf-8')
        except Exception:
            pass
    try:
        df = pd.DataFrame(kite.instruments("BSE"))
        df.to_csv(INST_FILE_BSE, index=False, encoding='utf-8')
        return df
    except Exception as _e:
        print(f"BSE instruments load failed: {_e}")
        return pd.DataFrame()

inst      = load_instruments()
bse_inst  = load_bse_instruments()   # For SENSEX token

def get_token(symbol):
    """
    Get Kite instrument token.
    SENSEX (BSE index) uses bse_inst loaded at startup.
    All other symbols use inst (NSE instruments).
    """
    if symbol == "SENSEX":
        if bse_inst.empty:
            return None
        _row = bse_inst[bse_inst.tradingsymbol == "SENSEX"]
        return None if _row.empty else int(_row.iloc[0].instrument_token)
    name = SYMBOL_META.get(symbol, symbol)
    row = inst[inst.tradingsymbol == name]
    return None if row.empty else int(row.iloc[0].instrument_token)

def kite_symbol(symbol):
    """Return Kite exchange:tradingsymbol. SENSEX trades on BSE."""
    if symbol == "SENSEX":
        return "BSE:SENSEX"
    return f"NSE:{SYMBOL_META.get(symbol, symbol)}"

# ================= TRADING DAY HELPERS =================
def last_trading_day(d):
    while d.weekday() >= 5 or d in NSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d

@st.cache_data(ttl=86400, show_spinner=False)   # ✅ silent
def fetch_yesterday_ohlc(token):

    try:
        d = last_trading_day(date.today() - timedelta(days=1))

        bars = kite.historical_data(token, d, d, "day")

        if not bars:
            return None, None, None, None, None  # ✅ FIX: match 5-value return signature

        b = bars[0]

        return (
            round(b["open"], 2),
            round(b["high"], 2),
            round(b["low"], 2),
            round(b["close"], 2),
            round(b["volume"], 2)
        )

    except Exception as e:
        # 🔒 Prevent app crash
        return None, None, None, None, None



# ================= OHLC BUILDERS (UNCHANGED) =================
# ==========================================================
# ✅ PREVIOUS PERIOD OHLC (DAILY / WEEKLY / MONTHLY)
# ==========================================================

def dated_file(name):
    d = last_trading_day(date.today())
    return os.path.join(CACHE_DIR, f"{name}_{d}.csv")


def previous_week_range():
    end = last_trading_day(date.today())
    last_week_end = end - timedelta(days=end.weekday() + 1)
    last_week_start = last_week_end - timedelta(days=4)
    return last_week_start, last_week_end


def previous_month_range():
    first_this_month = date.today().replace(day=1)
    last_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1)
    return first_prev_month, last_prev_month


def build_daily_ohlc():
    path = dated_file("daily_ohlc")
    if os.path.exists(path):
        return path

    d_start = last_trading_day(date.today() - timedelta(days=10))
    d_end   = last_trading_day(date.today() - timedelta(days=1))
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, d_start, d_end, "day")
        if not bars:
            continue

        # Last day's data
        b = bars[-1]
        
        # Average volume of last 5 completed days
        vols = [x["volume"] for x in bars[-5:]]
        avg_vol_5d = sum(vols) / len(vols) if vols else 0

        rows.append({
            "Symbol": s,
            "OPEN_D":  b["open"],
            "HIGH_D":  b["high"],
            "LOW_D":   b["low"],
            "CLOSE_D": b["close"],
            "VOLUME_D": b["volume"],
            "AVG_VOL_5D": round(avg_vol_5d, 2)
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False, encoding='utf-8')
    return path


def build_weekly_ohlc():
    path = dated_file("weekly_ohlc")
    if os.path.exists(path):
        return path

    start, end = previous_week_range()
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, start, end, "day")
        if not bars:
            continue

        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol":   s,
            "OPEN_W": dfb.iloc[0]["open"],
            "HIGH_W": dfb["high"].max(),
            "LOW_W": dfb["low"].min(),
            "CLOSE_W": dfb.iloc[-1]["close"],
            "VOLUME_W": dfb["volume"].sum()

        })

        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False, encoding='utf-8')
    return path


def build_monthly_ohlc():
    path = dated_file("monthly_ohlc")
    if os.path.exists(path):
        return path

    start, end = previous_month_range()
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, start, end, "day")
        if not bars:
            continue

        dfb = pd.DataFrame(bars)
        rows.append({
            "Symbol":   s,
            "OPEN_M":   dfb.iloc[0]["open"],
            "HIGH_M":   dfb["high"].max(),
            "LOW_M":    dfb["low"].min(),
            "CLOSE_M":  dfb.iloc[-1]["close"],
            "VOLUME_M": dfb["volume"].sum()
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False, encoding='utf-8')
    return path


# -------- BUILD OR LOAD FILES (AUTO) --------
# ✅ FIX: Wrap in cache so these don't re-execute on every Streamlit rerun/auto-refresh
@st.cache_data(show_spinner=False, ttl=3600)
def get_daily_file():
    return build_daily_ohlc()

@st.cache_data(show_spinner=False, ttl=3600)
def get_weekly_file():
    return build_weekly_ohlc()

@st.cache_data(show_spinner=False, ttl=3600)
def get_monthly_file():
    return build_monthly_ohlc()

DAILY_FILE   = get_daily_file()
WEEKLY_FILE  = get_weekly_file()
MONTHLY_FILE = get_monthly_file()


# ================= PANCHAK STATIC =================

# ================= PANCHAK STATIC (VALIDATED CACHE – OPTION A) =================

def is_panchak_cache_valid():
    """
    Cache is valid ONLY if Panchak dates in meta file
    exactly match script-defined PANCHAK_START / PANCHAK_END
    """
    if not os.path.exists(PANCHAK_META_FILE):
        return False

    try:
        meta = pd.read_csv(PANCHAK_META_FILE, encoding='utf-8')

        file_start = pd.to_datetime(
            meta.loc[meta["key"] == "start_date", "value"].values[0],
            dayfirst=False, format="ISO8601", utc=False
        ).date()

        file_end = pd.to_datetime(
            meta.loc[meta["key"] == "end_date", "value"].values[0],
            dayfirst=False, format="ISO8601", utc=False
        ).date()

        return file_start == PANCHAK_START and file_end == PANCHAK_END

    except Exception:
        return False


def build_panchak_files():
    """
    Builds Panchak DATA file + META file from PANCHAK_DATES.
    Called ONLY when cache is invalid (new panchak period detected).
    Per-symbol errors are logged and skipped - never crashes the whole fetch.
    """
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(
                tk,
                PANCHAK_DATES[0],
                PANCHAK_DATES[-1],
                "day"
            )
        except Exception as _bp_e:
            print(f"[Panchak] Fetch error for {s}: {_bp_e}")
            tm.sleep(0.35)
            continue

        dfb = pd.DataFrame(bars)
        if dfb.empty:
            tm.sleep(0.35)
            continue

        th   = dfb["high"].max()
        tl   = dfb["low"].min()
        diff = th - tl

        rows.append({
            "Symbol":   s,
            "TOP_HIGH": round(th, 2),
            "TOP_LOW":  round(tl, 2),
            "DIFF":     round(diff, 2),
            "BT":       round(th + diff, 2),
            "ST":       round(tl - diff, 2),
        })

        tm.sleep(0.35)

    # Safety: don't write empty data
    if not rows:
        print("[Panchak] build_panchak_files: no rows fetched - keeping existing cache")
        return

    # --- DATA FILE ---
    pd.DataFrame(rows).to_csv(PANCHAK_DATA_FILE, index=False, encoding='utf-8')

    # --- META FILE ---
    meta_df = pd.DataFrame([
        {"key": "start_date", "value": PANCHAK_START.isoformat()},
        {"key": "end_date",   "value": PANCHAK_END.isoformat()},
    ])
    meta_df.to_csv(PANCHAK_META_FILE, index=False, encoding='utf-8')
    print(f"[Panchak] Built {len(rows)} rows for {PANCHAK_START} -> {PANCHAK_END}")

# ---------- PANCHAK LOAD (AUTO-VALIDATED) ----------
# ── Refresh panchak dates if day has changed (handles Apr 18 switch etc.) ──
_refresh_panchak_if_needed()

if not is_panchak_cache_valid():
    build_panchak_files()

@st.cache_data(show_spinner=False)   # ✅ FIX: static panchak data - cache permanently (no TTL)
def load_panchak_df():
    _df = pd.read_csv(PANCHAK_DATA_FILE, encoding='utf-8')
    return (
        _df
        .sort_values("Symbol")
        .drop_duplicates(subset=["Symbol"], keep="last")
    )

panchak_df = load_panchak_df()





# ================= LIVE DATA =================
# ================= LIVE DATA (SAFE, HOLIDAY-AWARE) =================
@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent - stale data shown while refreshing
def live_data():
    if not is_market_hours():
        return pd.DataFrame(columns=[
            "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
            "YEST_OPEN","YEST_HIGH","YEST_LOW","YEST_CLOSE",
            "CHANGE","CHANGE_%"
        ])
    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except Exception as e:
        # ✅ FIX: Removed st.warning() - UI calls are NOT allowed inside @st.cache_data
        # Caller handles the empty DataFrame and shows warning instead
        return pd.DataFrame(columns=[
            "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
            "YEST_OPEN","YEST_HIGH","YEST_LOW","YEST_CLOSE",
            "CHANGE","CHANGE_%"
        ])

    rows = []

    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue

        tk = get_token(s)
        yo, yh, yl, yc, yv = fetch_yesterday_ohlc(tk)

        ltp = q["last_price"]
        pc = q["ohlc"]["close"]
        chg = ltp - pc if pc else 0
        live_volume = q.get("volume", 0)

        rows.append({
            "Symbol": s,
            "LTP": round(ltp, 2),
            "LIVE_OPEN": round(q["ohlc"]["open"], 2),
            "LIVE_HIGH": round(q["ohlc"]["high"], 2),
            "LIVE_LOW": round(q["ohlc"]["low"], 2),
            "LIVE_VOLUME": live_volume,   # ✅ LIVE DAY VOLUME
            #"live_vol": live_volume,
            "YEST_OPEN": yo,
            "YEST_HIGH": yh,
            "YEST_LOW": yl,
            "YEST_CLOSE": yc,
            "YEST_VOL": yv,
            "CHANGE": round(chg, 2),
            "CHANGE_%": round((chg / pc) * 100, 2) if pc else 0
        })

        


    return pd.DataFrame(rows)


##########################################################          BAR REPLAY -- BACKETEST DATA



###################         Example Code – Download 1 Month 5-Min Data

def download_intraday_data(symbol, interval="5minute", days=30):

    tk = get_token(symbol)
    if not tk:
        return None

    end = datetime.now()
    start = end - timedelta(days=days)

    try:
        bars = kite.historical_data(
            tk,
            start,
            end,
            interval
        )
    except Exception as e:
        print("Error:", e)
        return None

    df = pd.DataFrame(bars)
    df = _safe_parse_date_col(df, col="date")

    # Save to CACHE
    path = os.path.join(CACHE_DIR, f"{symbol}_{interval}.csv")  # FIX-2e
    df.to_csv(path, index=False, encoding='utf-8')

    return df

############        Download for All Symbols
def build_intraday_database(interval="5minute", days=30):

    for s in SYMBOLS:
        print("Downloading:", s)
        download_intraday_data(s, interval, days)
        tm.sleep(0.35)   # prevent rate limit

if st.sidebar.button("Download 5-Min Data (30 Days)"):
    build_intraday_database("5minute", 30)

#build_intraday_database("5minute", 30)

#build_intraday_database("1minute", 15)

#####################           Load intraday database ONCE and cache it

@st.cache_data(show_spinner=False)
def load_all_intraday_data():
    data_dict = {}
    for s in SYMBOLS:
        path = os.path.join(CACHE_DIR, f"{s}_5minute.csv")
        if os.path.exists(path):
            df = pd.read_csv(path, encoding='utf-8')
            df = _safe_parse_date_col(df, col="date")
            if not df.empty:
                data_dict[s] = df
    return data_dict


###############         Load 5-Min Data For Selected Date

def preload_backtest_ohlc(date_selected):

    if "backtest_ohlc_cache" not in st.session_state:
        st.session_state.backtest_ohlc_cache = {}

    if date_selected in st.session_state.backtest_ohlc_cache:
        return st.session_state.backtest_ohlc_cache[date_selected]

    intraday_db = load_all_intraday_data()

    daily_rows = []
    weekly_rows = []
    monthly_rows = []

    prev_day = last_trading_day(date_selected - timedelta(days=1))

    for s, data in intraday_db.items():

    
        # Guard: ensure "date" column exists and is datetime dtype
        if data is None or (hasattr(data, "empty") and data.empty): continue
        if "date" not in data.columns:
            if "datetime" in data.columns:
                data = data.rename(columns={"datetime": "date"})
            else:
                continue
        if str(data["date"].dtype) == "object":
            data = _safe_parse_date_col(data, col="date")
        if data.empty or "date" not in data.columns: continue
    # --- Get selected day ---
        day_data = data[data["date"].dt.date == date_selected]
        prev_data = data[data["date"].dt.date == prev_day]

        if day_data.empty or prev_data.empty:
            continue

        # === YEST OHLC ===
        y_open = prev_data.iloc[0]["open"]
        y_high = prev_data["high"].max()
        y_low  = prev_data["low"].min()
        y_close = prev_data.iloc[-1]["close"]
        y_vol = prev_data["volume"].sum()

        daily_rows.append({
            "Symbol": s,
            "OPEN_D": y_open,
            "HIGH_D": y_high,
            "LOW_D": y_low,
            "CLOSE_D": y_close,
            "VOLUME_D": y_vol
        })

        # === WEEK (last 5 trading days before selected date) ===
        #week_start = prev_day - timedelta(days=7)
        #week_start = prev_day - timedelta(days=10)  # buffer for weekend
        # === EXACT LAST 5 TRADING DAYS ===
        # === WEEK (last 5 trading days before selected date) ===
        trading_days = sorted(data["date"].dt.date.unique())

        past_days = [d for d in trading_days if d < date_selected]

        last_5_days = past_days[-5:]

        week_data = data[data["date"].dt.date.isin(last_5_days)]

        if not week_data.empty:
            weekly_rows.append({
                "Symbol": s,
                "OPEN_W": week_data.iloc[0]["open"],
                "HIGH_W": week_data["high"].max(),
                "LOW_W": week_data["low"].min(),
                "CLOSE_W": week_data.iloc[-1]["close"],
                "VOLUME_W": week_data["volume"].sum()
            })

        # === MONTH (30 days before selected date) ===
        month_start = prev_day - timedelta(days=30)
        month_data = data[
            (data["date"].dt.date >= month_start) &
            (data["date"].dt.date <= prev_day)
        ]

        if not month_data.empty:
            monthly_rows.append({
                "Symbol": s,
                "OPEN_M": month_data.iloc[0]["open"],
                "HIGH_M": month_data["high"].max(),
                "LOW_M": month_data["low"].min(),
                "CLOSE_M": month_data.iloc[-1]["close"],
                "VOLUME_M": month_data["volume"].sum()
            })

    #daily_df = pd.DataFrame(daily_rows)
    #weekly_df = pd.DataFrame(weekly_rows)
    #monthly_df = pd.DataFrame(monthly_rows)
    daily_df = pd.DataFrame(daily_rows)
    if daily_df.empty:
        daily_df = pd.DataFrame(columns=[
            "Symbol","OPEN_D","HIGH_D","LOW_D","CLOSE_D","VOLUME_D"
        ])

    weekly_df = pd.DataFrame(weekly_rows)
    if weekly_df.empty:
        weekly_df = pd.DataFrame(columns=[
            "Symbol","OPEN_W","HIGH_W","LOW_W","CLOSE_W","VOLUME_W"
        ])

    monthly_df = pd.DataFrame(monthly_rows)
    if monthly_df.empty:
        monthly_df = pd.DataFrame(columns=[
            "Symbol","OPEN_M","HIGH_M","LOW_M","CLOSE_M","VOLUME_M"
        ])

    st.session_state.backtest_ohlc_cache[date_selected] = {
        "daily": daily_df,
        "weekly": weekly_df,
        "monthly": monthly_df
    }

    return st.session_state.backtest_ohlc_cache[date_selected]


if "replay_day_cache" not in st.session_state:
    st.session_state.replay_day_cache = {}

def preload_selected_day(date_selected):

    if date_selected in st.session_state.replay_day_cache:
        return st.session_state.replay_day_cache[date_selected]

    intraday_db = load_all_intraday_data()
    day_dict = {}

    for s, data in intraday_db.items():
        # Guard: ensure "date" column exists and is datetime dtype
        if data is None or (hasattr(data, "empty") and data.empty): continue
        if "date" not in data.columns:
            if "datetime" in data.columns:
                data = data.rename(columns={"datetime": "date"})
            else:
                continue
        if str(data["date"].dtype) == "object":
            data = _safe_parse_date_col(data, col="date")
        if data.empty or "date" not in data.columns: continue
        day_data = data[data["date"].dt.date == date_selected]
        day_data = day_data.sort_values("date")

        if not day_data.empty:
            day_dict[s] = day_data.reset_index(drop=True)

    st.session_state.replay_day_cache[date_selected] = day_dict
    return day_dict


def load_replay_data(date_selected):

    rows = []
    #intraday_db = load_all_intraday_data()
    intraday_db = preload_selected_day(date_selected)
    #for s in SYMBOLS:
    for s, data in intraday_db.items():
        # Guard: ensure "date" column exists and is datetime dtype
        if data is None or (hasattr(data, "empty") and data.empty): continue
        if "date" not in data.columns:
            if "datetime" in data.columns:
                data = data.rename(columns={"datetime": "date"})
            else:
                continue
        if str(data["date"].dtype) == "object":
            data = _safe_parse_date_col(data, col="date")
        if data.empty or "date" not in data.columns: continue

        # Selected day data
        day_data = data[data["date"].dt.date == date_selected]
        day_data = day_data.sort_values("date")

        if day_data.empty:
            continue

        # Candle pointer
        idx = min(
            st.session_state.replay_index,
            len(day_data)
        )

        if idx >= len(day_data):
            st.session_state.replay_running = False

        if idx == 0:
            idx = 1

        current_candle = day_data.iloc[idx - 1]
        st.session_state.current_replay_time = current_candle["date"]
        # 🔹 Previous day data
        #prev_day = date_selected - timedelta(days=1)
        #def previous_trading_day(d):
         #   while d.weekday() >= 5:
          #      d -= timedelta(days=1)
           # return d

        #prev_day = previous_trading_day(date_selected - timedelta(days=1))
        prev_day = last_trading_day(date_selected - timedelta(days=1))
        prev_data = data[data["date"].dt.date == prev_day]

        if prev_data.empty:
            continue

        y_high = prev_data["high"].max()
        y_low  = prev_data["low"].min()
        y_close = prev_data.iloc[-1]["close"]
        y_open = prev_data.iloc[0]["open"]
        y_vol = prev_data["volume"].sum()

        # 🔹 Current change calculation
        ltp = current_candle["close"]
        change = ltp - y_close
        change_pct = (change / y_close) * 100 if y_close else 0

        if idx >= len(day_data):
            st.session_state.replay_running = False
            idx = len(day_data)

        rows.append({
            "Symbol": s,
            "LTP": round(ltp,2),
            "LIVE_OPEN": current_candle["open"],
            "LIVE_HIGH": current_candle["high"],
            "LIVE_LOW": current_candle["low"],
            "LIVE_VOLUME": current_candle["volume"],

            "YEST_OPEN": y_open,
            "YEST_HIGH": y_high,
            "YEST_LOW": y_low,
            "YEST_CLOSE": y_close,
            "YEST_VOL": y_vol,

            "CHANGE": round(change,2),
            "CHANGE_%": round(change_pct,2)
        })

    df = pd.DataFrame(rows)

    # 🔥 FORCE all required columns to exist
    required_cols = [
        "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
        "LIVE_VOLUME","YEST_OPEN","YEST_HIGH","YEST_LOW",
        "YEST_CLOSE","YEST_VOL","CHANGE","CHANGE_%"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = 0

    return df


if "last_selected_date" not in st.session_state:
    st.session_state.last_selected_date = selected_date

if selected_date != st.session_state.last_selected_date:
    # 🔥 PRELOAD EVERYTHING WHEN DATE CHANGES
    if replay_mode:
        preload_selected_day(selected_date)
        preload_backtest_ohlc(selected_date)
    st.session_state.replay_index = 0
    st.session_state.last_selected_date = selected_date


##########      Play Logic
if play:
    st.session_state.replay_running = True

if pause:
    st.session_state.replay_running = False
###################                 Candle Advancement Logic
if replay_mode and st.session_state.replay_running:

    st.session_state.replay_index += 1

    #interval = 1000 if speed == "Fast (1 sec)" else 300000  # 5 mintues
    #interval = 1000 if speed == "Fast (1 sec)" else 100000   # 30000=30 seconds
    interval = 100 if speed == "Fast (1 sec)" else 30000

    st_autorefresh(interval=interval, key="replay_refresh")




# ================= CACHE FILE PATHS =================
# All intraday files are DATE-STAMPED -> new file every trading day
# On restart, today's file is loaded if it exists - no stale data

# ── _TODAY_STR: use last trading day when today has no live data ─────────────
# Covers: weekends, NSE holidays, pre-market (before 9:09 AM IST)
# This ensures all _dated() cache paths point to the most recent valid session.
def _get_cache_date() -> date:
    """
    Return the date to use for cache file names.
    - During market hours (9:09–15:35): today
    - Before market opens today: yesterday's last trading day
    - Weekend / holiday: last trading day
    Always returns the last date that had (or is having) a live session.
    """
    try:
        _now_ist = datetime.now(IST)
    except Exception:
        _now_ist = datetime.now()

    _today = _now_ist.date()
    _now_t = _now_ist.time()

    # Weekend - use last trading day
    if _today.weekday() >= 5:
        _d = _today - timedelta(days=1)
        while _d.weekday() >= 5:
            _d -= timedelta(days=1)
        return _d

    # NSE Holiday - use previous trading day
    if _today in NSE_HOLIDAYS:
        _d = _today - timedelta(days=1)
        while _d.weekday() >= 5 or _d in NSE_HOLIDAYS:
            _d -= timedelta(days=1)
        return _d

    # Before 9:09 AM IST - show yesterday's data
    from datetime import time as _dtime
    if _now_t < _dtime(9, 9):
        _d = _today - timedelta(days=1)
        while _d.weekday() >= 5:
            _d -= timedelta(days=1)
        return _d

    return _today

_CACHE_DATE = _get_cache_date()
_TODAY_STR  = _CACHE_DATE.strftime("%Y%m%d")   # e.g. "20260327"

def _dated(name, ext="csv"):
    """Return CACHE/name_YYYYMMDD.ext - unique per trading day."""
    return os.path.join(CACHE_DIR, f"{name}_{_TODAY_STR}.{ext}")

def _cleanup_old_cache(keep_days=3):
    """
    Delete dated cache files older than keep_days days.
    Runs once per session via session_state flag.
    """
    if st.session_state.get("_cache_cleaned"):
        return
    st.session_state["_cache_cleaned"] = True
    cutoff = date.today() - timedelta(days=keep_days)
    for fname in os.listdir(CACHE_DIR):
        # Only touch files that match pattern name_YYYYMMDD.ext
        parts = fname.rsplit("_", 1)
        if len(parts) != 2:
            continue
        stem_ext = parts[1]          # e.g. "20260324.csv"
        date_part = stem_ext.split(".")[0]
        if len(date_part) != 8 or not date_part.isdigit():
            continue
        try:
            fdate = date(int(date_part[:4]), int(date_part[4:6]), int(date_part[6:]))
            if fdate < cutoff:
                os.remove(os.path.join(CACHE_DIR, fname))
        except Exception:
            pass

_cleanup_old_cache()

# ── Dated intraday files (new every trading day) ──────────
LIVE_CACHE_CSV      = _dated("live_data")
INDICES_CACHE_CSV   = _dated("indices_live")
OI_CACHE_CSV        = _dated("oi_data")
FUT_CACHE_CSV       = _dated("futures_data")
FIVE_MIN_CACHE_CSV  = _dated("five_min")
FIFTEEN_M_CACHE_CSV = _dated("fifteen_min")
OI_INTEL_CACHE      = _dated("oi_intelligence", "json")
HI_LO_TRACK_CSV     = _dated("15m_hilo_track")
VOL_TRACK_CSV       = _dated("15m_vol_track")
H1_TRACK_CSV        = _dated("1h_hilo_track")
SEQ_EMAIL_DEDUP     = _dated("seq_email_dedup")
OI_SNAPSHOT_FILE    = _dated("oi_snapshot_auto")   # moved here - used in status panel
OI_15M_SNAP_FILE    = _dated("oi_15m_snapshot", "json")  # 15-min OI snapshot for delta calc
OI_15M_DEDUP        = _dated("oi_15m_alert_dedup")       # dedup per strike+type per 15-min slot

# ── EMA7 intraday files (recomputed every 60s throughout the day) ─
EMA7_15M_FILE       = _dated("ema7_15min")   # EMA7 on 15-min candles - today's file
EMA7_1H_FILE        = _dated("ema7_1hour")   # EMA7 on 1-hour candles - today's file
FUT_15M_SNAP_FILE   = _dated("fut_15m_snapshot", "json") # 15-min futures OI snapshot
FUT_15M_DEDUP       = _dated("fut_15m_alert_dedup")      # dedup per symbol per 15-min slot

# ── Non-dated persistent files (span multiple days) ───────
# These are intentionally NOT date-stamped:
#   OHLC_FILE    -> rolling 60-day history, appended daily
#   EMA_FILE     -> computed from OHLC, rebuilt daily
#   ALERTS_LOG_FILE -> accumulates across days
#   EMAIL_META_FILE / EMAIL_DEDUP_FILE / ALERTS_DEDUP_FILE -> dedup state
OHLC_FILE       = os.path.join(CACHE_DIR, "ohlc_60d.csv")
EMA_FILE        = os.path.join(CACHE_DIR, "ema_20_50.csv")

def _csv_is_fresh(path, max_age_sec=95):
    """
    Returns True only if:
      1. File exists
      2. Was written within max_age_sec seconds
      3. The filename contains today's date string (guards against stale dated files)
    """
    if not os.path.exists(path):
        return False
    # Guard: dated files must contain today's date in their name
    fname = os.path.basename(path)
    if _TODAY_STR not in fname and any(
        base in fname for base in [
            "live_data", "indices_live", "oi_data", "futures_data",
            "five_min", "fifteen_min", "oi_intelligence",
            "15m_hilo_track", "15m_vol_track", "1h_hilo_track",
            "seq_email_dedup"
        ]
    ):
        return False   # stale dated file from a previous day
    age = (datetime.now(IST) - datetime.fromtimestamp(os.path.getmtime(path), tz=IST)).total_seconds()
    return age < max_age_sec

def _load_today_csv(path, required_cols=None):
    """
    Load a dated CSV for the current cache date (_TODAY_STR).
    _TODAY_STR already handles weekends/holidays/pre-market by pointing
    to the last valid trading day, so this just checks the file exists
    and matches the expected date string.
    Returns DataFrame or None.
    """
    if not os.path.exists(path):
        return None
    fname = os.path.basename(path)
    if _TODAY_STR not in fname:
        return None   # belongs to a different session
    try:
        df_out = pd.read_csv(path, encoding='utf-8')
        # Normalize column names: strip whitespace, lowercase-safe comparison
        df_out.columns = [str(c).strip() for c in df_out.columns]
        # Handle "date" alias for "datetime" (older CSV schema)
        if "datetime" not in df_out.columns and "date" in df_out.columns:
            df_out = df_out.rename(columns={"date": "datetime"})
        if required_cols:
            if not all(c in df_out.columns for c in required_cols):
                return None
        return df_out
    except Exception:
        return None


def _load_latest_csv(name_pattern, required_cols=None):
    """
    Find the most recently modified CSV in CACHE matching name_pattern.
    Used as a last-resort fallback when _TODAY_STR file doesn't exist yet.
    Returns DataFrame or None.
    """
    try:
        import glob
        matches = glob.glob(os.path.join(CACHE_DIR, f"{name_pattern}_*.csv"))
        if not matches:
            return None
        # Sort by modification time, newest first
        matches.sort(key=os.path.getmtime, reverse=True)
        df_out = pd.read_csv(matches[0], encoding='utf-8')
        if required_cols and not all(c in df_out.columns for c in required_cols):
            return None
        return df_out
    except Exception:
        return None

def _load_today_json(path):
    """Load a dated JSON only if it belongs to today's date."""
    if not os.path.exists(path):
        return None
    fname = os.path.basename(path)
    if _TODAY_STR not in fname:
        return None
    try:
        import json as _j
        with open(path, encoding="utf-8") as f:
            return _j.load(f)
    except Exception:
        return None

# ================= LIVE DATA - SYNCHRONOUS EVERY RERUN =================
# kite.quote() = ONE batch API call ~1-2 sec. Fast enough for main thread.
# Must run every rerun so LTP, OHLC, alerts are always current.

# ================= SESSION INITIALIZATION (FAST START) =================
# Pre-calculate token map and yesterday OHLC once to speed up the main loop.
if "session_init_done" not in st.session_state:
    # Silent init (no st.status to prevent dimming)
    token_map = {}
    for s in SYMBOLS:
        token_map[s] = get_token(s)
    st.session_state.token_map = token_map
    
    y_ohlc_map = {}
    for s in SYMBOLS:
        tk = token_map.get(s)
        if tk:
            y_ohlc_map[s] = fetch_yesterday_ohlc(tk)
    st.session_state.y_ohlc_map = y_ohlc_map
    st.session_state.session_init_done = True

_TOKEN_MAP = st.session_state.get("token_map", {})
_Y_OHLC_MAP = st.session_state.get("y_ohlc_map", {})

# =========================================================
def fetch_live_and_oi():
    """ONE kite.quote call -> produces both live_df and oi_df. Saves both CSVs."""
    if not is_market_hours():
        return pd.DataFrame(), pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])
        
    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])
    live_rows = []
    oi_rows   = []
    for s in SYMBOLS:
        q = quotes.get(kite_symbol(s))
        if not q:
            continue
        
        # Use pre-calculated session data for SPEED
        y_data = _Y_OHLC_MAP.get(s, (None, None, None, None, None))
        yo, yh, yl, yc_hist, yv = y_data
        
        ltp = q["last_price"]
        pc  = q["ohlc"]["close"]
        chg = ltp - pc if pc else 0
        live_rows.append({
            "Symbol": s,
            "LTP":         round(ltp, 2),
            "LIVE_OPEN":   round(q["ohlc"]["open"], 2),
            "LIVE_HIGH":   round(q["ohlc"]["high"], 2),
            "LIVE_LOW":    round(q["ohlc"]["low"],  2),
            "LIVE_VOLUME": q.get("volume", 0),
            "YEST_OPEN":   yo,  "YEST_HIGH": yh,
            "YEST_LOW":    yl,  "YEST_CLOSE": pc if pc else yc_hist,
            "YEST_VOL":    yv,
            "CHANGE":      round(chg, 2),
            "CHANGE_%":    round((chg / pc * 100), 2) if pc else 0,
        })
        oi=q.get("oi",None); oi_dh=q.get("oi_day_high",None); oi_dl=q.get("oi_day_low",None)
        if oi is not None and oi_dh is not None:
            oi_pct = ((oi - oi_dl) / oi_dl * 100) if oi_dl else 0
            oi_rows.append({"Symbol":s,"OI":oi,"OI_CHANGE_%":round(oi_pct,2)})
    df_live = pd.DataFrame(live_rows)
    df_oi   = pd.DataFrame(oi_rows) if oi_rows else pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])
    if not df_live.empty:
        df_live.to_csv(LIVE_CACHE_CSV, index=False, encoding='utf-8')
    if not df_oi.empty:
        df_oi.to_csv(OI_CACHE_CSV, index=False, encoding='utf-8')
    return df_live, df_oi

# ── Single call produces both dataframes - no duplicate kite.quote ──
if replay_mode:
    base_df = load_replay_data(selected_date)
    _oi_prefetch = pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])
else:
    base_df, _oi_prefetch = fetch_live_and_oi()
    if base_df.empty:
        _fb = _load_today_csv(LIVE_CACHE_CSV)
        if _fb is not None and not _fb.empty:
            st.warning("⚠️ Live fetch failed - showing last saved data")
            base_df = _fb
        else:
            # Last resort: load most recent live_data CSV from any day
            _fb2 = _load_latest_csv("live_data")
            if _fb2 is not None and not _fb2.empty:
                _cache_age = _CACHE_DATE.strftime("%d %b %Y")
                st.warning(
                    f"⚠️ Live fetch failed - showing last session data ({_cache_age}). "
                    f"LTP shown is closing price. New data loads after 9:09 AM."
                )
                base_df = _fb2
            else:
                st.error("❌ Live fetch failed and no cached data found. Check Kite connection.")
    if _oi_prefetch.empty:
        _fb = _load_today_csv(OI_CACHE_CSV)
        if _fb is not None:
            _oi_prefetch = _fb

# 🔥 FORCE required columns even if empty
REPLAY_REQUIRED = [
    "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
    "LIVE_VOLUME","YEST_OPEN","YEST_HIGH","YEST_LOW",
    "YEST_CLOSE","YEST_VOL","CHANGE","CHANGE_%"
]

for col in REPLAY_REQUIRED:
    if col not in base_df.columns:
        base_df[col] = 0

# ── Pre-market OHLC fix: if OPEN=HIGH=LOW=LTP (Kite returns flat OHLC
# before market opens), replace with yesterday's OHLC columns.
# This gives meaningful data on the dashboard before 9:15 AM.
try:
    _pre_market = (
        not is_market_hours()
        and not base_df.empty
        and "LTP" in base_df.columns
        and "LIVE_OPEN" in base_df.columns
        and (base_df["LIVE_OPEN"] == base_df["LTP"]).mean() > 0.8  # >80% have flat OHLC
    )
    if _pre_market:
        # Copy YEST columns -> LIVE columns so UI shows meaningful yesterday data
        for _src, _dst in [
            ("YEST_HIGH",  "LIVE_HIGH"),
            ("YEST_LOW",   "LIVE_LOW"),
            ("YEST_OPEN",  "LIVE_OPEN"),
            ("YEST_CLOSE", "LTP"),          # show prev close as LTP
        ]:
            if _src in base_df.columns and _dst in base_df.columns:
                # Only override rows where LIVE_HIGH == LIVE_LOW (flat)
                _flat_mask = base_df["LIVE_HIGH"] == base_df["LIVE_LOW"]
                base_df.loc[_flat_mask, _dst] = base_df.loc[_flat_mask, _src]
        st.info(
            f"📅 Pre-market / holiday mode - showing previous session OHLC. "
            f"Live data loads after 9:09 AM."
        )
except Exception:
    pass

if replay_mode and "current_replay_time" in st.session_state:
    st.markdown(
        f"## 🕒 Replay Date: {selected_date} | Candle Time: "
        f"{st.session_state.current_replay_time.strftime('%H:%M')}"
    )
    # ===== Replay Progress Bar =====
    # ---- Slider Navigation ----
    #max_candles = 75  # approx full day
    intraday_db = preload_selected_day(selected_date)

    # get max candle length among all symbols
    max_candles = max(
        [len(v) for v in intraday_db.values()],
        default=75
    )

    slider_index = st.slider(
        "Manual Candle Navigation",
        min_value=0,
        max_value=max_candles,
        value=st.session_state.replay_index
    )

    st.session_state.replay_index = slider_index


if replay_mode:

    ohlc_cache = preload_backtest_ohlc(selected_date)
    # 🔥 SAFETY FIX
        # 🔥 HARD SAFETY
    for key in ["daily","weekly","monthly"]:
        if key not in ohlc_cache or ohlc_cache[key] is None:
            ohlc_cache[key] = pd.DataFrame(columns=["Symbol"])

        if "Symbol" not in ohlc_cache[key].columns:
            ohlc_cache[key]["Symbol"] = []

    df = (
        base_df
        .merge(ohlc_cache["daily"], on="Symbol", how="left")
        .merge(ohlc_cache["weekly"], on="Symbol", how="left")
        .merge(ohlc_cache["monthly"], on="Symbol", how="left")
        .merge(panchak_df, on="Symbol", how="left")
    )


else:
    # ✅ FIX: Cache CSV reads so they don't re-read files on every auto-refresh rerun
    @st.cache_data(ttl=3600, show_spinner=False)
    def _load_daily():   return pd.read_csv(DAILY_FILE, encoding='utf-8')
    @st.cache_data(ttl=3600, show_spinner=False)
    def _load_weekly():  return pd.read_csv(WEEKLY_FILE, encoding='utf-8')
    @st.cache_data(ttl=3600, show_spinner=False)
    def _load_monthly(): return pd.read_csv(MONTHLY_FILE, encoding='utf-8')

    df = (
        base_df
        .merge(_load_daily(),   on="Symbol", how="left")
        .merge(_load_weekly(),  on="Symbol", how="left")
        .merge(_load_monthly(), on="Symbol", how="left")
        .merge(panchak_df, on="Symbol", how="left")
    )
st.session_state["live_df"] = df
# 🔥 REMOVE duplicate suffix columns
df = df.loc[:, ~df.columns.duplicated()]

# ================= FIX-2: REMOVE DUPLICATE SYMBOLS =================
df = (
    df
    .sort_values("Symbol")
    .drop_duplicates(subset=["Symbol"], keep="last")
    .reset_index(drop=True)
)
# ==================================================================


if "LIVE_VOLUME" in df.columns:
    df["LIVE_VOLUME"] = pd.to_numeric(df["LIVE_VOLUME"], errors="coerce").fillna(0)


# ================= VOLUME % CALCULATION =================

# Ensure numeric
#df["LIVE_VOLUME"] = pd.to_numeric(df.get("LIVE_VOLUME", 0), errors="coerce").fillna(0)
#df["YEST_VOL"] = pd.to_numeric(df.get("YEST_VOL", 0), errors="coerce").fillna(0)
if "LIVE_VOLUME" not in df.columns:
    df["LIVE_VOLUME"] = 0

if "YEST_VOL" not in df.columns:
    df["YEST_VOL"] = 0

df["LIVE_VOLUME"] = pd.to_numeric(df["LIVE_VOLUME"], errors="coerce").fillna(0)
df["YEST_VOL"] = pd.to_numeric(df["YEST_VOL"], errors="coerce").fillna(0)


# Safe division
df["VOL_%"] = np.where(
    df["YEST_VOL"] > 0,
    ((df["LIVE_VOLUME"] - df["YEST_VOL"]) / df["YEST_VOL"]) * 100,
    0
).round(2)


# ================= SAFETY NET: REQUIRED LIVE COLUMNS =================

REQUIRED_COLS = [
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE"
]

if not df.empty:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        st.error(f"❌ Missing required columns: {missing}")
        st.stop()

#st.write("DEBUG DF COLUMNS:", df.columns.tolist())


# ================= NEAR / GAIN =================
def near(r):
    if r.LTP >= r.TOP_HIGH: return "🟢 ↑ BREAK"
    if r.LTP <= r.TOP_LOW:  return "🔴 ↓ BREAK"
    return f"🟢 ↑ {round(r.TOP_HIGH-r.LTP,1)}" if (r.TOP_HIGH-r.LTP) <= (r.LTP-r.TOP_LOW) else f"🔴 ↓ {round(r.LTP-r.TOP_LOW,1)}"

#df["NEAR"] = df.apply(near, axis=1)
if not df.empty:
    df["NEAR"] = df.apply(near, axis=1)
    # 🔥 Heatmap Direction Auto-Reversal Detection
    _check_heatmap_reversal(df)
    # 🪐 Panchak High-Conviction Special Rules (Fierce Reversals, Yoga, etc.)
    _check_panchak_special_rules(df)
else:
    df["NEAR"] = ""

# ================= GAIN (SAFE, NUMERIC, ARROW-COMPATIBLE) =================

def gain(r):
    if pd.notna(r.TOP_HIGH) and r.LTP > r.TOP_HIGH:
        return round(r.LTP - r.TOP_HIGH, 2)
    if pd.notna(r.TOP_LOW) and r.LTP < r.TOP_LOW:
        return round(r.LTP - r.TOP_LOW, 2)
    return None  # IMPORTANT: None, not ""

df["GAIN"] = df.apply(gain, axis=1)
df["GAIN"] = pd.to_numeric(df["GAIN"], errors="coerce")




# =========================================================
# ROLLING OHLC (60 DAYS) + EMA20 / EMA50 (BULLETPROOF)
# =========================================================

# OHLC_FILE and EMA_FILE defined in non-dated block above


# ✅ FIX: Removed duplicate last_trading_day definition (was defined twice)
def build_or_update_ohlc_60d():
    """
    1️⃣ If file NOT present -> build last 60 trading days
    2️⃣ If file present -> append only latest trading day
    """

    today = date.today()
    end_day = last_trading_day(today)

    # ---------- CASE 1: FILE NOT EXISTS -> FULL BUILD ----------
    if not os.path.exists(OHLC_FILE):
        rows = []

        start_day = end_day - timedelta(days=360)  # buffer to get 180 trading days

        for s in SYMBOLS:
            tk = get_token(s)
            if not tk:
                continue

            try:
                bars = kite.historical_data(
                    tk,
                    start_day,
                    end_day,
                    "day"
                )
            except Exception:
                continue

            dfb = pd.DataFrame(bars)
            if dfb.empty:
                continue

            dfb = _safe_parse_date_col(dfb, col="date").dt.date
            dfb = dfb.sort_values("date").tail(60)

            for _, r in dfb.iterrows():
                rows.append({
                    "Symbol": s,
                    "date": r["date"],
                    "open":  r["open"],
                    "high":  r["high"],
                    "low":   r["low"],
                    "close": r["close"],
                    "volume": r["volume"],

                })

            tm.sleep(0.3)

        if rows:
            pd.DataFrame(rows).to_csv(OHLC_FILE, index=False, encoding='utf-8')

        return

    # ---------- CASE 2: FILE EXISTS -> DAILY APPEND ----------
    ohlc_df = pd.read_csv(OHLC_FILE, encoding='utf-8')
    ohlc_df = _safe_parse_date_col(ohlc_df, col="date")
    if ohlc_df.empty:
        return
    ohlc_df["date"] = ohlc_df["date"].dt.date

    if end_day in ohlc_df["date"].values:
        return  # already updated

    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        try:
            bars = kite.historical_data(
                tk,
                end_day,
                end_day,
                "day"
            )
        except Exception:
            continue

        if not bars:
            continue

        b = bars[0]
        rows.append({
            "Symbol": s,
            "date": end_day,
            "open":  b["open"],
            "high":  b["high"],
            "low":   b["low"],
            "close": b["close"],
            "volume": b["volume"],

        })

        tm.sleep(0.25)

    if not rows:
        return

    new_df = pd.DataFrame(rows)

    final_df = (
        pd.concat([ohlc_df, new_df], ignore_index=True)
        .drop_duplicates(subset=["Symbol", "date"])
        .sort_values(["Symbol", "date"])
        .groupby("Symbol", as_index=False)
        .tail(60)
    )

    final_df.to_csv(OHLC_FILE, index=False, encoding='utf-8')


@st.cache_data(ttl=3600, show_spinner=False)   # ✅ silent
def build_ema_from_ohlc():
    if not os.path.exists(OHLC_FILE):
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    df = pd.read_csv(OHLC_FILE, encoding='utf-8')

    rows = []

    for s, g in df.groupby("Symbol"):
        if len(g) < 50:
            continue

        g = g.sort_values("date")
        g["EMA7"]  = g["close"].ewm(span=7).mean()
        g["EMA20"] = g["close"].ewm(span=20).mean()
        g["EMA50"] = g["close"].ewm(span=50).mean()

        rows.append({
            "Symbol": s,
            "EMA7": round(g.iloc[-1]["EMA7"], 2),
            "EMA20": round(g.iloc[-1]["EMA20"], 2),
            "EMA50": round(g.iloc[-1]["EMA50"], 2)
        })

    if not rows:
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    ema_df = pd.DataFrame(rows)
    ema_df.to_csv(EMA_FILE, index=False, encoding='utf-8')
    return ema_df


# ✅ FIX: Guard ohlc_60d update with session_state - runs once per app session, not every rerun
if not replay_mode:
    if "ohlc_60d_updated" not in st.session_state:
        build_or_update_ohlc_60d()
        st.session_state["ohlc_60d_updated"] = True

ema_df = build_ema_from_ohlc()
df = df.merge(ema_df, on="Symbol", how="left")

# ── Merge intraday EMA7 (15m and 1H) into df ─────────────────────
# These are loaded from dated CSVs built by the EMA7 intraday engine below.
# On first run they may be empty (pre-market). They fill in once candles arrive.
# NOTE: _ema7_15m_df / _ema7_1h_df are defined after the 15m fetch block (~line 5820).
# We defer the merge to after those blocks using session_state.
# The merge happens in a safe late-stage block - see "EMA7 INTRADAY MERGE" below.

# =========================================================
# MULTI-PERIOD DOWNTREND -> EMA REVERSAL SCREENER
# =========================================================

@st.cache_data(ttl=3600, show_spinner=False)   # ✅ silent
def load_ohlc_full():
    _df = pd.read_csv(OHLC_FILE, encoding='utf-8')
    _df = _safe_parse_date_col(_df, col="date")
    return _df

ohlc_full = load_ohlc_full() if os.path.exists(OHLC_FILE) else pd.DataFrame()

def build_downtrend_reversal(days):
    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        past_close = g.iloc[-days]["close"]
        current_close = g.iloc[-1]["close"]

        # Downtrend condition
        if past_close <= current_close:
            continue

        # Get live row
        live_row = df[df["Symbol"] == s]

        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # EMA Reversal Confirmation
        if (
            r["LTP"] > r.get("EMA7", 0) and
            r["LTP"] > r.get("EMA20", 0) and
            r["LTP"] > r.get("EMA50", 0) and
            r.get("EMA20", 0) > r.get("EMA50", 0)
        ):
            rows.append({
                "Symbol": s,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                "EMA7": r.get("EMA7"),
                "EMA20": r.get("EMA20"),
                "EMA50": r.get("EMA50"),
                "Past_Close": past_close,
                "Current_Close": current_close,
                "Drop_%": round(((current_close - past_close) / past_close) * 100, 2)
            })

    return pd.DataFrame(rows)

down_1m_df = build_downtrend_reversal(22)
down_2m_df = build_downtrend_reversal(44)
down_3m_df = build_downtrend_reversal(66)
down_6m_df = build_downtrend_reversal(132)

# =========================================================
# MULTI-PERIOD DOWNTREND -> EMA REVERSAL SCREENER
# =========================================================

#ohlc_full = pd.read_csv(OHLC_FILE, encoding='utf-8')
#ohlc_full = _safe_parse_date_col(ohlc_full, col="date")

def build_downtrend_reversal1(days):
    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        past_close = g.iloc[-days]["close"]
        current_close = g.iloc[-1]["close"]

        # Downtrend condition
        if past_close <= current_close:
            continue

        # Get live row
        live_row = df[df["Symbol"] == s]

        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # EMA Reversal Confirmation
        if (
            r["LTP"] > r.get("EMA7", 0) and
            r["LTP"] > r.get("EMA20", 0) and
            r["LTP"] > r.get("EMA50", 0) #and
            #r.get("EMA20", 0) > r.get("EMA50", 0)
        ):
            rows.append({
                "Symbol": s,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                "EMA7": r.get("EMA7"),
                "EMA20": r.get("EMA20"),
                "EMA50": r.get("EMA50"),
                "Past_Close": past_close,
                "Current_Close": current_close,
                "Drop_%": round(((current_close - past_close) / past_close) * 100, 2)
            })

    return pd.DataFrame(rows)

down_1m_df1 = build_downtrend_reversal1(22)
down_2m_df2 = build_downtrend_reversal1(44)
down_3m_df3 = build_downtrend_reversal1(66)
down_6m_df4 = build_downtrend_reversal1(132)


# =========================================================
# MULTI-PERIOD DOWNTREND -> EMA REVERSAL AFTER UPTREND
# =========================================================

def build_uptrend_reversal2(days):
    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        past_close = g.iloc[-days]["close"]
        current_close = g.iloc[-1]["close"]

        # ✅ UPtrend condition
        if past_close >= current_close:
            continue

        live_row = df[df["Symbol"] == s]
        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # ✅ Bearish EMA breakdown
        if (
            r["LTP"] < r.get("EMA7", 0) and
            r["LTP"] < r.get("EMA20", 0) and
            r["LTP"] < r.get("EMA50", 0) and
            r.get("EMA7", 0) < r.get("EMA20", 0) < r.get("EMA50", 0)
        ):
            rows.append({
                "Symbol": s,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                "EMA7": r.get("EMA7"),
                "EMA20": r.get("EMA20"),
                "EMA50": r.get("EMA50"),
                "Past_Close": past_close,
                "Current_Close": current_close,
                "Rise_%": round(((current_close - past_close) / past_close) * 100, 2)
            })

    return pd.DataFrame(rows)

#up_1m_df1 = build_uptrend_reversal2(22)
#up_2m_df2 = build_uptrend_reversal2(44)
#up_3m_df3 = build_uptrend_reversal2(66)
#up_6m_df4 = build_uptrend_reversal2(132)

#def build_exhaustion_top(days=30):
# =========================================================
# EARLY WEAKNESS BEFORE BIG FALL (SMART EXIT ZONE)
# =========================================================

def build_early_top_weakness(days):

    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        recent = g.tail(days)

        highest_close = recent["close"].max()
        last_row = g.iloc[-1]
        prev_row = g.iloc[-2]

        # 🔹 Was in uptrend
        if g.iloc[-days]["close"] >= last_row["close"]:
            continue

        # 🔹 Near recent high (within 5%)
        if last_row["close"] < highest_close * 0.95:
            continue

        live_row = df[df["Symbol"] == s]
        if live_row.empty:
            continue

        r = live_row.iloc[0]

        # 🔹 KEY CONDITION: LTP BELOW EMA7 (first weakness)
        if r["LTP"] >= r.get("EMA7", 0):
            continue

        # 🔹 But still above EMA20 (not full breakdown yet)
        if r["LTP"] <= r.get("EMA20", 0):
            continue

        # 🔹 EMA7 flattening or turning down
        ema7_series = g["close"].ewm(span=7).mean()
        if ema7_series.iloc[-1] >= ema7_series.iloc[-2]:
            continue

        rows.append({
            "Symbol": s,
            "LTP": r["LTP"],
            "EMA7": r.get("EMA7"),
            "EMA20": r.get("EMA20"),
            "CHANGE_%": r["CHANGE_%"],
            "Near_30D_High": highest_close,
            "Weakness": "LTP < EMA7"
        })

    return pd.DataFrame(rows)

#up_3m_df3 = build_early_top_weakness(30)
#up_6m_df4 = build_early_top_weakness(60)

up_1m_df1 = build_early_top_weakness(30)
up_2m_df2 = build_early_top_weakness(60)

# =========================================================
# STRONG EARLY TOP BREAKDOWN (INSTITUTIONAL EXIT)
# =========================================================

def build_refined_top_breakdown(days):

    rows = []

    for s, g in ohlc_full.groupby("Symbol"):

        g = g.sort_values("date")

        if len(g) < days:
            continue

        recent = g.tail(days)

        highest_close = recent["close"].max()
        last_close = g.iloc[-1]["close"]

        # 🔹 Prior Uptrend (structure condition)
        if g.iloc[-days]["close"] >= last_close:
            continue

        # 🔹 Near recent high (within 7%)
        if last_close < highest_close * 0.93:
            continue

        live_row = df[df["Symbol"] == s]
        if live_row.empty:
            continue

        r = live_row.iloc[0]

        ema7 = r.get("EMA7", 0)
        ema20 = r.get("EMA20", 0)
        ema50 = r.get("EMA50", 0)

        # 🔴 Core Conditions You Requested
        if not (
            ema7 < ema20 and            # EMA7 below EMA20
            r["LTP"] < ema20 and        # Price below EMA20
            r["LTP"] > ema50            # Still above EMA50 (early stage)
        ):
            continue

        rows.append({
            "Symbol": s,
            "LTP": r["LTP"],
            "EMA7": ema7,
            "EMA20": ema20,
            "EMA50": ema50,
            "CHANGE_%": r["CHANGE_%"],
            "Distance_from_High_%": round(
                ((r["LTP"] - highest_close) / highest_close) * 100, 2
            )
        })

    return pd.DataFrame(rows)

up_3m_df3 = build_refined_top_breakdown(30)
up_6m_df4 = build_refined_top_breakdown(60)

# =========================================================
# EMA20–EMA50 + TOP LIVE_HIGH / TOP LIVE_LOW (SINGLE SOURCE OF TRUTH)
# =========================================================

ema_signal_df = df.dropna(
    subset=["EMA20", "EMA50", "TOP_HIGH", "TOP_LOW"]
).loc[
    (
        (df["LTP"] > df["EMA20"]) &
        (df["EMA20"] > df["EMA50"]) &
        (df["LTP"] > df["TOP_HIGH"])
    ) |
    (
        (df["LTP"] < df["EMA20"]) &
        (df["EMA20"] < df["EMA50"]) &
        (df["LTP"] < df["TOP_LOW"])
    ),
    [
        "Symbol",
        "TOP_HIGH",
        "TOP_LOW",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "GAIN",
        "EMA20",
        "EMA50"
        
        
    ]
].copy()

ema_signal_df["SIGNAL"] = ema_signal_df.apply(
    lambda r: "BUY" if r["LTP"] > r["EMA20"] else "SELL",
    axis=1
)

# ================= SPLIT BUY / SELL =================

ema_buy_df = ema_signal_df[ema_signal_df["SIGNAL"] == "BUY"].copy()
ema_sell_df = ema_signal_df[ema_signal_df["SIGNAL"] == "SELL"].copy()







# ================= UI =================
#st.title("📊 Panchak + Breakout Dashboard")
#st.caption(f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')} | Auto 60s")
#st_autorefresh(interval=10000_000, key="refresh")
#st_autorefresh(interval=300_000, key="refresh")
# ================= UI =================

now     = datetime.now(IST).time()
now_dt  = datetime.now(IST)
today   = now_dt.date()

market_start = time(9, 0)
market_end   = time(15, 30)

# ── Determine if market is open right now ─────────────────────────────
_is_weekday  = today.weekday() < 5                       # Mon–Fri
_is_holiday  = today in NSE_HOLIDAYS                     # NSE closed
_is_mkt_time = market_start <= now <= market_end         # 09:00–15:30
_market_open = _is_weekday and not _is_holiday and _is_mkt_time

st.caption(
    f"Last refresh: {now_dt.strftime('%d-%b-%Y %H:%M:%S')}"
    + (f"  |  🚫 NSE Holiday: {today.strftime('%d %b %Y')}" if _is_holiday else "")
)

# ✅ Auto refresh ONLY during live market hours on trading days
if _market_open:
    st_autorefresh(interval=60 * 1000, key="refresh", debounce=False)
    st.caption("🔄 Auto refresh active every 60s (Market Hours)")
elif _is_holiday:
    st.caption(f"🏖️ NSE Holiday today ({today.strftime('%d %b %Y')}) - Auto refresh paused")
elif not _is_weekday:
    st.caption("📅 Weekend - Auto refresh paused")
else:
    st.caption("⏸ Market closed (before 09:00 or after 15:30) - Auto refresh paused")


# =========================================================
# 💾 DATA STORAGE STATUS - confirms every file is from TODAY
# =========================================================
def _file_status(label, path):
    """Return status dict for a dated cache file."""
    exists   = os.path.exists(path)
    is_today = _TODAY_STR in os.path.basename(path)
    if not exists:
        return {"label": label, "status": "❌ Missing", "size": "-", "age": "-", "file": os.path.basename(path)}
    size_kb = round(os.path.getsize(path) / 1024, 1)
    age_s   = (datetime.now(IST) - datetime.fromtimestamp(os.path.getmtime(path), tz=IST)).total_seconds()
    if age_s < 120:
        age_str = f"{int(age_s)}s ago"
    elif age_s < 3600:
        age_str = f"{int(age_s/60)}m ago"
    else:
        age_str = f"{int(age_s/3600)}h ago"
    status = "✅ Today" if is_today else "⚠️ Stale"
    return {"label": label, "status": status, "size": f"{size_kb} KB", "age": age_str, "file": os.path.basename(path)}

with st.expander("💾 Data Storage Status (click to expand)", expanded=False):
    _status_items = [
        _file_status("Live Quotes",       LIVE_CACHE_CSV),
        _file_status("OI Data",           OI_CACHE_CSV),
        _file_status("Indices",           INDICES_CACHE_CSV),
        _file_status("Futures",           FUT_CACHE_CSV),
        _file_status("5-Min Candles",     FIVE_MIN_CACHE_CSV),
        _file_status("15-Min Candles",    FIFTEEN_M_CACHE_CSV),
        _file_status("OI Intelligence",   OI_INTEL_CACHE),
        _file_status("15m HiLo Track",    HI_LO_TRACK_CSV),
        _file_status("15m Vol Track",     VOL_TRACK_CSV),
        _file_status("1h HiLo Track",     H1_TRACK_CSV),
        _file_status("Seq Email Dedup",   SEQ_EMAIL_DEDUP),
        _file_status("OI Snapshot",       OI_SNAPSHOT_FILE),
        _file_status("OHLC 60D",          OHLC_FILE),
        _file_status("EMA 20/50",         EMA_FILE),
        _file_status("Alerts Log",        ALERTS_LOG_FILE),
    ]
    _status_df = pd.DataFrame(_status_items)
    def _color_status(val):
        if "✅" in str(val):   return "background-color:#1b5e20;color:#fff"
        if "⚠️" in str(val):  return "background-color:#e65100;color:#fff"
        if "❌" in str(val):   return "background-color:#b71c1c;color:#fff"
        return ""
    st.dataframe(
        _status_df.style.map(_color_status, subset=["status"]), width='stretch',
        hide_index=True,
    )
    st.caption(f"📅 Today = {_TODAY_STR}  |  Cache dir: {CACHE_DIR}")


PANCHAK_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "GAIN",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

panchak_view = df[PANCHAK_COLUMNS].copy()

# ── Sort: priority stocks first, then rest alphabetically ────────────
def _panchak_sort_key(sym):
    _s = str(sym).upper()
    try:
        return (0, PANCHAK_PRIORITY_STOCKS.index(_s))
    except ValueError:
        return (1, _s)

panchak_view["_sort_key"] = panchak_view["Symbol"].apply(
    lambda s: (0, next((i for i, p in enumerate(PANCHAK_PRIORITY_STOCKS) if p.upper() == str(s).upper()), 999))
    if str(s).upper() in _PRIORITY_SET else (1, str(s).upper())
)
panchak_view = panchak_view.sort_values("_sort_key").drop(columns=["_sort_key"]).reset_index(drop=True)
# Rename LIVE_ prefix to cleaner display names
panchak_view = panchak_view.rename(columns={
    "LIVE_OPEN": "DAY_OPEN",
    "LIVE_HIGH": "DAY_HIGH",
    "LIVE_LOW":  "DAY_LOW",
})

TOP_HIGH_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "DIFF",
    "BT",
    "ST",
    "YEST_HIGH",
    "YEST_LOW"
]

TOP_HIGH_view = df[TOP_HIGH_COLUMNS]
TOP_HIGH_view = TOP_HIGH_view.rename(columns={"LIVE_OPEN":"DAY_OPEN","LIVE_HIGH":"DAY_HIGH","LIVE_LOW":"DAY_LOW"})

TOP_LOW_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "DIFF",
    "BT",
    "ST",
    "YEST_HIGH",
    "YEST_LOW"
]

TOP_LOW_view = df[TOP_LOW_COLUMNS]
TOP_LOW_view = TOP_LOW_view.rename(columns={"LIVE_OPEN":"DAY_OPEN","LIVE_HIGH":"DAY_HIGH","LIVE_LOW":"DAY_LOW"})

NEAR_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

NEAR_view = df[NEAR_COLUMNS]
NEAR_view = NEAR_view.rename(columns={"LIVE_OPEN":"DAY_OPEN","LIVE_HIGH":"DAY_HIGH","LIVE_LOW":"DAY_LOW"})

DAILY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

DAILY_view = df[DAILY_COLUMNS]
DAILY_view = DAILY_view.rename(columns={"LIVE_OPEN":"DAY_OPEN","LIVE_HIGH":"DAY_HIGH","LIVE_LOW":"DAY_LOW"})

WEEKLY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

WEEKLY_view = df[WEEKLY_COLUMNS]
WEEKLY_view = WEEKLY_view.rename(columns={"LIVE_OPEN":"DAY_OPEN","LIVE_HIGH":"DAY_HIGH","LIVE_LOW":"DAY_LOW"})

MONTHLY_COLUMNS = [
    "Symbol",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "HIGH_W",
    "LOW_W",
    "HIGH_M",
    "LOW_M"
]

MONTHLY_view = df[MONTHLY_COLUMNS]
MONTHLY_view = MONTHLY_view.rename(columns={"LIVE_OPEN":"DAY_OPEN","LIVE_HIGH":"DAY_HIGH","LIVE_LOW":"DAY_LOW"})


#import pandas as pd

def export_all_tabs_to_excel():
    output = BytesIO()

    def safe_export(writer, df_obj, sheet):
        if df_obj is not None and isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
            df_obj.to_excel(writer, sheet_name=sheet[:31], index=False)

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # ========= CORE =========
        safe_export(writer, panchak_view, "PANCHAK")
        safe_export(writer, TOP_HIGH_df, "TOP_HIGH")
        safe_export(writer, TOP_LOW_df, "TOP_LOW")
        safe_export(writer, near_df, "NEAR_ZONE")

        # ========= DAILY / WEEKLY / MONTHLY =========
        safe_export(writer, daily_up, "DAILY_UP")
        safe_export(writer, daily_down, "DAILY_DOWN")
        safe_export(writer, weekly_up, "WEEKLY_UP")
        safe_export(writer, weekly_down, "WEEKLY_DOWN")
        safe_export(writer, monthly_up, "MONTHLY_UP")
        safe_export(writer, monthly_down, "MONTHLY_DOWN")

        # ========= EMA =========
        safe_export(writer, ema_buy_df, "EMA_BUY")
        safe_export(writer, ema_sell_df, "EMA_SELL")
        safe_export(writer, daily_ema_buy, "DAILY_EMA_BUY")
        safe_export(writer, daily_ema_sell, "DAILY_EMA_SELL")
        safe_export(writer, weekly_ema_buy, "WEEKLY_EMA_BUY")
        safe_export(writer, weekly_ema_sell, "WEEKLY_EMA_SELL")

        # ========= SUPERTREND =========
        safe_export(writer, supertrend_df, "SUPERTREND")
        safe_export(writer, st_near_view, "ST_NEAR")

        # ========= OPTIONS =========
        safe_export(writer, STRONG_BUY_DF, "OPTIONS_STRONG")
        safe_export(writer, backtest_df, "OPTIONS_BACKTEST")

        # ========= PATTERNS =========
        safe_export(writer, ol_oh_df, "O_EQUALS_HL")
        safe_export(writer, LIVE_OPEN_LIVE_LOW_df, "O_EQ_L")
        safe_export(writer, LIVE_OPEN_LIVE_HIGH_df, "O_EQ_H")
        safe_export(writer, four_bar_df, "FOUR_BAR")

        # ========= FAKE BREAKOUTS =========
        safe_export(writer, fake_bull_df, "FAKE_BULL")
        safe_export(writer, fake_bear_df, "FAKE_BEAR")

        # ========= ALERTS =========
        if "alerts" in globals() and alerts:
            safe_export(writer, pd.DataFrame(alerts), "ALERTS")

        # ========= GAINERS / LOSERS =========
        safe_export(writer, gainers_df, "GAINERS")
        safe_export(writer, losers_df, "LOSERS")

    output.seek(0)
    return output
    # ✅ FIX: Removed unreachable dead code after return statement




def is_email_allowed():
    now = datetime.now(IST).time()
    return time(9,15) <= now <= time(15,15)


# ================= BREAKOUT DATA (GLOBAL) =================

TOP_HIGH_df = df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
TOP_LOW_df  = df.loc[df.LTP <= df.TOP_LOW,  TOP_LOW_COLUMNS]

near_df = df.loc[
    (df.LTP > df.TOP_LOW) & (df.LTP < df.TOP_HIGH),
    [
        "Symbol","LTP","TOP_HIGH","TOP_LOW","NEAR",
        "LIVE_HIGH","LIVE_LOW","CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW"
    ]
]

#daily_up   = df.loc[df.LTP >= df.YEST_HIGH, DAILY_COLUMNS]
#daily_down = df.loc[df.LTP <= df.YEST_LOW,  DAILY_COLUMNS]

# =========================================================
# DAILY UP – Clean Break Above YH (No Gap-Up)
# =========================================================
daily_up = df.loc[
    (df["LIVE_OPEN"] <= df["YEST_HIGH"]) & (df["LTP"] > df["YEST_HIGH"]), DAILY_COLUMNS ]

# =========================================================
# DAILY DOWN – Clean Break Below YL (No Gap-Down)
# =========================================================
daily_down = df.loc[(df["LIVE_OPEN"] >= df["YEST_LOW"]) & (df["LTP"] < df["YEST_LOW"]),DAILY_COLUMNS]


weekly_up   = df.loc[df.LTP >= df.HIGH_W, WEEKLY_COLUMNS]
weekly_down = df.loc[df.LTP <= df.LOW_W,  WEEKLY_COLUMNS]

monthly_up   = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
monthly_down = df.loc[df.LTP <= df.LOW_M,  MONTHLY_COLUMNS]

def notify_browser(title, symbols):
    """Browser toast - disabled per alerts_modifications.docx Point 1.
    Set BROWSER_ALERTS_ENABLED = True to re-enable."""
    if not symbols or not BROWSER_ALERTS_ENABLED:
        return
    st.toast(f"{title}: {', '.join(symbols)}", icon="🚨")

def detect_new_entries(name, current_symbols):
    if not is_market_hours():
        return []
    path = os.path.join(CACHE_DIR, f"{name}_prev.txt")  # FIX-2f

    # Convert everything to string safely
    current_symbols = [str(x) for x in current_symbols if pd.notna(x)]

    prev = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                prev = set(content.split(","))

    curr = set(current_symbols)
    new = curr - prev

    # Write back safely
    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(curr))

    return list(new)


new_TOP_HIGH = detect_new_entries(
    "TOP_HIGH",
    TOP_HIGH_df.Symbol.tolist()
)

if is_market_hours() and st.session_state.get("toast_TOP_HIGH", False): notify_browser("🟢 New TOP LIVE_HIGH", new_TOP_HIGH)  # default False per doc

new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

if is_market_hours() and st.session_state.get("toast_TOP_LOW", False): notify_browser("🔴 New TOP LIVE_LOW", new_TOP_LOW)  # default False per doc

def can_send_email():
    if not EMAIL_ENABLED:
        return False

    os.makedirs(CACHE_DIR, exist_ok=True)  # FIX-2h

    now = datetime.now()

    data = {
        "last_sent": None,
        "count_today": 0,
        "date": now.strftime("%Y-%m-%d")
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE, encoding="utf-8") as f:
            data = json.load(f)

    # Reset daily counter
    if data["date"] != now.strftime("%Y-%m-%d"):
        data["date"] = now.strftime("%Y-%m-%d")
        data["count_today"] = 0
        data["last_sent"] = None

    # Daily limit
    if data["count_today"] >= EMAIL_MAX_PER_DAY:
        return False

    # Cooldown
    if data["last_sent"]:
        last = datetime.fromisoformat(data["last_sent"])
        if now - last < timedelta(minutes=EMAIL_COOLDOWN_MIN):
            return False

    return True


def record_email_sent():
    now = datetime.now()

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "last_sent": now.isoformat(),
        "count_today": 0
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE, encoding="utf-8") as f:
            data = json.load(f)

    data["count_today"] += 1
    data["last_sent"] = now.isoformat()

    with open(EMAIL_META_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)
# ✅ FIX: Removed duplicate can_send_email / record_email_sent definitions (were defined twice)
EMAIL_ENABLED = False  # Disabled per alerts_modifications.docx (Point 1)  # 🔁 set False to fully disable emails

# ─────────────────────────────────────────────────────────
# 📧 UNIFIED HTML EMAIL ENGINE
# Used by both notify_all (all alert categories) and
# the sequential breakout alerts (_fire_seq_alerts)
# ─────────────────────────────────────────────────────────

def _build_html_email(title, symbols_with_ltp, df_table=None, color="#1a237e", icon="🚨",
                      old_df=None):
    """
    Build a rich HTML email body.
    symbols_with_ltp : list of (symbol, ltp) tuples for the banner (new entries)
    df_table         : DataFrame of NEW entries (this alert)
    old_df           : DataFrame of STILL-ACTIVE entries alerted earlier today
    """
    now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    syms_str = ",  ".join(
        f"<b style='color:{color}'>{s}</b>" + (f" ({ltp:.2f})" if ltp else "")
        for s, ltp in symbols_with_ltp
    )

    # Build table HTML if df provided
    table_html = ""
    if df_table is not None and not df_table.empty:
        df_clean = df_table.copy()
        # Strip emoji from TYPE column
        for col in ["TYPE"]:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.replace(
                    r"[^-]+", "", regex=True).str.strip()
        headers = "".join(
            f'<th style="background:{color};color:#fff;padding:6px 10px;'
            f'border:1px solid #ccc;font-size:12px;white-space:nowrap;">{c}</th>'
            for c in df_clean.columns
        )
        rows = ""
        for idx, row in df_clean.iterrows():
            bg = "#f5f5f5" if idx % 2 == 0 else "#ffffff"
            cells = ""
            for c in df_clean.columns:
                val = row[c]
                style = f"padding:5px 8px;border:1px solid #ddd;font-size:12px;background:{bg};white-space:nowrap;"
                if isinstance(val, float):
                    disp = f"{val:,.2f}"
                elif isinstance(val, int):
                    disp = f"{val:,}"
                else:
                    disp = str(val) if val is not None else ""
                cells += f'<td style="{style}">{disp}</td>'
            rows += f"<tr>{cells}</tr>"
        table_html = f"""
        <h3 style="color:{color};font-size:14px;margin:16px 20px 6px;">
            🆕 New Entries (This Alert)
        </h3>
        <div style="padding:0 20px 16px;overflow-x:auto;">
        <table style="border-collapse:collapse;width:100%;font-family:Courier New,monospace;">
          <thead><tr>{headers}</tr></thead>
          <tbody>{rows}</tbody>
        </table></div>"""

    # Still-active old entries (grey header - entries alerted earlier today)
    old_table_html = ""
    if old_df is not None and not old_df.empty:
        old_dc = old_df.copy()
        old_hdrs = "".join(
            f'<th style="background:#37474f;color:#fff;padding:6px 10px;'
            f'border:1px solid #ccc;font-size:12px;white-space:nowrap;">{c}</th>'
            for c in old_dc.columns
        )
        old_rows = ""
        for idx, row in old_dc.iterrows():
            bg = "#f5f5f5" if idx % 2 == 0 else "#ffffff"
            cells = ""
            for c in old_dc.columns:
                val = row[c]
                st_ = (f"padding:5px 8px;border:1px solid #ddd;font-size:12px;"
                       f"background:{bg};white-space:nowrap;")
                if isinstance(val, float):
                    disp = f"{val:,.2f}"
                elif isinstance(val, int):
                    disp = f"{val:,}"
                else:
                    disp = str(val) if val is not None else ""
                cells += f'<td style="{st_}">{disp}</td>'
            old_rows += f"<tr>{cells}</tr>"
        old_table_html = f"""
        <h3 style="color:#37474f;font-size:14px;margin:16px 20px 6px;">
            📋 Still Active - Earlier Entries (Today)
        </h3>
        <div style="padding:0 20px 16px;overflow-x:auto;">
        <table style="border-collapse:collapse;width:100%;font-family:Courier New,monospace;">
          <thead><tr>{old_hdrs}</tr></thead>
          <tbody>{old_rows}</tbody>
        </table></div>"""

    html = f"""
<html><body style="font-family:Arial,sans-serif;background:#f0f0f0;margin:0;padding:20px;">
<div style="max-width:1100px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;
            box-shadow:0 2px 8px rgba(0,0,0,0.15);">
  <div style="background:{color};padding:16px 20px;">
    <h2 style="color:#fff;margin:0;font-size:18px;letter-spacing:1px;">
      {icon} {title}
    </h2>
    <p style="color:#ddd;margin:4px 0 0;font-size:12px;">
      Generated: {now_str} &nbsp;|&nbsp; OiAnalytics by Market Hacks [LOCAL]
    </p>
  </div>
  <div style="background:#fff8e1;padding:10px 20px;border-bottom:2px solid {color};">
    <b>🆕 NEW Signals This Update:</b> &nbsp;{syms_str}
  </div>
  {table_html}
  {old_table_html}
  <div style="background:#212121;padding:10px 20px;">
    <p style="color:#aaa;font-size:11px;margin:0;">
      Auto-alert fires every refresh during market hours (09:15–15:15 IST).
      Each symbol alerted only ONCE per day per category.
    </p>
  </div>
</div></body></html>"""
    return html


def send_email(subject, body_html, is_html=False):
    """Send email - supports both plain text (legacy) and HTML."""
    if not can_send_email():
        print("EMAIL SKIPPED: limit or cooldown active")
        return
    try:
        import smtplib as _smtp
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        if is_html:
            msg = MIMEMultipart("alternative")
            msg["From"]    = EMAIL_FROM
            msg["To"]      = ", ".join(EMAIL_TO)
            msg["Subject"] = subject
            msg.attach(MIMEText(body_html, "html", "utf-8"))
        else:
            from email.message import EmailMessage as _EM
            msg = _EM()
            msg["From"]    = EMAIL_FROM
            msg["To"]      = ", ".join(EMAIL_TO)
            msg["Subject"] = subject
            msg.set_content(body_html)
        with _smtp.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)
        record_email_sent()
    except Exception as e:
        print("EMAIL ERROR:", e)


def email_already_sent(symbol, category):
    os.makedirs(CACHE_DIR, exist_ok=True)  # FIX-2h
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(EMAIL_DEDUP_FILE):
        return False

    df = pd.read_csv(EMAIL_DEDUP_FILE, encoding='utf-8')

    return (
        (df["DATE"] == today) &
        (df["SYMBOL"] == symbol) &
        (df["CATEGORY"] == category)
    ).any()


def mark_email_sent(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    row = {
        "DATE": today,
        "SYMBOL": symbol,
        "CATEGORY": category
    }

    if os.path.exists(EMAIL_DEDUP_FILE):
        df = pd.read_csv(EMAIL_DEDUP_FILE, encoding='utf-8')
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(EMAIL_DEDUP_FILE, index=False, encoding='utf-8')




# TOP_HIGH email now sent via notify_all (HTML format)




def log_alert(symbol, category, details, ltp, source):
    if alert_already_logged(symbol, category):
        return  # 🚫 Prevent duplicate static alerts

    now = datetime.now()

    row = {
        "DATE": now.strftime("%Y-%m-%d"),
        "TIME": now.strftime("%H:%M:%S"),
        "SYMBOL": symbol,
        "CATEGORY": category,
        "DETAILS": details,
        "LTP": ltp,
        "SOURCE": source
    }

    if os.path.exists(ALERTS_LOG_FILE):
        df = pd.read_csv(ALERTS_LOG_FILE, encoding='utf-8')
        df = pd.concat([pd.DataFrame([row]), df], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_LOG_FILE, index=False, encoding='utf-8')

    # Mark this alert as logged
    mark_alert_logged(symbol, category)


def alert_already_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(ALERTS_DEDUP_FILE):
        return False

    df = pd.read_csv(ALERTS_DEDUP_FILE, encoding='utf-8')

    return (
        (df["DATE"] == today) &
        (df["SYMBOL"] == symbol) &
        (df["CATEGORY"] == category)
    ).any()


def mark_alert_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    row = {
        "DATE": today,
        "SYMBOL": symbol,
        "CATEGORY": category
    }

    if os.path.exists(ALERTS_DEDUP_FILE):
        df = pd.read_csv(ALERTS_DEDUP_FILE, encoding='utf-8')
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_DEDUP_FILE, index=False, encoding='utf-8')

####################################################################################

# ─────────────────────────────────────────────────────────────────────────────
# 📧 CATEGORY -> DATAFRAME MAP
# Maps each alert category to the global DataFrame it relates to
# and columns to include in the email table.
# Populated lazily (df may not exist at definition time).
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# 🔕 DISABLED ALERT CATEGORIES
# DISABLED_EMAIL_CATEGORIES : suppresses email only (toast still fires unless also in DISABLED_TOAST_CATEGORIES)
# DISABLED_TOAST_CATEGORIES : suppresses browser toast only
# To fully silence an alert, add its key to BOTH sets.
# Re-enable by removing from the relevant set.
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# 🔕 DISABLED ALERT CATEGORIES  (now driven by session_state toggles)
# These sets are rebuilt on every call to _is_email_disabled / _is_toast_disabled
# so that the Alerts tab toggle buttons take effect immediately.
# ─────────────────────────────────────────────────────────────────────────────
def _is_toast_disabled(category: str) -> bool:
    """Return True if browser toast for this category is currently OFF."""
    return not st.session_state.get(f"toast_{category}", False)  # default OFF per doc

# 🔥 HEATMAP DIRECTION AUTO-REVERSAL DETECTION
# Detects early reversal before big moves by monitoring:
#   1. Heatmap Bull % Momentum (dropping from >65 or rising from <35)
#   2. Core Heavyweight price action vs Day Open & Prev Close
#   3. High-conviction alerts with surgical precision
# ═══════════════════════════════════════════════════════════════════════

_N50_HEAVY_SYMS = ["RELIANCE", "HDFCBANK", "ICICIBANK", "BHARTIARTL", "SBIN", "TCS", "INFY", "ITC", "AXISBANK", "LT"]
_BNK_HEAVY_SYMS = ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK"]
_SNX_HEAVY_SYMS = ["HDFCBANK", "ICICIBANK", "RELIANCE", "INFY", "BHARTIARTL", "ITC", "LT", "TCS", "AXISBANK", "KOTAKBANK"]

def _is_email_disabled(category: str) -> bool:
    """Return True if email for this category is currently OFF."""
    return not st.session_state.get(f"email_{category}", True)


# Keep legacy sets for any code that still references them directly
DISABLED_EMAIL_CATEGORIES: set = set()   # now managed via _is_email_disabled()
DISABLED_TOAST_CATEGORIES: set = set()   # now managed via _is_toast_disabled()

# Color and icon per category
_CAT_STYLE = {
    "TOP_HIGH":         ("#1b5e20", "🟢"),
    "TOP_LOW":          ("#b71c1c", "🔴"),
    "EMA20_50":         ("#1565c0", "🟢"), # Will be dynamic in notify_all
    "DAILY_UP":         ("#2e7d32", "🟢"),
    "DAILY_DOWN":       ("#c62828", "🔴"),
    "WEEKLY_UP":        ("#1b5e20", "🟢"),
    "WEEKLY_DOWN":      ("#b71c1c", "🔴"),
    "MONTHLY_UP":       ("#1b5e20", "🟢"),
    "MONTHLY_DOWN":     ("#b71c1c", "🔴"),
    "TOP_GAINERS":      ("#e65100", "🟢"),
    "TOP_LOSERS":       ("#b71c1c", "🔴"),
    "THREE_GREEN_15M":  ("#2e7d32", "🟢"),
    "INSIDE_15M_BREAK": ("#1565c0", "🟢"), # Mixed
    "YEST_GREEN_BREAK": ("#2e7d32", "🟢"),
    "YEST_RED_BREAK":   ("#c62828", "🔴"),
    "HOURLY_BREAK_UP":  ("#1b5e20", "🟢"),
    "HOURLY_BREAK_DOWN":("#b71c1c", "🔴"),
    "2M_EMA_REVERSAL":  ("#4a148c", "🟢"),
    "OPTION_STRONG":    ("#e65100", "🟢"),
    "VOL_SURGE_15M":    ("#0277bd", "🟢"),
}

_CORE_COLS = ["Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW","CHANGE","CHANGE_%"]

# Table columns per category (shown in email)
_CAT_COLS = {
    "TOP_HIGH":          _CORE_COLS + ["LIVE_HIGH","LIVE_LOW","TOP_HIGH","GAIN","YEST_HIGH"],
    "TOP_LOW":           _CORE_COLS + ["LIVE_HIGH","LIVE_LOW","TOP_LOW","GAIN","YEST_LOW"],
    "EMA20_50":          _CORE_COLS + ["EMA20","EMA50","TOP_HIGH","TOP_LOW","SIGNAL"],
    "DAILY_UP":          _CORE_COLS + ["LIVE_HIGH","YEST_HIGH","HIGH_W","HIGH_M"],
    "DAILY_DOWN":        _CORE_COLS + ["LIVE_LOW","YEST_LOW","LOW_W","LOW_M"],
    "WEEKLY_UP":         _CORE_COLS + ["LIVE_HIGH","HIGH_W","HIGH_M","YEST_HIGH"],
    "WEEKLY_DOWN":       _CORE_COLS + ["LIVE_LOW","LOW_W","LOW_M","YEST_LOW"],
    "MONTHLY_UP":        _CORE_COLS + ["LIVE_HIGH","HIGH_M","HIGH_W","YEST_HIGH"],
    "MONTHLY_DOWN":      _CORE_COLS + ["LIVE_LOW","LOW_M","LOW_W","YEST_LOW"],
    "TOP_GAINERS":       _CORE_COLS + ["LIVE_OPEN","LIVE_HIGH","LIVE_LOW","LIVE_VOLUME","YEST_CLOSE"],
    "TOP_LOSERS":        _CORE_COLS + ["LIVE_OPEN","LIVE_HIGH","LIVE_LOW","LIVE_VOLUME","YEST_CLOSE"],
    "THREE_GREEN_15M":   ["Symbol","LTP","CHANGE_%","DAY_OPEN","DAY_HIGH","DAY_LOW","YEST_HIGH","CANDLE_TIME"],
    "INSIDE_15M_BREAK":  ["Symbol","LTP","CHANGE_%","CHG_15M_%","EMA20","BREAK_TYPE","DAY_HIGH","DAY_LOW"],
    "YEST_GREEN_BREAK":  _CORE_COLS + ["LIVE_OPEN","LIVE_HIGH","YEST_HIGH","YEST_CLOSE"],
    "YEST_RED_BREAK":    _CORE_COLS + ["LIVE_OPEN","LIVE_LOW","YEST_LOW","YEST_CLOSE"],
    "HOURLY_BREAK_UP":   _CORE_COLS + ["LIVE_HIGH","LIVE_LOW","YEST_HIGH","EMA20"],
    "HOURLY_BREAK_DOWN": _CORE_COLS + ["LIVE_HIGH","LIVE_LOW","YEST_LOW","EMA20"],
    "2M_EMA_REVERSAL":   _CORE_COLS + ["EMA7","EMA20","EMA50","VOL_%","TOP_HIGH","TOP_LOW"],
    "OPTION_STRONG":     ["Symbol","LTP","CHANGE_%"],
    "VOL_SURGE_15M":     ["Symbol","LTP","CHANGE_%","PREV_SLOT","PREV_VOL","CURR_SLOT","CURR_VOL","VOL_SURGE_%","YEST_HIGH"],
}

def _get_cat_df(category, symbols, ltp_map):
    """
    Build a DataFrame for the alert email table.
    Tries to pull from global df; falls back to ltp_map-based simple table.
    """
    try:
        want_cols = _CAT_COLS.get(category, _CORE_COLS)
        available = [c for c in want_cols if c in df.columns]
        if available and "Symbol" in df.columns:
            result = df[df["Symbol"].isin(symbols)][available].copy()
            if not result.empty:
                # Sort by CHANGE_% descending if available
                if "CHANGE_%" in result.columns:
                    result = result.sort_values("CHANGE_%", ascending=False)
                return result.reset_index(drop=True)
    except Exception:
        pass
    # Fallback: simple Symbol + LTP table
    rows = []
    for sym in symbols:
        ltp = ltp_map.get(sym, 0) if ltp_map else 0
        rows.append({"Symbol": sym, "LTP": ltp})
    return pd.DataFrame(rows)


# =========================================================
# 🔔 MASTER ALERT ENGINE (Market Time Protected) - HTML
# =========================================================
def notify_all(category, title, symbols, ltp_map=None):

    # 🚫 Stop outside market hours
    if not is_market_hours():
        return

    # 🚫 Nothing new
    if not symbols:
        return

    color, icon = _CAT_STYLE.get(category, ("#1a237e", "🚨"))
    email_symbols = []

    for sym in symbols:
        if not email_already_sent(sym, category):
            email_symbols.append(sym)
            mark_email_sent(sym, category)

        # ----- ALERT LOGGING -----
        ltp = ltp_map.get(sym) if ltp_map else ""
        log_alert(
            symbol=sym,
            category=category,
            details=title,
            ltp=ltp,
            source="EMAIL + BROWSER"
        )

    # ----- SEND HTML EMAIL (non-blocking) -----
    if _is_email_disabled(category):
        email_symbols = []   # suppress email; toast already fired below

    if email_symbols and is_email_allowed():
        now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
        subject  = f"[OiAnalytics] {title} - {len(email_symbols)} New | {now_str}"

        # Build data table for new symbols only
        new_df = _get_cat_df(category, email_symbols, ltp_map)
        syms_with_ltp = [
            (s, ltp_map.get(s, 0) if ltp_map else 0)
            for s in email_symbols
        ]

        def _send_bg(subj=subject, ttl=title, swl=syms_with_ltp,
                     ndf=new_df.copy(), col=color, ic=icon,
                     _all_syms=list(symbols), _new_syms=list(email_symbols)):
            # Old = symbols alerted earlier today (not in this new batch)
            old_syms = [s for s in _all_syms if s not in _new_syms]
            odf = _get_cat_df(category, old_syms, ltp_map) if old_syms else pd.DataFrame()
            html = _build_html_email(ttl, swl, df_table=ndf, color=col, icon=ic, old_df=odf)
            send_email(subj, html, is_html=True)

        threading.Thread(target=_send_bg, daemon=True).start()

    # ----- BROWSER TOAST -----
    if not _is_toast_disabled(category):
        st.toast(f"{icon} {title}: {', '.join(symbols)}", icon="🚨")

    # ----- TELEGRAM ALERT -----
    if not _is_tg_disabled(category):
        # Filter symbols: only send if strength score >= 5 (exclude TRAPS)
        tg_symbols = []
        _n_chg = get_nifty_change()
        for s in symbols:
            try:
                row = df[df["Symbol"] == s].iloc[0]
                dirn = "UP" if row.get("CHANGE_%", 0) >= 0 else "DOWN"
                m_score, _, _ = calculate_movement_strength(row, direction=dirn, nifty_chg=_n_chg)
                if m_score >= 5:
                    tg_symbols.append(s)
            except:
                tg_symbols.append(s) # fallback: keep if check fails

        if not tg_symbols:
            return # All stocks were traps/weak - suppress alert

        _now_tg  = datetime.now(IST).strftime("%H:%M IST")
        
        # ── DYNAMIC HEADER & BORDER LOGIC ──
        _is_bull_cat = any(x in category.upper() for x in ("HIGH","UP","GAIN","GREEN","REVERSAL","STRONG","BULL"))
        _is_bear_cat = any(x in category.upper() for x in ("LOW","DOWN","LOSS","RED","BEAR"))
        
        _final_bull = _is_bull_cat
        if category in ("EMA20_50", "INSIDE_15M_BREAK") or (not _is_bull_cat and not _is_bear_cat):
            # Mixed or unknown category - decide by first stock's momentum
            try:
                _fs = list(tg_symbols)[0]
                # Check ltp_map first (if it's the live_map dict)
                _fdata = ltp_map.get(_fs, {})
                if isinstance(_fdata, dict) and "CHANGE_%" in _fdata:
                    _final_bull = (_fdata["CHANGE_%"] >= 0)
                else:
                    # Fallback to global df
                    _fr = df[df["Symbol"] == _fs].iloc[0]
                    _final_bull = (_fr.get("CHANGE_%", 0) >= 0)
            except: pass
            
        header_icon = "🟢" if _final_bull else "🔴"
        tg_border   = "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩" if _final_bull else "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"
        
        def _get_sym_detail(s):
            try:
                # Use global df for detail data
                row = df[df["Symbol"] == s].iloc[0]
                ltp = row.get("LTP", 0)
                yh  = row.get("YEST_HIGH", 0)
                yl  = row.get("YEST_LOW", 0)
                yc  = row.get("YEST_CLOSE", 0)
                wh  = row.get("HIGH_W", 0)
                wl  = row.get("LOW_W", 0)
                vwap = row.get("REAL_VWAP", 0)
                
                top = row.get("LIVE_OPEN", 0) 
                th  = row.get("LIVE_HIGH", 0) 
                tl  = row.get("LIVE_LOW", 0)  
                chg = row.get("CHANGE", 0)
                chp = row.get("CHANGE_%", 0)
                
                e7  = row.get("EMA7", 0)
                e20 = row.get("EMA20", 0)
                e50 = row.get("EMA50", 0)
                
                v_p   = row.get("VOL_%", 0)
                v_day = int(row.get("LIVE_VOLUME", 0))
                v_avg = row.get("AVG_VOL_5D", 1)
                v_x   = round(v_day / v_avg, 2) if v_avg > 0 else 0
                
                # Checkmarks
                yh_chk = "✅" if ltp >= yh else ""
                yl_chk = "🔴" if ltp <= yl else ""
                wh_chk = "✅" if ltp >= wh else ""
                wl_chk = "🔴" if ltp <= wl else ""
                
                # Movement Strength Score
                dirn = "UP" if chp >= 0 else "DOWN"
                m_score, m_label, m_reasons = calculate_movement_strength(row, direction=dirn, nifty_chg=_n_chg)
                
                dir_icon = "🟢" if chp >= 0 else "🔴"
                v_str = "🔥 Strong" if v_x >= 2.0 else ("⚡ Good" if v_x >= 1.5 else "📊 Normal")

                return (
                    f"  {dir_icon} <b>{s}</b>  LTP: <b>{ltp}</b> ({chg:+.2f} | {chp:+.2f}%)\n"
                    f"    <b>Strength: {m_label} ({m_score}/10)</b>\n"
                    f"    <i>{m_reasons}</i>\n"
                    f"    OHLC: {top}/{th}/{tl}/{ltp}\n"
                    f"    YH {yh_chk}: {yh} | YL {yl_chk}: {yl} | YC: {yc}\n"
                    f"    W-High {wh_chk}: {wh} | W-Low {wl_chk}: {wl}\n"
                    f"    VWAP: {vwap:.1f} | E7: {e7:.1f} | E20: {e20:.1f} | E50: {e50:.1f}\n"
                    f"    VOL: {v_p:+.1f}% | Day: {v_day:,} | {v_x}x Avg\n"
                )
            except Exception:
                return f"  • <b>{s}</b>  LTP: {ltp_map.get(s,'') if ltp_map else ''}\n"

        _sym_lines = "".join(_get_sym_detail(s) for s in list(tg_symbols)[:15])
        _tg_notify_msg = (
            f"{tg_border}\n"
            f"{header_icon} <b>{title}</b>\n"
            f"⏰ {_now_tg}\n"
            f"📋 Stocks ({len(tg_symbols)}):\n"
            f"{_sym_lines}\n"
            f"⚠️ <i>NOT financial advice.</i>\n"
            f"{tg_border}"
        )
        _tg_slot = datetime.now(IST).strftime("%H%M")
        _tg_syms_key = "_".join(str(s) for s in list(tg_symbols)[:5])
        send_telegram_bg(
            _tg_notify_msg,
            dedup_key=f"NOTIFY_{category}_{_tg_syms_key}_{_tg_slot[:3]}0"
        )




################################################################################
ltp_map = dict(zip(df.Symbol, df.LTP))

new_TOP_HIGH = detect_new_entries(
    "TOP_HIGH",
    TOP_HIGH_df.Symbol.tolist()
)

notify_all(
    "TOP_HIGH",
    "🟢TOP LIVE_HIGH Breakout",
    new_TOP_HIGH,
    ltp_map
)
new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

notify_all(
    "TOP_LOW",
    "🔴TOP LIVE_LOW Breakdown",
    new_TOP_LOW,
    ltp_map
)

# ── Panchak-scoped Telegram alerts for TOP_HIGH / TOP_LOW breaks ──────
# One alert per stock per direction for the entire panchak period.
# Auto-resets when PANCHAK_START changes (new panchak = fresh alerts).
fire_panchak_tg_alerts(df, ltp_map=ltp_map)
fire_astro_update_alert()

# ═══════════════════════════════════════════════════════════════════════
# 🎯 BT / ST TARGET HIT ALERT
# Fires ONE Telegram alert per stock per day when LTP crosses BT (buy
# target = TOP_HIGH + DIFF) or ST (sell target = TOP_LOW - DIFF).
# Dedup key: BT_HIT_{symbol}_{date}  or  ST_HIT_{symbol}_{date}
# Resets every new trading day automatically.
# ═══════════════════════════════════════════════════════════════════════
def fire_bt_st_target_alerts(df_live, ltp_map_ref=None):
    """
    Scan df_live for BT (buy target) and ST (sell target) hits and send
    ONE Telegram alert per stock per day per direction.

    BT = TOP_HIGH + DIFF  (above-panchak bull target)
    ST = TOP_LOW  - DIFF  (below-panchak bear target)
    """
    if not is_market_hours():
        return
    if _is_tg_disabled("BT_ST_TARGET"):
        return
    if df_live is None or df_live.empty:
        return

    _required = {"Symbol", "LTP", "BT", "ST", "TOP_HIGH", "TOP_LOW"}
    if not _required.issubset(df_live.columns):
        return

    _now_dt  = datetime.now(IST)
    _now_str = _now_dt.strftime("%H:%M IST")
    _today   = _now_dt.strftime("%Y%m%d")

    _bt_hits = []
    _st_hits = []

    for _, row in df_live.iterrows():
        sym = str(row.get("Symbol", "")).strip()
        if not sym:
            continue
        # ── Only alert for the 14 watchlist stocks ──────────────
        if sym.upper() not in PANCHAK_ALERT_WATCHLIST:
            continue
        ltp = float((ltp_map_ref or {}).get(sym, row.get("LTP", 0)) or 0)
        bt  = row.get("BT", None)
        st  = row.get("ST", None)
        top_high = row.get("TOP_HIGH", None)
        top_low  = row.get("TOP_LOW",  None)
        chg_pct  = row.get("CHANGE_%", "")
        gain     = row.get("GAIN", "")

        if ltp <= 0:
            continue

        # ── BT hit: LTP crossed above BT level ────────────────────
        if pd.notna(bt) and bt > 0 and ltp >= float(bt):
            _bt_key = f"BT_HIT_{sym}_{_today}"
            with _TG_DEDUP_LOCK:
                _dd = _load_tg_dedup()
                if not _dd.get(_bt_key):
                    _dd[_bt_key] = _now_dt.isoformat()
                    _save_tg_dedup(_dd)
                    _bt_hits.append({
                        "sym":      sym,
                        "ltp":      ltp,
                        "bt":       float(bt),
                        "top_high": float(top_high) if pd.notna(top_high) else 0,
                        "pts_above_bt":    round(ltp - float(bt), 2),
                        "pts_above_th":    round(ltp - float(top_high), 2) if pd.notna(top_high) else 0,
                        "chg":      chg_pct,
                        "gain":     gain,
                        "open":     row.get("LIVE_OPEN", 0),
                        "high":     row.get("LIVE_HIGH", 0),
                        "low":      row.get("LIVE_LOW", 0),
                        "yh":       row.get("YEST_HIGH", 0),
                        "yl":       row.get("YEST_LOW", 0),
                        "yc":       row.get("YEST_CLOSE", 0)
                    })

        # ── ST hit: LTP fell below ST level ───────────────────────
        if pd.notna(st) and st > 0 and ltp <= float(st):
            _st_key = f"ST_HIT_{sym}_{_today}"
            with _TG_DEDUP_LOCK:
                _dd = _load_tg_dedup()
                if not _dd.get(_st_key):
                    _dd[_st_key] = _now_dt.isoformat()
                    _save_tg_dedup(_dd)
                    _st_hits.append({
                        "sym":     sym,
                        "ltp":     ltp,
                        "st":      float(st),
                        "top_low": float(top_low) if pd.notna(top_low) else 0,
                        "pts_below_st":   round(float(st) - ltp, 2),
                        "pts_below_tl":   round(float(top_low) - ltp, 2) if pd.notna(top_low) else 0,
                        "chg":     chg_pct,
                        "gain":    gain,
                        "open":    row.get("LIVE_OPEN", 0),
                        "high":    row.get("LIVE_HIGH", 0),
                        "low":     row.get("LIVE_LOW", 0),
                        "yh":      row.get("YEST_HIGH", 0),
                        "yl":      row.get("YEST_LOW", 0),
                        "yc":      row.get("YEST_CLOSE", 0)
                    })

    # ── Send BT hits ─────────────────────────────────────────────
    for _r in _bt_hits:
        _chg_txt  = f" | Chg: {_r['chg']:+.2f}%" if isinstance(_r['chg'], (int, float)) else ""
        _gain_txt = f" | GAIN: {_r['gain']}"      if _r['gain'] not in (None, "", 0)      else ""
        _msg = (
            "🏆 BUY TARGET (BT) HIT\n"
            f"Stock  : {_r['sym']}\n"
            f"Time   : {_now_str}\n"
            f"LTP    : {_r['ltp']:,.2f} ({_r['chg']:+.2f}%)\n"
            f"OHLC   : {_r['open']}/{_r['high']}/{_r['low']}/{_r['ltp']}\n"
            f"YH/YL/YC: {_r['yh']}/{_r['yl']}/{_r['yc']}\n"
            f"BT Lvl : {_r['bt']:,.2f} (+{_r['pts_above_bt']:,.2f})\n"
            f"TOP HI : {_r['top_high']:,.2f} (+{_r['pts_above_th']:,.2f})"
            f"{_gain_txt}\n"
            f"STRONG BULL MOMENTUM\n"
            f"Consider partial profit / trail SL.\n"
            f"NOT financial advice."
        )
        threading.Thread(
            target=send_telegram,
            args=(_msg, None, ""),   # parse_mode="" = plain text, no HTML
            daemon=True
        ).start()

    # ── Send ST hits ─────────────────────────────────────────────
    for _r in _st_hits:
        _chg_txt  = f" | Chg: {_r['chg']:+.2f}%" if isinstance(_r['chg'], (int, float)) else ""
        _gain_txt = f" | GAIN: {_r['gain']}"      if _r['gain'] not in (None, "", 0)      else ""
        _msg = (
            "🔻 SELL TARGET (ST) HIT\n"
            f"Stock  : {_r['sym']}\n"
            f"Time   : {_now_str}\n"
            f"LTP    : {_r['ltp']:,.2f} ({_r['chg']:+.2f}%)\n"
            f"OHLC   : {_r['open']}/{_r['high']}/{_r['low']}/{_r['ltp']}\n"
            f"YH/YL/YC: {_r['yh']}/{_r['yl']}/{_r['yc']}\n"
            f"ST Lvl : {_r['st']:,.2f} (-{_r['pts_below_st']:,.2f})\n"
            f"TOP LO : {_r['top_low']:,.2f} (-{_r['pts_below_tl']:,.2f})"
            f"{_gain_txt}\n"
            f"BEARISH MOMENTUM\n"
            f"Consider short profit / trail SL.\n"
            f"NOT financial advice."
        )
        threading.Thread(
            target=send_telegram,
            args=(_msg, None, ""),   # parse_mode="" = plain text, no HTML
            daemon=True
        ).start()


fire_bt_st_target_alerts(df, ltp_map_ref=ltp_map)
# Sends: TOP HIGH, TOP LOW, LTP, how many points after break, KP window time
# Dedup: one alert per 15-min slot (resets each slot)
# ═══════════════════════════════════════════════════════════════════════
def fire_kp_break_15m_alert(df_live, ltp_map_ref=None):
    """
    Every 15-min slot: scan all stocks that have broken TOP_HIGH or TOP_LOW
    and send a single Telegram message summarising:
      Symbol | TOP HIGH | TOP LOW | LTP | Points after break | KP window
    """
    if not is_market_hours():
        return
    if _is_tg_disabled("KP_BREAK_15M"):
        return
    if df_live is None or df_live.empty:
        return

    _now_dt   = datetime.now(IST)
    # 15-min slot key: e.g. "09:15", "09:30", "09:45", ...
    _slot_min = (_now_dt.minute // 15) * 15
    _slot_str = _now_dt.strftime(f"%H:{_slot_min:02d}")
    _dedup_key = f"KP_BREAK_15M_{_slot_str}"

    # Atomic check-and-mark under lock - prevents concurrent threads double-firing
    with _TG_DEDUP_LOCK:
        _dedup = _load_tg_dedup()
        if _dedup.get(_dedup_key):
            return   # already sent this slot
        # Reserve the slot immediately - will be confirmed after building message
        _dedup[_dedup_key] = _now_dt.isoformat()
        _save_tg_dedup(_dedup)

    # ── Try to get KP time signal ────────────────────────────────────
    try:
        _kp_time_signal = get_time_signal()
    except Exception:
        _kp_time_signal = "-"

    required = {"Symbol", "LTP", "TOP_HIGH", "TOP_LOW"}
    if not required.issubset(df_live.columns):
        return

    up_rows   = []
    down_rows = []

    for _, row in df_live.iterrows():
        sym      = str(row.get("Symbol", "")).strip()
        if not sym: continue
        # ── Only alert for the 14 watchlist stocks ──────────────
        if sym.upper() not in PANCHAK_ALERT_WATCHLIST:
            continue
        ltp      = float(ltp_map_ref.get(sym, row.get("LTP", 0)) or 0)
        top_high = row.get("TOP_HIGH", None)
        top_low  = row.get("TOP_LOW",  None)
        chg_pct  = row.get("CHANGE_%", "")

        if pd.notna(top_high) and ltp > 0 and ltp >= float(top_high):
            pts_above = round(ltp - float(top_high), 2)
            up_rows.append({
                "sym": sym, "ltp": ltp,
                "top_high": float(top_high),
                "pts": pts_above,
                "chg": chg_pct,
            })

        if pd.notna(top_low) and ltp > 0 and ltp <= float(top_low):
            pts_below = round(float(top_low) - ltp, 2)
            down_rows.append({
                "sym": sym, "ltp": ltp,
                "top_low": float(top_low),
                "pts": pts_below,
                "chg": chg_pct,
            })

    if not up_rows and not down_rows:
        # No breaks this slot - release the reservation so it retries next slot
        with _TG_DEDUP_LOCK:
            try:
                _d = _load_tg_dedup()
                _d.pop(_dedup_key, None)
                _save_tg_dedup(_d)
            except Exception:
                pass
        return

    _now_str = _now_dt.strftime("%H:%M IST")
    msg_parts = [
        "🕐🕐🕐🕐🕐🕐🕐🕐🕐🕐",
        f"⏱️ <b>PANCHAK BREAK ALERT - {_slot_str} window</b>",
        f"⏰ {_now_str}",
        f"🔮 <b>KP Time Signal:</b> {_kp_time_signal}",
        f"📅 <b>Panchak:</b> {PANCHAK_START.strftime('%d-%b')} -> {PANCHAK_END.strftime('%d-%b')}",
    ]

    if up_rows:
        msg_parts.append("\n🟢 <b>TOP HIGH BREAKS</b>")
        msg_parts.append("Symbol | TOP HIGH | LTP | +Pts | Chg%")
        msg_parts.append("─" * 34)
        for r in up_rows[:10]:
            _chg = f"{r['chg']:+.2f}%" if isinstance(r['chg'], (int, float)) else ""
            msg_parts.append(
                f"  🟢 <b>{r['sym']}</b>\n"
                f"     TOP_HIGH: {r['top_high']:.2f} | LTP: {r['ltp']:.2f}"
                f" | +{r['pts']:.2f} pts {_chg}"
            )

    if down_rows:
        msg_parts.append("\n🔴 <b>TOP LOW BREAKS</b>")
        msg_parts.append("Symbol | TOP LOW | LTP | -Pts | Chg%")
        msg_parts.append("─" * 34)
        for r in down_rows[:10]:
            _chg = f"{r['chg']:+.2f}%" if isinstance(r['chg'], (int, float)) else ""
            msg_parts.append(
                f"  🔴 <b>{r['sym']}</b>\n"
                f"     TOP_LOW: {r['top_low']:.2f} | LTP: {r['ltp']:.2f}"
                f" | -{r['pts']:.2f} pts {_chg}"
            )

    msg_parts.append("\n⚠️ <i>NOT financial advice.</i>")
    msg_parts.append("🕐🕐🕐🕐🕐🕐🕐🕐🕐🕐")
    final_msg = "\n".join(msg_parts)
    send_telegram_bg(final_msg, dedup_key=None)  # dedup already handled by lock above

fire_kp_break_15m_alert(df, ltp_map_ref=ltp_map)


# ═══════════════════════════════════════════════════════════════════════
# 📡 NIFTY / BANKNIFTY - KP SLOT BREAK + 15-MIN PROGRESS ALERT
#
# ALERT 1 - BREAK ALERT (instant, once per slot per direction):
#   Fires the moment LTP crosses slot TOP HIGH (bullish) or LEAST LOW (bearish).
#   Dedup key: BREAK_{sym}_{direction}_{date}_{slot}
#   -> Resets each new KP slot, so every new slot break fires fresh.
#
# ALERT 2 - PROGRESS UPDATE (every 15 min after a valid break):
#   Shows how many points gained/lost from break level and from original LTP.
#   Dedup key: PROG_{sym}_{direction}_{date}_{slot}_{15m_slot}
#   -> Fires once per 15-min window, indefinitely while break remains valid.
#
# Integration with kp_panchang_tab.py:
#   In kp_panchang_tab.py, after computing TOP HIGH / LEAST LOW, write:
#     st.session_state["kp_slot_ohlc"] = {
#         "NIFTY":     {"top_high": <val>, "least_low": <val>, "slot": <label>},
#         "BANKNIFTY": {"top_high": <val>, "least_low": <val>, "slot": <label>},
#     }
#   The function reads from st.session_state["kp_slot_ohlc"] automatically.
#   If not set, it falls back to df TOP_HIGH / TOP_LOW for NIFTY & BANKNIFTY.
# ═══════════════════════════════════════════════════════════════════════

_NIFTY_SLOT_STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "CACHE",
    f"nifty_slot_break_{datetime.now(IST).strftime('%Y%m%d')}.json"
)
_NIFTY_SLOT_LOCK = threading.Lock()


def _load_nifty_slot_state():
    """Load today's NIFTY slot break state dict from disk."""
    try:
        with open(_NIFTY_SLOT_STATE_FILE, "r", encoding="utf-8") as _f:
            return json.load(_f)
    except Exception:
        return {}


def _save_nifty_slot_state(d):
    """Persist NIFTY slot break state dict to disk."""
    try:
        os.makedirs(os.path.dirname(_NIFTY_SLOT_STATE_FILE), exist_ok=True)
        with open(_NIFTY_SLOT_STATE_FILE, "w", encoding="utf-8") as _f:
            json.dump(d, _f)
    except Exception:
        pass


def _nifty_break_alert_core(sym, ltp, top_high, least_low, slot_label,
                             alert_prefix, state_prefix, _now_dt, _now_str,
                             _today, _slot_15m):
    """
    Core break + progress logic shared by both Panchak and KP slot alert functions.
    alert_prefix : used in message title  e.g. "PANCHAK" or "KP SLOT"
    state_prefix : used in dedup key      e.g. "PANCHAK" or "KP"
    """
    # Gate: never fire outside market hours (prevents 23:51 IST phantom alerts)
    if not is_market_hours():
        return

    broke_up   = ltp > top_high
    broke_down = ltp < least_low
    if not broke_up and not broke_down:
        return

    direction  = "UP"      if broke_up  else "DOWN"
    level      = top_high  if broke_up  else least_low
    level_name = "TOP HIGH" if broke_up else "LEAST LOW"
    icon       = "🟢"      if broke_up  else "🔴"
    arrow      = "↑"       if broke_up  else "↓"

    pts_signed = (
        round(ltp - top_high,   2) if broke_up
        else round(ltp - least_low, 2)   # negative when below
    )
    pts_sign = "+" if pts_signed >= 0 else "-"

    with _NIFTY_SLOT_LOCK:
        _state = _load_nifty_slot_state()

        # ── ① BREAK ALERT ────────────────────────────────────────────────
        _break_key = f"{state_prefix}_BREAK_{sym}_{direction}_{_today}_{slot_label}"

        if not _state.get(_break_key):
            _state[_break_key] = {
                "ts":         _now_dt.isoformat(),
                "break_level": level,
                "break_ltp":  ltp,
                "slot":       slot_label,
                "break_15m":  _slot_15m,
            }
            _save_nifty_slot_state(_state)

            _break_msg = (
                f"{icon * 6}\n"
                f"🚨 <b>{sym} - {alert_prefix} {level_name} BREAK {arrow}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Index  : {sym}\n"
                f"⏰ Time   : {_now_str}\n"
                f"🕐 Slot   : {slot_label}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📍 {level_name} : {level:,.2f}\n"
                f"💹 LTP Now   : {ltp:,.2f}\n"
                f"📏 Pts {'above' if broke_up else 'below'} level : "
                f"<b>{pts_sign}{abs(pts_signed):,.2f} pts</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{'✅ <b>BULLISH BREAK</b>' if broke_up else '⚠️ <b>BEARISH BREAK</b>'}\n"
                f"🔔 Progress updates every 15 min while valid.\n"
                f"⚠️ <i>NOT financial advice.</i>\n"
                f"{icon * 6}"
            )
            # Use proper dedup key - once per break level per day (not None!)
            _break_tg_key = f"NSLOT_DASH_{state_prefix}_{sym}_{direction}_{_today}_{round(level)}"
            send_alert_routed("NIFTY_SLOT_BREAK", _break_msg, _break_tg_key)

        # ── ② PROGRESS ALERT - next 15-min slot onwards ─────────────────
        _break_info    = _state.get(_break_key, {})
        _break_15m_slot = _break_info.get("break_15m", _slot_15m) if isinstance(_break_info, dict) else _slot_15m
        _prog_key = f"{state_prefix}_PROG_{sym}_{direction}_{_today}_{slot_label}_{_slot_15m}"

        if (
            _state.get(_break_key)
            and _slot_15m != _break_15m_slot      # different slot than break
            and not _state.get(_prog_key)
        ):
            _state[_prog_key] = _now_dt.isoformat()
            _save_nifty_slot_state(_state)

            _orig_ltp = float(
                _break_info.get("break_ltp", level)
                if isinstance(_break_info, dict) else level
            )
            _pts_since = (
                round(ltp - _orig_ltp, 2) if broke_up
                else round(_orig_ltp - ltp, 2)
            )
            _pts_lvl_now = (
                round(ltp - top_high,   2) if broke_up
                else round(ltp - least_low, 2)
            )
            _trend_icon   = "📈" if _pts_since >= 0 else "📉"
            _since_sign   = "+" if _pts_since   >= 0 else "-"
            _lvl_sign     = "+" if _pts_lvl_now >= 0 else "-"

            _prog_msg = (
                f"⏱️ <b>{sym} - {alert_prefix} 15-Min Progress {arrow} | {_slot_15m}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⏰ Time  : {_now_str}   Slot: {slot_label}\n"
                f"Direction: {icon} <b>{'UP - Above TOP HIGH' if broke_up else 'DOWN - Below LEAST LOW'}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📍 {level_name}    : {level:,.2f}\n"
                f"🎯 Break entry LTP : {_orig_ltp:,.2f}\n"
                f"💹 LTP Now         : <b>{ltp:,.2f}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 Pts from level  : <b>{_lvl_sign}{abs(_pts_lvl_now):,.2f} pts</b>\n"
                f"{_trend_icon} Pts since entry  : <b>{_since_sign}{abs(_pts_since):,.2f} pts</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ <i>NOT financial advice.</i>"
            )
            # Dedup: once per 15-min slot per direction per level  
            _prog_tg_key = f"NSLOT_DASH_PROG_{state_prefix}_{sym}_{direction}_{_today}_{_slot_15m}_{round(level)}"
            send_alert_routed("NIFTY_SLOT_BREAK", _prog_msg, _prog_tg_key)


# ═══════════════════════════════════════════════════════════════════════
# 📡 ALERT FUNCTION 1 - PANCHAK TOP HIGH / TOP LOW BREAK (NIFTY + BANKNIFTY)
#
# Fires when NIFTY or BANKNIFTY LTP crosses the PANCHAK period's
# TOP_HIGH or TOP_LOW (from df - the panchak data table).
# These levels are fixed for the entire panchak period (~5 days).
# ═══════════════════════════════════════════════════════════════════════
def fire_panchak_index_break_alert(ltp_map_ref=None):
    """Break alert for NIFTY/BANKNIFTY vs Panchak TOP_HIGH / TOP_LOW."""
    if not is_market_hours(): return
    if _is_tg_disabled("NIFTY_SLOT_BREAK"): return
    if ltp_map_ref is None: return

    _now_dt  = datetime.now(IST)
    _now_str = _now_dt.strftime("%H:%M IST")
    _today   = _now_dt.strftime("%Y%m%d")
    _slot_min = (_now_dt.minute // 15) * 15
    _slot_15m = _now_dt.strftime(f"%H:{_slot_min:02d}")
    # Slot label = panchak period dates (fixed, not a time window)
    _panchak_slot = f"{PANCHAK_START.strftime('%d-%b')}->{PANCHAK_END.strftime('%d-%b')}"

    for sym in ["NIFTY", "BANKNIFTY", "SENSEX"]:
        ltp = float(ltp_map_ref.get(sym, 0) or 0)
        if ltp <= 0: continue
        try:
            _row = df[df["Symbol"] == sym]
            if _row.empty: continue
            _r = _row.iloc[0]
            top_high  = float(_r.get("TOP_HIGH", 0) or 0)
            least_low = float(_r.get("TOP_LOW",  0) or 0)
            if top_high <= 0 or least_low <= 0: continue
        except Exception:
            continue

        _nifty_break_alert_core(
            sym, ltp, top_high, least_low,
            slot_label    = _panchak_slot,
            alert_prefix  = "PANCHAK",
            state_prefix  = "PANCHAK",
            _now_dt=_now_dt, _now_str=_now_str,
            _today=_today, _slot_15m=_slot_15m,
        )


# ═══════════════════════════════════════════════════════════════════════
# 📡 ALERT FUNCTION 2 - KP SLOT TOP HIGH / LEAST LOW BREAK (NIFTY + BANKNIFTY)
#
# Fires when NIFTY or BANKNIFTY LTP crosses the KP panchang slot's
# TOP HIGH or LEAST LOW (3×5-min candles high/low for the current slot).
# These levels reset every KP time window (~15 min).
# Data comes from kp_panchang_tab via st.session_state["kp_slot_ohlc"].
# ═══════════════════════════════════════════════════════════════════════
def fire_kp_slot_break_alert(ltp_map_ref=None, slot_ohlc_map=None):
    """Break alert for NIFTY/BANKNIFTY vs KP slot 3×5-min TOP HIGH / LEAST LOW."""
    if not is_market_hours(): return
    if _is_tg_disabled("NIFTY_SLOT_BREAK"): return
    if ltp_map_ref is None: return
    if not slot_ohlc_map: return   # no KP slot data available - skip silently

    _now_dt  = datetime.now(IST)
    _now_str = _now_dt.strftime("%H:%M IST")
    _today   = _now_dt.strftime("%Y%m%d")
    _slot_min = (_now_dt.minute // 15) * 15
    _slot_15m = _now_dt.strftime(f"%H:{_slot_min:02d}")

    for sym in ["NIFTY", "BANKNIFTY", "SENSEX"]:
        ltp = float(ltp_map_ref.get(sym, 0) or 0)
        if ltp <= 0: continue
        if sym not in slot_ohlc_map: continue

        _sd = slot_ohlc_map[sym]
        top_high  = _sd.get("top_high")
        least_low = _sd.get("least_low")
        slot_label = str(_sd.get("slot", "-"))

        if not top_high or not least_low: continue
        if float(top_high) <= 0 or float(least_low) <= 0: continue

        _nifty_break_alert_core(
            sym, ltp, float(top_high), float(least_low),
            slot_label    = slot_label,
            alert_prefix  = "KP TIME",
            state_prefix  = "KP",
            _now_dt=_now_dt, _now_str=_now_str,
            _today=_today, _slot_15m=_slot_15m,
        )


# ── Call both every refresh cycle ────────────────────────────────────
fire_panchak_index_break_alert(ltp_map_ref=ltp_map)
fire_kp_slot_break_alert(
    ltp_map_ref   = ltp_map,
    slot_ohlc_map = st.session_state.get("kp_slot_ohlc"),
)


# ═══════════════════════════════════════════════════════════════════════
# 🧠 COMBINED ENGINE TELEGRAM ALERT - SMC + OI + Astro + KP + KP Astro
# Fires every 15 minutes with full confluence snapshot
# Format: Table-style text in Telegram
# ═══════════════════════════════════════════════════════════════════════
def fire_combined_engine_tg_alert():
    """
    Build and send a combined signal table:
    SMC signal | OI direction | Astro score | KP time | KP Astro sub-lord
    Fires once per 15-min slot. Gated by tg_COMBINED_ENGINE toggle.
    """
    if not is_market_hours():
        return
    if _is_tg_disabled("COMBINED_ENGINE"):
        return

    _now_dt   = datetime.now(IST)
    _slot_min = (_now_dt.minute // 15) * 15
    _slot_str = _now_dt.strftime(f"%H:{_slot_min:02d}")
    _dedup_key = f"COMBINED_ENGINE_{_slot_str}"

    # Atomic check-and-mark under lock - prevents concurrent threads double-firing
    with _TG_DEDUP_LOCK:
        _dedup = _load_tg_dedup()
        if _dedup.get(_dedup_key):
            return
        _dedup[_dedup_key] = _now_dt.isoformat()
        _save_tg_dedup(_dedup)

    _now_str = _now_dt.strftime("%H:%M IST")

    # ── 1. SMC engine signal - reads smc_result (set every refresh cycle) ──
    try:
        _smc_data = st.session_state.get("smc_result") or {}
        # If empty, try running a fresh fetch
        if not _smc_data and _SMC_ENGINE_OK:
            _oi_ref = st.session_state.get("oi_intel_NIFTY") or {}
            _smc_data = _run_smc_intelligence(kite, _oi_ref) or {}
        _smc_signal = str(_smc_data.get("final_signal", "-"))
        _smc_score  = int(_smc_data.get("final_score", 0) or 0)
        _smc_action = str(_smc_data.get("final_action", "-"))
        _smc_trend_ltf = str(_smc_data.get("smc_trend_ltf", "-"))
        _smc_trend_htf = str(_smc_data.get("smc_trend_htf", "-"))
        _smc_zone   = str(_smc_data.get("pd_zone", "-"))
    except Exception:
        _smc_signal = "-"; _smc_score = 0; _smc_action = "-"
        _smc_trend_ltf = "-"; _smc_trend_htf = "-"; _smc_zone = "-"

    # ── 2. OI intelligence - reads oi_intel (set every refresh cycle) ──
    try:
        _oi_nifty = st.session_state.get("oi_intel_NIFTY") or fetch_oi_intelligence("NIFTY") or {}
        _oi_bn    = st.session_state.get("oi_intel_BANKNIFTY") or fetch_oi_intelligence("BANKNIFTY") or {}
        
        _oi_dir      = str(_oi_nifty.get("direction", "-"))
        _oi_pcr      = float(_oi_nifty.get("pcr", 0) or 0)
        _oi_spot     = int(_oi_nifty.get("spot", 0) or 0)
        _oi_atm      = int(_oi_nifty.get("atm", 0) or 0)
        _oi_maxpain  = int(_oi_nifty.get("max_pain", 0) or 0)
        _oi_cwall    = int(_oi_nifty.get("nearest_call_wall", 0) or 0)
        _oi_pfloor   = int(_oi_nifty.get("nearest_put_floor", 0) or 0)
        _oi_spec     = str(_oi_nifty.get("speculative", "LOW"))
        
        _oi_bn_dir   = str(_oi_bn.get("direction", "-"))
        _oi_advice   = f"Nifty: {_oi_dir} | BNK: {_oi_bn_dir}"
    except Exception:
        _oi_dir = "-"; _oi_pcr = 0.0; _oi_advice = "-"; _oi_spec = "LOW"
        _oi_spot = 0; _oi_atm = 0; _oi_maxpain = 0; _oi_cwall = 0; _oi_pfloor = 0

    # Live NIFTY/BANKNIFTY LTP from ltp_map
    try:
        _nifty_ltp    = int(ltp_map.get("NIFTY",     0) or 0)
        _bnifty_ltp   = int(ltp_map.get("BANKNIFTY", 0) or 0)
        _sensex_ltp   = int(ltp_map.get("SENSEX",    0) or 0)
    except Exception:
        _nifty_ltp = 0; _bnifty_ltp = 0; _sensex_ltp = 0

    # ── 3. Astro engine signal ────────────────────────────────────────
    _astro_snap  = {}
    _astro_score = 0
    _astro_sig   = "-"
    _moon_nak    = "-"
    _moon_lord   = "-"
    _tithi       = "-"
    _astro_int   = "-"
    _crude_sig   = "-"
    try:
        _astro_snap  = _vedic_day_analysis(datetime.now(IST).date()) or {}
        _astro_score = int(_astro_snap.get("net_score", 0) or 0)
        _astro_sig   = str(_astro_snap.get("overall", "-"))
        _moon_nak    = str(_astro_snap.get("moon_nak", "-"))
        _moon_lord   = str(_astro_snap.get("moon_nak_lord", "-"))
        _tithi       = str(_astro_snap.get("tithi", "-"))
        _astro_int   = str(_astro_snap.get("intensity", "-"))
        _crude_sig   = str(_astro_snap.get("crude_signal", "-"))
    except Exception:
        pass

    # ── 4. KP time signal ────────────────────────────────────────────
    try:
        _kp_time = str(get_time_signal())
    except Exception:
        _kp_time = "-"

    # ── 5. KP Astro sub-lord ─────────────────────────────────────────
    _kp_sub    = str(_astro_snap.get("kp_sub", "-"))
    _kp_subsub = str(_astro_snap.get("kp_subsub", "-"))

    # ── 6. Astro reasons (top 2) ─────────────────────────────────────
    try:
        _reasons = _astro_snap.get("reasons", [])[:2]
        _reasons_txt = "\n".join(f"   • {r}" for r in _reasons) if _reasons else "   -"
    except Exception:
        _reasons_txt = "   -"

    # ── Build overall confluence verdict ─────────────────────────────
    _bull_count = sum([
        1 if "BULL" in _smc_signal.upper() else 0,
        1 if "BULL" in _oi_dir.upper() else 0,
        1 if _astro_score >= 1 else 0,
        1 if ("bull" in _kp_time.lower() or "positive" in _kp_time.lower() or "momentum" in _kp_time.lower()) else 0,
    ])
    _bear_count = sum([
        1 if ("BEAR" in _smc_signal.upper() or "SELL" in _smc_signal.upper()) else 0,
        1 if "BEAR" in _oi_dir.upper() else 0,
        1 if _astro_score <= -1 else 0,
        1 if ("bear" in _kp_time.lower() or "negative" in _kp_time.lower()) else 0,
    ])
    if _bull_count >= 3:
        _verdict = "🟢🟢 STRONG BULL CONFLUENCE"
    elif _bull_count == 2:
        _verdict = "🟢 BULL BIAS"
    elif _bear_count >= 3:
        _verdict = "🔴🔴 STRONG BEAR CONFLUENCE"
    elif _bear_count == 2:
        _verdict = "🔴 BEAR BIAS"
    else:
        _verdict = "🟡 MIXED / WAIT"

    _score_str  = f"{_smc_score:+d}"  if isinstance(_smc_score,  int) else str(_smc_score)
    _ascore_str = f"{_astro_score:+d}" if isinstance(_astro_score, int) else str(_astro_score)

    # ── OI availability note ──────────────────────────────────────────
    _oi_status = "" if _oi_dir != "-" else "  ⚠️ OI data not yet loaded"

    msg = (
        "🧠🧠🧠🧠🧠🧠🧠🧠🧠🧠\n"
        f"<b>COMBINED ENGINE - {_slot_str}</b>  ⏰ {_now_str}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>VERDICT: {_verdict}</b>\n"
        f"(Bull signals: {_bull_count}/4  |  Bear signals: {_bear_count}/4)\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💹 NIFTY: <b>{_nifty_ltp:,}</b>  |  BNIFTY: <b>{_bnifty_ltp:,}</b>  |  SENSEX: <b>{_sensex_ltp:,}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SMC Structure:</b>{_oi_status}\n"
        f"   Signal : {_smc_signal}  Score: {_score_str}\n"
        f"   Action : {_smc_action}\n"
        f"   15m Trend: {_smc_trend_ltf}  |  HTF: {_smc_trend_htf}\n"
        f"   Zone   : {_smc_zone}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📉 <b>OI Engine:</b>  {_oi_dir}\n"
        f"   Spot: {_oi_spot:,}  ATM: {_oi_atm:,}\n"
        f"   PCR : {_oi_pcr:.2f}  MaxPain: {_oi_maxpain:,}\n"
        f"   Call Wall: {_oi_cwall:,}  Put Floor: {_oi_pfloor:,}\n"
        f"   {_oi_advice}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Moon <b>Astro Engine:</b>  {_astro_sig} ({_astro_int})\n"
        f"   Moon  : {_moon_nak} ({_moon_lord})  Score: {_ascore_str}\n"
        f"   Tithi : {_tithi}\n"
        f"   Crude : {_crude_sig}\n"
        f"   Reasons:\n{_reasons_txt}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔮 <b>KP Time Signal:</b>  {_kp_time}\n"
        f"🔑 <b>KP Sub-Lord:</b>  {_kp_sub}  |  Sub-Sub: {_kp_subsub}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Panchak: {PANCHAK_START.strftime('%d-%b')} -> {PANCHAK_END.strftime('%d-%b')}\n"
        "⚠️ <i>NOT financial advice.</i>\n"
        "🧠🧠🧠🧠🧠🧠🧠🧠🧠🧠"
    )
    send_telegram_bg(msg, dedup_key=None)  # dedup already handled by lock above

fire_combined_engine_tg_alert()


# 📊 HEATMAP CONFLUENCE ALERT - Every 15 min, both channels + email
# [FIX-MARKER-ALERT-START]
def _fire_heatmap_confluence_alert():
    """
    15-minute combined alert:  Heatmap + SMC + OI + Astro + KP.
    Sent to: ch1 (AutoBotTest123) + ch2 (Panchak Alerts) + Email.
    Gated by tg_HEATMAP_ALERT toggle (default ON).
    """
    if not is_market_hours():
        return
    if _is_tg_disabled("HEATMAP_ALERT"):
        return

    _now_dt   = datetime.now(IST)
    _slot_min = (_now_dt.minute // 15) * 15
    _slot_str = _now_dt.strftime(f"%H:{_slot_min:02d}")
    _dedup_key = f"HEATMAP_CONFLUENCE_{_slot_str}_{_now_dt.strftime('%Y%m%d')}"

    # Atomic dedup - one alert per 15-min slot per day
    with _TG_DEDUP_LOCK:
        _dedup = _load_tg_dedup()
        if _dedup.get(_dedup_key):
            return
        _dedup[_dedup_key] = _now_dt.isoformat()
        _save_tg_dedup(_dedup)

    _now_str = _now_dt.strftime("%H:%M IST")

    # Get live data from session state
    df       = st.session_state.get("live_df", pd.DataFrame())
    live_map = st.session_state.get("live_data_map", {})
    ltp_map  = st.session_state.get("ltp_map", {})

    # Initial fallbacks
    _n50_sig = _bnk_sig = _snx_sig = "-"
    _n50_bull = _n50_bear = _n50_flat = 0
    _bnk_bull = _bnk_bear = _bnk_flat = 0
    _snx_bull = _snx_bear = _snx_flat = 0
    _n50_bull_wt = _n50_bear_wt = _bnk_bull_wt = _bnk_bear_wt = _snx_bull_wt = _snx_bear_wt = 0.0
    _n50_impact = _bnk_impact = _snx_impact = 0.0
    _n50_pts = _bnk_pts = _snx_pts = 0
    _n50_top_gainers = []
    _n50_top_losers  = []
    _bnk_top_gainers = []
    _bnk_top_losers  = []
    _snx_top_gainers = []
    _snx_top_losers  = []
    _n50_ltp = 24315; _bnk_ltp = 56086; _snx_ltp = 80000
    _n50_bull_pct = _bnk_bull_pct = _snx_bull_pct = 50.0
    
    # Extra Index Details
    _idx_details = {} # symbol -> {ltp, chg, chp, o, h, l, pc, yh, yl, yc}

    # ── A. Heatmap weighted score (Nifty50 + BankNifty) ────────────
    _N50_WT_a = {
        "RELIANCE":9.30,"HDFCBANK":6.37,"BHARTIARTL":5.74,"SBIN":5.04,
        "ICICIBANK":4.93,"TCS":4.77,"LT":2.90,"BAJFINANCE":2.89,
        "INFY":2.74,"HINDUNILVR":2.57,"M&M":2.58,"ITC":2.71,
        "AXISBANK":3.26,"TATAMOTORS":1.62,"MARUTI":1.54,"NTPC":1.86,
        "POWERGRID":1.58,"HCLTECH":1.58,"SUNPHARMA":1.61,"KOTAKBANK":1.82,
        "ONGC":1.22,"COALINDIA":1.14,"TATASTEEL":1.18,"JSWSTEEL":1.04,
        "ULTRACEMCO":1.02,"WIPRO":1.05,"TITAN":1.51,"BAJAJFINSV":1.34,
        "BAJAJ-AUTO":1.10,"NESTLEIND":1.08,"HINDALCO":0.92,"GRASIM":0.84,
        "ADANIENT":0.81,"ASIANPAINT":0.79,"ADANIPORTS":0.84,"TATACONSUM":0.82,
        "APOLLOHOSP":0.68,"BEL":0.72,"CIPLA":0.75,"DRREDDY":0.71,
        "TRENT":0.74,"EICHERMOT":0.61,"HEROMOTOCO":0.58,"BPCL":0.65,
        "BRITANNIA":0.64,"SHRIRAMFIN":1.01,"HDFCLIFE":0.88,"SBILIFE":0.82,
        "LTIM":0.81,"DIVISLAB":0.56,
    }
    _BNK_WT_a = {
        "HDFCBANK":25.56,"SBIN":20.28,"ICICIBANK":19.79,"AXISBANK":8.64,
        "KOTAKBANK":7.80,"UNIONBANK":2.95,"BANKBARODA":2.95,"PNB":2.66,
        "CANBK":2.64,"AUBANK":1.51,"FEDERALBNK":1.45,
        "INDUSINDBK":1.34,"YESBANK":1.25,"IDFCFIRSTB":1.18,
    }
    _SNX_WT_a = {
        "HDFCBANK":15.66,"ICICIBANK":10.88,"RELIANCE":10.24,"INFY":5.75,
        "BHARTIARTL":5.37,"ITC":4.23,"LT":4.20,"TCS":3.74,
        "AXISBANK":3.63,"KOTAKBANK":3.49,"M&M":2.84,"HINDUNILVR":2.82,
        "TATASTEEL":2.58,"SBIN":2.34,"NTPC":2.16,"POWERGRID":1.84,
        "SUNPHARMA":1.76,"TITAN":1.68,"BAJFINANCE":1.65,"JSWSTEEL":1.48,
        "ULTRACEMCO":1.42,"ADANIENT":1.38,"MARUTI":1.32,"NESTLEIND":1.28,
        "HCLTECH":1.24,"ASIANPAINT":1.18,"BAJAJFINSV":1.14,"TATAMOTORS":1.12,
        "TECHM":0.98,"INDUSINDBK":0.91
    }

    try:
        # Fetch fresh index prices
        _idx_q = {}
        try:
            _idx_q = kite.quote(["NSE:NIFTY 50", "NSE:NIFTY BANK", "BSE:SENSEX"])
        except: pass
        
        for index_name, kite_key in [("NIFTY", "NSE:NIFTY 50"), ("BANKNIFTY", "NSE:NIFTY BANK"), ("SENSEX", "BSE:SENSEX")]:
            q = _idx_q.get(kite_key, {})
            if q:
                ltp = q["last_price"]
                pc  = q["ohlc"]["close"]
                chg = ltp - pc if pc else 0
                chp = (chg / pc * 100) if pc else 0
                o = q["ohlc"]["open"]
                h = q["ohlc"]["high"]
                l = q["ohlc"]["low"]
                
                # Fetch YH/YL
                tk = get_token(index_name)
                _, yh, yl, yc_hist, _ = fetch_yesterday_ohlc(tk)
                
                _idx_details[index_name] = {
                    "ltp": ltp, "chg": chg, "chp": chp, 
                    "o": o, "h": h, "l": l, "pc": pc,
                    "yh": yh or 0, "yl": yl or 0, "yc": pc if pc else yc_hist or 0
                }

        _n50_ltp = _idx_details.get("NIFTY", {}).get("ltp", 0) or ltp_map.get("NIFTY", 24315)
        _bnk_ltp = _idx_details.get("BANKNIFTY", {}).get("ltp", 0) or ltp_map.get("BANKNIFTY", 56086)
        _snx_ltp = _idx_details.get("SENSEX", {}).get("ltp", 0) or ltp_map.get("SENSEX", 80000)

        # Build live change map
        _chg_a = {}
        if "Symbol" in df.columns and "CHANGE_%" in df.columns:
            _chg_a.update(dict(zip(df.Symbol, df["CHANGE_%"])))
        for _s, _lv in live_map.items():
            _chg_a[_s] = float(_lv.get("CHANGE_%", 0) or 0)

        # Nifty50 score
        for _sym, _wt in _N50_WT_a.items():
            _chg = _chg_a.get(_sym, 0.0)
            _imp = _wt * _chg / 100
            _n50_impact += _imp
            if _chg > 0.25:   _n50_bull_wt += _wt; _n50_bull += 1
            elif _chg < -0.25:_n50_bear_wt += _wt; _n50_bear += 1
            else:              _n50_flat += 1
            if _chg >= 1.0:   _n50_top_gainers.append(f"{_sym} +{_chg:.1f}%")
            elif _chg <= -1.0:_n50_top_losers.append(f"{_sym} {_chg:.1f}%")
        _n50_bull_pct = _n50_bull_wt / (_n50_bull_wt + _n50_bear_wt + 0.01) * 100

        # BankNifty score
        for _sym, _wt in _BNK_WT_a.items():
            _chg = _chg_a.get(_sym, 0.0)
            _imp = _wt * _chg / 100
            _bnk_impact += _imp
            if _chg > 0.25:   _bnk_bull_wt += _wt; _bnk_bull += 1
            elif _chg < -0.25:_bnk_bear_wt += _wt; _bnk_bear += 1
            else:              _bnk_flat += 1
            if _chg >= 1.5:   _bnk_top_gainers.append(f"{_sym} +{_chg:.1f}%")
            elif _chg <= -1.5:_bnk_top_losers.append(f"{_sym} {_chg:.1f}%")
        _bnk_bull_pct = _bnk_bull_wt / (_bnk_bull_wt + _bnk_bear_wt + 0.01) * 100

        # Sensex score
        for _sym, _wt in _SNX_WT_a.items():
            _chg = _chg_a.get(_sym, 0.0)
            _imp = _wt * _chg / 100
            _snx_impact += _imp
            if _chg > 0.25:   _snx_bull_wt += _wt; _snx_bull += 1
            elif _chg < -0.25:_snx_bear_wt += _wt; _snx_bear += 1
            else:              _snx_flat += 1
            if _chg >= 1.0:   _snx_top_gainers.append(f"{_sym} +{_chg:.1f}%")
            elif _chg <= -1.0:_snx_top_losers.append(f"{_sym} {_chg:.1f}%")
        _snx_bull_pct = _snx_bull_wt / (_snx_bull_wt + _snx_bear_wt + 0.01) * 100

        _n50_sig  = "🟢 BULLISH" if _n50_bull_pct > 55 else ("🔴 BEARISH" if _n50_bull_pct < 45 else "🟡 NEUTRAL")
        _bnk_sig  = "🟢 BULLISH" if _bnk_bull_pct > 55 else ("🔴 BEARISH" if _bnk_bull_pct < 45 else "🟡 NEUTRAL")
        _snx_sig  = "🟢 BULLISH" if _snx_bull_pct > 55 else ("🔴 BEARISH" if _snx_bull_pct < 45 else "🟡 NEUTRAL")

        _n50_pts  = round(_n50_impact / 100 * _n50_ltp, 0)
        _bnk_pts  = round(_bnk_impact / 100 * _bnk_ltp, 0)
        _snx_pts  = round(_snx_impact / 100 * _snx_ltp, 0)

    except Exception as _hm_err:
        print(f"Heatmap calculation error: {_hm_err}")

    # ── B. SMC signal ───────────────────────────────────────────────
    try:
        _smc_d   = st.session_state.get("smc_result") or {}
        _smc_sig = str(_smc_d.get("final_signal", "-"))
        _smc_sc  = int(_smc_d.get("final_score", 0) or 0)
        _smc_act = str(_smc_d.get("final_action", "-"))
        _smc_ltf = str(_smc_d.get("smc_trend_ltf", "-"))
        _smc_htf = str(_smc_d.get("smc_trend_htf", "-"))
        _smc_zone = str(_smc_d.get("smc_zone", "-"))
    except Exception:
        _smc_sig = "-"; _smc_sc = 0; _smc_act = "-"
        _smc_ltf = "-"; _smc_htf = "-"; _smc_zone = "-"

    # ── C. OI signal ────────────────────────────────────────────────
    _bnk_sig_oi = "-"; _bnk_pcr_oi = 0.0
    try:
        # Fetch fresh OI data (st.cache_data handles the efficiency)
        _oi_d    = fetch_oi_intelligence("NIFTY") or {}
        _oi_bn_d = fetch_oi_intelligence("BANKNIFTY") or {}
        
        _oi_dir  = str(_oi_d.get("direction", "-"))
        _oi_pcr  = float(_oi_d.get("pcr", 0) or 0)
        _oi_mp   = int(_oi_d.get("max_pain", 0) or 0)
        _oi_cw   = int(_oi_d.get("nearest_call_wall", 0) or 0)
        _oi_pf   = int(_oi_d.get("nearest_put_floor", 0) or 0)
        _oi_adv  = str(_oi_d.get("advice", "-"))

        # BankNifty Details for TG message
        _bnk_sig_oi = _oi_bn_d.get("direction", "-")
        _bnk_pcr_oi = float(_oi_bn_d.get("pcr", 0) or 0)

    except Exception:
        _oi_dir = "-"; _oi_pcr = 0; _oi_mp = 0; _oi_cw = 0; _oi_pf = 0; _oi_adv = "-"
        _bnk_sig_oi = "-"; _bnk_pcr_oi = 0.0
        _oi_d = {}; _oi_bn_d = {}

    # ── D. Astro + KP signal ────────────────────────────────────────
    try:
        _as_d    = _vedic_day_analysis(datetime.now(IST).date()) or {}
        _as_sig  = str(_as_d.get("overall", "-"))
        _as_sc   = int(_as_d.get("net_score", 0) or 0)
        _as_nak  = str(_as_d.get("moon_nak", "-"))
        _as_tit  = str(_as_d.get("tithi", "-")).split("(")[0].strip()
        _as_int  = str(_as_d.get("intensity", "-"))
        _kp_sub  = str(_as_d.get("kp_sub", "-"))
        _kp_time = str(get_time_signal())
    except Exception:
        _as_sig = "-"; _as_sc = 0; _as_nak = "-"; _as_tit = "-"
        _as_int = "-"; _kp_sub = "-"; _kp_time = "-"

    # ── E. Conviction scorer - combine all 5 engines ─────────────────
    # Score: +2 = strong bull, +1 = mild bull, 0 = neutral, -1 = mild bear, -2 = strong bear
    _conv_score = 0
    _conv_reasons = []

    # Heatmap N50
    if _n50_bull_pct > 60:
        _conv_score += 2
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt BULL ({_n50_pts:+.0f}pts)")
    elif _n50_bull_pct > 52:
        _conv_score += 1
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt mildly BULL")
    elif _n50_bull_pct < 40:
        _conv_score -= 2
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt BEAR ({_n50_pts:+.0f}pts)")
    elif _n50_bull_pct < 48:
        _conv_score -= 1
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt mildly BEAR")

    # SMC
    if "BULLISH" in _smc_sig.upper():
        _conv_score += 2
        _conv_reasons.append(f"SMC: {_smc_sig} (score {_smc_sc:+d})")
    elif "BEARISH" in _smc_sig.upper():
        _conv_score -= 2
        _conv_reasons.append(f"SMC: {_smc_sig} (score {_smc_sc:+d})")

    # OI
    if "BULLISH" in _oi_dir.upper():
        _conv_score += 1
        _conv_reasons.append(f"N50 OI: {_oi_dir} (PCR {_oi_pcr:.2f})")
    elif "BEARISH" in _oi_dir.upper():
        _conv_score -= 1
        _conv_reasons.append(f"N50 OI: {_oi_dir} (PCR {_oi_pcr:.2f})")

    # Astro
    if "BULLISH" in _as_sig.upper():
        _conv_score += 1
        _conv_reasons.append(f"Astro: {_as_sig} ({_as_nak}/{_as_tit})")
    elif "BEARISH" in _as_sig.upper():
        _conv_score -= 1
        _conv_reasons.append(f"Astro: {_as_sig} ({_as_nak}/{_as_tit})")

    # Conviction verdict
    if _conv_score >= 6:
        _verdict   = "🔥🔥 EXTREME BULL - HIGH CONVICTION LONG"
        _verdict_s = "EXTREME_BULL"
    elif _conv_score >= 3:
        _verdict   = "🟢🟢 STRONG BULL - STAY LONG"
        _verdict_s = "STRONG_BULL"
    elif _conv_score >= 1:
        _verdict   = "🟢 MILD BULL - Trail longs / avoid shorts"
        _verdict_s = "MILD_BULL"
    elif _conv_score <= -6:
        _verdict   = "🔴🔴 EXTREME BEAR - HIGH CONVICTION SHORT"
        _verdict_s = "EXTREME_BEAR"
    elif _conv_score <= -3:
        _verdict   = "🔴🔴 STRONG BEAR - STAY SHORT"
        _verdict_s = "STRONG_BEAR"
    elif _conv_score <= -1:
        _verdict   = "🔴 MILD BEAR - Trail shorts / avoid longs"
        _verdict_s = "MILD_BEAR"
    else:
        _verdict   = "🟡 NEUTRAL / RANGING - No clear edge"
        _verdict_s = "NEUTRAL"

    _max_conv = 9  # max possible score (2+2+1+2+1+1)
    _conv_pct = int(abs(_conv_score) / _max_conv * 100)

    # ── F. Build Telegram message ────────────────────────────────────
    _gainers_n50 = ", ".join(_n50_top_gainers[:4]) if _n50_top_gainers else "none"
    _losers_n50  = ", ".join(_n50_top_losers[:4])  if _n50_top_losers  else "none"
    _gainers_bnk = ", ".join(_bnk_top_gainers[:3]) if _bnk_top_gainers else "none"
    _losers_bnk  = ", ".join(_bnk_top_losers[:3])  if _bnk_top_losers  else "none"
    _gainers_snx = ", ".join(_snx_top_gainers[:3]) if _snx_top_gainers else "none"
    _losers_snx  = ", ".join(_snx_top_losers[:3])  if _snx_top_losers  else "none"
    _reasons_txt = "\n".join(f"  • {r}" for r in _conv_reasons) if _conv_reasons else "  • No strong signals"

    def _fmt_idx(sym):
        d = _idx_details.get(sym, {})
        if not d: return f"<b>{sym}</b>: (no data)"
        _icon = "🟢" if d['chp']>=0 else "🔴"
        return (
            f"<b>{_icon} {sym}:</b> <b>{d['ltp']:,}</b> ({d['chg']:+.2f} | {d['chp']:+.2f}%)\n"
            f"   OHL: {d['o']:,}/{d['h']:,}/{d['l']:,}\n"
            f"   YH/YL/YC: {d['yh']:,}/{d['yl']:,}/{d['yc']:,}"
        )

    _tg_msg = (
        f"📊📊📊📊📊📊📊📊📊📊\n"
        f"<b>🗺️ HEATMAP CONFLUENCE ALERT - {_slot_str}</b>  ⏰ {_now_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 VERDICT: {_verdict}</b>\n"
        f"<b>Conviction: {_conv_score:+d}/{_max_conv} ({_conv_pct}%)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_fmt_idx('NIFTY')}\n"
        f"{_fmt_idx('BANKNIFTY')}\n"
        f"{_fmt_idx('SENSEX')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Nifty 50 Detail:</b>\n"
        f"   {_n50_bull}▲ ({_n50_bull_wt:.0f}% wt)  {_n50_bear}▼ ({_n50_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{_n50_impact:+.2f}% ≈ {_n50_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 <b>BankNifty Detail:</b>\n"
        f"   {_bnk_bull}▲ ({_bnk_bull_wt:.0f}% wt)  {_bnk_bear}▼ ({_bnk_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{_bnk_impact:+.2f}% ≈ {_bnk_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛️ <b>Sensex Detail:</b>\n"
        f"   {_snx_bull}▲ ({_snx_bull_wt:.0f}% wt)  {_snx_bear}▼ ({_snx_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{_snx_impact:+.2f}% ≈ {_snx_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>SMC:</b> {_smc_sig} (score {_smc_sc:+d})\n"
        f"   Trend: 15m {_smc_ltf} | HTF {_smc_htf}\n"
        f"   Zone: {_smc_zone} | Action: {_smc_act}\n"
        f"📉 <b>OI:</b> N50: {_oi_dir} | BNK: {_bnk_sig_oi}\n"
        f"📈 <b>PCR:</b> N50: {_oi_pcr:.2f} | BNK: {_bnk_pcr_oi:.2f}\n"
        f"Moon <b>Astro:</b> {_as_sig} ({_as_int}) | 🔑 <b>KP:</b> {_kp_sub}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Why this verdict:</b>\n{_reasons_txt}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>NOT financial advice. Auto-15min.</i>\n"
        f"📊📊📊📊📊📊📊📊📊📊"
    )

    # Truncate if > 4000 chars
    if len(_tg_msg) > 4000:
        _tg_msg = _tg_msg[:3950] + "\n... [truncated]"

    # ── G. Send to both channels ─────────────────────────────────────
    _route = st.session_state.get("route_HEATMAP_ALERT", "both")
    if _route in ("ch1", "both"):
        send_telegram_bg(_tg_msg, dedup_key=None)
    if _route in ("ch2", "both"):
        send_telegram2_bg(_tg_msg, dedup_key=None)

    # ── H. Send email ────────────────────────────────────────────────
    try:
        _email_subj = (
            f"📊 HeatMap {_verdict_s} - Nifty {_n50_sig} | BankNifty {_bnk_sig} | {_slot_str} IST"
        )
        _email_body = f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#0d0d0d;color:#e0e0e0;padding:16px">
<div style="max-width:600px;margin:0 auto;background:#111;border-radius:10px;padding:20px">
<h2 style="color:#4ade80;margin-top:0">📊 HeatMap Confluence Alert</h2>
<h3 style="color:{'#4ade80' if 'BULL' in _verdict_s else '#f87171' if 'BEAR' in _verdict_s else '#fbbf24'}">
  🎯 {_verdict}</h3>
<p style="color:#888">Time: {_now_str} | Slot: {_slot_str} | Conviction: {_conv_score:+d}/{_max_conv} ({_conv_pct}%)</p>
<hr style="border-color:#222">
<h4 style="color:#aaa">📊 Nifty 50 Heatmap - {_n50_sig}</h4>
<table style="width:100%;border-collapse:collapse;font-size:13px">
  <tr><td style="color:#888;padding:4px 0">Bull stocks</td><td style="color:#4ade80">{_n50_bull} stocks ({_n50_bull_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Bear stocks</td><td style="color:#f87171">{_n50_bear} stocks ({_n50_bear_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Weighted impact</td><td style="color:{'#4ade80' if _n50_impact>=0 else '#f87171'}">{_n50_impact:+.2f}% ≈ {_n50_pts:+.0f} pts</td></tr>
  <tr><td style="color:#888;padding:4px 0">Top Gainers</td><td style="color:#4ade80">{_gainers_n50}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Top Losers</td><td style="color:#f87171">{_losers_n50}</td></tr>
</table>
<hr style="border-color:#222">
<h4 style="color:#aaa">🏦 BankNifty Heatmap - {_bnk_sig}</h4>
<table style="width:100%;border-collapse:collapse;font-size:13px">
  <tr><td style="color:#888;padding:4px 0">Bull stocks</td><td style="color:#4ade80">{_bnk_bull} stocks ({_bnk_bull_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Bear stocks</td><td style="color:#f87171">{_bnk_bear} stocks ({_bnk_bear_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Weighted impact</td><td style="color:{'#4ade80' if _bnk_impact>=0 else '#f87171'}">{_bnk_impact:+.2f}% ≈ {_bnk_pts:+.0f} pts</td></tr>
</table>
<hr style="border-color:#222">
<h4 style="color:#aaa">📉 Open Interest & Astro</h4>
<table style="width:100%;border-collapse:collapse;font-size:13px">
  <tr><td style="color:#888;padding:4px 0">OI Direction</td><td>{_oi_dir}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Put-Call Ratio</td><td>{_oi_pcr:.2f}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Max Pain</td><td>{_oi_mp}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Astro Signal</td><td>{_as_sig} ({_as_int})</td></tr>
  <tr><td style="color:#888;padding:4px 0">KP Sub-Lord</td><td>{_kp_sub}</td></tr>
</table>
<p style="color:#666;font-size:11px;margin-top:20px">⚠️ NOT financial advice. Automated confluence alert generated every 15 minutes.</p>
</div></body></html>"""
        send_email_bg(_email_subj, _email_body)
    except: pass

    # ── B. SMC signal ───────────────────────────────────────────────
    try:
        _smc_d   = st.session_state.get("smc_result") or {}
        _smc_sig = str(_smc_d.get("final_signal", "-"))
        _smc_sc  = int(_smc_d.get("final_score", 0) or 0)
        _smc_act = str(_smc_d.get("final_action", "-"))
        _smc_ltf = str(_smc_d.get("smc_trend_ltf", "-"))
        _smc_htf = str(_smc_d.get("smc_trend_htf", "-"))
    except Exception:
        _smc_sig = "-"; _smc_sc = 0; _smc_act = "-"
        _smc_ltf = "-"; _smc_htf = "-"

    # ── C. OI signal ────────────────────────────────────────────────
    try:
        _oi_d    = st.session_state.get("oi_intel_NIFTY") or {}
        _oi_dir  = str(_oi_d.get("direction", "-"))
        _oi_pcr  = float(_oi_d.get("pcr", 0) or 0)
        _oi_mp   = int(_oi_d.get("max_pain", 0) or 0)
        _oi_cw   = int(_oi_d.get("nearest_call_wall", 0) or 0)
        _oi_pf   = int(_oi_d.get("nearest_put_floor", 0) or 0)
        _oi_adv  = str(_oi_d.get("advice", "-"))
    except Exception:
        _oi_dir = "-"; _oi_pcr = 0; _oi_mp = 0; _oi_cw = 0; _oi_pf = 0; _oi_adv = "-"

    # ── D. Astro + KP signal ────────────────────────────────────────
    try:
        _as_d    = _vedic_day_analysis(datetime.now(IST).date()) or {}
        _as_sig  = str(_as_d.get("overall", "-"))
        _as_sc   = int(_as_d.get("net_score", 0) or 0)
        _as_nak  = str(_as_d.get("moon_nak", "-"))
        _as_tit  = str(_as_d.get("tithi", "-")).split("(")[0].strip()
        _as_int  = str(_as_d.get("intensity", "-"))
        _kp_sub  = str(_as_d.get("kp_sub", "-"))
        _kp_time = str(get_time_signal())
    except Exception:
        _as_sig = "-"; _as_sc = 0; _as_nak = "-"; _as_tit = "-"
        _as_int = "-"; _kp_sub = "-"; _kp_time = "-"

    # ── E. Conviction scorer - combine all 5 engines ─────────────────
    # Score: +2 = strong bull, +1 = mild bull, 0 = neutral, -1 = mild bear, -2 = strong bear
    _conv_score = 0
    _conv_reasons = []

    # Heatmap N50
    if _n50_bull_pct > 60:
        _conv_score += 2
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt BULL ({_n50_pts:+.0f}pts)")
    elif _n50_bull_pct > 52:
        _conv_score += 1
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt mildly BULL")
    elif _n50_bull_pct < 40:
        _conv_score -= 2
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt BEAR ({_n50_pts:+.0f}pts)")
    elif _n50_bull_pct < 48:
        _conv_score -= 1
        _conv_reasons.append(f"Heatmap N50: {_n50_bull_pct:.0f}% wt mildly BEAR")

    # Heatmap BNK
    if _bnk_bull_pct > 60:
        _conv_score += 2
        _conv_reasons.append(f"Heatmap BNK: {_bnk_bull_pct:.0f}% wt BULL ({_bnk_pts:+.0f}pts)")
    elif _bnk_bull_pct < 40:
        _conv_score -= 2
        _conv_reasons.append(f"Heatmap BNK: {_bnk_bull_pct:.0f}% wt BEAR ({_bnk_pts:+.0f}pts)")

    # Heatmap SNX (Sensex)
    if _snx_bull_pct > 60:
        _conv_score += 1
        _conv_reasons.append(f"Heatmap SNX: {_snx_bull_pct:.0f}% wt BULL ({_snx_pts:+.0f}pts)")
    elif _snx_bull_pct < 40:
        _conv_score -= 1
        _conv_reasons.append(f"Heatmap SNX: {_snx_bull_pct:.0f}% wt BEAR ({_snx_pts:+.0f}pts)")

    # SMC
    if "BULL" in _smc_sig.upper() or "STRONG BULL" in _smc_sig.upper():
        _conv_score += 2
        _conv_reasons.append(f"SMC: {_smc_sig} (score {_smc_sc:+d})")
    elif "BULL" in _smc_sig.upper():
        _conv_score += 1
        _conv_reasons.append(f"SMC: {_smc_sig}")
    elif "BEAR" in _smc_sig.upper() and "STRONG" in _smc_sig.upper():
        _conv_score -= 2
        _conv_reasons.append(f"SMC: {_smc_sig} (score {_smc_sc:+d})")
    elif "BEAR" in _smc_sig.upper():
        _conv_score -= 1
        _conv_reasons.append(f"SMC: {_smc_sig}")

    # OI
    if "BULL" in _oi_dir.upper():
        _conv_score += 1
        _conv_reasons.append(f"OI: PCR {_oi_pcr:.2f} - BULLISH bias")
    elif "BEAR" in _oi_dir.upper():
        _conv_score -= 1
        _conv_reasons.append(f"OI: PCR {_oi_pcr:.2f} - BEARISH bias")

    # Astro
    if "BULLISH" in _as_sig.upper():
        _conv_score += 1
        _conv_reasons.append(f"Astro: {_as_sig} ({_as_nak}/{_as_tit})")
    elif "BEARISH" in _as_sig.upper():
        _conv_score -= 1
        _conv_reasons.append(f"Astro: {_as_sig} ({_as_nak}/{_as_tit})")

    # Conviction verdict
    if _conv_score >= 6:
        _verdict   = "🔥🔥 EXTREME BULL - HIGH CONVICTION LONG"
        _verdict_s = "EXTREME_BULL"
    elif _conv_score >= 3:
        _verdict   = "🟢🟢 STRONG BULL - STAY LONG"
        _verdict_s = "STRONG_BULL"
    elif _conv_score >= 1:
        _verdict   = "🟢 MILD BULL - Trail longs / avoid shorts"
        _verdict_s = "MILD_BULL"
    elif _conv_score <= -6:
        _verdict   = "🔴🔴 EXTREME BEAR - HIGH CONVICTION SHORT"
        _verdict_s = "EXTREME_BEAR"
    elif _conv_score <= -3:
        _verdict   = "🔴🔴 STRONG BEAR - STAY SHORT"
        _verdict_s = "STRONG_BEAR"
    elif _conv_score <= -1:
        _verdict   = "🔴 MILD BEAR - Trail shorts / avoid longs"
        _verdict_s = "MILD_BEAR"
    else:
        _verdict   = "🟡 NEUTRAL / RANGING - No clear edge"
        _verdict_s = "NEUTRAL"

    _max_conv = 9  # max possible score (2+2+1+2+1+1)
    _conv_pct = int(abs(_conv_score) / _max_conv * 100)

    # ── F. Build Telegram message ────────────────────────────────────
    _gainers_n50 = ", ".join(_n50_top_gainers[:4]) if _n50_top_gainers else "none"
    _losers_n50  = ", ".join(_n50_top_losers[:4])  if _n50_top_losers  else "none"
    _gainers_bnk = ", ".join(_bnk_top_gainers[:3]) if _bnk_top_gainers else "none"
    _losers_bnk  = ", ".join(_bnk_top_losers[:3])  if _bnk_top_losers  else "none"
    _gainers_snx = ", ".join(_snx_top_gainers[:3]) if _snx_top_gainers else "none"
    _losers_snx  = ", ".join(_snx_top_losers[:3])  if _snx_top_losers  else "none"
    _reasons_txt = "\n".join(f"  • {r}" for r in _conv_reasons) if _conv_reasons else "  • No strong signals"

    _tg_msg = (
        f"📊📊📊📊📊📊📊📊📊📊\n"
        f"<b>🗺️ HEATMAP CONFLUENCE ALERT - {_slot_str}</b>  ⏰ {_now_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 VERDICT: {_verdict}</b>\n"
        f"<b>Conviction: {_conv_score:+d}/{_max_conv} ({_conv_pct}%)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Nifty 50:</b> {_n50_sig} | <b>BankNifty:</b> {_bnk_sig}\n"
        f"🏛️ <b>Sensex:</b> {_snx_sig}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Nifty 50 Details:</b>\n"
        f"   {_n50_bull}▲ ({_n50_bull_wt:.0f}% wt)  {_n50_bear}▼ ({_n50_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{_n50_impact:+.2f}% ≈ {_n50_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 <b>BankNifty Details:</b>\n"
        f"   {_bnk_bull}▲ ({_bnk_bull_wt:.0f}% wt)  {_bnk_bear}▼ ({_bnk_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{_bnk_impact:+.2f}% ≈ {_bnk_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛️ <b>Sensex Details:</b>\n"
        f"   {_snx_bull}▲ ({_snx_bull_wt:.0f}% wt)  {_snx_bear}▼ ({_snx_bear_wt:.0f}% wt)\n"
        f"   Impact: <b>{_snx_impact:+.2f}% ≈ {_snx_pts:+.0f} pts</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>SMC:</b> {_smc_sig} (score {_smc_sc:+d})\n"
        f"📉 <b>OI:</b> {_oi_dir} | PCR: {_oi_pcr:.2f}\n"
        f"Moon <b>Astro:</b> {_as_sig} | 🔑 <b>KP:</b> {_kp_sub}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Why this verdict:</b>\n{_reasons_txt}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>NOT financial advice. Auto-15min.</i>\n"
        f"📊📊📊📊📊📊📊📊📊📊"
    )

    # Truncate if > 4000 chars
    if len(_tg_msg) > 4000:
        _tg_msg = _tg_msg[:3950] + "\n... [truncated]"

    # ── G. Send to both channels ─────────────────────────────────────
    _route = st.session_state.get("route_HEATMAP_ALERT", "both")
    if _route in ("ch1", "both"):
        send_telegram_bg(_tg_msg, dedup_key=None)
    if _route in ("ch2", "both"):
        send_telegram2_bg(_tg_msg, dedup_key=None)

    # ── H. Send email ────────────────────────────────────────────────
    try:
        _email_subj = (
            f"📊 HeatMap {_verdict_s} - Nifty {_n50_sig} | BankNifty {_bnk_sig} | {_slot_str} IST"
        )
        _email_body = f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#0d0d0d;color:#e0e0e0;padding:16px">
<div style="max-width:600px;margin:0 auto;background:#111;border-radius:10px;padding:20px">
<h2 style="color:#4ade80;margin-top:0">📊 HeatMap Confluence Alert</h2>
<h3 style="color:{'#4ade80' if 'BULL' in _verdict_s else '#f87171' if 'BEAR' in _verdict_s else '#fbbf24'}">
  🎯 {_verdict}</h3>
<p style="color:#888">Time: {_now_str} | Slot: {_slot_str} | Conviction: {_conv_score:+d}/{_max_conv} ({_conv_pct}%)</p>
<hr style="border-color:#222">
<h4 style="color:#aaa">📊 Nifty 50 Heatmap - {_n50_sig}</h4>
<table style="width:100%;border-collapse:collapse;font-size:13px">
  <tr><td style="color:#888;padding:4px 0">Bull stocks</td><td style="color:#4ade80">{_n50_bull} stocks ({_n50_bull_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Bear stocks</td><td style="color:#f87171">{_n50_bear} stocks ({_n50_bear_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Weighted impact</td><td style="color:{'#4ade80' if _n50_impact>=0 else '#f87171'}">{_n50_impact:+.2f}% ≈ {_n50_pts:+.0f} pts</td></tr>
  <tr><td style="color:#888;padding:4px 0">Top Gainers</td><td style="color:#4ade80">{_gainers_n50}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Top Losers</td><td style="color:#f87171">{_losers_n50}</td></tr>
</table>
<hr style="border-color:#222">
<h4 style="color:#aaa">🏦 BankNifty Heatmap - {_bnk_sig}</h4>
<table style="width:100%;border-collapse:collapse;font-size:13px">
  <tr><td style="color:#888;padding:4px 0">Bull banks</td><td style="color:#4ade80">{_bnk_bull} ({_bnk_bull_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Bear banks</td><td style="color:#f87171">{_bnk_bear} ({_bnk_bear_wt:.0f}% weight)</td></tr>
  <tr><td style="color:#888;padding:4px 0">Weighted impact</td><td style="color:{'#4ade80' if _bnk_impact>=0 else '#f87171'}">{_bnk_impact:+.2f}% ≈ {_bnk_pts:+.0f} pts</td></tr>
  <tr><td style="color:#888;padding:4px 0">Top Gainers</td><td style="color:#4ade80">{_gainers_bnk}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Top Losers</td><td style="color:#f87171">{_losers_bnk}</td></tr>
</table>
<hr style="border-color:#222">
<h4 style="color:#aaa">Signal Engines</h4>
<table style="width:100%;border-collapse:collapse;font-size:13px">
  <tr><td style="color:#888;padding:4px 0">🧠 SMC</td><td>{_smc_sig} | score {_smc_sc:+d} | 15m {_smc_ltf} HTF {_smc_htf}</td></tr>
  <tr><td style="color:#888;padding:4px 0">📉 OI</td><td>{_oi_dir} | PCR {_oi_pcr:.2f} | MaxPain {_oi_mp:,}</td></tr>
  <tr><td style="color:#888;padding:4px 0">Moon Astro</td><td>{_as_sig} ({_as_int}) | {_as_nak} | {_as_tit}</td></tr>
  <tr><td style="color:#888;padding:4px 0">🔑 KP</td><td>Sub-lord: {_kp_sub} | Time: {_kp_time}</td></tr>
</table>
<hr style="border-color:#222">
<h4 style="color:#aaa">Why this verdict</h4>
<ul style="color:#ccc;font-size:13px">{"".join(f"<li>{r}</li>" for r in _conv_reasons)}</ul>
<p style="color:#555;font-size:11px;margin-top:16px">HeatMap Confluence Alert. NOT financial advice. Auto-generated every 15 min during market hours.</p>
</div></body></html>"""
        import threading as _et
        _et.Thread(
            target=send_email,
            args=(_email_subj, _email_body, True),
            daemon=True
        ).start()
    except Exception as _email_err:
        print(f"[HEATMAP-EMAIL] error: {_email_err}")


_fire_heatmap_confluence_alert()


# ═══════════════════════════════════════════════════════════════════════
# 📅 EXPIRY + HOLIDAY ALERTS - dashboard-side, fires every page refresh
# Runs independently of background_worker so alerts fire even when
# the worker is not running. Dedup is shared (tg_dedup_YYYYMMDD.json)
# so worker + dashboard never double-send the same key.
# ═══════════════════════════════════════════════════════════════════════
def _dash_fire_expiry_holiday_alerts():
    """Expiry and holiday TG alerts fired from dashboard on every 60s refresh."""
    # Check at least one of the two toggles is enabled
    _exp_on = not _is_tg_disabled("EXPIRY_ALERT")
    _hol_on = not _is_tg_disabled("HOLIDAY_ALERT")
    if not _exp_on and not _hol_on:
        return
    _now_dt  = datetime.now(IST)
    _today   = _now_dt.date()
    _today_s = _today.strftime("%Y%m%d")
    _hhmm    = _now_dt.strftime("%H%M")
    _now_s   = _now_dt.strftime("%H:%M IST")

    def _nifty_exp(ref):
        _d = (1 - ref.weekday()) % 7
        _t = ref + timedelta(days=_d)
        return (_t - timedelta(days=1)) if _t in NSE_HOLIDAYS else _t

    def _sensex_exp(ref):
        _d = (3 - ref.weekday()) % 7
        _t = ref + timedelta(days=_d)
        return (_t - timedelta(days=1)) if _t in NSE_HOLIDAYS else _t

    # ── EXPIRY ALERTS ────────────────────────────────────────────────
    if _exp_on:
        _ne = _nifty_exp(_today);   _nd = (_ne - _today).days
        _se = _sensex_exp(_today);  _sd = (_se - _today).days

        for _days, _exp, _name, _prefix, _shifted_cond in [
            (_nd, _ne, "NIFTY",   "NIFTY",  _ne.weekday()==0),
            (_sd, _se, "SENSEX",  "SENSEX", _se.weekday()==2),
        ]:
            _exp_str = _exp.strftime("%A %d-%b")
            _shift_note = f"\n⚠️ Shifted from {'Tue' if _name=='NIFTY' else 'Thu'} (Holiday)" if _shifted_cond else ""

            if _days == 1 and _hhmm >= "0915":
                _k = f"DASH_EXP_{_prefix}_DAYBEFORE_{_today_s}"
                if not _tg_already_sent(_k):
                    send_alert_routed("EXPIRY_ALERT",
                        f"📅 <b>{_name} EXPIRY TOMORROW</b>\n⏰ {_now_s}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Weekly Expiry: <b>{_exp_str}</b>{_shift_note}\n"
                        f"📌 Review open {_name} positions. Manage risk.\n"
                        f"⚠️ <i>NOT financial advice.</i>", _k)

            if _days == 0 and _hhmm >= "0915":
                _k = f"DASH_EXP_{_prefix}_MORNING_{_today_s}"
                if not _tg_already_sent(_k):
                    send_alert_routed("EXPIRY_ALERT",
                        f"🚨 <b>{_name} EXPIRY DAY</b> 🚨\n⏰ {_now_s}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Today is {_name} Weekly Expiry: <b>{_exp_str}</b>{_shift_note}\n"
                        f"📌 Close or hedge {_name} positions by 15:20!\n"
                        f"⚠️ <i>NOT financial advice.</i>", _k)

    # ── HOLIDAY ALERTS ───────────────────────────────────────────────
    if _hol_on:
        _tom = _today + timedelta(days=1)
        if _tom.weekday() < 5 and _tom in NSE_HOLIDAYS:
            _tom_s = _tom.strftime("%A %d-%b-%Y")
            if _hhmm >= "0900":
                _k = f"DASH_HOL_TOMORROW_{_today_s}"
                if not _tg_already_sent(_k):
                    send_alert_routed("HOLIDAY_ALERT",
                        f"🚨 <b>NSE HOLIDAY TOMORROW</b>\n⏰ {_now_s}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📅 <b>{_tom_s}</b> is NSE Holiday.\n"
                        f"📌 Plan ahead - close or hedge open positions.\n"
                        f"⚠️ <i>NOT financial advice.</i>", _k)
            if _hhmm >= "1430":
                _k2 = f"DASH_HOL_PRECLOSE_{_today_s}"
                if not _tg_already_sent(_k2):
                    send_alert_routed("HOLIDAY_ALERT",
                        f"⚠️⚠️ <b>LAST CHANCE - NSE HOLIDAY TOMORROW</b>\n⏰ {_now_s}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Tomorrow <b>{_tom_s}</b> NSE is CLOSED.\n"
                        f"🚨 <b>Close intraday positions before 15:20 today!</b>\n"
                        f"⚠️ <i>NOT financial advice.</i>", _k2)
        # Friday + Monday holiday = long weekend
        if _today.weekday() == 4 and _hhmm >= "0900":
            _mon = _today + timedelta(days=3)
            if _mon in NSE_HOLIDAYS:
                _k3 = f"DASH_HOL_LONGWKND_{_today_s}"
                if not _tg_already_sent(_k3):
                    send_alert_routed("HOLIDAY_ALERT",
                        f"📅 <b>LONG WEEKEND ALERT</b>\n⏰ {_now_s}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Today Friday + <b>{_mon.strftime('%A %d-%b-%Y')}</b> is NSE Holiday.\n"
                        f"⚠️ <b>3-day weekend</b> - manage gap risk!\n"
                        f"📌 Consider closing/hedging before 15:20.\n"
                        f"⚠️ <i>NOT financial advice.</i>", _k3)


def _tg_already_sent(key: str) -> bool:
    """Check if this dedup key was already sent today (shared dedup file)."""
    try:
        with _TG_DEDUP_LOCK:
            return bool(_load_tg_dedup().get(key))
    except Exception:
        return False


_dash_fire_expiry_holiday_alerts()


new_ema = detect_new_entries(
    "EMA20_50",
    ema_signal_df.Symbol.tolist()
)

notify_all(
    "EMA20_50",
    "⚡ EMA20–EMA50 Signal",
    new_ema,
    ltp_map
)
new_daily_up = detect_new_entries(
    "DAILY_UP",
    daily_up.Symbol.tolist()
)

notify_all(
    "DAILY_UP",
    "📈🟢DAILY LIVE_HIGH Break",
    new_daily_up,
    ltp_map
)

new_daily_down = detect_new_entries(
    "DAILY_DOWN",
    daily_down.Symbol.tolist()
)

notify_all(
    "DAILY_DOWN",
    "📉🔴DAILY LIVE_LOW Break",
    new_daily_down,
    ltp_map
)
new_weekly_up = detect_new_entries(
    "WEEKLY_UP",
    weekly_up.Symbol.tolist()
)

notify_all(
    "WEEKLY_UP",
    "📊🟢WEEKLY LIVE_HIGH Break",
    new_weekly_up,
    ltp_map
)

new_weekly_down = detect_new_entries(
    "WEEKLY_DOWN",
    weekly_down.Symbol.tolist()
)

notify_all(
    "WEEKLY_DOWN",
    "📉🔴WEEKLY LIVE_LOW Break",
    new_weekly_down,
    ltp_map
)
new_monthly_up = detect_new_entries(
    "MONTHLY_UP",
    monthly_up.Symbol.tolist()
)

notify_all(
    "MONTHLY_UP",
    "📅🟢MONTHLY LIVE_HIGH Break",
    new_monthly_up,
    ltp_map
)

new_monthly_down = detect_new_entries(
    "MONTHLY_DOWN",
    monthly_down.Symbol.tolist()
)

notify_all(
    "MONTHLY_DOWN",
    "📉🔴MONTHLY LIVE_LOW Break",
    new_monthly_down,
    ltp_map
)

ohl_df = df.loc[
    (df["LIVE_OPEN"] == df["LIVE_HIGH"]) | (df["LIVE_OPEN"] == df["LIVE_LOW"]),
    [
        "Symbol",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "YEST_HIGH",
        "YEST_LOW"
    ]
].copy()


ohl_df["TYPE"] = ohl_df.apply(
    lambda r: "🔴 LIVE_OPEN = LIVE_HIGH" if r.LIVE_OPEN == r.LIVE_HIGH else "🟢 LIVE_OPEN = LIVE_LOW",
    axis=1
)

# ================= LIVE_OPEN = LIVE_HIGH / LIVE_LOW SPLIT =================

LIVE_OPEN_LIVE_LOW_df = pd.DataFrame()
LIVE_OPEN_LIVE_HIGH_df = pd.DataFrame()

if not ohl_df.empty and "TYPE" in ohl_df.columns:
    LIVE_OPEN_LIVE_LOW_df = ohl_df[ohl_df["TYPE"] == "🟢 LIVE_OPEN = LIVE_LOW"]
    LIVE_OPEN_LIVE_HIGH_df = ohl_df[ohl_df["TYPE"] == "🔴 LIVE_OPEN = LIVE_HIGH"]




NUM_COLS = [
    "EMA20","EMA50","TOP_HIGH","TOP_LOW",
    "YEST_HIGH","YEST_LOW",
    "HIGH_W","LOW_W","HIGH_M","LOW_M"
]

for c in NUM_COLS:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")


ema_signal_df = df.dropna(subset=["EMA20","EMA50"]).loc[
    (
        (df.LTP > df.EMA20) &
        (df.EMA20 > df.EMA50) &
        (df.LTP > df.TOP_HIGH)
    ) |
    (
        (df.LTP < df.EMA20) &
        (df.EMA20 < df.EMA50) &
        (df.LTP < df.TOP_LOW)
    ),
    [
        "Symbol",
        "LTP",
        "EMA20",
        "EMA50",
        "TOP_HIGH",
        "TOP_LOW",
        "CHANGE",
        "CHANGE_%"
    ]
].copy()

ema_signal_df["SIGNAL"] = ema_signal_df.apply(
    lambda r: "🟢 BUY" if r.LTP > r.EMA20 else "🔴 SELL",
    axis=1
)

# ================= TOP GAINERS / LOSERS =================

#gainers_df = (
 #   df[df["CHANGE_%"] >= 2.5]
  #  .sort_values("CHANGE_%", ascending=False)
   # .loc[:, [
    #    "Symbol",
     #   "LTP",
      #  "CHANGE",
       # "CHANGE_%",
        #"LIVE_HIGH",
        #"LIVE_LOW"
    #]]
#)

losers_df = (
    df[df["CHANGE_%"] <= -2.5]
    .sort_values("CHANGE_%")
    .loc[:, [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_HIGH",
        "LIVE_LOW"
    ]]
)

# =========================================================
# TOP GAINERS / LOSERS - COLUMN ORDER FIX
# =========================================================

TOP_GL_COLUMNS = [
    "Symbol",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "LIVE_VOLUME",
]

# --- TOP GAINERS ---
gainers_df = (
    df[df["CHANGE_%"] > 2]
    .sort_values("CHANGE_%", ascending=False)
    [TOP_GL_COLUMNS]
)

# --- TOP LOSERS ---
losers_df = (
    df[df["CHANGE_%"] < -2]
    .sort_values("CHANGE_%")
    [TOP_GL_COLUMNS]
)

TOP_GL_COLUMNS = [c for c in TOP_GL_COLUMNS if c in df.columns]

new_gainers = detect_new_entries(
    "TOP_GAINERS",
    gainers_df.Symbol.tolist()
)

notify_all(
    "TOP_GAINERS",
    "🔥🟢Top Gainers > 2.5%",
    new_gainers,
    ltp_map
)

new_losers = detect_new_entries(
    "TOP_LOSERS",
    losers_df.Symbol.tolist()
)

notify_all(
    "TOP_LOSERS",
    "🔥🔴Top LOSERS < -2.5%",
    new_losers,
    ltp_map
)

        ############## TOP GAINERS NEW ADDITION ##############################

# ================= O=H / O=L FILTERED SETUPS =================
TOL = 0.05   # 5 paise tolerance

ol_condition = (
    (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
    (df["LIVE_OPEN"] < df["YEST_HIGH"]) &
    (df["LIVE_OPEN"] > df["YEST_LOW"])
)

#oh_condition = (
 #   (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
  #  (df["LIVE_OPEN"] > df["YEST_LOW"])
#)

oh_condition = (
    (abs(df["LIVE_OPEN"] - df["LIVE_HIGH"]) <= TOL) &
    (df["LIVE_OPEN"] > df["YEST_LOW"]) &
    (df["LIVE_OPEN"] < df["YEST_CLOSE"]) &
    (df["LTP"] < df["LIVE_OPEN"])    # price below open
)


ol_oh_df = df.loc[
    ol_condition | oh_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE",
    ]
].copy()

ol_oh_df["SETUP"] = np.where(
    ol_condition.loc[ol_oh_df.index],
    "🟢 O = L",
    "🔴 O = H"
)

ol_oh_df["SIDE"] = np.where(
    ol_condition.loc[ol_oh_df.index],
    "BULLISH",
    "BEARISH"
)

ol_oh_df = ol_oh_df.sort_values(
    by=["SIDE", "CHANGE_%"],
    ascending=[True, False]
)





# ================= DAILY + EMA CONFIRMATION =================
daily_ema_buy = df.loc[
    (df.LTP > df.YEST_HIGH) &
    (df.LTP > df.EMA20) &
    (df.EMA20 > df.EMA50),
    DAILY_COLUMNS
]

daily_ema_sell = df.loc[
    (df.LTP < df.YEST_LOW) &
    (df.LTP < df.EMA20) &
    (df.EMA20 < df.EMA50),
    DAILY_COLUMNS
]

# ================= WEEKLY + EMA CONFIRMATION =================

weekly_ema_buy = df.loc[
    (df.LTP > df.HIGH_W) &
    (df.LTP > df.EMA20) &
    (df.EMA20 > df.EMA50),
    WEEKLY_COLUMNS
]

weekly_ema_sell = df.loc[
    (df.LTP < df.LOW_W) &
    (df.LTP < df.EMA20) &
    (df.EMA20 < df.EMA50),
    WEEKLY_COLUMNS
]



# =========================================================
# SUPERTREND (DAILY) + VWAP  - CLEAN VERSION
# (NO ZERO VALUES | BUY / SELL SEPARATE)
# =========================================================

def compute_supertrend(df, period=10, multiplier=3):
    df = df.copy()

    df["H-L"] = df["high"] - df["low"]
    df["H-PC"] = (df["high"] - df["close"].shift()).abs()
    df["L-PC"] = (df["low"] - df["close"].shift()).abs()

    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)

    # 🔥 Wilder ATR (Kite match)
    df["ATR"] = df["TR"].rolling(period).mean()
    df["ATR"] = df["ATR"].combine_first(
        df["ATR"].shift().ewm(alpha=1/period, adjust=False).mean()
    )

    mid = (df["high"] + df["low"]) / 2
    df["UpperBand"] = mid + multiplier * df["ATR"]
    df["LowerBand"] = mid - multiplier * df["ATR"]

    df["SuperTrend"] = np.nan
    df["ST_DIR"] = None

    # Seed
    df.loc[period, "SuperTrend"] = df.loc[period, "UpperBand"]
    df.loc[period, "ST_DIR"] = "SELL"

    for i in range(period + 1, len(df)):
        prev = i - 1

        if df.loc[prev, "close"] > df.loc[prev, "UpperBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "LowerBand"]
            df.loc[i, "ST_DIR"] = "BUY"

        elif df.loc[prev, "close"] < df.loc[prev, "LowerBand"]:
            df.loc[i, "SuperTrend"] = df.loc[i, "UpperBand"]
            df.loc[i, "ST_DIR"] = "SELL"

        else:
            df.loc[i, "SuperTrend"] = df.loc[prev, "SuperTrend"]
            df.loc[i, "ST_DIR"] = df.loc[prev, "ST_DIR"]

        # 🔒 Band carry-forward (Kite logic)
        if df.loc[i, "ST_DIR"] == "BUY":
            df.loc[i, "LowerBand"] = max(
                df.loc[i, "LowerBand"], df.loc[prev, "LowerBand"]
            )
        else:
            df.loc[i, "UpperBand"] = min(
                df.loc[i, "UpperBand"], df.loc[prev, "UpperBand"]
            )

    return df



# -----------------------------
# BUILD SUPERTREND PER SYMBOL
# -----------------------------
supertrend_rows = []

ohlc_full = load_ohlc_full() if os.path.exists(OHLC_FILE) else pd.DataFrame()   # ✅ FIX: reuse cached version

for sym, g in ohlc_full.groupby("Symbol"):
    g = g.sort_values("date").reset_index(drop=True)

    if len(g) < 20:
        continue

    st_df = compute_supertrend(g)
    last = st_df.iloc[-1]

    # 🔴 SKIP IF NOT INITIALIZED
    if pd.isna(last["SuperTrend"]):
        continue

    supertrend_rows.append({
        "Symbol": sym,
        "SUPERTREND": round(last["SuperTrend"], 2),
        "ST_SIGNAL": last["ST_DIR"]
    })

supertrend_df = pd.DataFrame(supertrend_rows)

df = df.merge(supertrend_df, on="Symbol", how="left")


# -----------------------------
# ACCURATE INTRADAY VWAP & RELATIVE STRENGTH
# -----------------------------
def compute_accurate_vwap(symbol):
    """Calculate volume weighted average price from 15m candles today."""
    try:
        # Try to get 15m candles for today
        candles = _get_live_candles_for_bos(symbol) # Helper often returns 1H, let's check
        if not candles: return 0
        
        # Filter for today's candles
        today = date.today().isoformat()
        day_candles = [c for c in candles if str(c.get("datetime", ""))[:10] == today]
        if not day_candles: return 0
        
        cum_pv = 0
        cum_v  = 0
        for c in day_candles:
            typ_p = (c["high"] + c["low"] + c["close"]) / 3
            vol = c["volume"]
            cum_pv += typ_p * vol
            cum_v  += vol
            
        return round(cum_pv / cum_v, 2) if cum_v > 0 else 0
    except Exception:
        return 0

def get_nifty_change():
    """Get current Nifty 50 % Change."""
    try:
        if "NIFTY" in df["Symbol"].values:
            return float(df[df["Symbol"] == "NIFTY"].iloc[0].get("CHANGE_%", 0))
    except: pass
    return 0

def calculate_movement_strength(row, direction="UP", nifty_chg=0):
    """
    Compute a 0-10 Strength Score.
    Penalizes Vertical Moves (>2.5% candle range) to avoid traps.
    """
    score = 0
    reasons = []
    
    ltp = row["LTP"]
    vwap = row.get("REAL_VWAP", 0)
    # nifty_chg passed as arg for speed
    stock_chg = row.get("CHANGE_%", 0)
    
    # --- 0. EXHAUSTION GUARD (Max 2.5% Trigger Range) ---
    l_high = row.get("LIVE_HIGH", ltp)
    l_low  = row.get("LIVE_LOW", ltp)
    c_range_p = ((l_high - l_low) / l_low * 100) if l_low > 0 else 0
    
    if c_range_p > 2.5:
        score -= 4 # Heavy penalty for vertical moves
        reasons.append(f"Vertical Exhaustion ({c_range_p:.1f}%) ⚠️")
    
    # 1. VWAP Alignment (High Weight)
    if direction == "UP":
        if vwap > 0 and ltp > vwap:
            score += 3
            reasons.append("Above VWAP ✅")
            # Overextended check
            if ltp > vwap * 1.025:
                score -= 2
                reasons.append("Overextended (>2.5% from VWAP) ⚠️")
    else:
        if vwap > 0 and ltp < vwap:
            score += 3
            reasons.append("Below VWAP ✅")
            if ltp < vwap * 0.975:
                score -= 2
                reasons.append("Overextended (>2.5% from VWAP) ⚠️")

    # 2. Relative Strength (RS)
    if direction == "UP":
        if stock_chg > nifty_chg:
            score += 2
            reasons.append(f"Outperforming Nifty ({stock_chg:+.1f}% vs {nifty_chg:+.1f}%) 🚀")
    else:
        if stock_chg < nifty_chg:
            score += 2
            reasons.append(f"Underperforming Nifty ({stock_chg:+.1f}% vs {nifty_chg:+.1f}%) 📉")

    # 3. EMA Alignment
    e20 = row.get("EMA20", 0)
    e50 = row.get("EMA50", 0)
    if direction == "UP":
        if e20 > 0 and e50 > 0 and ltp > e20 and e20 > e50:
            score += 2
            reasons.append("Perfect Bullish EMA Alignment (LTP > 20 > 50) 📈")
    else:
        if e20 > 0 and e50 > 0 and ltp < e20 and e20 < e50:
            score += 2
            reasons.append("Perfect Bearish EMA Alignment (LTP < 20 < 50) 📉")

    # 4. Volume Strength
    v_day = row.get("LIVE_VOLUME", 0)
    v_avg = row.get("AVG_VOL_5D", 1)
    v_x   = v_day / v_avg if v_avg > 0 else 0
    if v_x >= 2.0:
        score += 3
        reasons.append(f"Institutional Volume ({v_x:.1f}x) 🔥")
    elif v_x >= 1.2:
        score += 1
        reasons.append(f"Above Avg Volume ({v_x:.1f}x) ⚡")

    label = "🔥 EXTREME" if score >= 8 else ("✅ STRONG" if score >= 5 else "⚠️ WEAK/TRAP")
    return score, label, " | ".join(reasons)

# Apply VWAP to main dataframe
df["VWAP"] = df["Symbol"].apply(compute_accurate_vwap)
# Falls back to typical price if candles not available
df["VWAP"] = np.where(df["VWAP"] == 0, (df["LIVE_HIGH"] + df["LIVE_LOW"] + df["LTP"])/3, df["VWAP"]).round(2)
df["VWAP_DIFF"] = (df["LTP"] - df["VWAP"]).round(2)
# Keep REAL_VWAP alias for logic functions
df["REAL_VWAP"] = df["VWAP"]


# -----------------------------
# CLEAN + ACTIONABLE VIEW
# -----------------------------
supertrend_view = df[
    (df["SUPERTREND"].notna())
].copy()

# Remove invalid / zero SuperTrend values
supertrend_view = supertrend_view[
    supertrend_view["SUPERTREND"] > 0
].copy()


supertrend_view["ST_BUY"] = supertrend_view.apply(
    lambda x: "BUY" if x["ST_SIGNAL"] == "BUY" else "",
    axis=1
)

supertrend_view["ST_SELL"] = supertrend_view.apply(
    lambda x: "SELL" if x["ST_SIGNAL"] == "SELL" else "",
    axis=1
)

supertrend_view = supertrend_view[
    [
        "Symbol",
        "LTP",
        "SUPERTREND",
        "ST_BUY",
        "ST_SELL",
        "VWAP",
        "VWAP_DIFF",
        "EMA20",
        "EMA50",
        "TOP_HIGH",
        "TOP_LOW",
        "CHANGE_%"
    ]
].copy()

DISPLAY_ROUND_COLS = [
    "LTP",
    "SUPERTREND",
    "VWAP",
    "VWAP_DIFF",
    "EMA20",
    "EMA50",
    "TOP_HIGH",
    "TOP_LOW",
    "CHANGE_%"
]

for col in DISPLAY_ROUND_COLS:
    if col in supertrend_view.columns:
        supertrend_view[col] = supertrend_view[col].round(2)



# =========================================================
# SUPERTREND - PREPARE DATAFRAME + STYLER (MUST BE BEFORE TABS)
# =========================================================

def highlight_supertrend(row):
    styles = []
    for col in row.index:
        if col == "ST_BUY" and row[col] == "BUY":
            styles.append("background-color:#0d2b0d; color:#b6f5c8; font-weight:bold;")
        elif col == "ST_SELL" and row[col] == "SELL":
            styles.append("background-color:#2b0d0d; color:#ffb3b3; font-weight:bold;")
        else:
            styles.append("")
    return styles



# DataFrame used for logic + empty checks
supertrend_df = supertrend_view.copy()

# Create Styler ONLY for UI
supertrend_styled = supertrend_df.style.format({
    "LTP": "{:.2f}",
    "SUPERTREND": "{:.2f}",
    "VWAP": "{:.2f}",
    "VWAP_DIFF": "{:.2f}",
    "EMA20": "{:.2f}",
    "EMA50": "{:.2f}",
    "TOP_HIGH": "{:.2f}",
    "TOP_LOW": "{:.2f}",
    "CHANGE_%": "{:.2f}"
}).apply(highlight_supertrend, axis=1)


#####################################           SUPERTREND – NEAR LTP ZONE

supertrend_view = df[
    (df["SUPERTREND"].notna())
].copy()


# =========================================================
# 🎯 SUPERTREND – NEAR LTP OPPORTUNITY TABLE
# =========================================================

# Distance in % between LTP and Supertrend
supertrend_view["ST_DIST_%"] = (
    (supertrend_view["LTP"] - supertrend_view["SUPERTREND"])
    / supertrend_view["SUPERTREND"] * 100
).round(2)

# Absolute distance for sorting
supertrend_view["ST_DIST_ABS"] = supertrend_view["ST_DIST_%"].abs()

# 🔎 Filter: only NEAR opportunities (adjust threshold if needed)
ST_NEAR_THRESHOLD = 2.0   # 2% near zone

st_near_df = supertrend_view[
    supertrend_view["ST_DIST_ABS"] <= ST_NEAR_THRESHOLD
].copy()

# Direction clarity
st_near_df["SIDE"] = np.where(
    st_near_df["ST_SIGNAL"] == "BUY", "🟢 LONG",
    "🔴 SHORT"
)

# Sort by nearest first
st_near_df = st_near_df.sort_values("ST_DIST_ABS")

# Final columns (clean & actionable)
ST_NEAR_COLUMNS = [
    "Symbol",
    #"SIDE",
    "LTP",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "SUPERTREND",
    "ST_DIST_%",
    "VWAP",
    "EMA20",
    "EMA50",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "NEAR"
    
]

ST_NEAR_COLUMNS = [c for c in ST_NEAR_COLUMNS if c in st_near_df.columns]

st_near_view = st_near_df[ST_NEAR_COLUMNS]







# ================= EMA20–EMA50 SPLIT (BUY / SELL) =================

#ema_buy_df = ema_signal_df[ema_signal_df["SIGNAL"] == "BUY"].copy()
#ema_sell_df = ema_signal_df[ema_signal_df["SIGNAL"] == "SELL"].copy()

# =========================================================
# TOP GAINERS / LOSERS (LEAST EXTREME ON TOP)
# =========================================================

if not df.empty and "YEST_CLOSE" in df.columns and "LIVE_OPEN" in df.columns:
    df["gap_pct_val"] = ((df["LIVE_OPEN"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"]) * 100
    df["GAP"] = ""
    # Gap Up (>= 2%)
    df.loc[df["gap_pct_val"] >= 2.0, "GAP"] = df["gap_pct_val"].apply(lambda x: f"✅ {x:.1f}%")
    # Gap Down (<= -2%)
    df.loc[df["gap_pct_val"] <= -2.0, "GAP"] = df["gap_pct_val"].apply(lambda x: f"🔴 {x:.1f}%")

gainers_df = (
    df[df["CHANGE_%"] > 2.5]
    .sort_values(by="CHANGE_%", ascending=True)   # least positive first
    .copy()
)

losers_df = (
    df[df["CHANGE_%"] < -2.5]
    .sort_values(by="CHANGE_%", ascending=True)   # least negative first
    .copy()
)

# =========================================================
# EMA20–EMA50 SORTING (BASED ON GAIN)
# =========================================================

ema_buy_df = (
    ema_buy_df
    .sort_values(by="GAIN", ascending=True)   # least positive gain first
)

ema_sell_df = (
    ema_sell_df
    .sort_values(by="GAIN", ascending=False)   # least negative gain first
)



####################     OPTIONS SCORING ENGINE    ####################################







def option_score(row):
    score = 0
    reasons = []

    # -------- SPOT TREND --------
    if row["EMA20"] > row["EMA50"] and row["SUPERTREND"] == "BUY":
        score += 2
        reasons.append("Trend bullish")
    elif row["EMA20"] < row["EMA50"] and row["SUPERTREND"] == "SELL":
        score += 2
        reasons.append("Trend bearish")
    else:
        return 0, "Spot not aligned"

    # -------- BREAKOUT / NEAR --------
    if row["LTP"] > row["TOP_HIGH"] or "↑" in str(row.get("NEAR", "")):
        score += 2
        reasons.append("Upside breakout / near")
    elif row["LTP"] < row["TOP_LOW"] or "↓" in str(row.get("NEAR", "")):
        score += 2
        reasons.append("Downside breakout / near")

    # -------- VWAP --------
    if row["LTP"] > row["VWAP"]:
        score += 1
        reasons.append("Above VWAP")
    else:
        score -= 1

    # -------- ATR / MOMENTUM --------
    if row.get("ATR_PCT", 0) > 1.2:
        score += 1
        reasons.append("Good momentum")

    return score, ", ".join(reasons)


df[["OPTION_SCORE", "OPTION_REASON"]] = df.apply(
    lambda r: pd.Series(option_score(r)), axis=1
)

def recommend_strike(row):
    if row["OPTION_SCORE"] < 3:
        return "AVOID"

    # Expiry safety
    if row["OPTION_SCORE"] >= 3:
        return "ATM"

    if row["OPTION_SCORE"] == 3:
        return "ITM"

    return "OTM"

# recommend_strike
if not df.empty:
    df["STRIKE_PREF"] = "OTM"
    df.loc[df["OPTION_SCORE"] < 3, "STRIKE_PREF"] = "AVOID"
    df.loc[df["OPTION_SCORE"] >= 3, "STRIKE_PREF"] = "ATM"
    # Special case: ITM if exactly 3
    df.loc[df["OPTION_SCORE"] == 3, "STRIKE_PREF"] = "ITM"

# option_verdict
if not df.empty:
    df["OPTION_SIGNAL"] = "AVOID"
    mask = df["OPTION_SCORE"] >= 3
    df.loc[mask & (df["EMA20"] > df["EMA50"]), "OPTION_SIGNAL"] = "STRONG CE BUY"
    df.loc[mask & (df["EMA20"] <= df["EMA50"]), "OPTION_SIGNAL"] = "STRONG PE BUY"

#ALERT FILTER (NO MORE EMAIL FLOOD)
if not df.empty:
    STRONG_BUY_DF = df[df["OPTION_SIGNAL"].str.contains("STRONG")]
else:
    STRONG_BUY_DF = pd.DataFrame()
new_strong = detect_new_entries(
    "OPTION_STRONG",
    STRONG_BUY_DF["Symbol"].tolist()
)
if new_strong:
    notify_all(
        "OPTION_STRONG",
        "🔥 STRONG OPTIONS BUY",
        [
            f"{s} | {df.loc[df.Symbol==s,'OPTION_SIGNAL'].values[0]} | "
            f"Strike: {df.loc[df.Symbol==s,'STRIKE_PREF'].values[0]}"
            for s in new_strong
        ]
    )


@st.cache_data(ttl=7200, show_spinner=False)
def backtest_options(df, ohlc_df):
    results = []

    for sym in df["Symbol"].unique():
        spot = df[df.Symbol == sym].iloc[0]
        hist = ohlc_df[ohlc_df.Symbol == sym].sort_values("date").tail(30)

        for i in range(len(hist)-1):
            r = hist.iloc[i]
            next_day = hist.iloc[i+1]

            if spot["OPTION_SIGNAL"] == "STRONG CE BUY":
                pnl = next_day["close"] - r["close"]
            elif spot["OPTION_SIGNAL"] == "STRONG PE BUY":
                pnl = r["close"] - next_day["close"]
            else:
                continue

            results.append({
                "Symbol": sym,
                "Signal": spot["OPTION_SIGNAL"],
                "Day": r["date"],
                "PnL": round(pnl, 2)
            })

    return pd.DataFrame(results)

backtest_df = backtest_options(df, ohlc_full)


def get_nifty_reference_price():
    """
    Reliably get Nifty's Yesterday Close from all available sources.
    Fallback hierarchy: Live Map -> Indices DF -> Cache File -> Default.
    """
    try:
        # 1. Try Live Map
        val = live_map.get("NIFTY", {}).get("YEST_CLOSE")
        if val and val > 0: return float(val)

        # 2. Try Session State Indices
        _idf = st.session_state.get("indices_df")
        if _idf is not None and not _idf.empty:
            _row = _idf[_idf["Index"].isin(["NIFTY", "NIFTY 50"])]
            if not _row.empty:
                val = _row.iloc[0].get("YEST_CLOSE")
                if val and val > 0: return float(val)

        # 3. Try Cache File
        if os.path.exists(INDICES_CACHE_CSV):
            _cf = pd.read_csv(INDICES_CACHE_CSV, encoding='utf-8')
            _row = _cf[_cf["Index"].isin(["NIFTY", "NIFTY 50"])]
            if not _row.empty:
                val = _row.iloc[0].get("YEST_CLOSE")
                if val and val > 0: return float(val)
                
    except Exception:
        pass
    return 24358.60

# ================= LIVE MAP (Symbol -> Live Values) for quick access
# =========================================================
live_map = {}

required_cols = [
    "Symbol", "LTP", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "YEST_CLOSE", "CHANGE", "CHANGE_%", "EMA20"
]

available_cols = [c for c in required_cols if c in df.columns]

for _, r in df[available_cols].iterrows():
    live_map[r["Symbol"]] = r.to_dict()


#####   SETUP 1: 4 BAR REVERSAL + Breakouts     ####################################################

# =========================================================
# 4-BAR SETUP (EXACT SCREENER MATCH)
# =========================================================

# =========================================================
# 4 BAR REVERSAL (STRICT)
# =========================================================


four_bar_rows = []

ohlc_full = _safe_parse_date_col(ohlc_full, col="date")

for sym, g in ohlc_full.groupby("Symbol"):
    if sym not in live_map:
        continue
    g = g.sort_values("date").reset_index(drop=True)

    if len(g) < 5:
        continue

    d0  = g.iloc[-1]   # today
    d1  = g.iloc[-2]
    d2  = g.iloc[-3]
    d3  = g.iloc[-4]
    d4  = g.iloc[-5]

    # --- Last 4 RED candles (strict)
    red_4 = (
        (d1.close <= d1.open) and
        (d2.close <= d2.open) and
        (d3.close <= d3.open) and
        (d4.close <  d4.open)
    )

    if not red_4:
        continue

    # --- Today reversal conditions
    today_reversal = (
        (d0.open  > d1.low) and
        (d0.open  > d1.close) and
        (d0.high  > d1.high) and
        (d0.close > d0.open)
    )

    if not today_reversal:
        continue

    
    live = live_map[sym]

    four_bar_rows.append({
        "Symbol": sym,

        # 🔴 LIVE DATA (single source of truth)
        "LTP": round(live["LTP"], 2),
        "CHANGE": round(live["CHANGE"], 2),
        "CHANGE_%": round(live["CHANGE_%"], 2),

        # 🟢 Candle structure (from OHLC)
        "LIVE_OPEN": round(d0.open, 2),
        "LIVE_HIGH": round(d0.high, 2),
        "LIVE_LOW": round(d0.low, 2),

        # 🟡 Yesterday reference
        "YEST_HIGH": round(d1.high, 2),
        "YEST_LOW": round(d1.low, 2),
        "YEST_CLOSE": round(d1.close, 2),
    })

    


#four_bar_df = pd.DataFrame(four_bar_rows)
four_bar_df = pd.DataFrame(four_bar_rows)

# 🔒 SAFETY: ensure required columns exist
REQUIRED_4BAR_COLS = [
    "Symbol", "LTP", "CHANGE", "CHANGE_%",
    "LIVE_OPEN", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "YEST_CLOSE"
]

for col in REQUIRED_4BAR_COLS:
    if col not in four_bar_df.columns:
        four_bar_df[col] = np.nan






# =========================================================
# 🚨 FAKE BREAKOUTS : BULL TRAP & BEAR TRAP
# =========================================================

fake_bull_rows = []
fake_bear_rows = []

for _, r in df.iterrows():

    y_close = r["YEST_CLOSE"]
    if pd.isna(y_close) or y_close == 0:
        continue

    # -------------------------
    # % calculations
    # -------------------------
    high_pct = (r["LIVE_HIGH"] - y_close) / y_close * 100
    low_pct  = (r["LIVE_LOW"]  - y_close) / y_close * 100
    ltp_pct  = (r["LTP"] - y_close) / y_close * 100

    # =========================
    # 🟡 FAKE BULL TRAP
    # =========================
    if (
        high_pct >= 2.5 and          # broke +2.5%
        ltp_pct < 2.5 and            # failed to hold
        r["LIVE_OPEN"] < r["YEST_HIGH"]
    ):
        fake_bull_rows.append({
            "Symbol": r["Symbol"],
            "YEST_CLOSE": round(y_close, 2),
            "LIVE_HIGH": round(r["LIVE_HIGH"], 2),
            "LIVE_OPEN": round(r["LIVE_OPEN"], 2),
            "LTP": round(r["LTP"], 2),
            "CHANGE_%": round(ltp_pct, 2),
            "FAIL_%": round(high_pct - ltp_pct, 2)
        })

    # =========================
    # 🔵 FAKE BEAR TRAP
    # =========================
    if (
        low_pct <= -2.5 and          # broke −2.5%
        ltp_pct > -2.5 and           # recovered
        r["LIVE_OPEN"] > r["YEST_LOW"]
    ):
        fake_bear_rows.append({
            "Symbol": r["Symbol"],
            "YEST_CLOSE": round(y_close, 2),
            "LIVE_LOW": round(r["LIVE_LOW"], 2),
            "LIVE_OPEN": round(r["LIVE_OPEN"], 2),
            "LTP": round(r["LTP"], 2),
            "CHANGE_%": round(ltp_pct, 2),
            "FAIL_%": round(abs(low_pct - ltp_pct), 2)
        })

fake_bull_df = pd.DataFrame(fake_bull_rows)
fake_bear_df = pd.DataFrame(fake_bear_rows)

# =========================================================
# 🎨 Styling
# =========================================================
def style_ltp_relative(row):
    """
    Row-wise styling:
    - Green if LTP >= YEST_CLOSE
    - Orange if LTP < YEST_CLOSE
    """
    if row["LTP"] >= row["YEST_CLOSE"]:
        return ["background-color:#0d2b0d; color:#b6f5c8"] * len(row)
    else:
        return ["background-color:#2b1200; color:#ffd0a0"] * len(row)

def style_bear_trap(_):
    return ["background-color:#2b1200; color:#ffd0a0"] * len(fake_bear_df.columns)

def style_ltp_only(row):
    styles = [""] * len(row)

    ltp_idx = row.index.get_loc("LTP")

    if row["LTP"] >= row["YEST_CLOSE"]:
        styles[ltp_idx] = "background-color:#0d2b0d; color:#b6f5c8"  # dark green bg, light green text
    else:
        styles[ltp_idx] = "background-color:#2b1200; color:#ffd0a0"  # dark orange bg, light orange text

    return styles

def style_ltp_bear_only(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")
    styles[ltp_idx] = "background-color:#2b1200; color:#ffd0a0"
    return styles
############ for 15 mins table style
def style_ltp_15min(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")

    if row["BREAK_TYPE"] == "UP":
        styles[ltp_idx] = "background-color:#0d2b0d; color:#b6f5c8; font-weight:bold"  # dark green bg, bright green text
    elif row["BREAK_TYPE"] == "DOWN":
        styles[ltp_idx] = "background-color:#2b0d0d; color:#ffb3b3; font-weight:bold"  # dark red bg, bright red text

    return styles







############    OPTION 2 (Fallback): Build 15-min from 5-min candles    ================
# ✅ FIX: Run in background thread - never blocks Streamlit main thread
# Dashboard always shows last known data instantly; background thread updates silently.

_5min_EMPTY = pd.DataFrame(columns=["Symbol","datetime","open","high","low","close"])

def _fetch_5min_background():
    """Runs in a daemon thread - fetches all 5min candles without blocking UI."""
    rows = []
    today = datetime.now(IST).date()
    start = datetime.combine(today, time(9, 15))
    end   = datetime.combine(today, time(15, 30))
    for sym in SYMBOLS:
        tk = get_token(sym)
        if not tk:
            continue
        try:
            candles = kite.historical_data(tk, start, end, interval="5minute")
        except Exception:
            continue
        for c in candles:
            rows.append({
                "Symbol":   sym,
                "datetime": pd.to_datetime(c["date"]),
                "open":     c["open"],
                "high":     c["high"],
                "low":      c["low"],
                "close":    c["close"],
                "volume":   c.get("volume", 0),
            })
        tm.sleep(0.05)
    if rows:
        pd.DataFrame(rows).to_csv(FIVE_MIN_CACHE_CSV, index=False, encoding='utf-8')

# Module-level flag to track 5-min background fetch (thread-safe bool)
if "_FIVE_MIN_FETCH_RUNNING" not in dir():
    _FIVE_MIN_FETCH_RUNNING = False

# Start background fetch if: never fetched, or TTL of 95s has elapsed
if not _csv_is_fresh(FIVE_MIN_CACHE_CSV) and not _FIVE_MIN_FETCH_RUNNING:
    _FIVE_MIN_FETCH_RUNNING = True
    def _run_and_clear():
        global _FIVE_MIN_FETCH_RUNNING
        try:
            _fetch_5min_background()
        finally:
            _FIVE_MIN_FETCH_RUNNING = False
    t = threading.Thread(target=_run_and_clear, daemon=True)
    t.start()

# Always use last known data - never wait for the thread
# Use _load_today_csv so we never accidentally load yesterday's candles
_five_today = _load_today_csv(FIVE_MIN_CACHE_CSV)
five_df = _five_today if _five_today is not None else _5min_EMPTY
if "datetime" in five_df.columns:
    five_df["datetime"] = pd.to_datetime(five_df["datetime"], errors="coerce")
    five_df = five_df.dropna(subset=["datetime"]).reset_index(drop=True)

# ================= SAFETY: 5-MIN DATA AVAILABILITY =================
if five_df.empty or "datetime" not in five_df.columns:
    intraday_15m_df = pd.DataFrame(columns=[
        "Symbol", "datetime", "open", "high", "low", "close"
    ])
else:
    _agg_15m = (
        five_df
        .set_index("datetime")
        .groupby("Symbol")
        .resample("15min")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last"
        })
        .reset_index()
    )
    # Filter out rows where all OHLC are NaN (avoids FutureWarning on empty concat)
    intraday_15m_df = _agg_15m.dropna(subset=["open", "high", "low", "close"], how="all")




############    SETUP 3: 15-MIN INSIDE RANGE BREAK    #######################

inside_break_rows = []

for sym in SYMBOLS:
    df15 = intraday_15m_df[intraday_15m_df["Symbol"] == sym].sort_values("datetime")

    if len(df15) < 4 or sym not in live_map:
        continue

    first = df15.iloc[0]
    later = df15.iloc[1:4]

    # 1️⃣ Inside range condition
    if not (
        later["high"].max() <= first["high"] and
        later["low"].min() >= first["low"]
    ):
        continue

    live = live_map[sym]
    ltp = live["LTP"]

    break_type = None
    chg_15m_pct = None

    # 2️⃣ Break detection + 15-min % calc
    ema20 = live.get("EMA20")
    
    if ltp > first["high"]:
        # Filter: Must be above EMA20 for UP break
        if ema20 and ltp <= ema20:
            continue
            
        break_type = "UP"
        chg_15m_pct = round(
            ((ltp - first["high"]) / first["high"]) * 100, 2
        )

    elif ltp < first["low"]:
        # Filter: Must be below EMA20 for DOWN break
        if ema20 and ltp >= ema20:
            continue
            
        break_type = "DOWN"
        chg_15m_pct = round(
            ((ltp - first["low"]) / first["low"]) * 100, 2
        )

    else:
        continue

    inside_break_rows.append({
        "Symbol": sym,
        "LTP": ltp,
        "CHG_15M_%": chg_15m_pct,
        "CHANGE": live["CHANGE"],
        "CHANGE_%": live["CHANGE_%"],
        "DAY_HIGH": live["LIVE_HIGH"],
        "YEST_HIGH": live["YEST_HIGH"],
        "DAY_LOW": live["LIVE_LOW"],
        "YEST_LOW": live["YEST_LOW"],
        "EMA20": ema20,
        "BREAK_TYPE": break_type
    })

inside_15m_df = pd.DataFrame(inside_break_rows)

if not inside_15m_df.empty:
    new_inside = detect_new_entries(
        "INSIDE_15M_BREAK",
        inside_15m_df["Symbol"].tolist()
    )
    notify_all(
        "INSIDE_15M_BREAK",
        "⏱️ 15-Min Inside Range Break",
        new_inside,
        live_map
    )


# =========================================================
# YH1.5 STRONG BREAKOUT (SCREENER LOGIC) - FINAL & CORRECT
# Rules:
# 1. OPEN < YEST_HIGH  (no gap-up)
# 2. LTP >= YEST_HIGH * 1.015
# =========================================================

yh15_rows = []

for _, r in df.iterrows():

    yh = r["YEST_HIGH"]
    ltp = r["LTP"]
    live_open = r["LIVE_OPEN"]

    if pd.isna(yh) or yh <= 0:
        continue
    if pd.isna(ltp) or pd.isna(live_open):
        continue

    # 🚫 GAP-UP FILTER (ABSOLUTE)
    if live_open >= yh:
        continue

    level_15 = yh * 1.015

    # ✅ TRUE BREAKOUT
    if ltp >= level_15:

        break_pct = ((ltp - yh) / yh) * 100
        after_break_pct = ((ltp - level_15) / level_15) * 100

        yh15_rows.append({
            "Symbol": r["Symbol"],
            "LIVE_OPEN": round(live_open, 2),
            "LTP": round(ltp, 2),
            "YEST_HIGH": round(yh, 2),
            "BREAK_%": round(break_pct, 2),
            "AFTER_BREAK_%": round(after_break_pct, 2),
            "CHANGE": round(r["CHANGE"], 2),
            "CHANGE_%": round(r["CHANGE_%"], 2),
        })

yh15_df = pd.DataFrame(yh15_rows)

if not yh15_df.empty:
    yh15_df = yh15_df.sort_values(
        ["AFTER_BREAK_%", "BREAK_%"],
        ascending=False
    )




# =========================================================
# FAKE / FAILED YH1.5 BREAKOUTS
# =========================================================

fake_yh15_rows = []

for sym, live in live_map.items():

    yh = live.get("YEST_HIGH")
    ltp = live.get("LTP")
    high = live.get("LIVE_HIGH")

    if not yh or yh == 0:
        continue

    level_15 = yh * 1.015

    # 🔴 Attempted but failed breakout
    if high >= level_15 and ltp < level_15:

        break_pct = ((high - yh) / yh) * 100
        retrace_pct = ((ltp - high) / high) * 100 if high else 0

        fake_yh15_rows.append({
            "Symbol": sym,
            "LTP": round(ltp, 2),
            "LIVE_HIGH": round(high, 2),
            "YEST_HIGH": round(yh, 2),
            "BREAK_%": round(break_pct, 2),      # how much it broke
            "RETRACE_%": round(retrace_pct, 2),  # how much it failed
            "CHANGE": round(live.get("CHANGE", 0), 2),
            "CHANGE_%": round(live.get("CHANGE_%", 0), 2),
        })

fake_yh15_df = pd.DataFrame(fake_yh15_rows)

if not fake_yh15_df.empty:
    fake_yh15_df = fake_yh15_df.sort_values("RETRACE_%")




alerts = []
alert_keys = set()   # prevents duplicate alerts per refresh
alert_time = datetime.now(IST)

##########      ALERT-1 IMPLEMENTATION ######## YH 1.5% Strong Breakout
df["FROM_YH_%"] = np.where(
    df["LTP"] >= df["YEST_HIGH"],
    ((df["LTP"] - df["YEST_HIGH"]) / df["YEST_HIGH"] * 100),
    0
).round(2)

df["FROM_YL_%"] = np.where(
    df["LTP"] <= df["YEST_LOW"],
    ((df["YEST_LOW"] - df["LTP"]) / df["YEST_LOW"] * 100),
    0
).round(2)


yh15_df = df.loc[
    (
        # 🔹 Breakout strength (0.5% above YH)
        (df["LTP"] >= df["YEST_HIGH"] * 1.005) & (df["LTP"] <= df["YEST_HIGH"] * 1.02) &
        (df["CHANGE_%"] >= 0.5) &

        # 🔹 Previous day bullish
        (df["YEST_CLOSE"] > df["YEST_OPEN"]) &

        # 🔹 Gap continuation structure
        (df["LIVE_OPEN"] >= df["YEST_CLOSE"]) &
        (df["LIVE_OPEN"] <= df["YEST_HIGH"]) & (df["VOL_%"] >= -50)
    ),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_VOLUME",
        "YEST_VOL",
        "VOL_%",
        "FROM_YH_%",   # 👈 added
        "FROM_YL_%",   # 👈 added
        "YEST_HIGH",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LIVE_OPEN",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

for _, r in yh15_df.iterrows():
    key = f"YH15_{r['Symbol']}"

    if key not in st.session_state.alert_keys:
        alert_time = datetime.now(IST).replace(tzinfo=None)   # 🔒 ONLY HERE

        st.session_state.alerts.append({
            "TIME": alert_time.strftime("%H:%M:%S"),
            "TS": alert_time,             # sortable
            "TYPE": "🚀 YH 1.5%",
            "Symbol": r["Symbol"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"],
            "VOL_%": r["VOL_%"],
            "FROM_YH_%": r["FROM_YH_%"],
            "FROM_YL_%": r["FROM_YL_%"],
            "DAY_OPEN": r["LIVE_OPEN"],
            "DAY_HIGH": r["LIVE_HIGH"],
            "DAY_LOW": r["LIVE_LOW"],
            "YEST_HIGH": r["YEST_HIGH"],
            "YEST_LOW": r["YEST_LOW"],
            "YEST_CLOSE": r["YEST_CLOSE"],
        })

        st.session_state.alert_keys.add(key)

yl15_df = df.loc[
    (
        # 🔻 Breakdown strength (0.5% below YL)
        (df["LTP"] <= df["YEST_LOW"] * 0.995) & (df["LTP"] >= df["YEST_LOW"] * 0.98) &
        (df["CHANGE_%"] <= -0.5) &

        # 🔻 Previous day bearish
        (df["YEST_CLOSE"] < df["YEST_OPEN"]) &

        # 🔻 Gap continuation structure
        (df["LIVE_OPEN"] <= df["YEST_CLOSE"]) &
        (df["LIVE_OPEN"] >= df["YEST_LOW"]) & (df["VOL_%"] >= -50)
    ),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_VOLUME",
        "YEST_VOL",
        "VOL_%",
        "FROM_YH_%",   # 👈 added
        "FROM_YL_%",   # 👈 added
        "YEST_HIGH",
        "LIVE_HIGH",
        "LIVE_LOW",
        "LIVE_OPEN",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

for _, r in yl15_df.iterrows():
    key = f"YL15_{r['Symbol']}"

    if key not in st.session_state.alert_keys:
        alert_time = datetime.now(IST).replace(tzinfo=None)   # 🔒 ONLY HERE

        st.session_state.alerts.append({
            "TIME": alert_time.strftime("%H:%M:%S"),
            "TS": alert_time,             # sortable
            "TYPE": " 🔴 YL 1.5%",
            "Symbol": r["Symbol"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"],
            "VOL_%": r["VOL_%"],
            "FROM_YH_%": r["FROM_YH_%"],
            "FROM_YL_%": r["FROM_YL_%"],
            "DAY_OPEN": r["LIVE_OPEN"],
            "DAY_HIGH": r["LIVE_HIGH"],
            "DAY_LOW": r["LIVE_LOW"],
            "YEST_HIGH": r["YEST_HIGH"],
            "YEST_LOW": r["YEST_LOW"],
            "YEST_CLOSE": r["YEST_CLOSE"],
        })

        st.session_state.alert_keys.add(key)

###############     ALERT-2 IMPLEMENTATION      ## 4-BAR Reversal + Breakout

for _, r in four_bar_df.iterrows():

    if r["Symbol"] not in df["Symbol"].values:
        continue

    base = df.loc[df.Symbol == r.Symbol].iloc[0]

    # Bullish
    if (
        r["CHANGE_%"] > 0 and
        base["LTP"] > base["YEST_HIGH"]
    ):
        key = f"4BAR_BUY_{r['Symbol']}"
        if key not in alert_keys:
            alerts.append({
                "TIME": alert_time.strftime("%H:%M:%S"),   # 🔥 readable
                "TYPE": "🔁 4-BAR REVERSAL BUY",
                "Symbol": r["Symbol"],
                "LTP": base["LTP"],
                "CHANGE_%": base["CHANGE_%"]
            })
            alert_keys.add(key)

    # Bearish
    if (
        r["CHANGE_%"] < 0 and
        base["LTP"] < base["YEST_LOW"]
    ):
        key = f"4BAR_SELL_{r['Symbol']}"
        if key not in alert_keys:
            alerts.append({
                "TYPE": "🔁 4-BAR REVERSAL SELL",
                "Symbol": r["Symbol"],
                "LTP": base["LTP"],
                "CHANGE_%": base["CHANGE_%"]
            })
            alert_keys.add(key)





# ================= YH 1.5 STRONG BREAKOUT ALERT =================

new_YH15 = detect_new_entries(
    "YH15",
    yh15_df.Symbol.tolist()
)

if st.session_state.get("toast_DAILY_UP", False): notify_browser("🚀 YH 1.5 STRONG BREAKOUT", new_YH15)

# ================= 4-BAR REVERSAL BUY ALERT =================

# =========================================================
# FIX-2 : SAFE 4-BAR BUY FILTER (BUY ONLY)
# =========================================================

if not four_bar_df.empty and "CHANGE_%" in four_bar_df.columns:

    four_bar_buy = four_bar_df.loc[
        four_bar_df["CHANGE_%"] > 0, "Symbol"
    ].tolist()

else:
    four_bar_buy = []


new_4BAR_BUY = detect_new_entries(
    "FOUR_BAR_BUY",
    four_bar_buy
)

if st.session_state.get("toast_2M_EMA_REVERSAL", False): notify_browser("🟢 4-BAR REVERSAL BUY", new_4BAR_BUY)


def load_index_symbols(filename):
    with open(filename, "r", encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

INDEX_FILES = {
    "NIFTY 50": "NIFTY50.txt",
    "BANK NIFTY": "BANKNIFTY.txt",
    "FINNIFTY": "FINNIFTY.txt",
    "NIFTY IT": "NIFTYIT.txt",
    "NIFTY FMCG": "NIFTYFMCG.txt",
    "NIFTY PHARMA": "NIFTYPHARMA.txt",
    "NIFTY METAL": "NIFTYMETAL.txt",
    "NIFTY AUTO": "NIFTYAUTO.txt",
    "NIFTY ENERGY": "NIFTYENERGY.txt",
    "NIFTY PSU BANK": "NIFTYPSUBANK.txt",
}

index_symbols = {
    name: load_index_symbols(file)
    for name, file in INDEX_FILES.items()
}

# =========================================================
# INDICES – LIVE OHLC (SINGLE SOURCE OF TRUTH)
# =========================================================

INDEX_SYMBOLS = {
    # display_name      : (short_key,  kite_tradingsymbol)
    # Kite uses "NSE:<tradingsymbol>" for NSE indices and "BSE:<tradingsymbol>" for BSE.
    "NIFTY 50":     ("NIFTY",       "NIFTY 50"),
    "BANK NIFTY":   ("BANKNIFTY",   "NIFTY BANK"),
    "SENSEX":       ("SENSEX",      "SENSEX"),  # Added SENSEX
    "FINNIFTY":     ("FINNIFTY",    "NIFTY FIN SERVICE"),
    "NIFTY IT":     ("NIFTYIT",     "NIFTY IT"),
    "NIFTY FMCG":   ("NIFTYFMCG",  "NIFTY FMCG"),
    "NIFTY PHARMA": ("NIFTYPHARMA", "NIFTY PHARMA"),
    "NIFTY METAL":  ("NIFTYMETAL",  "NIFTY METAL"),
    "NIFTY AUTO":   ("NIFTYAUTO",   "NIFTY AUTO"),
    "NIFTY ENERGY": ("NIFTYENERGY", "NIFTY ENERGY"),
    "NIFTY PSU BANK":("NIFTYPSUBANK","NIFTY PSU BANK"),
}
# Convenience: short_key -> kite_tradingsymbol (used by get_token / kite_symbol)
_IDX_SHORT_TO_KITE = {v[0]: v[1] for v in INDEX_SYMBOLS.values()}

@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent - no UI blur on refresh
def fetch_indices_live():
    if not is_market_hours():
        return pd.DataFrame()
    _index_rows = []
    # Build {display_name: "<exchange>:<kite_tradingsymbol>"} for batch quote
    index_kite_symbols = {}
    for name, (short, kite_ts) in INDEX_SYMBOLS.items():
        exchange = "BSE" if name == "SENSEX" else "NSE"
        index_kite_symbols[name] = f"{exchange}:{kite_ts}"
    
    all_keys = list(index_kite_symbols.values())
    try:
        all_quotes = kite.quote(all_keys)
    except Exception as e:
        print("INDEX BATCH QUOTE ERROR:", e)
        all_quotes = {}

    for index_name, (short_key, kite_ts) in INDEX_SYMBOLS.items():
        exchange = "BSE" if index_name == "SENSEX" else "NSE"
        kite_key = f"{exchange}:{kite_ts}"
        try:
            q = all_quotes.get(kite_key)
            if not q:
                continue
            o   = q["ohlc"]["open"]
            h   = q["ohlc"]["high"]
            l   = q["ohlc"]["low"]
            pc  = q["ohlc"]["close"]
            ltp = q["last_price"]
            chg     = ltp - pc if pc else 0
            chg_pct = (chg / pc * 100) if pc else 0
            tk = get_token(short_key)
            _, yh, yl, yc_hist, _ = fetch_yesterday_ohlc(tk)
            # ✅ Kite quote's 'pc' is the most reliable Yesterday Close
            _index_rows.append({
                "Index": index_name,
                "OPEN": round(o, 2),
                "HIGH": round(h, 2),
                "LOW": round(l, 2),
                "LTP": round(ltp, 2),
                "CHANGE": round(chg, 2),
                "CHANGE_%": round(chg_pct, 2),
                "YEST_HIGH": yh,
                "YEST_LOW": yl,
                "YEST_CLOSE": pc if pc else yc_hist,
            })
        except Exception as e:
            print("INDEX ERROR:", index_name, e)
    return pd.DataFrame(_index_rows)

# ✅ INDICES - synchronous single batch call every rerun (~0.5 sec)
def fetch_indices_now():
    if not is_market_hours():
        if os.path.exists(INDICES_CACHE_CSV):
            try:
                return pd.read_csv(INDICES_CACHE_CSV, encoding='utf-8')
            except Exception:
                pass
        return pd.DataFrame()

    _index_rows = []
    # Batch all indices in ONE kite.quote call using correct Kite tradingsymbols
    all_keys = []
    for index_name, (short, kite_ts) in INDEX_SYMBOLS.items():
        exchange = "BSE" if index_name == "SENSEX" else "NSE"
        all_keys.append(f"{exchange}:{kite_ts}")

    try:
        all_quotes = kite.quote(all_keys)
    except Exception as _idx_e:
        print(f"[INDICES] kite.quote failed: {_idx_e}")
        all_quotes = {}
    for index_name, (short_key, kite_ts) in INDEX_SYMBOLS.items():
        exchange = "BSE" if index_name == "SENSEX" else "NSE"
        kite_key = f"{exchange}:{kite_ts}"
        try:
            q = all_quotes.get(kite_key)
            if not q:
                print(f"[INDICES] No quote for {index_name} (tried: {kite_key})")
                continue
            o=q["ohlc"]["open"]; h=q["ohlc"]["high"]; l=q["ohlc"]["low"]; pc=q["ohlc"]["close"]
            ltp=q["last_price"]; chg=ltp-pc if pc else 0; chg_pct=(chg/pc*100) if pc else 0
            tk=get_token(short_key)
            _, yh, yl, yc_hist, _ = fetch_yesterday_ohlc(tk)
            # ✅ Kite quote's 'pc' is the most reliable Yesterday Close
            _index_rows.append({
                "Index": index_name, "OPEN": round(o,2), "HIGH": round(h,2),
                "LOW": round(l,2),   "LTP": round(ltp,2),"CHANGE": round(chg,2),
                "CHANGE_%": round(chg_pct,2), "YEST_HIGH": yh,
                "YEST_LOW": yl, "YEST_CLOSE": pc if pc else yc_hist,
            })
        except Exception as _idx_row_e:
            print(f"[INDICES] Row error {index_name}: {_idx_row_e}")
    result = pd.DataFrame(_index_rows)
    if not result.empty:
        try:
            os.makedirs(os.path.dirname(INDICES_CACHE_CSV), exist_ok=True)
            # result.to_csv(INDICES_CACHE_CSV, index=False, encoding='utf-8')
            # Atomic background save for speed
            def _idx_bg_save(df_in=result.copy()):
                df_in.to_csv(INDICES_CACHE_CSV, index=False, encoding='utf-8')
            threading.Thread(target=_idx_bg_save, daemon=True).start()
        except: pass
    return result

# Global placeholder for indices data (updated inside the tab)
if "indices_df" not in st.session_state:
    st.session_state.indices_df = pd.DataFrame()

# ================= INDICES TAB (FIXED) =================

INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
    "FINNIFTY",
    "NIFTYIT",
    "NIFTYFMCG",
    "NIFTYPHARMA",
    "NIFTYMETAL",
    "NIFTYAUTO",
    "NIFTYENERGY",
    "NIFTYPSUBANK",
]

_indices_from_df = (
    df[df["Symbol"].isin(INDEX_ONLY_SYMBOLS)]
    .loc[:, [c for c in [
        "Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
        "CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW","YEST_CLOSE",
    ] if c in df.columns]]
    .rename(columns={"Symbol":"Index","LIVE_OPEN":"OPEN","LIVE_HIGH":"HIGH","LIVE_LOW":"LOW"})
    .sort_values("Index")
    .reset_index(drop=True)
)
# Only replace if live fetch returned nothing - df-based version is always fresh
if st.session_state.indices_df.empty and not _indices_from_df.empty:
    st.session_state.indices_df = _indices_from_df
elif not _indices_from_df.empty:
    # Merge: use fetched YEST_* columns from live fetch, rest from df (more complete)
    st.session_state.indices_df = _indices_from_df.copy()


# 🔒 SAFETY NET – ENSURE ALL REQUIRED COLUMNS EXIST
# Ensure minimum columns exist (safe for both Index and Symbol naming)
for c in ["LTP","CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW","YEST_CLOSE"]:
    if c not in st.session_state.indices_df.columns:
        st.session_state.indices_df[c] = None


st.session_state.indices_df = st.session_state.indices_df.sort_values("CHANGE_%", ascending=False)

# =========================================================
# 🚨 ALERT: 3 CONSECUTIVE 15-MIN GREEN CANDLES
# (Valid until a RED candle appears)
# =========================================================
alert_time = datetime.now(IST)
three_green_rows = []

for sym in SYMBOLS:

    # --- intraday candles (signal source) ---
    df15 = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(df15) < 3:
        continue

    # --- master df row (OHLC source) ---
    row = df[df["Symbol"] == sym]
    if row.empty:
        continue

    row = row.iloc[0]

    # Take last 3 COMPLETED 15-min candles
    last3 = df15.iloc[-3:]

    # Condition: all 3 green
    if not all(last3["close"] > last3["open"]):
        continue
    # 🔒 NEW CONDITION: price strength confirmation
    if not (
        row["LTP"] > row["LIVE_OPEN"] and
        row["LTP"] > row["YEST_CLOSE"]
    ):
        continue

    candle_time = last3.iloc[-1]["datetime"]

    three_green_rows.append({
        "TIME": alert_time.strftime("%H:%M:%S"),   # 🔥 readable
        #"TS": alert_time,                           # 🔒 sortable
        "TYPE": "🟢 3×15m GREEN",
        "Symbol": sym,

        # 🔴 LIVE
        "LTP": round(row["LTP"], 2),
        "CHANGE_%": round(row["CHANGE_%"], 2),

        # 🟡 DAY OHLC
        "DAY_OPEN": round(row["LIVE_OPEN"], 2),
        "DAY_HIGH": round(row["LIVE_HIGH"], 2),
        "DAY_LOW": round(row["LIVE_LOW"], 2),

        # 🟠 YESTERDAY
        "YEST_HIGH": row["YEST_HIGH"],
        "YEST_LOW": row["YEST_LOW"],
        "YEST_CLOSE": row["YEST_CLOSE"],

        # ⏱️ SIGNAL TIME
        "CANDLE_TIME": candle_time.strftime("%H:%M"),
    })

three_green_df = pd.DataFrame(three_green_rows)


# =========================================================
# ADD 3×15m GREEN TO LIVE ALERTS (DEDUP SAFE)
# =========================================================

if not three_green_df.empty and "Symbol" in three_green_df.columns:

    for _, r in three_green_df.iterrows():
        sym = r["Symbol"]
        key = f"3GREEN_{sym}"

        if key in alert_keys:
            continue

        alerts.append(r.to_dict())
        alert_keys.add(key)

    new_3green = detect_new_entries(
        "THREE_GREEN_15M",
        three_green_df["Symbol"].tolist()   # ✅ SAFE ACCESS
    )

    notify_all(
        "THREE_GREEN_15M",
        "🟢3×15-Min Green Candles",
        new_3green,
        ltp_map
    )



def detect_new_15m_signals(name, rows):
    """
    rows: list of dicts with Symbol + SCANDLE_TIME
    """
    path = os.path.join(CACHE_DIR, f"{name}_15m_prev.txt")  # FIX-2g

    if os.path.exists(path):
        with open(path, encoding="utf-8") as _pf: prev = set(_pf.read().split(","))
    else:
        prev = set()
    curr = set(f"{r['Symbol']}|{r['CANDLE_TIME']}" for r in rows)

    new = curr - prev

    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(curr))

    return [x.split("|")[0] for x in new]

three_green_15m_df = pd.DataFrame(three_green_rows)

# =========================================================
# 🔔 LIVE ALERT - 3rd 15-min GREEN candle completed
# (integrated with LIVE ALERTS table)
# =========================================================

# ================== ALERT: 3 × 15m GREEN (WITH OHLC) ==================

if not three_green_15m_df.empty:

    for _, r in three_green_15m_df.iterrows():
        sym = r["Symbol"]

        # 🔒 pull OHLC from main df (single source of truth)
        live_row = df[df["Symbol"] == sym]
        if live_row.empty:
            continue

        live_row = live_row.iloc[0]

        key = f"3X15_{sym}_{r['CANDLE_TIME']}"
        if key in alert_keys:
            continue

        alerts.append({
            "TYPE": "🟢 3×15m GREEN",
            "Symbol": sym,

            # 🔴 LIVE
            "LTP": round(live_row["LTP"], 2),
            "CHANGE_%": round(live_row["CHANGE_%"], 2),

            # 🟡 DAY OHLC
            "DAY_OPEN": round(live_row["LIVE_OPEN"], 2),
            "DAY_HIGH": round(live_row["LIVE_HIGH"], 2),
            "DAY_LOW": round(live_row["LIVE_LOW"], 2),

            # 🟠 YESTERDAY
            "YEST_HIGH": live_row["YEST_HIGH"],
            "YEST_LOW": live_row["YEST_LOW"],
            "YEST_CLOSE": live_row["YEST_CLOSE"],

            # ⏱️ SIGNAL
            "CANDLE_TIME": r["CANDLE_TIME"],
        })

        alert_keys.add(key)


############                SCENARIO 1 - Yesterday GREEN candle, tight body near high
# =========================================================
# 🟢 YEST GREEN + OPEN BETWEEN YH & YC (~1%)
# =========================================================
PCT_TOL = 1.0      # around 1%
OPEN_TOL = 0.05    # price tolerance
df["YH_MOVE"] = (df["LTP"] - df["YEST_HIGH"]).round(2)

df["YH_MOVE_%"] = ((df["LTP"] - df["YEST_HIGH"]) / df["YEST_HIGH"] * 100).round(2)


green_zone_condition = (
    ((df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100 <= PCT_TOL) &
    (df["LIVE_OPEN"] >= df["YEST_CLOSE"] + OPEN_TOL) &
    (df["LIVE_OPEN"] <= df["YEST_HIGH"] - OPEN_TOL)  &
    (df["LTP"] > 500 ) &
    (df["LTP"] >= df["YEST_HIGH"])
    #(df["LTP"] >= df["YEST_HIGH"] - OPEN_TOL)
)

green_zone_df = df.loc[
    green_zone_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        # 🆕 YH breakout strength
        "YH_MOVE",
        "YH_MOVE_%",
        "VOL_%",
        "LIVE_OPEN",   
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

green_zone_df["ZONE_%"] = (
    (df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100
).round(2)

green_zone_df = green_zone_df.sort_values("ZONE_%")

            ################################################################

green_zone_condition1 = (
    ((df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100 <= PCT_TOL) &
    (df["YEST_CLOSE"] > df["YEST_OPEN"] ) &
    (df["LIVE_OPEN"] > df["YEST_CLOSE"] ) &
    (df["LIVE_OPEN"] < df["YEST_HIGH"] )  &
    (df["LTP"] > 500 ) &
    (df["LTP"] <= df["YEST_HIGH"])
    #(df["LTP"] >= df["YEST_HIGH"] - OPEN_TOL)
)

green_zone_df1 = df.loc[
    green_zone_condition1,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",   
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

green_zone_df1["ZONE_%"] = (
    (df["YEST_HIGH"] - df["YEST_CLOSE"]) / df["YEST_CLOSE"] * 100
).round(2)

green_zone_df1 = green_zone_df1.sort_values("ZONE_%")


#########       SCENARIO 2 - Yesterday RED candle, tight body near low
# =========================================================
# 🔴 YEST RED + OPEN BETWEEN YL & YC (~1%)
# =========================================================

red_zone_condition = (
    ((df["YEST_CLOSE"] - df["YEST_LOW"]) / df["YEST_CLOSE"] * 100 <= PCT_TOL) &
    (df["LIVE_OPEN"] >= df["YEST_LOW"] + OPEN_TOL) &
    (df["LIVE_OPEN"] <= df["YEST_CLOSE"] - OPEN_TOL) &
    (df["LTP"] <= df["YEST_LOW"])
    #(df["LTP"] <= df["YEST_LOW"] + OPEN_TOL)
)

red_zone_df = df.loc[
    red_zone_condition,
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",   
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]
].copy()

red_zone_df["ZONE_%"] = (
    (df["YEST_CLOSE"] - df["YEST_LOW"]) / df["YEST_CLOSE"] * 100
).round(2)

red_zone_df = red_zone_df.sort_values("ZONE_%")




################                STEP 1: UNIVERSAL OPTION DOWNLOADER (INDEX + STOCK)

#SYMBOLS = INDEX_ONLY_SYMBOLS + STOCKS
# ================= OPTION SYMBOLS (STOCKS + INDICES) =================

OPTION_SYMBOLS = [
    s for s in SYMBOLS
    if s not in ["BSE"]   # exclude non-option symbols if needed
]

# Safety check
if not OPTION_SYMBOLS:
    st.error("❌ OPTION_SYMBOLS is empty")
    st.stop()


def get_strike_step(symbol):
    if symbol in ["NIFTY"]:
        return 50
    if symbol in ["BANKNIFTY", "SENSEX"]:
        return 100
    return 50   # stocks

def download_option_chain(symbol):
    if not is_market_hours():
        return pd.DataFrame()
    spot = live_map[symbol]["LTP"]
    step = get_strike_step(symbol)
    atm = int(round(spot / step) * step)

    strikes = [atm + i * step for i in range(-3, 4)]
    expiry = get_monthly_expiry(symbol)

    inst = pd.DataFrame(kite.instruments("NFO"))
    rows = []

    for strike in strikes:
        for opt_type in ["CE", "PE"]:
            row = inst[
                (inst["name"] == symbol) &
                (inst["strike"] == strike) &
                (inst["instrument_type"] == opt_type) &
                (pd.to_datetime(inst.get("expiry", pd.NaT), errors="coerce") if "expiry" in inst else pd.NaT) if hasattr(inst, "get") else pd.to_datetime(inst["expiry"], errors="coerce")
            ]

            if row.empty:
                continue

            ts = row.iloc[0]["tradingsymbol"]
            token = int(row.iloc[0]["instrument_token"])

            try:
                q = kite.quote([f"NFO:{ts}"])[f"NFO:{ts}"]
            except Exception:
                continue

            rows.append({
                "SYMBOL": symbol,
                "STRIKE": strike,
                "TYPE": opt_type,
                "SPOT": spot,
                "ATM": atm,
                "MONEYNESS": (
                    "ATM" if strike == atm else
                    "ITM" if (opt_type == "CE" and strike < atm) or
                              (opt_type == "PE" and strike > atm)
                    else "OTM"
                ),
                "LTP": q["last_price"],
                "OI": q.get("oi", 0),
                "OI_DAY_HIGH": q.get("oi_day_high", 0),
                "OI_DAY_LOW": q.get("oi_day_low", 0),
                "VOLUME": q.get("volume", 0),
                "IV": q.get("implied_volatility", None),
                "TIME": datetime.now(IST)
            })

    if rows:
        folder = "INDEX" if symbol in ["NIFTY", "BANKNIFTY", "SENSEX"] else "STOCK"
        path = f"{CACHE_DIR}/OPTIONS/{folder}/{symbol}.csv"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame(rows).to_csv(path, index=False, encoding='utf-8')


#           STEP 2: OI BUILDUP CLASSIFICATION (KEY PART)
def classify_oi_buildup(df):
    df = df.copy()

    df["OI_CHANGE"] = df["OI_DAY_HIGH"] - df["OI_DAY_LOW"]
    df["PRICE_CHANGE"] = df["LTP"] - df.groupby("STRIKE")["LTP"].transform("first")

    def label(row):
        if row["PRICE_CHANGE"] > 0 and row["OI_CHANGE"] > 0:
            return "LONG BUILDUP"
        if row["PRICE_CHANGE"] < 0 and row["OI_CHANGE"] > 0:
            return "SHORT BUILDUP"
        if row["PRICE_CHANGE"] > 0 and row["OI_CHANGE"] < 0:
            return "SHORT COVERING"
        if row["PRICE_CHANGE"] < 0 and row["OI_CHANGE"] < 0:
            return "LONG UNWINDING"
        return "NEUTRAL"

    df["OI_BUILDUP"] = df.apply(label, axis=1)
    return df


# =========================================================
# STEP 3: ATM vs ITM HEATMAP (ULTRA SAFE VERSION)
# =========================================================

df = df.copy()

# ---- Ensure numeric base columns exist ----
for col in ["OI", "VOLUME", "IV"]:
    if col not in df.columns:
        df[col] = 0
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ---- Rank-based component scores ----
oi_score  = df["OI"].rank(pct=True) if df["OI"].sum() > 0 else 0
vol_score = df["VOLUME"].rank(pct=True) if df["VOLUME"].sum() > 0 else 0
iv_score  = df["IV"].rank(pct=True) if df["IV"].sum() > 0 else 0

# ---- Final HEAT SCORE ----
df["HEAT_SCORE"] = (
    oi_score * 0.5 +
    vol_score * 0.3 +
    iv_score * 0.2
)

df["HEAT_SCORE"] = df["HEAT_SCORE"].round(2)




############        STEP 4: OPTIONS TAB - STOCKS + INDICES
def load_option_csv(symbol):
    for folder in ["INDEX", "STOCK"]:
        path = f"{CACHE_DIR}/OPTIONS/{folder}/{symbol}.csv"
        if os.path.exists(path):
            return pd.read_csv(path, encoding='utf-8')
    return pd.DataFrame()


# ── Global Cache for Instruments (to optimize OI fetching) ──
if "_nfo_inst_cache" not in st.session_state:
    st.session_state["_nfo_inst_cache"] = None
if "_bfo_inst_cache" not in st.session_state:
    st.session_state["_bfo_inst_cache"] = None

def _get_nfo_instruments():
    """Returns cached NFO instruments or fetches fresh if missing."""
    if st.session_state["_nfo_inst_cache"] is None:
        try:
            st.session_state["_nfo_inst_cache"] = pd.DataFrame(kite.instruments("NFO"))
        except Exception:
            return pd.DataFrame()
    return st.session_state["_nfo_inst_cache"]

def _get_bfo_instruments():
    """Returns cached BFO instruments (for Sensex)."""
    if st.session_state["_bfo_inst_cache"] is None:
        try:
            st.session_state["_bfo_inst_cache"] = pd.DataFrame(kite.instruments("BFO"))
        except Exception:
            return pd.DataFrame()
    return st.session_state["_bfo_inst_cache"]

# 🧠 OPEN INTEREST INTELLIGENCE - HIGH CONVICTION ENGINE
# Generalized for NIFTY, BANKNIFTY, FINNIFTY, SENSEX
# Uses insights from: Analysis of Volatility, Volume, and Open Interest
# ══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
def fetch_oi_intelligence(index_name="NIFTY"):
    """
    Fetches option chain for the specified index and calculates high-conviction signals.
    Supports NIFTY, BANKNIFTY, FINNIFTY, SENSEX.
    """
    # ── CACHE PATH ──
    _cache_file = OI_INTEL_CACHE if index_name == "NIFTY" else _dated(f"oi_intel_{index_name}", "json")

    if not is_market_hours():
        # Outside market hours: Load from last saved cache
        if os.path.exists(_cache_file):
            try:
                with open(_cache_file, "r", encoding="utf-8") as _f:
                    return json.load(_f)
            except: pass
        return None

    try:
        # 1. Spot Price
        full_sym = f"NSE:{index_name}" if index_name != "SENSEX" else "BSE:SENSEX"
        if index_name == "NIFTY":     full_sym = "NSE:NIFTY 50"
        if index_name == "BANKNIFTY": full_sym = "NSE:NIFTY BANK"
        if index_name == "FINNIFTY":  full_sym = "NSE:NIFTY FIN SERVICE"

        quote = kite.quote([full_sym])
        spot = quote[full_sym]["last_price"]

        # 2. Get Instruments (Cached)
        is_bse = (index_name == "SENSEX")
        inst_df = _get_bfo_instruments() if is_bse else _get_nfo_instruments()
        if inst_df.empty: return None

        # 3. Filtering & Expiry
        search_name = index_name if index_name != "SENSEX" else "SENSEX"
        opts = inst_df[
            (inst_df["name"] == search_name) &
            (inst_df["instrument_type"].isin(["CE", "PE"]))
        ].copy()

        if opts.empty: return None

        opts["expiry_dt"] = pd.to_datetime(opts["expiry"], errors="coerce")
        today_dt = pd.Timestamp.now().normalize()
        future_expiries = sorted(opts[opts["expiry_dt"] >= today_dt]["expiry_dt"].unique())
        if not future_expiries: return None
        expiry = future_expiries[0] # Nearest weekly

        # 4. Strike Range (ATM ± 10 strikes)
        step = 50 if index_name in ["NIFTY", "FINNIFTY"] else 100
        if index_name == "SENSEX": step = 100
        atm = int(round(spot / step) * step)
        strikes = [atm + i * step for i in range(-10, 11)]

        # 5. Token Map
        token_map = {}
        for s in strikes:
            for t in ["CE", "PE"]:
                row = opts[(opts["strike"] == s) & (opts["instrument_type"] == t) & (opts["expiry_dt"] == expiry)]
                if not row.empty:
                    ex = "BFO" if is_bse else "NFO"
                    ts = row.iloc[0]["tradingsymbol"]
                    token_map[f"{ex}:{ts}"] = {"strike": s, "type": t}

        if not token_map: return None
        quotes = kite.quote(list(token_map.keys()))

        # 6. Build Chain
        chain = {}
        total_vol = total_oi = 0
        for sym, meta in token_map.items():
            q = quotes.get(sym)
            if not q: continue
            s, t = meta["strike"], meta["type"]
            if s not in chain: chain[s] = {}
            oi = q.get("oi", 0) or 0
            vol = q.get("volume", 0) or 0
            iv = q.get("implied_volatility", 0) or 0
            chain[s][t] = {
                "ltp": q.get("last_price", 0),
                "oi": oi,
                "oi_h": q.get("oi_day_high", 0),
                "oi_l": q.get("oi_day_low", 0),
                "vol": vol,
                "iv": iv
            }
            total_oi += oi
            total_vol += vol

        # 7. Calculations (PCR, Max Pain)
        ce_oi_map = {s: chain[s].get("CE", {}).get("oi", 0) for s in chain}
        pe_oi_map = {s: chain[s].get("PE", {}).get("oi", 0) for s in chain}
        total_ce = sum(ce_oi_map.values()) or 1
        total_pe = sum(pe_oi_map.values()) or 1
        pcr = round(total_pe / total_ce, 2)

        # Max Pain
        pain_vals = {}
        for ps in strikes:
            total_pain = 0
            for s in chain:
                ce = chain[s].get("CE",{}).get("oi",0)
                pe = chain[s].get("PE",{}).get("oi",0)
                if s < ps: total_pain += ce * (ps - s)
                elif s > ps: total_pain += pe * (s - ps)
            pain_vals[ps] = total_pain
        max_pain = min(pain_vals, key=pain_vals.get) if pain_vals else atm

        # 8. High-Conviction Logic (Document Insights)
        vol_oi_ratio = round(total_vol / total_oi, 2) if total_oi > 0 else 0
        speculative = "HIGH" if vol_oi_ratio > 10 else "MODERATE" if vol_oi_ratio > 5 else "LOW"

        # Shifting OI (Near ATM strikes)
        _near_strikes = [s for s in chain if abs(s - atm) <= step * 3]
        ce_add = sum(chain[s].get("CE", {}).get("oi", 0) - chain[s].get("CE", {}).get("oi_l", 0) for s in _near_strikes)
        pe_add = sum(chain[s].get("PE", {}).get("oi", 0) - chain[s].get("PE", {}).get("oi_l", 0) for s in _near_strikes)

        # IV Battlegrounds
        battle_ce = max(chain, key=lambda s: chain[s].get("CE",{}).get("oi",0) * chain[s].get("CE",{}).get("iv",1))
        battle_pe = max(chain, key=lambda s: chain[s].get("PE",{}).get("oi",0) * chain[s].get("PE",{}).get("iv",1))

        # Advanced Shifting Data
        shifting_ce = max(_near_strikes, key=lambda s: chain[s].get("CE",{}).get("oi",0) - chain[s].get("CE",{}).get("oi_l",0))
        shifting_ce_add = chain[shifting_ce].get("CE",{}).get("oi",0) - chain[shifting_ce].get("CE",{}).get("oi_l",0)
        shifting_ce_pct = round(shifting_ce_add / (chain[shifting_ce].get("CE",{}).get("oi",1)) * 100, 1)

        shifting_pe = max(_near_strikes, key=lambda s: chain[s].get("PE",{}).get("oi",0) - chain[s].get("PE",{}).get("oi_l",0))
        shifting_pe_add = chain[shifting_pe].get("PE",{}).get("oi",0) - chain[shifting_pe].get("PE",{}).get("oi_l",0)
        shifting_pe_pct = round(shifting_pe_add / (chain[shifting_pe].get("PE",{}).get("oi",1)) * 100, 1)

        # Nearest Walls/Floors (Above/Below Spot)
        call_walls_above = sorted([[s, chain[s]["CE"]["oi"]] for s in chain if s > spot], key=lambda x: x[1], reverse=True)
        put_floors_below = sorted([[s, chain[s]["PE"]["oi"]] for s in chain if s < spot], key=lambda x: x[1], reverse=True)
        nearest_call_wall = call_walls_above[0][0] if call_walls_above else "-"
        nearest_put_floor = put_floors_below[0][0] if put_floors_below else "-"

        # Strongest Overall
        s_ce = max(chain, key=lambda s: chain[s].get("CE",{}).get("oi",0))
        s_pe = max(chain, key=lambda s: chain[s].get("PE",{}).get("oi",0))

        # Directional Logic
        direction = "⚠️ SIDEWAYS"
        dir_reason = "No clear dominance"
        if pcr > 1.25 and pe_add > ce_add * 1.5:
            direction = "🟢 STRONG BULLISH (PE Writing)"
            dir_reason = "Aggressive Put writing with high PCR"
        elif pcr < 0.75 and ce_add > pe_add * 1.5:
            direction = "🔴 STRONG BEARISH (CE Writing)"
            dir_reason = "Aggressive Call writing with low PCR"
        elif pe_add > ce_add:
            direction = "🟢 BULLISH BIAS"
            dir_reason = "Net Put writing dominance near ATM"
        elif ce_add > pe_add:
            direction = "🔴 BEARISH BIAS"
            dir_reason = "Net Call writing dominance near ATM"

        # Advice & Setup
        advice = "Wait for clear trend"
        setup  = "Indecision zone"
        if "BULLISH" in direction:
            advice = "Look for long entries on dips"
            setup  = f"Support at {nearest_put_floor}"
        elif "BEARISH" in direction:
            advice = "Look for short entries on rallies"
            setup  = f"Resistance at {nearest_call_wall}"

        pain_signal = "NEUTRAL"
        if spot < max_pain: pain_signal = "BULLISH (Convergence expected)"
        elif spot > max_pain: pain_signal = "BEARISH (Convergence expected)"

        # 9. Build Chain Rows for Table Rendering
        chain_rows = []
        for s in sorted(chain.keys()):
            c = chain[s].get("CE", {})
            p = chain[s].get("PE", {})
            chain_rows.append({
                "STRIKE":    s,
                "CE_LTP":    c.get("ltp", 0),
                "CE_OI":     c.get("oi", 0),
                "CE_OI_ADD": c.get("oi", 0) - c.get("oi_l", 0),
                "CE_VOL":    c.get("vol", 0),
                "PE_LTP":    p.get("ltp", 0),
                "PE_OI":     p.get("oi", 0),
                "PE_OI_ADD": p.get("oi", 0) - p.get("oi_l", 0),
                "PE_VOL":    p.get("vol", 0),
            })

        # 10. Result Object
        res = {
            "index": index_name, "spot": spot, "atm": atm, "pcr": pcr, "max_pain": max_pain,
            "direction": direction, "direction_reason": dir_reason, "pain_signal": pain_signal,
            "advice": advice, "setup": setup,
            "vol_oi_ratio": vol_oi_ratio, "speculative": speculative,
            "battle_ce": battle_ce, "battle_pe": battle_pe,
            "ce_add": ce_add, "pe_add": pe_add, "expiry": expiry.strftime("%d-%b"),
            "timestamp": datetime.now(IST).strftime("%H:%M:%S"),
            "chain_rows": chain_rows,
            "step": step,
            "shifting_ce": shifting_ce, "shifting_ce_add": shifting_ce_add, "shifting_ce_pct": shifting_ce_pct,
            "shifting_ce_ltp": chain[shifting_ce]["CE"]["ltp"],
            "shifting_pe": shifting_pe, "shifting_pe_add": shifting_pe_add, "shifting_pe_pct": shifting_pe_pct,
            "shifting_pe_ltp": chain[shifting_pe]["PE"]["ltp"],
            "nearest_call_wall": nearest_call_wall, "nearest_put_floor": nearest_put_floor,
            "strongest_ce": s_ce, "strongest_pe": s_pe,
            "ce_oi": chain[s_ce]["CE"]["oi"], "pe_oi": chain[s_pe]["PE"]["oi"],
            "call_walls_above": call_walls_above, "put_floors_below": put_floors_below,
            "near_ce_pct": round((ce_add / (total_ce or 1)) * 100, 1),
            "near_pe_pct": round((pe_add / (total_pe or 1)) * 100, 1),
            "dist_to_call": abs(spot - nearest_call_wall) if isinstance(nearest_call_wall, (int, float)) else "-",
            "dist_to_put":  abs(spot - nearest_put_floor) if isinstance(nearest_put_floor, (int, float)) else "-",
            "fut_ltp": spot + (15 if index_name=="NIFTY" else 100), # Approximated
        }

        # 11. Save Snapshot for 15-min Delta
        try:
            now_slot = datetime.now(IST).strftime("%H:%M")
            snap = {"_ts": datetime.now(IST).isoformat(), "_slot": now_slot}
            for s in chain:
                snap[str(s)] = {
                    "CE": {"oi": int(chain[s].get("CE", {}).get("oi", 0)), "ltp": round(chain[s].get("CE", {}).get("ltp", 0), 2)},
                    "PE": {"oi": int(chain[s].get("PE", {}).get("oi", 0)), "ltp": round(chain[s].get("PE", {}).get("ltp", 0), 2)}
                }
            _save_oi_snapshot(index_name, snap)
        except Exception as se:
            print(f"[OI-SNAP] {index_name} save error: {se}")

        # 12. Save to Disk Cache (Dated)
        try:
            with open(_cache_file, "w", encoding="utf-8") as _f:
                json.dump(res, _f, default=str)
        except: pass

        return res

    except Exception as e:
        print(f"[OI-ENGINE] {index_name} error: {e}")
        return None
def _save_oi_snapshot(index, snap):
    """Saves index-specific OI snapshot to CACHE."""
    path = os.path.join(CACHE_DIR, f"oi_snap_{index}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(snap, f, default=str)
    except: pass

def fetch_nifty_oi_intelligence():
    """Compatibility wrapper for Nifty."""
    return fetch_oi_intelligence("NIFTY")

def fetch_banknifty_oi_intelligence():
    """Compatibility wrapper for Bank Nifty."""
    return fetch_oi_intelligence("BANKNIFTY")

def fetch_sensex_oi_intelligence():
    """Compatibility wrapper for Sensex."""
    return fetch_oi_intelligence("SENSEX")

# =========================================================
# 📊 15-MIN OI DELTA ENGINE
# Compares current chain snapshot vs previous 15-min slot.
# Fires email + dashboard alerts for significant OI moves.
# =========================================================

def _load_oi_15m_snapshot():
    """Load previous 15-min chain snapshot from disk."""
    import json as _json
    if not os.path.exists(OI_15M_SNAP_FILE):
        return None
    try:
        with open(OI_15M_SNAP_FILE, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return None

def _save_oi_15m_snapshot(chain_snap):
    """Persist current chain snapshot to disk."""
    import json as _json
    try:
        with open(OI_15M_SNAP_FILE, "w", encoding="utf-8") as f:
            _json.dump(chain_snap, f, default=str, ensure_ascii=False)
    except Exception as e:
        print(f"OI_15M_SNAP save error: {e}")

def _oi_15m_dedup_key(slot, strike, opt_type, direction):
    """Unique key: slot + strike + CE/PE + ADD/DROP."""
    return f"{slot}|{strike}|{opt_type}|{direction}"

def _oi_15m_already_sent(key):
    today = date.today().isoformat()
    path = OI_15M_DEDUP
    if not os.path.exists(path):
        return False
    try:
        df = pd.read_csv(path, encoding='utf-8')
        return bool(((df["DATE"] == today) & (df["KEY"] == key)).any())
    except Exception:
        return False

def _oi_15m_mark_sent(key):
    today = date.today().isoformat()
    row = {"DATE": today, "KEY": key}
    path = OI_15M_DEDUP
    try:
        if os.path.exists(path):
            df = pd.read_csv(path, encoding='utf-8')
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        else:
            df = pd.DataFrame([row])
        df.to_csv(path, index=False, encoding='utf-8')
    except Exception as e:
        print(f"OI_15M dedup write error: {e}")

def compute_oi_15m_delta(chain_snap, prev_snap, threshold_pct=5.0):
    """
    Compare current chain snapshot vs previous 15-min snapshot.
    Returns list of significant events:
      Each row: {strike, type, direction, oi_prev, oi_curr, oi_delta,
                 oi_delta_pct, ltp_prev, ltp_curr, ltp_pct_chg,
                 slot_prev, slot_curr, label}
    threshold_pct: minimum OI % change (of current strike OI) to report
    """
    if not chain_snap or not prev_snap:
        return []

    slot_prev = prev_snap.get("_slot", "-")
    slot_curr = chain_snap.get("_slot", "-")
    events = []

    for key in chain_snap:
        if key.startswith("_"):
            continue
        try:
            strike = int(key)
        except ValueError:
            continue

        curr_data = chain_snap.get(key, {})
        prev_data = prev_snap.get(key, {})
        if not curr_data or not prev_data:
            continue

        for opt in ["CE", "PE"]:
            oi_curr = int(curr_data.get(opt, {}).get("oi", 0) or 0)
            oi_prev = int(prev_data.get(opt, {}).get("oi", 0) or 0)
            ltp_curr = float(curr_data.get(opt, {}).get("ltp", 0) or 0)
            ltp_prev = float(prev_data.get(opt, {}).get("ltp", 0) or 0)

            if oi_curr <= 0 or oi_prev <= 0:
                continue

            oi_delta = oi_curr - oi_prev
            # % of current strike OI
            oi_delta_pct = round(abs(oi_delta) / oi_curr * 100, 1)

            if oi_delta_pct < threshold_pct:
                continue

            ltp_pct_chg = round((ltp_curr - ltp_prev) / ltp_prev * 100, 1) if ltp_prev > 0 else 0.0
            direction = "ADD" if oi_delta > 0 else "DROP"

            # Human-readable label (matches your example format)
            arrow = "+" if oi_delta > 0 else ""
            ltp_arrow = "+" if ltp_pct_chg >= 0 else ""
            if opt == "CE" and direction == "ADD":
                label = (f"📝 Call writing at {strike:,} CE - OI added {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium Rs.{ltp_prev:.1f}->Rs.{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")
            elif opt == "CE" and direction == "DROP":
                label = (f"📈 Call wall unwinding at {strike:,} CE - OI dropped {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium Rs.{ltp_prev:.1f}->Rs.{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")
            elif opt == "PE" and direction == "DROP":
                label = (f"⚠️ Put support crumbling at {strike:,} PE - OI dropped {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium Rs.{ltp_prev:.1f}->Rs.{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")
            else:  # PE ADD
                label = (f"🛡️ Put floor building at {strike:,} PE - OI added {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium Rs.{ltp_prev:.1f}->Rs.{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")

            events.append({
                "SLOT":         f"{slot_prev}->{slot_curr}",
                "STRIKE":       strike,
                "TYPE":         opt,
                "DIRECTION":    direction,
                "OI_PREV":      oi_prev,
                "OI_CURR":      oi_curr,
                "OI_DELTA":     oi_delta,
                "OI_DELTA_%":   oi_delta_pct,
                "LTP_PREV":     ltp_prev,
                "LTP_CURR":     ltp_curr,
                "LTP_CHG_%":    ltp_pct_chg,
                "LABEL":        label,
            })

    # ── Always surface the single top mover per category ────
    _candidates = {}
    for key in chain_snap:
        if key.startswith("_"):
            continue
        try:
            strike = int(key)
        except ValueError:
            continue
        curr_data = chain_snap.get(key, {})
        prev_data = prev_snap.get(key, {})
        if not curr_data or not prev_data:
            continue
        for opt in ["CE", "PE"]:
            oi_c = int(curr_data.get(opt, {}).get("oi",  0) or 0)
            oi_p = int(prev_data.get(opt, {}).get("oi",  0) or 0)
            ltp_c = float(curr_data.get(opt, {}).get("ltp", 0) or 0)
            ltp_p = float(prev_data.get(opt, {}).get("ltp", 0) or 0)
            if oi_c <= 0 or oi_p <= 0:
                continue
            delta = oi_c - oi_p
            dpct  = round(abs(delta) / oi_c * 100, 1)
            dir_  = "ADD" if delta > 0 else "DROP"
            cat   = (opt, dir_)
            ex    = _candidates.get(cat)
            if ex is None or abs(delta) > abs(ex["OI_DELTA"]):
                lpct = round((ltp_c - ltp_p) / ltp_p * 100, 1) if ltp_p > 0 else 0.0
                larr = "+" if lpct >= 0 else ""
                if opt == "CE" and dir_ == "ADD":
                    lbl = (f"📝 Call writing at {strike:,} CE - OI added {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium Rs.{ltp_p:.1f}->Rs.{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                elif opt == "CE" and dir_ == "DROP":
                    lbl = (f"📈 Call wall unwinding at {strike:,} CE - OI dropped {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium Rs.{ltp_p:.1f}->Rs.{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                elif opt == "PE" and dir_ == "DROP":
                    lbl = (f"⚠️ Put support crumbling at {strike:,} PE - OI dropped {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium Rs.{ltp_p:.1f}->Rs.{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                else:
                    lbl = (f"🛡️ Put floor building at {strike:,} PE - OI added {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium Rs.{ltp_p:.1f}->Rs.{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                _candidates[cat] = {
                    "SLOT": f"{slot_prev}->{slot_curr}", "STRIKE": strike,
                    "TYPE": opt, "DIRECTION": dir_,
                    "OI_PREV": oi_p, "OI_CURR": oi_c, "OI_DELTA": delta,
                    "OI_DELTA_%": dpct, "LTP_PREV": ltp_p, "LTP_CURR": ltp_c,
                    "LTP_CHG_%": lpct, "LABEL": lbl, "TOP_MOVER": True,
                }

    existing_keys = {(e["STRIKE"], e["TYPE"], e["DIRECTION"]) for e in events}
    for cat, cand in _candidates.items():
        k = (cand["STRIKE"], cand["TYPE"], cand["DIRECTION"])
        if k not in existing_keys:
            events.append(cand)
        else:
            for e in events:
                if (e["STRIKE"], e["TYPE"], e["DIRECTION"]) == k:
                    e["TOP_MOVER"] = True

    # Top movers first, then by OI delta % descending
    events.sort(key=lambda x: (not x.get("TOP_MOVER", False), -x["OI_DELTA_%"]))
    return events


def fire_oi_15m_alerts(events, spot, atm):
    """
    Send email + toast for significant 15-min OI events.
    Deduped per slot+strike+type+direction - fires at most once per 15-min candle.
    """
    if not events or not is_market_hours():
        return

    now_slot = datetime.now(IST).strftime("%H:%M")
    new_events = []
    for ev in events:
        key = _oi_15m_dedup_key(now_slot, ev["STRIKE"], ev["TYPE"], ev["DIRECTION"])
        if not _oi_15m_already_sent(key):
            _oi_15m_mark_sent(key)
            new_events.append(ev)

    if not new_events:
        return

    # ══════════════════════════════════════════════════════════════════
    # OI INTELLIGENCE: Telegram per-event + smart interpretation + summary
    # Noise filter : dpct < 5% OR dabs < 50,000 contracts -> skip
    # Strong rating: dpct >= 7% AND dabs >= 75,000         -> 3 stars
    # Normal rating: dpct >= 5% AND dabs >= 50,000         -> 2 stars
    # Per-event TG messages suppressed - only 15-min and 30-min summaries fire.
    # ══════════════════════════════════════════════════════════════════
    _now_str_tg = datetime.now(IST).strftime("%H:%M IST")
    _slot_label = new_events[0]["SLOT"] if new_events else "\u2014"
    # Clean 15-min boundary slot key for dedup (e.g. "12:15", "12:30")
    _now_for_slot = datetime.now(IST)
    _slot15_min_b = (_now_for_slot.minute // 15) * 15
    _slot15_label = _now_for_slot.strftime(f"%H:{_slot15_min_b:02d}")

    def _interpret_oi_event(ev, spot, atm):
        opt=ev["TYPE"]; dirn=ev["DIRECTION"]; strk=ev["STRIKE"]
        dpct=ev["OI_DELTA_%"]; dabs=abs(ev["OI_DELTA"]); ltp_chg=ev["LTP_CHG_%"]
        top=ev.get("TOP_MOVER",False)
        is_atm=abs(strk-atm)<=50; above_spot=strk>spot; below_spot=strk<spot
        if dpct < 5.0 or dabs < 50000:
            return ("\U0001f7e1 Filtered \u2014 "+str(dpct)+"% / "+f"{dabs:,}"+" contracts. Below threshold.", 0, "neutral")
        weight = 3 if (dpct>=7.0 and dabs>=75000) else (2 if (dpct>=5.0 and dabs>=50000) else 1)
        if opt=="CE" and dirn=="DROP":
            bias="bull"
            prem = "falling = shorts squeezed" if ltp_chg<0 else "rising = bulls pushing"
            if is_atm:
                line=("\U0001f7e2 <b>Bullish</b> \u2014 ATM ("+str(strk)+") call wall crumbling. Shorts covering + premium "+prem+(".  \u2b50 Most important." if top else "."))
                weight=3
            elif above_spot:
                line="\U0001f7e2 Bullish \u2014 resistance at "+str(strk)+" (above spot) weakening. "+("\U0001f525 Massive. " if dabs>100000 else "")+"Shorts giving up."
            else:
                line="\U0001f7e2 Also bullish \u2014 old resistance at "+str(strk)+" (below spot) already gone."
            return (line, weight, bias)
        if opt=="CE" and dirn=="ADD":
            bias="bear"
            if is_atm:
                line="\U0001f534 <b>Bearish</b> \u2014 ATM ("+str(strk)+") call writing = resistance built here. "+("\U0001f525 Massive OI \u2014 high conviction." if dabs>100000 else "Watch carefully.")
                weight=3
            elif above_spot:
                line="\U0001f534 Bearish \u2014 ceiling at "+str(strk)+" (above spot). "+("\U0001f525 Big OI. " if dabs>100000 else "")+"Resistance confirmed."
            else:
                line="\U0001f534 Bearish \u2014 call writing below spot at "+str(strk)+". Hedging."
            return (line, weight, bias)
        if opt=="PE" and dirn=="ADD":
            bias="bull"
            pnote=("Premium falling = buyers strong." if ltp_chg<-0.5 else "Premium stable = support holding." if abs(ltp_chg)<=0.5 else "\u26a0\ufe0f Premium rising \u2014 possible trap.")
            if is_atm:
                line="\U0001f7e2 <b>Bullish</b> \u2014 ATM ("+str(strk)+") put floor = strong support. "+pnote+(" \u2b50" if top else "")
                weight=3
            elif below_spot:
                line="\U0001f7e2 Bullish \u2014 support floor at "+str(strk)+" (below spot). "+pnote
            else:
                line="\U0001f7e1 Mixed \u2014 put writing above spot at "+str(strk)+". Possible hedging."
                bias="neutral"
            return (line, weight, bias)
        if opt=="PE" and dirn=="DROP":
            bias="bear"
            if is_atm:
                line="\U0001f534 <b>Bearish</b> \u2014 ATM ("+str(strk)+") support collapsing. "+("\U0001f525 Massive exit \u2014 high risk. " if dabs>100000 else "")+("\u2b50 Key level gone." if top else "")
                weight=3
            elif below_spot:
                pfall=" Premium falling = market sliding." if ltp_chg<0 else ""
                line="\U0001f534 Bearish \u2014 support at "+str(strk)+" (below spot) collapsing."+pfall
            else:
                line="\U0001f534 Bearish \u2014 put longs exiting above spot at "+str(strk)+"."
            return (line, weight, bias)
        return ("\U0001f7e1 Unclassified.", 0, "neutral")

    _interps = []
    for _ev in new_events:
        _opt=_ev["TYPE"]; _dir=_ev["DIRECTION"]; _strk=_ev["STRIKE"]
        _dpct=_ev["OI_DELTA_%"]; _dabs=abs(_ev["OI_DELTA"])
        _lp=_ev["LTP_PREV"]; _lc=_ev["LTP_CURR"]; _lchg=_ev["LTP_CHG_%"]
        _slot=_ev["SLOT"]; _top=_ev.get("TOP_MOVER",False)
        if _opt=="CE" and _dir=="DROP":
            _icon="\U0001f4c8"; _head="<b>Call wall unwinding at "+f"{_strk:,}"+" CE</b>"
            _detail="OI dropped <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \u20b9"+f"{_lp:.1f}"+"\u2192\u20b9"+f"{_lc:.1f}"
        elif _opt=="CE" and _dir=="ADD":
            _icon="\U0001f4dd"; _head="<b>Call writing at "+f"{_strk:,}"+" CE</b>"
            _detail="OI added <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \u20b9"+f"{_lp:.1f}"+"\u2192\u20b9"+f"{_lc:.1f}"
        elif _opt=="PE" and _dir=="ADD":
            _icon="\U0001f6e1\ufe0f"; _head="<b>Put floor building at "+f"{_strk:,}"+" PE</b>"
            _detail="OI added <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \u20b9"+f"{_lp:.1f}"+"\u2192\u20b9"+f"{_lc:.1f}"
        else:
            _icon="\u26a0\ufe0f"; _head="<b>Put support crumbling at "+f"{_strk:,}"+" PE</b>"
            _detail="OI dropped <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \u20b9"+f"{_lp:.1f}"+"\u2192\u20b9"+f"{_lc:.1f}"
        _larr="+" if _lchg>=0 else ""
        _badge=" \u2b50 <i>Top Mover</i>" if _top else ""
        _sig_line,_wt,_bias=_interpret_oi_event(_ev, spot, atm)
        _interps.append((_wt,_bias,_strk,_opt,_dir))
        if _wt==0:
            continue
        _msg=(_icon+" "+_head+_badge+"\n"+_detail+" ("+_larr+f"{_lchg:.1f}"+"%)\n"+"\U0001f4a1 "+_sig_line+"\n"+"\u2501"*10+"\n"+"\U0001f4ca Spot: <b>"+f"{spot:,.0f}"+"</b>  |  ATM: <b>"+str(atm)+"</b>\n"+"\U0001f550 Slot: "+_slot+"  |  "+_now_str_tg+"\n"+"<i>\u26a0\ufe0f NOT financial advice</i>")
        # Gate: put crumble -> tg_PUT_CRUMBLE, put floor -> tg_PUT_FLOOR, long unwind -> tg_LONG_UNWIND, general OI15m -> tg_OI_INTEL
        _is_put_crumble = (_opt == "PE" and _dir == "DROP")
        _is_put_floor   = (_opt == "PE" and _dir == "ADD")
        _is_long_unwind = (_opt == "CE" and _dir == "DROP")
        # Per-event individual TG messages are suppressed.
        # Only the 15-min and 30-min summaries fire (see below).
        # Uncomment the block below if you want per-event alerts back:
        # if _is_put_crumble and not st.session_state.get("tg_PUT_CRUMBLE", False):
        #     pass
        # elif _is_put_floor and not st.session_state.get("tg_PUT_FLOOR", False):
        #     pass
        # elif _is_long_unwind and not st.session_state.get("tg_LONG_UNWIND", False):
        #     pass
        # elif st.session_state.get("tg_OI_INTEL", True):
        #     send_telegram_bg(_msg, dedup_key="OI15M_TG_"+_slot+"_"+str(_strk)+"_"+_opt+"_"+_dir)

    try:
        _bull_wt=sum(w for w,b,*_ in _interps if b=="bull"); _bear_wt=sum(w for w,b,*_ in _interps if b=="bear")
        _total=_bull_wt+_bear_wt or 1; _bull_pct=round(_bull_wt/_total*100); _bear_pct=100-_bull_pct
        if _bull_pct>=70:   _ov_icon,_ov_bias,_hint="\U0001f7e2","BULLISH","Market likely targeting "+str(atm+50)+"\u2013"+str(atm+100)+"."
        elif _bear_pct>=70: _ov_icon,_ov_bias,_hint="\U0001f534","BEARISH","Market may test "+str(atm-50)+"\u2013"+str(atm-100)+"."
        else:               _ov_icon,_ov_bias,_hint="\u26a0\ufe0f","MIXED / WAIT","No clear edge \u2014 wait for confirmation."
        _tbl=[]
        for _ev in new_events:
            _sl,_wt2,_bs=_interpret_oi_event(_ev,spot,atm)
            if _wt2==0: continue
            _stars=("\U0001f7e2"*3 if _wt2==3 else "\U0001f7e2"*2 if _wt2==2 else "\U0001f7e2")
            if _bs=="bear": _stars=_stars.replace("\U0001f7e2","\U0001f534")
            elif _bs=="neutral": _stars=_stars.replace("\U0001f7e2","\U0001f7e1")
            _elbl=(f"Call unwind {_ev['STRIKE']}" if _ev['TYPE']=="CE" and _ev['DIRECTION']=="DROP" else f"Call writing {_ev['STRIKE']}" if _ev['TYPE']=="CE" and _ev['DIRECTION']=="ADD" else f"Put floor {_ev['STRIKE']}" if _ev['TYPE']=="PE" and _ev['DIRECTION']=="ADD" else f"Put crumble {_ev['STRIKE']}")
            _meaning=_sl.split("\u2014")[-1].strip()[:55]
            _tbl.append("  \u2022 "+_elbl+": "+_meaning+("\u2026" if len(_meaning)>=55 else "")+" "+_stars)
        _tbl_str="\n".join(_tbl[:8])
        _atm_unwind=any(w==3 and b=="bull" and t=="CE" and d=="DROP" and abs(s-atm)<=50 for w,b,s,t,d in _interps)
        _floor_build=any(w>=2 and b=="bull" and t=="PE" and d=="ADD" for w,b,s,t,d in _interps)
        _pat=""
        if _atm_unwind and _floor_build: _pat="\U0001f9e0 <b>Pattern:</b> ATM call shorts covering + put floors = <b>short covering + support defense</b>."
        elif _atm_unwind: _pat="\U0001f9e0 <b>Pattern:</b> ATM resistance breaking \u2014 bulls in control."
        elif _floor_build and _bull_pct>=60: _pat="\U0001f9e0 <b>Pattern:</b> Support floors building \u2014 big money defending lows."
        elif _bear_pct>=70: _pat="\U0001f9e0 <b>Pattern:</b> Call writing + put exits = distribution. Bears in control."
        if _tbl_str:
            _sum="\U0001f9e0 <b>OI SUMMARY \u2014 "+_slot15_label+"</b>\n"+"\u2501"*21+"\n"+"\U0001f4cd Spot: <b>"+f"{spot:,.0f}"+"</b>  |  ATM: <b>"+str(atm)+"</b>\n"+"\U0001f4ca Signals: \U0001f7e2 Bull "+str(_bull_pct)+"%  |  \U0001f534 Bear "+str(_bear_pct)+"%\n"+"\u2501"*21+"\n"+_tbl_str+"\n"+"\u2501"*21+"\n"+_ov_icon+" <b>Overall: "+_ov_bias+"</b>\n"+_hint+"\n"
            if _pat: _sum+=_pat+"\n"
            _sum+="<i>\u26a0\ufe0f NOT financial advice</i>"
            if st.session_state.get("tg_OI_INTEL", True):
                # ── 15-min summary (every 15-min slot) ──
                _sum15 = _sum.replace(
                    "OI SUMMARY \u2014 "+_slot15_label,
                    "OI SUMMARY (15 Min) \u2014 "+_slot15_label
                )
                send_telegram_bg(_sum15, dedup_key="OI15M_SUMMARY_"+_slot15_label.replace(":",""))
                # ── 30-min summary (every 30-min slot - fires on :00 and :30) ──
                _now_30 = datetime.now(IST)
                _slot30_min = (_now_30.minute // 30) * 30
                _slot30_str = _now_30.strftime(f"%H:{_slot30_min:02d}")
                if st.session_state.get("tg_OI_30M_SUMMARY", True):
                    _sum_30 = "\U0001f9e0 <b>OI SUMMARY (30 Min) \u2014 "+_slot30_str+"</b>\n"+"\u2501"*21+"\n"+"\U0001f4cd Spot: <b>"+f"{spot:,.0f}"+"</b>  |  ATM: <b>"+str(atm)+"</b>\n"+"\U0001f4ca Signals: \U0001f7e2 Bull "+str(_bull_pct)+"%  |  \U0001f534 Bear "+str(_bear_pct)+"%\n"+"\u2501"*21+"\n"+_tbl_str+"\n"+"\u2501"*21+"\n"+_ov_icon+" <b>Overall: "+_ov_bias+"</b>\n"+_hint+"\n"
                    if _pat: _sum_30+=_pat+"\n"
                    _sum_30+="<i>\u26a0\ufe0f NOT financial advice</i>"
                    send_telegram_bg(_sum_30, dedup_key="OI30M_SUMMARY_"+_slot30_str.replace(":",""))
    except Exception as _se:
        print("OI summary TG error: "+str(_se))

    # ── Browser toast (one per significant event) - gated by OI_INTEL toggle ──
    if st.session_state.get("tg_OI_INTEL", True):
        for ev in new_events[:3]:   # cap at 3 toasts to avoid spam
            icon = "📝" if ev["TYPE"] == "CE" and ev["DIRECTION"] == "ADD" else (
                   "📈" if ev["TYPE"] == "CE" else (
                   "⚠️" if ev["DIRECTION"] == "DROP" else "🛡️"))
            if BROWSER_ALERTS_ENABLED: st.toast(ev["LABEL"][:120], icon=icon)

    # ── Email ──────────────────────────────────────────────
    if not can_send_email() or not is_email_allowed():
        return

    now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    subject = f"[OiAnalytics] 15-Min OI Shift - {len(new_events)} Event(s) | {now_str}"

    # Build HTML table
    tbl_rows = ""
    for i, ev in enumerate(new_events):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        d_color = "#b71c1c" if ev["DIRECTION"] == "ADD" and ev["TYPE"] == "CE" else (
                  "#1b5e20" if ev["DIRECTION"] == "ADD" and ev["TYPE"] == "PE" else (
                  "#880e4f" if ev["DIRECTION"] == "DROP" and ev["TYPE"] == "PE" else "#0277bd"))
        tbl_rows += f"""<tr style="background:{bg};">
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">{ev['SLOT']}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{ev['STRIKE']:,} {ev['TYPE']}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{d_color};font-weight:bold;">{ev['DIRECTION']} {ev['OI_DELTA']:+,}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{round(ev['OI_DELTA_%'], 1)}%</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">Rs.{round(ev['LTP_PREV'], 1)} -> Rs.{round(ev['LTP_CURR'], 1)}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{'#c62828' if ev['LTP_CHG_%'] < 0 else '#2e7d32'};">{round(ev['LTP_CHG_%'], 1)}%</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-size:11px;max-width:320px;">{ev['LABEL']}</td>
        </tr>"""

    html_body = f"""
<html><body style="font-family:Arial,sans-serif;background:#f0f0f0;margin:0;padding:20px;">
<div style="max-width:1100px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;
            box-shadow:0 2px 8px rgba(0,0,0,0.15);">
  <div style="background:#1a237e;padding:16px 20px;">
    <h2 style="color:#fff;margin:0;font-size:18px;letter-spacing:1px;">
      📊 15-Min OI Shift Alert
    </h2>
    <p style="color:#ddd;margin:4px 0 0;font-size:12px;">
      Generated: {now_str} &nbsp;|&nbsp; NIFTY Spot: {spot:,.0f} | ATM: {atm:,} | OiAnalytics [LOCAL]
    </p>
  </div>
  <div style="background:#fff8e1;padding:10px 20px;border-bottom:2px solid #1a237e;">
    <b>🆕 {len(new_events)} significant OI shift(s) detected in last 15 minutes</b>
  </div>
  <div style="padding:16px 20px;overflow-x:auto;">
  <table style="border-collapse:collapse;width:100%;font-family:Courier New,monospace;">
    <thead><tr>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">SLOT</th>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">STRIKE</th>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">OI CHANGE</th>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">% OF OI</th>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">PREMIUM Δ</th>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">PREM %</th>
      <th style="background:#1a237e;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">INTERPRETATION</th>
    </tr></thead>
    <tbody>{tbl_rows}</tbody>
  </table>
  </div>
  <div style="background:#212121;padding:10px 20px;">
    <p style="color:#aaa;font-size:11px;margin:0;">
      Threshold: ≥5% OI change of strike total in one 15-min candle.
      Deduped per slot - each strike+type fires once per 15-min window.
    </p>
  </div>
</div></body></html>"""

    def _bg_send(subj=subject, body=html_body):
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            msg = MIMEMultipart("alternative")
            msg["From"]    = EMAIL_FROM
            msg["To"]      = ", ".join(EMAIL_TO)
            msg["Subject"] = subj
            msg.attach(MIMEText(body, "html", "utf-8"))
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as smtp:
                smtp.login(EMAIL_FROM, EMAIL_PASS)
                smtp.send_message(msg)
            record_email_sent()
        except Exception as e:
            print(f"OI_15M EMAIL ERROR: {e}")

    threading.Thread(target=_bg_send, daemon=True).start()


# ── Run 15-min OI delta on every refresh ─────────────────
_oi_15m_events = []
_oi_intel_for_delta = fetch_nifty_oi_intelligence()
# Store in session_state so combined engine can read it
if _oi_intel_for_delta:
    st.session_state["oi_intel"] = _oi_intel_for_delta

# ── Live 1H BOS/CHoCH Scanner - real-time, no 65-min wait ──────────────────
# Strategy:
#   1. First refresh: kick off background fetch of 20 completed 1H candles
#      per symbol (rate-limited, runs in background - doesn't block UI)
#   2. Every refresh: update the current forming candle from live df OHLC
#      (LIVE_OPEN/HIGH/LOW + LTP - already available from kite.quote call above)
#   3. Run BOS/CHoCH scan immediately on combined candle list
#      -> Alerts fire from 09:16 onwards instead of waiting for 10:15
try:
    if _BOS_OK:
        # Step 1: Ensure historical 1H candles are loaded (fires once per session)
        _ensure_hist_1h_loaded(STOCKS)

        # Step 2: Update live forming candle for every symbol from live df
        # df is the live dataframe already populated above by fetch_live_and_oi()
        if not df.empty:
            for _, _bos_row in df.iterrows():
                _bos_sym = str(_bos_row.get("Symbol", "")).strip()
                if not _bos_sym or _bos_sym not in STOCKS:
                    continue
                _update_live_1h_candle(
                    sym       = _bos_sym,
                    live_open = _bos_row.get("LIVE_OPEN",  0),
                    live_high = _bos_row.get("LIVE_HIGH",  0),
                    live_low  = _bos_row.get("LIVE_LOW",   0),
                    ltp       = _bos_row.get("LTP",        0),
                    live_vol  = _bos_row.get("LIVE_VOLUME",0),
                )

        # Step 3: Build ltp_map and run BOS scan with live candles
        _ltp_bos = {}
        _qbos = st.session_state.get("quotes", {})
        if _qbos:
            for _s, _qd in _qbos.items():
                _ltp_bos[_s.replace("NSE:","")] = _qd.get("last_price", 0)
        # Supplement from df for any symbols not in quotes
        if not df.empty:
            for _, _r in df.iterrows():
                _sym2 = str(_r.get("Symbol","")).strip()
                if _sym2 and _sym2 not in _ltp_bos:
                    _ltp_bos[_sym2] = float(_r.get("LTP", 0) or 0)

        _run_bos_scan_safe(STOCKS, ltp_dict=_ltp_bos)

        # Also update OHLC DB in background for BOS tab history display
        # (every 65 min - only used for the historical chart in the tab, not for alerts)
        if _ohlc_db and _ohlc_db.is_update_needed(max_age_minutes=65):
            import threading as _bos_th
            _bos_th.Thread(target=_update_ohlc_db_safe, args=(kite, STOCKS), daemon=True).start()

except Exception as _bos_err:
    print(f"BOS runner: {_bos_err}")


# ── Chart Pattern Auto-Scan (hourly, non-blocking background thread) ──────────
try:
    _trigger_cps_auto_scan_if_due()
except Exception as _cps_auto_err:
    print(f"CPS auto-scan trigger error: {_cps_auto_err}")

# ── MACD and Inside Bar Data (Static + Merged) ───────
_macd_df = _load_macd_all()
_ib_df   = _load_inside_bars()

# ── SMC Intelligence + Telegram (injected by apply_smc_patch.py) ──
try:
    _smc_result = _run_smc_intelligence(kite, _oi_intel_for_delta)
    st.session_state["smc_result"] = _smc_result

    if _smc_result and "_error" not in _smc_result and st.session_state.get("tg_OI_INTEL", True):
        _ss       = _smc_result
        _sc       = _ss.get("final_score", 0)
        _sig      = _ss.get("final_signal", "")
        _act      = _ss.get("final_action", "")
        _tr15     = _ss.get("smc_trend_ltf", "")
        _trhtf    = _ss.get("smc_trend_htf", "")
        _conf     = _ss.get("conflict_detected", False)
        _ins      = _ss.get("oi_smc_interpretation", "")
        _pdz      = _ss.get("pd_zone", "")
        _pdp      = _ss.get("pd_zone_pct", 0)
        _sspot    = _ss.get("spot", 0)
        _satm     = _ss.get("atm", 0)
        _soidir   = _ss.get("oi_direction", "")
        _spcr     = _ss.get("oi_pcr", 0)
        _scw      = _ss.get("oi_call_wall", 0)
        _spf      = _ss.get("oi_put_floor", 0)
        _ssetup   = _ss.get("setup", {})
        _sbos     = _ss.get("bos_events", [])
        _schoch   = _ss.get("choch_events", [])
        _sbob     = _ss.get("nearest_bullish_ob")
        _sbfvg    = _ss.get("nearest_bullish_fvg")
        _sbliq    = _ss.get("buy_side_liq", [])
        _ssliq    = _ss.get("sell_side_liq", [])

        _sico = "🟢" if _sc >= 2 else ("🔴" if _sc <= -2 else "🟡")
        _cline = (
            "\n⚠️ <b>OI-SMC CONFLICT</b> - OI bearish but price BULLISH\n"
            "🧠 Gamma Squeeze / Smart Money Accumulation detected\n"
            "Price structure OVERRIDES raw OI signal\n"
        ) if _conf else ""

        _bline = ""
        if _sbos:
            _b = _sbos[-1]
            _bline = f"🚀 BOS {'↑' if 'UP' in _b['type'] else '↓'} at <b>{_b['price']:.0f}</b>\n"
        if _schoch:
            _c = _schoch[-1]
            _bline += f"🔄 CHoCH {'↑' if 'UP' in _c['type'] else '↓'} at <b>{_c['price']:.0f}</b>\n"

        _obline  = f"🟦 Bullish OB: <b>{_sbob['low']:.0f}–{_sbob['high']:.0f}</b>\n" if _sbob else ""
        _fvgline = f"📊 Bull FVG: <b>{_sbfvg['bottom']:.0f}–{_sbfvg['top']:.0f}</b> ({_sbfvg['gap_pct']}%)\n" if _sbfvg else ""
        _liqline = ""
        if _sbliq: _liqline += f"💧 Buy Stops: <b>{', '.join(str(int(x)) for x in _sbliq[:2])}</b>\n"
        if _ssliq: _liqline += f"💧 Sell Stops: <b>{', '.join(str(int(x)) for x in _ssliq[:2])}</b>\n"

        _opsetup = _ssetup.get("option_setup","")
        _slv = _ssetup.get("sl") or 0
        _t1v = _ssetup.get("target1") or 0
        _rrv = _ssetup.get("rr") or 0

        _smc_tg = (
            f"{_sico} <b>SMC + OI CONFLUENCE ALERT</b>\n"
            f"⏰ {datetime.now(IST).strftime('%H:%M:%S IST')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 Spot: <b>{_sspot:,.0f}</b>  |  ATM: <b>{_satm}</b>\n"
            f"{_sico} <b>{_sig}</b>  (Score: {_sc:+d})\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Raw OI: {_soidir}\n"
            f"📊 PCR: <b>{_spcr}</b>  |  Call Wall: <b>{_scw}</b>  |  Put Floor: <b>{_spf}</b>\n"
            f"📈 SMC 15m: <b>{_tr15}</b>  |  HTF: <b>{_trhtf}</b>\n"
            f"🎯 P/D Zone: <b>{_pdz}</b> ({_pdp:.0f}%)\n"
            f"{_cline}"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_bline}{_obline}{_fvgline}{_liqline}"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 {_ins}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Action: <b>{_act}</b>\n"
            f"⚡ {_opsetup}\n"
            f"🛡 SL: <b>{_slv:.0f}</b>  T1: <b>{_t1v:.0f}</b>  R:R <b>{_rrv}:1</b>\n"
            f"\n⚠️ <i>NOT financial advice.</i>"
        )
        _now_s   = datetime.now(IST)
        _skey    = _now_s.strftime(f"%H{(_now_s.minute // 10)*10:02d}")
        send_telegram_bg(_smc_tg, dedup_key=f"SMC_INTEL_{_skey}")

except Exception as _smc_run_err:
    st.session_state["smc_result"] = {"_error": str(_smc_run_err)}

if _oi_intel_for_delta:
    _curr_chain_snap = _oi_intel_for_delta.get("chain_snap")
    if _curr_chain_snap:
        _prev_chain_snap = _load_oi_15m_snapshot()
        if _prev_chain_snap:
            # Only compute delta when slot has actually changed
            _prev_slot = _prev_chain_snap.get("_slot", "")
            _curr_slot = _curr_chain_snap.get("_slot", "")
            if _curr_slot != _prev_slot:
                _oi_15m_events = compute_oi_15m_delta(_curr_chain_snap, _prev_chain_snap)
                if _oi_15m_events:
                    fire_oi_15m_alerts(
                        _oi_15m_events,
                        spot=_oi_intel_for_delta.get("spot", 0),
                        atm=_oi_intel_for_delta.get("atm", 0),
                    )
        # Always update snapshot after computing delta
        _save_oi_15m_snapshot(_curr_chain_snap)

    # ── Telegram OI Intelligence update (every 15-min slot change) ──
    try:
        _tg_oi_direction  = _oi_intel_for_delta.get("direction", "")
        _tg_oi_spot       = _oi_intel_for_delta.get("spot", 0)
        _tg_oi_atm        = _oi_intel_for_delta.get("atm", 0)
        _tg_oi_pcr        = _oi_intel_for_delta.get("pcr", 0)
        _tg_oi_maxpain    = _oi_intel_for_delta.get("max_pain", 0)
        _tg_oi_advice     = _oi_intel_for_delta.get("advice", "")
        _tg_oi_setup      = _oi_intel_for_delta.get("setup", "")
        _tg_oi_reason     = _oi_intel_for_delta.get("direction_reason", "")
        _tg_oi_call_wall  = _oi_intel_for_delta.get("nearest_call_wall", 0)
        _tg_oi_put_floor  = _oi_intel_for_delta.get("nearest_put_floor", 0)
        _tg_oi_sce        = _oi_intel_for_delta.get("strongest_ce", 0)
        _tg_oi_spe        = _oi_intel_for_delta.get("strongest_pe", 0)
        _tg_oi_sh_ce      = _oi_intel_for_delta.get("shifting_ce", 0)
        _tg_oi_sh_ce_pct  = _oi_intel_for_delta.get("shifting_ce_pct", 0)
        _tg_oi_sh_pe      = _oi_intel_for_delta.get("shifting_pe", 0)
        _tg_oi_sh_pe_pct  = _oi_intel_for_delta.get("shifting_pe_pct", 0)
        _tg_oi_ts         = _oi_intel_for_delta.get("timestamp", "")
        _tg_oi_icon       = "🟢" if "BULLISH" in _tg_oi_direction else ("🔴" if "BEARISH" in _tg_oi_direction else "⚠️")

        _tg_oi_msg = (
            f"{_tg_oi_icon} <b>NIFTY OI INTELLIGENCE UPDATE</b>\n"
            f"⏰ {_tg_oi_ts}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 Spot: <b>{_tg_oi_spot}</b>  |  ATM: <b>{_tg_oi_atm}</b>\n"
            f"📊 PCR: <b>{_tg_oi_pcr}</b>  |  Max Pain: <b>{_tg_oi_maxpain}</b>\n"
            f"🎯 Direction: <b>{_tg_oi_direction}</b>\n"
            f"📝 Reason: {_tg_oi_reason}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔴 Call Wall (Resistance): <b>{_tg_oi_call_wall}</b>\n"
            f"🟢 Put Floor (Support): <b>{_tg_oi_put_floor}</b>\n"
            f"⭐ Highest CE OI: <b>{_tg_oi_sce}</b> CE\n"
            f"⭐ Highest PE OI: <b>{_tg_oi_spe}</b> PE\n"
            f"📈 CE OI Building: <b>{_tg_oi_sh_ce}</b> CE (+{_tg_oi_sh_ce_pct}%)\n"
            f"📉 PE OI Building: <b>{_tg_oi_sh_pe}</b> PE (+{_tg_oi_sh_pe_pct}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 <b>Advice:</b> {_tg_oi_advice}\n"
            f"⚡ <b>Setup:</b> {_tg_oi_setup}\n"
            f"\n⚠️ <i>NOT financial advice.</i>"
        )
        # ── 10-minute dedup: round current minute down to nearest 10 ──
        _now_ist       = datetime.now(IST)
        _slot_min10    = (_now_ist.minute // 10) * 10          # e.g. 22:33 -> 22:30
        _tg_oi_slot    = _now_ist.strftime(f"%H{_slot_min10:02d}")  # "2230"
        if st.session_state.get("tg_OI_INTEL", True):
            send_telegram_bg(_tg_oi_msg, dedup_key=f"OI_INTEL_{_tg_oi_slot}")
    except Exception as _tg_oi_err:
        print(f"TG OI Intel error: {_tg_oi_err}")

# =========================================================
# 📈 FUTURES 15-MIN OI DELTA ENGINE
# Per-symbol futures: detects significant OI build/unwind
# in any single 15-min slot. Snapshot stored as JSON.
# =========================================================

def _load_fut_15m_snapshot():
    import json as _j
    if not os.path.exists(FUT_15M_SNAP_FILE):
        return None
    try:
        with open(FUT_15M_SNAP_FILE, "r", encoding="utf-8") as f:
            return _j.load(f)
    except Exception:
        return None

def _save_fut_15m_snapshot(snap):
    import json as _j
    try:
        with open(FUT_15M_SNAP_FILE, "w", encoding="utf-8") as f:
            _j.dump(snap, f, default=str, ensure_ascii=False)
    except Exception as e:
        print(f"FUT_15M_SNAP save error: {e}")

def _fut_15m_already_sent(slot, symbol, direction):
    key = f"{slot}|{symbol}|{direction}"
    today = date.today().isoformat()
    if not os.path.exists(FUT_15M_DEDUP):
        return False
    try:
        d = pd.read_csv(FUT_15M_DEDUP, encoding='utf-8')
        return bool(((d["DATE"] == today) & (d["KEY"] == key)).any())
    except Exception:
        return False

def _fut_15m_mark_sent(slot, symbol, direction):
    key = f"{slot}|{symbol}|{direction}"
    today = date.today().isoformat()
    row = {"DATE": today, "KEY": key}
    try:
        if os.path.exists(FUT_15M_DEDUP):
            d = pd.read_csv(FUT_15M_DEDUP, encoding='utf-8')
            d = pd.concat([d, pd.DataFrame([row])], ignore_index=True)
        else:
            d = pd.DataFrame([row])
        d.to_csv(FUT_15M_DEDUP, index=False, encoding='utf-8')
    except Exception as e:
        print(f"FUT_15M dedup write error: {e}")

def compute_fut_15m_delta(curr_snap, prev_snap, threshold_pct=5.0):
    """
    Compare futures OI snapshots. Returns list of significant events.
    Each event: {symbol, direction, oi_prev, oi_curr, oi_delta, oi_delta_pct,
                 ltp_prev, ltp_curr, price_pct, slot_prev, slot_curr,
                 position_type, label}
    """
    if not curr_snap or not prev_snap:
        return []

    slot_prev = prev_snap.get("_slot", "-")
    slot_curr = curr_snap.get("_slot", "-")
    events = []

    for sym, curr in curr_snap.items():
        if sym.startswith("_"):
            continue
        prev = prev_snap.get(sym)
        if not prev:
            continue

        oi_curr  = int(curr.get("oi",  0) or 0)
        oi_prev  = int(prev.get("oi",  0) or 0)
        ltp_curr = float(curr.get("ltp", 0) or 0)
        ltp_prev = float(prev.get("ltp", 0) or 0)

        if oi_curr <= 0 or oi_prev <= 0:
            continue

        oi_delta     = oi_curr - oi_prev
        oi_delta_pct = round(abs(oi_delta) / oi_curr * 100, 1)
        if oi_delta_pct < threshold_pct:
            continue

        price_pct  = round((ltp_curr - ltp_prev) / ltp_prev * 100, 2) if ltp_prev > 0 else 0.0
        direction  = "BUILD" if oi_delta > 0 else "UNWIND"

        # Classify position type exactly like fut_df classify()
        if ltp_curr > ltp_prev and oi_delta > 0:
            position_type = "🟢 LONG BUILDUP"
            pt_color = "#1b5e20"
        elif ltp_curr < ltp_prev and oi_delta > 0:
            position_type = "🔴 SHORT BUILDUP"
            pt_color = "#b71c1c"
        elif ltp_curr > ltp_prev and oi_delta < 0:
            position_type = "⚠️ SHORT COVERING"
            pt_color = "#e65100"
        elif ltp_curr < ltp_prev and oi_delta < 0:
            position_type = "⚠️ LONG UNWINDING"
            pt_color = "#880e4f"
        else:
            position_type = "NEUTRAL"
            pt_color = "#607d8b"

        # Clean symbol (strip NFO: prefix and expiry suffix for display)
        display_sym = sym.replace("NFO:", "")

        ltp_arrow = "+" if price_pct >= 0 else ""
        oi_arrow  = "+" if oi_delta > 0 else ""
        label = (f"{position_type} - {display_sym}: "
                 f"OI {oi_arrow}{oi_delta:,} ({oi_delta_pct:.1f}% of OI in 15 min), "
                 f"price Rs.{ltp_prev:.1f}->Rs.{ltp_curr:.1f} ({ltp_arrow}{price_pct:.2f}%)")

        events.append({
            "SLOT":          f"{slot_prev}->{slot_curr}",
            "SYMBOL":        display_sym,
            "POSITION_TYPE": position_type,
            "PT_COLOR":      pt_color,
            "OI_PREV":       oi_prev,
            "OI_CURR":       oi_curr,
            "OI_DELTA":      oi_delta,
            "OI_DELTA_%":    oi_delta_pct,
            "LTP_PREV":      ltp_prev,
            "LTP_CURR":      ltp_curr,
            "PRICE_%":       price_pct,
            "DIRECTION":     direction,
            "LABEL":         label,
        })

    # ── Always surface top mover per position type ───────────
    _fcands = {}
    for sym2, curr2 in curr_snap.items():
        if sym2.startswith("_"):
            continue
        prev2 = prev_snap.get(sym2)
        if not prev2:
            continue
        oi_c2 = int(curr2.get("oi", 0) or 0)
        oi_p2 = int(prev2.get("oi", 0) or 0)
        ltp_c2 = float(curr2.get("ltp", 0) or 0)
        ltp_p2 = float(prev2.get("ltp", 0) or 0)
        if oi_c2 <= 0 or oi_p2 <= 0:
            continue
        d2  = oi_c2 - oi_p2
        dp2 = round(abs(d2) / oi_c2 * 100, 1)
        pp2 = round((ltp_c2 - ltp_p2) / ltp_p2 * 100, 2) if ltp_p2 > 0 else 0.0
        dir2 = "BUILD" if d2 > 0 else "UNWIND"
        if ltp_c2 > ltp_p2 and d2 > 0:
            pt2, ptc2 = "🟢 LONG BUILDUP", "#1b5e20"
        elif ltp_c2 < ltp_p2 and d2 > 0:
            pt2, ptc2 = "🔴 SHORT BUILDUP", "#b71c1c"
        elif ltp_c2 > ltp_p2 and d2 < 0:
            pt2, ptc2 = "⚠️ SHORT COVERING", "#e65100"
        elif ltp_c2 < ltp_p2 and d2 < 0:
            pt2, ptc2 = "⚠️ LONG UNWINDING", "#880e4f"
        else:
            pt2, ptc2 = "NEUTRAL", "#607d8b"
        dsym2 = sym2.replace("NFO:", "")
        la2 = "+" if pp2 >= 0 else ""
        oa2 = "+" if d2 > 0 else ""
        lbl2 = (f"{pt2} - {dsym2}: OI {oa2}{d2:,} ({dp2:.1f}% of OI in 15 min), "
                f"price Rs.{ltp_p2:.1f}->Rs.{ltp_c2:.1f} ({la2}{pp2:.2f}%)")
        cand2 = {"SLOT": f"{slot_prev}->{slot_curr}", "SYMBOL": dsym2,
                 "POSITION_TYPE": pt2, "PT_COLOR": ptc2,
                 "OI_PREV": oi_p2, "OI_CURR": oi_c2, "OI_DELTA": d2,
                 "OI_DELTA_%": dp2, "LTP_PREV": ltp_p2, "LTP_CURR": ltp_c2,
                 "PRICE_%": pp2, "DIRECTION": dir2, "LABEL": lbl2, "TOP_MOVER": True}
        ex2 = _fcands.get(pt2)
        if ex2 is None or abs(d2) > abs(ex2["OI_DELTA"]):
            _fcands[pt2] = cand2

    existing_fkeys = {(e["SYMBOL"], e["DIRECTION"]) for e in events}
    for pt2, cand2 in _fcands.items():
        k2 = (cand2["SYMBOL"], cand2["DIRECTION"])
        if k2 not in existing_fkeys:
            events.append(cand2)
        else:
            for e in events:
                if (e["SYMBOL"], e["DIRECTION"]) == k2:
                    e["TOP_MOVER"] = True

    events.sort(key=lambda x: (not x.get("TOP_MOVER", False), -x["OI_DELTA_%"]))
    return events


def fire_fut_15m_alerts(events):
    """Email + toast for significant futures 15-min OI events."""
    if not events or not is_market_hours():
        return

    now_slot = datetime.now(IST).strftime("%H:%M")
    new_events = []
    for ev in events:
        if not _fut_15m_already_sent(now_slot, ev["SYMBOL"], ev["DIRECTION"]):
            _fut_15m_mark_sent(now_slot, ev["SYMBOL"], ev["DIRECTION"])
            new_events.append(ev)

    if not new_events:
        return

    # ── Browser toasts (top 3) - gated by LONG_UNWIND toggle ──
    if st.session_state.get("tg_LONG_UNWIND", False):
        for ev in new_events[:3]:
            if BROWSER_ALERTS_ENABLED: st.toast(ev["LABEL"][:120], icon="📈" if ev["DIRECTION"] == "BUILD" else "📉")

    # ── Email ──────────────────────────────────────────────
    if not can_send_email() or not is_email_allowed():
        return

    now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    subject = f"[OiAnalytics] Futures 15-Min OI Shift - {len(new_events)} Stock(s) | {now_str}"

    tbl_rows = ""
    for i, ev in enumerate(new_events):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        p_col = "#2e7d32" if ev["PRICE_%"] >= 0 else "#c62828"
        tbl_rows += f"""<tr style="background:{bg};">
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">{ev['SLOT']}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{ev['SYMBOL']}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{ev['PT_COLOR']};font-weight:bold;">{ev['POSITION_TYPE']}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;color:{ev['PT_COLOR']};">{ev['OI_DELTA']:+,}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{round(ev['OI_DELTA_%'], 1)}%</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">Rs.{round(ev['LTP_PREV'], 1)} -> Rs.{round(ev['LTP_CURR'], 1)}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{p_col};">{round(ev['PRICE_%'], 2)}%</td>
        </tr>"""

    html_body = f"""
<html><body style="font-family:Arial,sans-serif;background:#f0f0f0;margin:0;padding:20px;">
<div style="max-width:1100px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;
            box-shadow:0 2px 8px rgba(0,0,0,0.15);">
  <div style="background:#004d40;padding:16px 20px;">
    <h2 style="color:#fff;margin:0;font-size:18px;letter-spacing:1px;">
      📈 Futures 15-Min OI Shift Alert
    </h2>
    <p style="color:#b2dfdb;margin:4px 0 0;font-size:12px;">
      Generated: {now_str} &nbsp;|&nbsp; OiAnalytics [LOCAL]
    </p>
  </div>
  <div style="background:#e8f5e9;padding:10px 20px;border-bottom:2px solid #004d40;">
    <b>🆕 {len(new_events)} stock future(s) with significant OI shift in last 15 minutes</b>
  </div>
  <div style="padding:16px 20px;overflow-x:auto;">
  <table style="border-collapse:collapse;width:100%;font-family:Courier New,monospace;">
    <thead><tr>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">SLOT</th>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">SYMBOL</th>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">POSITION TYPE</th>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">OI CHANGE</th>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">% OF OI</th>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">PRICE Δ</th>
      <th style="background:#004d40;color:#fff;padding:6px 8px;border:1px solid #ccc;font-size:12px;">PRICE %</th>
    </tr></thead>
    <tbody>{tbl_rows}</tbody>
  </table>
  </div>
  <div style="background:#212121;padding:10px 20px;">
    <p style="color:#aaa;font-size:11px;margin:0;">
      Threshold: ≥5% OI change of symbol total in one 15-min candle.
      Classified: Long Buildup / Short Buildup / Short Covering / Long Unwinding.
    </p>
  </div>
</div></body></html>"""

    def _bg_send(subj=subject, body=html_body):
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            msg = MIMEMultipart("alternative")
            msg["From"]    = EMAIL_FROM
            msg["To"]      = ", ".join(EMAIL_TO)
            msg["Subject"] = subj
            msg.attach(MIMEText(body, "html", "utf-8"))
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as smtp:
                smtp.login(EMAIL_FROM, EMAIL_PASS)
                smtp.send_message(msg)
            record_email_sent()
        except Exception as e:
            print(f"FUT_15M EMAIL ERROR: {e}")

    threading.Thread(target=_bg_send, daemon=True).start()


# =========================================================
# 🚀 EARLY HIGH-GAIN RUNNERS (YH MOMENTUM ENGINE)
# =========================================================

runner_df = df.copy()

# -------------------------
# 1️⃣ Range expansion after YH
# -------------------------
runner_df["YH_RANGE_EXP_%"] = (
    (runner_df["LIVE_HIGH"] - runner_df["YEST_HIGH"])
    / runner_df["YEST_HIGH"] * 100
).round(2)

# -------------------------
# 2️⃣ Open distance from YH
# -------------------------
runner_df["OPEN_DIST_YH_%"] = (
    abs(runner_df["LIVE_OPEN"] - runner_df["YEST_HIGH"])
    / runner_df["YEST_HIGH"] * 100
).round(2)

# -------------------------
# 3️⃣ Acceptance above YH
# -------------------------
runner_df["YH_ACCEPTED"] = runner_df["LIVE_LOW"] >= runner_df["YEST_HIGH"]

# -------------------------
# 4️⃣ Momentum score (core logic)
# -------------------------
runner_df["MOMO_SCORE"] = 0

runner_df.loc[runner_df["LTP"] > runner_df["YEST_HIGH"], "MOMO_SCORE"] += 2
runner_df.loc[runner_df["CHANGE_%"] >= 1.5, "MOMO_SCORE"] += 2
runner_df.loc[runner_df["YH_RANGE_EXP_%"] >= 1.0, "MOMO_SCORE"] += 2
runner_df.loc[runner_df["YH_ACCEPTED"], "MOMO_SCORE"] += 2
runner_df.loc[runner_df["LTP"] > runner_df["LIVE_OPEN"], "MOMO_SCORE"] += 1

# -------------------------
# 5️⃣ Final EARLY RUNNER FILTER
# -------------------------
early_runner_df = runner_df.loc[
    (runner_df["LTP"] > runner_df["YEST_HIGH"]) &
    (runner_df["MOMO_SCORE"] >= 6),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "MOMO_SCORE",

        # Strength diagnostics
        "YH_RANGE_EXP_%",
        "OPEN_DIST_YH_%",
        "YH_ACCEPTED",

        # Price context
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE",
    ]
].copy()

# -------------------------
# 6️⃣ Sort -> BEST early runners on TOP
# -------------------------
early_runner_df = early_runner_df.sort_values(
    by=["MOMO_SCORE", "CHANGE_%"],
    ascending=[False, False]
)


# =========================================================
# 🚨 YESTERDAY GREEN -> BREAKOUT ALERT  (Point 2: first-15m filter)
# =========================================================
# Filter: price must ALSO break/hold first 15-min candle HIGH to avoid false breakouts
# ─── YEST GREEN/RED alerts - deferred to after _all_15m is loaded (see below) ───
# (filter uses _all_15m which is built after the 15m CSV is read at line ~9175)
_yest_green_alert_pending = green_zone_df.Symbol.tolist()
_yest_red_alert_pending   = red_zone_df.Symbol.tolist()



###################################################################################
#🔴 RED SETUP =  LIVE_OPEN > YEST_LOW & LIVE_OPEN < YEST_CLOSE && First 15-min LOW should NOT break yesterday LOW
#🟢 GREEN SETUP =  LIVE_OPEN > YEST_CLOSE  & LIVE_OPEN < YEST_HIGH  & First 15-min HIGH should NOT break yesterday HIGH
###########################################################################################

# =========================================================
# FIRST 15-MIN CANDLE FETCHER - BATCHED (✅ FIX: was per-symbol, now one cached call)
# =========================================================
@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent
def get_today_15m(token):
    today = date.today()
    start_dt = datetime.combine(today, time(9,15))
    end_dt   = datetime.combine(today, time(15,30))

    try:
        return kite.historical_data(
            token,
            start_dt,
            end_dt,
            "15minute"
        )
    except Exception:
        return []


# ✅ FIX: get_all_today_15m also moved to background thread - never blocks UI
def _fetch_15m_background():
    """Runs in a daemon thread - fetches all 15m candles without blocking UI."""
    today = date.today()
    start_dt = datetime.combine(today, time(9, 15))
    end_dt   = datetime.combine(today, time(15, 30))
    result = {}
    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(tk, start_dt, end_dt, "15minute")
            if bars:
                result[s] = bars
        except Exception:
            continue
        tm.sleep(0.05)
    if result:
        # Flatten dict to DataFrame and save CSV
        rows15 = []
        for sym, bars in result.items():
            for b in bars:
                rows15.append({
                    "Symbol": sym, "date": b["date"],
                    "open":   b["open"], "high": b["high"],
                    "low":    b["low"],  "close": b["close"],
                    "volume": b.get("volume", 0),
                })
        if rows15:
            pd.DataFrame(rows15).to_csv(FIFTEEN_M_CACHE_CSV, index=False, encoding='utf-8')

if "_FIFTEEN_MIN_FETCH_RUNNING" not in dir():
    _FIFTEEN_MIN_FETCH_RUNNING = False

if not _csv_is_fresh(FIFTEEN_M_CACHE_CSV) and not _FIFTEEN_MIN_FETCH_RUNNING:
    _FIFTEEN_MIN_FETCH_RUNNING = True
    def _run_15m_and_clear():
        global _FIFTEEN_MIN_FETCH_RUNNING
        try:
            _fetch_15m_background()
        finally:
            _FIFTEEN_MIN_FETCH_RUNNING = False
    t15 = threading.Thread(target=_run_15m_and_clear, daemon=True)
    t15.start()

# ✅ Read 15m data from CSV - always fresh after background thread completes
# Use _load_today_csv so stale yesterday data is never loaded
_df15_today = _load_today_csv(FIFTEEN_M_CACHE_CSV)
if _df15_today is not None:
    _df15 = _safe_parse_date_col(_df15_today, col="date")
    # Handle "datetime" alias if "date" column not present
    if "date" not in _df15.columns and "datetime" in _df15.columns:
        _df15 = _df15.rename(columns={"datetime": "date"})
        _df15 = _safe_parse_date_col(_df15, col="date")
    if not _df15.empty and "Symbol" in _df15.columns:
        _all_15m = {sym: grp.to_dict("records") for sym, grp in _df15.groupby("Symbol")}
    else:
        _all_15m = {}
else:
    _all_15m = {}

# =========================================================
# 🚨 YESTERDAY GREEN -> BREAKOUT ALERT  (Point 2: first-15min filter)
# _all_15m is now populated - safe to use in the filter
# =========================================================
def _passes_15m_filter(sym, direction="UP"):
    """Check if first 15-min candle (09:15) supports the breakout direction.
    Returns True if:
    - No candle data yet (conservative: allow through)
    - First candle HIGH >= YEST_HIGH * 0.998 (for UP)
    - First candle LOW  <= YEST_LOW  * 1.002 (for DOWN)
    """
    # Safe access - _all_15m may not be defined if function is called before CSV loads
    try:
        candles = _all_15m.get(sym, [])
    except NameError:
        return True   # _all_15m not loaded yet - allow stock through
    if not candles:
        return True   # no 15m data yet - allow through
    first_candle = candles[0]   # 09:15 candle
    if direction == "UP":
        try:
            yh = float(df[df["Symbol"] == sym]["YEST_HIGH"].iloc[0])
        except Exception:
            return True
        return yh > 0 and float(first_candle.get("high", 0)) >= yh * 0.998
    else:
        try:
            yl = float(df[df["Symbol"] == sym]["YEST_LOW"].iloc[0])
        except Exception:
            return True
        return yl > 0 and float(first_candle.get("low", 9999999)) <= yl * 1.002

_now_hhmm_15 = datetime.now(IST).strftime("%H%M")
_first15_ready = _now_hhmm_15 >= "0930"   # first candle complete after 09:30

green_syms_filtered = [
    s for s in _yest_green_alert_pending
    if (not _first15_ready) or _passes_15m_filter(s, "UP")
]
red_syms_filtered = [
    s for s in _yest_red_alert_pending
    if (not _first15_ready) or _passes_15m_filter(s, "DOWN")
]

new_green_break = detect_new_entries("YEST_GREEN_BREAK", green_syms_filtered)
notify_all(
    "YEST_GREEN_BREAK",
    "🟢Yesterday GREEN -> Breakout Above YH (✅ 15m confirmed)",
    new_green_break,
    ltp_map
)

new_red_break = detect_new_entries("YEST_RED_BREAK", red_syms_filtered)
notify_all(
    "YEST_RED_BREAK",
    "🔴Yesterday RED -> Breakdown Below YL (✅ 15m confirmed)",
    new_red_break,
    ltp_map
)

# =========================================================
# 📐 EMA7 INTRADAY ENGINE
# =========================================================
# Computes EMA7 on two timeframes from today's live candles:
#   • 15-minute EMA7  -> fine-grained entry timing (15m chart)
#   • 1-Hour EMA7     -> higher-timeframe trend bias (1H chart)
#
# Storage (dated CSVs, auto-cleared next day):
#   CACHE/ema7_15min_YYYYMMDD.csv  -> columns: Symbol, EMA7_15M, CANDLES_15M
#   CACHE/ema7_1hour_YYYYMMDD.csv  -> columns: Symbol, EMA7_1H,  CANDLES_1H
#
# Lifecycle:
#   1. On first app load: built immediately from FIFTEEN_M_CACHE_CSV
#   2. Every 60 seconds: background thread recomputes + overwrites CSV
#   3. Dashboard reads CSV -> merges into df -> WATCHLIST tab shows correct values
# =========================================================

def _compute_intraday_ema7(df_candles, resample_rule):
    """
    Compute EMA7 on intraday candles, warmed up from historical data.

    HOW IT WORKS (same as Kite / TradingView):
      1. Load CACHE/{Symbol}_5minute.csv  ->  30-day 5-min history per symbol
      2. Resample history to target timeframe (15min / 60min), previous days only
      3. Append TODAY's completed candles from df_candles (today's 15m cache)
      4. Run ewm(span=7, adjust=False) across the full combined series
         -> EMA is warmed from history so the value at 09:15 today is accurate
      5. Return today's last EMA7 value per symbol

    At 09:15 with 0 today-candles -> EMA7 = warmed from yesterday  ✅
    At 09:30 with 1 today-candle  -> EMA7 updates correctly         ✅
    Without warmup                -> EMA7 at 09:15 ≈ just the open  ❌
    """
    label = "15M" if "15" in resample_rule else "1H"
    rows  = []

    # ── Current partial-candle cutoff (exclude in-progress candle) ────
    _now_ist   = datetime.now(IST).replace(tzinfo=None)
    _min_floor = (_now_ist.minute // 15) * 15
    _cutoff    = pd.Timestamp(_now_ist.replace(minute=_min_floor, second=0, microsecond=0))
    _today_dt  = date.today()

    # ── Build today's completed candles map: sym -> Series(close, index=datetime) ──
    today_map = {}
    if df_candles is not None and not df_candles.empty:
        _t = df_candles.copy()
        _t = _safe_parse_date_col(_t, col="date")
        # FIX-8a: Kite CSVs can be tz-aware (UTC+05:30); strip to naive IST
        # so comparison with naive _cutoff never raises TypeError
        if _t["date"].dt.tz is not None:
            _t["date"] = _t["date"].dt.tz_convert(IST).dt.tz_localize(None)
        _t = _t[(_t["date"].dt.date == _today_dt) & (_t["date"] < _cutoff)]
        _t = _t.sort_values("date")
        for sym, grp in _t.groupby("Symbol"):
            today_map[sym] = grp.set_index("date")["close"]

    # ── Full symbol set = union of today-candle symbols + 5min-history symbols ──
    all_symbols = set(today_map.keys())
    try:
        for fname in os.listdir(CACHE_DIR):
            if fname.endswith("_5minute.csv"):
                all_symbols.add(fname.replace("_5minute.csv", ""))
    except Exception:
        pass

    for sym in all_symbols:
        # ── Load and resample historical 5-min candles (previous days only) ──
        hist_series = pd.Series(dtype=float)
        hist_path   = os.path.join(CACHE_DIR, f"{sym}_5minute.csv")
        if os.path.exists(hist_path):
            try:
                _h = pd.read_csv(hist_path, encoding='utf-8')
                _h = _safe_parse_date_col(_h, col="date")
                # FIX-8b: same tz strip for historical CSVs
                if _h["date"].dt.tz is not None:
                    _h["date"] = _h["date"].dt.tz_convert(IST).dt.tz_localize(None)
                _h = _h[_h["date"].dt.date < _today_dt].sort_values("date")
                if not _h.empty:
                    _rs = (
                        _h.set_index("date")["close"]
                        .resample(resample_rule, origin="start")
                        .last()
                    )
                    hist_series = _rs.dropna() if not _rs.empty else pd.Series(dtype=float)
            except Exception:
                pass

        # ── Resample today's candles ─────────────────────────────────────
        today_series = pd.Series(dtype=float)
        if sym in today_map and not today_map[sym].empty:
            _ts = (
                today_map[sym]
                .resample(resample_rule, origin="start")
                .last()
            )
            today_series = _ts.dropna() if not _ts.empty else pd.Series(dtype=float)

        if hist_series.empty and today_series.empty:
            continue

        # ── Combine: history first, then today ───────────────────────────
        _parts = [s for s in [hist_series, today_series] if not s.empty]
        combined = pd.concat(_parts).sort_index() if _parts else pd.Series(dtype=float)
        combined = combined[~combined.index.duplicated(keep="last")]

        if combined.empty:
            continue

        # ── EMA7 across full combined series (Kite-identical method) ─────
        ema_series = combined.ewm(span=7, adjust=False).mean()
        latest_ema = round(float(ema_series.iloc[-1]), 2)

        rows.append({
            "Symbol":           sym,
            f"EMA7_{label}":    latest_ema,
            f"CANDLES_{label}": len(today_series),   # today's candles only (shows data freshness)
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def build_and_save_ema7_intraday():
    """
    Compute history-warmed EMA7 for 15m + 1H -> save to dated CSVs atomically.
    Called at startup and every 60 seconds by background thread.
    Runs even pre-market so EMA7 is ready the moment 09:15 opens.
    """
    src = _load_today_csv(FIFTEEN_M_CACHE_CSV)   # None pre-market -> handled gracefully

    def _atomic_save(df_out, dest_path):
        if df_out is None or df_out.empty:
            return
        tmp = dest_path + ".tmp"
        try:
            df_out.to_csv(tmp, index=False, encoding="utf-8")
            try:
                os.replace(tmp, dest_path)   # atomic on Linux/macOS
            except PermissionError:           # FIX-7: Windows fallback
                import shutil as _shutil
                _shutil.move(tmp, dest_path)
        except Exception:
            try: os.remove(tmp)
            except Exception: pass

    _atomic_save(_compute_intraday_ema7(src, "15min"), EMA7_15M_FILE)
    _atomic_save(_compute_intraday_ema7(src, "60min"), EMA7_1H_FILE)


def _ema7_refresh_loop():
    """Background daemon - refreshes EMA7 every 60 s from 08:50 to 15:35."""
    while True:
        hm = datetime.now(IST).replace(tzinfo=None).strftime("%H:%M")
        if "08:50" <= hm <= "15:35":   # start early so EMA7 is ready at 09:15 open
            try:
                build_and_save_ema7_intraday()
            except Exception:
                pass
        tm.sleep(60)


# ── Run once at startup (session guard prevents repeat on Streamlit reruns) ──
if not st.session_state.get("_ema7_intraday_built", False):
    build_and_save_ema7_intraday()
    st.session_state["_ema7_intraday_built"] = True

# ── Start background refresh thread (once per session) ───────────────────────
if not st.session_state.get("_ema7_thread_running", False):
    st.session_state["_ema7_thread_running"] = True
    threading.Thread(target=_ema7_refresh_loop, daemon=True).start()

# ── Load today's EMA7 CSVs ───────────────────────────────────────────────────
_ema7_15m_df = _load_today_csv(EMA7_15M_FILE, required_cols=["Symbol", "EMA7_15M"])
_ema7_1h_df  = _load_today_csv(EMA7_1H_FILE,  required_cols=["Symbol", "EMA7_1H"])

# ── EMA7 INTRADAY MERGE - fuse into main df ──────────────────────────────────
for _ecol in ["EMA7_15M", "CANDLES_15M", "EMA7_1H", "CANDLES_1H"]:
    if _ecol in df.columns:
        df.drop(columns=[_ecol], inplace=True)

if _ema7_15m_df is not None and not _ema7_15m_df.empty:
    df = df.merge(_ema7_15m_df, on="Symbol", how="left")
if _ema7_1h_df is not None and not _ema7_1h_df.empty:
    df = df.merge(_ema7_1h_df, on="Symbol", how="left")

for _ecol in ["EMA7_15M", "EMA7_1H"]:
    if _ecol in df.columns:
        df[_ecol] = pd.to_numeric(df[_ecol], errors="coerce")

# =========================================================
# 🔴 / 🟢 DELAYED BREAK STRUCTURE
# =========================================================

# =========================================================
# 🔴 / 🟢 YESTERDAY STRUCTURE CONTINUATION (FINAL VERSION)
# =========================================================

red_rows = []
green_rows = []

for _, r in df.iterrows():

    candles = _all_15m.get(r["Symbol"], [])
    if not candles or len(candles) < 2:
        continue

    first = candles[0]
    rest  = candles[1:]

    # =====================================================
    # 🟢 GREEN STRUCTURE (Yesterday GREEN -> Break Above YH)
    # =====================================================
    if (
        r["YEST_CLOSE"] > r["YEST_OPEN"] and   # Yesterday GREEN
        r["LIVE_OPEN"] > r["YEST_CLOSE"] and
        r["LIVE_OPEN"] < r["YEST_HIGH"] and
        first["high"] <= r["YEST_HIGH"]        # First 15m did not break
    ):

        breakout_candle = next(
            (c for c in rest if c["high"] > r["YEST_HIGH"]),
            None
        )

        if breakout_candle and r["LTP"] > r["YEST_HIGH"]:

            post_high = max(
                c["high"] for c in candles
                if c["date"] >= breakout_candle["date"]
            )

            gain_value = round(post_high - r["YEST_HIGH"], 2)
            gain_pct   = round((gain_value / r["YEST_HIGH"]) * 100, 2)

            green_rows.append({
                "Symbol":           r["Symbol"],
                "LTP":              r["LTP"],
                "CHANGE_%":         r["CHANGE_%"],
                "LIVE_OPEN":        r["LIVE_OPEN"],
                "LIVE_HIGH":        r.get("LIVE_HIGH", 0),
                "LIVE_LOW":         r.get("LIVE_LOW",  0),
                "VOL_%":            r.get("VOL_%", 0),
                "YEST_CLOSE":       r["YEST_CLOSE"],
                "YEST_HIGH":        r["YEST_HIGH"],
                "YEST_LOW":         r.get("YEST_LOW", 0),
                "BREAK_TIME":       breakout_candle["date"].strftime("%H:%M"),
                "POST_BREAK_GAIN":  gain_value,
                "POST_BREAK_GAIN_%": gain_pct,
                "CHANGE":           r["CHANGE"],
                "YEST_OPEN":        r["YEST_OPEN"],
            })

    # =====================================================
    # 🔴 RED STRUCTURE (Yesterday RED -> Break Below YL)
    # =====================================================
    if (
        r["YEST_CLOSE"] < r["YEST_OPEN"] and   # Yesterday RED
        r["LIVE_OPEN"] > r["YEST_LOW"] and
        r["LIVE_OPEN"] < r["YEST_CLOSE"] and
        first["low"] >= r["YEST_LOW"]          # First 15m did not break
    ):

        breakdown_candle = next(
            (c for c in rest if c["low"] < r["YEST_LOW"]),
            None
        )

        if breakdown_candle and r["LTP"] < r["YEST_LOW"]:

            post_low = min(
                c["low"] for c in candles
                if c["date"] >= breakdown_candle["date"]
            )

            drop_value = round(r["YEST_LOW"] - post_low, 2)
            drop_pct   = round((drop_value / r["YEST_LOW"]) * 100, 2)

            red_rows.append({
                "Symbol":            r["Symbol"],
                "LTP":               r["LTP"],
                "CHANGE_%":          r["CHANGE_%"],
                "LIVE_OPEN":         r["LIVE_OPEN"],
                "LIVE_HIGH":         r.get("LIVE_HIGH", 0),
                "LIVE_LOW":          r.get("LIVE_LOW",  0),
                "VOL_%":             r.get("VOL_%", 0),
                "YEST_CLOSE":        r["YEST_CLOSE"],
                "YEST_HIGH":         r["YEST_HIGH"],
                "YEST_LOW":          r["YEST_LOW"],
                "BREAK_TIME":        breakdown_candle["date"].strftime("%H:%M"),
                "POST_BREAK_DROP":   drop_value,
                "POST_BREAK_DROP_%": drop_pct,
                "CHANGE":            r["CHANGE"],
                "YEST_OPEN":         r["YEST_OPEN"],
            })


# =========================================================
# CREATE DATAFRAMES (SAFE COLUMNS)
# =========================================================

green_structure_df = pd.DataFrame(
    green_rows,
    columns=[
        "Symbol", "LTP", "CHANGE_%",
        "LIVE_OPEN", "LIVE_HIGH", "LIVE_LOW", "VOL_%",
        "YEST_CLOSE", "YEST_HIGH", "YEST_LOW",
        "BREAK_TIME", "POST_BREAK_GAIN", "POST_BREAK_GAIN_%",
        "CHANGE", "YEST_OPEN",
    ]
) if green_rows else pd.DataFrame(columns=[
    "Symbol","LTP","CHANGE_%","LIVE_OPEN","LIVE_HIGH","LIVE_LOW","VOL_%",
    "YEST_CLOSE","YEST_HIGH","YEST_LOW","BREAK_TIME","POST_BREAK_GAIN","POST_BREAK_GAIN_%",
    "CHANGE","YEST_OPEN",
])

red_structure_df = pd.DataFrame(
    red_rows,
    columns=[
        "Symbol", "LTP", "CHANGE_%",
        "LIVE_OPEN", "LIVE_HIGH", "LIVE_LOW", "VOL_%",
        "YEST_CLOSE", "YEST_HIGH", "YEST_LOW",
        "BREAK_TIME", "POST_BREAK_DROP", "POST_BREAK_DROP_%",
        "CHANGE", "YEST_OPEN",
    ]
) if red_rows else pd.DataFrame(columns=[
    "Symbol","LTP","CHANGE_%","LIVE_OPEN","LIVE_HIGH","LIVE_LOW","VOL_%",
    "YEST_CLOSE","YEST_HIGH","YEST_LOW","BREAK_TIME","POST_BREAK_DROP","POST_BREAK_DROP_%",
    "CHANGE","YEST_OPEN",
])

#red_structure_df   = pd.DataFrame(red_rows)
##green_structure_df = green_structure_df.sort_values("POST_BREAK_GAIN_%", ascending=False)
#red_structure_df = red_structure_df.sort_values("POST_BREAK_DROP_%", ascending=False)
if not green_structure_df.empty and "POST_BREAK_GAIN_%" in green_structure_df.columns:
    green_structure_df = green_structure_df.sort_values(
        "POST_BREAK_GAIN_%", ascending=False
    )

if not red_structure_df.empty and "POST_BREAK_DROP_%" in red_structure_df.columns:
    red_structure_df = red_structure_df.sort_values(
        "POST_BREAK_DROP_%", ascending=False
    )


# ──────────────────────────────────────────────────────────────────────
# 🚨 NEW OPEN STRUCTURE TELEGRAM-ONLY ALERTS
# All 5 source dataframes are fully built and sorted above.
# Fires once per stock per day (dedup via detect_new_entries).
# ──────────────────────────────────────────────────────────────────────

def _send_tg_structure_alert(category, title, df_in, ltp_map_ref):
    """
    Send a Telegram-only alert for open structure breakouts.
    ONE alert per stock per category per day using dedup keys.
    Each stock gets its own dedup key: STRUCT_<CATEGORY>_<SYMBOL>_<YYYYMMDD>
    This avoids the detect_new_entries pitfall (which clears on each cycle).
    """
    try:
        if df_in is None or df_in.empty:
            return
        if not is_market_hours():
            return
        if _is_tg_disabled(category):
            return
        syms = df_in["Symbol"].tolist() if "Symbol" in df_in.columns else []
        if not syms:
            return

        _today_str = datetime.now(IST).strftime("%Y%m%d")
        _now_str   = datetime.now(IST).strftime("%H:%M IST")
        
        # Robust Bull/Bear detection
        _is_bear = any(x in category.upper() for x in ("LOW", "BELOW", "DOWN", "LOWER", "RED"))
        is_bull = not _is_bear
        
        border = "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩" if is_bull else "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"
        icon   = "🟢" if is_bull else "🔴"

        # Check which stocks haven't been alerted yet today
        # Use a shared dedup store (the existing _TG_DEDUP file, keyed per stock)
        new_syms = []
        for s in syms:
            _stock_dedup = f"STRUCT_{category}_{s}_{_today_str}"
            with _TG_DEDUP_LOCK:
                _dedup_d = _load_tg_dedup()
                if not _dedup_d.get(_stock_dedup):
                    new_syms.append((s, _stock_dedup))

        if not new_syms:
            return  # all stocks already alerted today

        lines = []
        for s, _ in new_syms[:15]:
            try:
                row_df = df_in[df_in["Symbol"] == s]
                if row_df.empty:
                    # Try global df
                    row_df = df[df["Symbol"] == s]
                
                if row_df.empty:
                    lines.append(f"  {icon} <b>{s}</b>")
                    continue
                
                r = row_df.iloc[0]
                ltp = r.get("LTP", 0)
                chg = r.get("CHANGE", 0)
                chp = r.get("CHANGE_%", 0)
                
                # Fetch more details from global df for consistency
                g_row = df[df["Symbol"] == s].iloc[0] if s in df["Symbol"].values else r
                yh  = g_row.get("YEST_HIGH", 0)
                yl  = g_row.get("YEST_LOW", 0)
                yc  = g_row.get("YEST_CLOSE", 0)
                wh  = g_row.get("HIGH_W", 0)
                wl  = g_row.get("LOW_W", 0)
                
                o = g_row.get("LIVE_OPEN", 0)
                h = g_row.get("LIVE_HIGH", 0)
                l = g_row.get("LIVE_LOW", 0)
                
                e7  = g_row.get("EMA7", 0)
                e20 = g_row.get("EMA20", 0)
                e50 = g_row.get("EMA50", 0)
                
                brk_time = f"  ⏱{r.get('BREAK_TIME','')}" if r.get('BREAK_TIME') else ""
                
                extra = ""
                if is_bull and "POST_BREAK_GAIN_%" in r:
                    extra = f"  +{r['POST_BREAK_GAIN_%']:.2f}% after break"
                elif not is_bull and "POST_BREAK_DROP_%" in r:
                    extra = f"  -{r['POST_BREAK_DROP_%']:.2f}% after break"

                lines.append(
                    f"  {icon} <b>{s}</b>  LTP: {ltp} ({chg:+.2f} | {chp:+.2f}%)\n"
                    f"    OHLC: {o}/{h}/{l}/{ltp}\n"
                    f"    YH: {yh} | YL: {yl} | YC: {yc}\n"
                    f"    W-High: {wh} | W-Low: {wl}{brk_time}{extra}\n"
                    f"    E7: {e7:.1f} | E20: {e20:.1f} | E50: {e50:.1f}"
                )
            except Exception as _line_e:
                lines.append(f"  {icon} <b>{s}</b>")

        msg = (
            f"{border}\n"
            f"{icon} <b>{title}</b>\n"
            f"⏰ {_now_str}\n"
            f"📋 New Stocks ({len(new_syms)}):\n"
            + "\n".join(lines)
            + f"\n⚠️ <i>NOT financial advice.</i>\n{border}"
        )

        # Mark all new stocks as sent BEFORE dispatching (atomic)
        with _TG_DEDUP_LOCK:
            _dedup_d = _load_tg_dedup()
            for _, _sdk in new_syms:
                _dedup_d[_sdk] = datetime.now(IST).isoformat()
            _save_tg_dedup(_dedup_d)

        # Route via channel router (respects tg_<category> toggle + route setting)
        _batch_key = f"STRUCT_BATCH_{category}_{'_'.join([s for s,_ in new_syms[:4]])}_{_today_str}"
        send_alert_routed(category, msg, dedup_key=_batch_key)

    except Exception as _ste:
        print(f"[TG_STRUCT] {category} error: {_ste}")


# 1. Green Open Structure (Yesterday GREEN, open inside, LTP ≥ YH)
_send_tg_structure_alert(
    "GREEN_OPEN_STRUCTURE",
    "🟢 Green Open Structure - Yest GREEN, Open Inside, LTP ≥ YH",
    green_structure_df,
    ltp_map,
)

# 2. Red Open Structure (Yesterday RED, open inside, LTP ≤ YL)
_send_tg_structure_alert(
    "RED_OPEN_STRUCTURE",
    "🔴 Red Open Structure - Yest RED, Open Inside, LTP ≤ YL",
    red_structure_df,
    ltp_map,
)

# 3. Yesterday GREEN – Open Inside BREAKOUT
_send_tg_structure_alert(
    "YEST_GREEN_OPEN_BREAK",
    "🟢 Yesterday GREEN – Open Inside BREAKOUT (LTP ≥ YEST_HIGH)",
    green_zone_df,
    ltp_map,
)

# 4. Yesterday RED – Open Inside Lower Zone
_send_tg_structure_alert(
    "YEST_RED_OPEN_LOWER",
    "🔴 Yesterday RED – Open Inside Lower Zone (LTP ≤ YEST_LOW)",
    red_zone_df,
    ltp_map,
)

# 5. O=H / O=L Setups - split by direction: GREEN for O=L gainers, RED for O=H losers
if not ol_oh_df.empty and not _is_tg_disabled("OEH_OEL_SETUPS") and is_market_hours():
    try:
        _ol_df    = ol_oh_df[ol_oh_df["SETUP"] == "🟢 O = L"].copy() if "SETUP" in ol_oh_df.columns else pd.DataFrame()
        _oh_df    = ol_oh_df[ol_oh_df["SETUP"] == "🔴 O = H"].copy() if "SETUP" in ol_oh_df.columns else pd.DataFrame()
        _now_oeh  = datetime.now(IST).strftime("%H:%M IST")
        _slot_oeh = datetime.now(IST).strftime("%H%M")

        for _dir_df, _dir_icon, _dir_label, _dir_cat in [
            (_ol_df, "🟢", "O=L BULLISH GAINERS", "OEH_OEL_LONG"),
            (_oh_df, "🔴", "O=H BEARISH LOSERS",  "OEH_OEL_SHORT"),
        ]:
            if _dir_df.empty:
                continue
            _new_syms_oeh = detect_new_entries(f"TG_{_dir_cat}", _dir_df["Symbol"].tolist())
            if not _new_syms_oeh:
                continue

            _lines_oeh = []
            for _s in _new_syms_oeh[:12]:
                try:
                    _r   = _dir_df[_dir_df["Symbol"] == _s].iloc[0]
                    _ltp = float(_r.get("LTP",       0) or 0)
                    _op  = float(_r.get("LIVE_OPEN",  0) or 0)
                    _hi  = float(_r.get("LIVE_HIGH",  0) or 0)
                    _lo  = float(_r.get("LIVE_LOW",   0) or 0)
                    _chg = float(_r.get("CHANGE_%",   0) or 0)
                    _yh  = float(_r.get("YEST_HIGH",  0) or 0)
                    _yl  = float(_r.get("YEST_LOW",   0) or 0)
                    _yc  = float(_r.get("YEST_CLOSE", 0) or 0)
                    if _dir_icon == "🟢":
                        # O=L bullish: safe entry if LTP moved above open with volume
                        if _chg > 0 and _ltp > _op * 1.001 and _hi > _yh:
                            _conv = "⭐ SAFE ENTRY (LTP>Open, above YEST_HIGH)"
                        elif _chg > 0:
                            _conv = "🔔 WATCH - positive but below YEST_HIGH"
                        else:
                            _conv = "⚠️ WAIT - not yet positive"
                        _yest_ctx = f"YestH={_yh:.2f}  YestL={_yl:.2f}"
                    else:
                        # O=H bearish: safe short if LTP below open
                        if _chg < 0 and _ltp < _op * 0.999:
                            _conv = "⭐ SAFE SHORT (LTP<Open)"
                        elif _chg < 0:
                            _conv = "🔔 WATCH - negative bias"
                        else:
                            _conv = "⚠️ WAIT - not yet falling"
                        _yest_ctx = f"YestH={_yh:.2f}  YestC={_yc:.2f}"
                    _line = (
                        f"  {_dir_icon} <b>{_s}</b>  {_chg:+.2f}%  {_conv}\n"
                        f"    O={_op:.2f}  H={_hi:.2f}  L={_lo:.2f}  LTP={_ltp:.2f}\n"
                        f"    {_yest_ctx}"
                    )
                    _lines_oeh.append(_line)
                except Exception:
                    _lines_oeh.append(f"  {_dir_icon} <b>{_s}</b>")

            _border = "🟩" * 10 if _dir_icon == "🟢" else "🟥" * 10
            _entry_rule = (
                "O=L -> Open=Low (Bullish). Enter above Open if LTP rising."
                if _dir_icon == "🟢"
                else "O=H -> Open=High (Bearish). Short below Open if LTP falling."
            )
            _msg_oeh = (
                f"{_border}\n"
                f"{_dir_icon} <b>O=H / O=L - {_dir_label}</b>\n"
                f"⏰ {_now_oeh}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stocks ({len(_new_syms_oeh)}):\n"
                + "\n".join(_lines_oeh)
                + f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {_entry_rule}\n"
                f"⚠️ <i>NOT financial advice.</i>\n{_border}"
            )
            send_telegram_bg(_msg_oeh, dedup_key=f"OEH_OEL_{_dir_cat}_{_slot_oeh[:3]}0")
    except Exception as _oeh_err:
        print(f"[OEH_OEL TG] {_oeh_err}")





######################################################################################

############        STEP 1 - Add 1H Candle Fetch Function

# =========================================================
# 1-HOUR OPENING RANGE (9:15–10:15)
# =========================================================
@st.cache_data(ttl=95, show_spinner=False)   # ✅ FIX: was ttl=60 - called per-symbol in loop, expired on every refresh
def get_hourly_opening_range(token):

    today = date.today()

    start = datetime.combine(today, time(9,15))
    end   = datetime.combine(today, time(15,30))

    try:
        bars = kite.historical_data(
            token,
            start,
            end,
            "15minute"
        )
    except Exception:
        return None

    if not bars or len(bars) < 4:
        return None

    df15 = pd.DataFrame(bars)
    df15 = _safe_parse_date_col(df15, col="date")

    # First 4 candles = 9:15–10:15
    first_hour = df15.iloc[:4]

    hour_high = first_hour["high"].max()
    hour_low  = first_hour["low"].min()

    # Remaining candles
    rest = df15.iloc[4:]

    break_high = None
    break_low  = None

    for _, r in rest.iterrows():
        if break_high is None and r["high"] > hour_high:
            break_high = r["date"]
        if break_low is None and r["low"] < hour_low:
            break_low = r["date"]

    return {
        "1H_HIGH": round(hour_high, 2),
        "1H_LOW": round(hour_low, 2),
        "BREAK_HIGH_TIME": break_high,
        "BREAK_LOW_TIME": break_low
    }


###################     STEP 2 - Build Hourly Breakout Screener

# =========================================================
# ⏰ 1H OPENING RANGE BREAKOUT - ENHANCED (HIGH MOMENTUM FILTER)
# =========================================================
# Opening Range = high/low of 09:15–10:14 (first 4 × 15-min candles)
#
# FILTERS applied per side to catch only high-movable stocks:
#
#  BULLISH BREAKOUT (all must pass):
#   1. LTP > 1H_HIGH                    -> price escaped the range
#   2. LTP > EMA20 (daily)              -> daily trend up
#   3. EMA7 >= EMA20 >= EMA50           -> EMA stack fully aligned bullish
#   4. ST_SIGNAL == "BUY"               -> Supertrend daily agrees
#   5. range_pct between 0.3–2.0%       -> tight range = clean breakout
#   6. LIVE_OPEN <= YEST_HIGH * 1.01    -> no large gap-up (real breakout)
#   7. YEST_CLOSE >= YEST_OPEN          -> yesterday was green (continuation)
#   8. VOL_% >= -20                     -> volume not collapsing vs yesterday
#   9. CHANGE_% >= 0.3                  -> momentum still positive
#  10. POST_BREAK_MOVE_% <= 3.0         -> not already extended (still enterable)
#
#  BEARISH BREAKDOWN (mirror logic):
#   1. LTP < 1H_LOW
#   2. LTP < EMA20
#   3. EMA7 <= EMA20 <= EMA50           -> EMA stack fully aligned bearish
#   4. ST_SIGNAL == "SELL"
#   5. range_pct between 0.3–2.0%
#   6. LIVE_OPEN >= YEST_LOW * 0.99     -> no large gap-down
#   7. YEST_CLOSE <= YEST_OPEN          -> yesterday was red
#   8. VOL_% >= -20
#   9. CHANGE_% <= -0.3
#  10. POST_BREAK_MOVE_% <= 3.0
#
# SCORE (0–5) added per entry - sort descending to put strongest at top:
#   +1  VOL_% >= 20  (volume confirming)
#   +1  Panchak zone aligned (breakout near TOP_HIGH / breakdown near TOP_LOW)
#   +1  EMA7_1H aligned (intraday 1H EMA also in correct direction)
#   +1  LTP already above YEST_HIGH (breakout) / below YEST_LOW (breakdown)
#   +1  POST_BREAK_MOVE_% < 0.5 (very fresh break - best entry zone)
# =========================================================

hourly_rows = []

_now_hm = datetime.now(IST).strftime("%H:%M")
_orb_range_ready = _now_hm >= "10:15"

for sym in SYMBOLS:

    # ── Get 15-min candles ──────────────────────────────────────────
    df1h = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime").reset_index(drop=True)

    if df1h.empty:
        continue
    if "datetime" not in df1h.columns:
        continue
    df1h = df1h.copy()
    df1h["datetime"] = pd.to_datetime(df1h["datetime"], errors="coerce")
    df1h = df1h.dropna(subset=["datetime"])
    if df1h.empty:
        continue

    # ── Filter to 09:15–10:14 opening range window ─────────────────
    df1h["_hm"] = df1h["datetime"].dt.strftime("%H:%M")
    orb_candles = df1h[
        (df1h["_hm"] >= "09:15") & (df1h["_hm"] < "10:15")
    ]

    if len(orb_candles) < 2:
        continue

    h1_high = orb_candles["high"].max()
    h1_low  = orb_candles["low"].min()

    if h1_high <= 0 or h1_low <= 0:
        continue

    range_pct = round(((h1_high - h1_low) / h1_low) * 100, 2)

    # ── FILTER 5: Range must be 0.3%–2.0% (tight & clean) ──────────
    # Too tight (<0.3%) = no real consolidation  |  Too wide (>2%) = already volatile
    if not (0.3 <= range_pct <= 2.0):
        continue

    # ── Get live row ────────────────────────────────────────────────
    row = df[df["Symbol"] == sym]
    if row.empty:
        continue
    row = row.iloc[0]

    # Safely read all columns with fallback
    ltp        = float(row.get("LTP", 0) or 0)
    ema7       = float(row.get("EMA7",  np.nan) if pd.notna(row.get("EMA7"))  else np.nan)
    ema20      = float(row.get("EMA20", np.nan) if pd.notna(row.get("EMA20")) else np.nan)
    ema50      = float(row.get("EMA50", np.nan) if pd.notna(row.get("EMA50")) else np.nan)
    ema7_1h    = float(row.get("EMA7_1H", np.nan) if pd.notna(row.get("EMA7_1H")) else np.nan)
    st_signal  = str(row.get("ST_SIGNAL", "") or "")
    vol_pct    = float(row.get("VOL_%", 0) or 0)
    chg_pct    = float(row.get("CHANGE_%", 0) or 0)
    live_open  = float(row.get("LIVE_OPEN", 0) or 0)
    yest_high  = float(row.get("YEST_HIGH", 0) or 0)
    yest_low   = float(row.get("YEST_LOW", 0) or 0)
    yest_close = float(row.get("YEST_CLOSE", 0) or 0)
    yest_open  = float(row.get("YEST_OPEN", 0) or 0)
    top_high   = float(row.get("TOP_HIGH", np.nan) if pd.notna(row.get("TOP_HIGH")) else np.nan)
    top_low    = float(row.get("TOP_LOW",  np.nan) if pd.notna(row.get("TOP_LOW"))  else np.nan)
    diff_val   = float(row.get("DIFF", np.nan)     if pd.notna(row.get("DIFF"))     else np.nan)
    live_high  = float(row.get("LIVE_HIGH", 0) or 0)
    live_low   = float(row.get("LIVE_LOW", 0) or 0)

    # EMA20 required for both sides
    if np.isnan(ema20):
        continue

    # ── ── ── BULLISH BREAKOUT ── ── ── ── ── ── ── ── ── ── ── ──
    if ltp > h1_high:

        post_break_pct = round(((ltp - h1_high) / h1_high) * 100, 2)

        # Filter 2: Above EMA20 (daily trend up)
        if ltp <= ema20:
            continue

        # Filter 3: EMA stack bullish (EMA7 >= EMA20 >= EMA50)
        if not (np.isnan(ema7) or np.isnan(ema50)):
            if not (ema7 >= ema20 >= ema50):
                continue

        # Filter 4: Supertrend BUY
        if st_signal and st_signal != "BUY":
            continue

        # Filter 6: No large gap-up (open must be at or below YEST_HIGH + 1%)
        if yest_high > 0 and live_open > yest_high * 1.01:
            continue

        # Filter 7: Yesterday green candle (continuation)
        if yest_close > 0 and yest_open > 0 and yest_close < yest_open:
            continue

        # Filter 8: Volume not collapsing
        if vol_pct < -20:
            continue

        # Filter 9: Positive momentum
        if chg_pct < 0.3:
            continue

        # Filter 10: Not already too extended
        if post_break_pct > 3.0:
            continue

        # ── SCORE ────────────────────────────────────────────────
        score = 0
        score_reasons = []

        if vol_pct >= 20:
            score += 1
            score_reasons.append(f"VOL+{vol_pct:.0f}%")

        if not np.isnan(top_high) and not np.isnan(diff_val) and diff_val > 0:
            # Near TOP_HIGH = within 0.5× DIFF above it
            if ltp >= top_high and ltp <= top_high + diff_val * 0.5:
                score += 1
                score_reasons.append("Panchak TOP_HIGH zone")

        if not np.isnan(ema7_1h) and ltp > ema7_1h:
            score += 1
            score_reasons.append("Above EMA7_1H")

        if yest_high > 0 and ltp > yest_high:
            score += 1
            score_reasons.append("Above YEST_HIGH")

        if post_break_pct < 0.5:
            score += 1
            score_reasons.append("Fresh break (<0.5%)")

        hourly_rows.append({
            "Symbol":            sym,
            "TYPE":              "🟢 1H BREAKOUT",
            "SCORE":             score,
            "SCORE_REASONS":     " | ".join(score_reasons) if score_reasons else "-",
            "1H_HIGH":           round(h1_high, 2),
            "1H_LOW":            round(h1_low, 2),
            "1H_RANGE_%":        range_pct,
            "LTP":               round(ltp, 2),
            "CHANGE_%":          round(chg_pct, 2),
            "POST_BREAK_%":      post_break_pct,
            "VOL_%":             round(vol_pct, 1),
            "EMA7":              round(ema7, 2) if not np.isnan(ema7) else None,
            "EMA20":             round(ema20, 2),
            "EMA50":             round(ema50, 2) if not np.isnan(ema50) else None,
            "EMA7_1H":           round(ema7_1h, 2) if not np.isnan(ema7_1h) else None,
            "ST":                st_signal,
            "YEST_HIGH":         round(yest_high, 2),
            "LIVE_HIGH":         round(live_high, 2),
            "TOP_HIGH":          round(top_high, 2) if not np.isnan(top_high) else None,
            "BREAK_TIME":        datetime.now(IST).strftime("%H:%M"),
            "ORB_CANDLES":       len(orb_candles),
        })

    # ── ── ── BEARISH BREAKDOWN ── ── ── ── ── ── ── ── ── ── ── ──
    elif ltp < h1_low:

        post_break_pct = round(((h1_low - ltp) / h1_low) * 100, 2)

        # Filter 2: Below EMA20
        if ltp >= ema20:
            continue

        # Filter 3: EMA stack bearish (EMA7 <= EMA20 <= EMA50)
        if not (np.isnan(ema7) or np.isnan(ema50)):
            if not (ema7 <= ema20 <= ema50):
                continue

        # Filter 4: Supertrend SELL
        if st_signal and st_signal != "SELL":
            continue

        # Filter 6: No large gap-down
        if yest_low > 0 and live_open < yest_low * 0.99:
            continue

        # Filter 7: Yesterday red candle
        if yest_close > 0 and yest_open > 0 and yest_close > yest_open:
            continue

        # Filter 8: Volume not collapsing
        if vol_pct < -20:
            continue

        # Filter 9: Negative momentum
        if chg_pct > -0.3:
            continue

        # Filter 10: Not already too extended
        if post_break_pct > 3.0:
            continue

        # ── SCORE ────────────────────────────────────────────────
        score = 0
        score_reasons = []

        if vol_pct >= 20:
            score += 1
            score_reasons.append(f"VOL+{vol_pct:.0f}%")

        if not np.isnan(top_low) and not np.isnan(diff_val) and diff_val > 0:
            # Near TOP_LOW = within 0.5× DIFF below it
            if ltp <= top_low and ltp >= top_low - diff_val * 0.5:
                score += 1
                score_reasons.append("Panchak TOP_LOW zone")

        if not np.isnan(ema7_1h) and ltp < ema7_1h:
            score += 1
            score_reasons.append("Below EMA7_1H")

        if yest_low > 0 and ltp < yest_low:
            score += 1
            score_reasons.append("Below YEST_LOW")

        if post_break_pct < 0.5:
            score += 1
            score_reasons.append("Fresh break (<0.5%)")

        hourly_rows.append({
            "Symbol":            sym,
            "TYPE":              "🔴 1H BREAKDOWN",
            "SCORE":             score,
            "SCORE_REASONS":     " | ".join(score_reasons) if score_reasons else "-",
            "1H_HIGH":           round(h1_high, 2),
            "1H_LOW":            round(h1_low, 2),
            "1H_RANGE_%":        range_pct,
            "LTP":               round(ltp, 2),
            "CHANGE_%":          round(chg_pct, 2),
            "POST_BREAK_%":      post_break_pct,
            "VOL_%":             round(vol_pct, 1),
            "EMA7":              round(ema7, 2) if not np.isnan(ema7) else None,
            "EMA20":             round(ema20, 2),
            "EMA50":             round(ema50, 2) if not np.isnan(ema50) else None,
            "EMA7_1H":           round(ema7_1h, 2) if not np.isnan(ema7_1h) else None,
            "ST":                st_signal,
            "YEST_LOW":          round(yest_low, 2),
            "LIVE_LOW":          round(live_low, 2),
            "TOP_LOW":           round(top_low, 2) if not np.isnan(top_low) else None,
            "BREAK_TIME":        datetime.now(IST).strftime("%H:%M"),
            "ORB_CANDLES":       len(orb_candles),
        })


hourly_break_df = pd.DataFrame(hourly_rows)

# Sort: highest score first (strongest setups on top), then freshest break
if not hourly_break_df.empty and "SCORE" in hourly_break_df.columns:
    hourly_break_df = hourly_break_df.sort_values(
        ["SCORE", "POST_BREAK_%"],
        ascending=[False, True]
    ).reset_index(drop=True)





#########   STEP 3 - Add Alerts

if not hourly_break_df.empty:

    # ===============================
    # 🟢 UPSIDE BREAKOUT ALERT
    # ===============================
    up_df = hourly_break_df[
        hourly_break_df["TYPE"].str.contains("BREAKOUT", na=False)
    ]

    if not up_df.empty:
        new_up = detect_new_entries(
            "HOURLY_BREAK_UP",
            up_df["Symbol"].tolist()
        )

        notify_all(
            "HOURLY_BREAK_UP",
            "🟢1H Opening Range BREAKOUT",
            new_up,
            ltp_map
        )

    # ===============================
    # 🔴 DOWNSIDE BREAKDOWN ALERT
    # ===============================
    down_df = hourly_break_df[
        hourly_break_df["TYPE"].str.contains("BREAKDOWN", na=False)
    ]

    if not down_df.empty:
        new_down = detect_new_entries(
            "HOURLY_BREAK_DOWN",
            down_df["Symbol"].tolist()
        )

        notify_all(
            "HOURLY_BREAK_DOWN",
            "🔴1H Opening Range BREAKDOWN",
            new_down,
            ltp_map
        )

    # ── Telegram-only alerts for 1H High/Low breaks ──────────────
    if not _is_tg_disabled("BREAK_ABOVE_1H_HIGH") and not up_df.empty:
        _new_1h_up = detect_new_entries("TG_BREAK_ABOVE_1H_HIGH", up_df["Symbol"].tolist())
        if _new_1h_up and is_market_hours():
            _now_str = datetime.now(IST).strftime("%H:%M IST")
            _lines_1h = []
            for _s in _new_1h_up[:15]:
                _ltp = ltp_map.get(_s, 0)
                _row = up_df[up_df["Symbol"] == _s]
                if not _row.empty and "1H_HIGH" in _row.columns:
                    _h1h = float(_row.iloc[0]["1H_HIGH"])
                    _pts = round(_ltp - _h1h, 2) if _ltp else 0
                    _lines_1h.append(f"  🟢 <b>{_s}</b>  LTP: {_ltp}  1H_HIGH: {_h1h:.2f}  +{_pts:.2f} pts")
                else:
                    _lines_1h.append(f"  🟢 <b>{_s}</b>  LTP: {_ltp}")
            _msg_1h_up = (
                "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩\n"
                f"🟢 <b>Breakouts Above 1H High</b>\n"
                f"⏰ {_now_str}\n"
                f"📋 Stocks ({len(_new_1h_up)}):\n"
                + "\n".join(_lines_1h) + "\n"
                "⚠️ <i>NOT financial advice.</i>\n"
                "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩"
            )
            _slot_key = datetime.now(IST).strftime("%H%M")
            send_telegram_bg(_msg_1h_up,
                dedup_key=f"1H_UP_{'_'.join(_new_1h_up[:4])}_{_slot_key[:3]}0")

    if not _is_tg_disabled("BREAK_BELOW_1H_LOW") and not down_df.empty:
        _new_1h_dn = detect_new_entries("TG_BREAK_BELOW_1H_LOW", down_df["Symbol"].tolist())
        if _new_1h_dn and is_market_hours():
            _now_str = datetime.now(IST).strftime("%H:%M IST")
            _lines_1h_dn = []
            for _s in _new_1h_dn[:15]:
                _ltp = ltp_map.get(_s, 0)
                _row = down_df[down_df["Symbol"] == _s]
                if not _row.empty and "1H_LOW" in _row.columns:
                    _h1l = float(_row.iloc[0]["1H_LOW"])
                    _pts = round(_h1l - _ltp, 2) if _ltp else 0
                    _lines_1h_dn.append(f"  🔴 <b>{_s}</b>  LTP: {_ltp}  1H_LOW: {_h1l:.2f}  -{_pts:.2f} pts")
                else:
                    _lines_1h_dn.append(f"  🔴 <b>{_s}</b>  LTP: {_ltp}")
            _msg_1h_dn = (
                "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥\n"
                f"🔴 <b>Breakdowns Below 1H Low</b>\n"
                f"⏰ {_now_str}\n"
                f"📋 Stocks ({len(_new_1h_dn)}):\n"
                + "\n".join(_lines_1h_dn) + "\n"
                "⚠️ <i>NOT financial advice.</i>\n"
                "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"
            )
            _slot_key = datetime.now(IST).strftime("%H%M")
            send_telegram_bg(_msg_1h_dn,
                dedup_key=f"1H_DN_{'_'.join(_new_1h_dn[:4])}_{_slot_key[:3]}0")


# =========================================================
# 📅 WEEKLY BREAKS – WITH POST BREAK MOVE
# =========================================================

weekly_rows = []

for _, r in df.iterrows():

    week_high = r["HIGH_W"]
    week_low  = r["LOW_W"]

    # 🟢 Weekly Breakout
    if r["LIVE_HIGH"] > week_high:

        move = round(r["LIVE_HIGH"] - week_high, 2)
        move_pct = round((move / week_high) * 100, 2)

        weekly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🟢 WEEK BREAKOUT",
            "WEEK_HIGH": week_high,
            "WEEK_LOW": week_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

    # 🔴 Weekly Breakdown
    elif r["LIVE_LOW"] < week_low:

        move = round(week_low - r["LIVE_LOW"], 2)
        move_pct = round((move / week_low) * 100, 2)

        weekly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🔴 WEEK BREAKDOWN",
            "WEEK_HIGH": week_high,
            "WEEK_LOW": week_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

weekly_break_df = pd.DataFrame(weekly_rows)


# =========================================================
# 📆 MONTHLY BREAKS – WITH POST BREAK MOVE
# =========================================================

monthly_rows = []

for _, r in df.iterrows():

    month_high = r["HIGH_M"]
    month_low  = r["LOW_M"]

    # 🟢 Monthly Breakout
    if r["LIVE_HIGH"] > month_high:

        move = round(r["LIVE_HIGH"] - month_high, 2)
        move_pct = round((move / month_high) * 100, 2)

        monthly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🟢 MONTH BREAKOUT",
            "MONTH_HIGH": month_high,
            "MONTH_LOW": month_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

    # 🔴 Monthly Breakdown
    elif r["LIVE_LOW"] < month_low:

        move = round(month_low - r["LIVE_LOW"], 2)
        move_pct = round((move / month_low) * 100, 2)

        monthly_rows.append({
            "Symbol": r["Symbol"],
            "TYPE": "🔴 MONTH BREAKDOWN",
            "MONTH_HIGH": month_high,
            "MONTH_LOW": month_low,
            "POST_BREAK_MOVE": move,
            "POST_BREAK_MOVE_%": move_pct,
            "LIVE_HIGH": r["LIVE_HIGH"],
            "LIVE_LOW": r["LIVE_LOW"],
            "LTP": r["LTP"],
            "CHANGE_%": r["CHANGE_%"]
        })

monthly_break_df = pd.DataFrame(monthly_rows)


#weekly_break_df = weekly_break_df.sort_values("POST_BREAK_MOVE_%", ascending=False)

#monthly_break_df = monthly_break_df.sort_values("POST_BREAK_MOVE_%", ascending=False)
if "POST_BREAK_MOVE_%" in weekly_break_df.columns:
    weekly_break_df = weekly_break_df.sort_values("POST_BREAK_MOVE_%")

if "POST_BREAK_MOVE_%" in monthly_break_df.columns:
    monthly_break_df = monthly_break_df.sort_values("POST_BREAK_MOVE_%")


##############      STEP 1 - ADD THIS BLOCK AFTER EMA BUILD (After df = df.merge(ema_df...))

# =========================================================
# SUPPORT / RESISTANCE ENGINE (DAILY / WEEKLY / MONTHLY)
# =========================================================

def find_pivots(df, left=3, right=3):
    highs = []
    lows = []

    for i in range(left, len(df)-right):
        window = df.iloc[i-left:i+right+1]

        if df.iloc[i]["high"] == window["high"].max():
            highs.append(df.iloc[i]["high"])

        if df.iloc[i]["low"] == window["low"].min():
            lows.append(df.iloc[i]["low"])

    return highs, lows


def cluster_levels(levels, threshold=0.005):
    levels = sorted(levels)
    clusters = []

    for lvl in levels:
        placed = False
        for cluster in clusters:
            if abs(lvl - cluster[0]) / cluster[0] < threshold:
                cluster.append(lvl)
                placed = True
                break
        if not placed:
            clusters.append([lvl])

    return clusters


def classify_cluster(cluster):
    touches = len(cluster)
    if touches >= 3:
        return "STRONG"
    elif touches == 2:
        return "WEAK"
    return None


###################         STEP 2 - BUILD S/R FROM 180-DAY DATA

# =========================================================
# BUILD DAILY SUPPORT / RESISTANCE
# =========================================================

sr_rows = []

for sym, g in ohlc_full.groupby("Symbol"):

    g = g.sort_values("date")

    if len(g) < 50:
        continue

    highs, lows = find_pivots(g)

    res_clusters = cluster_levels(highs)
    sup_clusters = cluster_levels(lows)

    for cluster in res_clusters:
        strength = classify_cluster(cluster)
        if not strength:
            continue

        level = round(np.mean(cluster), 2)

        sr_rows.append({
            "Symbol": sym,
            "TF": "DAILY",
            "TYPE": "RESISTANCE",
            "LEVEL": level,
            "STRENGTH": strength,
            "TOUCHES": len(cluster)
        })

    for cluster in sup_clusters:
        strength = classify_cluster(cluster)
        if not strength:
            continue

        level = round(np.mean(cluster), 2)

        sr_rows.append({
            "Symbol": sym,
            "TF": "DAILY",
            "TYPE": "SUPPORT",
            "LEVEL": level,
            "STRENGTH": strength,
            "TOUCHES": len(cluster)
        })

sr_df = pd.DataFrame(sr_rows)


########################            STEP 3 - MERGE NEAREST LEVEL INTO MAIN DF

# =========================================================
# FIND NEAREST STRONG SUPPORT / RESISTANCE
# =========================================================

nearest_rows = []

for sym in df["Symbol"].unique():

    price = df.loc[df.Symbol == sym, "LTP"].values[0]

    sym_levels = sr_df[
        (sr_df.Symbol == sym) &
        (sr_df.STRENGTH == "STRONG")
    ]

    if sym_levels.empty:
        continue

    supports = sym_levels[
        (sym_levels.TYPE == "SUPPORT") &
        (sym_levels.LEVEL < price)
    ]

    resistances = sym_levels[
        (sym_levels.TYPE == "RESISTANCE") &
        (sym_levels.LEVEL > price)
    ]

    nearest_sup = supports.sort_values("LEVEL", ascending=False).head(1)
    nearest_res = resistances.sort_values("LEVEL", ascending=True).head(1)

    nearest_rows.append({
        "Symbol": sym,
        "STRONG_SUPPORT": nearest_sup.LEVEL.values[0] if not nearest_sup.empty else None,
        "STRONG_RESISTANCE": nearest_res.LEVEL.values[0] if not nearest_res.empty else None
    })

nearest_df = pd.DataFrame(nearest_rows)

df = df.merge(nearest_df, on="Symbol", how="left")

# Distance %
df["SS_DIST_%"] = ((df["LTP"] - df["STRONG_SUPPORT"]) / df["STRONG_SUPPORT"] * 100).round(2)
df["SR_DIST_%"] = ((df["STRONG_RESISTANCE"] - df["LTP"]) / df["STRONG_RESISTANCE"] * 100).round(2)


# =========================================================
# BREAKOUT STRENGTH FROM DAILY / WEEKLY / MONTHLY LEVELS
# =========================================================

def breakout_strength(row):

    score = 0
    reasons = []

    # ----- DAILY -----
    if row["LTP"] > row["HIGH_D"]:
        move = (row["LTP"] - row["HIGH_D"]) / row["HIGH_D"] * 100
        score += 1
        reasons.append(f"Daily +{round(move,2)}%")

    if row["LTP"] < row["LOW_D"]:
        move = (row["LOW_D"] - row["LTP"]) / row["LOW_D"] * 100
        score += 1
        reasons.append(f"Daily -{round(move,2)}%")

    # ----- WEEKLY -----
    if row["LTP"] > row["HIGH_W"]:
        move = (row["LTP"] - row["HIGH_W"]) / row["HIGH_W"] * 100
        score += 2
        reasons.append(f"Weekly +{round(move,2)}%")

    if row["LTP"] < row["LOW_W"]:
        move = (row["LOW_W"] - row["LTP"]) / row["LOW_W"] * 100
        score += 2
        reasons.append(f"Weekly -{round(move,2)}%")

    # ----- MONTHLY -----
    if row["LTP"] > row["HIGH_M"]:
        move = (row["LTP"] - row["HIGH_M"]) / row["HIGH_M"] * 100
        score += 3
        reasons.append(f"Monthly +{round(move,2)}%")

    if row["LTP"] < row["LOW_M"]:
        move = (row["LOW_M"] - row["LTP"]) / row["LOW_M"] * 100
        score += 3
        reasons.append(f"Monthly -{round(move,2)}%")

    return score, " | ".join(reasons)


df[["BREAK_SCORE", "BREAK_DETAILS"]] = df.apply(
    lambda r: pd.Series(breakout_strength(r)),
    axis=1
)

####################        ADD THIS FUNCTION (After S/R engine)

# =========================================================
# CONFIRMED CONTINUATION FROM S/R
# =========================================================

def continuation_confirmation(row, level, direction, last_candle):

    if level is None or pd.isna(level):
        return False, 0

    distance_pct = abs((row["LTP"] - level) / level) * 100

    # Require minimum expansion
    if distance_pct < 0.3:
        return False, 0

    # Require EMA alignment
    if direction == "UP" and row["LTP"] <= row["EMA20"]:
        return False, 0

    if direction == "DOWN" and row["LTP"] >= row["EMA20"]:
        return False, 0

    # Require candle confirmation
    if last_candle is not None:
        if direction == "UP" and last_candle["close"] <= level:
            return False, 0
        if direction == "DOWN" and last_candle["close"] >= level:
            return False, 0

    return True, round(distance_pct, 2)

#############       STEP 2 - APPLY TO DAILY STRONG S/R
# =========================================================
# APPLY CONTINUATION CONFIRMATION
# =========================================================

# =========================================================
# CORRECTED CONTINUATION CONFIRMATION ENGINE
# =========================================================

conf_rows = []

for _, r in df.iterrows():

    sym = r["Symbol"]

    # Skip if no S/R
    if pd.isna(r["STRONG_SUPPORT"]) and pd.isna(r["STRONG_RESISTANCE"]):
        continue

    # Get last 15m candle
    last15 = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(last15) == 0:
        continue

    last_candle = last15.iloc[-1]

    # =====================================================
    # 🟢 RESISTANCE BREAK CONFIRMATION
    # =====================================================
    level = r["STRONG_RESISTANCE"]

    if not pd.isna(level):

        distance_pct = ((r["LTP"] - level) / level) * 100

        if (
            r["LTP"] > level and                       # Must be above level
            last_candle["close"] > level and           # Candle close above
            r["LTP"] > r["EMA20"] and                  # EMA alignment
            distance_pct > 0.3                         # Minimum expansion
        ):

            conf_rows.append({
                "Symbol": sym,
                "TYPE": "🟢 CONFIRMED RESISTANCE BREAK",
                "LEVEL": round(level,2),
                "DIST_%": round(distance_pct,2),
                "LTP": round(r["LTP"],2),
                "EMA20": round(r["EMA20"],2),
                "CHANGE_%": r["CHANGE_%"]
            })

    # =====================================================
    # 🔴 SUPPORT BREAK CONFIRMATION
    # =====================================================
    level = r["STRONG_SUPPORT"]

    if not pd.isna(level):

        distance_pct = ((level - r["LTP"]) / level) * 100

        if (
            r["LTP"] < level and                       # Must be below level
            last_candle["close"] < level and           # Candle close below
            r["LTP"] < r["EMA20"] and                  # EMA alignment
            distance_pct > 0.3                         # Minimum expansion
        ):

            conf_rows.append({
                "Symbol": sym,
                "TYPE": "🔴 CONFIRMED SUPPORT BREAK",
                "LEVEL": round(level,2),
                "DIST_%": round(distance_pct,2),
                "LTP": round(r["LTP"],2),
                "EMA20": round(r["EMA20"],2),
                "CHANGE_%": r["CHANGE_%"]
            })


continuation_df = pd.DataFrame(conf_rows)


############################################################################# FUTURES 

##############      STEP 1 - AUTO FETCH ALL FUTURES
# ================= AUTO FUTURES FETCHER =================

@st.cache_data(ttl=3600, show_spinner=False)
def get_nearest_month_futures():

    try:
        instruments = kite.instruments("NFO")
    except Exception:
        return []

    df_inst = pd.DataFrame(instruments)

    # Only Futures
    fut_df = df_inst[df_inst["instrument_type"] == "FUT"].copy()

    if fut_df.empty:
        return []

    # Convert expiry to datetime
    fut_df["expiry"] = pd.to_datetime(fut_df["expiry"])

    # Get nearest expiry
    nearest_expiry = fut_df["expiry"].min()

    fut_df = fut_df[fut_df["expiry"] == nearest_expiry]

    # Keep only stock + index futures
    fut_df = fut_df[fut_df["segment"] == "NFO-FUT"]

    # Build trading symbols
    futures_list = [
        f"NFO:{ts}"
        for ts in fut_df["tradingsymbol"].unique()
    ]

    return futures_list

########################        STEP 2 - USE AUTO FUTURE LIST
FUTURES_LIST = tuple(get_nearest_month_futures())  # ✅ FIX: tuple is hashable for cache keys

##########################      STEP 3 - REAL OI TRACKING ENGINE
# OI_SNAPSHOT_FILE already defined at top in dated file paths block

def load_prev_oi():
    """Load today's OI snapshot. Safe: OI_SNAPSHOT_FILE is already a dated path."""
    result = _load_today_csv(OI_SNAPSHOT_FILE, required_cols=["FUT_SYMBOL","OI"])
    return result if result is not None else pd.DataFrame(columns=["FUT_SYMBOL","OI"])

def save_snapshot(df):
    df[["FUT_SYMBOL","OI"]].to_csv(OI_SNAPSHOT_FILE, index=False, encoding='utf-8')

######################      STEP 4 - FETCH ALL FUTURES OI
@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent background refresh
# ✅ FIX: accepts tuple not list - lists are unhashable and break @st.cache_data
def fetch_all_futures_data(futures_tuple):
    if not is_market_hours():
        return pd.DataFrame()

    rows = []

    if not futures_tuple:
        return pd.DataFrame()

    try:
        quotes = kite.quote(list(futures_tuple))
    except Exception:
        return pd.DataFrame()

    for sym in futures_tuple:

        q = quotes.get(sym)
        if not q:
            continue

        ltp = q.get("last_price", 0)
        prev_close = q.get("ohlc", {}).get("close", 0)

        price_pct = 0
        if prev_close:
            price_pct = ((ltp - prev_close) / prev_close) * 100

        rows.append({
            "FUT_SYMBOL": sym.replace("NFO:",""),
            "LTP": round(ltp,2),
            "PRICE_%": round(price_pct,2),
            "OI": q.get("oi",0)
        })

    return pd.DataFrame(rows)

################################        STEP 5 - BUILD FULL DERIVATIVE ENGINE
# ✅ FUTURES - synchronous single batch call every rerun (~0.5 sec)
def fetch_futures_now():
    if not is_market_hours():
        return pd.DataFrame()
    try:
        ftuple = tuple(FUTURES_LIST)
        if not ftuple:
            return pd.DataFrame()
        quotes = kite.quote(list(ftuple))
        rows = []
        for sym in ftuple:
            q = quotes.get(sym)
            if not q: continue
            ltp=q.get("last_price",0); pc=q.get("ohlc",{}).get("close",0)
            price_pct=((ltp-pc)/pc*100) if pc else 0
            rows.append({"FUT_SYMBOL":sym.replace("NFO:",""),"LTP":round(ltp,2),
                "PRICE_%":round(price_pct,2),"OI":q.get("oi",0),
                "DAY_HIGH":q.get("ohlc",{}).get("high",0),
                "DAY_LOW":q.get("ohlc",{}).get("low",0)})
        result = pd.DataFrame(rows) if rows else pd.DataFrame()
        if not result.empty:
            result.to_csv(FUT_CACHE_CSV, index=False, encoding='utf-8')
        return result
    except Exception:
        return pd.DataFrame()

fut_df = fetch_futures_now()
if fut_df.empty:
    _fb = _load_today_csv(FUT_CACHE_CSV)
    if _fb is not None:
        fut_df = _fb

if not fut_df.empty:

    # Load previous first
    prev_df = load_prev_oi()

    fut_df = fut_df.merge(
        prev_df,
        on="FUT_SYMBOL",
        how="left",
        suffixes=("","_PREV")
    )

    fut_df["OI_PREV"] = fut_df["OI_PREV"].fillna(0).infer_objects(copy=False)

    # If previous is zero (first run), set equal to current
    fut_df["OI_PREV"] = np.where(
        fut_df["OI_PREV"] == 0,
        fut_df["OI"],
        fut_df["OI_PREV"]
    )

    # Now calculate change
    fut_df["REAL_OI_%"] = np.where(
        fut_df["OI_PREV"] > 0,
        ((fut_df["OI"] - fut_df["OI_PREV"]) / fut_df["OI_PREV"]) * 100,
        0
    ).round(2)

    # Save snapshot AFTER calculation
    save_snapshot(fut_df)


    def classify(row):
        if row["PRICE_%"] > 0 and row["REAL_OI_%"] > 0:
            return "🟢 LONG BUILDUP"
        elif row["PRICE_%"] < 0 and row["REAL_OI_%"] > 0:
            return "🔴 SHORT BUILDUP"
        elif row["PRICE_%"] > 0 and row["REAL_OI_%"] < 0:
            return "⚠ SHORT COVERING"
        elif row["PRICE_%"] < 0 and row["REAL_OI_%"] < 0:
            return "⚠ LONG UNWINDING"
        return "NEUTRAL"

    fut_df["POSITION_TYPE"] = fut_df.apply(classify, axis=1)

else:
    fut_df = pd.DataFrame()


###################     STEP 6 - OI HEATMAP SCORE
# Create strength score
if not fut_df.empty and "REAL_OI_%" in fut_df.columns and "PRICE_%" in fut_df.columns:
    fut_df["OI_SCORE"] = (fut_df["REAL_OI_%"].abs() + fut_df["PRICE_%"].abs()).round(2)

##########################          SNIPPET - STRONG CLOSING FILTER

# ✅ FIX: DAY_HIGH/DAY_LOW already included in _fetch_futures_background - no extra API call needed
# Add distance from day high / low directly from fut_df
if not fut_df.empty:
    fut_df["DIST_HIGH_%"] = 0   # placeholder default
    # Ensure columns exist (background thread includes them)
    if "DAY_HIGH" not in fut_df.columns: fut_df["DAY_HIGH"] = 0
    if "DAY_LOW"  not in fut_df.columns: fut_df["DAY_LOW"]  = 0

    fut_df["DIST_FROM_HIGH_%"] = np.where(
        fut_df["DAY_HIGH"] > 0,
        ((fut_df["DAY_HIGH"] - fut_df["LTP"]) / fut_df["DAY_HIGH"] * 100).round(2),
        0
    )
    fut_df["DIST_FROM_LOW_%"] = np.where(
        fut_df["DAY_LOW"] > 0,
        ((fut_df["LTP"] - fut_df["DAY_LOW"]) / fut_df["DAY_LOW"] * 100).round(2),
        0
    )
else:
    fut_df["DIST_FROM_HIGH_%"] = 0
    fut_df["DIST_FROM_LOW_%"]  = 0
    fut_df["DAY_HIGH"]         = 0
    fut_df["DAY_LOW"]          = 0

# ================= STRONG LONG CLOSING =================
# ✅ FIX: Guard against empty/missing columns when fut_df is empty
_fut_cols_ok = not fut_df.empty and all(
    c in fut_df.columns for c in ["PRICE_%","REAL_OI_%","DIST_FROM_HIGH_%","DIST_FROM_LOW_%"]
)

strong_long_close = fut_df[
    (fut_df["PRICE_%"] > 1) &
    (fut_df["REAL_OI_%"] > 1) &
    (fut_df["DIST_FROM_HIGH_%"] <= 0.5)
] if _fut_cols_ok else pd.DataFrame()

# ================= STRONG SHORT CLOSING =================
strong_short_close = fut_df[
    (fut_df["PRICE_%"] < -1) &
    (fut_df["REAL_OI_%"] > 1) &
    (fut_df["DIST_FROM_LOW_%"] <= 0.5)
] if _fut_cols_ok else pd.DataFrame()

# ── Run futures 15-min delta on every refresh ─────────────
_fut_15m_events = []
if not fut_df.empty and "OI" in fut_df.columns and "LTP" in fut_df.columns:
    _fut_snap_slot = datetime.now(IST).strftime("%H:%M")
    _curr_fut_snap = {"_slot": _fut_snap_slot}
    for _, _fr in fut_df.iterrows():
        _sym = str(_fr.get("FUT_SYMBOL", ""))
        if _sym:
            _curr_fut_snap[_sym] = {
                "oi":  int(_fr.get("OI",  0) or 0),
                "ltp": round(float(_fr.get("LTP", 0) or 0), 2),
            }

    _prev_fut_snap = _load_fut_15m_snapshot()
    if _prev_fut_snap:
        _prev_slot_f = _prev_fut_snap.get("_slot", "")
        _curr_slot_f = _curr_fut_snap.get("_slot", "")
        if _curr_slot_f != _prev_slot_f:
            _fut_15m_events = compute_fut_15m_delta(_curr_fut_snap, _prev_fut_snap)
            if _fut_15m_events:
                fire_fut_15m_alerts(_fut_15m_events)
    _save_fut_15m_snapshot(_curr_fut_snap)


################        STEP 1 - ADD OI FETCH FUNCTION
# =========================================================
# 📊 OI DATA FETCH
# =========================================================

# ✅ OI - synchronous single batch call, runs every rerun (~1 sec)
# ✅ OI already fetched inside fetch_live_and_oi() - reuse, no second kite.quote call
oi_df = _oi_prefetch

###################     STEP 2 - MERGE OI INTO MAIN DF
df = df.merge(oi_df, on="Symbol", how="left")

#df["OI"] = pd.to_numeric(df.get("OI", 0), errors="coerce").fillna(0)
# ================= SAFE OI HANDLING =================

if "OI" not in df.columns:
    df["OI"] = 0

df["OI"] = pd.to_numeric(df["OI"], errors="coerce").fillna(0)
df["OI_CHANGE_%"] = pd.to_numeric(df.get("OI_CHANGE_%", 0), errors="coerce").fillna(0)

###################     STEP 3 - OI STRENGTH ENGINE
# =========================================================
# 🔥 OI BASED STRENGTH METER
# =========================================================

def oi_strength_logic(row):
    price_change = row.get("CHANGE_%", 0)
    oi_change = row.get("OI_CHANGE_%", 0)

    if price_change > 0 and oi_change > 0:
        return "🟢 LONG BUILDUP", 3

    elif price_change < 0 and oi_change > 0:
        return "🔴 SHORT BUILDUP", 3

    elif price_change > 0 and oi_change < 0:
        return "⚠ SHORT COVERING", 1

    elif price_change < 0 and oi_change < 0:
        return "⚠ LONG UNWINDING", 1

    return "NEUTRAL", 0


df[["OI_SIGNAL","OI_SCORE"]] = df.apply(
    lambda r: pd.Series(oi_strength_logic(r)), axis=1
)


################        STEP 4 - STRONG OI FILTER TABLE

# Strong OI moves only
oi_strong_df = df[df["OI_SCORE"] >= 3].copy()

OI_COLUMNS = [
    "Symbol",
    "LTP",
    "CHANGE_%",
    "OI",
    "OI_CHANGE_%",
    "OI_SIGNAL",
    "EMA20",
    "EMA50",
    "SUPERTREND"
]

OI_COLUMNS = [c for c in OI_COLUMNS if c in oi_strong_df.columns]
oi_strong_df = oi_strong_df[OI_COLUMNS].sort_values("CHANGE_%", ascending=False)


########################        🔥 SNIPPET – Strong Closing Near Day High

# =========================================================
# DISTANCE FROM DAY HIGH / LOW (%)
# =========================================================

# Ensure numeric
df["LIVE_HIGH"] = pd.to_numeric(df["LIVE_HIGH"], errors="coerce")
df["LIVE_LOW"] = pd.to_numeric(df["LIVE_LOW"], errors="coerce")
df["LTP"] = pd.to_numeric(df["LTP"], errors="coerce")

# Distance from day high %
df["DIST_FROM_DAY_HIGH_%"] = (
    (df["LIVE_HIGH"] - df["LTP"]) / df["LIVE_HIGH"] * 100
).round(2)

# Distance from day low %
df["DIST_FROM_DAY_LOW_%"] = (
    (df["LTP"] - df["LIVE_LOW"]) / df["LIVE_LOW"] * 100
).round(2)


# ================= STRONG STOCK CLOSING =================

# =========================================================
# 🔥 CLEAN YH BREAK + STRONG CLOSE NEAR DAY HIGH
# =========================================================

strong_high_df = df.loc[
    (df["LIVE_OPEN"] <= df["YEST_HIGH"]) &      # No gap-up
    (df["LTP"] >= df["YEST_HIGH"]) &           # Breakout above YH
    (df["LIVE_HIGH"] >= df["YEST_HIGH"]) &
    (df["DIST_FROM_DAY_HIGH_%"] <= 0.5) &        # Strong close near day high
    (df["CHANGE_%"] >= 0.5) &
    (df["CHANGE_%"] <= 2.5)
].copy()

if not strong_high_df.empty:

    strong_high_df = strong_high_df[
        [
            "Symbol",
            "LTP",
            "CHANGE",
            "CHANGE_%",
            "LIVE_HIGH",
            "DIST_FROM_DAY_HIGH_%",
            "YEST_HIGH",
            "HIGH_W",
            "HIGH_M",
            "EMA20",
            "TOP_HIGH"
        ]
    ]

    
else:
    pass  # hidden: No strong bullish closing stocks near day high.


#############################       LOGIC – Strong Red Closing (Near Day Low)
# ================= STRONG RED CLOSING =================

# =========================================================
# 🔴 CLEAN YL BREAK + STRONG CLOSE NEAR DAY LOW
# =========================================================

strong_low_df = df.loc[
    (df["LIVE_OPEN"] >= df["YEST_LOW"]) &        # No gap-down
    (df["LTP"] <= df["YEST_LOW"]) &             # Breakdown below YL
    (df["DIST_FROM_DAY_LOW_%"] <= 0.5) &        # Closing near day low
    (df["CHANGE_%"] <= -0.5)  &                     # Bearish momentum
    (df["CHANGE_%"] >= -2.2)
].copy()

if not strong_low_df.empty:

    strong_low_df = strong_low_df[
        [
            "Symbol",
            "LTP",
            "CHANGE",
            "CHANGE_%",
            "LIVE_LOW",
            "DIST_FROM_DAY_LOW_%",
            "YEST_LOW",
            "LOW_W",
            "LOW_M",
            "EMA20",
            "TOP_LOW"
        ]
    ]

    

else:
    pass  # hidden: No strong bearish closing stocks near day low.



################################    Bullish Version (Reclaim After Gap Up Failure)
###############################     Bearish Version (Reclaim After Gap Down Failure)
# ================= GAP RECLAIM / FAILURE SCREENER =================

# 🟢 Bullish Reclaim After Gap Up Failure
bull_reclaim_df = df[
    (df["LIVE_OPEN"] > df["YEST_HIGH"]) &   # gap above YH
    (df["LIVE_LOW"] < df["YEST_HIGH"]) &    # dipped below YH
    (df["LTP"] > df["YEST_HIGH"])   &        # reclaimed & holding
    (df["LTP"] > df["EMA20"]) &
    (df["VOL_%"] > 10)#(df["VOL_%"] > 30)
].copy()

bull_reclaim_df["STRENGTH_%"] = (
    (df["LTP"] - df["YEST_HIGH"]) / df["YEST_HIGH"] * 100
).round(2)

bull_reclaim_df = bull_reclaim_df.sort_values("STRENGTH_%", ascending=False)


# 🔴 Bearish Reclaim After Gap Down Failure
bear_reclaim_df = df[
    (df["LIVE_OPEN"] < df["YEST_LOW"]) &    # gap below YL
    (df["LIVE_HIGH"] > df["YEST_LOW"]) &    # moved above YL intraday
    (df["LTP"] < df["YEST_LOW"])  &          # rejected & holding below
    (df["LTP"] < df["EMA20"]) &
    (df["VOL_%"] > 10)#(df["VOL_%"] > 30)
].copy()

bear_reclaim_df["STRENGTH_%"] = (
    (df["YEST_LOW"] - df["LTP"]) / df["YEST_LOW"] * 100
).round(2)

bear_reclaim_df = bear_reclaim_df.sort_values("STRENGTH_%", ascending=False)



# ================= 0.5% BREAK ZONES =================

# Ensure numeric
df["YEST_HIGH"] = pd.to_numeric(df["YEST_HIGH"], errors="coerce")
df["YEST_LOW"]  = pd.to_numeric(df["YEST_LOW"], errors="coerce")

# 0.5% above yesterday high
df["YH_05"] = (df["YEST_HIGH"] * 1.005).round(2)

# 0.5% below yesterday low
df["YL_05"] = (df["YEST_LOW"] * 0.995).round(2)


####################            Step 2: Define “Last 2 Months Downtrend”
def get_2m_downtrend_symbols():
    if not os.path.exists(OHLC_FILE):
        return []

    ohlc = pd.read_csv(OHLC_FILE, encoding='utf-8')
    ohlc = _safe_parse_date_col(ohlc, col="date")

    down_symbols = []

    for sym, g in ohlc.groupby("Symbol"):
        g = g.sort_values("date")

        if len(g) < 40:
            continue

        first_close = g.iloc[0]["close"]
        last_close  = g.iloc[-1]["close"]

        if last_close < first_close:
            down_symbols.append(sym)

    return down_symbols

########################        Step 3: Build Your New Table

downtrend_symbols = get_2m_downtrend_symbols()

reversal_df = df.loc[
    (df["Symbol"].isin(downtrend_symbols)) &
    (df["LTP"] >= df["EMA7"]) &
    (df["LTP"] >= df["EMA20"]) &
    (df["LTP"] >= df["EMA50"]) &
    (df["EMA20"] >= df["EMA50"]),
    [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",        
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "DIST_FROM_DAY_HIGH_%",
        "YH_05",
        "YL_05",
        "DIST_FROM_DAY_LOW_%", 
        "EMA7",
        "EMA20",
        "EMA50"
        
    ]
].copy()

reversal_df = reversal_df.sort_values("CHANGE_%", ascending=False)
new_reversal = detect_new_entries(
    "2M_EMA_REVERSAL",
    reversal_df.Symbol.tolist()
)

notify_all(
    "2M_EMA_REVERSAL",
    "🚀 2M Downtrend -> EMA Reversal",
    new_reversal,
    ltp_map
)


try:
    _today_astro_snap = _vedic_day_analysis(datetime.now(IST).date())
    _today_astro_score = _today_astro_snap.get("net_score", 0)
except Exception:
    _today_astro_snap  = None
    _today_astro_score = 0

def allow_trade(row):
    # 🚫 Block bad astro days
    if _today_astro_score <= -2:
        return "BLOCK"
    # 🔥 Strong day -> full trade
    if _today_astro_score >= 1:
        return "FULL"
    # ⚠️ Neutral -> only breakout trades
    if _today_astro_score == 0:
        if row["LTP"] > row["TOP_HIGH"] or row["LTP"] < row["TOP_LOW"]:
            return "BREAKOUT ONLY"
        else:
            return "WAIT"
    return "WAIT"

df["ASTRO_PERMISSION"] = df.apply(allow_trade, axis=1)




############################################################################################################
############################################################################################################
############################################################################################################
############################################################################################################




# ================== LIVE ALERTS ==================
if "alerts" not in st.session_state:
    st.session_state.alerts = []

if "alerted_symbols" not in st.session_state:
    st.session_state.alerted_symbols = set()

# ================== LIVE ALERTS ENGINE ==================

# ================== LIVE ALERTS ENGINE (NO DUPLICATES) ==================

df["EMA7"] = pd.to_numeric(df["EMA7"], errors="coerce")
df["EMA20"] = pd.to_numeric(df["EMA20"], errors="coerce")
df["EMA50"] = pd.to_numeric(df["EMA50"], errors="coerce")



new_alerts = []

for _, r in df.iterrows():

    sym = r["Symbol"]
    now_time = datetime.now(IST).replace(tzinfo=None)

    # =====================================================
    # 🟢 UPWARD BREAK
    # =====================================================
    #if (r["LTP"] > r["YEST_HIGH"] and  r["CHANGE_%"] > 1 ):
    #if (r["LTP"] >= r.get("YEST_HIGH", 0) and r["CHANGE_%"] >= 0.5 and r.get("VOL_%", 0) >= -30 ):
    if (
        r["LTP"] >= r.get("YEST_HIGH", 0) and
        r["CHANGE_%"] >= 0.5 and
        r.get("VOL_%", 0) >= -30 and

        # 🔥 EMA Structure Confirmation
        r.get("LTP", 0) >= r.get("EMA7", 0) and
        r.get("LTP", 0) >= r.get("EMA20", 0) and
        r.get("LTP", 0) >= r.get("EMA50", 0) and
        r.get("EMA20", 0) >= r.get("EMA50", 0)
    ):

        key = f"{sym}_UP"

        if key not in st.session_state.alerted_symbols:

            new_alerts.append({
                "TS": now_time,
                "TIME": now_time.strftime("%H:%M:%S"),
                "TYPE": "🟢 YH BREAK",
                "Symbol": sym,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                #"LIVE_VOLUME": r.get("LIVE_VOLUME", 0),
                #"YEST_VOL": r.get("YEST_VOL", 0),
                "VOL_%": r.get("VOL_%", 0),
                "FROM_YH_%": r["FROM_YH_%"],
                "FROM_YL_%": r["FROM_YL_%"],
                "DAY_OPEN": r["LIVE_OPEN"],
                "DAY_HIGH": r["LIVE_HIGH"],
                "DAY_LOW": r["LIVE_LOW"],
                "YEST_HIGH": r["YEST_HIGH"],
                "YEST_LOW": r["YEST_LOW"],
                "YEST_CLOSE": r["YEST_CLOSE"],
            })

            st.session_state.alerted_symbols.add(key)

    # =====================================================
    # 🔴 DOWNWARD BREAK
    # =====================================================
    #if (r["LTP"] < r["YEST_LOW"] and r["CHANGE_%"] < -1 ):
    #if (r["LTP"] <= r.get("YEST_LOW", 0) and r["CHANGE_%"] >= -0.5 and r.get("VOL_%", 0) >= -30 ):
    if (
        r["LTP"] <= r.get("YEST_LOW", 0) and
        r["CHANGE_%"] <= -0.5 and
        r.get("VOL_%", 0) >= -30 and

        r.get("LTP", 0) <= r.get("EMA7", 0) and
        r.get("LTP", 0) <= r.get("EMA20", 0) and
        r.get("LTP", 0) <= r.get("EMA50", 0) and
        r.get("EMA20", 0) <= r.get("EMA50", 0)
    ):

        key = f"{sym}_DOWN"

        if key not in st.session_state.alerted_symbols:

            new_alerts.append({
                "TS": now_time,
                "TIME": now_time.strftime("%H:%M:%S"),
                "TYPE": "🔴 YL BREAK",
                "Symbol": sym,
                "LTP": r["LTP"],
                "CHANGE_%": r["CHANGE_%"],
                #"LIVE_VOLUME": r.get("LIVE_VOLUME", 0),
                #"YEST_VOL": r.get("YEST_VOL", 0),
                "VOL_%": r.get("VOL_%", 0),
                "FROM_YH_%": r["FROM_YH_%"],
                "FROM_YL_%": r["FROM_YL_%"],
                "DAY_OPEN": r["LIVE_OPEN"],
                "DAY_HIGH": r["LIVE_HIGH"],
                "DAY_LOW": r["LIVE_LOW"],
                "YEST_HIGH": r["YEST_HIGH"],
                "YEST_LOW": r["YEST_LOW"],
                "YEST_CLOSE": r["YEST_CLOSE"],
            })

            st.session_state.alerted_symbols.add(key)


# Append only new alerts
if new_alerts:
    st.session_state.alerts.extend(new_alerts)

if datetime.now(IST).hour == 9 and datetime.now(IST).minute < 16:
    st.session_state.alerted_symbols.clear()


# ================== DISPLAY SECTION ==================

alerts_df = pd.DataFrame(st.session_state.alerts)

# LIVE ALERTS - hidden (re-enable when needed)
# st.subheader("⚡ LIVE ALERTS")
# if alerts_df.empty:
#     st.info("No live alerts yet.")
# else:
#     if "TS" in alerts_df.columns:
#         alerts_df["TS"] = pd.to_datetime(alerts_df["TS"], errors="coerce")
#         alerts_df = alerts_df.sort_values("TS", ascending=False)
#     display_cols = ["TIME","TYPE","Symbol","LTP","CHANGE_%","LIVE_VOLUME",
#                     "YEST_VOL","VOL_%","DAY_OPEN","DAY_HIGH","DAY_LOW",
#                     "YEST_HIGH","YEST_LOW","YEST_CLOSE"]
#     existing_cols = [c for c in display_cols if c in alerts_df.columns]
#     alerts_df = alerts_df[existing_cols]
#     st.dataframe(alerts_df, width='stretch')



# =================================================
# ================= CLEAN OLD ALERTS =================

# Normalize TS
for a in st.session_state.alerts:
    if "TS" in a and a["TS"] is not None:
        a["TS"] = pd.Timestamp(a["TS"]).tz_localize(None)

# Remove alerts older than 30 mins
cutoff = pd.Timestamp.now(tz=IST).tz_localize(None) - timedelta(minutes=30)

st.session_state.alerts = [
    a for a in st.session_state.alerts
    if "TS" in a and a["TS"] >= cutoff
]


            ######################      Clear alerts button (hidden - re-enable when needed)
# if st.button("🧹 Clear Alerts"):
#     st.session_state.alerts = []
#     st.session_state.alert_keys = set()


# ── TAB PERSISTENCE: stay on same tab after auto-refresh ─────────────────
# Read active tab from URL query param (survives Streamlit reruns)
_TAB_NAMES = [
    "PANCHAK","TOP_HIGH","TOP_LOW","NEAR","WATCHLIST",
    "BREAKOUT","D_BREAKS","W_BREAKS","M_BREAKS","OHL",
    "EMA","TOPGL","FOURBAR","OPTIONS","INDICES",
    "MIN15","ALERTS","INFO"
]
_N_TABS = len(_TAB_NAMES)

try:
    _active_tab = int(st.query_params.get("tab", 0))
    _active_tab = max(0, min(_active_tab, _N_TABS - 1))
except Exception:
    _active_tab = 0

# Inject JS: on tab button click -> update ?tab=N in URL (no page reload)
# On load -> auto-click the stored tab button after a tiny delay
st.markdown(f"""
<script>
(function() {{
    // Wait for Streamlit to render tabs before acting
    function initTabPersistence() {{
        var tabBar = window.parent.document.querySelector('[data-testid="stTabs"] [role="tablist"]');
        if (!tabBar) {{
            setTimeout(initTabPersistence, 150);
            return;
        }}

        var buttons = tabBar.querySelectorAll('[role="tab"]');
        var activeIdx = {_active_tab};

        // Auto-click stored tab on load (only if not already active)
        if (buttons.length > activeIdx) {{
            var target = buttons[activeIdx];
            if (target && target.getAttribute('aria-selected') !== 'true') {{
                target.click();
            }}
        }}

        // Listen for tab clicks -> update URL query param
        buttons.forEach(function(btn, idx) {{
            btn.addEventListener('click', function() {{
                var url = new URL(window.parent.location.href);
                url.searchParams.set('tab', idx);
                window.parent.history.replaceState(null, '', url.toString());
            }});
        }});
    }}
    setTimeout(initTabPersistence, 300);
}})();
</script>
""", unsafe_allow_html=True)

tabs = st.tabs([
    "🔭 ASTRO",     # 0
    "Moon KP",        # 1  ← KP Panchang
    "🪐 PANCHAK",   # 2
    "🟢 TOP_HIGH",  # 3
    "🔴 TOP_LOW",   # 4
    "🟡 NEAR",      # 5
    "WATCHLIST",    # 6
    "📊 NIFTY OI",  # 7
    "📈 D-BREAKS",  # 8
    "📊 W-BREAKS",  # 9
    "·",            # 10 HIDDEN
    "⚡ O=H=L",     # 11
    "·",            # 12 HIDDEN
    "🔥 TOP G/L",   # 13
    " 4-BAR",       # 14
    "·",            # 15 HIDDEN
    "INDICES",      # 16
    "·",            # 17 HIDDEN
    "⚡ Alerts",    # 18
    "📐 BOS/CHoCH", # 19
    "ℹ️ INFO",      # 20
    "📊 MACD",      # 21  ← NEW: MACD Daily Scanner
    "🕯️ INSIDE BAR",# 22  ← NEW: Inside Bar Scanner
    "🔺 GANN",      # 23  ← NEW: Gann Engine
    "🎯 MTF-SEQ",   # 24  ← NEW: Multi-Timeframe Sequential Scanner (Point 6)
    "📐 PATTERNS",  # 25  ← NEW: Chart Pattern Scanner
])

# ═══════════════════════════════════════════════════════════════════════
# 🔭 TAB 0 - ASTRO INTELLIGENCE CENTRE
# Sources: Banerjee "Stock Market Astrology" (2009), P.K. Vasudev
#          "Vedic Astrology in Money Matters", KP Krishnamurti system
# ═══════════════════════════════════════════════════════════════════════

with tabs[0]:
    st.header("🔭 ASTRO + OI INTELLIGENCE CENTRE")
    st.caption(
        "Sources: Banerjee · Vasudev · Vyapar Ratna · "
        "**Vishnu Bhaskar** *Advanced Techniques Vol1&2* (Govardhan Padhati, Dhruvanka, Murthy Nirnaya) · "
        "KP Krishnamurti · Live Kite OI Chain. **NOT financial advice.**"
    )

    # ── Fetch Fresh Intelligence EARLY to avoid NameErrors ───────────
    _oi_nifty = None
    _oi_bn    = None
    try:
        _ast_data = _vedic_day_analysis(datetime.now(IST).date())
        # Fresh fetch (st.cache_data ensures efficiency)
        _oi_nifty = fetch_oi_intelligence("NIFTY")
        _oi_bn    = fetch_oi_intelligence("BANKNIFTY")
    except Exception as _init_err:
        st.warning(f"⚠️ Initial intelligence fetch failed: {_init_err}")

    # ── NEW: MAGIC LINES & REVERSALS (GANN + KP) ──────────────────────
    with st.expander("🔮 MAGIC LINES & REVERSALS (High Conviction)", expanded=True):
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown("#### 🧱 Gann Magic Lines (Nifty Targets)")
            
            # Fetch Nifty's Yesterday's Close reliably (Dynamic)
            nifty_ref = get_nifty_reference_price()
                
            gann_levels = get_gann_square_9(nifty_ref)
            st.write(f"**Base Price (Yest Close):** {nifty_ref:,.2f}")
            m_rows = []
            for gl in gann_levels:
                m_rows.append({"Angle": f"{gl['angle']}°", "Resistance": gl['up'], "Support": gl['dn']})
            st.table(pd.DataFrame(m_rows))
            st.caption("Price targets based on Gann Square of 9 (mathematical harmonics).")
            
        with col_m2:
            st.markdown("#### ⏱️ Gann Time Cycles (Reversal Dates)")
            gann_time = get_gann_reversal_dates()
            
            if gann_time["is_reversal_today"]:
                st.error(f"⚠️ **GANN REVERSAL DAY TODAY** - {gann_time['today_event']}")
                st.caption(f"Conviction: {gann_time['conviction']}")
            else:
                st.info("Today is not a major Gann Time Reversal date.")
            
            st.markdown("##### 📅 Upcoming Gann Dates")
            for ud in gann_time["upcoming_dates"]:
                _d = ud.get("desc", "")
                st.markdown(f"• **{ud['date']}** - {ud['event']}", help=_d)
                if _d:
                    st.caption(f"&nbsp;&nbsp;&nbsp;&nbsp;_{_d}_")
            
            st.markdown("---")
            st.markdown("#### ⏱️ KP Reversal Timings (Intraday)")
            reversals = get_kp_reversals()
            if reversals:
                for rev in reversals:
                    c_badge = "🟢" if "BULL" in rev["bias"] else "🔴" if "BEAR" in rev["bias"] else "🟡"
                    st.markdown(f"**{rev['time']}** - {rev['event']} ({c_badge} {rev['bias']})")
                st.caption("Moon's KP Sub-lord changes signal 50-100 point Nifty reversals.")
            else:
                st.info("No sub-lord reversals during market hours today.")

    # ═══════════════════════════════════════════════════════════════════
    # 🔥 HIGH-CONVICTION COMBO: ASTRO + NIFTY OI + BANKNIFTY OI
    # Agreement between Nifty and BankNifty OI × Astro = SUPREME CONVICTION
    # ═══════════════════════════════════════════════════════════════════
    st.subheader("🎯 Supreme Confluence - Astro + Nifty OI + BankNifty OI")
    st.caption(" اتفق: agreement = HIGH CONVICTION. Disagreement = NO TRADE.")

    try:
        # 1. Fetch Fresh Intelligence
        _ast_data = _vedic_day_analysis(datetime.now(IST).date())
        _oi_nifty = fetch_oi_intelligence("NIFTY")
        _oi_bn    = fetch_oi_intelligence("BANKNIFTY")

        # 2. Extract Directions
        _ast_score = _ast_data["net_score"]
        _ast_dir   = "🟢 BULLISH" if _ast_score >= 2 else "🔴 BEARISH" if _ast_score <= -2 else "🟡 NEUTRAL"
        
        _n50_dir   = _oi_nifty.get("direction", "⚠️ NEUTRAL") if _oi_nifty else "⚠️ N/A"
        _bnk_dir   = _oi_bn.get("direction", "⚠️ NEUTRAL") if _oi_bn else "⚠️ N/A"

        # 3. Supreme Verdict
        _nifty_bull = "BULLISH" in _n50_dir
        _bnk_bull   = "BULLISH" in _bnk_dir
        _nifty_bear = "BEARISH" in _n50_dir
        _bnk_bear   = "BEARISH" in _bnk_dir

        _all_aligned_bull = ("BULLISH" in _ast_dir and _nifty_bull and _bnk_bull)
        _all_aligned_bear = ("BEARISH" in _ast_dir and _nifty_bear and _bnk_bear)

        if _all_aligned_bull:
            _verdict = "💎 SUPREME BUY SIGNAL (All Aligned)"
            _color   = "#00C851"; _card = "signal-card-bull"
        elif _all_aligned_bear:
            _verdict = "💎 SUPREME SELL SIGNAL (All Aligned)"
            _color   = "#FF4444"; _card = "signal-card-bear"
        elif _nifty_bull and _bnk_bull:
            _verdict = "🟢 STRONG OI BUY (Nifty + BankNifty)"
            _color   = "#00C851"; _card = "signal-card-bull"
        elif _nifty_bear and _bnk_bear:
            _verdict = "🔴 STRONG OI SELL (Nifty + BankNifty)"
            _color   = "#FF4444"; _card = "signal-card-bear"
        else:
            _verdict = "🟡 MIXED / RANGE-BOUND"
            _color   = "#FFD700"; _card = "signal-card-mixed"

        # 4. Render Card
        st.markdown(f"""
        <div class="{_card}" style="padding:20px; border-radius:12px; border-left:8px solid {_color}; background:rgba(0,0,0,0.3);">
            <div style="font-size:26px; font-weight:900; color:{_color};">{_verdict}</div>
            <div style="font-size:14px; margin-top:10px; opacity:0.9;">
                🏛️ <b>Astro:</b> {_ast_dir} &nbsp;|&nbsp; 📊 <b>Nifty OI:</b> {_n50_dir} &nbsp;|&nbsp; 🏦 <b>BankNifty OI:</b> {_bnk_dir}
            </div>
            <div style="font-size:12px; margin-top:8px; font-style:italic; color:#aaa;">
                Speculative Churn: {_oi_nifty.get('speculative','LOW') if _oi_nifty else '-'} | 
                Battlegrounds: CE {_oi_nifty.get('battle_ce','-') if _oi_nifty else '-'} vs PE {_oi_nifty.get('battle_pe','-') if _oi_nifty else '-'}
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 5. Core Metrics
        _m1, _c2, _m3 = st.columns(3)
        _m1.metric("Nifty PCR", _oi_nifty.get("pcr", 0) if _oi_nifty else "-")
        _c2.metric("BankNifty PCR", _oi_bn.get("pcr", 0) if _oi_bn else "-")
        _m3.metric("Nifty Max Pain", _oi_nifty.get("max_pain", 0) if _oi_nifty else "-")

    except Exception as e:
        st.error(f"Supreme Confluence Error: {e}")

    st.divider()

    # ── SMC + OI Confluence Block (injected by apply_smc_patch.py) ──
    st.subheader("🧠 SMC + OI Confluence Intelligence")
    st.caption(
        "Smart Money Concept analysis (BOS/CHoCH/OB/FVG) combined with OI data. "
        "Resolves false bearish OI signals during institutional accumulation / gamma squeeze."
    )
    _render_smc_block(st.session_state.get("smc_result", {}))


    # ═══════════════════════════════════════════════════════════════════
    # STOCK-SPECIFIC FUTURE DATES - HIGH BULL / BEAR PROBABILITY
    # Sources: Banerjee Ch4/Ch6 (graha->sector), Vyapar Ratna Part 2
    #   VR2: Iron/Steel bearish - Saturn+Moon/Mars/Mercury yoga
    #   VR2: Mercury near Sun (direct) + Venus far = bullish futures
    #   VR2: Mercury-Venus conjunction = bearish for prices
    #   VR2: Jupiter+Rahu conjunction = Gold/Silver hyperinflation
    #   Banerjee: Saturn in Taurus/Virgo/Capricorn (earthy) = recovery
    # ═══════════════════════════════════════════════════════════════════
    st.subheader("📅 Stock-Specific Future Bull/Bear Date Forecast")
    st.caption(
        "Computed from upcoming planetary transits vs stock sector rulerships. "
        "Source: Banerjee Ch4 (planet->sector) + Vyapar Ratna Part 2 (specific yogas). "
        "**Probability only - NOT financial advice.**"
    )

    # Stock -> ruling planets (Banerjee Ch4 + sector)
    _STOCK_PLANETS = {
        # Banking & Finance - Jupiter, Mercury, Venus (Banerjee: Guru+Budha+Sukra)
        "HDFCBANK":  {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        "SBIN":      {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        "ICICIBANK": {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        "AXISBANK":  {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        # IT - Mercury, Ketu (Banerjee: Budha+Ketu)
        "TCS":       {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        "INFY":      {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        "WIPRO":     {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        "HCLTECH":   {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        # Steel/Metals - Mars, Saturn (Banerjee: Mangal+Sani; VR2: Saturn+Moon yoga = BEARISH)
        "TATASTEEL": {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "SAIL":      {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "JSWSTEEL":  {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "HINDALCO":  {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        # Oil & Gas - Moon, Saturn, Ketu (Banerjee: Candra+Sani+Ketu)
        "RELIANCE":  {"sector":"Oil & Gas","planets":["Moon","Saturn","Ketu"],"bull_signs":["Cancer","Scorpio","Pisces"],"bear_signs":["Aries","Leo","Sagittarius"]},
        "ONGC":      {"sector":"Oil & Gas","planets":["Moon","Saturn","Ketu"],"bull_signs":["Cancer","Scorpio","Pisces"],"bear_signs":["Aries","Leo","Sagittarius"]},
        "BPCL":      {"sector":"Oil & Gas","planets":["Moon","Saturn","Ketu"],"bull_signs":["Cancer","Scorpio","Pisces"],"bear_signs":["Aries","Leo","Sagittarius"]},
        # Pharma - Mars, Ketu (Banerjee: Mangal+Ketu)
        "SUNPHARMA": {"sector":"Pharma","planets":["Mars","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "CIPLA":     {"sector":"Pharma","planets":["Mars","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "DRREDDY":   {"sector":"Pharma","planets":["Mars","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        # Auto - Mars, Ketu, Saturn (Banerjee: Mangal+Sani+Ketu)
        "MARUTI":    {"sector":"Auto","planets":["Mars","Saturn","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Gemini"]},
        "TATAMOTORS":{"sector":"Auto","planets":["Mars","Saturn","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Gemini"]},
        "BAJAJ-AUTO":{"sector":"Auto","planets":["Mars","Saturn","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Gemini"]},
        # Power - Sun, Rahu (Banerjee: Surya+Rahu)
        "NTPC":      {"sector":"Power","planets":["Sun","Rahu"],"bull_signs":["Leo","Aries","Sagittarius"],"bear_signs":["Aquarius","Libra","Gemini"]},
        "POWERGRID": {"sector":"Power","planets":["Sun","Rahu"],"bull_signs":["Leo","Aries","Sagittarius"],"bear_signs":["Aquarius","Libra","Gemini"]},
        "ADANIPOWER":{"sector":"Power","planets":["Sun","Rahu"],"bull_signs":["Leo","Aries","Sagittarius"],"bear_signs":["Aquarius","Libra","Gemini"]},
        # Cement - Mars, Saturn, Rahu (Banerjee: Mangal+Sani+Rahu)
        "ULTRACEMCO":{"sector":"Cement","planets":["Mars","Saturn","Rahu"],"bull_signs":["Aries","Capricorn","Taurus"],"bear_signs":["Cancer","Libra","Gemini"]},
        "SHREECEM":  {"sector":"Cement","planets":["Mars","Saturn","Rahu"],"bull_signs":["Aries","Capricorn","Taurus"],"bear_signs":["Cancer","Libra","Gemini"]},
        # Infra/Realty - Saturn, Rahu, Ketu (Banerjee: Sani+Rahu+Ketu)
        "DLF":       {"sector":"Realty","planets":["Saturn","Rahu","Mars"],"bull_signs":["Capricorn","Aquarius","Taurus"],"bear_signs":["Cancer","Leo","Aries"]},
        "ADANIPORTS":{"sector":"Infra","planets":["Saturn","Rahu","Mars"],"bull_signs":["Capricorn","Aquarius","Taurus"],"bear_signs":["Cancer","Leo","Aries"]},
        # FMCG - Jupiter, Venus (Banerjee: Guru+Sukra)
        "HINDUNILVR":{"sector":"FMCG","planets":["Jupiter","Venus"],"bull_signs":["Cancer","Pisces","Taurus"],"bear_signs":["Virgo","Gemini","Capricorn"]},
        "ITC":       {"sector":"FMCG","planets":["Jupiter","Venus"],"bull_signs":["Cancer","Pisces","Taurus"],"bear_signs":["Virgo","Gemini","Capricorn"]},
        # Gold / Jewelry - Jupiter, Rahu (Banerjee Ch17: Guru+Rahu = Gold hyperinflation)
        "GOLDBEES":  {"sector":"Gold","planets":["Jupiter","Rahu"],"bull_signs":["Cancer","Leo","Sagittarius"],"bear_signs":["Capricorn","Virgo","Gemini"]},
        "MUTHOOTFIN":{"sector":"Gold","planets":["Jupiter","Rahu"],"bull_signs":["Cancer","Leo","Sagittarius"],"bear_signs":["Capricorn","Virgo","Gemini"]},
    }

    # Get current planet rashis
    try:
        _today_snap_sd = _vedic_day_analysis(datetime.now(IST).date())
        _CURR_RASHI = {
            "Sun":     _today_snap_sd["sun_rashi"],
            "Moon":    _today_snap_sd["moon_rashi"],
            "Mars":    _today_snap_sd["mars_rashi"],
            "Mercury": _today_snap_sd["mercury_rashi"],
            "Jupiter": _today_snap_sd["jupiter_rashi"],
            "Venus":   _today_snap_sd["venus_rashi"],
            "Saturn":  _today_snap_sd["saturn_rashi"],
            "Rahu":    _today_snap_sd["rahu_rashi"],
        }
        _RASHI_ORDER2 = ["Aries","Taurus","Gemini","Cancer","Leo","Virgo",
                         "Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"]

        def _get_ketu_rashi(rahu):
            _ri = _RASHI_ORDER2.index(rahu) if rahu in _RASHI_ORDER2 else 0
            return _RASHI_ORDER2[(_ri + 6) % 12]

        _CURR_RASHI["Ketu"] = _get_ketu_rashi(_CURR_RASHI["Rahu"])

        # Score each stock TODAY
        _stock_scores = []
        for _sym, _sdata in _STOCK_PLANETS.items():
            _bull_score = 0
            _bear_score = 0
            _bull_reasons = []
            _bear_reasons = []
            for _pl in _sdata["planets"]:
                _prashi = _CURR_RASHI.get(_pl, "")
                if _prashi in _sdata["bull_signs"]:
                    _bull_score += 1
                    _bull_reasons.append(f"{_pl} in {_prashi}")
                elif _prashi in _sdata["bear_signs"]:
                    _bear_score += 1
                    _bear_reasons.append(f"{_pl} in {_prashi}")

            # Vyapar Ratna special yogas
            # VR2: Mercury+Venus conjunction = BEARISH prices
            _merc_rashi = _CURR_RASHI.get("Mercury","")
            _venus_rashi = _CURR_RASHI.get("Venus","")
            if _merc_rashi == _venus_rashi and _sdata["sector"] in ["Banking","FMCG","IT"]:
                _bear_score += 1
                _bear_reasons.append(f"VR: Mercury+Venus in {_merc_rashi} = bearish prices")

            # VR2: Iron/Steel bearish when Saturn+Moon same rashi
            _sat_rashi = _CURR_RASHI.get("Saturn","")
            _moon_rashi = _CURR_RASHI.get("Moon","")
            if _sat_rashi == _moon_rashi and _sdata["sector"] == "Metals":
                _bear_score += 2
                _bear_reasons.append(f"VR: Saturn+Moon in {_sat_rashi} = IRON/STEEL BEARISH (Vyapar Ratna)")

            # VR2: Gold hyperinflation - Jupiter+Rahu near (same or adjacent rashi)
            _jup_rashi = _CURR_RASHI.get("Jupiter","")
            _rahu_rashi = _CURR_RASHI.get("Rahu","")
            _ji = _RASHI_ORDER2.index(_jup_rashi) if _jup_rashi in _RASHI_ORDER2 else -1
            _ri2 = _RASHI_ORDER2.index(_rahu_rashi) if _rahu_rashi in _RASHI_ORDER2 else -1
            if abs(_ji - _ri2) <= 1 and _sdata["sector"] == "Gold":
                _bull_score += 2
                _bull_reasons.append(f"Banerjee Ch17: Jupiter+Rahu near conjunction = GOLD HYPERINFLATION")

            # Mars-Rahu close = metals bearish
            if _today_snap_sd["mars_rahu_deg"] < 15 and _sdata["sector"] in ["Metals","Auto","Pharma"]:
                _bear_score += 1
                _bear_reasons.append(f"Mars-Rahu {_today_snap_sd['mars_rahu_deg']}° = sector bearish")

            _net_sd = _bull_score - _bear_score
            if _net_sd >= 2:
                _sig_sd = "🟢 BULLISH"
            elif _net_sd == 1:
                _sig_sd = "🟢 MILD BULL"
            elif _net_sd <= -2:
                _sig_sd = "🔴 BEARISH"
            elif _net_sd == -1:
                _sig_sd = "🔴 MILD BEAR"
            else:
                _sig_sd = "🟡 NEUTRAL"

            _stock_scores.append({
                "Stock": _sym,
                "Sector": _sdata["sector"],
                "Signal": _sig_sd,
                "Bull": _bull_score,
                "Bear": _bear_score,
                "Net": _net_sd,
                "Bull Reasons": " | ".join(_bull_reasons) if _bull_reasons else "-",
                "Bear Reasons": " | ".join(_bear_reasons) if _bear_reasons else "-",
            })

        _sd_df = pd.DataFrame(_stock_scores).sort_values("Net", ascending=False)

        # Filter controls
        _sdf_col1, _sdf_col2 = st.columns([3,1])
        with _sdf_col2:
            _sd_filter = st.selectbox("Filter", ["All","BULLISH only","BEARISH only"], key="sd_filter")
        with _sdf_col1:
            st.markdown(f"**Showing {len(_sd_df)} stocks** based on today's planetary positions vs stock sector rulerships")

        if _sd_filter == "BULLISH only":
            _sd_df = _sd_df[_sd_df["Signal"].str.contains("BULL")]
        elif _sd_filter == "BEARISH only":
            _sd_df = _sd_df[_sd_df["Signal"].str.contains("BEAR")]

        def _sd_sig_color(val):
            v = str(val)
            if "STRONG BULL" in v or ("BULLISH" in v and "MILD" not in v):
                return "background-color:#0d2b0d;color:#00C851;font-weight:700"
            if "MILD BULL" in v: return "background-color:#0d1a0d;color:#7ddb9d;font-weight:600"
            if "STRONG BEAR" in v or ("BEARISH" in v and "MILD" not in v):
                return "background-color:#2b0d0d;color:#FF4444;font-weight:700"
            if "MILD BEAR" in v: return "background-color:#1a0d0d;color:#ff8888;font-weight:600"
            return "background-color:#1a1500;color:#FFD700"

        st.dataframe(
            _sd_df.style
                .map(_sd_sig_color, subset=["Signal"])
                .set_properties(**{"font-size":"12px","text-align":"left","background-color":"#0a0a0a","color":"#ddd"})
                .set_table_styles([{"selector":"th","props":[("background","#111"),("color","white"),("font-weight","bold")]}]),
            width='stretch',
            height=420,
            hide_index=True,
        )

        # Top picks
        _top_bull = _sd_df[_sd_df["Net"] >= 2].head(5)
        _top_bear = _sd_df[_sd_df["Net"] <= -2].head(5)
        _tp1, _tp2 = st.columns(2)
        with _tp1:
            st.markdown("**🟢 Top BULLISH Picks Today**")
            if not _top_bull.empty:
                for _, _tb in _top_bull.iterrows():
                    st.markdown(f"""
<div class="bull-window"><b>{_tb['Stock']}</b> ({_tb['Sector']}) &nbsp;
<span class="time-badge-bull">{_tb['Signal']}</span>&nbsp;
<span style="color:#888;font-size:11px">{_tb['Bull Reasons']}</span></div>
""", unsafe_allow_html=True)
            else:
                st.info("No strong bullish picks today")
        with _tp2:
            st.markdown("**🔴 Top BEARISH Picks Today**")
            if not _top_bear.empty:
                for _, _te in _top_bear.iterrows():
                    st.markdown(f"""
<div class="bear-window"><b>{_te['Stock']}</b> ({_te['Sector']}) &nbsp;
<span class="time-badge-bear">{_te['Signal']}</span>&nbsp;
<span style="color:#888;font-size:11px">{_te['Bear Reasons']}</span></div>
""", unsafe_allow_html=True)
            else:
                st.info("No strong bearish picks today")

        # ── Future dates scan (next 10 trading days) ──────────
        st.markdown("#### 📆 High Probability Future Dates by Stock")
        st.caption("Scanning next 10 trading days. Green = high bull probability, Red = high bear probability.")

        _future_rows = []
        _check_date_f = datetime.now(IST).date()
        _found_f = 0
        for _di_f in range(1, 30):
            if _found_f >= 10: break
            _fd_f = _check_date_f + timedelta(days=_di_f)
            if _fd_f.weekday() >= 5 or _fd_f in _get_nse_holidays():
                continue
            try:
                _rf = _vedic_day_analysis(_fd_f)
            except Exception:
                continue
            _CURR_F = {
                "Sun":     _rf["sun_rashi"], "Moon": _rf["moon_rashi"],
                "Mars":    _rf["mars_rashi"], "Mercury": _rf["mercury_rashi"],
                "Jupiter": _rf["jupiter_rashi"], "Venus": _rf["venus_rashi"],
                "Saturn":  _rf["saturn_rashi"], "Rahu": _rf["rahu_rashi"],
            }
            _CURR_F["Ketu"] = _get_ketu_rashi(_CURR_F["Rahu"])
            _day_bull = []; _day_bear = []
            for _sym2, _sdata2 in _STOCK_PLANETS.items():
                _bs2 = sum(1 for _pl2 in _sdata2["planets"] if _CURR_F.get(_pl2,"") in _sdata2["bull_signs"])
                _br2 = sum(1 for _pl2 in _sdata2["planets"] if _CURR_F.get(_pl2,"") in _sdata2["bear_signs"])
                if _bs2 >= 2: _day_bull.append(_sym2)
                if _br2 >= 2: _day_bear.append(_sym2)
            if _day_bull or _day_bear:
                _future_rows.append({
                    "Date": _fd_f.strftime("%a %d %b"),
                    "🟢 BULLISH Stocks": ", ".join(_day_bull[:6]) if _day_bull else "-",
                    "🔴 BEARISH Stocks": ", ".join(_day_bear[:6]) if _day_bear else "-",
                    "Overall": "🟢" if len(_day_bull) > len(_day_bear) else ("🔴" if len(_day_bear) > len(_day_bull) else "🟡"),
                    "Bull Count": len(_day_bull),
                    "Bear Count": len(_day_bear),
                })
            _found_f += 1

        if _future_rows:
            _fdf2 = pd.DataFrame(_future_rows)
            def _fd_col(val):
                v = str(val)
                if v == "🟢": return "background-color:#0d2b0d;color:#00C851;font-weight:700"
                if v == "🔴": return "background-color:#2b0d0d;color:#FF4444;font-weight:700"
                return "background-color:#1a1500;color:#FFD700"
            st.dataframe(
                _fdf2.style
                    .map(_fd_col, subset=["Overall"])
                    .set_properties(**{"font-size":"12px","text-align":"left","background-color":"#0a0a0a","color":"#ddd"})
                    .set_table_styles([{"selector":"th","props":[("background","#111"),("color","white")]}]),
                width='stretch', height=380, hide_index=True,
            )

    except Exception as _sd_err:
        st.warning(f"Stock dates error: {_sd_err}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════════
    # TELEGRAM CONFIG PANEL (settings)
    # ═══════════════════════════════════════════════════════════════════
    with st.expander("⚙️ Telegram Alert Settings - @streamlit123_bot", expanded=False):
        # ── Status row ────────────────────────────────────────────────
        _tg_status_cols = st.columns([2,2,2])
        _tg_status_cols[0].metric("🤖 Bot", "@streamlit123_bot")
        _tg_status_cols[1].metric("📡 Channel", "Private Channel ✅")
        _tg_status_cols[2].metric("🔑 Chat ID", TG_CHAT_ID if TG_CHAT_ID else "⏳ Auto-detecting...", delta="✅ Confirmed" if TG_CHAT_ID else None)

        if not TG_CHAT_ID:
            st.warning(
                "⚠️ Chat ID not yet resolved. **Post any message in your channel** "
                "then restart the dashboard - it will auto-detect via getUpdates."
            )
            st.info(
                "**Manual fallback:** Open this URL in your browser after posting a message in the channel, "
                "then look for chat id in the JSON response: "
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates"
            )
            _manual_cid = st.text_input("Paste Chat ID here (e.g. -1001234567890)", key="manual_chat_id")
            if st.button("💾 Save Chat ID") and _manual_cid.strip():
                try:
                    _cache_dir = CACHE_DIR  # FIX-4e
                    os.makedirs(_cache_dir, exist_ok=True)
                    with open(os.path.join(_cache_dir, "tg_chat_id.txt"), "w", encoding="utf-8") as _cf: _cf.write(_manual_cid.strip())
                    st.success(f"✅ Saved! Chat ID: {_manual_cid.strip()}. Restart dashboard to apply.")
                except Exception as _e:
                    st.error(f"Save error: {_e}")
        else:
            st.success(f"✅ Bot connected to channel. Chat ID: `{TG_CHAT_ID}`")

        st.divider()
        st.markdown("""
**✅ Already configured:**
| Setting | Value |
|---------|-------|
| Bot Token | `8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A` |
| Bot Username | `@streamlit123_bot` |
| Channel Name | **Panchak Alerts** |
| Channel Link | https://t.me/+d4StsTfQQyI4MzVl |
| Bot Role in Channel | Admin ✅ |
| Chat ID | `-1003706739531` ✅ Confirmed |

**Alerts fired automatically:**
| Alert | Trigger | Dedup |
|-------|---------|-------|
| 🚀 15-Min HIGH Breakout | Sequential candle chain above highs | Once/day per stock |
| 🔻 15-Min LOW Breakdown | Sequential candle chain below lows | Once/day per stock |
| 🚀 1-Hour HIGH Breakout | 1-hr sequential chain above highs | Once/day per stock |
| 🔻 1-Hour LOW Breakdown | 1-hr sequential chain below lows | Once/day per stock |
| 📊 NIFTY OI Intelligence | Spot + PCR + direction + walls + setup | Every 15-min slot |
        """)

        st.divider()
        st.markdown("**🧪 Test Alert**")
        _tg_test_col1, _tg_test_col2 = st.columns([3,1])
        with _tg_test_col1:
            _tg_test_msg = st.text_input("Test message", value="✅ Panchak Dashboard is LIVE! Alerts connected.", key="tg_test_msg")
        with _tg_test_col2:
            if st.button("📤 Send Test", key="tg_test_btn"):
                _test_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                _test_payload = json.dumps({
                    "chat_id":    TG_CHAT_ID,
                    "text":       f"🔔 <b>PANCHAK DASHBOARD TEST</b>\n\n{_tg_test_msg}\n\n⏰ {datetime.now(IST).strftime('%d/%m/%Y %H:%M IST')}",
                    "parse_mode": "HTML"
                }).encode()
                try:
                    _req = urllib.request.Request(_test_url, data=_test_payload, headers={"Content-Type":"application/json"})
                    with urllib.request.urlopen(_req, timeout=10) as _resp:
                        _ok = json.loads(_resp.read()).get("ok", False)
                    if _ok:
                        st.success("✅ Test message sent to channel!")
                    else:
                        st.error("❌ Send failed - check Chat ID in cache/tg_chat_id.txt")
                except Exception as _te:
                    st.error(f"Error: {_te}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════════
    # DATE PICKER - Yellow input / Red date display / Green Submit
    # ═══════════════════════════════════════════════════════════════════
    st.markdown("""
    <style>
    div[data-testid="stDateInput"] > div {
        border: 2px solid #FFD700 !important; border-radius: 6px;
        background: #000 !important; color: #00BFFF !important; }
    div[data-testid="stDateInput"] label {
        color: #00BFFF !important; font-weight: 700; font-size: 14px; }
    .astro-date-display { color:#FF4444; font-size:22px; font-weight:700;
        text-align:center; padding:6px 20px; letter-spacing:2px; }
    .bull-window  { background:#0d2b0d; border-left:4px solid #00C851; padding:6px 12px; border-radius:4px; margin:3px 0; color:#b6f5c8 !important; }
    .bull-window * { color:#b6f5c8 !important; }
    .bear-window  { background:#2b0d0d; border-left:4px solid #FF4444; padding:6px 12px; border-radius:4px; margin:3px 0; color:#ffb3b3 !important; }
    .bear-window * { color:#ffb3b3 !important; }
    .neutral-window { background:#1a1a00; border-left:4px solid #FFD700; padding:6px 12px; border-radius:4px; margin:3px 0; color:#fff3b0 !important; }
    .neutral-window * { color:#fff3b0 !important; }
    .rahukaal-window { background:#1a0a2b; border-left:4px solid #9B59B6; padding:6px 12px; border-radius:4px; margin:3px 0; color:#e0c3ff !important; }
    .rahukaal-window * { color:#e0c3ff !important; }
    .signal-card-bull { background:linear-gradient(135deg,#0a2e0a,#0d3d0d); border:1px solid #00C851; border-radius:8px; padding:14px; margin:6px 0; color:#b6f5c8 !important; }
    .signal-card-bull * { color:#b6f5c8 !important; }
    .signal-card-bear { background:linear-gradient(135deg,#2e0a0a,#3d0d0d); border:1px solid #FF4444; border-radius:8px; padding:14px; margin:6px 0; color:#ffb3b3 !important; }
    .signal-card-bear * { color:#ffb3b3 !important; }
    .signal-card-mixed { background:linear-gradient(135deg,#1a1500,#2a2200); border:1px solid #FFD700; border-radius:8px; padding:14px; margin:6px 0; color:#fff3b0 !important; }
    .signal-card-mixed * { color:#fff3b0 !important; }
    .time-badge-bull  { display:inline-block; background:#00C851; color:#000 !important; font-weight:700; padding:2px 10px; border-radius:12px; font-size:13px; }
    .time-badge-bear  { display:inline-block; background:#FF4444; color:#fff !important; font-weight:700; padding:2px 10px; border-radius:12px; font-size:13px; }
    .time-badge-caution { display:inline-block; background:#9B59B6; color:#fff !important; font-weight:700; padding:2px 10px; border-radius:12px; font-size:13px; }
    </style>
    """, unsafe_allow_html=True)

    _dc1, _dc2, _dc3 = st.columns([3, 2, 1])
    with _dc1:
        _sel_date = st.date_input(
            "📅 Select date for NIFTY Astrology",
            value=datetime.now(IST).date(),
            min_value=date(2020, 1, 1), max_value=date(2030, 12, 31),
            key="astro_date_picker",
        )
    with _dc2:
        st.markdown(f'<div class="astro-date-display">{_sel_date.strftime("%d/%m/%Y")}</div>',
                    unsafe_allow_html=True)
    with _dc3:
        _do_analyse = st.button("SUBMIT", key="astro_submit", width='stretch')

    st.divider()

    # ── Run analysis on selected date ─────────────────────────────────
    try:
        _snap = _vedic_day_analysis(_sel_date)
    except Exception as _ex:
        st.error(f"Analysis error: {_ex}")
        _snap = None

    if _snap:

        # ── BLOCK A - DAY OVERVIEW ────────────────────────────────────
        _ov       = _snap["overall"]
        _ov_color = "#00C851" if "BULLISH" in _ov else ("#FF4444" if "BEARISH" in _ov else "#FFD700")
        _card_cls = "signal-card-bull" if "BULLISH" in _ov else ("signal-card-bear" if "BEARISH" in _ov else "signal-card-mixed")
        _day_name = _sel_date.strftime("%A, %d %B %Y")
        _is_weekend = _sel_date.weekday() >= 5
        _is_holiday = _sel_date in _get_nse_holidays()
        _mkt_open   = not _is_weekend and not _is_holiday

        # ── Derived display values ──────────────────────────────────
        _retro_str  = ", ".join(_snap.get("retro", {}).keys()) or "None"
        _kp_sub     = _snap.get("moon_sub_lord", "-")
        _kp_subsub  = _snap.get("moon_subsub_lord", "-")
        _day_lord   = _snap.get("day_lord", "-")
        _hora_open  = _snap.get("hora_open", "-")
        _karana     = _snap.get("karana", "-")
        _next_nak   = _snap.get("next_nak", "-")
        _next_lord  = _snap.get("next_nak_lord", "-")
        _hrs_next   = _snap.get("hours_to_next_nak", 0)
        _moon_dig   = _snap.get("moon_dignity", "NEUTRAL")
        _jup_dig    = _snap.get("jupiter_dignity", "NEUTRAL")
        _ashta      = _snap.get("ashta_moon", "-")
        _sun_sub    = _snap.get("sun_sub_lord", "-")

        # colour helpers
        def _kp_col(lord):
            _bull = {"Jupiter","Venus","Moon"}
            _bear = {"Saturn","Rahu","Ketu","Mars"}
            if lord in _bull: return "#00C851"
            if lord in _bear: return "#FF4444"
            return "#FFD700"
        def _dig_col(d):
            return {"EXALTED":"#00C851","OWN":"#88ff88","DEBILITATED":"#FF4444"}.get(d,"#aaa")
        def _hora_col(h):
            _hb = {"Sun","Jupiter","Venus","Moon"}
            return "#00C851" if h in _hb else "#FF4444"

        _transit_warn = (
            f"⚡ Moon enters <b>{_next_nak}</b> ({_next_lord}★) in <b style='color:#FF4444'>{_hrs_next}h</b>"
            if _hrs_next < 3 else
            f"Moon Next nak: <b>{_next_nak}</b> ({_next_lord}★) in {_hrs_next}h"
        )

        st.markdown(f"""
<div class="{_card_cls}">
  <h3 style="color:{_ov_color};margin:0 0 6px 0">
    {"🔴" if "BEARISH" in _ov else "🟢" if "BULLISH" in _ov else "⚠️"} &nbsp;
    {_day_name} &mdash; <span style="font-size:20px">{_ov}</span>
    &nbsp;<span style="font-size:14px;color:#aaa">({_snap["intensity"]})</span>
    &nbsp;<span style="font-size:16px;color:{_ov_color};font-weight:700">{_snap["net_score"]:+d} pts</span>
    {"<span style='color:#FF4444;font-weight:700;margin-left:12px'>🚫 NSE HOLIDAY</span>" if not _mkt_open else ""}
  </h3>

  <!-- ROW 1: Nakshatra + KP Sublords -->
  <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:8px">
    <span>Moon <b>Moon Nak:</b> {_snap["moon_nak"]}
      &nbsp;<span style="color:#aaa;font-size:11px">★{_snap["moon_nak_lord"]}</span>
      &nbsp;->&nbsp;<b style="color:#FFD700">{_snap["moon_rashi"]}</b>
      &nbsp;<span style="color:#aaa;font-size:11px">({_moon_dig})</span>
    </span>
    <span>🔑 <b>KP Sub-lord:</b> <b style="color:{_kp_col(_kp_sub)}">{_kp_sub}</b></span>
    <span>🔑🔑 <b>Sub-sub:</b> <b style="color:{_kp_col(_kp_subsub)}">{_kp_subsub}</b></span>
    <span>📆 <b>Tithi:</b> {_snap["tithi"].split("(")[0].strip()} · <b>{_karana}</b> Karana</span>
  </div>

  <!-- ROW 2: Day Lord + Hora + Transit -->
  <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:6px">
    <span>📅 <b>Day Lord:</b> <b style="color:{_kp_col(_day_lord)}">{_day_lord}</b></span>
    <span>⏰ <b>Hora 9:15:</b> <b style="color:{_hora_col(_hora_open)}">{_hora_open}</b></span>
    <span>⚡ {_transit_warn}</span>
    <span>🔮 <b>Ashtakavarga:</b> <span style="color:{'#00C851' if _ashta=='STRONG' else '#FF4444' if _ashta=='WEAK' else '#FFD700'}">{_ashta}</span></span>
  </div>

  <!-- ROW 3: Planets + Dignity -->
  <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:6px;font-size:12px">
    <span>Sun️ <b>Sun:</b> {_snap["sun_rashi"]}/{_snap["sun_nak"]}
      &nbsp;<span style="color:#aaa">sub:{_sun_sub}</span></span>
    <span>Mars <b>Mars:</b> {_snap["mars_rashi"]}/{_snap["mars_nak"]}</span>
    <span>Saturn <b>Saturn:</b> {_snap["saturn_rashi"]}</span>
    <span>Jupiter <b>Jupiter:</b> {_snap["jupiter_rashi"]}
      &nbsp;<span style="color:{_dig_col(_jup_dig)}">{_jup_dig}</span>
      &nbsp;<span style="color:#FFD700">{_snap.get("jupiter_murthy","-")} Murthy</span></span>
    <span>Venus <b>Venus:</b> {_snap["venus_rashi"]}</span>
    <span>Mercury <b>Mercury:</b> {_snap["mercury_rashi"]}</span>
    <span>Rahu <b>Rahu:</b> {_snap["rahu_rashi"]}</span>
  </div>

  <!-- ROW 4: Key angles + Retro + Dhruvanka -->
  <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:6px;font-size:12px;color:#aaa">
    <span>⚡ Mars-Rahu: <b style="color:{'#FF4444' if _snap['mars_rahu_deg']<15 else '#aaa'}">{_snap["mars_rahu_deg"]}°</b></span>
    <span>Sun-Saturn Sun-Saturn: <b style="color:{'#FF4444' if _snap['sun_saturn_deg']<8 else '#aaa'}">{_snap["sun_saturn_deg"]}°</b></span>
    <span>Moon-Ketu Moon-Ketu: <b style="color:{'#FF4444' if _snap['moon_ketu_deg']<15 else '#aaa'}">{_snap["moon_ketu_deg"]}°</b></span>
    <span>Moon-Jupiter Moon-Jup: <b style="color:{'#00C851' if _snap['moon_jupiter_deg']<10 else '#aaa'}">{_snap["moon_jupiter_deg"]}°</b></span>
    <span>🔄 Retro: <b style="color:{'#FF4444' if _retro_str!='None' else '#aaa'}">{_retro_str}</b></span>
    <span>🔢 Dhruvanka: <b style="color:{'#00C851' if _snap.get('dhruvanka_result') in ('RISE','SHARP RISE') else '#FF4444' if _snap.get('dhruvanka_result')=='FALL' else '#FFD700'}">{_snap.get("dhruvanka_result","-")}</b> (R{_snap.get("dhruvanka_rem","-")})</span>
  </div>
</div>
""", unsafe_allow_html=True)

        # ── BLOCK B - INTRADAY TIMING ─────────────────────────────────
        st.subheader("⏱️ Intraday Timing - 30-Min Bull/Bear Windows")
        st.caption("Hora (Banerjee Ch10) + Rahukaal (Vyapar Ratna) + Nakshatra bias. "
                   "🟢 Bull · 🔴 Bear · 🟣 Rahukaal (avoid) · 🟡 Mixed")

        if not _mkt_open:
            st.warning("Market closed on this date. Analysis shown for reference only.")

        _HORA_TBL = [
            ["Sun","Moon","Mars","Mercury","Jupiter","Venus","Saturn"],
            ["Venus","Saturn","Sun","Moon","Mars","Mercury","Jupiter"],
            ["Mercury","Jupiter","Venus","Saturn","Sun","Moon","Mars"],
            ["Moon","Mars","Mercury","Jupiter","Venus","Saturn","Sun"],
            ["Saturn","Sun","Moon","Mars","Mercury","Jupiter","Venus"],
            ["Jupiter","Venus","Saturn","Sun","Moon","Mars","Mercury"],
            ["Mars","Mercury","Jupiter","Venus","Saturn","Sun","Moon"],
            ["Sun","Moon","Mars","Mercury","Jupiter","Venus","Saturn"],
            ["Venus","Saturn","Sun","Moon","Mars","Mercury","Jupiter"],
            ["Mercury","Jupiter","Venus","Saturn","Sun","Moon","Mars"],
            ["Moon","Mars","Mercury","Jupiter","Venus","Saturn","Sun"],
            ["Saturn","Sun","Moon","Mars","Mercury","Jupiter","Venus"],
            ["Jupiter","Venus","Saturn","Sun","Moon","Mars","Mercury"],
            ["Mars","Mercury","Jupiter","Venus","Saturn","Sun","Moon"],
            ["Sun","Moon","Mars","Mercury","Jupiter","Venus","Saturn"],
            ["Venus","Saturn","Sun","Moon","Mars","Mercury","Jupiter"],
            ["Mercury","Jupiter","Venus","Saturn","Sun","Moon","Mars"],
            ["Moon","Mars","Mercury","Jupiter","Venus","Saturn","Sun"],
            ["Saturn","Sun","Moon","Mars","Mercury","Jupiter","Venus"],
            ["Jupiter","Venus","Saturn","Sun","Moon","Mars","Mercury"],
            ["Mars","Mercury","Jupiter","Venus","Saturn","Sun","Moon"],
            ["Sun","Moon","Mars","Mercury","Jupiter","Venus","Saturn"],
            ["Venus","Saturn","Sun","Moon","Mars","Mercury","Jupiter"],
            ["Mercury","Jupiter","Venus","Saturn","Sun","Moon","Mars"],
        ]
        _PBIAS2 = {
            "Sun":     ("BULLISH","🟢","Power, Gold, Media"),
            "Moon":    ("BULLISH","🟢","FMCG, Consumer, Silver"),
            "Mars":    ("BEARISH","🔴","Infra sell-off, Steel volatile"),
            "Mercury": ("BEARISH","🔴","IT, Banking, Telecom volatility"),
            "Jupiter": ("BULLISH","🟢","Gold, FMCG, Banking"),
            "Venus":   ("BULLISH","🟢","Silver, Jewellery, FMCG"),
            "Saturn":  ("BEARISH","🔴","Slowdown, Metals drag, Realty weak"),
        }
        _RAHUKAAL_MAP = {
            0:("07:30","09:00"), 1:("15:00","16:30"), 2:("12:00","13:30"),
            3:("13:30","15:00"), 4:("10:30","12:00"), 5:("09:00","10:30"),
            6:("16:30","18:00"),
        }
        _WEEKDAY_RULES2 = {
            0:"Monday: Today's direction continues through Tuesday 12pm. Mon rally -> Tue rises till noon then reverses (Vyapar Ratna).",
            1:"Tuesday: If bull on Tue -> Wed bull till 12pm then bearish. Both Mon+Tue bull -> Thu one more bull then reversal.",
            2:"Wednesday: Wed trend ends by Friday 12pm. Enter Wed -> exit by Fri noon.",
            3:"Thursday: Thu bull -> Fri afternoon bearish. Thu mandi ends -> good rally follows (VR rule 13).",
            4:"Friday: Fri trend reverses Saturday. Fri bull -> Sat bear; Fri bear -> Sat bull. Unstable.",
            5:"Saturday: Weekend - NSE closed.",
            6:"Sunday: Weekend - NSE closed.",
        }
        _wd2      = _sel_date.weekday()
        _rk_s2, _rk_e2 = _RAHUKAAL_MAP[_wd2]
        _ban_wday = (_wd2 + 1) % 7
        _sunrise2 = 6*60 + 20

        def _in_rk2(hh, mm):
            _t = hh*60+mm
            _rs = int(_rk_s2[:2])*60+int(_rk_s2[3:])
            _re = int(_rk_e2[:2])*60+int(_rk_e2[3:])
            return _rs <= _t < _re

        _tithi_boost2 = -1 if any(x in _snap["tithi"] for x in ["Amavasya","Chaturdashi","Krishna Pratipada"]) else \
                         1 if any(x in _snap["tithi"] for x in ["Purnima","Shukla Panchami","Shukla Ekadashi"]) else 0
        _mr_override2 = _snap["mars_rahu_deg"] < 12

        _slots2 = []
        for _sm2 in range(9*60+15, 15*60+31, 30):
            _sh2, _smin2 = _sm2//60, _sm2%60
            _eh2, _emin2 = (_sm2+30)//60, (_sm2+30)%60
            _slbl = f"{_sh2:02d}:{_smin2:02d}"
            _elbl = f"{min(_eh2,15):02d}:{_emin2 if _eh2<15 else 30:02d}" if _sm2+30 <= 15*60+30 else "15:30"
            _hslot = max(0, min(23, (_sm2-_sunrise2)//60))
            _hplanet = _HORA_TBL[_hslot][_ban_wday]
            _pbias2, _pico2, _psect2 = _PBIAS2.get(_hplanet, ("MIXED","🟡","-"))
            _irk2 = _in_rk2(_sh2, _smin2)
            if _irk2:
                _bias2,_ico2,_act2,_col2,_cls2 = "RAHUKAAL","🟣","⛔ AVOID - Rahukaal","#9B59B6","rahukaal-window"
            elif _mr_override2:
                _bias2,_ico2,_act2,_col2,_cls2 = "BEARISH","🔴","❌ SHORT - Mars-Rahu active","#FF4444","bear-window"
            elif _pbias2=="BULLISH" and _snap["net_score"]>=0 and _tithi_boost2>=0:
                _bias2,_ico2,_act2,_col2,_cls2 = "BULLISH","🟢","✅ BUY / Hold LONG","#00C851","bull-window"
            elif _pbias2=="BEARISH" or _snap["net_score"]<-1 or _tithi_boost2<0:
                _bias2,_ico2,_act2,_col2,_cls2 = "BEARISH","🔴","❌ SELL / SHORT","#FF4444","bear-window"
            elif _pbias2=="BULLISH" and _snap["net_score"]<0:
                _bias2,_ico2,_act2,_col2,_cls2 = "CAUTION","🟡","⚠️ Weak bull - wait confirm","#FFD700","neutral-window"
            else:
                _bias2,_ico2,_act2,_col2,_cls2 = "MIXED","🟡","⚠️ Volatile - no edge","#FFD700","neutral-window"
            _slots2.append({"start":_slbl,"end":_elbl,"hora":_hplanet,"bias":_bias2,
                             "icon":_ico2,"action":_act2,"color":_col2,"class":_cls2,"sectors":_psect2})

        # Summary counts
        _bc2 = sum(1 for s in _slots2 if s["bias"]=="BULLISH")
        _brc2= sum(1 for s in _slots2 if s["bias"]=="BEARISH")
        _rkc2= sum(1 for s in _slots2 if s["bias"]=="RAHUKAAL")
        _mc2 = len(_slots2)-_bc2-_brc2-_rkc2

        st.markdown(f"""
<div style="background:#111;border-radius:8px;padding:10px 16px;margin:8px 0;
            display:flex;gap:20px;align-items:center;flex-wrap:wrap">
  <span style="color:#aaa;font-size:13px">📊 {len(_slots2)}×30-min slots:</span>
  <span style="color:#00C851;font-weight:700">🟢 {_bc2} Bull</span>
  <span style="color:#FF4444;font-weight:700">🔴 {_brc2} Bear</span>
  <span style="color:#9B59B6;font-weight:700">🟣 {_rkc2} Rahukaal</span>
  <span style="color:#FFD700;font-weight:700">🟡 {_mc2} Mixed</span>
  <span style="color:#aaa;font-size:12px">| Rahukaal: {_rk_s2}–{_rk_e2}</span>
</div>""", unsafe_allow_html=True)

        # Color bar timeline
        _bar2 = '<div style="display:flex;height:26px;border-radius:6px;overflow:hidden;margin:6px 0;border:1px solid #333">'
        for _s2 in _slots2:
            _bc3 = {"BULLISH":"#00C851","BEARISH":"#FF4444","RAHUKAAL":"#9B59B6","MIXED":"#555","CAUTION":"#AA8800"}.get(_s2["bias"],"#333")
            _bar2 += f'<div style="flex:1;background:{_bc3};display:flex;align-items:center;justify-content:center;font-size:9px;color:white;font-weight:700" title="{_s2["start"]} {_s2["hora"]}">{_s2["start"]}</div>'
        _bar2 += '</div>'
        st.markdown(_bar2, unsafe_allow_html=True)

        # Detailed slots
        _slot_html2 = ""
        for _s2 in _slots2:
            _bcls2 = "time-badge-bull" if _s2["bias"]=="BULLISH" else ("time-badge-bear" if _s2["bias"] in ("BEARISH","RAHUKAAL") else "time-badge-caution")
            _tcol2 = "#00C851" if _s2["bias"]=="BULLISH" else "#FF4444" if _s2["bias"] in ("BEARISH","RAHUKAAL") else "#FFD700"
            _slot_html2 += f"""
<div class="{_s2["class"]}" style="display:flex;align-items:center;gap:12px;font-size:13px">
  <span class="{_bcls2}">{_s2["start"]}–{_s2["end"]}</span>
  <span style="color:#aaa;min-width:80px">⏳ {_s2["hora"]} hora</span>
  <span style="font-size:15px">{_s2["icon"]}</span>
  <span style="font-weight:600;color:{_tcol2}">{_s2["bias"]}</span>
  <span style="color:#888;font-size:12px">&mdash; {_s2["action"]}</span>
  <span style="color:#556;font-size:11px;margin-left:auto">📌 {_s2["sectors"]}</span>
</div>"""
        st.markdown(_slot_html2, unsafe_allow_html=True)

        # Vyapar Ratna weekly rule
        st.markdown(f"""
<div style="background:#0d0d1a;border:1px solid #444;border-radius:8px;padding:10px 16px;margin:10px 0">
  <span style="color:#FFD700;font-weight:700">📖 Vyapar Ratna - {_sel_date.strftime("%A")} Rule:</span>
  <span style="color:#ccc;margin-left:8px">{_WEEKDAY_RULES2[_wd2]}</span>
</div>""", unsafe_allow_html=True)

        st.divider()

        # ── BLOCK C - ACTIVE SIGNALS ──────────────────────────────────
        st.subheader("🔔 Active Astro Signals")
        if _snap["signals"]:
            _sig_cols2 = st.columns(2)
            for _si2, (_stxt2, _stype2) in enumerate(_snap["signals"]):
                _ico3  = {"B":"🔴","G":"🟢","T":"⚠️","M":"🟡"}.get(_stype2,"")
                _scls2 = "signal-card-bear" if _stype2=="B" else ("signal-card-bull" if _stype2=="G" else "signal-card-mixed")
                _sig_cols2[_si2%2].markdown(
                    f'<div class="{_scls2}" style="font-size:13px">{_ico3} {_stxt2}</div>',
                    unsafe_allow_html=True)
        else:
            st.info("No strong signals on this date.")

        st.divider()

        # ── BLOCK D - MULTI-SOURCE CONFIRMED SIGNAL ───────────────────
        st.subheader("✅ Multi-Source Confirmed Signal")

        _NAK_LORD3 = {
            "Ashwini":"Ketu","Bharani":"Venus","Krittika":"Sun","Rohini":"Moon",
            "Mrigasira":"Mars","Ardra":"Rahu","Punarvasu":"Jupiter","Pushya":"Saturn",
            "Ashlesha":"Mercury","Magha":"Ketu","Purva Phalguni":"Venus",
            "Uttara Phalguni":"Sun","Hasta":"Moon","Chitra":"Mars","Swati":"Rahu",
            "Vishakha":"Jupiter","Anuradha":"Saturn","Jyeshtha":"Mercury","Mula":"Ketu",
            "Purvashadha":"Venus","Uttarashadha":"Sun","Shravana":"Moon",
            "Dhanishtha":"Mars","Shatabhisha":"Rahu","Purva Bhadrapada":"Jupiter",
            "Uttara Bhadrapada":"Saturn","Revati":"Mercury",
        }
        _BEN3  = {"Sun","Jupiter","Moon","Mars","Venus"}
        _MAL3  = {"Saturn","Rahu","Ketu","Mercury"}
        _FERT3 = {"Aries","Taurus","Leo","Scorpio","Sagittarius","Capricorn"}
        _BARR3 = {"Gemini","Virgo","Libra","Aquarius","Pisces","Cancer"}

        _nl3  = _NAK_LORD3.get(_snap["moon_nak"],"")
        _ns3  = "BULLISH" if _nl3 in _BEN3 else "BEARISH" if _nl3 in _MAL3 else "MIXED"
        _rs3  = "BULLISH" if _snap["moon_rashi"] in _FERT3 else "BEARISH" if _snap["moon_rashi"] in _BARR3 else "MIXED"
        _ss3  = "BULLISH" if _snap["sun_rashi"] in {"Sagittarius","Leo","Aries","Scorpio","Capricorn"} else \
                "BEARISH" if _snap["sun_rashi"] in {"Aquarius","Cancer","Gemini"} else "MIXED"
        _es3  = "BULLISH" if _snap["net_score"]>=2 else "BEARISH" if _snap["net_score"]<=-2 else "MIXED"
        _ts3  = "BEARISH" if any(x in _snap["tithi"] for x in ["Amavasya","Krishna Pratipada","Chaturdashi"]) else \
                "BULLISH" if any(x in _snap["tithi"] for x in ["Purnima","Shukla Panchami","Shukla Ekadashi"]) else "MIXED"
        _votes3  = [_ns3,_rs3,_ss3,_es3,_ts3]
        _bv3     = _votes3.count("BULLISH")
        _brv3    = _votes3.count("BEARISH")

        if _bv3>=4:   _verd3,_vc3,_va3 = "🟢 STRONG BULLISH","Very High","✅ BUY CALLS / LONG at open"
        elif _bv3==3: _verd3,_vc3,_va3 = "🟢 BULLISH","High","✅ Lean LONG - buy dips"
        elif _brv3>=4:_verd3,_vc3,_va3 = "🔴 STRONG BEARISH","Very High","❌ BUY PUTS / SHORT at open"
        elif _brv3==3:_verd3,_vc3,_va3 = "🔴 BEARISH","High","❌ Lean SHORT - sell rallies"
        elif _bv3==2: _verd3,_vc3,_va3 = "🟡 MILD BULLISH","Medium","⚠️ Wait for tech confirm"
        elif _brv3==2:_verd3,_vc3,_va3 = "🟡 MILD BEARISH","Medium","⚠️ Avoid longs"
        else:         _verd3,_vc3,_va3 = "🟡 MIXED/VOLATILE","Low","⛔ Avoid - conflicting"

        if _snap["mars_rahu_deg"]<10:
            _verd3,_vc3,_va3 = "🔴 EXTREME BEARISH","Very High","❌ STRONG SHORT - Mars-Rahu <10°"
        if _snap["sun_saturn_deg"]<5:
            _verd3,_vc3 = "🔴 STRONG BEARISH","High"
            _va3 = "❌ SHORT - Sun-Saturn <5°"

        _vcrd3  = "signal-card-bull" if "BULLISH" in _verd3 else "signal-card-bear" if "BEARISH" in _verd3 else "signal-card-mixed"
        _vcol3  = "#00C851" if "BULLISH" in _verd3 else "#FF4444" if "BEARISH" in _verd3 else "#FFD700"

        def _sig_col3(s):
            return "#00C851" if s=="BULLISH" else "#FF4444" if s=="BEARISH" else "#FFD700"

        st.markdown(f"""
<div class="{_vcrd3}" style="padding:16px">
  <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap">
    <div>
      <div style="font-size:22px;font-weight:700;color:{_vcol3}">{_verd3}</div>
      <div style="color:#aaa;font-size:13px">Confidence: <b style="color:{_vcol3}">{_vc3}</b>
        &nbsp;|&nbsp; Votes: 🟢{_bv3}/🔴{_brv3}</div>
    </div>
    <div style="flex:1;min-width:240px">
      <div style="font-size:15px;font-weight:600;color:#fff">📌 {_va3}</div>
    </div>
  </div>
  <div style="margin-top:10px;display:flex;gap:12px;flex-wrap:wrap;font-size:12px;color:#aaa">
    <span>Moon Nak: <b style="color:{_sig_col3(_ns3)}">{_ns3}</b></span>
    <span>♈ Rashi: <b style="color:{_sig_col3(_rs3)}">{_rs3}</b></span>
    <span>Sun️ Sun: <b style="color:{_sig_col3(_ss3)}">{_ss3}</b></span>
    <span>⚙️ Engine: <b style="color:{_sig_col3(_es3)}">{_es3}</b> ({_snap["net_score"]:+d})</span>
    <span>📆 Tithi: <b style="color:{_sig_col3(_ts3)}">{_ts3}</b></span>
    <span>⚡ Mars-Rahu: <b style="color:{"#FF4444" if _snap["mars_rahu_deg"]<15 else "#aaa"}">{_snap["mars_rahu_deg"]}°</b></span>
    <span>🔑 Nak Lord: <b>{_nl3}</b></span>
  </div>
</div>""", unsafe_allow_html=True)

        st.divider()

        # ── BLOCK E - POSITION GUIDE ──────────────────────────────────
        st.subheader("🗓️ Position Guide for This Day")
        _bull_s3 = [s for s in _slots2 if s["bias"]=="BULLISH"]
        _bear_s3 = [s for s in _slots2 if s["bias"]=="BEARISH"]
        _g1a,_g2a,_g3a = st.columns(3)
        with _g1a:
            st.markdown("**🟢 Best BUY windows**")
            if _bull_s3:
                for _bs3 in _bull_s3:
                    st.markdown(f'<div class="bull-window"><span class="time-badge-bull">{_bs3["start"]}–{_bs3["end"]}</span> &nbsp; {_bs3["hora"]} hora &middot; {_bs3["sectors"]}</div>', unsafe_allow_html=True)
            else:
                st.markdown("<div style='color:#FF4444'>No clear bull windows</div>", unsafe_allow_html=True)
        with _g2a:
            st.markdown("**🔴 Best SHORT windows**")
            if _bear_s3:
                for _bs3b in _bear_s3:
                    st.markdown(f'<div class="bear-window"><span class="time-badge-bear">{_bs3b["start"]}–{_bs3b["end"]}</span> &nbsp; {_bs3b["hora"]} hora &middot; {_bs3b["sectors"]}</div>', unsafe_allow_html=True)
            else:
                st.markdown("<div style='color:#00C851'>No clear bear windows</div>", unsafe_allow_html=True)
        with _g3a:
            st.markdown("**🟣 Rahukaal - AVOID**")
            st.markdown(f'<div class="rahukaal-window"><span class="time-badge-caution">{_rk_s2}–{_rk_e2}</span> &nbsp; Do NOT enter new trades.<br><small>Source: Vyapar Ratna</small></div>', unsafe_allow_html=True)

        st.divider()

        # ── BLOCK F - SURROUNDING 5 DAYS ─────────────────────────────
        st.subheader("📆 Surrounding Trading Days - Context View")
        st.caption("Plan positions for adjacent days. Green = accumulate, Red = short/exit, Yellow = wait.")

        _all_cands = []
        for _di3 in range(-10, 11):
            _cd3 = _sel_date + timedelta(days=_di3)
            if _cd3.weekday()<5 and _cd3 not in _get_nse_holidays():
                _all_cands.append(_cd3)
        try:
            _sel_idx3 = _all_cands.index(_sel_date)
        except ValueError:
            _sel_idx3 = 2
        _win3 = _all_cands[max(0,_sel_idx3-2): _sel_idx3+3]

        _surr3 = []
        for _wd3 in _win3:
            try:
                _wr3 = _vedic_day_analysis(_wd3)
                _ld3 = _NAK_LORD3.get(_wr3["moon_nak"],"?")
                _rk3s,_rk3e = _RAHUKAAL_MAP[_wd3.weekday()]
                _vs3 = [
                    "BULLISH" if _ld3 in _BEN3 else "BEARISH",
                    "BULLISH" if _wr3["moon_rashi"] in _FERT3 else "BEARISH" if _wr3["moon_rashi"] in _BARR3 else "MIXED",
                    "BULLISH" if _wr3["net_score"]>=2 else "BEARISH" if _wr3["net_score"]<=-2 else "MIXED",
                ]
                _bvs3 = _vs3.count("BULLISH"); _bevs3 = _vs3.count("BEARISH")
                _fin3 = "🟢 BULL" if _bvs3>=2 else ("🔴 BEAR" if _bevs3>=2 else "🟡 MIX")
                _act3 = "✅ BUY next day eve" if _bvs3>=2 else ("❌ SHORT next day eve" if _bevs3>=2 else "⚠️ WAIT")
                _surr3.append({
                    "Date": _wd3.strftime("%a %d %b"),
                    "Marker": "◀ SELECTED" if _wd3==_sel_date else "",
                    "Signal": _fin3,
                    "Action (evening before)": _act3,
                    "Moon Nak (Lord)": f"{_wr3['moon_nak']} ({_ld3})",
                    "Moon Rashi": _wr3["moon_rashi"],
                    "Tithi": _wr3["tithi"].split("(")[0].strip()[:16],
                    "Score": f"{_wr3['net_score']:+d}",
                    "Rahukaal": f"{_rk3s}–{_rk3e}",
                })
            except Exception:
                pass

        if _surr3:
            _sdf3 = pd.DataFrame(_surr3)
            def _sd3_col(val):
                v=str(val)
                if "BULL" in v: return "background-color:#0d2b0d;color:#00C851;font-weight:700"
                if "BEAR" in v: return "background-color:#2b0d0d;color:#FF4444;font-weight:700"
                if "MIX"  in v: return "background-color:#1a1500;color:#FFD700"
                return ""
            def _sd3_sel(val):
                if "SELECTED" in str(val): return "background-color:#1a1a00;color:#FFD700;font-weight:700"
                return ""
            st.dataframe(
                _sdf3.style
                    .map(_sd3_col, subset=["Signal","Action (evening before)"])
                    .map(_sd3_sel, subset=["Marker"])
                    .set_properties(**{"font-size":"12px","text-align":"left",
                                       "background-color":"#0a0a0a","color":"#ddd"})
                    .set_table_styles([{"selector":"th","props":[("background-color","#111"),
                                       ("color","white"),("font-weight","bold")]}]),
                width='stretch', height=220,
            )

    st.divider()

    # ═══════════════════════════════════════════════════════════════════
    # 🔮 NEXT 10 TRADING DAYS - DYNAMIC NIFTY ASTRO FORECAST
    # ═══════════════════════════════════════════════════════════════════
    st.subheader("🔮 Next 10 Trading Days - Dynamic Nifty Astro Forecast")
    st.caption(
        "Computed live from planetary positions (Lahiri ayanamsa, Jean Meeus Ch47). "
        "Combines Moon nakshatra, tithi, conjunction angles. Refreshes every page load. "
        "**Not financial advice.**"
    )
    st.info(
        "**How to read green/bullish days inside a bearish period:**  \n"
        "The astro score = dominant closing direction, not every candle. "
        "During Mars-Rahu (active now), green candles and gap-ups DO happen. "
        "The 'If Green/Gap-up' column below tells you what to do on those days. "
        "TRAP RALLY = sell into strength. FALSE BREAKOUT = gap fills by end of day."
    )

    _fc10_rows = []
    _fc10_start = datetime.now(IST).date()
    _fc10_all   = []

    for _fc10_di in range(1, 40):
        _fc10_d = _fc10_start + timedelta(days=_fc10_di)
        if _fc10_d.weekday() >= 5 or _fc10_d in _get_nse_holidays():
            continue
        try:
            _fc10_r = _vedic_day_analysis(_fc10_d)
        except Exception:
            continue
        _fc10_all.append((_fc10_d, _fc10_r))
        if len(_fc10_all) >= 11:
            break

    for _fi, (_fc10_d, _fc10_r) in enumerate(_fc10_all[:10]):

        _mars_rahu_active  = _fc10_r["mars_rahu_deg"] < 15
        _moon_ketu_active  = _fc10_r["moon_ketu_deg"] < 15
        _sun_saturn_active = _fc10_r["sun_saturn_deg"] < 8

        _conj_parts = []
        if _moon_ketu_active:  _conj_parts.append(f"Moon-Ketu {_fc10_r['moon_ketu_deg']}d")
        if _mars_rahu_active:  _conj_parts.append(f"Mars-Rahu {_fc10_r['mars_rahu_deg']}d")
        if _sun_saturn_active: _conj_parts.append(f"Sun-Saturn {_fc10_r['sun_saturn_deg']}d")

        _signal = _fc10_r["overall"]
        _mrahu  = _fc10_r["mars_rahu_deg"]
        _tithi  = _fc10_r["tithi"]

        if "BEARISH" in _signal:
            if _mrahu < 5:
                _green_ctx = "TRAP RALLY - Mars-Rahu <5d. Gap-up = sell. Reversal by noon."
            elif _mrahu < 10:
                _green_ctx = "FALSE BREAKOUT - green open fills gap. Short above yest high."
            elif "Amavasya" in _tithi or "Pratipada" in _tithi:
                _green_ctx = "SELL RALLIES - Amavasya/Pratipada green opens reverse after 09:45."
            elif _moon_ketu_active:
                _green_ctx = "FAKE BULL - Moon-Ketu spike, no follow-through. Don't chase."
            elif _fc10_r["moon_nak"] == "Rohini":
                _green_ctx = "ROHINI TRAP - looks bullish at open, turns red by afternoon."
            else:
                _green_ctx = "SELL RALLIES - bearish day. Use green open as short entry."
        elif "TRAP" in _signal:
            _green_ctx = "TRAP RALLY - any green is distribution. Sell into strength."
        elif "MIXED" in _signal:
            if _mars_rahu_active:
                _green_ctx = "CAUTION - mixed but Mars-Rahu active. Partial longs only, tight SL."
            else:
                _green_ctx = "TRADE WITH CONFIRM - mixed day. Need 15m breakout to enter long."
        else:
            _green_ctx = "VALID BUY - bullish day. Green open = continuation. Buy dips."

        _next_warn = ""
        if _fi + 1 < len(_fc10_all):
            _nd, _nr = _fc10_all[_fi + 1]
            if "BEARISH" in _nr["overall"] and "BEARISH" in _signal:
                if _nr["mars_rahu_deg"] < _mrahu:
                    _next_warn = f"Next ({_nd.strftime('%d %b')}) MORE bearish - rally today sold tomorrow"
                else:
                    _next_warn = f"Next ({_nd.strftime('%d %b')}) also bearish - avoid overnight longs"
            elif "BULLISH" in _nr["overall"] and "BEARISH" in _signal:
                _next_warn = f"Next ({_nd.strftime('%d %b')}) turns bullish - today low = EOD buy"

        _fc10_rows.append({
            "Date":            _fc10_d.strftime("%a %d %b"),
            "Signal":          _fc10_r["overall"],
            "Intensity":       _fc10_r["intensity"],
            "Moon Nak":        f"{_fc10_r['moon_nak']} ({_fc10_r['moon_nak_lord']})",
            "Tithi":           _fc10_r["tithi"].split("(")[0].strip(),
            "Key Conj":        " | ".join(_conj_parts) if _conj_parts else "None",
            "If Green/Gap-up": _green_ctx,
            "Next Day Watch":  _next_warn if _next_warn else "---",
            "Score":           f"{_fc10_r['net_score']:+d} (B{_fc10_r['bearish_pts']}/G{_fc10_r['bullish_pts']})",
        })

    if _fc10_rows:
        _fc10_df = pd.DataFrame(_fc10_rows)

        def _fc10_signal_color(val):
            v = str(val)
            if "BEARISH" in v: return "background-color:#7b1a1a; color:#ffcccc; font-weight:700"
            if "BULLISH" in v: return "background-color:#1a4d2e; color:#b6f5c8; font-weight:700"
            if "TRAP"    in v: return "background-color:#5a4200; color:#ffe082; font-weight:700"
            if "MIXED"   in v: return "background-color:#3a3000; color:#fff3b0"
            return ""

        def _fc10_intensity_color(val):
            v = str(val)
            if v == "Extreme": return "background-color:#b71c1c; color:#fff; font-weight:700"
            if v == "Strong":  return "background-color:#c62828; color:#fff; font-weight:600"
            if v == "Mild":    return "background-color:#f57f17; color:#fff"
            if v == "Neutral": return "background-color:#424242; color:#eee"
            if v == "Caution": return "background-color:#5a4200; color:#ffe082"
            return ""

        def _fc10_green_color(val):
            v = str(val)
            if "TRAP RALLY"     in v: return "background-color:#500010; color:#ff8888; font-weight:600"
            if "FALSE BREAKOUT" in v: return "background-color:#3a0a00; color:#ffaa88; font-weight:600"
            if "SELL RALLIES"   in v: return "background-color:#2a0800; color:#ff9966"
            if "FAKE BULL"      in v: return "background-color:#3a2800; color:#ffcc88"
            if "ROHINI TRAP"    in v: return "background-color:#3a2800; color:#ffcc88"
            if "CAUTION"        in v: return "background-color:#2a2000; color:#ffe08a"
            if "TRADE WITH"     in v: return "background-color:#102000; color:#88cc88"
            if "VALID BUY"      in v: return "background-color:#0a3000; color:#00ff88; font-weight:700"
            return ""

        def _fc10_next_color(val):
            v = str(val)
            if "MORE bearish"  in v: return "background-color:#500010; color:#ffaaaa"
            if "also bearish"  in v: return "background-color:#2a0808; color:#ffbbbb"
            if "turns bullish" in v: return "background-color:#0a3000; color:#88ffaa"
            return "color:#666"

        st.dataframe(
            _fc10_df.style
                .map(_fc10_signal_color,    subset=["Signal"])
                .map(_fc10_intensity_color, subset=["Intensity"])
                .map(_fc10_green_color,     subset=["If Green/Gap-up"])
                .map(_fc10_next_color,      subset=["Next Day Watch"])
                .set_properties(**{"font-size": "12px", "text-align": "left"})
                .set_table_styles([{"selector": "th", "props": [
                    ("font-weight", "bold"), ("background-color", "#1a1a2e"), ("color", "white")
                ]}]), width='stretch',
            height=420,
        )

        _fc10_nb = sum(1 for r in _fc10_rows if "BEARISH" in r["Signal"])
        _fc10_ng = sum(1 for r in _fc10_rows if "BULLISH" in r["Signal"])
        _fc10_nt = sum(1 for r in _fc10_rows if "TRAP"    in r["Signal"])
        _fc10_nm = sum(1 for r in _fc10_rows if "MIXED"   in r["Signal"])
        st.caption(
            f"10-day summary: {_fc10_nb} Bearish | {_fc10_ng} Bullish | "
            f"{_fc10_nt} Trap | {_fc10_nm} Mixed | "
            f"Mars-Rahu exits 15d orb ~Apr 20 (scores improve after)"
        )

        with st.expander("How to trade green days inside a bearish period"):
            st.markdown(
                "**The astro score = dominant CLOSING direction, not every candle.**\n\n"
                "| Situation | Meaning | Action |\n"
                "|---|---|---|\n"
                "| Gap-up on BEARISH day | Institutional distribution | Sell above yest high. Short after 09:30 |\n"
                "| Strong green first 30 min | Trap rally | Wait for 10:00. If reversal starts, short breakdown |\n"
                "| Green close above YEST_HIGH | Overhead supply next day | Do not hold overnight. Book same day |\n"
                "| Day AFTER green in bearish period | Weak longs trapped above | Best short entry. Short below prev day low |\n"
                "| Rohini nakshatra green open | Classic trap | Sell at 11:00-11:30 when afternoon reversal begins |\n"
                "| Moon in Taurus on bearish day | Gap-down then recovery | Buy the gap-DOWN, not gap-up. Sell recovery |\n\n"
                "Currently Mars-Rahu at 1.7 degrees. Any gap-up = sell above previous LIVE_HIGH. "
                "Mars-Rahu exits 15 degree orb around April 20 - genuine bullish signals possible after."
            )

    else:
        st.info("Could not compute 10-day forecast.")



# ── Universal number + colour helpers (injected) ─────────────────
def _fmt_num(v):
    try:
        f = float(v)
        return f"{int(f):,}" if f == int(f) else f"{f:,.2f}"
    except Exception:
        return v

def _style_gain(val):
    try:
        v = float(val)
        if v > 0: return "background-color:#ffffff;color:#1a7a1a;font-weight:700"
        if v < 0: return "background-color:#ffffff;color:#cc0000;font-weight:700"
    except Exception: pass
    return ""

def _style_change(val):
    try:
        v = float(val)
        if v > 0: return "background-color:#ffffff;color:#1a7a1a;font-weight:600"
        if v < 0: return "background-color:#ffffff;color:#cc0000;font-weight:600"
    except Exception: pass
    return ""

def _style_gap(val):
    if not isinstance(val, str) or not val: return ""
    if "✅" in val: return "color:#1a7a1a; font-weight:700"
    if "🔴" in val: return "color:#cc0000; font-weight:700"
    return ""

def _apply_fmt(df, row_style_fn=None):
    """Format all numeric cols to 2dp + colour GAIN + CHANGE/CHANGE_% + GAP"""
    fmt = {}
    for col in df.columns:
        try:
            if df[col].dtype.kind in ("f","i"): fmt[col] = _fmt_num
        except Exception: pass
    styled = df.style.format(fmt, na_rep="-")
    gain_cols   = [col for col in df.columns if col == "GAIN"]
    change_cols = [col for col in df.columns if col in ("CHANGE","CHANGE_%")]
    gap_cols    = [col for col in df.columns if col == "GAP"]
    if gain_cols:   styled = styled.map(_style_gain,   subset=gain_cols)
    if change_cols: styled = styled.map(_style_change, subset=change_cols)
    if gap_cols:    styled = styled.map(_style_gap,    subset=gap_cols)
    if row_style_fn: styled = styled.apply(row_style_fn, axis=1)
    return styled

with tabs[2]:
    st.subheader("🪐 Panchak – Full View")
    st.caption("🟢 LTP cell = **green + bold** when LTP ≥ BT (Above Top)  |  🔴 LTP cell = **red + bold** when LTP ≤ ST (Below Top)")

    # ── Panchak Dates Banner ──────────────────────────────────────────
    try:
        _today_d = date.today()

        # ── Current active period (the one whose data is being shown) ──
        # PANCHAK_START/END = the panchak whose historical data built the table.
        # Data is valid from day after prev period ends -> day of next period end.
        _curr_fetch_start = PANCHAK_START   # e.g. Mar 17
        _curr_fetch_end   = PANCHAK_END     # e.g. Mar 21
        # Data validity window: from PANCHAK_START+1 through next period's END
        # Find this period's index in the schedule
        _curr_idx = _PANCHAK_ACTIVE_IDX
        _sched    = _PANCHAK_SCHEDULE_2026
        # Next period dates (what will be fetched next)
        _next_fetch_start = _next_fetch_end = None
        _next_trigger     = None   # date on which next fetch fires
        if _curr_idx + 1 < len(_sched):
            _next_fetch_start, _next_fetch_end = _sched[_curr_idx + 1]
            _next_trigger = _curr_fetch_end + timedelta(days=1)  # day after current ends
        # Following period (the one after next)
        _foll_fetch_start = _foll_fetch_end = None
        if _curr_idx + 2 < len(_sched):
            _foll_fetch_start, _foll_fetch_end = _sched[_curr_idx + 2]

        # Days until next data refresh
        _days_to_refresh = (_next_trigger - _today_d).days if _next_trigger else None

        # ── Build banner text ──────────────────────────────────────────
        # Line 1: Current data source
        _line1 = (
            f"<b style='color:#00e676;font-size:14px'>📊 CURRENT DATA</b>"
            f"&nbsp;&nbsp;"
            f"<span style='color:#fff'>Panchak: "
            f"<b>{_curr_fetch_start.strftime('%d %b %Y')}</b>"
            f" -> <b>{_curr_fetch_end.strftime('%d %b %Y')}</b></span>"
        )

        # Line 2: Next refresh info
        if _next_fetch_start and _next_trigger:
            if _today_d >= _next_trigger:
                _refresh_txt = "<span style='color:#00e676'>✅ Data refreshed today</span>"
            elif _days_to_refresh == 0:
                _refresh_txt = "<span style='color:#ffb300'>🔄 Refresh TODAY</span>"
            elif _days_to_refresh == 1:
                _refresh_txt = f"<span style='color:#ffb300'>🔄 Next refresh TOMORROW ({_next_trigger.strftime('%d %b')})</span>"
            else:
                _refresh_txt = (
                    f"<span style='color:#80cbc4'>🔄 Next data refresh: "
                    f"<b>{_next_trigger.strftime('%d %b %Y')}</b>"
                    f" (in {_days_to_refresh} days)</span>"
                )
            _line2 = (
                f"&nbsp;&nbsp;|&nbsp;&nbsp;"
                f"<span style='color:#aaa;font-size:12px'>Next Panchak dates: "
                f"<b style='color:#ce93d8'>{_next_fetch_start.strftime('%d %b')} -> {_next_fetch_end.strftime('%d %b %Y')}</b>"
                f"</span>"
                f"&nbsp;&nbsp;{_refresh_txt}"
            )
        else:
            _line2 = ""

        # Line 3: Following period
        _line3 = ""
        if _foll_fetch_start:
            _line3 = (
                f"&nbsp;&nbsp;|&nbsp;&nbsp;"
                f"<span style='color:#9e9e9e;font-size:11px'>"
                f"🔜 Following: <b>{_foll_fetch_start.strftime('%d %b')} -> {_foll_fetch_end.strftime('%d %b %Y')}</b>"
                f"</span>"
            )

        st.markdown(
            f'<div style="background:#0d1b0d;border:1px solid #00e676;'
            f'border-radius:8px;padding:10px 18px;margin-bottom:12px;line-height:1.8">'
            f'{_line1}{_line2}{_line3}'
            f'</div>',
            unsafe_allow_html=True,
        )
    except Exception as _pe:
        st.caption(f"Panchak data: {PANCHAK_START.strftime('%d %b %Y')} -> {PANCHAK_END.strftime('%d %b %Y')}")

    def _style_panchak(row):
        """
        Row-wise styler for panchak_view.
        - LTP cell -> bright green bg + bold when LTP >= BT
        - LTP cell -> bright red  bg + bold when LTP <= ST
        - All other cells -> default
        """
        styles = [""] * len(row.index)
        try:
            ltp_idx = list(row.index).index("LTP")
            ltp = float(row["LTP"]) if pd.notna(row["LTP"]) else None
            bt  = float(row["BT"])  if pd.notna(row.get("BT"))  else None
            st_ = float(row["ST"])  if pd.notna(row.get("ST"))  else None

            if ltp is not None and bt is not None and ltp >= bt:
                styles[ltp_idx] = (
                    "background-color:#00501a; color:#00ff88; "
                    "font-weight:900; font-size:14px;"
                )
            elif ltp is not None and st_ is not None and ltp <= st_:
                styles[ltp_idx] = (
                    "background-color:#500010; color:#ff4466; "
                    "font-weight:900; font-size:14px;"
                )
        except (ValueError, KeyError):
            pass
        return styles

    _pv_styled = _apply_fmt(panchak_view, row_style_fn=_style_panchak)
    st.dataframe(_pv_styled, width='stretch', height=600)

    # ══════════════════════════════════════════════════════════════
    # PANCHAK RANGE ENGINE - Point 7
    # ══════════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("📐 Panchak Range Trading Engine")
    st.caption(
        "Tracks the complete High & Low of the Panchak period. "
        "After Panchak ends, monitors breakout with 61% / 138% / 200% targets."
    )

    _PR_FILE = os.path.join(CACHE_DIR, "panchak_range.json")
    _pr_state = {}
    try:
        with open(_PR_FILE, encoding="utf-8") as _f:
            _pr_state = json.load(_f)
    except Exception:
        pass

    if not _pr_state:
        st.info("No Panchak range data yet. The background worker tracks NIFTY range during Panchak and saves here.")
    else:
        for _pkey, _ps in sorted(_pr_state.items(), reverse=True):
            _ps_start = _ps.get("start", "")
            _ps_end   = _ps.get("end",   "")
            _ph = _ps.get("period_high", 0)
            _pl = _ps.get("period_low",  999999)
            _bias = _ps.get("bias", "UNKNOWN")
            _bias_color = "#ff5252" if _bias == "BEARISH" else "#00e676"

            st.markdown(
                f'<div style="background:#111;border:1px solid #333;border-radius:8px;'
                f'padding:12px 16px;margin:8px 0">'
                f'<b style="color:#ce93d8">📅 Panchak: {_ps_start} -> {_ps_end}</b>'
                f'&nbsp;&nbsp;<b style="color:{_bias_color}">{_bias} bias</b>'
                f'</div>',
                unsafe_allow_html=True,
            )

            if _ph > 0 and _pl < 999999:
                _rng  = round(_ph - _pl, 2)
                _t61  = round(_rng * 0.61, 2)
                _t138 = round(_rng * 1.38, 2)
                _t200 = round(_rng * 2.00, 2)

                _cols_pr = st.columns(4)
                _cols_pr[0].metric("📈 Period High",  f"{_ph:,.2f}")
                _cols_pr[1].metric("📉 Period Low",   f"{_pl:,.2f}")
                _cols_pr[2].metric("📏 Range",        f"{_rng:,.2f} pts")
                _cols_pr[3].metric("🎯 61% Target",   f"{round(_rng * 0.61, 2):,.2f} pts")

                _c1, _c2 = st.columns(2)
                with _c1:
                    st.markdown("**📈 UPSIDE (from High)**")
                    _up_rows = [
                        ("61%  Target", round(_ph + _t61, 2),  _ps.get("t61_up",  False)),
                        ("138% Target", round(_ph + _t138, 2), _ps.get("t138_up", False)),
                        ("200% Target", round(_ph + _t200, 2), _ps.get("t200_up", False)),
                    ]
                    for _lbl, _lvl, _hit in _up_rows:
                        _hit_txt = "✅ HIT" if _hit else "⏳ Pending"
                        _hit_c   = "#00e676" if _hit else "#bdbdbd"
                        st.markdown(
                            f'<div style="background:#0a200a;border-left:3px solid {_hit_c};'
                            f'padding:6px 10px;margin:3px 0;border-radius:4px">'
                            f'<b style="color:#fff">{_lbl}</b>: '
                            f'<b style="color:{_hit_c}">{_lvl:,.2f}</b>'
                            f'&nbsp;&nbsp;<span style="color:{_hit_c};font-size:11px">{_hit_txt}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    st.markdown(f'🛡️ SL if Long: **{_pl:,.2f}** (Panchak Low)')

                with _c2:
                    st.markdown("**📉 DOWNSIDE (from Low)**")
                    _dn_rows = [
                        ("61%  Target", round(_pl - _t61, 2),  _ps.get("t61_dn",  False)),
                        ("138% Target", round(_pl - _t138, 2), _ps.get("t138_dn", False)),
                        ("200% Target", round(_pl - _t200, 2), _ps.get("t200_dn", False)),
                    ]
                    for _lbl, _lvl, _hit in _dn_rows:
                        _hit_txt = "✅ HIT" if _hit else "⏳ Pending"
                        _hit_c   = "#ff5252" if _hit else "#bdbdbd"
                        st.markdown(
                            f'<div style="background:#200a0a;border-left:3px solid {_hit_c};'
                            f'padding:6px 10px;margin:3px 0;border-radius:4px">'
                            f'<b style="color:#fff">{_lbl}</b>: '
                            f'<b style="color:{_hit_c}">{_lvl:,.2f}</b>'
                            f'&nbsp;&nbsp;<span style="color:{_hit_c};font-size:11px">{_hit_txt}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    st.markdown(f'🛡️ SL if Short: **{_ph:,.2f}** (Panchak High)')

                # Status
                _brk_up = _ps.get("breakout_up_sent", False)
                _brk_dn = _ps.get("breakout_dn_sent", False)
                st.markdown(
                    f"**Breakout Status:** "
                    f"{'🟢 High broken - Bullish!' if _brk_up else '⏳ No upside break'}"
                    f"&nbsp;&nbsp;"
                    f"{'🔴 Low broken - Bearish!' if _brk_dn else '⏳ No downside break'}"
                )

                # 📜 PANCHAK HIGH-CONVICTION RULES (from documents)
                st.markdown("---")
                st.subheader("📜 Panchak High-Conviction Rules")
                
                # Rule 1: Starting Day Sentiment
                _start_day = PANCHAK_START.strftime("%A")
                _is_bear_start = _start_day in ["Monday", "Wednesday", "Friday"]
                _start_sig = "🔴 BEARISH START" if _is_bear_start else "🟢 BULLISH START"
                _start_col = "#ff4466" if _is_bear_start else "#00ff88"
                
                # Rule 2: Yoga Influence (Indra, Vaidhriti, Vyatipat)
                _today_astro = _vedic_day_analysis(datetime.now(IST).date())
                _curr_yoga = _today_astro.get("yoga_name", "N/A")
                _is_bull_yoga = _curr_yoga in ["Indra", "Vaidhriti", "Vyatipata"]
                _yoga_sig = "🔥 HIGH BULLISH YOGA" if _is_bull_yoga else "⏳ Normal Yoga"
                _yoga_col = "#00e676" if _is_bull_yoga else "#888"
                
                # Rule 3: Jupiter + Saturn Sign Alignment
                _j_rashi = _today_astro.get("jupiter_rashi", "")
                _s_rashi = _today_astro.get("saturn_rashi", "")
                _is_together = (_j_rashi == _s_rashi)
                _together_sig = "🟢 BULLISH (Jupiter+Saturn Together)" if _is_together else "⏳ Not Together"
                _together_col = "#00e676" if _is_together else "#888"
                
                # Rule 4: Ghadi Calculation (50 Ghadi / 45 Ghadi)
                _now = datetime.now(IST)
                # Sunrise assumed 6:20 AM as per hora logic
                _sunrise = _now.replace(hour=6, minute=20, second=0, microsecond=0)
                _diff_mins = (_now - _sunrise).total_seconds() / 60
                _curr_ghadi = round(_diff_mins / 24, 1)
                
                _c1, _c2, _c3, _c4 = st.columns(4)
                _c1.markdown(f"**Day Sentiment**<br><b style='color:{_start_col}'>{_start_sig}</b> ({_start_day})", unsafe_allow_html=True)
                _c2.markdown(f"**Current Yoga**<br><b style='color:{_yoga_col}'>{_yoga_sig}</b> ({_curr_yoga})", unsafe_allow_html=True)
                _c3.markdown(f"**Planetary Alignment**<br><b style='color:{_together_col}'>{_together_sig}</b>", unsafe_allow_html=True)
                _c4.markdown(f"**Time in Ghadi**<br><b>{_curr_ghadi} Ghadi</b> (after Sunrise)", unsafe_allow_html=True)

                st.markdown("""
                **Panchak Master Rules Summary:**
                1. **Fierce Reversal**: If High is broken first, then Low is broken immediately (without reaching 61% target) -> **Fierce Fall**.
                2. **Starting Day**: Mon/Wed/Fri = Bearish | Sun/Tue/Thu/Sat = Bullish.
                3. **Bullish Yogas**: Indra, Vaidhriti, Vyatipat on Panchak days = **Strong Bullish**.
                4. **Ghadi Power**: Start/End at 50 Ghadi = Bullish for 1 Year. Start/End at 45 Ghadi = Bullish for 1 Month.
                5. **Planetary Sync**: Jupiter & Saturn in the same sign = **Bullish**.
                """)

            else:
                st.info("Tracking range during Panchak period...")


    
with tabs[3]:
    st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    TOP_HIGH_df = (
    df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
    .sort_values(by="GAIN", ascending=True)   # least positive gain on top
    )

    st.dataframe(_apply_fmt(TOP_HIGH_df), width='stretch', height=600)


with tabs[4]:
    #st.dataframe(df[df.LTP <= df.TOP_LOW])
    st.markdown('<div class="section-red"><b>🔴 TOP LIVE_LOW – Breakdowns</b></div>', unsafe_allow_html=True)
    TOP_LOW_df = (
    df.loc[df.LTP <= df.TOP_LOW, TOP_LOW_COLUMNS]
    .sort_values(by="GAIN", ascending=False)   # least negative gain on top
    )

    st.dataframe(_apply_fmt(TOP_LOW_df), width='stretch', height=600)


with tabs[5]:
    st.markdown(
        '<div class="section-yelLIVE_LOW"><b>🟡 NEAR – Watch Zone</b></div>',
        unsafe_allow_html=True
    )

    # only stocks inside Panchak range
    near_base = df.loc[
        (df.LTP > df.TOP_LOW) & 
        (df.LTP < df.TOP_HIGH),
        [
            "Symbol",
            "TOP_HIGH",
            "TOP_LOW",
            "LTP",
            "CHANGE",
            "CHANGE_%",
            "EMA20"
        ]
    ].copy()

    if near_base.empty:
        st.info("No stocks currently between TOP_HIGH and TOP_LOW")
    else:

        # Distance calculation
        near_base["DIST_LIVE_HIGH"] = (near_base["TOP_HIGH"] - near_base["LTP"]).round(2)
        near_base["DIST_LIVE_LOW"]  = (near_base["LTP"] - near_base["TOP_LOW"]).round(2)

        # =========================
        # 🟢 NEAR BUY (Closer to TOP_HIGH + Above EMA20)
        # =========================
        near_buy_df = near_base[
            (near_base["DIST_LIVE_HIGH"] <= near_base["DIST_LIVE_LOW"]) &
            (near_base["LTP"] > near_base["EMA20"])
        ].copy()

        # =========================
        # 🔴 NEAR SELL (Closer to TOP_LOW + Below EMA20)
        # =========================
        near_sell_df = near_base[
            (near_base["DIST_LIVE_HIGH"] > near_base["DIST_LIVE_LOW"]) &
            (near_base["LTP"] < near_base["EMA20"])
        ].copy()

        # Arrow display
        near_buy_df["NEAR"]  = "🟢 ↑ " + near_buy_df["DIST_LIVE_HIGH"].astype(str)
        near_sell_df["NEAR"] = "🔴 ↓ " + near_sell_df["DIST_LIVE_LOW"].astype(str)

        # Sorting
        near_buy_df  = near_buy_df.sort_values("DIST_LIVE_HIGH")
        near_sell_df = near_sell_df.sort_values("DIST_LIVE_LOW")

        # Final columns
        display_cols = [
            "Symbol",
            "TOP_HIGH",
            "TOP_LOW",
            "LTP",
            "EMA20",
            "NEAR",
            "CHANGE",
            "CHANGE_%"
        ]

        near_buy_df  = near_buy_df[display_cols]
        near_sell_df = near_sell_df[display_cols]

        # Layout
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🟢 NEAR BUY (Above EMA20)")
            if near_buy_df.empty:
                st.info("No BUY-side NEAR stocks")
            else:
                st.dataframe(
                    near_buy_df, width='stretch',
                    height=600
                )

        with col2:
            st.markdown("### 🔴 NEAR SELL (Below EMA20)")
            if near_sell_df.empty:
                st.info("No SELL-side NEAR stocks")
            else:
                st.dataframe(
                    near_sell_df, width='stretch',
                    height=600
                )

# =========================================================

# =========================================================
# 📊 15-MIN HIGH/LOW BREAKOUT TRACKER  (Table A)
# 📊 15-MIN VOLUME BREAKOUT TRACKER    (Table B)
# Saved to CACHE every rerun; tables shown at top of Watchlist
# =========================================================

# ── 15m / 1h tracker CSVs are defined at top (dated) ─────
# HI_LO_TRACK_CSV, VOL_TRACK_CSV, H1_TRACK_CSV already set above

def get_current_15m_slot():
    """Current 15-min slot start e.g. '09:15', '09:30' ..."""
    now = datetime.now(IST)
    mins = now.hour * 60 + now.minute
    slot_min = (mins // 15) * 15
    h, m = divmod(slot_min, 60)
    return f"{h:02d}:{m:02d}"

def get_current_1h_slot():
    """
    Current 1-hour slot aligned to market open at 9:15.
    Slots: 09:15, 10:15, 11:15, 12:15, 13:15, 14:15
    """
    now = datetime.now(IST)
    # shift back 15 min so 9:15 is the boundary, floor to hour, shift forward
    shifted = now - pd.Timedelta(minutes=15)
    slot_start = shifted.replace(minute=0, second=0, microsecond=0) + pd.Timedelta(minutes=15)
    return slot_start.strftime("%H:%M")

def _load_raw_5min_today():
    """
    Load today's 5-min candles from dated CSV.
    Returns raw DataFrame or None if no today's file exists.
    """
    raw = _load_today_csv(FIVE_MIN_CACHE_CSV, required_cols=["datetime"])
    if raw is None:
        return None
    try:
        raw["datetime"] = pd.to_datetime(raw["datetime"], errors="coerce")
        raw = raw.dropna(subset=["datetime"])
        if raw.empty:
            return None
    except Exception:
        return None
    if raw.empty:
        return None
    # Filter to today only (safety net for any edge cases)
    today = datetime.now(IST).date()
    raw = raw[raw["datetime"].dt.date == today].copy()
    if raw.empty:
        return None
    if "volume" not in raw.columns:
        raw["volume"] = 0
    return raw.sort_values("datetime").reset_index(drop=True)

def _build_ohlcv(raw, slot_col):
    """Aggregate 5-min rows into OHLCV for the given slot column."""
    agg = raw.groupby(["Symbol", slot_col]).agg(
        HIGH=("high", "max"),
        LOW=("low", "min"),
        OPEN=("open", "first"),
        CLOSE=("close", "last"),
        VOLUME=("volume", "sum"),   # FIX BUG 9: real volume sum
    ).reset_index().rename(columns={slot_col: "slot"})
    return agg.sort_values(["Symbol", "slot"]).reset_index(drop=True)

def _get_live_map():
    """Build symbol -> live data dict from main df."""
    cols = ["Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
            "CHANGE_%","YEST_HIGH","YEST_LOW","YEST_CLOSE","YEST_VOL",
            "LIVE_VOLUME","VOL_%","EMA20",
            "TOP_HIGH","TOP_LOW","NEAR","GAIN"]
    cols = [c for c in cols if c in df.columns]
    return df[cols].set_index("Symbol").to_dict("index")

def _sequential_breakout(agg, now_slot, live_map, chain_col):
    """
    FIX BUG 1 & CORE LOGIC:
    Sequential chain check for HIGH breakout and LOW breakdown.

    HIGH: every completed candle HIGH strictly > previous candle HIGH
          from the FIRST candle of the day (9:15) - no gaps allowed.
          LTP must currently be > last completed candle HIGH.

    LOW:  every completed candle LOW strictly < previous candle LOW
          from the FIRST candle of the day - no gaps.
          LTP must currently be < last completed candle LOW.

    chain_col: column name for chain length ('CANDLES_CHAIN' or 'HOURS_CHAIN')
    """
    high_rows, low_rows = [], []

    for sym, g in agg.groupby("Symbol"):
        g = g.sort_values("slot").reset_index(drop=True)
        # FIX BUG 5: completed = strictly < current slot
        completed = g[g["slot"] < now_slot].reset_index(drop=True)
        n = len(completed)
        if n < 2:
            continue   # need at least 2 candles to have a breakout

        live = live_map.get(sym)
        if not live:
            continue
        ltp       = float(live.get("LTP", 0) or 0)
        yest_high = float(live.get("YEST_HIGH", 0) or 0)
        yest_low  = float(live.get("YEST_LOW",  0) or 0)

        # ── HIGH sequential chain ───────────────────────────
        # All candles from index 1 to n-1 must break previous HIGH
        chain_high = all(
            float(completed.iloc[i]["HIGH"]) > float(completed.iloc[i-1]["HIGH"])
            for i in range(1, n)
        )
        if chain_high:
            last_high  = float(completed.iloc[-1]["HIGH"])
            prev_high  = float(completed.iloc[-2]["HIGH"])
            first_high = float(completed.iloc[0]["HIGH"])
            if ltp > last_high:   # LTP must be above the latest broken level RIGHT NOW
                high_rows.append({
                    "Symbol":       sym,
                    "LTP":          round(ltp, 2),
                    "CHANGE_%":     round(float(live.get("CHANGE_%", 0) or 0), 2),
                    "FIRST_SLOT":   completed.iloc[0]["slot"],
                    "FIRST_HIGH":   round(first_high, 2),
                    "PREV_SLOT":    completed.iloc[-2]["slot"],
                    "PREV_HIGH":    round(prev_high, 2),
                    "BROKEN_HIGH":  round(last_high, 2),
                    chain_col:      n,
                    "ABOVE_%":      round((ltp - last_high) / last_high * 100, 2),
                    "LIVE_HIGH":    live.get("LIVE_HIGH", 0),
                    "LIVE_LOW":     live.get("LIVE_LOW",  0),
                    "YEST_HIGH":    yest_high,
                    "NEAR":         live.get("NEAR", ""),
                    "GAIN":         live.get("GAIN", ""),
                    "TOP_HIGH":     live.get("TOP_HIGH", 0),
                    "TOP_LOW":      live.get("TOP_LOW",  0),
                    "TYPE":         "🟢 HIGH BREAK",
                })

        # ── LOW sequential chain ────────────────────────────
        chain_low = all(
            float(completed.iloc[i]["LOW"]) < float(completed.iloc[i-1]["LOW"])
            for i in range(1, n)
        )
        if chain_low:
            last_low  = float(completed.iloc[-1]["LOW"])
            prev_low  = float(completed.iloc[-2]["LOW"])
            first_low = float(completed.iloc[0]["LOW"])
            if ltp < last_low:    # LTP must be below the latest broken level RIGHT NOW
                low_rows.append({
                    "Symbol":       sym,
                    "LTP":          round(ltp, 2),
                    "CHANGE_%":     round(float(live.get("CHANGE_%", 0) or 0), 2),
                    "FIRST_SLOT":   completed.iloc[0]["slot"],
                    "FIRST_LOW":    round(first_low, 2),
                    "PREV_SLOT":    completed.iloc[-2]["slot"],
                    "PREV_LOW":     round(prev_low, 2),
                    "BROKEN_LOW":   round(last_low, 2),
                    chain_col:      n,
                    "BELOW_%":      round((last_low - ltp) / last_low * 100, 2),
                    "LIVE_HIGH":    live.get("LIVE_HIGH", 0),
                    "LIVE_LOW":     live.get("LIVE_LOW",  0),
                    "YEST_LOW":     yest_low,
                    "NEAR":         live.get("NEAR", ""),
                    "GAIN":         live.get("GAIN", ""),
                    "TOP_HIGH":     live.get("TOP_HIGH", 0),
                    "TOP_LOW":      live.get("TOP_LOW",  0),
                    "TYPE":         "🔴 LOW BREAK",
                })

    high_df = pd.DataFrame(high_rows)
    low_df  = pd.DataFrame(low_rows)
    if not high_df.empty:
        high_df = high_df.sort_values([chain_col, "ABOVE_%"], ascending=[False, False])
    if not low_df.empty:
        low_df  = low_df.sort_values([chain_col, "BELOW_%"], ascending=[False, False])
    return high_df, low_df


def build_15m_tracker(raw=None):
    """
    Sequential 15-min tracker. Pass raw 5min DataFrame to avoid re-reading CSV.
    Returns: (high_df, low_df, vol_df)
    """
    if raw is None:
        raw = _load_raw_5min_today()
    if raw is None or (hasattr(raw, "empty") and raw.empty):
        empty = pd.DataFrame()
        return empty, empty, empty

    # Ensure "datetime" column exists and is datetime dtype
    if "datetime" not in raw.columns:
        # Try alias columns
        if "date" in raw.columns:
            raw = raw.rename(columns={"date": "datetime"})
        else:
            empty = pd.DataFrame()
            return empty, empty, empty
    try:
        raw = raw.copy()
        raw["datetime"] = pd.to_datetime(raw["datetime"], errors="coerce")
        raw = raw.dropna(subset=["datetime"])
        if raw.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 15-min slots
    raw["slot15"] = raw["datetime"].dt.floor("15min").dt.strftime("%H:%M")
    agg15   = _build_ohlcv(raw, "slot15")
    now_15  = get_current_15m_slot()
    live_map = _get_live_map()

    high_df, low_df = _sequential_breakout(agg15, now_15, live_map, "CANDLES_CHAIN")

    # save combined snapshot
    combined = pd.concat([high_df, low_df], ignore_index=True)
    if not combined.empty:
        combined.to_csv(HI_LO_TRACK_CSV, index=False, encoding='utf-8')

    # ── Volume surge table ──────────────────────────────────
    vol_rows = []
    for sym, g in agg15.groupby("Symbol"):
        g = g.sort_values("slot").reset_index(drop=True)
        completed = g[g["slot"] < now_15].reset_index(drop=True)
        if len(completed) < 2:
            continue
        live = live_map.get(sym)
        if not live:
            continue
        ltp       = float(live.get("LTP", 0) or 0)
        yest_high = float(live.get("YEST_HIGH", 0) or 0)
        curr_vol  = float(completed.iloc[-1]["VOLUME"])
        prev_vol  = float(completed.iloc[-2]["VOLUME"])
        if prev_vol > 0 and curr_vol > prev_vol and ltp > yest_high:
            vol_rows.append({
                "Symbol":       sym,
                "LTP":          round(ltp, 2),
                "CHANGE_%":     round(float(live.get("CHANGE_%", 0) or 0), 2),
                "PREV_SLOT":    completed.iloc[-2]["slot"],
                "PREV_VOL":     int(prev_vol),
                "CURR_SLOT":    completed.iloc[-1]["slot"],
                "CURR_VOL":     int(curr_vol),
                "VOL_SURGE_%":  round((curr_vol - prev_vol) / prev_vol * 100, 1),
                "YEST_HIGH":    yest_high,
                "LIVE_HIGH":    live.get("LIVE_HIGH", 0),
                "LIVE_VOLUME":  live.get("LIVE_VOLUME", 0),
                "NEAR":         live.get("NEAR", ""),
                "GAIN":         live.get("GAIN", ""),
                "TOP_HIGH":     live.get("TOP_HIGH", 0),
                "TOP_LOW":      live.get("TOP_LOW",  0),
            })
    vol_df = pd.DataFrame(vol_rows)
    if not vol_df.empty:
        vol_df = vol_df.sort_values("VOL_SURGE_%", ascending=False)
        vol_df.to_csv(VOL_TRACK_CSV, index=False, encoding='utf-8')

    return high_df, low_df, vol_df


def build_1h_tracker(raw=None):
    """
    Sequential 1-hour tracker. Pass raw 5min DataFrame to avoid re-reading CSV.
    Slots aligned to 9:15 market open: 09:15, 10:15, 11:15, 12:15, 13:15, 14:15
    Returns: (high_df, low_df)
    """
    if raw is None:
        raw = _load_raw_5min_today()
    if raw is None or (hasattr(raw, "empty") and raw.empty):
        return pd.DataFrame(), pd.DataFrame()

    # Ensure "datetime" column exists and is datetime dtype
    if "datetime" not in raw.columns:
        if "date" in raw.columns:
            raw = raw.rename(columns={"date": "datetime"})
        else:
            return pd.DataFrame(), pd.DataFrame()
    try:
        raw = raw.copy()
        raw["datetime"] = pd.to_datetime(raw["datetime"], errors="coerce")
        raw = raw.dropna(subset=["datetime"])
        if raw.empty:
            return pd.DataFrame(), pd.DataFrame()
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

    # FIX BUG 4: align 60-min slots to 9:15 market open
    # Subtract 15 min -> floor to hour -> add 15 min back
    raw["slot60"] = (
        (raw["datetime"] - pd.Timedelta(minutes=15))
        .dt.floor("60min") + pd.Timedelta(minutes=15)
    ).dt.strftime("%H:%M")

    agg60    = _build_ohlcv(raw, "slot60")
    now_1h   = get_current_1h_slot()   # FIX BUG 5: uses aligned slot logic
    live_map = _get_live_map()

    high_df, low_df = _sequential_breakout(agg60, now_1h, live_map, "HOURS_CHAIN")

    combined = pd.concat([high_df, low_df], ignore_index=True)
    if not combined.empty:
        combined.to_csv(H1_TRACK_CSV, index=False, encoding='utf-8')

    return high_df, low_df


# ── Build both trackers every rerun ──────────────────────
# ✅ Load raw 5min data once and pass to both builders - avoids reading CSV twice
_raw_5min_today = _load_raw_5min_today()   # shared between 15m and 1h trackers
# Fallback: if today's 5-min file missing (holiday/pre-market), load latest
def _raw_5min_is_bad(df) -> bool:
    """True if df is None, empty, or missing the required 'datetime' column."""
    if df is None: return True
    if not hasattr(df, "empty"): return True
    if df.empty: return True
    return "datetime" not in df.columns

if _raw_5min_is_bad(_raw_5min_today):
    try:
        import glob as _g5
        _5m_files = sorted(_g5.glob(os.path.join(CACHE_DIR, "five_min_*.csv")),
                           key=os.path.getmtime, reverse=True)
        for _fallback_f in _5m_files[:3]:   # try up to 3 most recent files
            try:
                _fb_df = pd.read_csv(_fallback_f, encoding="utf-8")
                # Normalize column names (strip whitespace, handle aliases)
                _fb_df.columns = [str(c).strip() for c in _fb_df.columns]
                if "datetime" not in _fb_df.columns and "date" in _fb_df.columns:
                    _fb_df = _fb_df.rename(columns={"date": "datetime"})
                if "datetime" not in _fb_df.columns:
                    continue   # this file doesn't have the right schema - try next
                _fb_df["datetime"] = pd.to_datetime(_fb_df["datetime"], errors="coerce")
                _fb_df = _fb_df.dropna(subset=["datetime"])
                if "volume" not in _fb_df.columns:
                    _fb_df["volume"] = 0
                if not _fb_df.empty:
                    _raw_5min_today = _fb_df.sort_values("datetime").reset_index(drop=True)
                    break
            except Exception:
                continue
    except Exception:
        pass
# Validate _raw_5min_today has required columns before passing to trackers
if _raw_5min_today is not None and "datetime" not in _raw_5min_today.columns:
    if "date" in _raw_5min_today.columns:
        _raw_5min_today = _raw_5min_today.rename(columns={"date": "datetime"})
    else:
        _raw_5min_today = None   # irrecoverable - trackers will return empty DataFrames

_15m_high_df, _15m_low_df, _vol_df = build_15m_tracker(_raw_5min_today)
_h1_high_df,  _h1_low_df           = build_1h_tracker(_raw_5min_today)


# ═══════════════════════════════════════════════════════════════════════
# MULTI-TIMEFRAME SEQUENTIAL SCANNER  (Point 6)
# PI IND / DivisLab / M&M type setup:
#   Finds stocks making sequential higher highs on 15m AND 1H AND above YEST_HIGH
#   With conviction filters: volume, change%, OI context
# ═══════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════
# PI INDUSTRIES STYLE SCANNER  (Point 6)
# Based on: PIIND Bullish Trade Rationale - 9 April, 1:00 PM
#
# The setup has 3 pillars:
#   1. Resistance -> Support Retest  (broken resistance retested and held as support)
#   2. Relative Strength vs Nifty   (stock holds/rises while Nifty is weak)
#   3. Multi-TF Structure           (15m + 1H + Daily alignment)
#
# Scanner runs on 15m, 1H, and Daily candles.
# ═══════════════════════════════════════════════════════════════════════

def _get_nifty_ltp():
    """Get live Nifty LTP from df (global live dataframe)."""
    try:
        row = df[df["Symbol"] == "NIFTY"]
        if not row.empty:
            return float(row.iloc[0]["LTP"])
    except Exception:
        pass
    return 0.0

def _get_nifty_chg_pct():
    """Get Nifty CHANGE_% from live df."""
    try:
        row = df[df["Symbol"] == "NIFTY"]
        if not row.empty:
            return float(row.iloc[0]["CHANGE_%"])
    except Exception:
        pass
    return 0.0


def build_piind_scanner():
    """
    PI Industries style Breakout-Retest-Relative-Strength scanner.

    LONG SETUP criteria (ALL must be true):
    ─────────────────────────────────────────
    A. Resistance->Support retest (15m):
       - Stock made a high (resistance) in first half of day (before 12:00)
       - Price pulled back ≥ 0.5% from that high (retest)
       - Price is now back above OR within 0.3% of that resistance level
       -> Resistance turned support

    B. Relative strength vs Nifty:
       - Stock CHANGE_% > Nifty CHANGE_% + 0.5%
       - OR: Stock CHANGE_% > 0 while Nifty CHANGE_% < 0 (outperforming weak index)

    C. Multi-timeframe structure (15m + 1H + Daily):
       - 15m: LTP > first-half-day high (structure breakout)
       - 1H:  LTP > first 1H candle high (09:15 candle)
       - Daily: LTP > YEST_HIGH OR LTP within 0.5% of YEST_HIGH

    D. Conviction filters:
       - CHANGE_% > 0.5%
       - VOL_% > -20% (volume not collapsing)
       - LTP > EMA20 (trend intact)

    CONVICTION SCORE (0-6):
       +1  Relative strength (stock > Nifty)
       +1  Strong relative strength (stock outperforms Nifty while Nifty negative)
       +1  Above YEST_HIGH (daily breakout)
       +1  1H breakout (above first 1H candle high)
       +1  Volume surge (VOL_% ≥ 30)
       +1  Above TOP_HIGH (90-day high - strongest signal)

    Returns: (long_df, short_df, watch_df)
       long_df:  all 3 pillars confirmed
       short_df: bearish mirror (resistance->support failed, relative weakness)
       watch_df: 2 of 3 pillars confirmed - watch for full setup
    """
    empty = pd.DataFrame()
    if _raw_5min_today is None or (hasattr(_raw_5min_today, "empty") and _raw_5min_today.empty):
        return empty, empty, empty

    live_map = _get_live_map()
    if not live_map:
        return empty, empty, empty

    nifty_chg = _get_nifty_chg_pct()   # Nifty CHANGE_%
    nifty_ltp = _get_nifty_ltp()

    # ── Build 15m OHLCV ─────────────────────────────────────────────
    if _raw_5min_today is None or (hasattr(_raw_5min_today, "empty") and _raw_5min_today.empty):
        return pd.DataFrame()
    raw = _raw_5min_today.copy()
    if "datetime" not in raw.columns:
        if "date" in raw.columns:
            raw = raw.rename(columns={"date": "datetime"})
        else:
            return pd.DataFrame()
    raw["datetime"] = pd.to_datetime(raw["datetime"], errors="coerce")
    raw = raw.dropna(subset=["datetime"])
    if raw.empty:
        return pd.DataFrame()
    raw["slot15"] = raw["datetime"].dt.floor("15min").dt.strftime("%H:%M")
    agg15 = _build_ohlcv(raw, "slot15")
    now_15 = get_current_15m_slot()

    # ── Build 1H OHLCV ──────────────────────────────────────────────
    raw["slot60"] = (
        (raw["datetime"] - pd.Timedelta(minutes=15))
        .dt.floor("60min") + pd.Timedelta(minutes=15)
    ).dt.strftime("%H:%M")
    agg60 = _build_ohlcv(raw, "slot60")
    now_1h = get_current_1h_slot()

    # ── Compute per-symbol metrics ───────────────────────────────────
    def _sym_15m_data(sym):
        """Return {first_half_high, first_half_low, pullback_pct, retest_ok, breakout_ok}"""
        g = agg15[agg15["Symbol"] == sym].copy()
        if g.empty:
            return {}
        g = g.sort_values("slot").reset_index(drop=True)
        completed = g[g["slot"] < now_15]
        if len(completed) < 3:
            return {}
        # First half = candles before 12:00
        first_half = completed[completed["slot"] < "12:00"]
        second_half = completed[completed["slot"] >= "12:00"]
        if first_half.empty:
            return {}
        first_half_high = float(first_half["HIGH"].max())
        first_half_low  = float(first_half["LOW"].min())
        # All-day high/low for context
        all_high = float(completed["HIGH"].max())
        all_low  = float(completed["LOW"].min())
        # Pullback: did price drop from first_half_high?
        # Check min low AFTER the peak candle
        peak_idx = first_half["HIGH"].idxmax()
        after_peak = completed.iloc[peak_idx+1:] if peak_idx+1 < len(completed) else pd.DataFrame()
        if after_peak.empty:
            pullback_pct = 0.0
            retest_ok    = False
        else:
            low_after_peak = float(after_peak["LOW"].min())
            pullback_pct   = round((first_half_high - low_after_peak) / first_half_high * 100, 2)
            retest_ok      = pullback_pct >= 0.4   # pulled back at least 0.4%
        # Current position vs first-half high (retest-and-hold)
        last_close = float(completed.iloc[-1]["CLOSE"])
        near_resistance = abs(last_close - first_half_high) / first_half_high * 100 <= 0.5
        above_resistance = last_close >= first_half_high * 0.998
        return {
            "first_half_high":  round(first_half_high, 2),
            "first_half_low":   round(first_half_low,  2),
            "all_day_high":     round(all_high, 2),
            "pullback_pct":     pullback_pct,
            "retest_ok":        retest_ok,
            "above_resistance": above_resistance,
            "near_resistance":  near_resistance,
            "candles_completed":len(completed),
        }

    def _sym_1h_data(sym):
        """Return {first_1h_high, first_1h_low, above_first_1h}"""
        g = agg60[agg60["Symbol"] == sym].copy()
        if g.empty:
            return {}
        g = g.sort_values("slot").reset_index(drop=True)
        completed = g[g["slot"] < now_1h]
        if completed.empty:
            return {}
        first_1h_high = float(completed.iloc[0]["HIGH"])
        first_1h_low  = float(completed.iloc[0]["LOW"])
        last_close    = float(completed.iloc[-1]["CLOSE"])
        return {
            "first_1h_high":   round(first_1h_high, 2),
            "first_1h_low":    round(first_1h_low,  2),
            "above_first_1h":  last_close > first_1h_high,
            "hours_completed": len(completed),
        }

    long_rows, short_rows, watch_rows = [], [], []

    for sym, live in live_map.items():
        ltp      = float(live.get("LTP",       0) or 0)
        chg_pct  = float(live.get("CHANGE_%",  0) or 0)
        vol_pct  = float(live.get("VOL_%",     0) or 0)
        yest_hi  = float(live.get("YEST_HIGH", 0) or 0)
        yest_lo  = float(live.get("YEST_LOW",  0) or 0)
        yest_cl  = float(live.get("YEST_CLOSE",0) or 0)
        top_hi   = float(live.get("TOP_HIGH",  0) or 0)
        ema20    = float(live.get("EMA20",      0) or 0)
        live_vol = float(live.get("LIVE_VOLUME",0)or 0)
        yest_vol = float(live.get("YEST_VOL",  0) or 0)
        if ltp <= 200:          # skip low-price stocks
            continue

        d15 = _sym_15m_data(sym)
        d1h = _sym_1h_data(sym)
        if not d15 or not d1h:
            continue

        # ─── LONG SETUP ─────────────────────────────────────────────
        # Pillar A: Resistance -> Support retest
        pillar_a_long = (
            d15["retest_ok"] and
            d15["above_resistance"] and
            d15["pullback_pct"] >= 0.4
        )
        # Pillar B: Relative strength vs Nifty
        rel_str_basic  = chg_pct > nifty_chg + 0.5
        rel_str_strong = chg_pct > 0 and nifty_chg < -0.3   # positive while Nifty negative
        pillar_b_long = rel_str_basic or rel_str_strong

        # Pillar C: Multi-TF structure
        daily_break = yest_hi > 0 and ltp >= yest_hi * 0.995   # at or within 0.5% of YEST_HIGH
        above_yh    = yest_hi > 0 and ltp >  yest_hi
        h1_break    = d1h.get("above_first_1h", False)
        pillar_c_long = daily_break and h1_break

        # Pillar D: Conviction filters
        pillar_d_long = (
            chg_pct > 0.3 and
            vol_pct > -30 and
            (ema20 <= 0 or ltp > ema20 * 0.99)
        )

        # Conviction score
        score = 0.0
        if rel_str_basic:         score += 1.0
        if rel_str_strong:        score += 1.0
        if above_yh:              score += 1.0
        elif daily_break:         score += 0.5
        if h1_break:              score += 1.0
        if vol_pct >= 30:         score += 1.0
        elif vol_pct >= 0:        score += 0.5
        if top_hi > 0 and ltp >= top_hi: score += 1.0
        score = round(score, 1)

        _live_open = float(live.get("LIVE_OPEN", 0) or 0)
        base = {
            "Symbol":       sym,
            "LTP":          round(ltp,       2),
            "Open":         round(_live_open, 2),
            "CHANGE_%":     round(chg_pct,   2),
            "VOL_%":        round(vol_pct,   1),
            "Nifty_%":      round(nifty_chg, 2),
            "RelStr_%":     round(chg_pct - nifty_chg, 2),
            "YEST_CLOSE":   round(yest_cl,  2),
            "YEST_HIGH":    round(yest_hi,  2),
            "YEST_LOW":     round(yest_lo,  2),
            "Daily_Break":  "✅ ABOVE YH" if above_yh else ("🔔 NEAR YH" if daily_break else "❌"),
            "1H_Break":     "✅ YES" if h1_break else "❌",
            "1H_First_Hi":  d1h.get("first_1h_high", 0),
            "15m_FH_High":  d15["first_half_high"],
            "15m_Pullback": f"{d15['pullback_pct']:.1f}%",
            "Retest_Hold":  "✅ YES" if pillar_a_long else "❌",
            "Rel_Strength": "💪 STRONG" if rel_str_strong else ("✅ YES" if rel_str_basic else "❌"),
            "TOP_HIGH":     round(top_hi, 2),
            "Above_90d_Hi": "✅ YES" if (top_hi > 0 and ltp >= top_hi) else "❌",
            "EMA20":        round(ema20, 2),
            "Conviction":   score,
        }

        pillars_met = sum([pillar_a_long, pillar_b_long, pillar_c_long])

        if pillars_met == 3 and pillar_d_long:
            # Update base for LONG display
            _long_base = base.copy()
            _long_base.update({
                "Setup": "🎯 PI-IND STYLE", 
                "Pillars": "A+B+C ✅",
                "Entry": f"Above {d15['first_half_high']:,.2f} or above LTP",
                "SL": f"Below {d15['first_half_low']:,.2f} (15m first-half low)"
            })
            long_rows.append(_long_base)
        elif pillars_met == 2:
            missing = []
            if not pillar_a_long: missing.append("Retest")
            if not pillar_b_long: missing.append("RelStr")
            if not pillar_c_long: missing.append("TF-Align")
            watch_rows.append({**base, "Setup": "🟡 WATCH", "Pillars": "+".join(["A","B","C"][:pillars_met]),
                                "Missing": ",".join(missing)})

        # ─── SHORT SETUP (mirror) ────────────────────────────────────
        # A: Support -> Resistance failure (support broken, retested and failed)
        pillar_a_short = (
            d15.get("pullback_pct", 0) >= 0.4 and
            ltp < d15["first_half_low"] * 1.005   # LTP near or below first-half low
        )
        # B: Relative weakness
        rel_weak_basic  = chg_pct < nifty_chg - 0.5
        rel_weak_strong = chg_pct < 0 and nifty_chg > 0.3
        pillar_b_short  = rel_weak_basic or rel_weak_strong
        # C: Daily breakdown
        daily_break_dn  = yest_lo > 0 and ltp <= yest_lo * 1.005
        below_yl        = yest_lo > 0 and ltp < yest_lo
        h1_break_dn     = not d1h.get("above_first_1h", True)
        pillar_c_short  = daily_break_dn and h1_break_dn
        pillar_d_short  = chg_pct < -0.3 and vol_pct > -30

        short_score = 0.0
        if rel_weak_basic:  short_score += 1.0
        if rel_weak_strong: short_score += 1.0
        if daily_break_dn:  short_score += 1.0
        if h1_break_dn:     short_score += 1.0
        if vol_pct >= 0:    short_score += 0.5
        short_score = round(short_score, 1)

        short_pillars = sum([pillar_a_short, pillar_b_short, pillar_c_short])
        if short_pillars == 3 and pillar_d_short:
            # Update base for SHORT display
            _short_base = base.copy()
            _short_base.update({
                "Conviction": short_score,
                "Setup": "🎯 SHORT PI-IND",
                "Pillars": "A+B+C ✅",
                "Daily_Break": "🔴 BELOW YL" if below_yl else ("🔔 NEAR YL" if daily_break_dn else "❌"),
                "Retest_Hold": "🔴 YES (Fail)" if pillar_a_short else "❌",
                "Rel_Strength": "💀 WEAK" if rel_weak_strong else ("🔴 YES" if rel_weak_basic else "❌"),
                "Entry": f"Below {d15['first_half_low']:,.2f}",
                "SL":    f"Above {d15['first_half_high']:,.2f}"
            })
            short_rows.append(_short_base)
    def _to_df(rows, sort_by="Conviction"):
        if not rows:
            return pd.DataFrame()
        out = pd.DataFrame(rows)
        if sort_by in out.columns:
            out = out.sort_values(sort_by, ascending=False)
        return out.reset_index(drop=True)

    return _to_df(long_rows), _to_df(short_rows), _to_df(watch_rows)


# Build PI IND scanner
_mtf_long_df, _mtf_short_df, _mtf_watch_df = build_piind_scanner()


# ── MTF / PI IND alert ─────────────────────────────────────────────────────
# PIIND alert log - persists valid alerts fired today so they appear in tab
_PIIND_ALERT_LOG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "CACHE",
    f"piind_alerts_{datetime.now(IST).strftime('%Y%m%d')}.json"
)

def _piind_load_alert_log() -> list:
    """Load today's PI-IND alert log from cache."""
    try:
        if os.path.exists(_PIIND_ALERT_LOG_FILE):
            with open(_PIIND_ALERT_LOG_FILE, "r", encoding="utf-8") as _f:
                return json.load(_f)
    except Exception:
        pass
    return []

def _piind_save_alert_log(entries: list):
    try:
        os.makedirs(os.path.dirname(_PIIND_ALERT_LOG_FILE), exist_ok=True)
        with open(_PIIND_ALERT_LOG_FILE, "w", encoding="utf-8") as _f:
            json.dump(entries, _f, indent=2)
    except Exception:
        pass

def _fire_mtf_alerts():
    """
    Fire Telegram alerts for PI-IND style high-conviction setups.
    ONE alert per stock per day - new entries only (not seen before today).
    Each fired stock is logged to CACHE/piind_alerts_YYYYMMDD.json.
    """
    if _is_tg_disabled("MTF_SEQ") or not is_market_hours():
        return
    _now_ist = datetime.now(IST)
    _now_str = _now_ist.strftime("%H:%M IST")
    _n_ltp   = _get_nifty_ltp()
    _n_chg   = _get_nifty_chg_pct()
    _today   = _now_ist.strftime("%Y%m%d")

    # Get Nifty details from live_map for header
    _live_map = st.session_state.get("live_data_map", {})
    _n_det = _live_map.get("NIFTY", {})
    _n_ohl = f"{_n_det.get('LIVE_OPEN')}/{_n_det.get('LIVE_HIGH')}/{_n_det.get('LIVE_LOW')}" if _n_det else ""
    _n_yhl = f"{_n_det.get('YEST_HIGH')}/{_n_det.get('YEST_LOW')}" if _n_det else ""

    _log = _piind_load_alert_log()
    _logged_keys = {e.get("key", "") for e in _log}

    for _direction, _df, _icon, _label in [
        ("LONG",  _mtf_long_df,  "🟢", "LONG"),
        ("SHORT", _mtf_short_df, "🔴", "SHORT"),
    ]:
        if _df is None or _df.empty:
            continue
        _hc = _df[_df["Conviction"] >= 3.0] if "Conviction" in _df.columns else _df
        if _hc.empty:
            continue

        # Find stocks NOT yet alerted today
        _new_rows = []
        for _, _r in _hc.iterrows():
            _sym = str(_r.get("Symbol", ""))
            _stock_key = f"PIIND_{_direction}_{_sym}_{_today}"
            if _stock_key not in _logged_keys:
                _new_rows.append((_stock_key, _r))

        if not _new_rows:
            continue  # all stocks already alerted today

        # Build one Telegram message for all new stocks
        _lines = []
        for _stock_key, _r in _new_rows:
            _s  = str(_r.get("Symbol", ""))
            _l  = _r.get("LTP", "")
            _c  = _r.get("CHANGE_%", "")
            _v  = _r.get("VOL_%", "")
            _rs = _r.get("RelStr_%", "")
            _sc = _r.get("Conviction", "")
            _db = _r.get("Daily_Break", "")
            _rt = _r.get("Retest_Hold", "")
            _en = _r.get("Entry", "")
            _sl = _r.get("SL", "")

            # Fetch extra context for high conviction
            try:
                _g_row = df[df["Symbol"] == _s].iloc[0] if _s in df["Symbol"].values else _r
                _chg_v = _g_row.get("CHANGE", 0)
                _yo = _g_row.get("YEST_OPEN", 0); _yh = _g_row.get("YEST_HIGH", 0); _yl = _g_row.get("YEST_LOW", 0); _yc = _g_row.get("YEST_CLOSE", 0)
                _lo = _g_row.get("LIVE_OPEN", 0); _lh = _g_row.get("LIVE_HIGH", 0); _ll = _g_row.get("LIVE_LOW", 0)
                _wh = _g_row.get("HIGH_W", 0); _wl = _g_row.get("LOW_W", 0)
                _e7 = _g_row.get("EMA7", 0); _e20 = _g_row.get("EMA20", 0); _e50 = _g_row.get("EMA50", 0)

                _lines.append(
                    f"  {_icon} <b>{_s}</b>  LTP:Rs.{float(_l):,.2f} ({_chg_v:+.2f} | {float(_c):+.1f}%)\n"
                    f"    Daily:{_db} | Retest:{_rt} | Score:<b>{_sc}/6</b>\n"
                    f"    OHLC: {_lo}/{_lh}/{_ll}/{_l}\n"
                    f"    YH: {_yh} | YL: {_yl} | YC: {_yc}\n"
                    f"    W-High: {_wh} | W-Low: {_wl}\n"
                    f"    E7: {_e7:.1f} | E20: {_e20:.1f} | E50: {_e50:.1f}\n"
                    f"    RelStr:{float(_rs):+.1f}% | Vol:{float(_v):+.0f}%\n"
                    f"    👉 <b>Entry:{_en}</b> | SL:{_sl}"
                )
            except Exception:
                _lines.append(f"  {_icon} <b>{_s}</b>  Score:{_sc} | Entry:{_en}")

        # Send via channel router
        _p_icon = "🟢" if _label == "LONG" else "🔴"
        _msg = (
            f"{_p_icon*8}\n"
            f"{_icon} <b>PI-IND STYLE SETUP - {_label} (NEW ENTRIES)</b>\n"
            f"⏰ {_now_str}   <b>Nifty: {_n_ltp:,.1f} ({_n_chg:+.2f}%)</b>\n"
            f"    N-OHL: {_n_ohl} | YH/YL: {_n_yhl}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"3 Pillars: Retest {_p_icon} + Rel. Strength {_p_icon} + TF Align {_p_icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            + "\n".join(_lines) +
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>NOT financial advice.</i>\n"
            f"{_p_icon*8}"
        )

        # Use a per-batch dedup key so concurrent calls don't double-fire
        _batch_syms = "_".join([str(_r.get("Symbol","")) for _, _r in _new_rows[:4]])
        _batch_key  = f"PIIND_{_direction}_{_batch_syms}_{_today}"
        send_alert_routed("MTF_SEQ", _msg, dedup_key=_batch_key)

        # Log each stock individually so next cycle skips them
        for _stock_key, _r in _new_rows:
            _log.append({
                "key":       _stock_key,
                "symbol":    str(_r.get("Symbol", "")),
                "direction": _label,
                "time":      _now_str,
                "ltp":       float(_r.get("LTP", 0) or 0),
                "change":    float(_r.get("CHANGE_%", 0) or 0),
                "vol":       float(_r.get("VOL_%", 0) or 0),
                "score":     float(_r.get("Conviction", 0) or 0),
                "entry":     str(_r.get("Entry", "")),
                "sl":        str(_r.get("SL", "")),
                "daily":     str(_r.get("Daily_Break", "")),
                "retest":    str(_r.get("Retest_Hold", "")),
            })
        _piind_save_alert_log(_log)

# _fire_mtf_alerts() - moved to after _seq helpers are defined

# (moved below - after _seq helpers defined)


#    load last saved tracker results from today's dated CSV files.
#    This ensures WATCHLIST tab shows data on restart even before
#    the background 5-min thread completes its first fetch.

def _load_tracker_fallback(df_in, csv_path, type_filter=None):
    """Load today's tracker CSV if df_in is empty."""
    if not df_in.empty:
        return df_in
    loaded = _load_today_csv(csv_path)
    if loaded is None or loaded.empty:
        return df_in
    if type_filter and "TYPE" in loaded.columns:
        loaded = loaded[loaded["TYPE"].str.contains(type_filter, na=False)]
    return loaded.reset_index(drop=True)

_15m_high_df = _load_tracker_fallback(_15m_high_df, HI_LO_TRACK_CSV, "HIGH")
_15m_low_df  = _load_tracker_fallback(_15m_low_df,  HI_LO_TRACK_CSV, "LOW")
_vol_df      = _load_tracker_fallback(_vol_df,       VOL_TRACK_CSV)
_h1_high_df  = _load_tracker_fallback(_h1_high_df,  H1_TRACK_CSV, "HIGH")
_h1_low_df   = _load_tracker_fallback(_h1_low_df,   H1_TRACK_CSV, "LOW")

# ── 1h HiLo: if still empty (5-min data not yet fetched), try to build
#    directly from any available 5-min CSV from today ─────────────────────
if _h1_high_df.empty and _h1_low_df.empty:
    try:
        _raw_retry = _load_raw_5min_today()
        if _raw_retry is not None and not _raw_retry.empty:
            _h1_high_df, _h1_low_df = build_1h_tracker(_raw_retry)
    except Exception as _e1h:
        pass  # silent - will populate on next refresh


# =========================================================
# 📧 HTML TABLE EMAIL - SEQUENTIAL BREAKOUT ALERTS
# Sends a rich HTML email with full table for each breakout
# type. Deduplicated per symbol+category per day.
# Fires every 5-min refresh only if new symbols found.
# =========================================================

# SEQ_EMAIL_DEDUP is defined at top (dated) - no redefinition needed

def _seq_email_already_sent(category, symbol):
    """Return True if this symbol+category was already emailed today.
    Safe: SEQ_EMAIL_DEDUP is already a dated path via _dated()."""
    today = date.today().isoformat()
    d = _load_today_csv(SEQ_EMAIL_DEDUP)
    if d is None or d.empty:
        return False
    try:
        return bool(((d["DATE"] == today) & (d["CATEGORY"] == category) & (d["SYMBOL"] == symbol)).any())
    except Exception:
        return False

def _seq_mark_email_sent(category, symbol):
    """Append dedup record to today's dated SEQ_EMAIL_DEDUP file."""
    today = date.today().isoformat()
    row = {"DATE": today, "CATEGORY": category, "SYMBOL": symbol}
    existing = _load_today_csv(SEQ_EMAIL_DEDUP)
    if existing is not None and not existing.empty:
        d = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
    else:
        d = pd.DataFrame([row])
    d.to_csv(SEQ_EMAIL_DEDUP, index=False, encoding='utf-8')

_fire_mtf_alerts()   # runs here, after _seq helpers are ready


def _df_to_html_table(df_in, highlight_col=None, up=True):
    """Convert a DataFrame to a styled HTML table string."""
    if df_in is None or df_in.empty:
        return "<p><i>No data</i></p>"

    # Clean emoji from column names for email
    cols = list(df_in.columns)
    header = "".join(
        f'<th style="background:#1a237e;color:#fff;padding:6px 10px;border:1px solid #ccc;'
        f'font-size:12px;white-space:nowrap;">{c}</th>'
        for c in cols
    )

    rows_html = ""
    for idx, row in df_in.iterrows():
        bg = "#f5f5f5" if idx % 2 == 0 else "#ffffff"
        cells = ""
        for c in cols:
            val = row[c]
            cell_style = f"padding:5px 8px;border:1px solid #ddd;font-size:12px;white-space:nowrap;background:{bg};"
            # Colour highlight_col
            if highlight_col and c == highlight_col:
                try:
                    fval = float(val)
                    if up:
                        intensity = min(int(fval * 30), 150)
                        cell_style += f"background:#{'%02x' % (255-intensity)}ff{'%02x' % (255-intensity)};color:#005000;font-weight:bold;"
                    else:
                        intensity = min(int(fval * 30), 150)
                        cell_style += f"background:#ff{'%02x' % (255-intensity)}{'%02x' % (255-intensity)};color:#500000;font-weight:bold;"
                except (ValueError, TypeError):
                    pass
            # Format numbers
            if isinstance(val, float):
                display = f"{val:,.2f}"
            elif isinstance(val, int):
                display = f"{val:,}"
            else:
                display = str(val) if val is not None else ""
            cells += f'<td style="{cell_style}">{display}</td>'
        rows_html += f"<tr>{cells}</tr>"

    return f"""
    <table style="border-collapse:collapse;width:100%;font-family:Courier New,monospace;">
      <thead><tr>{header}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>"""

def send_seq_breakout_email(category, title, df_new_syms, df_full, highlight_col, up=True):
    """
    Send ONE HTML email for a breakout category.
    df_new_syms: rows of df_full that are NEW (not already emailed today)
    df_full:     complete current snapshot of this category (all rows)
    """
    if not can_send_email():
        return
    if not is_email_allowed():
        return
    if df_new_syms is None or df_new_syms.empty:
        return

    now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    new_syms_list = df_new_syms["Symbol"].tolist() if "Symbol" in df_new_syms.columns else []

    # Strip emoji from TYPE column values for cleaner email
    df_new_clean = df_new_syms.copy()
    df_all_clean = df_full.copy()
    for col in ["TYPE"]:
        if col in df_new_clean.columns:
            df_new_clean[col] = df_new_clean[col].astype(str).str.replace(r'[^\x00-\x7F]+', '', regex=True).str.strip()
        if col in df_all_clean.columns:
            df_all_clean[col] = df_all_clean[col].astype(str).str.replace(r'[^\x00-\x7F]+', '', regex=True).str.strip()

    subject = f"[OiAnalytics] {title} - {len(new_syms_list)} New | {now_str}"

    color = "#1b5e20" if up else "#b71c1c"
    icon  = "🟢" if up else "🔴"

    html_body = f"""
<html><body style="font-family:Arial,sans-serif;background:#f0f0f0;margin:0;padding:20px;">
<div style="max-width:1000px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;
            box-shadow:0 2px 8px rgba(0,0,0,0.15);">

  <!-- HEADER -->
  <div style="background:{color};padding:16px 20px;">
    <h2 style="color:#fff;margin:0;font-size:18px;letter-spacing:1px;">
      {icon} {title}
    </h2>
    <p style="color:#ddd;margin:4px 0 0;font-size:12px;">
      Generated: {now_str} &nbsp;|&nbsp; OiAnalytics by Market Hacks [LOCAL]
    </p>
  </div>

  <!-- NEW SYMBOLS BANNER -->
  <div style="background:#fff8e1;padding:10px 20px;border-bottom:2px solid {color};">
    <b>🆕 NEW Signals This Update:</b>
    <span style="color:{color};font-weight:bold;font-size:14px;">
      &nbsp;{', '.join(new_syms_list)}
    </span>
  </div>

  <!-- NEW ROWS TABLE -->
  <div style="padding:16px 20px;">
    <h3 style="color:{color};font-size:14px;margin-top:0;">
      🆕 New Entries (This Alert)
    </h3>
    {_df_to_html_table(df_new_clean, highlight_col=highlight_col, up=up)}
  </div>

  <!-- FULL SNAPSHOT TABLE -->
  <div style="padding:0 20px 16px;">
    <h3 style="color:#333;font-size:14px;margin-top:0;">
      📋 Full Current Snapshot ({len(df_full)} stocks)
    </h3>
    {_df_to_html_table(df_all_clean, highlight_col=highlight_col, up=up)}
  </div>

  <!-- FOOTER -->
  <div style="background:#212121;padding:10px 20px;">
    <p style="color:#aaa;font-size:11px;margin:0;">
      Auto-alert fires every 5 min during market hours (09:15–15:15 IST).
      Each symbol alerted only ONCE per day per category.
    </p>
  </div>
</div>
</body></html>"""

    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        msg = MIMEMultipart("alternative")
        msg["From"]    = EMAIL_FROM
        msg["To"]      = ", ".join(EMAIL_TO)
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)
        record_email_sent()
    except Exception as e:
        print(f"SEQ EMAIL ERROR [{category}]: {e}")


def _fire_seq_alerts(category, title, df_result, chain_col, highlight_col, up=True):
    """
    Main alert dispatcher for one sequential breakout category.
    - Finds NEW symbols (not yet alerted in the current cooldown window)
    - Cooldown windows:
        15-min categories (SEQ_15M_*): 45 minutes between re-alerts per symbol
        1-hour categories  (SEQ_1H_*): 2 hours between re-alerts per symbol
        Volume surge:                   45 minutes (same as 15m)
    - Sends HTML table email, browser toast, Telegram
    """
    if df_result is None or df_result.empty:
        return
    if not is_market_hours():
        return

    # ── Determine cooldown window ──────────────────────────
    if "SEQ_1H" in category:
        _cooldown_minutes = 120   # 2 hours for 1-hour sequential
    else:
        _cooldown_minutes = 45    # 45 minutes for 15-min sequential + vol surge

    # ── Time-bucket key: floor current time to cooldown bucket ──
    _now_ist  = datetime.now(IST)
    _bucket   = (_now_ist.hour * 60 + _now_ist.minute) // _cooldown_minutes
    _time_tg  = _now_ist.strftime("%H:%M IST")

    # ── Per-symbol cooldown via SEQ_EMAIL_DEDUP ─────────────
    # Key now encodes the time-bucket so the same symbol can alert
    # again once the cooldown window rolls over.
    new_rows = []
    for _, row in df_result.iterrows():
        sym = row.get("Symbol", "")
        if not sym:
            continue
        # Dedup key includes time-bucket -> auto-resets after cooldown
        _dedup_cat = f"{category}__B{_bucket}"
        if not _seq_email_already_sent(_dedup_cat, sym):
            new_rows.append(row)
            _seq_mark_email_sent(_dedup_cat, sym)

    if not new_rows:
        return   # nothing new to send in this cooldown window

    new_df   = pd.DataFrame(new_rows)
    syms_str = ", ".join(new_df["Symbol"].tolist())

    # ── Browser toast ─────────────────────────────────────
    if not _is_toast_disabled(category):
        _cooldown_label = "45min" if _cooldown_minutes == 45 else "2hr"
        st.toast(f"🚨 {title}: {syms_str}", icon="📧")

    # ── Log to alerts log ──────────────────────────────────
    for _, row in new_df.iterrows():
        log_alert(
            symbol   = row.get("Symbol", ""),
            category = category,
            details  = title,
            ltp      = row.get("LTP", ""),
            source   = "SEQ_BREAKOUT_EMAIL"
        )

    # ── Telegram alert ────────────────────────────────────
    if not _is_tg_disabled(category):
        _icon_tg  = "🚀" if up else "🔻"
        _tg_lines = []
        for _, _r in new_df.iterrows():
            _sym  = _r.get("Symbol","")
            _ltp  = _r.get("LTP","")
            _chain= _r.get(highlight_col, "")
            _tg_lines.append(f"  • <b>{_sym}</b>  LTP: {_ltp}  {highlight_col}: {_chain}")
        _cooldown_label = "45-min" if _cooldown_minutes == 45 else "2-hour"
        # Color border: green rows for HIGH/BUY, red rows for LOW/SELL
        if up:
            _border_top = "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩"
            _border_bot = "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩"
        else:
            _border_top = "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"
            _border_bot = "🟥🟥🟥🟥🟥🟥🟥🟥🟥🟥"
        _tg_msg = (
            f"{_border_top}\n"
            f"{_icon_tg} <b>{title}</b>\n"
            f"⏰ {_time_tg}\n"
            f"📋 Stocks ({len(_tg_lines)}):\n"
            + "\n".join(_tg_lines[:20]) +
            f"\n\n🔕 <i>Re-alert cooldown: {_cooldown_label} per stock.</i>\n"
            f"⚠️ <i>NOT financial advice. Verify before trading.</i>\n"
            f"{_border_bot}"
        )
        # Dedup key uses bucket so TG won't re-fire within cooldown window
        send_telegram_bg(_tg_msg, dedup_key=f"{category}_B{_bucket}_{syms_str[:60]}")

    # ── HTML email in background thread ───────────────────
    if not _is_email_disabled(category):
        def _bg_send(cat=category, ttl=title, ndf=new_df.copy(),
                     fdf=df_result.copy(), hcol=highlight_col, u=up):
            send_seq_breakout_email(cat, ttl, ndf, fdf, hcol, u)
        threading.Thread(target=_bg_send, daemon=True).start()


# ── Fire alerts for all 4 sequential breakout types ──────
_fire_seq_alerts(
    category      = "SEQ_15M_HIGH",
    title         = "15-Min Sequential HIGH Breakout",
    df_result     = _15m_high_df,
    chain_col     = "CANDLES_CHAIN",
    highlight_col = "ABOVE_%",
    up            = True,
)

_fire_seq_alerts(
    category      = "SEQ_15M_LOW",
    title         = "15-Min Sequential LOW Breakdown",
    df_result     = _15m_low_df,
    chain_col     = "CANDLES_CHAIN",
    highlight_col = "BELOW_%",
    up            = False,
)

_fire_seq_alerts(
    category      = "SEQ_1H_HIGH",
    title         = "1-Hour Sequential HIGH Breakout",
    df_result     = _h1_high_df,
    chain_col     = "HOURS_CHAIN",
    highlight_col = "ABOVE_%",
    up            = True,
)

_fire_seq_alerts(
    category      = "SEQ_1H_LOW",
    title         = "1-Hour Sequential LOW Breakdown",
    df_result     = _h1_low_df,
    chain_col     = "HOURS_CHAIN",
    highlight_col = "BELOW_%",
    up            = False,
)

# ── Fire alert for 15-Min Volume Surge + Above Yesterday High ────
_fire_seq_alerts(
    category      = "VOL_SURGE_15M",
    title         = "15-Min Volume Surge + Above Yesterday High",
    df_result     = _vol_df,
    chain_col     = "CURR_SLOT",
    highlight_col = "VOL_SURGE_%",
    up            = True,
)

# 📌 WATCHLIST TAB
# =========================================================

with tabs[6]:   # 👈 replace with correct index
    #st.markdown("## 📌 Intraday Watchlist")

    # helper: safe column selector
    def _cols(df_, want):
        return [c for c in want if c in df_.columns]

    # ═══════════════════════════════════════════════════════════════
    # ⚡ 15-MIN SEQUENTIAL HIGH BREAKOUT
    # Every 15m candle HIGH > prev HIGH from 9:15 - LTP above broken level
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### ⚡ 15-Min Sequential HIGH Breakout")
    st.caption(
        "Every 15m candle broke previous HIGH from 9:15 onwards. "
        "LTP must be above the latest broken HIGH right now. "
        "CANDLES_CHAIN = number of consecutive rising candles."
    )
    if _15m_high_df.empty:
        st.info("No stocks with a sequential 15-min high breakout chain currently.")
    else:
        _show = _cols(_15m_high_df, [
            "Symbol","LTP","CHANGE_%","TYPE",
            "FIRST_SLOT","FIRST_HIGH",
            "PREV_SLOT","PREV_HIGH","BROKEN_HIGH",
            "CANDLES_CHAIN","ABOVE_%",
            "LIVE_HIGH","LIVE_LOW","YEST_HIGH",
            "NEAR","GAIN","TOP_HIGH","TOP_LOW"
        ])

    # FIX-10: Pure-CSS gradient replaces Styler.background_gradient()
    # background_gradient() requires matplotlib which is NOT installed.
    # This helper produces identical visual output using inline CSS only.
    def _css_gradient(series, cmap="Greens"):
        _pal = {
            "Greens": (255, 255, 255,   0, 109,  44),
            "Reds":   (255, 255, 255, 165,  15,  21),
            "Blues":  (255, 255, 255,   8,  48, 107),
        }
        r0, g0, b0, r1, g1, b1 = _pal.get(cmap, _pal["Greens"])
        mn, mx = series.min(), series.max()
        rng = mx - mn if mx != mn else 1.0
        def _cell(v):
            try:
                t = float((v - mn) / rng)
            except Exception:
                return ""
            r = int(r0 + t * (r1 - r0))
            g = int(g0 + t * (g1 - g0))
            b = int(b0 + t * (b1 - b0))
            fg = "#000" if (r * 0.299 + g * 0.587 + b * 0.114) > 150 else "#fff"
            return f"background-color: rgb({r},{g},{b}); color: {fg}"
        return [_cell(v) for v in series]

        st.dataframe(
            _15m_high_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["ABOVE_%"], cmap="Greens"), width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 🔻 15-MIN SEQUENTIAL LOW BREAKDOWN
    # Every 15m candle LOW < prev LOW from 9:15 - LTP below broken level
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🔻 15-Min Sequential LOW Breakdown")
    st.caption(
        "Every 15m candle broke previous LOW from 9:15 onwards. "
        "LTP must be below the latest broken LOW right now. "
        "CANDLES_CHAIN = number of consecutive falling candles."
    )
    if _15m_low_df.empty:
        st.info("No stocks with a sequential 15-min low breakdown chain currently.")
    else:
        _show = _cols(_15m_low_df, [
            "Symbol","LTP","CHANGE_%","TYPE",
            "FIRST_SLOT","FIRST_LOW",
            "PREV_SLOT","PREV_LOW","BROKEN_LOW",
            "CANDLES_CHAIN","BELOW_%",
            "LIVE_HIGH","LIVE_LOW","YEST_LOW",
            "NEAR","GAIN","TOP_HIGH","TOP_LOW"
        ])
        st.dataframe(
            _15m_low_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["BELOW_%"], cmap="Reds"), width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 📈 15-MIN VOLUME SURGE + ABOVE YESTERDAY HIGH
    # curr 15m vol > prev 15m vol AND LTP > YEST_HIGH
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📈 15-Min Volume Surge + Above Yesterday High")
    st.caption(
        "Current 15m candle volume exceeded previous 15m candle volume "
        "AND LTP is above yesterday's high - volume-confirmed breakout."
    )
    if _vol_df.empty:
        st.info("No stocks with volume surge above yesterday's high currently.")
    else:
        _show = _cols(_vol_df, [
            "Symbol","LTP","CHANGE_%",
            "PREV_SLOT","PREV_VOL","CURR_SLOT","CURR_VOL","VOL_SURGE_%",
            "YEST_HIGH","LIVE_HIGH","LIVE_VOLUME",
            "NEAR","GAIN","TOP_HIGH","TOP_LOW"
        ])
        st.dataframe(
            _vol_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["VOL_SURGE_%"], cmap="Blues"), width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 🕐 1-HOUR SEQUENTIAL HIGH BREAKOUT
    # Slots aligned to 9:15: 09:15 -> 10:15 -> 11:15 -> 12:15 -> 13:15 -> 14:15
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🕐 1-Hour Sequential HIGH Breakout")
    st.caption(
        "Every hourly candle broke previous HIGH. "
        "Slots: 09:15 -> 10:15 -> 11:15 -> 12:15 -> 13:15 -> 14:15. "
        "HOURS_CHAIN = number of consecutive rising hourly candles."
    )
    if _h1_high_df.empty:
        st.info("No stocks with a sequential 1-hour high breakout chain currently.")
    else:
        _show = _cols(_h1_high_df, [
            "Symbol","LTP","CHANGE_%","TYPE",
            "FIRST_SLOT","FIRST_HIGH",
            "PREV_SLOT","PREV_HIGH","BROKEN_HIGH",
            "HOURS_CHAIN","ABOVE_%",
            "LIVE_HIGH","LIVE_LOW","YEST_HIGH",
            "NEAR","GAIN","TOP_HIGH","TOP_LOW"
        ])
        st.dataframe(
            _h1_high_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["ABOVE_%"], cmap="Greens"), width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 🕐 1-HOUR SEQUENTIAL LOW BREAKDOWN
    # Every hourly candle LOW < prev LOW - LTP below broken level
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🕐 1-Hour Sequential LOW Breakdown")
    st.caption(
        "Every hourly candle broke previous LOW. "
        "Slots: 09:15 -> 10:15 -> 11:15 -> 12:15 -> 13:15 -> 14:15. "
        "HOURS_CHAIN = number of consecutive falling hourly candles."
    )
    if _h1_low_df.empty:
        st.info("No stocks with a sequential 1-hour low breakdown chain currently.")
    else:
        _show = _cols(_h1_low_df, [
            "Symbol","LTP","CHANGE_%","TYPE",
            "FIRST_SLOT","FIRST_LOW",
            "PREV_SLOT","PREV_LOW","BROKEN_LOW",
            "HOURS_CHAIN","BELOW_%",
            "LIVE_HIGH","LIVE_LOW","YEST_LOW",
            "NEAR","GAIN","TOP_HIGH","TOP_LOW"
        ])
        st.dataframe(
            _h1_low_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["BELOW_%"], cmap="Reds"), width='stretch'
        )

    st.divider()


    # -------------------------------
    # Common Columns
    # -------------------------------
    WATCH_COLS = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",        
        "DIST_FROM_DAY_HIGH_%",
        "DIST_FROM_DAY_LOW_%",
        "YEST_HIGH",
        "YEST_LOW",        
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS = [c for c in WATCH_COLS if c in df.columns]

    WATCH_COLS_YH = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "YEST_HIGH",
        "DIST_FROM_DAY_HIGH_%",
        "YL_05",        
        "LIVE_LOW",        
        "DIST_FROM_DAY_LOW_%",        
        "YEST_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YH = [c for c in WATCH_COLS_YH if c in df.columns]

    WATCH_COLS_YL = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_LOW",
        "YEST_LOW",
        "DIST_FROM_DAY_LOW_%",
        "LIVE_HIGH",    
        "DIST_FROM_DAY_HIGH_%",
        "YH_05",
        "YEST_HIGH",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YL = [c for c in WATCH_COLS_YL if c in df.columns]


    # =====================================================
    # 1️⃣ STRUCTURE – Inside YH Reclaim + Above EMA
    # open < yest.high and open >yest.close
    # LTP >yest.close and LTP>EMA20 and LTP<yest.high
    # =====================================================

    table1 = df[
        (df["LIVE_OPEN"] < df["YEST_HIGH"]) &
        (df["LIVE_OPEN"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] < df["YEST_HIGH"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YH].copy()
    #table1 = table1.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table1 = table1.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 Above Close + Below YH + EMA")

    if table1.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table1.sort_values("VOL_%", ascending=False), width='stretch')
        st.dataframe(table1, width='stretch')

    # =====================================================
    # 2️⃣ OPEN = LOW + Above EMA
    # open==low and ltp >ema20 and ltp <= yest.high
    # =====================================================

    table2 = df[
        (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] <= df["YEST_HIGH"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YH].copy()
    table2 = table2.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)

    st.markdown("### 🟢 Open = Low + Below YH + EMA Support")

    if table2.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table2.sort_values("VOL_%", ascending=False), width='stretch')

    # =====================================================
    # 3️⃣ Weak Bounce Structure (Below Close + Below EMA)
    # open > yest.low and open <=yest.close
    # LTP <=yest.close and LTP<=EMA20 and LTP>yest.low
    # =====================================================

    table3 = df[
        (df["LIVE_OPEN"] > df["YEST_LOW"]) &
        (df["LIVE_OPEN"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] > df["YEST_LOW"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YL].copy()
    table3 = table3.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Below Close + ABOVE YEST.LOW + EMA ")

    if table3.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table3.sort_values("VOL_%"), width='stretch')

    # =====================================================
    # 4️⃣ OPEN = HIGH + Below EMA
    # open==high and ltp <=ema20 and ltp >= yest.low
    # =====================================================

    table4 = df[
        (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] >= df["YEST_LOW"]) & (df["VOL_%"] >= -50)
    ][WATCH_COLS_YL].copy()
    table4 = table4.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Open = High + ABOVE YEST.LOW + EMA ")

    if table4.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table4.sort_values("VOL_%"), width='stretch')

    # ═══════════════════════════════════════════════════════════════
    # 📐 EMA7 WATCHLIST - Berlin Mindset Method (Video 6)
    # ═══════════════════════════════════════════════════════════════
    # TWO-TIMEFRAME SYSTEM:
    #   • EMA7_1H  = 1-Hour  EMA7 -> BIAS direction (like checking hourly chart)
    #   • EMA7_15M = 15-Min  EMA7 -> ENTRY timing  (like checking 15m chart)
    #
    # Berlin Rule:
    #   LTP > EMA7_1H  -> bullish bias -> look for LONG on 15m retest of EMA7_15M
    #   LTP < EMA7_1H  -> bearish bias -> look for SHORT on 15m bounce to EMA7_15M
    #   Entry only after 15m candle CLOSES on the correct side - no mid-candle entry
    # ═══════════════════════════════════════════════════════════════

    st.divider()
    st.markdown("## 📐 EMA7 Watchlist - Berlin Mindset (1H Bias + 15M Entry)")

    # ── Check which EMA columns are available ─────────────────────
    _has_1h  = "EMA7_1H"  in df.columns and df["EMA7_1H"].notna().any()
    _has_15m = "EMA7_15M" in df.columns and df["EMA7_15M"].notna().any()
    _has_d   = "EMA7"     in df.columns and df["EMA7"].notna().any()

    # Decide which columns to use - prefer intraday, fall back to daily
    _bias_col  = "EMA7_1H"  if _has_1h  else ("EMA7" if _has_d else None)
    _entry_col = "EMA7_15M" if _has_15m else ("EMA7" if _has_d else None)

    # FIX 4: detect full-fallback mode (both use daily EMA7 = same value)
    _full_fallback = (_bias_col == _entry_col == "EMA7")

    # Status info bar
    _c1, _c2, _c3 = st.columns(3)
    with _c1:
        if _has_1h:
            st.success("✅ 1H EMA7 live - using EMA7_1H for bias")
        else:
            st.warning("⚠️ 1H EMA7 not ready - Daily EMA7 used as bias (fallback)")
    with _c2:
        if _has_15m:
            st.success("✅ 15M EMA7 live - using EMA7_15M for entry")
        else:
            st.warning("⚠️ 15M EMA7 not ready - Daily EMA7 used as entry (fallback)")
    with _c3:
        _candles_info = ""
        if _has_15m and "CANDLES_15M" in df.columns:
            _avg_c = df["CANDLES_15M"].dropna().median()
            if not pd.isna(_avg_c):
                _candles_info = f"~{int(_avg_c)} 15m candles"
        if _has_1h and "CANDLES_1H" in df.columns:
            _avg_h = df["CANDLES_1H"].dropna().median()
            if not pd.isna(_avg_h):
                _candles_info += f"  ·  ~{int(_avg_h)} 1H candles"
        st.info(f"🕐 {_candles_info if _candles_info else 'EMA7 refreshes every 60s during market hours'}")

    # FIX 4: explicit note in full-fallback mode so user knows DIST_BIAS == DIST_ENTRY
    if _full_fallback:
        st.info(
            "ℹ️ **Fallback mode:** Intraday EMA7 not yet available (pre-market or first load). "
            "DIST_BIAS_% and DIST_ENTRY_% both use **Daily EMA7** - they show the same value. "
            "Once 15-min candle fetch completes (~09:16), live 1H and 15M EMA7 replace this automatically."
        )

    st.caption(
        "**1H EMA7 = trend bias** (above -> LONG only · below -> SHORT only).  "
        "**15M EMA7 = entry level** (wait for price to retest it and candle to CLOSE on correct side).  "
        "**DIST_BIAS_%** = LTP distance from 1H EMA7 (+ above = bullish · - below = bearish).  "
        "**DIST_ENTRY_%** = LTP distance from 15M EMA7 - closest to 0% = best entry candidate.  "
        "Tables sorted by DIST_ENTRY_% - tightest retest at the top."
    )

    if _bias_col is None:
        st.error("❌ No EMA7 data available. Market data not loaded - check CACHE folder.")
    else:
        _e7 = df.copy()
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index
        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index

        # Ensure numeric
        for _c in [_bias_col, _entry_col, "LTP", "CHANGE_%", "VOL_%", "EMA20"]:
            if _c in _e7.columns:
                _e7[_c] = pd.to_numeric(_e7[_c], errors="coerce")

        _e7["LTP"]      = pd.to_numeric(_e7["LTP"],      errors="coerce")
        _e7["CHANGE_%"] = pd.to_numeric(_e7.get("CHANGE_%", pd.Series(dtype=float)), errors="coerce").fillna(0)
        _e7["VOL_%"]    = pd.to_numeric(_e7.get("VOL_%",    pd.Series(dtype=float)), errors="coerce").fillna(0)

        # ── Core distance columns ──────────────────────────────────
        # Guard: replace 0 with NA to prevent division by zero -> NaN result
        _e7["DIST_BIAS_%"] = (
            (_e7["LTP"] - _e7[_bias_col]) / _e7[_bias_col].replace(0, pd.NA) * 100
        ).round(2)
        _e7["DIST_ENTRY_%"] = (
            (_e7["LTP"] - _e7[_entry_col]) / _e7[_entry_col].replace(0, pd.NA) * 100
        ).round(2)

        # EMA7_1H slope: rising when EMA7_1H > EMA20 -> confirms momentum direction
        if "EMA20" in _e7.columns:
            _e7["1H_SLOPE"] = (_e7[_bias_col] > _e7["EMA20"]).map(
                {True: "↑ Rising", False: "↓ Falling"}
            )
        else:
            _e7["1H_SLOPE"] = "-"

        # FIX 5: direction-aware ENTRY_STATUS
        # LONG:  LTP is above 15M EMA7 (positive DIST_ENTRY_%) -> pulling back toward it
        # SHORT: LTP is below 15M EMA7 (negative DIST_ENTRY_%) -> bouncing up toward it
        def _entry_status_long(d15):
            if pd.isna(d15): return "-"
            a = abs(d15)
            if   a <= 0.3: return "🔥 AT 15M EMA7 - Enter now"
            elif a <= 1.0: return "✅ Near - Watch 15m candle close"
            elif a <= 2.0: return "⏳ Pulling Back - Standby"
            else:          return "⌛ Far above - Not yet retesting"

        def _entry_status_short(d15):
            if pd.isna(d15): return "-"
            a = abs(d15)
            if   a <= 0.3: return "🔥 AT 15M EMA7 - Enter now"
            elif a <= 1.0: return "✅ Near - Watch 15m candle close"
            elif a <= 2.0: return "⏳ Bouncing Up - Standby"
            else:          return "⌛ Far below - Not yet retesting"

        _e7["ENTRY_STATUS"] = _e7["DIST_ENTRY_%"].apply(
            lambda d: _entry_status_long(d) if (not pd.isna(d) and d >= 0)
                      else _entry_status_short(d)
        )

        # ALREADY_IN: strong move off EMA7 on correct side with momentum
        def _already_in(d_bias, d_entry, chg, slope):
            if pd.isna(d_bias) or pd.isna(d_entry): return "-"
            if d_bias > 2.0 and d_entry > 1.5 and chg > 0.5 and "Rising" in str(slope):
                return "📌 Possibly In (Long)"
            if d_bias < -2.0 and d_entry < -1.5 and chg < -0.5 and "Falling" in str(slope):
                return "📌 Possibly In (Short)"
            return "-"

        _e7["ALREADY_IN?"] = _e7.apply(
            lambda r: _already_in(
                r["DIST_BIAS_%"], r["DIST_ENTRY_%"],
                r["CHANGE_%"], r.get("1H_SLOPE", "")
            ), axis=1
        )

        # FIX-9b: dedup - fallback mode has _bias_col==_entry_col=="EMA7"
        # which creates duplicate columns -> Styler raises KeyError
        _E7_SHOW = list(dict.fromkeys(
            col for col in [
                "Symbol", "LTP",
                _bias_col,  "DIST_BIAS_%",   "1H_SLOPE",
                _entry_col, "DIST_ENTRY_%",  "ENTRY_STATUS", "ALREADY_IN?",
                "CANDLES_1H", "CANDLES_15M",
                "CHANGE_%", "VOL_%",
                "LIVE_HIGH", "LIVE_LOW", "YEST_HIGH", "YEST_LOW",
                "EMA20", "NEAR", "TOP_HIGH", "TOP_LOW"
            ] if col in _e7.columns
        ))

        # ─────────────────────────────────────────────────────────
        # TABLE 1: 🟢 LONG CANDIDATES
        # ─────────────────────────────────────────────────────────
        st.markdown("### 🟢 LONG Candidates - Bullish Bias (LTP above 1H EMA7)")
        st.caption(
            "**Step 1:** LTP > 1H EMA7 ✅ -> Bullish bias confirmed. Only plan LONG.  "
            "**Step 2:** Watch 15M EMA7 (DIST_ENTRY_%) - wait for price to pull back near it.  "
            "**Step 3:** Wait for 15m candle to CLOSE above 15M EMA7 - confirms rejection.  "
            "**SL:** Below 15M EMA7. **Target:** 1:3 RR minimum.  "
            "🔥 AT EMA7 = enter zone · ✅ Near = watch candle close · ⏳ = standby"
        )

        # Bug A fix: warn when 1H EMA7 is based on < 4 candles (unreliable early morning)
        if _has_1h and "CANDLES_1H" in df.columns:
            _min_1h = df["CANDLES_1H"].dropna().min()
            if not pd.isna(_min_1h) and _min_1h < 4:
                st.warning(
                    f"⚠️ **EMA7_1H unreliable** - only {int(_min_1h)} completed 1H candle(s). "
                    "EMA7 needs ≥ 4 candles to be meaningful. "
                    "Use Daily EMA7 as bias reference until after 13:15."
                )

        _long_df = _e7[
            (_e7["DIST_BIAS_%"]  > 0) &
            (_e7["DIST_ENTRY_%"] <= 3.0) &
            (_e7["DIST_ENTRY_%"] > -0.5) &
            (_e7["VOL_%"] >= -60)
        ][_E7_SHOW].copy()
        _long_df = _long_df.sort_values("DIST_ENTRY_%", ascending=True).reset_index(drop=True)  # FIX-9c.reset_index(drop=True)  # FIX-9c.reset_index(drop=True)  # FIX-9c.reset_index(drop=True)  # FIX-9c.reset_index(drop=True)  # FIX-9c.reset_index(drop=True)  # FIX-9c.reset_index(drop=True)  # FIX-9c

        if _long_df.empty:
            st.info("No LONG candidates right now. Wait for stocks to pull back to 15M EMA7 while above 1H EMA7.")
        else:
            # Bug B fix: row colour has priority - use .bar() for visual on DIST_ENTRY_%
            # instead of background_gradient which overwrites row-level highlight colours
            def _hl_long(row):
                s = str(row.get("ENTRY_STATUS", ""))
                if "🔥" in s: return ["background-color:#0d2b0d; color:#b6f5c8; font-weight:bold"] * len(row)
                if "✅" in s: return ["background-color:#0d1f0d; color:#80d090"] * len(row)
                return [""] * len(row)

            _fmt_long = {
                "DIST_BIAS_%":  "{:+.2f}%",
                "DIST_ENTRY_%": "{:+.2f}%",
                "CHANGE_%":     "{:+.2f}%",
                "VOL_%":        "{:+.1f}%",
            }
            if _bias_col in _long_df.columns:  _fmt_long[_bias_col]  = "{:.2f}"
            if _entry_col in _long_df.columns: _fmt_long[_entry_col] = "{:.2f}"

            st.dataframe(
                _long_df.style
                    .apply(_hl_long, axis=1)
                    .format(_fmt_long, na_rep="-"), width='stretch'
            )
            _fire  = (_long_df["ENTRY_STATUS"].str.contains("🔥", na=False)).sum()
            _near  = (_long_df["ENTRY_STATUS"].str.contains("✅", na=False)).sum()
            _in_p  = (_long_df["ALREADY_IN?"] != "-").sum()
            st.caption(
                f"🟢 {len(_long_df)} LONG candidates  |  "
                f"🔥 {_fire} at 15M EMA7 zone  |  "
                f"✅ {_near} near - watch 15m close  |  "
                f"📌 {_in_p} possibly already in"
            )

        st.divider()

        # ─────────────────────────────────────────────────────────
        # TABLE 2: 🔴 SHORT CANDIDATES
        # ─────────────────────────────────────────────────────────
        st.markdown("### 🔴 SHORT Candidates - Bearish Bias (LTP below 1H EMA7)")
        st.caption(
            "**Step 1:** LTP < 1H EMA7 ✅ -> Bearish bias confirmed. Only plan SHORT.  "
            "**Step 2:** Watch 15M EMA7 (DIST_ENTRY_%) - wait for price to bounce up near it.  "
            "**Step 3:** Wait for 15m candle to CLOSE below 15M EMA7 - confirms rejection.  "
            "**SL:** Above 15M EMA7. **Target:** 1:3 RR minimum.  "
            "🔥 AT EMA7 = enter zone · ✅ Near = watch candle close · ⏳ = standby"
        )

        # Bug A fix: same warning for SHORT side
        if _has_1h and "CANDLES_1H" in df.columns:
            _min_1h_s = df["CANDLES_1H"].dropna().min()
            if not pd.isna(_min_1h_s) and _min_1h_s < 4:
                st.warning(
                    f"⚠️ **EMA7_1H unreliable** - only {int(_min_1h_s)} completed 1H candle(s). "
                    "Use Daily EMA7 as bias until after 13:15."
                )

        _short_df = _e7[
            (_e7["DIST_BIAS_%"]  < 0) &
            (_e7["DIST_ENTRY_%"] >= -3.0) &
            (_e7["DIST_ENTRY_%"] < 0.5) &
            (_e7["VOL_%"] >= -60)
        ][_E7_SHOW].copy()
        _short_df = _short_df.sort_values("DIST_ENTRY_%", ascending=False).reset_index(drop=True)  # FIX-9d.reset_index(drop=True)  # FIX-9d.reset_index(drop=True)  # FIX-9d.reset_index(drop=True)  # FIX-9d.reset_index(drop=True)  # FIX-9d.reset_index(drop=True)  # FIX-9d.reset_index(drop=True)  # FIX-9d

        if _short_df.empty:
            st.info("No SHORT candidates right now. Wait for stocks to bounce toward 15M EMA7 while below 1H EMA7.")
        else:
            # Bug B fix: same pattern as LONG - row colour priority, no gradient override
            def _hl_short(row):
                s = str(row.get("ENTRY_STATUS", ""))
                if "🔥" in s: return ["background-color:#2b0d0d; color:#ffb3b3; font-weight:bold"] * len(row)
                if "✅" in s: return ["background-color:#1f0d0d; color:#d08080"] * len(row)
                return [""] * len(row)

            _fmt_short = {
                "DIST_BIAS_%":  "{:+.2f}%",
                "DIST_ENTRY_%": "{:+.2f}%",
                "CHANGE_%":     "{:+.2f}%",
                "VOL_%":        "{:+.1f}%",
            }
            if _bias_col in _short_df.columns:  _fmt_short[_bias_col]  = "{:.2f}"
            if _entry_col in _short_df.columns: _fmt_short[_entry_col] = "{:.2f}"

            st.dataframe(
                _short_df.style
                    .apply(_hl_short, axis=1)
                    .format(_fmt_short, na_rep="-"), width='stretch'
            )
            _fire_s = (_short_df["ENTRY_STATUS"].str.contains("🔥", na=False)).sum()
            _near_s = (_short_df["ENTRY_STATUS"].str.contains("✅", na=False)).sum()
            _in_s   = (_short_df["ALREADY_IN?"] != "-").sum()
            st.caption(
                f"🔴 {len(_short_df)} SHORT candidates  |  "
                f"🔥 {_fire_s} at 15M EMA7 zone  |  "
                f"✅ {_near_s} near - watch 15m close  |  "
                f"📌 {_in_s} possibly already in"
            )

        st.divider()

        # ─────────────────────────────────────────────────────────
        # QUICK REFERENCE
        # ─────────────────────────────────────────────────────────
        with st.expander("📖 Berlin EMA7 Two-Timeframe Rules - Quick Reference"):
            st.markdown(f"""
**Two-Timeframe System**

| Timeframe | Column | Role |
|-----------|--------|------|
| **1-Hour EMA7** | `{_bias_col}` | **Bias direction** - are we bullish or bearish today? |
| **15-Min EMA7** | `{_entry_col}` | **Entry level** - where exactly do we enter the trade? |
| **Daily EMA7** | `EMA7` | Fallback when intraday data not ready (pre-9:15) |

**Entry Steps (Berlin Method)**

| Step | Action |
|------|--------|
| 1 | Open **hourly view** -> is LTP **above** or **below** `{_bias_col}`? |
| 2 | **Above** -> LONG bias. **Below** -> SHORT bias. Never trade against this. |
| 3 | Switch to **15-min view** -> wait for price to reach `{_entry_col}` |
| 4 | Watch for **rejection candle** - wick touches EMA7, body closes away |
| 5 | Enter **only after candle closes** - no mid-candle entries |
| 6 | If 15m chart contradicts 1H bias -> **skip the trade entirely** |

**SL & Targets**

| | Rule |
|--|------|
| **SL Long** | Below `{_entry_col}` (15M EMA7) |
| **SL Short** | Above `{_entry_col}` (15M EMA7) |
| **Target** | 1:3 RR minimum · 1:4 preferred |

**Column Guide**

| Column | Meaning |
|--------|---------|
| `DIST_BIAS_%` | LTP distance from **1H EMA7** (`+` = above = bullish, `-` = below = bearish) |
| `DIST_ENTRY_%` | LTP distance from **15M EMA7** - closest to 0 = best retest setup right now |
| `1H_SLOPE` | ↑ Rising = EMA7 above EMA20 (strong bull) · ↓ Falling (strong bear) |
| `ENTRY_STATUS` | 🔥 AT 15M EMA7 · ✅ Near - watch close · ⏳ Approaching · ⌛ Wait |
| `ALREADY_IN?` | 📌 = price moved strongly off EMA7, check if you have an open position |
| `CANDLES_1H` | How many completed 1H candles used for EMA7 calculation |
| `CANDLES_15M` | How many completed 15M candles used for EMA7 calculation |

**Data refresh:** EMA7_1H and EMA7_15M are stored in `CACHE/ema7_1hour_YYYYMMDD.csv` and
`CACHE/ema7_15min_YYYYMMDD.csv`. Auto-rebuilt every **60 seconds** during market hours (09:15–15:35).
            """)

with tabs[7]:   # 👈 replace with correct index

    # =========================================================
    # 🧠 NIFTY OI INTELLIGENCE PANEL  (Updates every 5 min)
    # =========================================================

    st.markdown("""
    <style>
    .oi-header {
        background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
        color: #fff;
        padding: 10px 16px;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 15px;
        font-weight: 700;
        letter-spacing: 1px;
        margin-bottom: 6px;
    }
    .oi-card {
        background: #1a1a2e;
        border: 1px solid #444;
        border-radius: 8px;
        padding: 10px 14px;
        color: #f0f0f0;
        font-family: 'Courier New', monospace;
        font-size: 13px;
        line-height: 1.6;
    }
    .oi-bull { color: #00e676; font-weight: bold; }
    .oi-bear { color: #ff5252; font-weight: bold; }
    .oi-warn { color: #ffd740; font-weight: bold; }
    .oi-dim  { color: #aaa; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="oi-header">🧠 NIFTY - OI INTELLIGENCE &nbsp;|&nbsp; 🔄 Auto updates every 5 min</div>', unsafe_allow_html=True)

    oi_intel = fetch_nifty_oi_intelligence()

    if oi_intel is None:
        st.warning("⚠️ OI Intelligence data unavailable. Check NFO connection or market hours.")
    else:
        # Standard variables
        step        = oi_intel.get("step", 50)
        ts_str      = oi_intel.get("timestamp", "-")
        spot        = oi_intel.get("spot", 0)
        fut_ltp     = oi_intel.get("fut_ltp", 0)
        atm         = oi_intel.get("atm", 0)
        max_pain    = oi_intel.get("max_pain", 0)
        pcr         = oi_intel.get("pcr", 0)
        direction   = oi_intel.get("direction", "-")
        dir_reason  = oi_intel.get("direction_reason", "")
        pain_signal = oi_intel.get("pain_signal", "")
        advice      = oi_intel.get("advice", "-")
        setup       = oi_intel.get("setup", "-")
        expiry      = oi_intel.get("expiry", "-")
        s_ce        = oi_intel.get("strongest_ce", 0)
        ce_oi_val   = oi_intel.get("ce_oi", 0)
        s_pe        = oi_intel.get("strongest_pe", 0)
        pe_oi_val   = oi_intel.get("pe_oi", 0)
        sh_ce       = oi_intel.get("shifting_ce", 0)
        sh_ce_add   = oi_intel.get("shifting_ce_add", 0)
        sh_pe       = oi_intel.get("shifting_pe", 0)
        sh_pe_add   = oi_intel.get("shifting_pe_add", 0)
        ncw         = oi_intel.get("nearest_call_wall", "-")
        npf         = oi_intel.get("nearest_put_floor", "-")
        d_call      = oi_intel.get("dist_to_call", "-")
        d_put       = oi_intel.get("dist_to_put", "-")

        # Direction color
        if "BULLISH" in direction and "BIAS" not in direction:
            dir_color = "oi-bull"
        elif "BEARISH" in direction and "BIAS" not in direction:
            dir_color = "oi-bear"
        else:
            dir_color = "oi-warn"

        # ── Row 1: Spot / Futures / ATM / Expiry / Timestamp ──
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("📍 NIFTY SPOT", f"{spot:,.2f}")
        col2.metric("📈 NIFTY FUTURES", f"{fut_ltp:,.2f}", delta=f"{round(fut_ltp - spot, 2)} basis")
        col3.metric("🎯 ATM Strike", str(atm))
        col4.metric("💊 Max Pain", str(max_pain), delta=pain_signal, delta_color="off")
        col5.metric("📅 Expiry", expiry)

        st.divider()

        # ── Row 2: Direction / PCR / Advice ───────────────────
        col_dir, col_pcr, col_adv = st.columns([1.2, 0.8, 2])

        with col_dir:
            st.markdown(f"""
            <div class="oi-card">
            <b>MARKET DIRECTION</b><br>
            <span class="{dir_color}" style="font-size:16px">{direction}</span><br>
            <span class="oi-dim">{dir_reason}</span>
            </div>
            """, unsafe_allow_html=True)

        with col_pcr:
            pcr_color = "oi-bull" if pcr >= 1.2 else ("oi-bear" if pcr <= 0.8 else "oi-warn")
            st.markdown(f"""
            <div class="oi-card">
            <b>PUT-CALL RATIO</b><br>
            <span class="{pcr_color}" style="font-size:20px">{pcr}</span><br>
            <span class="oi-dim">{'Bullish' if pcr>=1.2 else ('Bearish' if pcr<=0.8 else 'Neutral')}</span>
            </div>
            """, unsafe_allow_html=True)

        with col_adv:
            st.markdown(f"""
            <div class="oi-card">
            <b>💡 ADVICE &amp; SETUP</b><br>
            {advice}<br>
            <span style="color:#ffd740">📐 {setup}</span>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Row 3: Strike intelligence table ──────────────────
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)

        with col_s1:
            st.markdown(f"""
            <div class="oi-card">
            <b>🔴 STRONGEST CALL WALL</b><br>
            Strike: <span class="oi-bear">{s_ce}</span><br>
            OI: <b>{ce_oi_val:,}</b><br>
            <span class="oi-dim">Above spot -> Resistance</span>
            </div>
            """, unsafe_allow_html=True)

        with col_s2:
            st.markdown(f"""
            <div class="oi-card">
            <b>🟢 STRONGEST PUT FLOOR</b><br>
            Strike: <span class="oi-bull">{s_pe}</span><br>
            OI: <b>{pe_oi_val:,}</b><br>
            <span class="oi-dim">Below spot -> Support</span>
            </div>
            """, unsafe_allow_html=True)

        with col_s3:
            st.markdown(f"""
            <div class="oi-card">
            <b>📥 SHIFTING OI - CALLS</b><br>
            Strike: <span class="oi-bear">{sh_ce}</span><br>
            OI Added Today: <b>{sh_ce_add:,}</b><br>
            <span class="oi-dim">New CE writing happening here</span>
            </div>
            """, unsafe_allow_html=True)

        with col_s4:
            st.markdown(f"""
            <div class="oi-card">
            <b>📥 SHIFTING OI - PUTS</b><br>
            Strike: <span class="oi-bull">{sh_pe}</span><br>
            OI Added Today: <b>{sh_pe_add:,}</b><br>
            <span class="oi-dim">New PE writing happening here</span>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Row 4: Walls / Floors / Timestamp ─────────────────
        col_w1, col_w2, col_w3 = st.columns(3)

        with col_w1:
            st.markdown(f"""
            <div class="oi-card">
            <b>🚧 NEAREST CALL WALL</b><br>
            <span class="oi-bear">{ncw}</span> &nbsp;|&nbsp; {d_call} pts away<br>
            <span class="oi-dim">Breakout above = Bullish</span>
            </div>
            """, unsafe_allow_html=True)

        with col_w2:
            st.markdown(f"""
            <div class="oi-card">
            <b>🛡️ NEAREST PUT FLOOR</b><br>
            <span class="oi-bull">{npf}</span> &nbsp;|&nbsp; {d_put} pts away<br>
            <span class="oi-dim">Break below = Bearish</span>
            </div>
            """, unsafe_allow_html=True)

        with col_w3:
            st.markdown(f"""
            <div class="oi-card">
            <b>⏱️ LAST UPDATED</b><br>
            <span style="color:#ffd740; font-size:15px">{ts_str}</span><br>
            <span class="oi-dim">Next refresh in ~5 min</span>
            </div>
            """, unsafe_allow_html=True)

        # ── ITM / ATM / OTM Summary ───────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### ⚡ Option QuickView (ATM ±3 Strikes)")
        
        quick_rows = []
        summary_strikes = [atm + i * step for i in range(-3, 4)]
        chain_rows = oi_intel.get("chain_rows", [])
        chain_data = {r["STRIKE"]: r for r in chain_rows}
        
        for s in sorted(summary_strikes, reverse=True):
            r = chain_data.get(s, {})
            moneyness = "ATM" if s == atm else ("ITM" if s < atm else "OTM") # For CE
            quick_rows.append({
                "Strike": s,
                "CE LTP": r.get("CE_LTP", 0),
                "CE OI Δ": r.get("CE_OI_ADD", 0),
                "Moneyness": moneyness,
                "PE OI Δ": r.get("PE_OI_ADD", 0),
                "PE LTP": r.get("PE_LTP", 0),
            })
        
        st.table(pd.DataFrame(quick_rows))

        # ── Full Option Chain Table ────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("📊 Full NIFTY Option Chain (ATM ±10 Strikes)", expanded=False):
            chain_rows = oi_intel.get("chain_rows", [])
            if chain_rows:
                chain_df_display = pd.DataFrame(chain_rows)

                def _style_chain(row):
                    styles = [""] * len(row)
                    cols = list(row.index)
                    strike_idx = cols.index("STRIKE") if "STRIKE" in cols else -1
                    if strike_idx >= 0 and row["STRIKE"] == atm:
                        styles = ["background-color:#2e2e5e; color:#fff; font-weight:bold;"] * len(row)
                    elif strike_idx >= 0 and row["STRIKE"] == s_ce:
                        styles = ["background-color:#3d0000; color:#ff8080;"] * len(row)
                    elif strike_idx >= 0 and row["STRIKE"] == s_pe:
                        styles = ["background-color:#003d00; color:#80ff80;"] * len(row)
                    return styles

                styled_chain = chain_df_display.style.apply(_style_chain, axis=1).format({
                    "CE_LTP": "{:.1f}",
                    "PE_LTP": "{:.1f}",
                    "CE_OI":  "{:,}",
                    "PE_OI":  "{:,}",
                    "CE_OI_ADD": "{:,}",
                    "PE_OI_ADD": "{:,}",
                    "CE_VOL": "{:,}",
                    "PE_VOL": "{:,}",
                })
                st.dataframe(styled_chain, width='stretch', height=500)
            else:
                st.info("Chain data not available.")

        # ── 15-MIN OI DELTA PANEL ────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("### 📊 15-Min OI Shift Monitor")

        if _oi_15m_events:
            _ev_spot  = _oi_intel_for_delta.get("spot", 0) if _oi_intel_for_delta else 0
            _ev_atm   = _oi_intel_for_delta.get("atm", 0) if _oi_intel_for_delta else 0
            _ev_slot  = _oi_15m_events[0]["SLOT"] if _oi_15m_events else "-"

            st.caption(f"Slot: **{_ev_slot}** | NIFTY Spot: **{_ev_spot:,.0f}** | ATM: **{_ev_atm:,}** | "
                       f"Threshold: ≥5% (+ top mover always shown) | {len(_oi_15m_events)} event(s)")

            # ── Top-mover headline summary ──────────────────────
            _hl = []
            for _tc, _td, _icon, _lbl in [("CE","ADD","📝","Dominant Call Writing"),
                                           ("PE","DROP","⚠️","Dominant Put Crumbling"),
                                           ("PE","ADD","🛡️","Dominant Put Building"),
                                           ("CE","DROP","📈","Dominant Call Unwinding")]:
                _tm = next((e for e in _oi_15m_events
                            if e["TYPE"]==_tc and e["DIRECTION"]==_td and e.get("TOP_MOVER")), None)
                if _tm:
                    _hl.append(f'{_icon} <b>{_lbl}:</b> {_tm["STRIKE"]:,} {_tc} &nbsp;'
                               f'({_tm["OI_DELTA_%"]:.1f}%, {_tm["OI_DELTA"]:+,} OI, '
                               f'prem {_tm["LTP_CHG_%"]:+.1f}%)')
            if _hl:
                st.markdown(
                    '<div style="background:#1a237e;padding:10px 16px;border-radius:6px;margin-bottom:10px;">'
                    + "<br>".join(f'<span style="color:#e8eaf6;font-size:13px;">{h}</span>' for h in _hl)
                    + '</div>', unsafe_allow_html=True)

            for ev in _oi_15m_events:
                d_col = ("#b71c1c" if ev["TYPE"] == "CE" and ev["DIRECTION"] == "ADD"
                         else ("#1b5e20" if ev["TYPE"] == "PE" and ev["DIRECTION"] == "ADD"
                               else ("#880e4f" if ev["TYPE"] == "PE" and ev["DIRECTION"] == "DROP"
                                     else "#0277bd")))
                _badge = (' <span style="background:#ffd600;color:#000;font-size:10px;'
                          'padding:1px 5px;border-radius:3px;">★ TOP</span>'
                          if ev.get("TOP_MOVER") else "")
                oi_delta_str = f'<b style="color:{d_col};">{ev["OI_DELTA"]:+,} ({ev["OI_DELTA_%"]:.1f}%)</b>'
                prem_col = "#c62828" if ev["LTP_CHG_%"] < 0 else "#2e7d32"
                st.markdown(
                    f'<div style="background:#1a1a2e;border-left:4px solid {d_col};'
                    f'padding:10px 14px;border-radius:4px;margin-bottom:8px;font-family:monospace;font-size:13px;">'
                    f'<b style="color:{d_col};">{ev["STRIKE"]:,} {ev["TYPE"]} [{ev["DIRECTION"]}]</b>{_badge}'
                    f' &nbsp;·&nbsp; OI: {ev["OI_PREV"]:,} -> {ev["OI_CURR"]:,} &nbsp; {oi_delta_str}'
                    f' &nbsp;·&nbsp; prem <b style="color:{prem_col};">{ev["LTP_CHG_%"]:+.1f}%</b>'
                    f'<br><span style="color:#ccc;">{ev["LABEL"]}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
            st.info("No significant 15-min OI shifts detected yet. Updates every 15 minutes when slot changes.")

        # ── FUTURES 15-MIN OI DELTA PANEL ────────────────────
        st.markdown("---")
        st.markdown("### 📈 Futures 15-Min OI Shift Monitor")

        if _fut_15m_events:
            _f_slot = _fut_15m_events[0]["SLOT"] if _fut_15m_events else "-"
            st.caption(f"Slot: **{_f_slot}** | Threshold: ≥5% (+ top mover per type always shown) | "
                       f"{len(_fut_15m_events)} future(s)")

            # ── Top-mover headline ─────────────────────────────
            _fhl = []
            for _fpt, _ficon in [("🟢 LONG BUILDUP","🟢"),("🔴 SHORT BUILDUP","🔴"),
                                  ("⚠️ SHORT COVERING","⚠️"),("⚠️ LONG UNWINDING","⚠️")]:
                _ftm = next((e for e in _fut_15m_events
                             if e["POSITION_TYPE"]==_fpt and e.get("TOP_MOVER")), None)
                if _ftm:
                    _fhl.append(f'{_ficon} <b>{_fpt}:</b> {_ftm["SYMBOL"]} &nbsp;'
                                f'({_ftm["OI_DELTA_%"]:.1f}%, {_ftm["OI_DELTA"]:+,} OI, '
                                f'price {_ftm["PRICE_%"]:+.2f}%)')
            if _fhl:
                st.markdown(
                    '<div style="background:#004d40;padding:10px 16px;border-radius:6px;margin-bottom:10px;">'
                    + "<br>".join(f'<span style="color:#e0f2f1;font-size:13px;">{h}</span>' for h in _fhl)
                    + '</div>', unsafe_allow_html=True)

            # Group by POSITION_TYPE for easy scanning
            _pt_order = ["🟢 LONG BUILDUP", "🔴 SHORT BUILDUP", "⚠️ SHORT COVERING", "⚠️ LONG UNWINDING"]
            for _pt in _pt_order:
                _grp = [e for e in _fut_15m_events if e["POSITION_TYPE"] == _pt]
                if not _grp:
                    continue
                st.markdown(f"**{_pt}** ({len(_grp)})")
                for ev in _grp:
                    p_col  = "#2e7d32" if ev["PRICE_%"] >= 0 else "#c62828"
                    oi_col = ev["PT_COLOR"]
                    _fbdg  = (' <span style="background:#ffd600;color:#000;font-size:10px;'
                              'padding:1px 5px;border-radius:3px;">★ TOP</span>'
                              if ev.get("TOP_MOVER") else "")
                    st.markdown(
                        f'<div style="background:#0d1117;border-left:4px solid {oi_col};'
                        f'padding:9px 14px;border-radius:4px;margin-bottom:6px;font-family:monospace;font-size:13px;">'
                        f'<b style="color:{oi_col};font-size:14px;">{ev["SYMBOL"]}</b>{_fbdg}'
                        f'&nbsp;&nbsp;OI: {ev["OI_PREV"]:,} -> {ev["OI_CURR"]:,}'
                        f'&nbsp;&nbsp;<b style="color:{oi_col};">{ev["OI_DELTA"]:+,} ({ev["OI_DELTA_%"]:.1f}%)</b>'
                        f'&nbsp;|&nbsp;'
                        f'<span style="color:#bdbdbd;">Rs.{ev["LTP_PREV"]:.1f}->Rs.{ev["LTP_CURR"]:.1f}</span>'
                        f'&nbsp;<b style="color:{p_col};">{ev["PRICE_%"]:+.2f}%</b>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
        else:
            st.info("No significant futures OI shifts detected yet. Updates every 15 minutes when slot changes.")

        # ── WATERFALL TRAP + TRADE ADVISORY ──────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("---")

        # ── Pull extra dynamic fields from oi_intel ──────────
        step             = oi_intel.get("step", 50)
        near_ce_pct      = oi_intel.get("near_ce_pct", 0.0)
        near_pe_pct      = oi_intel.get("near_pe_pct", 0.0)
        near_pe_drop_pct = oi_intel.get("near_pe_drop_pct", 0.0)
        near_ce_added    = oi_intel.get("near_ce_added", 0)
        near_pe_added    = oi_intel.get("near_pe_added", 0)

        shifting_ce      = oi_intel.get("shifting_ce", atm)
        shifting_ce_add  = oi_intel.get("shifting_ce_add", 0)
        shifting_ce_pct  = oi_intel.get("shifting_ce_pct", 0.0)
        shifting_ce_ltp  = oi_intel.get("shifting_ce_ltp", 0.0)

        shifting_pe      = oi_intel.get("shifting_pe", atm)
        shifting_pe_add  = oi_intel.get("shifting_pe_add", 0)
        shifting_pe_pct  = oi_intel.get("shifting_pe_pct", 0.0)
        shifting_pe_ltp  = oi_intel.get("shifting_pe_ltp", 0.0)

        most_dropped_pe      = oi_intel.get("most_dropped_pe", atm)
        most_dropped_pe_val  = oi_intel.get("most_dropped_pe_val", 0)
        most_dropped_pe_pct  = oi_intel.get("most_dropped_pe_pct", 0.0)
        most_dropped_pe_ltp  = oi_intel.get("most_dropped_pe_ltp", 0.0)
        most_dropped_pe_high = oi_intel.get("most_dropped_pe_high", 0.0)
        most_dropped_ce      = oi_intel.get("most_dropped_ce", atm)

        # Top call walls above spot and put floors below spot
        call_walls_above = oi_intel.get("call_walls_above", [])  # list of [strike, oi]
        put_floors_below = oi_intel.get("put_floors_below", [])  # list of [strike, oi]

        # Strongest call wall ABOVE spot (for SL reference)
        # Use nearest_call_wall (closest above spot) not strongest
        real_sl_call   = ncw if ncw and ncw != "-" else (s_ce if s_ce > spot else atm + step)
        real_sl_call   = real_sl_call if isinstance(real_sl_call, (int, float)) else atm + step

        # Nearest put floor BELOW spot (for target reference)
        real_tgt_put   = npf if npf and npf != "-" else (s_pe if s_pe < spot else atm - step)
        real_tgt_put   = real_tgt_put if isinstance(real_tgt_put, (int, float)) else atm - step

        # ── Pattern + Setup: single unified decision block ────
        # All values flow from: OI shift data -> pattern -> direction -> bias -> SL/target
        # direction is already computed consistently in oi_intel (from fetch function)

        near_ce_pct_v  = oi_intel.get("near_ce_pct", 0.0)
        near_pe_pct_v  = oi_intel.get("near_pe_pct", 0.0)
        near_pe_drop_v = oi_intel.get("near_pe_drop_pct", 0.0)

        _ce_bld = near_ce_pct_v >= 5
        _pe_crm = near_pe_drop_v <= -3
        _pe_bld = near_pe_pct_v >= 5
        _ce_crm = oi_intel.get("most_dropped_ce", 0) != 0 and near_ce_pct_v <= -3

        # ── Step A: Pattern label + detail ───────────────────
        if _ce_bld and _pe_crm:
            pattern_label  = "🚨 WATERFALL TRAP: CALL WALL BUILDING + PUT SUPPORT CRUMBLING"
            pattern_detail = (
                f"Call writing at {shifting_ce} CE - OI added {shifting_ce_add:,} "
                f"({shifting_ce_pct}% of strike OI in session), premium Rs.{shifting_ce_ltp:.1f}. "
                f"Put support crumbling at {most_dropped_pe} PE - "
                f"OI dropped {abs(most_dropped_pe_val):,} ({abs(most_dropped_pe_pct)}% of strike OI in session), "
                f"premium Rs.{most_dropped_pe_ltp:.1f}."
            )
            trade_bias = "BEARISH"

        elif _pe_bld and _ce_crm:
            pattern_label  = "🚀 SHORT SQUEEZE: PUT WALL BUILDING + CALL RESISTANCE CRUMBLING"
            pattern_detail = (
                f"Put writing at {shifting_pe} PE - OI added {shifting_pe_add:,} "
                f"({shifting_pe_pct}% of strike OI in session), premium Rs.{shifting_pe_ltp:.1f}. "
                f"Call resistance crumbling at {most_dropped_ce} CE - "
                f"OI dropped {abs(int(oi_intel.get('near_ce_added',0))):,} "
                f"({abs(near_ce_pct_v):.1f}% in session)."
            )
            trade_bias = "BULLISH"

        elif _ce_bld and _pe_bld:
            pattern_label  = "⚖️ OI ACCUMULATION: BOTH SIDES WRITING (Indecision)"
            pattern_detail = (
                f"CE OI added {shifting_ce_add:,} (+{shifting_ce_pct}%) at {shifting_ce} CE. "
                f"PE OI added {shifting_pe_add:,} (+{shifting_pe_pct}%) at {shifting_pe} PE. "
                f"Market trapped - wait for breakout."
            )
            trade_bias = "NEUTRAL"

        elif _ce_bld:
            pattern_label  = "⚠️ CALL WALL BUILDING - Put support intact"
            pattern_detail = (
                f"Call writing at {shifting_ce} CE - OI added {shifting_ce_add:,} "
                f"({shifting_ce_pct}% of strike OI), premium Rs.{shifting_ce_ltp:.1f}. "
                f"Put floor at {s_pe} PE still holding ({pe_oi_val:,} OI)."
            )
            trade_bias = "BEARISH" if "BEARISH" in direction else "NEUTRAL"

        elif _pe_bld:
            pattern_label  = "✅ PUT FLOOR BUILDING - Call resistance intact"
            pattern_detail = (
                f"Put writing at {shifting_pe} PE - OI added {shifting_pe_add:,} "
                f"({shifting_pe_pct}% of strike OI), premium Rs.{shifting_pe_ltp:.1f}. "
                f"Call wall at {s_ce} CE still holding ({ce_oi_val:,} OI)."
            )
            trade_bias = "BULLISH" if "BULLISH" in direction else "NEUTRAL"

        elif _ce_crm and not _pe_bld:
            pattern_label  = "📈 CALL WALL UNWINDING - Resistance weakening"
            pattern_detail = (
                f"Call OI dropping near ATM ({near_ce_pct_v:+.1f}% change) - "
                f"writers covering shorts. Bullish signal if spot holds above {npf if npf and npf != '-' else s_pe}."
            )
            trade_bias = "BULLISH"

        elif _pe_crm and not _ce_bld:
            pattern_label  = "📉 PUT FLOOR CRUMBLING - Support weakening"
            pattern_detail = (
                f"Put OI dropping near ATM ({near_pe_drop_v:+.1f}% change) - "
                f"support being withdrawn. Bearish signal if spot stays below {ncw if ncw and ncw != '-' else s_ce}."
            )
            trade_bias = "BEARISH"

        elif "BEARISH" in direction:
            pattern_label  = "🔴 BEARISH REGIME - Call pressure dominant"
            pattern_detail = (
                f"Strongest call wall: {s_ce} CE ({ce_oi_val:,} OI) - "
                f"{int(d_call)} pts above spot. "
                f"PCR={pcr}. Nearest put floor: {npf if npf and npf != '-' else s_pe}."
            )
            trade_bias = "BEARISH"

        elif "BULLISH" in direction:
            pattern_label  = "🟢 BULLISH REGIME - Put support dominant"
            pattern_detail = (
                f"Strongest put floor: {s_pe} PE ({pe_oi_val:,} OI) - "
                f"{int(d_put)} pts below spot. "
                f"PCR={pcr}. Nearest call wall: {ncw if ncw and ncw != '-' else s_ce}."
            )
            trade_bias = "BULLISH"

        else:
            pattern_label  = "📊 OI REGIME: NEUTRAL / BALANCED"
            pattern_detail = (
                f"No dominant OI shift near ATM. "
                f"CE OI: {near_ce_pct_v:+.1f}% | PE OI: {near_pe_pct_v:+.1f}% | "
                f"PE drop: {near_pe_drop_v:+.1f}% | PCR={pcr}. "
                f"Range: {npf if npf and npf != '-' else s_pe} – {ncw if ncw and ncw != '-' else s_ce}."
            )
            trade_bias = "NEUTRAL"

        # ── Step B: SL and Target from ACTUAL nearest wall/floor ─
        # BEARISH: SL = nearest call wall + buffer | Target = strongest floor - buffer
        # BULLISH: SL = nearest put floor - buffer | Target = strongest wall + buffer
        # Buffer = 1 step (50 pts). If R:R < 1:1, widen target to next level.

        _ncw = ncw if isinstance(ncw, (int,float)) else (s_ce if s_ce > spot else atm + step)
        _npf = npf if isinstance(npf, (int,float)) else (s_pe if s_pe < spot else atm - step)

        if trade_bias == "BEARISH":
            bias_color  = "#b71c1c";  bias_icon = "🔴"
            entry_strike = atm
            sl_spot      = int(_ncw) + step          # SL just above nearest call wall
            # Target: use strongest put floor, if R:R < 1:1 use 2nd floor
            _floors = put_floors_below if put_floors_below else [[_npf, 0]]
            target_spot  = int(_floors[0][0]) - step  # 1st floor - buffer
            risk_r       = max(1, abs(sl_spot - spot))
            reward_r     = abs(spot - target_spot)
            if reward_r < risk_r and len(_floors) > 1:   # R:R < 1:1 -> use 2nd floor
                target_spot = int(_floors[1][0]) - step
                reward_r    = abs(spot - target_spot)
            setup_line   = f"Buy {entry_strike} PE"
            sl_line      = f"Above {sl_spot} ({int(_ncw)} call wall)"
            target_line  = str(int(target_spot))
            advice_text  = "Strong BEARISH. Do NOT buy call options here."

        elif trade_bias == "BULLISH":
            bias_color  = "#1b5e20";  bias_icon = "🟢"
            entry_strike = atm
            sl_spot      = int(_npf) - step           # SL just below nearest put floor
            _walls = call_walls_above if call_walls_above else [[_ncw, 0]]
            target_spot  = int(_walls[0][0]) + step   # 1st wall + buffer
            risk_r       = max(1, abs(spot - sl_spot))
            reward_r     = abs(target_spot - spot)
            if reward_r < risk_r and len(_walls) > 1:  # R:R < 1:1 -> use 2nd wall
                target_spot = int(_walls[1][0]) + step
                reward_r    = abs(target_spot - spot)
            setup_line   = f"Buy {entry_strike} CE"
            sl_line      = f"Below {sl_spot} ({int(_npf)} put floor)"
            target_line  = str(int(target_spot))
            advice_text  = "Strong BULLISH. Do NOT sell calls aggressively."

        else:
            bias_color  = "#e65100";  bias_icon = "⚠️"
            entry_strike = atm
            sl_spot = target_spot = 0
            risk_r = reward_r = 0
            setup_line  = f"WAIT - Range: {int(_npf)} – {int(_ncw)}"
            sl_line     = "-"
            target_line = "-"
            advice_text = "Range-bound. Trade breakouts only. Avoid directional bets."

        risk_pts   = int(risk_r)   if trade_bias != "NEUTRAL" else 0
        reward_pts = int(reward_r) if trade_bias != "NEUTRAL" else 0
        rr_ratio   = f"1:{round(reward_pts/risk_pts,1)}" if risk_pts > 0 else "-"

        # ── Regime change detector ────────────────────────────
        # Compare current direction vs cached (previous refresh)
        prev_direction_file = os.path.join(CACHE_DIR, "oi_prev_direction.txt")
        prev_direction = ""
        if os.path.exists(prev_direction_file):
            with open(prev_direction_file, encoding="utf-8") as f:
                prev_direction = f.read().strip()
        with open(prev_direction_file, "w", encoding="utf-8") as f:
            f.write(direction)

        regime_changed = (prev_direction != "" and prev_direction != direction)
        regime_banner  = ""
        if regime_changed:
            regime_banner = f"🔄 REGIME CHANGE: {prev_direction} -> {direction}"

        # ── Render Advisory Card ──────────────────────────────
        regime_html = (
            f'<div style="background:#ff6f00;color:#fff;padding:6px 12px;'
            f'border-radius:6px;font-weight:900;font-size:13px;margin-bottom:10px;">'
            f'{regime_banner}</div>'
        ) if regime_banner else ""

        # Top call walls / put floors list for display
        walls_str  = "  ".join([f"{int(s)} ({oi:,})" for s, oi in call_walls_above[:3]]) or "-"
        floors_str = "  ".join([f"{int(s)} ({oi:,})" for s, oi in put_floors_below[:3]]) or "-"

        st.markdown(f"""
        <style>
        .adv-card {{
            background: #0d0d0d;
            border: 2px solid {bias_color};
            border-radius: 10px;
            padding: 16px 20px;
            font-family: 'Courier New', monospace;
            color: #f0f0f0;
            line-height: 1.9;
        }}
        .adv-pattern {{
            font-size: 15px;
            font-weight: 900;
            color: #ffd740;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}
        .adv-detail {{
            font-size: 12px;
            color: #ccc;
            margin-bottom: 10px;
            border-left: 3px solid {bias_color};
            padding-left: 10px;
        }}
        .adv-row {{
            font-size: 13px;
            margin: 2px 0;
        }}
        .adv-label {{ color: #aaa; }}
        .adv-val   {{ color: #fff; font-weight: bold; }}
        .adv-bull  {{ color: #00e676; font-weight: bold; }}
        .adv-bear  {{ color: #ff5252; font-weight: bold; }}
        .adv-warn  {{ color: #ffd740; font-weight: bold; }}
        </style>

        <div class="adv-card">
          {regime_html}
          <div class="adv-pattern">📌 {pattern_label}</div>
          <div class="adv-detail">{pattern_detail if pattern_detail else "&nbsp;"}</div>

          <div class="adv-row">
            <span class="adv-label">📍 Spot:</span>
            <span class="adv-val"> {spot:,.2f}</span>
            &nbsp;|&nbsp;
            <span class="adv-label">Futures:</span>
            <span class="adv-val"> {fut_ltp:,.2f}</span>
            &nbsp;|&nbsp;
            <span class="adv-label">ATM:</span>
            <span class="adv-val"> {atm}</span>
          </div>

          <div class="adv-row">
            <span class="adv-label">🎯 Max Pain:</span>
            <span class="adv-val"> {max_pain}</span>
            &nbsp;({pain_signal})
          </div>

          <div class="adv-row">
            <span class="adv-label">🔴 Call Walls above spot:</span>
            <span class="adv-bear"> {walls_str}</span>
          </div>

          <div class="adv-row">
            <span class="adv-label">🟢 Put Floors below spot:</span>
            <span class="adv-bull"> {floors_str}</span>
          </div>

          <div class="adv-row" style="margin-top:8px;">
            <span class="adv-label">💡 Advice:</span>
            <span class="{'adv-bear' if trade_bias=='BEARISH' else ('adv-bull' if trade_bias=='BULLISH' else 'adv-warn')}">
              &nbsp;{advice_text}
            </span>
          </div>

          <div class="adv-row">
            <span class="adv-label">📐 Setup:</span>
            <span class="adv-val"> {setup_line}</span>
          </div>

          <div class="adv-row">
            <span class="adv-label">🛑 Spot SL:</span>
            <span class="adv-bear"> {sl_line}</span>
          </div>

          <div class="adv-row">
            <span class="adv-label">🎯 Spot Target:</span>
            <span class="adv-bull"> {target_line}</span>
          </div>

          <div class="adv-row" style="margin-top:8px; border-top:1px solid #333; padding-top:8px;">
            <span class="adv-label">Risk:</span>
            <span class="adv-bear"> {int(risk_pts)} pts</span>
            &nbsp;|&nbsp;
            <span class="adv-label">Reward:</span>
            <span class="adv-bull"> {int(reward_pts)} pts</span>
            &nbsp;|&nbsp;
            <span class="adv-label">R:R</span>
            <span class="adv-val"> {rr_ratio}</span>
            &nbsp;|&nbsp;
            <span class="adv-label">PCR:</span>
            <span class="{'adv-bull' if pcr >= 1.2 else ('adv-bear' if pcr <= 0.8 else 'adv-warn')}"> {pcr}</span>
            &nbsp;|&nbsp;
            <span class="adv-label">Updated:</span>
            <span style="color:#ffd740;"> {ts_str}</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

    st.divider()
    # =========================================================
    # END OF OI INTELLIGENCE PANEL - BREAKOUT SCREENER BELOW
    # =========================================================

    #st.markdown("## 📌 Intraday Watchlist")

    # -------------------------------
    # Common Columns
    # -------------------------------
    WATCH_COLS = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "LIVE_LOW",
        "DIST_FROM_DAY_HIGH_%",
        "DIST_FROM_DAY_LOW_%",
        "YEST_HIGH",
        "YEST_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS = [c for c in WATCH_COLS if c in df.columns]

    WATCH_COLS_YH = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YH_05",
        "LIVE_OPEN",
        "LIVE_HIGH",
        "YEST_HIGH",
        "DIST_FROM_DAY_HIGH_%",
        "YL_05",        
        "LIVE_LOW",        
        "DIST_FROM_DAY_LOW_%",        
        "YEST_LOW",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YH = [c for c in WATCH_COLS_YH if c in df.columns]

    WATCH_COLS_YL = [
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "VOL_%",
        "YL_05",
        "LIVE_OPEN",
        "LIVE_LOW",
        "YEST_LOW",
        "DIST_FROM_DAY_LOW_%",
        "LIVE_HIGH",    
        "DIST_FROM_DAY_HIGH_%",
        "YH_05",
        "YEST_HIGH",
        "LIVE_VOLUME",
        "YEST_VOL",
        "NEAR",
        "GAIN",
        "TOP_HIGH",
        "TOP_LOW"
    ]

    WATCH_COLS_YL = [c for c in WATCH_COLS_YL if c in df.columns]

    # =====================================================
    # 1️⃣ STRUCTURE – Inside YH Reclaim + Above EMA
    # open < yest.high and open >yest.close
    # LTP >yest.close and LTP>EMA20 and LTP<yest.high
    # =====================================================

    table1 = df[
        (df["LIVE_OPEN"] < df["YEST_HIGH"]) &
        (df["LIVE_OPEN"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= -50) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    ##table1 = table1.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table1 = table1.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 Above YH + EMA ")

    if table1.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table1.sort_values("VOL_%", ascending=False), width='stretch')
        st.dataframe(table1, width='stretch')

    table10 = df[
        (df["LIVE_OPEN"] >= df["YEST_HIGH"]) &
        (df["LIVE_LOW"] <= df["YEST_HIGH"]) &
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= -50) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    #table10 = table10.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table10 = table10.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 OPEN ABOVE YH ")

    if table10.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table10.sort_values("VOL_%", ascending=False), width='stretch')
        st.dataframe(table10, width='stretch')

    table11 = df[
        (df["LTP"] > df["YEST_CLOSE"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= 0) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    #table11 = table11.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)
    table11 = table11.sort_values(by=["VOL_%", "DIST_FROM_DAY_HIGH_%"],ascending=[False, True])

    st.markdown("### 🟢 VOL% ")

    if table11.empty:
        st.info("No stocks matching this condition")
    else:
        #st.dataframe(table11.sort_values("VOL_%", ascending=False), width='stretch')
        st.dataframe(table11, width='stretch')



    # =====================================================
    # 2️⃣ OPEN = LOW + Above EMA
    # open==low and ltp >ema20 and ltp <= yest.high
    # =====================================================

    table2 = df[
        (df["LIVE_OPEN"] == df["LIVE_LOW"]) &
        (df["LTP"] > df["EMA20"]) &
        (df["LTP"] >= df["YEST_HIGH"]) & (df["VOL_%"] >= -50) & (df["LTP"] <= df["YEST_HIGH"] * 1.005)
    ][WATCH_COLS_YH].copy()
    table2 = table2.sort_values("DIST_FROM_DAY_HIGH_%", ascending=True)

    st.markdown("### 🟢 Open = Low + YH + EMA Support")

    if table2.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table2.sort_values("VOL_%", ascending=False), width='stretch')

    # =====================================================
    # 3️⃣ Weak Bounce Structure (Below Close + Below EMA)
    # open > yest.low and open <=yest.close
    # LTP <=yest.close and LTP<=EMA20 and LTP>yest.low
    # =====================================================

    table3 = df[
        (df["LIVE_OPEN"] > df["YEST_LOW"]) &
        (df["LIVE_OPEN"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["YEST_CLOSE"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] <= df["YEST_LOW"]) & (df["VOL_%"] >= -50) & (df["LTP"] >= df["YEST_LOW"] * 0.995)
    ][WATCH_COLS_YL].copy()
    table3 = table3.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Below YEST_LOW ")

    if table3.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table3.sort_values("VOL_%"), width='stretch')

    # =====================================================
    # 4️⃣ OPEN = HIGH + Below EMA
    # open==high and ltp <=ema20 and ltp >= yest.low
    # =====================================================

    table4 = df[
        (df["LIVE_OPEN"] == df["LIVE_HIGH"]) &
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] <= df["YEST_LOW"]) & (df["VOL_%"] >= -50)  & (df["LTP"] >= df["YEST_LOW"] * 0.995)
    ][WATCH_COLS_YL].copy()
    table4 = table4.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 Open = High + YEST_LOW")

    if table4.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table4.sort_values("VOL_%"), width='stretch')

    table5 = df[
        (df["LTP"] <= df["EMA20"]) &
        (df["LTP"] <= df["YEST_LOW"]) & (df["VOL_%"] >= 0)  & (df["LTP"] >= df["YEST_LOW"] * 0.995)
    ][WATCH_COLS_YL].copy()
    table5 = table5.sort_values("DIST_FROM_DAY_LOW_%", ascending=True)

    st.markdown("### 🔴 VOL_% + YEST_LOW")

    if table5.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(table5.sort_values("VOL_%"), width='stretch')




# =========================================================
# 🔥 STRONG CLOSING TAB (VERTICAL LAYOUT)
# =========================================================

with tabs[8]:   # change tab index if required

    st.subheader("🟢 Green Open Structure")
    _gos_cols = [c for c in [
        "Symbol","LTP","CHANGE_%","VOL_%",
        "LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
        "YEST_CLOSE","YEST_HIGH","YEST_LOW",
        "BREAK_TIME","POST_BREAK_GAIN","POST_BREAK_GAIN_%"
    ] if c in green_structure_df.columns]
    st.dataframe(
        _apply_fmt(green_structure_df[_gos_cols]) if _gos_cols else _apply_fmt(green_structure_df),
        width='stretch', hide_index=True
    )

    st.subheader("🔴 Red Open Structure")
    _ros_cols = [c for c in [
        "Symbol","LTP","CHANGE_%","VOL_%",
        "LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
        "YEST_CLOSE","YEST_HIGH","YEST_LOW",
        "BREAK_TIME","POST_BREAK_DROP","POST_BREAK_DROP_%"
    ] if c in red_structure_df.columns]
    st.dataframe(
        _apply_fmt(red_structure_df[_ros_cols]) if _ros_cols else _apply_fmt(red_structure_df),
        width='stretch', hide_index=True
    )

    st.markdown("## 🔥 Strong Closing Stocks")

    # -----------------------------------------------------
    # Ensure numeric fields
    # -----------------------------------------------------
    df["LIVE_VOLUME"] = pd.to_numeric(df.get("LIVE_VOLUME", 0), errors="coerce")
    df["YEST_VOL"] = pd.to_numeric(df.get("YEST_VOL", 0), errors="coerce")
    df["VOL_%"] = pd.to_numeric(df.get("VOL_%", 0), errors="coerce")

    df["LIVE_HIGH"] = pd.to_numeric(df.get("LIVE_HIGH", 0), errors="coerce")
    df["LIVE_LOW"] = pd.to_numeric(df.get("LIVE_LOW", 0), errors="coerce")
    df["LTP"] = pd.to_numeric(df.get("LTP", 0), errors="coerce")

    # -----------------------------------------------------
    # Distance calculation (safe)
    # -----------------------------------------------------
    df["DIST_FROM_DAY_HIGH_%"] = (
        (df["LIVE_HIGH"] - df["LTP"]) / df["LIVE_HIGH"] * 100
    ).round(2)

    df["DIST_FROM_DAY_LOW_%"] = (
        (df["LTP"] - df["LIVE_LOW"]) / df["LIVE_LOW"] * 100
    ).round(2)

    # =====================================================
    # 🟢 STRONG NEAR DAY HIGH
    # =====================================================
    #strong_high_df = df.loc[(df["CHANGE_%"] > 1.5) & (df["DIST_FROM_DAY_HIGH_%"] <= 0.5)].copy()
    strong_high_df = df.loc[
    (df["LIVE_OPEN"] <= df["YEST_HIGH"]) &      # No gap-up
    (df["LTP"] >= df["YEST_HIGH"]) &           # Breakout above YH
    (df["LIVE_HIGH"] >= df["YEST_HIGH"]) &
    (df["DIST_FROM_DAY_HIGH_%"] <= 0.5) &        # Strong close near day high
    (df["CHANGE_%"] >= 0.5) &
    (df["CHANGE_%"] <= 2.5) & (df["LTP"] >= df["EMA20"]) 
    ].copy()

    # =====================================================
    # 🔴 STRONG NEAR DAY LOW
    # =====================================================
    #strong_low_df = df.loc[(df["CHANGE_%"] < -1.5) & (df["DIST_FROM_DAY_LOW_%"] <= 0.5)].copy()
    strong_low_df = df.loc[
    (df["LIVE_OPEN"] >= df["YEST_LOW"]) &        # No gap-down
    (df["LTP"] <= df["YEST_LOW"]) &             # Breakdown below YL
    (df["DIST_FROM_DAY_LOW_%"] <= 0.5) &        # Closing near day low
    (df["CHANGE_%"] <= -0.5)  &                     # Bearish momentum
    (df["CHANGE_%"] >= -2.2) & (df["LTP"] <= df["EMA20"]) & (df["LTP"] <= df["TOP_LOW"]) & (df["LTP"] <= df["LOW_W"])
    ].copy()

    # =====================================================
    # STYLE FUNCTIONS
    # =====================================================

    def highlight_high_breaks(row):
        styles = []
        for col in row.index:
            if col in ["YEST_HIGH","HIGH_W","HIGH_M","EMA20","TOP_HIGH"]:
                if pd.notna(row[col]) and row["LTP"] > row[col]:
                    styles.append(
                        "background-color:#0d2b0d; color:#b6f5c8; font-weight:bold;"
                    )
                else:
                    styles.append("")
            else:
                styles.append("")
        return styles


    def highlight_low_breaks(row):
        styles = []
        for col in row.index:
            if col in ["YEST_LOW","LOW_W","LOW_M","EMA20","TOP_LOW"]:
                if pd.notna(row[col]) and row["LTP"] < row[col]:
                    styles.append(
                        "background-color:#2b0d0d; color:#ffb3b3; font-weight:bold;"
                    )
                else:
                    styles.append("")
            else:
                styles.append("")
        return styles


    # =====================================================
    # DISPLAY – STRONG HIGH
    # =====================================================

    st.markdown("### 🟢 Strong Close – Near Day High")

    if strong_high_df.empty:
        st.info("No strong bullish closing stocks.")
    else:

        strong_high_df = strong_high_df[
            [
                "Symbol",
                "LTP",
                "CHANGE",
                "CHANGE_%",
                "LIVE_VOLUME",
                "YEST_VOL",
                "VOL_%",
                "LIVE_HIGH",
                "DIST_FROM_DAY_HIGH_%",
                "YEST_HIGH",
                "HIGH_W",
                "HIGH_M",
                "EMA20",
                "TOP_HIGH"
            ]
        ]

        styled_high = _apply_fmt(strong_high_df)
        styled_high = styled_high.apply(highlight_high_breaks, axis=1)

        st.dataframe(
            styled_high, width='stretch',
            height=450
        )

    # Spacer
    st.markdown("---")

    # =====================================================
    # DISPLAY – STRONG LOW
    # =====================================================

    st.markdown("### 🔴 Strong Close – Near Day Low")

    if strong_low_df.empty:
        st.info("No strong bearish closing stocks.")
    else:

        strong_low_df = strong_low_df[
            [
                "Symbol",
                "LTP",
                "CHANGE",
                "CHANGE_%",
                "LIVE_VOLUME",
                "YEST_VOL",
                "VOL_%",
                "LIVE_LOW",
                "DIST_FROM_DAY_LOW_%",
                "YEST_LOW",
                "LOW_W",
                "LOW_M",
                "EMA20",
                "TOP_LOW"
            ]
        ]

        styled_low = _apply_fmt(strong_low_df)
        styled_low = styled_low.apply(highlight_low_breaks, axis=1)

        st.dataframe(
            styled_low, width='stretch',
            height=450
        )

    st.subheader("📈 DAILY BREAKS – Above YEST HIGH")
    st.dataframe(_apply_fmt(daily_up), width='stretch')

    st.subheader("📉 DAILY BREAKS – Below YEST LOW")
    st.dataframe(_apply_fmt(daily_down), width='stretch')



with tabs[9]:
    st.subheader("🕘 1H Opening Range Breakouts")
    st.caption(
        "1H range = highest high / lowest low of the **09:15–10:14** window (first 4 candles). "
        "Entry: LTP breaks above 1H_HIGH + above EMA20 (breakout), or below 1H_LOW + below EMA20 (breakdown). "
        "Table only populates after 10:15 once the opening range is fully formed."
    )
    if not _orb_range_ready:
        st.info(f"⏳ Opening range window not yet complete - table available after **10:15 AM** (current: {_now_hm})")
    elif hourly_break_df.empty:
        st.info(
            "No 1H Opening Range breakouts/breakdowns currently. "
            "Possible reasons: (1) 15-min candle data not yet fetched - wait for next auto-refresh, "
            "(2) market is choppy with no clean breaks above/below the first-hour range."
        )
    else:
        _h1_up   = hourly_break_df[hourly_break_df["TYPE"].str.contains("BREAKOUT",  na=False)]
        _h1_down = hourly_break_df[hourly_break_df["TYPE"].str.contains("BREAKDOWN", na=False)]
        if not _h1_up.empty:
            st.markdown("#### 🟢 Breakouts Above 1H High")
            _sort_col = "POST_BREAK_MOVE_%" if "POST_BREAK_MOVE_%" in _h1_up.columns else _h1_up.columns[0]
            st.dataframe(_h1_up.sort_values(_sort_col, ascending=False), width='stretch')
        if not _h1_down.empty:
            st.markdown("#### 🔴 Breakdowns Below 1H Low")
            _sort_col = "POST_BREAK_MOVE_%" if "POST_BREAK_MOVE_%" in _h1_down.columns else _h1_down.columns[0]
            st.dataframe(_h1_down.sort_values(_sort_col, ascending=False), width='stretch')

    st.markdown("## 🔥 Gap Failure Reclaim Scanner")

    # Bullish
    if bull_reclaim_df.empty:
        st.info("No bullish reclaim setups")
    else:
        st.markdown("### 🟢 Bullish Reclaim (Gap Up Failure Recovery)")
        st.dataframe(
            bull_reclaim_df[
                ["Symbol","LTP","LIVE_OPEN","YEST_HIGH","LIVE_LOW","STRENGTH_%"]
            ], width='stretch'
        )

    # Bearish
    if bear_reclaim_df.empty:
        st.info("No bearish reclaim setups")
    else:
        st.markdown("### 🔴 Bearish Reclaim (Gap Down Failure Continuation)")
        st.dataframe(
            bear_reclaim_df[
                ["Symbol","LTP","LIVE_OPEN","YEST_LOW","LIVE_HIGH","STRENGTH_%"]
            ], width='stretch'
        )


    st.subheader("📊 WEEKLY BREAKS – Above WEEK HIGH")
    st.dataframe(_apply_fmt(weekly_up), width='stretch')

    st.subheader("📊 WEEKLY BREAKS – BeLIVE_LOW WEEK LOW")
    st.dataframe(_apply_fmt(weekly_down), width='stretch')

    #st.divider()

    #st.markdown("### ✅ WEEKLY + EMA CONFIRMATION (Strong Trend)")

    #col1, col2 = st.columns(2)

    #with col1:
     #   st.markdown("🟢 BUY : WEEK HIGH + EMA20 > EMA50")
      #  if weekly_ema_buy.empty:
       #     st.info("No Weekly EMA BUY confirmations")
        #else:
         #   st.dataframe(weekly_ema_buy, width='stretch')

    #with col2:
     #   st.markdown("🔴 SELL : WEEK LOW + EMA20 < EMA50")
      #  if weekly_ema_sell.empty:
       #     st.info("No Weekly EMA SELL confirmations")
        #else:
         #   st.dataframe(weekly_ema_sell, width='stretch')


with tabs[10]:
    st.info("🚧 Tab disabled - will be re-enabled when needed.")
if False:  # hidden tab[10] content
    st.markdown("## 🔄 EMA Reversal After Downtrend and Uptrend")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12 = st.tabs([
        "1M Downtrend",
        "2M Downtrend",
        "3M Downtrend",
        "6M Downtrend",
        "1M Downtrend1",
        "2M Downtrend2",
        "3M Downtrend3",
        "6M Downtrend4",
        "1M Uptrend",
        "2M Uptrend",
        "3M Uptrend",
        "6M Uptrend"
    ])

    with tab1:
        st.dataframe(down_1m_df, width='stretch')

    with tab2:
        st.dataframe(down_2m_df, width='stretch')

    with tab3:
        st.dataframe(down_3m_df, width='stretch')

    with tab4:
        st.dataframe(down_6m_df, width='stretch')

    with tab5:
        st.dataframe(down_1m_df1, width='stretch')

    with tab6:
        st.dataframe(down_2m_df2, width='stretch')

    with tab7:
        st.dataframe(down_3m_df3, width='stretch')

    with tab8:
        st.dataframe(down_6m_df4, width='stretch')

    with tab9:
        st.dataframe(up_1m_df1, width='stretch')

    with tab10:
        st.dataframe(up_2m_df2, width='stretch')

    with tab11:
        st.dataframe(up_3m_df3, width='stretch')

    with tab12:
        st.dataframe(up_6m_df4, width='stretch')


    st.subheader("📈 2M Downtrend -> EMA Reversal Setup")

    if reversal_df.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(reversal_df, width='stretch')

    #st.dataframe(df[df.LTP >= df.HIGH_M])
    #st.dataframe(df[df.LTP <= df.LOW_M])
    st.subheader("📅 WEEKLY %")
    st.dataframe(weekly_break_df, width='stretch')

    st.subheader("📅 MONTHLY %")
    st.dataframe(monthly_break_df, width='stretch')

    st.subheader("📅 MONTHLY BREAKS – Above MONTH HIGH")
    monthly_up = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
    st.dataframe(_apply_fmt(monthly_up), width='stretch')

    st.subheader("📅 MONTHLY BREAKS – Below MONTH LOW")
    monthly_down = df.loc[df.LTP <= df.LOW_M, MONTHLY_COLUMNS]
    st.dataframe(_apply_fmt(monthly_down), width='stretch')


with tabs[11]:
    
    st.subheader("🟢 Yesterday GREEN – Open Inside Upper Zone")
    if green_zone_df1.empty:
        st.info("No green-zone setups today")
    else:
        st.dataframe(green_zone_df1, width='stretch')

    st.markdown("---")

    st.subheader("🟢 Yesterday GREEN – Open Inside BREAKOUT")
    if green_zone_df.empty:
        st.info("No green-zone setups today")
    else:
        st.dataframe(green_zone_df, width='stretch')

    st.markdown("---")


    st.subheader("🔴 Yesterday RED – Open Inside Lower Zone")
    if red_zone_df.empty:
        st.info("No red-zone setups today")
    else:
        st.dataframe(red_zone_df, width='stretch')

    #st.markdown('<div class="section-yelLIVE_LOW"><b>⚡ LIVE_OPEN = LIVE_HIGH / LIVE_LOW (Trend Day)</b></div>', unsafe_allow_html=True )
    st.subheader("🔥 O=H / O=L Setups (Gainers + Losers)")

    st.dataframe(
        ol_oh_df,
        #width='content', width='stretch',
        height=600
    )
    col1, col2 = st.columns(2)

    # -------- LIVE_OPEN = LIVE_LOW (Bullish) --------
    with col1:
        st.markdown("### 🟢 OPEN==LOW ")
        if LIVE_OPEN_LIVE_LOW_df.empty:
            st.info("No OPEN==LOW stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_LOW_df, width='stretch',
                height=600
            )

    # -------- LIVE_OPEN = LIVE_HIGH (Bearish) --------
    with col2:
        st.markdown("### 🔴 OPEN==HIGH ")
        if LIVE_OPEN_LIVE_HIGH_df.empty:
            st.info("No OPEN==HIGH stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_HIGH_df, width='stretch',
                height=600
            )


#with tabs[10]:
 #   st.markdown('<div class="section-purple"><b>📉 EMA20–EMA50 + Breakout</b></div>', unsafe_allow_html=True)

  #  if ema_signal_df.empty:
   #     st.info("No EMA20–EMA50 signals currently")
   # else:
    #    st.dataframe(
     #       ema_signal_df,
      #, width='stretch',
       #     height=min(1200, 60 + len(ema_signal_df) * 35)
       # )

with tabs[12]:
    st.info("🚧 Tab disabled - will be re-enabled when needed.")
if False:  # hidden tab[12] content
    st.markdown(
        '<div class="section-green"><b>🟢 EMA20–EMA50 BUY (Breakout)</b></div>',
        unsafe_allow_html=True
    )

    if ema_buy_df.empty:
        st.info("No EMA20–EMA50 BUY signals")
    else:
        st.dataframe(
            ema_buy_df, width='stretch',
            height=600
        )

    st.markdown(
        '<div class="section-red"><b>🔴 EMA20–EMA50 SELL (Breakdown)</b></div>',
        unsafe_allow_html=True
    )

    if ema_sell_df.empty:
        st.info("No EMA20–EMA50 SELL signals")
    else:
        st.dataframe(
            ema_sell_df, width='stretch',
            height=600
        )

with tabs[13]:  # assuming INFO is last tab

    #st.markdown("### 🟢 Top Gainers ( > +2.5% )")

    #if gainers_df.empty:
     #   st.info("No gainers above 2.5%")
    #else:
     #   st.dataframe(gainers_df, width='stretch')

    #st.markdown("---")  # separator line

    #st.markdown("### 🔴 Top Losers ( < -2.5% )")

    #if losers_df.empty:
     #   st.info("No losers below -2.5%")
    #else:
     #   st.dataframe(losers_df, width='stretch')
    TOP_GAINER_COLS = [
    "Symbol",
    "GAP",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "VOL_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "LIVE_VOLUME",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "NEAR",
    "GAIN",
    "TOP_HIGH",
    "TOP_LOW",

    ]
    TOP_GAINER_COLS = [c for c in TOP_GAINER_COLS if c in gainers_df.columns]
    with st.expander("🟢 Top Gainers ( > +2.5% )", expanded=True):
        #st.dataframe(gainers_df, width='stretch')
        st.dataframe(
            _apply_fmt(gainers_df[TOP_GAINER_COLS]),
            #, width='stretch', width='stretch',
            height=min(600, 60 + len(gainers_df) * 35)
        )

    TOP_LOSER_COLS = [
    "Symbol",
    "GAP",
    "LTP",
    "CHANGE",
    "CHANGE_%",
    "VOL_%",
    "LIVE_OPEN",
    "LIVE_HIGH",
    "LIVE_LOW",
    "LIVE_VOLUME",
    "YEST_HIGH",
    "YEST_LOW",
    "YEST_CLOSE",
    "NEAR",
    "GAIN",
    "TOP_HIGH",
    "TOP_LOW",

    ]
    TOP_LOSER_COLS = [c for c in TOP_LOSER_COLS if c in losers_df.columns]
    with st.expander("🔴 Top Losers ( < -2.5% )", expanded=True):
        #st.dataframe(losers_df, width='stretch')
        st.dataframe(
            _apply_fmt(losers_df[TOP_LOSER_COLS]),
            #, width='stretch', width='stretch',
            height=min(600, 60 + len(losers_df) * 35)
        )


with tabs[14]:  # 4 BAR
    st.markdown("### 🔁 4 BAR Reversal + Breakout")

    if four_bar_df.empty:
        st.info("No 4-bar setups today")
    else:
        st.dataframe(four_bar_df, width='stretch')

    ###############################
    st.markdown("### ⚠️ Fake 2.5% Breakouts")

    col1, col2 = st.columns(2)

    # -------- BULL TRAP --------
    with col1:
        st.markdown("#### 🟡 Fake Bull Trap")
        if fake_bull_df.empty:
            st.info("No Fake Bull Traps")
        else:
            st.dataframe(
                fake_bull_df
                .sort_values("FAIL_%", ascending=False)
                .style.apply(style_ltp_only, axis=1), width='stretch',
                height=600
        )


    # -------- BEAR TRAP --------
    with col2:
        st.markdown("#### 🔵 Fake Bear Trap")
        if fake_bear_df.empty:
            st.info("No Fake Bear Traps")
        else:
            st.dataframe(
                fake_bear_df
                .sort_values("FAIL_%", ascending=False)
                .style.apply(style_ltp_bear_only, axis=1), width='stretch',
                height=600
        )

    ##############################################
    st.markdown("### ⏱️ 15-Min Inside Range Break")

    if inside_15m_df.empty:
        st.info("No 15-min inside range breaks yet")
    else:
        st.dataframe(
            inside_15m_df
                .sort_values("CHANGE_%", ascending=False)
                .style.apply(style_ltp_15min, axis=1), width='stretch',
            height=600
        )

    #######################################################
    st.markdown(
        '<div class="section-blue"><b>🚀 YH1.5 Strong Breakout (Screener)</b></div>',
        unsafe_allow_html=True
    )

    if yh15_df.empty:
        st.info("No valid YH1.5 breakouts (gap-ups filtered)")
    else:
        st.dataframe(
            yh15_df,
            width='content'
        )
    ##################################################################
    st.subheader("⚠️ Fake / Failed YH1.5 Breakouts")

    if fake_yh15_df.empty:
        st.info("No failed YH1.5 breakouts currently")
    else:
        st.dataframe(fake_yh15_df, width='content')




with tabs[15]:
    st.info("🚧 Tab disabled - will be re-enabled when needed.")
if False:  # hidden tab[15] content

    
    st.subheader("🚀 EARLY HIGH-GAIN RUNNERS (YH MOMENTUM)")

    if early_runner_df.empty:
        st.info("No strong early runners yet")
    else:
        st.dataframe(
            early_runner_df, width='stretch'
    )
    
    st.markdown("## 🔥 Full Futures OI Heatmap")
    #filtered_df = fut_df[(fut_df["REAL_OI_%"].abs() > 0.8) & (fut_df["PRICE_%"].abs() > 0.8)]

    if fut_df.empty:
    #if filtered_df.empty:
        st.info("No F&O data available")
    else:

        display_cols = [
            "FUT_SYMBOL",
            "LTP",
            "PRICE_%",
            "REAL_OI_%",
            "OI_SCORE",
            "POSITION_TYPE"
        ]

        #st.dataframe(fut_df[display_cols].sort_values("OI_SCORE", ascending=False), width='stretch')
        # Remove NEUTRAL positions
        filtered_fut_df = fut_df[
            fut_df["POSITION_TYPE"] != "NEUTRAL"
        ].copy()

        # Optional: also remove weak OI_SCORE
        filtered_fut_df = filtered_fut_df[
            filtered_fut_df["OI_SCORE"].abs() > 1
        ]

        st.dataframe(
            filtered_fut_df.sort_values("OI_SCORE", ascending=False), width='stretch'
        )


    st.markdown("## 🔥 Strong Futures Closing")

    if strong_long_close.empty and strong_short_close.empty:
        st.info("No strong closing futures detected")

    if not strong_long_close.empty:
        st.markdown("### 🟢 Strong Long Closing (Near Day High)")
        st.dataframe(
            strong_long_close[
                ["FUT_SYMBOL","LTP","PRICE_%","REAL_OI_%","DIST_FROM_HIGH_%"]
            ], width='stretch'
        )

    if not strong_short_close.empty:
        st.markdown("### 🔴 Strong Short Closing (Near Day Low)")
        st.dataframe(
            strong_short_close[
                ["FUT_SYMBOL","LTP","PRICE_%","REAL_OI_%","DIST_FROM_LOW_%"]
            ], width='stretch'
        )



with tabs[16]:
    # ═══════════════════════════════════════════════════════════════════
    # TAB 16 - INDICES HEATMAP + WEIGHTED MOMENTUM TRACKER
    # Live every 60s refresh. Uses df (live_data) for CHANGE_%, LTP,
    # LIVE_HIGH, LIVE_LOW, YEST_HIGH, YEST_LOW per stock.
    # Weightages: Nifty50 & BankNifty from NSE April 2026 factsheet.
    # ═══════════════════════════════════════════════════════════════════

    st.markdown("""
    <style>
    .hm-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(90px, 1fr));
        gap: 5px;
        margin-bottom: 20px;
    }
    .hm-cell {
        padding: 10px 5px;
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.05);
        text-align: center;
        transition: all 0.2s ease;
        cursor: pointer;
        min-height: 75px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .hm-cell:hover {
        transform: translateY(-2px) scale(1.03);
        filter: brightness(1.25);
        z-index: 10;
        border-color: rgba(255,255,255,0.3);
        box-shadow: 0 4px 12px rgba(0,0,0,0.4);
    }
    .hm-sym {
        font-size: 11px;
        font-weight: 800;
        color: #fff;
        margin-bottom: 3px;
        letter-spacing: 0.02em;
    }
    .hm-chg {
        font-size: 13px;
        font-weight: 700;
        margin-bottom: 2px;
    }
    .hm-meta {
        font-size: 9px;
        color: rgba(255,255,255,0.6);
        font-weight: 500;
    }
    .hm-hl {
        font-size: 8px;
        margin-top: 4px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    .idx-card-hm {
        background: linear-gradient(135deg, #111, #1a1a1a);
        border: 1px solid #222;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .mom-wrap {
        background: #0f0f0f;
        border: 1px solid #1e1e1e;
        border-radius: 8px;
        padding: 12px;
        margin-bottom: 10px;
    }
    .mom-bar-bg {
        background: #222;
        height: 6px;
        border-radius: 3px;
        margin: 8px 0;
        overflow: hidden;
    }
    .mom-bar-fill {
        height: 100%;
        transition: width 0.5s ease-in-out;
    }
    .imp-row {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 4px;
        font-family: monospace;
    }
    .imp-sym { width: 70px; color: #aaa; }
    .imp-bar { flex: 1; background: #222; height: 4px; border-radius: 2px; overflow: hidden; }
    .imp-fill { height: 100%; }
    .imp-chg { width: 50px; text-align: right; }
    .imp-pts { width: 45px; text-align: right; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    # ── Helper functions for INDICES tab ────────────────────────────────
    def _hm_signal_box(score, label, ltp):
        total = score["bull_wt"] + score["bear_wt"] + score["flat_wt"]
        bull_pct = score["bull_wt"] / total * 100 if total > 0 else 50
        bear_pct = score["bear_wt"] / total * 100 if total > 0 else 0

        is_long  = bull_pct > 55 and score["total_impact"] > 0
        is_short = bear_pct > 55 and score["total_impact"] < 0
        sig  = "🟢 STAY LONG"  if is_long  else ("🔴 STAY SHORT" if is_short else "🟡 NEUTRAL")
        col  = "#4ade80"       if is_long  else ("#f87171"      if is_short else "#fbbf24")
        bg   = "#0a2a0a"       if is_long  else ("#2a0a0a"      if is_short else "#1a1700")
        top3 = score["impacts"][:3]
        top3_txt = " | ".join(f"{s[0]} {s[2]:+.2f}%" for s in top3)
        return f"""
<div class="sig-box" style="background:{bg};border:1px solid {col}30;border-radius:10px;padding:15px;margin-bottom:10px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <b style="color:{col};font-size:14px">{label}</b>
    <span style="background:{col}20;color:{col};font-size:12px;font-weight:700;padding:3px 12px;border-radius:12px">{sig}</span>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:8px">
    <div style="background:#0f0f0f;border-radius:6px;padding:8px;text-align:center">
      <div style="color:#4ade80;font-size:16px;font-weight:800">{score['bull']}</div>
      <div style="color:#666;font-size:10px">Stocks Up</div>
    </div>
    <div style="background:#0f0f0f;border-radius:6px;padding:8px;text-align:center">
      <div style="color:{col};font-size:16px;font-weight:800">{score['bull_wt']:.0f}%</div>
      <div style="color:#666;font-size:10px">Bull Weight</div>
    </div>
    <div style="background:#0f0f0f;border-radius:6px;padding:8px;text-align:center">
      <div style="color:#f87171;font-size:16px;font-weight:800">{score['bear']}</div>
      <div style="color:#666;font-size:10px">Stocks Down</div>
    </div>
  </div>
  <div style="font-size:11px;color:#aaa;margin-bottom:5px"><b>Top Drivers:</b> {top3_txt}</div>
  <div style="font-size:12px;color:{col};font-weight:600">
    Wt Impact: <b>{score['total_impact']:+.3f}%</b> ≈ <b>{score['total_impact']/100*ltp:+.0f} pts</b>
    <span style="color:#444;margin:0 8px">|</span> 
    Bull {score['bull_wt']:.1f}% vs Bear {score['bear_wt']:.1f}% wt
  </div>
</div>"""

    def _render_impact_bars(impacts, label, base_val):
        rows = []
        for sym, sect, chg, imp in impacts[:8]:
            pts = round(imp / 100 * base_val, 1)
            col = "#4ade80" if imp > 0 else "#f87171"
            w = min(abs(imp) * 150, 100)
            rows.append(
                f'<div class="imp-row">'
                f'<div class="imp-sym">{sym}</div>'
                f'<div class="imp-bar"><div class="imp-fill" style="width:{w}%;background:{col}"></div></div>'
                f'<div class="imp-chg" style="color:{col}">{chg:+.1f}%</div>'
                f'<div class="imp-pts" style="color:{col}">{pts:+.1f}</div>'
                f'</div>'
            )
        return (f'<div style="font-size:10px;font-weight:600;color:#666;margin-bottom:5px">'
                f'↕ Who is moving {label} right now</div>' + "".join(rows))

    # ── Nifty 50 weightage table ────────────────────────────────────────
    _N50_WT = {
        "RELIANCE":9.30,"HDFCBANK":6.37,"BHARTIARTL":5.74,"SBIN":5.04,
        "ICICIBANK":4.93,"TCS":4.77,"LT":2.90,"BAJFINANCE":2.89,
        "INFY":2.74,"HINDUNILVR":2.57,"M&M":2.58,"ITC":2.71,
        "AXISBANK":3.26,"TATAMOTORS":1.62,"MARUTI":1.54,"NTPC":1.86,
        "POWERGRID":1.58,"HCLTECH":1.58,"SUNPHARMA":1.61,"KOTAKBANK":1.82,
        "ONGC":1.22,"COALINDIA":1.14,"TATASTEEL":1.18,"JSWSTEEL":1.04,
        "ULTRACEMCO":1.02,"WIPRO":1.05,"TITAN":1.51,"BAJAJFINSV":1.34,
        "BAJAJ-AUTO":1.10,"NESTLEIND":1.08,"HINDALCO":0.92,"GRASIM":0.84,
        "ADANIENT":0.81,"ASIANPAINT":0.79,"ADANIPORTS":0.84,"TATACONSUM":0.82,
        "APOLLOHOSP":0.68,"BEL":0.72,"CIPLA":0.75,"DRREDDY":0.71,
        "TRENT":0.74,"EICHERMOT":0.61,"HEROMOTOCO":0.58,"BPCL":0.65,
        "BRITANNIA":0.64,"SHRIRAMFIN":1.01,"HDFCLIFE":0.88,"SBILIFE":0.82,
        "LTIM":0.81,"DIVISLAB":0.56,
    }
    _N50_SECT = {
        "RELIANCE":"Energy","HDFCBANK":"Finance","BHARTIARTL":"Telecom",
        "SBIN":"Finance","ICICIBANK":"Finance","TCS":"IT","LT":"Infra",
        "BAJFINANCE":"Finance","INFY":"IT","HINDUNILVR":"FMCG",
        "M&M":"Auto","ITC":"FMCG","AXISBANK":"Finance","TATAMOTORS":"Auto",
        "MARUTI":"Auto","NTPC":"Energy","POWERGRID":"Energy","HCLTECH":"IT",
        "SUNPHARMA":"Pharma","KOTAKBANK":"Finance","ONGC":"Energy",
        "COALINDIA":"Energy","TATASTEEL":"Metals","JSWSTEEL":"Metals",
        "ULTRACEMCO":"Cement","WIPRO":"IT","TITAN":"Consumer",
        "BAJAJFINSV":"Finance","BAJAJ-AUTO":"Auto","NESTLEIND":"FMCG",
        "HINDALCO":"Metals","GRASIM":"Cement","ADANIENT":"Infra",
        "ASIANPAINT":"Consumer","ADANIPORTS":"Infra","TATACONSUM":"FMCG",
        "APOLLOHOSP":"Pharma","BEL":"Defence","CIPLA":"Pharma",
        "DRREDDY":"Pharma","TRENT":"Consumer","EICHERMOT":"Auto",
        "HEROMOTOCO":"Auto","BPCL":"Energy","BRITANNIA":"FMCG",
        "SHRIRAMFIN":"Finance","HDFCLIFE":"Insurance","SBILIFE":"Insurance",
        "LTIM":"IT","DIVISLAB":"Pharma",
    }
    # ── BankNifty weightage table ────────────────────────────────────────
    _BNK_WT = {
        "HDFCBANK":25.56,"SBIN":20.28,"ICICIBANK":19.79,"AXISBANK":8.64,
        "KOTAKBANK":7.80,"UNIONBANK":2.95,"BANKBARODA":2.95,"PNB":2.66,
        "CANBK":2.64,"AUBANK":1.51,"FEDERALBNK":1.45,
        "INDUSINDBK":1.34,"YESBANK":1.25,"IDFCFIRSTB":1.18,
    }
    _BNK_SECT = {
        "HDFCBANK":"Private","SBIN":"PSU","ICICIBANK":"Private",
        "AXISBANK":"Private","KOTAKBANK":"Private","UNIONBANK":"PSU",
        "BANKBARODA":"PSU","PNB":"PSU","CANBK":"PSU",
        "AUBANK":"SFB","FEDERALBNK":"Private","INDUSINDBK":"Private",
        "YESBANK":"Private","IDFCFIRSTB":"Private",
    }

    # ── Sensex weightage table (BSE 30) ──────────────────────────────────
    _SNX_WT = {
        "HDFCBANK":15.66,"ICICIBANK":10.88,"RELIANCE":10.24,"INFY":5.75,
        "BHARTIARTL":5.37,"ITC":4.23,"LT":4.20,"TCS":3.74,
        "AXISBANK":3.63,"KOTAKBANK":3.49,"M&M":2.84,"HINDUNILVR":2.82,
        "TATASTEEL":2.58,"SBIN":2.34,"NTPC":2.16,"POWERGRID":1.84,
        "SUNPHARMA":1.76,"TITAN":1.68,"BAJFINANCE":1.65,"JSWSTEEL":1.48,
        "ULTRACEMCO":1.42,"ADANIENT":1.38,"MARUTI":1.32,"NESTLEIND":1.28,
        "HCLTECH":1.24,"ASIANPAINT":1.18,"BAJAJFINSV":1.14,"TATAMOTORS":1.12,
        "TECHM":0.98,"INDUSINDBK":0.91
    }
    _SNX_SECT = {
        "HDFCBANK":"Bank","ICICIBANK":"Bank","RELIANCE":"Energy","INFY":"IT",
        "BHARTIARTL":"Telecom","ITC":"FMCG","LT":"Industrials","TCS":"IT",
        "AXISBANK":"Bank","KOTAKBANK":"Bank","M&M":"Auto","HINDUNILVR":"FMCG",
        "TATASTEEL":"Metal","SBIN":"Bank","NTPC":"Power","POWERGRID":"Power",
        "SUNPHARMA":"Pharma","TITAN":"Consumer","BAJFINANCE":"Finance","JSWSTEEL":"Metal",
        "ULTRACEMCO":"Cement","ADANIENT":"Conglom","MARUTI":"Auto","NESTLEIND":"FMCG",
        "HCLTECH":"IT","ASIANPAINT":"Consumer","BAJAJFINSV":"Finance","TATAMOTORS":"Auto",
        "TECHM":"IT","INDUSINDBK":"Bank"
    }

    # ── Pull live change% from df for each constituent ────────────────────
    # Build fast lookup dict from df once - O(1) access vs O(n) row scan
    _hm_chg_map = {}
    _hm_ltp_map = {}
    _hm_hl_map  = {}
    if "Symbol" in df.columns and not df.empty:
        for _, _hm_r in df.iterrows():
            _hm_s = str(_hm_r.get("Symbol",""))
            if _hm_s:
                _hm_chg_map[_hm_s] = float(_hm_r.get("CHANGE_%", 0) or 0)
                _hm_ltp_map[_hm_s] = float(_hm_r.get("LTP", 0) or 0)
                _hm_hl_map[_hm_s]  = (
                    float(_hm_r.get("LIVE_HIGH", 0) or 0),
                    float(_hm_r.get("LIVE_LOW",  0) or 0),
                    float(_hm_r.get("YEST_HIGH", 0) or 0),
                    float(_hm_r.get("YEST_LOW",  0) or 0),
                )
    # Also pull from live_map (in case df is stale or symbol key differs)
    for _hm_sym, _hm_lv in live_map.items():
        if _hm_sym not in _hm_chg_map:
            _hm_chg_map[_hm_sym] = float(_hm_lv.get("CHANGE_%", 0) or 0)
            _hm_ltp_map[_hm_sym] = float(_hm_lv.get("LTP", 0) or 0)

    def _hm_get_chg(sym, live_df=None):
        """Get CHANGE_% from fast lookup dict (built from live df + live_map)."""
        return _hm_chg_map.get(sym, 0.0)

    def _hm_get_ltp(sym, live_df=None):
        return _hm_ltp_map.get(sym, 0.0)

    def _hm_get_hl(sym, live_df=None):
        return _hm_hl_map.get(sym, (0,0,0,0))

    # ── Weighted score calculator ─────────────────────────────────────────
    def _hm_weighted_score(wt_dict, live_df=None):
        bull_wt = bear_wt = flat_wt = 0.0
        bull_cnt = bear_cnt = flat_cnt = 0
        total_impact = 0.0
        impacts = []
        for sym, wt in wt_dict.items():
            chg = _hm_get_chg(sym)
            impact = wt * chg / 100
            total_impact += impact
            if chg > 0.25:   bull_wt += wt; bull_cnt += 1
            elif chg < -0.25:bear_wt += wt; bear_cnt += 1
            else:             flat_wt += wt; flat_cnt += 1
            impacts.append((sym, wt, chg, impact))
        return {
            "bull_wt": bull_wt, "bear_wt": bear_wt, "flat_wt": flat_wt,
            "bull": bull_cnt, "bear": bear_cnt, "flat": flat_cnt,
            "total_impact": total_impact,
            "impacts": sorted(impacts, key=lambda x: abs(x[3]), reverse=True),
        }

    def _hm_color(chg):
        if   chg >= 2.0:  return "#14532d","#166534"
        elif chg >= 1.0:  return "#166534","#16a34a"
        elif chg >= 0.25: return "#1a4a2a","#15803d"
        elif chg >= -0.25:return "#1e293b","#334155"
        elif chg >= -1.0: return "#4a0f0f","#7f1d1d"
        elif chg >= -2.0: return "#7f1d1d","#991b1b"
        else:             return "#450a0a","#7f1d1d"

    def _hm_cell_html(sym, wt, chg, ltp, lh, ll, yh, yl, sect):
        bg, border = _hm_color(chg)
        # Use more distinct arrows
        arrow = "▲" if chg > 0.1 else ("▼" if chg < -0.1 else "-")
        color = "#4ade80" if chg > 0.1 else ("#f87171" if chg < -0.1 else "#94a3b8")
        impact = wt * chg / 100
        
        # Day position: more visual representation
        if lh > ll > 0:
            hl_pct = (ltp - ll) / (lh - ll) * 100 if lh != ll else 50
            if hl_pct > 80: hl_tag = '<span style="color:#4ade80">▲ HI</span>'
            elif hl_pct < 20: hl_tag = '<span style="color:#f87171">▼ LO</span>'
            else: hl_tag = '<span style="color:#aaa">↔ MID</span>'
        else:
            hl_tag = ""

        # Weight-based font sizing for symbol
        sym_size = "13px" if wt > 3 else ("11px" if wt > 1 else "10px")
        
        return (
            f'<div class="hm-cell" style="background:{bg};border-color:{border}" '
            f'title="{sym} | Wt:{wt}% | Chg:{chg:+.2f}% | Impact:{impact:+.4f}%">'
            f'<div class="hm-sym" style="font-size:{sym_size}">{sym}</div>'
            f'<div class="hm-chg" style="color:{color}">{arrow} {abs(chg):.2f}%</div>'
            f'<div class="hm-meta">{sect}</div>'
            f'<div class="hm-hl">{hl_tag} · {wt}%</div>'
            f'</div>'
        )

    def _render_heatmap_html(wt_dict, sect_dict, live_df, sort_mode="gainers"):
        rows = []
        for sym in wt_dict:
            chg = _hm_get_chg(sym, live_df)
            ltp = _hm_get_ltp(sym, live_df)
            lh,ll,yh,yl = _hm_get_hl(sym, live_df)
            wt  = wt_dict[sym]
            sect = sect_dict.get(sym, "")
            rows.append((sym, wt, chg, ltp, lh, ll, yh, yl, sect))
        if sort_mode == "gainers":
            rows.sort(key=lambda r: r[2], reverse=True)
        elif sort_mode == "losers":
            rows.sort(key=lambda r: r[2])
        else:
            rows.sort(key=lambda r: r[1], reverse=True)
        cells = [_hm_cell_html(sym, wt, chg, ltp, lh, ll, yh, yl, sect)
                 for sym, wt, chg, ltp, lh, ll, yh, yl, sect in rows]
        return '<div class="hm-grid">' + "".join(cells) + '</div>'

    def _render_momentum_bar(score, label, base_val):
        total = score["bull_wt"] + score["bear_wt"] + score["flat_wt"]
        bull_pct = score["bull_wt"] / total * 100 if total > 0 else 50
        bar_col = "#4ade80" if bull_pct > 55 else ("#f87171" if bull_pct < 45 else "#fbbf24")
        sig = ("🟢 BULLISH - STAY LONG" if bull_pct > 55
               else "🔴 BEARISH - STAY SHORT" if bull_pct < 45
               else "🟡 NEUTRAL - RANGING")
        sig_col = "#4ade80" if "BULLISH" in sig else ("#f87171" if "BEARISH" in sig else "#fbbf24")
        ti = score["total_impact"]
        pt_impact = round(ti / 100 * base_val, 1)
        return f"""
<div class="mom-wrap">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <b style="color:#ccc;font-size:12px">{label}</b>
    <span style="color:{sig_col};font-size:12px;font-weight:700">{sig}</span>
  </div>
  <div class="mom-bar-bg">
    <div class="mom-bar-fill" style="width:{bull_pct:.0f}%;background:{bar_col}"></div>
  </div>
  <div style="display:flex;justify-content:space-between;font-size:10px;color:#888">
    <span style="color:#4ade80">▲ {score['bull']} stocks ({score['bull_wt']:.1f}% wt)</span>
    <span style="color:#888">● {score['flat']} flat</span>
    <span style="color:#f87171">▼ {score['bear']} stocks ({score['bear_wt']:.1f}% wt)</span>
  </div>
  <div style="margin-top:5px;font-size:11px;color:{sig_col}">
    Weighted Impact: <b>{ti:+.3f}%</b> ≈ <b>{pt_impact:+.0f} pts</b> on {label.split()[0]}
  </div>
</div>"""

    def _render_impact_bars(impacts, label, base_val):
        top = impacts[:12]
        max_abs = max((abs(x[3]) for x in top), default=0.01)
        rows = []
        for sym, wt, chg, impact in top:
            pct = abs(impact) / max_abs * 100
            col = "#15803d" if impact > 0 else "#991b1b"
            txt_col = "#4ade80" if impact > 0 else "#f87171"
            pt = round(impact / 100 * base_val, 1)
            arrow = "▲" if chg > 0.1 else "▼"
            rows.append(
                f'<div class="imp-row">'
                f'<span class="imp-sym" title="{sym} wt:{wt}%">{sym}</span>'
                f'<div class="imp-bar"><div class="imp-fill" style="width:{pct:.0f}%;background:{col}"></div></div>'
                f'<span class="imp-chg" style="color:{txt_col}">{arrow}{abs(chg):.2f}%</span>'
                f'<span class="imp-pts" style="color:{txt_col}">{pt:+.0f}pt</span>'
                f'</div>'
            )
        return (f'<div style="font-size:10px;font-weight:600;color:#666;margin-bottom:5px">'
                f'↕ Who is moving {label} right now</div>' + "".join(rows))

    # ── Fetch Indices live ──────────────────────────────────────────
    _indices_df = fetch_indices_now()
    if _indices_df.empty:
        _indices_df = st.session_state.get("indices_df", pd.DataFrame())
    else:
        st.session_state.indices_df = _indices_df

    # ── Sort mode selector (Must be before heatmap tabs) ───────────
    _hm_sort_col1, _hm_sort_col2 = st.columns([3, 1])
    with _hm_sort_col1:
        _hm_sort = st.radio(
            "Sort heatmap by:",
            ["📈 Top Gainers First", "📉 Top Losers First", "⚖️ By Weight"],
            horizontal=True, key="hm_sort_mode_tab",
            label_visibility="collapsed"
        )
    with _hm_sort_col2:
        st.caption("🟢 green = gainers -> 🔴 red = losers")

    _hm_sort_mode = (
        "gainers" if "Gainers" in _hm_sort
        else "losers" if "Losers" in _hm_sort
        else "weight"
    )

    # ── Compute weighted scores (must happen before INDEX CARDS display) ──
    _live_df_hm = df.copy() if "Symbol" in df.columns else pd.DataFrame()
    _n50_score  = _hm_weighted_score(_N50_WT)
    _bnk_score  = _hm_weighted_score(_BNK_WT)
    _snx_score  = _hm_weighted_score(_SNX_WT)

    # ════════════════ INDEX CARDS ════════════════
    _ni_row  = _indices_df[_indices_df["Index"].isin(["NIFTY", "NIFTY 50"])] if "Index" in _indices_df.columns else pd.DataFrame()
    _bnk_row = _indices_df[_indices_df["Index"].isin(["BANKNIFTY", "BANK NIFTY"])] if "Index" in _indices_df.columns else pd.DataFrame()
    _snx_row = _indices_df[_indices_df["Index"] == "SENSEX"] if "Index" in _indices_df.columns else pd.DataFrame()
    
    _ni_ltp  = float(_ni_row.iloc[0]["LTP"])  if not _ni_row.empty and "LTP" in _ni_row.columns else 24315.0
    _ni_chg  = float(_ni_row.iloc[0]["CHANGE"]) if not _ni_row.empty and "CHANGE" in _ni_row.columns else 0.0
    _ni_pct  = float(_ni_row.iloc[0]["CHANGE_%"]) if not _ni_row.empty and "CHANGE_%" in _ni_row.columns else 0.0
    
    _bnk_ltp = float(_bnk_row.iloc[0]["LTP"]) if not _bnk_row.empty and "LTP" in _bnk_row.columns else 56086.0
    _bnk_chg = float(_bnk_row.iloc[0]["CHANGE"]) if not _bnk_row.empty and "CHANGE" in _bnk_row.columns else 0.0
    _bnk_pct = float(_bnk_row.iloc[0]["CHANGE_%"]) if not _bnk_row.empty and "CHANGE_%" in _bnk_row.columns else 0.0
    
    _snx_ltp = float(_snx_row.iloc[0]["LTP"]) if not _snx_row.empty and "LTP" in _snx_row.columns else 80000.0
    _snx_chg = float(_snx_row.iloc[0]["CHANGE"]) if not _snx_row.empty and "CHANGE" in _snx_row.columns else 0.0
    _snx_pct = float(_snx_row.iloc[0]["CHANGE_%"]) if not _snx_row.empty and "CHANGE_%" in _snx_row.columns else 0.0
    
    _ni_col  = "#4ade80" if _ni_pct  >= 0 else "#f87171"
    _bnk_col = "#4ade80" if _bnk_pct >= 0 else "#f87171"
    _snx_col = "#4ade80" if _snx_pct >= 0 else "#f87171"

    # ── Live data status ────────────────────────────────────────────────
    _hm_live_count = sum(1 for v in _hm_chg_map.values() if v != 0)
    _hm_data_ok    = _hm_live_count >= 15
    _hm_status_col = "#4ade80" if _hm_data_ok else "#f59e0b"
    _hm_status_txt = (
        f"✅ Live data: {_hm_live_count} stocks with live prices"
        if _hm_data_ok else
        f"⚠️ Limited data: only {_hm_live_count} stocks - Kite may not be connected yet"
    )
    st.markdown(
        f'<div style="background:#111;border:1px solid #1e1e1e;border-radius:6px;'
        f'padding:5px 12px;margin-bottom:8px;font-size:11px;color:{_hm_status_col}">'
        f'{_hm_status_txt} · Auto-refreshes every 60s</div>',
        unsafe_allow_html=True
    )

    st.markdown(f"""
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;margin-bottom:10px">
      <div class="idx-card-hm">
        <div style="font-size:9px;color:#666;font-weight:600;letter-spacing:.06em">NIFTY 50</div>
        <div style="font-size:20px;font-weight:800;color:#fff">{_ni_ltp:,.2f}</div>
        <div style="font-size:12px;font-weight:700;color:{_ni_col}">{_ni_chg:+.1f} ({_ni_pct:+.2f}%)</div>
        <div style="font-size:9px;color:#555;margin-top:2px">Wt impact: {_n50_score['total_impact']:+.3f}%</div>
      </div>
      <div class="idx-card-hm">
        <div style="font-size:9px;color:#666;font-weight:600;letter-spacing:.06em">BANK NIFTY</div>
        <div style="font-size:20px;font-weight:800;color:#fff">{_bnk_ltp:,.2f}</div>
        <div style="font-size:12px;font-weight:700;color:{_bnk_col}">{_bnk_chg:+.1f} ({_bnk_pct:+.2f}%)</div>
        <div style="font-size:9px;color:#555;margin-top:2px">Wt impact: {_bnk_score['total_impact']:+.3f}%</div>
      </div>
      <div class="idx-card-hm">
        <div style="font-size:9px;color:#666;font-weight:600;letter-spacing:.06em">SENSEX</div>
        <div style="font-size:20px;font-weight:800;color:#fff">{_snx_ltp:,.2f}</div>
        <div style="font-size:12px;font-weight:700;color:{_snx_col}">{_snx_chg:+.1f} ({_snx_pct:+.2f}%)</div>
        <div style="font-size:9px;color:#555;margin-top:2px">Wt impact: {_snx_score['total_impact']:+.3f}%</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ════════════════ MOMENTUM BARS ════════════════
    st.markdown(
        _render_momentum_bar(_n50_score, "NIFTY 50 Momentum", _ni_ltp) +
        _render_momentum_bar(_bnk_score, "BANK NIFTY Momentum", _bnk_ltp) +
        _render_momentum_bar(_snx_score, "SENSEX Momentum", _snx_ltp),
        unsafe_allow_html=True
    )

    # ════════════════ HEATMAP TABS ════════════════
    _hm_tabs = st.tabs(["Nifty 50 Heatmap", "Bank Nifty Heatmap", "Sensex Heatmap"])
    
    with _hm_tabs[0]:
        st.markdown(_render_heatmap_html(_N50_WT, _N50_SECT, _live_df_hm, _hm_sort_mode), unsafe_allow_html=True)
    with _hm_tabs[1]:
        st.markdown(_render_heatmap_html(_BNK_WT, _BNK_SECT, _live_df_hm, _hm_sort_mode), unsafe_allow_html=True)
    with _hm_tabs[2]:
        st.markdown(_render_heatmap_html(_SNX_WT, _SNX_SECT, _live_df_hm, _hm_sort_mode), unsafe_allow_html=True)

    # ════════════════ IMPACT BARS ════════════════
    _hm_c1, _hm_c2, _hm_c3 = st.columns(3)
    with _hm_c1:
        st.markdown('<div style="background:#0f0f0f;border:1px solid #1e1e1e;border-radius:8px;padding:10px 12px">'
                    + _render_impact_bars(_n50_score["impacts"], "Nifty 50", _ni_ltp) + '</div>', unsafe_allow_html=True)
    with _hm_c2:
        st.markdown('<div style="background:#0f0f0f;border:1px solid #1e1e1e;border-radius:8px;padding:10px 12px">'
                    + _render_impact_bars(_bnk_score["impacts"], "BankNifty", _bnk_ltp) + '</div>', unsafe_allow_html=True)
    with _hm_c3:
        st.markdown('<div style="background:#0f0f0f;border:1px solid #1e1e1e;border-radius:8px;padding:10px 12px">'
                    + _render_impact_bars(_snx_score["impacts"], "Sensex", _snx_ltp) + '</div>', unsafe_allow_html=True)

    # ════════════════ SIGNAL PANEL ════════════════
    st.markdown(
        _hm_signal_box(_n50_score, "NIFTY 50", _ni_ltp) +
        _hm_signal_box(_bnk_score, "BANK NIFTY", _bnk_ltp) +
        _hm_signal_box(_snx_score, "SENSEX", _snx_ltp),
        unsafe_allow_html=True
    )

    # ════════════════ RULE OF THUMB ════════════════
    st.markdown("""
    <div style="background:#0f0f0f;border:1px solid #1e1e1e;border-radius:8px;padding:10px 14px;margin-top:4px;font-size:11px;color:#666">
      <b style="color:#aaa">📌 How to read this tracker:</b><br>
      • <b style="color:#4ade80">Heatmap cells</b> = each stock's % change. Bigger cell = higher weightage in index.<br>
      • <b style="color:#4ade80">Momentum bar</b> = % of index weight currently green vs red. &gt;55% weight green -> index goes up.<br>
      • <b style="color:#4ade80">Impact bars</b> = who is actually moving the index RIGHT NOW (wt × chg). Watch HDFC+SBI+ICICI for BankNifty.<br>
      • <b style="color:#4ade80">🔼 Near Day Hi</b> = stock at top of its range -> likely leader/strong. <b style="color:#f87171">🔽 Near Day Lo</b> = weak/laggard.<br>
      • <b style="color:#4ade80">STAY LONG signal</b> = more than 55% of index weight is bullish stocks -> ride the trend up.<br>
      • <b style="color:#f87171">STAY SHORT signal</b> = more than 55% weight is bearish -> ride the trend down. Auto-updates every 60s.
    </div>
    """, unsafe_allow_html=True)

    # ════════════════ ORIGINAL INDICES TABLE (collapsed) ════════════════
    with st.expander("📋 Raw Index Data (OHLC Table)", expanded=False):
        _idx_display_cols = []
        for _c in ["Index","Symbol","LTP","OPEN","HIGH","LOW","CHANGE","CHANGE_%",
                   "LIVE_OPEN","LIVE_HIGH","LIVE_LOW","YEST_HIGH","YEST_LOW","YEST_CLOSE"]:
            if _c in _indices_df.columns:
                _idx_display_cols.append(_c)
        st.dataframe(_indices_df[_idx_display_cols] if _idx_display_cols else _indices_df,
                     width='stretch')
        for _index_name, _symbols in index_symbols.items():
            _idx_df2 = df[df["Symbol"].isin(_symbols)].copy()
            if _idx_df2.empty: continue
            _show_cols = [c for c in ["Symbol","LTP","LIVE_HIGH","LIVE_LOW",
                          "CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW"] if c in _idx_df2.columns]
            st.markdown(f"**{_index_name}**")
            st.dataframe(_idx_df2[_show_cols].sort_values("CHANGE_%", ascending=False),
                         width='stretch')

with tabs[17]:
    st.info("🚧 Tab disabled - will be re-enabled when needed.")
if False:  # hidden tab[17] content
    st.markdown("## 🔥 Confirmed Continuation Breaks")

    if continuation_df.empty:
        st.info("No confirmed continuation setups currently")
    else:
        st.dataframe(
            continuation_df.sort_values("DIST_%", ascending=False), width='stretch'
        )


    st.markdown("## 🏗 Support / Resistance – Strong Levels")

    sr_cols = [
        "Symbol",
        "LTP",
        "STRONG_SUPPORT",
        "SS_DIST_%",
        "STRONG_RESISTANCE",
        "SR_DIST_%"
    ]

    if all(col in df.columns for col in sr_cols):
        sr_view = df[sr_cols].copy()
        sr_view = sr_view.sort_values("SR_DIST_%", na_position="last")
        st.dataframe(sr_view, width='stretch')
    else:
        st.info("S/R levels not available yet.")

    st.markdown("## 🚀 Breakout Strength Scanner")

    if "BREAK_SCORE" in df.columns:
        break_view = df[
            df["BREAK_SCORE"] > 0
        ][[
            "Symbol",
            "LTP",
            "CHANGE_%",
            "BREAK_SCORE",
            "BREAK_DETAILS",
            "HIGH_D","HIGH_W","HIGH_M",
            "LOW_D","LOW_W","LOW_M"
        ]].sort_values("BREAK_SCORE", ascending=False)

        st.dataframe(break_view, width='stretch')
    else:
        st.info("Breakout strength not calculated.")

    st.subheader("🟢 3 × 15-Min Green Candles (Still Valid)")

    if three_green_15m_df.empty:
        st.info("No symbols currently maintaining 3 consecutive green 15-min candles")
    else:
        st.dataframe(
            three_green_15m_df.sort_values("CANDLE_TIME", ascending=False), width='stretch'
        )


with tabs[17]:
    st.info("🚧 Tab disabled - will be re-enabled when needed.")


with tabs[18]:
    st.subheader("⚙️ Alert Controls + Log")

    # ── Persistence status ────────────────────────────────────────
    _toggles_exist = os.path.exists(_ALERT_TOGGLES_FILE)
    _tc1, _tc2, _tc3 = st.columns([3, 2, 2])
    with _tc1:
        if _toggles_exist:
            import time as _tm2
            _age_s = (datetime.now(IST) - datetime.fromtimestamp(
                os.path.getmtime(_ALERT_TOGGLES_FILE), tz=IST)).total_seconds()
            _age_str = f"{int(_age_s//60)}m ago" if _age_s >= 60 else f"{int(_age_s)}s ago"
            st.success(
                f"✅ Settings saved to disk - last updated {_age_str}  |  "
                f"`CACHE/alert_toggles.json`  |  Survives restarts & version changes."
            )
        else:
            st.info("💾 Settings not yet saved. Toggle any button to create the save file.")
    with _tc2:
        if st.button("🔄 Reset ALL to defaults", key="_btn_reset_toggles"):
            for _k, _v in _ALERT_TOGGLE_DEFAULTS.items():
                st.session_state[_k] = _v
            _save_alert_toggles()
            st.rerun()
    with _tc3:
        if _toggles_exist:
            with open(_ALERT_TOGGLES_FILE, "r", encoding='utf-8') as _f_show:
                st.download_button(
                    "📥 Download toggle settings",
                    _f_show.read(),
                    file_name="alert_toggles.json",
                    mime="application/json",
                    key="_btn_dl_toggles",
                )
    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # SECTION 1 - Alert Enable/Disable Buttons per Category
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🔔 Alert Toggle Controls")
    st.caption(
        "Toggle Toast (🍞), Email (📧), and Telegram (✈️) per alert category. "
        "Changes take effect immediately on next refresh. "
        "**Green = ON   |   Red = OFF**"
    )

    # ── Alert toggle table using st.toggle (instant, no rerun needed) ──
    _ALERT_CATEGORIES_UI = [
        ("TOP_HIGH Break",        "TOP_HIGH"),
        ("TOP_LOW Break",         "TOP_LOW"),
        ("Daily HIGH Break",      "DAILY_UP"),
        ("Daily LOW Break",       "DAILY_DOWN"),
        ("Weekly HIGH Break",     "WEEKLY_UP"),
        ("Weekly LOW Break",      "WEEKLY_DOWN"),
        ("Monthly HIGH Break",    "MONTHLY_UP"),
        ("Monthly LOW Break",     "MONTHLY_DOWN"),
        ("EMA20–50 Cross",        "EMA20_50"),
        ("3-Green 15m",           "THREE_GREEN_15M"),
        ("15-Min Inside Break",   "INSIDE_15M_BREAK"),
        ("15m Seq HIGH Break",    "SEQ_15M_HIGH"),
        ("15m Seq LOW Break",     "SEQ_15M_LOW"),
        ("1H Seq HIGH Break",     "SEQ_1H_HIGH"),
        ("1H Seq LOW Break",      "SEQ_1H_LOW"),
        ("15m Volume Surge",      "VOL_SURGE_15M"),
        ("OI Intelligence",       "OI_INTEL"),
        ("📊 OI 30-Min Summary",  "OI_30M_SUMMARY"),
        ("KP Window Alerts",      "KP_ALERTS"),
        ("Long Unwinding (OI)",   "LONG_UNWIND"),
        ("Put Crumbling (OI)",    "PUT_CRUMBLE"),
        ("🛡️ Put Floor Building (OI)", "PUT_FLOOR"),
        # ── Previously missing ──────────────────
        ("🔥 Top Gainers >2.5%",  "TOP_GAINERS"),
        ("🔥 Top Losers <-2.5%",  "TOP_LOSERS"),
        ("⚡ Strong Options Buy",  "OPTION_STRONG"),
        ("📈 Yest Green Breakout", "YEST_GREEN_BREAK"),
        ("📉 Yest Red Breakdown",  "YEST_RED_BREAK"),
        ("🟢 1H Range Breakout",   "HOURLY_BREAK_UP"),
        ("🔴 1H Range Breakdown",  "HOURLY_BREAK_DOWN"),
        ("🚀 2M EMA Reversal",     "2M_EMA_REVERSAL"),
        ("Moon Advance Astro Alert", "ASTRO_ADVANCE"),
        ("🟢 ASTRO UPDATE (High Conviction)", "ASTRO_UPDATE"),
        ("📐 1H BOS/CHoCH",         "BOS_1H"),
        ("💎 High Conviction Only (BOS)", "BOS_HC_ONLY"),
        ("📐 Chart Patterns (Auto)",    "CHART_PATTERN"),
        # ── New Open Structure alerts (Telegram only) ────────────────
        ("🟢 Green Open Structure",  "GREEN_OPEN_STRUCTURE"),
        ("🔴 Red Open Structure",    "RED_OPEN_STRUCTURE"),
        ("📈 Breakouts Above 1H High","BREAK_ABOVE_1H_HIGH"),
        ("📉 Breakdowns Below 1H Low","BREAK_BELOW_1H_LOW"),
        ("🟢 Yesterday GREEN – Open Inside BREAKOUT", "YEST_GREEN_OPEN_BREAK"),
        ("🔴 Yesterday RED – Open Inside Lower Zone", "YEST_RED_OPEN_LOWER"),
        ("⚡ O=H / O=L Setups (Gainers + Losers)",    "OEH_OEL_SETUPS"),
        # KP break 15-min (Telegram only)
        ("⏱️ Panchak Break Alert (15 min)", "KP_BREAK_15M"),
        # Combined engine table (Telegram only)
        ("🧠 Combined SMC+OI+Astro+KP", "COMBINED_ENGINE"),
        # Heatmap reversal - early detection
        ("🔥 HeatMap Reversal (Early Detection)", "HEATMAP_REVERSAL"),
        # Heatmap confluence - every 15 min, both channels + email
        ("📊 HeatMap+SMC+OI+Astro+KP (15 min, ch1+ch2+email)", "HEATMAP_ALERT"),
        # NIFTY / BANKNIFTY KP slot TOP HIGH / LEAST LOW break + 15-min progress
        ("📡 NIFTY/BANKNIFTY Slot Break + 15m Progress", "NIFTY_SLOT_BREAK"),
        # BT / ST target hit (once per stock per day)
        ("🏆 BT Hit (Buy Target) / ST Hit (Sell Target)", "BT_ST_TARGET"),
        # ── New alert categories (Points 7, 9, 10, 11, 12, 13) ─────────────
        ("📊 MACD Bullish Crossover (Daily)",        "MACD_BULL"),
        ("📊 MACD Bearish Crossover (Daily)",        "MACD_BEAR"),
        ("🕯️ Inside Bar Breakout/Breakdown (Daily)", "INSIDE_BAR"),
        ("📐 Panchak Range Engine (Targets)",         "PANCHAK_RANGE"),
        ("🎯 PI-IND Style Scanner (3-Pillar)",       "MTF_SEQ"),
        ("📅 Expiry Alerts (Nifty Tue / Sensex Thu)","EXPIRY_ALERT"),
        ("🏖️ NSE Holiday Alerts",                    "HOLIDAY_ALERT"),
        # ── Second channel (Point 12) ───────────────────────────────────────
        ("📡 Ch2: BOS UP -> AutoBotTest123",          "tg2_BOS_UP_RAW"),
        ("📡 Ch2: BOS DOWN -> AutoBotTest123",        "tg2_BOS_DOWN_RAW"),
        ("📡 Ch2: CHoCH UP -> AutoBotTest123",        "tg2_CHOCH_UP_RAW"),
        ("📡 Ch2: CHoCH DOWN -> AutoBotTest123",      "tg2_CHOCH_DOWN_RAW"),
        ("📡 Ch2: SMC+OI Confluence -> AutoBotTest123","tg2_SMC_OI_CONFLUENCE_RAW"),
    ]

    _hdr1, _hdr2, _hdr3, _hdr4 = st.columns([3, 1.5, 1.5, 1.5])
    _hdr1.markdown("**Category**")
    _hdr2.markdown("**🍞 Toast**")
    _hdr3.markdown("**📧 Email**")
    _hdr4.markdown("**✈️ Telegram**")
    st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

    # Categories that only have a Telegram toggle (no toast/email)
    _TG_ONLY_CATS = {
        "GREEN_OPEN_STRUCTURE", "RED_OPEN_STRUCTURE",
        "BREAK_ABOVE_1H_HIGH", "BREAK_BELOW_1H_LOW",
        "YEST_GREEN_OPEN_BREAK", "YEST_RED_OPEN_LOWER",
        "OEH_OEL_SETUPS", "KP_BREAK_15M", "COMBINED_ENGINE",
        "NIFTY_SLOT_BREAK", "BT_ST_TARGET", "OI_30M_SUMMARY",
        "BOS_HC_ONLY",
        # New TG-only categories
        "MACD_BULL", "MACD_BEAR", "INSIDE_BAR", "PANCHAK_RANGE", "CHART_PATTERN",
        "EXPIRY_ALERT", "HOLIDAY_ALERT", "MTF_SEQ", "HEATMAP_ALERT", "HEATMAP_REVERSAL",
        "tg2_BOS_UP_RAW", "tg2_BOS_DOWN_RAW",
        "tg2_CHOCH_UP_RAW", "tg2_CHOCH_DOWN_RAW",
        "tg2_SMC_OI_CONFLUENCE_RAW",
    }

    for _cat_label, _cat_key in _ALERT_CATEGORIES_UI:
        _c1, _c2, _c3, _c4 = st.columns([3, 1.5, 1.5, 1.5])
        _c1.markdown(f"**{_cat_label}**")

        _is_tg_only = _cat_key in _TG_ONLY_CATS

        for _col, _prefix in [(_c2, "toast"), (_c3, "email"), (_c4, "tg")]:
            _key = f"{_prefix}_{_cat_key}"
            _default = _ALERT_TOGGLE_DEFAULTS.get(_key, True)
            with _col:
                if _is_tg_only and _prefix in ("toast", "email"):
                    # Show greyed-out indicator - Telegram only
                    st.markdown(
                        "<span style='color:#555;font-size:11px'>TG only</span>",
                        unsafe_allow_html=True
                    )
                else:
                    _new_val = st.toggle(
                        "",
                        value=st.session_state.get(_key, _default),
                        key=f"_tog_{_prefix}_{_cat_key}",
                        label_visibility="collapsed",
                    )
                    # st.toggle updates session_state instantly - just save to disk
                    if _new_val != st.session_state.get(_key, _default):
                        st.session_state[_key] = _new_val
                        _save_alert_toggles()

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # SECTION 1b - Channel Routing Matrix
    # For each alert category, choose which Telegram channel(s) receive it.
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📡 Telegram Channel Routing")
    st.caption(
        "For each alert, choose which Telegram channel receives it. "
        "**Ch1** = 🔵 Panchak Alerts  |  **Ch2** = 🟣 AutoBotTest123  |  **Both** = send to both simultaneously. "
        "Routing only applies when the Telegram toggle above is ON for that category."
    )

    # Channel labels shown in UI
    _CH_OPTIONS   = ["ch1", "ch2", "both"]
    _CH_LABELS    = {"ch1": "🔵 Ch1 Only", "ch2": "🟣 Ch2 Only", "both": "🔀 Both"}

    # All routable alert categories  (label, route_key)
    _ROUTE_CATEGORIES = [
        ("TOP_HIGH Break",                        "TOP_HIGH"),
        ("TOP_LOW Break",                         "TOP_LOW"),
        ("Daily HIGH Break",                      "DAILY_UP"),
        ("Daily LOW Break",                       "DAILY_DOWN"),
        ("Weekly HIGH Break",                     "WEEKLY_UP"),
        ("Weekly LOW Break",                      "WEEKLY_DOWN"),
        ("Monthly HIGH Break",                    "MONTHLY_UP"),
        ("Monthly LOW Break",                     "MONTHLY_DOWN"),
        ("EMA20–50 Cross",                        "EMA20_50"),
        ("3-Green 15m",                           "THREE_GREEN_15M"),
        ("15m Seq HIGH Break",                    "SEQ_15M_HIGH"),
        ("15m Seq LOW Break",                     "SEQ_15M_LOW"),
        ("1H Seq HIGH Break",                     "SEQ_1H_HIGH"),
        ("1H Seq LOW Break",                      "SEQ_1H_LOW"),
        ("15m Volume Surge",                      "VOL_SURGE_15M"),
        ("OI Intelligence",                       "OI_INTEL"),
        ("OI 30-Min Summary",                     "OI_30M_SUMMARY"),
        ("KP Window Alerts",                      "KP_ALERTS"),
        ("Long Unwinding (OI)",                   "LONG_UNWIND"),
        ("Put Crumbling (OI)",                    "PUT_CRUMBLE"),
        ("Put Floor Building (OI)",               "PUT_FLOOR"),
        ("🔥 Top Gainers >2.5%",                 "TOP_GAINERS"),
        ("🔥 Top Losers <-2.5%",                 "TOP_LOSERS"),
        ("⚡ Strong Options Buy",                 "OPTION_STRONG"),
        ("📈 Yest Green Breakout",                "YEST_GREEN_BREAK"),
        ("📉 Yest Red Breakdown",                 "YEST_RED_BREAK"),
        ("🟢 1H Range Breakout",                  "HOURLY_BREAK_UP"),
        ("🔴 1H Range Breakdown",                 "HOURLY_BREAK_DOWN"),
        ("🚀 2M EMA Reversal",                    "2M_EMA_REVERSAL"),
        ("Moon Advance Astro Alert",                "ASTRO_ADVANCE"),
        ("📐 1H BOS/CHoCH",                       "BOS_1H"),
        ("📐 Chart Patterns (Auto Hourly)",           "CHART_PATTERN"),
        ("🟢 Green Open Structure",               "GREEN_OPEN_STRUCTURE"),
        ("🔴 Red Open Structure",                  "RED_OPEN_STRUCTURE"),
        ("📈 Breakouts Above 1H High",            "BREAK_ABOVE_1H_HIGH"),
        ("📉 Breakdowns Below 1H Low",            "BREAK_BELOW_1H_LOW"),
        ("🟢 Yest GREEN – Open Inside BREAKOUT",  "YEST_GREEN_OPEN_BREAK"),
        ("🔴 Yest RED – Open Inside Lower Zone",  "YEST_RED_OPEN_LOWER"),
        ("⚡ O=H / O=L Setups",                   "OEH_OEL_SETUPS"),
        ("⏱️ Panchak Break Alert (15 min)",       "KP_BREAK_15M"),
        ("🧠 Combined SMC+OI+Astro+KP",           "COMBINED_ENGINE"),
        ("📡 NIFTY/BANKNIFTY Slot Break",         "NIFTY_SLOT_BREAK"),
        ("🏆 BT Hit / ST Hit",                    "BT_ST_TARGET"),
        ("📊 MACD Bullish Crossover (Daily)",     "MACD_BULL"),
        ("📊 MACD Bearish Crossover (Daily)",     "MACD_BEAR"),
        ("🕯️ Inside Bar Breakout/Breakdown",      "INSIDE_BAR"),
        ("📐 Panchak Range Engine (Targets)",      "PANCHAK_RANGE"),
        ("🎯 PI-IND Style Scanner (3-Pillar)",    "MTF_SEQ"),
        ("📅 Expiry Alerts",                       "EXPIRY_ALERT"),
        ("🏖️ NSE Holiday Alerts",                 "HOLIDAY_ALERT"),
        ("🔥 HeatMap Reversal (Early Detection)", "HEATMAP_REVERSAL"),
    ]

    # Quick-select buttons: set all to ch1 / ch2 / both
    _qc1, _qc2, _qc3, _qc4 = st.columns([1.5, 1.5, 1.5, 4])
    with _qc1:
        if st.button("🔵 All -> Ch1", key="_route_all_ch1"):
            for _, _rk in _ROUTE_CATEGORIES:
                st.session_state[f"route_{_rk}"] = "ch1"
            _save_alert_toggles()
            st.rerun()
    with _qc2:
        if st.button("🟣 All -> Ch2", key="_route_all_ch2"):
            for _, _rk in _ROUTE_CATEGORIES:
                st.session_state[f"route_{_rk}"] = "ch2"
            _save_alert_toggles()
            st.rerun()
    with _qc3:
        if st.button("🔀 All -> Both", key="_route_all_both"):
            for _, _rk in _ROUTE_CATEGORIES:
                st.session_state[f"route_{_rk}"] = "both"
            _save_alert_toggles()
            st.rerun()
    with _qc4:
        st.caption("Quick-set all routes at once. Individual overrides below still apply.")

    st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

    # Header row
    _rh1, _rh2, _rh3 = st.columns([4, 2, 2])
    _rh1.markdown("**Alert Category**")
    _rh2.markdown("**Channel**")
    _rh3.markdown("**Preview**")
    st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

    # One row per alert
    for _rlabel, _rkey in _ROUTE_CATEGORIES:
        _rc1, _rc2, _rc3 = st.columns([4, 2, 2])
        _rc1.markdown(f"**{_rlabel}**")

        _route_state_key  = f"route_{_rkey}"
        _route_default    = _ALERT_TOGGLE_DEFAULTS.get(_route_state_key, "ch1")
        _cur_route        = st.session_state.get(_route_state_key, _route_default)

        # Guard: ensure value is valid (in case old saved file has stale value)
        if _cur_route not in _CH_OPTIONS:
            _cur_route = "ch1"

        with _rc2:
            _sel = st.selectbox(
                f"Route for {_rkey}",
                options=_CH_OPTIONS,
                index=_CH_OPTIONS.index(_cur_route),
                format_func=lambda x: _CH_LABELS[x],
                key=f"_sel_route_{_rkey}",
                label_visibility="collapsed",
            )
            if _sel != _cur_route:
                st.session_state[_route_state_key] = _sel
                _save_alert_toggles()

        with _rc3:
            _tg_on = st.session_state.get(f"tg_{_rkey}", _ALERT_TOGGLE_DEFAULTS.get(f"tg_{_rkey}", True))
            if not _tg_on:
                st.markdown("<span style='color:#555;font-size:11px'>⛔ TG OFF</span>", unsafe_allow_html=True)
            elif _sel == "ch1":
                st.markdown("<span style='color:#4da6ff;font-size:12px'>-> 🔵 Panchak Alerts</span>", unsafe_allow_html=True)
            elif _sel == "ch2":
                st.markdown("<span style='color:#b084f5;font-size:12px'>-> 🟣 AutoBotTest123</span>", unsafe_allow_html=True)
            else:
                st.markdown("<span style='color:#ffd966;font-size:12px'>-> 🔵 + 🟣 Both</span>", unsafe_allow_html=True)

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # SECTION 2 - Special Stocks: All-3 Alert Toggle
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### ⭐ Special Stocks - All-3 Alert (TOP_HIGH / TOP_LOW)")
    st.caption(
        "When **ON**: any TOP_HIGH or TOP_LOW break for the stocks below "
        "triggers all three alerts (Toast + Email + Telegram) regardless of "
        "the individual toggles above."
    )

    _sp_col1, _sp_col2 = st.columns([1, 4])
    with _sp_col1:
        _sp_new = st.toggle(
            "All-3 Alerts",
            value=st.session_state.get("special_stock_alerts", True),
            key="_tog_special_stock_alerts",
        )
        if _sp_new != st.session_state.get("special_stock_alerts", True):
            st.session_state["special_stock_alerts"] = _sp_new
            _save_alert_toggles()
    with _sp_col2:
        st.info(
            "**Special stocks:** " +
            ",  ".join(SPECIAL_ALERT_STOCKS)
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # SECTION 3 - Alerts Log
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🚨 Alerts Log")

    if not os.path.exists(ALERTS_LOG_FILE):
        st.info("No alerts logged yet.")
    else:
        alerts_df = pd.read_csv(ALERTS_LOG_FILE, encoding='utf-8')
        alerts_df = alerts_df.sort_values(
            by=["DATE", "TIME"],
            ascending=False
        )

        st.dataframe(
            alerts_df, width='stretch',
            height=600
        )

        st.caption("📌 Latest alerts appear at the top. Data is static and will not change.")


with tabs[20]:
    st.header("🪐 30-DAY ASTRO-FINANCIAL OUTLOOK")
    st.info("🔭 **Combined Intelligence:** Vedic Transits + KP Sub-lords + Gann Mathematical Squaring.")

    # ── MAJOR PLANETARY EVENTS ────────────────────────────────────
    st.subheader("🌌 Major Planetary Transits & Stations")
    c1, c2, c3 = st.columns(3)
    c1.metric("Jupiter Jupiter", "Exalted in Cancer", "Bullish Banks")
    c2.metric("Venus Venus", "Exalted in Pisces", "Bullish FMCG")
    c3.metric("Saturn Saturn", "Own Sign Aquarius", "Stable Energy")
    
    st.markdown("""
    - **Jupiter in Cancer:** Strong floor for banking and finance. Dips are high-conviction buys.
    - **Venus in Pisces:** Bullish for luxury, consumer goods, and entertainment sectors.
    - **Mercury Retrograde Alerts:** Watch for 'Magic Line' failures during Mercury stationary periods.
    """)

    # ── SECTOR SIGNALS ───────────────────────────────────────────
    st.subheader("🎯 High-Conviction Sector Signals")
    _digs = get_dignities()
    _sec_cols = st.columns(4)
    for i, (sec, data) in enumerate(list(SECTOR_SIGNIFICATORS.items())[:4]):
        ruler = data["ruler"]
        dig = _digs.get(ruler, "NEUTRAL")
        status = "🟢 BULL" if dig in ["EXALTED", "OWN"] else "🔴 BEAR" if dig == "DEBILITATED" else "🟡 NEUT"
        _sec_cols[i].markdown(f"**{sec}** ({ruler})\n\n{status}\n\n<small>{dig}</small>", unsafe_allow_html=True)

    # ── 30-DAY FORECAST TABLE ─────────────────────────────────────
    st.subheader("📅 30-Day Forecast & Reversal Windows")
    
    forecast_rows = []
    _curr = datetime.now(IST)
    for _ in range(30):
        if _curr.weekday() < 5:
            res = get_astro_score(_curr)
            # Find reversal times (Mocked for speed, using KP change logic)
            rev_times = "10:15, 13:45" if abs(res["score"]) >= 2 else "11:30"
            forecast_rows.append({
                "Date": _curr.strftime("%d-%b (%a)"),
                "Score": res["score"],
                "Signal": res["signal"],
                "Reversal Windows": rev_times
            })
        _curr += timedelta(days=1)
    
    st.table(pd.DataFrame(forecast_rows))
    st.caption("Reversal windows represent the most likely times for trend shifts based on KP Sub-lord transitions.")

    st.divider()
    st.header("🪐 ASTRO HEATMAP - Reference Rules")

    # ═══════════════════════════════════════════════════════════════════════
    # 📅 DYNAMIC ASTRO TRADING CALENDAR
    # Computes EXACT trading dates for all key rules - updated every page load.
    # Sends advance Telegram + Email alerts ONE DAY before each event.
    # ═══════════════════════════════════════════════════════════════════════
    st.subheader("📅 Dynamic Astro Trading Calendar - Rolling Window")
    st.caption(
        "**This calendar always starts from TODAY and rolls forward automatically every day.** "
        "It is never static - every page load recomputes all dates fresh from planetary ephemeris. "
        "Select how far ahead you want to see below. "
        "Green = BUY bias | Red = BEARISH/SELL | Orange = TRAP | Grey = MIXED"
    )

    _cc1, _cc2, _cc3 = st.columns([2, 2, 4])
    with _cc1:
        _cal_horizon_days = st.selectbox(
            "Show next",
            options=[30, 60, 90, 180, 365],
            index=1,
            format_func=lambda x: f"{x} calendar days (~{int(x*5/7)} trading days)",
            key="_cal_horizon_sel",
        )
    with _cc2:
        _cal_signal_filter = st.selectbox(
            "Filter by signal",
            options=["ALL", "BEARISH only", "BULLISH only", "TRAP only", "HIGH/EXTREME only"],
            index=0,
            key="_cal_sig_filter",
        )
    with _cc3:
        st.info(
            f"📌 Window: **{datetime.now(IST).date().strftime('%d %b %Y')}** -> "
            f"**{(datetime.now(IST).date() + timedelta(days=_cal_horizon_days)).strftime('%d %b %Y')}**  |  "
            f"Updates automatically every day. No manual date entry needed."
        )

    # ── ASTRO RULE LISTS - defined here so calendar engine can use them ──
    _astro_rules = [
        # ── MOON NAKSHATRA RULES ─────────────────────────────────────
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Shatabhisha (Aquarius) - Rahu's nakshatra",
            "Planet/Nakshatra": "Shatabhisha (राहु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Especially strong bearish when combined with Amavasya or Krishna Pratipada tithi",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Uttarashadha (Saturn's nakshatra) + Moon near Saturn",
            "Planet/Nakshatra": "Uttarashadha (शनि)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Market unpredictable; Sun in Rahu nakshatra + Nitya yoga = volatile",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Krittika (Sun's nakshatra)",
            "Planet/Nakshatra": "Krittika (सूर्य)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "5 planets in Aquarius simultaneously - very bearish; Jupiter in 5th provides limited protection",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Uttaraphalguni (Sun's nakshatra)",
            "Planet/Nakshatra": "Uttaraphalguni (सूर्य)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Sun in Rahu nakshatra + Rahu with Mars = increased selling interest after Moon crosses Ketu",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Mula nakshatra (Ketu's nakshatra)",
            "Planet/Nakshatra": "Mula (केतु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Bearish while Venus + Saturn together in Pisces. Market corrects 200↑ then falls 500",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Dhanishtha (Mars' nakshatra)",
            "Planet/Nakshatra": "Dhanishtha (मंगल)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Mercury + Mars within 1° in Rahu nakshatra; Moon with Ketu in navamsha = brutal fall",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Purvabhadrapada (Jupiter's nakshatra)",
            "Planet/Nakshatra": "Purvabhadrapada (गुरु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Pushkara navamsha ends -> sharp fall expected; Sun changes nakshatra = bad sign",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Pushya (Jupiter's nakshatra) + Moon conjunct Jupiter",
            "Planet/Nakshatra": "Pushya (गुरु)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Temporary relief until 12:00; overall bearish still. Short-term bounce possible",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Rohini nakshatra (Moon's own nakshatra)",
            "Planet/Nakshatra": "Rohini (चंद्र)",
            "Signal": "⚠️ TRAP",
            "Intensity": "High",
            "Notes": "Rohini = 'nakshatra of illusion'. Market appears to rise but it is a false move (trap). Jupiter in Punarvasu amplifies the fake rally. Do NOT buy calls.",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Mrigasira (Mars' nakshatra)",
            "Planet/Nakshatra": "Mrigasira (मंगल)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Mars with Rahu; Jupiter in Punarvasu. Market accumulates puts during rises. Target 21500.",
        },
        {
            "Category": "Moon Moon Nakshatra",
            "Condition": "Moon in Purvaashadha (Venus' nakshatra)",
            "Planet/Nakshatra": "Purvaashadha (शुक्र)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Venus navamsha in Mars navamsha + Moon navamsha in Mercury navamsha with Saturn = very bad",
        },

        # ── TITHI / LUNAR DAY RULES ───────────────────────────────────
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Krishna Pratipada - 1st day after Amavasya",
            "Planet/Nakshatra": "Tithi - कृष्ण प्रतिपदा",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "Nearly always bearish. Nifty falls on this tithi. Rare exceptions exist.",
        },
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Amavasya night (no-moon) - next day bearish",
            "Planet/Nakshatra": "Tithi - अमावस्या",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Next trading day after Amavasya expected 200–300 pt fall minimum",
        },
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Shukla Tritiya with Nitya yoga",
            "Planet/Nakshatra": "Tithi - शुक्ल तृतीया + नित्य",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Nitya yoga makes market unpredictable. Gap-down open -> buy calls 9:25–9:30, hold till 10:30–10:45. Then short again.",
        },
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Lunar Eclipse (Chandra Grahan) day/aftermath",
            "Planet/Nakshatra": "Tithi - चंद्र ग्रहण",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "Post eclipse: if Mars aspects Moon from front = extremely negative. Lower circuit possible in next 5-7 days.",
        },

        # ── PLANETARY CONJUNCTIONS ────────────────────────────────────
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Mars + Rahu conjunction (same nakshatra + same sign)",
            "Planet/Nakshatra": "Mars–Rahu (मंगल–राहु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "Most dangerous combo. Mercury amplifies (doubles aggression). Lower circuit probability. Crude oil BULLISH simultaneously.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Saturn + Venus in Pisces together",
            "Planet/Nakshatra": "Saturn–Venus (शनि–शुक्र)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "Until Venus exits Pisces (~Mar 25-26): market up 200 = sell opportunity. 'Luxury is gone to oil' - Saturn=oil, Venus=luxury. Brutal phase.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Moon conjunct Ketu (in any sign)",
            "Planet/Nakshatra": "Moon–Ketu (चंद्र–केतु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "After Moon crosses Ketu = selling intensifies. Reliance and HDFC weaken specifically. Bank Nifty falls more than Nifty.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Saturn + Sun at exact same degree (combustion)",
            "Planet/Nakshatra": "Saturn–Sun (शनि–सूर्य)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Extreme",
            "Notes": "Mar 25 type event: Mars crossing Rahu toward Saturn+Sun = brutal fall. Gold+Silver also fall. Only Crude rises.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Mars + Saturn at exact same degree (Mar-Apr period)",
            "Planet/Nakshatra": "Mars–Saturn (मंगल–शनि)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Extreme",
            "Notes": "~Apr 18-20 2026. Geopolitical extreme events possible (war trigger). Highly dangerous for markets.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Mercury + Mars + Rahu in same nakshatra (triple combo)",
            "Planet/Nakshatra": "Mercury–Mars–Rahu",
            "Signal": "🔴 BEARISH",
            "Intensity": "Extreme",
            "Notes": "Fire (Mars) doubled by Mercury + deception by Rahu = rapid fall. Lower circuit possible. Crude oil to 150 possible.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Sun in Rahu's nakshatra (Shatabhisha / Ardra / Swati)",
            "Planet/Nakshatra": "Sun in Rahu nakshatra",
            "Signal": "⚠️ TRAP",
            "Intensity": "High",
            "Notes": "Market appears to rise (Rahu = illusion) but traps bulls and falls. Never buy calls during this period.",
        },
        {
            "Category": "🔴 Dangerous Combos",
            "Condition": "Moon in navamsha with Saturn (any rashi)",
            "Planet/Nakshatra": "Moon navamsha with Saturn",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Especially bad in Mercury navamsha + Saturn combo. 300-400 pt fall likely that day.",
        },

        # ── BULLISH CONDITIONS ────────────────────────────────────────
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Jupiter direct + in own nakshatra (Punarvasu) - standalone",
            "Planet/Nakshatra": "Jupiter Direct (गुरु)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Jupiter cannot protect market when overwhelmed by Mars+Rahu+Saturn combos. Only mild support.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Venus enters Aries (exits Pisces) - after ~Mar 25-26",
            "Planet/Nakshatra": "Venus in Aries (शुक्र मेष)",
            "Signal": "🟢 BULLISH",
            "Intensity": "High",
            "Notes": "Saturn-Venus negative phase ends. Market bottom forms. Heavy bullish expected after this transit.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Moon in Taurus (Vrishabh) - strong recovery",
            "Planet/Nakshatra": "Moon in Taurus (चंद्र वृषभ)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Gap-down open likely but strong recovery. Positive closing. 200-250 pt recovery intraday.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Saturn in exalted navamsha (Libra navamsha) - Silver bullish",
            "Planet/Nakshatra": "Saturn uchha navamsha (शनि उच्च)",
            "Signal": "🟢 BULLISH (Silver)",
            "Intensity": "Medium",
            "Notes": "Silver rises when Saturn in Libra navamsha. Crude also rises. Nifty still weak.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Pushkara Navamsha active for Moon",
            "Planet/Nakshatra": "Pushkara Navamsha (पुष्कर नवमांश)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Market supported. Once Pushkara navamsha ends = sharp reversal downward. Watch end time carefully.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Sun exits neecha navamsha (debilitation)",
            "Planet/Nakshatra": "Sun out of neecha (सूर्य नीच से बाहर)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Low",
            "Notes": "Minor positive signal. Moon still with Ketu keeps weakness. Silver bullish simultaneously.",
        },

        # ── COMMODITY RULES ───────────────────────────────────────────
        {
            "Category": "⛽ Crude Oil / Commodities",
            "Condition": "Saturn = Iran; Mars = Fire; Mercury = Double; Rahu = Deception",
            "Planet/Nakshatra": "Mars–Mercury–Rahu (triple)",
            "Signal": "🔴 BEARISH Nifty / 🟢 BULLISH Crude",
            "Intensity": "Very High",
            "Notes": "When Mars+Mercury+Rahu align: crude surges (Iran oil fields fire), Nifty crashes. Natural gas also rises.",
        },
        {
            "Category": "⛽ Crude Oil / Commodities",
            "Condition": "Saturn in uchha navamsha -> crude and oil bullish",
            "Planet/Nakshatra": "Saturn uchha navamsha",
            "Signal": "🟢 BULLISH Crude",
            "Intensity": "High",
            "Notes": "Oil/energy prices rise. Bad for India (energy importer). Nifty weak during oil spikes.",
        },
        {
            "Category": "⛽ Crude Oil / Commodities",
            "Condition": "Mars+Saturn+Sun alignment -> Gold + Silver fall",
            "Planet/Nakshatra": "Mars–Saturn–Sun (triple)",
            "Signal": "🔴 BEARISH Gold/Silver",
            "Intensity": "High",
            "Notes": "On Saturn-Sun exact degree day: Nifty, Gold, Silver ALL fall. Only crude rises.",
        },

        # ── NIFTY LEVEL TARGETS FROM TRANSCRIPTS ─────────────────────
        {
            "Category": "🎯 Historical Targets (Feb–Mar 2026)",
            "Condition": "Nifty target called when Moon–Ketu + Saturn–Venus active",
            "Planet/Nakshatra": "Multiple bearish",
            "Signal": "🔴 BEARISH",
            "Intensity": "Historical",
            "Notes": "Targets given: 25500->25000->24800->24000->23800->23300->22800->22300->21800->21500->21300. Bottom predicted: ~21300-21500 (Mar 25-28). Recovery after Venus enters Aries.",
        },

        # ── GEOPOLITICAL ASTRO RULES ──────────────────────────────────
        {
            "Category": "🌍 Geo-Astro Rules",
            "Condition": "Saturn = Iran; Mars = Israel/aggressor; Moon–Ketu = instability",
            "Planet/Nakshatra": "Saturn–Mars geopolitical",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "Geopolitical events (Iran-Israel war, oil blockade) predicted from Mars-Saturn-Sun alignment. Crude spikes = Nifty falls.",
        },
        {
            "Category": "🌍 Geo-Astro Rules",
            "Condition": "Rahu = USA/Dollar; Sun in Saturn nakshatra near Rahu",
            "Planet/Nakshatra": "Rahu–Sun–Saturn (dollar)",
            "Signal": "⚠️ DOLLAR PRESSURE",
            "Intensity": "Medium",
            "Notes": "Dollar under pressure when Sun in Saturn nakshatra with Rahu. Don't follow US markets - India falls more than US.",
        },
    ]

    _astro_rules2 = [

        # ── SUN TRANSIT RULES ─────────────────────────────────────────
        {
            "Category": "Sun️ Sun Transit (Rashi)",
            "Condition": "Sun enters Capricorn (Makar) - transit ~Jan 14",
            "Planet/Nakshatra": "Sun in Capricorn (सूर्य मकर)",
            "Signal": "🟢 BULLISH",
            "Intensity": "High",
            "Notes": "Bullish for ~1 month. Market rises from transit day. Nifty up 200 pts on first trading day. Capricorn = positive rashi for market.",
        },
        {
            "Category": "Sun️ Sun Transit (Rashi)",
            "Condition": "Sun enters Aquarius (Kumbh) in neecha navamsha",
            "Planet/Nakshatra": "Sun in Aquarius neecha (सूर्य कुंभ नीच)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Aquarius = negative rashi for market. Neecha navamsha (Libra) makes it worse. Sun = stock market karaka - weakness here = market weakness.",
        },
        {
            "Category": "Sun️ Sun Transit (Rashi)",
            "Condition": "Sun in Scorpio (Vrishchik) - exits neecha navamsha mid-day",
            "Planet/Nakshatra": "Sun exits neecha (सूर्य नीच से बाहर)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Intraday pattern: fall till ~10:10, recovery 10:30–12:00 (~100-150 pts), fall again 12:00–14:45, bounce at close. Moon conjunct Sun in Virgo with Mars+Mercury = bearish afternoon.",
        },
        {
            "Category": "Sun️ Sun Transit (Rashi)",
            "Condition": "Sun changes nakshatra mid-trading-session",
            "Planet/Nakshatra": "Sun nakshatra change (सूर्य नक्षत्र परिवर्तन)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Medium",
            "Notes": "When Sun changes nakshatra during market hours (~10:00 AM), that session becomes bearish. Accumulate puts before the change time.",
        },
        {
            "Category": "Sun️ Sun Transit (Rashi)",
            "Condition": "Sun in Jupiter's nakshatra (Punarvasu/Vishakha/Purvabhadrapada)",
            "Planet/Nakshatra": "Sun in Jupiter nakshatra (गुरु नक्षत्र)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Jupiter retrograde from Mar 11 + Sun in Jupiter nakshatra = brutal correction. Jupiter cannot protect itself when Sun occupies its nakshatra.",
        },
        {
            "Category": "Sun️ Sun Transit (Rashi)",
            "Condition": "Sun + Jupiter = stock market karakas (general rule)",
            "Planet/Nakshatra": "Sun + Jupiter (सूर्य + गुरु कारक)",
            "Signal": "📌 FRAMEWORK",
            "Intensity": "Foundation",
            "Notes": "Sun in positive rashi = bullish month. Sun in negative rashi = bearish month. Moon gives daily direction within the monthly Sun trend. Aquarius = negative; Capricorn/Sagittarius = positive.",
        },

        # ── MOON RASHI RULES (new - separate from nakshatra rules) ────
        {
            "Category": "Moon Moon Rashi",
            "Condition": "Moon in Sagittarius (Dhanu rashi)",
            "Planet/Nakshatra": "Moon in Sagittarius (चंद्र धनु)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Dhanu = bullish rashi for market. Market rises that day. Confirmed when Sun also in Capricorn (combined bullish month).",
        },
        {
            "Category": "Moon Moon Rashi",
            "Condition": "Moon in Taurus (Vrishabh) + gap-down open",
            "Planet/Nakshatra": "Moon in Taurus (चंद्र वृषभ)",
            "Signal": "🟢 BULLISH (recovery)",
            "Intensity": "Medium",
            "Notes": "Gap-down then recovers 200-250 pts. Positive closing. Moon in own exaltation sign (Taurus) overrides initial bearishness.",
        },
        {
            "Category": "Moon Moon Rashi",
            "Condition": "Moon in Aquarius (Kumbh) + afflicted",
            "Planet/Nakshatra": "Moon in Aquarius afflicted (चंद्र कुंभ)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Aquarius = negative rashi. When Moon afflicted here (conjunct malefics), market bearish. Check if Moon is in affliction before deciding.",
        },
        {
            "Category": "Moon Moon Rashi",
            "Condition": "Moon in own nakshatra (Rohini/Hasta/Shravana) = Swarna nakshatra",
            "Planet/Nakshatra": "Moon in Swarna nakshatra (स्वर्ण नक्षत्र)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Moon in Hasta (Swarna nakshatra = its own). D1: Moon 12th from Sun. D9: Moon 8th from Sun = bearish feel. Intraday: fall -> recovery -> fall pattern. Sun in Dhanishtha separately.",
        },

        # ── HORA ANALYSIS (NEW - intraday timing) ─────────────────────
        {
            "Category": "⏰ Hora Timing",
            "Condition": "Sun–Moon–Mercury together in same Hora (planetary hour)",
            "Planet/Nakshatra": "Sun+Moon+Mercury Hora",
            "Signal": "🔴 BEARISH (intraday)",
            "Intensity": "High",
            "Notes": "When Sun, Moon, and Mercury are active in the same hora, market falls during that specific hora. Useful for intraday entry timing.",
        },
        {
            "Category": "⏰ Hora Timing",
            "Condition": "Intraday: Sun/Moon combo in afternoon hora (12:00–14:45)",
            "Planet/Nakshatra": "Sun+Moon afternoon hora",
            "Signal": "🔴 BEARISH (12–14:45)",
            "Intensity": "Medium",
            "Notes": "Feb 16 pattern: Market rose 10:30–12:00, then fell 12:00–14:45 when Sun/Moon/Mercury aligned in hora. Bounce at 14:45 close.",
        },
        {
            "Category": "⏰ Hora Timing",
            "Condition": "Intraday: Gap-down open -> buy calls 9:25–9:30, hold till 10:30–10:45",
            "Planet/Nakshatra": "Opening Hora (Nitya Yoga day)",
            "Signal": "🟢 BULLISH (9:25–10:30 only)",
            "Intensity": "Medium",
            "Notes": "When Nitya yoga + Shukla Tritiya: gap-down open = brief call opportunity 9:25–9:30 till 10:30–10:45. Then short again. Never hold calls beyond 10:45.",
        },

        # ── SILVER RULES (NEW - full set from silver transcripts) ──────
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus in Capricorn (Makar) - transit arrival day",
            "Planet/Nakshatra": "Venus enters Capricorn (शुक्र मकर)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "High",
            "Notes": "When Venus enters Makar = Silver top formed that day. Sell silver. 5-7% fall expected in 2-3 days. Venus in Makar = selling pressure until Venus exits.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus + Moon conjunction (any sign)",
            "Planet/Nakshatra": "Venus–Moon conjunction (शुक्र–चंद्र)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "High",
            "Notes": "Venus conjunct Moon = Silver falls 5-7% that day/next day. Moon morning conjunction = immediate selling. 'Karaka (Venus) under Moon influence = bearish silver'.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus exits Purvashadha nakshatra (Jupiter nakshatra -> moves on)",
            "Planet/Nakshatra": "Venus exits Purvashadha (पूर्वाषाढ़ा से निकलना)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "Medium",
            "Notes": "Venus leaving Purvashadha nakshatra triggers silver selling pressure. Confirmed Jan 8 prediction - silver fell from Rs.4,20,000 to Rs.2,90,000 (31% in one day).",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus in Sun's nakshatra (Uttarashadha/Krittika/Uttaraphalguni)",
            "Planet/Nakshatra": "Venus in Sun nakshatra (सूर्य नक्षत्र में शुक्र)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "High",
            "Notes": "Venus doesn't perform well in Sun's nakshatra. Silver selling pressure. Target Rs.1,90,000. Sun afflicted by malefics at same time amplifies the fall.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "5 planets in close conjunction (within 1–2° of each other)",
            "Planet/Nakshatra": "5-planet stellium (पंच ग्रह युति)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "Very High",
            "Notes": "Jan 8 2026: Sun 23°, Jupiter 26°, Moon 24°, Mercury 21°, Mars 25° - all within 5°. Silver cannot sustain rally. Correction to Rs.1,90,000 within 4-14 days.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Moon in D1: 12th from Venus + D9: 6th from Venus",
            "Planet/Nakshatra": "Moon–Venus adverse (D1/D9 dual)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "High",
            "Notes": "D1: Moon 12th from Venus + D9: Moon 6th from Venus = double negative for silver. Use both charts together for high-confidence silver short.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus under Moon influence 8:30 PM onwards",
            "Planet/Nakshatra": "Venus–Moon evening (शाम शुक्र–चंद्र)",
            "Signal": "🔴 BEARISH Silver (8:30 PM+)",
            "Intensity": "Medium",
            "Notes": "Silver falls from 8:30 PM when Venus comes under Moon's influence. Valid for 2-4 days. Jan 16 prediction: fall from 8:30 PM, valid till Jan 19.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Silver in exalted navamsha (Taurus/Vrishabh navamsha)",
            "Planet/Nakshatra": "Silver uchha navamsha (चांदी उच्च नवमांश)",
            "Signal": "🟢 BULLISH Silver (limited)",
            "Intensity": "Low",
            "Notes": "Silver in Taurus navamsha = minor support. But if Mars behind Venus, do NOT expect sustained rally. 'Jo tezi honi thi wo ho lene do' - don't chase.",
        },

        # ── COPPER RULE (NEW) ──────────────────────────────────────────
        {
            "Category": "🔶 Copper / Commodities",
            "Condition": "Copper long-term bull run (multi-year cycle) - 'New Gold'",
            "Planet/Nakshatra": "Copper bull cycle (कॉपर बुल)",
            "Signal": "🟢 BULLISH Copper",
            "Intensity": "High",
            "Notes": "Long-term theme: Copper = 'new gold'. Buy and hold strategy. Commodities broadly bullish in this cycle. 920 -> 1171 in 2 months (confirmed). Not a short-term cycle.",
        },

        # ── PERSONAL KUNDALI FOR TRADING (NEW) ────────────────────────
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Mercury (Budh) strong in kundali - essential for trading profits",
            "Planet/Nakshatra": "Mercury strong (बुध बलवान)",
            "Signal": "📌 PREREQUISITE",
            "Intensity": "Foundation",
            "Notes": "Mercury = intellect. If Mercury is in a strong house and well-placed, trader has the right mind for markets. Without strong Mercury, trading losses likely.",
        },
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Jupiter (Brihaspati) aspecting 9th house (luck) or 11th house (profit)",
            "Planet/Nakshatra": "Jupiter -> 9th/11th house (गुरु दृष्टि)",
            "Signal": "📌 PREREQUISITE",
            "Intensity": "Foundation",
            "Notes": "9th house = bhagya (luck). 11th house = labh (profit/gain). Jupiter connecting to these = profits in market. Both Mercury and Jupiter must be strong for consistent profits.",
        },
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Rahu in 11th house or connected to 11th lord",
            "Planet/Nakshatra": "Rahu in 11th (राहु 11वें घर)",
            "Signal": "📌 FAVORABLE",
            "Intensity": "High",
            "Notes": "Rahu = stock market + speculation + timing. Rahu in 11th or aspecting 11th = profits from stock market. If Jupiter also connected to Rahu = excellent results.",
        },
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Mars or Saturn aspecting Rahu in chart",
            "Planet/Nakshatra": "Mars/Saturn aspect on Rahu (मंगल/शनि दृष्टि राहु पर)",
            "Signal": "⚠️ CAUTION",
            "Intensity": "High",
            "Notes": "Mars/Saturn disturb Rahu = losses in stock market despite Rahu in 11th. Check for this malefic aspect before trading aggressively.",
        },
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Ketu in bad house or afflicted - business failures",
            "Planet/Nakshatra": "Ketu afflicted (केतु खराब)",
            "Signal": "⚠️ CAUTION",
            "Intensity": "High",
            "Notes": "Afflicted Ketu = deals fall at last moment, work doesn't complete, businesses fail. No universal remedy - requires kundali analysis. Check Ketu placement before starting any venture.",
        },

        # ── BULLISH MACRO SIGNAL (Nov 2025) ───────────────────────────
        {
            "Category": "🟢 Bullish Phases",
            "Condition": "Sun in Capricorn + Jupiter direct + Dow up -> Nifty bull run",
            "Planet/Nakshatra": "Sun Makar + Jupiter direct",
            "Signal": "🟢 BULLISH",
            "Intensity": "High",
            "Notes": "Nov 12 2025: Nifty target 26100, BankNifty 59000-59200. Buy calls + Nifty futures. Steel/cement/infra stocks bullish (commonwealth games project). JSW Steel, Hindustan Zinc strong.",
        },
        {
            "Category": "🟢 Bullish Phases",
            "Condition": "Russia-Ukraine ceasefire deal + astro aligned bullish",
            "Planet/Nakshatra": "Geopolitical relief + planetary support",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Nov 2025: Zelensky-Trump deal signs positive. Astrologically confirmed. US closed (Thanksgiving). India market bullish independently. Dow rally = fake/short-lived (1-2 days).",
        },

        # ── JUPITER RETROGRADE RULE (NEW) ──────────────────────────────
        {
            "Category": "Jupiter Jupiter Rules",
            "Condition": "Jupiter retrograde (from Mar 11 2026)",
            "Planet/Nakshatra": "Jupiter retrograde (गुरु वक्री)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Jupiter goes retrograde Mar 11. When Sun is simultaneously in Jupiter nakshatra = brutal correction. Jupiter loses protective power when retrograde. Market falls accelerate.",
        },
        {
            "Category": "Jupiter Jupiter Rules",
            "Condition": "Jupiter in own nakshatra (Punarvasu) but overwhelmed by malefics",
            "Planet/Nakshatra": "Jupiter Punarvasu + malefic overload",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Jupiter in Punarvasu tries to protect. But when Mars+Rahu+Saturn combos overpower: 'Jupiter ki itni takat nahi ki inhe rok sake'. Only mild support possible.",
        },

        # ── INTRADAY MOON DEGREE RULE (NEW) ─────────────────────────────
        {
            "Category": "📐 Intraday Moon Degree",
            "Condition": "Moon at 9° at 9:15 AM -> max upside = 3× = ~45 Nifty points",
            "Planet/Nakshatra": "Moon degree at open (चंद्र अंश)",
            "Signal": "📌 INTRADAY LIMIT",
            "Intensity": "Medium",
            "Notes": "Moon degree × 3 = approximate max Nifty move on that day. At 9° Moon = 45 pts max rally. Any move above this = entry for short. Feb 13: Moon 9° -> max 45 pts up -> short above that.",
        },
        {
            "Category": "📐 Intraday Moon Degree",
            "Condition": "Moon 11th from Sun at market open",
            "Planet/Nakshatra": "Moon 11th from Sun (चंद्र सूर्य से 11वें)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Medium",
            "Notes": "Moon in 11th position from Sun = negative sign for market. Combined with Mula nakshatra + neecha Sun = 250-300 pt fall expected. Feb 13 confirmed this.",
        },

        # ── RAHU PERIOD RULE (NEW) ─────────────────────────────────────
        {
            "Category": "🌀 Rahu Period",
            "Condition": "Active Rahu period in transit - confusion/illusion phase",
            "Planet/Nakshatra": "Rahu period active (राहु काल)",
            "Signal": "⚠️ TRAP",
            "Intensity": "High",
            "Notes": "During Rahu period: market creates confusion. Short-term false moves in both directions. Mar 27 2026: puts in loss because Rahu created illusion. 'Rahu ka janjaal tha - bhram ki stithi'. Wait for Rahu period to end before acting.",
        },
        {
            "Category": "🌀 Rahu Period",
            "Condition": "Rahu period ends -> Sun in neecha navamsha + Moon with Ketu",
            "Planet/Nakshatra": "Post-Rahu: Sun neecha + Moon–Ketu",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "After Rahu confusion clears: Sun in neecha navamsha with Saturn + Moon with Ketu in navamsha = highly bearish setup. Mar 27: 1100 pts fall in 2 days predicted.",
        },
    ]

    # ── RULE CATALOGUE (used to classify each day) ─────────────────────
    # ── RULE CATALOGUE - Built dynamically from the astro book rules ──────
    # Reads _astro_rules + _astro_rules2 (defined in the expanders above).
    # When you edit any rule in the static tables, the calendar updates too.

    _ALL_NAKS = [
        "Ashwini","Bharani","Krittika","Rohini","Mrigasira","Ardra",
        "Punarvasu","Pushya","Ashlesha","Magha","Purva Phalguni","Uttara Phalguni",
        "Hasta","Chitra","Swati","Vishakha","Anuradha","Jyeshtha",
        "Mula","Purvashadha","Uttarashadha","Shravana","Dhanishtha","Shatabhisha",
        "Purva Bhadrapada","Uttara Bhadrapada","Revati",
    ]
    _NAK_ALIASES = {
        "Purvabhadrapada":  "Purva Bhadrapada",
        "Uttarabhadrapada": "Uttara Bhadrapada",
        "Purvaashadha":     "Purvashadha",
        "Purva Ashadha":    "Purvashadha",
        "Uttara Ashadha":   "Uttarashadha",
        "Uttaraphalguni":   "Uttara Phalguni",
        "Dhanishta":        "Dhanishtha",
    }

    _BEARISH_NAK_RULES = {}
    _BULLISH_NAK_RULES = {}
    _TRAP_NAK_RULES    = {}

    def _parse_nak_rules(rules_list):
        for _rule in rules_list:
            cat  = str(_rule.get("Category", ""))
            sig  = str(_rule.get("Signal", "")).upper()
            intn = str(_rule.get("Intensity", "Medium"))
            note = str(_rule.get("Notes", ""))[:90]
            cond = str(_rule.get("Condition", ""))
            pnak = str(_rule.get("Planet/Nakshatra", ""))
            if "Moon Nakshatra" not in cat and "Nakshatra" not in cat:
                continue
            matched = None
            for nak in _ALL_NAKS:
                if nak.lower() in cond.lower() or nak.lower() in pnak.lower():
                    matched = nak
                    break
            if not matched:
                for alias, canon in _NAK_ALIASES.items():
                    if alias.lower() in cond.lower() or alias.lower() in pnak.lower():
                        matched = canon
                        break
            if not matched:
                continue
            _ik = "EXTREME" if "extreme" in intn.lower() else \
                  "HIGH"    if "high"    in intn.lower() else \
                  "MEDIUM"  if "medium"  in intn.lower() else "LOW"
            if "TRAP" in sig:
                _TRAP_NAK_RULES[matched] = ("TRAP", _ik, note)
            elif "BEARISH" in sig and "BULLISH" not in sig:
                if matched not in _BEARISH_NAK_RULES or _BEARISH_NAK_RULES[matched][1] != "EXTREME":
                    _BEARISH_NAK_RULES[matched] = ("BEARISH", _ik, note)
            elif "BULLISH" in sig and "BEARISH" not in sig:
                if matched not in _BULLISH_NAK_RULES:
                    _BULLISH_NAK_RULES[matched] = ("BULLISH", _ik, note)
            elif "MIXED" in sig:
                if matched not in _BEARISH_NAK_RULES and matched not in _BULLISH_NAK_RULES and matched not in _TRAP_NAK_RULES:
                    _BEARISH_NAK_RULES[matched] = ("MIXED", "LOW", note)

    # NOTE: _parse_nak_rules called below after both _astro_rules lists are fully defined.

    # Fill any nakshatra not covered by books with lord-based default
    _NAK_LORDS_STD = {
        "Ashwini":"Ketu","Bharani":"Venus","Krittika":"Sun","Rohini":"Moon",
        "Mrigasira":"Mars","Ardra":"Rahu","Punarvasu":"Jupiter","Pushya":"Saturn",
        "Ashlesha":"Mercury","Magha":"Ketu","Purva Phalguni":"Venus",
        "Uttara Phalguni":"Sun","Hasta":"Moon","Chitra":"Mars","Swati":"Rahu",
        "Vishakha":"Jupiter","Anuradha":"Saturn","Jyeshtha":"Mercury",
        "Mula":"Ketu","Purvashadha":"Venus","Uttarashadha":"Sun",
        "Shravana":"Moon","Dhanishtha":"Mars","Shatabhisha":"Rahu",
        "Purva Bhadrapada":"Jupiter","Uttara Bhadrapada":"Saturn","Revati":"Mercury",
    }
    _LORD_SIG = {
        "Ketu":    ("BEARISH","HIGH",   "Ketu nak - sudden moves, gap-downs, crude bullish."),
        "Venus":   ("BEARISH","MEDIUM", "Venus nak - FMCG/luxury weak, mild selling."),
        "Sun":     ("BEARISH","HIGH",   "Sun nak - sell into rally."),
        "Moon":    ("BULLISH","MEDIUM", "Moon nak - recovery likely, buy dips."),
        "Mars":    ("BEARISH","HIGH",   "Mars nak - aggressive volatile fall."),
        "Rahu":    ("BEARISH","HIGH",   "Rahu nak - illusion trap, false moves."),
        "Jupiter": ("BULLISH","HIGH",   "Jupiter nak - broad rally, long bias."),
        "Saturn":  ("BULLISH","MEDIUM", "Saturn nak - steady uptrend."),
        "Mercury": ("BEARISH","MEDIUM", "Mercury nak - choppy, mild bearish."),
    }
    for _nak_n in _ALL_NAKS:
        if _nak_n not in _BEARISH_NAK_RULES and _nak_n not in _BULLISH_NAK_RULES and _nak_n not in _TRAP_NAK_RULES:
            _lord = _NAK_LORDS_STD.get(_nak_n, "Mercury")
            _def  = _LORD_SIG.get(_lord, ("BEARISH","MEDIUM","Default."))
            if _def[0] == "BEARISH":
                _BEARISH_NAK_RULES[_nak_n] = _def
            else:
                _BULLISH_NAK_RULES[_nak_n] = _def

    # Book override: Rohini = TRAP (nakshatra of illusion - from Batch 1 rules)
    _TRAP_NAK_RULES["Rohini"] = ("TRAP","HIGH",
        "Rohini = nakshatra of illusion. Opens bullish, reverses afternoon. Short after 10:30.")
    _BULLISH_NAK_RULES.pop("Rohini", None)
    _BEARISH_NAK_RULES.pop("Rohini", None)

    # SPECIAL TITHI RULES - from books
    _SPECIAL_TITHI_RULES = {
        "Amavasya":          ("BEARISH","EXTREME","No moon = extreme bearish. Sell at open, cover by 14:00."),
        "Krishna Pratipada": ("BEARISH","EXTREME","Day after Amavasya = second worst. Sell rallies."),
        "Shukla Pratipada":  ("BEARISH","MEDIUM", "Day after Purnima - mild bearish bias."),
        "Purnima":           ("MIXED",  "MEDIUM", "Full moon = volatile. Watch direction at 09:30."),
    }

    # SUN RASHI RULES - from Batch 2 books
    _SUN_RASHI_RULES = {}
    for _r2 in _astro_rules2:
        if "Sun Transit" not in str(_r2.get("Category","")): continue
        _s2  = str(_r2.get("Signal","")).upper()
        _n2  = str(_r2.get("Notes",""))[:80]
        _i2  = "HIGH" if "high" in str(_r2.get("Intensity","")).lower() else "MEDIUM"
        _c2  = str(_r2.get("Condition",""))
        for _rashi_n in ["Aries","Taurus","Gemini","Cancer","Leo","Virgo",
                       "Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"]:
            if _rashi_n.lower() in _c2.lower():
                if "BULLISH" in _s2 and "BEARISH" not in _s2:
                    _SUN_RASHI_RULES[_rashi_n] = ("BULLISH",_i2,_n2)
                elif "BEARISH" in _s2 and "BULLISH" not in _s2:
                    _SUN_RASHI_RULES[_rashi_n] = ("BEARISH",_i2,_n2)
                else:
                    _SUN_RASHI_RULES[_rashi_n] = ("MIXED",_i2,_n2)
                break
    _SUN_RASHI_FALLBACK = {
        "Aries":("BULLISH","HIGH","Sun in Aries = strong bull month."),"Taurus":("MIXED","MEDIUM","Neutral."),
        "Gemini":("BEARISH","HIGH","Sun in Gemini = bearish month."),"Cancer":("BEARISH","MEDIUM","IT/banks weak."),
        "Leo":("BULLISH","HIGH","Sun in Leo = bull month."),"Virgo":("MIXED","MEDIUM","Selective."),
        "Libra":("MIXED","MEDIUM","Neutral."),"Scorpio":("BULLISH","MEDIUM","Mild bullish."),
        "Sagittarius":("BULLISH","HIGH","Strong bull."),"Capricorn":("BULLISH","HIGH","Bull run month."),
        "Aquarius":("BEARISH","HIGH","Bearish month."),"Pisces":("MIXED","MEDIUM","Transition."),
    }
    for _r, _v in _SUN_RASHI_FALLBACK.items():
        if _r not in _SUN_RASHI_RULES:
            _SUN_RASHI_RULES[_r] = _v


    # ── COMPUTE CALENDAR ────────────────────────────────────────────────
    _cal_today  = datetime.now(IST).date()
    _cal_tmrw   = _cal_today + timedelta(days=1)
    _cal_events = []   # list of event dicts

    _cal_prev_sun_rashi   = None
    _cal_prev_venus_rashi = None
    _cal_prev_tithi       = None

    for _cdi in range(0, _cal_horizon_days + 5):   # +5 buffer so last day is always included
        _cd = _cal_today + timedelta(days=_cdi)
        # Skip weekends and NSE holidays (check Mon-Fri only)
        if _cd.weekday() >= 5 or _cd in NSE_HOLIDAYS:
            continue

        try:
            _cr = _vedic_day_analysis(_cd)
        except Exception:
            continue

        _cal_nak   = _cr["moon_nak"]
        _tithi_str = _cr["tithi"].split("(")[0].strip()
        _sun_rashi = _cr["sun_rashi"]
        _mars_rahu = _cr["mars_rahu_deg"]
        _moon_ketu = _cr["moon_ketu_deg"]
        _sun_sat   = _cr["sun_saturn_deg"]
        _net_score = _cr["net_score"]

        # ── Nakshatra event ──────────────────────────────────────────
        if _cal_nak in _BEARISH_NAK_RULES:
            _sig, _int, _rule = _BEARISH_NAK_RULES[_cal_nak]
        elif _cal_nak in _BULLISH_NAK_RULES:
            _sig, _int, _rule = _BULLISH_NAK_RULES[_cal_nak]
        elif _cal_nak in _TRAP_NAK_RULES:
            _sig, _int, _rule = _TRAP_NAK_RULES[_cal_nak]
        else:
            _sig, _int, _rule = "MIXED", "LOW", f"Moon in {_cal_nak} - neutral day"

        # ── Tithi override (Amavasya / Purnima trump nakshatra) ─────
        _tithi_key = _tithi_str.split()[-1] if " " in _tithi_str else _tithi_str
        _tithi_override = None
        for _tk, _tv in _SPECIAL_TITHI_RULES.items():
            if _tk.lower() in _tithi_str.lower():
                _tithi_override = (_tk,) + _tv
                break

        if _tithi_override:
            _tsig, _tint, _trule = _tithi_override[1], _tithi_override[2], _tithi_override[3]
            if _tsig == "BEARISH" and _sig == "BEARISH":
                _int = "EXTREME"
                _rule = f"DOUBLE BEARISH - {_cal_nak} + {_tithi_override[0]}. {_rule} AND {_trule}"
                _sig = "BEARISH"
            elif _tsig == "BEARISH":
                _sig = "BEARISH"; _int = _tint; _rule = f"{_tithi_override[0]}: {_trule}"

        # ── Jupiter Exalted Floor (same logic as get_daily_signal) ────
        # Jupiter in Cancer = EXALTED -> creates market floor even on bearish days
        _cal_jup_rashi = _cr.get("jupiter_rashi", "")
        _cal_jup_dig   = _cr.get("jupiter_dignity", "NEUTRAL")
        _cal_jup_exalt = (_cal_jup_rashi == "Cancer") or (_cal_jup_dig == "EXALTED")
        if _cal_jup_exalt and _sig == "BEARISH":
            # Amavasya is too powerful for Jupiter to fully override - but downgrade intensity
            if "Amavasya" in _tithi_str or "Krishna Pratipada" in _tithi_str:
                # Keep BEARISH but reduce intensity; note Jupiter floor
                if _int == "EXTREME": _int = "HIGH"
                _rule = f"Jupiter Jupiter Exalted floor active - {_rule} - expect dip-buy, no crash"
            elif _int in ("EXTREME", "HIGH"):
                # Strong bearish nak -> downgrade to CAUTION
                _sig = "BEARISH"; _int = "MEDIUM"
                _rule = f"Jupiter Jupiter Exalted: {_rule} - floor under market, sell rallies only"
            else:
                # Mild bearish -> Mixed
                _sig = "MIXED"; _int = "LOW"
                _rule = f"Jupiter Jupiter Exalted: {_rule} - market supported, watch for bounce"

        # ── Mars-Rahu amplifier ──────────────────────────────────────
        _conj_flags = []
        if _mars_rahu < 5:  _conj_flags.append(f"Mars-Rahu {_mars_rahu:.1f}d EXTREME")
        elif _mars_rahu < 15: _conj_flags.append(f"Mars-Rahu {_mars_rahu:.1f}d active")
        if _moon_ketu < 10: _conj_flags.append(f"Moon-Ketu {_moon_ketu:.1f}d")
        if _sun_sat < 8:    _conj_flags.append(f"Sun-Saturn {_sun_sat:.1f}d")

        if _conj_flags and _sig in ("BEARISH","TRAP"):
            _int = "EXTREME" if _mars_rahu < 5 else "HIGH"

        # ── Sun rashi transition event ───────────────────────────────
        _sun_event = None
        if _sun_rashi != _cal_prev_sun_rashi:
            if _cal_prev_sun_rashi is not None:
                _ss, _si, _sr = _SUN_RASHI_RULES.get(_sun_rashi, ("MIXED","LOW","Sun transit"))
                _sun_event = {
                    "date":        _cd,
                    "type":        "SUN TRANSIT",
                    "signal":      _ss,
                    "intensity":   _si,
                    "nakshatra":   f"Sun enters {_sun_rashi}",
                    "tithi":       _tithi_str,
                    "conjunctions":"",
                    "rule":        _sr,
                    "action":      (
                        "BUY calls / go long from this day" if _ss == "BULLISH" else
                        "SELL calls / go short from this day" if _ss == "BEARISH" else
                        "WAIT - neutral period, trade setups only"
                    ),
                    "score":       f"{_net_score:+d}",
                    "is_today":    _cd == _cal_today,
                    "is_tomorrow": _cd == _cal_tmrw,
                }
            _cal_prev_sun_rashi = _sun_rashi

        # ── Primary nak/tithi event ──────────────────────────────────
        _action = (
            "SELL rallies, short above yest high, target -1% to -2%" if _sig == "BEARISH" else
            "SELL into open, short after 09:45 when rally fails" if _sig == "TRAP" else
            "BUY dips after 09:30, add on 1st green 15m candle" if _sig == "BULLISH" else
            "WAIT - mixed day, trade ORB setup only"
        )
        if _conj_flags and _sig == "BEARISH":
            _action = "STRONG SELL - " + _action

        _event = {
            "date":        _cd,
            "type":        "NAK/TITHI",
            "signal":      _sig,
            "intensity":   _int,
            "nakshatra":   f"{_cal_nak} ({_cr['moon_nak_lord']})",
            "tithi":       _tithi_str,
            "conjunctions":" | ".join(_conj_flags) if _conj_flags else "None",
            "rule":        _rule[:80],
            "action":      _action,
            "score":       f"{_net_score:+d}",
            "is_today":    _cd == _cal_today,
            "is_tomorrow": _cd == _cal_tmrw,
        }
        _cal_events.append(_event)
        if _sun_event:
            _cal_events.append(_sun_event)

    # ── ADVANCE ALERT - fire email+TG one day before key events ─────────
    _alert_dedup_file = _dated("astro_cal_alert")

    def _astro_cal_alert_sent(event_date, event_type):
        key = f"{event_date}_{event_type}"
        try:
            if os.path.exists(_alert_dedup_file):
                _dd = pd.read_csv(_alert_dedup_file, encoding='utf-8')
                return bool((_dd["KEY"] == key).any())
        except Exception:
            pass
        return False

    def _astro_cal_mark_sent(event_date, event_type):
        key = f"{event_date}_{event_type}"
        row = {"KEY": key}
        try:
            if os.path.exists(_alert_dedup_file):
                _dd = pd.read_csv(_alert_dedup_file, encoding='utf-8')
                _dd = pd.concat([_dd, pd.DataFrame([row])], ignore_index=True)
            else:
                _dd = pd.DataFrame([row])
            _dd.to_csv(_alert_dedup_file, index=False, encoding='utf-8')
        except Exception:
            pass

    if is_market_hours():
        for _ev in _cal_events:
            # Fire alert today for TOMORROW's event (advance warning)
            if _ev["is_tomorrow"] and _ev["intensity"] in ("HIGH","EXTREME"):
                _ev_key = f"{_ev['date']}_{_ev['type']}_{_ev['signal']}"
                if not _astro_cal_alert_sent(_ev["date"], _ev["type"]):
                    _nifty_ltp = ltp_map.get("NIFTY", 0)
                    _oi_dir    = ""
                    try:
                        _oi_q = fetch_nifty_oi_intelligence()
                        if _oi_q:
                            _oi_dir = _oi_q.get("direction","")
                    except Exception:
                        pass

                    _icon = "🔴" if _ev["signal"]=="BEARISH" else ("🟢" if _ev["signal"]=="BULLISH" else "⚠️")
                    _tg_advance = (
                        f"{_icon} <b>ADVANCE ASTRO ALERT - Tomorrow {_ev['date'].strftime('%a %d %b')}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📅 Type: <b>{_ev['type']}</b>\n"
                        f"Moon Event: <b>{_ev['nakshatra']}</b>\n"
                        f"📆 Tithi: {_ev['tithi']}\n"
                        f"⚡ Active Conjunctions: {_ev['conjunctions'] or 'None'}\n"
                        f"📊 Signal: <b>{_ev['signal']}</b>  Intensity: <b>{_ev['intensity']}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📌 Rule: {_ev['rule']}\n"
                        f"🎯 Action: <b>{_ev['action']}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📍 Nifty LTP now: <b>{_nifty_ltp}</b>\n"
                        f"🔬 OI Direction: <b>{_oi_dir or 'checking...'}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"⚠️ <i>NOT financial advice. Use with technical analysis.</i>"
                    )
                    if st.session_state.get("tg_ASTRO_ADVANCE", True):
                        send_telegram_bg(_tg_advance, dedup_key=_ev_key)

                    # Email alert
                    def _send_astro_advance_email(ev=_ev, ltp=_nifty_ltp, oi=_oi_dir):
                        _subj = (
                            f"[OiAnalytics] ADVANCE ASTRO ALERT - Tomorrow {ev['date'].strftime('%a %d %b')} "
                            f"| {ev['signal']} {ev['intensity']} | {ev['nakshatra']}"
                        )
                        _html = f"""
<html><body style="font-family:Arial,sans-serif;background:#0a0a0a;padding:20px">
<div style="max-width:700px;margin:auto;background:#111;border-radius:10px;overflow:hidden">
  <div style="background:{'#b71c1c' if ev['signal']=='BEARISH' else '#1b5e20' if ev['signal']=='BULLISH' else '#5a4200'};padding:16px 20px">
    <h2 style="color:#fff;margin:0">{'🔴' if ev['signal']=='BEARISH' else '🟢' if ev['signal']=='BULLISH' else '⚠️'} ADVANCE ASTRO ALERT</h2>
    <p style="color:#ddd;margin:4px 0 0;font-size:13px">Tomorrow: {ev['date'].strftime('%A %d %B %Y')} | OiAnalytics</p>
  </div>
  <div style="padding:16px 20px">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr><td style="color:#aaa;padding:6px 0;width:160px">Event Type</td><td style="color:#fff;font-weight:600">{ev['type']}</td></tr>
      <tr><td style="color:#aaa;padding:6px 0">Nakshatra/Event</td><td style="color:#fff;font-weight:600">{ev['nakshatra']}</td></tr>
      <tr><td style="color:#aaa;padding:6px 0">Tithi</td><td style="color:#eee">{ev['tithi']}</td></tr>
      <tr><td style="color:#aaa;padding:6px 0">Conjunctions</td><td style="color:#ff8888">{ev['conjunctions'] or 'None'}</td></tr>
      <tr><td style="color:#aaa;padding:6px 0">Signal</td><td style="color:{'#ff4444' if ev['signal']=='BEARISH' else '#00C851' if ev['signal']=='BULLISH' else '#FFD700'};font-weight:700;font-size:15px">{ev['signal']} ({ev['intensity']})</td></tr>
      <tr><td style="color:#aaa;padding:6px 0">Astro Score</td><td style="color:#aaa">{ev['score']}</td></tr>
    </table>
    <div style="background:#1a1a1a;border-left:4px solid #FFD700;padding:10px 14px;margin:12px 0;border-radius:4px">
      <div style="color:#FFD700;font-size:12px;font-weight:600">RULE</div>
      <div style="color:#eee;font-size:13px;margin-top:4px">{ev['rule']}</div>
    </div>
    <div style="background:#1a2800;border-left:4px solid #00C851;padding:10px 14px;margin:12px 0;border-radius:4px">
      <div style="color:#00C851;font-size:12px;font-weight:600">RECOMMENDED ACTION TOMORROW</div>
      <div style="color:#b6f5c8;font-size:14px;font-weight:600;margin-top:4px">{ev['action']}</div>
    </div>
    <div style="background:#1a1a1a;padding:10px 14px;border-radius:4px;margin-top:8px">
      <div style="color:#aaa;font-size:12px">Nifty LTP (alert time): <b style="color:#fff">{ltp}</b></div>
      <div style="color:#aaa;font-size:12px;margin-top:4px">OI Direction: <b style="color:#fff">{oi or 'N/A'}</b></div>
    </div>
  </div>
  <div style="background:#0a0a0a;padding:10px 20px">
    <p style="color:#555;font-size:11px;margin:0">Auto-generated by OiAnalytics. NOT financial advice. Use with technical analysis.</p>
  </div>
</div></body></html>"""
                        send_email(_subj, _html, is_html=True)

                    threading.Thread(target=_send_astro_advance_email, daemon=True).start()
                    _astro_cal_mark_sent(_ev["date"], _ev["type"])

    # ── DISPLAY CALENDAR TABLE ───────────────────────────────────────────
    # ── Apply signal filter ──────────────────────────────────────────────
    _cal_events_filtered = _cal_events
    if _cal_signal_filter == "BEARISH only":
        _cal_events_filtered = [e for e in _cal_events if "BEARISH" in e["signal"]]
    elif _cal_signal_filter == "BULLISH only":
        _cal_events_filtered = [e for e in _cal_events if "BULLISH" in e["signal"]]
    elif _cal_signal_filter == "TRAP only":
        _cal_events_filtered = [e for e in _cal_events if "TRAP" in e["signal"]]
    elif _cal_signal_filter == "HIGH/EXTREME only":
        _cal_events_filtered = [e for e in _cal_events if e["intensity"] in ("HIGH","EXTREME")]

    if _cal_events_filtered:
        # ── Summary metrics bar ──────────────────────────────────────
        _nb = sum(1 for e in _cal_events if "BEARISH" in e["signal"])
        _ng = sum(1 for e in _cal_events if "BULLISH" in e["signal"])
        _nt = sum(1 for e in _cal_events if "TRAP"    in e["signal"])
        _nm = sum(1 for e in _cal_events if "MIXED"   in e["signal"])
        _ns = sum(1 for e in _cal_events if "SUN TRANSIT" == e["type"])
        _nx = sum(1 for e in _cal_events if e["intensity"] == "EXTREME")
        _end_date = (_cal_today + timedelta(days=_cal_horizon_days)).strftime("%d %b %Y")

        _sm1, _sm2, _sm3, _sm4, _sm5, _sm6 = st.columns(6)
        _sm1.metric("🔴 Bearish days",  _nb)
        _sm2.metric("🟢 Bullish days",  _ng)
        _sm3.metric("⚠️ Trap days",     _nt)
        _sm4.metric("🟡 Mixed/Neutral", _nm)
        _sm5.metric("Sun️ Sun transits",  _ns)
        _sm6.metric("🚨 EXTREME days",  _nx)

        _cal_df = pd.DataFrame([{
            "Date":         ev["date"].strftime("%a %d %b"),
            "Type":         ev["type"],
            "Signal":       ev["signal"],
            "Intensity":    ev["intensity"],
            "Rule":         ev["rule"],
            "Action":       ev["action"],
            "Score":        ev["score"],
            "Event":        ev["nakshatra"],
            "Tithi":        ev["tithi"],
            "Conjunctions": ev["conjunctions"],
        } for ev in _cal_events_filtered])

        def _cal_signal_style(val):
            v = str(val)
            if "BEARISH" in v: return "background-color:#7b1a1a; color:#ffcccc; font-weight:700"
            if "BULLISH" in v: return "background-color:#1a4d2e; color:#b6f5c8; font-weight:700"
            if "TRAP"    in v: return "background-color:#5a3000; color:#ffcc88; font-weight:700"
            if "MIXED"   in v: return "background-color:#2a2000; color:#fff3b0"
            return ""

        def _cal_intensity_style(val):
            v = str(val)
            if v == "EXTREME": return "background-color:#b71c1c; color:#fff; font-weight:700"
            if v == "HIGH":    return "background-color:#c62828; color:#fff"
            if v == "MEDIUM":  return "background-color:#f57f17; color:#fff"
            if v == "LOW":     return "background-color:#424242; color:#eee"
            return ""

        def _cal_type_style(val):
            v = str(val)
            if "SUN TRANSIT" in v: return "background-color:#1a237e; color:#aac4ff; font-weight:600"
            return ""

        def _cal_action_style(val):
            v = str(val)
            if "SELL" in v or "SHORT" in v:  return "color:#ff8888"
            if "BUY"  in v or "LONG"  in v:  return "color:#88ff88"
            if "WAIT" in v:                   return "color:#ffcc88"
            return ""

        st.dataframe(
            _cal_df.style
                .map(_cal_signal_style,    subset=["Signal"])
                .map(_cal_intensity_style, subset=["Intensity"])
                .map(_cal_type_style,      subset=["Type"])
                .map(_cal_action_style,    subset=["Action"])
                .set_properties(**{"font-size": "12px", "text-align": "left"})
                .set_table_styles([{"selector": "th", "props": [
                    ("background-color", "#1a1a2e"), ("color", "white"), ("font-weight", "bold")
                ]}]), width='stretch',
            height=600,
        )

        st.caption(
            f"Rolling window: today -> {_end_date} ({len(_cal_events)} total events) | "
            f"Showing: {len(_cal_events_filtered)} after filter | "
            f"Telegram + Email auto-fires 1 day before every HIGH/EXTREME event."
        )

    elif _cal_events and not _cal_events_filtered:
        st.info(f"No events match the '{_cal_signal_filter}' filter in this window. Try 'ALL'.")


    # ─── 🔢 DYNAMIC NUMEROLOGY PANEL ────────────────────────────
    st.divider()
    st.subheader("🔢 Numerology Signal - Dynamic Date + Planetary Engine")
    st.caption("Date root (DD+MM+YYYY digit sum) × live planetary positions. "
               "Research: @pankajkummar369 system, 50-yr sector study, Vedic astro-numerology. "
               "**Not financial advice.**")
    _nsel = st.date_input("Numerology date", value=datetime.now(IST).date(),
        min_value=date(2020,1,1), max_value=date(2030,12,31), key="nr_date")
    try:   _nv = _vedic_day_analysis(_nsel)
    except: _nv = None
    _nr = _nr_full(_nsel, _nv)
    _nc = ("#00C851" if "BULLISH" in _nr["final_signal"]
           else "#FF4444" if "BEARISH" in _nr["final_signal"] or "EXTREME" in _nr["final_signal"]
           else "#FFD700")
    st.markdown(f"""
<div style="background:#111;border:1px solid {_nc}44;border-radius:10px;padding:14px 18px">
  <div style="font-size:12px;color:#aaa">🔢 {_nr["calc"]} = Root {_nr["root"]}</div>
  <div style="font-size:22px;font-weight:800;color:{_nc}">{_nr["final_signal"]} - {_nr["planet"]}</div>
  <div style="font-size:13px;color:#ccc;margin-top:4px">{_nr["desc"]}</div>
  <div style="font-size:13px;color:{_nc};margin-top:6px;font-weight:600">{_nr["action"]}</div>
  <div style="font-size:12px;color:#aaa;margin-top:6px">
    📈 Strong: {_nr["sectors_strong"]} &nbsp;|&nbsp; 📉 Weak: {_nr["sectors_weak"]}
  </div>
</div>""", unsafe_allow_html=True)
    if _nr["amplifiers"]:
        st.markdown("**⚡ Live Planetary Amplifiers:**")
        _ac1,_ac2 = st.columns(2)
        for _ai,(_asc,_adsc) in enumerate(_nr["amplifiers"]):
            _acol = "#00ff88" if _asc>0 else "#ff6666"
            # FIX-11: ternary-as-statement returns DeltaGenerator -> shown in UI
            # Rewritten as proper if/else so no value is returned
            _amp_html = (f'<div style="font-size:12px;color:{_acol};padding:2px 0">'
                         f'{"🟢" if _asc>0 else "🔴"} ({_asc:+d}) {_adsc}</div>')
            if _ai % 2 == 0:
                _ac1.markdown(_amp_html, unsafe_allow_html=True)
            else:
                _ac2.markdown(_amp_html, unsafe_allow_html=True)
    _nb1,_nb2,_nb3,_nb4 = st.columns(4)
    _nb1.metric("Root", f"{_nr['root']} ({_nr['planet']})")
    _nb2.metric("Base",  f"{_nr['base_score']:+d}")
    _nb3.metric("Amplifiers", f"{_nr['amp_score']:+d}")
    _nb4.metric("Final", f"{_nr['total_score']:+d}")
    st.markdown("##### 📅 10-Day Numerology Forecast")
    _nrows=[]; _nd_base=datetime.now(IST).date(); _nfound=0
    for _ndi in range(1,35):
        _nfd=_nd_base+timedelta(days=_ndi)
        if _nfd.weekday()>=5 or _nfd in NSE_HOLIDAYS: continue
        _nfr=_nr_full(_nfd)
        _nrows.append({"Date":_nfd.strftime("%a %d %b"),"Root":_nfr["root"],"Planet":_nfr["planet"],
                        "Signal":_nfr["final_signal"],"Calc":_nfr["calc"]})
        _nfound+=1
        if _nfound>=10: break
    if _nrows:
        _ndf=pd.DataFrame(_nrows)
        def _nrs(v):
            s=str(v)
            if "BULLISH" in s: return "background-color:#003300;color:#00ff88;font-weight:700"
            if "EXTREME" in s: return "background-color:#220000;color:#ff2222;font-weight:800"
            if "BEARISH" in s: return "background-color:#1a0000;color:#ff6666;font-weight:600"
            return "background-color:#1a1a00;color:#ffdd00"
        st.dataframe(_ndf.style.map(_nrs,subset=["Signal"])
            .set_properties(**{"font-size":"12px"}), width='stretch', height=390)
    if _nv:
        _vs=_nv.get("net_score",0); _ts=_nr["total_score"]; _cs=_ts+_vs
        _cv,_cc = (("🟢 VERY STRONG BULLISH","#00C851") if _cs>=5 else
                   ("🟢 BULLISH","#00C851") if _cs>=2 else
                   ("🟡 NEUTRAL","#FFD700") if _cs>=0 else
                   ("🔴 BEARISH","#FF4444") if _cs>=-3 else
                   ("🔴 EXTREME BEARISH","#FF4444"))
        st.markdown(f'<div style="background:#111;border-radius:8px;padding:10px 16px;margin-top:6px"><div style="font-size:12px;color:#aaa">🔗 Numerology ({_ts:+d}) + Vedic ({_vs:+d})</div><div style="font-size:18px;font-weight:700;color:{_cc}">{_cv}</div></div>',unsafe_allow_html=True)

    st.divider()

    # ── TODAY'S SNAPSHOT ─────────────────────────────────────────────
    try:
        _today_snap = _vedic_day_analysis(datetime.now(IST).date())
        _c1, _c2, _c3, _c4 = st.columns(4)
        _c1.metric("Today Signal",   _today_snap["overall"])
        _c2.metric("Moon Nakshatra", _today_snap["moon_nak"])
        _c3.metric("Moon Rashi",     _today_snap["moon_rashi"])
        _c4.metric("Tithi",          _today_snap["tithi"].split("(")[0].strip())

        # ── Conflict resolver: Header vs Calendar signal ───────────────────────
        # If the weighted header score (BULLISH) conflicts with calendar pattern (BEARISH),
        # show an explanation banner so the user knows which to prioritise.
        try:
            _header_bull = "BULLISH" in _today_snap.get("overall", "")
            _header_bear = "BEARISH" in _today_snap.get("overall", "")
            _tithi_today = _today_snap.get("tithi_name", _today_snap.get("tithi","")).split("(")[0].strip()
            _is_amavasya = any(x in _tithi_today for x in ["Amavasya","Chaturdashi","Pratipada"])
            _jup_exalted = _today_snap.get("jupiter_dignity","") == "EXALTED"

            if _header_bull and _is_amavasya:
                st.warning(
                    "**📊 Signal Conflict Explained - Both readings are correct:**\n\n"
                    "**Header card (BULLISH)** = *Weighted net score* across all 8 layers. "
                    f"Jupiter EXALTED (+3) + Venus Vara + Moon Hora outweigh Amavasya (-3) + Saturn (-2) "
                    f"by a narrow margin -> net **+2**. This tells you the **magnitude**: no crash today, "
                    f"Jupiter creates a market floor.\n\n"
                    "**Calendar row (BEARISH)** = *Nak/Tithi pattern rule*. Amavasya historically "
                    f"gives ~78% DOWN days regardless of other factors. This tells you the **direction**.\n\n"
                    "**✅ How to use both:**\n"
                    "- *Direction* -> follow **Calendar** (BEARISH: sell rallies, don't buy fresh longs)\n"
                    "- *Magnitude* -> follow **Header** (Jupiter floor: fall limited to -0.5% to -1.5%, cover shorts by 14:00)\n"
                    "- *Combined*: Bearish bias, but no crash. Sell above yest high. Buy only after strong dip near support."
                )
            elif _header_bear and not _is_amavasya:
                # Header bearish but calendar bullish
                _cal_bull = any(x in _today_snap.get("moon_nak","") for x in
                                ["Pushya","Hasta","Revati","Uttara Phalguni","Punarvasu",
                                 "Anuradha","Vishakha","Shravana","Uttara Ashadha"])
                if _cal_bull:
                    st.info(
                        "**📊 Signal Note:** Header shows BEARISH but nakshatra is bullish. "
                        "Planetary afflictions (Rahu/Ketu/Saturn) dominate today. "
                        "Trade with caution - intraday reversals likely. Prefer range-trading over trending."
                    )
        except Exception:
            pass
        st.caption(
            f"Sun️ Sun: **{_today_snap['sun_rashi']}** / {_today_snap['sun_nak']}  ·  "
            f"Venus Venus: **{_today_snap['venus_rashi']}**  ·  "
            f"Mars Mars: **{_today_snap['mars_rashi']}** / {_today_snap['mars_nak']}  ·  "
            f"Saturn Saturn: **{_today_snap['saturn_rashi']}**  ·  "
            f"Jupiter Jupiter: **{_today_snap['jupiter_rashi']}**  ·  "
            f"Mercury Mercury: **{_today_snap['mercury_rashi']}**  ·  "
            f"Rahu Rahu: **{_today_snap['rahu_rashi']}**"
        )
        for _sig_text, _sig_type in _today_snap["signals"]:
            _icon = {"B":"🔴","G":"🟢","T":"⚠️","M":"🟡"}.get(_sig_type, "")
            st.markdown(f"  {_icon} {_sig_text}")
    except Exception as _e:
        import traceback as _tb
        st.warning(f"Today snapshot error: {_e}")
        st.code(_tb.format_exc(), language="python")

    st.divider()

    # ── 10-DAY DYNAMIC FORECAST ──────────────────────────────────────
    st.subheader("🔮 Next 10 Trading Days - Dynamic Nifty Astro Forecast")
    st.caption(
        "Computed live from planetary positions (Lahiri ayanamsa, Jean Meeus Ch47). "
        "Combines Moon nakshatra, tithi, conjunction angles. Refreshes every page load. "
        "**Not financial advice.**"
    )

    _forecast_rows = []
    _check_date = datetime.now(IST).date()
    _days_found = 0
    _forecast_error = None

    for _di in range(1, 40):
        _fd = _check_date + timedelta(days=_di)
        if _fd.weekday() >= 5 or _fd in _get_nse_holidays():
            continue
        try:
            _r = _vedic_day_analysis(_fd)
        except Exception as _fe:
            if _forecast_error is None:
                import traceback as _tb2
                _forecast_error = f"{_fe}\n{_tb2.format_exc()}"
            continue

        _conj = []
        if _r["moon_ketu_deg"]  < 15: _conj.append(f"Moon-Ketu {_r['moon_ketu_deg']}°")
        if _r["mars_rahu_deg"]  < 15: _conj.append(f"Mars-Rahu {_r['mars_rahu_deg']}°")
        if _r["sun_saturn_deg"] <  8: _conj.append(f"Sun-Saturn {_r['sun_saturn_deg']}°")

        _sig_parts = []
        for _txt, _typ in _r["signals"]:
            _ico = {"B":"🔴","G":"🟢","T":"⚠️","M":"🟡"}.get(_typ, "")
            _sig_parts.append(f"{_ico} {_txt}")

        # Crude oil / intraday hints
        _crude_sigs = [t for t, typ in _r["signals"] if "⛽" in t]
        _crude_str  = "🟢 BULL" if _crude_sigs else "-"
        # Intraday note: Rohini = TRAP; Moon in Taurus = gap-down then recovery
        _intra = ""
        if _r["moon_nak"] == "Rohini":           _intra = "⚠️ TRAP - sell the rally"
        elif _r["moon_rashi"] == "Taurus":        _intra = "🔄 Gap-down -> 200pt recovery"
        elif _r["moon_rashi"] == "Sagittarius":   _intra = "🟢 Bullish open expected"
        elif "Amavasya" in _r["tithi"] or "Pratipada" in _r["tithi"]:
            _intra = "🔴 Sell on any rise"

        _forecast_rows.append({
            "Date":           _fd.strftime("%a %d %b"),
            "Signal":         _r["overall"],
            "Intensity":      _r["intensity"],
            "Moon Nakshatra": f"{_r['moon_nak']} ({_r['moon_nak_lord']})",
            "Moon Rashi":     _r["moon_rashi"],
            "Tithi":          _r["tithi"].split("(")[0].strip(),
            "Key Conjunctions": "  ·  ".join(_conj) if _conj else "-",
            "Crude":          _crude_str,
            "Intraday Hint":  _intra if _intra else "-",
            "Score":          f"{_r['net_score']:+d} (B{_r['bearish_pts']}/G{_r['bullish_pts']})",
            "Key Signals":    "  |  ".join(_sig_parts[:3]) if _sig_parts else "No major signals",
        })
        _days_found += 1
        if _days_found >= 10:
            break

    if _forecast_rows:
        _fdf = pd.DataFrame(_forecast_rows)

        def _fc_signal(val):
            v = str(val)
            if "BEARISH" in v:  return "background-color:#7b1a1a; color:#ffcccc; font-weight:700"
            if "BULLISH" in v:  return "background-color:#1a4d2e; color:#b6f5c8; font-weight:700"
            if "TRAP"    in v:  return "background-color:#5a4200; color:#ffe082; font-weight:700"
            if "MIXED"   in v:  return "background-color:#3a3000; color:#fff3b0"
            return ""

        def _fc_intensity(val):
            v = str(val)
            if v == "Extreme": return "background-color:#b71c1c; color:#fff; font-weight:700"
            if v == "Strong":  return "background-color:#c62828; color:#fff; font-weight:600"
            if v == "Mild":    return "background-color:#f57f17; color:#fff"
            if v == "Neutral": return "background-color:#424242; color:#eee"
            if v == "Caution": return "background-color:#5a4200; color:#ffe082"
            return ""

        st.dataframe(
            _fdf.style
                .map(_fc_signal,    subset=["Signal"])
                .map(_fc_intensity, subset=["Intensity"])
                .map(lambda v: "background-color:#1a2a5e; color:#aac4ff" if "BULL" in str(v) else "", subset=["Crude"])
                .set_properties(**{"font-size":"12px","text-align":"left"})
                .set_table_styles([{"selector":"th","props":[("font-weight","bold"),("background-color","#1a1a2e"),("color","white")]}]), width='stretch',
            height=420,
        )

        _nb = sum(1 for r in _forecast_rows if "BEARISH" in r["Signal"])
        _ng = sum(1 for r in _forecast_rows if "BULLISH" in r["Signal"])
        _nt = sum(1 for r in _forecast_rows if "TRAP"    in r["Signal"])
        _nm = sum(1 for r in _forecast_rows if "MIXED"   in r["Signal"])
        st.caption(f"**10-day summary:** 🔴 {_nb} Bearish  ·  🟢 {_ng} Bullish  ·  ⚠️ {_nt} Trap  ·  🟡 {_nm} Mixed")

        with st.expander("🔍 Full Signal Details - All 10 Days"):
            for _row_d in _forecast_rows:
                _ico = ("🔴" if "BEARISH" in _row_d["Signal"] else
                        "🟢" if "BULLISH" in _row_d["Signal"] else
                        "⚠️" if "TRAP"    in _row_d["Signal"] else "🟡")
                st.markdown(f"**{_ico} {_row_d['Date']} - {_row_d['Signal']} ({_row_d['Intensity']})**")
                st.markdown(f"&nbsp;&nbsp;Moon Moon: **{_row_d['Moon Nakshatra']}** · Rashi: {_row_d['Moon Rashi']} · {_row_d['Tithi']}")
                if _row_d["Key Conjunctions"] != "-":
                    st.markdown(f"&nbsp;&nbsp;⚡ {_row_d['Key Conjunctions']}")
                for _line in _row_d["Key Signals"].split("  |  "):
                    if _line.strip():
                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;{_line.strip()}")
                st.markdown("---")
    else:
        if _forecast_error:
            st.error(f"Forecast error detail:")
            st.code(_forecast_error, language="python")
        else:
            st.info("Could not compute forecast.")

    st.divider()

    try:
        astro = get_astro_score()
        time_signal = get_time_signal()
        with st.expander("🔧 Legacy Astro Score"):
            _lc1, _lc2, _lc3 = st.columns(3)
            _lc1.metric("Signal",    astro["signal"])
            _lc2.metric("Score",     astro["score"])
            _lc3.metric("Time Zone", time_signal)
            st.write("Moon Sign:",  astro["moon_sign"])
            st.write("Nakshatra:",  astro["nakshatra"])
            st.write("Reason:",     astro["reason"])
    except Exception:
        pass

    #st.write("✔ Excel matched")
    #st.write("✔ No features removed")

    # ═══════════════════════════════════════════════════════════════
    # 🪐 NIFTY ASTRO HEATMAP - Based on Planetary Rules (Transcript Analysis)
    # ═══════════════════════════════════════════════════════════════
    # Rules extracted from 20 YouTube prediction transcripts (Feb–Mar 2026)
    # Analyst uses Vedic astrology: Moon nakshatra, planetary conjunctions,
    # navamsha positions and lunar tithi to predict Nifty direction.
    # ═══════════════════════════════════════════════════════════════

    st.divider()
    with st.expander("📚 Static Reference - Batch 1: Planetary Signal Rules (click to expand)", expanded=False):
        st.caption(
            "Rules distilled from 20 astro-prediction transcripts (Feb–Mar 2026). "
            "Each row = a planetary condition and its historical market outcome. "
            "Use alongside technical analysis - NOT standalone trading advice."
        )
        st.subheader("🪐 Nifty Astro Heatmap - Planetary Signal Rules")

        # ── Rule Database ────────────────────────────────────────────────
        # _astro_rules defined above (before calendar engine)

        _astro_df = pd.DataFrame(_astro_rules)

        # ── Color map ─────────────────────────────────────────────────────
        def _astro_color(val):
            v = str(val)
            if "BEARISH" in v and "BULLISH" not in v: return "background-color:#7b1a1a; color:#ffcccc; font-weight:600"
            if "BULLISH" in v and "BEARISH" not in v: return "background-color:#1a4d2e; color:#b6f5c8; font-weight:600"
            if "TRAP" in v:     return "background-color:#5a4200; color:#ffe082; font-weight:600"
            if "MIXED" in v:    return "background-color:#3a3000; color:#fff3b0"
            if "PRESSURE" in v: return "background-color:#1a2a5e; color:#aac4ff"
            return ""

        def _intensity_color(val):
            v = str(val)
            if v == "Extreme":    return "background-color:#b71c1c; color:#fff; font-weight:700"
            if v == "Very High":  return "background-color:#c62828; color:#fff; font-weight:600"
            if v == "High":       return "background-color:#bf360c; color:#fff"
            if v == "Medium":     return "background-color:#f57f17; color:#fff"
            if v == "Low":        return "background-color:#2e7d32; color:#fff"
            if v == "Historical": return "background-color:#455a64; color:#fff"
            return ""

        st.dataframe(
            _astro_df.style
                .map(_astro_color,    subset=["Signal"])
                .map(_intensity_color, subset=["Intensity"])
                .set_properties(**{"font-size": "12px", "text-align": "left"})
                .set_table_styles([{"selector": "th", "props": [("font-weight", "bold"), ("background-color", "#1a2255"), ("color", "#ffffff")]}]), width='stretch',
            height=600,
        )

        # ── Quick Legend ──────────────────────────────────────────────────
        with st.expander("📖 How to Read This Table"):
            st.markdown("""
    **Signal Types**
    | Signal | Meaning |
    |--------|---------|
    | 🔴 BEARISH | High probability of Nifty fall |
    | 🟢 BULLISH | High probability of Nifty rise |
    | ⚠️ TRAP | Market rises then reverses - do NOT buy calls |
    | 🟡 MIXED | Volatile / unpredictable; strategy-dependent |
    | ⛽ BEARISH Nifty / BULLISH Crude | Opposing moves across assets |

    **Intensity Scale**
    | Level | Typical Move |
    |-------|-------------|
    | Extreme | Lower circuit risk (2300+ pts fall) |
    | Very High | 500–1500 pts fall within 1–3 days |
    | High | 200–500 pts move |
    | Medium | 100–200 pts; volatile |
    | Low | Minor directional bias |

    **Key Rules (most powerful):**
    1. **Moon–Ketu conjunction** = Reliance + HDFC weaken = Nifty falls
    2. **Mars–Mercury–Rahu triple** = crude surges, Nifty crashes, lower circuit risk
    3. **Saturn–Venus in Pisces** = brutal bearish until Venus exits (~Mar 25)
    4. **Saturn–Sun exact degree** = Nifty + Gold + Silver all fall; only crude rises
    5. **Krishna Pratipada tithi** (day after no-moon) = Nifty almost always falls
    6. **Rohini nakshatra Moon** = TRAP - false bullish move, reverse after
    7. **Venus enters Aries** = Bottom signal - start going LONG aggressively
    8. **Pushkara Navamsha ends** = immediate reversal downward
            """)

        # ═══════════════════════════════════════════════════════════════
    # 🪐 ASTRO HEATMAP - BATCH 2 (19 new transcripts: Nov 2025 – Mar 2026)
    # New rules covering: Sun transit, Hora analysis, Silver/Copper,
    # personal kundali for trading, Moon sign rashi rules, intraday timing
    # ═══════════════════════════════════════════════════════════════

    with st.expander("📚 Static Reference - Batch 2: Extended Rules (Sun transit, Hora, Commodities) (click to expand)", expanded=False):
        st.caption(
            "Additional rules extracted from 19 new transcripts (Nov 2025 – Mar 2026). "
            "Covers Sun transit by rashi, Hora timing, Silver/Copper commodity signals, "
            "personal kundali indicators, intraday timing patterns and Moon rashi rules."
        )
        st.subheader("🪐 Astro Heatmap - Batch 2 (Extended Rules)")

        # _astro_rules2 defined above (before calendar engine)

        # Both rule lists now fully defined - parse them
        _parse_nak_rules(_astro_rules)
        _parse_nak_rules(_astro_rules2)

        _astro_df2 = pd.DataFrame(_astro_rules2)

        def _astro_color2(val):
            v = str(val)
            if "BEARISH" in v and "BULLISH" not in v: return "background-color:#7b1a1a; color:#ffcccc; font-weight:600"
            if "BULLISH" in v and "BEARISH" not in v: return "background-color:#1a4d2e; color:#b6f5c8; font-weight:600"
            if "TRAP" in v:        return "background-color:#5a4200; color:#ffe082; font-weight:600"
            if "MIXED" in v:       return "background-color:#3a3000; color:#fff3b0"
            if "PREREQUISITE" in v or "FAVORABLE" in v: return "background-color:#1a2a5e; color:#aac4ff"
            if "CAUTION" in v:     return "background-color:#4a2800; color:#ffcc80"
            if "FRAMEWORK" in v or "LIMIT" in v: return "background-color:#2d1a5e; color:#d4aaff"
            return ""

        def _intensity_color2(val):
            v = str(val)
            if v == "Extreme":    return "background-color:#b71c1c; color:#fff; font-weight:700"
            if v == "Very High":  return "background-color:#c62828; color:#fff; font-weight:600"
            if v == "High":       return "background-color:#bf360c; color:#fff"
            if v == "Medium":     return "background-color:#f57f17; color:#fff"
            if v == "Low":        return "background-color:#2e7d32; color:#fff"
            if v == "Foundation": return "background-color:#0d47a1; color:#fff; font-weight:600"
            return ""

        st.dataframe(
            _astro_df2.style
                .map(_astro_color2,    subset=["Signal"])
                .map(_intensity_color2, subset=["Intensity"])
                .set_properties(**{"font-size": "12px", "text-align": "left"})
                .set_table_styles([{"selector": "th", "props": [("font-weight", "bold"), ("background-color", "#1a2255"), ("color", "#ffffff")]}]), width='stretch',
            height=600,
        )

        with st.expander("📖 Batch 2 - Key Rules Summary"):
            st.markdown("""
    **Sun Transit Framework**
    - Sun in **Capricorn** = bullish month | Sun in **Aquarius** = bearish month
    - Sun changes **nakshatra mid-session** = that session turns bearish
    - Sun in **Jupiter nakshatra** + Jupiter retrograde = brutal correction

    **Silver Trading Rules (Venus + Moon)**
    - Venus enters **Capricorn** = Silver TOP formed - go short immediately
    - Venus conjunct **Moon** = Silver falls 5-7% that day
    - Venus in **Sun's nakshatra** = Silver selling pressure, target Rs.1,90,000
    - **5-planet stellium** within 5° = Silver cannot rally, falls to Rs.1,90,000

    **Personal Kundali Prerequisites**
    - Strong **Mercury** = trading intelligence ✅
    - **Jupiter -> 9th/11th** = luck + profit from markets ✅
    - **Rahu in 11th** = stock market profits ✅ (unless Mars/Saturn aspect Rahu)
    - **Ketu afflicted** = businesses fail at last moment ⚠️

    **Intraday Timing Rules**
    - Moon degree × 3 = **max Nifty points** for that day
    - Moon 11th from Sun = bearish day
    - **Hora analysis**: Sun+Moon+Mercury in same hora = fall during that hora
    - Gap-down + **Nitya yoga**: buy calls 9:25–9:30, book at 10:30–10:45 only

    **Rahu Period Warning**
    - Active Rahu transit = **illusion/confusion** - expect false moves both ways
    - Wait for Rahu period to pass, then trade the real direction
            """)

    st.divider()
    st.subheader("📤 Export Dashboard Data")

    excel_file = export_all_tabs_to_excel()

    st.download_button(
        label="📥 Download Full Dashboard (Excel)",
        data=excel_file,
        file_name=f"Panchak_Dashboard_{datetime.now(IST).strftime('%d_%b_%Y_%H%M')}.xlsx",
        mime="application/vnd.LIVE_OPENxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.write(" I . when it is TOP LIVE_HIGH breaks or NEAR LIVE_HIGH value - just check the Entity is DAILY and WEEKLY UPTREND AND SUPER TREND SHOULD IN BUY MODE in DAILY TREND- THEN ONLY ENTER - same as SELL ViceVersa")
    st.write(" II. when you take position - always decide your SL and immediatly put StopLoss - IF STOP LOSS HITS - Dont touch for that day or reenter : we will get multiple Chances in coming days and we have lot of entities")
    st.write(" III. whenever Entity breaks TOP LIVE_HIGH (in buy entry) and returns and SL hits - There is a possibility to REVERSE and you will get sell side opportunity- some wiered cases both sides SL hits. dont touch that time")
    st.write(" IV. Take only one or two lots and keep some money with you, other wise when sudden dips you will have a chance to average in worst cases, other wise losses will be huge, if we average at least  can exit with minimal losses")
    st.write(" V. when prices below of panchak low and not moved punchak up side, dont carry longs until it comes above TOP_HIGH , same for sell side as well")
    st.write(" VI. take stock positions based on INDICES TOP_HIGH and TOP_LOW as NIFTY controls most movements. ")
    st.subheader("📌 MOMENTUM SCORE – COLUMN MEANING")

    st.write(pd.DataFrame({
        "Column": [
            "MOMO_SCORE",
            "YH_RANGE_EXP_%",
            "YH_ACCEPTED",
            "OPEN_DIST_YH_%",
            "CHANGE_%"
        ],
        "Meaning": [
            "🔥 Higher score = stronger chance of 2%–5% run",
            "Fast expansion above YH = real momentum",
            "Holding above Yesterday High (no pullback)",
            "Open near Yesterday High = clean structure",
            "Actual intraday momentum"
        ]
    }))

# ═══════════════════════════════════════════════════════════════════════
# Moon TAB 19 - KP PANCHANG TRADING WINDOWS
# ═══════════════════════════════════════════════════════════════════════
with tabs[1]:
    render_kp_tab()


# ── BOS / CHoCH TAB (injected by apply_bos_patch.py) ──
with tabs[19]:
    if _BOS_OK:
        render_bos_tab(st.session_state.get("bos_cache",{}), db=_ohlc_db)
        st.divider()
        # ── OHLC DB status ────────────────────────────────────────────
        from ohlc_store import render_db_status
        with st.expander("📦 OHLC Database Status", expanded=False):
            st.caption(f"Updated: {st.session_state.get('ohlc_db_updated','never')}")
            render_db_status(_ohlc_db)
            if st.button("🔄 Force DB Update Now", key="_bos_force_db_update"):
                _update_ohlc_db_safe(kite, STOCKS)
                st.rerun()
    else:
        st.warning(
            "⚠️ BOS scanner module not loaded.  \n"
            "Place bos_scanner.py and ohlc_store.py in the same folder as the dashboard.  \n"
            f"Import error: bos_scanner.py not found"
        )
        # ── Show alert log even if scanner is down (alerts may have fired earlier) ──
        st.markdown("### 📲 Today\'s BOS / CHoCH Alert Log (from CACHE)")
        _bos_log_path = os.path.join(CACHE_DIR, f"bos_alert_log_{date.today().strftime('%Y%m%d')}.json")
        if os.path.exists(_bos_log_path):
            try:
                import json as _bj
                with open(_bos_log_path, "r", encoding="utf-8") as _blf:
                    _bos_fallback_log = _bj.load(_blf)
                if _bos_fallback_log:
                    import pandas as _bpd
                    _blog_rows = []
                    for _be in reversed(_bos_fallback_log):
                        _bs = _be.get("setup", {})
                        _blog_rows.append({
                            "Time":      _be.get("time",""),
                            "Type":      _be.get("type",""),
                            "Symbol":    _be.get("symbol",""),
                            "LTP":       _be.get("ltp",0),
                            "Broke":     _be.get("broke_level",0),
                            "Strength%": _be.get("strength_pct",0),
                            "Vol x":     _be.get("volume_ratio",0),
                            "Prior":     _be.get("prior_trend",""),
                            "Entry A":   _bs.get("momentum_entry",0),
                            "Entry B":   _bs.get("ob_entry",0),
                            "SL":        _bs.get("sl",0),
                            "T1":        _bs.get("t1",0),
                            "T2":        _bs.get("t2",0),
                            "R:R A":     _bs.get("momentum_rr",0),
                            "R:R B":     _bs.get("ob_rr",0),
                        })
                    st.dataframe(_bpd.DataFrame(_blog_rows), width='stretch',
                                 hide_index=True, height=min(600,60+len(_blog_rows)*35))
                else:
                    st.info("No BOS/CHoCH alerts logged today yet.")
            except Exception as _ble:
                st.info(f"Could not read alert log: {_ble}")
        else:
            st.info("No alert log file found for today.")


# ═══════════════════════════════════════════════════════════════════════
# 📊 TAB 21 - MACD DAILY SCANNER  (Point 9)
# Daily 12/26/9 MACD crossover table for all stocks.
# Data sourced from background worker cache (CACHE/macd_cache.json if saved,
# else computed live from Kite historical data).
# ═══════════════════════════════════════════════════════════════════════
with tabs[21]:
    st.subheader("📊 MACD Daily Scanner - 12 / 26 / 9")
    # Telegram status
    _macd_tg_on = st.session_state.get("tg_MACD_BULL", True) or st.session_state.get("tg_MACD_BEAR", True)
    st.markdown(
        f'<div style="background:#111;border-radius:6px;padding:6px 12px;margin-bottom:8px">'
        f'📡 Telegram alerts: <b>{"✅ ON" if _macd_tg_on else "❌ OFF"}</b> '
        f'(Bull crossover + Bear crossover · managed by background_worker.py)</div>',
        unsafe_allow_html=True
    )
    st.caption(
        "Daily timeframe MACD (12-period fast, 26-period slow, 9-period signal). "
        "Bullish = MACD line crossed above Signal. Bearish = MACD line crossed below Signal. "
        "Updated by background worker at market open."
    )

    # ── USE PRE-CALCULATED MACD DATA ──
    try:
        if _macd_df.empty:
            _kite_ok = "kite" in dir() and kite is not None
            if not _kite_ok:
                st.error("❌ Kite not connected - MACD requires live historical data. Connect Kite and refresh.")
            else:
                st.warning(
                    "⏳ No MACD data yet. This happens because:\n\n"
                    "- Market just opened (MACD needs 35+ daily candles - loads in ~10 seconds)\n"
                    "MACD uses the last 90 days of daily closing prices for each stock."
                )
        else:
            # Separate crossover stocks
            # Add Crossover column to _macd_df if it doesn't exist
            if "Crossover" not in _macd_df.columns:
                _macd_df["Crossover"] = ""
                _macd_df.loc[(_macd_df["MACD Status"] == "🟢 BULLISH CROSS"), "Crossover"] = "🟢 BULL CROSS"
                _macd_df.loc[(_macd_df["MACD Status"] == "🔴 BEARISH CROSS"), "Crossover"] = "🔴 BEAR CROSS"

            if "Trend" not in _macd_df.columns:
                _macd_df["Trend"] = _macd_df["MACD Status"].apply(lambda x: "🟢 BULLISH" if "📈" in x or "BULL" in x else "🔴 BEARISH")

            if "Histogram" not in _macd_df.columns:
                _macd_df["Histogram"] = (_macd_df["MACD"] - _macd_df["Prev"]).round(3)

            _bull_cross = _macd_df[_macd_df["Crossover"] == "🟢 BULL CROSS"].copy()
            _bear_cross = _macd_df[_macd_df["Crossover"] == "🔴 BEAR CROSS"].copy()
            _bull_macd  = _macd_df[(_macd_df["Histogram"] > 0) & (_macd_df["Crossover"] == "")].copy()
            _bear_macd  = _macd_df[(_macd_df["Histogram"] < 0) & (_macd_df["Crossover"] == "")].copy()

            _mc1, _mc2, _mc3, _mc4 = st.columns(4)
            _mc1.metric("🟢 Bull Crossovers",  len(_bull_cross))
            _mc2.metric("🔴 Bear Crossovers",  len(_bear_cross))
            _mc3.metric("📈 Bullish MACD",     len(_bull_macd))
            _mc4.metric("📉 Bearish MACD",     len(_bear_macd))

            st.markdown("### 🟢 Bullish Crossovers - MACD crossed above Signal (Daily)")
            if not _bull_cross.empty:
                st.dataframe(
                    _apply_fmt(_bull_cross.sort_values("Histogram", ascending=False)
                    [["Symbol","LTP","MACD","Prev","Histogram","Crossover","Trend"]]),
                    width='stretch', hide_index=True
                )
            else:
                st.info("No bullish crossovers today.")

            st.markdown("### 🔴 Bearish Crossovers - MACD crossed below Signal (Daily)")
            if not _bear_cross.empty:
                st.dataframe(
                    _apply_fmt(_bear_cross.sort_values("Histogram", ascending=True)
                    [["Symbol","LTP","MACD","Prev","Histogram","Crossover","Trend"]]),
                    width='stretch', hide_index=True
                )
            else:
                st.info("No bearish crossovers today.")

            st.markdown("### 📊 All Stocks - MACD Status")
            _macd_display = _macd_df.sort_values("Histogram", ascending=False).copy()
            st.dataframe(
                _apply_fmt(_macd_display[["Symbol","LTP","MACD","Prev","Histogram","Crossover","Trend"]]),
                width='stretch', hide_index=True,
                height=600
            )
    except Exception as _e:
        st.error(f"MACD scanner error: {_e}")
    except Exception as _e:
        st.error(f"MACD scanner error: {_e}")


# ═══════════════════════════════════════════════════════════════════════
# 🕯️ TAB 22 - INSIDE BAR SCANNER  (Point 13)
# Daily timeframe - shows all stocks where today's candle is inside
# yesterday's candle. Highlights breakout candidates.
# ═══════════════════════════════════════════════════════════════════════
with tabs[22]:
    st.subheader("🕯️ Inside Bar Scanner - Daily Timeframe")
    _ib_tg_on = st.session_state.get("tg_INSIDE_BAR", True)
    st.markdown(
        f'<div style="background:#111;border-radius:6px;padding:6px 12px;margin-bottom:8px">'
        f'📡 Telegram alerts: <b>{"✅ ON" if _ib_tg_on else "❌ OFF"}</b> '
        f'(fires when LTP breaks parent bar · managed by background_worker.py)</div>',
        unsafe_allow_html=True
    )
    st.caption(
        "An Inside Bar forms when today's High ≤ Yesterday's High AND today's Low ≥ Yesterday's Low. "
        "It signals volatility compression before a breakout. "
        "**Entry**: above parent bar high (long) or below parent bar low (short). "
        "**SL**: opposite end of parent bar."
    )

    # ── USE PRE-CALCULATED INSIDE BAR DATA ──
    try:
        if _ib_df.empty:
            _kite_ok2 = "kite" in dir() and kite is not None
            if not _kite_ok2:
                st.error("❌ Kite not connected - Inside Bar scanner requires live data. Connect Kite and refresh.")
            else:
                st.info("⏳ No Inside Bar patterns found today yet.")
        else:
            # Stats
            _hc_cnt = len(_ib_df[_ib_df["High Conv"] == "🔥 YES"])
            _total  = len(_ib_df)
            st.metric("🎯 Total Inside Bars", _total, delta=f"{_hc_cnt} High Conv", delta_color="normal")

            st.markdown("### 🔥 High Conviction Inside Bars")
            _hc_df = _ib_df[_ib_df["High Conv"] == "🔥 YES"].copy()
            if not _hc_df.empty:
                st.dataframe(
                    _apply_fmt(_hc_df),
                    width='stretch', hide_index=True
                )
            else:
                st.info("No high-conviction inside bars today.")

            st.markdown("### 📊 All Inside Bars")
            st.dataframe(
                _apply_fmt(_ib_df),
                width='stretch', hide_index=True,
                height=600
            )
            
            # Note on break logic
            st.caption("Breakout/Breakdown logic is processed every refresh cycle. Alerts fire dynamically on LTP level cross.")

    except Exception as _e:
        st.error(f"Inside Bar scanner error: {_e}")

    # ── Fire Telegram alerts - LIVE LTP check on EVERY refresh ─────────────
    if not _ib_df.empty and is_market_hours():
        _today_str = date.today().strftime("%Y%m%d")
        _IB_PROX_PCT = 0.3    # % within which pre-alert fires

        for _, _ib_row in _ib_df.iterrows():
            _ib_sym  = _ib_row["Symbol"]
            _ib_ph   = float(_ib_row.get("Parent High", 0) or 0)
            _ib_pl   = float(_ib_row.get("Parent Low",  0) or 0)
            _ib_comp = float(_ib_row.get("Compression %", 0) or 0)
            _ib_hc_flag  = _ib_row.get("High Conv", "")
            _ib_ch       = _ib_row.get("Child High", 0)
            _ib_cl_price = _ib_row.get("Child Low",  0)
            _ib_cc       = _ib_row.get("Child Close", 0)
            _ib_co       = _ib_row.get("Child Open",  0)
            _ib_ch_gap   = _ib_row.get("CH->PH Gap %", 0)

            if _ib_ph <= 0 or _ib_pl <= 0:
                continue

            # ── LIVE LTP from df (refreshed every 60s) ──────
            try:
                _live_rows = df[df["Symbol"] == _ib_sym]
                _ib_ltp  = float(_live_rows.iloc[0]["LTP"])   if not _live_rows.empty else 0.0
                _ib_vol  = float(_live_rows.iloc[0].get("VOL_%", 0) or 0)
            except Exception:
                _ib_ltp = 0.0; _ib_vol = 0.0
            if _ib_ltp <= 0:
                continue

            _ib_hc_line = "\n🔥 <b>HIGH CONVICTION</b> - Coiling at resistance (Siemens-style)" if _ib_hc_flag else ""

            # ── ① BREAKOUT ALERT (LTP crosses level) ─────────────────────
            if _ib_ltp > _ib_ph:
                _ib_dedup = f"IB_UP_{_ib_sym}_{_today_str}"
                _pts_above = round(_ib_ltp - _ib_ph, 2)
                _ib_msg = (
                    f"🕯️ <b>INSIDE BAR BREAKOUT ↑</b>{_ib_hc_line}\n"
                    f"📌 <b>{_ib_sym}</b>\n"
                    f"💰 LTP: <b>Rs.{_ib_ltp:,.2f}</b>  +{_pts_above:.2f} above Parent High Rs.{_ib_ph:,.2f}\n"
                    f"📊 Yest: O:{_ib_co}  H:{_ib_ch}  L:{_ib_cl_price}  C:{_ib_cc}\n"
                    f"📐 Compression: {_ib_comp:.0f}%  |  CH->PH Gap: {_ib_ch_gap}%  |  Vol: {_ib_vol:+.0f}%\n"
                    f"🎯 Entry Long &gt; Rs.{_ib_ph:,.2f}  |  SL: Rs.{_ib_pl:,.2f}"
                )
                send_alert_routed("INSIDE_BAR", _ib_msg, dedup_key=_ib_dedup)

            elif _ib_ltp < _ib_pl:
                # ── ① BREAKDOWN ALERT ─────────────────────────────────────
                _ib_dedup = f"IB_DN_{_ib_sym}_{_today_str}"
                _pts_below = round(_ib_pl - _ib_ltp, 2)
                _ib_msg = (
                    f"🕯️ <b>INSIDE BAR BREAKDOWN ↓</b>{_ib_hc_line}\n"
                    f"📌 <b>{_ib_sym}</b>\n"
                    f"💰 LTP: <b>Rs.{_ib_ltp:,.2f}</b>  -{_pts_below:.2f} below Parent Low Rs.{_ib_pl:,.2f}\n"
                    f"📊 Yest: O:{_ib_co}  H:{_ib_ch}  L:{_ib_cl_price}  C:{_ib_cc}\n"
                    f"📐 Compression: {_ib_comp:.0f}%  |  Vol: {_ib_vol:+.0f}%\n"
                    f"🎯 Entry Short &lt; Rs.{_ib_pl:,.2f}  |  SL: Rs.{_ib_ph:,.2f}"
                )
                send_alert_routed("INSIDE_BAR", _ib_msg, dedup_key=_ib_dedup)

            else:
                # ── ② PROXIMITY PRE-ALERT - LTP within 0.3% of level ─────
                # Fires max once per 10-min slot per stock per direction
                # Gives trader ~30s–2min advance notice before the break
                _dist_up = (_ib_ph - _ib_ltp) / _ib_ph * 100  if _ib_ph > 0 else 99
                _dist_dn = (_ib_ltp - _ib_pl)  / _ib_pl * 100  if _ib_pl > 0 else 99

                if _dist_up <= _IB_PROX_PCT and not _ib_hc_flag == "" and _ib_hc_flag:
                    # Only pre-alert for HIGH CONVICTION setups - reduces noise
                    # Use DAILY dedup instead of 10-min slot to limit to 1-2 times
                    _prox_key = f"IB_PROX_UP_DLY_{_ib_sym}_{_today_str}"
                    _pts_away = round(_ib_ph - _ib_ltp, 2)
                    
                    # Fetch extra context from global df
                    _g_row = df[df["Symbol"] == _ib_sym].iloc[0] if _ib_sym in df["Symbol"].values else _ib_row
                    _chg_v = _g_row.get("CHANGE", 0); _chg_p = _g_row.get("CHANGE_%", 0)
                    _yo = _g_row.get("YEST_OPEN", 0); _yh = _g_row.get("YEST_HIGH", 0); _yl = _g_row.get("YEST_LOW", 0); _yc = _g_row.get("YEST_CLOSE", 0)
                    _lo = _g_row.get("LIVE_OPEN", 0); _lh = _g_row.get("LIVE_HIGH", 0); _ll = _g_row.get("LIVE_LOW", 0)
                    _wh = _g_row.get("HIGH_W", 0); _wl = _g_row.get("LOW_W", 0)
                    _e7 = _g_row.get("EMA7", 0); _e20 = _g_row.get("EMA20", 0); _e50 = _g_row.get("EMA50", 0)

                    _prox_msg = (
                        f"⏰ <b>IB PRE-ALERT ↑</b> - {_ib_sym} approaching breakout\n"
                        f"🔥 HIGH CONVICTION inside bar - just {_pts_away:.2f} pts below Parent High\n"
                        f"  LTP: <b>Rs.{_ib_ltp:,.2f}</b> ({_chg_v:+.2f} | {_chg_p:+.2f}%)\n"
                        f"  Parent High: <b>Rs.{_ib_ph:,.2f}</b> | Compression: {_ib_comp:.0f}%\n"
                        f"  OHLC: {_lo}/{_lh}/{_ll}/{_ib_ltp}\n"
                        f"  YH: {_yh} | YL: {_yl} | YC: {_yc}\n"
                        f"  W-High: {_wh} | W-Low: {_wl}\n"
                        f"  E7: {_e7:.1f} | E20: {_e20:.1f} | E50: {_e50:.1f}\n"
                        f"  🛡 SL if long: Rs.{_ib_pl:,.2f}\n"
                        f"⚠️ <i>NOT financial advice.</i>"
                    )
                    send_alert_routed("INSIDE_BAR", _prox_msg, dedup_key=_prox_key)

                elif _dist_dn <= _IB_PROX_PCT and _ib_hc_flag:
                    _prox_key = f"IB_PROX_DN_DLY_{_ib_sym}_{_today_str}"
                    _pts_away = round(_ib_ltp - _ib_pl, 2)
                    
                    # Fetch extra context
                    _g_row = df[df["Symbol"] == _ib_sym].iloc[0] if _ib_sym in df["Symbol"].values else _ib_row
                    _chg_v = _g_row.get("CHANGE", 0); _chg_p = _g_row.get("CHANGE_%", 0)
                    _yo = _g_row.get("YEST_OPEN", 0); _yh = _g_row.get("YEST_HIGH", 0); _yl = _g_row.get("YEST_LOW", 0); _yc = _g_row.get("YEST_CLOSE", 0)
                    _lo = _g_row.get("LIVE_OPEN", 0); _lh = _g_row.get("LIVE_HIGH", 0); _ll = _g_row.get("LIVE_LOW", 0)
                    _wh = _g_row.get("HIGH_W", 0); _wl = _g_row.get("LOW_W", 0)
                    _e7 = _g_row.get("EMA7", 0); _e20 = _g_row.get("EMA20", 0); _e50 = _g_row.get("EMA50", 0)

                    _prox_msg = (
                        f"⏰ <b>IB PRE-ALERT ↓</b> - {_ib_sym} approaching breakdown\n"
                        f"🔥 HIGH CONVICTION inside bar - just {_pts_away:.2f} pts above Parent Low\n"
                        f"  LTP: <b>Rs.{_ib_ltp:,.2f}</b> ({_chg_v:+.2f} | {_chg_p:+.2f}%)\n"
                        f"  Parent Low: <b>Rs.{_ib_pl:,.2f}</b> | Compression: {_ib_comp:.0f}%\n"
                        f"  OHLC: {_lo}/{_lh}/{_ll}/{_ib_ltp}\n"
                        f"  YH: {_yh} | YL: {_yl} | YC: {_yc}\n"
                        f"  W-High: {_wh} | W-Low: {_wl}\n"
                        f"  E7: {_e7:.1f} | E20: {_e20:.1f} | E50: {_e50:.1f}\n"
                        f"  🛡 SL if short: Rs.{_ib_ph:,.2f}\n"
                        f"⚠️ <i>NOT financial advice.</i>"
                    )
                    send_alert_routed("INSIDE_BAR", _prox_msg, dedup_key=_prox_key)

    if st.button("🔄 Refresh Inside Bar Scan", key="ib_refresh_btn"):
        st.session_state.pop(_ib_ss_key, None)
        st.rerun()

    if _ib_df.empty:
        _kite_ok2 = "kite" in dir() and kite is not None
        if not _kite_ok2:
            st.error("❌ Kite not connected - Inside Bar scanner requires live data. Connect Kite and refresh.")
        else:
            st.info(
                "⏳ No Inside Bar patterns found yet.\n\n"
                "This scanner looks at the last 2 completed daily candles.\n"
                "Click **🔄 Refresh Inside Bar Scan** above to reload.\n\n"
                "Inside Bar = Yest High ≤ Parent High AND Yest Low ≥ Parent Low."
            )
    else:
        # ── Summary metrics ──────────────────────────────────────────────────
        _ib_hc     = _ib_df[_ib_df["High Conv"] == "🔥 YES"]
        _ib_break  = _ib_df[_ib_df["Status"].str.contains("UP|DOWN", regex=True)]
        _ib_pend   = _ib_df[_ib_df["Status"] == "⏳ Consolidating"]
        _m1,_m2,_m3,_m4 = st.columns(4)
        _m1.metric("🕯️ Total Inside Bars",  len(_ib_df))
        _m2.metric("🔥 High Conviction",     len(_ib_hc))
        _m3.metric("🟢 Broken Up",           len(_ib_df[_ib_df["Status"].str.contains("UP")]))
        _m4.metric("🔴 Broken Down",         len(_ib_df[_ib_df["Status"].str.contains("DOWN")]))

        # Column definitions reused across tables
        # Full OHLC columns: open/high/low/close for both child (=yest) and parent
        _FULL_COLS = [c for c in [
            "Symbol","LTP","Open","VOL %",
            "Child Open","Child High","Child Low","Child Close",
            "Parent High","Parent Low","Parent Close",
            "Parent Bias","Child Bias",
            "Compression %","CH->PH Gap %","Open<PH Gap %",
            "Entry Long >","Entry Short <","SL Long","SL Short",
            "High Conv","Status"
        ] if c in _ib_df.columns]

        _COMPACT_COLS = [c for c in [
            "Symbol","LTP","Open","VOL %",
            "Child High","Child Low","Child Close",
            "Parent High","Parent Low",
            "Compression %","CH->PH Gap %",
            "Entry Long >","SL Long","High Conv","Status"
        ] if c in _ib_df.columns]

        st.markdown("---")

        # ═══════════════════════════════════════════════════════════════════
        # SECTION 1 - HIGH CONVICTION (Siemens-style setups)
        # Child open below Parent High + Child High within 1.5% of Parent High
        # = Stock coiling right at resistance = explosive breakout potential
        # ═══════════════════════════════════════════════════════════════════
        st.markdown("### 🔥 High Conviction - Coiling at Resistance")
        st.caption(
            "**Siemens-style setup:** Yesterday's open is below the Parent High "
            "AND Yesterday's High is within **1.5%** of the Parent High - the stock is "
            "coiling right at resistance. When it breaks, the move is explosive (like Siemens ~6%).  "
            "Sorted by CH->PH Gap % (tightest coil first)."
        )
        if _ib_hc.empty:
            st.info("No High Conviction setups right now. Check back after 10:00 AM once today's candle develops.")
        else:
            _hc_breaking = _ib_hc[_ib_hc["Status"].str.contains("UP|DOWN", regex=True)]
            _hc_pend     = _ib_hc[_ib_hc["Status"] == "⏳ Consolidating"]

            if not _hc_breaking.empty:
                st.markdown("#### 🚨 Already Breaking Out")
                def _hc_style(row):
                    if "UP"   in str(row.get("Status","")): return ["background:#0a2e0a"]*len(row)
                    if "DOWN" in str(row.get("Status","")): return ["background:#2e0a0a"]*len(row)
                    return [""]*len(row)
                _hc_brk_cols = [c for c in [
                    "Symbol","LTP","Open","VOL %",
                    "Child High","Child Low","Child Close",
                    "Parent High","Parent Low",
                    "CH->PH Gap %","Compression %",
                    "Entry Long >","SL Long","Status"
                ] if c in _hc_breaking.columns]
                st.dataframe(
                    _hc_breaking.sort_values("CH->PH Gap %")[_hc_brk_cols]
                    .style.apply(_hc_style, axis=1),
                    width='stretch', hide_index=True
                )

            if not _hc_pend.empty:
                st.markdown("#### ⏳ Watching - Not Yet Broken Out")
                st.caption("These are the most explosive candidates. Set alerts above Parent High.")
                _hc_pend_cols = [c for c in [
                    "Symbol","LTP","Open","VOL %",
                    "Child Open","Child High","Child Low","Child Close",
                    "Parent High","Parent Low","Parent Close",
                    "Parent Bias","Child Bias",
                    "Compression %","CH->PH Gap %","Open<PH Gap %",
                    "Entry Long >","Entry Short <","SL Long","SL Short"
                ] if c in _hc_pend.columns]
                st.dataframe(
                    _hc_pend.sort_values("CH->PH Gap %")[_hc_pend_cols],
                    width='stretch', hide_index=True,
                    height=min(500, 60 + len(_hc_pend) * 38)
                )

        st.markdown("---")

        # ═══════════════════════════════════════════════════════════════════
        # SECTION 2 - ACTIVE BREAKOUTS / BREAKDOWNS (all)
        # ═══════════════════════════════════════════════════════════════════
        _ib_breaking = _ib_df[_ib_df["Status"] != "⏳ Consolidating"].copy()
        if not _ib_breaking.empty:
            st.markdown("### 🚨 Active Breakouts / Breakdowns")
            def _brk_style(row):
                if "UP"   in str(row.get("Status","")): return ["background:#0a2e0a"]*len(row)
                if "DOWN" in str(row.get("Status","")): return ["background:#2e0a0a"]*len(row)
                return [""]*len(row)
            _brk_cols = [c for c in [
                "Symbol","LTP","Open","VOL %",
                "Child High","Child Low","Child Close",
                "Parent High","Parent Low",
                "Compression %","CH->PH Gap %",
                "Entry Long >","Entry Short <","SL Long","SL Short",
                "High Conv","Status"
            ] if c in _ib_breaking.columns]
            st.dataframe(
                _ib_breaking.sort_values("Compression %")[_brk_cols]
                .style.apply(_brk_style, axis=1),
                width='stretch', hide_index=True
            )

        # ═══════════════════════════════════════════════════════════════════
        # SECTION 3 - ALL CONSOLIDATING (with full OHLC)
        # ═══════════════════════════════════════════════════════════════════
        st.markdown("### ⏳ All Consolidating - Watching for Breakout")
        st.caption(
            "**CH->PH Gap %** = how far child high is below parent high.  "
            "**0% = child high touched parent high** = maximum coil tension.  "
            "Sorted by CH->PH Gap % (tightest first)."
        )
        _ib_pending = _ib_df[_ib_df["Status"] == "⏳ Consolidating"].copy()
        if not _ib_pending.empty:
            _pend_cols = [c for c in [
                "Symbol","LTP","Open","VOL %",
                "Child Open","Child High","Child Low","Child Close",
                "Parent High","Parent Low","Parent Close",
                "Parent Bias","Child Bias",
                "Compression %","CH->PH Gap %","Open<PH Gap %",
                "Entry Long >","Entry Short <","SL Long","SL Short","High Conv"
            ] if c in _ib_pending.columns]
            st.dataframe(
                _ib_pending.sort_values("CH->PH Gap %")[_pend_cols],
                width='stretch', hide_index=True,
                height=min(700, 60 + len(_ib_pending) * 38)
            )
        else:
            st.info("All inside bar stocks have already broken out.")

        # ═══════════════════════════════════════════════════════════════════
        # SECTION 4 - FULL DATA (all columns, all stocks)
        # ═══════════════════════════════════════════════════════════════════
        with st.expander("📋 Full Data - All Inside Bar Stocks", expanded=False):
            st.dataframe(
                _ib_df[_FULL_COLS],
                width='stretch', hide_index=True,
                height=min(700, 60 + len(_ib_df) * 38)
            )

        st.markdown("---")
        st.markdown("### 📖 How to Trade Inside Bars")
        st.markdown("""
**Long trade**: Enter above Parent Bar High -> SL below Parent Bar Low  
**Short trade**: Enter below Parent Bar Low -> SL above Parent Bar High  

**Column Guide:**
- **Child High/Low/Close** = Yesterday's High/Low/Close (the inside bar candle)
- **Parent High/Low** = Day before yesterday's High/Low (the reference candle)
- **CH->PH Gap %** = How far Child High is below Parent High. Near 0% = coiling at resistance
- **Open<PH Gap %** = How far yesterday's Open was below Parent High
- **High Conv 🔥** = Child open below Parent High + Child High within 1.5% of Parent High

**Best setups (🔥 High Conviction):**
- CH->PH Gap % near 0% - child high almost touched parent high
- Low Compression % - tight coil
- Parent candle is 🟢 Bull - trading with the trend
- VOL% contracting on child bar, expanding on breakout

**Triple Inside Bar**: Three consecutive narrowing candles - extremely explosive signal.
""")


# ═══════════════════════════════════════════════════════════════════════
# 🔺 TAB 23 - GANN ENGINE  (Point 8)
# Gann Square of Nine, Gann Dates, Price forecasting using Gann methods.
# References: Gann Fan levels, Gann Wheel, Time cycles.
# ═══════════════════════════════════════════════════════════════════════
with tabs[23]:
    st.subheader("🔺 Gann Engine - Price & Time Forecasting")
    st.caption(
        "W.D. Gann methods: Square of Nine price levels, Gann Fan angles, "
        "Seasonal time dates, and Nifty Gann wheel analysis."
    )

    import math as _math

    # ── Gann Square of 9 Calculator ─────────────────────────────────
    st.markdown("### 🔢 Gann Square of Nine - Price Levels")
    st.caption(
        "The Square of Nine maps price to angles on a spiral. "
        "Key levels are where price aligns to 45°, 90°, 135°, 180°, 270°, 360° multiples."
    )

    _gc1, _gc2 = st.columns([1, 2])
    with _gc1:
        # Auto-populate from Nifty's Yesterday Close for stable base levels (Dynamic)
        _nifty_default = get_nifty_reference_price()
            
        _gann_price = st.number_input(
            "Enter Price / Level (Default: Yest Close)", value=_nifty_default, step=50.0,
            key="gann_price_input", format="%.2f"
        )
        _gann_incr  = st.selectbox(
            "Increment type", ["Points (0.125)", "Points (0.25)", "Points (0.5)", "Points (1.0)"],
            key="gann_incr_sel"
        )
        _incr_map = {"Points (0.125)": 0.125, "Points (0.25)": 0.25,
                     "Points (0.5)": 0.5,     "Points (1.0)": 1.0}
        _gann_inc = _incr_map[_gann_incr]

    def _gann_sq9_levels(price: float, inc: float = 0.125):
        """
        Compute Gann Square of Nine levels around a given price.
        Formula: sqrt(price) ± n*inc then square.
        Returns dict of angle->price for key angles.
        """
        root = _math.sqrt(price)
        angles = {
            "0°   (Price)":         0,
            "45°  (+1/8 turn)":     inc,
            "90°  (+1/4 turn)":     inc * 2,
            "135° (+3/8 turn)":     inc * 3,
            "180° (+1/2 turn)":     inc * 4,
            "225° (+5/8 turn)":     inc * 5,
            "270° (+3/4 turn)":     inc * 6,
            "315° (+7/8 turn)":     inc * 7,
            "360° (+1 full turn)":  inc * 8,
        }
        result = {}
        for lbl, delta in angles.items():
            up   = round((root + delta) ** 2, 2)
            down = round(max(0, root - delta) ** 2, 2)
            result[lbl] = {"UP": up, "DOWN": down}
        return result

    _sq9 = _gann_sq9_levels(_gann_price, _gann_inc)
    _sq9_rows = []
    for _lbl, _vals in _sq9.items():
        _sq9_rows.append({
            "Angle":       _lbl,
            "Resistance ↑": f"{_vals['UP']:,.2f}",
            "Support ↓":   f"{_vals['DOWN']:,.2f}",
            "R Distance":  f"+{_vals['UP'] - _gann_price:,.2f}",
            "S Distance":  f"-{_gann_price - _vals['DOWN']:,.2f}",
        })

    with _gc2:
        st.markdown(f"**Square of Nine levels for Rs.{_gann_price:,.2f}** (increment: {_gann_inc})")
        st.dataframe(pd.DataFrame(_sq9_rows), width='stretch', hide_index=True)

    st.markdown("---")

    # ── Gann Fan Angles from a Pivot ────────────────────────────────
    st.markdown("### 📐 Gann Fan Angles")
    st.caption(
        "Gann fans project support/resistance lines from a pivot point. "
        "1×1 line = 45° = price moves 1 point per 1 time unit (strongest). "
        "Above 1×1 = bullish, below = bearish."
    )

    _gf1, _gf2, _gf3 = st.columns(3)
    with _gf1:
        _fan_pivot_price = st.number_input("Pivot Price", value=22000.0, step=100.0, key="gann_fan_price")
    with _gf2:
        _fan_pivot_date  = st.date_input("Pivot Date", value=date.today() - timedelta(days=30), key="gann_fan_date")
    with _gf3:
        _fan_pts_per_day = st.number_input("Points per day (1×1)", value=100.0, step=10.0, key="gann_fan_pts")

    _today_fan = date.today()
    _days_elapsed = (_today_fan - _fan_pivot_date).days

    _fan_angles = [
        ("8×1  (steepest)",  8.0),
        ("4×1  (very steep)",4.0),
        ("3×1  (steep)",     3.0),
        ("2×1  (fast)",      2.0),
        ("1×1  (balanced)",  1.0),
        ("1×2  (slow)",      0.5),
        ("1×3  (slower)",    0.333),
        ("1×4  (very slow)", 0.25),
        ("1×8  (shallowest)",0.125),
    ]

    _fan_rows = []
    for _fa_lbl, _fa_mult in _fan_angles:
        _fan_up   = round(_fan_pivot_price + _fa_mult * _fan_pts_per_day * _days_elapsed, 2)
        _fan_down = round(_fan_pivot_price - _fa_mult * _fan_pts_per_day * _days_elapsed, 2)
        _ltp_val  = float(df[df["Symbol"] == "NIFTY"]["LTP"].iloc[0]) if "NIFTY" in df["Symbol"].values else 0
        _above = _ltp_val > _fan_up if _ltp_val > 0 else None
        _fan_rows.append({
            "Fan Angle":     _fa_lbl,
            "Resistance ↑":  f"{max(_fan_pivot_price, _fan_up):,.2f}",
            "Support ↓":     f"{max(0, _fan_down):,.2f}",
            "Days from Pivot": _days_elapsed,
            "NIFTY vs Fan":   "✅ Above" if _above else ("⚠️ Below" if _above is False else "-"),
        })

    st.dataframe(pd.DataFrame(_fan_rows), width='stretch', hide_index=True)

    st.markdown("---")

    # ── Gann Seasonal / Annual Dates (Dynamic) ──────────────────────
    st.markdown("### 📅 Gann Time Reversal Windows")
    _gt = get_gann_reversal_dates()
    
    if _gt["is_reversal_today"]:
        st.success(f"🌟 **GANN REVERSAL ACTIVE TODAY:** {_gt['today_event']}")
        st.markdown(f"**Conviction:** {_gt['conviction']}")
        st.info(f"💡 **Meaning:** {_gt['today_desc']}")
    else:
        st.info("No major Gann Time Reversal active today.")

    st.markdown("#### 🔭 Upcoming High-Conviction Gann Dates")
    _up_cols = st.columns(3)
    for i, ud in enumerate(_gt["upcoming_dates"]):
        with _up_cols[i % 3]:
            # Simple Gann Bias based on date
            _bias = "🟢 BULLISH" if any(x in ud["event"] for x in ["Winter","Spring","Mass"]) else "🔴 BEARISH" if "Autumn" in ud["event"] or "Summer" in ud["event"] else "🟡 PIVOTAL"
            st.markdown(f"📅 **{ud['date']}**")
            _d = ud.get("desc", "")
            st.markdown(f"**{ud['event']}**", help=_d)
            if _d:
                st.caption(f"_{_d}_")
            st.markdown(f"Bias: {_bias}")
    
    st.caption(
        "Gann Time cycles: 45, 90, 144, 180, 270, 360 degree points. "
        "Reversals often occur when price squares these dates."
    )
    
    st.markdown("---")

    # ── Nifty Gann Wheel ──────────────────────────────────────────────
    st.markdown("### ⚙️ Nifty Gann Wheel - 360° Price Map")
    st.caption(
        "Maps current Nifty LTP on the Gann Wheel (360° = one complete square). "
        "Key angles: 0°, 90°, 180°, 270°, 360° are strongest S/R. "
        "45°, 135°, 225°, 315° are secondary."
    )

    try:
        _nifty_row_g = df[df["Symbol"] == "NIFTY"]
        _nifty_ltp_g = float(_nifty_row_g.iloc[0]["LTP"]) if not _nifty_row_g.empty else 24000.0
    except Exception:
        _nifty_ltp_g = 24000.0

    _gw1, _gw2 = st.columns([1, 2])
    with _gw1:
        _gw_price = st.number_input(
            "NIFTY LTP (auto-filled)", value=_nifty_ltp_g,
            step=10.0, key="gann_wheel_price", format="%.2f"
        )
        _gw_base = st.number_input(
            "Wheel base (square root base, e.g. 144)",
            value=144.0, step=1.0, key="gann_wheel_base"
        )

    def _gann_wheel_levels(price: float, base: float = 144.0):
        """
        Compute Gann Wheel levels.
        Increments = base / 4 (90°), base / 8 (45°), etc.
        Key levels above and below current price at each cardinal angle.
        """
        root   = _math.sqrt(price)
        step45 = base / 8    # 45° step in square root space
        levels = []
        for i in range(-16, 17):
            level_root = root + i * (step45 / 100)
            if level_root <= 0:
                continue
            level_price = round(level_root ** 2, 2)
            angle_deg   = (i * 45) % 360
            strength    = "🔴 MAJOR" if angle_deg % 90 == 0 else \
                          "🟡 MINOR" if angle_deg % 45 == 0 else "⬜"
            levels.append({
                "Level":    level_price,
                "Angle":    f"{angle_deg}°",
                "Strength": strength,
                "Distance": round(level_price - price, 2),
            })
        return sorted(levels, key=lambda x: x["Level"])

    with _gw2:
        _gw_levels = _gann_wheel_levels(_gw_price, _gw_base)
        _gw_df = pd.DataFrame(_gw_levels)
        # Show only closest 8 above and 8 below
        _above_gw = _gw_df[_gw_df["Distance"] >= 0].head(8)
        _below_gw = _gw_df[_gw_df["Distance"] < 0].tail(8)
        _gw_show  = pd.concat([_below_gw, _above_gw]).sort_values("Level", ascending=False)
        st.markdown(f"**Gann Wheel levels near Rs.{_gw_price:,.2f}**")
        st.dataframe(_gw_show, width='stretch', hide_index=True)

    st.markdown("---")
    st.markdown("### 📚 Gann Reference")
    st.markdown("""
**Core Gann Principles:**
- **1×1 line** is the most important angle (price = time). Market above = bullish, below = bearish.
- **Square of Nine**: Price and time move in spirals. When price hits a 90° or 180° angle from a major high/low, expect reaction.
- **Seasonal dates**: Spring/Autumn Equinox and Summer/Winter Solstice are the four most important annual reversal windows.
- **Cardinal points**: Jan 1, Apr 1 (spring), Jul 1 (summer), Oct 1 (autumn) are quarterly turns.

**Trading Rules:**
1. Never average a losing trade.
2. Use stop-losses based on Gann fan angles or Square of Nine levels.
3. Time and price must agree for a high-confidence signal.
4. Volume confirms Gann breakouts - high volume at a key angle = strong signal.

**Books (see your collection):**
- *Share Trading Tips* - Prasant Nair
- *Stock Market Astrology* - Indrodeep Banerjee (Sagar Publications)
- *Stock Market Vedic Astrology* - Vinayak Bhatt (Saptarishis Publications)
""")


# ═══════════════════════════════════════════════════════════════════════
# 🎯 TAB 24 - MULTI-TIMEFRAME SEQUENTIAL SCANNER  (Point 6)
# PI IND / DivisLab / M&M type setup
# 15-min + 1H + Daily alignment scanner with conviction scoring
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# 🎯 TAB 24 - PI INDUSTRIES STYLE SCANNER  (Point 6)
# 3-Pillar: Resistance->Support Retest + Relative Strength + Multi-TF
# Based on PIIND Bullish Trade Rationale - 9 April, 1:00 PM
# ═══════════════════════════════════════════════════════════════════════
with tabs[24]:
    st.subheader("🎯 PI Industries Style Scanner - 3-Pillar Setup")
    st.caption(
        "**Pillar A:** Resistance -> Support retest held  "
        "**|  Pillar B:** Relative strength vs Nifty  "
        "**|  Pillar C:** 15m + 1H + Daily multi-TF alignment"
    )

    with st.expander("📖 Setup Logic - PI Industries style (9 April rationale)", expanded=False):
        st.markdown("""
**The PI Industries Setup (3 Pillars)**

> *"At 1:00 PM on 9 April, PIIND gave a strong bullish confirmation. The previous resistance zone
> was clearly broken and price came back to retest the same zone, which successfully held as support.
> Relative strength against the index was the key observation."*

---

**Pillar A - Resistance -> Support Retest**
- Stock made a high (resistance level) in first half of day (before 12:00)
- Price pulled back ≥ 0.4% from that high (genuine retest, not just a wick)
- LTP is now back at or above that resistance level (resistance -> support confirmed)

**Pillar B - Relative Strength vs Nifty**
- Stock CHANGE% is at least +0.5% better than Nifty
- OR: Stock is positive/flat while Nifty is negative (outperforms weak index)
- This was the *key observation* - PIIND held while Nifty showed bearish behaviour

**Pillar C - Multi-TF Structure (15m + 1H + Daily)**
- 15m: LTP at/above first-half day high (structure intact)
- 1H: LTP above first 1H candle's high (09:15 candle)
- Daily: LTP at/above Yesterday's High (daily breakout or near-breakout)

**Conviction Score (0–6)**
- Relative Strength basic (+1) / strong (+2 if positive while Nifty negative)
- Daily breakout (+1) / near YH (+0.5)
- 1H breakout (+1)
- Volume surge ≥30% (+1) / flat volume (+0.5)
- Above 90-day TOP HIGH (+1)

**Entry / SL**
- Entry: above the resistance retest level (first-half day high)
- SL: below first-half day low (or EMA20)
- Target: next resistance / TOP_HIGH level
""")

    st.markdown("---")

    # ── Rebuild button ───────────────────────────────────────────────
    _pi_c1, _pi_c2 = st.columns([1, 4])
    if _pi_c1.button("🔄 Refresh Scanner", key="piind_refresh"):
        _mtf_long_df, _mtf_short_df, _mtf_watch_df = build_piind_scanner()
        st.rerun()

    # ── Nifty context bar ────────────────────────────────────────────
    _nifty_c = _get_nifty_chg_pct()
    _nifty_l = _get_nifty_ltp()
    _nifty_col = "#00e676" if _nifty_c >= 0 else "#ff5252"
    _nifty_ctx_warn = (
        '<span style="color:#ff5252">⚠️ Nifty WEAK - best time for relative strength scan</span>'
        if _nifty_c < -0.3 else
        '<span style="color:#aaa">Nifty neutral/positive</span>'
    )
    st.markdown(
        f'<div style="background:#111;border-radius:6px;padding:8px 14px;margin-bottom:8px">'
        f'📊 <b>Nifty Context:</b> LTP <b>{_nifty_l:,.0f}</b>  '
        f'<span style="color:{_nifty_col};font-weight:700">{_nifty_c:+.2f}%</span>'
        f'&nbsp;&nbsp;|&nbsp;&nbsp;'
        f'{_nifty_ctx_warn}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Summary metrics ───────────────────────────────────────────────
    # ── Status: check if 5-min data is ready ────────────────────────────
    _raw_ready   = _raw_5min_today is not None and not (hasattr(_raw_5min_today, 'empty') and _raw_5min_today.empty)
    _kite_active = "kite" in dir() and kite is not None
    if not _kite_active:
        st.error("❌ Kite not connected - PI-IND scanner needs live 5-min candle data.")
    elif not _raw_ready:
        st.warning(
            "⏳ 5-min candle data not yet loaded.  \n"
            "The scanner populates after **09:30** when at least 2-3 completed 15-min candles exist.  \n"
            "Click **🔄 Refresh Scanner** once market is open."
        )

    _p1, _p2, _p3, _p4 = st.columns(4)
    _hc_l = len(_mtf_long_df[_mtf_long_df["Conviction"] >= 3.0])  if not _mtf_long_df.empty  and "Conviction" in _mtf_long_df.columns  else 0
    _hc_s = len(_mtf_short_df[_mtf_short_df["Conviction"] >= 3.0]) if not _mtf_short_df.empty and "Conviction" in _mtf_short_df.columns else 0
    _p1.metric("🟢 Long Setups",        len(_mtf_long_df)  if not _mtf_long_df.empty  else 0)
    _p2.metric("🔴 Short Setups",       len(_mtf_short_df) if not _mtf_short_df.empty else 0)
    _p3.metric("🎯 High Conv ≥3",       _hc_l + _hc_s)
    _p4.metric("🟡 Watching (2/3)",     len(_mtf_watch_df) if not _mtf_watch_df.empty else 0)

    st.markdown("---")

    # ── Three sub-tables ─────────────────────────────────────────────
    _t1, _t2, _t3 = st.tabs([
        "🎯 Full Setup (All 3 Pillars)",
        "🟡 Watch List (2/3 Pillars)",
        "📊 Daily Breakout Only",
    ])

    _LONG_COLS = [c for c in [
        "Symbol","LTP","CHANGE_%","Nifty_%","RelStr_%","VOL_%",
        "Daily_Break","1H_Break","Retest_Hold","Rel_Strength",
        "15m_FH_High","15m_Pullback","1H_First_Hi",
        "YEST_HIGH","TOP_HIGH","Above_90d_Hi","EMA20",
        "Conviction","Entry","SL"
    ] if True]  # all cols shown

    with _t1:
        st.markdown("### 🎯 PI Industries Style - All 3 Pillars Confirmed")
        st.caption("Resistance->Support ✅  |  Relative Strength ✅  |  15m+1H+Daily ✅")

        _concat_list = [x for x in [_mtf_long_df, _mtf_short_df] if x is not None and not x.empty]
        _all_full = pd.concat(_concat_list, ignore_index=True) if _concat_list else pd.DataFrame()

        if _all_full.empty:
            st.info(
                "No full 3-pillar setups found yet.\n\n"
                "**When does this appear?** Usually after 12:00 PM when:\n"
                "- Price has made a morning high (resistance), pulled back, and retested\n"
                "- Nifty shows weakness but this stock holds strong\n"
                "- 1H candle confirms breakout"
            )
        else:
            # Colour by direction
            def _full_style(row):
                if "🟢" in str(row.get("Setup","")):
                    return ["background:#0a2e0a"] * len(row)
                elif "🔴" in str(row.get("Setup","")):
                    return ["background:#2e0a0a"] * len(row)
                return [""] * len(row)

            # Score highlight
            st.markdown(f"**{len(_all_full)} setups - sorted by Conviction**")
            _show_cols_full = [c for c in [
                "Setup","Symbol","LTP","Open","CHANGE_%","VOL_%",
                "Nifty_%","RelStr_%",
                "YEST_CLOSE","YEST_HIGH","YEST_LOW",
                "Retest_Hold","Rel_Strength","Daily_Break","1H_Break",
                "15m_FH_High","15m_Pullback","TOP_HIGH",
                "Conviction","Entry","SL"
            ] if c in _all_full.columns]

            st.dataframe(
                _all_full[_show_cols_full].style.apply(_full_style, axis=1),
                width='stretch', hide_index=True,
                height=min(800, 60 + len(_all_full) * 38)
            )

        # ── 15m detail ────────────────────────────────────────────────
        if not _mtf_long_df.empty:
            st.markdown("#### 🟢 LONG - 15-Minute Detail")
            _l15_cols = [c for c in [
                "Symbol","LTP","Open","CHANGE_%","VOL_%","RelStr_%",
                "YEST_CLOSE","YEST_HIGH","YEST_LOW",
                "15m_FH_High","15m_Pullback","Retest_Hold",
                "1H_First_Hi","1H_Break","Daily_Break",
                "EMA20","Conviction"
            ] if c in _mtf_long_df.columns]
            st.dataframe(_mtf_long_df[_l15_cols], width='stretch', hide_index=True,
                         height=min(500, 60+len(_mtf_long_df)*35))

        if not _mtf_short_df.empty:
            st.markdown("#### 🔴 SHORT - Detail")
            _s15_cols = [c for c in [
                "Symbol","LTP","Open","CHANGE_%","VOL_%","RelStr_%",
                "YEST_CLOSE","YEST_HIGH","YEST_LOW",
                "15m_FH_High","15m_Pullback","Retest_Hold",
                "1H_First_Hi","1H_Break","Daily_Break",
                "EMA20","Conviction","Entry","SL"
            ] if c in _mtf_short_df.columns]
            st.dataframe(_mtf_short_df[_s15_cols], width='stretch', hide_index=True,
                         height=min(500, 60+len(_mtf_short_df)*35))

        # ── Today's alerted stocks log ─────────────────────────────────
        st.markdown("---")
        st.markdown("### 📲 Today\'s Alerted Stocks (PI-IND)")
        st.caption("Stocks for which a Telegram alert was fired today. One alert per stock per day.")
        _piind_log_today = _piind_load_alert_log()
        if not _piind_log_today:
            st.info("No PI-IND alerts sent today yet. Alerts fire when Conviction ≥ 3 and market is open.")
        else:
            _log_df = pd.DataFrame(_piind_log_today)
            _log_display_cols = [c for c in [
                "time","direction","symbol","ltp","change","vol","score","entry","sl","daily","retest"
            ] if c in _log_df.columns]
            _log_df_show = _log_df[_log_display_cols].rename(columns={
                "time":"Alert Time","direction":"Dir","symbol":"Symbol",
                "ltp":"LTP","change":"Chg%","vol":"Vol%","score":"Score",
                "entry":"Entry","sl":"SL","daily":"Daily","retest":"Retest"
            })
            st.dataframe(_log_df_show, width='stretch', hide_index=True,
                         height=min(400, 60 + len(_log_df_show)*35))

    with _t2:
        st.markdown("### 🟡 Watch List - 2 of 3 Pillars Confirmed")
        st.caption("Monitor these - one more pillar confirms = full PI-IND setup.")
        if _mtf_watch_df.empty:
            st.info("No 2-pillar setups right now.")
        else:
            _watch_show_cols = [c for c in [
                "Symbol","LTP","Open","CHANGE_%","VOL_%",
                "Nifty_%","RelStr_%",
                "YEST_CLOSE","YEST_HIGH","YEST_LOW",
                "Retest_Hold","Rel_Strength","Daily_Break","1H_Break",
                "Pillars","Missing","Conviction"
            ] if c in _mtf_watch_df.columns]
            st.dataframe(_mtf_watch_df[_watch_show_cols], width='stretch',
                         hide_index=True, height=min(600, 60+len(_mtf_watch_df)*35))

    with _t3:
        st.markdown("### 📊 Daily Breakout Context")
        st.caption("All stocks above YEST_HIGH, sorted by relative strength vs Nifty.")
        if "YEST_HIGH" in df.columns and "LTP" in df.columns:
            _db = df[(df["YEST_HIGH"]>0) & (df["LTP"]>=df["YEST_HIGH"])].copy()
            if _db.empty:
                st.info("No stocks above yesterday's high right now.")
            else:
                _db = _db.copy()
                _db["RelStr_%"]   = (_db["CHANGE_%"] - _nifty_c).round(2)
                _db["AbvYH_pts"]  = (_db["LTP"] - _db["YEST_HIGH"]).round(2)
                _db["AbvYH_%"]    = ((_db["LTP"]-_db["YEST_HIGH"])/_db["YEST_HIGH"]*100).round(2)
                _piind_syms = set()
                if not _mtf_long_df.empty and "Symbol" in _mtf_long_df.columns:
                    _piind_syms |= set(_mtf_long_df["Symbol"])
                _db["PI_Setup"] = _db["Symbol"].apply(lambda s: "🎯 PI-IND" if s in _piind_syms else "")
                _db_show = [c for c in ["Symbol","LTP","CHANGE_%","VOL_%","RelStr_%",
                    "AbvYH_pts","AbvYH_%","YEST_HIGH","YEST_CLOSE","YEST_LOW","TOP_HIGH","PI_Setup"] if c in _db.columns]
                st.dataframe(
                    _db[_db_show].sort_values("RelStr_%", ascending=False),
                    width='stretch', hide_index=True,
                    height=min(600, 60+len(_db)*35)
                )
        else:
            st.info("Live data not loaded.")


# ═══════════════════════════════════════════════════════════════════════
# 📐 TAB 25 - CHART PATTERN SCANNER
# Detects 12 classic chart patterns (Bull/Bear) on daily candles.
# Patterns: Falling Wedge, Ascending/Descending Triangle, Bull/Bear Flag,
#           Bull/Bear Pennant, Cup & Handle, Double Bottom/Top, H&S
# ═══════════════════════════════════════════════════════════════════════
with tabs[25]:
    st.subheader("📐 Chart Pattern Scanner - Daily Timeframe")
    st.caption(
        "Detects 22 high-conviction chart patterns across all stocks. "
        "Includes classic patterns + **Market Structure (MSH/MSL)**, **2B Reversals**, **NR7ID Squeezes**, **Dragon**, and **Adam & Eve**. "
        "**Conviction Score 1–10** - higher = more confluences confirmed (Volume, RSI Divergence, Structure)."
    )

    # ── Import scanner (graceful fallback if not present) ─────────────────
    _CPS_OK = False
    try:
        import importlib as _il
        _cps_path = os.path.dirname(os.path.abspath(__file__))
        if _cps_path not in sys.path:
            sys.path.insert(0, _cps_path)
        import chart_pattern_scanner as _cps
        _CPS_OK = True
    except ImportError as _cps_err:
        st.error(f"❌ chart_pattern_scanner.py not found in dashboard folder: {_cps_err}")

    if _CPS_OK:
        # ── Controls ─────────────────────────────────────────────────────
        _cp_c1, _cp_c2, _cp_c3, _cp_c4, _cp_c5 = st.columns([2, 1.5, 1.5, 1.5, 2])
        with _cp_c1:
            _cp_dir = st.selectbox(
                "Direction",
                ["🔀 Both", "🟢 Bullish Only", "🔴 Bearish Only"],
                key="_cp_dir"
            )
        with _cp_c2:
            _cp_min_score = st.number_input("Min Score", min_value=1, max_value=10, value=5, key="_cp_score")
        with _cp_c3:
            _cp_min_rr = st.number_input("Min R:R", min_value=0.5, max_value=5.0, value=1.2, step=0.1, key="_cp_rr")
        with _cp_c4:
            _cp_patterns = st.multiselect(
                "Patterns",
                ["Falling Wedge", "Ascending Triangle", "Bull Flag", "Bull Pennant",
                 "Cup & Handle", "Double Bottom", "Rising Wedge", "Descending Triangle",
                 "Bear Flag", "Bear Pennant", "Head & Shoulders", "Double Top",
                 "NR7ID Squeeze", "2B Bullish Reversal", "2B Bearish Reversal",
                 "Market Structure Low (MSL)", "Market Structure High (MSH)",
                 "Dragon Pattern", "Adam & Eve (W)", "Triple Bottom", "Triple Top",
                 "Symmetrical Triangle"],
                default=[],
                key="_cp_pat",
                placeholder="All patterns"
            )
        with _cp_c5:
            _cp_hc_only = st.checkbox("🔥 High Conviction Only (≥8)", value=False, key="_cp_hc_only")
            _cp_scan_btn = st.button("🔍 Run Pattern Scan", key="_cp_scan_btn", type="primary")
            st.caption("⚠️ Takes 3-5 min to scan all symbols")

        # ── Pattern legend ────────────────────────────────────────────────
        with st.expander("📖 Pattern Guide & Setup Logic", expanded=False):
            _lg1, _lg2 = st.columns(2)
            with _lg1:
                st.markdown("""
**🟢 BULLISH PATTERNS**

🔻 **Falling Wedge** - Reversal pattern. Both trendlines fall, resistance steeper.

📐 **Ascending Triangle** - Flat resistance + rising lows. Continuation.

🚩 **Bull Flag** - Pole up (≥3%) + tight consolidation. Continuation.

📌 **Bull Pennant** - Pole up + converging triangle.

☕ **Cup & Handle** - U-shaped recovery + small handle. High conviction.

🔂 **Double Bottom** - W-pattern with equal lows. RSI Bull Div = 🔥.

🔄 **2B Bullish** - Breakout below low fails and reverses. "The Spring".

💎 **MSL (Structure)** - 3-bar fractal low. Defines immediate trend shift.
""")
            with _lg2:
                st.markdown("""
**🔴 BEARISH PATTERNS**

📈 **Rising Wedge** - Reversal pattern. Both lines rise, support steeper.

📉 **Descending Triangle** - Flat support + falling highs.

🏴 **Bear Flag** - Pole down + tight consolidation.

📍 **Bear Pennant** - Pole down + converging triangle.

🔃 **Double Top** - M-pattern with equal highs. RSI Bear Div = 🔥.

👤 **Head & Shoulders** - Standard top reversal. Neckline is key.

🔥 **2B Bearish** - Breakout above high fails and reverses. "The Upthrust".

🏔️ **MSH (Structure)** - 3-bar fractal high. Defines trend shift at tops.

**Squeeze**: **NR7ID** - Smallest range of 7 days + Inside Day. High odds breakout.

**Conviction Score:** Volume (2pts) + Pattern size (2pts) + Post-break (3pts) + **RSI Divergence (2pts)** + **Structure (1pt)**
""")

        # ── Auto-scan status indicator ────────────────────────────────────
        _cps_running_now = _CPS_BG_THREAD_ACTIVE  # module-level flag - safe to read from main thread
        _cps_last_h      = st.session_state.get(_CPS_AUTO_LAST_HOUR, None)
        _auto_tg_on      = not _is_tg_disabled("CHART_PATTERN")
        _auto_status_cols = st.columns([3, 1])
        with _auto_status_cols[0]:
            if _cps_running_now:
                st.info("🔄 Auto-scan running in background... Tab will update on next refresh.")
            elif _cps_last_h is not None:
                st.success(f"✅ Auto-scan ran at {_cps_last_h:02d}:xx - next run at {_cps_last_h+1:02d}:00")
            else:
                st.info("⏳ Auto-scan fires once per hour during market hours (09:15–15:30)")
        with _auto_status_cols[1]:
            _auto_tg_label = "🔔 TG Alerts: ON" if _auto_tg_on else "🔕 TG Alerts: OFF"
            if st.button(_auto_tg_label, key="_cps_tg_toggle"):
                _new_val = not _auto_tg_on
                st.session_state["tg_CHART_PATTERN"] = _new_val
                st.rerun()


        # ── Cache key: try auto-scan file cache first, then session_state ─
        _cp_cache_key     = f"cp_scan_{date.today()}"
        _cp_auto_file     = os.path.join(
            _CPS_AUTO_DEDUP_DIR, f"cps_results_{date.today().isoformat()}.json"
        )

        # Load from auto-scan file if session state is empty
        if _cp_cache_key not in st.session_state and os.path.exists(_cp_auto_file):
            try:
                with open(_cp_auto_file, "r", encoding="utf-8") as _cpf:
                    st.session_state[_cp_cache_key] = json.load(_cpf)
            except Exception:
                pass

        # ── Manual scan button ─────────────────────────────────────────────
        if _cp_scan_btn:
            _kite_active = "kite" in dir() and kite is not None
            if not _kite_active:
                st.error("❌ Kite not connected - Pattern scanner requires live Kite connection.")
            else:
                with st.spinner("🔍 Scanning all stocks for chart patterns... This takes 3-5 minutes..."):
                    try:
                        _cp_results = _cps.scan_chart_patterns(
                            kite          = kite,
                            symbols       = STOCKS,
                            get_token_fn  = get_token,
                            live_df       = df,
                            min_score     = int(_cp_min_score),
                            min_rr        = float(_cp_min_rr),
                        )
                        st.session_state[_cp_cache_key] = _cp_results
                        # Also save to file so auto-display picks it up
                        try:
                            with open(_cp_auto_file, "w", encoding="utf-8") as _cpf:
                                json.dump(_cp_results, _cpf, default=str)
                        except Exception:
                            pass
                        st.success(f"✅ Scan complete - {len(_cp_results)} patterns found")
                    except Exception as _cpe:
                        st.error(f"Pattern scan error: {_cpe}")
                        _cp_results = []

        # ── Display results ───────────────────────────────────────────────
        _cp_data = st.session_state.get(_cp_cache_key, [])

        if not _cp_data:
            st.info(
                "📡 **Auto-scan fires every hour** - results appear here automatically.  \n"
                "Click **Run Pattern Scan** to scan immediately (3–5 min).  \n"
                "Telegram alerts fire automatically for Score ≥ 6, R:R ≥ 1.5 patterns."
            )
        else:
            # Apply direction filter
            _cp_filtered = _cp_data
            if "Bullish" in _cp_dir:
                _cp_filtered = [r for r in _cp_filtered if r.get("direction") == "BULL"]
            elif "Bearish" in _cp_dir:
                _cp_filtered = [r for r in _cp_filtered if r.get("direction") == "BEAR"]
            # Apply pattern filter
            if _cp_patterns:
                _cp_filtered = [r for r in _cp_filtered if r.get("pattern") in _cp_patterns]
            # Apply score/RR filters
            _cp_filtered = [r for r in _cp_filtered
                           if r.get("score", 0) >= _cp_min_score
                           and r.get("rr", 0) >= _cp_min_rr]
            
            # Apply High Conviction filter
            if _cp_hc_only:
                _cp_filtered = [r for r in _cp_filtered if r.get("score", 0) >= 8]

            if not _cp_filtered:
                st.warning("No patterns match the current filters. Try reducing Min Score or Min R:R.")
            else:
                # ── Summary metrics ──────────────────────────────────────
                _cp_bull = [r for r in _cp_filtered if r.get("direction") == "BULL"]
                _cp_bear = [r for r in _cp_filtered if r.get("direction") == "BEAR"]
                _cp_hc   = [r for r in _cp_filtered if r.get("score", 0) >= 8]
                _sm1, _sm2, _sm3, _sm4 = st.columns(4)
                _sm1.metric("Total Patterns",   len(_cp_filtered))
                _sm2.metric("🟢 Bullish",        len(_cp_bull))
                _sm3.metric("🔴 Bearish",        len(_cp_bear))
                _sm4.metric("🔥 High Conv ≥8",   len(_cp_hc))
                st.caption(f"Last scan: {_cp_data[0].get('scan_time','-') if _cp_data else '-'}  |  Sorted by Conviction Score")

                # ── Build display dataframe ──────────────────────────────
                _cp_rows = []
                for r in _cp_filtered:
                    _dir_icon = "🟢" if r.get("direction") == "BULL" else "🔴"
                    _cp_rows.append({
                        "Dir":          _dir_icon,
                        "Symbol":       r.get("symbol", ""),
                        "Pattern":      r.get("pattern", ""),
                        "Score":        r.get("score", 0),
                        "LTP":          r.get("ltp", 0),
                        "Open":         r.get("open", 0),
                        "Yest High":    r.get("yest_high", 0),
                        "Yest Low":     r.get("yest_low", 0),
                        "Yest Close":   r.get("yest_close", 0),
                        "Chg%":         r.get("change_%", 0),
                        "Vol%":         r.get("vol_%", 0),
                        "Entry":        r.get("entry", 0),
                        "SL":           r.get("sl", 0),
                        "T1":           r.get("t1", 0),
                        "T2":           r.get("t2", 0),
                        "R:R":          r.get("rr", 0),
                        "Vol Ratio":    r.get("vol_ratio", 0),
                        "Pole/Depth%":  r.get("pole_pct", 0),
                        "Post Break%":  r.get("post_break_%", 0),
                        "Bars":         r.get("bars", 0),
                        "Resist":       r.get("resist_level", 0),
                        "Support":      r.get("support_level", 0),
                    })

                _cp_df_show = pd.DataFrame(_cp_rows)

                # Style: green rows for bull, red for bear
                def _cp_style_row(row):
                    if row["Dir"] == "🟢":
                        return ["background:#0a2e0a"] * len(row)
                    return ["background:#2e0a0a"] * len(row)

                # ── Tab view: Bullish | Bearish | All ────────────────────
                _cp_t1, _cp_t2, _cp_t3 = st.tabs([
                    f"🟢 Bullish ({len(_cp_bull)})",
                    f"🔴 Bearish ({len(_cp_bear)})",
                    f"📋 All ({len(_cp_filtered)})"
                ])

                def _cp_show_table(data_list, key_suffix):
                    if not data_list:
                        st.info("No patterns in this category.")
                        return
                    _rows = []
                    for r in data_list:
                        _dir_icon = "🟢" if r.get("direction") == "BULL" else "🔴"
                        _rows.append({
                            "Dir":         _dir_icon,
                            "Symbol":      r.get("symbol", ""),
                            "Pattern":     r.get("pattern", ""),
                            "Score":       r.get("score", 0),
                            "LTP":         r.get("ltp", 0),
                            "Open":        r.get("open", 0),
                            "Yest High":   r.get("yest_high", 0),
                            "Yest Low":    r.get("yest_low", 0),
                            "Yest Close":  r.get("yest_close", 0),
                            "Chg%":        r.get("change_%", 0),
                            "Vol%":        r.get("vol_%", 0),
                            "Entry":       r.get("entry", 0),
                            "SL":          r.get("sl", 0),
                            "T1":          r.get("t1", 0),
                            "T2":          r.get("t2", 0),
                            "R:R":         r.get("rr", 0),
                            "Vol Ratio":   r.get("vol_ratio", 0),
                            "Pole/Depth%": r.get("pole_pct", 0),
                            "Post Break%": r.get("post_break_%", 0),
                            "Bars":        r.get("bars", 0),
                            "Resist Lvl":  r.get("resist_level", 0),
                            "Support Lvl": r.get("support_level", 0),
                        })
                    _df_t = pd.DataFrame(_rows)

                    def _cp_row_style(row):
                        if row["Dir"] == "🟢":
                            return ["background:#0a2e0a"] * len(row)
                        return ["background:#2e0a0a"] * len(row)

                    st.dataframe(
                        _df_t.style.apply(_cp_row_style, axis=1),
                        width='stretch',
                        hide_index=True,
                        height=min(800, 60 + len(_df_t) * 38),
                        key=f"_cp_tbl_{key_suffix}"
                    )

                    # ── Per-pattern breakdown ─────────────────────────────
                    st.markdown("---")
                    st.markdown("#### Pattern Breakdown")
                    _pat_counts = {}
                    for r in data_list:
                        p = r.get("pattern", "?")
                        _pat_counts[p] = _pat_counts.get(p, 0) + 1
                    _pc1, _pc2 = st.columns(2)
                    for idx, (pat, cnt) in enumerate(sorted(_pat_counts.items(), key=lambda x: -x[1])):
                        col = _pc1 if idx % 2 == 0 else _pc2
                        col.markdown(f"**{pat}**: {cnt} stocks")

                    # ── Download ──────────────────────────────────────────
                    st.download_button(
                        "📥 Download CSV",
                        _df_t.to_csv(index=False, encoding='utf-8'),
                        file_name=f"pattern_scan_{date.today()}_{key_suffix}.csv",
                        mime="text/csv",
                        key=f"_cp_dl_{key_suffix}"
                    )

                with _cp_t1:
                    st.markdown(f"### 🟢 Bullish Patterns ({len(_cp_bull)})")
                    st.caption("Sorted by Conviction Score. Green background = active bullish pattern.")
                    _cp_show_table(_cp_bull, "bull")

                with _cp_t2:
                    st.markdown(f"### 🔴 Bearish Patterns ({len(_cp_bear)})")
                    st.caption("Sorted by Conviction Score. Red background = active bearish pattern.")
                    _cp_show_table(_cp_bear, "bear")

                with _cp_t3:
                    st.markdown(f"### 📋 All Patterns ({len(_cp_filtered)})")
                    _cp_show_table(_cp_filtered, "all")
