# bos_scanner.py — 1-Hour BOS / CHoCH Scanner  v2.0
# =====================================================================
# NEW in v2.0:
#   1. OB RETEST ALERT — when LTP pulls back INTO the OB zone after a
#      BOS, a second Telegram fires with the exact B-entry setup.
#   2. HOURLY HIGH/LOW BREAK ALERT — fires when LTP breaks the previous
#      completed hourly candle's high (bull) or low (bear), independent
#      of whether a full BOS was detected.
#   3. DAILY EMA FILTER — BOS and hourly-break alerts are confirmed only
#      when LTP is above EMA7 + EMA20 (bull) or below both (bear) on
#      the daily timeframe. Configurable — set REQUIRE_DAILY_EMA=False
#      to disable.
#   4. CANDLE QUALITY FILTER — the breaking candle must close in the top
#      33% of its range (bull) or bottom 33% (bear). Eliminates wicks
#      that technically cross but immediately reverse.
#   5. VOLUME SURGE FILTER — breaking candle volume must be ≥ MIN_VOL_RATIO
#      × the 10-bar average. Eliminates low-conviction breaks.
# =====================================================================

from __future__ import annotations
import json, os
from datetime import datetime, date, timedelta
from typing import Optional

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
except ImportError:
    IST = None

from ohlc_store import OHLCStore

# ── Tuneable parameters ───────────────────────────────────────────────────────
BOS_DEDUP_FILE     = "bos_dedup.json"
BOS_CACHE_FILE     = "bos_scan_cache.json"
BOS_LOOKBACK       = 40
BOS_SWING_BARS     = 2
MIN_BOS_PCT        = 0.25      # minimum % move beyond swing to count as BOS
MIN_RR             = 1.5       # minimum R:R for alert
MIN_VOL_RATIO      = 1.2       # breaking candle volume vs 10-bar average
REQUIRE_DAILY_EMA  = True      # set False to skip EMA7/20 daily filter
OB_RETEST_TOLERANCE= 0.003     # 0.3% — LTP within this % of OB high/low counts as retest
HOURLY_BREAK_PCT   = 0.05      # minimum % beyond prev-hour high/low to count as break

INDEX_SYMBOLS = {
    "NIFTY","BANKNIFTY","FINNIFTY","NIFTYIT","NIFTYFMCG",
    "NIFTYPHARMA","NIFTYMETAL","NIFTYAUTO","NIFTYENERGY","NIFTYPSUBANK"
}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — SWING POINTS
# ─────────────────────────────────────────────────────────────────────────────

def get_swing_points(candles, bars=BOS_SWING_BARS):
    n = len(candles)
    highs = [c["high"] for c in candles]
    lows  = [c["low"]  for c in candles]
    sh, sl = [], []
    for i in range(bars, n - bars):
        if highs[i] == max(highs[i - bars : i + bars + 1]):
            sh.append((i, highs[i]))
        if lows[i] == min(lows[i - bars : i + bars + 1]):
            sl.append((i, lows[i]))
    return {"swing_highs": sh, "swing_lows": sl}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — CANDLE QUALITY (close must be in top/bottom 33% of range)
# ─────────────────────────────────────────────────────────────────────────────

def _candle_quality_ok(candle, direction):
    """
    For a BULL break: close must be in top 33% of candle range.
    For a BEAR break: close must be in bottom 33% of candle range.
    This eliminates wick-only spikes that immediately reverse.
    """
    h = candle["high"]; l = candle["low"]; c = candle["close"]
    rng = h - l
    if rng <= 0:
        return True   # doji / no range — don't filter
    if direction == "UP":
        return c >= l + rng * 0.67   # closed in top third
    else:
        return c <= l + rng * 0.33   # closed in bottom third


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — DAILY EMA FILTER (fetches from Kite if available)
# ─────────────────────────────────────────────────────────────────────────────

# Cache daily EMA per symbol to avoid repeated API calls
_daily_ema_cache: dict = {}   # {symbol: {"ema7": float, "ema20": float, "date": str}}


def _calc_ema(values: list, span: int) -> float:
    """Simple EMA calculation — no pandas dependency."""
    k = 2 / (span + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return round(ema, 2)


def get_daily_ema(symbol: str, kite=None) -> dict:
    """
    Returns {"ema7": float, "ema20": float, "ltp_vs_ema": "above"/"below"/"unknown"}.
    Uses Kite historical_data if available, otherwise returns unknown.
    Result cached per symbol per day.
    """
    today_str = date.today().isoformat()
    cached = _daily_ema_cache.get(symbol)
    if cached and cached.get("date") == today_str:
        return cached

    if not kite:
        return {"ema7": 0, "ema20": 0, "ltp_vs_ema": "unknown", "date": today_str}

    try:
        from_date = date.today() - timedelta(days=60)
        # Get instrument token via NSE instruments
        nse_inst = None
        try:
            import pandas as pd
            nse_inst = pd.DataFrame(kite.instruments("NSE"))
            row = nse_inst[nse_inst["tradingsymbol"] == symbol]
            if row.empty:
                return {"ema7": 0, "ema20": 0, "ltp_vs_ema": "unknown", "date": today_str}
            token = int(row.iloc[0]["instrument_token"])
        except Exception:
            return {"ema7": 0, "ema20": 0, "ltp_vs_ema": "unknown", "date": today_str}

        bars = kite.historical_data(token, from_date, date.today(), "day")
        if len(bars) < 20:
            return {"ema7": 0, "ema20": 0, "ltp_vs_ema": "unknown", "date": today_str}

        closes = [b["close"] for b in bars]
        ema7   = _calc_ema(closes, 7)
        ema20  = _calc_ema(closes, 20)
        ltp    = closes[-1]

        ltp_vs = "above" if ltp > ema7 and ltp > ema20 else \
                 "below" if ltp < ema7 and ltp < ema20 else "mixed"

        result = {"ema7": ema7, "ema20": ema20, "ltp_vs_ema": ltp_vs, "date": today_str}
        _daily_ema_cache[symbol] = result
        return result

    except Exception as e:
        return {"ema7": 0, "ema20": 0, "ltp_vs_ema": "unknown", "date": today_str}


def _daily_ema_confirms(symbol: str, direction: str, kite=None) -> tuple[bool, str]:
    """
    Returns (confirmed: bool, note: str).
    Bull BOS: LTP must be above daily EMA7 and EMA20.
    Bear BOS: LTP must be below daily EMA7 and EMA20.
    If REQUIRE_DAILY_EMA=False or kite not available, returns (True, "EMA filter off").
    """
    if not REQUIRE_DAILY_EMA:
        return True, "EMA filter disabled"

    ema = get_daily_ema(symbol, kite)
    ltp_vs = ema.get("ltp_vs_ema", "unknown")

    if ltp_vs == "unknown":
        return True, "EMA unknown — allowing"   # don't block when we can't fetch

    if direction == "UP":
        if ltp_vs == "above":
            return True, f"✅ Daily EMA7({ema['ema7']}) + EMA20({ema['ema20']}) confirmed bull"
        else:
            return False, f"❌ Daily EMA not bull-aligned (LTP {ltp_vs} EMA)"
    else:
        if ltp_vs == "below":
            return True, f"✅ Daily EMA7({ema['ema7']}) + EMA20({ema['ema20']}) confirmed bear"
        else:
            return False, f"❌ Daily EMA not bear-aligned (LTP {ltp_vs} EMA)"


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — CORE BOS DETECTION (unchanged logic, added quality filter)
# ─────────────────────────────────────────────────────────────────────────────

def detect_bos(candles):
    empty = {
        "bos_type": None, "broken_level": 0, "bos_candle": None,
        "prev_trend": "RANGING", "swing_high": 0, "swing_low": 0,
        "ob_zone": (0, 0), "next_liquidity": 0, "strength": 0, "volume_ratio": 1.0
    }
    if not candles or len(candles) < BOS_SWING_BARS * 2 + 4:
        return empty

    history = candles[:-1]
    trigger = candles[-1]
    sw      = get_swing_points(history, BOS_SWING_BARS)
    sh_list = sw["swing_highs"]
    sl_list = sw["swing_lows"]

    if len(sh_list) < 2 or len(sl_list) < 2:
        return empty

    last_sh  = sh_list[-1][1]; prev_sh = sh_list[-2][1]
    last_sl  = sl_list[-1][1]; prev_sl = sl_list[-2][1]
    hh = last_sh > prev_sh;    hl = last_sl > prev_sl
    lh = last_sh < prev_sh;    ll = last_sl < prev_sl
    prev_trend = "BULLISH" if hh and hl else ("BEARISH" if lh and ll else "RANGING")

    avg_vol   = sum(c["volume"] for c in history[-10:]) / 10 if len(history) >= 10 else 1
    vol_ratio = round(trigger["volume"] / avg_vol, 2) if avg_vol > 0 else 1.0
    close     = trigger["close"]

    str_up   = round((close - last_sh) / last_sh * 100, 2) if last_sh > 0 else 0
    str_down = round((last_sl - close) / last_sl * 100, 2) if last_sl > 0 else 0

    def last_opp(d):
        for c in reversed(history[-8:]):
            if d == "UP"   and c["close"] < c["open"]: return c
            if d == "DOWN" and c["close"] > c["open"]: return c
        return None

    if close > last_sh and str_up >= MIN_BOS_PCT:
        # ── Candle quality: close must be in top 33% of range
        if not _candle_quality_ok(trigger, "UP"):
            return empty
        ob    = last_opp("UP")
        # next_liquidity must be ABOVE current close (not just above broken level)
        # otherwise t1 ends up below LTP giving negative R:R
        above = [h for _, h in sh_list if h > close * 1.001]
        return {
            "bos_type":       "CHOCH_UP" if prev_trend == "BEARISH" else "BOS_UP",
            "broken_level":   last_sh,
            "bos_candle":     trigger,
            "prev_trend":     prev_trend,
            "swing_high":     last_sh,
            "swing_low":      last_sl,
            "ob_zone":        (ob["low"], ob["high"]) if ob else (0, 0),
            "next_liquidity": min(above) if above else round(close * 1.02, 1),
            "strength":       str_up,
            "volume_ratio":   vol_ratio,
        }

    if close < last_sl and str_down >= MIN_BOS_PCT:
        # ── Candle quality: close must be in bottom 33% of range
        if not _candle_quality_ok(trigger, "DOWN"):
            return empty
        ob    = last_opp("DOWN")
        # next_liquidity must be BELOW current close
        below = [l for _, l in sl_list if l < close * 0.999]
        return {
            "bos_type":       "CHOCH_DOWN" if prev_trend == "BULLISH" else "BOS_DOWN",
            "broken_level":   last_sl,
            "bos_candle":     trigger,
            "prev_trend":     prev_trend,
            "swing_high":     last_sh,
            "swing_low":      last_sl,
            "ob_zone":        (ob["low"], ob["high"]) if ob else (0, 0),
            "next_liquidity": max(below) if below else round(close * 0.98, 1),
            "strength":       str_down,
            "volume_ratio":   vol_ratio,
        }

    return empty


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — TRADE SETUP BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _rr(entry, sl, t, bull):
    risk = (entry - sl) if bull else (sl - entry)
    rwd  = (t - entry) if bull else (entry - t)
    return round(rwd / risk, 1) if risk > 0 else 0


def build_bos_setup(bos, symbol, ltp):
    bt           = bos["bos_type"]
    ob_low, ob_high = bos["ob_zone"]
    broken       = bos["broken_level"]
    nxt          = bos["next_liquidity"]  # already guaranteed beyond LTP by detect_bos fix
    is_bull      = bt in ("BOS_UP", "CHOCH_UP")

    if is_bull:
        sl = round((ob_low  * 0.998) if ob_low  else (broken * 0.995), 1)
        risk = round(ltp - sl, 2)
        if risk <= 0:
            risk = ltp * 0.02  # safety fallback

        # T1: next liquidity if it is above LTP (already guaranteed), else 1.5×risk
        t1 = round(nxt if nxt > ltp * 1.001 else ltp + risk * 1.5, 1)
        # T2: extend beyond T1 by 0.5×risk (meaningful, not just +1.5 pts)
        t2 = round(t1 + risk * 0.5, 1)

        # OB retest entry = top of order block
        e_ret = round(ob_high, 1) if ob_high else round(broken * 1.002, 1)

    else:  # BEAR
        sl = round((ob_high * 1.002) if ob_high else (broken * 1.005), 1)
        risk = round(sl - ltp, 2)
        if risk <= 0:
            risk = ltp * 0.02

        # T1: next liquidity below LTP (already guaranteed), else 1.5×risk below
        t1 = round(nxt if nxt < ltp * 0.999 else ltp - risk * 1.5, 1)
        # T2: extend below T1 by 0.5×risk
        t2 = round(t1 - risk * 0.5, 1)

        # OB retest entry = bottom of order block
        e_ret = round(ob_low, 1) if ob_low else round(broken * 0.998, 1)

    return {
        "symbol":          symbol,
        "bos_type":        bt,
        "ltp":             ltp,
        "broken_level":    broken,
        "entry_now":       ltp,
        "entry_retest":    round(e_ret, 1),
        "sl":              sl,
        "t1":              t1,
        "t2":              t2,
        "rr_now":          _rr(ltp,   sl, t1, is_bull),
        "rr_retest":       _rr(e_ret, sl, t1, is_bull),
        "ob_low":          ob_low,
        "ob_high":         ob_high,
        "next_liq":        nxt,
        "volume_ratio":    bos["volume_ratio"],
        "strength":        bos["strength"],
        "prev_trend":      bos.get("prev_trend", ""),
        "already_alerted": False,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — DEDUP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_dedup():
    if not os.path.exists(BOS_DEDUP_FILE):
        return {}
    try:
        data  = json.loads(open(BOS_DEDUP_FILE).read())
        today = date.today().isoformat()
        return {k: v for k, v in data.items() if v.get("date") == today}
    except:
        return {}


def _save_dedup(d):
    open(BOS_DEDUP_FILE, "w").write(json.dumps(d))


def _dkey(sym, bt, lvl):
    return f"{sym}_{bt}_{round(lvl / 5) * 5}"


def _already_alerted(sym, bt, lvl):
    return _dkey(sym, bt, lvl) in _load_dedup()


def _mark_alerted(sym, bt, lvl):
    d = _load_dedup()
    d[_dkey(sym, bt, lvl)] = {
        "date": date.today().isoformat(),
        "time": datetime.now().strftime("%H:%M"),
        "level": lvl,
    }
    _save_dedup(d)


# OB retest dedup — separate from BOS dedup
def _ob_retest_key(sym, broken_level):
    return f"OB_RETEST_{sym}_{round(broken_level / 5) * 5}"


def _already_alerted_retest(sym, broken_level):
    return _ob_retest_key(sym, broken_level) in _load_dedup()


def _mark_alerted_retest(sym, broken_level):
    d = _load_dedup()
    d[_ob_retest_key(sym, broken_level)] = {
        "date": date.today().isoformat(),
        "time": datetime.now().strftime("%H:%M"),
    }
    _save_dedup(d)


# Hourly break dedup
def _hourly_break_key(sym, direction, level):
    return f"HRLY_BREAK_{sym}_{direction}_{round(level / 5) * 5}"


def _already_alerted_hourly(sym, direction, level):
    return _hourly_break_key(sym, direction, level) in _load_dedup()


def _mark_alerted_hourly(sym, direction, level):
    d = _load_dedup()
    d[_hourly_break_key(sym, direction, level)] = {
        "date": date.today().isoformat(),
        "time": datetime.now().strftime("%H:%M"),
    }
    _save_dedup(d)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6b — ALERT LOG (persists every fired alert to CACHE for dashboard)
# ─────────────────────────────────────────────────────────────────────────────

import threading as _threading
_alert_log_lock = _threading.Lock()

def _alert_log_path():
    """Today's BOS alert log: CACHE/bos_alert_log_YYYYMMDD.json"""
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"bos_alert_log_{date.today().isoformat()}.json")

def load_bos_alert_log() -> list:
    """Load today's BOS/CHoCH alert log. Called by render_bos_tab."""
    try:
        p = _alert_log_path()
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []

def _append_bos_alert_log(entry: dict):
    """Thread-safe append of a fired alert to today's log."""
    with _alert_log_lock:
        try:
            p   = _alert_log_path()
            log = []
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    log = json.load(f)
            log.append(entry)
            with open(p, "w", encoding="utf-8") as f:
                json.dump(log, f, indent=2, default=str)
        except Exception as _e:
            print(f"[BOS] alert log error: {_e}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — TELEGRAM MESSAGE BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def _ts_now():
    return datetime.now(IST).strftime("%H:%M IST") if IST else datetime.now().strftime("%H:%M")


def build_bos_telegram(setup, bos, ema_note=""):
    bt       = setup["bos_type"]
    sym      = setup["symbol"]
    ltp      = setup["ltp"]
    broken   = setup["broken_level"]
    sl       = setup["sl"]
    t1       = setup["t1"]
    t2       = setup["t2"]
    e_now    = setup["entry_now"]
    e_ret    = setup["entry_retest"]
    rr_now   = setup["rr_now"]
    rr_ret   = setup["rr_retest"]
    ob_low   = setup["ob_low"]
    ob_high  = setup["ob_high"]
    vol      = setup["volume_ratio"]
    strength = setup["strength"]
    nxt      = setup["next_liq"]
    pt       = bos.get("prev_trend", "")

    is_bull  = bt in ("BOS_UP", "CHOCH_UP")
    is_choch = "CHOCH" in bt
    icon     = "🚀" if is_bull and not is_choch else ("🔄" if is_choch else "💥")
    dirlbl   = "UP ↑" if is_bull else "DOWN ↓"
    typelbl  = "CHoCH" if is_choch else "BOS"
    vol_ic   = "🔥" if vol >= 2.0 else ("⚡" if vol >= 1.5 else "📊")
    choch_note = f"\n🔄 <b>Trend Reversal</b> — Prior trend was {pt}\n" if is_choch else ""
    ob_line    = (f"🟦 {'Bullish OB' if is_bull else 'Bearish OB'}: "
                  f"<b>{ob_low:.1f}–{ob_high:.1f}</b>\n") if ob_low and ob_high else ""
    ema_line   = f"📊 <i>{ema_note}</i>\n" if ema_note else ""

    return (
        f"{icon} <b>{typelbl} {dirlbl} — {sym}</b>\n"
        f"⏰ {_ts_now()}  |  ⏱ <b>1 Hour</b> TF\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 LTP: <b>{ltp:.1f}</b>\n"
        f"🔓 Broke {'swing high' if is_bull else 'swing low'}: <b>{broken:.1f}</b>\n"
        f"💪 Strength: <b>{strength:.2f}%</b>  |  {vol_ic} Volume: <b>{vol:.1f}x</b>\n"
        f"{choch_note}"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{ob_line}"
        f"💧 Next liquidity: <b>{nxt:.1f}</b>\n"
        f"{ema_line}"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>TRADE SETUP</b>\n"
        f"\n<b>A — Momentum Entry</b>\n"
        f"  Entry <b>{e_now:.1f}</b> | SL <b>{sl:.1f}</b> | "
        f"T1 <b>{t1:.1f}</b> | T2 <b>{t2:.1f}</b> | R:R <b>{rr_now}:1</b>\n"
        f"\n<b>B — OB Retest (Better R:R)</b>\n"
        f"  Entry <b>{e_ret:.1f}</b> | SL <b>{sl:.1f}</b> | "
        f"T1 <b>{t1:.1f}</b> | T2 <b>{t2:.1f}</b> | R:R <b>{rr_ret}:1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>NOT financial advice. Verify before trading.</i>"
    )


def build_ob_retest_telegram(setup, bos):
    """
    Second alert — fires when LTP retraces INTO the Order Block zone.
    This is the B-entry the original alert mentioned.
    """
    sym     = setup["symbol"]
    bt      = setup["bos_type"]
    is_bull = bt in ("BOS_UP", "CHOCH_UP")
    ltp     = setup["ltp"]
    ob_low  = setup["ob_low"]
    ob_high = setup["ob_high"]
    sl      = setup["sl"]
    t1      = setup["t1"]
    t2      = setup["t2"]
    e_ret   = setup["entry_retest"]
    rr_ret  = setup["rr_retest"]
    icon    = "🎯" if is_bull else "🎯"

    return (
        f"{icon} <b>OB RETEST ENTRY — {sym}</b>\n"
        f"⏰ {_ts_now()}  |  ⏱ <b>1 Hour</b> TF\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 LTP: <b>{ltp:.1f}</b>  has pulled back into the OB zone\n"
        f"🟦 {'Bullish OB' if is_bull else 'Bearish OB'}: <b>{ob_low:.1f}–{ob_high:.1f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>B — OB Retest Entry (Better R:R)</b>\n"
        f"  Entry <b>{e_ret:.1f}</b> | SL <b>{sl:.1f}</b> | "
        f"T1 <b>{t1:.1f}</b> | T2 <b>{t2:.1f}</b> | R:R <b>{rr_ret}:1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 <i>Price returned to institution's buying zone. "
        f"{'Watch for bullish rejection / pin bar at OB.' if is_bull else 'Watch for bearish rejection at OB.'}</i>\n"
        f"⚠️ <i>NOT financial advice. Verify before trading.</i>"
    )


def build_hourly_break_telegram(symbol, direction, prev_high, prev_low,
                                 ltp, sl, t1, t2, rr, vol_ratio, ema_note=""):
    """
    Alert for when LTP breaks the previous completed 1H candle's high or low.
    Direction: "UP" or "DOWN"
    """
    is_bull = direction == "UP"
    icon    = "⚡" if is_bull else "⚡"
    broke   = prev_high if is_bull else prev_low
    dirlbl  = "High Break ↑" if is_bull else "Low Break ↓"
    vol_ic  = "🔥" if vol_ratio >= 2.0 else ("⚡" if vol_ratio >= 1.5 else "📊")
    ema_line = f"📊 <i>{ema_note}</i>\n" if ema_note else ""

    return (
        f"{icon} <b>1H {dirlbl} — {symbol}</b>\n"
        f"⏰ {_ts_now()}  |  ⏱ <b>1 Hour</b> TF\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 LTP: <b>{ltp:.1f}</b>\n"
        f"🔓 Broke prev-hour {'high' if is_bull else 'low'}: <b>{broke:.1f}</b>\n"
        f"{vol_ic} Volume: <b>{vol_ratio:.1f}x</b>\n"
        f"{ema_line}"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>QUICK SETUP</b>\n"
        f"  Entry <b>{ltp:.1f}</b> | SL <b>{sl:.1f}</b> | "
        f"T1 <b>{t1:.1f}</b> | T2 <b>{t2:.1f}</b> | R:R <b>{rr}:1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>NOT financial advice. Verify before trading.</i>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — OB RETEST CHECKER
# Called on every scan cycle for symbols that had a prior BOS alert today.
# ─────────────────────────────────────────────────────────────────────────────

def check_ob_retest(symbol, setup, ltp, send_telegram_fn, tg_enabled=True):
    """
    For a symbol that already had a BOS alert:
    Fire a second alert when LTP retraces back into the OB zone.

    Bull BOS: ltp falls back to within OB_RETEST_TOLERANCE of ob_high (top of OB)
    Bear BOS: ltp rises back to within OB_RETEST_TOLERANCE of ob_low (bottom of OB)
    """
    ob_low  = setup.get("ob_low", 0)
    ob_high = setup.get("ob_high", 0)
    if not ob_low or not ob_high:
        return False   # no OB zone defined

    bt      = setup.get("bos_type","")
    is_bull = bt in ("BOS_UP","CHOCH_UP")
    broken  = setup.get("broken_level", 0)

    if is_bull:
        # Retest: ltp has come back down to ob_high ± tolerance
        at_ob = ob_low <= ltp <= ob_high * (1 + OB_RETEST_TOLERANCE)
        # Also ensure price hasn't gone below broken level (would invalidate BOS)
        still_valid = ltp > broken * 0.995
    else:
        # Retest: ltp has come back up to ob_low ± tolerance
        at_ob = ob_low * (1 - OB_RETEST_TOLERANCE) <= ltp <= ob_high
        still_valid = ltp < broken * 1.005

    if not (at_ob and still_valid):
        return False

    if _already_alerted_retest(symbol, broken):
        return False   # already sent retest alert today for this level

    setup["ltp"] = ltp   # update LTP to current
    msg = build_ob_retest_telegram(setup, {"bos_type": bt})
    if tg_enabled:
        try:
            _ob_dk = f"OB_RETEST_{symbol}_{round(broken/5)*5}"
            send_telegram_fn(msg, dedup_key=_ob_dk)
            _mark_alerted_retest(symbol, broken)
            # Log OB retest alert
            _append_bos_alert_log({
                "type":         "OB_RETEST",
                "symbol":       symbol,
                "time":         _ts_now(),
                "date":         date.today().isoformat(),
                "ltp":          setup.get("ltp", 0),
                "broken":       broken,
                "sl":           setup.get("sl", 0),
                "t1":           setup.get("t1", 0),
                "t2":           setup.get("t2", 0),
                "entry_now":    setup.get("entry_now", 0),
                "entry_retest": setup.get("entry_retest", 0),
                "rr_now":       setup.get("rr_now", 0),
                "rr_retest":    setup.get("rr_retest", 0),
                "ob_low":       setup.get("ob_low", 0),
                "ob_high":      setup.get("ob_high", 0),
                "next_liq":     setup.get("next_liq", 0),
                "strength":     setup.get("strength", 0),
                "vol_ratio":    setup.get("volume_ratio", 0),
                "prev_trend":   setup.get("prev_trend", ""),
                "ema_note":     "",
                "alert_type":   "OB_RETEST",
            })
            return True
        except Exception:
            pass
    return False


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 — HOURLY HIGH/LOW BREAK SCANNER
# ─────────────────────────────────────────────────────────────────────────────

def scan_hourly_breaks(db, symbols, send_telegram_fn, ltp_dict=None,
                       kite=None, tg_enabled=True):
    """
    Scan all symbols for LTP breaking the previous completed 1H candle's high or low.

    Rules:
    - Previous candle must be the last COMPLETED hourly candle (not current forming candle)
    - LTP must exceed prev-high by at least HOURLY_BREAK_PCT %
    - Candle quality: the break must happen with strong close (top/bottom 33%)
    - Volume: breaking candle volume ≥ MIN_VOL_RATIO × 10-bar avg
    - Daily EMA filter (if REQUIRE_DAILY_EMA)
    - One alert per direction per level per day
    """
    events = []
    for symbol in symbols:
        try:
            if symbol in INDEX_SYMBOLS:
                continue
            candles = db.get(symbol, n=20)
            if not candles or len(candles) < 4:
                continue

            # candles[-1] = current (possibly forming); candles[-2] = last completed
            current   = candles[-1]
            prev      = candles[-2]
            prev2     = candles[-3]  # one before that, for averaging

            # Ensure the "previous" candle is actually from today or yesterday
            try:
                prev_date = datetime.strptime(prev["datetime"][:10], "%Y-%m-%d").date()
                if prev_date < date.today() - timedelta(days=2):
                    continue
            except Exception:
                pass

            ltp = (ltp_dict or {}).get(symbol, current["close"])

            # Volume check on current candle
            avg_vol   = sum(c["volume"] for c in candles[-11:-1]) / 10 if len(candles) >= 11 else 1
            vol_ratio = round(current["volume"] / avg_vol, 2) if avg_vol > 0 else 1.0

            prev_high = prev["high"]
            prev_low  = prev["low"]

            # ── Bull: LTP breaks above previous candle high ──
            if ltp > prev_high * (1 + HOURLY_BREAK_PCT / 100):
                if not _candle_quality_ok(current, "UP"):
                    continue
                if vol_ratio < MIN_VOL_RATIO:
                    continue

                ema_ok, ema_note = _daily_ema_confirms(symbol, "UP", kite)
                if not ema_ok:
                    continue   # daily trend not confirmed

                if _already_alerted_hourly(symbol, "UP", prev_high):
                    continue

                # Quick setup: SL = prev candle low, T1 = prev2 high or +1.5%, T2 = +3%
                sl = round(prev_low * 0.998, 1)
                t1 = round(max(prev2["high"], ltp * 1.01), 1)
                t2 = round(ltp * 1.03, 1)
                rr = _rr(ltp, sl, t1, True)

                event = {
                    "symbol":    symbol,
                    "direction": "UP",
                    "ltp":       ltp,
                    "prev_high": prev_high,
                    "prev_low":  prev_low,
                    "sl":        sl,
                    "t1":        t1,
                    "t2":        t2,
                    "rr":        rr,
                    "vol_ratio": vol_ratio,
                    "ema_note":  ema_note,
                }
                events.append(event)

                if tg_enabled:
                    try:
                        msg = build_hourly_break_telegram(
                            symbol, "UP", prev_high, prev_low,
                            ltp, sl, t1, t2, rr, vol_ratio, ema_note
                        )
                        _hup_dk = f"HRLY_UP_{symbol}_{round(prev_high/5)*5}"
                        send_telegram_fn(msg, dedup_key=_hup_dk)
                        _mark_alerted_hourly(symbol, "UP", prev_high)
                        _append_bos_alert_log({
                            "type": "HRLY_BREAK_UP", "symbol": symbol,
                            "time": _ts_now(), "date": date.today().isoformat(),
                            "ltp": ltp, "broken": prev_high,
                            "sl": sl, "t1": t1, "t2": t2,
                            "entry_now": ltp, "entry_retest": 0,
                            "rr_now": rr, "rr_retest": 0,
                            "ob_low": 0, "ob_high": 0,
                            "next_liq": t1, "strength": 0,
                            "vol_ratio": vol_ratio, "prev_trend": "",
                            "ema_note": ema_note, "alert_type": "HOURLY_BREAK",
                        })
                    except Exception:
                        pass

            # ── Bear: LTP breaks below previous candle low ──
            elif ltp < prev_low * (1 - HOURLY_BREAK_PCT / 100):
                if not _candle_quality_ok(current, "DOWN"):
                    continue
                if vol_ratio < MIN_VOL_RATIO:
                    continue

                ema_ok, ema_note = _daily_ema_confirms(symbol, "DOWN", kite)
                if not ema_ok:
                    continue

                if _already_alerted_hourly(symbol, "DOWN", prev_low):
                    continue

                sl = round(prev_high * 1.002, 1)
                t1 = round(min(prev2["low"], ltp * 0.99), 1)
                t2 = round(ltp * 0.97, 1)
                rr = _rr(ltp, sl, t1, False)

                event = {
                    "symbol":    symbol,
                    "direction": "DOWN",
                    "ltp":       ltp,
                    "prev_high": prev_high,
                    "prev_low":  prev_low,
                    "sl":        sl,
                    "t1":        t1,
                    "t2":        t2,
                    "rr":        rr,
                    "vol_ratio": vol_ratio,
                    "ema_note":  ema_note,
                }
                events.append(event)

                if tg_enabled:
                    try:
                        msg = build_hourly_break_telegram(
                            symbol, "DOWN", prev_high, prev_low,
                            ltp, sl, t1, t2, rr, vol_ratio, ema_note
                        )
                        _hdn_dk = f"HRLY_DOWN_{symbol}_{round(prev_low/5)*5}"
                        send_telegram_fn(msg, dedup_key=_hdn_dk)
                        _mark_alerted_hourly(symbol, "DOWN", prev_low)
                        _append_bos_alert_log({
                            "type": "HRLY_BREAK_DOWN", "symbol": symbol,
                            "time": _ts_now(), "date": date.today().isoformat(),
                            "ltp": ltp, "broken": prev_low,
                            "sl": sl, "t1": t1, "t2": t2,
                            "entry_now": ltp, "entry_retest": 0,
                            "rr_now": rr, "rr_retest": 0,
                            "ob_low": 0, "ob_high": 0,
                            "next_liq": t1, "strength": 0,
                            "vol_ratio": vol_ratio, "prev_trend": "",
                            "ema_note": ema_note, "alert_type": "HOURLY_BREAK",
                        })
                    except Exception:
                        pass

        except Exception:
            continue

    return events


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 10 — MAIN BOS SCAN (upgraded with EMA filter + OB retest check)
# ─────────────────────────────────────────────────────────────────────────────

# In-memory store of today's BOS setups for OB retest checking
_todays_bos_setups: dict = {}   # {symbol: setup_dict}


def run_bos_scan(db, symbols, send_telegram_fn, ltp_dict=None,
                 min_rr=MIN_RR, min_vol=MIN_VOL_RATIO,
                 tg_enabled=True, kite=None):
    """
    Main BOS scan. Now also:
    1. Applies daily EMA filter.
    2. Checks OB retest for symbols that already had a BOS alert today.
    3. Runs hourly high/low break scan as a separate pass.
    """
    global _todays_bos_setups
    events = []

    for symbol in symbols:
        try:
            if symbol in INDEX_SYMBOLS:
                continue
            candles = db.get(symbol, n=BOS_LOOKBACK)
            if not candles or len(candles) < 10:
                continue
            try:
                last_date = datetime.strptime(candles[-1]["datetime"][:10], "%Y-%m-%d").date()
                if last_date < date.today():
                    continue
            except Exception:
                pass

            ltp = (ltp_dict or {}).get(symbol, candles[-1]["close"])

            # ── OB Retest check for symbols with prior BOS ──────────────────
            prior_setup = _todays_bos_setups.get(symbol)
            if prior_setup:
                check_ob_retest(symbol, prior_setup, ltp, send_telegram_fn, tg_enabled)

            # ── BOS detection ────────────────────────────────────────────────
            bos = detect_bos(candles)
            if not bos["bos_type"]:
                continue

            setup = build_bos_setup(bos, symbol, ltp)

            # ── R:R and volume filters ───────────────────────────────────────
            best_rr = max(setup["rr_now"] or 0, setup["rr_retest"] or 0)
            if best_rr < min_rr or bos["volume_ratio"] < min_vol:
                continue

            # ── Daily EMA filter ─────────────────────────────────────────────
            direction  = "UP" if bos["bos_type"] in ("BOS_UP","CHOCH_UP") else "DOWN"
            ema_ok, ema_note = _daily_ema_confirms(symbol, direction, kite)
            if not ema_ok:
                # Still add to events (visible on dashboard) but don't alert
                setup["ema_filtered"] = True
                setup["ema_note"]     = ema_note
                events.append({"setup": setup, "bos": bos})
                continue

            setup["ema_filtered"] = False
            setup["ema_note"]     = ema_note

            # ── Dedup and alert ──────────────────────────────────────────────
            already = _already_alerted(symbol, bos["bos_type"], bos["broken_level"])
            setup["already_alerted"] = already

            if not already and tg_enabled:
                try:
                    msg = build_bos_telegram(setup, bos, ema_note)
                    _dedup_key = f"BOS_1H_{symbol}_{bos['bos_type']}_{round(bos['broken_level']/5)*5}"
                    send_telegram_fn(msg, dedup_key=_dedup_key)
                    _mark_alerted(symbol, bos["bos_type"], bos["broken_level"])
                    # Store setup for OB retest monitoring
                    _todays_bos_setups[symbol] = setup
                    # ── Persist to alert log for dashboard display ──
                    _append_bos_alert_log({
                        "type":        bos["bos_type"],
                        "symbol":      symbol,
                        "time":        _ts_now(),
                        "date":        date.today().isoformat(),
                        "ltp":         setup["ltp"],
                        "broken":      setup["broken_level"],
                        "sl":          setup["sl"],
                        "t1":          setup["t1"],
                        "t2":          setup["t2"],
                        "entry_now":   setup["entry_now"],
                        "entry_retest":setup["entry_retest"],
                        "rr_now":      setup["rr_now"],
                        "rr_retest":   setup["rr_retest"],
                        "ob_low":      setup["ob_low"],
                        "ob_high":     setup["ob_high"],
                        "next_liq":    setup["next_liq"],
                        "strength":    setup["strength"],
                        "vol_ratio":   setup["volume_ratio"],
                        "prev_trend":  setup["prev_trend"],
                        "ema_note":    setup.get("ema_note", ""),
                        "alert_type":  "BOS_CHOCH",
                    })
                except Exception as _ae:
                    print(f"[BOS] alert/log error {symbol}: {_ae}")

            events.append({"setup": setup, "bos": bos})

        except Exception:
            continue

    _save_scan_cache(events)

    # ── Hourly high/low break scan (separate pass) ───────────────────────────
    hourly_events = scan_hourly_breaks(
        db, symbols, send_telegram_fn,
        ltp_dict=ltp_dict, kite=kite, tg_enabled=tg_enabled
    )

    return events, hourly_events   # returns tuple now — callers should unpack


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 11 — CACHE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _save_scan_cache(events):
    try:
        cache = {
            "date": date.today().isoformat(),
            "time": datetime.now().strftime("%H:%M:%S"),
            "events": [{
                "symbol":      e["setup"]["symbol"],
                "bos_type":    e["setup"]["bos_type"],
                "ltp":         e["setup"]["ltp"],
                "broken":      e["setup"]["broken_level"],
                "sl":          e["setup"]["sl"],
                "t1":          e["setup"]["t1"],
                "t2":          e["setup"]["t2"],
                "rr_now":      e["setup"]["rr_now"],
                "rr_retest":   e["setup"]["rr_retest"],
                "strength":    e["setup"]["strength"],
                "vol_ratio":   e["setup"]["volume_ratio"],
                "ob_low":      e["setup"]["ob_low"],
                "ob_high":     e["setup"]["ob_high"],
                "next_liq":    e["setup"]["next_liq"],
                "prev_trend":  e["setup"].get("prev_trend",""),
                "ema_note":    e["setup"].get("ema_note",""),
                "ema_filtered":e["setup"].get("ema_filtered", False),
                "alerted":     not e["setup"].get("already_alerted", False),
            } for e in events]
        }
        open(BOS_CACHE_FILE, "w").write(json.dumps(cache, default=str))
    except Exception:
        pass


def load_scan_cache():
    if not os.path.exists(BOS_CACHE_FILE):
        return {}
    try:
        data = json.loads(open(BOS_CACHE_FILE).read())
        return data if data.get("date") == date.today().isoformat() else {}
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 12 — STREAMLIT RENDER  v3.0
# Shows: (1) Today's Alert Log with trade setup cards
#        (2) Live scan table with filter controls
#        (3) Filter by alert type
# ─────────────────────────────────────────────────────────────────────────────

def render_bos_tab(cache, db=None):
    try:
        import streamlit as st
        import pandas as pd
    except Exception:
        return

    st.subheader("📐 1-Hour BOS / CHoCH Scanner  v3.0")
    st.caption(
        "BOS = trend continuation  |  CHoCH = trend reversal  |  "
        "OB Retest = pullback to order block  |  Alerts logged once per event per day"
    )

    # ══════════════════════════════════════════════════════════════
    # SECTION A — TODAY'S FIRED ALERTS (the same alerts as Telegram)
    # ══════════════════════════════════════════════════════════════
    st.markdown("### 📲 Today\'s Alerts (same as Telegram)")
    st.caption("Every alert that was sent to Telegram today appears here with full trade details.")

    alert_log = load_bos_alert_log()

    if not alert_log:
        st.info(
            "No BOS/CHoCH/Hourly-Break alerts fired today yet.  \n"
            "Alerts appear the moment they fire — identical to what Telegram receives."
        )
    else:
        # ── Summary metrics ──
        _types = [e.get("type", "") for e in alert_log]
        _m1, _m2, _m3, _m4, _m5 = st.columns(5)
        _m1.metric("Total Today",    len(alert_log))
        _m2.metric("🟢 BOS/CHoCH UP",  sum(1 for t in _types if "UP"    in t and "HRLY" not in t))
        _m3.metric("🔴 BOS/CHoCH DN",  sum(1 for t in _types if "DOWN"  in t and "HRLY" not in t))
        _m4.metric("🎯 OB Retest",     sum(1 for t in _types if t == "OB_RETEST"))
        _m5.metric("⚡ Hourly Break",   sum(1 for t in _types if "HRLY" in t))

        # ── Alert type filter ──
        _af_opts = ["All"] + sorted(set(_types))
        _af = st.selectbox("Filter by type", _af_opts, key="_bos_alert_filter")
        _show_log = alert_log if _af == "All" else [e for e in alert_log if e.get("type") == _af]

        st.markdown(f"**{len(_show_log)} alert(s)**")

        for _entry in reversed(_show_log):
            _et   = _entry.get("type", "")
            _sym  = _entry.get("symbol", "")
            _ts   = _entry.get("time", "")
            _ltp  = _entry.get("ltp", 0)
            _brk  = _entry.get("broken", 0)
            _sl   = _entry.get("sl", 0)
            _t1   = _entry.get("t1", 0)
            _t2   = _entry.get("t2", 0)
            _en   = _entry.get("entry_now", 0)
            _er   = _entry.get("entry_retest", 0)
            _rn   = _entry.get("rr_now", 0)
            _rr   = _entry.get("rr_retest", 0)
            _obl  = _entry.get("ob_low", 0)
            _obh  = _entry.get("ob_high", 0)
            _nl   = _entry.get("next_liq", 0)
            _str  = _entry.get("strength", 0)
            _vol  = _entry.get("vol_ratio", 0)
            _pt   = _entry.get("prev_trend", "")
            _ema  = _entry.get("ema_note", "")

            _is_up   = "UP" in _et
            _is_choch= "CHOCH" in _et
            _is_hrly = "HRLY" in _et
            _is_ob   = _et == "OB_RETEST"

            if _is_hrly:
                _icon = "⚡"; _lbl = _et.replace("HRLY_BREAK_", "1H Break ")
                _bg = "#0a1a2e"; _bd = "#4da6ff"
            elif _is_ob:
                _icon = "🎯"; _lbl = "OB Retest Entry"
                _bg = "#1a1a0a"; _bd = "#ffdd44"
            elif _is_choch:
                _icon = "🔄"
                _lbl = f"CHoCH {'UP ↑' if _is_up else 'DOWN ↓'}"
                _bg = "#0a2e0a" if _is_up else "#2e0a0a"
                _bd = "#00e676" if _is_up else "#ff5252"
            else:
                _icon = "🚀" if _is_up else "💥"
                _lbl = f"BOS {'UP ↑' if _is_up else 'DOWN ↓'}"
                _bg = "#0a2e0a" if _is_up else "#2e0a0a"
                _bd = "#00e676" if _is_up else "#ff5252"

            with st.expander(
                f"{_icon} {_lbl} — **{_sym}**  |  LTP {_ltp}  |  {_ts}",
                expanded=False
            ):
                # Header card
                _extra = ""
                if _str:  _extra += f"  |  Strength: <b>{_str:.2f}%</b>"
                if _vol:  _extra += f"<br>Volume: <b>{_vol:.1f}x</b>"
                if _pt:   _extra += f"  |  Prior trend: <b>{_pt}</b>"
                if _ema:  _extra += f'<br><i style="color:#aaa;font-size:11px">{_ema}</i>'
                st.markdown(
                    f'<div style="background:{_bg};border-left:4px solid {_bd};padding:10px;border-radius:6px;margin-bottom:8px">'
                    f'<b>{_icon} {_lbl} — {_sym}</b> <span style="color:#aaa;font-size:11px">{_ts}</span><br>'
                    f'LTP: <b>{_ltp}</b>  |  Broke: <b>{_brk}</b>'
                    + _extra + "</div>",
                    unsafe_allow_html=True
                )

                _ca, _cb, _cc = st.columns(3)
                with _ca:
                    st.markdown("**📦 Levels**")
                    if _nl:  st.markdown(f"Next Liq: `{_nl:.1f}`")
                    if _obl and _obh:
                        _ob_lbl = "Bullish OB" if _is_up else "Bearish OB"
                        st.markdown(f"{_ob_lbl}: `{_obl:.1f}–{_obh:.1f}`")
                with _cb:
                    st.markdown("**🎯 A — Momentum Entry**")
                    st.markdown(
                        f"Entry: `{_en}`  SL: `{_sl}`  \n"
                        f"T1: `{_t1}`  T2: `{_t2}`  \n"
                        f"R:R `{_rn}:1`"
                    )
                with _cc:
                    st.markdown("**🎯 B — OB Retest**")
                    if _er and _er != _en:
                        st.markdown(
                            f"Entry: `{_er}`  SL: `{_sl}`  \n"
                            f"T1: `{_t1}`  T2: `{_t2}`  \n"
                            f"R:R `{_rr}:1`"
                        )
                    else:
                        st.caption("No OB zone available")

        # ── Flat table view ──
        with st.expander("📋 Table View — All Today\'s Alerts", expanded=False):
            _tbl_rows = []
            for _e in reversed(alert_log):
                _tbl_rows.append({
                    "Time":     _e.get("time", ""),
                    "Type":     _e.get("type", ""),
                    "Symbol":   _e.get("symbol", ""),
                    "LTP":      _e.get("ltp", 0),
                    "Broke":    _e.get("broken", 0),
                    "Str%":     _e.get("strength", 0),
                    "Vol x":    _e.get("vol_ratio", 0),
                    "Prior":    _e.get("prev_trend", ""),
                    "Entry A":  _e.get("entry_now", 0),
                    "Entry B":  _e.get("entry_retest", 0),
                    "SL":       _e.get("sl", 0),
                    "T1":       _e.get("t1", 0),
                    "T2":       _e.get("t2", 0),
                    "R:R A":    _e.get("rr_now", 0),
                    "R:R B":    _e.get("rr_retest", 0),
                })
            if _tbl_rows:
                st.dataframe(
                    pd.DataFrame(_tbl_rows),
                    use_container_width=True,
                    hide_index=True,
                    height=min(500, 60 + len(_tbl_rows) * 35)
                )

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════
    # SECTION B — LIVE SCAN EVENTS TABLE (current scan cycle results)
    # ══════════════════════════════════════════════════════════════
    st.markdown("### 📊 Live Scan Results")

    if not cache or not cache.get("events"):
        st.info("No BOS events detected in current scan cycle.")
    else:
        events = cache.get("events", [])
        ts     = cache.get("time", "")
        bull   = sum(1 for e in events if "UP"    in e.get("bos_type", ""))
        bear   = len(events) - bull
        choch  = sum(1 for e in events if "CHOCH" in e.get("bos_type", ""))
        filt   = sum(1 for e in events if e.get("ema_filtered", False))

        st.caption(
            f"Last scan: **{ts}** | {len(events)} events | "
            f"🚀 {bull} bull | 💥 {bear} bear | 🔄 {choch} CHoCH | ⚠️ {filt} EMA-filtered"
        )

        # Filter controls
        cf1, cf2, cf3, cf4 = st.columns(4)
        with cf1: ftype     = st.selectbox("Type", ["All","BOS only","CHoCH only","Bullish","Bearish"], key="_bos_ft")
        with cf2: f_rr      = st.number_input("Min R:R", value=1.5, step=0.5, min_value=0.0, key="_bos_rr")
        with cf3: f_vol     = st.number_input("Min Vol Ratio", value=1.0, step=0.5, min_value=0.0, key="_bos_vl")
        with cf4: show_filt = st.checkbox("Show EMA-filtered", value=False, key="_bos_ef")

        filtered = []
        for ev in events:
            bt    = ev.get("bos_type", "")
            rr    = max(ev.get("rr_now") or 0, ev.get("rr_retest") or 0)
            vol   = ev.get("vol_ratio", 1.0)
            ema_f = ev.get("ema_filtered", False)
            if ftype == "BOS only"   and "CHOCH" in bt:  continue
            if ftype == "CHoCH only" and "CHOCH" not in bt: continue
            if ftype == "Bullish"    and "DOWN"  in bt:  continue
            if ftype == "Bearish"    and "UP"    in bt:  continue
            if rr < f_rr or vol < f_vol:                 continue
            if ema_f and not show_filt:                  continue
            filtered.append(ev)

        if not filtered:
            st.warning("No events match the current filters.")
        else:
            for ev in filtered:
                bt       = ev.get("bos_type", "")
                sym      = ev.get("symbol", "")
                ltp      = ev.get("ltp", 0)
                broken   = ev.get("broken", 0)
                sl       = ev.get("sl", 0)
                t1       = ev.get("t1", 0)
                t2       = ev.get("t2", 0)
                rr_now   = ev.get("rr_now", 0)
                rr_ret   = ev.get("rr_retest", 0)
                strength = ev.get("strength", 0)
                vol_r    = ev.get("vol_ratio", 1.0)
                ob_low   = ev.get("ob_low", 0)
                ob_high  = ev.get("ob_high", 0)
                nxt_liq  = ev.get("next_liq", 0)
                pt       = ev.get("prev_trend", "")
                alerted  = ev.get("alerted", False)
                ema_filt = ev.get("ema_filtered", False)
                ema_note = ev.get("ema_note", "")

                is_bull  = "UP"    in bt
                is_choch = "CHOCH" in bt
                icon     = "🚀" if is_bull and not is_choch else ("🔄" if is_choch else "💥")
                color    = "#0a2e0a" if (is_bull and not ema_filt) else ("#2e0a0a" if (not is_bull and not ema_filt) else "#2d2d00")
                bord     = "#00ff88" if (is_bull and not ema_filt) else ("#ff4444" if (not is_bull and not ema_filt) else "#ffaa00")
                ema_badge= "⚠️ EMA filtered" if ema_filt else "✅ EMA confirmed"

                st.markdown(f"""
<div style="border:2px solid {bord};border-radius:8px;background:{color};padding:10px 14px;margin:6px 0;">
  <div style="display:flex;justify-content:space-between;">
    <span style="font-size:16px;font-weight:bold;color:{bord};">{icon} {sym} — {bt.replace("_"," ")}</span>
    <span style="font-size:11px;color:#aaa;">
      {'✅ Alerted' if alerted else '⏳ Pending'} | {ema_badge} | Prior:{pt} | 1H
    </span>
  </div>
  <div style="font-size:11px;color:#aaa;margin-top:2px;">{ema_note}</div>
  <div style="display:flex;gap:14px;flex-wrap:wrap;margin-top:6px;font-size:12px;color:#ddd;">
    <span>LTP <b>{ltp:.1f}</b></span>
    <span>Broke <b>{broken:.1f}</b></span>
    <span>Strength <b>{strength:.2f}%</b></span>
    <span>Vol <b>{vol_r:.1f}x</b></span>
    <span>OB <b>{ob_low:.1f}–{ob_high:.1f}</b></span>
    <span>NextLiq <b>{nxt_liq:.1f}</b></span>
  </div>
  <div style="display:flex;gap:14px;flex-wrap:wrap;margin-top:4px;font-size:12px;color:#ddd;">
    <span>SL <b style="color:#ff8888;">{sl:.1f}</b></span>
    <span>T1 <b style="color:#88ff88;">{t1:.1f}</b></span>
    <span>T2 <b style="color:#88ff88;">{t2:.1f}</b></span>
    <span>R:R now <b>{rr_now}:1</b> | retest <b>{rr_ret}:1</b></span>
  </div>
</div>""", unsafe_allow_html=True)
