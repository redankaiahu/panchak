"""
chart_pattern_scanner.py — Chart Pattern Scanner for Panchak Dashboard
========================================================================
Detects 12 high-conviction chart patterns on daily candles:

BULLISH PATTERNS (6):
  1. Falling Wedge      — converging downtrend lines → explosive breakout
  2. Ascending Triangle — flat top resistance + rising lows
  3. Bullish Flag       — sharp pole up + tight rectangular consolidation
  4. Bullish Pennant    — sharp pole up + converging triangle consolidation
  5. Cup & Handle       — U-shaped recovery + small pullback handle
  6. Double Bottom      — two equal lows with rally between

BEARISH PATTERNS (6):
  7. Rising Wedge       — converging uptrend lines → breakdown
  8. Descending Triangle— flat bottom support + falling highs
  9. Bearish Flag       — sharp pole down + tight rectangular consolidation
  10. Bearish Pennant   — sharp pole down + converging triangle consolidation
  11. Head & Shoulders  — left shoulder + head + right shoulder + neckline
  12. Double Top        — two equal highs with pullback between

Each pattern returns:
  - symbol, pattern_name, direction (BULL/BEAR)
  - entry_price, sl_price, target_1, target_2, rr
  - breakout_time, breakout_price, post_break_pct (% move after breakout)
  - conviction_score (0-10)
  - OHLC context: LTP, Open, YEST_HIGH, YEST_LOW, YEST_CLOSE, CHANGE_%, VOL_%
  - pattern_bars (number of candles in pattern)
  - pole_pct (flag/pennant pole size)

Author: Panchak Dashboard v3.4
"""

from __future__ import annotations
import os, json, math
from datetime import datetime, date, timedelta
from typing import Optional

try:
    import pandas as pd
    import numpy as np
    _NUMPY_OK = True
except ImportError:
    _NUMPY_OK = False

# ── Pattern detection parameters ──────────────────────────────────────────────
MIN_PATTERN_BARS   = 5      # minimum candles for a pattern
MAX_PATTERN_BARS   = 60     # maximum lookback
MIN_POLE_PCT       = 3.0    # minimum % move for flag/pennant pole
MIN_BREAKOUT_VOL   = 1.3    # minimum volume ratio on breakout candle
WEDGE_SLOPE_THRESH = 0.001  # minimum slope convergence for wedge
TRIANGLE_FLAT_TOL  = 0.005  # 0.5% tolerance for "flat" side of triangle
DOUBLE_LEVEL_TOL   = 0.02   # 2% tolerance for double top/bottom equality
CUP_MIN_DEPTH      = 0.08   # minimum 8% depth for cup
CUP_MAX_DEPTH      = 0.50   # maximum 50% depth for cup
HS_SHOULDER_TOL    = 0.04   # 4% tolerance for shoulder equality


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _calc_rsi(prices: list, n: int = 14) -> float:
    if len(prices) < n + 1: return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    seed = deltas[:n]
    up = sum(d for d in seed if d > 0) / n
    down = sum(-d for d in seed if d < 0) / n
    if down == 0: return 100.0
    rs = up / down
    res = 100.0 - (100.0 / (1.0 + rs))
    for d in deltas[n:]:
        gain = d if d > 0 else 0
        loss = -d if d < 0 else 0
        up = (up * (n - 1) + gain) / n
        down = (down * (n - 1) + loss) / n
        if down == 0: rs = 100.0
        else: rs = up / down
        res = 100.0 - (100.0 / (1.0 + rs))
    return round(res, 2)

def _has_bullish_divergence(candles: list) -> bool:
    """Price lower low, RSI higher low."""
    if len(candles) < 20: return False
    closes = [c["close"] for c in candles]
    lows = [c["low"] for c in candles]
    # Local troughs in lows
    troughs = _find_local_troughs(lows, window=3)
    if len(troughs) < 2: return False
    t1, t2 = troughs[-2], troughs[-1]
    if lows[t2] < lows[t1]:
        rsi1 = _calc_rsi(closes[:t1+1])
        rsi2 = _calc_rsi(closes[:t2+1])
        if rsi2 > rsi1 + 2: return True
    return False

def _has_bearish_divergence(candles: list) -> bool:
    """Price higher high, RSI lower high."""
    if len(candles) < 20: return False
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    peaks = _find_local_peaks(highs, window=3)
    if len(peaks) < 2: return False
    p1, p2 = peaks[-2], peaks[-1]
    if highs[p2] > highs[p1]:
        rsi1 = _calc_rsi(closes[:p1+1])
        rsi2 = _calc_rsi(closes[:p2+1])
        if rsi2 < rsi1 - 2: return True
    return False

def _detect_nr7id(sym: str, candles: list, ltp: float) -> list:
    """NR7ID: Smallest range of 7 days + Inside Day."""
    if len(candles) < 8: return []
    ranges = [c["high"] - c["low"] for c in candles[-7:]]
    last_range = ranges[-1]
    if last_range == min(ranges): # NR7
        prev = candles[-2]
        curr = candles[-1]
        if curr["high"] <= prev["high"] and curr["low"] >= prev["low"]: # Inside Day
            # This is a volatility squeeze setup
            score = 8
            entry_up = curr["high"] + (last_range * 0.1)
            entry_dn = curr["low"] - (last_range * 0.1)
            results = []
            # Bullish scenario if LTP breaks high
            if ltp > curr["high"]:
                results.append({
                    "pattern": "NR7ID Squeeze", "direction": "BULL", "bars": 7,
                    "entry": round(ltp, 2), "sl": round(curr["low"], 2),
                    "t1": round(ltp + last_range * 2, 2), "t2": round(ltp + last_range * 4, 2),
                    "rr": 2.0, "vol_ratio": 1.0, "pole_pct": 0, "post_break_%": 0, "score": 9
                })
            elif ltp < curr["low"]:
                results.append({
                    "pattern": "NR7ID Squeeze", "direction": "BEAR", "bars": 7,
                    "entry": round(ltp, 2), "sl": round(curr["high"], 2),
                    "t1": round(ltp - last_range * 2, 2), "t2": round(ltp - last_range * 4, 2),
                    "rr": 2.0, "vol_ratio": 1.0, "pole_pct": 0, "post_break_%": 0, "score": 9
                })
            return results
    return []

def _detect_2b_reversal(sym: str, candles: list, ltp: float) -> list:
    """Trader Vic's 2B Pattern: Failure to sustain breakout."""
    if len(candles) < 20: return []
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    
    # Bearish 2B (Top)
    peaks = _find_local_peaks(highs[:-2], window=5)
    if peaks:
        prev_high = highs[peaks[-1]]
        if highs[-2] > prev_high and candles[-1]["close"] < prev_high:
            # Price broke high but closed back below it
            return [{
                "pattern": "2B Bearish Reversal", "direction": "BEAR", "bars": 10,
                "entry": round(ltp, 2), "sl": round(highs[-2], 2),
                "t1": round(ltp - (highs[-2]-ltp)*2, 2), "t2": round(ltp - (highs[-2]-ltp)*4, 2),
                "rr": 2.0, "vol_ratio": 1.5, "pole_pct": 0, "post_break_%": 0, "score": 9
            }]
            
    # Bullish 2B (Bottom)
    troughs = _find_local_troughs(lows[:-2], window=5)
    if troughs:
        prev_low = lows[troughs[-1]]
        if lows[-2] < prev_low and candles[-1]["close"] > prev_low:
            return [{
                "pattern": "2B Bullish Reversal", "direction": "BULL", "bars": 10,
                "entry": round(ltp, 2), "sl": round(lows[-2], 2),
                "t1": round(ltp + (ltp-lows[-2])*2, 2), "t2": round(ltp + (ltp-lows[-2])*4, 2),
                "rr": 2.0, "vol_ratio": 1.5, "pole_pct": 0, "post_break_%": 0, "score": 9
            }]
    return []

def _detect_dragon(sym: str, candles: list, ltp: float) -> list:
    """Dragon Pattern (Bullish variation of Double Bottom)."""
    if len(candles) < 25: return []
    lows = [c["low"] for c in candles]
    troughs = _find_local_troughs(lows, window=4)
    if len(troughs) < 2: return []
    
    t1_idx, t2_idx = troughs[-2], troughs[-1]
    if t2_idx - t1_idx < 10: return []
    
    head = max(c["high"] for c in candles[max(0, t1_idx-15):t1_idx])
    hump = max(c["high"] for c in candles[t1_idx:t2_idx])
    
    # Second leg (t2) should be higher than or equal to first leg (t1) - "tail" rising
    if lows[t2_idx] < lows[t1_idx] * 0.98: return []
    
    # Breakout above hump
    if ltp > hump:
        return [{
            "pattern": "Dragon Pattern", "direction": "BULL", "bars": t2_idx - t1_idx,
            "entry": round(ltp, 2), "sl": round(min(lows[t1_idx], lows[t2_idx]), 2),
            "t1": round(hump + (hump - lows[t2_idx]), 2),
            "t2": round(head, 2),
            "rr": 2.5, "vol_ratio": 1.3, "pole_pct": 0, "post_break_%": 0, "score": 9,
            "note": "High conviction reversal"
        }]
    return []

def _detect_adam_eve(sym: str, candles: list, ltp: float) -> list:
    """Adam & Eve Double Bottom (Sharp low followed by Rounded low)."""
    if len(candles) < 30: return []
    lows = [c["low"] for c in candles]
    troughs = _find_local_troughs(lows, window=3)
    if len(troughs) < 2: return []
    
    t1_idx, t2_idx = troughs[-2], troughs[-1]
    # Adam (t1) should be a sharp spike
    t1_range = candles[t1_idx-1:t1_idx+2]
    if len(t1_range) < 3: return []
    adam_sharpness = min(t1_range[0]["low"], t1_range[2]["low"]) - candles[t1_idx]["low"]
    
    # Eve (t2) should be more rounded (lows around t2_idx are similar)
    t2_range = [c["low"] for c in candles[t2_idx-2:t2_idx+3]]
    eve_flatness = max(t2_range) - min(t2_range)
    
    if adam_sharpness > eve_flatness * 2: # Adam is sharper than Eve
        neckline = max(c["high"] for c in candles[t1_idx:t2_idx])
        if ltp > neckline:
            return [{
                "pattern": "Adam & Eve (W)", "direction": "BULL", "bars": t2_idx - t1_idx,
                "entry": round(ltp, 2), "sl": round(min(lows[t1_idx], lows[t2_idx]), 2),
                "t1": round(neckline + (neckline - min(lows[t1_idx], lows[t2_idx])), 2),
                "t2": round(neckline + (neckline - min(lows[t1_idx], lows[t2_idx])) * 1.618, 2),
                "rr": 2.0, "vol_ratio": 1.2, "pole_pct": 0, "post_break_%": 0, "score": 8,
                "note": "Professional accumulation pattern"
            }]
    return []

def _detect_triple_bottom(sym: str, candles: list, ltp: float) -> list:
    """Triple Bottom (BULLISH): 3 equal lows."""
    if len(candles) < 25: return []
    lows_idx = _find_local_troughs([c["low"] for c in candles], window=3)
    if len(lows_idx) < 3: return []
    t3, t2, t1 = lows_idx[-1], lows_idx[-2], lows_idx[-3]
    l1, l2, l3 = candles[t1]["low"], candles[t2]["low"], candles[t3]["low"]
    avg_l = (l1 + l2 + l3) / 3
    if all(abs(_pct(avg_l, lx)) < 1.5 for lx in [l1, l2, l3]):
        neckline = max(c["high"] for c in candles[t1:t3])
        if ltp > neckline:
            return [{
                "pattern": "Triple Bottom", "direction": "BULL", "bars": t3 - t1,
                "entry": round(ltp, 2), "sl": round(avg_l * 0.998, 2),
                "t1": round(neckline + (neckline - avg_l), 2), "t2": round(neckline + (neckline - avg_l)*1.618, 2),
                "rr": 2.0, "vol_ratio": 1.4, "pole_pct": 0, "post_break_%": 0, "score": 9
            }]
    return []

def _detect_triple_top(sym: str, candles: list, ltp: float) -> list:
    """Triple Top (BEARISH): 3 equal highs."""
    if len(candles) < 25: return []
    highs_idx = _find_local_peaks([c["high"] for c in candles], window=3)
    if len(highs_idx) < 3: return []
    h3, h2, h1 = highs_idx[-1], highs_idx[-2], highs_idx[-3]
    v1, v2, v3 = candles[h1]["high"], candles[h2]["high"], candles[h3]["high"]
    avg_h = (v1 + v2 + v3) / 3
    if all(abs(_pct(avg_h, vx)) < 1.5 for vx in [v1, v2, v3]):
        neckline = min(c["low"] for c in candles[h1:h3])
        if ltp < neckline:
            return [{
                "pattern": "Triple Top", "direction": "BEAR", "bars": h3 - h1,
                "entry": round(ltp, 2), "sl": round(avg_h * 1.002, 2),
                "t1": round(neckline - (avg_h - neckline), 2), "t2": round(neckline - (avg_h - neckline)*1.618, 2),
                "rr": 2.0, "vol_ratio": 1.4, "pole_pct": 0, "post_break_%": 0, "score": 9
            }]
    return []

def _detect_symmetrical_triangle(sym: str, candles: list, ltp: float) -> list:
    """Symmetrical Triangle (Neutral until Breakout)."""
    results = []
    for n in [20, 30, 40]:
        if len(candles) < n + 2: continue
        seg = candles[-(n+2):-2]
        highs = [c["high"] for c in seg]
        lows = [c["low"] for c in seg]
        rs, rb = _resistance_line(highs)
        ss, sb = _support_line(lows)
        if rs < 0 and ss > 0: # Converging
            resist_now = rb + rs * (n - 1)
            support_now = sb + ss * (n - 1)
            if ltp > resist_now * 1.002:
                results.append({
                    "pattern": "Symmetrical Triangle", "direction": "BULL", "bars": n,
                    "entry": round(ltp, 2), "sl": round(support_now, 2),
                    "t1": round(ltp + (max(highs)-min(lows)), 2), "t2": round(ltp + (max(highs)-min(lows))*1.618, 2),
                    "rr": 1.8, "vol_ratio": 1.2, "pole_pct": 0, "post_break_%": 0, "score": 7
                })
            elif ltp < support_now * 0.998:
                results.append({
                    "pattern": "Symmetrical Triangle", "direction": "BEAR", "bars": n,
                    "entry": round(ltp, 2), "sl": round(resist_now, 2),
                    "t1": round(ltp - (max(highs)-min(lows)), 2), "t2": round(ltp - (max(highs)-min(lows))*1.618, 2),
                    "rr": 1.8, "vol_ratio": 1.2, "pole_pct": 0, "post_break_%": 0, "score": 7
                })
            break
    return results

def _detect_msh_msl(sym: str, candles: list, ltp: float) -> list:


    """Market Structure High/Low (3-bar fractal)."""
    if len(candles) < 5: return []
    c = candles[-3:]
    # MSL (Bottom)
    if c[1]["low"] < c[0]["low"] and c[1]["low"] < c[2]["low"]:
        if ltp > c[2]["high"]: # Break of fractal high
            return [{
                "pattern": "Market Structure Low (MSL)", "direction": "BULL", "bars": 3,
                "entry": round(ltp, 2), "sl": round(c[1]["low"], 2),
                "t1": round(ltp + (ltp-c[1]["low"])*2, 2), "t2": round(ltp + (ltp-c[1]["low"])*4, 2),
                "rr": 2.0, "vol_ratio": 1.2, "pole_pct": 0, "post_break_%": 0, "score": 8
            }]
    # MSH (Top)
    if c[1]["high"] > c[0]["high"] and c[1]["high"] > c[2]["high"]:
        if ltp < c[2]["low"]: # Break of fractal low
            return [{
                "pattern": "Market Structure High (MSH)", "direction": "BEAR", "bars": 3,
                "entry": round(ltp, 2), "sl": round(c[1]["high"], 2),
                "t1": round(ltp - (c[1]["high"]-ltp)*2, 2), "t2": round(ltp - (c[1]["high"]-ltp)*4, 2),
                "rr": 2.0, "vol_ratio": 1.2, "pole_pct": 0, "post_break_%": 0, "score": 8
            }]
    return []


def _linreg_slope(y_vals: list) -> float:
    """Simple linear regression slope (normalised)."""
    n = len(y_vals)
    if n < 2:
        return 0.0
    x = list(range(n))
    xm = sum(x) / n
    ym = sum(y_vals) / n
    num = sum((xi - xm) * (yi - ym) for xi, yi in zip(x, y_vals))
    den = sum((xi - xm) ** 2 for xi in x)
    return (num / den) if den != 0 else 0.0

def _avg_vol(candles: list, lookback: int = 20) -> float:
    vols = [c.get("volume", 0) for c in candles[-lookback:] if c.get("volume", 0) > 0]
    return sum(vols) / len(vols) if vols else 1.0

def _pct(a, b) -> float:
    """Percentage change from a to b."""
    return round((b - a) / a * 100, 2) if a else 0.0

def _rr(entry, sl, target, bull: bool) -> float:
    risk   = abs(entry - sl)
    reward = abs(target - entry)
    if risk <= 0:
        return 0.0
    return round(reward / risk, 1)

def _resistance_line(highs: list) -> tuple:
    """Fit line through highs → (slope, intercept)."""
    n = len(highs)
    x = list(range(n))
    s = _linreg_slope(highs)
    b = sum(highs) / n - s * sum(x) / n
    return s, b

def _support_line(lows: list) -> tuple:
    """Fit line through lows → (slope, intercept)."""
    n = len(lows)
    s = _linreg_slope(lows)
    b = (sum(lows) / n) - s * (sum(range(n)) / n) if n > 0 else 0.0
    return s, b

def _find_local_peaks(values: list, window: int = 3) -> list:
    """Indices of local maxima."""
    peaks = []
    for i in range(window, len(values) - window):
        if values[i] == max(values[i-window:i+window+1]):
            peaks.append(i)
    return peaks

def _find_local_troughs(values: list, window: int = 3) -> list:
    """Indices of local minima."""
    troughs = []
    for i in range(window, len(values) - window):
        if values[i] == min(values[i-window:i+window+1]):
            troughs.append(i)
    return troughs

def _candle_date_str(c: dict) -> str:
    dt = c.get("date") or c.get("datetime") or ""
    return str(dt)[:16] if dt else "—"

def _post_break_pct(candles: list, break_idx: int, break_price: float, direction: str) -> float:
    """
    Calculate % move from breakout candle to highest high (bull) or lowest low (bear)
    after the break. Uses subsequent candles only.
    """
    if break_idx >= len(candles) - 1:
        return 0.0
    post = candles[break_idx + 1:]
    if not post:
        return 0.0
    if direction == "BULL":
        extreme = max(c["high"] for c in post)
        return _pct(break_price, extreme)
    else:
        extreme = min(c["low"] for c in post)
        return _pct(break_price, extreme)

def _ang_dist(a, b):
    return abs((a - b + 180) % 360 - 180)

def _rashi(lon):
    return _RASHIS[int(lon / 30)]

def _nak(lon):
    idx = int(lon / (360/27))
    return _NAKS[idx], _NAK_LORDS[idx], idx

def _d9(lon):
    si = int(lon / 30)
    di = int((lon % 30) / (30.0/9))
    starts = {0:0,1:9,2:6,3:3,4:0,5:9,6:6,7:3,8:0,9:9,10:6,11:3}
    return _RASHIS[(starts.get(si,0) + di) % 12]


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN DETECTORS
# Each returns a list of pattern dicts or empty list.
# ══════════════════════════════════════════════════════════════════════════════

def _detect_falling_wedge(sym: str, candles: list, ltp: float) -> list:
    """
    Falling Wedge (BULLISH):
    - Both resistance line (highs) AND support line (lows) slope DOWN
    - Resistance line has STEEPER downward slope than support (converging)
    - Pattern takes 10-40 candles
    - Breakout: LTP > resistance line value at last candle
    """
    results = []
    for n in [15, 20, 30, 40]:
        if len(candles) < n + 3:
            continue
        seg = candles[-(n+3):-3]  # leave last 3 as breakout zone
        highs = [c["high"] for c in seg]
        lows  = [c["low"]  for c in seg]
        rs, rb = _resistance_line(highs)
        ss, sb = _support_line(lows)
        # Both must slope down
        if rs >= 0 or ss >= 0:
            continue
        # Resistance must fall faster (converging)
        if not (rs < ss):
            continue
        # Convergence must be meaningful
        if abs(rs - ss) < WEDGE_SLOPE_THRESH:
            continue
        # Current resistance level
        resist_now = rb + rs * (n - 1)
        support_now = sb + ss * (n - 1)
        if resist_now <= 0 or support_now <= 0:
            continue
        # Breakout: LTP above resistance
        if ltp <= resist_now * 1.002:
            continue
        # Breakout candle volume
        last_vol = candles[-1].get("volume", 0)
        avg_v    = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        # Pattern depth
        pattern_high = max(highs)
        pattern_low  = min(lows)
        pole_pct     = _pct(pattern_low, pattern_high)
        entry  = round(ltp, 2)
        sl     = round(support_now * 0.998, 2)
        target1= round(entry + (entry - sl) * 1.5, 2)
        target2= round(entry + (entry - sl) * 2.5, 2)
        # Post-breakout move
        post_pct = _post_break_pct(candles, len(candles)-3, resist_now, "BULL")
        score = min(10, int(3 + (vol_ratio >= 1.5) * 2 + (pole_pct > 10) * 2 + (post_pct > 3) * 3))
        results.append({
            "pattern":      "Falling Wedge",
            "direction":    "BULL",
            "bars":         n,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, True),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(pole_pct, 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(resist_now, 2),
            "support_level":round(support_now, 2),
        })
        break  # take the first (shortest) valid window
    return results


def _detect_rising_wedge(sym: str, candles: list, ltp: float) -> list:
    """Rising Wedge (BEARISH): both lines slope up, support steeper — converging upward."""
    results = []
    for n in [15, 20, 30, 40]:
        if len(candles) < n + 3:
            continue
        seg = candles[-(n+3):-3]
        highs = [c["high"] for c in seg]
        lows  = [c["low"]  for c in seg]
        rs, rb = _resistance_line(highs)
        ss, sb = _support_line(lows)
        # Both must slope up
        if rs <= 0 or ss <= 0:
            continue
        # Support rises faster (converging)
        if not (ss > rs):
            continue
        if abs(ss - rs) < WEDGE_SLOPE_THRESH:
            continue
        support_now  = sb + ss * (n - 1)
        resist_now   = rb + rs * (n - 1)
        if support_now <= 0:
            continue
        # Breakdown: LTP below support
        if ltp >= support_now * 0.998:
            continue
        last_vol  = candles[-1].get("volume", 0)
        avg_v     = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        pattern_high = max(highs)
        pattern_low  = min(lows)
        pole_pct     = _pct(pattern_low, pattern_high)
        entry  = round(ltp, 2)
        sl     = round(resist_now * 1.002, 2)
        target1= round(entry - (sl - entry) * 1.5, 2)
        target2= round(entry - (sl - entry) * 2.5, 2)
        post_pct = _post_break_pct(candles, len(candles)-3, support_now, "BEAR")
        score = min(10, int(3 + (vol_ratio >= 1.5)*2 + (pole_pct > 10)*2 + (abs(post_pct) > 3)*3))
        results.append({
            "pattern":      "Rising Wedge",
            "direction":    "BEAR",
            "bars":         n,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, False),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(pole_pct, 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(resist_now, 2),
            "support_level":round(support_now, 2),
        })
        break
    return results


def _detect_ascending_triangle(sym: str, candles: list, ltp: float) -> list:
    """
    Ascending Triangle (BULLISH):
    - Resistance: roughly flat (top < 0.5% slope over pattern)
    - Support: rising (positive slope)
    - Breakout: LTP > resistance level
    """
    results = []
    for n in [12, 20, 30]:
        if len(candles) < n + 2:
            continue
        seg = candles[-(n+2):-2]
        highs = [c["high"] for c in seg]
        lows  = [c["low"]  for c in seg]
        rs, rb = _resistance_line(highs)
        ss, sb = _support_line(lows)
        # Resistance roughly flat (|slope| < threshold relative to price)
        avg_h = sum(highs) / len(highs)
        if avg_h <= 0 or abs(rs) > avg_h * TRIANGLE_FLAT_TOL:
            continue
        # Support rising
        if ss <= 0:
            continue
        resist_now  = rb + rs * (n - 1)
        support_now = sb + ss * (n - 1)
        if ltp <= resist_now * 1.001:
            continue
        last_vol  = candles[-1].get("volume", 0)
        avg_v     = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        height    = resist_now - min(lows)
        entry  = round(ltp, 2)
        sl     = round(support_now * 0.998, 2)
        target1= round(resist_now + height, 2)        # measured move = pattern height
        target2= round(resist_now + height * 1.618, 2)
        post_pct = _post_break_pct(candles, len(candles)-2, resist_now, "BULL")
        score = min(10, int(4 + (vol_ratio >= 1.5)*2 + (post_pct > 3)*2 + (n >= 20)*2))
        results.append({
            "pattern":      "Ascending Triangle",
            "direction":    "BULL",
            "bars":         n,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, True),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(_pct(min(lows), resist_now), 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(resist_now, 2),
            "support_level":round(support_now, 2),
        })
        break
    return results


def _detect_descending_triangle(sym: str, candles: list, ltp: float) -> list:
    """
    Descending Triangle (BEARISH):
    - Support: roughly flat
    - Resistance: declining
    - Breakdown: LTP < support level
    """
    results = []
    for n in [12, 20, 30]:
        if len(candles) < n + 2:
            continue
        seg = candles[-(n+2):-2]
        highs = [c["high"] for c in seg]
        lows  = [c["low"]  for c in seg]
        rs, rb = _resistance_line(highs)
        ss, sb = _support_line(lows)
        avg_l = sum(lows) / len(lows)
        if avg_l <= 0 or abs(ss) > avg_l * TRIANGLE_FLAT_TOL:
            continue
        if rs >= 0:
            continue
        support_now = sb + ss * (n - 1)
        resist_now  = rb + rs * (n - 1)
        if ltp >= support_now * 0.999:
            continue
        last_vol  = candles[-1].get("volume", 0)
        avg_v     = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        height    = max(highs) - support_now
        entry  = round(ltp, 2)
        sl     = round(resist_now * 1.002, 2)
        target1= round(support_now - height, 2)
        target2= round(support_now - height * 1.618, 2)
        post_pct = _post_break_pct(candles, len(candles)-2, support_now, "BEAR")
        score = min(10, int(4 + (vol_ratio >= 1.5)*2 + (abs(post_pct) > 3)*2 + (n >= 20)*2))
        results.append({
            "pattern":      "Descending Triangle",
            "direction":    "BEAR",
            "bars":         n,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, False),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(_pct(support_now, max(highs)), 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(resist_now, 2),
            "support_level":round(support_now, 2),
        })
        break
    return results


def _detect_bull_flag(sym: str, candles: list, ltp: float) -> list:
    """
    Bullish Flag:
    - Pole: strong up move (≥ MIN_POLE_PCT) in 3-8 candles
    - Flag: 5-15 candle rectangular consolidation (slight downward drift, <50% pole retracement)
    - Breakout: LTP > flag high
    """
    results = []
    if len(candles) < 12:
        return results
    # Find pole: look for strong up move in last 3-8 candles before the flag
    for pole_len in [3, 4, 5, 6, 8]:
        for flag_len in [5, 8, 10, 12, 15]:
            total = pole_len + flag_len + 2
            if len(candles) < total:
                continue
            pole  = candles[-(total)   : -(total-pole_len)]
            flag  = candles[-(total-pole_len) : -2]
            break_c = candles[-2:]
            if not pole or not flag or len(flag) < 3:
                continue
            pole_low  = min(c["low"]  for c in pole)
            pole_high = max(c["high"] for c in pole)
            pole_pct  = _pct(pole_low, pole_high)
            if pole_pct < MIN_POLE_PCT:
                continue
            # Pole must be mostly green (close > open)
            pole_body = sum(1 for c in pole if c["close"] > c["open"])
            if pole_body < pole_len * 0.6:
                continue
            flag_high = max(c["high"] for c in flag)
            flag_low  = min(c["low"]  for c in flag)
            flag_range_pct = _pct(flag_low, flag_high)
            # Flag should be tight (<40% of pole) — rectangular consolidation
            if flag_range_pct > pole_pct * 0.45:
                continue
            # Flag must not retrace more than 50% of pole
            retrace = _pct(pole_high, flag_low)
            if retrace < -pole_pct * 0.5:
                continue
            # Breakout: LTP above flag high
            if ltp <= flag_high * 1.001:
                continue
            last_vol  = candles[-1].get("volume", 0)
            avg_v     = _avg_vol(candles, 20)
            vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
            entry  = round(ltp, 2)
            sl     = round(flag_low * 0.998, 2)
            target1= round(flag_high + (pole_high - pole_low), 2)  # measured move
            target2= round(flag_high + (pole_high - pole_low) * 1.5, 2)
            post_pct = _post_break_pct(candles, len(candles)-2, flag_high, "BULL")
            score = min(10, int(4 + (vol_ratio >= 1.5)*2 + (pole_pct > 8)*2 + (post_pct > 3)*2))
            results.append({
                "pattern":      "Bull Flag",
                "direction":    "BULL",
                "bars":         pole_len + flag_len,
                "entry":        entry,
                "sl":           sl,
                "t1":           target1,
                "t2":           target2,
                "rr":           _rr(entry, sl, target1, True),
                "vol_ratio":    vol_ratio,
                "pole_pct":     round(pole_pct, 1),
                "post_break_%": round(post_pct, 2),
                "score":        score,
                "resist_level": round(flag_high, 2),
                "support_level":round(flag_low, 2),
            })
            return results  # return first match
    return results


def _detect_bear_flag(sym: str, candles: list, ltp: float) -> list:
    """Bearish Flag: pole down + tight rectangular consolidation + breakdown below flag low."""
    results = []
    if len(candles) < 12:
        return results
    for pole_len in [3, 4, 5, 6, 8]:
        for flag_len in [5, 8, 10, 12, 15]:
            total = pole_len + flag_len + 2
            if len(candles) < total:
                continue
            pole = candles[-(total) : -(total-pole_len)]
            flag = candles[-(total-pole_len) : -2]
            if not pole or not flag or len(flag) < 3:
                continue
            pole_high = max(c["high"] for c in pole)
            pole_low  = min(c["low"]  for c in pole)
            pole_pct  = abs(_pct(pole_high, pole_low))
            if pole_pct < MIN_POLE_PCT:
                continue
            pole_body = sum(1 for c in pole if c["close"] < c["open"])
            if pole_body < pole_len * 0.6:
                continue
            flag_high = max(c["high"] for c in flag)
            flag_low  = min(c["low"]  for c in flag)
            flag_range_pct = _pct(flag_low, flag_high)
            if flag_range_pct > pole_pct * 0.45:
                continue
            retrace = _pct(pole_low, flag_high)
            if retrace > pole_pct * 0.5:
                continue
            if ltp >= flag_low * 0.999:
                continue
            last_vol  = candles[-1].get("volume", 0)
            avg_v     = _avg_vol(candles, 20)
            vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
            entry  = round(ltp, 2)
            sl     = round(flag_high * 1.002, 2)
            target1= round(flag_low - (pole_high - pole_low), 2)
            target2= round(flag_low - (pole_high - pole_low) * 1.5, 2)
            post_pct = _post_break_pct(candles, len(candles)-2, flag_low, "BEAR")
            score = min(10, int(4 + (vol_ratio >= 1.5)*2 + (pole_pct > 8)*2 + (abs(post_pct) > 3)*2))
            results.append({
                "pattern":      "Bear Flag",
                "direction":    "BEAR",
                "bars":         pole_len + flag_len,
                "entry":        entry,
                "sl":           sl,
                "t1":           target1,
                "t2":           target2,
                "rr":           _rr(entry, sl, target1, False),
                "vol_ratio":    vol_ratio,
                "pole_pct":     round(pole_pct, 1),
                "post_break_%": round(post_pct, 2),
                "score":        score,
                "resist_level": round(flag_high, 2),
                "support_level":round(flag_low, 2),
            })
            return results
    return results


def _detect_bull_pennant(sym: str, candles: list, ltp: float) -> list:
    """
    Bullish Pennant: pole up + converging symmetrical triangle (both lines converge) + breakout up.
    Key difference from flag: the consolidation is a pennant/triangle (converging), not rectangular.
    """
    results = []
    if len(candles) < 12:
        return results
    for pole_len in [3, 4, 5]:
        for pnn_len in [5, 8, 10, 12]:
            total = pole_len + pnn_len + 2
            if len(candles) < total:
                continue
            pole    = candles[-(total) : -(total-pole_len)]
            pennant = candles[-(total-pole_len) : -2]
            if len(pennant) < 4:
                continue
            pole_low  = min(c["low"]  for c in pole)
            pole_high = max(c["high"] for c in pole)
            pole_pct  = _pct(pole_low, pole_high)
            if pole_pct < MIN_POLE_PCT:
                continue
            # Pennant: highs declining, lows rising (converging)
            p_highs = [c["high"] for c in pennant]
            p_lows  = [c["low"]  for c in pennant]
            rs, _   = _resistance_line(p_highs)
            ss, _   = _support_line(p_lows)
            if rs >= 0 or ss <= 0:
                continue  # not converging
            # Convergence point should be within reasonable distance
            pnn_high = max(p_highs)
            pnn_low  = min(p_lows)
            if ltp <= pnn_high * 1.001:
                continue
            last_vol  = candles[-1].get("volume", 0)
            avg_v     = _avg_vol(candles, 20)
            vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
            entry  = round(ltp, 2)
            sl     = round(pnn_low * 0.998, 2)
            target1= round(pnn_high + (pole_high - pole_low), 2)
            target2= round(pnn_high + (pole_high - pole_low) * 1.618, 2)
            post_pct = _post_break_pct(candles, len(candles)-2, pnn_high, "BULL")
            score = min(10, int(5 + (vol_ratio >= 1.5)*2 + (pole_pct > 8)*2 + (post_pct > 3)*1))
            results.append({
                "pattern":      "Bull Pennant",
                "direction":    "BULL",
                "bars":         pole_len + pnn_len,
                "entry":        entry,
                "sl":           sl,
                "t1":           target1,
                "t2":           target2,
                "rr":           _rr(entry, sl, target1, True),
                "vol_ratio":    vol_ratio,
                "pole_pct":     round(pole_pct, 1),
                "post_break_%": round(post_pct, 2),
                "score":        score,
                "resist_level": round(pnn_high, 2),
                "support_level":round(pnn_low, 2),
            })
            return results
    return results


def _detect_bear_pennant(sym: str, candles: list, ltp: float) -> list:
    """Bearish Pennant: pole down + converging triangle + breakdown."""
    results = []
    if len(candles) < 12:
        return results
    for pole_len in [3, 4, 5]:
        for pnn_len in [5, 8, 10, 12]:
            total = pole_len + pnn_len + 2
            if len(candles) < total:
                continue
            pole    = candles[-(total) : -(total-pole_len)]
            pennant = candles[-(total-pole_len) : -2]
            if len(pennant) < 4:
                continue
            pole_high = max(c["high"] for c in pole)
            pole_low  = min(c["low"]  for c in pole)
            pole_pct  = abs(_pct(pole_high, pole_low))
            if pole_pct < MIN_POLE_PCT:
                continue
            p_highs = [c["high"] for c in pennant]
            p_lows  = [c["low"]  for c in pennant]
            rs, _ = _resistance_line(p_highs)
            ss, _ = _support_line(p_lows)
            if rs >= 0 or ss <= 0:
                continue
            pnn_high = max(p_highs)
            pnn_low  = min(p_lows)
            if ltp >= pnn_low * 0.999:
                continue
            last_vol  = candles[-1].get("volume", 0)
            avg_v     = _avg_vol(candles, 20)
            vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
            entry  = round(ltp, 2)
            sl     = round(pnn_high * 1.002, 2)
            target1= round(pnn_low - (pole_high - pole_low), 2)
            target2= round(pnn_low - (pole_high - pole_low) * 1.618, 2)
            post_pct = _post_break_pct(candles, len(candles)-2, pnn_low, "BEAR")
            score = min(10, int(5 + (vol_ratio >= 1.5)*2 + (pole_pct > 8)*2 + (abs(post_pct) > 3)*1))
            results.append({
                "pattern":      "Bear Pennant",
                "direction":    "BEAR",
                "bars":         pole_len + pnn_len,
                "entry":        entry,
                "sl":           sl,
                "t1":           target1,
                "t2":           target2,
                "rr":           _rr(entry, sl, target1, False),
                "vol_ratio":    vol_ratio,
                "pole_pct":     round(pole_pct, 1),
                "post_break_%": round(post_pct, 2),
                "score":        score,
                "resist_level": round(pnn_high, 2),
                "support_level":round(pnn_low, 2),
            })
            return results
    return results


def _detect_double_bottom(sym: str, candles: list, ltp: float) -> list:
    """
    Double Bottom (BULLISH):
    - Two lows roughly equal (within 2%)
    - Rally between (neckline) at least 5% above the lows
    - LTP breaks above neckline → entry
    """
    results = []
    if len(candles) < 15:
        return results
    lows_idx = _find_local_troughs([c["low"] for c in candles], window=3)
    highs_idx = _find_local_peaks([c["high"] for c in candles], window=3)
    if len(lows_idx) < 2:
        return results
    # Take last two troughs
    for i in range(len(lows_idx)-1, 0, -1):
        t2_i = lows_idx[i]
        t1_i = lows_idx[i-1]
        if t2_i - t1_i < 5 or t2_i - t1_i > 50:
            continue
        t1 = candles[t1_i]["low"]
        t2 = candles[t2_i]["low"]
        # Roughly equal lows
        if abs(_pct(t1, t2)) > DOUBLE_LEVEL_TOL * 100:
            continue
        # Neckline: highest high between the two lows
        between = candles[t1_i:t2_i]
        if not between:
            continue
        neckline = max(c["high"] for c in between)
        avg_low  = (t1 + t2) / 2
        if _pct(avg_low, neckline) < 3.0:  # neckline must be at least 3% above lows
            continue
        # Breakout above neckline
        if ltp <= neckline * 1.001:
            continue
        last_vol  = candles[-1].get("volume", 0)
        avg_v     = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        pattern_height = neckline - avg_low
        entry  = round(ltp, 2)
        sl     = round(avg_low * 0.998, 2)
        target1= round(neckline + pattern_height, 2)
        target2= round(neckline + pattern_height * 1.618, 2)
        post_pct = _post_break_pct(candles, t2_i, neckline, "BULL")
        div_bonus = 2 if _has_bullish_divergence(candles) else 0
        score = min(10, int(5 + (vol_ratio >= 1.5)*2 + (pattern_height/avg_low > 0.08)*2 + (post_pct > 3)*1 + div_bonus))
        results.append({
            "pattern":      "Double Bottom",
            "direction":    "BULL",
            "bars":         t2_i - t1_i,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, True),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(_pct(avg_low, neckline), 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(neckline, 2),
            "support_level":round(avg_low, 2),
            "note": "Bull Div confirmed" if div_bonus else ""
        })
        break
    return results


def _detect_double_top(sym: str, candles: list, ltp: float) -> list:
    """
    Double Top (BEARISH): two equal highs + breakdown below neckline.
    """
    results = []
    if len(candles) < 15:
        return results
    highs_idx = _find_local_peaks([c["high"] for c in candles], window=3)
    if len(highs_idx) < 2:
        return results
    for i in range(len(highs_idx)-1, 0, -1):
        h2_i = highs_idx[i]
        h1_i = highs_idx[i-1]
        if h2_i - h1_i < 5 or h2_i - h1_i > 50:
            continue
        h1 = candles[h1_i]["high"]
        h2 = candles[h2_i]["high"]
        if abs(_pct(h1, h2)) > DOUBLE_LEVEL_TOL * 100:
            continue
        between  = candles[h1_i:h2_i]
        if not between:
            continue
        neckline = min(c["low"] for c in between)
        avg_high = (h1 + h2) / 2
        if _pct(neckline, avg_high) < 3.0:
            continue
        if ltp >= neckline * 0.999:
            continue
        last_vol  = candles[-1].get("volume", 0)
        avg_v     = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        pattern_height = avg_high - neckline
        entry  = round(ltp, 2)
        sl     = round(avg_high * 1.002, 2)
        target1= round(neckline - pattern_height, 2)
        target2= round(neckline - pattern_height * 1.618, 2)
        post_pct = _post_break_pct(candles, h2_i, neckline, "BEAR")
        div_bonus = 2 if _has_bearish_divergence(candles) else 0
        score = min(10, int(5 + (vol_ratio >= 1.5)*2 + (pattern_height/avg_high > 0.08)*2 + (abs(post_pct) > 3)*1 + div_bonus))
        results.append({
            "pattern":      "Double Top",
            "direction":    "BEAR",
            "bars":         h2_i - h1_i,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, False),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(_pct(neckline, avg_high), 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(avg_high, 2),
            "support_level":round(neckline, 2),
            "note": "Bear Div confirmed" if div_bonus else ""
        })
        break
    return results


def _detect_cup_and_handle(sym: str, candles: list, ltp: float) -> list:
    """
    Cup & Handle (BULLISH):
    - Cup: U-shaped recovery over 15-40 candles with 8-50% depth
    - Handle: 5-15 candle slight pullback (< 50% of cup depth)
    - Breakout: above left rim of cup (neckline)
    """
    results = []
    for cup_len in [15, 20, 30, 40]:
        for hdl_len in [5, 8, 10, 15]:
            total = cup_len + hdl_len + 2
            if len(candles) < total:
                continue
            cup    = candles[-(total) : -(total-cup_len)]
            handle = candles[-(total-cup_len) : -2]
            if len(handle) < 3:
                continue
            cup_left_high  = cup[0]["high"]
            cup_right_high = cup[-1]["high"]
            cup_bottom     = min(c["low"] for c in cup)
            neckline       = min(cup_left_high, cup_right_high)
            depth          = _pct(neckline, cup_bottom)  # negative = down
            if depth > -CUP_MIN_DEPTH * 100 or depth < -CUP_MAX_DEPTH * 100:
                continue
            # Handle: slight pullback
            hdl_high  = max(c["high"] for c in handle)
            hdl_low   = min(c["low"]  for c in handle)
            hdl_retrace = _pct(neckline, hdl_low)  # should be small negative
            if hdl_retrace < depth * 0.5:  # handle can't retrace more than half the cup
                continue
            # Breakout above neckline
            if ltp <= neckline * 1.001:
                continue
            last_vol  = candles[-1].get("volume", 0)
            avg_v     = _avg_vol(candles, 20)
            vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
            cup_depth_pts = neckline - cup_bottom
            entry  = round(ltp, 2)
            sl     = round(hdl_low * 0.998, 2)
            target1= round(neckline + cup_depth_pts, 2)
            target2= round(neckline + cup_depth_pts * 1.618, 2)
            post_pct = _post_break_pct(candles, len(candles)-(hdl_len+2), neckline, "BULL")
            score = min(10, int(5 + (vol_ratio >= 1.5)*2 + (cup_len >= 20)*1 + (post_pct > 5)*2))
            results.append({
                "pattern":      "Cup & Handle",
                "direction":    "BULL",
                "bars":         cup_len + hdl_len,
                "entry":        entry,
                "sl":           sl,
                "t1":           target1,
                "t2":           target2,
                "rr":           _rr(entry, sl, target1, True),
                "vol_ratio":    vol_ratio,
                "pole_pct":     round(abs(depth), 1),
                "post_break_%": round(post_pct, 2),
                "score":        score,
                "resist_level": round(neckline, 2),
                "support_level":round(cup_bottom, 2),
            })
            return results
    return results


def _detect_head_and_shoulders(sym: str, candles: list, ltp: float) -> list:
    """
    Head & Shoulders (BEARISH):
    - Left shoulder peak + higher head peak + right shoulder peak (≈ left shoulder)
    - Neckline: line through the two troughs between shoulders and head
    - Breakdown: LTP < neckline
    """
    results = []
    if len(candles) < 20:
        return results
    highs_idx = _find_local_peaks([c["high"] for c in candles], window=2)
    if len(highs_idx) < 3:
        return results
    # Scan last 3 peaks for H&S structure
    for i in range(len(highs_idx)-1, 1, -1):
        rs_i = highs_idx[i]      # right shoulder
        hd_i = highs_idx[i-1]   # head
        ls_i = highs_idx[i-2]   # left shoulder
        if rs_i - ls_i < 8 or rs_i - ls_i > 60:
            continue
        ls = candles[ls_i]["high"]
        hd = candles[hd_i]["high"]
        rs = candles[rs_i]["high"]
        # Head must be highest
        if not (hd > ls and hd > rs):
            continue
        # Shoulders roughly equal (within 4%)
        if abs(_pct(ls, rs)) > HS_SHOULDER_TOL * 100:
            continue
        # Neckline: avg of the two troughs
        trough1 = min(c["low"] for c in candles[ls_i:hd_i])
        trough2 = min(c["low"] for c in candles[hd_i:rs_i])
        neckline = (trough1 + trough2) / 2
        # Pattern height
        height = hd - neckline
        if height / neckline < 0.03:  # head must be at least 3% above neckline
            continue
        # Breakdown below neckline
        if ltp >= neckline * 0.999:
            continue
        last_vol  = candles[-1].get("volume", 0)
        avg_v     = _avg_vol(candles, 20)
        vol_ratio = round(last_vol / avg_v, 1) if avg_v > 0 else 1.0
        entry  = round(ltp, 2)
        sl     = round(((ls + rs) / 2) * 1.002, 2)
        target1= round(neckline - height, 2)
        target2= round(neckline - height * 1.618, 2)
        post_pct = _post_break_pct(candles, rs_i, neckline, "BEAR")
        score = min(10, int(6 + (vol_ratio >= 1.5)*2 + (abs(post_pct) > 5)*2))
        results.append({
            "pattern":      "Head & Shoulders",
            "direction":    "BEAR",
            "bars":         rs_i - ls_i,
            "entry":        entry,
            "sl":           sl,
            "t1":           target1,
            "t2":           target2,
            "rr":           _rr(entry, sl, target1, False),
            "vol_ratio":    vol_ratio,
            "pole_pct":     round(_pct(neckline, hd), 1),
            "post_break_%": round(post_pct, 2),
            "score":        score,
            "resist_level": round(hd, 2),
            "support_level":round(neckline, 2),
        })
        break
    return results


# ══════════════════════════════════════════════════════════════════════════════
# MASTER SCANNER
# ══════════════════════════════════════════════════════════════════════════════

ALL_DETECTORS = [
    _detect_falling_wedge,
    _detect_ascending_triangle,
    _detect_bull_flag,
    _detect_bull_pennant,
    _detect_cup_and_handle,
    _detect_double_bottom,
    _detect_rising_wedge,
    _detect_descending_triangle,
    _detect_bear_flag,
    _detect_bear_pennant,
    _detect_head_and_shoulders,
    _detect_double_top,
    _detect_nr7id,
    _detect_2b_reversal,
    _detect_msh_msl,
    _detect_dragon,
    _detect_adam_eve,
    _detect_triple_bottom,
    _detect_triple_top,
    _detect_symmetrical_triangle,
]

def scan_chart_patterns(
    kite,
    symbols: list,
    get_token_fn,
    live_df=None,
    min_score: int = 4,
    min_rr: float = 1.2,
) -> list:
    """
    Run all 12 pattern detectors on daily candles for all symbols.

    Returns list of result dicts sorted by conviction score (highest first).
    Each dict contains full OHLC context from live_df.
    """
    from datetime import date as _date, timedelta as _td
    import time as _time

    today   = _date.today()
    end_d   = today
    start_d = end_d - _td(days=120)   # 4 months of daily candles (~90 bars)

    results = []

    for sym in symbols:
        try:
            tok = get_token_fn(sym)
            if not tok:
                continue
            bars = kite.historical_data(tok, start_d, end_d, "day")
            if not bars or len(bars) < 15:
                continue

            # Normalize bar keys
            candles = []
            for b in bars:
                candles.append({
                    "date":     str(b.get("date", "")),
                    "open":     float(b.get("open", 0)),
                    "high":     float(b.get("high", 0)),
                    "low":      float(b.get("low", 0)),
                    "close":    float(b.get("close", 0)),
                    "volume":   int(b.get("volume", 0)),
                })

            # Get live LTP from live_df if available
            ltp = float(candles[-1]["close"])
            if live_df is not None and not live_df.empty:
                try:
                    _r = live_df[live_df["Symbol"] == sym]
                    if not _r.empty:
                        ltp = float(_r.iloc[0]["LTP"])
                except Exception:
                    pass

            # Get OHLC context
            yest      = candles[-2] if len(candles) >= 2 else candles[-1]
            yest_high = yest["high"]
            yest_low  = yest["low"]
            yest_close= yest["close"]
            yest_open = yest["open"]
            live_open = 0.0
            chg_pct   = 0.0
            vol_pct   = 0.0
            if live_df is not None and not live_df.empty:
                try:
                    _r = live_df[live_df["Symbol"] == sym]
                    if not _r.empty:
                        live_open = float(_r.iloc[0].get("LIVE_OPEN", 0) or 0)
                        chg_pct   = float(_r.iloc[0].get("CHANGE_%", 0) or 0)
                        vol_pct   = float(_r.iloc[0].get("VOL_%", 0) or 0)
                except Exception:
                    pass

            # Run all detectors
            closes = [c["close"] for c in candles]
            rsi_now = _calc_rsi(closes)
            
            for detector in ALL_DETECTORS:
                try:
                    hits = detector(sym, candles, ltp)
                    for hit in hits:
                        # Conviction bonus for RSI alignment
                        rsi_bonus = 0
                        if hit["direction"] == "BULL" and rsi_now > 55: rsi_bonus = 1
                        if hit["direction"] == "BEAR" and rsi_now < 45: rsi_bonus = 1
                        hit["score"] = min(10, hit.get("score", 0) + rsi_bonus)
                        
                        if hit.get("score", 0) < min_score:
                            continue
                        if hit.get("rr", 0) < min_rr:
                            continue
                        # Enrich with live OHLC context
                        hit.update({
                            "symbol":     sym,
                            "ltp":        round(ltp, 2),
                            "rsi":        rsi_now,
                            "open":       round(live_open, 2),
                            "yest_high":  round(yest_high, 2),
                            "yest_low":   round(yest_low, 2),
                            "yest_close": round(yest_close, 2),
                            "yest_open":  round(yest_open, 2),
                            "change_%":   round(chg_pct, 2),
                            "vol_%":      round(vol_pct, 1),
                            "scan_time":  datetime.now().strftime("%H:%M"),
                            "total_candles": len(candles),
                        })
                        results.append(hit)
                except Exception:
                    continue

        except Exception:
            continue
        _time.sleep(0.12)   # rate limit

    # Sort: score descending, then R:R descending
    results.sort(key=lambda x: (-x.get("score", 0), -x.get("rr", 0)))
    return results
