# =============================================================
# smc_engine.py  — Smart Money Concept (SMC) Engine for NSE
# =============================================================
# Integrates SMC price-structure analysis with OI data to give
# a CONFLUENT signal that prevents pure-OI false bearish reads
# during institutional markup (buy-side accumulation) phases.
#
# Core SMC concepts implemented:
#   • Market Structure (BOS / CHoCH) from OHLC data
#   • Order Blocks (OB) — last opposite-colour candle before expansion
#   • Fair Value Gaps (FVG) — imbalances / price voids
#   • Liquidity Pools — equal highs/lows; stop hunts
#   • Premium / Discount zones
#   • OI + Price Confluence scoring
#
# Drop-in usage:
#   from smc_engine import get_smc_confluence
#   result = get_smc_confluence(oi_intel_dict, ohlc_list)
# =============================================================

from __future__ import annotations
import json
import math
from datetime import datetime, timedelta
from typing import Optional

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
except ImportError:
    IST = None

# ─────────────────────────────────────────────────────────────
# 1. MARKET STRUCTURE  (BOS / CHoCH)
# ─────────────────────────────────────────────────────────────

def detect_market_structure(candles: list[dict]) -> dict:
    """
    Detect Break of Structure (BOS) and Change of Character (CHoCH).

    candles: list of dicts with keys:
        open, high, low, close, datetime  (sorted oldest → newest)

    Returns:
        {
          "trend":      "BULLISH" | "BEARISH" | "RANGING",
          "bos":        list of {type:"BOS_UP"|"BOS_DOWN", price, index},
          "choch":      list of {type:"CHoCH_UP"|"CHoCH_DOWN", price, index},
          "last_hh":    float (last Higher High price),
          "last_hl":    float (last Higher Low price),
          "last_lh":    float (last Lower High price),
          "last_ll":    float (last Lower Low price),
          "swing_highs": list[float],
          "swing_lows":  list[float],
          "structure_summary": str,
        }
    """
    if not candles or len(candles) < 5:
        return {"trend": "RANGING", "bos": [], "choch": [], "structure_summary": "Insufficient data"}

    highs = [c["high"] for c in candles]
    lows  = [c["low"]  for c in candles]
    n     = len(candles)

    # ── Pivot swing detection (simplified: local max/min over ±2 bars) ──
    swing_highs = []
    swing_lows  = []
    for i in range(2, n - 2):
        if highs[i] == max(highs[i-2:i+3]):
            swing_highs.append((i, highs[i]))
        if lows[i]  == min(lows[i-2:i+3]):
            swing_lows.append((i, lows[i]))

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {"trend": "RANGING", "bos": [], "choch": [],
                "swing_highs": [h for _, h in swing_highs],
                "swing_lows":  [l for _, l in swing_lows],
                "structure_summary": "Not enough swing points"}

    bos_events   = []
    choch_events = []

    # ── Classify last two swing highs and lows ──
    recent_highs = swing_highs[-3:]
    recent_lows  = swing_lows[-3:]

    # Higher Highs / Lower Lows patterns
    hh_count = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i][1] > recent_highs[i-1][1])
    hl_count = sum(1 for i in range(1, len(recent_lows))  if recent_lows[i][1]  > recent_lows[i-1][1])
    lh_count = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i][1] < recent_highs[i-1][1])
    ll_count = sum(1 for i in range(1, len(recent_lows))  if recent_lows[i][1]  < recent_lows[i-1][1])

    # BOS detection — current close breaks last swing high/low
    last_close = candles[-1]["close"]
    prev_swing_high = recent_highs[-2][1] if len(recent_highs) >= 2 else None
    prev_swing_low  = recent_lows[-2][1]  if len(recent_lows)  >= 2 else None
    last_swing_high = recent_highs[-1][1]
    last_swing_low  = recent_lows[-1][1]

    if prev_swing_high and last_close > prev_swing_high:
        bos_events.append({"type": "BOS_UP", "price": prev_swing_high, "index": n-1})
    if prev_swing_low and last_close < prev_swing_low:
        bos_events.append({"type": "BOS_DOWN", "price": prev_swing_low, "index": n-1})

    # CHoCH — first opposite BOS after a run
    if hh_count >= 1 and hl_count >= 1 and prev_swing_low and last_close < prev_swing_low:
        choch_events.append({"type": "CHoCH_DOWN", "price": prev_swing_low, "index": n-1})
    if lh_count >= 1 and ll_count >= 1 and prev_swing_high and last_close > prev_swing_high:
        choch_events.append({"type": "CHoCH_UP", "price": prev_swing_high, "index": n-1})

    # ── Overall trend classification ──
    bull_score = hh_count + hl_count
    bear_score = lh_count + ll_count

    if bull_score >= 2 and bear_score == 0:
        trend = "BULLISH"
    elif bear_score >= 2 and bull_score == 0:
        trend = "BEARISH"
    elif bull_score > bear_score:
        trend = "BULLISH_BIAS"
    elif bear_score > bull_score:
        trend = "BEARISH_BIAS"
    else:
        trend = "RANGING"

    # Override: if we just had a BOS_UP, it's bullish regardless
    if bos_events and bos_events[-1]["type"] == "BOS_UP":
        trend = "BULLISH"
    elif bos_events and bos_events[-1]["type"] == "BOS_DOWN":
        trend = "BEARISH"

    summary_parts = [f"Trend: {trend}"]
    if bos_events:
        b = bos_events[-1]
        summary_parts.append(f"BOS {'UP' if 'UP' in b['type'] else 'DOWN'} at {b['price']:.0f}")
    if choch_events:
        c = choch_events[-1]
        summary_parts.append(f"CHoCH {'UP' if 'UP' in c['type'] else 'DOWN'} at {c['price']:.0f}")

    return {
        "trend":    trend,
        "bos":      bos_events,
        "choch":    choch_events,
        "last_hh":  max((h for _, h in recent_highs), default=0),
        "last_hl":  max((l for _, l in recent_lows),  default=0),
        "last_lh":  min((h for _, h in recent_highs), default=0),
        "last_ll":  min((l for _, l in recent_lows),  default=0),
        "swing_highs": [h for _, h in swing_highs],
        "swing_lows":  [l for _, l in swing_lows],
        "structure_summary": " | ".join(summary_parts),
    }


# ─────────────────────────────────────────────────────────────
# 2. ORDER BLOCKS (OB)
# ─────────────────────────────────────────────────────────────

def find_order_blocks(candles: list[dict], n_look: int = 40) -> dict:
    """
    Identify Bullish and Bearish Order Blocks (OB) and Breaker Blocks (BB).

    Bullish OB  = last bearish candle BEFORE an impulsive up-move.
    Bearish OB  = last bullish candle BEFORE an impulsive down-move.
    
    Breaker Block (BB):
    - Bullish OB closed BELOW = Bearish Breaker Block (resistance).
    - Bearish OB closed ABOVE = Bullish Breaker Block (support).

    Returns:
        {
          "bullish_obs": list, "bearish_obs": list,
          "bullish_bbs": list, "bearish_bbs": list,
          "nearest_bullish_ob": dict | None,
          "nearest_bearish_ob": dict | None,
        }
    """
    if not candles or len(candles) < 5:
        return {"bullish_obs": [], "bearish_obs": [], "bullish_bbs": [], "bearish_bbs": [],
                "nearest_bullish_ob": None, "nearest_bearish_ob": None}

    recent = candles[-min(n_look, len(candles)):]
    n = len(recent)
    spot = recent[-1]["close"]

    bullish_obs = []
    bearish_obs = []
    bullish_bbs = []
    bearish_bbs = []

    # Impulse threshold: candle body ≥ 0.3% of price
    impulse_pct = 0.003

    for i in range(1, n - 2):
        c    = recent[i]
        c_body = abs(c["close"] - c["open"])
        
        # Look at next 1-3 candles for impulse displacement
        impulse_window = recent[i+1:min(i+4, n)]
        if not impulse_window:
            continue

        # ── Potential Bullish OB: down-close candle + next candle(s) strongly up ──
        if c["close"] < c["open"]:
            up_move = sum(max(iw["close"] - iw["open"], 0) for iw in impulse_window)
            if c["open"] > 0 and up_move / c["open"] >= impulse_pct:
                validated = any(fw["high"] > c["high"] for fw in recent[i+1:min(i+6, n)])
                strength = round(up_move / c["open"] * 100, 2)
                ob = {
                    "open":      c["open"],
                    "high":      c["high"],
                    "low":       c["low"],
                    "close":     c["close"],
                    "midpoint":  round((c["high"] + c["low"]) / 2, 1),
                    "index":     i,
                    "validated": validated,
                    "strength":  strength,
                    "ob_type":   "BULLISH",
                }
                # Check if it was later CLOSED BELOW (Breaker)
                later = recent[i+1:]
                broken = False
                for lc in later:
                    if lc["close"] < c["low"]:
                        broken = True
                        break
                
                if broken:
                    ob["ob_type"] = "BEARISH_BREAKER"
                    bearish_bbs.append(ob)
                else:
                    bullish_obs.append(ob)

        # ── Potential Bearish OB: up-close candle + next candle(s) strongly down ──
        if c["close"] > c["open"]:
            down_move = sum(max(iw["open"] - iw["close"], 0) for iw in impulse_window)
            if c["open"] > 0 and down_move / c["open"] >= impulse_pct:
                validated = any(fw["low"] < c["low"] for fw in recent[i+1:min(i+6, n)])
                strength = round(down_move / c["open"] * 100, 2)
                ob = {
                    "open":      c["open"],
                    "high":      c["high"],
                    "low":       c["low"],
                    "close":     c["close"],
                    "midpoint":  round((c["high"] + c["low"]) / 2, 1),
                    "index":     i,
                    "validated": validated,
                    "strength":  strength,
                    "ob_type":   "BEARISH",
                }
                # Check if it was later CLOSED ABOVE (Breaker)
                later = recent[i+1:]
                broken = False
                for lc in later:
                    if lc["close"] > c["high"]:
                        broken = True
                        break
                
                if broken:
                    ob["ob_type"] = "BULLISH_BREAKER"
                    bullish_bbs.append(ob)
                else:
                    bearish_obs.append(ob)

    # Find nearest OBs or BBs relative to spot
    bullish_below = [ob for ob in bullish_obs if ob["high"] < spot] + \
                    [bb for bb in bullish_bbs if bb["high"] < spot]
    
    bearish_above = [ob for ob in bearish_obs if ob["low"]  > spot] + \
                    [bb for bb in bearish_bbs if bb["low"]  > spot]

    nearest_bull = max(bullish_below, key=lambda x: x["high"], default=None)
    nearest_bear = min(bearish_above, key=lambda x: x["low"],  default=None)

    return {
        "bullish_obs":         bullish_obs,
        "bearish_obs":         bearish_obs,
        "bullish_bbs":         bullish_bbs,
        "bearish_bbs":         bearish_bbs,
        "nearest_bullish_ob":  nearest_bull,
        "nearest_bearish_ob":  nearest_bear,
    }


# ─────────────────────────────────────────────────────────────
# 3. FAIR VALUE GAPS (FVG)
# ─────────────────────────────────────────────────────────────

def find_fvg(candles: list[dict], n_look: int = 40) -> dict:
    """
    Find Fair Value Gaps (FVG) and Inversion FVGs (iFVG).

    Bullish FVG: candle[i-1].high < candle[i+1].low
    Bearish FVG: candle[i-1].low  > candle[i+1].high

    Inversion FVG (iFVG):
    - When a Bullish FVG is CLOSED BELOW, it becomes a Bearish iFVG (resistance).
    - When a Bearish FVG is CLOSED ABOVE, it becomes a Bullish iFVG (support).

    Returns:
        {
          "bullish_fvgs": list, "bearish_fvgs": list,
          "bullish_ifvgs": list, "bearish_ifvgs": list,
          "nearest_bullish_fvg": dict|None, "nearest_bearish_fvg": dict|None,
        }
    """
    if not candles or len(candles) < 5:
        return {"bullish_fvgs": [], "bearish_fvgs": [], "bullish_ifvgs": [], "bearish_ifvgs": [],
                "nearest_bullish_fvg": None, "nearest_bearish_fvg": None}

    recent = candles[-min(n_look, len(candles)):]
    n    = len(recent)
    spot = recent[-1]["close"]

    bullish_fvgs = []
    bearish_fvgs = []
    bullish_ifvgs = []
    bearish_ifvgs = []
    min_gap_pct  = 0.0005   # slightly tighter filter

    for i in range(1, n - 1):
        prev = recent[i-1]
        curr = recent[i]
        nxt  = recent[i+1]

        # ── Bullish FVG ──
        if prev["high"] < nxt["low"]:
            bottom = prev["high"]
            top    = nxt["low"]
            gap_pct = (top - bottom) / bottom if bottom > 0 else 0
            if gap_pct >= min_gap_pct:
                # Check if later candles have CLOSED below this FVG (Inversion)
                later = recent[i+2:]
                inverted = False
                for c in later:
                    if c["close"] < bottom:
                        inverted = True
                        break
                
                # Check if it was touched (filled/retested) but not inverted
                filled = any(c["low"] <= bottom + (top - bottom) * 0.5 for c in later)

                fvg_data = {
                    "top":      top,
                    "bottom":   bottom,
                    "midpoint": round((top + bottom) / 2, 1),
                    "gap_pct":  round(gap_pct * 100, 2),
                    "filled":   filled,
                    "index":    i,
                    "fvg_type": "BULLISH",
                }
                if inverted:
                    fvg_data["fvg_type"] = "BEARISH_INVERSION"
                    bearish_ifvgs.append(fvg_data)
                else:
                    bullish_fvgs.append(fvg_data)

        # ── Bearish FVG ──
        if prev["low"] > nxt["high"]:
            top    = prev["low"]
            bottom = nxt["high"]
            gap_pct = (top - bottom) / bottom if bottom > 0 else 0
            if gap_pct >= min_gap_pct:
                later = recent[i+2:]
                inverted = False
                for c in later:
                    if c["close"] > top:
                        inverted = True
                        break
                
                filled = any(c["high"] >= top - (top - bottom) * 0.5 for c in later)

                fvg_data = {
                    "top":      top,
                    "bottom":   bottom,
                    "midpoint": round((top + bottom) / 2, 1),
                    "gap_pct":  round(gap_pct * 100, 2),
                    "filled":   filled,
                    "index":    i,
                    "fvg_type": "BEARISH",
                }
                if inverted:
                    fvg_data["fvg_type"] = "BULLISH_INVERSION"
                    bullish_ifvgs.append(fvg_data)
                else:
                    bearish_fvgs.append(fvg_data)

    # Nearest unfilled below/above spot
    unfilled_bull = [f for f in bullish_fvgs if not f["filled"] and f["top"] < spot]
    unfilled_bear = [f for f in bearish_fvgs if not f["filled"] and f["bottom"] > spot]
    
    # Also consider iFVGs as valid support/resistance targets
    active_bull_ifvgs = [f for f in bullish_ifvgs if f["top"] < spot]
    active_bear_ifvgs = [f for f in bearish_ifvgs if f["bottom"] > spot]

    nearest_bull = max(unfilled_bull + active_bull_ifvgs, key=lambda x: x["top"],    default=None)
    nearest_bear = min(unfilled_bear + active_bear_ifvgs, key=lambda x: x["bottom"], default=None)

    return {
        "bullish_fvgs":         bullish_fvgs,
        "bearish_fvgs":         bearish_fvgs,
        "bullish_ifvgs":        bullish_ifvgs,
        "bearish_ifvgs":        bearish_ifvgs,
        "nearest_bullish_fvg":  nearest_bull,
        "nearest_bearish_fvg":  nearest_bear,
    }


# ─────────────────────────────────────────────────────────────
# 4. LIQUIDITY POOLS (Equal Highs / Equal Lows)
# ─────────────────────────────────────────────────────────────

def find_liquidity_pools(candles: list[dict], tolerance_pct: float = 0.1) -> dict:
    """
    Detect equal highs (buy-stop liquidity) and equal lows (sell-stop liquidity).

    Equal highs = two or more swing highs within tolerance → buy-stops sitting above.
    Equal lows  = two or more swing lows within tolerance  → sell-stops sitting below.

    Returns:
        {
          "equal_highs": list of {level, count, swept},
          "equal_lows":  list of {level, count, swept},
          "buy_side_liquidity":  list[float],  # unswept equal highs
          "sell_side_liquidity": list[float],  # unswept equal lows
          "nearest_buy_liq":  float|None,      # nearest buy-stop above spot
          "nearest_sell_liq": float|None,      # nearest sell-stop below spot
        }
    """
    if not candles or len(candles) < 6:
        return {"equal_highs": [], "equal_lows": [], "buy_side_liquidity": [],
                "sell_side_liquidity": [], "nearest_buy_liq": None, "nearest_sell_liq": None}

    spot = candles[-1]["close"]
    tol  = spot * tolerance_pct / 100

    # ── Swing highs / lows ──
    highs = [c["high"] for c in candles]
    lows  = [c["low"]  for c in candles]
    n     = len(candles)

    pivot_highs = [highs[i] for i in range(2, n-2) if highs[i] == max(highs[i-2:i+3])]
    pivot_lows  = [lows[i]  for i in range(2, n-2) if lows[i]  == min(lows[i-2:i+3])]

    def cluster(vals, tolerance):
        """Group values within tolerance into clusters."""
        clusters = []
        for v in sorted(vals):
            placed = False
            for cl in clusters:
                if abs(v - cl["level"]) <= tolerance:
                    cl["values"].append(v)
                    cl["level"] = sum(cl["values"]) / len(cl["values"])  # running mean
                    placed = True
                    break
            if not placed:
                clusters.append({"level": v, "values": [v]})
        return [{"level": round(cl["level"], 1), "count": len(cl["values"])}
                for cl in clusters if len(cl["values"]) >= 2]

    eq_highs = cluster(pivot_highs, tol)
    eq_lows  = cluster(pivot_lows,  tol)

    # Mark if already swept (price has traded through)
    last_high = max(highs[-3:]) if len(highs) >= 3 else spot
    last_low  = min(lows[-3:])  if len(lows)  >= 3 else spot

    for eh in eq_highs:
        eh["swept"] = last_high > eh["level"]
        eh["above_spot"] = eh["level"] > spot
    for el in eq_lows:
        el["swept"] = last_low < el["level"]
        el["below_spot"] = el["level"] < spot

    buy_liq  = [eh["level"] for eh in eq_highs if not eh["swept"] and eh["level"] > spot]
    sell_liq = [el["level"] for el in eq_lows  if not el["swept"] and el["level"] < spot]

    return {
        "equal_highs":        eq_highs,
        "equal_lows":         eq_lows,
        "buy_side_liquidity": sorted(buy_liq),
        "sell_side_liquidity": sorted(sell_liq, reverse=True),
        "nearest_buy_liq":   min(buy_liq)  if buy_liq  else None,
        "nearest_sell_liq":  max(sell_liq) if sell_liq else None,
    }


# ─────────────────────────────────────────────────────────────
# 5. PREMIUM / DISCOUNT ZONES
# ─────────────────────────────────────────────────────────────

def get_premium_discount(candles: list[dict], lookback: int = 20) -> dict:
    """
    Identify premium (above equilibrium) and discount (below equilibrium) zones.

    Equilibrium = midpoint of the recent dealing range.
    Discount  → buy zone (Bullish PDA)
    Premium   → sell zone (Bearish PDA)

    Returns:
        {
          "range_high": float,
          "range_low":  float,
          "equilibrium": float,
          "zone": "DISCOUNT" | "PREMIUM" | "EQUILIBRIUM",
          "zone_pct": float,   # how far into premium/discount (0-100%)
          "bias": "BUY" | "SELL" | "NEUTRAL",
        }
    """
    if not candles or len(candles) < 5:
        return {"zone": "EQUILIBRIUM", "bias": "NEUTRAL",
                "range_high": 0, "range_low": 0, "equilibrium": 0, "zone_pct": 50}

    recent = candles[-min(lookback, len(candles)):]
    range_high = max(c["high"] for c in recent)
    range_low  = min(c["low"]  for c in recent)
    spot       = candles[-1]["close"]
    span       = range_high - range_low

    if span < 1:
        return {"zone": "EQUILIBRIUM", "bias": "NEUTRAL",
                "range_high": range_high, "range_low": range_low,
                "equilibrium": (range_high + range_low) / 2, "zone_pct": 50}

    equilibrium = (range_high + range_low) / 2
    zone_pct    = round((spot - range_low) / span * 100, 1)

    if zone_pct > 60:
        zone = "PREMIUM"
        bias = "SELL"
    elif zone_pct < 40:
        zone = "DISCOUNT"
        bias = "BUY"
    else:
        zone = "EQUILIBRIUM"
        bias = "NEUTRAL"

    return {
        "range_high":  range_high,
        "range_low":   range_low,
        "equilibrium": round(equilibrium, 1),
        "zone":        zone,
        "zone_pct":    zone_pct,
        "bias":        bias,
    }


# ─────────────────────────────────────────────────────────────
# 6. SMC SIGNAL — OI CONFLICT RESOLUTION
# ─────────────────────────────────────────────────────────────

def resolve_oi_smc_conflict(oi_direction: str, smc_trend: str,
                             bos_events: list, choch_events: list,
                             prem_disc: dict, fvg: dict, ob: dict) -> dict:
    """
    THE CORE FUNCTION:
    Resolves conflict between OI bearish signal and price structure bullish trend.
    """
    score = 0     # positive = bullish, negative = bearish
    reasons = []
    conflict_detected = False

    oi_bull = "BULLISH" in oi_direction.upper()
    oi_bear = "BEARISH" in oi_direction.upper()

    smc_bull = "BULLISH" in smc_trend
    smc_bear = "BEARISH" in smc_trend

    # ── OI base score ──
    if oi_bull:
        score += 1
        reasons.append("✅ OI: Bullish flow (put build / call unwind)")
    elif oi_bear:
        score -= 1
        reasons.append("🔴 OI: Bearish flow (call build / put unwind)")
        if smc_bull:
            conflict_detected = True
            reasons.append("⚠️ CONFLICT: OI bearish but price structure bullish — SMC overrides")

    # ── Price structure (BOS / CHoCH) — high weight ──
    if bos_events:
        last_bos = bos_events[-1]
        if "UP" in last_bos["type"]:
            score += 3
            reasons.append(f"🚀 BOS UP at {last_bos['price']:.0f} — structural bullish breakout")
        else:
            score -= 3
            reasons.append(f"💥 BOS DOWN at {last_bos['price']:.0f} — structural breakdown")

    if choch_events:
        last_choch = choch_events[-1]
        if "UP" in last_choch["type"]:
            score += 2
            reasons.append(f"🔄 CHoCH UP at {last_choch['price']:.0f} — trend reversal to bullish")
        else:
            score -= 2
            reasons.append(f"🔄 CHoCH DOWN at {last_choch['price']:.0f} — trend reversal to bearish")

    # ── SMC trend (HH/HL or LH/LL sequence) ──
    if smc_bull:
        score += 2
        reasons.append(f"📈 SMC Trend: {smc_trend} (HH/HL structure intact)")
    elif smc_bear:
        score -= 2
        reasons.append(f"📉 SMC Trend: {smc_trend} (LH/LL structure)")
    else:
        reasons.append(f"⚖️ SMC Trend: {smc_trend} (ranging)")

    # ── Order Block & Breaker Block confluence ──
    nb_ob = ob.get("nearest_bullish_ob")
    if nb_ob and not conflict_detected:
        score += 2
        reasons.append(f"🟦 Bullish OB at {nb_ob['low']:.0f}–{nb_ob['high']:.0f} acting as support")

    bear_ob = ob.get("nearest_bearish_ob")
    if bear_ob and not smc_bull:
        score -= 1
        reasons.append(f"🟥 Bearish OB at {bear_ob['low']:.0f}–{bear_ob['high']:.0f} is resistance")

    # Breakers (High Conviction Flip Zones)
    bull_bbs = ob.get("bullish_bbs", [])
    if bull_bbs:
        last_bb = bull_bbs[-1]
        score += 2
        reasons.append(f"🧱 Bullish Breaker Block at {last_bb['low']:.0f}–{last_bb['high']:.0f} (Flip Support)")

    bear_bbs = ob.get("bearish_bbs", [])
    if bear_bbs:
        last_bb = bear_bbs[-1]
        score -= 2
        reasons.append(f"🧱 Bearish Breaker Block at {last_bb['low']:.0f}–{last_bb['high']:.0f} (Flip Resistance)")

    # ── FVG & Inversion FVG confluence ──
    bull_fvg = fvg.get("nearest_bullish_fvg")
    if bull_fvg and "INVERSION" not in bull_fvg.get("fvg_type", ""):
        score += 1
        reasons.append(f"📊 Bullish FVG {bull_fvg['bottom']:.0f}–{bull_fvg['top']:.0f} acting as magnet support")
    
    bear_fvg = fvg.get("nearest_bearish_fvg")
    if bear_fvg and "INVERSION" not in bear_fvg.get("fvg_type", ""):
        score -= 1
        reasons.append(f"📊 Bearish FVG {bear_fvg['bottom']:.0f}–{bear_fvg['top']:.0f} acting as magnet resistance")

    # Inversions
    bull_ifvgs = fvg.get("bullish_ifvgs", [])
    if bull_ifvgs:
        last_ifvg = bull_ifvgs[-1]
        score += 1
        reasons.append(f"🔄 Bullish Inversion FVG {last_ifvg['bottom']:.0f}–{last_ifvg['top']:.0f} (Support)")

    bear_ifvgs = fvg.get("bearish_ifvgs", [])
    if bear_ifvgs:
        last_ifvg = bear_ifvgs[-1]
        score -= 1
        reasons.append(f"🔄 Bearish Inversion FVG {last_ifvg['bottom']:.0f}–{last_ifvg['top']:.0f} (Resistance)")

    # ── Premium / Discount zone ──
    zone = prem_disc.get("zone", "EQUILIBRIUM")
    if zone == "DISCOUNT":
        score += 1
        reasons.append(f"🛒 Price in DISCOUNT zone ({prem_disc['zone_pct']:.0f}%) → buy bias")
    elif zone == "PREMIUM":
        score -= 1
        reasons.append(f"🏷 Price in PREMIUM zone ({prem_disc['zone_pct']:.0f}%) → sell bias")

    # ── Final signal ──
    if score >= 6:
        signal = "🔥 STRONG BULLISH — Institutional accumulation confirmed"
        action = "BUY CALLS / BUY FUTURES"
        color  = "green"
    elif score >= 2:
        signal = "🟢 BULLISH — SMC + OI confluence"
        action = "BUY DIPS / HOLD LONGS"
        color  = "green"
    elif score >= 0:
        signal = "⚖️ NEUTRAL / RANGING"
        action = "WAIT FOR BREAKOUT"
        color  = "grey"
    elif score >= -5:
        signal = "🔴 BEARISH — OI + Structure aligned"
        action = "BUY PUTS / SHORT RALLIES"
        color  = "red"
    else:
        signal = "💥 STRONG BEARISH — Distribution confirmed"
        action = "AGGRESSIVE SHORT / HEDGE LONGS"
        color  = "red"

    return {
        "confluent_score":    score,
        "confluent_signal":   signal,
        "confluent_action":   action,
        "signal_color":       color,
        "conflict_detected":  conflict_detected,
        "oi_direction":       oi_direction,
        "smc_trend":          smc_trend,
        "reasons":            reasons,
        "short_reason":       reasons[0] if reasons else "",
    }

def candles_last_close_from_ob(ob):
    """Helper for OB distance (dummy, overridden in full integration)."""
    return ob.get("midpoint", 0)


# ─────────────────────────────────────────────────────────────
# 7. MASTER CONFLUENCE ENGINE
# ─────────────────────────────────────────────────────────────

def get_smc_confluence(
    oi_intel: dict,
    candles_15m: list[dict],
    candles_1h:  Optional[list[dict]] = None,
    candles_daily: Optional[list[dict]] = None,
) -> dict:
    """
    MASTER function: Combines OI intelligence + SMC analysis across timeframes.

    Parameters:
        oi_intel     : Output dict from fetch_nifty_oi_intelligence()
        candles_15m  : List of 15-min OHLC dicts (at least 30 candles)
        candles_1h   : List of 1-hour OHLC dicts (optional but recommended)
        candles_daily: List of daily OHLC dicts (optional but recommended)

    Each candle dict: {open, high, low, close, datetime}

    Returns comprehensive SMC + OI dict with:
        - final_signal, final_action, score
        - BOS/CHoCH events
        - Order blocks (support/resistance levels)
        - FVGs (magnet targets)
        - Liquidity pools (stop hunt levels)
        - P/D zone
        - OI + SMC conflict resolution
        - Key levels for trade setup
        - Telegram-ready summary string
    """
    if not oi_intel:
        oi_intel = {}

    spot        = oi_intel.get("spot", 0)
    oi_dir      = oi_intel.get("direction", "⚠️ SIDEWAYS/NEUTRAL")
    call_wall   = oi_intel.get("nearest_call_wall", 0)
    put_floor   = oi_intel.get("nearest_put_floor", 0)
    pcr         = oi_intel.get("pcr", 1.0)
    max_pain    = oi_intel.get("max_pain", 0)
    atm         = oi_intel.get("atm", spot)

    # ── Use 15m as primary; 1h for HTF bias ──
    primary    = candles_15m or []
    htf        = candles_1h  or candles_daily or primary

    # ── Run SMC modules ──
    ms_ltf   = detect_market_structure(primary)
    ms_htf   = detect_market_structure(htf) if htf is not primary else ms_ltf
    ob_ltf   = find_order_blocks(primary)
    fvg_ltf  = find_fvg(primary)
    liq_ltf  = find_liquidity_pools(primary)
    pd_ltf   = get_premium_discount(primary)
    pd_htf   = get_premium_discount(htf, lookback=40) if htf is not primary else pd_ltf

    # ── Conflict resolution ──
    resolution = resolve_oi_smc_conflict(
        oi_direction = oi_dir,
        smc_trend    = ms_ltf["trend"],
        bos_events   = ms_ltf["bos"],
        choch_events = ms_ltf["choch"],
        prem_disc    = pd_ltf,
        fvg          = fvg_ltf,
        ob           = ob_ltf,
    )

    score = resolution["confluent_score"]

    # ── HTF alignment bonus / penalty ──
    htf_trend = ms_htf["trend"]
    if "BULLISH" in htf_trend and "BULLISH" in ms_ltf["trend"]:
        score += 1
        resolution["reasons"].append(f"✅ HTF aligned BULLISH — higher probability long")
    elif "BEARISH" in htf_trend and "BEARISH" in ms_ltf["trend"]:
        score -= 1
        resolution["reasons"].append(f"✅ HTF aligned BEARISH — higher probability short")
    elif "BULLISH" in htf_trend and "BEARISH" in ms_ltf["trend"]:
        resolution["reasons"].append(f"⚠️ HTF bullish but LTF bearish — likely retracement, not reversal")
    elif "BEARISH" in htf_trend and "BULLISH" in ms_ltf["trend"]:
        resolution["reasons"].append(f"⚠️ HTF bearish but LTF bullish — possible counter-trend bounce")

    # ── OI-Smart Money interpretation (the KEY insight) ──
    oi_smc_interpretation = _interpret_oi_with_smc(oi_intel, ms_ltf, pd_ltf)
    resolution["reasons"].extend(oi_smc_interpretation["notes"])
    score += oi_smc_interpretation["score_adj"]

    # ── Key level summary for telegram ──
    key_levels = _build_key_levels(spot, call_wall, put_floor, ob_ltf, fvg_ltf, liq_ltf, ms_ltf)

    # ── Build trade setup ──
    setup = _build_trade_setup(score, spot, atm, key_levels, pd_ltf, ob_ltf, fvg_ltf)

    # ── Telegram summary ──
    tg_summary = _build_telegram_summary(
        spot=spot, atm=atm, score=score,
        resolution=resolution,
        ms_ltf=ms_ltf, ms_htf=ms_htf,
        ob_ltf=ob_ltf, fvg_ltf=fvg_ltf,
        liq_ltf=liq_ltf, pd_ltf=pd_ltf,
        oi_intel=oi_intel, key_levels=key_levels,
        setup=setup,
        oi_smc_interp=oi_smc_interpretation,
    )

    return {
        # ── Core signals ──
        "final_score":       score,
        "final_signal":      resolution["confluent_signal"],
        "final_action":      resolution["confluent_action"],
        "signal_color":      resolution["signal_color"],
        "conflict_detected": resolution["conflict_detected"],

        # ── OI ──
        "oi_direction":   oi_dir,
        "oi_pcr":         pcr,
        "oi_call_wall":   call_wall,
        "oi_put_floor":   put_floor,
        "oi_max_pain":    max_pain,

        # ── SMC ──
        "smc_trend_ltf":  ms_ltf["trend"],
        "smc_trend_htf":  htf_trend,
        "bos_events":     ms_ltf["bos"],
        "choch_events":   ms_ltf["choch"],
        "structure_summary": ms_ltf["structure_summary"],

        # ── Order Blocks & Breakers ──
        "nearest_bullish_ob": ob_ltf["nearest_bullish_ob"],
        "nearest_bearish_ob": ob_ltf["nearest_bearish_ob"],
        "all_bullish_obs":    ob_ltf["bullish_obs"][-3:],
        "all_bearish_obs":    ob_ltf["bearish_obs"][-3:],
        "bullish_bbs":        ob_ltf.get("bullish_bbs", []),
        "bearish_bbs":        ob_ltf.get("bearish_bbs", []),

        # ── FVGs & Inversions ──
        "nearest_bullish_fvg": fvg_ltf["nearest_bullish_fvg"],
        "nearest_bearish_fvg": fvg_ltf["nearest_bearish_fvg"],
        "bullish_ifvgs":       fvg_ltf.get("bullish_ifvgs", []),
        "bearish_ifvgs":       fvg_ltf.get("bearish_ifvgs", []),

        # ── Liquidity ──
        "nearest_buy_liq":   liq_ltf["nearest_buy_liq"],
        "nearest_sell_liq":  liq_ltf["nearest_sell_liq"],
        "buy_side_liq":      liq_ltf["buy_side_liquidity"][:3],
        "sell_side_liq":     liq_ltf["sell_side_liquidity"][:3],

        # ── Premium/Discount ──
        "pd_zone":     pd_ltf["zone"],
        "pd_zone_pct": pd_ltf["zone_pct"],
        "pd_bias":     pd_ltf["bias"],
        "equilibrium": pd_ltf["equilibrium"],
        "pd_htf_zone": pd_htf["zone"],

        # ── Key levels ──
        "key_levels":    key_levels,
        "setup":         setup,
        "reasons":       resolution["reasons"],

        # ── OI + SMC reconciled interpretation ──
        "oi_smc_interpretation": oi_smc_interpretation["summary"],
        "oi_smc_score_adj":      oi_smc_interpretation["score_adj"],

        # ── Telegram ready ──
        "telegram_summary": tg_summary,

        # ── Metadata ──
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "spot":      spot,
        "atm":       atm,
    }


# ─────────────────────────────────────────────────────────────
# 8. OI + SMC Interpretation (the KEY insight engine)
# ─────────────────────────────────────────────────────────────

def _interpret_oi_with_smc(oi_intel: dict, ms: dict, pd: dict) -> dict:
    """
    THIS IS THE CORE FIX for the false-bearish-OI problem.

    Interprets WHY OI shows bearish (call writing) while price is rising.

    Smart Money plays:
    1. GAMMA SQUEEZE / CALL WALL MELTING:
       - Institutions sold calls at strike X as resistance
       - Price pushes through X → they MUST hedge by buying futures
       - This forces MORE buying → price goes higher
       - OI sees "call building" but it's actually covering, not new shorts

    2. SHORT GAMMA UNWIND:
       - Market makers who sold calls at the call wall are underwater
       - As spot approaches call wall, delta of calls increases
       - MMs hedge by buying underlying → price rockets
       - Traditional OI signals this as "bearish" but it's a bullish gamma squeeze

    3. STOP HUNT ACCUMULATION:
       - Smart money drives price below swing lows to collect sell stops
       - OI shows "put floor crumbling" (bearish)
       - But this is actually smart money BUYING the washed-out longs
       - Price then reverses sharply higher

    4. DISTRIBUTION FAKE-OUT:
       - Call writing above spot while price is consolidating
       - This looks bearish BUT if HH/HL structure is intact,
         the calls are being written as a HEDGE against longs, not new shorts
    """
    notes = []
    score_adj = 0

    direction = oi_intel.get("direction", "")
    near_ce_pct = oi_intel.get("near_ce_pct", 0)
    near_pe_drop = oi_intel.get("near_pe_drop_pct", 0)
    call_wall = oi_intel.get("nearest_call_wall", 0)
    put_floor = oi_intel.get("nearest_put_floor", 0)
    spot = oi_intel.get("spot", 0)
    pcr  = oi_intel.get("pcr", 1.0)

    smc_trend = ms.get("trend", "RANGING")
    bos = ms.get("bos", [])
    pd_zone = pd.get("zone", "EQUILIBRIUM")

    summary_parts = []

    # ── Scenario 1: GAMMA SQUEEZE — call wall is being consumed ──
    dist_to_call = (call_wall - spot) if call_wall and spot else 9999
    if (dist_to_call < 150 and "BULLISH" in smc_trend and
            "BEARISH" in direction and near_ce_pct > 5):
        notes.append(
            f"🧠 SMC INSIGHT: OI shows call-writing at {call_wall} but BOS UP intact — "
            f"likely GAMMA HEDGE (MMs buying futures to delta-hedge short calls). "
            f"Call wall at {call_wall} is being consumed, not strengthened. "
            f"This is BULLISH, not bearish."
        )
        score_adj += 3
        summary_parts.append(f"GAMMA SQUEEZE developing at {call_wall}")

    # ── Scenario 2: CALL WALL MELTING — consecutive call unwinds ──
    if "BULLISH" in direction and "BIAS" not in direction and "BEARISH" in smc_trend:
        notes.append(
            f"🧠 SMC INSIGHT: Call walls unwinding (OI bullish) but LTF price structure "
            f"bearish. Wait for CHoCH confirmation before going long. "
            f"This could be short-covering rally, not new accumulation."
        )
        score_adj -= 1
        summary_parts.append("Call unwind without structure confirmation")

    # ── Scenario 3: STOP HUNT ACCUMULATION (discount zone + OI bearish) ──
    if pd_zone == "DISCOUNT" and "BEARISH" in direction and bos:
        last_bos = bos[-1]
        if "UP" in last_bos.get("type", ""):
            notes.append(
                f"🧠 SMC INSIGHT: Price in DISCOUNT zone with OI bearish = "
                f"classic STOP HUNT / accumulation pattern. "
                f"Smart money sold below old lows to collect stop-run orders, "
                f"now price has BOS UP at {last_bos['price']:.0f}. "
                f"OI bearish signal is LAGGING. Actual bias: BULLISH."
            )
            score_adj += 3
            summary_parts.append(f"Stop hunt accumulation — BOS UP {last_bos['price']:.0f}")

    # ── Scenario 4: PCR trap — low PCR with rising market ──
    if pcr < 0.8 and "BULLISH" in smc_trend:
        notes.append(
            f"🧠 SMC INSIGHT: PCR={pcr} (low, bearish by standard reading) "
            f"but HH/HL structure is intact. "
            f"Low PCR in a rising market = call writers being squeezed. "
            f"Standard OI interpretation FAILS here. Trust the structure."
        )
        score_adj += 2
        summary_parts.append(f"PCR={pcr} trap — structure overrides")

    # ── Scenario 5: Put floor building with rising market = genuine support ──
    near_pe_pct = oi_intel.get("near_pe_pct", 0)
    if near_pe_pct > 8 and "BULLISH" in smc_trend:
        notes.append(
            f"🧠 SMC INSIGHT: Put floor BUILDING (+{near_pe_pct}% OI) at support levels "
            f"while structure is bullish = institutional DEFENSE of key level. "
            f"This is very BULLISH — smart money is protecting longs."
        )
        score_adj += 2
        summary_parts.append(f"Put defense confirms bullish structure")

    # ── Scenario 6: OI + SMC fully aligned ──
    if not summary_parts:
        if "BULLISH" in direction and "BULLISH" in smc_trend:
            summary_parts.append("OI and SMC aligned BULLISH — high conviction long")
            score_adj += 1
        elif "BEARISH" in direction and "BEARISH" in smc_trend:
            summary_parts.append("OI and SMC aligned BEARISH — high conviction short")
            score_adj -= 1
        else:
            summary_parts.append("OI and SMC mixed — wait for confluence")

    return {
        "summary": " | ".join(summary_parts),
        "notes":   notes,
        "score_adj": score_adj,
    }


# ─────────────────────────────────────────────────────────────
# 9. KEY LEVEL BUILDER
# ─────────────────────────────────────────────────────────────

def _build_key_levels(spot, call_wall, put_floor, ob, fvg, liq, ms) -> dict:
    """Compile all key levels into a clean dict for display."""
    levels = {
        "resistance": [],
        "support":    [],
    }

    # ── Resistance ──
    if call_wall:
        levels["resistance"].append({"level": call_wall, "type": "OI Call Wall", "strength": "STRONG"})
    
    if ob.get("nearest_bearish_ob"):
        bOb = ob["nearest_bearish_ob"]
        levels["resistance"].append({"level": bOb["high"], "type": "Bearish OB", "strength": "MEDIUM"})
    
    for bb in ob.get("bearish_bbs", [])[-2:]:
        levels["resistance"].append({"level": bb["high"], "type": "Bearish Breaker", "strength": "STRONG"})

    if fvg.get("nearest_bearish_fvg"):
        bFvg = fvg["nearest_bearish_fvg"]
        levels["resistance"].append({"level": bFvg["bottom"], "type": "Bearish FVG", "strength": "MEDIUM"})
    
    for ifvg in fvg.get("bearish_ifvgs", [])[-2:]:
        levels["resistance"].append({"level": ifvg["bottom"], "type": "Bearish iFVG", "strength": "KEY"})

    if liq.get("nearest_buy_liq"):
        levels["resistance"].append({"level": liq["nearest_buy_liq"], "type": "Buy Stops", "strength": "TARGET"})

    # ── Support ──
    if put_floor:
        levels["support"].append({"level": put_floor, "type": "OI Put Floor", "strength": "STRONG"})
    
    if ob.get("nearest_bullish_ob"):
        bOb = ob["nearest_bullish_ob"]
        levels["support"].append({"level": bOb["low"], "type": "Bullish OB", "strength": "MEDIUM"})
    
    for bb in ob.get("bullish_bbs", [])[-2:]:
        levels["support"].append({"level": bb["low"], "type": "Bullish Breaker", "strength": "STRONG"})

    if fvg.get("nearest_bullish_fvg"):
        bFvg = fvg["nearest_bullish_fvg"]
        levels["support"].append({"level": bFvg["bottom"], "type": "Bullish FVG", "strength": "MEDIUM"})
    
    for ifvg in fvg.get("bullish_ifvgs", [])[-2:]:
        levels["support"].append({"level": ifvg["top"], "type": "Bullish iFVG", "strength": "KEY"})

    if liq.get("nearest_sell_liq"):
        levels["support"].append({"level": liq["nearest_sell_liq"], "type": "Sell Stops", "strength": "TARGET"})

    # Sort
    levels["resistance"] = sorted(levels["resistance"], key=lambda x: x["level"])
    levels["support"]    = sorted(levels["support"],    key=lambda x: x["level"], reverse=True)

    return levels


# ─────────────────────────────────────────────────────────────
# 10. TRADE SETUP BUILDER
# ─────────────────────────────────────────────────────────────

def _build_trade_setup(score, spot, atm, key_levels, pd, ob, fvg) -> dict:
    """Generate actionable trade setup from SMC + OI confluence."""
    setup = {"bias": "NEUTRAL", "entry": None, "sl": None, "target1": None, "target2": None,
             "setup_type": None, "rr": None, "notes": []}

    supports    = key_levels.get("support",    [])
    resistances = key_levels.get("resistance", [])

    nearest_support    = supports[0]["level"]    if supports    else spot - 100
    nearest_resistance = resistances[0]["level"] if resistances else spot + 100

    if score >= 3:
        # Long setup
        setup["bias"]       = "LONG"
        setup["setup_type"] = "OTE (Optimal Trade Entry) at Discount Array"
        # Entry near nearest bullish OB or FVG
        bull_ob  = ob.get("nearest_bullish_ob")
        bull_fvg = fvg.get("nearest_bullish_fvg")
        if bull_ob and abs(bull_ob["high"] - spot) < 200:
            setup["entry"]  = round(bull_ob["high"], 0)
            setup["sl"]     = round(bull_ob["low"] - 30, 0)
            setup["notes"].append(f"Entry at Bullish OB high {bull_ob['high']:.0f}")
        elif bull_fvg:
            setup["entry"]  = round(bull_fvg["top"], 0)
            setup["sl"]     = round(bull_fvg["bottom"] - 30, 0)
            setup["notes"].append(f"Entry at Bullish FVG top {bull_fvg['top']:.0f}")
        else:
            setup["entry"]  = round(spot, 0)
            setup["sl"]     = round(nearest_support - 30, 0)

        setup["target1"] = round(nearest_resistance, 0)
        setup["target2"] = round(nearest_resistance + (nearest_resistance - nearest_support) * 0.5, 0)
        risk = setup["entry"] - setup["sl"]
        rwd  = setup["target1"] - setup["entry"]
        setup["rr"] = round(rwd / risk, 1) if risk > 0 else 0
        setup["notes"].append(f"SL: {setup['sl']:.0f} | T1: {setup['target1']:.0f} | RR: {setup['rr']}:1")

        # Option setup
        setup["option_setup"] = f"Buy {atm}CE | SL below {setup['sl']:.0f}"

    elif score <= -3:
        # Short setup
        setup["bias"]       = "SHORT"
        setup["setup_type"] = "Distribution at Premium Array"
        bear_ob = ob.get("nearest_bearish_ob")
        if bear_ob and abs(bear_ob["low"] - spot) < 200:
            setup["entry"]  = round(bear_ob["low"], 0)
            setup["sl"]     = round(bear_ob["high"] + 30, 0)
        else:
            setup["entry"]  = round(spot, 0)
            setup["sl"]     = round(nearest_resistance + 30, 0)
        setup["target1"] = round(nearest_support, 0)
        setup["target2"] = round(nearest_support - (nearest_resistance - nearest_support) * 0.5, 0)
        risk = setup["sl"] - setup["entry"]
        rwd  = setup["entry"] - setup["target1"]
        setup["rr"] = round(rwd / risk, 1) if risk > 0 else 0
        setup["notes"].append(f"SL: {setup['sl']:.0f} | T1: {setup['target1']:.0f} | RR: {setup['rr']}:1")
        setup["option_setup"] = f"Buy {atm}PE | SL above {setup['sl']:.0f}"
    else:
        setup["bias"]       = "NEUTRAL"
        setup["setup_type"] = "Wait for BOS or CHoCH confirmation"
        setup["notes"].append("No high-probability setup. Stand aside.")
        setup["option_setup"] = "WAIT — no trade"

    return setup


# ─────────────────────────────────────────────────────────────
# 11. TELEGRAM SUMMARY BUILDER
# ─────────────────────────────────────────────────────────────

def _build_telegram_summary(
    spot, atm, score, resolution, ms_ltf, ms_htf,
    ob_ltf, fvg_ltf, liq_ltf, pd_ltf, oi_intel,
    key_levels, setup, oi_smc_interp
) -> str:
    """Build a clean Telegram-ready summary string."""

    signal_emoji = {
        "green":  "🟢",
        "red":    "🔴",
        "yellow": "🟡",
        "grey":   "⚪",
    }.get(resolution["signal_color"], "⚪")

    lines = [
        f"🧠 SMC + OI CONFLUENCE SIGNAL",
        f"━━━━━━━━━━━━━━━━━━━━━",
        f"📍 Spot: {spot:,.0f} | ATM: {atm}",
        f"{signal_emoji} {resolution['confluent_signal']}",
        f"Score: {score:+d} | Action: {resolution['confluent_action']}",
        f"━━━━━━━━━━━━━━━━━━━━━",
        f"📊 OI Direction: {oi_intel.get('direction','?')}",
        f"📈 SMC (15m): {ms_ltf['trend']} | HTF: {ms_htf['trend']}",
        f"🎯 P/D Zone: {pd_ltf['zone']} ({pd_ltf['zone_pct']:.0f}% of range)",
    ]

    # Structure events
    if ms_ltf["bos"]:
        b = ms_ltf["bos"][-1]
        lines.append(f"🚀 BOS {'↑' if 'UP' in b['type'] else '↓'} at {b['price']:.0f}")
    if ms_ltf["choch"]:
        c = ms_ltf["choch"][-1]
        lines.append(f"🔄 CHoCH {'↑' if 'UP' in c['type'] else '↓'} at {c['price']:.0f}")

    # Key levels
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━")
    if ob_ltf.get("nearest_bullish_ob"):
        ob = ob_ltf["nearest_bullish_ob"]
        lines.append(f"🟦 Bullish OB Support: {ob['low']:.0f}–{ob['high']:.0f}")
    if ob_ltf.get("bullish_bbs"):
        bb = ob_ltf["bullish_bbs"][-1]
        lines.append(f"🧱 Bullish Breaker (Support): {bb['low']:.0f}–{bb['high']:.0f}")
    if ob_ltf.get("nearest_bearish_ob"):
        ob = ob_ltf["nearest_bearish_ob"]
        lines.append(f"🟥 Bearish OB Resistance: {ob['low']:.0f}–{ob['high']:.0f}")
    if ob_ltf.get("bearish_bbs"):
        bb = ob_ltf["bearish_bbs"][-1]
        lines.append(f"🧱 Bearish Breaker (Resistance): {bb['low']:.0f}–{bb['high']:.0f}")

    if fvg_ltf.get("nearest_bullish_fvg"):
        fvg = fvg_ltf["nearest_bullish_fvg"]
        lbl = "🔄 Bullish iFVG" if "INVERSION" in fvg.get("fvg_type","") else "📊 Bullish FVG"
        lines.append(f"{lbl}: {fvg['bottom']:.0f}–{fvg['top']:.0f} ({fvg['gap_pct']}%)")
    if fvg_ltf.get("nearest_bearish_fvg"):
        fvg = fvg_ltf["nearest_bearish_fvg"]
        lbl = "🔄 Bearish iFVG" if "INVERSION" in fvg.get("fvg_type","") else "📊 Bearish FVG"
        lines.append(f"{lbl}: {fvg['bottom']:.0f}–{fvg['top']:.0f} ({fvg['gap_pct']}%)")

    if liq_ltf.get("nearest_buy_liq"):
        lines.append(f"💧 Buy Stops (Liquidity): {liq_ltf['nearest_buy_liq']:.0f}")
    if liq_ltf.get("nearest_sell_liq"):
        lines.append(f"💧 Sell Stops (Liquidity): {liq_ltf['nearest_sell_liq']:.0f}")

    # SMC insight
    if oi_smc_interp.get("summary"):
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"🧠 Insight: {oi_smc_interp['summary']}")

    # Conflict flag
    if resolution.get("conflict_detected"):
        lines.append(f"⚠️ OI-SMC CONFLICT DETECTED — Price structure OVERRIDES OI signal")

    # Trade setup
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🎯 Setup: {setup.get('setup_type','')}")
    if setup.get("option_setup"):
        lines.append(f"⚡ {setup['option_setup']}")
    if setup.get("notes"):
        for n in setup["notes"]:
            lines.append(f"   • {n}")

    lines.append(f"⚠️ NOT financial advice.")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# 12. KITE OHLC HELPER
# ─────────────────────────────────────────────────────────────

def fetch_nifty_candles_kite(kite_instance, interval: str = "15minute", days: int = 5) -> list[dict]:
    """
    Helper to fetch NIFTY OHLC from Kite for SMC analysis.

    interval: "5minute" | "15minute" | "60minute" | "day"
    days:     how many calendar days of history to fetch

    Returns list of dicts: {open, high, low, close, volume, datetime}
    """
    try:
        import pandas as pd
        from datetime import date

        instrument_token = 256265   # NSE:NIFTY 50
        to_date   = date.today()
        from_date = to_date - timedelta(days=days)

        raw = kite_instance.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            continuous=False,
            oi=False,
        )
        candles = []
        for r in raw:
            candles.append({
                "open":     float(r["open"]),
                "high":     float(r["high"]),
                "low":      float(r["low"]),
                "close":    float(r["close"]),
                "volume":   int(r.get("volume", 0)),
                "datetime": str(r.get("date", "")),
            })
        return candles
    except Exception as e:
        return []


# ─────────────────────────────────────────────────────────────
# 13. STANDALONE TEST / DEMO (no Kite required)
# ─────────────────────────────────────────────────────────────

def _demo_run():
    """
    Demo run using synthetic data that mimics the situation described:
    OI shows BEARISH, but price is making Higher Highs (market moving up).
    """
    import random

    # Simulate a 3-day rally: NIFTY from ~22900 to ~23950 (matches the chart)
    base = 22900.0
    candles = []
    for i in range(60):
        open_  = base + i * 17.5 + random.uniform(-20, 20)
        close_ = open_ + random.uniform(-15, 35)   # slight upward drift
        high_  = max(open_, close_) + random.uniform(5, 25)
        low_   = min(open_, close_) - random.uniform(5, 20)
        candles.append({
            "open":  round(open_,  1),
            "high":  round(high_,  1),
            "low":   round(low_,   1),
            "close": round(close_, 1),
        })

    # Simulate OI data showing "BEARISH" (call writing) while price is rising
    oi_intel_mock = {
        "spot":               23950.0,
        "atm":                24000,
        "direction":          "🔴 BEARISH",
        "direction_reason":   "WATERFALL: CE writing (+8.2%) + PE unwinding (-5.1%)",
        "pcr":                0.72,
        "max_pain":           23700,
        "nearest_call_wall":  24000,
        "nearest_put_floor":  23700,
        "near_ce_pct":        8.2,
        "near_pe_pct":        -2.1,
        "near_pe_drop_pct":   -5.1,
        "shifting_ce":        24000,
        "shifting_ce_pct":    58.4,
        "shifting_pe":        23900,
        "shifting_pe_pct":    99.8,
    }

    result = get_smc_confluence(
        oi_intel   = oi_intel_mock,
        candles_15m = candles,
        candles_1h  = candles[-20:],   # use last 20 as "hourly"
    )

    print("=" * 60)
    print("SMC + OI CONFLUENCE ENGINE — DEMO OUTPUT")
    print("=" * 60)
    print(result["telegram_summary"])
    print()
    print(f"Final Score:  {result['final_score']:+d}")
    print(f"Final Signal: {result['final_signal']}")
    print(f"OI Direction: {result['oi_direction']}")
    print(f"SMC Trend:    {result['smc_trend_ltf']}")
    print(f"Conflict:     {result['conflict_detected']}")
    print(f"P/D Zone:     {result['pd_zone']} ({result['pd_zone_pct']}%)")
    if result.get("nearest_bullish_ob"):
        ob = result["nearest_bullish_ob"]
        print(f"Bullish OB:   {ob['low']:.0f} – {ob['high']:.0f}")
    if result.get("nearest_bullish_fvg"):
        fvg = result["nearest_bullish_fvg"]
        print(f"Bullish FVG:  {fvg['bottom']:.0f} – {fvg['top']:.0f}")
    print()
    print("REASONS:")
    for r in result["reasons"]:
        print(f"  {r}")


if __name__ == "__main__":
    _demo_run()
