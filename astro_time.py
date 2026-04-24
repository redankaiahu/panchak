# =============================================================
# astro_time.py  — Time-of-Day Trading Signal
# =============================================================
# Provides time-based trading bias based on NSE market structure,
# F&O expiry patterns, and intraday volatility research.
# =============================================================

from datetime import datetime, time as dtime
import pytz

IST = pytz.timezone("Asia/Kolkata")


_TIME_ZONES = [
    # (start_hhmm, end_hhmm, signal, icon, description)
    # NSE pre-open session: 09:00-09:08 (order entry), 09:08-09:15 (matching)
    # NSE regular session: 09:15-15:30
    ("00:00", "09:00", "⛔ PRE-MARKET",      "⛔", "Market closed — no trades"),
    ("09:00", "09:15", "🚧 OPENING RISK",    "🚧", "Pre-open auction — gap risk, avoid entries"),
    ("09:15", "09:30", "⚡ OPENING RANGE",   "⚡", "First 15-min ORB candle forming — observe direction only"),
    ("09:30", "10:30", "🔥 STRONG TREND",    "🔥", "Best trending hour — enter after ORB candle closes"),
    ("10:30", "11:30", "🟢 MOMENTUM",        "🟢", "Momentum continuation — trail existing trades"),
    ("11:30", "12:00", "🟡 SLOW ZONE",       "🟡", "Lunch drift — reduce size, tighten stops"),
    ("12:00", "13:00", "🟡 CONSOLIDATION",   "🟡", "Sideways — wait for breakout from range"),
    ("13:00", "13:45", "🚀 AFTERNOON PUSH",  "🚀", "News-driven moves — institutional accumulation"),
    ("13:45", "14:30", "🚀 BREAKOUT ZONE",   "🚀", "F&O expiry pressure builds — best for breakout trades"),
    ("14:30", "15:00", "⚡ CLOSING RUSH",    "⚡", "Hedging + squaring off — sharp directional moves"),
    ("15:00", "15:30", "⛔ AVOID ENTRY",     "⛔", "Last 30 min — do not open new positions"),
    ("15:30", "23:59", "⛔ MARKET CLOSED",   "⛔", "Post-market — no trades"),
]


def get_time_signal(dt=None) -> str:
    """
    Return the time-of-day trading signal string.
    Backward-compatible with original interface.
    """
    if dt is None:
        dt = datetime.now(IST)
    t = dt.time()

    for start_s, end_s, signal, icon, desc in _TIME_ZONES:
        start = dtime(*map(int, start_s.split(":")))
        end   = dtime(*map(int, end_s.split(":")))
        if start <= t < end:
            return signal

    return "⛔ MARKET CLOSED"


def get_time_signal_detail(dt=None) -> dict:
    """
    Return full time signal with icon, description, and session name.
    """
    if dt is None:
        dt = datetime.now(IST)
    t = dt.time()

    for start_s, end_s, signal, icon, desc in _TIME_ZONES:
        start = dtime(*map(int, start_s.split(":")))
        end   = dtime(*map(int, end_s.split(":")))
        if start <= t < end:
            # Minutes remaining in this zone
            end_dt   = datetime.combine(dt.date(), end).replace(tzinfo=IST)
            mins_rem = int((end_dt - dt).total_seconds() / 60)
            return {
                "signal":      signal,
                "icon":        icon,
                "description": desc,
                "zone_start":  start_s,
                "zone_end":    end_s,
                "mins_left":   mins_rem,
                "time_str":    dt.strftime("%H:%M IST"),
            }

    return {
        "signal": "⛔ MARKET CLOSED", "icon": "⛔",
        "description": "NSE market closed",
        "zone_start": "15:30", "zone_end": "09:08",
        "mins_left": 0, "time_str": dt.strftime("%H:%M IST"),
    }


def is_good_entry_time(dt=None) -> bool:
    """Return True during prime entry windows (avoid opening + closing chaos)."""
    sig = get_time_signal(dt)
    return any(x in sig for x in ["STRONG TREND", "MOMENTUM", "BREAKOUT", "AFTERNOON", "OPENING RANGE"])
