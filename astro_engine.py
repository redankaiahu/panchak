# -*- coding: utf-8 -*-
# =============================================================
# astro_engine.py - Swiss Ephemeris Planet Engine
# =============================================================
# Uses pyswisseph for high-precision sidereal positions.
# Applies Lahiri ayanamsha (standard for KP / Vedic astrology).
# Fixes: UTC conversion, sidereal zodiac, all 9 planets.
# =============================================================

import swisseph as swe
from datetime import datetime, timezone, timedelta
import pytz

IST = pytz.timezone("Asia/Kolkata")

# Swiss Ephemeris setup
swe.set_ephe_path(".")
swe.set_sid_mode(swe.SIDM_LAHIRI)   # <- Lahiri ayanamsha (standard Vedic/KP)

SIGNS = [
    "Aries","Taurus","Gemini","Cancer","Leo","Virgo",
    "Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"
]

NAKSHATRAS = [
    "Ashwini","Bharani","Krittika","Rohini","Mrigashira","Ardra",
    "Punarvasu","Pushya","Ashlesha","Magha","Purva Phalguni","Uttara Phalguni",
    "Hasta","Chitra","Swati","Vishakha","Anuradha","Jyeshtha",
    "Mula","Purva Ashadha","Uttara Ashadha","Shravana","Dhanishtha",
    "Shatabhisha","Purva Bhadrapada","Uttara Bhadrapada","Revati"
]

NAK_LORDS = [
    "Ke","Ve","Su","Mo","Ma","Ra","Ju","Sa","Me",  # Ashwini->Ashlesha
    "Ke","Ve","Su","Mo","Ma","Ra","Ju","Sa","Me",  # Magha->Jyeshtha
    "Ke","Ve","Su","Mo","Ma","Ra","Ju","Sa","Me"   # Mula->Revati
]

# Planet IDs in pyswisseph
_PLANET_IDS = {
    "Sun":     swe.SUN,
    "Moon":    swe.MOON,
    "Mars":    swe.MARS,
    "Mercury": swe.MERCURY,
    "Jupiter": swe.JUPITER,
    "Venus":   swe.VENUS,
    "Saturn":  swe.SATURN,
    "Rahu":    swe.MEAN_NODE,  # North Node (Rahu)
}


def _to_jd(dt) -> float:
    """Convert datetime (any timezone) to Julian Day Number (UTC-based)."""
    # Convert to UTC
    if dt.tzinfo is None:
        # Assume IST if naive
        dt = IST.localize(dt)
    dt_utc = dt.astimezone(timezone.utc)
    frac_hour = dt_utc.hour + dt_utc.minute / 60.0 + dt_utc.second / 3600.0
    return swe.julday(dt_utc.year, dt_utc.month, dt_utc.day, frac_hour)


def _sidereal_lon(jd: float, planet_id: int) -> float:
    """Return sidereal longitude (Lahiri) for a planet at given JD."""
    result = swe.calc_ut(jd, planet_id, swe.FLG_SIDEREAL)[0]
    return result[0] % 360


def _parse_position(lon: float) -> dict:
    """Parse a sidereal longitude into sign, nakshatra, degree within sign, etc."""
    sign_idx = int(lon // 30)
    nak_idx  = int(lon * 27 / 360)
    nak_idx  = min(nak_idx, 26)   # safety clamp
    deg_in_sign = lon % 30

    return {
        "degree":      round(lon, 4),
        "sign":        SIGNS[sign_idx],
        "sign_degree": round(deg_in_sign, 2),
        "nakshatra":   NAKSHATRAS[nak_idx],
        "nak_lord":    NAK_LORDS[nak_idx],
    }


def get_all_planets(dt=None) -> dict:
    """
    Return sidereal positions for all 9 Vedic planets at given datetime.
    If dt is None, uses current IST time.

    Returns:
        {
          "Sun":     {"degree":..., "sign":..., "nakshatra":..., "nak_lord":...},
          "Moon":    {...},
          ...
          "Ketu":    {...},   # computed as Rahu + 180 deg
          "jd":      <Julian Day>,
          "ayanamsha": <Lahiri ayanamsha value>,
        }
    """
    if dt is None:
        dt = datetime.now(IST)

    jd = _to_jd(dt)
    ayanamsha = swe.get_ayanamsa_ut(jd)

    positions = {}
    for name, pid in _PLANET_IDS.items():
        try:
            lon = _sidereal_lon(jd, pid)
            positions[name] = _parse_position(lon)
        except Exception:
            positions[name] = {"degree": 0, "sign": "?", "nakshatra": "?", "nak_lord": "?"}

    # Ketu = Rahu + 180 deg
    rahu_lon = positions["Rahu"]["degree"]
    ketu_lon = (rahu_lon + 180) % 360
    positions["Ketu"] = _parse_position(ketu_lon)

    positions["jd"]         = jd
    positions["ayanamsha"]  = round(ayanamsha, 4)
    positions["computed_at"] = dt.strftime("%Y-%m-%d %H:%M IST")

    return positions


def get_moon_data(dt=None) -> dict:
    """
    Return Moon's sidereal position.
    Backward-compatible with original astro_engine interface.
    """
    if dt is None:
        dt = datetime.now(IST)
    jd  = _to_jd(dt)
    lon = _sidereal_lon(jd, swe.MOON)
    pos = _parse_position(lon)
    return {
        "degree":    pos["degree"],
        "sign":      pos["sign"],
        "nakshatra": pos["nakshatra"],
        "nak_lord":  pos["nak_lord"],
    }


def get_moon_data_for_date(dt) -> dict:
    """Alias for get_moon_data with explicit datetime. Backward-compatible."""
    return get_moon_data(dt)


def get_angular_distance(planet_a: str, planet_b: str, dt=None) -> float:
    """Return the angular distance (0–180 deg) between two planets."""
    planets = get_all_planets(dt)
    a = planets.get(planet_a, {}).get("degree", 0)
    b = planets.get(planet_b, {}).get("degree", 0)
    diff = abs(a - b)
    return min(diff, 360 - diff)


def get_dignities(dt=None) -> dict:
    """
    Return dignity status for each planet.
    Returns dict: {planet: "EXALTED"|"OWN"|"DEBILITATED"|"NEUTRAL"}
    """
    planets = get_all_planets(dt)
    EXALT = {
        "Sun": "Aries", "Moon": "Taurus", "Mars": "Capricorn",
        "Mercury": "Virgo", "Jupiter": "Cancer", "Venus": "Pisces",
        "Saturn": "Libra",
    }
    OWN = {
        "Sun": {"Leo"}, "Moon": {"Cancer"},
        "Mars": {"Aries","Scorpio"}, "Mercury": {"Gemini","Virgo"},
        "Jupiter": {"Sagittarius","Pisces"}, "Venus": {"Taurus","Libra"},
        "Saturn": {"Capricorn","Aquarius"},
    }
    DEBI = {
        "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
        "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo",
        "Saturn": "Aries",
    }
    result = {}
    for p in ["Sun","Moon","Mars","Mercury","Jupiter","Venus","Saturn"]:
        sign = planets.get(p, {}).get("sign", "")
        if EXALT.get(p) == sign:
            result[p] = "EXALTED"
        elif sign in OWN.get(p, set()):
            result[p] = "OWN"
        elif DEBI.get(p) == sign:
            result[p] = "DEBILITATED"
        else:
            result[p] = "NEUTRAL"
    return result


def get_gann_square_9(price: float) -> list:
    """
    Calculate Gann Square of 9 support/resistance levels.
    Formula: (sqrt(price) ± (degrees / 180))^2
    """
    import math
    if price <= 0: return []
    base = math.sqrt(price)
    # Gann angles for Square of 9
    angles = [45, 90, 135, 180, 225, 270, 315, 360, 405, 450, 495, 540, 585, 630, 675, 720]
    levels = []
    for a in angles:
        up = pow(base + (a / 180.0), 2)
        dn = pow(base - (a / 180.0), 2)
        levels.append({"angle": a, "up": round(up, 2), "dn": round(dn, 2) if dn > 0 else 0})
    return levels


def get_gann_reversal_dates(dt=None) -> dict:
    """
    Identify Gann Time Reversal dates based on Solar Degrees and seasonal cycles.
    """
    if dt is None:
        dt = datetime.now(IST)
    
    # 1. Seasonal / Solar Cycle Dates (Tropical based, approximate)
    # 0, 45, 90, 135, 180, 225, 270, 315 degrees of Sun
    GANN_DATES = [
        (3, 21, "Spring Equinox (0 deg)", "Market Mood Reset: Expect big swings and a possible complete change in direction."), 
        (5, 6, "Gann Mid-Point (45 deg)", "Trend Booster: The current trend usually speeds up or takes a quick break here."),
        (6, 21, "Summer Solstice (90 deg)", "The Exhaustion Peak: The market is 'running out of gas.' High chance it hits a ceiling and flips."),
        (8, 8, "Gann Mid-Point (135 deg)", "The Mid-Way Flip: A high chance for a quick zig-zag or reversal in the current trend."),
        (9, 22, "Autumn Equinox (180 deg)", "The Great Balance: The market levels out and often starts a completely new path."),
        (11, 8, "Gann Mid-Point (225 deg)", "The Pressure Cooker: Watch for sudden, sharp moves or panic selling/buying."),
        (12, 21, "Winter Solstice (270 deg)", "The Bargain Bottom: Market often hits a floor here. Good time for trends to start climbing."),
        (2, 4, "Gann Mid-Point (315 deg)", "The Pre-Spring Shakeup: The old trend is weakening. Watch for a cleanup before the reset.")
    ]
    
    # 2. Key High-Probability Dates
    HIGH_PROB = [
        (1, 3, "Year Start (Mass Pressure)", "New Year Re-Tune: Big players move money around, setting the mood for the months ahead."),
        (2, 12, "Feb Pivot", "Seasonal Turn: A standard time for indices to stop their current move and pivot."),
        (4, 16, "Apr Pivot", "Spring Swing: Mid-season check-in where strong trends often take a breather."),
        (5, 21, "May Pivot", "May Sensitivity: Part of the 'Sell in May' window. Very high chance for a swing high/low."),
        (7, 7, "July Pivot", "Mid-Year Shake: A time to re-evaluate the primary direction for the rest of the year."),
        (8, 28, "Aug Pivot", "Late Summer Storm: Historically a time for sudden, sharp reversals in momentum."),
        (10, 11, "Oct Pivot", "The Big Reversal: October is famous for trend changes. Watch for high-conviction flips."),
        (11, 21, "Nov Pivot", "Year-End Pivot: The last major turn before the final rally or cleanup into December.")
    ]
    
    today_m, today_d = dt.month, dt.day
    
    is_reversal = False
    event_name  = ""
    event_desc  = ""
    conviction  = "NORMAL"
    
    # Check if today is a reversal date (+/- 1 day window)
    for m, d, name, desc in GANN_DATES + HIGH_PROB:
        # Check window
        try:
            target_dt = datetime(dt.year, m, d)
            diff = abs((dt.replace(tzinfo=None) - target_dt).days)
            if diff <= 1:
                is_reversal = True
                event_name = name
                event_desc = desc
                conviction = "HIGH" if diff == 0 else "NORMAL"
                break
        except: continue
        
    # Find next 3 reversal dates
    upcoming = []
    check_dt = dt + timedelta(days=1)
    while len(upcoming) < 5:
        m, d = check_dt.month, check_dt.day
        for rm, rd, name, desc in GANN_DATES + HIGH_PROB:
            if rm == m and rd == d:
                upcoming.append({
                    "date":  check_dt.strftime("%d-%b"),
                    "event": name,
                    "desc":  desc
                })
        check_dt += timedelta(days=1)
        if (check_dt - dt).days > 365: break # safety
        
    return {
        "is_reversal_today": is_reversal,
        "today_event": event_name,
        "today_desc":  event_desc,
        "conviction": conviction,
        "upcoming_dates": upcoming
    }


def get_squaring_time_price(price: float, dt=None) -> dict:
    """
    Check if price is squared with time (Gann principle).
    Price in degrees (or points) should align with solar degree or calendar days.
    """
    if dt is None:
        dt = datetime.now(IST)
    
    planets = get_all_planets(dt)
    sun_deg = planets["Sun"]["degree"] # Absolute longitude 0-360
    
    # Simple check: Is Price % 360 close to Sun's absolute longitude?
    price_deg = price % 360
    diff = abs(price_deg - sun_deg)
    squared = diff < 5 or diff > 355
    
    return {
        "price": price,
        "sun_deg": round(sun_deg, 2),
        "diff": round(min(diff, 360-diff), 2),
        "is_squared": squared,
        "reversal_conviction": "HIGH" if squared else "LOW"
    }
