# =============================================================
# astro_logic.py  — Vedic Astro Score Engine (KP / Vedic)
# =============================================================
# Replaces the old primitive 4-rule engine with a proper
# multi-layer Vedic/KP analysis that matches _vedic_day_analysis
# in the main dashboard.
#
# Used by the dashboard INFO tab "Legacy Astro Score" expander
# and by external tools / background_worker.py
# =============================================================

from datetime import datetime, timedelta
import pytz

IST = pytz.timezone("Asia/Kolkata")

try:
    from astro_engine import get_moon_data, get_moon_data_for_date, get_all_planets, get_dignities, get_angular_distance
    _ENGINE_OK = True
except ImportError:
    _ENGINE_OK = False

# ── Nakshatra classifications ─────────────────────────────────────
# Based on VB Govardhan, KP Krishnamurti, standard Vedic books
NAK_LORDS = {
    "Ashwini":"Ke","Bharani":"Ve","Krittika":"Su","Rohini":"Mo","Mrigashira":"Ma","Ardra":"Ra",
    "Punarvasu":"Ju","Pushya":"Sa","Ashlesha":"Me","Magha":"Ke","Purva Phalguni":"Ve","Uttara Phalguni":"Su",
    "Hasta":"Mo","Chitra":"Ma","Swati":"Ra","Vishakha":"Ju","Anuradha":"Sa","Jyeshtha":"Me",
    "Mula":"Ke","Purva Ashadha":"Ve","Uttara Ashadha":"Su","Shravana":"Mo","Dhanishtha":"Ma",
    "Shatabhisha":"Ra","Purva Bhadrapada":"Ju","Uttara Bhadrapada":"Sa","Revati":"Me",
}

# Nakshatra market bias — based on KP + VB Govardhan research
# "G"=bullish, "B"=bearish, "T"=trap/volatile, "N"=neutral
NAK_BIAS = {
    "Ashwini":          "G",   # Ketu — quick momentum bursts
    "Bharani":          "B",   # Venus 6/8/12 = loss
    "Krittika":         "B",   # Sun — cutting, sharp falls
    "Rohini":           "T",   # Moon — TRAP (illusion rally, reverse after 10:30)
    "Mrigashira":       "N",   # Mars — searching, choppy
    "Ardra":            "B",   # Rahu — extreme volatile, usually down
    "Punarvasu":        "G",   # Jupiter — return, recovery
    "Pushya":           "G",   # Saturn — slow steady up (best nak)
    "Ashlesha":         "B",   # Mercury — weak, deceitful
    "Magha":            "B",   # Ketu — ancestral, bearish for index
    "Purva Phalguni":   "N",   # Venus — mildly positive luxury
    "Uttara Phalguni":  "G",   # Sun — stable, contract-making
    "Hasta":            "G",   # Moon — skillful, supportive
    "Chitra":           "N",   # Mars — construction, volatile
    "Swati":            "N",   # Rahu — independent, choppy
    "Vishakha":         "G",   # Jupiter — goal-oriented rally
    "Anuradha":         "G",   # Saturn — friendship, cooperative rally
    "Jyeshtha":         "B",   # Mercury — jealousy, sell-off
    "Mula":             "T",   # Ketu — VOLATILE (sudden up OR down)
    "Purva Ashadha":    "G",   # Venus — invincible, bull trend
    "Uttara Ashadha":   "G",   # Sun — final victory, strong
    "Shravana":         "G",   # Moon — listening, steady bullish
    "Dhanishtha":       "B",   # Mars — aggressive, often falls
    "Shatabhisha":      "B",   # Rahu — extreme bearish, sudden crash
    "Purva Bhadrapada": "B",   # Jupiter — fear, volatile down
    "Uttara Bhadrapada":"G",   # Saturn — steady, auspicious
    "Revati":           "G",   # Mercury — journey end, positive
}

NAK_EXTREME = {"Ardra", "Ashlesha", "Shatabhisha", "Dhanishtha"}
NAK_STRONG  = {"Pushya", "Hasta", "Anuradha", "Punarvasu", "Uttara Phalguni",
               "Revati", "Purva Ashadha", "Uttara Ashadha", "Shravana", "Uttara Bhadrapada"}

# Moon sign market bias
SIGN_BIAS = {
    "Taurus":      ("G", 2, "Moon exalted — gap-down then 200pt recovery; buy"),
    "Cancer":      ("G", 1, "Moon own sign — emotionally supportive"),
    "Sagittarius": ("G", 1, "Moon in Sagittarius — positive close tendency"),
    "Scorpio":     ("N", 0, "Moon in Scorpio — intense but mixed"),
    "Pisces":      ("G", 1, "Moon in Pisces — gentle bullish"),
    "Aries":       ("B", 1, "Moon in Aries — aggressive, risky day"),
    "Capricorn":   ("B", 1, "Moon in Capricorn — restrictive, sluggish"),
    "Aquarius":    ("B", 1, "Moon in Aquarius — Saturn energy, bearish"),
    "Gemini":      ("N", 0, "Moon in Gemini — two-sided volatile"),
    "Leo":         ("N", 0, "Moon in Leo — pride, unpredictable"),
    "Virgo":       ("N", 0, "Moon in Virgo — analytical, choppy"),
    "Libra":       ("N", 0, "Moon in Libra — balanced, mild positive"),
}

# KP sub-lord signal (most important decider)
KP_SUB_SIGNAL = {
    "Ju": ("STRONG BUY",  "bullish",   2),
    "Ve": ("STRONG BUY",  "bullish",   2),
    "Mo": ("BUY",         "bullish",   1),
    "Su": ("MILD BUY",    "mild_bull", 1),
    "Me": ("MIXED",       "neutral",   0),
    "Sa": ("SELL",        "bearish",  -2),
    "Ra": ("STRONG SELL", "bearish",  -2),
    "Ke": ("STRONG SELL", "bearish",  -2),
    "Ma": ("SELL",        "bearish",  -1),
}

# KP sub-lord sequence (Vimshottari dasha order: Ke,Ve,Su,Mo,Ma,Ra,Ju,Sa,Me)
_SUB_SEQUENCE   = ["Ke","Ve","Su","Mo","Ma","Ra","Ju","Sa","Me"]

# ── Sector & Commodity Significators ──────────────────────────────
SECTOR_SIGNIFICATORS = {
    "GOLD":        {"ruler": "Sun",     "bias": "G", "desc": "Sun rules precious metals and sovereign assets."},
    "SILVER":      {"ruler": "Moon",    "bias": "G", "desc": "Moon rules silver and white metals."},
    "CRUDE":       {"ruler": "Saturn",  "bias": "B", "desc": "Saturn rules petroleum, coal, and deep-earth minerals."},
    "NAT GAS":     {"ruler": "Neptune", "bias": "N", "desc": "Neptune rules liquids and gases; high volatility."},
    "COPPER":      {"ruler": "Mars",    "bias": "B", "desc": "Mars rules copper, iron, and weaponry."},
    "BANKS":       {"ruler": "Jupiter", "bias": "G", "desc": "Jupiter rules banking, finances, and expansion."},
    "IT":          {"ruler": "Ketu",    "bias": "B", "desc": "Rahu/Ketu rule technology and software systems."},
    "FMCG":        {"ruler": "Venus",   "bias": "G", "desc": "Venus rules consumer goods, luxury, and sweets."},
    "AUTO":        {"ruler": "Mars",    "bias": "B", "desc": "Mars rules engineering, engines, and machinery."},
}


def get_kp_reversals(dt=None) -> list:
    """
    Find exact times when Moon's KP sub-lord changes during market hours.
    These are high-conviction reversal points.
    """
    if dt is None:
        dt = datetime.now(IST)
    
    # Analyze from 09:00 to 15:30 IST in 1-minute steps
    start_time = dt.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time   = dt.replace(hour=15, minute=30, second=0, microsecond=0)
    
    reversals = []
    current_time = start_time
    last_sub = None
    
    while current_time <= end_time:
        moon_deg = get_moon_data(current_time)["degree"]
        sub = _get_sub_lord(moon_deg)
        
        if last_sub is not None and sub != last_sub:
            label, bias, pts = KP_SUB_SIGNAL.get(sub, ("MIXED","neutral",0))
            reversals.append({
                "time": current_time.strftime("%H:%M"),
                "event": f"Sub-lord Change: {last_sub} → {sub}",
                "bias": label,
                "conviction": "HIGH" if abs(pts) >= 2 else "MEDIUM"
            })
        last_sub = sub
        current_time += timedelta(minutes=1)
        
    return reversals


# Vimshottari dasha years — proportional sub-lord spans within each nakshatra
_KP_DASHA_YEARS = [7, 20, 6, 10, 7, 18, 16, 19, 17]  # total = 120

def _get_sub_lord(moon_degree: float) -> str:
    """
    Get KP sub-lord using correct Vimshottari proportional formula.
    Each nakshatra (13°20') is divided proportionally by dasha years,
    NOT equally (equal division gives wrong sub-lord for ~60% of positions).
    """
    nak_span   = 360 / 27                    # 13.333° per nakshatra
    pos_in_nak = moon_degree % nak_span      # position within current nak
    frac       = pos_in_nak / nak_span       # 0.0 → 1.0
    cumulative = 0.0
    for i, yrs in enumerate(_KP_DASHA_YEARS):
        cumulative += yrs / 120.0
        if frac <= cumulative:
            return _SUB_SEQUENCE[i]
    return _SUB_SEQUENCE[-1]


# ── Day Lord (Vara) scoring ─────────────────────────────────────────
_VARA_SIGNAL = {
    "Sun":     ("G",  1, "Sun Vara — market opens strong (+1)"),
    "Moon":    ("G",  1, "Moon Vara — emotional, often green open (+1)"),
    "Mars":    ("B",  1, "Mars Vara — aggressive selling tendency (-1)"),
    "Mercury": ("N",  0, "Mercury Vara — volatile two-sided (0)"),
    "Jupiter": ("G",  2, "Jupiter Vara — most bullish weekday (+2)"),
    "Venus":   ("G",  1, "Venus Vara — positive close tendency (+1)"),
    "Saturn":  ("B",  1, "Saturn Vara — sluggish; bearish tendency (-1)"),
}
# Mon=Moon, Tue=Mars, Wed=Mercury, Thu=Jupiter, Fri=Venus, Sat=Saturn, Sun=Sun
_WEEKDAY_LORD = {0:"Moon", 1:"Mars", 2:"Mercury", 3:"Jupiter", 4:"Venus", 5:"Saturn", 6:"Sun"}

# ── Opening Hora at 9:15 IST ─────────────────────────────────────────
_HORA_BULL = {"Jupiter", "Venus", "Moon", "Sun"}
_HORA_BEAR = {"Saturn", "Mars", "Rahu", "Ketu"}
# Hora sequence: Sun→Venus→Mercury→Moon→Saturn→Jupiter→Mars (repeat)
_HORA_SEQ  = ["Sun","Venus","Mercury","Moon","Saturn","Jupiter","Mars"]

def _hora_lord_at_915(dt) -> str:
    """Return planetary hora lord at 09:15 IST for the given date."""
    day_lord  = _WEEKDAY_LORD.get(dt.weekday(), "Sun")
    start_idx = _HORA_SEQ.index(day_lord)
    # Hora starts at sunrise (~06:00 IST), each hora = 1 hour
    # 09:15 IST = 3h15m after 06:00 → hora index 3
    hora_num  = 3   # 06:00=0, 07:00=1, 08:00=2, 09:00=3
    return _HORA_SEQ[(start_idx + hora_num) % 7]

# ── Tithi scoring ─────────────────────────────────────────────────────
_TITHI_NAMES = [
    "Pratipada","Dwitiya","Tritiya","Chaturthi","Panchami",
    "Shashthi","Saptami","Ashtami","Navami","Dashami",
    "Ekadashi","Dwadashi","Trayodashi","Chaturdashi","Purnima"
]

def _get_tithi(dt) -> tuple:
    """
    Compute current tithi (lunar day) and paksha from Moon-Sun angular difference.
    Returns (tithi_name: str, paksha: str).
    """
    try:
        from astro_engine import get_all_planets
        p = get_all_planets(dt)
        diff = (p["Moon"]["degree"] - p["Sun"]["degree"]) % 360
        tithi_num = int(diff / 12) + 1   # 1–30
        if tithi_num >= 30 or diff >= 348:
            return "Amavasya", "Krishna"
        if tithi_num <= 15:
            return _TITHI_NAMES[tithi_num - 1], "Shukla"
        else:
            t = _TITHI_NAMES[tithi_num - 16]
            return t, "Krishna"
    except Exception:
        return "Unknown", "Shukla"


def _interpret_score(score: int, has_trap: bool, jup_exalted: bool) -> str:
    """Convert net score to signal label — aligned with dashboard labels."""
    if jup_exalted and score < 0:
        return "⚠️ CAUTION (Jupiter Floor)"
    if score >= 5:   return "🟢 STRONG BULLISH"
    if score >= 2:   return "🟢 BULLISH"
    if score == 1:   return "🟢 MILD BULLISH"
    if score == 0:   return "🟡 MIXED"
    if score == -1 and has_trap: return "⚠️ TRAP DAY"
    if score >= -3:  return "🔴 BEARISH"
    if score >= -6:  return "🔴 STRONG BEARISH"
    return "🔴 EXTREME BEARISH"


def get_astro_score(dt=None) -> dict:
    """
    Full Vedic/KP astro score for a given datetime (default = now IST).

    Returns dict compatible with the dashboard's expected format:
    {
        "score":     int,
        "signal":    str,
        "moon_sign": str,
        "nakshatra": str,
        "nak_lord":  str,
        "sub_lord":  str,
        "reason":    str,
        "details":   list of (text, type) pairs,
    }
    """
    if dt is None:
        dt = datetime.now(IST)

    reasons = []
    score   = 0
    has_trap = False
    jup_exalted = False

    if not _ENGINE_OK:
        return {
            "score": 0, "signal": "🟡 Engine unavailable",
            "moon_sign": "?", "nakshatra": "?", "nak_lord": "?",
            "sub_lord": "?", "reason": "astro_engine not installed",
            "details": []
        }

    # ── Get positions ─────────────────────────────────────────────
    try:
        moon    = get_moon_data(dt)
        planets = get_all_planets(dt)
        digs    = get_dignities(dt)
    except Exception as e:
        return {
            "score": 0, "signal": "🟡 Calc error",
            "moon_sign": "?", "nakshatra": "?", "nak_lord": "?",
            "sub_lord": "?", "reason": str(e), "details": []
        }

    moon_nak  = moon["nakshatra"]
    moon_sign = moon["sign"]
    moon_deg  = moon["degree"]
    moon_lord = moon.get("nak_lord", NAK_LORDS.get(moon_nak, "?"))
    sub_lord  = _get_sub_lord(moon_deg)

    # ── LAYER 1: Nakshatra ────────────────────────────────────────
    bias = NAK_BIAS.get(moon_nak, "N")
    if bias == "G":
        pts = 2   # flat +2 matching dashboard _vedic_day_analysis (no NAK_STRONG bonus)
        score += pts
        reasons.append(f"Moon in {moon_nak} ({moon_lord}★) — bullish nak (+{pts})")
    elif bias == "B":
        pts = 3 if moon_nak in NAK_EXTREME else 2
        score -= pts
        reasons.append(f"Moon in {moon_nak} ({moon_lord}★) — bearish nak (-{pts})")
    elif bias == "T":
        score -= 1    # small penalty for uncertainty
        has_trap = True
        reasons.append(f"Moon in {moon_nak} — VOLATILE TRAP (up or down, wait for break)")

    # ── LAYER 2: Moon Sign ────────────────────────────────────────
    s_type, s_pts, s_reason = SIGN_BIAS.get(moon_sign, ("N", 0, ""))
    if s_pts:
        score += s_pts if s_type == "G" else -s_pts
        reasons.append(s_reason)

    # ── LAYER 3: KP Sub-lord ─────────────────────────────────────
    sub_label, sub_bias, sub_pts = KP_SUB_SIGNAL.get(sub_lord, ("MIXED","neutral",0))
    if sub_pts:
        score += sub_pts
        reasons.append(f"KP Sub-lord {sub_lord} = {sub_label} ({sub_pts:+d})")


    # ── LAYER 3b: Day Lord (Vara) — matches dashboard LAYER C ─────────────
    _vara_lord = _WEEKDAY_LORD.get(dt.weekday(), "Sun")
    _vara_bias, _vara_pts, _vara_note = _VARA_SIGNAL.get(_vara_lord, ("N", 0, ""))
    if _vara_pts:
        _adj = _vara_pts if _vara_bias == "G" else -_vara_pts
        score += _adj
        reasons.append(f"Day lord {_vara_lord} — {_vara_note}")

    # ── LAYER 3c: Opening Hora at 9:15 — matches dashboard LAYER D ─────────
    _hora_lord = _hora_lord_at_915(dt)
    if _hora_lord in _HORA_BULL:
        score += 1
        reasons.append(f"Opening hora: {_hora_lord} (benefic) — bullish (+1)")
    elif _hora_lord in _HORA_BEAR:
        score -= 1
        reasons.append(f"Opening hora: {_hora_lord} (malefic) — bearish (-1)")

    # ── LAYER 3d: Tithi — matches dashboard LAYER E ──────────────────────────
    try:
        _tithi_name, _paksha = _get_tithi(dt)
        if _tithi_name == "Amavasya":
            score -= 3
            reasons.append("Amavasya — extreme bearish; sell at open (-3)")
        elif _paksha == "Krishna" and _tithi_name == "Pratipada":
            score -= 3
            reasons.append("Krishna Pratipada (post-Amavasya) — bearish (-3)")
        elif _paksha == "Shukla" and _tithi_name == "Pratipada":
            score -= 1
            reasons.append("Shukla Pratipada — mild bearish (-1)")
        elif _tithi_name == "Chaturdashi":
            score -= 1
            reasons.append(f"{_paksha} Chaturdashi — avoid fresh longs (-1)")
        elif _paksha == "Krishna" and _tithi_name in ("Ashtami","Navami","Dashami"):
            score -= 1
            reasons.append(f"Krishna {_tithi_name} — inauspicious tithi (-1)")
        elif _paksha == "Shukla" and _tithi_name in ("Panchami","Saptami","Dashami","Ekadashi","Dwadashi"):
            score += 1
            reasons.append(f"Shukla {_tithi_name} — auspicious tithi (+1)")
    except Exception:
        pass

    # ── LAYER 4: Jupiter dignity ──────────────────────────────────
    jup_dig = digs.get("Jupiter", "NEUTRAL")
    jup_sign = planets.get("Jupiter", {}).get("sign", "")
    if jup_dig == "EXALTED":
        score += 3
        jup_exalted = True
        reasons.append("Jupiter EXALTED in Cancer — market floor, no crash (+3)")
    elif jup_dig == "OWN":
        score += 2
        reasons.append(f"Jupiter in own sign {jup_sign} — expansion (+2)")
    elif jup_dig == "DEBILITATED":
        score -= 1
        reasons.append("Jupiter debilitated — markets depressed (-1)")

    # ── LAYER 5: Venus dignity ────────────────────────────────────
    ven_dig  = digs.get("Venus", "NEUTRAL")
    ven_sign = planets.get("Venus", {}).get("sign", "")
    if ven_dig == "EXALTED":
        score += 2
        reasons.append("Venus exalted (Pisces) — FMCG/banks bullish (+2)")
    elif ven_dig == "OWN":
        score += 1
        reasons.append(f"Venus own sign {ven_sign} — mild bullish (+1)")
    elif ven_dig == "DEBILITATED":
        score -= 2
        reasons.append("Venus debilitated (Virgo) — luxury weak (-2)")

    # ── LAYER 6: Moon-Jupiter aspect (Gajakesari) ─────────────────
    try:
        mj = get_angular_distance("Moon", "Jupiter", dt)
        if mj < 10:
            score += 2; reasons.append(f"Moon-Jupiter conjunct {mj:.0f}° (Gajakesari +2)")
        elif abs(mj - 120) < 10:
            score += 2; reasons.append(f"Moon trine Jupiter {mj:.0f}° (Gajakesari +2)")
        elif abs(mj - 180) < 8:
            score += 1; reasons.append(f"Moon opp Jupiter {mj:.0f}° — recovery tendency (+1)")
    except Exception:
        pass

    # ── LAYER 7: Moon-Ketu conjunction (bearish) ──────────────────
    try:
        mk = get_angular_distance("Moon", "Ketu", dt)
        if mk < 10:
            score -= 3; reasons.append(f"Moon-Ketu {mk:.0f}° conjunct — BankNifty weak (-3)")
        elif mk < 15:
            score -= 2; reasons.append(f"Moon-Ketu {mk:.0f}° — confusion, bearish (-2)")
    except Exception:
        pass

    # ── LAYER 8: Moon-Saturn (Vish Yoga) ─────────────────────────
    try:
        ms = get_angular_distance("Moon", "Saturn", dt)
        if ms < 10:
            score -= 2; reasons.append(f"Moon-Saturn {ms:.0f}° (Vish Yoga) — heavy fall risk (-2)")
        elif abs(ms - 90) < 8:
            score -= 1; reasons.append(f"Moon sq Saturn {ms:.0f}° — selling pressure (-1)")
    except Exception:
        pass

    # ── LAYER 9: Jupiter Exalted Floor override ───────────────────
    if jup_exalted and score < 0:
        shift = min(2, abs(score))
        score += shift
        reasons.append(f"Jupiter Floor: bearish downgraded by {shift} pts — buy dips")

    # ── Final ─────────────────────────────────────────────────────
    signal = _interpret_score(score, has_trap, jup_exalted)

    return {
        "score":     score,
        "signal":    signal,
        "moon_sign": moon_sign,
        "nakshatra": moon_nak,
        "nak_lord":  moon_lord,
        "sub_lord":  sub_lord,
        "reason":    " | ".join(reasons[:4]),   # top 4 reasons for display
        "details":   reasons,
        "jup_exalted": jup_exalted,
    }


def get_future_astro(days_ahead: int = 1) -> dict:
    """
    Return astro score for N days ahead. Backward-compatible with old interface.
    """
    dt = datetime.now(IST) + timedelta(days=days_ahead)
    result = get_astro_score(dt)
    return {
        "date":      dt.strftime("%d-%b"),
        "sign":      result["moon_sign"],
        "nakshatra": result["nakshatra"],
        "score":     result["score"],
        "signal":    result["signal"],
        "reason":    result["reason"],
    }


def get_week_forecast() -> list:
    """Return astro scores for the next 7 trading days."""
    from datetime import date
    NSE_HOLIDAYS = {
        date(2026,1,26), date(2026,3,17), date(2026,4,2),
        date(2026,4,10), date(2026,4,14), date(2026,5,1),
        date(2026,8,15), date(2026,10,2), date(2026,10,24),
        date(2026,11,5), date(2026,12,25),
    }
    results = []
    d = datetime.now(IST)
    count = 0
    delta = 1
    while count < 7:
        d2 = d + timedelta(days=delta)
        if d2.weekday() < 5 and d2.date() not in NSE_HOLIDAYS:
            r = get_astro_score(d2)
            r["date"] = d2.strftime("%a %d-%b")
            results.append(r)
            count += 1
        delta += 1
    return results
