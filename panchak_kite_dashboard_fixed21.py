# ==========================================================
# PANCHAK + BREAKOUT DASHBOARD (PRODUCTION – HOLIDAY AWARE)
# ==========================================================

import os
import threading
#import time
from datetime import datetime, timedelta, date, time
import pandas as pd
import streamlit as st
from kiteconnect import KiteConnect
import pytz
from streamlit_autorefresh import st_autorefresh
from io import BytesIO
import smtplib
from email.message import EmailMessage
import numpy as np
import time as tm
import math
import json
import urllib.request
#from astro_logic import get_astro_score
from astro_time import get_time_signal
from astro_logic import get_astro_score, get_future_astro

# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM ALERT CONFIG
# Bot   : @streamlit123_bot  (Name: streamlit)
# Token : 8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A
# Channel: "Panchak Alerts"  https://t.me/+d4StsTfQQyI4MzVl  (bot is Admin ✅)
# Chat ID : -1003706739531  ✅ CONFIRMED
# ═══════════════════════════════════════════════════════════════════════
TG_BOT_TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"

# ── Chat ID resolution ────────────────────────────────────────────────
# Private channels have numeric IDs like -100XXXXXXXXXX.
# The dashboard auto-detects this on first run via getUpdates and saves
# to cache/tg_chat_id.txt. After that it loads from file instantly.
# You can also run  python3 get_chat_id.py  to fetch it manually.
# ─────────────────────────────────────────────────────────────────────
_TG_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
_TG_ID_FILE   = os.path.join(_TG_CACHE_DIR, "tg_chat_id.txt")

def _resolve_tg_chat_id():
    """
    1. Check cache/tg_chat_id.txt (fastest, runs every time after first)
    2. Call getUpdates API to find the 'Panchak Alerts' channel
    3. If found, save to file and return
    4. Return "" if unavailable (alerts silently skipped)
    """
    import urllib.request as _ur, json as _js
    os.makedirs(_TG_CACHE_DIR, exist_ok=True)

    # Step 1 — cached
    if os.path.exists(_TG_ID_FILE):
        _cid = open(_TG_ID_FILE).read().strip()
        if _cid:
            return _cid

    # Step 2 — fetch from API
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
                # Prefer the Panchak Alerts channel by name
                if "panchak" in _title or "alert" in _title:
                    _best = _cid
                    break
                _best = _cid  # fallback: last channel found

        if _best:
            open(_TG_ID_FILE, "w").write(_best)
            return _best

    except Exception:
        pass

    return ""

# Resolve at startup (fast from cache, slow only on first ever run)
TG_CHAT_ID = _resolve_tg_chat_id() or "-1003706739531"  # Hardcoded fallback: Panchak Alerts channel

_TG_DEDUP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache", f"tg_dedup_{datetime.now().strftime('%Y%m%d')}.json")

def _load_tg_dedup():
    try:
        with open(_TG_DEDUP_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_tg_dedup(d):
    try:
        os.makedirs(os.path.dirname(_TG_DEDUP_FILE), exist_ok=True)
        with open(_TG_DEDUP_FILE, "w") as f:
            json.dump(d, f)
    except Exception:
        pass

def send_telegram(message: str, dedup_key: str = None, parse_mode: str = "HTML") -> bool:
    """
    Send a Telegram message via bot.
    dedup_key: if provided, message is sent only ONCE per trading day per key.
    Returns True on success.
    """
    if not TG_BOT_TOKEN:
        return False
    # Dedup check
    if dedup_key:
        dedup = _load_tg_dedup()
        if dedup.get(dedup_key):
            return False
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
        if ok and dedup_key:
            dedup = _load_tg_dedup()
            dedup[dedup_key] = datetime.now().isoformat()
            _save_tg_dedup(dedup)
        return ok
    except Exception as e:
        print(f"TG send error: {e}")
        return False

def send_telegram_bg(message: str, dedup_key: str = None):
    """Send telegram in background thread — non-blocking."""
    threading.Thread(target=send_telegram, args=(message, dedup_key), daemon=True).start()

# ═══════════════════════════════════════════════════════════════════════
# 🪐 VEDIC ASTRO ENGINE — Pure Python (no external deps)
# Computes Moon nakshatra, rashi, tithi, planetary positions (sidereal/Lahiri)
# for any date. Used by the 10-Day Nifty Astro Forecast table.
# ═══════════════════════════════════════════════════════════════════════

def _jd(dt):
    """Gregorian date → Julian Day Number (UTC datetime)."""
    a = (14 - dt.month) // 12
    y = dt.year + 4800 - a
    m = dt.month + 12 * a - 3
    n = dt.day + (153*m+2)//5 + 365*y + y//4 - y//100 + y//400 - 32045
    return n + (dt.hour + dt.minute/60 + dt.second/3600 - 12) / 24

def _lahiri(jd_val):
    """Lahiri ayanamsa (degrees) for given Julian Day."""
    return 23.8506 + (jd_val - 2451545.0) * (50.27 / (3600 * 365.25))

def _sid(trop, jd_val):
    """Tropical → sidereal (Lahiri)."""
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

def _vedic_day_analysis(d_date):
    """
    Compute full Vedic day snapshot for a trading date.
    Returns dict with planetary positions and a scored Nifty signal.
    """
    # Use 9:15 AM IST = 3:45 AM UTC
    dt_utc = datetime(d_date.year, d_date.month, d_date.day, 3, 45, 0)
    jd_val = _jd(dt_utc)
    ay = _lahiri(jd_val)

    # Sidereal positions
    m_s  = _sid(_moon_trop(jd_val),    jd_val)
    s_s  = _sid(_sun_trop(jd_val),     jd_val)
    v_s  = _sid(_venus_trop(jd_val),   jd_val)
    ma_s = _sid(_mars_trop(jd_val),    jd_val)
    j_s  = _sid(_jupiter_trop(jd_val), jd_val)
    sa_s = _sid(_saturn_trop(jd_val),  jd_val)
    me_s = _sid(_mercury_trop(jd_val), jd_val)
    r_s  = _sid(_rahu_trop(jd_val),    jd_val)
    k_s  = (r_s + 180) % 360

    moon_nak, moon_nak_lord, moon_nak_idx = _nak(m_s)
    sun_nak,  sun_nak_lord,  _            = _nak(s_s)
    mars_nak, _,             _            = _nak(ma_s)
    rahu_nak, _,             _            = _nak(r_s)

    moon_rashi   = _rashi(m_s)
    sun_rashi    = _rashi(s_s)
    venus_rashi  = _rashi(v_s)
    mars_rashi   = _rashi(ma_s)
    saturn_rashi = _rashi(sa_s)
    jupiter_rashi= _rashi(j_s)
    mercury_rashi= _rashi(me_s)
    rahu_rashi   = _rashi(r_s)

    # Angular separations
    moon_ketu   = _ang_dist(m_s,  k_s)
    moon_rahu   = _ang_dist(m_s,  r_s)
    moon_saturn = _ang_dist(m_s,  sa_s)
    mars_rahu   = _ang_dist(ma_s, r_s)
    sun_saturn  = _ang_dist(s_s,  sa_s)
    mars_saturn = _ang_dist(ma_s, sa_s)
    merc_mars   = _ang_dist(me_s, ma_s)
    sat_venus   = _ang_dist(sa_s, v_s)
    merc_rahu   = _ang_dist(me_s, r_s)
    moon_sun    = _ang_dist(m_s,  s_s)

    # Tithi
    tithi_raw = (m_s - s_s) % 360
    tithi_num = int(tithi_raw / 12) + 1
    tithi_name = _TITHI_NAMES[min(tithi_num - 1, 29)]
    paksha = "Shukla" if tithi_num <= 15 else "Krishna"

    # Moon position in navamsha (D9) — each navamsha = 3.33°
    nav_idx = int((m_s % 30) / (30/9))   # 0–8 within sign
    nav_sign = (int(m_s / 30) * 9 + nav_idx) % 12
    moon_d9_rashi = _RASHIS[nav_sign]

    # Moon from Sun (house position)
    moon_from_sun = int(((m_s - s_s) % 360) / 30) + 1  # 1-12

    # ── SCORING ENGINE ─────────────────────────────────────────────
    bearish_pts = 0
    bullish_pts = 0
    signals = []   # list of (text, type) where type = B/G/T/M

    # --- NAKSHATRA ---
    if moon_nak in _BEARISH_NAKS:
        bearish_pts += 2
        signals.append((f"Moon in {moon_nak} ({moon_nak_lord}'s nak)", "B"))
    elif moon_nak in _TRAP_NAKS:
        signals.append((f"Moon in Rohini — TRAP (false rally)", "T"))
    elif moon_nak in _BULLISH_NAKS:
        bullish_pts += 2
        signals.append((f"Moon in {moon_nak} ({moon_nak_lord}'s nak)", "G"))

    # --- TITHI ---
    if tithi_name == "Amavasya":
        bearish_pts += 3
        signals.append(("Amavasya — extreme bearish", "B"))
    elif paksha == "Krishna" and tithi_name == "Pratipada":
        bearish_pts += 3
        signals.append(("Krishna Pratipada (day after Amavasya) — almost always bearish", "B"))
    elif paksha == "Shukla" and tithi_name == "Pratipada":
        bearish_pts += 1
        signals.append(("Shukla Pratipada — mild bearish tendency", "B"))
    elif tithi_name == "Purnima":
        signals.append(("Purnima — volatile, watch direction at open", "M"))

    # --- PLANETARY CONJUNCTIONS ---
    # Moon-Ketu (within 15°)
    if moon_ketu < 15:
        bearish_pts += 3
        signals.append((f"Moon–Ketu {moon_ketu:.0f}° apart — Reliance/HDFC weak, Nifty falls", "B"))
    # Moon-Rahu (within 10°)
    if moon_rahu < 10:
        bearish_pts += 2
        signals.append((f"Moon–Rahu {moon_rahu:.0f}° — confusion/illusion; trap rallies possible", "T"))

    # Mars-Rahu (within 15°) — most dangerous
    if mars_rahu < 15:
        bearish_pts += 4
        signals.append((f"Mars–Rahu {mars_rahu:.0f}° — EXTREME bearish; crude bullish; lower circuit risk", "B"))
    # Mercury+Mars+Rahu triple (all within 15° of each other)
    if mars_rahu < 15 and merc_rahu < 15 and merc_mars < 15:
        bearish_pts += 2
        signals.append(("Mercury–Mars–Rahu TRIPLE — fire doubled; crude to surge; Nifty crash", "B"))

    # Saturn-Venus same sign (bearish while in Pisces)
    if saturn_rashi == venus_rashi:
        bearish_pts += 2
        signals.append((f"Saturn–Venus conjunct in {saturn_rashi} — bearish until Venus exits", "B"))
    elif sat_venus < 20:
        bearish_pts += 1
        signals.append((f"Saturn–Venus {sat_venus:.0f}° — mild bearish pressure", "B"))

    # Sun-Saturn exact (within 3°)
    if sun_saturn < 3:
        bearish_pts += 4
        signals.append((f"Sun–Saturn {sun_saturn:.1f}° — Nifty+Gold+Silver ALL fall; only crude rises", "B"))
    elif sun_saturn < 8:
        bearish_pts += 2
        signals.append((f"Sun–Saturn {sun_saturn:.1f}° — strong bearish; Gold/Silver weak", "B"))

    # Mars-Saturn exact (within 3°)
    if mars_saturn < 3:
        bearish_pts += 4
        signals.append((f"Mars–Saturn {mars_saturn:.1f}° — EXTREME; geopolitical risk; market crash", "B"))
    elif mars_saturn < 8:
        bearish_pts += 2
        signals.append((f"Mars–Saturn {mars_saturn:.1f}° — very strong bearish", "B"))

    # Mercury-Mars within 5°
    if merc_mars < 5:
        bearish_pts += 2
        signals.append((f"Mercury–Mars {merc_mars:.1f}° — aggression doubled; market fall accelerates", "B"))

    # Moon-Saturn within 10°
    if moon_saturn < 10:
        bearish_pts += 2
        signals.append((f"Moon–Saturn {moon_saturn:.0f}° — mood heavy; bearish", "B"))

    # Sun in neecha navamsha (Aquarius, Libra navamsha)
    if sun_rashi == "Aquarius":
        bearish_pts += 1
        signals.append(("Sun in Aquarius — negative rashi for markets", "B"))

    # Sun nakshatra lord = Jupiter + Jupiter retrograde (bearish amplifier)
    if sun_nak_lord == "Jupiter":
        bearish_pts += 1
        signals.append((f"Sun in Jupiter's nakshatra ({sun_nak}) — correction tendency", "B"))

    # Moon 11th from Sun
    if moon_from_sun == 11:
        bearish_pts += 1
        signals.append(("Moon 11th from Sun — bearish sign", "B"))

    # Sun in Moon's nakshatra (Rohini/Hasta/Shravana) — volatile
    if sun_nak_lord == "Moon":
        signals.append((f"Sun in Moon's nakshatra ({sun_nak}) — volatile day", "M"))

    # Moon navamsha in Saturn → bearish (from transcripts: Moon navamsha with Saturn = bad)
    if moon_d9_rashi in ("Capricorn", "Aquarius"):
        bearish_pts += 1
        signals.append((f"Moon D9 in Saturn's sign ({moon_d9_rashi}) — bearish tinge", "B"))

    # Jupiter retrograde period (Mar 11 – Jul 2026 approx) — amplifies bearish
    # Jupiter goes retrograde when it slows and reverses; approx check by date
    _j_retro_start = date(2026, 3, 11)
    _j_retro_end   = date(2026, 7, 10)   # approximate direct station
    if _j_retro_start <= d_date <= _j_retro_end:
        if sun_nak_lord == "Jupiter" or moon_nak_lord == "Jupiter":
            bearish_pts += 1
            signals.append(("Jupiter retrograde + Sun/Moon in Jupiter nakshatra — brutal correction amplifier", "B"))

    # Crude oil signal (inverse to Nifty)
    crude_bull = []
    if mars_rahu < 15:                            crude_bull.append("Mars–Rahu")
    if mars_saturn < 8:                           crude_bull.append("Mars–Saturn")
    if saturn_rashi in ("Libra", "Capricorn"):    crude_bull.append(f"Saturn uchha ({saturn_rashi})")
    if crude_bull:
        signals.append((f"⛽ CRUDE BULLISH signal — {', '.join(crude_bull)}", "M"))

    # --- BULLISH CONDITIONS ---
    if venus_rashi == "Aries":
        bullish_pts += 3
        signals.append(("Venus in Aries — bottom zone; heavy LONG bias", "G"))
    if moon_rashi == "Taurus":
        bullish_pts += 2
        signals.append(("Moon in Taurus — gap-down then 200-250pt recovery; positive close", "G"))
    if moon_rashi == "Sagittarius":
        bullish_pts += 1
        signals.append(("Moon in Sagittarius (Dhanu) — bullish rashi", "G"))
    if moon_rashi == "Cancer":
        bullish_pts += 1
        signals.append(("Moon in Cancer (exalted) — supportive", "G"))
    if sun_rashi == "Capricorn":
        bullish_pts += 2
        signals.append(("Sun in Capricorn — bullish monthly bias", "G"))

    # ── VASUDEV SIGN-BIAS RULES ──────────────────────────────────
    _v_fertile = {"Aries","Taurus","Leo","Scorpio","Sagittarius"}
    _v_barren  = {"Gemini","Cancer","Libra","Aquarius","Pisces"}
    _all_rashis = [jupiter_rashi, saturn_rashi, rahu_rashi,
                   sun_rashi, moon_rashi, venus_rashi, mercury_rashi, mars_rashi]
    _fertile_cnt = sum(1 for r in _all_rashis if r in _v_fertile)
    _barren_cnt  = sum(1 for r in _all_rashis if r in _v_barren)
    if _fertile_cnt >= 5:
        bullish_pts += 2
        signals.append((f"Vasudev: {_fertile_cnt}/8 planets in fertile signs — strong buying bias", "G"))
    elif _fertile_cnt >= 4 and _fertile_cnt > _barren_cnt:
        bullish_pts += 1
        signals.append((f"Vasudev: majority planets in fertile signs ({_fertile_cnt}) — mild bullish", "G"))
    if _barren_cnt >= 5:
        bearish_pts += 2
        signals.append((f"Vasudev: {_barren_cnt}/8 planets in barren signs — depressed market", "B"))
    elif _barren_cnt >= 4 and _barren_cnt > _fertile_cnt:
        bearish_pts += 1
        signals.append((f"Vasudev: majority planets in barren signs ({_barren_cnt}) — mild bearish", "B"))
    # Sun monthly rashi bias
    if sun_rashi in {"Aquarius","Gemini"}:
        bearish_pts += 1
        signals.append((f"Vasudev: Sun in {sun_rashi} — bearish monthly rashi", "B"))
    elif sun_rashi in {"Sagittarius","Leo","Aries","Scorpio"} and sun_rashi != "Capricorn":
        bullish_pts += 1
        signals.append((f"Vasudev: Sun in {sun_rashi} — bullish monthly rashi", "G"))

    # ── FINAL SIGNAL ──────────────────────────────────────────────
    net = bullish_pts - bearish_pts
    if net >= 3:
        overall = "🟢 BULLISH"
        intensity = "Strong"
    elif net >= 1:
        overall = "🟢 BULLISH"
        intensity = "Mild"
    elif net == 0 and any(s[1] == "T" for s in signals):
        overall = "⚠️ TRAP"
        intensity = "Caution"
    elif net == 0:
        overall = "🟡 MIXED"
        intensity = "Neutral"
    elif net >= -2:
        overall = "🔴 BEARISH"
        intensity = "Mild"
    elif net >= -4:
        overall = "🔴 BEARISH"
        intensity = "Strong"
    else:
        overall = "🔴 BEARISH"
        intensity = "Extreme"

    return {
        "date":          d_date,
        "moon_nak":      moon_nak,
        "moon_nak_lord": moon_nak_lord,
        "moon_rashi":    moon_rashi,
        "moon_d9":       moon_d9_rashi,
        "sun_rashi":     sun_rashi,
        "sun_nak":       sun_nak,
        "venus_rashi":   venus_rashi,
        "mars_rashi":    mars_rashi,
        "mars_nak":      mars_nak,
        "saturn_rashi":  saturn_rashi,
        "jupiter_rashi": jupiter_rashi,
        "mercury_rashi": mercury_rashi,
        "rahu_rashi":    rahu_rashi,
        "tithi":         f"{paksha} {tithi_name} ({tithi_num})",
        "moon_ketu_deg": round(moon_ketu, 1),
        "mars_rahu_deg": round(mars_rahu, 1),
        "sun_saturn_deg":round(sun_saturn, 1),
        "signals":       signals,
        "bearish_pts":   bearish_pts,
        "bullish_pts":   bullish_pts,
        "net_score":     net,
        "overall":       overall,
        "intensity":     intensity,
    }

EMAIL_FROM = "awslabuppala1985@gmail.com"
EMAIL_PASS = "uiipybranzfsgmxm"
EMAIL_TO = ["uppala.wla@gmail.com"]
EMAIL_ENABLED = True
EMAIL_MAX_PER_DAY = 40        # safe under Gmail limit
EMAIL_COOLDOWN_MIN = 10       # minutes between emails

EMAIL_META_FILE = "CACHE/email_meta.json"
EMAIL_DEDUP_FILE = "CACHE/email_dedup.csv"
ALERTS_DEDUP_FILE = "CACHE/alerts_dedup.csv"

if "alerts" not in st.session_state:
    st.session_state.alerts = []

if "alert_keys" not in st.session_state:
    st.session_state.alert_keys = set()




# ================= BASIC SETUP =================
st.set_page_config("Panchak Dashboard", layout="wide")
st.markdown("""
<style>
/* Section headers */
.section-green {background:#e8f5e9;padding:8px;border-radius:6px;}
.section-red {background:#fdecea;padding:8px;border-radius:6px;}
.section-yelLIVE_LOW {background:#fff8e1;padding:8px;border-radius:6px;}
.section-blue {background:#e3f2fd;padding:8px;border-radius:6px;}
.section-purple {background:#f3e5f5;padding:8px;border-radius:6px;}
.section-orange {background:#fff3e0;padding:8px;border-radius:6px;}

/* Table tweaks */
thead tr th {
    background-color:#f5f7fa !important;
    font-weight:600 !important;
}
</style>
""", unsafe_allow_html=True)


IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

#OHLC_FILE = os.path.join(CACHE_DIR, "ohlc_60d.csv")
INST_FILE    = os.path.join(CACHE_DIR, "instruments_NSE.csv")
# DAILY_FILE / WEEKLY_FILE / MONTHLY_FILE are dated files assigned below
# via get_daily_file() / get_weekly_file() / get_monthly_file()
# Do NOT pre-assign undated paths here — they would be stale on next-day restart
DAILY_FILE   = None
WEEKLY_FILE  = None
MONTHLY_FILE = None
#PANCHAK_FILE = os.path.join(CACHE_DIR, "panchak_static.csv")
#EMA_FILE = os.path.join(CACHE_DIR, "ema_20_50.csv")


API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN_FILE = "access_token.txt"

# ================= NSE HOLIDAYS – 2026 =================
NSE_HOLIDAYS = {
    date(2026,1,15), date(2026,1,26), date(2026,3,3),
    date(2026,3,26), date(2026,3,31), date(2026,4,3),
    date(2026,4,14), date(2026,5,1), date(2026,5,28),
    date(2026,6,26), date(2026,9,14), date(2026,10,2),
    date(2026,10,20), date(2026,11,10), date(2026,11,24),
    date(2026,12,25)
}

# ================= SYMBOL MASTER =================
SYMBOL_META = {
    "NIFTY":     "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
}

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA",
    "BANKINDIA","BANKNIFTY","BDL","BEL","BHEL","BHARATFORG","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BPCL","BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR","DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GODREJCP","GODREJPROP","GRASIM",
    "HAL","HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","HUDCO",
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
    "FINNIFTY": "FINNIFTY",
    "NIFTYIT": "NIFTY IT",
    "NIFTYFMCG": "NIFTY FMCG",
    "NIFTYPHARMA": "NIFTY PHARMA",
    "NIFTYMETAL": "NIFTY METAL",
    "NIFTYAUTO": "NIFTY AUTO",
    "NIFTYENERGY": "NIFTY ENERGY",
    "NIFTYPSUBANK": "NIFTY PSU BANK",
})

# ================= PANCHAK STATIC DATES =================
PANCHAK_DATES = [
    date(2026,3,17),
    date(2026,3,18),
    date(2026,3,19),
    date(2026,3,20),
    #date(2026,2,25),
]

# FEB 2026
#PANCHAK_DATES = [
 #   date(2026,2,17),
  #  date(2026,2,18),
   # date(2026,2,19),
    #date(2026,2,20),
    
#]

PANCHAK_START = date(2026, 3, 17)
PANCHAK_END   = date(2026, 3, 20)

PANCHAK_DATA_FILE = os.path.join(CACHE_DIR, "panchak_data.csv")
PANCHAK_META_FILE = os.path.join(CACHE_DIR, "panchak_meta.csv")
ALERTS_LOG_FILE = "CACHE/alerts_log.csv"



# ================= KITE INIT =================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(open(ACCESS_TOKEN_FILE).read().strip())


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
        return pd.read_csv(INST_FILE)
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def get_token(symbol):
    name = SYMBOL_META.get(symbol, symbol)
    row = inst[inst.tradingsymbol == name]
    return None if row.empty else int(row.iloc[0].instrument_token)

def kite_symbol(symbol):
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

    d = last_trading_day(date.today() - timedelta(days=1))
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(tk, d, d, "day")
        if not bars:
            continue

        b = bars[0]
        rows.append({
            "Symbol": s,
            "OPEN_D":  b["open"],
            "HIGH_D":  b["high"],
            "LOW_D":   b["low"],
            "CLOSE_D": b["close"],
            "VOLUME_D": b["volume"]
        })


        tm.sleep(0.25)

    pd.DataFrame(rows).to_csv(path, index=False)
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

    pd.DataFrame(rows).to_csv(path, index=False)
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

    pd.DataFrame(rows).to_csv(path, index=False)
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
        meta = pd.read_csv(PANCHAK_META_FILE)

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
    Builds Panchak DATA file + META file
    Called ONLY when cache is invalid
    """
    rows = []

    for s in SYMBOLS:
        tk = get_token(s)
        if not tk:
            continue

        bars = kite.historical_data(
            tk,
            PANCHAK_DATES[0],
            PANCHAK_DATES[-1],
            "day"
        )

        dfb = pd.DataFrame(bars)
        if dfb.empty:
            continue

        th = dfb["high"].max()
        tl = dfb["low"].min()
        diff = th - tl

        rows.append({
            "Symbol": s,
            "TOP_HIGH": round(th, 2),
            "TOP_LOW":  round(tl, 2),
            "DIFF":     round(diff, 2),
            "BT":       round(th + diff, 2),
            "ST":       round(tl - diff, 2),
        })

        tm.sleep(0.35)

    # 🔒 Safety: don’t write empty data
    if not rows:
        return

    # --- DATA FILE ---
    pd.DataFrame(rows).to_csv(PANCHAK_DATA_FILE, index=False)

    # --- META FILE ---
    meta_df = pd.DataFrame([
        {"key": "start_date", "value": PANCHAK_START.isoformat()},
        {"key": "end_date",   "value": PANCHAK_END.isoformat()},
    ])

    meta_df.to_csv(PANCHAK_META_FILE, index=False)


# ---------- PANCHAK LOAD (AUTO-VALIDATED) ----------
if not is_panchak_cache_valid():
    build_panchak_files()

@st.cache_data(show_spinner=False)   # ✅ FIX: static panchak data — cache permanently (no TTL)
def load_panchak_df():
    _df = pd.read_csv(PANCHAK_DATA_FILE)
    return (
        _df
        .sort_values("Symbol")
        .drop_duplicates(subset=["Symbol"], keep="last")
    )

panchak_df = load_panchak_df()





# ================= LIVE DATA =================
# ================= LIVE DATA (SAFE, HOLIDAY-AWARE) =================
@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent — stale data shown while refreshing
def live_data():
    try:
        quotes = kite.quote([kite_symbol(s) for s in SYMBOLS])
    except Exception as e:
        # ✅ FIX: Removed st.warning() — UI calls are NOT allowed inside @st.cache_data
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
    df["date"] = pd.to_datetime(df["date"])

    # Save to CACHE
    path = f"CACHE/{symbol}_{interval}.csv"
    df.to_csv(path, index=False)

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
            df = pd.read_csv(path)
            df["date"] = pd.to_datetime(df["date"])
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
# All intraday files are DATE-STAMPED → new file every trading day
# On restart, today's file is loaded if it exists — no stale data

_TODAY_STR = date.today().strftime("%Y%m%d")   # e.g. "20260327"

def _dated(name, ext="csv"):
    """Return CACHE/name_YYYYMMDD.ext — unique per trading day."""
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
OI_SNAPSHOT_FILE    = _dated("oi_snapshot_auto")   # moved here — used in status panel
OI_15M_SNAP_FILE    = _dated("oi_15m_snapshot", "json")  # 15-min OI snapshot for delta calc
OI_15M_DEDUP        = _dated("oi_15m_alert_dedup")       # dedup per strike+type per 15-min slot

# ── EMA7 intraday files (recomputed every 60s throughout the day) ─
EMA7_15M_FILE       = _dated("ema7_15min")   # EMA7 on 15-min candles — today's file
EMA7_1H_FILE        = _dated("ema7_1hour")   # EMA7 on 1-hour candles — today's file
FUT_15M_SNAP_FILE   = _dated("fut_15m_snapshot", "json") # 15-min futures OI snapshot
FUT_15M_DEDUP       = _dated("fut_15m_alert_dedup")      # dedup per symbol per 15-min slot

# ── Non-dated persistent files (span multiple days) ───────
# These are intentionally NOT date-stamped:
#   OHLC_FILE    → rolling 60-day history, appended daily
#   EMA_FILE     → computed from OHLC, rebuilt daily
#   ALERTS_LOG_FILE → accumulates across days
#   EMAIL_META_FILE / EMAIL_DEDUP_FILE / ALERTS_DEDUP_FILE → dedup state
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
    Load a dated CSV only if it belongs to today's date.
    Returns DataFrame or None.
    """
    if not os.path.exists(path):
        return None
    fname = os.path.basename(path)
    if _TODAY_STR not in fname:
        return None   # belongs to another day
    try:
        df_out = pd.read_csv(path)
        if required_cols:
            if not all(c in df_out.columns for c in required_cols):
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

# ================= LIVE DATA — SYNCHRONOUS EVERY RERUN =================
# kite.quote() = ONE batch API call ~1-2 sec. Fast enough for main thread.
# Must run every rerun so LTP, OHLC, alerts are always current.

def fetch_live_and_oi():
    """ONE kite.quote call → produces both live_df and oi_df. Saves both CSVs."""
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
        tk  = get_token(s)
        yo, yh, yl, yc, yv = fetch_yesterday_ohlc(tk)
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
            "YEST_LOW":    yl,  "YEST_CLOSE": yc,
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
        df_live.to_csv(LIVE_CACHE_CSV, index=False)
    if not df_oi.empty:
        df_oi.to_csv(OI_CACHE_CSV, index=False)
    return df_live, df_oi

# ── Single call produces both dataframes — no duplicate kite.quote ──
if replay_mode:
    base_df = load_replay_data(selected_date)
    _oi_prefetch = pd.DataFrame(columns=["Symbol","OI","OI_CHANGE_%"])
else:
    base_df, _oi_prefetch = fetch_live_and_oi()
    if base_df.empty:
        _fb = _load_today_csv(LIVE_CACHE_CSV)
        if _fb is not None:
            st.warning("⚠️ Live fetch failed — showing today's last saved data")
            base_df = _fb
        else:
            st.error("❌ Live fetch failed and no today's cache exists. Check Kite connection.")
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
    def _load_daily():   return pd.read_csv(DAILY_FILE)
    @st.cache_data(ttl=3600, show_spinner=False)
    def _load_weekly():  return pd.read_csv(WEEKLY_FILE)
    @st.cache_data(ttl=3600, show_spinner=False)
    def _load_monthly(): return pd.read_csv(MONTHLY_FILE)

    df = (
        base_df
        .merge(_load_daily(),   on="Symbol", how="left")
        .merge(_load_weekly(),  on="Symbol", how="left")
        .merge(_load_monthly(), on="Symbol", how="left")
        .merge(panchak_df, on="Symbol", how="left")
    )
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
    1️⃣ If file NOT present → build last 60 trading days
    2️⃣ If file present → append only latest trading day
    """

    today = date.today()
    end_day = last_trading_day(today)

    # ---------- CASE 1: FILE NOT EXISTS → FULL BUILD ----------
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
            except:
                continue

            dfb = pd.DataFrame(bars)
            if dfb.empty:
                continue

            dfb["date"] = pd.to_datetime(dfb["date"]).dt.date
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
            pd.DataFrame(rows).to_csv(OHLC_FILE, index=False)

        return

    # ---------- CASE 2: FILE EXISTS → DAILY APPEND ----------
    ohlc_df = pd.read_csv(OHLC_FILE)
    ohlc_df["date"] = pd.to_datetime(ohlc_df["date"]).dt.date

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
        except:
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

    final_df.to_csv(OHLC_FILE, index=False)


@st.cache_data(ttl=3600, show_spinner=False)   # ✅ silent
def build_ema_from_ohlc():
    if not os.path.exists(OHLC_FILE):
        return pd.DataFrame(columns=["Symbol","EMA20","EMA50"])

    df = pd.read_csv(OHLC_FILE)

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
    ema_df.to_csv(EMA_FILE, index=False)
    return ema_df


# ✅ FIX: Guard ohlc_60d update with session_state — runs once per app session, not every rerun
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
# The merge happens in a safe late-stage block — see "EMA7 INTRADAY MERGE" below.

# =========================================================
# MULTI-PERIOD DOWNTREND → EMA REVERSAL SCREENER
# =========================================================

@st.cache_data(ttl=3600, show_spinner=False)   # ✅ silent
def load_ohlc_full():
    _df = pd.read_csv(OHLC_FILE)
    _df["date"] = pd.to_datetime(_df["date"])
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
# MULTI-PERIOD DOWNTREND → EMA REVERSAL SCREENER
# =========================================================

#ohlc_full = pd.read_csv(OHLC_FILE)
#ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

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
# MULTI-PERIOD DOWNTREND → EMA REVERSAL AFTER UPTREND
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

now = datetime.now(IST).time()

market_start = time(9, 0)
market_end   = time(15, 30)

st.caption(
    f"Last refresh: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')}"
)

# ✅ FIX: Auto refresh only during market hours — interval matches live_data TTL
if market_start <= now <= market_end:
    st_autorefresh(interval=100 * 1000, key="refresh", debounce=False)  # show_spinner handled by cache functions   # 100 seconds
    st.caption("🔄 Auto refresh active every 100s (Market Hours)")
else:
    st.caption("⏸ Auto refresh paused (Market Closed)")


# =========================================================
# 💾 DATA STORAGE STATUS — confirms every file is from TODAY
# =========================================================
def _file_status(label, path):
    """Return status dict for a dated cache file."""
    exists   = os.path.exists(path)
    is_today = _TODAY_STR in os.path.basename(path)
    if not exists:
        return {"label": label, "status": "❌ Missing", "size": "—", "age": "—", "file": os.path.basename(path)}
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
        _status_df.style.map(_color_status, subset=["status"]),
        width='stretch',
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
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

panchak_view = df[PANCHAK_COLUMNS]

TOP_HIGH_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
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

TOP_LOW_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "GAIN",
    "LTP",
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

NEAR_COLUMNS = [
    "Symbol",
    "TOP_HIGH",
    "TOP_LOW",
    "DIFF",
    "BT",
    "ST",
    "NEAR",
    "LTP",
    "LIVE_HIGH",
    "LIVE_LOW",
    "CHANGE",
    "CHANGE_%",
    "YEST_HIGH",
    "YEST_LOW"
]

NEAR_view = df[NEAR_COLUMNS]

DAILY_COLUMNS = [
    "Symbol",
    "LTP",
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

WEEKLY_COLUMNS = [
    "Symbol",
    "LTP",
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

MONTHLY_COLUMNS = [
    "Symbol",
    "LTP",
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




def is_market_hours():
    now_dt = datetime.now(IST)
    now = now_dt.time()

    if now_dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False

    return time(9,15) <= now <= time(15,30)


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
    if not symbols:
        return
    st.toast(f"{title}: {', '.join(symbols)}", icon="🚨")

def detect_new_entries(name, current_symbols):
    path = f"CACHE/{name}_prev.txt"

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

notify_browser("🟢 New TOP LIVE_HIGH", new_TOP_HIGH)

new_TOP_LOW = detect_new_entries(
    "TOP_LOW",
    TOP_LOW_df.Symbol.tolist()
)

notify_browser("🔴 New TOP LIVE_LOW", new_TOP_LOW)

def can_send_email():
    if not EMAIL_ENABLED:
        return False

    os.makedirs("CACHE", exist_ok=True)

    now = datetime.now()

    data = {
        "last_sent": None,
        "count_today": 0,
        "date": now.strftime("%Y-%m-%d")
    }

    if os.path.exists(EMAIL_META_FILE):
        with open(EMAIL_META_FILE) as f:
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
        with open(EMAIL_META_FILE) as f:
            data = json.load(f)

    data["count_today"] += 1
    data["last_sent"] = now.isoformat()

    with open(EMAIL_META_FILE, "w") as f:
        json.dump(data, f)
# ✅ FIX: Removed duplicate can_send_email / record_email_sent definitions (were defined twice)
EMAIL_ENABLED = True  # 🔁 set False to fully disable emails

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

    # Still-active old entries (grey header — entries alerted earlier today)
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
            📋 Still Active — Earlier Entries (Today)
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
    """Send email — supports both plain text (legacy) and HTML."""
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
    os.makedirs("CACHE", exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(EMAIL_DEDUP_FILE):
        return False

    df = pd.read_csv(EMAIL_DEDUP_FILE)

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
        df = pd.read_csv(EMAIL_DEDUP_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(EMAIL_DEDUP_FILE, index=False)




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
        df = pd.read_csv(ALERTS_LOG_FILE)
        df = pd.concat([pd.DataFrame([row]), df], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_LOG_FILE, index=False)

    # Mark this alert as logged
    mark_alert_logged(symbol, category)


def alert_already_logged(symbol, category):
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(ALERTS_DEDUP_FILE):
        return False

    df = pd.read_csv(ALERTS_DEDUP_FILE)

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
        df = pd.read_csv(ALERTS_DEDUP_FILE)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(ALERTS_DEDUP_FILE, index=False)

####################################################################################

# ─────────────────────────────────────────────────────────────────────────────
# 📧 CATEGORY → DATAFRAME MAP
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
DISABLED_EMAIL_CATEGORIES = {
    # User-disabled (email off)
    "TOP_HIGH",       # 1
    "TOP_LOW",        # 2
    "DAILY_UP",       # 3
    "DAILY_DOWN",     # 4
    "OPTION_STRONG",  # 16
    # Previously disabled
    "EMA20_50",
    "THREE_GREEN_15M",
}

DISABLED_TOAST_CATEGORIES = {
    # User-disabled (toast off)
    "TOP_HIGH",       # 1
    "TOP_LOW",        # 2
    "DAILY_UP",       # 3
    "DAILY_DOWN",     # 4
    "OPTION_STRONG",  # 16
    "EMA20_50",       # 17
    "THREE_GREEN_15M",# 18
}

# Color and icon per category
_CAT_STYLE = {
    "TOP_HIGH":         ("#1b5e20", "🟢"),
    "TOP_LOW":          ("#b71c1c", "🔴"),
    "EMA20_50":         ("#1565c0", "⚡"),
    "DAILY_UP":         ("#2e7d32", "📈"),
    "DAILY_DOWN":       ("#c62828", "📉"),
    "WEEKLY_UP":        ("#1b5e20", "📊"),
    "WEEKLY_DOWN":      ("#b71c1c", "📉"),
    "MONTHLY_UP":       ("#1b5e20", "📅"),
    "MONTHLY_DOWN":     ("#b71c1c", "📉"),
    "TOP_GAINERS":      ("#e65100", "🔥"),
    "TOP_LOSERS":       ("#880e4f", "🔥"),
    "THREE_GREEN_15M":  ("#1b5e20", "🟢"),
    "YEST_GREEN_BREAK": ("#2e7d32", "🟢"),
    "YEST_RED_BREAK":   ("#c62828", "🔴"),
    "HOURLY_BREAK_UP":  ("#1b5e20", "🕐"),
    "HOURLY_BREAK_DOWN":("#b71c1c", "🕐"),
    "2M_EMA_REVERSAL":  ("#4a148c", "🚀"),
    "OPTION_STRONG":    ("#e65100", "🔥"),
    "VOL_SURGE_15M":    ("#0277bd", "📊"),
}

_CORE_COLS = ["Symbol","LTP","CHANGE","CHANGE_%"]

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
# 🔔 MASTER ALERT ENGINE (Market Time Protected) — HTML
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
    if category in DISABLED_EMAIL_CATEGORIES:
        email_symbols = []   # suppress email; toast already fired below

    if email_symbols and is_email_allowed():
        now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
        subject  = f"[OiAnalytics] {title} — {len(email_symbols)} New | {now_str}"

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
    if category not in DISABLED_TOAST_CATEGORIES:
        st.toast(f"{icon} {title}: {', '.join(symbols)}", icon="🚨")




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
# TOP GAINERS / LOSERS — COLUMN ORDER FIX
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
# SUPERTREND (DAILY) + VWAP  — CLEAN VERSION
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
# VWAP (INTRADAY APPROX)
# -----------------------------
def compute_vwap(row):
    price = (row["LIVE_HIGH"] + row["LIVE_LOW"] + row["LTP"]) / 3
    return round(price, 2)

df["VWAP"] = df.apply(compute_vwap, axis=1)
df["VWAP_DIFF"] = (df["LTP"] - df["VWAP"]).round(2)


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
# SUPERTREND — PREPARE DATAFRAME + STYLER (MUST BE BEFORE TABS)
# =========================================================

def highlight_supertrend(row):
    styles = []
    for col in row.index:
        if col == "ST_BUY" and row[col] == "BUY":
            styles.append("background-color:#d4f8d4;color:#006400;font-weight:bold;")
        elif col == "ST_SELL" and row[col] == "SELL":
            styles.append("background-color:#ffd6d6;color:#8b0000;font-weight:bold;")
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

df["STRIKE_PREF"] = df.apply(recommend_strike, axis=1)

def option_verdict(row):
    if row["OPTION_SCORE"] >= 3:
        if row["EMA20"] > row["EMA50"]:
            return "STRONG CE BUY"
        else:
            return "STRONG PE BUY"
    return "AVOID"

df["OPTION_SIGNAL"] = df.apply(option_verdict, axis=1)

#ALERT FILTER (NO MORE EMAIL FLOOD)
STRONG_BUY_DF = df[df["OPTION_SIGNAL"].str.contains("STRONG")]
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


# =========================================================
# LIVE MAP (Symbol → Live Values) for quick access
# =========================================================
live_map = {}

required_cols = [
    "Symbol", "LTP", "LIVE_HIGH", "LIVE_LOW",
    "YEST_HIGH", "YEST_LOW", "CHANGE", "CHANGE_%"
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

ohlc_full["date"] = pd.to_datetime(ohlc_full["date"])

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
        return ["background-color:#e8f5e9"] * len(row)
    else:
        return ["background-color:#fff3e0"] * len(row)

def style_bear_trap(_):
    return ["background-color:#fff3e0"] * len(fake_bear_df.columns)

def style_ltp_only(row):
    styles = [""] * len(row)

    ltp_idx = row.index.get_loc("LTP")

    if row["LTP"] >= row["YEST_CLOSE"]:
        styles[ltp_idx] = "background-color:#e8f5e9"  # light green
    else:
        styles[ltp_idx] = "background-color:#fff3e0"  # light orange

    return styles

def style_ltp_bear_only(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")
    styles[ltp_idx] = "background-color:#fff3e0"
    return styles
############ for 15 mins table style
def style_ltp_15min(row):
    styles = [""] * len(row)
    ltp_idx = row.index.get_loc("LTP")

    if row["BREAK_TYPE"] == "UP":
        styles[ltp_idx] = "background-color:#e8f5e9; color:#1b5e20"  # green
    elif row["BREAK_TYPE"] == "DOWN":
        styles[ltp_idx] = "background-color:#ffebee; color:#b71c1c"  # red

    return styles







############    OPTION 2 (Fallback): Build 15-min from 5-min candles    ================
# ✅ FIX: Run in background thread — never blocks Streamlit main thread
# Dashboard always shows last known data instantly; background thread updates silently.

_5min_EMPTY = pd.DataFrame(columns=["Symbol","datetime","open","high","low","close"])

def _fetch_5min_background():
    """Runs in a daemon thread — fetches all 5min candles without blocking UI."""
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
        pd.DataFrame(rows).to_csv(FIVE_MIN_CACHE_CSV, index=False)

# Start background fetch if: never fetched, or TTL of 95s has elapsed
if not _csv_is_fresh(FIVE_MIN_CACHE_CSV) and not st.session_state.get("_five_df_thread_running", False):
    st.session_state["_five_df_thread_running"] = True
    def _run_and_clear():
        _fetch_5min_background()
        st.session_state["_five_df_thread_running"] = False
    t = threading.Thread(target=_run_and_clear, daemon=True)
    t.start()

# Always use last known data — never wait for the thread
# Use _load_today_csv so we never accidentally load yesterday's candles
_five_today = _load_today_csv(FIVE_MIN_CACHE_CSV)
five_df = _five_today if _five_today is not None else _5min_EMPTY
if "datetime" in five_df.columns:
    five_df["datetime"] = pd.to_datetime(five_df["datetime"])

# ================= SAFETY: 5-MIN DATA AVAILABILITY =================
if five_df.empty or "datetime" not in five_df.columns:
    intraday_15m_df = pd.DataFrame(columns=[
        "Symbol", "datetime", "open", "high", "low", "close"
    ])
else:
    intraday_15m_df = (
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
        .dropna()
        .reset_index()
    )




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
    if ltp > first["high"]:
        break_type = "UP"
        chg_15m_pct = round(
            ((ltp - first["high"]) / first["high"]) * 100, 2
        )

    elif ltp < first["low"]:
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
        "BREAK_TYPE": break_type
    })

inside_15m_df = pd.DataFrame(inside_break_rows)


# =========================================================
# YH1.5 STRONG BREAKOUT (SCREENER LOGIC) — FINAL & CORRECT
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

notify_browser("🚀 YH 1.5 STRONG BREAKOUT", new_YH15)

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

notify_browser("🟢 4-BAR REVERSAL BUY", new_4BAR_BUY)


def load_index_symbols(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line.strip()]

INDEX_FILES = {
    "NIFTY 50": "NIFTY 50.txt",
    "BANK NIFTY": "BANK NIFTY.txt",
    "FINNIFTY": "FINNIFTY.txt",
    "NIFTY IT": "NIFTY IT.txt",
    "NIFTY FMCG": "NIFTY FMCG.txt",
    "NIFTY PHARMA": "NIFTY PHARMA.txt",
    "NIFTY METAL": "NIFTY METAL.txt",
    "NIFTY AUTO": "NIFTY AUTO.txt",
    "NIFTY ENERGY": "NIFTY ENERGY.txt",
    "NIFTY PSU BANK": "NIFTY PSU BANK.txt",
}

index_symbols = {
    name: load_index_symbols(file)
    for name, file in INDEX_FILES.items()
}

# =========================================================
# INDICES – LIVE OHLC (SINGLE SOURCE OF TRUTH)
# =========================================================

INDEX_SYMBOLS = {
    "NIFTY 50": "NIFTY",
    "BANK NIFTY": "BANKNIFTY",
    "FINNIFTY": "FINNIFTY",
    "NIFTY IT": "NIFTYIT",
    "NIFTY FMCG": "NIFTYFMCG",
    "NIFTY PHARMA": "NIFTYPHARMA",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY AUTO": "NIFTYAUTO",
    "NIFTY ENERGY": "NIFTYENERGY",
    "NIFTY PSU BANK": "NIFTYPSUBANK",
}

@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent — no UI blur on refresh
def fetch_indices_live():
    _index_rows = []
    # Batch quote all indices in ONE call instead of one per symbol
    index_kite_symbols = {name: f"NSE:{sym}" for name, sym in INDEX_SYMBOLS.items()}
    all_keys = list(index_kite_symbols.values())
    try:
        all_quotes = kite.quote(all_keys)
    except Exception as e:
        print("INDEX BATCH QUOTE ERROR:", e)
        all_quotes = {}

    for index_name, sym in INDEX_SYMBOLS.items():
        kite_key = f"NSE:{sym}"
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
            tk = get_token(sym)
            _, yh, yl, yc, _ = fetch_yesterday_ohlc(tk)   # unpack all 5 values correctly
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
                "YEST_CLOSE": yc,
            })
        except Exception as e:
            print("INDEX ERROR:", index_name, e)
    return pd.DataFrame(_index_rows)

# ✅ INDICES — synchronous single batch call every rerun (~0.5 sec)
def fetch_indices_now():
    _index_rows = []
    all_keys = [f"NSE:{sym}" for sym in INDEX_SYMBOLS.values()]
    try:
        all_quotes = kite.quote(all_keys)
    except Exception:
        return pd.DataFrame()
    for index_name, sym in INDEX_SYMBOLS.items():
        try:
            q = all_quotes.get(f"NSE:{sym}")
            if not q: continue
            o=q["ohlc"]["open"]; h=q["ohlc"]["high"]; l=q["ohlc"]["low"]; pc=q["ohlc"]["close"]
            ltp=q["last_price"]; chg=ltp-pc if pc else 0; chg_pct=(chg/pc*100) if pc else 0
            tk=get_token(sym); _, yh, yl, yc, _ = fetch_yesterday_ohlc(tk)
            _index_rows.append({"Index":index_name,"OPEN":round(o,2),"HIGH":round(h,2),
                "LOW":round(l,2),"LTP":round(ltp,2),"CHANGE":round(chg,2),
                "CHANGE_%":round(chg_pct,2),"YEST_HIGH":yh,"YEST_LOW":yl,"YEST_CLOSE":yc})
        except Exception:
            pass
    result = pd.DataFrame(_index_rows)
    if not result.empty:
        result.to_csv(INDICES_CACHE_CSV, index=False)
    return result

indices_df = fetch_indices_now()
if indices_df.empty:
    _fb = _load_today_csv(INDICES_CACHE_CSV)
    if _fb is not None:
        indices_df = _fb

# ================= INDICES TAB (FIXED) =================

INDEX_ONLY_SYMBOLS = [
    "NIFTY",
    "BANKNIFTY",
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
# Only replace if live fetch returned nothing — df-based version is always fresh
if indices_df.empty and not _indices_from_df.empty:
    indices_df = _indices_from_df
elif not _indices_from_df.empty:
    # Merge: use fetched YEST_* columns from live fetch, rest from df (more complete)
    indices_df = _indices_from_df.copy()


# 🔒 SAFETY NET – ENSURE ALL REQUIRED COLUMNS EXIST
# Ensure minimum columns exist (safe for both Index and Symbol naming)
for c in ["LTP","CHANGE","CHANGE_%","YEST_HIGH","YEST_LOW","YEST_CLOSE"]:
    if c not in indices_df.columns:
        indices_df[c] = None


indices_df = indices_df.sort_values("CHANGE_%", ascending=False)

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
    path = f"CACHE/{name}_15m_prev.txt"

    prev = set(open(path, encoding='utf-8').read().split(",")) if os.path.exists(path) else set()
    curr = set(f"{r['Symbol']}|{r['CANDLE_TIME']}" for r in rows)

    new = curr - prev

    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(curr))

    return [x.split("|")[0] for x in new]

three_green_15m_df = pd.DataFrame(three_green_rows)

# =========================================================
# 🔔 LIVE ALERT — 3rd 15-min GREEN candle completed
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


############                SCENARIO 1 — Yesterday GREEN candle, tight body near high
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


#########       SCENARIO 2 — Yesterday RED candle, tight body near low
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
                (pd.to_datetime(inst["expiry"]).dt.date == expiry)
            ]

            if row.empty:
                continue

            ts = row.iloc[0]["tradingsymbol"]
            token = int(row.iloc[0]["instrument_token"])

            try:
                q = kite.quote([f"NFO:{ts}"])[f"NFO:{ts}"]
            except:
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
        pd.DataFrame(rows).to_csv(path, index=False)


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




############        STEP 4: OPTIONS TAB — STOCKS + INDICES
def load_option_csv(symbol):
    for folder in ["INDEX", "STOCK"]:
        path = f"{CACHE_DIR}/OPTIONS/{folder}/{symbol}.csv"
        if os.path.exists(path):
            return pd.read_csv(path)
    return pd.DataFrame()


# =========================================================
# 🧠 NIFTY OI INTELLIGENCE — FULL OPTION CHAIN ANALYSIS
# Fetches NIFTY option chain: ATM ±10 strikes
# Cached every 5 min, returns structured dict for display
# =========================================================

OI_INTEL_CACHE_PATH = OI_INTEL_CACHE   # use module-level path

@st.cache_data(ttl=300, show_spinner=False)   # 5-min cache
def fetch_nifty_oi_intelligence():
    """
    Fetches NIFTY option chain (ATM ±10 strikes, current monthly expiry).
    Returns dict with:
      - spot, futures_ltp, atm, timestamp
      - max_pain
      - strongest_call_strike, strongest_put_strike
      - shifting_oi_call (OI gaining CE strike), shifting_oi_put (OI gaining PE strike)
      - market_direction
      - pcr (put-call ratio)
      - chain_df (full chain as list of dicts)
    """
    import json

    try:
        # ── 1. NIFTY Spot ──────────────────────────────────────
        nifty_quote = kite.quote(["NSE:NIFTY 50"])
        spot = nifty_quote["NSE:NIFTY 50"]["last_price"]

        # ── 2. NIFTY Futures (nearest monthly) ─────────────────
        try:
            nfo_inst = pd.DataFrame(kite.instruments("NFO"))
            nifty_fut = nfo_inst[
                (nfo_inst["name"] == "NIFTY") &
                (nfo_inst["instrument_type"] == "FUT")
            ].copy()
            nifty_fut["expiry_dt"] = pd.to_datetime(nifty_fut["expiry"])
            today_dt = pd.Timestamp.now()
            nifty_fut = nifty_fut[nifty_fut["expiry_dt"] >= today_dt].sort_values("expiry_dt")
            fut_ltp = 0.0
            if not nifty_fut.empty:
                fut_ts = nifty_fut.iloc[0]["tradingsymbol"]
                fq = kite.quote([f"NFO:{fut_ts}"])
                fut_ltp = fq[f"NFO:{fut_ts}"]["last_price"]
        except Exception:
            fut_ltp = spot   # fallback

        # ── 3. ATM + strikes ───────────────────────────────────
        step = 50
        atm = int(round(spot / step) * step)
        strikes = [atm + i * step for i in range(-10, 11)]

        # ── 4. NFO instruments already loaded in step 2; no reload needed ──

        # ── 5. Get nearest WEEKLY expiry ───────────────────────
        nifty_opts = nfo_inst[
            (nfo_inst["name"] == "NIFTY") &
            (nfo_inst["instrument_type"].isin(["CE", "PE"]))
        ].copy()
        nifty_opts["expiry_dt"] = pd.to_datetime(nifty_opts["expiry"])
        today_dt = pd.Timestamp.now()
        future_expiries = nifty_opts[nifty_opts["expiry_dt"] >= today_dt]["expiry_dt"].unique()
        if len(future_expiries) == 0:
            return None
        expiry = sorted(future_expiries)[0]   # nearest expiry

        # ── 6. Build token list for batch quote ────────────────
        token_map = {}   # tradingsymbol → {strike, type}
        for strike in strikes:
            for opt_type in ["CE", "PE"]:
                row = nifty_opts[
                    (nifty_opts["strike"] == strike) &
                    (nifty_opts["instrument_type"] == opt_type) &
                    (nifty_opts["expiry_dt"] == expiry)
                ]
                if not row.empty:
                    ts = row.iloc[0]["tradingsymbol"]
                    token_map[f"NFO:{ts}"] = {"strike": strike, "type": opt_type}

        if not token_map:
            return None

        # ── 7. Batch quote ──────────────────────────────────────
        quotes_raw = kite.quote(list(token_map.keys()))

        # ── 8. Build chain ──────────────────────────────────────
        chain = {}   # strike → {CE: {...}, PE: {...}}
        for symbol, meta in token_map.items():
            q = quotes_raw.get(symbol)
            if not q:
                continue
            strike = meta["strike"]
            opt_type = meta["type"]
            if strike not in chain:
                chain[strike] = {}
            chain[strike][opt_type] = {
                "ltp":     q.get("last_price", 0),
                "oi":      q.get("oi", 0) or 0,
                "oi_high": q.get("oi_day_high", 0) or 0,
                "oi_low":  q.get("oi_day_low", 0) or 0,
                "volume":  q.get("volume", 0) or 0,
                "iv":      q.get("implied_volatility", 0) or 0,
            }

        if not chain:
            return None

        # ── 9. Max Pain ─────────────────────────────────────────
        pain_vals = {}
        for pain_strike in strikes:
            total = 0
            for s, data in chain.items():
                ce_oi = data.get("CE", {}).get("oi", 0)
                pe_oi = data.get("PE", {}).get("oi", 0)
                if s < pain_strike:
                    total += ce_oi * (pain_strike - s)
                elif s > pain_strike:
                    total += pe_oi * (s - pain_strike)
            pain_vals[pain_strike] = total
        max_pain = min(pain_vals, key=pain_vals.get) if pain_vals else atm

        # ── 10. Strongest CE / PE strike (highest OI) ──────────
        ce_oi_map = {s: chain[s].get("CE", {}).get("oi", 0) for s in chain}
        pe_oi_map = {s: chain[s].get("PE", {}).get("oi", 0) for s in chain}

        strongest_ce = max(ce_oi_map, key=ce_oi_map.get) if ce_oi_map else atm
        strongest_pe = max(pe_oi_map, key=pe_oi_map.get) if pe_oi_map else atm

        # LTP of strongest strikes
        strongest_ce_ltp = chain.get(strongest_ce, {}).get("CE", {}).get("ltp", 0)
        strongest_pe_ltp = chain.get(strongest_pe, {}).get("PE", {}).get("ltp", 0)
        # Day-high premium of strongest strikes (for premium change calc)
        strongest_ce_ltp_high = chain.get(strongest_ce, {}).get("CE", {}).get("oi_high", 0)
        strongest_pe_ltp_high = chain.get(strongest_pe, {}).get("PE", {}).get("oi_high", 0)

        # ── 11. Shifting OI (OI added today = oi - oi_low) ─────
        ce_added = {s: chain[s].get("CE", {}).get("oi", 0) - chain[s].get("CE", {}).get("oi_low", 0)
                    for s in chain}
        pe_added = {s: chain[s].get("PE", {}).get("oi", 0) - chain[s].get("PE", {}).get("oi_low", 0)
                    for s in chain}

        shifting_ce = max(ce_added, key=ce_added.get) if ce_added else atm
        shifting_pe = max(pe_added, key=pe_added.get) if pe_added else atm

        # OI % added vs total for shifting strikes
        shifting_ce_oi_total = chain.get(shifting_ce, {}).get("CE", {}).get("oi", 0)
        shifting_pe_oi_total = chain.get(shifting_pe, {}).get("PE", {}).get("oi", 0)
        shifting_ce_pct = round((ce_added.get(shifting_ce, 0) / shifting_ce_oi_total * 100)
                                 if shifting_ce_oi_total > 0 else 0, 1)
        shifting_pe_pct = round((pe_added.get(shifting_pe, 0) / shifting_pe_oi_total * 100)
                                 if shifting_pe_oi_total > 0 else 0, 1)
        shifting_ce_ltp = chain.get(shifting_ce, {}).get("CE", {}).get("ltp", 0)
        shifting_pe_ltp = chain.get(shifting_pe, {}).get("PE", {}).get("ltp", 0)
        # Opening premium = oi_low proxy (ltp at open)
        shifting_ce_ltp_open = chain.get(shifting_ce, {}).get("CE", {}).get("oi_high", 0)  # day high as open-proxy
        shifting_pe_ltp_open = chain.get(shifting_pe, {}).get("PE", {}).get("oi_high", 0)

        # ── 11b. PE OI drop — find biggest PE OI decrease ───────
        pe_dropped = {s: chain[s].get("PE", {}).get("oi", 0) - chain[s].get("PE", {}).get("oi_high", 0)
                      for s in chain}  # negative = dropped
        most_dropped_pe = min(pe_dropped, key=pe_dropped.get) if pe_dropped else atm
        most_dropped_pe_val   = pe_dropped.get(most_dropped_pe, 0)
        most_dropped_pe_oi    = chain.get(most_dropped_pe, {}).get("PE", {}).get("oi", 0)
        most_dropped_pe_pct   = round((most_dropped_pe_val / most_dropped_pe_oi * 100)
                                       if most_dropped_pe_oi > 0 else 0, 1)
        most_dropped_pe_ltp   = chain.get(most_dropped_pe, {}).get("PE", {}).get("ltp", 0)
        most_dropped_pe_high  = chain.get(most_dropped_pe, {}).get("PE", {}).get("oi_high", 0)

        # CE OI drop
        ce_dropped = {s: chain[s].get("CE", {}).get("oi", 0) - chain[s].get("CE", {}).get("oi_high", 0)
                      for s in chain}
        most_dropped_ce = min(ce_dropped, key=ce_dropped.get) if ce_dropped else atm

        # ── 11c. Near-ATM OI shift summary (for pattern) ────────
        near_atm_strikes = [s for s in chain if abs(s - atm) <= step * 4]  # ATM ±200 pts = broader pressure zone
        near_ce_oi     = sum(chain[s].get("CE", {}).get("oi", 0) for s in near_atm_strikes)
        near_ce_added  = sum(ce_added.get(s, 0) for s in near_atm_strikes)
        near_pe_oi     = sum(chain[s].get("PE", {}).get("oi", 0) for s in near_atm_strikes)
        near_pe_added  = sum(pe_added.get(s, 0) for s in near_atm_strikes)
        near_pe_dropped = sum(pe_dropped.get(s, 0) for s in near_atm_strikes)   # sum of drops (negative)

        near_ce_pct  = round((near_ce_added  / near_ce_oi  * 100) if near_ce_oi  > 0 else 0, 1)
        near_pe_pct  = round((near_pe_added  / near_pe_oi  * 100) if near_pe_oi  > 0 else 0, 1)
        near_pe_drop_pct = round((near_pe_dropped / near_pe_oi * 100) if near_pe_oi > 0 else 0, 1)

        # ── 12. PCR & market direction ──────────────────────────
        total_ce_oi = sum(ce_oi_map.values())
        total_pe_oi = sum(pe_oi_map.values())
        pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0

        # Walls and floors
        call_walls_above_list = [s for s in ce_oi_map if s > spot]
        put_floors_below_list = [s for s in pe_oi_map if s < spot]

        nearest_call_wall = min(call_walls_above_list, key=lambda x: x - spot) if call_walls_above_list else None
        nearest_put_floor = min(put_floors_below_list, key=lambda x: spot - x) if put_floors_below_list else None

        dist_to_call = round(nearest_call_wall - spot, 0) if nearest_call_wall else 9999
        dist_to_put  = round(spot - nearest_put_floor, 0) if nearest_put_floor else 9999

        # ── OI shift signals (for direction override) ──────────
        _near_strikes   = [s for s in chain if abs(s - atm) <= step * 4]
        _near_ce_oi     = sum(chain[s].get("CE", {}).get("oi", 0) for s in _near_strikes) or 1
        _near_pe_oi     = sum(chain[s].get("PE", {}).get("oi", 0) for s in _near_strikes) or 1
        _near_ce_added  = sum(ce_added.get(s, 0) for s in _near_strikes)
        _near_pe_added  = sum(pe_added.get(s, 0) for s in _near_strikes)
        _near_pe_drop   = sum(
            chain[s].get("PE", {}).get("oi", 0) - chain[s].get("PE", {}).get("oi_high", 0)
            for s in _near_strikes
        )
        _ce_build_pct   = round(_near_ce_added / _near_ce_oi * 100, 1)
        _pe_drop_pct    = round(_near_pe_drop   / _near_pe_oi * 100, 1)
        _pe_build_pct   = round(_near_pe_added  / _near_pe_oi * 100, 1)
        _ce_drop_pct    = round(
            sum(chain[s].get("CE",{}).get("oi",0) - chain[s].get("CE",{}).get("oi_high",0)
                for s in _near_strikes) / _near_ce_oi * 100, 1
        )

        # OI pattern flags
        _ce_building  = _ce_build_pct >= 5
        _pe_crumbling = _pe_drop_pct  <= -3
        _pe_building  = _pe_build_pct >= 5
        _ce_crumbling = _ce_drop_pct  <= -3

        # ── Unified direction — OI pattern takes priority over PCR ──
        if _ce_building and _pe_crumbling:
            direction        = "🔴 BEARISH"
            direction_reason = f"WATERFALL: CE writing (+{_ce_build_pct}%) + PE unwinding ({_pe_drop_pct}%)"
        elif _pe_building and _ce_crumbling:
            direction        = "🟢 BULLISH"
            direction_reason = f"SHORT SQUEEZE: PE writing (+{_pe_build_pct}%) + CE unwinding ({_ce_drop_pct}%)"
        elif _ce_building and not _pe_crumbling:
            direction        = "🔴 BEARISH BIAS"
            direction_reason = f"Call wall building (+{_ce_build_pct}% OI), put support intact"
        elif _pe_building and not _ce_crumbling:
            direction        = "🟢 BULLISH BIAS"
            direction_reason = f"Put floor building (+{_pe_build_pct}% OI), call resistance intact"
        elif _ce_crumbling and not _pe_building:
            direction        = "🟢 BULLISH"
            direction_reason = f"Call wall unwinding ({_ce_drop_pct}%) — resistance weakening"
        elif _pe_crumbling and not _ce_building:
            direction        = "🔴 BEARISH"
            direction_reason = f"Put floor unwinding ({_pe_drop_pct}%) — support weakening"
        elif pcr >= 1.3:
            direction        = "🟢 BULLISH"
            direction_reason = f"PCR={pcr} (Strong put support, more puts written)"
        elif pcr <= 0.7:
            direction        = "🔴 BEARISH"
            direction_reason = f"PCR={pcr} (Strong call pressure, more calls written)"
        elif dist_to_call < dist_to_put and nearest_call_wall:
            direction        = "🔴 BEARISH BIAS"
            direction_reason = f"Nearest call wall ({nearest_call_wall}) only {int(dist_to_call)} pts away"
        elif dist_to_put < dist_to_call and nearest_put_floor:
            direction        = "🟢 BULLISH BIAS"
            direction_reason = f"Nearest put floor ({nearest_put_floor}) only {int(dist_to_put)} pts away"
        else:
            direction        = "⚠️ SIDEWAYS/NEUTRAL"
            direction_reason = f"PCR={pcr}, balanced OI, no dominant shift"

        # Spot vs Max Pain
        spot_vs_pain = spot - max_pain
        if abs(spot_vs_pain) < 50:
            pain_signal = f"AT MAX PAIN ({max_pain})"
        elif spot_vs_pain > 0:
            pain_signal = f"Spot ↑ {round(spot_vs_pain)} pts above Max Pain ({max_pain})"
        else:
            pain_signal = f"Spot ↓ {round(abs(spot_vs_pain))} pts below Max Pain ({max_pain})"

        # ── 13. Advice (derived from unified direction) ─────────
        if "BEARISH" in direction and "BIAS" not in direction:
            advice = "⚠️ Strong BEARISH setup. Do NOT buy call options here."
            setup  = f"Buy {atm} PE | SL above {nearest_call_wall or strongest_ce}"
        elif "BULLISH" in direction and "BIAS" not in direction:
            advice = "✅ Strong BULLISH setup. Do NOT sell calls aggressively."
            setup  = f"Buy {atm} CE | SL below {nearest_put_floor or strongest_pe}"
        elif "BEARISH BIAS" in direction:
            advice = "Mild BEARISH. Sell rallies near call wall, avoid aggressive longs."
            setup  = f"Sell/Hedge near {nearest_call_wall or strongest_ce} CE wall"
        elif "BULLISH BIAS" in direction:
            advice = "Mild BULLISH. Buy dips near put floor, avoid aggressive shorts."
            setup  = f"Buy dips near {nearest_put_floor or strongest_pe} PE floor"
        else:
            advice = "Range-bound. Trade breakouts only. Avoid directional bets."
            setup  = f"Wait: breakout above {nearest_call_wall or strongest_ce} OR below {nearest_put_floor or strongest_pe}"

        # ── 14. Build chain_rows for display ───────────────────
        chain_rows = []
        for s in sorted(chain.keys()):
            ce = chain[s].get("CE", {})
            pe = chain[s].get("PE", {})
            moneyness = "ATM" if s == atm else ("ITM CE / OTM PE" if s < atm else "OTM CE / ITM PE")
            ce_oi_added = ce.get("oi", 0) - ce.get("oi_low", 0)
            pe_oi_added = pe.get("oi", 0) - pe.get("oi_low", 0)
            chain_rows.append({
                "STRIKE": s,
                "STATUS": moneyness,
                "CE_LTP":   round(ce.get("ltp", 0), 1),
                "CE_OI":    int(ce.get("oi", 0)),
                "CE_OI_ADD": int(ce_oi_added),
                "CE_VOL":   int(ce.get("volume", 0)),
                "PE_LTP":   round(pe.get("ltp", 0), 1),
                "PE_OI":    int(pe.get("oi", 0)),
                "PE_OI_ADD": int(pe_oi_added),
                "PE_VOL":   int(pe.get("volume", 0)),
            })

        result = {
            "spot":            round(spot, 2),
            "fut_ltp":         round(fut_ltp, 2),
            "atm":             atm,
            "step":            step,
            "max_pain":        max_pain,
            "pcr":             pcr,
            # Strongest OI strikes + LTPs
            "strongest_ce":      strongest_ce,
            "ce_oi":             ce_oi_map.get(strongest_ce, 0),
            "strongest_ce_ltp":  round(strongest_ce_ltp, 1),
            "strongest_pe":      strongest_pe,
            "pe_oi":             pe_oi_map.get(strongest_pe, 0),
            "strongest_pe_ltp":  round(strongest_pe_ltp, 1),
            # Shifting OI strikes + LTPs + pct
            "shifting_ce":       shifting_ce,
            "shifting_ce_add":   ce_added.get(shifting_ce, 0),
            "shifting_ce_pct":   shifting_ce_pct,
            "shifting_ce_ltp":   round(shifting_ce_ltp, 1),
            "shifting_pe":       shifting_pe,
            "shifting_pe_add":   pe_added.get(shifting_pe, 0),
            "shifting_pe_pct":   shifting_pe_pct,
            "shifting_pe_ltp":   round(shifting_pe_ltp, 1),
            # Most dropped PE (put support crumbling)
            "most_dropped_pe":       most_dropped_pe,
            "most_dropped_pe_val":   int(most_dropped_pe_val),
            "most_dropped_pe_pct":   most_dropped_pe_pct,
            "most_dropped_pe_ltp":   round(most_dropped_pe_ltp, 1),
            "most_dropped_pe_high":  round(most_dropped_pe_high, 1),
            # Most dropped CE (call resistance crumbling)
            "most_dropped_ce":       most_dropped_ce,
            # Near-ATM OI shifts (for pattern detection)
            "near_ce_pct":           near_ce_pct,
            "near_pe_pct":           near_pe_pct,
            "near_pe_drop_pct":      near_pe_drop_pct,
            "near_ce_added":         int(near_ce_added),
            "near_pe_added":         int(near_pe_added),
            # Call/Put walls
            "nearest_call_wall":     nearest_call_wall,
            "nearest_put_floor":     nearest_put_floor,
            "dist_to_call":          dist_to_call,
            "dist_to_put":           dist_to_put,
            # All call walls above spot sorted by OI (for SL)
            "call_walls_above":      sorted(
                [(s, ce_oi_map[s]) for s in ce_oi_map if s > spot],
                key=lambda x: x[1], reverse=True
            )[:3],
            # All put floors below spot sorted by OI (for target)
            "put_floors_below":      sorted(
                [(s, pe_oi_map[s]) for s in pe_oi_map if s < spot],
                key=lambda x: x[1], reverse=True
            )[:3],
            "direction":       direction,
            "direction_reason": direction_reason,
            "pain_signal":     pain_signal,
            "advice":          advice,
            "setup":           setup,
            "expiry":          str(expiry.date()),
            "timestamp":       datetime.now(IST).strftime("%H:%M:%S IST"),
            "chain_rows":      chain_rows,
        }

        # ── Save chain snapshot for 15-min delta ──────────────
        # Stored as {strike: {CE: {oi, ltp}, PE: {oi, ltp}}, "_ts": ..., "_slot": ...}
        import json as _json
        now_slot = datetime.now(IST).strftime("%H:%M")
        chain_snap = {"_ts": datetime.now(IST).isoformat(), "_slot": now_slot}
        for s in chain:
            chain_snap[str(s)] = {
                "CE": {"oi": int(chain[s].get("CE", {}).get("oi", 0)),
                       "ltp": round(chain[s].get("CE", {}).get("ltp", 0), 2)},
                "PE": {"oi": int(chain[s].get("PE", {}).get("oi", 0)),
                       "ltp": round(chain[s].get("PE", {}).get("ltp", 0), 2)},
            }
        # Rotate: save current as new snapshot only after building delta first
        result["chain_snap"] = chain_snap   # pass through to caller

        # Save to disk
        with open(OI_INTEL_CACHE_PATH, "w", encoding="utf-8") as f:
            _json.dump(result, f, default=str, ensure_ascii=False)

        return result

    except Exception as e:
        # Try loading today's cached version only
        cached = _load_today_json(OI_INTEL_CACHE_PATH)
        if cached is not None:
            return cached
        return None


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
        df = pd.read_csv(path)
        return bool(((df["DATE"] == today) & (df["KEY"] == key)).any())
    except Exception:
        return False

def _oi_15m_mark_sent(key):
    today = date.today().isoformat()
    row = {"DATE": today, "KEY": key}
    path = OI_15M_DEDUP
    try:
        if os.path.exists(path):
            df = pd.read_csv(path)
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        else:
            df = pd.DataFrame([row])
        df.to_csv(path, index=False)
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

    slot_prev = prev_snap.get("_slot", "—")
    slot_curr = chain_snap.get("_slot", "—")
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
                label = (f"📝 Call writing at {strike:,} CE — OI added {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium ₹{ltp_prev:.1f}→₹{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")
            elif opt == "CE" and direction == "DROP":
                label = (f"📈 Call wall unwinding at {strike:,} CE — OI dropped {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium ₹{ltp_prev:.1f}→₹{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")
            elif opt == "PE" and direction == "DROP":
                label = (f"⚠️ Put support crumbling at {strike:,} PE — OI dropped {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium ₹{ltp_prev:.1f}→₹{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")
            else:  # PE ADD
                label = (f"🛡️ Put floor building at {strike:,} PE — OI added {abs(oi_delta):,} "
                         f"({oi_delta_pct}% of strike OI in 15 min), "
                         f"premium ₹{ltp_prev:.1f}→₹{ltp_curr:.1f} ({ltp_arrow}{ltp_pct_chg:.1f}%)")

            events.append({
                "SLOT":         f"{slot_prev}→{slot_curr}",
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
                    lbl = (f"📝 Call writing at {strike:,} CE — OI added {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium ₹{ltp_p:.1f}→₹{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                elif opt == "CE" and dir_ == "DROP":
                    lbl = (f"📈 Call wall unwinding at {strike:,} CE — OI dropped {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium ₹{ltp_p:.1f}→₹{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                elif opt == "PE" and dir_ == "DROP":
                    lbl = (f"⚠️ Put support crumbling at {strike:,} PE — OI dropped {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium ₹{ltp_p:.1f}→₹{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                else:
                    lbl = (f"🛡️ Put floor building at {strike:,} PE — OI added {abs(delta):,} "
                           f"({dpct}% of strike OI in 15 min), "
                           f"premium ₹{ltp_p:.1f}→₹{ltp_c:.1f} ({larr}{lpct:.1f}%)")
                _candidates[cat] = {
                    "SLOT": f"{slot_prev}→{slot_curr}", "STRIKE": strike,
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
    Deduped per slot+strike+type+direction — fires at most once per 15-min candle.
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

    # ── Browser toast (one per significant event) ─────────
    for ev in new_events[:3]:   # cap at 3 toasts to avoid spam
        icon = "📝" if ev["TYPE"] == "CE" and ev["DIRECTION"] == "ADD" else (
               "📈" if ev["TYPE"] == "CE" else (
               "⚠️" if ev["DIRECTION"] == "DROP" else "🛡️"))
        st.toast(ev["LABEL"][:120], icon=icon)

    # ── Email ──────────────────────────────────────────────
    if not can_send_email() or not is_email_allowed():
        return

    now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    subject = f"[OiAnalytics] 15-Min OI Shift — {len(new_events)} Event(s) | {now_str}"

    # Build HTML table
    tbl_rows = ""
    for i, ev in enumerate(new_events):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        d_color = "#b71c1c" if ev["DIRECTION"] == "ADD" and ev["TYPE"] == "CE" else (
                  "#1b5e20" if ev["DIRECTION"] == "ADD" and ev["TYPE"] == "PE" else (
                  "#880e4f" if ev["DIRECTION"] == "DROP" and ev["TYPE"] == "PE" else "#0277bd"))
        tbl_rows += f"""<tr style="background:{bg};">
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">{ev["SLOT"]}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{ev["STRIKE"]:,} {ev["TYPE"]}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{d_color};font-weight:bold;">{ev["DIRECTION"]} {ev["OI_DELTA"]:+,}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{ev["OI_DELTA_%"]:.1f}%</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">₹{ev["LTP_PREV"]:.1f} → ₹{ev["LTP_CURR"]:.1f}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{"#c62828" if ev["LTP_CHG_%"] < 0 else "#2e7d32"};">{ev["LTP_CHG_%"]:+.1f}%</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-size:11px;max-width:320px;">{ev["LABEL"]}</td>
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
      Deduped per slot — each strike+type fires once per 15-min window.
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
        _tg_oi_slot = datetime.now(IST).strftime("%H%M")
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
        d = pd.read_csv(FUT_15M_DEDUP)
        return bool(((d["DATE"] == today) & (d["KEY"] == key)).any())
    except Exception:
        return False

def _fut_15m_mark_sent(slot, symbol, direction):
    key = f"{slot}|{symbol}|{direction}"
    today = date.today().isoformat()
    row = {"DATE": today, "KEY": key}
    try:
        if os.path.exists(FUT_15M_DEDUP):
            d = pd.read_csv(FUT_15M_DEDUP)
            d = pd.concat([d, pd.DataFrame([row])], ignore_index=True)
        else:
            d = pd.DataFrame([row])
        d.to_csv(FUT_15M_DEDUP, index=False)
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

    slot_prev = prev_snap.get("_slot", "—")
    slot_curr = curr_snap.get("_slot", "—")
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
        label = (f"{position_type} — {display_sym}: "
                 f"OI {oi_arrow}{oi_delta:,} ({oi_delta_pct:.1f}% of OI in 15 min), "
                 f"price ₹{ltp_prev:.1f}→₹{ltp_curr:.1f} ({ltp_arrow}{price_pct:.2f}%)")

        events.append({
            "SLOT":          f"{slot_prev}→{slot_curr}",
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
        lbl2 = (f"{pt2} — {dsym2}: OI {oa2}{d2:,} ({dp2:.1f}% of OI in 15 min), "
                f"price ₹{ltp_p2:.1f}→₹{ltp_c2:.1f} ({la2}{pp2:.2f}%)")
        cand2 = {"SLOT": f"{slot_prev}→{slot_curr}", "SYMBOL": dsym2,
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

    # ── Browser toasts (top 3) ─────────────────────────────
    for ev in new_events[:3]:
        st.toast(ev["LABEL"][:120], icon="📈" if ev["DIRECTION"] == "BUILD" else "📉")

    # ── Email ──────────────────────────────────────────────
    if not can_send_email() or not is_email_allowed():
        return

    now_str = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    subject = f"[OiAnalytics] Futures 15-Min OI Shift — {len(new_events)} Stock(s) | {now_str}"

    tbl_rows = ""
    for i, ev in enumerate(new_events):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        p_col = "#2e7d32" if ev["PRICE_%"] >= 0 else "#c62828"
        tbl_rows += f"""<tr style="background:{bg};">
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">{ev["SLOT"]}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{ev["SYMBOL"]}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{ev["PT_COLOR"]};font-weight:bold;">{ev["POSITION_TYPE"]}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;color:{ev["PT_COLOR"]};">{ev["OI_DELTA"]:+,}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;font-weight:bold;">{ev["OI_DELTA_%"]:.1f}%</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;">₹{ev["LTP_PREV"]:.1f} → ₹{ev["LTP_CURR"]:.1f}</td>
            <td style="padding:6px 8px;border:1px solid #ddd;font-size:12px;color:{p_col};">{ev["PRICE_%"]:+.2f}%</td>
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
# 6️⃣ Sort → BEST early runners on TOP
# -------------------------
early_runner_df = early_runner_df.sort_values(
    by=["MOMO_SCORE", "CHANGE_%"],
    ascending=[False, False]
)


# =========================================================
# 🚨 YESTERDAY GREEN → BREAKOUT ALERT
# =========================================================
new_green_break = detect_new_entries(
    "YEST_GREEN_BREAK",
    green_zone_df.Symbol.tolist()
)

notify_all(
    "YEST_GREEN_BREAK",
    "🟢Yesterday GREEN → Breakout Above YH",
    new_green_break,
    ltp_map
)


# =========================================================
# 🚨 YESTERDAY RED → BREAKDOWN ALERT
# =========================================================
new_red_break = detect_new_entries(
    "YEST_RED_BREAK",
    red_zone_df.Symbol.tolist()
)

notify_all(
    "YEST_RED_BREAK",
    "🔴Yesterday RED → Breakdown Below YL",
    new_red_break,
    ltp_map
)



###################################################################################
#🔴 RED SETUP =  LIVE_OPEN > YEST_LOW & LIVE_OPEN < YEST_CLOSE && First 15-min LOW should NOT break yesterday LOW
#🟢 GREEN SETUP =  LIVE_OPEN > YEST_CLOSE  & LIVE_OPEN < YEST_HIGH  & First 15-min HIGH should NOT break yesterday HIGH
###########################################################################################

# =========================================================
# FIRST 15-MIN CANDLE FETCHER — BATCHED (✅ FIX: was per-symbol, now one cached call)
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
    except:
        return []


# ✅ FIX: get_all_today_15m also moved to background thread — never blocks UI
def _fetch_15m_background():
    """Runs in a daemon thread — fetches all 15m candles without blocking UI."""
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
        except:
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
            pd.DataFrame(rows15).to_csv(FIFTEEN_M_CACHE_CSV, index=False)

if not _csv_is_fresh(FIFTEEN_M_CACHE_CSV) and not st.session_state.get("_15m_thread_running", False):
    st.session_state["_15m_thread_running"] = True
    def _run_15m_and_clear():
        _fetch_15m_background()
        st.session_state["_15m_thread_running"] = False
    t15 = threading.Thread(target=_run_15m_and_clear, daemon=True)
    t15.start()

# ✅ Read 15m data from CSV — always fresh after background thread completes
# Use _load_today_csv so stale yesterday data is never loaded
_df15_today = _load_today_csv(FIFTEEN_M_CACHE_CSV)
if _df15_today is not None:
    _df15 = _df15_today
    _df15["date"] = pd.to_datetime(_df15["date"])
    _all_15m = {sym: grp.to_dict("records") for sym, grp in _df15.groupby("Symbol")}
else:
    _all_15m = {}

# =========================================================
# 📐 EMA7 INTRADAY ENGINE
# =========================================================
# Computes EMA7 on two timeframes from today's live candles:
#   • 15-minute EMA7  → fine-grained entry timing (15m chart)
#   • 1-Hour EMA7     → higher-timeframe trend bias (1H chart)
#
# Storage (dated CSVs, auto-cleared next day):
#   CACHE/ema7_15min_YYYYMMDD.csv  → columns: Symbol, EMA7_15M, CANDLES_15M
#   CACHE/ema7_1hour_YYYYMMDD.csv  → columns: Symbol, EMA7_1H,  CANDLES_1H
#
# Lifecycle:
#   1. On first app load: built immediately from FIFTEEN_M_CACHE_CSV
#   2. Every 60 seconds: background thread recomputes + overwrites CSV
#   3. Dashboard reads CSV → merges into df → WATCHLIST tab shows correct values
# =========================================================

def _compute_intraday_ema7(df_candles, resample_rule):
    """
    Compute EMA7 on intraday candles, warmed up from historical data.

    HOW IT WORKS (same as Kite / TradingView):
      1. Load CACHE/{Symbol}_5minute.csv  →  30-day 5-min history per symbol
      2. Resample history to target timeframe (15min / 60min), previous days only
      3. Append TODAY's completed candles from df_candles (today's 15m cache)
      4. Run ewm(span=7, adjust=False) across the full combined series
         → EMA is warmed from history so the value at 09:15 today is accurate
      5. Return today's last EMA7 value per symbol

    At 09:15 with 0 today-candles → EMA7 = warmed from yesterday  ✅
    At 09:30 with 1 today-candle  → EMA7 updates correctly         ✅
    Without warmup                → EMA7 at 09:15 ≈ just the open  ❌
    """
    label = "15M" if "15" in resample_rule else "1H"
    rows  = []

    # ── Current partial-candle cutoff (exclude in-progress candle) ────
    _now_ist   = datetime.now(IST).replace(tzinfo=None)
    _min_floor = (_now_ist.minute // 15) * 15
    _cutoff    = pd.Timestamp(_now_ist.replace(minute=_min_floor, second=0, microsecond=0))
    _today_dt  = date.today()

    # ── Build today's completed candles map: sym → Series(close, index=datetime) ──
    today_map = {}
    if df_candles is not None and not df_candles.empty:
        _t = df_candles.copy()
        _t["date"] = pd.to_datetime(_t["date"])
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
                _h = pd.read_csv(hist_path)
                _h["date"] = pd.to_datetime(_h["date"])
                _h = _h[_h["date"].dt.date < _today_dt].sort_values("date")
                if not _h.empty:
                    hist_series = (
                        _h.set_index("date")["close"]
                        .resample(resample_rule, origin="start")
                        .last()
                        .dropna()
                    )
            except Exception:
                pass

        # ── Resample today's candles ─────────────────────────────────────
        today_series = pd.Series(dtype=float)
        if sym in today_map and not today_map[sym].empty:
            today_series = (
                today_map[sym]
                .resample(resample_rule, origin="start")
                .last()
                .dropna()
            )

        if hist_series.empty and today_series.empty:
            continue

        # ── Combine: history first, then today ───────────────────────────
        combined = pd.concat([hist_series, today_series]).sort_index()
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
    Compute history-warmed EMA7 for 15m + 1H → save to dated CSVs atomically.
    Called at startup and every 60 seconds by background thread.
    Runs even pre-market so EMA7 is ready the moment 09:15 opens.
    """
    src = _load_today_csv(FIFTEEN_M_CACHE_CSV)   # None pre-market → handled gracefully

    def _atomic_save(df_out, dest_path):
        if df_out is None or df_out.empty:
            return
        tmp = dest_path + ".tmp"
        try:
            df_out.to_csv(tmp, index=False)
            os.replace(tmp, dest_path)   # atomic on POSIX — UI never sees partial file
        except Exception:
            try: os.remove(tmp)
            except Exception: pass

    _atomic_save(_compute_intraday_ema7(src, "15min"), EMA7_15M_FILE)
    _atomic_save(_compute_intraday_ema7(src, "60min"), EMA7_1H_FILE)


def _ema7_refresh_loop():
    """Background daemon — refreshes EMA7 every 60 s from 08:50 to 15:35."""
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

# ── EMA7 INTRADAY MERGE — fuse into main df ──────────────────────────────────
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
    # 🟢 GREEN STRUCTURE (Yesterday GREEN → Break Above YH)
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
                "Symbol": r["Symbol"],
                "LTP": r["LTP"],
                "CHANGE": r["CHANGE"],
                "LIVE_OPEN": r["LIVE_OPEN"],
                "YEST_OPEN": r["YEST_OPEN"],
                "YEST_CLOSE": r["YEST_CLOSE"],
                "YEST_HIGH": r["YEST_HIGH"],
                "BREAK_TIME": breakout_candle["date"].strftime("%H:%M"),
                "POST_BREAK_GAIN": gain_value,
                "POST_BREAK_GAIN_%": gain_pct,
                "CHANGE_%": r["CHANGE_%"]
            })

    # =====================================================
    # 🔴 RED STRUCTURE (Yesterday RED → Break Below YL)
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
                "Symbol": r["Symbol"],
                "LTP": r["LTP"],
                "CHANGE": r["CHANGE"],
                "LIVE_OPEN": r["LIVE_OPEN"],
                "YEST_OPEN": r["YEST_OPEN"],
                "YEST_HIGH": r["YEST_HIGH"],
                "YEST_CLOSE": r["YEST_CLOSE"],
                "YEST_LOW": r["YEST_LOW"],
                "BREAK_TIME": breakdown_candle["date"].strftime("%H:%M"),
                "POST_BREAK_DROP": drop_value,
                "POST_BREAK_DROP_%": drop_pct,
                "CHANGE_%": r["CHANGE_%"]
            })


# =========================================================
# CREATE DATAFRAMES (SAFE COLUMNS)
# =========================================================

green_structure_df = pd.DataFrame(
    green_rows,
    columns=[
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "YEST_HIGH",
        "BREAK_TIME",
        "POST_BREAK_GAIN",
        "POST_BREAK_GAIN_%"
        
    ]
)

red_structure_df = pd.DataFrame(
    red_rows,
    columns=[
        "Symbol",
        "LTP",
        "CHANGE",
        "CHANGE_%",
        "LIVE_OPEN",
        "YEST_HIGH",
        "BREAK_TIME",
        "POST_BREAK_DROP",
        "POST_BREAK_DROP_%"
    ]
)

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







######################################################################################

############        STEP 1 — Add 1H Candle Fetch Function

# =========================================================
# 1-HOUR OPENING RANGE (9:15–10:15)
# =========================================================
@st.cache_data(ttl=95, show_spinner=False)   # ✅ FIX: was ttl=60 — called per-symbol in loop, expired on every refresh
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
    except:
        return None

    if not bars or len(bars) < 4:
        return None

    df15 = pd.DataFrame(bars)
    df15["date"] = pd.to_datetime(df15["date"])

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


###################     STEP 2 — Build Hourly Breakout Screener

# =========================================================
# ⏰ 1H OPENING RANGE BREAKOUT (WITH EMA20 FILTER)
# =========================================================

hourly_rows = []

for sym in SYMBOLS:

    # ---- Get 1H candle (9:15–10:15)
    df1h = intraday_15m_df[
        intraday_15m_df["Symbol"] == sym
    ].sort_values("datetime")

    if len(df1h) < 4:
        continue

    first_hour = df1h.iloc[:4]

    h1_high = first_hour["high"].max()
    h1_low  = first_hour["low"].min()
    # 1️⃣ First Hour Range %
    range_pct = round(
        ((h1_high - h1_low) / h1_low) * 100,
        2
    )
    current_type = None
    break_time = ""
    post_break_pct = 0

    # ---- Get live row
    row = df[df["Symbol"] == sym]
    if row.empty:
        continue

    row = row.iloc[0]

    # 🔥 REQUIRE EMA20
    if pd.isna(row["EMA20"]):
        continue

    # =====================================================
    # 🟢 BREAKOUT (Above 1H High + Above EMA20)
    # =====================================================
    if (
        row["LTP"] > h1_high and
        row["LTP"] > row["EMA20"]
    ):
        post_break_pct = round(
            ((row["LTP"] - h1_high) / h1_high) * 100,
            2
        )

        hourly_rows.append({
            "Symbol": sym,
            "TYPE": "🟢 1H BREAKOUT",
            "1H_HIGH": round(h1_high, 2),
            "1H_LOW": round(h1_low, 2),
            "1H_RANGE_%": range_pct,
            "LTP": round(row["LTP"], 2),
            "CHANGE": row["CHANGE"],
            "CHANGE_%": row["CHANGE_%"],
            "POST_BREAK_MOVE_%": post_break_pct,
            "EMA20": round(row["EMA20"], 2),
            "LIVE_HIGH": row["LIVE_HIGH"],
            "LIVE_LOW": row["LIVE_LOW"],
            "YEST_HIGH": row["YEST_HIGH"],
            "YEST_LOW": row["YEST_LOW"],
            #"CHANGE_%": row["CHANGE_%"],
            "BREAK_TIME": datetime.now(IST).strftime("%H:%M:%S")
        })

    # =====================================================
    # 🔴 BREAKDOWN (Below 1H Low + Below EMA20)
    # =====================================================
    elif (
        row["LTP"] < h1_low and
        row["LTP"] < row["EMA20"]
    ):
        post_break_pct = round(
            ((h1_low - row["LTP"]) / h1_low) * 100,
            2
        )
        hourly_rows.append({
            "Symbol": sym,
            "TYPE": "🔴 1H BREAKDOWN",
            "1H_HIGH": round(h1_high, 2),
            "1H_LOW": round(h1_low, 2),
            "1H_RANGE_%": range_pct,
            "LTP": round(row["LTP"], 2),
            "CHANGE": row["CHANGE"],
            "CHANGE_%": row["CHANGE_%"],
            "POST_BREAK_MOVE_%": post_break_pct,
            "EMA20": round(row["EMA20"], 2),
            "LIVE_HIGH": row["LIVE_HIGH"],
            "LIVE_LOW": row["LIVE_LOW"],
            "YEST_HIGH": row["YEST_HIGH"],
            "YEST_LOW": row["YEST_LOW"],
            "BREAK_TIME": datetime.now(IST).strftime("%H:%M:%S")
        })


hourly_break_df = pd.DataFrame(hourly_rows)





#########   STEP 3 — Add Alerts

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


##############      STEP 1 — ADD THIS BLOCK AFTER EMA BUILD (After df = df.merge(ema_df...))

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


###################         STEP 2 — BUILD S/R FROM 180-DAY DATA

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


########################            STEP 3 — MERGE NEAREST LEVEL INTO MAIN DF

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

#############       STEP 2 — APPLY TO DAILY STRONG S/R
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

##############      STEP 1 — AUTO FETCH ALL FUTURES
# ================= AUTO FUTURES FETCHER =================

@st.cache_data(ttl=3600, show_spinner=False)
def get_nearest_month_futures():

    try:
        instruments = kite.instruments("NFO")
    except:
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

########################        STEP 2 — USE AUTO FUTURE LIST
FUTURES_LIST = tuple(get_nearest_month_futures())  # ✅ FIX: tuple is hashable for cache keys

##########################      STEP 3 — REAL OI TRACKING ENGINE
# OI_SNAPSHOT_FILE already defined at top in dated file paths block

def load_prev_oi():
    """Load today's OI snapshot. Safe: OI_SNAPSHOT_FILE is already a dated path."""
    result = _load_today_csv(OI_SNAPSHOT_FILE, required_cols=["FUT_SYMBOL","OI"])
    return result if result is not None else pd.DataFrame(columns=["FUT_SYMBOL","OI"])

def save_snapshot(df):
    df[["FUT_SYMBOL","OI"]].to_csv(OI_SNAPSHOT_FILE, index=False)

######################      STEP 4 — FETCH ALL FUTURES OI
@st.cache_data(ttl=95, show_spinner=False)   # ✅ silent background refresh
# ✅ FIX: accepts tuple not list — lists are unhashable and break @st.cache_data
def fetch_all_futures_data(futures_tuple):

    rows = []

    if not futures_tuple:
        return pd.DataFrame()

    try:
        quotes = kite.quote(list(futures_tuple))
    except:
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

################################        STEP 5 — BUILD FULL DERIVATIVE ENGINE
# ✅ FUTURES — synchronous single batch call every rerun (~0.5 sec)
def fetch_futures_now():
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
            result.to_csv(FUT_CACHE_CSV, index=False)
        return result
    except:
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


###################     STEP 6 — OI HEATMAP SCORE
# Create strength score
if not fut_df.empty and "REAL_OI_%" in fut_df.columns and "PRICE_%" in fut_df.columns:
    fut_df["OI_SCORE"] = (fut_df["REAL_OI_%"].abs() + fut_df["PRICE_%"].abs()).round(2)

##########################          SNIPPET — STRONG CLOSING FILTER

# ✅ FIX: DAY_HIGH/DAY_LOW already included in _fetch_futures_background — no extra API call needed
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


################        STEP 1 — ADD OI FETCH FUNCTION
# =========================================================
# 📊 OI DATA FETCH
# =========================================================

# ✅ OI — synchronous single batch call, runs every rerun (~1 sec)
# ✅ OI already fetched inside fetch_live_and_oi() — reuse, no second kite.quote call
oi_df = _oi_prefetch

###################     STEP 2 — MERGE OI INTO MAIN DF
df = df.merge(oi_df, on="Symbol", how="left")

#df["OI"] = pd.to_numeric(df.get("OI", 0), errors="coerce").fillna(0)
# ================= SAFE OI HANDLING =================

if "OI" not in df.columns:
    df["OI"] = 0

df["OI"] = pd.to_numeric(df["OI"], errors="coerce").fillna(0)
df["OI_CHANGE_%"] = pd.to_numeric(df.get("OI_CHANGE_%", 0), errors="coerce").fillna(0)

###################     STEP 3 — OI STRENGTH ENGINE
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


################        STEP 4 — STRONG OI FILTER TABLE

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
    st.info("No strong bullish closing stocks near day high.")


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
    st.info("No strong bearish closing stocks near day low.")



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

    ohlc = pd.read_csv(OHLC_FILE)
    ohlc["date"] = pd.to_datetime(ohlc["date"])

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
    "🚀 2M Downtrend → EMA Reversal",
    new_reversal,
    ltp_map
)


astro = get_astro_score()

def allow_trade(row):

    # 🚫 Block bad astro days
    if astro["score"] <= -2:
        return "BLOCK"

    # 🔥 Strong day → full trade
    if astro["score"] >= 1:
        return "FULL"

    # ⚠️ Neutral → only breakout trades
    if astro["score"] == 0:
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

st.subheader("⚡ LIVE ALERTS")

if alerts_df.empty:
    st.info("No live alerts yet.")
else:

    #alerts_df = alerts_df.sort_values("TS", ascending=False)
    if "TS" in alerts_df.columns:
        alerts_df["TS"] = pd.to_datetime(alerts_df["TS"], errors="coerce")
        alerts_df = alerts_df.sort_values("TS", ascending=False)


    display_cols = [
        "TIME",
        "TYPE",
        "Symbol",
        "LTP",
        "CHANGE_%",
        "LIVE_VOLUME",
        "YEST_VOL",
        "VOL_%",
        "DAY_OPEN",
        "DAY_HIGH",
        "DAY_LOW",
        "YEST_HIGH",
        "YEST_LOW",
        "YEST_CLOSE"
    ]

    existing_cols = [c for c in display_cols if c in alerts_df.columns]
    alerts_df = alerts_df[existing_cols]

    st.dataframe(
        alerts_df,
        width='stretch'
    )



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


            ######################      Clear alerts button
if st.button("🧹 Clear Alerts"):
    st.session_state.alerts = []
    st.session_state.alert_keys = set()


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

# Inject JS: on tab button click → update ?tab=N in URL (no page reload)
# On load → auto-click the stored tab button after a tiny delay
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

        // Listen for tab clicks → update URL query param
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
    "🔭 ASTRO",
    "🪐 PANCHAK",
    "🟢 TOP_HIGH",
    "🔴 TOP_LOW",
    "🟡 NEAR",
    "WATCHLIST",
    "BREAKOUT",
    "📈 D-BREAKS",
    "📊 W-BREAKS",
    "📅 M-BREAKS",
    "⚡ O=H=L",  
    "📉 EMA20-50",
    "🔥 TOP G/L",
    " 4-BAR",
    "🧠 OPTIONS",
    "INDICES",
    "15-MIN-3",
    "⚡ Alerts",    
    "ℹ️ INFO"
])

# ═══════════════════════════════════════════════════════════════════════
# 🔭 TAB 0 — ASTRO INTELLIGENCE CENTRE
# Sources: Banerjee "Stock Market Astrology" (2009), P.K. Vasudev
#          "Vedic Astrology in Money Matters", KP Krishnamurti system
# ═══════════════════════════════════════════════════════════════════════
with tabs[0]:
    st.header("🔭 ASTRO + OI INTELLIGENCE CENTRE")
    st.caption(
        "Sources: Banerjee *Stock Market Astrology* · Vasudev *Vedic Astrology in Money Matters* · "
        "Vyapar Ratna (Trivedi & Ojha) · KP Krishnamurti · Live Kite OI Chain. **NOT financial advice.**"
    )

    # ═══════════════════════════════════════════════════════════════════
    # COMBINED ASTRO + OI SIGNAL PANEL (TOP — most important)
    # Merges OI direction + Astro direction → CONFIRMED TRADE SIGNAL
    # ═══════════════════════════════════════════════════════════════════
    st.subheader("🎯 Combined Astro + OI — Confirmed Trade Signal")
    st.caption("OI engine (live option chain) × Astro engine (nakshatra/hora/tithi). "
               "Agreement = HIGH CONFIDENCE. Disagreement = WAIT.")

    try:
        _today_combined = _vedic_day_analysis(datetime.now(IST).date())
        _oi_combined    = fetch_nifty_oi_intelligence()

        # ── Astro direction for today ──────────────────────────
        _ast_score = _today_combined["net_score"]
        _ast_nak   = _today_combined["moon_nak"]
        _ast_lord  = _today_combined.get("moon_nak_lord","")
        _BENEF_C   = {"Sun","Jupiter","Moon","Mars"}
        _MALEF_C   = {"Saturn","Rahu","Ketu","Mercury","Venus"}
        _ast_nak_signal = "BULLISH" if _ast_lord in _BENEF_C else "BEARISH" if _ast_lord in _MALEF_C else "MIXED"
        if _ast_score >= 3 or (_ast_score >= 1 and _ast_nak_signal == "BULLISH"):
            _ast_dir = "🟢 BULLISH"
        elif _ast_score <= -3 or (_ast_score <= -1 and _ast_nak_signal == "BEARISH"):
            _ast_dir = "🔴 BEARISH"
        elif _today_combined["mars_rahu_deg"] < 12:
            _ast_dir = "🔴 BEARISH"
        else:
            _ast_dir = "🟡 MIXED"

        # ── OI direction ──────────────────────────────────────
        if _oi_combined:
            _oi_dir    = _oi_combined.get("direction","⚠️ SIDEWAYS")
            _oi_spot   = _oi_combined.get("spot", 0)
            _oi_atm    = _oi_combined.get("atm", 0)
            _oi_pcr    = _oi_combined.get("pcr", 0)
            _oi_mp     = _oi_combined.get("max_pain", 0)
            _oi_reason = _oi_combined.get("direction_reason","")
            _oi_advice = _oi_combined.get("advice","")
            _oi_setup  = _oi_combined.get("setup","")
            _oi_cwall  = _oi_combined.get("nearest_call_wall", 0)
            _oi_pfloor = _oi_combined.get("nearest_put_floor", 0)
            _oi_sce    = _oi_combined.get("strongest_ce", 0)
            _oi_sce_ltp= _oi_combined.get("strongest_ce_ltp", 0)
            _oi_spe    = _oi_combined.get("strongest_pe", 0)
            _oi_spe_ltp= _oi_combined.get("strongest_pe_ltp", 0)
            _oi_sh_ce  = _oi_combined.get("shifting_ce", 0)
            _oi_sh_ce_pct = _oi_combined.get("shifting_ce_pct", 0)
            _oi_sh_pe  = _oi_combined.get("shifting_pe", 0)
            _oi_sh_pe_pct = _oi_combined.get("shifting_pe_pct", 0)
            _oi_ne_ce_pct = _oi_combined.get("near_ce_pct", 0)
            _oi_ne_pe_pct = _oi_combined.get("near_pe_pct", 0)
            _oi_ne_pe_drop= _oi_combined.get("near_pe_drop_pct", 0)
            _oi_call_walls= _oi_combined.get("call_walls_above",[])
            _oi_put_floors= _oi_combined.get("put_floors_below",[])
            _oi_expiry = _oi_combined.get("expiry","")
            _oi_ts     = _oi_combined.get("timestamp","")
        else:
            _oi_dir = "⚠️ OI data not available"
            _oi_spot = _oi_atm = _oi_pcr = _oi_mp = 0
            _oi_reason = _oi_advice = _oi_setup = ""
            _oi_cwall = _oi_pfloor = _oi_sce = _oi_spe = 0
            _oi_sce_ltp = _oi_spe_ltp = _oi_sh_ce = _oi_sh_pe = 0
            _oi_sh_ce_pct = _oi_sh_pe_pct = _oi_ne_ce_pct = _oi_ne_pe_pct = _oi_ne_pe_drop = 0
            _oi_call_walls = _oi_put_floors = []
            _oi_expiry = _oi_ts = ""

        # ── COMBINE: Check agreement ──────────────────────────
        _both_bull = "BULLISH" in _ast_dir and "BULLISH" in _oi_dir
        _both_bear = "BEARISH" in _ast_dir and "BEARISH" in _oi_dir
        _conflict  = ("BULLISH" in _ast_dir and "BEARISH" in _oi_dir) or \
                     ("BEARISH" in _ast_dir and "BULLISH" in _oi_dir)

        if _both_bull:
            _combined_verdict = "🟢 STRONG BUY — CONFIRMED"
            _combined_color   = "#00C851"
            _combined_card    = "signal-card-bull"
            _combined_conf    = "VERY HIGH"
            _combined_action  = f"✅ BUY CE — ATM {_oi_atm} CE or next strike"
            _buy_strike       = _oi_atm
            _buy_type         = "CE"
            _sl_level         = _oi_pfloor or (_oi_atm - 100)
            _target_level     = _oi_cwall or (_oi_atm + 150)
        elif _both_bear:
            _combined_verdict = "🔴 STRONG SELL — CONFIRMED"
            _combined_color   = "#FF4444"
            _combined_card    = "signal-card-bear"
            _combined_conf    = "VERY HIGH"
            _combined_action  = f"❌ BUY PE — ATM {_oi_atm} PE or next strike"
            _buy_strike       = _oi_atm
            _buy_type         = "PE"
            _sl_level         = _oi_cwall or (_oi_atm + 100)
            _target_level     = _oi_pfloor or (_oi_atm - 150)
        elif _conflict:
            _combined_verdict = "⚠️ CONFLICTING — WAIT"
            _combined_color   = "#FFD700"
            _combined_card    = "signal-card-mixed"
            _combined_conf    = "LOW"
            _combined_action  = "⛔ DO NOT ENTER — Astro and OI disagree. Wait for alignment."
            _buy_strike       = _oi_atm
            _buy_type         = "—"
            _sl_level         = 0
            _target_level     = 0
        elif "BULLISH" in _oi_dir:
            _combined_verdict = "🟢 BULLISH BIAS (OI dominant)"
            _combined_color   = "#7ddb9d"
            _combined_card    = "signal-card-bull"
            _combined_conf    = "MEDIUM"
            _combined_action  = f"✅ Lean LONG — small CE position, tight SL"
            _buy_strike       = _oi_atm
            _buy_type         = "CE"
            _sl_level         = _oi_pfloor or (_oi_atm - 100)
            _target_level     = _oi_cwall or (_oi_atm + 100)
        elif "BEARISH" in _oi_dir:
            _combined_verdict = "🔴 BEARISH BIAS (OI dominant)"
            _combined_color   = "#ff8888"
            _combined_card    = "signal-card-bear"
            _combined_conf    = "MEDIUM"
            _combined_action  = f"❌ Lean SHORT — small PE position, tight SL"
            _buy_strike       = _oi_atm
            _buy_type         = "PE"
            _sl_level         = _oi_cwall or (_oi_atm + 100)
            _target_level     = _oi_pfloor or (_oi_atm - 100)
        else:
            _combined_verdict = "🟡 SIDEWAYS — RANGE TRADE"
            _combined_color   = "#FFD700"
            _combined_card    = "signal-card-mixed"
            _combined_conf    = "LOW"
            _combined_action  = f"⚠️ Sell {_oi_sce} CE + Buy {_oi_spe} PE hedge"
            _buy_strike       = _oi_atm
            _buy_type         = "—"
            _sl_level         = _oi_cwall or 0
            _target_level     = _oi_pfloor or 0

        # ── Display combined card ─────────────────────────────
        st.markdown(f"""
<div class="{_combined_card}" style="padding:16px">
  <div style="font-size:24px;font-weight:800;color:{_combined_color};margin-bottom:8px">
    {_combined_verdict}
  </div>
  <div style="display:flex;gap:20px;flex-wrap:wrap;margin-bottom:10px">
    <span style="background:#111;padding:4px 12px;border-radius:16px;font-size:13px">
      🌙 Astro: <b style="color:{_combined_color if 'BULLISH' in _ast_dir else '#FF4444' if 'BEARISH' in _ast_dir else '#FFD700'}">{_ast_dir}</b>
    </span>
    <span style="background:#111;padding:4px 12px;border-radius:16px;font-size:13px">
      📊 OI: <b style="color:{_combined_color if 'BULLISH' in _oi_dir else '#FF4444' if 'BEARISH' in _oi_dir else '#FFD700'}">{_oi_dir}</b>
    </span>
    <span style="background:#111;padding:4px 12px;border-radius:16px;font-size:13px">
      🎯 Confidence: <b style="color:{_combined_color}">{_combined_conf}</b>
    </span>
    <span style="background:#111;padding:4px 12px;border-radius:16px;font-size:13px">
      📍 Spot: <b>{_oi_spot}</b>  ATM: <b>{_oi_atm}</b>
    </span>
    <span style="background:#111;padding:4px 12px;border-radius:16px;font-size:13px">
      📈 PCR: <b>{_oi_pcr}</b>  MaxPain: <b>{_oi_mp}</b>
    </span>
  </div>
  <div style="font-size:16px;font-weight:600;color:#fff;margin-bottom:10px">
    📌 {_combined_action}
  </div>
  <div style="font-size:13px;color:#bbb">{_oi_reason}</div>
</div>""", unsafe_allow_html=True)

        # ── STRIKE RECOMMENDATION PANEL ───────────────────────
        if _buy_type != "—":
            st.markdown("#### 🎯 Strike Selection & Levels")
            _sc1, _sc2, _sc3, _sc4 = st.columns(4)
            _sc1.metric("🛒 Buy Strike", f"{_buy_strike} {_buy_type}")
            if _buy_type == "CE":
                _atm_ltp = _oi_combined.get("chain_rows",[]) if _oi_combined else []
                _atm_chain = next((r for r in _atm_ltp if r.get("STRIKE")==_buy_strike), {}) if _atm_ltp else {}
                _strike_ltp = _atm_chain.get("CE_LTP",0) if _atm_chain else 0
            else:
                _atm_ltp = _oi_combined.get("chain_rows",[]) if _oi_combined else []
                _atm_chain = next((r for r in _atm_ltp if r.get("STRIKE")==_buy_strike), {}) if _atm_ltp else {}
                _strike_ltp = _atm_chain.get("PE_LTP",0) if _atm_chain else 0
            _sc2.metric("💰 Option LTP", f"₹{_strike_ltp}" if _strike_ltp else "—")
            _sc3.metric("🛑 SL Level", f"{_sl_level}" if _sl_level else "—",
                       delta=f"{round(_sl_level - _oi_spot,0)} pts" if _sl_level and _oi_spot else None)
            _sc4.metric("🎯 Target Level", f"{_target_level}" if _target_level else "—",
                       delta=f"{round(_target_level - _oi_spot,0)} pts" if _target_level and _oi_spot else None)

        # ── OI WALLS + FLOORS TABLE ───────────────────────────
        st.markdown("#### 🧱 Call Walls (Resistance) & Put Floors (Support)")
        _wf1, _wf2 = st.columns(2)
        with _wf1:
            st.markdown("**🔴 Call Walls — Resistance levels (CE OI)**")
            _wall_rows = []
            for _ws, _woi in (_oi_call_walls or []):
                _wltp = next((r.get("CE_LTP",0) for r in (_oi_combined.get("chain_rows",[]) if _oi_combined else []) if r.get("STRIKE")==_ws), 0)
                _wall_rows.append({"Strike": _ws, "CE OI": f"{int(_woi):,}", "CE LTP": f"₹{_wltp}",
                                   "Dist from Spot": f"{int(_ws - _oi_spot)} pts"})
            if _wall_rows:
                st.dataframe(pd.DataFrame(_wall_rows).style
                    .set_properties(**{"background-color":"#2b0d0d","color":"#FF4444","font-size":"12px"})
                    .set_table_styles([{"selector":"th","props":[("background","#111"),("color","white")]}]),
                    use_container_width=True, height=150, hide_index=True)
            else:
                st.info("No wall data")
        with _wf2:
            st.markdown("**🟢 Put Floors — Support levels (PE OI)**")
            _floor_rows = []
            for _fs, _foi in (_oi_put_floors or []):
                _fltp = next((r.get("PE_LTP",0) for r in (_oi_combined.get("chain_rows",[]) if _oi_combined else []) if r.get("STRIKE")==_fs), 0)
                _floor_rows.append({"Strike": _fs, "PE OI": f"{int(_foi):,}", "PE LTP": f"₹{_fltp}",
                                    "Dist from Spot": f"{int(_oi_spot - _fs)} pts"})
            if _floor_rows:
                st.dataframe(pd.DataFrame(_floor_rows).style
                    .set_properties(**{"background-color":"#0d2b0d","color":"#00C851","font-size":"12px"})
                    .set_table_styles([{"selector":"th","props":[("background","#111"),("color","white")]}]),
                    use_container_width=True, height=150, hide_index=True)
            else:
                st.info("No floor data")

        # ── OI SHIFT ALERTS ───────────────────────────────────
        st.markdown("#### 📈 Huge OI Shifts (Fresh Build / Unwind)")
        _os1, _os2, _os3 = st.columns(3)
        with _os1:
            st.markdown(f"""
<div class="signal-card-bear" style="padding:10px">
  <b>🔴 CE Building (Bear signal)</b><br>
  Strike: <b>{_oi_sh_ce}</b> CE<br>
  OI Added: <b>+{_oi_sh_ce_pct}%</b><br>
  Near ATM CE build: <b>{_oi_ne_ce_pct}%</b>
</div>""", unsafe_allow_html=True)
        with _os2:
            st.markdown(f"""
<div class="signal-card-bull" style="padding:10px">
  <b>🟢 PE Building (Bull signal)</b><br>
  Strike: <b>{_oi_sh_pe}</b> PE<br>
  OI Added: <b>+{_oi_sh_pe_pct}%</b><br>
  Near ATM PE build: <b>{_oi_ne_pe_pct}%</b>
</div>""", unsafe_allow_html=True)
        with _os3:
            _dropped_pe = _oi_combined.get("most_dropped_pe", 0) if _oi_combined else 0
            _dropped_pe_pct = _oi_combined.get("most_dropped_pe_pct", 0) if _oi_combined else 0
            _pe_drop_color = "#FF4444" if _dropped_pe_pct < -5 else "#FFD700"
            st.markdown(f"""
<div class="signal-card-mixed" style="padding:10px">
  <b>⚠️ PE Unwinding (Bear alert)</b><br>
  Strike: <b>{_dropped_pe}</b> PE<br>
  OI Drop: <b>{_dropped_pe_pct}%</b><br>
  Near ATM PE drop: <b style="color:{_pe_drop_color}">{_oi_ne_pe_drop}%</b>
</div>""", unsafe_allow_html=True)

        # ── FULL OPTION CHAIN TABLE ────────────────────────────
        if _oi_combined and _oi_combined.get("chain_rows"):
            with st.expander("📋 Full Option Chain (ATM ±10 Strikes)"):
                _chain_df = pd.DataFrame(_oi_combined["chain_rows"])
                def _chain_style(val):
                    v = str(val)
                    if "ATM" in v: return "background-color:#1a1a2e;color:#FFD700;font-weight:700"
                    if "OTM CE" in v: return "background-color:#0d0d1a;color:#aaa"
                    if "ITM CE" in v: return "background-color:#1a0d0d;color:#ff8888"
                    return ""
                def _oi_add_style(val):
                    try:
                        v = int(val)
                        if v > 0: return "color:#00C851;font-weight:600"
                        if v < 0: return "color:#FF4444;font-weight:600"
                    except: pass
                    return ""
                st.dataframe(
                    _chain_df.style
                        .map(_chain_style, subset=["STATUS"])
                        .map(_oi_add_style, subset=["CE_OI_ADD","PE_OI_ADD"])
                        .set_properties(**{"font-size":"12px","text-align":"right","background-color":"#0a0a0a","color":"#ddd"})
                        .set_table_styles([{"selector":"th","props":[("background","#111"),("color","white"),("font-weight","bold")]}]),
                    use_container_width=True, height=400, hide_index=True,
                )
                st.caption(f"Expiry: {_oi_expiry} | Updated: {_oi_ts}")

    except Exception as _comb_err:
        st.warning(f"Combined panel error: {_comb_err}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════════
    # STOCK-SPECIFIC FUTURE DATES — HIGH BULL / BEAR PROBABILITY
    # Sources: Banerjee Ch4/Ch6 (graha→sector), Vyapar Ratna Part 2
    #   VR2: Iron/Steel bearish — Saturn+Moon/Mars/Mercury yoga
    #   VR2: Mercury near Sun (direct) + Venus far = bullish futures
    #   VR2: Mercury-Venus conjunction = bearish for prices
    #   VR2: Jupiter+Rahu conjunction = Gold/Silver hyperinflation
    #   Banerjee: Saturn in Taurus/Virgo/Capricorn (earthy) = recovery
    # ═══════════════════════════════════════════════════════════════════
    st.subheader("📅 Stock-Specific Future Bull/Bear Date Forecast")
    st.caption(
        "Computed from upcoming planetary transits vs stock sector rulerships. "
        "Source: Banerjee Ch4 (planet→sector) + Vyapar Ratna Part 2 (specific yogas). "
        "**Probability only — NOT financial advice.**"
    )

    # Stock → ruling planets (Banerjee Ch4 + sector)
    _STOCK_PLANETS = {
        # Banking & Finance — Jupiter, Mercury, Venus (Banerjee: Guru+Budha+Sukra)
        "HDFCBANK":  {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        "SBIN":      {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        "ICICIBANK": {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        "AXISBANK":  {"sector":"Banking","planets":["Jupiter","Mercury","Venus"],"bull_signs":["Cancer","Pisces","Sagittarius"],"bear_signs":["Gemini","Virgo","Capricorn"]},
        # IT — Mercury, Ketu (Banerjee: Budha+Ketu)
        "TCS":       {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        "INFY":      {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        "WIPRO":     {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        "HCLTECH":   {"sector":"IT","planets":["Mercury","Ketu"],"bull_signs":["Virgo","Gemini","Scorpio"],"bear_signs":["Pisces","Sagittarius","Aries"]},
        # Steel/Metals — Mars, Saturn (Banerjee: Mangal+Sani; VR2: Saturn+Moon yoga = BEARISH)
        "TATASTEEL": {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "SAIL":      {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "JSWSTEEL":  {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "HINDALCO":  {"sector":"Metals","planets":["Mars","Saturn"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        # Oil & Gas — Moon, Saturn, Ketu (Banerjee: Candra+Sani+Ketu)
        "RELIANCE":  {"sector":"Oil & Gas","planets":["Moon","Saturn","Ketu"],"bull_signs":["Cancer","Scorpio","Pisces"],"bear_signs":["Aries","Leo","Sagittarius"]},
        "ONGC":      {"sector":"Oil & Gas","planets":["Moon","Saturn","Ketu"],"bull_signs":["Cancer","Scorpio","Pisces"],"bear_signs":["Aries","Leo","Sagittarius"]},
        "BPCL":      {"sector":"Oil & Gas","planets":["Moon","Saturn","Ketu"],"bull_signs":["Cancer","Scorpio","Pisces"],"bear_signs":["Aries","Leo","Sagittarius"]},
        # Pharma — Mars, Ketu (Banerjee: Mangal+Ketu)
        "SUNPHARMA": {"sector":"Pharma","planets":["Mars","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "CIPLA":     {"sector":"Pharma","planets":["Mars","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        "DRREDDY":   {"sector":"Pharma","planets":["Mars","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Taurus"]},
        # Auto — Mars, Ketu, Saturn (Banerjee: Mangal+Sani+Ketu)
        "MARUTI":    {"sector":"Auto","planets":["Mars","Saturn","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Gemini"]},
        "TATAMOTORS":{"sector":"Auto","planets":["Mars","Saturn","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Gemini"]},
        "BAJAJ-AUTO":{"sector":"Auto","planets":["Mars","Saturn","Ketu"],"bull_signs":["Aries","Scorpio","Capricorn"],"bear_signs":["Cancer","Libra","Gemini"]},
        # Power — Sun, Rahu (Banerjee: Surya+Rahu)
        "NTPC":      {"sector":"Power","planets":["Sun","Rahu"],"bull_signs":["Leo","Aries","Sagittarius"],"bear_signs":["Aquarius","Libra","Gemini"]},
        "POWERGRID": {"sector":"Power","planets":["Sun","Rahu"],"bull_signs":["Leo","Aries","Sagittarius"],"bear_signs":["Aquarius","Libra","Gemini"]},
        "ADANIPOWER":{"sector":"Power","planets":["Sun","Rahu"],"bull_signs":["Leo","Aries","Sagittarius"],"bear_signs":["Aquarius","Libra","Gemini"]},
        # Cement — Mars, Saturn, Rahu (Banerjee: Mangal+Sani+Rahu)
        "ULTRACEMCO":{"sector":"Cement","planets":["Mars","Saturn","Rahu"],"bull_signs":["Aries","Capricorn","Taurus"],"bear_signs":["Cancer","Libra","Gemini"]},
        "SHREECEM":  {"sector":"Cement","planets":["Mars","Saturn","Rahu"],"bull_signs":["Aries","Capricorn","Taurus"],"bear_signs":["Cancer","Libra","Gemini"]},
        # Infra/Realty — Saturn, Rahu, Ketu (Banerjee: Sani+Rahu+Ketu)
        "DLF":       {"sector":"Realty","planets":["Saturn","Rahu","Mars"],"bull_signs":["Capricorn","Aquarius","Taurus"],"bear_signs":["Cancer","Leo","Aries"]},
        "ADANIPORTS":{"sector":"Infra","planets":["Saturn","Rahu","Mars"],"bull_signs":["Capricorn","Aquarius","Taurus"],"bear_signs":["Cancer","Leo","Aries"]},
        # FMCG — Jupiter, Venus (Banerjee: Guru+Sukra)
        "HINDUNILVR":{"sector":"FMCG","planets":["Jupiter","Venus"],"bull_signs":["Cancer","Pisces","Taurus"],"bear_signs":["Virgo","Gemini","Capricorn"]},
        "ITC":       {"sector":"FMCG","planets":["Jupiter","Venus"],"bull_signs":["Cancer","Pisces","Taurus"],"bear_signs":["Virgo","Gemini","Capricorn"]},
        # Gold / Jewelry — Jupiter, Rahu (Banerjee Ch17: Guru+Rahu = Gold hyperinflation)
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

            # VR2: Gold hyperinflation — Jupiter+Rahu near (same or adjacent rashi)
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
                "Bull Reasons": " | ".join(_bull_reasons) if _bull_reasons else "—",
                "Bear Reasons": " | ".join(_bear_reasons) if _bear_reasons else "—",
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
            use_container_width=True,
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
                    "🟢 BULLISH Stocks": ", ".join(_day_bull[:6]) if _day_bull else "—",
                    "🔴 BEARISH Stocks": ", ".join(_day_bear[:6]) if _day_bear else "—",
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
                use_container_width=True, height=380, hide_index=True,
            )

    except Exception as _sd_err:
        st.warning(f"Stock dates error: {_sd_err}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════════
    # TELEGRAM CONFIG PANEL (settings)
    # ═══════════════════════════════════════════════════════════════════
    with st.expander("⚙️ Telegram Alert Settings — @streamlit123_bot", expanded=False):
        # ── Status row ────────────────────────────────────────────────
        _tg_status_cols = st.columns([2,2,2])
        _tg_status_cols[0].metric("🤖 Bot", "@streamlit123_bot")
        _tg_status_cols[1].metric("📡 Channel", "Private Channel ✅")
        _tg_status_cols[2].metric("🔑 Chat ID", TG_CHAT_ID if TG_CHAT_ID else "⏳ Auto-detecting...", delta="✅ Confirmed" if TG_CHAT_ID else None)

        if not TG_CHAT_ID:
            st.warning(
                "⚠️ Chat ID not yet resolved. **Post any message in your channel** "
                "then restart the dashboard — it will auto-detect via getUpdates."
            )
            st.info(
                "**Manual fallback:** Open this URL in your browser after posting a message in the channel, "
                "then look for chat id in the JSON response: "
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates"
            )
            _manual_cid = st.text_input("Paste Chat ID here (e.g. -1001234567890)", key="manual_chat_id")
            if st.button("💾 Save Chat ID") and _manual_cid.strip():
                try:
                    _cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
                    os.makedirs(_cache_dir, exist_ok=True)
                    open(os.path.join(_cache_dir, "tg_chat_id.txt"), "w").write(_manual_cid.strip())
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
                        st.error("❌ Send failed — check Chat ID in cache/tg_chat_id.txt")
                except Exception as _te:
                    st.error(f"Error: {_te}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════════
    # DATE PICKER — Yellow input / Red date display / Green Submit
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
    .bull-window  { background:#0d2b0d; border-left:4px solid #00C851; padding:6px 12px; border-radius:4px; margin:3px 0; }
    .bear-window  { background:#2b0d0d; border-left:4px solid #FF4444; padding:6px 12px; border-radius:4px; margin:3px 0; }
    .neutral-window { background:#1a1a00; border-left:4px solid #FFD700; padding:6px 12px; border-radius:4px; margin:3px 0; }
    .rahukaal-window { background:#1a0a2b; border-left:4px solid #9B59B6; padding:6px 12px; border-radius:4px; margin:3px 0; }
    .signal-card-bull { background:linear-gradient(135deg,#0a2e0a,#0d3d0d); border:1px solid #00C851; border-radius:8px; padding:14px; margin:6px 0; }
    .signal-card-bear { background:linear-gradient(135deg,#2e0a0a,#3d0d0d); border:1px solid #FF4444; border-radius:8px; padding:14px; margin:6px 0; }
    .signal-card-mixed { background:linear-gradient(135deg,#1a1500,#2a2200); border:1px solid #FFD700; border-radius:8px; padding:14px; margin:6px 0; }
    .time-badge-bull  { display:inline-block; background:#00C851; color:#000; font-weight:700; padding:2px 10px; border-radius:12px; font-size:13px; }
    .time-badge-bear  { display:inline-block; background:#FF4444; color:#fff; font-weight:700; padding:2px 10px; border-radius:12px; font-size:13px; }
    .time-badge-caution { display:inline-block; background:#9B59B6; color:#fff; font-weight:700; padding:2px 10px; border-radius:12px; font-size:13px; }
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
        _do_analyse = st.button("SUBMIT", key="astro_submit", use_container_width=True)

    st.divider()

    # ── Run analysis on selected date ─────────────────────────────────
    try:
        _snap = _vedic_day_analysis(_sel_date)
    except Exception as _ex:
        st.error(f"Analysis error: {_ex}")
        _snap = None

    if _snap:

        # ── BLOCK A — DAY OVERVIEW ────────────────────────────────────
        _ov       = _snap["overall"]
        _ov_color = "#00C851" if "BULLISH" in _ov else ("#FF4444" if "BEARISH" in _ov else "#FFD700")
        _card_cls = "signal-card-bull" if "BULLISH" in _ov else ("signal-card-bear" if "BEARISH" in _ov else "signal-card-mixed")
        _day_name = _sel_date.strftime("%A, %d %B %Y")
        _is_weekend = _sel_date.weekday() >= 5
        _is_holiday = _sel_date in _get_nse_holidays()
        _mkt_open   = not _is_weekend and not _is_holiday

        st.markdown(f"""
<div class="{_card_cls}">
  <h3 style="color:{_ov_color};margin:0 0 6px 0">
    {"🔴" if "BEARISH" in _ov else "🟢" if "BULLISH" in _ov else "⚠️"} &nbsp;
    {_day_name} &mdash; <span style="font-size:20px">{_ov}</span>
    &nbsp;<span style="font-size:14px;color:#aaa">({_snap["intensity"]})</span>
  </h3>
  <div style="display:flex;gap:24px;flex-wrap:wrap;margin-top:8px">
    <span>🌙 <b>Moon:</b> {_snap["moon_nak"]} ({_snap["moon_nak_lord"]}) &middot; {_snap["moon_rashi"]}</span>
    <span>📆 <b>Tithi:</b> {_snap["tithi"].split("(")[0].strip()}</span>
    <span>☀️ <b>Sun:</b> {_snap["sun_rashi"]} / {_snap["sun_nak"]}</span>
    <span>♂ <b>Mars:</b> {_snap["mars_rashi"]}</span>
    <span>♄ <b>Saturn:</b> {_snap["saturn_rashi"]}</span>
    <span>♃ <b>Jupiter:</b> {_snap["jupiter_rashi"]}</span>
    <span>⚖️ <b>Score:</b> <span style="color:{_ov_color}">{_snap["net_score"]:+d}</span> (B{_snap["bearish_pts"]}/G{_snap["bullish_pts"]})</span>
    {"<span style='color:#FF4444;font-weight:700'>🚫 HOLIDAY / WEEKEND</span>" if not _mkt_open else ""}
  </div>
</div>
""", unsafe_allow_html=True)

        # ── BLOCK B — INTRADAY TIMING ─────────────────────────────────
        st.subheader("⏱️ Intraday Timing — 30-Min Bull/Bear Windows")
        st.caption("Hora (Banerjee Ch10) + Rahukaal (Vyapar Ratna) + Nakshatra bias. "
                   "🟢 Bull · 🔴 Bear · 🟣 Rahukaal (avoid) · 🟡 Mixed")

        if not _mkt_open:
            st.warning("Market closed on this date. Analysis shown for reference only.")

        _HORA_TBL = [
            ["Sun","Venus","Mars","Mercury","Jupiter","Venus","Saturn"],
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
            "Moon":    ("BEARISH","🔴","Crude, Silver, Shipping"),
            "Mars":    ("BULLISH","🟢","Infra, Steel, Pharma"),
            "Mercury": ("BEARISH","🔴","IT, Banking, Telecom"),
            "Jupiter": ("MIXED",  "🟡","Gold, FMCG, Banking"),
            "Venus":   ("BULLISH","🟢","Silver, Jewel, FMCG"),
            "Saturn":  ("BULLISH","🟢","Infra, Realty, Metals"),
        }
        _RAHUKAAL_MAP = {
            0:("07:30","09:00"), 1:("15:00","16:30"), 2:("12:00","13:30"),
            3:("13:30","15:00"), 4:("10:30","12:00"), 5:("09:00","10:30"),
            6:("16:30","18:00"),
        }
        _WEEKDAY_RULES2 = {
            0:"Monday: Today's direction continues through Tuesday 12pm. Mon rally → Tue rises till noon then reverses (Vyapar Ratna).",
            1:"Tuesday: If bull on Tue → Wed bull till 12pm then bearish. Both Mon+Tue bull → Thu one more bull then reversal.",
            2:"Wednesday: Wed trend ends by Friday 12pm. Enter Wed → exit by Fri noon.",
            3:"Thursday: Thu bull → Fri afternoon bearish. Thu mandi ends → good rally follows (VR rule 13).",
            4:"Friday: Fri trend reverses Saturday. Fri bull → Sat bear; Fri bear → Sat bull. Unstable.",
            5:"Saturday: Weekend — NSE closed.",
            6:"Sunday: Weekend — NSE closed.",
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
            _pbias2, _pico2, _psect2 = _PBIAS2.get(_hplanet, ("MIXED","🟡","—"))
            _irk2 = _in_rk2(_sh2, _smin2)
            if _irk2:
                _bias2,_ico2,_act2,_col2,_cls2 = "RAHUKAAL","🟣","⛔ AVOID — Rahukaal","#9B59B6","rahukaal-window"
            elif _mr_override2:
                _bias2,_ico2,_act2,_col2,_cls2 = "BEARISH","🔴","❌ SHORT — Mars-Rahu active","#FF4444","bear-window"
            elif _pbias2=="BULLISH" and _snap["net_score"]>=0 and _tithi_boost2>=0:
                _bias2,_ico2,_act2,_col2,_cls2 = "BULLISH","🟢","✅ BUY / Hold LONG","#00C851","bull-window"
            elif _pbias2=="BEARISH" or _snap["net_score"]<-1 or _tithi_boost2<0:
                _bias2,_ico2,_act2,_col2,_cls2 = "BEARISH","🔴","❌ SELL / SHORT","#FF4444","bear-window"
            elif _pbias2=="BULLISH" and _snap["net_score"]<0:
                _bias2,_ico2,_act2,_col2,_cls2 = "CAUTION","🟡","⚠️ Weak bull — wait confirm","#FFD700","neutral-window"
            else:
                _bias2,_ico2,_act2,_col2,_cls2 = "MIXED","🟡","⚠️ Volatile — no edge","#FFD700","neutral-window"
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
  <span style="color:#FFD700;font-weight:700">📖 Vyapar Ratna — {_sel_date.strftime("%A")} Rule:</span>
  <span style="color:#ccc;margin-left:8px">{_WEEKDAY_RULES2[_wd2]}</span>
</div>""", unsafe_allow_html=True)

        st.divider()

        # ── BLOCK C — ACTIVE SIGNALS ──────────────────────────────────
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

        # ── BLOCK D — MULTI-SOURCE CONFIRMED SIGNAL ───────────────────
        st.subheader("✅ Multi-Source Confirmed Signal")

        _NAK_LORD3 = {
            "Ashwini":"Ketu","Bharani":"Venus","Krittika":"Sun","Rohini":"Moon",
            "Mrigasira":"Mars","Ardra":"Rahu","Punarvasu":"Jupiter","Pushya":"Saturn",
            "Ashlesha":"Mercury","Magha":"Ketu","Purva Phalguni":"Venus",
            "Uttara Phalguni":"Sun","Hasta":"Moon","Chitra":"Mars","Swati":"Rahu",
            "Vishakha":"Jupiter","Anuradha":"Saturn","Jyeshtha":"Mercury","Mula":"Ketu",
            "Purva Ashadha":"Venus","Uttara Ashadha":"Sun","Shravana":"Moon",
            "Dhanishtha":"Mars","Shatabhisha":"Rahu","Purva Bhadrapada":"Jupiter",
            "Uttara Bhadrapada":"Saturn","Revati":"Mercury",
        }
        _BEN3  = {"Sun","Jupiter","Moon","Mars"}
        _MAL3  = {"Saturn","Rahu","Ketu","Mercury","Venus"}
        _FERT3 = {"Aries","Taurus","Leo","Scorpio","Sagittarius"}
        _BARR3 = {"Gemini","Cancer","Libra","Aquarius","Pisces"}

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
        elif _bv3==3: _verd3,_vc3,_va3 = "🟢 BULLISH","High","✅ Lean LONG — buy dips"
        elif _brv3>=4:_verd3,_vc3,_va3 = "🔴 STRONG BEARISH","Very High","❌ BUY PUTS / SHORT at open"
        elif _brv3==3:_verd3,_vc3,_va3 = "🔴 BEARISH","High","❌ Lean SHORT — sell rallies"
        elif _bv3==2: _verd3,_vc3,_va3 = "🟡 MILD BULLISH","Medium","⚠️ Wait for tech confirm"
        elif _brv3==2:_verd3,_vc3,_va3 = "🟡 MILD BEARISH","Medium","⚠️ Avoid longs"
        else:         _verd3,_vc3,_va3 = "🟡 MIXED/VOLATILE","Low","⛔ Avoid — conflicting"

        if _snap["mars_rahu_deg"]<10:
            _verd3,_vc3,_va3 = "🔴 EXTREME BEARISH","Very High","❌ STRONG SHORT — Mars-Rahu <10°"
        if _snap["sun_saturn_deg"]<5:
            _verd3,_vc3 = "🔴 STRONG BEARISH","High"
            _va3 = "❌ SHORT — Sun-Saturn <5°"

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
    <span>🌙 Nak: <b style="color:{_sig_col3(_ns3)}">{_ns3}</b></span>
    <span>♈ Rashi: <b style="color:{_sig_col3(_rs3)}">{_rs3}</b></span>
    <span>☀️ Sun: <b style="color:{_sig_col3(_ss3)}">{_ss3}</b></span>
    <span>⚙️ Engine: <b style="color:{_sig_col3(_es3)}">{_es3}</b> ({_snap["net_score"]:+d})</span>
    <span>📆 Tithi: <b style="color:{_sig_col3(_ts3)}">{_ts3}</b></span>
    <span>⚡ Mars-Rahu: <b style="color:{"#FF4444" if _snap["mars_rahu_deg"]<15 else "#aaa"}">{_snap["mars_rahu_deg"]}°</b></span>
    <span>🔑 Nak Lord: <b>{_nl3}</b></span>
  </div>
</div>""", unsafe_allow_html=True)

        st.divider()

        # ── BLOCK E — POSITION GUIDE ──────────────────────────────────
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
            st.markdown("**🟣 Rahukaal — AVOID**")
            st.markdown(f'<div class="rahukaal-window"><span class="time-badge-caution">{_rk_s2}–{_rk_e2}</span> &nbsp; Do NOT enter new trades.<br><small>Source: Vyapar Ratna</small></div>', unsafe_allow_html=True)

        st.divider()

        # ── BLOCK F — SURROUNDING 5 DAYS ─────────────────────────────
        st.subheader("📆 Surrounding Trading Days — Context View")
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
                use_container_width=True, height=220,
            )

    st.divider()


with tabs[1]:
    #st.dataframe(df, width="stretch")
    #st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    st.subheader("🪐 Panchak – Full View")
    st.dataframe(panchak_view, width="stretch",height=7800)

with tabs[2]:
    
    st.markdown('<div class="section-green"><b>🟢 TOP LIVE_HIGH – Breakouts</b></div>', unsafe_allow_html=True)
    TOP_HIGH_df = (
    df.loc[df.LTP >= df.TOP_HIGH, TOP_HIGH_COLUMNS]
    .sort_values(by="GAIN", ascending=True)   # least positive gain on top
    )

    st.dataframe(TOP_HIGH_df, width="stretch", height=7000)


with tabs[3]:
    #st.dataframe(df[df.LTP <= df.TOP_LOW])
    st.markdown('<div class="section-red"><b>🔴 TOP LIVE_LOW – Breakdowns</b></div>', unsafe_allow_html=True)
    TOP_LOW_df = (
    df.loc[df.LTP <= df.TOP_LOW, TOP_LOW_COLUMNS]
    .sort_values(by="GAIN", ascending=False)   # least negative gain on top
    )

    st.dataframe(TOP_LOW_df, width="stretch", height=7000)


with tabs[4]:
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
                    near_buy_df,
                    width="stretch",
                    height=min(4200, 60 + len(near_buy_df) * 35)
                )

        with col2:
            st.markdown("### 🔴 NEAR SELL (Below EMA20)")
            if near_sell_df.empty:
                st.info("No SELL-side NEAR stocks")
            else:
                st.dataframe(
                    near_sell_df,
                    width="stretch",
                    height=min(4200, 60 + len(near_buy_df) * 35)
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
    """Current 15-min slot start e.g. '09:15', '09:30' …"""
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
        raw["datetime"] = pd.to_datetime(raw["datetime"])
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
    """Build symbol → live data dict from main df."""
    cols = ["Symbol","LTP","LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
            "CHANGE_%","YEST_HIGH","YEST_LOW","LIVE_VOLUME",
            "TOP_HIGH","TOP_LOW","NEAR","GAIN"]
    cols = [c for c in cols if c in df.columns]
    return df[cols].set_index("Symbol").to_dict("index")

def _sequential_breakout(agg, now_slot, live_map, chain_col):
    """
    FIX BUG 1 & CORE LOGIC:
    Sequential chain check for HIGH breakout and LOW breakdown.

    HIGH: every completed candle HIGH strictly > previous candle HIGH
          from the FIRST candle of the day (9:15) — no gaps allowed.
          LTP must currently be > last completed candle HIGH.

    LOW:  every completed candle LOW strictly < previous candle LOW
          from the FIRST candle of the day — no gaps.
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
    if raw is None:
        empty = pd.DataFrame()
        return empty, empty, empty

    # 15-min slots
    raw["slot15"] = raw["datetime"].dt.floor("15min").dt.strftime("%H:%M")
    agg15   = _build_ohlcv(raw, "slot15")
    now_15  = get_current_15m_slot()
    live_map = _get_live_map()

    high_df, low_df = _sequential_breakout(agg15, now_15, live_map, "CANDLES_CHAIN")

    # save combined snapshot
    combined = pd.concat([high_df, low_df], ignore_index=True)
    if not combined.empty:
        combined.to_csv(HI_LO_TRACK_CSV, index=False)

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
        vol_df.to_csv(VOL_TRACK_CSV, index=False)

    return high_df, low_df, vol_df


def build_1h_tracker(raw=None):
    """
    Sequential 1-hour tracker. Pass raw 5min DataFrame to avoid re-reading CSV.
    Slots aligned to 9:15 market open: 09:15, 10:15, 11:15, 12:15, 13:15, 14:15
    Returns: (high_df, low_df)
    """
    if raw is None:
        raw = _load_raw_5min_today()
    if raw is None:
        return pd.DataFrame(), pd.DataFrame()

    # FIX BUG 4: align 60-min slots to 9:15 market open
    # Subtract 15 min → floor to hour → add 15 min back
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
        combined.to_csv(H1_TRACK_CSV, index=False)

    return high_df, low_df


# ── Build both trackers every rerun ──────────────────────
# ✅ Load raw 5min data once and pass to both builders — avoids reading CSV twice
_raw_5min_today = _load_raw_5min_today()   # shared between 15m and 1h trackers
_15m_high_df, _15m_low_df, _vol_df = build_15m_tracker(_raw_5min_today)
_h1_high_df,  _h1_low_df           = build_1h_tracker(_raw_5min_today)

# ── Fallback: if 5-min data not yet fetched (e.g. first run of day),
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


# =========================================================
# 📧 HTML TABLE EMAIL — SEQUENTIAL BREAKOUT ALERTS
# Sends a rich HTML email with full table for each breakout
# type. Deduplicated per symbol+category per day.
# Fires every 5-min refresh only if new symbols found.
# =========================================================

# SEQ_EMAIL_DEDUP is defined at top (dated) — no redefinition needed

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
    d.to_csv(SEQ_EMAIL_DEDUP, index=False)

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

    subject = f"[OiAnalytics] {title} — {len(new_syms_list)} New | {now_str}"

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
    - Finds NEW symbols (not yet emailed today for this category)
    - Sends HTML table email
    - Fires browser toast
    - Logs to alerts log
    """
    if df_result is None or df_result.empty:
        return
    if not is_market_hours():
        return

    new_rows = []
    for _, row in df_result.iterrows():
        sym = row.get("Symbol", "")
        if sym and not _seq_email_already_sent(category, sym):
            new_rows.append(row)
            _seq_mark_email_sent(category, sym)

    if not new_rows:
        return   # nothing new to send

    new_df = pd.DataFrame(new_rows)

    # ── Browser toast ─────────────────────────────────────
    syms_str = ", ".join(new_df["Symbol"].tolist())
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
    _icon_tg  = "🚀" if up else "🔻"
    _time_tg  = datetime.now(IST).strftime("%H:%M IST")
    _tg_lines = []
    for _, _r in new_df.iterrows():
        _sym  = _r.get("Symbol","")
        _ltp  = _r.get("LTP","")
        _chain= _r.get(highlight_col, "")
        _tg_lines.append(f"  • <b>{_sym}</b>  LTP: {_ltp}  {highlight_col}: {_chain}")
    _tg_msg = (
        f"{_icon_tg} <b>{title}</b>\n"
        f"⏰ {_time_tg}\n"
        f"📋 Stocks ({len(_tg_lines)}):\n"
        + "\n".join(_tg_lines[:20]) +
        "\n\n⚠️ <i>NOT financial advice. Verify before trading.</i>"
    )
    send_telegram_bg(_tg_msg, dedup_key=f"{category}_{syms_str}_{_time_tg[:5]}")

    # ── HTML email in background thread ───────────────────
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

with tabs[5]:   # 👈 replace with correct index
    #st.markdown("## 📌 Intraday Watchlist")

    # helper: safe column selector
    def _cols(df_, want):
        return [c for c in want if c in df_.columns]

    # ═══════════════════════════════════════════════════════════════
    # ⚡ 15-MIN SEQUENTIAL HIGH BREAKOUT
    # Every 15m candle HIGH > prev HIGH from 9:15 — LTP above broken level
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
        st.dataframe(
            _15m_high_df[_show].style.background_gradient(subset=["ABOVE_%"], cmap="Greens"),
            width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 🔻 15-MIN SEQUENTIAL LOW BREAKDOWN
    # Every 15m candle LOW < prev LOW from 9:15 — LTP below broken level
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
            _15m_low_df[_show].style.background_gradient(subset=["BELOW_%"], cmap="Reds"),
            width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 📈 15-MIN VOLUME SURGE + ABOVE YESTERDAY HIGH
    # curr 15m vol > prev 15m vol AND LTP > YEST_HIGH
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📈 15-Min Volume Surge + Above Yesterday High")
    st.caption(
        "Current 15m candle volume exceeded previous 15m candle volume "
        "AND LTP is above yesterday's high — volume-confirmed breakout."
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
            _vol_df[_show].style.background_gradient(subset=["VOL_SURGE_%"], cmap="Blues"),
            width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 🕐 1-HOUR SEQUENTIAL HIGH BREAKOUT
    # Slots aligned to 9:15: 09:15 → 10:15 → 11:15 → 12:15 → 13:15 → 14:15
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🕐 1-Hour Sequential HIGH Breakout")
    st.caption(
        "Every hourly candle broke previous HIGH. "
        "Slots: 09:15 → 10:15 → 11:15 → 12:15 → 13:15 → 14:15. "
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
            _h1_high_df[_show].style.background_gradient(subset=["ABOVE_%"], cmap="Greens"),
            width='stretch'
        )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 🕐 1-HOUR SEQUENTIAL LOW BREAKDOWN
    # Every hourly candle LOW < prev LOW — LTP below broken level
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🕐 1-Hour Sequential LOW Breakdown")
    st.caption(
        "Every hourly candle broke previous LOW. "
        "Slots: 09:15 → 10:15 → 11:15 → 12:15 → 13:15 → 14:15. "
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
            _h1_low_df[_show].style.background_gradient(subset=["BELOW_%"], cmap="Reds"),
            width='stretch'
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
        #st.dataframe(table1.sort_values("VOL_%", ascending=False), width="stretch")
        st.dataframe(table1, width="stretch")

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
    # 📐 EMA7 WATCHLIST — Berlin Mindset Method (Video 6)
    # ═══════════════════════════════════════════════════════════════
    # TWO-TIMEFRAME SYSTEM:
    #   • EMA7_1H  = 1-Hour  EMA7 → BIAS direction (like checking hourly chart)
    #   • EMA7_15M = 15-Min  EMA7 → ENTRY timing  (like checking 15m chart)
    #
    # Berlin Rule:
    #   LTP > EMA7_1H  → bullish bias → look for LONG on 15m retest of EMA7_15M
    #   LTP < EMA7_1H  → bearish bias → look for SHORT on 15m bounce to EMA7_15M
    #   Entry only after 15m candle CLOSES on the correct side — no mid-candle entry
    # ═══════════════════════════════════════════════════════════════

    st.divider()
    st.markdown("## 📐 EMA7 Watchlist — Berlin Mindset (1H Bias + 15M Entry)")

    # ── Check which EMA columns are available ─────────────────────
    _has_1h  = "EMA7_1H"  in df.columns and df["EMA7_1H"].notna().any()
    _has_15m = "EMA7_15M" in df.columns and df["EMA7_15M"].notna().any()
    _has_d   = "EMA7"     in df.columns and df["EMA7"].notna().any()

    # Decide which columns to use — prefer intraday, fall back to daily
    _bias_col  = "EMA7_1H"  if _has_1h  else ("EMA7" if _has_d else None)
    _entry_col = "EMA7_15M" if _has_15m else ("EMA7" if _has_d else None)

    # FIX 4: detect full-fallback mode (both use daily EMA7 = same value)
    _full_fallback = (_bias_col == _entry_col == "EMA7")

    # Status info bar
    _c1, _c2, _c3 = st.columns(3)
    with _c1:
        if _has_1h:
            st.success("✅ 1H EMA7 live — using EMA7_1H for bias")
        else:
            st.warning("⚠️ 1H EMA7 not ready — Daily EMA7 used as bias (fallback)")
    with _c2:
        if _has_15m:
            st.success("✅ 15M EMA7 live — using EMA7_15M for entry")
        else:
            st.warning("⚠️ 15M EMA7 not ready — Daily EMA7 used as entry (fallback)")
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
            "DIST_BIAS_% and DIST_ENTRY_% both use **Daily EMA7** — they show the same value. "
            "Once 15-min candle fetch completes (~09:16), live 1H and 15M EMA7 replace this automatically."
        )

    st.caption(
        "**1H EMA7 = trend bias** (above → LONG only · below → SHORT only).  "
        "**15M EMA7 = entry level** (wait for price to retest it and candle to CLOSE on correct side).  "
        "**DIST_BIAS_%** = LTP distance from 1H EMA7 (+ above = bullish · - below = bearish).  "
        "**DIST_ENTRY_%** = LTP distance from 15M EMA7 — closest to 0% = best entry candidate.  "
        "Tables sorted by DIST_ENTRY_% — tightest retest at the top."
    )

    if _bias_col is None:
        st.error("❌ No EMA7 data available. Market data not loaded — check CACHE folder.")
    else:
        _e7 = df.copy()

        # Ensure numeric
        for _c in [_bias_col, _entry_col, "LTP", "CHANGE_%", "VOL_%", "EMA20"]:
            if _c in _e7.columns:
                _e7[_c] = pd.to_numeric(_e7[_c], errors="coerce")

        _e7["LTP"]      = pd.to_numeric(_e7["LTP"],      errors="coerce")
        _e7["CHANGE_%"] = pd.to_numeric(_e7.get("CHANGE_%", pd.Series(dtype=float)), errors="coerce").fillna(0)
        _e7["VOL_%"]    = pd.to_numeric(_e7.get("VOL_%",    pd.Series(dtype=float)), errors="coerce").fillna(0)

        # ── Core distance columns ──────────────────────────────────
        # Guard: replace 0 with NA to prevent division by zero → NaN result
        _e7["DIST_BIAS_%"] = (
            (_e7["LTP"] - _e7[_bias_col]) / _e7[_bias_col].replace(0, pd.NA) * 100
        ).round(2)
        _e7["DIST_ENTRY_%"] = (
            (_e7["LTP"] - _e7[_entry_col]) / _e7[_entry_col].replace(0, pd.NA) * 100
        ).round(2)

        # EMA7_1H slope: rising when EMA7_1H > EMA20 → confirms momentum direction
        if "EMA20" in _e7.columns:
            _e7["1H_SLOPE"] = (_e7[_bias_col] > _e7["EMA20"]).map(
                {True: "↑ Rising", False: "↓ Falling"}
            )
        else:
            _e7["1H_SLOPE"] = "—"

        # FIX 5: direction-aware ENTRY_STATUS
        # LONG:  LTP is above 15M EMA7 (positive DIST_ENTRY_%) → pulling back toward it
        # SHORT: LTP is below 15M EMA7 (negative DIST_ENTRY_%) → bouncing up toward it
        def _entry_status_long(d15):
            if pd.isna(d15): return "—"
            a = abs(d15)
            if   a <= 0.3: return "🔥 AT 15M EMA7 — Enter now"
            elif a <= 1.0: return "✅ Near — Watch 15m candle close"
            elif a <= 2.0: return "⏳ Pulling Back — Standby"
            else:          return "⌛ Far above — Not yet retesting"

        def _entry_status_short(d15):
            if pd.isna(d15): return "—"
            a = abs(d15)
            if   a <= 0.3: return "🔥 AT 15M EMA7 — Enter now"
            elif a <= 1.0: return "✅ Near — Watch 15m candle close"
            elif a <= 2.0: return "⏳ Bouncing Up — Standby"
            else:          return "⌛ Far below — Not yet retesting"

        _e7["ENTRY_STATUS"] = _e7["DIST_ENTRY_%"].apply(
            lambda d: _entry_status_long(d) if (not pd.isna(d) and d >= 0)
                      else _entry_status_short(d)
        )

        # ALREADY_IN: strong move off EMA7 on correct side with momentum
        def _already_in(d_bias, d_entry, chg, slope):
            if pd.isna(d_bias) or pd.isna(d_entry): return "—"
            if d_bias > 2.0 and d_entry > 1.5 and chg > 0.5 and "Rising" in str(slope):
                return "📌 Possibly In (Long)"
            if d_bias < -2.0 and d_entry < -1.5 and chg < -0.5 and "Falling" in str(slope):
                return "📌 Possibly In (Short)"
            return "—"

        _e7["ALREADY_IN?"] = _e7.apply(
            lambda r: _already_in(
                r["DIST_BIAS_%"], r["DIST_ENTRY_%"],
                r["CHANGE_%"], r.get("1H_SLOPE", "")
            ), axis=1
        )

        # Display columns
        _E7_SHOW = [c for c in [
            "Symbol", "LTP",
            _bias_col,  "DIST_BIAS_%",   "1H_SLOPE",
            _entry_col, "DIST_ENTRY_%",  "ENTRY_STATUS", "ALREADY_IN?",
            "CANDLES_1H", "CANDLES_15M",
            "CHANGE_%", "VOL_%",
            "LIVE_HIGH", "LIVE_LOW", "YEST_HIGH", "YEST_LOW",
            "EMA20", "NEAR", "TOP_HIGH", "TOP_LOW"
        ] if c in _e7.columns]

        # ─────────────────────────────────────────────────────────
        # TABLE 1: 🟢 LONG CANDIDATES
        # ─────────────────────────────────────────────────────────
        st.markdown("### 🟢 LONG Candidates — Bullish Bias (LTP above 1H EMA7)")
        st.caption(
            "**Step 1:** LTP > 1H EMA7 ✅ → Bullish bias confirmed. Only plan LONG.  "
            "**Step 2:** Watch 15M EMA7 (DIST_ENTRY_%) — wait for price to pull back near it.  "
            "**Step 3:** Wait for 15m candle to CLOSE above 15M EMA7 — confirms rejection.  "
            "**SL:** Below 15M EMA7. **Target:** 1:3 RR minimum.  "
            "🔥 AT EMA7 = enter zone · ✅ Near = watch candle close · ⏳ = standby"
        )

        # Bug A fix: warn when 1H EMA7 is based on < 4 candles (unreliable early morning)
        if _has_1h and "CANDLES_1H" in df.columns:
            _min_1h = df["CANDLES_1H"].dropna().min()
            if not pd.isna(_min_1h) and _min_1h < 4:
                st.warning(
                    f"⚠️ **EMA7_1H unreliable** — only {int(_min_1h)} completed 1H candle(s). "
                    "EMA7 needs ≥ 4 candles to be meaningful. "
                    "Use Daily EMA7 as bias reference until after 13:15."
                )

        _long_df = _e7[
            (_e7["DIST_BIAS_%"]  > 0) &
            (_e7["DIST_ENTRY_%"] <= 3.0) &
            (_e7["DIST_ENTRY_%"] > -0.5) &
            (_e7["VOL_%"] >= -60)
        ][_E7_SHOW].copy()
        _long_df = _long_df.sort_values("DIST_ENTRY_%", ascending=True)

        if _long_df.empty:
            st.info("No LONG candidates right now. Wait for stocks to pull back to 15M EMA7 while above 1H EMA7.")
        else:
            # Bug B fix: row colour has priority — use .bar() for visual on DIST_ENTRY_%
            # instead of background_gradient which overwrites row-level highlight colours
            def _hl_long(row):
                s = str(row.get("ENTRY_STATUS", ""))
                if "🔥" in s: return ["background-color:#b6f0b6; font-weight:bold"] * len(row)
                if "✅" in s: return ["background-color:#e6fae6"] * len(row)
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
                    .format(_fmt_long, na_rep="—"),
                width="stretch"
            )
            _fire  = (_long_df["ENTRY_STATUS"].str.contains("🔥", na=False)).sum()
            _near  = (_long_df["ENTRY_STATUS"].str.contains("✅", na=False)).sum()
            _in_p  = (_long_df["ALREADY_IN?"] != "—").sum()
            st.caption(
                f"🟢 {len(_long_df)} LONG candidates  |  "
                f"🔥 {_fire} at 15M EMA7 zone  |  "
                f"✅ {_near} near — watch 15m close  |  "
                f"📌 {_in_p} possibly already in"
            )

        st.divider()

        # ─────────────────────────────────────────────────────────
        # TABLE 2: 🔴 SHORT CANDIDATES
        # ─────────────────────────────────────────────────────────
        st.markdown("### 🔴 SHORT Candidates — Bearish Bias (LTP below 1H EMA7)")
        st.caption(
            "**Step 1:** LTP < 1H EMA7 ✅ → Bearish bias confirmed. Only plan SHORT.  "
            "**Step 2:** Watch 15M EMA7 (DIST_ENTRY_%) — wait for price to bounce up near it.  "
            "**Step 3:** Wait for 15m candle to CLOSE below 15M EMA7 — confirms rejection.  "
            "**SL:** Above 15M EMA7. **Target:** 1:3 RR minimum.  "
            "🔥 AT EMA7 = enter zone · ✅ Near = watch candle close · ⏳ = standby"
        )

        # Bug A fix: same warning for SHORT side
        if _has_1h and "CANDLES_1H" in df.columns:
            _min_1h_s = df["CANDLES_1H"].dropna().min()
            if not pd.isna(_min_1h_s) and _min_1h_s < 4:
                st.warning(
                    f"⚠️ **EMA7_1H unreliable** — only {int(_min_1h_s)} completed 1H candle(s). "
                    "Use Daily EMA7 as bias until after 13:15."
                )

        _short_df = _e7[
            (_e7["DIST_BIAS_%"]  < 0) &
            (_e7["DIST_ENTRY_%"] >= -3.0) &
            (_e7["DIST_ENTRY_%"] < 0.5) &
            (_e7["VOL_%"] >= -60)
        ][_E7_SHOW].copy()
        _short_df = _short_df.sort_values("DIST_ENTRY_%", ascending=False)

        if _short_df.empty:
            st.info("No SHORT candidates right now. Wait for stocks to bounce toward 15M EMA7 while below 1H EMA7.")
        else:
            # Bug B fix: same pattern as LONG — row colour priority, no gradient override
            def _hl_short(row):
                s = str(row.get("ENTRY_STATUS", ""))
                if "🔥" in s: return ["background-color:#ffc5c5; font-weight:bold"] * len(row)
                if "✅" in s: return ["background-color:#fff0f0"] * len(row)
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
                    .format(_fmt_short, na_rep="—"),
                width="stretch"
            )
            _fire_s = (_short_df["ENTRY_STATUS"].str.contains("🔥", na=False)).sum()
            _near_s = (_short_df["ENTRY_STATUS"].str.contains("✅", na=False)).sum()
            _in_s   = (_short_df["ALREADY_IN?"] != "—").sum()
            st.caption(
                f"🔴 {len(_short_df)} SHORT candidates  |  "
                f"🔥 {_fire_s} at 15M EMA7 zone  |  "
                f"✅ {_near_s} near — watch 15m close  |  "
                f"📌 {_in_s} possibly already in"
            )

        st.divider()

        # ─────────────────────────────────────────────────────────
        # QUICK REFERENCE
        # ─────────────────────────────────────────────────────────
        with st.expander("📖 Berlin EMA7 Two-Timeframe Rules — Quick Reference"):
            st.markdown(f"""
**Two-Timeframe System**

| Timeframe | Column | Role |
|-----------|--------|------|
| **1-Hour EMA7** | `{_bias_col}` | **Bias direction** — are we bullish or bearish today? |
| **15-Min EMA7** | `{_entry_col}` | **Entry level** — where exactly do we enter the trade? |
| **Daily EMA7** | `EMA7` | Fallback when intraday data not ready (pre-9:15) |

**Entry Steps (Berlin Method)**

| Step | Action |
|------|--------|
| 1 | Open **hourly view** → is LTP **above** or **below** `{_bias_col}`? |
| 2 | **Above** → LONG bias. **Below** → SHORT bias. Never trade against this. |
| 3 | Switch to **15-min view** → wait for price to reach `{_entry_col}` |
| 4 | Watch for **rejection candle** — wick touches EMA7, body closes away |
| 5 | Enter **only after candle closes** — no mid-candle entries |
| 6 | If 15m chart contradicts 1H bias → **skip the trade entirely** |

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
| `DIST_ENTRY_%` | LTP distance from **15M EMA7** — closest to 0 = best retest setup right now |
| `1H_SLOPE` | ↑ Rising = EMA7 above EMA20 (strong bull) · ↓ Falling (strong bear) |
| `ENTRY_STATUS` | 🔥 AT 15M EMA7 · ✅ Near — watch close · ⏳ Approaching · ⌛ Wait |
| `ALREADY_IN?` | 📌 = price moved strongly off EMA7, check if you have an open position |
| `CANDLES_1H` | How many completed 1H candles used for EMA7 calculation |
| `CANDLES_15M` | How many completed 15M candles used for EMA7 calculation |

**Data refresh:** EMA7_1H and EMA7_15M are stored in `CACHE/ema7_1hour_YYYYMMDD.csv` and
`CACHE/ema7_15min_YYYYMMDD.csv`. Auto-rebuilt every **60 seconds** during market hours (09:15–15:35).
            """)

with tabs[6]:   # 👈 replace with correct index

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

    st.markdown('<div class="oi-header">🧠 NIFTY — OI INTELLIGENCE &nbsp;|&nbsp; 🔄 Auto updates every 5 min</div>', unsafe_allow_html=True)

    oi_intel = fetch_nifty_oi_intelligence()

    if oi_intel is None:
        st.warning("⚠️ OI Intelligence data unavailable. Check NFO connection or market hours.")
    else:
        ts_str      = oi_intel.get("timestamp", "—")
        spot        = oi_intel.get("spot", 0)
        fut_ltp     = oi_intel.get("fut_ltp", 0)
        atm         = oi_intel.get("atm", 0)
        max_pain    = oi_intel.get("max_pain", 0)
        pcr         = oi_intel.get("pcr", 0)
        direction   = oi_intel.get("direction", "—")
        dir_reason  = oi_intel.get("direction_reason", "")
        pain_signal = oi_intel.get("pain_signal", "")
        advice      = oi_intel.get("advice", "—")
        setup       = oi_intel.get("setup", "—")
        expiry      = oi_intel.get("expiry", "—")
        s_ce        = oi_intel.get("strongest_ce", 0)
        ce_oi_val   = oi_intel.get("ce_oi", 0)
        s_pe        = oi_intel.get("strongest_pe", 0)
        pe_oi_val   = oi_intel.get("pe_oi", 0)
        sh_ce       = oi_intel.get("shifting_ce", 0)
        sh_ce_add   = oi_intel.get("shifting_ce_add", 0)
        sh_pe       = oi_intel.get("shifting_pe", 0)
        sh_pe_add   = oi_intel.get("shifting_pe_add", 0)
        ncw         = oi_intel.get("nearest_call_wall", "—")
        npf         = oi_intel.get("nearest_put_floor", "—")
        d_call      = oi_intel.get("dist_to_call", "—")
        d_put       = oi_intel.get("dist_to_put", "—")

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
            <span class="oi-dim">Above spot → Resistance</span>
            </div>
            """, unsafe_allow_html=True)

        with col_s2:
            st.markdown(f"""
            <div class="oi-card">
            <b>🟢 STRONGEST PUT FLOOR</b><br>
            Strike: <span class="oi-bull">{s_pe}</span><br>
            OI: <b>{pe_oi_val:,}</b><br>
            <span class="oi-dim">Below spot → Support</span>
            </div>
            """, unsafe_allow_html=True)

        with col_s3:
            st.markdown(f"""
            <div class="oi-card">
            <b>📥 SHIFTING OI — CALLS</b><br>
            Strike: <span class="oi-bear">{sh_ce}</span><br>
            OI Added Today: <b>{sh_ce_add:,}</b><br>
            <span class="oi-dim">New CE writing happening here</span>
            </div>
            """, unsafe_allow_html=True)

        with col_s4:
            st.markdown(f"""
            <div class="oi-card">
            <b>📥 SHIFTING OI — PUTS</b><br>
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
            _ev_slot  = _oi_15m_events[0]["SLOT"] if _oi_15m_events else "—"

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
                    f' &nbsp;·&nbsp; OI: {ev["OI_PREV"]:,} → {ev["OI_CURR"]:,} &nbsp; {oi_delta_str}'
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
            _f_slot = _fut_15m_events[0]["SLOT"] if _fut_15m_events else "—"
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
                        f'&nbsp;&nbsp;OI: {ev["OI_PREV"]:,} → {ev["OI_CURR"]:,}'
                        f'&nbsp;&nbsp;<b style="color:{oi_col};">{ev["OI_DELTA"]:+,} ({ev["OI_DELTA_%"]:.1f}%)</b>'
                        f'&nbsp;|&nbsp;'
                        f'<span style="color:#bdbdbd;">₹{ev["LTP_PREV"]:.1f}→₹{ev["LTP_CURR"]:.1f}</span>'
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
        real_sl_call   = ncw if ncw and ncw != "—" else (s_ce if s_ce > spot else atm + step)
        real_sl_call   = real_sl_call if isinstance(real_sl_call, (int, float)) else atm + step

        # Nearest put floor BELOW spot (for target reference)
        real_tgt_put   = npf if npf and npf != "—" else (s_pe if s_pe < spot else atm - step)
        real_tgt_put   = real_tgt_put if isinstance(real_tgt_put, (int, float)) else atm - step

        # ── Pattern + Setup: single unified decision block ────
        # All values flow from: OI shift data → pattern → direction → bias → SL/target
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
                f"Call writing at {shifting_ce} CE — OI added {shifting_ce_add:,} "
                f"({shifting_ce_pct}% of strike OI in session), premium ₹{shifting_ce_ltp:.1f}. "
                f"Put support crumbling at {most_dropped_pe} PE — "
                f"OI dropped {abs(most_dropped_pe_val):,} ({abs(most_dropped_pe_pct)}% of strike OI in session), "
                f"premium ₹{most_dropped_pe_ltp:.1f}."
            )
            trade_bias = "BEARISH"

        elif _pe_bld and _ce_crm:
            pattern_label  = "🚀 SHORT SQUEEZE: PUT WALL BUILDING + CALL RESISTANCE CRUMBLING"
            pattern_detail = (
                f"Put writing at {shifting_pe} PE — OI added {shifting_pe_add:,} "
                f"({shifting_pe_pct}% of strike OI in session), premium ₹{shifting_pe_ltp:.1f}. "
                f"Call resistance crumbling at {most_dropped_ce} CE — "
                f"OI dropped {abs(int(oi_intel.get('near_ce_added',0))):,} "
                f"({abs(near_ce_pct_v):.1f}% in session)."
            )
            trade_bias = "BULLISH"

        elif _ce_bld and _pe_bld:
            pattern_label  = "⚖️ OI ACCUMULATION: BOTH SIDES WRITING (Indecision)"
            pattern_detail = (
                f"CE OI added {shifting_ce_add:,} (+{shifting_ce_pct}%) at {shifting_ce} CE. "
                f"PE OI added {shifting_pe_add:,} (+{shifting_pe_pct}%) at {shifting_pe} PE. "
                f"Market trapped — wait for breakout."
            )
            trade_bias = "NEUTRAL"

        elif _ce_bld:
            pattern_label  = "⚠️ CALL WALL BUILDING — Put support intact"
            pattern_detail = (
                f"Call writing at {shifting_ce} CE — OI added {shifting_ce_add:,} "
                f"({shifting_ce_pct}% of strike OI), premium ₹{shifting_ce_ltp:.1f}. "
                f"Put floor at {s_pe} PE still holding ({pe_oi_val:,} OI)."
            )
            trade_bias = "BEARISH" if "BEARISH" in direction else "NEUTRAL"

        elif _pe_bld:
            pattern_label  = "✅ PUT FLOOR BUILDING — Call resistance intact"
            pattern_detail = (
                f"Put writing at {shifting_pe} PE — OI added {shifting_pe_add:,} "
                f"({shifting_pe_pct}% of strike OI), premium ₹{shifting_pe_ltp:.1f}. "
                f"Call wall at {s_ce} CE still holding ({ce_oi_val:,} OI)."
            )
            trade_bias = "BULLISH" if "BULLISH" in direction else "NEUTRAL"

        elif _ce_crm and not _pe_bld:
            pattern_label  = "📈 CALL WALL UNWINDING — Resistance weakening"
            pattern_detail = (
                f"Call OI dropping near ATM ({near_ce_pct_v:+.1f}% change) — "
                f"writers covering shorts. Bullish signal if spot holds above {npf if npf and npf != '—' else s_pe}."
            )
            trade_bias = "BULLISH"

        elif _pe_crm and not _ce_bld:
            pattern_label  = "📉 PUT FLOOR CRUMBLING — Support weakening"
            pattern_detail = (
                f"Put OI dropping near ATM ({near_pe_drop_v:+.1f}% change) — "
                f"support being withdrawn. Bearish signal if spot stays below {ncw if ncw and ncw != '—' else s_ce}."
            )
            trade_bias = "BEARISH"

        elif "BEARISH" in direction:
            pattern_label  = "🔴 BEARISH REGIME — Call pressure dominant"
            pattern_detail = (
                f"Strongest call wall: {s_ce} CE ({ce_oi_val:,} OI) — "
                f"{int(d_call)} pts above spot. "
                f"PCR={pcr}. Nearest put floor: {npf if npf and npf != '—' else s_pe}."
            )
            trade_bias = "BEARISH"

        elif "BULLISH" in direction:
            pattern_label  = "🟢 BULLISH REGIME — Put support dominant"
            pattern_detail = (
                f"Strongest put floor: {s_pe} PE ({pe_oi_val:,} OI) — "
                f"{int(d_put)} pts below spot. "
                f"PCR={pcr}. Nearest call wall: {ncw if ncw and ncw != '—' else s_ce}."
            )
            trade_bias = "BULLISH"

        else:
            pattern_label  = "📊 OI REGIME: NEUTRAL / BALANCED"
            pattern_detail = (
                f"No dominant OI shift near ATM. "
                f"CE OI: {near_ce_pct_v:+.1f}% | PE OI: {near_pe_pct_v:+.1f}% | "
                f"PE drop: {near_pe_drop_v:+.1f}% | PCR={pcr}. "
                f"Range: {npf if npf and npf != '—' else s_pe} – {ncw if ncw and ncw != '—' else s_ce}."
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
            if reward_r < risk_r and len(_floors) > 1:   # R:R < 1:1 → use 2nd floor
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
            if reward_r < risk_r and len(_walls) > 1:  # R:R < 1:1 → use 2nd wall
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
            setup_line  = f"WAIT — Range: {int(_npf)} – {int(_ncw)}"
            sl_line     = "—"
            target_line = "—"
            advice_text = "Range-bound. Trade breakouts only. Avoid directional bets."

        risk_pts   = int(risk_r)   if trade_bias != "NEUTRAL" else 0
        reward_pts = int(reward_r) if trade_bias != "NEUTRAL" else 0
        rr_ratio   = f"1:{round(reward_pts/risk_pts,1)}" if risk_pts > 0 else "—"

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
            regime_banner = f"🔄 REGIME CHANGE: {prev_direction} → {direction}"

        # ── Render Advisory Card ──────────────────────────────
        regime_html = (
            f'<div style="background:#ff6f00;color:#fff;padding:6px 12px;'
            f'border-radius:6px;font-weight:900;font-size:13px;margin-bottom:10px;">'
            f'{regime_banner}</div>'
        ) if regime_banner else ""

        # Top call walls / put floors list for display
        walls_str  = "  ".join([f"{int(s)} ({oi:,})" for s, oi in call_walls_above[:3]]) or "—"
        floors_str = "  ".join([f"{int(s)} ({oi:,})" for s, oi in put_floors_below[:3]]) or "—"

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
    # END OF OI INTELLIGENCE PANEL — BREAKOUT SCREENER BELOW
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
        st.dataframe(table1, width="stretch")

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
        st.dataframe(table10, width="stretch")

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
        st.dataframe(table11, width="stretch")



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

with tabs[7]:   # change tab index if required

    st.subheader("🟢 Green Open Structure")
    st.dataframe(green_structure_df, width='stretch')
    #st.divider()

    st.subheader("🔴 Red Open Structure")
    st.dataframe(red_structure_df, width='stretch')

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
                        "background-color:#d4f8d4; color:#006400; font-weight:bold;"
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
                        "background-color:#ffd6d6; color:#8b0000; font-weight:bold;"
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

        styled_high = strong_high_df.style.apply(
            highlight_high_breaks,
            axis=1
        ).format({
            "LTP": "{:.2f}",
            "CHANGE_%": "{:.2f}",
            "VOL_%": "{:.2f}",
            "DIST_FROM_DAY_HIGH_%": "{:.2f}",
        })

        st.dataframe(
            styled_high,
            width='stretch',
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

        styled_low = strong_low_df.style.apply(
            highlight_low_breaks,
            axis=1
        ).format({
            "LTP": "{:.2f}",
            "CHANGE_%": "{:.2f}",
            "VOL_%": "{:.2f}",
            "DIST_FROM_DAY_LOW_%": "{:.2f}",
        })

        st.dataframe(
            styled_low,
            width='stretch',
            height=450
        )

    st.subheader("📈 DAILY BREAKS – Above YEST HIGH")
    st.dataframe(daily_up, width="stretch")

    st.subheader("📉 DAILY BREAKS – Below YEST LOW")
    st.dataframe(daily_down, width="stretch")



with tabs[8]:
    st.subheader("🕘 1H Opening Range Breakouts")
    st.dataframe(hourly_break_df, width='stretch')

    st.markdown("## 🔥 Gap Failure Reclaim Scanner")

    # Bullish
    if bull_reclaim_df.empty:
        st.info("No bullish reclaim setups")
    else:
        st.markdown("### 🟢 Bullish Reclaim (Gap Up Failure Recovery)")
        st.dataframe(
            bull_reclaim_df[
                ["Symbol","LTP","LIVE_OPEN","YEST_HIGH","LIVE_LOW","STRENGTH_%"]
            ],
            width='stretch'
        )

    # Bearish
    if bear_reclaim_df.empty:
        st.info("No bearish reclaim setups")
    else:
        st.markdown("### 🔴 Bearish Reclaim (Gap Down Failure Continuation)")
        st.dataframe(
            bear_reclaim_df[
                ["Symbol","LTP","LIVE_OPEN","YEST_LOW","LIVE_HIGH","STRENGTH_%"]
            ],
            width='stretch'
        )


    st.subheader("📊 WEEKLY BREAKS – Above WEEK HIGH")
    st.dataframe(weekly_up, width="stretch")

    st.subheader("📊 WEEKLY BREAKS – BeLIVE_LOW WEEK LOW")
    st.dataframe(weekly_down, width="stretch")

    #st.divider()

    #st.markdown("### ✅ WEEKLY + EMA CONFIRMATION (Strong Trend)")

    #col1, col2 = st.columns(2)

    #with col1:
     #   st.markdown("🟢 BUY : WEEK HIGH + EMA20 > EMA50")
      #  if weekly_ema_buy.empty:
       #     st.info("No Weekly EMA BUY confirmations")
        #else:
         #   st.dataframe(weekly_ema_buy, width="stretch")

    #with col2:
     #   st.markdown("🔴 SELL : WEEK LOW + EMA20 < EMA50")
      #  if weekly_ema_sell.empty:
       #     st.info("No Weekly EMA SELL confirmations")
        #else:
         #   st.dataframe(weekly_ema_sell, width="stretch")


with tabs[9]:
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


    st.subheader("📈 2M Downtrend → EMA Reversal Setup")

    if reversal_df.empty:
        st.info("No stocks matching this condition")
    else:
        st.dataframe(reversal_df, width='stretch')

    #st.dataframe(df[df.LTP >= df.HIGH_M])
    #st.dataframe(df[df.LTP <= df.LOW_M])
    st.subheader("📅 WEEKLY %")
    st.dataframe(weekly_break_df, width="stretch")

    st.subheader("📅 MONTHLY %")
    st.dataframe(monthly_break_df, width="stretch")

    st.subheader("📅 MONTHLY BREAKS – Above MONTH HIGH")
    monthly_up = df.loc[df.LTP >= df.HIGH_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_up, width="stretch")

    st.subheader("📅 MONTHLY BREAKS – Below MONTH LOW")
    monthly_down = df.loc[df.LTP <= df.LOW_M, MONTHLY_COLUMNS]
    st.dataframe(monthly_down, width="stretch")


with tabs[10]:
    
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
        #width='content'
        width="stretch",
        height=min(3200, 60 + len(ol_oh_df) * 35)
    )
    col1, col2 = st.columns(2)

    # -------- LIVE_OPEN = LIVE_LOW (Bullish) --------
    with col1:
        st.markdown("### 🟢 OPEN==LOW ")
        if LIVE_OPEN_LIVE_LOW_df.empty:
            st.info("No OPEN==LOW stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_LOW_df,
                width="stretch",
                height=min(3000, 60 + len(LIVE_OPEN_LIVE_LOW_df) * 35)
            )

    # -------- LIVE_OPEN = LIVE_HIGH (Bearish) --------
    with col2:
        st.markdown("### 🔴 OPEN==HIGH ")
        if LIVE_OPEN_LIVE_HIGH_df.empty:
            st.info("No OPEN==HIGH stocks today")
        else:
            st.dataframe(
                LIVE_OPEN_LIVE_HIGH_df,
                width="stretch",
                height=min(3000, 60 + len(LIVE_OPEN_LIVE_HIGH_df) * 35)
            )


#with tabs[9]:
 #   st.markdown('<div class="section-purple"><b>📉 EMA20–EMA50 + Breakout</b></div>', unsafe_allow_html=True)

  #  if ema_signal_df.empty:
   #     st.info("No EMA20–EMA50 signals currently")
   # else:
    #    st.dataframe(
     #       ema_signal_df,
      #      width="stretch",
       #     height=min(1200, 60 + len(ema_signal_df) * 35)
       # )

with tabs[11]:
    st.markdown(
        '<div class="section-green"><b>🟢 EMA20–EMA50 BUY (Breakout)</b></div>',
        unsafe_allow_html=True
    )

    if ema_buy_df.empty:
        st.info("No EMA20–EMA50 BUY signals")
    else:
        st.dataframe(
            ema_buy_df,
            width="stretch",
            height=min(2000, 60 + len(ema_buy_df) * 35)
        )

    st.markdown(
        '<div class="section-red"><b>🔴 EMA20–EMA50 SELL (Breakdown)</b></div>',
        unsafe_allow_html=True
    )

    if ema_sell_df.empty:
        st.info("No EMA20–EMA50 SELL signals")
    else:
        st.dataframe(
            ema_sell_df,
            width="stretch",
            height=min(2000, 60 + len(ema_sell_df) * 35)
        )

with tabs[12]:  # assuming INFO is last tab

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
            gainers_df[TOP_GAINER_COLS],
            #width='stretch'
            width="stretch",
            height=min(2200, 60 + len(gainers_df) * 35)
        )

    TOP_LOSER_COLS = [
    "Symbol",
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
            losers_df[TOP_LOSER_COLS],
            #width='stretch'
            width="stretch",
            height=min(2200, 60 + len(losers_df) * 35)
        )


with tabs[13]:  # 4 BAR
    st.markdown("### 🔁 4 BAR Reversal + Breakout")

    if four_bar_df.empty:
        st.info("No 4-bar setups today")
    else:
        st.dataframe(four_bar_df, width="stretch")

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
                .style.apply(style_ltp_only, axis=1),
                width="stretch",
                height=min(2000, 60 + len(fake_bull_df) * 35)
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
                .style.apply(style_ltp_bear_only, axis=1),
                width="stretch",
                height=min(2000, 60 + len(fake_bear_df) * 35)
        )

    ##############################################
    st.markdown("### ⏱️ 15-Min Inside Range Break")

    if inside_15m_df.empty:
        st.info("No 15-min inside range breaks yet")
    else:
        st.dataframe(
            inside_15m_df
                .sort_values("CHANGE_%", ascending=False)
                .style.apply(style_ltp_15min, axis=1),
            width="stretch",
            height=min(2000, 60 + len(inside_15m_df) * 35)
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




with tabs[14]:

    
    st.subheader("🚀 EARLY HIGH-GAIN RUNNERS (YH MOMENTUM)")

    if early_runner_df.empty:
        st.info("No strong early runners yet")
    else:
        st.dataframe(
            early_runner_df,
            width='stretch'
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

        #st.dataframe(fut_df[display_cols].sort_values("OI_SCORE", ascending=False),width='stretch')
        # Remove NEUTRAL positions
        filtered_fut_df = fut_df[
            fut_df["POSITION_TYPE"] != "NEUTRAL"
        ].copy()

        # Optional: also remove weak OI_SCORE
        filtered_fut_df = filtered_fut_df[
            filtered_fut_df["OI_SCORE"].abs() > 1
        ]

        st.dataframe(
            filtered_fut_df.sort_values("OI_SCORE", ascending=False),
            width="stretch"
        )


    st.markdown("## 🔥 Strong Futures Closing")

    if strong_long_close.empty and strong_short_close.empty:
        st.info("No strong closing futures detected")

    if not strong_long_close.empty:
        st.markdown("### 🟢 Strong Long Closing (Near Day High)")
        st.dataframe(
            strong_long_close[
                ["FUT_SYMBOL","LTP","PRICE_%","REAL_OI_%","DIST_FROM_HIGH_%"]
            ],
            width='stretch'
        )

    if not strong_short_close.empty:
        st.markdown("### 🔴 Strong Short Closing (Near Day Low)")
        st.dataframe(
            strong_short_close[
                ["FUT_SYMBOL","LTP","PRICE_%","REAL_OI_%","DIST_FROM_LOW_%"]
            ],
            width='stretch'
        )



with tabs[15]:
    st.subheader("📊 NSE INDICES – LIVE")

    # Build display — use whichever name column is present
    _idx_display_cols = []
    for _c in ["Index","Symbol","LTP","OPEN","HIGH","LOW","CHANGE","CHANGE_%",
               "LIVE_OPEN","LIVE_HIGH","LIVE_LOW",
               "YEST_HIGH","YEST_LOW","YEST_CLOSE"]:
        if _c in indices_df.columns:
            _idx_display_cols.append(_c)
    st.dataframe(
        indices_df[_idx_display_cols] if _idx_display_cols else indices_df,
        width='stretch'
    )

    st.subheader("📊 Index-wise Top Gainers (Live)")

    for index_name, symbols in index_symbols.items():

        idx_df = df[df["Symbol"].isin(symbols)].copy()

        if idx_df.empty:
            continue

        idx_df = idx_df[
            [
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
            ]
        ].sort_values("CHANGE_%", ascending=False)

        st.markdown(f"### 🔹 {index_name}")

        st.dataframe(
            idx_df,
            width='content'
        )

with tabs[16]:
    st.markdown("## 🔥 Confirmed Continuation Breaks")

    if continuation_df.empty:
        st.info("No confirmed continuation setups currently")
    else:
        st.dataframe(
            continuation_df.sort_values("DIST_%", ascending=False),
            width='stretch'
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
            three_green_15m_df.sort_values("CANDLE_TIME", ascending=False),
            width='stretch'
        )


with tabs[17]:
    st.subheader("🚨 Alerts Log (Static)")

    if not os.path.exists(ALERTS_LOG_FILE):
        st.info("No alerts logged yet.")
    else:
        #alerts_df = pd.read_csv(ALERTS_LOG_FILE)
        alerts_df = pd.read_csv(ALERTS_LOG_FILE)
        alerts_df = alerts_df.sort_values(
            by=["DATE", "TIME"],
            ascending=False
        )

        st.dataframe(
            alerts_df,
            width="stretch",
            height=min(4200, 60 + len(alerts_df) * 32)
        )

        st.caption("📌 Latest alerts appear at the top. Data is static and will not change.")


with tabs[18]:
    st.info("🔭 **All Astro content has moved to the new ASTRO tab (first tab on the left).** The heatmap rules below are kept for reference.")
    st.header("🪐 ASTRO HEATMAP — Reference Rules")

    # ── TODAY'S SNAPSHOT ─────────────────────────────────────────────
    try:
        _today_snap = _vedic_day_analysis(datetime.now(IST).date())
        _c1, _c2, _c3, _c4 = st.columns(4)
        _c1.metric("Today Signal",   _today_snap["overall"])
        _c2.metric("Moon Nakshatra", _today_snap["moon_nak"])
        _c3.metric("Moon Rashi",     _today_snap["moon_rashi"])
        _c4.metric("Tithi",          _today_snap["tithi"].split("(")[0].strip())
        st.caption(
            f"☀️ Sun: **{_today_snap['sun_rashi']}** / {_today_snap['sun_nak']}  ·  "
            f"♀ Venus: **{_today_snap['venus_rashi']}**  ·  "
            f"♂ Mars: **{_today_snap['mars_rashi']}** / {_today_snap['mars_nak']}  ·  "
            f"♄ Saturn: **{_today_snap['saturn_rashi']}**  ·  "
            f"♃ Jupiter: **{_today_snap['jupiter_rashi']}**  ·  "
            f"☿ Mercury: **{_today_snap['mercury_rashi']}**  ·  "
            f"☊ Rahu: **{_today_snap['rahu_rashi']}**"
        )
        for _sig_text, _sig_type in _today_snap["signals"]:
            _icon = {"B":"🔴","G":"🟢","T":"⚠️","M":"🟡"}.get(_sig_type, "")
            st.markdown(f"  {_icon} {_sig_text}")
    except Exception as _e:
        st.warning(f"Today snapshot error: {_e}")

    st.divider()

    # ── 10-DAY DYNAMIC FORECAST ──────────────────────────────────────
    st.subheader("🔮 Next 10 Trading Days — Dynamic Nifty Astro Forecast")
    st.caption(
        "Computed live from planetary positions (Lahiri ayanamsa, Jean Meeus Ch47). "
        "Combines Moon nakshatra, tithi, conjunction angles. Refreshes every page load. "
        "**Not financial advice.**"
    )

    _forecast_rows = []
    _check_date = datetime.now(IST).date()
    _days_found = 0

    for _di in range(1, 40):
        _fd = _check_date + timedelta(days=_di)
        if _fd.weekday() >= 5 or _fd in _get_nse_holidays():
            continue
        try:
            _r = _vedic_day_analysis(_fd)
        except Exception:
            continue

        _conj = []
        if _r["moon_ketu_deg"]  < 15: _conj.append(f"🌙-Ketu {_r['moon_ketu_deg']}°")
        if _r["mars_rahu_deg"]  < 15: _conj.append(f"♂-Rahu {_r['mars_rahu_deg']}°")
        if _r["sun_saturn_deg"] <  8: _conj.append(f"☀-♄ {_r['sun_saturn_deg']}°")

        _sig_parts = []
        for _txt, _typ in _r["signals"]:
            _ico = {"B":"🔴","G":"🟢","T":"⚠️","M":"🟡"}.get(_typ, "")
            _sig_parts.append(f"{_ico} {_txt}")

        # Crude oil / intraday hints
        _crude_sigs = [t for t, typ in _r["signals"] if "⛽" in t]
        _crude_str  = "🟢 BULL" if _crude_sigs else "—"
        # Intraday note: Rohini = TRAP; Moon in Taurus = gap-down then recovery
        _intra = ""
        if _r["moon_nak"] == "Rohini":           _intra = "⚠️ TRAP — sell the rally"
        elif _r["moon_rashi"] == "Taurus":        _intra = "🔄 Gap-down → 200pt recovery"
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
            "Key Conjunctions": "  ·  ".join(_conj) if _conj else "—",
            "Crude":          _crude_str,
            "Intraday Hint":  _intra if _intra else "—",
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
            if "BEARISH" in v:  return "background-color:#fde8e8; color:#c0392b; font-weight:700"
            if "BULLISH" in v:  return "background-color:#e8f8ee; color:#1e8449; font-weight:700"
            if "TRAP"    in v:  return "background-color:#fff3cd; color:#856404; font-weight:700"
            if "MIXED"   in v:  return "background-color:#fff9e6; color:#7d6608"
            return ""

        def _fc_intensity(val):
            v = str(val)
            if v == "Extreme": return "background-color:#d32f2f; color:white; font-weight:700"
            if v == "Strong":  return "background-color:#f44336; color:white; font-weight:600"
            if v == "Mild":    return "background-color:#ffd54f; color:#333"
            if v == "Neutral": return "background-color:#e8e8e8; color:#333"
            if v == "Caution": return "background-color:#fff3cd; color:#856404"
            return ""

        st.dataframe(
            _fdf.style
                .map(_fc_signal,    subset=["Signal"])
                .map(_fc_intensity, subset=["Intensity"])
                .map(lambda v: "background-color:#e8f0fe" if "BULL" in str(v) else "", subset=["Crude"])
                .set_properties(**{"font-size":"12px","text-align":"left"})
                .set_table_styles([{"selector":"th","props":[("font-weight","bold"),("background-color","#1a1a2e"),("color","white")]}]),
            width="stretch",
            height=420,
        )

        _nb = sum(1 for r in _forecast_rows if "BEARISH" in r["Signal"])
        _ng = sum(1 for r in _forecast_rows if "BULLISH" in r["Signal"])
        _nt = sum(1 for r in _forecast_rows if "TRAP"    in r["Signal"])
        _nm = sum(1 for r in _forecast_rows if "MIXED"   in r["Signal"])
        st.caption(f"**10-day summary:** 🔴 {_nb} Bearish  ·  🟢 {_ng} Bullish  ·  ⚠️ {_nt} Trap  ·  🟡 {_nm} Mixed")

        with st.expander("🔍 Full Signal Details — All 10 Days"):
            for _row_d in _forecast_rows:
                _ico = ("🔴" if "BEARISH" in _row_d["Signal"] else
                        "🟢" if "BULLISH" in _row_d["Signal"] else
                        "⚠️" if "TRAP"    in _row_d["Signal"] else "🟡")
                st.markdown(f"**{_ico} {_row_d['Date']} — {_row_d['Signal']} ({_row_d['Intensity']})**")
                st.markdown(f"&nbsp;&nbsp;🌙 Moon: **{_row_d['Moon Nakshatra']}** · Rashi: {_row_d['Moon Rashi']} · {_row_d['Tithi']}")
                if _row_d["Key Conjunctions"] != "—":
                    st.markdown(f"&nbsp;&nbsp;⚡ {_row_d['Key Conjunctions']}")
                for _line in _row_d["Key Signals"].split("  |  "):
                    if _line.strip():
                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;{_line.strip()}")
                st.markdown("---")
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
    # 🪐 NIFTY ASTRO HEATMAP — Based on Planetary Rules (Transcript Analysis)
    # ═══════════════════════════════════════════════════════════════
    # Rules extracted from 20 YouTube prediction transcripts (Feb–Mar 2026)
    # Analyst uses Vedic astrology: Moon nakshatra, planetary conjunctions,
    # navamsha positions and lunar tithi to predict Nifty direction.
    # ═══════════════════════════════════════════════════════════════

    st.divider()
    st.subheader("🪐 Nifty Astro Heatmap — Planetary Signal Rules")
    st.caption(
        "Rules distilled from 20 astro-prediction transcripts (Feb–Mar 2026). "
        "Each row = a planetary condition and its historical market outcome. "
        "Use alongside technical analysis — NOT standalone trading advice."
    )

    # ── Rule Database ────────────────────────────────────────────────
    _astro_rules = [
        # ── MOON NAKSHATRA RULES ─────────────────────────────────────
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Shatabhisha (Aquarius) — Rahu's nakshatra",
            "Planet/Nakshatra": "Shatabhisha (राहु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Especially strong bearish when combined with Amavasya or Krishna Pratipada tithi",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Uttarashadha (Saturn's nakshatra) + Moon near Saturn",
            "Planet/Nakshatra": "Uttarashadha (शनि)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Market unpredictable; Sun in Rahu nakshatra + Nitya yoga = volatile",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Krittika (Sun's nakshatra)",
            "Planet/Nakshatra": "Krittika (सूर्य)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "5 planets in Aquarius simultaneously — very bearish; Jupiter in 5th provides limited protection",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Uttaraphalguni (Sun's nakshatra)",
            "Planet/Nakshatra": "Uttaraphalguni (सूर्य)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Sun in Rahu nakshatra + Rahu with Mars = increased selling interest after Moon crosses Ketu",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Mula nakshatra (Ketu's nakshatra)",
            "Planet/Nakshatra": "Mula (केतु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Bearish while Venus + Saturn together in Pisces. Market corrects 200↑ then falls 500",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Dhanishtha (Mars' nakshatra)",
            "Planet/Nakshatra": "Dhanishtha (मंगल)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Mercury + Mars within 1° in Rahu nakshatra; Moon with Ketu in navamsha = brutal fall",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Purvabhadrapada (Jupiter's nakshatra)",
            "Planet/Nakshatra": "Purvabhadrapada (गुरु)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Pushkara navamsha ends → sharp fall expected; Sun changes nakshatra = bad sign",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Pushya (Jupiter's nakshatra) + Moon conjunct Jupiter",
            "Planet/Nakshatra": "Pushya (गुरु)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Temporary relief until 12:00; overall bearish still. Short-term bounce possible",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Rohini nakshatra (Moon's own nakshatra)",
            "Planet/Nakshatra": "Rohini (चंद्र)",
            "Signal": "⚠️ TRAP",
            "Intensity": "High",
            "Notes": "Rohini = 'nakshatra of illusion'. Market appears to rise but it is a false move (trap). Jupiter in Punarvasu amplifies the fake rally. Do NOT buy calls.",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Mrigasira (Mars' nakshatra)",
            "Planet/Nakshatra": "Mrigasira (मंगल)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Mars with Rahu; Jupiter in Punarvasu. Market accumulates puts during rises. Target 21500.",
        },
        {
            "Category": "🌙 Moon Nakshatra",
            "Condition": "Moon in Purvaashadha (Venus' nakshatra)",
            "Planet/Nakshatra": "Purvaashadha (शुक्र)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Venus navamsha in Mars navamsha + Moon navamsha in Mercury navamsha with Saturn = very bad",
        },

        # ── TITHI / LUNAR DAY RULES ───────────────────────────────────
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Krishna Pratipada — 1st day after Amavasya",
            "Planet/Nakshatra": "Tithi — कृष्ण प्रतिपदा",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "Nearly always bearish. Nifty falls on this tithi. Rare exceptions exist.",
        },
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Amavasya night (no-moon) — next day bearish",
            "Planet/Nakshatra": "Tithi — अमावस्या",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Next trading day after Amavasya expected 200–300 pt fall minimum",
        },
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Shukla Tritiya with Nitya yoga",
            "Planet/Nakshatra": "Tithi — शुक्ल तृतीया + नित्य",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Nitya yoga makes market unpredictable. Gap-down open → buy calls 9:25–9:30, hold till 10:30–10:45. Then short again.",
        },
        {
            "Category": "📅 Lunar Tithi",
            "Condition": "Lunar Eclipse (Chandra Grahan) day/aftermath",
            "Planet/Nakshatra": "Tithi — चंद्र ग्रहण",
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
            "Notes": "Until Venus exits Pisces (~Mar 25-26): market up 200 = sell opportunity. 'Luxury is gone to oil' — Saturn=oil, Venus=luxury. Brutal phase.",
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
            "Condition": "Jupiter direct + in own nakshatra (Punarvasu) — standalone",
            "Planet/Nakshatra": "Jupiter Direct (गुरु)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Jupiter cannot protect market when overwhelmed by Mars+Rahu+Saturn combos. Only mild support.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Venus enters Aries (exits Pisces) — after ~Mar 25-26",
            "Planet/Nakshatra": "Venus in Aries (शुक्र मेष)",
            "Signal": "🟢 BULLISH",
            "Intensity": "High",
            "Notes": "Saturn-Venus negative phase ends. Market bottom forms. Heavy bullish expected after this transit.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Moon in Taurus (Vrishabh) — strong recovery",
            "Planet/Nakshatra": "Moon in Taurus (चंद्र वृषभ)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Gap-down open likely but strong recovery. Positive closing. 200-250 pt recovery intraday.",
        },
        {
            "Category": "🟢 Bullish Conditions",
            "Condition": "Saturn in exalted navamsha (Libra navamsha) — Silver bullish",
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
            "Condition": "Saturn in uchha navamsha → crude and oil bullish",
            "Planet/Nakshatra": "Saturn uchha navamsha",
            "Signal": "🟢 BULLISH Crude",
            "Intensity": "High",
            "Notes": "Oil/energy prices rise. Bad for India (energy importer). Nifty weak during oil spikes.",
        },
        {
            "Category": "⛽ Crude Oil / Commodities",
            "Condition": "Mars+Saturn+Sun alignment → Gold + Silver fall",
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
            "Notes": "Targets given: 25500→25000→24800→24000→23800→23300→22800→22300→21800→21500→21300. Bottom predicted: ~21300-21500 (Mar 25-28). Recovery after Venus enters Aries.",
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
            "Notes": "Dollar under pressure when Sun in Saturn nakshatra with Rahu. Don't follow US markets — India falls more than US.",
        },
    ]

    _astro_df = pd.DataFrame(_astro_rules)

    # ── Color map ─────────────────────────────────────────────────────
    def _astro_color(val):
        v = str(val)
        if "BEARISH" in v and "BULLISH" not in v: return "background-color:#fde8e8; color:#c0392b; font-weight:600"
        if "BULLISH" in v and "BEARISH" not in v: return "background-color:#e8f8ee; color:#1e8449; font-weight:600"
        if "TRAP" in v:     return "background-color:#fff3cd; color:#856404; font-weight:600"
        if "MIXED" in v:    return "background-color:#fff9e6; color:#8a6d00"
        if "PRESSURE" in v: return "background-color:#e8f0fe; color:#1a56db"
        return ""

    def _intensity_color(val):
        v = str(val)
        if v == "Extreme":    return "background-color:#d32f2f; color:white; font-weight:700"
        if v == "Very High":  return "background-color:#f44336; color:white; font-weight:600"
        if v == "High":       return "background-color:#ff7043; color:white"
        if v == "Medium":     return "background-color:#ffd54f; color:#333"
        if v == "Low":        return "background-color:#a5d6a7; color:#333"
        if v == "Historical": return "background-color:#b0bec5; color:#333"
        return ""

    st.dataframe(
        _astro_df.style
            .map(_astro_color,    subset=["Signal"])
            .map(_intensity_color, subset=["Intensity"])
            .set_properties(**{"font-size": "12px", "text-align": "left"})
            .set_table_styles([{"selector": "th", "props": [("font-weight", "bold"), ("background-color", "#f0f4ff")]}]),
        width="stretch",
        height=900,
    )

    # ── Quick Legend ──────────────────────────────────────────────────
    with st.expander("📖 How to Read This Table"):
        st.markdown("""
**Signal Types**
| Signal | Meaning |
|--------|---------|
| 🔴 BEARISH | High probability of Nifty fall |
| 🟢 BULLISH | High probability of Nifty rise |
| ⚠️ TRAP | Market rises then reverses — do NOT buy calls |
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
6. **Rohini nakshatra Moon** = TRAP — false bullish move, reverse after
7. **Venus enters Aries** = Bottom signal — start going LONG aggressively
8. **Pushkara Navamsha ends** = immediate reversal downward
        """)

    # ═══════════════════════════════════════════════════════════════
    # 🪐 ASTRO HEATMAP — BATCH 2 (19 new transcripts: Nov 2025 – Mar 2026)
    # New rules covering: Sun transit, Hora analysis, Silver/Copper,
    # personal kundali for trading, Moon sign rashi rules, intraday timing
    # ═══════════════════════════════════════════════════════════════

    st.divider()
    st.subheader("🪐 Astro Heatmap — Batch 2 (Extended Rules)")
    st.caption(
        "Additional rules extracted from 19 new transcripts (Nov 2025 – Mar 2026). "
        "Covers Sun transit by rashi, Hora timing, Silver/Copper commodity signals, "
        "personal kundali indicators, intraday timing patterns and Moon rashi rules."
    )

    _astro_rules2 = [

        # ── SUN TRANSIT RULES ─────────────────────────────────────────
        {
            "Category": "☀️ Sun Transit (Rashi)",
            "Condition": "Sun enters Capricorn (Makar) — transit ~Jan 14",
            "Planet/Nakshatra": "Sun in Capricorn (सूर्य मकर)",
            "Signal": "🟢 BULLISH",
            "Intensity": "High",
            "Notes": "Bullish for ~1 month. Market rises from transit day. Nifty up 200 pts on first trading day. Capricorn = positive rashi for market.",
        },
        {
            "Category": "☀️ Sun Transit (Rashi)",
            "Condition": "Sun enters Aquarius (Kumbh) in neecha navamsha",
            "Planet/Nakshatra": "Sun in Aquarius neecha (सूर्य कुंभ नीच)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Aquarius = negative rashi for market. Neecha navamsha (Libra) makes it worse. Sun = stock market karaka — weakness here = market weakness.",
        },
        {
            "Category": "☀️ Sun Transit (Rashi)",
            "Condition": "Sun in Scorpio (Vrishchik) — exits neecha navamsha mid-day",
            "Planet/Nakshatra": "Sun exits neecha (सूर्य नीच से बाहर)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Intraday pattern: fall till ~10:10, recovery 10:30–12:00 (~100-150 pts), fall again 12:00–14:45, bounce at close. Moon conjunct Sun in Virgo with Mars+Mercury = bearish afternoon.",
        },
        {
            "Category": "☀️ Sun Transit (Rashi)",
            "Condition": "Sun changes nakshatra mid-trading-session",
            "Planet/Nakshatra": "Sun nakshatra change (सूर्य नक्षत्र परिवर्तन)",
            "Signal": "🔴 BEARISH",
            "Intensity": "Medium",
            "Notes": "When Sun changes nakshatra during market hours (~10:00 AM), that session becomes bearish. Accumulate puts before the change time.",
        },
        {
            "Category": "☀️ Sun Transit (Rashi)",
            "Condition": "Sun in Jupiter's nakshatra (Punarvasu/Vishakha/Purvabhadrapada)",
            "Planet/Nakshatra": "Sun in Jupiter nakshatra (गुरु नक्षत्र)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Jupiter retrograde from Mar 11 + Sun in Jupiter nakshatra = brutal correction. Jupiter cannot protect itself when Sun occupies its nakshatra.",
        },
        {
            "Category": "☀️ Sun Transit (Rashi)",
            "Condition": "Sun + Jupiter = stock market karakas (general rule)",
            "Planet/Nakshatra": "Sun + Jupiter (सूर्य + गुरु कारक)",
            "Signal": "📌 FRAMEWORK",
            "Intensity": "Foundation",
            "Notes": "Sun in positive rashi = bullish month. Sun in negative rashi = bearish month. Moon gives daily direction within the monthly Sun trend. Aquarius = negative; Capricorn/Sagittarius = positive.",
        },

        # ── MOON RASHI RULES (new — separate from nakshatra rules) ────
        {
            "Category": "🌙 Moon Rashi",
            "Condition": "Moon in Sagittarius (Dhanu rashi)",
            "Planet/Nakshatra": "Moon in Sagittarius (चंद्र धनु)",
            "Signal": "🟢 BULLISH",
            "Intensity": "Medium",
            "Notes": "Dhanu = bullish rashi for market. Market rises that day. Confirmed when Sun also in Capricorn (combined bullish month).",
        },
        {
            "Category": "🌙 Moon Rashi",
            "Condition": "Moon in Taurus (Vrishabh) + gap-down open",
            "Planet/Nakshatra": "Moon in Taurus (चंद्र वृषभ)",
            "Signal": "🟢 BULLISH (recovery)",
            "Intensity": "Medium",
            "Notes": "Gap-down then recovers 200-250 pts. Positive closing. Moon in own exaltation sign (Taurus) overrides initial bearishness.",
        },
        {
            "Category": "🌙 Moon Rashi",
            "Condition": "Moon in Aquarius (Kumbh) + afflicted",
            "Planet/Nakshatra": "Moon in Aquarius afflicted (चंद्र कुंभ)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Aquarius = negative rashi. When Moon afflicted here (conjunct malefics), market bearish. Check if Moon is in affliction before deciding.",
        },
        {
            "Category": "🌙 Moon Rashi",
            "Condition": "Moon in own nakshatra (Rohini/Hasta/Shravana) = Swarna nakshatra",
            "Planet/Nakshatra": "Moon in Swarna nakshatra (स्वर्ण नक्षत्र)",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Moon in Hasta (Swarna nakshatra = its own). D1: Moon 12th from Sun. D9: Moon 8th from Sun = bearish feel. Intraday: fall → recovery → fall pattern. Sun in Dhanishtha separately.",
        },

        # ── HORA ANALYSIS (NEW — intraday timing) ─────────────────────
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
            "Condition": "Intraday: Gap-down open → buy calls 9:25–9:30, hold till 10:30–10:45",
            "Planet/Nakshatra": "Opening Hora (Nitya Yoga day)",
            "Signal": "🟢 BULLISH (9:25–10:30 only)",
            "Intensity": "Medium",
            "Notes": "When Nitya yoga + Shukla Tritiya: gap-down open = brief call opportunity 9:25–9:30 till 10:30–10:45. Then short again. Never hold calls beyond 10:45.",
        },

        # ── SILVER RULES (NEW — full set from silver transcripts) ──────
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus in Capricorn (Makar) — transit arrival day",
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
            "Condition": "Venus exits Purvashadha nakshatra (Jupiter nakshatra → moves on)",
            "Planet/Nakshatra": "Venus exits Purvashadha (पूर्वाषाढ़ा से निकलना)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "Medium",
            "Notes": "Venus leaving Purvashadha nakshatra triggers silver selling pressure. Confirmed Jan 8 prediction — silver fell from ₹4,20,000 to ₹2,90,000 (31% in one day).",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "Venus in Sun's nakshatra (Uttarashadha/Krittika/Uttaraphalguni)",
            "Planet/Nakshatra": "Venus in Sun nakshatra (सूर्य नक्षत्र में शुक्र)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "High",
            "Notes": "Venus doesn't perform well in Sun's nakshatra. Silver selling pressure. Target ₹1,90,000. Sun afflicted by malefics at same time amplifies the fall.",
        },
        {
            "Category": "🥈 Silver Rules",
            "Condition": "5 planets in close conjunction (within 1–2° of each other)",
            "Planet/Nakshatra": "5-planet stellium (पंच ग्रह युति)",
            "Signal": "🔴 BEARISH Silver",
            "Intensity": "Very High",
            "Notes": "Jan 8 2026: Sun 23°, Jupiter 26°, Moon 24°, Mercury 21°, Mars 25° — all within 5°. Silver cannot sustain rally. Correction to ₹1,90,000 within 4-14 days.",
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
            "Notes": "Silver in Taurus navamsha = minor support. But if Mars behind Venus, do NOT expect sustained rally. 'Jo tezi honi thi wo ho lene do' — don't chase.",
        },

        # ── COPPER RULE (NEW) ──────────────────────────────────────────
        {
            "Category": "🔶 Copper / Commodities",
            "Condition": "Copper long-term bull run (multi-year cycle) — 'New Gold'",
            "Planet/Nakshatra": "Copper bull cycle (कॉपर बुल)",
            "Signal": "🟢 BULLISH Copper",
            "Intensity": "High",
            "Notes": "Long-term theme: Copper = 'new gold'. Buy and hold strategy. Commodities broadly bullish in this cycle. 920 → 1171 in 2 months (confirmed). Not a short-term cycle.",
        },

        # ── PERSONAL KUNDALI FOR TRADING (NEW) ────────────────────────
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Mercury (Budh) strong in kundali — essential for trading profits",
            "Planet/Nakshatra": "Mercury strong (बुध बलवान)",
            "Signal": "📌 PREREQUISITE",
            "Intensity": "Foundation",
            "Notes": "Mercury = intellect. If Mercury is in a strong house and well-placed, trader has the right mind for markets. Without strong Mercury, trading losses likely.",
        },
        {
            "Category": "🔮 Personal Kundali",
            "Condition": "Jupiter (Brihaspati) aspecting 9th house (luck) or 11th house (profit)",
            "Planet/Nakshatra": "Jupiter → 9th/11th house (गुरु दृष्टि)",
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
            "Condition": "Ketu in bad house or afflicted — business failures",
            "Planet/Nakshatra": "Ketu afflicted (केतु खराब)",
            "Signal": "⚠️ CAUTION",
            "Intensity": "High",
            "Notes": "Afflicted Ketu = deals fall at last moment, work doesn't complete, businesses fail. No universal remedy — requires kundali analysis. Check Ketu placement before starting any venture.",
        },

        # ── BULLISH MACRO SIGNAL (Nov 2025) ───────────────────────────
        {
            "Category": "🟢 Bullish Phases",
            "Condition": "Sun in Capricorn + Jupiter direct + Dow up → Nifty bull run",
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
            "Category": "♃ Jupiter Rules",
            "Condition": "Jupiter retrograde (from Mar 11 2026)",
            "Planet/Nakshatra": "Jupiter retrograde (गुरु वक्री)",
            "Signal": "🔴 BEARISH",
            "Intensity": "High",
            "Notes": "Jupiter goes retrograde Mar 11. When Sun is simultaneously in Jupiter nakshatra = brutal correction. Jupiter loses protective power when retrograde. Market falls accelerate.",
        },
        {
            "Category": "♃ Jupiter Rules",
            "Condition": "Jupiter in own nakshatra (Punarvasu) but overwhelmed by malefics",
            "Planet/Nakshatra": "Jupiter Punarvasu + malefic overload",
            "Signal": "🟡 MIXED",
            "Intensity": "Medium",
            "Notes": "Jupiter in Punarvasu tries to protect. But when Mars+Rahu+Saturn combos overpower: 'Jupiter ki itni takat nahi ki inhe rok sake'. Only mild support possible.",
        },

        # ── INTRADAY MOON DEGREE RULE (NEW) ─────────────────────────────
        {
            "Category": "📐 Intraday Moon Degree",
            "Condition": "Moon at 9° at 9:15 AM → max upside = 3× = ~45 Nifty points",
            "Planet/Nakshatra": "Moon degree at open (चंद्र अंश)",
            "Signal": "📌 INTRADAY LIMIT",
            "Intensity": "Medium",
            "Notes": "Moon degree × 3 = approximate max Nifty move on that day. At 9° Moon = 45 pts max rally. Any move above this = entry for short. Feb 13: Moon 9° → max 45 pts up → short above that.",
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
            "Condition": "Active Rahu period in transit — confusion/illusion phase",
            "Planet/Nakshatra": "Rahu period active (राहु काल)",
            "Signal": "⚠️ TRAP",
            "Intensity": "High",
            "Notes": "During Rahu period: market creates confusion. Short-term false moves in both directions. Mar 27 2026: puts in loss because Rahu created illusion. 'Rahu ka janjaal tha — bhram ki stithi'. Wait for Rahu period to end before acting.",
        },
        {
            "Category": "🌀 Rahu Period",
            "Condition": "Rahu period ends → Sun in neecha navamsha + Moon with Ketu",
            "Planet/Nakshatra": "Post-Rahu: Sun neecha + Moon–Ketu",
            "Signal": "🔴 BEARISH",
            "Intensity": "Very High",
            "Notes": "After Rahu confusion clears: Sun in neecha navamsha with Saturn + Moon with Ketu in navamsha = highly bearish setup. Mar 27: 1100 pts fall in 2 days predicted.",
        },
    ]

    _astro_df2 = pd.DataFrame(_astro_rules2)

    def _astro_color2(val):
        v = str(val)
        if "BEARISH" in v and "BULLISH" not in v: return "background-color:#fde8e8; color:#c0392b; font-weight:600"
        if "BULLISH" in v and "BEARISH" not in v: return "background-color:#e8f8ee; color:#1e8449; font-weight:600"
        if "TRAP" in v:        return "background-color:#fff3cd; color:#856404; font-weight:600"
        if "MIXED" in v:       return "background-color:#fff9e6; color:#8a6d00"
        if "PREREQUISITE" in v or "FAVORABLE" in v: return "background-color:#e8f0fe; color:#1a56db"
        if "CAUTION" in v:     return "background-color:#fef3c7; color:#92400e"
        if "FRAMEWORK" in v or "LIMIT" in v: return "background-color:#f3e8ff; color:#6d28d9"
        return ""

    def _intensity_color2(val):
        v = str(val)
        if v == "Extreme":    return "background-color:#d32f2f; color:white; font-weight:700"
        if v == "Very High":  return "background-color:#f44336; color:white; font-weight:600"
        if v == "High":       return "background-color:#ff7043; color:white"
        if v == "Medium":     return "background-color:#ffd54f; color:#333"
        if v == "Low":        return "background-color:#a5d6a7; color:#333"
        if v == "Foundation": return "background-color:#90caf9; color:#0d2137; font-weight:600"
        return ""

    st.dataframe(
        _astro_df2.style
            .map(_astro_color2,    subset=["Signal"])
            .map(_intensity_color2, subset=["Intensity"])
            .set_properties(**{"font-size": "12px", "text-align": "left"})
            .set_table_styles([{"selector": "th", "props": [("font-weight", "bold"), ("background-color", "#f0f4ff")]}]),
        width="stretch",
        height=900,
    )

    with st.expander("📖 Batch 2 — Key Rules Summary"):
        st.markdown("""
**Sun Transit Framework**
- Sun in **Capricorn** = bullish month | Sun in **Aquarius** = bearish month
- Sun changes **nakshatra mid-session** = that session turns bearish
- Sun in **Jupiter nakshatra** + Jupiter retrograde = brutal correction

**Silver Trading Rules (Venus + Moon)**
- Venus enters **Capricorn** = Silver TOP formed — go short immediately
- Venus conjunct **Moon** = Silver falls 5-7% that day
- Venus in **Sun's nakshatra** = Silver selling pressure, target ₹1,90,000
- **5-planet stellium** within 5° = Silver cannot rally, falls to ₹1,90,000

**Personal Kundali Prerequisites**
- Strong **Mercury** = trading intelligence ✅
- **Jupiter → 9th/11th** = luck + profit from markets ✅
- **Rahu in 11th** = stock market profits ✅ (unless Mars/Saturn aspect Rahu)
- **Ketu afflicted** = businesses fail at last moment ⚠️

**Intraday Timing Rules**
- Moon degree × 3 = **max Nifty points** for that day
- Moon 11th from Sun = bearish day
- **Hora analysis**: Sun+Moon+Mercury in same hora = fall during that hora
- Gap-down + **Nitya yoga**: buy calls 9:25–9:30, book at 10:30–10:45 only

**Rahu Period Warning**
- Active Rahu transit = **illusion/confusion** — expect false moves both ways
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

