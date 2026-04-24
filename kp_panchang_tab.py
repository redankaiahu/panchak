# ==========================================================
# KP PANCHANG TAB — kp_panchang_tab.py
# ==========================================================
# Features:
#   1. Dynamic KP interpretation per window (sign/star/sub/sub-sub)
#   2. Auto 5-min OHLC capture from Kite → CSV per day
#   3. Auto Top-High / Least-Low from 3 candles in slot
#   4. Auto Telegram alerts (open / break / false / summary)
#   5. Next-5-day upcoming windows with KP signal
#   6. Breakout memory (last 5 days) — fully automated
# ==========================================================

import os, json, threading, urllib.request, csv
from datetime import datetime, timedelta, date, time as dtime
import pandas as pd
import streamlit as st

# ── paths ─────────────────────────────────────────────────
_HERE      = os.path.dirname(os.path.abspath(__file__))
_CSV_PATH  = os.path.join(_HERE, "kp_panchang_2026.csv")
_MEM_PATH  = os.path.join(_HERE, "CACHE", "kp_breakout_memory.json")
_TG_DEDUP  = os.path.join(_HERE, "CACHE", "kp_tg_dedup.json")
_OHLC_DIR  = os.path.join(_HERE, "CACHE", "kp_ohlc")

os.makedirs(_OHLC_DIR, exist_ok=True)
os.makedirs(os.path.join(_HERE, "CACHE"), exist_ok=True)

# ═══════════════════════════════════════════════════════════
# KP KNOWLEDGE BASE
# ═══════════════════════════════════════════════════════════

PLANET_FULL = {
    "Su": "Sun",     "Mo": "Moon",   "Ma": "Mars",
    "Me": "Mercury", "Ju": "Jupiter","Ve": "Venus",
    "Sa": "Saturn",  "Ra": "Rahu",   "Ke": "Ketu",
}

PLANET_COLOR = {
    "Ju": "#00c853", "Ve": "#00bcd4", "Mo": "#64b5f6",
    "Me": "#aed581", "Su": "#ffb300", "Sa": "#ef5350",
    "Ra": "#e040fb", "Ke": "#ff7043", "Ma": "#f44336",
}

# Market signal by sub lord (most important layer)
SUB_SIGNAL = {
    "Ju": ("STRONG BUY 🟢🟢",  "bullish"),
    "Ve": ("STRONG BUY 🟢🟢",  "bullish"),
    "Mo": ("BUY 🟢",            "bullish"),
    "Me": ("MIXED ⚪",          "neutral"),
    "Su": ("MILD BUY 🟡",       "mild_bull"),
    "Sa": ("SELL 🔴",           "bearish"),
    "Ra": ("STRONG SELL 🔴🔴",  "bearish"),
    "Ke": ("STRONG SELL 🔴🔴",  "bearish"),
    "Ma": ("SELL 🔴",           "bearish"),
}

# What each planet rules (market/sector context)
PLANET_RULES = {
    "Su": ("Government, power, authority, gold, leaders",
           "PSU stocks, gold ETF, energy, infrastructure"),
    "Mo": ("Public mood, liquidity, FMCG, water, public opinion",
           "FMCG, dairy, retail, consumer staples"),
    "Ma": ("Energy, aggression, defence, engineering, fire",
           "Defence, steel, metals, real estate, construction"),
    "Me": ("Communication, IT, trade, commerce, analytics",
           "IT/Tech, telecom, media, NBFC, trading companies"),
    "Ju": ("Finance, banking, wealth, law, education, expansion",
           "Banks, NBFC, insurance, mutual funds, education"),
    "Ve": ("Luxury, arts, beauty, money, vehicles, entertainment",
           "FMCG luxury, auto, hotels, pharma, entertainment"),
    "Sa": ("Delays, labour, oil, mining, restriction, patience",
           "Oil & gas, mining, cement, infra — usually bearish"),
    "Ra": ("Technology, foreign, unconventional, sudden events",
           "Foreign stocks, crypto-adjacent, speculative — bearish surge"),
    "Ke": ("Spiritual, sudden drops, confusion, past karma",
           "Unpredictable moves, stops/reversals — bearish"),
}

# KP house significations for each planet (simplified)
PLANET_HOUSES = {
    "Su": ("1, 9", "self/fortune — mild wealth"),
    "Mo": ("4, 11", "comfort/gains — positive for profit"),
    "Ma": ("3, 8", "effort/sudden loss — bearish risk"),
    "Me": ("6, 11", "service/gains — mixed"),
    "Ju": ("2, 9, 11", "wealth/fortune/gains — bullish"),
    "Ve": ("2, 7, 11", "money/partnership/gains — bullish"),
    "Sa": ("8, 10, 12", "loss/restriction/expense — bearish"),
    "Ra": ("6, 11 (distorted)", "shadowy gains or sudden loss"),
    "Ke": ("12, 8", "expense/sudden loss — bearish"),
}

# Sign lord quick meaning
SIGN_MEANING = {
    "Ju": "expansive / optimistic (Sagittarius/Pisces)",
    "Sa": "restrictive / slow (Capricorn/Aquarius)",
    "Ma": "aggressive / volatile (Aries/Scorpio)",
    "Ve": "harmonious / financial (Taurus/Libra)",
    "Me": "analytical / communicative (Gemini/Virgo)",
    "Mo": "emotional / public sentiment (Cancer)",
    "Su": "powerful / authoritative (Leo)",
    "Ra": "unconventional / foreign influence",
    "Ke": "detached / sudden reversal",
}

# Sub-sub reinforcement description
SUBSUB_TEXT = {
    "Ju": "reinforces wealth & expansion",
    "Ve": "double-confirms money & luxury flow",
    "Mo": "public buying pressure confirmed",
    "Me": "mixed signal — wait for breakout",
    "Su": "authority / slight upward bias",
    "Sa": "delays and selling pressure confirmed",
    "Ra": "sharp volatile move possible (both ways)",
    "Ke": "confusion / sudden reversal risk",
    "Ma": "aggressive selling energy",
}

# Price action tendency by overall signal
PRICE_ACTION = {
    "bullish":    "Price tends to hold support and move UP. Buyers dominate. "
                  "Look for breakout above Top-High for entry.",
    "mild_bull":  "Slight upward bias but weak. Enter only on confirmed breakout above Top-High.",
    "neutral":    "Price may chop sideways. Wait for clear breakout — no bias.",
    "bearish":    "Price tends to break support and move DOWN. Sellers dominate. "
                  "Look for breakdown below Least-Low for short entry.",
}

# House group keywords from PDF
HOUSE_KEYWORDS = {
    2:  "Wealth accumulation, financial status, money inflow",
    6:  "Service, competition, loans, litigation",
    7:  "Partnerships, market relationships, trading",
    8:  "Sudden gains/losses, unexpected events, inheritance",
    9:  "Fortune, luck, long-term gains",
    11: "Gains, desires fulfilled, profit booking",
    12: "Expenses, losses, foreign investment",
}


def _kp_interpret(p1, p2, p3, p4, slot_str, now_str=""):
    """
    Generate full dynamic KP interpretation for a Moon Transit entry.
    p1=sign lord, p2=star lord, p3=sub lord (KEY), p4=sub-sub lord
    Returns dict with all display fields.
    """
    sig_label, sig_type = SUB_SIGNAL.get(p3, ("UNKNOWN", "neutral"))

    # Reinforcement check: if p3==p4, signal is stronger
    if p3 == p4:
        reinforce = f"⚡ Sub = Sub-Sub ({PLANET_FULL[p3]}) — signal is **double-confirmed**"
    elif p3 in ("Ju","Ve","Mo") and p4 in ("Ju","Ve","Mo"):
        reinforce = f"✅ Sub ({PLANET_FULL[p3]}) + Sub-Sub ({PLANET_FULL[p4]}) both benefic — strong confirmation"
    elif p3 in ("Sa","Ra","Ke","Ma") and p4 in ("Sa","Ra","Ke","Ma"):
        reinforce = f"⚠️ Sub ({PLANET_FULL[p3]}) + Sub-Sub ({PLANET_FULL[p4]}) both malefic — strong bearish"
    else:
        reinforce = f"Sub-Sub {PLANET_FULL[p4]} {SUBSUB_TEXT.get(p4,'')}"

    # Dominant house signification
    h_p3, h_p3_desc = PLANET_HOUSES.get(p3, ("?", ""))
    h_p4, h_p4_desc = PLANET_HOUSES.get(p4, ("?", ""))

    # Best sectors
    _, sector_p1 = PLANET_RULES.get(p1, ("",""))
    _, sector_p3 = PLANET_RULES.get(p3, ("","All indices"))
    rules_p1, _  = PLANET_RULES.get(p1, ("",""))
    rules_p2, _  = PLANET_RULES.get(p2, ("",""))
    rules_p3, _  = PLANET_RULES.get(p3, ("",""))

    # If p2==p3==p4 (triple) → extra strong
    if p2 == p3 == p4:
        triple = f"🔥 TRIPLE {PLANET_FULL[p2]} — exceptionally strong signal!"
    elif p3 == p4:
        triple = f"⚡ Double {PLANET_FULL[p3]} (Sub+Sub-Sub)"
    else:
        triple = ""

    return {
        "signal_label":  sig_label,
        "signal_type":   sig_type,
        "triple_note":   triple,
        "reinforce":     reinforce,
        "sign_lord":     p1,
        "star_lord":     p2,
        "sub_lord":      p3,
        "subsub_lord":   p4,
        "sign_meaning":  SIGN_MEANING.get(p1, ""),
        "rules_sign":    rules_p1,
        "rules_star":    rules_p2,
        "rules_sub":     rules_p3,
        "sector_best":   sector_p3,
        "houses_sub":    h_p3,
        "houses_sub_desc": h_p3_desc,
        "houses_subsub": h_p4,
        "houses_subsub_desc": h_p4_desc,
        "price_action":  PRICE_ACTION.get(sig_type, PRICE_ACTION["neutral"]),
        "slot":          slot_str,
        "time":          now_str,
    }


def _render_interpretation(interp, expanded=True):
    """Render the full dynamic KP interpretation card."""
    sig   = interp["signal_label"]
    stype = interp["signal_type"]
    p1,p2,p3,p4 = (interp["sign_lord"], interp["star_lord"],
                   interp["sub_lord"],   interp["subsub_lord"])

    # colour by signal
    sig_color = {"bullish":"#00e676","mild_bull":"#c6ff00",
                 "neutral":"#bdbdbd","bearish":"#ff5252"}.get(stype,"#bdbdbd")
    bg_color  = {"bullish":"#0a2a0a","mild_bull":"#1a2a00",
                 "neutral":"#1a1a1a","bearish":"#2a0a0a"}.get(stype,"#1a1a1a")

    with st.expander(
        f"🪐 KP Interpretation: {p1}·{p2}·{p3}·{p4}  →  {sig}",
        expanded=expanded
    ):
        # signal banner
        _triple_html = (
            '<br><span style="color:#ffb300;font-size:14px">' + interp["triple_note"] + '</span>'
            if interp["triple_note"] else ""
        )
        st.markdown(
            f'<div style="background:{bg_color};border:2px solid {sig_color};'
            f'border-radius:10px;padding:12px 16px;margin-bottom:10px">'
            f'<span style="font-size:20px;font-weight:700;color:{sig_color}">{sig}</span>'
            f'{_triple_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Layer breakdown
        st.markdown("#### 🔍 Planet Layer Breakdown")
        layer_data = [
            ("Sign Lord (Rashi)",  p1, "Sets the general mood / tone of the period",
             interp["sign_meaning"], interp["rules_sign"]),
            ("Star Lord (Nakshatra)", p2, "Governs the karmic theme of the transit",
             "", interp["rules_star"]),
            ("⭐ Sub Lord (KEY DECIDER)", p3, "This decides the market direction",
             f"Houses: {interp['houses_sub']} — {interp['houses_sub_desc']}",
             interp["rules_sub"]),
            ("Sub-Sub Lord (Confirms)", p4, interp["reinforce"],
             f"Houses: {interp['houses_subsub']} — {interp['houses_subsub_desc']}", ""),
        ]

        for layer_name, planet, role, extra, rules in layer_data:
            pc = PLANET_COLOR.get(planet,"#9e9e9e")
            is_key = "KEY" in layer_name
            border = f"border-left:4px solid {pc}" if is_key else f"border-left:2px solid {pc}"
            _extra_html = (
                '<br><span style="color:#80cbc4;font-size:11px">🏛 ' + extra + '</span>'
                if extra else ""
            )
            _rules_html = (
                '<br><span style="color:#aaa;font-size:11px">⚡ Rules: ' + rules + '</span>'
                if rules else ""
            )
            st.markdown(
                f'<div style="background:#111;{border};'
                f'border-radius:6px;padding:8px 12px;margin:4px 0">'
                f'<span style="color:#9e9e9e;font-size:11px">{layer_name}</span><br>'
                f'<span style="background:{pc};color:#000;font-weight:700;'
                f'padding:2px 8px;border-radius:4px;font-size:14px">{planet}</span>'
                f'&nbsp;<b style="color:#e0e0e0">{PLANET_FULL.get(planet,planet)}</b>'
                f'<br><span style="color:#bbb;font-size:12px">📌 {role}</span>'
                f'{_extra_html}'
                f'{_rules_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # Market signals
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📊 What It Says for Market")
            st.markdown(
                f'<div style="background:#111;border-radius:6px;padding:10px">'
                f'<b style="color:#ffb300">Sub Lord Signal ({p3} = {PLANET_FULL[p3]}):</b><br>'
                f'<span style="color:#e0e0e0">{interp["rules_sub"]}</span><br><br>'
                f'<b style="color:#80cbc4">Sign Lord ({p1} = {PLANET_FULL[p1]}) sets mood:</b><br>'
                f'<span style="color:#e0e0e0">{interp["sign_meaning"]}</span><br><br>'
                f'<b style="color:#ce93d8">Best Sectors:</b><br>'
                f'<span style="color:#e0e0e0">{interp["sector_best"]}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown("#### 🏛 KP House Groups")
            # highlight relevant houses
            def _house_badge(h_str, desc):
                houses = [x.strip() for x in h_str.replace("(","").replace(")","").split(",") if x.strip()]
                badges = ""
                for h in houses:
                    try:
                        hnum = int(h)
                        meaning = HOUSE_KEYWORDS.get(hnum, "")
                        badges += (f'<span style="background:#1a237e;color:#fff;'
                                   f'padding:2px 6px;border-radius:4px;margin:2px;'
                                   f'font-size:12px;display:inline-block">'
                                   f'H{h}</span> ')
                        if meaning:
                            badges += f'<span style="color:#aaa;font-size:11px">{meaning}</span><br>'
                    except Exception:
                        pass
                return badges

            st.markdown(
                f'<div style="background:#111;border-radius:6px;padding:10px">'
                f'<b style="color:#ffb300">{p3} (Sub) signifies Houses {interp["houses_sub"]}:</b><br>'
                f'{_house_badge(interp["houses_sub"], interp["houses_sub_desc"])}'
                f'<br><b style="color:#80cbc4">{p4} (Sub-Sub) signifies Houses {interp["houses_subsub"]}:</b><br>'
                f'{_house_badge(interp["houses_subsub"], interp["houses_subsub_desc"])}'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown("#### 📈 Likely Price Action")
        _reinforce_html = (
            '<br><br><b style="color:#ffb300">' + interp["reinforce"] + '</b>'
            if interp["reinforce"] else ""
        )
        st.markdown(
            f'<div style="background:{bg_color};border:1px solid {sig_color};'
            f'border-radius:8px;padding:12px 16px">'
            f'<span style="color:{sig_color};font-size:15px;font-weight:700">{sig}</span><br>'
            f'<span style="color:#e0e0e0">{interp["price_action"]}</span>'
            f'{_reinforce_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # quick rule summary table
        st.markdown("#### 📋 Sub Lord Quick Reference")
        tbl_rows = ""
        for pl, (lbl, ltype) in SUB_SIGNAL.items():
            c = {"bullish":"#00e676","mild_bull":"#c6ff00",
                 "neutral":"#bdbdbd","bearish":"#ff5252"}.get(ltype,"#bdbdbd")
            is_cur = "→ " if pl == p3 else ""
            tbl_rows += (
                f'<tr style="background:{"#1a2a1a" if pl==p3 else "#111"}">'
                f'<td style="padding:4px 8px">'
                f'<span style="background:{PLANET_COLOR.get(pl,"#555")};color:#000;'
                f'font-weight:700;padding:1px 6px;border-radius:3px">{pl}</span>'
                f'&nbsp;<span style="color:#e0e0e0">{PLANET_FULL[pl]}</span></td>'
                f'<td style="padding:4px 8px;color:{c}">{is_cur}{lbl}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<table style="width:100%;border-collapse:collapse;font-size:12px">'
            f'<tr style="background:#222"><th style="text-align:left;padding:4px 8px;color:#ffb300">Sub Lord</th>'
            f'<th style="text-align:left;padding:4px 8px;color:#ffb300">Market Tendency</th></tr>'
            f'{tbl_rows}</table>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════
# KITE 5-MIN OHLC AUTO-FETCH
# ═══════════════════════════════════════════════════════════

# Kite instrument tokens for indices
_INDEX_TOKENS = {
    "NIFTY":      256265,
    "BANKNIFTY":  260105,
    "SENSEX":     265,        # BSE:SENSEX instrument token (Kite BSE)
}
_INDEX_KITE_KEY = {
    "NIFTY":     "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "SENSEX":    "BSE:SENSEX",
}

def _get_kite():
    """Get kite object from main dashboard globals."""
    try:
        import __main__ as _m
        return getattr(_m, "kite", None)
    except Exception:
        return None


def _ohlc_csv_path(date_str):
    """CACHE/kp_ohlc/YYYY-MM-DD.csv"""
    return os.path.join(_OHLC_DIR, f"{date_str}.csv")


def _load_ohlc_today(date_str):
    """Load today's OHLC from CSV. Returns DataFrame or empty."""
    fp = _ohlc_csv_path(date_str)
    if not os.path.exists(fp):
        return pd.DataFrame()
    try:
        df = pd.read_csv(fp, encoding='utf-8')
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def _save_ohlc_row(date_str, symbol, dt, open_, high, low, close, volume=0):
    """Append or update one 5-min candle row to the daily CSV."""
    fp  = _ohlc_csv_path(date_str)
    key = dt.strftime("%H:%M")

    rows = []
    exists = False
    if os.path.exists(fp):
        with open(fp, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["symbol"] == symbol and row["datetime"].startswith(dt.strftime("%Y-%m-%d %H:%M")):
                    rows.append({"symbol": symbol, "datetime": str(dt),
                                 "open": open_, "high": high, "low": low,
                                 "close": close, "volume": volume})
                    exists = True
                else:
                    rows.append(row)

    if not exists:
        rows.append({"symbol": symbol, "datetime": str(dt),
                     "open": open_, "high": high, "low": low,
                     "close": close, "volume": volume})

    with open(fp, "w", newline="", encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["symbol","datetime","open","high","low","close","volume"])
        writer.writeheader()
        writer.writerows(rows)


def _fetch_5min_slot(symbol, slot_start_str, slot_end_str, date_str):
    """
    Fetch 5-min candles for a slot from Kite.
    slot_start_str = "11:45", slot_end_str = "12:00"
    Returns list of candle dicts or [].
    """
    kite = _get_kite()
    if kite is None:
        return []

    token = _INDEX_TOKENS.get(symbol)
    if token is None:
        return []

    try:
        d        = datetime.strptime(date_str, "%Y-%m-%d").date()
        sh, sm   = map(int, slot_start_str.split(":"))
        eh, em   = map(int, slot_end_str.split(":"))
        start_dt = datetime.combine(d, dtime(sh, sm))
        end_dt   = datetime.combine(d, dtime(eh, em))

        candles = kite.historical_data(token, start_dt, end_dt, interval="5minute")
        return candles
    except Exception as e:
        print(f"[KP-OHLC] fetch error {symbol} {slot_start_str}: {e}")
        return []


def _fetch_and_cache_slot(symbol, slot_start, slot_end, date_str):
    """
    Fetch 5-min candles for slot, save each candle to CSV.
    Returns (top_high, least_low, candles_list).
    """
    candles = _fetch_5min_slot(symbol, slot_start, slot_end, date_str)
    if not candles:
        return None, None, []

    all_high = [c["high"]  for c in candles]
    all_low  = [c["low"]   for c in candles]
    top_high  = max(all_high) if all_high else None
    least_low = min(all_low)  if all_low  else None

    for c in candles:
        _save_ohlc_row(date_str, symbol,
                       pd.to_datetime(c["date"]),
                       c["open"], c["high"], c["low"], c["close"],
                       c.get("volume", 0))

    return top_high, least_low, candles


def _get_slot_ohlc(symbol, slot_start, slot_end, date_str):
    """
    Load slot OHLC from CSV if already fetched, else fetch from Kite.
    Returns (top_high, least_low, candles_df).
    """
    df = _load_ohlc_today(date_str)
    if not df.empty and symbol in df["symbol"].values:
        sh, sm = map(int, slot_start.split(":"))
        eh, em = map(int, slot_end.split(":"))
        slot_df = df[
            (df["symbol"] == symbol) &
            (df["datetime"].dt.hour * 60 + df["datetime"].dt.minute >= sh * 60 + sm) &
            (df["datetime"].dt.hour * 60 + df["datetime"].dt.minute <  eh * 60 + em)
        ]
        if not slot_df.empty:
            return slot_df["high"].max(), slot_df["low"].min(), slot_df

    # not cached — fetch from Kite
    th, ll, candles = _fetch_and_cache_slot(symbol, slot_start, slot_end, date_str)
    return th, ll, candles


def _get_ltp(symbol):
    """Quick LTP from Kite quote."""
    kite = _get_kite()
    if kite is None:
        return None
    try:
        key = _INDEX_KITE_KEY.get(symbol, f"NSE:{symbol}")
        q   = kite.quote([key])
        return q[key]["last_price"]
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

def _get_tg_cfg():
    try:
        import __main__ as _m
        tok = getattr(_m, "TG_BOT_TOKEN", "") or os.environ.get("TG_BOT_TOKEN","")
        cid = getattr(_m, "TG_CHAT_ID",   "") or os.environ.get("TG_CHAT_ID","")
        return tok, cid
    except Exception:
        return "", ""


def _tg_dedup_load():
    try:
        with open(_TG_DEDUP) as f:
            return json.load(f)
    except Exception:
        return {}


def _tg_dedup_save(d):
    with open(_TG_DEDUP, "w", encoding='utf-8') as f:
        json.dump(d, f)


def _send_tg(msg, key=None):
    tok, cid = _get_tg_cfg()
    if not tok or not cid:
        return False
    # Check KP alerts toggle (from main dashboard session state)
    try:
        import streamlit as _st_check
        if not _st_check.session_state.get("tg_KP_ALERTS", True):
            return False  # KP alerts disabled in Alert Control
    except Exception:
        pass
    if key:
        dd = _tg_dedup_load()
        if dd.get(key):
            return False

    def _go():
        try:
            url = f"https://api.telegram.org/bot{tok}/sendMessage"
            payload = json.dumps({"chat_id": cid, "text": msg,
                                  "parse_mode": "HTML"}).encode()
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                ok = json.loads(r.read()).get("ok", False)
            if ok and key:
                dd = _tg_dedup_load()
                dd[key] = datetime.now().isoformat()
                _tg_dedup_save(dd)
        except Exception as e:
            print(f"[KP-TG] {e}")

    threading.Thread(target=_go, daemon=True).start()
    return True


def _sig_label(p1,p2,p3,p4):
    l,_ = SUB_SIGNAL.get(p3, ("NEUTRAL ⚪","neutral"))
    return l


def _tg_open(slot, p1,p2,p3,p4, nifty_ltp=0, bnf_ltp=0, interp=None):
    pnames = " · ".join(PLANET_FULL.get(p,p) for p in [p1,p2,p3,p4])
    sig = _sig_label(p1,p2,p3,p4)
    price_action = interp["price_action"] if interp else ""
    best_for     = interp["sector_best"]  if interp else ""
    # Yellow border markers for easy identification
    _sx_ltp = _get_ltp("SENSEX") or 0
    msg = (f"🟡🟡🟡🟡🟡🟡🟡🟡🟡🟡\n"
           f"🌙 <b>KP WINDOW OPEN</b>\n"
           f"⏰ Slot    : <b>{slot}</b>\n"
           f"🪐 Planets : {p1} {p2} {p3} {p4}\n"
           f"   ({pnames})\n"
           f"📡 Signal  : <b>{sig}</b>\n"
           f"📊 NIFTY: <b>{nifty_ltp:,.0f}</b>  |  BNIFTY: <b>{bnf_ltp:,.0f}</b>  |  SENSEX: <b>{_sx_ltp:,.0f}</b>\n"
           f"📈 Expect  : {price_action[:80]}...\n"
           f"🎯 Best for: {best_for[:60]}\n"
           f"🟡🟡🟡🟡🟡🟡🟡🟡🟡🟡")
    _send_tg(msg, key=f"KP_OPEN_{slot}_{p1}{p2}{p3}{p4}")


def _tg_high(sym, slot, high, ltp, pts, p1,p2,p3,p4):
    """
    sym   = "NIFTY" or "BANKNIFTY"
    slot  = "11:45-12:00"
    high  = Top-High level (highest high of the 3 candles in slot)
    ltp   = current live LTP at time of break
    pts   = how far LTP has moved above Top-High (ltp - high)
    p1-p4 = KP planet lords (Sign, Star, Sub, Sub-Sub)
    """
    sig   = _sig_label(p1,p2,p3,p4)
    pnames = " → ".join(PLANET_FULL.get(p,p) for p in [p1,p2,p3,p4])
    # Always get fresh LTP for both indices for context
    n_ltp  = _get_ltp("NIFTY")     or 0
    b_ltp  = _get_ltp("BANKNIFTY") or 0
    live_ltp = _get_ltp(sym) or ltp  # fresh LTP for the broken symbol
    breakout_pts = max(0, live_ltp - high)  # pts above Top-High
    slot_range = pts  # range = Top-High minus Least-Low (passed as pts)
    msg = (f"🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\n"
           f"🟢 <b>KP TOP-HIGH BREAK — {sym}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"⏰ KP Slot  : <b>{slot}</b>\n"
           f"🪐 Planets  : {p1}·{p2}·{p3}·{p4}\n"
           f"   ({pnames})\n"
           f"📡 Signal   : <b>{sig}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"📈 Top-High : <b>{high:,.2f}</b>  ← 15-min slot HIGH\n"
           f"💹 {sym} LTP: <b>{live_ltp:,.2f}</b>  ← current price\n"
           f"💰 Above TH : <b>+{breakout_pts:.0f} pts</b>  ← LTP minus Top-High\n"
           f"📏 Slot Range: {slot_range:.0f} pts  ← Top-High minus Least-Low\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"📊 NIFTY spot  : <b>{n_ltp:,.0f}</b>\n"
           f"🏦 BNIFTY spot : <b>{b_ltp:,.0f}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"✅ <b>BUY BREAKOUT CONFIRMED</b>\n"
           f"🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢")
    _send_tg(msg, key=f"KP_HIGH_{sym}_{slot}")


def _tg_low(sym, slot, low, ltp, pts, p1,p2,p3,p4):
    """
    sym   = "NIFTY" or "BANKNIFTY"
    slot  = "11:45-12:00"
    low   = Least-Low level (lowest low of the 3 candles in slot)
    ltp   = current live LTP at time of break
    pts   = slot range = Top-High minus Least-Low
    p1-p4 = KP planet lords (Sign, Star, Sub, Sub-Sub)
    """
    sig   = _sig_label(p1,p2,p3,p4)
    pnames = " → ".join(PLANET_FULL.get(p,p) for p in [p1,p2,p3,p4])
    n_ltp  = _get_ltp("NIFTY")     or 0
    b_ltp  = _get_ltp("BANKNIFTY") or 0
    live_ltp = _get_ltp(sym) or ltp
    breakdown_pts = max(0, low - live_ltp)  # pts below Least-Low
    slot_range = pts  # range = Top-High minus Least-Low
    msg = (f"🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴\n"
           f"🔴 <b>KP LEAST-LOW BREAK — {sym}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"⏰ KP Slot  : <b>{slot}</b>\n"
           f"🪐 Planets  : {p1}·{p2}·{p3}·{p4}\n"
           f"   ({pnames})\n"
           f"📡 Signal   : <b>{sig}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"📉 Least-Low: <b>{low:,.2f}</b>  ← 15-min slot LOW\n"
           f"💹 {sym} LTP: <b>{live_ltp:,.2f}</b>  ← current price\n"
           f"💰 Below LL : <b>-{breakdown_pts:.0f} pts</b>  ← Low minus LTP\n"
           f"📏 Slot Range: {slot_range:.0f} pts  ← Top-High minus Least-Low\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"📊 NIFTY spot  : <b>{n_ltp:,.0f}</b>\n"
           f"🏦 BNIFTY spot : <b>{b_ltp:,.0f}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"✅ <b>SELL BREAKOUT CONFIRMED</b>\n"
           f"🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴")
    _send_tg(msg, key=f"KP_LOW_{sym}_{slot}")


def _tg_false(sym, slot, direction, ltp, p1,p2,p3,p4):
    msg = (f"⚠️ <b>KP FALSE BREAKOUT — {sym}</b>\n"
           f"⏰ Slot : <b>{slot}</b>\n"
           f"🪐 {p1} {p2} {p3} {p4}\n"
           f"Broke <b>{direction}</b> then reversed\n"
           f"💹 LTP : <b>{ltp:,.2f}</b>\n"
           f"⚠️ <b>REVERSAL — exit / stay cautious</b>")
    _send_tg(msg, key=f"KP_FALSE_{sym}_{direction}_{slot}")


def _tg_summary(slot, nh, nl, bh, bl, n_broke, b_broke, p1,p2,p3,p4, sh=0, sl=0, s_broke="None"):
    n_ltp = _get_ltp("NIFTY")     or 0
    b_ltp = _get_ltp("BANKNIFTY") or 0
    s_ltp = _get_ltp("SENSEX")    or 0
    sig   = _sig_label(p1,p2,p3,p4)
    # Yellow border for summary
    def _broke_icon(broke):
        if "High" in broke and "False" not in broke: return "🟢 HIGH BREAK"
        if "Low"  in broke and "False" not in broke: return "🔴 LOW BREAK"
        if "False" in broke: return "⚠️ FALSE BREAK"
        if broke == "Both": return "🟡 BOTH BROKE"
        return "⬜ No Break"
    _sx_line = (
        f"<b>SENSEX</b>\n"
        f"  ▲ High: <b>{sh:,.0f}</b>  ▼ Low: <b>{sl:,.0f}</b>\n"
        f"  Spot LTP: <b>{s_ltp:,.0f}</b>  {_broke_icon(s_broke)}\n"
    ) if sh and sl else ""
    msg = (f"🟡🟡🟡🟡🟡🟡🟡🟡🟡🟡\n"
           f"📊 <b>KP WINDOW SUMMARY</b>\n"
           f"⏰ Slot : <b>{slot}</b>\n"
           f"🪐 {p1} {p2} {p3} {p4}  |  {sig}\n"
           f"━━━━━━━━━━━━━━━━━━━━━━\n"
           f"<b>NIFTY</b>\n"
           f"  ▲ High: <b>{nh:,.0f}</b>  ▼ Low: <b>{nl:,.0f}</b>\n"
           f"  Spot LTP: <b>{n_ltp:,.0f}</b>  {_broke_icon(n_broke)}\n"
           f"<b>BANKNIFTY</b>\n"
           f"  ▲ High: <b>{bh:,.0f}</b>  ▼ Low: <b>{bl:,.0f}</b>\n"
           f"  Spot LTP: <b>{b_ltp:,.0f}</b>  {_broke_icon(b_broke)}\n"
           f"{_sx_line}"
           f"🟡🟡🟡🟡🟡🟡🟡🟡🟡🟡")
    _send_tg(msg, key=f"KP_SUM_{slot}")


# ═══════════════════════════════════════════════════════════
# BREAKOUT MEMORY
# ═══════════════════════════════════════════════════════════

def _load_mem():
    try:
        with open(_MEM_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_mem(mem):
    with open(_MEM_PATH, "w", encoding='utf-8') as f:
        json.dump(mem, f, indent=2)


def _prune_mem(mem, keep=5):
    for old in sorted(mem.keys())[:-keep]:
        del mem[old]
    return mem


# ═══════════════════════════════════════════════════════════
# CSV LOADER
# ═══════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def _load_kp_csv():
    if not os.path.exists(_CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(_CSV_PATH, encoding='utf-8')
    df["DateObj"] = pd.to_datetime(df["Date"], format="%d/%b/%Y")
    df["Hour"]    = df["Time"].str.split(":").str[0].astype(int)
    df["Minute"]  = df["Time"].str.split(":").str[1].astype(int)
    return df


# ═══════════════════════════════════════════════════════════
# MISC UI HELPERS
# ═══════════════════════════════════════════════════════════

def _now_ist():
    try:
        import pytz
        return datetime.now(pytz.timezone("Asia/Kolkata"))
    except Exception:
        return datetime.now()


def _slot_min(s):
    h, m = map(int, s.split(":"))
    return h * 60 + m


def _badge(p):
    c = PLANET_COLOR.get(p,"#9e9e9e")
    return (f'<span style="background:{c};color:#000;font-weight:700;'
            f'padding:2px 8px;border-radius:4px;margin:1px 2px;font-size:13px">{p}</span>')


def _prow(*planets):
    return "".join(_badge(p) for p in planets)


def _card(bg, bc, content):
    return (f'<div style="background:{bg};border:1px solid {bc};'
            f'border-radius:8px;padding:10px 14px;margin:5px 0">{content}</div>')


# ═══════════════════════════════════════════════════════════
# MAIN TAB RENDERER
# ═══════════════════════════════════════════════════════════

def render_kp_tab():
    st.header("🌙 KP Panchang — Trading Windows")
    st.caption(
        "KP Panchang 2026 (Kanak Bosmia · KPAstro 4.5)  |  "
        "09:00–15:00 IST  |  15-min slots  |  "
        "Auto 5-min OHLC from Kite  |  Telegram → Panchak Alerts"
    )

    df = _load_kp_csv()
    if df.empty:
        st.error(f"❌ kp_panchang_2026.csv not found at {_CSV_PATH}")
        return

    now      = _now_ist()
    today    = now.date()
    tod_str  = today.strftime("%d/%b/%Y")
    date_key = today.strftime("%Y-%m-%d")
    now_min  = now.hour * 60 + now.minute

    today_df = df[df["Date"] == tod_str].copy()

    kite     = _get_kite()
    kite_ok  = kite is not None
    tok, cid = _get_tg_cfg()
    tg_ok    = bool(tok and cid)

    # status bar
    cols_st = st.columns(3)
    cols_st[0].markdown(
        f'<span style="font-size:12px;color:{"#00e676" if kite_ok else "#ef5350"}">'
        f'{"✅ Kite connected — auto OHLC ON" if kite_ok else "⚠️ Kite not connected — manual mode"}'
        f'</span>', unsafe_allow_html=True)
    cols_st[1].markdown(
        f'<span style="font-size:12px;color:{"#00e676" if tg_ok else "#ef5350"}">'
        f'{"✅ Telegram connected" if tg_ok else "❌ Telegram not configured"}'
        f'</span>', unsafe_allow_html=True)
    cols_st[2].markdown(
        f'<span style="font-size:12px;color:#80cbc4">'
        f'🕐 IST: {now.strftime("%H:%M:%S")}  |  {today_df.shape[0]} windows today</span>',
        unsafe_allow_html=True)

    # ── find active / next window ─────────────────────────
    active_row = None
    next_row   = None
    for _, row in today_df.iterrows():
        sm = _slot_min(row["Slot_Start"])
        em = _slot_min(row["Slot_End"])
        if sm <= now_min < em:
            active_row = row
        elif now_min < sm and next_row is None:
            next_row = row

    # top banner
    if active_row is not None:
        r   = active_row
        sig = _sig_label(r["P1"],r["P2"],r["P3"],r["P4"])
        sig_c = {"STRONG BUY 🟢🟢":"#00e676","BUY 🟢":"#00e676",
                 "MILD BUY 🟡":"#c6ff00","MIXED ⚪":"#bdbdbd",
                 "SELL 🔴":"#ff5252","STRONG SELL 🔴🔴":"#ff1744"}.get(sig,"#bdbdbd")
        mins_left = _slot_min(r["Slot_End"]) - now_min
        st.markdown(
            _card("#1b5e20","#00e676",
                f'<span style="font-size:17px;font-weight:700;color:#00e676">🟢 ACTIVE</span>'
                f'&nbsp;<b style="color:#fff">⏰ {r["Slot_Start"]}–{r["Slot_End"]}</b>'
                f'&nbsp;<span style="color:{sig_c};font-weight:700">{sig}</span>'
                f'&nbsp;<span style="color:#aaa;font-size:12px">{mins_left} min remaining</span>'
                f'<br>{_prow(r["P1"],r["P2"],r["P3"],r["P4"])}'
            ),
            unsafe_allow_html=True,
        )
    elif next_row is not None:
        r = next_row
        mins_away = _slot_min(r["Slot_Start"]) - now_min
        st.markdown(
            _card("#0d1b2a","#5c6bc0",
                f'<span style="color:#7986cb;font-weight:700;font-size:15px">'
                f'⏳ Next window in <b style="color:#ffb300">{mins_away} min</b></span>'
                f'&nbsp;<b style="color:#fff">⏰ {r["Slot_Start"]}–{r["Slot_End"]}</b>'
                f'<br>{_prow(r["P1"],r["P2"],r["P3"],r["P4"])}'
            ),
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 1 — TODAY'S WINDOWS  (auto OHLC + alerts)
    # ══════════════════════════════════════════════════════
    st.subheader(f"📅 Today — {now.strftime('%A, %d %b %Y')}")

    if today_df.empty:
        st.info("No KP windows today (holiday or weekend).")
    else:
        mon_key = f"kp_mon_{date_key}"
        if mon_key not in st.session_state:
            st.session_state[mon_key] = {}

        # ══════════════════════════════════════════════════
        # PASS 1 — OHLC TABLES ONLY (all slots, top of page)
        # First slot first, second slot below it.
        # ══════════════════════════════════════════════════
        st.markdown("#### 📊 5-Min OHLC — All Windows Today")
        _ohlc_df_all = _load_ohlc_today(date_key)
        _any_ohlc_shown = False

        for _, row in today_df.iterrows():
            p1o,p2o,p3o,p4o = row["P1"],row["P2"],row["P3"],row["P4"]
            slot_o  = row["Slot_15min"]
            s_str_o = row["Slot_Start"]
            e_str_o = row["Slot_End"]
            sm_o    = _slot_min(s_str_o)
            em_o    = _slot_min(e_str_o)
            is_act_o  = sm_o <= now_min < em_o
            is_past_o = now_min >= em_o
            show_o    = is_act_o or is_past_o

            # Initialise session state for this slot
            ss_o = st.session_state[mon_key].setdefault(slot_o, {
                "nh": None, "nl": None, "bh": None, "bl": None,
                "sh": None, "sl": None,            # SENSEX top_high / least_low
                "n_broke": "None", "b_broke": "None", "s_broke": "None",
                "n_ltp": 0.0, "b_ltp": 0.0,       "s_ltp": 0.0,
                "open_sent":False,"nh_sent":False,"nl_sent":False,
                "bh_sent":False,"bl_sent":False,
                "sh_sent":False,"sl_sent":False,   # SENSEX alert sent flags
                "nf_sent":False,"bf_sent":False,"sum_sent":False,
                "auto_fetched": False,
            })

            if not show_o:
                # Future slot — show placeholder
                sig_o = _sig_label(p1o,p2o,p3o,p4o)
                st.markdown(
                    f'<div style="background:#0d1b2a;border:1px dashed #37474f;border-radius:6px;'
                    f'padding:8px 14px;margin:4px 0;font-size:12px;color:#607d8b">'
                    f'⏳ <b>{s_str_o}–{e_str_o}</b> &nbsp;'
                    f'{_prow(p1o,p2o,p3o,p4o)}'
                    f'&nbsp;<span style="color:#ffb300">{sig_o}</span>'
                    f'&nbsp;— window not yet started</div>',
                    unsafe_allow_html=True,
                )
                continue

            # ── Auto-fetch when slot ends ──────────────────
            _slot_complete_o = is_past_o
            _need_fetch_o = _slot_complete_o and (ss_o.get("nh") is None or ss_o.get("bh") is None)
            if _need_fetch_o and not ss_o.get("auto_fetched"):
                for sym_f, hk_f, lk_f in [("NIFTY","nh","nl"),("BANKNIFTY","bh","bl"),("SENSEX","sh","sl")]:
                    if ss_o.get(hk_f) is None:
                        th, ll, _ = _fetch_and_cache_slot(sym_f, s_str_o, e_str_o, date_key)
                        if th is not None:
                            ss_o[hk_f] = th
                            ss_o[lk_f] = ll
                ss_o["auto_fetched"] = True

            # ── Reload OHLC df after possible fetch ────────
            ohlc_df_o = _load_ohlc_today(date_key)
            sh_o, sm2_o = map(int, s_str_o.split(":"))
            eh_o, em2_o = map(int, e_str_o.split(":"))

            def _slot_candles(sym_c, odf, sh, sm2, eh, em2):
                if odf is None or odf.empty or sym_c not in odf["symbol"].values:
                    return pd.DataFrame()
                if "datetime" not in odf.columns:
                    return pd.DataFrame()
                _odf = odf.copy()
                _odf["datetime"] = pd.to_datetime(_odf["datetime"], errors="coerce")
                _odf = _odf.dropna(subset=["datetime"])
                if _odf.empty:
                    return pd.DataFrame()
                return _odf[
                    (_odf["symbol"] == sym_c) &
                    (_odf["datetime"].dt.hour*60 + _odf["datetime"].dt.minute >= sh*60+sm2) &
                    (_odf["datetime"].dt.hour*60 + _odf["datetime"].dt.minute <  eh*60+em2)
                ].sort_values("datetime").reset_index(drop=True)

            n_cdf  = _slot_candles("NIFTY",     ohlc_df_o, sh_o,sm2_o,eh_o,em2_o)
            bn_cdf = _slot_candles("BANKNIFTY",  ohlc_df_o, sh_o,sm2_o,eh_o,em2_o)
            sx_cdf = _slot_candles("SENSEX",     ohlc_df_o, sh_o,sm2_o,eh_o,em2_o)

            # Auto-set high/low from candles
            for cdf_x, hk_x, lk_x in [(n_cdf,"nh","nl"),(bn_cdf,"bh","bl"),(sx_cdf,"sh","sl")]:
                if not cdf_x.empty:
                    if ss_o[hk_x] is None: ss_o[hk_x] = float(cdf_x["high"].max())
                    if ss_o[lk_x] is None: ss_o[lk_x] = float(cdf_x["low"].min())

            # Auto-detect broke
            for sym_ab, bk_ab, hk_ab, lk_ab in [
                ("NIFTY","n_broke","nh","nl"),
                ("BANKNIFTY","b_broke","bh","bl"),
                ("SENSEX","s_broke","sh","sl"),
            ]:
                if ss_o[bk_ab] == "None" and ss_o.get(hk_ab) and ss_o.get(lk_ab):
                    ltp_ab = _get_ltp(sym_ab) or 0
                    if ltp_ab > ss_o[hk_ab]:   ss_o[bk_ab] = "High"
                    elif ltp_ab < ss_o[lk_ab]:  ss_o[bk_ab] = "Low"

            # ── Build combined OHLC table ──────────────────
            def _cv_o(cdf, t_label, row_type):
                if cdf.empty: return "—"
                for _, r in cdf.iterrows():
                    if r["datetime"].strftime("%H:%M") == t_label:
                        v = r["high"] if row_type == "High" else r["low"]
                        return f"{v:,.0f}"
                return "—"

            n_times_o  = [r["datetime"].strftime("%H:%M") for _,r in n_cdf.iterrows()] if not n_cdf.empty else []
            bn_times_o = [r["datetime"].strftime("%H:%M") for _,r in bn_cdf.iterrows()] if not bn_cdf.empty else []
            sx_times_o = [r["datetime"].strftime("%H:%M") for _,r in sx_cdf.iterrows()] if not sx_cdf.empty else []
            all_times_o = sorted(set(n_times_o + bn_times_o + sx_times_o)) or [
                s_str_o,
                f"{sh_o:02d}:{sm2_o+5:02d}" if sm2_o+5 < 60 else f"{sh_o+1:02d}:{(sm2_o+5)%60:02d}",
                f"{sh_o:02d}:{sm2_o+10:02d}" if sm2_o+10 < 60 else f"{sh_o+1:02d}:{(sm2_o+10)%60:02d}",
            ]

            nh_o  = ss_o.get("nh") or 0
            nl_o  = ss_o.get("nl") or 0
            bh_o  = ss_o.get("bh") or 0
            bl_o  = ss_o.get("bl") or 0
            sh_o  = ss_o.get("sh") or 0
            sl_o  = ss_o.get("sl") or 0
            n_ltp_o = _get_ltp("NIFTY")     or 0
            b_ltp_o = _get_ltp("BANKNIFTY") or 0
            s_ltp_o = _get_ltp("SENSEX")    or 0
            date_disp_o = now.strftime("%A, %d %b %Y")
            sig_o = _sig_label(p1o,p2o,p3o,p4o)
            stype_o = SUB_SIGNAL.get(p3o,("","neutral"))[1]
            sig_co = {"bullish":"#00e676","mild_bull":"#c6ff00","neutral":"#bdbdbd","bearish":"#ff5252"}.get(stype_o,"#bdbdbd")

            # Slot label row above table
            status_o = "🟢 ACTIVE" if is_act_o else "✅ Done"
            st.markdown(
                f'<div style="background:#111;border-left:3px solid {sig_co};'
                f'padding:6px 12px;margin:10px 0 2px 0;border-radius:4px">'
                f'<b style="color:#fff">⏰ {s_str_o}–{e_str_o}</b>'
                f'&nbsp;{_prow(p1o,p2o,p3o,p4o)}'
                f'&nbsp;<span style="color:{sig_co};font-weight:700">{sig_o}</span>'
                f'&nbsp;<b style="color:#aaa;font-size:11px">{status_o}</b>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Fetch button + status
            fc1, fc2 = st.columns([1,3])
            with fc1:
                if st.button("🔄 Fetch OHLC", key=f"fetch_top_{slot_o}"):
                    with st.spinner("Fetching..."):
                        for sym_f2,hk_f2,lk_f2 in [("NIFTY","nh","nl"),("BANKNIFTY","bh","bl"),("SENSEX","sh","sl")]:
                            th2,ll2,_ = _fetch_and_cache_slot(sym_f2, s_str_o, e_str_o, date_key)
                            if th2 is not None:
                                ss_o[hk_f2]=th2; ss_o[lk_f2]=ll2; ss_o["auto_fetched"]=True
            with fc2:
                if _slot_complete_o and ss_o.get("auto_fetched"):
                    st.success("✅ OHLC auto-fetched after slot ended")
                elif _slot_complete_o:
                    st.warning("⏳ Auto-fetching... refresh to load")
                else:
                    st.info(f"⏳ Auto-fetch after slot ends ({em_o - now_min} min remaining)")

            # Build HTML table
            def _ltp_ind_o(ltp, top, low):
                if not ltp or not top or not low: return ""
                if ltp > top:   return f' <span style="color:#00e676">▲ BREAK +{ltp-top:.0f}</span>'
                elif ltp < low: return f' <span style="color:#ff5252">▼ BREAK {ltp-low:.0f}</span>'
                rng = top-low
                pct = int((ltp-low)/rng*100) if rng else 50
                return f' <span style="color:#bdbdbd">⚪ {pct}% in range</span>'

            def _build_sym_row_o(sym_label, cdf, top, low, ltp_v):
                hdr = ""
                for t in all_times_o[:3]:
                    hdr += f'<th style="padding:6px 10px;background:#1a237e;color:#fff;text-align:center">{t}</th>'
                for _ in range(3-len(all_times_o[:3])):
                    hdr += '<th style="padding:6px 10px;background:#1a237e;color:#fff;text-align:center">—</th>'

                high_cells = ""
                for t in all_times_o[:3]:
                    v = _cv_o(cdf, t, "High")
                    is_top = (v!="—" and top and abs(float(v.replace(",",""))-top)<0.01)
                    bg2 = "background:#00400a;color:#00e676;font-weight:700" if is_top else "color:#c8e6c9"
                    high_cells += f'<td style="padding:5px 10px;text-align:center;{bg2}">{v}</td>'
                for _ in range(3-len(all_times_o[:3])):
                    high_cells += '<td style="padding:5px 10px;text-align:center;color:#555">—</td>'

                low_cells = ""
                for t in all_times_o[:3]:
                    v = _cv_o(cdf, t, "Low")
                    is_bot = (v!="—" and low and abs(float(v.replace(",",""))-low)<0.01)
                    bg2 = "background:#400000;color:#ff5252;font-weight:700" if is_bot else "color:#ffcdd2"
                    low_cells += f'<td style="padding:5px 10px;text-align:center;{bg2}">{v}</td>'
                for _ in range(3-len(all_times_o[:3])):
                    low_cells += '<td style="padding:5px 10px;text-align:center;color:#555">—</td>'

                ltp_ind = _ltp_ind_o(ltp_v, top, low)
                rows = (
                    f'<tr>'
                    f'<td rowspan="2" style="padding:5px 10px;font-weight:700;color:#fff;text-align:center;'
                    f'border-right:1px solid #333;vertical-align:middle">'
                    f'{sym_label}<br><span style="font-size:11px;color:#aaa">'
                    f'LTP:{ltp_v:,.0f}{ltp_ind}</span></td>'
                    f'<td style="padding:5px 10px;color:#aaa">{date_disp_o}<br>'
                    f'<span style="color:#c8e6c9;font-size:12px">High</span></td>'
                    f'{high_cells}'
                    f'<td rowspan="2" style="padding:5px 10px;text-align:center;background:#00400a;'
                    f'color:#00e676;font-weight:700;font-size:15px;vertical-align:middle">'
                    f'{"—" if not top else f"{top:,.0f}"}</td>'
                    f'<td rowspan="2" style="padding:5px 10px;text-align:center;background:#400000;'
                    f'color:#ff5252;font-weight:700;font-size:15px;vertical-align:middle">'
                    f'{"—" if not low else f"{low:,.0f}"}</td>'
                    f'</tr>'
                    f'<tr>'
                    f'<td style="padding:5px 10px;color:#ffcdd2;font-size:12px">Low</td>'
                    f'{low_cells}'
                    f'</tr>'
                )
                hdr_row = (
                    f'<tr>'
                    f'<th style="padding:6px 10px;background:#1a237e;color:#fff;text-align:center">Symbol</th>'
                    f'<th style="padding:6px 10px;background:#1a237e;color:#fff">Date / Row</th>'
                    f'{hdr}'
                    f'<th style="padding:6px 10px;background:#00400a;color:#00e676;text-align:center">TOP HIGH</th>'
                    f'<th style="padding:6px 10px;background:#400000;color:#ff5252;text-align:center">LEAST LOW</th>'
                    f'</tr>'
                )
                return hdr_row, rows

            hdr_n,  rows_n  = _build_sym_row_o("NIFTY",     n_cdf,  nh_o, nl_o, n_ltp_o)
            hdr_bn, rows_bn = _build_sym_row_o("BANKNIFTY",  bn_cdf, bh_o, bl_o, b_ltp_o)
            hdr_sx, rows_sx = _build_sym_row_o("SENSEX",     sx_cdf, sh_o, sl_o, s_ltp_o)
            spacer = '<tr><td colspan="7" style="height:5px;background:#0a0a1a"></td></tr>'

            st.markdown(
                f'<div style="overflow-x:auto;margin:4px 0 12px 0">'
                f'<table style="width:100%;border-collapse:collapse;background:#111;border-radius:8px;font-size:13px">'
                f'<thead>{hdr_n}</thead>'
                f'<tbody>{rows_n}{spacer}{rows_bn}{spacer}{rows_sx}</tbody>'
                f'</table></div>',
                unsafe_allow_html=True,
            )

            # ── Broke? dropdowns (below table) ────────────
            broke_opts_o = ["None","High","Low","Both","False-High","False-Low"]
            bc1_o, bc2_o, bc3_o = st.columns(3)
            ss_o["n_broke"] = bc1_o.selectbox(
                "NIFTY — Broke? (auto)", broke_opts_o,
                index=broke_opts_o.index(ss_o["n_broke"]),
                key=f"broke_N_top_{slot_o}"
            )
            ss_o["b_broke"] = bc2_o.selectbox(
                "BANKNIFTY — Broke? (auto)", broke_opts_o,
                index=broke_opts_o.index(ss_o["b_broke"]),
                key=f"broke_B_top_{slot_o}"
            )
            ss_o["s_broke"] = bc3_o.selectbox(
                "SENSEX — Broke? (auto)", broke_opts_o,
                index=broke_opts_o.index(ss_o["s_broke"]),
                key=f"broke_S_top_{slot_o}"
            )

            # ── Telegram buttons ───────────────────────────
            tb1,tb2,tb3,tb4,tb5,tb6 = st.columns(6)
            interp_o = _kp_interpret(p1o,p2o,p3o,p4o, slot_o, row["Time"])
            if tb1.button("🔔 Open",  key=f"btntg_op_top_{slot_o}"):
                _tg_open(slot_o,p1o,p2o,p3o,p4o, n_ltp_o, b_ltp_o, interp_o)
                st.toast("📤 Window-open alert sent!")
            if tb2.button("🟢 N↑",   key=f"btntg_nh_top_{slot_o}"):
                _tg_high("NIFTY",slot_o,nh_o,n_ltp_o,abs(nh_o-nl_o),p1o,p2o,p3o,p4o)
                st.toast("📤 NIFTY high-break sent!")
            if tb3.button("🔴 N↓",   key=f"btntg_nl_top_{slot_o}"):
                _tg_low("NIFTY",slot_o,nl_o,n_ltp_o,abs(nh_o-nl_o),p1o,p2o,p3o,p4o)
                st.toast("📤 NIFTY low-break sent!")
            if tb4.button("📊 BNF",  key=f"btntg_bnf_top_{slot_o}"):
                if ss_o["b_broke"] in ("High","Both"):
                    _tg_high("BANKNIFTY",slot_o,bh_o,b_ltp_o,abs(bh_o-bl_o),p1o,p2o,p3o,p4o)
                else:
                    _tg_low("BANKNIFTY",slot_o,bl_o,b_ltp_o,abs(bh_o-bl_o),p1o,p2o,p3o,p4o)
                st.toast("📤 BankNifty alert sent!")
            if tb5.button("📈 SX",   key=f"btntg_sx_top_{slot_o}"):
                if ss_o["s_broke"] in ("High","Both"):
                    _tg_high("SENSEX",slot_o,sh_o,s_ltp_o,abs(sh_o-sl_o),p1o,p2o,p3o,p4o)
                else:
                    _tg_low("SENSEX",slot_o,sl_o,s_ltp_o,abs(sh_o-sl_o),p1o,p2o,p3o,p4o)
                st.toast("📤 Sensex alert sent!")
            if tb6.button("📋 Sum",  key=f"btntg_sum_top_{slot_o}"):
                _tg_summary(slot_o,nh_o,nl_o,bh_o,bl_o,
                            ss_o["n_broke"],ss_o["b_broke"],p1o,p2o,p3o,p4o,
                            sh=sh_o, sl=sl_o, s_broke=ss_o["s_broke"])
                st.toast("📤 Summary sent!")

            # ── AUTO alerts ────────────────────────────────
            if is_act_o and not ss_o["open_sent"]:
                _tg_open(slot_o,p1o,p2o,p3o,p4o, n_ltp_o, b_ltp_o, interp_o)
                ss_o["open_sent"] = True
            if nh_o and ss_o["n_broke"] in ("High","Both") and not ss_o["nh_sent"]:
                _tg_high("NIFTY",slot_o,nh_o,n_ltp_o,abs(nh_o-nl_o),p1o,p2o,p3o,p4o); ss_o["nh_sent"]=True
            if nl_o and ss_o["n_broke"] in ("Low","Both") and not ss_o["nl_sent"]:
                _tg_low("NIFTY",slot_o,nl_o,n_ltp_o,abs(nh_o-nl_o),p1o,p2o,p3o,p4o);  ss_o["nl_sent"]=True
            if bh_o and ss_o["b_broke"] in ("High","Both") and not ss_o["bh_sent"]:
                _tg_high("BANKNIFTY",slot_o,bh_o,b_ltp_o,abs(bh_o-bl_o),p1o,p2o,p3o,p4o); ss_o["bh_sent"]=True
            if bl_o and ss_o["b_broke"] in ("Low","Both") and not ss_o["bl_sent"]:
                _tg_low("BANKNIFTY",slot_o,bl_o,b_ltp_o,abs(bh_o-bl_o),p1o,p2o,p3o,p4o); ss_o["bl_sent"]=True
            if sh_o and ss_o["s_broke"] in ("High","Both") and not ss_o["sh_sent"]:
                _tg_high("SENSEX",slot_o,sh_o,s_ltp_o,abs(sh_o-sl_o),p1o,p2o,p3o,p4o); ss_o["sh_sent"]=True
            if sl_o and ss_o["s_broke"] in ("Low","Both") and not ss_o["sl_sent"]:
                _tg_low("SENSEX",slot_o,sl_o,s_ltp_o,abs(sh_o-sl_o),p1o,p2o,p3o,p4o);  ss_o["sl_sent"]=True
            if ss_o["n_broke"] in ("False-High","False-Low") and not ss_o["nf_sent"]:
                d_ab = ss_o["n_broke"].replace("False-","")
                _tg_false("NIFTY",slot_o,d_ab,nh_o if d_ab=="High" else nl_o,p1o,p2o,p3o,p4o)
                ss_o["nf_sent"] = True
            if ss_o["b_broke"] in ("False-High","False-Low") and not ss_o["bf_sent"]:
                d_ab = ss_o["b_broke"].replace("False-","")
                _tg_false("BANKNIFTY",slot_o,d_ab,bh_o if d_ab=="High" else bl_o,p1o,p2o,p3o,p4o)
                ss_o["bf_sent"] = True
            if is_past_o and not ss_o["sum_sent"] and (nh_o or bh_o):
                _tg_summary(slot_o,nh_o,nl_o,bh_o,bl_o,
                            ss_o["n_broke"],ss_o["b_broke"],p1o,p2o,p3o,p4o,
                            sh=sh_o, sl=sl_o, s_broke=ss_o["s_broke"])
                ss_o["sum_sent"] = True

            # ── Push current slot OHLC to session_state for main dashboard ──
            # This lets fire_nifty_slot_break_alert() in the main file read
            # the exact KP slot TOP HIGH / LEAST LOW for break + progress alerts.
            if "kp_slot_ohlc" not in st.session_state:
                st.session_state["kp_slot_ohlc"] = {}
            if nh_o:
                st.session_state["kp_slot_ohlc"]["NIFTY"] = {
                    "top_high":  nh_o,
                    "least_low": nl_o,
                    "slot":      slot_o,
                }
            if bh_o:
                st.session_state["kp_slot_ohlc"]["BANKNIFTY"] = {
                    "top_high":  bh_o,
                    "least_low": bl_o,
                    "slot":      slot_o,
                }
            if sh_o:
                st.session_state["kp_slot_ohlc"]["SENSEX"] = {
                    "top_high":  sh_o,
                    "least_low": sl_o,
                    "slot":      slot_o,
                }

            # ── 15-Min PROGRESS ALERT after a valid break ──────────────────
            # Fires once per 15-min window showing points gained/lost since break.
            # Uses existing _send_tg() so it respects the KP_ALERTS toggle.
            _now_prog    = _now_ist()
            _slot15_min  = (_now_prog.minute // 15) * 15
            _slot15_str  = _now_prog.strftime(f"%H:{_slot15_min:02d}")
            _today_str2  = _now_prog.strftime("%Y%m%d")
            _now_str_prog = _now_prog.strftime("%H:%M IST")

            for _sym_p, _broke_p, _top_p, _low_p, _ltp_p in [
                ("NIFTY",     ss_o["n_broke"], nh_o, nl_o, n_ltp_o),
                ("BANKNIFTY", ss_o["b_broke"], bh_o, bl_o, b_ltp_o),
                ("SENSEX",    ss_o["s_broke"], sh_o, sl_o, s_ltp_o),
            ]:
                if not _top_p or not _low_p or not _ltp_p:
                    continue
                # Only send progress if a real break is active (not false, not None)
                _broke_up   = _broke_p in ("High", "Both") and "False" not in _broke_p
                _broke_down = _broke_p in ("Low",  "Both") and "False" not in _broke_p
                if not _broke_up and not _broke_down:
                    continue

                _prog_tg_key = f"KP_PROG_{_sym_p}_{slot_o}_{_today_str2}_{_slot15_str}"
                _direction   = "UP"   if _broke_up else "DOWN"
                _level       = _top_p if _broke_up  else _low_p
                _level_name  = "TOP HIGH" if _broke_up else "LEAST LOW"
                _icon        = "🟢"   if _broke_up  else "🔴"
                _arrow       = "↑"    if _broke_up  else "↓"
                _pts_from_level = (
                    round(_ltp_p - _top_p, 2) if _broke_up
                    else round(_low_p - _ltp_p, 2)
                )
                # Points from the original break entry (stored in ss_o)
                _entry_key   = f"prog_entry_{_sym_p}_{slot_o}"
                if _entry_key not in ss_o:
                    ss_o[_entry_key] = _ltp_p   # record first LTP at break
                _orig_entry  = ss_o[_entry_key]
                _pts_since   = (
                    round(_ltp_p - _orig_entry, 2) if _broke_up
                    else round(_orig_entry - _ltp_p, 2)
                )
                _trend_icon  = "📈" if _pts_since >= 0 else "📉"
                _sign        = "+" if _pts_since >= 0 else ""

                _prog_msg = (
                    f"⏱️ <b>{_sym_p} — 15-Min Progress {_arrow}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ <b>Time:</b> {_now_str_prog}  "
                    f"🕐 <b>KP Slot:</b> {slot_o}\n"
                    f"Direction: {_icon} <b>{'UP — Above TOP HIGH' if _broke_up else 'DOWN — Below LEAST LOW'}</b>\n"
                    f"🪐 Planets: {p1o}·{p2o}·{p3o}·{p4o}  "
                    f"📡 {_sig_label(p1o,p2o,p3o,p4o)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📍 <b>{_level_name}:</b> {_level:,.2f}\n"
                    f"🎯 <b>Break Entry LTP:</b> {_orig_entry:,.2f}\n"
                    f"💹 <b>LTP Now:</b> <b>{_ltp_p:,.2f}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📌 Pts from level : <b>+{abs(_pts_from_level):,.2f} pts</b>\n"
                    f"{_trend_icon} Pts since entry : <b>{_sign}{_pts_since:,.2f} pts</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <i>NOT financial advice.</i>"
                )
                _send_tg(_prog_msg, key=_prog_tg_key)

            # Auto-save to memory
            if is_past_o and (nh_o or bh_o):
                mem_o = _load_mem()
                mem_o.setdefault(date_key, {})
                for sym_m,hv_m,lv_m,bv_m in [
                    ("NIFTY",nh_o,nl_o,ss_o["n_broke"]),
                    ("BANKNIFTY",bh_o,bl_o,ss_o["b_broke"]),
                ]:
                    if hv_m:
                        kid_m = f"{sym_m}_{slot_o}"
                        mem_o[date_key][kid_m] = {
                            "symbol":sym_m,"slot":slot_o,
                            "top_high":hv_m,"least_low":lv_m,"broke":bv_m,
                            "pts_high":abs(hv_m-lv_m) if "High" in bv_m and "False" not in bv_m else 0,
                            "pts_low":abs(hv_m-lv_m)  if "Low"  in bv_m and "False" not in bv_m else 0,
                            "false_breakout":1 if "False" in bv_m else 0,
                            "p1":p1o,"p2":p2o,"p3":p3o,"p4":p4o,
                            "signal":_sig_label(p1o,p2o,p3o,p4o),"auto":True,
                        }
                _prune_mem(mem_o); _save_mem(mem_o)

            _any_ohlc_shown = True

        st.markdown("---")

        # ══════════════════════════════════════════════════
        # PASS 2 — KP INTERPRETATIONS PER WINDOW (below tables)
        # ══════════════════════════════════════════════════
        st.markdown("#### 🪐 KP Interpretations — All Windows")

        for _, row in today_df.iterrows():
            p1,p2,p3,p4 = row["P1"],row["P2"],row["P3"],row["P4"]
            slot   = row["Slot_15min"]
            s_str  = row["Slot_Start"]
            e_str  = row["Slot_End"]
            sm     = _slot_min(s_str)
            em     = _slot_min(e_str)
            is_act  = sm <= now_min < em
            is_past = now_min >= em
            show_ctrl = is_act or (is_past and now_min - em <= 30)

            interp = _kp_interpret(p1,p2,p3,p4, slot, row["Time"])
            sig    = interp["signal_label"]
            stype  = interp["signal_type"]
            sig_c  = {"bullish":"#00e676","mild_bull":"#c6ff00",
                      "neutral":"#bdbdbd","bearish":"#ff5252"}.get(stype,"#bdbdbd")
            bc = "#00e676" if is_act else ("#444" if is_past else "#5c6bc0")
            bg = "#1b5e20" if is_act else ("#111" if is_past else "#0d1b2a")
            status = "🟢 ACTIVE" if is_act else ("✅ Done" if is_past else "⏳ Soon")
            pnames = "  ·  ".join(PLANET_FULL.get(p,p) for p in [p1,p2,p3,p4])

            st.markdown(
                _card(bg, bc,
                    f'{_prow(p1,p2,p3,p4)}'
                    f'&nbsp;<b style="color:#fff">⏰ {s_str}–{e_str}</b>'
                    f'&nbsp;<span style="color:{sig_c};font-weight:700">{sig}</span>'
                    f'&nbsp;<b style="color:{bc}">{status}</b>'
                    f'<br><span style="color:#9e9e9e;font-size:11px">'
                    f'Sign:{p1} Star:{p2} <b>Sub:{p3}</b> SubSub:{p4}'
                    f'&nbsp;|&nbsp;{pnames}&nbsp;|&nbsp;Raw: {row["Time"]}</span>'
                ),
                unsafe_allow_html=True,
            )
            _render_interpretation(interp, expanded=is_act)



    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 2 — NEXT 5 TRADING DAYS
    # ══════════════════════════════════════════════════════
    st.subheader("📆 Upcoming — Next 5 Trading Days")

    upcoming = []
    for delta in range(1, 12):
        d = today + timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        d_str = d.strftime("%d/%b/%Y")
        chunk = df[df["Date"] == d_str]
        if not chunk.empty:
            upcoming.append((d_str, d.strftime("%A"), chunk))
        if len(upcoming) >= 5:
            break

    for d_str, day_name, chunk in upcoming:
        with st.expander(f"📅 {day_name}  {d_str}  ({len(chunk)} windows)", expanded=False):
            for _, row in chunk.iterrows():
                p1,p2,p3,p4 = row["P1"],row["P2"],row["P3"],row["P4"]
                interp = _kp_interpret(p1,p2,p3,p4, row["Slot_15min"], row["Time"])
                sig    = interp["signal_label"]
                stype  = interp["signal_type"]
                sig_c  = {"bullish":"#00e676","mild_bull":"#c6ff00",
                          "neutral":"#bdbdbd","bearish":"#ff5252"}.get(stype,"#bdbdbd")

                st.markdown(
                    _card("#0d1b2a","#37474f",
                        f'<b style="color:#80cbc4">⏰ {row["Slot_Start"]}–{row["Slot_End"]}</b>'
                        f'&nbsp;{_prow(p1,p2,p3,p4)}'
                        f'&nbsp;<span style="color:{sig_c};font-weight:700">{sig}</span>'
                        f'<br><span style="color:#9e9e9e;font-size:11px">'
                        f'Sub:{p3}={PLANET_FULL[p3]} → {interp["rules_sub"][:60]}...'
                        f'&nbsp;|&nbsp;Best: {interp["sector_best"][:40]}'
                        f'</span>'
                    ),
                    unsafe_allow_html=True,
                )

    if not upcoming:
        st.info("No upcoming windows found.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 3 — BREAKOUT MEMORY (last 5 days, auto-saved)
    # ══════════════════════════════════════════════════════
    st.subheader("💾 Breakout Memory — Last 5 Days (Auto-Saved)")

    mem = _load_mem()

    if not mem:
        st.info("No records yet. Records auto-save when a window ends with OHLC data.")
    else:
        for dk in sorted(mem.keys(), reverse=True):
            if not mem[dk]:
                continue
            d_obj = datetime.strptime(dk, "%Y-%m-%d")
            st.markdown(f"### 📅 {d_obj.strftime('%A, %d %b %Y')}")

            for kid, rec in mem[dk].items():
                broke     = rec.get("broke","None")
                is_false  = rec.get("false_breakout",0)
                pts_h     = rec.get("pts_high",0)
                pts_l     = rec.get("pts_low",0)
                p1b,p2b   = rec.get("p1",""),rec.get("p2","")
                p3b,p4b   = rec.get("p3",""),rec.get("p4","")
                sig_b     = rec.get("signal","")
                auto_b    = "🤖" if rec.get("auto") else "✏️"

                if "False" in broke:
                    bc2, icon = "#ff9800", f"⚠️ FALSE {broke.replace('False-','')}"
                elif broke == "Both":
                    bc2, icon = "#ffd740", "🟡 BOTH BROKE"
                elif broke == "High":
                    bc2, icon = "#00e676", "🟢 HIGH BREAK"
                elif broke == "Low":
                    bc2, icon = "#ff5252", "🔴 LOW BREAK"
                else:
                    bc2, icon = "#9e9e9e", "⬜ No Break"

                pts_txt = ""
                if pts_h: pts_txt += f"  📈 +{pts_h:.0f} pts"
                if pts_l: pts_txt += f"  📉 +{pts_l:.0f} pts"

                planet_html = _prow(p1b,p2b,p3b,p4b) if all([p1b,p2b,p3b,p4b]) else ""

                sig_b_span = f'<span style="color:#ffb300">{sig_b}</span>'
                st.markdown(
                    f'<div style="background:#1a1a2e;border:1px solid #2d2d5e;'
                    f'border-left:4px solid {bc2};border-radius:6px;'
                    f'padding:10px 14px;margin:5px 0">'
                    f'<b style="color:#e0e0e0">{auto_b} {rec["symbol"]}</b>'
                    f'&nbsp;<span style="color:#80cbc4">⏰ {rec["slot"]}</span>'
                    f'&nbsp;{planet_html}'
                    f'<br>'
                    f'▲ <b style="color:#00e676">{rec["top_high"]:,.0f}</b>'
                    f'&nbsp;&nbsp;▼ <b style="color:#ff5252">{rec["least_low"]:,.0f}</b>'
                    f'&nbsp;&nbsp;<b style="color:{bc2}">{icon}</b>'
                    f'{"&nbsp;&nbsp;" + pts_txt if pts_txt else ""}'
                    f'{"&nbsp;&nbsp;" + sig_b_span if sig_b else ""}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        # Stats
        st.markdown("---")
        st.subheader("📊 Stats (Last 5 Days)")
        total = false_c = high_b = low_b = 0
        total_pts = 0.0
        win_signals = {}
        for dk_s, entries in mem.items():
            for _, rec in entries.items():
                total += 1
                broke = rec.get("broke","None")
                sig_r = rec.get("signal","")
                if rec.get("false_breakout"): false_c += 1
                if "High" in broke and "False" not in broke:
                    high_b    += 1
                    total_pts += rec.get("pts_high",0)
                if "Low" in broke and "False" not in broke:
                    low_b     += 1
                    total_pts += rec.get("pts_low",0)
                if sig_r:
                    win_signals[sig_r] = win_signals.get(sig_r,0) + 1

        c = st.columns(5)
        c[0].metric("Windows", total)
        c[1].metric("🟢 High Breaks", high_b)
        c[2].metric("🔴 Low Breaks",  low_b)
        c[3].metric("⚠️ False",        false_c)
        c[4].metric("💰 Points",       f"{total_pts:.0f}")

        if win_signals:
            st.markdown("**Signals distribution:**  " +
                        "  |  ".join(f"`{k}` × {v}" for k,v in
                                     sorted(win_signals.items(), key=lambda x:-x[1])))

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 4 — CSV Browser
    # ══════════════════════════════════════════════════════
    with st.expander("🔍 Browse KP Panchang 2026", expanded=False):
        months    = sorted(df["DateObj"].dt.strftime("%b %Y").unique())
        sel_month = st.selectbox("Month", months, key="kp_mb")
        filt = df[df["DateObj"].dt.strftime("%b %Y") == sel_month][
            ["Date","Day","Planet_String","Time","Slot_15min"]
        ].copy()
        filt.columns = ["Date","Day","Planets","Time","Slot"]
        # Add signal column dynamically
        filt["Signal"] = filt["Planets"].apply(
            lambda x: _sig_label(*x.split()[:4]) if len(x.split()) >= 4 else ""
        )
        st.dataframe(filt, width='stretch', hide_index=True)
        st.caption(f"{len(filt)} windows in {sel_month}  |  Total 2026: {len(df)}")

    # ── OHLC CSV download ──────────────────────────────────
    with st.expander("📥 Download Today's 5-Min OHLC CSV", expanded=False):
        fp = _ohlc_csv_path(date_key)
        if os.path.exists(fp):
            with open(fp) as f:
                st.download_button(
                    "⬇️ Download OHLC CSV",
                    data=f.read(),
                    file_name=f"kp_ohlc_{date_key}.csv",
                    mime="text/csv",
                )
        else:
            st.info("No OHLC data captured yet today.")
