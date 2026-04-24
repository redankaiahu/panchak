#!/usr/bin/env python3
"""
patch_dashboard.py
==================
Run this ONCE on your EC2 (or Windows) to permanently patch
panchak_kite_dashboard_fixed28_4.py with ALL known fixes.

Usage:
    python3 patch_dashboard.py

It creates a backup first, then patches in-place.
Re-running is safe — already-patched lines are skipped automatically.
"""

import os, sys, shutil, ast
from datetime import datetime

TARGET = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "panchak_kite_dashboard_fixed28_4.py")

if not os.path.exists(TARGET):
    print(f"❌  File not found: {TARGET}")
    sys.exit(1)

# ── Backup ────────────────────────────────────────────────────────────────
backup = TARGET.replace(".py", f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
shutil.copy2(TARGET, backup)
print(f"✅  Backup created: {os.path.basename(backup)}")

with open(TARGET, "r", encoding="utf-8") as f:
    src = f.read()

applied = []
skipped = []

def patch(tag, old, new, count=1):
    """Replace old→new in src. Skip silently if already patched."""
    global src
    if old in src:
        src = src.replace(old, new, count)
        applied.append(tag)
    elif new.split('\n')[0].strip() in src or tag.split(':')[0] + ' already' in src:
        skipped.append(f"{tag} (already applied)")
    else:
        skipped.append(f"{tag} (pattern not found — may be refactored)")

# ══════════════════════════════════════════════════════════════════════════
# FIX 1 — CACHE_DIR defined BEFORE EMAIL_META_FILE
# Without this: NameError: name 'CACHE_DIR' is not defined (line ~1204)
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-1a: remove late BASE_DIR block",
    "BASE_DIR = os.getcwd()\n"
    "CACHE_DIR = os.path.join(BASE_DIR, \"CACHE\")\n"
    "os.makedirs(CACHE_DIR, exist_ok=True)\n",
    "# BASE_DIR/CACHE_DIR moved earlier by patch_dashboard.py (FIX-1)\n")

patch("FIX-1b: inject BASE_DIR/CACHE_DIR early",
    'EMAIL_META_FILE = "CACHE/email_meta.json"',
    'import sys as _sys\n'
    '# ── FIX-1: BASE_DIR/CACHE_DIR defined here (before EMAIL_META_FILE uses CACHE_DIR) ──\n'
    '# os.getcwd() varies by launch method; __file__ is always the script location.\n'
    'BASE_DIR  = os.path.dirname(os.path.abspath(__file__))\n'
    'CACHE_DIR = os.path.join(BASE_DIR, "CACHE")\n'
    'os.makedirs(CACHE_DIR, exist_ok=True)\n'
    '\n'
    'EMAIL_META_FILE = "CACHE/email_meta.json"')

# ══════════════════════════════════════════════════════════════════════════
# FIX 2 — All hardcoded "CACHE/..." string paths → os.path.join(CACHE_DIR)
# Without this: wrong/missing paths on Windows; NameError if CACHE_DIR not set
# ══════════════════════════════════════════════════════════════════════════
for tag, old, fname in [
    ("FIX-2a", 'EMAIL_META_FILE = "CACHE/email_meta.json"',   "email_meta.json"),
    ("FIX-2b", 'EMAIL_DEDUP_FILE = "CACHE/email_dedup.csv"',  "email_dedup.csv"),
    ("FIX-2c", 'ALERTS_DEDUP_FILE = "CACHE/alerts_dedup.csv"',"alerts_dedup.csv"),
    ("FIX-2d", 'ALERTS_LOG_FILE = "CACHE/alerts_log.csv"',    "alerts_log.csv"),
]:
    varname = old.split(" = ")[0].strip()
    patch(f"{tag}: {varname} absolute path", old,
          f'{varname:<17} = os.path.join(CACHE_DIR, "{fname}")  # {tag}')

patch("FIX-2e: f'CACHE/symbol_interval'",
    '    path = f"CACHE/{symbol}_{interval}.csv"',
    '    path = os.path.join(CACHE_DIR, f"{symbol}_{interval}.csv")  # FIX-2e')

patch("FIX-2f: f'CACHE/name_prev.txt'",
    '    path = f"CACHE/{name}_prev.txt"',
    '    path = os.path.join(CACHE_DIR, f"{name}_prev.txt")  # FIX-2f')

patch("FIX-2g: f'CACHE/name_15m_prev.txt'",
    '    path = f"CACHE/{name}_15m_prev.txt"',
    '    path = os.path.join(CACHE_DIR, f"{name}_15m_prev.txt")  # FIX-2g')

n2h = src.count('os.makedirs("CACHE", exist_ok=True)')
if n2h:
    src = src.replace('os.makedirs("CACHE", exist_ok=True)',
                      'os.makedirs(CACHE_DIR, exist_ok=True)  # FIX-2h')
    applied.append(f"FIX-2h: {n2h}x os.makedirs('CACHE') → CACHE_DIR")

# ══════════════════════════════════════════════════════════════════════════
# FIX 3 — ACCESS_TOKEN_FILE: bare filename → absolute path
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-3: ACCESS_TOKEN_FILE absolute",
    'ACCESS_TOKEN_FILE = "access_token.txt"',
    'ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")  # FIX-3')

# ══════════════════════════════════════════════════════════════════════════
# FIX 4 — Normalise lowercase "cache" → "CACHE" (Linux is case-sensitive)
# Without this: TG files go to a different directory than CACHE_DIR
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-4a: _TG_CACHE_DIR uppercase",
    '_TG_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")\n'
    '_TG_ID_FILE   = os.path.join(_TG_CACHE_DIR, "tg_chat_id.txt")',
    '# FIX-4a — uppercase CACHE; __file__-based (CACHE_DIR not yet defined this early)\n'
    '_TG_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")\n'
    '_TG_ID_FILE   = os.path.join(_TG_CACHE_DIR, "tg_chat_id.txt")')

patch("FIX-4b: _TG_DEDUP_FILE uppercase",
    'os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache", '
    'f"tg_dedup_{datetime.now().strftime(\'%Y%m%d\')}.json")',
    'os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE", '
    'f"tg_dedup_{datetime.now().strftime(\'%Y%m%d\')}.json")  # FIX-4b')

patch("FIX-4c: panchak dedup _dir uppercase",
    '    _dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")',
    '    _dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")  # FIX-4c')

patch("FIX-4d: alert_toggles path",
    '    os.path.dirname(os.path.abspath(__file__)), "CACHE", "alert_toggles.json"\n',
    '    CACHE_DIR, "alert_toggles.json"  # FIX-4d\n')

patch("FIX-4e: TG UI save _cache_dir",
    '                    _cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")',
    '                    _cache_dir = CACHE_DIR  # FIX-4e')

# ══════════════════════════════════════════════════════════════════════════
# FIX 5 — Subprocess cross-platform
# "python3" doesn't exist on Windows; start_new_session not supported
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-5a: python3 → sys.executable",
    '        p = subprocess.Popen(["python3", script],',
    '        # FIX-5a: sys.executable = correct interpreter on Linux AND Windows\n'
    '        p = subprocess.Popen([_sys.executable, script],')

patch("FIX-5b: start_new_session Windows-safe",
    '            stdout=_log_fh, stderr=subprocess.STDOUT, start_new_session=True)',
    '            stdout=_log_fh, stderr=subprocess.STDOUT,\n'
    '            # FIX-5b: start_new_session not available on Windows\n'
    '            **({\"start_new_session\": True} if _sys.platform != \"win32\"\n'
    '               else {\"creationflags\": subprocess.CREATE_NEW_PROCESS_GROUP}))')

# ══════════════════════════════════════════════════════════════════════════
# FIX 6 — os.kill(pid, 0) raises PermissionError on Windows
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-6: os.kill PermissionError on Windows",
    '    if _os.path.exists(pid_f):\n'
    '        try:\n'
    '            with open(pid_f) as _pf: _os.kill(int(_pf.read().strip()), 0)\n'
    '            return  # already running\n'
    '        except (OSError, ValueError): pass',
    '    if _os.path.exists(pid_f):\n'
    '        try:\n'
    '            with open(pid_f, encoding="utf-8") as _pf:\n'
    '                _pid_val = int(_pf.read().strip())\n'
    '            _os.kill(_pid_val, 0)  # FIX-6: PermissionError on Windows\n'
    '            return  # already running\n'
    '        except (OSError, ValueError, PermissionError): pass')

# ══════════════════════════════════════════════════════════════════════════
# FIX 7 — os.replace() PermissionError on Windows when dest file is open
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-7: os.replace → shutil.move fallback",
    '        tmp = dest_path + ".tmp"\n'
    '        try:\n'
    '            df_out.to_csv(tmp, index=False)\n'
    '            os.replace(tmp, dest_path)   # atomic on POSIX — UI never sees partial file\n'
    '        except Exception:\n'
    '            try: os.remove(tmp)\n'
    '            except Exception: pass',
    '        tmp = dest_path + ".tmp"\n'
    '        try:\n'
    '            df_out.to_csv(tmp, index=False, encoding="utf-8")\n'
    '            try:\n'
    '                os.replace(tmp, dest_path)   # atomic on Linux/macOS\n'
    '            except PermissionError:           # FIX-7: Windows fallback\n'
    '                import shutil as _shutil\n'
    '                _shutil.move(tmp, dest_path)\n'
    '        except Exception:\n'
    '            try: os.remove(tmp)\n'
    '            except Exception: pass')

# ══════════════════════════════════════════════════════════════════════════
# FIX 8 — TypeError: Invalid comparison datetime64[ns, UTC+05:30] vs Timestamp
# Kite CSV timestamps can be tz-aware; _cutoff is naive → strip tz first
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-8a: tz strip today-candle dates",
    '    # ── Build today\'s completed candles map: sym → Series(close, index=datetime) ──\n'
    '    today_map = {}\n'
    '    if df_candles is not None and not df_candles.empty:\n'
    '        _t = df_candles.copy()\n'
    '        _t["date"] = pd.to_datetime(_t["date"])\n'
    '        _t = _t[(_t["date"].dt.date == _today_dt) & (_t["date"] < _cutoff)]',
    '    # ── Build today\'s completed candles map: sym → Series(close, index=datetime) ──\n'
    '    today_map = {}\n'
    '    if df_candles is not None and not df_candles.empty:\n'
    '        _t = df_candles.copy()\n'
    '        _t["date"] = pd.to_datetime(_t["date"])\n'
    '        # FIX-8a: Kite CSVs can be tz-aware (UTC+05:30); strip to naive IST\n'
    '        # so comparison with naive _cutoff never raises TypeError\n'
    '        if _t["date"].dt.tz is not None:\n'
    '            _t["date"] = _t["date"].dt.tz_convert(IST).dt.tz_localize(None)\n'
    '        _t = _t[(_t["date"].dt.date == _today_dt) & (_t["date"] < _cutoff)]')

patch("FIX-8b: tz strip historical 5-min CSVs",
    '                _h = pd.read_csv(hist_path)\n'
    '                _h["date"] = pd.to_datetime(_h["date"])\n'
    '                _h = _h[_h["date"].dt.date < _today_dt].sort_values("date")',
    '                _h = pd.read_csv(hist_path)\n'
    '                _h["date"] = pd.to_datetime(_h["date"])\n'
    '                # FIX-8b: same tz strip for historical CSVs\n'
    '                if _h["date"].dt.tz is not None:\n'
    '                    _h["date"] = _h["date"].dt.tz_convert(IST).dt.tz_localize(None)\n'
    '                _h = _h[_h["date"].dt.date < _today_dt].sort_values("date")')

# ══════════════════════════════════════════════════════════════════════════
# FIX 9 — Styler: non-unique index + duplicate columns crash
# KeyError: 'Styler.apply and .map are not compatible with non-unique index'
# ══════════════════════════════════════════════════════════════════════════
patch("FIX-9a: _e7 reset_index",
    '    if _bias_col is None:\n'
    '        st.error("❌ No EMA7 data available. Market data not loaded — check CACHE folder.")\n'
    '    else:\n'
    '        _e7 = df.copy()',
    '    if _bias_col is None:\n'
    '        st.error("❌ No EMA7 data available. Market data not loaded — check CACHE folder.")\n'
    '    else:\n'
    '        _e7 = df.copy()\n'
    '        _e7 = _e7.reset_index(drop=True)  # FIX-9a: Styler needs unique index')

patch("FIX-9b: _E7_SHOW dedup (fallback has EMA7 twice)",
    '        # Display columns\n'
    '        _E7_SHOW = [c for c in [\n'
    '            "Symbol", "LTP",\n'
    '            _bias_col,  "DIST_BIAS_%",   "1H_SLOPE",\n'
    '            _entry_col, "DIST_ENTRY_%",  "ENTRY_STATUS", "ALREADY_IN?",\n'
    '            "CANDLES_1H", "CANDLES_15M",\n'
    '            "CHANGE_%", "VOL_%",\n'
    '            "LIVE_HIGH", "LIVE_LOW", "YEST_HIGH", "YEST_LOW",\n'
    '            "EMA20", "NEAR", "TOP_HIGH", "TOP_LOW"\n'
    '        ] if c in _e7.columns]',
    '        # FIX-9b: dedup — fallback mode has _bias_col==_entry_col=="EMA7"\n'
    '        # which creates duplicate columns → Styler raises KeyError\n'
    '        _E7_SHOW = list(dict.fromkeys(\n'
    '            col for col in [\n'
    '                "Symbol", "LTP",\n'
    '                _bias_col,  "DIST_BIAS_%",   "1H_SLOPE",\n'
    '                _entry_col, "DIST_ENTRY_%",  "ENTRY_STATUS", "ALREADY_IN?",\n'
    '                "CANDLES_1H", "CANDLES_15M",\n'
    '                "CHANGE_%", "VOL_%",\n'
    '                "LIVE_HIGH", "LIVE_LOW", "YEST_HIGH", "YEST_LOW",\n'
    '                "EMA20", "NEAR", "TOP_HIGH", "TOP_LOW"\n'
    '            ] if col in _e7.columns\n'
    '        ))')

patch("FIX-9c: _long_df reset_index",
    '        _long_df = _long_df.sort_values("DIST_ENTRY_%", ascending=True)',
    '        _long_df = _long_df.sort_values("DIST_ENTRY_%", ascending=True).reset_index(drop=True)  # FIX-9c')

patch("FIX-9d: _short_df reset_index",
    '        _short_df = _short_df.sort_values("DIST_ENTRY_%", ascending=False)',
    '        _short_df = _short_df.sort_values("DIST_ENTRY_%", ascending=False).reset_index(drop=True)  # FIX-9d')

# ══════════════════════════════════════════════════════════════════════════
# FIX 10 — ImportError: background_gradient requires matplotlib
# Replace with pure-CSS gradient — no matplotlib needed
# ══════════════════════════════════════════════════════════════════════════
CSS_GRADIENT_HELPER = '''
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
'''

ANCHOR_10 = (
    '        st.dataframe(\n'
    '            _15m_high_df[_show].style.background_gradient(subset=["ABOVE_%"], cmap="Greens"), use_container_width=True\n'
    '        )'
)
if '_css_gradient' not in src and ANCHOR_10 in src:
    src = src.replace(ANCHOR_10, CSS_GRADIENT_HELPER + '\n' + ANCHOR_10, 1)
    applied.append("FIX-10: _css_gradient helper injected")
elif '_css_gradient' in src:
    skipped.append("FIX-10: _css_gradient already present")

for tag, old, new in [
    ("FIX-10a", '_15m_high_df[_show].style.background_gradient(subset=["ABOVE_%"], cmap="Greens")',
                '_15m_high_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["ABOVE_%"], cmap="Greens")'),
    ("FIX-10b", '_15m_low_df[_show].style.background_gradient(subset=["BELOW_%"], cmap="Reds")',
                '_15m_low_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["BELOW_%"], cmap="Reds")'),
    ("FIX-10c", '_vol_df[_show].style.background_gradient(subset=["VOL_SURGE_%"], cmap="Blues")',
                '_vol_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["VOL_SURGE_%"], cmap="Blues")'),
    ("FIX-10d", '_h1_high_df[_show].style.background_gradient(subset=["ABOVE_%"], cmap="Greens")',
                '_h1_high_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["ABOVE_%"], cmap="Greens")'),
    ("FIX-10e", '_h1_low_df[_show].style.background_gradient(subset=["BELOW_%"], cmap="Reds")',
                '_h1_low_df[_show].reset_index(drop=True).style.apply(_css_gradient, subset=["BELOW_%"], cmap="Reds")'),
]:
    patch(tag, old, new)

# ══════════════════════════════════════════════════════════════════════════
# FIX 11 — DeltaGenerator object printed to UI
# Ternary-as-statement: _ac1.markdown(...) if cond else _ac2.markdown(...)
# returns the DeltaGenerator which Streamlit renders as raw object repr
# ══════════════════════════════════════════════════════════════════════════
lines = src.split('\n')
for i, line in enumerate(lines):
    if '_ac1.markdown' in line and '_ai%2==0' in line and '_ac2.markdown' in line:
        lines[i] = (
            '            # FIX-11: ternary-as-statement returns DeltaGenerator → shown in UI\n'
            '            # Rewritten as proper if/else so no value is returned\n'
            '            _amp_html = (f\'<div style="font-size:12px;color:{_acol};padding:2px 0">\'\n'
            '                         f\'{"🟢" if _asc>0 else "🔴"} ({_asc:+d}) {_adsc}</div>\')\n'
            '            if _ai % 2 == 0:\n'
            '                _ac1.markdown(_amp_html, unsafe_allow_html=True)\n'
            '            else:\n'
            '                _ac2.markdown(_amp_html, unsafe_allow_html=True)'
        )
        applied.append("FIX-11: DeltaGenerator ternary → proper if/else")
        break
src = '\n'.join(lines)

# ══════════════════════════════════════════════════════════════════════════
# FIX 13 — OI smart Telegram alerts (interpretation + noise filter + summary)
# Replaces old basic TG block with full intelligence engine.
# Works whether dashboard has: no TG block, old basic TG, or partially patched.
# ══════════════════════════════════════════════════════════════════════════
_SMART_OI_TG = """    # ══════════════════════════════════════════════════════════════════
    # OI INTELLIGENCE: Telegram per-event + smart interpretation + summary
    # Noise filter : dpct < 5% OR dabs < 50,000 contracts -> skip
    # Strong rating: dpct >= 7% AND dabs >= 75,000         -> 3 stars
    # Normal rating: dpct >= 5% AND dabs >= 50,000         -> 2 stars
    # ══════════════════════════════════════════════════════════════════
    _now_str_tg = datetime.now(IST).strftime("%H:%M IST")
    _slot_label = new_events[0]["SLOT"] if new_events else "\\u2014"

    def _interpret_oi_event(ev, spot, atm):
        opt=ev["TYPE"]; dirn=ev["DIRECTION"]; strk=ev["STRIKE"]
        dpct=ev["OI_DELTA_%"]; dabs=abs(ev["OI_DELTA"]); ltp_chg=ev["LTP_CHG_%"]
        top=ev.get("TOP_MOVER",False)
        is_atm=abs(strk-atm)<=50; above_spot=strk>spot; below_spot=strk<spot
        if dpct < 5.0 or dabs < 50000:
            return ("\\U0001f7e1 Filtered \\u2014 "+str(dpct)+"% / "+f"{dabs:,}"+" contracts. Below threshold.", 0, "neutral")
        weight = 3 if (dpct>=7.0 and dabs>=75000) else (2 if (dpct>=5.0 and dabs>=50000) else 1)
        if opt=="CE" and dirn=="DROP":
            bias="bull"
            prem = "falling = shorts squeezed" if ltp_chg<0 else "rising = bulls pushing"
            if is_atm:
                line=("\\U0001f7e2 <b>Bullish</b> \\u2014 ATM ("+str(strk)+") call wall crumbling. Shorts covering + premium "+prem+(".  \\u2b50 Most important." if top else "."))
                weight=3
            elif above_spot:
                line="\\U0001f7e2 Bullish \\u2014 resistance at "+str(strk)+" (above spot) weakening. "+("\\U0001f525 Massive. " if dabs>100000 else "")+"Shorts giving up."
            else:
                line="\\U0001f7e2 Also bullish \\u2014 old resistance at "+str(strk)+" (below spot) already gone."
            return (line, weight, bias)
        if opt=="CE" and dirn=="ADD":
            bias="bear"
            if is_atm:
                line="\\U0001f534 <b>Bearish</b> \\u2014 ATM ("+str(strk)+") call writing = resistance built here. "+("\\U0001f525 Massive OI \\u2014 high conviction." if dabs>100000 else "Watch carefully.")
                weight=3
            elif above_spot:
                line="\\U0001f534 Bearish \\u2014 ceiling at "+str(strk)+" (above spot). "+("\\U0001f525 Big OI. " if dabs>100000 else "")+"Resistance confirmed."
            else:
                line="\\U0001f534 Bearish \\u2014 call writing below spot at "+str(strk)+". Hedging."
            return (line, weight, bias)
        if opt=="PE" and dirn=="ADD":
            bias="bull"
            pnote=("Premium falling = buyers strong." if ltp_chg<-0.5 else "Premium stable = support holding." if abs(ltp_chg)<=0.5 else "\\u26a0\\ufe0f Premium rising \\u2014 possible trap.")
            if is_atm:
                line="\\U0001f7e2 <b>Bullish</b> \\u2014 ATM ("+str(strk)+") put floor = strong support. "+pnote+(" \\u2b50" if top else "")
                weight=3
            elif below_spot:
                line="\\U0001f7e2 Bullish \\u2014 support floor at "+str(strk)+" (below spot). "+pnote
            else:
                line="\\U0001f7e1 Mixed \\u2014 put writing above spot at "+str(strk)+". Possible hedging."
                bias="neutral"
            return (line, weight, bias)
        if opt=="PE" and dirn=="DROP":
            bias="bear"
            if is_atm:
                line="\\U0001f534 <b>Bearish</b> \\u2014 ATM ("+str(strk)+") support collapsing. "+("\\U0001f525 Massive exit \\u2014 high risk. " if dabs>100000 else "")+("\\u2b50 Key level gone." if top else "")
                weight=3
            elif below_spot:
                pfall=" Premium falling = market sliding." if ltp_chg<0 else ""
                line="\\U0001f534 Bearish \\u2014 support at "+str(strk)+" (below spot) collapsing."+pfall
            else:
                line="\\U0001f534 Bearish \\u2014 put longs exiting above spot at "+str(strk)+"."
            return (line, weight, bias)
        return ("\\U0001f7e1 Unclassified.", 0, "neutral")

    _interps = []
    for _ev in new_events:
        _opt=_ev["TYPE"]; _dir=_ev["DIRECTION"]; _strk=_ev["STRIKE"]
        _dpct=_ev["OI_DELTA_%"]; _dabs=abs(_ev["OI_DELTA"])
        _lp=_ev["LTP_PREV"]; _lc=_ev["LTP_CURR"]; _lchg=_ev["LTP_CHG_%"]
        _slot=_ev["SLOT"]; _top=_ev.get("TOP_MOVER",False)
        if _opt=="CE" and _dir=="DROP":
            _icon="\\U0001f4c8"; _head="<b>Call wall unwinding at "+f"{_strk:,}"+" CE</b>"
            _detail="OI dropped <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \\u20b9"+f"{_lp:.1f}"+"\\u2192\\u20b9"+f"{_lc:.1f}"
        elif _opt=="CE" and _dir=="ADD":
            _icon="\\U0001f4dd"; _head="<b>Call writing at "+f"{_strk:,}"+" CE</b>"
            _detail="OI added <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \\u20b9"+f"{_lp:.1f}"+"\\u2192\\u20b9"+f"{_lc:.1f}"
        elif _opt=="PE" and _dir=="ADD":
            _icon="\\U0001f6e1\\ufe0f"; _head="<b>Put floor building at "+f"{_strk:,}"+" PE</b>"
            _detail="OI added <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \\u20b9"+f"{_lp:.1f}"+"\\u2192\\u20b9"+f"{_lc:.1f}"
        else:
            _icon="\\u26a0\\ufe0f"; _head="<b>Put support crumbling at "+f"{_strk:,}"+" PE</b>"
            _detail="OI dropped <b>"+f"{_dabs:,}"+"</b> ("+str(_dpct)+"% in 15 min), premium \\u20b9"+f"{_lp:.1f}"+"\\u2192\\u20b9"+f"{_lc:.1f}"
        _larr="+" if _lchg>=0 else ""
        _badge=" \\u2b50 <i>Top Mover</i>" if _top else ""
        _sig_line,_wt,_bias=_interpret_oi_event(_ev, spot, atm)
        _interps.append((_wt,_bias,_strk,_opt,_dir))
        if _wt==0:
            continue
        _msg=(_icon+" "+_head+_badge+"\\n"+_detail+" ("+_larr+f"{_lchg:.1f}"+"%)\\n"+"\\U0001f4a1 "+_sig_line+"\\n"+"\\u2501"*10+"\\n"+"\\U0001f4ca Spot: <b>"+f"{spot:,.0f}"+"</b>  |  ATM: <b>"+str(atm)+"</b>\\n"+"\\U0001f550 Slot: "+_slot+"  |  "+_now_str_tg+"\\n"+"<i>\\u26a0\\ufe0f NOT financial advice</i>")
        send_telegram_bg(_msg, dedup_key="OI15M_TG_"+_slot+"_"+str(_strk)+"_"+_opt+"_"+_dir)

    try:
        _bull_wt=sum(w for w,b,*_ in _interps if b=="bull"); _bear_wt=sum(w for w,b,*_ in _interps if b=="bear")
        _total=_bull_wt+_bear_wt or 1; _bull_pct=round(_bull_wt/_total*100); _bear_pct=100-_bull_pct
        if _bull_pct>=70:   _ov_icon,_ov_bias,_hint="\\U0001f7e2","BULLISH","Market likely targeting "+str(atm+50)+"\\u2013"+str(atm+100)+"."
        elif _bear_pct>=70: _ov_icon,_ov_bias,_hint="\\U0001f534","BEARISH","Market may test "+str(atm-50)+"\\u2013"+str(atm-100)+"."
        else:               _ov_icon,_ov_bias,_hint="\\u26a0\\ufe0f","MIXED / WAIT","No clear edge \\u2014 wait for confirmation."
        _tbl=[]
        for _ev in new_events:
            _sl,_wt2,_bs=_interpret_oi_event(_ev,spot,atm)
            if _wt2==0: continue
            _stars=("\\U0001f7e2"*3 if _wt2==3 else "\\U0001f7e2"*2 if _wt2==2 else "\\U0001f7e2")
            if _bs=="bear": _stars=_stars.replace("\\U0001f7e2","\\U0001f534")
            elif _bs=="neutral": _stars=_stars.replace("\\U0001f7e2","\\U0001f7e1")
            _elbl=(f"Call unwind {_ev['STRIKE']}" if _ev['TYPE']=="CE" and _ev['DIRECTION']=="DROP" else f"Call writing {_ev['STRIKE']}" if _ev['TYPE']=="CE" and _ev['DIRECTION']=="ADD" else f"Put floor {_ev['STRIKE']}" if _ev['TYPE']=="PE" and _ev['DIRECTION']=="ADD" else f"Put crumble {_ev['STRIKE']}")
            _meaning=_sl.split("\\u2014")[-1].strip()[:55]
            _tbl.append("  \\u2022 "+_elbl+": "+_meaning+("\\u2026" if len(_meaning)>=55 else "")+" "+_stars)
        _tbl_str="\\n".join(_tbl[:8])
        _atm_unwind=any(w==3 and b=="bull" and t=="CE" and d=="DROP" and abs(s-atm)<=50 for w,b,s,t,d in _interps)
        _floor_build=any(w>=2 and b=="bull" and t=="PE" and d=="ADD" for w,b,s,t,d in _interps)
        _pat=""
        if _atm_unwind and _floor_build: _pat="\\U0001f9e0 <b>Pattern:</b> ATM call shorts covering + put floors = <b>short covering + support defense</b>."
        elif _atm_unwind: _pat="\\U0001f9e0 <b>Pattern:</b> ATM resistance breaking \\u2014 bulls in control."
        elif _floor_build and _bull_pct>=60: _pat="\\U0001f9e0 <b>Pattern:</b> Support floors building \\u2014 big money defending lows."
        elif _bear_pct>=70: _pat="\\U0001f9e0 <b>Pattern:</b> Call writing + put exits = distribution. Bears in control."
        if _tbl_str:
            _sum="\\U0001f9e0 <b>OI SUMMARY \\u2014 "+_slot_label+"</b>\\n"+"\\u2501"*21+"\\n"+"\\U0001f4cd Spot: <b>"+f"{spot:,.0f}"+"</b>  |  ATM: <b>"+str(atm)+"</b>\\n"+"\\U0001f4ca Signals: \\U0001f7e2 Bull "+str(_bull_pct)+"%  |  \\U0001f534 Bear "+str(_bear_pct)+"%\\n"+"\\u2501"*21+"\\n"+_tbl_str+"\\n"+"\\u2501"*21+"\\n"+_ov_icon+" <b>Overall: "+_ov_bias+"</b>\\n"+_hint+"\\n"
            if _pat: _sum+=_pat+"\\n"
            _sum+="<i>\\u26a0\\ufe0f NOT financial advice</i>"
            send_telegram_bg(_sum, dedup_key="OI15M_SUMMARY_"+_slot_label.replace("→","_").replace(":",""))
    except Exception as _se:
        print("OI summary TG error: "+str(_se))
"""

# The patcher strategy: find old TG block by line scan and replace
_FIX13_MARKERS = [
    "    # \u2500\u2500 Telegram alerts \u2014 one message per event",  # old basic
    "    # \u2550"*1 + " OI INTELLIGENCE",  # partial smart
]

def _apply_fix13(src_text):
    lines_list = src_text.split("\n")
    start_i = end_i = None
    for i, line in enumerate(lines_list):
        if ("Telegram alerts" in line and "one message per event" in line) or \
           ("OI INTELLIGENCE" in line and "Telegram" in line):
            start_i = i
        if start_i is not None and "send_telegram_bg(_tg_msg, dedup_key=_tg_dedup_key)" in line:
            end_i = i
            break
    if start_i is not None and end_i is not None:
        return "\n".join(lines_list[:start_i]) + "\n" + _SMART_OI_TG + "\n".join(lines_list[end_i+1:]), True
    return src_text, False

if "OI SUMMARY" in src:
    applied.append("FIX-13: OI smart alerts already applied")
else:
    _patched_src, _did_patch = _apply_fix13(src)
    if _did_patch:
        src = _patched_src
        applied.append("FIX-13: OI smart Telegram alerts with interpretation + summary")
    else:
        applied.append("FIX-13: WARNING — could not find TG block anchor in dashboard")


# ══════════════════════════════════════════════════════════════════════════
# Write patched file
# ══════════════════════════════════════════════════════════════════════════
with open(TARGET, "w", encoding="utf-8") as f:
    f.write(src)

# ── Syntax check ──────────────────────────────────────────────────────────
try:
    ast.parse(src)
    syntax_ok = True
except SyntaxError as e:
    syntax_ok = False
    print(f"\n❌  SyntaxError at line {e.lineno}: {e.msg}")
    ctx = src.split('\n')
    for i in range(max(0, e.lineno-3), min(len(ctx), e.lineno+3)):
        print(f"     {i+1}: {ctx[i]}")

# ── Summary ───────────────────────────────────────────────────────────────
print("\n" + "═" * 60)
print(f"  patch_dashboard.py — {'✅ SUCCESS' if syntax_ok else '❌ FAILED'}")
print("═" * 60)
print(f"\n  Applied  ({len(applied)}):")
for x in applied:
    print(f"    ✅  {x}")
if skipped:
    print(f"\n  Skipped  ({len(skipped)}) — already patched or refactored:")
    for x in skipped:
        print(f"    ⏭   {x}")

# ── Residual scan ─────────────────────────────────────────────────────────
print("\n  Residual check:")
residuals = {
    "background_gradient remaining":  sum(1 for l in src.split('\n') if '.background_gradient(' in l and not l.strip().startswith('#')),
    "hardcoded 'CACHE/' strings":     src.count('"CACHE/'),
    "bare python3 in Popen":          src.count('["python3"'),
    "lowercase 'cache' dir paths":    src.count('__file__), "cache"'),
    "start_new_session bare":         src.count("start_new_session=True"),
}
all_clean = all(v == 0 for v in residuals.values())
for k, v in residuals.items():
    print(f"    {'✅' if v == 0 else '⚠ '}  {k}: {v}")

print()
if syntax_ok and all_clean:
    print("  🎉  All fixes applied. Run your dashboard normally:")
    print("       streamlit run panchak_kite_dashboard_fixed28_4.py")
elif syntax_ok:
    print("  ⚠   Some residuals remain — check warnings above.")
else:
    print("  ❌  Syntax error — restoring backup.")
    shutil.copy2(backup, TARGET)
    print(f"      Restored from: {os.path.basename(backup)}")
print()
