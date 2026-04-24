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
# FIX 12 — encoding="utf-8" on all text file opens
# Windows uses cp1252 by default → corrupts ₹ symbol, emoji, Unicode chars
# ══════════════════════════════════════════════════════════════════════════
enc_fixes = [
    ('with open(_TG_ID_FILE) as _f: _cid = _f.read().strip()',
     'with open(_TG_ID_FILE, encoding="utf-8") as _f: _cid = _f.read().strip()'),
    ('with open(_TG_ID_FILE, "w") as _f: _f.write(_best)',
     'with open(_TG_ID_FILE, "w", encoding="utf-8") as _f: _f.write(_best)'),
    ('        with open(_TG_DEDUP_FILE, "r") as f:',
     '        with open(_TG_DEDUP_FILE, "r", encoding="utf-8") as f:'),
    ('        with open(_TG_DEDUP_FILE, "w") as f:',
     '        with open(_TG_DEDUP_FILE, "w", encoding="utf-8") as f:'),
    ('        with open(pid_f,"w") as _pf2: _pf2.write(str(p.pid))',
     '        with open(pid_f, "w", encoding="utf-8") as _pf2: _pf2.write(str(p.pid))'),
    ('        _log_fh = open(log_f,"a")  # kept open: subprocess owns it until process ends',
     '        _log_fh = open(log_f, "a", encoding="utf-8")  # kept open'),
    ('with open(ACCESS_TOKEN_FILE) as _tf: kite.set_access_token(_tf.read().strip())',
     'with open(ACCESS_TOKEN_FILE, encoding="utf-8") as _tf: kite.set_access_token(_tf.read().strip())'),
    ('    with open(EMAIL_META_FILE, "w") as f:',
     '    with open(EMAIL_META_FILE, "w", encoding="utf-8") as f:'),
    ('        with open(EMAIL_META_FILE) as f:',
     '        with open(EMAIL_META_FILE, encoding="utf-8") as f:'),
]
enc_n = 0
for old, new in enc_fixes:
    if old in src:
        src = src.replace(old, new)
        enc_n += 1
applied.append(f"FIX-12: {enc_n} file opens now have encoding='utf-8'")

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
