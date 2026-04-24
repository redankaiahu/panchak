#!/usr/bin/env python3
# apply_bos_patch.py — BOS Scanner + OHLC DB patcher
# Run once:  python apply_bos_patch.py

import os, sys, ast
from datetime import datetime

DASHBOARD_FILE = "panchak_kite_dashboard_v2.py"
PATCH_MARKER   = "# ── BOS PATCH APPLIED ──"

def check_files():
    ok = True
    for f in [DASHBOARD_FILE, "bos_scanner.py", "ohlc_store.py"]:
        if not os.path.exists(f):
            print(f"ERROR: {f} not found"); ok = False
    return ok

def backup(content):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = f"panchak_kite_dashboard_v2_bos_backup_{ts}.py"
    open(dest,"w",encoding="utf-8").write(content)
    print(f"Backup -> {dest}")

# ── PATCH 1: imports + helpers ─────────────────────────────────────────────────
PATCH1 = r'''
# ── BOS PATCH APPLIED ──
try:
    from ohlc_store import OHLCStore, render_db_status
    from bos_scanner import run_bos_scan, load_scan_cache, render_bos_tab
    _BOS_OK = True
    _ohlc_db = OHLCStore("ohlc_1h.db")
except ImportError as _bimp:
    _BOS_OK = False
    _ohlc_db = None
    print(f"BOS import error: {_bimp}")

def _update_ohlc_db_safe(kite_inst, symbols):
    """Update local OHLC DB — called once per hour or on startup."""
    if not _BOS_OK or not kite_inst or not _ohlc_db: return
    try:
        _ohlc_db.update_all(
            kite         = kite_inst,
            symbols      = symbols,
            get_token_fn = get_token,
            batch_size   = 10,
            delay_secs   = 0.35,
        )
        import streamlit as st
        st.session_state["ohlc_db_updated"] = datetime.now(IST).strftime("%H:%M:%S")
    except Exception as ex:
        print(f"OHLC DB update error: {ex}")

def _run_bos_scan_safe(symbols, ltp_dict=None):
    """Scan for BOS — reads from local DB, no API call."""
    if not _BOS_OK or not _ohlc_db: return []
    import streamlit as st
    tg_on = st.session_state.get("tg_BOS_1H", True)
    try:
        events = run_bos_scan(
            db               = _ohlc_db,
            symbols          = symbols,
            send_telegram_fn = send_telegram_bg,
            ltp_dict         = ltp_dict or {},
            tg_enabled       = tg_on,
        )
        st.session_state["bos_cache"] = load_scan_cache()
        return events
    except Exception as ex:
        print(f"BOS scan error: {ex}"); return []
# ── END BOS IMPORTS ──
'''

# ── PATCH 2: OHLC DB update + BOS scan after OI intel ─────────────────────────
PATCH2 = r'''
# ── OHLC DB + BOS Scanner (injected by apply_bos_patch.py) ──
try:
    # Update OHLC DB once per hour (checks internally if update needed)
    if _BOS_OK and _ohlc_db and _ohlc_db.is_update_needed(max_age_minutes=65):
        import threading
        threading.Thread(target=_update_ohlc_db_safe, args=(kite, STOCKS), daemon=True).start()

    # BOS scan — reads from DB (fast, no API wait)
    _ltp_bos = {}
    _qbos = st.session_state.get("quotes", {})
    if _qbos:
        for _s, _qd in _qbos.items():
            _ltp_bos[_s.replace("NSE:","")] = _qd.get("last_price", 0)
    _run_bos_scan_safe(STOCKS, ltp_dict=_ltp_bos)
except Exception as _bos_err:
    print(f"BOS runner: {_bos_err}")
'''

RUNNER_ANCHOR = "_oi_intel_for_delta = fetch_nifty_oi_intelligence()"

# ── PATCH 3: add tg_BOS_1H to default alert toggles ──────────────────────────
PATCH3_OLD = '"tg_OI_INTEL":          True,'
PATCH3_NEW = '"tg_OI_INTEL":          True,\n    "tg_BOS_1H":            True,   # 1H BOS/CHoCH alerts'

# ── PATCH 4: add BOS_1H to alert categories UI list ──────────────────────────
PATCH4_OLD = '("🌙 Advance Astro Alert", "ASTRO_ADVANCE"),'
PATCH4_NEW = '("🌙 Advance Astro Alert", "ASTRO_ADVANCE"),\n        ("📐 1H BOS/CHoCH",         "BOS_1H"),'

# ── PATCH 5: add tab to tabs list ─────────────────────────────────────────────
PATCH5_OLD = '"⚡ Alerts",    # 18'
PATCH5_NEW = '"⚡ Alerts",    # 18\n    "📐 BOS/CHoCH", # 19'

# ── PATCH 6: render BOS tab at end of file ────────────────────────────────────
PATCH6 = '''
# ── BOS / CHoCH TAB (injected by apply_bos_patch.py) ──
with tabs[19]:
    if _BOS_OK:
        render_bos_tab(st.session_state.get("bos_cache",{}), db=_ohlc_db)
        st.divider()
        from ohlc_store import render_db_status
        st.subheader("📦 OHLC Database")
        st.caption(f"Updated: {st.session_state.get('ohlc_db_updated','never')}")
        render_db_status(_ohlc_db)
        if st.button("🔄 Force DB Update Now"):
            _update_ohlc_db_safe(kite, STOCKS)
            st.rerun()
    else:
        st.error("BOS scanner not loaded. Check bos_scanner.py and ohlc_store.py are in the same folder.")
'''

def apply_patches(content):
    results = []

    # Patch 1: imports
    lines = content.split("\n"); insert_at=0
    for i,line in enumerate(lines[:80]):
        if line.startswith("import ") or line.startswith("from "): insert_at=i+1
    lines.insert(insert_at, PATCH1); content="\n".join(lines)
    results.append("Patch 1: OHLC + BOS imports injected")

    # Patch 2: runner
    if RUNNER_ANCHOR in content:
        content=content.replace(RUNNER_ANCHOR, RUNNER_ANCHOR+"\n"+PATCH2, 1)
        results.append("Patch 2: OHLC update + BOS scan runner injected")
    else:
        results.append("FAILED Patch 2: anchor not found")

    # Patch 3: default toggle
    if PATCH3_OLD in content:
        content=content.replace(PATCH3_OLD, PATCH3_NEW, 1)
        results.append("Patch 3: tg_BOS_1H default toggle added")
    else:
        results.append("FAILED Patch 3: toggle defaults anchor not found")

    # Patch 4: alert categories UI
    if PATCH4_OLD in content:
        content=content.replace(PATCH4_OLD, PATCH4_NEW, 1)
        results.append("Patch 4: BOS_1H added to Alert Controls UI")
    else:
        results.append("FAILED Patch 4: alert categories anchor not found")

    # Patch 5: tabs list
    if PATCH5_OLD in content:
        content=content.replace(PATCH5_OLD, PATCH5_NEW, 1)
        results.append("Patch 5: BOS/CHoCH tab added to tabs list")
    else:
        results.append("FAILED Patch 5: tabs anchor not found")

    # Patch 6: render at end
    content = content.rstrip() + "\n\n" + PATCH6 + "\n"
    results.append("Patch 6: BOS tab render block added")

    return content, results

def main():
    print("\n" + "="*60)
    print("  BOS + OHLC DATABASE PATCHER")
    print("="*60+"\n")
    if not check_files(): sys.exit(1)
    content = open(DASHBOARD_FILE,"r",encoding="utf-8").read()
    if PATCH_MARKER in content:
        print("Already patched. Restore a backup first."); sys.exit(0)
    backup(content)
    print("Applying patches...")
    patched, results = apply_patches(content)
    open(DASHBOARD_FILE,"w",encoding="utf-8").write(patched)
    print("\n"+"-"*60)
    for r in results:
        print(f"  [{'FAILED' if r.startswith('FAILED') else 'OK'}] {r}")
    print("-"*60)
    try:
        ast.parse(patched); print("\n  Syntax check: PASSED")
    except SyntaxError as e:
        print(f"\n  Syntax check: FAILED line {e.lineno} — {e.msg}"); sys.exit(1)
    print("\nDone. Run:  streamlit run panchak_kite_dashboard_v2.py\n")

if __name__=="__main__": main()
