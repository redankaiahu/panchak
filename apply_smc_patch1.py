#!/usr/bin/env python3
# =============================================================
# apply_smc_patch.py  — SMC + OI Confluence patcher
# Run once from dashboard folder:  python apply_smc_patch.py
# =============================================================

import os, sys
from datetime import datetime

DASHBOARD_FILE = "panchak_kite_dashboard_v2.py"
SMC_ENGINE     = "smc_engine.py"
PATCH_MARKER   = "# ── SMC PATCH APPLIED ──"

def check_files():
    ok = True
    if not os.path.exists(DASHBOARD_FILE):
        print(f"ERROR: {DASHBOARD_FILE} not found here."); ok = False
    if not os.path.exists(SMC_ENGINE):
        print(f"ERROR: {SMC_ENGINE} not found here."); ok = False
    return ok

def already_patched(content):
    return PATCH_MARKER in content

def backup(content):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = f"panchak_kite_dashboard_v2_backup_{ts}.py"
    open(dest, "w", encoding="utf-8").write(content)
    print(f"Backup saved -> {dest}")
    return dest

# ── PATCH 1: imports + helper functions ───────────────────────────────────────

PATCH1 = r'''
# ── SMC PATCH APPLIED ──
try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
    _SMC_ENGINE_OK = True
except ImportError:
    _SMC_ENGINE_OK = False

def _run_smc_intelligence(kite_inst, oi_intel_dict):
    if not _SMC_ENGINE_OK or not kite_inst or not oi_intel_dict:
        return {}
    try:
        c15 = fetch_nifty_candles_kite(kite_inst, interval="15minute", days=5)
        c1h = fetch_nifty_candles_kite(kite_inst, interval="60minute", days=15)
        if not c15:
            return {}
        r = get_smc_confluence(oi_intel=oi_intel_dict,
                               candles_15m=c15,
                               candles_1h=c1h or None)
        r["_fetched_at"] = datetime.now(IST).strftime("%H:%M:%S")
        return r
    except Exception as ex:
        return {"_error": str(ex)}

def _render_smc_block(smc):
    if not smc or not isinstance(smc, dict):
        return
    if "_error" in smc:
        st.warning(f"SMC Engine error: {smc['_error']}"); return

    score   = smc.get("final_score", 0)
    signal  = smc.get("final_signal", "NEUTRAL")
    action  = smc.get("final_action", "WAIT")
    color   = smc.get("signal_color", "grey")
    conflict= smc.get("conflict_detected", False)
    ts      = smc.get("_fetched_at", "")

    bg_map     = {"green":"#1a4d2e","red":"#4d1a1a","yellow":"#3d3a00","grey":"#1a1a2e"}
    border_map = {"green":"#00ff88","red":"#ff4444","yellow":"#ffdd00","grey":"#666688"}
    bg = bg_map.get(color,"#1a1a2e"); border = border_map.get(color,"#666688")

    st.markdown(f"""
<div style="border:2px solid {border};border-radius:10px;background:{bg};padding:12px;margin:8px 0;">
  <div style="font-size:11px;color:#aaa;margin-bottom:4px;">🧠 SMC + OI CONFLUENCE &nbsp;·&nbsp; {ts}</div>
  <div style="font-size:17px;font-weight:bold;color:{border};margin-bottom:4px;">{signal}</div>
  <div style="font-size:12px;color:#ddd;">Score: <b>{score:+d}</b> &nbsp;|&nbsp; {action}</div>
</div>""", unsafe_allow_html=True)

    if conflict:
        st.error(
            "**OI-SMC CONFLICT** — OI shows BEARISH but price structure is BULLISH.\n\n"
            "This is a **Gamma Squeeze / Smart Money Accumulation** pattern.\n"
            "Call writers are being squeezed as price rises through their strikes.\n"
            "**Price structure overrides raw OI signal.**"
        )

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**📊 OI**")
        st.caption(smc.get("oi_direction","?"))
        st.markdown(f"PCR `{smc.get('oi_pcr',0):.2f}` | MaxPain `{smc.get('oi_max_pain',0)}`")
        cw = smc.get("oi_call_wall")
        pf = smc.get("oi_put_floor")
        if cw: st.markdown(f"🔴 Call Wall `{cw}`")
        if pf: st.markdown(f"🟢 Put Floor `{pf}`")
    with c2:
        st.markdown("**📈 SMC Structure**")
        st.markdown(f"15m: `{smc.get('smc_trend_ltf','?')}`  HTF: `{smc.get('smc_trend_htf','?')}`")
        st.markdown(f"Zone: `{smc.get('pd_zone','?')} ({smc.get('pd_zone_pct',0):.0f}%)`")
        bob = smc.get("nearest_bullish_ob")
        if bob: st.markdown(f"🟦 OB Support `{bob['low']:.0f}`–`{bob['high']:.0f}`")
        bfvg = smc.get("nearest_bullish_fvg")
        if bfvg: st.markdown(f"📊 Bull FVG `{bfvg['bottom']:.0f}`–`{bfvg['top']:.0f}`")
        bos = smc.get("bos_events",[])
        if bos:
            b = bos[-1]
            st.markdown(f"🚀 BOS {'↑' if 'UP' in b['type'] else '↓'} `{b['price']:.0f}`")
    with c3:
        st.markdown("**🎯 Trade Setup**")
        setup = smc.get("setup",{})
        bias  = setup.get("bias","NEUTRAL")
        icon  = {"LONG":"🟢","SHORT":"🔴","NEUTRAL":"⚪"}.get(bias,"⚪")
        st.markdown(f"{icon} `{bias}`")
        if setup.get("entry"):   st.markdown(f"Entry `{setup['entry']:.0f}`")
        if setup.get("sl"):      st.markdown(f"SL    `{setup['sl']:.0f}`")
        if setup.get("target1"): st.markdown(f"T1    `{setup['target1']:.0f}`")
        if setup.get("rr"):      st.markdown(f"R:R   `{setup['rr']}:1`")
        if setup.get("option_setup"): st.code(setup["option_setup"])

    insight = smc.get("oi_smc_interpretation","")
    if insight:
        st.info(f"🧠 **SMC Insight:** {insight}")

    buy_liq = smc.get("buy_side_liq",[])
    sel_liq = smc.get("sell_side_liq",[])
    if buy_liq or sel_liq:
        lc1, lc2 = st.columns(2)
        with lc1:
            if buy_liq: st.markdown(f"💧 Buy Stops: **{', '.join(str(int(x)) for x in buy_liq)}**")
        with lc2:
            if sel_liq: st.markdown(f"💧 Sell Stops: **{', '.join(str(int(x)) for x in sel_liq)}**")

    reasons = smc.get("reasons",[])
    if reasons:
        with st.expander("📋 Full SMC Analysis", expanded=False):
            for r in reasons: st.markdown(f"- {r}")

    tg = smc.get("telegram_summary","")
    if tg:
        with st.expander("📤 Telegram Summary (copy)", expanded=False):
            st.code(tg, language=None)
# ── END SMC PATCH ──
'''

# ── PATCH 2: SMC runner + Telegram alert after fetch_nifty_oi_intelligence() ─

PATCH2 = r'''
# ── SMC Intelligence + Telegram (injected by apply_smc_patch.py) ──
try:
    _smc_result = _run_smc_intelligence(kite, _oi_intel_for_delta)
    st.session_state["smc_result"] = _smc_result

    if _smc_result and "_error" not in _smc_result and st.session_state.get("tg_OI_INTEL", True):
        _ss       = _smc_result
        _sc       = _ss.get("final_score", 0)
        _sig      = _ss.get("final_signal", "")
        _act      = _ss.get("final_action", "")
        _tr15     = _ss.get("smc_trend_ltf", "")
        _trhtf    = _ss.get("smc_trend_htf", "")
        _conf     = _ss.get("conflict_detected", False)
        _ins      = _ss.get("oi_smc_interpretation", "")
        _pdz      = _ss.get("pd_zone", "")
        _pdp      = _ss.get("pd_zone_pct", 0)
        _sspot    = _ss.get("spot", 0)
        _satm     = _ss.get("atm", 0)
        _soidir   = _ss.get("oi_direction", "")
        _spcr     = _ss.get("oi_pcr", 0)
        _scw      = _ss.get("oi_call_wall", 0)
        _spf      = _ss.get("oi_put_floor", 0)
        _ssetup   = _ss.get("setup", {})
        _sbos     = _ss.get("bos_events", [])
        _schoch   = _ss.get("choch_events", [])
        _sbob     = _ss.get("nearest_bullish_ob")
        _sbfvg    = _ss.get("nearest_bullish_fvg")
        _sbliq    = _ss.get("buy_side_liq", [])
        _ssliq    = _ss.get("sell_side_liq", [])

        _sico = "🟢" if _sc >= 2 else ("🔴" if _sc <= -2 else "🟡")
        _cline = (
            "\n⚠️ <b>OI-SMC CONFLICT</b> — OI bearish but price BULLISH\n"
            "🧠 Gamma Squeeze / Smart Money Accumulation detected\n"
            "Price structure OVERRIDES raw OI signal\n"
        ) if _conf else ""

        _bline = ""
        if _sbos:
            _b = _sbos[-1]
            _bline = f"🚀 BOS {'↑' if 'UP' in _b['type'] else '↓'} at <b>{_b['price']:.0f}</b>\n"
        if _schoch:
            _c = _schoch[-1]
            _bline += f"🔄 CHoCH {'↑' if 'UP' in _c['type'] else '↓'} at <b>{_c['price']:.0f}</b>\n"

        _obline  = f"🟦 Bullish OB: <b>{_sbob['low']:.0f}–{_sbob['high']:.0f}</b>\n" if _sbob else ""
        _fvgline = f"📊 Bull FVG: <b>{_sbfvg['bottom']:.0f}–{_sbfvg['top']:.0f}</b> ({_sbfvg['gap_pct']}%)\n" if _sbfvg else ""
        _liqline = ""
        if _sbliq: _liqline += f"💧 Buy Stops: <b>{', '.join(str(int(x)) for x in _sbliq[:2])}</b>\n"
        if _ssliq: _liqline += f"💧 Sell Stops: <b>{', '.join(str(int(x)) for x in _ssliq[:2])}</b>\n"

        _opsetup = _ssetup.get("option_setup","")
        _slv = _ssetup.get("sl") or 0
        _t1v = _ssetup.get("target1") or 0
        _rrv = _ssetup.get("rr") or 0

        _smc_tg = (
            f"{_sico} <b>SMC + OI CONFLUENCE ALERT</b>\n"
            f"⏰ {datetime.now(IST).strftime('%H:%M:%S IST')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 Spot: <b>{_sspot:,.0f}</b>  |  ATM: <b>{_satm}</b>\n"
            f"{_sico} <b>{_sig}</b>  (Score: {_sc:+d})\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Raw OI: {_soidir}\n"
            f"📊 PCR: <b>{_spcr}</b>  |  Call Wall: <b>{_scw}</b>  |  Put Floor: <b>{_spf}</b>\n"
            f"📈 SMC 15m: <b>{_tr15}</b>  |  HTF: <b>{_trhtf}</b>\n"
            f"🎯 P/D Zone: <b>{_pdz}</b> ({_pdp:.0f}%)\n"
            f"{_cline}"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_bline}{_obline}{_fvgline}{_liqline}"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 {_ins}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Action: <b>{_act}</b>\n"
            f"⚡ {_opsetup}\n"
            f"🛡 SL: <b>{_slv:.0f}</b>  T1: <b>{_t1v:.0f}</b>  R:R <b>{_rrv}:1</b>\n"
            f"\n⚠️ <i>NOT financial advice.</i>"
        )
        _now_s   = datetime.now(IST)
        _skey    = _now_s.strftime(f"%H{(_now_s.minute // 10)*10:02d}")
        send_telegram_bg(_smc_tg, dedup_key=f"SMC_INTEL_{_skey}")

except Exception as _smc_run_err:
    st.session_state["smc_result"] = {"_error": str(_smc_run_err)}
'''

RUNNER_ANCHOR = "_oi_intel_for_delta = fetch_nifty_oi_intelligence()"

# ── PATCH 3: render block in Tab 0 ────────────────────────────────────────────

PATCH3 = '''
    # ── SMC + OI Confluence Block (injected by apply_smc_patch.py) ──
    st.subheader("🧠 SMC + OI Confluence Intelligence")
    st.caption(
        "Smart Money Concept analysis (BOS/CHoCH/OB/FVG) combined with OI data. "
        "Resolves false bearish OI signals during institutional accumulation / gamma squeeze."
    )
    _render_smc_block(st.session_state.get("smc_result", {}))
'''

RENDER_ANCHOR = '    except Exception as _comb_err:\n        st.warning(f"Combined panel error: {_comb_err}")\n\n    st.divider()'

# ──────────────────────────────────────────────────────────────────────────────

def apply_patches(content):
    results = []

    # Patch 1 — import block after last top-level import
    lines = content.split("\n")
    insert_at = 0
    for i, line in enumerate(lines[:80]):
        if line.startswith("import ") or line.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, PATCH1)
    content = "\n".join(lines)
    results.append("Patch 1: SMC imports + helper functions injected")

    # Patch 2 — runner + telegram after fetch call
    if RUNNER_ANCHOR in content:
        content = content.replace(RUNNER_ANCHOR, RUNNER_ANCHOR + "\n" + PATCH2, 1)
        results.append("Patch 2: SMC runner + Telegram alert injected")
    else:
        results.append("FAILED Patch 2: anchor not found — add runner manually")

    # Patch 3 — render block in Tab 0
    if RENDER_ANCHOR in content:
        content = content.replace(RENDER_ANCHOR, RENDER_ANCHOR + "\n" + PATCH3, 1)
        results.append("Patch 3: SMC render block injected in Tab 0")
    else:
        soft = '    except Exception as _comb_err:'
        if soft in content:
            content = content.replace(soft, PATCH3 + "\n" + soft, 1)
            results.append("Patch 3: SMC render block injected (soft anchor)")
        else:
            results.append("FAILED Patch 3: Tab 0 anchor not found — add render manually")

    return content, results


def main():
    print("\n" + "="*60)
    print("  SMC PATCH APPLICATOR")
    print("="*60 + "\n")

    if not check_files():
        sys.exit(1)

    content = open(DASHBOARD_FILE, "r", encoding="utf-8").read()

    if already_patched(content):
        print("Already patched. Restore a backup first to re-patch.")
        sys.exit(0)

    backup(content)
    print("Applying patches...")
    patched, results = apply_patches(content)

    open(DASHBOARD_FILE, "w", encoding="utf-8").write(patched)

    print("\n" + "-"*60)
    for r in results:
        icon = "FAILED" if r.startswith("FAILED") else "OK"
        print(f"  [{icon}] {r}")
    print("-"*60)

    # Syntax check
    import ast
    try:
        ast.parse(patched)
        print("\n  Syntax check: PASSED")
    except SyntaxError as e:
        print(f"\n  Syntax check: FAILED at line {e.lineno} — {e.msg}")
        print("  Restore the backup and report this error.")
        sys.exit(1)

    print("\nDone. Run:  streamlit run panchak_kite_dashboard_v2.py\n")


if __name__ == "__main__":
    main()
