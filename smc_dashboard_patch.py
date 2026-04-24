# =============================================================
# smc_dashboard_patch.py
# =============================================================
# DROP-IN PATCH for panchak_kite_dashboard_v2.py
#
# HOW TO USE:
#   1. Copy smc_engine.py to your dashboard directory
#   2. In panchak_kite_dashboard_v2.py, add this import near the top:
#        from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
#   3. Call get_smc_dashboard_block() wherever you render OI intelligence
#      in the dashboard (search for "OI_INTEL" or "oi_intelligence" tab)
#   4. The SMC block replaces/augments the existing "BEARISH/BULLISH" raw signal
#      with a conflict-aware, price-structure-informed signal.
#
# FULL PATCH INSTRUCTIONS:
#   Search for: "fetch_nifty_oi_intelligence" call in your background worker
#   After it runs, add:
#
#       _smc_result = run_smc_intelligence(kite, _oi_intel_for_delta)
#       st.session_state["smc_result"] = _smc_result
#
#   Then in your Streamlit OI tab render section, call:
#       render_smc_block(st.session_state.get("smc_result"))
# =============================================================

from __future__ import annotations
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional

try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
    _SMC_AVAILABLE = True
except ImportError:
    _SMC_AVAILABLE = False


# ─────────────────────────────────────────────────────────────
# A. MAIN RUNNER — call in background worker
# ─────────────────────────────────────────────────────────────

def run_smc_intelligence(kite_instance, oi_intel: dict) -> dict:
    """
    Fetch candles and run SMC + OI confluence.
    Call this AFTER fetch_nifty_oi_intelligence() completes.

    Returns the SMC confluence dict (or empty dict on error).
    """
    if not _SMC_AVAILABLE or not kite_instance or not oi_intel:
        return {}

    try:
        # Fetch 15-min candles (last 5 days = ~100 candles)
        candles_15m = fetch_nifty_candles_kite(kite_instance, interval="15minute", days=5)

        # Fetch 1-hour candles (last 15 days)
        candles_1h  = fetch_nifty_candles_kite(kite_instance, interval="60minute", days=15)

        if not candles_15m:
            return {}

        result = get_smc_confluence(
            oi_intel    = oi_intel,
            candles_15m = candles_15m,
            candles_1h  = candles_1h if candles_1h else None,
        )
        result["_fetched_at"] = datetime.now().strftime("%H:%M:%S")
        return result

    except Exception as e:
        return {"_error": str(e)}


# ─────────────────────────────────────────────────────────────
# B. STREAMLIT RENDER BLOCK
# ─────────────────────────────────────────────────────────────

def render_smc_block(smc: dict, compact: bool = False):
    """
    Render the SMC + OI confluence block in the Streamlit dashboard.

    Call this inside your OI intelligence tab / section.
    Replaces the misleading "raw OI = BEARISH" display with a context-aware signal.

    Parameters:
        smc     : Output dict from run_smc_intelligence()
        compact : If True, show a compact single-line version (for sidebar)
    """
    if not smc or "_error" in smc:
        if smc.get("_error"):
            st.warning(f"SMC Engine: {smc['_error']}")
        return

    score  = smc.get("final_score", 0)
    signal = smc.get("final_signal", "⚪ NEUTRAL")
    action = smc.get("final_action", "WAIT")
    color  = smc.get("signal_color", "grey")
    conflict = smc.get("conflict_detected", False)
    ts     = smc.get("_fetched_at", "")

    # Color map
    bg_map = {
        "green":  "#1a4d2e",
        "red":    "#4d1a1a",
        "yellow": "#3d3a00",
        "grey":   "#1a1a2e",
    }
    border_map = {
        "green":  "#00ff88",
        "red":    "#ff4444",
        "yellow": "#ffdd00",
        "grey":   "#666688",
    }
    bg     = bg_map.get(color, "#1a1a2e")
    border = border_map.get(color, "#666688")

    if compact:
        # One-liner for sidebar
        icon = {"green": "🟢", "red": "🔴", "yellow": "🟡"}.get(color, "⚪")
        st.markdown(f"{icon} **SMC:** {signal} `Score:{score:+d}`")
        return

    # ── Full block ──
    st.markdown(f"""
    <div style="border:2px solid {border}; border-radius:10px;
                background:{bg}; padding:12px; margin:8px 0;">
      <div style="font-size:13px; color:#aaa; margin-bottom:4px;">
        🧠 SMC + OI CONFLUENCE INTELLIGENCE &nbsp;·&nbsp; {ts}
      </div>
      <div style="font-size:18px; font-weight:bold; color:{border}; margin-bottom:6px;">
        {signal}
      </div>
      <div style="font-size:13px; color:#ddd;">
        Score: <b>{score:+d}</b> &nbsp;|&nbsp; Action: <b>{action}</b>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Conflict alert ──
    if conflict:
        st.error(
            "⚠️ **OI-SMC CONFLICT DETECTED**\n\n"
            "OI signals BEARISH but **price structure is BULLISH** (BOS UP / HH/HL intact). "
            "This is a classic **Smart Money accumulation** pattern where:\n"
            "- Call writers are being SQUEEZED as price rises through their strikes\n"
            "- OI shows 'call building' but it's actually **gamma hedge buying** by MMs\n"
            "- Trust the **price structure FIRST**, OI is lagging\n\n"
            f"**SMC overrides OI signal.**"
        )

    # ── Three columns: OI | SMC | Setup ──
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("**📊 OI Data**")
        st.markdown(f"Direction: `{smc.get('oi_direction','?')}`")
        st.markdown(f"PCR: `{smc.get('oi_pcr', 0):.2f}`")
        cw = smc.get('oi_call_wall')
        pf = smc.get('oi_put_floor')
        if cw: st.markdown(f"Call Wall: `{cw}`")
        if pf: st.markdown(f"Put Floor: `{pf}`")
        mp = smc.get('oi_max_pain')
        if mp: st.markdown(f"Max Pain: `{mp}`")

    with c2:
        st.markdown("**📈 SMC Structure**")
        st.markdown(f"15m Trend: `{smc.get('smc_trend_ltf','?')}`")
        st.markdown(f"HTF Trend: `{smc.get('smc_trend_htf','?')}`")
        st.markdown(f"P/D Zone: `{smc.get('pd_zone','?')} ({smc.get('pd_zone_pct',0):.0f}%)`")
        if smc.get("nearest_bullish_ob"):
            ob = smc["nearest_bullish_ob"]
            st.markdown(f"🟦 OB Support: `{ob['low']:.0f}–{ob['high']:.0f}`")
        if smc.get("nearest_bearish_ob"):
            ob = smc["nearest_bearish_ob"]
            st.markdown(f"🟥 OB Resist: `{ob['low']:.0f}–{ob['high']:.0f}`")

    with c3:
        st.markdown("**🎯 Trade Setup**")
        setup = smc.get("setup", {})
        bias  = setup.get("bias", "NEUTRAL")
        bias_icon = {"LONG": "🟢", "SHORT": "🔴", "NEUTRAL": "⚪"}.get(bias, "⚪")
        st.markdown(f"Bias: {bias_icon} `{bias}`")
        if setup.get("entry"):
            st.markdown(f"Entry: `{setup['entry']:.0f}`")
        if setup.get("sl"):
            st.markdown(f"SL: `{setup['sl']:.0f}`")
        if setup.get("target1"):
            st.markdown(f"T1: `{setup['target1']:.0f}`")
        if setup.get("rr"):
            st.markdown(f"R:R: `{setup['rr']}:1`")
        if setup.get("option_setup"):
            st.markdown(f"⚡ `{setup['option_setup']}`")

    # ── BOS / CHoCH events ──
    bos    = smc.get("bos_events", [])
    choch  = smc.get("choch_events", [])
    if bos or choch:
        st.markdown("**🏗 Market Structure Events**")
        ev_cols = st.columns(2)
        with ev_cols[0]:
            if bos:
                b = bos[-1]
                icon = "🚀" if "UP" in b["type"] else "💥"
                st.markdown(f"{icon} **BOS {'↑' if 'UP' in b['type'] else '↓'}** at `{b['price']:.0f}`")
        with ev_cols[1]:
            if choch:
                c = choch[-1]
                icon = "🔄"
                st.markdown(f"{icon} **CHoCH {'↑' if 'UP' in c['type'] else '↓'}** at `{c['price']:.0f}`")

    # ── FVG levels ──
    bull_fvg = smc.get("nearest_bullish_fvg")
    bear_fvg = smc.get("nearest_bearish_fvg")
    if bull_fvg or bear_fvg:
        with st.expander("📊 Fair Value Gaps (FVG)", expanded=False):
            if bull_fvg:
                st.markdown(f"🟦 **Bullish FVG** (support magnet): `{bull_fvg['bottom']:.0f}` – `{bull_fvg['top']:.0f}` ({bull_fvg['gap_pct']}%)")
            if bear_fvg:
                st.markdown(f"🟥 **Bearish FVG** (resistance magnet): `{bear_fvg['bottom']:.0f}` – `{bear_fvg['top']:.0f}` ({bear_fvg['gap_pct']}%)")

    # ── Liquidity pools ──
    buy_liq  = smc.get("buy_side_liq", [])
    sell_liq = smc.get("sell_side_liq", [])
    if buy_liq or sell_liq:
        with st.expander("💧 Liquidity Pools (Stop Hunt Levels)", expanded=False):
            if buy_liq:
                st.markdown(f"📈 **Buy Stops (target above):** {', '.join(str(int(x)) for x in buy_liq)}")
            if sell_liq:
                st.markdown(f"📉 **Sell Stops (target below):** {', '.join(str(int(x)) for x in sell_liq)}")

    # ── SMC Insight (the key explanation) ──
    insight = smc.get("oi_smc_interpretation", "")
    if insight:
        st.info(f"🧠 **SMC Insight:** {insight}")

    # ── All reasons ──
    reasons = smc.get("reasons", [])
    if reasons:
        with st.expander("📋 Full Analysis Reasoning", expanded=False):
            for r in reasons:
                st.markdown(f"- {r}")

    # ── Telegram copy button ──
    tg = smc.get("telegram_summary", "")
    if tg:
        with st.expander("📤 Telegram-Ready Summary", expanded=False):
            st.code(tg, language=None)


# ─────────────────────────────────────────────────────────────
# C. ENHANCED OI INTELLIGENCE TELEGRAM MESSAGE
# ─────────────────────────────────────────────────────────────

def build_enhanced_oi_tg_message(oi_intel: dict, smc: dict) -> str:
    """
    Build enhanced Telegram OI Intelligence message that includes SMC context.

    This REPLACES the raw OI telegram in the dashboard's
    _send_oi_intel_tg() function call.

    The key enhancement: when OI says BEARISH but SMC says BULLISH,
    the message now explains the conflict instead of blindly saying "SELL".
    """
    spot     = oi_intel.get("spot", 0)
    atm      = oi_intel.get("atm", 0)
    pcr      = oi_intel.get("pcr", 0)
    max_pain = oi_intel.get("max_pain", 0)
    oi_dir   = oi_intel.get("direction", "")
    ts       = oi_intel.get("timestamp", "")
    cw       = oi_intel.get("nearest_call_wall", 0)
    pf       = oi_intel.get("nearest_put_floor", 0)

    smc_signal  = smc.get("final_signal", "")
    smc_score   = smc.get("final_score", 0)
    smc_trend   = smc.get("smc_trend_ltf", "")
    conflict    = smc.get("conflict_detected", False)
    smc_action  = smc.get("final_action", "")
    setup       = smc.get("setup", {})
    insight     = smc.get("oi_smc_interpretation", "")

    lines = [
        f"🧠 NIFTY OI + SMC INTELLIGENCE",
        f"⏰ {ts}",
        f"━━━━━━━━━━━━━━━━━━━━━",
        f"📍 Spot: {spot:,.1f} | ATM: {atm}",
        f"📊 PCR: {pcr} | Max Pain: {max_pain}",
        f"━━━━━━━━━━━━━━━━━━━━━",
        f"📋 RAW OI: {oi_dir}",
        f"📋 OI Reason: {oi_intel.get('direction_reason','')}",
    ]

    if cw:
        lines.append(f"🔴 Call Wall (Resist): {cw}")
    if pf:
        lines.append(f"🟢 Put Floor (Support): {pf}")

    lines.append(f"━━━━━━━━━━━━━━━━━━━━━")

    # SMC section
    if smc_signal:
        lines.append(f"🧠 SMC + OI CONFLUENCE: {smc_signal}")
        lines.append(f"Score: {smc_score:+d} | SMC Trend: {smc_trend}")

        if conflict:
            lines.append("")
            lines.append("⚠️ OI-SMC CONFLICT:")
            lines.append("OI says BEARISH but price structure says BULLISH.")
            lines.append("This = Smart Money GAMMA SQUEEZE / Accumulation.")
            lines.append("DO NOT blindly follow OI bearish signal.")

        if insight:
            lines.append(f"💡 {insight}")

        lines.append(f"━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"🎯 ACTION: {smc_action}")

        if setup.get("option_setup"):
            lines.append(f"⚡ {setup['option_setup']}")
        if setup.get("sl"):
            lines.append(f"🛡 SL: {setup['sl']:.0f} | T1: {setup.get('target1',0):.0f} | RR: {setup.get('rr',0)}:1")
    else:
        # Fallback to original OI advice if SMC not available
        lines.append(f"💡 {oi_intel.get('advice','')}")
        lines.append(f"⚡ {oi_intel.get('setup','')}")

    lines.append(f"⚠️ NOT financial advice.")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# D. INLINE INTEGRATION SNIPPET
# ─────────────────────────────────────────────────────────────
#
# Copy-paste this block into your dashboard background worker section
# (search for "7333" or "_oi_intel_for_delta = fetch_nifty_oi_intelligence()"):
#
# ─── PASTE START ───────────────────────────────────────────────
#
# from smc_dashboard_patch import run_smc_intelligence
#
# # After line: _oi_intel_for_delta = fetch_nifty_oi_intelligence()
# if _oi_intel_for_delta:
#     try:
#         _smc_result = run_smc_intelligence(kite, _oi_intel_for_delta)
#         st.session_state["smc_result"] = _smc_result
#         st.session_state["smc_updated_at"] = datetime.now(IST).strftime("%H:%M:%S")
#     except Exception as _smc_err:
#         st.session_state["smc_result"] = {"_error": str(_smc_err)}
#
# ─── PASTE END ─────────────────────────────────────────────────
#
# In your OI tab render function, add:
#
# from smc_dashboard_patch import render_smc_block
# render_smc_block(st.session_state.get("smc_result", {}))
#
# ──────────────────────────────────────────────────────────────
