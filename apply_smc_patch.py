#!/usr/bin/env python3
# =============================================================
# apply_smc_patch.py
# =============================================================
# Run this ONCE from your dashboard directory:
#
#     python apply_smc_patch.py
#
# It will:
#   1. Backup panchak_kite_dashboard_v2.py → _backup_<timestamp>.py
#   2. Inject SMC runner after line 7333 (fetch_nifty_oi_intelligence)
#   3. Inject SMC render block inside Tab 0 after the OI panel
#   4. Add import at top
#   5. Write patched file
#
# Safe to re-run — checks if already patched first.
# =============================================================

import os
import sys
import shutil
from datetime import datetime

DASHBOARD_FILE = "panchak_kite_dashboard_v2.py"
SMC_ENGINE     = "smc_engine.py"
PATCH_MARKER   = "# ── SMC PATCH APPLIED ──"

# ─────────────────────────────────────────────────────────────
# Sanity checks
# ─────────────────────────────────────────────────────────────
def check_files():
    ok = True
    if not os.path.exists(DASHBOARD_FILE):
        print(f"❌ {DASHBOARD_FILE} not found in current directory.")
        print(f"   Run this script from the same folder as the dashboard.")
        ok = False
    if not os.path.exists(SMC_ENGINE):
        print(f"❌ {SMC_ENGINE} not found. Copy it here first.")
        ok = False
    return ok


def already_patched(content: str) -> bool:
    return PATCH_MARKER in content


# ─────────────────────────────────────────────────────────────
# Backup
# ─────────────────────────────────────────────────────────────
def backup(content: str):
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = f"panchak_kite_dashboard_v2_backup_{ts}.py"
    with open(dest, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ Backup saved → {dest}")
    return dest


# ─────────────────────────────────────────────────────────────
# PATCH 1 — Add import at top (after existing imports block)
# ─────────────────────────────────────────────────────────────
SMC_IMPORT_BLOCK = """
# ── SMC PATCH APPLIED ──
# Smart Money Concept + OI Confluence Engine
try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
    _SMC_ENGINE_OK = True
except ImportError:
    _SMC_ENGINE_OK = False

def _run_smc_intelligence(kite_inst, oi_intel_dict):
    \"\"\"Fetch NIFTY candles and compute SMC + OI confluence.\"\"\"
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
    except Exception as _smc_ex:
        return {"_error": str(_smc_ex)}

def _render_smc_block(smc):
    \"\"\"Render SMC + OI confluence block in Streamlit.\"\"\"
    if not smc or not isinstance(smc, dict):
        return
    if "_error" in smc:
        st.warning(f"SMC Engine error: {smc['_error']}")
        return

    score   = smc.get("final_score", 0)
    signal  = smc.get("final_signal", "⚪ NEUTRAL")
    action  = smc.get("final_action", "WAIT")
    color   = smc.get("signal_color", "grey")
    conflict= smc.get("conflict_detected", False)
    ts      = smc.get("_fetched_at", "")

    bg_map     = {"green":"#1a4d2e","red":"#4d1a1a","yellow":"#3d3a00","grey":"#1a1a2e"}
    border_map = {"green":"#00ff88","red":"#ff4444","yellow":"#ffdd00","grey":"#666688"}
    bg     = bg_map.get(color, "#1a1a2e")
    border = border_map.get(color, "#666688")

    st.markdown(f\"\"\"
<div style="border:2px solid {border};border-radius:10px;background:{bg};padding:12px;margin:8px 0;">
  <div style="font-size:11px;color:#aaa;margin-bottom:4px;">
    🧠 SMC + OI CONFLUENCE &nbsp;·&nbsp; {ts}
  </div>
  <div style="font-size:17px;font-weight:bold;color:{border};margin-bottom:4px;">
    {signal}
  </div>
  <div style="font-size:12px;color:#ddd;">
    Score: <b>{score:+d}</b> &nbsp;|&nbsp; {action}
  </div>
</div>
\"\"\", unsafe_allow_html=True)

    if conflict:
        st.error(
            "⚠️ **OI-SMC CONFLICT** — OI shows BEARISH but price structure is BULLISH.\\n\\n"
            "This is a **Smart Money Gamma Squeeze / Accumulation** pattern.\\n"
            "Call writers are being squeezed as price rises through their strikes.\\n"
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
        if setup.get("entry"):  st.markdown(f"Entry `{setup['entry']:.0f}`")
        if setup.get("sl"):     st.markdown(f"SL `{setup['sl']:.0f}`")
        if setup.get("target1"):st.markdown(f"T1 `{setup['target1']:.0f}`")
        if setup.get("rr"):     st.markdown(f"R:R `{setup['rr']}:1`")
        if setup.get("option_setup"): st.code(setup["option_setup"])

    insight = smc.get("oi_smc_interpretation","")
    if insight:
        st.info(f"🧠 **SMC Insight:** {insight}")

    buy_liq  = smc.get("buy_side_liq",[])
    sell_liq = smc.get("sell_side_liq",[])
    if buy_liq or sell_liq:
        liq_cols = st.columns(2)
        with liq_cols[0]:
            if buy_liq:
                st.markdown(f"💧 Buy Stops: **{', '.join(str(int(x)) for x in buy_liq)}**")
        with liq_cols[1]:
            if sell_liq:
                st.markdown(f"💧 Sell Stops: **{', '.join(str(int(x)) for x in sell_liq)}**")

    reasons = smc.get("reasons",[])
    if reasons:
        with st.expander("📋 Full SMC Analysis", expanded=False):
            for r in reasons:
                st.markdown(f"- {r}")

    tg = smc.get("telegram_summary","")
    if tg:
        with st.expander("📤 Telegram Summary (copy)", expanded=False):
            st.code(tg, language=None)
# ── END SMC PATCH ──
"""

# ─────────────────────────────────────────────────────────────
# PATCH 2 — SMC runner after fetch_nifty_oi_intelligence()
# ─────────────────────────────────────────────────────────────
# We find the line:  _oi_intel_for_delta = fetch_nifty_oi_intelligence()
# and inject right after it.

SMC_RUNNER_INJECTION = """
# ── SMC Intelligence (injected by apply_smc_patch.py) ──
try:
    _smc_result = _run_smc_intelligence(kite, _oi_intel_for_delta)
    st.session_state["smc_result"] = _smc_result
except Exception as _smc_run_err:
    st.session_state["smc_result"] = {"_error": str(_smc_run_err)}
"""

RUNNER_ANCHOR = "_oi_intel_for_delta = fetch_nifty_oi_intelligence()"

# ─────────────────────────────────────────────────────────────
# PATCH 3 — SMC render block inside Tab 0 after OI panel
# ─────────────────────────────────────────────────────────────
# We find: "except Exception as _comb_err:"  followed by the divider
# and inject the render block between except block and st.divider()

SMC_RENDER_INJECTION = """
    # ── SMC + OI Confluence Block (injected by apply_smc_patch.py) ──
    st.subheader("🧠 SMC + OI Confluence Intelligence")
    st.caption(
        "Smart Money Concept analysis of price structure (BOS/CHoCH/OB/FVG) "
        "combined with OI data. Resolves false bearish OI signals during "
        "institutional accumulation / gamma squeeze phases."
    )
    _render_smc_block(st.session_state.get("smc_result", {}))
"""

RENDER_ANCHOR = "    except Exception as _comb_err:\n        st.warning(f\"Combined panel error: {_comb_err}\")\n\n    st.divider()"


# ─────────────────────────────────────────────────────────────
# APPLY PATCHES
# ─────────────────────────────────────────────────────────────

def apply_patches(content: str) -> tuple[str, list[str]]:
    applied = []
    failed  = []

    # ── Patch 1: Import block ──
    # Insert after the last existing `import` or `from` line in the first 50 lines
    lines = content.split("\n")
    insert_at = 0
    for i, line in enumerate(lines[:80]):
        if line.startswith("import ") or line.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, SMC_IMPORT_BLOCK)
    content = "\n".join(lines)
    applied.append("✅ Patch 1: SMC import + helper functions added")

    # ── Patch 2: Runner after fetch_nifty_oi_intelligence() ──
    if RUNNER_ANCHOR in content:
        content = content.replace(
            RUNNER_ANCHOR,
            RUNNER_ANCHOR + "\n" + SMC_RUNNER_INJECTION,
            1   # replace only first occurrence
        )
        applied.append("✅ Patch 2: SMC runner injected after fetch_nifty_oi_intelligence()")
    else:
        failed.append("⚠️  Patch 2: Could not find anchor '_oi_intel_for_delta = fetch_nifty_oi_intelligence()'. Add runner manually.")

    # ── Patch 3: Render block in Tab 0 ──
    if RENDER_ANCHOR in content:
        content = content.replace(
            RENDER_ANCHOR,
            RENDER_ANCHOR + "\n" + SMC_RENDER_INJECTION,
            1
        )
        applied.append("✅ Patch 3: SMC render block injected in Tab 0 after OI panel")
    else:
        # Try a softer anchor
        soft_anchor = "    except Exception as _comb_err:"
        if soft_anchor in content:
            content = content.replace(
                soft_anchor,
                SMC_RENDER_INJECTION + "\n" + soft_anchor,
                1
            )
            applied.append("✅ Patch 3: SMC render block injected (soft anchor)")
        else:
            failed.append(
                "⚠️  Patch 3: Could not find Tab 0 OI panel anchor. "
                "Add '_render_smc_block(st.session_state.get(\"smc_result\", {}))' "
                "manually in Tab 0 after the OI combined panel."
            )

    return content, applied + failed


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  SMC PATCH APPLICATOR for panchak_kite_dashboard_v2.py")
    print("=" * 60)
    print()

    if not check_files():
        sys.exit(1)

    # Read dashboard
    with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    # Check already patched
    if already_patched(content):
        print("ℹ️  Dashboard is already patched (PATCH_MARKER found).")
        print("   To re-patch, remove the existing SMC block first or restore a backup.")
        sys.exit(0)

    # Backup
    backup(content)

    # Apply
    print("Applying patches...")
    patched_content, results = apply_patches(content)

    # Write patched file
    with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
        f.write(patched_content)

    print()
    print("─" * 60)
    for r in results:
        print(f"  {r}")
    print("─" * 60)

    if any("❌" in r or "⚠️" in r for r in results):
        print()
        print("⚠️  Some patches failed — see messages above.")
        print("   The dashboard was still saved. Check manually.")
    else:
        print()
        print("🎉  ALL PATCHES APPLIED SUCCESSFULLY!")

    print()
    print("Next step:  streamlit run panchak_kite_dashboard_v2.py")
    print()


if __name__ == "__main__":
    main()
