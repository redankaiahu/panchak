# ==========================================================
# KP PANCHANG TAB — kp_panchang_tab.py
# ---------------------------------------------------------
# Features:
#   1. Today's KP windows (09:00–15:00 IST, 15-min slots)
#   2. Live LTP monitoring → auto Telegram alerts on break
#   3. Manual alert buttons per window
#   4. Next-5-day upcoming windows
#   5. Breakout memory (last 5 days) — high/low/false/pts
#   6. Full CSV browser
#
# Telegram alerts (same bot & channel as main dashboard):
#   • Auto window-open alert at slot start
#   • Auto Top-High break alert (buy signal)
#   • Auto Least-Low break alert (sell signal)
#   • Auto False-breakout alert (reversal detected)
#   • Auto end-of-window summary
#   • Manual buttons for each alert type
# ==========================================================

import os, json, threading, urllib.request
from datetime import datetime, timedelta, date, time as dtime
import pandas as pd
import streamlit as st

# ── paths ─────────────────────────────────────────────────
_HERE     = os.path.dirname(os.path.abspath(__file__))
_CSV_PATH = os.path.join(_HERE, "kp_panchang_2026.csv")
_MEM_PATH = os.path.join(_HERE, "CACHE", "kp_breakout_memory.json")
_TG_DEDUP = os.path.join(_HERE, "CACHE", "kp_tg_dedup.json")

# ── planet metadata ───────────────────────────────────────
PLANET_FULL = {
    "Su": "Sun", "Mo": "Moon", "Ma": "Mars", "Me": "Mercury",
    "Ju": "Jupiter", "Ve": "Venus", "Sa": "Saturn",
    "Ra": "Rahu", "Ke": "Ketu",
}
PLANET_COLOR = {
    "Ju": "#00c853", "Ve": "#00bcd4", "Mo": "#64b5f6",
    "Me": "#aed581", "Su": "#ffb300",
    "Sa": "#ef5350", "Ra": "#e040fb",
    "Ke": "#ff7043", "Ma": "#f44336",
}
KP_BENEFIC = {"Ju", "Ve", "Mo", "Me"}
KP_MALEFIC = {"Sa", "Ra", "Ke", "Ma"}


# ═══════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════

def _get_tg_cfg():
    """Pull bot token + chat-id from the main dashboard's globals."""
    try:
        import __main__ as _m
        tok = getattr(_m, "TG_BOT_TOKEN", "") or os.environ.get("TG_BOT_TOKEN", "")
        cid = getattr(_m, "TG_CHAT_ID",   "") or os.environ.get("TG_CHAT_ID",   "")
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
    os.makedirs(os.path.join(_HERE, "CACHE"), exist_ok=True)
    with open(_TG_DEDUP, "w") as f:
        json.dump(d, f)


def _send_tg(message: str, dedup_key: str = None) -> bool:
    """Send Telegram message in background thread. One-shot dedup by key."""
    tok, cid = _get_tg_cfg()
    if not tok or not cid:
        return False

    if dedup_key:
        dd = _tg_dedup_load()
        if dd.get(dedup_key):
            return False          # already sent

    def _do():
        try:
            url     = f"https://api.telegram.org/bot{tok}/sendMessage"
            payload = json.dumps({
                "chat_id": cid, "text": message, "parse_mode": "HTML"
            }).encode()
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                ok = json.loads(resp.read()).get("ok", False)
            if ok and dedup_key:
                dd = _tg_dedup_load()
                dd[dedup_key] = datetime.now().isoformat()
                _tg_dedup_save(dd)
        except Exception as e:
            print(f"[KP-TG] {e}")

    threading.Thread(target=_do, daemon=True).start()
    return True


# ── KP signal from P3+P4 sub-sub lords ───────────────────
def _sig(p1, p2, p3, p4):
    b = sum(1 for p in [p3, p4] if p in KP_BENEFIC)
    m = sum(1 for p in [p3, p4] if p in KP_MALEFIC)
    return "BUY 🟢" if b > m else ("SELL 🔴" if m > b else "NEUTRAL ⚪")


# ── Alert builders ────────────────────────────────────────
def _tg_open(slot, p1, p2, p3, p4, nifty_ltp=0, bnf_ltp=0):
    pn = " · ".join(PLANET_FULL.get(p, p) for p in [p1,p2,p3,p4])
    msg = (f"🌙 <b>KP WINDOW OPEN</b>\n"
           f"⏰ Slot   : <b>{slot}</b>\n"
           f"🪐 Planets: {p1} {p2} {p3} {p4}\n"
           f"   ({pn})\n"
           f"📡 Signal : <b>{_sig(p1,p2,p3,p4)}</b>\n"
           f"📊 NIFTY <b>{nifty_ltp:,.0f}</b>  |  BANKNIFTY <b>{bnf_ltp:,.0f}</b>\n"
           f"⚡ Record 15-min High & Low now!")
    _send_tg(msg, dedup_key=f"KP_OPEN_{slot}_{p1}{p2}{p3}{p4}")


def _tg_high(sym, slot, top_high, ltp, pts, p1,p2,p3,p4):
    msg = (f"🟢 <b>KP TOP-HIGH BREAK — {sym}</b>\n"
           f"⏰ Slot    : <b>{slot}</b>\n"
           f"🪐 {p1} {p2} {p3} {p4}  |  {_sig(p1,p2,p3,p4)}\n"
           f"📈 Top-High: <b>{top_high:,.2f}</b>\n"
           f"💹 LTP     : <b>{ltp:,.2f}</b>\n"
           f"💰 Points  : <b>+{pts:.0f}</b>\n"
           f"✅ <b>BUY BREAKOUT CONFIRMED</b>")
    _send_tg(msg, dedup_key=f"KP_HIGH_{sym}_{slot}")


def _tg_low(sym, slot, least_low, ltp, pts, p1,p2,p3,p4):
    msg = (f"🔴 <b>KP LEAST-LOW BREAK — {sym}</b>\n"
           f"⏰ Slot     : <b>{slot}</b>\n"
           f"🪐 {p1} {p2} {p3} {p4}  |  {_sig(p1,p2,p3,p4)}\n"
           f"📉 Least-Low: <b>{least_low:,.2f}</b>\n"
           f"💹 LTP      : <b>{ltp:,.2f}</b>\n"
           f"💰 Points   : <b>+{pts:.0f}</b>\n"
           f"✅ <b>SELL BREAKOUT CONFIRMED</b>")
    _send_tg(msg, dedup_key=f"KP_LOW_{sym}_{slot}")


def _tg_false(sym, slot, direction, ltp, p1,p2,p3,p4):
    msg = (f"⚠️ <b>KP FALSE BREAKOUT — {sym}</b>\n"
           f"⏰ Slot     : <b>{slot}</b>\n"
           f"🪐 {p1} {p2} {p3} {p4}\n"
           f"Direction  : broke <b>{direction}</b> then reversed\n"
           f"💹 LTP     : <b>{ltp:,.2f}</b>\n"
           f"⚠️ <b>REVERSAL — exit / stay cautious</b>")
    _send_tg(msg, dedup_key=f"KP_FALSE_{sym}_{direction}_{slot}")


def _tg_summary(slot, nh, nl, bh, bl, n_broke, b_broke, p1,p2,p3,p4):
    msg = (f"📊 <b>KP WINDOW SUMMARY</b>\n"
           f"⏰ Slot : <b>{slot}</b>\n"
           f"🪐 {p1} {p2} {p3} {p4}  |  {_sig(p1,p2,p3,p4)}\n\n"
           f"<b>NIFTY</b>     ▲{nh:,.0f}  ▼{nl:,.0f}  → {n_broke}\n"
           f"<b>BANKNIFTY</b> ▲{bh:,.0f}  ▼{bl:,.0f}  → {b_broke}")
    _send_tg(msg, dedup_key=f"KP_SUM_{slot}")


# ── Public aliases (used by external code if needed) ─────
alert_window_open   = _tg_open
alert_high_break    = _tg_high
alert_low_break     = _tg_low
alert_false_breakout = _tg_false
alert_window_summary = _tg_summary


# ═══════════════════════════════════════════════════════════
# CSV LOADER
# ═══════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def _load_kp_csv():
    if not os.path.exists(_CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(_CSV_PATH)
    df["DateObj"] = pd.to_datetime(df["Date"], format="%d/%b/%Y")
    df["Hour"]    = df["Time"].str.split(":").str[0].astype(int)
    df["Minute"]  = df["Time"].str.split(":").str[1].astype(int)
    return df


# ═══════════════════════════════════════════════════════════
# BREAKOUT MEMORY
# ═══════════════════════════════════════════════════════════
def _load_mem():
    os.makedirs(os.path.join(_HERE, "CACHE"), exist_ok=True)
    try:
        with open(_MEM_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_mem(mem):
    os.makedirs(os.path.join(_HERE, "CACHE"), exist_ok=True)
    with open(_MEM_PATH, "w") as f:
        json.dump(mem, f, indent=2)


def _prune_mem(mem, keep=5):
    for old in sorted(mem.keys())[:-keep]:
        del mem[old]
    return mem


# ═══════════════════════════════════════════════════════════
# MISC HELPERS
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
    c = PLANET_COLOR.get(p, "#9e9e9e")
    return (f'<span style="background:{c};color:#000;font-weight:700;'
            f'padding:2px 8px;border-radius:4px;margin:1px 2px;'
            f'font-size:13px;display:inline-block">{p}</span>')


def _prow(p1, p2, p3, p4):
    return "".join(_badge(p) for p in [p1, p2, p3, p4])


def _card(bg, bc, content):
    return (f'<div style="background:{bg};border:1px solid {bc};'
            f'border-radius:8px;padding:10px 14px;margin:5px 0">'
            f'{content}</div>')


# ═══════════════════════════════════════════════════════════
# MAIN RENDER
# ═══════════════════════════════════════════════════════════
def render_kp_tab():
    st.header("🌙 KP Panchang — Trading Windows")
    st.caption(
        "Source: KP Panchang 2026 (Kanak Bosmia · KPAstro 4.5)  |  "
        "09:00–15:00 IST  |  15-min windows  |  "
        "✈️ Telegram → <b>Panchak Alerts</b> channel",
        unsafe_allow_html=True,
    )

    df = _load_kp_csv()
    if df.empty:
        st.error(f"❌ CSV not found: {_CSV_PATH}")
        return

    now     = _now_ist()
    today   = now.date()
    tod_str = today.strftime("%d/%b/%Y")
    now_min = now.hour * 60 + now.minute

    today_df = df[df["Date"] == tod_str].copy()

    # ── quick-status banner ───────────────────────────────
    active_row = None
    next_row   = None
    for _, row in today_df.iterrows():
        sm = _slot_min(row["Slot_Start"])
        em = _slot_min(row["Slot_End"])
        if sm <= now_min < em:
            active_row = row
        elif now_min < sm and next_row is None:
            next_row = row

    if active_row is not None:
        r = active_row
        mins_left = _slot_min(r["Slot_End"]) - now_min
        st.markdown(
            _card("#1b5e20","#00e676",
                f'<span style="font-size:17px;font-weight:700;color:#00e676">🟢 ACTIVE KP WINDOW</span>'
                f' &nbsp; <b style="color:#fff">⏰ {r["Slot_Start"]}–{r["Slot_End"]}</b>'
                f' &nbsp; <span style="color:#ffb300">{_sig(r["P1"],r["P2"],r["P3"],r["P4"])}</span>'
                f' &nbsp; <span style="color:#aaa;font-size:12px">{mins_left} min left</span>'
                f'<br>{_prow(r["P1"],r["P2"],r["P3"],r["P4"])}'
                f'<span style="color:#9e9e9e;font-size:11px"> '
                f'{"  ·  ".join(PLANET_FULL.get(p,p) for p in [r["P1"],r["P2"],r["P3"],r["P4"]])}'
                f'</span>'
            ),
            unsafe_allow_html=True,
        )
    elif next_row is not None:
        r = next_row
        mins_away = _slot_min(r["Slot_Start"]) - now_min
        st.markdown(
            _card("#0d1b2a","#5c6bc0",
                f'<span style="font-size:15px;font-weight:700;color:#7986cb">'
                f'⏳ Next window in <b style="color:#ffb300">{mins_away} min</b>'
                f'</span> &nbsp; <b style="color:#fff">⏰ {r["Slot_Start"]}–{r["Slot_End"]}</b>'
                f'<br>{_prow(r["P1"],r["P2"],r["P3"],r["P4"])}'
            ),
            unsafe_allow_html=True,
        )

    # ── Telegram config status ────────────────────────────
    tok, cid = _get_tg_cfg()
    tg_ok = bool(tok and cid)
    st.markdown(
        f'<span style="font-size:12px;color:{"#00e676" if tg_ok else "#ef5350"}">'
        f'{"✅ Telegram connected" if tg_ok else "❌ Telegram not configured — set TG_BOT_TOKEN / TG_CHAT_ID"}'
        f'</span>',
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 1 — LIVE MONITOR + ALERTS
    # ══════════════════════════════════════════════════════
    st.subheader("📡 Live Monitor & Telegram Alerts")
    st.caption("Enter 15-min High & Low → select 'Broke?' → alerts fire automatically. "
               "Manual buttons also available.")

    mon_key = f"kp_mon_{tod_str}"
    if mon_key not in st.session_state:
        st.session_state[mon_key] = {}

    if today_df.empty:
        st.info("No KP windows today.")
    else:
        for _, row in today_df.iterrows():
            slot  = row["Slot_15min"]
            sm    = _slot_min(row["Slot_Start"])
            em    = _slot_min(row["Slot_End"])
            is_act  = sm <= now_min < em
            is_past = now_min >= em

            bc = "#00e676" if is_act else ("#444" if is_past else "#5c6bc0")
            bg = "#1b5e20" if is_act else ("#111" if is_past else "#0d1b2a")

            pnames = "  ·  ".join(PLANET_FULL.get(p,p) for p in [row["P1"],row["P2"],row["P3"],row["P4"]])

            # header card
            st.markdown(
                _card(bg, bc,
                    f'{_prow(row["P1"],row["P2"],row["P3"],row["P4"])}'
                    f'&nbsp;<b style="color:#fff">⏰ {row["Slot_Start"]}–{row["Slot_End"]}</b>'
                    f'&nbsp;<span style="color:#ffb300;font-size:12px">'
                    f'{_sig(row["P1"],row["P2"],row["P3"],row["P4"])}</span>'
                    f'<br><span style="color:#9e9e9e;font-size:11px">{pnames}</span>'
                ),
                unsafe_allow_html=True,
            )

            # controls — active window OR just ended (within 30 min)
            show_ctrl = is_act or (is_past and now_min - em <= 30)
            if show_ctrl:
                ss = st.session_state[mon_key].setdefault(slot, {
                    "nh": 0.0, "nl": 0.0, "bh": 0.0, "bl": 0.0,
                    "nb": "None", "bb": "None",
                    "open_sent":   False,
                    "nh_sent":     False, "nl_sent":  False,
                    "bh_sent":     False, "bl_sent":  False,
                    "nf_sent":     False, "bf_sent":  False,
                    "sum_sent":    False,
                })

                broke_opts = ["None","High","Low","Both","False-High","False-Low"]
                cn, cb = st.columns(2)

                with cn:
                    st.markdown("**NIFTY**")
                    c1, c2 = st.columns(2)
                    ss["nh"] = c1.number_input("▲ High", value=float(ss["nh"]), step=1.0, key=f"nh_{slot}")
                    ss["nl"] = c2.number_input("▼ Low",  value=float(ss["nl"]), step=1.0, key=f"nl_{slot}")
                    ss["nb"] = st.selectbox("Broke?", broke_opts,
                                             index=broke_opts.index(ss["nb"]), key=f"nb_{slot}")

                with cb:
                    st.markdown("**BANKNIFTY**")
                    c3, c4 = st.columns(2)
                    ss["bh"] = c3.number_input("▲ High", value=float(ss["bh"]), step=5.0, key=f"bh_{slot}")
                    ss["bl"] = c4.number_input("▼ Low",  value=float(ss["bl"]), step=5.0, key=f"bl_{slot}")
                    ss["bb"] = st.selectbox("Broke?", broke_opts,
                                             index=broke_opts.index(ss["bb"]), key=f"bb_{slot}")

                # Manual buttons
                b1, b2, b3, b4, b5 = st.columns(5)

                if b1.button("🔔 Open", key=f"b_op_{slot}", help="Send window-open alert"):
                    _tg_open(slot, row["P1"],row["P2"],row["P3"],row["P4"],
                             ss["nh"] or 0, ss["bh"] or 0)
                    st.toast("📤 Window-open alert sent!")

                if b2.button("🟢 N High", key=f"b_nh_{slot}", help="NIFTY high-break alert"):
                    pts = abs(ss["nh"] - ss["nl"])
                    _tg_high("NIFTY", slot, ss["nh"], ss["nh"], pts,
                              row["P1"],row["P2"],row["P3"],row["P4"])
                    st.toast("📤 NIFTY high-break sent!")

                if b3.button("🔴 N Low", key=f"b_nl_{slot}", help="NIFTY low-break alert"):
                    pts = abs(ss["nh"] - ss["nl"])
                    _tg_low("NIFTY", slot, ss["nl"], ss["nl"], pts,
                             row["P1"],row["P2"],row["P3"],row["P4"])
                    st.toast("📤 NIFTY low-break sent!")

                if b4.button("📊 BNF Hi/Lo", key=f"b_bk_{slot}", help="BankNifty alerts"):
                    pts = abs(ss["bh"] - ss["bl"])
                    if ss["bb"] in ("High","Both"):
                        _tg_high("BANKNIFTY", slot, ss["bh"], ss["bh"], pts,
                                  row["P1"],row["P2"],row["P3"],row["P4"])
                    elif ss["bb"] in ("Low",):
                        _tg_low("BANKNIFTY", slot, ss["bl"], ss["bl"], pts,
                                 row["P1"],row["P2"],row["P3"],row["P4"])
                    st.toast("📤 BankNifty alert sent!")

                if b5.button("📋 Summary", key=f"b_sum_{slot}", help="Send window summary"):
                    _tg_summary(slot, ss["nh"],ss["nl"],ss["bh"],ss["bl"],
                                ss["nb"],ss["bb"],row["P1"],row["P2"],row["P3"],row["P4"])
                    st.toast("📤 Summary sent!")

                # ── AUTO alerts on each page refresh ──
                p1,p2,p3,p4 = row["P1"],row["P2"],row["P3"],row["P4"]

                # Window open (once)
                if is_act and not ss["open_sent"]:
                    _tg_open(slot, p1,p2,p3,p4, ss["nh"] or 0, ss["bh"] or 0)
                    ss["open_sent"] = True

                # NIFTY high break
                if ss["nh"] > 0 and ss["nb"] in ("High","Both") and not ss["nh_sent"]:
                    _tg_high("NIFTY", slot, ss["nh"], ss["nh"],
                              abs(ss["nh"]-ss["nl"]), p1,p2,p3,p4)
                    ss["nh_sent"] = True

                # NIFTY low break
                if ss["nl"] > 0 and ss["nb"] in ("Low","Both") and not ss["nl_sent"]:
                    _tg_low("NIFTY", slot, ss["nl"], ss["nl"],
                             abs(ss["nh"]-ss["nl"]), p1,p2,p3,p4)
                    ss["nl_sent"] = True

                # BANKNIFTY high break
                if ss["bh"] > 0 and ss["bb"] in ("High","Both") and not ss["bh_sent"]:
                    _tg_high("BANKNIFTY", slot, ss["bh"], ss["bh"],
                              abs(ss["bh"]-ss["bl"]), p1,p2,p3,p4)
                    ss["bh_sent"] = True

                # BANKNIFTY low break
                if ss["bl"] > 0 and ss["bb"] in ("Low","Both") and not ss["bl_sent"]:
                    _tg_low("BANKNIFTY", slot, ss["bl"], ss["bl"],
                             abs(ss["bh"]-ss["bl"]), p1,p2,p3,p4)
                    ss["bl_sent"] = True

                # NIFTY false breakout
                if ss["nb"] in ("False-High","False-Low") and not ss["nf_sent"]:
                    d = ss["nb"].replace("False-","")
                    ltp = ss["nh"] if d=="High" else ss["nl"]
                    _tg_false("NIFTY", slot, d, ltp, p1,p2,p3,p4)
                    ss["nf_sent"] = True

                # BANKNIFTY false breakout
                if ss["bb"] in ("False-High","False-Low") and not ss["bf_sent"]:
                    d = ss["bb"].replace("False-","")
                    ltp = ss["bh"] if d=="High" else ss["bl"]
                    _tg_false("BANKNIFTY", slot, d, ltp, p1,p2,p3,p4)
                    ss["bf_sent"] = True

                # End-of-window summary (once, if any values set)
                if is_past and not ss["sum_sent"] and (ss["nh"] > 0 or ss["bh"] > 0):
                    _tg_summary(slot, ss["nh"],ss["nl"],ss["bh"],ss["bl"],
                                ss["nb"],ss["bb"],p1,p2,p3,p4)
                    ss["sum_sent"] = True

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 2 — TODAY READ-ONLY LIST
    # ══════════════════════════════════════════════════════
    st.subheader(f"📅 Today — {now.strftime('%A, %d %b %Y')}")

    if today_df.empty:
        st.info("No KP windows today.")
    else:
        for _, row in today_df.iterrows():
            sm = _slot_min(row["Slot_Start"])
            em = _slot_min(row["Slot_End"])
            if sm <= now_min < em:
                status,bg,bc = "🟢 ACTIVE", "#1b5e20", "#00e676"
            elif now_min >= em:
                status,bg,bc = "✅ Done",   "#111",    "#444"
            else:
                status,bg,bc = "⏳ Soon",   "#0d1b2a", "#5c6bc0"

            pnames = " → ".join(PLANET_FULL.get(p,p) for p in [row["P1"],row["P2"],row["P3"],row["P4"]])
            st.markdown(
                _card(bg, bc,
                    f'{_prow(row["P1"],row["P2"],row["P3"],row["P4"])}'
                    f'&nbsp;<b style="color:#fff">⏰ {row["Slot_Start"]}–{row["Slot_End"]}</b>'
                    f'&nbsp;<span style="color:#ffb300;font-size:12px">'
                    f'{_sig(row["P1"],row["P2"],row["P3"],row["P4"])}</span>'
                    f'&nbsp;<b style="color:{bc}">{status}</b>'
                    f'<br><span style="color:#9e9e9e;font-size:11px">{pnames}'
                    f'&nbsp;|&nbsp;Raw: {row["Time"]}</span>'
                ),
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 3 — NEXT 5 TRADING DAYS
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

    if not upcoming:
        st.info("No upcoming windows found.")
    else:
        for d_str, day_name, chunk in upcoming:
            with st.expander(f"📅 {day_name}  {d_str}  ({len(chunk)} windows)", expanded=False):
                for _, row in chunk.iterrows():
                    pnames = " · ".join(PLANET_FULL.get(p,p) for p in [row["P1"],row["P2"],row["P3"],row["P4"]])
                    st.markdown(
                        _card("#0d1b2a","#37474f",
                            f'<b style="color:#80cbc4">⏰ {row["Slot_Start"]}–{row["Slot_End"]}</b>'
                            f'&nbsp;{_prow(row["P1"],row["P2"],row["P3"],row["P4"])}'
                            f'&nbsp;<span style="color:#ffb300;font-size:12px">'
                            f'{_sig(row["P1"],row["P2"],row["P3"],row["P4"])}</span>'
                            f'<br><span style="color:#9e9e9e;font-size:11px">{pnames}</span>'
                        ),
                        unsafe_allow_html=True,
                    )

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 4 — BREAKOUT MEMORY
    # ══════════════════════════════════════════════════════
    st.subheader("💾 Breakout Memory — Last 5 Days")
    st.caption("Manual record of outcomes. Tracks high/low levels, which broke, points, false breakouts.")

    mem = _load_mem()

    with st.expander("➕ Add / Update Record", expanded=True):
        c1, c2, c3 = st.columns(3)
        rec_date  = c1.date_input("Date", value=today, key="kp_rd")
        rec_sym   = c1.selectbox("Symbol", ["NIFTY","BANKNIFTY","SENSEX"], key="kp_rs")
        rec_slot  = c2.text_input("Slot (HH:MM-HH:MM)", placeholder="10:15-10:30", key="kp_rsl")
        rec_high  = c2.number_input("▲ Top High",  value=0.0, step=0.5, key="kp_rh")
        rec_low   = c3.number_input("▼ Least Low", value=0.0, step=0.5, key="kp_rl")
        broke_opts2 = ["None","High","Low","Both","False-High","False-Low"]
        rec_broke = c3.selectbox("Broke?", broke_opts2, key="kp_rb")
        pts_h = st.number_input("Points from High break (0 if not broken)", value=0.0, step=0.5, key="kp_ph")
        pts_l = st.number_input("Points from Low break  (0 if not broken)", value=0.0, step=0.5, key="kp_pl")

        if st.button("💾 Save Record", key="kp_save"):
            dk  = rec_date.strftime("%Y-%m-%d")
            kid = f"{rec_sym}_{rec_slot}"
            is_false = 1 if "False" in rec_broke else 0
            mem.setdefault(dk, {})[kid] = {
                "symbol": rec_sym, "slot": rec_slot,
                "top_high": rec_high, "least_low": rec_low,
                "broke": rec_broke,
                "pts_high": pts_h, "pts_low": pts_l,
                "false_breakout": is_false,
                "saved_at": now.strftime("%H:%M:%S"),
            }
            # Auto-send false breakout telegram
            if is_false:
                direction = rec_broke.replace("False-","")
                ltp = rec_high if direction=="High" else rec_low
                match = df[(df["Date"] == rec_date.strftime("%d/%b/%Y")) &
                           (df["Slot_15min"].str.strip() == rec_slot.strip())]
                if not match.empty:
                    r2 = match.iloc[0]
                    _tg_false(rec_sym, rec_slot, direction, ltp,
                               r2["P1"],r2["P2"],r2["P3"],r2["P4"])
            mem = _prune_mem(mem)
            _save_mem(mem)
            st.success(f"✅ Saved {rec_sym} {rec_slot} for {dk}")
            st.rerun()

    if not mem:
        st.info("No records yet. Add above.")
    else:
        for dk in sorted(mem.keys(), reverse=True):
            if not mem[dk]:
                continue
            d_obj = datetime.strptime(dk, "%Y-%m-%d")
            st.markdown(f"### 📅 {d_obj.strftime('%A, %d %b %Y')}")

            for kid, rec in mem[dk].items():
                broke    = rec.get("broke","None")
                is_false = rec.get("false_breakout", 0)
                pts_h    = rec.get("pts_high", 0)
                pts_l    = rec.get("pts_low",  0)

                if "False" in broke:
                    bc2, icon = "#ff9800", f"⚠️ FALSE {broke.replace('False-','')} BREAKOUT"
                elif broke == "Both":
                    bc2, icon = "#ffd740", "🟡 BOTH BROKE"
                elif broke == "High":
                    bc2, icon = "#00e676", "🟢 HIGH BREAK"
                elif broke == "Low":
                    bc2, icon = "#ff5252", "🔴 LOW BREAK"
                else:
                    bc2, icon = "#9e9e9e", "⬜ No Break"

                pts_txt = ""
                if pts_h: pts_txt += f"  📈 +{pts_h:.0f} pts (high)"
                if pts_l: pts_txt += f"  📉 +{pts_l:.0f} pts (low)"

                st.markdown(
                    f'<div style="background:#1a1a2e;border:1px solid #2d2d5e;'
                    f'border-left:4px solid {bc2};border-radius:6px;'
                    f'padding:10px 14px;margin:5px 0">'
                    f'<b style="color:#e0e0e0">{rec["symbol"]}</b>'
                    f'&nbsp;<span style="color:#80cbc4">⏰ {rec["slot"]}</span>'
                    f'<br>'
                    f'▲ <b style="color:#00e676">{rec["top_high"]}</b>'
                    f'&nbsp;&nbsp;▼ <b style="color:#ff5252">{rec["least_low"]}</b>'
                    f'&nbsp;&nbsp;<b style="color:{bc2}">{icon}</b>'
                    f'{"&nbsp;&nbsp;" + pts_txt if pts_txt else ""}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        # Stats
        st.markdown("---")
        st.subheader("📊 Summary Stats (Last 5 Days)")
        total = false_c = high_b = low_b = 0
        total_pts = 0.0
        for dk, entries in mem.items():
            for _, rec in entries.items():
                total += 1
                broke = rec.get("broke","None")
                if rec.get("false_breakout"): false_c += 1
                if "High" in broke and "False" not in broke:
                    high_b    += 1
                    total_pts += rec.get("pts_high", 0)
                if "Low" in broke and "False" not in broke:
                    low_b     += 1
                    total_pts += rec.get("pts_low", 0)

        cols = st.columns(5)
        cols[0].metric("Windows Recorded", total)
        cols[1].metric("🟢 High Breaks",    high_b)
        cols[2].metric("🔴 Low Breaks",     low_b)
        cols[3].metric("⚠️ False Breakouts", false_c)
        cols[4].metric("💰 Total Points",   f"{total_pts:.0f}")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # SECTION 5 — CSV BROWSER
    # ══════════════════════════════════════════════════════
    with st.expander("🔍 Browse Full KP Panchang 2026", expanded=False):
        months    = sorted(df["DateObj"].dt.strftime("%b %Y").unique())
        sel_month = st.selectbox("Month", months, key="kp_mb")
        filt = df[df["DateObj"].dt.strftime("%b %Y") == sel_month][
            ["Date","Day","Planet_String","Time","Slot_15min"]
        ].copy()
        filt.columns = ["Date","Day","Planets (P1·P2·P3·P4)","Time","15-min Slot"]
        st.dataframe(filt, use_container_width=True, hide_index=True)
        st.caption(f"{len(filt)} windows in {sel_month}  |  Total 2026: {len(df)}")
