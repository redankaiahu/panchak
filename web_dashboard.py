"""
web_dashboard.py — Panchak Web Dashboard (FastAPI)
====================================================
Runs on AWS Linux (or any headless server). Open in any browser:
    http://your-aws-ip:8000

Install once:
    pip install fastapi uvicorn pytz kiteconnect pandas

Run:
    python3 web_dashboard.py

Run in background (keeps running after SSH disconnect):
    screen -S dashboard
    python3 web_dashboard.py
    Ctrl+A then D  (detach)

To reattach:
    screen -r dashboard

Or use nohup:
    nohup python3 web_dashboard.py > dashboard.log 2>&1 &
"""

import os, sys, json, threading, time
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── FastAPI ───────────────────────────────────────────────────────────────────
try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
except ImportError:
    print("Install: pip install fastapi uvicorn")
    sys.exit(1)

# ── Kite ──────────────────────────────────────────────────────────────────────
try:
    from kiteconnect import KiteConnect
    import pandas as pd
    KITE_OK = True
except ImportError:
    KITE_OK = False

# ── Your existing modules ─────────────────────────────────────────────────────
try:
    from ohlc_store import OHLCStore, is_market_hours
    DB_OK = True
except ImportError:
    DB_OK = False

try:
    from bos_scanner import detect_bos, build_bos_setup, load_scan_cache, BOS_LOOKBACK
    BOS_OK = True
except ImportError:
    BOS_OK = False

try:
    from astro_logic import get_astro_score, get_week_forecast
    ASTRO_OK = True
except ImportError:
    ASTRO_OK = False

try:
    from astro_time import get_time_signal_detail, is_good_entry_time
    TIME_OK = True
except ImportError:
    TIME_OK = False

try:
    from smc_engine import get_smc_confluence, fetch_nifty_candles_kite
    SMC_OK = True
except ImportError:
    SMC_OK = False

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY           = "7am67kxijfsusk9i"
_WD_BASE  = os.path.dirname(os.path.abspath(__file__))
_WD_CACHE = os.path.join(_WD_BASE, "CACHE")
os.makedirs(_WD_CACHE, exist_ok=True)

def _wcp(f): return os.path.join(_WD_CACHE, f)

# Web dashboard specific files (_qt suffix kept for compatibility)
OI_CACHE_FILE = _wcp("oi_intel_cache_qt.json")   # shared OI snapshot with Qt dashboard

# Access token
ACCESS_TOKEN_FILE = (
    _wcp("access_token.txt")
    if os.path.exists(_wcp("access_token.txt"))
    else os.path.join(_WD_BASE, "access_token.txt")
)
AUTO_REFRESH_SECS = 60      # ← refresh every 60 seconds during market hours
PORT              = 8000

PANCHAK_START     = date(2026, 4, 13)
PANCHAK_END       = date(2026, 4, 17)
PANCHAK_DATA_FILE = _wcp("panchak_data.csv")

STOCKS = [
    "360ONE","ABB","ABCAPITAL","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS",
    "ALKEM","AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","APLAPOLLO","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV",
    "BAJFINANCE","BAJAJHLDNG","BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
    "BHEL","BHARATFORG","BHARTIARTL","BIOCON","BLUESTARCO","BOSCHLTD","BPCL",
    "BRITANNIA","BSE","CAMS","CANBK","CDSL","CGPOWER","CHOLAFIN","CIPLA",
    "COALINDIA","COFORGE","COLPAL","CONCOR","CROMPTON","CUMMINSIND","DABUR",
    "DALBHARAT","DELHIVERY","DIXON","DIVISLAB","DLF","DMART","DRREDDY",
    "EICHERMOT","ETERNAL","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK",
    "GODREJCP","GODREJPROP","GRASIM","HAL","HAVELLS","HCLTECH","HDFCAMC",
    "HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDCOPPER","HINDPETRO",
    "HINDUNILVR","HINDZINC","HUDCO","ICICIBANK","ICICIGI","ICICIPRULI","IEX",
    "INDHOTEL","INDIANB","INDIGO","INDUSINDBK","INDUSTOWER","INFY","INOXWIND",
    "IRCTC","IRFC","IREDA","ITC","JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL",
    "JUBLFOOD","KALYANKJIL","KAYNES","KEI","KFINTECH","KPITTECH","KOTAKBANK",
    "LAURUSLABS","LICHSGFIN","LICI","LODHA","LTF","LT","LTIM","LUPIN","M&M",
    "MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MAZDOCK","MCX","MFSL","MPHASIS",
    "MOTHERSON","MUTHOOTFIN","NAUKRI","NBCC","NESTLEIND","NTPC","NUVAMA","NYKAA",
    "NATIONALUM","OBEROIRLTY","OFSS","OIL","ONGC","PAGEIND","PATANJALI","PAYTM",
    "PERSISTENT","PETRONET","PFC","PGEL","PHOENIXLTD","PIDILITIND","PIIND","PNB",
    "PNBHOUSING","POLICYBZR","POLYCAB","POWERGRID","POWERINDIA","PRESTIGE",
    "RBLBANK","RECLTD","RELIANCE","RVNL","SAIL","SBICARD","SBILIFE","SBIN",
    "SHREECEM","SHRIRAMFIN","SIEMENS","SOLARINDS","SRF","SUNPHARMA","SUPREMEIND",
    "SWIGGY","SYNGENE","TATACONSUM","TATAELXSI","TATAPOWER","TATATECH","TATASTEEL",
    "TCS","TECHM","TIINDIA","TITAN","TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR",
    "ULTRACEMCO","UNIONBANK","UNITDSPR","UPL","VBL","VEDL","VOLTAS","WIPRO","ZYDUSLIFE",
]

# ── Global state (in-memory cache, refreshed by background thread) ─────────────
_state = {
    "kite":         None,
    "kite_ok":      False,
    "kite_error":   "",
    "oi":           {},
    "smc":          {},
    "bos":          [],
    "astro":        {},
    "time_signal":  {},
    "screener":     {},
    "movers":       {},
    "kp":           {},
    "alerts_log":   [],
    "last_refresh": "",
    "refreshing":   False,
}
_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# KITE SESSION
# ─────────────────────────────────────────────────────────────────────────────

def init_kite():
    if not KITE_OK:
        _state["kite_error"] = "kiteconnect not installed"; return False
    try:
        k = KiteConnect(api_key=API_KEY)
        with open(ACCESS_TOKEN_FILE, encoding="utf-8") as f:
            k.set_access_token(f.read().strip())
        k.quote(["NSE:NIFTY 50"])   # validate
        _state["kite"] = k
        _state["kite_ok"] = True
        _state["kite_error"] = ""
        print(f"[{_ts()}] ✅ Kite connected")
        return True
    except FileNotFoundError:
        _state["kite_error"] = f"access_token.txt not found at {ACCESS_TOKEN_FILE}"
    except Exception as e:
        _state["kite_error"] = str(e)
    _state["kite_ok"] = False
    print(f"[{_ts()}] ❌ Kite error: {_state['kite_error']}")
    return False


def _ts():
    return datetime.now().strftime("%H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_oi():
    kite = _state.get("kite")
    if not kite: return {}
    try:
        spot = kite.quote(["NSE:NIFTY 50"])["NSE:NIFTY 50"]["last_price"]
        nfo  = pd.DataFrame(kite.instruments("NFO"))

        # Futures
        fut_ltp = spot
        try:
            nf = nfo[(nfo["name"]=="NIFTY") & (nfo["instrument_type"]=="FUT")].copy()
            nf["expiry_dt"] = pd.to_datetime(nf["expiry"])
            nf = nf[nf["expiry_dt"] >= pd.Timestamp.now()].sort_values("expiry_dt")
            if not nf.empty:
                fts = nf.iloc[0]["tradingsymbol"]
                fq  = kite.quote([f"NFO:{fts}"])
                fut_ltp = fq[f"NFO:{fts}"]["last_price"]
        except Exception: pass

        step = 50; atm = int(round(spot / step) * step)
        strikes = [atm + i * step for i in range(-10, 11)]

        opts = nfo[(nfo["name"]=="NIFTY") & (nfo["instrument_type"].isin(["CE","PE"]))].copy()
        opts["expiry_dt"] = pd.to_datetime(opts["expiry"])
        fut_exp = opts[opts["expiry_dt"] >= pd.Timestamp.now()]["expiry_dt"].unique()
        if not len(fut_exp): return {}
        expiry = sorted(fut_exp)[0]
        expiry_label = str(expiry.date())

        tok_map = {}
        for s in strikes:
            for t in ["CE","PE"]:
                row = opts[(opts["strike"]==s)&(opts["instrument_type"]==t)&(opts["expiry_dt"]==expiry)]
                if not row.empty:
                    tok_map[f"NFO:{row.iloc[0]['tradingsymbol']}"] = {"strike":s,"type":t}

        q_raw = kite.quote(list(tok_map.keys()))
        chain = {}
        for sym, meta in tok_map.items():
            q = q_raw.get(sym)
            if not q: continue
            s = meta["strike"]; t = meta["type"]
            chain.setdefault(s,{})[t] = {
                "ltp":     q.get("last_price",0),
                "oi":      q.get("oi",0) or 0,
                "oi_high": q.get("oi_day_high",0) or 0,
                "oi_low":  q.get("oi_day_low",0) or 0,
                "iv":      q.get("implied_volatility",0) or 0,
            }

        ce_oi = {s: chain[s].get("CE",{}).get("oi",0) for s in chain}
        pe_oi = {s: chain[s].get("PE",{}).get("oi",0) for s in chain}
        ce_add= {s: chain[s].get("CE",{}).get("oi",0)-chain[s].get("CE",{}).get("oi_low",0) for s in chain}
        pe_add= {s: chain[s].get("PE",{}).get("oi",0)-chain[s].get("PE",{}).get("oi_low",0) for s in chain}
        pe_drp= {s: chain[s].get("PE",{}).get("oi",0)-chain[s].get("PE",{}).get("oi_high",0) for s in chain}

        tot_ce = sum(ce_oi.values()) or 1
        tot_pe = sum(pe_oi.values()) or 1
        pcr    = round(tot_pe/tot_ce, 2)

        near = [s for s in chain if abs(s-atm) <= step*4]
        _nce = sum(chain[s].get("CE",{}).get("oi",0) for s in near) or 1
        _npe = sum(chain[s].get("PE",{}).get("oi",0) for s in near) or 1
        _ce_bld = round(sum(ce_add.get(s,0) for s in near)/_nce*100, 1)
        _pe_bld = round(sum(pe_add.get(s,0) for s in near)/_npe*100, 1)
        _pe_drp = round(sum(pe_drp.get(s,0) for s in near)/_npe*100, 1)
        _ce_drp = round(sum(chain[s].get("CE",{}).get("oi",0)-chain[s].get("CE",{}).get("oi_high",0) for s in near)/_nce*100, 1)

        cw_list = [s for s in ce_oi if s > spot]
        pf_list = [s for s in pe_oi if s < spot]
        ncw = min(cw_list, key=lambda x:x-spot) if cw_list else None
        npf = min(pf_list, key=lambda x:spot-x) if pf_list else None

        if   _ce_bld>=5 and _pe_drp<=-3: direction="🔴 BEARISH";       dr=f"Waterfall: CE +{_ce_bld}% / PE {_pe_drp}%"
        elif _pe_bld>=5 and _ce_drp<=-3: direction="🟢 BULLISH";       dr=f"Short squeeze: PE +{_pe_bld}% / CE {_ce_drp}%"
        elif _ce_bld>=5:                  direction="🔴 BEARISH BIAS";  dr=f"Call wall building +{_ce_bld}%"
        elif _pe_bld>=5:                  direction="🟢 BULLISH BIAS";  dr=f"Put floor building +{_pe_bld}%"
        elif _ce_drp<=-3:                 direction="🟢 BULLISH";       dr=f"Call wall unwinding {_ce_drp}%"
        elif _pe_drp<=-3:                 direction="🔴 BEARISH";       dr=f"Put floor unwinding {_pe_drp}%"
        elif pcr>=1.3:                    direction="🟢 BULLISH";       dr=f"PCR={pcr} — strong put support"
        elif pcr<=0.7:                    direction="🔴 BEARISH";       dr=f"PCR={pcr} — strong call pressure"
        else:                             direction="⚠️ SIDEWAYS";       dr=f"PCR={pcr}, balanced OI"

        pain_vals = {}
        for ps in strikes:
            t=0
            for s,d in chain.items():
                if s<ps: t+=d.get("CE",{}).get("oi",0)*(ps-s)
                elif s>ps: t+=d.get("PE",{}).get("oi",0)*(s-ps)
            pain_vals[ps]=t
        max_pain = min(pain_vals, key=pain_vals.get) if pain_vals else atm

        chain_rows = []
        for s in sorted(chain.keys()):
            ce=chain[s].get("CE",{}); pe=chain[s].get("PE",{})
            chain_rows.append({
                "strike": s,
                "status": "ATM" if s==atm else ("ITM" if s<atm else "OTM"),
                "is_atm": s==atm,
                "ce_ltp": round(ce.get("ltp",0),1),
                "ce_oi":  int(ce.get("oi",0)),
                "ce_oi_add": int(ce.get("oi",0)-ce.get("oi_low",0)),
                "ce_iv":  round(ce.get("iv",0),1),
                "pe_ltp": round(pe.get("ltp",0),1),
                "pe_oi":  int(pe.get("oi",0)),
                "pe_oi_add": int(pe.get("oi",0)-pe.get("oi_low",0)),
                "pe_iv":  round(pe.get("iv",0),1),
            })

        result = {
            "spot": round(spot,2), "fut_ltp": round(fut_ltp,2),
            "atm": atm, "max_pain": max_pain, "pcr": pcr,
            "expiry": expiry_label,
            "nearest_call_wall": ncw, "nearest_put_floor": npf,
            "strongest_ce": max(ce_oi,key=ce_oi.get) if ce_oi else atm,
            "strongest_pe": max(pe_oi,key=pe_oi.get) if pe_oi else atm,
            "direction": direction, "direction_reason": dr,
            "total_ce_oi": tot_ce, "total_pe_oi": tot_pe,
            "chain_rows": chain_rows,
            "timestamp": _ts(),
        }
        try:
            with open(OI_CACHE_FILE,"w") as f: json.dump(result, f, default=str)
        except Exception: pass
        return result
    except Exception as e:
        print(f"[{_ts()}] OI fetch error: {e}")
        return {}


def fetch_smc(oi_data):
    if not SMC_OK or not _state.get("kite"): return {}
    try:
        c15 = fetch_nifty_candles_kite(_state["kite"], interval="15minute", days=5)
        c1h = fetch_nifty_candles_kite(_state["kite"], interval="60minute", days=15)
        if not c15: return {}
        r = get_smc_confluence(oi_intel=oi_data, candles_15m=c15, candles_1h=c1h or None)
        r["timestamp"] = _ts()
        return r
    except Exception as e:
        print(f"[{_ts()}] SMC error: {e}"); return {}


def fetch_bos():
    if not BOS_OK or not DB_OK: return load_scan_cache().get("events", []) if BOS_OK else []
    try:
        db = OHLCStore()
        symbols = db.get_all_symbols()
        results = []
        for sym in symbols:
            try:
                candles = db.get(sym, n=BOS_LOOKBACK)
                if not candles or len(candles) < 10: continue
                if datetime.strptime(candles[-1]["datetime"][:10],"%Y-%m-%d").date() < date.today(): continue
                bos = detect_bos(candles)
                if not bos["bos_type"]: continue
                setup = build_bos_setup(bos, sym, candles[-1]["close"])
                results.append({
                    "symbol":    setup["symbol"],
                    "bos_type":  setup["bos_type"],
                    "ltp":       setup["ltp"],
                    "broken":    setup["broken_level"],
                    "sl":        setup["sl"],
                    "t1":        setup["t1"],
                    "t2":        setup["t2"],
                    "rr_now":    setup["rr_now"],
                    "rr_retest": setup["rr_retest"],
                    "strength":  setup["strength"],
                    "vol_ratio": setup["volume_ratio"],
                    "ob_low":    setup["ob_low"],
                    "ob_high":   setup["ob_high"],
                    "next_liq":  setup["next_liq"],
                    "prev_trend":setup.get("prev_trend",""),
                })
            except Exception: continue
        return results
    except Exception as e:
        print(f"[{_ts()}] BOS error: {e}"); return []


def fetch_screener():
    kite = _state.get("kite")
    if not kite: return {}
    try:
        quotes = {}
        syms_nse = [f"NSE:{s}" for s in STOCKS]
        for i in range(0, len(syms_nse), 400):
            try: quotes.update(kite.quote(syms_nse[i:i+400]))
            except Exception: pass

        panchak = {}
        if os.path.exists(PANCHAK_DATA_FILE):
            try:
                pdf = pd.read_csv(PANCHAK_DATA_FILE)
                for _, r in pdf.iterrows():
                    panchak[r["Symbol"]] = {
                        "TOP_HIGH": float(r.get("TOP_HIGH",0)),
                        "TOP_LOW":  float(r.get("TOP_LOW",0)),
                        "DIFF":     float(r.get("DIFF",0)),
                        "BT":       float(r.get("BT",0)),
                        "ST":       float(r.get("ST",0)),
                    }
            except Exception: pass

        rows = []
        for sym in STOCKS:
            q = quotes.get(f"NSE:{sym}")
            if not q: continue
            ltp   = q.get("last_price",0)
            pc    = q["ohlc"]["close"]
            chg_p = round((ltp-pc)/pc*100, 2) if pc else 0
            lo    = round(q["ohlc"]["open"],2)
            lh    = round(q["ohlc"]["high"],2)
            ll    = round(q["ohlc"]["low"],2)
            pan   = panchak.get(sym,{})
            th    = pan.get("TOP_HIGH",0); tl = pan.get("TOP_LOW",0)
            diff  = pan.get("DIFF",0)
            bt    = pan.get("BT",0);      st = pan.get("ST",0)

            if th>0 and tl>0:
                if   ltp>=th:   near = "BREAK ↑"
                elif ltp<=tl:   near = "BREAK ↓"
                elif (th-ltp)<=(ltp-tl): near = f"↑ {th-ltp:.1f}"
                else:           near = f"↓ {ltp-tl:.1f}"
            else: near = "—"

            rows.append({
                "symbol": sym, "ltp": round(ltp,2), "chg_p": chg_p,
                "day_open": lo, "day_high": lh, "day_low": ll,
                "top_high": th, "top_low": tl, "diff": round(diff,2),
                "bt": round(bt,2), "st": round(st,2), "near": near,
            })

        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        if df.empty: return {"rows":[], "top_high":[], "top_low":[], "ohl":[], "near":[]}

        top_high = df[df["ltp"]>=df["top_high"].replace(0,float("inf"))].to_dict("records") if "top_high" in df else []
        top_low  = df[(df["top_low"]>0) & (df["ltp"]<=df["top_low"])].to_dict("records")
        # O=L: open within 0.1% of day low (genuine bullish setup)
        ohl_bull = df[((df["day_low"]-df["day_open"]).abs() / df["day_open"].clip(lower=1) <= 0.001)].copy()
        ohl_bull["setup"] = "O=L 🟢"
        # O=H: open within 0.1% of day high (genuine bearish setup)
        ohl_bear = df[((df["day_high"]-df["day_open"]).abs() / df["day_open"].clip(lower=1) <= 0.001)].copy()
        ohl_bear["setup"] = "O=H 🔴"
        # Remove overlaps (stock can't be both O=H and O=L unless it's a doji)
        ohl_bull = ohl_bull[~ohl_bull["symbol"].isin(ohl_bear["symbol"])]
        ohl      = pd.concat([ohl_bull, ohl_bear]).drop_duplicates("symbol").to_dict("records")

        def is_near(r):
            th=r.get("top_high",0); tl=r.get("top_low",0); ltp=r.get("ltp",0); diff=r.get("diff",0) or (th-tl)
            if th==0 or tl==0: return False
            tol = max(diff*0.5, ltp*0.005)
            return (abs(ltp-th)<=tol or abs(ltp-tl)<=tol) and not (ltp>=th or ltp<=tl)
        near_rows = [r for r in rows if is_near(r)]

        return {
            "rows":     rows,
            "top_high": [r for r in rows if r.get("near","").startswith("BREAK ↑")],
            "top_low":  [r for r in rows if r.get("near","").startswith("BREAK ↓")],
            "ohl":      ohl,
            "near":     near_rows,
            "timestamp": _ts(),
        }
    except Exception as e:
        print(f"[{_ts()}] Screener error: {e}"); return {}



# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4: MOVERS + KP PANCHANG FETCHERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_movers():
    """Top gainers, top losers, volume surge — from live quotes."""
    kite = _state.get("kite")
    if not kite: return {}
    try:
        quotes = {}
        for i in range(0, len(STOCKS), 400):
            try: quotes.update(kite.quote([f"NSE:{s}" for s in STOCKS[i:i+400]]))
            except Exception: pass
        rows = []
        for sym in STOCKS:
            q = quotes.get(f"NSE:{sym}")
            if not q: continue
            ltp   = q.get("last_price", 0)
            pc    = q["ohlc"]["close"]
            chg_p = round((ltp - pc) / pc * 100, 2) if pc else 0
            vol   = q.get("volume", 0)
            rows.append({"symbol": sym, "ltp": round(ltp, 2), "chg_p": chg_p,
                         "volume": vol, "day_high": round(q["ohlc"]["high"], 2),
                         "day_low": round(q["ohlc"]["low"], 2),
                         "day_open": round(q["ohlc"]["open"], 2), "prev_close": round(pc, 2)})
        if not rows: return {}
        import pandas as pd
        df = pd.DataFrame(rows)
        gainers = df[df["chg_p"] > 2].sort_values("chg_p", ascending=False).head(20).to_dict("records")
        losers  = df[df["chg_p"] < -2].sort_values("chg_p").head(20).to_dict("records")
        # Volume surge: stocks whose day volume is already > 1.5x yesterday avg
        # Proxy: volume > 1M and in top 20 by volume (no yesterday data in this fast path)
        vol_surge = df[df["volume"] > 500000].sort_values("volume", ascending=False).head(20).to_dict("records")
        result = {
            "gainers":   gainers,
            "losers":    losers,
            "vol_surge": vol_surge,
            "timestamp": _ts(),
        }
        # Telegram alerts for big movers
        _alert_movers(gainers, losers)
        return result
    except Exception as e:
        print(f"[{_ts()}] Movers error: {e}"); return {}


_alert_movers_dedup: dict = {}   # {key: date_str} — fires once per day per category

def _alert_movers(gainers, losers):
    """Send Telegram movers alert — once per day per category."""
    import urllib.request
    TG_BOT  = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
    TG_CHAT = "-1003706739531"
    today   = date.today().isoformat()
    ts      = _ts()

    def _can_send(key):
        return _alert_movers_dedup.get(key) != today

    def _mark_sent(key):
        _alert_movers_dedup[key] = today

    def _send(msg, key):
        if not _can_send(key): return
        try:
            url = f"https://api.telegram.org/bot{TG_BOT}/sendMessage"
            payload = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=8): pass
            _mark_sent(key)
            entry = {"time": ts, "category": key, "message": msg[:100]}
            with _lock:
                _state["alerts_log"].insert(0, entry)
                _state["alerts_log"] = _state["alerts_log"][:100]
        except Exception: pass

    if gainers:
        lines = "\n".join(f"  • <b>{r['symbol']}</b>  {r['ltp']}  +{r['chg_p']}%" for r in gainers[:8])
        _send(f"🔥 <b>TOP GAINERS</b>  ⏰ {ts}\n{lines}", "TOP_GAINERS")
    if losers:
        lines = "\n".join(f"  • <b>{r['symbol']}</b>  {r['ltp']}  {r['chg_p']}%" for r in losers[:8])
        _send(f"💥 <b>TOP LOSERS</b>  ⏰ {ts}\n{lines}", "TOP_LOSERS")


def fetch_kp():
    """KP Panchang — calls astro_engine for planet positions + computes trading windows."""
    try:
        import pytz
        from astro_engine import get_all_planets, get_moon_data, get_dignities, get_angular_distance
        IST = pytz.timezone("Asia/Kolkata")
        now = datetime.now(IST)

        planets  = get_all_planets(now)
        moon     = get_moon_data(now)
        digs     = get_dignities(now)

        # Moon sub-lord (KP)
        nak_span = 360 / 27
        SUB_SEQ  = ["Ke","Ve","Su","Mo","Ma","Ra","Ju","Sa","Me"]
        moon_deg = moon["degree"]
        pos_in_nak = moon_deg % nak_span
        sub_idx = int(pos_in_nak / nak_span * 9)
        kp_sub = SUB_SEQ[min(sub_idx, 8)]

        KP_BULL = {"Ju","Ve","Mo","Su"}
        KP_BEAR = {"Sa","Ra","Ke","Ma"}

        # Hora lord at market open 9:15
        DAY_LORDS = ["Su","Mo","Ma","Me","Ju","Ve","Sa"]
        HORA_SEQ  = ["Su","Ve","Me","Mo","Sa","Ju","Ma"]
        day_lord  = DAY_LORDS[now.weekday()]  # Mon=Mo, Tue=Ma etc
        HORA_START= {"Su":0,"Ve":1,"Me":2,"Mo":3,"Sa":4,"Ju":5,"Ma":6}
        hora_idx  = (HORA_START.get(day_lord, 0) + 2) % 7   # 9:15 = slot 2
        hora_open = HORA_SEQ[hora_idx]

        # Next 6 horas (each 1 hour)
        horas = []
        for i in range(6):
            idx   = (hora_idx + i) % 7
            planet= HORA_SEQ[idx]
            start_h = 9 + i
            bias  = "🟢 Bull" if planet in {"Su","Ju","Ve","Mo"} else "🔴 Bear"
            horas.append({"time": f"{start_h:02d}:15–{start_h+1:02d}:15",
                           "planet": planet, "bias": bias})

        # Key angles
        def ang(pa, pb):
            try: return get_angular_distance(pa, pb, now)
            except: return 0

        # Bull signals
        bull_sigs = []
        bear_sigs = []
        if kp_sub in KP_BULL: bull_sigs.append(f"KP Sub-lord {kp_sub} = BULLISH")
        if kp_sub in KP_BEAR: bear_sigs.append(f"KP Sub-lord {kp_sub} = BEARISH")
        if digs.get("Jupiter") == "EXALTED": bull_sigs.append("Jupiter EXALTED — market floor")
        if digs.get("Moon")    == "EXALTED": bull_sigs.append("Moon EXALTED in Taurus — recovery")
        mj = ang("Moon","Jupiter")
        if mj < 10: bull_sigs.append(f"Moon-Jupiter conjunct {mj:.0f}° (Gajakesari)")
        mk = ang("Moon","Ketu")
        if mk < 12: bear_sigs.append(f"Moon-Ketu {mk:.0f}° — BankNifty weak")
        ms = ang("Moon","Saturn")
        if ms < 10: bear_sigs.append(f"Moon-Saturn {ms:.0f}° (Vish Yoga) — fall risk")

        # Overall KP signal
        if len(bull_sigs) > len(bear_sigs):   kp_signal = "🟢 KP BULLISH"
        elif len(bear_sigs) > len(bull_sigs):  kp_signal = "🔴 KP BEARISH"
        else:                                   kp_signal = "⚠️ KP MIXED"

        return {
            "moon_nak":      moon.get("nakshatra","—"),
            "moon_sign":     moon.get("sign","—"),
            "moon_lord":     moon.get("nak_lord","—"),
            "kp_sub":        kp_sub,
            "hora_open":     hora_open,
            "hora_bias":     "🟢 Bull" if hora_open in {"Su","Ju","Ve","Mo"} else "🔴 Bear",
            "kp_signal":     kp_signal,
            "bull_signals":  bull_sigs,
            "bear_signals":  bear_sigs,
            "horas":         horas,
            "dignities":     {k: v for k, v in digs.items()},
            "moon_jupiter":  round(mj, 1),
            "moon_ketu":     round(mk, 1),
            "moon_saturn":   round(ms, 1),
            "planets": {
                k: {"sign": v.get("sign",""), "nakshatra": v.get("nakshatra",""), "degree": round(v.get("degree",0),2)}
                for k, v in planets.items() if k not in ("jd","ayanamsha","computed_at")
            },
            "timestamp": now.strftime("%H:%M:%S IST"),
        }
    except Exception as e:
        print(f"[{_ts()}] KP fetch error: {e}"); return {}


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND REFRESH THREAD
# ─────────────────────────────────────────────────────────────────────────────

def _refresh_loop():
    """Background thread: refreshes all data every AUTO_REFRESH_SECS during market hours."""
    print(f"[{_ts()}] Background refresh thread started (interval: {AUTO_REFRESH_SECS}s)")
    while True:
        try:
            mh = is_market_hours() if DB_OK else True   # always refresh if DB not available
            if mh and _state["kite_ok"]:
                print(f"[{_ts()}] Refreshing data…")
                with _lock:
                    _state["refreshing"] = True

                oi   = fetch_oi()
                smc  = fetch_smc(oi) if oi else {}
                bos  = fetch_bos()
                scr  = fetch_screener()
                movs = fetch_movers()
                kp   = fetch_kp()

                try:
                    astro = get_astro_score() if ASTRO_OK else {}
                except Exception: astro = {}
                try:
                    time_sig = get_time_signal_detail() if TIME_OK else {}
                except Exception: time_sig = {}

                with _lock:
                    if oi:   _state["oi"]       = oi
                    if smc:  _state["smc"]       = smc
                    if bos:  _state["bos"]       = bos
                    if scr:  _state["screener"]  = scr
                    if movs: _state["movers"]    = movs
                    if kp:   _state["kp"]        = kp
                    if astro:    _state["astro"]      = astro
                    if time_sig: _state["time_signal"]= time_sig
                    _state["last_refresh"] = _ts()
                    _state["refreshing"]   = False
                print(f"[{_ts()}] ✅ Refresh | OI:{bool(oi)} BOS:{len(bos)} Gainers:{len(movs.get('gainers',[]))} Losers:{len(movs.get('losers',[]))}")
            else:
                # Market closed: refresh astro/time only (no API cost)
                try:
                    with _lock:
                        if ASTRO_OK: _state["astro"] = get_astro_score()
                        if TIME_OK:  _state["time_signal"] = get_time_signal_detail()
                except Exception: pass
        except Exception as e:
            print(f"[{_ts()}] Refresh loop error: {e}")
            with _lock: _state["refreshing"] = False
        time.sleep(AUTO_REFRESH_SECS)


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP + ROUTES
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Panchak Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/status")
def api_status():
    return {
        "kite_ok":      _state["kite_ok"],
        "kite_error":   _state["kite_error"],
        "last_refresh": _state["last_refresh"],
        "refreshing":   _state["refreshing"],
        "market_open":  is_market_hours() if DB_OK else None,
        "server_time":  _ts(),
    }

@app.get("/api/oi")
def api_oi():
    with _lock: return JSONResponse(_state["oi"] or {})

@app.get("/api/smc")
def api_smc():
    with _lock: return JSONResponse(_state["smc"] or {})

@app.get("/api/bos")
def api_bos():
    with _lock: return JSONResponse({"events": _state["bos"], "count": len(_state["bos"])})

@app.get("/api/astro")
def api_astro():
    with _lock: return JSONResponse(_state["astro"] or {})

@app.get("/api/time")
def api_time():
    with _lock: data = dict(_state["time_signal"])
    try:
        if TIME_OK: data = get_time_signal_detail()   # always fresh
    except Exception: pass
    return JSONResponse(data)

@app.get("/api/screener")
def api_screener():
    with _lock: return JSONResponse(_state["screener"] or {})

@app.get("/api/movers")
def api_movers():
    with _lock: return JSONResponse(_state["movers"] or {})

@app.get("/api/kp")
def api_kp():
    with _lock: return JSONResponse(_state["kp"] or {})

@app.get("/api/alerts")
def api_alerts():
    with _lock: return JSONResponse({"log": _state["alerts_log"]})

@app.post("/api/refresh")
def api_force_refresh():
    """Force an immediate refresh."""
    def _do():
        oi   = fetch_oi()
        smc  = fetch_smc(oi) if oi else {}
        bos  = fetch_bos()
        scr  = fetch_screener()
        movs = fetch_movers()
        kp   = fetch_kp()
        with _lock:
            if oi:   _state["oi"]      = oi
            if smc:  _state["smc"]     = smc
            if bos:  _state["bos"]     = bos
            if scr:  _state["screener"]= scr
            if movs: _state["movers"]  = movs
            if kp:   _state["kp"]      = kp
            _state["last_refresh"] = _ts()
    threading.Thread(target=_do, daemon=True).start()
    return {"status": "refresh started"}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD HTML (single-file, no CDN dependencies, auto-refreshes via fetch)
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📈 Panchak Dashboard</title>
<style>
  :root {
    --bg:      #0d1117; --bg2: #161b22; --bg3: #21262d;
    --border:  #30363d; --text: #e6edf3; --dim:  #8b949e;
    --green:   #3fb950; --red:  #f85149; --yellow:#d29922;
    --blue:    #58a6ff; --purple:#bc8cff;
    --bg-bull: #0d2819; --bg-bear:#2d0f0f;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: "Segoe UI", monospace; font-size: 13px; }

  /* Header */
  #header {
    display: flex; align-items: center; gap: 16px;
    background: var(--bg2); border-bottom: 1px solid var(--border);
    padding: 10px 20px; position: sticky; top: 0; z-index: 100;
  }
  #header h1 { font-size: 16px; color: var(--blue); }
  #spot  { font-size: 15px; font-weight: bold; color: var(--yellow); }
  #clock { margin-left: auto; font-size: 13px; font-weight: bold; color: var(--yellow); }
  #kite-status { font-size: 11px; }
  #market-status{ font-size: 11px; }
  .refresh-bar {
    display:flex; align-items:center; gap:10px;
    background: var(--bg2); padding: 6px 20px;
    border-bottom: 1px solid var(--border);
  }
  .prog-wrap { flex:1; background:var(--bg3); border-radius:3px; height:4px; }
  .prog-fill  { height:4px; background:var(--blue); border-radius:3px; transition:width 1s linear; }
  .btn { background:var(--bg3); color:var(--blue); border:1px solid var(--border);
         padding:5px 14px; border-radius:5px; cursor:pointer; font-size:12px; }
  .btn:hover { background:var(--blue); color:#fff; }

  /* Tabs */
  .tabs { display:flex; background:var(--bg2); border-bottom:1px solid var(--border); padding:0 12px; gap:2px; }
  .tab  { padding:8px 14px; cursor:pointer; color:var(--dim); border-bottom:2px solid transparent;
          font-size:12px; white-space:nowrap; }
  .tab.active { color:var(--blue); border-bottom-color:var(--blue); }
  .tab:hover  { color:var(--text); }
  .panel { display:none; padding:12px 16px; }
  .panel.active { display:block; }

  /* Cards */
  .card { background:var(--bg2); border:1px solid var(--border); border-radius:6px;
          padding:12px; margin-bottom:10px; }
  .card-row { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:10px; }
  .pill { background:var(--bg3); border:1px solid var(--border); border-radius:5px;
          padding:6px 14px; min-width:120px; }
  .pill .pill-label { font-size:9px; color:var(--dim); text-transform:uppercase; }
  .pill .pill-value { font-size:15px; font-weight:bold; margin-top:2px; }

  /* Tables */
  .tbl-wrap { overflow-x:auto; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th { background:var(--bg2); color:var(--dim); padding:5px 8px;
       text-align:center; border-bottom:2px solid var(--border);
       font-size:11px; text-transform:uppercase; white-space:nowrap; position:sticky; top:0; }
  td { padding:4px 8px; text-align:center; border-bottom:1px solid #21262d; white-space:nowrap; }
  tr.atm td  { background: #1c1c3a; font-weight:bold; }
  tr.bull td { background: var(--bg-bull); }
  tr.bear td { background: var(--bg-bear); }
  tr:hover td { filter:brightness(1.2); }

  /* Direction banner */
  .dir-banner { padding:10px 14px; border-radius:6px; margin-bottom:10px;
                border:2px solid var(--border); display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
  .dir-banner .dir-main { font-size:17px; font-weight:bold; }
  .dir-banner .dir-sub  { font-size:12px; color:var(--dim); }

  /* Color helpers */
  .green  { color:var(--green) !important; }
  .red    { color:var(--red) !important; }
  .yellow { color:var(--yellow) !important; }
  .blue   { color:var(--blue) !important; }
  .purple { color:var(--purple) !important; }
  .dim    { color:var(--dim) !important; }

  .section-title { font-size:13px; font-weight:bold; color:var(--blue); margin:10px 0 6px; }

  /* Score badge */
  .score-badge {
    font-size:28px; font-weight:bold; text-align:center; padding:16px;
    border-radius:8px; margin-bottom:10px;
  }
  /* Reasons list */
  .reason { padding:4px 8px; border-left:3px solid var(--border); margin:3px 0; font-size:12px; }
  .reason.bull { border-color:var(--green); color:var(--green); }
  .reason.bear { border-color:var(--red);   color:var(--red);   }

  /* Screener sub-tabs */
  .sub-tabs { display:flex; gap:4px; flex-wrap:wrap; margin-bottom:10px; }
  .sub-tab  { padding:4px 12px; cursor:pointer; color:var(--dim); background:var(--bg3);
               border:1px solid var(--border); border-radius:4px; font-size:11px; }
  .sub-tab.active { color:var(--blue); border-color:var(--blue); }

  /* Time zone */
  .time-zone { font-size:22px; font-weight:bold; text-align:center; margin:8px 0; }
  .time-clock{ font-size:36px; font-weight:bold; text-align:center; color:var(--yellow); }
  .time-good { font-size:14px; font-weight:bold; text-align:center; margin:6px 0; }

  @media(max-width:600px) {
    .tabs { overflow-x:auto; }
    .card-row { flex-direction:column; }
  }
</style>
</head>
<body>

<div id="header">
  <h1>📈 PANCHAK DASHBOARD</h1>
  <span id="spot">NIFTY: —</span>
  <span id="kite-status" class="dim">🔴 Connecting…</span>
  <span id="market-status" class="dim">—</span>
  <span id="clock">—</span>
</div>

<div class="refresh-bar">
  <span id="refresh-lbl" class="dim" style="font-size:11px;">Auto-refresh every 60s</span>
  <div class="prog-wrap"><div class="prog-fill" id="prog" style="width:100%"></div></div>
  <span id="last-refresh" class="dim" style="font-size:11px;">—</span>
  <button class="btn" onclick="forceRefresh()">⟳ Refresh Now</button>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('oi')">📊 OI Intel</div>
  <div class="tab" onclick="showTab('smc')">🧠 SMC + OI</div>
  <div class="tab" onclick="showTab('bos')">📐 BOS Scanner</div>
  <div class="tab" onclick="showTab('screener')">📈 Screeners</div>
  <div class="tab" onclick="showTab('movers')">🔥 Movers</div>
  <div class="tab" onclick="showTab('kp')">🪐 KP Panchang</div>
  <div class="tab" onclick="showTab('alerts')">🔔 Alerts</div>
  <div class="tab" onclick="showTab('astro')">🌙 Astro</div>
  <div class="tab" onclick="showTab('time')">⏰ Time Signal</div>
</div>

<!-- ═══════════════ OI PANEL ═══════════════ -->
<div id="panel-oi" class="panel active">
  <div class="section-title">📊 OI Intelligence — NIFTY Options Chain</div>
  <div class="card-row" id="oi-pills">
    <div class="pill"><div class="pill-label">NIFTY Spot</div><div class="pill-value blue" id="oi-spot">—</div></div>
    <div class="pill"><div class="pill-label">ATM Strike</div><div class="pill-value yellow" id="oi-atm">—</div></div>
    <div class="pill"><div class="pill-label">Max Pain</div><div class="pill-value purple" id="oi-pain">—</div></div>
    <div class="pill"><div class="pill-label">PCR</div><div class="pill-value" id="oi-pcr">—</div></div>
    <div class="pill"><div class="pill-label">Expiry</div><div class="pill-value dim" id="oi-expiry">—</div></div>
    <div class="pill"><div class="pill-label">Total CE OI</div><div class="pill-value red" id="oi-ce-tot">—</div></div>
    <div class="pill"><div class="pill-label">Total PE OI</div><div class="pill-value green" id="oi-pe-tot">—</div></div>
  </div>
  <div id="oi-dir-banner" class="dir-banner">
    <div><div class="dir-main" id="oi-dir">—</div><div class="dir-sub" id="oi-dr">—</div></div>
    <div><span class="dim">Call Wall: </span><b id="oi-cw">—</b>
         <span class="dim" style="margin-left:12px">Put Floor: </span><b id="oi-pf">—</b>
         <span class="dim" style="margin-left:12px">Str CE: </span><b id="oi-sce">—</b>
         <span class="dim" style="margin-left:12px">Str PE: </span><b id="oi-spe">—</b></div>
  </div>
  <div class="section-title">Options Chain (ATM ± 10 strikes)</div>
  <div class="tbl-wrap">
    <table id="chain-tbl">
      <thead><tr>
        <th>CE IV</th><th>CE OI Δ</th><th>CE OI</th><th>CE LTP</th>
        <th>STRIKE</th><th>STATUS</th>
        <th>PE LTP</th><th>PE OI</th><th>PE OI Δ</th><th>PE IV</th>
      </tr></thead>
      <tbody id="chain-body"></tbody>
    </table>
  </div>
</div>

<!-- ═══════════════ SMC PANEL ═══════════════ -->
<div id="panel-smc" class="panel">
  <div class="section-title">🧠 SMC + OI Confluence</div>
  <div id="smc-banner" class="score-badge" style="background:var(--bg2);border:2px solid var(--border);">
    Run a refresh to load SMC analysis
  </div>
  <div class="card-row">
    <div class="pill"><div class="pill-label">OI Direction</div><div class="pill-value" id="smc-oi-dir">—</div></div>
    <div class="pill"><div class="pill-label">SMC LTF</div><div class="pill-value" id="smc-ltf">—</div></div>
    <div class="pill"><div class="pill-label">SMC HTF</div><div class="pill-value" id="smc-htf">—</div></div>
    <div class="pill"><div class="pill-label">P/D Zone</div><div class="pill-value" id="smc-pd">—</div></div>
    <div class="pill"><div class="pill-label">PCR</div><div class="pill-value" id="smc-pcr">—</div></div>
    <div class="pill"><div class="pill-label">Conflict</div><div class="pill-value" id="smc-conflict">—</div></div>
  </div>
  <div class="card-row">
    <div class="pill"><div class="pill-label">Call Wall</div><div class="pill-value red" id="smc-cw">—</div></div>
    <div class="pill"><div class="pill-label">Put Floor</div><div class="pill-value green" id="smc-pf">—</div></div>
    <div class="pill"><div class="pill-label">Bull OB</div><div class="pill-value green" id="smc-ob-b">—</div></div>
    <div class="pill"><div class="pill-label">Bear OB</div><div class="pill-value red" id="smc-ob-r">—</div></div>
    <div class="pill"><div class="pill-label">Bull FVG</div><div class="pill-value green" id="smc-fvg-b">—</div></div>
    <div class="pill"><div class="pill-label">Bear FVG</div><div class="pill-value red" id="smc-fvg-r">—</div></div>
    <div class="pill"><div class="pill-label">Buy Liq</div><div class="pill-value yellow" id="smc-bliq">—</div></div>
    <div class="pill"><div class="pill-label">Sell Liq</div><div class="pill-value yellow" id="smc-sliq">—</div></div>
  </div>
  <div class="section-title">Analysis Reasons</div>
  <div id="smc-reasons"></div>
</div>

<!-- ═══════════════ BOS PANEL ═══════════════ -->
<div id="panel-bos" class="panel">
  <div class="section-title">📐 1-Hour BOS / CHoCH Scanner</div>
  <div class="card-row" id="bos-stats" style="margin-bottom:10px;">
    <div class="pill"><div class="pill-label">Total Events</div><div class="pill-value blue" id="bos-total">0</div></div>
    <div class="pill"><div class="pill-label">Bullish 🚀</div><div class="pill-value green" id="bos-bull">0</div></div>
    <div class="pill"><div class="pill-label">Bearish 💥</div><div class="pill-value red" id="bos-bear">0</div></div>
    <div class="pill"><div class="pill-label">CHoCH 🔄</div><div class="pill-value purple" id="bos-choch">0</div></div>
  </div>
  <div class="tbl-wrap">
    <table id="bos-tbl">
      <thead><tr>
        <th>Symbol</th><th>Type</th><th>LTP</th><th>Broke</th>
        <th>Strength</th><th>Vol×</th><th>OB Zone</th><th>Next Liq</th>
        <th>SL</th><th>T1</th><th>T2</th><th>R:R now</th><th>R:R ret</th><th>Prior</th>
      </tr></thead>
      <tbody id="bos-body"></tbody>
    </table>
  </div>
</div>

<!-- ═══════════════ SCREENER PANEL ═══════════════ -->
<div id="panel-screener" class="panel">
  <div class="section-title">📈 Stock Screeners</div>
  <div style="font-size:11px;color:var(--dim);margin-bottom:8px;">
    Panchak period: <b id="panchak-period" class="yellow">—</b> &nbsp;|&nbsp;
    TOP_HIGH = period HIGH &nbsp;|&nbsp; TOP_LOW = period LOW
  </div>
  <div class="card-row">
    <div class="pill"><div class="pill-label">Panchak TH Breaks</div><div class="pill-value green" id="scr-th">0</div></div>
    <div class="pill"><div class="pill-label">Panchak TL Breaks</div><div class="pill-value red" id="scr-tl">0</div></div>
    <div class="pill"><div class="pill-label">OHL Setups</div><div class="pill-value yellow" id="scr-ohl">0</div></div>
    <div class="pill"><div class="pill-label">Near Levels</div><div class="pill-value purple" id="scr-near">0</div></div>
  </div>
  <div class="sub-tabs">
    <div class="sub-tab active" onclick="showScr('all')">📊 All</div>
    <div class="sub-tab" onclick="showScr('th')">🟢 TOP_HIGH</div>
    <div class="sub-tab" onclick="showScr('tl')">🔴 TOP_LOW</div>
    <div class="sub-tab" onclick="showScr('ohl')">📐 OHL</div>
    <div class="sub-tab" onclick="showScr('near')">🎯 Near</div>
  </div>
  <div class="tbl-wrap" id="scr-all-wrap">
    <table><thead><tr><th>Symbol</th><th>LTP</th><th>Chg%</th><th>TOP_HIGH</th><th>TOP_LOW</th><th>DIFF</th><th>BT</th><th>ST</th><th>NEAR</th><th>Day H</th><th>Day L</th></tr></thead>
    <tbody id="scr-all-body"></tbody></table>
  </div>
  <div class="tbl-wrap" id="scr-th-wrap" style="display:none">
    <table><thead><tr><th>Symbol</th><th>LTP</th><th>TOP_HIGH</th><th>GAIN</th><th>Chg%</th></tr></thead>
    <tbody id="scr-th-body"></tbody></table>
  </div>
  <div class="tbl-wrap" id="scr-tl-wrap" style="display:none">
    <table><thead><tr><th>Symbol</th><th>LTP</th><th>TOP_LOW</th><th>LOSS</th><th>Chg%</th></tr></thead>
    <tbody id="scr-tl-body"></tbody></table>
  </div>
  <div class="tbl-wrap" id="scr-ohl-wrap" style="display:none">
    <table><thead><tr><th>Symbol</th><th>LTP</th><th>Chg%</th><th>Day Open</th><th>Day H</th><th>Day L</th><th>Setup</th></tr></thead>
    <tbody id="scr-ohl-body"></tbody></table>
  </div>
  <div class="tbl-wrap" id="scr-near-wrap" style="display:none">
    <table><thead><tr><th>Symbol</th><th>LTP</th><th>Chg%</th><th>TOP_HIGH</th><th>TOP_LOW</th><th>NEAR</th></tr></thead>
    <tbody id="scr-near-body"></tbody></table>
  </div>
</div>

<!-- ═══════════════ ASTRO PANEL ═══════════════ -->
<div id="panel-astro" class="panel">
  <div class="section-title">🌙 Vedic / KP Astro Score</div>
  <div class="card" style="text-align:center;">
    <div style="font-size:22px;font-weight:bold;margin-bottom:6px;" id="astro-signal">—</div>
    <div style="font-size:18px;font-weight:bold;" id="astro-score">Score: —</div>
  </div>
  <div class="card-row">
    <div class="pill"><div class="pill-label">Nakshatra</div><div class="pill-value blue" id="astro-nak">—</div></div>
    <div class="pill"><div class="pill-label">Moon Sign</div><div class="pill-value yellow" id="astro-sign">—</div></div>
    <div class="pill"><div class="pill-label">Nak Lord</div><div class="pill-value dim" id="astro-lord">—</div></div>
    <div class="pill"><div class="pill-label">KP Sub-lord</div><div class="pill-value purple" id="astro-sub">—</div></div>
  </div>
  <div class="card" id="astro-reason" style="color:var(--dim);font-size:12px;">—</div>
</div>

<!-- ═══════════════ TIME PANEL ═══════════════ -->
<div id="panel-time" class="panel">
  <div class="section-title">⏰ Time-of-Day Trading Signal</div>
  <div class="card" style="text-align:center;">
    <div class="time-clock" id="time-clock">—</div>
    <div class="time-zone" id="time-zone">—</div>
    <div style="font-size:12px;color:var(--dim);margin:4px 0;" id="time-desc">—</div>
    <div style="font-size:12px;color:var(--dim);" id="time-mins">—</div>
    <div class="time-good" id="time-good">—</div>
  </div>
  <div class="section-title">Session Guide</div>
  <div class="tbl-wrap">
    <table>
      <thead><tr><th>Time</th><th>Zone</th><th>Note</th></tr></thead>
      <tbody>
        <tr><td class="dim">09:08–09:25</td><td class="yellow">🚧 Opening Risk</td><td class="dim">Avoid immediate trades</td></tr>
        <tr><td class="dim">09:25–09:45</td><td class="yellow">⚡ Opening Range</td><td class="dim">Observe only</td></tr>
        <tr><td class="dim">09:45–10:30</td><td class="green">🔥 Strong Trend</td><td class="dim">Best trending hour</td></tr>
        <tr><td class="dim">10:30–11:30</td><td class="green">🟢 Momentum</td><td class="dim">Trail existing trades</td></tr>
        <tr><td class="dim">11:30–12:00</td><td class="yellow">🟡 Slow Zone</td><td class="dim">Reduce size</td></tr>
        <tr><td class="dim">12:00–13:00</td><td class="yellow">🟡 Consolidation</td><td class="dim">Wait for breakout</td></tr>
        <tr><td class="dim">13:00–13:45</td><td class="blue">🚀 Afternoon Push</td><td class="dim">Institutional accumulation</td></tr>
        <tr><td class="dim">13:45–14:30</td><td class="blue">🚀 Breakout Zone</td><td class="dim">F&O expiry pressure</td></tr>
        <tr><td class="dim">14:30–15:00</td><td class="yellow">⚡ Closing Rush</td><td class="dim">Sharp directional moves</td></tr>
        <tr><td class="dim">15:00–15:30</td><td class="red">⛔ Avoid Entry</td><td class="dim">No new positions</td></tr>
      </tbody>
    </table>
  </div>
</div>


<!-- ═══════════════ MOVERS PANEL ═══════════════ -->
<div id="panel-movers" class="panel">
  <div class="section-title">🔥 Top Movers — Live</div>
  <div class="card-row">
    <div class="pill"><div class="pill-label">Gainers >2%</div><div class="pill-value green" id="mov-g-count">0</div></div>
    <div class="pill"><div class="pill-label">Losers <-2%</div><div class="pill-value red" id="mov-l-count">0</div></div>
    <div class="pill"><div class="pill-label">Vol Surge (Top20)</div><div class="pill-value blue" id="mov-v-count">0</div></div>
    <div class="pill"><div class="pill-label">Updated</div><div class="pill-value dim" id="mov-ts">—</div></div>
  </div>
  <div class="sub-tabs">
    <div class="sub-tab active" onclick="showMovers('gainers')">🟢 Top Gainers</div>
    <div class="sub-tab" onclick="showMovers('losers')">🔴 Top Losers</div>
    <div class="sub-tab" onclick="showMovers('vol')">📊 Volume Surge</div>
  </div>
  <div id="mov-gainers-wrap" class="tbl-wrap">
    <table><thead><tr><th>#</th><th>Symbol</th><th>LTP</th><th>Chg%</th><th>Day High</th><th>Day Low</th><th>Day Open</th><th>Prev Close</th></tr></thead>
    <tbody id="mov-gainers-body"></tbody></table>
  </div>
  <div id="mov-losers-wrap" class="tbl-wrap" style="display:none">
    <table><thead><tr><th>#</th><th>Symbol</th><th>LTP</th><th>Chg%</th><th>Day High</th><th>Day Low</th><th>Day Open</th><th>Prev Close</th></tr></thead>
    <tbody id="mov-losers-body"></tbody></table>
  </div>
  <div id="mov-vol-wrap" class="tbl-wrap" style="display:none">
    <table><thead><tr><th>#</th><th>Symbol</th><th>LTP</th><th>Chg%</th><th>Volume</th><th>Day High</th><th>Day Low</th></tr></thead>
    <tbody id="mov-vol-body"></tbody></table>
  </div>
</div>

<!-- ═══════════════ KP PANCHANG PANEL ═══════════════ -->
<div id="panel-kp" class="panel">
  <div class="section-title">🪐 KP Panchang — Trading Windows</div>
  <div class="card-row">
    <div class="pill"><div class="pill-label">KP Signal</div><div class="pill-value" id="kp-signal">—</div></div>
    <div class="pill"><div class="pill-label">Moon Nakshatra</div><div class="pill-value blue" id="kp-nak">—</div></div>
    <div class="pill"><div class="pill-label">Moon Sign</div><div class="pill-value yellow" id="kp-sign">—</div></div>
    <div class="pill"><div class="pill-label">KP Sub-lord</div><div class="pill-value purple" id="kp-sub">—</div></div>
    <div class="pill"><div class="pill-label">Opening Hora</div><div class="pill-value" id="kp-hora">—</div></div>
    <div class="pill"><div class="pill-label">Hora Bias</div><div class="pill-value" id="kp-hora-bias">—</div></div>
  </div>
  <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
    <div class="card" style="flex:1;min-width:240px;">
      <div class="section-title" style="margin-top:0">🟢 Bullish Signals</div>
      <div id="kp-bull-sigs" style="font-size:12px;color:var(--green);">—</div>
    </div>
    <div class="card" style="flex:1;min-width:240px;">
      <div class="section-title" style="margin-top:0">🔴 Bearish Signals</div>
      <div id="kp-bear-sigs" style="font-size:12px;color:var(--red);">—</div>
    </div>
    <div class="card" style="flex:1;min-width:240px;">
      <div class="section-title" style="margin-top:0">🔑 Key Aspects</div>
      <div id="kp-angles" style="font-size:12px;color:var(--dim);">—</div>
    </div>
  </div>
  <div class="section-title">⏰ Intraday Hora Windows</div>
  <div class="tbl-wrap">
    <table><thead><tr><th>Time</th><th>Hora Planet</th><th>Bias</th></tr></thead>
    <tbody id="kp-hora-body"></tbody></table>
  </div>
  <div class="section-title" style="margin-top:12px">🌍 Planet Positions</div>
  <div class="tbl-wrap">
    <table><thead><tr><th>Planet</th><th>Sign</th><th>Nakshatra</th><th>Dignity</th><th>Degree</th></tr></thead>
    <tbody id="kp-planets-body"></tbody></table>
  </div>
</div>

<!-- ═══════════════ ALERTS PANEL ═══════════════ -->
<div id="panel-alerts" class="panel">
  <div class="section-title">🔔 Alert Log</div>
  <div class="card" style="margin-bottom:10px;">
    <div style="font-size:12px;color:var(--dim);">
      All Telegram alerts fired by the dashboard are logged here.
      Alerts fire automatically on each refresh cycle during market hours.
    </div>
  </div>
  <div class="tbl-wrap">
    <table><thead><tr><th>Time</th><th>Category</th><th>Details</th></tr></thead>
    <tbody id="alerts-body"></tbody></table>
  </div>
</div>
<script>
const REFRESH_SECS = 60;
let _remaining = REFRESH_SECS;
let _scrTab = 'all';

// ── Tab switching ──────────────────────────────────────────────────────────
function showTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('panel-' + id).classList.add('active');
}
function showScr(id) {
  _scrTab = id;
  document.querySelectorAll('.sub-tab').forEach(t => t.classList.remove('active'));
  event.target.classList.add('active');
  ['all','th','tl','ohl','near'].forEach(k => {
    document.getElementById('scr-' + k + '-wrap').style.display = (k === id) ? '' : 'none';
  });
}

// ── Helpers ────────────────────────────────────────────────────────────────
function fmtOI(n) {
  n = parseInt(n) || 0;
  if (n >= 10000000) return (n/10000000).toFixed(1) + 'Cr';
  if (n >= 100000)   return (n/100000).toFixed(1) + 'L';
  return n.toString();
}
function colorChg(v) { return parseFloat(v) >= 0 ? 'green' : 'red'; }
function fmtChg(v)   { v=parseFloat(v)||0; return (v>=0?'+':'')+v.toFixed(2)+'%'; }
function colorDir(s) { return s.includes('BULL') ? '#3fb950' : s.includes('BEAR') ? '#f85149' : '#d29922'; }
function el(id) { return document.getElementById(id); }
function td(text, cls='') { return `<td class="${cls}">${text}</td>`; }
function setText(id, text, cls='') {
  const e = el(id); if(!e) return;
  e.textContent = text;
  if(cls) e.className = cls;
}

// ── Clock (runs locally every second) ─────────────────────────────────────
function updateClock() {
  const now = new Date();
  const ist = new Date(now.toLocaleString('en-US', {timeZone:'Asia/Kolkata'}));
  const pad = n => String(n).padStart(2,'0');
  el('clock').textContent =
    `${pad(ist.getHours())}:${pad(ist.getMinutes())}:${pad(ist.getSeconds())} IST`;
}
setInterval(updateClock, 1000); updateClock();

// ── Progress bar countdown ─────────────────────────────────────────────────
function tickProgress() {
  _remaining = Math.max(0, _remaining - 1);
  const pct = (_remaining / REFRESH_SECS * 100).toFixed(1);
  el('prog').style.width = pct + '%';
  el('refresh-lbl').textContent = `Next refresh: ${_remaining}s`;
  if (_remaining <= 0) { _remaining = REFRESH_SECS; fetchAll(); }
}
setInterval(tickProgress, 1000);

// ── Force refresh ──────────────────────────────────────────────────────────
async function forceRefresh() {
  try { await fetch('/api/refresh', {method:'POST'}); } catch(e) {}
  _remaining = REFRESH_SECS;
  setTimeout(fetchAll, 2000);
}

// ── Status ─────────────────────────────────────────────────────────────────
async function fetchStatus() {
  try {
    const r = await fetch('/api/status'); const d = await r.json();
    el('kite-status').textContent = d.kite_ok ? '🟢 Kite' : '🔴 Kite: ' + (d.kite_error||'disconnected');
    el('kite-status').className = d.kite_ok ? 'green' : 'red';
    el('market-status').textContent = d.market_open ? '🟢 MARKET OPEN' : '🔴 MARKET CLOSED';
    el('market-status').className = d.market_open ? 'green' : 'red';
    if (d.last_refresh) el('last-refresh').textContent = 'Last: ' + d.last_refresh;
  } catch(e) {}
}

// ── OI ─────────────────────────────────────────────────────────────────────
async function fetchOI() {
  try {
    const r = await fetch('/api/oi'); const d = await r.json();
    if (!d.spot) return;
    setText('oi-spot',   d.spot?.toLocaleString('en-IN', {minimumFractionDigits:1}));
    setText('oi-atm',    d.atm);
    setText('oi-pain',   d.max_pain);
    const pcrEl = el('oi-pcr'); pcrEl.textContent = d.pcr;
    pcrEl.className = 'pill-value ' + (d.pcr>=1.3?'green':d.pcr<=0.7?'red':'yellow');
    setText('oi-expiry', d.expiry||'—');
    setText('oi-ce-tot', fmtOI(d.total_ce_oi));
    setText('oi-pe-tot', fmtOI(d.total_pe_oi));
    el('spot').textContent = 'NIFTY: ' + (d.spot?.toLocaleString('en-IN',{minimumFractionDigits:1}));

    const dirEl = el('oi-dir');
    dirEl.textContent = d.direction; dirEl.style.color = colorDir(d.direction||'');
    setText('oi-dr', d.direction_reason||'');

    const banner = el('oi-dir-banner');
    banner.style.borderColor = colorDir(d.direction||'');
    banner.style.background = d.direction?.includes('BULL') ? '#0d2819' : d.direction?.includes('BEAR') ? '#2d0f0f' : '#1c1c2e';

    setText('oi-cw',  d.nearest_call_wall||'—'); el('oi-cw').className='red';
    setText('oi-pf',  d.nearest_put_floor||'—'); el('oi-pf').className='green';
    setText('oi-sce', d.strongest_ce||'—');
    setText('oi-spe', d.strongest_pe||'—');

    // Chain table
    const body = el('chain-body'); body.innerHTML = '';
    (d.chain_rows||[]).forEach(row => {
      const tr = document.createElement('tr');
      if (row.is_atm) tr.className = 'atm';
      const oacc = v => v>0?'green':v<0?'red':'dim';
      tr.innerHTML =
        td(row.ce_iv?.toFixed(1)+'%','dim') +
        td(fmtOI(row.ce_oi_add), oacc(row.ce_oi_add)) +
        td(fmtOI(row.ce_oi),'red') +
        td(row.ce_ltp?.toFixed(1),'red') +
        `<td style="font-weight:${row.is_atm?'bold':'normal'};color:${row.is_atm?'var(--yellow)':'var(--text)'}">${row.strike}</td>` +
        td(row.status, row.is_atm?'yellow':'dim') +
        td(row.pe_ltp?.toFixed(1),'green') +
        td(fmtOI(row.pe_oi),'green') +
        td(fmtOI(row.pe_oi_add), oacc(row.pe_oi_add)) +
        td(row.pe_iv?.toFixed(1)+'%','dim');
      body.appendChild(tr);
    });
  } catch(e) { console.error('OI fetch error:', e); }
}

// ── SMC ─────────────────────────────────────────────────────────────────────
async function fetchSMC() {
  try {
    const r = await fetch('/api/smc'); const d = await r.json();
    if (!d.final_signal) return;
    const score = d.final_score||0;
    const sc = score>0?'var(--green)':score<0?'var(--red)':'var(--yellow)';
    const cm = {green:'var(--bg-bull)',red:'var(--bg-bear)',yellow:'#1c1c2e',grey:'var(--bg2)'};
    el('smc-banner').style.background = cm[d.signal_color]||'var(--bg2)';
    el('smc-banner').style.borderColor = {green:'var(--green)',red:'var(--red)',yellow:'var(--yellow)',grey:'var(--border)'}[d.signal_color]||'var(--border)';
    el('smc-banner').innerHTML = `<div style="font-size:18px;font-weight:bold;color:${sc}">${d.final_signal}</div><div style="font-size:13px;color:var(--dim)">${d.final_action||''}</div><div style="font-size:15px;font-weight:bold;color:${sc}">Score: ${score>0?'+':''}${score}</div>`;

    const tc = v => v?.includes('BULL')?'green':v?.includes('BEAR')?'red':'yellow';
    setText('smc-oi-dir',  d.oi_direction||'—'); el('smc-oi-dir').className=tc(d.oi_direction);
    setText('smc-ltf',     d.smc_trend_ltf||'—'); el('smc-ltf').className=tc(d.smc_trend_ltf);
    setText('smc-htf',     d.smc_trend_htf||'—'); el('smc-htf').className=tc(d.smc_trend_htf);
    setText('smc-pd',      d.pd_zone||'—');
    setText('smc-pcr',     (d.oi_pcr||0).toFixed(2));
    const conf = d.conflict_detected;
    setText('smc-conflict', conf?'⚠️ YES':'✅ NO'); el('smc-conflict').className=conf?'yellow':'green';

    const ob_b=d.nearest_bullish_ob; const ob_r=d.nearest_bearish_ob;
    const fvg_b=d.nearest_bullish_fvg; const fvg_r=d.nearest_bearish_fvg;
    setText('smc-cw',   d.oi_call_wall||'—');
    setText('smc-pf',   d.oi_put_floor||'—');
    setText('smc-ob-b', ob_b  ? `${ob_b.low?.toFixed(0)}–${ob_b.high?.toFixed(0)}`  : '—');
    setText('smc-ob-r', ob_r  ? `${ob_r.low?.toFixed(0)}–${ob_r.high?.toFixed(0)}`  : '—');
    setText('smc-fvg-b',fvg_b ? `${fvg_b.bottom?.toFixed(0)}–${fvg_b.top?.toFixed(0)}` : '—');
    setText('smc-fvg-r',fvg_r ? `${fvg_r.bottom?.toFixed(0)}–${fvg_r.top?.toFixed(0)}` : '—');
    setText('smc-bliq', d.nearest_buy_liq  ? d.nearest_buy_liq.toFixed(0)  : '—');
    setText('smc-sliq', d.nearest_sell_liq ? d.nearest_sell_liq.toFixed(0) : '—');

    const reasonsEl = el('smc-reasons'); reasonsEl.innerHTML='';
    (d.reasons||[]).forEach(r => {
      const div=document.createElement('div');
      div.className='reason '+(r.includes('✅')||r.includes('🟢')||r.includes('BULL')?'bull':r.includes('❌')||r.includes('🔴')||r.includes('BEAR')?'bear':'');
      div.textContent=r; reasonsEl.appendChild(div);
    });
  } catch(e) {}
}

// ── BOS ─────────────────────────────────────────────────────────────────────
async function fetchBOS() {
  try {
    const r = await fetch('/api/bos'); const d = await r.json();
    const events = d.events||[];
    setText('bos-total', events.length);
    const bull=events.filter(e=>e.bos_type?.includes('UP')).length;
    const bear=events.length-bull;
    const choch=events.filter(e=>e.bos_type?.includes('CHOCH')).length;
    setText('bos-bull',bull); setText('bos-bear',bear); setText('bos-choch',choch);

    const body=el('bos-body'); body.innerHTML='';
    events.forEach(ev => {
      const ib='UP' in (ev.bos_type||''); const ic=ev.bos_type?.includes('CHOCH');
      const icon=ib&&!ic?'🚀':ic?'🔄':'💥';
      const tr=document.createElement('tr');
      tr.className = ib?'bull':'bear';
      tr.innerHTML =
        `<td class="blue">${ev.symbol}</td>` +
        `<td class="${ib?'green':ic?'purple':'red'}">${icon} ${(ev.bos_type||'').replace('_',' ')}</td>` +
        td(ev.ltp?.toFixed(1)) +
        td(ev.broken?.toFixed(1),'yellow') +
        td((ev.strength||0).toFixed(2)+'%','dim') +
        td((ev.vol_ratio||1).toFixed(1)+'×') +
        td(`${ev.ob_low?.toFixed(1)||0}–${ev.ob_high?.toFixed(1)||0}`,'dim') +
        td(ev.next_liq?.toFixed(1)||'—','dim') +
        td(ev.sl?.toFixed(1)||'—','red') +
        td(ev.t1?.toFixed(1)||'—','green') +
        td(ev.t2?.toFixed(1)||'—','green') +
        td((ev.rr_now||0)+':1', ev.rr_now>=2?'green':'dim') +
        td((ev.rr_retest||0)+':1', ev.rr_retest>=2?'green':'dim') +
        td(ev.prev_trend||'—','dim');
      body.appendChild(tr);
    });
  } catch(e) {}
}

// ── SCREENER ──────────────────────────────────────────────────────────────
async function fetchScreener() {
  try {
    const r = await fetch('/api/screener'); const d = await r.json();
    if (!d.rows) return;
    setText('scr-th',   (d.top_high||[]).length);
    setText('scr-tl',   (d.top_low||[]).length);
    setText('scr-ohl',  (d.ohl||[]).length);
    setText('scr-near', (d.near||[]).length);

    // All panchak table
    const allBody=el('scr-all-body'); allBody.innerHTML='';
    (d.rows||[]).forEach(row => {
      const near=row.near||'';
      const tr=document.createElement('tr');
      tr.className=near.startsWith('BREAK ↑')?'bull':near.startsWith('BREAK ↓')?'bear':'';
      const nc=near.startsWith('BREAK ↑')?'green':near.startsWith('BREAK ↓')?'red':near.includes('↑')?'green':'yellow';
      tr.innerHTML=
        `<td class="blue">${row.symbol}</td>`+
        td(row.ltp)+
        td(fmtChg(row.chg_p),colorChg(row.chg_p))+
        td(row.top_high||'—','green')+
        td(row.top_low||'—','red')+
        td(row.diff||'—','dim')+
        td(row.bt||'—','dim')+
        td(row.st||'—','dim')+
        `<td class="${nc}">${near}</td>`+
        td(row.day_high||'—','dim')+
        td(row.day_low||'—','dim');
      allBody.appendChild(tr);
    });

    // TOP_HIGH table
    const thBody=el('scr-th-body'); thBody.innerHTML='';
    (d.top_high||[]).forEach(row => {
      const gain=(row.ltp-(row.top_high||0)).toFixed(2);
      const tr=document.createElement('tr'); tr.className='bull';
      tr.innerHTML=`<td class="blue">${row.symbol}</td>`+td(row.ltp)+td(row.top_high,'yellow')+td('+'+gain,'green')+td(fmtChg(row.chg_p),colorChg(row.chg_p));
      thBody.appendChild(tr);
    });

    // TOP_LOW table
    const tlBody=el('scr-tl-body'); tlBody.innerHTML='';
    (d.top_low||[]).forEach(row => {
      const loss=((row.top_low||0)-row.ltp).toFixed(2);
      const tr=document.createElement('tr'); tr.className='bear';
      tr.innerHTML=`<td class="blue">${row.symbol}</td>`+td(row.ltp)+td(row.top_low,'yellow')+td('-'+loss,'red')+td(fmtChg(row.chg_p),colorChg(row.chg_p));
      tlBody.appendChild(tr);
    });

    // OHL table
    const ohlBody=el('scr-ohl-body'); ohlBody.innerHTML='';
    (d.ohl||[]).forEach(row => {
      const ib=(row.setup||'').startsWith('O=L');
      const tr=document.createElement('tr'); tr.className=ib?'bull':'bear';
      tr.innerHTML=`<td class="blue">${row.symbol}</td>`+td(row.ltp)+td(fmtChg(row.chg_p),colorChg(row.chg_p))+td(row.day_open||'—','dim')+td(row.day_high||'—','dim')+td(row.day_low||'—','dim')+td(row.setup||'—',ib?'green':'red');
      ohlBody.appendChild(tr);
    });

    // Near table
    const nearBody=el('scr-near-body'); nearBody.innerHTML='';
    (d.near||[]).forEach(row => {
      const iu=(row.near||'').includes('↑');
      const tr=document.createElement('tr'); tr.className=iu?'bull':'bear';
      tr.innerHTML=`<td class="blue">${row.symbol}</td>`+td(row.ltp)+td(fmtChg(row.chg_p),colorChg(row.chg_p))+td(row.top_high||'—','green')+td(row.top_low||'—','red')+td(row.near||'—','yellow');
      nearBody.appendChild(tr);
    });
  } catch(e) {}
}

// ── ASTRO ──────────────────────────────────────────────────────────────────
async function fetchAstro() {
  try {
    const r = await fetch('/api/astro'); const d = await r.json();
    if (!d.signal) return;
    const score=d.score||0;
    const sc=score>0?'var(--green)':score<0?'var(--red)':'var(--yellow)';
    el('astro-signal').textContent=d.signal; el('astro-signal').style.color=sc;
    el('astro-score').textContent='Score: '+(score>0?'+':'')+score; el('astro-score').style.color=sc;
    setText('astro-nak',  d.nakshatra||'—');
    setText('astro-sign', d.moon_sign||'—');
    setText('astro-lord', d.nak_lord||'—');
    setText('astro-sub',  d.sub_lord||'—');
    el('astro-reason').textContent = d.reason||'—';
  } catch(e) {}
}

// ── TIME ───────────────────────────────────────────────────────────────────
async function fetchTime() {
  try {
    const r = await fetch('/api/time'); const d = await r.json();
    if (!d.signal) return;
    const good = d.signal?.includes('TREND')||d.signal?.includes('MOMENTUM')||d.signal?.includes('BREAK')||d.signal?.includes('AFTERNOON')||d.signal?.includes('OPENING RANGE');
    const zc = good?'var(--green)':d.signal?.includes('AVOID')||d.signal?.includes('CLOSED')?'var(--red)':'var(--yellow)';
    setText('time-clock', d.time_str||'—');
    el('time-zone').textContent=d.signal||'—'; el('time-zone').style.color=zc;
    setText('time-desc', d.description||'—');
    setText('time-mins', (d.mins_left||0)+' min remaining in zone');
    el('time-good').textContent=good?'✅ GOOD ENTRY TIME':'⏸ WAIT / NO ENTRY';
    el('time-good').style.color=good?'var(--green)':'var(--red)';
  } catch(e) {}
}


// ── MOVERS sub-tab switching ───────────────────────────────────────────────
let _movTab = 'gainers';
function showMovers(id) {
  _movTab = id;
  document.querySelectorAll('#panel-movers .sub-tab').forEach(t => t.classList.remove('active'));
  event.target.classList.add('active');
  ['gainers','losers','vol'].forEach(k => {
    el('mov-' + k + '-wrap').style.display = (k === id) ? '' : 'none';
  });
}

// ── MOVERS fetch ───────────────────────────────────────────────────────────
async function fetchMovers() {
  try {
    const r = await fetch('/api/movers'); const d = await r.json();
    if (!d.gainers && !d.losers) return;
    const g = d.gainers||[], l = d.losers||[], v = d.vol_surge||[];
    setText('mov-g-count', g.length);
    setText('mov-l-count', l.length);
    setText('mov-v-count', v.length);
    setText('mov-ts', d.timestamp||'—');

    function fillMovTable(bodyId, rows, isVol) {
      const body = el(bodyId); body.innerHTML = '';
      rows.forEach((row, i) => {
        const chg = parseFloat(row.chg_p||0);
        const cc  = chg >= 0 ? 'green' : 'red';
        const bg  = chg >= 0 ? 'bull' : 'bear';
        const tr  = document.createElement('tr');
        tr.className = bg;
        if (isVol) {
          tr.innerHTML =
            `<td class="dim">${i+1}</td>` +
            `<td class="blue">${row.symbol}</td>` +
            td(row.ltp) + td(fmtChg(row.chg_p), cc) +
            `<td class="${chg>=0?'green':'red'}" style="font-weight:bold">${(row.volume/100000).toFixed(1)}L</td>` +
            td(row.day_high,'dim') + td(row.day_low,'dim');
        } else {
          tr.innerHTML =
            `<td class="dim">${i+1}</td>` +
            `<td class="blue">${row.symbol}</td>` +
            td(row.ltp) +
            `<td class="${cc}" style="font-size:14px;font-weight:bold">${fmtChg(row.chg_p)}</td>` +
            td(row.day_high,'dim') + td(row.day_low,'dim') +
            td(row.day_open,'dim') + td(row.prev_close,'dim');
        }
        body.appendChild(tr);
      });
    }
    fillMovTable('mov-gainers-body', g, false);
    fillMovTable('mov-losers-body',  l, false);
    fillMovTable('mov-vol-body',     v, true);
  } catch(e) { console.error('Movers error:', e); }
}

// ── KP PANCHANG fetch ─────────────────────────────────────────────────────
async function fetchKP() {
  try {
    const r = await fetch('/api/kp'); const d = await r.json();
    if (!d.kp_signal) return;
    const sc = d.kp_signal.includes('BULL') ? 'green' : d.kp_signal.includes('BEAR') ? 'red' : 'yellow';
    const sigEl = el('kp-signal');
    sigEl.textContent = d.kp_signal; sigEl.className = sc;
    setText('kp-nak',  d.moon_nak||'—');
    setText('kp-sign', d.moon_sign||'—');
    setText('kp-sub',  d.kp_sub||'—');
    const horaEl = el('kp-hora');
    horaEl.textContent = d.hora_open||'—';
    horaEl.className = (d.hora_bias||'').includes('Bull') ? 'green' : 'red';
    const hbEl = el('kp-hora-bias');
    hbEl.textContent = d.hora_bias||'—';
    hbEl.className = (d.hora_bias||'').includes('Bull') ? 'green' : 'red';

    // Bull / Bear signals
    const bullEl = el('kp-bull-sigs');
    bullEl.innerHTML = (d.bull_signals||[]).length
      ? (d.bull_signals||[]).map(s => `<div style="margin:3px 0">✅ ${s}</div>`).join('')
      : '<div class="dim">None</div>';
    const bearEl = el('kp-bear-sigs');
    bearEl.innerHTML = (d.bear_signals||[]).length
      ? (d.bear_signals||[]).map(s => `<div style="margin:3px 0">⚠️ ${s}</div>`).join('')
      : '<div class="dim">None</div>';

    // Key angles
    el('kp-angles').innerHTML =
      `<div>Moon–Jupiter: <b class="${(d.moon_jupiter||999)<10?'green':'dim'}">${d.moon_jupiter||'—'}°</b></div>` +
      `<div>Moon–Ketu: <b class="${(d.moon_ketu||999)<12?'red':'dim'}">${d.moon_ketu||'—'}°</b></div>` +
      `<div>Moon–Saturn: <b class="${(d.moon_saturn||999)<10?'red':'dim'}">${d.moon_saturn||'—'}°</b></div>`;

    // Hora table
    const horaBody = el('kp-hora-body'); horaBody.innerHTML = '';
    (d.horas||[]).forEach(h => {
      const tr = document.createElement('tr');
      const bc = h.bias.includes('Bull') ? 'bull' : 'bear';
      tr.className = bc;
      tr.innerHTML = td(h.time,'dim') + `<td style="font-weight:bold">${h.planet}</td>` + td(h.bias, h.bias.includes('Bull')?'green':'red');
      horaBody.appendChild(tr);
    });

    // Planets table
    const planBody = el('kp-planets-body'); planBody.innerHTML = '';
    const DIG_COLOR = {EXALTED:'green', OWN:'blue', DEBILITATED:'red', NEUTRAL:'dim'};
    const digs = d.dignities||{};
    const planets = d.planets||{};
    const order = ['Sun','Moon','Mars','Mercury','Jupiter','Venus','Saturn','Rahu','Ketu'];
    order.forEach(p => {
      const pdata = planets[p]; if(!pdata) return;
      const dig = digs[p]||'NEUTRAL';
      const dc  = DIG_COLOR[dig]||'dim';
      const tr  = document.createElement('tr');
      tr.innerHTML =
        `<td style="font-weight:bold">${p}</td>` +
        td(pdata.sign||'—','yellow') +
        td(pdata.nakshatra||'—','dim') +
        `<td class="${dc}">${dig}</td>` +
        td((pdata.degree||0).toFixed(2)+'°','dim');
      planBody.appendChild(tr);
    });
  } catch(e) { console.error('KP error:', e); }
}

// ── ALERTS LOG fetch ───────────────────────────────────────────────────────
async function fetchAlerts() {
  try {
    const r = await fetch('/api/alerts'); const d = await r.json();
    const log = d.log||[];
    const body = el('alerts-body'); body.innerHTML = '';
    const CAT_COLOR = {
      'TOP_GAINERS':'green','TOP_LOSERS':'red',
      'TOP_HIGH':'green','TOP_LOW':'red',
      'OHL_BULL':'green','OHL_BEAR':'red',
    };
    log.forEach(entry => {
      const tr = document.createElement('tr');
      const cc = CAT_COLOR[(entry.category||'').toUpperCase()] || 'dim';
      tr.innerHTML =
        td(entry.time||'','dim') +
        `<td class="${cc}" style="font-weight:bold">${entry.category||''}</td>` +
        td(entry.message||'');
      body.appendChild(tr);
    });
  } catch(e) {}
}

// ── MASTER FETCH ───────────────────────────────────────────────────────────
async function fetchAll() {
  await fetchStatus();
  await Promise.all([
    fetchOI(), fetchBOS(), fetchAstro(), fetchTime(),
    fetchScreener(), fetchMovers(), fetchKP(), fetchAlerts()
  ]);
  await fetchSMC();   // after OI
  el('panchak-period').textContent = 'Mar 17–20 2026';
}

// Start
fetchAll();
setInterval(fetchStatus, 10000);
setInterval(fetchAlerts, 30000);   // alert log refreshes every 30s
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Panchak Web Dashboard")
    print(f"  Open in browser: http://0.0.0.0:{PORT}")
    print(f"  Or from other devices: http://YOUR-AWS-IP:{PORT}")
    print(f"  Auto-refresh: every {AUTO_REFRESH_SECS}s during market hours")
    print("=" * 60)

    # Connect Kite
    if not init_kite():
        print(f"⚠️  Kite not connected: {_state['kite_error']}")
        print("   Place access_token.txt in the same folder and restart.")

    # Load initial data from cache
    try:
        if os.path.exists(OI_CACHE_FILE):
            with open(OI_CACHE_FILE) as f:
                _state["oi"] = json.load(f)
            print(f"[{_ts()}] Loaded OI cache")
    except Exception: pass

    try:
        if BOS_OK:
            c = load_scan_cache()
            if c: _state["bos"] = c.get("events", [])
            print(f"[{_ts()}] Loaded BOS cache: {len(_state['bos'])} events")
    except Exception: pass

    try:
        if ASTRO_OK: _state["astro"] = get_astro_score()
        if TIME_OK:  _state["time_signal"] = get_time_signal_detail()
    except Exception: pass

    try:
        _state["kp"] = fetch_kp()
        if _state["kp"]:
            print(f"[{_ts()}] Loaded KP: {_state['kp'].get('kp_signal','')}")
    except Exception: pass

    # Start background refresh thread
    t = threading.Thread(target=_refresh_loop, daemon=True)
    t.start()

    # Run server
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
