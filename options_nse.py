import requests
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

# =========================
# CONFIG
# =========================
SYMBOL = "RELIANCE"
STRIKE_GAP = 20
EXCEL_FILE = "RELIANCE_OPTION_CHAIN.xlsx"
INTERVAL_SECONDS = 300  # 5 minutes

# =========================
# NSE SESSION (HARDENED)
# =========================
def create_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive"
    })

    # VERY IMPORTANT – warmup
    s.get("https://www.nseindia.com", timeout=10)
    time.sleep(1)
    s.get("https://www.nseindia.com/option-chain", timeout=10)
    time.sleep(1)

    return s

session = create_session()

# =========================
# SAFE JSON FETCH
# =========================
def safe_get_json(url):
    r = session.get(url, timeout=10)
    if r.status_code != 200 or not r.text.strip():
        raise Exception("Empty / blocked NSE response")
    return r.json()

# =========================
# SPOT DATA
# =========================
def get_spot_data():
    url = f"https://www.nseindia.com/api/quote-equity?symbol={SYMBOL}"
    j = safe_get_json(url)
    p = j["priceInfo"]
    return {
        "spot": p["lastPrice"],
        "open": p["open"],
        "high": p["intraDayHighLow"]["max"],
        "low": p["intraDayHighLow"]["min"],
        "prev_close": p["previousClose"]
    }

# =========================
# OPTION CHAIN
# =========================
def get_option_chain():
    url = f"https://www.nseindia.com/api/option-chain-equities?symbol={SYMBOL}"
    return safe_get_json(url)

# =========================
# ATM ±2
# =========================
def get_atm_strikes(spot):
    atm = round(spot / STRIKE_GAP) * STRIKE_GAP
    return [atm - 40, atm - 20, atm, atm + 20, atm + 40]

# =========================
# EXTRACT DATA
# =========================
def extract_rows(chain, strikes, spot):
    rows = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for d in chain["records"]["data"]:
        if d.get("strikePrice") in strikes:
            ce = d.get("CE", {})
            pe = d.get("PE", {})

            rows.append({
                "Time": ts,
                "Spot": spot,
                "Strike": d["strikePrice"],
                "CE_LTP": ce.get("lastPrice"),
                "CE_OI": ce.get("openInterest"),
                "CE_OI_Change": ce.get("changeinOpenInterest"),
                "CE_Volume": ce.get("totalTradedVolume"),
                "PE_LTP": pe.get("lastPrice"),
                "PE_OI": pe.get("openInterest"),
                "PE_OI_Change": pe.get("changeinOpenInterest"),
                "PE_Volume": pe.get("totalTradedVolume"),
            })

    return pd.DataFrame(rows)

# =========================
# STRENGTH LOGIC
# =========================
def calculate_strength(df):
    out = {}
    for mins, rows in [(15, 3), (30, 6), (60, 12)]:
        r = df.tail(rows)
        ce = r["CE_OI_Change"].sum()
        pe = r["PE_OI_Change"].sum()

        if ce > pe and ce > 0:
            out[f"{mins}m"] = "BULLISH"
        elif pe > ce and pe > 0:
            out[f"{mins}m"] = "BEARISH"
        else:
            out[f"{mins}m"] = "RANGE"
    return out

# =========================
# MAIN LOOP
# =========================
print("🚀 RELIANCE NSE OPTION ENGINE STARTED")

while True:
    try:
        spot = get_spot_data()
        chain = get_option_chain()
        strikes = get_atm_strikes(spot["spot"])

        df_new = extract_rows(chain, strikes, spot["spot"])

        if Path(EXCEL_FILE).exists():
            df_old = pd.read_excel(EXCEL_FILE)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_all = df_new

        df_all.to_excel(EXCEL_FILE, index=False)

        strength = calculate_strength(df_all)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"Spot {spot['spot']} | "
              f"15m:{strength['15m']} "
              f"30m:{strength['30m']} "
              f"60m:{strength['60m']}")

    except Exception as e:
        print("⚠ NSE blocked / retrying:", e)
        session = create_session()

    time.sleep(INTERVAL_SECONDS)
