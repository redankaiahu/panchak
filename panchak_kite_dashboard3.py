import os
import time
import pandas as pd
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
import pytz

# =========================
# CONFIG
# =========================
BASE_DIR = os.getcwd()
CACHE_DIR = os.path.join(BASE_DIR, "CACHE")
os.makedirs(CACHE_DIR, exist_ok=True)

INST_FILE = os.path.join(CACHE_DIR, "instruments_NSE.csv")
DAILY_FILE = os.path.join(CACHE_DIR, "DAILY_OHLC.xlsx")
WEEKLY_FILE = os.path.join(CACHE_DIR, "WEEKLY_OHLC.xlsx")
MONTHLY_FILE = os.path.join(CACHE_DIR, "MONTHLY_OHLC.xlsx")

ACCESS_TOKEN_FILE = "AccessToken_23jan26.txt"
API_KEY = "YOUR_API_KEY"
API_KEY_FILE = os.path.join(BASE_DIR, "API_KEY.txt")
ACCESS_TOKEN_FILE = os.path.join(BASE_DIR, "access_token.txt")

IST = pytz.timezone("Asia/Kolkata")

SYMBOLS = [
    "NIFTY","BANKNIFTY","RELIANCE","INFY","HCLTECH","TVSMOTOR","BHARATFORG",
    "JUBLFOOD","LAURUSLABS","SUNPHARMA","TATACONSUM","COFORGE","ASIANPAINT",
    "MUTHOOTFIN","CHOLAFIN","BSE","GRASIM","ACC","ADANIENT","BHARTIARTL",
    "BIOCON","BRITANNIA","DIVISLAB","ESCORTS","JSWSTEEL","M&M","PAGEIND",
    "SHREECEM","BOSCHLTD","DIXON","MARUTI","ULTRACEMCO","APOLLOHOSP","MCX",
    "POLYCAB","PERSISTENT","TRENT","EICHERMOT","HAL","TIINDIA","SIEMENS",
    "GAIL","NATIONALUM","TATASTEEL","MOTHERSON","SHRIRAMFIN","VEDL","VBL",
    "GRANULES","LICHSGFIN","UPL","ANGELONE","INDHOTEL","APLAPOLLO","CAMS",
    "CUMMINSIND","MAXHEALTH","POLICYBZR","HAVELLS","GLENMARK","ADANIPORTS",
    "SRF","CDSL","TITAN","SBILIFE","COLPAL","HDFCLIFE","VOLTAS","NAUKRI",
    "TATACHEM","KALYANKJIL"
]

# =========================
# KITE LOGIN
# =========================
with open(ACCESS_TOKEN_FILE) as f:
    ACCESS_TOKEN = f.read().strip()
with open(API_KEY_FILE) as f:
    API_KEY = f.read().strip()

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# =========================
# LOAD / CACHE INSTRUMENTS
# =========================
def load_instruments():
    if os.path.exists(INST_FILE):
        return pd.read_csv(INST_FILE)

    print("📥 Downloading NSE instruments (one time)")
    df = pd.DataFrame(kite.instruments("NSE"))
    df.to_csv(INST_FILE, index=False)
    return df

inst = load_instruments()

def token(symbol):
    row = inst[inst.tradingsymbol == symbol]
    if row.empty:
        return None
    return int(row.iloc[0].instrument_token)

# =========================
# SAFE HISTORICAL BUILDER
# =========================
def build_static_ohlc():
    if os.path.exists(DAILY_FILE):
        print("✅ DAILY OHLC cache exists, skipping")
        return

    rows = []
    today = date.today()
    yesterday = today - timedelta(days=7)  # buffer for holidays

    print("📦 Building DAILY / WEEKLY / MONTHLY OHLC (one-time today)")

    for sym in SYMBOLS:
        tk = token(sym)
        if not tk:
            continue

        try:
            bars = kite.historical_data(
                tk,
                yesterday,
                today,
                "day"
            )
            if not bars:
                continue

            df = pd.DataFrame(bars)
            df["date"] = pd.to_datetime(df["date"]).dt.date

            yday = df.iloc[-2]
            rows.append({
                "Symbol": sym,
                "Y_Close": yday["close"],
                "Y_High": yday["high"],
                "Y_Low": yday["low"]
            })

            time.sleep(0.35)  # CRITICAL to avoid block

        except Exception as e:
            print(f"⚠ {sym} skipped: {e}")
            time.sleep(1)

    pd.DataFrame(rows).to_excel(DAILY_FILE, index=False)
    print("✅ Static OHLC cache saved")

# =========================
# LIVE DATA FETCH
# =========================
def fetch_live():
    instruments = [f"NSE:{s}" for s in SYMBOLS]
    quotes = kite.quote(instruments)

    rows = []
    for sym in SYMBOLS:
        q = quotes.get(f"NSE:{sym}")
        if not q:
            continue

        rows.append({
            "Symbol": sym,
            "LTP": q["last_price"],
            "Today_High": q["ohlc"]["high"],
            "Today_Low": q["ohlc"]["low"],
            "Timestamp": datetime.now(IST).replace(tzinfo=None)
        })

    return pd.DataFrame(rows)

# =========================
# MAIN FLOW
# =========================
if __name__ == "__main__":
    build_static_ohlc()

    live_df = fetch_live()
    daily_df = pd.read_excel(DAILY_FILE)

    final = live_df.merge(daily_df, on="Symbol", how="left")

    final["YH_Broken"] = final["LTP"] >= final["Y_High"]
    final["YL_Broken"] = final["LTP"] <= final["Y_Low"]

    print("\n🚀 YESTERDAY HIGH BROKEN")
    print(final[final["YH_Broken"]][["Symbol","LTP","Y_High"]])

    print("\n🔻 YESTERDAY LOW BROKEN")
    print(final[final["YL_Broken"]][["Symbol","LTP","Y_Low"]])

    print("\n✅ Script completed safely")
