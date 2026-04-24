from kiteconnect import KiteConnect
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

# ===============================
# ZERODHA CREDENTIALS
# ===============================
API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN = "BI70C5DhZwgADwJ6A1YzPpRR2S0qob3a"

# ===============================
# PATH SETUP
# ===============================
BASE_DIR = Path("ZERODHA_OPTION_DATA")
TODAY = datetime.now().strftime("%Y-%m-%d")
TODAY_DIR = BASE_DIR / TODAY
TODAY_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# CONFIG
# ===============================
SYMBOLS = [
    "RELIANCE","INFY","TCS","HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK",
    "LT","ITC","HINDUNILVR","BHARTIARTL","ASIANPAINT","MARUTI","SUNPHARMA","TITAN",
    "ULTRACEMCO","ONGC","POWERGRID","NTPC","COALINDIA","TATAMOTORS","TATASTEEL",
    "JSWSTEEL","HINDALCO","ADANIPORTS","ADANIENT","BAJFINANCE","BAJAJFINSV",
    "HDFCLIFE","SBILIFE","ICICIPRULI","DIVISLAB","DRREDDY","CIPLA","APOLLOHOSP",
    "GRASIM","UPL","BPCL","IOC","GAIL","PNB","BANKBARODA","CANBK","IDFCFIRSTB",
    "FEDERALBNK","INDUSINDBK","AUBANK","M&M","HEROMOTOCO","EICHERMOT","TVSMOTOR",
    "BAJAJ-AUTO","ASHOKLEY","AMBUJACEM","ACC","DALBHARAT","RAMCOCEM","SHREECEM",
    "DLF","GODREJPROP","OBEROIRLTY","PRESTIGE","LODHA","TRENT","DMART","NAUKRI",
    "TECHM","WIPRO","LTIM","MPHASIS","COFORGE","PERSISTENT","HCLTECH","OFSS",
    "BEL","HAL","BHEL","IRCTC","IRFC","RVNL","NBCC","RECLTD","PFC","SIEMENS",
    "ABB","CGPOWER","CUMMINSIND","SRF","PIDILITIND","BERGEPAINT","BRITANNIA",
    "COLPAL","DABUR","MARICO","MCDOWELL-N","UBL","INDIGO","SPICEJET","MFSL",
    "CHOLAFIN","MUTHOOTFIN","MANAPPURAM","LTF","ABCAPITAL","TATAPOWER",
    "TORNTPHARM","ALKEM","AUROPHARMA","BIOCON","LUPIN","ZYDUSLIFE","ICICIGI",
    "SBICARD","HDFCAMC","LICHSGFIN","BANDHANBNK","RBLBANK","YESBANK","SAIL",
    "NMDC","VEDL","HAVELLS","DIXON","VOLTAS","CROMPTON","INDHOTEL","CONCOR",
    "CANFINHOME","SUNTV","ZEEL","IDEA","PAYTM","NYKAA","ZOMATO","DELHIVERY"
]

EXCHANGE_SPOT = "NSE"
EXCHANGE_OPT = "NFO"
STRIKE_RANGE = [-2, -1, 0, 1, 2]
INTERVAL = 300  # 5 minutes

# ===============================
# CONNECT
# ===============================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)
print("✅ Zerodha connected")

# ===============================
# LOAD INSTRUMENTS ONCE
# ===============================
print("📥 Loading NFO instruments...")
inst_df = pd.DataFrame(kite.instruments(EXCHANGE_OPT))

# ===============================
# HELPERS
# ===============================
def get_nearest_expiry(df):
    expiries = sorted(df["expiry"].dropna().unique())
    return expiries[0] if expiries else None

def detect_strike_gap(strikes):
    strikes = sorted(strikes)
    diffs = [j - i for i, j in zip(strikes[:-1], strikes[1:])]
    return min(diffs) if diffs else 10

# ===============================
# MAIN LOOP
# ===============================
print("🚀 Multi-Stock Excel Collector STARTED")

try:
    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for SYMBOL in SYMBOLS:
            try:
                # ---------------- SPOT ----------------
                spot_data = kite.ltp(f"{EXCHANGE_SPOT}:{SYMBOL}")
                spot = spot_data[f"{EXCHANGE_SPOT}:{SYMBOL}"]["last_price"]

                # ---------------- OPTIONS FILTER ----------------
                opt_df = inst_df[
                    (inst_df["name"] == SYMBOL) &
                    (inst_df["segment"] == "NFO-OPT")
                ]

                nearest_expiry = get_nearest_expiry(opt_df)
                if not nearest_expiry:
                    raise Exception("No expiry found")

                opt_df = opt_df[opt_df["expiry"] == nearest_expiry]

                STRIKE_GAP = detect_strike_gap(opt_df["strike"].unique())
                atm = int(round(spot / STRIKE_GAP) * STRIKE_GAP)

                strikes = [atm + i * STRIKE_GAP for i in STRIKE_RANGE]
                opt_df = opt_df[opt_df["strike"].isin(strikes)]

                if opt_df.empty:
                    raise Exception("No ATM strikes")

                # ---------------- QUOTES ----------------
                token_map = {
                    f"{EXCHANGE_OPT}:{r['tradingsymbol']}": r
                    for _, r in opt_df.iterrows()
                }

                quotes = kite.quote(list(token_map.keys()))

                rows = []
                for tsym, q in quotes.items():
                    if q.get("oi") is None or q.get("volume") is None:
                        continue

                    r = token_map[tsym]

                    rows.append({
                        "Time": now,
                        "Symbol": SYMBOL,
                        "Spot": round(spot, 2),
                        "ATM": atm,
                        "Expiry": nearest_expiry,
                        "Strike": r["strike"],
                        "Type": r["instrument_type"],
                        "LTP": q["last_price"],
                        "OI": q["oi"],
                        "Volume": q["volume"],
                        "Bid": q["depth"]["buy"][0]["price"] if q["depth"]["buy"] else None,
                        "Ask": q["depth"]["sell"][0]["price"] if q["depth"]["sell"] else None
                    })

                if len(rows) < 6:
                    raise Exception("Illiquid / banned")

                df_new = pd.DataFrame(rows)

                # ---------------- SAVE EXCEL ----------------
                file_path = TODAY_DIR / f"{SYMBOL}.xlsx"

                if file_path.exists():
                    df_old = pd.read_excel(file_path)
                    df_old = df_old[df_old["Time"].str.startswith(TODAY)]
                    df_all = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df_all = df_new

                df_all.to_excel(file_path, index=False)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] {SYMBOL} | Spot {spot} | ATM {atm}")

                time.sleep(0.2)  # API safety

            except Exception as e:
                print(f"⚠ {SYMBOL} skipped:", e)

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("🛑 Collector stopped safely")
