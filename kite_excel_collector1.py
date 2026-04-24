from kiteconnect import KiteConnect
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
import math

# ===============================
# ZERODHA CREDENTIALS
# ===============================
API_KEY = "7am67kxijfsusk9i"
ACCESS_TOKEN = "BI70C5DhZwgADwJ6A1YzPpRR2S0qob3a"

BASE_DIR = Path("ZERODHA_OPTION_DATA")
TODAY_DIR = BASE_DIR / datetime.now().strftime("%Y-%m-%d")
TODAY_DIR.mkdir(parents=True, exist_ok=True)


# ===============================
# CONFIG
# ===============================
#SYMBOL = "RELIANCE"
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

#for SYMBOL in SYMBOLS:
 #   collect_data(SYMBOL)

EXCHANGE_SPOT = "NSE"
EXCHANGE_OPT = "NFO"

STRIKE_GAP = 20          # RELIANCE strike gap
STRIKE_RANGE = [-2, -1, 0, 1, 2]  # ATM ±2
INTERVAL = 300           # 5 minutes

EXCEL_FILE = "ZERODHA_RELIANCE_OPTION_DATA.xlsx"

# ===============================
# CONNECT
# ===============================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

print("✅ Zerodha connected")

# ===============================
# LOAD INSTRUMENTS (ONCE)
# ===============================
print("📥 Loading instruments...")
instruments = kite.instruments(EXCHANGE_OPT)
inst_df = pd.DataFrame(instruments)

# Filter RELIANCE options only
opt_df = inst_df[
    (inst_df["name"] == SYMBOL) &
    (inst_df["segment"] == "NFO-OPT")
]

# Nearest expiry
#nearest_expiry = sorted(opt_df["expiry"].unique())[0]
nearest_expiry = get_nearest_expiry(opt_df)
if not nearest_expiry:
    raise Exception("No valid expiry")

opt_df = opt_df[opt_df["expiry"] == nearest_expiry]

print(f"📅 Using expiry: {nearest_expiry}")

# ===============================
# HELPERS
# ===============================
def round_to_strike(price):
    return int(round(price / STRIKE_GAP) * STRIKE_GAP)

def get_spot_price():
    ltp = kite.ltp(f"{EXCHANGE_SPOT}:{SYMBOL}")
    return ltp[f"{EXCHANGE_SPOT}:{SYMBOL}"]["last_price"]

def get_option_symbols(atm):
    strikes = [atm + i * STRIKE_GAP for i in STRIKE_RANGE]

    rows = opt_df[opt_df["strike"].isin(strikes)]
    return {
        "CE": rows[rows["instrument_type"] == "CE"],
        "PE": rows[rows["instrument_type"] == "PE"]
    }

# ===============================
# MAIN LOOP
# ===============================

def detect_strike_gap(strikes):
    strikes = sorted(strikes)
    diffs = [j - i for i, j in zip(strikes[:-1], strikes[1:])]
    return min(diffs) if diffs else 10


print("🚀 Multi-Stock Excel Collector Started")

while True:
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for SYMBOL in SYMBOLS:
            try:
                # ---------------------------
                # SPOT PRICE
                # ---------------------------
                spot = kite.ltp(f"NSE:{SYMBOL}")[f"NSE:{SYMBOL}"]["last_price"]

                #STRIKE_GAP = 20 if SYMBOL == "RELIANCE" else 10
                STRIKE_GAP = detect_strike_gap(opt_df["strike"].unique())

                atm = int(round(spot / STRIKE_GAP) * STRIKE_GAP)

                # ---------------------------
                # OPTIONS FILTER
                # ---------------------------
                opt_df = inst_df[
                    (inst_df["name"] == SYMBOL) &
                    (inst_df["segment"] == "NFO-OPT")
                ]

                nearest_expiry = sorted(opt_df["expiry"].unique())[0]
                opt_df = opt_df[opt_df["expiry"] == nearest_expiry]

                strikes = [atm + i * STRIKE_GAP for i in [-2, -1, 0, 1, 2]]
                opt_df = opt_df[opt_df["strike"].isin(strikes)]

                tokens = {}
                for _, r in opt_df.iterrows():
                    ts = f"NFO:{r['tradingsymbol']}"
                    tokens[ts] = r

                quotes = kite.quote(list(tokens.keys()))

                # ---------------------------
                # BUILD ROWS
                # ---------------------------
                rows = []
                for tsym, q in quotes.items():
                    r = tokens[tsym]

                    rows.append({
                        "Time": now,
                        "Symbol": SYMBOL,
                        "Spot": spot,
                        "Expiry": nearest_expiry,
                        "Strike": r["strike"],
                        "Type": r["instrument_type"],
                        "LTP": q["last_price"],
                        "OI": q.get("oi"),
                        "Volume": q.get("volume"),
                        "Bid": q["depth"]["buy"][0]["price"] if q["depth"]["buy"] else None,
                        "Ask": q["depth"]["sell"][0]["price"] if q["depth"]["sell"] else None
                    })

                df_new = pd.DataFrame(rows)

                # ---------------------------
                # SAVE PER STOCK EXCEL
                # ---------------------------
                file_path = TODAY_DIR / f"{SYMBOL}.xlsx"

                if file_path.exists():
                    df_old = pd.read_excel(file_path)
                    df_all = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df_all = df_new

                df_all.to_excel(file_path, index=False)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] {SYMBOL} | Spot {spot} | ATM {atm} | {len(rows)} rows")

            except Exception as e:
                print(f"⚠ {SYMBOL} skipped:", e)

    except Exception as e:
        print("❌ Cycle error:", e)

    time.sleep(300)   # 5 minutes

