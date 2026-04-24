from kiteconnect import KiteConnect
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path

def load_access_token():
    with open(r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24\access_token.txt") as f:
        return f.read().strip()

# ===============================
# ZERODHA CREDENTIALS
# ===============================
API_KEY = "7am67kxijfsusk9i"
#ACCESS_TOKEN = "BI70C5DhZwgADwJ6A1YzPpRR2S0qob3a"

# ===============================
# CONNECT
# ===============================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(load_access_token())
print("✅ Zerodha connected")


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
# LOAD INSTRUMENTS ONCE
# ===============================
print("📥 Loading instruments...")
inst_nse = pd.DataFrame(kite.instruments("NSE"))
inst_nfo = pd.DataFrame(kite.instruments("NFO"))

# ===============================
# HELPERS
# ===============================
def get_prev_day_ohlc(token):
    to_d = datetime.now().date()
    from_d = to_d - timedelta(days=7)
    data = kite.historical_data(token, from_d, to_d, "day")
    if len(data) < 2:
        return None
    y = data[-2]
    return {
    "Y_Open": y["open"],
    "Y_High": y["high"],
    "Y_Low": y["low"],
    "Y_Close": y["close"]
    }


def get_today_ohlc(token):
    today = datetime.now().date()
    data = kite.historical_data(token, today, today, "5minute")
    if not data:
        return None
    return {
    "D_Open": data[0]["open"],
    "D_High": max(x["high"] for x in data),
    "D_Low": min(x["low"] for x in data)
    }


def nearest_expiry(df):
    exps = sorted(df["expiry"].dropna().unique())
    return exps[0] if exps else None

def strike_gap(strikes):
    strikes = sorted(strikes)
    return min(j - i for i, j in zip(strikes[:-1], strikes[1:]))

# ===============================
# MAIN LOOP
# ===============================
print("🚀 Multi-Stock Excel Collector STARTED")

while True:
    now = datetime.now()

    for SYMBOL in SYMBOLS:
        try:
            # -------- SPOT TOKEN --------
            inst_row = inst_nse[inst_nse["tradingsymbol"] == SYMBOL].iloc[0]
            spot_token = inst_row["instrument_token"]

            ltp = kite.ltp(f"{EXCHANGE_SPOT}:{SYMBOL}")
            spot = ltp[f"{EXCHANGE_SPOT}:{SYMBOL}"]["last_price"]

            prev_ohlc = get_prev_day_ohlc(spot_token)
            today_ohlc = get_today_ohlc(spot_token)

            if not prev_ohlc or not today_ohlc:
                continue

            # -------- OPTIONS --------
            opt_df = inst_nfo[
                (inst_nfo["name"] == SYMBOL) &
                (inst_nfo["segment"] == "NFO-OPT")
            ]

            exp = nearest_expiry(opt_df)
            opt_df = opt_df[opt_df["expiry"] == exp]

            gap = strike_gap(opt_df["strike"].unique())
            atm = round(spot / gap) * gap
            strikes = [atm + i * gap for i in STRIKE_RANGE]
            opt_df = opt_df[opt_df["strike"].isin(strikes)]

            symbols = [f"NFO:{x}" for x in opt_df["tradingsymbol"]]
            quotes = kite.quote(symbols)

            rows = []
            for _, r in opt_df.iterrows():
                q = quotes.get(f"NFO:{r['tradingsymbol']}")
                if not q or q["oi"] is None:
                    continue

                rows.append({
                    "Timestamp": now,
                    "Symbol": SYMBOL,
                    "Spot": spot,
                    "LTP": spot,
                    **prev_ohlc,
                    **today_ohlc,
                    "ATM": atm,
                    "Expiry": exp,
                    "Strike": r["strike"],
                    "Type": r["instrument_type"],
                    "Option LTP": q["last_price"],
                    "OI": q["oi"],
                    "Volume": q["volume"]
                })

            if len(rows) < 6:
                continue

            df_new = pd.DataFrame(rows)

            file = TODAY_DIR / f"{SYMBOL}.xlsx"
            if file.exists():
                df_old = pd.read_excel(file)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_all = df_new

            df_all.to_excel(file, index=False)

            print(
                f"[{now.strftime('%H:%M:%S')}] {SYMBOL} | "
                f"Spot {spot} | ATM {atm} | "
                f"YH {prev_ohlc['Y_High']} | YL {prev_ohlc['Y_Low']} | "
                f"TH {today_ohlc['D_High']} | TL {today_ohlc['D_Low']}"
            )

            time.sleep(0.15)

        except Exception as e:
            print(f"⚠ {SYMBOL} skipped:", e)

    time.sleep(INTERVAL)
