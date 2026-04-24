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

# ===============================
# CONFIG
# ===============================
SYMBOL = "RELIANCE"
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
nearest_expiry = sorted(opt_df["expiry"].unique())[0]
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
print("🚀 RELIANCE Excel Collector Started")

while True:
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ---------------------------
        # SPOT
        # ---------------------------
        spot = get_spot_price()
        atm = round_to_strike(spot)

        # ---------------------------
        # OPTIONS
        # ---------------------------
        opt_syms = get_option_symbols(atm)

        tokens = []
        symbol_map = {}

        for side in ["CE", "PE"]:
            for _, row in opt_syms[side].iterrows():
                tsym = f"{EXCHANGE_OPT}:{row['tradingsymbol']}"
                tokens.append(tsym)
                symbol_map[tsym] = row

        quotes = kite.quote(tokens)

        # ---------------------------
        # BUILD ROWS
        # ---------------------------
        rows = []

        for tsym, q in quotes.items():
            row = symbol_map[tsym]

            rows.append({
                "Time": now,
                "Symbol": SYMBOL,
                "Spot": spot,
                "Expiry": nearest_expiry,
                "Strike": row["strike"],
                "Type": row["instrument_type"],

                "LTP": q["last_price"],
                "OI": q.get("oi"),
                "Volume": q.get("volume"),

                "Bid": q["depth"]["buy"][0]["price"] if q["depth"]["buy"] else None,
                "Ask": q["depth"]["sell"][0]["price"] if q["depth"]["sell"] else None
            })

        df_new = pd.DataFrame(rows)

        # ---------------------------
        # SAVE TO EXCEL (APPEND)
        # ---------------------------
        if Path(EXCEL_FILE).exists():
            df_old = pd.read_excel(EXCEL_FILE)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_all = df_new

        df_all.to_excel(EXCEL_FILE, index=False)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Spot {spot} | ATM {atm} | Rows {len(df_new)} saved")

    except Exception as e:
        print("❌ Error:", e)

    time.sleep(INTERVAL)
