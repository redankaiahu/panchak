import os
import pandas as pd
from datetime import datetime, timedelta, time
from kiteconnect import KiteConnect


OUTPUT_DIR = os.getcwd()

symbols = [
    'NIFTY',
    'BANKNIFTY',
    'RELIANCE',
    'INFY',
    'HCLTECH',
    'TVSMOTOR',
    'BHARATFORG',
    'JUBLFOOD',
    'LAURUSLABS',
    'SUNPHARMA',
    'TATACONSUM',
    'COFORGE',
    'ASIANPAINT',
    'MUTHOOTFIN',
    'CHOLAFIN',
    'BSE',
    'GRASIM',
    'ACC',
    'ADANIENT',
    'BHARTIARTL',
    'BIOCON',
    'BRITANNIA',
    'DIVISLAB',
    'ESCORTS',
    'JSWSTEEL',
    'M&M',
    'PAGEIND',         # PAGE Industries
    'SHREECEM',        # Shree Cement
    'BOSCHLTD',        # BOSCH
    'DIXON',           # Dixon Technologies
    'MARUTI',          # Maruti Suzuki
    'ULTRACEMCO',      # UltraTech Cement
    'APOLLOHOSP',      # Apollo Hospitals
    'MCX',             # Multi Commodity Exchange
    'POLYCAB',
    'PERSISTENT',
    'TRENT',
    'EICHERMOT',       # Eicher Motors
    'HAL',             # Hindustan Aeronautics
    'TIINDIA',         # Tube Investments
    'SIEMENS',
    'GAIL',
    'NATIONALUM',
    'TATASTEEL',
    'MOTHERSON',
    'SHRIRAMFIN',
    'VEDL',
    'VBL',
    'GRANULES',
    'LICHSGFIN',
    'UPL',
    'ANGELONE',
    'INDHOTEL',
    'APLAPOLLO',
    'CAMS',
    'CUMMINSIND',
    'MAXHEALTH',
    'POLICYBZR',
    'HAVELLS',
    'GLENMARK',
    'ADANIPORTS',
    'SRF',
    'CDSL',
    'TITAN',
    'SBILIFE',
    'COLPAL',
    'HDFCLIFE',
    'VOLTAS',
    'NAUKRI',
    'TATACHEM',
    'KALYANKJIL'
]

specific_dates = ['21-01-2026', '22-01-2026', '23-01-2026', '24-01-2026', '25-01-2026']
specific_dates = [datetime.strptime(d, "%d-%m-%Y").date() for d in specific_dates]

# ===============================
# KITE LOGIN
# ===============================
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
# LOAD INSTRUMENTS
# ===============================
instruments = pd.DataFrame(kite.instruments("NSE"))

def get_token(symbol):
    if symbol == "NIFTY":
        return instruments[(instruments.tradingsymbol == "NIFTY 50") &
                           (instruments.segment == "INDICES")].iloc[0].instrument_token
    if symbol == "BANKNIFTY":
        return instruments[(instruments.tradingsymbol == "NIFTY BANK") &
                           (instruments.segment == "INDICES")].iloc[0].instrument_token

    return instruments[(instruments.tradingsymbol == symbol) &
                       (instruments.exchange == "NSE")].iloc[0].instrument_token

# ===============================
# FETCH OHLC
# ===============================
def fetch_ohlc(symbol, from_date, to_date):
    try:
        token = get_token(symbol)
        data = kite.historical_data(
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
            interval="day"
        )
        df = pd.DataFrame(data)
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    except Exception as e:
        print(f"⚠ {symbol} error: {e}")
        return pd.DataFrame()

# ===============================
# MAIN LOGIC
# ===============================
final_df = pd.DataFrame()

for symbol in symbols:
    df = fetch_ohlc(symbol, min(specific_dates), max(specific_dates))
    if df.empty:
        continue
    df = df[df["date"].isin(specific_dates)]
    final_df = pd.concat([final_df, df], ignore_index=True)

if final_df.empty:
    print("❌ No OHLC data fetched")
    exit()

blocks = []

for symbol in final_df.symbol.unique():
    sdf = final_df[final_df.symbol == symbol]

    rows = []
    for d in specific_dates:
        r = sdf[sdf.date == d]
        if not r.empty:
            rows.append(r[["symbol", "date", "close", "open", "high", "low"]].iloc[0])
        else:
            rows.append(pd.Series({
                "symbol": symbol,
                "date": d,
                "close": "",
                "open": "",
                "high": "",
                "low": ""
            }))

    block = pd.DataFrame(rows)

    valid = block[block.high != ""]
    if not valid.empty:
        top_high = valid.high.astype(float).max()
        top_low = valid.low.astype(float).min()
        diff = top_high - top_low
        bt = top_high + diff
        st = top_low - diff
    else:
        top_high = top_low = diff = bt = st = ""

    for c in ["TOP HIGH", "TOP LOW", "DIFF", "BT", "ST"]:
        block[c] = ""

    block.loc[block.index[0], "TOP HIGH"] = round(top_high, 2)
    block.loc[block.index[0], "TOP LOW"] = round(top_low, 2)
    block.loc[block.index[0], "DIFF"] = round(diff, 2)
    block.loc[block.index[0], "BT"] = round(bt, 2)
    block.loc[block.index[0], "ST"] = round(st, 2)

    blocks.append(block)
    blocks.append(pd.DataFrame([[""] * len(block.columns)], columns=block.columns))

final_output = pd.concat(blocks, ignore_index=True)

# ===============================
# SAVE EXCEL
# ===============================
fname = f"OHLC_Data_{datetime.now().strftime('%d%m%Y_%H%M%S')}.xlsx"
path = os.path.join(OUTPUT_DIR, fname)

with pd.ExcelWriter(path, engine="openpyxl") as writer:
    final_output.to_excel(writer, index=False, sheet_name="OHLC")

print(f"✅ OHLC data saved: {path}")
