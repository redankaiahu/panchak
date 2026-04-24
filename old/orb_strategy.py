
import time
from datetime import datetime, time as dt_time, timedelta
from alice_blue import AliceBlue, TransactionType, OrderType, ProductType
import pandas as pd

# Alice Blue credentials
user_id = 'your_user_id'
api_key = 'your_api_key'
alice = AliceBlue(user_id=user_id, api_key=api_key)
session_id = alice.get_session_id()

# Setup variables
opening_range = {}
placed_orders = set()
fno_symbols = ['RELIANCE', 'INFY', 'TCS', 'HDFCBANK', 'ICICIBANK']  # Use dynamic F&O fetch if needed

# Fetch average volume
def get_avg_volume(symbol):
    instrument = alice.get_instrument_by_symbol('NSE', symbol)
    df = alice.get_historical(instrument=instrument,
                               from_datetime=datetime.now() - timedelta(days=5),
                               to_datetime=datetime.now(),
                               interval='Day',
                               indices=False)
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df['volume'].mean()
    return 0

# Update opening range for each symbol
def update_opening_range(symbol, high, low):
    if symbol not in opening_range:
        opening_range[symbol] = {'high': high, 'low': low}
    else:
        opening_range[symbol]['high'] = max(opening_range[symbol]['high'], high)
        opening_range[symbol]['low'] = min(opening_range[symbol]['low'], low)

# Place order
def place_orb_order(symbol, ltp, direction):
    if symbol in placed_orders:
        return
    instrument = alice.get_instrument_by_symbol('NSE', symbol)
    transaction_type = TransactionType.Buy if direction == "BUY" else TransactionType.Sell
    print(f"Placing {direction} order for {symbol} at {ltp}")
    response = alice.place_order(transaction_type=transaction_type,
                                 instrument=instrument,
                                 quantity=1,
                                 order_type=OrderType.Market,
                                 product_type=ProductType.Intraday,
                                 price=0.0,
                                 trigger_price=None,
                                 stop_loss=None,
                                 square_off=None,
                                 trailing_sl=None,
                                 is_amo=False,
                                 order_tag='ORB')
    placed_orders.add(symbol)
    print("Order placed:", response)

# Main loop
avg_volumes = {symbol: get_avg_volume(symbol) for symbol in fno_symbols}
start_time = dt_time(9, 15)
end_time = dt_time(9, 30)
check_time = dt_time(9, 31)

while True:
    now = datetime.now().time()

    for symbol in fno_symbols:
        instrument = alice.get_instrument_by_symbol('NSE', symbol)
        ltp_data = alice.get_ltp(instrument)[instrument]
        ltp = ltp_data['ltp']
        high = ltp_data['high_price']
        low = ltp_data['low_price']
        volume = ltp_data['volume_traded_today']

        if start_time <= now <= end_time:
            update_opening_range(symbol, high, low)
        elif now >= check_time:
            if symbol not in opening_range or symbol in placed_orders:
                continue
            orb = opening_range[symbol]
            if ltp > orb['high'] and volume > 1.5 * avg_volumes[symbol]:
                place_orb_order(symbol, ltp, "BUY")
            elif ltp < orb['low'] and volume > 1.5 * avg_volumes[symbol]:
                place_orb_order(symbol, ltp, "SELL")

    time.sleep(10)
