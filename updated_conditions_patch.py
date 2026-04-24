
# ✅ Updated Buy Conditions with Volume, Trend, and Breakout Quality Filters

from datetime import datetime, time as datetime_time
from alice_blue import TransactionType

def is_quality_breakout(data):
    return (
        data["yesterday_close"] > data["yesterday_open"] and
        data["yesterday_vol"] > data["daybefore_vol"] and
        data["weekly_close"] > data["weekly_open"] and
        data["SMA_Volume_5"] > 0 and
        data["ema_20"] > data["ema_50"] > data["ema_200"]
    )

def evaluate_buy_condition_1(data, ltp, open_price, high_price, low_price, volume, symbol_name, instrument,
                              yesterday_open, yesterday_high, yesterday_low, yesterday_close, yesterday_vol,
                              daybefore_open, daybefore_high, daybefore_low, daybefore_close, daybefore_vol,
                              three_days_ago_open, three_days_ago_high, three_days_ago_low, three_days_ago_close, three_days_ago_vol,
                              four_days_ago_open, four_days_ago_high, four_days_ago_low, four_days_ago_close, four_days_ago_vol,
                              five_days_ago_close, Y_H_C_d, Y_H_L_d, Y_C_L_d, YHCon, YLCon, latest_max_5, latest_max_120,
                              SMA_Volume_5, ema_200, ema_50, ema_20, ema_13, conditionB1, conditionB2, conditionB3, conditionB4,
                              conditionI1, conditionI2, conditionSB, conditionS1, conditionS2, conditionS3, conditionS4,
                              YCCon, YH15, YH15Con, OL, weekly_open, weekly_high, weekly_low, weekly_close):
    try:
        if datetime.now().time() < datetime_time(9, 20):
            return False

        if volume < 1.5 * SMA_Volume_5 or volume < yesterday_vol:
            return False

        if ltp < weekly_high * 1.0025:
            return False

        if not is_quality_breakout(data):
            return False

        if (
            ltp > weekly_high and
            open_price == low_price and
            open_price > data["yesterday_close"] and
            open_price < data["yesterday_high"] and
            ltp > data["yesterday_high"] and
            ltp > open_price and
            high_price < YHCon and
            ltp < YHCon
        ):
            place_order_with_prevention(instrument, TransactionType.Buy, ltp, symbol_name,
                                        open_price, high_price, low_price, volume,
                                        yesterday_open, yesterday_high, yesterday_low, yesterday_close,
                                        conditionB1, yesterday_vol, weekly_open, weekly_high, weekly_low, weekly_close)
            return True
    except KeyError as e:
        print(f"Missing data for evaluate_buy_condition_1: {e}")
    return False
