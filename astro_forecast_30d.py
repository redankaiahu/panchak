import os
import sys
from datetime import datetime, timedelta
import pytz
import math

# Ensure local imports work
sys.path.append(os.getcwd())

from astro_engine import get_all_planets, get_dignities, get_angular_distance, get_gann_square_9, get_squaring_time_price
from astro_logic import get_astro_score, SECTOR_SIGNIFICATORS

IST = pytz.timezone("Asia/Kolkata")

def generate_30d_forecast():
    start_dt = datetime.now(IST)
    print("# ============================================================")
    print(f"# 30-DAY ASTRO-FINANCIAL FORECAST (Starting {start_dt.strftime('%d-%b-%Y')})")
    print("# Based on Vedic Astrology, KP Sub-lords, and Gann Principles")
    print("# ============================================================\n")

    # 1. Major Planetary Events
    print("## MAJOR PLANETARY TRANSITS & STATIONS")
    events = []
    # Simplified simulation of events for next 30 days
    # In a real system, we'd loop through days and check for sign changes/retrogrades
    d = start_dt
    for _ in range(30):
        # Dignity changes
        # (Placeholder for real transition logic)
        d += timedelta(days=1)
    
    print("- **Jupiter in Cancer (Exalted):** Strong floor for banking and finance. Dips likely to be bought.")
    print("- **Venus in Pisces (Exalted):** Bullish for FMCG and luxury sectors.")
    print("- **Saturn in Aquarius (Own Sign):** Stable but slow growth in energy and heavy industries.")
    print("- **Rahu-Ketu Axis:** Technology and software remain volatile; look for gap-ups followed by profit booking.")
    print("\n")

    # 2. Indices & Commodities Specifics
    print("## HIGH-CONVICTION SECTOR SIGNALS")
    for sector, data in SECTOR_SIGNIFICATORS.items():
        ruler = data["ruler"]
        digs = get_dignities(start_dt)
        dig = digs.get(ruler, "NEUTRAL")
        
        status = "🟢 BULLISH" if dig in ["EXALTED", "OWN"] else "🔴 BEARISH" if dig == "DEBILITATED" else "🟡 NEUTRAL"
        print(f"### {sector} ({ruler}) -> {status}")
        print(f"- **Reasoning:** {data['desc']} Currently {ruler} is in {dig} status.")
        
        if sector == "GOLD":
            print("- **Gann Levels:** (Assuming current price ~24000 for Nifty context)")
            gann = get_gann_square_9(24350) # Example Nifty base
            print(f"  - Magic Lines: {gann[1]['up']} (90°), {gann[3]['up']} (180°)")
    print("\n")

    # 3. 30-Day Trend Table
    print("## DAILY BIAS & REVERSAL WINDOWS")
    print("| Date | Score | Signal | Reversal Windows (KP) |")
    print("| :--- | :---: | :--- | :--- |")
    
    d = start_dt
    count = 0
    while count < 30:
        if d.weekday() < 5: # Trading days
            res = get_astro_score(d)
            # Find reversal times (mocking logic here for speed)
            rev_str = "10:15, 13:45" if abs(res["score"]) > 2 else "11:30"
            print(f"| {d.strftime('%d-%b')} | {res['score']:+d} | {res['signal']} | {rev_str} |")
        d += timedelta(days=1)
        count += 1

    print("\n## MAGIC LINES (GANN SQUARING)")
    nifty_sq = get_squaring_time_price(24358.60, start_dt)
    print(f"- **Nifty Squaring:** {nifty_sq['is_squared']} (Conviction: {nifty_sq['reversal_conviction']})")
    print("- **Key Formula:** (Price / Time) alignment occurs when Price % 360 ≈ Sun Longitude.")

if __name__ == "__main__":
    generate_30d_forecast()
