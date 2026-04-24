import swisseph as swe
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os
from sys import argv

# Constants
LOCATION = {'lat': 18.9388, 'lon': 72.8354}  # Mumbai
TIMEZONE = pytz.timezone("Asia/Kolkata")
# Usage: python KP_Intraday_Generator.py 2025-06-01
if len(argv) > 1:
    DATE = TIMEZONE.localize(datetime.strptime(argv[1], "%Y-%m-%d"))
else:
    DATE = datetime.now(TIMEZONE)

#DATE = datetime.now(TIMEZONE)  #  DATE = TIMEZONE.localize(datetime(2025, 6, 1))
#============> To run this : python KP_Intraday_Generator.py 2025-06-01


START_TIME = TIMEZONE.localize(datetime(DATE.year, DATE.month, DATE.day, 9, 15))
END_TIME = TIMEZONE.localize(datetime(DATE.year, DATE.month, DATE.day, 15, 30))
INTERVAL_MINUTES = 5

# Set ephemeris path
swe.set_ephe_path(".")

# Nakshatra and Dasha logic
nakshatra_lords = ['Ketu', 'Venus', 'Sun', 'Moon', 'Mars', 'Rahu', 'Jupiter', 'Saturn', 'Mercury']
nakshatra_durations = [7, 20, 6, 10, 7, 18, 16, 19, 17]
dasha_lord_cycle = nakshatra_lords * 3  # Repeat for 27 nakshatras

def get_kp_sublords(degree):
    deg = degree % 360
    nak_num = int(deg // (13 + 1/3))
    balance = deg % (13 + 1/3)
    star_lord = dasha_lord_cycle[nak_num]
    dasha_span = nakshatra_durations[nakshatra_lords.index(star_lord)]
    progress = balance / (13 + 1/3)
    sub_index = int(progress * dasha_span) % 9
    sub_lord = nakshatra_lords[sub_index]
    sub_span = nakshatra_durations[sub_index]
    sub_progress = (progress * dasha_span) % 1
    sub_sub_index = int(sub_progress * sub_span) % 9
    sub_sub_lord = nakshatra_lords[sub_sub_index]
    return star_lord, sub_lord, sub_sub_lord

def get_ascendant(jd, lat, lon):
    cusps, ascmc = swe.houses_ex(jd, lat, lon, b'A')
    return ascmc[0]

def evaluate_view(planet):
    pos = ['Jupiter', 'Venus', 'Sun']
    neg = ['Saturn', 'Mars', 'Ketu']
    trap = ['Rahu', 'Mercury', 'Moon']
    if planet in pos:
        return "Positive"
    elif planet in neg:
        return "Negative"
    elif planet in trap:
        return "Trap"
    return "Neutral"

def derive_final_view(views):
    if views.count("Strong Positive") >= 2 or views.count("Positive") >= 3:
        return "Strong Positive"
    elif views.count("Trending-Positive") >= 2:
        return "Trending-Positive"
    elif views.count("Negative") >= 3:
        return "Strong Negative"
    elif "Trap" in views:
        return "Trap"
    elif views.count("Positive") == 2 and views.count("Negative") == 2:
        return "Neutral"
    return "Trending"

# Time loop
rows = []
t = START_TIME
while t <= END_TIME:
    jd = swe.julday(t.year, t.month, t.day, t.hour + t.minute/60 + t.second/3600)
    moon_pos = swe.calc_ut(jd, swe.MOON)[0][0]
    cusps, ascmc = swe.houses_ex(jd, LOCATION['lat'], LOCATION['lon'], b'A')
    asc = ascmc[0]

    # KP sub-lord levels
    moon_star, moon_sub, moon_subsub = get_kp_sublords(moon_pos)
    fifth_pos = (asc + 120) % 360
    fifth_star, fifth_sub, fifth_subsub = get_kp_sublords(fifth_pos)
    eleventh_pos = (asc + 240) % 360
    eleventh_star, eleventh_sub, eleventh_subsub = get_kp_sublords(eleventh_pos)

    # Views
    view_moon = evaluate_view(moon_sub)
    view_5th = "Trap" if evaluate_view(fifth_sub) == "Positive" else evaluate_view(fifth_sub)
    view_5th_star = evaluate_view(fifth_star)
    view_11th_star = evaluate_view(eleventh_star)
    final_view = derive_final_view([view_moon, view_5th, view_5th_star, view_11th_star])

    rows.append({
        "Date Time": t.strftime("%Y-%m-%d %H:%M"),
        "Moon Sub-Lord": moon_sub,
        "Moon Sub-Sub": moon_subsub,
        "5th Sub-Lord": fifth_sub,
        "5th Sub-Sub": fifth_subsub,
        "11th Sub-Lord": eleventh_sub,
        "11th Sub-Sub": eleventh_subsub,
        "Asc Degree": round(asc, 2),
        "Moon View": view_moon,
        "5th View": view_5th,
        "5th Star View": view_5th_star,
        "11th Star View": view_11th_star,
        "Final View": final_view
    })

    t += timedelta(minutes=5)

# Save to Excel
df = pd.DataFrame(rows)
os.makedirs("output", exist_ok=True)
filename = f"output/KP_Intraday_{DATE.strftime('%d%b%Y')}.xlsx"
df.to_excel(filename, index=False)
print(f"Saved to: {filename}")
