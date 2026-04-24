
import swisseph as swe
import datetime
import pandas as pd

nakshatras = [
    ('Ashwini', 'Ketu'), ('Bharani', 'Venus'), ('Krittika', 'Sun'), ('Rohini', 'Moon'),
    ('Mrigashira', 'Mars'), ('Ardra', 'Rahu'), ('Punarvasu', 'Jupiter'), ('Pushya', 'Saturn'),
    ('Ashlesha', 'Mercury'), ('Magha', 'Ketu'), ('Purva Phalguni', 'Venus'), ('Uttara Phalguni', 'Sun'),
    ('Hasta', 'Moon'), ('Chitra', 'Mars'), ('Swati', 'Rahu'), ('Vishakha', 'Jupiter'),
    ('Anuradha', 'Saturn'), ('Jyeshtha', 'Mercury'), ('Mula', 'Ketu'), ('Purva Ashadha', 'Venus'),
    ('Uttara Ashadha', 'Sun'), ('Shravana', 'Moon'), ('Dhanishta', 'Mars'), ('Shatabhisha', 'Rahu'),
    ('Purva Bhadrapada', 'Jupiter'), ('Uttara Bhadrapada', 'Saturn'), ('Revati', 'Mercury')
]

benefics = ['Jupiter', 'Venus', 'Moon', 'Mercury']
malefics = ['Mars', 'Saturn', 'Rahu', 'Ketu', 'Sun']
bearish_nakshatras = ['Ashlesha', 'Ardra', 'Jyeshtha', 'Mula']

def get_moon_data(date_obj):
    swe.set_sid_mode(swe.SIDM_LAHIRI)
    jd = swe.julday(date_obj.year, date_obj.month, date_obj.day, 9.25)
    moon_pos = swe.calc_ut(jd, swe.MOON)[0][0]
    nak_index = int((moon_pos % 360) / (360 / 27))
    nak_name, sub_lord = nakshatras[nak_index]
    return nak_name, sub_lord

def astro_score_day(date_obj):
    nak, sub = get_moon_data(date_obj)
    score = 0
    reasons = []
    if sub in benefics:
        score += 1
        reasons.append(f"Sub-lord {sub} is benefic (+1)")
    if sub in malefics:
        score -= 1
        reasons.append(f"Sub-lord {sub} is malefic (-1)")
    if nak not in bearish_nakshatras:
        score += 1
        reasons.append(f"Nakshatra {nak} is not bearish (+1)")
    else:
        score -= 1
        reasons.append(f"Nakshatra {nak} is bearish (-1)")

    if score >= 2:
        pred = 'UP'
    elif score <= -1:
        pred = 'DOWN'
    else:
        pred = 'SIDEWAYS'

    return {
        'Date': date_obj.strftime('%Y-%m-%d'),
        'Nakshatra': nak,
        'Sub-lord': sub,
        'Score': score,
        'Prediction': pred,
        'Reason': "; ".join(reasons)
    }

# Generate for March 1 to April 30
start = datetime.date(2025, 3, 1)
end = datetime.date(2025, 4, 30)
delta = datetime.timedelta(days=1)

results = []
while start <= end:
    if start.weekday() < 5:  # Monday–Friday only
        results.append(astro_score_day(start))
    start += delta

df = pd.DataFrame(results)
df.to_excel('astro_predictions_Mar_Apr.xlsx', index=False)
print("✅ Saved results to astro_predictions_Mar_Apr.xlsx")
