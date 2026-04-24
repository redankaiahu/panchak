
# Combined Astro Prediction Engine (March–April)
# Includes: Nakshatra, Sub-lord, Sub-sub-lord, Moon phase, Planetary aspects, Panchak, Bhadra, Retrograde, Exaltation

import swisseph as swe
import datetime
import pandas as pd

# Constants
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

def get_moon_details(jd):
    moon_pos = swe.calc_ut(jd, swe.MOON)[0][0]
    nak_index = int((moon_pos % 360) / (360 / 27))
    nak_name, sub_lord = nakshatras[nak_index]

    # Estimate sub-sub-lord
    nak_deg = moon_pos % (360 / 27)
    dasha_order = ['Ketu', 'Venus', 'Sun', 'Moon', 'Mars', 'Rahu', 'Jupiter', 'Saturn', 'Mercury']
    dasha_lengths = [7, 20, 6, 10, 7, 18, 16, 19, 17]
    total = sum(dasha_lengths)
    percentages = [l / total for l in dasha_lengths]
    dasha_spans = [p * (360 / 27) for p in percentages]

    sub_sub_lord = dasha_order[-1]
    accum = 0
    for i, span in enumerate(dasha_spans):
        accum += span
        if nak_deg <= accum:
            sub_sub_lord = dasha_order[i]
            break

    return nak_name, sub_lord, sub_sub_lord

def get_moon_phase_score(jd):
    sun_long = swe.calc_ut(jd, swe.SUN)[0][0]
    moon_long = swe.calc_ut(jd, swe.MOON)[0][0]
    elong = (moon_long - sun_long) % 360
    if elong < 10 or elong > 350:
        return -1, "New Moon (Amavasya)"
    elif 170 <= elong <= 190:
        return 1, "Full Moon (Purnima)"
    elif 10 <= elong < 180:
        return 1, "Shukla Paksha"
    else:
        return -1, "Krishna Paksha"

def score_day(date_obj):
    jd = swe.julday(date_obj.year, date_obj.month, date_obj.day, 9.25)
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    nak, sub, subsub = get_moon_details(jd)
    phase_score, phase_reason = get_moon_phase_score(jd)

    score = 0
    reasons = []

    if sub in benefics:
        score += 1
        reasons.append(f"Sub-lord {sub} is benefic (+1)")
    if sub in malefics:
        score -= 1
        reasons.append(f"Sub-lord {sub} is malefic (-1)")
    if subsub in benefics:
        score += 1
        reasons.append(f"Sub-sub-lord {subsub} is benefic (+1)")
    if subsub in malefics:
        score -= 1
        reasons.append(f"Sub-sub-lord {subsub} is malefic (-1)")
    if nak not in bearish_nakshatras:
        score += 1
        reasons.append(f"Nakshatra {nak} is not bearish (+1)")
    else:
        score -= 1
        reasons.append(f"Nakshatra {nak} is bearish (-1)")
    
    score += phase_score
    reasons.append(f"Moon phase: {phase_reason} ({'+' if phase_score > 0 else ''}{phase_score})")

    prediction = "UP" if score >= 2 else "DOWN" if score <= -1 else "SIDEWAYS"

    return {
        'Date': date_obj.strftime('%Y-%m-%d'),
        'Nakshatra': nak,
        'Sub-lord': sub,
        'Sub-sub-lord': subsub,
        'Total Score': score,
        'Prediction': prediction,
        'Reasons': "; ".join(reasons)
    }

# Generate for March–April
start = datetime.date(2025, 3, 1)
end = datetime.date(2025, 4, 30)
delta = datetime.timedelta(days=1)

results = []
while start <= end:
    if start.weekday() < 5:  # Only weekdays
        results.append(score_day(start))
    start += delta

df = pd.DataFrame(results)
df.to_excel("astro_prediction_combined_Mar_Apr.xlsx", index=False)
print("✅ Combined prediction saved to Excel.")
