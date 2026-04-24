import swisseph as swe
import datetime

# Nakshatra and their sub-lords
nakshatras = [
    ('Ashwini', 'Ketu'), ('Bharani', 'Venus'), ('Krittika', 'Sun'), ('Rohini', 'Moon'),
    ('Mrigashira', 'Mars'), ('Ardra', 'Rahu'), ('Punarvasu', 'Jupiter'), ('Pushya', 'Saturn'),
    ('Ashlesha', 'Mercury'), ('Magha', 'Ketu'), ('Purva Phalguni', 'Venus'), ('Uttara Phalguni', 'Sun'),
    ('Hasta', 'Moon'), ('Chitra', 'Mars'), ('Swati', 'Rahu'), ('Vishakha', 'Jupiter'),
    ('Anuradha', 'Saturn'), ('Jyeshtha', 'Mercury'), ('Mula', 'Ketu'), ('Purva Ashadha', 'Venus'),
    ('Uttara Ashadha', 'Sun'), ('Shravana', 'Moon'), ('Dhanishta', 'Mars'), ('Shatabhisha', 'Rahu'),
    ('Purva Bhadrapada', 'Jupiter'), ('Uttara Bhadrapada', 'Saturn'), ('Revati', 'Mercury')
]

# Vedic friendly benefics
benefics = ['Jupiter', 'Venus', 'Moon', 'Mercury']
malefics = ['Mars', 'Saturn', 'Rahu', 'Ketu', 'Sun']

# Enemy nakshatras (can customize more)
bearish_nakshatras = ['Ashlesha', 'Ardra', 'Jyeshtha']

def calculate_prediction(date_str, time_str, lat=19.0760, lon=72.8777):
    # Format and convert
    dt = datetime.datetime.strptime(f"{date_str} {time_str}", "%d/%b/%Y (%A) %H:%M:%S")
    jd = swe.julday(dt.year, dt.month, dt.day, dt.hour + dt.minute / 60 + dt.second / 3600)
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    # Ascendant degree (Lagna)
    lagna_deg = swe.houses(jd, lat, lon, b'A')[0][0]
    sign_index = int(lagna_deg / 30)
    signs = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo',
             'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces']
    lords = ['Mars', 'Venus', 'Mercury', 'Moon', 'Sun', 'Mercury',
             'Venus', 'Mars', 'Jupiter', 'Saturn', 'Saturn', 'Jupiter']
    lagna_sign = signs[sign_index]
    lagna_lord = lords[sign_index]

    # Moon position and Nakshatra
    moon_pos = swe.calc_ut(jd, swe.MOON)[0][0]
    nak_index = int((moon_pos % 360) / (360 / 27))
    nak_name, sub_lord = nakshatras[nak_index]

    # Basic prediction logic
    if lagna_lord in benefics and sub_lord in benefics and nak_name not in bearish_nakshatras:
        prediction = 'UP'
    elif lagna_lord in malefics or sub_lord in malefics or nak_name in bearish_nakshatras:
        prediction = 'DOWN'
    else:
        prediction = 'SIDEWAYS/UNCLEAR'

    # Return dictionary
    return {
        'DateTime': dt.strftime('%Y-%m-%d %H:%M:%S'),
        'Lagna Degree': round(lagna_deg, 2),
        'Lagna Sign': lagna_sign,
        'Lagna Lord': lagna_lord,
        'Moon Nakshatra': nak_name,
        'Sub Lord': sub_lord,
        'Prediction': prediction
    }

# Example run
if __name__ == "__main__":
    # Replace this with loop over your Excel or dynamic input
    result = calculate_prediction("17/Apr/2025 (Thursday)", "09:15:00")
    for key, value in result.items():
        print(f"{key}: {value}")
