import swisseph as swe
import datetime

nakshatras = [
    ('Ashwini', 'Ketu'), ('Bharani', 'Venus'), ('Krittika', 'Sun'), ('Rohini', 'Moon'),
    ('Mrigashira', 'Mars'), ('Ardra', 'Rahu'), ('Punarvasu', 'Jupiter'), ('Pushya', 'Saturn'),
    ('Ashlesha', 'Mercury'), ('Magha', 'Ketu'), ('Purva Phalguni', 'Venus'), ('Uttara Phalguni', 'Sun'),
    ('Hasta', 'Moon'), ('Chitra', 'Mars'), ('Swati', 'Rahu'), ('Vishakha', 'Jupiter'),
    ('Anuradha', 'Saturn'), ('Jyeshtha', 'Mercury'), ('Mula', 'Ketu'), ('Purva Ashadha', 'Venus'),
    ('Uttara Ashadha', 'Sun'), ('Shravana', 'Moon'), ('Dhanishta', 'Mars'), ('Shatabhisha', 'Rahu'),
    ('Purva Bhadrapada', 'Jupiter'), ('Uttara Bhadrapada', 'Saturn'), ('Revati', 'Mercury')
]

planet_dignities = {
    'Sun': (120, 270), 'Moon': (30, 210), 'Mars': (300, 90),
    'Mercury': (180, 330), 'Jupiter': (90, 270), 'Venus': (330, 180), 'Saturn': (210, 0)
}

planets = {
    'Sun': swe.SUN, 'Moon': swe.MOON, 'Mars': swe.MARS,
    'Mercury': swe.MERCURY, 'Jupiter': swe.JUPITER,
    'Venus': swe.VENUS, 'Saturn': swe.SATURN
}

benefics = ['Jupiter', 'Venus', 'Moon', 'Mercury']
malefics = ['Mars', 'Saturn', 'Rahu', 'Ketu', 'Sun']
bearish_nakshatras = ['Ashlesha', 'Ardra', 'Jyeshtha']

def get_planet_status(jd, planet_id, degree):
    try:
        planet_data = swe.calc_ut(jd, planet_id)
        speed = planet_data[3] if len(planet_data) > 3 else 0.0
    except Exception as e:
        print(f"Error fetching speed for planet ID {planet_id}: {e}")
        speed = 0.0

    retro = speed < 0
    dignity = "Neutral"
    for pname, (exalt, debil) in planet_dignities.items():
        if planet_id == planets[pname]:
            if abs(degree - exalt) <= 5:
                dignity = "Exalted"
            elif abs(degree - debil) <= 5:
                dignity = "Debilitated"
    return ("Retrograde" if retro else "Direct"), dignity


def get_full_astro_prediction(date_obj, time_str="09:15:00", lat=19.0760, lon=72.8777):
    dt = datetime.datetime.combine(date_obj, datetime.datetime.strptime(time_str, "%H:%M:%S").time())
    jd = swe.julday(dt.year, dt.month, dt.day, dt.hour + dt.minute / 60 + dt.second / 3600)
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    lagna_deg = swe.houses(jd, lat, lon, b'A')[0][0]
    sign_index = int(lagna_deg / 30)
    signs = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo',
             'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces']
    lords = ['Mars', 'Venus', 'Mercury', 'Moon', 'Sun', 'Mercury',
             'Venus', 'Mars', 'Jupiter', 'Saturn', 'Saturn', 'Jupiter']
    lagna_sign = signs[sign_index]
    lagna_lord = lords[sign_index]

    moon_pos = swe.calc_ut(jd, swe.MOON)[0][0]
    nak_index = int((moon_pos % 360) / (360 / 27))
    nak_name, sub_lord = nakshatras[nak_index]

    planet_status = {}
    for pname, pid in planets.items():
        pos = swe.calc_ut(jd, pid)[0][0]
        motion, dignity = get_planet_status(jd, pid, pos)
        planet_status[pname] = {'Degree': round(pos, 2), 'Motion': motion, 'Dignity': dignity}

    if lagna_lord in benefics and sub_lord in benefics and nak_name not in bearish_nakshatras:
        prediction = 'UP'
    elif lagna_lord in malefics or sub_lord in malefics or nak_name in bearish_nakshatras:
        prediction = 'DOWN'
    else:
        prediction = 'SIDEWAYS/UNCLEAR'

    return {
        'DateTime': dt.strftime("%Y-%m-%d %H:%M:%S"),
        'Lagna Sign': lagna_sign,
        'Lagna Lord': lagna_lord,
        'Moon Nakshatra': nak_name,
        'Sub Lord': sub_lord,
        'Prediction': prediction,
        'Planets': planet_status
    }

def run_mode(mode='weekly'):
    base_date = datetime.date(2025, 4, 1)
    if mode == 'daily':
        dates = [base_date]
    elif mode == 'weekly':
        dates = [base_date + datetime.timedelta(days=i) for i in range(5)]
    elif mode == 'monthly':
        dates = [base_date + datetime.timedelta(days=i) for i in range(22)]
    else:
        print("Invalid mode")
        return

    for d in dates:
        result = get_full_astro_prediction(d)
        print(f"\n📅 {result['DateTime']} | Prediction: {result['Prediction']}")
        print(f"🪐 Lagna: {result['Lagna Sign']} ({result['Lagna Lord']}) | Moon Nakshatra: {result['Moon Nakshatra']} (Sub-Lord: {result['Sub Lord']})")
        print("📉 Planetary Status:")
        for planet, status in result['Planets'].items():
            print(f"  {planet:8}: {status['Degree']:6}° | {status['Motion']} | {status['Dignity']}")

if __name__ == "__main__":
    run_mode('weekly')  # options: 'daily', 'weekly', 'monthly'
