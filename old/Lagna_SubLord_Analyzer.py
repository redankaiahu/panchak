
import swisseph as swe
import datetime
from math import floor

# Constants
swe.set_ephe_path(".")  # Path to Swiss Ephemeris data files

# Nakshatra list
NAKSHATRAS = [
    "Ashwini", "Bharani", "Krittika", "Rohini", "Mrigashirsha", "Ardra",
    "Punarvasu", "Pushya", "Ashlesha", "Magha", "Purva Phalguni", "Uttara Phalguni",
    "Hasta", "Chitra", "Swati", "Vishakha", "Anuradha", "Jyeshtha", "Mula",
    "Purva Ashadha", "Uttara Ashadha", "Shravana", "Dhanishta", "Shatabhisha",
    "Purva Bhadrapada", "Uttara Bhadrapada", "Revati"
]

def get_nakshatra(moon_longitude):
    nak_index = floor(moon_longitude / (13 + 1/3))
    return NAKSHATRAS[nak_index], nak_index + 1

def get_ascendant(jd_ut, latitude, longitude):
    # Returns ascendant degree and sign
    asc = swe.houses(jd_ut, latitude, longitude.decode(), 'P')[0][0]
    return asc

def get_planet_details(jd_ut):
    planets = {
        swe.SUN: "Sun",
        swe.MOON: "Moon",
        swe.MERCURY: "Mercury",
        swe.VENUS: "Venus",
        swe.MARS: "Mars",
        swe.JUPITER: "Jupiter",
        swe.SATURN: "Saturn",
        swe.TRUE_NODE: "Rahu"
    }
    details = {}
    for pl_code, pl_name in planets.items():
        lon, ret, _ = swe.calc_ut(jd_ut, pl_code)
        details[pl_name] = {
            "Longitude": lon,
            "Retrograde": bool(ret)
        }
    return details

def run_lagna_analysis(year, month, day, hour, minute, second, lat, lon):
    dt = datetime.datetime(year, month, day, hour, minute, second)
    jd = swe.julday(dt.year, dt.month, dt.day, dt.hour + dt.minute/60 + dt.second/3600)
    jd_ut = jd - swe.deltat(jd)

    # Ascendant
    asc = get_ascendant(jd_ut, lat, lon.encode())
    asc_sign = floor(asc / 30) + 1
    asc_degree = asc % 30

    # Moon position for Nakshatra
    moon_lon = swe.calc_ut(jd_ut, swe.MOON)[0]
    nakshatra, nak_num = get_nakshatra(moon_lon)

    # Planet positions
    planets = get_planet_details(jd_ut)

    return {
        "DateTime": dt,
        "Ascendant Degree": round(asc, 2),
        "Ascendant Sign (1=Aries)": asc_sign,
        "Ascendant Degree in Sign": round(asc_degree, 2),
        "Moon Nakshatra": nakshatra,
        "Moon Nakshatra No": nak_num,
        "Planetary Details": planets
    }

# Example usage for Mumbai on April 1, 2025, 9:15 AM IST (UTC+5:30)
if __name__ == "__main__":
    result = run_lagna_analysis(2025, 4, 1, 3, 45, 0, 19.0760, 72.8777)
    for key, val in result.items():
        print(f"{key}: {val}")
