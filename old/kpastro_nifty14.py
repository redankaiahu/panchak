import swisseph as swe
from datetime import datetime, timedelta
import pytz
import sys
sys.stdout.reconfigure(encoding='utf-8')


# Set the ephemeris path
swe.set_ephe_path("/usr/share/ephe")  # Update this path based on your system

ist = pytz.timezone('Asia/Kolkata')

# Nakshatra (Star) and Sublord details
nakshatras = [
    (0, "Ashwini", "Ketu"), (13.33, "Bharani", "Venus"), (26.67, "Krittika", "Sun"),
    (40, "Rohini", "Moon"), (53.33, "Mrigashira", "Mars"), (66.67, "Ardra", "Rahu"),
    (80, "Punarvasu", "Jupiter"), (93.33, "Pushya", "Saturn"), (106.67, "Ashlesha", "Mercury"),
    (120, "Magha", "Ketu"), (133.33, "Purva Phalguni", "Venus"), (146.67, "Uttara Phalguni", "Sun"),
    (160, "Hasta", "Moon"), (173.33, "Chitra", "Mars"), (186.67, "Swati", "Rahu"),
    (200, "Vishakha", "Jupiter"), (213.33, "Anuradha", "Saturn"), (226.67, "Jyeshtha", "Mercury"),
    (240, "Moola", "Ketu"), (253.33, "Purva Ashadha", "Venus"), (266.67, "Uttara Ashadha", "Sun"),
    (280, "Shravana", "Moon"), (293.33, "Dhanishta", "Mars"), (306.67, "Shatabhisha", "Rahu"),
    (320, "Purva Bhadrapada", "Jupiter"), (333.33, "Uttara Bhadrapada", "Saturn"), (346.67, "Revati", "Mercury")
]

sector_impact = {
    "Sun": "Leadership, Government, Large-Cap Stocks",
    "Mercury": "IT, Banking, Communication",
    "Moon": "FMCG, Real Estate, Market Sentiment",
    "Saturn": "Infrastructure, Oil & Gas, Long-Term Investments",
    "Mars": "Energy, Metals, Defense",
    "Jupiter": "Banking, Education, Financial Growth",
    "Venus": "Luxury, FMCG, Auto, Entertainment",
    "Rahu": "High Volatility, Speculative Trades, Sudden Moves",
    "Ketu": "Uncertainty, Emotional Decision Making"
}


def get_moon_phase_description(phase, illumination):
    """Returns a detailed description of the moon phase impact on the market."""
    phase_descriptions = {
        "New Moon": "Consolidation & uncertainty.",
        "First Quarter": "Range-bound, neutral trends.",
        "Waxing Gibbous": "Bullish strength.",
        "Full Moon": "Market Reversals.",
        "Waning Gibbous": "Profit booking starts.",
        "Last Quarter": "Bearish consolidation.",
        "Waning Crescent": "Bearish weakness, sell-off risk."
    }
    return f"{phase} ({illumination:.1f}% Illumination) → {phase_descriptions.get(phase, 'Market Impact Unknown')}"


sublords = [
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury"
]

def format_highlight(text, nature):
    """Formats text with color: red for malefic, green for benefic."""
    color_code = "\033[41m" if nature == "Malefic" else "\033[42m"
    reset_code = "\033[0m"
    return f"{color_code} {text} {reset_code}"


def aspect_impact(aspect):
    """ Determines impact level and sentiment for planetary aspects."""
    impact_mapping = {
        "Rahu ⨀ Ketu": ("Heavy", "Bearish"),
        "Saturn ⨀ Mars": ("Strong", "Bearish"),
        "Moon ⨀ Rahu": ("High", "Bearish"),
        "Moon ⨀ Ketu": ("High", "Bearish"),
        "Jupiter ⨀ Venus": ("Moderate", "Bullish"),
        "Sun □ Rahu": ("Medium", "Bearish"),
        "Sun □ Ketu": ("Medium", "Bearish"),
        "Jupiter □ Saturn": ("High", "Bearish")
    }
    return impact_mapping.get(aspect, ("Low", "Neutral"))

def detect_conjunctions_squares(positions):
    """Detects planetary conjunctions (0°) and squares (90°) which cause volatility."""
    aspects = {
        "Conjunctions": [],
        "Squares": []
    }
    
    planets = list(positions.keys())
    for i in range(len(planets)):
        for j in range(i + 1, len(planets)):
            angle = abs(positions[planets[i]] - positions[planets[j]]) % 360
            
            if angle < 5 or abs(angle - 360) < 5:
                aspects["Conjunctions"].append(f"{planets[i]} ⨀ {planets[j]}")
            elif abs(angle - 90) < 5 or abs(angle - 270) < 5:
                aspects["Squares"].append(f"{planets[i]} □ {planets[j]}")
    
    return aspects



# Function to determine the Nakshatra (Star) and its Lord
def get_nakshatra(longitude):
    for start_degree, nakshatra, lord in nakshatras[::-1]:
        if longitude >= start_degree:
            return nakshatra, lord
    return "Unknown", "Unknown"

# Function to determine the Sublord based on the longitude
def get_sublord(longitude):
    sublord_index = int((longitude % 13.33) / (13.33 / 9))
    return sublords[sublord_index]

def get_planet_sign(degree):
    """Returns the zodiac sign based on planetary degree."""
    signs = ["Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo", "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"]
    return signs[int(degree // 30)]

# Function to get planetary positions using Swiss Ephemeris
def get_planetary_positions(date):
    """ Fetch planetary positions at market opening (9:15 AM IST) for a given date """
    ist_datetime = ist.localize(datetime(date.year, date.month, date.day, 9, 15))
    jd = swe.julday(ist_datetime.year, ist_datetime.month, ist_datetime.day, ist_datetime.hour + ist_datetime.minute / 60.0)
    
    positions = {}
    planets = {
        "Sun": swe.SUN, "Moon": swe.MOON, "Mercury": swe.MERCURY, "Venus": swe.VENUS, "Mars": swe.MARS,
        "Jupiter": swe.JUPITER, "Saturn": swe.SATURN, "Rahu": swe.TRUE_NODE, "Ketu": swe.MEAN_NODE
    }
    
    for name, planet in planets.items():
        pos, _ = swe.calc_ut(jd, planet)
        positions[name] = pos[0]  # Store only longitude
    
    # Correct Ketu calculation (always opposite to Rahu)
    positions["Ketu"] = (positions["Rahu"] + 180) % 360
    return positions

def get_moon_phase(jd):
    """Determine the Moon phase and illumination percentage."""
    sun_pos, _ = swe.calc_ut(jd, swe.SUN)
    moon_pos, _ = swe.calc_ut(jd, swe.MOON)
    phase_angle = (moon_pos[0] - sun_pos[0]) % 360
    
    if 0 <= phase_angle < 45:
        phase = "New Moon"
    elif 45 <= phase_angle < 90:
        phase = "First Quarter"
    elif 90 <= phase_angle < 135:
        phase = "Waxing Gibbous"
    elif 135 <= phase_angle < 180:
        phase = "Full Moon"
    elif 180 <= phase_angle < 225:
        phase = "Waning Gibbous"
    elif 225 <= phase_angle < 270:
        phase = "Last Quarter"
    else:
        phase = "Waning Crescent"
    
    illumination = abs(phase_angle - 180) / 180 * 100  # Approximate illumination percentage
    return phase, illumination


def is_mercury_retrograde(date):
    """Checks if Mercury is in retrograde motion."""
    jd_today = swe.julday(date.year, date.month, date.day)
    jd_yesterday = jd_today - 1
    
    mercury_today, _ = swe.calc_ut(jd_today, swe.MERCURY)
    mercury_yesterday, _ = swe.calc_ut(jd_yesterday, swe.MERCURY)
    
    return mercury_today[0] < mercury_yesterday[0]  # Retrograde if moving backward

def analyze_market_trend(positions):
    bullish_factors = 0
    bearish_factors = 0
    mercury_retro = is_mercury_retrograde(datetime.now())
    
    if mercury_retro:
        bearish_factors += 4  # Increased weightage for Mercury retrograde
    if abs(positions['Moon'] - positions['Rahu']) < 5 or abs(positions['Moon'] - positions['Ketu']) < 5:
        bearish_factors += 3  # Moon with Rahu/Ketu causes volatility
    
    # Jupiter & Venus Bullish Impact
    if 30 <= positions['Jupiter'] % 360 <= 150:
        bullish_factors += 3  # Jupiter in strong position
    if 30 <= positions['Venus'] % 360 <= 150:
        bullish_factors += 2  # Venus supports bullish moves
    
    # Saturn-Mars Bearish Impact
    if 0 < abs(positions['Saturn'] - positions['Mars']) < 15:
        bearish_factors += 4  # Strong bearish impact if close
    elif 150 < abs(positions['Saturn'] - positions['Mars']) < 180:
        bullish_factors += 2  # Saturn-Mars opposition supports bullish recovery
    
    # Adjusted astrology prediction logic
    if bullish_factors - bearish_factors >= 2:
        trend = "Bullish"
    elif bearish_factors - bullish_factors >= 2:
        trend = "Bearish"
    else:
        trend = "Neutral"
    
    return trend, bullish_factors, bearish_factors

def get_planet_effects(sign, planet):
    """Determines if a planet's position is bullish or bearish."""
    bullish_planets = {"Jupiter", "Venus", "Mercury", "Moon"}
    bearish_planets = {"Saturn", "Mars", "Rahu", "Ketu"}
    
    if planet in bullish_planets:
        return "🟢 Bullish"
    elif planet in bearish_planets:
        return "🔴 Bearish"
    else:
        return "🟡 Neutral"

def predict_market_for_date(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    positions = get_planetary_positions(date)
    jd = swe.julday(date.year, date.month, date.day)
    moon_phase, illumination = get_moon_phase(jd)
    moon_phase_details = get_moon_phase_description(moon_phase, illumination)
    moon_nakshatra, star_lord = get_nakshatra(positions['Moon'])
    star_lord_planet = positions.get(star_lord, "Unknown")
    star_nature = "Benefic" if star_lord in ["Jupiter", "Venus", "Mercury", "Moon"] else "Malefic"
    
    sublord = get_sublord(positions['Moon'])
    sublord_planet = positions.get(sublord, "Unknown")
    sublord_nature = "Benefic" if sublord in ["Jupiter", "Venus", "Mercury", "Moon"] else "Malefic"
    
    market_trend, bullish_factors, bearish_factors = analyze_market_trend(positions)

    aspects = detect_conjunctions_squares(positions)

    

    
    print(f"\n📅 Market Prediction for {date.strftime('%Y-%m-%d')}")
    print("------------------------------------------------")
    print("1️⃣ Key Astrological Factors")
    #print(f"   - Sun & Mercury in {get_planet_sign(positions['Sun'])}")
    #print(f"   - Moon & Saturn in {get_planet_sign(positions['Moon'])}")
    #print(f"   - Mars in {get_planet_sign(positions['Mars'])}")
    #print(f"   - Jupiter in {get_planet_sign(positions['Jupiter'])}")
    #print(f"   - Venus in {get_planet_sign(positions['Venus'])}")
    #print(f"   - Rahu in {get_planet_sign(positions['Rahu'])} & Ketu in {get_planet_sign(positions['Ketu'])}")

    for planet in ["Sun", "Mercury", "Moon", "Saturn", "Mars", "Jupiter", "Venus", "Rahu", "Ketu"]:
        sign = get_planet_sign(positions[planet])
        effect = get_planet_effects(sign, planet)
        print(f"   - {planet} in {sign} → {effect}")

    
    print("\n🌙 Moon Phase Impact:")
    #print(f"   {moon_phase} ({illumination:.1f}% Illumination)")
    print(f"   {moon_phase_details}")
    
    print("\n🌟 Moon & Star Details:")
    #print(f"   - Moon in {moon_nakshatra} Nakshatra ({'Benefic' if star_lord in ['Jupiter', 'Venus', 'Mercury', 'Moon'] else 'Malefic'})")
    print(f"   - Moon in {moon_nakshatra} Nakshatra {format_highlight(f'({star_nature})', star_nature)}")
    print(f"   - Star Lord: {star_lord}")
    print(f"   - Star Lord positioned in: {star_lord_planet}")
    #print(f"   - Star Lord in which sign: {star_lord_planet // 30}° ({'Benefic' if star_lord in ['Jupiter', 'Venus', 'Mercury', 'Moon'] else 'Malefic'})")
    print(f"   - Star Lord in which sign: {star_lord_planet // 30}° {format_highlight(f'({star_nature})', star_nature)}")

    print("\n🔹 Sublord Details:")
    print(f"   - Sublord: {sublord}")
    print(f"   - Sublord positioned in: {sublord_planet}")
    #print(f"   - Sublord in which sign: {sublord_planet // 30}° ({'Benefic' if sublord in ['Jupiter', 'Venus', 'Mercury', 'Moon'] else 'Malefic'})")
    print(f"   - Sublord in which sign: {sublord_planet // 30}° {format_highlight(f'({sublord_nature})', sublord_nature)}")

    

    print("\n⚡ Planetary Aspects Influencing Market:")
    for aspect in aspects["Conjunctions"]:
        impact, sentiment = aspect_impact(aspect)
        print(f"   🔴 {aspect} → {impact} Impact ({sentiment})")
    for aspect in aspects["Squares"]:
        impact, sentiment = aspect_impact(aspect)
        print(f"   🔶 {aspect} → {impact} Impact ({sentiment})")

    #print(f"\n Overall Market Trend: {market_trend} ")
        
    print("------------------------------------------------\n")

if __name__ == "__main__":
    user_date = sys.stdin.read().strip()  # Read input date from stdin
    if user_date:
        predict_market_for_date(user_date)
    else:
        print("Error: No date provided")