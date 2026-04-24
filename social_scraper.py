import time
import requests
import json
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ================= CONFIGURATION =================
TELEGRAM_BOT_TOKEN = "8183479936:AAHyIgC1zyGOy-yoSAidDcO5KJ9nReaPP78"
TELEGRAM_CHAT_ID   = "-1002360390807" # From Telegram_Trade_Agent.py

# Dictionary of accounts to watch { "UniqueName": "URL" }
WATCHLIST = {
    "TheBullsEye":    "https://www.threads.net/@the_bulls_eye2",
    "SharadThite":    "https://www.threads.net/@sharadthite",
    "Gugapriyan":     "https://www.threads.net/@gugapriyan_varahi",
    "OptionEdge":     "https://www.threads.net/@optionedgetamil",
    "ThalaSiva":      "https://www.threads.net/@thala_siva30",
    "PankajKumar_X":  "https://x.com/pankajkummar369"
}

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_FILE  = os.path.join(BASE_DIR, "social_scraper_state.json")
PROFILE_DIR = os.path.join(BASE_DIR, "chrome_bot_profile")

# Ensure profile directory exists
if not os.path.exists(PROFILE_DIR):
    os.makedirs(PROFILE_DIR)
# =================================================

def send_telegram_message(text):
    """Sends a formatted alert to the Telegram channel."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print("✅ Telegram alert sent.")
        else:
            print(f"❌ Telegram Error: {r.text}")
    except Exception as e:
        print(f"❌ Connection error sending to Telegram: {e}")

def load_state():
    """Loads the last scraped posts to avoid duplicates."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state):
    """Saves the current posts as the last seen."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4)

def scrape_threads(driver, url):
    """Scrapes the latest post text from a Threads profile."""
    try:
        driver.get(url)
        time.sleep(6)
        spans = driver.find_elements(By.CSS_SELECTOR, 'span[dir="auto"]')
        for s in spans:
            txt = s.text.strip()
            if len(txt) > 20 and "@" not in txt[:10]:
                if any(x in txt for x in ["Contact Uploading", "Non-Users", "Privacy Policy", "Cookie Policy"]):
                    continue
                return txt
    except Exception as e:
        print(f"⚠️ Threads scrape failed for {url}: {e}")
    return None

def scrape_x(driver, url):
    """Scrapes the latest tweet from an X profile."""
    try:
        driver.get(url)
        time.sleep(8)
        if "login" in driver.current_url.lower():
            print(f"🛑 X asking for login for {url}. Please log in manually.")
            return None
        tweets = driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweetText"]')
        if tweets:
            return tweets[0].text.strip()
    except Exception as e:
        print(f"⚠️ X scrape failed for {url}: {e}")
    return None

def main():
    print("="*50)
    print("🚀 SOCIAL MEDIA ALERT AGENT STARTED")
    print(f"   Watching {len(WATCHLIST)} accounts...")
    print("   Mode: Continuous Loop (10 min interval)")
    print("="*50)
    
    while True:
        state = load_state()
        driver = None
        
        try:
            is_linux = sys.platform.startswith('linux')
            chrome_options = Options()
            if is_linux:
                chrome_options.add_argument("--headless=new")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
            
            chrome_options.add_argument(f"--user-data-dir={PROFILE_DIR}")
            chrome_options.add_argument("--profile-directory=Default")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", False)
            chrome_options.add_argument("window-size=1280,1000")
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            for name, url in WATCHLIST.items():
                is_x = "x.com" in url
                platform = "X" if is_x else "THREADS"
                print(f"\n🔍 Checking {platform}: {name}...")
                
                latest = scrape_x(driver, url) if is_x else scrape_threads(driver, url)
                
                if latest and latest != state.get(name):
                    print(f"✨ New {platform} Post found for {name}!")
                    icon = "🐦" if is_x else "📱"
                    msg = f"{icon} <b>{platform} UPDATE: {name}</b>\n\n{latest}\n\n🔗 <a href='{url}'>View Post</a>"
                    send_telegram_message(msg)
                    state[name] = latest
                else:
                    print(f"✅ {name}: No new updates.")
                
                time.sleep(3) # Small gap between account visits

        except Exception as e:
            print(f"❌ Cycle Error: {e}")
        finally:
            if driver:
                try: driver.quit()
                except: pass
            save_state(state)
            print(f"\n🏁 Cycle complete. Sleeping for 10 minutes... ({time.strftime('%H:%M:%S')})")
            time.sleep(600)

if __name__ == "__main__":
    main()
