from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from telegram import Bot
import time

# --- YOUR SETTINGS ---

WHATSAPP_CHANNEL_NAME = "The Trader Edge Official"  # WhatsApp Channel Name (exactly as it appears)
TELEGRAM_BOT_TOKEN = "8183479936:AAHyIgC1zyGOy-yoSAidDcO5KJ9nReaPP78"  # Your Bot Token
TELEGRAM_GROUP_ID = "-1002360390807"  # Your Telegram Group ID

# --- Setup Telegram Bot ---
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# --- Setup Chrome for WhatsApp Web ---
options = webdriver.ChromeOptions()
options.add_argument("--user-data-dir=./User_Data")  # Save session (No need to scan QR every time)
options.add_argument("--profile-directory=Default")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Open WhatsApp Web
driver.get("https://web.whatsapp.com/")
print("Please scan QR code if not already logged in...")
time.sleep(20)  # Wait for manual login

last_message = ""

# --- Main Monitoring Loop ---
while True:
    try:
        # Search and click Channel if not already open
        search_box = driver.find_element(By.XPATH, '//div[@contenteditable="true"][@data-tab="3"]')
        search_box.clear()
        search_box.send_keys(WHATSAPP_CHANNEL_NAME)
        time.sleep(2)
        channel = driver.find_element(By.XPATH, f'//span[@title="{WHATSAPP_CHANNEL_NAME}"]')
        channel.click()
        time.sleep(2)

        # Get the latest message
        message_boxes = driver.find_elements(By.XPATH, '//div[contains(@class, "message-in")]//div[@class="_21Ahp"]')
        if message_boxes:
            latest_message = message_boxes[-1].text.strip()
            if latest_message and latest_message != last_message:
                print(f"New Message Detected: {latest_message}")
                
                # Send to Telegram Group
                bot.send_message(chat_id=TELEGRAM_GROUP_ID, text=latest_message)
                
                last_message = latest_message
        else:
            print("No message found yet...")

    except Exception as e:
        print(f"Error: {e}")

    time.sleep(5)  # Check every 5 seconds
