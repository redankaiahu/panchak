from kiteconnect import KiteConnect
from datetime import datetime
import os

# ===============================
# CONFIG
# ===============================
API_KEY = "7am67kxijfsusk9i"
API_SECRET = "05ipdus0twmqrkfgjeee6w3af4l8ij71"

TOKEN_FILE = r"C:\Users\aarya\gemini\access_token.txt"

# ===============================
# GENERATE TOKEN
# ===============================
kite = KiteConnect(api_key=API_KEY)

print("\n🔐 Open this URL & login:")
print(kite.login_url())
print("\nAfter login, paste request_token below 👇\n")

request_token = input("Paste request_token here: ").strip()

data = kite.generate_session(request_token, api_secret=API_SECRET)
access_token = data["access_token"]

# ===============================
# SAVE TOKEN
# ===============================
with open(TOKEN_FILE, "w") as f:
    f.write(access_token)

print("\n✅ ACCESS TOKEN GENERATED SUCCESSFULLY")
print(f"📁 Saved to: {TOKEN_FILE}")
print(f"🕒 Time     : {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")
