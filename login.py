from kiteconnect import KiteConnect
import os
import urllib.parse as urlparse

# CONFIG
API_KEY = "7am67kxijfsusk9i"
API_SECRET = "05ipdus0twmqrkfgjeee6w3af4l8ij71"
TOKEN_FILE = "/home/ec2-user/access_token.txt"

kite = KiteConnect(api_key=API_KEY)

print("\n1. 👉 Open this URL in your browser:")
print(kite.login_url())

print("\n2. 🔑 Login and then copy the resulting URL or just the request_token.")
user_input = input("\n3. 📋 Paste here: ").strip()

# AUTO-CLEAN: If user pasted the whole URL, extract the token
request_token = user_input
if "request_token=" in user_input:
    try:
        parsed = urlparse.urlparse(user_input)
        request_token = urlparse.parse_qs(parsed.query)['request_token'][0]
    except Exception:
        request_token = user_input

try:
    data = kite.generate_session(request_token, api_secret=API_SECRET)
    access_token = data["access_token"]
    
    with open(TOKEN_FILE, "w") as f:
        f.write(access_token)
        
    print("\n" + "="*40)
    print("✅ SUCCESS: Session Created")
    print(f"👤 User  : {data.get('user_name', 'N/A')}")
    print(f"📁 Saved : {TOKEN_FILE}")
    print("="*40)

except Exception as e:
    print("\n" + "!"*40)
    print(f"❌ ERROR: {e}")
    print("\nHint: A request_token can only be used ONCE.")
    print("If it fails, open the login URL again to get a fresh token.")
    print("!"*40)
