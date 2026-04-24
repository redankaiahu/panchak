from kiteconnect import KiteConnect

API_KEY = "7am67kxijfsusk9i"
API_SECRET = "05ipdus0twmqrkfgjeee6w3af4l8ij71"

kite = KiteConnect(api_key=API_KEY)

request_token = input("Paste request_token here: ").strip()

data = kite.generate_session(request_token, api_secret=API_SECRET)

access_token = data["access_token"]

print("ACCESS TOKEN:", access_token)
