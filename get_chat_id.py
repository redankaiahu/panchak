"""
Run this once on your machine to get your Panchak Alerts channel chat ID.
Usage: python3 get_chat_id.py
"""
import urllib.request, json

#TOKEN = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TOKEN = "8183479936:AAHyIgC1zyGOy-yoSAidDcO5KJ9nReaPP78"

print("Fetching updates from Telegram...")
print("(Make sure you posted at least 1 message in your 'Panchak Alerts' channel)\n")

try:
    url  = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())

    if not data.get("ok"):
        print(f"API error: {data}")
    elif not data.get("result"):
        print("No updates found.")
        print("→ Please post any message in your 'AutoBotwhatsapp' channel first, then re-run this script.")
    else:
        found = []
        for upd in data["result"]:
            # channel post
            cp = upd.get("channel_post") or upd.get("message") or {}
            chat = cp.get("chat", {})
            if chat.get("id"):
                found.append({
                    "id":    chat["id"],
                    "title": chat.get("title",""),
                    "type":  chat.get("type",""),
                })

        if not found:
            print("No chat entries found in updates.")
            print("→ Post a message in 'Panchak Alerts' channel then re-run.")
        else:
            print("=" * 50)
            print("FOUND CHATS:")
            for ch in found:
                print(f"  Chat ID : {ch['id']}")
                print(f"  Title   : {ch['title']}")
                print(f"  Type    : {ch['type']}")
                print()
            print("=" * 50)
            # Find 'Panchak Alerts'
            target = next((c for c in found if "panchak" in c["title"].lower() or "alert" in c["title"].lower()), found[-1])
            print(f"\n✅ Use this Chat ID in your dashboard:")
            print(f"   TG_CHAT_ID = \"{target['id']}\"")

            # Auto-save to cache file
            import os
            cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
            os.makedirs(cache_dir, exist_ok=True)
            with open(os.path.join(cache_dir, "tg_chat_id.txt"), "w") as f:
                f.write(str(target["id"]))
            print(f"\n✅ Also saved to: cache/tg_chat_id1.txt")
            print("   The dashboard will auto-load this on next start.")

except Exception as e:
    print(f"Error: {e}")
