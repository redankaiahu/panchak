# =================================================================
# numerology_monitor.py  —  @pankajkummar369 X/Twitter Monitor
# =================================================================
# Monitors @pankajkummar369 on X (Twitter) for new posts.
# Parses numerology predictions (date + root number + signal).
# Sends Telegram alert when a new post is found.
#
# HOW IT WORKS:
#   • Uses nitter.net (free, no API key needed) to read X posts
#   • Runs every 10 minutes (configurable)
#   • Deduplicates — same post never alerted twice
#   • Parses the digit sum from tweet text automatically
#
# RUN:  python3 numerology_monitor.py
# STOP: Ctrl+C
#
# On Linux, add to background_worker.py or run as separate process:
#   nohup python3 numerology_monitor.py >> CACHE/numeo_monitor.log 2>&1 &
# =================================================================

import os, re, json, time, logging, urllib.request, urllib.parse
from datetime import datetime, date
import pytz

# ── Config ────────────────────────────────────────────────────────
TG_BOT_TOKEN  = "8674294774:AAFUgjUUdepnsSiCKmh0emcZcn4cXlef35A"
TG_CHAT_ID    = "-1003706739531"
X_USERNAME    = "pankajkummar369"
CHECK_EVERY   = 600   # seconds (10 minutes)
CACHE_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CACHE")
DEDUP_FILE    = os.path.join(CACHE_DIR, "numeo_seen_posts.json")
IST           = pytz.timezone("Asia/Kolkata")

# Nitter instances (free X mirror, no API key)
NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
]

os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(CACHE_DIR, "numeo_monitor.log")),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("numeo")

# ── Numerology engine ─────────────────────────────────────────────
ROOT_PLANET = {
    1: "Sun",    2: "Moon",    3: "Jupiter", 4: "Rahu",
    5: "Mercury",6: "Venus",   7: "Ketu",    8: "Saturn", 9: "Mars",
}
ROOT_SIGNAL = {
    1: ("🟢 BULLISH",  "Buy dips, positive energy, new beginnings"),
    2: ("🟡 NEUTRAL",  "Emotional market, wait & watch"),
    3: ("🟢 BULLISH",  "Growth, expansion — lean long"),
    4: ("🔴 BEARISH",  "Rahu volatile — extreme swings, beware traps"),
    5: ("🟡 VOLATILE", "Mercury — IT/banking choppy, two-sided day"),
    6: ("🟢 BULLISH",  "Venus — rally expected, buy dips"),
    7: ("🔴 BEARISH",  "Ketu — losses, detachment energy, avoid longs"),
    8: ("🔴 BEARISH",  "Saturn — slowdown, sell on rise"),
    9: ("🔴 EXTREME BEARISH", "Mars — biggest crash risk, war energy, strong sell"),
}

def date_root(d: date) -> int:
    """Compute numerology root number for a date."""
    s = sum(int(c) for c in d.strftime("%d%m%Y"))
    while s > 9:
        s = sum(int(c) for c in str(s))
    return s

def today_numerology() -> dict:
    """Return today's numerology signal."""
    today = datetime.now(IST).date()
    root  = date_root(today)
    sig, action = ROOT_SIGNAL[root]
    planet = ROOT_PLANET[root]
    digits = list(d.strftime("%d%m%Y"))
    calc   = "+".join(digits) + "=" + str(sum(int(c) for c in digits))
    return {
        "date":   today,
        "root":   root,
        "planet": planet,
        "signal": sig,
        "action": action,
        "calc":   calc,
    }

def parse_tweet_prediction(text: str) -> dict | None:
    """
    Try to extract a numerology prediction from tweet text.
    Looks for patterns like: "1+0+4+2+0+2+6=15=6"
    Returns dict with root, signal, or None if not found.
    """
    # Match digit sum patterns e.g. 1+0+4+2+0+2+6=15=6 or =19=1
    pattern = r'[\d+]+\s*=\s*(\d+)\s*=\s*(\d)'
    m = re.search(pattern, text)
    if m:
        final_sum = int(m.group(1))
        root      = int(m.group(2))
        # Validate (sometimes they show intermediate: =19=10=1)
        while final_sum > 9:
            final_sum = sum(int(c) for c in str(final_sum))
        if final_sum != root:
            root = final_sum
        sig, action = ROOT_SIGNAL.get(root, ("🟡 MIXED", "Unknown"))
        planet = ROOT_PLANET.get(root, "?")
        return {"root": root, "planet": planet, "signal": sig, "action": action}

    # Fallback: look for "root number X" or "= X" at end
    m2 = re.search(r'=\s*(\d)\s*(?:🔥|🚀|💥|🔴|🟢|$)', text)
    if m2:
        root = int(m2.group(1))
        sig, action = ROOT_SIGNAL.get(root, ("🟡 MIXED", "Unknown"))
        return {"root": root, "planet": ROOT_PLANET.get(root,"?"), "signal": sig, "action": action}
    return None

# ── Seen posts dedup ───────────────────────────────────────────────
def load_seen() -> set:
    try:
        with open(DEDUP_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen: set):
    try:
        with open(DEDUP_FILE, "w") as f:
            json.dump(list(seen), f)
    except Exception:
        pass

# ── Telegram sender ────────────────────────────────────────────────
def send_tg(msg: str):
    try:
        url  = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            if not resp.get("ok"):
                log.warning(f"TG error: {resp}")
            else:
                log.info("✅ TG sent")
    except Exception as e:
        log.warning(f"TG send failed: {e}")

# ── Fetch X posts via Nitter ───────────────────────────────────────
def fetch_posts(username: str) -> list[dict]:
    """
    Fetch recent posts from @username via Nitter (free, no API).
    Returns list of {id, text, url, date}.
    Falls back through multiple Nitter instances.
    """
    for instance in NITTER_INSTANCES:
        try:
            url = f"{instance}/{username}/rss"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; PanchakBot/1.0)"
            })
            with urllib.request.urlopen(req, timeout=12) as r:
                raw = r.read().decode("utf-8", errors="replace")

            # Parse RSS items
            items = re.findall(
                r'<item>(.*?)</item>', raw, re.DOTALL
            )
            posts = []
            for item in items[:10]:  # last 10 posts
                title = re.search(r'<title[^>]*>(.*?)</title>', item, re.DOTALL)
                link  = re.search(r'<link[^>]*>(.*?)</link>', item, re.DOTALL)
                desc  = re.search(r'<description[^>]*>(.*?)</description>', item, re.DOTALL)
                guid  = re.search(r'<guid[^>]*>(.*?)</guid>', item, re.DOTALL)
                pub   = re.search(r'<pubDate[^>]*>(.*?)</pubDate>', item, re.DOTALL)

                text = ""
                if desc:
                    # Strip HTML tags
                    text = re.sub(r'<[^>]+>', ' ', desc.group(1))
                    text = re.sub(r'\s+', ' ', text).strip()
                if title:
                    t = re.sub(r'<[^>]+>', ' ', title.group(1)).strip()
                    if t and t != "R to @" + username:
                        text = t + " " + text

                post_id = guid.group(1).strip() if guid else (link.group(1).strip() if link else text[:40])
                post_url = link.group(1).strip() if link else f"https://x.com/{username}"

                if text:
                    posts.append({
                        "id":   post_id,
                        "text": text[:500],
                        "url":  post_url,
                        "date": pub.group(1).strip() if pub else "",
                    })

            if posts:
                log.info(f"✅ Fetched {len(posts)} posts from {instance}")
                return posts

        except Exception as e:
            log.debug(f"Nitter {instance} failed: {e}")
            continue

    log.warning("All Nitter instances failed")
    return []

# ── Format Telegram alert ──────────────────────────────────────────
def format_alert(post: dict, prediction: dict) -> str:
    sig    = prediction["signal"]
    planet = prediction["planet"]
    root   = prediction["root"]
    action = prediction["action"]
    icon   = "🟢" if "BULLISH" in sig else ("🔴" if "BEARISH" in sig else "🟡")

    return (
        f"{icon} <b>@pankajkummar369 NEW POST — Numerology Prediction</b>\n\n"
        f"📊 <b>Root: {root} ({planet})</b>\n"
        f"📈 <b>Signal: {sig}</b>\n"
        f"💡 Action: {action}\n\n"
        f"📝 Post:\n<i>{post['text'][:280]}</i>\n\n"
        f"🔗 <a href='{post['url']}'>View on X</a>\n"
        f"🕐 {datetime.now(IST).strftime('%d %b %Y %H:%M IST')}"
    )

def format_new_post_alert(post: dict) -> str:
    """Alert for posts without parseable numerology."""
    return (
        f"📢 <b>@pankajkummar369 NEW POST</b>\n\n"
        f"📝 <i>{post['text'][:300]}</i>\n\n"
        f"🔗 <a href='{post['url']}'>View on X</a>\n"
        f"🕐 {datetime.now(IST).strftime('%d %b %Y %H:%M IST')}"
    )

# ── Daily morning numerology alert ────────────────────────────────
_last_morning_alert = ""

def maybe_send_morning_alert():
    global _last_morning_alert
    now = datetime.now(IST)
    today_str = now.strftime("%Y%m%d")
    if now.hour == 8 and now.minute < 10 and _last_morning_alert != today_str:
        n = today_numerology()
        sig, action = n["signal"], n["action"]
        icon = "🟢" if "BULLISH" in sig else ("🔴" if "BEARISH" in sig else "🟡")
        msg = (
            f"{icon} <b>Numerology Daily Signal — {n['date'].strftime('%d %b %Y')}</b>\n\n"
            f"🔢 <b>{n['calc']} = Root {n['root']} ({n['planet']})</b>\n"
            f"📈 <b>{sig}</b>\n"
            f"💡 {action}\n\n"
            f"📌 Source: @pankajkummar369 system\n"
            f"⚠️ <i>Not financial advice</i>"
        )
        send_tg(msg)
        _last_morning_alert = today_str
        log.info(f"🌅 Morning numerology alert sent: Root {n['root']} {sig}")

# ── Main loop ──────────────────────────────────────────────────────
def main():
    log.info("="*55)
    log.info("  @pankajkummar369 Numerology Monitor starting...")
    log.info(f"  Checking every {CHECK_EVERY//60} minutes")
    log.info("="*55)

    seen = load_seen()
    log.info(f"Loaded {len(seen)} already-seen post IDs")

    # Send startup test
    n = today_numerology()
    send_tg(
        f"🔢 <b>Numerology Monitor Started</b>\n"
        f"Today = Root {n['root']} ({n['planet']}) {n['signal']}\n"
        f"Watching @{X_USERNAME} for new predictions.\n"
        f"Morning alert at 8:00 AM IST every market day."
    )

    while True:
        try:
            maybe_send_morning_alert()

            posts = fetch_posts(X_USERNAME)
            new_count = 0

            for post in posts:
                pid = post["id"]
                if pid in seen:
                    continue

                # New post found!
                seen.add(pid)
                new_count += 1
                log.info(f"🆕 New post: {post['text'][:80]}")

                # Try to parse numerology
                prediction = parse_tweet_prediction(post["text"])
                if prediction:
                    msg = format_alert(post, prediction)
                    log.info(f"   → Parsed: Root {prediction['root']} {prediction['signal']}")
                else:
                    msg = format_new_post_alert(post)
                    log.info("   → No numerology parsed, sending raw post alert")

                send_tg(msg)
                time.sleep(2)  # small delay between TG messages

            if new_count:
                save_seen(seen)
                log.info(f"✅ Processed {new_count} new posts")
            else:
                log.debug("No new posts found")

        except KeyboardInterrupt:
            log.info("🛑 Monitor stopped by user")
            break
        except Exception as e:
            log.error(f"Loop error: {e}")

        time.sleep(CHECK_EVERY)

if __name__ == "__main__":
    main()
