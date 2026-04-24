from telethon import TelegramClient, events
import asyncio

# Load your API ID, API HASH, and session from setup.py or your bot file
api_id = 29840797
api_hash = '9069896125bdbc5bacccfec478e8c64a'
session_name = 'AutoTradeBotApp'  # same session your bot is using

client = TelegramClient(session_name, api_id, api_hash)

async def list_all_groups():
    await client.start()

    dialogs = await client.get_dialogs()

    print("\n📋 Your Active Groups/Channels:\n")
    for dialog in dialogs:
        if dialog.is_group or dialog.is_channel:
            print(f"- {dialog.title}")

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(list_all_groups())
