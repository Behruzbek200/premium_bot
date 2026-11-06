# -*- coding: utf-8 -*-
"""Generate Telegram StringSession on YOUR own machine. Never publish session/keys."""
import getpass
import os
from pathlib import Path
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("Telegram API_ID (my.telegram.org): ").strip())
api_hash = getpass.getpass("Telegram API_HASH (input hidden): ").strip()
print("Telefoningizga Telegram login kodi keladi. U kodni faqat shu terminalga kiriting.")
with TelegramClient(StringSession(), api_id, api_hash) as client:
    client.start(phone=lambda: input("Telefon (+998...): ").strip(),
                 code_callback=lambda: input("Telegram login kodi: ").strip(),
                 password=lambda: getpass.getpass("2FA parol (kerak bo‘lsa): "))
    secret = StringSession.save(client.session)

path = Path("telethon_session_SECRET.txt")
path.write_text(secret, encoding="utf-8")
try:
    os.chmod(path, 0o600)
except OSError:
    pass
print("\n✅ StringSession yaratildi: ", path.resolve())
print("⚠️ Bu fayl akkauntingizga kirish huquqini beradi. GitHub yoki chatga YUBORMANG.")
print("✅ .env ichidagi TELETHON_STRING_SESSION= qiymatiga shu fayldagi satrni o‘zingiz kiriting.")
