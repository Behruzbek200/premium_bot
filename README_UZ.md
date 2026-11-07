# Premium Market V10 — HUMO UZS wallet prototipi

## DIQQAT: Telegram to'lov qoidasi
Telegram bot ichidagi Stars, Premium va Gifts raqamli mahsulot hisoblanadi. Telegramning rasmiy qoidasi bunday tovarlar uchun Telegram Stars (XTR) to'lovini talab qiladi:
https://core.telegram.org/bots/payments-stars
Shu sababli quyidagi HUMO-wallet checkout kodi **texnik prototip**, Telegram ichida bunday raqamli tovarlarni shu usulda ommaga sotish platforma qoidalariga mos kelmaydi va hisob/bot cheklanishi mumkin. Ishlaydi va production-ready deb kafolatlanmaydi.

## Nima yangilandi?
- HUMO balansini to'ldirish oynasida tayyor 5 000/10 000/20 000 paketlar YO'Q. «💳 Hisobni to'ldirish» bosilganda bot SUMMANI QO'LDA yozishni so'raydi.
- 5000 band bo'lsa 5001, u band bo'lsa 5002 ... To'lov oynasi 5 daqiqalik.
- Stars/Premium/Gift buyurtma ekranida xarid narxi UZS ko'rsatiladi; **mijozga Telegram XTR invoice YARATILMAYDI**.
- Ichki so'm balansi yetarli bo'lsa, «✅ Balansimdan sotib olish» tugmasi ko'rsatiladi.
- Yetarli bo'lmasa «💳 Hisobni to'ldirish» tugmasi ochiladi, to'lov qiymatining yetmayotgan qismi ko'rsatiladi.
- Balans va referal bonusi PostgreSQL tranzaksiyasi yordamida bir marta yechiladi, status 'paid' yaratilib, mavjud provider worker yetkazishni bajarishga urinadi.
- Yetkazish noaniq bo'lib `review` qolsa, admin natijani MUSTAQIL tekshirib, UZS hamyoniga qaytarishni tanlashi mumkin. Tasdiqsiz qayta yuborish yo'q.
- Eski V9 XTR invoice'larini endi yaratib bo'lmaydi. Eski receipts/refund mexanizmlari mavjud to'lovlarni yo'qotmaslik uchun qoldirilgan.
- Admin paneldagi XTR invoice tugmalari olib tashlangan. Gift yuborilishi uchun botning O'Z Stars balansida zaxira bo'lishi kerak — bu mijoz to'loviga aloqador emas.

## Bank xabari haqida
- `HUMO_AUTO_CREDIT=0` (standart): Telethon xabarni aniqlaydi, admin bank ilovasida to'lovni tekshiradi va tasdiqlaydi.
- `HUMO_AUTO_CREDIT=1`: mos Telegram bank-bot xabarini avtomatik kredit deb olishi mumkin. BU HIMOYALANGAN BANK API EMAS. Qalbaki/noto'g'ri xabar yoki dublikat matching tufayli pul yo'qotish xavfi bor. Bunday rejimni real pul uchun tavsiya qilmaymiz. Rasmiy bank provayderining imzolangan callback/API'si kerak.
- HUMO karta, ism, bot username va kartaning 4 raqami admin/ENV orqali sozlanadi.

## Render
Build: `pip install -r requirements.txt`
Start: `gunicorn -c gunicorn.conf.py wsgi:app`
Health: `https://SIZNING-XIZMAT.onrender.com/health`

Majburiy: `BOT_TOKEN`, `OWNER_ID`, `DATABASE_URL`, `WEBHOOK_SECRET`.
API: `FRAGMENT_API_KEY` (yoki admin paneldan); `API_ENCRYPTION_KEY` (kalitni shifrlash uchun tavsiya).
Telethon: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELETHON_STRING_SESSION`; `HUMO_BOT_USERNAME`, `HUMO_CARD_LAST4`.

## Sinovlar
Python sintaksis va lokal model-sinovlar: hamyon bir marta yechiladi, bonus chegirmasi, yetishmagan balans, qo'lda summa so'rashi va XTR invoice yuboruvchi kod yo'qligi tekshirildi.
Haqiqiy HUMO pul o'tkazmasi, Render/Supabase, Fragment yetkazishi va Telegram Gift jo'natish jonli sinovdan O'TMAGAN. Jonli pul bilan ishlatishdan oldin to'liq integratsion sinovlar zarur.

## Maxfiylik
Avval chat/screenshotda oshkor bo'lgan BOT_TOKEN, Telethon StringSession va Supabase parolini yangilang; .env faylini GitHub'ga yuklamang.
