# Premium Market V4 — Render + pyTelegramBotAPI

Bu loyiha **Stars, Premium va Gift** mahsulotlarini Telegram bot orqali sotish uchun. Asosiy algoritmlar: UZS ko'rinishidagi katalog, XTR invoice, `successful_payment` kelgandan keyin **avtomatik yetkazish**, bonus, Gift katalogi va admin panel. Telegram uchun raqamli mahsulotlar bot ichida faqat **XTR bilan** sotiladi: https://core.telegram.org/bots/payments-stars

> Muhim: hech kim tashqi xizmatlarning **100%** ishlashiga kafolat bera olmaydi. `fragment-api.uz` API javobi noaniq yoki xato bo'lsa, shu mahsulotni yana yuborib qo'ymaslik uchun `review` holatiga o'tkaziladi. Providerning bu versiya hujjatlarida idempotent buyurtma identifikatori va buyurtmani mustaqil tekshirish endpointi ko'rsatilmagan. Uni ko'r-ko'rona takrorlash moliyaviy yo'qotish keltirishi mumkin. **Haqiqiy pul bilan jonli sinov amalga oshirilmagan.**

## Qanday ishlaydi?

- **Stars**: admin belgilagan to'plam narxi (masalan, 50 Stars — 18 888 so'm). Xaridor "O'zimga" yoki "Do'stimga"ni tanlaydi. Username `fragment-api.uz/api/v1/getInfo` orqali tekshiriladi; Telegram `XTR` invoice to'lanadi; `successful_payment` kelganidan keyin `/stars/buy` bajariladi.
- **Premium**: 3/6/12 oy, xuddi shu usul, `/premium/buy`.
- **Gift**: Telegram `getAvailableGifts`, haqiqiy stiker, o'zbekcha nom, UZS narx. To'lov XTR bilan tasdiqlangach botning haqiqiy Stars balansidan `sendGift`. Boshqa odamga yuborish uchun uning Telegram ID raqami kerak; odatda do'st ham botni `/start` qilgan bo'lishi kerak.
- **Referal**: taklif qilgan do'sti muvaffaqiyatli xarid qilsa belgilangan foiz UZS **bonus chegirmasi** sifatida saqlanadi. Keyingi buyurtmada ishlatiladi; bir buyurtma uchun bir marta beriladi.
- **HUMO**: `StringSession` orqali bank xabarnomalarini kuzatadi; takrorlanishdan himoyalangan **jurnal** va sinov moslashtiruvi. Bu bankning rasmiy tasdiqlovchi webhooki emas. Balans avtomatik oshirilmaydi, raqamli mahsulotlar uchun karta checkouti ishlatilmaydi.
- **Himoya**: SQLite `UNIQUE charge_id`, atomik status o'tishlari, at-most-once yetkazish, kechikkan/ortiqcha XTR receiptlarni avtomatik refund navbatiga qo'yish. Refund API natijasi noaniq bo'lsa qayta-qayta pul jo'natmaydi, admin tekshiradi.

## Render'ga joylash (Blueprint, tavsiya)

**Paid Starter Web Service + 1GB persistent disk** kerak. Render Free vaqtinchalik disk bilan ishlashni tavsiya etmaymiz: buyurtmalar, balans va referallar yo'qolishi mumkin. `render.yaml` loyiha ichida tayyor. Blueprint tanlaganingizda xizmatning doimiy diski `/var/data`ga ulanadi.

1. **Shu ZIP ichidagi fayllarni** yangi GitHub repository'ning ildiziga yuklang. `.env`, haqiqiy token va `StringSession`ni GitHub'ga **hech qachon** yuklamang.
2. Render → **New → Blueprint** → GitHub'dagi yangi repository'ni tanlang. `render.yaml` avtomatik o'qiladi.
3. Blueprint environment variables so'raganda quyidagi qiymatlarni kiriting: `BOT_TOKEN` (BotFather'dan **YANGI token**), `OWNER_ID` (Telegram user ID), `FRAGMENT_API_KEY` (fragment-api.uz). `WEBHOOK_SECRET` Render tomonidan avtomatik yaratiladi. `PUBLIC_URL` talab qilinmaydi — Render `RENDER_EXTERNAL_URL`ni o'zi ta'minlaydi.
4. HUMO xabarnomalarini *faqat audit* sifatida ko'rmoqchi bo'lsangiz, Render Environment bo'limida qo'shimcha `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELETHON_STRING_SESSION`, `HUMO_CARD_LAST4` qiymatlarini kiriting. Bu to'rtta ENV Blueprintda majburiy emas.
5. Deploy'dan keyin Render'dagi `https://...onrender.com/health` da `ok: true`, `database_ready: true` bo'lishini tekshiring. Webhook serverning o'zidan maxfiy token bilan o'rnatiladi, siz terminalda `setup_webhook.py` ishga tushirmaysiz.
6. Telegram'da `/start` va `/admin` kiriting. `API/Balans` orqali provider hamyoni va bot Stars balansini tekshiring. Gift katalogini sinxronlang.
7. Admin paneldan **narxlarni real tannarxga moslang**: Stars paketlari, Premium, Gift, `USD/UZS`, `XTR/UZS` (narxni XTRga *hisoblash* uchun), Gift Stars tannarxi, referal foizi. Starter narxlar faqat demo!
8. Boshqa mahsulotdan oldin **kichik haqiqiy test buyurtma** qilib `XTR` receipt va provider yetkazish natijasini tekshiring. Agar tekshirilgan bo'lsa `✅ Narxlarni tekshirdim` va `🔴/🟢 Savdoni yoqish` ni bosing. Asl holatda real savdo **o'chiq**.

**Render start command:** `gunicorn -c gunicorn.conf.py wsgi:app`

**Render build command:** `pip install -r requirements.txt`

**Disk mount:** `/var/data`, **DB_PATH:** `/var/data/market.sqlite3`.

`gunicorn.conf.py` bitta worker ishlatadi, chunki SQLite, buyurtma/navbat va Telethon oqimi bittadan bo'lishi kerak. Ixtiyoriy miqyoslash yoki ko'p server kerak bo'lsa PostgreSQL va alohida distributed worker kerak.

## Token va sessiya xavfsizligi

- Oldin tokeningiz chat/loglarda oshkor bo'lgan: **BotFather orqali yangi token** oling va eskisini bekor qiling. Har qanday eski webhook va bot jarayonini to'xtating.
- `create_session.py`ni **faqat o'z kompyuteringizda** ishlating, kodni va 2FA parolni hech kimga bermang. Hosil bo'lgan `TELETHON_STRING_SESSION` akkauntingizga to'liq kirish huquqini beradi: chatga/GitHub'ga yubormang.
- Render Dashboard → Environment bo'limidan maxfiy qiymatlarni kiriting. `.env.example`da faqat nomlar keltirilgan.
- API hamyoningizda real Stars/Premium yetkazish uchun yetarli mablag' bo'lishi zarur; Gift uchun bot Stars balansi yetarli bo'lishi zarur. To'lovni qabul qilmaslik xavfi uchun narxlarni xatar zaxirasi bilan belgilang.

## Foydalanuvchida to'lov qanday ko'rinadi?

Katalogda **so'mdagi hisoblangan narx** ko'rsatiladi. Checkout tugmasi Telegram `XTR` invoice chiqaradi — foydalanuvchi bevosita karta raqamiga pul o'tkazmaydi. Shuning uchun bu loyiha siz so'ragan **UZS kartaga to'lov → Telethon bank xabari → balans avtomatik oshishi** mexanizmini jonli savdoga ulamaydi; Telegram ichida Stars/Premium/Gift uchun bunday checkout Telegram to'lov qoidalariga zid (https://core.telegram.org/bots/payments-stars). O'z tasdiqlangan merchant saytingizda qonuniy/tegishli foydalanish holatlarida UZS to'lovi kerak bo'lsa rasmiy provider webhooki zarur; shunchaki HUMO chatbot xabari yetarli emas.

## Monitoring va xatolar

- `/health` — bazaga ulanish holati; maxfiy kalitlar ko'rsatilmaydi.
- Render Runtime Logs — xatolar va webhookni o'rnatish.
- Admin panel → 📦 Buyurtmalar — `review` holatida provider natijasini avval hamyon/servis orqali tekshiring; **hech qachon pul bilan bog'liq buyurtmani ko'r-ko'rona qayta yubormang**.
- `payment_receipts` jadvali `telegram_payment_charge_id`ni bir marta saqlaydi. Bir xil webhook takrorlanganida yangi yetkazish boshlanmaydi.
- Render Persistent Disk backup'larini yoqing; SQLite fayli va WAL nusxalari alohida zaxira qilinishi kerak. Diskni olib tashlamang.

## Offline tekshiruv

`python test_business_logic.py` — haqiqiy Telegram, Fragment yoki HUMO xizmatiga ulanmasdan mantiqiy testlar (fake bot). `python -m py_compile bot.py wsgi.py create_session.py` — sintaksis.

## API hujjatlari

- fragment-api.uz: https://fragment-api.uz/docs (OpenAPI `/api/v1/getInfo`, `/api/v1/stars/buy`, `/api/v1/premium/buy`, `/api/v1/wallet/balance`)
- Telegram: https://core.telegram.org/bots/api va https://core.telegram.org/bots/payments-stars
- Render: https://render.com/docs/blueprint-spec, https://render.com/docs/disks
