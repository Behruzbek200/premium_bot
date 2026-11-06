# V4 yangiliklar

- Render Blueprint `render.yaml`: paid Starter, 1GB persistent disk, health check, native Python.
- `wsgi.py` / `gunicorn.conf.py`: yagona ishchi, webhook va fon ishchilarini avtomatik ishga tushirish.
- `RENDER_EXTERNAL_URL` orqali avtomatik webhook; secret header, setup retry.
- DB disk topilmasa ishga tushmaslik, muhim checkout ma'lumotlarini yo'qotmaslik.
- Stars/Premium qabul qiluvchi username'ini API orqali to'lovdan AVVAL tekshirish.
- XTR to'lov receipts uchun yangi `payment_receipts` ledger; charge ID UNIQUE.
- Kutilmagan kech to'lov yoki ikki marta bosilgan yangi charge uchun xavfsiz refund navbati.
- Provider timeout xatosidan keyin ikki marta yetkazishni boshlamaslik.
- HUMO StringSession faqat audit; bank xabarnomasi tasdiqlangan merchant tranzaksiyasi emas.
- UZS kartaga to'lov raqamli mahsulotlar uchun foydalanuvchi menyusidan o'chirildi; so'm narxlar saqlanadi, haqiqiy checkout Telegram XTR.
- Demo narxlar va sotuv OFF default, real kalit bilan sinov talab qilinadi.
