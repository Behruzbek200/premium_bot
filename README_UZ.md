# Premium Market V6 — Supabase PostgreSQL + Render

## 1. Supabase
1. https://supabase.com/dashboard -> New Project -> bazani yarating.
2. Project -> **Connect** -> **Session pooler (port 5432)** -> Connection string (URI).
3. Parol joyini Supabase **database password** bilan almashtiring. Maxsus belgilarni URL-encode qiling.
4. Jadvallarni qo‘lda yaratish shart emas: bot ilk startup'da `CREATE TABLE IF NOT EXISTS` bilan yaratadi.

## 2. Render
1. GitHub repo asosiy katalogiga ZIP tarkibini joylang.
2. Render -> New -> Blueprint -> repo -> deploy. `render.yaml` ishlatiladi.
3. Env: `BOT_TOKEN`, `OWNER_ID`, `DATABASE_URL`. `FRAGMENT_API_KEY` ixtiyoriy.
4. `WEBHOOK_SECRET` Render Blueprint tomonidan avtomatik yaratiladi; `RENDER_EXTERNAL_URL`ni Render belgilaydi.
5. Health: `https://<render-domen>/health` -> `database_ready: true`.
6. Telegram `/start`, `/admin` va Gift katalogini sinang.

**Supabase project URL, anon key yoki service_role key EMAS, aynan PostgreSQL Connection URI kerak!**

## Muhim xavfsizlik va moslashuv
- Ushbu versiyada eski SQLite (`market.sqlite3`) avtomatik Supabase'ga ko‘chirilmaydi: yangi baza bo‘sh boshlanadi. Eski ma'lumotlarni yo‘qotmaslik uchun SQLite nusxasini saqlang.
- Render persistent disk kerak emas. `DATABASE_URL` oshkor qilinmasin, GitHub'ga yuklamang.
- Faqat bitta Gunicorn worker ishlating (fon ishchilari takrorlanmasligi uchun).
- V5 funksiyalari saqlanadi. HUMO orqali to‘lov navbati **sinov/audit**, real balansga avtomatik qo‘shilmaydi.
- Xaridor Stars, Premium va Gift uchun kartadan bot ichida so‘m to‘lashi ruxsat etilmaydi. V5 xaridor to‘lovi (va live delivery) o‘chiq qoladi; faqat adminning bot Stars to‘ldirish XTR invoice'i mavjud.
- To‘lovni va mahsulotni real yetkazishni jonli integratsiya bilan tekshirmasdan savdoni yoqmang.
- Eski oshkor bo‘lgan BotFather tokeningizni yangilang.

## Windows lokal sinash
```powershell
pip install -r requirements.txt
copy .env.example .env
# .env qiymatlarini to‘ldiring
python bot.py
```

Bot `public` jadvallarida RLS avtomatik yoqiladi. `anon` kalit bilan bu jadvallarga siyosatsiz kira bo‘lmaydi. Baza uchun `DATABASE_URL`dagi Postgres egasi huquqlari ishlatiladi.

## Ma’lumotlar jadvallari
`settings`, `users`, `sessions`, `bundles`, `rules`, `gifts`, `orders`, `admin_funding`, `payment_receipts`, `admins`, `required_channels`, `broadcasts`, `humo_requests`, `humo_events`, `uzs_queue`.
