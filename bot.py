# -*- coding: utf-8 -*-
"""Stars, Premium, 11 Gifts, referral and HUMO shop — Render + Supabase.

Single application file. Live sales REQUIRES a permanent PostgreSQL DATABASE_URL,
a stable BOT_TOKEN and verified real payment/delivery tests. PostgreSQL serializes balance
mutations across workers via transaction-scoped advisory locks. Local SQLite is
only used for offline demonstrations when DATABASE_URL is absent and live sales
are disabled. @HUMOcardbot via Telethon StringSession is a bank *notification*,
NOT a bank-signed payment confirmation; automatic credits remain a financial risk.
Telegram's policy for in-bot digital products requires Telegram Stars (XTR).
Fragment API contract: https://fragment-api.uz/docs/openapi.json

Start: python bot.py --render-start
"""

import os
import re
import json
import time
import math
import uuid
import html
import hmac
import hashlib
import secrets
import logging
import contextvars
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import psycopg2
from psycopg2 import extras as pg_extras
from psycopg2.pool import ThreadedConnectionPool
import threading
import asyncio
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from contextlib import contextmanager

import requests
import telebot
from telebot import types
from flask import Flask, request, jsonify
from cryptography.fernet import Fernet, InvalidToken
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# Render -> Web Service -> Environment bo'limida kiriting.
# Tokenni GitHub yoki ushbu faylga kiritmang.
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))  # Bosh adminning Telegram ID raqami

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Live real money must not use a volatile SQLite file on Render Free.
# DATABASE_URL is Supabase's *rotated* Postgres pooler URL; store only in Render ENV.
DATABASE_URL = os.getenv('DATABASE_URL','').strip()
USE_POSTGRES = bool(DATABASE_URL)
LOCAL_DEMO_DB = os.path.join(BASE_DIR, 'demo_only.sqlite3')
LIVE_SALES_ENABLED = os.getenv('ENABLE_LIVE_SALES', '0') == '1'
RENDER_HOST = os.getenv('RENDER_EXTERNAL_HOSTNAME', '').strip()
PUBLIC_URL = (os.getenv('PUBLIC_URL') or ('https://' + RENDER_HOST if RENDER_HOST else '')).rstrip('/')

FRAGMENT_BASE = 'https://fragment-api.uz/api/v1'
PAYMENT_PROVIDER = 'bank_session'  # Faqat HUMO String Session
# Bank xabarlari imzolangan bank API tasdig'i emas.
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID','0'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH','').strip()
TELETHON_STRING_SESSION = os.getenv('TELETHON_STRING_SESSION','').strip()
# ID kiritish shart emas: Telethon @HUMOcardbot username orqali aniq topadi.
HUMO_BOT_USERNAME = os.getenv('HUMO_BOT_USERNAME','HUMOcardbot').strip().lstrip('@')
HUMO_SENDER_ID = int(os.getenv('HUMO_SENDER_ID','0'))
HUMO_CARD_LAST4 = os.getenv('HUMO_CARD_LAST4','').strip()
HUMO_AUTO_CREDIT = os.getenv('HUMO_AUTO_CREDIT','1') == '1'
HUMO_LISTENER_STARTED = threading.Event()
HUMO_LISTENER_CONNECTED = threading.Event()

REQUEST_TIMEOUT = (3, 12)

if not BOT_TOKEN or OWNER_ID <= 0:
    raise SystemExit('Render Environment: BOT_TOKEN va OWNER_ID ni kiriting!')
if DATABASE_URL and not DATABASE_URL.startswith(('postgresql://','postgres://')):
    raise SystemExit('DATABASE_URL PostgreSQL ulanish manzili bo‘lishi kerak.')
if LIVE_SALES_ENABLED and not USE_POSTGRES:
    raise SystemExit('ENABLE_LIVE_SALES=1 uchun PostgreSQL DATABASE_URL majburiy.')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('stars_shop')
# Webhookdagi haqiqiy Telegram javoblarini xavfsiz diagnostika qilish.
_WEBHOOK_METRICS = contextvars.ContextVar('webhook_metrics', default=None)
# Process updates from the same Telegram chat sequentially; different chats parallel.
_CHAT_PROCESS_LOCKS = [threading.Lock() for _ in range(256)]
# Tashqi Fragment so'rovi HTTP request threadini to'smaydi.
_PURCHASE_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix='fragment-purchase')
_PURCHASE_CAPACITY = threading.BoundedSemaphore(8)

# Qisqa muddatli sozlamalar keshi, global/maxfiy sirlarni log qilmaymiz.
_SETTING_CACHE = {}
_SETTING_CACHE_LOCK = threading.Lock()
_SETTINGS_TTL = 12.0
_USER_TOUCH_CACHE = {}
_USER_TOUCH_LOCK = threading.Lock()
_CHANNEL_CACHE = {'expires': 0.0, 'rows': ()}
_CHANNEL_CACHE_LOCK = threading.Lock()
_BOT_USERNAME = None
_HTTP_LOCAL = threading.local()


def http_session():
    s = getattr(_HTTP_LOCAL, 'session', None)
    if s is None:
        s = requests.Session()
        _HTTP_LOCAL.session = s
    return s

bot = telebot.TeleBot(BOT_TOKEN, threaded=False, parse_mode='HTML')
app = Flask(__name__)


def now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def money(amount):
    return f'{int(amount):,}'.replace(',', ' ') + " so'm"


def esc(s):
    return html.escape(str(s or ''))






# Avoid opening a fresh Supabase TLS connection for every menu/DB query.
# One thread-safe pool per Render worker process. Transactions are still
# committed or rolled back on each request before returning connections.
_PG_POOL = None
_PG_POOL_PID = None
_PG_POOL_LOCK = threading.Lock()


def _pg_pool():
    global _PG_POOL, _PG_POOL_PID
    pid = os.getpid()
    with _PG_POOL_LOCK:
        if _PG_POOL is None or _PG_POOL_PID != pid:
            _PG_POOL = ThreadedConnectionPool(
                minconn=1, maxconn=12, dsn=DATABASE_URL,
                connect_timeout=12, application_name='stars-premium-gift-shop'
            )
            _PG_POOL_PID = pid
        return _PG_POOL


class PGConnection:
    """SQLite-shaped DB methods backed by psycopg2 and PostgreSQL.

    All balance/payment/order writes take one transaction-scoped advisory lock,
    serializing transactions across threads/processes. SQL placeholders are
    converted from SQLite ? to PostgreSQL %s. This also supports old schema.
    """
    def __init__(self):
        self._pool = _pg_pool()
        self.conn = self._pool.getconn()
        self.cur = None
        self.lastrowid = None
        self.rowcount = -1

    def execute(self, statement, params=()):
        q = statement.strip()
        if q.upper() in ('BEGIN IMMEDIATE', 'BEGIN WEBHOOK_LOCK'):
            self.cur = self.conn.cursor(cursor_factory=pg_extras.RealDictCursor)
            key = 34889021171 if q.upper() == 'BEGIN IMMEDIATE' else 34889021172
            self.cur.execute('SELECT pg_advisory_xact_lock(%s)', (key,))
            return self
        if q.upper().startswith('PRAGMA '):
            return self
        if re.match(r'^INSERT\s+OR\s+IGNORE\s+INTO\s+',q,re.I):
            q=re.sub(r'^INSERT\s+OR\s+IGNORE\s+INTO\s+','INSERT INTO ',q,flags=re.I)
            q=q.rstrip(';')+' ON CONFLICT DO NOTHING'
        if re.match(r'^INSERT\s+OR\s+REPLACE\s+INTO\s+packages\b',q,re.I):
            q=re.sub(r'^INSERT\s+OR\s+REPLACE\s+INTO\s+','INSERT INTO ',q,flags=re.I)
            q=q.rstrip(';')+' ON CONFLICT(amount) DO UPDATE SET enabled=EXCLUDED.enabled'
        returns_ticket_id=bool(re.match(r'^INSERT\s+INTO\s+tickets\b',q,re.I))
        if returns_ticket_id:
            q=q.rstrip(';')+' RETURNING id'
        if self.cur is not None:
            self.cur.close()
        self.cur=self.conn.cursor(cursor_factory=pg_extras.RealDictCursor)
        # psycopg2 uses '%' as a formatting marker. SQL LIKE patterns such as
        # 'BANK:%' and 'channel_%' must be escaped before ? -> %s conversion.
        # Only double SQL literal '%' when psycopg2 is formatting parameters.
        # With zero parameters a LIKE 'channel_%' pattern must remain intact.
        has_params = bool(params)
        if has_params:
            q = q.replace('%','%%')
        q=q.replace('?','%s')
        self.cur.execute(q,params if has_params else None)
        self.rowcount=self.cur.rowcount
        self.lastrowid=self.cur.fetchone()['id'] if returns_ticket_id else None
        return self

    def executemany(self,statement,rows):
        for x in rows:self.execute(statement,x)
        return self

    def executescript(self,script):
        # Postgres uses BIGSERIAL instead of SQLite AUTOINCREMENT.
        sql=re.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT','BIGSERIAL PRIMARY KEY',script,flags=re.I)
        # Telegram user IDs are 64-bit, not 32-bit PostgreSQL INTEGER.
        sql=re.sub(r'\bINTEGER\b','BIGINT',sql,flags=re.I)
        for statement in sql.split(';'):
            if statement.strip():self.execute(statement)

    def fetchone(self):
        return self.cur.fetchone() if self.cur is not None else None
    def fetchall(self):
        return self.cur.fetchall() if self.cur is not None else []
    def __iter__(self):
        """Match sqlite3.Cursor iteration: c.execute(SELECT ...) is iterable."""
        if self.cur is None:
            return iter(())
        return iter(self.cur)
    def commit(self):self.conn.commit()
    def rollback(self):self.conn.rollback()
    def close(self):
        try:
            if self.cur is not None:
                self.cur.close()
        finally:
            # Discard dead connections; healthy ones are reused.
            self._pool.putconn(self.conn, close=bool(self.conn.closed))


def db_open():
    if USE_POSTGRES:return PGConnection()
    conn=sqlite3.connect(LOCAL_DEMO_DB,timeout=30)
    conn.row_factory=sqlite3.Row
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


DB_INTEGRITY_ERRORS = (sqlite3.IntegrityError, psycopg2.IntegrityError)


@contextmanager
def db(write=False, financial=False, webhook_lock=False):
    """Short PostgreSQL transactions. Global lock ONLY for money or update claims.

    PostgreSQL starts a transaction implicitly on first execute. The old code
    acquired one advisory lock for EVERY write, blocking /start and buttons
    behind slow purchases. SQLite retains BEGIN IMMEDIATE for test/demo mode.
    """
    c = db_open()
    try:
        if write:
            if not USE_POSTGRES:
                c.execute('BEGIN IMMEDIATE')
            elif financial:
                c.execute('BEGIN IMMEDIATE')  # serialized financial ledger
            elif webhook_lock:
                c.execute('BEGIN WEBHOOK_LOCK')  # separate, short-lived lock
        yield c
        if write or USE_POSTGRES:
            c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def setup_db():
    c = db_open()
    try:
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript('''
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT '',
          username TEXT NOT NULL DEFAULT '', balance INTEGER NOT NULL DEFAULT 0,
          banned INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, val TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS admins (id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS states (user_id INTEGER PRIMARY KEY, step TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS packages (amount INTEGER PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS orders (
          id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, kind TEXT NOT NULL,
          qty INTEGER NOT NULL, recipient TEXT NOT NULL, price INTEGER NOT NULL,
          status TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created TEXT NOT NULL, updated TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_orders_user ON orders(user_id,created);
        CREATE TABLE IF NOT EXISTS payments (
          id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, amount INTEGER NOT NULL,
          provider_id TEXT UNIQUE, card TEXT NOT NULL DEFAULT '',
          card_holder TEXT NOT NULL DEFAULT '', expires_at INTEGER,
          status TEXT NOT NULL, created TEXT NOT NULL, updated TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_pay_user ON payments(user_id,created);
        CREATE TABLE IF NOT EXISTS bank_events (
          sender_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
          tx_ref TEXT NOT NULL UNIQUE, amount BIGINT NOT NULL,
          payment_id TEXT NOT NULL, status TEXT NOT NULL,
          created TEXT NOT NULL, PRIMARY KEY(sender_id,message_id));
        CREATE INDEX IF NOT EXISTS ix_bank_events_payment ON bank_events(payment_id);
        CREATE TABLE IF NOT EXISTS gifts (
          slot INTEGER PRIMARY KEY CHECK(slot BETWEEN 1 AND 11),
          name TEXT NOT NULL, emoji TEXT NOT NULL,
          price INTEGER NOT NULL DEFAULT 0,
          gift_id TEXT NOT NULL DEFAULT '',
          sticker_file_id TEXT NOT NULL DEFAULT '',
          stars_cost INTEGER NOT NULL DEFAULT 0,
          enabled INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS referrals (
          user_id INTEGER PRIMARY KEY, referrer_id INTEGER NOT NULL,
          created TEXT NOT NULL, CHECK(user_id != referrer_id));
        CREATE INDEX IF NOT EXISTS ix_referrals_parent ON referrals(referrer_id);
        CREATE TABLE IF NOT EXISTS bot_star_deposits (
          charge_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL,
          stars INTEGER NOT NULL, created TEXT NOT NULL);

        CREATE TABLE IF NOT EXISTS ledger (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
          delta INTEGER NOT NULL, reason TEXT NOT NULL, ref TEXT NOT NULL,
          created TEXT NOT NULL, UNIQUE(reason,ref));
        CREATE TABLE IF NOT EXISTS tickets (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'open', created TEXT NOT NULL, updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS ticket_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER NOT NULL,
          sender_id INTEGER NOT NULL, body TEXT NOT NULL, created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS telegram_updates (
          update_id INTEGER PRIMARY KEY, status TEXT NOT NULL, arrived TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS broadcast_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
          from_chat INTEGER NOT NULL, source_message INTEGER NOT NULL,
          target_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          created TEXT NOT NULL, UNIQUE(job_id,target_id));
        CREATE INDEX IF NOT EXISTS ix_broadcast_queue ON broadcast_queue(status,id);
        ''')
        defaults = {
          'star_price': '0', 'premium_3': '0', 'premium_6': '0', 'premium_12': '0',
          'usd_rate': '0', 'min_topup': '5000', 'max_topup': '10000000',
          'min_stars': '50', 'max_stars': '100000', 'maintenance': '0',
          'welcome': '',
          'public_url':'', 'payment_enabled':'1',
          'fragment_key': '', 'referral_percent': '5', 'referral_enabled': '1',
          'payment_card_number': '', 'payment_card_name': '',
          'telegram_webhook_path': secrets.token_urlsafe(26),
          'telegram_header_secret': secrets.token_urlsafe(32),
          'payment_monitor_token': secrets.token_urlsafe(30)
        }
        for k,v in defaults.items():
            c.execute('INSERT OR IGNORE INTO settings(key,val) VALUES(?,?)', (k,v))
        # Oldingi SQLite bazalariga yangilanish (ma'lumotlar o'chirilmaydi).
        if USE_POSTGRES:
            paycols = {r['column_name'] for r in c.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='payments'"
            ).fetchall()}
        else:
            paycols = {r[1] for r in c.execute('PRAGMA table_info(payments)').fetchall()}
        if 'card_holder' not in paycols:
            c.execute("ALTER TABLE payments ADD COLUMN card_holder TEXT NOT NULL DEFAULT ''")
        if 'expires_at' not in paycols:
            c.execute('ALTER TABLE payments ADD COLUMN expires_at INTEGER')
        if USE_POSTGRES:
            usercols={r['column_name'] for r in c.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='users'"
            ).fetchall()}
        else:
            usercols={x[1] for x in c.execute('PRAGMA table_info(users)').fetchall()}
        if 'referral_balance' not in usercols:
            c.execute('ALTER TABLE users ADD COLUMN referral_balance INTEGER NOT NULL DEFAULT 0')
        names=[('🌹','Atirgul'),('🧸','Ayiqcha'),('💝','Yurakcha'),('🌸','Gul'),
               ('🎂','Tort'),('⭐','Yulduz'),('💍','Uzuk'),('🏆','Kubok'),
               ('🚀','Raketa'),('🎈','Sharlar'),('🎁','Sovg‘a')]
        for i,(emoji,name) in enumerate(names,1):
            c.execute('INSERT OR IGNORE INTO gifts(slot,name,emoji) VALUES(?,?,?)',(i,name,emoji))
        for qty in (50,100,250,500,1000,2500,5000,10000):
            c.execute('INSERT OR IGNORE INTO packages(amount) VALUES(?)', (qty,))
        c.commit()
    finally:
        c.close()


def get(k, default=''):
    instant = time.monotonic()
    with _SETTING_CACHE_LOCK:
        cached = _SETTING_CACHE.get(k)
        if cached and cached[1] > instant:
            return cached[0] if cached[0] is not None else default
    with db() as c:
        r = c.execute('SELECT val FROM settings WHERE key=?',(k,)).fetchone()
    value = r['val'] if r else None
    with _SETTING_CACHE_LOCK:
        if len(_SETTING_CACHE) > 512:
            _SETTING_CACHE.clear()
        _SETTING_CACHE[k] = (value, instant + _SETTINGS_TTL)
    return value if value is not None else default


def put(k, v):
    with db(True) as c:
        c.execute('INSERT INTO settings(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val',(k,str(v)))
    with _SETTING_CACHE_LOCK:
        _SETTING_CACHE[k] = (str(v), time.monotonic() + _SETTINGS_TTL)
    if str(k).startswith('channel_'):
        with _CHANNEL_CACHE_LOCK:
            _CHANNEL_CACHE['expires'] = 0.0


def channel_rows():
    instant = time.monotonic()
    with _CHANNEL_CACHE_LOCK:
        if _CHANNEL_CACHE['expires'] > instant:
            return list(_CHANNEL_CACHE['rows'])
    with db() as c:
        rows = c.execute("SELECT key,val FROM settings WHERE key LIKE 'channel_%' ORDER BY key").fetchall()
    with _CHANNEL_CACHE_LOCK:
        _CHANNEL_CACHE['rows'] = tuple(rows)
        _CHANNEL_CACHE['expires'] = time.monotonic() + _SETTINGS_TTL
    return rows


def bot_username():
    global _BOT_USERNAME
    if not _BOT_USERNAME:
        _BOT_USERNAME = bot.get_me().username
    return _BOT_USERNAME


def param(k, fallback=0):
    try: return int(get(k, str(fallback)))
    except (TypeError, ValueError): return fallback


def stars_total(qty, unit_price=None):
    """Barcha Stars tariflari bitta 1⭐ narxidan hisoblanadi."""
    if unit_price is None:
        unit_price = param('star_price')
    return int(qty) * int(unit_price)


def init_cipher():
    """Stable Fernet key without extra ENV settings.

    Existing APP_SECRET is still accepted for backwards compatibility, but
    new installations need only BOT_TOKEN. Since rotating BOT_TOKEN changes
    the derived key, re-enter Fragment API Key in /admin after token rotation.
    """
    secret = os.getenv('APP_SECRET', '').strip()  # optional legacy setting
    if not secret:
        import base64
        secret = base64.urlsafe_b64encode(
            hashlib.sha256(b'stars-humo-local-fernet-v1:' + BOT_TOKEN.encode()).digest()
        ).decode()
        logger.warning(
            'Fragment kaliti BOT_TOKEN dan olingan kalit bilan shifrlanadi. '
            'BOT_TOKEN o‘zgarsa /admin orqali Fragment API Key ni qayta kiriting.'
        )
    try:
        return Fernet(secret.encode())
    except (ValueError, TypeError) as exc:
        raise SystemExit('Oldingi ixtiyoriy APP_SECRET noto‘g‘ri formatda.') from exc


CIPHER = None
SECURE_KEYS = {'fragment_key'}


def set_secret(k,v):
    if k not in SECURE_KEYS: raise ValueError('secret setting')
    put(k, 'enc:' + CIPHER.encrypt(v.encode()).decode())


def read_secret(k):
    value = get(k)
    if not value: return ''
    if not value.startswith('enc:'):
        return ''  # eski plaintext qiymatlarni yuklamang
    try: return CIPHER.decrypt(value[4:].encode()).decode()
    except InvalidToken:
        logger.error('Shifrlash kaliti topilmadi: %s', k)
        return ''


def mask_secret(k):
    v=read_secret(k)
    return '✅ ••••'+v[-4:] if v else '❌ Kiritilmagan'


def is_admin(uid):
    if uid == OWNER_ID: return True
    with db() as c:
        return c.execute('SELECT 1 FROM admins WHERE id=?',(uid,)).fetchone() is not None


def only_owner(uid):
    return uid == OWNER_ID


def admin_ids():
    with db() as c: ids=[r['id'] for r in c.execute('SELECT id FROM admins').fetchall()]
    return list(set(ids+[OWNER_ID]))


def notify_admin(text, markup=None):
    for uid in admin_ids():
        try: bot.send_message(uid,text,reply_markup=markup)
        except Exception as e: logger.info('Admin xabar yetmadi (%s): %s',uid,type(e).__name__)


def safe_send(uid, text, markup=None):
    # Old version silently returned None, so webhook returned 200 although
    # Telegram rejected the bot's reply (400/403/429/etc).
    metrics = _WEBHOOK_METRICS.get()
    try:
        result = bot.send_message(uid, text, reply_markup=markup,
                                  disable_web_page_preview=True)
        if metrics is not None:
            metrics['sent'] += 1
        return result
    except Exception as exc:
        if metrics is not None:
            metrics['failed'] += 1
        details = str(exc).replace(BOT_TOKEN, '[REDACTED]')[:350]
        logger.error('Telegram sendMessage FAILED chat=%s type=%s details=%s',
                     uid, type(exc).__name__, details)
        return None


def user_record(u):
    if not u: return
    uid = u.id
    name, username = (u.first_name or '')[:100], (u.username or '')[:100]
    stamp = time.monotonic()
    with _USER_TOUCH_LOCK:
        old = _USER_TOUCH_CACHE.get(uid)
        if old and old[0] == name and old[1] == username and old[2] > stamp:
            return
    with db(True) as c:
        c.execute('''INSERT INTO users(id,name,username,created) VALUES(?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET name=excluded.name,username=excluded.username
          WHERE users.name IS DISTINCT FROM excluded.name OR users.username IS DISTINCT FROM excluded.username''',
                  (uid, name, username,now()))
    with _USER_TOUCH_LOCK:
        if len(_USER_TOUCH_CACHE) > 8192:
            _USER_TOUCH_CACHE.clear()
        _USER_TOUCH_CACHE[uid] = (name, username, time.monotonic()+30.0)


def user_row(uid):
    with db() as c: return c.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()


def set_state(uid,step,data=None):
    with db(True) as c:
        c.execute('INSERT INTO states(user_id,step,data) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET step=excluded.step,data=excluded.data',
                  (uid,step,json.dumps(data or {},ensure_ascii=False)))


def state(uid):
    with db() as c:
        r=c.execute('SELECT * FROM states WHERE user_id=?',(uid,)).fetchone()
    return (r['step'],json.loads(r['data'])) if r else (None,{})


def clear_state(uid):
    with db(True) as c: c.execute('DELETE FROM states WHERE user_id=?',(uid,))


def keyboard(rows):
    k=types.InlineKeyboardMarkup(row_width=2)
    for row in rows:
        k.row(*[types.InlineKeyboardButton(label,callback_data=data) for label,data in row])
    return k


def menu_kb(admin=False):
    k=types.ReplyKeyboardMarkup(resize_keyboard=True)
    if admin:
        buttons=[('⭐ Stars narxlari','💎 Premium narxlari'),('🎁 Gift boshqaruvi','👥 Referal sozlamalari'),('⭐ Bot Stars balansi','📦 Buyurtmalar'),('👥 Foydalanuvchilar',),
                 ('📊 Statistika','💳 To‘lovlar'),('🔌 API va to‘lov','📢 Xabar yuborish'),
                 ('📣 Majburiy obuna','👑 Adminlar'),('🎧 Murojaatlar','⚙️ Sozlamalar'),('🏠 User menyu',)]
    else:
        buttons=[('⭐ Stars olish','💎 Premium olish'),('🎁 Gift olish','👥 Referal'),('💳 Hisob to‘ldirish','💰 Hisobim'),
                 ('📦 Buyurtmalarim','🎧 Yordam')]
    for row in buttons: k.row(*[types.KeyboardButton(s) for s in row])
    return k


# Reply keyboard show, change through /admin, no public admin buttons.
def main_menu(uid):
    clear_state(uid)
    r=user_row(uid)
    name=esc(r['name'] if r else 'do‘stimiz')
    greeting=get('welcome').strip() or f'Assalomu alaykum, <b>{name}</b>! 👋\n\n⭐ Stars, 💎 Premium va 🎁 Gift xarid qilish botiga xush kelibsiz!'
    safe_send(uid,greeting+'\n\n🛍 Kerakli bo‘limni tanlang:',menu_kb())


def admin_menu(uid):
    clear_state(uid)
    safe_send(uid,'👑 <b>ADMIN PANEL</b>\n\nKerakli bo‘limni tanlang.',menu_kb(admin=True))


def needed_subscription(uid):
    channels = channel_rows()
    if not channels or is_admin(uid): return []
    missing=[]
    for r in channels:
        val=r['val']
        if not val: continue
        parts=val.split('|',1)
        chat=parts[0]
        link=parts[1] if len(parts)>1 else ''
        try:
            cm=bot.get_chat_member(chat,uid)
            if cm.status not in ('creator','administrator','member') and not (cm.status=='restricted' and getattr(cm,'is_member',False)):
                missing.append((chat,link))
        except Exception:
            # kanal adminligi sozlanmagan bo'lishi mumkin; adminlarga xabar beriladi
            missing.append((chat,link))
    return missing


def sub_prompt(uid, missing):
    k=types.InlineKeyboardMarkup()
    for chat,link in missing:
        url=link or ('https://t.me/'+chat.lstrip('@') if chat.startswith('@') else '')
        if url.startswith('https://t.me/'):
            k.add(types.InlineKeyboardButton('📢 Kanalga obuna bo‘lish',url=url))
    k.add(types.InlineKeyboardButton('✅ Obunani tekshirish',callback_data='sub:check'))
    safe_send(uid,'🔒 Botdan foydalanish uchun kanal(lar)ga obuna bo‘ling.',k)


def check_access(uid):
    u=user_row(uid)
    if u and u['banned'] and uid!=OWNER_ID:
        safe_send(uid,'🚫 Hisobingiz vaqtincha bloklangan.'); return False
    missing=needed_subscription(uid)
    if missing:
        sub_prompt(uid,missing); return False
    if get('maintenance')=='1' and not is_admin(uid):
        safe_send(uid,'🛠 Botda texnik xizmat ishlari olib borilmoqda.'); return False
    return True


def stars_menu(uid):
    if not check_access(uid): return
    price=param('star_price')
    if price<=0:
        safe_send(uid,'⭐ Stars narxlari hali administrator tomonidan belgilanmagan.');return
    with db() as c: packs=[r['amount'] for r in c.execute('SELECT amount FROM packages WHERE enabled=1 ORDER BY amount').fetchall()]
    rows=[]
    for i in range(0,len(packs),2):
        rows.append([(f'⭐ {x:,} — {money(stars_total(x, price))}',f's:pack:{x}') for x in packs[i:i+2]])
    rows += [[('🔢 Boshqa miqdor','s:custom')],[('🏠 Bosh menyu','home')]]
    safe_send(uid,f'⭐ <b>Stars olish</b>\n\n1 ⭐ = <b>{money(price)}</b>\nTo‘plamni tanlang:',keyboard(rows))


def premium_menu(uid):
    if not check_access(uid):return
    rows=[]
    for months in (3,6,12):
        price=param(f'premium_{months}')
        if price>0: rows.append([(f'💎 {months} oy — {money(price)}',f'p:pack:{months}')])
    if not rows: safe_send(uid,'💎 Premium narxlari hali sozlanmagan.');return
    rows.append([('🏠 Bosh menyu','home')])
    safe_send(uid,'💎 <b>Telegram Premium</b>\n\nObuna muddatini tanlang:',keyboard(rows))


def choose_target(uid, kind, qty):
    if kind=='stars':
        minq,maxq=param('min_stars'),param('max_stars')
        if not minq<=qty<=maxq or param('star_price')<=0: safe_send(uid,'❌ Miqdor yoki narx xato.');return
        total=stars_total(qty); title=f'⭐ {qty} Stars'
    else:
        if qty not in (3,6,12) or param(f'premium_{qty}')<=0:return
        total=param(f'premium_{qty}');title=f'💎 {qty} oylik Premium'
    set_state(uid,'choose_target',{'kind':kind,'qty':qty,'total':total})
    safe_send(uid,f'<b>{title}</b>\n💰 Narx: <b>{money(total)}</b>\n\nKim uchun xarid qilasiz?',
              keyboard([[('👤 O‘zim uchun','t:self'),('🎁 Boshqaga','t:other')],[('❌ Bekor qilish','home')]]))


def extract_username(s):
    s=(s or '').strip()
    if s.startswith('https://t.me/') or s.startswith('http://t.me/'):
        s=s.split('t.me/',1)[1].split('?',1)[0].split('/',1)[0]
    elif s.startswith('t.me/'):
        s=s[5:].split('?',1)[0].split('/',1)[0]
    s=s.lstrip('@')
    if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{4,31}',s):return s
    return ''


def preview_recipient(uid, username):
    step,data=state(uid)
    if step!='enter_recipient':return
    api_key=read_secret('fragment_key')
    if not api_key:
        safe_send(uid,'❌ Fragment API kaliti sozlanmagan.');clear_state(uid);return
    safe_send(uid,'🔎 Telegram foydalanuvchisi tekshirilmoqda...')
    try:
        j=fragment('/getInfo',{'username':username})
        if j.get('ok') is not True: raise ValueError(str(j.get('message') or 'Profil topilmadi'))
        profile=j.get('result') or {}
        resolved=extract_username(str(profile.get('username') or username))
        if resolved.lower()!=username.lower(): raise ValueError('Qabul qiluvchi username mos kelmadi')
    except Exception as e:
        safe_send(uid,'❌ Qabul qiluvchi tekshirilmadi: '+esc(str(e))[:300]+'\nUsername yoki havolani qayta yuboring.');return
    data['recipient']=username
    set_state(uid,'review_order',data)
    r=user_row(uid)
    title=f"⭐ {data['qty']} Stars" if data['kind']=='stars' else f"💎 {data['qty']} oylik Premium"
    safe_send(uid,f'🧾 <b>Buyurtmani tekshiring</b>\n\n📦 {title}\n👤 Qabul qiluvchi: @{esc(username)}'
             f"\n💰 Narx: <b>{money(data['total'])}</b>\n💳 Balans: {money(r['balance'] if r else 0)}"
             '\n\n✅ Tasdiqlasangiz balansingizdan pul yechilib, buyurtma avtomatik yuboriladi.',
             keyboard([[('✅ Tasdiqlash','buy:confirm')],[('💳 Hisob to‘ldirish','top:menu'),('❌ Bekor','home')]]))


def fragment(path,payload):
    key=read_secret('fragment_key')
    if not key: raise RuntimeError('Fragment API Key kiritilmagan')
    try:
        res=http_session().post(FRAGMENT_BASE+path,json=payload,
            headers={'X-API-Key':key,'Content-Type':'application/json'},timeout=REQUEST_TIMEOUT)
        data=res.json()
    except requests.RequestException as e:
        raise RuntimeError('Fragment bilan tarmoq aloqasi uzildi') from e
    except ValueError as e:
        raise RuntimeError('Fragment javobi JSON emas') from e
    if not isinstance(data,dict):raise RuntimeError('Fragment javobi noto‘g‘ri')
    return data


def safe_price_check(kind,qty,sell):
    # API tannarxiga nisbatan zararli narx qo'yilishini cheklaydi.
    usd_rate=param('usd_rate')
    if usd_rate<=0: return False,'Admin USD/UZS kursini kiritmagan.'
    try:
        j=fragment('/stars/pricing',{'amount':qty}) if kind=='stars' else fragment('/premium/pricing',{})
        if j.get('ok') is not True: return False,str(j.get('message','Tannarx olinmadi'))
        result=j.get('result') or {}
        if kind=='stars':
            usd=Decimal(str((result.get('price') or {}).get('usd','')))
        else:
            package=next((x for x in result.get('packages',[]) if int(x.get('months',0))==qty),None)
            usd=Decimal(str(package.get('usd',''))) if package else Decimal('0')
        if usd<=0: return False,'API tannarxi aniqlanmadi.'
        # 2% bufer (1.5% komissiya va dumaloqlashdan himoya), qo'shimcha TON gas alohida
        minimum=math.ceil(usd*Decimal(usd_rate)*Decimal('1.02'))
        if sell<minimum:
            return False,f'Sotuv narxi past: {money(sell)}. Taxminiy tannarx+komissiya: {money(minimum)}.'
        return True,''
    except (InvalidOperation,TypeError,ValueError) as e:
        return False,'Narx formati xato: '+str(e)[:60]
    except Exception as e:
        return False,'Tannarx tekshiruvi ishlamadi: '+str(e)[:110]


def debit_and_create(uid,data):
    if not LIVE_SALES_ENABLED:
        return None, 'Savdo o‘chirilgan. Render ENV: ENABLE_LIVE_SALES=1 qiling.'
    # Bir userning ikkita confirm bosishi bitta orderga aylanadi.
    oid='O'+uuid.uuid4().hex[:12].upper()
    with db(True, financial=True) as c:
        r=c.execute('SELECT step,data FROM states WHERE user_id=?',(uid,)).fetchone()
        if not r or r['step']!='review_order':return None,'Buyurtma allaqachon yuborilgan yoki bekor qilingan.'
        fresh=json.loads(r['data'])
        if fresh!=data:return None,'Buyurtma ma’lumoti yangilandi.'
        expected=stars_total(fresh['qty']) if fresh['kind']=='stars' else param('premium_'+str(fresh['qty']))
        if expected<=0 or expected!=fresh['total']:
            c.execute('DELETE FROM states WHERE user_id=?',(uid,))
            return None,'Narx o‘zgargan. Qayta tanlang.'
        u=c.execute('SELECT balance FROM users WHERE id=?',(uid,)).fetchone()
        if not u or u['balance']<fresh['total']:return None,'Balans yetarli emas.'
        c.execute('UPDATE users SET balance=balance-? WHERE id=?',(fresh['total'],uid))
        c.execute('''INSERT INTO orders(id,user_id,kind,qty,recipient,price,status,created,updated)
                  VALUES(?,?,?,?,?,?,?,?,?)''',
                  (oid,uid,fresh['kind'],fresh['qty'],fresh['recipient'],fresh['total'],'processing',now(),now()))
        c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                  (uid,-fresh['total'],'order_debit',oid,now()))
        c.execute('DELETE FROM states WHERE user_id=?',(uid,))
    return oid,''


def finish_order(oid, status, reason=''):
    reward=None
    with db(True, financial=True) as c:
        o=c.execute('SELECT * FROM orders WHERE id=?',(oid,)).fetchone()
        if not o or o['status'] not in ('processing','review'):
            return None
        if status=='refunded':
            # Aynan bir marta refund, ledger reference UNIQUE.
            c.execute('UPDATE users SET balance=balance+? WHERE id=?',(o['price'],o['user_id']))
            c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                      (o['user_id'],o['price'],'order_refund',oid,now()))
        if status=='completed':
            reward=award_referral_in_tx(c,o)
        c.execute('UPDATE orders SET status=?,detail=?,updated=? WHERE id=?',
                  (status,reason[:500],now(),oid))
    if reward:
        safe_send(reward[0],f'🎉 Do‘stingizning muvaffaqiyatli xarididan {money(reward[1])} referal bonus oldingiz!')
    return o


def _confirm_purchase_async(uid, data):
    """No request thread waiting for Fragment /pricing and /buy.

    A 1-user state transition prevents duplicate confirm callbacks. Monetary
    debit and order creation remain atomic. An uncertain delivery is NEVER
    retried blindly: existing process_purchase marks it review instead.
    """
    try:
        ok, reason = safe_price_check(data['kind'], data['qty'], data['total'])
        if not ok:
            with db(True) as c:
                st = c.execute('SELECT step,data FROM states WHERE user_id=?',(uid,)).fetchone()
                if st and st['step']=='checking_price' and json.loads(st['data'])==data:
                    c.execute("UPDATE states SET step='review_order' WHERE user_id=?",(uid,))
            notify_admin('⚠️ Tannarx tekshiruvi: '+esc(reason))
            safe_send(uid,'⚠️ Hozir buyurtma qabul qilinmadi. Admin narxlarni tekshiradi.\n'+esc(reason)[:180])
            return
        with db(True) as c:
            st = c.execute('SELECT step,data FROM states WHERE user_id=?',(uid,)).fetchone()
            if not st or st['step']!='checking_price' or json.loads(st['data'])!=data:
                logger.info('Xarid narx tekshiruvi bekor qilingan user=%s',uid)
                return
            c.execute("UPDATE states SET step='review_order' WHERE user_id=?",(uid,))
        oid, msg = debit_and_create(uid, data)
        if not oid:
            safe_send(uid,'❌ '+esc(msg))
            return
        safe_send(uid,f'⏳ Buyurtma #{esc(oid)} qabul qilindi. Yetkazish boshlandi...')
        process_purchase(oid)
    except Exception as exc:
        logger.exception('Xaridni tekshirishda xatolik, user=%s',uid)
        # If no debit occurred, allow the user to retry. Once an order has
        # been created, never repeat an uncertain Fragment request automatically.
        with db(True) as c:
            st = c.execute('SELECT step,data FROM states WHERE user_id=?',(uid,)).fetchone()
            if st and st['step']=='checking_price' and json.loads(st['data'])==data:
                c.execute("UPDATE states SET step='review_order' WHERE user_id=?",(uid,))
        safe_send(uid,'⚠️ Xarid tekshiruvi vaqtincha ishlamadi. /start orqali qayta urinib ko‘ring.')
    finally:
        _PURCHASE_CAPACITY.release()


def process_purchase(oid):
    if not LIVE_SALES_ENABLED:
        logger.warning('Demo rejim: Fragment API orqali yetkazish bloklandi: %s', oid)
        return
    with db() as c: o=c.execute('SELECT * FROM orders WHERE id=?',(oid,)).fetchone()
    if not o or o['status']!='processing':return
    try:
        path='/stars/buy' if o['kind']=='stars' else '/premium/buy'
        payload={'username':o['recipient']}
        payload['amount' if o['kind']=='stars' else 'duration']=o['qty']
        j=fragment(path,payload)  # ATAYLAB hech qachon avtomatik retry qilinmaydi.
        if j.get('ok') is True:
            finish_order(oid,'completed',str(j.get('result',{}))[:450])
            safe_send(o['user_id'],f'✅ <b>Buyurtma bajarildi!</b>\n🧾 {esc(oid)}\n👤 @{esc(o["recipient"])}')
            notify_admin(f'✅ Buyurtma bajarildi: {oid} | {o["kind"]} {o["qty"]}')
            return
        code=str(j.get('code',''))
        reason=str(j.get('message','API xatosi'))[:220]
        # faqat provayderning aniq deterministik xatolari auto-refund
        safe_codes={'INSUFFICIENT_FUNDS','INSUFFICIENT_GAS','INVALID_AMOUNT_MIN','INVALID_AMOUNT_MAX','INVALID_DURATION','IP_NOT_ALLOWED'}
        if code in safe_codes:
            finish_order(oid,'refunded',code+': '+reason)
            safe_send(o['user_id'],f'❌ Buyurtma bajarilmadi. Pul balansga qaytarildi.\n🧾 {esc(oid)}\nSabab: {esc(reason)}')
            notify_admin(f'⚠️ Buyurtma qaytarildi: {oid} | {esc(code)} {esc(reason)}')
            return
        finish_order(oid,'review',code+': '+reason)
        safe_send(o['user_id'],f'⚠️ Buyurtma #{esc(oid)} tekshiruvda. Pul vaqtincha ushlab turiladi. Qayta buyurtma bermang; admin bilan bog‘laning.')
        notify_admin(f'🚨 BUYURTMA TEKSHIRUVDA\n{oid}\nXato: {esc(code)} {esc(reason)}')
    except Exception as e:
        logger.exception('Buyurtma noaniq natija: %s',oid)
        finish_order(oid,'review',str(e)[:300])
        safe_send(o['user_id'],f'⚠️ {esc(oid)} bo‘yicha API javobi aniq emas. Ikki marta xarid bo‘lmasligi uchun qayta yuborilmadi. Admin tekshiradi.')
        notify_admin(f'🚨 BUYURTMA NOANIQ NATIJA: {oid} — {esc(str(e))[:220]}')


def order_card(uid, oid, admin=False):
    with db() as c: o=c.execute('SELECT * FROM orders WHERE id=?',(oid,)).fetchone()
    if not o or (not admin and o['user_id']!=uid):return safe_send(uid,'Buyurtma topilmadi.')
    text=(f'📦 <b>Buyurtma {esc(o["id"])}</b>\n'
          f'Turi: {"⭐ Stars" if o["kind"]=="stars" else "🎁 Gift" if o["kind"]=="gift" else "💎 Premium"}\n'
          f'Miqdori: {o["qty"]}{" ta" if o["kind"]=="stars" else "-slot" if o["kind"]=="gift" else " oy"}\n'
          f'Qabul qiluvchi: @{esc(o["recipient"])}\nNarx: {money(o["price"])}\n'
          f'Holat: <b>{esc(o["status"])}</b>\nSana: {esc(o["created"])}')
    if admin:text+=f'\nUser: <code>{o["user_id"]}</code>\nIzoh: {esc(o["detail"])[:250]}'
    rows=[]
    if admin and o['status']=='review':
        rows=[[('✅ Yetkazilgan deb belgilash','a:done:'+oid)],
              [('↩️ Qaytarish (faqat tekshirib)','a:refund:'+oid)]]
    rows.append([('⬅️ Buyurtmalar','a:orders' if admin else 'u:orders')])
    safe_send(uid,text,keyboard(rows))


def my_orders(uid):
    with db() as c: items=c.execute('SELECT * FROM orders WHERE user_id=? ORDER BY created DESC LIMIT 15',(uid,)).fetchall()
    if not items:return safe_send(uid,'📦 Hozircha buyurtmalaringiz yo‘q.')
    rows=[[(f'{"⭐" if o["kind"]=="stars" else "🎁" if o["kind"]=="gift" else "💎"} {o["qty"]} | {o["status"]} | {o["id"][-6:]}','o:show:'+o['id'])] for o in items]
    safe_send(uid,'📦 <b>Buyurtmalarim</b>',keyboard(rows))


# ------------------ TELEGRAM GIFTS (11 inline slots) ------------------

def tg_api(method, payload=None):
    """Bot API raw call; avoids a separate pyTelegramBotAPI-specific gift implementation."""
    try:
        r=http_session().post('https://api.telegram.org/bot'+BOT_TOKEN+'/'+method,
                        json=payload or {}, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        # Never log the raw RequestException URL because it contains BOT_TOKEN.
        raise RuntimeError('Telegram API bilan bog‘lanib bo‘lmadi') from None
    try: obj=r.json()
    except ValueError as exc: raise RuntimeError('Telegram API javobi noto‘g‘ri') from exc
    if not obj.get('ok'):
        raise RuntimeError('Telegram API: '+str(obj.get('description','xatolik'))[:180])
    return obj['result']


def gift_row(slot):
    with db() as c:return c.execute('SELECT * FROM gifts WHERE slot=?',(int(slot),)).fetchone()


def gift_catalog(uid):
    if not check_access(uid):return
    with db() as c:
        rows = c.execute(
            "SELECT * FROM gifts WHERE enabled=1 AND price>0 AND gift_id<>'' ORDER BY slot"
        ).fetchall()
    if not rows:
        return safe_send(uid,'🎁 Sovg‘alar tez orada sotuvga qo‘yiladi.')
    rows_k=[[(f'{x["emoji"]} {x["name"]} — {money(x["price"])}',f'g:view:{x["slot"]}')]
            for x in rows]
    rows_k.append([('🏠 Bosh menyu','home')])
    return safe_send(uid,'🎁 <b>Telegram sovg‘alari</b>\n\nSovg‘ani tanlang:',keyboard(rows_k))


def gift_preview(uid,slot):
    g=gift_row(slot)
    if not g or not g['enabled'] or not g['gift_id'] or g['price']<=0:
        return safe_send(uid,'Bu sovg‘a hozir mavjud emas.')
    if g['sticker_file_id']:
        try:bot.send_sticker(uid,g['sticker_file_id'])
        except Exception:logger.info('Gift stiker yuborilmadi: slot=%s',slot)
    return safe_send(uid,f'{esc(g["emoji"])} <b>{esc(g["name"])}</b>\n\n'
             f'💰 Narxi: <b>{money(g["price"])}</b>\n'
             'Kimga sovg‘a qilasiz?',keyboard([
                 [('👤 O‘zimga',f'g:self:{slot}'),('🎁 Do‘stimga',f'g:friend:{slot}')],
                 [('⬅️ Sovg‘alar','g:list')]]))


def resolve_friend_username(username):
    """Only a username input; resolve user_id internally from users who started bot."""
    name=extract_username(username)
    if not name:return None
    with db() as c:
        row=c.execute('SELECT id,username FROM users WHERE LOWER(username)=LOWER(?)',(name,)).fetchone()
    if not row:return None
    try:
        info=bot.get_chat(int(row['id']))
        current=getattr(info,'username',None) or ''
        if current.lower()!=name.lower():return None
    except Exception:
        # Cannot confirm a stale username, never gift to the cached account blindly.
        return None
    return int(row['id'])


def begin_gift_review(uid,slot,target_id,target_username):
    g=gift_row(slot)
    if not g or not g['gift_id'] or not g['enabled'] or g['price']<=0:
        return safe_send(uid,'Sovg‘a sotuvdan olingan. Qaytadan tanlang.')
    u=user_row(uid)
    set_state(uid,'gift_review',{'slot':int(slot),'target_uid':int(target_id),
                                'target_username':target_username,'price':int(g['price']),
                                'gift_id':g['gift_id']})
    return safe_send(uid,'🎁 <b>Buyurtmani tasdiqlash</b>\n\n'
                     f'{esc(g["emoji"])} Sovg‘a: <b>{esc(g["name"])}</b>\n'
                     f'👤 Qabul qiluvchi: <b>{esc(target_username)}</b>\n'
                     f'💵 Narxi: <b>{money(g["price"])}</b>\n'
                     f'💰 Hisobingiz: <b>{money(u["balance"] if u else 0)}</b>',
                     keyboard([[('✅ Tasdiqlash','g:confirm')],
                               [('💳 Hisob to‘ldirish','top:menu'),('❌ Bekor qilish','home')]]))


def live_gift(gift_id):
    result=tg_api('getAvailableGifts')
    items=result.get('gifts') or []
    for item in items:
        if str(item.get('id'))==str(gift_id):
            if item.get('remaining_count') is not None and int(item['remaining_count'])<=0:
                return None
            return item
    return None


def gift_bot_stars():
    return int(tg_api('getMyStarBalance').get('amount',0))


def _confirm_gift_async(uid):
    """Gift API preflight and delivery off the webhook request thread.

    Once money is debited, gift handling preserves the existing
    no-auto-retry/review semantics for uncertain Telegram API outcomes.
    """
    try:
        buy_gift(uid)
    except Exception:
        logger.exception('Gift fon ishchi xatoligi, user=%s',uid)
        safe_send(uid,'⚠️ Sovg‘ani tekshirishda xato. Buyurtmalarim bo‘limini ko‘ring.')
    finally:
        # If validation failed before debit, make the checkout retryable.
        try:
            with db(True, financial=True) as c:
                c.execute("UPDATE states SET step='gift_review' WHERE user_id=? AND step='gift_processing'",(uid,))
        except Exception:
            logger.exception('Gift holatini tiklashda xatolik')
        _PURCHASE_CAPACITY.release()


def buy_gift(uid):
    if not LIVE_SALES_ENABLED:return safe_send(uid,'Xarid vaqtincha mavjud emas.')
    step,data=state(uid)
    if step not in ('gift_review','gift_processing'):return safe_send(uid,'Buyurtma eskirgan. Sovg‘ani qayta tanlang.')
    g=gift_row(data['slot'])
    if (not g or not g['enabled'] or g['price']<=0 or g['price']!=data['price']
            or g['gift_id']!=data['gift_id']):
        return safe_send(uid,'Sovg‘a narxi yoki turi o‘zgargan. Qayta tanlang.')
    with db() as c:
        u=c.execute('SELECT balance FROM users WHERE id=?',(uid,)).fetchone()
    if not u or u['balance']<g['price']:
        return safe_send(uid,'💳 Hisobingizda mablag‘ yetarli emas.',
                         keyboard([[('💳 Balansni to‘ldirish','top:menu')]]))
    # Do not charge customer unless bot has enough Stars and gift is available.
    try:
        item=live_gift(g['gift_id'])
        if item is None:return safe_send(uid,'Sovg‘a vaqtincha mavjud emas.')
        required=int(item['star_count'])
        if required<=0 or gift_bot_stars()<required:
            notify_admin(f'⚠️ Gift {g["slot"]}: bot Stars balansi yetarli emas ({required} ⭐).')
            return safe_send(uid,'Bu sovg‘a vaqtincha mavjud emas.')
    except Exception as exc:
        logger.warning('Gift preflight: %s',type(exc).__name__)
        return safe_send(uid,'Sovg‘alarni tekshirishda xatolik. Keyinroq urinib ko‘ring.')
    oid='G'+uuid.uuid4().hex[:12].upper()
    with db(True, financial=True) as c:
        st=c.execute('SELECT step,data FROM states WHERE user_id=?',(uid,)).fetchone()
        if not st or st['step'] not in ('gift_review','gift_processing') or json.loads(st['data'])!=data:
            return safe_send(uid,'Buyurtma allaqachon yuborilgan.')
        updated=c.execute('UPDATE users SET balance=balance-? WHERE id=? AND balance>=?',
                          (g['price'],uid,g['price']))
        if updated.rowcount!=1:return safe_send(uid,'Balans yetarli emas.')
        c.execute("INSERT INTO orders(id,user_id,kind,qty,recipient,price,status,detail,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                  (oid,uid,'gift',int(g['slot']),data['target_username'],g['price'],
                   'processing',json.dumps({'gift_id':g['gift_id'],'target_id':data['target_uid'],
                                           'stars_cost':required},ensure_ascii=False),now(),now()))
        c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                  (uid,-g['price'],'order_debit',oid,now()))
        c.execute('DELETE FROM states WHERE user_id=?',(uid,))
    safe_send(uid,'⏳ Sovg‘angiz yuborilmoqda...')
    try:
        # No retries: a timeout might occur after Telegram successfully sends gift.
        result=tg_api('sendGift',{'user_id':data['target_uid'],'gift_id':g['gift_id']})
        if result is not True:raise RuntimeError('Telegram tasdiqlamadi')
    except Exception as exc:
        # Treat uncertain Bot API outcomes as manual review; no automatic refund/retry.
        logger.exception('Gift yuborish noaniq: %s',oid)
        finish_order(oid,'review',str(exc)[:260])
        notify_admin(f'🚨 Gift buyurtma tekshiruvda: {oid}\n{esc(str(exc))[:140]}')
        return safe_send(uid,'⚠️ Sovg‘a buyurtmangiz tekshiruvda. Iltimos, qayta xarid qilmang.')
    finish_order(oid,'completed','Telegram Bot API sendGift: OK')
    safe_send(uid,f'✅ <b>Sovg‘a yuborildi!</b>\n🎁 {esc(g["name"])}\n'
                  f'👤 {esc(data["target_username"])}\n📦 Buyurtma: {esc(oid)}')
    notify_admin(f'🎁 Gift bajarildi: {oid} | {esc(g["name"])} | {money(g["price"])}')


def admin_gift_menu(uid):
    if not is_admin(uid):return
    with db() as c:rows=c.execute('SELECT * FROM gifts ORDER BY slot').fetchall()
    k=[]
    for i in range(0,11,2):
        k.append([(f'{"✅" if g["enabled"] and g["gift_id"] and g["price"] else "⚪"} {g["slot"]}. {g["emoji"]} {g["name"]}',f'a:gift:{g["slot"]}') for g in rows[i:i+2]])
    k += [[('🔄 Telegram Giftlarni yuklash','a:gift:sync')],
          [('⭐ Bot Stars balansi','a:stars:menu')]]
    return safe_send(uid,'🎁 <b>11 ta sovg‘ani boshqarish</b>\n\n'
                     '✅ — sotuvda  ⚪ — sozlanmagan/yopiq\n'
                     'Har bir Gift nomi, narxi va Telegram Gift turini alohida belgilang.',
                     keyboard(k))


def admin_gift_slot(uid,slot):
    g=gift_row(slot)
    if not g:return safe_send(uid,'Sovg‘a topilmadi.')
    if g['sticker_file_id']:
        try:bot.send_sticker(uid,g['sticker_file_id'])
        except Exception:pass
    return safe_send(uid,f'🎁 <b>Gift #{g["slot"]}</b>\n\n'
                     f'{esc(g["emoji"])} Nomi: <b>{esc(g["name"])}</b>\n'
                     f'💵 Narxi: <b>{money(g["price"])}</b>\n'
                     f'⭐ Telegram narxi: <b>{g["stars_cost"]} Stars</b>\n'
                     f'🆔 Telegram Gift: {esc(g["gift_id"] or "Tanlanmagan")}\n'
                     f'📦 Holati: {"✅ Sotuvda" if g["enabled"] else "🔴 Yopiq"}',
                     keyboard([[('✏️ Nomini o‘zgartirish',f'a:giftname:{slot}')],
                               [('💰 Narxini o‘zgartirish',f'a:giftprice:{slot}')],
                               [('🔗 Telegram Giftni tanlash',f'a:giftpick:{slot}')],
                               [('🟢 / 🔴 Sotuv',f'a:gifttoggle:{slot}')],
                               [('⬅️ 11 ta Gift','a:gift:menu')]]))


def admin_gift_choices(uid,slot):
    try:items=tg_api('getAvailableGifts').get('gifts',[])
    except Exception as exc:
        return safe_send(uid,'❌ Telegram Giftlar olinmadi: '+esc(str(exc))[:170])
    # Telegram may return more gifts; first 60 menu options, 10 per page only.
    with db(True) as c:
        for item in items:
            gid=str(item.get('id',''))
            if not gid:continue
            c.execute('INSERT OR IGNORE INTO settings(key,val) VALUES(?,?)',
                      ('giftcache_'+gid,json.dumps(item,ensure_ascii=False)))
    set_state(uid,'admin_giftpick',{'slot':slot})
    rows=[[(f'🎁 {i+1}. {it.get("star_count",0)} ⭐',f'a:gset:{slot}:{i}')]
          for i,it in enumerate(items[:60])]
    # index mapping stored under a cache key with a stable session for this admin
    put('gift_pick_ids_'+str(uid),json.dumps([str(it.get('id','')) for it in items[:60]]))
    rows.append([('⬅️ Gift oynasi',f'a:gift:{slot}')])
    return safe_send(uid,'🎁 <b>Telegram sovg‘asini tanlang</b>\nNarxi Stars’da ko‘rsatilgan.',keyboard(rows))


def admin_sync_gifts(uid):
    try:items=tg_api('getAvailableGifts').get('gifts',[])
    except Exception as exc:return safe_send(uid,'❌ Giftlar olinmadi: '+esc(str(exc))[:160])
    if not items:return safe_send(uid,'Hozir Telegram bot uchun mavjud Giftlar yo‘q.')
    with db(True) as c:
        for slot,it in enumerate(items[:11],1):
            row=c.execute('SELECT gift_id FROM gifts WHERE slot=?',(slot,)).fetchone()
            if row and row['gift_id']:continue
            sticker=it.get('sticker') or {}
            c.execute('UPDATE gifts SET gift_id=?,sticker_file_id=?,stars_cost=? WHERE slot=?',
                      (str(it['id']),str(sticker.get('file_id','')),int(it.get('star_count',0)),slot))
    return safe_send(uid,'✅ Telegram Giftlar yuklandi. Narxlarni belgilab, sotuvni yoqing.',
                     keyboard([[('🎁 Gift boshqaruvi','a:gift:menu')]]))


def referral_register(uid,ref_id,is_new):
    if not is_new or uid==ref_id or ref_id<=0 or get('referral_enabled')!='1':return
    with db(True) as c:
        r=c.execute('SELECT id FROM users WHERE id=?',(ref_id,)).fetchone()
        if not r:return
        c.execute('INSERT OR IGNORE INTO referrals(user_id,referrer_id,created) VALUES(?,?,?)',(uid,ref_id,now()))


def award_referral_in_tx(c,order):
    if get('referral_enabled')!='1':return
    parent=c.execute('SELECT referrer_id FROM referrals WHERE user_id=?',(order['user_id'],)).fetchone()
    if not parent:return
    pct=param('referral_percent')
    if not 1<=pct<=20:return
    bonus=(int(order['price'])*pct)//100
    if bonus<=0:return
    exists=c.execute('SELECT 1 FROM ledger WHERE reason=? AND ref=?',('referral_bonus',order['id'])).fetchone()
    if exists:return
    c.execute('UPDATE users SET referral_balance=referral_balance+? WHERE id=?',(bonus,parent['referrer_id']))
    c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
              (parent['referrer_id'],bonus,'referral_bonus',order['id'],now()))
    return (parent['referrer_id'],bonus)


def referral_menu(uid):
    if not check_access(uid):return
    try:uname=bot_username()
    except Exception:return safe_send(uid,'Havolani olishda xatolik. Keyinroq urinib ko‘ring.')
    with db() as c:
        count=c.execute('SELECT COUNT(*) n FROM referrals WHERE referrer_id=?',(uid,)).fetchone()['n']
        r=c.execute('SELECT referral_balance FROM users WHERE id=?',(uid,)).fetchone()
    link=f'https://t.me/{uname}?start=ref_{uid}'
    k=types.InlineKeyboardMarkup()
    k.row(types.InlineKeyboardButton('📋 Havolani nusxalash', copy_text=types.CopyTextButton(text=link)))
    k.row(types.InlineKeyboardButton('💳 Asosiy balansga o‘tkazish',callback_data='r:transfer'))
    k.row(types.InlineKeyboardButton('🏠 Bosh menyu',callback_data='home'))
    return safe_send(uid,'👥 <b>Referal dasturi</b>\n\n'
                     f'🔗 Havolangiz:\n<code>{esc(link)}</code>\n\n'
                     f'👤 Takliflar: <b>{count}</b>\n'
                     f'🎁 Bonus balans: <b>{money(r["referral_balance"] if r else 0)}</b>\n'
                     f'💸 Xariddan bonus: <b>{param("referral_percent")}%</b>\n\n'
                     'Do‘stingizning muvaffaqiyatli xarididan so‘ng bonus yoziladi.',
                     k)


def referral_transfer(uid):
    with db(True, financial=True) as c:
        r=c.execute('SELECT referral_balance FROM users WHERE id=?',(uid,)).fetchone()
        bonus=int(r['referral_balance']) if r else 0
        if bonus<=0:return safe_send(uid,'Referal balansda pul yo‘q.')
        c.execute('UPDATE users SET referral_balance=0,balance=balance+? WHERE id=?',(bonus,uid))
        c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                  (uid,bonus,'referral_transfer','R'+uuid.uuid4().hex[:12],now()))
    return safe_send(uid,f'✅ {money(bonus)} asosiy balansga o‘tkazildi.')


def admin_referral_menu(uid):
    if not is_admin(uid):return
    with db() as c:
        cnt=c.execute('SELECT COUNT(*) n FROM referrals').fetchone()['n']
        total=c.execute('SELECT COALESCE(SUM(delta),0) n FROM ledger WHERE reason=?',('referral_bonus',)).fetchone()['n']
    return safe_send(uid,'👥 <b>Referal boshqaruvi</b>\n\n'
                     f'🟢 Tizim: {"Yoqilgan" if get("referral_enabled")=="1" else "O‘chirilgan"}\n'
                     f'💸 Bonus: <b>{param("referral_percent")}%</b>\n'
                     f'👥 Takliflar: <b>{cnt}</b>\n💰 Bonuslar: {money(total)}',
                     keyboard([[('✏️ Foizni o‘zgartirish','a:ref:rate')],
                               [('🟢 / 🔴 Yoqish-o‘chirish','a:ref:toggle')]]))


def admin_bot_stars(uid):
    if not only_owner(uid):return safe_send(uid,'Faqat bosh admin uchun.')
    try:balance=gift_bot_stars()
    except Exception as exc:return safe_send(uid,'Bot Stars hisobini o‘qib bo‘lmadi: '+esc(str(exc))[:160])
    return safe_send(uid,'⭐ <b>Bot Stars hamyoni</b>\n\n'
                     f'💰 Balans: <b>{balance:,} ⭐</b>\n\n'
                     'Giftlarni yuborish aynan shu balansdan amalga oshiriladi.',
                     keyboard([[('⭐ 100 Stars','a:stars:add:100'),('⭐ 250 Stars','a:stars:add:250')],
                               [('⭐ 500 Stars','a:stars:add:500'),('⭐ 1000 Stars','a:stars:add:1000')],
                               [('✏️ Boshqa miqdor','a:stars:custom')],
                               [('🔄 Balansni yangilash','a:stars:menu')]]))


def invoice_for_owner(uid,qty):
    if not only_owner(uid):return
    if not 1<=qty<=100000:return safe_send(uid,'Stars miqdori 1–100 000 oralig‘ida bo‘lsin.')
    payload=f'owner_stars:{uid}:{qty}:{uuid.uuid4().hex[:12]}'
    try:
        return bot.send_invoice(chat_id=uid,title='⭐ Bot Stars hisobini to‘ldirish',
            description=f'Bot hamyoniga {qty} Telegram Stars',
            invoice_payload=payload,provider_token='',currency='XTR',
            prices=[types.LabeledPrice('Bot Stars',qty)])
    except Exception as exc:
        logger.exception('XTR invoice')
        safe_send(uid,'❌ Stars to‘lov oynasi ochilmadi: '+esc(str(exc))[:150])


def topup_menu(uid):
    if not check_access(uid):return
    safe_send(uid,f'💳 <b>Hisob to‘ldirish</b>\nMinimal: {money(param("min_topup"))}\n'
         'Summani o‘zingiz kiriting:', keyboard([
             [('✏️ Summani kiritish','top:other')],
             [('🏠 Bosh menyu','home')],
         ]))







def expected_humo_last4():
    card = normalize_card(get('payment_card_number'))
    if not re.fullmatch(r'\d{16,19}', card):
        return ''
    if HUMO_CARD_LAST4 and HUMO_CARD_LAST4 != card[-4:]:
        return ''  # ENV karta raqami bilan admin panelidagi karta mos bo'lmasa STOP.
    return card[-4:]


def bank_configured():
    # Username Telethon orqali bank botining aniq numeric ID'siga bog'lanadi.
    # Faqat admin paneldagi karta sozlanganidan keyin monitoring ishga tushadi.
    return bool(TELEGRAM_API_ID and TELEGRAM_API_HASH and TELETHON_STRING_SESSION
                and (HUMO_SENDER_ID > 0 or HUMO_BOT_USERNAME)
                and re.fullmatch(r'\d{4}', expected_humo_last4()))


def payment_configured():
    return bank_configured() and HUMO_LISTENER_CONNECTED.is_set()



def payment_ready():
    return LIVE_SALES_ENABLED and payment_configured() and get('payment_enabled') == '1'


# -------------------- TELETHON / HUMO BANK NOTIFICATION --------------------
# Incoming HUMO notifications are not signed bank confirmations.
# If HUMO_AUTO_CREDIT=1 is enabled, keep bank-side reconciliation on.
BANK_INCOMING_RE = re.compile(r'(?:kirim|tushum|incoming|credited|зачислен|поступлен|получен)', re.I)
BANK_AMOUNT_RE = re.compile(
    r'(?:kirim|tushum|incoming|credited|зачислен\w*|поступлен\w*|получен\w*)'
    r'[^\n\r]{0,26}?[+:]?\s*([0-9][0-9\s.,]{1,18})\s*(?:uzs|so.m|sum|сум)', re.I)
BANK_REFERENCE_RE = re.compile(
    r'(?:tranzaksiya|operatsiya|transaction|transfer|tx|ref|чек|операция)'
    r'\s*(?:id|№|no\.?|#)?\s*[:#=№-]?\s*([A-Za-z0-9_-]{8,64})', re.I)


def parse_bank_notice(text):
    """HUMO Card bank-botidagi HAQIQIY kirim xabarini tanish.

    Misol (bank xabari, balance emas):
       🎉 To'ldirish
       ➕ 1.001,00 UZS
       📍 HUMO TO HUMO UZPAYNE
       💳 HUMOCARD *1802
       💰 1.474,20 UZS   # bu *qoldiq*, to'lov emas

    Bank tranzaksiya ID HUMOcardbot xabarida bo'lmasligi mumkin.
    ID yo'q holatni process_bank_notice yagona Telegram bank-bot xabari
    sender_id + message_id yordamida deduplikatsiya qiladi.
    """
    text = (text or '').replace('\u00a0', ' ').replace('\u202f', ' ').strip()
    text = (text.replace('’', "'").replace('ʻ', "'").replace('ʼ', "'")
                .replace('‘', "'").replace('`', "'"))
    if not re.search(
        r"(?:\bto'ldirish\b|\bkirim\b|\btushum\b|\bincoming\b|"
        r"\bcredited\b|зачислен\w*|поступлен\w*|получен\w*)",
        text, re.IGNORECASE,
    ):
        return None

    # Karta qoldig'i ham UZS bilan keladi, shuning uchun faqat
    # «➕ 1.001,00 UZS» kabi ijobiy o'tkazmani birinchi o'rinda olamiz.
    match = re.search(
        r'(?m)^[ \t]*(?:\+|➕|＋)[ \t]*([0-9][0-9 \t.,]{0,18})[ \t]*(?:UZS|so\.?m|сўм|сум)\b',
        text, re.IGNORECASE,
    )
    if not match:
        # Eski bank-bot kirim shakllarini ham qo'llab-quvvatlash.
        match = BANK_AMOUNT_RE.search(text)
    if not match:
        return None

    number = match.group(1).strip().replace(' ', '').replace('\t', '')
    if ',' in number and '.' in number:
        decimal_sep = ',' if number.rfind(',') > number.rfind('.') else '.'
        thousand_sep = '.' if decimal_sep == ',' else ','
        number = number.replace(thousand_sep, '').replace(decimal_sep, '.')
    elif ',' in number or '.' in number:
        sep = ',' if ',' in number else '.'
        trailing = len(number.rsplit(sep, 1)[-1])
        if number.count(sep) > 1 or trailing == 3:
            number = number.replace(sep, '')
        elif trailing in (1, 2):
            number = number.replace(sep, '.')
        else:
            return None
    try:
        amount = Decimal(number)
    except InvalidOperation:
        return None
    if not amount.is_finite() or amount != int(amount) or amount <= 0:
        return None

    card4 = expected_humo_last4()
    if not card4:
        return None
    # «HUMOCARD *1802», «Karta: ****1802» kabi bank formatlari.
    card_pattern = (r'(?:karta|card|карта|humocard|humo|uzcard|[*xX•]{2,})'
                    r'[^\n\r]{0,24}' + re.escape(card4) + r'(?!\d)')
    if not re.search(card_pattern, text, re.IGNORECASE):
        return None
    ref = BANK_REFERENCE_RE.search(text)
    return {'amount': int(amount), 'tx_ref': ref.group(1) if ref else None}


def create_bank_payment(uid, requested):
    if not payment_ready():
        return safe_send(uid,'⏳ To‘lov qabul qilish hozircha mavjud emas. Iltimos, keyinroq urinib ko‘ring.')
    if not param('min_topup')<=requested<=param('max_topup'):
        return safe_send(uid,'❌ Summa belgilangan limitdan tashqarida.')
    pid='B'+uuid.uuid4().hex[:15].upper()
    card=get('payment_card_number').strip()
    owner=get('payment_card_name').strip()
    now_ts=int(time.time())
    with db(True, financial=True) as c:
        # To'lov 5 daqiqa amal qiladi; qo'shimcha 120 soniya kechikkan
        # xabar kelishiga vaqt beramiz, keyin muddati o'tgan summani bo'shatamiz.
        # Kechikib kelgan haqiqiy to'lov admin tomonidan tekshirilishi kerak.
        c.execute("UPDATE payments SET status='expired',updated=? WHERE status='pending' AND provider_id LIKE 'BANK:%' AND expires_at < ?",
                  (now(), now_ts-120))
        # One active invoice per user. New request cancels old pending invoice.
        # Its original amount is quarantined, so a late HUMO message can't credit new invoice.
        previous=c.execute("SELECT id FROM payments WHERE user_id=? AND status='pending' AND provider_id LIKE 'BANK:%'",(uid,)).fetchall()
        if previous:
            c.execute("UPDATE payments SET status='cancel',updated=? WHERE user_id=? AND status='pending' AND provider_id LIKE 'BANK:%'",(now(),uid))
        amount=reserve_payment_amount(c,requested)
        if amount==requested:
            c.execute("INSERT INTO payments(id,user_id,amount,provider_id,card,card_holder,expires_at,status,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (pid,uid,amount,'BANK:'+pid,card,owner,now_ts+300,'pending',now(),now()))
    if amount != requested:
        set_state(uid,'payment_suggestion',{'amount':amount})
        return safe_send(uid,('♻️ Avvalgi to‘lov bekor qilindi.\n\n' if previous else '')+f'💳 {money(requested)} summasi hozir band.\n\n✅ {money(amount)} miqdorida yangi to‘lov ochilsinmi?',
                         keyboard([[('✅ Roziman','pay:accept_suggest')],[('⬅️ Orqaga','top:menu')]]))
    with db() as c:
        p=c.execute('SELECT * FROM payments WHERE id=?',(pid,)).fetchone()
    prefix='🔄 Oldingi faol to‘lov bekor qilindi. Faqat yangi summani to‘lang.\n\n' if previous else ''
    return safe_send(uid, prefix+payment_receipt_text(p,new=True), payment_keyboard(p))


def process_bank_notice(sender_id, message_id, text, event_timestamp):
    """Idempotent bank notification processor. Never rely on amount alone.

    Require: pinned sender ID, inbound credit, correct card suffix, unique bank
    notification (sender+message ID), exact unique active amount and notification
    time within the invoice window. Auto credit is opt-in; unresolved cases must
    be reconciled with the bank, not guessed from a matching amount alone.
    """
    if not (PAYMENT_PROVIDER=='bank_session' and LIVE_SALES_ENABLED):
        return 'disabled'
    if sender_id != HUMO_SENDER_ID:return 'untrusted_sender'
    record=parse_bank_notice(text)
    if not record:return 'unrecognized'
    # HUMO Card xabarida bank transaction ID yo'q: manbasi bank botining
    # aniq Telethon sender_id va message_id juftligi orqali dedup qilinadi.
    # Bu bankdan imzolangan tasdiq EMAS; faqat Telegram bank-bot bildirishnomasi.
    notice_ref = record['tx_ref'] or f'TG:{sender_id}:{message_id}'
    event_ts=float(event_timestamp)
    pid=None
    with db(True, financial=True) as c:
        matching=c.execute("SELECT * FROM payments WHERE status='pending' AND amount=? AND provider_id LIKE 'BANK:%' AND expires_at >= ?",(record['amount'],int(event_ts))).fetchall()
        matching=[p for p in matching if 0 <= event_ts-datetime.strptime(p['created'],'%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc).timestamp()<=300]
        if len(matching)!=1:return 'ambiguous'
        p=matching[0]
        pid=p['id']
        inserted = c.execute("INSERT OR IGNORE INTO bank_events(sender_id,message_id,tx_ref,amount,payment_id,status,created) VALUES(?,?,?,?,?,?,?)", (sender_id,message_id,notice_ref,record['amount'],pid,'observed',now()))
        if inserted.rowcount != 1:return 'duplicate'
        if not HUMO_AUTO_CREDIT:
            c.execute("UPDATE bank_events SET status='review' WHERE tx_ref=?",(notice_ref,))
        else:
            # Atomic transaction: one bank event -> one paid invoice + one ledger credit.
            updated = c.execute("UPDATE payments SET status='paid',updated=? WHERE id=? AND status='pending'",(now(),pid))
            if updated.rowcount!=1:return 'already_processed'
            c.execute('UPDATE users SET balance=balance+? WHERE id=?',(p['amount'],p['user_id']))
            c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                      (p['user_id'],p['amount'],'bank_credit',pid,now()))
            c.execute("UPDATE bank_events SET status='credited' WHERE tx_ref=?",(notice_ref,))
    if HUMO_AUTO_CREDIT:
        safe_send(p['user_id'],f'✅ <b>To‘lov qabul qilindi!</b>\n💰 {money(p["amount"])} hisobingizga qo‘shildi.')
        notify_admin(f'✅ HUMO avtomatik: {esc(pid)} | {money(p["amount"])} | bank ID {esc(notice_ref)}')
        return 'credited'
    notify_admin(f'🔎 HUMO xabari topildi: {esc(pid)} | {money(record["amount"])}\n'
                 'Bank ilovangizda tekshirib, keyin /bankapprove BUYURTMA_ID buyrug‘ini yuboring.')
    return 'review'


def approve_bank_payment(uid, pid):
    if uid!=OWNER_ID:return 'Ruxsat yo‘q.'
    with db(True, financial=True) as c:
        p=c.execute('SELECT * FROM payments WHERE id=?',(pid,)).fetchone()
        if not p or p['status']!='pending' or not str(p['provider_id']).startswith('BANK:'):
            return 'Pending HUMO to‘lov topilmadi.'
        ev=c.execute("SELECT * FROM bank_events WHERE payment_id=? AND status='review' ORDER BY created DESC LIMIT 1",(pid,)).fetchone()
        if not ev:return 'Bank xabari topilmagan. Shu buyruq bilan pul qo‘shib bo‘lmaydi.'
        updated = c.execute("UPDATE payments SET status='paid',updated=? WHERE id=? AND status='pending'",(now(),pid))
        if updated.rowcount != 1:return 'Allaqachon hisoblangan.'
        c.execute('UPDATE users SET balance=balance+? WHERE id=?',(p['amount'],p['user_id']))
        c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                  (p['user_id'],p['amount'],'bank_credit',pid,now()))
        c.execute("UPDATE bank_events SET status='credited' WHERE sender_id=? AND message_id=?",(ev['sender_id'],ev['message_id']))
    safe_send(p['user_id'],f'✅ Bank to‘lovi admin tomonidan tasdiqlandi. {money(p["amount"])} qo‘shildi.')
    return f'✅ {pid}: {money(p["amount"])} qo‘shildi.'


def telethon_listener_worker():
    async def run():
        global HUMO_SENDER_ID
        async with TelegramClient(StringSession(TELETHON_STRING_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
            if not await client.is_user_authorized():
                raise RuntimeError('StringSession bekor qilingan, yangisini kiriting')
            bank_entity = await client.get_entity(HUMO_SENDER_ID or HUMO_BOT_USERNAME)
            # Username orqali faqat aniq @HUMOcardbot identifikatori olinadi.
            uname = (getattr(bank_entity,'username',None) or '').lower()
            if uname != HUMO_BOT_USERNAME.lower():
                raise RuntimeError('Bank bot username mos emas, listener to‘xtatildi')
            if HUMO_SENDER_ID and int(bank_entity.id) != HUMO_SENDER_ID:
                raise RuntimeError('Bank botning numeric ID raqami mos emas')
            HUMO_SENDER_ID = int(bank_entity.id)
            HUMO_LISTENER_CONNECTED.set()
            logger.info('Telethon @%s ulandi, sender_id=%s', HUMO_BOT_USERNAME, HUMO_SENDER_ID)
            # Recover recently missed notifications after deployment or reconnect.
            # The persistent unique bank_events + invoice timestamp guard prevent replay credits.
            try:
                recent=[]
                async for previous in client.iter_messages(bank_entity, limit=60):
                    if previous.date and previous.date.timestamp() < time.time()-86400:
                        break
                    if not previous.out and not previous.fwd_from:
                        recent.append(previous)
                for previous in reversed(recent):
                    if previous.message:
                        await asyncio.to_thread(process_bank_notice, HUMO_SENDER_ID,
                                                previous.id,previous.message,previous.date.timestamp())
            except Exception:
                logger.exception('Bank xabarlarini qayta ko‘rishda xatolik')
            @client.on(events.NewMessage(incoming=True, chats=HUMO_SENDER_ID))
            async def bank_message(e):
                if e.message.out or e.message.fwd_from or e.sender_id!=HUMO_SENDER_ID:
                    return
                try:
                    status=process_bank_notice(e.sender_id,e.message.id,e.message.message,
                                               e.message.date.timestamp())
                    logger.info('HUMO bank xabari qayta ishlash: %s', status)
                except Exception:
                    logger.exception('HUMO xabarini qayta ishlashda xatolik')
            await client.run_until_disconnected()
    # Keep retrying on network failures. The thread is a daemon owned by the
    # single Gunicorn worker, not a Render scheduled background job.
    while True:
        try:
            asyncio.run(run())
        except Exception:
            logger.exception('Telethon listener uzildi; qayta ulanadi')
        finally:
            HUMO_LISTENER_CONNECTED.clear()
        time.sleep(15)


_HUMO_CHECK_NEXT = 0.0
_HUMO_CHECK_LOCK = threading.Lock()

def maybe_start_bank_listener():
    # Monitoring may connect while live sales is OFF: /bankstatus can be
    # checked without accepting actual money. Crediting itself remains gated.
    global _HUMO_CHECK_NEXT
    if PAYMENT_PROVIDER != 'bank_session' or HUMO_LISTENER_STARTED.is_set():return
    with _HUMO_CHECK_LOCK:
        if HUMO_LISTENER_STARTED.is_set() or time.monotonic() < _HUMO_CHECK_NEXT:
            return
        _HUMO_CHECK_NEXT = time.monotonic()+20.0
        if not bank_configured():return
        HUMO_LISTENER_STARTED.set()
    threading.Thread(target=telethon_listener_worker,name='humo-listener',daemon=True).start()
















def normalize_card(number):
    """Karta raqamlarini faqat taqqoslash uchun raqamlarga keltirish."""
    return re.sub(r'\D', '', str(number or ''))










def payment_time_text(p):
    expiry = p['expires_at']
    if expiry is None:
        return '⏱ Muddat aniqlanmadi.'
    secs = int(expiry) - int(time.time())
    if secs <= 0:
        return ('⌛ <b>To‘lov uchun ajratilgan vaqt tugagan.</b>\n'
                '❗ Endi bu rekvizitga pul yubormang. Agar avval to‘lagan bo‘lsangiz, '
                'adminga murojaat qiling; muddati o‘tgan to‘lov avtomatik tasdiqlanmaydi.')
    minutes, seconds = divmod(secs, 60)
    return f'⏱ Qolgan vaqt: <b>{minutes:02d}:{seconds:02d}</b> (daq:soniya)'


def payment_keyboard(p):
    k=types.InlineKeyboardMarkup(row_width=2)
    # Official Bot API CopyTextButton: no extra user actions or IDs.
    k.row(types.InlineKeyboardButton('📋 Kartani nusxalash',
          copy_text=types.CopyTextButton(text=str(p['card']))),
          types.InlineKeyboardButton('📋 Summani nusxalash',
          copy_text=types.CopyTextButton(text=str(p['amount']))))
    if p['status']=='pending':
        k.row(types.InlineKeyboardButton('🔄 To‘lovni tekshirish',callback_data='pay:check:'+p['id']))
        k.row(types.InlineKeyboardButton('❌ Bekor qilish',callback_data='pay:cancel:'+p['id']))
    k.row(types.InlineKeyboardButton('🏠 Bosh menyu',callback_data='home'))
    return k


def payment_receipt_text(p, new=False):
    holder=str(p['card_holder'] or '').strip()
    lines=[
      '💳 <b>HISOBNI TO‘LDIRISH</b>' if new else '💳 <b>TO‘LOV MA’LUMOTLARI</b>',
      '━━━━━━━━━━━━━━━━',
      '🏦 <b>HUMO karta</b>',
      f'<code>{esc(p["card"])}</code>',
      f'👤 {esc(holder) if holder else "Karta egasi kiritilmagan"}',
      '',
      f'💰 To‘lov: <b>{money(p["amount"])}</b>',
    ]
    if p['status']=='pending':
        lines += [payment_time_text(p),'',
                  '📋 Karta va summani quyidagi tugmalar orqali nusxalang.',
                  '✅ To‘lov tasdiqlangach, balansingiz avtomatik yangilanadi.',
                  '❗ Aynan shu summani yuboring.']
    else:
        labels={'paid':'✅ Muvaffaqiyatli','cancel':'❌ Bekor qilingan',
                'expired':'⌛ Vaqti tugagan','review':'⏳ Tekshirilmoqda'}
        lines += ['',f'📌 Holat: <b>{labels.get(p["status"],esc(p["status"]))}</b>']
    return '\n'.join(lines)


def reserve_payment_amount(c, requested):
    """Provider identifies transfers by amount. Reserve a different sum per pending payment."""
    # Do not reclaim a temporarily unknown payment amount automatically.
    # Also quarantine recent canceled/paid invoices against late bank notifications.
    cutoff=(datetime.now(timezone.utc)-timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S UTC')
    used={int(x['amount']) for x in c.execute(
        "SELECT amount FROM payments WHERE status IN ('creating','pending','review') OR created>=?",(cutoff,)).fetchall()}
    for add in range(100):
        if requested+add not in used and requested+add<=param('max_topup'):
            return requested+add
    raise RuntimeError('Hozir bu summa band. Boshqa summa kiriting.')


def create_payment(uid, requested):
    return create_bank_payment(uid, requested)



def verify_payment(pid):
    with db() as c:
        p = c.execute('SELECT status FROM payments WHERE id=?',(pid,)).fetchone()
    return p['status'] if p else 'missing'


def cancel_pending_payment(uid,pid):
    with db(True, financial=True) as c:
        p=c.execute('SELECT * FROM payments WHERE id=?',(pid,)).fetchone()
        if not p or p['user_id']!=uid:
            return safe_send(uid,'⛔ Bu to‘lovga ruxsat yo‘q.')
        if p['status']=='pending':
            c.execute("UPDATE payments SET status='cancel',updated=? WHERE id=?",(now(),pid))
    return safe_send(uid,'✅ To‘lov oynasi yopildi. Agar pul yuborgan bo‘lsangiz, adminga yozing: bank o‘tkazmasi avtomatik qaytarilmaydi.')



def payment_sweep(limit=3):
    # Expiration is conservative: two-minute buffer for late bank notifications.
    with db(True, financial=True) as c:
        n=c.execute("UPDATE payments SET status='expired',updated=? WHERE status='pending' AND provider_id LIKE 'BANK:%' AND expires_at < ?",(now(),int(time.time())-120))
        return n.rowcount


def payment_card(uid,pid,admin=False):
    with db() as c: p=c.execute('SELECT * FROM payments WHERE id=?',(pid,)).fetchone()
    if not p or (not admin and p['user_id']!=uid): return safe_send(uid,'To‘lov topilmadi.')
    text=payment_receipt_text(p)
    safe_send(uid,text,payment_keyboard(p))


def balance_history(uid):
    with db() as c: rows=c.execute('SELECT * FROM ledger WHERE user_id=? ORDER BY id DESC LIMIT 15',(uid,)).fetchall()
    if not rows:return safe_send(uid,'🧾 Hozircha tranzaksiyalar yo‘q.')
    text='🧾 <b>Balans harakatlari</b>\n\n'
    for r in rows:
        text+=f'{"➕" if r["delta"]>0 else "➖"} {money(abs(r["delta"]))} | {esc(r["reason"])} | {esc(r["created"][:16])}\n'
    safe_send(uid,text[:4000])


def profile(uid):
    r=user_row(uid)
    if not r:return
    with db() as c:
        x=c.execute("SELECT COUNT(*) as n, COALESCE(SUM(CASE WHEN status='completed' THEN price ELSE 0 END),0) AS sold FROM orders WHERE user_id=?",(uid,)).fetchone()
    safe_send(uid,f'💰 <b>Hisobim</b>\n\n👤 {esc(r["name"])}\n'
              f'🔗 @{esc(r["username"] or "username yo‘q")}\n🆔 <code>{uid}</code>\n'
              f'💳 Balans: <b>{money(r["balance"])}</b>\n'
              f'📦 Buyurtmalar: {x["n"]} ta\n🛍 Bajarilgan xarid: {money(x["sold"])}',
              keyboard([[('🧾 Tranzaksiyalar tarixi','u:ledger')],[('💳 Hisob to‘ldirish','top:menu')]]))


def report_ticket(uid, m):
    with db(True) as c:
        t=c.execute('INSERT INTO tickets(user_id,status,created,updated) VALUES(?,?,?,?)',(uid,'open',now(),now()))
        tid=t.lastrowid
        c.execute('INSERT INTO ticket_messages(ticket_id,sender_id,body,created) VALUES(?,?,?,?)',
                  (tid,uid,str(m.text or m.caption or '['+m.content_type+']')[:2000],now()))
    k=keyboard([[('↩️ Javob berish',f't:reply:{tid}'),('✅ Yopish',f't:close:{tid}')]])
    for a in admin_ids():
        safe_send(a,f'📩 <b>Yangi murojaat #{tid}</b>\nUser: <code>{uid}</code>\nIsm: {esc(m.from_user.first_name)}',k)
        try:bot.copy_message(a,uid,m.message_id)
        except Exception:pass
    safe_send(uid,f'✅ Murojaatingiz #{tid} adminga yuborildi. Javob shu botga keladi.')
    clear_state(uid)


def admin_star(uid):
    price = param('star_price')
    with db() as c:
        packs = c.execute('SELECT * FROM packages ORDER BY amount').fetchall()
    pack_text = '\n'.join(
        f' {"✅" if p["enabled"] else "❌"} {p["amount"]:,} ⭐ = '
        + (money(stars_total(p['amount'], price)) if price > 0 else 'narx kiritilmagan')
        for p in packs
    ) or 'To‘plam mavjud emas.'
    safe_send(uid,
              f'⭐ <b>Stars narxlarini boshqarish</b>\n\n'
              f'💵 <b>1 ⭐ = {money(price) if price > 0 else "Narx belgilanmagan"}</b>\n'
              f'🧮 Barcha to‘plamlar va boshqa miqdor avtomatik hisoblanadi.\n'
              f'📏 Chegara: {param("min_stars"):,}–{param("max_stars"):,} ⭐\n\n'
              f'<b>To‘plam narxlari:</b>\n{pack_text}',
              keyboard([[('✏️ 1 Stars narxini o‘zgartirish','a:input:star_price')],
                        [('➕ To‘plam qo‘shish','a:input:pack_add'),('🗑 To‘plam o‘chirish','a:input:pack_del')],
                        [('🔢 Minimal miqdor','a:input:min_stars'),('🔢 Maksimal miqdor','a:input:max_stars')]]))


def admin_premium(uid):
    rows=[[(f'💎 {m} oy — {money(param("premium_"+str(m)))}',f'a:input:premium_{m}')] for m in (3,6,12)]
    safe_send(uid,'💎 <b>Premium narxlari</b>\nTanlang va so‘mda yangi narx kiriting.',keyboard(rows))


def admin_orders(uid):
    with db() as c:items=c.execute('SELECT * FROM orders ORDER BY created DESC LIMIT 20').fetchall()
    if not items:return safe_send(uid,'📦 Hozircha buyurtmalar yo‘q.')
    safe_send(uid,'📦 <b>Oxirgi 20 buyurtma</b>',keyboard([[(f'{o["id"][-6:]} | {o["status"]} | {o["qty"]}', 'a:order:'+o['id'])] for o in items]))


def admin_payments(uid):
    with db() as c:items=c.execute('SELECT * FROM payments ORDER BY created DESC LIMIT 15').fetchall()
    if not items:return safe_send(uid,'💳 To‘lovlar hali yo‘q.')
    safe_send(uid,'💳 <b>Oxirgi to‘lovlar</b>',keyboard([[(f'{p["id"][-7:]} | {money(p["amount"])} | {p["status"]}','a:pay:'+p['id'])] for p in items]))


def admin_stats(uid):
    with db() as c:
        u=c.execute('SELECT COUNT(*) n, COALESCE(SUM(balance),0) b FROM users').fetchone()
        o=c.execute('''SELECT COUNT(*) n,
          COALESCE(SUM(CASE WHEN status='completed' THEN price ELSE 0 END),0) revenue,
          SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) done,
          SUM(CASE WHEN status='review' THEN 1 ELSE 0 END) review FROM orders''').fetchone()
        p=c.execute("SELECT COALESCE(SUM(amount),0) n FROM payments WHERE status='paid'").fetchone()
    safe_send(uid,f'📊 <b>Statistika</b>\n\n👥 Userlar: {u["n"]}\n💰 User balanslari yig‘indisi: {money(u["b"])}\n'
              f'📦 Buyurtmalar: {o["n"]}\n✅ Bajarilgan: {o["done"] or 0}\n'
              f'⚠️ Tekshiruvda: {o["review"] or 0}\n🛍 Bajarilgan savdo: {money(o["revenue"])}\n'
              f'💳 Tasdiqlangan to‘lovlar: {money(p["n"])}\n\n'
              'ℹ️ Sof foyda emas: API tannarxi, kurs va komissiya alohida hisoblanadi.')


def admin_integrations(uid):
    if not only_owner(uid):return safe_send(uid,'🔒 Faqat bosh admin sozlaydi.')
    card=get('payment_card_number').strip()
    holder=get('payment_card_name').strip()
    safe_send(uid,(
        '🏦 <b>HUMO — String Session avtomatik to‘lov</b>\n\n'
        f'🤖 Bot: @{esc(HUMO_BOT_USERNAME)}\n'
        f'📡 Telethon: {"✅ Ulangan" if HUMO_LISTENER_CONNECTED.is_set() else "❌ Ulanmagan"}\n'
        f'💳 Karta: {esc(card or "Kiritilmagan")}\n'
        f'👤 Karta egasi: {esc(holder or "Kiritilmagan")}\n'
        f'💰 To‘lovlar: {"✅ Yoqilgan" if payment_ready() else "❌ O‘chirilgan"}\n'
        f'🔎 Avto tasdiq: {"✅ 1 (yoqilgan)" if HUMO_AUTO_CREDIT else "❌ 0"}\n'
        f'🔐 Bank bot ID: {HUMO_SENDER_ID or "Aniqlanmoqda"}\n'
        f'⭐ Fragment API Key: {mask_secret("fragment_key")}\n'
        f'📊 USD/UZS kursi: {param("usd_rate")}\n\n'
        '✅ /bankstatus — bank xabarlarini kuzatish holati.\n'
        '⚠️ Bank-bot xabari imzolangan bank API tasdig‘i emas.'),
        keyboard([
            [('💳 Karta raqamini o‘zgartirish','a:input:payment_card_number')],
            [('👤 Karta egasini o‘zgartirish','a:input:payment_card_name')],
            [('🔑 Fragment API Key','a:input:fragment_key')],
            [('💱 USD/UZS kursi','a:input:usd_rate')],
            [('🔎 Fragmentni tekshirish','a:testfrag'),('💼 Fragment hamyon','a:wallet')],
            [('🟢 / 🔴 To‘lovni yoqish/o‘chirish','a:paytoggle')],
        ]))



def admin_channels(uid):
    chs = channel_rows()
    lines='\n'.join(esc(x['val'].split('|')[0]) for x in chs)
    safe_send(uid,'📣 <b>Majburiy obuna kanallari</b>\n'+(lines or 'Hozircha yo‘q'),
              keyboard([[('➕ Kanal qo‘shish','a:input:channel_add'),('🗑 Kanal o‘chirish','a:input:channel_del')]]))


def admin_admins(uid):
    with db() as c:admins=[x['id'] for x in c.execute('SELECT id FROM admins').fetchall()]
    safe_send(uid,'👑 <b>Adminlar</b>\nBosh admin: '+str(OWNER_ID)+'\n'+('\n'.join(map(str,admins)) or 'Qo‘shimcha admin yo‘q.'),
              keyboard([[('➕ Admin qo‘shish','a:input:admin_add'),('🗑 Admin o‘chirish','a:input:admin_del')]]))


def admin_settings(uid):
    safe_send(uid,'⚙️ <b>Bot sozlamalari</b>\n\n'
              f'Texnik rejim: {"YOQILGAN" if get("maintenance")=="1" else "O‘CHIRILGAN"}\n'
              f'Min to‘ldirish: {money(param("min_topup"))}',
              keyboard([[('🛠 Texnik rejimni almashtirish','a:maintenance')],
                        [('💳 Min to‘ldirish','a:input:min_topup')],
                        [('✍️ Salomlashuv matni','a:input:welcome')]]))


def admin_users(uid):
    safe_send(uid,'👥 Foydalanuvchi ID raqamini kiriting.',keyboard([[('🔎 ID bo‘yicha qidirish','a:input:user_find')]]))


def user_admin_card(uid,target):
    r=user_row(target)
    if not r:return safe_send(uid,'Bunday foydalanuvchi bazada yo‘q.')
    safe_send(uid,f'👤 <b>Foydalanuvchi</b>\n🆔 <code>{target}</code>\n'
              f'Ism: {esc(r["name"])}\n@{esc(r["username"])}\n'
              f'Balans: <b>{money(r["balance"])}</b>\nBloklangan: {"HA" if r["banned"] else "YO‘Q"}',
              keyboard([[('➕/➖ Balansni tahrirlash','a:balance:'+str(target))],
                        [('🚫 Bloklash / ochish','a:ban:'+str(target))]]))


def admin_tickets(uid):
    with db() as c:ts=c.execute('SELECT * FROM tickets ORDER BY created DESC LIMIT 15').fetchall()
    if not ts:return safe_send(uid,'🎧 Murojaatlar yo‘q.')
    safe_send(uid,'🎧 <b>Murojaatlar</b>',keyboard([[(f'#{t["id"]} User {t["user_id"]} | {t["status"]}',f't:show:{t["id"]}')] for t in ts]))


def ticket_show(uid,tid):
    with db() as c:
        t=c.execute('SELECT * FROM tickets WHERE id=?',(tid,)).fetchone()
        msgs=c.execute('SELECT * FROM ticket_messages WHERE ticket_id=? ORDER BY id DESC LIMIT 7',(tid,)).fetchall()
    if not t:return safe_send(uid,'Murojaat topilmadi.')
    text=f'🎧 <b>Murojaat #{tid}</b>\nUser: <code>{t["user_id"]}</code>\nHolat: {esc(t["status"])}\n\n'
    for m in reversed(msgs):text+=f'{"👤" if m["sender_id"]==t["user_id"] else "👑"}: {esc(m["body"])[:250]}\n'
    safe_send(uid,text[:3500],keyboard([[('↩️ Javob berish',f't:reply:{tid}'),('✅ Yopish',f't:close:{tid}')]]))


def broadcast_admin(uid):
    if not is_admin(uid):return
    set_state(uid,'admin_broadcast')
    safe_send(uid,'📢 Hammaga yuboriladigan matn, rasm yoki video yuboring.\nBekor qilish: /cancel')


def admin_input(uid,k):
    owner_keys={'fragment_key','usd_rate','admin_add','admin_del','payment_card_number','payment_card_name'}
    if k in owner_keys and not only_owner(uid):return safe_send(uid,'🔒 Faqat bosh admin.')
    allowed={'star_price','pack_add','pack_del','min_stars','max_stars','premium_3','premium_6','premium_12',
             'fragment_key','usd_rate','channel_add','channel_del',
             'admin_add','admin_del','min_topup','welcome','user_find',
             'payment_card_number','payment_card_name'}
    if k not in allowed:return
    set_state(uid,'admin_input',{'key':k})
    prompt={
      'star_price':'1 ⭐ narxini so‘mda kiriting (masalan, 250).\nBot 50, 100, 500 va boshqa miqdorlarning narxini o‘zi hisoblaydi.',
      'fragment_key':'Fragment API Key ni yuboring (xabar o‘chiriladi).',
      'payment_card_number':'HUMO karta raqamini kiriting (16–19 raqam). To‘lov paytida foydalanuvchiga shu karta chiqadi.',
      'payment_card_name':'HUMO karta egasining ism va familiyasini kiriting.',
      'usd_rate':'1 USD necha so‘m? (Masalan: 12500).',
      'channel_add':'Kanalni kiriting: @kanal yoki -100... ID. Private kanal uchun: -100...|https://t.me/+invite',
      'channel_del':'O‘chiriladigan kanal @username yoki ID sini kiriting.',
      'admin_add':'Yangi admin Telegram ID sini yuboring.', 'admin_del':'O‘chirish uchun admin ID sini yuboring.',
      'pack_add':'Stars to‘plam miqdorini yuboring (masalan 750).',
      'pack_del':'O‘chirish uchun Stars to‘plam miqdorini yuboring.',
      'user_find':'Foydalanuvchining Telegram ID sini kiriting.',
      'min_topup':'Minimal to‘lov summasini so‘mda yuboring.',
      'min_stars':'Minimal Stars miqdorini kiriting (kamida 50).',
      'max_stars':'Maksimal Stars miqdorini kiriting.',
      'welcome':'Yangi salomlashuv matnini yuboring. HTML ishlatmang.'
    }
    safe_send(uid,'✏️ '+prompt.get(k,'Yangi qiymatni yuboring.'))


def handle_admin_input(m,data):
    global _HUMO_CHECK_NEXT
    uid=m.from_user.id
    k=data.get('key','')
    if not is_admin(uid):return
    if k in {'fragment_key','usd_rate','admin_add','admin_del',
              'payment_card_number','payment_card_name'} and not only_owner(uid):return
    value=(m.text or '').strip()
    if not value:return safe_send(uid,'Matn yuboring.')
    try:
        if k=='fragment_key':
            set_secret(k,value)
            try:bot.delete_message(uid,m.message_id)
            except Exception:pass
            clear_state(uid);safe_send(uid,'✅ Maxfiy kalit shifrlangan holda saqlandi.');return
        if k=='payment_card_number':
            cardval = normalize_card(value)
            if not 16 <= len(cardval) <= 19 or not value.replace(' ','').isdigit():
                raise ValueError('Karta raqami 16–19 raqamdan iborat bo‘lsin.')
            put(k,cardval)
            _HUMO_CHECK_NEXT = 0.0
        elif k=='payment_card_name':
            if not 3<=len(value)<=120 or not re.fullmatch(r"[\w .\-’'ʻҒғЎўҚқҲҳЁёА-Яа-я]+",value,re.UNICODE):
                raise ValueError('Karta egasining ismini to‘g‘ri yuboring (3–120 belgi).')
            put(k,value)
            _HUMO_CHECK_NEXT = 0.0
        elif k=='public_url':
            if not re.fullmatch(r'https://[a-zA-Z0-9.-]+(?::\d{2,5})?',value.rstrip('/')):
                raise ValueError('Masalan: https://shop.onrender.com')
            put(k,value.rstrip('/'))
        elif k=='channel_add':
            parts=value.split('|',1)
            chat=parts[0].strip()
            if not (re.fullmatch(r'@[A-Za-z0-9_]{5,32}',chat) or re.fullmatch(r'-100\d{5,20}',chat)):
                raise ValueError('Kanal: @username yoki -100... formatida bo‘lsin.')
            channel=bot.get_chat(chat)
            member=bot.get_chat_member(channel.id,bot.get_me().id)
            if member.status not in ('creator','administrator'):
                raise ValueError('Bot avval kanalga admin qilinishi kerak.')
            link=parts[1].strip() if len(parts)>1 else ''
            if link and not re.fullmatch(r'https://t\.me/[A-Za-z0-9_+/-]+',link):
                raise ValueError('Taklif havolasi noto‘g‘ri.')
            put('channel_'+str(channel.id),str(channel.id)+'|'+(link or ('https://t.me/'+chat[1:] if chat.startswith('@') else '')))
        elif k=='channel_del':
            with db(True) as c:
                entries=c.execute("SELECT key,val FROM settings WHERE key LIKE 'channel_%'").fetchall()
                found=[r['key'] for r in entries if value in r['val'].split('|',1)[0] or value==r['key'].replace('channel_','')]
                if not found:
                    try: found=['channel_'+str(bot.get_chat(value).id)]
                    except Exception:pass
                for key in found:c.execute('DELETE FROM settings WHERE key=?',(key,))
            if not found:raise ValueError('Kanal topilmadi.')
            with _CHANNEL_CACHE_LOCK:
                _CHANNEL_CACHE['expires'] = 0.0
        elif k in ('admin_add','admin_del'):
            n=int(value)
            if n<=0 or n==OWNER_ID:raise ValueError('Bosh adminni o‘zgartirish mumkin emas.')
            with db(True) as c:
                if k=='admin_add':c.execute('INSERT OR IGNORE INTO admins(id) VALUES(?)',(n,))
                else:c.execute('DELETE FROM admins WHERE id=?',(n,))
        elif k=='user_find':
            n=int(value)
            clear_state(uid)
            return user_admin_card(uid,n)
        elif k=='welcome':
            if len(value)>3500:raise ValueError('Matn juda uzun.')
            put(k,html.escape(value))
        else:
            n=int(value.replace(' ','').replace(',',''))
            if k in ('pack_add','pack_del'):
                if not 50<=n<=10000000:raise ValueError('50—10 000 000 oralig‘i.')
                with db(True) as c:
                    if k=='pack_add':c.execute('INSERT OR REPLACE INTO packages(amount,enabled) VALUES(?,1)',(n,))
                    else:c.execute('DELETE FROM packages WHERE amount=?',(n,))
            else:
                if n<0 or n>1000000000:raise ValueError('Noto‘g‘ri son.')
                if k=='star_price' and n<=0:raise ValueError('1 Stars narxi 0 dan katta bo‘lishi kerak.')
                if k=='min_stars' and n<50:raise ValueError('Fragment API minimal 50 Stars.')
                if k=='max_stars' and not 50<=n<=10000000:raise ValueError('Chegara 50—10 000 000.')
                if k=='usd_rate' and n<1000:raise ValueError('USD/UZS kursi juda kichik.')
                if k=='min_topup' and n<1000:raise ValueError('Minimal 1000 so‘m.')
                put(k,n)
        clear_state(uid)
        if k=='star_price':
            safe_send(uid, f'✅ 1 ⭐ narxi {money(n)} qilib belgilandi.\n'
                           'Barcha Stars to‘plamlari va ixtiyoriy miqdor narxi yangilandi.')
            return admin_star(uid)
        safe_send(uid,'✅ Sozlama saqlandi.')
    except (ValueError,TypeError) as e:
        safe_send(uid,'❌ '+esc(str(e))[:280]+'\nQayta yuboring.')
    except Exception:
        logger.exception('Admin input error')
        safe_send(uid,'❌ Xatolik. Qiymatni qayta tekshiring.')


def handle_admin_adjust(m,data):
    uid=m.from_user.id
    target=int(data['user'])
    s=(m.text or '').replace(' ','').strip()
    if not re.fullmatch(r'[+-]\d{1,10}',s):
        return safe_send(uid,'Masalan +50000 yoki -10000 kiriting.')
    delta=int(s)
    if delta==0 or abs(delta)>10000000:return safe_send(uid,'Bir urinishda ko‘pi bilan 10 mln so‘m.')
    ref='A'+uuid.uuid4().hex[:12]
    with db(True, financial=True) as c:
        r=c.execute('SELECT balance FROM users WHERE id=?',(target,)).fetchone()
        if not r:return safe_send(uid,'Foydalanuvchi topilmadi.')
        if r['balance']+delta<0:return safe_send(uid,'❌ Balans manfiy bo‘lib qoladi.')
        c.execute('UPDATE users SET balance=balance+? WHERE id=?',(delta,target))
        c.execute('INSERT INTO ledger(user_id,delta,reason,ref,created) VALUES(?,?,?,?,?)',
                  (target,delta,f'admin_{uid}',ref,now()))
    clear_state(uid)
    safe_send(uid,f'✅ Balans o‘zgardi: {delta:+,} so‘m')
    safe_send(target,f'💳 Balansingiz administrator tomonidan {money(abs(delta))} ga {"to‘ldirildi" if delta>0 else "kamaytirildi"}.')


_BCAST_WAKE = threading.Event()
_BCAST_STARTED = threading.Event()


def _broadcast_worker():
    while True:
        _BCAST_WAKE.wait(timeout=50)
        _BCAST_WAKE.clear()
        try:
            while True:
                sent = drain_broadcast(limit=8)
                if sent == 0:
                    break
                time.sleep(0.6)
        except Exception:
            logger.exception('Broadcast background worker xatoligi')
            time.sleep(5)


def start_broadcast_worker():
    if _BCAST_STARTED.is_set():
        return
    _BCAST_STARTED.set()
    threading.Thread(target=_broadcast_worker, daemon=True, name='broadcast-worker').start()
    _BCAST_WAKE.set()  # finish pending rows left from before a restart


def send_broadcast(m):
    """WSGI worker ichida background thread emas, saqlanadigan yuborish navbati."""
    uid=m.from_user.id
    if not is_admin(uid):return
    clear_state(uid)
    with db() as c: ids=[r['id'] for r in c.execute('SELECT id FROM users WHERE banned=0').fetchall()]
    job_id='B'+uuid.uuid4().hex[:14].upper()
    with db(True) as c:
        c.executemany('INSERT INTO broadcast_queue(job_id,from_chat,source_message,target_id,status,created) VALUES(?,?,?,?,?,?)',
                      [(job_id,uid,m.message_id,i,'pending',now()) for i in ids])
    safe_send(uid,f'📢 Xabar {len(ids)} foydalanuvchi navbatiga qo‘shildi.\n'
                  '⏱ Qolgan xabarlar server faol turganida avtomatik yuboriladi.')
    _BCAST_WAKE.set()


def drain_broadcast(limit=5):
    with db() as c:
        rows=c.execute("SELECT * FROM broadcast_queue WHERE status='pending' ORDER BY id LIMIT ?",(limit,)).fetchall()
    sent=0
    for row in rows:
        # Bir xil qatorni boshqa worker parallel yubormasligi uchun atomic claim.
        with db(True) as c:
            claim=c.execute("UPDATE broadcast_queue SET status='sending' WHERE id=? AND status='pending'",(row['id'],))
        if not claim.rowcount:continue
        try:
            bot.copy_message(row['target_id'],row['from_chat'],row['source_message'])
            state='sent';sent+=1
        except Exception:
            state='error';logger.exception('Broadcast user: %s',row['target_id'])
        with db(True) as c:
            c.execute('UPDATE broadcast_queue SET status=? WHERE id=?',(state,row['id']))
        time.sleep(0.07)
    return sent


@app.get('/')
@app.get('/health')
def site_home():
    return jsonify({'ok':True,'service':'Stars Shop / Render Webhook','sales_enabled':LIVE_SALES_ENABLED})


@app.get('/internal/payment-check/<token>')
def internal_payment_check(token):
    """Maxfiy monitoring endpoint. UptimeRobot HTTP(s) monitor yoki cron uchun."""
    if not hmac.compare_digest(token, get('payment_monitor_token')):
        return jsonify({'ok': False}), 403
    count=payment_sweep(limit=6)
    sent=drain_broadcast(limit=4)
    return jsonify({'ok':True,'payments_checked':count,'broadcast_sent':sent})


@app.post('/telegram/<token>')
def telegram_webhook(token):
    """pyTelegramBotAPI uchun Telegram webhook; majburiy header secret va update dedup."""
    if not hmac.compare_digest(token, get('telegram_webhook_path')):
        return jsonify({'ok':False}),403
    header=request.headers.get('X-Telegram-Bot-Api-Secret-Token','')
    if not hmac.compare_digest(header, get('telegram_header_secret')):
        return jsonify({'ok':False}),403
    if not request.is_json or len(request.data)>1024*1024:
        return jsonify({'ok':False,'error':'JSON required'}),415
    try:
        raw=request.get_json()
        update_id=int(raw['update_id'])
        update=telebot.types.Update.de_json(json.dumps(raw))
    except (KeyError,ValueError,TypeError):
        return jsonify({'ok':False,'error':'invalid update'}),400
    # Xuddi shu update Telegram tomonidan takror yuborilsa, ikki marta bajarmaymiz.
    with db(True, webhook_lock=True) as c:
        row=c.execute('SELECT status,arrived FROM telegram_updates WHERE update_id=?',(update_id,)).fetchone()
        if row:
            if row['status']=='done':
                return jsonify({'ok':True,'duplicate':True})
            if row['status']=='processing':
                # Don't acknowledge a concurrent retry while the first worker
                # may still be handling a payment or delivery callback.
                try:
                    started=datetime.strptime(row['arrived'],'%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                    active=(datetime.now(timezone.utc)-started).total_seconds()<300
                except (ValueError,TypeError):
                    active=False
                if active:
                    return jsonify({'ok':False,'retry_later':True}),503
            # Failed/stale updates can be retried. Order/bank ledger state has
            # separate idempotency keys; never auto-retry uncertain deliveries.
            c.execute('UPDATE telegram_updates SET status=?,arrived=? WHERE update_id=?',
                      ('processing',now(),update_id))
        else:
            c.execute('INSERT INTO telegram_updates(update_id,status,arrived) VALUES(?,?,?)',
                      (update_id,'processing',now()))
    started = time.monotonic()
    metrics = {'sent': 0, 'failed': 0}
    kind = next((k for k in ('message','callback_query','pre_checkout_query') if k in raw), 'other')
    token_metrics = _WEBHOOK_METRICS.set(metrics)
    logger.info('Webhook IN update=%s kind=%s', update_id, kind)
    try:
        chat_uid = 0
        if isinstance(raw.get('message'), dict):
            chat_uid = int(raw['message'].get('chat', {}).get('id') or 0)
        elif isinstance(raw.get('callback_query'), dict):
            chat_uid = int(raw['callback_query'].get('from', {}).get('id') or 0)
        elif isinstance(raw.get('pre_checkout_query'), dict):
            chat_uid = int(raw['pre_checkout_query'].get('from', {}).get('id') or 0)
        with _CHAT_PROCESS_LOCKS[chat_uid % len(_CHAT_PROCESS_LOCKS)]:
            bot.process_new_updates([update])
    except Exception:
        logger.exception('Telegram webhook update failed: %s',update_id)
        with db(True, webhook_lock=True) as c:
            c.execute("UPDATE telegram_updates SET status='failed' WHERE update_id=?",(update_id,))
        # Fail-closed: API buyurtma bajarilgan-bajarilmagan noma'lum bo'lishi mumkin.
        return jsonify({'ok':False,'error':'update failed; manual review'}),500
    finally:
        _WEBHOOK_METRICS.reset(token_metrics)
        logger.info('Webhook OUT update=%s kind=%s send_ok=%s send_failed=%s duration_ms=%s',
                    update_id, kind, metrics['sent'], metrics['failed'],
                    int((time.monotonic()-started)*1000))
    # Read-only menu requests should not be acknowledged as delivered if no
    # Telegram message was accepted; they may safely be retried. Purchase and
    # payment callbacks are NEVER auto-replayed on uncertain outcomes.
    raw_text = str((raw.get('message') or {}).get('text') or '').strip()
    cmd = raw_text.split(' ', 1)[0].split('@', 1)[0]
    read_only_commands = {'/start','/id','/menu','/admin','/terms','/paysupport'}
    menu_texts = {'⭐ Stars olish','💎 Premium olish','👥 Referal','🎁 Gift olish',
                  '💰 Hisobim','💳 Hisob to‘ldirish','📦 Buyurtmalarim','🎧 Yordam',
                  '🏠 User menyu','🏠 Bosh menyu','⬅️ Orqaga'}
    should_reply = cmd in read_only_commands or raw_text in menu_texts
    if should_reply and (metrics['failed'] or metrics['sent'] == 0):
        logger.warning('Bot menu reply unavailable: update=%s text=%s sent=%s failed=%s',
                       update_id, raw_text[:32], metrics['sent'], metrics['failed'])
        with db(True, webhook_lock=True) as c:
            c.execute("UPDATE telegram_updates SET status='failed' WHERE update_id=?",(update_id,))
        return jsonify({'ok':False,'retry':'sendMessage unavailable'}),503
    with db(True, webhook_lock=True) as c:
        c.execute("UPDATE telegram_updates SET status='done' WHERE update_id=?",(update_id,))
        # Cheklanmagan ko'payishni oldini olish (so'nggi 20 ming update saqlanadi).
        if update_id % 256 == 0:
            c.execute('DELETE FROM telegram_updates WHERE update_id < (SELECT COALESCE(MAX(update_id),0)-20000 FROM telegram_updates)')
    return jsonify({'ok':True})





@bot.pre_checkout_query_handler(func=lambda q: True)
def owner_precheckout(q):
    try:
        info=str(q.invoice_payload).split(':')
        valid=(len(info)==4 and info[0]=='owner_stars' and q.from_user.id==OWNER_ID
               and int(info[1])==OWNER_ID and int(info[2])==q.total_amount
               and q.currency=='XTR' and 1<=q.total_amount<=100000)
        bot.answer_pre_checkout_query(q.id,ok=bool(valid),
             error_message=None if valid else 'To‘lov faqat bosh admin uchun.')
    except Exception:
        bot.answer_pre_checkout_query(q.id,ok=False,error_message='Invoice tekshiruvida xatolik.')


@bot.message_handler(content_types=['successful_payment'])
def owner_stars_payment(m):
    if m.chat.type!='private':return
    sp=m.successful_payment
    try:
        payload=str(sp.invoice_payload).split(':')
        if (m.from_user.id!=OWNER_ID or len(payload)!=4 or payload[0]!='owner_stars'
            or int(payload[1])!=OWNER_ID or sp.currency!='XTR'
            or int(payload[2])!=int(sp.total_amount)):
            return
        charge=sp.telegram_payment_charge_id
        with db(True, financial=True) as c:
            new=c.execute('INSERT OR IGNORE INTO bot_star_deposits(charge_id,user_id,stars,created) VALUES(?,?,?,?)',
                          (charge,OWNER_ID,sp.total_amount,now()))
        if new.rowcount:
            safe_send(OWNER_ID,f'✅ Bot hisobiga <b>{sp.total_amount} ⭐</b> to‘landi.')
    except Exception:
        logger.exception('Owner Stars payment handler')


@bot.message_handler(commands=['bankstatus','bankapprove'])
def bank_commands(m):
    if m.chat.type!='private' or m.from_user.id!=OWNER_ID:return
    if m.text.strip().startswith('/bankstatus'):
        status='✅ Ulangan' if HUMO_LISTENER_CONNECTED.is_set() else '❌ Ulanmagan'
        return safe_send(m.chat.id,f'🏦 HUMO kuzatuv: {status}\nProvayder: {esc(PAYMENT_PROVIDER)}\nAvto kredit: {HUMO_AUTO_CREDIT}\nSender ID: {HUMO_SENDER_ID}\nKarta: ****{esc(expected_humo_last4())}')
    parts=m.text.strip().split()
    if len(parts)!=2:return safe_send(m.chat.id,'Format: /bankapprove BXXXXXXXXXXXXXXX')
    return safe_send(m.chat.id,esc(approve_bank_payment(m.from_user.id,parts[1])))

@bot.message_handler(commands=['diag'])
def owner_diagnostics(m):
    """Bosh admin uchun webhook va PostgreSQL holatini bitta xabarda ko'rsatadi."""
    if m.chat.type != 'private' or m.from_user.id != OWNER_ID:
        return
    started = time.monotonic()
    try:
        with db() as c:
            c.execute('SELECT 1').fetchone()
        db_line = f'✅ Ulandi ({(time.monotonic() - started)*1000:.0f} ms)'
    except Exception as exc:
        db_line = f'❌ {esc(type(exc).__name__)}'
        logger.exception('Owner /diag: PostgreSQL tekshiruv')
    try:
        info = bot.get_webhook_info()
        webhook_line = ('✅ Ulangan' if info.url else '❌ Webhook bo‘sh')
        webhook_line += f'\n📨 Navbat: {info.pending_update_count}'
        if info.last_error_message:
            webhook_line += '\n⚠️ Oxirgi xato: '+esc(info.last_error_message)[:300]
    except Exception as exc:
        webhook_line = f'❌ {esc(type(exc).__name__)}'
        logger.exception('Owner /diag: getWebhookInfo tekshiruv')
    safe_send(m.chat.id,
              '🔧 <b>Bot diagnostikasi</b>\n\n'
              '🗄 PostgreSQL: '+db_line+'\n'
              '🌐 Webhook: '+webhook_line+'\n'
              '🏦 HUMO monitor: '+('✅ ulangan' if HUMO_LISTENER_CONNECTED.is_set() else '❌ ulanmagan')+'\n'
              '🛒 Savdo rejimi: '+('YOQIQ' if LIVE_SALES_ENABLED else 'O‘CHIQ')+'\n\n'
              '⚠️ Bu faqat texnik holat; haqiqiy to‘lov sinovi emas.')


@bot.message_handler(commands=['start','menu','admin','cancel','id','paysupport','terms'])
def commands(m):
    if m.chat.type!='private':return
    uid=m.from_user.id
    first_visit=user_row(uid) is None
    user_record(m.from_user)
    if m.text.startswith('/start '):
        match=re.fullmatch(r'/start\s+ref_(\d{1,20})\s*',m.text.strip())
        if match:referral_register(uid,int(match.group(1)),first_visit)
    if m.text.startswith('/id'):
        return safe_send(uid,f'🆔 Telegram ID: <code>{uid}</code>')
    if m.text.startswith('/paysupport'):
        set_state(uid,'user_support')
        return safe_send(uid,'🎧 To‘lov yoki buyurtma yuzasidan murojaatingizni yuboring. Pul o‘tkazgan bo‘lsangiz to‘lov vaqti va summasini yozing; maxfiy kod yoki parolni yubormang.')
    if m.text.startswith('/terms'):
        return safe_send(uid,'📜 <b>Xizmat ma’lumotlari</b>\nNarx va mavjudlikni buyurtmadan oldin tekshiring.\nTo‘lovni tasdiqlashdan oldin karta raqami va summani solishtiring.\nTashqi API ishlamay qolsa, buyurtma tekshiruvga olinadi.\nSavollar uchun /paysupport.')
    if m.text.startswith('/cancel'):
        main_menu(uid);return
    if m.text.startswith('/admin'):
        return admin_menu(uid) if is_admin(uid) else safe_send(uid,'⛔ Admin huquqi yo‘q.')
    if not check_access(uid):return
    main_menu(uid)


@bot.callback_query_handler(func=lambda call: True)
def callbacks(call):
    uid=call.from_user.id
    if call.message.chat.type!='private':return
    user_record(call.from_user)
    s=call.data or ''
    try:bot.answer_callback_query(call.id)
    except Exception:pass
    # SECURITY: admin callbacks must be denied BEFORE any gift/referral action.
    # Otherwise a normal user could toggle catalog prices or referral settings
    # by crafting callback_data, even if the admin menu itself is hidden.
    if s.startswith('a:') and not is_admin(uid):
        return safe_send(uid,'⛔ Admin huquqi yo‘q.')
    if s=='g:list':return gift_catalog(uid)
    if s.startswith('g:view:'):
        if not check_access(uid):return
        return gift_preview(uid,int(s.rsplit(':',1)[1]))
    if s.startswith('g:self:'):
        if not check_access(uid):return
        slot=int(s.rsplit(':',1)[1])
        return begin_gift_review(uid,slot,uid,'O‘zimga'+(' (@'+call.from_user.username+')' if call.from_user.username else ''))
    if s.startswith('g:friend:'):
        if not check_access(uid):return
        slot=int(s.rsplit(':',1)[1]);set_state(uid,'gift_friend',{'slot':slot})
        return safe_send(uid,'🎁 Do‘stingizning <b>@username</b>ini kiriting.\n\n'
                             'Do‘stingiz sovg‘ani olishi uchun avval botni /start orqali ochgan bo‘lishi kerak.')
    if s=='g:confirm':
        if not check_access(uid):return
        if not LIVE_SALES_ENABLED:
            return safe_send(uid,'Sovg‘a xaridi vaqtincha o‘chirilgan.')
        if not _PURCHASE_CAPACITY.acquire(blocking=False):
            return safe_send(uid,'⏳ Xaridlar navbati band. Birozdan keyin urining.')
        submitted = False
        try:
            with db(True) as c:
                st = c.execute('SELECT step FROM states WHERE user_id=?',(uid,)).fetchone()
                if not st or st['step']!='gift_review':
                    return safe_send(uid,'⏳ Sovg‘a allaqachon tekshirilmoqda yoki bajarilgan.')
                c.execute("UPDATE states SET step='gift_processing' WHERE user_id=?",(uid,))
            _PURCHASE_POOL.submit(_confirm_gift_async,uid)
            submitted = True
            return safe_send(uid,'🎁 Sovg‘a tekshirilmoqda. Tasdiqlashni qayta bosmang.')
        except Exception:
            logger.exception('Gift xaridini navbatga qo‘shish xatosi')
            with db(True) as c:
                c.execute("UPDATE states SET step='gift_review' WHERE user_id=? AND step='gift_processing'",(uid,))
            return safe_send(uid,'⚠️ Sovg‘ani tekshirishda xatolik. Qayta urinib ko‘ring.')
        finally:
            if not submitted:
                _PURCHASE_CAPACITY.release()
    if s=='r:copy':
        # Referral menu displays a copyable link; copy button is actual clipboard action.
        try:uname=bot_username()
        except Exception:return
        link=f'https://t.me/{uname}?start=ref_{uid}'
        k=types.InlineKeyboardMarkup()
        k.add(types.InlineKeyboardButton('📋 Havolani nusxalash',
                   copy_text=types.CopyTextButton(text=link)))
        return safe_send(uid,'🔗 Referal havolangiz:',k)
    if s=='r:transfer':return referral_transfer(uid)
    if s=='a:gift:menu':return admin_gift_menu(uid)
    if s=='a:gift:sync':return admin_sync_gifts(uid)
    if s.startswith('a:gift:'):
        slot=s.rsplit(':',1)[1]
        if slot.isdigit():return admin_gift_slot(uid,int(slot))
    if s.startswith('a:giftname:') or s.startswith('a:giftprice:'):
        part=s.split(':')
        slot=int(part[-1]);key='name' if part[1]=='giftname' else 'price'
        set_state(uid,'admin_gift_edit',{'slot':slot,'field':key})
        return safe_send(uid,'✏️ '+('Sovg‘aning yangi o‘zbekcha nomini yuboring:' if key=='name'
                                    else '💵 Yangi narxni so‘mda kiriting:'))
    if s.startswith('a:giftpick:'):
        return admin_gift_choices(uid,int(s.rsplit(':',1)[1]))
    if s.startswith('a:gset:'):
        _,_,slot,index=s.split(':')
        slot,index=int(slot),int(index)
        try:
            gift_ids=json.loads(get('gift_pick_ids_'+str(uid),'[]'))
            gift_id=gift_ids[index]
            item=live_gift(gift_id)
            if item is None:return safe_send(uid,'Bu Gift hozir sotuvda yo‘q.')
            stk=item.get('sticker') or {}
            with db(True) as c:
                c.execute('UPDATE gifts SET gift_id=?,sticker_file_id=?,stars_cost=?,enabled=0 WHERE slot=?',
                          (str(item['id']),str(stk.get('file_id','')),int(item['star_count']),slot))
            safe_send(uid,'✅ Gift tanlandi. Sotuv narxini qo‘yib, yoqing.')
            return admin_gift_slot(uid,slot)
        except Exception:return safe_send(uid,'Sovg‘ani qayta tanlang.')
    if s.startswith('a:gifttoggle:'):
        slot=int(s.rsplit(':',1)[1]);g=gift_row(slot)
        if not g:return
        if not g['gift_id'] or g['price']<=0:return safe_send(uid,'Avval haqiqiy Gift va so‘m narxini tanlang.')
        with db(True) as c:c.execute('UPDATE gifts SET enabled=1-enabled WHERE slot=?',(slot,))
        return admin_gift_slot(uid,slot)
    if s=='a:ref:rate':
        set_state(uid,'admin_ref_percent')
        return safe_send(uid,'Referal foizini kiriting (0–20):')
    if s=='a:ref:toggle':
        put('referral_enabled','0' if get('referral_enabled')=='1' else '1')
        return admin_referral_menu(uid)
    if s=='a:stars:menu':return admin_bot_stars(uid)
    if s.startswith('a:stars:add:'):
        return invoice_for_owner(uid,int(s.rsplit(':',1)[1]))
    if s=='a:stars:custom':
        if not only_owner(uid):return
        set_state(uid,'admin_stars_deposit')
        return safe_send(uid,'⭐ Nechta Starsni bot hisobiga o‘tkazasiz? (1–100 000):')
    if s=='sub:check':
        if check_access(uid):main_menu(uid)
        return
    if s=='home':return main_menu(uid)
    if (s.startswith('a:') or (s.startswith('t:') and s not in ('t:self','t:other'))) and not is_admin(uid):
        return safe_send(uid,'⛔ Ruxsat yo‘q.')
    if s.startswith('a:') and not check_access(uid):return
    if s in ('s:custom','top:other'):
        if not check_access(uid):return
        set_state(uid,'stars_custom' if s=='s:custom' else 'topup_other')
        return safe_send(uid,f'🔢 Stars miqdorini kiriting ({param("min_stars"):,}–{param("max_stars"):,}).\nNarx: miqdor × {money(param("star_price"))}' if s=='s:custom' else '💳 To‘lov summasini so‘mda kiriting:')
    if s.startswith('s:pack:'):
        if not check_access(uid):return
        return choose_target(uid,'stars',int(s.split(':')[2]))
    if s.startswith('p:pack:'):
        if not check_access(uid):return
        return choose_target(uid,'premium',int(s.split(':')[2]))
    if s in ('t:self','t:other'):
        if not check_access(uid):return
        step,data=state(uid)
        if step!='choose_target':return safe_send(uid,'Xaridni qaytadan boshlang.')
        set_state(uid,'enter_recipient',data)
        if s=='t:self' and call.from_user.username:
            username=extract_username(call.from_user.username)
            if username:return preview_recipient(uid,username)
        return safe_send(uid,('🔗 Sizda umumiy username yo‘q. Telegram profilingizga username qo‘ying va @username yoki t.me/username yuboring.'
                             if s=='t:self' else '🎁 Qabul qiluvchining @username yoki https://t.me/username havolasini yuboring.'))
    if s=='buy:confirm':
        if not check_access(uid):return
        if not LIVE_SALES_ENABLED:return safe_send(uid,'Xarid vaqtincha o‘chirilgan.')
        if not read_secret('fragment_key'):return safe_send(uid,'❌ API kaliti sozlanmagan.')
        if not _PURCHASE_CAPACITY.acquire(blocking=False):
            return safe_send(uid,'⏳ Xaridlar navbati band. Qayta urinib ko‘ring.')
        # Mark claimed INSIDE one transaction. Two rapid clicks cannot debit twice.
        submitted = False
        try:
            with db(True) as c:
                record=c.execute('SELECT step,data FROM states WHERE user_id=?',(uid,)).fetchone()
                if not record or record['step']!='review_order':
                    return safe_send(uid,'⏳ Buyurtma allaqachon tekshirilmoqda yoki bajarilgan.')
                data=json.loads(record['data'])
                c.execute("UPDATE states SET step='checking_price' WHERE user_id=?",(uid,))
            _PURCHASE_POOL.submit(_confirm_purchase_async,uid,data)
            submitted = True
            return safe_send(uid,'🔎 Tannarx tekshirilmoqda. Buyurtmani qayta bosmang.')
        except Exception:
            logger.exception('Xaridni navbatga qo‘shish xatosi')
            with db(True) as c:
                c.execute("UPDATE states SET step='review_order' WHERE user_id=? AND step='checking_price'",(uid,))
            return safe_send(uid,'⚠️ Xaridni boshlashda xatolik. Qayta urinib ko‘ring.')
        finally:
            if not submitted:
                _PURCHASE_CAPACITY.release()
    if s=='top:menu':return topup_menu(uid)
    if s.startswith('top:sum:'):
        if not check_access(uid):return
        return create_payment(uid,int(s.split(':')[2]))
    if s=='pay:accept_suggest':
        if not check_access(uid):return
        step,data=state(uid)
        if step!='payment_suggestion':return safe_send(uid,'Summa taklifi eskirgan. Qayta boshlang.')
        clear_state(uid)
        return create_payment(uid,int(data['amount']))
    if s.startswith('pay:cancel:'):
        if not check_access(uid):return
        pid=s.split(':',2)[2]
        return cancel_pending_payment(uid,pid)
    if s.startswith('pay:check:'):
        pid=s.split(':',2)[2]
        with db() as c:p=c.execute('SELECT user_id FROM payments WHERE id=?',(pid,)).fetchone()
        if not p or (p['user_id']!=uid and not is_admin(uid)):
            return safe_send(uid,'⛔ Bunday to‘lovga ruxsat yo‘q.')
        result=verify_payment(pid)
        if PAYMENT_PROVIDER=='bank_session' and result=='pending':
            safe_send(uid,'⏳ To‘lovingiz hali tasdiqlanmadi. Pul o‘tkazgan bo‘lsangiz, chekni saqlang.')
        if result=='unavailable':safe_send(uid,'⏳ Provayderdan javob olinmadi. Avtomatik tekshirish davom etadi.')
        if result=='mismatch':safe_send(uid,'⚠️ To‘lov ma’lumotlari mos kelmadi. Admin tekshiradi.')
        return payment_card(uid,pid,admin=is_admin(uid))
    if s.startswith('pay:show:'):return payment_card(uid,s.split(':',2)[2])
    if s.startswith('o:show:'):return order_card(uid,s.split(':',2)[2])
    if s=='u:ledger':return balance_history(uid)
    if s=='u:orders':return my_orders(uid)
    if s=='a:orders':return admin_orders(uid)
    if s.startswith('a:order:'):return order_card(uid,s.split(':',2)[2],admin=True)
    if s.startswith('a:pay:'):return payment_card(uid,s.split(':',2)[2],admin=True)
    if s.startswith('a:input:'):return admin_input(uid,s.split(':',2)[2])
    if s=='a:maintenance':
        if not only_owner(uid):return safe_send(uid,'Faqat bosh admin.')
        put('maintenance','0' if get('maintenance')=='1' else '1');return admin_settings(uid)
    if s=='a:testfrag':
        try:
            d=fragment('/wallet/balance',{})
            safe_send(uid,'✅ Fragment API javobi:\n'+esc(json.dumps(d,ensure_ascii=False)[:1700]))
        except Exception as e:safe_send(uid,'❌ '+esc(str(e)))
        return
    if s=='a:wallet':
        try:
            d=fragment('/wallet/balance',{})
            safe_send(uid,'💼 <b>Fragment hamyon</b>\n'+esc(json.dumps(d.get('result',d),ensure_ascii=False)[:1900]))
        except Exception as e:safe_send(uid,'❌ '+esc(str(e)))
        return
    if s=='a:paytoggle':
        if not only_owner(uid):return
        if not LIVE_SALES_ENABLED:return safe_send(uid,'🔒 Savdo rejimi o‘chirilgan: ENABLE_LIVE_SALES=1 kiriting.')
        if get('payment_enabled')=='1':
            put('payment_enabled','0')
            return safe_send(uid,'🛑 Yangi to‘lovlar to‘xtatildi. Oldingi to‘lovlar HUMO xabarlari asosida tekshiriladi.')
        if not payment_configured():
            return safe_send(uid,'❌ To‘lov integratsiyasi sozlanmagan yoki kuzatuv ishlamayapti.')
        return safe_send(uid,('⚠️ HUMO bank-bot xabari imzolangan bank API tasdig‘i emas.\n'
                              'Tranzaksiya ID, karta va summaga mos xabar bo‘lsa avto ishlaydi.'),
                         keyboard([[('⚠️ TASDIQLAYMAN — YOQISH','a:payenable')]]))
    if s=='a:payenable':
        if not only_owner(uid):return
        if not LIVE_SALES_ENABLED:return safe_send(uid,'🔒 Savdo rejimi o‘chirilgan: ENABLE_LIVE_SALES=1 kiriting.')
        if not payment_configured():return safe_send(uid,'❌ To‘lov sozlamalari to‘liq emas.')
        put('payment_enabled','1')
        return safe_send(uid,'✅ HUMO to‘lov kuzatuvi yoqildi. /bankstatus bilan tekshiring.')
    if s.startswith('a:balance:'):
        target=int(s.rsplit(':',1)[1]);set_state(uid,'admin_balance',{'user':target})
        return safe_send(uid,f'User {target} balansi uchun +50000 yoki -10000 yuboring.')
    if s.startswith('a:ban:'):
        target=int(s.rsplit(':',1)[1])
        if target in admin_ids():return safe_send(uid,'Adminni bloklab bo‘lmaydi.')
        with db(True) as c:c.execute('UPDATE users SET banned=1-banned WHERE id=?',(target,))
        return user_admin_card(uid,target)
    if s.startswith('a:done:') or s.startswith('a:refund:'):
        oid=s.split(':',2)[2]
        with db() as c:o=c.execute('SELECT status FROM orders WHERE id=?',(oid,)).fetchone()
        if not o or o['status']!='review':return safe_send(uid,'Buyurtma tekshiruvda emas.')
        action='done' if s.startswith('a:done:') else 'refund'
        choice = 'YETKAZILGAN' if action=='done' else 'PULNI QAYTARISH'
        return safe_send(uid,f'⚠️ {oid}: {choice}\n'
                 'Avval Fragment dashboarddagi real natijani tekshiring! Tasdiqlaysizmi?',
                 keyboard([[('⚠️ HA, TASDIQLAYMAN',f'a:resolve:{action}:{oid}')],[('❌ Orqaga',f'a:order:{oid}')]]))
    if s.startswith('a:resolve:'):
        _,_,action,oid=s.split(':',3)
        if action not in ('refund','done'):return
        o=finish_order(oid,'refunded' if action=='refund' else 'completed','Admin tekshirgan: '+str(uid))
        if o:
            safe_send(o['user_id'],f'🧾 Buyurtma {oid}: '+('pul balansga qaytarildi' if action=='refund' else 'yetkazish tasdiqlandi'))
            safe_send(uid,'✅ Holat o‘zgartirildi.')
        return
    if s.startswith('t:'):
        _,action,n=s.split(':',2)
        tid=int(n)
        with db() as c:t=c.execute('SELECT * FROM tickets WHERE id=?',(tid,)).fetchone()
        if not t:return safe_send(uid,'Murojaat topilmadi.')
        if action=='show':return ticket_show(uid,tid)
        if action=='reply':
            set_state(uid,'admin_reply',{'ticket':tid})
            return safe_send(uid,f'✍️ Murojaat #{tid} uchun javobni yuboring (matn, rasm, video).')
        if action=='close':
            with db(True) as c:c.execute('UPDATE tickets SET status=?,updated=? WHERE id=?',('closed',now(),tid))
            return safe_send(uid,'✅ Murojaat yopildi.')

    logger.info('Noma’lum inline tugma user=%s prefix=%s',uid, s.split(':',1)[0][:15])
    return safe_send(uid,'⚠️ Bu tugma eskirgan. /start orqali menyuni qayta oching.')


@bot.message_handler(content_types=['text','photo','video','document','voice','audio','sticker'])
def messages(m):
    if m.chat.type!='private':return
    uid=m.from_user.id
    user_record(m.from_user)
    text=(m.text or '').strip()
    if text in ('🏠 User menyu','🏠 Bosh menyu','⬅️ Orqaga'):
        return main_menu(uid)
    if text=='/cancel':return main_menu(uid)
    if text=='👑 Admin panel' and is_admin(uid):return admin_menu(uid)
    if text in ('⭐ Stars olish','💎 Premium olish','🎁 Gift olish','👥 Referal','💳 Hisob to‘ldirish','💰 Hisobim','📦 Buyurtmalarim','🎧 Yordam'):
        # Five submenus check permissions themselves; avoid double DB/network checks.
        if text in ('💰 Hisobim','📦 Buyurtmalarim','🎧 Yordam') and not check_access(uid):return
        clear_state(uid)
        if text=='⭐ Stars olish':return stars_menu(uid)
        if text=='💎 Premium olish':return premium_menu(uid)
        if text=='🎁 Gift olish':return gift_catalog(uid)
        if text=='👥 Referal':return referral_menu(uid)
        if text=='💳 Hisob to‘ldirish':return topup_menu(uid)
        if text=='💰 Hisobim':return profile(uid)
        if text=='📦 Buyurtmalarim':return my_orders(uid)
        if text=='🎧 Yordam':
            set_state(uid,'user_support')
            return safe_send(uid,'🎧 <b>Adminga murojaat</b>\nSavolingizni matn, rasm yoki video ko‘rinishida yuboring.\nBekor: /cancel')
    admin_texts={
        '⭐ Stars narxlari':admin_star,'💎 Premium narxlari':admin_premium,
        '🎁 Gift boshqaruvi':admin_gift_menu,'👥 Referal sozlamalari':admin_referral_menu,
        '⭐ Bot Stars balansi':admin_bot_stars,
        '📦 Buyurtmalar':admin_orders,'👥 Foydalanuvchilar':admin_users,
        '📊 Statistika':admin_stats,'💳 To‘lovlar':admin_payments,
        '🔌 API va to‘lov':admin_integrations,'📢 Xabar yuborish':broadcast_admin,
        '📣 Majburiy obuna':admin_channels,'👑 Adminlar':admin_admins,
        '🎧 Murojaatlar':admin_tickets,'⚙️ Sozlamalar':admin_settings,
    }
    if text in admin_texts:
        if not is_admin(uid):return safe_send(uid,'⛔ Ruxsat yo‘q.')
        clear_state(uid)
        return admin_texts[text](uid)
    if not check_access(uid):return
    step,data=state(uid)
    if step=='admin_gift_edit':
        if not is_admin(uid):return
        slot,field=int(data['slot']),data['field']
        if field=='name':
            if not 2<=len(text)<=42:return safe_send(uid,'Nom 2–42 belgidan iborat bo‘lsin.')
            with db(True) as c:c.execute('UPDATE gifts SET name=? WHERE slot=?',(text,slot))
        else:
            try:value=int(text.replace(' ','').replace(',',''))
            except ValueError:return safe_send(uid,'Narxni raqamda yuboring.')
            if not 100<=value<=100000000:return safe_send(uid,'Narx 100–100 000 000 so‘m oralig‘ida bo‘lsin.')
            with db(True) as c:c.execute('UPDATE gifts SET price=? WHERE slot=?',(value,slot))
        clear_state(uid);return admin_gift_slot(uid,slot)
    if step=='admin_ref_percent':
        if not is_admin(uid):return
        try:pct=int(text)
        except ValueError:return safe_send(uid,'0–20 oralig‘idagi sonni kiriting.')
        if not 0<=pct<=20:return safe_send(uid,'0–20 oralig‘idagi sonni kiriting.')
        put('referral_percent',pct);clear_state(uid);return admin_referral_menu(uid)
    if step=='admin_stars_deposit':
        if not only_owner(uid):return
        try:qty=int(text.replace(' ',''))
        except ValueError:return safe_send(uid,'Butun son kiriting.')
        clear_state(uid);return invoice_for_owner(uid,qty)
    if step=='admin_input':return handle_admin_input(m,data)
    if step=='admin_balance':
        if is_admin(uid):return handle_admin_adjust(m,data)
        return
    if step=='admin_broadcast':return send_broadcast(m)
    if step=='admin_reply':
        if not is_admin(uid):return
        tid=data.get('ticket')
        with db() as c:t=c.execute('SELECT * FROM tickets WHERE id=?',(tid,)).fetchone()
        if not t:return safe_send(uid,'Murojaat topilmadi.')
        with db(True) as c:
            c.execute('INSERT INTO ticket_messages(ticket_id,sender_id,body,created) VALUES(?,?,?,?)',
                      (tid,uid,str(m.text or m.caption or '['+m.content_type+']')[:2000],now()))
            c.execute('UPDATE tickets SET status=?,updated=? WHERE id=?',('answered',now(),tid))
        safe_send(t['user_id'],f'🎧 <b>Admin javobi, murojaat #{tid}</b>')
        try:bot.copy_message(t['user_id'],uid,m.message_id)
        except Exception:safe_send(uid,'⚠️ Javobni foydalanuvchiga yuborib bo‘lmadi.')
        clear_state(uid);return safe_send(uid,'✅ Javob yuborildi.')
    if step=='user_support':return report_ticket(uid,m)
    if m.content_type!='text':return safe_send(uid,'Iltimos, matn yuboring yoki /cancel bosing.')
    if step=='gift_friend':
        name=extract_username(text)
        if not name:return safe_send(uid,'Do‘stingizning @username ini kiriting.')
        friend=resolve_friend_username(name)
        if not friend:
            return safe_send(uid,'⚠️ Bu username hali botda topilmadi. Do‘stingiz /start bosgach qayta urinib ko‘ring.')
        return begin_gift_review(uid,int(data['slot']),friend,'@'+name)
    if step=='stars_custom':
        try:qty=int(text.replace(' ','').replace(',',''))
        except ValueError:return safe_send(uid,'❌ Butun son kiriting.')
        if not param('min_stars')<=qty<=param('max_stars'):
            return safe_send(uid,f'❌ {param("min_stars")} — {param("max_stars")} oralig‘ida kiriting.')
        return choose_target(uid,'stars',qty)
    if step=='enter_recipient':
        name=extract_username(text)
        if not name:return safe_send(uid,'❌ Faqat @username yoki https://t.me/username yuboring. Raqamli ID yoki taklif havolasi o‘tmaydi.')
        return preview_recipient(uid,name)
    if step=='topup_other':
        try:amount=int(text.replace(' ','').replace(',',''))
        except ValueError:return safe_send(uid,'❌ To‘g‘ri summa kiriting.')
        clear_state(uid);return create_payment(uid,amount)
    return main_menu(uid)


def initialize_runtime():
    """Gunicorn import paytida PostgreSQL/SQLite va shifrlashni tayyorlash."""
    global CIPHER
    setup_db()
    CIPHER=init_cipher()
    # Verification jobs are not monetary debits: recover if Render restarted.
    with db(True) as c:
        c.execute("UPDATE states SET step='review_order' WHERE step='checking_price'")
        c.execute("UPDATE states SET step='gift_review' WHERE step='gift_processing'")
    # O'rtada server to'xtagan buyurtmalarni qayta yubormaymiz.
    # 5 daqiqadan eski processing holati qo'lda tekshirishga yuboriladi.
    cutoff=(datetime.now(timezone.utc)-timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S UTC')
    with db(True) as c:
        c.execute("UPDATE orders SET status='review', detail='Server restart/timeout: Fragment dashboarddan tekshiring',updated=? WHERE status='processing' AND updated<?",(now(),cutoff))


def setup_telegram_webhook(public_url):
    """Faqat bir marta Bash konsolida: python bot.py --set-webhook https://..."""
    public_url=public_url.strip().rstrip('/')
    if not re.fullmatch(r'https://[A-Za-z0-9.-]+',public_url):
        raise ValueError('Faqat https://SIZNIKI.onrender.com kabi HTTPS manzil kiriting.')
    put('public_url',public_url)
    webhook_url=public_url+'/telegram/'+get('telegram_webhook_path')
    result=bot.set_webhook(url=webhook_url, secret_token=get('telegram_header_secret'),
                           allowed_updates=['message','callback_query','pre_checkout_query'], max_connections=8,
                           drop_pending_updates=False)
    print('Webhook o‘rnatildi:',result)
    if not result:
        raise RuntimeError('Telegram webhook o‘rnatilmadi')
    info=bot.get_webhook_info()
    print('Telegram URL mos:',info.url==webhook_url)
    print('Oxirgi Telegram xatosi:',info.last_error_message or 'yo‘q')
    print('Maxfiy webhook URL va secretni boshqalarga yubormang.')


initialize_runtime()  # Gunicorn: from bot import app; DB+shifrlash tayyor.

@app.before_request
def _ensure_bank_monitor():
    if PAYMENT_PROVIDER == 'bank_session' and not HUMO_LISTENER_STARTED.is_set():
        maybe_start_bank_listener()

if 'gunicorn' in os.path.basename(sys.argv[0]).lower():
    start_broadcast_worker()
    maybe_start_bank_listener()  # Gunicorn --workers 1, worker import only; not CLI set-webhook

application = app


def start_render():
    """Render Start Command: python bot.py --render-start

    Telegram webhookini har bir yangi deployda to'g'ri hostga ulaydi,
    so'ng yagona Gunicorn worker bilan HTTP serverni ishga tushiradi.
    """
    if not PUBLIC_URL:
        raise SystemExit('PUBLIC_URL yoki RENDER_EXTERNAL_HOSTNAME topilmadi. Render Environment ni tekshiring.')
    logger.info('Render start: %s; live_sales=%s', PUBLIC_URL, LIVE_SALES_ENABLED)
    for i in range(3):
        try:
            setup_telegram_webhook(PUBLIC_URL)
            break
        except Exception as exc:
            logger.warning('Webhook urinish %s/3: %s', i + 1, type(exc).__name__)
            if i == 2:
                raise
            time.sleep(2 ** i)
    port = os.getenv('PORT', '10000')
    if not port.isdecimal() or not (1 <= int(port) <= 65535):
        raise SystemExit('PORT noto‘g‘ri')
    os.execvp('gunicorn', [
        'gunicorn', '--bind', '0.0.0.0:' + port,
        '--workers', '1', '--threads', '8', '--timeout', '120',
        # Log HTTP status/latency without exposing the secret webhook path.
        '--access-logfile', '-', '--access-logformat', '%(h)s %(m)s %(s)s %(L)s',
        '--error-logfile', '-', 'bot:app'
    ])


if __name__=='__main__':
    if len(sys.argv)>=2 and sys.argv[1]=='--render-start':
        start_render()
    elif len(sys.argv)>=3 and sys.argv[1]=='--set-webhook':
        setup_telegram_webhook(sys.argv[2])
    elif len(sys.argv)>=2 and sys.argv[1]=='--bank-sender-id':
        if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and TELETHON_STRING_SESSION):
            raise SystemExit('TELEGRAM_API_ID, TELEGRAM_API_HASH, TELETHON_STRING_SESSION ENV kerak')
        name=os.getenv('HUMO_BOT_USERNAME','HUMOcardbot').strip().lstrip('@')
        async def show_sender():
            async with TelegramClient(StringSession(TELETHON_STRING_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH) as cl:
                entity=await cl.get_entity(name)
                print('HUMO_SENDER_ID='+str(entity.id))
        asyncio.run(show_sender())
    elif len(sys.argv)>=2 and sys.argv[1]=='--webhook-info':
        info=bot.get_webhook_info()
        print('Webhook mavjud:',bool(info.url))
        print('Oxirgi xato:',info.last_error_message or 'yo‘q')
        print('Navbatdagi update soni:',info.pending_update_count)
    elif len(sys.argv)>=2 and sys.argv[1]=='--check-api':
        # Maxfiy kalitlar hech qachon ko‘rsatilmaydi.
        for title,url in (('Telegram','https://api.telegram.org'),
                          ('Fragment','https://fragment-api.uz/api/v1/getInfo'),
                          ('HUMO bank-bot','https://t.me/HUMOcardbot')):
            try:
                r=requests.get(url,timeout=(4,8))
                print(title, 'HTTP',r.status_code, '- HTTP 403 bo‘lsa proxy/kalit cheklovini tekshiring' if r.status_code==403 else '- HTTP javobi olindi')
            except requests.RequestException as e:
                print(title,'ULANMADI:',type(e).__name__,str(e)[:180])
    else:
        print('Render Start Command: python bot.py --render-start')
        print('Qo‘lda webhook: python bot.py --set-webhook https://SIZNIKI.onrender.com')
        print('Render env: BOT_TOKEN, OWNER_ID, TELEGRAM_API_ID, TELEGRAM_API_HASH, TELETHON_STRING_SESSION, ENABLE_LIVE_SALES=1')
