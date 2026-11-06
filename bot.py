# -*- coding: utf-8 -*-
"""Premium Market V4: Render persistent SQLite, webhook, XTR + automatic delivery.

Real orders are disabled until the owner turns on live delivery.
Never host a money-handling SQLite bot on ephemeral/serverless filesystems.
"""
from __future__ import annotations

import html
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from decimal import Decimal, ROUND_CEILING, InvalidOperation
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv
load_dotenv()
import telebot
from flask import Flask, abort, jsonify, request
from telebot import types

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("premium-market")
TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
FRAGMENT_KEY = os.getenv("FRAGMENT_API_KEY", "").strip()
PUBLIC_URL = (os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
DB_PATH = os.getenv("DB_PATH", "market.sqlite3")
PERSISTENT_REQUIRED = os.getenv("PERSISTENT_DB_REQUIRED", "0") == "1"
PORT = int(os.getenv("PORT", "10000"))
FRAGMENT_BASE = "https://fragment-api.uz/api/v1"
if not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", TOKEN):
    raise RuntimeError("BOT_TOKEN env kerak: BotFather tokeningizni kiriting")
if OWNER_ID <= 0:
    raise RuntimeError("OWNER_ID env kerak: Telegram raqamli ID")
if PUBLIC_URL and not PUBLIC_URL.startswith("https://"):
    raise RuntimeError("PUBLIC_URL https:// bilan boshlanishi kerak")

bot = telebot.TeleBot(TOKEN, parse_mode="HTML", threaded=False)
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1024*1024
BOT_USERNAME = ""
PATH_KEY = hashlib.sha256(TOKEN.encode()).hexdigest()[:48]
DB_LOCK = threading.RLock()
STOP = threading.Event()

DEFAULTS = {
    "bot_active": "1", "live_delivery": "0", "usd_uzs": "0",
    "xtr_uzs": "0", "gift_cost_per_star_uzs": "378",
    "global_markup_pct": "20", "provider_fee_pct": "1.5",
    "referral_pct": "5", "card_number": "", "card_owner": "",
    "payment_wait_min": "15", "support_username": "", "min_profit_pct": "0",
    "bundles_seeded": "0", "prices_reviewed": "0",
}


@contextmanager
def db():
    con = sqlite3.connect(DB_PATH, timeout=20, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=20000")
    try:
        yield con
    finally:
        con.close()


def init_db():
    if PERSISTENT_REQUIRED and not os.path.realpath(DB_PATH).startswith('/var/data/'):
        raise RuntimeError("Render: DB_PATH must be on /var/data persistent disk")
    if PERSISTENT_REQUIRED and not os.path.isdir('/var/data'):
        raise RuntimeError("Render: /var/data disk not mounted; do not start with ephemeral DB")
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with DB_LOCK, db() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS users (
            uid INTEGER PRIMARY KEY, username TEXT NOT NULL DEFAULT '',
            first_name TEXT NOT NULL DEFAULT '', referrer INTEGER,
            bonus_xtr INTEGER NOT NULL DEFAULT 0 CHECK(bonus_xtr>=0),
            created INTEGER NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS users_username ON users(username) WHERE username<>'';
        CREATE TABLE IF NOT EXISTS sessions (uid INTEGER PRIMARY KEY, step TEXT, data TEXT, updated INTEGER);
        CREATE TABLE IF NOT EXISTS bundles (
            kind TEXT NOT NULL CHECK(kind IN ('stars','premium')),
            units INTEGER NOT NULL CHECK(units>0),
            active INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(kind,units));
        CREATE TABLE IF NOT EXISTS admin_funding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL, stars INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'invoice',
            charge_id TEXT UNIQUE, created INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS rules (
            product TEXT PRIMARY KEY, mode TEXT NOT NULL DEFAULT 'auto',
            manual_uzs INTEGER NOT NULL DEFAULT 0, markup_pct TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS gifts (
            gift_id TEXT PRIMARY KEY, title TEXT NOT NULL,
            emoji TEXT NOT NULL DEFAULT '🎁', file_id TEXT NOT NULL DEFAULT '',
            stars INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1,
            available INTEGER NOT NULL DEFAULT 1, updated INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL,
            kind TEXT NOT NULL, units INTEGER NOT NULL, gift_id TEXT NOT NULL DEFAULT '',
            recipient_uid INTEGER, recipient_username TEXT NOT NULL DEFAULT '',
            cost_uzs INTEGER NOT NULL, price_uzs INTEGER NOT NULL,
            original_xtr INTEGER NOT NULL, discount_xtr INTEGER NOT NULL DEFAULT 0,
            payable_xtr INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'invoice',
            charge_id TEXT UNIQUE, api_result TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            created INTEGER NOT NULL, paid_at INTEGER, finished_at INTEGER,
            referral_awarded INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS orders_user ON orders(uid, id DESC);
        CREATE INDEX IF NOT EXISTS orders_status ON orders(status, created);
        """)
        # Existing databases are migrated in place; orders and users are preserved.
        ucols = {r[1] for r in c.execute("PRAGMA table_info(users)")}
        if "bonus_uzs" not in ucols:
            c.execute("ALTER TABLE users ADD COLUMN bonus_uzs INTEGER NOT NULL DEFAULT 0")
        ocols = {r[1] for r in c.execute("PRAGMA table_info(orders)")}
        for name in ("discount_uzs", "payable_uzs"):
            if name not in ocols:
                c.execute(f"ALTER TABLE orders ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0")
        for k, v in DEFAULTS.items():
            c.execute("INSERT OR IGNORE INTO settings (k,v) VALUES (?,?)", (k, v))
        c.execute("INSERT OR IGNORE INTO rules (product) VALUES ('stars')")
        # Starter price tags are editable DEMO values, not guaranteed market prices.
        starter_stars = [(50,18888),(100,37776),(250,94440),(500,188880),
                         (1000,377760),(2500,944400)]
        starter_premium = [(3,195000),(6,270000),(12,480000)]
        if c.execute("SELECT v FROM settings WHERE k='bundles_seeded'").fetchone()["v"] == "0":
            for units, uzs in starter_stars:
                c.execute("INSERT OR IGNORE INTO bundles(kind,units) VALUES('stars',?)", (units,))
                c.execute("INSERT OR IGNORE INTO rules(product,mode,manual_uzs) VALUES(?, 'manual', ?)",
                          (f"stars:{units}",uzs))
            for units, uzs in starter_premium:
                c.execute("INSERT OR IGNORE INTO bundles(kind,units) VALUES('premium',?)", (units,))
                c.execute("INSERT OR IGNORE INTO rules(product,mode,manual_uzs) VALUES(?, 'manual', ?)",
                          (f"premium{units}",uzs))
                # V1 seeded premium3/6/12 in auto mode. If no API key
                # and price was never customized, migrate to demo UZS price.
                if not FRAGMENT_KEY:
                    c.execute("UPDATE rules SET mode='manual',manual_uzs=? WHERE product=? "
                              "AND mode='auto' AND manual_uzs=0 AND markup_pct=''",
                              (uzs, f"premium{units}"))
            # Custom Stars requires a per-star price; admin can change it.
            c.execute("UPDATE rules SET mode='manual',manual_uzs=378 WHERE product='stars' AND mode='auto'")
            # V1 had 0 here and therefore Gift catalog could not show UZS.
            # 378 is a DISPLAY DEMO COST; admin must replace it before sales.
            c.execute("UPDATE settings SET v='378' WHERE k='gift_cost_per_star_uzs' AND v='0'")
            c.execute("UPDATE settings SET v='1' WHERE k='bundles_seeded'")
        c.execute("INSERT OR IGNORE INTO settings(k,v) VALUES('referral_pct','5')")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS payment_receipts (
            charge_id TEXT PRIMARY KEY, order_id INTEGER,
            uid INTEGER NOT NULL, amount INTEGER NOT NULL,
            status TEXT NOT NULL, created INTEGER NOT NULL,
            updated INTEGER NOT NULL, error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS payment_receipts_status ON payment_receipts(status);
        CREATE TABLE IF NOT EXISTS admins (
            uid INTEGER PRIMARY KEY, added_by INTEGER NOT NULL, created INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS required_channels (
            chat_id INTEGER PRIMARY KEY, title TEXT NOT NULL, url TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1, added INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sender_uid INTEGER NOT NULL,
            source_chat_id INTEGER NOT NULL, source_message_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft', last_uid INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0,
            created INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS humo_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL,
            asked_uzs INTEGER NOT NULL, pay_uzs INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'waiting',
            created INTEGER NOT NULL, expires INTEGER NOT NULL,
            event_chat_id INTEGER, event_message_id INTEGER
        );
        CREATE UNIQUE INDEX IF NOT EXISTS humo_pending_amount
        ON humo_requests(pay_uzs) WHERE status='waiting';
        CREATE INDEX IF NOT EXISTS humo_req_user ON humo_requests(uid,id DESC);
        CREATE TABLE IF NOT EXISTS humo_events (
            chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
            amount_uzs INTEGER, card_last4 TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL, raw_hash TEXT NOT NULL, received_ts INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'audit_only',
            PRIMARY KEY(chat_id,message_id)
        );
        """)



def q1(sql, pars=()):
    with db() as c:
        return c.execute(sql, pars).fetchone()


def qall(sql, pars=()):
    with db() as c:
        return c.execute(sql, pars).fetchall()


def change(sql, pars=()):
    with DB_LOCK, db() as c:
        c.execute(sql, pars)


def setting(key):
    r = q1("SELECT v FROM settings WHERE k=?", (key,))
    return r["v"] if r else DEFAULTS.get(key, "")


def set_setting(key, value):
    if key not in DEFAULTS:
        raise ValueError("Noaniq sozlama")
    change("INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, str(value)))


def d(value):
    return Decimal(str(value))


def whole(value):
    return int(d(value).to_integral_value(rounding=ROUND_CEILING))


def money(v):
    return f"{int(v):,}".replace(",", " ") + " so‘m"


def esc(v):
    return html.escape(str(v), quote=True)


def is_admin(uid):
    if uid == OWNER_ID:
        return True
    return bool(q1("SELECT 1 FROM admins WHERE uid=?", (uid,)))


def format_int(v):
    return f"{int(v):,}".replace(",", " ")


def user_row(uid):
    return q1("SELECT * FROM users WHERE uid=?", (uid,))


def upsert_user(tguser, referral=None):
    uid = tguser.id
    username = (tguser.username or "").lower()
    name = tguser.first_name or "Foydalanuvchi"
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        existing = c.execute("SELECT uid FROM users WHERE uid=?", (uid,)).fetchone()
        if not existing:
            ref_id = int(referral) if referral and str(referral).isdigit() else None
            if ref_id == uid or not c.execute("SELECT uid FROM users WHERE uid=?", (ref_id,)).fetchone():
                ref_id = None
            # Username may have changed owner; clear stale values before unique update.
            if username:
                c.execute("UPDATE users SET username='' WHERE username=? AND uid<>?", (username, uid))
            c.execute("INSERT INTO users(uid,username,first_name,referrer,created) VALUES(?,?,?,?,?)",
                      (uid, username, name, ref_id, int(time.time())))
        else:
            if username:
                c.execute("UPDATE users SET username='' WHERE username=? AND uid<>?", (username, uid))
            c.execute("UPDATE users SET username=?,first_name=? WHERE uid=?", (username, name, uid))
        c.execute("COMMIT")


def session_get(uid):
    r = q1("SELECT step,data FROM sessions WHERE uid=?", (uid,))
    if not r:
        return None, {}
    return r["step"], json.loads(r["data"] or "{}")


def session_set(uid, step, data=None):
    change("INSERT INTO sessions(uid,step,data,updated) VALUES(?,?,?,?) "
           "ON CONFLICT(uid) DO UPDATE SET step=excluded.step,data=excluded.data,updated=excluded.updated",
           (uid, step, json.dumps(data or {}, ensure_ascii=False), int(time.time())))


def session_clear(uid):
    change("DELETE FROM sessions WHERE uid=?", (uid,))


def rows(*lines):
    kb = types.InlineKeyboardMarkup(row_width=2)
    for line in lines:
        kb.row(*(types.InlineKeyboardButton(label, callback_data=cb) for label, cb in line))
    return kb


def copy_btn(label, value):
    if hasattr(types, "CopyTextButton"):
        return types.InlineKeyboardButton(label, copy_text=types.CopyTextButton(text=str(value)))
    return types.InlineKeyboardButton(label + ": " + str(value), callback_data="copy_unsupported")


def send(uid, msg, keyboard=None):
    return bot.send_message(uid, msg, reply_markup=keyboard, disable_web_page_preview=True)


def notice_owner(msg):
    try:
        send(OWNER_ID, "🔔 " + msg)
    except Exception:
        log.exception("Owner notification failed")


def main_menu(uid):
    lines = [
        [("⭐ Stars", "catalog_stars"), ("💎 Premium", "catalog_premium")],
        [("🎁 Gifts", "catalog_gifts_0"), ("💰 Hisobim", "profile")],
        [("💳 To‘lov haqida", "topup")],
        [("📦 Buyurtmalarim", "my_orders")],
        [("👥 Referal tizimi", "referral")],
        [("🎧 Yordam", "support")],
    ]
    if is_admin(uid):
        lines.append([("⚙️ Admin panel", "admin")])
    return rows(*lines)


def required_membership(uid):
    """Return missing channels. Fail closed on lookup errors."""
    if is_admin(uid):
        return []
    missing = []
    for ch in qall("SELECT chat_id,title,url FROM required_channels WHERE active=1 ORDER BY added"):
        try:
            member = bot.get_chat_member(int(ch["chat_id"]), uid)
            status = getattr(member, "status", "left")
            ok = status in ("creator", "administrator", "member") or (
                status == "restricted" and getattr(member, "is_member", False))
        except Exception:
            ok = False
        if not ok:
            missing.append(ch)
    return missing


def show_subscriptions(uid, missing=None):
    if missing is None:
        missing = required_membership(uid)
    if not missing:
        return True
    k = types.InlineKeyboardMarkup()
    for ch in missing:
        k.add(types.InlineKeyboardButton("📢 " + ch["title"][:45], url=ch["url"]))
    k.add(types.InlineKeyboardButton("✅ Obunani tekshirish", callback_data="check_subs"))
    send(uid, "🔒 <b>Majburiy obuna</b>\nQuyidagi kanal(lar)ga qo‘shilib, tasdiqlang.", k)
    return False


def welcome(uid):
    if not show_subscriptions(uid):
        return
    send(uid, "💎 <b>PREMIUM MARKET</b>\n\n⭐ Stars • 💎 Premium • 🎁 Gifts\n\nKerakli bo‘limni tanlang.", main_menu(uid))


def need_active(uid):
    if setting("bot_active") == "1" or is_admin(uid):
        return True
    send(uid, "⏸ Bot hozir texnik rejimda.")
    return False


def valid_username(val):
    val = val.strip().lstrip("@").lower()
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]{4,31}", val):
        raise ValueError("Username noto‘g‘ri. Masalan: @telegramuser")
    return val


class ProviderError(Exception):
    pass


def api_call(endpoint, body=None, timeout=25):
    if not FRAGMENT_KEY:
        raise ProviderError("FRAGMENT_API_KEY kiritilmagan")
    try:
        resp = requests.post(FRAGMENT_BASE + endpoint, json=body or {},
                             headers={"X-API-Key": FRAGMENT_KEY, "Accept": "application/json"},
                             timeout=timeout)
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        raise ProviderError("API tarmoq xatosi yoki natija noaniq; qayta xarid qilmang: " + str(e)[:120]) from e
    if resp.status_code != 200 or not data.get("ok"):
        raise ProviderError(f"HTTP {resp.status_code} | {str(data.get('code',''))} | {str(data.get('message',''))[:150]}")
    return data.get("result") or {}


def provider_cost(kind, units):
    if kind == "stars":
        result = api_call("/stars/pricing", {"amount": units})
        return d(result["price"]["usd"])
    if kind == "premium":
        result = api_call("/premium/pricing")
        for obj in result.get("packages", []):
            if int(obj["months"]) == units:
                return d(obj["usd"])
        raise ProviderError("API bu Premium muddatiga narx qaytarmadi")
    raise ProviderError("Mahsulot turi noto‘g‘ri")


def rule(product):
    r = q1("SELECT * FROM rules WHERE product=?", (product,))
    return r or {"mode": "auto", "manual_uzs": 0, "markup_pct": "", "enabled": 1}


def get_price(kind, units, gift_id=""):
    """Return (known procurement cost in UZS or 0, sale UZS).

    Manual prices must work without a Fragment key, USD conversion or
    configured gift-cost valuation. This is a price QUOTE, not a delivery.
    """
    product = (f"stars:{units}" if kind == "stars" and
               q1("SELECT 1 FROM rules WHERE product=?", (f"stars:{units}",))
               else "stars") if kind == "stars" else (
               "premium" + str(units) if kind == "premium" else "gift:" + gift_id)
    r = rule(product)
    if not r["enabled"]:
        raise ValueError("Mahsulot sotuvda emas")
    if kind == "gift":
        gift = q1("SELECT * FROM gifts WHERE gift_id=? AND active=1 AND available=1", (gift_id,))
        if not gift:
            raise ValueError("Gift hozir mavjud emas")
    cost = 0
    if kind == "gift":
        per_star = d(setting("gift_cost_per_star_uzs"))
        if per_star > 0:
            cost = whole(d(gift["stars"]) * per_star)
    elif r["mode"] != "manual":
        if not FRAGMENT_KEY or d(setting("usd_uzs")) <= 0:
            raise ValueError("Avtomatik narx uchun API kalit va USD/UZS kerak. Admin qo‘lda narx qo‘ysin.")
        cost_usd = provider_cost(kind, units)
        cost = whole(cost_usd * d(setting("usd_uzs")) *
                     (1 + d(setting("provider_fee_pct")) / 100))
    if r["mode"] == "manual":
        sale = int(r["manual_uzs"])
        if product == "stars":  # only global fallback is a per-star price
            sale *= int(units)
        if sale <= 0:
            raise ValueError("Admin so‘mdagi sotuv narxini hali belgilamagan")
    else:
        if cost <= 0:
            raise ValueError("API tannarxi yoki Gift Stars tannarxi yo‘q. Admin qo‘lda narx belgilasin.")
        markup = d(r["markup_pct"] or setting("global_markup_pct"))
        sale = whole(d(cost) * (1 + markup / 100))
    minprofit = d(setting("min_profit_pct"))
    if cost and sale < whole(d(cost) * (1 + minprofit/100)):
        raise ValueError("Narx minimal foyda chegarasidan past. Admin sozlasin.")
    return cost, sale


def bundle_list(uid, kind, admin=False):
    bundles = qall("SELECT * FROM bundles WHERE kind=? " +
                   ("" if admin else "AND active=1 ") + "ORDER BY units", (kind,))
    menu = []
    for b in bundles:
        units = b["units"]
        try:
            _, uzs = get_price(kind, units)
            price = money(uzs)
        except Exception:
            price = "narx belgilanmagan"
        label = ((f"⭐ {units} Stars" if kind == "stars" else f"💎 {units} oy") +
                 f" — {price}" + (" 🚫" if not b["active"] else ""))
        menu.append([(label, f"{'admin_bundle' if admin else 'buy_bundle'}_{kind}_{units}")])
    if admin:
        menu.append([("➕ Stars to‘plami qo‘shish" if kind == "stars" else "➕ Premium to‘plami qo‘shish",
                      f"add_bundle_{kind}")])
        if kind == "stars":
            menu.append([("⚙️ Boshqa miqdor (1 Stars narxi)", "edit_product_stars")])
        menu.append([("🔙 Admin", "admin")])
    else:
        if kind == "stars":
            menu.append([("✏️ Boshqa miqdor", "stars_custom")])
        menu.append([("🏠 Bosh menyu", "home")])
    title = "⭐ <b>Stars to‘plamlari</b>" if kind == "stars" else "💎 <b>Premium to‘plamlari</b>"
    send(uid, title + "\n\nNarxlar so‘mda ko‘rsatiladi. To‘plamni tanlang:", rows(*menu))


def admin_bundle(uid, kind, units):
    b = q1("SELECT active FROM bundles WHERE kind=? AND units=?", (kind, units))
    if not b:
        raise ValueError("To‘plam topilmadi")
    product = f"stars:{units}" if kind == "stars" else f"premium{units}"
    r = rule(product)
    try:
        _, price = get_price(kind, units)
        price_label = money(price)
    except Exception as e:
        price_label = str(e)[:90]
    send(uid, f"📦 <b>{'Stars' if kind=='stars' else 'Premium'} to‘plami</b>\n"
              f"Miqdor: {units} {'Stars' if kind=='stars' else 'oy'}\n"
              f"💰 Narx: {esc(price_label)}\n🧮 Rejim: {esc(r['mode'])}\n"
              f"📈 Ustama: {esc(r['markup_pct'] or setting('global_markup_pct'))}%\n"
              f"Holat: {'✅ Faol' if b['active'] else '🚫 O‘chiq'}",
         rows([("✏️ So‘mdagi narx", "edit_manual_" + product)],
              [("📈 Foiz qo‘shish", "edit_pct_" + product)],
              [("🔄 API + foiz", "mode_auto_" + product)],
              [("🔴/🟢 Sotuv", f"bundle_toggle_{kind}_{units}")],
              [("🗑 To‘plamni o‘chirish", f"bundle_delete_ask_{kind}_{units}")],
              [("🔙 To‘plamlar", f"admin_bundles_{kind}")]))


def xtr_from_uzs(price):
    rate = d(setting("xtr_uzs"))
    if rate <= 0:
        raise ValueError("Admin XTR hisoblash kursini belgilamagan")
    return max(1, whole(d(price) / rate))


def gift_sync():
    res = bot.get_available_gifts()
    gifts = getattr(res, "gifts", None) or []
    if not gifts:
        return 0
    now = int(time.time())
    ids = []
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        for obj in gifts:
            gid = str(obj.id)
            ids.append(gid)
            sticker = getattr(obj, "sticker", None)
            emoji = getattr(sticker, "emoji", None) or "🎁"
            fid = getattr(sticker, "file_id", "") or ""
            price = int(obj.star_count)
            uz_names = {"🌹": "Atirgul", "🧸": "Ayiqcha", "💝": "Sevgi yuragi", "❤️": "Yurakcha",
                        "🎂": "Tort", "💍": "Uzuk", "💐": "Guldasta", "🍫": "Shokolad",
                        "🌷": "Lola", "🎁": "Sovg‘a", "🏆": "Kubok", "💎": "Olmos",
                        "🍀": "Omad belgisi", "🎀": "Lenta", "🐻": "Ayiq"}
            uz_name = uz_names.get(emoji, f"Sovg‘a {gid[-5:]}")
            c.execute("""INSERT INTO gifts(gift_id,title,emoji,file_id,stars,updated,available)
                         VALUES(?,?,?,?,?,?,1)
                         ON CONFLICT(gift_id) DO UPDATE SET
                         emoji=excluded.emoji,file_id=excluded.file_id,
                         stars=excluded.stars,updated=excluded.updated,available=1""",
                      (gid, uz_name, emoji, fid, price, now))
        c.execute("UPDATE gifts SET available=0 WHERE updated<>?", (now,))
        c.execute("COMMIT")
    return len(ids)


def gift_catalog(uid, page=0, admin=False):
    limit = 8
    gifts = qall("SELECT * FROM gifts WHERE available=1 " + ("" if admin else "AND active=1 ") +
                 "ORDER BY stars,gift_id LIMIT ? OFFSET ?", (limit + 1, page * limit))
    menu = []
    for g in gifts[:limit]:
        if admin:
            label = f"{g['emoji']} {g['title'][:15]} — ⭐{g['stars']}"
        else:
            try:
                _, amount_uzs = get_price("gift", 1, g["gift_id"])
                price_display = money(amount_uzs)
            except Exception:
                price_display = "narx kiritilmagan"
            label = f"{g['emoji']} {g['title'][:17]} — {price_display}"
        menu.append([(label, f"{'agift' if admin else 'gift'}_{g['gift_id']}")])
    nav = []
    if page > 0:
        nav.append(("⬅️", f"catalog_{'agifts' if admin else 'gifts'}_{page-1}"))
    if len(gifts) > limit:
        nav.append(("➡️", f"catalog_{'agifts' if admin else 'gifts'}_{page+1}"))
    if nav:
        menu.append(nav)
    menu.append([("🔙 Orqaga", "admin_gifts" if admin else "home")])
    send(uid, "🎁 <b>Gift katalogi</b>\n\nStikerini ko‘rish uchun sovg‘ani tanlang.\n" +
         ("\nHali Gift yuklanmagan. Admin «Sinxronlash»ni bosing." if not gifts else ""), rows(*menu))


def gift_card(uid, gid, admin=False):
    g = q1("SELECT * FROM gifts WHERE gift_id=?", (gid,))
    if not g:
        return send(uid, "Gift topilmadi.")
    if g["file_id"]:
        try:
            bot.send_sticker(uid, g["file_id"])
        except Exception:
            log.warning("Cannot send gift sticker: %s", gid)
    try:
        cost, sale = get_price("gift", 1, gid)
        price = money(sale)
    except Exception as e:
        price = "Sozlanmagan (" + esc(e) + ")"
    product = "gift:" + gid
    r = rule(product)
    msg = (f"{esc(g['emoji'])} <b>{esc(g['title'])}</b>\n"
           + (f"⭐ Telegramdagi tannarxi: {g['stars']} Stars\n" if admin else "") +
           f"💰 Sotuv narxi: <b>{price}</b>\n"
           f"📦 Holati: {'✅ Sotuvda' if g['active'] else '🚫 Yashirilgan'}")
    if admin:
        msg += f"\n🧮 Rejim: {r['mode']}"
        kb = rows([("✏️ O‘zbekcha nom", "edit_title_" + gid)],
                  [("💰 Qo‘lda narx", "edit_manual_" + product)],
                  [("📈 Foiz", "edit_pct_" + product)],
                  [("🔄 Avtomatik narx", "mode_auto_" + product)],
                  [("🚫 Yashirish / Ko‘rsatish", "toggle_" + product)],
                  [("🔙 Katalog", "catalog_agifts_0")])
    else:
        kb = rows([("🛒 Sotib olish", "buy_gift_" + gid)],
                  [("🔙 Giftlar", "catalog_gifts_0")])
    send(uid, msg, kb)


def product_name(kind, units, gid=""):
    if kind == "stars":
        return f"⭐ {units} Stars"
    if kind == "premium":
        return f"💎 Premium {units} oy"
    g = q1("SELECT title,emoji FROM gifts WHERE gift_id=?", (gid,))
    return f"{g['emoji']} {g['title']}" if g else f"🎁 Gift {gid}"


def ask_recipient(uid, kind, units, gift_id=""):
    if not need_active(uid):
        return
    try:
        cost, sale = get_price(kind, units, gift_id)
        xtr = xtr_from_uzs(sale) if d(setting("xtr_uzs")) > 0 else 0
    except (ProviderError, ValueError, InvalidOperation, KeyError) as e:
        return send(uid, "⚠️ Narxni hisoblab bo‘lmadi: " + esc(e))
    session_set(uid, "recipient_choice", {"kind": kind, "units": units, "gift_id": gift_id})
    send(uid, f"🛒 <b>{esc(product_name(kind,units,gift_id))}</b>\n"
              f"💰 Sotuv narxi: <b>{money(sale)}</b>\n\n🎯 <b>Kimga yuborilsin?</b>",
         rows([("👤 O‘zimga", "who_self"), ("👥 Do‘stimga", "who_friend")],
              [("🔙 Bosh menyu", "home")]))


def confirm_recipient(uid, username="", rec_uid=None):
    step, sess = session_get(uid)
    if step not in ("recipient_choice", "recipient_input"):
        return send(uid, "Buyurtmani boshidan tanlang.", main_menu(uid))
    kind, units, gid = sess["kind"], int(sess["units"]), sess.get("gift_id", "")
    if kind == "gift" and not rec_uid:
        return send(uid, "❌ Gift uchun do‘stingiz avval botga /start yuborishi kerak.\n"
                   "Shundan keyin uning @username'ini kiriting.")
    try:
        cost, sale = get_price(kind, units, gid)
        xtr = xtr_from_uzs(sale) if d(setting("xtr_uzs")) > 0 else 0
    except (ProviderError, ValueError, KeyError, InvalidOperation) as e:
        return send(uid, "Narx xatosi: " + esc(e))
    sess["username"] = username
    sess["recipient_uid"] = rec_uid
    sess["quoted_price_uzs"] = sale
    sess["quoted_xtr"] = xtr
    session_set(uid, "checkout", sess)
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("✅ Telegram Stars bilan to‘lash", callback_data="checkout_pay"))
    kb.add(copy_btn("📋 Hisoblangan so‘mni nusxalash", str(sale)))
    kb.add(types.InlineKeyboardButton("👥 Qabul qiluvchini o‘zgartirish", callback_data="checkout_change"))
    kb.add(types.InlineKeyboardButton("🏠 Bosh menyu", callback_data="home"))
    available = (user_row(uid)["bonus_uzs"] if user_row(uid) else 0)
    discount = min(available, max(0, sale - 1))
    send(uid, f"✅ <b>Buyurtmani tekshiring</b>\n\n"
              f"📦 {esc(product_name(kind, units, gid))}\n"
              f"👤 Qabul qiluvchi: {('@' + esc(username)) if username else ('ID: ' + str(rec_uid))}\n"
              f"💰 Narx: <b>{money(sale)}</b>\n"
              f"🎁 Referal bonus: -{money(discount)}\n"
              f"✅ Qolgan hisob: <b>{money(sale-discount)}</b>\n\n"
              "📌 Telegram bot ichidagi raqamli mahsulotlar to‘lovi XTR invoice orqali olinadi. "
              "To‘lov tugmasi ochilganda aniq XTR miqdori ko‘rsatiladi.", kb)


def create_order_from_session(uid):
    step, s = session_get(uid)
    if step != "checkout":
        raise ValueError("Buyurtma eskirgan. Qaytadan tanlang.")
    if setting("live_delivery") != "1":
        raise ValueError("Savdo hali ishga tushirilmagan. Admin sozlamalarni yakunlasin.")
    kind, units, gid = s["kind"], int(s["units"]), s.get("gift_id", "")
    username, rec_uid = s.get("username", ""), s.get("recipient_uid")
    if kind in ("stars", "premium"):
        if not FRAGMENT_KEY or not username:
            raise ValueError("Fragment API kaliti yoki qabul qiluvchi username yo‘q")
        valid_username(username)
    if kind == "gift":
        if not rec_uid:
            raise ValueError("Gift uchun qabul qiluvchining Telegram ID raqami kerak")
        gift = q1("SELECT stars FROM gifts WHERE gift_id=? AND available=1", (gid,))
        if not gift:
            raise ValueError("Gift tugagan")
        bal = bot.get_my_star_balance()
        if int(getattr(bal, "amount", 0)) < int(gift["stars"]):
            raise ValueError("Bot Stars balansi Gift yuborish uchun yetarli emas")
    cost, sale = get_price(kind, units, gid)
    original = xtr_from_uzs(sale)
    if s.get("quoted_price_uzs") != sale or s.get("quoted_xtr") != original:
        raise ValueError("Narx yangilangan. Qabul qiluvchini tanlab, yangi narxni tasdiqlang.")
    now = int(time.time())
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        u = c.execute("SELECT bonus_uzs FROM users WHERE uid=?", (uid,)).fetchone()
        bonus_uzs = min(int(u["bonus_uzs"] or 0), max(0, sale - 1))
        payable_uzs = sale - bonus_uzs
        charge_xtr = xtr_from_uzs(payable_uzs)
        if bonus_uzs:
            c.execute("UPDATE users SET bonus_uzs=bonus_uzs-? WHERE uid=?", (bonus_uzs, uid))
        cur = c.execute("""INSERT INTO orders(uid,kind,units,gift_id,recipient_uid,recipient_username,
                 cost_uzs,price_uzs,original_xtr,discount_xtr,payable_xtr,status,created,discount_uzs,payable_uzs)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,'invoice',?,?,?)""",
                 (uid, kind, units, gid, rec_uid, username, cost, sale, original, 0, charge_xtr, now,
                  bonus_uzs, payable_uzs))
        oid = cur.lastrowid
        c.execute("COMMIT")
    session_clear(uid)
    return oid


def release_order(oid, newstatus="expired"):
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        r = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        if not r or r["status"] != "invoice":
            c.execute("ROLLBACK")
            return False
        c.execute("UPDATE orders SET status=? WHERE id=? AND status='invoice'", (newstatus, oid))
        if r["discount_uzs"]:
            c.execute("UPDATE users SET bonus_uzs=bonus_uzs+? WHERE uid=?", (r["discount_uzs"], r["uid"]))
        if r["discount_xtr"]:  # legacy reserved XTR credit from v1
            c.execute("UPDATE users SET bonus_xtr=bonus_xtr+? WHERE uid=?", (r["discount_xtr"], r["uid"]))
        c.execute("COMMIT")
        return True


def create_funding_invoice(uid, amount):
    """Owner funds the bot's real Telegram Stars balance via a paid XTR invoice."""
    if uid != OWNER_ID or not 1 <= amount <= 100000:
        raise ValueError("Faqat admin 1–100000 Stars to‘ldira oladi")
    now = int(time.time())
    with DB_LOCK, db() as c:
        cur = c.execute("INSERT INTO admin_funding(owner_id,stars,status,created) VALUES(?,?,'invoice',?)",
                        (uid,amount,now))
        fid = cur.lastrowid
    try:
        bot.send_invoice(chat_id=uid,title="⭐ Bot Stars balansini to‘ldirish",
                         description=f"Bot balansiga {amount} Telegram Stars qo‘shish",
                         invoice_payload=f"fund_{fid}",provider_token="",currency="XTR",
                         prices=[types.LabeledPrice("Bot Stars balansini to‘ldirish",amount)])
        send(uid, f"✅ ⭐{amount} uchun Telegram invoice yuborildi. To‘lov tasdiqlangach bot balansini tekshiring.")
    except Exception:
        change("UPDATE admin_funding SET status='cancelled' WHERE id=?", (fid,))
        raise


def mark_funding_paid(fid, uid, charge, stars):
    if uid != OWNER_ID:
        return False
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        f = c.execute("SELECT * FROM admin_funding WHERE id=?", (fid,)).fetchone()
        if not f or f["owner_id"] != uid or f["stars"] != stars or f["status"] != "invoice":
            c.execute("ROLLBACK")
            return False
        c.execute("UPDATE admin_funding SET status='paid',charge_id=? WHERE id=?", (charge,fid))
        c.execute("COMMIT")
    return True


def create_invoice(uid, oid):
    r = q1("SELECT * FROM orders WHERE id=? AND uid=?", (oid, uid))
    if not r or r["status"] != "invoice":
        return send(uid, "Buyurtma topilmadi.")
    # Validate the recipient before the user can pay. Gift IDs must be known to us.
    if setting("live_delivery") == "1" and r["kind"] in ("stars", "premium"):
        try:
            info = api_call("/getInfo", {"username": valid_username(r["recipient_username"])}, timeout=12)
            if not info or not info.get("username"):
                raise ProviderError("Qabul qiluvchi tasdiqlanmadi")
        except Exception:
            release_order(oid, "cancelled")
            raise
    try:
        bot.send_invoice(
            chat_id=uid,
            title=product_name(r["kind"], r["units"], r["gift_id"])[:32],
            description=f"Buyurtma #{oid}; qabul qiluvchi: @{r['recipient_username']}" if r["recipient_username"] else f"Buyurtma #{oid}",
            invoice_payload=f"pm_{oid}", provider_token="", currency="XTR",
            prices=[types.LabeledPrice("Telegram raqamli mahsuloti", r["payable_xtr"])],
            start_parameter=f"order{oid}",
        )
        send(uid, f"🧾 Buyurtma #{oid}\n💰 So‘mda: {money(r['payable_uzs'])}\n"
                  f"⭐ Telegram orqali to‘lov: {r['payable_xtr']} XTR\n"
                  f"⏳ To‘lovni {setting('payment_wait_min')} daqiqa ichida yakunlang.")
    except Exception:
        release_order(oid, "cancelled")
        raise


def mark_paid(oid, uid, charge, amount):
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        r = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        if not r or r["uid"] != uid or r["payable_xtr"] != amount or r["status"] != "invoice":
            c.execute("ROLLBACK")
            return False
        c.execute("UPDATE orders SET status='paid',charge_id=?,paid_at=? WHERE id=?",
                  (charge, int(time.time()), oid))
        c.execute("COMMIT")
        return True


def complete_order(oid):
    # Claim the job before contacting supplier. Never send the same paid order twice.
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        r = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        if not r or r["status"] != "paid":
            c.execute("ROLLBACK")
            return
        c.execute("UPDATE orders SET status='processing' WHERE id=?", (oid,))
        c.execute("COMMIT")
    try:
        if r["kind"] == "stars":
            result = api_call("/stars/buy", {"username": r["recipient_username"], "amount": r["units"]}, timeout=70)
        elif r["kind"] == "premium":
            result = api_call("/premium/buy", {"username": r["recipient_username"], "duration": r["units"]}, timeout=70)
        else:
            # Paid gifts can only be sent to Telegram users identified by ID.
            ok = bot.send_gift(user_id=int(r["recipient_uid"]), gift_id=r["gift_id"], pay_for_upgrade=False)
            if not ok:
                raise ProviderError("sendGift muvaffaqiyatsiz")
            result = {"bot_api": "sendGift", "ok": True}
        with DB_LOCK, db() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("UPDATE orders SET status='done',api_result=?,finished_at=? WHERE id=? AND status='processing'",
                      (json.dumps(result, ensure_ascii=False)[:1000], int(time.time()), oid))
            # Give referral bonus once and only once on successful fulfillment.
            order = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
            user = c.execute("SELECT referrer FROM users WHERE uid=?", (r["uid"],)).fetchone()
            if order["status"] == "done" and not order["referral_awarded"] and user and user["referrer"]:
                bonus = int(d(r["payable_uzs"] or r["price_uzs"]) * d(setting("referral_pct")) / 100)
                if bonus > 0:
                    c.execute("UPDATE users SET bonus_uzs=bonus_uzs+? WHERE uid=?", (bonus, user["referrer"]))
                c.execute("UPDATE orders SET referral_awarded=1 WHERE id=?", (oid,))
            c.execute("COMMIT")
        try:
            user_info = user_row(r["uid"])
            if user_info and user_info["referrer"]:
                b = int(d(r["payable_uzs"] or r["price_uzs"]) * d(setting("referral_pct")) / 100)
                if b > 0:
                    send(user_info["referrer"], f"🎉 Do‘stingiz buyurtma #{oid} ni yakunladi!\n"
                         f"📈 Sizga {setting('referral_pct')}% — {money(b)} referal bonus yozildi.\n"
                         "💰 Bonus hisobingizga tushdi va keyingi xaridga ishlatiladi.")
        except Exception:
            log.warning("Referrer notification failed for order %s",oid)
        try:
            send(r["uid"], f"✅ <b>Buyurtma #{oid} bajarildi!</b>\n{esc(product_name(r['kind'],r['units'],r['gift_id']))}", main_menu(r["uid"]))
        except Exception:
            log.warning("Customer notification failed for order %s",oid)
        notice_owner(f"✅ Buyurtma #{oid} yakunlandi.")
    except Exception as e:
        # Important: timeout may mean provider has already sent product.
        # Do not retry automatically or issue a blind refund.
        log.exception("Ambiguous fulfillment, order #%s", oid)
        change("UPDATE orders SET status='review',error=? WHERE id=? AND status='processing'", (str(e)[:450], oid))
        try:
            send(r["uid"], f"⚠️ Buyurtma #{oid} tekshirilmoqda.\nPul qabul qilingan, ammo yetkazish natijasi noaniq.\nYordamga murojaat qiling.")
        except Exception:
            pass
        notice_owner(f"⚠️ Buyurtma #{oid} tekshirilishi kerak. Qayta avtomatik yuborilmaydi.\n{esc(str(e)[:150])}")


def gift_refresh_worker():
    # Keep real gift availability reasonably fresh without depending on user clicks.
    while not STOP.is_set():
        try:
            count = gift_sync()
            log.info("Synced %s Telegram gifts", count)
        except Exception:
            log.exception("Gift catalog refresh failed; cached catalog retained")
        STOP.wait(1800)


def order_worker():
    while not STOP.is_set():
        try:
            # Mark ancient processing jobs as REVIEW, never blindly replay them.
            change("UPDATE orders SET status='review', error='Server restart while sending; check supplier' "
                   "WHERE status='processing' AND paid_at<?", (int(time.time())-120,))
            change("UPDATE payment_receipts SET status='review',error='Restart during refund: confirm bank outcome first' "
                   "WHERE status='refund_processing' AND updated<?", (int(time.time())-120,))
            for r in qall("SELECT id FROM orders WHERE status='paid' ORDER BY id LIMIT 10"):
                complete_order(r["id"])
            refund_unfulfilled_receipts()
            cutoff = int(time.time()) - 60*int(setting("payment_wait_min"))
            for r in qall("SELECT id FROM orders WHERE status='invoice' AND created<? LIMIT 50", (cutoff,)):
                release_order(r["id"])
        except Exception:
            log.exception("Background worker exception")
        STOP.wait(4)


def profile(uid):
    r = user_row(uid)
    completed = q1("SELECT COUNT(*) as c FROM orders WHERE uid=? AND status='done'", (uid,))["c"]
    invites = q1("SELECT COUNT(*) as c FROM users WHERE referrer=?", (uid,))["c"]
    send(uid, f"👤 <b>Hisobim</b>\n🆔 {uid}\n📱 @{esc(r['username'] or 'username yo‘q')}\n"
              f"💰 Referal bonus hisobingiz: {money(r['bonus_uzs'])}\n"
              f"✅ Bajarilgan buyurtmalar: {completed}\n👥 Takliflar: {invites}\n\n"
              "Referal bonusni keyingi xaridda ishlatishingiz mumkin." +
              (f"\nℹ️ Eski XTR bonusi: {r['bonus_xtr']} (v1, avtomatik almashtirilmagan)" if r['bonus_xtr'] else ""),
         rows([("👥 Referal", "referral"), ("📦 Buyurtmalar", "my_orders")], [("🏠 Bosh menyu", "home")]))


def my_orders(uid):
    items = qall("SELECT * FROM orders WHERE uid=? ORDER BY id DESC LIMIT 12", (uid,))
    if not items:
        return send(uid, "📦 Hozircha buyurtmalaringiz yo‘q.", rows([("🏠 Bosh menyu", "home")]))
    text = "📦 <b>Buyurtmalarim</b>\n\n"
    names = {"done":"✅ Bajarildi", "invoice":"⏳ To‘lov kutilmoqda", "paid":"🔄 Qabul qilindi", "processing":"🔄 Yuborilmoqda", "review":"⚠️ Tekshirilmoqda", "refunded":"↩️ Qaytarildi", "expired":"⌛ Tugagan", "cancelled":"❌ Bekor"}
    for o in items:
        text += f"#{o['id']} — {esc(product_name(o['kind'],o['units'],o['gift_id']))} — {names.get(o['status'],o['status'])}\n"
    send(uid, text, rows([("🏠 Bosh menyu", "home")]))


def referral(uid):
    global BOT_USERNAME
    if not BOT_USERNAME:
        BOT_USERNAME = bot.get_me().username
    invites = q1("SELECT COUNT(*) as c FROM users WHERE referrer=?", (uid,))["c"]
    bonus = user_row(uid)["bonus_uzs"]
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
    kb = types.InlineKeyboardMarkup()
    kb.add(copy_btn("📋 Havolani nusxalash", link))
    kb.add(types.InlineKeyboardButton("🏠 Bosh menyu", callback_data="home"))
    send(uid, f"👥 <b>Referal tizimi</b>\n\n🔗 {esc(link)}\n"
              f"👥 Do‘stlar: {invites}\n💰 Bonus hisobingiz: {money(bonus)}\n"
              f"📈 Do‘stingiz mahsulot sotib olsa, xarid qiymatining {setting('referral_pct')}% qismi hisobingizga tushadi.\n\n"
              "Bu bonusni keyingi Stars, Premium yoki Gift xaridingizda ishlatasiz. "
              "Bonus faqat muvaffaqiyatli yetkazilgan buyurtma uchun beriladi.", kb)


def support(uid):
    support_un = (setting("support_username") or "").strip().lstrip("@")
    text = "🎧 <b>Yordam</b>\nBuyurtma raqamini yozib, adminga murojaat qiling.\n"
    kb = types.InlineKeyboardMarkup()
    if support_un:
        kb.add(types.InlineKeyboardButton("👨‍💻 Admin bilan bog‘lanish", url="https://t.me/" + quote(support_un)))
    kb.add(types.InlineKeyboardButton("🏠 Bosh menyu", callback_data="home"))
    send(uid, text + ("@"+esc(support_un) if support_un else "Admin hali yordam username'ini belgilamagan."), kb)


def admin_menu(uid):
    if not is_admin(uid):
        return
    paid = q1("SELECT COUNT(*) AS c,COALESCE(SUM(price_uzs),0) AS s FROM orders WHERE status='done'")
    users = q1("SELECT COUNT(*) AS c FROM users")["c"]
    send(uid, f"⚙️ <b>ADMIN PANEL — V3</b>\n👥 Foydalanuvchilar: {users}\n"
              f"✅ Sotilgan: {paid['c']} ta\n💰 Savdo qiymati: {money(paid['s'])}\n"
              f"📦 Holat: {'🟢 Faol' if setting('live_delivery')=='1' else '🔴 Sinov'}",
         rows([("⭐ Stars paketlari", "admin_bundles_stars"), ("💎 Premium", "admin_bundles_premium")],
              [("🎁 Giftlar", "admin_gifts"), ("💳 To‘lov sozlamalari", "admin_pay")],
              [("⭐ Bot uchun Stars to‘plash", "admin_gift_topup")],
              [("👥 Referal", "admin_ref"), ("📦 Buyurtmalar", "admin_orders")],
              [("👮 Adminlar", "admin_users"), ("📢 Majburiy kanallar", "admin_channels")],
              [("📣 Xabar yuborish", "admin_broadcast"), ("💳 HUMO monitoring", "admin_humo")],
              [("🌐 API/Balans", "admin_api"), ("⚙️ Sozlamalar", "admin_settings")],
              [("✅ Narxlarni tekshirdim", "review_prices")],
              [("🔴/🟢 Savdoni yoqish", "toggle_live")],
              [("🏠 Bosh menyu", "home")]))


def admin_product(uid, product):
    if not is_admin(uid):
        return
    r = rule(product)
    title = product
    if product.startswith("gift:"):
        g = q1("SELECT title FROM gifts WHERE gift_id=?", (product[5:],))
        title = g["title"] if g else product
    elif product == "stars":
        title = "⭐ Boshqa miqdor: 1 Stars narxi"
    elif product.startswith("stars:"):
        title = "⭐ " + product.split(":",1)[1] + " Stars paketi"
    else:
        title = "💎 Premium " + product.replace("premium", "") + " oy"
    send(uid, f"💰 <b>{esc(title)}</b>\n"
              f"🧮 Rejim: {r['mode']}\n"
              f"✏️ Qo‘lda: {money(r['manual_uzs'])}\n"
              f"📈 Ustama: {r['markup_pct'] or setting('global_markup_pct')}%\n"
              f"📦 Holat: {'Faol' if r['enabled'] else 'O‘chiq'}\n\n"
              "Faqat 'stars' uchun qo‘lda narx — 1 dona; to‘plam, Premium va Gift uchun — to‘liq narx.",
         rows([("💰 Qo‘lda so‘m", "edit_manual_" + product)],
              [("📈 Ustama %", "edit_pct_" + product)],
              [("🔄 API + foiz", "mode_auto_" + product)],
              [("🚫 Yoqish / O‘chirish", "toggle_" + product)],
              [("🔙 To‘plamlar", "admin_bundles_stars" if product.startswith("stars") else
                "admin_bundles_premium" if product.startswith("premium") else "admin_gifts")]))


def admin_pay(uid):
    card = setting("card_number")
    owner = setting("card_owner")
    send(uid, f"💳 <b>Karta rekvizitlari</b>\n"
              f"💳 Karta: {esc(card or 'Kiritilmagan')}\n"
              f"👤 Egasi: {esc(owner or 'Kiritilmagan')}\n"
              f"⏳ Kutish vaqti: {setting('payment_wait_min')} daqiqa\n\n"
              "⚠️ Bu rekvizitlar raqamli mahsulotlar uchun checkout sifatida ishlatilmaydi. "
              "Stars/Premium/Gift to‘lovi Telegram XTR orqali.",
         rows([("✏️ Karta raqami", "edit_setting_card_number")],
              [("👤 Karta egasi", "edit_setting_card_owner")],
              [("⏳ Kutish vaqti", "edit_setting_payment_wait_min")],
              [("📋 Rekvizitlar/nusxalash", "admin_card_copy")],
              [("🔙 Admin", "admin")]))


def admin_settings(uid):
    send(uid, "⚙️ <b>Sozlamalar</b>\n\n"
              f"USD/UZS: {setting('usd_uzs')}\n"
              f"1 XTR (hisoblash): {setting('xtr_uzs')} UZS\n"
              f"1 Gift Stars tannarxi: {setting('gift_cost_per_star_uzs')} UZS\n"
              f"Umumiy ustama: {setting('global_markup_pct')}%\n"
              f"API komissiya: {setting('provider_fee_pct')}%\n"
              f"Minimum foyda: {setting('min_profit_pct')}%",
         rows([("💱 USD/UZS", "edit_setting_usd_uzs"), ("⭐ XTR/UZS", "edit_setting_xtr_uzs")],
              [("🎁 Gift Stars tannarxi", "edit_setting_gift_cost_per_star_uzs")],
              [("📈 Umumiy ustama", "edit_setting_global_markup_pct")],
              [("💼 API komissiya", "edit_setting_provider_fee_pct")],
              [("🛡 Minimal foyda", "edit_setting_min_profit_pct")],
              [("🎧 Yordam username", "edit_setting_support_username")],
              [("🔙 Admin", "admin")]))


def admin_orders(uid):
    rs = qall("SELECT * FROM orders ORDER BY id DESC LIMIT 15")
    text = "📦 <b>Oxirgi 15 buyurtma</b>\n\n"
    for r in rs:
        text += f"#{r['id']} • {esc(r['kind'])} • {r['status']} • {money(r['price_uzs'])}\n"
    text += "\n⚠️ 'review' — API holatini qo‘lda tekshiring, avtomatik qayta yuborilmaydi."
    kb = [[(f"⚠️ #{r['id']}", f"admin_order_{r['id']}")] for r in rs if r["status"] in ("review", "processing")]
    kb.append([("🔙 Admin", "admin")])
    send(uid, text, rows(*kb))


def admin_manual_done(oid):
    # Only after checking supplier's independent transaction/order record.
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        r = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        if not r or r["status"] != "review":
            c.execute("ROLLBACK")
            raise ValueError("Faqat 'review' buyurtmasi qo‘lda tasdiqlanadi")
        c.execute("UPDATE orders SET status='done',error='',finished_at=? WHERE id=?", (int(time.time()), oid))
        if not r["referral_awarded"]:
            u = c.execute("SELECT referrer FROM users WHERE uid=?", (r["uid"],)).fetchone()
            if u and u["referrer"]:
                bonus = int(d(r["payable_uzs"] or r["price_uzs"]) * d(setting("referral_pct")) / 100)
                if bonus:
                    c.execute("UPDATE users SET bonus_uzs=bonus_uzs+? WHERE uid=?", (bonus, u["referrer"]))
                c.execute("UPDATE orders SET referral_awarded=1 WHERE id=?", (oid,))
        c.execute("COMMIT")
    send(r["uid"], f"✅ Buyurtma #{oid} tekshirilib, yetkazilgani tasdiqlandi.")
    try:
        u = user_row(r["uid"])
        if u and u["referrer"] and not r["referral_awarded"]:
            bonus = int(d(r["payable_uzs"] or r["price_uzs"]) * d(setting("referral_pct")) / 100)
            if bonus > 0:
                send(u["referrer"], f"🎉 Do‘stingiz xarid qildi! Referal bonusi: {money(bonus)}\n"
                     "Bonus hisobingizga tushdi va keyingi xaridda ishlatishingiz mumkin.")
    except Exception:
        log.warning("Manual referral notification failed",exc_info=True)


def admin_refund_review(oid):
    # This is a real money-moving API call. Do not attempt twice after uncertain network failure.
    r = q1("SELECT * FROM orders WHERE id=?", (oid,))
    if not r or r["status"] != "review" or not r["charge_id"]:
        raise ValueError("Faqat tekshirilayotgan, to‘langan buyurtmani qaytarish mumkin")
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        cur = c.execute("UPDATE orders SET status='refund_processing' WHERE id=? AND status='review'", (oid,))
        if cur.rowcount != 1:
            c.execute("ROLLBACK")
            raise ValueError("Bu buyurtma avval ishlangan")
        c.execute("COMMIT")
    try:
        ok = bot.refund_star_payment(user_id=r["uid"], telegram_payment_charge_id=r["charge_id"])
        if not ok:
            raise RuntimeError("refundStarPayment False qaytardi")
        with DB_LOCK, db() as c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute("UPDATE orders SET status='refunded',finished_at=? "
                            "WHERE id=? AND status='refund_processing'", (int(time.time()),oid))
            if cur.rowcount and r["discount_uzs"]:
                c.execute("UPDATE users SET bonus_uzs=bonus_uzs+? WHERE uid=?",
                          (r["discount_uzs"],r["uid"]))
            if cur.rowcount and r["discount_xtr"]:
                c.execute("UPDATE users SET bonus_xtr=bonus_xtr+? WHERE uid=?",
                          (r["discount_xtr"],r["uid"]))
            c.execute("COMMIT")
        send(r["uid"], f"↩️ #{oid} buyurtmangiz uchun Telegram Stars to‘lovi qaytarildi."
             " Agar ishlatilgan referal bonusingiz bo‘lsa, u ham hisobingizga tiklandi.")
    except Exception as e:
        change("UPDATE orders SET status='review',error=? WHERE id=? AND status='refund_processing'",
               ("Refund noaniq, qayta bosmang; transactionni tekshiring: " + str(e)[:200], oid))
        raise ValueError("Refund natijasi noaniq; Telegram tranzaksiyasini tekshirmay qayta urinmang") from e


def admin_order(uid, oid):
    r = q1("SELECT * FROM orders WHERE id=?", (oid,))
    if not r:
        return
    keyboard = []
    if r["status"] == "review":
        keyboard.append([("✅ Yetkazilganini tasdiqlash", f"settle_done_{oid}")])
        keyboard.append([("↩️ XTR refund", f"settle_refund_{oid}")])
    keyboard.append([("🔙 Buyurtmalar", "admin_orders")])
    send(uid, f"🧾 <b>Buyurtma #{oid}</b>\n"
              f"👤 Xaridor ID: {r['uid']}\n🎯 {esc(r['recipient_username'] or r['recipient_uid'])}\n"
              f"📦 {esc(product_name(r['kind'],r['units'],r['gift_id']))}\n"
              f"💰 {money(r['price_uzs'])}\n⭐ {r['payable_xtr']} XTR\n"
              f"Status: {r['status']}\n"
              f"⚠️ {esc(r['error'][:300])}\n\n"
              "Oldin mustaqil yetkazib berish dalilini tekshiring. Shundan keyingina qo‘lda yopish/refund.",
         rows(*keyboard))


def parse_admin_input(uid, text, action):
    key = action["key"]
    if action["target"] == "fund_custom":
        amt = int(text.replace(" ", ""))
        session_clear(uid)
        return create_funding_invoice(uid, amt)
    if action["target"] == "bundle_add":
        kind = key
        match = re.fullmatch(r"(\d+)\s+(\d+)", text.strip())
        if not match:
            raise ValueError("Miqdor va so‘mdagi narxni bitta bo‘shliq bilan kiriting. Masalan: 75 18888")
        units, uzs = map(int, match.groups())
        if kind == "stars" and not 50 <= units <= 1000000:
            raise ValueError("Stars miqdori 50–1 000 000 oralig‘ida")
        if kind == "premium" and units not in (3,6,12):
            raise ValueError("Premium faqat 3, 6 yoki 12 oy")
        if not 1 <= uzs <= 1000000000:
            raise ValueError("So‘mdagi narx 1–1 000 000 000")
        with DB_LOCK, db() as con:
            con.execute("BEGIN IMMEDIATE")
            con.execute("INSERT INTO bundles(kind,units,active) VALUES(?,?,1) "
                        "ON CONFLICT(kind,units) DO UPDATE SET active=1", (kind, units))
            prod = f"stars:{units}" if kind == "stars" else f"premium{units}"
            con.execute("INSERT INTO rules(product,mode,manual_uzs,enabled) VALUES(?,'manual',?,1) "
                        "ON CONFLICT(product) DO UPDATE SET mode='manual',manual_uzs=excluded.manual_uzs,enabled=1",
                        (prod, uzs))
            con.execute("COMMIT")
        session_clear(uid)
        send(uid, f"✅ {units} {'Stars' if kind=='stars' else 'oy'} — {money(uzs)} qo‘shildi.")
        return bundle_list(uid, kind, admin=True)
    if action["target"] == "settings":
        if key == "card_number":
            value = re.sub(r"\s", "", text)
            if not re.fullmatch(r"\d{16,19}", value):
                raise ValueError("Karta raqami 16–19 ta raqamdan iborat bo‘lsin")
        elif key == "card_owner":
            value = text.strip()
            if not 2 <= len(value) <= 80:
                raise ValueError("Ism 2–80 ta belgidan iborat bo‘lsin")
        elif key == "support_username":
            value = valid_username(text)
        else:
            value = d(text.replace(" ", "").replace(",", "."))
            if not value.is_finite() or value < 0 or value > 100_000_000:
                raise ValueError("0 dan katta, normal son kiriting")
            if key == "payment_wait_min":
                if value != int(value) or not 5 <= value <= 60:
                    raise ValueError("5–60 oralig‘idagi butun daqiqani kiriting")
            elif key.endswith("pct") and value > 500:
                raise ValueError("Foiz 0–500 oralig‘ida bo‘lishi kerak")
            value = str(int(value)) if key == "payment_wait_min" else str(value)
        set_setting(key, value)
    elif action["target"] == "title":
        value = text.strip()
        if not 2 <= len(value) <= 40:
            raise ValueError("Gift nomi 2–40 ta belgidan iborat bo‘lsin")
        change("UPDATE gifts SET title=? WHERE gift_id=?", (value, key))
    else:
        product = key
        r = rule(product)
        if action["target"] == "manual":
            value = int(text.replace(" ", ""))
            if not 1 <= value <= 1_000_000_000:
                raise ValueError("Narx 1–1 000 000 000 so‘m oralig‘ida")
            change("INSERT INTO rules(product,mode,manual_uzs) VALUES(?,'manual',?) "
                   "ON CONFLICT(product) DO UPDATE SET mode='manual',manual_uzs=excluded.manual_uzs", (product, value))
        elif action["target"] == "pct":
            value = d(text.replace(",", "."))
            if not value.is_finite() or not 0 <= value <= 500:
                raise ValueError("0–500% kiriting")
            change("INSERT INTO rules(product,mode,markup_pct) VALUES(?,'auto',?) "
                   "ON CONFLICT(product) DO UPDATE SET mode='auto',markup_pct=excluded.markup_pct", (product, str(value)))
    session_clear(uid)
    send(uid, "✅ Saqlandi!", rows([("⚙️ Admin panel", "admin")]))


# ======================== V3: ADMINS / CHANNELS / BROADCAST ========================

def admin_users_panel(uid):
    if uid != OWNER_ID:
        return send(uid,"🔒 Adminlarni faqat bosh admin boshqaradi.")
    lst = qall("SELECT uid FROM admins ORDER BY created DESC LIMIT 60")
    k = [[(f"🗑 Admin {r['uid']}ni o‘chirish", f"admin_delask_{r['uid']}")] for r in lst]
    k.append([("➕ Kontaktlardan admin qo‘shish", "admin_select_user")])
    k.append([("🆔 ID orqali qo‘shish", "admin_add_id")])
    k.append([("🔙 Admin panel", "admin")])
    send(uid,"👮 <b>ADMINLAR</b>\n\n👑 Bosh admin: " + str(OWNER_ID) +
         "\nYordamchilar: " + str(len(lst)) +
         "\n\nAdmin bo‘ladigan odam ham botni /start qilgani yaxshi.",rows(*k))


def user_picker(uid):
    # Telegram picker, not unrestricted access to the admin's contacts.
    try:
        ru=types.KeyboardButtonRequestUsers(request_id=94007,user_is_bot=False,max_quantity=1,
                                             request_name=True,request_username=True)
        k=types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        k.add(types.KeyboardButton("👤 Kontaktlardan tanlash",request_users=ru))
        k.add(types.KeyboardButton("❌ Bekor"))
        send(uid,"👤 Quyidagi tugmani bosing. Telegram akkaunt tanlash oynasini ochadi.",k)
    except Exception:
        log.exception("User-picker API not supported")
        send(uid,"⚠️ Telegram/pyTelegramBotAPI yangilanishi kerak. ID orqali qo‘shish tugmasidan foydalaning.")


def admin_add(uid, target):
    if uid != OWNER_ID:
        raise ValueError("Faqat bosh admin")
    target=int(target)
    if target<=0 or target==OWNER_ID:
        raise ValueError("Noto‘g‘ri ID")
    change("INSERT OR IGNORE INTO admins(uid,added_by,created) VALUES(?,?,?)",
           (target,uid,int(time.time())))
    try:
        send(target,"✅ Siz Premium Market botiga admin qilib qo‘shildingiz. /admin")
    except Exception:
        pass
    send(uid,f"✅ ID {target} admin qilib qo‘shildi.",types.ReplyKeyboardRemove())
    admin_users_panel(uid)


def channel_panel(uid):
    if not is_admin(uid):return
    cs=qall("SELECT * FROM required_channels ORDER BY added DESC LIMIT 40")
    k=[[(f"🗑 {r['title'][:28]}",f"channel_delask_{r['chat_id']}")] for r in cs]
    k.extend([[ ("➕ Kanal qo‘shish","channel_add") ], [("🔙 Admin","admin")]])
    send(uid,"📢 <b>MAJBURIY KANALLAR</b>\n"+
         ("\n".join(f"• {esc(r['title'])} | {r['chat_id']}" for r in cs) if cs else "Hozircha kanal yo‘q.")+
         "\n\nKanalni tekshirish uchun botni kanalga admin qiling.",rows(*k))


def channel_add(uid,text):
    # Format @publicchannel|https://t.me/publicchannel or -100...|https://t.me/+invite
    parts=text.strip().split("|",1)
    ref=parts[0].strip()
    if not (ref.startswith("@") or re.fullmatch(r"-100\d{7,20}",ref)):
        raise ValueError("@kanal yoki -100... ID yozing")
    ch=bot.get_chat(int(ref) if ref.startswith("-100") else ref)
    me=bot.get_me().id
    st=bot.get_chat_member(ch.id,me).status
    if st not in ("administrator","creator"):
        raise ValueError("Bot kanalga ADMIN qilib qo‘shilmagan")
    if len(parts)>1:
        url=parts[1].strip()
    elif getattr(ch,"username",None):
        url="https://t.me/"+ch.username
    else:
        raise ValueError("Yopiq kanal uchun taklif havolasi kerak: -100...|https://t.me/+...")
    if not re.fullmatch(r"https://t\.me/[A-Za-z0-9_+\-/]+",url):
        raise ValueError("Telegram kanal havolasi noto‘g‘ri")
    change("INSERT INTO required_channels(chat_id,title,url,added) VALUES(?,?,?,?) "
           "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title,url=excluded.url,active=1",
           (ch.id, ch.title or ch.username or str(ch.id),url,int(time.time())))
    send(uid,"✅ Kanal qo‘shildi: "+esc(ch.title or str(ch.id)))
    channel_panel(uid)


def broadcast_panel(uid):
    if not is_admin(uid):return
    jobs=qall("SELECT * FROM broadcasts ORDER BY id DESC LIMIT 5")
    msg="📣 <b>XABAR TARQATISH</b>\n\nMatn, rasm, video, audio, GIF, ovozli xabar yoki hujjat yuborish mumkin.\n\n"
    for j in jobs:msg+=f"#{j['id']} {j['status']} | ✅{j['sent']} / ❌{j['failed']}\n"
    send(uid,msg,rows([("✉️ Yangi xabar", "broadcast_new")],[("🔙 Admin", "admin")]))


def make_broadcast_draft(uid,m):
    if not is_admin(uid):return
    # copy_message retains attachments/caption without exposing sender as forwarded
    with DB_LOCK,db() as c:
        c.execute("BEGIN IMMEDIATE")
        cur=c.execute("INSERT INTO broadcasts(sender_uid,source_chat_id,source_message_id,status,created) "
                      "VALUES(?,?,?,'draft',?)",(uid,m.chat.id,m.message_id,int(time.time())))
        jid=cur.lastrowid
        c.execute("COMMIT")
    session_clear(uid)
    send(uid,f"📣 <b>Yangi tarqatma #{jid}</b>\nQuyida xabarning nusxasi:")
    try:
        bot.copy_message(uid, m.chat.id, m.message_id)
    except Exception as exc:
        change("UPDATE broadcasts SET status='cancelled' WHERE id=?",(jid,))
        return send(uid,"❌ Ushbu xabarni nusxalab bo‘lmadi: "+esc(str(exc)[:160]))
    count=q1("SELECT COUNT(*) c FROM users")["c"]
    send(uid,f"Hozir bot bazasida {count} foydalanuvchi bor.\n\nHammaga yuborilsinmi?",
         rows([("✅ Xabarni yuborish",f"broadcast_start_{jid}")],
              [("❌ Bekor qilish",f"broadcast_cancel_{jid}")]))


def broadcast_worker():
    # Intended for one persistent VPS process, not multi-instance serverless.
    while not STOP.is_set():
        try:
            job=q1("SELECT * FROM broadcasts WHERE status='sending' ORDER BY id LIMIT 1")
            if not job:
                STOP.wait(2)
                continue
            next_user=q1("SELECT uid FROM users WHERE uid>? ORDER BY uid LIMIT 1",(job['last_uid'],))
            if not next_user:
                change("UPDATE broadcasts SET status='done' WHERE id=? AND status='sending'",(job['id'],))
                try:send(job['sender_uid'],f"✅ Tarqatma #{job['id']} tugadi.")
                except Exception:pass
                continue
            target=next_user['uid']
            ok=False
            try:
                bot.copy_message(target,job['source_chat_id'],job['source_message_id'])
                ok=True
            except Exception as e:
                # Rate limit: do not advance cursor when Telegram asks to retry.
                try:retry=int((getattr(e,'result_json',{}) or {}).get('parameters',{}).get('retry_after',0))
                except Exception:retry=0
                if retry:
                    STOP.wait(min(retry+1,90))
                    continue
                log.warning("Broadcast target %s failed: %s",target,str(e)[:120])
            with DB_LOCK,db() as c:
                c.execute("BEGIN IMMEDIATE")
                c.execute("UPDATE broadcasts SET last_uid=?,sent=sent+?,failed=failed+? "
                          "WHERE id=? AND status='sending'",(target,int(ok),int(not ok),job['id']))
                c.execute("COMMIT")
            STOP.wait(0.12)  # <= ~8 copies per second
        except Exception:
            log.exception("Broadcast worker error")
            STOP.wait(3)


# HUMO banking notifications are NOT an authenticated merchant payment webhook.
# We record incoming events once, but NEVER credit a wallet from notifications alone.
# Receipt example: 🎉 To'ldirish / ➕ 5.000,00 UZS / 💳 HUMOCARD *1382
def topup_menu(uid):
    """Explain checkout method without collecting impermissible card transfers."""
    send(uid, "💳 <b>TO‘LOV HAQIDA</b>\n\n"
              "⭐ Stars, 💎 Premium va 🎁 Gift raqamli mahsulotlar uchun "
              "Telegram ichidagi to‘lov <b>XTR</b> orqali qilinadi.\n"
              "💰 Mahsulot narxi katalogda so‘mda ko‘rsatiladi, xarid "
              "paytida to‘lanadigan XTR soni alohida chiqadi.\n\n"
              "Bank kartasiga UZS o‘tkazish orqali raqamli mahsulot sotish "
              "bu botda yoqilmagan.",
         rows([("⭐ Stars", "catalog_stars"), ("💎 Premium", "catalog_premium")],
              [("🎁 Gifts", "catalog_gifts_0")], [("🏠 Bosh menyu", "home")]))


def create_humo_test_request(uid, amount):
    if uid != OWNER_ID:
        raise ValueError("HUMO bank monitoring faqat adminning sinov bo‘limida mavjud")
    if not 5000<=amount<=50000000:
        raise ValueError("5 000–50 000 000 so‘m oralig‘idagi summa kiriting")
    card=setting("card_number")
    owner=setting("card_owner")
    if not card or not owner:
        raise ValueError("Admin hali HUMO karta raqami va egasini kiritmagan")
    now=int(time.time())
    exp=now+60*int(setting("payment_wait_min"))
    with DB_LOCK,db() as c:
        c.execute("BEGIN IMMEDIATE")
        c.execute("UPDATE humo_requests SET status='expired' WHERE status='waiting' AND expires<?",(now,))
        # Unique amounts are just matching hints, not certified payment IDs.
        pay=None
        for extra in range(1,1000):
            candidate=amount+extra
            if not c.execute("SELECT 1 FROM humo_requests WHERE pay_uzs=? AND status='waiting'",(candidate,)).fetchone():
                pay=candidate
                break
        if pay is None:
            c.execute("ROLLBACK")
            raise ValueError("Bo‘sh noyob to‘lov summasi yo‘q. Keyinroq urinib ko‘ring")
        cur=c.execute("INSERT INTO humo_requests(uid,asked_uzs,pay_uzs,created,expires) VALUES(?,?,?,?,?)",
                      (uid,amount,pay,now,exp))
        rid=cur.lastrowid
        c.execute("COMMIT")
    k=types.InlineKeyboardMarkup()
    k.add(copy_btn("📋 Karta raqamini nusxalash",card))
    k.add(copy_btn("💰 Summani nusxalash",str(pay)))
    k.add(types.InlineKeyboardButton("🔎 Holatini tekshirish",callback_data=f"topup_status_{rid}"))
    k.add(types.InlineKeyboardButton("🏠 Bosh menyu",callback_data="home"))
    send(uid,f"💳 <b>Sinov so‘rov #{rid}</b>\n\n"
             f"💳 Karta: <code>{esc(card)}</code>\n"
             f"👤 Egasi: {esc(owner)}\n"
             f"💰 So‘ralgan: {money(amount)}\n"
             f"💵 Ajratilgan noyob summa: <b>{money(pay)}</b>\n"
             f"⏳ Kutish: {setting('payment_wait_min')} daqiqa\n\n"
             "❗ Bu faqat matching/jurnal SINOVI. Haqiqiy to‘lov yubormang: "
             "rasmiy bank/merchant tasdiqlovchi integratsiyasi ulanmagan.", k)


def topup_status(uid,rid):
    r=q1("SELECT * FROM humo_requests WHERE id=? AND uid=?",(rid,uid))
    if not r:
        return send(uid,"❌ So‘rov topilmadi")
    txt={'waiting':'⏳ HUMO bildirishnomasi kutilmoqda',
         'matched_review':'🔎 HUMO bildirishnomasi mos tushdi, lekin bank tasdig‘i yo‘q',
         'expired':'⏰ Kutish vaqti tugagan'}.get(r['status'],r['status'])
    send(uid,f"💳 Sinov so‘rov #{rid}\n💰 {money(r['pay_uzs'])}\n{txt}",
         rows([("🔙 Hisob to‘ldirish", "topup")]))


HUMO_INCOMING = re.compile(r"(?:🎉\s*To['’]ldirish).*?➕\s*([\d\s.,]+)\s*UZS",re.I|re.S)
HUMO_CARD = re.compile(r"HUMOCARD\s*\*(\d{4})",re.I)
HUMO_BANK_TIME = re.compile(r"🕓\s*(\d{1,2}:\d{2})\s+(\d{2}\.\d{2}\.\d{4})")


def humo_bank_timestamp(text):
    """Timestamp from bank receipt interpreted at UTC+5 (Uzbekistan)."""
    from datetime import datetime, timezone, timedelta
    match=HUMO_BANK_TIME.search(text or "")
    if not match:
        return None
    try:
        val=datetime.strptime(match.group(2)+" "+match.group(1),"%d.%m.%Y %H:%M")
        return int(val.replace(tzinfo=timezone(timedelta(hours=5))).timestamp())
    except ValueError:
        return None


def parse_humo_notification(text):
    """Return positive incoming amount in UZS, last 4; ignore expenses/balances."""
    txt=str(text or "")
    match=HUMO_INCOMING.search(txt)
    if not match:return None
    val=match.group(1).strip().replace(" ","").replace(".","").replace(",", ".")
    try:amt=Decimal(val)
    except (InvalidOperation,ValueError):return None
    if amt<=0 or amt>Decimal("10000000000") or amt!=amt.to_integral_value():return None
    card=HUMO_CARD.search(txt)
    return int(amt),card.group(1) if card else ""


def store_humo_event(chat_id, message_id, text):
    data=parse_humo_notification(text)
    if not data:return False
    amount,card=data
    # Guard against misattributed card/other unknown cards; set an explicit last4.
    wanted=os.getenv("HUMO_CARD_LAST4", "").strip()
    status="audit_only" if wanted and wanted==card else "unverified_card"
    with DB_LOCK,db() as c:
        c.execute("BEGIN IMMEDIATE")
        cur=c.execute("INSERT OR IGNORE INTO humo_events(chat_id,message_id,amount_uzs,card_last4,kind,raw_hash,received_ts,status) "
                      "VALUES(?,?,?,?,'incoming',?,?,?)",(int(chat_id),int(message_id),amount,card,
                       hashlib.sha256(text.encode()).hexdigest(),int(time.time()),status))
        matched_req=None
        bank_ts=humo_bank_timestamp(text)
        if cur.rowcount and status=='audit_only' and bank_ts is not None:
            # Don't treat stale bank notifications as payments for a fresh request.
            req=c.execute("SELECT id,uid FROM humo_requests WHERE pay_uzs=? AND status='waiting' "
                          "AND created-120<=? AND expires>=? AND expires>=? ORDER BY id LIMIT 1",
                          (amount,bank_ts,bank_ts,int(time.time()))).fetchone()
            if req:
                c.execute("UPDATE humo_requests SET status='matched_review',event_chat_id=?,event_message_id=? "
                          "WHERE id=? AND status='waiting'",(int(chat_id),int(message_id),req['id']))
                matched_req=(req['id'],req['uid'])
        c.execute("COMMIT")
    if cur.rowcount:
        notice_owner(f"💳 HUMO kirim kuzatildi: {money(amount)}, *{card}. "
                     + (f"Sinov so‘rov #{matched_req[0]} bilan mos. " if matched_req else "Mos kelgan so‘rov yo‘q. ")
                     + "Bu rasmiy to‘lov tasdig‘i emas; balansga pul qo‘shilmadi.")
        if matched_req:
            try:send(matched_req[1],f"🔎 HUMO bildirishnomasi sinov so‘rov #{matched_req[0]} bilan mos tushdi. "
                     "Rasmiy tasdiq yo‘qligi uchun hisobingiz o‘zgarmadi.")
            except Exception:pass
        return True
    return False


def admin_humo(uid):
    if not is_admin(uid):return
    rs=qall("SELECT amount_uzs,card_last4,status,received_ts FROM humo_events ORDER BY received_ts DESC LIMIT 10")
    lines=["💳 <b>HUMO MONITORING</b>","⚠️ Xabar — bankning tasdiqlangan to‘lov webhooki emas.",
           "🔐 Telethon: "+("sozlangan" if os.getenv('TELETHON_STRING_SESSION') else "ulanmagan"),
           "💳 Karta oxiri: "+(os.getenv('HUMO_CARD_LAST4') or 'ko‘rsatilmagan'),""]
    for r in rs:lines.append(f"• {money(r['amount_uzs'])}, *{r['card_last4']}, {r['status']}")
    reqs=qall("SELECT * FROM humo_requests ORDER BY id DESC LIMIT 5")
    lines.append("\n🧪 UZS sinov so‘rovlari:")
    for r in reqs:
        lines.append(f"#{r['id']}: {money(r['pay_uzs'])} — {r['status']}")
    send(uid,"\n".join(lines),rows([("🔙 Admin", "admin")]))


def start_humo_listener():
    api_id=os.getenv("TELEGRAM_API_ID","").strip()
    api_hash=os.getenv("TELEGRAM_API_HASH","").strip()
    session=os.getenv("TELETHON_STRING_SESSION","").strip()
    if not all((api_id,api_hash,session)):
        log.info("HUMO monitor disabled (Telethon keys are missing)")
        return
    def run():
        import asyncio
        from telethon import TelegramClient,events
        from telethon.sessions import StringSession
        async def listen():
            client=TelegramClient(StringSession(session),int(api_id),api_hash)
            await client.connect()
            if not await client.is_user_authorized():
                log.error("Telethon session not authorized. Generate a new StringSession")
                await client.disconnect()
                return
            source=await client.get_entity(os.getenv('HUMO_BOT_USERNAME','HUMOcardbot').lstrip('@'))
            @client.on(events.NewMessage(from_users=source))
            async def on_bank_message(event):
                try:
                    if store_humo_event(event.chat_id,event.id,event.raw_text or ""):
                        log.info("New bank event stored once: id=%s",event.id)
                except Exception:
                    log.exception("HUMO event failed")
            log.info("HUMO monitoring started (audit-only)")
            await client.run_until_disconnected()
        asyncio.run(listen())
    threading.Thread(target=run,name="humo-listener",daemon=True).start()


@bot.message_handler(content_types=['users_shared','user_shared','contact'])
def handle_admin_share(m):
    if m.chat.type!='private' or m.from_user.id!=OWNER_ID:return
    step,dat=session_get(m.from_user.id)
    if step!='admin_select_user':return
    target=None
    if getattr(m,'users_shared',None):
        shared=m.users_shared
        if getattr(shared,'request_id',None)!=94007:return
        if getattr(shared,'users',None):target=shared.users[0].user_id
    elif getattr(m,'user_shared',None) and getattr(m.user_shared,'user_id',None):
        target=m.user_shared.user_id
    elif getattr(m,'contact',None) and getattr(m.contact,'user_id',None):
        target=m.contact.user_id
    if not target:
        return send(m.from_user.id,"❌ Bu kontakt Telegram ID raqamini ulashmadi. ID orqali qo‘shing.")
    session_clear(m.from_user.id)
    try:admin_add(m.from_user.id,target)
    except ValueError as e:send(m.from_user.id,"❌ "+esc(e))


@bot.message_handler(content_types=['photo','video','document','audio','animation','voice','sticker','video_note'])
def handle_broadcast_media(m):
    if m.chat.type!='private' or not is_admin(m.from_user.id):return
    step,_=session_get(m.from_user.id)
    if step=='broadcast_input':return make_broadcast_draft(m.from_user.id,m)


@bot.message_handler(commands=["start", "cancel", "id", "admin", "paysupport", "terms"])
def on_commands(m):
    if m.chat.type != "private":
        return
    uid = m.from_user.id
    if m.text.startswith("/start"):
        ref = re.search(r"ref_(\d+)", m.text)
        upsert_user(m.from_user, ref.group(1) if ref else None)
        session_clear(uid)
        return welcome(uid)
    upsert_user(m.from_user)
    if m.text.startswith("/cancel"):
        session_clear(uid)
        return welcome(uid)
    if m.text.startswith("/id"):
        return send(uid, f"🆔 Telegram ID: <code>{uid}</code>")
    if m.text.startswith("/admin"):
        return admin_menu(uid) if is_admin(uid) else welcome(uid)
    if m.text.startswith("/paysupport"):
        return support(uid)
    if m.text.startswith("/terms"):
        return send(uid, "📄 <b>Shartlar</b>\nMahsulot turi, qabul qiluvchi va narxni to‘lovdan oldin tekshiring. "
                    "To‘lov faqat Telegram Stars (XTR) orqali olinadi. "
                    "Yetkazib berish muammosi yuz bersa /paysupport orqali murojaat qiling. "
                    "Pulni qaytarish har bir holat bo‘yicha yetkazib berish va tranzaksiya tekshirilgach ko‘riladi.")


def record_verified_receipt(oid, uid, charge, amount):
    """Atomic, deduplicated XTR charge ledger. A paid receipt is NEVER lost.

    Returns: paid | refund_pending | duplicate | review.
    An expired/cancelled invoice might still produce a successful payment event.
    It is never fulfilled; instead we queue an XTR refund.
    """
    now = int(time.time())
    with DB_LOCK, db() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute("INSERT INTO payment_receipts(charge_id,order_id,uid,amount,status,created,updated) "
                      "VALUES(?,?,?,?,?,?,?)", (charge,oid,uid,amount,"received",now,now))
        except sqlite3.IntegrityError:
            c.execute("ROLLBACK")
            return "duplicate"
        order = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        if not order or order["uid"] != uid or order["payable_xtr"] != amount:
            c.execute("UPDATE payment_receipts SET status='review',error='No matching invoice' WHERE charge_id=?", (charge,))
            c.execute("COMMIT")
            return "review"
        if order["status"] == "invoice":
            cur = c.execute("UPDATE orders SET status='paid',charge_id=?,paid_at=? WHERE id=? AND status='invoice'",
                            (charge,now,oid))
            state = "paid" if cur.rowcount else "review"
        elif order["status"] in ("cancelled", "expired", "refunded"):
            # A real charge exists on a closed/unfulfilled order; do not deliver.
            state = "refund_pending"
        elif order["charge_id"] == charge:
            state = "duplicate"
        else:
            # Already fulfilled or another charge is recorded: refund the new charge.
            state = "refund_pending"
        c.execute("UPDATE payment_receipts SET status=?,updated=? WHERE charge_id=?",(state,now,charge))
        c.execute("COMMIT")
        return state


def refund_unfulfilled_receipts():
    """Attempt exactly once per charge; ambiguous API timeout -> review, not repeated blindly."""
    for row in qall("SELECT * FROM payment_receipts WHERE status='refund_pending' ORDER BY created LIMIT 5"):
        charge = row["charge_id"]
        with DB_LOCK,db() as c:
            c.execute("BEGIN IMMEDIATE")
            count = c.execute("UPDATE payment_receipts SET status='refund_processing',updated=? "
                              "WHERE charge_id=? AND status='refund_pending'", (int(time.time()),charge)).rowcount
            c.execute("COMMIT")
        if not count:
            continue
        try:
            ok = bot.refund_star_payment(user_id=row["uid"], telegram_payment_charge_id=charge)
            if not ok:
                raise RuntimeError("Refund returned false")
            change("UPDATE payment_receipts SET status='refunded',updated=? WHERE charge_id=? AND status='refund_processing'",
                   (int(time.time()),charge))
            try:
                send(row["uid"], f"↩️ #{row['order_id']} buyurtma yopilgan edi. {row['amount']} XTR avtomatik qaytarildi.")
            except Exception:
                log.warning("Refund notification failed", exc_info=True)
        except Exception as e:
            # Do not retry after network ambiguity (request may have succeeded).
            change("UPDATE payment_receipts SET status='review',updated=?,error=? "
                   "WHERE charge_id=? AND status='refund_processing'",
                   (int(time.time()),str(e)[:300],charge))
            notice_owner(f"⚠️ Refund natijasi noaniq. Order #{row['order_id']} — charge IDni bazada tekshiring.")


@bot.message_handler(content_types=["successful_payment"])
def on_success(m):
    uid = m.from_user.id
    sp = m.successful_payment
    funding = re.fullmatch(r"fund_(\d+)", sp.invoice_payload or "")
    if funding and sp.currency == "XTR":
        fid = int(funding.group(1))
        try:
            if mark_funding_paid(fid,uid,sp.telegram_payment_charge_id,int(sp.total_amount)):
                return send(uid, f"✅ Bot uchun ⭐{sp.total_amount} to‘landi. Gift balansini tekshiring.")
        except sqlite3.IntegrityError:
            pass
        return
    match = re.fullmatch(r"pm_(\d+)", sp.invoice_payload or "")
    if not match or sp.currency != "XTR":
        notice_owner(f"⚠️ Kutilmagan payment: user={uid}. Tekshirilsin.")
        return
    oid = int(match.group(1))
    result = record_verified_receipt(oid, uid, sp.telegram_payment_charge_id, int(sp.total_amount))
    if result == "duplicate":
        return
    if result == "refund_pending":
        send(uid, "↩️ To‘lov qabul qilindi, ammo invoice avval yopilgan. XTR qaytarish navbatiga qo‘shildi.")
        return
    if result == "review":
        notice_owner(f"⚠️ #{oid} uchun noaniq receipt: {int(sp.total_amount)} XTR; qo‘lda tekshiring.")
        return send(uid, "⚠️ To‘lov qabul qilindi, buyurtma qo‘lda tekshiriladi. /paysupport")
    send(uid, f"✅ To‘lov qabul qilindi! Buyurtma #{oid} avtomatik yetkazish navbatiga o‘tdi.")
    notice_owner(f"💳 Buyurtma #{oid} — {int(sp.total_amount)} XTR to‘landi.")


@bot.pre_checkout_query_handler(func=lambda q: True)
def on_pre_checkout(q):
    fund = re.fullmatch(r"fund_(\d+)", q.invoice_payload or "")
    if fund and q.currency == "XTR":
        f = q1("SELECT * FROM admin_funding WHERE id=?", (int(fund.group(1)),))
        ok = bool(f and f["owner_id"] == OWNER_ID == q.from_user.id and
                  f["status"] == "invoice" and f["stars"] == q.total_amount)
        return bot.answer_pre_checkout_query(q.id,ok=ok,
                       error_message=None if ok else "Invoice eskirgan yoki noto‘g‘ri")
    match = re.fullmatch(r"pm_(\d+)", q.invoice_payload or "")
    ok = False
    if match and q.currency == "XTR" and setting("live_delivery") == "1":
        r = q1("SELECT uid,payable_xtr,status,created FROM orders WHERE id=?", (int(match.group(1)),))
        ok = (r is not None and r["uid"] == q.from_user.id and r["status"] == "invoice"
              and r["payable_xtr"] == q.total_amount
              and time.time() < r["created"] + 60*int(setting("payment_wait_min")))
    bot.answer_pre_checkout_query(q.id, ok=bool(ok),
                                  error_message=None if ok else "Buyurtma muddati tugagan yoki sotuv o‘chirilgan. Yangi buyurtma yarating.")


@bot.message_handler(content_types=["text"])
def on_text(m):
    if m.chat.type != "private":
        return
    uid = m.from_user.id
    upsert_user(m.from_user)
    step, session = session_get(uid)
    text = (m.text or "").strip()
    if is_admin(uid) and step == "admin_input":
        try:
            return parse_admin_input(uid, text, session)
        except Exception as e:
            return send(uid, "❌ " + esc(e) + "\nQayta kiriting yoki /cancel.")
    if step == "broadcast_input" and is_admin(uid):
        if text == "❌ Bekor":
            session_clear(uid)
            return admin_menu(uid)
        return make_broadcast_draft(uid,m)
    if step == "admin_select_user" and uid == OWNER_ID and text == "❌ Bekor":
        session_clear(uid)
        return admin_users_panel(uid)
    if step == "admin_extra_input" and is_admin(uid):
        try:
            if session.get("target") == "channel":
                channel_add(uid,text)
            elif session.get("target") == "admin_id":
                admin_add(uid,int(text))
            session_clear(uid)
        except Exception as e:
            return send(uid,"❌ "+esc(e)+"\nQayta kiriting yoki /cancel")
        return
    if not show_subscriptions(uid):
        return
    if step == "topup_input":
        try:
            amt=int(re.sub(r"\s+","",text))
            result=create_humo_test_request(uid,amt)
            session_clear(uid)
            return result
        except Exception as e:
            return send(uid,"❌ "+esc(e)+"\nQayta summa yuboring yoki /cancel")
    if step == "recipient_input":
        try:
            username = valid_username(text)
            rec_uid = None
            if session["kind"] == "gift":
                rec = q1("SELECT uid FROM users WHERE username=?", (username,))
                if not rec:
                    raise ValueError("Gift oluvchi avval botni /start qilib, username'ini ro‘yxatdan o‘tkazsin.")
                rec_uid = int(rec["uid"])
                # Verify current chat's username; don't rely on stale mappings.
                tg = bot.get_chat(rec_uid)
                if (getattr(tg, "username", "") or "").lower() != username:
                    raise ValueError("Qabul qiluvchi username'i o‘zgargan; do‘stingiz /start yuborsin.")
            elif FRAGMENT_KEY:
                # External provider profile preflight only when the key exists.
                api_call("/getInfo", {"username": username}, timeout=18)
            return confirm_recipient(uid, username, rec_uid)
        except (ValueError, ProviderError) as e:
            return send(uid, "❌ " + esc(e) + "\nQayta username yozing yoki /cancel.")
    if step == "amount_input":
        try:
            count = int(text)
            if not 50 <= count <= 100000:
                raise ValueError("50–100 000 Stars oralig‘ida kiriting")
            return ask_recipient(uid, "stars", count)
        except ValueError as e:
            return send(uid, "❌ " + esc(e) + "\nQayta kiriting yoki /cancel.")
    return welcome(uid)


@bot.callback_query_handler(func=lambda c: True)
def on_callback(c):
    uid = c.from_user.id
    if c.message.chat.type != "private":
        bot.answer_callback_query(c.id)
        return
    upsert_user(c.from_user)
    data = c.data or ""
    try:
        bot.answer_callback_query(c.id)
    except Exception:
        pass
    try:
        if data == "check_subs":
            if show_subscriptions(uid):
                return welcome(uid)
            return
        if not show_subscriptions(uid):
            return
        if data == "admin_users" and uid == OWNER_ID:
            return admin_users_panel(uid)
        if data == "admin_select_user" and uid == OWNER_ID:
            session_set(uid,"admin_select_user")
            return user_picker(uid)
        if data == "admin_add_id" and uid == OWNER_ID:
            session_set(uid,"admin_extra_input",{"target":"admin_id"})
            return send(uid,"🆔 Yangi adminning Telegram raqamli ID sini yuboring.")
        if data.startswith("admin_delask_") and uid == OWNER_ID:
            target=int(data.rsplit("_",1)[1])
            return send(uid,f"⚠️ {target} admin huquqini olib tashlaysizmi?",
                        rows([("✅ O‘chirish",f"admin_delconfirm_{target}")],[("❌ Bekor","admin_users")]))
        if data.startswith("admin_delconfirm_") and uid == OWNER_ID:
            target=int(data.rsplit("_",1)[1])
            change("DELETE FROM admins WHERE uid=?",(target,))
            return admin_users_panel(uid)
        if data == "admin_channels" and is_admin(uid):
            return channel_panel(uid)
        if data == "channel_add" and is_admin(uid):
            session_set(uid,"admin_extra_input",{"target":"channel"})
            return send(uid,"📢 Kanalni kiriting:\n<code>@kanal</code>\nYoki yopiq kanal: <code>-1001234567890|https://t.me/+taklif</code>")
        if data.startswith("channel_delask_") and is_admin(uid):
            target=int(data.rsplit("_",1)[1])
            return send(uid,f"⚠️ Kanal {target} o‘chirilsinmi?",
                        rows([("🗑 Ha",f"channel_delconfirm_{target}")],[("❌ Bekor","admin_channels")]))
        if data.startswith("channel_delconfirm_") and is_admin(uid):
            target=int(data.rsplit("_",1)[1])
            change("DELETE FROM required_channels WHERE chat_id=?",(target,))
            return channel_panel(uid)
        if data == "admin_broadcast" and is_admin(uid):
            return broadcast_panel(uid)
        if data == "broadcast_new" and is_admin(uid):
            session_set(uid,"broadcast_input")
            return send(uid,"📣 Yubormoqchi bo‘lgan xabarni hozir botga yuboring: matn, rasm, video, audio, GIF, fayl... Bekor: /cancel")
        if data.startswith("broadcast_start_") and is_admin(uid):
            jid=int(data.rsplit("_",1)[1])
            change("UPDATE broadcasts SET status='sending' WHERE id=? AND sender_uid=? AND status='draft'",(jid,uid))
            return send(uid,f"✅ Tarqatma #{jid} yuborish navbatiga qo‘shildi.")
        if data.startswith("broadcast_cancel_") and is_admin(uid):
            jid=int(data.rsplit("_",1)[1])
            change("UPDATE broadcasts SET status='cancelled' WHERE id=? AND sender_uid=? AND status IN ('draft','sending')",(jid,uid))
            return broadcast_panel(uid)
        if data == "admin_humo" and is_admin(uid):
            return admin_humo(uid)
        if data == "home":
            session_clear(uid)
            return welcome(uid)
        if data == "copy_unsupported":
            return send(uid, "Ilovani yangilang, avtomatik nusxalash tugmasi kutubxonada qo‘llanmayapti.")
        if data == "catalog_stars":
            return bundle_list(uid, "stars")
        if data.startswith("stars_"):
            if data == "stars_custom":
                session_set(uid, "amount_input")
                return send(uid, "✏️ Nechta Stars kerak? (50–100000)\nBekor: /cancel")
            count = int(data[6:])
            if not q1("SELECT 1 FROM bundles WHERE kind='stars' AND units=? AND active=1", (count,)):
                return bundle_list(uid, "stars")
            return ask_recipient(uid, "stars", count)
        if data == "catalog_premium":
            return bundle_list(uid, "premium")
        if data.startswith("buy_bundle_"):
            _, _, kind, units = data.split("_", 3)
            units = int(units)
            if not q1("SELECT 1 FROM bundles WHERE kind=? AND units=? AND active=1", (kind, units)):
                return send(uid, "⚠️ To‘plam hozir mavjud emas.")
            return ask_recipient(uid, kind, units)
        if data.startswith("premium_"):
            count = int(data[8:])
            if not q1("SELECT 1 FROM bundles WHERE kind='premium' AND units=? AND active=1", (count,)):
                return bundle_list(uid, "premium")
            return ask_recipient(uid, "premium", count)
        if data.startswith("catalog_gifts_"):
            return gift_catalog(uid, int(data.rsplit("_", 1)[1]))
        if data.startswith("gift_"):
            return gift_card(uid, data[5:])
        if data.startswith("buy_gift_"):
            return ask_recipient(uid, "gift", 1, data[9:])
        if data in ("who_self", "who_friend"):
            step, s = session_get(uid)
            if step != "recipient_choice":
                return send(uid, "Buyurtmani qayta boshlang.", main_menu(uid))
            if data == "who_friend":
                session_set(uid, "recipient_input", s)
                return send(uid, "👥 Do‘stingizning @username'ini yozing:\nBekor: /cancel")
            if s["kind"] != "gift" and not c.from_user.username:
                return send(uid, "❌ Telegram profilingizga username qo‘ying. Fragment API username talab qiladi.")
            return confirm_recipient(uid, (c.from_user.username or "").lower(), uid)
        if data == "checkout_change":
            step, s = session_get(uid)
            if step != "checkout":
                return welcome(uid)
            session_set(uid, "recipient_choice", s)
            return send(uid, "🎯 Kimga yuborilsin?",
                        rows([("👤 O‘zimga", "who_self"), ("👥 Do‘stimga", "who_friend")]))
        if data == "checkout_pay":
            try:
                oid = create_order_from_session(uid)
                return create_invoice(uid, oid)
            except Exception as e:
                return send(uid, "⚠️ Buyurtma yaratilmadi: " + esc(e))
        if data == "profile":
            return profile(uid)
        if data == "topup":
            return topup_menu(uid)
        if data == "topup_custom":
            if uid != OWNER_ID:
                return topup_menu(uid)
            session_set(uid,"topup_input")
            return send(uid,"💳 Qancha so‘m uchun sinov so‘rovi yaratilsin? (Masalan: 20000)\n❗ Haqiqiy pul yubormang. /cancel")
        if data.startswith("topup_amount_"):
            if uid != OWNER_ID:
                return topup_menu(uid)
            return create_humo_test_request(uid,int(data.rsplit("_",1)[1]))
        if data.startswith("topup_status_"):
            return topup_status(uid,int(data.rsplit("_",1)[1]))
        if data == "my_orders":
            return my_orders(uid)
        if data == "referral":
            return referral(uid)
        if data == "support":
            return support(uid)
        if not is_admin(uid):
            return welcome(uid)
        if data == "admin":
            return admin_menu(uid)
        if data == "admin_settings":
            return admin_settings(uid)
        if data.startswith("admin_bundles_"):
            return bundle_list(uid, data.rsplit("_", 1)[1], admin=True)
        if data.startswith("admin_bundle_"):
            parts = data.split("_")
            return admin_bundle(uid, parts[2], int(parts[3]))
        if data.startswith("add_bundle_"):
            kind = data.rsplit("_", 1)[1]
            session_set(uid, "admin_input", {"target":"bundle_add", "key":kind})
            if kind == "stars":
                return send(uid, "➕ Stars to‘plami: MIQDOR NARX ko‘rinishida kiriting.\nMasalan: <code>75 18888</code>\nBekor: /cancel")
            return send(uid, "➕ Premium paketi: 3, 6 yoki 12 oy va narx kiriting.\nMasalan: <code>3 65000</code>\nBekor: /cancel")
        if data.startswith("bundle_toggle_"):
            _, _, kind, units = data.split("_", 3)
            change("UPDATE bundles SET active=1-active WHERE kind=? AND units=?", (kind, int(units)))
            return admin_bundle(uid, kind, int(units))
        if data.startswith("bundle_delete_ask_"):
            _, _, _, kind, units = data.split("_", 4)
            return send(uid, "⚠️ Bu to‘plamni katalogdan o‘chirishni tasdiqlaysizmi? Eski buyurtmalar saqlanadi.",
                        rows([("🗑 Ha, o‘chirilsin", f"bundle_delete_confirm_{kind}_{units}")],
                             [("🔙 Bekor", f"admin_bundle_{kind}_{units}")]))
        if data.startswith("bundle_delete_confirm_"):
            _, _, _, kind, units = data.split("_", 4)
            with DB_LOCK, db() as cdb:
                cdb.execute("BEGIN IMMEDIATE")
                cdb.execute("DELETE FROM bundles WHERE kind=? AND units=?", (kind, int(units)))
                cdb.execute("DELETE FROM rules WHERE product=?", (
                    f"stars:{units}" if kind == "stars" else f"premium{units}",))
                cdb.execute("COMMIT")
            return bundle_list(uid, kind, admin=True)
        if data == "admin_pay":
            return admin_pay(uid)
        if data == "admin_card_copy":
            kb = types.InlineKeyboardMarkup()
            if setting("card_number"):
                kb.add(copy_btn("📋 Karta raqamini nusxalash", setting("card_number")))
            kb.add(copy_btn("📋 Kutish vaqti", setting("payment_wait_min") + " daqiqa"))
            kb.add(types.InlineKeyboardButton("🔙 Orqaga", callback_data="admin_pay"))
            return send(uid, "📋 Rekvizitlar (admin ma'lumotlari):\n👤 " + esc(setting("card_owner")), kb)
        if data == "admin_premium":
            return send(uid, "💎 Premium narxlari", rows([("3 oy", "edit_product_premium3")],
                      [("6 oy", "edit_product_premium6")], [("12 oy", "edit_product_premium12")],
                      [("🔙 Admin", "admin")]))
        if data.startswith("edit_product_"):
            return admin_product(uid, data[len("edit_product_"):])
        if data == "admin_gifts":
            return send(uid, "🎁 <b>Giftlar boshqaruvi</b>\nStikerlar Telegramdan olinadi.",
                        rows([("🔄 Telegramdan sinxronlash", "admin_gift_sync")],
                             [("🎁 Ro‘yxat", "catalog_agifts_0")],
                             [("⭐ Botga Stars tashlash", "admin_gift_topup")],
                             [("🔙 Admin", "admin")]))
        if data == "admin_gift_sync":
            n = gift_sync()
            return send(uid, f"✅ Telegramdan {n} ta Gift olindi.",
                        rows([("🎁 Katalog", "catalog_agifts_0")], [("🔙 Admin", "admin")]))
        if data.startswith("catalog_agifts_"):
            return gift_catalog(uid, int(data.rsplit("_", 1)[1]), admin=True)
        if data.startswith("agift_"):
            return gift_card(uid, data[6:], admin=True)
        if data == "admin_ref":
            return send(uid, "👥 Referal bonusi: xaridor do‘stining muvaffaqiyatli xarididan " + setting("referral_pct") + "% SO‘M chegirma bonusi oladi.",
                        rows([("✏️ Foizni o‘zgartirish", "edit_setting_referral_pct")], [("🔙 Admin", "admin")]))
        if data == "admin_orders":
            return admin_orders(uid)
        if data.startswith("admin_order_"):
            return admin_order(uid, int(data.rsplit("_", 1)[1]))
        if data.startswith(("settle_done_", "settle_refund_")):
            oid = int(data.rsplit("_", 1)[1])
            action = "done" if data.startswith("settle_done_") else "refund"
            message = ("✅ Mahsulot haqiqatda yetkazilganini API/Telegram tarixidan tasdiqladingizmi?" if action == "done"
                       else "↩️ Mahsulot YETKAZILMAGANINI tekshirdingizmi? XTR qaytariladi. Yetkazilgan mahsulot avtomatik qaytarib olinmaydi.")
            return send(uid, message,
                        rows([("⚠️ HA, TASDIQLAYMAN", f"confirm_{action}_{oid}")],
                             [("❌ Bekor", f"admin_order_{oid}")]))
        if data.startswith("confirm_done_"):
            oid = int(data.rsplit("_", 1)[1])
            admin_manual_done(oid)
            return admin_order(uid, oid)
        if data.startswith("confirm_refund_"):
            oid = int(data.rsplit("_", 1)[1])
            admin_refund_review(oid)
            return admin_order(uid, oid)
        if data == "admin_gift_topup":
            bal = bot.get_my_star_balance()
            balance = getattr(bal, "amount", 0)
            return send(uid, f"⭐ <b>Bot Stars balansini to‘ldirish</b>\n\n"
                        f"Joriy balans: ⭐{balance}\n"
                        "Botga Stars qo‘shish uchun o‘zingiz Telegram XTR invoice'ini to‘laysiz. "
                        "To‘lov muvaffaqiyatli bo‘lsa Stars bot balansiga tushadi.\n\n"
                        "Nechta Stars o'tkazmoqchisiz?",
                        rows([("⭐ 100", "fund_100"), ("⭐ 500", "fund_500")],
                             [("⭐ 1000", "fund_1000"), ("✏️ Boshqa", "fund_custom")],
                             [("🔙 Giftlar", "admin_gifts")]))
        if data == "fund_custom":
            session_set(uid, "admin_input", {"target":"fund_custom", "key":"stars"})
            return send(uid, "⭐ Bot balansiga necha Stars o'tkazmoqchisiz? 1–100000 yozing.")
        if re.fullmatch(r"fund_\d+", data):
            return create_funding_invoice(uid, int(data.split("_",1)[1]))
        if data == "admin_api":
            lines = ["🌐 <b>API tekshiruv</b>", "Fragment kalit: " + ("✅ bor" if FRAGMENT_KEY else "❌ yo‘q")]
            try:
                bal = bot.get_my_star_balance()
                lines.append(f"Bot Stars: ⭐{getattr(bal, 'amount', '?')}")
            except Exception as e:
                lines.append("Bot Stars xatosi: " + esc(str(e)[:110]))
            if FRAGMENT_KEY:
                try:
                    wallet = api_call("/wallet/balance")
                    lines.append("Fragment hamyoni: " + esc(json.dumps(wallet,ensure_ascii=False)[:550]))
                except Exception as e:
                    lines.append("Fragment xatosi: " + esc(e))
            return send(uid, "\n".join(lines), rows([("🔙 Admin", "admin")]))
        if data == "review_prices":
            return send(uid, "⚠️ Boshlang‘ich narxlar faqat NAMUNA. Haqiqiy Fragment/Gift "
                        "tannarxi, kurs va foyda marjasini tekshirib, qo‘lda sotuv narxlarini "
                        "yangiladingizmi? Tasdiqlash sotuvni hali yoqmaydi.",
                        rows([("✅ Ha, tekshirdim", "review_prices_confirm")],
                             [("❌ Bekor", "admin")]))
        if data == "review_prices_confirm":
            set_setting("prices_reviewed", "1")
            return admin_menu(uid)
        if data == "toggle_live":
            if setting("live_delivery") == "0":
                if setting("prices_reviewed") != "1":
                    return send(uid, "⚠️ Avval barcha mahsulot narxlarini haqiqiy tannarx bilan "
                                "tekshiring va admin paneldagi 'Narxlarni tekshirdim' ni tasdiqlang.")
                if not FRAGMENT_KEY:
                    return send(uid, "⚠️ FRAGMENT_API_KEY yo‘q. Stars va Premium yetkazilishi uchun API kalit talab qilinadi.")
                if d(setting("xtr_uzs"))<=0:
                    return send(uid, "⚠️ To‘lov hisoblash uchun XTR/UZS koeffitsiyentini sozlang.")
                set_setting("live_delivery", "1")
            else:
                set_setting("live_delivery", "0")
            return admin_menu(uid)
        if data.startswith("edit_setting_"):
            key = data[len("edit_setting_"):]
            if key not in DEFAULTS:
                return
            session_set(uid, "admin_input", {"target":"settings", "key":key})
            return send(uid, f"✏️ <b>{esc(key)}</b> ning yangi qiymatini kiriting.\nBekor: /cancel")
        if data.startswith("edit_title_"):
            gid = data[len("edit_title_"):]
            session_set(uid, "admin_input", {"target":"title", "key":gid})
            return send(uid, "🎁 Yangi o‘zbekcha nomni yozing.\nBekor: /cancel")
        if data.startswith("edit_manual_"):
            prod = data[len("edit_manual_"):]
            session_set(uid, "admin_input", {"target":"manual", "key":prod})
            return send(uid, "💰 Sotuv narxini so‘mda kiriting (faqat raqam).\n"
                        "Faqat maxsus 'Boshqa miqdor' rejimida 1 Stars narxi kiritiladi; "
                        "oddiy Stars to‘plamida butun paket narxi.\nBekor: /cancel")
        if data.startswith("edit_pct_"):
            prod = data[len("edit_pct_"):]
            session_set(uid, "admin_input", {"target":"pct", "key":prod})
            return send(uid, "📈 Ustama foizini kiriting (masalan: 20).\nBekor: /cancel")
        if data.startswith("mode_auto_"):
            prod = data[len("mode_auto_"):]
            change("INSERT INTO rules(product,mode) VALUES(?,'auto') ON CONFLICT(product) DO UPDATE SET mode='auto'", (prod,))
            return admin_product(uid, prod)
        if data.startswith("toggle_"):
            prod = data[len("toggle_"):]
            change("INSERT INTO rules(product,enabled) VALUES(?,0) "
                   "ON CONFLICT(product) DO UPDATE SET enabled=1-enabled", (prod,))
            if prod.startswith("gift:"):
                change("UPDATE gifts SET active=1-active WHERE gift_id=?", (prod[5:],))
                return gift_card(uid, prod[5:], admin=True)
            return admin_product(uid, prod)
    except Exception as e:
        log.exception("Callback failed: %s", data)
        send(uid, "⚠️ Xatolik: " + esc(str(e)[:250]))


@app.get("/")
@app.get("/health")
def health():
    try:
        with db() as c:
            c.execute("SELECT 1").fetchone()
        ok = True
    except Exception:
        ok = False
    return jsonify(ok=ok, service="Premium Market V4", webhook_mode=bool(PUBLIC_URL),
                   database_ready=ok, fragment_configured=bool(FRAGMENT_KEY),
                   sales_enabled=ok and setting("live_delivery") == "1"), (200 if ok else 503)


@app.post("/tg/" + PATH_KEY)
def webhook():
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        abort(403)
    if not request.is_json:
        abort(415)
    try:
        update = types.Update.de_json(request.get_data(as_text=True))
        bot.process_new_updates([update])
    except Exception:
        log.exception("Telegram update error")
        return "error", 500
    return "ok", 200


def _webhook_maintainer():
    # Configure the endpoint after the server has begun booting; retries transient network errors.
    delay = 3
    while not STOP.is_set():
        try:
            me = bot.get_me()
            global BOT_USERNAME
            BOT_USERNAME = me.username or ""
            bot.set_my_commands([
                types.BotCommand("start", "Bosh menyu"),
                types.BotCommand("cancel", "Bekor qilish"),
                types.BotCommand("id", "Mening ID raqamim"),
                types.BotCommand("paysupport", "To‘lov bo‘yicha yordam"),
                types.BotCommand("terms", "Savdo shartlari"),
            ])
            if PUBLIC_URL:
                if not WEBHOOK_SECRET:
                    raise RuntimeError("WEBHOOK_SECRET missing for deployed webhook")
                bot.set_webhook(url=f"{PUBLIC_URL}/tg/{PATH_KEY}",
                                secret_token=WEBHOOK_SECRET,
                                allowed_updates=["message","callback_query","pre_checkout_query"],
                                drop_pending_updates=False)
                log.info("Telegram webhook active at %s (secret path hidden)", PUBLIC_URL)
            else:
                bot.remove_webhook()
                log.info("Polling mode configured")
            return
        except Exception:
            log.exception("Webhook setup failed; will retry")
            STOP.wait(delay)
            delay = min(60,delay*2)


def startup():
    if getattr(startup, "started", False):
        return
    if PUBLIC_URL and not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET is required when PUBLIC_URL/RENDER_EXTERNAL_URL is set")
    init_db()
    startup.started = True
    threading.Thread(target=order_worker, name="order-worker", daemon=True).start()
    threading.Thread(target=broadcast_worker, name="broadcast-worker", daemon=True).start()
    start_humo_listener()
    threading.Thread(target=gift_refresh_worker, name="gift-refresh", daemon=True).start()
    threading.Thread(target=_webhook_maintainer, name="webhook-setup", daemon=True).start()


if __name__ == "__main__":
    startup()
    if PUBLIC_URL:
        app.run(host="0.0.0.0",port=PORT,threaded=True,use_reloader=False)
    else:
        while True:
            try:
                bot.infinity_polling(timeout=25,long_polling_timeout=20,
                    allowed_updates=["message","callback_query","pre_checkout_query"], skip_pending=False)
            except Exception:
                log.exception("Polling error, retry")
                time.sleep(5)
