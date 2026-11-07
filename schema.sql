-- Premium Market V6: optional Supabase SQL Editor schema.
-- Bot can create tables on first startup; this file is optional.
-- This does not migrate SQLite data.

CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS users (
            uid BIGINT PRIMARY KEY, username TEXT NOT NULL DEFAULT '',
            first_name TEXT NOT NULL DEFAULT '', referrer BIGINT,
            bonus_xtr BIGINT NOT NULL DEFAULT 0 CHECK(bonus_xtr>=0),
            created BIGINT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS users_username ON users(username) WHERE username<>'';
        CREATE TABLE IF NOT EXISTS sessions (uid BIGINT PRIMARY KEY, step TEXT, data TEXT, updated BIGINT);
        CREATE TABLE IF NOT EXISTS bundles (
            kind TEXT NOT NULL CHECK(kind IN ('stars','premium')),
            units BIGINT NOT NULL CHECK(units>0),
            active BIGINT NOT NULL DEFAULT 1,
            PRIMARY KEY(kind,units));
        CREATE TABLE IF NOT EXISTS admin_funding (
            id BIGSERIAL PRIMARY KEY,
            owner_id BIGINT NOT NULL, stars BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'invoice',
            charge_id TEXT UNIQUE, created BIGINT NOT NULL);
        CREATE TABLE IF NOT EXISTS rules (
            product TEXT PRIMARY KEY, mode TEXT NOT NULL DEFAULT 'auto',
            manual_uzs BIGINT NOT NULL DEFAULT 0, markup_pct TEXT NOT NULL DEFAULT '',
            enabled BIGINT NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS gifts (
            gift_id TEXT PRIMARY KEY, title TEXT NOT NULL,
            emoji TEXT NOT NULL DEFAULT '🎁', file_id TEXT NOT NULL DEFAULT '',
            stars BIGINT NOT NULL, active BIGINT NOT NULL DEFAULT 1,
            available BIGINT NOT NULL DEFAULT 1, updated BIGINT NOT NULL);
        CREATE TABLE IF NOT EXISTS orders (
            id BIGSERIAL PRIMARY KEY, uid BIGINT NOT NULL,
            kind TEXT NOT NULL, units BIGINT NOT NULL, gift_id TEXT NOT NULL DEFAULT '',
            recipient_uid BIGINT, recipient_username TEXT NOT NULL DEFAULT '',
            cost_uzs BIGINT NOT NULL, price_uzs BIGINT NOT NULL,
            original_xtr BIGINT NOT NULL, discount_xtr BIGINT NOT NULL DEFAULT 0,
            payable_xtr BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'invoice',
            charge_id TEXT UNIQUE, api_result TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            created BIGINT NOT NULL, paid_at BIGINT, finished_at BIGINT,
            referral_awarded BIGINT NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS orders_user ON orders(uid, id DESC);
        CREATE INDEX IF NOT EXISTS orders_status ON orders(status, created);

CREATE TABLE IF NOT EXISTS payment_receipts (
            charge_id TEXT PRIMARY KEY, order_id BIGINT,
            uid BIGINT NOT NULL, amount BIGINT NOT NULL,
            status TEXT NOT NULL, created BIGINT NOT NULL,
            updated BIGINT NOT NULL, error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS payment_receipts_status ON payment_receipts(status);
        CREATE TABLE IF NOT EXISTS admins (
            uid BIGINT PRIMARY KEY, added_by BIGINT NOT NULL, created BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS required_channels (
            chat_id BIGINT PRIMARY KEY, title TEXT NOT NULL, url TEXT NOT NULL,
            active BIGINT NOT NULL DEFAULT 1, added BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS broadcasts (
            id BIGSERIAL PRIMARY KEY, sender_uid BIGINT NOT NULL,
            source_chat_id BIGINT NOT NULL, source_message_id BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft', last_uid BIGINT NOT NULL DEFAULT 0,
            sent BIGINT NOT NULL DEFAULT 0, failed BIGINT NOT NULL DEFAULT 0,
            created BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS humo_requests (
            id BIGSERIAL PRIMARY KEY, uid BIGINT NOT NULL,
            asked_uzs BIGINT NOT NULL, pay_uzs BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'waiting',
            created BIGINT NOT NULL, expires BIGINT NOT NULL,
            event_chat_id BIGINT, event_message_id BIGINT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS humo_pending_amount
        ON humo_requests(pay_uzs) WHERE status='waiting';
        CREATE INDEX IF NOT EXISTS humo_req_user ON humo_requests(uid,id DESC);
        CREATE TABLE IF NOT EXISTS humo_events (
            chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
            amount_uzs BIGINT, card_last4 TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL, raw_hash TEXT NOT NULL, received_ts BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'audit_only',
            PRIMARY KEY(chat_id,message_id)
        );

CREATE TABLE IF NOT EXISTS uzs_queue (
                id BIGSERIAL PRIMARY KEY,
                uid BIGINT NOT NULL,
                amount BIGINT NOT NULL CHECK(amount>=200),
                status TEXT NOT NULL CHECK(status IN
                ('queued','active','observed','expired','cancelled','reviewed')),
                created BIGINT NOT NULL,
                activated BIGINT NOT NULL DEFAULT 0,
                expires BIGINT NOT NULL DEFAULT 0,
                bank_chat_id BIGINT,
                bank_message_id BIGINT,
                view_message_id BIGINT,
                last_view TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_only_one_active
                ON uzs_queue(status) WHERE status='active';
            CREATE UNIQUE INDEX IF NOT EXISTS uq_one_open_per_user
                ON uzs_queue(uid) WHERE status IN ('queued','active','observed');
            CREATE UNIQUE INDEX IF NOT EXISTS uq_observed_bank_event
                ON uzs_queue(bank_chat_id,bank_message_id)
                WHERE bank_chat_id IS NOT NULL AND bank_message_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS uzs_queue_order ON uzs_queue(status,created,id);

ALTER TABLE users ADD COLUMN IF NOT EXISTS bonus_uzs BIGINT NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_uzs BIGINT NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payable_uzs BIGINT NOT NULL DEFAULT 0;

-- Prevent unauthorized Supabase Data API access to finance/customer tables.
ALTER TABLE settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE bundles ENABLE ROW LEVEL SECURITY;
ALTER TABLE admin_funding ENABLE ROW LEVEL SECURITY;
ALTER TABLE rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE gifts ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE payment_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE admins ENABLE ROW LEVEL SECURITY;
ALTER TABLE required_channels ENABLE ROW LEVEL SECURITY;
ALTER TABLE broadcasts ENABLE ROW LEVEL SECURITY;
ALTER TABLE humo_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE humo_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE uzs_queue ENABLE ROW LEVEL SECURITY;

-- V8 HUMO deposit wallet migrations
ALTER TABLE users ADD COLUMN IF NOT EXISTS balance_uzs BIGINT NOT NULL DEFAULT 0;
ALTER TABLE humo_requests ADD COLUMN IF NOT EXISTS paid_at BIGINT;
ALTER TABLE humo_requests ADD COLUMN IF NOT EXISTS confirm_source TEXT;
CREATE INDEX IF NOT EXISTS humo_open_by_user ON humo_requests(uid,status,created DESC);
