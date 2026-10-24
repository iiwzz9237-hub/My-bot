import asyncio
import json
import logging
import math
import re
import secrets
import time
import unicodedata

from datetime import datetime, timezone

import psycopg2

from aiogram import Bot, Dispatcher, BaseMiddleware, types, F
from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.methods import (
    SendMessage,
    EditMessageText,
    EditMessageCaption,
)
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    MessageEntity,
    LinkPreviewOptions,
)

from config import BOT_TOKEN, ADMIN_ID, DATABASE_URL_TABLET


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("mio")

ADMIN_USER_ID = int(ADMIN_ID)
MAX_USER_ID = 9223372036854775807

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

NORMAL_TRANSFER_DELAY = 35
BANK_TRANSFER_DELAY = 300

QUEUE_POLL_INTERVAL = 5
QUEUE_UPDATE_INTERVAL = 15
QUEUE_ERROR_RETRY = 60
QUEUE_FORBIDDEN_RETRY = 3600

USERS_PAGE_SIZE = 10
WITHDRAW_PAGE_SIZE = 8
SUSPICIOUS_MEMBERS_PAGE_SIZE = 6
SUSPICIOUS_CACHE_SECONDS = 300
BROADCAST_FAILED_PAGE_SIZE = 30

BAN_REASON_MAX_LENGTH = 1000
NORMAL_BALANCE_CAP = 500000

GIFT_PAGE_SIZE = 8
GIFT_RECIPIENT_PAGE_SIZE = 8
GIFT_MAX_HOURS = 1000000
GIFT_PANEL_REFRESH_INTERVAL = 10

GIFT_CAP_NOTICE = "مبلغ تا سقف موجودی ۵۰۰٬۰۰۰ میو واریز می‌شود."

CHANNEL_URL = "https://t.me/FreemeowieChannel"

queue_wakeup = asyncio.Event()
queue_message_lock = asyncio.Lock()
gift_panel_lock = asyncio.Lock()

queue_telegram_pause_until = 0.0
profile_refresh_pause_until = 0.0

DEFAULT_PREMIUM_PACKS = (
    "RestrictedEmoji",
    "NewsEmoji",
    "FinanceEmoji",
)
PREMIUM_PACKS = DEFAULT_PREMIUM_PACKS

PREMIUM_SELECTION_KEY = "mio_premium_selected_packs_v1"
PREMIUM_CACHE_KEY = "mio_premium_appearance_v2"
BUTTON_STYLE = "success"
MAX_OUTGOING_ENTITIES = 100

UI_EMOJIS = (
    "🐾", "💎", "📤", "👥", "🎁", "🔙",
    "✅", "❌", "⬅️", "➡️", "🔄", "📚",
    "🕒", "🆔", "👤", "🚫", "⏳", "📌",
    "🧾", "🔎", "🏦", "🎯", "📥", "💸",
    "🔍", "✉️", "💰", "📨", "⚠️", "✍️",
    "1️⃣", "2️⃣", "📋", "⏱", "🎉", "🫷",
    "😔", "🤖", "🏠", "🎨", "❤️", "🎟", "🗑",
)

premium_emojis = {}
premium_pattern = None
premium_refresh_errors = []
premium_last_error = ""
premium_last_warning_at = 0.0
premium_refresh_lock = asyncio.Lock()


# ================= DATABASE =================

db = psycopg2.connect(DATABASE_URL_TABLET)
db.autocommit = False
cursor = db.cursor()


class UserBannedError(Exception):
    pass


def rollback_db():
    try:
        db.rollback()
    except Exception:
        pass


def db_execute(query, params=None):
    global db, cursor

    try:
        cursor.execute(
            query,
            params if params is not None else (),
        )

    except (
        psycopg2.InterfaceError,
        psycopg2.OperationalError,
    ):
        try:
            db.close()
        except Exception:
            pass

        db = psycopg2.connect(DATABASE_URL_TABLET)
        db.autocommit = False
        cursor = db.cursor()

        # عملیات نامطمئن خودکار تکرار نمی‌شود.
        raise

    except psycopg2.Error:
        rollback_db()
        raise


def fetch_one(query, params=None):
    db_execute(query, params)
    row = cursor.fetchone()
    db.commit()
    return row


def fetch_all(query, params=None):
    db_execute(query, params)
    rows = cursor.fetchall()
    db.commit()
    return rows


def fetch_dicts(query, params=None):
    db_execute(query, params)
    columns = [column[0] for column in cursor.description]
    rows = [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]
    db.commit()
    return rows


def current_cursor_dict():
    row = cursor.fetchone()

    if row is None:
        return None

    columns = [column[0] for column in cursor.description]
    return dict(zip(columns, row))


def init_database():
    try:
        db_execute("SELECT pg_advisory_xact_lock(748219306)")

        statements = [
            """
            CREATE TABLE IF NOT EXISTS users(
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                balance BIGINT DEFAULT 300000,
                withdraw_requested INTEGER DEFAULT 0,
                referrer_id BIGINT DEFAULT 0,
                daily_mio INTEGER DEFAULT 0,
                last_daily TEXT DEFAULT '',
                failed_withdraw INTEGER DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS referrals(
                id SERIAL PRIMARY KEY,
                inviter_id BIGINT,
                invited_id BIGINT UNIQUE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS withdraw_queue(
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                target_username TEXT,
                amount BIGINT,
                withdraw_type TEXT,
                status INTEGER DEFAULT 0,
                created_time DOUBLE PRECISION,
                message_id BIGINT,
                completed_time DOUBLE PRECISION
            )
            """,
            """
            ALTER TABLE withdraw_queue
            ADD COLUMN IF NOT EXISTS completed_time DOUBLE PRECISION
            """,
            """
            CREATE TABLE IF NOT EXISTS withdraw_success(
                user_id BIGINT PRIMARY KEY,
                status INTEGER DEFAULT 1
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS receipt_request(
                user_id BIGINT PRIMARY KEY,
                request INTEGER DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS withdraw_receipts(
                user_id BIGINT PRIMARY KEY,
                message_id BIGINT,
                withdraw_type TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS settings(
                name TEXT PRIMARY KEY,
                value TEXT
            )
            """,
            """
            INSERT INTO settings(name, value)
            VALUES ('group_withdraw', 'on')
            ON CONFLICT (name) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS withdraw_transfer_limits(
                method TEXT PRIMARY KEY,
                delay_seconds INTEGER NOT NULL
                    CHECK (delay_seconds > 0),
                next_allowed_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bot_withdraw_queue_messages(
                request_id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                telegram_message_id BIGINT,
                last_text TEXT NOT NULL DEFAULT '',
                last_status INTEGER,
                finished BOOLEAN NOT NULL DEFAULT FALSE,
                retry_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mio_user_bans(
                user_id BIGINT PRIMARY KEY,
                reason TEXT NOT NULL,
                banned_by BIGINT NOT NULL,
                banned_at DOUBLE PRECISION NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mio_gift_codes(
                id BIGSERIAL PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                amount BIGINT NOT NULL CHECK (amount > 0),
                capacity BIGINT NOT NULL CHECK (capacity > 0),
                used_count BIGINT NOT NULL DEFAULT 0,
                duration_hours BIGINT,
                expires_at DOUBLE PRECISION,
                created_by BIGINT NOT NULL,
                created_at DOUBLE PRECISION NOT NULL,
                deleted_at DOUBLE PRECISION,
                creation_token TEXT NOT NULL UNIQUE,
                CHECK (
                    used_count >= 0
                    AND used_count <= capacity
                ),
                CHECK (
                    duration_hours IS NULL
                    OR duration_hours > 0
                )
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mio_gift_redemptions(
                gift_id BIGINT NOT NULL
                    REFERENCES mio_gift_codes(id),
                user_id BIGINT NOT NULL,
                amount BIGINT NOT NULL CHECK (amount > 0),
                redeemed_at DOUBLE PRECISION NOT NULL,
                balance_after BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                PRIMARY KEY (gift_id, user_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS mio_gift_recipient_order_idx
            ON mio_gift_redemptions(gift_id, redeemed_at, user_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS mio_active_gifts_idx
            ON mio_gift_codes(id DESC)
            WHERE deleted_at IS NULL
            """,
        ]

        for statement in statements:
            db_execute(statement)

        for method, delay in (
            ("normal", NORMAL_TRANSFER_DELAY),
            ("card", BANK_TRANSFER_DELAY),
        ):
            db_execute("""
                INSERT INTO withdraw_transfer_limits(
                    method, delay_seconds, next_allowed_at, updated_at
                )
                VALUES (%s, %s, 0, 0)
                ON CONFLICT (method) DO NOTHING
            """, (method, delay))

        db_execute("""
            CREATE OR REPLACE FUNCTION mio_guard_banned_withdrawal()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NEW.status IN (0, 1) THEN
                    IF TG_OP = 'INSERT' THEN
                        PERFORM 1
                        FROM users
                        WHERE user_id = NEW.user_id
                        FOR UPDATE;
                    END IF;

                    IF EXISTS (
                        SELECT 1
                        FROM mio_user_bans
                        WHERE user_id = NEW.user_id
                          AND active = TRUE
                    ) THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '23514',
                            MESSAGE = 'MIO_USER_BANNED',
                            CONSTRAINT = 'mio_banned_withdrawal';
                    END IF;
                END IF;

                RETURN NEW;
            END;
            $$
        """)

        db_execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'mio_banned_withdrawal_trigger'
                      AND tgrelid = 'withdraw_queue'::regclass
                      AND NOT tgisinternal
                ) THEN
                    CREATE TRIGGER mio_banned_withdrawal_trigger
                    BEFORE INSERT OR UPDATE OF user_id, status
                    ON withdraw_queue
                    FOR EACH ROW
                    EXECUTE FUNCTION mio_guard_banned_withdrawal();
                END IF;
            END;
            $$
        """)

        db_execute("""
            CREATE OR REPLACE FUNCTION mio_guard_banned_credit()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF COALESCE(NEW.balance, 0)
                   > COALESCE(OLD.balance, 0) THEN
                    IF EXISTS (
                        SELECT 1
                        FROM mio_user_bans
                        WHERE user_id = NEW.user_id
                          AND active = TRUE
                    ) THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '23514',
                            MESSAGE = 'MIO_USER_BANNED',
                            CONSTRAINT = 'mio_banned_credit';
                    END IF;
                END IF;

                RETURN NEW;
            END;
            $$
        """)

        db_execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'mio_banned_credit_trigger'
                      AND tgrelid = 'users'::regclass
                      AND NOT tgisinternal
                ) THEN
                    CREATE TRIGGER mio_banned_credit_trigger
                    BEFORE UPDATE OF balance
                    ON users
                    FOR EACH ROW
                    EXECUTE FUNCTION mio_guard_banned_credit();
                END IF;
            END;
            $$
        """)

        db_execute("""
            CREATE OR REPLACE FUNCTION mio_withdraw_destination(
                kind TEXT,
                target TEXT
            )
            RETURNS TEXT
            LANGUAGE SQL
            IMMUTABLE
            PARALLEL SAFE
            AS $$
                WITH normalized AS (
                    SELECT
                        CASE
                            WHEN kind='id' THEN
                                LOWER(
                                    REGEXP_REPLACE(
                                        BTRIM(COALESCE(target, '')),
                                        '^@',
                                        ''
                                    )
                                )
                            WHEN kind='card' THEN
                                REGEXP_REPLACE(
                                    TRANSLATE(
                                        COALESCE(target, ''),
                                        '۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩',
                                        '01234567890123456789'
                                    ),
                                    '[[:space:]-]',
                                    '',
                                    'g'
                                )
                            ELSE NULL
                        END AS destination
                )
                SELECT
                    CASE
                        WHEN kind='id'
                         AND destination ~ '^[a-z0-9_]{1,32}$'
                        THEN destination
                        WHEN kind='card'
                         AND destination ~ '^[0-9]{10,20}$'
                        THEN destination
                        ELSE NULL
                    END
                FROM normalized;
            $$
        """)

        db_execute("""
            CREATE INDEX IF NOT EXISTS mio_withdraw_destination_idx
            ON withdraw_queue(
                withdraw_type,
                mio_withdraw_destination(
                    withdraw_type,
                    target_username
                ),
                user_id,
                id
            )
            WHERE withdraw_type IN ('id', 'card')
              AND user_id > 0
        """)

        db.commit()

    except Exception:
        rollback_db()
        raise


init_database()


# ================= STATES =================

waiting_withdraw_id = set()
waiting_bank_card = set()
waiting_gift_codes = set()

admin_add_balance = {}
admin_broadcast = False
admin_pending_ban = {}
admin_gift_create = {}
admin_gift_delete = {}
admin_pack_input = {}

sending_success = set()
sending_failed = set()

gift_panel_watch = None

suspicious_cache = {
    "until_id": None,
    "groups": [],
    "loaded_at": 0.0,
}


# ================= HELPERS =================

def B(text, callback_data, style=None):
    values = {
        "text": text,
        "callback_data": callback_data,
    }

    if style:
        values["style"] = style

    return InlineKeyboardButton(**values)


def K(*rows):
    return InlineKeyboardMarkup(
        inline_keyboard=[list(row) for row in rows]
    )


def english_digits(value):
    return str(value).translate(str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789",
    ))


def persian_number(value):
    return str(value).translate(str.maketrans(
        "0123456789",
        "۰۱۲۳۴۵۶۷۸۹",
    ))


def normalize_command(value):
    text = unicodedata.normalize("NFKC", str(value or ""))

    text = text.translate(str.maketrans({
        "ي": "ی",
        "ى": "ی",
        "ك": "ک",
        "\u200c": " ",
        "\u200d": "",
        "\u200e": "",
        "\u200f": "",
        "\ufeff": "",
        "\u202a": "",
        "\u202b": "",
        "\u202c": "",
        "\u202d": "",
        "\u202e": "",
        "\u2066": "",
        "\u2067": "",
        "\u2068": "",
        "\u2069": "",
    }))

    return re.sub(r"\s+", " ", text).strip()


def parse_find_user_command(value):
    text = english_digits(normalize_command(value))

    match = re.fullmatch(
        r"(@[A-Za-z0-9_]{1,32}|[0-9]{1,19})"
        r"\s+پیداش\s*کن",
        text,
        flags=re.IGNORECASE,
    )

    if not match:
        return None

    target = match.group(1)

    if target.startswith("@"):
        return target

    user_id = int(target)
    return str(user_id) if 0 < user_id <= MAX_USER_ID else None


def admin_command_key(value):
    if parse_find_user_command(value) is not None:
        return "find_user"

    text = normalize_command(value)
    compact = text.replace(" ", "")

    commands = {
        "کاربران": "users",
        "برداشتهایاخیر": "withdrawals",
        "پیامبهکاربران": "broadcast",
        "میوبدهبههمه": "daily_all",
        "مشکوکها": "suspicious",
        "مشکوک": "suspicious",
        "کدهدیه": "gifts",
        "کدهایهدیه": "gifts",
        "پنلکدهدیه": "gifts",
        "پک": "packs",
        "پنلپک": "packs",
        "وضعیتایموجی": "emoji_status",
        "بروزرسانیایموجی": "emoji_reload",
        "بهروزرسانیایموجی": "emoji_reload",
        "لغو": "cancel",
    }

    if compact in commands:
        return commands[compact]

    command = text.split(maxsplit=1)[0] if text else ""
    command = command.split("@", 1)[0].lower()

    return {
        "/users": "users",
        "/withdrawals": "withdrawals",
        "/suspicious": "suspicious",
        "/gifts": "gifts",
        "/packs": "packs",
        "/emoji_status": "emoji_status",
        "/emoji_reload": "emoji_reload",
        "/cancel": "cancel",
    }.get(command)


def clear_input_state(user_id):
    global admin_broadcast
    global gift_panel_watch

    waiting_withdraw_id.discard(user_id)
    waiting_bank_card.discard(user_id)
    waiting_gift_codes.discard(user_id)

    if user_id == ADMIN_USER_ID:
        admin_add_balance.pop(user_id, None)
        admin_pending_ban.pop(user_id, None)
        admin_gift_create.pop(user_id, None)
        admin_gift_delete.pop(user_id, None)
        admin_pack_input.pop(user_id, None)
        admin_broadcast = False
        gift_panel_watch = None


def report_text(value, limit=80):
    text = re.sub(
        r"\s+",
        " ",
        str(value if value is not None else ""),
    ).strip()

    if not text:
        return "—"

    return text if len(text) <= limit else text[:limit - 1] + "…"


def username_text(username):
    if not username:
        return "بدون یوزرنیم"

    return "@" + report_text(username, 64).lstrip("@")


def human_duration(seconds):
    seconds = max(0, math.ceil(seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    parts = []

    if hours:
        parts.append(f"{persian_number(hours)} ساعت")
    if minutes:
        parts.append(f"{persian_number(minutes)} دقیقه")
    if seconds or not parts:
        parts.append(f"{persian_number(seconds)} ثانیه")

    return " و ".join(parts)


def format_report_time(value):
    try:
        timestamp = float(value)

        if not math.isfinite(timestamp) or timestamp <= 0:
            return "نامشخص"

        if 100_000_000_000 <= timestamp < 100_000_000_000_000:
            timestamp /= 1000

        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

    except (TypeError, ValueError, OverflowError, OSError):
        return "نامشخص"


def utf16_length(text):
    return len(text.encode("utf-16-le")) // 2


def positive_integer(value, maximum=MAX_USER_ID):
    text = english_digits(normalize_command(value))
    text = text.replace(",", "").replace("٬", "")

    if not re.fullmatch(r"[0-9]{1,19}", text):
        raise ValueError("Positive integer required")

    number = int(text)

    if not 0 < number <= maximum:
        raise ValueError("Number out of range")

    return number


def page_number(value):
    number = int(value)

    if not 0 <= number <= 1_000_000:
        raise ValueError("Invalid page")

    return number


def record_id(value):
    number = int(value)

    if not 0 < number <= MAX_USER_ID:
        raise ValueError("Invalid ID")

    return number


# ================= SETTINGS =================

def get_setting(name):
    row = fetch_one(
        "SELECT value FROM settings WHERE name=%s",
        (name,),
    )
    return row[0] if row else None


def set_setting(name, value):
    db_execute("""
        INSERT INTO settings(name, value)
        VALUES (%s, %s)
        ON CONFLICT (name)
        DO UPDATE SET value=EXCLUDED.value
    """, (name, value))
    db.commit()


# ================= PREMIUM EMOJIS =================

def normalize_emoji(value):
    return str(value or "").replace("\ufe0f", "").replace("\ufe0e", "")


def valid_custom_emoji_id(value):
    return (
        isinstance(value, str)
        and re.fullmatch(r"[0-9]{1,30}", value) is not None
    )


def valid_emoji_key(value):
    return (
        isinstance(value, str)
        and 0 < len(value) <= 40
        and not any(char.isspace() for char in value)
        and any(ord(char) > 127 for char in value)
        and bool(normalize_emoji(value))
    )


def valid_pack_name(value):
    return (
        isinstance(value, str)
        and re.fullmatch(r"[A-Za-z0-9_]{1,64}", value) is not None
    )


def parse_emoji_pack_link(value):
    if not isinstance(value, str):
        return None

    match = re.fullmatch(
        r"(?:https?://)?(?:www\.)?t\.me/addemoji/"
        r"([A-Za-z0-9_]{1,64})/?",
        value.strip(),
        flags=re.IGNORECASE,
    )

    return match.group(1) if match else None


def meowie_coin_enabled():
    return (
        len(PREMIUM_PACKS) == 1
        and PREMIUM_PACKS[0].casefold() == "meowieq"
        and bool(premium_emojis)
    )


def themed_emoji_text(text):
    if not isinstance(text, str):
        return text

    # هر دو نماد در UTF-16 دو واحد دارند.
    if meowie_coin_enabled():
        return text.replace("💎", "🪙")

    return text.replace("🪙", "💎")


def install_premium_mapping(mapping):
    global premium_pattern

    cleaned = {
        normalize_emoji(symbol): custom_id
        for symbol, custom_id in mapping.items()
        if valid_emoji_key(symbol)
        and valid_custom_emoji_id(custom_id)
    }

    patterns = []

    for symbol in sorted(cleaned, key=len, reverse=True):
        parts = []

        for char in symbol:
            parts.append(re.escape(char))

            if char not in ("\u200d", "\u20e3"):
                parts.append("[\ufe0e\ufe0f]?")

        patterns.append("".join(parts))

    compiled = re.compile("|".join(patterns)) if patterns else None

    premium_emojis.clear()
    premium_emojis.update(cleaned)
    premium_pattern = compiled


def load_premium_selection():
    global PREMIUM_PACKS

    raw = get_setting(PREMIUM_SELECTION_KEY)

    if raw is None:
        PREMIUM_PACKS = DEFAULT_PREMIUM_PACKS
        return

    try:
        packs = json.loads(raw)

        if (
            not isinstance(packs, list)
            or len(packs) > len(DEFAULT_PREMIUM_PACKS)
            or not all(valid_pack_name(name) for name in packs)
        ):
            raise ValueError("Invalid saved emoji packs")

        # لیست خالی یعنی ادمین پک را حذف کرده است.
        PREMIUM_PACKS = tuple(packs)

    except (ValueError, TypeError):
        PREMIUM_PACKS = ()
        logger.exception("تنظیم ذخیره‌شدهٔ پک معتبر نیست؛ ظاهر معمولی فعال شد")


def read_premium_cache():
    if not PREMIUM_PACKS:
        return {}

    try:
        raw = get_setting(PREMIUM_CACHE_KEY)

        if not raw:
            return {}

        data = json.loads(raw)

        if not isinstance(data, dict):
            return {}

        if data.get("packs") != list(PREMIUM_PACKS):
            return {}

        mapping = data.get("emojis")

        if not isinstance(mapping, dict):
            return {}

        return {
            normalize_emoji(symbol): custom_id
            for symbol, custom_id in mapping.items()
            if valid_emoji_key(symbol)
            and valid_custom_emoji_id(custom_id)
        }

    except Exception:
        rollback_db()
        logger.exception("خواندن کش ایموجی ناموفق")
        return {}


def save_premium_cache():
    set_setting(
        PREMIUM_CACHE_KEY,
        json.dumps(
            {
                "packs": list(PREMIUM_PACKS),
                "emojis": premium_emojis,
            },
            ensure_ascii=False,
        ),
    )


def save_premium_selection(packs, mapping, errors=None):
    global PREMIUM_PACKS
    global premium_refresh_errors
    global premium_last_error

    try:
        values = (
            (
                PREMIUM_SELECTION_KEY,
                json.dumps(list(packs), ensure_ascii=False),
            ),
            (
                PREMIUM_CACHE_KEY,
                json.dumps(
                    {
                        "packs": list(packs),
                        "emojis": mapping,
                    },
                    ensure_ascii=False,
                ),
            ),
        )

        for name, value in values:
            db_execute("""
                INSERT INTO settings(name, value)
                VALUES (%s, %s)
                ON CONFLICT (name)
                DO UPDATE SET value=EXCLUDED.value
            """, (name, value))

        db.commit()

    except Exception:
        rollback_db()
        raise

    PREMIUM_PACKS = tuple(packs)
    install_premium_mapping(mapping)
    premium_refresh_errors = list(errors or [])
    premium_last_error = ""


async def fetch_premium_pack_mapping(packs):
    fresh = {}
    errors = []

    for pack_name in packs:
        try:
            sticker_set = await bot.get_sticker_set(
                name=pack_name,
                request_timeout=20,
            )

            if sticker_set.sticker_type != "custom_emoji":
                errors.append(
                    f"{pack_name}: پک ایموجی سفارشی نیست."
                )
                continue

            count = 0

            for sticker in sticker_set.stickers:
                symbol = sticker.emoji
                custom_id = sticker.custom_emoji_id

                if (
                    not valid_emoji_key(symbol)
                    or not valid_custom_emoji_id(custom_id)
                ):
                    continue

                key = normalize_emoji(symbol)

                if key not in fresh:
                    fresh[key] = custom_id
                    count += 1

            if not count:
                errors.append(
                    f"{pack_name}: ایموجی قابل استفادهٔ جدیدی پیدا نشد."
                )

            logger.info(
                "پک بارگذاری شد؛ pack=%s added=%s",
                pack_name,
                count,
            )

        except TelegramRetryAfter as error:
            errors.append(
                f"{pack_name}: محدودیت تلگرام؛ "
                f"{int(error.retry_after)} ثانیه بعد تلاش کن."
            )
            break

        except Exception as error:
            errors.append(
                f"{pack_name}: دریافت انجام نشد "
                f"({type(error).__name__})."
            )
            logger.warning(
                "دریافت پک ناموفق؛ pack=%s type=%s",
                pack_name,
                type(error).__name__,
            )

    return fresh, errors


async def refresh_premium_emojis():
    global premium_refresh_errors

    async with premium_refresh_lock:
        if not PREMIUM_PACKS:
            install_premium_mapping({})
            premium_refresh_errors = []
            return 0

        fresh, errors = await fetch_premium_pack_mapping(PREMIUM_PACKS)

        if fresh:
            updated = dict(premium_emojis) if errors else {}
            updated.update(fresh)
            install_premium_mapping(updated)

            try:
                save_premium_cache()
            except Exception:
                rollback_db()
                logger.exception("ذخیرهٔ کش ایموجی ناموفق")
                errors.append("شناسه‌ها دریافت شدند؛ کش ذخیره نشد.")

        elif not premium_emojis:
            errors.append("هنوز شناسهٔ ایموجی بارگذاری نشده است.")

        premium_refresh_errors = errors
        return len(premium_emojis)


async def change_premium_packs(packs, admin_id, input_token=None):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    packs = tuple(packs)

    if not all(valid_pack_name(name) for name in packs):
        raise ValueError("نام پک معتبر نیست.")

    async with premium_refresh_lock:
        if input_token is not None:
            if admin_pack_input.get(admin_id) != input_token:
                return "cancelled"

            if PREMIUM_PACKS:
                return "delete_first"

        if packs:
            mapping, errors = await fetch_premium_pack_mapping(packs)
        else:
            mapping, errors = {}, []

        if (
            input_token is not None
            and admin_pack_input.get(admin_id) != input_token
        ):
            return "cancelled"

        if packs and not mapping:
            raise ValueError(
                "\n".join(errors)
                or "ایموجی قابل استفاده‌ای از این پک دریافت نشد."
            )

        save_premium_selection(packs, mapping, errors)

        if input_token is not None:
            admin_pack_input.pop(admin_id, None)

        return "ok"


def custom_id_for(symbol):
    if not PREMIUM_PACKS:
        return None

    return premium_emojis.get(
        normalize_emoji(themed_emoji_text(symbol))
    )


def is_emoji_continuation(char):
    if not char:
        return False

    code = ord(char)

    return (
        char in ("\u200d", "\ufe0e", "\ufe0f", "\u20e3")
        or 0x1F3FB <= code <= 0x1F3FF
        or 0xE0020 <= code <= 0xE007F
        or unicodedata.combining(char) != 0
    )


def iter_premium_matches(text):
    pattern = premium_pattern

    if not PREMIUM_PACKS or not pattern or not text:
        return

    for match in pattern.finditer(text):
        start, end = match.span()

        if start > 0 and text[start - 1] == "\u200d":
            continue

        if end < len(text) and is_emoji_continuation(text[end]):
            continue

        custom_id = custom_id_for(match.group(0))

        if custom_id:
            yield match, custom_id


def premium_text_entities(text, existing_entities=None):
    entities = [
        entity
        for entity in (existing_entities or [])
        if entity.type != "custom_emoji"
    ]

    if not text or not PREMIUM_PACKS or not premium_emojis:
        return entities

    formatting_types = {
        "bold", "italic", "underline", "strikethrough", "spoiler",
    }

    occupied = [
        (entity.offset, entity.offset + entity.length)
        for entity in entities
        if entity.type not in formatting_types
    ]

    offsets = [0]
    units = 0

    for char in text:
        units += 2 if ord(char) > 0xFFFF else 1
        offsets.append(units)

    for match, custom_id in iter_premium_matches(text):
        if len(entities) >= MAX_OUTGOING_ENTITIES:
            break

        start = offsets[match.start()]
        end = offsets[match.end()]

        if any(
            start < old_end and old_start < end
            for old_start, old_end in occupied
        ):
            continue

        entities.append(
            MessageEntity(
                type="custom_emoji",
                offset=start,
                length=end - start,
                custom_emoji_id=custom_id,
            )
        )
        occupied.append((start, end))

    entities.sort(key=lambda entity: (entity.offset, -entity.length))
    return entities


def plain_entity_mode(method):
    parse_mode = getattr(method, "parse_mode", None)

    return (
        parse_mode is None
        or parse_mode == ""
        or not isinstance(parse_mode, str)
    )


def decorate_keyboard(markup, custom_icons=True):
    if not isinstance(markup, InlineKeyboardMarkup):
        return markup

    rows = []

    for row in markup.inline_keyboard:
        buttons = []

        for button in row:
            data = button.model_dump(exclude_none=True)
            label = themed_emoji_text(button.text)
            data["text"] = label

            if not data.get("style"):
                data["style"] = BUTTON_STYLE

            data.pop("icon_custom_emoji_id", None)

            if custom_icons and PREMIUM_PACKS:
                for match, custom_id in iter_premium_matches(label):
                    clean_text = (
                        label[:match.start()] + label[match.end():]
                    ).strip()

                    data["text"] = clean_text or "انتخاب"
                    data["icon_custom_emoji_id"] = custom_id
                    break

            buttons.append(InlineKeyboardButton(**data))

        rows.append(buttons)

    return markup.model_copy(update={"inline_keyboard": rows})


def keyboard_has_custom_icons(markup):
    if not isinstance(markup, InlineKeyboardMarkup):
        return False

    return any(
        getattr(button, "icon_custom_emoji_id", None)
        for row in markup.inline_keyboard
        for button in row
    )


def decorate_outgoing_request(method):
    updates = {}
    markup = getattr(method, "reply_markup", None)

    if isinstance(markup, InlineKeyboardMarkup):
        updates["reply_markup"] = decorate_keyboard(markup)

    if isinstance(method, (SendMessage, EditMessageText)):
        if plain_entity_mode(method) and isinstance(method.text, str):
            text = themed_emoji_text(method.text)
            updates["text"] = text
            updates["parse_mode"] = None
            updates["entities"] = premium_text_entities(
                text, method.entities,
            )

    elif isinstance(method, EditMessageCaption):
        if plain_entity_mode(method) and isinstance(method.caption, str):
            caption = themed_emoji_text(method.caption)
            updates["caption"] = caption
            updates["parse_mode"] = None
            updates["caption_entities"] = premium_text_entities(
                caption, method.caption_entities,
            )

    return method.model_copy(update=updates) if updates else method


def request_has_custom_emoji(method):
    if keyboard_has_custom_icons(getattr(method, "reply_markup", None)):
        return True

    return any(
        entity.type == "custom_emoji"
        for field in ("entities", "caption_entities")
        for entity in (getattr(method, field, None) or [])
    )


def fallback_outgoing_request(original):
    updates = {}
    markup = getattr(original, "reply_markup", None)

    if isinstance(markup, InlineKeyboardMarkup):
        updates["reply_markup"] = decorate_keyboard(
            markup, custom_icons=False,
        )

    if isinstance(original, (SendMessage, EditMessageText)):
        if plain_entity_mode(original):
            updates["text"] = themed_emoji_text(original.text)
            updates["parse_mode"] = None
            updates["entities"] = [
                entity
                for entity in (original.entities or [])
                if entity.type != "custom_emoji"
            ]

    elif isinstance(original, EditMessageCaption):
        if plain_entity_mode(original):
            updates["caption"] = themed_emoji_text(original.caption)
            updates["parse_mode"] = None
            updates["caption_entities"] = [
                entity
                for entity in (original.caption_entities or [])
                if entity.type != "custom_emoji"
            ]

    return original.model_copy(update=updates)


def is_premium_rejection(error):
    description = str(getattr(error, "message", "")).lower()

    return any(
        marker in description
        for marker in (
            "premium_account_required",
            "premium account",
            "custom_emoji",
            "custom emoji",
            "emoji_id_invalid",
            "document_invalid",
            "document invalid",
        )
    )


class PremiumAppearanceMiddleware(BaseRequestMiddleware):
    async def __call__(self, make_request, bot, method):
        global premium_last_error
        global premium_last_warning_at

        decorated = decorate_outgoing_request(method)

        try:
            return await make_request(bot, decorated)

        except TelegramBadRequest as error:
            if (
                not request_has_custom_emoji(decorated)
                or not is_premium_rejection(error)
            ):
                raise

            premium_last_error = (
                "تلگرام ایموجی سفارشی یکی از پیام‌ها را رد کرد. "
                "پریمیوم مالک ربات یا اعتبار پک باید بررسی شود."
            )

            now = time.monotonic()

            if now - premium_last_warning_at >= 60:
                premium_last_warning_at = now
                logger.warning(
                    "ایموجی سفارشی رد شد؛ تلاش با نماد معمولی."
                )

            return await make_request(
                bot,
                fallback_outgoing_request(method),
            )


bot.session.middleware(PremiumAppearanceMiddleware())


def premium_status_text():
    missing = [
        symbol for symbol in UI_EMOJIS
        if not custom_id_for(symbol)
    ]

    lines = [
        "🎨 وضعیت ظاهر پریمیوم",
        "",
        (
            f"پک‌ها: {', '.join(PREMIUM_PACKS)}"
            if PREMIUM_PACKS
            else "پک فعال: ندارد؛ ایموجی‌ها معمولی هستند."
        ),
        f"شناسه‌های ایموجی: {len(premium_emojis)}",
        f"پوشش نمادها: {len(UI_EMOJIS) - len(missing)} از {len(UI_EMOJIS)}",
        "رنگ پیش‌فرض دکمه‌ها: سبز",
        "کد منقضی یا پُر: قرمز",
        f"نماد موجودی: {themed_emoji_text('💎')}",
    ]

    if missing:
        lines.extend([
            "",
            "نمادهای بدون ایموجی سفارشی متناظر:",
            " ".join(missing),
        ])

    if premium_refresh_errors:
        lines.extend(["", *premium_refresh_errors])

    if premium_last_error:
        lines.extend(["", premium_last_error])

    lines.extend([
        "",
        "🐾 💎 📤 👥 🎁 ✅ ❌ 🔄 🏦 ❤️",
        "",
        "/packs",
        "/emoji_reload",
        "/emoji_status",
    ])

    return "\n".join(lines)


def premium_preview_keyboard():
    return K(
        [B("💎 نمونهٔ دکمهٔ سبز", "premium:preview")],
        [B("🔄 دریافت مجدد ایموجی‌ها", "premium:reload")],
        [B("🎨 پک", "ep:panel")],
    )


def build_pack_panel():
    lines = [
        "🎨 پنل پک ایموجی",
        "",
    ]
    buttons = []

    if PREMIUM_PACKS:
        lines.append("پک‌های انتخاب‌شده:")

        for pack_name in PREMIUM_PACKS:
            lines.append(f"https://t.me/addemoji/{pack_name}")

        lines.extend([
            "",
            "برای وارد کردن پک جدید، اول «حذف پک فعلی» را بزن.",
        ])

        buttons.append([
            B("🗑 حذف پک فعلی", "ep:delete", "danger")
        ])

    else:
        lines.extend([
            "پک فعال نیست؛ ایموجی‌ها به حالت معمولی برگشته‌اند.",
            "",
            "برای انتخاب پک، «اضافه کردن پک» را بزن و لینک آن را بفرست.",
        ])

        buttons.append([
            B("🎨 اضافه کردن پک", "ep:add")
        ])

    lines.extend([
        "",
        "بازنشانی، پک‌های اولیهٔ ربات را برمی‌گرداند:",
        ", ".join(DEFAULT_PREMIUM_PACKS),
        "",
        "فقط با پک MeowieQ نماد موجودی به میوکوین تبدیل می‌شود.",
        "نمادهایی که در پک موجود نباشند، معمولی نمایش داده می‌شوند.",
        "",
        "نمونه:",
        "💎 ✅ ❌ 🎁 📤 ❤️",
    ])

    if premium_refresh_errors:
        lines.extend(["", *premium_refresh_errors])

    buttons.extend([
        [B("🔄 بازنشانی به پک‌های پیش‌فرض", "ep:reset")],
        [B("🏠 برگشت به خانه", "main_menu")],
    ])

    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


async def show_pack_panel(message, edit=False):
    if (
        message.chat.type != "private"
        or message.chat.id != ADMIN_USER_ID
    ):
        return

    text, keyboard = build_pack_panel()

    if edit:
        await edit_report_message(message, text, keyboard)
    else:
        await message.answer(
            text,
            reply_markup=keyboard,
            parse_mode=None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )


# ================= BAN DATABASE =================

def get_user_ban(user_id):
    rows = fetch_dicts("""
        SELECT user_id, reason, banned_by, banned_at, active
        FROM mio_user_bans
        WHERE user_id=%s AND active=TRUE
    """, (user_id,))
    return rows[0] if rows else None


def is_user_banned(user_id):
    return bool(fetch_one("""
        SELECT 1
        FROM mio_user_bans
        WHERE user_id=%s AND active=TRUE
    """, (user_id,)))


def ban_notice(reason):
    return f"🚫 شما از ربات بن شدید.\n\nدلیل:\n{reason}"


def ban_registered_user(user_id, reason, admin_id):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    if user_id == ADMIN_USER_ID:
        raise ValueError("Admin cannot be banned")

    reason = reason.strip()

    if not reason or len(reason) > BAN_REASON_MAX_LENGTH:
        raise ValueError("Invalid ban reason")

    try:
        db_execute("""
            SELECT user_id FROM users
            WHERE user_id=%s FOR UPDATE
        """, (user_id,))

        if cursor.fetchone() is None:
            db.commit()
            return {
                "found": False,
                "already_banned": False,
                "cancelled": 0,
                "processing": 0,
            }

        db_execute("""
            SELECT 1 FROM mio_user_bans
            WHERE user_id=%s AND active=TRUE
        """, (user_id,))

        if cursor.fetchone():
            db.commit()
            return {
                "found": True,
                "already_banned": True,
                "cancelled": 0,
                "processing": 0,
            }

        db_execute("""
            INSERT INTO mio_user_bans(
                user_id, reason, banned_by, banned_at, active
            )
            VALUES (
                %s, %s, %s,
                EXTRACT(EPOCH FROM clock_timestamp()), TRUE
            )
            ON CONFLICT (user_id) DO UPDATE SET
                reason=EXCLUDED.reason,
                banned_by=EXCLUDED.banned_by,
                banned_at=EXCLUDED.banned_at,
                active=TRUE
        """, (user_id, reason, admin_id))

        db_execute(
            "UPDATE users SET daily_mio=0 WHERE user_id=%s",
            (user_id,),
        )

        db_execute("""
            UPDATE withdraw_queue
            SET
                status=4,
                completed_time=COALESCE(
                    completed_time,
                    EXTRACT(EPOCH FROM clock_timestamp())
                )
            WHERE user_id=%s AND status=0
        """, (user_id,))
        cancelled = cursor.rowcount

        db_execute("""
            SELECT COUNT(*) FROM withdraw_queue
            WHERE user_id=%s AND status=1
        """, (user_id,))
        processing = int(cursor.fetchone()[0])

        db_execute("""
            UPDATE bot_withdraw_queue_messages
            SET
                finished=TRUE,
                updated_at=EXTRACT(EPOCH FROM clock_timestamp())
            WHERE user_id=%s AND finished=FALSE
        """, (user_id,))

        db_execute("""
            UPDATE receipt_request
            SET request=0 WHERE user_id=%s
        """, (user_id,))

        db.commit()
        clear_input_state(user_id)
        queue_wakeup.set()

        return {
            "found": True,
            "already_banned": False,
            "cancelled": cancelled,
            "processing": processing,
        }

    except Exception:
        rollback_db()
        raise


def unban_registered_user(user_id, admin_id):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    try:
        db_execute("""
            SELECT user_id FROM users
            WHERE user_id=%s FOR UPDATE
        """, (user_id,))

        if cursor.fetchone() is None:
            db.commit()
            return {"found": False, "changed": False}

        db_execute("""
            UPDATE mio_user_bans SET active=FALSE
            WHERE user_id=%s AND active=TRUE
        """, (user_id,))

        changed = cursor.rowcount > 0
        db.commit()
        clear_input_state(user_id)

        return {"found": True, "changed": changed}

    except Exception:
        rollback_db()
        raise


# ================= BALANCE / PROFILE =================

def get_balance(user_id):
    row = fetch_one(
        "SELECT balance FROM users WHERE user_id=%s",
        (user_id,),
    )
    return int(row[0] or 0) if row else 0


def add_balance(user_id, amount):
    # اگر موجودی قدیمی بالاتر از سقف باشد، با شارژ مثبت کم نمی‌شود.
    db_execute("""
        UPDATE users AS u
        SET balance=(
            CASE
                WHEN %s >= 0 THEN
                    GREATEST(
                        COALESCE(balance, 0)::NUMERIC,
                        LEAST(
                            %s::NUMERIC,
                            COALESCE(balance, 0)::NUMERIC + %s
                        )
                    )
                ELSE COALESCE(balance, 0)::NUMERIC + %s
            END
        )::BIGINT
        WHERE u.user_id=%s
          AND NOT EXISTS (
              SELECT 1 FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id AND b.active=TRUE
          )
        RETURNING balance
    """, (
        amount, NORMAL_BALANCE_CAP, amount, amount, user_id,
    ))

    row = cursor.fetchone()
    db.commit()

    if row:
        return int(row[0])

    if is_user_banned(user_id):
        raise UserBannedError()

    return 0


def reset_balance(user_id):
    db_execute(
        "UPDATE users SET balance=0 WHERE user_id=%s",
        (user_id,),
    )
    db.commit()


def sync_user_profile(user_id, username, first_name):
    db_execute("""
        UPDATE users
        SET username=%s, first_name=%s
        WHERE user_id=%s
          AND (
              username IS DISTINCT FROM %s
              OR first_name IS DISTINCT FROM %s
          )
    """, (
        username, first_name, user_id, username, first_name,
    ))
    db.commit()


def add_user(user_id, username, first_name, referrer_id=0):
    try:
        db_execute("""
            INSERT INTO users(
                user_id, username, first_name, referrer_id
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            RETURNING user_id
        """, (user_id, username, first_name, referrer_id))

        inserted = cursor.fetchone()

        if not inserted:
            db_execute("""
                UPDATE users SET username=%s, first_name=%s
                WHERE user_id=%s
            """, (username, first_name, user_id))
            db.commit()
            return False

        if referrer_id and referrer_id != user_id:
            db_execute("""
                INSERT INTO referrals(inviter_id, invited_id)
                VALUES (%s, %s)
                ON CONFLICT (invited_id) DO NOTHING
            """, (referrer_id, user_id))

            if cursor.rowcount:
                db_execute("""
                    UPDATE users AS u
                    SET balance=GREATEST(
                        COALESCE(balance, 0)::NUMERIC,
                        LEAST(
                            500000::NUMERIC,
                            COALESCE(balance, 0)::NUMERIC + 400000
                        )
                    )::BIGINT
                    WHERE u.user_id=%s
                      AND NOT EXISTS (
                          SELECT 1 FROM mio_user_bans AS b
                          WHERE b.user_id=u.user_id AND b.active=TRUE
                      )
                """, (referrer_id,))

        db.commit()
        return True

    except Exception:
        rollback_db()
        raise


def enable_daily_mio_by_admin(user_id, admin_id):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    try:
        # با فعال‌سازی خود کاربر و ثبت بن هماهنگ است.
        db_execute("""
            SELECT daily_mio
            FROM users
            WHERE user_id=%s
            FOR UPDATE
        """, (user_id,))
        row = cursor.fetchone()

        if row is None:
            db.commit()
            return "missing"

        db_execute("""
            SELECT 1
            FROM mio_user_bans
            WHERE user_id=%s
              AND active=TRUE
        """, (user_id,))

        if cursor.fetchone():
            db.commit()
            return "banned"

        if row[0] == 1:
            db.commit()
            return "already"

        # فعال‌سازی مدیریتی فقط پرچم روزانه را روشن می‌کند.
        # موجودی و پاداش فعال‌سازی تغییر نمی‌کنند.
        db_execute("""
            UPDATE users
            SET daily_mio=1
            WHERE user_id=%s
        """, (user_id,))

        db.commit()
        return "ok"

    except Exception:
        rollback_db()
        raise


class UserProfileMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        global gift_panel_watch

        user = getattr(event, "from_user", None)

        if user is None or user.is_bot:
            return await handler(event, data)

        if user.id == ADMIN_USER_ID:
            gift_panel_watch = None

            if isinstance(event, CallbackQuery):
                admin_pack_input.pop(user.id, None)

        if isinstance(event, CallbackQuery):
            waiting_gift_codes.discard(user.id)

        if user.id != ADMIN_USER_ID:
            try:
                banned = get_user_ban(user.id)

            except Exception:
                rollback_db()
                logger.exception(
                    "بررسی بن ناموفق؛ user_id=%s", user.id,
                )

                if isinstance(event, CallbackQuery):
                    await event.answer(
                        "بررسی دسترسی انجام نشد؛ کمی بعد تلاش کن.",
                        show_alert=True,
                    )

                elif (
                    isinstance(event, types.Message)
                    and event.chat.type == "private"
                ):
                    await event.answer(
                        "بررسی حساب انجام نشد؛ کمی بعد دوباره تلاش کن."
                    )

                return None

            if banned:
                if isinstance(event, CallbackQuery):
                    await event.answer(
                        "🚫 شما مسدود شده‌اید.\n"
                        "برای مشاهدهٔ دلیل، /start را بزنید.",
                        show_alert=True,
                    )

                elif (
                    isinstance(event, types.Message)
                    and event.chat.type == "private"
                ):
                    text = (event.text or "").strip()
                    command = text.split(maxsplit=1)[0] if text else ""
                    command = command.split("@", 1)[0].lower()

                    if command == "/start":
                        await event.answer(
                            ban_notice(banned["reason"]),
                            parse_mode=None,
                        )

                return None

        try:
            sync_user_profile(
                user.id, user.username, user.first_name,
            )

        except Exception:
            rollback_db()
            logger.exception(
                "به‌روزرسانی پروفایل ناموفق؛ user_id=%s", user.id,
            )

        return await handler(event, data)


dp.message.outer_middleware(UserProfileMiddleware())
dp.callback_query.outer_middleware(UserProfileMiddleware())


async def refresh_page_profiles(rows):
    # صفحه‌بندی کاربران و مشکوک‌ها از این تابع استفاده نمی‌کند.
    global profile_refresh_pause_until

    profiles = {}
    attempted = set()

    for row in rows:
        user_id = row["user_id"]

        if user_id in attempted:
            continue

        attempted.add(user_id)

        if time.monotonic() < profile_refresh_pause_until:
            break

        try:
            chat = await bot.get_chat(user_id, request_timeout=5)

            if chat.type != "private":
                continue

            sync_user_profile(user_id, chat.username, chat.first_name)
            profiles[user_id] = (chat.username, chat.first_name)

        except TelegramRetryAfter as error:
            profile_refresh_pause_until = (
                time.monotonic() + int(error.retry_after) + 1
            )
            break

        except (TelegramBadRequest, TelegramForbiddenError):
            continue

        except Exception:
            rollback_db()
            logger.exception(
                "خواندن پروفایل ناموفق؛ user_id=%s", user_id,
            )
            break

    for row in rows:
        if row["user_id"] in profiles:
            row["username"], row["first_name"] = profiles[row["user_id"]]

    return len(profiles), len({row["user_id"] for row in rows})


# ================= KEYBOARDS / MESSAGE EDIT =================

def menu_keyboard(user_id=None):
    rows = [
        [B("📤 برداشت", "withdraw")],
        [B("👥 لینک دعوت", "invite")],
        [B("🎁 دریافت میو روزانه", "daily_mio")],
        [B("🎟 ثبت کد هدیه", "gift")],
    ]

    if user_id == ADMIN_USER_ID:
        rows.append([B("🎨 پک", "ep:panel")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_keyboard(callback_data="main_menu"):
    return K([B("🔙 برگشت", callback_data)])


def receipt_keyboard():
    return K(
        [B("✅ بله، رسید می‌خواهم", "want_receipt")],
        [B("❌ خیر", "no_receipt")],
    )


def gift_balance_warning():
    return (
        "⚠️ موجودیت به سقف ۵۰۰٬۰۰۰ میو رسیده!\n\n"
        "اول موجودیت رو برداشت کن، بعد کد هدیه رو وارد کن.\n\n"
        "کد هدیه مصرف نشده و از ظرفیتش هم کم نشده است.\n"
        "بعد از برداشت می‌تونی دوباره از کد استفاده کنی؛ "
        "به شرط اینکه هنوز زمان و ظرفیت داشته باشد."
    )


def gift_withdraw_keyboard():
    return K(
        [B("📤 برداشت موجودی", "withdraw")],
        [B("🏠 برگشت به خانه", "main_menu")],
    )


async def edit_report_message(message, text, keyboard):
    try:
        return await message.edit_text(
            text,
            reply_markup=keyboard,
            parse_mode=None,
        )

    except TelegramBadRequest as error:
        description = str(error).lower()

        if "message is not modified" in description:
            return message

        if (
            "message to edit not found" in description
            or "message can't be edited" in description
            or "message can not be edited" in description
        ):
            return await message.answer(
                text,
                reply_markup=keyboard,
                parse_mode=None,
            )

        raise


# ================= GIFT DATABASE =================

def normalize_gift_code(value):
    code = english_digits(normalize_command(value)).casefold()

    if not re.fullmatch(r"[\w-]{1,64}", code, flags=re.UNICODE):
        return None

    return code


def gift_status(row):
    if row["deleted_at"] is not None:
        return "deleted"

    if (
        row["expires_at"] is not None
        and float(row["expires_at"]) <= float(row["server_now"])
    ):
        return "expired"

    if int(row["used_count"]) >= int(row["capacity"]):
        return "full"

    return "active"


def gift_status_label(status):
    return {
        "active": "✅ فعال",
        "expired": "❌ منقضی؛ مهلت تمام شده",
        "full": "❌ منقضی؛ ظرفیت تکمیل شده",
        "deleted": "❌ حذف‌شده",
    }[status]


def read_gift(gift_id):
    rows = fetch_dicts("""
        SELECT
            g.*,
            EXTRACT(EPOCH FROM clock_timestamp()) AS server_now
        FROM mio_gift_codes AS g
        WHERE g.id=%s
    """, (gift_id,))
    return rows[0] if rows else None


def create_gift_code(state, capacity, admin_id):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    try:
        db_execute("""
            SELECT id
            FROM mio_gift_codes
            WHERE creation_token=%s
        """, (state["token"],))
        existing = cursor.fetchone()

        if existing:
            db.commit()
            return existing[0]

        db_execute("""
            INSERT INTO mio_gift_codes(
                code, amount, capacity, duration_hours,
                expires_at, created_by, created_at, creation_token
            )
            VALUES (
                %s, %s, %s, %s,
                CASE
                    WHEN %s::BIGINT IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM clock_timestamp())
                         + %s::BIGINT * 3600
                END,
                %s,
                EXTRACT(EPOCH FROM clock_timestamp()),
                %s
            )
            ON CONFLICT (creation_token) DO NOTHING
            RETURNING id
        """, (
            state["code"],
            state["amount"],
            capacity,
            state["hours"],
            state["hours"],
            state["hours"],
            admin_id,
            state["token"],
        ))

        row = cursor.fetchone()

        if row is None:
            db_execute("""
                SELECT id FROM mio_gift_codes
                WHERE creation_token=%s
            """, (state["token"],))
            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Gift creation result unavailable")

        db.commit()
        return row[0]

    except Exception:
        rollback_db()
        raise


def delete_gift_code(gift_id, admin_id):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    try:
        db_execute("""
            UPDATE mio_gift_codes
            SET deleted_at=EXTRACT(EPOCH FROM clock_timestamp())
            WHERE id=%s AND deleted_at IS NULL
        """, (gift_id,))
        changed = cursor.rowcount > 0
        db.commit()
        return changed

    except Exception:
        rollback_db()
        raise


def redeem_gift_code(user_id, code):
    try:
        # قفل حساب: دریافت هم‌زمان چند هدیه از سقف عبور نمی‌کند.
        db_execute("""
            SELECT user_id, balance, username, first_name
            FROM users
            WHERE user_id=%s
            FOR UPDATE
        """, (user_id,))
        user = current_cursor_dict()

        if user is None:
            db.commit()
            return {"status": "unregistered"}

        db_execute("""
            SELECT 1
            FROM mio_user_bans
            WHERE user_id=%s AND active=TRUE
        """, (user_id,))

        if cursor.fetchone():
            raise UserBannedError()

        # قفل کد: ظرفیت بین کاربران مختلف هماهنگ می‌شود.
        db_execute("""
            SELECT *
            FROM mio_gift_codes
            WHERE code=%s
            FOR UPDATE
        """, (code,))
        gift = current_cursor_dict()

        if gift is None or gift["deleted_at"] is not None:
            db.commit()
            return {"status": "invalid"}

        db_execute("""
            SELECT amount
            FROM mio_gift_redemptions
            WHERE gift_id=%s AND user_id=%s
        """, (gift["id"], user_id))
        previous = cursor.fetchone()

        if previous:
            db.commit()
            return {
                "status": "already",
                "amount": int(previous[0]),
            }

        db_execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
        server_now = float(cursor.fetchone()[0])

        if (
            gift["expires_at"] is not None
            and float(gift["expires_at"]) <= server_now
        ):
            db.commit()
            return {"status": "expired"}

        if int(gift["used_count"]) >= int(gift["capacity"]):
            db.commit()
            return {"status": "full"}

        old_balance = int(user["balance"] or 0)
        requested_amount = int(gift["amount"])

        available_space = max(
            0,
            NORMAL_BALANCE_CAP - old_balance,
        )

        # موجودی پُر: نه کد مصرف می‌شود، نه ظرفیت.
        if available_space == 0:
            db.commit()
            return {
                "status": "balance_full",
                "balance": old_balance,
            }

        credited_amount = min(
            requested_amount,
            available_space,
        )

        db_execute("""
            UPDATE mio_gift_codes
            SET used_count=used_count + 1
            WHERE id=%s
              AND deleted_at IS NULL
              AND used_count < capacity
              AND (
                  expires_at IS NULL
                  OR expires_at >
                     EXTRACT(EPOCH FROM clock_timestamp())
              )
            RETURNING
                EXTRACT(EPOCH FROM clock_timestamp())
        """, (gift["id"],))
        claimed = cursor.fetchone()

        if claimed is None:
            db.commit()
            return {"status": "expired"}

        db_execute("""
            UPDATE users
            SET balance=COALESCE(balance, 0) + %s
            WHERE user_id=%s
            RETURNING balance
        """, (credited_amount, user_id))
        new_balance = int(cursor.fetchone()[0])

        # سابقه شامل مبلغ واقعی واریزشده است.
        db_execute("""
            INSERT INTO mio_gift_redemptions(
                gift_id, user_id, amount, redeemed_at,
                balance_after, username, first_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            gift["id"],
            user_id,
            credited_amount,
            float(claimed[0]),
            new_balance,
            user["username"],
            user["first_name"],
        ))

        db.commit()

        return {
            "status": "ok",
            "amount": credited_amount,
            "requested_amount": requested_amount,
            "balance": new_balance,
        }

    except Exception:
        rollback_db()
        raise


# ================= GIFT ADMIN PANELS =================

def gift_cancel_keyboard():
    return K([B("❌ لغو و برگشت به پنل", "gc:list:0")])


def build_gift_list(page=0):
    total = int(fetch_one("""
        SELECT COUNT(*)
        FROM mio_gift_codes
        WHERE deleted_at IS NULL
    """)[0])

    pages = max(1, math.ceil(total / GIFT_PAGE_SIZE))
    page = min(max(0, page), pages - 1)

    rows = fetch_dicts("""
        SELECT
            g.*,
            EXTRACT(EPOCH FROM clock_timestamp()) AS server_now
        FROM mio_gift_codes AS g
        WHERE g.deleted_at IS NULL
        ORDER BY g.id DESC
        LIMIT %s OFFSET %s
    """, (GIFT_PAGE_SIZE, page * GIFT_PAGE_SIZE))

    text = (
        "🎟 پنل مدیریت کد هدیه\n\n"
        f"تعداد کدها: {total:,}\n"
        f"صفحه {page + 1} از {pages}\n\n"
        "سبز: قابل دریافت\n"
        "قرمز: پایان مهلت یا تکمیل ظرفیت\n\n"
        f"{GIFT_CAP_NOTICE}\n\n"
        "برای مشاهدهٔ اطلاعات، روی کد بزن."
    )

    if not rows:
        text += "\n\nهنوز کد هدیه‌ای ثبت نشده است."

    buttons = [[B("🎁 ثبت کد هدیه جدید", "gc:new")]]

    for row in rows:
        active = gift_status(row) == "active"
        label = (
            f"{'✅' if active else '❌'} "
            f"{report_text(row['code'], 36)}"
            f" | {row['used_count']}/{row['capacity']}"
        )

        buttons.append([
            B(
                label,
                f"gc:view:{row['id']}:{page}",
                "success" if active else "danger",
            )
        ])

    navigation = []

    if page > 0:
        navigation.append(B("⬅️ قبلی", f"gc:list:{page - 1}"))

    if page + 1 < pages:
        navigation.append(B("بعدی ➡️", f"gc:list:{page + 1}"))

    if navigation:
        buttons.append(navigation)

    buttons.extend([
        [B("🔄 به‌روزرسانی", f"gc:list:{page}")],
        [B("🎟 استفاده از کد هدیه", "gc:redeem")],
        [B("🏠 خانه", "main_menu")],
    ])

    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


def build_gift_detail(gift_id, page=0):
    row = read_gift(gift_id)

    if not row:
        return (
            "❌ کد هدیه پیدا نشد.",
            back_keyboard(f"gc:list:{page}"),
        )

    status = gift_status(row)
    expiration = (
        "نامحدود"
        if row["expires_at"] is None
        else format_report_time(row["expires_at"])
    )
    duration = (
        "نامحدود"
        if row["duration_hours"] is None
        else f"{int(row['duration_hours']):,} ساعت"
    )

    text = (
        "🎟 اطلاعات کد هدیه\n\n"
        f"کد: {row['code']}\n"
        f"وضعیت: {gift_status_label(status)}\n\n"
        f"💎 مبلغ کد برای هر نفر: {int(row['amount']):,} میو\n"
        f"{GIFT_CAP_NOTICE}\n\n"
        f"👥 ظرفیت: {int(row['capacity']):,} نفر\n"
        f"✅ دریافت‌کنندگان: {int(row['used_count']):,} نفر\n"
        f"📋 ظرفیت باقی‌مانده: "
        f"{max(0, int(row['capacity']) - int(row['used_count'])):,}\n\n"
        f"⏱ مدت اعتبار: {duration}\n"
        f"🕒 ثبت: {format_report_time(row['created_at'])}\n"
        f"🕒 پایان اعتبار: {expiration}"
    )

    buttons = [
        [
            B(
                f"👥 دریافت‌کنندگان ({row['used_count']})",
                f"gc:users:{gift_id}:0:{page}",
            )
        ],
        [B("🔄 به‌روزرسانی", f"gc:view:{gift_id}:{page}")],
    ]

    if row["deleted_at"] is None:
        buttons.append([
            B(
                "🗑 حذف کد هدیه",
                f"gc:delete:{gift_id}:{page}",
                "danger",
            )
        ])

    buttons.append([B("🔙 برگشت", f"gc:list:{page}")])
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


def build_gift_recipients(gift_id, member_page=0, page=0):
    gift = read_gift(gift_id)

    if not gift:
        return (
            "❌ کد هدیه پیدا نشد.",
            back_keyboard(f"gc:list:{page}"),
        )

    total = int(fetch_one("""
        SELECT COUNT(*) FROM mio_gift_redemptions
        WHERE gift_id=%s
    """, (gift_id,))[0])

    pages = max(1, math.ceil(total / GIFT_RECIPIENT_PAGE_SIZE))
    member_page = min(max(0, member_page), pages - 1)

    rows = fetch_dicts("""
        SELECT
            r.user_id, r.amount, r.redeemed_at, r.balance_after,
            CASE
                WHEN u.user_id IS NULL THEN r.username
                ELSE u.username
            END AS username,
            CASE
                WHEN u.user_id IS NULL THEN r.first_name
                ELSE u.first_name
            END AS first_name,
            u.balance,
            (u.user_id IS NOT NULL) AS registered
        FROM mio_gift_redemptions AS r
        LEFT JOIN users AS u ON u.user_id=r.user_id
        WHERE r.gift_id=%s
        ORDER BY r.redeemed_at ASC, r.user_id ASC
        LIMIT %s OFFSET %s
    """, (
        gift_id,
        GIFT_RECIPIENT_PAGE_SIZE,
        member_page * GIFT_RECIPIENT_PAGE_SIZE,
    ))

    lines = [
        "👥 دریافت‌کنندگان کد هدیه",
        f"کد: {gift['code']}",
        f"تعداد: {total:,}",
        f"صفحه {member_page + 1} از {pages}",
        "",
    ]

    if not rows:
        lines.append("هنوز کسی این کد را دریافت نکرده است.")

    for row in rows:
        balance_text = (
            f"{int(row['balance'] or 0):,} میو"
            if row["registered"]
            else "حساب در جدول کاربران موجود نیست"
        )

        lines.extend([
            f"👤 نام: {report_text(row['first_name'], 60)}",
            f"یوزرنیم: {username_text(row['username'])}",
            f"🆔 آیدی عددی: {row['user_id']}",
            f"💎 موجودی فعلی: {balance_text}",
            f"🎁 هدیهٔ واریزشده: {int(row['amount']):,} میو",
            f"🕒 {format_report_time(row['redeemed_at'])}",
            "──────────",
        ])

    buttons = []
    navigation = []

    if member_page > 0:
        navigation.append(B(
            "⬅️ قبلی",
            f"gc:users:{gift_id}:{member_page - 1}:{page}",
        ))

    if member_page + 1 < pages:
        navigation.append(B(
            "بعدی ➡️",
            f"gc:users:{gift_id}:{member_page + 1}:{page}",
        ))

    if navigation:
        buttons.append(navigation)

    buttons.extend([
        [
            B(
                "🔄 به‌روزرسانی",
                f"gc:users:{gift_id}:{member_page}:{page}",
            )
        ],
        [B("🔙 برگشت", f"gc:view:{gift_id}:{page}")],
    ])

    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


def build_gift_panel(kind, gift_id=0, page=0, member_page=0):
    if kind == "list":
        return build_gift_list(page)

    if kind == "view":
        return build_gift_detail(gift_id, page)

    if kind == "users":
        return build_gift_recipients(gift_id, member_page, page)

    raise ValueError("Invalid gift panel")


async def show_gift_panel(
    message,
    kind="list",
    gift_id=0,
    page=0,
    member_page=0,
    edit=False,
):
    global gift_panel_watch

    if message.chat.type != "private":
        return

    async with gift_panel_lock:
        text, keyboard = build_gift_panel(
            kind, gift_id, page, member_page,
        )

        if edit:
            sent = await edit_report_message(message, text, keyboard)
        else:
            sent = await message.answer(
                text,
                reply_markup=keyboard,
                parse_mode=None,
            )

        gift_panel_watch = {
            "message": sent,
            "kind": kind,
            "gift_id": gift_id,
            "page": page,
            "member_page": member_page,
            "signature": (text, keyboard.model_dump_json()),
        }


async def check_gift_panel():
    global gift_panel_watch

    while True:
        watch = None
        delay = GIFT_PANEL_REFRESH_INTERVAL

        try:
            async with gift_panel_lock:
                watch = gift_panel_watch

                if watch is not None:
                    text, keyboard = build_gift_panel(
                        watch["kind"],
                        watch["gift_id"],
                        watch["page"],
                        watch["member_page"],
                    )
                    signature = (text, keyboard.model_dump_json())

                    if signature != watch["signature"]:
                        sent = await edit_report_message(
                            watch["message"],
                            text,
                            keyboard,
                        )

                        if gift_panel_watch is watch:
                            watch["message"] = sent
                            watch["signature"] = signature

        except TelegramRetryAfter as error:
            delay = max(delay, int(error.retry_after) + 1)

        except (TelegramForbiddenError, TelegramBadRequest):
            if gift_panel_watch is watch:
                gift_panel_watch = None

        except Exception:
            rollback_db()
            logger.exception("به‌روزرسانی پنل هدیه ناموفق")
            delay = 30

        await asyncio.sleep(delay)


# ================= ADMIN REPORTS =================

def admin_report_keyboard(kind, page, pages, until):
    buttons = []
    navigation = []

    if page > 0:
        navigation.append(B(
            "⬅️ صفحه قبل", f"ap:{kind}:{page - 1}:{until}",
        ))

    if page + 1 < pages:
        navigation.append(B(
            "صفحه بعد ➡️", f"ap:{kind}:{page + 1}:{until}",
        ))

    if navigation:
        buttons.append(navigation)

    buttons.append([B("🔄 به‌روزرسانی", f"ap:{kind}:0:0")])

    if kind == "w":
        buttons.append([
            B("📚 آخرین برداشت‌ها؛ همه زمان‌ها", "ap:a:0:0")
        ])

    elif kind == "a":
        buttons.append([
            B("🕒 برداشت‌های ۲۴ ساعت اخیر", "ap:w:0:0")
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def read_users_page(page):
    total = int(fetch_one("SELECT COUNT(*) FROM users")[0])
    pages = max(1, math.ceil(total / USERS_PAGE_SIZE))
    page = min(max(0, page), pages - 1)

    rows = fetch_dicts("""
        SELECT
            u.user_id, u.username, u.first_name, u.balance,
            EXISTS(
                SELECT 1 FROM mio_user_bans AS b
                WHERE b.user_id=u.user_id AND b.active=TRUE
            ) AS is_banned
        FROM users AS u
        ORDER BY u.user_id ASC
        LIMIT %s OFFSET %s
    """, (USERS_PAGE_SIZE, page * USERS_PAGE_SIZE))

    return rows, total, page, pages


def read_withdraw_page(kind, page, until):
    if kind not in ("w", "a"):
        raise ValueError("Invalid report kind")

    if kind == "w" and not until:
        until = math.ceil(float(fetch_one("""
            SELECT EXTRACT(EPOCH FROM clock_timestamp())
        """)[0]))

    epoch_sql = """
        CASE
            WHEN q.created_time >= 100000000000
             AND q.created_time < 100000000000000
            THEN q.created_time / 1000.0
            ELSE q.created_time
        END
    """

    if kind == "w":
        condition = f"({epoch_sql}) >= %s AND ({epoch_sql}) <= %s"
        params = (until - 86400, until)

    else:
        condition = "TRUE"
        params = ()
        until = 0

    total = int(fetch_one(
        f"SELECT COUNT(*) FROM withdraw_queue AS q WHERE {condition}",
        params,
    )[0])

    pages = max(1, math.ceil(total / WITHDRAW_PAGE_SIZE))
    page = min(max(0, page), pages - 1)

    rows = fetch_dicts(
        f"""
            SELECT
                q.id AS request_id,
                q.user_id, q.target_username, q.amount,
                q.created_time, q.status, q.withdraw_type,
                u.username, u.first_name
            FROM withdraw_queue AS q
            LEFT JOIN users AS u ON u.user_id=q.user_id
            WHERE {condition}
            ORDER BY q.id DESC
            LIMIT %s OFFSET %s
        """,
        params + (WITHDRAW_PAGE_SIZE, page * WITHDRAW_PAGE_SIZE),
    )

    return rows, total, page, pages, until


async def build_admin_report(kind, page=0, until=0):
    if kind == "u":
        rows, total, page, pages = read_users_page(page)

        lines = [
            "👥 فهرست کاربران",
            f"تعداد کل: {total:,}",
            f"صفحه {page + 1} از {pages}",
            "",
        ]

        if not rows:
            lines.append("کاربری ثبت نشده است.")

        for row in rows:
            lines.extend([
                f"🆔 آیدی عددی: {row['user_id']}",
                f"👤 {username_text(row['username'])}",
                f"نام: {report_text(row['first_name'], 60)}",
                f"💎 موجودی: {int(row['balance'] or 0):,} میو",
                "🚫 وضعیت: مسدود" if row["is_banned"] else "✅ وضعیت: فعال",
                "──────────",
            ])

        lines.extend([
            "",
            "نام و یوزرنیم براساس آخرین اطلاعات ذخیره‌شده است.",
        ])

        return (
            "\n".join(lines),
            admin_report_keyboard(kind, page, pages, 0),
        )

    rows, total, page, pages, until = read_withdraw_page(
        kind, page, until,
    )

    lines = [
        (
            "📤 برداشت‌های ۲۴ ساعت اخیر"
            if kind == "w"
            else "📚 آخرین برداشت‌ها؛ همه زمان‌ها"
        ),
        f"تعداد: {total:,}",
        f"صفحه {page + 1} از {pages}",
        "",
    ]

    if not rows:
        lines.append("برداشتی در این گزارش پیدا نشد.")

        if kind == "w":
            lines.append(
                "برای رکوردهای قدیمی یا بدون زمان، «همه زمان‌ها» را بزن."
            )

    for row in rows:
        status_text = {
            0: "⏳ در صف",
            1: "🔄 در حال پردازش",
            2: "✅ انجام شده",
            3: "❌ شکست",
            4: "❌ ناموفق یا لغوشده؛ بررسی شده",
        }.get(row["status"], f"نامشخص ({row['status']})")

        method = {
            "card": "بانک میویی",
            "id": "آیدی",
            "group": "گپی",
        }.get(row["withdraw_type"], "نامشخص")

        lines.extend([
            f"🧾 درخواست: {row['request_id']}",
            f"🆔 کاربر: {row['user_id']}",
            f"👤 {username_text(row['username'])}",
            f"روش: {method}",
            "📤 مقصد: " + report_text(row["target_username"] or "گپی", 80),
            f"💎 مقدار: {int(row['amount'] or 0):,} میو",
            f"🕒 {format_report_time(row['created_time'])}",
            f"📌 {status_text}",
            "──────────",
        ])

    return (
        "\n".join(lines),
        admin_report_keyboard(kind, page, pages, until),
    )


async def show_admin_report(
    message, kind, page=0, until=0, edit=False,
):
    if message.chat.type != "private":
        await message.answer("این دستور را در خصوصی ربات بفرست.")
        return

    target = message

    try:
        if not edit:
            target = await message.answer("⏳ در حال دریافت گزارش…")

        text, keyboard = await build_admin_report(kind, page, until)
        await edit_report_message(target, text, keyboard)

    except Exception:
        rollback_db()
        logger.exception("گزارش ادمین ناموفق؛ kind=%s", kind)

        try:
            await edit_report_message(
                target,
                "❌ دریافت گزارش انجام نشد؛ دوباره تلاش کن.",
                K([B("🔄 تلاش دوباره", f"ap:{kind}:0:0")]),
            )

        except Exception:
            logger.exception("ارسال خطای گزارش ناموفق")


# ================= FAST SUSPICIOUS REPORT =================

def get_suspicious_groups(until_id=0):
    now = time.monotonic()

    if (
        until_id
        and suspicious_cache["until_id"] == until_id
        and now - suspicious_cache["loaded_at"] < SUSPICIOUS_CACHE_SECONDS
    ):
        return suspicious_cache["groups"], until_id

    if not until_id:
        until_id = int(fetch_one("""
            SELECT COALESCE(MAX(id), 0) FROM withdraw_queue
        """)[0])

    groups = fetch_dicts("""
        SELECT
            q.withdraw_type AS method,
            mio_withdraw_destination(
                q.withdraw_type, q.target_username
            ) AS destination,
            COUNT(DISTINCT q.user_id) AS member_count,
            COUNT(*) AS request_count,
            MAX(q.id) AS latest_id
        FROM withdraw_queue AS q
        WHERE q.id <= %s
          AND q.user_id > 0
          AND q.withdraw_type IN ('id', 'card')
          AND mio_withdraw_destination(
              q.withdraw_type, q.target_username
          ) IS NOT NULL
        GROUP BY
            q.withdraw_type,
            mio_withdraw_destination(
                q.withdraw_type, q.target_username
            )
        HAVING COUNT(DISTINCT q.user_id) >= 2
        ORDER BY latest_id DESC, method ASC, destination ASC
    """, (until_id,))

    suspicious_cache.update({
        "until_id": until_id,
        "groups": groups,
        "loaded_at": time.monotonic(),
    })

    return groups, until_id


def read_suspicious_page(group_page=0, member_page=0, until_id=0):
    groups, until_id = get_suspicious_groups(until_id)
    group_count = len(groups)

    if not groups:
        return {
            "group": None,
            "rows": [],
            "group_page": 0,
            "group_count": 0,
            "member_page": 0,
            "member_pages": 1,
            "until_id": until_id,
        }

    group_page = min(max(0, group_page), group_count - 1)
    group = groups[group_page]

    member_pages = max(
        1,
        math.ceil(
            int(group["member_count"]) / SUSPICIOUS_MEMBERS_PAGE_SIZE
        ),
    )
    member_page = min(max(0, member_page), member_pages - 1)

    rows = fetch_dicts("""
        SELECT
            members.*,
            u.username, u.first_name, u.balance, u.daily_mio,
            (u.user_id IS NOT NULL) AS registered,
            EXISTS(
                SELECT 1 FROM mio_user_bans AS b
                WHERE b.user_id=members.user_id AND b.active=TRUE
            ) AS is_banned
        FROM (
            SELECT
                q.user_id,
                COUNT(*) AS request_count,
                COALESCE(SUM(COALESCE(q.amount, 0)), 0)
                    AS requested_amount,
                COUNT(*) FILTER (WHERE q.status=2)
                    AS success_count,
                COALESCE(
                    SUM(COALESCE(q.amount, 0))
                    FILTER (WHERE q.status=2), 0
                ) AS success_amount,
                MAX(
                    CASE
                        WHEN q.created_time >= 100000000000
                         AND q.created_time < 100000000000000
                        THEN q.created_time / 1000.0
                        WHEN q.created_time > 0
                         AND q.created_time < 100000000000
                        THEN q.created_time
                        ELSE NULL
                    END
                ) AS last_request_at
            FROM withdraw_queue AS q
            WHERE q.id <= %s
              AND q.user_id > 0
              AND q.withdraw_type IN ('id', 'card')
              AND q.withdraw_type=%s
              AND mio_withdraw_destination(
                  q.withdraw_type, q.target_username
              )=%s
            GROUP BY q.user_id
        ) AS members
        LEFT JOIN users AS u ON u.user_id=members.user_id
        ORDER BY members.user_id ASC
        LIMIT %s OFFSET %s
    """, (
        until_id,
        group["method"],
        group["destination"],
        SUSPICIOUS_MEMBERS_PAGE_SIZE,
        member_page * SUSPICIOUS_MEMBERS_PAGE_SIZE,
    ))

    return {
        "group": group,
        "rows": rows,
        "group_page": group_page,
        "group_count": group_count,
        "member_page": member_page,
        "member_pages": member_pages,
        "until_id": until_id,
    }


def suspicious_report_keyboard(report):
    buttons = []
    gp = report["group_page"]
    mp = report["member_page"]
    until = report["until_id"]
    user_buttons = []

    for row in report["rows"]:
        if row["registered"]:
            label = (
                username_text(row["username"])
                if row["username"]
                else str(row["user_id"])
            )
            user_buttons.append(
                B(f"👤 {label}", f"ub:view:{row['user_id']}")
            )

    for offset in range(0, len(user_buttons), 2):
        buttons.append(user_buttons[offset:offset + 2])

    navigation = []

    if mp > 0:
        navigation.append(B("⬅️ اعضای قبلی", f"sp:{gp}:{mp - 1}:{until}"))

    if mp + 1 < report["member_pages"]:
        navigation.append(B("اعضای بعدی ➡️", f"sp:{gp}:{mp + 1}:{until}"))

    if navigation:
        buttons.append(navigation)

    navigation = []

    if gp > 0:
        navigation.append(B("⬅️ مقصد قبلی", f"sp:{gp - 1}:0:{until}"))

    if gp + 1 < report["group_count"]:
        navigation.append(B("مقصد بعدی ➡️", f"sp:{gp + 1}:0:{until}"))

    if navigation:
        buttons.append(navigation)

    buttons.append([B("🔄 بررسی دوبارهٔ مشکوک‌ها", "sp:0:0:0")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def build_suspicious_report(
    group_page=0, member_page=0, until_id=0,
):
    report = read_suspicious_page(group_page, member_page, until_id)
    group = report["group"]

    if group is None:
        return (
            "🔎 گزارش مشکوک‌ها\n\n"
            "مقصد مشترکی برای حداقل دو آیدی عددی متفاوت پیدا نشد.\n"
            "آیدی و کارت میویی جداگانه بررسی می‌شوند.",
            suspicious_report_keyboard(report),
        )

    destination = (
        group["destination"]
        if group["method"] == "card"
        else "@" + group["destination"]
    )
    method = "🏦 بانک میویی" if group["method"] == "card" else "📤 آیدی"

    lines = [
        "🔎 گزارش مشکوک‌ها — مقصد مشترک",
        "",
        f"مقصد {report['group_page'] + 1} از {report['group_count']}",
        f"روش: {method}",
        f"🎯 مقصد: {destination}",
        f"👥 حساب‌های متفاوت: {int(group['member_count']):,}",
        f"🧾 درخواست‌ها: {int(group['request_count']):,}",
        f"صفحهٔ اعضا: {report['member_page'] + 1} از {report['member_pages']}",
        "",
        "شامل درخواست‌های موفق، ناموفق و لغوشده در همهٔ زمان‌ها.",
        "اشتراک مقصد به‌تنهایی اثبات تخلف نیست.",
        "━━━━━━━━━━",
    ]

    first_number = report["member_page"] * SUSPICIOUS_MEMBERS_PAGE_SIZE + 1

    for number, row in enumerate(report["rows"], start=first_number):
        balance_text = (
            f"{int(row['balance'] or 0):,} میو"
            if row["registered"]
            else "حساب در جدول کاربران موجود نیست"
        )
        daily_text = (
            ("فعال" if row["daily_mio"] == 1 else "غیرفعال")
            if row["registered"]
            else "نامشخص"
        )

        lines.extend([
            f"👤 حساب {number}",
            f"نام: {report_text(row['first_name'], 60)}",
            f"یوزرنیم: {username_text(row['username'])}",
            f"🆔 آیدی عددی: {row['user_id']}",
            f"💎 موجودی: {balance_text}",
            "🚫 وضعیت: مسدود" if row["is_banned"] else "وضعیت بن: مسدود نیست",
            f"🎁 روزانه: {daily_text}",
            f"🧾 درخواست: {int(row['request_count']):,}",
            f"📥 مجموع درخواستی: {int(row['requested_amount']):,} میو",
            f"✅ موفق: {int(row['success_count']):,} مورد",
            f"💸 مجموع موفق: {int(row['success_amount']):,} میو",
            f"🕒 آخرین درخواست: {format_report_time(row['last_request_at'])}",
            "──────────",
        ])

    lines.extend([
        "",
        "برای مشاهدهٔ اطلاعات و مدیریت کاربر، دکمهٔ کاربر را بزن.",
        "نام و یوزرنیم براساس آخرین اطلاعات ذخیره‌شده است.",
    ])

    return "\n".join(lines), suspicious_report_keyboard(report)


async def show_suspicious_report(
    message, group_page=0, member_page=0, until_id=0, edit=False,
):
    if message.chat.type != "private":
        await message.answer("گزارش فقط در خصوصی ربات نمایش داده می‌شود.")
        return

    target = message

    try:
        if not edit:
            target = await message.answer("⏳ در حال بررسی مقصدهای مشترک…")

        text, keyboard = await build_suspicious_report(
            group_page, member_page, until_id,
        )
        await edit_report_message(target, text, keyboard)

    except Exception:
        rollback_db()
        logger.exception("گزارش مشکوک‌ها ناموفق")

        try:
            await edit_report_message(
                target,
                "❌ بررسی انجام نشد؛ دوباره تلاش کن.",
                K([B("🔄 تلاش دوباره", "sp:0:0:0")]),
            )

        except Exception:
            logger.exception("ارسال خطای مشکوک‌ها ناموفق")


# ================= ADMIN SEARCH =================

def read_registered_user(user_id):
    rows = fetch_dicts("""
        SELECT user_id, username, first_name, balance, daily_mio
        FROM users
        WHERE user_id=%s
    """, (user_id,))
    return rows[0] if rows else None


def admin_user_card(row, banned):
    lines = [
        "👤 اطلاعات کاربر",
        "",
        f"نام: {report_text(row['first_name'], 100)}",
        f"یوزرنیم: {username_text(row['username'])}",
        f"🆔 آیدی عددی: {row['user_id']}",
        f"💎 موجودی: {int(row['balance'] or 0):,} میو",
        (
            "🎁 میو روزانه: فعال"
            if row["daily_mio"] == 1
            else "🎁 میو روزانه: غیرفعال"
        ),
        "",
    ]
    buttons = []

    if banned:
        lines.extend([
            "🚫 وضعیت: مسدود",
            f"🕒 زمان بن: {format_report_time(banned['banned_at'])}",
            "",
            "دلیل بن:",
            banned["reason"],
        ])

        if row["user_id"] != ADMIN_USER_ID:
            buttons.append([
                B("✅ آن‌بن / رفع مسدودی", f"ub:unbanask:{row['user_id']}")
            ])

    else:
        lines.append("✅ وضعیت: فعال")

        if row["user_id"] != ADMIN_USER_ID:
            buttons.append([
                B("🚫 بن از ربات / مسدود کردن", f"ub:ask:{row['user_id']}")
            ])

        # فقط وقتی روزانه خاموش باشد و کاربر بن نباشد.
        if row["daily_mio"] != 1:
            buttons.append([
                B(
                    "🎁 روشن کردن میو روزانه",
                    f"ud:on:{row['user_id']}",
                )
            ])

    buttons.append([B("🔙 برگشت", "ub:back:0")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


async def show_admin_user(
    message,
    user_id,
    edit=False,
    refresh_profile=True,
):
    row = read_registered_user(user_id)

    if row and refresh_profile:
        await refresh_page_profiles([row])
        row = read_registered_user(user_id)

    if row:
        text, keyboard = admin_user_card(row, get_user_ban(user_id))
    else:
        text = "❌ این کاربر در دیتابیس ثبت نشده است."
        keyboard = back_keyboard("ub:back:0")

    if edit:
        await edit_report_message(message, text, keyboard)
    else:
        await message.answer(
            text,
            reply_markup=keyboard,
            parse_mode=None,
        )


async def find_admin_user(message, target):
    if target.startswith("@"):
        username = target[1:].lower()

        rows = fetch_dicts("""
            SELECT user_id, username, first_name, balance, daily_mio
            FROM users
            WHERE LOWER(LTRIM(username, '@'))=%s
            ORDER BY user_id ASC
            LIMIT 11
        """, (username,))

        await refresh_page_profiles(rows)

        rows = [
            row for row in rows
            if str(row["username"] or "").lstrip("@").lower() == username
        ]

    else:
        row = read_registered_user(int(target))
        rows = [row] if row else []

    if not rows:
        await message.answer(
            "❌ کاربر در دیتابیس پیدا نشد.\n\n"
            "جست‌وجو بین کاربران همین ربات است.\n"
            "اگر یوزرنیم تغییر کرده، با آیدی عددی جست‌وجو کن."
        )
        return

    if len(rows) > 1:
        lines = [
            "چند رکورد با این یوزرنیم ذخیره شده است.",
            "با آیدی عددی جست‌وجو کن:",
            "",
        ]
        lines.extend(
            f"{row['user_id']} — {report_text(row['first_name'], 60)}"
            for row in rows[:10]
        )
        lines.extend(["", "مثال:", "828292228 پیداش کن"])
        await message.answer("\n".join(lines), parse_mode=None)
        return

    await show_admin_user(message, rows[0]["user_id"])


# ================= BROADCAST REPORT =================

async def send_admin_text(message, text):
    try:
        await message.answer(text, parse_mode=None)

    except TelegramRetryAfter as error:
        await asyncio.sleep(int(error.retry_after) + 1)
        await message.answer(text, parse_mode=None)


async def report_broadcast_result(message, sent, failed_users, skipped=0):
    text = (
        "✅ ارسال پیام همگانی پایان یافت\n\n"
        f"📤 موفق: {sent}\n"
        f"❌ ناموفق: {len(failed_users)}"
    )

    if skipped:
        text += f"\n🚫 ارسال‌نشده به علت بن: {skipped}"

    await send_admin_text(message, text)

    if not failed_users:
        return

    latest = {}

    try:
        latest = dict(fetch_all("""
            SELECT user_id, username FROM users
            WHERE user_id=ANY(%s)
        """, ([uid for uid, _ in failed_users],)))

    except Exception:
        rollback_db()
        logger.exception("خواندن یوزرنیم ارسال‌های ناموفق انجام نشد")

    pages = math.ceil(len(failed_users) / BROADCAST_FAILED_PAGE_SIZE)

    for offset in range(0, len(failed_users), BROADCAST_FAILED_PAGE_SIZE):
        page = offset // BROADCAST_FAILED_PAGE_SIZE + 1
        lines = [
            "❌ ارسال پیام به این کاربران ناموفق بود:",
            f"صفحه {page} از {pages}",
            "",
        ]

        for uid, saved in failed_users[offset:offset + BROADCAST_FAILED_PAGE_SIZE]:
            username = latest.get(uid, saved)
            lines.append(
                username_text(username)
                if username
                else f"بدون یوزرنیم — آیدی عددی: {uid}"
            )

        await asyncio.sleep(1.1)
        await send_admin_text(message, "\n".join(lines))


# ================= QUEUE DATABASE =================

def get_pending_withdraw(user_id):
    return fetch_one("""
        SELECT id, amount, withdraw_type, status
        FROM withdraw_queue
        WHERE user_id=%s AND status IN (0, 1)
        ORDER BY id ASC LIMIT 1
    """, (user_id,))


def add_withdraw_queue(user_id, target_username, withdraw_type):
    try:
        db_execute("""
            SELECT balance FROM users
            WHERE user_id=%s FOR UPDATE
        """, (user_id,))
        user = cursor.fetchone()

        if not user:
            db.commit()
            return None, False

        db_execute("""
            SELECT 1 FROM mio_user_bans
            WHERE user_id=%s AND active=TRUE
        """, (user_id,))

        if cursor.fetchone():
            raise UserBannedError()

        db_execute("""
            SELECT id FROM withdraw_queue
            WHERE user_id=%s AND status IN (0, 1)
            ORDER BY id ASC LIMIT 1
        """, (user_id,))
        pending = cursor.fetchone()

        if pending:
            db.commit()
            return pending[0], False

        amount = int(user[0] or 0)

        if amount <= 0:
            db.commit()
            return None, False

        db_execute("""
            INSERT INTO withdraw_queue(
                user_id, target_username, amount,
                withdraw_type, status, created_time
            )
            VALUES (
                %s, %s, %s, %s, 0,
                EXTRACT(EPOCH FROM clock_timestamp())
            )
            RETURNING id
        """, (user_id, target_username, amount, withdraw_type))

        request_id = cursor.fetchone()[0]
        db.commit()
        return request_id, True

    except Exception:
        rollback_db()
        raise


def request_queue_update(user_id):
    db_execute("""
        UPDATE bot_withdraw_queue_messages
        SET retry_at=0
        WHERE user_id=%s AND finished=FALSE
    """, (user_id,))
    db.commit()
    queue_wakeup.set()


def get_queue_candidates():
    rows = fetch_all("""
        SELECT q.id
        FROM withdraw_queue AS q
        LEFT JOIN bot_withdraw_queue_messages AS n ON n.request_id=q.id
        WHERE (
            q.status IN (0, 1)
            OR (
                n.request_id IS NOT NULL
                AND n.finished=FALSE
                AND q.status IN (2, 3, 4)
            )
        )
        AND NOT EXISTS (
            SELECT 1 FROM mio_user_bans AS b
            WHERE b.user_id=q.user_id AND b.active=TRUE
        )
        AND COALESCE(n.retry_at, 0)
            <= EXTRACT(EPOCH FROM clock_timestamp())
        ORDER BY COALESCE(n.retry_at, 0), q.id
        LIMIT 100
    """)
    return [row[0] for row in rows]


def get_queue_info(request_id):
    db_execute("""
        SELECT
            q.id AS request_id,
            q.user_id, q.target_username, q.amount,
            q.withdraw_type, q.status,
            EXTRACT(EPOCH FROM clock_timestamp()) AS server_now,
            COALESCE(
                own_limit.delay_seconds,
                CASE WHEN q.withdraw_type='card' THEN 300 ELSE 35 END
            ) AS delay_seconds,
            COALESCE(own_limit.next_allowed_at, 0) AS own_ready_at,
            COALESCE(normal_limit.next_allowed_at, 0) AS normal_ready_at,
            counts.waiting_ahead,
            counts.processing_ahead,
            counts.queue_total,
            EXISTS(
                SELECT 1 FROM withdraw_queue AS active
                WHERE active.status=1 AND active.id <> q.id
            ) AS another_processing,
            n.telegram_message_id,
            COALESCE(n.last_text, '') AS last_text,
            n.last_status,
            COALESCE(n.finished, FALSE) AS finished,
            COALESCE(n.retry_at, 0) AS retry_at
        FROM withdraw_queue AS q
        LEFT JOIN withdraw_transfer_limits AS own_limit
          ON own_limit.method=(
              CASE WHEN q.withdraw_type='card' THEN 'card' ELSE 'normal' END
          )
        LEFT JOIN withdraw_transfer_limits AS normal_limit
          ON normal_limit.method='normal'
        LEFT JOIN bot_withdraw_queue_messages AS n ON n.request_id=q.id
        CROSS JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (
                    WHERE other.status=0 AND other.id < q.id
                ) AS waiting_ahead,
                COUNT(*) FILTER (
                    WHERE other.status=1
                      AND other.id <> q.id
                      AND (q.status=0 OR other.id < q.id)
                ) AS processing_ahead,
                COUNT(*) AS queue_total
            FROM withdraw_queue AS other
            WHERE other.status IN (0, 1)
              AND (
                  CASE WHEN other.withdraw_type='card'
                       THEN 'card' ELSE 'normal' END
              ) = (
                  CASE WHEN q.withdraw_type='card'
                       THEN 'card' ELSE 'normal' END
              )
        ) AS counts
        WHERE q.id=%s
    """, (request_id,))

    info = current_cursor_dict()
    db.commit()

    if info is None:
        return None

    for field in (
        "amount", "status", "delay_seconds",
        "waiting_ahead", "processing_ahead", "queue_total",
    ):
        info[field] = int(info[field] or 0)

    for field in (
        "server_now", "own_ready_at", "normal_ready_at", "retry_at",
    ):
        info[field] = float(info[field] or 0)

    if info["status"] == 0:
        info["queue_position"] = (
            info["waiting_ahead"] + info["processing_ahead"] + 1
        )

    elif info["status"] == 1:
        info["queue_position"] = info["processing_ahead"] + 1

    else:
        info["queue_position"] = None

    ready_at = info["own_ready_at"]

    if info["withdraw_type"] == "card":
        ready_at = max(ready_at, info["normal_ready_at"])

    cooldown = max(0, math.ceil(ready_at - info["server_now"]))

    info["wait_seconds"] = (
        cooldown + info["waiting_ahead"] * info["delay_seconds"]
        if info["status"] == 0
        else 0
    )

    return info


def ensure_queue_message_record(info):
    db_execute("""
        INSERT INTO bot_withdraw_queue_messages(request_id, user_id)
        VALUES (%s, %s)
        ON CONFLICT (request_id) DO NOTHING
    """, (info["request_id"], info["user_id"]))
    db.commit()


def save_queue_message(info, message_id, text):
    db_execute("""
        INSERT INTO bot_withdraw_queue_messages(
            request_id, user_id, telegram_message_id,
            last_text, last_status, finished, retry_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            EXTRACT(EPOCH FROM clock_timestamp()) + %s,
            EXTRACT(EPOCH FROM clock_timestamp())
        )
        ON CONFLICT (request_id) DO UPDATE SET
            user_id=EXCLUDED.user_id,
            telegram_message_id=EXCLUDED.telegram_message_id,
            last_text=EXCLUDED.last_text,
            last_status=EXCLUDED.last_status,
            finished=EXCLUDED.finished,
            retry_at=EXCLUDED.retry_at,
            updated_at=EXCLUDED.updated_at
    """, (
        info["request_id"],
        info["user_id"],
        message_id,
        text,
        info["status"],
        info["status"] in (2, 3, 4),
        QUEUE_UPDATE_INTERVAL,
    ))
    db.commit()


def postpone_queue_message(request_id, seconds):
    db_execute("""
        UPDATE bot_withdraw_queue_messages
        SET retry_at=EXTRACT(EPOCH FROM clock_timestamp()) + %s
        WHERE request_id=%s
    """, (max(1, math.ceil(seconds)), request_id))
    db.commit()


def finish_missing_queue_message(request_id, status):
    db_execute("""
        UPDATE bot_withdraw_queue_messages
        SET
            finished=TRUE,
            last_status=%s,
            updated_at=EXTRACT(EPOCH FROM clock_timestamp())
        WHERE request_id=%s
    """, (status, request_id))
    db.commit()


# ================= QUEUE TEXT / MESSAGES =================

def get_wait_text(info):
    if info["status"] == 1:
        return "برداشت شما اکنون در حال پردازش است."

    if info["wait_seconds"] > 0:
        return (
            f"حدود {human_duration(info['wait_seconds'])} "
            "تا شروع پردازش"
        )

    if info["another_processing"]:
        return (
            "در انتظار پایان برداشت در حال پردازش؛ "
            "زمان دقیق شروع هنوز مشخص نیست."
        )

    return "نوبت شما نزدیک است؛ در انتظار شروع پردازش."


def queue_message_text(info):
    status = info["status"]

    method = {
        "card": "🏦 برداشت با بانک میویی",
        "id": "📤 برداشت با آیدی",
        "group": "👥 برداشت گپی",
    }.get(info["withdraw_type"], "📤 برداشت میو")

    lines = [
        method,
        "",
        f"🧾 شماره درخواست: {info['request_id']}",
        f"💎 مقدار: {info['amount']:,} میو",
    ]

    target = info["target_username"] or ""

    if target and info["withdraw_type"] == "card":
        lines.append(f"🏦 شماره کارت: {target}")

    elif target and info["withdraw_type"] == "id":
        lines.append(f"👤 مقصد: {target}")

    lines.append("")

    if status == 2:
        lines.extend([
            "✅ برداشت با موفقیت ثبت شد.",
            "این درخواست از صف خارج شده است.",
        ])

    elif status in (3, 4):
        lines.extend([
            "❌ برداشت ناموفق بود.",
            "اطلاعات مقصد را بررسی کنید و دوباره تلاش کنید.",
        ])

    elif status == 1:
        lines.extend([
            f"👤 نوبت فعلی شما: {persian_number(info['queue_position'])}",
            "",
            "🔄 نوبت شما رسیده؛ برداشت در حال پردازش است.",
            "نتیجه پس از پایان پردازش اعلام می‌شود.",
        ])

    elif status == 0:
        queue_name = (
            "برداشت بانکی"
            if info["withdraw_type"] == "card"
            else "برداشت آیدی و گپی"
        )

        lines.extend([
            f"👤 شما نفر {persian_number(info['queue_position'])} در صف هستید.",
            "",
            f"📋 صف: {queue_name}",
            f"⏳ درخواست‌های منتظر جلوتر: {persian_number(info['waiting_ahead'])}",
            f"🔄 در حال پردازش جلوتر: {persian_number(info['processing_ahead'])}",
            f"👥 کل درخواست‌های فعال: {persian_number(info['queue_total'])}",
            "",
            "⏱ زمان تقریبی:",
            get_wait_text(info),
            "",
            "شماره نوبت در همین پیام به‌روزرسانی می‌شود.",
            "زمان تخمینی است و ممکن است تغییر کند.",
        ])

    else:
        lines.append("وضعیت درخواست نیاز به بررسی دارد.")

    return "\n".join(lines)


async def sync_queue_message(request_id, force=False):
    async with queue_message_lock:
        return await _sync_queue_message(request_id, force)


async def _sync_queue_message(request_id, force=False):
    global queue_telegram_pause_until

    info = get_queue_info(request_id)

    if not info:
        return False

    if is_user_banned(info["user_id"]):
        finish_missing_queue_message(request_id, info["status"])
        return False

    ensure_queue_message_record(info)

    if not force and info["retry_at"] > info["server_now"]:
        return False

    text = queue_message_text(info)
    message_id = info["telegram_message_id"]
    terminal = info["status"] in (2, 3, 4)

    if terminal and not message_id and not force:
        finish_missing_queue_message(request_id, info["status"])
        return False

    if message_id and text == info["last_text"]:
        save_queue_message(info, message_id, text)
        return True

    pause_remaining = queue_telegram_pause_until - time.monotonic()

    if pause_remaining > 0:
        postpone_queue_message(request_id, pause_remaining)
        return False

    try:
        if message_id:
            try:
                await bot.edit_message_text(
                    chat_id=info["user_id"],
                    message_id=message_id,
                    text=text,
                )

            except TelegramBadRequest as error:
                description = str(error).lower()

                if "message is not modified" in description:
                    pass

                elif (
                    "message to edit not found" in description
                    or "message can't be edited" in description
                    or "message can not be edited" in description
                ):
                    if terminal and not force:
                        finish_missing_queue_message(request_id, info["status"])
                        return False

                    if is_user_banned(info["user_id"]):
                        return False

                    sent = await bot.send_message(info["user_id"], text)
                    message_id = sent.message_id

                else:
                    raise

        else:
            sent = await bot.send_message(info["user_id"], text)
            message_id = sent.message_id

        save_queue_message(info, message_id, text)
        return True

    except TelegramRetryAfter as error:
        delay = int(error.retry_after) + 1
        queue_telegram_pause_until = max(
            queue_telegram_pause_until,
            time.monotonic() + delay,
        )
        postpone_queue_message(request_id, delay)

    except TelegramForbiddenError:
        if terminal:
            finish_missing_queue_message(request_id, info["status"])
        else:
            postpone_queue_message(request_id, QUEUE_FORBIDDEN_RETRY)

    except TelegramBadRequest as error:
        description = str(error).lower()
        retry = (
            QUEUE_FORBIDDEN_RETRY
            if (
                "chat not found" in description
                or "user is deactivated" in description
            )
            else QUEUE_ERROR_RETRY
        )
        postpone_queue_message(request_id, retry)
        logger.warning(
            "خطای پیام صف؛ request=%s error=%s", request_id, error,
        )

    return False


async def show_request_queue_now(request_id):
    try:
        return await sync_queue_message(request_id, force=True)

    except Exception:
        rollback_db()
        logger.exception("نمایش فوری صف ناموفق؛ request=%s", request_id)
        return False

    finally:
        queue_wakeup.set()


async def check_withdraw_queue():
    while True:
        queue_wakeup.clear()

        try:
            if time.monotonic() >= queue_telegram_pause_until:
                for request_id in get_queue_candidates():
                    if time.monotonic() < queue_telegram_pause_until:
                        break

                    try:
                        await sync_queue_message(request_id)

                    except Exception:
                        rollback_db()
                        logger.exception(
                            "بررسی صف ناموفق؛ request=%s", request_id,
                        )

                        try:
                            postpone_queue_message(request_id, QUEUE_ERROR_RETRY)
                        except Exception:
                            rollback_db()

                    await asyncio.sleep(1.1)

        except Exception:
            rollback_db()
            logger.exception("خطا در حلقهٔ صف")

        try:
            await asyncio.wait_for(
                queue_wakeup.wait(),
                timeout=QUEUE_POLL_INTERVAL,
            )
        except asyncio.TimeoutError:
            pass


# ================= HOME =================

async def show_menu(message, user_id=None):
    if user_id is None:
        user_id = message.from_user.id

    row = fetch_one("""
        SELECT first_name, balance FROM users WHERE user_id=%s
    """, (user_id,))

    if row:
        first_name = row[0] or "کاربر عزیز"
        amount = int(row[1] or 0)

    else:
        sender = getattr(message, "from_user", None)
        first_name = (
            sender.first_name
            if sender and sender.id == user_id
            else "کاربر عزیز"
        )
        amount = 0

    name = report_text(first_name, 100)

    introduction = (
        f"سلام {name} به ربات Free meowie خوش آمدید❤️\n\n"
        "در این ربات می‌تونید رایگان میو بگیرید "
        "و میو روزانه دریافت کنید!\n\n"
        "این ربات برای کسایی که تازه ربات میو رو شروع کردن "
        "مناسبه تا به ارتقای کارخونهٔ پیشی‌شون کمک کنه!\n\n"
        "کانال اطلاع‌رسانی و کد هدیه:\n"
        f"{CHANNEL_URL}\n\n"
    )

    balance_block = (
        "💎 موجودی فعلی شما:\n\n"
        f"{amount:,} میو"
    )

    await message.answer(
        introduction + balance_block + "\n\nانتخاب کنید:",
        entities=[
            MessageEntity(
                type="bold",
                offset=utf16_length(introduction),
                length=utf16_length(balance_block),
            )
        ],
        parse_mode=None,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=menu_keyboard(user_id),
    )


# ================= ADMIN PRIORITY COMMANDS =================

@dp.message(
    F.from_user.id == ADMIN_USER_ID,
    lambda m: admin_command_key(m.text) is not None,
)
async def admin_priority_commands(message: types.Message):
    global admin_broadcast
    global premium_last_error

    if message.chat.type != "private":
        await message.answer("دستورهای مدیریت را در خصوصی ربات بفرست.")
        return

    key = admin_command_key(message.text)
    target = parse_find_user_command(message.text)
    clear_input_state(message.from_user.id)

    if key == "find_user":
        try:
            await find_admin_user(message, target)

        except Exception:
            rollback_db()
            logger.exception("جست‌وجو ناموفق")
            await message.answer("❌ جست‌وجو انجام نشد؛ دوباره تلاش کن.")

        return

    if key == "users":
        await show_admin_report(message, "u")

    elif key == "withdrawals":
        await show_admin_report(message, "w")

    elif key == "suspicious":
        await show_suspicious_report(message)

    elif key == "gifts":
        await show_gift_panel(message)

    elif key == "packs":
        await show_pack_panel(message)

    elif key in ("emoji_status", "emoji_reload"):
        if key == "emoji_reload":
            await message.answer("⏳ در حال دریافت ایموجی‌ها…")
            premium_last_error = ""
            await refresh_premium_emojis()

        await message.answer(
            premium_status_text(),
            reply_markup=premium_preview_keyboard(),
            parse_mode=None,
        )

    elif key == "broadcast":
        admin_broadcast = True
        await message.answer(
            "✉️ پیام خود را وارد کنید:\n\n"
            "برای انصراف «لغو» را بفرست."
        )

    elif key == "daily_all":
        await message.answer("⏳ در حال واریز میو روزانه…")
        count = await give_daily_mio()
        await message.answer(
            f"✅ میو روزانه پردازش شد.\n\n👥 تعداد: {count}"
        )

    elif key == "cancel":
        await message.answer("✅ عملیات ورودی لغو شد.")
        await show_menu(message, message.from_user.id)


# ================= EMOJI PACK BUTTONS =================

@dp.callback_query(F.data.startswith("ep:"))
async def emoji_pack_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer("این بخش فقط در خصوصی ربات است.", show_alert=True)
        return

    if call.data not in (
        "ep:panel",
        "ep:add",
        "ep:delete",
        "ep:reset",
    ):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    clear_input_state(ADMIN_USER_ID)

    if call.data == "ep:add" and PREMIUM_PACKS:
        await call.answer(
            "اول «حذف پک فعلی» را بزن.",
            show_alert=True,
        )
        await show_pack_panel(call.message, edit=True)
        return

    await call.answer()

    if call.data == "ep:panel":
        await show_pack_panel(call.message, edit=True)
        return

    if call.data == "ep:add":
        admin_pack_input[ADMIN_USER_ID] = secrets.token_hex(12)

        await edit_report_message(
            call.message,
            "🎨 اضافه کردن پک ایموجی\n\n"
            "لینک پک ایموجی را بفرست:\n\n"
            "https://t.me/addemoji/MeowieQ\n\n"
            "لینک باید مربوط به پک ایموجی سفارشی تلگرام باشد.\n"
            "برای انصراف «لغو» را بفرست یا دکمهٔ برگشت را بزن.",
            K([B("🔙 برگشت به پنل پک", "ep:panel")]),
        )
        return

    try:
        if call.data == "ep:delete":
            await change_premium_packs((), ADMIN_USER_ID)
            await show_pack_panel(call.message, edit=True)
            return

        if call.data == "ep:reset":
            await edit_report_message(
                call.message,
                "⏳ در حال بازنشانی به پک‌های پیش‌فرض…",
                None,
            )

            await change_premium_packs(
                DEFAULT_PREMIUM_PACKS,
                ADMIN_USER_ID,
            )

            await show_pack_panel(call.message, edit=True)

    except ValueError as error:
        await call.message.answer(
            f"❌ تغییر پک انجام نشد.\n\n{error}",
            reply_markup=K([B("🔙 پنل پک", "ep:panel")]),
            parse_mode=None,
        )

    except Exception:
        rollback_db()
        logger.exception("تغییر تنظیم پک ناموفق")

        await call.message.answer(
            "❌ ثبت تنظیم پک انجام نشد؛ دوباره تلاش کن.",
            reply_markup=K([B("🔙 پنل پک", "ep:panel")]),
        )


@dp.callback_query(F.data.startswith("premium:"))
async def premium_admin_callback(call: CallbackQuery):
    global premium_last_error

    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer("این بخش فقط در خصوصی ربات است.", show_alert=True)
        return

    if call.data not in ("premium:preview", "premium:reload"):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    clear_input_state(ADMIN_USER_ID)

    if call.data == "premium:preview":
        await call.answer("دکمه فعال است.")
        await call.message.answer(
            "❤️ Free meowie\n\n"
            "💎 نمونهٔ ایموجی‌های پک\n\n"
            "✅ تأیید   ❌ لغو   🎁 هدیه\n"
            "📤 برداشت   🏦 بانک   👥 کاربران",
            reply_markup=premium_preview_keyboard(),
        )
        return

    await call.answer("دریافت ایموجی‌ها شروع شد.")
    premium_last_error = ""
    await refresh_premium_emojis()

    await edit_report_message(
        call.message,
        premium_status_text(),
        premium_preview_keyboard(),
    )


@dp.callback_query(F.data.startswith("ap:"))
async def admin_report_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer("گزارش فقط در خصوصی است.", show_alert=True)
        return

    try:
        prefix, kind, page_text, until_text = call.data.split(":")

        if prefix != "ap" or kind not in ("u", "w", "a"):
            raise ValueError

        page = page_number(page_text)
        until = int(until_text)

        if not 0 <= until <= 10_000_000_000:
            raise ValueError

    except (ValueError, TypeError):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    clear_input_state(ADMIN_USER_ID)
    await call.answer()

    await show_admin_report(
        call.message, kind, page=page, until=until, edit=True,
    )


@dp.callback_query(F.data.startswith("sp:"))
async def suspicious_report_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer("گزارش فقط در خصوصی است.", show_alert=True)
        return

    try:
        prefix, group_text, member_text, until_text = call.data.split(":")

        if prefix != "sp":
            raise ValueError

        group_page = page_number(group_text)
        member_page = page_number(member_text)
        until_id = int(until_text)

        if not 0 <= until_id <= MAX_USER_ID:
            raise ValueError

    except (ValueError, TypeError):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    clear_input_state(ADMIN_USER_ID)
    await call.answer()

    await show_suspicious_report(
        call.message,
        group_page=group_page,
        member_page=member_page,
        until_id=until_id,
        edit=True,
    )


# ================= REMOTE DAILY ACTIVATION =================

@dp.callback_query(F.data.startswith("ud:"))
async def admin_enable_daily_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer(
            "مدیریت کاربران فقط در خصوصی ربات انجام می‌شود.",
            show_alert=True,
        )
        return

    try:
        prefix, action, user_text = call.data.split(":")

        if prefix != "ud" or action != "on":
            raise ValueError

        user_id = record_id(user_text)

    except (ValueError, TypeError):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    clear_input_state(ADMIN_USER_ID)

    try:
        status = enable_daily_mio_by_admin(
            user_id,
            call.from_user.id,
        )

    except Exception:
        rollback_db()
        logger.exception(
            "فعال‌سازی مدیریتی روزانه ناموفق؛ user_id=%s",
            user_id,
        )
        await call.answer(
            "ثبت فعال‌سازی انجام نشد؛ دوباره تلاش کن.",
            show_alert=True,
        )
        return

    notices = {
        "ok": (
            "✅ میو روزانهٔ کاربر روشن شد.\n"
            "موجودی تغییر نکرد."
        ),
        "already": "میو روزانهٔ این کاربر از قبل روشن است.",
        "banned": "🚫 ابتدا کاربر را آن‌بن کن؛ روزانه روشن نشد.",
        "missing": "کاربر دیگر در دیتابیس موجود نیست.",
    }

    await call.answer(
        notices.get(status, "عملیات انجام نشد."),
        show_alert=True,
    )

    # کارت از دیتابیس بازخوانی می‌شود و دکمهٔ روزانه حذف می‌شود.
    await show_admin_user(
        call.message,
        user_id,
        edit=True,
        refresh_profile=False,
    )


# ================= GIFT BUTTONS =================

async def ask_for_gift_code(message, user_id, edit=False):
    clear_input_state(user_id)

    if get_balance(user_id) >= NORMAL_BALANCE_CAP:
        text = gift_balance_warning()
        keyboard = gift_withdraw_keyboard()

    else:
        waiting_gift_codes.add(user_id)

        text = (
            "🎟 کد هدیه را وارد کنید:\n\n"
            f"{GIFT_CAP_NOTICE}\n\n"
            "هر کد برای هر حساب فقط یک‌بار قابل استفاده است.\n"
            "اگر فقط بخشی از مبلغ تا سقف جا داشته باشد، "
            "همان بخش واریز می‌شود و کد مصرف می‌شود.\n\n"
            "برای دریافت مبلغ بیشتر، می‌تونی اول موجودیت رو برداشت کنی."
        )

        keyboard = K(
            [B("📤 برداشت موجودی", "withdraw")],
            [B("🔙 برگشت", "main_menu")],
        )

    if edit:
        await edit_report_message(message, text, keyboard)

    else:
        await message.answer(
            text,
            reply_markup=keyboard,
            parse_mode=None,
        )


@dp.callback_query(F.data == "gift")
async def gift_menu(call: CallbackQuery):
    if not call.message or call.message.chat.type != "private":
        await call.answer("این بخش فقط در خصوصی ربات است.", show_alert=True)
        return

    clear_input_state(call.from_user.id)
    await call.answer()

    if call.from_user.id == ADMIN_USER_ID:
        await show_gift_panel(call.message, edit=True)

    else:
        await ask_for_gift_code(
            call.message, call.from_user.id, edit=True,
        )


@dp.callback_query(F.data.startswith("gc:"))
async def gift_admin_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer("پنل فقط در خصوصی ربات است.", show_alert=True)
        return

    parts = (call.data or "").split(":")

    try:
        action = parts[1]
        gift_id = 0
        page = 0
        member_page = 0
        nonce = ""

        if action in ("new", "redeem"):
            if len(parts) != 2:
                raise ValueError

        elif action == "list":
            if len(parts) != 3:
                raise ValueError
            page = page_number(parts[2])

        elif action in ("view", "delete"):
            if len(parts) != 4:
                raise ValueError
            gift_id = record_id(parts[2])
            page = page_number(parts[3])

        elif action == "users":
            if len(parts) != 5:
                raise ValueError
            gift_id = record_id(parts[2])
            member_page = page_number(parts[3])
            page = page_number(parts[4])

        elif action == "delok":
            if len(parts) != 5:
                raise ValueError
            gift_id = record_id(parts[2])
            page = page_number(parts[3])
            nonce = parts[4]

        else:
            raise ValueError

    except (ValueError, TypeError, IndexError):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    if action == "delok":
        pending = admin_gift_delete.get(ADMIN_USER_ID)

        if (
            not pending
            or pending["gift_id"] != gift_id
            or pending["nonce"] != nonce
        ):
            await call.answer(
                "این درخواست دیگر فعال نیست.",
                show_alert=True,
            )
            return

        try:
            changed = delete_gift_code(gift_id, ADMIN_USER_ID)

        except Exception:
            rollback_db()
            logger.exception("حذف کد هدیه ناموفق")
            await call.answer(
                "حذف انجام نشد؛ دوباره تلاش کن.",
                show_alert=True,
            )
            return

        clear_input_state(ADMIN_USER_ID)
        await call.answer(
            "کد حذف شد." if changed else "کد قبلاً حذف شده است."
        )
        await show_gift_panel(call.message, page=page, edit=True)
        return

    clear_input_state(ADMIN_USER_ID)
    await call.answer()

    try:
        if action == "new":
            admin_gift_create[ADMIN_USER_ID] = {
                "stage": "code",
                "token": secrets.token_hex(16),
            }

            await edit_report_message(
                call.message,
                "🎁 ثبت کد هدیه\n\n"
                "کد هدیه را وارد کن:\n\n"
                "از حروف، عدد، خط‌تیره یا زیرخط استفاده کن؛ بدون فاصله.\n"
                "حداکثر ۶۴ کاراکتر.\n\n"
                "برای انصراف «لغو» را بفرست.",
                gift_cancel_keyboard(),
            )

        elif action == "redeem":
            await ask_for_gift_code(
                call.message, ADMIN_USER_ID, edit=True,
            )

        elif action == "list":
            await show_gift_panel(call.message, page=page, edit=True)

        elif action == "view":
            await show_gift_panel(
                call.message,
                kind="view",
                gift_id=gift_id,
                page=page,
                edit=True,
            )

        elif action == "users":
            await show_gift_panel(
                call.message,
                kind="users",
                gift_id=gift_id,
                page=page,
                member_page=member_page,
                edit=True,
            )

        elif action == "delete":
            gift = read_gift(gift_id)

            if not gift or gift["deleted_at"] is not None:
                await edit_report_message(
                    call.message,
                    "این کد وجود ندارد یا قبلاً حذف شده است.",
                    back_keyboard(f"gc:list:{page}"),
                )
                return

            nonce = secrets.token_hex(6)
            admin_gift_delete[ADMIN_USER_ID] = {
                "gift_id": gift_id,
                "nonce": nonce,
            }

            await edit_report_message(
                call.message,
                "🗑 این کد هدیه حذف شود؟\n\n"
                f"کد: {gift['code']}\n"
                f"دریافت‌کنندگان: {gift['used_count']}\n\n"
                "پس از حذف، دریافت جدید بسته می‌شود.\n"
                "مبالغ قبلاً واریزشده و سابقهٔ دریافت حفظ می‌شوند.",
                K(
                    [
                        B(
                            "🗑 تأیید حذف",
                            f"gc:delok:{gift_id}:{page}:{nonce}",
                            "danger",
                        )
                    ],
                    [B("🔙 لغو", f"gc:view:{gift_id}:{page}")],
                ),
            )

    except Exception:
        rollback_db()
        logger.exception("نمایش پنل هدیه ناموفق")

        await call.message.answer(
            "❌ نمایش پنل انجام نشد؛ دوباره /gifts را بفرست."
        )


# ================= BAN / UNBAN BUTTONS =================

@dp.callback_query(F.data.startswith("ub:"))
async def admin_user_ban_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer("مدیریت کاربران فقط در خصوصی است.", show_alert=True)
        return

    try:
        parts = call.data.split(":")
        action = parts[1]
        user_id = int(parts[2])

        if not 0 <= user_id <= MAX_USER_ID:
            raise ValueError

        if action in ("back", "view", "ask", "unbanask"):
            if len(parts) != 3:
                raise ValueError

        elif action in ("confirm", "unbanconfirm", "cancel"):
            if len(parts) != 4:
                raise ValueError

        else:
            raise ValueError

        if action != "back" and user_id == 0:
            raise ValueError

    except (ValueError, TypeError, IndexError):
        await call.answer("دکمه معتبر نیست.", show_alert=True)
        return

    if action == "back":
        clear_input_state(ADMIN_USER_ID)
        await call.answer()

        await edit_report_message(
            call.message,
            "برای جست‌وجوی کاربر:\n\n"
            "@username پیداش کن\n"
            "828292228 پیداش کن\n\n"
            "در کارت کاربر می‌توانی بن، آن‌بن یا روزانهٔ خاموش را روشن کنی.\n\n"
            "برای مقصدهای مشترک برداشت:\nمشکوک‌ها",
            None,
        )
        return

    if action == "view":
        clear_input_state(ADMIN_USER_ID)
        await call.answer()
        await show_admin_user(call.message, user_id, edit=True)
        return

    if user_id == ADMIN_USER_ID:
        await call.answer("حساب ادمین قابل بن شدن نیست.", show_alert=True)
        return

    row = read_registered_user(user_id)

    if not row:
        await call.answer("کاربر در دیتابیس پیدا نشد.", show_alert=True)
        return

    if action in ("ask", "unbanask"):
        banned = get_user_ban(user_id)
        unbanning = action == "unbanask"

        if (unbanning and not banned) or (not unbanning and banned):
            clear_input_state(ADMIN_USER_ID)

            await call.answer(
                "این کاربر مسدود نیست."
                if unbanning
                else "این کاربر قبلاً مسدود شده است.",
                show_alert=True,
            )

            await show_admin_user(call.message, user_id, edit=True)
            return

        clear_input_state(ADMIN_USER_ID)
        nonce = secrets.token_hex(6)

        admin_pending_ban[ADMIN_USER_ID] = {
            "target_id": user_id,
            "nonce": nonce,
            "stage": "confirm",
            "operation": "unban" if unbanning else "ban",
        }

        confirm_action = "unbanconfirm" if unbanning else "confirm"

        lines = [
            (
                "✅ آیا مطمئن هستید این کاربر آن‌بن شود؟"
                if unbanning
                else "🚫 آیا مطمئن هستید این کاربر بن شود؟"
            ),
            "",
            f"نام: {report_text(row['first_name'], 100)}",
            f"یوزرنیم: {username_text(row['username'])}",
            f"🆔 آیدی عددی: {user_id}",
            f"💎 موجودی: {int(row['balance'] or 0):,} میو",
        ]

        if unbanning:
            lines.extend([
                "",
                "دلیل بن قبلی:",
                banned["reason"],
                "",
                "برداشت‌های لغوشده به صف برنمی‌گردند.",
                "میو روزانه خودکار فعال نمی‌شود.",
            ])

        await call.answer()

        await edit_report_message(
            call.message,
            "\n".join(lines),
            K(
                [
                    B(
                        "✅ تأیید آن‌بن" if unbanning else "✅ تأیید بن",
                        f"ub:{confirm_action}:{user_id}:{nonce}",
                    )
                ],
                [B("❌ لغو", f"ub:cancel:{user_id}:{nonce}")],
            ),
        )
        return

    pending = admin_pending_ban.get(ADMIN_USER_ID)
    nonce = parts[3]

    if (
        not pending
        or pending["target_id"] != user_id
        or pending["nonce"] != nonce
    ):
        await call.answer(
            "این درخواست دیگر فعال نیست؛ کاربر را دوباره جست‌وجو کن.",
            show_alert=True,
        )
        return

    if action == "cancel":
        clear_input_state(ADMIN_USER_ID)
        await call.answer("عملیات لغو شد.")
        await show_admin_user(call.message, user_id, edit=True)
        return

    if action == "unbanconfirm":
        if pending["operation"] != "unban" or pending["stage"] != "confirm":
            await call.answer("درخواست معتبر نیست.", show_alert=True)
            return

        try:
            result = unban_registered_user(user_id, ADMIN_USER_ID)

        except Exception:
            rollback_db()
            logger.exception("آن‌بن ناموفق؛ user_id=%s", user_id)
            await call.answer("ثبت آن‌بن انجام نشد.", show_alert=True)
            return

        clear_input_state(ADMIN_USER_ID)

        if not result["found"]:
            await call.answer("کاربر دیگر موجود نیست.", show_alert=True)
            return

        await call.answer(
            "کاربر آن‌بن شد." if result["changed"] else "کاربر مسدود نبود."
        )

        await edit_report_message(
            call.message,
            "✅ این کاربر اکنون مسدود نیست.\n\n"
            f"🆔 آیدی عددی: {user_id}\n\n"
            "دسترسی و ثبت برداشت جدید باز شد.\n"
            "برداشت‌های لغوشده به صف برنگشتند.\n"
            "میو روزانه خودکار فعال نشده است.",
            K(
                [B("👤 اطلاعات کاربر", f"ub:view:{user_id}")],
                [B("🔙 برگشت", "ub:back:0")],
            ),
        )
        return

    if pending["operation"] != "ban":
        await call.answer("درخواست بن معتبر نیست.", show_alert=True)
        return

    if pending["stage"] != "confirm":
        await call.answer("دلیل بن را به‌صورت پیام بفرست.", show_alert=True)
        return

    if is_user_banned(user_id):
        clear_input_state(ADMIN_USER_ID)
        await call.answer("کاربر قبلاً مسدود شده است.", show_alert=True)
        return

    pending["stage"] = "reason"
    await call.answer()

    await edit_report_message(
        call.message,
        "✍️ دلیل بن این کاربر را بنویس:\n\n"
        f"👤 {username_text(row['username'])}\n"
        f"🆔 {user_id}\n\n"
        "این دلیل با زدن /start به کاربر نمایش داده می‌شود.\n"
        f"حداکثر {BAN_REASON_MAX_LENGTH} کاراکتر.\n\n"
        "برای انصراف «لغو» را بفرست.",
        K([B("❌ لغو بن", f"ub:cancel:{user_id}:{nonce}")]),
    )


# ================= START / CANCEL =================

@dp.message(Command("start"))
async def start(message: types.Message):
    user = message.from_user
    referrer_id = 0
    args = (message.text or "").split()

    if len(args) > 1:
        try:
            referrer_id = int(english_digits(args[1]))

            if not 0 < referrer_id <= MAX_USER_ID:
                referrer_id = 0

        except ValueError:
            referrer_id = 0

    add_user(user.id, user.username, user.first_name, referrer_id)
    clear_input_state(user.id)
    request_queue_update(user.id)
    await show_menu(message, user.id)


@dp.message(Command("meno"))
async def meno(message: types.Message):
    clear_input_state(message.from_user.id)
    request_queue_update(message.from_user.id)
    await show_menu(message, message.from_user.id)


@dp.message(Command("cancel"))
@dp.message(lambda m: normalize_command(m.text) == "لغو")
async def cancel_user_input(message: types.Message):
    if not message.from_user:
        return

    clear_input_state(message.from_user.id)
    await message.answer("✅ عملیات لغو شد.")
    await show_menu(message, message.from_user.id)


# ================= RECEIVE EMOJI PACK LINK =================

@dp.message(
    F.from_user.id == ADMIN_USER_ID,
    lambda m: ADMIN_USER_ID in admin_pack_input,
)
async def receive_emoji_pack_link(message: types.Message):
    if message.chat.type != "private":
        return

    token = admin_pack_input.get(ADMIN_USER_ID)

    if token is None:
        return

    pack_name = parse_emoji_pack_link(message.text)

    if pack_name is None:
        await message.answer(
            "❌ لینک معتبر پک ایموجی را بفرست.\n\n"
            "مثال:\nhttps://t.me/addemoji/MeowieQ",
            reply_markup=K([B("🔙 برگشت به پنل پک", "ep:panel")]),
            parse_mode=None,
        )
        return

    await message.answer("⏳ در حال دریافت و بررسی پک…")

    try:
        status = await change_premium_packs(
            (pack_name,),
            ADMIN_USER_ID,
            input_token=token,
        )

    except ValueError as error:
        if admin_pack_input.get(ADMIN_USER_ID) != token:
            return

        await message.answer(
            f"❌ پک اضافه نشد.\n\n{error}\n\n"
            "لینک را بررسی کن و دوباره بفرست.",
            reply_markup=K([B("🔙 برگشت به پنل پک", "ep:panel")]),
            parse_mode=None,
        )
        return

    except Exception:
        rollback_db()
        logger.exception("افزودن پک ایموجی ناموفق")

        if admin_pack_input.get(ADMIN_USER_ID) != token:
            return

        await message.answer(
            "❌ ثبت پک انجام نشد؛ دوباره تلاش کن.",
            reply_markup=K([B("🔙 برگشت به پنل پک", "ep:panel")]),
        )
        return

    if status == "cancelled":
        return

    if status == "delete_first":
        if admin_pack_input.get(ADMIN_USER_ID) == token:
            admin_pack_input.pop(ADMIN_USER_ID, None)

        await message.answer("اول «حذف پک فعلی» را بزن.")
        await show_pack_panel(message)
        return

    await message.answer("✅ پک ایموجی فعال شد.")
    await show_pack_panel(message)


# ================= GIFT ADMIN INPUT =================

@dp.message(
    F.from_user.id == ADMIN_USER_ID,
    lambda m: ADMIN_USER_ID in admin_gift_create,
)
async def receive_gift_admin_input(message: types.Message):
    if message.chat.type != "private":
        return

    state = admin_gift_create.get(ADMIN_USER_ID)

    if not state:
        return

    text = (message.text or "").strip()

    if not text:
        await message.answer(
            "ورودی را به‌صورت متن بفرست.",
            reply_markup=gift_cancel_keyboard(),
        )
        return

    stage = state["stage"]

    if stage == "code":
        code = normalize_gift_code(text)

        if code is None:
            await message.answer(
                "❌ کد باید ۱ تا ۶۴ کاراکتر و بدون فاصله باشد.\n"
                "حروف، عدد، خط‌تیره و زیرخط مجازند.",
                reply_markup=gift_cancel_keyboard(),
            )
            return

        exists = fetch_one(
            "SELECT 1 FROM mio_gift_codes WHERE code=%s",
            (code,),
        )

        if exists:
            await message.answer(
                "❌ این کد قبلاً ثبت شده است؛ یک کد جدید وارد کن.\n"
                "نام کدهای حذف‌شده هم دوباره استفاده نمی‌شود.",
                reply_markup=gift_cancel_keyboard(),
            )
            return

        state["code"] = code
        state["stage"] = "hours"

        await message.answer(
            f"✅ کد: {code}\n\n"
            "⏱ مدت اعتبار کد را وارد کن:\n\n"
            "فقط عدد صحیح به ساعت؛ مثلاً ۲۴ یعنی ۲۴ ساعت.\n"
            "برای بدون محدودیت زمانی بنویس: نامحدود",
            reply_markup=gift_cancel_keyboard(),
            parse_mode=None,
        )
        return

    if stage == "hours":
        normalized = normalize_command(text).replace(" ", "")

        if normalized == "نامحدود":
            hours = None

        else:
            try:
                hours = positive_integer(text, GIFT_MAX_HOURS)

            except ValueError:
                await message.answer(
                    "❌ مدت را با عدد صحیح مثبت به ساعت وارد کن "
                    "یا بنویس «نامحدود».\n"
                    f"حداکثر مدت عددی: {GIFT_MAX_HOURS:,} ساعت.",
                    reply_markup=gift_cancel_keyboard(),
                )
                return

        state["hours"] = hours
        state["stage"] = "amount"

        await message.answer(
            "💎 مبلغ کد هدیه برای هر نفر را وارد کن:\n\n"
            "مثال: 40000\n\n"
            f"{GIFT_CAP_NOTICE}",
            reply_markup=gift_cancel_keyboard(),
        )
        return

    if stage == "amount":
        try:
            amount = positive_integer(text)

        except ValueError:
            await message.answer(
                "❌ مبلغ باید عدد صحیح مثبت باشد.",
                reply_markup=gift_cancel_keyboard(),
            )
            return

        state["amount"] = amount
        state["stage"] = "capacity"

        await message.answer(
            f"💎 مبلغ کد برای هر نفر: {amount:,} میو\n"
            f"{GIFT_CAP_NOTICE}\n\n"
            "👥 چند نفر این هدیه را دریافت کنند؟\n\n"
            "یک عدد صحیح مثبت وارد کن؛ مثلاً ۱۰ یا ۳۳.",
            reply_markup=gift_cancel_keyboard(),
        )
        return

    if stage == "capacity":
        try:
            capacity = positive_integer(text)

        except ValueError:
            await message.answer(
                "❌ ظرفیت باید عدد صحیح مثبت باشد.",
                reply_markup=gift_cancel_keyboard(),
            )
            return

        try:
            gift_id = create_gift_code(state, capacity, ADMIN_USER_ID)

        except psycopg2.errors.UniqueViolation:
            rollback_db()
            admin_gift_create[ADMIN_USER_ID] = {
                "stage": "code",
                "token": secrets.token_hex(16),
            }

            await message.answer(
                "❌ این نام کد قبلاً ثبت شده است؛ کد جدید را وارد کن.",
                reply_markup=gift_cancel_keyboard(),
            )
            return

        except Exception:
            rollback_db()
            logger.exception("ساخت کد هدیه ناموفق")

            await message.answer(
                "❌ نتیجهٔ ثبت مشخص نشد؛ ظرفیت را دوباره بفرست.\n"
                "تکرار این مرحله کد دوم ایجاد نمی‌کند.",
                reply_markup=gift_cancel_keyboard(),
            )
            return

        clear_input_state(ADMIN_USER_ID)

        await message.answer(
            "✅ کد هدیه ثبت شد.\n"
            "زمان اعتبار از لحظهٔ ثبت نهایی محاسبه می‌شود."
        )

        await show_gift_panel(
            message,
            kind="view",
            gift_id=gift_id,
        )


# ================= RECEIVE BAN REASON =================

@dp.message(
    F.from_user.id == ADMIN_USER_ID,
    lambda m: (
        admin_pending_ban.get(ADMIN_USER_ID, {}).get("stage") == "reason"
    ),
)
async def receive_ban_reason(message: types.Message):
    if message.chat.type != "private":
        return

    pending = admin_pending_ban.get(ADMIN_USER_ID)

    if not pending or pending.get("operation") != "ban":
        return

    reason = (message.text or "").strip()

    if not reason or len(reason) > BAN_REASON_MAX_LENGTH:
        await message.answer(
            f"دلیل بن را به‌صورت متن و حداکثر {BAN_REASON_MAX_LENGTH} "
            "کاراکتر بفرست.\nبرای انصراف «لغو» را بفرست."
        )
        return

    user_id = pending["target_id"]

    try:
        result = ban_registered_user(user_id, reason, ADMIN_USER_ID)

    except Exception:
        rollback_db()
        logger.exception("ثبت بن ناموفق؛ user_id=%s", user_id)

        await message.answer(
            "❌ ثبت بن انجام نشد؛ دوباره دلیل را بفرست یا «لغو» را بزن."
        )
        return

    clear_input_state(ADMIN_USER_ID)

    if not result["found"]:
        await message.answer("❌ کاربر دیگر در دیتابیس موجود نیست.")
        return

    if result["already_banned"]:
        await message.answer("این کاربر قبلاً مسدود شده است.")
        return

    lines = [
        "✅ کاربر از ربات مسدود شد.",
        "",
        f"🆔 آیدی عددی: {user_id}",
        "",
        "دلیل:",
        reason,
        "",
        "🎁 میو روزانه غیرفعال شد.",
        "🎟 دریافت کد هدیه مسدود شد.",
        "📨 ارسال پیام‌های این بات به کاربر متوقف شد.",
        f"📤 درخواست‌های منتظر لغوشده: {result['cancelled']}",
        "🚫 ثبت برداشت جدید در دیتابیس مشترک مسدود شد.",
    ]

    if result["processing"]:
        lines.extend([
            "",
            f"⚠️ {result['processing']} برداشت قبلاً در حال پردازش بوده است.",
            "توقف انتقالی که یوزربات قبلاً شروع کرده از این فایل تضمین نمی‌شود.",
        ])

    await message.answer(
        "\n".join(lines),
        reply_markup=K([B("👤 اطلاعات کاربر", f"ub:view:{user_id}")]),
        parse_mode=None,
    )


# ================= RECEIVE GIFT CODE =================

@dp.message(
    lambda m: (
        m.from_user is not None
        and m.from_user.id in waiting_gift_codes
    )
)
async def receive_gift_code(message: types.Message):
    if message.chat.type != "private":
        return

    user_id = message.from_user.id
    code = normalize_gift_code(message.text)

    if code is None:
        await message.answer(
            "❌ کد معتبر نیست؛ کد هدیه را بدون فاصله وارد کن.",
            reply_markup=back_keyboard(),
        )
        return

    try:
        result = redeem_gift_code(user_id, code)

    except UserBannedError:
        waiting_gift_codes.discard(user_id)
        banned = get_user_ban(user_id)

        await message.answer(
            ban_notice(banned["reason"] if banned else "حساب مسدود است."),
            parse_mode=None,
        )
        return

    except Exception:
        rollback_db()
        logger.exception(
            "دریافت کد هدیه ناموفق؛ user_id=%s", user_id,
        )

        await message.answer(
            "❌ نتیجهٔ دریافت مشخص نشد؛ کمی بعد همین کد را دوباره بفرست.\n"
            "اگر قبلاً ثبت شده باشد، دوباره واریز نمی‌شود "
            "و از ظرفیت کم نمی‌شود.",
            reply_markup=back_keyboard(),
        )
        return

    status = result["status"]

    if status == "ok":
        waiting_gift_codes.discard(user_id)

        text = (
            "🎉 تبریک!\n\n"
            f"💎 {result['amount']:,} میو به موجودی شما اضافه شد.\n\n"
            f"💰 موجودی فعلی شما: {result['balance']:,} میو"
        )

        if result["amount"] < result["requested_amount"]:
            text += (
                "\n\n"
                f"{GIFT_CAP_NOTICE}\n"
                "فقط مبلغ باقی‌مانده تا سقف واریز شد.\n"
                "این کد برای حساب شما مصرف شد و باقی مبلغ آن "
                "قابل دریافت مجدد نیست."
            )

        buttons = [[B("✅ تأیید", "main_menu")]]

        if result["balance"] >= NORMAL_BALANCE_CAP:
            text += (
                "\n\n"
                "⚠️ موجودیت به سقف رسیده؛ "
                "برای دریافت هدیه‌های بعدی اول موجودیت رو برداشت کن."
            )
            buttons.insert(0, [B("📤 برداشت موجودی", "withdraw")])

        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=buttons
            ),
        )
        return

    if status == "balance_full":
        waiting_gift_codes.discard(user_id)

        await message.answer(
            gift_balance_warning(),
            reply_markup=gift_withdraw_keyboard(),
        )
        return

    notices = {
        "invalid": "❌ کد هدیه معتبر نیست یا حذف شده است.",
        "expired": "❌ کد منقضی شده؛ زمان استفاده از کد تمام شده است.",
        "full": "❌ کد منقضی شده؛ ظرفیت دریافت این کد تکمیل شده است.",
        "already": "❌ شما قبلاً این کد هدیه را دریافت کرده‌اید.",
        "unregistered": "برای دریافت هدیه ابتدا /start را بزن.",
    }

    await message.answer(
        notices.get(status, "❌ دریافت کد انجام نشد."),
        reply_markup=back_keyboard(),
    )


# ================= MAIN MENU / INVITE =================

@dp.callback_query(F.data == "main_menu")
async def main_menu(call: CallbackQuery):
    clear_input_state(call.from_user.id)
    await call.answer()

    try:
        await call.message.delete()
    except TelegramBadRequest:
        pass

    await show_menu(call.message, call.from_user.id)


@dp.callback_query(F.data == "balance")
async def balance(call: CallbackQuery):
    await call.answer()

    await call.message.edit_text(
        f"💎 موجودی شما:\n\n{get_balance(call.from_user.id):,} میو",
        reply_markup=back_keyboard(),
    )


@dp.callback_query(F.data == "invite")
async def invite(call: CallbackQuery):
    clear_input_state(call.from_user.id)
    await call.answer()
    bot_info = await bot.get_me()

    link = f"https://t.me/{bot_info.username}?start={call.from_user.id}"

    await call.message.edit_text(
        "👥 لینک دعوت شما:\n\n"
        f"{link}\n\n"
        "🎁 با هر دعوت موفق:\n\n"
        "400,000 میو دریافت می‌کنید.",
        reply_markup=back_keyboard(),
    )


# ================= WITHDRAW =================

@dp.callback_query(F.data == "withdraw")
async def withdraw(call: CallbackQuery):
    clear_input_state(call.from_user.id)
    await call.answer()

    await call.message.edit_text(
        "📤 برداشت میو\n\n"
        "روش قدیمی:\n\n"
        "1️⃣ وارد گپ شوید:\n\n"
        "https://t.me/panke_saghfe\n\n"
        "2️⃣ دستور زیر را بزنید:\n\n"
        "میوهام رو بده\n\n"
        "یا:\n\nمیو هام رو بده\n\n"
        "یا بدون عضویت در گپ از روش‌های زیر استفاده کنید:",
        reply_markup=K(
            [B("📤 برداشت با آیدی (بدون عضویت گپ)", "withdraw_by_id")],
            [B("🏦 برداشت با بانک میویی", "withdraw_bank")],
            [B("🔙 برگشت", "main_menu")],
        ),
    )


@dp.callback_query(F.data == "withdraw_by_id")
async def withdraw_by_id(call: CallbackQuery):
    await call.answer()

    await call.message.edit_text(
        "📤 برداشت با آیدی\n\n"
        "بدون نیاز به عضویت گپ، درخواست برداشت ثبت کنید.\n\n"
        "⚠️ آیدی مقصد باید با @ شروع شود.\n\n"
        "مثال:\n@username",
        reply_markup=K(
            [B("✅ شروع", "start_withdraw_by_id")],
            [B("🔙 برگشت", "withdraw")],
        ),
    )


async def pending_callback_notice(call):
    pending = get_pending_withdraw(call.from_user.id)

    if not pending:
        return False

    request_id = pending[0]
    info = get_queue_info(request_id)

    if info and info["status"] == 0:
        notice = (
            "⏳ درخواست فعال داری.\n"
            f"شما نفر {persian_number(info['queue_position'])} در صف هستید."
        )

    elif info and info["status"] == 1:
        notice = "🔄 نوبت شما رسیده؛ برداشت در حال پردازش است."

    else:
        notice = "وضعیت برداشت قبلی به‌روزرسانی شد."

    await call.answer(notice, show_alert=True)
    await show_request_queue_now(request_id)
    return True


@dp.callback_query(F.data == "start_withdraw_by_id")
async def start_withdraw_by_id(call: CallbackQuery):
    user_id = call.from_user.id

    if await pending_callback_notice(call):
        return

    if get_balance(user_id) <= 0:
        await call.answer("❌ موجودی شما صفر است.", show_alert=True)
        return

    clear_input_state(user_id)
    waiting_withdraw_id.add(user_id)
    await call.answer()

    await call.message.edit_text(
        "📤 آیدی تلگرام مقصد را وارد کنید:\n\n"
        "⚠️ با @ شروع شود.\n\nمثال:\n@username",
        reply_markup=back_keyboard("withdraw"),
    )


async def submit_withdrawal(message, target, method):
    user_id = message.from_user.id

    try:
        request_id, created = add_withdraw_queue(user_id, target, method)

    except UserBannedError:
        clear_input_state(user_id)
        banned = get_user_ban(user_id)

        await message.answer(
            ban_notice(banned["reason"] if banned else "حساب مسدود است."),
            parse_mode=None,
        )
        return

    except Exception:
        rollback_db()
        logger.exception("ثبت برداشت ناموفق؛ method=%s", method)

        await message.answer(
            "ارتباط با دیتابیس کامل نشد؛ چند لحظه بعد دوباره بررسی کنید."
        )
        return

    waiting_withdraw_id.discard(user_id)
    waiting_bank_card.discard(user_id)

    if request_id is None:
        await message.answer(
            "❌ موجودی قابل برداشت ندارید؛ در صورت نیاز /start را بزنید."
        )
        return

    shown = await show_request_queue_now(request_id)

    if not shown and not is_user_banned(user_id):
        await message.answer(
            (
                "✅ درخواست برداشت ثبت شد.\n\n"
                if created
                else "⏳ درخواست قبلی شما هنوز فعال است.\n\n"
            )
            + "ارسال پیام نوبت فعلاً انجام نشد؛ بات دوباره آن را بررسی می‌کند."
        )


@dp.message(
    lambda m: (
        m.from_user is not None
        and m.from_user.id in waiting_withdraw_id
    )
)
async def receive_withdraw_username(message: types.Message):
    target = (message.text or "").strip()

    if not re.fullmatch(r"@[A-Za-z0-9_]{1,32}", target):
        await message.answer(
            "❌ آیدی معتبر با @ وارد کنید.\n\nمثال: @username"
        )
        return

    await submit_withdrawal(message, target, "id")


@dp.callback_query(F.data == "withdraw_bank")
async def withdraw_bank(call: CallbackQuery):
    user_id = call.from_user.id

    if await pending_callback_notice(call):
        return

    amount = get_balance(user_id)

    if amount <= 0:
        await call.answer("❌ موجودی شما صفر است.", show_alert=True)
        return

    clear_input_state(user_id)
    await call.answer()

    await call.message.edit_text(
        "🏦 برداشت با بانک میویی\n\n"
        f"موجودی قابل برداشت:\n\n{amount:,} میو\n\n"
        "برای ادامه روی تأیید بزنید.",
        reply_markup=K(
            [B("✅ تأیید", "confirm_bank_withdraw")],
            [B("❌ لغو", "withdraw")],
        ),
    )


@dp.callback_query(F.data == "confirm_bank_withdraw")
async def confirm_bank_withdraw(call: CallbackQuery):
    user_id = call.from_user.id

    if await pending_callback_notice(call):
        return

    if get_balance(user_id) <= 0:
        await call.answer("❌ موجودی شما صفر است.", show_alert=True)
        return

    clear_input_state(user_id)
    waiting_bank_card.add(user_id)
    await call.answer()

    await call.message.edit_text(
        "🏦 شماره کارت میویی‌تان را وارد کنید:\n\n"
        "⚠️ فقط عدد، بدون فاصله یا خط‌تیره.\n\n"
        "مثال:\n232491826963",
        reply_markup=back_keyboard("withdraw"),
    )


@dp.message(
    lambda m: (
        m.from_user is not None
        and m.from_user.id in waiting_bank_card
    )
)
async def receive_bank_card(message: types.Message):
    card = english_digits((message.text or "").strip())

    if not re.fullmatch(r"[0-9]{10,20}", card):
        await message.answer(
            "❌ شماره کارت باید فقط عدد و بین ۱۰ تا ۲۰ رقم باشد."
        )
        return

    await submit_withdrawal(message, card, "card")


# ================= SUCCESS / FAILED WITHDRAW =================

async def check_withdraw_success():
    while True:
        try:
            users = fetch_all("""
                SELECT user_id FROM withdraw_success WHERE status=1
            """)

            for row in users:
                user_id = row[0]

                if user_id in sending_success:
                    continue

                try:
                    sending_success.add(user_id)

                    db_execute("""
                        UPDATE withdraw_success SET status=2
                        WHERE user_id=%s AND status=1
                    """, (user_id,))
                    claimed = cursor.rowcount
                    db.commit()

                    if not claimed:
                        continue

                    reset_balance(user_id)

                    if not is_user_banned(user_id):
                        await bot.send_message(
                            user_id,
                            "🎉 میوهای شما واریز شد 🫷✅\n\n"
                            "💎 موجودی حساب شما:\n\n0 میو\n\n"
                            "آیا مایل هستید رسید انتقال برای شما ارسال شود؟",
                            reply_markup=receipt_keyboard(),
                        )

                    db_execute("""
                        UPDATE withdraw_success SET status=0
                        WHERE user_id=%s AND status=2
                    """, (user_id,))
                    db.commit()
                    queue_wakeup.set()

                except Exception:
                    rollback_db()
                    logger.exception(
                        "خطا در موفقیت برداشت؛ user_id=%s", user_id,
                    )

                finally:
                    sending_success.discard(user_id)

        except Exception:
            rollback_db()
            logger.exception("خطا در حلقهٔ موفقیت برداشت")

        await asyncio.sleep(5)


async def check_failed_withdraw():
    while True:
        try:
            failed = fetch_all("""
                SELECT id, user_id FROM withdraw_queue WHERE status=3
            """)

            for request_id, user_id in failed:
                if request_id in sending_failed:
                    continue

                try:
                    sending_failed.add(request_id)

                    db_execute("""
                        UPDATE withdraw_queue SET status=4
                        WHERE id=%s AND status=3
                    """, (request_id,))
                    claimed = cursor.rowcount
                    db.commit()

                    if not claimed:
                        continue

                    if not is_user_banned(user_id):
                        await bot.send_message(
                            user_id,
                            "😔 دوست من متأسفانه انتقال با مشکل مواجه شد.\n\n"
                            "دوباره تلاش کن.\n\n"
                            "⚠️ اطلاعات واردشده را بررسی کن.\n\n"
                            "اگر مشکل از سرورهای ما باشد، به‌زودی حل خواهد شد.",
                        )

                    queue_wakeup.set()

                except Exception:
                    rollback_db()
                    logger.exception(
                        "خطا در پیام شکست؛ request=%s", request_id,
                    )

                finally:
                    sending_failed.discard(request_id)

        except Exception:
            rollback_db()
            logger.exception("خطا در حلقهٔ شکست برداشت")

        await asyncio.sleep(5)


# ================= RECEIPT =================

@dp.callback_query(F.data == "want_receipt")
async def want_receipt(call: CallbackQuery):
    db_execute("""
        INSERT INTO receipt_request(user_id, request)
        VALUES (%s, 1)
        ON CONFLICT (user_id)
        DO UPDATE SET request=EXCLUDED.request
    """, (call.from_user.id,))
    db.commit()
    await call.answer()

    await call.message.edit_text(
        "✅ درخواست رسید ثبت شد.\n\n"
        "به اکانت:\n\n@Pott5y6jg\n\n"
        "پیام بده:\n\nرسیدم رو بده\n\n"
        "🤖 رسید به صورت خودکار ارسال می‌شود."
    )


@dp.callback_query(F.data == "no_receipt")
async def no_receipt(call: CallbackQuery):
    await call.answer()

    try:
        await call.message.delete()
    except TelegramBadRequest:
        pass

    await show_menu(call.message, call.from_user.id)


# ================= DAILY SYSTEM =================

def is_daily_enabled(user_id):
    row = fetch_one(
        "SELECT daily_mio FROM users WHERE user_id=%s",
        (user_id,),
    )
    return bool(row and row[0] == 1)


def activate_daily_mio(user_id):
    # فعال‌سازی توسط خود کاربر با پاداش قبلی.
    try:
        db_execute("""
            SELECT daily_mio FROM users
            WHERE user_id=%s FOR UPDATE
        """, (user_id,))
        row = cursor.fetchone()

        if row is None:
            db.commit()
            return "missing", None

        db_execute("""
            SELECT 1 FROM mio_user_bans
            WHERE user_id=%s AND active=TRUE
        """, (user_id,))

        if cursor.fetchone():
            raise UserBannedError()

        if row[0] == 1:
            db.commit()
            return "already", None

        db_execute("""
            UPDATE users
            SET
                daily_mio=1,
                balance=GREATEST(
                    COALESCE(balance, 0)::NUMERIC,
                    LEAST(
                        500000::NUMERIC,
                        COALESCE(balance, 0)::NUMERIC + 340000
                    )
                )::BIGINT
            WHERE user_id=%s
            RETURNING balance
        """, (user_id,))

        new_balance = int(cursor.fetchone()[0])
        db.commit()
        return "ok", new_balance

    except Exception:
        rollback_db()
        raise


def credit_daily_user(user_id):
    db_execute("""
        UPDATE users AS u
        SET balance=GREATEST(
            COALESCE(balance, 0)::NUMERIC,
            LEAST(
                500000::NUMERIC,
                COALESCE(balance, 0)::NUMERIC + 340000
            )
        )::BIGINT
        WHERE u.user_id=%s AND u.daily_mio=1
          AND NOT EXISTS (
              SELECT 1 FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id AND b.active=TRUE
          )
        RETURNING balance
    """, (user_id,))

    row = cursor.fetchone()
    db.commit()
    return int(row[0]) if row else None


@dp.callback_query(F.data == "daily_mio")
async def daily_mio(call: CallbackQuery):
    clear_input_state(call.from_user.id)
    await call.answer()

    if is_daily_enabled(call.from_user.id):
        await call.message.edit_text(
            "🎁 دریافت میو روزانه\n\n"
            "✅ این قابلیت برای شما فعال است.\n\n"
            "هر روز منتظر دریافت میو باشید.",
            reply_markup=back_keyboard(),
        )

    else:
        await call.message.edit_text(
            "🎁 دریافت میو روزانه\n\n"
            "💎 هر روز 340,000 میو تا سقف شارژ عادی دریافت می‌کنید.\n\n"
            "برای فعال‌سازی روی دکمه زیر بزنید.",
            reply_markup=K(
                [B("✅ فعال کن", "confirm_daily")],
                [B("🔙 برگشت", "main_menu")],
            ),
        )


@dp.callback_query(F.data == "confirm_daily")
async def confirm_daily(call: CallbackQuery):
    try:
        status, new_balance = activate_daily_mio(call.from_user.id)

    except UserBannedError:
        await call.answer("حساب مسدود است.", show_alert=True)
        return

    except Exception:
        rollback_db()
        logger.exception("فعال‌سازی روزانه ناموفق")
        await call.answer(
            "فعال‌سازی انجام نشد؛ دوباره تلاش کن.",
            show_alert=True,
        )
        return

    if status == "already":
        await call.answer("قبلاً فعال شده.", show_alert=True)
        return

    if status == "missing":
        await call.answer("ابتدا /start را بزن.", show_alert=True)
        return

    await call.answer()

    await call.message.edit_text(
        "🎉 فعال شد\n\n"
        "🎁 پاداش فعال‌سازی تا سقف شارژ عادی محاسبه شد.\n\n"
        f"💰 موجودی شما:\n\n{new_balance:,} میو",
        reply_markup=menu_keyboard(call.from_user.id),
    )


async def give_daily_mio():
    users = fetch_all("""
        SELECT u.user_id FROM users AS u
        WHERE u.daily_mio=1
          AND NOT EXISTS (
              SELECT 1 FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id AND b.active=TRUE
          )
    """)

    count = 0

    for row in users:
        user_id = row[0]

        try:
            new_balance = credit_daily_user(user_id)

        except Exception:
            rollback_db()
            logger.exception("واریز روزانه ناموفق؛ user_id=%s", user_id)
            continue

        if new_balance is None:
            continue

        count += 1

        try:
            if is_user_banned(user_id):
                continue

            await bot.send_message(
                user_id,
                "🎁 میو روزانه شما واریز شد✅\n\n"
                "💎 شارژ روزانه: تا 340,000 میو با سقف شارژ عادی\n\n"
                f"💰 موجودی فعلی: {new_balance:,} میو",
                reply_markup=K(
                    [B("✅ تأیید", "daily_confirm")],
                    [B("🏠 بازگشت به منوی اصلی", "main_menu")],
                ),
            )

        except Exception:
            rollback_db()
            logger.exception("پیام روزانه ناموفق؛ user_id=%s", user_id)

    return count


@dp.callback_query(F.data == "daily_confirm")
async def daily_confirm(call: CallbackQuery):
    await call.answer()

    try:
        await call.message.delete()
    except TelegramBadRequest:
        pass

    await show_menu(call.message, call.from_user.id)


# ================= ADMIN INPUT STATES =================

@dp.message(F.from_user.id == ADMIN_USER_ID)
async def admin_commands(message: types.Message):
    global admin_broadcast

    if message.chat.type != "private":
        return

    text = (message.text or "").strip()

    if admin_broadcast:
        if not text:
            await message.answer("پیام متنی بفرست یا «لغو» را بزن.")
            return

        admin_broadcast = False
        broadcast_text = message.text
        broadcast_entities = list(message.entities or [])

        users = fetch_all("""
            SELECT u.user_id, u.username FROM users AS u
            WHERE NOT EXISTS (
                SELECT 1 FROM mio_user_bans AS b
                WHERE b.user_id=u.user_id AND b.active=TRUE
            )
            ORDER BY u.user_id ASC
        """)

        sent = 0
        skipped = 0
        failed_users = []

        for uid, username in users:
            delivered = False
            excluded = False

            try:
                if is_user_banned(uid):
                    skipped += 1
                    continue

                await bot.send_message(
                    uid,
                    broadcast_text,
                    entities=broadcast_entities,
                    parse_mode=None,
                )
                delivered = True

            except TelegramRetryAfter as error:
                await asyncio.sleep(int(error.retry_after) + 1)

                try:
                    if is_user_banned(uid):
                        excluded = True

                    else:
                        await bot.send_message(
                            uid,
                            broadcast_text,
                            entities=broadcast_entities,
                            parse_mode=None,
                        )
                        delivered = True

                except Exception:
                    rollback_db()
                    logger.exception("ارسال مجدد همگانی ناموفق؛ uid=%s", uid)

            except Exception:
                rollback_db()
                logger.exception("پیام همگانی ناموفق؛ uid=%s", uid)

            if excluded:
                skipped += 1
            elif delivered:
                sent += 1
            else:
                failed_users.append((uid, username))

            await asyncio.sleep(0.05)

        await report_broadcast_result(message, sent, failed_users, skipped)
        return

    if message.from_user.id in admin_add_balance:
        try:
            amount = int(
                english_digits(text).replace(",", "").replace("٬", "")
            )

            if not -MAX_USER_ID <= amount <= MAX_USER_ID:
                raise ValueError

        except ValueError:
            await message.answer(
                "❌ مقدار باید عدد معتبر باشد.\nبرای انصراف «لغو» را بفرست."
            )
            return

        user_id = admin_add_balance[message.from_user.id]

        try:
            new_balance = add_balance(user_id, amount)

        except UserBannedError:
            admin_add_balance.pop(message.from_user.id, None)
            await message.answer("🚫 کاربر مسدود است؛ شارژ انجام نشد.")
            return

        except Exception:
            rollback_db()
            logger.exception("شارژ ادمین ناموفق؛ user_id=%s", user_id)
            await message.answer("❌ ثبت مبلغ انجام نشد؛ دوباره بررسی کن.")
            return

        admin_add_balance.pop(message.from_user.id, None)

        try:
            if is_user_banned(user_id):
                await message.answer(
                    "تغییر موجودی ثبت شد؛ به دلیل بن، اعلان ارسال نشد."
                )
                return

            await bot.send_message(
                user_id,
                "🎉 موجودی شما به‌روزرسانی شد\n\n"
                f"💎 مبلغ درخواست‌شده: {amount:,} میو\n\n"
                f"💰 موجودی جدید: {new_balance:,} میو",
            )

        except Exception:
            rollback_db()
            logger.exception("اعلان شارژ ناموفق؛ user_id=%s", user_id)
            await message.answer("✅ تغییر ثبت شد؛ اعلان به کاربر ارسال نشد.")
            return

        await message.answer("✅ شارژ انجام شد.")
        return

    normalized = normalize_command(text)

    if normalized.endswith("اضافه"):
        try:
            user_id = record_id(english_digits(normalized.split()[0]))

        except (ValueError, IndexError):
            await message.answer(
                "❌ فرمت اشتباه است.\nمثال:\n123456789 اضافه"
            )
            return

        exists = fetch_one(
            "SELECT 1 FROM users WHERE user_id=%s",
            (user_id,),
        )

        if not exists:
            await message.answer("❌ این کاربر در دیتابیس ثبت نشده است.")
            return

        if is_user_banned(user_id):
            await message.answer("🚫 این کاربر مسدود است.")
            return

        clear_input_state(ADMIN_USER_ID)
        admin_add_balance[ADMIN_USER_ID] = user_id

        await message.answer(
            "💎 مقدار میو را وارد کنید:\nبرای انصراف «لغو» را بفرست."
        )
        return

    await message.answer(
        "دستورهای مدیریت:\n\n"
        "👥 کاربران\n"
        "📤 برداشت‌های اخیر\n"
        "🔎 مشکوک‌ها\n"
        "🎟 کد هدیه\n"
        "✉️ پیام به کاربران\n"
        "🎁 میو بده به همه\n"
        "💎 آیدی‌عددی اضافه\n"
        "🎨 پک\n"
        "🎨 وضعیت ایموجی\n\n"
        "🔍 جست‌وجو و مدیریت کاربر:\n"
        "@username پیداش کن\n"
        "828292228 پیداش کن\n\n"
        "در کارت کاربر: بن، آن‌بن و روشن کردن روزانهٔ خاموش.\n\n"
        "/users\n"
        "/withdrawals\n"
        "/suspicious\n"
        "/gifts\n"
        "/packs\n"
        "/emoji_status\n"
        "/emoji_reload\n"
        "/cancel"
    )


# ================= UNKNOWN MESSAGE =================

@dp.message()
async def unknown_message(message: types.Message):
    if not message.from_user:
        return

    user_id = message.from_user.id

    if user_id == ADMIN_USER_ID:
        return

    if (
        user_id in waiting_withdraw_id
        or user_id in waiting_bank_card
        or user_id in waiting_gift_codes
    ):
        return

    await show_menu(message, user_id)


# ================= RUN =================

async def main():
    background_tasks = []

    try:
        logger.info("Free meowie Started")

        load_premium_selection()
        cached = read_premium_cache()

        if cached:
            install_premium_mapping(cached)

        await refresh_premium_emojis()

        logger.info(
            "ایموجی‌های بارگذاری‌شده: %s",
            len(premium_emojis),
        )

        missing = [
            symbol for symbol in UI_EMOJIS
            if not custom_id_for(symbol)
        ]

        if missing:
            logger.warning(
                "نمادهای بدون ایموجی سفارشی: %s",
                " ".join(missing),
            )

        logger.info("کد هدیه با سقف موجودی ۵۰۰ هزار فعال است")
        logger.info("فعال‌سازی روزانه از کارت مدیریت کاربر فعال است")
        logger.info("صفحه‌بندی بدون دریافت زندهٔ پروفایل اجرا می‌شود")
        logger.info("بن، آن‌بن و محدودیت‌های دیتابیس فعال است")

        background_tasks = [
            asyncio.create_task(check_withdraw_queue()),
            asyncio.create_task(check_withdraw_success()),
            asyncio.create_task(check_failed_withdraw()),
            asyncio.create_task(check_gift_panel()),
        ]

        await dp.start_polling(bot, close_bot_session=False)

    finally:
        for task in background_tasks:
            task.cancel()

        await asyncio.gather(
            *background_tasks,
            return_exceptions=True,
        )

        await bot.session.close()

        try:
            cursor.close()
            db.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
