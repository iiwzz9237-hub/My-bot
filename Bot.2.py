import asyncio
import logging
import math
import re
import secrets
import time
import unicodedata

from datetime import datetime, timezone

import psycopg2

from aiogram import Bot, Dispatcher, BaseMiddleware, types, F
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from config import BOT_TOKEN, ADMIN_ID, DATABASE_URL_TABLET


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("mio")

ADMIN_USER_ID = int(ADMIN_ID)

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
BROADCAST_FAILED_PAGE_SIZE = 30
BAN_REASON_MAX_LENGTH = 1000

queue_wakeup = asyncio.Event()
queue_message_lock = asyncio.Lock()

queue_telegram_pause_until = 0.0
profile_refresh_pause_until = 0.0


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

    columns = [
        column[0]
        for column in cursor.description
    ]

    rows = [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

    db.commit()
    return rows


# ================= TABLES / DATABASE BAN GUARDS =================

def init_database():
    try:
        db_execute(
            "SELECT pg_advisory_xact_lock(748219306)"
        )

        db_execute("""
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
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS referrals(
                id SERIAL PRIMARY KEY,
                inviter_id BIGINT,
                invited_id BIGINT UNIQUE
            )
        """)

        db_execute("""
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
        """)

        db_execute("""
            ALTER TABLE withdraw_queue
            ADD COLUMN IF NOT EXISTS completed_time DOUBLE PRECISION
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS withdraw_success(
                user_id BIGINT PRIMARY KEY,
                status INTEGER DEFAULT 1
            )
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS receipt_request(
                user_id BIGINT PRIMARY KEY,
                request INTEGER DEFAULT 0
            )
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS withdraw_receipts(
                user_id BIGINT PRIMARY KEY,
                message_id BIGINT,
                withdraw_type TEXT
            )
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS settings(
                name TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        db_execute("""
            INSERT INTO settings(name, value)
            VALUES ('group_withdraw', 'on')
            ON CONFLICT (name) DO NOTHING
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS withdraw_transfer_limits(
                method TEXT PRIMARY KEY,
                delay_seconds INTEGER NOT NULL
                    CHECK (delay_seconds > 0),
                next_allowed_at DOUBLE PRECISION
                    NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION
                    NOT NULL DEFAULT 0
            )
        """)

        for method, delay in (
            ("normal", NORMAL_TRANSFER_DELAY),
            ("card", BANK_TRANSFER_DELAY),
        ):
            db_execute("""
                INSERT INTO withdraw_transfer_limits(
                    method,
                    delay_seconds,
                    next_allowed_at,
                    updated_at
                )
                VALUES (%s, %s, 0, 0)
                ON CONFLICT (method) DO NOTHING
            """, (method, delay))

        db_execute("""
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
        """)

        db_execute("""
            CREATE TABLE IF NOT EXISTS mio_user_bans(
                user_id BIGINT PRIMARY KEY,
                reason TEXT NOT NULL,
                banned_by BIGINT NOT NULL,
                banned_at DOUBLE PRECISION NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)

        # این محدودیت در دیتابیس اجرا می‌شود.
        # در نتیجه INSERT مینی‌اپ و شروع پردازش توسط یوزربات
        # نیز برای کاربر مسدودشده رد می‌شوند.
        #
        # وضعیت‌های نهایی مجازند تا نتیجهٔ انتقالی که پیش‌تر
        # شروع شده است همچنان قابل ثبت باشد.
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

        # افزایش موجودی کاربر بن‌شده از مسیرهای دیگر، از جمله
        # فعال‌سازی روزانه یا جایزه در بک‌اند، نیز مجاز نیست.
        # کم شدن موجودی برای تسویهٔ انتقال قبلی مسدود نمی‌شود.
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

        db.commit()

    except Exception:
        rollback_db()
        raise


init_database()


# ================= VARIABLES =================

waiting_withdraw_id = set()
waiting_bank_card = set()

admin_add_balance = {}
admin_broadcast = False

# admin_id -> target_id, nonce, stage
admin_pending_ban = {}

sending_success = set()
sending_failed = set()


# ================= TEXT HELPERS =================

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
    text = unicodedata.normalize(
        "NFKC",
        str(value or ""),
    )

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
    text = english_digits(
        normalize_command(value)
    )

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

    if not 0 < user_id <= 9223372036854775807:
        return None

    return str(user_id)


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
        "لغو": "cancel",
    }

    if compact in commands:
        return commands[compact]

    command = text.split(maxsplit=1)[0] if text else ""
    command = command.split("@", 1)[0].lower()

    return {
        "/users": "users",
        "/withdrawals": "withdrawals",
        "/cancel": "cancel",
    }.get(command)


def clear_input_state(user_id):
    global admin_broadcast

    waiting_withdraw_id.discard(user_id)
    waiting_bank_card.discard(user_id)

    if user_id == ADMIN_USER_ID:
        admin_add_balance.pop(user_id, None)
        admin_pending_ban.pop(user_id, None)
        admin_broadcast = False


def report_text(value, limit=80):
    text = re.sub(
        r"\s+",
        " ",
        str(value if value is not None else ""),
    ).strip()

    if not text:
        return "—"

    if len(text) > limit:
        return text[:limit - 1] + "…"

    return text


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

    except (
        TypeError,
        ValueError,
        OverflowError,
        OSError,
    ):
        return "نامشخص"


# ================= BAN DATABASE =================

def get_user_ban(user_id):
    rows = fetch_dicts("""
        SELECT
            user_id,
            reason,
            banned_by,
            banned_at,
            active
        FROM mio_user_bans
        WHERE user_id=%s
          AND active=TRUE
    """, (user_id,))

    return rows[0] if rows else None


def is_user_banned(user_id):
    return bool(fetch_one("""
        SELECT 1
        FROM mio_user_bans
        WHERE user_id=%s
          AND active=TRUE
    """, (user_id,)))


def ban_notice(reason):
    return (
        "🚫 شما از ربات بن شدید.\n\n"
        "دلیل:\n"
        f"{reason}"
    )


def ban_registered_user(user_id, reason, admin_id):
    if admin_id != ADMIN_USER_ID:
        raise PermissionError("Admin access required")

    if user_id == ADMIN_USER_ID:
        raise ValueError("Admin cannot be banned")

    reason = reason.strip()

    if not reason or len(reason) > BAN_REASON_MAX_LENGTH:
        raise ValueError("Invalid ban reason")

    try:
        # با قفل حساب، ثبت برداشت جدید و ثبت بن هماهنگ می‌شوند.
        db_execute("""
            SELECT user_id
            FROM users
            WHERE user_id=%s
            FOR UPDATE
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
            SELECT 1
            FROM mio_user_bans
            WHERE user_id=%s
              AND active=TRUE
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
                user_id,
                reason,
                banned_by,
                banned_at,
                active
            )
            VALUES (
                %s, %s, %s,
                EXTRACT(EPOCH FROM clock_timestamp()),
                TRUE
            )

            ON CONFLICT (user_id) DO UPDATE SET
                reason=EXCLUDED.reason,
                banned_by=EXCLUDED.banned_by,
                banned_at=EXCLUDED.banned_at,
                active=TRUE
        """, (
            user_id,
            reason,
            admin_id,
        ))

        # میو روزانه برای کاربر غیرفعال می‌شود.
        db_execute("""
            UPDATE users
            SET daily_mio=0
            WHERE user_id=%s
        """, (user_id,))

        # فقط درخواست‌های هنوز شروع‌نشده لغو می‌شوند.
        # status=4 با بات، بک‌اند و یوزربات فعلی سازگار است.
        db_execute("""
            UPDATE withdraw_queue
            SET
                status=4,
                completed_time=COALESCE(
                    completed_time,
                    EXTRACT(EPOCH FROM clock_timestamp())
                )
            WHERE user_id=%s
              AND status=0
        """, (user_id,))

        cancelled = cursor.rowcount

        db_execute("""
            SELECT COUNT(*)
            FROM withdraw_queue
            WHERE user_id=%s
              AND status=1
        """, (user_id,))

        processing = int(cursor.fetchone()[0])

        db_execute("""
            UPDATE bot_withdraw_queue_messages
            SET
                finished=TRUE,
                updated_at=EXTRACT(
                    EPOCH FROM clock_timestamp()
                )
            WHERE user_id=%s
              AND finished=FALSE
        """, (user_id,))

        db_execute("""
            UPDATE receipt_request
            SET request=0
            WHERE user_id=%s
        """, (user_id,))

        db.commit()

        waiting_withdraw_id.discard(user_id)
        waiting_bank_card.discard(user_id)
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


# ================= BALANCE =================

def get_balance(user_id):
    row = fetch_one(
        "SELECT balance FROM users WHERE user_id=%s",
        (user_id,),
    )
    return int(row[0] or 0) if row else 0


def add_balance(user_id, amount):
    db_execute("""
        UPDATE users AS u

        SET balance=LEAST(
            500000,
            COALESCE(balance, 0) + %s
        )

        WHERE u.user_id=%s

          AND NOT EXISTS (
              SELECT 1
              FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id
                AND b.active=TRUE
          )

        RETURNING balance
    """, (amount, user_id))

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


# ================= USER PROFILE =================

def sync_user_profile(user_id, username, first_name):
    db_execute("""
        UPDATE users
        SET
            username=%s,
            first_name=%s

        WHERE user_id=%s

          AND (
              username IS DISTINCT FROM %s
              OR first_name IS DISTINCT FROM %s
          )
    """, (
        username,
        first_name,
        user_id,
        username,
        first_name,
    ))

    db.commit()


def add_user(
    user_id,
    username,
    first_name,
    referrer_id=0,
):
    try:
        db_execute("""
            INSERT INTO users(
                user_id,
                username,
                first_name,
                referrer_id
            )
            VALUES (%s, %s, %s, %s)

            ON CONFLICT (user_id) DO NOTHING

            RETURNING user_id
        """, (
            user_id,
            username,
            first_name,
            referrer_id,
        ))

        inserted = cursor.fetchone()

        if not inserted:
            db_execute("""
                UPDATE users
                SET
                    username=%s,
                    first_name=%s
                WHERE user_id=%s
            """, (
                username,
                first_name,
                user_id,
            ))

            db.commit()
            return False

        if referrer_id and referrer_id != user_id:
            db_execute("""
                INSERT INTO referrals(
                    inviter_id,
                    invited_id
                )
                VALUES (%s, %s)
                ON CONFLICT (invited_id) DO NOTHING
            """, (referrer_id, user_id))

            if cursor.rowcount:
                db_execute("""
                    UPDATE users AS u

                    SET balance=LEAST(
                        500000,
                        COALESCE(balance, 0) + 400000
                    )

                    WHERE u.user_id=%s

                      AND NOT EXISTS (
                          SELECT 1
                          FROM mio_user_bans AS b
                          WHERE b.user_id=u.user_id
                            AND b.active=TRUE
                      )
                """, (referrer_id,))

        db.commit()
        return True

    except Exception:
        rollback_db()
        raise


class UserProfileMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)

        if user is None or user.is_bot:
            return await handler(event, data)

        # بررسی بن پیش از اجرای هر هندلر.
        if user.id != ADMIN_USER_ID:
            try:
                banned = get_user_ban(user.id)

            except Exception:
                rollback_db()

                logger.exception(
                    "بررسی بن انجام نشد؛ user_id=%s",
                    user.id,
                )

                # در خطای بررسی دسترسی، عملیات کاربر اجرا نمی‌شود.
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
                        "🚫 شما از ربات مسدود شده‌اید.\n"
                        "برای مشاهدهٔ دلیل، /start را بزنید.",
                        show_alert=True,
                    )

                elif (
                    isinstance(event, types.Message)
                    and event.chat.type == "private"
                ):
                    text = (event.text or "").strip()
                    command = (
                        text.split(maxsplit=1)[0]
                        if text
                        else ""
                    )
                    command = command.split("@", 1)[0].lower()

                    if command == "/start":
                        await event.answer(
                            ban_notice(banned["reason"]),
                            parse_mode=None,
                        )

                return None

        try:
            sync_user_profile(
                user.id,
                user.username,
                user.first_name,
            )

        except Exception:
            rollback_db()

            logger.exception(
                "به‌روزرسانی پروفایل انجام نشد؛ user_id=%s",
                user.id,
            )

        return await handler(event, data)


dp.message.outer_middleware(UserProfileMiddleware())
dp.callback_query.outer_middleware(UserProfileMiddleware())


async def refresh_page_profiles(rows):
    global profile_refresh_pause_until

    profiles = {}
    checked = 0

    for row in rows:
        user_id = row["user_id"]

        if user_id in profiles:
            continue

        if time.monotonic() < profile_refresh_pause_until:
            break

        try:
            chat = await bot.get_chat(user_id)

            if chat.type != "private":
                continue

            username = chat.username
            first_name = chat.first_name

            sync_user_profile(
                user_id,
                username,
                first_name,
            )

            profiles[user_id] = (
                username,
                first_name,
            )
            checked += 1

        except TelegramRetryAfter as error:
            profile_refresh_pause_until = (
                time.monotonic()
                + int(error.retry_after)
                + 1
            )
            break

        except (
            TelegramBadRequest,
            TelegramForbiddenError,
        ):
            continue

        except Exception:
            rollback_db()

            logger.exception(
                "خواندن پروفایل صفحه ناموفق؛ user_id=%s",
                user_id,
            )
            break

        await asyncio.sleep(0.05)

    for row in rows:
        profile = profiles.get(row["user_id"])

        if profile is not None:
            row["username"], row["first_name"] = profile

    return checked, len({
        row["user_id"]
        for row in rows
    })


# ================= KEYBOARDS =================

def menu_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💎 موجودی",
                    callback_data="balance",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📤 برداشت",
                    callback_data="withdraw",
                )
            ],
            [
                InlineKeyboardButton(
                    text="👥 لینک دعوت",
                    callback_data="invite",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎁 دریافت میو روزانه",
                    callback_data="daily_mio",
                )
            ],
        ]
    )


def back_keyboard(callback_data="main_menu"):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔙 برگشت",
                    callback_data=callback_data,
                )
            ]
        ]
    )


def receipt_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ بله، رسید می‌خواهم",
                    callback_data="want_receipt",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ خیر",
                    callback_data="no_receipt",
                )
            ],
        ]
    )


# ================= ADMIN REPORTS =================

def admin_report_keyboard(kind, page, pages, until):
    buttons = []
    navigation = []

    if page > 0:
        navigation.append(
            InlineKeyboardButton(
                text="⬅️ صفحه قبل",
                callback_data=f"ap:{kind}:{page - 1}:{until}",
            )
        )

    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(
                text="صفحه بعد ➡️",
                callback_data=f"ap:{kind}:{page + 1}:{until}",
            )
        )

    if navigation:
        buttons.append(navigation)

    buttons.append([
        InlineKeyboardButton(
            text="🔄 به‌روزرسانی",
            callback_data=f"ap:{kind}:0:0",
        )
    ])

    if kind == "w":
        buttons.append([
            InlineKeyboardButton(
                text="📚 آخرین برداشت‌ها؛ همه زمان‌ها",
                callback_data="ap:a:0:0",
            )
        ])

    elif kind == "a":
        buttons.append([
            InlineKeyboardButton(
                text="🕒 برداشت‌های ۲۴ ساعت اخیر",
                callback_data="ap:w:0:0",
            )
        ])

    return InlineKeyboardMarkup(
        inline_keyboard=buttons
    )


def read_users_page(page):
    total = int(fetch_one(
        "SELECT COUNT(*) FROM users"
    )[0])

    pages = max(
        1,
        math.ceil(total / USERS_PAGE_SIZE),
    )

    page = min(max(0, page), pages - 1)

    rows = fetch_dicts("""
        SELECT
            u.user_id,
            u.username,
            u.first_name,
            u.balance,
            EXISTS(
                SELECT 1
                FROM mio_user_bans AS b
                WHERE b.user_id=u.user_id
                  AND b.active=TRUE
            ) AS is_banned

        FROM users AS u
        ORDER BY u.user_id ASC
        LIMIT %s OFFSET %s
    """, (
        USERS_PAGE_SIZE,
        page * USERS_PAGE_SIZE,
    ))

    return rows, total, page, pages


def read_withdraw_page(kind, page, until):
    if kind not in ("w", "a"):
        raise ValueError("Invalid report kind")

    if kind == "w" and not until:
        until = math.ceil(float(fetch_one("""
            SELECT EXTRACT(
                EPOCH FROM clock_timestamp()
            )
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
        condition = (
            f"({epoch_sql}) >= %s "
            f"AND ({epoch_sql}) <= %s"
        )
        params = (until - 86400, until)

    else:
        condition = "TRUE"
        params = ()
        until = 0

    total = int(fetch_one(
        f"""
            SELECT COUNT(*)
            FROM withdraw_queue AS q
            WHERE {condition}
        """,
        params,
    )[0])

    pages = max(
        1,
        math.ceil(total / WITHDRAW_PAGE_SIZE),
    )

    page = min(max(0, page), pages - 1)

    rows = fetch_dicts(
        f"""
            SELECT
                q.id AS request_id,
                q.user_id,
                q.target_username,
                q.amount,
                q.created_time,
                q.status,
                q.withdraw_type,
                u.username,
                u.first_name

            FROM withdraw_queue AS q

            LEFT JOIN users AS u
              ON u.user_id=q.user_id

            WHERE {condition}

            ORDER BY q.id DESC
            LIMIT %s OFFSET %s
        """,
        params + (
            WITHDRAW_PAGE_SIZE,
            page * WITHDRAW_PAGE_SIZE,
        ),
    )

    return rows, total, page, pages, until


async def build_admin_report(kind, page=0, until=0):
    if kind == "u":
        rows, total, page, pages = read_users_page(page)

        checked, unique_count = await refresh_page_profiles(rows)

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
                (
                    "🚫 وضعیت: مسدود"
                    if row["is_banned"]
                    else "✅ وضعیت: فعال"
                ),
                "──────────",
            ])

        if unique_count:
            lines.extend([
                "",
                f"اطلاعات {checked} از {unique_count} "
                "کاربر این صفحه از تلگرام بررسی شد.",
            ])

            if checked < unique_count:
                lines.append(
                    "برای بقیه، آخرین اطلاعات ذخیره‌شده "
                    "نمایش داده شده است."
                )

        return (
            "\n".join(lines),
            admin_report_keyboard(kind, page, pages, 0),
        )

    rows, total, page, pages, until = read_withdraw_page(
        kind,
        page,
        until,
    )

    title = (
        "📤 برداشت‌های ۲۴ ساعت اخیر"
        if kind == "w"
        else "📚 آخرین برداشت‌ها؛ همه زمان‌ها"
    )

    lines = [
        title,
        f"تعداد: {total:,}",
        f"صفحه {page + 1} از {pages}",
        "",
    ]

    if not rows:
        lines.append(
            "برداشتی با زمان ثبت معتبر در این بازه پیدا نشد."
            if kind == "w"
            else "هیچ برداشت ذخیره‌شده‌ای پیدا نشد."
        )

        if kind == "w":
            lines.append(
                "برای دیدن رکوردهای قدیمی یا بدون زمان، "
                "«همه زمان‌ها» را بزن."
            )

    for row in rows:
        status = row["status"]

        status_text = {
            0: "⏳ در صف",
            1: "🔄 در حال پردازش",
            2: "✅ انجام شده",
            3: "❌ شکست",
            4: "❌ ناموفق یا لغوشده؛ بررسی شده",
        }.get(status, f"نامشخص ({status})")

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
            "📤 مقصد ثبت‌شده: "
            + report_text(
                row["target_username"] or "گپی",
                80,
            ),
            f"💎 مقدار: {int(row['amount'] or 0):,} میو",
            f"🕒 {format_report_time(row['created_time'])}",
            f"📌 {status_text}",
            "──────────",
        ])

    return (
        "\n".join(lines),
        admin_report_keyboard(kind, page, pages, until),
    )


async def edit_report_message(message, text, keyboard):
    try:
        await message.edit_text(
            text,
            reply_markup=keyboard,
            parse_mode=None,
        )

    except TelegramBadRequest as error:
        description = str(error).lower()

        if "message is not modified" in description:
            return

        if (
            "message to edit not found" in description
            or "message can't be edited" in description
            or "message can not be edited" in description
        ):
            await message.answer(
                text,
                reply_markup=keyboard,
                parse_mode=None,
            )
            return

        raise


async def show_admin_report(
    message,
    kind,
    page=0,
    until=0,
    edit=False,
):
    if message.chat.type != "private":
        await message.answer(
            "این دستور را در خصوصی ربات بفرست."
        )
        return

    target = message

    try:
        if not edit:
            target = await message.answer(
                "⏳ در حال دریافت گزارش…"
            )

        text, keyboard = await build_admin_report(
            kind,
            page,
            until,
        )

        await edit_report_message(target, text, keyboard)

    except Exception:
        rollback_db()

        logger.exception(
            "خطا در گزارش ادمین؛ kind=%s page=%s",
            kind,
            page,
        )

        try:
            await edit_report_message(
                target,
                "❌ دریافت گزارش انجام نشد.\n"
                "دکمهٔ تلاش دوباره را بزن.\n"
                "جزئیات خطا در لاگ ثبت شده است.",
                InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="🔄 تلاش دوباره",
                                callback_data=f"ap:{kind}:0:0",
                            )
                        ]
                    ]
                ),
            )

        except Exception:
            logger.exception(
                "ارسال خطای گزارش نیز انجام نشد"
            )


# ================= ADMIN USER SEARCH =================

def read_registered_user(user_id):
    rows = fetch_dicts("""
        SELECT
            user_id,
            username,
            first_name,
            balance,
            daily_mio
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

    else:
        lines.append("✅ وضعیت: فعال")

        if row["user_id"] != ADMIN_USER_ID:
            buttons.append([
                InlineKeyboardButton(
                    text="🚫 بن از ربات / مسدود کردن",
                    callback_data=f"ub:ask:{row['user_id']}",
                )
            ])

    buttons.append([
        InlineKeyboardButton(
            text="🔙 برگشت",
            callback_data="ub:back:0",
        )
    ])

    return (
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def show_admin_user(message, user_id, edit=False):
    row = read_registered_user(user_id)

    if not row:
        text = "❌ این کاربر در دیتابیس ثبت نشده است."
        keyboard = back_keyboard("ub:back:0")

    else:
        await refresh_page_profiles([row])

        # موجودی و وضعیت بعد از دریافت پروفایل دوباره خوانده می‌شوند.
        row = read_registered_user(user_id)

        if not row:
            text = "❌ این کاربر دیگر در دیتابیس موجود نیست."
            keyboard = back_keyboard("ub:back:0")
        else:
            banned = get_user_ban(user_id)
            text, keyboard = admin_user_card(row, banned)

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
            SELECT
                user_id,
                username,
                first_name,
                balance,
                daily_mio
            FROM users
            WHERE LOWER(LTRIM(username, '@'))=%s
            ORDER BY user_id ASC
            LIMIT 11
        """, (username,))

        await refresh_page_profiles(rows)

        # اگر یوزرنیم ذخیره‌شده قدیمی بوده و تغییرش مشخص شد،
        # آن کاربر با یوزرنیم قبلی معرفی نمی‌شود.
        rows = [
            row
            for row in rows
            if str(row["username"] or "").lstrip("@").lower()
            == username
        ]

    else:
        row = read_registered_user(int(target))
        rows = [row] if row else []

    if not rows:
        await message.answer(
            "❌ کاربری با این مشخصات در دیتابیس پیدا نشد.\n\n"
            "جست‌وجو بین کاربران ثبت‌شدهٔ همین بات است.\n"
            "اگر یوزرنیم کاربر تازه عوض شده، "
            "با آیدی عددی جست‌وجو کن.",
            parse_mode=None,
        )
        return

    if len(rows) > 1:
        lines = [
            "چند رکورد با این یوزرنیم ذخیره شده است.",
            "برای انتخاب دقیق، آیدی عددی را بفرست:",
            "",
        ]

        for row in rows[:10]:
            lines.append(
                f"{row['user_id']} — "
                f"{report_text(row['first_name'], 60)}"
            )

        lines.extend([
            "",
            "مثال:",
            "828292228 پیداش کن",
        ])

        await message.answer(
            "\n".join(lines),
            parse_mode=None,
        )
        return

    await show_admin_user(
        message,
        rows[0]["user_id"],
    )


# ================= BROADCAST REPORT =================

async def send_admin_text(message, text):
    try:
        await message.answer(text, parse_mode=None)

    except TelegramRetryAfter as error:
        await asyncio.sleep(int(error.retry_after) + 1)
        await message.answer(text, parse_mode=None)


async def report_broadcast_result(
    message,
    sent,
    failed_users,
    skipped=0,
):
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

    latest_usernames = {}

    try:
        failed_ids = [
            uid
            for uid, _ in failed_users
        ]

        latest_usernames = dict(fetch_all("""
            SELECT user_id, username
            FROM users
            WHERE user_id = ANY(%s)
        """, (failed_ids,)))

    except Exception:
        rollback_db()
        logger.exception("خواندن یوزرنیم ارسال‌های ناموفق انجام نشد")

    pages = (
        len(failed_users) + BROADCAST_FAILED_PAGE_SIZE - 1
    ) // BROADCAST_FAILED_PAGE_SIZE

    for offset in range(
        0,
        len(failed_users),
        BROADCAST_FAILED_PAGE_SIZE,
    ):
        page = offset // BROADCAST_FAILED_PAGE_SIZE + 1

        lines = [
            "❌ ارسال پیام به این کاربران ناموفق بود:",
            f"صفحه {page} از {pages}",
            "",
        ]

        for uid, saved_username in failed_users[
            offset:offset + BROADCAST_FAILED_PAGE_SIZE
        ]:
            username = latest_usernames.get(
                uid,
                saved_username,
            )

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
        WHERE user_id=%s
          AND status IN (0, 1)
        ORDER BY id ASC
        LIMIT 1
    """, (user_id,))


def add_withdraw_queue(
    user_id,
    target_username,
    withdraw_type,
):
    try:
        db_execute("""
            SELECT balance
            FROM users
            WHERE user_id=%s
            FOR UPDATE
        """, (user_id,))

        user = cursor.fetchone()

        if not user:
            db.commit()
            return None, False

        db_execute("""
            SELECT 1
            FROM mio_user_bans
            WHERE user_id=%s
              AND active=TRUE
        """, (user_id,))

        if cursor.fetchone():
            raise UserBannedError()

        db_execute("""
            SELECT id
            FROM withdraw_queue
            WHERE user_id=%s
              AND status IN (0, 1)
            ORDER BY id ASC
            LIMIT 1
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
                user_id,
                target_username,
                amount,
                withdraw_type,
                status,
                created_time
            )
            VALUES (
                %s, %s, %s, %s, 0,
                EXTRACT(EPOCH FROM clock_timestamp())
            )
            RETURNING id
        """, (
            user_id,
            target_username,
            amount,
            withdraw_type,
        ))

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
        WHERE user_id=%s
          AND finished=FALSE
    """, (user_id,))

    db.commit()
    queue_wakeup.set()


def get_queue_candidates():
    rows = fetch_all("""
        SELECT q.id
        FROM withdraw_queue AS q

        LEFT JOIN bot_withdraw_queue_messages AS n
          ON n.request_id=q.id

        WHERE (
            q.status IN (0, 1)
            OR (
                n.request_id IS NOT NULL
                AND n.finished=FALSE
                AND q.status IN (2, 3, 4)
            )
        )

        AND NOT EXISTS (
            SELECT 1
            FROM mio_user_bans AS b
            WHERE b.user_id=q.user_id
              AND b.active=TRUE
        )

        AND COALESCE(n.retry_at, 0) <=
            EXTRACT(EPOCH FROM clock_timestamp())

        ORDER BY COALESCE(n.retry_at, 0), q.id
        LIMIT 100
    """)

    return [row[0] for row in rows]


def get_queue_info(request_id):
    db_execute("""
        SELECT
            q.id AS request_id,
            q.user_id,
            q.target_username,
            q.amount,
            q.withdraw_type,
            q.status,

            EXTRACT(
                EPOCH FROM clock_timestamp()
            ) AS server_now,

            COALESCE(
                own_limit.delay_seconds,
                CASE
                    WHEN q.withdraw_type='card' THEN 300
                    ELSE 35
                END
            ) AS delay_seconds,

            COALESCE(
                own_limit.next_allowed_at, 0
            ) AS own_ready_at,

            COALESCE(
                normal_limit.next_allowed_at, 0
            ) AS normal_ready_at,

            counts.waiting_ahead,
            counts.processing_ahead,
            counts.queue_total,

            EXISTS(
                SELECT 1
                FROM withdraw_queue AS active
                WHERE active.status=1
                  AND active.id <> q.id
            ) AS another_processing,

            n.telegram_message_id,
            COALESCE(n.last_text, '') AS last_text,
            n.last_status,
            COALESCE(n.finished, FALSE) AS finished,
            COALESCE(n.retry_at, 0) AS retry_at

        FROM withdraw_queue AS q

        LEFT JOIN withdraw_transfer_limits AS own_limit
          ON own_limit.method=(
              CASE
                  WHEN q.withdraw_type='card' THEN 'card'
                  ELSE 'normal'
              END
          )

        LEFT JOIN withdraw_transfer_limits AS normal_limit
          ON normal_limit.method='normal'

        LEFT JOIN bot_withdraw_queue_messages AS n
          ON n.request_id=q.id

        CROSS JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (
                    WHERE other.status=0
                      AND other.id < q.id
                ) AS waiting_ahead,

                COUNT(*) FILTER (
                    WHERE other.status=1
                      AND other.id <> q.id
                      AND (
                          q.status=0
                          OR other.id < q.id
                      )
                ) AS processing_ahead,

                COUNT(*) AS queue_total

            FROM withdraw_queue AS other

            WHERE other.status IN (0, 1)

              AND (
                  CASE
                      WHEN other.withdraw_type='card'
                      THEN 'card'
                      ELSE 'normal'
                  END
              ) = (
                  CASE
                      WHEN q.withdraw_type='card'
                      THEN 'card'
                      ELSE 'normal'
                  END
              )
        ) AS counts

        WHERE q.id=%s
    """, (request_id,))

    row = cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    db.commit()

    if row is None:
        return None

    info = dict(zip(columns, row))

    for field in (
        "amount",
        "status",
        "delay_seconds",
        "waiting_ahead",
        "processing_ahead",
        "queue_total",
    ):
        info[field] = int(info[field] or 0)

    for field in (
        "server_now",
        "own_ready_at",
        "normal_ready_at",
        "retry_at",
    ):
        info[field] = float(info[field] or 0)

    status = info["status"]

    if status == 0:
        info["queue_position"] = (
            info["waiting_ahead"]
            + info["processing_ahead"]
            + 1
        )

    elif status == 1:
        info["queue_position"] = (
            info["processing_ahead"] + 1
        )

    else:
        info["queue_position"] = None

    ready_at = info["own_ready_at"]

    if info["withdraw_type"] == "card":
        ready_at = max(
            ready_at,
            info["normal_ready_at"],
        )

    cooldown = max(
        0,
        math.ceil(ready_at - info["server_now"]),
    )

    info["wait_seconds"] = (
        cooldown
        + info["waiting_ahead"] * info["delay_seconds"]
        if status == 0
        else 0
    )

    return info


def ensure_queue_message_record(info):
    db_execute("""
        INSERT INTO bot_withdraw_queue_messages(
            request_id,
            user_id
        )
        VALUES (%s, %s)
        ON CONFLICT (request_id) DO NOTHING
    """, (
        info["request_id"],
        info["user_id"],
    ))

    db.commit()


def save_queue_message(info, message_id, text):
    terminal = info["status"] in (2, 3, 4)

    db_execute("""
        INSERT INTO bot_withdraw_queue_messages(
            request_id,
            user_id,
            telegram_message_id,
            last_text,
            last_status,
            finished,
            retry_at,
            updated_at
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
        terminal,
        QUEUE_UPDATE_INTERVAL,
    ))

    db.commit()


def postpone_queue_message(request_id, seconds):
    db_execute("""
        UPDATE bot_withdraw_queue_messages
        SET retry_at=(
            EXTRACT(EPOCH FROM clock_timestamp()) + %s
        )
        WHERE request_id=%s
    """, (
        max(1, math.ceil(seconds)),
        request_id,
    ))

    db.commit()


def finish_missing_queue_message(request_id, status):
    db_execute("""
        UPDATE bot_withdraw_queue_messages
        SET
            finished=TRUE,
            last_status=%s,
            updated_at=EXTRACT(
                EPOCH FROM clock_timestamp()
            )
        WHERE request_id=%s
    """, (status, request_id))

    db.commit()


# ================= QUEUE TEXT =================

def get_wait_text(info):
    if info["status"] == 1:
        return "برداشت شما اکنون در حال پردازش است."

    seconds = info["wait_seconds"]

    if seconds > 0:
        return (
            f"حدود {human_duration(seconds)} "
            "تا شروع پردازش"
        )

    if info["another_processing"]:
        return (
            "در انتظار پایان برداشت در حال پردازش؛ "
            "زمان دقیق شروع هنوز مشخص نیست."
        )

    return (
        "نوبت شما نزدیک است؛ "
        "در انتظار شروع پردازش."
    )


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
        return "\n".join(lines)

    if status in (3, 4):
        lines.extend([
            "❌ برداشت ناموفق بود.",
            "اطلاعات مقصد را بررسی کنید و دوباره تلاش کنید.",
        ])
        return "\n".join(lines)

    if status not in (0, 1):
        lines.append(
            "وضعیت درخواست نیاز به بررسی دارد."
        )
        return "\n".join(lines)

    position = info["queue_position"]

    queue_name = (
        "برداشت بانکی"
        if info["withdraw_type"] == "card"
        else "برداشت آیدی و گپی"
    )

    if status == 1:
        lines.extend([
            f"👤 نوبت فعلی شما: {persian_number(position)}",
            "",
            "🔄 نوبت شما رسیده؛ برداشت در حال پردازش است.",
            "",
            "نتیجه پس از پایان پردازش اعلام می‌شود.",
        ])
        return "\n".join(lines)

    lines.extend([
        f"👤 شما نفر {persian_number(position)} در صف هستید.",
        "",
        f"📋 صف: {queue_name}",
        "⏳ درخواست‌های منتظر جلوتر از شما: "
        f"{persian_number(info['waiting_ahead'])}",
        "🔄 درخواست‌های در حال پردازش جلوتر از شما: "
        f"{persian_number(info['processing_ahead'])}",
        "👥 کل درخواست‌های فعال این صف: "
        f"{persian_number(info['queue_total'])}",
        "",
        "⏱ زمان تقریبی:",
        get_wait_text(info),
        "",
        "با پایان برداشت‌های جلوتر، "
        "شماره نوبت در همین پیام به‌روزرسانی می‌شود.",
        "زمان تخمینی است و ممکن است تغییر کند.",
    ])

    return "\n".join(lines)


# ================= QUEUE TELEGRAM UPDATES =================

async def sync_queue_message(request_id, force=False):
    async with queue_message_lock:
        return await _sync_queue_message(
            request_id,
            force=force,
        )


async def _sync_queue_message(request_id, force=False):
    global queue_telegram_pause_until

    info = get_queue_info(request_id)

    if not info:
        return False

    if is_user_banned(info["user_id"]):
        finish_missing_queue_message(
            request_id,
            info["status"],
        )
        return False

    ensure_queue_message_record(info)

    if (
        not force
        and info["retry_at"] > info["server_now"]
    ):
        return False

    text = queue_message_text(info)
    message_id = info["telegram_message_id"]
    terminal = info["status"] in (2, 3, 4)

    if terminal and not message_id and not force:
        finish_missing_queue_message(
            request_id,
            info["status"],
        )
        return False

    if message_id and text == info["last_text"]:
        save_queue_message(info, message_id, text)
        return True

    pause_remaining = (
        queue_telegram_pause_until
        - time.monotonic()
    )

    if pause_remaining > 0:
        postpone_queue_message(
            request_id,
            pause_remaining,
        )
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
                        finish_missing_queue_message(
                            request_id,
                            info["status"],
                        )
                        return False

                    if is_user_banned(info["user_id"]):
                        return False

                    sent = await bot.send_message(
                        info["user_id"],
                        text,
                    )

                    message_id = sent.message_id

                else:
                    raise

        else:
            sent = await bot.send_message(
                info["user_id"],
                text,
            )
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

        logger.warning(
            "محدودیت پیام صف؛ request=%s delay=%s",
            request_id,
            delay,
        )

    except TelegramForbiddenError:
        if terminal:
            finish_missing_queue_message(
                request_id,
                info["status"],
            )
        else:
            postpone_queue_message(
                request_id,
                QUEUE_FORBIDDEN_RETRY,
            )

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
            "خطای پیام صف؛ request=%s error=%s",
            request_id,
            error,
        )

    return False


async def show_request_queue_now(request_id):
    try:
        return await sync_queue_message(
            request_id,
            force=True,
        )

    except Exception:
        rollback_db()
        logger.exception(
            "خطا در نمایش فوری نوبت؛ request=%s",
            request_id,
        )
        return False

    finally:
        queue_wakeup.set()


async def check_withdraw_queue():
    while True:
        queue_wakeup.clear()

        try:
            if time.monotonic() >= queue_telegram_pause_until:
                request_ids = get_queue_candidates()

                for request_id in request_ids:
                    if time.monotonic() < queue_telegram_pause_until:
                        break

                    try:
                        await sync_queue_message(request_id)

                    except Exception:
                        rollback_db()
                        logger.exception(
                            "خطا در بررسی صف؛ request=%s",
                            request_id,
                        )

                        try:
                            postpone_queue_message(
                                request_id,
                                QUEUE_ERROR_RETRY,
                            )
                        except Exception:
                            rollback_db()

                    await asyncio.sleep(1.1)

        except Exception:
            rollback_db()
            logger.exception("خطا در حلقه نمایش صف")

        try:
            await asyncio.wait_for(
                queue_wakeup.wait(),
                timeout=QUEUE_POLL_INTERVAL,
            )
        except asyncio.TimeoutError:
            pass


# ================= SHOW MENU =================

async def show_menu(message, user_id=None):
    if user_id is None:
        user_id = message.from_user.id

    await message.answer(
        "🐾 Mio Point\n\n"
        "💎 موجودی فعلی شما:\n\n"
        f"{get_balance(user_id):,} میو\n\n"
        "انتخاب کنید:",
        reply_markup=menu_keyboard(),
    )


# ================= ADMIN PRIORITY COMMANDS =================

@dp.message(
    F.from_user.id == ADMIN_USER_ID,
    lambda m: admin_command_key(m.text) is not None,
)
async def admin_priority_commands(message: types.Message):
    global admin_broadcast

    if message.chat.type != "private":
        await message.answer(
            "دستورهای مدیریت را در خصوصی ربات بفرست."
        )
        return

    key = admin_command_key(message.text)
    target = parse_find_user_command(message.text)

    clear_input_state(message.from_user.id)

    if key == "find_user":
        try:
            await find_admin_user(message, target)

        except Exception:
            rollback_db()
            logger.exception("جست‌وجوی کاربر انجام نشد")

            await message.answer(
                "❌ جست‌وجو انجام نشد؛ دوباره تلاش کن."
            )
        return

    if key == "users":
        await show_admin_report(message, "u")
        return

    if key == "withdrawals":
        await show_admin_report(message, "w")
        return

    if key == "broadcast":
        admin_broadcast = True

        await message.answer(
            "✉️ پیام خود را وارد کنید:\n\n"
            "برای انصراف، «لغو» را بفرست."
        )
        return

    if key == "daily_all":
        await message.answer(
            "⏳ در حال واریز میو روزانه…"
        )

        count = await give_daily_mio()

        await message.answer(
            "✅ انجام شد\n\n"
            "🎁 میو روزانه پردازش شد\n\n"
            f"👥 تعداد: {count}"
        )
        return

    if key == "cancel":
        await message.answer(
            "✅ عملیات ورودی لغو شد."
        )
        await show_menu(message, message.from_user.id)


@dp.callback_query(F.data.startswith("ap:"))
async def admin_report_callback(call: CallbackQuery):
    if call.from_user.id != ADMIN_USER_ID:
        await call.answer("دسترسی ندارید.", show_alert=True)
        return

    if not call.message or call.message.chat.type != "private":
        await call.answer(
            "گزارش فقط در خصوصی ربات نمایش داده می‌شود.",
            show_alert=True,
        )
        return

    try:
        prefix, kind, page_text, until_text = call.data.split(":")

        if prefix != "ap" or kind not in ("u", "w", "a"):
            raise ValueError

        page = int(page_text)
        until = int(until_text)

        if not 0 <= page <= 1_000_000:
            raise ValueError

        if not 0 <= until <= 10_000_000_000:
            raise ValueError

    except (ValueError, TypeError):
        await call.answer(
            "دکمهٔ گزارش معتبر نیست.",
            show_alert=True,
        )
        return

    await call.answer()

    await show_admin_report(
        call.message,
        kind,
        page=page,
        until=until,
        edit=True,
    )


# ================= BAN BUTTONS =================

@dp.callback_query(F.data.startswith("ub:"))
async def admin_user_ban_callback(call: CallbackQuery):
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
        parts = call.data.split(":")

        if len(parts) not in (3, 4):
            raise ValueError

        action = parts[1]
        user_id = int(parts[2])

        if not 0 <= user_id <= 9223372036854775807:
            raise ValueError

        if action not in ("back", "ask", "confirm", "cancel"):
            raise ValueError

    except (ValueError, TypeError, IndexError):
        await call.answer(
            "دکمه معتبر نیست.",
            show_alert=True,
        )
        return

    if action == "back":
        clear_input_state(ADMIN_USER_ID)
        await call.answer()

        await edit_report_message(
            call.message,
            "برای پیدا کردن کاربر یکی از این دستورها را بفرست:\n\n"
            "@username پیداش کن\n"
            "828292228 پیداش کن",
            None,
        )
        return

    if user_id == ADMIN_USER_ID:
        await call.answer(
            "حساب ادمین قابل بن شدن نیست.",
            show_alert=True,
        )
        return

    row = read_registered_user(user_id)

    if not row:
        await call.answer(
            "کاربر در دیتابیس پیدا نشد.",
            show_alert=True,
        )
        return

    if action == "ask":
        if is_user_banned(user_id):
            await call.answer(
                "این کاربر قبلاً مسدود شده است.",
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
        }

        await call.answer()

        await edit_report_message(
            call.message,
            "🚫 آیا مطمئن هستید این کاربر بن شود؟\n\n"
            f"نام: {report_text(row['first_name'], 100)}\n"
            f"یوزرنیم: {username_text(row['username'])}\n"
            f"🆔 آیدی عددی: {user_id}\n"
            f"💎 موجودی: {int(row['balance'] or 0):,} میو",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="✅ تأیید بن",
                            callback_data=f"ub:confirm:{user_id}:{nonce}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="❌ لغو",
                            callback_data=f"ub:cancel:{user_id}:{nonce}",
                        )
                    ],
                ]
            ),
        )
        return

    pending = admin_pending_ban.get(ADMIN_USER_ID)
    nonce = parts[3] if len(parts) == 4 else ""

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
        await call.answer("عملیات بن لغو شد.")

        await show_admin_user(
            call.message,
            user_id,
            edit=True,
        )
        return

    if action == "confirm":
        if pending["stage"] != "confirm":
            await call.answer(
                "دلیل بن را به‌صورت پیام متنی بفرست.",
                show_alert=True,
            )
            return

        if is_user_banned(user_id):
            clear_input_state(ADMIN_USER_ID)

            await call.answer(
                "این کاربر قبلاً مسدود شده است.",
                show_alert=True,
            )
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
            "برای انصراف، «لغو» را بفرست.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="❌ لغو بن",
                            callback_data=f"ub:cancel:{user_id}:{nonce}",
                        )
                    ]
                ]
            ),
        )


# ================= START =================

@dp.message(Command("start"))
async def start(message: types.Message):
    user = message.from_user
    referrer_id = 0

    args = (message.text or "").split()

    if len(args) > 1:
        try:
            referrer_id = int(args[1])
        except ValueError:
            referrer_id = 0

    add_user(
        user.id,
        user.username,
        user.first_name,
        referrer_id,
    )

    clear_input_state(user.id)
    request_queue_update(user.id)

    await show_menu(message, user.id)


@dp.message(Command("meno"))
async def meno(message: types.Message):
    user_id = message.from_user.id

    clear_input_state(user_id)
    request_queue_update(user_id)

    await show_menu(message, user_id)


# ================= RECEIVE BAN REASON =================

@dp.message(
    F.from_user.id == ADMIN_USER_ID,
    lambda m: (
        admin_pending_ban.get(ADMIN_USER_ID, {}).get("stage")
        == "reason"
    ),
)
async def receive_ban_reason(message: types.Message):
    if message.chat.type != "private":
        return

    pending = admin_pending_ban.get(ADMIN_USER_ID)

    if not pending or pending["stage"] != "reason":
        return

    reason = (message.text or "").strip()

    if not reason:
        await message.answer(
            "دلیل بن را به‌صورت پیام متنی بفرست.\n"
            "برای انصراف «لغو» را بفرست."
        )
        return

    if len(reason) > BAN_REASON_MAX_LENGTH:
        await message.answer(
            f"دلیل بن باید حداکثر {BAN_REASON_MAX_LENGTH} کاراکتر باشد."
        )
        return

    user_id = pending["target_id"]

    try:
        result = ban_registered_user(
            user_id,
            reason,
            ADMIN_USER_ID,
        )

    except Exception:
        rollback_db()

        logger.exception(
            "ثبت بن انجام نشد؛ user_id=%s",
            user_id,
        )

        await message.answer(
            "❌ ثبت بن انجام نشد؛ دوباره دلیل را بفرست "
            "یا برای انصراف «لغو» را بزن."
        )
        return

    clear_input_state(ADMIN_USER_ID)

    if not result["found"]:
        await message.answer(
            "❌ کاربر دیگر در دیتابیس موجود نیست."
        )
        return

    if result["already_banned"]:
        await message.answer(
            "این کاربر قبلاً مسدود شده است."
        )
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
        "📨 ارسال پیام‌های این بات به کاربر متوقف شد.",
        f"📤 درخواست‌های منتظر لغوشده: {result['cancelled']}",
        "🚫 ثبت برداشت جدید از بات و مینی‌اپ مسدود شد.",
    ]

    if result["processing"]:
        lines.extend([
            "",
            f"⚠️ {result['processing']} برداشت از قبل "
            "در حال پردازش بوده است.",
            "توقف انتقالی که یوزربات قبلاً شروع کرده "
            "از این فایل قابل تضمین نیست.",
        ])

    await message.answer(
        "\n".join(lines),
        parse_mode=None,
    )


# ================= MAIN MENU =================

@dp.callback_query(F.data == "main_menu")
async def main_menu(call: CallbackQuery):
    clear_input_state(call.from_user.id)

    await call.answer()

    try:
        await call.message.delete()
    except TelegramBadRequest:
        pass

    await show_menu(call.message, call.from_user.id)


# ================= BALANCE =================

@dp.callback_query(F.data == "balance")
async def balance(call: CallbackQuery):
    amount = get_balance(call.from_user.id)

    await call.answer()

    await call.message.edit_text(
        f"💎 موجودی شما:\n\n{amount:,} میو",
        reply_markup=back_keyboard(),
    )


# ================= INVITE =================

@dp.callback_query(F.data == "invite")
async def invite(call: CallbackQuery):
    await call.answer()

    bot_info = await bot.get_me()

    link = (
        f"https://t.me/{bot_info.username}"
        f"?start={call.from_user.id}"
    )

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
    waiting_withdraw_id.discard(call.from_user.id)
    waiting_bank_card.discard(call.from_user.id)

    await call.answer()

    await call.message.edit_text(
        "📤 برداشت میو\n\n"
        "روش قدیمی:\n\n"
        "1️⃣ وارد گپ شوید:\n\n"
        "https://t.me/panke_saghfe\n\n"
        "2️⃣ دستور زیر را بزنید:\n\n"
        "میوهام رو بده\n\n"
        "یا:\n\n"
        "میو هام رو بده\n\n"
        "یا بدون عضویت در گپ:\n\n"
        "📤 برداشت با آیدی",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📤 برداشت با آیدی (بدون عضویت گپ)",
                        callback_data="withdraw_by_id",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🏦 برداشت با بانک میویی",
                        callback_data="withdraw_bank",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🔙 برگشت",
                        callback_data="main_menu",
                    )
                ],
            ]
        ),
    )


@dp.callback_query(F.data == "withdraw_by_id")
async def withdraw_by_id(call: CallbackQuery):
    await call.answer()

    await call.message.edit_text(
        "📤 برداشت با آیدی\n\n"
        "بدون نیاز به عضویت گپ،\n"
        "می‌توانید درخواست برداشت ثبت کنید.\n\n"
        "⚠️ آیدی مقصد باید با @ شروع شود.\n\n"
        "مثال:\n\n"
        "@username",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ شروع",
                        callback_data="start_withdraw_by_id",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🔙 برگشت",
                        callback_data="withdraw",
                    )
                ],
            ]
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
            f"شما نفر {persian_number(info['queue_position'])} "
            "در صف هستید."
        )

    elif info and info["status"] == 1:
        notice = (
            "🔄 نوبت شما رسیده؛ برداشت در حال پردازش است."
        )

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
        await call.answer(
            "❌ موجودی شما صفر است.",
            show_alert=True,
        )
        return

    waiting_bank_card.discard(user_id)
    waiting_withdraw_id.add(user_id)

    await call.answer()

    await call.message.edit_text(
        "📤 آیدی تلگرام مقصد را وارد کنید:\n\n"
        "⚠️ با @ شروع شود.\n\n"
        "مثال:\n\n"
        "@username"
    )


# ================= RECEIVE USERNAME =================

@dp.message(
    lambda m: (
        m.from_user is not None
        and m.from_user.id in waiting_withdraw_id
    )
)
async def receive_withdraw_username(message: types.Message):
    user_id = message.from_user.id
    target = (message.text or "").strip()

    if not re.fullmatch(r"@[A-Za-z0-9_]{1,32}", target):
        await message.answer(
            "❌ آیدی اشتباه است.\n\n"
            "آیدی معتبر با @ وارد کنید.\n\n"
            "مثال: @username"
        )
        return

    try:
        request_id, created = add_withdraw_queue(
            user_id,
            target,
            "id",
        )

    except UserBannedError:
        waiting_withdraw_id.discard(user_id)
        banned = get_user_ban(user_id)

        await message.answer(
            ban_notice(
                banned["reason"] if banned else "حساب مسدود است."
            ),
            parse_mode=None,
        )
        return

    except Exception:
        rollback_db()
        logger.exception("خطا در ثبت برداشت آیدی")

        await message.answer(
            "ارتباط با دیتابیس کامل نشد؛ "
            "چند لحظه بعد دوباره بررسی کنید."
        )
        return

    waiting_withdraw_id.discard(user_id)

    if request_id is None:
        await message.answer(
            "❌ موجودی قابل برداشت ندارید؛ "
            "در صورت نیاز ابتدا /start را بزنید."
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
            + "ارسال پیام نوبت فعلاً انجام نشد؛ "
            "بات دوباره آن را بررسی می‌کند."
        )


# ================= BANK WITHDRAW =================

@dp.callback_query(F.data == "withdraw_bank")
async def withdraw_bank(call: CallbackQuery):
    user_id = call.from_user.id

    if await pending_callback_notice(call):
        return

    amount = get_balance(user_id)

    if amount <= 0:
        await call.answer(
            "❌ موجودی شما صفر است.",
            show_alert=True,
        )
        return

    await call.answer()

    await call.message.edit_text(
        "🏦 برداشت با بانک میویی\n\n"
        "شما می‌توانید:\n\n"
        f"{amount:,} میو\n\n"
        "از موجودیتان را با بانک میویی برداشت کنید.\n\n"
        "برای ادامه روی تایید بزنید.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ تایید",
                        callback_data="confirm_bank_withdraw",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="❌ لغو",
                        callback_data="withdraw",
                    )
                ],
            ]
        ),
    )


@dp.callback_query(F.data == "confirm_bank_withdraw")
async def confirm_bank_withdraw(call: CallbackQuery):
    user_id = call.from_user.id

    if await pending_callback_notice(call):
        return

    if get_balance(user_id) <= 0:
        await call.answer(
            "❌ موجودی شما صفر است.",
            show_alert=True,
        )
        return

    waiting_withdraw_id.discard(user_id)
    waiting_bank_card.add(user_id)

    await call.answer()

    await call.message.edit_text(
        "🏦 شماره کارت میوییتان را وارد کنید:\n\n"
        "⚠️ فقط عدد، بدون فاصله یا خط‌تیره.\n\n"
        "مثال:\n\n"
        "232491826963"
    )


@dp.message(
    lambda m: (
        m.from_user is not None
        and m.from_user.id in waiting_bank_card
    )
)
async def receive_bank_card(message: types.Message):
    user_id = message.from_user.id
    card = english_digits((message.text or "").strip())

    if not re.fullmatch(r"[0-9]{10,20}", card):
        await message.answer(
            "❌ شماره کارت میویی اشتباه است.\n\n"
            "فقط باید عدد باشد (بین ۱۰ تا ۲۰ رقم)."
        )
        return

    try:
        request_id, created = add_withdraw_queue(
            user_id,
            card,
            "card",
        )

    except UserBannedError:
        waiting_bank_card.discard(user_id)
        banned = get_user_ban(user_id)

        await message.answer(
            ban_notice(
                banned["reason"] if banned else "حساب مسدود است."
            ),
            parse_mode=None,
        )
        return

    except Exception:
        rollback_db()
        logger.exception("خطا در ثبت برداشت بانک")

        await message.answer(
            "ارتباط با دیتابیس کامل نشد؛ "
            "چند لحظه بعد دوباره بررسی کنید."
        )
        return

    waiting_bank_card.discard(user_id)

    if request_id is None:
        await message.answer(
            "❌ موجودی قابل برداشت ندارید؛ "
            "در صورت نیاز ابتدا /start را بزنید."
        )
        return

    shown = await show_request_queue_now(request_id)

    if not shown and not is_user_banned(user_id):
        await message.answer(
            (
                "✅ درخواست برداشت بانکی ثبت شد.\n\n"
                if created
                else "⏳ درخواست قبلی شما هنوز فعال است.\n\n"
            )
            + "ارسال پیام نوبت فعلاً انجام نشد؛ "
            "بات دوباره آن را بررسی می‌کند."
        )


# ================= WITHDRAW SUCCESS CHECK =================

async def check_withdraw_success():
    while True:
        try:
            users = fetch_all("""
                SELECT user_id
                FROM withdraw_success
                WHERE status=1
            """)

            for row in users:
                user_id = row[0]

                if user_id in sending_success:
                    continue

                try:
                    sending_success.add(user_id)

                    db_execute("""
                        UPDATE withdraw_success
                        SET status=2
                        WHERE user_id=%s
                          AND status=1
                    """, (user_id,))

                    claimed = cursor.rowcount
                    db.commit()

                    if not claimed:
                        continue

                    # تسویهٔ نتیجهٔ انتقال قبلی همچنان انجام می‌شود.
                    reset_balance(user_id)

                    if not is_user_banned(user_id):
                        await bot.send_message(
                            user_id,
                            "🎉 میوهای شما واریز شد 🫷✅\n\n"
                            "💎 موجودی حساب شما:\n\n"
                            "0 میو\n\n"
                            "آیا مایل هستید رسید انتقال "
                            "برای شما ارسال شود؟",
                            reply_markup=receipt_keyboard(),
                        )

                    db_execute("""
                        UPDATE withdraw_success
                        SET status=0
                        WHERE user_id=%s
                          AND status=2
                    """, (user_id,))

                    db.commit()
                    queue_wakeup.set()

                except Exception:
                    rollback_db()
                    logger.exception(
                        "خطا در موفقیت برداشت؛ user_id=%s",
                        user_id,
                    )

                finally:
                    sending_success.discard(user_id)

        except Exception:
            rollback_db()
            logger.exception("خطا در حلقه موفقیت برداشت")

        await asyncio.sleep(5)


# ================= FAILED WITHDRAW CHECK =================

async def check_failed_withdraw():
    while True:
        try:
            failed = fetch_all("""
                SELECT id, user_id
                FROM withdraw_queue
                WHERE status=3
            """)

            for request_id, user_id in failed:
                if request_id in sending_failed:
                    continue

                try:
                    sending_failed.add(request_id)

                    db_execute("""
                        UPDATE withdraw_queue
                        SET status=4
                        WHERE id=%s
                          AND status=3
                    """, (request_id,))

                    claimed = cursor.rowcount
                    db.commit()

                    if not claimed:
                        continue

                    if not is_user_banned(user_id):
                        await bot.send_message(
                            user_id,
                            "😔 دوست من متاسفانه انتقال "
                            "با مشکل مواجه شد.\n\n"
                            "دوباره تلاش کن.\n\n"
                            "⚠️ اطلاعات وارد شده را بررسی کن.\n\n"
                            "اگر مشکل از سرورهای ما باشد،\n"
                            "به زودی حل خواهد شد.",
                        )

                    queue_wakeup.set()

                except Exception:
                    rollback_db()
                    logger.exception(
                        "خطا در پیام شکست؛ request=%s",
                        request_id,
                    )

                finally:
                    sending_failed.discard(request_id)

        except Exception:
            rollback_db()
            logger.exception("خطا در حلقه شکست برداشت")

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
        "به اکانت:\n\n"
        "@Pott5y6jg\n\n"
        "پیام بده:\n\n"
        "رسیدم رو بده\n\n"
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

def enable_daily_mio(user_id):
    db_execute("""
        UPDATE users AS u
        SET daily_mio=1

        WHERE u.user_id=%s

          AND NOT EXISTS (
              SELECT 1
              FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id
                AND b.active=TRUE
          )
    """, (user_id,))

    db.commit()


def is_daily_enabled(user_id):
    row = fetch_one("""
        SELECT daily_mio
        FROM users
        WHERE user_id=%s
    """, (user_id,))

    return bool(row and row[0] == 1)


def credit_daily_user(user_id):
    db_execute("""
        UPDATE users AS u

        SET balance=LEAST(
            500000,
            COALESCE(balance, 0) + 340000
        )

        WHERE u.user_id=%s
          AND u.daily_mio=1

          AND NOT EXISTS (
              SELECT 1
              FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id
                AND b.active=TRUE
          )

        RETURNING balance
    """, (user_id,))

    row = cursor.fetchone()
    db.commit()

    return int(row[0]) if row else None


@dp.callback_query(F.data == "daily_mio")
async def daily_mio(call: CallbackQuery):
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
            "با فعال کردن این قابلیت:\n\n"
            "💎 هر روز 340,000 میو دریافت می‌کنید.\n\n"
            "برای فعال‌سازی روی دکمه زیر بزنید.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="✅ فعال کن",
                            callback_data="confirm_daily",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="🔙 برگشت",
                            callback_data="main_menu",
                        )
                    ],
                ]
            ),
        )


@dp.callback_query(F.data == "confirm_daily")
async def confirm_daily(call: CallbackQuery):
    user_id = call.from_user.id

    if is_daily_enabled(user_id):
        await call.answer(
            "قبلاً فعال شده",
            show_alert=True,
        )
        return

    enable_daily_mio(user_id)
    new_balance = add_balance(user_id, 340000)

    await call.answer()

    await call.message.edit_text(
        "🎉 فعال شد\n\n"
        "🎁 پاداش فعال‌سازی:\n\n"
        "340,000 میو\n\n"
        "💰 موجودی شما:\n\n"
        f"{new_balance:,} میو",
        reply_markup=menu_keyboard(),
    )


async def give_daily_mio():
    users = fetch_all("""
        SELECT u.user_id
        FROM users AS u

        WHERE u.daily_mio=1

          AND NOT EXISTS (
              SELECT 1
              FROM mio_user_bans AS b
              WHERE b.user_id=u.user_id
                AND b.active=TRUE
          )
    """)

    count = 0

    for row in users:
        user_id = row[0]

        try:
            new_balance = credit_daily_user(user_id)

        except Exception:
            rollback_db()
            logger.exception(
                "خطا در واریز روزانه؛ user_id=%s",
                user_id,
            )
            continue

        if new_balance is None:
            continue

        count += 1

        try:
            if is_user_banned(user_id):
                continue

            await bot.send_message(
                user_id,
                "🎁 میو روزانه شما واریز شد 🫷✅\n\n"
                "💎 مقدار:\n\n"
                "340,000 میو",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="✅ تایید",
                                callback_data="daily_confirm",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text="🏠 بازگشت به منوی اصلی",
                                callback_data="main_menu",
                            )
                        ],
                    ]
                ),
            )

        except Exception:
            rollback_db()
            logger.exception(
                "خطا در ارسال میو روزانه؛ user_id=%s",
                user_id,
            )

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

    # ================= BROADCAST =================

    if admin_broadcast:
        if not text:
            await message.answer(
                "پیام متنی بفرست یا «لغو» را بزن."
            )
            return

        admin_broadcast = False

        users = fetch_all("""
            SELECT u.user_id, u.username
            FROM users AS u

            WHERE NOT EXISTS (
                SELECT 1
                FROM mio_user_bans AS b
                WHERE b.user_id=u.user_id
                  AND b.active=TRUE
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

                await bot.send_message(uid, text)
                delivered = True

            except TelegramRetryAfter as error:
                await asyncio.sleep(
                    int(error.retry_after) + 1
                )

                try:
                    # ممکن است کاربر هنگام انتظار بن شده باشد.
                    if is_user_banned(uid):
                        excluded = True
                    else:
                        await bot.send_message(uid, text)
                        delivered = True

                except Exception:
                    rollback_db()
                    logger.exception(
                        "ارسال مجدد پیام همگانی ناموفق؛ uid=%s",
                        uid,
                    )

            except Exception:
                rollback_db()
                logger.exception(
                    "پیام همگانی ناموفق؛ uid=%s",
                    uid,
                )

            if excluded:
                skipped += 1
            elif delivered:
                sent += 1
            else:
                failed_users.append((uid, username))

            await asyncio.sleep(0.05)

        await report_broadcast_result(
            message,
            sent,
            failed_users,
            skipped,
        )
        return

    # ================= ADD BALANCE VALUE =================

    if message.from_user.id in admin_add_balance:
        try:
            amount = int(english_digits(text))

        except ValueError:
            await message.answer(
                "❌ مقدار باید عدد باشد.\n"
                "برای انصراف «لغو» را بفرست."
            )
            return

        user_id = admin_add_balance[message.from_user.id]

        if is_user_banned(user_id):
            admin_add_balance.pop(message.from_user.id, None)

            await message.answer(
                "🚫 این کاربر مسدود است؛ افزایش موجودی انجام نشد."
            )
            return

        try:
            new_balance = add_balance(user_id, amount)

        except UserBannedError:
            admin_add_balance.pop(message.from_user.id, None)

            await message.answer(
                "🚫 این کاربر مسدود شده است؛ افزایش موجودی انجام نشد."
            )
            return

        del admin_add_balance[message.from_user.id]

        try:
            if is_user_banned(user_id):
                await message.answer(
                    "تغییر موجودی ثبت شد؛ "
                    "به دلیل بن کاربر، اعلان ارسال نشد."
                )
                return

            await bot.send_message(
                user_id,
                "🎉 موجودی شما افزایش یافت\n\n"
                "💎 مقدار:\n\n"
                f"{amount:,} میو\n\n"
                "💰 موجودی جدید:\n\n"
                f"{new_balance:,} میو",
            )

        except Exception:
            rollback_db()
            logger.exception(
                "اعلان افزایش موجودی ناموفق؛ uid=%s",
                user_id,
            )

            await message.answer(
                "✅ تغییر موجودی ثبت شد؛ "
                "ارسال پیام به کاربر انجام نشد."
            )
            return

        await message.answer("✅ شارژ انجام شد.")
        return

    # ================= ADD BALANCE REQUEST =================

    normalized = normalize_command(text)

    if normalized.endswith("اضافه"):
        try:
            user_id = int(
                english_digits(normalized.split()[0])
            )

            if not 0 < user_id <= 9223372036854775807:
                raise ValueError

        except (ValueError, IndexError):
            await message.answer(
                "❌ فرمت اشتباه است.\n"
                "مثال:\n123456789 اضافه"
            )
            return

        exists = fetch_one(
            "SELECT 1 FROM users WHERE user_id=%s",
            (user_id,),
        )

        if not exists:
            await message.answer(
                "❌ این کاربر در دیتابیس ثبت نشده است."
            )
            return

        if is_user_banned(user_id):
            await message.answer(
                "🚫 این کاربر مسدود است؛ افزایش موجودی مجاز نیست."
            )
            return

        admin_add_balance[message.from_user.id] = user_id

        await message.answer(
            "💎 مقدار میو را وارد کنید:\n"
            "برای انصراف «لغو» را بفرست."
        )
        return

    await message.answer(
        "دستورهای مدیریت:\n\n"
        "👥 کاربران\n"
        "📤 برداشت‌های اخیر\n"
        "✉️ پیام به کاربران\n"
        "🎁 میو بده به همه\n"
        "💎 آیدی‌عددی اضافه\n\n"
        "🔎 جست‌وجو و مسدود کردن کاربر:\n"
        "@username پیداش کن\n"
        "828292228 پیداش کن\n\n"
        "دستورهای جایگزین:\n"
        "/users\n"
        "/withdrawals\n"
        "/cancel"
    )


# ================= NORMAL USER UNKNOWN MESSAGE =================

@dp.message()
async def unknown_message(message: types.Message):
    if not message.from_user:
        return

    user_id = message.from_user.id

    if user_id == ADMIN_USER_ID:
        return

    if user_id in waiting_withdraw_id:
        return

    if user_id in waiting_bank_card:
        return

    await show_menu(message, user_id)


# ================= RUN =================

async def main():
    logger.info("Mio Point Started 🚀")
    logger.info("به‌روزرسانی نام و یوزرنیم فعال است")
    logger.info("گزارش کاربران و برداشت‌ها صفحه‌بندی شده است")
    logger.info("جست‌وجو و مسدود کردن کاربران فعال است")
    logger.info("محدودیت برداشت و افزایش موجودی در دیتابیس فعال است")
    logger.info("کاربران مسدود از پیام همگانی و میو روزانه حذف می‌شوند")

    background_tasks = [
        asyncio.create_task(check_withdraw_queue()),
        asyncio.create_task(check_withdraw_success()),
        asyncio.create_task(check_failed_withdraw()),
    ]

    try:
        await dp.start_polling(
            bot,
            close_bot_session=False,
        )

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
