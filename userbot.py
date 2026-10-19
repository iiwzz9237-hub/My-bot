from telethon import TelegramClient, events
import psycopg2
import asyncio
import time
import re
import unicodedata

from config import DATABASE_URL_TABLET

================= API =================

API_ID = 35
API_HASH = "fcfe769c0857faked340983"

client = TelegramClient(
"mio_account",
API_ID,
API_HASH
)

================= CONFIG =================

گپ بانک و برداشت گپی

WITHDRAW_GROUP = "panke_saghfe"

گپ برداشت با آیدی

ID_GROUP = "themaws_gap"

ربات بانک و برداشت گپی

MEOWIE = "MeowieQIVBot"

ربات برداشت با آیدی

ID_MEOWIE = "MeowieQBot"

ACTIVE_WITHDRAW = None
LAST_TRANSFER_TIME = 0

فاصله برداشت با آیدی و برداشت گپی

TRANSFER_DELAY = 35

فاصله برداشت بانکی: پنج دقیقه

BANK_TRANSFER_DELAY = 300

CONFIRM_TIMEOUT = 30
CONFIRM_RETRY = 3
CONFIRM_SCAN_INTERVAL = 3

آیدی عددی ربات‌ها بعد از ورود دریافت می‌شود.

MEOWIE_ID = None
ID_MEOWIE_ID = None

ID_GROUP_ID = None
WITHDRAW_GROUP_ID = None

================= DATABASE =================

db = psycopg2.connect(DATABASE_URL_TABLET)
db.autocommit = False
cursor = db.cursor()

cursor.execute("SELECT pg_advisory_xact_lock(748219306)")

cursor.execute("""
CREATE TABLE IF NOT EXISTS withdraw_success(
user_id BIGINT PRIMARY KEY,
status INTEGER DEFAULT 1
)
""")

cursor.execute("""
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

cursor.execute("""
ALTER TABLE withdraw_queue
ADD COLUMN IF NOT EXISTS completed_time DOUBLE PRECISION
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS withdraw_receipts(
user_id BIGINT PRIMARY KEY,
message_id BIGINT,
withdraw_type TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS receipt_request(
user_id BIGINT PRIMARY KEY,
request INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS withdraw_transfer_limits(
method TEXT PRIMARY KEY,
delay_seconds INTEGER NOT NULL CHECK (delay_seconds > 0),
next_allowed_at DOUBLE PRECISION NOT NULL DEFAULT 0,
updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
)
""")

for method, delay in (
("normal", TRANSFER_DELAY),
("card", BANK_TRANSFER_DELAY),
):
cursor.execute("""
INSERT INTO withdraw_transfer_limits(
method,
delay_seconds,
next_allowed_at,
updated_at
)
VALUES (%s, %s, 0, 0)

ON CONFLICT (method) DO UPDATE SET  
        delay_seconds = EXCLUDED.delay_seconds,  

        next_allowed_at = GREATEST(  
            withdraw_transfer_limits.next_allowed_at,  
            CASE  
                WHEN withdraw_transfer_limits.updated_at > 0  
                THEN withdraw_transfer_limits.updated_at  
                     + EXCLUDED.delay_seconds  
                ELSE 0  
            END  
        )  
""", (method, delay))

db.commit()

================= DB HELPER =================

def db_execute(query, params=None):
global db, cursor

try:  
    cursor.execute(query, params or ())  

except (psycopg2.InterfaceError, psycopg2.OperationalError):  
    try:  
        db.close()  
    except Exception:  
        pass  

    db = psycopg2.connect(DATABASE_URL_TABLET)  
    db.autocommit = False  
    cursor = db.cursor()  

    # کوئری نامطمئن خودکار تکرار نمی‌شود.  
    raise

def rollback_db():
try:
db.rollback()
except Exception:
pass

================= BALANCE =================

def get_balance(user_id):
db_execute(
"SELECT balance FROM users WHERE user_id=%s",
(user_id,)
)

row = cursor.fetchone()  
db.commit()  

return row[0] if row else 0

def reset_balance(user_id):
db_execute(
"UPDATE users SET balance=0 WHERE user_id=%s",
(user_id,)
)

db.commit()

================= TRANSFER COOLDOWN =================

def set_transfer_cooldown(withdraw_type):
"""
زمان انتظار در دیتابیس ذخیره می‌شود:

بانک: ۳۰۰ ثانیه  
آیدی و گپی: ۳۵ ثانیه  
"""  
method = "card" if withdraw_type == "card" else "normal"  

delay = (  
    BANK_TRANSFER_DELAY  
    if method == "card"  
    else TRANSFER_DELAY  
)  

db_execute("""  
    INSERT INTO withdraw_transfer_limits(  
        method,  
        delay_seconds,  
        next_allowed_at,  
        updated_at  
    )  
    VALUES (  
        %s,  
        %s,  
        EXTRACT(EPOCH FROM clock_timestamp()) + %s,  
        EXTRACT(EPOCH FROM clock_timestamp())  
    )  

    ON CONFLICT (method) DO UPDATE SET  
        delay_seconds=EXCLUDED.delay_seconds,  

        next_allowed_at=GREATEST(  
            withdraw_transfer_limits.next_allowed_at,  
            EXCLUDED.next_allowed_at  
        ),  

        updated_at=EXCLUDED.updated_at  
""", (method, delay, delay))  

db.commit()

================= QUEUE FUNCTIONS =================

def add_withdraw_queue(
user_id,
target_username,
amount,
withdraw_type,
message_id
):
try:
db_execute("""
SELECT user_id
FROM users
WHERE user_id=%s
FOR UPDATE
""", (user_id,))

if not cursor.fetchone():  
        db.commit()  
        return False  

    db_execute("""  
        SELECT id  
        FROM withdraw_queue  
        WHERE user_id=%s  
          AND status IN (0, 1)  
        LIMIT 1  
    """, (user_id,))  

    if cursor.fetchone():  
        db.commit()  
        return False  

    db_execute("""  
        INSERT INTO withdraw_queue(  
            user_id,  
            target_username,  
            amount,  
            withdraw_type,  
            status,  
            created_time,  
            message_id  
        )  
        VALUES (%s, %s, %s, %s, 0, %s, %s)  
    """, (  
        user_id,  
        target_username,  
        amount,  
        withdraw_type,  
        time.time(),  
        message_id  
    ))  

    db.commit()  
    return True  

except Exception:  
    rollback_db()  
    raise

def set_withdraw_status(request_id, status):
db_execute("""
UPDATE withdraw_queue
SET
status=%s,

completed_time=CASE  
            WHEN %s IN (2, 3, 4)  
            THEN COALESCE(  
                completed_time,  
                EXTRACT(EPOCH FROM clock_timestamp())  
            )  
            ELSE completed_time  
        END  

    WHERE id=%s  
""", (status, status, request_id))  

db.commit()

def get_next_withdraw():
"""
قدیمی‌ترین درخواست آماده انتخاب می‌شود.

اگر محدودیت بانک تمام نشده باشد،  
درخواست آیدی یا گپی آماده می‌تواند پردازش شود.  
"""  
db_execute("""  
    SELECT  
        q.id,  
        q.user_id,  
        q.target_username,  
        q.amount,  
        q.withdraw_type,  
        q.message_id  

    FROM withdraw_queue AS q  

    JOIN withdraw_transfer_limits AS own_limit  
      ON own_limit.method = CASE  
          WHEN q.withdraw_type='card' THEN 'card'  
          ELSE 'normal'  
      END  

    JOIN withdraw_transfer_limits AS normal_limit  
      ON normal_limit.method='normal'  

    WHERE q.status=0  

      AND own_limit.next_allowed_at <=  
          EXTRACT(EPOCH FROM clock_timestamp())  

      AND normal_limit.next_allowed_at <=  
          EXTRACT(EPOCH FROM clock_timestamp())  

    ORDER BY q.id ASC  
    LIMIT 1  
""")  

row = cursor.fetchone()  
db.commit()  

return row

def fail_active_withdraw(job=None):
global ACTIVE_WITHDRAW

job = job or ACTIVE_WITHDRAW  

if not job or ACTIVE_WITHDRAW is not job:  
    return  

set_withdraw_status(job["id"], 3)  

ACTIVE_WITHDRAW = None  

print(  
    f"❌ برداشت ناموفق ثبت شد؛ "  
    f"درخواست {job['id']}"  
)

def save_success(
request_id,
user_id,
message_id,
withdraw_type
):
try:
db_execute("""
INSERT INTO withdraw_receipts(
user_id,
message_id,
withdraw_type
)
VALUES (%s, %s, %s)

ON CONFLICT (user_id) DO UPDATE SET  
            message_id=EXCLUDED.message_id,  
            withdraw_type=EXCLUDED.withdraw_type  
    """, (  
        user_id,  
        message_id,  
        withdraw_type  
    ))  

    db_execute("""  
        INSERT INTO withdraw_success(  
            user_id,  
            status  
        )  
        VALUES (%s, 1)  

        ON CONFLICT (user_id) DO UPDATE SET  
            status=EXCLUDED.status  
    """, (user_id,))  

    db_execute("""  
        UPDATE withdraw_queue  
        SET  
            status=2,  

            completed_time=COALESCE(  
                completed_time,  
                EXTRACT(EPOCH FROM clock_timestamp())  
            )  

        WHERE id=%s  
    """, (request_id,))  

    db.commit()  

except Exception:  
    rollback_db()  
    raise

================= TEXT HELPERS =================

def normalize_text(value):
text = unicodedata.normalize("NFKC", value or "")

text = text.translate(str.maketrans({  
    "ي": "ی",  
    "ك": "ک",  
    "آ": "ا",  
    "أ": "ا",  
    "إ": "ا",  
    "\u200c": " ",  
    "\u200d": "",  
    "\u200e": "",  
    "\u200f": "",  
    "\ufeff": "",  
}))  

return re.sub(r"\s+", " ", text).strip()

def is_confirmation_text(text):
return "ایا از انتقال" in normalize_text(text)

================= BOT IDENTITIES =================

async def get_meowie_id(withdraw_type="group"):
"""
id:
@MeowieQBot

group / card:  
    @MeowieQIVBot  
"""  
global MEOWIE_ID, ID_MEOWIE_ID  

if withdraw_type == "id":  
    if ID_MEOWIE_ID is None:  
        entity = await client.get_entity(ID_MEOWIE)  
        ID_MEOWIE_ID = entity.id  

    return ID_MEOWIE_ID  

if MEOWIE_ID is None:  
    entity = await client.get_entity(MEOWIE)  
    MEOWIE_ID = entity.id  

return MEOWIE_ID

def transfer_bot_id(job):
"""
ربات مورد انتظار برای برداشت جاری.
"""
if job["type"] == "id":
return ID_MEOWIE_ID

return MEOWIE_ID

================= BANK HELPERS =================

async def find_meowie_reply(
reply_to_id,
must_contain=None,
button_text=None,
timeout=30
):
# بانک همیشه با MeowieQIVBot کار می‌کند.
meowie_id = await get_meowie_id("card")
deadline = time.monotonic() + timeout

while time.monotonic() < deadline:  
    messages = await client.get_messages(  
        WITHDRAW_GROUP,  
        limit=40  
    )  

    for message in messages:  
        if message.sender_id != meowie_id:  
            continue  

        if message.reply_to_msg_id != reply_to_id:  
            continue  

        text = message.raw_text or ""  

        if must_contain and not any(  
            phrase in text for phrase in must_contain  
        ):  
            continue  

        if button_text:  
            found = any(  
                button.text and button_text in button.text  
                for row in (message.buttons or [])  
                for button in row  
            )  

            if not found:  
                continue  

        return message  

    await asyncio.sleep(1)  

return None

async def find_confirm_panel(
card,
min_id,
timeout=30
):
meowie_id = await get_meowie_id("card")
deadline = time.monotonic() + timeout

while time.monotonic() < deadline:  
    messages = await client.get_messages(  
        WITHDRAW_GROUP,  
        limit=40  
    )  

    for message in messages:  
        if message.sender_id != meowie_id:  
            continue  

        if message.id < min_id:  
            continue  

        if card not in (message.raw_text or ""):  
            continue  

        for row in (message.buttons or []):  
            for button in row:  
                if (  
                    button.text  
                    and "تایید تراکنش" in button.text  
                ):  
                    return message  

    await asyncio.sleep(1)  

return None

async def click_button_containing(message, text):
for row in (message.buttons or []):
for button in row:
if button.text and text in button.text:
await button.click()
return True

return False

================= BANK WITHDRAW =================

async def process_bank_card_withdraw(
request_id,
user_id,
card,
amount
):
global LAST_TRANSFER_TIME

amount_text = str(amount)  

try:  
    sent = await client.send_message(  
        WITHDRAW_GROUP,  
        "بانک میویی"  
    )  

    menu_msg = await find_meowie_reply(  
        sent.id,  
        button_text="کارت به کارت",  
        timeout=30  
    )  

    if not menu_msg:  
        print("❌ منوی بانک میویی نیومد")  
        set_withdraw_status(request_id, 3)  
        return  

    if not await click_button_containing(  
        menu_msg,  
        "کارت به کارت"  
    ):  
        print("❌ دکمه کارت به کارت پیدا نشد")  
        set_withdraw_status(request_id, 3)  
        return  

    prompt_msg = await find_meowie_reply(  
        sent.id,  
        must_contain=[  
            "در جواب همین پنل",  
            "تعیین مبلغ"  
        ],  
        timeout=30  
    )  

    if not prompt_msg:  
        print("❌ پنل مبلغ و شماره حساب نیومد")  
        set_withdraw_status(request_id, 3)  
        return  

    await client.send_message(  
        WITHDRAW_GROUP,  
        f"{amount_text} {card}",  
        reply_to=prompt_msg.id  
    )  

    confirm_msg = await find_confirm_panel(  
        card,  
        min_id=prompt_msg.id,  
        timeout=30  
    )  

    if not confirm_msg:  
        print("❌ پنل تأیید بانک نیومد")  
        set_withdraw_status(request_id, 3)  
        return  

    set_transfer_cooldown("card")  

    if not await click_button_containing(  
        confirm_msg,  
        "تایید تراکنش"  
    ):  
        print("❌ دکمه تأیید تراکنش پیدا نشد")  
        set_withdraw_status(request_id, 3)  
        return  

    set_transfer_cooldown("card")  

    await asyncio.sleep(3)  

    final_msg = await client.get_messages(  
        WITHDRAW_GROUP,  
        ids=confirm_msg.id  
    )  

    final_text = (  
        (final_msg.raw_text or "")  
        if final_msg  
        else ""  
    )  

    print(  
        f"📩 پاسخ نهایی بانک برای {user_id}: "  
        f"{final_text}"  
    )  

    # منطق ثبت نتیجه همان نسخه قبلی است.  
    save_success(  
        request_id,  
        user_id,  
        confirm_msg.id,  
        "card"  
    )  

    # تسویه موجودی بانکی و پیام اطلاع‌رسانی  
    # توسط check_withdraw_success در bot.py انجام می‌شود.  

    print(  
        f"🎉 برداشت بانک ثبت شد: "  
        f"{user_id} مقدار {amount_text}"  
    )  

except Exception as error:  
    print("❌ خطای برداشت بانکی:", error)  
    rollback_db()  
    set_withdraw_status(request_id, 3)  

finally:  
    LAST_TRANSFER_TIME = time.time()  

    set_transfer_cooldown("card")  
    set_transfer_cooldown("normal")

================= ID / GROUP CONFIRMATION =================

async def confirmation_matches_job(event, job):
"""
بررسی تعلق پیام به برداشت جاری.

آیدی: ربات MeowieQBot  
گپی: ربات MeowieQIVBot  
"""  
expected_bot_id = transfer_bot_id(job)  

if event.sender_id != expected_bot_id:  
    return False  

if event.chat_id != job["chat_id"]:  
    return False  

if not job.get("message_id"):  
    return False  

allowed_reply_ids = {job["message_id"]}  

if (  
    job["type"] == "group"  
    and job.get("source_message_id")  
):  
    allowed_reply_ids.add(  
        job["source_message_id"]  
    )  

reply_id = event.reply_to_msg_id  

if reply_id:  
    if reply_id in allowed_reply_ids:  
        return True  

    # پاسخ به پیام واسط همان ربات  
    try:  
        parent = await client.get_messages(  
            job["chat_id"],  
            ids=reply_id  
        )  

        if (  
            parent  
            and parent.sender_id == expected_bot_id  
            and parent.reply_to_msg_id in allowed_reply_ids  
        ):  
            return True  

    except Exception as error:  
        print(  
            "⚠️ بررسی پیام واسط انجام نشد:",  
            error  
        )  

    return False  

# پنل بدون ریپلای:  
# مقصد و مبلغ باید با درخواست مطابقت داشته باشند.  
text = normalize_text(  
    event.raw_text or ""  
).lower()  

digits_map = str.maketrans(  
    "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",  
    "01234567890123456789"  
)  

numeric_text = text.translate(digits_map)  

amounts = {  
    int(re.sub(r"[,٬،]", "", token))  
    for token in re.findall(  
        r"(?<!\d)\d+(?:[,٬،]\d{3})*(?!\d)",  
        numeric_text  
    )  
}  

if job["amount"] not in amounts:  
    return False  

if job["type"] == "id":  
    target = (  
        job.get("target") or ""  
    ).strip().lower()  

    if target:  
        target_pattern = (  
            r"(?<![a-z0-9_])"  
            + re.escape(target)  
            + r"(?![a-z0-9_])"  
        )  

        return bool(  
            event.id > job["message_id"]  
            and re.search(target_pattern, text)  
        )  

return False

async def confirm_normal_transfer(
event,
job,
source
):
"""
تأیید هر دو نوع برداشت آیدی و گپی:
await event.click(0, 0)
"""
global ACTIVE_WITHDRAW

if ACTIVE_WITHDRAW is not job:  
    return  

if job.get("confirming"):  
    return  

if not is_confirmation_text(  
    event.raw_text or ""  
):  
    return  

matched = await confirmation_matches_job(  
    event,  
    job  
)  

if not matched:  
    if event.id not in job["ignored_messages"]:  
        job["ignored_messages"].add(event.id)  

        print(  
            "⚠️ پیام تأیید دریافت شد ولی تعلق آن "  
            "به برداشت جاری مشخص نشد:",  
            f"message={event.id}",  
            f"reply_to={event.reply_to_msg_id}",  
            f"command={job.get('message_id')}"  
        )  

    return  

# جلوگیری از کلیک هم‌زمان توسط رویداد و بررسی دوره‌ای  
if (  
    ACTIVE_WITHDRAW is not job  
    or job.get("confirming")  
):  
    return  

job["confirming"] = True  

print(  
    f"📩 پنل تأیید برداشت {job['type']} پیدا شد؛ "  
    f"مسیر دریافت: {source}"  
)  

confirmed = False  

try:  
    for attempt in range(CONFIRM_RETRY):  
        if ACTIVE_WITHDRAW is not job:  
            return  

        try:  
            fresh = await client.get_messages(  
                job["chat_id"],  
                ids=event.id  
            )  

            if fresh:  
                event = fresh  

            buttons = await event.get_buttons()  

            if not buttons or not buttons[0]:  
                print(  
                    f"⚠️ تلاش {attempt + 1}: "  
                    "دکمه‌های پنل هنوز موجود نیستند"  
                )  

                await asyncio.sleep(2)  
                continue  

            print(  
                "🔘 دکمه‌های پنل:",  
                [  
                    [button.text for button in row]  
                    for row in buttons  
                ]  
            )  

            # همان روش فایل مرجع:  
            # دکمه اول، ردیف اول  
            await event.click(0, 0)  

            confirmed = True  

            print(  
                "✅ دکمه اول ردیف اول زده شد؛ "  
                f"نوع برداشت: {job['type']}"  
            )  

            break  

        except Exception as error:  
            print(  
                f"❌ تلاش تأیید {attempt + 1} ناموفق:",  
                error  
            )  

            await asyncio.sleep(2)  

    if not confirmed:  
        print(  
            "❌ کلیک تأیید انجام نشد؛ "  
            f"درخواست {job['id']}"  
        )  

        return  

    await asyncio.sleep(3)  

    if ACTIVE_WITHDRAW is not job:  
        return  

    # سازوکار ثبت نتیجه مطابق نسخه قبلی است.  
    save_success(  
        job["id"],  
        job["user_id"],  
        event.id,  
        job["type"]  
    )  

    reset_balance(job["user_id"])  

    ACTIVE_WITHDRAW = None  

    print(  
        f"🎉 انتقال ثبت شد؛ "  
        f"کاربر {job['user_id']}، "  
        f"درخواست {job['id']}"  
    )  

except Exception as error:  
    print("❌ خطا در ثبت انتقال:", error)  
    rollback_db()  

    if ACTIVE_WITHDRAW is job:  
        fail_active_withdraw(job)  

finally:  
    if ACTIVE_WITHDRAW is job:  
        job["confirming"] = False

================= RECEIVE BOT MESSAGES =================

@client.on(events.NewMessage(incoming=True))
@client.on(events.MessageEdited(incoming=True))
async def meowie_handler(event):
job = ACTIVE_WITHDRAW

if not job:  
    return  

# هر نوع برداشت ربات مخصوص خود را دارد.  
if event.sender_id != transfer_bot_id(job):  
    return  

if event.chat_id != job["chat_id"]:  
    return  

try:  
    bot_name = (  
        ID_MEOWIE  
        if job["type"] == "id"  
        else MEOWIE  
    )  

    print(  
        f"📩 پیام @{bot_name} دریافت شد: "  
        f"message={event.id}, "  
        f"confirmation="  
        f"{is_confirmation_text(event.raw_text or '')}"  
    )  

    # پاسخ خیلی سریع ممکن است پیش از برگشت  
    # send_message دریافت شود.  
    await asyncio.wait_for(  
        job["ready"].wait(),  
        timeout=CONFIRM_TIMEOUT  
    )  

    if ACTIVE_WITHDRAW is not job:  
        return  

    await confirm_normal_transfer(  
        event,  
        job,  
        source="رویداد تلگرام"  
    )  

except asyncio.TimeoutError:  
    print(  
        "⚠️ اطلاعات دستور انتقال "  
        "به‌موقع آماده نشد"  
    )  

except Exception as error:  
    print("❌ خطای دریافت پیام ربات:", error)  
    rollback_db()

================= FALLBACK MESSAGE CHECK =================

async def scan_confirmation_messages(job):
"""
بررسی آخرین پیام‌های گپ برای پوشش رویدادهای جاافتاده.
"""
if ACTIVE_WITHDRAW is not job:
return

if (  
    job.get("confirming")  
    or not job["ready"].is_set()  
):  
    return  

now = time.monotonic()  

if (  
    now - job["last_scan"]  
    < CONFIRM_SCAN_INTERVAL  
):  
    return  

job["last_scan"] = now  

expected_bot_id = transfer_bot_id(job)  

try:  
    messages = await client.get_messages(  
        job["chat_id"],  
        limit=30  
    )  

    for message in reversed(messages):  
        if (  
            ACTIVE_WITHDRAW is not job  
            or job.get("confirming")  
        ):  
            return  

        if message.sender_id != expected_bot_id:  
            continue  

        if not is_confirmation_text(  
            message.raw_text or ""  
        ):  
            continue  

        if (  
            message.id < job["message_id"]  
            and message.reply_to_msg_id != job["message_id"]  
        ):  
            continue  

        await confirm_normal_transfer(  
            message,  
            job,  
            source="بررسی پیام‌های گپ"  
        )  

except Exception as error:  
    print(  
        "⚠️ بررسی دوره‌ای پیام‌های گپ ناموفق بود:",  
        error  
    )

================= PROCESS WITHDRAW QUEUE =================

async def process_withdraw_queue():
global ACTIVE_WITHDRAW
global LAST_TRANSFER_TIME

while True:  
    try:  
        if ACTIVE_WITHDRAW:  
            job = ACTIVE_WITHDRAW  

            if not job.get("confirming"):  
                await scan_confirmation_messages(job)  

                if (  
                    ACTIVE_WITHDRAW is job  
                    and not job.get("confirming")  
                    and time.monotonic() - job["time"]  
                    > CONFIRM_TIMEOUT  
                ):  
                    print(  
                        "⏰ زمان دریافت تأیید تمام شد؛ "  
                        f"درخواست {job['id']}"  
                    )  

                    fail_active_withdraw(job)  

            await asyncio.sleep(1)  
            continue  

        if (  
            time.time() - LAST_TRANSFER_TIME  
            < TRANSFER_DELAY  
        ):  
            await asyncio.sleep(2)  
            continue  

        queued_request = get_next_withdraw()  

        if not queued_request:  
            await asyncio.sleep(2)  
            continue  

        (  
            request_id,  
            user_id,  
            target,  
            amount,  
            withdraw_type,  
            message_id  
        ) = queued_request  

        amount_text = f"{amount:,}"  

        # ================= BANK =================  

        if withdraw_type == "card":  
            set_transfer_cooldown("card")  
            set_withdraw_status(request_id, 1)  

            print(  
                f"🏦 شروع برداشت بانک: "  
                f"{user_id} مقدار {amount_text}"  
            )  

            await process_bank_card_withdraw(  
                request_id,  
                user_id,  
                target,  
                amount  
            )  

            continue  

        # ================= ID / GROUP =================  

        # اطلاعات درخواست قبل از ارسال پیام آماده می‌شود.  
        job = {  
            "id": request_id,  
            "user_id": user_id,  
            "amount": int(amount),  
            "target": target,  
            "type": withdraw_type,  
            "message_id": None,  
            "source_message_id": message_id,  

            "chat_id": (  
                ID_GROUP_ID  
                if withdraw_type == "id"  
                else WITHDRAW_GROUP_ID  
            ),  

            "time": time.monotonic(),  
            "confirming": False,  
            "ready": asyncio.Event(),  
            "last_scan": 0,  
            "ignored_messages": set()  
        }  

        set_withdraw_status(request_id, 1)  
        set_transfer_cooldown("normal")  

        ACTIVE_WITHDRAW = job  

        try:  
            if withdraw_type == "id":  
                # گپ themaws_gap، ربات MeowieQBot  
                msg = await client.send_message(  
                    ID_GROUP,  
                    f"انتقال میویی {amount_text} {target}"  
                )  

            else:  
                # گپ panke_saghfe، ربات MeowieQIVBot  
                msg = await client.send_message(  
                    WITHDRAW_GROUP,  
                    f"انتقال میویی {amount_text}",  
                    reply_to=message_id  
                )  

            job["message_id"] = msg.id  
            job["chat_id"] = msg.chat_id  
            job["time"] = time.monotonic()  

            LAST_TRANSFER_TIME = time.time()  
            set_transfer_cooldown("normal")  

            print(  
                f"🚀 دستور انتقال ارسال شد؛ "  
                f"type={withdraw_type}, "  
                f"chat={msg.chat_id}, "  
                f"message={msg.id}, "  
                f"amount={amount_text}"  
            )  

            job["ready"].set()  

        except Exception as error:  
            print(  
                "❌ خطا در ارسال دستور انتقال:",  
                error  
            )  

            rollback_db()  

            if ACTIVE_WITHDRAW is job:  
                ACTIVE_WITHDRAW = None  

            job["ready"].set()  
            set_withdraw_status(request_id, 3)  

            await asyncio.sleep(5)  

    except Exception as error:  
        print(  
            "❌ خطا در حلقه پردازش صف:",  
            error  
        )  

        rollback_db()  
        await asyncio.sleep(5)

================= OLD / GROUP WITHDRAW =================

@client.on(events.NewMessage(chats=WITHDRAW_GROUP))
async def old_withdraw_handler(event):
text = (event.raw_text or "").strip()

if text not in [  
    "میوهام رو بده",  
    "میو هام رو بده"  
]:  
    return  

user = await event.get_sender()  

if not user:  
    return  

balance = get_balance(user.id)  

if balance <= 0:  
    await event.reply("❌ موجودی میویی ندارید")  
    return  

added = add_withdraw_queue(  
    user.id,  
    "",  
    balance,  
    "group",  
    event.id  
)  

if not added:  
    await event.reply(  
        "⏳ یک درخواست برداشت در حال انتظار داری."  
    )  
    return  

await event.reply(  
    f"""

⏳ درخواست برداشت ثبت شد ✅

💎 مقدار:

{balance:,} میو

صف انتقال فعال شد.

لطفاً منتظر بمانید.
"""
)

================= WITHDRAW BY ID =================

async def add_id_withdraw(user_id, target):
amount = get_balance(user_id)

if amount <= 0:  
    return False  

return add_withdraw_queue(  
    user_id,  
    target,  
    amount,  
    "id",  
    0  
)

================= RECEIPT HANDLER =================

@client.on(events.NewMessage(incoming=True))
async def receipt_handler(event):
if not event.is_private:
return

text = (event.raw_text or "").strip()  

if text not in [  
    "رسیدم رو بده",  
    "رسیدم رو بده ❤️"  
]:  
    return  

user = await event.get_sender()  

if not user:  
    return  

db_execute("""  
    SELECT request  
    FROM receipt_request  
    WHERE user_id=%s  
""", (user.id,))  

receipt_request = cursor.fetchone()  
db.commit()  

if (  
    not receipt_request  
    or receipt_request[0] != 1  
):  
    await event.reply(  
        "❌ درخواست رسیدی ثبت نشده."  
    )  
    return  

db_execute("""  
    SELECT message_id, withdraw_type  
    FROM withdraw_receipts  
    WHERE user_id=%s  
""", (user.id,))  

receipt = cursor.fetchone()  
db.commit()  

if not receipt:  
    await event.reply("❌ رسید پیدا نشد.")  
    return  

receipt_message_id, withdraw_type = receipt  

if withdraw_type == "group":  
    await event.reply(  
        "برو تو گپ رسیدت هست 😐🤣"  
    )  
    return  

source_chat = (  
    WITHDRAW_GROUP  
    if withdraw_type == "card"  
    else ID_GROUP  
)  

await client.forward_messages(  
    user.id,  
    receipt_message_id,  
    source_chat  
)  

db_execute("""  
    UPDATE receipt_request  
    SET request=0  
    WHERE user_id=%s  
""", (user.id,))  

db.commit()  

print(f"✅ رسید ارسال شد: {user.id}")

================= RUN =================

async def main():
global ID_GROUP_ID
global WITHDRAW_GROUP_ID

print("Mio Userbot Started 🚀")  

await client.start()  

# دریافت آیدی عددی هر دو ربات  
bank_bot_id = await get_meowie_id("group")  
id_bot_id = await get_meowie_id("id")  

# دریافت آیدی عددی هر دو گپ  
ID_GROUP_ID = await client.get_peer_id(  
    ID_GROUP  
)  

WITHDRAW_GROUP_ID = await client.get_peer_id(  
    WITHDRAW_GROUP  
)  

print("✅ Userbot Online")  

print(  
    f"🏦 بانک و برداشت گپی: "  
    f"@{WITHDRAW_GROUP} → @{MEOWIE} "  
    f"(bot_id={bank_bot_id})"  
)  

print(  
    f"📤 برداشت با آیدی: "  
    f"@{ID_GROUP} → @{ID_MEOWIE} "  
    f"(bot_id={id_bot_id})"  
)  

print(  
    f"🏦 فاصله بانک: "  
    f"{BANK_TRANSFER_DELAY} ثانیه"  
)  

print(  
    f"📤 فاصله آیدی و گپی: "  
    f"{TRANSFER_DELAY} ثانیه"  
)  

print("✅ تأیید آیدی و گپی: click(0, 0)")  
print("✅ دریافت پیام جدید و ویرایش پیام")  
print("✅ بررسی دوره‌ای پیام‌های گپ فعال است")  

await asyncio.gather(  
    client.run_until_disconnected(),  
    process_withdraw_queue()  
)

if name == "main":
asyncio.run(main())
