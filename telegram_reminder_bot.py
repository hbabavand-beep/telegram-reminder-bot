"""
ربات یادآوری مناسبت‌ها برای تلگرام

قابل اجرا به صورت محلی با polling (پیشنهاد شده برای تست) یا با webhook.

ویژگی‌ها:
- کار در گروه‌ها یا چت خصوصی: کاربر می‌تواند پیام‌هایی شامل اطلاعات مناسبتی ارسال کند و بات آنها را ذخیره می‌کند.
- فرمت ساده: پیام را با کلمهٔ شروع `ثبت` یا از طریق دستور `/add` ارسال کنید و فیلدها را با `;` جدا کنید.
- پشتیبانی از تولد، ازدواج، نام همسر، تولد همسر، تاریخ تولد فرزندان (چندتایی)، تاریخ فوت پدر/مادر.
- یادآوری روزانه (چک کردن تاریخِ ماه-روز) و ارسال یادآوری در همان چتی که اطلاعات از آن ارسال شده بود.

نمونهٔ فرمت پیام (یک خطی، بدون کوتیشن):

ثبت نام: علی; تولد: 1990-05-12; ازدواج: 2016-07-01; همسر: سارا; تولد_همسر: 1992-03-04; فرزندان: 2018-01-01,2020-02-02; وفات_پدر: 2010-06-01; وفات_مادر: 2015-08-03

یا معادلِ انگلیسی (برای /add):
/add name:Ali; bdate:1990-05-12; marriage:2016-07-01; spouse:Sara; spouse_bdate:1992-03-04; children:2018-01-01,2020-02-02; father_death:2010-06-01; mother_death:2015-08-03

نصب پیش‌نیازها:
pip install python-telegram-bot apscheduler pytz

نحوهٔ اجرا:
1) مقدار BOT_TOKEN را در متغیر محیطی BOT_TOKEN قرار دهید یا مستقیماً در فایل جایگزین کنید (توصیه: از متغیر محیطی استفاده کنید).
2) `python telegram_reminder_bot.py`

نکات فنی:
- از SQLite برای ذخیره‌سازی استفاده شده (فایل reminders.db کنار اسکریپت ایجاد می‌شود).
- زمان‌بندی با APScheduler و منطقهٔ زمانی Europe/Berlin انجام می‌شود.
- در گروه‌ها برای کار کردنِ بات حتماً باید بات را به عنوان عضو و با اجازهٔ خواندن پیام‌ها اضافه کنید و در صورت نیاز دسترسی لازم دهید.

تذکر امنیتی و حفظ حریم خصوصی:
- اطلاعات در یک دیتابیس محلی ذخیره می‌شود. برای استفادهٔ واقعی توصیه می‌شود از دیتابیس امن‌تری و عملیات رمزنگاری/پشتیبان‌گیری استفاده کنید.

-- کد پایین --
"""

import os
import re
import json
import sqlite3
from datetime import datetime, date
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram import Update, Chat
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

# CONFIG
BOT_TOKEN = os.environ.get('BOT_TOKEN') or 'PUT_YOUR_TOKEN_HERE'
TIMEZONE = 'Europe/Berlin'
DAILY_REMINDER_HOUR = 9  # ساعت محلی برای ارسال یادآوری

DB_PATH = 'reminders.db'

# ---------- Database helpers ----------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER,
            username TEXT,
            label TEXT,
            event_type TEXT,
            event_date TEXT,
            created_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


def add_event(chat_id, user_id, username, label, event_type, event_date):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('INSERT INTO events (chat_id, user_id, username, label, event_type, event_date, created_at) VALUES (?,?,?,?,?,?,?)',
                (chat_id, user_id, username, label, event_type, event_date, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


def get_events_for_monthday(month, day):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # event_date stored as YYYY-MM-DD; we match by --MM-DD
    pattern = f'%-{month:02d}-{day:02d}'
    # SQLite doesn't have built-in LIKE for date parts easily; instead fetch all and filter in Python
    cur.execute('SELECT id, chat_id, user_id, username, label, event_type, event_date FROM events')
    rows = cur.fetchall()
    conn.close()
    matches = []
    for r in rows:
        try:
            ed = datetime.strptime(r[6], '%Y-%m-%d').date()
            if ed.month == month and ed.day == day:
                matches.append(r)
        except Exception:
            continue
    return matches

# ---------- Parsing user messages ----------

# نقشهٔ کلیدهای فارسی/انگلیسی به فیلدها
KEYS = {
    'نام': 'name', 'name': 'name',
    'تولد': 'bdate', 'bdate': 'bdate',
    'ازدواج': 'marriage', 'marriage': 'marriage',
    'همسر': 'spouse', 'spouse': 'spouse',
    'تولد_همسر': 'spouse_bdate', 'tavalod_hamsar': 'spouse_bdate', 'spouse_bdate': 'spouse_bdate',
    'فرزندان': 'children', 'children': 'children',
    'وفات_پدر': 'father_death', 'father_death': 'father_death',
    'وفات_مادر': 'mother_death', 'mother_death': 'mother_death'
}


def parse_message_text(text):
    """
    منتظر رشته‌ای به صورت key: value; key2: value2; ...
    مقدار فرزندان می‌تواند با کاما جدا شده باشد.
    برمی‌گرداند دیکشنری از فیلدها.
    """
    data = {}
    # اگر پیام با "ثبت" شروع کند، آن را پاک می‌کنیم
    text = text.strip()
    if text.lower().startswith('ثبت'):
        text = text[len('ثبت'):].strip()
    # پشتیبانی از خط‌های جدا
    parts = re.split(r'[;\n]+', text)
    for part in parts:
        if not part.strip():
            continue
        # قبول فرمت key:value یا key = value
        m = re.split(r'[:=]', part, maxsplit=1)
        if len(m) < 2:
            continue
        raw_key = m[0].strip()
        raw_val = m[1].strip()
        key_normal = raw_key.replace(' ', '_')
        mapped = KEYS.get(key_normal, None)
        if not mapped:
            # شاید کاربر از کلید فارسی با فاصله استفاده کرده
            key_no_space = raw_key.replace(' ', '')
            mapped = KEYS.get(key_no_space, None)
        if not mapped:
            # اگر کلید ناشناخته بود، نادیده بگیر
            continue
        data[mapped] = raw_val
    return data


def try_parse_date(s):
    # انتظار فرمت YYYY-MM-DD. اگر فرمت دیگری بود سعی کنیم آن را تشخیص دهیم.
    s = s.strip()
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            dt = datetime.strptime(s, fmt).date()
            return dt
        except Exception:
            continue
    return None

# ---------- Telegram handlers ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('سلام! برای ثبت مناسبت‌ها پیام را مطابق فرمت نمونه ارسال کنید. برای راهنمایی /help را بزنید.')


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('''راهنما:

برای ثبت رویدادها پیام را به صورت زیر ارسال کنید:

ثبت نام: علی; تولد: 1990-05-12; ازدواج: 2016-07-01; همسر: سارا; تولد_همسر: 1992-03-04; فرزندان: 2018-01-01,2020-02-02; وفات_پدر: 2010-06-01; وفات_مادر: 2015-08-03

یا از دستور /add استفاده کنید با همان فرمت (بدون کلمهٔ "ثبت").
')


async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # متن پس از دستور را بخوانیم
    text = update.message.text
    payload = text[len('/add'):].strip()
    if not payload:
        await update.message.reply_text('لطفا پس از /add داده‌ها را وارد کنید. برای نمونه /help را ببینید.')
        return
    data = parse_message_text(payload)
    await process_parsed_data(update, context, data)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # اگر پیام با "ثبت" شروع شود آن را پردازش کن
    text = update.message.text or ''
    if text.strip().lower().startswith('ثبت'):
        data = parse_message_text(text)
        await process_parsed_data(update, context, data)
    else:
        # پیام دیگری است؛ نادیده می‌گیریم یا می‌توانیم help پیشنهاد دهیم
        return


async def process_parsed_data(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict):
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id if user else None
    username = user.username if user and user.username else (user.full_name if user else None)

    # اگر فیلد تولد وجود داشت، ذخیره کن
    inserted = 0
    if 'bdate' in data:
        dt = try_parse_date(data['bdate'])
        if dt:
            add_event(chat_id, user_id, username, data.get('name') or username or '', 'birthday', dt.isoformat())
            inserted += 1
    if 'marriage' in data:
        dt = try_parse_date(data['marriage'])
        if dt:
            add_event(chat_id, user_id, username, data.get('name') or username or '', 'marriage', dt.isoformat())
            inserted += 1
    if 'spouse_bdate' in data:
        dt = try_parse_date(data['spouse_bdate'])
        if dt:
            label = f"همسر: {data.get('spouse','') }"
            add_event(chat_id, user_id, username, label, 'spouse_birthday', dt.isoformat())
            inserted += 1
    if 'children' in data:
        # می‌تواند چند تاریخ با کاما جدا داشته باشد
        parts = [p.strip() for p in re.split(r'[,
]+', data['children']) if p.strip()]
        for p in parts:
            dt = try_parse_date(p)
            if dt:
                add_event(chat_id, user_id, username, 'فرزند', 'child_birthday', dt.isoformat())
                inserted += 1
    if 'father_death' in data:
        dt = try_parse_date(data['father_death'])
        if dt:
            add_event(chat_id, user_id, username, 'وفات پدر', 'father_death', dt.isoformat())
            inserted += 1
    if 'mother_death' in data:
        dt = try_parse_date(data['mother_death'])
        if dt:
            add_event(chat_id, user_id, username, 'وفات مادر', 'mother_death', dt.isoformat())
            inserted += 1

    if inserted > 0:
        await update.message.reply_text(f'اطلاعات با موفقیت ثبت شد. {inserted} مورد اضافه گردید. من در همان روزها یادآوری ارسال خواهم کرد.')
    else:
        await update.message.reply_text('هیچ تاریخ معتبری پیدا نشد؛ لطفا فرمت تاریخ را به صورت YYYY-MM-DD یا DD-MM-YYYY ارسال کنید یا /help را ببینید.')

# ---------- Reminder job ----------

async def send_reminders(app):
    tz = pytz.timezone(TIMEZONE)
    today = datetime.now(tz).date()
    month = today.month
    day = today.day
    matches = get_events_for_monthday(month, day)
    for r in matches:
        _id, chat_id, user_id, username, label, event_type, event_date = r
        # متن پیام بر اساس event_type
        try:
            label_text = label or ''
            if event_type == 'birthday':
                text = f'یادآوری: امروز تولد {label_text} است. تولد: {event_date}'
            elif event_type == 'spouse_birthday':
                text = f'یادآوری: امروز تولد همسر ({label_text}) است. تولد همسر: {event_date}'
            elif event_type == 'child_birthday':
                text = f'یادآوری: امروز تولد فرزند است. تولد: {event_date}'
            elif event_type == 'marriage':
                text = f'یادآوری: امروز سالگرد ازدواج {label_text} است. تاریخ ازدواج: {event_date}'
            elif event_type == 'father_death':
                text = f'یادآوری: امروز سالگرد وفات پدر ({label_text}) است. تاریخ: {event_date}'
            elif event_type == 'mother_death':
                text = f'یادآوری: امروز سالگرد وفات مادر ({label_text}) است. تاریخ: {event_date}'
            else:
                text = f'یادآوری: امروز مناسبت ({event_type}) برای {label_text} می‌باشد. تاریخ: {event_date}'

            # ارسال پیام به چت مربوط
            await app.bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            print('Error sending reminder to', chat_id, e)

# ---------- Main ----------

async def main():
    init_db()
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_cmd))
    application.add_handler(CommandHandler('add', add_cmd))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    # Scheduler
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    # اجرا هر روز در ساعت مشخص
    trigger = CronTrigger(hour=DAILY_REMINDER_HOUR, minute=0)
    scheduler.add_job(lambda: application.create_task(send_reminders(application)), trigger)
    scheduler.start()

    print('Bot started...')
    await application.run_polling()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
