import os
import datetime
import asyncio
import logging

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    InputMediaVideo,
)
import aiosqlite

from db import init_db, DB
from gsheets import GSheetWrapper

# ========= LOGGING =========
logging.basicConfig(level=logging.INFO)

# ========= ENV =========
BOT_TOKEN = os.getenv("BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")

LEADER_ID = os.getenv("LEADER_ID")  # '@username' или числовой id в строке
OFFICERS = [
    s
    for s in (os.getenv("OFFICERS") or "")
    .replace(" ", "")
    .split(",")
    if s
] or ["@Maffins89", "@Gi_Di_Al", "@oOMEMCH1KOo", "@Ferbi55", "@Ahaha_Ohoho", "@yakovlef"]

# Канал новостей по умолчанию (можно переопределить в рантайме командой)
DEFAULT_NEWS_SOURCE = os.getenv("NEWS_SOURCE", "@pwascend")

CLASS_LIST = [
    "Вульпин",
    "Варвар",
    "Лучник",
    "Жрец",
    "Воин",
    "Маг",
    "Убийца",
    "Окультист",
    "Дух меча",
    "Отшельник",
    "Мечник",
]

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN env var is required")

# ========= BOT =========
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

BOT_USERNAME = None  # Получим на старте

# ========= Google Sheets =========
gsheet = None
if GSHEET_ID:
    try:
        gsheet = GSheetWrapper(sheet_id=GSHEET_ID)
        gsheet.ensure_tabs()
    except Exception as e:
        logging.error(f"GSheet init error: {e}")

SHEET_PLAYERS = "Игроки"
SHEET_AUCTION = "Аукцион"

# ========= Scope (темы) =========
SCOPE_CHAT_ID = None
SCOPE_TOPIC_INFO = None
SCOPE_TOPIC_AUCTION = None
SCOPE_TOPIC_ABS = None
SCOPE_TOPIC_NEWS = None  # тема для автоновостей

# ========= HELPERS =========


def norm_username(u: str) -> str:
    if not u:
        return ""
    return "@" + u if not u.startswith("@") else u


def mention_user(u: types.User) -> str:
    if u.username:
        return f"@{u.username}"
    return u.full_name


async def ensure_extra_tables():
    """Создаём служебные таблицы: settings, violations, туториал."""
    async with aiosqlite.connect(DB) as conn:
        # settings
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        # violations
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS violations (
                tg_id INTEGER,
                chat_id INTEGER,
                count INTEGER DEFAULT 0,
                last_ts TEXT,
                last_reason TEXT,
                PRIMARY KEY (tg_id, chat_id)
            )
            """
        )
        # tutorial steps
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tutorial_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE,
                title TEXT
            )
            """
        )
        # tutorial progress
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tutorial_progress (
                tg_id INTEGER,
                step_code TEXT,
                done_ts TEXT,
                PRIMARY KEY (tg_id, step_code)
            )
            """
        )
        await conn.commit()

        # дефолтные шаги обучения, если ещё нет
        cur = await conn.execute("SELECT COUNT(*) FROM tutorial_steps")
        cnt = (await cur.fetchone())[0]
        if cnt == 0:
            await conn.executemany(
                "INSERT INTO tutorial_steps(code,title) VALUES(?,?)",
                [
                    ("nick", "Шаг 1: установить ник через /ник"),
                    ("class", "Шаг 2: выбрать класс через /класс"),
                    ("bm", "Шаг 3: указать свой БМ через /бм"),
                ],
            )
            await conn.commit()


async def get_setting(conn, key, default=None):
    cur = await conn.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cur.fetchone()
    return row[0] if row else default


async def set_setting(conn, key, value):
    await conn.execute(
        "INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
        (key, value),
    )
    await conn.commit()


async def load_scope():
    global SCOPE_CHAT_ID, SCOPE_TOPIC_INFO, SCOPE_TOPIC_AUCTION, SCOPE_TOPIC_ABS, SCOPE_TOPIC_NEWS
    async with aiosqlite.connect(DB) as conn:
        chat = await get_setting(conn, "scope_chat_id")
        info = await get_setting(conn, "scope_topic_info")
        auction = await get_setting(conn, "scope_topic_auction")
        abs_t = await get_setting(conn, "scope_topic_absence")
        news_t = await get_setting(conn, "scope_topic_news")

    SCOPE_CHAT_ID = int(chat) if chat not in (None, "") else None
    SCOPE_TOPIC_INFO = int(info) if info not in (None, "") else None
    SCOPE_TOPIC_AUCTION = int(auction) if auction not in (None, "") else None
    SCOPE_TOPIC_ABS = int(abs_t) if abs_t not in (None, "") else None
    SCOPE_TOPIC_NEWS = int(news_t) if news_t not in (None, "") else None


def in_scope(message: types.Message, role: str) -> bool:
    if SCOPE_CHAT_ID is not None and message.chat.id != SCOPE_CHAT_ID:
        return False
    mtid = getattr(message, "message_thread_id", None)
    if role == "info" and SCOPE_TOPIC_INFO is not None and mtid != SCOPE_TOPIC_INFO:
        return False
    if role == "auction" and SCOPE_TOPIC_AUCTION is not None and mtid != SCOPE_TOPIC_AUCTION:
        return False
    if role == "absence" and SCOPE_TOPIC_ABS is not None and mtid != SCOPE_TOPIC_ABS:
        return False
    if role == "news" and SCOPE_TOPIC_NEWS is not None and mtid != SCOPE_TOPIC_NEWS:
        return False
    return True


def is_leader(message: types.Message) -> bool:
    if not LEADER_ID:
        return False
    if str(LEADER_ID).startswith("@") and (message.from_user.username or ""):
        if norm_username(message.from_user.username).lower() == str(LEADER_ID).lower():
            return True
    try:
        if int(str(LEADER_ID)) == message.from_user.id:
            return True
    except:
        pass
    return False


def is_officer(message: types.Message) -> bool:
    uname = message.from_user.username or ""
    if not uname:
        return False
    nu = norm_username(uname)
    return any(nu.lower() == o.lower() for o in OFFICERS)


async def only_leader_officers(message: types.Message) -> bool:
    return is_leader(message) or is_officer(message)


async def send_to_leader(text: str):
    if not LEADER_ID:
        return
    try:
        if str(LEADER_ID).startswith("@"):
            chat = await bot.get_chat(LEADER_ID)
            await bot.send_message(chat.id, text)
        else:
            await bot.send_message(int(LEADER_ID), text)
    except Exception as e:
        logging.warning(f"send_to_leader failed: {e}")


# ========= ВИЗУАЛЬНЫЙ СТИЛЬ =========


async def get_ui_style() -> str:
    async with aiosqlite.connect(DB) as conn:
        style = await get_setting(conn, "ui_style", "classic")
    return style or "classic"


async def set_ui_style(style: str):
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "ui_style", style)


@dp.message_handler(commands=["set_style"])
async def cmd_set_style(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Менять оформление могут только кураторы гильдии.")
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip() not in ("classic", "compact"):
        return await message.answer("Использование: /set_style classic|compact")
    await set_ui_style(parts[1].strip())
    await message.answer(f"✅ Стиль интерфейса обновлён: {parts[1].strip()}")


# ========= АВТОУДАЛЕНИЕ =========


async def delete_later(chat_id, msg_id, delay=15):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except Exception as e:
        logging.debug(f"delete_later failed: {e}")


def schedule_cleanup(
    user_msg: types.Message = None,
    bot_msg: types.Message = None,
    user_delay: int = 0,
    bot_delay: int = 15,
    keep_admin: bool = False,
):
    if user_msg:
        if not (keep_admin and (is_leader(user_msg) or is_officer(user_msg))):
            asyncio.create_task(
                delete_later(user_msg.chat.id, user_msg.message_id, user_delay)
            )
    if bot_msg:
        asyncio.create_task(
            delete_later(bot_msg.chat.id, bot_msg.message_id, bot_delay)
        )


# ========= ТРЕКЕР НАРУШЕНИЙ =========


async def add_violation(message: types.Message, reason: str):
    if not message.from_user or message.from_user.is_bot:
        return
    try:
        async with aiosqlite.connect(DB) as conn:
            now = datetime.datetime.utcnow().isoformat()
            await conn.execute(
                """
                INSERT INTO violations(tg_id,chat_id,count,last_ts,last_reason)
                VALUES(?,?,?,?,?)
                ON CONFLICT(tg_id,chat_id) DO UPDATE SET
                    count = count + 1,
                    last_ts = excluded.last_ts,
                    last_reason = excluded.last_reason
                """,
                (
                    message.from_user.id,
                    message.chat.id,
                    1,
                    now,
                    reason,
                ),
            )
            cur = await conn.execute(
                "SELECT count FROM violations WHERE tg_id=? AND chat_id=?",
                (message.from_user.id, message.chat.id),
            )
            row = await cur.fetchone()
            await conn.commit()
            count = row[0] if row else 1
    except Exception as e:
        logging.debug(f"add_violation error: {e}")
        return

    # Мягкие автоуведомления
    if count in (3, 5):
        try:
            await bot.send_message(
                chat_id=message.chat.id,
                text=f"⚠️ {mention_user(message.from_user)}, нарушений правил темы: {count}. Будьте внимательнее.",
                reply_to_message_id=message.message_id,
            )
        except:
            pass

    # Уведомление лидера при частых нарушениях
    if count >= 7:
        await send_to_leader(
            f"⚠️ Пользователь {mention_user(message.from_user)} набрал {count} нарушений в чате {message.chat.id}."
        )


@dp.message_handler(commands=["violations", "warns"])
async def cmd_violations(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав.")
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            """
            SELECT tg_id, count, last_ts
            FROM violations
            WHERE chat_id=?
            ORDER BY count DESC
            LIMIT 30
            """,
            (message.chat.id,),
        )
        rows = await cur.fetchall()
    if not rows:
        return await message.answer("Нарушений не зафиксировано.")
    lines = []
    for tg_id, cnt, ts in rows:
        lines.append(f"{tg_id}: {cnt} (последнее: {ts})")
    await message.answer("📊 Нарушения:\n" + "\n".join(lines))


# ========= КОМАНДЫ СПИСКА =========


async def set_commands():
    cmds = [
        BotCommand("nik", "Регистрация/смена ника"),
        BotCommand("klass", "Выбор класса"),
        BotCommand("bm", "Обновить БМ"),
        BotCommand("profil", "Показать профиль"),
        BotCommand("topbm", "Топ прироста БМ"),
        BotCommand("net", "Сообщить об отсутствии"),
        BotCommand("auk", "Выбор предметов аукциона"),
        BotCommand("ochered", "Показать очередь"),
        BotCommand("moya_ochered", "Мои места в очередях"),
        BotCommand("viyti", "Выйти из очереди"),
        BotCommand("zabral", "Отметить получение предметов"),
        BotCommand("help_master", "Список команд"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())


# ========= КЛАВИАТУРЫ =========


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def class_keyboard():
    kb = InlineKeyboardMarkup(row_width=3)
    for row in chunk(CLASS_LIST, 3):
        kb.row(
            *[
                InlineKeyboardButton(
                    text=txt, callback_data=f"class:{txt}"
                )
                for txt in row
            ]
        )
    kb.row(
        InlineKeyboardButton("↩️ Назад", callback_data="class_back"),
        InlineKeyboardButton("✅ Готово", callback_data="class_ok"),
    )
    return kb


def multi_keyboard(header, selected: set, prefix: str, ok_text: str):
    kb = InlineKeyboardMarkup(row_width=3)
    for row in chunk(header, 3):
        btns = []
        for item in row:
            if not item:
                continue
            mark = "✅ " if item in selected else ""
            btns.append(
                InlineKeyboardButton(
                    text=f"{mark}{item}", callback_data=f"{prefix}:{item}"
                )
            )
        if btns:
            kb.row(*btns)
    kb.row(
        InlineKeyboardButton("↩️ Назад", callback_data=f"{prefix}_back"),
        InlineKeyboardButton(ok_text, callback_data=f"{prefix}_ok"),
    )
    return kb


# ========= СОСТОЯНИЯ =========
CLASS_STATE = {}
AUC_STATE = {}
ZABRAL_STATE = {}
QUEUE_STATE = {}

# ========= ТУТОРИАЛ =========


async def mark_tutorial_step(tg_id: int, code: str):
    async with aiosqlite.connect(DB) as conn:
        now = datetime.datetime.utcnow().isoformat()
        await conn.execute(
            """
            INSERT OR IGNORE INTO tutorial_progress(tg_id, step_code, done_ts)
            VALUES(?,?,?)
            """,
            (tg_id, code, now),
        )
        await conn.commit()


async def get_tutorial_status(tg_id: int):
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT code,title FROM tutorial_steps")
        steps = await cur.fetchall()
        cur = await conn.execute(
            "SELECT step_code FROM tutorial_progress WHERE tg_id=?",
            (tg_id,),
        )
        done_rows = await cur.fetchall()
    done = {r[0] for r in done_rows}
    return [
        (code, title, code in done)
        for code, title in steps
    ]


@dp.message_handler(commands=["guide", "tutorial", "start_guide"])
async def cmd_tutorial(message: types.Message):
    status = await get_tutorial_status(message.from_user.id)
    if not status:
        return await message.answer("Обучающие шаги не настроены.")
    lines = []
    for code, title, done in status:
        mark = "✅" if done else "⬜"
        lines.append(f"{mark} {title}")
    await message.answer(
        f"{mention_user(message.from_user)}, твой прогресс:\n" + "\n".join(lines)
    )


# ========= HELP / START =========


@dp.message_handler(commands=["start", "help_master"])
async def help_master(message: types.Message):
    text = (
        f"{mention_user(message.from_user)}, вот что я умею:\n\n"
        "📌 Профиль и БМ:\n"
        "• /ник <имя> — регистрация/смена ника\n"
        "• /класс — выбор класса\n"
        "• /бм <число> — обновить БМ\n"
        "• /профиль — твой профиль или /профиль @user — профиль игрока\n"
        "• /топбм — топ-5 прироста БМ за 7 дней\n\n"
        "🕒 Отсутствия:\n"
        "• /нет <дд.мм> <причина> — отметить отсутствие\n\n"
        "🎁 Аукцион и очереди:\n"
        "• /аук — выбор предметов аукциона\n"
        "• /очередь [предмет] — очередь по предмету или меню выбора\n"
        "• /мояочередь — твои места во всех очередях\n"
        "• /выйти [предмет] — выйти из очереди\n"
        "• /забрал — отметить полученные предметы\n"
        "• /список_предметов — все доступные предметы\n\n"
        "⚙️ Управление (кураторы гильдии):\n"
        "• /добавить_предмет /удалить_предмет\n"
        "• /привязать_инфо /привязать_аук /привязать_отсутствие /привязать_новости\n"
        "• /otvyazat_vse — сброс привязок\n"
        "• /sync — синхронизация с Google Sheets\n"
        "• /set_style classic|compact — стиль сообщений\n"
        "• /violations — список нарушений\n"
        "• /debug — отладка (только лидер)\n"
    )
    reply = await message.answer(text)
    schedule_cleanup(message, reply, bot_delay=60)


# ========= ПРИВЯЗКИ ТЕМ =========


@dp.message_handler(commands=["привязать_инфо"])
async def bind_info(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав.")
    if message.chat.type not in ("group", "supergroup") or message.message_thread_id is None:
        return await message.answer("Вызови команду внутри темы в группе.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_info", str(mtid))
    await load_scope()
    reply = await message.answer(
        f"✅ Привязано: тема <b>ИНФО</b>\n"
        f"<b>chat_id:</b> <code>{message.chat.id}</code>\n"
        f"<b>info_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    schedule_cleanup(message, reply, bot_delay=10)


@dp.message_handler(commands=["привязать_аук"])
async def bind_auction(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав.")
    if message.chat.type not in ("group", "supergroup") or message.message_thread_id is None:
        return await message.answer("Вызови команду внутри темы.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_auction", str(mtid))
    await load_scope()
    reply = await message.answer(
        f"✅ Привязано: тема <b>АУКЦИОН</b>\n"
        f"<b>chat_id:</b> <code>{message.chat.id}</code>\n"
        f"<b>auction_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    schedule_cleanup(message, reply, bot_delay=10)


@dp.message_handler(commands=["привязать_отсутствие", "privyazat_ots"])
async def bind_abs(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав.")
    if message.chat.type not in ("group", "supergroup") or message.message_thread_id is None:
        return await message.answer("Вызови команду внутри темы.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_absence", str(mtid))
    await load_scope()
    reply = await message.answer(
        f"✅ Привязано: тема <b>ОТСУТСТВИЯ</b>\n"
        f"<b>chat_id:</b> <code>{message.chat.id}</code>\n"
        f"<b>absence_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    schedule_cleanup(message, reply, bot_delay=10)


@dp.message_handler(commands=["privyazat_news", "привязать_новости"])
async def bind_news(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав.")
    if message.chat.type not in ("group", "supergroup") or message.message_thread_id is None:
        return await message.answer("Вызови команду внутри темы.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_news", str(mtid))
    await load_scope()
    reply = await message.answer(
        f"✅ Привязано: тема <b>НОВОСТИ</b> для автопостинга из канала.\n"
        f"<b>chat_id:</b> <code>{message.chat.id}</code>\n"
        f"<b>news_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    schedule_cleanup(message, reply, bot_delay=10)


@dp.message_handler(commands=["set_news_source"])
async def set_news_source_cmd(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав.")
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.answer("Использование: /set_news_source @channel или ID")
    src = parts[1].strip()
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "news_source", src)
    await message.answer(f"✅ Источник новостей обновлён: {src}")


async def get_news_source():
    async with aiosqlite.connect(DB) as conn:
        val = await get_setting(conn, "news_source", DEFAULT_NEWS_SOURCE)
    return val or DEFAULT_NEWS_SOURCE


@dp.message_handler(commands=["отвязать_все", "otvyazat_vse"])
async def unbind_all(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return await message.answer("Только в группе.")
    if not await only_leader_officers(message):
        return await message.answer("Недостаточно прав.")
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_topic_info", "")
        await set_setting(conn, "scope_topic_auction", "")
        await set_setting(conn, "scope_topic_absence", "")
        await set_setting(conn, "scope_topic_news", "")
    await load_scope()
    reply = await message.answer("✅ Все привязки тем сняты.")
    schedule_cleanup(message, reply, bot_delay=10)


# ========= ПРОФИЛЬ / НИК / КЛАСС / БМ =========


@dp.message_handler(commands=["ник", "nik"])
async def cmd_nick(message: types.Message):
    if not in_scope(message, "info"):
        return
    parts = message.text.split(maxsplit=1)
    tg_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick, old_nicks FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()

    if len(parts) < 2:
        if row and row[0]:
            reply = await message.answer(
                f"{mention_user(message.from_user)}, твой текущий ник: {row[0]}\n"
                f"Измени так: /ник <новый_ник>"
            )
        else:
            reply = await message.answer(
                f"{mention_user(message.from_user)}, используй: /ник <имя>"
            )
        return schedule_cleanup(message, reply)

    new_nick = parts[1].strip()
    now = datetime.datetime.utcnow().isoformat()
    old_nick = row[0] if row else None
    old_nicks = row[1] or "" if row else ""

    if old_nick and old_nick != new_nick:
        old_nicks = (old_nicks + ";" if old_nicks else "") + old_nick

    async with aiosqlite.connect(DB) as conn:
        if row:
            await conn.execute(
                """
                UPDATE players
                SET nick=?, old_nicks=?, username=?, bm_updated=?
                WHERE tg_id=?
                """,
                (new_nick, old_nicks, username, now, tg_id),
            )
        else:
            await conn.execute(
                """
                INSERT INTO players(tg_id,username,nick,old_nicks,bm_updated)
                VALUES(?,?,?,?,?)
                """,
                (tg_id, username, new_nick, old_nicks, now),
            )
        await conn.commit()

    if gsheet and gsheet.sheet:
        try:
            player = {
                "tg_id": tg_id,
                "telegram": username,
                "nick": new_nick,
                "old_nicks": old_nicks,
                "class": "",
                "current_bm": "",
                "bm_updated": now,
            }
            gsheet.update_player(player)
            if old_nick and old_nick != new_nick:
                gsheet.rename_everywhere(old_nick, new_nick)
            gsheet.write_log(
                now,
                tg_id,
                new_nick,
                "update_nick",
                f"{old_nick} -> {new_nick}" if old_nick else "set",
            )
        except Exception as e:
            logging.warning(f"GSheet nick update failed: {e}")

    await mark_tutorial_step(tg_id, "nick")
    reply = await message.answer(
        f"{mention_user(message.from_user)}, ник сохранён: {new_nick}"
    )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["класс", "klass"])
async def cmd_class(message: types.Message):
    if not in_scope(message, "info"):
        return
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT class FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()
    current = row[0] if row and row[0] else "-"
    CLASS_STATE[tg_id] = None
    reply = await message.answer(
        f"{mention_user(message.from_user)}, твой текущий класс: {current}\n"
        f"Выбери новый класс:",
        reply_markup=class_keyboard(),
    )
    schedule_cleanup(message, reply, bot_delay=30)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("class:"))
async def class_pick(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    _, picked = callback_query.data.split(":", 1)
    if picked not in CLASS_LIST:
        return await callback_query.answer("Неизвестный класс")
    CLASS_STATE[tg_id] = picked
    await callback_query.answer(f"Выбрано: {picked}")


@dp.callback_query_handler(lambda c: c.data == "class_back")
async def class_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    CLASS_STATE[tg_id] = None
    await callback_query.message.edit_reply_markup(
        reply_markup=class_keyboard()
    )
    await callback_query.answer("Выбор сброшен")


@dp.callback_query_handler(lambda c: c.data == "class_ok")
async def class_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = CLASS_STATE.get(tg_id)
    if not sel:
        return await callback_query.answer("Сначала выбери класс")
    now = datetime.datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT username,nick FROM players WHERE tg_id=?", (tg_id,)
        )
        base = await cur.fetchone()
        if not base:
            await conn.execute(
                """
                INSERT INTO players(tg_id,username,class,bm_updated)
                VALUES(?,?,?,?)
                """,
                (
                    tg_id,
                    callback_query.from_user.username
                    or callback_query.from_user.full_name,
                    sel,
                    now,
                ),
            )
        else:
            await conn.execute(
                "UPDATE players SET class=?, bm_updated=? WHERE tg_id=?",
                (sel, now, tg_id),
            )
        await conn.commit()

        if gsheet and gsheet.sheet:
            cur2 = await conn.execute(
                """
                SELECT tg_id, username, nick, old_nicks, class, bm, bm_updated
                FROM players WHERE tg_id=?
                """,
                (tg_id,),
            )
            pr = await cur2.fetchone()
            if pr:
                player = {
                    "tg_id": pr[0],
                    "telegram": pr[1],
                    "nick": pr[2] or "",
                    "old_nicks": pr[3] or "",
                    "class": pr[4] or "",
                    "current_bm": pr[5] or "",
                    "bm_updated": pr[6] or "",
                }
                try:
                    gsheet.update_player(player)
                    gsheet.write_log(
                        now,
                        tg_id,
                        pr[2] or "",
                        "update_class",
                        sel,
                    )
                except Exception as e:
                    logging.warning(
                        f"GSheet class update failed: {e}"
                    )

    CLASS_STATE[tg_id] = None
    await mark_tutorial_step(tg_id, "class")
    await callback_query.message.edit_text(
        f"{mention_user(callback_query.from_user)}, класс обновлён: {sel}"
    )
    asyncio.create_task(
        delete_later(
            callback_query.message.chat.id,
            callback_query.message.message_id,
            15,
        )
    )
    await callback_query.answer("Сохранено")


@dp.message_handler(commands=["бм", "bm"])
async def cmd_bm(message: types.Message):
    if not in_scope(message, "info"):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        reply = await message.answer(
            f"{mention_user(message.from_user)}, используй: /бм <число>"
        )
        return schedule_cleanup(message, reply)

    new_bm = int(parts[1].strip())
    tg_id = message.from_user.id
    now = datetime.datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            """
            SELECT nick,bm,class,username
            FROM players WHERE tg_id=?
            """,
            (tg_id,),
        )
        row = await cur.fetchone()
        if not row:
            reply = await message.answer(
                f"{mention_user(message.from_user)}, сначала /ник <имя>."
            )
            return schedule_cleanup(message, reply)
        nick, old_bm, cls, username = (
            row[0],
            row[1] or 0,
            row[2] or "",
            row[3],
        )
        await conn.execute(
            "UPDATE players SET bm=?, bm_updated=? WHERE tg_id=?",
            (new_bm, now, tg_id),
        )
        await conn.execute(
            """
            INSERT INTO bm_history(tg_id,nick,old_bm,new_bm,diff,ts)
            VALUES(?,?,?,?,?,?)
            """,
            (
                tg_id,
                nick,
                old_bm,
                new_bm,
                new_bm - old_bm,
                now,
            ),
        )
        await conn.commit()

    if gsheet and gsheet.sheet:
        try:
            player = {
                "tg_id": tg_id,
                "telegram": username,
                "nick": nick,
                "old_nicks": "",
                "class": cls,
                "current_bm": new_bm,
                "bm_updated": now,
            }
            gsheet.update_player(player)
            gsheet.append_bm_history(
                {
                    "tg_id": tg_id,
                    "nick": nick,
                    "class": cls,
                    "old_bm": old_bm,
                    "new_bm": new_bm,
                    "diff": new_bm - old_bm,
                    "ts": now,
                }
            )
            gsheet.write_log(
                now,
                tg_id,
                nick,
                "update_bm",
                f"{old_bm}->{new_bm}",
            )
        except Exception as e:
            logging.warning(f"GSheet bm update failed: {e}")

    await mark_tutorial_step(tg_id, "bm")
    reply = await message.answer(
        f"{mention_user(message.from_user)}, БМ обновлён: {old_bm} → {new_bm} (прирост {new_bm-old_bm})"
    )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["профиль", "profil"])
async def cmd_profile(message: types.Message):
    if not in_scope(message, "info"):
        return

    args = message.get_args().strip() if hasattr(message, "get_args") else ""
    lookup_user = None

    async with aiosqlite.connect(DB) as conn:
        if args:
            lookup = args.lstrip("@").strip()
            cur = await conn.execute(
                """
                SELECT username,nick,old_nicks,class,bm,bm_updated,tg_id
                FROM players
                WHERE lower(username)=lower(?)
                   OR lower(nick)=lower(?)
                """,
                (lookup, lookup),
            )
        else:
            cur = await conn.execute(
                """
                SELECT username,nick,old_nicks,class,bm,bm_updated,tg_id
                FROM players WHERE tg_id=?
                """,
                (message.from_user.id,),
            )
        row = await cur.fetchone()

    if not row:
        reply = await message.answer(
            "Профиль не найден. Сначала /ник <имя>."
            if not args
            else "Профиль игрока не найден."
        )
        return schedule_cleanup(message, reply, bot_delay=20)

    username, nick, old_nicks, cls, bm, bm_updated, tg_id = row
    title = (
        f"Профиль @{username}"
        if username
        else f"Профиль {nick or '—'}"
    )
    text = (
        f"📜 {title}\n"
        f"Ник: {nick or '-'}\n"
        f"Старые ники: {old_nicks or '-'}\n"
        f"Класс: {cls or '-'}\n"
        f"БМ: {bm or '-'}\n"
        f"Обновлено: {bm_updated or '-'}"
    )

    kb = InlineKeyboardMarkup()
    if username:
        kb.add(
            InlineKeyboardButton(
                "Открыть профиль", url=f"https://t.me/{username}"
            )
        )

    reply = await message.answer(text, reply_markup=kb if username else None)
    schedule_cleanup(message, reply, bot_delay=40)


@dp.message_handler(commands=["топбм", "topbm"])
async def cmd_topbm(message: types.Message):
    if not in_scope(message, "info"):
        return
    week = (
        datetime.datetime.utcnow()
        - datetime.timedelta(days=7)
    ).isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            """
            SELECT nick, SUM(diff) as s
            FROM bm_history
            WHERE ts>=?
            GROUP BY nick
            ORDER BY s DESC
            LIMIT 5
            """,
            (week,),
        )
        rows = await cur.fetchall()
    if not rows:
        reply = await message.answer("Данных за 7 дней нет.")
        return schedule_cleanup(message, reply)
    text = "🏆 Топ прироста БМ за 7 дней:\n" + "\n".join(
        f"{i+1}. {r[0]} (+{r[1]})"
        for i, r in enumerate(rows)
    )
    reply = await message.answer(text)
    schedule_cleanup(message, reply, bot_delay=25)


# ========= ОТСУТСТВИЯ =========


@dp.message_handler(commands=["нет", "отсутствие", "net"])
async def cmd_absence(message: types.Message):
    role = "absence" if SCOPE_TOPIC_ABS else "info"
    if not in_scope(message, role):
        return
    parts = message.text.split(maxsplit=2)
    date = (
        parts[1].strip()
        if len(parts) >= 2
        else datetime.datetime.utcnow().strftime("%d.%m")
    )
    reason = parts[2].strip() if len(parts) >= 3 else "—"
    tg_id = message.from_user.id

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick,username FROM players WHERE tg_id=?",
            (tg_id,),
        )
        row = await cur.fetchone()
    if not row:
        reply = await message.answer(
            f"{mention_user(message.from_user)}, сначала /ник <имя>."
        )
        return schedule_cleanup(message, reply)
    nick, username = row[0], row[1]

    if gsheet and gsheet.sheet:
        try:
            gsheet.append_absence(
                date,
                nick,
                message.from_user.username
                or message.from_user.full_name,
                reason,
            )
            gsheet.write_log(
                datetime.datetime.utcnow().isoformat(),
                tg_id,
                nick,
                "absence",
                f"{date} {reason}",
            )
        except Exception as e:
            logging.warning(f"GSheet absence failed: {e}")

    if (
        SCOPE_TOPIC_ABS
        and message.chat.id == SCOPE_CHAT_ID
        and message.message_thread_id != SCOPE_TOPIC_ABS
    ):
        try:
            await bot.send_message(
                SCOPE_CHAT_ID,
                f"🛌 {nick}: отсутствует {date}. Причина: {reason}",
                message_thread_id=SCOPE_TOPIC_ABS,
            )
        except:
            pass

    reply = await message.answer(
        f"{mention_user(message.from_user)}, отсутствие зафиксировано."
    )
    schedule_cleanup(message, reply, bot_delay=15)


# ========= АУКЦИОН ВСПОМОГАТЕЛЬНОЕ =========


def get_items_safe():
    try:
        if not (gsheet and gsheet.sheet):
            return []
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        return [h for h in header if h]
    except Exception as e:
        logging.warning(f"get_items_safe error: {e}")
        return []


# ========= АУКЦИОН: ВЫБОР =========


@dp.message_handler(commands=["аук", "auk"])
async def cmd_auction(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer("Google Sheets недоступен.")
        return schedule_cleanup(message, reply)
    header = get_items_safe()
    if not header:
        reply = await message.answer("Лист 'Аукцион' пуст или без шапки.")
        return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    AUC_STATE[tg_id] = set()
    reply = await message.answer(
        f"{mention_user(message.from_user)}, выбери предметы аукциона:",
        reply_markup=multi_keyboard(
            header, AUC_STATE[tg_id], "auc", "✅ Подтвердить"
        ),
    )
    schedule_cleanup(message, reply, bot_delay=60)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("auc:"))
async def auc_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":", 1)[1]
    header = get_items_safe()
    if item not in header:
        return await callback_query.answer("Недоступно")
    sel = AUC_STATE.setdefault(tg_id, set())
    if item in sel:
        sel.remove(item)
        note = f"Снято: {item}"
    else:
        sel.add(item)
        note = f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(
        reply_markup=multi_keyboard(
            header, sel, "auc", "✅ Подтвердить"
        )
    )
    await callback_query.answer(note)


@dp.callback_query_handler(lambda c: c.data == "auc_back")
async def auc_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    AUC_STATE[tg_id] = set()
    header = get_items_safe()
    await callback_query.message.edit_reply_markup(
        reply_markup=multi_keyboard(
            header, AUC_STATE[tg_id], "auc", "✅ Подтвердить"
        )
    )
    await callback_query.answer("Выбор сброшен")


@dp.callback_query_handler(lambda c: c.data == "auc_ok")
async def auc_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = AUC_STATE.get(tg_id, set())
    if not sel:
        return await callback_query.answer("Сначала выбери предметы")

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()
    if not row or not row[0]:
        return await callback_query.answer(
            "Сначала зарегистрируй ник: /ник <имя>",
            show_alert=True,
        )
    nick = row[0]

    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        msgs = []
        for item in sel:
            if item not in header:
                continue
            ci = header.index(item)
            col = [r[ci] if len(r) > ci else "" for r in matrix[1:]]
            col = [c for c in col if c]
            if nick in col:
                col = [c for c in col if c != nick]
                col.append(nick)
                msgs.append(
                    f"🔁 {item} — перемещён в конец (место №{len(col)})"
                )
            else:
                col.append(nick)
                msgs.append(
                    f"✅ {item} — добавлен (место №{len(col)})"
                )
            max_len = max(len(col), len(matrix) - 1)
            while len(matrix) - 1 < max_len:
                matrix.append([""] * len(header))
            for i in range(max_len):
                matrix[i + 1][ci] = col[i] if i < len(col) else ""
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(
            datetime.datetime.utcnow().isoformat(),
            tg_id,
            nick,
            "auction_join",
            ", ".join(sel),
        )
    except Exception as e:
        await callback_query.message.edit_text(
            "Ошибка Google Sheets: " + str(e)
        )
        return

    AUC_STATE[tg_id] = set()
    await callback_query.message.edit_text(
        f"{mention_user(callback_query.from_user)}, твой выбор сохранён:\n" +
        "\n".join(msgs)
    )
    asyncio.create_task(
        delete_later(
            callback_query.message.chat.id,
            callback_query.message.message_id,
            20,
        )
    )
    await callback_query.answer("Сохранено")


# ========= ОЧЕРЕДЬ: ПРОСМОТР =========


@dp.message_handler(commands=["очередь", "ochered"])
async def cmd_queue(message: types.Message):
    if not in_scope(message, "auction"):
        return
    parts = message.text.split(maxsplit=1)
    header = get_items_safe()

    if len(parts) >= 2:
        item = parts[1].strip()
        if item not in header:
            reply = await message.answer("Предмет не найден.")
            return schedule_cleanup(message, reply)
        try:
            matrix, _ = gsheet.get_auction_matrix()
            ci = header.index(item)
            col = [r[ci] if len(r) > ci else "" for r in matrix[1:]]
            col = [c for c in col if c]
            if col:
                text = "Очередь — {}:\n{}".format(
                    item,
                    "\n".join(
                        f"{i+1}. {v}" for i, v in enumerate(col)
                    ),
                )
            else:
                text = f"Очередь — {item}: пусто"
            reply = await message.answer(text)
            return schedule_cleanup(message, reply, bot_delay=20)
        except Exception as e:
            reply = await message.answer("Ошибка: " + str(e))
            return schedule_cleanup(message, reply)

    tg_id = message.from_user.id
    QUEUE_STATE[tg_id] = set()
    reply = await message.answer(
        f"{mention_user(message.from_user)}, выбери предметы для просмотра очередей:",
        reply_markup=multi_keyboard(
            header, QUEUE_STATE[tg_id], "qsel", "✅ Показать очереди"
        ),
    )
    schedule_cleanup(message, reply, bot_delay=60)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("qsel:"))
async def qsel_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":", 1)[1]
    header = get_items_safe()
    sel = QUEUE_STATE.setdefault(tg_id, set())
    if item not in header:
        return await callback_query.answer("Недоступно")
    if item in sel:
        sel.remove(item)
        note = f"Снято: {item}"
    else:
        sel.add(item)
        note = f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(
        reply_markup=multi_keyboard(
            header, sel, "qsel", "✅ Показать очереди"
        )
    )
    await callback_query.answer(note)


@dp.callback_query_handler(lambda c: c.data == "qsel_back")
async def qsel_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    QUEUE_STATE[tg_id] = set()
    header = get_items_safe()
    await callback_query.message.edit_reply_markup(
        reply_markup=multi_keyboard(
            header, QUEUE_STATE[tg_id], "qsel", "✅ Показать очереди"
        )
    )
    await callback_query.answer("Выбор сброшен")


@dp.callback_query_handler(lambda c: c.data == "qsel_ok")
async def qsel_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = list(QUEUE_STATE.get(tg_id, set()))
    if not sel:
        return await callback_query.answer("Сначала выбери предметы")

    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        blocks = []
        for item in sel:
            if item not in header:
                continue
            ci = header.index(item)
            col = [r[ci] if len(r) > ci else "" for r in matrix[1:]]
            col = [c for c in col if c]
            if col:
                block = "Очередь — {}:\n{}".format(
                    item,
                    "\n".join(
                        f"{i+1}. {v}"
                        for i, v in enumerate(col)
                    ),
                )
            else:
                block = f"Очередь — {item}: пусто"
            blocks.append(block)

        username = mention_user(callback_query.from_user)
        text = f"Запросил: {username}\n\n" + (
            "\n\n".join(blocks) if blocks else "Нет данных."
        )
        await callback_query.message.edit_text(text)
        asyncio.create_task(
            delete_later(
                callback_query.message.chat.id,
                callback_query.message.message_id,
                20,
            )
        )
        await callback_query.answer("Готово")
    except Exception as e:
        await callback_query.message.edit_text(
            "Ошибка: " + str(e)
        )


# ========= МОЯ ОЧЕРЕДЬ =========


@dp.message_handler(commands=["мояочередь", "moya_ochered"])
async def my_queue_positions(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer("Google Sheets недоступен.")
        return schedule_cleanup(message, reply)

    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()
    if not row or not row[0]:
        reply = await message.answer(
            f"{mention_user(message.from_user)}, сначала /ник <имя>."
        )
        return schedule_cleanup(message, reply)
    nick = row[0]

    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        if not header:
            reply = await message.answer("Лист 'Аукцион' пуст.")
            return schedule_cleanup(message, reply)
        positions = []
        for col_idx, item in enumerate(header):
            if not item:
                continue
            col = [r[col_idx] if len(r) > col_idx else "" for r in matrix[1:]]
            col = [c for c in col if c]
            if nick in col:
                pos = col.index(nick) + 1
                positions.append(f"{item} — {pos} место")
            else:
                positions.append(f"{item} — не участвуешь")
        text = f"📦 {mention_user(message.from_user)}, твои позиции в очередях:\n\n" + "\n".join(
            positions
        )
        reply = await message.answer(text)
        schedule_cleanup(message, reply, bot_delay=40)
    except Exception as e:
        reply = await message.answer(
            "Ошибка при чтении очередей: " + str(e)
        )
        schedule_cleanup(message, reply)


# ========= ВЫЙТИ / УДАЛИТЬ / ЗАБРАЛ =========


@dp.message_handler(commands=["выйти", "viyti"])
async def cmd_leave(message: types.Message):
    if not in_scope(message, "auction"):
        return
    parts = message.text.split(maxsplit=1)
    target = parts[1].strip() if len(parts) > 1 else None
    tg_id = message.from_user.id

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()
    if not row or not row[0]:
        reply = await message.answer(
            f"{mention_user(message.from_user)}, сначала /ник <имя>."
        )
        return schedule_cleanup(message, reply)
    nick = row[0]

    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        removed = []
        cols = [target] if target else header
        for item in cols:
            if item not in header:
                continue
            ci = header.index(item)
            col = [r[ci] if len(r) > ci else "" for r in matrix[1:]]
            col = [c for c in col if c and c != nick]
            max_len = max(len(col), len(matrix) - 1)
            while len(matrix) - 1 < max_len:
                matrix.append([""] * len(header))
            for i in range(max_len):
                matrix[i + 1][ci] = col[i] if i < len(col) else ""
            removed.append(item)
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(
            datetime.datetime.utcnow().isoformat(),
            tg_id,
            nick,
            "auction_leave",
            ", ".join(removed) or "-",
        )
    except Exception as e:
        reply = await message.answer(
            "Ошибка Google Sheets: " + str(e)
        )
        return schedule_cleanup(message, reply)

    msg = (
        "Удалён из всех очередей ✅"
        if not target
        else f"Удалён из очереди: {target} ✅"
    )
    reply = await message.answer(
        f"{mention_user(message.from_user)}, {msg}"
    )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["удалить", "udalit"])
async def cmd_remove(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not await only_leader_officers(message):
        reply = await message.answer("Недостаточно прав.")
        return schedule_cleanup(message, reply)

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        reply = await message.answer(
            "Использование: /удалить <предмет> <ник>"
        )
        return schedule_cleanup(message, reply)

    item, nick = parts[1].strip(), parts[2].strip()
    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        if item not in header:
            reply = await message.answer("Предмет не найден.")
            return schedule_cleanup(message, reply)
        ci = header.index(item)
        col = [r[ci] if len(r) > ci else "" for r in matrix[1:]]
        col = [c for c in col if c and c != nick]
        max_len = max(len(col), len(matrix) - 1)
        while len(matrix) - 1 < max_len:
            matrix.append([""] * len(header))
        for i in range(max_len):
            matrix[i + 1][ci] = col[i] if i < len(col) else ""
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(
            datetime.datetime.utcnow().isoformat(),
            message.from_user.id,
            message.from_user.username or "",
            "auction_kick",
            f"{nick} ({item})",
        )
    except Exception as e:
        reply = await message.answer(
            "Ошибка Google Sheets: " + str(e)
        )
        return schedule_cleanup(message, reply)

    reply = await message.answer(
        f"🗑 Игрок {nick} удалён из очереди по предмету {item}"
    )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["забрал", "zabral"])
async def cmd_zabral(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer("Google Sheets недоступен.")
        return schedule_cleanup(message, reply)
    header = get_items_safe()
    if not header:
        reply = await message.answer("Лист 'Аукцион' пуст.")
        return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    ZABRAL_STATE[tg_id] = set()
    reply = await message.answer(
        f"{mention_user(message.from_user)}, отметь полученные предметы:",
        reply_markup=multi_keyboard(
            header, ZABRAL_STATE[tg_id], "zabral", "✅ Готово"
        ),
    )
    schedule_cleanup(message, reply, bot_delay=60)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("zabral:"))
async def zabral_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":", 1)[1]
    header = get_items_safe()
    if item not in header:
        return await callback_query.answer("Недоступно")
    sel = ZABRAL_STATE.setdefault(tg_id, set())
    if item in sel:
        sel.remove(item)
        note = f"Снято: {item}"
    else:
        sel.add(item)
        note = f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(
        reply_markup=multi_keyboard(
            header, sel, "zabral", "✅ Готово"
        )
    )
    await callback_query.answer(note)


@dp.callback_query_handler(lambda c: c.data == "zabral_back")
async def zabral_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    ZABRAL_STATE[tg_id] = set()
    header = get_items_safe()
    await callback_query.message.edit_reply_markup(
        reply_markup=multi_keyboard(
            header, ZABRAL_STATE[tg_id], "zabral", "✅ Готово"
        )
    )
    await callback_query.answer("Выбор сброшен")


@dp.callback_query_handler(lambda c: c.data == "zabral_ok")
async def zabral_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = ZABRAL_STATE.get(tg_id, set())
    if not sel:
        return await callback_query.answer("Сначала выбери предметы")

    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()
    if not row or not row[0]:
        return await callback_query.answer(
            "Сначала зарегистрируй ник: /ник <имя>",
            show_alert=True,
        )
    nick = row[0]

    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        msgs = []
        for item in sel:
            if item not in header:
                continue
            ci = header.index(item)
            col = [r[ci] if len(r) > ci else "" for r in matrix[1:]]
            col = [c for c in col if c]
            if nick in col:
                col = [c for c in col if c != nick]
                col.append(nick)
                msgs.append(
                    f"🎁 {item} — отмечено, ты в конце (место №{len(col)})"
                )
            else:
                msgs.append(
                    f"🎁 {item} — отмечено (ты не стоял в очереди)"
                )
            max_len = max(len(col), len(matrix) - 1)
            while len(matrix) - 1 < max_len:
                matrix.append([""] * len(header))
            for i in range(max_len):
                matrix[i + 1][ci] = col[i] if i < len(col) else ""
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(
            datetime.datetime.utcnow().isoformat(),
            tg_id,
            nick,
            "auction_got_items",
            ", ".join(sel),
        )
    except Exception as e:
        await callback_query.message.edit_text(
            "Ошибка Google Sheets: " + str(e)
        )
        return

    ZABRAL_STATE[tg_id] = set()
    await callback_query.message.edit_text(
        f"{mention_user(callback_query.from_user)},\n" +
        "\n".join(msgs)
    )
    asyncio.create_task(
        delete_later(
            callback_query.message.chat.id,
            callback_query.message.message_id,
            20,
        )
    )
    await callback_query.answer("Сохранено")


# ========= АВТОПОСТИНГ НОВОСТЕЙ ИЗ КАНАЛА =========
# Бот должен быть админом в канале и в чате гильдии.


@dp.channel_post_handler()
async def channel_post_handler(message: types.Message):
    try:
        news_source = await get_news_source()
        # Сравнение по username или id
        ok = False
        if news_source.startswith("@"):
            if message.chat.username and ("@" + message.chat.username.lower()) == news_source.lower():
                ok = True
        else:
            try:
                if int(news_source) == message.chat.id:
                    ok = True
            except:
                pass
        if not ok:
            return

        if not (SCOPE_CHAT_ID and SCOPE_TOPIC_NEWS):
            return

        # Копируем текст + медиа в тему новостей
        caption = message.caption or message.text or ""
        if message.photo:
            await bot.send_photo(
                SCOPE_CHAT_ID,
                message.photo[-1].file_id,
                caption=caption,
                message_thread_id=SCOPE_TOPIC_NEWS,
            )
        elif message.video:
            await bot.send_video(
                SCOPE_CHAT_ID,
                message.video.file_id,
                caption=caption,
                message_thread_id=SCOPE_TOPIC_NEWS,
            )
        elif message.media_group_id:
            # Упрощённая обработка альбомов: как отдельные медиа
            if message.photo:
                await bot.send_photo(
                    SCOPE_CHAT_ID,
                    message.photo[-1].file_id,
                    caption=caption,
                    message_thread_id=SCOPE_TOPIC_NEWS,
                )
            elif message.video:
                await bot.send_video(
                    SCOPE_CHAT_ID,
                    message.video.file_id,
                    caption=caption,
                    message_thread_id=SCOPE_TOPIC_NEWS,
                )
        else:
            if caption:
                await bot.send_message(
                    SCOPE_CHAT_ID,
                    caption,
                    message_thread_id=SCOPE_TOPIC_NEWS,
                )
    except Exception as e:
        logging.warning(f"channel_post_handler error: {e}")
        await send_to_leader(f"⚠️ Ошибка автоновостей: {e}")


# ========= DEBUG =========


@dp.message_handler(commands=["debug"])
async def debug_cmd(message: types.Message):
    if not is_leader(message):
        return await message.reply("🚫 Команда доступна только лидеру гильдии.")
    info = (
        "🧩 Debug info:\n"
        f"Chat ID: `{message.chat.id}`\n"
        f"Thread ID: `{getattr(message, 'message_thread_id', None)}`\n"
        f"User ID: `{message.from_user.id}`\n"
        f"Username: @{message.from_user.username or ''}\n"
        f"Message ID: `{message.message_id}`\n"
        f"SCOPE_CHAT_ID: `{SCOPE_CHAT_ID}`\n"
        f"INFO_TOPIC: `{SCOPE_TOPIC_INFO}`\n"
        f"AUCTION_TOPIC: `{SCOPE_TOPIC_AUCTION}`\n"
        f"ABS_TOPIC: `{SCOPE_TOPIC_ABS}`\n"
        f"NEWS_TOPIC: `{SCOPE_TOPIC_NEWS}`"
    )
    await message.reply(info, parse_mode="Markdown")


# ========= АВТОУДАЛЕНИЕ НЕВЕРНЫХ СООБЩЕНИЙ =========
# Инфо: только команды. ОТС: только команды. Аук: только команды и медиа (фото/видео) от игроков.
# Бота, лидера и офицеров не трогаем.


@dp.message_handler(lambda m:
                    m.text
                    and not m.text.startswith("/")
                    and SCOPE_CHAT_ID
                    and SCOPE_TOPIC_INFO
                    and m.chat.id == SCOPE_CHAT_ID
                    and getattr(m, "message_thread_id", None) == SCOPE_TOPIC_INFO)
async def auto_delete_info(message: types.Message):
    if message.from_user.is_bot or is_leader(message) or is_officer(message):
        return
    try:
        await message.delete()
        await add_violation(message, "Текст в инфо-теме")
    except Exception as e:
        logging.debug(f"auto_delete_info delete fail: {e}")
        return
    try:
        hint = await bot.send_message(
            chat_id=message.chat.id,
            text=(
                f"💡 {mention_user(message.from_user)}, в этой теме только команды.\n"
                "Используй: /ник, /класс, /бм, /профиль, /топбм, /help_master"
            ),
            message_thread_id=message.message_thread_id,
        )
        asyncio.create_task(delete_later(hint.chat.id, hint.message_id, 10))
    except Exception as e:
        logging.debug(f"auto_delete_info hint fail: {e}")


@dp.message_handler(lambda m:
                    not m.from_user.is_bot
                    and SCOPE_CHAT_ID
                    and SCOPE_TOPIC_ABS
                    and m.chat.id == SCOPE_CHAT_ID
                    and getattr(m, "message_thread_id", None) == SCOPE_TOPIC_ABS
                    and not m.text.startswith("/нет")
                    and not m.text.startswith("/отсутствие")
                    and not m.text.startswith("/net"))
async def auto_delete_abs(message: types.Message):
    if is_leader(message) or is_officer(message):
        return
    try:
        await message.delete()
        await add_violation(message, "Лишнее сообщение в теме отсутствий")
    except Exception as e:
        logging.debug(f"auto_delete_abs delete fail: {e}")
        return
    try:
        hint = await bot.send_message(
            chat_id=message.chat.id,
            text=(
                f"💡 {mention_user(message.from_user)}, в этой теме только уведомления об отсутствии.\n"
                "Формат: /нет <дд.мм> <причина>"
            ),
            message_thread_id=message.message_thread_id,
        )
        asyncio.create_task(delete_later(hint.chat.id, hint.message_id, 10))
    except Exception as e:
        logging.debug(f"auto_delete_abs hint fail: {e}")


@dp.message_handler(lambda m:
                    SCOPE_CHAT_ID
                    and SCOPE_TOPIC_AUCTION
                    and m.chat.id == SCOPE_CHAT_ID
                    and getattr(m, "message_thread_id", None) == SCOPE_TOPIC_AUCTION
                    and not m.from_user.is_bot)
async def auto_filter_auction(message: types.Message):
    # Разрешаем:
    # - команды (/аук, /очередь, /мояочередь, /выйти, /забрал, /список_предметов)
    # - фото/видео от игроков (лоты)
    text = message.text or ""
    if is_leader(message) or is_officer(message):
        return  # кураторам не трогаем
    if text.startswith("/"):
        return
    if message.photo or message.video:
        return  # оставляем медиа как заявку/скрин
    # всё остальное удаляем
    try:
        await message.delete()
        await add_violation(message, "Лишнее сообщение в теме аукциона")
    except Exception as e:
        logging.debug(f"auto_filter_auction delete fail: {e}")
        return
    try:
        hint = await bot.send_message(
            chat_id=message.chat.id,
            text=(
                f"💡 {mention_user(message.from_user)}, в теме аукциона "
                "оставляем только команды и изображения/видео предметов."
            ),
            message_thread_id=message.message_thread_id,
        )
        asyncio.create_task(delete_later(hint.chat.id, hint.message_id, 10))
    except Exception as e:
        logging.debug(f"auto_filter_auction hint fail: {e}")


# ========= СИНХРОНИЗАЦИЯ ИГРОКОВ ИЗ GOOGLE SHEETS =========


async def sync_players_from_gsheet_to_db() -> int:
    if not (gsheet and gsheet.sheet):
        return 0
    try:
        ws = gsheet.sheet.worksheet(SHEET_PLAYERS)
        rows = ws.get_all_values()
    except Exception as e:
        logging.warning(f"sync_players_from_gsheet_to_db: {e}")
        return 0

    if not rows or len(rows) < 2:
        return 0

    header = rows[0]
    idx = {name: i for i, name in enumerate(header)}

    count = 0
    async with aiosqlite.connect(DB) as conn:
        for row in rows[1:]:
            if not any(row):
                continue
            try:
                tg_id = (
                    int(row[idx["tg_id"]])
                    if "tg_id" in idx and row[idx["tg_id"]]
                    else None
                )
            except:
                tg_id = None
            nick = (
                row[idx["nick"]]
                if "nick" in idx and len(row) > idx["nick"]
                else ""
            )
            if not (tg_id or nick):
                continue
            username = (
                row[idx["telegram"]].lstrip("@")
                if "telegram" in idx and len(row) > idx["telegram"]
                else None
            )
            old_nicks = (
                row[idx["old_nicks"]]
                if "old_nicks" in idx and len(row) > idx["old_nicks"]
                else ""
            )
            cls = (
                row[idx["class"]]
                if "class" in idx and len(row) > idx["class"]
                else ""
            )
            bm_str = (
                row[idx["current_bm"]]
                if "current_bm" in idx
                and len(row) > idx["current_bm"]
                else ""
            )
            bm = int(bm_str) if bm_str.isdigit() else None
            bm_updated = (
                row[idx["bm_updated"]]
                if "bm_updated" in idx
                and len(row) > idx["bm_updated"]
                else ""
            )

            if tg_id:
                await conn.execute(
                    """
                    INSERT INTO players(tg_id,username,nick,old_nicks,class,bm,bm_updated)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(tg_id) DO UPDATE SET
                        username=COALESCE(excluded.username, username),
                        nick=COALESCE(excluded.nick, nick),
                        old_nicks=COALESCE(excluded.old_nicks, old_nicks),
                        class=COALESCE(excluded.class, class),
                        bm=COALESCE(excluded.bm, bm),
                        bm_updated=COALESCE(excluded.bm_updated, bm_updated)
                    """,
                    (
                        tg_id,
                        username,
                        nick,
                        old_nicks,
                        cls,
                        bm,
                        bm_updated,
                    ),
                )
            count += 1
        await conn.commit()
    logging.info(f"Players sync: {count} rows")
    return count


@dp.message_handler(commands=["синхронизировать", "sync"])
async def manual_sync(message: types.Message):
    if not await only_leader_officers(message):
        reply = await message.answer(
            "Недостаточно прав для запуска синхронизации."
        )
        return schedule_cleanup(message, reply)

    reply = await message.answer(
        "🔄 Синхронизация данных с Google Sheets..."
    )
    count = await sync_players_from_gsheet_to_db()
    await reply.edit_text(
        f"✅ Синхронизация завершена.\nОбновлено игроков: {count}"
    )


# ========= STARTUP =========


async def on_startup(_):
    global BOT_USERNAME
    await init_db()
    await ensure_extra_tables()
    await load_scope()
    await set_commands()

    me = await bot.get_me()
    BOT_USERNAME = me.username

    count = await sync_players_from_gsheet_to_db()

    # Личное уведомление лидеру со списком обновлений
    await send_to_leader(
        "🤖 WinxClubSup обновлён и запущен (v4.0 Rebirth)\n\n"
        "Куратор гильдии, вот список изменений:\n"
        "1️⃣ Привязка тем: инфо / аук / отсутствия / новости.\n"
        "2️⃣ Автоудаление лишних сообщений в привязанных темах.\n"
        "3️⃣ Трекер нарушений с уведомлением при частых нарушениях.\n"
        "4️⃣ Профили игроков с кнопкой перехода в Telegram.\n"
        "5️⃣ Очереди и /moya_ochered для личных позиций.\n"
        "6️⃣ Полная интеграция с листом 'Игроки' и команда /sync.\n"
        "7️⃣ Аукцион: выбор, выход, отметка получения, управление предметами.\n"
        "8️⃣ Автопостинг новостей из канала в тему новостей (текст + медиа).\n"
        "9️⃣ Система обучения новичков из трёх шагов (/guide).\n"
        "🔟 Визуальные улучшения и понятные ответы с указанием адресата.\n\n"
        f"👥 Подгружено/обновлено игроков при старте: {count}"
    )

    logging.info(
        f"Bot started; scope: chat_id={SCOPE_CHAT_ID}, "
        f"info={SCOPE_TOPIC_INFO}, auction={SCOPE_TOPIC_AUCTION}, "
        f"abs={SCOPE_TOPIC_ABS}, news={SCOPE_TOPIC_NEWS}"
    )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
