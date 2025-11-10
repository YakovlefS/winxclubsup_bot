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

# ========= Scope (три темы) =========
SCOPE_CHAT_ID = None
SCOPE_TOPIC_INFO = None
SCOPE_TOPIC_AUCTION = None
SCOPE_TOPIC_ABS = None

# ========= HELPERS =========


def norm_username(u: str) -> str:
    if not u:
        return ""
    return "@" + u if not u.startswith("@") else u


async def ensure_settings_table():
    """Создаёт таблицу settings, если её нет."""
    async with aiosqlite.connect(DB) as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        await conn.commit()


async def get_setting(conn, key):
    cur = await conn.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cur.fetchone()
    return row[0] if row else None


async def set_setting(conn, key, value):
    await conn.execute(
        "INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
        (key, value),
    )
    await conn.commit()


async def load_scope():
    global SCOPE_CHAT_ID, SCOPE_TOPIC_INFO, SCOPE_TOPIC_AUCTION, SCOPE_TOPIC_ABS
    async with aiosqlite.connect(DB) as conn:
        chat = await get_setting(conn, "scope_chat_id")
        info = await get_setting(conn, "scope_topic_info")
        auction = await get_setting(conn, "scope_topic_auction")
        abs_t = await get_setting(conn, "scope_topic_absence")

    SCOPE_CHAT_ID = int(chat) if chat not in (None, "") else None
    SCOPE_TOPIC_INFO = int(info) if info not in (None, "") else None
    SCOPE_TOPIC_AUCTION = int(auction) if auction not in (None, "") else None
    SCOPE_TOPIC_ABS = int(abs_t) if abs_t not in (None, "") else None


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
    return True


def is_leader(message: types.Message) -> bool:
    if not LEADER_ID:
        return False
    if str(LEADER_ID).startswith("@") and (message.from_user.username or ""):
        if norm_username(message.from_user.username).lower() == str(
            LEADER_ID
        ).lower():
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


# ========= Автоудаление =========


async def delete_later(chat_id, msg_id, delay=15):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except Exception as e:
        logging.debug(f"delete_later failed: {e}")


def schedule_cleanup(
    user_msg: types.Message,
    bot_msg: types.Message = None,
    user_delay: int = 0,
    bot_delay: int = 15,
    keep_admin: bool = False,
):
    # Удаляем сообщение пользователя, если это не лидер/офицер (если не keep_admin)
    if not (keep_admin and (is_leader(user_msg) or is_officer(user_msg))):
        asyncio.create_task(
            delete_later(user_msg.chat.id, user_msg.message_id, user_delay)
        )
    # Удаляем ответ бота
    if bot_msg:
        asyncio.create_task(
            delete_later(bot_msg.chat.id, bot_msg.message_id, bot_delay)
        )


# ========= Команды =========


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
        BotCommand("viyti", "Выйти из очереди"),
        BotCommand("zabral", "Отметить получение предметов"),
        BotCommand("dobavit_predmet", "Добавить предмет"),
        BotCommand("udalit_predmet", "Удалить предмет"),
        BotCommand("spisok_predmetov", "Список предметов"),
        BotCommand("privyazat_info", "Привязать тему инфо"),
        BotCommand("privyazat_auk", "Привязать тему аукциона"),
        BotCommand("privyazat_ots", "Привязать тему отсутствий"),
        BotCommand("otvyazat_vse", "Сбросить привязки"),
        BotCommand("help_master", "Список команд"),
        BotCommand("moya_ochered", "Мои места в очередях"),
        BotCommand("sync", "Синхронизация игроков"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())


# ========= Клавиатуры =========


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
            mark = "✅ " if item in selected else ""
            btns.append(
                InlineKeyboardButton(
                    text=f"{mark}{item}", callback_data=f"{prefix}:{item}"
                )
            )
        kb.row(*btns)
    kb.row(
        InlineKeyboardButton("↩️ Назад", callback_data=f"{prefix}_back"),
        InlineKeyboardButton(ok_text, callback_data=f"{prefix}_ok"),
    )
    return kb


# ========= Состояния =========
CLASS_STATE = {}
AUC_STATE = {}
ZABRAL_STATE = {}
QUEUE_STATE = {}

# ========= HELP / START =========


@dp.message_handler(commands=["start", "help_master"])
async def help_master(message: types.Message):
    text = (
        "Команды:\n"
        "• /ник <имя> — регистрация/смена ника\n"
        "• /класс — выбор класса\n"
        "• /бм <число> — обновить БМ\n"
        "• /профиль — твой профиль или /профиль @user — профиль игрока\n"
        "• /топбм — топ-5 прироста БМ за 7 дней\n"
        "• /нет <дд.мм> <причина> — отметить отсутствие\n"
        "• /аук — выбор предметов аукциона\n"
        "• /очередь [предмет] — очередь по предмету или меню\n"
        "• /мояочередь — твои места во всех очередях\n"
        "• /выйти [предмет] — выйти из очереди\n"
        "• /удалить <предмет> <ник> — снять из очереди (офицеры)\n"
        "• /забрал — отметить полученные предметы\n"
        "• /добавить_предмет / удалить_предмет — управление предметами\n"
        "• /список_предметов — список предметов\n"
        "• /привязать_инфо /привязать_аук /привязать_отсутствие — привязка тем\n"
        "• /sync — синхронизировать игроков из Google Sheets (офицеры)\n"
        "• /debug — только владелец\n"
    )
    reply = await message.answer(text)
    schedule_cleanup(message, reply)


# ========= Привязки =========


@dp.message_handler(commands=["привязать_инфо"])
async def bind_info(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав для выполнения этой команды.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_info", str(mtid))
    reply = await message.answer(
        f"✅ Привязано: тема <b>ИНФО</b>.<br>"
        f"<b>chat_id:</b> <code>{message.chat.id}</code><br>"
        f"<b>info_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    await delete_later(reply, 10)


@dp.message_handler(commands=["привязать_аук"])
async def bind_auction(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав для выполнения этой команды.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_auction", str(mtid))
    reply = await message.answer(
        f"✅ Привязано: тема <b>АУК</b>.<br>"
        f"<b>chat_id:</b> <code>{message.chat.id}</code><br>"
        f"<b>auction_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    await delete_later(reply, 10)


@dp.message_handler(commands=["привязать_отсутствие"])
async def bind_abs(message: types.Message):
    if not await only_leader_officers(message):
        return await message.answer("🚫 Недостаточно прав для выполнения этой команды.")
    mtid = message.message_thread_id
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_absence", str(mtid))
    reply = await message.answer(
        f"✅ Привязано: тема <b>ОТСУТСТВИЯ</b>.<br>"
        f"<b>chat_id:</b> <code>{message.chat.id}</code><br>"
        f"<b>absence_topic_id:</b> <code>{mtid}</code>",
        parse_mode="HTML",
    )
    await delete_later(reply, 10)


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
    await load_scope()
    reply = await message.answer("✅ Все привязки тем сняты.")
    schedule_cleanup(message, reply)


# ========= Профиль: ник / класс / БМ =========


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
                f"Текущий ник: {row[0]}\nИзмени так: /ник <новый_ник>"
            )
        else:
            reply = await message.answer("Использование: /ник <имя>")
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

    reply = await message.answer(f"Ник сохранён: {new_nick}")
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
        f"🧙 Текущий класс: {current}\nВыбери новый класс:",
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
    await callback_query.message.edit_text(
        f"✅ Класс обновлён: {sel}"
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
        reply = await message.answer("Использование: /бм <число>")
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
                "Сначала зарегистрируй ник: /ник <имя>"
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

    reply = await message.answer(
        f"БМ обновлён: {old_bm} → {new_bm} (прирост {new_bm-old_bm})"
    )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["профиль", "profil"])
async def cmd_profile(message: types.Message):
    if not in_scope(message, "info"):
        return

    args = message.get_args().strip()

    async with aiosqlite.connect(DB) as conn:
        if args:
            lookup = args.lstrip("@").strip()
            cur = await conn.execute(
                """
                SELECT username,nick,old_nicks,class,bm,bm_updated
                FROM players
                WHERE lower(username)=lower(?)
                   OR lower(nick)=lower(?)
                """,
                (lookup, lookup),
            )
        else:
            cur = await conn.execute(
                """
                SELECT username,nick,old_nicks,class,bm,bm_updated
                FROM players WHERE tg_id=?
                """,
                (message.from_user.id,),
            )
        row = await cur.fetchone()

    if not row:
        reply = await message.answer(
            "Профиль не найден. Зарегистрируй ник: /ник <имя>"
            if not args
            else "Профиль игрока не найден."
        )
        return schedule_cleanup(message, reply, bot_delay=20)

    username, nick, old_nicks, cls, bm, bm_updated = row
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
    reply = await message.answer(text)
    schedule_cleanup(message, reply, bot_delay=25)


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
    text = "Топ прироста БМ за 7 дней:\n" + "\n".join(
        f"{i+1}. {r[0]} (+{r[1]})"
        for i, r in enumerate(rows)
    )
    reply = await message.answer(text)
    schedule_cleanup(message, reply, bot_delay=25)


# ========= Отсутствие =========


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
            "Сначала зарегистрируй ник: /ник <имя>"
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

    reply = await message.answer("Спасибо, отсутствие зафиксировано.")
    schedule_cleanup(message, reply, bot_delay=15)


# ========= Вспомогательное для аукциона =========


def get_items_safe():
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        return header
    except Exception as e:
        logging.warning(f"get_items_safe error: {e}")
        return []


# ========= Аукцион: выбор =========


@dp.message_handler(commands=["аук", "auk"])
async def cmd_auction(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer(
            "Google Sheets недоступен."
        )
        return schedule_cleanup(message, reply)
    header = get_items_safe()
    if not header:
        reply = await message.answer(
            "Лист 'Аукцион' пуст или без шапки."
        )
        return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    AUC_STATE[tg_id] = set()
    reply = await message.answer(
        "🎯 Выбери предметы аукциона:",
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
            col = [
                r[ci] if len(r) > ci else ""
                for r in matrix[1:]
            ]
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
    await callback_query.message.edit_text("\n".join(msgs))
    asyncio.create_task(
        delete_later(
            callback_query.message.chat.id,
            callback_query.message.message_id,
            15,
        )
    )
    await callback_query.answer("Сохранено")


# ========= Очередь: просмотр =========


@dp.message_handler(commands=["очередь", "ochered"])
async def cmd_queue(message: types.Message):
    if not in_scope(message, "auction"):
        return
    parts = message.text.split(maxsplit=1)
    header = get_items_safe()

    # Если указан конкретный предмет
    if len(parts) >= 2:
        item = parts[1].strip()
        if item not in header:
            reply = await message.answer("Предмет не найден.")
            return schedule_cleanup(message, reply)
        try:
            matrix, _ = gsheet.get_auction_matrix()
            ci = header.index(item)
            col = [
                r[ci] if len(r) > ci else ""
                for r in matrix[1:]
            ]
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
            return schedule_cleanup(message, reply, bot_delay=15)
        except Exception as e:
            reply = await message.answer("Ошибка: " + str(e))
            return schedule_cleanup(message, reply)

    # Меню выбора нескольких
    tg_id = message.from_user.id
    QUEUE_STATE[tg_id] = set()
    reply = await message.answer(
        "📜 Выбери предметы для просмотра очередей:",
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
    if item not in header:
        return await callback_query.answer("Недоступно")
    sel = QUEUE_STATE.setdefault(tg_id, set())
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
            col = [
                r[ci] if len(r) > ci else ""
                for r in matrix[1:]
            ]
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

        username = (
            f"@{callback_query.from_user.username}"
            if callback_query.from_user.username
            else callback_query.from_user.full_name
        )
        text = f"Запросил: {username}\n\n" + (
            "\n\n".join(blocks) if blocks else "Нет данных."
        )
        await callback_query.message.edit_text(text)
        asyncio.create_task(
            delete_later(
                callback_query.message.chat.id,
                callback_query.message.message_id,
                15,
            )
        )
        await callback_query.answer("Готово")
    except Exception as e:
        await callback_query.message.edit_text(
            "Ошибка: " + str(e)
        )


# ========= Моя очередь =========


@dp.message_handler(commands=["мояочередь", "moya_ochered"])
async def my_queue_positions(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer(
            "Google Sheets недоступен."
        )
        return schedule_cleanup(message, reply)

    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT nick FROM players WHERE tg_id=?", (tg_id,)
        )
        row = await cur.fetchone()
    if not row or not row[0]:
        reply = await message.answer(
            "Сначала зарегистрируй ник: /ник <имя>"
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
            col = [
                r[col_idx] if len(r) > col_idx else ""
                for r in matrix[1:]
            ]
            col = [c for c in col if c]
            if nick in col:
                pos = col.index(nick) + 1
                positions.append(f"{item} — {pos} место")
            else:
                positions.append(f"{item} — не участвуешь")
        text = "📦 Твои позиции в очередях:\n\n" + "\n".join(
            positions
        )
        reply = await message.answer(text)
        schedule_cleanup(message, reply, bot_delay=40)
    except Exception as e:
        reply = await message.answer(
            "Ошибка при чтении очередей: " + str(e)
        )
        schedule_cleanup(message, reply)


# ========= Выйти / удалить / забрал =========
# (логика как раньше, с логированием)


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
            "Сначала зарегистрируй ник: /ник <имя>"
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
            col = [
                r[ci] if len(r) > ci else ""
                for r in matrix[1:]
            ]
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
    reply = await message.answer(msg)
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
        col = [
            r[ci] if len(r) > ci else ""
            for r in matrix[1:]
        ]
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
        "🎁 Отметь полученные предметы:",
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
            col = [
                r[ci] if len(r) > ci else ""
                for r in matrix[1:]
            ]
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
    await callback_query.message.edit_text("\n".join(msgs))
    asyncio.create_task(
        delete_later(
            callback_query.message.chat.id,
            callback_query.message.message_id,
            15,
        )
    )
    await callback_query.answer("Сохранено")


# ========= Управление предметами =========


@dp.message_handler(commands=["добавить_предмет", "dobavit_predmet"])
async def add_item_cmd(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not await only_leader_officers(message):
        reply = await message.answer("Недостаточно прав.")
        return schedule_cleanup(message, reply)
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        reply = await message.answer(
            "Использование: /добавить_предмет <название>"
        )
        return schedule_cleanup(message, reply)
    name = parts[1].strip()
    try:
        created = gsheet.add_item(name)
        if created:
            reply = await message.answer(
                f"🆕 Предмет «{name}» добавлен."
            )
            gsheet.write_log(
                datetime.datetime.utcnow().isoformat(),
                message.from_user.id,
                message.from_user.username or "",
                "item_add",
                name,
            )
        else:
            reply = await message.answer(
                "Такой предмет уже существует."
            )
    except Exception as e:
        reply = await message.answer(
            "Ошибка Google Sheets: " + str(e)
        )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["удалить_предмет", "udalit_predmet"])
async def del_item_cmd(message: types.Message):
    if not in_scope(message, "auction"):
        return
    if not await only_leader_officers(message):
        reply = await message.answer("Недостаточно прав.")
        return schedule_cleanup(message, reply)
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        reply = await message.answer(
            "Использование: /удалить_предмет <название>"
        )
        return schedule_cleanup(message, reply)
    name = parts[1].strip()
    try:
        ok = gsheet.remove_item(name)
        if ok:
            reply = await message.answer(
                f"🗑 Предмет «{name}» удалён."
            )
            gsheet.write_log(
                datetime.datetime.utcnow().isoformat(),
                message.from_user.id,
                message.from_user.username or "",
                "item_del",
                name,
            )
        else:
            reply = await message.answer("Предмет не найден.")
    except Exception as e:
        reply = await message.answer(
            "Ошибка Google Sheets: " + str(e)
        )
    schedule_cleanup(message, reply)


@dp.message_handler(commands=["список_предметов", "spisok_predmetov"])
async def list_items_cmd(message: types.Message):
    if not in_scope(message, "auction"):
        return
    items = (
        gsheet.list_items()
        if (gsheet and gsheet.sheet)
        else []
    )
    text = (
        "Предметы аукциона:\n- "
        + "\n- ".join(items)
        if items
        else "Список предметов пуст."
    )
    reply = await message.answer(text)
    schedule_cleanup(message, reply)


# ========= Синхронизация игроков из Google Sheets =========


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


# ========= DEBUG (только владелец) =========


@dp.message_handler(commands=["debug"])
async def debug_cmd(message: types.Message):
    if not is_leader(message):
        await message.reply(
            "🚫 Команда доступна только владельцу бота."
        )
        return
    info = (
        "🧩 Debug info:\n"
        f"Chat ID: `{message.chat.id}`\n"
        f"Thread ID: `{message.message_thread_id}`\n"
        f"User ID: `{message.from_user.id}`\n"
        f"Username: @{message.from_user.username or ''}\n"
        f"Message ID: `{message.message_id}`"
    )
    await message.reply(info, parse_mode="Markdown")


# ========= Автоудаление неверных сообщений в теме инфо =========

@dp.message_handler(
    lambda m: m.text
    and not m.text.startswith("/")
    and SCOPE_CHAT_ID
    and SCOPE_TOPIC_INFO
    and m.chat.id == SCOPE_CHAT_ID
    and getattr(m, "message_thread_id", None) == SCOPE_TOPIC_INFO
)
async def auto_delete_wrong_in_info(message: types.Message):
    # Не трогаем лидера и офицеров
    if is_leader(message) or is_officer(message):
        return

    try:
        await message.delete()
    except Exception as e:
        logging.debug(f"auto_delete_wrong_in_info: can't delete user msg: {e}")
        return

    try:
        hint = await bot.send_message(
            chat_id=message.chat.id,
            text=(
                f"💡 @{message.from_user.username or message.from_user.full_name}, "
                "в этой теме можно писать только команды бота.\n"
                "Используй: /профиль, /аук, /очередь, /мояочередь, /топбм, /help_master"
            ),
            message_thread_id=message.message_thread_id,
        )
        asyncio.create_task(
            delete_later(hint.chat.id, hint.message_id, 10)
        )
    except Exception as e:
        logging.debug(f"auto_delete_wrong_in_info: can't send hint: {e}")


# ========= Startup =========


async def on_startup(_):
    await init_db()
    await ensure_settings_table()
    await load_scope()
    await set_commands()

    # Однократная синхронизация игроков
    count = await sync_players_from_gsheet_to_db()

    # Личное уведомление владельцу
    await send_to_leader(
        "🤖 WinxClubSup обновлён и запущен\n\n"
        "📋 Версия: v3.4\n"
        "🧩 Изменения:\n"
        "— автозагрузка игроков из листа 'Игроки' при старте\n"
        "— /профиль @user и /мояочередь\n"
        "— ручной /sync\n"
        "— автоудаление лишних сообщений в теме инфо\n"
        "— расширенные аукцион и очереди\n\n"
        f"👥 Подгружено игроков: {count}"
    )

    logging.info(
        f"Bot started; scope: chat_id={SCOPE_CHAT_ID}, "
        f"info={SCOPE_TOPIC_INFO}, auction={SCOPE_TOPIC_AUCTION}, abs={SCOPE_TOPIC_ABS}"
    )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
