
import os, datetime, asyncio
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import BotCommand, BotCommandScopeAllGroupChats, InlineKeyboardMarkup, InlineKeyboardButton
import aiosqlite

from db import init_db, DB
from gsheets import GSheetWrapper

BOT_TOKEN = os.getenv("BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
LEADER_ID = os.getenv("LEADER_ID")  # '@yakovlef' либо числовой id в строке
OFFICERS = ["@Maffins89", "@Gi_Di_Al", "@oOMEMCH1KOo", "@Ferbi55", "@Ahaha_Ohoho"]
CLASS_LIST = ["Вульпин", "Варвар", "Лучник", "Жрец", "Воин", "Маг", "Убийца", "Окультист", "Дух меча", "Отшельник", "Мечник"]

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN env var is required")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
gsheet = None
if GSHEET_ID:
    try:
        gsheet = GSheetWrapper(sheet_id=GSHEET_ID)
        gsheet.ensure_tabs()
    except Exception as e:
        print("GSheet init error:", e)

# ========= Scope (три темы) =========
SCOPE_CHAT_ID = None
SCOPE_TOPIC_INFO = None
SCOPE_TOPIC_AUCTION = None
SCOPE_TOPIC_ABS = None

async def get_setting(conn, key):
    cur = await conn.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cur.fetchone()
    return row[0] if row else None

async def set_setting(conn, key, value):
    await conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
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
    if LEADER_ID:
        if LEADER_ID.startswith("@") and (message.from_user.username or ""):
            if ("@" + (message.from_user.username or "").lower()) == LEADER_ID.lower():
                return True
        try:
            if int(LEADER_ID) == message.from_user.id:
                return True
        except:
            pass
    return False

def is_officer(message: types.Message) -> bool:
    uname = message.from_user.username or ""
    return ("@" + uname) in OFFICERS if uname else False

async def only_leader_officers(message: types.Message) -> bool:
    return is_leader(message) or is_officer(message)

# ========= Автоудаление =========
async def delete_later(chat_id, msg_id, delay=15):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except:
        pass

def schedule_cleanup(user_msg: types.Message, bot_msg: types.Message=None, user_delay=0, bot_delay=15, keep_admin=False):
    if not (keep_admin and (is_leader(user_msg) or is_officer(user_msg))):
        asyncio.create_task(delete_later(user_msg.chat.id, user_msg.message_id, user_delay))
    if bot_msg:
        asyncio.create_task(delete_later(bot_msg.chat.id, bot_msg.message_id, bot_delay))

# ========= Меню команд (только группы) =========
async def set_commands():
    cmds = [
        BotCommand("nik","Регистрация/смена ника"),
        BotCommand("klass","Выбор класса (кнопки)"),
        BotCommand("bm","Обновить БМ"),
        BotCommand("profil","Показать профиль"),
        BotCommand("topbm","Топ прироста БМ"),
        BotCommand("net","Сообщить об отсутствии"),
        BotCommand("auk","Выбор предметов аукциона"),
        BotCommand("ochered","Показать очередь"),
        BotCommand("viyti","Выйти из очереди"),
        BotCommand("udalit","Удалить игрока (офицеры)"),
        BotCommand("zabral","Отметить получение предметов"),
        BotCommand("dobavit_predmet","Добавить предмет (офицеры)"),
        BotCommand("udalit_predmet","Удалить предмет (офицеры)"),
        BotCommand("spisok_predmetov","Список предметов"),
        BotCommand("privyazat_info","Привязать тему персонажей"),
        BotCommand("privyazat_auk","Привязать тему аукциона"),
        BotCommand("privyazat_ots","Привязать тему отсутствий"),
        BotCommand("otvyazat_vse","Сбросить привязки"),
        BotCommand("help_master","Список команд"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())

# ========= Клавиатуры =========
def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def class_keyboard():
    kb = InlineKeyboardMarkup(row_width=3)
    for row in chunk(CLASS_LIST, 3):
        kb.row(*[InlineKeyboardButton(text=txt, callback_data=f"class:{txt}") for txt in row])
    kb.row(InlineKeyboardButton("↩️ Назад", callback_data="class_back"),
           InlineKeyboardButton("✅ Готово", callback_data="class_ok"))
    return kb

def multi_keyboard(selected:set, header:list, prefix:str, ok_text:str):
    kb = InlineKeyboardMarkup(row_width=3)
    for row in chunk(header, 3):
        btns=[]
        for item in row:
            mark = "✅ " if item in selected else ""
            btns.append(InlineKeyboardButton(text=f"{mark}{item}", callback_data=f"{prefix}:{item}"))
        kb.row(*btns)
    kb.row(InlineKeyboardButton("↩️ Назад", callback_data=f"{prefix}_back"),
           InlineKeyboardButton(ok_text, callback_data=f"{prefix}_ok"))
    return kb

# ========= Состояния =========
CLASS_STATE = {}
AUC_STATE = {}
ZABRAL_STATE = {}
QUEUE_STATE = {}

# ========= Help =========
@dp.message_handler(commands=["start","help_master"])
async def help_master(message: types.Message):
    text = (
        "Команды:\\n"
        "• /ник <имя> — регистрация/смена ника\\n"
        "• /класс — выбор класса (кнопки)\\n"
        "• /бм <число> — обновить БМ (с историей)\\n"
        "• /профиль — показать профиль\\n"
        "• /топбм — топ-5 прироста БМ за 7 дней\\n"
        "• /нет <дд.мм> <причина> — отметить отсутствие\\n"
        "• /отсутствие [дд.мм причина] — быстрый учёт отсутствия\\n"
        "• /аук — выбор предметов аукциона (множественный + подтверждение)\\n"
        "• /очередь [предмет] — выбрать один/несколько предметов и показать очередь\\n"
        "• /выйти [предмет] — выйти из очереди (одной или всех)\\n"
        "• /удалить <предмет> <ник> — удалить из очереди (офицеры/лидер)\\n"
        "• /забрал — отметить получение предметов и уйти в конец очереди\\n"
        "• /добавить_предмет <название> — добавить столбец (офицеры/лидер)\\n"
        "• /удалить_предмет <название> — удалить столбец (офицеры/лидер)\\n"
        "• /список_предметов — показать текущие предметы\\n"
        "• Привязки: /привязать_инфо, /привязать_аук, /привязать_отсутствие, /отвязать_все\\n"
    )
    reply = await message.answer(text)
    schedule_cleanup(message, reply)

# ========= Привязки =========
@dp.message_handler(commands=["привязать_инфо"])
async def bind_info(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return await message.answer("Команда доступна только в группе.")
    if not await only_leader_officers(message):
        return await message.answer("Недостаточно прав.")
    mtid = getattr(message, "message_thread_id", None)
    if mtid is None:
        return await message.answer("Вызови команду внутри темы (форум-поста).")
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_info", str(mtid))
    await load_scope()
    reply = await message.answer(f"✅ Привязано: тема ИНФО.\\nchat_id=`{message.chat.id}`\\ninfo_topic_id=`{mtid}`", parse_mode="Markdown")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["привязать_аук"])
async def bind_auction(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return await message.answer("Команда доступна только в группе.")
    if not await only_leader_officers(message):
        return await message.answer("Недостаточно прав.")
    mtid = getattr(message, "message_thread_id", None)
    if mtid is None:
        return await message.answer("Вызови команду внутри темы (форум-поста).")
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_auction", str(mtid))
    await load_scope()
    reply = await message.answer(f"✅ Привязано: тема АУК.\\nchat_id=`{message.chat.id}`\\nauction_topic_id=`{mtid}`", parse_mode="Markdown")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["привязать_отсутствие"])
async def bind_abs(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return await message.answer("Команда доступна только в группе.")
    if not await only_leader_officers(message):
        return await message.answer("Недостаточно прав.")
    mtid = getattr(message, "message_thread_id", None)
    if mtid is None:
        return await message.answer("Вызови команду внутри темы (форум-поста).")
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_chat_id", str(message.chat.id))
        await set_setting(conn, "scope_topic_absence", str(mtid))
    await load_scope()
    reply = await message.answer(f"✅ Привязано: тема ОТС.\\nchat_id=`{message.chat.id}`\\nabsence_topic_id=`{mtid}`", parse_mode="Markdown")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["отвязать_все"])
async def unbind_all(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return await message.answer("Команда доступна только в группе.")
    if not await only_leader_officers(message):
        return await message.answer("Недостаточно прав.")
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_topic_info", "")
        await set_setting(conn, "scope_topic_auction", "")
        await set_setting(conn, "scope_topic_absence", "")
    await load_scope()
    reply = await message.answer("✅ Все привязки тем сняты. Бот остаётся привязан к группе.")
    schedule_cleanup(message, reply)

# ========= Профиль: ник / класс / БМ =========
@dp.message_handler(commands=["ник","nik"])
async def cmd_nick(message: types.Message):
    if not in_scope(message, "info"): return
    parts = message.text.split(maxsplit=1)
    tg_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
    if len(parts) < 2:
        if row and row[0]:
            reply = await message.answer(f"Текущий ник: {row[0]}\\nИзмени так: /ник <новый_ник>")
            return schedule_cleanup(message, reply)
        else:
            reply = await message.answer("Использование: /ник <имя>")
            return schedule_cleanup(message, reply)
    new_nick = parts[1].strip()
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB) as conn:
        old = row[0] if row else None
        if row and row[0]:
            cur2 = await conn.execute("SELECT old_nicks FROM players WHERE tg_id=?", (tg_id,))
            orow = await cur2.fetchone()
            olds = (orow[0] or "") if orow else ""
            new_olds = (olds + ";" if olds else "") + old if old != new_nick else olds
            await conn.execute("UPDATE players SET nick=?, old_nicks=?, username=?, bm_updated=? WHERE tg_id=?", (new_nick, new_olds, username, now, tg_id))
        else:
            await conn.execute("INSERT INTO players(tg_id,username,nick,bm_updated) VALUES(?,?,?,?)", (tg_id, username, new_nick, now))
        await conn.commit()
    if gsheet and gsheet.sheet:
        try:
            player = {"tg_id": tg_id,"telegram": username,"nick": new_nick,"old_nicks": "", "class": "", "current_bm": "", "bm_updated": now}
            gsheet.update_player(player)
            if old and old != new_nick:
                gsheet.rename_everywhere(old, new_nick)
            gsheet.write_log(now, tg_id, new_nick, "update_nick", f"{old} -> {new_nick}" if old else "set")
        except: pass
    reply = await message.answer(f"Ник сохранён: {new_nick}")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["класс", "klass"])
async def choose_class(message: types.Message):
    """
    Команда /класс — выводит клавиатуру выбора класса.
    Если класс уже выбран — он отмечается ✅
    """
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute(
            "SELECT class FROM players WHERE tg_id=?", (message.from_user.id,)
        )
        row = await cur.fetchone()
        user_class = row[0] if row else None

    # Формируем клавиатуру с выделением текущего класса
    buttons = []
    for cls in CLASS_LIST:
        if cls == user_class:
            btn_text = f"✅ {cls}"
        else:
            btn_text = cls
        buttons.append(
            types.InlineKeyboardButton(text=btn_text, callback_data=f"class_{cls}")
        )

    markup = types.InlineKeyboardMarkup(row_width=3)
    markup.add(*buttons)
    markup.add(
        types.InlineKeyboardButton("🔙 Назад", callback_data="class_back"),
        types.InlineKeyboardButton("✅ Готово", callback_data="class_done"),
    )

    msg = await message.reply("🎓 Выбери свой класс:", reply_markup=markup)
    # Удаляем команду игрока сразу, сообщение бота — через 30 секунд
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    asyncio.create_task(delete_later(msg.chat.id, msg.message_id, 30))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("class:"))
async def class_pick(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    _, picked = callback_query.data.split(":", 1)
    if picked not in CLASS_LIST:
        return await callback_query.answer("Неизвестный класс")
    CLASS_STATE[tg_id] = picked
    await callback_query.answer(f"Выбрано: {picked}", show_alert=False)

@dp.callback_query_handler(lambda c: c.data == "class_back")
async def class_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    CLASS_STATE[tg_id] = None
    await callback_query.message.edit_reply_markup(reply_markup=class_keyboard())
    await callback_query.answer("Выбор сброшен")

@dp.callback_query_handler(lambda c: c.data == "class_ok")
async def class_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = CLASS_STATE.get(tg_id)
    if not sel:
        return await callback_query.answer("Сначала выбери класс")
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT username,nick FROM players WHERE tg_id=?", (tg_id,))
        base = await cur.fetchone()
        if not base:
            await conn.execute("INSERT INTO players(tg_id,username,class,bm_updated) VALUES(?,?,?,?)", (tg_id, callback_query.from_user.username or callback_query.from_user.full_name, sel, now))
        else:
            await conn.execute("UPDATE players SET class=?, bm_updated=? WHERE tg_id=?", (sel, now, tg_id))
        await conn.commit()
        if gsheet and gsheet.sheet:
            cur2 = await conn.execute("SELECT tg_id, username, nick, old_nicks, class, bm, bm_updated FROM players WHERE tg_id=?", (tg_id,))
            pr = await cur2.fetchone()
            player = {"tg_id": pr[0], "telegram": pr[1], "nick": pr[2] or '', "old_nicks": pr[3] or "", "class": pr[4] or "", "current_bm": pr[5] or "", "bm_updated": pr[6] or ""}
            try: gsheet.update_player(player); gsheet.write_log(now, tg_id, pr[2] or '', "update_class", sel)
            except: pass
    CLASS_STATE[tg_id] = None
    await callback_query.message.edit_text(f"✅ Класс обновлён: {sel}")
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 15))
    await callback_query.answer("Сохранено")

# ========= БМ / профиль / топ =========
@dp.message_handler(commands=["бм","bm"])
async def cmd_bm(message: types.Message):
    if not in_scope(message, "info"): return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        reply = await message.answer("Использование: /бм <число>"); return schedule_cleanup(message, reply)
    new_bm = int(parts[1].strip())
    tg_id = message.from_user.id
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick,bm,class,username FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row:
            reply = await message.answer("Сначала зарегистрируй ник: /ник <имя>"); return schedule_cleanup(message, reply)
        nick, old_bm, cls, username = row[0], row[1] or 0, row[2] or "", row[3]
        await conn.execute("UPDATE players SET bm=?, bm_updated=? WHERE tg_id=?", (new_bm, now, tg_id))
        await conn.execute("INSERT INTO bm_history(tg_id,nick,old_bm,new_bm,diff,ts) VALUES(?,?,?,?,?,?)", (tg_id, nick, old_bm, new_bm, new_bm - old_bm, now))
        await conn.commit()
    if gsheet and gsheet.sheet:
        try:
            player = {"tg_id": tg_id, "telegram": username, "nick": nick, "old_nicks": "", "class": cls, "current_bm": new_bm, "bm_updated": now}
            gsheet.update_player(player)
            gsheet.append_bm_history({"tg_id":tg_id,"nick":nick,"class":cls,"old_bm":old_bm,"new_bm":new_bm,"diff":new_bm-old_bm,"ts":now})
            gsheet.write_log(now, tg_id, nick, "update_bm", f"{old_bm}->{new_bm}")
        except: pass
    reply = await message.answer(f"БМ обновлён: {old_bm} → {new_bm} (прирост {new_bm-old_bm})")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["профиль"])
async def show_profile(message: types.Message):
    target_id = message.from_user.id
    target_nick = None

    # Если указали ник в команде
    args = message.get_args().strip()
    if args:
        target_nick = args

    # Если ответ на сообщение
    elif message.reply_to_message:
        target_id = message.reply_to_message.from_user.id

    async with aiosqlite.connect(DB) as conn:
        if target_nick:
            cur = await conn.execute("SELECT nick, old_nicks, class, bm, bm_updated FROM players WHERE nick LIKE ?", (target_nick,))
        else:
            cur = await conn.execute("SELECT nick, old_nicks, class, bm, bm_updated FROM players WHERE tg_id=?", (target_id,))
        row = await cur.fetchone()

    if not row:
        msg = await message.reply("⚠️ Профиль не найден.")
        asyncio.create_task(delete_later(msg.chat.id, msg.message_id, 10))
        return

    nick, old_nicks, cls, bm, updated = row
    bm_str = f"{bm:,}".replace(",", " ") if bm else "-"
    old_nicks = old_nicks if old_nicks and old_nicks.strip() else "-"
    updated = updated if updated and updated.strip() else "-"

    text = (
        "🧙‍♂️ *Профиль игрока*\n\n"
        f"🎮 Ник: *{nick}*\n"
        f"🕰 Старые ники: {old_nicks}\n"
        f"⚔️ Класс: {cls}\n"
        f"💪 Боевой рейтинг: *{bm_str}*\n"
        f"📅 Последнее обновление: {updated}"
    )

    msg = await message.reply(text, parse_mode="Markdown")
    asyncio.create_task(delete_later(msg.chat.id, msg.message_id, 15))

@dp.message_handler(commands=["топбм","topbm"])
async def cmd_topbm(message: types.Message):
    if not in_scope(message, "info"): return
    week = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick, SUM(diff) as s FROM bm_history WHERE ts>=? GROUP BY nick ORDER BY s DESC LIMIT 5", (week,))
        rows = await cur.fetchall()
    if not rows:
        reply = await message.answer("Данных за 7 дней нет."); return schedule_cleanup(message, reply)
    reply = await message.answer("Топ прироста БМ за 7 дней:\\n" + "\\n".join([f"{i+1}. {r[0]} (+{r[1]})" for i,r in enumerate(rows)]))
    schedule_cleanup(message, reply, bot_delay=25)

# ========= Отсутствие =========
@dp.message_handler(commands=["нет","отсутствие","net"])
async def cmd_absence(message: types.Message):
    role = "absence" if SCOPE_TOPIC_ABS else "info"
    if not in_scope(message, role): return
    parts = message.text.split(maxsplit=2)
    date = parts[1].strip() if len(parts) >= 2 else datetime.datetime.utcnow().strftime("%d.%m")
    reason = parts[2].strip() if len(parts) >= 3 else "—"
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick,username FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row:
            reply = await message.answer("Сначала зарегистрируй ник: /ник <имя>"); return schedule_cleanup(message, reply)
        nick, username = row[0], row[1]
    if gsheet and gsheet.sheet:
        try:
            gsheet.append_absence(date, nick, message.from_user.username or message.from_user.full_name, reason)
            gsheet.write_log(datetime.datetime.utcnow().isoformat(), tg_id, nick, "absence", f"{date} {reason}")
        except: pass
    text = "Спасибо, что предупредили об отсутствии."
    if SCOPE_TOPIC_ABS and message.chat.id == SCOPE_CHAT_ID and (message.message_thread_id != SCOPE_TOPIC_ABS):
        try:
            await bot.send_message(SCOPE_CHAT_ID, f"🛌 {nick}: отсутствует {date}. Причина: {reason}", message_thread_id=SCOPE_TOPIC_ABS)
        except: pass
    reply = await message.answer(text)
    schedule_cleanup(message, reply, user_delay=0, bot_delay=15)

# ========= Вспомогательное =========
def get_items_safe():
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        return header
    except:
        return []

def multi_keyboard(header_set, selected_set, prefix, ok_text):
    kb = InlineKeyboardMarkup(row_width=3)
    rows = [header_set[i:i+3] for i in range(0, len(header_set), 3)]
    for row in rows:
        buttons = []
        for item in row:
            mark = "✅ " if item in selected_set else ""
            buttons.append(InlineKeyboardButton(text=f"{mark}{item}", callback_data=f"{prefix}:{item}"))
        kb.row(*buttons)
    kb.row(InlineKeyboardButton("↩️ Назад", callback_data=f"{prefix}_back"),
           InlineKeyboardButton(ok_text, callback_data=f"{prefix}_ok"))
    return kb

# ========= Аукцион: запись =========
AUC_STATE = {}
@dp.message_handler(commands=["аук","auk"])
async def cmd_auction(message: types.Message):
    if not in_scope(message, "auction"): return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer("Google Sheets недоступен. Проверь GOOGLE_CREDENTIALS и GSHEET_ID."); return schedule_cleanup(message, reply)
    header = get_items_safe()
    if not header:
        reply = await message.answer("Лист 'Аукцион' пуст или без шапки."); return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    AUC_STATE[tg_id] = set()
    reply = await message.answer("🎯 Выбери предметы аукциона (можно несколько):", reply_markup=multi_keyboard(header, AUC_STATE[tg_id], "auc", "✅ Подтвердить"))
    schedule_cleanup(message, reply, user_delay=0, bot_delay=60)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("auc:"))
async def auc_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":",1)[1]
    header = get_items_safe()
    if item not in header:
        return await callback_query.answer("Этот предмет сейчас недоступен")
    sel = AUC_STATE.setdefault(tg_id, set())
    if item in sel: sel.remove(item); note=f"Снято: {item}"
    else: sel.add(item); note=f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=multi_keyboard(header, sel, "auc", "✅ Подтвердить"))
    await callback_query.answer(note)

@dp.callback_query_handler(lambda c: c.data == "auc_back")
async def auc_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    AUC_STATE[tg_id] = set()
    header = get_items_safe()
    await callback_query.message.edit_reply_markup(reply_markup=multi_keyboard(header, AUC_STATE[tg_id], "auc", "✅ Подтвердить"))
    await callback_query.answer("Выбор сброшен")

@dp.callback_query_handler(lambda c: c.data == "auc_ok")
async def auc_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = AUC_STATE.get(tg_id, set())
    if not sel: return await callback_query.answer("Сначала выбери предметы")
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row or not row[0]: return await callback_query.answer("Сначала зарегистрируй ник: /ник <имя>", show_alert=True)
        nick = row[0]
    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        msgs = []
        for item in sel:
            if item not in header: continue
            ci = header.index(item)
            col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
            col = [c for c in col if c]
            if nick in col:
                col = [c for c in col if c != nick]
                col.append(nick)
                msgs.append(f"🔁 {item} — перемещён в конец (место №{len(col)})")
            else:
                col.append(nick)
                msgs.append(f"✅ {item} — добавлен (место №{len(col)})")
            max_len = max(len(col), len(matrix)-1)
            while len(matrix)-1 < max_len: matrix.append(['']*len(header))
            for i in range(max_len): matrix[i+1][ci] = col[i] if i < len(col) else ''
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(datetime.datetime.utcnow().isoformat(), tg_id, nick, "auction_join", ", ".join(sel))
    except Exception as e:
        return await callback_query.message.edit_text("Ошибка Google Sheets: " + str(e))
    AUC_STATE[tg_id] = set()
    await callback_query.message.edit_text("\\n".join(msgs))
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 15))
    await callback_query.answer("Сохранено")

# ========= Очередь: просмотр (один или несколько) =========
QUEUE_STATE = {}
@dp.message_handler(commands=["очередь","ochered"])
async def cmd_queue(message: types.Message):
    if not in_scope(message, "auction"): return
    parts = message.text.split(maxsplit=1)
    header = get_items_safe()
    if len(parts) >= 2:
        item = parts[1].strip()
        if item not in header:
            reply = await message.answer("Предмет не найден."); return schedule_cleanup(message, reply)
        try:
            matrix, _ = gsheet.get_auction_matrix()
            ci = header.index(item)
            col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
            col = [c for c in col if c]
            text = "Очередь — {}:\\n{}".format(item, "\\n".join("{}. {}".format(i+1,v) for i,v in enumerate(col))) if col else f"Очередь — {item}: пусто"
            reply = await message.answer(text)
            return schedule_cleanup(message, reply, user_delay=0, bot_delay=15)
        except Exception as e:
            reply = await message.answer("Ошибка: " + str(e)); return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    QUEUE_STATE[tg_id] = set()
    reply = await message.answer("📜 Выберите предметы для просмотра очередей (можно несколько):",
                                 reply_markup=multi_keyboard(header, QUEUE_STATE[tg_id], "qsel", "✅ Показать очереди"))
    schedule_cleanup(message, reply, user_delay=0, bot_delay=60)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("qsel:"))
async def qsel_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":",1)[1]
    header = get_items_safe()
    if item not in header:
        return await callback_query.answer("Предмет недоступен")
    sel = QUEUE_STATE.setdefault(tg_id, set())
    if item in sel: sel.remove(item); note=f"Снято: {item}"
    else: sel.add(item); note=f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=multi_keyboard(header, sel, "qsel", "✅ Показать очереди"))
    await callback_query.answer(note)

@dp.callback_query_handler(lambda c: c.data == "qsel_back")
async def qsel_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    QUEUE_STATE[tg_id] = set()
    header = get_items_safe()
    await callback_query.message.edit_reply_markup(reply_markup=multi_keyboard(header, QUEUE_STATE[tg_id], "qsel", "✅ Показать очереди"))
    await callback_query.answer("Выбор сброшен")

@dp.callback_query_handler(lambda c: c.data == "qsel_ok")
async def qsel_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = list(QUEUE_STATE.get(tg_id, set()))
    username = callback_query.from_user.username or callback_query.from_user.full_name
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
            col = [r[ci] if len(r) > ci else '' for r in matrix[1:]]
            col = [c for c in col if c]
            user_pos = None
            formatted_lines = []

            for i, name in enumerate(col, start=1):
                # обычные цифры вместо эмодзи
                if username and name.lower() == username.lower():
                    formatted_lines.append(f"{i}. **@{name}**")
                    user_pos = i
                else:
                    formatted_lines.append(f"{i}. @{name}")

            if not formatted_lines:
                text_block = f"💎 Очередь по предмету: *{item}*\n━━━━━━━━━━━━━━━━━━\n(пока пуста)\n━━━━━━━━━━━━━━━━━━"
            else:
                text_block = (
                    f"💎 Очередь по предмету: *{item}*\n━━━━━━━━━━━━━━━━━━\n"
                    + "\n".join(formatted_lines)
                )
                if user_pos:
                    text_block += f"\n\n📍 Твоя позиция: №{user_pos}"
                text_block += "\n━━━━━━━━━━━━━━━━━━"

            blocks.append(text_block)

        final_text = f"📋 Запросил: @{username}\n\n" + "\n\n".join(blocks)
        msg = await callback_query.message.edit_text(final_text, parse_mode="Markdown")
        asyncio.create_task(delete_later(msg.chat.id, msg.message_id, 15))
        await callback_query.answer("Очередь обновлена")

    except Exception as e:
        await callback_query.message.edit_text(f"⚠️ Ошибка: {e}")

# ========= Аукцион: выйти / удалить / забрал =========
@dp.message_handler(commands=["выйти","viyti"])
async def cmd_leave(message: types.Message):
    if not in_scope(message, "auction"): return
    parts = message.text.split(maxsplit=1)
    target = parts[1].strip() if len(parts) > 1 else None
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row or not row[0]: reply = await message.answer("Сначала зарегистрируй ник: /ник <имя>"); return schedule_cleanup(message, reply)
        nick = row[0]
    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        removed = []
        cols = [target] if target else header
        for item in cols:
            if item not in header: continue
            ci = header.index(item)
            col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
            col = [c for c in col if c and c != nick]
            max_len = max(len(col), len(matrix)-1)
            while len(matrix)-1 < max_len: matrix.append(['']*len(header))
            for i in range(max_len): matrix[i+1][ci] = col[i] if i < len(col) else ''
            removed.append(item)
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(datetime.datetime.utcnow().isoformat(), tg_id, nick, "auction_leave", ", ".join(removed) or "-")
    except Exception as e:
        reply = await message.answer("Ошибка Google Sheets: " + str(e)); return schedule_cleanup(message, reply)
    reply = await message.answer(("Удалён из всех очередей" if not target else f"Удалён из очереди: {target}") + " ✅")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["удалить","udalit"])
async def cmd_remove(message: types.Message):
    if not in_scope(message, "auction"): return
    if not await only_leader_officers(message):
        reply = await message.answer("Недостаточно прав."); return schedule_cleanup(message, reply)
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        reply = await message.answer("Использование: /удалить <предмет> <ник>"); return schedule_cleanup(message, reply)
    item, nick = parts[1].strip(), parts[2].strip()
    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        if item not in header: reply = await message.answer("Предмет не найден."); return schedule_cleanup(message, reply)
        ci = header.index(item)
        col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
        col = [c for c in col if c and c != nick]
        max_len = max(len(col), len(matrix)-1)
        while len(matrix)-1 < max_len: matrix.append(['']*len(header))
        for i in range(max_len): matrix[i+1][ci] = col[i] if i < len(col) else ''
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(datetime.datetime.utcnow().isoformat(), message.from_user.id, message.from_user.username or "", "auction_kick", f"{nick} ({item})")
    except Exception as e:
        reply = await message.answer("Ошибка Google Sheets: " + str(e)); return schedule_cleanup(message, reply)
    reply = await message.answer(f"🗑 Игрок {nick} удалён из очереди по предмету {item}")
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["забрал","zabral"])
async def cmd_zabral(message: types.Message):
    if not in_scope(message, "auction"): return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer("Google Sheets недоступен."); return schedule_cleanup(message, reply)
    header = get_items_safe()
    if not header:
        reply = await message.answer("Лист 'Аукцион' пуст."); return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    global ZABRAL_STATE
    ZABRAL_STATE[tg_id] = set()
    reply = await message.answer("🎁 Отметь полученные предметы (можно несколько):", reply_markup=multi_keyboard(header, ZABRAL_STATE[tg_id], "zabral", "✅ Готово"))
    schedule_cleanup(message, reply, user_delay=0, bot_delay=60)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("zabral:"))
async def zabral_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":",1)[1]
    header = get_items_safe()
    if item not in header:
        return await callback_query.answer("Этот предмет сейчас недоступен")
    sel = ZABRAL_STATE.setdefault(tg_id, set())
    if item in sel: sel.remove(item); note=f"Снято: {item}"
    else: sel.add(item); note=f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=multi_keyboard(header, sel, "zabral", "✅ Готово"))
    await callback_query.answer(note)

@dp.callback_query_handler(lambda c: c.data == "zabral_back")
async def zabral_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    ZABRAL_STATE[tg_id] = set()
    header = get_items_safe()
    await callback_query.message.edit_reply_markup(reply_markup=multi_keyboard(header, ZABRAL_STATE[tg_id], "zabral", "✅ Готово"))
    await callback_query.answer("Выбор сброшен")

@dp.callback_query_handler(lambda c: c.data == "zabral_ok")
async def zabral_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = ZABRAL_STATE.get(tg_id, set())
    if not sel: return await callback_query.answer("Сначала выбери предметы")
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row or not row[0]: return await callback_query.answer("Сначала зарегистрируй ник: /ник <имя>", show_alert=True)
        nick = row[0]
    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        msgs = []
        for item in sel:
            if item not in header: continue
            ci = header.index(item)
            col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
            col = [c for c in col if c]
            if nick in col:
                col = [c for c in col if c != nick]
                col.append(nick)
                msgs.append(f"🎁 {item} — отмечено, ты перемещён в конец (место №{len(col)})")
            else:
                msgs.append(f"🎁 {item} — отмечено (ты ещё не в очереди)")
            max_len = max(len(col), len(matrix)-1)
            while len(matrix)-1 < max_len: matrix.append(['']*len(header))
            for i in range(max_len): matrix[i+1][ci] = col[i] if i < len(col) else ''
        gsheet.write_auction_matrix(ws, matrix)
        gsheet.write_log(datetime.datetime.utcnow().isoformat(), tg_id, nick, "auction_got_items", ", ".join(sel))
    except Exception as e:
        return await callback_query.message.edit_text("Ошибка Google Sheets: " + str(e))
    ZABRAL_STATE[tg_id] = set()
    await callback_query.message.edit_text("\\n".join(msgs))
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 15))
    await callback_query.answer("Сохранено")

# ========= Управление предметами (офицеры/лидер) =========
@dp.message_handler(commands=["добавить_предмет","dobavit_predmet"])
async def add_item_cmd(message: types.Message):
    if not in_scope(message, "auction"): return
    if not await only_leader_officers(message):
        reply = await message.answer("Недостаточно прав."); return schedule_cleanup(message, reply)
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        reply = await message.answer("Использование: /добавить_предмет <название>"); return schedule_cleanup(message, reply)
    name = parts[1].strip()
    try:
        created = gsheet.add_item(name)
        if created:
            reply = await message.answer(f"🆕 Предмет «{name}» добавлен в аукцион!")
            gsheet.write_log(datetime.datetime.utcnow().isoformat(), message.from_user.id, message.from_user.username or "", "item_add", name)
        else:
            reply = await message.answer("Такой предмет уже существует.")
    except Exception as e:
        reply = await message.answer("Ошибка Google Sheets: " + str(e))
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["удалить_предмет","udalit_predmet"])
async def del_item_cmd(message: types.Message):
    if not in_scope(message, "auction"): return
    if not await only_leader_officers(message):
        reply = await message.answer("Недостаточно прав."); return schedule_cleanup(message, reply)
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        reply = await message.answer("Использование: /удалить_предмет <название>"); return schedule_cleanup(message, reply)
    name = parts[1].strip()
    try:
        ok = gsheet.remove_item(name)
        if ok:
            reply = await message.answer(f"🗑 Предмет «{name}» удалён из аукциона!")
            gsheet.write_log(datetime.datetime.utcnow().isoformat(), message.from_user.id, message.from_user.username or "", "item_del", name)
        else:
            reply = await message.answer("Предмет не найден.")
    except Exception as e:
        reply = await message.answer("Ошибка Google Sheets: " + str(e))
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["список_предметов","spisok_predmetov"])
async def list_items_cmd(message: types.Message):
    if not in_scope(message, "auction"): return
    items = gsheet.list_items() if (gsheet and gsheet.sheet) else []
    text = "Предметы аукциона:\\n- " + "\\n- ".join(items) if items else "Список предметов пуст."
    reply = await message.answer(text)
    schedule_cleanup(message, reply)

@dp.message_handler(commands=["синхронизировать"])
async def sync_data(message: types.Message):
    if not (is_leader(message) or is_officer(message)):
        return await message.reply("❌ Команда доступна только лидеру и офицерам.")

    if not gsheet:
        return await message.reply("⚠️ Google Sheets недоступен.")

    async with aiosqlite.connect(DB) as conn:
        players_ws = gsheet.sheet.worksheet("Игроки")
        data = players_ws.get_all_values()
        header = data[0]
        nick_idx = header.index("nick")
        tg_idx = header.index("tg_id") if "tg_id" in header else 0
        class_idx = header.index("class")
        bm_idx = header.index("current_bm")

        count = 0
        for row in data[1:]:
            if len(row) <= nick_idx:
                continue
            nick = row[nick_idx]
            tg_id = int(row[tg_idx]) if row[tg_idx].isdigit() else None
            cls = row[class_idx]
            bm = int(row[bm_idx]) if row[bm_idx].isdigit() else 0
            await conn.execute(
                "INSERT OR REPLACE INTO players(tg_id,nick,class,bm) VALUES(?,?,?,?)",
                (tg_id, nick, cls, bm),
            )
            count += 1
        await conn.commit()

        # settings
        try:
            ws = gsheet.sheet.worksheet("settings")
            settings = ws.get_all_records()
            for row in settings:
                await conn.execute(
                    "INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
                    (row["key"], str(row["value"]))
                )
            await conn.commit()
        except Exception as e:
            print("settings not found:", e)

    msg = await message.reply(f"✅ Синхронизация завершена\n👥 Обновлено игроков: {count}")
    asyncio.create_task(delete_later(msg.chat.id, msg.message_id, 15))

# ========= Startup =========
async def on_startup(_):
    await init_db()
    await load_scope()
    await set_commands()
    if os.getenv("STARTUP_ANNOUNCE") and SCOPE_CHAT_ID:
        try:
            if SCOPE_TOPIC_INFO:
                await bot.send_message(SCOPE_CHAT_ID, "✅ Бот запущен (инфо).", message_thread_id=SCOPE_TOPIC_INFO)
            if SCOPE_TOPIC_AUCTION:
                await bot.send_message(SCOPE_CHAT_ID, "✅ Бот запущен (аукцион).", message_thread_id=SCOPE_TOPIC_AUCTION)
            if SCOPE_TOPIC_ABS:
                await bot.send_message(SCOPE_CHAT_ID, "✅ Бот запущен (отсутствия).", message_thread_id=SCOPE_TOPIC_ABS)
        except: pass
    print("Bot started; scope:", "chat_id", SCOPE_CHAT_ID, "info", SCOPE_TOPIC_INFO, "auction", SCOPE_TOPIC_AUCTION, "abs", SCOPE_TOPIC_ABS)

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
