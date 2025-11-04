
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
            if ("@" + message.from_user.username.lower()) == LEADER_ID.lower():
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

def schedule_cleanup(user_msg: types.Message, bot_msg: types.Message=None, delay=15, keep_admin=False):
    if not (keep_admin and (is_leader(user_msg) or is_officer(user_msg))):
        asyncio.create_task(delete_later(user_msg.chat.id, user_msg.message_id, delay))
    if bot_msg:
        asyncio.create_task(delete_later(bot_msg.chat.id, bot_msg.message_id, delay))

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

def auction_keyboard(selected:set, header:list, prefix="auc"):
    kb = InlineKeyboardMarkup(row_width=3)
    for row in chunk(header, 3):
        btns=[]
        for item in row:
            mark = "✅ " if item in selected else ""
            btns.append(InlineKeyboardButton(text=f"{mark}{item}", callback_data=f"{prefix}:{item}"))
        kb.row(*btns)
    ok_text = "✅ Подтвердить" if prefix=="auc" else "✅ Готово"
    kb.row(InlineKeyboardButton("↩️ Назад", callback_data=f"{prefix}_back"),
           InlineKeyboardButton(ok_text, callback_data=f"{prefix}_ok"))
    return kb

# ========= Состояния =========
CLASS_STATE = {}     # user_id -> selected_class
AUC_STATE = {}       # user_id -> set(items)  for /аук
ZABRAL_STATE = {}    # user_id -> set(items)  for /забрал

# ========= Help =========
@dp.message_handler(commands=["start","help_master"])
async def help_master(message: types.Message):
    text = (
        "Команды:\n"
        "• /ник <имя> — регистрация/смена ника\n"
        "• /класс — выбор класса (кнопки)\n"
        "• /бм <число> — обновить БМ (с историей)\n"
        "• /профиль — показать профиль\n"
        "• /топбм — топ-5 прироста БМ за 7 дней\n"
        "• /нет <дд.мм> <причина> — отметить отсутствие\n"
        "• /отсутствие [дд.мм причина] — быстрый учёт отсутствия\n"
        "• /аук — выбор предметов аукциона (множественный + подтверждение)\n"
        "• /очередь <предмет> — показать очередь по предмету\n"
        "• /выйти [предмет] — выйти из очереди (одной или всех)\n"
        "• /удалить <предмет> <ник> — удалить из очереди (офицеры/лидер)\n"
        "• /забрал — отметить получение предметов и уйти в конец очереди\n"
        "\n"
        "Привязки: /привязать_инфо, /привязать_аук, /привязать_отсутствие, /отвязать_все\n"
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
    reply = await message.answer(f"✅ Привязано: тема ИНФО.\nchat_id=`{message.chat.id}`\ninfo_topic_id=`{mtid}`", parse_mode="Markdown")
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
    reply = await message.answer(f"✅ Привязано: тема АУК.\nchat_id=`{message.chat.id}`\nauction_topic_id=`{mtid}`", parse_mode="Markdown")
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
    reply = await message.answer(f"✅ Привязано: тема ОТС.\nchat_id=`{message.chat.id}`\nabsence_topic_id=`{mtid}`", parse_mode="Markdown")
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
            reply = await message.answer(f"Текущий ник: {row[0]}\nИзмени так: /ник <новый_ник>")
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

@dp.message_handler(commands=["класс","klass"])
async def cmd_class(message: types.Message):
    if not in_scope(message, "info"): return
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT class FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
    current = row[0] if row and row[0] else "-"
    CLASS_STATE[tg_id] = None
    reply = await message.answer(f"🧙 Текущий класс: {current}\nВыбери новый класс:", reply_markup=class_keyboard())
    schedule_cleanup(message, reply, delay=30)

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

@dp.message_handler(commands=["профиль","profil"])
async def cmd_profile(message: types.Message):
    if not in_scope(message, "info"): return
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT username,nick,old_nicks,class,bm,bm_updated FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
    if not row:
        reply = await message.answer("Профиль не найден. Зарегистрируй ник: /ник <имя>"); return schedule_cleanup(message, reply)
    reply = await message.answer(f"Ник: {row[1]}\nСтарые ники: {row[2] or '-'}\nКласс: {row[3] or '-'}\nБМ: {row[4] or '-'}\nОбновлено: {row[5] or '-'}")
    schedule_cleanup(message, reply, delay=25)

@dp.message_handler(commands=["топбм","topbm"])
async def cmd_topbm(message: types.Message):
    if not in_scope(message, "info"): return
    week = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick, SUM(diff) as s FROM bm_history WHERE ts>=? GROUP BY nick ORDER BY s DESC LIMIT 5", (week,))
        rows = await cur.fetchall()
    if not rows:
        reply = await message.answer("Данных за 7 дней нет."); return schedule_cleanup(message, reply)
    reply = await message.answer("Топ прироста БМ за 7 дней:\n" + "\n".join([f"{i+1}. {r[0]} (+{r[1]})" for i,r in enumerate(rows)]))
    schedule_cleanup(message, reply, delay=25)

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
    schedule_cleanup(message, reply, delay=15)

# ========= Аукцион =========
@dp.message_handler(commands=["аук","auk"])
async def cmd_auction(message: types.Message):
    if not in_scope(message, "auction"): return
    if not (gsheet and gsheet.sheet):
        reply = await message.answer("Google Sheets недоступен. Проверь GOOGLE_CREDENTIALS и GSHEET_ID."); return schedule_cleanup(message, reply)
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        if not header:
            reply = await message.answer("Лист 'Аукцион' пуст или без шапки."); return schedule_cleanup(message, reply)
    except Exception as e:
        reply = await message.answer("Ошибка Google Sheets: " + str(e)); return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    AUC_STATE[tg_id] = set()
    reply = await message.answer("🎯 Выбери предметы аукциона (можно несколько):", reply_markup=auction_keyboard(AUC_STATE[tg_id], header, prefix="auc"))
    schedule_cleanup(message, reply, delay=60)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("auc:"))
async def auc_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":",1)[1]
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
    except:
        header = []
    if item not in header:
        return await callback_query.answer("Этот предмет сейчас недоступен")
    sel = AUC_STATE.setdefault(tg_id, set())
    if item in sel: sel.remove(item); note=f"Снято: {item}"
    else: sel.add(item); note=f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=auction_keyboard(sel, header, prefix="auc"))
    await callback_query.answer(note)

@dp.callback_query_handler(lambda c: c.data == "auc_back")
async def auc_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    AUC_STATE[tg_id] = set()
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
    except:
        header = []
    await callback_query.message.edit_reply_markup(reply_markup=auction_keyboard(AUC_STATE[tg_id], header, prefix="auc"))
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
    await callback_query.message.edit_text("\n".join(msgs))
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 15))
    await callback_query.answer("Сохранено")

@dp.message_handler(commands=["очередь","ochered"])
async def cmd_queue(message: types.Message):
    if not in_scope(message, "auction"): return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        reply = await message.answer("Использование: /очередь <предмет>"); return schedule_cleanup(message, reply, 20)
    item = parts[1].strip()
    if gsheet and gsheet.sheet:
        try:
            matrix, _ = gsheet.get_auction_matrix()
            header = matrix[0] if matrix else []
            if item not in header: reply = await message.answer("Очередь пуста или предмет не найден."); return schedule_cleanup(message, reply, 20)
            ci = header.index(item)
            col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
            col = [c for c in col if c]
            if not col: reply = await message.answer("Очередь пуста."); return schedule_cleanup(message, reply, 20)
            reply = await message.answer("Очередь — {}:\n{}".format(item, "\n".join("{}. {}".format(i+1,v) for i,v in enumerate(col))))
            return schedule_cleanup(message, reply, 25)
        except Exception as e:
            reply = await message.answer("Ошибка: " + str(e)); return schedule_cleanup(message, reply, 20)
    else:
        reply = await message.answer("Google Sheets недоступен. Проверь настройки."); return schedule_cleanup(message, reply, 20)

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
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        if not header:
            reply = await message.answer("Лист 'Аукцион' пуст."); return schedule_cleanup(message, reply)
    except Exception as e:
        reply = await message.answer("Ошибка Google Sheets: " + str(e)); return schedule_cleanup(message, reply)
    tg_id = message.from_user.id
    ZABRAL_STATE[tg_id] = set()
    reply = await message.answer("🎁 Отметь полученные предметы (можно несколько):", reply_markup=auction_keyboard(ZABRAL_STATE[tg_id], header, prefix="zabral"))
    schedule_cleanup(message, reply, delay=60)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("zabral:"))
async def zabral_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":",1)[1]
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
    except:
        header = []
    if item not in header:
        return await callback_query.answer("Этот предмет сейчас недоступен")
    sel = ZABRAL_STATE.setdefault(tg_id, set())
    if item in sel: sel.remove(item); note=f"Снято: {item}"
    else: sel.add(item); note=f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=auction_keyboard(sel, header, prefix="zabral"))
    await callback_query.answer(note)

@dp.callback_query_handler(lambda c: c.data == "zabral_back")
async def zabral_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    ZABRAL_STATE[tg_id] = set()
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
    except:
        header = []
    await callback_query.message.edit_reply_markup(reply_markup=auction_keyboard(ZABRAL_STATE[tg_id], header, prefix="zabral"))
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
    await callback_query.message.edit_text("\n".join(msgs))
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 15))
    await callback_query.answer("Сохранено")

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
