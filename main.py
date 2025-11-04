
import os, datetime
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

SCOPE_CHAT_ID = None
SCOPE_TOPIC_INFO = None
SCOPE_TOPIC_AUCTION = None

async def get_setting(conn, key):
    cur = await conn.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cur.fetchone()
    return row[0] if row else None

async def set_setting(conn, key, value):
    await conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
    await conn.commit()

async def load_scope():
    global SCOPE_CHAT_ID, SCOPE_TOPIC_INFO, SCOPE_TOPIC_AUCTION
    async with aiosqlite.connect(DB) as conn:
        chat = await get_setting(conn, "scope_chat_id")
        info = await get_setting(conn, "scope_topic_info")
        auction = await get_setting(conn, "scope_topic_auction")
    SCOPE_CHAT_ID = int(chat) if chat not in (None, "") else None
    SCOPE_TOPIC_INFO = int(info) if info not in (None, "") else None
    SCOPE_TOPIC_AUCTION = int(auction) if auction not in (None, "") else None

def in_scope(message: types.Message, role: str) -> bool:
    if SCOPE_CHAT_ID is not None and message.chat.id != SCOPE_CHAT_ID:
        return False
    mtid = getattr(message, "message_thread_id", None)
    if role == "info" and SCOPE_TOPIC_INFO is not None and mtid != SCOPE_TOPIC_INFO:
        return False
    if role == "auction" and SCOPE_TOPIC_AUCTION is not None and mtid != SCOPE_TOPIC_AUCTION:
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
    if uname:
        if ("@" + uname) in OFFICERS:
            return True
    return False

async def only_leader_officers(message: types.Message) -> bool:
    return is_leader(message) or is_officer(message)

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
        BotCommand("privyazat_info","Привязать тему персонажей"),
        BotCommand("privyazat_auk","Привязать тему аукциона"),
        BotCommand("otvyazat_vse","Сбросить привязки"),
        BotCommand("help_master","Список команд"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def class_keyboard():
    kb = InlineKeyboardMarkup(row_width=3)
    rows = list(chunk(CLASS_LIST, 3))
    for row in rows:
        kb.row(*[InlineKeyboardButton(text=txt, callback_data=f"class:{txt}") for txt in row])
    kb.row(
        InlineKeyboardButton("↩️ Назад", callback_data="class_back"),
        InlineKeyboardButton("✅ Готово", callback_data="class_ok")
    )
    return kb

def auction_keyboard(selected:set, header:list):
    kb = InlineKeyboardMarkup(row_width=3)
    titles = header
    for row in chunk(titles, 3):
        btns=[]
        for item in row:
            mark = "✅ " if item in selected else ""
            btns.append(InlineKeyboardButton(text=f"{mark}{item}", callback_data=f"auc:{item}"))
        kb.row(*btns)
    kb.row(
        InlineKeyboardButton("↩️ Назад", callback_data="auc_back"),
        InlineKeyboardButton("✅ Подтвердить", callback_data="auc_ok")
    )
    return kb

CLASS_STATE = {}
AUC_STATE = {}

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
        "• /аук — выбор предметов аукциона (множественный + подтверждение)\n"
        "• /очередь <предмет> — показать очередь по предмету\n"
        "\n"
        "Команды привязки (только лидер/офицеры): /привязать_инфо, /привязать_аук, /отвязать_все\n"
    )
    await message.answer(text)

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
    await message.answer(f"✅ Привязано: тема ИНФО.\nchat_id=`{message.chat.id}`\ninfo_topic_id=`{mtid}`", parse_mode="Markdown")

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
    await message.answer(f"✅ Привязано: тема АУКЦИОН.\nchat_id=`{message.chat.id}`\nauction_topic_id=`{mtid}`", parse_mode="Markdown")

@dp.message_handler(commands=["отвязать_все"])
async def unbind_all(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return await message.answer("Команда доступна только в группе.")
    if not await only_leader_officers(message):
        return await message.answer("Недостаточно прав.")
    async with aiosqlite.connect(DB) as conn:
        await set_setting(conn, "scope_topic_info", "")
        await set_setting(conn, "scope_topic_auction", "")
    await load_scope()
    await message.answer("✅ Все привязки тем сняты. Бот остаётся привязан к группе.")

@dp.message_handler(commands=["ник"])
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
            return await message.answer(f"Текущий ник: {row[0]}\nВведи новый ник: /ник <новый_ник>")
        else:
            return await message.answer("Использование: /ник <имя>")
    new_nick = parts[1].strip()
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB) as conn:
        if row and row[0]:
            old = row[0]
            cur2 = await conn.execute("SELECT old_nicks FROM players WHERE tg_id=?", (tg_id,))
            orow = await cur2.fetchone()
            olds = (orow[0] or "") if orow else ""
            new_olds = (olds + ";" if olds else "") + old if old != new_nick else olds
            await conn.execute("UPDATE players SET nick=?, old_nicks=?, username=?, bm_updated=? WHERE tg_id=?", (new_nick, new_olds, username, now, tg_id))
        else:
            await conn.execute("INSERT INTO players(tg_id,username,nick,bm_updated) VALUES(?,?,?,?)", (tg_id, username, new_nick, now))
        await conn.commit()
        if gsheet and gsheet.sheet:
            cur3 = await conn.execute("SELECT tg_id, username, nick, old_nicks, class, bm, bm_updated FROM players WHERE tg_id=?", (tg_id,))
            pr = await cur3.fetchone()
            player = {"tg_id": pr[0], "telegram": pr[1], "nick": pr[2], "old_nicks": pr[3] or "", "class": pr[4] or "", "current_bm": pr[5] or "", "bm_updated": pr[6] or ""}
            try: gsheet.update_player(player)
            except: pass
    await message.answer(f"Ник сохранён: {new_nick}")

@dp.message_handler(commands=["класс"])
async def cmd_class(message: types.Message):
    if not in_scope(message, "info"): return
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT class FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
    current = row[0] if row and row[0] else "-"
    CLASS_STATE[tg_id] = None
    await message.answer(f"🧙 Текущий класс: {current}\nВыбери новый класс:", reply_markup=class_keyboard())

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
            try: gsheet.update_player(player)
            except: pass
    CLASS_STATE[tg_id] = None
    await callback_query.message.edit_text(f"✅ Класс обновлён: {sel}")
    await callback_query.answer("Сохранено")

@dp.message_handler(commands=["бм"])
async def cmd_bm(message: types.Message):
    if not in_scope(message, "info"): return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        return await message.answer("Использование: /бм <число>")
    new_bm = int(parts[1].strip())
    tg_id = message.from_user.id
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick,bm,class FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row:
            return await message.answer("Сначала зарегистрируй ник: /ник <имя>")
        nick, old_bm, cls = row[0], row[1] or 0, row[2] or ""
        await conn.execute("UPDATE players SET bm=?, bm_updated=? WHERE tg_id=?", (new_bm, now, tg_id))
        await conn.execute("INSERT INTO bm_history(tg_id,nick,old_bm,new_bm,diff,ts) VALUES(?,?,?,?,?,?)", (tg_id, nick, old_bm, new_bm, new_bm - old_bm, now))
        await conn.commit()
        if gsheet and gsheet.sheet:
            try:
                cur2 = await conn.execute("SELECT tg_id, username, nick, old_nicks, class, bm, bm_updated FROM players WHERE tg_id=?", (tg_id,))
                pr = await cur2.fetchone()
                player = {"tg_id": pr[0], "telegram": pr[1], "nick": pr[2], "old_nicks": pr[3] or "", "class": pr[4] or "", "current_bm": pr[5] or "", "bm_updated": pr[6] or ""}
                gsheet.update_player(player)
                gsheet.append_bm_history({"tg_id":tg_id,"nick":nick,"class":cls,"old_bm":old_bm,"new_bm":new_bm,"diff":new_bm-old_bm,"ts":now})
            except: pass
    await message.answer(f"БМ обновлён: {old_bm} → {new_bm} (прирост {new_bm-old_bm})")

@dp.message_handler(commands=["профиль"])
async def cmd_profile(message: types.Message):
    if not in_scope(message, "info"): return
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT username,nick,old_nicks,class,bm,bm_updated FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
    if not row:
        return await message.answer("Профиль не найден. Зарегистрируй ник: /ник <имя>")
    await message.answer(f"Ник: {row[1]}\nСтарые ники: {row[2] or '-'}\nКласс: {row[3] or '-'}\nБМ: {row[4] or '-'}\nОбновлено: {row[5] or '-'}")

@dp.message_handler(commands=["топбм"])
async def cmd_topbm(message: types.Message):
    if not in_scope(message, "info"): return
    week = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick, SUM(diff) as s FROM bm_history WHERE ts>=? GROUP BY nick ORDER BY s DESC LIMIT 5", (week,))
        rows = await cur.fetchall()
    if not rows:
        return await message.answer("Данных за 7 дней нет.")
    lines = []
    for i, r in enumerate(rows):
        lines.append(f"{i+1}. {r[0]} (+{r[1]})")
    await message.answer("Топ прироста БМ за 7 дней:\n" + "\n".join(lines))

@dp.message_handler(commands=["нет"])
async def cmd_absence(message: types.Message):
    if not in_scope(message, "info"): return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        return await message.answer("Использование: /нет <дд.мм> <причина>")
    date = parts[1].strip()
    reason = parts[2].strip()
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row:
            return await message.answer("Сначала зарегистрируй ник: /ник <имя>")
        nick = row[0]
        try:
            if gsheet and gsheet.sheet:
                gsheet.append_absence(date, nick, message.from_user.username or message.from_user.full_name, reason)
        except: pass
    await message.answer("Отсутствие добавлено.")

def parse_items(text):
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return []
    return [p.strip() for p in parts[1].replace(',', ' ').split() if p.strip()]

@dp.message_handler(commands=["аук"])
async def cmd_auction(message: types.Message):
    if not in_scope(message, "auction"): return
    tg_id = message.from_user.id
    if not (gsheet and gsheet.sheet):
        return await message.answer("Google Sheets недоступен. Проверь GOOGLE_CREDENTIALS и GSHEET_ID.")
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        if not header:
            return await message.answer("Лист 'Аукцион' пуст или без шапки.")
    except Exception as e:
        return await message.answer("Ошибка Google Sheets: " + str(e))
    AUC_STATE[tg_id] = set()
    await message.answer("🎯 Выбери предметы аукциона (можно несколько):", reply_markup=auction_keyboard(AUC_STATE[tg_id], header))

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
        return await callback_query.answer("Этот предмет сейчас недоступен", show_alert=False)
    sel = AUC_STATE.setdefault(tg_id, set())
    if item in sel:
        sel.remove(item); note = f"Снято: {item}"
    else:
        sel.add(item); note = f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=auction_keyboard(sel, header))
    await callback_query.answer(note, show_alert=False)

@dp.callback_query_handler(lambda c: c.data == "auc_back")
async def auc_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    AUC_STATE[tg_id] = set()
    try:
        matrix, _ = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
    except:
        header = []
    await callback_query.message.edit_reply_markup(reply_markup=auction_keyboard(AUC_STATE[tg_id], header))
    await callback_query.answer("Выбор сброшен")

@dp.callback_query_handler(lambda c: c.data == "auc_ok")
async def auc_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    sel = AUC_STATE.get(tg_id, set())
    if not sel:
        return await callback_query.answer("Сначала выбери предметы")
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT nick FROM players WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        if not row or not row[0]:
            return await callback_query.answer("Сначала зарегистрируй ник: /ник <имя>", show_alert=True)
        nick = row[0]
    try:
        matrix, ws = gsheet.get_auction_matrix()
        header = matrix[0] if matrix else []
        msgs = []
        for item in sel:
            if item not in header:
                continue
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
            while len(matrix)-1 < max_len:
                matrix.append(['']*len(header))
            for i in range(max_len):
                matrix[i+1][ci] = col[i] if i < len(col) else ''
        gsheet.write_auction_matrix(ws, matrix)
    except Exception as e:
        return await callback_query.message.edit_text("Ошибка Google Sheets: " + str(e))
    AUC_STATE[tg_id] = set()
    await callback_query.message.edit_text("\n".join(msgs))
    await callback_query.answer("Сохранено")

@dp.message_handler(commands=["очередь"])
async def cmd_queue(message: types.Message):
    if not in_scope(message, "auction"): return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.answer("Использование: /очередь <предмет>")
    item = parts[1].strip()
    if gsheet and gsheet.sheet:
        try:
            matrix, _ = gsheet.get_auction_matrix()
            header = matrix[0] if matrix else []
            if item not in header:
                return await message.answer("Очередь пуста или предмет не найден.")
            ci = header.index(item)
            col = [r[ci] if len(r)>ci else '' for r in matrix[1:]]
            col = [c for c in col if c]
            if not col:
                return await message.answer("Очередь пуста.")
            return await message.answer("Очередь — {}:\n{}".format(item, "\n".join("{}. {}".format(i+1,v) for i,v in enumerate(col))))
        except Exception as e:
            return await message.answer("Ошибка: " + str(e))
    else:
        return await message.answer("Google Sheets недоступен. Проверь настройки.")

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
        except: pass
    print("Bot started; scope:", "chat_id", SCOPE_CHAT_ID, "info", SCOPE_TOPIC_INFO, "auction", SCOPE_TOPIC_AUCTION)

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
