import os
import json
import asyncio
import aiosqlite
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# ========== ENV ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
LEADER_ID = int(os.getenv("LEADER_ID", "0"))
OFFICER_IDS = {int(x) for x in (os.getenv("OFFICER_IDS") or "").replace(" ", "").split(",") if x.isdigit()}

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is required")

DB = "guildmaster.db"

# ========== BOT ==========
bot = Bot(token=BOT_TOKEN, parse_mode="Markdown")
dp = Dispatcher(bot)

# ========== GOOGLE SHEETS WRAPPER ==========
gsheet = None
try:
    import gspread
    from google.oauth2.service_account import Credentials

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    def make_creds():
        if not GOOGLE_CREDENTIALS:
            return None
        data = json.loads(GOOGLE_CREDENTIALS)
        return Credentials.from_service_account_info(data, scopes=SCOPES)

    class GSheetWrapper:
        def __init__(self, sheet_id: str):
            self.sheet_id = sheet_id
            self.gc = None
            self.sheet = None
            self._connect()

        def _connect(self):
            creds = make_creds()
            if not creds:
                return
            self.gc = gspread.authorize(creds)
            self.sheet = self.gc.open_by_key(self.sheet_id)

        def ensure_tabs(self):
            if not self.sheet:
                return
            needed = ["Игроки", "Аукцион", "settings"]
            existing = {ws.title for ws in self.sheet.worksheets()}
            for name in needed:
                if name not in existing:
                    self.sheet.add_worksheet(title=name, rows=1000, cols=26)
            # Минимальные хедеры
            try:
                ws = self.sheet.worksheet("Игроки")
                vals = ws.get_all_values()
                if not vals:
                    ws.update("A1:F1", [["tg_id", "telegram", "nick", "old_nicks", "class", "current_bm"]])
            except Exception:
                pass
            try:
                ws = self.sheet.worksheet("Аукцион")
                vals = ws.get_all_values()
                if not vals:
                    ws.update("A1:C1", [["Булла_Ред", "Клеймо", "Галун"]])
            except Exception:
                pass

        def get_auction_matrix(self):
            ws = self.sheet.worksheet("Аукцион")
            data = ws.get_all_values()
            return (data, ws)

        def get_players(self):
            ws = self.sheet.worksheet("Игроки")
            return ws.get_all_values()

        def upsert_player(self, player: dict):
            """player keys: tg_id, telegram, nick, old_nicks, class, current_bm"""
            ws = self.sheet.worksheet("Игроки")
            data = ws.get_all_values()
            if not data:
                ws.update("A1:F1", [["tg_id","telegram","nick","old_nicks","class","current_bm"]])
                data = ws.get_all_values()
            header = data[0]
            idx = {name: header.index(name) for name in header if name in header}
            # ищем строку по tg_id
            row_idx = None
            if "tg_id" in idx:
                for i, row in enumerate(data[1:], start=2):
                    if len(row) > idx["tg_id"] and row[idx["tg_id"]].strip() == str(player.get("tg_id","")):
                        row_idx = i
                        break
            # подготовим строку
            out = [""] * max(6, len(header))
            out[idx.get("tg_id",0)] = str(player.get("tg_id","") or "")
            out[idx.get("telegram",1)] = player.get("telegram","") or ""
            out[idx.get("nick",2)] = player.get("nick","") or ""
            out[idx.get("old_nicks",3)] = player.get("old_nicks","") or ""
            out[idx.get("class",4)] = player.get("class","") or ""
            out[idx.get("current_bm",5)] = str(player.get("current_bm","") or "")
            if row_idx:
                ws.update(f"A{row_idx}:F{row_idx}", [out[:6]])
            else:
                ws.append_row(out[:6], value_input_option="USER_ENTERED")

    if GSHEET_ID:
        gsheet = GSheetWrapper(GSHEET_ID)
        gsheet.ensure_tabs()
except Exception as e:
    print("GSheets init error:", e)

# ========== DATA ==========
CLASS_LIST = [
    "Вульпин","Варвар","Лучник","Жрец","Воин",
    "Маг","Убийца","Окультист","Дух меча","Отшельник","Мечник"
]
CLASS_STATE = {}
QUEUE_STATE = {}

# ========== HELPERS ==========
async def delete_later(chat_id: int, message_id: int, delay: int = 15):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

async def schedule_cleanup(user_message: types.Message, bot_reply: types.Message, delay: int = 15):
    try:
        await bot.delete_message(user_message.chat.id, user_message.message_id)
    except Exception:
        pass
    asyncio.create_task(delete_later(bot_reply.chat.id, bot_reply.message_id, delay))

def split_rows(items, n=3):
    return [items[i:i+n] for i in range(0, len(items), n)]

def is_leader(message: types.Message) -> bool:
    return LEADER_ID and message.from_user.id == LEADER_ID

def is_officer(message: types.Message) -> bool:
    return message.from_user.id in OFFICER_IDS

def leader_or_officer(message: types.Message) -> bool:
    return is_leader(message) or is_officer(message)

# ========== DB INIT ==========
async def ensure_db():
    async with aiosqlite.connect(DB) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS players("
            "tg_id INTEGER PRIMARY KEY, "
            "nick TEXT, old_nicks TEXT, class TEXT, bm INTEGER, bm_updated TEXT)"
        )
        await conn.commit()

# ========== CLASS SELECT (/класс) ==========
@dp.message_handler(commands=["класс","klass"])
async def choose_class(message: types.Message):
    await ensure_db()
    async with aiosqlite.connect(DB) as conn:
        cur = await conn.execute("SELECT class FROM players WHERE tg_id=?", (message.from_user.id,))
        row = await cur.fetchone()
        user_class = row[0] if row else None

    buttons = []
    for cls in CLASS_LIST:
        btn_text = f"✅ {cls}" if cls == user_class else cls
        buttons.append(types.InlineKeyboardButton(text=btn_text, callback_data=f"class:{cls}"))

    kb = types.InlineKeyboardMarkup(row_width=3)
    for row_btns in split_rows(buttons, 3):
        kb.row(*row_btns)
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="class_back"),
        types.InlineKeyboardButton("✅ Готово", callback_data="class_ok"),
    )
    msg = await message.reply("🎓 Выбери свой класс:", reply_markup=kb)
    await schedule_cleanup(message, msg, 30)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("class:"))
async def class_pick(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    _, picked = callback_query.data.split(":", 1)
    CLASS_STATE[tg_id] = picked

    buttons = []
    for cls in CLASS_LIST:
        btn_text = f"✅ {cls}" if cls == picked else cls
        buttons.append(types.InlineKeyboardButton(text=btn_text, callback_data=f"class:{cls}"))
    kb = types.InlineKeyboardMarkup(row_width=3)
    for row_btns in split_rows(buttons, 3):
        kb.row(*row_btns)
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="class_back"),
        types.InlineKeyboardButton("✅ Готово", callback_data="class_ok"),
    )
    try:
        await callback_query.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass
    await callback_query.answer(f"Выбран класс: {picked}")

@dp.callback_query_handler(lambda c: c.data == "class_back")
async def class_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    CLASS_STATE[tg_id] = None
    await callback_query.message.edit_text("🔙 Выбор отменён.")
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 10))

@dp.callback_query_handler(lambda c: c.data == "class_ok")
async def class_ok(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    chosen = CLASS_STATE.get(tg_id)
    if not chosen:
        return await callback_query.answer("Сначала выбери класс")
    await ensure_db()
    async with aiosqlite.connect(DB) as conn:
        await conn.execute("INSERT OR IGNORE INTO players(tg_id) VALUES(?)", (tg_id,))
        await conn.execute("UPDATE players SET class=?, bm_updated=? WHERE tg_id=?", (chosen, datetime.utcnow().isoformat(), tg_id))
        await conn.commit()
    try:
        if gsheet and gsheet.sheet:
            username = callback_query.from_user.username or callback_query.from_user.full_name
            gsheet.upsert_player({
                "tg_id": tg_id, "telegram": username, "nick": "",
                "old_nicks": "", "class": chosen, "current_bm": ""
            })
    except Exception as e:
        print("GSheet push error (class):", e)

    await callback_query.message.edit_text(f"✅ Класс сохранён: *{chosen}*")
    asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 10))
    await callback_query.answer("Сохранено")

# ========== PROFILE (/профиль [ник]) ==========
@dp.message_handler(commands=["профиль","profil","profile"])
async def show_profile(message: types.Message):
    await ensure_db()
    arg = ""
    parts = message.text.split(maxsplit=1)
    if len(parts) == 2:
        arg = parts[1].strip()

    async with aiosqlite.connect(DB) as conn:
        if arg:
            cur = await conn.execute("SELECT nick, old_nicks, class, bm, bm_updated FROM players WHERE nick LIKE ?", (arg,))
        else:
            cur = await conn.execute("SELECT nick, old_nicks, class, bm, bm_updated FROM players WHERE tg_id=?", (message.from_user.id,))
        row = await cur.fetchone()

    if not row:
        msg = await message.reply("⚠️ Профиль не найден. Укажи /ник, /класс и /бм для регистрации.")
        return await schedule_cleanup(message, msg, 15)

    nick, old_nicks, cls, bm, updated = row
    bm_str = f"{(bm or 0):,}".replace(",", " ")
    old_nicks = old_nicks if (old_nicks or "").strip() else "-"
    updated = updated if (updated or "").strip() else "-"

    text = (
        "🧙‍♂️ *Профиль игрока*\n\n"
        f"🎮 Ник: *{nick or '-'}*\n"
        f"🕰 Старые ники: {old_nicks}\n"
        f"⚔️ Класс: {cls or '-'}\n"
        f"💪 Боевой рейтинг: *{bm_str}*\n"
        f"📅 Последнее обновление: {updated}"
    )
    msg = await message.reply(text)
    await schedule_cleanup(message, msg, 15)

# ========== QUEUE VIEW (/очередь) ==========
def get_items_safe():
    try:
        if not (gsheet and gsheet.sheet):
            return []
        matrix, _ws = gsheet.get_auction_matrix()
        return matrix[0] if matrix else []
    except Exception as e:
        print("get_items_safe error:", e)
        return []

def queue_keyboard(selected:set):
    header = get_items_safe()
    kb = types.InlineKeyboardMarkup(row_width=3)
    for row in split_rows(header, 3):
        btns = []
        for item in row:
            mark = "✅ " if item in selected else ""
            btns.append(types.InlineKeyboardButton(text=f"{mark}{item}", callback_data=f"qsel:{item}"))
        if btns:
            kb.row(*btns)
    kb.row(
        types.InlineKeyboardButton("↩️ Назад", callback_data="qsel_back"),
        types.InlineKeyboardButton("✅ Показать", callback_data="qsel_ok"),
    )
    return kb

@dp.message_handler(commands=["очередь","ochered","queue"])
async def cmd_queue(message: types.Message):
    header = get_items_safe()
    if not header:
        msg = await message.answer("⚠️ Предметы аукциона не найдены. Проверь лист *Аукцион* в Google Sheets.")
        return await schedule_cleanup(message, msg, 15)
    tg_id = message.from_user.id
    QUEUE_STATE[tg_id] = set()
    kb = queue_keyboard(QUEUE_STATE[tg_id])
    msg = await message.answer("📜 Выберите предметы для просмотра очередей (можно несколько):", reply_markup=kb)
    await schedule_cleanup(message, msg, 60)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("qsel:"))
async def qsel_toggle(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    item = callback_query.data.split(":",1)[1]
    header = get_items_safe()
    if item not in header:
        return await callback_query.answer("Этот предмет недоступен")
    sel = QUEUE_STATE.setdefault(tg_id, set())
    if item in sel:
        sel.remove(item); note = f"Снято: {item}"
    else:
        sel.add(item); note = f"Выбрано: {item}"
    await callback_query.message.edit_reply_markup(reply_markup=queue_keyboard(sel))
    await callback_query.answer(note)

@dp.callback_query_handler(lambda c: c.data == "qsel_back")
async def qsel_back(callback_query: types.CallbackQuery):
    tg_id = callback_query.from_user.id
    QUEUE_STATE[tg_id] = set()
    await callback_query.message.edit_reply_markup(reply_markup=queue_keyboard(QUEUE_STATE[tg_id]))
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
                uname = (name or "").strip()
                if username and uname.lower() == (username or "").lower():
                    formatted_lines.append(f"{i}. **@{uname}**")
                    user_pos = i
                else:
                    formatted_lines.append(f"{i}. @{uname}")
            if not formatted_lines:
                text_block = f"💎 Очередь по предмету: *{item}*\n━━━━━━━━━━━━━━━━━━\n(пока пуста)\n━━━━━━━━━━━━━━━━━━"
            else:
                text_block = f"💎 Очередь по предмету: *{item}*\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(formatted_lines)
                if user_pos:
                    text_block += f"\n\n📍 Твоя позиция: №{user_pos}"
                text_block += "\n━━━━━━━━━━━━━━━━━━"
            blocks.append(text_block)
        final_text = (f"📋 Запросил: @{username}\n\n" if username else "") + "\n\n".join(blocks)
        await callback_query.message.edit_text(final_text, parse_mode="Markdown")
        asyncio.create_task(delete_later(callback_query.message.chat.id, callback_query.message.message_id, 15))
        await callback_query.answer("Очередь обновлена")
    except Exception as e:
        await callback_query.message.edit_text(f"⚠️ Ошибка: {e}")

# ========== SYNC: авто и ручной ==========
async def auto_sync():
    try:
        await ensure_db()
        if not (gsheet and gsheet.sheet):
            print("auto_sync: Sheets not available, skip.")
            return
        async with aiosqlite.connect(DB) as conn:
            data = gsheet.get_players()
            if not data:
                return
            header = data[0]
            def idx(name, default=None):
                return header.index(name) if name in header else default
            idx_tg = idx("tg_id", 0)
            idx_nick = idx("nick", 2 if len(header)>2 else None)
            idx_class = idx("class", None)
            idx_bm = idx("current_bm", None)

            count = 0
            for row in data[1:]:
                if not row: continue
                tg_id = None
                try:
                    if idx_tg is not None and len(row) > idx_tg and row[idx_tg].strip().isdigit():
                        tg_id = int(row[idx_tg])
                except Exception:
                    tg_id = None
                nick = row[idx_nick] if idx_nick is not None and len(row) > idx_nick else ""
                cls = row[idx_class] if idx_class is not None and len(row) > idx_class else ""
                bm = 0
                try:
                    if idx_bm is not None and len(row) > idx_bm and row[idx_bm].strip().isdigit():
                        bm = int(row[idx_bm])
                except Exception:
                    bm = 0
                if tg_id is None and not nick:
                    continue
                await conn.execute(
                    "INSERT OR REPLACE INTO players(tg_id,nick,class,bm,bm_updated) VALUES(?,?,?,?,?)",
                    (tg_id, nick, cls, bm, datetime.utcnow().isoformat())
                )
                count += 1
            await conn.commit()
        msg = f"✅ Автосинхронизация завершена\n👥 Обновлено игроков: {count}"
        print(msg)
        if LEADER_ID:
            try:
                await bot.send_message(LEADER_ID, msg)
            except Exception:
                pass
    except Exception as e:
        err = f"⚠️ Ошибка автосинхронизации: {e}"
        print(err)
        if LEADER_ID:
            try:
                await bot.send_message(LEADER_ID, err)
            except Exception:
                pass

@dp.message_handler(commands=["синхронизировать","sync"])
async def manual_sync(message: types.Message):
    msg = await message.reply("🔄 Выполняю синхронизацию данных...")
    try:
        await auto_sync()
        await msg.edit_text("✅ Синхронизация завершена успешно!")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка синхронизации: {e}")
    asyncio.create_task(delete_later(message.chat.id, msg.message_id, 15))

# ========== HELP ==========
@dp.message_handler(commands=["help_master"])
async def help_master(message: types.Message):
    text = (
        """*Команды бота*

• /класс — выбор класса (с отметкой ✅)
• /профиль [ник] — показать профиль (свой или другого)
• /очередь — выбор предметов и просмотр очередей
• /синхронизировать (или /sync) — ручная синхронизация
• Сообщения игроков удаляются сразу, ответы бота — через 15 сек
"""
    )
    msg = await message.reply(text)
    await schedule_cleanup(message, msg, 20)


# ========== WEBHOOK MODE ==========
WEBHOOK_HOST = os.getenv("WEBHOOK_URL") or (f"https://{os.getenv('RAILWAY_PUBLIC_DOMAIN')}" if os.getenv('RAILWAY_PUBLIC_DOMAIN') else None)
WEBHOOK_PATH = f"/bot/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

async def on_startup(dp):
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        print(f"✅ Webhook set: {WEBHOOK_URL}")
    else:
        print("⚠️ WEBHOOK_URL/RAILWAY_PUBLIC_DOMAIN not set; webhook can't be enabled.")
    await auto_sync()

async def on_shutdown(dp):
    try:
        await bot.delete_webhook()
        print("🛑 Webhook removed")
    except Exception:
        pass

if __name__ == "__main__":
    executor.start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH if WEBHOOK_URL else f"/bot/{BOT_TOKEN}",
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080))
    )
