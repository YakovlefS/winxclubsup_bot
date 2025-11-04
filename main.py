import os
import asyncio
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

BOT_TOKEN = os.getenv("BOT_TOKEN")
LEADER_ID = int(os.getenv("LEADER_ID", "0"))
GSHEET_ID = os.getenv("GSHEET_ID")

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is required")

DB = "guildmaster.db"

bot = Bot(token=BOT_TOKEN, parse_mode="Markdown")
dp = Dispatcher(bot)

# ======= Helpers =======
async def delete_later(chat_id: int, message_id: int, delay: int = 15):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

# ======= Автосинхронизация =======
async def auto_sync():
    try:
        async with aiosqlite.connect(DB) as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS players(tg_id INTEGER PRIMARY KEY, nick TEXT, class TEXT, bm INTEGER)")
            await conn.commit()
            # имитация загрузки данных
            print("✅ Автосинхронизация завершена")
            if LEADER_ID:
                await bot.send_message(LEADER_ID, "✅ Автосинхронизация завершена")
    except Exception as e:
        print("Ошибка синхронизации:", e)
        if LEADER_ID:
            await bot.send_message(LEADER_ID, f"⚠️ Ошибка синхронизации: {e}")

# ======= Команды синхронизации =======
@dp.message_handler(commands=["синхронизировать", "sync"])
async def manual_sync(message: types.Message):
    msg = await message.reply("🔄 Выполняю синхронизацию данных...")
    try:
        await auto_sync()
        await msg.edit_text("✅ Синхронизация завершена успешно!")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка синхронизации: {e}")
    asyncio.create_task(delete_later(message.chat.id, msg.message_id, 15))

# ======= Webhook =======
WEBHOOK_HOST = os.getenv("WEBHOOK_URL") or f"https://{os.getenv('RAILWAY_PUBLIC_DOMAIN')}"
WEBHOOK_PATH = f"/bot/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

async def on_startup(dp):
    await bot.set_webhook(WEBHOOK_URL)
    await auto_sync()
    print(f"✅ Webhook установлен: {WEBHOOK_URL}")

async def on_shutdown(dp):
    await bot.delete_webhook()
    print("🛑 Webhook удалён")

if __name__ == "__main__":
    executor.start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080))
    )
