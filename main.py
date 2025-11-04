import os
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import BotCommand, BotCommandScopeAllGroupChats
from db import init_db

BOT_TOKEN=os.getenv("BOT_TOKEN")
bot=Bot(token=BOT_TOKEN)
dp=Dispatcher(bot)
OFFICERS=["@Maffins89", "@Gi_Di_Al", "@oOMEMCH1KOo", "@Ferbi55", "@Ahaha_Ohoho"]

async def set_commands():
    cmds=[
        BotCommand("nik","Регистрация или смена ника"),
        BotCommand("klass","Указать класс"),
        BotCommand("bm","Обновить БМ"),
        BotCommand("profil","Показать профиль"),
        BotCommand("topbm","Топ прироста БМ"),
        BotCommand("net","Сообщить об отсутствии"),
        BotCommand("auk","Запись в очередь"),
        BotCommand("ochered","Показать очередь"),
        BotCommand("privyazat_info","Привязать тему персонажей"),
        BotCommand("privyazat_auk","Привязать тему аукциона"),
        BotCommand("otvyazat_vse","Сбросить все привязки"),
        BotCommand("help","Помощь")
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())

@dp.message_handler(commands=['start','help'])
async def help_cmd(message: types.Message):
    await message.answer("Бот активен. Используйте /ник, /класс, /бм, /аук и другие команды.")

async def on_startup(_):
    await init_db()
    await set_commands()
    print("✅ Bot started with fixed command menu (latin-safe).")

if __name__=='__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
