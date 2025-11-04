import aiosqlite, asyncio, os
DB = os.path.join(os.path.dirname(__file__), 'guildmaster.db')

async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS players (
    tg_id INTEGER PRIMARY KEY,
    username TEXT,
    nick TEXT,
    old_nicks TEXT,
    class TEXT,
    bm INTEGER,
    bm_updated TEXT
)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS bm_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER,
    nick TEXT,
    old_bm INTEGER,
    new_bm INTEGER,
    diff INTEGER,
    ts TEXT
)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS absences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER,
    nick TEXT,
    date TEXT,
    reason TEXT,
    ts TEXT
)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT,
    tg_id INTEGER,
    nick TEXT,
    action TEXT,
    details TEXT
)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
)""")
        await db.commit()

if __name__=='__main__':
    asyncio.run(init_db())
