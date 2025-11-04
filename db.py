import os, aiosqlite

DB = os.getenv("DB_PATH", "guildmaster.db")

async def init_db():
    async with aiosqlite.connect(DB) as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS players(
            tg_id INTEGER PRIMARY KEY,
            username TEXT,
            nick TEXT,
            old_nicks TEXT,
            class TEXT,
            bm INTEGER,
            bm_updated TEXT
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS bm_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            nick TEXT,
            old_bm INTEGER,
            new_bm INTEGER,
            diff INTEGER,
            ts TEXT
        )""")
        await conn.commit()
