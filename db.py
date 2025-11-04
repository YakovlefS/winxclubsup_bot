import aiosqlite, asyncio, os
DB=os.path.join(os.path.dirname(__file__),'guildmaster.db')
async def init_db():
 async with aiosqlite.connect(DB) as db:
  await db.execute('CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT)')
  await db.commit()
if __name__=='__main__': asyncio.run(init_db())
