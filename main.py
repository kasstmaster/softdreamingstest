# ============================================================
# RULES FOR CHATGPT AND GROK (DO NOT VIOLATE)
# • Use ONLY these sections, in this exact order:
#   ############### IMPORTS ###############
#   ############### CONSTANTS & CONFIG ###############
#   ############### GLOBAL STATE / STORAGE ###############
#   ############### HELPER FUNCTIONS ###############
#   ############### VIEWS / UI COMPONENTS ###############
#   ############### AUTOCOMPLETE FUNCTIONS ###############
#   ############### BACKGROUND TASKS & SCHEDULERS ###############
#   ############### EVENT HANDLERS ###############
#   ############### COMMAND GROUPS ###############
#   ############### ON_READY & BOT START ###############
# • Do NOT add any other sections.
# • Do NOT add comments inside the code. No inline labels.
# ============================================================

############### IMPORTS ###############
import os
import asyncio
import asyncpg

############### CONSTANTS & CONFIG ###############
DATABASE_URL = os.getenv("DATABASE_URL")

############### GLOBAL STATE / STORAGE ###############

############### HELPER FUNCTIONS ###############
async def db_heartbeat():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    conn = await asyncpg.connect(DATABASE_URL)
    value = await conn.fetchval("SELECT 1;")
    await conn.close()
    print("✅ Connected to Postgres, SELECT 1 returned:", value)

############### VIEWS / UI COMPONENTS ###############

############### AUTOCOMPLETE FUNCTIONS ###############

############### BACKGROUND TASKS & SCHEDULERS ###############

############### EVENT HANDLERS ###############

############### COMMAND GROUPS ###############

############### ON_READY & BOT START ###############
if __name__ == "__main__":
    asyncio.run(db_heartbeat())

