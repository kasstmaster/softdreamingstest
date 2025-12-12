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
GUILD_SETTINGS_SQL = """
CREATE TABLE IF NOT EXISTS guild_settings (
  guild_id BIGINT PRIMARY KEY,
  active_role_id BIGINT NULL,
  active_threshold_minutes INT NOT NULL DEFAULT 60,
  deadchat_role_id BIGINT NULL,
  deadchat_idle_minutes INT NOT NULL DEFAULT 30,
  deadchat_requires_active BOOLEAN NOT NULL DEFAULT TRUE,
  deadchat_cooldown_minutes INT NOT NULL DEFAULT 60,
  plague_role_id BIGINT NULL,
  plague_duration_hours INT NOT NULL DEFAULT 72,
  prizes_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

async def ensure_schema():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("SET TIME ZONE 'UTC';")
    await conn.execute(GUILD_SETTINGS_SQL)
    value = await conn.fetchval("SELECT 1;")
    await conn.close()
    print("✅ Schema ensured, SELECT 1 returned:", value)

############### VIEWS / UI COMPONENTS ###############

############### AUTOCOMPLETE FUNCTIONS ###############

############### BACKGROUND TASKS & SCHEDULERS ###############

############### EVENT HANDLERS ###############

############### COMMAND GROUPS ###############

############### ON_READY & BOT START ###############
if __name__ == "__main__":
    asyncio.run(ensure_schema())

