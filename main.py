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
import discord
from discord.ext import commands

############### CONSTANTS & CONFIG ###############
DATABASE_URL = os.getenv("DATABASE_URL")
TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN")

############### GLOBAL STATE / STORAGE ###############
db_pool = None

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

async def init_db():
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute("SET TIME ZONE 'UTC';")
        await conn.execute(GUILD_SETTINGS_SQL)

async def close_db():
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None

async def upsert_guild_setting_timezone(guild_id: int, timezone: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, timezone, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET timezone = EXCLUDED.timezone, updated_at = NOW();
            """,
            guild_id,
            timezone,
        )

async def get_guild_timezone(guild_id: int) -> str:
    async with db_pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT timezone FROM guild_settings WHERE guild_id = $1;",
            guild_id,
        )
    return value or "America/Los_Angeles"

def require_guild(interaction: discord.Interaction) -> int:
    if interaction.guild is None:
        raise RuntimeError("This command must be used in a server.")
    return interaction.guild.id

############### VIEWS / UI COMPONENTS ###############

############### AUTOCOMPLETE FUNCTIONS ###############

############### BACKGROUND TASKS & SCHEDULERS ###############

############### EVENT HANDLERS ###############

############### COMMAND GROUPS ###############
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.tree.command(name="ping", description="Bot heartbeat")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong ✅")

config_group = discord.app_commands.Group(name="config", description="Server configuration")

timezone_group = discord.app_commands.Group(name="timezone", description="Timezone settings", parent=config_group)

@timezone_group.command(name="set", description="Set this server's timezone")
@discord.app_commands.describe(tz="Example: America/Los_Angeles")
async def tz_set(interaction: discord.Interaction, tz: str):
    guild_id = require_guild(interaction)
    await upsert_guild_setting_timezone(guild_id, tz)
    await interaction.response.send_message(f"✅ Timezone set to `{tz}`", ephemeral=True)

@timezone_group.command(name="show", description="Show this server's timezone")
async def tz_show(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    tz = await get_guild_timezone(guild_id)
    await interaction.response.send_message(f"⏰ Current timezone: `{tz}`", ephemeral=True)

bot.tree.add_command(config_group)

############### ON_READY & BOT START ###############
@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"✅ Logged in as {bot.user} ({bot.user.id})")

async def runner():
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN or TOKEN is missing")
    await init_db()
    try:
        await bot.start(TOKEN)
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(runner())
