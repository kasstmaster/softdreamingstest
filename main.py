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

ACTIVE_MODE_CHOICES = [
    discord.app_commands.Choice(name="Any message anywhere in the server", value="all"),
    discord.app_commands.Choice(name="Messages only in configured channels", value="channels"),
]

ACTIVE_THRESHOLD_CHOICES = [
    discord.app_commands.Choice(name="15 minutes", value=15),
    discord.app_commands.Choice(name="30 minutes", value=30),
    discord.app_commands.Choice(name="60 minutes", value=60),
    discord.app_commands.Choice(name="120 minutes", value=120),
    discord.app_commands.Choice(name="6 hours", value=360),
    discord.app_commands.Choice(name="12 hours", value=720),
    discord.app_commands.Choice(name="24 hours", value=1440),
]

############### GLOBAL STATE / STORAGE ###############
db_pool = None
active_cleanup_task = None

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

SCHEMA_ALTERS_SQL = """
ALTER TABLE guild_settings
  ADD COLUMN IF NOT EXISTS active_mode TEXT NOT NULL DEFAULT 'all';
"""

MEMBER_ACTIVITY_SQL = """
CREATE TABLE IF NOT EXISTS member_activity (
  guild_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  last_message_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (guild_id, user_id)
);
"""

ACTIVITY_CHANNELS_SQL = """
CREATE TABLE IF NOT EXISTS activity_channels (
  guild_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  PRIMARY KEY (guild_id, channel_id)
);
"""

def require_guild(interaction: discord.Interaction) -> int:
    if interaction.guild is None:
        raise RuntimeError("This command must be used in a server.")
    return interaction.guild.id

async def init_db():
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute("SET TIME ZONE 'UTC';")
        await conn.execute(GUILD_SETTINGS_SQL)
        await conn.execute(SCHEMA_ALTERS_SQL)
        await conn.execute(MEMBER_ACTIVITY_SQL)
        await conn.execute(ACTIVITY_CHANNELS_SQL)

async def close_db():
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None

async def ensure_guild_row(guild_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO guild_settings (guild_id) VALUES ($1) ON CONFLICT (guild_id) DO NOTHING;",
            guild_id,
        )

async def set_active_role(guild_id: int, role_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, active_role_id, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET active_role_id = EXCLUDED.active_role_id, updated_at = NOW();
            """,
            guild_id,
            role_id,
        )

async def set_active_threshold(guild_id: int, minutes: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, active_threshold_minutes, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET active_threshold_minutes = EXCLUDED.active_threshold_minutes, updated_at = NOW();
            """,
            guild_id,
            minutes,
        )

async def set_active_mode(guild_id: int, mode: str) -> None:
    mode = mode.lower().strip()
    if mode not in ("all", "channels"):
        raise RuntimeError("Mode must be 'all' or 'channels'")
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, active_mode, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET active_mode = EXCLUDED.active_mode, updated_at = NOW();
            """,
            guild_id,
            mode,
        )

async def add_activity_channel(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO activity_channels (guild_id, channel_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            guild_id,
            channel_id,
        )

async def remove_activity_channel(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM activity_channels WHERE guild_id = $1 AND channel_id = $2;",
            guild_id,
            channel_id,
        )

async def list_activity_channels(guild_id: int) -> list[int]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT channel_id FROM activity_channels WHERE guild_id = $1 ORDER BY channel_id ASC;",
            guild_id,
        )
    return [int(r["channel_id"]) for r in rows]

async def get_activity_config(guild_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT active_role_id, active_threshold_minutes, active_mode
            FROM guild_settings
            WHERE guild_id = $1;
            """,
            guild_id,
        )
    if not row:
        return {"active_role_id": None, "active_threshold_minutes": 60, "active_mode": "all"}
    return {
        "active_role_id": row["active_role_id"],
        "active_threshold_minutes": int(row["active_threshold_minutes"]),
        "active_mode": row["active_mode"] or "all",
    }

async def record_activity(guild_id: int, user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO member_activity (guild_id, user_id, last_message_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id, user_id)
            DO UPDATE SET last_message_at = NOW();
            """,
            guild_id,
            user_id,
        )

async def should_count_message(guild_id: int, channel_id: int) -> bool:
    cfg = await get_activity_config(guild_id)
    if cfg["active_mode"] == "all":
        return True
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM activity_channels WHERE guild_id = $1 AND channel_id = $2;",
            guild_id,
            channel_id,
        )
    return bool(exists)

async def maybe_apply_active_role(member: discord.Member) -> None:
    cfg = await get_activity_config(member.guild.id)
    role_id = cfg["active_role_id"]
    if not role_id:
        return
    role = member.guild.get_role(int(role_id))
    if role is None:
        return
    if role in member.roles:
        return
    try:
        await member.add_roles(role, reason="Activity Tracking: member became active")
    except Exception:
        return

async def active_cleanup_once():
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT guild_id, active_role_id, active_threshold_minutes
            FROM guild_settings
            WHERE active_role_id IS NOT NULL;
            """
        )
    for r in rows:
        guild_id = int(r["guild_id"])
        role_id = int(r["active_role_id"])
        threshold_minutes = int(r["active_threshold_minutes"])
        guild = bot.get_guild(guild_id)
        if guild is None:
            continue
        role = guild.get_role(role_id)
        if role is None:
            continue
        async with db_pool.acquire() as conn:
            stale = await conn.fetch(
                """
                SELECT user_id
                FROM member_activity
                WHERE guild_id = $1
                  AND last_message_at < (NOW() - ($2::int * INTERVAL '1 minute'));
                """,
                guild_id,
                threshold_minutes,
            )
        for s in stale:
            user_id = int(s["user_id"])
            member = guild.get_member(user_id)
            if member is None:
                continue
            if role not in member.roles:
                continue
            try:
                await member.remove_roles(role, reason="Activity Tracking: inactivity threshold exceeded")
            except Exception:
                continue

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

############### VIEWS / UI COMPONENTS ###############

############### AUTOCOMPLETE FUNCTIONS ###############
async def timezone_autocomplete(interaction: discord.Interaction, current: str):
    current_l = (current or "").lower()
    common = [
        "America/Los_Angeles",
        "America/Denver",
        "America/Chicago",
        "America/New_York",
        "America/Phoenix",
        "America/Anchorage",
        "Pacific/Honolulu",
        "Europe/London",
        "Europe/Paris",
        "Europe/Berlin",
        "Australia/Sydney",
        "Asia/Tokyo",
    ]
    matches = []
    for tz in common:
        if current_l in tz.lower():
            matches.append(discord.app_commands.Choice(name=tz, value=tz))
        if len(matches) >= 25:
            break
    if matches:
        return matches
    return [discord.app_commands.Choice(name="America/Los_Angeles", value="America/Los_Angeles")]

############### BACKGROUND TASKS & SCHEDULERS ###############
async def active_cleanup_loop():
    while True:
        try:
            await active_cleanup_once()
        except Exception:
            pass
        await asyncio.sleep(60)

############### EVENT HANDLERS ###############
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.guild is None:
        return
    guild_id = int(message.guild.id)
    channel_id = int(message.channel.id)
    ok = await should_count_message(guild_id, channel_id)
    if not ok:
        await bot.process_commands(message)
        return
    await ensure_guild_row(guild_id)
    await record_activity(guild_id, int(message.author.id))
    if isinstance(message.author, discord.Member):
        await maybe_apply_active_role(message.author)
    await bot.process_commands(message)

############### COMMAND GROUPS ###############
@bot.tree.command(name="ping", description="Bot heartbeat")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong ✅")

config_group = discord.app_commands.Group(name="config", description="Server configuration")

active_group = discord.app_commands.Group(name="active", description="Activity Tracking settings", parent=config_group)

@active_group.command(name="set_role", description="Set the Active Member role")
async def active_set_role(interaction: discord.Interaction, role: discord.Role):
    guild_id = require_guild(interaction)
    await ensure_guild_row(guild_id)
    await set_active_role(guild_id, int(role.id))
    await interaction.response.send_message(f"✅ Active role set to {role.mention}", ephemeral=True)

@active_group.command(name="clear_role", description="Clear the Active Member role")
async def active_clear_role(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    await ensure_guild_row(guild_id)
    await set_active_role(guild_id, None)
    await interaction.response.send_message("✅ Active role cleared", ephemeral=True)

@active_group.command(name="set_threshold", description="Set inactivity threshold")
@discord.app_commands.choices(minutes=ACTIVE_THRESHOLD_CHOICES)
async def active_set_threshold(interaction: discord.Interaction, minutes: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await ensure_guild_row(guild_id)
    await set_active_threshold(guild_id, minutes.value)
    await interaction.response.send_message(f"✅ Active threshold set to {minutes.value} minute(s)", ephemeral=True)

@active_group.command(name="set_mode", description="Set activity mode")
@discord.app_commands.choices(mode=ACTIVE_MODE_CHOICES)
async def active_set_mode(interaction: discord.Interaction, mode: discord.app_commands.Choice[str]):
    guild_id = require_guild(interaction)
    await ensure_guild_row(guild_id)
    await set_active_mode(guild_id, mode.value)
    await interaction.response.send_message(f"✅ Activity mode set to `{mode.value}`", ephemeral=True)

@active_group.command(name="add_channel", description="Add a channel to count activity (channels mode)")
async def active_add_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = require_guild(interaction)
    await ensure_guild_row(guild_id)
    await add_activity_channel(guild_id, int(channel.id))
    await interaction.response.send_message(f"✅ Added {channel.mention} to activity channels", ephemeral=True)

@active_group.command(name="remove_channel", description="Remove a channel from activity channels")
async def active_remove_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = require_guild(interaction)
    await remove_activity_channel(guild_id, int(channel.id))
    await interaction.response.send_message(f"✅ Removed {channel.mention} from activity channels", ephemeral=True)

@active_group.command(name="list_channels", description="List activity channels (channels mode)")
async def active_list_channels(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    ids = await list_activity_channels(guild_id)
    if not ids:
        await interaction.response.send_message("No activity channels set.", ephemeral=True)
        return
    mentions = []
    for cid in ids:
        ch = interaction.guild.get_channel(cid) if interaction.guild else None
        mentions.append(ch.mention if ch else f"`{cid}`")
    await interaction.response.send_message("Activity channels:\n" + "\n".join(mentions), ephemeral=True)

@active_group.command(name="show", description="Show current Activity Tracking settings")
async def active_show(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    cfg = await get_activity_config(guild_id)
    await interaction.response.send_message(
        f"Mode: `{cfg['active_mode']}`\nThreshold: `{cfg['active_threshold_minutes']}` minute(s)\nRole ID: `{cfg['active_role_id']}`",
        ephemeral=True,
    )

timezone_group = discord.app_commands.Group(name="timezone", description="Timezone settings", parent=config_group)

@timezone_group.command(name="set", description="Set this server's timezone")
@discord.app_commands.describe(tz="Example: America/Los_Angeles")
@discord.app_commands.autocomplete(tz=timezone_autocomplete)
async def tz_set(interaction: discord.Interaction, tz: str):
    guild_id = require_guild(interaction)
    await ensure_guild_row(guild_id)
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
    global active_cleanup_task
    await bot.tree.sync()
    if active_cleanup_task is None:
        active_cleanup_task = asyncio.create_task(active_cleanup_loop())
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
