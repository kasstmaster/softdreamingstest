############### IMPORTS ###############
import os
import asyncio
import uuid
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
import asyncpg
import discord
from discord.ext import commands
from urllib.parse import quote_plus

############### MESSAGE TEMPLATES ###############
MSG = {
    # Dead Chat
    "deadchat_awarded": "💀 {user} revived the chat and stole **Dead Chat**!",

    # Plague
    "plague_infected": "☣️ **Plague Day!** {user} has been infected for **{days} days**.",

    # Prize
    "prize_drop": "🎁 Prize Drop! First to claim it wins.",
    "prize_claimed_channel": "🏆 {user} claimed **{prize}**!",

    # Birthday
    "birthday_announce": "🎉 Happy Birthday {user}! 🎂",

    # Welcome
    "welcome": "Welcome to the server, {user}! 👋",

    # QOTD
    "qotd_header": "❓ **Question of the Day**",
}

############### CONSTANTS & CONFIG ###############
DATABASE_URL = os.getenv("DATABASE_URL")
TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN")

# Only allow certain actions in these guild(s)
DEV_GUILD_IDS = {
    int(gid.strip())
    for gid in os.getenv("DEV_GUILD_ID", "").split(",")
    if gid.strip().isdigit()
}

DEV_GUILD_IDS.discard(0)

# Legacy storage channel for one-time import/preview
LEGACY_STORAGE_CHANNEL_ID = int(os.getenv("LEGACY_STORAGE_CHANNEL_ID", "1440912334813134868"))


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

VOICE_MODE_CHOICES = [
    discord.app_commands.Choice(name="Add role on join (remove on leave)", value="add_on_join"),
    discord.app_commands.Choice(name="Remove role on join (add on leave)", value="remove_on_join"),
]

DEADCHAT_IDLE_CHOICES = [
    discord.app_commands.Choice(name="5 minutes", value=5),
    discord.app_commands.Choice(name="10 minutes", value=10),
    discord.app_commands.Choice(name="15 minutes", value=15),
    discord.app_commands.Choice(name="30 minutes", value=30),
    discord.app_commands.Choice(name="60 minutes", value=60),
    discord.app_commands.Choice(name="120 minutes", value=120),
]

DEADCHAT_COOLDOWN_CHOICES = [
    discord.app_commands.Choice(name="0 minutes", value=0),
    discord.app_commands.Choice(name="15 minutes", value=15),
    discord.app_commands.Choice(name="30 minutes", value=30),
    discord.app_commands.Choice(name="60 minutes", value=60),
    discord.app_commands.Choice(name="120 minutes", value=120),
    discord.app_commands.Choice(name="6 hours", value=360),
    discord.app_commands.Choice(name="12 hours", value=720),
    discord.app_commands.Choice(name="24 hours", value=1440),
]

PLAGUE_DURATION_CHOICES = [
    discord.app_commands.Choice(name="24 hours", value=24),
    discord.app_commands.Choice(name="48 hours", value=48),
    discord.app_commands.Choice(name="72 hours", value=72),
    discord.app_commands.Choice(name="96 hours", value=96),
    discord.app_commands.Choice(name="7 days", value=168),
]

BOOL_CHOICES = [
    discord.app_commands.Choice(name="Enabled", value=1),
    discord.app_commands.Choice(name="Disabled", value=0),
]

PRIZE_TIME_CHOICES = [
    discord.app_commands.Choice(name="Any time", value=""),
    discord.app_commands.Choice(name="08:00", value="08:00"),
    discord.app_commands.Choice(name="12:00", value="12:00"),
    discord.app_commands.Choice(name="15:00", value="15:00"),
    discord.app_commands.Choice(name="18:00", value="18:00"),
    discord.app_commands.Choice(name="20:00", value="20:00"),
    discord.app_commands.Choice(name="21:00", value="21:00"),
]

############### GLOBAL STATE / STORAGE ###############
db_pool = None
active_cleanup_task = None
plague_cleanup_task = None
deadchat_cleanup_task = None
birthday_task = None
qotd_task = None
deadchat_locks = {}

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
  ADD COLUMN IF NOT EXISTS active_mode TEXT NOT NULL DEFAULT 'all',
  ADD COLUMN IF NOT EXISTS birthday_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS birthday_role_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS birthday_channel_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS birthday_message_text TEXT NOT NULL DEFAULT '🎉 Happy Birthday {user}! 🎂',
  ADD COLUMN IF NOT EXISTS birthday_list_channel_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS birthday_list_message_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS qotd_channel_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS qotd_role_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS qotd_message_prefix TEXT NOT NULL DEFAULT '❓ **Question of the Day**',
  ADD COLUMN IF NOT EXISTS qotd_source_url TEXT NULL,
  ADD COLUMN IF NOT EXISTS qotd_last_posted_date DATE NULL,
  ADD COLUMN IF NOT EXISTS welcome_channel_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS welcome_message_text TEXT NOT NULL DEFAULT 'Welcome to the server, {user}! 👋',
  ADD COLUMN IF NOT EXISTS welcome_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS member_role_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS member_role_delay_seconds INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS bot_role_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS plague_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS plague_scheduled_day DATE NULL,
  ADD COLUMN IF NOT EXISTS prize_drop_channel_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS winner_announce_channel_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS logging_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS modlog_channel_id BIGINT NULL;
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

DEADCHAT_CHANNELS_SQL = """
CREATE TABLE IF NOT EXISTS deadchat_channels (
  guild_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  idle_minutes INT NULL,
  PRIMARY KEY (guild_id, channel_id)
);
"""

DEADCHAT_STATE_SQL = """
CREATE TABLE IF NOT EXISTS deadchat_state (
  guild_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  last_message_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  current_holder_user_id BIGINT NULL,
  last_award_at TIMESTAMPTZ NULL,
  last_award_message_id BIGINT NULL,
  PRIMARY KEY (guild_id, channel_id)
);
"""

DEADCHAT_COOLDOWNS_SQL = """
CREATE TABLE IF NOT EXISTS deadchat_user_cooldowns (
  guild_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  cooldown_until TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (guild_id, channel_id, user_id)
);
"""

PLAGUE_DAYS_SQL = """
CREATE TABLE IF NOT EXISTS plague_days (
  guild_id BIGINT NOT NULL,
  day DATE NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (guild_id, day)
);
"""

PLAGUE_DAILY_STATE_SQL = """
CREATE TABLE IF NOT EXISTS plague_daily_state (
  guild_id BIGINT NOT NULL,
  day DATE NOT NULL,
  infected_user_id BIGINT NULL,
  infected_at TIMESTAMPTZ NULL,
  PRIMARY KEY (guild_id, day)
);
"""

PLAGUE_INFECTIONS_SQL = """
CREATE TABLE IF NOT EXISTS plague_infections (
  guild_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  infected_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  source_channel_id BIGINT NULL,
  PRIMARY KEY (guild_id, user_id)
);
"""

PRIZE_DEFS_SQL = """
CREATE TABLE IF NOT EXISTS prize_definitions (
  guild_id BIGINT NOT NULL,
  prize_id UUID NOT NULL,
  title TEXT NOT NULL,
  description TEXT NULL,
  image_url TEXT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (guild_id, prize_id)
);
"""

PRIZE_SCHEDULES_SQL = """
CREATE TABLE IF NOT EXISTS prize_schedules (
  guild_id BIGINT NOT NULL,
  schedule_id UUID NOT NULL,
  day DATE NOT NULL,
  not_before_time TIME NULL,
  channel_id BIGINT NOT NULL,
  prize_id UUID NOT NULL,
  used BOOLEAN NOT NULL DEFAULT FALSE,
  used_at TIMESTAMPTZ NULL,
  PRIMARY KEY (guild_id, schedule_id)
);
"""

PRIZE_DROPS_SQL = """
CREATE TABLE IF NOT EXISTS prize_drops (
  guild_id BIGINT NOT NULL,
  drop_id UUID NOT NULL,
  schedule_id UUID NOT NULL,
  dropped_at TIMESTAMPTZ NOT NULL,
  message_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  claimed_by_user_id BIGINT NULL,
  claimed_at TIMESTAMPTZ NULL,
  PRIMARY KEY (guild_id, drop_id)
);
"""

BIRTHDAYS_SQL = """
CREATE TABLE IF NOT EXISTS birthdays (
  guild_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  month INT NOT NULL CHECK (month >= 1 AND month <= 12),
  day INT NOT NULL CHECK (day >= 1 AND day <= 31),
  year INT NULL,
  set_by_user_id BIGINT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (guild_id, user_id)
);
"""

BIRTHDAY_ANNOUNCE_LOG_SQL = """
CREATE TABLE IF NOT EXISTS birthday_announce_log (
  guild_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  day DATE NOT NULL,
  announced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (guild_id, user_id, day)
);
"""

STICKY_MESSAGES_SQL = """
CREATE TABLE IF NOT EXISTS sticky_messages (
  guild_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  content TEXT NOT NULL,
  message_id BIGINT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (guild_id, channel_id)
);
"""

AUTODELETE_CHANNELS_SQL = """
CREATE TABLE IF NOT EXISTS autodelete_channels (
  guild_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  delete_after_seconds INT NOT NULL DEFAULT 3600,
  log_channel_id BIGINT NULL,
  PRIMARY KEY (guild_id, channel_id)
);
"""

AUTODELETE_IGNORE_PHRASES_SQL = """
CREATE TABLE IF NOT EXISTS autodelete_ignore_phrases (
  guild_id BIGINT NOT NULL,
  phrase TEXT NOT NULL,
  PRIMARY KEY (guild_id, phrase)
);
"""

VOICE_ROLE_LINKS_SQL = """
CREATE TABLE IF NOT EXISTS voice_role_links (
  guild_id BIGINT NOT NULL,
  voice_channel_id BIGINT NOT NULL,
  role_id BIGINT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'add_on_join', -- add_on_join or remove_on_join
  PRIMARY KEY (guild_id, voice_channel_id)
);
"""

QOTD_HISTORY_SQL = """
CREATE TABLE IF NOT EXISTS qotd_history (
  guild_id BIGINT NOT NULL,
  question_hash TEXT NOT NULL,
  posted_on DATE NOT NULL,
  question_text TEXT NOT NULL,
  PRIMARY KEY (guild_id, posted_on)
);
"""

REQUIRED_TABLES = [
    "guild_settings",
    "member_activity",
    "activity_channels",
    "deadchat_channels",
    "deadchat_state",
    "deadchat_user_cooldowns",
    "plague_days",
    "plague_daily_state",
    "plague_infections",
    "prize_definitions",
    "prize_schedules",
    "prize_drops",
]

async def table_exists(name: str) -> bool:
    async with db_pool.acquire() as conn:
        v = await conn.fetchval(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1;",
            name,
        )
    return bool(v)

async def ensure_can_write_guild_settings(guild_id: int) -> bool:
    try:
        await ensure_guild_row(guild_id)
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE guild_settings SET updated_at = NOW() WHERE guild_id = $1;",
                guild_id,
            )
        return True
    except Exception:
        return False

def fmt(ok: bool, label: str, detail: str = "") -> str:
    return f"{'✅' if ok else '❌'} {label}{(' — ' + detail) if detail else ''}"

async def run_test_all(interaction: discord.Interaction) -> tuple[str, list[str]]:
    if interaction.guild is None:
        return ("❌ Must be used in a server.", [])
    guild = interaction.guild
    channel = interaction.channel
    guild_id = int(guild.id)

    lines = []

    ok_token = bool(TOKEN)
    lines.append(fmt(ok_token, "Token present"))

    ok_db = db_pool is not None
    lines.append(fmt(ok_db, "DB pool initialized"))

    if ok_db:
        try:
            async with db_pool.acquire() as conn:
                v = await conn.fetchval("SELECT 1;")
            lines.append(fmt(v == 1, "DB query", f"SELECT 1 -> {v}"))
        except Exception as e:
            lines.append(fmt(False, "DB query", str(e)))

        table_results = []
        for t in REQUIRED_TABLES:
            try:
                ex = await table_exists(t)
                table_results.append((t, ex))
            except Exception:
                table_results.append((t, False))
        missing = [t for (t, ex) in table_results if not ex]
        lines.append(fmt(len(missing) == 0, "Tables present", "" if not missing else f"missing: {', '.join(missing)}"))

        ok_write = await ensure_can_write_guild_settings(guild_id)
        lines.append(fmt(ok_write, "Guild settings writable"))

        try:
            s = await get_guild_settings(guild_id)
            lines.append(fmt(True, "Guild settings readable", f"tz={s['timezone']} active_mode={s['active_mode']}"))
            active_role_ok = True
            deadchat_role_ok = True
            plague_role_ok = True

            if s["active_role_id"]:
                active_role_ok = guild.get_role(int(s["active_role_id"])) is not None
            if s["deadchat_role_id"]:
                deadchat_role_ok = guild.get_role(int(s["deadchat_role_id"])) is not None
            if s["plague_role_id"]:
                plague_role_ok = guild.get_role(int(s["plague_role_id"])) is not None

            lines.append(fmt(active_role_ok, "Active role exists" if s["active_role_id"] else "Active role not set"))
            lines.append(fmt(deadchat_role_ok, "Dead Chat role exists" if s["deadchat_role_id"] else "Dead Chat role not set"))
            lines.append(fmt(plague_role_ok, "Plague role exists" if s["plague_role_id"] else "Plague role not set"))
        except Exception as e:
            lines.append(fmt(False, "Guild settings readable", str(e)))

        try:
            dc = await list_deadchat_channels(guild_id)
            lines.append(fmt(True, "Dead Chat channels readable", f"count={len(dc)}"))
        except Exception as e:
            lines.append(fmt(False, "Dead Chat channels readable", str(e)))

        try:
            s = await get_guild_settings(guild_id)
            local_now = guild_now(s["timezone"])
            days = await plague_list_days(guild_id)
            lines.append(fmt(True, "Plague days readable", f"count={len(days)} today={local_now.date().isoformat()}"))
        except Exception as e:
            lines.append(fmt(False, "Plague days readable", str(e)))

        try:
            defs = await prize_list_definitions(guild_id, limit=5)
            lines.append(fmt(True, "Prize definitions readable", f"sample_count={len(defs)}"))
        except Exception as e:
            lines.append(fmt(False, "Prize definitions readable", str(e)))

        try:
            s = await get_guild_settings(guild_id)
            local_now = guild_now(s["timezone"])
            scheds = await prize_schedule_list_upcoming(guild_id, local_now.date(), limit=5)
            lines.append(fmt(True, "Prize schedules readable", f"sample_count={len(scheds)}"))
        except Exception as e:
            lines.append(fmt(False, "Prize schedules readable", str(e)))

    perms = None
    if isinstance(channel, discord.abc.GuildChannel):
        perms = channel.permissions_for(guild.me) if guild.me else None
    if perms:
        lines.append(fmt(perms.send_messages, "Permission: send_messages"))
        lines.append(fmt(perms.manage_roles, "Permission: manage_roles"))
        lines.append(fmt(perms.read_message_history, "Permission: read_message_history"))
    else:
        lines.append(fmt(False, "Permission check", "could not evaluate"))

    cmd_count = len(bot.tree.get_commands())
    lines.append(fmt(cmd_count > 0, "Slash commands registered", f"count={cmd_count}"))

    summary_ok = all(l.startswith("✅") for l in lines if "missing:" in l or l.startswith("✅") or l.startswith("❌"))
    title = "✅ TEST ALL PASSED" if summary_ok else "⚠️ TEST ALL FOUND ISSUES"
    return (title, lines)

def require_guild(interaction: discord.Interaction) -> int:
    if interaction.guild is None:
        raise RuntimeError("This command must be used in a server.")
    return interaction.guild.id


async def require_dev_guild(interaction: discord.Interaction) -> bool:
    """Return True only if the interaction happened in an allowed developer guild."""
    if interaction.guild is None:
        try:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        except Exception:
            try:
                await interaction.followup.send("❌ Must be used in a server.", ephemeral=True)
            except Exception:
                pass
        return False

    if int(interaction.guild.id) not in DEV_GUILD_IDS:
        try:
            await interaction.response.send_message(
                "❌ This option is only available in the developer server.",
                ephemeral=True,
            )
        except Exception:
            try:
                await interaction.followup.send(
                    "❌ This option is only available in the developer server.",
                    ephemeral=True,
                )
            except Exception:
                pass
        return False

    return True


async def legacy_preview(interaction: discord.Interaction) -> dict:
    """Read legacy *_DATA messages from the legacy storage channel and return parsed objects."""
    result: dict = {"raw": {}, "errors": []}

    ch = bot.get_channel(LEGACY_STORAGE_CHANNEL_ID)
    if ch is None:
        try:
            ch = await bot.fetch_channel(LEGACY_STORAGE_CHANNEL_ID)
        except Exception as e:
            result["errors"].append(f"Could not fetch storage channel: {e!r}")
            return result

    try:
        history = [m async for m in ch.history(limit=200, oldest_first=False)]
    except Exception as e:
        result["errors"].append(f"Could not read channel history: {e!r}")
        return result

    prefixes = [
        "POOL_DATA",
        "STICKY_DATA",
        "MEMBERJOIN_DATA",
        "PLAGUE_DATA",
        "DEADCHAT_DATA",
        "DEADCHAT_STATE",
        "PRIZE_MOVIE_DATA",
        "PRIZE_NITRO_DATA",
        "PRIZE_STEAM_DATA",
        "TWITCH_STATE",
        "ACTIVITY_DATA",
        "CONFIG_DATA",
    ]

    import json as _json

    def _try_parse_json(blob: str):
        try:
            return _json.loads(blob)
        except Exception as e:
            return ("__error__", str(e))

    for m in history:
        content = (m.content or "").strip()
        if not content:
            continue

        if content.startswith("{") and content.endswith("}"):
            obj = _try_parse_json(content)
            if not (isinstance(obj, tuple) and obj and obj[0] == "__error__"):
                try:
                    if isinstance(obj, dict) and any(isinstance(v, dict) and "birthdays" in v for v in obj.values()):
                        result["raw"]["BIRTHDAYS_JSON"] = obj
                        continue
                except Exception:
                    pass

        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            for pfx in prefixes:
                if line.startswith(pfx + ":"):
                    blob = line.split(":", 1)[1].strip()
                    obj = _try_parse_json(blob)
                    if isinstance(obj, tuple) and obj and obj[0] == "__error__":
                        result["errors"].append(f"{pfx}: JSON parse error: {obj[1]}")
                    else:
                        result["raw"][pfx] = obj

    return result

def parse_date_yyyy_mm_dd(s: str) -> date:
    parts = (s or "").strip().split("-")
    if len(parts) != 3:
        raise RuntimeError("Date must be YYYY-MM-DD")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    return date(y, m, d)

def parse_hh_mm(s: str) -> time | None:
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    hh, mm = s.split(":")
    return time(int(hh), int(mm), 0)

def now_utc() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC"))

def guild_now(guild_tz: str) -> datetime:
    try:
        tz = ZoneInfo(guild_tz)
    except Exception:
        tz = ZoneInfo("America/Los_Angeles")
    return datetime.now(tz=tz)

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
        await conn.execute(DEADCHAT_CHANNELS_SQL)
        await conn.execute(DEADCHAT_STATE_SQL)
        await conn.execute(DEADCHAT_COOLDOWNS_SQL)
        await conn.execute(PLAGUE_DAYS_SQL)
        await conn.execute(PLAGUE_DAILY_STATE_SQL)
        await conn.execute(PLAGUE_INFECTIONS_SQL)
        await conn.execute(PRIZE_DEFS_SQL)
        await conn.execute(PRIZE_SCHEDULES_SQL)
        await conn.execute(PRIZE_DROPS_SQL)
        await conn.execute(BIRTHDAYS_SQL)
        await conn.execute(BIRTHDAY_ANNOUNCE_LOG_SQL)
        await conn.execute(STICKY_MESSAGES_SQL)
        await conn.execute(AUTODELETE_CHANNELS_SQL)
        await conn.execute(AUTODELETE_IGNORE_PHRASES_SQL)
        await conn.execute(VOICE_ROLE_LINKS_SQL)
        await conn.execute(QOTD_HISTORY_SQL)

async def close_db():
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None

def get_deadchat_lock(guild_id: int, channel_id: int) -> asyncio.Lock:
    key = (guild_id, channel_id)
    lock = deadchat_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        deadchat_locks[key] = lock
    return lock

async def ensure_guild_row(guild_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO guild_settings (guild_id) VALUES ($1) ON CONFLICT (guild_id) DO NOTHING;", guild_id)

async def get_guild_settings(guild_id: int) -> dict:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT active_role_id, active_threshold_minutes, active_mode,
                   deadchat_role_id, deadchat_idle_minutes, deadchat_requires_active, deadchat_cooldown_minutes,
                   plague_role_id, plague_duration_hours, plague_enabled, plague_scheduled_day,
                   prizes_enabled,
                   timezone
            FROM guild_settings
            WHERE guild_id = $1;
            """,
            guild_id,
        )
    return {
        "active_role_id": row["active_role_id"],
        "active_threshold_minutes": int(row["active_threshold_minutes"]),
        "active_mode": row["active_mode"] or "all",
        "deadchat_role_id": row["deadchat_role_id"],
        "deadchat_idle_minutes": int(row["deadchat_idle_minutes"]),
        "deadchat_requires_active": bool(row["deadchat_requires_active"]),
        "deadchat_cooldown_minutes": int(row["deadchat_cooldown_minutes"]),
        "plague_role_id": row["plague_role_id"],
        "plague_duration_hours": int(row["plague_duration_hours"]),
        "prizes_enabled": bool(row["prizes_enabled"]),
        "timezone": row["timezone"] or "America/Los_Angeles",
    }

async def upsert_timezone(guild_id: int, timezone: str) -> None:
    await ensure_guild_row(guild_id)
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

async def set_active_role(guild_id: int, role_id: int | None) -> None:
    await ensure_guild_row(guild_id)
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
    await ensure_guild_row(guild_id)
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
    await ensure_guild_row(guild_id)
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
        await conn.execute("INSERT INTO activity_channels (guild_id, channel_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;", guild_id, channel_id)

async def remove_activity_channel(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM activity_channels WHERE guild_id = $1 AND channel_id = $2;", guild_id, channel_id)

async def list_activity_channels(guild_id: int) -> list[int]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT channel_id FROM activity_channels WHERE guild_id = $1 ORDER BY channel_id ASC;", guild_id)
    return [int(r["channel_id"]) for r in rows]

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

async def should_count_activity_message(guild_id: int, channel_id: int) -> bool:
    settings = await get_guild_settings(guild_id)
    if settings["active_mode"] == "all":
        return True
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM activity_channels WHERE guild_id = $1 AND channel_id = $2;", guild_id, channel_id)
    return bool(exists)

async def maybe_apply_active_role(member: discord.Member) -> None:
    settings = await get_guild_settings(member.guild.id)
    role_id = settings["active_role_id"]
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

async def active_cleanup_once(bot: commands.Bot):
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

async def set_deadchat_role(guild_id: int, role_id: int | None) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, deadchat_role_id, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET deadchat_role_id = EXCLUDED.deadchat_role_id, updated_at = NOW();
            """,
            guild_id,
            role_id,
        )

async def set_deadchat_idle(guild_id: int, minutes: int) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, deadchat_idle_minutes, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET deadchat_idle_minutes = EXCLUDED.deadchat_idle_minutes, updated_at = NOW();
            """,
            guild_id,
            minutes,
        )

async def set_deadchat_cooldown(guild_id: int, minutes: int) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, deadchat_cooldown_minutes, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET deadchat_cooldown_minutes = EXCLUDED.deadchat_cooldown_minutes, updated_at = NOW();
            """,
            guild_id,
            minutes,
        )

async def set_deadchat_requires_active(guild_id: int, enabled: bool) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, deadchat_requires_active, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET deadchat_requires_active = EXCLUDED.deadchat_requires_active, updated_at = NOW();
            """,
            guild_id,
            enabled,
        )

async def add_deadchat_channel(guild_id: int, channel_id: int, idle_minutes_override: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO deadchat_channels (guild_id, channel_id, enabled, idle_minutes)
            VALUES ($1, $2, TRUE, $3)
            ON CONFLICT (guild_id, channel_id)
            DO UPDATE SET enabled = TRUE, idle_minutes = EXCLUDED.idle_minutes;
            """,
            guild_id,
            channel_id,
            idle_minutes_override,
        )
        await conn.execute(
            """
            INSERT INTO deadchat_state (guild_id, channel_id, last_message_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id, channel_id) DO NOTHING;
            """,
            guild_id,
            channel_id,
        )

async def remove_deadchat_channel(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM deadchat_channels WHERE guild_id = $1 AND channel_id = $2;", guild_id, channel_id)

async def list_deadchat_channels(guild_id: int) -> list[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT channel_id, enabled, idle_minutes
            FROM deadchat_channels
            WHERE guild_id = $1
            ORDER BY channel_id ASC;
            """,
            guild_id,
        )
    return [{"channel_id": int(r["channel_id"]), "enabled": bool(r["enabled"]), "idle_minutes": r["idle_minutes"]} for r in rows]

async def get_deadchat_channel_config(guild_id: int, channel_id: int) -> dict | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT enabled, idle_minutes
            FROM deadchat_channels
            WHERE guild_id = $1 AND channel_id = $2;
            """,
            guild_id,
            channel_id,
        )
    if not row:
        return None
    return {"enabled": bool(row["enabled"]), "idle_minutes": row["idle_minutes"]}

async def deadchat_update_last_message(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO deadchat_state (guild_id, channel_id, last_message_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id, channel_id)
            DO UPDATE SET last_message_at = NOW();
            """,
            guild_id,
            channel_id,
        )

async def deadchat_get_state(guild_id: int, channel_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT last_message_at, current_holder_user_id, last_award_at, last_award_message_id
            FROM deadchat_state
            WHERE guild_id = $1 AND channel_id = $2;
            """,
            guild_id,
            channel_id,
        )
    if not row:
        return {"last_message_at": now_utc(), "current_holder_user_id": None, "last_award_at": None, "last_award_message_id": None}
    return {
        "last_message_at": row["last_message_at"],
        "current_holder_user_id": row["current_holder_user_id"],
        "last_award_at": row["last_award_at"],
        "last_award_message_id": row["last_award_message_id"],
    }

async def deadchat_get_user_cooldown_until(guild_id: int, channel_id: int, user_id: int) -> datetime | None:
    async with db_pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT cooldown_until
            FROM deadchat_user_cooldowns
            WHERE guild_id = $1 AND channel_id = $2 AND user_id = $3;
            """,
            guild_id,
            channel_id,
            user_id,
        )
    return value

async def deadchat_set_user_cooldown(guild_id: int, channel_id: int, user_id: int, until: datetime) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO deadchat_user_cooldowns (guild_id, channel_id, user_id, cooldown_until)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (guild_id, channel_id, user_id)
            DO UPDATE SET cooldown_until = EXCLUDED.cooldown_until;
            """,
            guild_id,
            channel_id,
            user_id,
            until,
        )

async def deadchat_set_holder(guild_id: int, channel_id: int, user_id: int | None, award_message_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO deadchat_state (guild_id, channel_id, current_holder_user_id, last_award_at, last_award_message_id, last_message_at)
            VALUES ($1, $2, $3, NOW(), $4, NOW())
            ON CONFLICT (guild_id, channel_id)
            DO UPDATE SET current_holder_user_id = EXCLUDED.current_holder_user_id,
                          last_award_at = NOW(),
                          last_award_message_id = EXCLUDED.last_award_message_id,
                          last_message_at = NOW();
            """,
            guild_id,
            channel_id,
            user_id,
            award_message_id,
        )

async def plague_set_role(guild_id: int, role_id: int | None) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, plague_role_id, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET plague_role_id = EXCLUDED.plague_role_id, updated_at = NOW();
            """,
            guild_id,
            role_id,
        )

async def plague_set_duration(guild_id: int, hours: int) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, plague_duration_hours, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET plague_duration_hours = EXCLUDED.plague_duration_hours, updated_at = NOW();
            """,
            guild_id,
            hours,
        )


async def plague_set_enabled(guild_id: int, enabled: bool) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, plague_enabled, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET plague_enabled = EXCLUDED.plague_enabled, updated_at = NOW();
            """,
            guild_id,
            enabled,
        )

async def plague_set_scheduled_day(guild_id: int, day: date | None) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, plague_scheduled_day, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET plague_scheduled_day = EXCLUDED.plague_scheduled_day, updated_at = NOW();
            """,
            guild_id,
            day,
        )

async def plague_add_day(guild_id: int, day: date) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO plague_days (guild_id, day, enabled)
            VALUES ($1, $2, TRUE)
            ON CONFLICT (guild_id, day) DO UPDATE SET enabled = TRUE;
            """,
            guild_id,
            day,
        )

async def plague_remove_day(guild_id: int, day: date) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM plague_days WHERE guild_id = $1 AND day = $2;", guild_id, day)

async def plague_list_days(guild_id: int) -> list[date]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT day FROM plague_days WHERE guild_id = $1 AND enabled = TRUE ORDER BY day ASC;", guild_id)
    return [r["day"] for r in rows]

async def plague_is_day(guild_id: int, day: date) -> bool:
    async with db_pool.acquire() as conn:
        value = await conn.fetchval("SELECT 1 FROM plague_days WHERE guild_id = $1 AND day = $2 AND enabled = TRUE;", guild_id, day)
    return bool(value)

async def plague_daily_already_triggered(guild_id: int, day: date) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT infected_user_id FROM plague_daily_state WHERE guild_id = $1 AND day = $2;", guild_id, day)
    return bool(row and row["infected_user_id"])

async def plague_mark_triggered(guild_id: int, day: date, user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO plague_daily_state (guild_id, day, infected_user_id, infected_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (guild_id, day)
            DO UPDATE SET infected_user_id = EXCLUDED.infected_user_id, infected_at = NOW();
            """,
            guild_id,
            day,
            user_id,
        )

async def plague_add_infection(guild_id: int, user_id: int, expires_at: datetime, source_channel_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO plague_infections (guild_id, user_id, infected_at, expires_at, source_channel_id)
            VALUES ($1, $2, NOW(), $3, $4)
            ON CONFLICT (guild_id, user_id)
            DO UPDATE SET infected_at = NOW(), expires_at = EXCLUDED.expires_at, source_channel_id = EXCLUDED.source_channel_id;
            """,
            guild_id,
            user_id,
            expires_at,
            source_channel_id,
        )

async def plague_get_expired() -> list[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT guild_id, user_id FROM plague_infections WHERE expires_at <= NOW();")
    return [{"guild_id": int(r["guild_id"]), "user_id": int(r["user_id"])} for r in rows]

async def plague_delete_infection(guild_id: int, user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM plague_infections WHERE guild_id = $1 AND user_id = $2;", guild_id, user_id)

async def prize_set_enabled(guild_id: int, enabled: bool) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_settings (guild_id, prizes_enabled, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (guild_id)
            DO UPDATE SET prizes_enabled = EXCLUDED.prizes_enabled, updated_at = NOW();
            """,
            guild_id,
            enabled,
        )

async def prize_set_drop_channel(guild_id: int, channel_id: int | None) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE guild_settings SET prize_drop_channel_id=$2, updated_at=NOW() WHERE guild_id=$1;",
            guild_id,
            channel_id,
        )

async def prize_set_winner_announce_channel(guild_id: int, channel_id: int | None) -> None:
    await ensure_guild_row(guild_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE guild_settings SET winner_announce_channel_id=$2, updated_at=NOW() WHERE guild_id=$1;",
            guild_id,
            channel_id,
        )

async def prize_add_definition(guild_id: int, title: str, description: str | None, image_url: str | None) -> uuid.UUID:
    pid = uuid.uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prize_definitions (guild_id, prize_id, title, description, image_url, enabled)
            VALUES ($1, $2, $3, $4, $5, TRUE);
            """,
            guild_id,
            pid,
            title,
            description,
            image_url,
        )
    return pid

async def prize_delete_definition(guild_id: int, prize_id: uuid.UUID) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM prize_definitions WHERE guild_id = $1 AND prize_id = $2;", guild_id, prize_id)

async def prize_list_definitions(guild_id: int, limit: int = 25) -> list[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT prize_id, title, enabled
            FROM prize_definitions
            WHERE guild_id = $1
            ORDER BY title ASC
            LIMIT $2;
            """,
            guild_id,
            limit,
        )
    return [{"prize_id": r["prize_id"], "title": r["title"], "enabled": bool(r["enabled"])} for r in rows]

async def prize_find_definitions(guild_id: int, query: str, limit: int = 25) -> list[dict]:
    q = (query or "").strip()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT prize_id, title, enabled
            FROM prize_definitions
            WHERE guild_id = $1 AND title ILIKE $2
            ORDER BY title ASC
            LIMIT $3;
            """,
            guild_id,
            f"%{q}%",
            limit,
        )
    return [{"prize_id": r["prize_id"], "title": r["title"], "enabled": bool(r["enabled"])} for r in rows]

async def prize_get_definition(guild_id: int, prize_id: uuid.UUID) -> dict | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT prize_id, title, description, image_url, enabled
            FROM prize_definitions
            WHERE guild_id = $1 AND prize_id = $2;
            """,
            guild_id,
            prize_id,
        )
    if not row:
        return None
    return {"prize_id": row["prize_id"], "title": row["title"], "description": row["description"], "image_url": row["image_url"], "enabled": bool(row["enabled"])}

async def prize_schedule_add(guild_id: int, day: date, not_before: time | None, channel_id: int, prize_id: uuid.UUID) -> uuid.UUID:
    sid = uuid.uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prize_schedules (guild_id, schedule_id, day, not_before_time, channel_id, prize_id, used)
            VALUES ($1, $2, $3, $4, $5, $6, FALSE);
            """,
            guild_id,
            sid,
            day,
            not_before,
            channel_id,
            prize_id,
        )
    return sid

async def prize_schedule_list_upcoming(guild_id: int, from_day: date, limit: int = 25) -> list[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT schedule_id, day, not_before_time, channel_id, prize_id, used
            FROM prize_schedules
            WHERE guild_id = $1 AND day >= $2
            ORDER BY day ASC, not_before_time NULLS FIRST
            LIMIT $3;
            """,
            guild_id,
            from_day,
            limit,
        )
    return [
        {
            "schedule_id": r["schedule_id"],
            "day": r["day"],
            "not_before_time": r["not_before_time"],
            "channel_id": int(r["channel_id"]),
            "prize_id": r["prize_id"],
            "used": bool(r["used"]),
        }
        for r in rows
    ]

async def prize_schedule_remove(guild_id: int, schedule_id: uuid.UUID) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM prize_schedules WHERE guild_id = $1 AND schedule_id = $2;", guild_id, schedule_id)

async def prize_find_available_schedule_for_today(guild_id: int, day: date, local_time: time) -> dict | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT schedule_id, not_before_time, channel_id, prize_id
            FROM prize_schedules
            WHERE guild_id = $1 AND day = $2 AND used = FALSE
            ORDER BY not_before_time NULLS FIRST, schedule_id ASC
            LIMIT 1;
            """,
            guild_id,
            day,
        )
    if not row:
        return None
    nbt = row["not_before_time"]
    if nbt is not None and local_time < nbt:
        return None
    return {"schedule_id": row["schedule_id"], "not_before_time": nbt, "channel_id": int(row["channel_id"]), "prize_id": row["prize_id"]}

async def prize_mark_used(guild_id: int, schedule_id: uuid.UUID) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE prize_schedules SET used = TRUE, used_at = NOW() WHERE guild_id = $1 AND schedule_id = $2;",
            guild_id,
            schedule_id,
        )

async def prize_create_drop(guild_id: int, schedule_id: uuid.UUID, channel_id: int, message_id: int) -> uuid.UUID:
    drop_id = uuid.uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prize_drops (guild_id, drop_id, schedule_id, dropped_at, message_id, channel_id)
            VALUES ($1, $2, $3, NOW(), $4, $5);
            """,
            guild_id,
            drop_id,
            schedule_id,
            message_id,
            channel_id,
        )
    return drop_id

async def prize_claim_drop_atomic(guild_id: int, drop_id: uuid.UUID, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        res = await conn.execute(
            """
            UPDATE prize_drops
            SET claimed_by_user_id = $3, claimed_at = NOW()
            WHERE guild_id = $1 AND drop_id = $2 AND claimed_by_user_id IS NULL;
            """,
            guild_id,
            drop_id,
            user_id,
        )
    return res.endswith("UPDATE 1")

async def prize_get_drop(guild_id: int, drop_id: uuid.UUID) -> dict | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT drop_id, schedule_id, message_id, channel_id, claimed_by_user_id, claimed_at
            FROM prize_drops
            WHERE guild_id = $1 AND drop_id = $2;
            """,
            guild_id,
            drop_id,
        )
    if not row:
        return None
    return {
        "drop_id": row["drop_id"],
        "schedule_id": row["schedule_id"],
        "message_id": int(row["message_id"]),
        "channel_id": int(row["channel_id"]),
        "claimed_by_user_id": row["claimed_by_user_id"],
        "claimed_at": row["claimed_at"],
    }

async def deadchat_is_configured(guild_id: int, channel_id: int) -> bool:
    async with db_pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT 1 FROM deadchat_channels WHERE guild_id = $1 AND channel_id = $2 AND enabled = TRUE;",
            guild_id,
            channel_id,
        )
    return bool(value)

async def deadchat_attempt_award(bot: commands.Bot, message: discord.Message) -> None:
    if message.guild is None:
        return
    if not isinstance(message.author, discord.Member):
        return
    guild_id = int(message.guild.id)
    channel_id = int(message.channel.id)
    cfg = await get_deadchat_channel_config(guild_id, channel_id)
    if not cfg or not cfg["enabled"]:
        await deadchat_update_last_message(guild_id, channel_id)
        return
    settings = await get_guild_settings(guild_id)
    role_id = settings["deadchat_role_id"]
    if not role_id:
        await deadchat_update_last_message(guild_id, channel_id)
        return
    state = await deadchat_get_state(guild_id, channel_id)
    idle_minutes = int(cfg["idle_minutes"]) if cfg["idle_minutes"] is not None else int(settings["deadchat_idle_minutes"])
    last_msg_at = state["last_message_at"]
    now = now_utc()
    is_dead = (now - last_msg_at) >= timedelta(minutes=idle_minutes)
    await deadchat_update_last_message(guild_id, channel_id)
    if not is_dead:
        return
    if settings["deadchat_requires_active"]:
        active_role_id = settings["active_role_id"]
        if active_role_id:
            active_role = message.guild.get_role(int(active_role_id))
            if active_role and active_role not in message.author.roles:
                return
    cooldown_until = await deadchat_get_user_cooldown_until(guild_id, channel_id, int(message.author.id))
    if cooldown_until and cooldown_until > now:
        return
    lock = get_deadchat_lock(guild_id, channel_id)
    async with lock:
        state = await deadchat_get_state(guild_id, channel_id)
        last_msg_at = state["last_message_at"]
        now = now_utc()
        is_dead = (now - last_msg_at) >= timedelta(minutes=idle_minutes)
        await deadchat_update_last_message(guild_id, channel_id)
        if not is_dead:
            return
        cooldown_until = await deadchat_get_user_cooldown_until(guild_id, channel_id, int(message.author.id))
        if cooldown_until and cooldown_until > now:
            return
        deadchat_role = message.guild.get_role(int(role_id))
        if deadchat_role is None:
            return
        prev_holder_id = state["current_holder_user_id"]
        if prev_holder_id:
            prev_member = message.guild.get_member(int(prev_holder_id))
            if prev_member and deadchat_role in prev_member.roles:
                try:
                    await prev_member.remove_roles(deadchat_role, reason="Dead Chat: transferred to new holder")
                except Exception:
                    pass
        if deadchat_role not in message.author.roles:
            try:
                await message.author.add_roles(deadchat_role, reason="Dead Chat: awarded to channel reviver")
            except Exception:
                return
        old_msg_id = state["last_award_message_id"]
        if old_msg_id:
            try:
                old_msg = await message.channel.fetch_message(int(old_msg_id))
                await old_msg.delete()
            except Exception:
                pass
        text = MSG["deadchat_awarded"]
        text = format_template(text, message.author)
        sent = await message.channel.send(text)
        try:
            sent = await message.channel.send(announce_text)
        except Exception:
            sent = None
        await deadchat_set_holder(guild_id, channel_id, int(message.author.id), int(sent.id) if sent else None)
        cd_minutes = int(settings["deadchat_cooldown_minutes"])
        if cd_minutes > 0:
            await deadchat_set_user_cooldown(guild_id, channel_id, int(message.author.id), now + timedelta(minutes=cd_minutes))
        await maybe_trigger_plague(message.guild, int(message.author.id), channel_id)
        await maybe_trigger_prize_drop(message.guild, int(message.author.id))


async def maybe_trigger_plague(guild: discord.Guild, winner_user_id: int, source_channel_id: int) -> None:
    settings = await get_guild_settings(int(guild.id))

    if not settings.get("plague_enabled"):
        return
    scheduled_day = settings.get("plague_scheduled_day")
    if not scheduled_day:
        return

    utc_now = now_utc()
    if utc_now.date() != scheduled_day:
        return
    if utc_now.hour < 12:
        return

    if await plague_daily_already_triggered(int(guild.id), scheduled_day):
        return

    role_id = settings.get("plague_role_id")
    if not role_id:
        return

    member = guild.get_member(winner_user_id)
    if member is None:
        return
    role = guild.get_role(int(role_id))
    if role is None:
        return

    expires_at = utc_now + timedelta(days=3)

    try:
        await member.add_roles(role, reason="Plague Day: first Dead Chat winner after 12:00 UTC")
    except Exception:
        return

    await plague_add_infection(int(guild.id), winner_user_id, expires_at, source_channel_id)
    await plague_mark_triggered(int(guild.id), scheduled_day, winner_user_id)

    channel = guild.get_channel(int(source_channel_id))
    if channel:
        try:
            text = MSG["plague_infected"].replace("{days}", "3")
            text = format_template(text, member)
            await channel.send(text)
        except Exception:
            pass

async def maybe_trigger_prize_drop(guild: discord.Guild, winner_user_id: int) -> None:
    settings = await get_guild_settings(int(guild.id))
    if not settings["prizes_enabled"]:
        return
    local_now = guild_now(settings["timezone"])
    today = local_now.date()
    sched = await prize_find_available_schedule_for_today(int(guild.id), today, local_now.time())
    if not sched:
        return
    prize = await prize_get_definition(int(guild.id), sched["prize_id"])
    if not prize or not prize["enabled"]:
        return
    channel = guild.get_channel(int(sched["channel_id"]))
    if channel is None or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return
    view = PrizeClaimView(guild_id=int(guild.id), schedule_id=sched["schedule_id"], prize_id=sched["prize_id"])
    embed = discord.Embed(title=prize["title"], description=prize["description"] or None)
    if prize["image_url"]:
        try:
            embed.set_image(url=prize["image_url"])
        except Exception:
            pass
    content = MSG["prize_drop"]
    try:
        msg = await channel.send(content=content, embed=embed, view=view)
    except Exception:
        return
    await prize_mark_used(int(guild.id), sched["schedule_id"])
    drop_id = await prize_create_drop(int(guild.id), sched["schedule_id"], int(channel.id), int(msg.id))
    view.drop_id = drop_id
    try:
        await msg.edit(view=view)
    except Exception:
        pass

async def plague_cleanup_once(bot: commands.Bot):
    expired = await plague_get_expired()
    for e in expired:
        guild = bot.get_guild(int(e["guild_id"]))
        if guild is None:
            await plague_delete_infection(int(e["guild_id"]), int(e["user_id"]))
            continue
        settings = await get_guild_settings(int(guild.id))
        role_id = settings["plague_role_id"]
        if role_id:
            role = guild.get_role(int(role_id))
        else:
            role = None
        member = guild.get_member(int(e["user_id"]))
        if member and role and role in member.roles:
            try:
                await member.remove_roles(role, reason="Plague: infection expired")
            except Exception:
                pass
        await plague_delete_infection(int(guild.id), int(e["user_id"]))

############### VIEWS / UI COMPONENTS ###############
class PrizeClaimView(discord.ui.View):
    def __init__(self, guild_id: int, schedule_id: uuid.UUID, prize_id: uuid.UUID):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.schedule_id = schedule_id
        self.prize_id = prize_id
        self.drop_id = None

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success)
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if self.drop_id is None:
            await interaction.response.send_message("This drop is not ready yet.", ephemeral=True)
            return
        ok = await prize_claim_drop_atomic(int(interaction.guild.id), self.drop_id, int(interaction.user.id))
        if not ok:
            await interaction.response.send_message("Too late — already claimed.", ephemeral=True)
            return
        button.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        prize = await prize_get_definition(int(interaction.guild.id), self.prize_id)
        title = prize["title"] if prize else "Prize"
        await interaction.response.send_message(f"✅ You claimed **{title}**!", ephemeral=True)
        try:
            text = MSG["prize_claimed_channel"].replace("{prize}", title)
            text = format_template(text, interaction.user)
            await interaction.channel.send(text)
        except Exception:
            pass

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
    out = []
    for tz in common:
        if current_l in tz.lower():
            out.append(discord.app_commands.Choice(name=tz, value=tz))
        if len(out) >= 25:
            break
    if not out:
        out = [discord.app_commands.Choice(name="America/Los_Angeles", value="America/Los_Angeles")]
    return out

async def prize_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []
    guild_id = int(interaction.guild.id)
    rows = await prize_find_definitions(guild_id, current, limit=25)
    out = []
    for r in rows:
        name = r["title"][:90]
        out.append(discord.app_commands.Choice(name=name, value=str(r["prize_id"])))
    return out

async def schedule_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []
    guild_id = int(interaction.guild.id)
    settings = await get_guild_settings(guild_id)
    local_now = guild_now(settings["timezone"])
    upcoming = await prize_schedule_list_upcoming(guild_id, local_now.date(), limit=25)
    out = []
    cur = (current or "").lower().strip()
    for s in upcoming:
        nbt = s["not_before_time"].strftime("%H:%M") if s["not_before_time"] else "Any"
        label = f"{s['day'].isoformat()} @ {nbt}"
        if cur and cur not in label.lower():
            continue
        out.append(discord.app_commands.Choice(name=label, value=str(s["schedule_id"])))
        if len(out) >= 25:
            break
    return out

############### BACKGROUND TASKS & SCHEDULERS ###############

async def birthday_daily_loop(bot: commands.Bot):
    last_seen: dict[int, date] = {}
    while not bot.is_closed():
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT guild_id, timezone, birthday_enabled, birthday_role_id, birthday_channel_id, birthday_message_text FROM guild_settings;")
            for r in rows:
                guild_id = int(r["guild_id"])
                tz = r["timezone"] or "America/Los_Angeles"
                today = guild_now(tz).date()
                if last_seen.get(guild_id) == today:
                    continue
                last_seen[guild_id] = today

                guild = bot.get_guild(guild_id)
                if guild is None:
                    continue

                if not r.get("birthday_enabled"):
                    continue
                if not r.get("birthday_role_id") or not r.get("birthday_channel_id"):
                    continue

                role_id = r["birthday_role_id"]
                role = guild.get_role(int(role_id)) if role_id else None

                bdays = await birthday_list_all(guild_id)
                todays = [b for b in bdays if int(b["month"]) == today.month and int(b["day"]) == today.day]
                if role:
                    try:
                        for m in role.members:
                            if int(m.id) not in {int(b["user_id"]) for b in todays}:
                                await m.remove_roles(role, reason="Birthday ended")
                    except Exception:
                        pass

                if not todays:
                    await update_birthday_list_message(bot, guild_id)
                    continue

                channel = guild.get_channel(int(r["birthday_channel_id"])) if r["birthday_channel_id"] else None
                msg_t = r["birthday_message_text"] or MSG["birthday_announce"]

                for b in todays:
                    uid = int(b["user_id"])
                    member = guild.get_member(uid)
                    if member is None:
                        continue
                    if role:
                        try:
                            await member.add_roles(role, reason="Birthday")
                        except Exception:
                            pass
                    if channel:
                        try:
                            if not await birthday_was_announced(guild_id, uid, today):
                                await channel.send(format_template(msg_t, member))
                                await birthday_mark_announced(guild_id, uid, today)
                        except Exception:
                            pass

                await update_birthday_list_message(bot, guild_id)
        except Exception:
            pass
        await asyncio.sleep(60)

async def update_birthday_list_message(bot: commands.Bot, guild_id: int) -> None:
    try:
        settings = await get_guild_extras(guild_id)
        ch_id = settings.get("birthday_list_channel_id")
        msg_id = settings.get("birthday_list_message_id")
        if not ch_id or not msg_id:
            return
        guild = bot.get_guild(guild_id)
        if guild is None:
            return
        channel = guild.get_channel(int(ch_id))
        if channel is None:
            return
        try:
            msg = await channel.fetch_message(int(msg_id))
        except Exception:
            return

        bdays = await birthday_list_all(guild_id)
        lines=[]
        for b in bdays:
            u = guild.get_member(int(b["user_id"])) or guild.get_member(int(b["user_id"]))
            tag = u.mention if u else f"<@{int(b['user_id'])}>"
            md = f"{int(b['month']):02d}/{int(b['day']):02d}"
            lines.append(f"{md} — {tag}")
        desc = "\n".join(lines) if lines else "No birthdays saved yet."
        embed = discord.Embed(title="🎂 Birthdays", description=desc)
        await msg.edit(embed=embed, content=None)
    except Exception:
        pass

async def qotd_daily_loop(bot: commands.Bot):
    last_seen: dict[int, date] = {}
    while not bot.is_closed():
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT guild_id, timezone, qotd_channel_id, qotd_role_id, qotd_message_prefix, qotd_source_url FROM guild_settings;")
            for r in rows:
                guild_id = int(r["guild_id"])
                tz = r["timezone"] or "America/Los_Angeles"
                today = guild_now(tz).date()
                now_local = guild_now(tz)
                if now_local.hour != 9:
                    continue
                if last_seen.get(guild_id) == today:
                    continue
                if not r["qotd_channel_id"] or not r["qotd_source_url"]:
                    continue
                if await qotd_was_posted_today(guild_id, today):
                    last_seen[guild_id] = today
                    continue

                guild = bot.get_guild(guild_id)
                if guild is None:
                    continue
                channel = guild.get_channel(int(r["qotd_channel_id"]))
                if channel is None:
                    continue

                try:
                    questions = await fetch_questions_from_source(r["qotd_source_url"])
                except Exception:
                    continue
                if not questions:
                    continue
                recent = await qotd_recent_hashes(guild_id, limit=300)
                pick = None
                for q in questions:
                    if _hash_question(q) not in recent:
                        pick = q
                        break
                if pick is None:
                    pick = questions[0] 

                prefix = r["qotd_message_prefix"] or MSG["qotd_header"]
                role_ping = ""
                if r["qotd_role_id"]:
                    role = guild.get_role(int(r["qotd_role_id"]))
                    if role:
                        role_ping = role.mention + " "
                try:
                    await channel.send(f"{role_ping}{prefix}\n{pick}")
                    await qotd_record_post(guild_id, today, pick)
                    last_seen[guild_id] = today
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(30)

# -------- Message-level scheduler helpers --------
_autodelete_tasks: dict[tuple[int,int,int], asyncio.Task] = {}

async def schedule_message_delete(message: discord.Message, delay_seconds: int, log_channel_id: int | None):
    key = (int(message.guild.id), int(message.channel.id), int(message.id))
    async def _job():
        try:
            await asyncio.sleep(max(1, delay_seconds))
            try:
                await message.delete()
            except Exception:
                pass
        finally:
            _autodelete_tasks.pop(key, None)
    t = asyncio.create_task(_job())
    _autodelete_tasks[key] = t


async def active_cleanup_loop(bot: commands.Bot):
    while True:
        try:
            await active_cleanup_once(bot)
        except Exception:
            pass
        await asyncio.sleep(60)

async def plague_cleanup_loop(bot: commands.Bot):
    while True:
        try:
            await plague_cleanup_once(bot)
        except Exception:
            pass
        await asyncio.sleep(60)

async def deadchat_cleanup_loop():
    while True:
        await asyncio.sleep(300)


############### BIRTHDAY / QOTD / STICKY / AUTODELETE / VOICE / WELCOME HELPERS ###############
import hashlib
import aiohttp

def _hash_question(q: str) -> str:
    return hashlib.sha256(q.strip().encode("utf-8")).hexdigest()

async def get_guild_extras(guild_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""SELECT
            birthday_role_id, birthday_channel_id, birthday_message_text,
            birthday_list_channel_id, birthday_list_message_id,
            qotd_channel_id, qotd_role_id, qotd_message_prefix, qotd_source_url, qotd_last_posted_date,
            welcome_channel_id, welcome_message_text, welcome_enabled, member_role_id, member_role_delay_seconds, bot_role_id,
            logging_enabled, modlog_channel_id, timezone
          FROM guild_settings WHERE guild_id=$1""", guild_id)
    if not row:
        return {}
    return dict(row)

# -------- Birthdays --------
async def birthday_set(guild_id: int, user_id: int, month: int, day: int, year: int | None, set_by: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO birthdays (guild_id,user_id,month,day,year,set_by_user_id)
                 VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (guild_id,user_id) DO UPDATE
                   SET month=EXCLUDED.month, day=EXCLUDED.day, year=EXCLUDED.year,
                       set_by_user_id=EXCLUDED.set_by_user_id, updated_at=NOW();""",
            guild_id, user_id, month, day, year, set_by
        )

async def birthday_remove(guild_id: int, user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM birthdays WHERE guild_id=$1 AND user_id=$2;", guild_id, user_id)

async def birthday_get(guild_id: int, user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT month,day,year FROM birthdays WHERE guild_id=$1 AND user_id=$2;", guild_id, user_id)

async def birthday_list_all(guild_id: int):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id,month,day,year FROM birthdays WHERE guild_id=$1 ORDER BY month,day,user_id;", guild_id)
    return [dict(r) for r in rows]

async def birthday_mark_announced(guild_id: int, user_id: int, d: date) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO birthday_announce_log (guild_id,user_id,day) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING;",
            guild_id, user_id, d
        )

async def birthday_was_announced(guild_id: int, user_id: int, d: date) -> bool:
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT 1 FROM birthday_announce_log WHERE guild_id=$1 AND user_id=$2 AND day=$3;", guild_id, user_id, d)
    return r is not None

async def birthday_set_role_channel_message(guild_id: int, role_id: int | None, channel_id: int | None, message_text: str | None) -> None:
    async with db_pool.acquire() as conn:
        await ensure_guild_row(guild_id)
        await conn.execute(
            """UPDATE guild_settings
                 SET birthday_role_id=COALESCE($2,birthday_role_id),
                     birthday_channel_id=COALESCE($3,birthday_channel_id),
                     birthday_message_text=COALESCE($4,birthday_message_text),
                     updated_at=NOW()
                 WHERE guild_id=$1;""",
            guild_id, role_id, channel_id, message_text
        )


async def birthday_set_enabled(guild_id: int, enabled: bool) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE guild_settings SET birthday_enabled=$2 WHERE guild_id=$1;",
            int(guild_id),
            bool(enabled),
        )


async def birthday_set_list_message(guild_id: int, channel_id: int | None, message_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await ensure_guild_row(guild_id)
        await conn.execute(
            "UPDATE guild_settings SET birthday_list_channel_id=$2, birthday_list_message_id=$3, updated_at=NOW() WHERE guild_id=$1;",
            guild_id, channel_id, message_id
        )

# -------- Sticky --------
async def sticky_set(guild_id: int, channel_id: int, content: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO sticky_messages (guild_id,channel_id,content)
                 VALUES ($1,$2,$3)
                 ON CONFLICT (guild_id,channel_id) DO UPDATE
                   SET content=EXCLUDED.content, updated_at=NOW();""",
            guild_id, channel_id, content
        )

async def sticky_clear(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM sticky_messages WHERE guild_id=$1 AND channel_id=$2;", guild_id, channel_id)

async def sticky_get(guild_id: int, channel_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT content,message_id FROM sticky_messages WHERE guild_id=$1 AND channel_id=$2;", guild_id, channel_id)

async def sticky_update_message_id(guild_id: int, channel_id: int, message_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sticky_messages SET message_id=$3, updated_at=NOW() WHERE guild_id=$1 AND channel_id=$2;", guild_id, channel_id, message_id)

# -------- Autodelete --------
async def autodelete_set_channel(guild_id: int, channel_id: int, seconds: int, log_channel_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO autodelete_channels (guild_id,channel_id,delete_after_seconds,log_channel_id)
                 VALUES ($1,$2,$3,$4)
                 ON CONFLICT (guild_id,channel_id) DO UPDATE
                   SET delete_after_seconds=EXCLUDED.delete_after_seconds, log_channel_id=EXCLUDED.log_channel_id;""",
            guild_id, channel_id, seconds, log_channel_id
        )

async def autodelete_remove_channel(guild_id: int, channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM autodelete_channels WHERE guild_id=$1 AND channel_id=$2;", guild_id, channel_id)

async def autodelete_get_channel(guild_id: int, channel_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT delete_after_seconds,log_channel_id FROM autodelete_channels WHERE guild_id=$1 AND channel_id=$2;", guild_id, channel_id)

async def autodelete_add_ignore_phrase(guild_id: int, phrase: str) -> None:
    phrase = phrase.strip()
    if not phrase:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO autodelete_ignore_phrases (guild_id,phrase) VALUES ($1,$2) ON CONFLICT DO NOTHING;", guild_id, phrase)

async def autodelete_remove_ignore_phrase(guild_id: int, phrase: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM autodelete_ignore_phrases WHERE guild_id=$1 AND phrase=$2;", guild_id, phrase.strip())

async def autodelete_list_ignore_phrases(guild_id: int) -> list[str]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT phrase FROM autodelete_ignore_phrases WHERE guild_id=$1 ORDER BY phrase ASC;", guild_id)
    return [r["phrase"] for r in rows]

# -------- Voice Role Links --------
async def voice_role_set_link(guild_id: int, voice_channel_id: int, role_id: int, mode: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO voice_role_links (guild_id,voice_channel_id,role_id,mode)
                 VALUES ($1,$2,$3,$4)
                 ON CONFLICT (guild_id,voice_channel_id) DO UPDATE
                   SET role_id=EXCLUDED.role_id, mode=EXCLUDED.mode;""",
            guild_id, voice_channel_id, role_id, mode
        )

async def voice_role_remove_link(guild_id: int, voice_channel_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM voice_role_links WHERE guild_id=$1 AND voice_channel_id=$2;", guild_id, voice_channel_id)

async def voice_role_list_links(guild_id: int):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT voice_channel_id,role_id,mode FROM voice_role_links WHERE guild_id=$1 ORDER BY voice_channel_id ASC;", guild_id)
    return [dict(r) for r in rows]

async def voice_role_get_link(guild_id: int, voice_channel_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT role_id,mode FROM voice_role_links WHERE guild_id=$1 AND voice_channel_id=$2;", guild_id, voice_channel_id)

# -------- Welcome / Modlog --------

async def set_modlog_channel(guild_id: int, channel_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE guild_settings SET modlog_channel_id=$2, updated_at=NOW() WHERE guild_id=$1",
            guild_id,
            channel_id,
        )

async def set_logging_enabled(guild_id: int, enabled: bool) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE guild_settings SET logging_enabled=$2, updated_at=NOW() WHERE guild_id=$1",
            guild_id,
            enabled,
        )

async def welcome_set(guild_id: int, channel_id: int | None, text: str | None, member_role_id: int | None, delay_seconds: int | None, bot_role_id: int | None, modlog_channel_id: int | None) -> None:
    async with db_pool.acquire() as conn:
        await ensure_guild_row(guild_id)
        await conn.execute(
            """UPDATE guild_settings SET
                welcome_channel_id=COALESCE($2,welcome_channel_id),
                welcome_message_text=COALESCE($3,welcome_message_text),
                member_role_id=COALESCE($4,member_role_id),
                member_role_delay_seconds=COALESCE($5,member_role_delay_seconds),
                bot_role_id=COALESCE($6,bot_role_id),
                modlog_channel_id=COALESCE($7,modlog_channel_id),
                updated_at=NOW()
              WHERE guild_id=$1;""",
            guild_id, channel_id, text, member_role_id, delay_seconds, bot_role_id, modlog_channel_id
        )



async def welcome_set_enabled(guild_id: int, enabled: bool) -> None:
    async with db_pool.acquire() as conn:
        await ensure_guild_row(guild_id)
        await conn.execute(
            "UPDATE guild_settings SET welcome_enabled=$2, updated_at=NOW() WHERE guild_id=$1;",
            guild_id, bool(enabled)
        )

async def welcome_set_message(guild_id: int, text: str) -> None:
    async with db_pool.acquire() as conn:
        await ensure_guild_row(guild_id)
        await conn.execute(
            "UPDATE guild_settings SET welcome_message_text=$2, updated_at=NOW() WHERE guild_id=$1;",
            guild_id, text
        )

# -------- QOTD --------
async def qotd_set(guild_id: int, channel_id: int | None, role_id: int | None, prefix: str | None, source_url: str | None) -> None:
    async with db_pool.acquire() as conn:
        await ensure_guild_row(guild_id)
        await conn.execute(
            """UPDATE guild_settings SET
                qotd_channel_id=COALESCE($2,qotd_channel_id),
                qotd_role_id=COALESCE($3,qotd_role_id),
                qotd_message_prefix=COALESCE($4,qotd_message_prefix),
                qotd_source_url=COALESCE($5,qotd_source_url),
                updated_at=NOW()
              WHERE guild_id=$1;""",
            guild_id, channel_id, role_id, prefix, source_url
        )

async def qotd_record_post(guild_id: int, posted_on: date, question_text: str) -> None:
    qh = _hash_question(question_text)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO qotd_history (guild_id,question_hash,posted_on,question_text) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING;",
            guild_id, qh, posted_on, question_text
        )
        await conn.execute("UPDATE guild_settings SET qotd_last_posted_date=$2, updated_at=NOW() WHERE guild_id=$1;", guild_id, posted_on)

async def qotd_was_posted_today(guild_id: int, d: date) -> bool:
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT 1 FROM qotd_history WHERE guild_id=$1 AND posted_on=$2;", guild_id, d)
    return r is not None

async def qotd_recent_hashes(guild_id: int, limit: int = 200) -> set[str]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT question_hash FROM qotd_history WHERE guild_id=$1 ORDER BY posted_on DESC LIMIT $2;", guild_id, limit)
    return {r["question_hash"] for r in rows}

async def fetch_questions_from_source(source_url: str) -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(source_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            txt = await resp.text()
    if "," in txt.splitlines()[0]:
        out=[]
        for line in txt.splitlines()[1:]:
            if not line.strip():
                continue
            q=line.split(",")[0].strip().strip('"')
            if q:
                out.append(q)
        return out
    return [l.strip() for l in txt.splitlines() if l.strip()]

def format_template(t: str, user: discord.abc.User) -> str:
    return (t or "").replace("{user}", user.mention).replace("{name}", user.display_name)


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
    try:
        if await should_count_activity_message(guild_id, channel_id):
            await ensure_guild_row(guild_id)
            await record_activity(guild_id, int(message.author.id))
            if isinstance(message.author, discord.Member):
                await maybe_apply_active_role(message.author)
    except Exception:
        pass
    try:
        if await deadchat_is_configured(guild_id, channel_id):
            await deadchat_attempt_award(bot, message)
        else:
            await deadchat_update_last_message(guild_id, channel_id)
    except Exception:
        pass

    try:
        s = await sticky_get(guild_id, channel_id)
        if s:
            old_id = s["message_id"]
            content = s["content"]
            if old_id:
                try:
                    old_msg = await message.channel.fetch_message(int(old_id))
                    await old_msg.delete()
                except Exception:
                    pass
            try:
                new_msg = await message.channel.send(content)
                await sticky_update_message_id(guild_id, channel_id, int(new_msg.id))
            except Exception:
                pass
    except Exception:
        pass

    try:
        ad = await autodelete_get_channel(guild_id, channel_id)
        if ad:
            phrases = await autodelete_list_ignore_phrases(guild_id)
            lowered = (message.content or "").lower()
            if any(p.lower() in lowered for p in phrases):
                pass
            else:
                await schedule_message_delete(message, int(ad["delete_after_seconds"]), int(ad["log_channel_id"]) if ad["log_channel_id"] else None)
    except Exception:
        pass
    await bot.process_commands(message)


@bot.event
async def on_member_join(member: discord.Member):
    if member.guild is None:
        return
    guild_id = int(member.guild.id)
    try:
        await ensure_guild_row(guild_id)
        s = await get_guild_extras(guild_id)

        if member.bot and s.get("bot_role_id"):
            role = member.guild.get_role(int(s["bot_role_id"]))
            if role:
                try:
                    await member.add_roles(role, reason="Auto role for bots")
                except Exception:
                    pass
        if (not member.bot) and s.get("member_role_id"):
            role = member.guild.get_role(int(s["member_role_id"]))
            if role:
                delay = int(s.get("member_role_delay_seconds") or 0)
                async def _add_later():
                    await asyncio.sleep(max(0, delay))
                    try:
                        await member.add_roles(role, reason="Delayed member role")
                    except Exception:
                        pass
                asyncio.create_task(_add_later())

        if s.get("welcome_enabled") and s.get("welcome_channel_id"):
            ch = member.guild.get_channel(int(s["welcome_channel_id"]))
            if ch:
                txt = s.get("welcome_message_text") or MSG["welcome"]
                try:
                    await ch.send(format_template(txt, member))
                except Exception:
                    pass

        if s.get("logging_enabled") and s.get("modlog_channel_id"):
            ch = member.guild.get_channel(int(s["modlog_channel_id"]))
            if ch:
                try:
                    await ch.send(f"✅ {member.mention} joined. (id: {member.id})")
                except Exception:
                    pass
    except Exception:
        pass

@bot.event
async def on_member_remove(member: discord.Member):
    if member.guild is None:
        return
    guild_id = int(member.guild.id)
    try:
        s = await get_guild_extras(guild_id)
        if (not s.get("logging_enabled")) or (not s.get("modlog_channel_id")):
            return
        ch = member.guild.get_channel(int(s["modlog_channel_id"]))
        if not ch:
            return

        action = "left"
        actor = None
        try:
            async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.kick):
                if entry.target and int(entry.target.id) == int(member.id):
                    actor = entry.user
                    action = "was kicked"
                    break
        except Exception:
            pass

        msg = f"🚪 {member} {action}."
        if actor:
            msg += f" By {actor}."
        await ch.send(msg)
    except Exception:
        pass

@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    try:
        s = await get_guild_extras(int(guild.id))
        if (not s.get("logging_enabled")) or (not s.get("modlog_channel_id")):
            return
        ch = guild.get_channel(int(s["modlog_channel_id"]))
        if not ch:
            return
        actor = None
        try:
            async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
                if entry.target and int(entry.target.id) == int(user.id):
                    actor = entry.user
                    break
        except Exception:
            pass
        msg = f"⛔ {user} was banned."
        if actor:
            msg += f" By {actor}."
        await ch.send(msg)
    except Exception:
        pass

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.guild is None:
        return
    guild_id = int(member.guild.id)

    if before.channel and (not after.channel or int(after.channel.id) != int(before.channel.id)):
        link = await voice_role_get_link(guild_id, int(before.channel.id))
        if link:
            role = member.guild.get_role(int(link["role_id"]))
            if role:
                try:
                    if link["mode"] == "add_on_join":
                        await member.remove_roles(role, reason="Left voice channel")
                    else:
                        await member.add_roles(role, reason="Left voice channel")
                except Exception:
                    pass

    if after.channel and (not before.channel or int(before.channel.id) != int(after.channel.id)):
        link = await voice_role_get_link(guild_id, int(after.channel.id))
        if link:
            role = member.guild.get_role(int(link["role_id"]))
            if role:
                try:
                    if link["mode"] == "add_on_join":
                        await member.add_roles(role, reason="Joined voice channel")
                    else:
                        await member.remove_roles(role, reason="Joined voice channel")
                except Exception:
                    pass



############### COMMAND GROUPS ###############

def _count_set(**kwargs) -> list[str]:
    """Return list of keys where value is not None / not Falsey for strings. Booleans count if not None."""
    used: list[str] = []
    for k, v in kwargs.items():
        if isinstance(v, bool):
            if v is not None:
                used.append(k)
        else:
            if v is not None:
                used.append(k)
    return used

async def _require_one_action(interaction: discord.Interaction, used: list[str], hint: str) -> bool:
    if len(used) == 0:
        await interaction.response.send_message(f"❌ Choose one option.\n{hint}", ephemeral=True)
        return False
    if len(used) > 1:
        await interaction.response.send_message(
            f"❌ Use **only one** option at a time.\nYou used: {', '.join(used)}",
            ephemeral=True,
        )
        return False
    return True

@bot.tree.command(name="birthday_set", description="Set your birthday (month/day/year)")
async def birthday_set_cmd(interaction: discord.Interaction, month: int, day: int, year: int | None = None):
    guild_id = require_guild(interaction)
    await birthday_set(guild_id, int(interaction.user.id), int(month), int(day), year, int(interaction.user.id))
    await update_birthday_list_message(bot, guild_id)
    await interaction.response.send_message(f"✅ Birthday saved: {int(month):02d}/{int(day):02d}", ephemeral=True)

config_group = discord.app_commands.Group(name="config", description="Configuration commands")
messages_group = discord.app_commands.Group(name="messages", description="Message/announcement commands")
schedule_group = discord.app_commands.Group(name="schedule", description="Scheduling commands")

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="system", description="System utilities")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
@discord.app_commands.autocomplete(timezone_set=timezone_autocomplete)
async def config_system_cmd(
    interaction: discord.Interaction,
    info: bool | None = None,
    timezone_set: str | None = None,
    timezone_show: bool | None = None,
    ping: bool | None = None,
    health_check: bool | None = None,
    legacy_preview: bool | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(info=info, timezone_set=timezone_set, timezone_show=timezone_show, ping=ping, health_check=health_check, legacy_preview=legacy_preview)
    ok = await _require_one_action(
        interaction,
        used,
        "Examples: `/config system ping:true` • `/config system health_check:true` • `/config system legacy_preview:true` • `/config system timezone_set:America/Los_Angeles` • `/config system timezone_show:true` • `/config system info:true`",
    )
    if not ok:
        return

    action = used[0]

    if action == "ping":
        if not await require_dev_guild(interaction):
            return
        await interaction.response.send_message("pong ✅", ephemeral=True)
        return

    if action == "health_check":
        if not await require_dev_guild(interaction):
            return
        title, lines = await run_test_all(interaction)
        embed = discord.Embed(title=title, description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    if action == "legacy_preview":
        if not await require_dev_guild(interaction):
            return
        data = await legacy_preview(interaction)

        lines = []
        raw = data.get("raw", {})
        errs = data.get("errors", [])

        b = raw.get("BIRTHDAYS_JSON")
        if isinstance(b, dict):
            total_bd = 0
            for _g, payload in b.items():
                try:
                    bd = payload.get("birthdays", {})
                    total_bd += len(bd) if isinstance(bd, dict) else 0
                except Exception:
                    pass
            lines.append(f"• birthdays: {total_bd}")
            try:
                any_payload = next(iter(b.values()))
                pm = any_payload.get("public_message") if isinstance(any_payload, dict) else None
                if isinstance(pm, dict) and pm.get("channel_id") and pm.get("message_id"):
                    lines.append("• birthday public message: present")
            except Exception:
                pass
        else:
            lines.append("• birthdays: not found")

        pool = raw.get("POOL_DATA")
        if isinstance(pool, dict):
            total_entries = 0
            try:
                for _g, payload in pool.items():
                    entries = payload.get("entries", [])
                    total_entries += len(entries) if isinstance(entries, list) else 0
            except Exception:
                pass
            lines.append(f"• movie pool entries: {total_entries}")
            try:
                any_payload = next(iter(pool.values()))
                msg = any_payload.get("message") if isinstance(any_payload, dict) else None
                if isinstance(msg, dict) and msg.get("channel_id") and msg.get("message_id"):
                    lines.append("• pool public message: present")
            except Exception:
                pass
        else:
            lines.append("• movie pool: not found")

        for key, label in [
            ("STICKY_DATA", "stickies"),
            ("DEADCHAT_DATA", "deadchat timestamps"),
            ("DEADCHAT_STATE", "deadchat state"),
            ("PLAGUE_DATA", "plague state"),
            ("ACTIVITY_DATA", "activity data"),
            ("CONFIG_DATA", "config data"),
            ("PRIZE_MOVIE_DATA", "prize movie"),
            ("PRIZE_NITRO_DATA", "prize nitro"),
            ("PRIZE_STEAM_DATA", "prize steam"),
            ("MEMBERJOIN_DATA", "member join data"),
            ("TWITCH_STATE", "twitch state"),
        ]:
            val = raw.get(key)
            if val is None:
                lines.append(f"• {label}: not found")
            else:
                try:
                    lines.append(f"• {label}: present ({len(val)})")
                except Exception:
                    lines.append(f"• {label}: present")

        if errs:
            lines.append("• errors:")
            for e in errs[:5]:
                lines.append(f"  - {e}")

        await interaction.response.send_message(
            "Legacy preview (no DB writes):\n" + "\n".join(lines),
            ephemeral=True,
        )
        return


    if action == "timezone_set":
        await upsert_timezone(guild_id, timezone_set)
        await interaction.response.send_message(f"✅ Timezone set to `{timezone_set}`", ephemeral=True)
        return

    if action == "timezone_show":
        s = await get_guild_settings(guild_id)
        await interaction.response.send_message(f"⏰ Current timezone: `{s['timezone']}`", ephemeral=True)
        return

    s = await get_guild_extras(guild_id)
    await interaction.response.send_message(
        "Settings:\n"
        f"- timezone: `{s.get('timezone')}`\n"
        f"- birthday_enabled: `{bool(s.get('birthday_enabled'))}`\n"
        f"- welcome_enabled: `{bool(s.get('welcome_enabled'))}`\n"
        f"- logging_enabled: `{bool(s.get('logging_enabled'))}`\n",
        ephemeral=True,
    )

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="logging", description="Configure join/leave logging")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def config_logging_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    set_channel: discord.TextChannel | None = None,
):
    guild_id = require_guild(interaction)

    used = _count_set(enable=enable, set_channel=set_channel)
    ok = await _require_one_action(
        interaction,
        used,
        "Example: `/config logging enable:true` or `/config logging set_channel:#modlog`",
    )
    if not ok:
        return

    action = used[0]

    if action == "set_channel":
        await set_modlog_channel(guild_id, int(set_channel.id))
        await interaction.response.send_message(
            f"✅ Logging channel set to {set_channel.mention}",
            ephemeral=True,
        )
        return

    if action == "enable":
        if enable:
            s = await get_guild_extras(guild_id)
            if not s.get("modlog_channel_id"):
                return await interaction.response.send_message(
                    "❌ Set a logging channel first using `set_channel`.",
                    ephemeral=True,
                )

        await set_logging_enabled(guild_id, bool(enable))
        await interaction.response.send_message(
            f"✅ Logging enabled set to `{bool(enable)}`",
            ephemeral=True,
        )

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="active", description="Activity Tracking settings")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
@discord.app_commands.choices(set_threshold=ACTIVE_THRESHOLD_CHOICES, set_mode=ACTIVE_MODE_CHOICES)
async def config_active_cmd(
    interaction: discord.Interaction,
    add_channel: discord.TextChannel | None = None,
    remove_channel: discord.TextChannel | None = None,
    set_role: discord.Role | None = None,
    clear_role: bool | None = None,
    set_threshold: discord.app_commands.Choice[int] | None = None,
    set_mode: discord.app_commands.Choice[str] | None = None,
    list_channels: bool | None = None,
    show_active: bool | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(
        add_channel=add_channel,
        remove_channel=remove_channel,
        set_role=set_role,
        clear_role=clear_role,
        set_threshold=set_threshold,
        set_mode=set_mode,
        list_channels=list_channels,
        show_active=show_active,
    )
    ok = await _require_one_action(interaction, used, "Example: `/config active set_role:@Active` or `/config active add_channel:#general`")
    if not ok:
        return
    action = used[0]

    if action == "set_role":
        await set_active_role(guild_id, int(set_role.id))
        await interaction.response.send_message(f"✅ Active role set to {set_role.mention}", ephemeral=True)
        return

    if action == "clear_role":
        await set_active_role(guild_id, None)
        await interaction.response.send_message("✅ Active role cleared", ephemeral=True)
        return

    if action == "set_threshold":
        await set_active_threshold(guild_id, int(set_threshold.value))
        await interaction.response.send_message(f"✅ Active threshold set to {set_threshold.value} minute(s)", ephemeral=True)
        return

    if action == "set_mode":
        await set_active_mode(guild_id, str(set_mode.value))
        await interaction.response.send_message(f"✅ Activity mode set to `{set_mode.value}`", ephemeral=True)
        return

    if action == "add_channel":
        await add_activity_channel(guild_id, int(add_channel.id))
        await interaction.response.send_message(f"✅ Added {add_channel.mention} to activity channels", ephemeral=True)
        return

    if action == "remove_channel":
        await remove_activity_channel(guild_id, int(remove_channel.id))
        await interaction.response.send_message(f"✅ Removed {remove_channel.mention} from activity channels", ephemeral=True)
        return

    if action == "list_channels":
        ids = await list_activity_channels(guild_id)
        if not ids:
            await interaction.response.send_message("No activity channels set.", ephemeral=True)
            return
        mentions = []
        for cid in ids:
            ch = interaction.guild.get_channel(cid) if interaction.guild else None
            mentions.append(ch.mention if ch else f"`{cid}`")
        await interaction.response.send_message("Activity channels:\n" + "\n".join(mentions), ephemeral=True)
        return

    s = await get_guild_settings(guild_id)
    await interaction.response.send_message(
        f"Mode: `{s['active_mode']}`\nThreshold: `{s['active_threshold_minutes']}` minute(s)\nActive role ID: `{s['active_role_id']}`",
        ephemeral=True,
    )

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="qotd", description="Configure QOTD")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def config_qotd_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    set_channel: discord.TextChannel | None = None,
    set_role: discord.Role | None = None,
    set_prefix: str | None = None,
    set_source: str | None = None,
    post_now: bool | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(enable=enable, set_channel=set_channel, set_role=set_role, set_prefix=set_prefix, set_source=set_source, post_now=post_now)
    ok = await _require_one_action(interaction, used, "Example: `/config qotd set_channel:#general` or `/config qotd set_source:https://...`")
    if not ok:
        return
    action = used[0]

    if action == "enable":
        if enable:
            s = await get_guild_extras(guild_id)
            if not s.get("qotd_channel_id") or not s.get("qotd_source_url"):
                return await interaction.response.send_message("❌ Set `set_channel` and `set_source` first.", ephemeral=True)
        await interaction.response.send_message("✅ QOTD enable/disable is controlled by whether channel+source are set.", ephemeral=True)
        return

    if action == "set_channel":
        await qotd_set(guild_id, int(set_channel.id), None, None, None)
        await interaction.response.send_message(f"✅ QOTD channel set to {set_channel.mention}", ephemeral=True)
        return

    if action == "set_role":
        await qotd_set(guild_id, None, int(set_role.id) if set_role else None, None, None)
        await interaction.response.send_message("✅ QOTD role updated.", ephemeral=True)
        return

    if action == "set_prefix":
        await qotd_set(guild_id, None, None, set_prefix, None)
        await interaction.response.send_message("✅ QOTD prefix updated.", ephemeral=True)
        return

    if action == "set_source":
        await qotd_set(guild_id, None, None, None, set_source)
        await interaction.response.send_message("✅ QOTD source updated.", ephemeral=True)
        return

    s = await get_guild_extras(guild_id)
    if not s.get("qotd_channel_id") or not s.get("qotd_source_url"):
        return await interaction.response.send_message("❌ Set qotd channel + source first.", ephemeral=True)
    guild = interaction.guild
    channel = guild.get_channel(int(s["qotd_channel_id"])) if guild else None
    if not channel:
        return await interaction.response.send_message("❌ QOTD channel not found.", ephemeral=True)
    try:
        questions = await fetch_questions_from_source(s["qotd_source_url"])
        recent = await qotd_recent_hashes(guild_id, limit=300)
        pick = None
        for q in questions:
            if _hash_question(q) not in recent:
                pick = q
                break
        if pick is None:
            pick = questions[0] if questions else None
        if not pick:
            return await interaction.response.send_message("❌ No questions found at source.", ephemeral=True)
        role_ping = ""
        if s.get("qotd_role_id"):
            role = guild.get_role(int(s["qotd_role_id"]))
            if role:
                role_ping = role.mention + " "
        prefix = s.get("qotd_message_prefix") or "❓ **Question of the Day**"
        await channel.send(f"{role_ping}{prefix}\n{pick}")
        await qotd_record_post(guild_id, guild_now(s.get("timezone") or "America/Los_Angeles").date(), pick)
        await interaction.response.send_message("✅ Posted QOTD.", ephemeral=True)
    except Exception:
        await interaction.response.send_message("❌ Failed to post QOTD (check source URL).", ephemeral=True)

@discord.app_commands.default_permissions(manage_channels=True)
@config_group.command(name="autodelete", description="Auto-delete configuration")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_channels=True)
async def config_autodelete_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    add_channels: discord.TextChannel | None = None,
    filter_minutes: int | None = None,
    filter_ignore_words: str | None = None,
    remove_channel: discord.TextChannel | None = None,
    ignore_remove: str | None = None,
    ignore_list: bool | None = None,
    log_channel: discord.TextChannel | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(
        enable=enable,
        add_channels=add_channels,
        filter_minutes=filter_minutes,
        filter_ignore_words=filter_ignore_words,
        remove_channel=remove_channel,
        ignore_remove=ignore_remove,
        ignore_list=ignore_list,
    )
    ok = await _require_one_action(interaction, used, "Example: `/config autodelete add_channels:#general filter_minutes:60`")
    if not ok:
        return
    action = used[0]

    if action == "enable":
        await interaction.response.send_message("✅ Auto-delete is enabled per-channel (use add_channels/remove_channel).", ephemeral=True)
        return

    if action == "add_channels":
        if filter_minutes is None:
            return await interaction.response.send_message("❌ `filter_minutes` is required with `add_channels`.", ephemeral=True)
        await autodelete_set_channel(guild_id, int(add_channels.id), int(filter_minutes) * 60, int(log_channel.id) if log_channel else None)
        await interaction.response.send_message(f"✅ Auto-delete enabled in {add_channels.mention} after {filter_minutes} minute(s).", ephemeral=True)
        return

    if action == "remove_channel":
        await autodelete_remove_channel(guild_id, int(remove_channel.id))
        await interaction.response.send_message(f"✅ Auto-delete disabled in {remove_channel.mention}.", ephemeral=True)
        return

    if action == "filter_ignore_words":
        await autodelete_add_ignore_phrase(guild_id, filter_ignore_words)
        await interaction.response.send_message("✅ Ignore phrase added.", ephemeral=True)
        return

    if action == "ignore_remove":
        await autodelete_remove_ignore_phrase(guild_id, ignore_remove)
        await interaction.response.send_message("✅ Ignore phrase removed.", ephemeral=True)
        return

    if action == "ignore_list":
        phrases = await autodelete_list_ignore_phrases(guild_id)
        txt = "\n".join(f"- {p}" for p in phrases) if phrases else "(none)"
        await interaction.response.send_message(txt, ephemeral=True)
        return

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="birthday", description="Configure birthday system")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def config_birthday_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    set_for: discord.Member | None = None,
    remove: discord.Member | None = None,
    set_role: discord.Role | None = None,
    set_channel: discord.TextChannel | None = None,
    set_custom_message: str | None = None,
    publish_list: discord.TextChannel | None = None,
    announce: bool | None = None,
    month: int | None = None,
    day: int | None = None,
    year: int | None = None,
):
    guild_id = require_guild(interaction)

    used = _count_set(
        enable=enable,
        set_for=set_for,
        remove=remove,
        set_role=set_role,
        set_channel=set_channel,
        set_custom_message=set_custom_message,
        publish_list=publish_list,
        announce=announce,
    )
    ok = await _require_one_action(
        interaction,
        used,
        "Example: `/config birthday enable:true` or `/config birthday set_role:@Role` or `/config birthday set_for:@User month:12 day:25`",
    )
    if not ok:
        return

    action = used[0]

    if action == "enable":
        if enable:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT birthday_role_id, birthday_channel_id FROM guild_settings WHERE guild_id=$1;",
                    int(guild_id),
                )
            missing = []
            if not row or not row.get("birthday_role_id"):
                missing.append("birthday role")
            if not row or not row.get("birthday_channel_id"):
                missing.append("birthday channel")
            if missing:
                return await interaction.response.send_message(
                    f"❌ Can't enable yet. Missing: {', '.join(missing)}. Set them first.",
                    ephemeral=True,
                )
        await birthday_set_enabled(guild_id, bool(enable))
        await interaction.response.send_message(f"✅ Birthday enabled set to `{bool(enable)}`", ephemeral=True)
        return

    if action == "set_for":
        if set_for is None or month is None or day is None:
            await interaction.response.send_message("❌ `set_for` requires `month` and `day` too.", ephemeral=True)
            return
        await birthday_set(guild_id, int(set_for.id), int(month), int(day), year, int(interaction.user.id))
        await update_birthday_list_message(bot, guild_id)
        await interaction.response.send_message(f"✅ Birthday saved for {set_for.mention}: {int(month):02d}/{int(day):02d}", ephemeral=True)
        return

    if action == "remove":
        await birthday_remove(guild_id, int(remove.id))
        await update_birthday_list_message(bot, guild_id)
        await interaction.response.send_message(f"✅ Removed birthday for {remove.mention}", ephemeral=True)
        return

    if action == "set_role":
        await birthday_set_role_channel_message(guild_id, int(set_role.id), None, None)
        await interaction.response.send_message(f"✅ Birthday role set to {set_role.mention}", ephemeral=True)
        return

    if action == "set_channel":
        await birthday_set_role_channel_message(guild_id, None, int(set_channel.id), None)
        await interaction.response.send_message(f"✅ Birthday channel set to {set_channel.mention}", ephemeral=True)
        return

    if action == "set_custom_message":
        await birthday_set_role_channel_message(guild_id, None, None, set_custom_message)
        await interaction.response.send_message("✅ Birthday message updated.", ephemeral=True)
        return

    if action == "publish_list":
        embed = discord.Embed(title="🎂 Birthdays", description="(initializing...)")
        msg = await publish_list.send(embed=embed)
        await birthday_set_list_message(guild_id, int(publish_list.id), int(msg.id))
        await update_birthday_list_message(bot, guild_id)
        await interaction.response.send_message(f"✅ Birthday list published in {publish_list.mention}", ephemeral=True)
        return

    await update_birthday_list_message(bot, guild_id)
    await interaction.response.send_message("✅ Birthday list updated (daily announce loop will handle announcements).", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="deadchat", description="Configure Dead Chat")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
@discord.app_commands.choices(require_active=BOOL_CHOICES, set_idle=DEADCHAT_IDLE_CHOICES, set_cooldown=DEADCHAT_COOLDOWN_CHOICES)
async def config_deadchat_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    add_channels: discord.TextChannel | None = None,
    remove_channel: discord.TextChannel | None = None,
    set_role: discord.Role | None = None,
    clear_role: bool | None = None,
    set_idle: discord.app_commands.Choice[int] | None = None,
    set_cooldown: discord.app_commands.Choice[int] | None = None,
    require_active: discord.app_commands.Choice[int] | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(
        enable=enable,
        add_channels=add_channels,
        remove_channel=remove_channel,
        set_role=set_role,
        clear_role=clear_role,
        set_idle=set_idle,
        set_cooldown=set_cooldown,
        require_active=require_active,
    )
    ok = await _require_one_action(interaction, used, "Example: `/config deadchat add_channels:#general` or `/config deadchat set_role:@DeadChat`")
    if not ok:
        return
    action = used[0]

    if action == "enable":
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE deadchat_channels SET enabled=$2 WHERE guild_id=$1;", guild_id, bool(enable))
        await interaction.response.send_message(f"✅ Dead Chat enabled set to `{bool(enable)}` (all configured channels).", ephemeral=True)
        return

    if action == "add_channels":
        await add_deadchat_channel(guild_id, int(add_channels.id), None)
        await interaction.response.send_message(f"✅ Dead Chat enabled in {add_channels.mention}", ephemeral=True)
        return

    if action == "remove_channel":
        await remove_deadchat_channel(guild_id, int(remove_channel.id))
        await interaction.response.send_message(f"✅ Dead Chat disabled in {remove_channel.mention}", ephemeral=True)
        return

    if action == "set_role":
        await set_deadchat_role(guild_id, int(set_role.id))
        await interaction.response.send_message(f"✅ Dead Chat role set to {set_role.mention}", ephemeral=True)
        return

    if action == "clear_role":
        await set_deadchat_role(guild_id, None)
        await interaction.response.send_message("✅ Dead Chat role cleared.", ephemeral=True)
        return

    if action == "set_idle":
        await set_deadchat_idle(guild_id, int(set_idle.value))
        await interaction.response.send_message(f"✅ Dead Chat idle set to {set_idle.value} minute(s)", ephemeral=True)
        return

    if action == "set_cooldown":
        await set_deadchat_cooldown(guild_id, int(set_cooldown.value))
        await interaction.response.send_message(f"✅ Dead Chat cooldown set to {set_cooldown.value} minute(s)", ephemeral=True)
        return

    await set_deadchat_requires_active(guild_id, bool(require_active.value))
    await interaction.response.send_message(f"✅ Require Active set to `{bool(require_active.value)}`", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="plague", description="Configure Plague Day")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def config_plague_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    set_role: discord.Role | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(enable=enable, set_role=set_role)
    ok = await _require_one_action(interaction, used, "Example: `/config plague set_role:@Plague` or `/config plague enable:true`")
    if not ok:
        return
    action = used[0]

    if action == "set_role":
        await plague_set_role(guild_id, int(set_role.id))
        await interaction.response.send_message(f"✅ Plague role set to {set_role.mention}", ephemeral=True)
        return

    if enable:
        s = await get_guild_settings(guild_id)
        missing = []
        if not s.get("plague_role_id"):
            missing.append("plague role")
        if not s.get("plague_scheduled_day"):
            missing.append("scheduled day")
        if missing:
            return await interaction.response.send_message("❌ Can't enable yet. Missing: " + ", ".join(missing), ephemeral=True)
    await plague_set_enabled(guild_id, bool(enable))
    await interaction.response.send_message(f"✅ Plague enabled set to `{bool(enable)}`", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@schedule_group.command(name="plague", description="Schedule a Plague Day (YYYY-MM-DD)")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def schedule_plague_cmd(interaction: discord.Interaction, day: str):
    guild_id = require_guild(interaction)
    try:
        d = parse_date_yyyy_mm_dd(day)
    except Exception:
        return await interaction.response.send_message("❌ Date must be YYYY-MM-DD", ephemeral=True)

    await plague_set_scheduled_day(guild_id, d)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM plague_daily_state WHERE guild_id = $1 AND day = $2;", guild_id, d)

    await interaction.response.send_message(f"✅ Plague Day scheduled for `{d.isoformat()}` (triggers after 12:00 UTC).", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="prize", description="Configure prizes")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
@discord.app_commands.choices(enable=BOOL_CHOICES)
async def config_prize_cmd(
    interaction: discord.Interaction,
    enable: discord.app_commands.Choice[int] | None = None,
    prize_drop_channel: discord.TextChannel | None = None,
    winner_announce_channel: discord.TextChannel | None = None,
    add_prize: str | None = None,
    remove_prize: str | None = None,
    description: str | None = None,
    image_url: str | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(
        enable=enable,
        prize_drop_channel=prize_drop_channel,
        winner_announce_channel=winner_announce_channel,
        add_prize=add_prize,
        remove_prize=remove_prize,
    )
    ok = await _require_one_action(
        interaction,
        used,
        "Examples: `/config prize enable:Enabled` or `/config prize prize_drop_channel:#drops` or `/config prize add_prize:\"Gift Card\"`",
    )
    if not ok:
        return
    action = used[0]

    if action == "enable":
        await prize_set_enabled(guild_id, bool(enable.value))
        await interaction.response.send_message(f"✅ Prizes enabled set to `{bool(enable.value)}`", ephemeral=True)
        return

    if action == "prize_drop_channel":
        await prize_set_drop_channel(guild_id, int(prize_drop_channel.id))
        await interaction.response.send_message(f"✅ Prize drop channel set to {prize_drop_channel.mention}", ephemeral=True)
        return

    if action == "winner_announce_channel":
        await prize_set_winner_announce_channel(guild_id, int(winner_announce_channel.id))
        await interaction.response.send_message(f"✅ Winner announce channel set to {winner_announce_channel.mention}", ephemeral=True)
        return

    if action == "add_prize":
        pid = await prize_add_definition(
            guild_id,
            add_prize.strip()[:200],
            (description.strip()[:1000] if description else None),
            (image_url.strip()[:500] if image_url else None),
        )
        await interaction.response.send_message(f"✅ Prize added: `{add_prize}`\nID: `{pid}`", ephemeral=True)
        return

    try:
        pid = uuid.UUID(remove_prize)
    except Exception:
        return await interaction.response.send_message("❌ `remove_prize` must be a prize UUID (copy from list).", ephemeral=True)
    await prize_delete_definition(guild_id, pid)
    await interaction.response.send_message("✅ Prize deleted.", ephemeral=True)

@schedule_group.command(name="prize", description="Prize schedule actions")
@discord.app_commands.autocomplete(select_prize=prize_autocomplete, cancel_prize=schedule_autocomplete)
@discord.app_commands.choices(not_before=PRIZE_TIME_CHOICES)
async def schedule_prize_cmd(
    interaction: discord.Interaction,
    set_month: int | None = None,
    set_day: int | None = None,
    select_prize: str | None = None,
    channel: discord.TextChannel | None = None,
    not_before: discord.app_commands.Choice[str] | None = None,
    schedule_list: bool | None = None,
    cancel_prize: str | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(
        set_month=set_month,
        set_day=set_day,
        select_prize=select_prize,
        schedule_list=schedule_list,
        cancel_prize=cancel_prize,
    )
    ok = await _require_one_action(
        interaction,
        used,
        "Examples: `/schedule prize schedule_list:true` • `/schedule prize cancel_prize:<select>` • `/schedule prize select_prize:<pick> set_month:1 set_day:15 channel:#general`",
    )
    if not ok:
        return
    action = used[0]

    if action == "schedule_list":
        settings = await get_guild_settings(guild_id)
        local_now = guild_now(settings["timezone"])
        scheds = await prize_schedule_list_upcoming(guild_id, local_now.date(), limit=25)
        if not scheds:
            return await interaction.response.send_message("No upcoming schedules.", ephemeral=True)
        lines = []
        for s in scheds:
            nbt = s["not_before_time"].strftime("%H:%M") if s["not_before_time"] else "Any"
            ch = interaction.guild.get_channel(int(s["channel_id"])) if interaction.guild else None
            chs = ch.mention if ch else f"`{s['channel_id']}`"
            lines.append(f"`{s['schedule_id']}` — {s['day'].isoformat()} @ {nbt} in {chs} (used: {s['used']})")
        return await interaction.response.send_message("\n".join(lines), ephemeral=True)

    if action == "cancel_prize":
        try:
            sid = uuid.UUID(cancel_prize)
        except Exception:
            return await interaction.response.send_message("❌ Invalid schedule id.", ephemeral=True)
        await prize_schedule_remove(guild_id, sid)
        return await interaction.response.send_message("✅ Schedule removed.", ephemeral=True)

    if action in ("set_month", "set_day"):
        return await interaction.response.send_message(
            "ℹ️ `set_month` and `set_day` are used together with `select_prize` in the same command.\n"
            "Example: `/schedule prize select_prize:<pick> set_month:1 set_day:15 channel:#general`",
            ephemeral=True,
        )

    if set_month is None or set_day is None:
        return await interaction.response.send_message("❌ For `select_prize`, you must also provide `set_month` and `set_day`.", ephemeral=True)
    if channel is None:
        return await interaction.response.send_message("❌ For `select_prize`, you must provide `channel`.", ephemeral=True)

    settings = await get_guild_settings(guild_id)
    tzname = settings.get("timezone") or "America/Los_Angeles"
    now_local = guild_now(tzname)
    try:
        d = date(int(now_local.year), int(set_month), int(set_day))
    except Exception:
        return await interaction.response.send_message("❌ Invalid month/day.", ephemeral=True)

    try:
        pid = uuid.UUID(select_prize)
    except Exception:
        return await interaction.response.send_message("❌ Invalid prize id.", ephemeral=True)

    t = parse_hh_mm(not_before.value) if (not_before and not_before.value) else None
    sid = await prize_schedule_add(guild_id, d, t, int(channel.id), pid)
    await interaction.response.send_message(f"✅ Scheduled.\nSchedule ID: `{sid}`", ephemeral=True)

@discord.app_commands.default_permissions(manage_roles=True)
@config_group.command(name="vc_link_roles", description="Configure voice role links")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_roles=True)
@discord.app_commands.choices(mode=VOICE_MODE_CHOICES)
async def config_vc_link_roles_cmd(
    interaction: discord.Interaction,
    link: bool | None = None,
    unlink: bool | None = None,
    list: bool | None = None,
    voice_channel: discord.VoiceChannel | None = None,
    role: discord.Role | None = None,
    mode: discord.app_commands.Choice[str] | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(link=link, unlink=unlink, list=list)
    ok = await _require_one_action(interaction, used, "Example: `/config vc_link_roles link:true voice_channel:... role:... mode:...`")
    if not ok:
        return
    action = used[0]

    if action == "list":
        links = await voice_role_list_links(guild_id)
        if not links:
            return await interaction.response.send_message("(none)", ephemeral=True)
        lines = []
        for l in links:
            vc = interaction.guild.get_channel(int(l["voice_channel_id"]))
            r = interaction.guild.get_role(int(l["role_id"]))
            lines.append(f"- {(vc.mention if vc else l['voice_channel_id'])} → {(r.mention if r else l['role_id'])} (`{l['mode']}`)")
        return await interaction.response.send_message("\n".join(lines), ephemeral=True)

    if voice_channel is None:
        return await interaction.response.send_message("❌ `voice_channel` is required.", ephemeral=True)

    if action == "unlink":
        await voice_role_remove_link(guild_id, int(voice_channel.id))
        await interaction.response.send_message(f"✅ Unlinked {voice_channel.mention}", ephemeral=True)
        return

    if role is None or mode is None:
        return await interaction.response.send_message("❌ `role` and `mode` are required for link.", ephemeral=True)
    await voice_role_set_link(guild_id, int(voice_channel.id), int(role.id), mode.value)
    await interaction.response.send_message(f"✅ Linked {voice_channel.mention} ↔ {role.mention} ({mode.value})", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@config_group.command(name="join_roles", description="Configure join roles")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def config_join_roles_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    set_member_role: discord.Role | None = None,
    set_timer: int | None = None,
    enable_bot: bool | None = None,
    disable_bot: bool | None = None,
    set_bot_role: discord.Role | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(enable=enable, set_member_role=set_member_role, set_timer=set_timer, enable_bot=enable_bot, disable_bot=disable_bot, set_bot_role=set_bot_role)
    ok = await _require_one_action(interaction, used, "Example: `/config join_roles set_member_role:@Member set_timer:10`")
    if not ok:
        return
    action = used[0]

    if action == "enable":
        await interaction.response.send_message("✅ Join roles are active when roles are set.", ephemeral=True)
        return

    if action == "set_member_role":
        delay = int(set_timer or 0)
        await welcome_set(guild_id, None, None, int(set_member_role.id), delay, None, None)
        await interaction.response.send_message("✅ Member join role updated.", ephemeral=True)
        return

    if action == "set_timer":
        await interaction.response.send_message("❌ `set_timer` must be used with `set_member_role`.", ephemeral=True)
        return

    if action == "enable_bot":
        await interaction.response.send_message("✅ Bot join roles are active when `set_bot_role` is set.", ephemeral=True)
        return

    if action == "disable_bot":
        await interaction.response.send_message("✅ To disable bot role, clear it in DB (not implemented as a single action here).", ephemeral=True)
        return

    await welcome_set(guild_id, None, None, None, None, int(set_bot_role.id), None)
    await interaction.response.send_message("✅ Bot join role updated.", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@messages_group.command(name="welcome", description="Configure welcome messages")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def messages_welcome_cmd(
    interaction: discord.Interaction,
    enable: bool | None = None,
    set_custom_welcome: str | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(enable=enable, set_custom_welcome=set_custom_welcome)
    ok = await _require_one_action(interaction, used, "Example: `/messages welcome enable:true` or `/messages welcome set_custom_welcome:\"Welcome {user}!\"`")
    if not ok:
        return
    if used[0] == "enable":
        await welcome_set_enabled(guild_id, bool(enable))
        await interaction.response.send_message(f"✅ Welcome enabled set to `{bool(enable)}`", ephemeral=True)
        return
    await welcome_set_message(guild_id, set_custom_welcome)
    await interaction.response.send_message("✅ Welcome message updated.", ephemeral=True)

@discord.app_commands.default_permissions(manage_channels=True)
@messages_group.command(name="sticky", description="Sticky message")
@discord.app_commands.checks.cooldown(rate=1, per=5.0)
@discord.app_commands.checks.has_permissions(manage_channels=True)
async def messages_sticky_cmd(
    interaction: discord.Interaction,
    set: bool | None = None,
    clear: bool | None = None,
    channel: discord.TextChannel | None = None,
    content: str | None = None,
):
    guild_id = require_guild(interaction)
    used = _count_set(set=set, clear=clear)
    ok = await _require_one_action(interaction, used, "Example: `/messages sticky set:true channel:#general content:\"Hello\"` or `/messages sticky clear:true channel:#general`")
    if not ok:
        return
    if channel is None:
        return await interaction.response.send_message("❌ `channel` is required.", ephemeral=True)

    if used[0] == "set":
        if not content:
            return await interaction.response.send_message("❌ `content` is required when setting.", ephemeral=True)
        await sticky_set(guild_id, int(channel.id), content)
        await interaction.response.send_message(f"✅ Sticky set for {channel.mention}", ephemeral=True)
        return

    await sticky_clear(guild_id, int(channel.id))
    await interaction.response.send_message(f"✅ Sticky cleared for {channel.mention}", ephemeral=True)



############### MOVIE NIGHT (PUBLIC + DEV) ###############
# NOTE: This section is intentionally additive. It does not modify existing features or templates.

# Separate templates for movie features so we do NOT touch your existing MSG dict.
MOVIE_MSG = {
    # Public/manual pool
    "pool_title": "🎬 Movie Night Pool",
    "pool_empty": "No picks yet. Use /pick to add one!",
    "pick_added": "✅ Added to the pool: **{title}**",
    "pick_removed": "✅ Removed from the pool: **{title}**",
    "pick_not_found": "❌ I couldn't find that pick in *your* picks.",
    "pick_duplicate": "❌ That title is already in the pool.",
    "pick_limit": "❌ You’ve reached the pick limit (**{limit}**). Use /replace_pick or /unpick first.",
    "winner_announce": "Pool Winner: **{winner_title}**\n{mention}'s pick! {rollover_text}",
    "rollover_text": "All other picks roll over to the next pool.",

    # Dev/library flow (your server only)
    "dev_only": "❌ This command is only available in the developer server.",
    "library_reload_ok": "✅ Library reloaded from Sheets export.",
    "library_sync_ok": "✅ Library channel synced.",
    "pool_message_ok": "✅ Pool display message is set.",
    "library_not_configured": "❌ Movie library is not configured yet.",
}


def _movies_default_csv_url() -> str | None:
    """Build a CSV export URL for the Movies tab using env vars.

    Uses QOTD_SHEET_ID (preferred) or GOOGLE_SHEET_ID, and MOVIES_TAB (default 'Movies').
    """
    sheet_id = os.getenv("QOTD_SHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        return None
    tab = os.getenv("MOVIES_TAB", "Movies")
    # gviz csv export
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={quote_plus(tab)}"

async def ensure_movie_tables():
    """Create movie tables if they don't exist yet. Called lazily from movie commands only."""
    global db_pool
    if db_pool is None:
        await init_db()
    async with db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS movie_settings (
            guild_id BIGINT PRIMARY KEY,
            mode TEXT NOT NULL DEFAULT 'public_manual',
            per_user_limit INT NOT NULL DEFAULT 3,
            pool_display_channel_id BIGINT,
            pool_display_message_id BIGINT,
            announce_channel_id_1 BIGINT,
            announce_channel_id_2 BIGINT,
            library_channel_id BIGINT,
            library_source_url TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS movie_pool_picks (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            title TEXT NOT NULL,
            added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, lower(title))
        );
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_movie_pool_picks_guild_user
        ON movie_pool_picks (guild_id, user_id);
        """)
        # Dev/library tables (only used in DEV guild)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS movie_library_items (
            guild_id BIGINT NOT NULL,
            sheet_key TEXT NOT NULL,
            title TEXT NOT NULL,
            poster_url TEXT,
            trailer_url TEXT,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, sheet_key)
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS movie_library_messages (
            guild_id BIGINT NOT NULL,
            sheet_key TEXT NOT NULL,
            channel_id BIGINT NOT NULL,
            message_id BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, sheet_key)
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS movie_night_history (
            guild_id BIGINT NOT NULL,
            title TEXT NOT NULL,
            picked_by BIGINT,
            picked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

async def movie_get_settings(guild_id: int) -> dict:
    await ensure_movie_tables()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM movie_settings WHERE guild_id=$1", guild_id)
        if not row:
            await conn.execute("INSERT INTO movie_settings (guild_id) VALUES ($1)", guild_id)
            row = await conn.fetchrow("SELECT * FROM movie_settings WHERE guild_id=$1", guild_id)
        return dict(row)

async def movie_set_settings(guild_id: int, **kwargs) -> None:
    await ensure_movie_tables()
    if not kwargs:
        return
    cols = []
    vals = [guild_id]
    i = 2
    for k, v in kwargs.items():
        cols.append(f"{k}=${i}")
        vals.append(v)
        i += 1
    sql = f"UPDATE movie_settings SET {', '.join(cols)}, updated_at=NOW() WHERE guild_id=$1"
    async with db_pool.acquire() as conn:
        await conn.execute(sql, *vals)

def _norm_title(t: str) -> str:
    return " ".join(t.strip().split())

async def movie_pool_add(guild_id: int, user_id: int, title: str) -> tuple[bool, str]:
    """Returns (ok, error_code) where error_code is one of: duplicate, limit."""
    await ensure_movie_tables()
    title = _norm_title(title)
    settings = await movie_get_settings(guild_id)
    limit = int(settings.get("per_user_limit") or 3)
    async with db_pool.acquire() as conn:
        # duplicate check (pool-wide)
        existing = await conn.fetchrow(
            "SELECT 1 FROM movie_pool_picks WHERE guild_id=$1 AND lower(title)=lower($2)",
            guild_id, title
        )
        if existing:
            return False, "duplicate"
        # per-user limit
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM movie_pool_picks WHERE guild_id=$1 AND user_id=$2",
            guild_id, user_id
        )
        if cnt is not None and int(cnt) >= limit:
            return False, "limit"
        await conn.execute(
            "INSERT INTO movie_pool_picks (guild_id, user_id, title) VALUES ($1, $2, $3)",
            guild_id, user_id, title
        )
    return True, ""

async def movie_pool_remove(guild_id: int, user_id: int, title: str) -> bool:
    await ensure_movie_tables()
    title = _norm_title(title)
    async with db_pool.acquire() as conn:
        res = await conn.execute(
            "DELETE FROM movie_pool_picks WHERE guild_id=$1 AND user_id=$2 AND lower(title)=lower($3)",
            guild_id, user_id, title
        )
    # asyncpg returns "DELETE X"
    try:
        n = int(res.split()[-1])
    except Exception:
        n = 0
    return n > 0

async def movie_pool_list(guild_id: int) -> list[dict]:
    await ensure_movie_tables()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT guild_id, user_id, title, added_at FROM movie_pool_picks WHERE guild_id=$1",
            guild_id
        )
    return [dict(r) for r in rows]

async def movie_pool_render_embed(guild: discord.Guild) -> discord.Embed:
    settings = await movie_get_settings(int(guild.id))
    picks = await movie_pool_list(int(guild.id))
    embed = discord.Embed(title=MOVIE_MSG["pool_title"])
    if not picks:
        embed.description = MOVIE_MSG["pool_empty"]
        return embed

    # Group by user_id
    by_user: dict[int, list[str]] = {}
    for r in picks:
        by_user.setdefault(int(r["user_id"]), []).append(r["title"])

    # Resolve names + sort by name
    groups = []
    for uid, titles in by_user.items():
        member = guild.get_member(uid)
        name = member.display_name if member else str(uid)
        groups.append((name.lower(), name, uid, titles))
    groups.sort(key=lambda x: x[0])

    lines = []
    for _, name, uid, titles in groups:
        member = guild.get_member(uid)
        header = member.mention if member else name
        lines.append(f"**{header}**")
        for t in titles:
            lines.append(f"• {t}")
        lines.append("")  # spacing
    embed.description = "\n".join(lines).strip()
    embed.set_footer(text=f"Total picks: {len(picks)} • Limit per user: {int(settings.get('per_user_limit') or 3)}")
    return embed

async def movie_pool_update_display(guild: discord.Guild) -> None:
    """If a persistent pool display message is configured, update it."""
    settings = await movie_get_settings(int(guild.id))
    ch_id = settings.get("pool_display_channel_id")
    msg_id = settings.get("pool_display_message_id")
    if not ch_id or not msg_id:
        return
    channel = guild.get_channel(int(ch_id))
    if channel is None:
        return
    try:
        msg = await channel.fetch_message(int(msg_id))
    except Exception:
        return
    embed = await movie_pool_render_embed(guild)
    try:
        await msg.edit(embed=embed)
    except Exception:
        pass

async def movie_pick_random(guild: discord.Guild) -> tuple[str | None, int | None]:
    """Returns (title, user_id) or (None, None) if empty."""
    import random
    picks = await movie_pool_list(int(guild.id))
    if not picks:
        return None, None
    choice = random.choice(picks)
    title = choice["title"]
    user_id = int(choice["user_id"])
    # Remove winner (rollover for the rest)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM movie_pool_picks WHERE guild_id=$1 AND lower(title)=lower($2)",
            int(guild.id), title
        )
        await conn.execute(
            "INSERT INTO movie_night_history (guild_id, title, picked_by) VALUES ($1, $2, $3)",
            int(guild.id), title, user_id
        )
    await movie_pool_update_display(guild)
    return title, user_id

# -------- Public commands (manual pool) --------



@bot.tree.command(name="browse", description="Browse the synced movie library (dev server only)")
async def browse_cmd(interaction: discord.Interaction):
    await open_movie_browser(interaction)

@bot.tree.command(name="pick", description="Add a movie to the Movie Night pool")
@discord.app_commands.describe(title="Movie title (optional if this server has a synced movie database)")
async def pick_cmd(interaction: discord.Interaction, title: str | None = None):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    guild_id = int(interaction.guild.id)
    user_id = int(interaction.user.id)

    # If this server has a synced movie database (dev_library), and no title was provided,
    # open the browser UI instead of requiring manual entry.
    settings = await movie_get_settings(guild_id)
    if title is None and settings.get("mode") == "dev_library":
        await open_movie_browser(interaction)
        return

    if not title:
        await interaction.response.send_message("❌ Please provide a movie title.", ephemeral=True)
        return

    title = _norm_title(title)

    ok, err = await movie_pool_add(guild_id, user_id, title)
    if not ok:
        settings = await movie_get_settings(guild_id)
        if err == "duplicate":
            await interaction.response.send_message(MOVIE_MSG["pick_duplicate"], ephemeral=True)
            return
        if err == "limit":
            limit = int(settings.get("per_user_limit") or 3)
            await interaction.response.send_message(
                MOVIE_MSG["pick_limit"].replace("{limit}", str(limit)),
                ephemeral=True
            )
            return
        await interaction.response.send_message("❌ Could not add that pick.", ephemeral=True)
        return

    await movie_pool_update_display(interaction.guild)
    await interaction.response.send_message(
        MOVIE_MSG["pick_added"].replace("{title}", title),
        ephemeral=True
    )

@bot.tree.command(name="unpick", description="Remove one of your picks from the Movie Night pool")
@discord.app_commands.describe(title="Movie title to remove")
async def unpick_cmd(interaction: discord.Interaction, title: str):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    guild_id = int(interaction.guild.id)
    user_id = int(interaction.user.id)
    title = _norm_title(title)

    removed = await movie_pool_remove(guild_id, user_id, title)
    if not removed:
        await interaction.response.send_message(MOVIE_MSG["pick_not_found"], ephemeral=True)
        return

    await movie_pool_update_display(interaction.guild)
    await interaction.response.send_message(
        MOVIE_MSG["pick_removed"].replace("{title}", title),
        ephemeral=True
    )

@bot.tree.command(name="replace_pick", description="Replace one of your picks with a new title")
@discord.app_commands.describe(old_title="Your existing pick to replace", new_title="The new title to add")
async def replace_pick_cmd(interaction: discord.Interaction, old_title: str, new_title: str):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    guild_id = int(interaction.guild.id)
    user_id = int(interaction.user.id)
    old_title = _norm_title(old_title)
    new_title = _norm_title(new_title)

    removed = await movie_pool_remove(guild_id, user_id, old_title)
    if not removed:
        await interaction.response.send_message(MOVIE_MSG["pick_not_found"], ephemeral=True)
        return

    ok, err = await movie_pool_add(guild_id, user_id, new_title)
    if not ok:
        # put old back if new fails
        await movie_pool_add(guild_id, user_id, old_title)
        if err == "duplicate":
            await interaction.response.send_message(MOVIE_MSG["pick_duplicate"], ephemeral=True)
            return
        settings = await movie_get_settings(guild_id)
        if err == "limit":
            limit = int(settings.get("per_user_limit") or 3)
            await interaction.response.send_message(
                MOVIE_MSG["pick_limit"].replace("{limit}", str(limit)),
                ephemeral=True
            )
            return
        await interaction.response.send_message("❌ Could not replace that pick.", ephemeral=True)
        return

    await movie_pool_update_display(interaction.guild)
    await interaction.response.send_message(
        f"✅ Replaced **{old_title}** → **{new_title}**",
        ephemeral=True
    )

@bot.tree.command(name="pool", description="Show the current Movie Night pool")
async def pool_cmd(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    embed = await movie_pool_render_embed(interaction.guild)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="random", description="Pick a random winner from the Movie Night pool")
async def random_cmd(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    title, picked_by = await movie_pick_random(interaction.guild)
    if not title:
        await interaction.response.send_message(MOVIE_MSG["pool_empty"], ephemeral=True)
        return
    member = interaction.guild.get_member(int(picked_by)) if picked_by else None
    mention = member.mention if member else "Someone"
    msg = MOVIE_MSG["winner_announce"] \
        .replace("{winner_title}", title) \
        .replace("{mention}", mention) \
        .replace("{rollover_text}", MOVIE_MSG["rollover_text"])
    await interaction.response.send_message(msg)

# -------- Dev-only library features (your server only) --------

movies_group = discord.app_commands.Group(name="movies", description="Movie library tools (dev server only)")

def _is_dev_guild(interaction: discord.Interaction) -> bool:
    return interaction.guild is not None and int(interaction.guild.id) in DEV_GUILD_IDS

async def _require_dev_guild(interaction: discord.Interaction) -> bool:
    if not _is_dev_guild(interaction):
        await interaction.response.send_message(MOVIE_MSG["dev_only"], ephemeral=True)
        return False
    return True

async def _fetch_csv_rows(url: str) -> list[dict]:
    # Expected headers: title, poster_url, trailer_url (extra columns ignored)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            resp.raise_for_status()
            text = await resp.text()
    import csv, io
    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    rows = []
    for i, row in enumerate(reader, start=1):
        title = _norm_title(row.get("title", "") or "")
        if not title:
            continue
        rows.append({
            "sheet_key": str(i),
            "title": title,
            "poster_url": (row.get("poster_url") or row.get("poster") or "").strip() or None,
            "trailer_url": (row.get("trailer_url") or row.get("trailer") or "").strip() or None,
        })
    return rows

class MovieAddToPoolView(discord.ui.View):
    def __init__(self, guild_id: int, title: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.title = title

    @discord.ui.button(label="Add to Pool", style=discord.ButtonStyle.success)
    async def add_to_pool(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
            return
        if int(interaction.guild.id) != self.guild_id:
            await interaction.response.send_message("❌ Wrong server.", ephemeral=True)
            return

        ok, err = await movie_pool_add(int(interaction.guild.id), int(interaction.user.id), self.title)
        if not ok:
            settings = await movie_get_settings(int(interaction.guild.id))
            if err == "duplicate":
                await interaction.response.send_message(MOVIE_MSG["pick_duplicate"], ephemeral=True)
                return
            if err == "limit":
                limit = int(settings.get("per_user_limit") or 3)
                await interaction.response.send_message(
                    MOVIE_MSG["pick_limit"].replace("{limit}", str(limit)),
                    ephemeral=True
                )
                return
            await interaction.response.send_message("❌ Could not add that pick.", ephemeral=True)
            return

        await movie_pool_update_display(interaction.guild)
        await interaction.response.send_message(
            MOVIE_MSG["pick_added"].replace("{title}", self.title),
            ephemeral=True
        )

@discord.app_commands.default_permissions(manage_guild=True)
@movies_group.command(name="set_mode", description="Set movies mode for this server (public_manual or dev_library)")
@discord.app_commands.describe(mode="public_manual or dev_library", per_user_limit="Max picks per user")
async def movies_set_mode_cmd(interaction: discord.Interaction, mode: str, per_user_limit: int | None = None):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    if mode not in ("public_manual", "dev_library"):
        await interaction.response.send_message("❌ Mode must be public_manual or dev_library.", ephemeral=True)
        return
    await movie_set_settings(int(interaction.guild.id), mode=mode, per_user_limit=int(per_user_limit) if per_user_limit else None)
    await interaction.response.send_message("✅ Updated movie settings.", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@movies_group.command(name="set_pool_display", description="Set/update the persistent pool display message in this channel")
async def movies_set_pool_display_cmd(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    embed = await movie_pool_render_embed(interaction.guild)
    try:
        msg = await interaction.channel.send(embed=embed)
    except Exception:
        await interaction.response.send_message("❌ Could not post pool message here.", ephemeral=True)
        return
    await movie_set_settings(
        int(interaction.guild.id),
        pool_display_channel_id=int(interaction.channel.id),
        pool_display_message_id=int(msg.id),
    )
    await interaction.response.send_message(MOVIE_MSG["pool_message_ok"], ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@movies_group.command(name="set_library_source", description="Set the library source URL (CSV export) for the dev library")
@discord.app_commands.describe(url="A CSV URL with headers: title,poster_url,trailer_url")
async def movies_set_library_source_cmd(interaction: discord.Interaction, url: str):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    if not await _require_dev_guild(interaction):
        return
    await movie_set_settings(int(interaction.guild.id), library_source_url=url)
    await interaction.response.send_message("✅ Library source URL saved.", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@movies_group.command(name="library_reload", description="Reload the dev movie library from the configured CSV source")
async def movies_library_reload_cmd(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    if not await _require_dev_guild(interaction):
        return
    settings = await movie_get_settings(int(interaction.guild.id))
    src = settings.get("library_source_url") or _movies_default_csv_url()
    if not src:
        await interaction.response.send_message(
            "❌ Movie library source is not configured. Set `/movies set_library_source` or set `QOTD_SHEET_ID` and `MOVIES_TAB` in Railway.",
            ephemeral=True,
        )
        return
    rows = await _fetch_csv_rows(src)
    async with db_pool.acquire() as conn:
        # mark all inactive, then upsert actives
        await conn.execute("UPDATE movie_library_items SET active=FALSE WHERE guild_id=$1", int(interaction.guild.id))
        for r in rows:
            await conn.execute("""
                INSERT INTO movie_library_items (guild_id, sheet_key, title, poster_url, trailer_url, active)
                VALUES ($1, $2, $3, $4, $5, TRUE)
                ON CONFLICT (guild_id, sheet_key)
                DO UPDATE SET title=EXCLUDED.title, poster_url=EXCLUDED.poster_url, trailer_url=EXCLUDED.trailer_url,
                              active=TRUE, updated_at=NOW()
            """, int(interaction.guild.id), r["sheet_key"], r["title"], r["poster_url"], r["trailer_url"])
    await interaction.response.send_message(MOVIE_MSG["library_reload_ok"], ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@movies_group.command(name="set_library_channel", description="Set the library channel for dev library sync")
async def movies_set_library_channel_cmd(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    if not await _require_dev_guild(interaction):
        return
    await movie_set_settings(int(interaction.guild.id), library_channel_id=int(interaction.channel.id))
    await interaction.response.send_message("✅ Library channel set to this channel.", ephemeral=True)

@discord.app_commands.default_permissions(manage_guild=True)
@movies_group.command(name="library_sync", description="Sync one message per movie into the configured library channel (dev only)")
async def movies_library_sync_cmd(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    if not await _require_dev_guild(interaction):
        return
    settings = await movie_get_settings(int(interaction.guild.id))
    ch_id = settings.get("library_channel_id")
    if not ch_id:
        await interaction.response.send_message("❌ Set a library channel first (use /movies set_library_channel in that channel).", ephemeral=True)
        return
    channel = interaction.guild.get_channel(int(ch_id))
    if channel is None:
        await interaction.response.send_message("❌ Library channel not found.", ephemeral=True)
        return

    async with db_pool.acquire() as conn:
        items = await conn.fetch("""
            SELECT sheet_key, title, poster_url, trailer_url
            FROM movie_library_items
            WHERE guild_id=$1 AND active=TRUE
            ORDER BY title ASC
        """, int(interaction.guild.id))
        msg_map = {r["sheet_key"]: r for r in await conn.fetch("""
            SELECT sheet_key, channel_id, message_id FROM movie_library_messages WHERE guild_id=$1
        """, int(interaction.guild.id))}

    for item in items:
        sheet_key = item["sheet_key"]
        title = item["title"]
        poster_url = item["poster_url"]
        trailer_url = item["trailer_url"]

        embed = discord.Embed(title=title)
        if trailer_url:
            embed.description = f"[Trailer]({trailer_url})"
        if poster_url:
            try:
                embed.set_image(url=poster_url)
            except Exception:
                pass
        view = MovieAddToPoolView(guild_id=int(interaction.guild.id), title=title)

        if sheet_key in msg_map:
            try:
                old_msg = await channel.fetch_message(int(msg_map[sheet_key]["message_id"]))
                await old_msg.edit(embed=embed, view=view)
            except Exception:
                # If edit fails, post a new one and update mapping
                try:
                    new_msg = await channel.send(embed=embed, view=view)
                except Exception:
                    continue
                async with db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO movie_library_messages (guild_id, sheet_key, channel_id, message_id)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (guild_id, sheet_key)
                        DO UPDATE SET channel_id=EXCLUDED.channel_id, message_id=EXCLUDED.message_id, updated_at=NOW()
                    """, int(interaction.guild.id), sheet_key, int(channel.id), int(new_msg.id))
        else:
            try:
                new_msg = await channel.send(embed=embed, view=view)
            except Exception:
                continue
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO movie_library_messages (guild_id, sheet_key, channel_id, message_id)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (guild_id, sheet_key)
                    DO UPDATE SET channel_id=EXCLUDED.channel_id, message_id=EXCLUDED.message_id, updated_at=NOW()
                """, int(interaction.guild.id), sheet_key, int(channel.id), int(new_msg.id))

    await interaction.response.send_message(MOVIE_MSG["library_sync_ok"], ephemeral=True)
bot.tree.add_command(config_group)
bot.tree.add_command(messages_group)
bot.tree.add_command(schedule_group)
bot.tree.add_command(movies_group)

############### ON_READY & BOT START ###############
@bot.event
async def on_ready():
    global active_cleanup_task, plague_cleanup_task, deadchat_cleanup_task, birthday_task, qotd_task
    await bot.tree.sync()
    if active_cleanup_task is None:
        active_cleanup_task = asyncio.create_task(active_cleanup_loop(bot))
    if plague_cleanup_task is None:
        plague_cleanup_task = asyncio.create_task(plague_cleanup_loop(bot))
    if deadchat_cleanup_task is None:
        deadchat_cleanup_task = asyncio.create_task(deadchat_cleanup_loop())
    if birthday_task is None:
        birthday_task = asyncio.create_task(birthday_daily_loop(bot))
    if qotd_task is None:
        qotd_task = asyncio.create_task(qotd_daily_loop(bot))
    print(f"✅ Logged in as {bot.user} ({bot.user.id})")

async def _safe_reply(interaction: discord.Interaction, content: str):
    if interaction.response.is_done():
        await interaction.followup.send(content, ephemeral=True)
    else:
        await interaction.response.send_message(content, ephemeral=True)

@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: discord.app_commands.AppCommandError,
):
    if isinstance(error, discord.app_commands.errors.CommandOnCooldown):
        await _safe_reply(
            interaction,
            f"⏳ This command is on cooldown. Try again in {error.retry_after:.1f}s.",
        )
        return

    if isinstance(error, discord.app_commands.errors.MissingPermissions):
        await _safe_reply(
            interaction,
            "❌ You don’t have permission to use this command.",
        )
        return

    if isinstance(error, discord.app_commands.errors.BotMissingPermissions):
        await _safe_reply(
            interaction,
            "⚠️ I’m missing required permissions to do that.",
        )
        return

    await _safe_reply(
        interaction,
        "⚠️ An unexpected error occurred. The issue has been logged.",
    )

    import traceback
    traceback.print_exception(type(error), error, error.__traceback__)

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

# -------- Library browsing UI (dev_library mode) --------

async def movie_library_list(guild_id: int):
    await ensure_movie_tables()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT sheet_key, title
            FROM movie_library_items
            WHERE guild_id=$1 AND active=TRUE
            ORDER BY title ASC
            """,
            int(guild_id),
        )
    return [(str(r["sheet_key"]), str(r["title"])) for r in rows]


class MovieBrowserView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int, items: list[tuple[str, str]], page: int = 0):
        super().__init__(timeout=600)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.items = items
        self.page = int(page)

        # Build initial select
        self._rebuild_select()

    def _page_count(self) -> int:
        if not self.items:
            return 1
        return (len(self.items) + 24) // 25

    def _slice(self) -> list[tuple[str, str]]:
        start = self.page * 25
        end = start + 25
        return self.items[start:end]

    def _rebuild_select(self):
        # Remove existing selects
        for child in list(self.children):
            if isinstance(child, discord.ui.Select):
                self.remove_item(child)

        page_items = self._slice()
        options = []
        for sheet_key, title in page_items:
            label = title[:100]
            options.append(discord.SelectOption(label=label, value=sheet_key))

        select = discord.ui.Select(placeholder="✅ Select One", min_values=1, max_values=1, options=options)

        async def _select_callback(interaction: discord.Interaction):
            if interaction.guild is None or int(interaction.guild.id) != self.guild_id:
                await interaction.response.send_message("❌ Wrong server.", ephemeral=True)
                return
            if int(interaction.user.id) != self.user_id:
                await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
                return

            sheet_key = select.values[0]
            title_map = {k: t for k, t in self._slice()}
            # If the selected key isn't in the current slice (rare), fall back to global map
            if sheet_key not in title_map:
                title_map = {k: t for k, t in self.items}
            picked_title = title_map.get(sheet_key)
            if not picked_title:
                await interaction.response.send_message("❌ Could not find that title.", ephemeral=True)
                return

            ok, err = await movie_pool_add(self.guild_id, int(interaction.user.id), picked_title)
            if not ok:
                settings = await movie_get_settings(self.guild_id)
                if err == "duplicate":
                    await interaction.response.send_message(MOVIE_MSG["pick_duplicate"], ephemeral=True)
                    return
                if err == "limit":
                    limit = int(settings.get("per_user_limit") or 3)
                    await interaction.response.send_message(
                        MOVIE_MSG["pick_limit"].replace("{limit}", str(limit)),
                        ephemeral=True,
                    )
                    return
                await interaction.response.send_message("❌ Could not add that pick.", ephemeral=True)
                return

            await movie_pool_update_display(interaction.guild)
            await interaction.response.send_message(
                MOVIE_MSG["pick_added"].replace("{title}", picked_title),
                ephemeral=True,
            )

        select.callback = _select_callback
        self.add_item(select)

    async def _edit(self, interaction: discord.Interaction):
        # rebuild select options for this page and edit message
        self._rebuild_select()
        embed = build_movie_browser_embed(self.items, self.page)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if int(interaction.user.id) != self.user_id:
            await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
            return
        self.page = max(0, self.page - 1)
        await self._edit(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if int(interaction.user.id) != self.user_id:
            await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
            return
        self.page = min(self._page_count() - 1, self.page + 1)
        await self._edit(interaction)


def build_movie_browser_embed(items: list[tuple[str, str]], page: int) -> discord.Embed:
    total = len(items)
    pages = (total + 24) // 25 if total else 1
    page = max(0, min(int(page), pages - 1))

    start = page * 25
    end = min(start + 25, total)
    lines = []
    for i, (_, title) in enumerate(items[start:end], start=start + 1):
        lines.append(f"{i}. {title}")

    desc = "\n".join(lines) if lines else "No movies found."
    embed = discord.Embed(
        title="Movies",
        description=desc
    )
    embed.set_footer(text=f"Page {page+1}/{pages} ({total} total)")
    return embed


async def open_movie_browser(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
        return
    guild_id = int(interaction.guild.id)
    if DEV_GUILD_IDS and guild_id not in DEV_GUILD_IDS:
        await interaction.response.send_message("❌ No movie database is synced for this server.", ephemeral=True)
        return
    settings = await movie_get_settings(guild_id)
    if settings.get("mode") != "dev_library":
        await interaction.response.send_message("❌ No movie database is synced for this server.", ephemeral=True)
        return

    items = await movie_library_list(guild_id)
    if not items:
        await interaction.response.send_message("❌ No movie database is synced for this server.", ephemeral=True)
        return

    view = MovieBrowserView(guild_id=guild_id, user_id=int(interaction.user.id), items=items, page=0)
    embed = build_movie_browser_embed(items, 0)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


