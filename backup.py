############### IMPORTS ###############
import os
import asyncio
import uuid
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
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
                   plague_role_id, plague_duration_hours,
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
        announce_text = f"💀 {message.author.mention} revived the chat and stole **Dead Chat**!"
        sent = None
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
    role_id = settings["plague_role_id"]
    if not role_id:
        return
    local_now = guild_now(settings["timezone"])
    today = local_now.date()
    if not await plague_is_day(int(guild.id), today):
        return
    if await plague_daily_already_triggered(int(guild.id), today):
        return
    member = guild.get_member(winner_user_id)
    if member is None:
        return
    role = guild.get_role(int(role_id))
    if role is None:
        return
    expires_at = now_utc() + timedelta(hours=int(settings["plague_duration_hours"]))
    try:
        await member.add_roles(role, reason="Plague: first Dead Chat winner on plague day")
    except Exception:
        return
    await plague_add_infection(int(guild.id), winner_user_id, expires_at, source_channel_id)
    await plague_mark_triggered(int(guild.id), today, winner_user_id)

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
    content = f"🎁 Prize Drop! First to claim it wins."
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
            await interaction.channel.send(f"🏆 {interaction.user.mention} claimed **{title}**!")
        except Exception:
            pass
        try:
            await interaction.user.send(f"✅ You claimed **{title}** in **{interaction.guild.name}**.")
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
    await bot.process_commands(message)

############### COMMAND GROUPS ###############
@bot.tree.command(name="test_all", description="Run a full system test for this server")
async def test_all(interaction: discord.Interaction):
    title, lines = await run_test_all(interaction)
    embed = discord.Embed(title=title, description="\n".join(lines))
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="ping", description="Bot heartbeat")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong ✅")

active_group = discord.app_commands.Group(name="active", description="Activity Tracking settings")

@active_group.command(name="set_role", description="Set the Active Member role")
async def active_set_role(interaction: discord.Interaction, role: discord.Role):
    guild_id = require_guild(interaction)
    await set_active_role(guild_id, int(role.id))
    await interaction.response.send_message(f"✅ Active role set to {role.mention}", ephemeral=True)

@active_group.command(name="clear_role", description="Clear the Active Member role")
async def active_clear_role(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    await set_active_role(guild_id, None)
    await interaction.response.send_message("✅ Active role cleared", ephemeral=True)

@active_group.command(name="set_threshold", description="Set inactivity threshold")
@discord.app_commands.choices(minutes=ACTIVE_THRESHOLD_CHOICES)
async def active_set_threshold(interaction: discord.Interaction, minutes: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await set_active_threshold(guild_id, minutes.value)
    await interaction.response.send_message(f"✅ Active threshold set to {minutes.value} minute(s)", ephemeral=True)

@active_group.command(name="set_mode", description="Set activity mode")
@discord.app_commands.choices(mode=ACTIVE_MODE_CHOICES)
async def active_set_mode(interaction: discord.Interaction, mode: discord.app_commands.Choice[str]):
    guild_id = require_guild(interaction)
    await set_active_mode(guild_id, mode.value)
    await interaction.response.send_message(f"✅ Activity mode set to `{mode.value}`", ephemeral=True)

@active_group.command(name="add_channel", description="Add a channel to count activity (channels mode)")
async def active_add_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = require_guild(interaction)
    await add_activity_channel(guild_id, int(channel.id))
    await interaction.response.send_message(f"✅ Added {channel.mention} to activity channels", ephemeral=True)

@active_group.command(name="remove_channel", description="Remove a channel from activity channels")
async def active_remove_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = require_guild(interaction)
    await remove_activity_channel(guild_id, int(channel.id))
    await interaction.response.send_message(f"✅ Removed {channel.mention} from activity channels", ephemeral=True)

@active_group.command(name="list_channels", description="List activity channels (channels mode)")
async def active_list_channels_cmd(interaction: discord.Interaction):
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
    s = await get_guild_settings(guild_id)
    await interaction.response.send_message(
        f"Mode: `{s['active_mode']}`\nThreshold: `{s['active_threshold_minutes']}` minute(s)\nActive role ID: `{s['active_role_id']}`",
        ephemeral=True,
    )

deadchat_group = discord.app_commands.Group(name="deadchat", description="Dead Chat settings")

@deadchat_group.command(name="set_role", description="Set the Dead Chat role")
async def deadchat_set_role_cmd(interaction: discord.Interaction, role: discord.Role):
    guild_id = require_guild(interaction)
    await set_deadchat_role(guild_id, int(role.id))
    await interaction.response.send_message(f"✅ Dead Chat role set to {role.mention}", ephemeral=True)

@deadchat_group.command(name="clear_role", description="Clear the Dead Chat role")
async def deadchat_clear_role_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    await set_deadchat_role(guild_id, None)
    await interaction.response.send_message("✅ Dead Chat role cleared", ephemeral=True)

@deadchat_group.command(name="set_idle", description="Set the idle threshold for Dead Chat")
@discord.app_commands.choices(minutes=DEADCHAT_IDLE_CHOICES)
async def deadchat_set_idle_cmd(interaction: discord.Interaction, minutes: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await set_deadchat_idle(guild_id, minutes.value)
    await interaction.response.send_message(f"✅ Dead Chat idle set to {minutes.value} minute(s)", ephemeral=True)

@deadchat_group.command(name="set_cooldown", description="Set cooldown before a user can win again")
@discord.app_commands.choices(minutes=DEADCHAT_COOLDOWN_CHOICES)
async def deadchat_set_cooldown_cmd(interaction: discord.Interaction, minutes: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await set_deadchat_cooldown(guild_id, minutes.value)
    await interaction.response.send_message(f"✅ Dead Chat cooldown set to {minutes.value} minute(s)", ephemeral=True)

@deadchat_group.command(name="require_active", description="Require Active role to win Dead Chat")
@discord.app_commands.choices(enabled=BOOL_CHOICES)
async def deadchat_require_active_cmd(interaction: discord.Interaction, enabled: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await set_deadchat_requires_active(guild_id, bool(enabled.value))
    await interaction.response.send_message(f"✅ Require Active set to `{bool(enabled.value)}`", ephemeral=True)

@deadchat_group.command(name="add_channel", description="Enable Dead Chat in a channel")
@discord.app_commands.describe(idle_override_minutes="Optional per-channel idle override (minutes)")
async def deadchat_add_channel_cmd(interaction: discord.Interaction, channel: discord.TextChannel, idle_override_minutes: int | None = None):
    guild_id = require_guild(interaction)
    if idle_override_minutes is not None and (idle_override_minutes < 1 or idle_override_minutes > 1440):
        await interaction.response.send_message("❌ idle_override_minutes must be between 1 and 1440", ephemeral=True)
        return
    await add_deadchat_channel(guild_id, int(channel.id), idle_override_minutes)
    await interaction.response.send_message(f"✅ Dead Chat enabled in {channel.mention}", ephemeral=True)

@deadchat_group.command(name="remove_channel", description="Disable Dead Chat in a channel")
async def deadchat_remove_channel_cmd(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = require_guild(interaction)
    await remove_deadchat_channel(guild_id, int(channel.id))
    await interaction.response.send_message(f"✅ Dead Chat disabled in {channel.mention}", ephemeral=True)

@deadchat_group.command(name="list_channels", description="List Dead Chat channels")
async def deadchat_list_channels_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    rows = await list_deadchat_channels(guild_id)
    if not rows:
        await interaction.response.send_message("No Dead Chat channels set.", ephemeral=True)
        return
    lines = []
    for r in rows:
        ch = interaction.guild.get_channel(int(r["channel_id"])) if interaction.guild else None
        name = ch.mention if ch else f"`{r['channel_id']}`"
        idle = f"{r['idle_minutes']}m" if r["idle_minutes"] is not None else "default"
        lines.append(f"{name} (idle: {idle})")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@deadchat_group.command(name="show", description="Show Dead Chat settings")
async def deadchat_show_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    s = await get_guild_settings(guild_id)
    await interaction.response.send_message(
        f"Dead Chat role ID: `{s['deadchat_role_id']}`\nIdle: `{s['deadchat_idle_minutes']}` minute(s)\nCooldown: `{s['deadchat_cooldown_minutes']}` minute(s)\nRequires Active: `{s['deadchat_requires_active']}`",
        ephemeral=True,
    )

plague_group = discord.app_commands.Group(name="plague", description="Plague settings")

@plague_group.command(name="set_role", description="Set the Plague role")
async def plague_set_role_cmd(interaction: discord.Interaction, role: discord.Role):
    guild_id = require_guild(interaction)
    await plague_set_role(guild_id, int(role.id))
    await interaction.response.send_message(f"✅ Plague role set to {role.mention}", ephemeral=True)

@plague_group.command(name="clear_role", description="Clear the Plague role")
async def plague_clear_role_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    await plague_set_role(guild_id, None)
    await interaction.response.send_message("✅ Plague role cleared", ephemeral=True)

@plague_group.command(name="set_duration", description="Set infection duration")
@discord.app_commands.choices(hours=PLAGUE_DURATION_CHOICES)
async def plague_set_duration_cmd(interaction: discord.Interaction, hours: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await plague_set_duration(guild_id, hours.value)
    await interaction.response.send_message(f"✅ Plague duration set to {hours.value} hour(s)", ephemeral=True)

@plague_group.command(name="add_day", description="Add a plague day (YYYY-MM-DD)")
async def plague_add_day_cmd(interaction: discord.Interaction, day: str):
    guild_id = require_guild(interaction)
    try:
        d = parse_date_yyyy_mm_dd(day)
    except Exception:
        await interaction.response.send_message("❌ Date must be YYYY-MM-DD", ephemeral=True)
        return
    await plague_add_day(guild_id, d)
    await interaction.response.send_message(f"✅ Plague day added: `{d.isoformat()}`", ephemeral=True)

@plague_group.command(name="remove_day", description="Remove a plague day (YYYY-MM-DD)")
async def plague_remove_day_cmd(interaction: discord.Interaction, day: str):
    guild_id = require_guild(interaction)
    try:
        d = parse_date_yyyy_mm_dd(day)
    except Exception:
        await interaction.response.send_message("❌ Date must be YYYY-MM-DD", ephemeral=True)
        return
    await plague_remove_day(guild_id, d)
    await interaction.response.send_message(f"✅ Plague day removed: `{d.isoformat()}`", ephemeral=True)

@plague_group.command(name="list_days", description="List plague days")
async def plague_list_days_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    days = await plague_list_days(guild_id)
    if not days:
        await interaction.response.send_message("No plague days set.", ephemeral=True)
        return
    await interaction.response.send_message("\n".join(d.isoformat() for d in days), ephemeral=True)

prizes_group = discord.app_commands.Group(name="prizes", description="Prize settings")

@prizes_group.command(name="enable", description="Enable/disable prize system")
@discord.app_commands.choices(enabled=BOOL_CHOICES)
async def prizes_enable_cmd(interaction: discord.Interaction, enabled: discord.app_commands.Choice[int]):
    guild_id = require_guild(interaction)
    await prize_set_enabled(guild_id, bool(enabled.value))
    await interaction.response.send_message(f"✅ Prizes enabled set to `{bool(enabled.value)}`", ephemeral=True)

@prizes_group.command(name="add_definition", description="Add a prize definition")
async def prizes_add_definition_cmd(interaction: discord.Interaction, title: str, description: str | None = None, image_url: str | None = None):
    guild_id = require_guild(interaction)
    pid = await prize_add_definition(guild_id, title.strip()[:200], (description.strip()[:1000] if description else None), (image_url.strip()[:500] if image_url else None))
    await interaction.response.send_message(f"✅ Prize added: `{title}`\nID: `{pid}`", ephemeral=True)

@prizes_group.command(name="list_definitions", description="List prize definitions")
async def prizes_list_definitions_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    defs = await prize_list_definitions(guild_id, limit=25)
    if not defs:
        await interaction.response.send_message("No prize definitions.", ephemeral=True)
        return
    lines = [f"`{d['prize_id']}` — {d['title']}" for d in defs]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@prizes_group.command(name="delete_definition", description="Delete a prize definition by selecting from list")
@discord.app_commands.autocomplete(prize_id=prize_autocomplete)
async def prizes_delete_definition_cmd(interaction: discord.Interaction, prize_id: str):
    guild_id = require_guild(interaction)
    try:
        pid = uuid.UUID(prize_id)
    except Exception:
        await interaction.response.send_message("❌ Invalid prize id.", ephemeral=True)
        return
    await prize_delete_definition(guild_id, pid)
    await interaction.response.send_message("✅ Prize deleted.", ephemeral=True)

@prizes_group.command(name="schedule", description="Schedule a prize drop day")
@discord.app_commands.autocomplete(prize_id=prize_autocomplete)
@discord.app_commands.choices(not_before=PRIZE_TIME_CHOICES)
async def prizes_schedule_cmd(interaction: discord.Interaction, day: str, prize_id: str, channel: discord.TextChannel, not_before: discord.app_commands.Choice[str]):
    guild_id = require_guild(interaction)
    try:
        d = parse_date_yyyy_mm_dd(day)
    except Exception:
        await interaction.response.send_message("❌ Date must be YYYY-MM-DD", ephemeral=True)
        return
    try:
        pid = uuid.UUID(prize_id)
    except Exception:
        await interaction.response.send_message("❌ Invalid prize id.", ephemeral=True)
        return
    t = parse_hh_mm(not_before.value)
    sid = await prize_schedule_add(guild_id, d, t, int(channel.id), pid)
    await interaction.response.send_message(f"✅ Scheduled.\nSchedule ID: `{sid}`", ephemeral=True)

@prizes_group.command(name="list_schedules", description="List upcoming prize schedules")
async def prizes_list_schedules_cmd(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    settings = await get_guild_settings(guild_id)
    local_now = guild_now(settings["timezone"])
    scheds = await prize_schedule_list_upcoming(guild_id, local_now.date(), limit=25)
    if not scheds:
        await interaction.response.send_message("No upcoming schedules.", ephemeral=True)
        return
    lines = []
    for s in scheds:
        nbt = s["not_before_time"].strftime("%H:%M") if s["not_before_time"] else "Any"
        ch = interaction.guild.get_channel(int(s["channel_id"])) if interaction.guild else None
        chs = ch.mention if ch else f"`{s['channel_id']}`"
        lines.append(f"`{s['schedule_id']}` — {s['day'].isoformat()} @ {nbt} in {chs} (used: {s['used']})")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@prizes_group.command(name="remove_schedule", description="Remove a schedule by selecting from list")
@discord.app_commands.autocomplete(schedule_id=schedule_autocomplete)
async def prizes_remove_schedule_cmd(interaction: discord.Interaction, schedule_id: str):
    guild_id = require_guild(interaction)
    try:
        sid = uuid.UUID(schedule_id)
    except Exception:
        await interaction.response.send_message("❌ Invalid schedule id.", ephemeral=True)
        return
    await prize_schedule_remove(guild_id, sid)
    await interaction.response.send_message("✅ Schedule removed.", ephemeral=True)

timezone_group = discord.app_commands.Group(name="timezone", description="Timezone settings")

@timezone_group.command(name="set", description="Set this server's timezone")
@discord.app_commands.autocomplete(tz=timezone_autocomplete)
async def tz_set(interaction: discord.Interaction, tz: str):
    guild_id = require_guild(interaction)
    await upsert_timezone(guild_id, tz)
    await interaction.response.send_message(f"✅ Timezone set to `{tz}`", ephemeral=True)

@timezone_group.command(name="show", description="Show this server's timezone")
async def tz_show(interaction: discord.Interaction):
    guild_id = require_guild(interaction)
    s = await get_guild_settings(guild_id)
    await interaction.response.send_message(f"⏰ Current timezone: `{s['timezone']}`", ephemeral=True)

status_group = discord.app_commands.Group(name="status", description="View system state")

@status_group.command(name="deadchat", description="Show current Dead Chat holder for this channel")
async def status_deadchat(interaction: discord.Interaction, channel: discord.TextChannel | None = None):
    if interaction.guild is None:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    ch = channel or interaction.channel
    if not isinstance(ch, discord.TextChannel):
        await interaction.response.send_message("Pick a text channel.", ephemeral=True)
        return
    state = await deadchat_get_state(int(interaction.guild.id), int(ch.id))
    holder = state["current_holder_user_id"]
    last_award = state["last_award_at"]
    last_msg = state["last_message_at"]
    holder_s = f"<@{holder}>" if holder else "None"
    await interaction.response.send_message(
        f"Channel: {ch.mention}\nHolder: {holder_s}\nLast award: `{last_award}`\nLast message tracked: `{last_msg}`",
        ephemeral=True,
    )

@status_group.command(name="activity", description="Show activity info for a user")
async def status_activity(interaction: discord.Interaction, user: discord.Member | None = None):
    if interaction.guild is None:
        await interaction.response.send_message("Use this in a server.", ephemeral=True)
        return
    member = user or interaction.user
    async with db_pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT last_message_at FROM member_activity WHERE guild_id = $1 AND user_id = $2;",
            int(interaction.guild.id),
            int(member.id),
        )
    await interaction.response.send_message(f"{member.mention} last message at: `{val}`", ephemeral=True)

bot.tree.add_command(active_group)
bot.tree.add_command(deadchat_group)
bot.tree.add_command(plague_group)
bot.tree.add_command(prizes_group)
bot.tree.add_command(timezone_group)
bot.tree.add_command(status_group)

############### ON_READY & BOT START ###############
@bot.event
async def on_ready():
    global active_cleanup_task, plague_cleanup_task, deadchat_cleanup_task
    await bot.tree.sync()
    if active_cleanup_task is None:
        active_cleanup_task = asyncio.create_task(active_cleanup_loop(bot))
    if plague_cleanup_task is None:
        plague_cleanup_task = asyncio.create_task(plague_cleanup_loop(bot))
    if deadchat_cleanup_task is None:
        deadchat_cleanup_task = asyncio.create_task(deadchat_cleanup_loop())
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
