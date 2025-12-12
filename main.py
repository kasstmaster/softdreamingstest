############### IMPORTS ###############
import discord
import os
import asyncio
import aiohttp
import json
import traceback
import sys
import asyncpg
from datetime import datetime, timedelta
from discord import TextChannel
from discord.ui import Select


############### CONSTANTS & CONFIG ###############
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.voice_states = True

DEBUG_GUILD_ID = int(os.getenv("DEBUG_GUILD_ID", "0"))
bot = discord.Bot(intents=intents, debug_guilds=[DEBUG_GUILD_ID])

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

DELETE_DELAY_SECONDS = int(os.getenv("DELETE_DELAY_SECONDS", "3600"))
INACTIVE_DAYS_THRESHOLD = int(os.getenv("INACTIVE_DAYS_THRESHOLD", "14"))
DEAD_CHAT_IDLE_SECONDS = int(os.getenv("DEAD_CHAT_IDLE_SECONDS", "600"))
DEAD_CHAT_COOLDOWN_SECONDS = int(os.getenv("DEAD_CHAT_COOLDOWN_SECONDS", "0"))
PRIZE_PLAGUE_TRIGGER_HOUR_UTC = int(os.getenv("PRIZE_PLAGUE_TRIGGER_HOUR_UTC", "12"))
PRIZE_EMOJI = "🎁"

IGNORE_MEMBER_IDS = {int(x.strip()) for x in os.getenv("IGNORE_MEMBER_IDS", "").split(",") if x.strip().isdigit()}
MONTH_CHOICES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
MONTH_TO_NUM = {name: i for i, name in enumerate(MONTH_CHOICES, start=1)}

DEFAULT_BIRTHDAY_TEXT = "<a:pepebirthday:1296553298895310971> It's {mention}'s birthday!\n-# Add your own </set:1440919374310408234> — @everyone"
DEFAULT_TWITCH_LIVE_MESSAGE = "{name} is live on Twitch ┃ https://twitch.tv/{name}\n-# @everyone"
DEFAULT_DEADCHAT_STEAL_MESSAGE = "{mention} has stolen the {role} role after {minutes}+ minutes of silence.\n-# There's a random chance to win prizes with this role.\n-# [Learn More](https://discord.com/channels/1205041211610501120/1447330327923265586)"
DEFAULT_PLAGUE_OUTBREAK_MESSAGE = "**PLAGUE OUTBREAK**\n-# The sickness has chosen its host.\n-# {mention} bears the infection, binding the plague and ending today’s contagion.\n-# Those who claim Dead Chat after this moment will not be touched by the disease.\n-# [Learn More](https://discord.com/channels/1205041211610501120/1447330327923265586)"
DEFAULT_PRIZE_ANNOUNCE_MESSAGE = "{winner} has won a **{gift}** with {role}!\n-# Drop Rate: {rarity}"
DEFAULT_PRIZE_CLAIM_MESSAGE = "You claimed a **{gift}**!"
DEFAULT_AUTO_DELETE_IGNORE_PHRASES = []

GAME_NOTIF_OPEN_TEXT = "Choose the game notifications you want:"
GAME_NOTIF_NO_CHANGES = "No changes."
GAME_NOTIF_ADDED_PREFIX = "Added: "
GAME_NOTIF_REMOVED_PREFIX = "Removed: "


############### GLOBAL STATE / STORAGE ###############
guild_configs: dict[int, dict] = {}
db_pool: asyncpg.Pool | None = None

twitch_access_token: str | None = None
twitch_live_state: dict[str, bool] = {}
twitch_state_storage_message_id: int | None = None
all_twitch_channels: set[str] = set()

last_activity_storage_message_id: int | None = None
last_activity: dict[int, dict[int, str]] = {}

dead_current_holders: dict[int, int | None] = {}
dead_last_notice_message_ids: dict[int, int | None] = {}
dead_last_win_time: dict[int, datetime] = {}
deadchat_last_times: dict[int, str] = {}
deadchat_storage_message_id: int | None = None
deadchat_state_storage_message_id: int | None = None

sticky_messages: dict[int, int] = {}
sticky_texts: dict[int, str] = {}
sticky_storage_message_id: int | None = None

pending_member_joins: list[dict] = []
member_join_storage_message_id: int | None = None

plague_scheduled: dict[int, list[dict]] = {}
infected_members: dict[int, dict[int, str]] = {}
plague_storage_message_id: int | None = None

prize_defs: dict[int, dict[str, str]] = {}
scheduled_prizes: dict[int, list[dict]] = {}

startup_logging_done: bool = False
startup_log_buffer: list[str] = []


############### HELPER FUNCTIONS ###############
async def init_db():
    global db_pool
    if db_pool is not None:
        return
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        await log_to_bot_channel("@everyone DATABASE_URL is not set. Postgres not initialized.")
        return
    db_pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sticky_data (
            channel_id BIGINT PRIMARY KEY,
            text TEXT,
            message_id BIGINT
        );
        """)

        # Member join queue (delayed member role)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS member_join_queue (
            guild_id BIGINT,
            member_id BIGINT,
            assign_at TIMESTAMPTZ,
            PRIMARY KEY (guild_id, member_id)
        );
        """)

        # Dead Chat last timestamps
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS deadchat_last_times (
            channel_id BIGINT PRIMARY KEY,
            last_time TIMESTAMPTZ
        );
        """)

        # Dead Chat state (single row storing the JSON blobs)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS deadchat_state (
            guild_id BIGINT PRIMARY KEY,
            current_holders JSONB,
            last_win_times JSONB,
            notice_msg_ids JSONB
        );
        """)

        # Twitch live/offline cache
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS twitch_state (
            username TEXT PRIMARY KEY,
            is_live BOOLEAN
        );
        """)

        # Last activity per member
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS last_activity (
            guild_id BIGINT,
            member_id BIGINT,
            last_seen TIMESTAMPTZ,
            PRIMARY KEY (guild_id, member_id)
        );
        """)

async def save_guild_config_db(guild: discord.Guild, cfg: dict):
    if db_pool is None:
        return
    dead_chat_ids = cfg.get("dead_chat_channel_ids") or []
    auto_delete_ids = cfg.get("auto_delete_channel_ids") or []
    dead_chat_json = json.dumps([int(x) for x in dead_chat_ids])
    auto_delete_json = json.dumps([int(x) for x in auto_delete_ids])
    auto_delete_delay = cfg.get("auto_delete_delay_seconds")
    auto_delete_ignore_json = json.dumps(cfg.get("auto_delete_ignore_phrases", []))
    twitch_configs_json = json.dumps(cfg.get("twitch_configs", []))
    prize_defs_json = json.dumps(cfg.get("prize_defs", {}))
    prize_scheduled_json = json.dumps(cfg.get("prize_scheduled", []))
    plague_scheduled_json = json.dumps(cfg.get("plague_scheduled", []))
    infected_members_json = json.dumps({str(k): v for k, v in cfg.get("infected_members", {}).items()})
    birthday_text = cfg.get("birthday_text")
    twitch_live_text = cfg.get("twitch_live_text")
    plague_outbreak_text = cfg.get("plague_outbreak_text")
    deadchat_steal_text = cfg.get("deadchat_steal_text")
    prize_announce_text = cfg.get("prize_announce_text")
    prize_claim_text = cfg.get("prize_claim_text")
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_configs (
                guild_id,
                welcome_channel_id,
                birthday_role_id,
                member_join_role_id,
                bot_join_role_id,
                dead_chat_role_id,
                infected_role_id,
                active_role_id,
                dead_chat_channel_ids,
                auto_delete_channel_ids,
                mod_log_channel_id,
                bot_log_channel_id,
                prize_drop_channel_id,
                birthday_announce_channel_id,
                twitch_announce_channel_id,
                prize_announce_channel_id,
                auto_delete_delay_seconds,
                auto_delete_ignore_phrases,
                twitch_configs,
                prize_defs,
                prize_scheduled,
                plague_scheduled,
                infected_members,
                birthday_text,
                twitch_live_text,
                plague_outbreak_text,
                deadchat_steal_text,
                prize_announce_text,
                prize_claim_text
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29
            )
            ON CONFLICT (guild_id) DO UPDATE SET
                welcome_channel_id = EXCLUDED.welcome_channel_id,
                birthday_role_id = EXCLUDED.birthday_role_id,
                member_join_role_id = EXCLUDED.member_join_role_id,
                bot_join_role_id = EXCLUDED.bot_join_role_id,
                dead_chat_role_id = EXCLUDED.dead_chat_role_id,
                infected_role_id = EXCLUDED.infected_role_id,
                active_role_id = EXCLUDED.active_role_id,
                dead_chat_channel_ids = EXCLUDED.dead_chat_channel_ids,
                auto_delete_channel_ids = EXCLUDED.auto_delete_channel_ids,
                mod_log_channel_id = EXCLUDED.mod_log_channel_id,
                bot_log_channel_id = EXCLUDED.bot_log_channel_id,
                prize_drop_channel_id = EXCLUDED.prize_drop_channel_id,
                birthday_announce_channel_id = EXCLUDED.birthday_announce_channel_id,
                twitch_announce_channel_id = EXCLUDED.twitch_announce_channel_id,
                prize_announce_channel_id = EXCLUDED.prize_announce_channel_id,
                auto_delete_delay_seconds = EXCLUDED.auto_delete_delay_seconds,
                auto_delete_ignore_phrases = EXCLUDED.auto_delete_ignore_phrases,
                twitch_configs = EXCLUDED.twitch_configs,
                prize_defs = EXCLUDED.prize_defs,
                prize_scheduled = EXCLUDED.prize_scheduled,
                plague_scheduled = EXCLUDED.plague_scheduled,
                infected_members = EXCLUDED.infected_members,
                birthday_text = EXCLUDED.birthday_text,
                twitch_live_text = EXCLUDED.twitch_live_text,
                plague_outbreak_text = EXCLUDED.plague_outbreak_text,
                deadchat_steal_text = EXCLUDED.deadchat_steal_text,
                prize_announce_text = EXCLUDED.prize_announce_text,
                prize_claim_text = EXCLUDED.prize_claim_text
            """,
            guild.id,
            cfg.get("welcome_channel_id"),
            cfg.get("birthday_role_id"),
            cfg.get("member_join_role_id"),
            cfg.get("bot_join_role_id"),
            cfg.get("dead_chat_role_id"),
            cfg.get("infected_role_id"),
            cfg.get("active_role_id"),
            dead_chat_json,
            auto_delete_json,
            cfg.get("mod_log_channel_id"),
            cfg.get("bot_log_channel_id"),
            cfg.get("prize_drop_channel_id"),
            cfg.get("birthday_announce_channel_id"),
            cfg.get("twitch_announce_channel_id"),
            cfg.get("prize_announce_channel_id"),
            auto_delete_delay,
            auto_delete_ignore_json,
            twitch_configs_json,
            prize_defs_json,
            prize_scheduled_json,
            plague_scheduled_json,
            infected_members_json,
            birthday_text,
            twitch_live_text,
            plague_outbreak_text,
            deadchat_steal_text,
            prize_announce_text,
            prize_claim_text
        )

async def get_guild_config(guild: discord.Guild) -> dict:
    if not guild or db_pool is None:
        return {}

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM guild_configs WHERE guild_id = $1", guild.id)

    if not row:
        # No row yet — return empty config instead of None
        return {}

    data = dict(row)

    if data.get("dead_chat_channel_ids"):
        try:
            data["dead_chat_channel_ids"] = [int(x) for x in json.loads(data["dead_chat_channel_ids"])]
        except:
            data["dead_chat_channel_ids"] = []
    else:
        data["dead_chat_channel_ids"] = []

    if data.get("auto_delete_channel_ids"):
        try:
            data["auto_delete_channel_ids"] = [int(x) for x in json.loads(data["auto_delete_channel_ids"])]
        except:
            data["auto_delete_channel_ids"] = []
    else:
        data["auto_delete_channel_ids"] = []

    data["auto_delete_ignore_phrases"] = json.loads(data.get("auto_delete_ignore_phrases") or "[]")
    data["twitch_configs"] = json.loads(data.get("twitch_configs") or "[]")
    data["prize_defs"] = json.loads(data.get("prize_defs") or "{}")
    data["prize_scheduled"] = json.loads(data.get("prize_scheduled") or "[]")
    data["plague_scheduled"] = json.loads(data.get("plague_scheduled") or "[]")
    data["infected_members"] = {int(k): v for k, v in json.loads(data.get("infected_members") or "{}").items()}
    return data

async def ensure_guild_config(guild: discord.Guild) -> dict:
    """Return this guild's config, creating an empty row in Postgres if needed."""
    if db_pool is None or guild is None:
        return {}

    # First try to load from DB
    cfg = await get_guild_config(guild)
    if cfg is not None:
        return cfg

    # If missing, insert a bare row, then load again
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_configs (guild_id)
            VALUES ($1)
            ON CONFLICT (guild_id) DO NOTHING
            """,
            guild.id,
        )

    cfg = await get_guild_config(guild)
    return cfg or {}

async def get_config_role(guild: discord.Guild, key: str) -> discord.Role | None:
    cfg = await ensure_guild_config(guild)
    if not cfg:
        return None
    rid = cfg.get(key)
    if not rid:
        return None
    return guild.get_role(rid)

async def log_to_guild_mod_log(guild: discord.Guild, content: str):
    ch = await get_config_channel(guild, "mod_log_channel_id")
    if not ch:
        return
    try:
        await ch.send(content)
    except Exception:
        pass

async def log_to_guild_bot_channel(guild: discord.Guild, content: str):
    ch = await get_config_channel(guild, "bot_log_channel_id")
    if not ch:
        return
    try:
        await ch.send(content)
    except Exception:
        pass

async def log_to_bot_channel(content: str):
    if not startup_logging_done:
        startup_log_buffer.append(content)
        return
    guild = bot.get_guild(DEBUG_GUILD_ID) or bot.guilds[0] if bot.guilds else None
    if guild:
        await log_to_guild_bot_channel(guild, content)

async def flush_startup_logs():
    if not startup_log_buffer:
        return
    guild = bot.get_guild(DEBUG_GUILD_ID) or bot.guilds[0] if bot.guilds else None
    if not guild:
        return
    cfg = await ensure_guild_config(guild)
    channel = guild.get_channel(cfg.get("bot_log_channel_id")) if cfg else None
    if not channel:
        return
    early = []
    watcher_lines = []
    startup_summaries = []
    ready_lines = []
    activity_loaded_lines = []
    report_entry = None
    for entry in startup_log_buffer:
        if "[STORAGE]" in entry and "[RUNTIME CONFIG]" in entry:
            report_entry = entry
        elif entry.startswith("[TWITCH] watcher started") or entry.startswith("[PLAGUE] infected_watcher started") or entry.startswith("[MEMBERJOIN] watcher started") or entry.startswith("[ACTIVITY] activity_inactive_watcher started"):
            watcher_lines.append(entry)
        elif entry.startswith("[STARTUP] "):
            startup_summaries.append(entry)
        elif entry.startswith("[ACTIVITY] Loaded last activity"):
            activity_loaded_lines.append(entry)
        elif entry.startswith("Bot ready as "):
            ready_lines.append(entry)
        else:
            early.append(entry)
    parts = ["---------------------------- STARTUP LOGS ----------------------------",
    ""]
    parts.extend(early)
    basic_line = None
    if report_entry:
        lines = report_entry.split("\n")
        trimmed = [l.rstrip() for l in lines]
        idx = len(trimmed) - 1
        while idx >= 0 and trimmed[idx] == "":
            idx -= 1
        if idx >= 0 and trimmed[idx] == "All systems passed basic storage and runtime checks.":
            basic_line = trimmed[idx]
            trimmed = trimmed[:idx]
            while trimmed and trimmed[-1] == "":
                trimmed.pop()
        if trimmed:
            parts.extend(trimmed)
    if watcher_lines:
        parts.append("")
        parts.extend(watcher_lines)
    tail_present = startup_summaries or activity_loaded_lines or basic_line or ready_lines
    if tail_present:
        parts.append("")
        parts.extend(startup_summaries)
        parts.extend(activity_loaded_lines)
        if basic_line or ready_lines:
            parts.append("")
        if basic_line:
            parts.append(basic_line)
        parts.extend(ready_lines)
    text = "\n".join(parts)
    text = "@everyone\n" + text
    if len(text) > 1900:
        text = text[:1900]
    await channel.send(text)

async def log_exception(tag: str, exc: Exception):
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    text = f"{tag}: {exc}\n{tb}"
    text = "@everyone " + text
    if len(text) > 1900:
        text = text[:1900]
    await log_to_bot_channel(text)

async def check_runtime_systems():
    problems = []
    results = {
        "CHANNELS": True,
        "ROLES": True,
        "AUTO_DELETE": True,
        "DEAD_CHAT_CHANNELS": True,
        "TWITCH_CONFIG": True,
    }
    main_guild = bot.get_guild(DEBUG_GUILD_ID) or bot.guilds[0] if bot.guilds else None
    if main_guild is None:
        problems.append("Runtime: no guild found for checks")
        results["CHANNELS"] = False
        results["ROLES"] = False
        return problems, results
    me = main_guild.me
    if me is None:
        problems.append("Runtime: unable to resolve bot member in main guild")
        results["CHANNELS"] = False
        results["ROLES"] = False
        return problems, results
    cfg = await get_guild_config(main_guild)
    def fail(key: str, message: str):
        problems.append(message)
        if key in results:
            results[key] = False
    def check_channel_permissions(channel_id: int, label: str, key: str, need_manage: bool = False):
        if channel_id == 0:
            return
        channel = bot.get_channel(channel_id)
        if channel is None:
            fail(key, f"{label}: channel {channel_id} not found")
            return
        perms = channel.permissions_for(me)
        if not perms.view_channel or not perms.send_messages:
            fail(key, f"{label}: insufficient permissions to view/send")
        if not perms.read_message_history:
            fail(key, f"{label}: missing Read Message History")
        if need_manage and not perms.manage_messages:
            fail(key, f"{label}: missing Manage Messages")
    check_channel_permissions(cfg.get("welcome_channel_id", 0), "WELCOME_CHANNEL", "CHANNELS")
    check_channel_permissions(cfg.get("bot_log_channel_id", 0), "BOT_LOG_CHANNEL", "CHANNELS")
    check_channel_permissions(cfg.get("twitch_announce_channel_id", 0), "TWITCH_ANNOUNCE_CHANNEL", "CHANNELS")
    for cid in cfg.get("dead_chat_channel_ids", []):
        check_channel_permissions(cid, f"DEAD_CHAT_CHANNEL_{cid}", "DEAD_CHAT_CHANNELS", need_manage=True)
    for cid in cfg.get("auto_delete_channel_ids", []):
        check_channel_permissions(cid, f"AUTO_DELETE_CHANNEL_{cid}", "AUTO_DELETE", need_manage=True)
    roles_to_check = [
        (cfg.get("birthday_role_id", 0), "BIRTHDAY_ROLE_ID"),
        (cfg.get("member_join_role_id", 0), "MEMBER_JOIN_ROLE_ID"),
        (cfg.get("bot_join_role_id", 0), "BOT_JOIN_ROLE_ID"),
        (cfg.get("dead_chat_role_id", 0), "DEAD_CHAT_ROLE_ID"),
        (cfg.get("infected_role_id", 0), "INFECTED_ROLE_ID"),
    ]
    for role_id, label in roles_to_check:
        if role_id == 0:
            continue
        role = main_guild.get_role(role_id)
        if role is None:
            fail("ROLES", f"{label}: role {role_id} not found in main guild")
    if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
        results["TWITCH_CONFIG"] = True
    else:
        fail("TWITCH_CONFIG", "TWITCH_CONFIG: missing client id/secret")
    return problems, results

async def run_all_inits_with_logging():
    problems = []
    storage = {
        "STICKY": True,
        "DEADCHAT": True,
        "DEADCHAT_STATE": True,
        "TWITCH_STATE": True,
        "MEMBERJOIN": True,
        "ACTIVITY": True,
    }

    # DB-backed storage initializers
    try:
        await init_sticky_storage()
    except Exception as e:
        storage["STICKY"] = False
        problems.append("init_sticky_storage failed; sticky messages could not be loaded from Postgres.")
        await log_exception("init_sticky_storage", e)

    try:
        await init_deadchat_storage()
    except Exception as e:
        storage["DEADCHAT"] = False
        problems.append("init_deadchat_storage failed; Dead Chat timestamps could not be loaded from Postgres.")
        await log_exception("init_deadchat_storage", e)

    try:
        await init_deadchat_state_storage()
    except Exception as e:
        storage["DEADCHAT_STATE"] = False
        problems.append("init_deadchat_state_storage failed; Dead Chat state could not be loaded from Postgres.")
        await log_exception("init_deadchat_state_storage", e)

    try:
        await init_twitch_state_storage()
    except Exception as e:
        storage["TWITCH_STATE"] = False
        problems.append("init_twitch_state_storage failed; Twitch live state could not be loaded from Postgres.")
        await log_exception("init_twitch_state_storage", e)

    try:
        await init_member_join_storage()
    except Exception as e:
        storage["MEMBERJOIN"] = False
        problems.append("init_member_join_storage failed; pending member joins could not be loaded from Postgres.")
        await log_exception("init_member_join_storage", e)

    try:
        await init_last_activity_storage()
    except Exception as e:
        storage["ACTIVITY"] = False
        problems.append("init_last_activity_storage failed; last activity could not be loaded from Postgres.")
        await log_exception("init_last_activity_storage", e)

    # Runtime system checks (permissions, roles, etc.)
    try:
        runtime_problems, runtime_results = await check_runtime_systems()
        problems.extend(runtime_problems)
    except Exception as e:
        runtime_results = {
            "CHANNELS": False,
            "ROLES": False,
            "AUTO_DELETE": False,
            "DEAD_CHAT_CHANNELS": False,
            "TWITCH_CONFIG": False,
        }
        problems.append("Runtime system checks failed; see logs for details.")
        await log_exception("check_runtime_systems", e)

    # Build summary text
    lines = []
    lines.append("")
    lines.append("[STORAGE]")
    lines.append("`✅` Sticky storage (Postgres)" if storage["STICKY"] else "`⚠️` **Sticky storage** — Failed to load from Postgres.")
    lines.append("`✅` Dead Chat storage (Postgres)" if storage["DEADCHAT"] else "`⚠️` **Dead Chat storage** — Failed to load from Postgres.")
    lines.append("`✅` Dead Chat state (Postgres)" if storage["DEADCHAT_STATE"] else "`⚠️` **Dead Chat state** — Failed to load from Postgres.")
    lines.append("`✅` Member-join storage (Postgres)" if storage["MEMBERJOIN"] else "`⚠️` **Member-join storage** — Failed to load from Postgres.")
    lines.append("`✅` Twitch state storage (Postgres)" if storage["TWITCH_STATE"] else "`⚠️` **Twitch state storage** — Failed to load from Postgres.")
    lines.append("`✅` Activity storage (Postgres)" if storage["ACTIVITY"] else "`⚠️` **Activity storage** — Failed to load from Postgres.")

    lines.append("")
    lines.append("[RUNTIME CONFIG]")
    if runtime_results.get("CHANNELS", False):
        lines.append("`✅` Channels and basic permissions")
    else:
        lines.append("`⚠️` **Channels and basic permissions** — One or more required channels are missing or the bot lacks view, send, history, or manage permissions for them.")
    if runtime_results.get("ROLES", False):
        lines.append("`✅` Required roles present")
    else:
        lines.append("`⚠️` **Required roles present** — One or more required roles are missing from the main guild, so some automations cannot run.")
    if runtime_results.get("DEAD_CHAT_CHANNELS", False):
        lines.append("`✅` Dead Chat channels ready")
    else:
        lines.append("`⚠️` **Dead Chat channels ready** — One or more Dead Chat channels are missing or have insufficient permissions for idle tracking and role steals.")
    if runtime_results.get("AUTO_DELETE", False):
        lines.append("`✅` Auto-delete channels ready")
    else:
        lines.append("`⚠️` **Auto-delete channels ready** — One or more auto-delete channels are missing or lack message management permissions.")
    if runtime_results.get("TWITCH_CONFIG", False):
        lines.append("`✅` Twitch config and announce channel")
    else:
        lines.append("`⚠️` **Twitch config and announce channel** — Twitch client ID/secret or announce channel is misconfigured, so live notifications cannot be sent.")

    if problems:
        lines.append("")
        lines.append("[DETAILS]")
        for p in problems:
            lines.append(f"`⚠️` **Detail** — {p}")
    else:
        lines.append("")
        lines.append("All systems passed basic storage and runtime checks.")

    text = "\n".join(lines)
    if len(text) > 1900:
        text = text[:1900]
    await log_to_bot_channel(text)

    if problems:
        await log_to_bot_channel(f"[STARTUP] {len(problems)} problems detected, see report above.")
    else:
        await log_to_bot_channel("[STARTUP] All systems passed storage and runtime checks.")

async def init_sticky_storage():
    global sticky_storage_message_id  # kept only so code compiles, but unused now
    sticky_storage_message_id = None

    if db_pool is None:
        return

    sticky_texts.clear()
    sticky_messages.clear()

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT channel_id, text, message_id FROM sticky_data")

    for row in rows:
        cid = row["channel_id"]
        if row["text"]:
            sticky_texts[cid] = row["text"]
        if row["message_id"]:
            sticky_messages[cid] = row["message_id"]

    await log_to_bot_channel(f"[STICKY] Loaded {len(sticky_texts)} sticky entries from Postgres.")

async def save_stickies():
    if db_pool is None:
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE sticky_data")
            for cid, text in sticky_texts.items():
                mid = sticky_messages.get(cid)
                await conn.execute(
                    "INSERT INTO sticky_data (channel_id, text, message_id) VALUES ($1, $2, $3)",
                    cid,
                    text,
                    mid,
                )

async def init_member_join_storage():
    global member_join_storage_message_id, pending_member_joins
    member_join_storage_message_id = None  # no Discord message in DB mode

    if db_pool is None:
        return

    pending_member_joins = []

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT guild_id, member_id, assign_at FROM member_join_queue"
        )

    for row in rows:
        assign_at = row["assign_at"]
        pending_member_joins.append(
            {
                "guild_id": row["guild_id"],
                "member_id": row["member_id"],
                "assign_at": assign_at.isoformat() + "Z",
            }
        )

    await log_to_bot_channel(
        f"[MEMBERJOIN] Loaded {len(pending_member_joins)} pending entries from Postgres."
    )

async def save_member_join_storage():
    if db_pool is None:
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE member_join_queue")
            for entry in pending_member_joins:
                guild_id = entry.get("guild_id")
                member_id = entry.get("member_id")
                assign_at_str = entry.get("assign_at")
                if not (guild_id and member_id and assign_at_str):
                    continue
                try:
                    assign_at = datetime.fromisoformat(assign_at_str.replace("Z", ""))
                except Exception:
                    continue
                await conn.execute(
                    "INSERT INTO member_join_queue (guild_id, member_id, assign_at) VALUES ($1, $2, $3)",
                    guild_id,
                    member_id,
                    assign_at,
                )

async def trigger_plague_infection(member: discord.Member):
    guild = member.guild
    cfg = await ensure_guild_config(guild)
    infected_role_id = cfg.get("infected_role_id", 0)
    infected_role = guild.get_role(infected_role_id)
    if not infected_role or infected_role in member.roles:
        return
    await member.add_roles(infected_role, reason="Caught the monthly Dead Chat plague")
    expires_at = (datetime.utcnow() + timedelta(days=3)).isoformat() + "Z"
    infected_guild = infected_members.get(guild.id, {})
    infected_guild[member.id] = expires_at
    infected_members[guild.id] = infected_guild
    plague_scheduled_guild = plague_scheduled.get(guild.id, [])
    plague_scheduled_guild.clear()
    plague_scheduled[guild.id] = plague_scheduled_guild
    cfg["infected_members"] = {str(k): v for k, v in infected_guild.items()}
    cfg["plague_scheduled"] = plague_scheduled_guild
    await save_guild_config_db(guild, cfg)
    await log_to_guild_bot_channel(guild, f"[PLAGUE] {member.mention} infected; role expires at {expires_at}.")

async def check_plague_active(guild: discord.Guild):
    sch = plague_scheduled.get(guild.id, [])
    if not sch:
        return False
    now = datetime.utcnow()
    for entry in sch:
        start_str = entry.get("start")
        if start_str:
            try:
                start = datetime.fromisoformat(start_str.replace("Z", ""))
            except:
                continue
        else:
            date_str = entry.get("date")
            if not date_str:
                continue
            try:
                start = datetime.fromisoformat(date_str + "T00:00:00")
            except:
                continue
        if now >= start:
            return True
    return False

async def add_scheduled_prize(guild: discord.Guild, title: str, channel_id: int, content: str, date_str: str):
    cfg = await ensure_guild_config(guild)
    sch = cfg.get("prize_scheduled", [])
    existing_ids = [p.get("id", 0) for p in sch]
    new_id = max(existing_ids) + 1 if existing_ids else 1
    target_channel_id = cfg.get("prize_drop_channel_id") or channel_id
    sch.append(
        {
            "id": new_id,
            "channel_id": target_channel_id,
            "content": content,
            "date": date_str,
            "title": title,
        }
    )
    cfg["prize_scheduled"] = sch
    await save_guild_config_db(guild, cfg)

async def initialize_dead_chat():
    for guild in bot.guilds:
        cfg = await ensure_guild_config(guild)
        dead_role_id = cfg.get("dead_chat_role_id", 0)
        if dead_role_id == 0:
            continue
        role = guild.get_role(dead_role_id)
        if role and role.members:
            dead_current_holders[guild.id] = role.members[0].id
        for chan_id in cfg.get("dead_chat_channel_ids", []):
            if chan_id in deadchat_last_times:
                continue
            deadchat_last_times[chan_id] = discord.utils.utcnow().isoformat() + "Z"
    await save_deadchat_storage()

async def handle_dead_chat_message(message: discord.Message):
    guild = message.guild
    cfg = await ensure_guild_config(guild)
    dead_role_id = cfg.get("dead_chat_role_id", 0)
    dead_channels = cfg.get("dead_chat_channel_ids", [])
    active_role_id = cfg.get("active_role_id", 0)
    if dead_role_id == 0 or message.channel.id not in dead_channels or message.author.id in IGNORE_MEMBER_IDS:
        return
    if active_role_id != 0:
        active_role = guild.get_role(active_role_id)
        if active_role and active_role not in message.author.roles:
            return
    now = discord.utils.utcnow()
    cid = message.channel.id
    now_s = now.isoformat() + "Z"
    last_raw = deadchat_last_times.get(cid)
    deadchat_last_times[cid] = now_s
    await save_deadchat_storage()
    if not last_raw:
        return
    try:
        last_time = datetime.fromisoformat(last_raw.replace("Z", ""))
    except:
        return
    if (now - last_time).total_seconds() < DEAD_CHAT_IDLE_SECONDS:
        return
    role = guild.get_role(dead_role_id)
    if not role:
        return
    if DEAD_CHAT_COOLDOWN_SECONDS > 0:
        last_win = dead_last_win_time.get(message.author.id)
        if last_win and (now - last_win).total_seconds() < DEAD_CHAT_COOLDOWN_SECONDS:
            return
    for member in list(role.members):
        if member.id != message.author.id:
            await member.remove_roles(role, reason="Dead Chat stolen")
    await message.author.add_roles(role, reason="Dead Chat claimed")
    dead_current_holders[guild.id] = message.author.id
    dead_last_win_time[message.author.id] = now

    today_str = now.strftime("%Y-%m-%d")
    hour_utc = now.hour
    triggered_plague = False
    if hour_utc >= PRIZE_PLAGUE_TRIGGER_HOUR_UTC and await check_plague_active(guild):
        await trigger_plague_infection(message.author)
        triggered_plague = True

    triggered_prize = False
    if hour_utc >= PRIZE_PLAGUE_TRIGGER_HOUR_UTC:
        sch = cfg.get("prize_scheduled", [])
        matching = [p for p in sch if p.get("date") == today_str]
        if matching:
            triggered_prize = True
        for p in matching:
            title = p.get("title")
            rarity = cfg.get("prize_defs", {}).get(title)
            if not rarity:
                continue
            content = p.get("content") or f"**YOU'VE FOUND A PRIZE!**\nPrize: *{title}*\nDrop Rate: *{rarity}*"
            channel_id = cfg.get("prize_drop_channel_id") or p.get("channel_id") or message.channel.id
            channel = guild.get_channel(channel_id)
            if not channel:
                channel = message.channel
            view = PrizeView(title, rarity)
            await channel.send(content, view=view)
        new_sch = [p for p in sch if p.get("date") != today_str]
        if len(new_sch) != len(sch):
            cfg["prize_scheduled"] = new_sch
            await save_guild_config_db(guild, cfg)
            await log_to_guild_bot_channel(guild, f"[PRIZE] Daily prize drop(s) sent for {today_str}.")

    for old_cid, mid in list(dead_last_notice_message_ids.items()):
        if mid:
            ch = guild.get_channel(old_cid)
            if ch:
                try:
                    m = await ch.fetch_message(mid)
                    await m.delete()
                except:
                    pass

    if triggered_plague:
        plague_text = cfg.get("plague_outbreak_text", DEFAULT_PLAGUE_OUTBREAK_MESSAGE).format(mention=message.author.mention)
        notice = await message.channel.send(plague_text)
        await log_to_guild_bot_channel(guild, f"[PLAGUE] {message.author.mention} infected on {today_str} in {message.channel.mention}.")
    else:
        minutes = DEAD_CHAT_IDLE_SECONDS // 60
        deadchat_text = cfg.get("deadchat_steal_text", DEFAULT_DEADCHAT_STEAL_MESSAGE)
        notice_text = deadchat_text.format(mention=message.author.mention, role=role.mention, minutes=minutes)
        notice = await message.channel.send(notice_text)
        await log_to_guild_bot_channel(guild, f"[DEADCHAT] {message.author.mention} stole {role.mention} in {message.channel.mention} after {minutes}+ minutes idle.")

    dead_last_notice_message_ids[message.channel.id] = notice.id
    await save_deadchat_state()

async def init_deadchat_storage():
    global deadchat_storage_message_id, deadchat_last_times
    deadchat_storage_message_id = None  # no Discord message

    if db_pool is None:
        return

    deadchat_last_times.clear()

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT channel_id, last_time FROM deadchat_last_times")

    for row in rows:
        cid = row["channel_id"]
        ts = row["last_time"]
        deadchat_last_times[cid] = ts.isoformat() + "Z"

    await log_to_bot_channel(
        f"[DEADCHAT] Loaded timestamps for {len(deadchat_last_times)} channel(s) from Postgres."
    )

async def save_deadchat_storage():
    if db_pool is None:
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE deadchat_last_times")
            for cid, ts_str in deadchat_last_times.items():
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", ""))
                except Exception:
                    continue
                await conn.execute(
                    "INSERT INTO deadchat_last_times (channel_id, last_time) VALUES ($1, $2)",
                    cid,
                    dt,
                )

async def init_deadchat_state_storage():
    global deadchat_state_storage_message_id
    deadchat_state_storage_message_id = None  # no Discord message
    await load_deadchat_state()

async def load_deadchat_state():
    global dead_last_win_time, dead_last_notice_message_ids, dead_current_holders

    if db_pool is None:
        return

    dead_current_holders = {}
    dead_last_win_time = {}
    dead_last_notice_message_ids = {}

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT current_holders, last_win_times, notice_msg_ids FROM deadchat_state WHERE guild_id = 0"
        )

    if not row:
        return

    try:
        ch_json = row["current_holders"] or {}
        lw_json = row["last_win_times"] or {}
        nm_json = row["notice_msg_ids"] or {}
    except Exception:
        return

    dead_current_holders = {int(k): v for k, v in ch_json.items()}

    for k, v in lw_json.items():
        try:
            dead_last_win_time[int(k)] = datetime.fromisoformat(v.replace("Z", ""))
        except Exception:
            pass

    dead_last_notice_message_ids = {
        int(k): v for k, v in nm_json.items() if v
    }

async def save_deadchat_state():
    if db_pool is None:
        return

    data = {
        "current_holders": {str(k): v for k, v in dead_current_holders.items()},
        "last_win_times": {
            str(k): v.isoformat() + "Z" for k, v in dead_last_win_time.items()
        },
        "notice_msg_ids": {str(k): v for k, v in dead_last_notice_message_ids.items()},
    }

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO deadchat_state (guild_id, current_holders, last_win_times, notice_msg_ids)
            VALUES (0, $1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                current_holders = EXCLUDED.current_holders,
                last_win_times = EXCLUDED.last_win_times,
                notice_msg_ids = EXCLUDED.notice_msg_ids
            """,
            data["current_holders"],
            data["last_win_times"],
            data["notice_msg_ids"],
        )

async def init_twitch_state_storage():
    global twitch_state_storage_message_id
    twitch_state_storage_message_id = None  # no Discord message
    await load_twitch_state()
    await log_to_bot_channel("[TWITCH] State storage initialized from Postgres.")

async def load_twitch_state():
    global twitch_live_state

    if db_pool is None:
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT username, is_live FROM twitch_state")

    twitch_live_state = {row["username"].lower(): bool(row["is_live"]) for row in rows}
    await log_to_bot_channel(f"[TWITCH] Loaded live state for {len(twitch_live_state)} channel(s) from Postgres.")

async def save_twitch_state():
    if db_pool is None:
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE twitch_state")
            for name, is_live in twitch_live_state.items():
                await conn.execute(
                    "INSERT INTO twitch_state (username, is_live) VALUES ($1, $2)",
                    name,
                    is_live,
                )

async def get_twitch_token():
    global twitch_access_token
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return None
    url = "https://id.twitch.tv/oauth2/token"
    params = {"client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET, "grant_type": "client_credentials"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                twitch_access_token = data["access_token"]
                return twitch_access_token
    return None

async def fetch_twitch_streams():
    global twitch_access_token
    if not twitch_access_token:
        await get_twitch_token()
    if not twitch_access_token:
        return {}
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {twitch_access_token}"}
    names = list(all_twitch_channels)
    params = [("user_login", name) for name in names]
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.twitch.tv/helix/streams", headers=headers, params=params) as resp:
            if resp.status == 401:
                twitch_access_token = None
                await get_twitch_token()
                headers["Authorization"] = f"Bearer {twitch_access_token}"
                async with session.get("https://api.twitch.tv/helix/streams", headers=headers, params=params) as resp2:
                    data = await resp2.json()
            elif resp.status == 200:
                data = await resp.json()
            else:
                return {}
    result = {s["user_login"].lower(): s for s in data.get("data", [])}
    return result

async def init_last_activity_storage():
    global last_activity_storage_message_id, last_activity
    last_activity_storage_message_id = None  # no Discord message

    if db_pool is None:
        last_activity = {}
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT guild_id, member_id, last_seen FROM last_activity"
        )

    last_activity = {}
    for row in rows:
        gid = row["guild_id"]
        mid = row["member_id"]
        ts = row["last_seen"].isoformat() + "Z"
        last_activity.setdefault(gid, {})[mid] = ts

    total_members = sum(len(act) for act in last_activity.values())
    await log_to_bot_channel(
        f"[ACTIVITY] Loaded last activity for {total_members} member(s) across {len(last_activity)} guild(s) from Postgres."
    )

async def save_last_activity_storage():
    if db_pool is None:
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE last_activity")
            for gid, members in last_activity.items():
                for mid, ts_str in members.items():
                    try:
                        dt = datetime.fromisoformat(ts_str.replace("Z", ""))
                    except Exception:
                        continue
                    await conn.execute(
                        "INSERT INTO last_activity (guild_id, member_id, last_seen) VALUES ($1, $2, $3)",
                        gid,
                        mid,
                        dt,
                    )

async def touch_member_activity(member: discord.Member):
    if member.bot:
        return
    guild_id = member.guild.id
    if guild_id not in last_activity:
        last_activity[guild_id] = {}
    now = discord.utils.utcnow().isoformat() + "Z"
    last_activity[guild_id][member.id] = now
    await save_last_activity_storage()
    cfg = await get_guild_config(member.guild)
    active_role_id = cfg.get("active_role_id", 0)
    if active_role_id == 0:
        return
    role = member.guild.get_role(active_role_id)
    if role and role not in member.roles:
        try:
            await member.add_roles(role, reason="Marked active by activity tracking")
            await log_to_guild_bot_channel(member.guild, f"[ACTIVITY] {member.mention} marked active and given active role.")
        except Exception as e:
            await log_exception("touch_member_activity_add_role", e)


############### VIEWS / UI COMPONENTS ###############
class BasePrizeView(discord.ui.View):
    gift_title: str = ""
    rarity: str = ""
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="Claim Your Prize!", style=discord.ButtonStyle.primary)
    async def claim_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        try:
            await interaction.message.delete()
        except Exception as e:
            await log_exception("BasePrizeView_claim_button_delete", e)
        cfg = await ensure_guild_config(guild)
        dead_role_id = cfg.get("dead_chat_role_id", 0)
        dead_role = guild.get_role(dead_role_id)
        role_mention = dead_role.mention if dead_role else "the Dead Chat role"
        ch = await get_config_channel(guild, "prize_announce_channel_id") or await get_config_channel(guild, "welcome_channel_id")
        text = cfg.get("prize_announce_text", DEFAULT_PRIZE_ANNOUNCE_MESSAGE)
        if ch:
            msg = text.format(
                winner=interaction.user.mention,
                gift=self.gift_title,
                role=role_mention,
                rarity=self.rarity
            )
            await ch.send(msg)
        await log_to_guild_bot_channel(guild, f"[PRIZE] {interaction.user.mention} claimed prize '{self.gift_title}' (rarity {self.rarity}).")
        claim_text = cfg.get("prize_claim_text", DEFAULT_PRIZE_CLAIM_MESSAGE)
        await interaction.response.send_message(claim_text.format(gift=self.gift_title), ephemeral=True)

class PrizeView(BasePrizeView):
    def __init__(self, gift_title: str, rarity: str):
        super().__init__()
        self.gift_title = gift_title
        self.rarity = rarity


############### AUTOCOMPLETE FUNCTIONS ###############


############### BACKGROUND TASKS & SCHEDULERS ###############
async def twitch_watcher():
    await bot.wait_until_ready()
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return
    while not bot.is_closed():
        try:
            streams = await fetch_twitch_streams()
            for name in all_twitch_channels:
                is_live = name in streams
                was_live = twitch_live_state.get(name, False)
                if is_live and not was_live:
                    twitch_live_state[name] = True
                    await save_twitch_state()
                    for guild in bot.guilds:
                        cfg = await ensure_guild_config(guild)
                        text = cfg.get("twitch_live_text", DEFAULT_TWITCH_LIVE_MESSAGE)
                        for tc in cfg.get("twitch_configs", []):
                            if tc["username"].lower() == name:
                                ch = guild.get_channel(tc["announce_channel_id"])
                                if ch:
                                    await ch.send(text.format(name=name))
                    await log_to_bot_channel(f"[TWITCH] {name} went LIVE.")
                elif not is_live and was_live:
                    twitch_live_state[name] = False
                    await save_twitch_state()
                    await log_to_bot_channel(f"[TWITCH] {name} went OFFLINE.")
        except Exception as e:
            await log_exception("twitch_watcher", e)
        await asyncio.sleep(60)

async def infected_watcher():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = datetime.utcnow()
            for guild in bot.guilds:
                cfg = await ensure_guild_config(guild)
                infected = cfg.get("infected_members", {})
                expired_ids = []
                for mid, ts in infected.items():
                    try:
                        expires = datetime.fromisoformat(ts.replace("Z", ""))
                        if now >= expires:
                            expired_ids.append(mid)
                    except:
                        continue
                if expired_ids:
                    cleared_mentions = []
                    role_id = cfg.get("infected_role_id", 0)
                    role = guild.get_role(role_id)
                    if role:
                        for mid in expired_ids:
                            member = guild.get_member(mid)
                            if member and role in member.roles:
                                try:
                                    await member.remove_roles(role, reason="Plague expired")
                                    cleared_mentions.append(member.mention)
                                except:
                                    pass
                    new_infected = {k: v for k, v in infected.items() if k not in expired_ids}
                    cfg["infected_members"] = new_infected
                    await save_guild_config_db(guild, cfg)
                    if cleared_mentions:
                        await log_to_guild_bot_channel(guild, f"[PLAGUE] Cleared infected role for: {', '.join(cleared_mentions)}")
                    else:
                        await log_to_guild_bot_channel(guild, f"[PLAGUE] Cleared infected role for: {', '.join(f'<@{i}>' for i in expired_ids)}")
        except Exception as e:
            await log_exception("infected_watcher", e)
        await asyncio.sleep(3600)

async def member_join_watcher():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = datetime.utcnow()
            remaining = []
            changed = False
            for entry in pending_member_joins:
                assign_at_str = entry.get("assign_at")
                guild_id = entry.get("guild_id")
                member_id = entry.get("member_id")
                if not assign_at_str or not guild_id or not member_id:
                    continue
                try:
                    assign_at = datetime.fromisoformat(assign_at_str.replace("Z", ""))
                except:
                    continue
                if now >= assign_at:
                    guild = bot.get_guild(guild_id)
                    if not guild:
                        continue
                    cfg = await ensure_guild_config(guild)
                    member_role_id = cfg.get("member_join_role_id", 0)
                    if not member_role_id:
                        continue
                    member = guild.get_member(member_id)
                    if not member:
                        continue
                    role = guild.get_role(member_role_id)
                    if not role:
                        continue
                    if role not in member.roles:
                        try:
                            await member.add_roles(role, reason="Delayed member join role")
                            await log_to_guild_bot_channel(guild, f"[MEMBERJOIN] Applied member role to {member.mention}.")
                        except:
                            pass
                    await touch_member_activity(member)
                    changed = True
                else:
                    remaining.append(entry)
            if changed or len(remaining) != len(pending_member_joins):
                pending_member_joins[:] = remaining
                await save_member_join_storage()
                await log_to_bot_channel(f"[MEMBERJOIN] Remaining pending entries: {len(pending_member_joins)}")
        except Exception as e:
            await log_exception("member_join_watcher", e)
        await asyncio.sleep(300)

async def activity_inactive_watcher():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = discord.utils.utcnow()
            cutoff = now - timedelta(days=INACTIVE_DAYS_THRESHOLD)
            for guild in bot.guilds:
                cfg = await ensure_guild_config(guild)
                active_role_id = cfg.get("active_role_id", 0)
                if not active_role_id:
                    continue
                role = guild.get_role(active_role_id)
                if not role:
                    continue
                guild_activity = last_activity.get(guild.id, {})
                inactive_ids = set()
                for mid, ts in guild_activity.items():
                    try:
                        dt = datetime.fromisoformat(ts.replace("Z", ""))
                    except:
                        continue
                    if dt < cutoff:
                        inactive_ids.add(mid)
                for member in list(role.members):
                    if member.id in inactive_ids or member.id not in guild_activity:
                        try:
                            await member.remove_roles(role, reason="Marked inactive by activity tracking")
                            await log_to_guild_bot_channel(guild, f"{member.mention} is officially inactive @everyone")
                        except Exception as e:
                            await log_exception("activity_inactive_watcher_remove_role", e)
        except Exception as e:
            await log_exception("activity_inactive_watcher", e)
        await asyncio.sleep(86400)


############### EVENT HANDLERS ###############
@bot.event
async def on_ready():
    print(f"{bot.user} is online!")
    await init_db()
    await run_all_inits_with_logging()

    global startup_logging_done, startup_log_buffer

    # Initialize Dead Chat holders / timestamps based on loaded config
    await initialize_dead_chat()

    for guild in bot.guilds:
        cfg = await ensure_guild_config(guild)
        plague_scheduled[guild.id] = cfg.get("plague_scheduled", [])
        infected_members[guild.id] = cfg.get("infected_members", {})
        prize_defs[guild.id] = cfg.get("prize_defs", {})
        scheduled_prizes[guild.id] = cfg.get("prize_scheduled", [])
        for tc in cfg.get("twitch_configs", []):
            all_twitch_channels.add(tc["username"].lower())

@bot.event
async def on_member_update(before, after):
    guild = after.guild
    cfg = await ensure_guild_config(guild)
    ch = await get_config_channel(guild, "birthday_announce_channel_id") or await get_config_channel(guild, "welcome_channel_id")
    if not ch:
        return
    birthday_role_id = cfg.get("birthday_role_id", 0)
    new_roles = set(after.roles) - set(before.roles)
    for role in new_roles:
        if role.id == birthday_role_id:
            text = cfg.get("birthday_text", DEFAULT_BIRTHDAY_TEXT)
            await ch.send(text.replace("{mention}", after.mention))
            await log_to_guild_bot_channel(guild, f"[BIRTHDAY] Birthday role granted and message sent for {after.mention}.")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    await touch_member_activity(message.author)
    await handle_dead_chat_message(message)
    if message.channel.id in sticky_texts:
        old_id = sticky_messages.get(message.channel.id)
        if old_id:
            try:
                old_msg = await message.channel.fetch_message(old_id)
                await old_msg.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                await log_exception("on_message_sticky_delete", e)
        new_msg = await message.channel.send(sticky_texts[message.channel.id])
        sticky_messages[message.channel.id] = new_msg.id
        await save_stickies()
    guild = message.guild
    cfg = await ensure_guild_config(guild)
    auto_ids = cfg.get("auto_delete_channel_ids", [])
    if message.channel.id in auto_ids:
        content_lower = message.content.lower()
        # 100% custom, optional ignore list
        ignore_phrases = cfg.get("auto_delete_ignore_phrases") or []
        if not any(phrase.lower() in content_lower for phrase in ignore_phrases):
            delay = cfg.get("auto_delete_delay_seconds", DELETE_DELAY_SECONDS)

            async def delete_later():
                await asyncio.sleep(delay)
                try:
                    await message.delete()
                    await log_to_guild_bot_channel(
                        guild,
                        f"[AUTO-DELETE] Message {message.id} in <#{message.channel.id}> deleted after {delay} seconds."
                    )
                except Exception as e:
                    await log_exception("auto_delete_delete_later", e)

            bot.loop.create_task(delete_later())

@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    cfg = await ensure_guild_config(guild)
    ch = await get_config_channel(guild, "welcome_channel_id")

    if member.bot:
        bot_role_id = cfg.get("bot_join_role_id", 0)
        bot_role = guild.get_role(bot_role_id)
        if bot_role:
            try:
                await member.add_roles(bot_role)
            except Exception as e:
                await log_to_guild_bot_channel(guild, f"[WARN] Could not assign bot role to {member.mention}: {e}")
        return

    if ch:
        try:
            await ch.send(f"Welcome {member.mention}!")
        except Exception as e:
            await log_to_guild_bot_channel(guild, f"[ERROR] Could not send welcome for joiner {member.mention}: {e}")

    member_role_id = cfg.get("member_join_role_id", 0)
    if member_role_id:
        assign_at = (discord.utils.utcnow() + timedelta(seconds=180)).isoformat() + "Z"
        pending_member_joins.append(
            {
                "guild_id": guild.id,
                "member_id": member.id,
                "assign_at": assign_at,
            }
        )
        await save_member_join_storage()

@bot.event
async def on_member_ban(guild, user):
    moderator = None
    async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
        if entry.target.id == user.id:
            moderator = entry.user
            break
    text = f"{user.mention} was banned by {moderator.mention if moderator else 'Unknown'}"
    if user.bot:
        await log_to_guild_bot_channel(guild, text)
    else:
        await log_to_guild_mod_log(guild, text)

@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    now = discord.utils.utcnow()
    async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
        if entry.target.id == member.id and (now - entry.created_at).total_seconds() < 10:
            return
    kicked = False
    moderator = None
    async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.kick):
        if entry.target.id == member.id and (now - entry.created_at).total_seconds() < 10:
            moderator = entry.user
            kicked = True
            break
    if kicked:
        text = f"{member.mention} was kicked by {moderator.mention if moderator else 'Unknown'}"
    else:
        text = f"{member.mention} has left the server"
    if member.bot:
        await log_to_guild_bot_channel(guild, text)
    else:
        await log_to_guild_mod_log(guild, text)

@bot.event
async def on_application_command_error(ctx, error):
    await log_exception("application_command_error", error)
    try:
        await ctx.respond("An internal error occurred.", ephemeral=True)
    except:
        pass

@bot.event
async def on_error(event, *args, **kwargs):
    exc_type, exc, tb = sys.exc_info()
    if exc is None:
        await log_to_bot_channel(f"@everyone Unhandled error in event {event} with no exception info.")
    else:
        await log_exception(f"Unhandled error in event {event}", exc)


############### COMMAND GROUPS ###############
@bot.slash_command(name="db_test", description="Test Postgres connectivity")
async def db_test(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    if db_pool is None:
        return await ctx.respond("db_pool is not initialized. Check DATABASE_URL.", ephemeral=True)
    try:
        async with db_pool.acquire() as conn:
            value = await conn.fetchval("SELECT 1;")
    except Exception as e:
        return await ctx.respond(f"DB error: {e}", ephemeral=True)
    await ctx.respond(f"DB OK, SELECT 1 returned: {value}", ephemeral=True)

async def _update_guild_config(ctx, updates: dict, summary_label: str):
    if not ctx.author.guild_permissions.administrator:
        await ctx.respond("Admin only.", ephemeral=True)
        return None

    cfg = await ensure_guild_config(ctx.guild)
    cfg.update(updates)

    # keep in-memory cache in sync
    global guild_configs
    guild_configs[ctx.guild.id] = cfg

    await save_guild_config_db(ctx.guild, cfg)
    await log_to_guild_bot_channel(ctx.guild, f"[CONFIG] {ctx.author.mention} updated {summary_label}.")
    if "twitch_configs" in updates:
        for tc in cfg.get("twitch_configs", []):
            all_twitch_channels.add(tc["username"].lower())
    return cfg

@bot.slash_command(name="deadchat_rescan", description="Force-scan all dead-chat channels for latest message timestamps")
async def deadchat_rescan(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    count = 0
    async with ctx.typing():
        cfg = await get_guild_config(ctx.guild)
        for channel_id in cfg.get("dead_chat_channel_ids", []):
            channel = bot.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                continue
            try:
                async for message in channel.history(limit=1, oldest_first=False):
                    if message.author.bot:
                        continue
                    deadchat_last_times[channel_id] = message.created_at.isoformat() + "Z"
                    count += 1
                    break
            except Exception as e:
                await log_exception("deadchat_rescan_history", e)
        await save_deadchat_storage()
    await ctx.respond(f"Rescan complete — found latest message in {count} dead-chat channels and saved timestamps.", ephemeral=True)

@bot.slash_command(name="config_show", description="Show this server's Admin Bot config")
async def config_show(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    cfg = await get_guild_config(ctx.guild) or {}
    text = json.dumps(cfg, indent=2)
    if len(text) > 1900:
        text = text[:1900] + "\n...[truncated]"
    await ctx.respond(f"```json\n{text}\n```", ephemeral=True)

@bot.slash_command(name="config_db_show", description="Show this server's Admin Bot config from the database")
async def config_db_show(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    if db_pool is None:
        return await ctx.respond("Database not initialized.", ephemeral=True)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM guild_configs WHERE guild_id = $1", ctx.guild.id)
    if not row:
        return await ctx.respond("No database config found for this server.", ephemeral=True)
    data = dict(row)
    if data.get("dead_chat_channel_ids"):
        try:
            data["dead_chat_channel_ids"] = json.loads(data["dead_chat_channel_ids"])
        except:
            pass
    if data.get("auto_delete_channel_ids"):
        try:
            data["auto_delete_channel_ids"] = json.loads(data["auto_delete_channel_ids"])
        except:
            pass
    text = json.dumps(data, indent=2, default=str)
    if len(text) > 1900:
        text = text[:1900] + "\n...[truncated]"
    await ctx.respond(f"```json\n{text}\n```", ephemeral=True)

@bot.slash_command(
    name="setup",
    description="Show Admin Bot setup commands for this server"
)
async def setup(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)

    text = """
SETUP COMMANDS:
/add_channel_welcome
/add_channel_birthday_announce
/add_channel_twitch_announceme
/add_channel_dead_chat_triggers
/add_channel_prize_announce
/add_channel_member_log
/add_channel_bot_log
/add_channel_prize_drop
/add_channel_auto_delete
- What Triggers deletion
- Ignore words / Phrases
/add_role_active
/add_role_birthday
/add_role_dead_chat
/add_role_infected
/add_role_member_join
/add_role_bot_join
/add_text_birthday
/add_text_twitch
/add_text_plague
/add_twitch_notification (as many as they want)
- twitch channel
- notification channel
/add_prize
- Create a prize title
- Create drop rate
    """.strip("\n")

    await ctx.respond(f"```txt\n{text}\n```", ephemeral=True)

@bot.slash_command(name="say", description="Make the bot say something right here")
async def say(ctx, message: discord.Option(str, "Message to send", required=True)):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("You need Administrator.", ephemeral=True)
    await ctx.channel.send(message.replace("\\n", "\n"))
    await ctx.respond("Sent!", ephemeral=True)

@bot.slash_command(name="editbotmsg", description="Edit a bot message in this channel with up to 4 lines")
async def editbotmsg(
    ctx,
    message_id: discord.Option(str, "Message ID", required=True),
    line1: discord.Option(str, "Line 1 (optional)", required=False) = None,
    line2: discord.Option(str, "Line 2 (optional)", required=False) = None,
    line3: discord.Option(str, "Line 3 (optional)", required=False) = None,
    line4: discord.Option(str, "Line 4 (optional)", required=False) = None,
):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)
    try:
        msg_id_int = int(message_id)
    except ValueError:
        return await ctx.respond("Invalid message ID.", ephemeral=True)
    try:
        msg = await ctx.channel.fetch_message(msg_id_int)
    except discord.NotFound:
        return await ctx.respond("Message not found in this channel.", ephemeral=True)
    except discord.Forbidden:
        return await ctx.respond("I cannot access that message.", ephemeral=True)
    except discord.HTTPException:
        return await ctx.respond("Error fetching that message.", ephemeral=True)
    if msg.author.id != bot.user.id:
        return await ctx.respond("That message was not sent by me.", ephemeral=True)

    existing_lines = msg.content.split("\n")
    while len(existing_lines) < 4:
        existing_lines.append("")

    new_lines = [
        line1 if line1 is not None else existing_lines[0],
        line2 if line2 is not None else existing_lines[1],
        line3 if line3 is not None else existing_lines[2],
        line4 if line4 is not None else existing_lines[3],
    ]
    new_content = "\n".join(new_lines)

    await msg.edit(content=new_content)
    await ctx.respond("Message updated.", ephemeral=True)

@bot.slash_command(name="birthday_announce", description="Manually send the birthday message for a member")
async def birthday_announce(ctx, member: discord.Option(discord.Member, "Member", required=True)):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("You need Administrator.", ephemeral=True)
    guild = ctx.guild
    cfg = await ensure_guild_config(guild)
    ch = await get_config_channel(guild, "birthday_announce_channel_id") or await get_config_channel(guild, "welcome_channel_id")
    if not ch:
        return await ctx.respond("Announce channel not found.", ephemeral=True)
    text = cfg.get("birthday_text", DEFAULT_BIRTHDAY_TEXT)
    msg = text.replace("{mention}", member.mention)
    await ch.send(msg)
    await log_to_guild_bot_channel(guild, f"[BIRTHDAY] Manual birthday announce sent for {member.mention} by {ctx.author.mention}.")
    await ctx.respond(f"Sent birthday message for {member.mention}.", ephemeral=True)

@bot.slash_command(name="activity_add", description="Mark a member as active right now")
async def activity_add(
    ctx,
    member: discord.Option(discord.Member, "Member to mark active", required=True),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    await touch_member_activity(member)
    await ctx.respond(f"{member.mention} marked active and given the active role.", ephemeral=True)

@bot.slash_command(name="prize_list", description="List scheduled prizes")
async def prize_list(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    cfg = await get_guild_config(ctx.guild)
    sch = cfg.get("prize_scheduled", [])
    if not sch:
        return await ctx.respond("No scheduled prizes.", ephemeral=True)
    lines = []
    for p in sch:
        lines.append(f"ID {p['id']} ┃ {p['title']} ┃ {p['date']} ┃ <#{p['channel_id']}>")
    text = "\n".join(lines)
    await ctx.respond(f"Scheduled prizes:\n{text}", ephemeral=True)

@bot.slash_command(name="prize_delete", description="Delete a scheduled prize")
async def prize_delete(
    ctx,
    prize_id: discord.Option(int, "ID from /prize_list", required=True),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    cfg = await get_guild_config(ctx.guild)
    sch = cfg.get("prize_scheduled", [])
    before = len(sch)
    sch = [p for p in sch if p.get("id") != prize_id]
    after = len(sch)
    if before == after:
        return await ctx.respond("No prize with that ID.", ephemeral=True)
    cfg["prize_scheduled"] = sch
    await save_guild_config_db(ctx.guild, cfg)
    await ctx.respond(f"Deleted scheduled prize ID {prize_id}.", ephemeral=True)

@bot.slash_command(name="schedule_prize")
async def schedule_prize(
    ctx,
    title: discord.Option(str, "Prize title", required=True),
    month: discord.Option(str, "Month (UTC date)", required=False, choices=MONTH_CHOICES),
    day: discord.Option(int, "Day of month", required=False),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)

    guild = ctx.guild
    
    cfg = await get_guild_config(ctx.guild)
    defs = cfg.get("prize_defs", {})
    rarity = defs.get(title)
    if not rarity:
        return await ctx.respond(f"Prize '{title}' not defined. Use /add_prize first.", ephemeral=True)
    content = f"**YOU'VE FOUND A PRIZE!**\nPrize: *{title}*\nDrop Rate: *{rarity}*"
    if month is None and day is None:
        drop_ch = guild.get_channel(cfg.get("prize_drop_channel_id")) or ctx.channel
        view = PrizeView(title, rarity)
        await drop_ch.send(content, view=view)
        await log_to_guild_bot_channel(ctx.guild, f"[PRIZE] Immediate prize drop '{title}' triggered by {ctx.author.mention} in {drop_ch.mention}.")
        return await ctx.respond(f"Prize drop sent in {drop_ch.mention}.", ephemeral=True)
    if month is None or day is None:
        return await ctx.respond("Provide month and day, or leave both blank.", ephemeral=True)
    month_num = MONTH_TO_NUM.get(month)
    if not month_num:
        return await ctx.respond("Invalid month.", ephemeral=True)
    now = datetime.utcnow()
    try:
        target = datetime(now.year, month_num, day, 0, 0)
    except ValueError:
        return await ctx.respond("Invalid date.", ephemeral=True)
    if target.date() < now.date():
        try:
            target = datetime(now.year + 1, month_num, day, 0, 0)
        except ValueError:
            return await ctx.respond("Invalid date.", ephemeral=True)
    date_str = target.strftime("%Y-%m-%d")
    await add_scheduled_prize(ctx.guild, title, ctx.channel.id, content, date_str)
    await log_to_guild_bot_channel(ctx.guild, f"[PRIZE] Scheduled prize '{title}' on {date_str} by {ctx.author.mention} for {ctx.channel.mention}.")
    await ctx.respond(f"Scheduled for {date_str} (triggers on first Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC).", ephemeral=True)

@bot.slash_command(name="prize_announce")
async def prize_announce(ctx, member: discord.Option(discord.Member, required=True), prize: discord.Option(str, "Prize title", required=True)):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    guild = ctx.guild
    cfg = await ensure_guild_config(guild)
    rarity = cfg.get("prize_defs", {}).get(prize)
    if not rarity:
        return await ctx.respond("Prize not defined.", ephemeral=True)
    dead_role_id = cfg.get("dead_chat_role_id", 0)
    dead_role = guild.get_role(dead_role_id)
    role_mention = dead_role.mention if dead_role else "the Dead Chat role"
    text = cfg.get("prize_announce_text", DEFAULT_PRIZE_ANNOUNCE_MESSAGE)
    msg = text.format(
        winner=member.mention,
        gift=prize,
        role=role_mention,
        rarity=rarity
    )
    await ctx.channel.send(msg)
    await log_to_guild_bot_channel(guild, f"[PRIZE] Manual prize announce: {member.mention} → '{prize}' (rarity {rarity}) by {ctx.author.mention}.")
    await ctx.respond("announce sent.", ephemeral=True)

@bot.slash_command(name="sticky")
async def sticky(ctx, action: discord.Option(str, choices=["set", "clear"], required=True), text: discord.Option(str, required=False)):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    if action == "set":
        if not text:
            return await ctx.respond("Text required.", ephemeral=True)
        sticky_texts[ctx.channel.id] = text
        old_id = sticky_messages.get(ctx.channel.id)
        if old_id:
            try:
                msg = await ctx.channel.fetch_message(old_id)
                await msg.edit(content=text)
                await ctx.respond("Sticky updated.", ephemeral=True)
                await log_to_guild_bot_channel(ctx.guild, f"[STICKY] Updated sticky in {ctx.channel.mention} by {ctx.author.mention}.")
            except discord.NotFound:
                msg = await ctx.channel.send(text)
                sticky_messages[ctx.channel.id] = msg.id
                await ctx.respond("Sticky created.", ephemeral=True)
                await log_to_guild_bot_channel(ctx.guild, f"[STICKY] Created sticky in {ctx.channel.mention} by {ctx.author.mention}.")
        else:
            msg = await ctx.channel.send(text)
            sticky_messages[ctx.channel.id] = msg.id
            await ctx.respond("Sticky created.", ephemeral=True)
            await log_to_guild_bot_channel(ctx.guild, f"[STICKY] Created sticky in {ctx.channel.mention} by {ctx.author.mention}.")
        await save_stickies()
    else:
        old_id = sticky_messages.pop(ctx.channel.id, None)
        sticky_texts.pop(ctx.channel.id, None)
        if old_id:
            try:
                msg = await ctx.channel.fetch_message(old_id)
                await msg.delete()
            except discord.NotFound:
                pass
        await save_stickies()
        await ctx.respond("Sticky cleared.", ephemeral=True)
        await log_to_guild_bot_channel(ctx.guild, f"[STICKY] Cleared sticky in {ctx.channel.mention} by {ctx.author.mention}.")

@bot.slash_command(name="plague_infect", description="Schedule a contagious Dead Chat day")
async def plague_infect(
    ctx,
    month: discord.Option(str, "Month", required=False, choices=MONTH_CHOICES),
    day: discord.Option(int, "Day of month", required=False),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    now = datetime.utcnow()
    if month is None and day is None:
        target = now
    else:
        if month is None or day is None:
            return await ctx.respond("Provide both month and day, or leave both blank.", ephemeral=True)
        month_num = MONTH_TO_NUM.get(month)
        if not month_num:
            return await ctx.respond("Invalid month.", ephemeral=True)
        try:
            target = datetime(now.year, month_num, day, 0, 0)
        except ValueError:
            return await ctx.respond("Invalid date.", ephemeral=True)
        if target.date() < now.date():
            try:
                target = datetime(now.year + 1, month_num, day, 0, 0)
            except ValueError:
                return await ctx.respond("Invalid date.", ephemeral=True)
    date_str = target.strftime("%Y-%m-%d")
    guild_id = ctx.guild.id
    if guild_id not in plague_scheduled:
        plague_scheduled[guild_id] = []
    plague_scheduled[guild_id] = [{"date": date_str}]
    cfg = await get_guild_config(ctx.guild)
    updates = {"plague_scheduled": plague_scheduled[guild_id]}
    await _update_guild_config(ctx, updates, "plague schedule")
    await log_to_guild_bot_channel(ctx.guild, f"[PLAGUE] Plague scheduled for {date_str} by {ctx.author.mention}.")
    if month is None:
        await ctx.respond(f"Plague set for today. First Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC will be infected.", ephemeral=True)
    else:
        await ctx.respond(f"Plague scheduled for {date_str}. First Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC will be infected.", ephemeral=True)

@bot.slash_command(name="add_channel_welcome")
async def add_channel_welcome(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"welcome_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "welcome channel")
    if cfg:
        await ctx.respond(f"Set welcome channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_birthday_announce")
async def add_channel_birthday_announce(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"birthday_announce_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "birthday announce channel")
    if cfg:
        await ctx.respond(f"Set birthday announce channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_twitch_announce")
async def add_channel_twitch_announce(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"twitch_announce_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "twitch announce channel")
    if cfg:
        await ctx.respond(f"Set twitch announce channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_dead_chat_triggers")
async def add_channel_dead_chat_triggers(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    cfg = await get_guild_config(ctx.guild)
    ids = cfg.get("dead_chat_channel_ids", [])
    if channel.id in ids:
        await ctx.respond("Already added.", ephemeral=True)
        return
    ids.append(channel.id)
    updates = {"dead_chat_channel_ids": ids}
    new_cfg = await _update_guild_config(ctx, updates, "dead chat trigger channel")
    if new_cfg:
        await ctx.respond(f"Added {channel.mention} to dead chat triggers.", ephemeral=True)

@bot.slash_command(name="add_channel_prize_announce")
async def add_channel_prize_announce(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"prize_announce_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "prize announce channel")
    if cfg:
        await ctx.respond(f"Set prize announce channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_member_log")
async def add_channel_member_log(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"mod_log_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "member log channel")
    if cfg:
        await ctx.respond(f"Set member log channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_bot_log")
async def add_channel_bot_log(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"bot_log_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "bot log channel")
    if cfg:
        await ctx.respond(f"Set bot log channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_prize_drop")
async def add_channel_prize_drop(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    updates = {"prize_drop_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "prize drop channel")
    if cfg:
        await ctx.respond(f"Set prize drop channel to {channel.mention}", ephemeral=True)

@bot.slash_command(name="add_channel_auto_delete")
async def add_channel_auto_delete(ctx, channel: discord.Option(discord.TextChannel, required=True)):
    cfg = await ensure_guild_config(ctx.guild)
    ids = cfg.get("auto_delete_channel_ids", [])
    if channel.id in ids:
        await ctx.respond("Already added.", ephemeral=True)
        return

    ids.append(channel.id)
    updates = {"auto_delete_channel_ids": ids}
    new_cfg = await _update_guild_config(ctx, updates, "auto delete channel")
    if new_cfg:
        await ctx.respond(f"Added {channel.mention} to auto delete channels.", ephemeral=True)

@bot.slash_command(name="add_role_active")
async def add_role_active(ctx, role: discord.Option(discord.Role, required=True)):
    updates = {"active_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "active role")
    if cfg:
        await ctx.respond(f"Set active role to {role.mention}", ephemeral=True)

@bot.slash_command(name="add_role_birthday")
async def add_role_birthday(ctx, role: discord.Option(discord.Role, required=True)):
    updates = {"birthday_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "birthday role")
    if cfg:
        await ctx.respond(f"Set birthday role to {role.mention}", ephemeral=True)

@bot.slash_command(name="add_role_dead_chat")
async def add_role_dead_chat(ctx, role: discord.Option(discord.Role, required=True)):
    updates = {"dead_chat_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "dead chat role")
    if cfg:
        await ctx.respond(f"Set dead chat role to {role.mention}", ephemeral=True)

@bot.slash_command(name="add_role_infected")
async def add_role_infected(ctx, role: discord.Option(discord.Role, required=True)):
    updates = {"infected_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "infected role")
    if cfg:
        await ctx.respond(f"Set infected role to {role.mention}", ephemeral=True)

@bot.slash_command(name="add_role_member_join")
async def add_role_member_join(ctx, role: discord.Option(discord.Role, required=True)):
    updates = {"member_join_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "member join role")
    if cfg:
        await ctx.respond(f"Set member join role to {role.mention}", ephemeral=True)

@bot.slash_command(name="add_role_bot_join")
async def add_role_bot_join(ctx, role: discord.Option(discord.Role, required=True)):
    updates = {"bot_join_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "bot join role")
    if cfg:
        await ctx.respond(f"Set bot join role to {role.mention}", ephemeral=True)

@bot.slash_command(name="add_text_birthday")
async def add_text_birthday(ctx, text: discord.Option(str, required=True)):
    updates = {"birthday_text": text}
    cfg = await _update_guild_config(ctx, updates, "birthday text")
    if cfg:
        await ctx.respond("Set birthday text.", ephemeral=True)

@bot.slash_command(name="add_text_twitch")
async def add_text_twitch(ctx, text: discord.Option(str, required=True)):
    updates = {"twitch_live_text": text}
    cfg = await _update_guild_config(ctx, updates, "twitch text")
    if cfg:
        await ctx.respond("Set twitch live text.", ephemeral=True)

@bot.slash_command(name="add_text_plague")
async def add_text_plague(ctx, text: discord.Option(str, required=True)):
    updates = {"plague_outbreak_text": text}
    cfg = await _update_guild_config(ctx, updates, "plague text")
    if cfg:
        await ctx.respond("Set plague outbreak text.", ephemeral=True)

@bot.slash_command(name="add_twitch_notification")
async def add_twitch_notification(ctx, twitch_channel: discord.Option(str, required=True), notification_channel: discord.Option(discord.TextChannel, required=False)):
    cfg = await get_guild_config(ctx.guild)
    configs = cfg.get("twitch_configs", [])
    low_name = twitch_channel.lower()
    if any(tc["username"].lower() == low_name for tc in configs):
        await ctx.respond("Twitch channel already added.", ephemeral=True)
        return
    if notification_channel is None:
        announce_id = cfg.get("twitch_announce_channel_id", 0)
        if announce_id == 0:
            await ctx.respond("Set default twitch announce channel first with /add_channel_twitch_announce.", ephemeral=True)
            return
    else:
        announce_id = notification_channel.id
    configs.append({"username": twitch_channel, "announce_channel_id": announce_id})
    updates = {"twitch_configs": configs}
    new_cfg = await _update_guild_config(ctx, updates, "twitch notification")
    if new_cfg:
        await ctx.respond(f"Added twitch notification for {twitch_channel} to <#{announce_id}>.", ephemeral=True)

@bot.slash_command(name="add_prize")
async def add_prize(ctx, title: discord.Option(str, required=True), drop_rate: discord.Option(str, choices=["Common", "Uncommon", "Rare"], required=True)):
    cfg = await get_guild_config(ctx.guild)
    defs = cfg.get("prize_defs", {})
    if title in defs:
        await ctx.respond("Prize title already exists.", ephemeral=True)
        return
    defs[title] = drop_rate
    updates = {"prize_defs": defs}
    new_cfg = await _update_guild_config(ctx, updates, "prize")
    if new_cfg:
        await ctx.respond(f"Added prize '{title}' with drop rate {drop_rate}.", ephemeral=True)

@bot.slash_command(name="set_auto_delete_delay")
async def set_auto_delete_delay(ctx, seconds: discord.Option(int, required=True)):
    updates = {"auto_delete_delay_seconds": seconds}
    cfg = await _update_guild_config(ctx, updates, "auto delete delay")
    if cfg:
        await ctx.respond(f"Set auto delete delay to {seconds} seconds.", ephemeral=True)

@bot.slash_command(name="add_auto_delete_ignore")
async def add_auto_delete_ignore(ctx, phrase: discord.Option(str, required=True)):
    cfg = await get_guild_config(ctx.guild)
    phrases = cfg.get("auto_delete_ignore_phrases", [])
    if phrase in phrases:
        await ctx.respond("Phrase already added.", ephemeral=True)
        return
    phrases.append(phrase)
    updates = {"auto_delete_ignore_phrases": phrases}
    new_cfg = await _update_guild_config(ctx, updates, "auto delete ignore phrase")
    if new_cfg:
        await ctx.respond(f"Added ignore phrase '{phrase}'.", ephemeral=True)


############### ON_READY & BOT START ###############
bot.run(os.getenv("TOKEN"))
