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
import discord
import os
import asyncio
import aiohttp
import json
import traceback
import sys
import asyncpg
import urllib.request
import random as pyrandom
import gspread
from datetime import datetime, timedelta
from discord import TextChannel
from discord.ui import Select
from google.oauth2.service_account import Credentials
from datetime import datetime


############### CONSTANTS & CONFIG ###############
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.voice_states = True

DEBUG_GUILD_ID = int(os.getenv("DEBUG_GUILD_ID", "0"))
bot = discord.Bot(intents=intents, debug_guilds=[DEBUG_GUILD_ID])

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

GOOGLE_CREDS_RAW = os.getenv("GOOGLE_CREDENTIALS")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
gc = None
if GOOGLE_CREDS_RAW and SHEET_ID:
    try:
        creds_dict = json.loads(GOOGLE_CREDS_RAW)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)
        print("QOTD: Google Sheets client initialized.")
    except Exception as e:
        print("QOTD init error:", repr(e))
        traceback.print_exc()
else:
    print("QOTD disabled: missing GOOGLE_CREDENTIALS or GOOGLE_SHEET_ID")

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

ICON_DEFAULT_URL = os.getenv("ICON_DEFAULT_URL", "")
ICON_CHRISTMAS_URL = os.getenv("ICON_CHRISTMAS_URL", "")
ICON_HALLOWEEN_URL = os.getenv("ICON_HALLOWEEN_URL", "")

THEME_CHRISTMAS_ROLES = {
    "Sandy Claws": "Admin",
    "Grinch": "Original Member",
    "Cranberry": "Member",
    "Christmas": "Bots",
}

THEME_HALLOWEEN_ROLES = {
    "Cauldron": "Admin",
    "Candy": "Original Member",
    "Witchy": "Member",
    "Halloween": "Bots",
}

THEME_CHRISTMAS_EMOJIS_RAW = os.getenv("THEME_CHRISTMAS_EMOJIS", "[]")
THEME_HALLOWEEN_EMOJIS_RAW = os.getenv("THEME_HALLOWEEN_EMOJIS", "[]")

try:
    THEME_EMOJI_CONFIG = {
        "christmas": json.loads(THEME_CHRISTMAS_EMOJIS_RAW) if THEME_CHRISTMAS_EMOJIS_RAW else [],
        "halloween": json.loads(THEME_HALLOWEEN_EMOJIS_RAW) if THEME_HALLOWEEN_EMOJIS_RAW else [],
    }
except Exception:
    THEME_EMOJI_CONFIG = {
        "christmas": [],
        "halloween": [],
    }

############### GLOBAL STATE / STORAGE ###############
guild_configs: dict[int, dict] = {}
db_pool: asyncpg.Pool | None = None

twitch_access_token: str | None = None
twitch_live_state: dict[str, bool] = {}
all_twitch_channels: set[str] = set()

last_activity: dict[int, dict[int, str]] = {}

dead_current_holders: dict[int, int | None] = {}
dead_last_notice_message_ids: dict[int, int | None] = {}
dead_last_win_time: dict[int, datetime] = {}
deadchat_last_times: dict[int, str] = {}

sticky_messages: dict[int, int] = {}
sticky_texts: dict[int, str] = {}

pending_member_joins: list[dict] = []

plague_scheduled: dict[int, list[dict]] = {}
infected_members: dict[int, dict[int, str]] = {}

prize_defs: dict[int, dict[str, str]] = {}
scheduled_prizes: dict[int, list[dict]] = {}


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
        # Sticky messages
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sticky_data (
            channel_id BIGINT PRIMARY KEY,
            text TEXT,
            message_id BIGINT
        );
        """)

        # QOTD settings (with optional ping role)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS qotd_settings (
            guild_id BIGINT PRIMARY KEY,
            channel_id BIGINT,
            enabled BOOLEAN NOT NULL DEFAULT FALSE,
            ping_role_id BIGINT DEFAULT 0
        );
        """)

        # Migration for older DBs that don't have ping_role_id yet
        await conn.execute("""
        ALTER TABLE qotd_settings
        ADD COLUMN IF NOT EXISTS ping_role_id BIGINT DEFAULT 0;
        """)

async def get_theme_settings(guild_id: int) -> dict:
    """
    Returns theme settings for a guild:
    { 'enabled': bool, 'mode': 'auto'|'none'|'halloween'|'christmas' }
    Defaults: enabled=True, mode='auto'
    """
    if db_pool is None:
        return {"enabled": True, "mode": "auto"}

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT enabled, mode FROM theme_settings WHERE guild_id = $1",
            guild_id,
        )

    if row is None:
        return {"enabled": True, "mode": "auto"}

    return {
        "enabled": bool(row["enabled"]),
        "mode": (row["mode"] or "auto"),
    }


async def set_theme_enabled(guild_id: int, enabled: bool):
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO theme_settings (guild_id, enabled, mode)
            VALUES ($1, $2, 'auto')
            ON CONFLICT (guild_id)
            DO UPDATE SET enabled = EXCLUDED.enabled;
            """,
            guild_id,
            enabled,
        )


async def set_theme_mode(guild_id: int, mode: str):
    """
    mode must be one of: 'auto', 'none', 'halloween', 'christmas'
    """
    if db_pool is None:
        return
    if mode not in ("auto", "none", "halloween", "christmas"):
        # silently ignore bad values in case someone passes nonsense
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO theme_settings (guild_id, enabled, mode)
            VALUES ($1, TRUE, $2)
            ON CONFLICT (guild_id)
            DO UPDATE SET mode = EXCLUDED.mode;
            """,
            guild_id,
            mode,
        )


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
    if db_pool is None or guild is None:
        return {}

    cfg = await get_guild_config(guild)

    # If config exists (i.e., row exists), return it
    # We tell existence by checking if the DB row existed!
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM guild_configs WHERE guild_id=$1)", guild.id
        )

    if exists:
        return cfg

    # If row does NOT exist, insert it
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO guild_configs (guild_id) VALUES ($1) ON CONFLICT DO NOTHING",
            guild.id
        )

    # load again after inserting
    return await get_guild_config(guild)

async def get_config_role(guild: discord.Guild, key: str) -> discord.Role | None:
    cfg = await ensure_guild_config(guild)
    if not cfg:
        return None
    rid = cfg.get(key)
    if not rid:
        return None
    return guild.get_role(rid)

async def get_config_channel(guild, config_key):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM guild_configs WHERE guild_id=$1", guild.id
        )
        if not row:
            return None

        channel_id = row.get(config_key)
        if not channel_id:
            return None

        channel = guild.get_channel(channel_id)
        return channel

async def log_to_guild_mod_log(guild: discord.Guild, content: str):
    """Log moderation-related info to Railway logs only."""
    gid = guild.id if guild else "?"
    print(f"[MOD-LOG][GUILD {gid}] {content}")


async def log_to_guild_bot_channel(guild: discord.Guild, content: str):
    """Log bot-related info to Railway logs only."""
    gid = guild.id if guild else "?"
    print(f"[BOT-LOG][GUILD {gid}] {content}")


async def log_to_bot_channel(content: str):
    """Global logs to Railway only."""
    print(f"[BOT-LOG][GLOBAL] {content}")

async def log_exception(tag: str, exc: Exception):
    """Log exceptions to Railway logs only."""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    print(f"[EXCEPTION] {tag}: {exc}\n{tb}")

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

def build_mm_dd(month_name: str, day: int) -> str | None:
    month_num = MONTH_TO_NUM.get(month_name)
    if not month_num or not (1 <= day <= 31):
        return None
    return f"{month_num:02d}-{day:02d}"


async def set_birthday(guild_id: int, user_id: int, mm_dd: str):
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO birthdays (guild_id, user_id, mm_dd)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET mm_dd = EXCLUDED.mm_dd;
            """,
            guild_id,
            user_id,
            mm_dd,
        )


async def remove_birthday(guild_id: int, user_id: int) -> bool:
    if db_pool is None:
        return False
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM birthdays WHERE guild_id = $1 AND user_id = $2;",
            guild_id,
            user_id,
        )
    # result is like "DELETE 0" or "DELETE 1"
    return result.split()[-1] != "0"

async def get_guild_qotd_settings(guild_id: int):
    """
    Returns QOTD settings for a guild:
    {
        "channel_id": int,
        "enabled": bool,
        "ping_role_id": int  # 0 means "no role"
    }
    """
    if db_pool is None:
        return None

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT channel_id, enabled, ping_role_id FROM qotd_settings WHERE guild_id = $1",
            guild_id,
        )

    if not row:
        return None

    # Some old rows might have NULL for ping_role_id; treat that as 0
    ping_role_id = row.get("ping_role_id", 0) if isinstance(row, dict) else row["ping_role_id"]
    if ping_role_id is None:
        ping_role_id = 0

    return {
        "channel_id": int(row["channel_id"]) if row["channel_id"] else 0,
        "enabled": bool(row["enabled"]),
        "ping_role_id": int(ping_role_id or 0),
    }

async def get_guild_vc_links(guild_id: int) -> dict[int, int]:
    """
    Returns {channel_id: role_id} for this guild.
    """
    if db_pool is None:
        return {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT channel_id, role_id FROM vc_role_links WHERE guild_id = $1",
            guild_id,
        )
    return {int(r["channel_id"]): int(r["role_id"]) for r in rows}


async def set_vc_role_link(guild_id: int, channel_id: int, role_id: int):
    """
    Link a voice channel to a role (join = add, leave = remove).
    """
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO vc_role_links (guild_id, channel_id, role_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id, channel_id)
            DO UPDATE SET role_id = EXCLUDED.role_id;
            """,
            guild_id,
            channel_id,
            role_id,
        )


async def remove_vc_role_link(guild_id: int, channel_id: int) -> bool:
    """
    Remove the VC→role link for this channel.
    Returns True if something was deleted.
    """
    if db_pool is None:
        return False
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM vc_role_links WHERE guild_id = $1 AND channel_id = $2;",
            guild_id,
            channel_id,
        )
    # result is like "DELETE 0" or "DELETE 1"
    return result.split()[-1] != "0"
    

async def get_qotd_sheet_and_tab():
    """
    Returns (worksheet, season_name) based on the current month.
    Tabs supported: 'Regular', 'Fall Season', 'Christmas'
    """
    if gc is None or not SHEET_ID:
        raise RuntimeError("QOTD is not configured (Google credentials or sheet id missing).")

    sh = gc.open_by_key(SHEET_ID)
    today = datetime.utcnow()

    # Seasonal logic
    if 10 <= today.month <= 11:
        tab = "Fall Season"
    elif today.month == 12:
        tab = "Christmas"
    else:
        tab = "Regular"

    # Try seasonal tab, fall back to first sheet
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.sheet1
        tab = ws.title
        print(f"QOTD: worksheet '{tab}' not found; using first sheet instead.")

    return ws, tab


async def get_question_of_the_day() -> str | None:
    """
    Pull an unused QOTD from the Google Sheet, mark it as used, and return it.
    Automatically switches seasonal tabs.
    """
    worksheet, season = await get_qotd_sheet_and_tab()

    all_vals = worksheet.get_all_values()
    if len(all_vals) < 2:
        print("QOTD: Sheet empty or missing questions.")
        return None

    # Skip header
    questions = all_vals[1:]
    unused = []

    for row in questions:
        # Normalize rows to 2 columns minimum
        row += [""] * (2 - len(row))

        status_a = row[0].strip()
        status_b = row[1].strip()
        question_text = row[1].strip() or row[0].strip()

        # Add to unused list if unused & not blank
        if question_text and (not status_a or not status_b):
            unused.append(row)

    if not unused:
        print("QOTD: All questions used; resetting sheet.")
        worksheet.update("A2:B", [[""] * 2 for _ in range(len(questions))])
        unused = questions

    chosen = pyrandom.choice(unused)
    chosen += [""] * (2 - len(chosen))
    question = chosen[1].strip() or chosen[0].strip()

    if not question:
        print("QOTD: Chosen row empty, skipping.")
        return None

    # Mark used
    row_idx = questions.index(chosen) + 2  # +2 → header offset
    status_col = "A" if chosen[1].strip() else "B"

    try:
        worksheet.update(
            f"{status_col}{row_idx}",
            [[f"Used {datetime.utcnow().strftime('%Y-%m-%d')}"]]
        )
    except Exception as e:
        print("QOTD: Failed marking used:", repr(e))

    return question

async def post_daily_qotd():
    """Pulls the daily QOTD from Google Sheets and posts to all servers."""
    try:
        question = await get_question_of_the_day()
        if not question:
            print("No QOTD available.")
            return
    except Exception as e:
        await log_exception("QOTD Google Sheets Fetch Error", e)
        return

    for guild in bot.guilds:
        try:
            settings = await get_guild_qotd_settings(guild.id)
            if not settings or not settings["enabled"]:
                continue

            channel = guild.get_channel(settings["channel_id"])
            if not channel:
                continue

            embed = discord.Embed(
                title="❓ Question of the Day",
                description=question,
                color=discord.Color.gold(),
            )

            # Optional ping role
            ping_role_id = settings.get("ping_role_id", 0) or 0
            ping_role = guild.get_role(ping_role_id) if ping_role_id else None

            if ping_role:
                allowed = discord.AllowedMentions(roles=True)
                await channel.send(
                    content=ping_role.mention,
                    embed=embed,
                    allowed_mentions=allowed,
                )
            else:
                await channel.send(embed=embed)

        except Exception as e:
            await log_exception(f"post_daily_qotd guild {guild.id}", e)


async def get_guild_birthdays(guild_id: int) -> dict[str, str]:
    if db_pool is None:
        return {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, mm_dd FROM birthdays WHERE guild_id = $1;",
            guild_id,
        )
    return {str(r["user_id"]): r["mm_dd"] for r in rows}


async def get_birthday_public_location(guild_id: int) -> tuple[int, int] | None:
    if db_pool is None:
        return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT channel_id, message_id FROM birthday_public_messages WHERE guild_id = $1;",
            guild_id,
        )
    if row:
        return int(row["channel_id"]), int(row["message_id"])
    return None


async def set_birthday_public_location(guild_id: int, channel_id: int, message_id: int):
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO birthday_public_messages (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE
            SET channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id;
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def build_birthday_embed(guild: discord.Guild) -> discord.Embed:
    birthdays = await get_guild_birthdays(guild.id)
    lines: list[str] = []

    for user_id, mm_dd in sorted(birthdays.items(), key=lambda x: x[1]):
        member = guild.get_member(int(user_id))
        if member:
            lines.append(f"{member.mention} — `{mm_dd}`")
        else:
            lines.append(f"<@{user_id}> — `{mm_dd}`")

    description = "\n".join(lines) if lines else "No birthdays yet!"
    description += (
        "\n\n**SHARE YOUR BIRTHDAY**\n"
        "• </birthday_set:0> - Add your birthday to the server’s shared birthday list."
    )

    embed = discord.Embed(
        title="OUR BIRTHDAYS!",
        description=description,
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="Messages in this channel may auto-delete after a while.")
    return embed


async def update_birthday_list_message(guild: discord.Guild):
    loc = await get_birthday_public_location(guild.id)
    if not loc:
        return
    ch_id, msg_id = loc
    channel = guild.get_channel(ch_id)
    if not channel or not isinstance(channel, discord.TextChannel):
        return
    try:
        msg = await channel.fetch_message(msg_id)
        embed = await build_birthday_embed(guild)
        await msg.edit(embed=embed, allowed_mentions=discord.AllowedMentions(users=True))
    except Exception as e:
        await log_exception("update_birthday_list_message", e)


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


# ---------- THEME HELPERS ----------

def find_role_by_name(guild: discord.Guild, name: str) -> discord.Role | None:
    name_lower = name.lower()
    for role in guild.roles:
        if role.name.lower() == name_lower:
            return role
    return None


async def clear_theme_roles(guild: discord.Guild) -> list[str]:
    removed: list[str] = []
    seasonal_names = set(THEME_CHRISTMAS_ROLES.keys()) | set(THEME_HALLOWEEN_ROLES.keys())

    for role in guild.roles:
        if role.name in seasonal_names:
            for member in guild.members:
                if role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Seasonal theme clear")
                        if role.name not in removed:
                            removed.append(role.name)
                    except Exception as e:
                        await log_exception("clear_theme_roles", e)

    return removed


async def apply_theme_roles(guild: discord.Guild, mapping: dict[str, str]) -> list[str]:
    added: list[str] = []

    for seasonal_name, base_name in mapping.items():
        base_role = find_role_by_name(guild, base_name)
        if not base_role:
            continue

        seasonal_role = find_role_by_name(guild, seasonal_name)
        if not seasonal_role:
            try:
                seasonal_role = await guild.create_role(
                    name=seasonal_name,
                    reason="Creating seasonal theme role",
                )
            except Exception as e:
                await log_exception("apply_theme_roles_create", e)
                continue

        for member in guild.members:
            if base_role in member.roles and seasonal_role not in member.roles:
                try:
                    await member.add_roles(seasonal_role, reason="Seasonal theme")
                except Exception as e:
                    await log_exception("apply_theme_roles_add", e)

        added.append(seasonal_name)

    return added


async def apply_theme_icon(guild: discord.Guild, url: str | None):
    if not url:
        return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    img_bytes = await resp.read()
                    await guild.edit(icon=img_bytes, reason="Seasonal theme icon")
    except Exception as e:
        await log_exception("apply_theme_icon", e)

async def clear_theme_emojis(guild: discord.Guild) -> list[str]:
    names = set()
    for items in THEME_EMOJI_CONFIG.values():
        for entry in items:
            name = entry.get("name")
            if name:
                names.add(name.lower())
    removed = []
    if not names:
        return removed
    for emoji in list(guild.emojis):
        if emoji.name.lower() in names:
            try:
                await emoji.delete(reason="Seasonal theme emoji clear")
                removed.append(emoji.name)
            except Exception as e:
                await log_exception("clear_theme_emojis", e)
    return removed


async def apply_theme_emojis(guild: discord.Guild, key: str) -> list[str]:
    items = THEME_EMOJI_CONFIG.get(key) or []
    added = []
    if not items:
        return added
    existing = {e.name.lower() for e in guild.emojis}
    async with aiohttp.ClientSession() as session:
        for entry in items:
            name = entry.get("name")
            url = entry.get("url")
            if not name or not url:
                continue
            if name.lower() in existing:
                continue
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.read()
                emoji = await guild.create_custom_emoji(name=name, image=data, reason="Seasonal theme emoji")
                added.append(emoji.name)
                existing.add(emoji.name.lower())
            except Exception as e:
                await log_exception("apply_theme_emojis", e)
    return added

async def apply_theme_for_today_with_mode(
    guild: discord.Guild,
    today: str | None = None,
    mode: str = "auto",
):
    if today is None:
        from datetime import datetime
        today = datetime.utcnow().strftime("%m-%d")

    removed_roles: list[str] = []
    removed_emojis: list[str] = []
    added_roles: list[str] = []
    added_emojis: list[str] = []

    if mode == "none":
        removed_roles = await clear_theme_roles(guild)
        removed_emojis = await clear_theme_emojis(guild)
        await apply_theme_icon(guild, ICON_DEFAULT_URL)
        await log_to_bot_channel(
            f"[THEME] guild={guild.id} date={today} mode=none roles_cleared={removed_roles} emojis_cleared={removed_emojis}"
        )
        return "none", removed_roles, removed_emojis, added_roles, added_emojis

    if mode == "halloween":
        removed_roles = await clear_theme_roles(guild)
        removed_emojis = await clear_theme_emojis(guild)
        added_roles = await apply_theme_roles(guild, THEME_HALLOWEEN_ROLES)
        added_emojis = await apply_theme_emojis(guild, "halloween")
        await apply_theme_icon(guild, ICON_HALLOWEEN_URL or ICON_DEFAULT_URL)
        await log_to_bot_channel(
            f"[THEME] guild={guild.id} date={today} mode=halloween roles_cleared={removed_roles} emojis_cleared={removed_emojis} roles_added={added_roles} emojis_added={added_emojis}"
        )
        return "halloween", removed_roles, removed_emojis, added_roles, added_emojis

    if mode == "christmas":
        removed_roles = await clear_theme_roles(guild)
        removed_emojis = await clear_theme_emojis(guild)
        added_roles = await apply_theme_roles(guild, THEME_CHRISTMAS_ROLES)
        added_emojis = await apply_theme_emojis(guild, "christmas")
        await apply_theme_icon(guild, ICON_CHRISTMAS_URL or ICON_DEFAULT_URL)
        await log_to_bot_channel(
            f"[THEME] guild={guild.id} date={today} mode=christmas roles_cleared={removed_roles} emojis_cleared={removed_emojis} roles_added={added_roles} emojis_added={added_emojis}"
        )
        return "christmas", removed_roles, removed_emojis, added_roles, added_emojis

    mode_auto, removed_roles, removed_emojis, added_roles, added_emojis = await apply_theme_for_today(guild, today)
    return mode_auto, removed_roles, removed_emojis, added_roles, added_emojis

async def apply_theme_for_today(guild: discord.Guild, mm_dd: str):
    month_str, _ = mm_dd.split("-")
    month = int(month_str)

    if month == 10:
        mode = "halloween"
    elif month == 12:
        mode = "christmas"
    else:
        mode = "none"

    removed_roles = await clear_theme_roles(guild)
    removed_emojis = await clear_theme_emojis(guild)
    added_roles: list[str] = []
    added_emojis: list[str] = []

    if mode == "halloween":
        added_roles = await apply_theme_roles(guild, THEME_HALLOWEEN_ROLES)
        added_emojis = await apply_theme_emojis(guild, "halloween")
        await apply_theme_icon(guild, ICON_HALLOWEEN_URL or ICON_DEFAULT_URL)
    elif mode == "christmas":
        added_roles = await apply_theme_roles(guild, THEME_CHRISTMAS_ROLES)
        added_emojis = await apply_theme_emojis(guild, "christmas")
        await apply_theme_icon(guild, ICON_CHRISTMAS_URL or ICON_DEFAULT_URL)
    else:
        await apply_theme_icon(guild, ICON_DEFAULT_URL)

    await log_to_bot_channel(
        f"[THEME] guild={guild.id} date={mm_dd} mode={mode} roles_cleared={removed_roles} emojis_cleared={removed_emojis} roles_added={added_roles} emojis_added={added_emojis}"
    )

    return mode, removed_roles, removed_emojis, added_roles, added_emojis


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

class SetupPagerView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.active_page: str | None = None  # "features", "commands", or None (home)

    def make_home_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Admin Bot Setup",
            description="Use the buttons below to view **Features** or the **Commands Checklist**.",
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="Admin-only • /setup")
        return embed

    def make_features_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Admin Bot Features",
            description="Overview of what this bot does for your server.",
            color=discord.Color.blurple(),
        )

        embed.add_field(
            name="Onboarding & Roles",
            value=(
                "• Welcome messages for new members\n"
                "• Delayed member-join role\n"
                "• Auto roles for new bots\n"
                "• Birthday role + birthday announcements\n"
                "• Public birthday list message (auto-updating)\n"
                "• Optional Active Member role based on activity"
            ),
            inline=False,
        )

        embed.add_field(
            name="Birthdays & QOTD",
            value=(
                "• Members can register their birthdays\n"
                "• Shared birthday list + refreshable public birthday message\n"
                "• Daily Question of the Day from Google Sheets\n"
                "• Optional QOTD ping role for people who want pings\n"
                "• Admin controls for sending birthday/QOTD announcements"
            ),
            inline=False,
        )

        embed.add_field(
            name="Dead Chat, Plague & Prizes",
            value=(
                "• Dead Chat idle tracking & role steals\n"
                "• Scheduled plague days with infection role\n"
                "• Scheduled prize drops linked to Dead Chat\n"
                "• Prize claim buttons with announcements\n"
                "• Channel rescanning for accurate Dead Chat timestamps"
            ),
            inline=False,
        )

        embed.add_field(
            name="Activity, Cleanup & Sticky",
            value=(
                "• Tracks last activity per member\n"
                "• Removes Active role after inactivity\n"
                "• Auto-delete channels with ignore phrases\n"
                "• Sticky messages that stay on top in a channel\n"
                "• Mark members active manually when needed"
            ),
            inline=False,
        )

        embed.add_field(
            name="Twitch & Logging",
            value=(
                "• Watch multiple Twitch channels\n"
                "• Live notifications with customizable text\n"
                "• Member join/leave/ban/kick logs\n"
                "• Bot join/leave/ban logs"
            ),
            inline=False,
        )

        embed.add_field(
            name="Voice & Seasonal Themes",
            value=(
                "• Voice-channel ↔ role links (VC join = get role)\n"
                "• Automatic seasonal themes (Halloween / Christmas)\n"
                "• Manual theme mode: auto / none / halloween / christmas"
            ),
            inline=False,
        )

        embed.add_field(
            name="Utility & Admin Tools",
            value=(
                "• Setup dashboard for admins (/setup)\n"
                "• Send or edit messages as the bot\n"
                "• Config viewers for both runtime and database state\n"
                "• Database connection test\n"
                "• Full setup checklist available in the setup UI"
            ),
            inline=False,
        )

        embed.set_footer(text="Use the buttons below to switch pages.")
        return embed

    def make_commands_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Admin Bot Setup Checklist",
            description="Run these commands to configure all features.",
            color=discord.Color.blurple(),
        )

        embed.add_field(
            name="SETUP",
            value=(
                "> `/setup` - Show the full Admin Bot setup checklist."
            ),
            inline=False,
        )

        embed.add_field(
            name="CHANNELS",
            value=(
                "> `/welcome_channel` - Set the welcome channel.\n"
                "> `/birthday_announce_channel` - Set birthday announcements.\n"
                "> `/qotd_set_channel` - Choose the QOTD channel.\n"
                "> `/twitch_stream_channel` - Set default Twitch alert channel.\n"
                "> `/deadchat_trigger_channels` - Add Dead Chat channels.\n"
                "> `/prize_channel` - Set prize drop channel.\n"
                "> `/prize_announce_channel` - Set prize announcement channel.\n"
                "> `/log_channel_members` - Set member log channel.\n"
                "> `/auto_delete_channel` - Add auto-delete channels."
            ),
            inline=False,
        )

        embed.add_field(
            name="ROLES",
            value=(
                "> `/active_member_role` - Role for active members.\n"
                "> `/active_member_role_add` - Mark a member active right now.\n"
                "> `/birthday_role` - Role for birthdays.\n"
                "> `/deadchat_role` - Dead Chat holder role.\n"
                "> `/plague_role` - Plague infection role.\n"
                "> `/member_join_role` - Auto-assign on member join.\n"
                "> `/bot_join_role` - Auto-assign on bot join.\n"
                "> `/qotd_ping_role` - Set or clear the optional QOTD ping role."
            ),
            inline=False,
        )

        embed.add_field(
            name="BIRTHDAYS & QOTD",
            value=(
                "> `/birthday_set` - Members share their birthday.\n"
                "> `/birthday_set_for` - Admin sets a member's birthday.\n"
                "> `/birthday_remove` - Remove a member's birthday.\n"
                "> `/birthday_list` - View server birthday list.\n"
                "> `/birthday_public` - Create/refresh public birthday message.\n"
                "> `/birthday_announce_send` - Manually send a birthday message.\n"
                "> `/qotd_enable` - Enable daily QOTD.\n"
                "> `/qotd_disable` - Disable daily QOTD.\n"
                "> `/qotd_ping_role` - Set or clear the optional QOTD ping role."
            ),
            inline=False,
        )

        embed.add_field(
            name="TEXT & MESSAGES",
            value=(
                "> `/birthday_msg` - Set birthday announcement text.\n"
                "> `/twitch_msg` - Set Twitch alert text.\n"
                "> `/plague_msg` - Set plague outbreak message.\n"
                "> `/sticky_message` - Set or clear sticky messages.\n"
                "> `/send_msg` - Make the bot send a message.\n"
                "> `/edit_msg` - Edit a bot message."
            ),
            inline=False,
        )

        embed.add_field(
            name="AUTO DELETE & ACTIVITY",
            value=(
                "> `/auto_delete_delay` - Set deletion delay for auto-delete channels.\n"
                "> `/auto_delete_filters` - Add phrases that never get deleted.\n"
                "> `/active_member_role_add` - Mark a member active right now."
            ),
            inline=False,
        )

        embed.add_field(
            name="TWITCH",
            value="> `/twitch_channel` - Add a Twitch channel and announcement target.",
            inline=False,
        )

        embed.add_field(
            name="PRIZES, DEAD CHAT & PLAGUE",
            value=(
                "> `/prize_add` - Define a prize title + rarity.\n"
                "> `/prize_day` - Schedule or instantly drop a prize.\n"
                "> `/prize_list` - View scheduled prize drops.\n"
                "> `/prize_delete` - Delete a scheduled prize.\n"
                "> `/prize_announce_send` - Manually announce a prize winner.\n"
                "> `/plague_day` - Schedule a plague day.\n"
                "> `/deadchat_scan` - Rescan Dead Chat channels for last message."
            ),
            inline=False,
        )

        embed.add_field(
            name="VOICE CHANNEL ROLES",
            value=(
                "> `/vc_role_link` - Link a voice channel to a role.\n"
                "> `/vc_role_unlink` - Remove a VC → role link.\n"
                "> `/vc_role_list` - List all VC role links."
            ),
            inline=False,
        )

        embed.add_field(
            name="THEMES",
            value=(
                "> `/theme_enable` - Enable seasonal themes.\n"
                "> `/theme_disable` - Disable seasonal themes.\n"
                "> `/theme_mode` - Set mode: auto / none / halloween / christmas.\n"
                "> `/theme_update` - Force-refresh today's theme now."
            ),
            inline=False,
        )

        embed.add_field(
            name="CONFIG & DEBUG",
            value=(
                "> `/config_show` - Show the current config for this server.\n"
                "> `/config_db_show` - Show raw DB config row.\n"
                "> `/db_test` - Test connection to the Postgres database."
            ),
            inline=False,
        )

        embed.set_footer(text="Use this checklist to finish setup for each server.")
        return embed

    def refresh_button_styles(self):
        # Both gray on home (active_page is None)
        for child in self.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if child.custom_id == "setup_features":
                child.style = (
                    discord.ButtonStyle.primary
                    if self.active_page == "features"
                    else discord.ButtonStyle.secondary
                )
            elif child.custom_id == "setup_commands":
                child.style = (
                    discord.ButtonStyle.primary
                    if self.active_page == "commands"
                    else discord.ButtonStyle.secondary
                )

    @discord.ui.button(
        label="Features",
        style=discord.ButtonStyle.secondary,  # gray by default
        custom_id="setup_features",
    )
    async def features_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        self.active_page = "features"
        self.refresh_button_styles()
        embed = self.make_features_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(
        label="Commands",
        style=discord.ButtonStyle.secondary,  # gray by default
        custom_id="setup_commands",
    )
    async def commands_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        self.active_page = "commands"
        self.refresh_button_styles()
        embed = self.make_commands_embed()
        await interaction.response.edit_message(embed=embed, view=self)


############### AUTOCOMPLETE FUNCTIONS ###############


############### BACKGROUND TASKS & SCHEDULERS ###############
async def qotd_scheduler():
    """Posts the daily QOTD to every guild that has QOTD enabled."""
    await bot.wait_until_ready()

    TARGET_HOUR_UTC = 16      # <-- CHANGE THIS IF NEEDED
    TARGET_MINUTE = 0

    already_ran = False

    while not bot.is_closed():
        now = datetime.utcnow()

        # Trigger at the exact minute
        if now.hour == TARGET_HOUR_UTC and now.minute == TARGET_MINUTE:
            if not already_ran:
                await post_daily_qotd()
                already_ran = True
        else:
            already_ran = False

        await asyncio.sleep(30)

async def theme_scheduler():
    await bot.wait_until_ready()
    TARGET_HOUR_UTC = 9
    TARGET_MINUTE = 0
    while not bot.is_closed():
        now = datetime.utcnow()
        if now.hour == TARGET_HOUR_UTC and now.minute == TARGET_MINUTE:
            today = now.strftime("%m-%d")
            for guild in bot.guilds:
                try:
                    settings = await get_theme_settings(guild.id)
                    if not settings["enabled"]:
                        continue
                    mode = settings["mode"]
                    await apply_theme_for_today_with_mode(guild, today, mode)
                except Exception as e:
                    await log_exception(f"theme_scheduler_guild_{guild.id}", e)
            await asyncio.sleep(61)
        await asyncio.sleep(30)

async def birthday_checker():
    """Runs once a day and gives/removes the birthday role for each guild."""
    await bot.wait_until_ready()

    TARGET_HOUR_UTC = 15   # same time as your old bot
    TARGET_MINUTE = 0

    while not bot.is_closed():
        now = datetime.utcnow()

        if now.hour == TARGET_HOUR_UTC and now.minute == TARGET_MINUTE:
            today = now.strftime("%m-%d")

            for guild in bot.guilds:
                try:
                    cfg = await ensure_guild_config(guild)
                    birthday_role_id = cfg.get("birthday_role_id")
                    if not birthday_role_id:
                        continue

                    role = guild.get_role(birthday_role_id)
                    if not role:
                        continue

                    birthdays = await get_guild_birthdays(guild.id)

                    for member in guild.members:
                        mm_dd = birthdays.get(str(member.id))

                        # Give role if today is their birthday
                        if mm_dd == today:
                            if role not in member.roles:
                                await member.add_roles(role, reason="Birthday!")
                        # Remove role if their birthday is over
                        else:
                            if role in member.roles:
                                await member.remove_roles(role, reason="Birthday over")

                except Exception as e:
                    await log_exception(f"birthday_checker_guild_{guild.id}", e)

            # wait a bit so we don't trigger twice in the same minute
            await asyncio.sleep(61)

        await asyncio.sleep(30)

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
    await initialize_dead_chat()

    for guild in bot.guilds:
        cfg = await ensure_guild_config(guild)
        plague_scheduled[guild.id] = cfg.get("plague_scheduled", [])
        infected_members[guild.id] = cfg.get("infected_members", {})
        prize_defs[guild.id] = cfg.get("prize_defs", {})
        scheduled_prizes[guild.id] = cfg.get("prize_scheduled", [])
        for tc in cfg.get("twitch_configs", []):
            all_twitch_channels.add(tc["username"].lower())

    bot.loop.create_task(qotd_scheduler())
    bot.loop.create_task(theme_scheduler())
    bot.loop.create_task(birthday_checker())

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
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    """
    VC Role System:
    - When a member joins a linked VC, they get the configured role.
    - When they leave / move away from that VC, the role is removed.
    - Supports multiple VC→role links per guild.
    """
    guild = member.guild
    if guild is None:
        return
    if member.bot:
        # If you want bots to get VC roles too, remove this line.
        return

    links = await get_guild_vc_links(guild.id)
    if not links:
        return

    # For each configured VC link, make sure the member's roles match
    # whether they are currently in that specific VC.
    for channel_id, role_id in links.items():
        role = guild.get_role(role_id)
        if not role:
            continue

        in_this_vc = after.channel is not None and after.channel.id == channel_id
        has_role = role in member.roles

        # Joined that VC → ensure they have the role
        if in_this_vc and not has_role:
            try:
                await member.add_roles(role, reason="VC role link - joined voice channel")
                await log_to_guild_bot_channel(
                    guild,
                    f"[VC-ROLE] Gave {role.mention} to {member.mention} for joining <#{channel_id}>."
                )
            except Exception as e:
                await log_exception("on_voice_state_update_add_vc_role", e)

        # Not in that VC → ensure they do NOT have the role
        elif not in_this_vc and has_role:
            try:
                await member.remove_roles(role, reason="VC role link - left voice channel")
                await log_to_guild_bot_channel(
                    guild,
                    f"[VC-ROLE] Removed {role.mention} from {member.mention} (no longer in <#{channel_id}>)."
                )
            except Exception as e:
                await log_exception("on_voice_state_update_remove_vc_role", e)

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
@bot.slash_command(name="db_test", description="Test the bot's connection to the Postgres database.")
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


@bot.slash_command(
    name="deadchat_scan",
    description="Scan Dead Chat channels and refresh idle timestamps."
)
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
    await ctx.respond(
        f"Rescan complete — found latest message in {count} dead-chat channels and saved timestamps.",
        ephemeral=True
    )


@bot.slash_command(
    name="config_show",
    description="Show the current configuration for this server."
)
async def config_show(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    cfg = await get_guild_config(ctx.guild) or {}
    text = json.dumps(cfg, indent=2)
    if len(text) > 1900:
        text = text[:1900] + "\n...[truncated]"
    await ctx.respond(f"```json\n{text}\n```", ephemeral=True)


@bot.slash_command(
    name="config_db_show",
    description="Show the raw database configuration for this server."
)
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
    description="Show the full Admin Bot setup checklist."
)
async def setup(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)

    view = SetupPagerView()
    # active_page is None by default, so both buttons stay gray
    view.refresh_button_styles()
    embed = view.make_home_embed()

    await ctx.respond(embed=embed, view=view, ephemeral=True)


@bot.slash_command(
    name="birthday_announce_send",
    description="Manually send a birthday message for a member."
)
async def birthday_announce(ctx, member: discord.Option(discord.Member, "Member", required=True)):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("You need Administrator.", ephemeral=True)
    guild = ctx.guild
    cfg = await ensure_guild_config(guild)
    ch = await get_config_channel(guild, "birthday_announce_channel_id") or await get_config_channel(
        guild, "welcome_channel_id"
    )
    if not ch:
        return await ctx.respond("Announce channel not found.", ephemeral=True)
    text = cfg.get("birthday_text", DEFAULT_BIRTHDAY_TEXT)
    msg = text.replace("{mention}", member.mention)
    await ch.send(msg)
    await log_to_guild_bot_channel(guild, f"[BIRTHDAY] Manual birthday announce sent for {member.mention} by {ctx.author.mention}.")
    await ctx.respond(f"Sent birthday message for {member.mention}.", ephemeral=True)

@bot.slash_command(name="send_msg", description="Send a custom message as the bot in this channel")
async def send_msg(
    ctx,
    message: discord.Option(str, "Message to send (use \\n for new lines)", required=True),
):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("You need Administrator.", ephemeral=True)
    try:
        await ctx.channel.send(message.replace("\\n", "\n"))
        await ctx.respond("Message sent.", ephemeral=True)
    except Exception as e:
        await log_exception("send_msg", e)
        try:
            await ctx.respond("Failed to send message.", ephemeral=True)
        except:
            pass

@bot.slash_command(
    name="birthday_set",
    description="Share your birthday with the server."
)
async def birthday_set(
    ctx,
    month: discord.Option(str, choices=MONTH_CHOICES),
    day: discord.Option(int),
):
    if ctx.guild is None:
        return await ctx.respond("Use this in a server.", ephemeral=True)

    mm_dd = build_mm_dd(month, day)
    if not mm_dd:
        return await ctx.respond("Invalid date.", ephemeral=True)

    await set_birthday(ctx.guild.id, ctx.author.id, mm_dd)
    await update_birthday_list_message(ctx.guild)
    await ctx.respond(f"Birthday set to `{mm_dd}`!", ephemeral=True)


@bot.slash_command(
    name="birthday_set_for",
    description="Admin: add or change a member's birthday."
)
async def birthday_set_for(
    ctx,
    member: discord.Member,
    month: discord.Option(str, choices=MONTH_CHOICES),
    day: discord.Option(int),
):
    if ctx.guild is None:
        return await ctx.respond("Use this in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    mm_dd = build_mm_dd(month, day)
    if not mm_dd:
        return await ctx.respond("Invalid date.", ephemeral=True)

    await set_birthday(ctx.guild.id, member.id, mm_dd)
    await update_birthday_list_message(ctx.guild)
    await ctx.respond(f"Set {member.mention}'s birthday to `{mm_dd}`.", ephemeral=True)


@bot.slash_command(
    name="birthday_remove",
    description="Admin: remove a member's birthday."
)
async def birthday_remove(
    ctx,
    member: discord.Member,
):
    if ctx.guild is None:
        return await ctx.respond("Use this in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    removed = await remove_birthday(ctx.guild.id, member.id)
    if removed:
        await update_birthday_list_message(ctx.guild)
        await ctx.respond(f"Removed birthday for {member.mention}.", ephemeral=True)
    else:
        await ctx.respond("No birthday found for that member.", ephemeral=True)


@bot.slash_command(
    name="birthday_list",
    description="View the server’s birthday list."
)
async def birthday_list(ctx):
    if ctx.guild is None:
        return await ctx.respond("Use this in a server.", ephemeral=True)

    embed = await build_birthday_embed(ctx.guild)
    await ctx.respond(embed=embed, ephemeral=True)


@bot.slash_command(
    name="birthday_public",
    description="Create or refresh the public birthday list message in this channel."
)
async def birthday_public(ctx):
    if ctx.guild is None:
        return await ctx.respond("Use this in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    embed = await build_birthday_embed(ctx.guild)
    loc = await get_birthday_public_location(ctx.guild.id)

    if loc:
        ch_id, msg_id = loc
        channel = ctx.guild.get_channel(ch_id)
        if channel:
            try:
                msg = await channel.fetch_message(msg_id)
                await msg.edit(embed=embed, allowed_mentions=discord.AllowedMentions(users=True))
                return await ctx.respond("Updated the existing public birthday list message.", ephemeral=True)
            except Exception:
                pass

    msg = await ctx.channel.send(embed=embed)
    await set_birthday_public_location(ctx.guild.id, ctx.channel.id, msg.id)
    await ctx.respond("Created a new public birthday list message in this channel.", ephemeral=True)

@bot.slash_command(
    name="qotd_set_channel",
    description="Admin: Choose which channel the daily QOTD will post in."
)
async def qotd_set_channel(
    ctx,
    channel: discord.Option(discord.TextChannel, "Select channel for QOTD"),
):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO qotd_settings (guild_id, channel_id, enabled)
            VALUES ($1, $2, TRUE)
            ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id
            """,
            ctx.guild.id,
            channel.id,
        )

    await ctx.respond(f"QOTD will now post daily in {channel.mention}.", ephemeral=True)

@bot.slash_command(name="qotd_enable", description="Enable daily QOTD for this server.")
async def qotd_enable(ctx):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO qotd_settings (guild_id, channel_id, enabled)
            VALUES ($1, 0, TRUE)
            ON CONFLICT (guild_id) DO UPDATE SET enabled = TRUE
            """,
            ctx.guild.id,
        )

    await ctx.respond("QOTD enabled!", ephemeral=True)


@bot.slash_command(name="qotd_disable", description="Disable daily QOTD for this server.")
async def qotd_disable(ctx):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO qotd_settings (guild_id, channel_id, enabled)
            VALUES ($1, 0, FALSE)
            ON CONFLICT (guild_id) DO UPDATE SET enabled = FALSE
            """,
            ctx.guild.id,
        )

    await ctx.respond("QOTD disabled.", ephemeral=True)

@bot.slash_command(
    name="qotd_ping_role",
    description="Admin: set or clear the optional QOTD ping role."
)
async def qotd_ping_role(
    ctx,
    action: discord.Option(str, choices=["set", "clear"], required=True),
    role: discord.Option(
        discord.Role,
        "Role to ping for QOTD (use with 'set')",
        required=False
    ) = None,
):
    # Admin / owner check
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    if db_pool is None:
        return await ctx.respond("Database is not initialized. Check DATABASE_URL.", ephemeral=True)

    # SET: store the chosen role id in ping_role_id
    if action == "set":
        if role is None:
            return await ctx.respond("You must pick a role when using `set`.", ephemeral=True)

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO qotd_settings (guild_id, channel_id, enabled, ping_role_id)
                VALUES ($1, 0, TRUE, $2)
                ON CONFLICT (guild_id) DO UPDATE
                    SET ping_role_id = EXCLUDED.ping_role_id
                """,
                ctx.guild.id,
                role.id,
            )

        return await ctx.respond(
            f"QOTD ping role set to {role.mention}. Members with this role will be pinged on QOTD.",
            ephemeral=True,
        )

    # CLEAR: set ping_role_id back to 0 (no pings)
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO qotd_settings (guild_id, channel_id, enabled, ping_role_id)
            VALUES ($1, 0, TRUE, 0)
            ON CONFLICT (guild_id) DO UPDATE
                SET ping_role_id = EXCLUDED.ping_role_id
            """,
            ctx.guild.id,
        )

    await ctx.respond("Cleared the QOTD ping role. QOTD will no longer ping anyone.", ephemeral=True)

@bot.slash_command(
    name="theme_enable",
    description="Enable the automatic seasonal theme system for this server."
)
async def theme_enable(ctx):
    if ctx.guild is None:
        return await ctx.respond("This can only be used in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    await set_theme_enabled(ctx.guild.id, True)
    await ctx.respond(
        "Seasonal themes are now **enabled** for this server.\n"
        "They will update daily according to the configured mode (default: `auto`).",
        ephemeral=True,
    )

@bot.slash_command(
    name="theme_disable",
    description="Disable the automatic seasonal theme system for this server."
)
async def theme_disable(ctx):
    if ctx.guild is None:
        return await ctx.respond("This can only be used in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    await set_theme_enabled(ctx.guild.id, False)
    await ctx.respond(
        "Seasonal themes are now **disabled** for this server.\n"
        "The scheduler will not change icons, roles, or emojis automatically.",
        ephemeral=True,
    )

@bot.slash_command(
    name="theme_mode",
    description="Set the seasonal theme mode for this server."
)
async def theme_mode(
    ctx,
    mode: discord.Option(
        str,
        "How themes should behave",
        choices=["auto", "none", "halloween", "christmas"],
    ),
):
    if ctx.guild is None:
        return await ctx.respond("This can only be used in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    await set_theme_mode(ctx.guild.id, mode)

    if mode == "auto":
        msg = (
            "Theme mode set to **auto**.\n"
            "🎃 October → Halloween\n"
            "🎄 December → Christmas\n"
            "📘 All other dates → Default"
        )
    elif mode == "none":
        msg = (
            "Theme mode set to **none**.\n"
            "The scheduler will clear theme roles/emojis and keep the default icon."
        )
    elif mode == "halloween":
        msg = (
            "Theme mode set to **halloween**.\n"
            "The scheduler will keep applying Halloween roles/emojis/icons, even outside October."
        )
    else:  # christmas
        msg = (
            "Theme mode set to **christmas**.\n"
            "The scheduler will keep applying Christmas roles/emojis/icons, even outside December."
        )

    await ctx.respond(msg, ephemeral=True)

@bot.slash_command(
    name="theme_update",
    description="Recheck the theme for this server using its saved settings."
)
async def theme_update(ctx):
    if ctx.guild is None:
        return await ctx.respond("This can only be used in a server.", ephemeral=True)
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    await ctx.defer(ephemeral=True)

    settings = await get_theme_settings(ctx.guild.id)
    if not settings["enabled"]:
        return await ctx.followup.send(
            "Theme system is currently **disabled** for this server. "
            "Use `/theme_enable` to turn it back on.",
            ephemeral=True,
        )

    today = datetime.utcnow().strftime("%m-%d")
    mode, removed_roles, removed_emojis, added_roles, added_emojis = await apply_theme_for_today_with_mode(
        ctx.guild,
        today,
        settings["mode"],
    )

    if mode == "halloween":
        label = "🎃 Halloween theme applied."
    elif mode == "christmas":
        label = "🎄 Christmas theme applied."
    elif mode == "none":
        label = "🎨 Theme cleared and reverted to default."
    else:
        label = "🍂 Auto theme applied based on today’s date."

    summary = (
        f"{label}\n"
        f"Roles cleared: `{removed_roles}`\n"
        f"Emojis cleared: `{removed_emojis}`\n"
        f"Roles added: `{added_roles}`\n"
        f"Emojis added: `{added_emojis}`\n"
        f"Mode: `{settings['mode']}`"
    )
    await ctx.followup.send(summary, ephemeral=True)


@bot.slash_command(name="edit_msg", description="Edit a bot message in this channel (up to 4 lines)")
async def edit_msg(
    ctx,
    message_id: discord.Option(str, "ID of the bot message", required=True),
    line1: discord.Option(str, "Line 1 (optional)", required=False) = None,
    line2: discord.Option(str, "Line 2 (optional)", required=False) = None,
    line3: discord.Option(str, "Line 3 (optional)", required=False) = None,
    line4: discord.Option(str, "Line 4 (optional)", required=False) = None,
):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("You need Administrator.", ephemeral=True)
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

    try:
        await msg.edit(content=new_content)
        await ctx.respond("Message updated.", ephemeral=True)
    except Exception as e:
        await log_exception("edit_msg", e)
        try:
            await ctx.respond("Failed to edit message.", ephemeral=True)
        except:
            pass
            

@bot.slash_command(
    name="active_member_role_add",
    description="Mark a member as active right now (gives active role)."
)
async def activity_add(
    ctx,
    member: discord.Option(discord.Member, "Member to mark active", required=True),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    await touch_member_activity(member)
    await ctx.respond(f"{member.mention} marked active and given the active role.", ephemeral=True)


@bot.slash_command(
    name="prize_list",
    description="View all scheduled Dead Chat prize drops."
)
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


@bot.slash_command(
    name="prize_delete",
    description="Delete a scheduled prize drop by ID."
)
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


@bot.slash_command(
    name="prize_day",
    description="Schedule or instantly drop a Dead Chat prize."
)
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
        return await ctx.respond(f"Prize '{title}' not defined. Use /prize_add first.", ephemeral=True)
    content = f"**YOU'VE FOUND A PRIZE!**\nPrize: *{title}*\nDrop Rate: *{rarity}*"
    if month is None and day is None:
        drop_ch = guild.get_channel(cfg.get("prize_drop_channel_id")) or ctx.channel
        view = PrizeView(title, rarity)
        await drop_ch.send(content, view=view)
        await log_to_guild_bot_channel(
            ctx.guild,
            f"[PRIZE] Immediate prize drop '{title}' triggered by {ctx.author.mention} in {drop_ch.mention}."
        )
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
    await log_to_guild_bot_channel(
        ctx.guild,
        f"[PRIZE] Scheduled prize '{title}' on {date_str} by {ctx.author.mention} for {ctx.channel.mention}."
    )
    await ctx.respond(
        f"Scheduled for {date_str} (triggers on first Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC).",
        ephemeral=True
    )


@bot.slash_command(
    name="prize_announce_send",
    description="Manually announce a Dead Chat prize winner."
)
async def prize_announce(
    ctx,
    member: discord.Option(discord.Member, "Member", required=True),
    prize: discord.Option(str, "Prize title", required=True),
):
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
    await log_to_guild_bot_channel(
        guild,
        f"[PRIZE] Manual prize announce: {member.mention} → '{prize}' (rarity {rarity}) by {ctx.author.mention}."
    )
    await ctx.respond("Announce sent.", ephemeral=True)


@bot.slash_command(
    name="sticky_message",
    description="Set or clear a sticky message in this channel."
)
async def sticky(
    ctx,
    action: discord.Option(str, choices=["set", "clear"], required=True),
    text: discord.Option(str, required=False),
):
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
                await log_to_guild_bot_channel(
                    ctx.guild,
                    f"[STICKY] Updated sticky in {ctx.channel.mention} by {ctx.author.mention}."
                )
            except discord.NotFound:
                msg = await ctx.channel.send(text)
                sticky_messages[ctx.channel.id] = msg.id
                await ctx.respond("Sticky created.", ephemeral=True)
                await log_to_guild_bot_channel(
                    ctx.guild,
                    f"[STICKY] Created sticky in {ctx.channel.mention} by {ctx.author.mention}."
                )
        else:
            msg = await ctx.channel.send(text)
            sticky_messages[ctx.channel.id] = msg.id
            await ctx.respond("Sticky created.", ephemeral=True)
            await log_to_guild_bot_channel(
                ctx.guild,
                f"[STICKY] Created sticky in {ctx.channel.mention} by {ctx.author.mention}."
            )
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
        await log_to_guild_bot_channel(
            ctx.guild,
            f"[STICKY] Cleared sticky in {ctx.channel.mention} by {ctx.author.mention}."
        )


@bot.slash_command(
    name="plague_day",
    description="Schedule a plague day; first Dead Chat steal gets infected."
)
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
    await log_to_guild_bot_channel(
        ctx.guild,
        f"[PLAGUE] Plague scheduled for {date_str} by {ctx.author.mention}."
    )
    if month is None:
        await ctx.respond(
            f"Plague set for today. First Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC will be infected.",
            ephemeral=True
        )
    else:
        await ctx.respond(
            f"Plague scheduled for {date_str}. First Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC will be infected.",
            ephemeral=True
        )


@bot.slash_command(
    name="welcome_channel",
    description="Set the default welcome channel for new members."
)
async def add_channel_welcome(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"welcome_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "welcome channel")
    if cfg:
        await ctx.respond(f"Set welcome channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="birthday_announce_channel",
    description="Set the channel where birthday messages are posted."
)
async def add_channel_birthday_announce(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"birthday_announce_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "birthday announce channel")
    if cfg:
        await ctx.respond(f"Set birthday announce channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="twitch_stream_channel",
    description="Set the default channel for Twitch live notifications."
)
async def add_channel_twitch_announce(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"twitch_announce_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "twitch announce channel")
    if cfg:
        await ctx.respond(f"Set twitch announce channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="deadchat_trigger_channels",
    description="Add a channel that counts for Dead Chat steals."
)
async def add_channel_dead_chat_triggers(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
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


@bot.slash_command(
    name="prize_announce_channel",
    description="Set the channel where prize wins are announced."
)
async def add_channel_prize_announce(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"prize_announce_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "prize announce channel")
    if cfg:
        await ctx.respond(f"Set prize announce channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="log_channel_members",
    description="Set the member join/leave/ban/kick log channel."
)
async def add_channel_member_log(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"mod_log_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "member log channel")
    if cfg:
        await ctx.respond(f"Set member log channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="log_channel_bots",
    description="Set the bot join/leave/ban log channel."
)
async def add_channel_bot_log(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"bot_log_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "bot log channel")
    if cfg:
        await ctx.respond(f"Set bot log channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="prize_channel",
    description="Set the channel where prize drops appear."
)
async def add_channel_prize_drop(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
    updates = {"prize_drop_channel_id": channel.id}
    cfg = await _update_guild_config(ctx, updates, "prize drop channel")
    if cfg:
        await ctx.respond(f"Set prize drop channel to {channel.mention}", ephemeral=True)


@bot.slash_command(
    name="auto_delete_channel",
    description="Add a channel to the auto-delete system."
)
async def add_channel_auto_delete(
    ctx,
    channel: discord.Option(discord.TextChannel, required=True),
):
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


@bot.slash_command(
    name="active_member_role",
    description="Set the role used for active members."
)
async def add_role_active(
    ctx,
    role: discord.Option(discord.Role, required=True),
):
    updates = {"active_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "active role")
    if cfg:
        await ctx.respond(f"Set active role to {role.mention}", ephemeral=True)


@bot.slash_command(
    name="birthday_role",
    description="Set the role used to mark birthdays."
)
async def add_role_birthday(
    ctx,
    role: discord.Option(discord.Role, required=True),
):
    updates = {"birthday_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "birthday role")
    if cfg:
        await ctx.respond(f"Set birthday role to {role.mention}", ephemeral=True)

@bot.slash_command(
    name="vc_role_link",
    description="Link a voice channel to a role (join = add, leave = remove)."
)
async def vc_role_link(
    ctx,
    channel: discord.Option(discord.VoiceChannel, "Voice channel to link", required=True),
    role: discord.Option(discord.Role, "Role to give while in this VC", required=True),
):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    if db_pool is None:
        return await ctx.respond("Database is not initialized. Check DATABASE_URL.", ephemeral=True)

    await set_vc_role_link(ctx.guild.id, channel.id, role.id)
    await ctx.respond(
        f"Linked voice channel {channel.mention} → role {role.mention}. "
        f"Members will get this role while in that VC.",
        ephemeral=True,
    )
    await log_to_guild_bot_channel(
        ctx.guild,
        f"[VC-ROLE] {ctx.author.mention} linked VC {channel.mention} to role {role.mention}."
    )

@bot.slash_command(
    name="vc_role_unlink",
    description="Remove the VC→role link for a voice channel."
)
async def vc_role_unlink(
    ctx,
    channel: discord.Option(discord.VoiceChannel, "Voice channel to unlink", required=True),
):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    if db_pool is None:
        return await ctx.respond("Database is not initialized. Check DATABASE_URL.", ephemeral=True)

    removed = await remove_vc_role_link(ctx.guild.id, channel.id)
    if removed:
        await ctx.respond(
            f"Unlinked VC {channel.mention} from its role mapping.",
            ephemeral=True,
        )
        await log_to_guild_bot_channel(
            ctx.guild,
            f"[VC-ROLE] {ctx.author.mention} unlinked VC {channel.mention}."
        )
    else:
        await ctx.respond(
            "There was no VC role link for that channel.",
            ephemeral=True,
        )

@bot.slash_command(
    name="vc_role_list",
    description="Show all voice-channel role links for this server."
)
async def vc_role_list(ctx):
    if not (ctx.author.guild_permissions.administrator or ctx.guild.owner_id == ctx.author.id):
        return await ctx.respond("Admin only.", ephemeral=True)

    links = await get_guild_vc_links(ctx.guild.id)
    if not links:
        return await ctx.respond("No VC role links are configured for this server.", ephemeral=True)

    lines = []
    for channel_id, role_id in links.items():
        ch = ctx.guild.get_channel(channel_id)
        role = ctx.guild.get_role(role_id)
        ch_label = ch.mention if ch else f"<#{channel_id}> (missing)"
        role_label = role.mention if role else f"<@&{role_id}> (missing)"
        lines.append(f"{ch_label} → {role_label}")

    text = "\n".join(lines)
    if len(text) > 1900:
        text = text[:1900] + "\n...[truncated]"

    embed = discord.Embed(
        title="VC Role Links",
        description=text,
        color=discord.Color.blurple(),
    )
    await ctx.respond(embed=embed, ephemeral=True)


@bot.slash_command(
    name="deadchat_role",
    description="Set the role given to the current Dead Chat holder."
)
async def add_role_dead_chat(
    ctx,
    role: discord.Option(discord.Role, required=True),
):
    updates = {"dead_chat_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "dead chat role")
    if cfg:
        await ctx.respond(f"Set dead chat role to {role.mention}", ephemeral=True)


@bot.slash_command(
    name="plague_role",
    description="Set the role used for plague infections."
)
async def add_role_infected(
    ctx,
    role: discord.Option(discord.Role, required=True),
):
    updates = {"infected_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "infected role")
    if cfg:
        await ctx.respond(f"Set infected role to {role.mention}", ephemeral=True)


@bot.slash_command(
    name="member_join_role",
    description="Set the role given to members after a short delay."
)
async def add_role_member_join(
    ctx,
    role: discord.Option(discord.Role, required=True),
):
    updates = {"member_join_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "member join role")
    if cfg:
        await ctx.respond(f"Set member join role to {role.mention}", ephemeral=True)


@bot.slash_command(
    name="bot_join_role",
    description="Set the role given automatically to new bots."
)
async def add_role_bot_join(
    ctx,
    role: discord.Option(discord.Role, required=True),
):
    updates = {"bot_join_role_id": role.id}
    cfg = await _update_guild_config(ctx, updates, "bot join role")
    if cfg:
        await ctx.respond(f"Set bot join role to {role.mention}", ephemeral=True)


@bot.slash_command(
    name="birthday_msg",
    description="Set the birthday announcement template text."
)
async def add_text_birthday(
    ctx,
    text: discord.Option(str, required=True),
):
    updates = {"birthday_text": text}
    cfg = await _update_guild_config(ctx, updates, "birthday text")
    if cfg:
        await ctx.respond("Set birthday text.", ephemeral=True)


@bot.slash_command(
    name="twitch_msg",
    description="Set the Twitch \"went live\" announcement template."
)
async def add_text_twitch(
    ctx,
    text: discord.Option(str, required=True),
):
    updates = {"twitch_live_text": text}
    cfg = await _update_guild_config(ctx, updates, "twitch text")
    if cfg:
        await ctx.respond("Set twitch live text.", ephemeral=True)


@bot.slash_command(
    name="plague_msg",
    description="Set the \"plague outbreak\" Dead Chat message."
)
async def add_text_plague(
    ctx,
    text: discord.Option(str, required=True),
):
    updates = {"plague_outbreak_text": text}
    cfg = await _update_guild_config(ctx, updates, "plague text")
    if cfg:
        await ctx.respond("Set plague outbreak text.", ephemeral=True)


@bot.slash_command(
    name="twitch_channel",
    description="Add a Twitch channel and where its lives are announced."
)
async def add_twitch_notification(
    ctx,
    twitch_channel: discord.Option(str, "Twitch username", required=True),
    notification_channel: discord.Option(discord.TextChannel, required=False),
):
    cfg = await get_guild_config(ctx.guild)
    configs = cfg.get("twitch_configs", [])
    low_name = twitch_channel.lower()
    if any(tc["username"].lower() == low_name for tc in configs):
        await ctx.respond("Twitch channel already added.", ephemeral=True)
        return
    if notification_channel is None:
        announce_id = cfg.get("twitch_announce_channel_id", 0)
        if announce_id == 0:
            await ctx.respond("Set default twitch announce channel first with /twitch_stream_channel.", ephemeral=True)
            return
    else:
        announce_id = notification_channel.id
    configs.append({"username": twitch_channel, "announce_channel_id": announce_id})
    updates = {"twitch_configs": configs}
    new_cfg = await _update_guild_config(ctx, updates, "twitch notification")
    if new_cfg:
        await ctx.respond(f"Added twitch notification for {twitch_channel} to <#{announce_id}>.", ephemeral=True)


@bot.slash_command(
    name="prize_add",
    description="Define a prize title and its drop rarity."
)
async def add_prize(
    ctx,
    title: discord.Option(str, required=True),
    drop_rate: discord.Option(str, choices=["Common", "Uncommon", "Rare"], required=True),
):
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


@bot.slash_command(
    name="auto_delete_delay",
    description="Set how long messages last in auto-delete channels (seconds)."
)
async def set_auto_delete_delay(
    ctx,
    seconds: discord.Option(int, required=True),
):
    updates = {"auto_delete_delay_seconds": seconds}
    cfg = await _update_guild_config(ctx, updates, "auto delete delay")
    if cfg:
        await ctx.respond(f"Set auto delete delay to {seconds} seconds.", ephemeral=True)


@bot.slash_command(
    name="auto_delete_filters",
    description="Add a phrase that never gets auto-deleted."
)
async def add_auto_delete_ignore(
    ctx,
    phrase: discord.Option(str, required=True),
):
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
