
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
# BOT NAME: ADMIN BOT
# PURPOSE 
# • System backbone: shared storage messages, state load/save, one-time init commands
# • Entry/exit flow: welcome text, delayed member role, bot join role, boost + birthday messages
# • Moderation logging: bans, kicks, leaves routed to staff log thread/bot log channel
# • Dead Chat engine: idle tracking, role steals, daily reset, cooldowns, state persistence
# • Plague events: schedule contagious days, infect winners, manage infected role + expiry
# • Prize system: schedule drops, send claim buttons, announce winners with rarity metadata
# • Sticky messages: per-channel sticky text + recreation on new messages
# • Game pings: “Get Notified” button + select menu that manages game notification roles
# • Twitch watcher: poll Twitch API, track live state, send live notifications
# • Auto-delete: timed deletion for configured channels while allowing birthday messages
# ============================================================
# SERVER: Soft Dreamings (≈25 members, ages 25–40)
# • Private friend group; movie nights, QOTD, games, light role events
# • Bots: Admin Bot + Member Bot (this file)
# • Notable Channels:
#   - #k • one-letter chat
#   - #codes • game codes
#   - #graveyard • Dead Chat role, plague events, monthly prize drops
#   - #movies • movie pool
#   - #ratings • post-watch ratings
#   - #qotd • daily questions
# ROLE ACCESS
# OWNER
# • Permissions: Full
# • Commands: All
# ADMINS
# • Permissions: Full admin/moderation
# • Commands: All in this file
# TRUSTED
# • Permissions: Member + announcements + VC status
# • Commands:
#   /birthdays /birthdays_public /media_reload /library_sync
#   /pool_public /pool_remove /qotd_send /random /set_for /remove_for
# MEMBER
# • Permissions: Standard chat + VC + app commands
# • Commands:
#   /birthdays /set /color /pick /pool /replace /search 
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
from datetime import datetime, timedelta
from discord import TextChannel
from discord.ui import Select


############### CONSTANTS & CONFIG ###############
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.voice_states = True

DEBUG_GUILD_ID = int(os.getenv("DEBUG_GUILD_ID"))
bot = discord.Bot(intents=intents, debug_guilds=[DEBUG_GUILD_ID])

ACTIVE_ROLE_ID = int(os.getenv("ACTIVE_ROLE_ID", "0"))
BIRTHDAY_ROLE_ID = int(os.getenv("BIRTHDAY_ROLE_ID"))
DEAD_CHAT_ROLE_ID = int(os.getenv("DEAD_CHAT_ROLE_ID", "0"))
INFECTED_ROLE_ID = int(os.getenv("INFECTED_ROLE_ID", "0"))
MEMBER_JOIN_ROLE_ID = int(os.getenv("MEMBER_JOIN_ROLE_ID"))
BOT_JOIN_ROLE_ID = int(os.getenv("BOT_JOIN_ROLE_ID"))

WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID"))
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "0"))
MOD_LOG_THREAD_ID = int(os.getenv("MOD_LOG_THREAD_ID"))
BOT_LOG_THREAD_ID = int(os.getenv("BOT_LOG_THREAD_ID", "0"))
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID"))
PRIZE_DROP_CHANNEL_ID = int(os.getenv("PRIZE_DROP_CHANNEL_ID", "0"))
AUTO_DELETE_CHANNEL_IDS = [int(x.strip()) for x in os.getenv("AUTO_DELETE_CHANNEL_IDS", "").split(",") if x.strip().isdigit()]
DEAD_CHAT_CHANNEL_IDS = [int(x.strip()) for x in os.getenv("DEAD_CHAT_CHANNEL_IDS", "").split(",") if x.strip().isdigit()]

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_CHANNELS = [c.strip().lower() for c in os.getenv("TWITCH_CHANNELS", "").split(",") if c.strip()]

DELETE_DELAY_SECONDS = int(os.getenv("DELETE_DELAY_SECONDS", "3600"))
INACTIVE_DAYS_THRESHOLD = int(os.getenv("INACTIVE_DAYS_THRESHOLD", "14"))
DEAD_CHAT_IDLE_SECONDS = int(os.getenv("DEAD_CHAT_IDLE_SECONDS", "600"))
DEAD_CHAT_COOLDOWN_SECONDS = int(os.getenv("DEAD_CHAT_COOLDOWN_SECONDS", "0"))
PRIZE_PLAGUE_TRIGGER_HOUR_UTC = int(os.getenv("PRIZE_PLAGUE_TRIGGER_HOUR_UTC", "12"))

IGNORE_MEMBER_IDS = {int(x.strip()) for x in os.getenv("IGNORE_MEMBER_IDS", "").split(",") if x.strip().isdigit()}
MONTH_CHOICES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
MONTH_TO_NUM = {name: i for i, name in enumerate(MONTH_CHOICES, start=1)}
PRIZE_DEFS = {"Movie Request": "Common", "Month of Nitro Basic": "Uncommon", "Steam Gift Card": "Rare"}

BIRTHDAY_TEXT = "<a:pepebirthday:1296553298895310971> It's {mention}'s birthday!\n-# Add your own </set:1440919374310408234> — @everyone"
TWITCH_LIVE_MESSAGE = "{name} is live on Twitch ┃ https://twitch.tv/{name}\n-# @everyone"
DEADCHAT_STEAL_MESSAGE = "{mention} has stolen the {role} role after {minutes}+ minutes of silence.\n-# There's a random chance to win prizes with this role.\n-# [Learn More](https://discord.com/channels/1205041211610501120/1447330327923265586)"
PLAGUE_OUTBREAK_MESSAGE = "**PLAGUE OUTBREAK**\n-# The sickness has chosen its host.\n-# {mention} bears the infection, binding the plague and ending today’s contagion.\n-# Those who claim Dead Chat after this moment will not be touched by the disease.\n-# [Learn More](https://discord.com/channels/1205041211610501120/1447330327923265586)"
PRIZE_ANNOUNCE_MESSAGE = "{winner} has won a **{gift}** with {role}!\n-# Drop Rate: {rarity}"
PRIZE_CLAIM_MESSAGE = "You claimed a **{gift}**!"
GAME_NOTIF_OPEN_TEXT = "Choose the game notifications you want:"
GAME_NOTIF_NO_CHANGES = "No changes."
GAME_NOTIF_ADDED_PREFIX = "Added: "
GAME_NOTIF_REMOVED_PREFIX = "Removed: "


############### GLOBAL STATE / STORAGE ###############
guild_configs: dict[int, dict] = {}
guild_config_storage_message_id: int | None = None
db_pool: asyncpg.Pool | None = None

twitch_access_token: str | None = None
twitch_live_state: dict[str, bool] = {}
twitch_state_storage_message_id: int | None = None

last_activity_storage_message_id: int | None = None
last_activity: dict[int, str] = {}

dead_current_holder_id: int | None = None
dead_last_notice_message_ids: dict[int, int | None] = {}
dead_last_win_time: dict[int, datetime] = {}
deadchat_last_times: dict[int, str] = {}
deadchat_storage_message_id: int | None = None
deadchat_state_storage_message_id: int | None = None
movie_prize_storage_message_id: int | None = None
nitro_prize_storage_message_id: int | None = None
steam_prize_storage_message_id: int | None = None
movie_scheduled_prizes: list[dict] = []
nitro_scheduled_prizes: list[dict] = []
steam_scheduled_prizes: list[dict] = []

sticky_messages: dict[int, int] = {}
sticky_texts: dict[int, str] = {}
sticky_storage_message_id: int | None = None

pending_member_joins: list[dict] = []
member_join_storage_message_id: int | None = None

plague_scheduled: list[dict] = []
plague_storage_message_id: int | None = None
infected_members: dict[int, str] = {}

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
        CREATE TABLE IF NOT EXISTS guild_configs (
            guild_id BIGINT PRIMARY KEY,
            welcome_channel_id BIGINT,
            birthday_role_id BIGINT,
            member_join_role_id BIGINT,
            bot_join_role_id BIGINT,
            dead_chat_role_id BIGINT,
            infected_role_id BIGINT,
            active_role_id BIGINT,
            dead_chat_channel_ids TEXT,
            auto_delete_channel_ids TEXT,
            mod_log_channel_id BIGINT,
            bot_log_channel_id BIGINT,
            prize_drop_channel_id BIGINT
        );
        """)

async def init_guild_config_storage():
    global guild_config_storage_message_id, guild_configs
    msg = await find_storage_message("CONFIG_DATA:")
    if not msg:
        return
    guild_config_storage_message_id = msg.id
    raw = msg.content[len("CONFIG_DATA:"):]
    if not raw.strip():
        guild_configs = {}
        return
    try:
        data = json.loads(raw)
        guild_configs = {int(gid): cfg for gid, cfg in data.items()}
        await log_to_bot_channel(f"[CONFIG] Loaded config for {len(guild_configs)} guild(s).")
    except Exception as e:
        guild_configs = {}
        await log_to_bot_channel(f"init_guild_config_storage failed: {e}")

async def save_guild_config_storage():
    if STORAGE_CHANNEL_ID == 0 or guild_config_storage_message_id is None:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        return
    payload = {str(gid): cfg for gid, cfg in guild_configs.items()}
    try:
        msg = await ch.fetch_message(guild_config_storage_message_id)
        await msg.edit(content="CONFIG_DATA:" + json.dumps(payload))
    except Exception as e:
        await log_to_bot_channel(f"save_guild_config_storage failed: {e}")

def get_guild_config(guild: discord.Guild) -> dict | None:
    if not guild:
        return None
    return guild_configs.get(guild.id)

def ensure_guild_config(guild: discord.Guild) -> dict:
    cfg = guild_configs.get(guild.id)
    if cfg is None:
        cfg = {}
        guild_configs[guild.id] = cfg
    return cfg

def get_config_channel(guild: discord.Guild, key: str) -> TextChannel | None:
    cfg = get_guild_config(guild)
    if not cfg:
        return None
    cid = cfg.get(key)
    if not cid:
        return None
    ch = guild.get_channel(cid)
    if isinstance(ch, TextChannel):
        return ch
    return None

def get_config_role(guild: discord.Guild, key: str) -> discord.Role | None:
    cfg = get_guild_config(guild)
    if not cfg:
        return None
    rid = cfg.get(key)
    if not rid:
        return None
    return guild.get_role(rid)

async def debug_scan_storage_channel(limit: int = 20) -> str:
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return "Storage channel not found."
    lines = []
    async for msg in ch.history(limit=limit, oldest_first=True):
        prefix = msg.content[:40].replace("\n", "\\n")
        lines.append(f"id={msg.id} author={msg.author.id} bot={msg.author.bot} content={prefix}")
    if not lines:
        return "No messages visible in storage channel."
    return "\n".join(lines)

async def log_to_thread(content: str):
    channel = bot.get_channel(MOD_LOG_THREAD_ID)
    if not channel:
        return
    try:
        await channel.send(content)
    except Exception:
        pass

async def log_to_bot_channel(content: str):
    if not startup_logging_done:
        startup_log_buffer.append(content)
        return
    if BOT_LOG_THREAD_ID == 0:
        return await log_to_thread(f"[BOT] {content}")
    channel = bot.get_channel(BOT_LOG_THREAD_ID)
    if not channel:
        return
    try:
        await channel.send(content)
    except Exception:
        pass

async def flush_startup_logs():
    if not startup_log_buffer:
        return
    if BOT_LOG_THREAD_ID != 0:
        channel = bot.get_channel(BOT_LOG_THREAD_ID)
    else:
        channel = bot.get_channel(MOD_LOG_THREAD_ID) if MOD_LOG_THREAD_ID != 0 else None
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
    main_guild = bot.get_guild(DEBUG_GUILD_ID)
    if main_guild is None and bot.guilds:
        main_guild = bot.guilds[0]
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
    check_channel_permissions(STORAGE_CHANNEL_ID, "STORAGE_CHANNEL", "CHANNELS", need_manage=True)
    check_channel_permissions(WELCOME_CHANNEL_ID, "WELCOME_CHANNEL", "CHANNELS")
    check_channel_permissions(BOT_LOG_THREAD_ID, "BOT_LOG_CHANNEL", "CHANNELS")
    check_channel_permissions(WELCOME_CHANNEL_ID, "TWITCH_ANNOUNCE_CHANNEL", "CHANNELS")
    for cid in DEAD_CHAT_CHANNEL_IDS:
        check_channel_permissions(cid, f"DEAD_CHAT_CHANNEL_{cid}", "DEAD_CHAT_CHANNELS", need_manage=True)
    for cid in AUTO_DELETE_CHANNEL_IDS:
        check_channel_permissions(cid, f"AUTO_DELETE_CHANNEL_{cid}", "AUTO_DELETE", need_manage=True)
    roles_to_check = [
        (BIRTHDAY_ROLE_ID, "BIRTHDAY_ROLE_ID"),
        (MEMBER_JOIN_ROLE_ID, "MEMBER_JOIN_ROLE_ID"),
        (BOT_JOIN_ROLE_ID, "BOT_JOIN_ROLE_ID"),
        (DEAD_CHAT_ROLE_ID, "DEAD_CHAT_ROLE_ID"),
        (INFECTED_ROLE_ID, "INFECTED_ROLE_ID"),
    ]
    for role_id, label in roles_to_check:
        if role_id == 0:
            continue
        role = main_guild.get_role(role_id)
        if role is None:
            fail("ROLES", f"{label}: role {role_id} not found in main guild")
    if TWITCH_CHANNELS and (not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET or WELCOME_CHANNEL_ID == 0):
        fail("TWITCH_CONFIG", "TWITCH_CONFIG: missing client id/secret or announce channel")
    return problems, results

async def run_all_inits_with_logging():
    problems = []
    storage = {
        "STICKY": True,
        "PRIZE": True,
        "DEADCHAT": True,
        "DEADCHAT_STATE": True,
        "TWITCH_STATE": True,
        "PLAGUE": True,
        "MEMBERJOIN": True,
        "CONFIG": True,
    }
    try:
        await init_guild_config_storage()
        if guild_config_storage_message_id is None:
            storage["CONFIG"] = False
            problems.append("CONFIG_DATA storage missing; run /config_init to create it.")
    except Exception as e:
        storage["CONFIG"] = False
        problems.append("init_guild_config_storage failed; guild configs could not be loaded.")
        await log_exception("init_guild_config_storage", e)
    try:
        await init_sticky_storage()
        if sticky_storage_message_id is None:
            storage["STICKY"] = False
            problems.append("STICKY_DATA storage missing; run /sticky_init to create it.")
    except Exception as e:
        storage["STICKY"] = False
        problems.append("init_sticky_storage failed; sticky messages could not be loaded.")
        await log_exception("init_sticky_storage", e)
    try:
        await init_prize_storage()
        if movie_prize_storage_message_id is None or nitro_prize_storage_message_id is None or steam_prize_storage_message_id is None:
            storage["PRIZE"] = False
            problems.append("One or more PRIZE_* storage messages are missing; run /prize_init to create them.")
    except Exception as e:
        storage["PRIZE"] = False
        problems.append("init_prize_storage failed; prize schedules could not be loaded.")
        await log_exception("init_prize_storage", e)
    try:
        await init_deadchat_storage()
        if deadchat_storage_message_id is None:
            storage["DEADCHAT"] = False
            problems.append("DEADCHAT_DATA storage missing; run /deadchat_init to create it.")
    except Exception as e:
        storage["DEADCHAT"] = False
        problems.append("init_deadchat_storage failed; Dead Chat timestamps could not be loaded.")
        await log_exception("init_deadchat_storage", e)
    try:
        await init_deadchat_state_storage()
        if deadchat_state_storage_message_id is None:
            storage["DEADCHAT_STATE"] = False
            problems.append("DEADCHAT_STATE storage missing; run /deadchat_state_init to create it.")
    except Exception as e:
        storage["DEADCHAT_STATE"] = False
        problems.append("init_deadchat_state_storage failed; Dead Chat state could not be loaded.")
        await log_exception("init_deadchat_state_storage", e)
    try:
        await init_twitch_state_storage()
        if twitch_state_storage_message_id is None:
            storage["TWITCH_STATE"] = False
            problems.append("TWITCH_STATE storage missing; run /twitch_state_init to create it.")
    except Exception as e:
        storage["TWITCH_STATE"] = False
        problems.append("init_twitch_state_storage failed; Twitch live state could not be loaded.")
        await log_exception("init_twitch_state_storage", e)
    try:
        await init_plague_storage()
        if plague_storage_message_id is None:
            storage["PLAGUE"] = False
            problems.append("PLAGUE_DATA storage missing; run /plague_init to create it.")
    except Exception as e:
        storage["PLAGUE"] = False
        problems.append("init_plague_storage failed; plague schedule and infected list could not be loaded.")
        await log_exception("init_plague_storage", e)
    try:
        await init_member_join_storage()
        if member_join_storage_message_id is None:
            storage["MEMBERJOIN"] = False
            problems.append("MEMBERJOIN_DATA storage missing; run /memberjoin_init to create it.")
    except Exception as e:
        storage["MEMBERJOIN"] = False
        problems.append("init_member_join_storage failed; pending member joins could not be loaded.")
        await log_exception("init_member_join_storage", e)
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
    lines = []
    lines.append("")
    lines.append("[STORAGE]")
    if storage["STICKY"]:
        lines.append("`✅` Sticky storage")
    else:
        lines.append("`⚠️` **Sticky storage** — STICKY_DATA storage message is missing or unreadable, so sticky messages cannot be loaded or saved.")
    if storage["DEADCHAT"]:
        lines.append("`✅` Dead Chat storage")
    else:
        lines.append("`⚠️` **Dead Chat storage** — DEADCHAT_DATA storage message is missing or unreadable, so Dead Chat idle timestamps cannot be persisted.")
    if storage["DEADCHAT_STATE"]:
        lines.append("`✅` Dead Chat state")
    else:
        lines.append("`⚠️` **Dead Chat state** — DEADCHAT_STATE storage message is missing or unreadable, so current holder and state cannot be persisted.")
    if storage["PLAGUE"]:
        lines.append("`✅` Plague storage")
    else:
        lines.append("`⚠️` **Plague storage** — PLAGUE_DATA storage message is missing or unreadable, so plague schedule and infected members cannot be persisted.")
    if storage["PRIZE"]:
        lines.append("`✅` Prize storage (movie/nitro/steam)")
    else:
        lines.append("`⚠️` **Prize storage** — One or more PRIZE_* storage messages are missing or unreadable, so scheduled prizes cannot be persisted.")
    if storage["MEMBERJOIN"]:
        lines.append("`✅` Member-join storage")
    else:
        lines.append("`⚠️` **Member-join storage** — MEMBERJOIN_DATA storage message is missing or unreadable, so delayed member roles cannot be persisted.")
    if storage["TWITCH_STATE"]:
        lines.append("`✅` Twitch state storage")
    else:
        lines.append("`⚠️` **Twitch state storage** — TWITCH_STATE storage message is missing or unreadable, so Twitch live/offline state cannot be persisted.")
    if storage["CONFIG"]:
        lines.append("`✅` Guild config storage")
    else:
        lines.append("`⚠️` **Guild config storage** — CONFIG_DATA storage message is missing or unreadable, so per-guild setup cannot be persisted.")
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

async def find_storage_message(prefix: str) -> discord.Message | None:
    if STORAGE_CHANNEL_ID == 0:
        await log_to_bot_channel(f"find_storage_message: STORAGE_CHANNEL_ID is 0 for {prefix}")
        return None
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        await log_to_bot_channel(f"find_storage_message: storage channel invalid for {prefix}")
        return None
    try:
        async for msg in ch.history(limit=200, oldest_first=False):
            if msg.content.startswith(prefix):
                return msg
    except Exception as e:
        await log_to_bot_channel(f"find_storage_message error for {prefix}: {e}")
        return None
    await log_to_bot_channel(f"find_storage_message: no storage message found for {prefix}")
    return None

async def init_sticky_storage():
    global sticky_storage_message_id
    if STORAGE_CHANNEL_ID == 0:
        return
    msg = await find_storage_message("STICKY_DATA:")
    if not msg:
        return
    sticky_storage_message_id = msg.id
    data_str = msg.content[len("STICKY_DATA:"):]
    if not data_str.strip():
        return
    try:
        data = json.loads(data_str)
        sticky_texts.clear()
        sticky_messages.clear()
        for cid_str, info in data.items():
            try:
                cid = int(cid_str)
                if info.get("text"):
                    sticky_texts[cid] = info["text"]
                if info.get("message_id"):
                    sticky_messages[cid] = info["message_id"]
            except:
                continue
        await log_to_bot_channel(f"[STICKY] Loaded {len(sticky_texts)} sticky entries from storage.")
    except Exception as e:
        await log_to_bot_channel(f"Failed to load sticky data: {e}")

async def save_stickies():
    if STORAGE_CHANNEL_ID == 0 or sticky_storage_message_id is None:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        return
    try:
        msg = await ch.fetch_message(sticky_storage_message_id)
        data = {}
        for cid, text in sticky_texts.items():
            entry = {"text": text}
            if cid in sticky_messages:
                entry["message_id"] = sticky_messages[cid]
            data[str(cid)] = entry
        await msg.edit(content="STICKY_DATA:" + json.dumps(data))
    except Exception as e:
        await log_exception("save_stickies", e)

async def init_member_join_storage():
    global member_join_storage_message_id, pending_member_joins
    msg = await find_storage_message("MEMBERJOIN_DATA:")
    if not msg:
        return
    member_join_storage_message_id = msg.id
    raw = msg.content[len("MEMBERJOIN_DATA:"):]
    try:
        data = json.loads(raw or "[]")
        if isinstance(data, list):
            pending_member_joins[:] = data
        else:
            pending_member_joins[:] = []
        await log_to_bot_channel(f"[MEMBERJOIN] Loaded {len(pending_member_joins)} pending entries from storage.")
    except Exception:
        pending_member_joins[:] = []

async def save_member_join_storage():
    if STORAGE_CHANNEL_ID == 0 or member_join_storage_message_id is None:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        return
    try:
        msg = await ch.fetch_message(member_join_storage_message_id)
        await msg.edit(content="MEMBERJOIN_DATA:" + json.dumps(pending_member_joins))
    except Exception as e:
        await log_exception("save_member_join_storage", e)

async def init_plague_storage():
    global plague_storage_message_id, plague_scheduled, infected_members
    msg = await find_storage_message("PLAGUE_DATA:")
    if not msg:
        return
    plague_storage_message_id = msg.id
    try:
        raw = msg.content[len("PLAGUE_DATA:"):]
        data = json.loads(raw or "[]")
        plague_scheduled.clear()
        infected_members.clear()
        if isinstance(data, list):
            plague_scheduled.extend(data)
        elif isinstance(data, dict):
            plague_scheduled.extend(data.get("scheduled", []))
            infected_raw = data.get("infected", {})
            for mid_str, ts in infected_raw.items():
                try:
                    infected_members[int(mid_str)] = ts
                except:
                    pass
        await log_to_bot_channel(
            f"[PLAGUE] Loaded {len(plague_scheduled)} scheduled day(s), {len(infected_members)} infected member(s) from storage."
        )
    except Exception as e:
        await log_to_bot_channel(f"init_plague_storage failed: {e}")

async def save_plague_storage():
    if STORAGE_CHANNEL_ID == 0 or plague_storage_message_id is None:
        await log_to_bot_channel("save_plague_storage: storage id missing")
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        await log_to_bot_channel("save_plague_storage: storage channel invalid")
        return
    payload = {
        "scheduled": plague_scheduled,
        "infected": {str(k): v for k, v in infected_members.items()},
    }
    try:
        msg = await ch.fetch_message(plague_storage_message_id)
        await msg.edit(content="PLAGUE_DATA:" + json.dumps(payload))
    except Exception as e:
        await log_to_bot_channel(f"save_plague_storage failed: {e}")

async def trigger_plague_infection(member: discord.Member):
    infected_role = member.guild.get_role(INFECTED_ROLE_ID)
    if not infected_role or infected_role in member.roles:
        return
    await member.add_roles(infected_role, reason="Caught the monthly Dead Chat plague")
    expires_at = (datetime.utcnow() + timedelta(days=3)).isoformat() + "Z"
    infected_members[member.id] = expires_at
    plague_scheduled.clear()
    await save_plague_storage()
    await log_to_bot_channel(
        f"[PLAGUE] {member.mention} infected; role expires at {expires_at}."
    )

async def check_plague_active():
    if not plague_scheduled or INFECTED_ROLE_ID == 0:
        return False
    now = datetime.utcnow()
    for entry in plague_scheduled:
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

def parse_schedule_datetime(when: str) -> datetime | None:
    try:
        return datetime.strptime(when, "%Y-%m-%d %H:%M")
    except ValueError:
        return None

async def init_prize_storage():
    global movie_prize_storage_message_id, nitro_prize_storage_message_id, steam_prize_storage_message_id
    global movie_scheduled_prizes, nitro_scheduled_prizes, steam_scheduled_prizes
    if STORAGE_CHANNEL_ID == 0:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        await log_to_bot_channel("init_prize_storage: invalid storage channel")
        return
    movie_msg = await find_storage_message("PRIZE_MOVIE_DATA:")
    nitro_msg = await find_storage_message("PRIZE_NITRO_DATA:")
    steam_msg = await find_storage_message("PRIZE_STEAM_DATA:")
    if not (movie_msg and nitro_msg and steam_msg):
        await log_to_bot_channel("Prize storage messages missing → Run /prize_init first")
        return
    movie_prize_storage_message_id = movie_msg.id
    nitro_prize_storage_message_id = nitro_msg.id
    steam_prize_storage_message_id = steam_msg.id
    def safe_load(content, prefix):
        try:
            return json.loads(content[len(prefix):]) if content.startswith(prefix) else []
        except:
            return []
    movie_scheduled_prizes = safe_load(movie_msg.content, "PRIZE_MOVIE_DATA:")
    nitro_scheduled_prizes = safe_load(nitro_msg.content, "PRIZE_NITRO_DATA:")
    steam_scheduled_prizes = safe_load(steam_msg.content, "PRIZE_STEAM_DATA:")
    await log_to_bot_channel(
        f"[PRIZE] Loaded {len(movie_scheduled_prizes)} movie, {len(nitro_scheduled_prizes)} nitro, {len(steam_scheduled_prizes)} steam scheduled prizes from storage."
    )

async def save_prize_storage():
    if STORAGE_CHANNEL_ID == 0:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        return
    for msg_id, data, prefix in [
        (movie_prize_storage_message_id, movie_scheduled_prizes, "PRIZE_MOVIE_DATA:"),
        (nitro_prize_storage_message_id, nitro_scheduled_prizes, "PRIZE_NITRO_DATA:"),
        (steam_prize_storage_message_id, steam_scheduled_prizes, "PRIZE_STEAM_DATA:"),
    ]:
        if msg_id:
            try:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(content=prefix + json.dumps(data))
            except Exception as e:
                await log_exception(f"save_prize_storage_{prefix}", e)

def get_prize_list_and_entries(prize_type: str):
    if prize_type == "movie":
        return movie_scheduled_prizes
    if prize_type == "nitro":
        return nitro_scheduled_prizes
    if prize_type == "steam":
        return steam_scheduled_prizes
    return None

async def run_scheduled_prize(prize_type: str, prize_id: int):
    if prize_type == "movie":
        entries = movie_scheduled_prizes
        view_cls = MoviePrizeView
    elif prize_type == "nitro":
        entries = nitro_scheduled_prizes
        view_cls = NitroPrizeView
    elif prize_type == "steam":
        entries = steam_scheduled_prizes
        view_cls = SteamPrizeView
    else:
        return
    record = None
    for p in entries:
        if p.get("id") == prize_id:
            record = p
            break
    if not record:
        return
    send_at = parse_schedule_datetime(record.get("send_at", ""))
    if not send_at:
        return
    now = datetime.utcnow()
    delay = (send_at - now).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
    channel_id = PRIZE_DROP_CHANNEL_ID or record.get("channel_id")
    content = record.get("content")
    if not channel_id or not content:
        return
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    view = view_cls()
    await channel.send(content, view=view)
    entries[:] = [p for p in entries if p.get("id") != prize_id]
    await save_prize_storage()
    await log_to_bot_channel(f"[PRIZE] Sent scheduled {prize_type} prize ID {prize_id} to channel {channel_id}.")

async def add_scheduled_prize(prize_type: str, channel_id: int, content: str, date_str: str):
    if prize_type == "movie":
        entries = movie_scheduled_prizes
    elif prize_type == "nitro":
        entries = nitro_scheduled_prizes
    elif prize_type == "steam":
        entries = steam_scheduled_prizes
    else:
        return
    existing_ids = [p.get("id", 0) for p in entries]
    new_id = max(existing_ids) + 1 if existing_ids else 1
    target_channel_id = PRIZE_DROP_CHANNEL_ID or channel_id
    entries.append(
        {
            "id": new_id,
            "channel_id": target_channel_id,
            "content": content,
            "date": date_str,
        }
    )
    await save_prize_storage()

async def initialize_dead_chat():
    global dead_current_holder_id
    if not DEAD_CHAT_CHANNEL_IDS or DEAD_CHAT_ROLE_ID == 0:
        return
    for guild in bot.guilds:
        role = guild.get_role(DEAD_CHAT_ROLE_ID)
        if role and role.members:
            dead_current_holder_id = role.members[0].id
            break
    for chan_id in DEAD_CHAT_CHANNEL_IDS:
        if chan_id in deadchat_last_times:
            continue
        deadchat_last_times[chan_id] = discord.utils.utcnow().isoformat() + "Z"
    await save_deadchat_storage()

async def handle_dead_chat_message(message: discord.Message):
    global dead_current_holder_id
    cfg = get_guild_config(message.guild)
    dead_role_id = cfg.get("dead_chat_role_id", DEAD_CHAT_ROLE_ID) if cfg else DEAD_CHAT_ROLE_ID
    dead_channels = cfg.get("dead_chat_channel_ids", DEAD_CHAT_CHANNEL_IDS) if cfg else DEAD_CHAT_CHANNEL_IDS
    active_role_id = cfg.get("active_role_id", ACTIVE_ROLE_ID) if cfg else ACTIVE_ROLE_ID
    if dead_role_id == 0 or message.channel.id not in dead_channels or message.author.id in IGNORE_MEMBER_IDS:
        return
    if active_role_id != 0:
        active_role = message.guild.get_role(active_role_id)
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
    role = message.guild.get_role(dead_role_id)
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
    dead_current_holder_id = message.author.id
    dead_last_win_time[message.author.id] = now

    today_str = now.strftime("%Y-%m-%d")
    hour_utc = now.hour
    triggered_plague = False
    if hour_utc >= PRIZE_PLAGUE_TRIGGER_HOUR_UTC and plague_scheduled:
        for entry in list(plague_scheduled):
            if entry.get("date") == today_str:
                await trigger_plague_infection(message.author)
                triggered_plague = True
                break

    triggered_prize = False
    if hour_utc >= PRIZE_PLAGUE_TRIGGER_HOUR_UTC:
        for prize_list, view_cls in [
            (movie_scheduled_prizes, MoviePrizeView),
            (nitro_scheduled_prizes, NitroPrizeView),
            (steam_scheduled_prizes, SteamPrizeView)
        ]:
            matching = [p for p in prize_list if p.get("date") == today_str]
            if matching:
                triggered_prize = True
            for p in matching:
                cfg_drop_id = cfg.get("prize_drop_channel_id") if cfg else None
                channel_id = cfg_drop_id or PRIZE_DROP_CHANNEL_ID or p.get("channel_id") or message.channel.id
                channel = message.guild.get_channel(channel_id)
                if not channel:
                    channel = message.channel
                view = view_cls()
                await channel.send(p.get("content", ""), view=view)
            prize_list[:] = [p for p in prize_list if p.get("date") != today_str]
        if triggered_prize:
            await save_prize_storage()
            await log_to_bot_channel(f"[PRIZE] Daily prize drop(s) sent for {today_str}.")

    for old_cid, mid in list(dead_last_notice_message_ids.items()):
        if mid:
            ch = message.guild.get_channel(old_cid)
            if ch:
                try:
                    m = await ch.fetch_message(mid)
                    await m.delete()
                except:
                    pass

    if triggered_plague:
        plague_text = PLAGUE_OUTBREAK_MESSAGE.format(mention=message.author.mention)
        notice = await message.channel.send(plague_text)
        await log_to_bot_channel(
            f"[PLAGUE] {message.author.mention} infected on {today_str} in {message.channel.mention}."
        )
    else:
        minutes = DEAD_CHAT_IDLE_SECONDS // 60
        notice_text = DEADCHAT_STEAL_MESSAGE.format(
            mention=message.author.mention,
            role=role.mention,
            minutes=minutes
        )
        notice = await message.channel.send(notice_text)
        await log_to_bot_channel(
            f"[DEADCHAT] {message.author.mention} stole {role.mention} in {message.channel.mention} after {minutes}+ minutes idle."
        )

    dead_last_notice_message_ids[message.channel.id] = notice.id
    await save_deadchat_state()

async def init_deadchat_storage():
    global deadchat_storage_message_id, deadchat_last_times
    msg = await find_storage_message("DEADCHAT_DATA:")
    if not msg:
        return
    deadchat_storage_message_id = msg.id
    raw = msg.content[len("DEADCHAT_DATA:"):]
    if not raw.strip():
        deadchat_last_times.clear()
        return
    data = json.loads(raw)
    deadchat_last_times.clear()
    for cid_str, ts in data.items():
        try:
            deadchat_last_times[int(cid_str)] = ts
        except:
            pass
    await log_to_bot_channel(f"[DEADCHAT] Loaded timestamps for {len(deadchat_last_times)} channel(s).")

async def save_deadchat_storage():
    global deadchat_storage_message_id
    if STORAGE_CHANNEL_ID == 0 or deadchat_storage_message_id is None:
        await log_to_bot_channel("save_deadchat_storage: storage id missing")
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        await log_to_bot_channel("save_deadchat_storage: storage channel invalid")
        return
    try:
        msg = await ch.fetch_message(deadchat_storage_message_id)
        await msg.edit(content="DEADCHAT_DATA:" + json.dumps(deadchat_last_times))
    except discord.Forbidden:
        await log_to_bot_channel("DEADCHAT_DATA: Bot missing 'Manage Messages' in storage channel")
    except discord.NotFound:
        await log_to_bot_channel("DEADCHAT_DATA message deleted — Run /deadchat_init")
        deadchat_storage_message_id = None
    except Exception as e:
        await log_to_bot_channel(f"Deadchat save failed: {e}")

async def init_deadchat_state_storage():
    global deadchat_state_storage_message_id
    msg = await find_storage_message("DEADCHAT_STATE:")
    if not msg:
        return
    deadchat_state_storage_message_id = msg.id
    await load_deadchat_state()

async def load_deadchat_state():
    global dead_current_holder_id, dead_last_win_time, dead_last_notice_message_ids
    if not deadchat_state_storage_message_id or STORAGE_CHANNEL_ID == 0:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch:
        return
    try:
        msg = await ch.fetch_message(deadchat_state_storage_message_id)
        raw = msg.content[len("DEADCHAT_STATE:"):]
        if not raw.strip():
            return
        data = json.loads(raw)
        dead_current_holder_id = data.get("current_holder")
        dead_last_win_time = {}
        for k, v in data.get("last_win_times", {}).items():
            try:
                dead_last_win_time[int(k)] = datetime.fromisoformat(v.replace("Z", ""))
            except:
                pass
        dead_last_notice_message_ids = {int(k): v for k, v in data.get("notice_msg_ids", {}).items() if v}
    except Exception as e:
        await log_to_bot_channel(f"Failed to load DEADCHAT_STATE: {e}")

async def save_deadchat_state():
    if STORAGE_CHANNEL_ID == 0 or deadchat_state_storage_message_id is None:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        return
    data = {
        "current_holder": dead_current_holder_id,
        "last_win_times": {str(k): v.isoformat() + "Z" for k, v in dead_last_win_time.items()},
        "notice_msg_ids": dead_last_notice_message_ids
    }
    try:
        msg = await ch.fetch_message(deadchat_state_storage_message_id)
        await msg.edit(content="DEADCHAT_STATE:" + json.dumps(data))
    except Exception as e:
        await log_to_bot_channel(f"Deadchat state save failed: {e}")

async def init_twitch_state_storage():
    global twitch_state_storage_message_id
    msg = await find_storage_message("TWITCH_STATE:")
    if not msg:
        return
    twitch_state_storage_message_id = msg.id
    await load_twitch_state()
    await log_to_bot_channel(f"[TWITCH] State storage initialized with id {twitch_state_storage_message_id}.")

async def load_twitch_state():
    global twitch_live_state
    if not twitch_state_storage_message_id:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    try:
        msg = await ch.fetch_message(twitch_state_storage_message_id)
        loaded = json.loads(msg.content[len("TWITCH_STATE:"):])
        twitch_live_state = {k.lower(): bool(v) for k, v in loaded.items()}
        await log_to_bot_channel(f"[TWITCH] Loaded live state for {len(twitch_live_state)} channel(s).")
    except Exception as e:
        await log_exception("load_twitch_state", e)
        twitch_live_state = {name: False for name in TWITCH_CHANNELS}

async def save_twitch_state():
    if STORAGE_CHANNEL_ID == 0 or twitch_state_storage_message_id is None:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch:
        return
    try:
        msg = await ch.fetch_message(twitch_state_storage_message_id)
        await msg.edit(content="TWITCH_STATE:" + json.dumps(twitch_live_state))
    except Exception as e:
        await log_exception("save_twitch_state", e)

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
    params = [("user_login", name) for name in TWITCH_CHANNELS]
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
    msg = await find_storage_message("ACTIVITY_DATA:")
    if not msg:
        return
    last_activity_storage_message_id = msg.id
    raw = msg.content[len("ACTIVITY_DATA:"):]
    if not raw.strip():
        last_activity = {}
        return
    try:
        data = json.loads(raw)
        last_activity = {}
        for mid_str, ts in data.items():
            try:
                last_activity[int(mid_str)] = ts
            except:
                pass
        await log_to_bot_channel(f"[ACTIVITY] Loaded last activity for {len(last_activity)} member(s).")
    except Exception as e:
        await log_to_bot_channel(f"init_last_activity_storage failed: {e}")
        last_activity = {}

async def save_last_activity_storage():
    global last_activity_storage_message_id
    if STORAGE_CHANNEL_ID == 0 or last_activity_storage_message_id is None:
        return
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not ch or not isinstance(ch, TextChannel):
        return
    try:
        msg = await ch.fetch_message(last_activity_storage_message_id)
        payload = {str(k): v for k, v in last_activity.items()}
        await msg.edit(content="ACTIVITY_DATA:" + json.dumps(payload))
    except Exception as e:
        await log_to_bot_channel(f"save_last_activity_storage failed: {e}")

async def touch_member_activity(member: discord.Member):
    if member.bot:
        return
    now = discord.utils.utcnow().isoformat() + "Z"
    last_activity[member.id] = now
    await save_last_activity_storage()
    if ACTIVE_ROLE_ID == 0:
        return
    role = member.guild.get_role(ACTIVE_ROLE_ID)
    if role and role not in member.roles:
        try:
            await member.add_roles(role, reason="Marked active by activity tracking")
            await log_to_bot_channel(
                f"[ACTIVITY] {member.mention} marked active and given active role in guild {member.guild.id}."
            )
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
        dead_role = guild.get_role(DEAD_CHAT_ROLE_ID)
        role_mention = dead_role.mention if dead_role else "the Dead Chat role"
        ch = guild.get_channel(WELCOME_CHANNEL_ID)
        if ch:
            msg = PRIZE_ANNOUNCE_MESSAGE.format(
                emoji=PRIZE_EMOJI,
                winner=interaction.user.mention,
                gift=self.gift_title,
                role=role_mention,
                rarity=self.rarity
            )
            await ch.send(msg)
        await log_to_bot_channel(
            f"[PRIZE] {interaction.user.mention} claimed prize '{self.gift_title}' (rarity {self.rarity})."
        )
        claim_text = PRIZE_CLAIM_MESSAGE.format(gift=self.gift_title)
        await interaction.response.send_message(claim_text, ephemeral=True)

class MoviePrizeView(BasePrizeView):
    gift_title = "Movie Request"
    rarity = "Common"
class NitroPrizeView(BasePrizeView):
    gift_title = "Month of Nitro Basic"
    rarity = "Uncommon"
class SteamPrizeView(BasePrizeView):
    gift_title = "Steam Gift Card"
    rarity = "Rare"

class GameNotificationSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="General games", value="1352405080703504384"),
            discord.SelectOption(label="Among Us Vanilla", value="1406868589893652520"),
            discord.SelectOption(label="Among Us Modded", value="1406868685225725976"),
            discord.SelectOption(label="Among Us Proximity Chat", value="1342246913663303702"),
        ]
        super().__init__(
            placeholder="Select game notifications…",
            min_values=0,
            max_values=len(options),
            options=options,
            custom_id="game_notif_select"
        )
    async def callback(self, interaction: discord.Interaction):
        selected = [int(x) for x in self.values]
        member = interaction.user
        added = []
        removed = []
        for opt in self.options:
            role_id = int(opt.value)
            role = interaction.guild.get_role(role_id)
            if not role:
                continue
            if role_id in selected:
                if role not in member.roles:
                    await member.add_roles(role, reason="Game notification opt-in")
                    added.append(role.name)
            else:
                if role in member.roles:
                    await member.remove_roles(role, reason="Game notification opt-out")
                    removed.append(role.name)
        text = ""
        if added:
            text += GAME_NOTIF_ADDED_PREFIX + ", ".join(added) + "\n"
        if removed:
            text += GAME_NOTIF_REMOVED_PREFIX + ", ".join(removed) + "\n"
        if not text:
            text = GAME_NOTIF_NO_CHANGES
        if added or removed:
            await log_to_bot_channel(
                f"[GAMES] {member.mention} updated game notif roles. Added: {', '.join(added) or 'none'}; Removed: {', '.join(removed) or 'none'}."
            )
        await interaction.response.send_message(text.strip(), ephemeral=True)

class GameNotificationView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Get Notified",
        style=discord.ButtonStyle.grey,
        custom_id="game_notif_persistent_button_v9"
    )
    async def open_menu(self, button: discord.ui.Button, interaction: discord.Interaction):
        await interaction.response.send_message(
            GAME_NOTIF_OPEN_TEXT,
            view=GameNotificationSelectView(),
            ephemeral=True
        )

class GameNotificationSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(GameNotificationSelect())


############### AUTOCOMPLETE FUNCTIONS ###############


############### BACKGROUND TASKS & SCHEDULERS ###############
async def twitch_watcher():
    await bot.wait_until_ready()
    if not WELCOME_CHANNEL_ID or not TWITCH_CHANNELS:
        return
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch:
        return
    while not bot.is_closed():
        try:
            streams = await fetch_twitch_streams()
            for name in TWITCH_CHANNELS:
                is_live = name in streams
                was_live = twitch_live_state.get(name, False)
                if is_live and not was_live:
                    msg = TWITCH_LIVE_MESSAGE.format(name=name)
                    await ch.send(msg)
                    twitch_live_state[name] = True
                    await save_twitch_state()
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
    if INFECTED_ROLE_ID == 0:
        return
    while not bot.is_closed():
        try:
            now = datetime.utcnow()
            expired_ids = []
            for mid, ts in list(infected_members.items()):
                try:
                    expires = datetime.fromisoformat(ts.replace("Z", ""))
                except:
                    continue
                if now >= expires:
                    expired_ids.append(mid)
            if expired_ids:
                cleared_mentions = []
                for guild in bot.guilds:
                    role = guild.get_role(INFECTED_ROLE_ID)
                    if not role:
                        continue
                    for mid in expired_ids:
                        member = guild.get_member(mid)
                        if member and role in member.roles:
                            try:
                                await member.remove_roles(role, reason="Plague expired")
                                cleared_mentions.append(member.mention)
                            except:
                                pass
                for mid in expired_ids:
                    infected_members.pop(mid, None)
                await save_plague_storage()
                if cleared_mentions:
                    await log_to_bot_channel(f"[PLAGUE] Cleared infected role for: {', '.join(cleared_mentions)}")
                else:
                    await log_to_bot_channel(
                        f"[PLAGUE] Cleared infected role for: {', '.join(f'<@{i}>' for i in expired_ids)}"
                    )
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
                    cfg = get_guild_config(guild)
                    member_role_id = cfg.get("member_join_role_id", MEMBER_JOIN_ROLE_ID) if cfg else MEMBER_JOIN_ROLE_ID
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
                            await log_to_bot_channel(
                                f"[MEMBERJOIN] Applied member role to {member.mention} in guild {guild.id}."
                            )
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
            inactive_ids = set()
            for mid, ts in list(last_activity.items()):
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", ""))
                except:
                    continue
                if dt < cutoff:
                    inactive_ids.add(mid)
            for guild in bot.guilds:
                cfg = get_guild_config(guild)
                active_role_id = cfg.get("active_role_id", ACTIVE_ROLE_ID) if cfg else ACTIVE_ROLE_ID
                if not active_role_id:
                    continue
                role = guild.get_role(active_role_id)
                if not role:
                    continue
                for member in list(role.members):
                    if member.id in inactive_ids or member.id not in last_activity:
                        try:
                            await member.remove_roles(role, reason="Marked inactive by activity tracking")
                            await log_to_bot_channel(f"{member.mention} is officially inactive @everyone")
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
    bot.add_view(GameNotificationView())
    await run_all_inits_with_logging()

    global startup_logging_done, startup_log_buffer

    await init_last_activity_storage()

    bot.loop.create_task(twitch_watcher())
    bot.loop.create_task(infected_watcher())
    bot.loop.create_task(member_join_watcher())
    bot.loop.create_task(activity_inactive_watcher())

    await log_to_bot_channel("[TWITCH] watcher started.")
    await log_to_bot_channel("[PLAGUE] infected_watcher started.")
    await log_to_bot_channel("[MEMBERJOIN] watcher started.")
    await log_to_bot_channel("[ACTIVITY] activity_inactive_watcher started.")

    await log_to_bot_channel(f"Bot ready as {bot.user} in {len(bot.guilds)} guild(s).")

    await flush_startup_logs()

    startup_logging_done = True
    startup_log_buffer = []

    if sticky_storage_message_id is None:
        print("STORAGE NOT INITIALIZED — Run /sticky_init, /prize_init and /deadchat_init")
    else:
        await initialize_dead_chat()

@bot.event
async def on_member_update(before, after):
    cfg = get_guild_config(after.guild)
    ch = get_config_channel(after.guild, "welcome_channel_id") if cfg else bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch:
        return
    birthday_role_id = cfg.get("birthday_role_id", BIRTHDAY_ROLE_ID) if cfg else BIRTHDAY_ROLE_ID
    new_roles = set(after.roles) - set(before.roles)
    for role in new_roles:
        if role.id == birthday_role_id:
            if BIRTHDAY_TEXT:
                await ch.send(BIRTHDAY_TEXT.replace("{mention}", after.mention))
                await log_to_bot_channel(
                    f"[BIRTHDAY] Birthday role granted and message sent for {after.mention}."
                )

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
        view = GameNotificationView()
        new_msg = await message.channel.send(sticky_texts[message.channel.id], view=view)
        sticky_messages[message.channel.id] = new_msg.id
        await save_stickies()
    cfg = get_guild_config(message.guild)
    auto_ids = cfg.get("auto_delete_channel_ids", AUTO_DELETE_CHANNEL_IDS) if cfg else AUTO_DELETE_CHANNEL_IDS
    if message.channel.id in auto_ids:
        content = message.content.lower()
        if not ("happy birthday" in content or "happy bday" in content or "happy b-day" in content or "happy belated bday" in content or "happy belated b-day" in content or "happy belated birthday" in content):
            async def delete_later():
                await asyncio.sleep(DELETE_DELAY_SECONDS)
                try:
                    await message.delete()
                    await log_to_bot_channel(
                        f"[AUTO-DELETE] Message {message.id} in <#{message.channel.id}> deleted after {DELETE_DELAY_SECONDS} seconds."
                    )
                except Exception as e:
                    await log_exception("auto_delete_delete_later", e)
            bot.loop.create_task(delete_later())

@bot.event
async def on_member_join(member: discord.Member):
    cfg = get_guild_config(member.guild)
    ch = get_config_channel(member.guild, "welcome_channel_id") if cfg else bot.get_channel(WELCOME_CHANNEL_ID)
    if member.bot:
        await log_to_bot_channel(f"Bot joined: {member.mention}")
        bot_role_id = cfg.get("bot_join_role_id", BOT_JOIN_ROLE_ID) if cfg else BOT_JOIN_ROLE_ID
        if bot_role_id:
            role = member.guild.get_role(bot_role_id)
            if role:
                await member.add_roles(role)
        return
    await log_to_bot_channel(
        f"[JOIN] Member joined: {member.mention} (ID {member.id}) in guild {member.guild.id}."
    )
    member_role_id = cfg.get("member_join_role_id", MEMBER_JOIN_ROLE_ID) if cfg else MEMBER_JOIN_ROLE_ID
    if member_role_id:
        assign_at = datetime.utcnow() + timedelta(days=1)
        pending_member_joins.append(
            {
                "guild_id": member.guild.id,
                "member_id": member.id,
                "assign_at": assign_at.isoformat() + "Z",
            }
        )
        await save_member_join_storage()
        await log_to_bot_channel(
            f"[MEMBERJOIN] Queued delayed member role for {member.mention} at {assign_at.isoformat()}Z."
        )

@bot.event
async def on_member_ban(guild, user):
    moderator = None
    async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
        if entry.target.id == user.id:
            moderator = entry.user
            break
    text = f"{user.mention} was banned by {moderator.mention if moderator else 'Unknown'}"
    if user.bot:
        await log_to_bot_channel(text)
    else:
        await log_to_thread(text)

@bot.event
async def on_member_remove(member: discord.Member):
    now = discord.utils.utcnow()
    async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
        if entry.target.id == member.id and (now - entry.created_at).total_seconds() < 10:
            return
    kicked = False
    moderator = None
    async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.kick):
        if entry.target.id == member.id and (now - entry.created_at).total_seconds() < 10:
            moderator = entry.user
            kicked = True
            break
    log_fn = log_to_bot_channel if member.bot else log_to_thread
    if kicked:
        await log_fn(f"{member.mention} was kicked by {moderator.mention if moderator else 'Unknown'}")
    else:
        await log_fn(f"{member.mention} has left the server")

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

@bot.slash_command(name="storage_debug", description="Show storage message IDs for all systems")
async def storage_debug(
    ctx,
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    lines = [
        f"STORAGE_CHANNEL_ID: {STORAGE_CHANNEL_ID}",
        f"sticky_storage_message_id: {sticky_storage_message_id}",
        f"member_join_storage_message_id: {member_join_storage_message_id}",
        f"plague_storage_message_id: {plague_storage_message_id}",
        f"deadchat_storage_message_id: {deadchat_storage_message_id}",
        f"deadchat_state_storage_message_id: {deadchat_state_storage_message_id}",
        f"movie_prize_storage_message_id: {movie_prize_storage_message_id}",
        f"nitro_prize_storage_message_id: {nitro_prize_storage_message_id}",
        f"steam_prize_storage_message_id: {steam_prize_storage_message_id}",
        f"twitch_state_storage_message_id: {twitch_state_storage_message_id}",
    ]
    await ctx.respond("Storage debug:\n" + "\n".join(lines), ephemeral=True)

@bot.slash_command(name="storage_scan", description="Show what the bot sees in the storage channel")
async def storage_scan(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    text = await debug_scan_storage_channel()
    if len(text) > 1900:
        text = text[:1900] + "\n...[truncated]"
    await ctx.respond(f"```{text}```", ephemeral=True)

@bot.slash_command(name="storage_refresh", description="Rescan storage channel and reload storage message IDs")
async def storage_refresh(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    await ctx.defer(ephemeral=True)
    try:
        await run_all_inits_with_logging()
        await ctx.followup.send("Storage reload complete. Run /storage_debug to verify.", ephemeral=True)
    except Exception as e:
        await log_exception("storage_refresh", e)
        try:
            await ctx.followup.send(f"storage_refresh error: {e}", ephemeral=True)
        except:
            pass

@bot.slash_command(name="deadchat_rescan", description="Force-scan all dead-chat channels for latest message timestamps")
async def deadchat_rescan(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    count = 0
    async with ctx.typing():
        for channel_id in DEAD_CHAT_CHANNEL_IDS:
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
    await ctx.respond(f"Rescan complete — found latest message in {count}/{len(DEAD_CHAT_CHANNEL_IDS)} dead-chat channels and saved timestamps.", ephemeral=True)

@bot.slash_command(name="memberjoin_init", description="Create member-join storage message")
async def memberjoin_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Invalid storage channel", ephemeral=True)
    msg = await ch.send("MEMBERJOIN_DATA:[]")
    global member_join_storage_message_id
    member_join_storage_message_id = msg.id
    await ctx.respond(f"Member join storage created: {msg.id}", ephemeral=True)

@bot.slash_command(name="activity_init", description="Create last-activity storage message")
async def activity_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Invalid storage channel", ephemeral=True)
    msg = await ch.send("ACTIVITY_DATA:{}")
    global last_activity_storage_message_id
    last_activity_storage_message_id = msg.id
    await save_last_activity_storage()
    await ctx.respond(f"Activity storage message created: {msg.id}", ephemeral=True)

@bot.slash_command(name="deadchat_state_init", description="Create DEADCHAT_STATE storage message")
async def deadchat_state_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Invalid storage channel", ephemeral=True)
    msg = await ch.send("DEADCHAT_STATE:{\"current_holder\":null,\"last_win_times\":{},\"notice_msg_ids\":{}}")
    global deadchat_state_storage_message_id
    deadchat_state_storage_message_id = msg.id
    await save_deadchat_state()
    await ctx.respond(f"Created DEADCHAT_STATE message: {msg.id}", ephemeral=True)

@bot.slash_command(name="twitch_state_init", description="Create TWITCH_STATE storage message")
async def twitch_state_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Invalid storage channel", ephemeral=True)
    msg = await ch.send("TWITCH_STATE:{}")
    global twitch_state_storage_message_id
    twitch_state_storage_message_id = msg.id
    await save_twitch_state()
    await ctx.respond(f"Created TWITCH_STATE message: {msg.id}", ephemeral=True)

@bot.slash_command(name="prize_init", description="Manually create prize storage messages")
async def prize_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Storage channel invalid.", ephemeral=True)
    movie_msg = await ch.send("PRIZE_MOVIE_DATA:[]")
    nitro_msg = await ch.send("PRIZE_NITRO_DATA:[]")
    steam_msg = await ch.send("PRIZE_STEAM_DATA:[]")
    global movie_prize_storage_message_id, nitro_prize_storage_message_id, steam_prize_storage_message_id
    movie_prize_storage_message_id = movie_msg.id
    nitro_prize_storage_message_id = nitro_msg.id
    steam_prize_storage_message_id = steam_msg.id
    await save_prize_storage()
    await ctx.respond(f"Prize storage messages created:\nMovie: {movie_msg.id}\nNitro: {nitro_msg.id}\nSteam: {steam_msg.id}", ephemeral=True)

@bot.slash_command(name="sticky_init", description="Manually create sticky storage message")
async def sticky_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Storage channel invalid.", ephemeral=True)
    msg = await ch.send("STICKY_DATA:{}")
    global sticky_storage_message_id
    sticky_storage_message_id = msg.id
    await save_stickies()
    await ctx.respond(f"Sticky storage message created: {msg.id}", ephemeral=True)

@bot.slash_command(name="deadchat_init", description="Manually create deadchat storage message")
async def deadchat_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Storage channel invalid.", ephemeral=True)
    msg = await ch.send("DEADCHAT_DATA:{}")
    global deadchat_storage_message_id
    deadchat_storage_message_id = msg.id
    await save_deadchat_storage()
    await ctx.respond(f"Deadchat storage message created: {msg.id}", ephemeral=True)

@bot.slash_command(name="config_init", description="Create guild config storage message")
async def config_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Invalid storage channel", ephemeral=True)
    msg = await ch.send("CONFIG_DATA:{}")
    global guild_config_storage_message_id
    guild_config_storage_message_id = msg.id
    await save_guild_config_storage()
    await ctx.respond(f"Guild config storage message created: {msg.id}", ephemeral=True)

@bot.slash_command(name="config_show", description="Show this server's Admin Bot config")
async def config_show(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    cfg = get_guild_config(ctx.guild) or {}
    text = json.dumps(cfg, indent=2)
    if len(text) > 1900:
        text = text[:1900] + "\n...[truncated]"
    await ctx.respond(f"```json\n{text}\n```", ephemeral=True)

@bot.slash_command(name="setup", description="Configure Admin Bot for this server")
async def setup(
    ctx,
    welcome_channel: discord.Option(discord.TextChannel, "Where welcome/birthday messages go", required=True),
    storage_channel: discord.Option(discord.TextChannel, "Storage channel for this guild (or same as global)", required=False),
    birthday_role: discord.Option(discord.Role, "Birthday role", required=False),
    member_role: discord.Option(discord.Role, "Member join role", required=False),
    bot_role: discord.Option(discord.Role, "Role given to bots on join", required=False),
    deadchat_role: discord.Option(discord.Role, "Dead Chat role", required=False),
    infected_role: discord.Option(discord.Role, "Plague infected role", required=False),
    active_role: discord.Option(discord.Role, "Active member role", required=False),
    deadchat_trigger_channels: discord.Option(str, "Dead Chat channel IDs (comma separated)", required=False),
    autodelete_channels: discord.Option(str, "Auto-delete channel IDs (comma separated)", required=False),
    mod_log_channel: discord.Option(discord.TextChannel, "Mod log thread/channel", required=False),
    bot_log_channel: discord.Option(discord.TextChannel, "Bot log thread/channel", required=False),
    prize_drop_channel: discord.Option(discord.TextChannel, "Prize drop channel", required=False),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    cfg = ensure_guild_config(ctx.guild)
    cfg["welcome_channel_id"] = welcome_channel.id
    if storage_channel:
        cfg["storage_channel_id"] = storage_channel.id
    if birthday_role:
        cfg["birthday_role_id"] = birthday_role.id
    if member_role:
        cfg["member_join_role_id"] = member_role.id
    if bot_role:
        cfg["bot_join_role_id"] = bot_role.id
    if deadchat_role:
        cfg["dead_chat_role_id"] = deadchat_role.id
    if infected_role:
        cfg["infected_role_id"] = infected_role.id
    if active_role:
        cfg["active_role_id"] = active_role.id
    if deadchat_trigger_channels:
        try:
            cfg["dead_chat_channel_ids"] = [int(x.strip()) for x in deadchat_trigger_channels.split(",") if x.strip().isdigit()]
        except:
            pass
    if autodelete_channels:
        try:
            cfg["auto_delete_channel_ids"] = [int(x.strip()) for x in autodelete_channels.split(",") if x.strip().isdigit()]
        except:
            pass
    if mod_log_channel:
        cfg["mod_log_channel_id"] = mod_log_channel.id
    if bot_log_channel:
        cfg["bot_log_channel_id"] = bot_log_channel.id
    if prize_drop_channel:
        cfg["prize_drop_channel_id"] = prize_drop_channel.id
    await save_guild_config_storage()
    await log_to_bot_channel(f"[CONFIG] Setup updated for guild {ctx.guild.id} by {ctx.author.mention}.")
    await ctx.respond("Configuration saved for this server.", ephemeral=True)
    
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
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch:
        return await ctx.respond("Welcome channel not found.", ephemeral=True)
    msg = BIRTHDAY_TEXT.replace("{mention}", member.mention) if BIRTHDAY_TEXT else f"Happy birthday, {member.mention}!"
    await ch.send(msg)
    await log_to_bot_channel(
        f"[BIRTHDAY] Manual birthday announcement sent for {member.mention} by {ctx.author.mention}."
    )
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
async def prize_list(
    ctx,
    prize_type: discord.Option(str, choices=["movie", "nitro", "steam"], required=True),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    entries = get_prize_list_and_entries(prize_type)
    if not entries:
        return await ctx.respond("No scheduled prizes.", ephemeral=True)
    if len(entries) == 0:
        return await ctx.respond("No scheduled prizes.", ephemeral=True)
    lines = []
    for p in entries:
        lines.append(f"ID {p['id']} ┃ {p['date']} ┃ <#{p['channel_id']}>")
    text = "\n".join(lines)
    await ctx.respond(f"Scheduled {prize_type} prizes:\n{text}", ephemeral=True)

@bot.slash_command(name="prize_delete", description="Delete a scheduled prize")
async def prize_delete(
    ctx,
    prize_type: discord.Option(str, choices=["movie", "nitro", "steam"], required=True),
    prize_id: discord.Option(int, "ID from /prize_list", required=True),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    entries = get_prize_list_and_entries(prize_type)
    if entries is None:
        return await ctx.respond("Invalid prize type.", ephemeral=True)
    before = len(entries)
    entries[:] = [p for p in entries if p.get("id") != prize_id]
    after = len(entries)
    if before == after:
        return await ctx.respond("No prize with that ID.", ephemeral=True)
    await save_prize_storage()
    await ctx.respond(f"Deleted scheduled {prize_type} prize ID {prize_id}.", ephemeral=True)

@bot.slash_command(name="prize_movie")
async def prize_movie(
    ctx,
    month: discord.Option(str, "Month (UTC date)", required=False, choices=MONTH_CHOICES),
    day: discord.Option(int, "Day of month", required=False),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    content = "**YOU'VE FOUND A PRIZE!**\nPrize: *Movie Request*\nDrop Rate: *Common*"
    if month is None and day is None:
        drop_ch = bot.get_channel(PRIZE_DROP_CHANNEL_ID) or ctx.channel
        if isinstance(drop_ch, discord.TextChannel):
            await drop_ch.send(content, view=MoviePrizeView())
            await log_to_bot_channel(
                f"[PRIZE] Immediate movie prize drop triggered by {ctx.author.mention} in {drop_ch.mention}."
            )
            return await ctx.respond(f"Prize drop sent in {drop_ch.mention}.", ephemeral=True)
        await log_to_bot_channel(
            f"[PRIZE] Immediate movie prize drop triggered by {ctx.author.mention} in {ctx.channel.mention}."
        )
        return await ctx.respond(content, view=MoviePrizeView())
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
    await add_scheduled_prize("movie", ctx.channel.id, content, date_str)
    await log_to_bot_channel(
        f"[PRIZE] Scheduled movie prize on {date_str} by {ctx.author.mention} for {ctx.channel.mention}."
    )
    await ctx.respond(f"Scheduled for {date_str} (triggers on first Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC).", ephemeral=True)

@bot.slash_command(name="prize_nitro")
async def prize_nitro(
    ctx,
    month: discord.Option(str, "Month (UTC date)", required=False, choices=MONTH_CHOICES),
    day: discord.Option(int, "Day of month", required=False),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    content = "**YOU'VE FOUND A PRIZE!**\nPrize: *Month of Nitro Basic*\nDrop Rate: *Uncommon*"
    if month is None and day is None:
        drop_ch = bot.get_channel(PRIZE_DROP_CHANNEL_ID) or ctx.channel
        if isinstance(drop_ch, discord.TextChannel):
            await drop_ch.send(content, view=NitroPrizeView())
            await log_to_bot_channel(
                f"[PRIZE] Immediate nitro prize drop triggered by {ctx.author.mention} in {drop_ch.mention}."
            )
            return await ctx.respond(f"Prize drop sent in {drop_ch.mention}.", ephemeral=True)
        await log_to_bot_channel(
            f"[PRIZE] Immediate nitro prize drop triggered by {ctx.author.mention} in {ctx.channel.mention}."
        )
        return await ctx.respond(content, view=NitroPrizeView())
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
    await add_scheduled_prize("nitro", ctx.channel.id, content, date_str)
    await log_to_bot_channel(
        f"[PRIZE] Scheduled nitro prize on {date_str} by {ctx.author.mention} for {ctx.channel.mention}."
    )
    await ctx.respond(f"Scheduled for {date_str} (triggers on first Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC).", ephemeral=True)

@bot.slash_command(name="prize_steam")
async def prize_steam(
    ctx,
    month: discord.Option(str, "Month (UTC date)", required=False, choices=MONTH_CHOICES),
    day: discord.Option(int, "Day of month", required=False),
):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    content = "**YOU'VE FOUND A PRIZE!**\nPrize: *Steam Gift Card*\nDrop Rate: *Rare*"
    if month is None and day is None:
        drop_ch = bot.get_channel(PRIZE_DROP_CHANNEL_ID) or ctx.channel
        if isinstance(drop_ch, discord.TextChannel):
            await drop_ch.send(content, view=SteamPrizeView())
            await log_to_bot_channel(
                f"[PRIZE] Immediate steam prize drop triggered by {ctx.author.mention} in {drop_ch.mention}."
            )
            return await ctx.respond(f"Prize drop sent in {drop_ch.mention}.", ephemeral=True)
        await log_to_bot_channel(
            f"[PRIZE] Immediate steam prize drop triggered by {ctx.author.mention} in {ctx.channel.mention}."
        )
        return await ctx.respond(content, view=SteamPrizeView())
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
    await add_scheduled_prize("steam", ctx.channel.id, content, date_str)
    await log_to_bot_channel(
        f"[PRIZE] Scheduled steam prize on {date_str} by {ctx.author.mention} for {ctx.channel.mention}."
    )
    await ctx.respond(f"Scheduled for {date_str} (triggers on first Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC).", ephemeral=True)

@bot.slash_command(name="prize_announce")
async def prize_announce(ctx, member: discord.Option(discord.Member, required=True), prize: discord.Option(str, choices=list(PRIZE_DEFS.keys()), required=True)):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    dead_role = ctx.guild.get_role(DEAD_CHAT_ROLE_ID)
    role_mention = dead_role.mention if dead_role else "the Dead Chat role"
    rarity = PRIZE_DEFS[prize]
    msg = PRIZE_ANNOUNCE_MESSAGE.format(
        emoji=PRIZE_EMOJI,
        winner=member.mention,
        gift=prize,
        role=role_mention,
        rarity=rarity
    )
    await ctx.channel.send(msg)
    await log_to_bot_channel(
        f"[PRIZE] Manual prize announcement: {member.mention} → '{prize}' (rarity {rarity}) by {ctx.author.mention}."
    )
    await ctx.respond("Announcement sent.", ephemeral=True)

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
                await log_to_bot_channel(
                    f"[STICKY] Updated sticky in {ctx.channel.mention} by {ctx.author.mention}."
                )
            except discord.NotFound:
                msg = await ctx.channel.send(text)
                sticky_messages[ctx.channel.id] = msg.id
                await ctx.respond("Sticky created.", ephemeral=True)
                await log_to_bot_channel(
                    f"[STICKY] Created sticky in {ctx.channel.mention} by {ctx.author.mention}."
                )
        else:
            msg = await ctx.channel.send(text)
            sticky_messages[ctx.channel.id] = msg.id
            await ctx.respond("Sticky created.", ephemeral=True)
            await log_to_bot_channel(
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
        await log_to_bot_channel(
            f"[STICKY] Cleared sticky in {ctx.channel.mention} by {ctx.author.mention}."
        )

@bot.slash_command(name="plague_init", description="Create plague storage message (run once)")
async def plague_init(ctx):
    if not ctx.author.guild_permissions.administrator:
        return await ctx.respond("Admin only.", ephemeral=True)
    ch = bot.get_channel(STORAGE_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await ctx.respond("Invalid storage channel", ephemeral=True)
    msg = await ch.send("PLAGUE_DATA:[]")
    global plague_storage_message_id
    plague_storage_message_id = msg.id
    await save_plague_storage()
    await ctx.respond(f"Plague storage created: {msg.id}", ephemeral=True)

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
    plague_scheduled.clear()
    plague_scheduled.append(
        {
            "date": date_str,
        }
    )
    await save_plague_storage()
    await log_to_bot_channel(
        f"[PLAGUE] Plague scheduled for {date_str} by {ctx.author.mention}."
    )
    if month is None:
        await ctx.respond(f"Plague set for today. First Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC will be infected.", ephemeral=True)
    else:
        await ctx.respond(f"Plague scheduled for {date_str}. First Dead Chat steal after {PRIZE_PLAGUE_TRIGGER_HOUR_UTC}:00 UTC will be infected.", ephemeral=True)


############### ON_READY & BOT START ###############
bot.run(os.getenv("TOKEN"))
