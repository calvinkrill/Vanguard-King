import os
import json
import datetime
import asyncio
import logging
import time
import hashlib
import uuid
import re
import io
from collections import defaultdict, deque
from urllib.parse import quote, urlparse
from pathlib import Path
from typing import Optional, Union
import aiohttp
import discord
import yt_dlp
from discord import app_commands
from discord.ext import commands, tasks
from deep_translator import GoogleTranslator
from langdetect import DetectorFactory, LangDetectException, detect
from env_utils import load_dotenv
from keep_alive import keep_alive
import database # Import database module
from slash_commands.extended_systems import (
    AgendaAttendanceView,
    handle_giveaway_reaction,
    process_agenda_reminders,
    process_birthdays,
    process_due_giveaways,
    process_due_polls,
    register_extended_slash_commands,
)

# Load environment variables
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
if TOKEN:
    TOKEN = TOKEN.strip().strip('`').strip('"').strip("'")
DASHBOARD_URL = (os.getenv('DASHBOARD_URL') or "").strip()
# DATA_DIR = "data" # No longer needed for main storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configure intents
intents = discord.Intents.default()
intents.members = True  # Required to detect member joins and updates
intents.message_content = True # Required for reading commands


DEFAULT_PREFIX = '!'


def _sanitize_prefix(value: str | None) -> str:
    if not value:
        return DEFAULT_PREFIX
    cleaned = str(value).strip()
    if not cleaned or len(cleaned) > 8 or any(ch.isspace() for ch in cleaned):
        return DEFAULT_PREFIX
    return cleaned


def _get_guild_prefix(guild: discord.Guild | None) -> str:
    if guild is None:
        return DEFAULT_PREFIX

    config = database.get_guild_config(guild.id) or {}
    return _sanitize_prefix(config.get('bot_prefix'))


def get_dynamic_prefix(_bot, message: discord.Message):
    return _get_guild_prefix(message.guild)


bot = commands.Bot(command_prefix=get_dynamic_prefix, intents=intents, case_insensitive=True)
bot.remove_command('help')
register_extended_slash_commands(bot)
sticky_channels = {}
last_revive_ping_hour = {}
server_copy_cache = {}
server_revert_cache = {}
# Runtime guard against posting the same source message quote repeatedly.
quoted_source_message_ids = set()
staff_duty_dm_notices = {}
anniversary_daily_runs = {}
afk_statuses = {}
AFK_TAG = " [AFK]"

PET_TYPES = {
    "dog": "Dog",
    "cat": "Cat",
    "dragon": "Dragon",
    "fox": "Fox",
    "rabbit": "Rabbit",
}
PET_TYPE_ALIASES = {k.lower(): v for k, v in PET_TYPES.items()}
PET_COOLDOWNS_SECONDS = {
    "feed": 6 * 3600,
    "play": 4 * 3600,
    "clean": 8 * 3600,
    "sleep": 10 * 3600,
    "daily": 20 * 3600,
}
PET_STAT_MAX = 100
STREAK_TIMEZONE = datetime.timezone(datetime.timedelta(hours=8))
PH_TIMEZONE = datetime.timezone(datetime.timedelta(hours=8))
INVITE_LINK_REGEX = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite)/[A-Za-z0-9-]+",
    re.IGNORECASE,
)
GENERIC_LINK_REGEX = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
KNOWN_SAFE_LINK_DOMAINS = {
    "discord.com",
    "discord.gg",
    "discordapp.com",
    "youtube.com",
    "youtu.be",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "instagram.com",
    "facebook.com",
    "threads.net",
    "twitch.tv",
    "spotify.com",
    "open.spotify.com",
    "github.com",
    "imgur.com",
    "tenor.com",
    "giphy.com",
}
AUTOMOD_SPAM_WINDOW_SECONDS = 4
AUTOMOD_SPAM_THRESHOLD = 6
AUTOMOD_PROFANITY_REGEX = re.compile(r"[^\W_]+", re.UNICODE)
AUTOMOD_LEETSPEAK_MAP = str.maketrans({
    "0": "o",
    "1": "i",
    "3": "e",
    "4": "a",
    "5": "s",
    "7": "t",
    "@": "a",
    "$": "s",
    "!": "i",
})
# NOTE: This is intentionally broad and multilingual, but no static list can cover every
# bad word in every language. Admins can extend this list as needed.
AUTOMOD_BAD_WORDS = {
    "abortionista", "anal", "anus", "arse", "arsehole", "ass", "assfucker", "asshole", "bastard", "bitch",
    "blowjob", "bollock", "bollocks", "boob", "boobs", "bugger", "cabron", "caralho", "chingar", "chink",
    "choad", "christonabike", "clit", "cock", "con", "coño", "cretin", "cum", "cunt", "damn", "dick",
    "dildo", "dipshit", "douche", "dumbass", "dyke", "enculer", "fag", "faggot", "fck", "feces", "felch",
    "foda", "foder", "fornicate", "fotze", "frigger", "fuck", "fucker", "fucking", "fuk", "fuker", "goddamn",
    "hell", "hijoeputa", "ho", "hoe", "idiot", "imbecil", "jackass", "jerkoff", "joder", "kanker",
    "khara", "kys", "lameass", "lesbo", "mamaguevo", "marica", "mierda", "milf", "motherfucker", "nazi",
    "nigga", "nigger", "nutsack", "paki", "pendejo", "penis", "perra", "phuck", "piss", "porn", "porno",
    "puta", "putain", "puto", "queer", "rape", "rapist", "retard", "rimjob", "schlampe", "scheisse",
    "shit", "shite", "slut", "spastic", "stfu", "suckdick", "testicle", "tit", "tosser", "twat", "vajina",
    "verga", "violar", "wanker", "whore", "zorra",
}
automod_message_windows: dict[tuple[int, int], deque[float]] = defaultdict(deque)


def _utc_now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _iso_now() -> str:
    return _utc_now().isoformat() + "Z"


def _parse_iso(ts: str | None) -> datetime.datetime | None:
    if not ts:
        return None
    cleaned = ts.replace("Z", "+00:00")
    try:
        parsed = datetime.datetime.fromisoformat(cleaned)
        if parsed.tzinfo:
            parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def _clamp_stat(value: int) -> int:
    return max(0, min(PET_STAT_MAX, int(value)))


def _extract_message_links(content: str) -> list[str]:
    if not content:
        return []
    return GENERIC_LINK_REGEX.findall(content)


def _is_known_safe_link(link: str) -> bool:
    parsed = urlparse(link)
    host = (parsed.netloc or "").lower()
    if not host:
        return False
    if host.startswith("www."):
        host = host[4:]
    return any(host == domain or host.endswith(f".{domain}") for domain in KNOWN_SAFE_LINK_DOMAINS)


def _normalize_automod_token(token: str) -> str:
    lowered = token.lower().translate(AUTOMOD_LEETSPEAK_MAP)
    return re.sub(r"(.)\1{2,}", r"\1\1", lowered)


def _contains_bad_word(content: str) -> bool:
    if not content:
        return False

    for token in AUTOMOD_PROFANITY_REGEX.findall(content):
        normalized = _normalize_automod_token(token)
        if normalized in AUTOMOD_BAD_WORDS:
            return True
    return False


def _is_spam_burst(guild_id: int, user_id: int) -> bool:
    key = (guild_id, user_id)
    now = time.monotonic()
    timestamps = automod_message_windows[key]
    timestamps.append(now)

    while timestamps and now - timestamps[0] > AUTOMOD_SPAM_WINDOW_SECONDS:
        timestamps.popleft()

    return len(timestamps) >= AUTOMOD_SPAM_THRESHOLD


def _parse_anniversary_milestones(raw: str | None) -> list[int]:
    if not raw:
        return list(range(1, 51))
    cleaned = raw.lower().replace("years", "").replace("year", "").replace(" ", "")
    milestones = set()
    for token in cleaned.split(","):
        if not token:
            continue
        if token.endswith("+"):
            base = int(token[:-1])
            for value in range(max(1, base), 201):
                milestones.add(value)
            continue
        if "-" in token:
            start_s, end_s = token.split("-", 1)
            start, end = int(start_s), int(end_s)
            if start > end:
                start, end = end, start
            for value in range(max(1, start), min(end, 200) + 1):
                milestones.add(value)
        else:
            value = int(token)
            if value > 0:
                milestones.add(min(value, 200))
    return sorted(milestones)


def _format_ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _anniversary_embed(
    guild: discord.Guild,
    title: str,
    description: str,
    *,
    color: discord.Color = discord.Color.gold(),
) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    embed.set_footer(text=guild.name, icon_url=guild.icon.url if guild.icon else None)
    return embed


async def _send_anniversary_dm(
    member: discord.Member,
    guild: discord.Guild,
    template: str,
    years: int,
):
    dm_message = template.format(
        server_name=guild.name,
        years=years,
        ordinal=_format_ordinal(years),
        user=member.mention,
    )
    dm_embed = _anniversary_embed(
        guild,
        "🎉 Anniversary Role Awarded",
        dm_message,
        color=discord.Color.green(),
    )
    try:
        await member.send(embed=dm_embed)
    except (discord.Forbidden, discord.HTTPException):
        pass


async def _process_anniversary_rewards(guild: discord.Guild):
    config = database.get_anniversary_config(guild.id)
    if not config:
        return

    today = datetime.datetime.utcnow().date()
    daily_key = today.isoformat()
    if anniversary_daily_runs.get(guild.id) == daily_key:
        return

    configured_md = (config.get("anniversary_date_md") or "").strip()
    today_md = today.strftime("%m-%d")
    if configured_md != today_md:
        anniversary_daily_runs[guild.id] = daily_key
        return

    role = guild.get_role(int(config["role_id"]))
    if not role:
        anniversary_daily_runs[guild.id] = daily_key
        return

    milestones = set(int(year) for year in (config.get("milestone_years") or []))
    if not milestones:
        anniversary_daily_runs[guild.id] = daily_key
        return

    destination = guild.get_channel(config.get("channel_id") or 0) if config.get("channel_id") else guild.system_channel
    template = config.get("message_template") or "Happy anniversary {server_name}!"
    created_date = guild.created_at.astimezone(datetime.timezone.utc).date() if guild.created_at else today
    years = today.year - created_date.year
    if years <= 0 or years not in milestones:
        anniversary_daily_runs[guild.id] = daily_key
        return

    awarded_members = []
    for member in guild.members:
        if member.bot:
            continue
        if database.has_anniversary_award(guild.id, member.id, years, daily_key):
            continue
        try:
            if role not in member.roles:
                await member.add_roles(role, reason=f"{years} year server anniversary reward")
        except discord.Forbidden:
            continue
        inserted = database.add_anniversary_award(guild.id, member.id, years, daily_key)
        if inserted:
            awarded_members.append(member)
            await _send_anniversary_dm(member, guild, template, years)

    anniversary_daily_runs[guild.id] = daily_key

    if destination and awarded_members:
        ping_role = guild.get_role(int(config.get("ping_role_id") or 0)) if config.get("ping_role_id") else None
        for member in awarded_members[:20]:
            message = template.format(
                server_name=guild.name,
                years=years,
                ordinal=_format_ordinal(years),
                user=member.mention,
            )
            award_embed = _anniversary_embed(guild, "🎉 Happy Anniversary!", message)
            content = ping_role.mention if ping_role else None
            await destination.send(content=content, embed=award_embed)


def _get_anniversary_reward_year_if_eligible(
    guild: discord.Guild,
    config: dict | None,
    *,
    today: datetime.date | None = None,
) -> int | None:
    """Return milestone years when the guild is eligible for anniversary rewards today, else None."""
    if not config:
        return None

    check_date = today or datetime.datetime.utcnow().date()
    configured_md = (config.get("anniversary_date_md") or "").strip()
    if configured_md != check_date.strftime("%m-%d"):
        return None

    milestones = set(int(year) for year in (config.get("milestone_years") or []))
    if not milestones:
        return None

    created_date = guild.created_at.astimezone(datetime.timezone.utc).date() if guild.created_at else check_date
    years = check_date.year - created_date.year
    if years <= 0 or years not in milestones:
        return None
    return years


def _default_pet_profile(pet_type: str) -> dict:
    now_iso = _iso_now()
    return {
        "pet_name": PET_TYPES.get(pet_type, pet_type.title()),
        "pet_type": pet_type,
        "hunger": 80,
        "happiness": 80,
        "cleanliness": 80,
        "energy": 80,
        "bond": 0,
        "coins": 0,
        "streak": 0,
        "total_checkins": 0,
        "last_checkin_date": None,
        "adopted_at": now_iso,
        "updated_at": now_iso,
        "last_fed_at": None,
        "last_played_at": None,
        "last_cleaned_at": None,
        "last_slept_at": None,
        "evolved_stage": "base",
    }


def _bond_tier(bond: int) -> str:
    if bond < 10:
        return "Stranger 😐"
    if bond < 30:
        return "Friend 🙂"
    if bond < 60:
        return "Close Companion 🐾"
    return "Best Friend 💖"


def _pet_form_label(pet_type: str, bond: int, days_together: int) -> str:
    evolved = bond >= 60 and days_together >= 7
    if not evolved:
        return PET_TYPES.get(pet_type, pet_type.title())
    evolved_map = {
        "dog": "Guard Dog 🐕‍🦺",
        "cat": "Mystic Cat 🐈‍⬛",
        "dragon": "Fire Dragon 🔥🐉",
        "fox": "Spirit Fox ✨🦊",
        "rabbit": "Moon Rabbit 🌙🐇",
    }
    return evolved_map.get(pet_type, f"Evolved {pet_type.title()}")


def _format_remaining(seconds_left: int) -> str:
    hours = seconds_left // 3600
    minutes = (seconds_left % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _cooldown_remaining(profile: dict, action_key: str) -> int:
    last_field_map = {
        "feed": "last_fed_at",
        "play": "last_played_at",
        "clean": "last_cleaned_at",
        "sleep": "last_slept_at",
        "daily": "last_checkin_date",
    }
    ts = _parse_iso(profile.get(last_field_map[action_key]))
    if not ts:
        return 0
    elapsed = (_utc_now() - ts).total_seconds()
    return max(0, int(PET_COOLDOWNS_SECONDS[action_key] - elapsed))


def _apply_pet_decay(profile: dict) -> tuple[dict, bool]:
    updated_at = _parse_iso(profile.get("updated_at"))
    if not updated_at:
        profile["updated_at"] = _iso_now()
        return profile, True

    elapsed_hours = (_utc_now() - updated_at).total_seconds() / 3600
    decay_ticks = int(elapsed_hours // 6)
    if decay_ticks <= 0:
        return profile, False

    profile["hunger"] = _clamp_stat(profile.get("hunger", 0) - (2 * decay_ticks))
    profile["happiness"] = _clamp_stat(profile.get("happiness", 0) - (2 * decay_ticks))
    profile["cleanliness"] = _clamp_stat(profile.get("cleanliness", 0) - (2 * decay_ticks))
    profile["energy"] = _clamp_stat(profile.get("energy", 0) - (2 * decay_ticks))
    profile["updated_at"] = _iso_now()
    return profile, True


def _days_between(start_iso: str | None, end_dt: datetime.datetime | None = None) -> int:
    start = _parse_iso(start_iso)
    if not start:
        return 0
    now = end_dt or _utc_now()
    return max(0, (now.date() - start.date()).days)


def _streak_now() -> datetime.datetime:
    return datetime.datetime.now(STREAK_TIMEZONE)


def _streak_window_key(now: datetime.datetime) -> str:
    slot = "am" if now.hour < 12 else "pm"
    return f"{now.date().isoformat()}-{slot}"


def _streak_status_for_claim(current: dict | None) -> tuple[dict, bool]:
    now = _streak_now()
    today = now.date()
    window_key = _streak_window_key(now)
    current = current or {}

    previous_date_raw = current.get("last_claim_date")
    previous_date = None
    if previous_date_raw:
        try:
            previous_date = datetime.date.fromisoformat(previous_date_raw)
        except ValueError:
            previous_date = None

    current_streak = int(current.get("streak_days") or 0)
    if current.get("last_claim_window") == window_key:
        return {
            "streak_days": current_streak,
            "last_claim_date": today.isoformat(),
            "last_claim_window": window_key,
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        }, False

    if previous_date is None:
        new_streak = 1
    elif previous_date == today:
        new_streak = current_streak
    elif previous_date == (today - datetime.timedelta(days=1)):
        new_streak = current_streak + 1
    else:
        new_streak = 1

    return {
        "streak_days": new_streak,
        "last_claim_date": today.isoformat(),
        "last_claim_window": window_key,
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
    }, True


def _streak_window_bounds(now: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
    window_start_hour = 0 if now.hour < 12 else 12
    start = now.replace(hour=window_start_hour, minute=0, second=0, microsecond=0)
    end = start + datetime.timedelta(hours=12)
    return start, end


def _format_streak_status_message(payload: dict | None) -> str:
    now = _streak_now()
    start, end = _streak_window_bounds(now)

    if payload and payload.get("last_claim_window") == _streak_window_key(now):
        claim_state = "✅ Claimed in this window"
    else:
        claim_state = "🕒 Not claimed in this window yet"

    streak_days = int((payload or {}).get("streak_days") or 0)
    last_claim_date = (payload or {}).get("last_claim_date") or "Never"

    return (
        f"🔥 Current streak: **{streak_days}** day{'s' if streak_days != 1 else ''}\n"
        f"📅 Last claim date (PH): **{last_claim_date}**\n"
        f"{claim_state}\n"
        f"🪟 Current window (PH): **{start.strftime('%I:%M %p')} - {end.strftime('%I:%M %p')}**\n"
        f"⏭️ Next window starts: **{end.strftime('%Y-%m-%d %I:%M %p')} PH**"
    )


def _format_ph_time(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(PH_TIMEZONE).strftime("%I:%M %p").lstrip("0")


async def _send_staff_duty_dm(member: discord.Member, message_text: str, dedupe_key: str | None = None) -> bool:
    if dedupe_key:
        cache_key = (member.guild.id, member.id, dedupe_key)
        if staff_duty_dm_notices.get(cache_key):
            return False

    try:
        await member.send(message_text)
        if dedupe_key:
            staff_duty_dm_notices[(member.guild.id, member.id, dedupe_key)] = True
        return True
    except discord.Forbidden:
        return False
    except discord.HTTPException:
        return False


def _format_duration(duration: datetime.timedelta) -> str:
    total_minutes = max(0, int(duration.total_seconds() // 60))
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours}h {minutes}m"




def build_revive_style_embed(*, title: str, body_lines: list[str], guild: discord.Guild | None = None, requested_by: Union[discord.Member, discord.User, None] = None) -> discord.Embed:
    """Build a cinematic dark embed style for revive/mention bot messaging."""
    description = "\n".join(body_lines)
    embed = discord.Embed(
        title=title,
        description=description,
        color=discord.Color.from_rgb(10, 10, 14)
    )

    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    footer_parts = []
    if requested_by:
        footer_parts.append(f"Requested by {requested_by.display_name}")
    if guild:
        footer_parts.append(guild.name)
    if footer_parts:
        embed.set_footer(text=" • ".join(footer_parts))

    return embed


def resolve_persistent_data_root() -> Path:
    """Resolve a writable directory that survives restarts/redeploys when a volume is mounted."""
    running_on_hosted = any(
        (os.getenv(key) or "").strip()
        for key in (
            "RAILWAY_ENVIRONMENT",
            "RAILWAY_ENVIRONMENT_NAME",
            "RAILWAY_PROJECT_ID",
            "RENDER",
            "RENDER_SERVICE_ID",
            "KOYEB_SERVICE_ID",
        )
    )
    require_persistent = (os.getenv("REQUIRE_PERSISTENT_STORAGE", "0") or "").strip().lower() in {
        "1", "true", "yes", "on"
    }
    configured_dir = os.getenv("BOT_DATA_DIR") or os.getenv("DB_DIR")
    if configured_dir:
        root = Path(configured_dir)
        root.mkdir(parents=True, exist_ok=True)
        return root

    platform_volume_dir = (
        os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
        or os.getenv("RENDER_DISK_PATH")
        or os.getenv("PERSISTENT_VOLUME_DIR")
    )
    if platform_volume_dir:
        root = Path(platform_volume_dir)
        root.mkdir(parents=True, exist_ok=True)
        return root

    default_persistent = Path("/data")
    if default_persistent.exists() and default_persistent.is_dir():
        return default_persistent

    if require_persistent:
        raise RuntimeError(
            "REQUIRE_PERSISTENT_STORAGE=1 is set, but no persistent data volume was detected. "
            "Mount a persistent disk (for example at /data) or set BOT_DATA_DIR/DB_DIR to a persistent path."
        )
    if running_on_hosted:
        logger.warning(
            "No persistent data volume detected on hosted platform; using ephemeral local storage. "
            "Configure BOT_DATA_DIR/DB_DIR or mount a disk to persist bot data."
        )

    fallback = Path("data")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


PERSISTENT_DATA_ROOT = resolve_persistent_data_root()

# Configuration
SUFFIX = ""
# Set this to the name of the role that triggers the nickname change
# If None, it will trigger on ANY role change (which might be spammy, so be careful)
TRIGGER_ROLE_NAME = None 
BLIND_DATE_DATA_DIR = Path(os.getenv("BLIND_DATE_DATA_DIR", str(PERSISTENT_DATA_ROOT / "blind_date_data")))
BLIND_DATE_SELF_MATCH_TESTING = True
LOFI_DATA_DIR = Path(os.getenv("LOFI_DATA_DIR", str(PERSISTENT_DATA_ROOT / "lofi_data")))
STICKY_STATE_FILE = Path(os.getenv("STICKY_STATE_FILE", str(PERSISTENT_DATA_ROOT / "sticky_channels.json")))
LOG_SETUP_FILE = Path(os.getenv("LOG_SETUP_FILE", str(PERSISTENT_DATA_ROOT / "logs.json")))
DEFAULT_LOFI_STREAM_URL = os.getenv("DEFAULT_LOFI_STREAM_URL", "https://play.streamafrica.net/lofiradio")
DEFAULT_LOFI_VOLUME = 1.0
MIN_AUDIBLE_LOFI_VOLUME = 0.15
LOFI_MAX_BACKOFF_SECONDS = 300
YTDL_FORMAT_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch1",
    "source_address": "0.0.0.0",
}
lofi_states = {}
DetectorFactory.seed = 0




def load_sticky_channels() -> dict:
    if not STICKY_STATE_FILE.exists():
        return {}
    try:
        payload = json.loads(STICKY_STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}

        restored = {}
        for channel_id, sticky_info in payload.items():
            if not str(channel_id).isdigit() or not isinstance(sticky_info, dict):
                continue
            content = sticky_info.get("content")
            message_id = sticky_info.get("message_id")
            if not isinstance(content, str) or not content.strip():
                continue
            restored[int(channel_id)] = {
                "content": content,
                "message_id": int(message_id) if str(message_id).isdigit() else None
            }
        return restored
    except Exception as e:
        logger.warning("Failed to load sticky channel state: %s", e)
        return {}


def save_sticky_channels():
    try:
        STICKY_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        serialized = {str(channel_id): sticky_info for channel_id, sticky_info in sticky_channels.items()}
        STICKY_STATE_FILE.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("Failed to persist sticky channel state: %s", e)


def load_log_setup_data() -> dict:
    if not LOG_SETUP_FILE.exists():
        return {}
    try:
        payload = json.loads(LOG_SETUP_FILE.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception as e:
        logger.warning("Failed to load log setup file: %s", e)
        return {}


def save_log_setup_data(payload: dict):
    LOG_SETUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_SETUP_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_guild_logs(guild_id: int) -> dict:
    payload = load_log_setup_data()
    guild_payload = payload.get(str(guild_id), {})
    if not isinstance(guild_payload, dict):
        return {}
    logs = guild_payload.get("logs", {})
    return logs if isinstance(logs, dict) else {}


SERVER_LOG_TYPES: list[tuple[str, str, str]] = [
    ("server_updates", "server-updates", "Server updates (icon/name/settings updates)"),
    ("server", "server-log", "General server logs"),
    ("staff", "staff-log", "Staff actions and staff-related logs"),
    ("confession", "confession-log", "Confession activity logs"),
    ("permission", "permission-log", "Role/permission change logs"),
    ("moderation", "mods-log", "Moderation actions"),
    ("automod", "automod-log", "AutoMod actions"),
    ("actions", "action-log", "General action logs"),
    ("events", "event-log", "Event logs"),
    ("voice", "vc-log", "Voice activity logs"),
    ("invites", "invites-log", "Invite activity logs"),
    ("join_leave", "join_leave-log", "Join/leave member logs"),
    ("tickets", "ticket-log", "Ticket system logs"),
    ("deleted", "deleted-log", "Deleted message logs"),
    ("posts", "post-log", "New message post logs"),
    ("updated", "updated-log", "Edited message logs"),
]


LOG_FALLBACK_KEYS: dict[str, list[str]] = {
    "join_leave": ["server"],
}


def _resolve_log_channel(guild: discord.Guild, log_key: str) -> discord.TextChannel | None:
    logs = get_guild_logs(guild.id)
    candidate_keys = [log_key] + LOG_FALLBACK_KEYS.get(log_key, [])
    for key in candidate_keys:
        channel_id = logs.get(key)
        if not channel_id:
            continue
        channel = guild.get_channel(int(channel_id))
        if isinstance(channel, discord.TextChannel):
            return channel
    return None


async def _send_configured_log_embed(
    guild: discord.Guild,
    log_key: str,
    *,
    title: str,
    color: discord.Color,
    fields: list[tuple[str, str, bool]],
):
    log_channel = _resolve_log_channel(guild, log_key)
    if log_channel is None:
        return

    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=discord.utils.utcnow(),
    )
    for name, value, inline in fields:
        embed.add_field(name=name, value=_truncate_for_embed(value, 1024), inline=inline)

    try:
        await log_channel.send(embed=embed)
    except Exception as e:
        logger.warning("Failed to send configured log `%s` in guild %s: %s", log_key, guild.id, e)


def _truncate_for_embed(text: str, limit: int = 1000) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return "No text content"
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."


async def _send_deleted_message_log(
    *,
    guild: discord.Guild,
    channel_id: int | None,
    message_id: int | None,
    author: discord.abc.User | discord.Member | None = None,
    content: str | None = None,
    attachment_urls: list[str] | None = None,
):
    logs = get_guild_logs(guild.id)
    deleted_channel_id = logs.get("deleted")
    if not deleted_channel_id:
        return

    log_channel = guild.get_channel(int(deleted_channel_id))
    if not isinstance(log_channel, discord.TextChannel):
        return

    source_channel = guild.get_channel(channel_id or 0) if channel_id else None
    attachment_urls = attachment_urls or []

    embed = discord.Embed(
        title="🗑️ Message Deleted",
        color=discord.Color.red(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(
        name="Author",
        value=author.mention if author else "Unknown",
        inline=True,
    )
    embed.add_field(
        name="Channel",
        value=source_channel.mention if isinstance(source_channel, discord.TextChannel) else f"`{channel_id}`",
        inline=True,
    )
    if message_id:
        embed.add_field(name="Message ID", value=f"`{message_id}`", inline=False)

    embed.add_field(
        name="Content",
        value=_truncate_for_embed(content or ""),
        inline=False,
    )
    if attachment_urls:
        preview = "\n".join(attachment_urls[:5])
        if len(attachment_urls) > 5:
            preview += f"\n...and {len(attachment_urls) - 5} more attachment(s)"
        embed.add_field(name="Attachments", value=_truncate_for_embed(preview), inline=False)

    try:
        await log_channel.send(embed=embed)
    except Exception as e:
        logger.warning("Failed to send deleted message log in guild %s: %s", guild.id, e)


def _blind_date_file_path(guild_id: int) -> Path:
    BLIND_DATE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return BLIND_DATE_DATA_DIR / f"{guild_id}.json"


def _lofi_file_path(guild_id: int) -> Path:
    LOFI_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return LOFI_DATA_DIR / f"{guild_id}.json"


def load_lofi_data(guild_id: int) -> dict:
    defaults = {
        "guild_id": guild_id,
        "enabled": False,
        "voice_channel_id": None,
        "stream_url": DEFAULT_LOFI_STREAM_URL,
        "volume": DEFAULT_LOFI_VOLUME,
        "music_queue": [],
        "now_playing": None,
        "backoff_seconds": 1,
        "next_retry_at": 0.0,
        "last_error": None
    }
    path = _lofi_file_path(guild_id)
    if not path.exists():
        return defaults
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return defaults
        for key, value in defaults.items():
            payload.setdefault(key, value)
        if not isinstance(payload.get("music_queue"), list):
            payload["music_queue"] = []
        if payload.get("now_playing") is not None and not isinstance(payload.get("now_playing"), dict):
            payload["now_playing"] = None
        try:
            payload["volume"] = max(0.0, min(2.0, float(payload.get("volume", DEFAULT_LOFI_VOLUME))))
        except (TypeError, ValueError):
            payload["volume"] = DEFAULT_LOFI_VOLUME
        return payload
    except Exception as e:
        logger.warning("Failed to load lofi config for guild %s: %s", guild_id, e)
        return defaults


def save_lofi_data(guild_id: int, payload: dict):
    path = _lofi_file_path(guild_id)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_lofi_state(guild_id: int) -> dict:
    state = lofi_states.get(guild_id)
    if state is None:
        state = load_lofi_data(guild_id)
        lofi_states[guild_id] = state
    return state


def build_lofi_audio_source(stream_url: str, volume: float = DEFAULT_LOFI_VOLUME):
    raw_source = discord.FFmpegPCMAudio(
        stream_url,
        before_options="-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 30 -nostdin",
        options="-vn"
    )
    return discord.PCMVolumeTransformer(raw_source, volume=max(0.0, min(2.0, float(volume))))


def _on_lofi_playback_end(guild_id: int, error):
    if error:
        logger.warning("Lofi playback ended with error in guild %s: %s", guild_id, error)
    async def _restart():
        await restart_lofi_playback(guild_id, reason=f"stream ended ({error})" if error else "stream ended")
    bot.loop.call_soon_threadsafe(lambda: asyncio.create_task(_restart()))


async def restart_lofi_playback(guild_id: int, reason: str = "manual restart"):
    state = get_lofi_state(guild_id)
    if not state.get("enabled"):
        return
    guild = bot.get_guild(guild_id)
    if not guild:
        return
    voice_client = guild.voice_client
    if not voice_client or not voice_client.is_connected():
        return
    try:
        if voice_client.is_playing():
            voice_client.stop()
        voice_client.play(
            build_lofi_audio_source(
                state.get("stream_url") or DEFAULT_LOFI_STREAM_URL,
                volume=state.get("volume", DEFAULT_LOFI_VOLUME)
            ),
            after=lambda error: _on_lofi_playback_end(guild_id, error)
        )
        state["last_error"] = None
        state["backoff_seconds"] = 1
        state["next_retry_at"] = 0.0
        save_lofi_data(guild_id, state)
        logger.info("Lofi playback restarted for guild %s (%s)", guild_id, reason)
    except Exception as e:
        logger.error("Failed to restart lofi playback for guild %s: %s", guild_id, e)
        state["last_error"] = str(e)
        wait_for = min(max(int(state.get("backoff_seconds", 1)), 1), LOFI_MAX_BACKOFF_SECONDS)
        state["next_retry_at"] = time.time() + wait_for
        state["backoff_seconds"] = min(wait_for * 2, LOFI_MAX_BACKOFF_SECONDS)
        save_lofi_data(guild_id, state)


async def ensure_lofi_connected(guild_id: int) -> tuple[bool, str]:
    state = get_lofi_state(guild_id)
    if not state.get("enabled"):
        return False, "Lofi mode is disabled."

    next_retry_at = float(state.get("next_retry_at") or 0.0)
    if next_retry_at and time.time() < next_retry_at:
        retry_in = max(1, int(next_retry_at - time.time()))
        return False, f"Reconnect backoff active. Next attempt in {retry_in}s."

    guild = bot.get_guild(guild_id)
    if not guild:
        return False, "Guild is unavailable."

    voice_channel_id = state.get("voice_channel_id")
    channel = guild.get_channel(voice_channel_id) if voice_channel_id else None
    if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
        return False, "Configured voice channel is missing."

    try:
        voice_client = guild.voice_client
        if voice_client and voice_client.channel and voice_client.channel.id != channel.id:
            await voice_client.move_to(channel)
        elif not voice_client or not voice_client.is_connected():
            voice_client = await channel.connect(reconnect=True, timeout=30.0)

        if voice_client and not voice_client.is_playing():
            voice_client.play(
                build_lofi_audio_source(
                    state.get("stream_url") or DEFAULT_LOFI_STREAM_URL,
                    volume=state.get("volume", DEFAULT_LOFI_VOLUME)
                ),
                after=lambda error: _on_lofi_playback_end(guild_id, error)
            )

        state["backoff_seconds"] = 1
        state["next_retry_at"] = 0.0
        state["last_error"] = None
        save_lofi_data(guild_id, state)
        return True, f"Connected to {channel.name} and streaming."
    except Exception as e:
        error_text = str(e)
        normalized_error = error_text.lower()
        missing_voice_dependency = (
            "library needed" in normalized_error and "use voice" in normalized_error
        ) or ("pynacl" in normalized_error)
        missing_ffmpeg_dependency = (
            "ffmpeg was not found" in normalized_error
            or "executable ffmpeg" in normalized_error
            or ("[errno 2]" in normalized_error and "ffmpeg" in normalized_error)
        )
        if missing_voice_dependency:
            state["enabled"] = False
            state["last_error"] = error_text
            state["next_retry_at"] = 0.0
            state["backoff_seconds"] = 1
            save_lofi_data(guild_id, state)
            logger.error("PyNaCl is missing; disabling lofi mode for guild %s until dependency is installed.", guild_id)
            return (
                False,
                "Voice playback is unavailable because **PyNaCl** is not installed on the host. "
                "Install `PyNaCl` and restart the bot, then run `/join` (or `!join`) again."
            )
        if missing_ffmpeg_dependency:
            state["enabled"] = False
            state["last_error"] = error_text
            state["next_retry_at"] = 0.0
            state["backoff_seconds"] = 1
            save_lofi_data(guild_id, state)
            logger.error("FFmpeg is missing; disabling lofi mode for guild %s until dependency is installed.", guild_id)
            return (
                False,
                "Voice playback is unavailable because **FFmpeg** is not installed on the host. "
                "Install `ffmpeg` and restart the bot, then run `/join` (or `!join`) again."
            )

        wait_for = min(max(int(state.get("backoff_seconds", 1)), 1), LOFI_MAX_BACKOFF_SECONDS)
        state["last_error"] = error_text
        state["next_retry_at"] = time.time() + wait_for
        state["backoff_seconds"] = min(wait_for * 2, LOFI_MAX_BACKOFF_SECONDS)
        save_lofi_data(guild_id, state)
        logger.warning("Lofi ensure connection failed for guild %s: %s", guild_id, error_text)
        return False, f"Connection/playback failed: {error_text}. Retrying in {wait_for}s."


def load_blind_date_data(guild_id: int) -> dict:
    path = _blind_date_file_path(guild_id)
    defaults = {
        "guild_id": guild_id,
        "category_id": None,
        "lobby_channel_id": None,
        "dashboard_channel_id": None,
        "dashboard_message_id": None,
        "queue_user_ids": [],
        "active_room_ids": []
    }

    if not path.exists():
        return defaults

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return defaults
        for key, value in defaults.items():
            payload.setdefault(key, value)
        if not isinstance(payload.get("queue_user_ids"), list):
            payload["queue_user_ids"] = []
        if not isinstance(payload.get("active_room_ids"), list):
            payload["active_room_ids"] = []
        return payload
    except Exception as e:
        logger.warning("Failed to load blind date config for guild %s: %s", guild_id, e)
        return defaults


def save_blind_date_data(guild_id: int, payload: dict):
    path = _blind_date_file_path(guild_id)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _remove_user_from_blind_date_queue(payload: dict, user_id: int) -> bool:
    queue = payload.get("queue_user_ids", [])
    original_length = len(queue)
    payload["queue_user_ids"] = [uid for uid in queue if uid != user_id]
    return len(payload["queue_user_ids"]) != original_length


async def _create_blind_date_room(
    guild: discord.Guild,
    payload: dict,
    member_one: discord.Member,
    member_two: discord.Member
):
    category = guild.get_channel(payload.get("category_id"))
    if not isinstance(category, discord.CategoryChannel):
        raise RuntimeError("Blind Dating category is missing. Run /setupblinddate again.")

    room_name = f"Date Room {int(time.time())}"
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False),
        guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, manage_channels=True, move_members=True)
    }
    overwrites[member_one] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
    overwrites[member_two] = discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)

    room = await guild.create_voice_channel(
        name=room_name,
        category=category,
        overwrites=overwrites,
        user_limit=2
    )
    payload.setdefault("active_room_ids", [])
    payload["active_room_ids"].append(room.id)
    payload["active_room_ids"] = list(dict.fromkeys(payload["active_room_ids"]))
    save_blind_date_data(guild.id, payload)

    move_failures = []
    moved = set()
    for member in (member_one, member_two):
        if member.id in moved:
            continue
        moved.add(member.id)
        try:
            if member.voice and member.voice.channel:
                await member.move_to(room)
            else:
                move_failures.append(member.mention)
        except Exception as e:
            logger.error("Failed to move %s (%s) to blind date room %s: %s", member.name, member.id, room.id, e)
            move_failures.append(member.mention)

    dashboard_channel = guild.get_channel(payload.get("dashboard_channel_id"))
    if isinstance(dashboard_channel, discord.TextChannel):
        users_text = f"{member_one.mention} + {member_two.mention}" if member_one.id != member_two.id else member_one.mention
        message = f"💘 Match found! {users_text} moved to {room.mention}."
        if move_failures:
            message += f"\n⚠️ Could not move: {', '.join(move_failures)}."
        await dashboard_channel.send(message)


async def process_blind_date_queue(guild: discord.Guild):
    payload = load_blind_date_data(guild.id)
    queue = payload.get("queue_user_ids", [])
    lobby_id = payload.get("lobby_channel_id")
    if not lobby_id:
        return

    lobby = guild.get_channel(lobby_id)
    if not isinstance(lobby, discord.VoiceChannel):
        return

    sanitized_queue = []
    for user_id in queue:
        member = guild.get_member(user_id)
        if member and member.voice and member.voice.channel and member.voice.channel.id == lobby.id:
            sanitized_queue.append(user_id)
    payload["queue_user_ids"] = sanitized_queue

    while len(payload["queue_user_ids"]) >= 2:
        first_id = payload["queue_user_ids"].pop(0)
        second_id = payload["queue_user_ids"].pop(0)
        member_one = guild.get_member(first_id)
        member_two = guild.get_member(second_id)
        if not member_one or not member_two:
            continue
        await _create_blind_date_room(guild, payload, member_one, member_two)

    save_blind_date_data(guild.id, payload)


class BlindDateMatchView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Match Me", style=discord.ButtonStyle.success, emoji="💘", custom_id="blind_date_match_me")
    async def match_me(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This button only works in a server.", ephemeral=True)
            return

        payload = load_blind_date_data(interaction.guild.id)
        lobby_id = payload.get("lobby_channel_id")
        if not lobby_id:
            await interaction.response.send_message("Blind Dating is not configured here. Ask an admin to run /setupblinddate.", ephemeral=True)
            return

        member = interaction.user
        if not member.voice or not member.voice.channel or member.voice.channel.id != lobby_id:
            await interaction.response.send_message("Join the Lobby voice channel first, then click **Match Me**.", ephemeral=True)
            return

        if member.id in payload.get("queue_user_ids", []):
            if BLIND_DATE_SELF_MATCH_TESTING:
                _remove_user_from_blind_date_queue(payload, member.id)
                save_blind_date_data(interaction.guild.id, payload)
                await _create_blind_date_room(interaction.guild, payload, member, member)
                await interaction.response.send_message("🧪 Self-match test triggered. Check your private date room.", ephemeral=True)
                return
            await interaction.response.send_message("You're already queued. Waiting for a match...", ephemeral=True)
            return

        payload.setdefault("queue_user_ids", [])
        payload["queue_user_ids"].append(member.id)
        save_blind_date_data(interaction.guild.id, payload)
        await process_blind_date_queue(interaction.guild)
        await interaction.response.send_message("✅ You're in the queue. Stay in Lobby while we find your match.", ephemeral=True)


def resolve_activity_type(status_type: str):
    if (status_type or "").lower() == "watching":
        return discord.ActivityType.watching
    if (status_type or "").lower() == "listening":
        return discord.ActivityType.listening
    return discord.ActivityType.playing


async def apply_persisted_presence():
    presence = database.get_bot_presence()
    status_text = (presence.get("status_text") or "").strip()
    if not status_text:
        await bot.change_presence(activity=None)
        return

    activity_type = resolve_activity_type(presence.get("status_type"))
    await bot.change_presence(activity=discord.Activity(type=activity_type, name=status_text))


async def set_and_persist_presence(status_type: str | None, status_text: str | None):
    """
    Save presence settings to the database first, then apply them to Discord.
    This keeps custom status durable across reconnects/restarts.
    """
    cleaned_type = (status_type or "playing").strip().lower() if status_text else None
    cleaned_text = (status_text or "").strip() or None

    database.set_bot_presence(cleaned_type, cleaned_text)

    if not cleaned_text:
        await bot.change_presence(activity=None)
        return

    activity_type = resolve_activity_type(cleaned_type)
    await bot.change_presence(activity=discord.Activity(type=activity_type, name=cleaned_text))

def can_manage_nick(ctx, member):
    """Checks if the bot has permission to change the member's nickname."""
    # Bot cannot change the server owner's nickname
    if member.id == ctx.guild.owner_id:
        return False, "I cannot change the Server Owner's nickname due to Discord's security limitations."
    
    # Bot cannot change nickname of someone with higher or equal role
    if member.top_role >= ctx.guild.me.top_role:
        return False, f"I cannot change {member.display_name}'s nickname because their role ({member.top_role.name}) is higher than or equal to my highest role ({ctx.guild.me.top_role.name}). Please move my role higher in the Server Settings."
        
    return True, None


def can_bot_edit_member_nick(guild: discord.Guild, member: discord.Member):
    """Checks if the bot can edit a member nickname in this guild."""
    if member.id == guild.owner_id:
        return False, "Cannot edit the server owner's nickname."
    bot_member = guild.me
    if not bot_member or member.top_role >= bot_member.top_role:
        return False, "Role hierarchy prevents nickname edit."
    return True, None


def _remove_afk_tag(name: str) -> str:
    cleaned = (name or "").strip()
    if cleaned.lower().endswith(AFK_TAG.lower()):
        cleaned = cleaned[: -len(AFK_TAG)].rstrip()
    return cleaned


def _build_afk_nick(member: discord.Member) -> str:
    base_name = _remove_afk_tag(member.display_name) or (member.global_name or member.name)
    max_name_length = max(1, 32 - len(AFK_TAG))
    base_name = base_name[:max_name_length]
    return f"{base_name}{AFK_TAG}"


async def _set_member_afk(member: discord.Member, reason: str):
    guild_afk = afk_statuses.setdefault(member.guild.id, {})
    if member.id in guild_afk:
        guild_afk[member.id]["reason"] = reason
        guild_afk[member.id]["set_at"] = datetime.datetime.utcnow()
        return

    guild_afk[member.id] = {
        "reason": reason,
        "set_at": datetime.datetime.utcnow(),
        "previous_nick": member.nick,
    }

    allowed, _ = can_bot_edit_member_nick(member.guild, member)
    if not allowed:
        return
    try:
        await member.edit(nick=_build_afk_nick(member), reason="User set AFK status")
    except (discord.Forbidden, discord.HTTPException):
        pass


async def _clear_member_afk(member: discord.Member):
    guild_afk = afk_statuses.get(member.guild.id, {})
    afk_data = guild_afk.pop(member.id, None)
    if afk_data is None:
        return False
    if not guild_afk:
        afk_statuses.pop(member.guild.id, None)

    allowed, _ = can_bot_edit_member_nick(member.guild, member)
    if not allowed:
        return True

    previous_nick = afk_data.get("previous_nick")
    try:
        await member.edit(nick=previous_nick, reason="User returned from AFK")
    except (discord.Forbidden, discord.HTTPException):
        pass
    return True


def _get_member_afk(member: discord.Member):
    return afk_statuses.get(member.guild.id, {}).get(member.id)

async def apply_nickname(member):
    """Helper function to apply the nickname suffix."""
    settings = load_settings(member.guild.id)
    suffix = settings.get("suffix_format", SUFFIX)
    
    try:
        current_name = member.display_name
        
        # Avoid double tagging if they already have the suffix
        if current_name.endswith(suffix):
            return

        # Truncate original name if necessary to fit the suffix within 32 chars (Discord limit)
        max_name_length = 32 - len(suffix)
        
        if len(current_name) > max_name_length:
            new_nick = current_name[:max_name_length] + suffix
        else:
            new_nick = current_name + suffix
            
        logger.info(f'Attempting to change nickname for {member.name} to {new_nick}')
        await member.edit(nick=new_nick)
        logger.info(f'Successfully changed nickname for {member.name} to {new_nick}')
        
    except discord.Forbidden:
        logger.warning(f"Failed to change nickname for {member.name}: Missing Permissions (Check role hierarchy)")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

async def remove_nickname(member):
    """Helper function to remove the nickname suffix."""
    settings = load_settings(member.guild.id)
    suffix = settings.get("suffix_format", SUFFIX)
    
    try:
        current_name = member.display_name
        
        # Only attempt removal if the suffix exists
        if current_name.endswith(suffix):
            new_nick = current_name[:-len(suffix)]
            
            # If the name becomes empty (edge case), don't change it or revert to name
            if not new_nick.strip():
                new_nick = member.global_name or member.name
            
            logger.info(f'Attempting to remove nickname suffix for {member.name} to {new_nick}')
            await member.edit(nick=new_nick)
            logger.info(f'Successfully removed nickname suffix for {member.name}')
            
    except discord.Forbidden:
        logger.warning(f"Failed to remove nickname for {member.name}: Missing Permissions (Check role hierarchy)")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def _strip_known_tags_from_end(name: str, tags: set[str]) -> str:
    """Strip configured tags repeatedly from the tail of a nickname."""
    cleaned = (name or "").rstrip()
    ordered_tags = sorted([t for t in tags if t], key=len, reverse=True)
    changed = True
    while changed:
        changed = False
        for tag in ordered_tags:
            if cleaned.endswith(tag):
                cleaned = cleaned[:-len(tag)].rstrip()
                changed = True
                break
    return cleaned


def _normalize_autonick_tag(tag: str | None) -> str:
    """Normalize tags to always include one leading space (e.g. ` [ADMIN]`)."""
    clean_tag = (tag or "").strip()
    if not clean_tag:
        return ""
    if clean_tag.startswith(" "):
        return clean_tag
    return f" {clean_tag}"


def _fit_nick_with_tag(base_name: str, tag: str) -> str:
    """Build a Discord-valid nick (<=32 chars) with tag appended."""
    normalized_tag = _normalize_autonick_tag(tag)
    max_name_length = 32 - len(normalized_tag)
    truncated = base_name[:max_name_length] if len(base_name) > max_name_length else base_name
    return f"{truncated}{normalized_tag}"


def _resolve_member_autonick_tag(member: discord.Member, role_rules: dict[int, str], default_tag: str | None):
    """Resolve tag by highest matching configured role, else default tag."""
    matched_roles = [role for role in member.roles if role.id in role_rules]
    if matched_roles:
        highest = max(matched_roles, key=lambda r: r.position)
        return role_rules.get(highest.id)
    return default_tag


def _parse_bulk_autonick_mappings(raw_mappings: str, max_pairs: int = 100):
    """
    Parse mappings formatted like:
    <@&ROLE_ID> [TAG] <@&ROLE_ID> [ANOTHER TAG]
    Returns (pairs, error_message). `pairs` is list[(role_id, normalized_tag)].
    """
    text = (raw_mappings or "").strip()
    if not text:
        return [], None

    mention_pattern = re.compile(r"<@&(\d+)>")
    matches = list(mention_pattern.finditer(text))
    if not matches:
        return [], "No role mentions found. Use format: `@Role [TAG] @Role [TAG]`."
    if len(matches) > max_pairs:
        return [], f"You can only configure up to {max_pairs} role-tag pairs at once."

    parsed_pairs: list[tuple[int, str]] = []
    for index, match in enumerate(matches):
        role_id = int(match.group(1))
        tag_start = match.end()
        tag_end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        raw_tag = text[tag_start:tag_end].strip()
        clean_tag = _normalize_autonick_tag(raw_tag)
        if not clean_tag:
            return [], f"Missing nickname tag after role mention #{index + 1}."
        parsed_pairs.append((role_id, clean_tag))

    return parsed_pairs, None


async def apply_autonick_for_member(member: discord.Member, extra_tags_to_strip: set[str] | None = None):
    """Apply configured autonick rules/default for one member."""
    role_rules = database.get_autonick_rules(member.guild.id)
    config = database.get_guild_config(member.guild.id) or {}
    default_tag = config.get("default_nick_tag")
    settings = load_settings(member.guild.id)
    legacy_suffix = _normalize_autonick_tag(settings.get("suffix_format"))

    known_tags = set()
    for existing_tag in role_rules.values():
        known_tags.add(existing_tag)
        normalized = _normalize_autonick_tag(existing_tag)
        if normalized:
            known_tags.add(normalized)
    if default_tag:
        known_tags.add(default_tag)
        normalized_default = _normalize_autonick_tag(default_tag)
        if normalized_default:
            known_tags.add(normalized_default)
    if legacy_suffix:
        known_tags.add(legacy_suffix)
    if extra_tags_to_strip:
        known_tags.update([tag for tag in extra_tags_to_strip if tag])
        known_tags.update([_normalize_autonick_tag(tag) for tag in extra_tags_to_strip if tag])

    if not known_tags:
        return False, "No autonick rules configured."

    if member.id == member.guild.owner_id:
        return False, "Cannot modify server owner nickname."

    bot_member = member.guild.me
    if not bot_member or member.top_role >= bot_member.top_role:
        return False, "Role hierarchy prevents nickname edit."

    account_name = (member.global_name or member.name).strip()
    current_effective_name = member.display_name
    base_name = _strip_known_tags_from_end(current_effective_name, known_tags).strip() or account_name

    resolved_tag = _resolve_member_autonick_tag(member, role_rules, default_tag)
    if resolved_tag and legacy_suffix and _normalize_autonick_tag(resolved_tag).lower() == legacy_suffix.lower():
        resolved_tag = None
    if resolved_tag:
        desired_nick = _fit_nick_with_tag(base_name, resolved_tag)
    else:
        desired_nick = None if base_name == account_name else base_name

    desired_effective_name = desired_nick if desired_nick is not None else account_name
    if current_effective_name == desired_effective_name:
        return False, "Nickname already in desired state."

    try:
        await member.edit(nick=desired_nick)
        return True, f"Updated to `{desired_effective_name}`."
    except discord.Forbidden:
        return False, "Missing permissions to edit nickname."
    except Exception as e:
        logger.error("Autonick update failed for %s: %s", member.id, e)
        return False, f"Error: {e}"


@bot.event
async def on_member_join(member):
    logger.info(f"Member joined: {member.name}")
    await _send_configured_log_embed(
        member.guild,
        "join_leave",
        title="📥 Member Joined",
        color=discord.Color.green(),
        fields=[
            ("Member", f"{member.mention} (`{member.id}`)", False),
            ("Account Created", discord.utils.format_dt(member.created_at, style="F"), False),
        ],
    )
    
    # If member is pending (Membership Screening), wait for on_member_update
    if member.pending:
        logger.info(f"Member {member.name} is pending verification. Skipping auto-nick.")
        return

    await apply_autonick_for_member(member)

@bot.event
async def on_member_update(before, after):
    settings = load_settings(after.guild.id)
    
    # Handle Membership Screening Completion (Pending -> Member)
    if before.pending and not after.pending:
        logger.info(f"Member {after.name} completed verification.")
        await apply_autonick_for_member(after)

    # Role change auto-nickname handling
    if before.roles != after.roles:
        await apply_autonick_for_member(after)

    # Enforce Suffix
    if settings.get("enforce_suffix", False):
        # Check if nickname changed and suffix was removed
        if before.display_name != after.display_name:
             suffix = settings.get("suffix_format", SUFFIX)
             if not after.display_name.endswith(suffix):
                 await apply_nickname(after)

    # Remove on Role Loss (placeholder logic)
    pass
    # Auto-nickname disabled by request
    # pass
    # Check if roles have changed
    # if len(before.roles) < len(after.roles):
    #     # A role was added
    #     new_roles = [role for role in after.roles if role not in before.roles]
    #     for role in new_roles:
    #         print(f"User {after.name} received role: {role.name}")
            
    #         if TRIGGER_ROLE_NAME:
    #             if role.name == TRIGGER_ROLE_NAME:
    #                 await apply_nickname(after)
    #         else:
    #             await apply_nickname(after)

    # elif len(before.roles) > len(after.roles):
    #     # A role was removed


@bot.event
async def on_member_remove(member):
    await _send_configured_log_embed(
        member.guild,
        "join_leave",
        title="📤 Member Left",
        color=discord.Color.orange(),
        fields=[
            ("Member", f"{member} (`{member.id}`)", False),
            ("Joined Server", discord.utils.format_dt(member.joined_at, style="F") if member.joined_at else "Unknown", False),
        ],
    )


@bot.event
async def on_voice_state_update(member, before, after):
    guild = member.guild
    if before.channel != after.channel:
        await _send_configured_log_embed(
            guild,
            "voice",
            title="🎙️ Voice Channel Update",
            color=discord.Color.blurple(),
            fields=[
                ("Member", f"{member.mention} (`{member.id}`)", False),
                ("From", before.channel.mention if before.channel else "None", True),
                ("To", after.channel.mention if after.channel else "None", True),
            ],
        )

    payload = load_blind_date_data(guild.id)
    lobby_id = payload.get("lobby_channel_id")

    # Queue Management: remove user from queue when leaving lobby before matching.
    if lobby_id and before.channel and before.channel.id == lobby_id:
        if not after.channel or after.channel.id != lobby_id:
            if _remove_user_from_blind_date_queue(payload, member.id):
                save_blind_date_data(guild.id, payload)

    # Auto-cleanup date rooms when empty.
    active_rooms = payload.get("active_room_ids", [])
    if not active_rooms:
        return

    channels_to_check = []
    if before.channel and before.channel.id in active_rooms:
        channels_to_check.append(before.channel)
    if after.channel and after.channel.id in active_rooms:
        channels_to_check.append(after.channel)

    deleted_room_ids = []
    for channel in channels_to_check:
        if isinstance(channel, discord.VoiceChannel) and len(channel.members) == 0:
            try:
                await channel.delete(reason="Blind Dating room auto-cleanup")
                deleted_room_ids.append(channel.id)
            except Exception as e:
                logger.warning("Failed to auto-delete blind date room %s in guild %s: %s", channel.id, guild.id, e)

    if deleted_room_ids:
        payload["active_room_ids"] = [rid for rid in active_rooms if rid not in deleted_room_ids]
        save_blind_date_data(guild.id, payload)
    #     removed_roles = [role for role in before.roles if role not in after.roles]
    #     for role in removed_roles:
    #         print(f"User {after.name} lost role: {role.name}")
            
    #         if TRIGGER_ROLE_NAME:
    #             if role.name == TRIGGER_ROLE_NAME:
    #                 await remove_nickname(after)
    #         else:
    #             if len(after.roles) <= 1:
    #                  await remove_nickname(after)


@bot.event
async def on_audit_log_entry_create(entry: discord.AuditLogEntry):
    guild = entry.guild
    if guild is None or entry.user is None:
        return
    if entry.action != discord.AuditLogAction.member_update:
        return

    after_timeout = getattr(entry.after, "timed_out_until", None)
    if after_timeout is None:
        return

    data = load_attendance_data(guild.id)
    if not data.get("staff_tracker_enabled"):
        return

    monitored_role_ids = set(data.get("staff_tracker_role_ids") or [])
    if not monitored_role_ids:
        return

    staff_member = guild.get_member(entry.user.id)
    if not staff_member:
        try:
            staff_member = await guild.fetch_member(entry.user.id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

    if staff_member.id == guild.owner_id:
        return

    exempt_role_ids = set(data.get("staff_tracker_exempt_role_ids") or [])
    staff_role_ids = {role.id for role in staff_member.roles}
    if exempt_role_ids and any(role_id in staff_role_ids for role_id in exempt_role_ids):
        return
    if not any(role_id in staff_role_ids for role_id in monitored_role_ids):
        return

    reason_category = _staff_tracker_reason_category(entry.reason)
    strikeable_timeout = reason_category in {"missing", "placeholder"}

    strike_count = database.get_staff_tracker_strikes(guild.id, staff_member.id)
    if strikeable_timeout:
        strike_count = database.increment_staff_tracker_strike(guild.id, staff_member.id)

    punishment_mode = (data.get("staff_tracker_punishment_mode") or "timeout").lower()
    punishment_text = "Logged only"
    punishment_error = None
    threshold_hit = strikeable_timeout and strike_count in {3, 5, 7, 8}

    if threshold_hit:
        try:
            if punishment_mode == "timeout":
                duration = _staff_tracker_timeout_for_strike(strike_count)
                target_until = datetime.datetime.now(datetime.timezone.utc) + duration if duration else None
                if target_until:
                    await staff_member.timeout(target_until, reason=f"Staff tracker auto-punishment (strike {strike_count})")
                    punishment_text = f"Timeout for {_format_duration(duration)}"
            elif punishment_mode == "kick":
                await staff_member.kick(reason=f"Staff tracker auto-punishment (strike {strike_count})")
                punishment_text = "Kicked"
            elif punishment_mode == "ban":
                await guild.ban(staff_member, reason=f"Staff tracker auto-punishment (strike {strike_count})", delete_message_days=0)
                punishment_text = "Banned"
        except (discord.Forbidden, discord.HTTPException) as exc:
            punishment_error = str(exc)
            punishment_text = f"Failed to apply punishment ({punishment_mode})"

    target_id = getattr(entry.target, "id", None)
    action_text = "timeout_without_reason" if strikeable_timeout else "timeout_with_reason"
    case_id = database.add_staff_tracker_case(
        guild_id=guild.id,
        staff_user_id=staff_member.id,
        target_user_id=target_id,
        action=action_text,
        reason_text=entry.reason or "",
        strike_count=strike_count,
        punishment_applied=punishment_text,
        audit_entry_id=entry.id,
    )

    if strikeable_timeout:
        dm_text = (
            "⚠️ **Staff Warning Notice**\n\n"
            "You received a strike for moderation abuse.\n"
            "🎯 Reason: Timeout without reason\n"
            f"📊 Total Strikes: {strike_count}\n\n"
            "⚠️ Next penalties:\n"
            f"{_staff_tracker_next_penalties_text()}"
        )
        try:
            await staff_member.send(dm_text)
        except (discord.Forbidden, discord.HTTPException):
            logger.info("Staff tracker DM failed for user %s in guild %s.", staff_member.id, guild.id)

    log_channel_id = data.get("staff_tracker_log_channel_id")
    log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
    if log_channel:
        reason_status = {
            "missing": "Missing reason",
            "placeholder": "Placeholder reason",
            "provided": "Reason provided",
        }.get(reason_category, "Unknown")
        details = [
            "🚨 **Staff Timeout Tracked**",
            f"👤 Staff: {staff_member.mention}",
            f"🎯 Action: {'Timeout without reason' if strikeable_timeout else 'Timeout with reason'}",
            f"📝 Reason status: {reason_status}",
            f"🗒️ Audit reason: {(entry.reason or 'None')[:300]}",
            f"📊 Strikes: {strike_count}",
            f"⛔ Punishment: {punishment_text}",
            f"🧾 Case ID: {case_id}",
        ]
        if punishment_error:
            details.append(f"⚠️ Punishment error: {punishment_error[:300]}")
        await log_channel.send("\n".join(details))

@bot.command(name='setnick')
async def set_nickname(ctx, member: discord.Member, *, new_name: str):
    """
    Sets a nickname and appends the suffix.
    Users can set their own nickname; staff can set for others.
    Usage: !setnick @Member NewName
    """
    # Only staff can set nicknames for other members.
    if member.id != ctx.author.id and not ctx.author.guild_permissions.manage_nicknames:
        await ctx.send("You can only use this command on yourself unless you have Manage Nicknames permission.")
        return

    # Check hierarchy first
    allowed, message = can_manage_nick(ctx, member)
    if not allowed:
        await ctx.send(f"Failed: {message}")
        return

    try:
        # Check if the suffix is already in the provided name, if not, append it
        if not new_name.endswith(SUFFIX):
             # Truncate if necessary
            max_name_length = 32 - len(SUFFIX)
            if len(new_name) > max_name_length:
                new_nick = new_name[:max_name_length] + SUFFIX
            else:
                new_nick = new_name + SUFFIX
        else:
            new_nick = new_name
            
        await member.edit(nick=new_nick)
        await ctx.send(f"Successfully changed nickname for {member.mention} to `{new_nick}`")
        
    except discord.Forbidden:
        await ctx.send("Failed: I don't have permission to change that user's nickname. (Unexpected Forbidden error)")
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")

@set_nickname.error
async def set_nickname_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: `!setnick @Member New Name`")

@bot.command(name='nick')
async def nick(ctx, *, name: str = None):
    """
    Sets your own nickname with the suffix.
    Usage: !nick NewName
    Usage: !nick remove (to remove the suffix)
    """
    logger.info(f"Command !nick triggered by {ctx.author}")
    member = ctx.author
    
    if name is None:
        await ctx.send("Usage: Type `!nick YourName` to change your nickname, or `!nick remove` to remove the suffix.")
        return

    if name.lower() == "remove":
        # Check hierarchy first
        allowed, message = can_manage_nick(ctx, member)
        if not allowed:
            await ctx.send(f"Failed: {message}")
            return
            
        await remove_nickname(member)
        await ctx.send(f"Nickname suffix removed for {member.mention}.")
    else:
        try:
            # Check hierarchy first
            allowed, message = can_manage_nick(ctx, member)
            if not allowed:
                await ctx.send(f"Failed: {message}")
                return

            # Check if the suffix is already in the provided name, if not, append it
            settings = load_settings(ctx.guild.id)
            suffix = settings.get("suffix_format", SUFFIX)
            if not name.endswith(suffix):
                 # Truncate if necessary
                max_name_length = 32 - len(suffix)
                if len(name) > max_name_length:
                    new_nick = name[:max_name_length] + suffix
                else:
                    new_nick = name + suffix
            else:
                new_nick = name
                
            logger.info(f"Changing nickname for {member} to {new_nick}")
            await member.edit(nick=new_nick)
            await ctx.send(f"Successfully changed nickname for {member.mention} to `{new_nick}`")
            
        except discord.Forbidden:
            logger.warning("Forbidden: Cannot change nickname.")
            await ctx.send("Failed: I don't have permission to change your nickname. Ensure my role is higher than yours in the server settings.")
        except Exception as e:
            logger.error(f"Error in !nick: {e}")
            await ctx.send(f"An error occurred: {e}")

@nick.error
async def nick_error(ctx, error):
    # MissingRequiredArgument is now handled inside the command function
    pass


async def reconcile_autonicks_for_guild(
    guild: discord.Guild,
    role: discord.Role | None = None,
    extra_tags_to_strip: set[str] | None = None
):
    """Re-apply autonick rules to all eligible members (or members in one role)."""
    members: list[discord.Member] = []
    try:
        fetched_members = [member async for member in guild.fetch_members(limit=None)]
        if role:
            role_id = role.id
            members = [member for member in fetched_members if any(r.id == role_id for r in member.roles)]
        else:
            members = fetched_members
    except Exception as e:
        logger.warning("Falling back to cached members during autonick reconcile for guild %s: %s", guild.id, e)
        members = role.members if role else guild.members

    updated = 0
    skipped = 0
    for member in members:
        changed, _ = await apply_autonick_for_member(member, extra_tags_to_strip=extra_tags_to_strip)
        if changed:
            updated += 1
        else:
            skipped += 1
    return updated, skipped


@bot.command(name='autonick')
@commands.has_permissions(manage_nicknames=True)
async def set_autonick_rule(ctx, role: discord.Role = None, *, tag: str = None):
    """Associates a nickname tag with a role."""
    if role is None or tag is None:
        await ctx.send("Usage: `!autonick @Role [Tag]`")
        return

    clean_tag = _normalize_autonick_tag(tag)
    if not clean_tag:
        await ctx.send("Tag cannot be empty.")
        return

    database.upsert_autonick_rule(ctx.guild.id, role.id, clean_tag)
    _settings_cache.pop(ctx.guild.id, None)
    updated, skipped = await reconcile_autonicks_for_guild(ctx.guild, role=role)
    await ctx.send(
        f"✅ Auto-nick rule saved: {role.mention} -> `{clean_tag}`. "
        f"Updated: **{updated}**, unchanged/skipped: **{skipped}**."
    )


@bot.command(name='setupautonickrole')
@commands.has_permissions(manage_nicknames=True)
async def setup_autonick_role_bulk(ctx, *, mappings: str = None):
    """
    Bulk setup for role-based autonick tags.
    Usage:
      !setupautonickrole @Admin [ADMIN] @Moderator [MODERATOR]
    """
    if not mappings:
        await ctx.send(
            "Usage: `!setupautonickrole @Role [TAG] @Role [TAG]` (up to 100 pairs).\n"
            "Example: `!setupautonickrole @Admin [ADMIN] @Moderator [MODERATOR]`"
        )
        return

    parsed_pairs, error = _parse_bulk_autonick_mappings(mappings, max_pairs=100)
    if error:
        await ctx.send(f"❌ {error}")
        return

    missing_roles = []
    applied = 0
    for role_id, tag in parsed_pairs:
        role = ctx.guild.get_role(role_id)
        if not role:
            missing_roles.append(role_id)
            continue
        database.upsert_autonick_rule(ctx.guild.id, role.id, tag)
        applied += 1

    _settings_cache.pop(ctx.guild.id, None)
    updated, skipped = await reconcile_autonicks_for_guild(ctx.guild) if applied else (0, 0)
    response_lines = [
        f"✅ Saved **{applied}** auto-nick role mapping(s).",
        "These mappings are persisted per server and will still apply after bot restarts/redeploys.",
        f"Reconciled members now. Updated: **{updated}**, unchanged/skipped: **{skipped}**.",
    ]
    if missing_roles:
        sample = ", ".join(f"`{role_id}`" for role_id in missing_roles[:10])
        extra = " ..." if len(missing_roles) > 10 else ""
        response_lines.append(f"⚠️ Missing roles skipped ({len(missing_roles)}): {sample}{extra}")
    await ctx.send("\n".join(response_lines))


@bot.command(name='defaultnick')
@commands.has_permissions(manage_nicknames=True)
async def set_default_nick_tag(ctx, *, tag: str = None):
    """Sets default autonick tag for users without any mapped role."""
    if tag is None:
        await ctx.send("Usage: `!defaultnick [Tag]`")
        return

    clean_tag = _normalize_autonick_tag(tag)
    database.update_guild_config(ctx.guild.id, default_nick_tag=clean_tag if clean_tag else None)
    _settings_cache.pop(ctx.guild.id, None)
    if clean_tag:
        await ctx.send(f"✅ Default nickname tag set to `{clean_tag}`")
    else:
        await ctx.send("✅ Default nickname tag cleared.")


@bot.command(name='removenick')
@commands.has_permissions(manage_nicknames=True)
async def remove_autonick_rule(ctx, role: discord.Role = None):
    """Removes autonick role mapping."""
    if role is None:
        await ctx.send("Usage: `!removenick @Role`")
        return

    role_rules = database.get_autonick_rules(ctx.guild.id)
    removed_tag = role_rules.get(role.id)
    removed = database.delete_autonick_rule(ctx.guild.id, role.id)
    if removed:
        _settings_cache.pop(ctx.guild.id, None)
        updated, skipped = await reconcile_autonicks_for_guild(
            ctx.guild,
            extra_tags_to_strip={removed_tag} if removed_tag else None
        )
        await ctx.send(
            f"✅ Removed autonick mapping for {role.mention}. "
            f"Updated: **{updated}**, unchanged/skipped: **{skipped}**."
        )
    else:
        await ctx.send(f"ℹ️ No autonick mapping exists for {role.mention}.")


@bot.command(name='updateall')
@commands.has_permissions(manage_nicknames=True)
async def update_all_autonicks(ctx, role: discord.Role = None):
    """Re-applies configured autonick rules for everyone or one role."""
    updated, skipped = await reconcile_autonicks_for_guild(ctx.guild, role)
    scope = f"members with {role.mention}" if role else "all members"
    await ctx.send(f"✅ Reconciled autonick for {scope}. Updated: **{updated}**, unchanged/skipped: **{skipped}**.")


@bot.command(name='removeall')
@commands.has_permissions(manage_nicknames=True)
async def remove_autonick_from_all(ctx, role: discord.Role = None):
    """Remove one role's configured tag from all current role members and delete rule."""
    if role is None:
        await ctx.send("Usage: `!removeall @Role`")
        return

    role_rules = database.get_autonick_rules(ctx.guild.id)
    tag = role_rules.get(role.id)
    if not tag:
        await ctx.send(f"ℹ️ No autonick tag configured for {role.mention}.")
        return

    changed = 0
    for member in role.members:
        if member.id == ctx.guild.owner_id or member.top_role >= ctx.guild.me.top_role:
            continue
        current_effective_name = member.nick if member.nick else member.name
        new_effective_name = current_effective_name.replace(tag, "").strip()
        if new_effective_name == current_effective_name:
            continue
        new_nick = None if new_effective_name == member.name else new_effective_name
        try:
            await member.edit(nick=new_nick)
            changed += 1
        except discord.Forbidden:
            continue

    database.delete_autonick_rule(ctx.guild.id, role.id)
    await ctx.send(f"✅ Removed `{tag}` from **{changed}** members in {role.mention} and deleted that rule.")


@bot.command(name='stripall')
@commands.has_permissions(manage_nicknames=True)
async def strip_all_from_role(ctx, role: discord.Role = None, *, text_to_remove: str = None):
    """Remove any provided text from nicknames of members in one role."""
    if role is None or text_to_remove is None:
        await ctx.send("Usage: `!stripall @Role [TagToRemove]`")
        return

    token = text_to_remove.strip()
    if not token:
        await ctx.send("TagToRemove cannot be empty.")
        return

    changed = 0
    for member in role.members:
        if member.id == ctx.guild.owner_id or member.top_role >= ctx.guild.me.top_role:
            continue
        current_effective_name = member.nick if member.nick else member.name
        new_effective_name = current_effective_name.replace(token, "").strip()
        if new_effective_name == current_effective_name:
            continue
        new_nick = None if new_effective_name == member.name else new_effective_name
        try:
            await member.edit(nick=new_nick)
            changed += 1
        except discord.Forbidden:
            continue

    await ctx.send(f"✅ Removed `{token}` from **{changed}** nicknames in {role.mention}.")


async def _reset_member_nicknames(guild: discord.Guild, members: list[discord.Member]):
    changed = 0
    skipped = 0
    failed = 0

    for member in members:
        if member.id == guild.owner_id or member.top_role >= guild.me.top_role:
            skipped += 1
            continue
        if member.nick is None:
            skipped += 1
            continue
        try:
            await member.edit(nick=None)
            changed += 1
        except discord.Forbidden:
            failed += 1
        except Exception as e:
            logger.warning("Failed to reset nickname for %s in guild %s: %s", member.id, guild.id, e)
            failed += 1

    return changed, skipped, failed


@bot.command(name='nickresetall', aliases=['nickreset'])
@commands.has_permissions(manage_nicknames=True)
async def nick_reset_all(ctx, role: discord.Role = None, *, scope: str = None):
    """
    Reset nicknames back to account names for everyone or for one role.
    Usage:
      !nickresetall
      !nickresetall @Role
      !nickreset @Role everyone
    """
    if scope and scope.strip().lower() not in {"everyone", "all"}:
        await ctx.send("Usage: `!nickresetall [@Role]` or `!nickreset @Role everyone`")
        return

    if role is None:
        try:
            members = [m async for m in ctx.guild.fetch_members(limit=None)]
        except Exception as e:
            logger.warning("Falling back to cached members during nick reset for guild %s: %s", ctx.guild.id, e)
            members = list(ctx.guild.members)
        scope_label = "all members"
    else:
        members = list(role.members)
        scope_label = f"members with {role.mention}"

    changed, skipped, failed = await _reset_member_nicknames(ctx.guild, members)

    await ctx.send(
        f"✅ Nickname reset complete for {scope_label}. "
        f"Reset: **{changed}**, skipped: **{skipped}**, failed: **{failed}**."
    )


@bot.tree.command(name="resetallnick", description="Reset nicknames to account names for everyone or one role.")
@app_commands.describe(role="Optional role to limit the reset.")
async def resetallnick_slash(interaction: discord.Interaction, role: discord.Role | None = None):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used inside a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_nicknames:
        await interaction.response.send_message("You need **Manage Nicknames** permission to use this command.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    if role is None:
        try:
            members = [m async for m in interaction.guild.fetch_members(limit=None)]
        except Exception as e:
            logger.warning("Falling back to cached members during slash nick reset for guild %s: %s", interaction.guild.id, e)
            members = list(interaction.guild.members)
        scope_label = "all members"
    else:
        members = list(role.members)
        scope_label = f"members with {role.mention}"

    changed, skipped, failed = await _reset_member_nicknames(interaction.guild, members)
    await interaction.followup.send(
        f"✅ Nickname reset complete for {scope_label}. "
        f"Reset: **{changed}**, skipped: **{skipped}**, failed: **{failed}**.",
        ephemeral=True
    )


@bot.command(name='nicksettings')
async def show_nick_settings(ctx):
    """Show default nickname tag and role mappings for this server."""
    config = database.get_guild_config(ctx.guild.id) or {}
    default_tag = config.get("default_nick_tag")
    role_rules = database.get_autonick_rules(ctx.guild.id)

    lines = []
    if default_tag:
        lines.append(f"**Default tag:** `{default_tag}`")
    else:
        lines.append("**Default tag:** *(not set)*")

    if role_rules:
        lines.append("**Role mappings:**")
        for role_id, tag in role_rules.items():
            role = ctx.guild.get_role(role_id)
            role_label = role.mention if role else f"`{role_id}` (deleted role)"
            lines.append(f"- {role_label} -> `{tag}`")
    else:
        lines.append("**Role mappings:** *(none configured)*")

    await ctx.send("\n".join(lines))

# --- Helper Functions ---

def parse_time_input(time_str):
    """Parses various time string formats into HH:MM (24h)."""
    time_str = time_str.lower().replace(" ", "").replace(".", "")
    
    # Formats to try
    # %H:%M (14:30), %I:%M%p (2:30pm), %I%p (2pm), %H (14)
    formats = [
        "%H:%M", 
        "%I:%M%p", 
        "%I%p", 
        "%H"
    ]
    
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(time_str, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            continue
            
    return None


def normalize_custom_command_name(command_name, prefix: str = DEFAULT_PREFIX):
    """Normalizes a custom command name so it can be stored and matched safely."""
    if not command_name:
        return None

    normalized = command_name.strip().lower()
    if prefix and normalized.startswith(prefix.lower()):
        normalized = normalized[len(prefix):]

    if not normalized or ' ' in normalized:
        return None

    return normalized


def extract_prefixed_command_name(message_content, prefix: str = DEFAULT_PREFIX):
    """Return a normalized prefixed command name, or None for empty/invalid prefixed input."""
    if not message_content or not prefix:
        return None

    stripped = message_content.strip()
    if not stripped.lower().startswith(prefix.lower()):
        return None

    parts = stripped.split(maxsplit=1)
    if not parts:
        return None

    return normalize_custom_command_name(parts[0], prefix)


def parse_direct_message_request(message_content: str) -> tuple[int, str] | None:
    """
    Parse messages in this format:
    /directmessage @user message: your message here
    """
    if not message_content:
        return None

    raw = message_content.strip()
    match = re.match(
        r"^/directmessage\s+<@!?(?P<user_id>\d+)>\s+message\s*:\s*(?P<message>.+)$",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None

    user_id = int(match.group("user_id"))
    text = match.group("message").strip()
    if not text:
        return None

    return user_id, text

@bot.command(name='ping')
async def ping(ctx):
    """Checks if the bot is alive."""
    await ctx.send(f"Pong! 🏓 Latency: {round(bot.latency * 1000)}ms")


@bot.hybrid_command(
    name='directmessage',
    with_app_command=True,
    description="Send a DM to a user through the bot."
)
@app_commands.default_permissions(manage_messages=True)
@commands.has_permissions(manage_messages=True)
async def directmessage(ctx, user: Union[discord.Member, discord.User], *, message: str):
    """Send a direct message to a user."""
    dm_text = (message or "").strip()
    if not dm_text:
        await ctx.send("Usage: `!directmessage @user <message>`")
        return

    try:
        await user.send(dm_text)
        await ctx.send(f"✅ Sent a DM to **{user}**.")
    except discord.Forbidden:
        await ctx.send("⚠️ I couldn't send a DM to that user (DMs may be closed).")
    except discord.HTTPException:
        await ctx.send("❌ Failed to send the DM due to a Discord API error.")


@bot.command(name='afk')
async def afk(ctx, *, message: str = None):
    """Set your AFK status and append [AFK] to your nickname when possible."""
    if not ctx.guild:
        await ctx.send("❌ This command can only be used inside a server.")
        return

    reason = (message or "AFK").strip() or "AFK"
    if len(reason) > 180:
        reason = reason[:180]

    await _set_member_afk(ctx.author, reason)
    await ctx.send(f"✅ {ctx.author.mention} is now AFK: **{reason}**")


@bot.hybrid_command(name='setprefix', with_app_command=True, description="Set this server's bot prefix (e.g. !, ., ?, #).")
@app_commands.default_permissions(administrator=True)
@commands.has_permissions(administrator=True)
async def setprefix(ctx, prefix: str):
    if not ctx.guild:
        await ctx.send("❌ This command can only be used inside a server.")
        return

    clean_prefix = _sanitize_prefix(prefix)
    if clean_prefix != prefix.strip():
        await ctx.send("❌ Invalid prefix. Use 1-8 non-space characters (examples: `!`, `.`, `?`, `#`).")
        return

    database.update_guild_config(ctx.guild.id, bot_prefix=clean_prefix)
    await ctx.send(f"✅ Prefix updated. Use `{clean_prefix}help` for commands.")


async def _set_lock_state_for_all_channels(
    interaction: discord.Interaction,
    *,
    locked: bool,
    target_role: Optional[discord.Role] = None,
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used inside a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    role_to_update = target_role or guild.default_role
    updated = 0
    failed = []

    for channel in guild.channels:
        overwrite = channel.overwrites_for(role_to_update)
        changed = False

        if isinstance(channel, (discord.TextChannel, discord.ForumChannel, discord.CategoryChannel)):
            desired_send = False if locked else None
            desired_threads = False if locked else None
            desired_public_threads = False if locked else None
            desired_private_threads = False if locked else None

            if overwrite.send_messages != desired_send:
                overwrite.send_messages = desired_send
                changed = True
            if hasattr(overwrite, 'send_messages_in_threads') and overwrite.send_messages_in_threads != desired_threads:
                overwrite.send_messages_in_threads = desired_threads
                changed = True
            if hasattr(overwrite, 'create_public_threads') and overwrite.create_public_threads != desired_public_threads:
                overwrite.create_public_threads = desired_public_threads
                changed = True
            if hasattr(overwrite, 'create_private_threads') and overwrite.create_private_threads != desired_private_threads:
                overwrite.create_private_threads = desired_private_threads
                changed = True

        if isinstance(channel, (discord.VoiceChannel, discord.StageChannel, discord.CategoryChannel)):
            desired_connect = False if locked else None
            if overwrite.connect != desired_connect:
                overwrite.connect = desired_connect
                changed = True

        if not changed:
            continue

        try:
            await channel.set_permissions(role_to_update, overwrite=overwrite, reason=(
                f"Requested by {interaction.user} via /{'lockallchannels' if locked else 'unlockallchannels'}"
            ))
            updated += 1
        except (discord.Forbidden, discord.HTTPException):
            failed.append(channel.name)

    action = "Locked" if locked else "Unlocked"
    summary = f"✅ {action} channels: `{updated}` updated."
    if failed:
        preview = ", ".join(failed[:10])
        suffix = "..." if len(failed) > 10 else ""
        summary += f"\n⚠️ Failed: `{len(failed)}` ({preview}{suffix})"

    data = load_attendance_data(guild.id)
    announce_channel = None
    report_channel_id = data.get('report_channel_id')
    if report_channel_id:
        announce_channel = guild.get_channel(report_channel_id)

    if announce_channel:
        mention_target = role_to_update.mention
        if locked:
            alert_message = (
                f"{mention_target} the server channels and voice channels are now lock please wait for the owner to unlock"
            )
        else:
            alert_message = f"{mention_target} all channels now are unlocked you may now chat and join calls"

        try:
            await announce_channel.send(
                alert_message,
                allowed_mentions=discord.AllowedMentions(everyone=True, roles=True)
            )
            summary += f"\n📢 Announcement sent to {announce_channel.mention}."
        except (discord.Forbidden, discord.HTTPException):
            summary += "\n⚠️ I couldn't send the lock/unlock announcement to the assigned report channel."
    else:
        summary += "\n⚠️ No lock/unlock announcement channel found. Set one with `/lockunlockchannels #channel`."

    await interaction.followup.send(summary, ephemeral=True)


@bot.tree.command(name='lockallchannels', description='Lock all text and voice channels server-wide.')
@app_commands.describe(role='Optional role to lock (defaults to @everyone)')
@app_commands.default_permissions(administrator=True)
async def lockallchannels_slash(interaction: discord.Interaction, role: Optional[discord.Role] = None):
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You need Administrator permission to use this command.", ephemeral=True)
        return
    if interaction.guild is None:
        await interaction.response.send_message("❌ This command can only be used inside a server.", ephemeral=True)
        return
    await _set_lock_state_for_all_channels(interaction, locked=True, target_role=role)


@bot.tree.command(name='unlockallchannels', description='Unlock all text and voice channels server-wide.')
@app_commands.describe(role='Optional role to unlock (defaults to @everyone)')
@app_commands.default_permissions(administrator=True)
async def unlockallchannels_slash(interaction: discord.Interaction, role: Optional[discord.Role] = None):
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You need Administrator permission to use this command.", ephemeral=True)
        return
    if interaction.guild is None:
        await interaction.response.send_message("❌ This command can only be used inside a server.", ephemeral=True)
        return
    await _set_lock_state_for_all_channels(interaction, locked=False, target_role=role)


@bot.tree.command(name='lockunlockchannels', description='Set or clear the channel used for lock/unlock announcements.')
@app_commands.describe(channel='Channel used for lock/unlock announcements (leave empty to clear)')
@app_commands.default_permissions(administrator=True)
async def lockunlockchannels_slash(interaction: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    if interaction.guild is None:
        await interaction.response.send_message("❌ This command can only be used inside a server.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You need Administrator permission to use this command.", ephemeral=True)
        return

    data = load_attendance_data(interaction.guild.id)
    if channel is None:
        data['report_channel_id'] = None
        save_attendance_data(interaction.guild.id, data)
        await interaction.response.send_message(
            "✅ Lock/unlock announcements have been disabled. I won't send notices after `/lockallchannels` or `/unlockallchannels`.",
            ephemeral=True
        )
        return

    data['report_channel_id'] = channel.id
    save_attendance_data(interaction.guild.id, data)
    await interaction.response.send_message(
        f"✅ Lock/unlock announcements will be sent to {channel.mention} after `/lockallchannels` and `/unlockallchannels`.",
        ephemeral=True
    )

@bot.command(name='av', aliases=['avatar', 'profile'])
async def avatar_profile(ctx, member: discord.Member = None):
    """
    Shows a member profile card with avatar.
    Usage: !av [@member]
    """
    target_member = member or ctx.author

    embed = discord.Embed(
        title=f"{target_member.display_name}'s Profile",
        color=target_member.color if target_member.color != discord.Color.default() else discord.Color.blurple(),
        timestamp=discord.utils.utcnow()
    )
    embed.set_thumbnail(url=target_member.display_avatar.url)
    embed.add_field(name="User", value=f"{target_member} (`{target_member.id}`)", inline=False)
    embed.add_field(
        name="Joined Server",
        value=discord.utils.format_dt(target_member.joined_at, style='F') if target_member.joined_at else "Unknown",
        inline=False
    )
    embed.add_field(
        name="Account Created",
        value=discord.utils.format_dt(target_member.created_at, style='F'),
        inline=False
    )
    embed.add_field(name="Avatar Link", value=f"[Open Avatar]({target_member.display_avatar.url})", inline=False)
    embed.set_image(url=target_member.display_avatar.url)
    embed.set_footer(text=f"Requested by {ctx.author}", icon_url=ctx.author.display_avatar.url)

    await ctx.send(embed=embed)


async def handle_plain_avatar_request(message, ctx):
    """Support plain-text avatar lookup like `av` or `av @member` without the ! prefix."""
    if not message.guild:
        return False

    raw_content = message.content.strip()
    if not raw_content:
        return False

    parts = raw_content.split(maxsplit=1)
    if parts[0].lower() != "av":
        return False

    target_member = None
    if len(parts) > 1 and parts[1].strip():
        member_query = parts[1].strip()
        try:
            target_member = await commands.MemberConverter().convert(ctx, member_query)
        except commands.BadArgument:
            await message.channel.send("❌ I couldn't find that user. Try `av @member`.")
            return True

    av_command = bot.get_command("av")
    if av_command is None:
        return False

    await ctx.invoke(av_command, member=target_member)
    return True


def compute_reto_percentage(guild_id: int, user_id_one: int, user_id_two: int) -> int:
    """Return a deterministic compatibility score for a pair of users."""
    first_id, second_id = sorted((int(user_id_one), int(user_id_two)))
    seed = f"{guild_id}:{first_id}:{second_id}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 101


def build_reto_response_lines(member_one: discord.Member, member_two: discord.Member, compatibility: int) -> list[str]:
    """Build an enhanced response block for reto/ship outputs."""
    clamped_score = max(0, min(100, int(compatibility)))
    meter_slots = 10
    filled_slots = round((clamped_score / 100) * meter_slots)
    compatibility_meter = "🟩" * filled_slots + "⬛" * (meter_slots - filled_slots)

    if clamped_score >= 90:
        tier_label = "Soulmate Tier"
        verdict = "🔥 Perfect vibe! This duo is unstoppable."
    elif clamped_score >= 70:
        tier_label = "High Chemistry"
        verdict = "✨ Great chemistry detected."
    elif clamped_score >= 40:
        tier_label = "Promising Match"
        verdict = "😌 Solid match. Keep getting to know each other."
    else:
        tier_label = "Slow Burn"
        verdict = "🤝 Might need more time, but every story can grow."

    ship_name = f"{member_one.display_name[:5]}{member_two.display_name[-5:]}".strip()

    return [
        f"💘 Reto Match: {member_one.mention} + {member_two.mention}",
        f"🧪 Compatibility: {clamped_score}%",
        f"📊 Match Meter: {compatibility_meter}",
        f"🏷️ Tier: {tier_label}",
        f"🪄 Ship Name: {ship_name}",
        verdict,
    ]


def build_reto_embed(member_one: discord.Member, member_two: discord.Member, compatibility: int) -> discord.Embed:
    """Build a richer reto/ship embed with both users highlighted."""
    response_lines = build_reto_response_lines(member_one, member_two, compatibility)
    clamped_score = max(0, min(100, int(compatibility)))
    pair_label = quote(f"{member_one.display_name} & {member_two.display_name}")
    combined_avatar = f"https://ui-avatars.com/api/?name={pair_label}&background=1f8b4c&color=ffffff&size=256"
    server_logo = member_one.guild.icon.url if member_one.guild and member_one.guild.icon else None
    if clamped_score >= 90:
        embed_color = discord.Color.from_rgb(225, 69, 122)
        score_emoji = "💖"
    elif clamped_score >= 70:
        embed_color = discord.Color.from_rgb(156, 89, 255)
        score_emoji = "✨"
    elif clamped_score >= 40:
        embed_color = discord.Color.from_rgb(57, 197, 187)
        score_emoji = "🌟"
    else:
        embed_color = discord.Color.from_rgb(120, 130, 145)
        score_emoji = "🤝"

    quote_block = "\n".join(f"> {line}" for line in response_lines)
    embed = discord.Embed(
        title=f"{score_emoji} Reto Match Report",
        description=quote_block,
        color=embed_color,
        timestamp=discord.utils.utcnow()
    )
    embed.add_field(name="Pair", value=f"{member_one.mention}  ×  {member_two.mention}", inline=False)
    embed.add_field(name=f"{member_one.display_name} Profile", value=f"[Open Avatar]({member_one.display_avatar.url})", inline=True)
    embed.add_field(name=f"{member_two.display_name} Profile", value=f"[Open Avatar]({member_two.display_avatar.url})", inline=True)
    embed.add_field(name="Luck Reading", value=f"{score_emoji} Destiny confidence is **{clamped_score}%**.", inline=False)
    embed.set_author(name="Registrarbot 💘")
    embed.set_thumbnail(url=server_logo or combined_avatar)
    embed.set_footer(text=f"Compatibility Score: {clamped_score}% • Deterministic per pair")
    return embed


async def maybe_send_reto_star_match(
    guild: discord.Guild,
    source_channel: discord.abc.Messageable,
    embed: discord.Embed,
    compatibility: int,
):
    """Send extreme reto matches (0% or 100%) to the configured star channel."""
    if compatibility not in {0, 100}:
        return

    data = load_attendance_data(guild.id)
    star_channel_id = data.get("reto_star_channel_id")
    if not star_channel_id:
        return

    star_channel = guild.get_channel(star_channel_id)
    if not isinstance(star_channel, discord.TextChannel):
        return

    if hasattr(source_channel, "id") and source_channel.id == star_channel.id:
        return

    try:
        header = "💯 **Perfect Reto Match Found!**" if compatibility == 100 else "🧊 **Zero-Percent Reto Match Found!**"
        await star_channel.send(header, embed=embed)
    except discord.Forbidden:
        logger.warning("Missing permissions to send reto star match in guild %s channel %s.", guild.id, star_channel_id)
    except Exception as e:
        logger.warning("Failed to send reto star match in guild %s: %s", guild.id, e)


async def handle_plain_reto_request(message):
    """Support plain-text ship command like `reto @user @user` (no ! prefix)."""
    if not message.guild:
        return False

    raw_content = message.content.strip()
    if not raw_content:
        return False

    lowered = raw_content.lower()
    if not (lowered == "reto" or lowered.startswith("reto ")):
        return False

    mentions = list(message.mentions)
    if len(mentions) >= 2:
        member_one, member_two = mentions[0], mentions[1]
    elif len(mentions) == 1:
        member_one, member_two = message.author, mentions[0]
    else:
        await message.channel.send("Usage: `reto @user` or `reto @user @user`")
        return True

    compatibility = compute_reto_percentage(message.guild.id, member_one.id, member_two.id)
    embed = build_reto_embed(member_one, member_two, compatibility)
    await message.channel.send(embed=embed)
    await maybe_send_reto_star_match(
        guild=message.guild,
        source_channel=message.channel,
        embed=embed,
        compatibility=compatibility,
    )
    return True


async def handle_plain_gay_radar_request(message):
    """Support plain-text fun command like `gay radar @user` (no ! prefix)."""
    if not message.guild:
        return False

    raw_content = message.content.strip()
    if not raw_content:
        return False

    lowered = raw_content.lower()
    if not (
        lowered == "gay radar"
        or lowered.startswith("gay radar ")
        or lowered == "gayradar"
        or lowered.startswith("gayradar ")
    ):
        return False

    target = message.mentions[0] if message.mentions else message.author
    seed = f"gayradar:{message.guild.id}:{target.id}"
    score = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16) % 101

    if score >= 85:
        vibe_line = "🌈 Radar says MAX rainbow energy detected."
    elif score >= 60:
        vibe_line = "✨ Radar says colorful vibes are strong today."
    elif score >= 35:
        vibe_line = "🪩 Radar says medium rainbow signal."
    else:
        vibe_line = "🛰️ Radar says low rainbow signal right now."

    embed = discord.Embed(
        title="🌈 Gay Radar (For Fun)",
        description=(
            f"{target.mention} scored **{score}%** on the Rainbow Vibes Meter.\n"
            f"{vibe_line}"
        ),
        color=discord.Color.magenta(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(
        name="Important",
        value="This is a joke command and cannot determine sexual orientation.",
        inline=False,
    )
    embed.set_footer(text=f"Requested by {message.author}", icon_url=message.author.display_avatar.url)
    await message.channel.send(embed=embed)
    return True


async def handle_plain_lesbiancheck_request(message):
    """Support plain-text safety command like `lesbiancheck @user` (no ! prefix)."""
    if not message.guild:
        return False

    raw_content = message.content.strip()
    if not raw_content:
        return False

    lowered = raw_content.lower()
    if not (
        lowered == "lesbiancheck"
        or lowered.startswith("lesbiancheck ")
        or lowered == "lesbian check"
        or lowered.startswith("lesbian check ")
    ):
        return False

    target = message.mentions[0] if message.mentions else message.author
    embed = discord.Embed(
        title="🛡️ LesbianCheck",
        description=(
            f"{target.mention} I will not guess whether someone is lesbian or not.\n"
            "Sexual orientation is personal and cannot be determined by this bot."
        ),
        color=discord.Color.blurple(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_footer(text=f"Requested by {message.author}", icon_url=message.author.display_avatar.url)
    await message.channel.send(embed=embed)
    return True


async def handle_plain_cute_check_request(message):
    """Support plain-text fun command like `cute check @user` (no ! prefix)."""
    if not message.guild:
        return False

    raw_content = message.content.strip()
    if not raw_content:
        return False

    lowered = raw_content.lower()
    if not (
        lowered == "cutecheck"
        or lowered.startswith("cutecheck ")
        or lowered == "cute check"
        or lowered.startswith("cute check ")
    ):
        return False

    target = message.mentions[0] if message.mentions else message.author
    score = random.randint(1, 100)
    verdict = "✅ Cute confirmed!" if score >= 50 else "❌ Not cute today (try again for better luck)."
    compliments = [
        "Certified adorable energy 💖",
        "Main character smile detected ✨",
        "Cute levels are legally dangerous 😳",
        "Serving soft vibes and charm 🌸",
    ]

    embed = discord.Embed(
        title="💘 Cute Check",
        description=f"{target.mention} got **{score}/100** on the cute scale.",
        color=discord.Color.pink(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="Result", value=verdict, inline=False)
    if score >= 50:
        embed.add_field(name="Vibe", value=random.choice(compliments), inline=False)
    embed.set_footer(text=f"Requested by {message.author}", icon_url=message.author.display_avatar.url)
    embed.set_thumbnail(url=target.display_avatar.url)
    await message.channel.send(embed=embed)
    return True


async def auto_translate_to_english(message: discord.Message, ctx: commands.Context) -> bool:
    """
    Detect non-English messages and post an automatic English translation.
    Returns True when a translation is posted.
    """
    if not message.guild or ctx.command is not None:
        return False

    data = load_attendance_data(message.guild.id)
    if not bool(data.get("translation_enabled", False)):
        return False

    content = (message.content or "").strip()
    if not content or content.startswith("!"):
        return False

    if len(content) < 4 or content.startswith(("http://", "https://")):
        return False

    try:
        detected_language = detect(content)
    except LangDetectException:
        return False
    except Exception as e:
        logger.debug("Language detection failed for message %s: %s", message.id, e)
        return False

    if not detected_language or detected_language.lower() == "en":
        return False

    try:
        translated_text = GoogleTranslator(source="auto", target="en").translate(content)
    except Exception as e:
        logger.debug("Translation failed for message %s: %s", message.id, e)
        return False

    if not translated_text or translated_text.strip().lower() == content.lower():
        return False

    dual_embed = discord.Embed(
        title="🌐 Auto Translation",
        color=discord.Color.teal(),
        timestamp=discord.utils.utcnow()
    )
    dual_embed.add_field(name="Original", value=content[:1024], inline=False)
    dual_embed.add_field(name="English", value=translated_text[:1024], inline=False)
    dual_embed.set_footer(text=f"Detected language: {detected_language.upper()}")

    english_channel_id = data.get("translation_channel_id")
    dual_channel_id = data.get("translation_dual_channel_id")
    english_channel = None
    dual_channel = None

    if english_channel_id:
        configured_english_channel = message.guild.get_channel(english_channel_id)
        if isinstance(configured_english_channel, discord.TextChannel):
            english_channel = configured_english_channel

    if dual_channel_id:
        configured_dual_channel = message.guild.get_channel(dual_channel_id)
        if isinstance(configured_dual_channel, discord.TextChannel):
            dual_channel = configured_dual_channel

    sent = False
    if isinstance(english_channel, discord.TextChannel):
        english_embed = discord.Embed(
            title="🇬🇧 English Translation",
            description=translated_text[:4000],
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        if english_channel.id != message.channel.id:
            english_embed.add_field(name="Source", value=message.channel.mention, inline=False)
        english_embed.set_footer(text=f"From {message.author.display_name} • {detected_language.upper()} → EN")
        await english_channel.send(embed=english_embed)
        sent = True

    if isinstance(dual_channel, discord.TextChannel):
        if dual_channel.id != message.channel.id:
            dual_embed.add_field(name="Source", value=message.channel.mention, inline=False)
        await dual_channel.send(
            f"{message.author.mention} translated automatically:",
            embed=dual_embed,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False)
        )
        sent = True

    if not english_channel_id and not dual_channel_id and isinstance(message.channel, discord.TextChannel):
        await message.channel.send(
            f"{message.author.mention} translated automatically:",
            embed=dual_embed,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False)
        )
        sent = True

    if not sent:
        return False
    return True

GAME_COMMANDS = {
    "uno": {"label": "UNO", "intro": "Gather your friends and race to discard all your cards. Don't forget to call **UNO!**"},
    "owo": {"label": "OWO", "intro": "OWO mini-games are ready! Use your favorite OWO game commands and start grinding."},
    "mafia": {"label": "Mafia", "intro": "Pick your roles, assign a host, and let the social deduction chaos begin."},
    "gartic": {"label": "Gartic", "intro": "Time to draw and guess! Create a room and invite everyone to play."},
    "casino": {"label": "Casino", "intro": "Try your luck in casino games—but play responsibly."},
    "mudae": {"label": "Mudae", "intro": "Roll your favorite characters and collect your wishlist picks."},
    "asterie": {"label": "Asterie", "intro": "Asterie game mode activated. Ready for your next adventure?"},
    "hangman": {"label": "Hangman", "intro": "Guess the hidden word one letter at a time before the drawing is complete."},
    "truthordare": {"label": "Truth or Dare", "intro": "Pick your challenge—be honest or be brave!"},
    "virtualfisher": {"label": "Virtual Fisher", "intro": "Cast your line and see what you can catch today."},
    "guessthenumber": {"label": "Guess the Number", "intro": "A secret number is waiting. Take your best shot!"},
}


def register_game_command(command_name: str, game_label: str, intro_text: str, aliases: list[str] | None = None):
    """Register a game command with persisted progress support."""
    command_aliases = aliases or []

    @bot.command(name=command_name, aliases=command_aliases)
    async def _game_command(ctx, action: str = None, *, progress_text: str = None):
        saved_progress = database.get_game_progress(ctx.guild.id, ctx.author.id, command_name)

        if action is None:
            progress_message = saved_progress if saved_progress else "*No saved progress yet.*"
            await ctx.send(
                f"🎮 **{game_label}**: {intro_text}\n"
                f"📝 **Your saved progress:** {progress_message}\n\n"
                f"Use `!{command_name} set <progress>` to save and "
                f"`!{command_name} clear` to reset."
            )
            return

        normalized_action = action.lower()
        if normalized_action == "set":
            if not progress_text:
                await ctx.send(f"Usage: `!{command_name} set <progress>`")
                return

            database.upsert_game_progress(ctx.guild.id, ctx.author.id, command_name, progress_text.strip())
            await ctx.send(f"💾 Saved your **{game_label}** progress: `{progress_text.strip()}`")
            return

        if normalized_action == "clear":
            if database.clear_game_progress(ctx.guild.id, ctx.author.id, command_name):
                await ctx.send(f"🗑️ Cleared your saved **{game_label}** progress.")
            else:
                await ctx.send(f"ℹ️ You have no saved **{game_label}** progress yet.")
            return

        await ctx.send(
            f"Unknown action `{action}`.\n"
            f"Use `!{command_name}` to view, `!{command_name} set <progress>` to save, "
            f"or `!{command_name} clear` to reset."
        )

    _game_command.__name__ = f"game_{command_name}"


for game_name, game_meta in GAME_COMMANDS.items():
    if game_name == "truthordare":
        register_game_command(game_name, game_meta["label"], game_meta["intro"], aliases=["truthdare", "tod"])
    elif game_name == "virtualfisher":
        register_game_command(game_name, game_meta["label"], game_meta["intro"], aliases=["vfisher"])
    elif game_name == "guessthenumber":
        register_game_command(game_name, game_meta["label"], game_meta["intro"], aliases=["guessnumber", "gtn"])
    else:
        register_game_command(game_name, game_meta["label"], game_meta["intro"])


def build_help_embeds() -> list[discord.Embed]:
    command_sections = {
        "General": [
            "`/help` - Show this command list.",
            "`!ping` - Check if the bot is online.",
            "`!av [@member]` - Show a user's profile card and avatar.",
            "`av [@member]` - Same avatar command without `!`.",
            "`!reto @user @user` or `!ship @user @user` - Check ship compatibility.",
            "`reto @user @user` - Same ship command without `!`.",
            "`!lesbiancheck [@user]` or `lesbiancheck [@user]` - Refuse to guess sexual orientation.",
            "`/retostarchannel #channel` - Send 0%/100% reto matches to a specific channel (admin)."
        ],
        "Nickname": [
            "`!nick <name|remove>` - Set your nickname with suffix or remove it.",
            "`!setnick @member <name>` - Set your nickname (or set others if you have Manage Nicknames).",
            "`/setnick new_name [member]` - Slash version of nickname setting.",
            "`!autonick @Role [Tag]` - Assign auto-tag for a role.",
            "`!defaultnick [Tag]` - Set default tag when no mapped roles match.",
            "`!removenick @Role` - Remove a role-tag mapping.",
            "`!updateall [@Role]` - Reconcile nicknames for all members or one role.",
            "`!removeall @Role` - Remove that role's tag from members and delete mapping.",
            "`!stripall @Role [TagToRemove]` - Strip specific text from nicknames by role.",
            "`!nickresetall [@Role]` - Reset nicknames to account names for everyone or one role.",
            "`/resetallnick [role]` - Slash version to reset nicknames for everyone or one role.",
            "`!nicksettings` - Show current default tag and role mappings."
        ],
        "Custom Commands": [
            "`!addcommand <command> <response>` - Create/update a custom command (admin).",
            "`!removecommand <command>` - Remove a custom command (admin).",
            "`!listcommands` - Show all custom commands."
        ],
        "Attendance Setup": [
            "`!settime <start> to <end>` - Set attendance window (admin).",
            "`!presentrole @role` - Set present role (admin).",
            "`!absentrole @role` - Set absent role (admin).",
            "`!excuserole @role` - Set excused role (admin).",
            "`!setpermitrole @role` - Set who can mark attendance (admin).",
            "`!channelpresent #channel` - Restrict attendance messages to a channel (admin).",
            "`!assignchannel #channel` - Set attendance report channel (admin).",
            "`/lockunlockchannels [#channel]` - Set or clear lock/unlock announcement channel (admin).",
            "`!setup_attendance` - Post attendance button UI (admin)."
        ],
        "Attendance Actions": [
            "`!present [@member]` - Mark present (self or member if allowed).",
            "`!absent @member [reason]` - Mark absent.",
            "`!excuse @member [reason]` - Mark excused.",
            "`!removepresent @member` - Remove present role.",
            "`!attendance` - Show current attendance report.",
            "`!attendance_leaderboard` - Show attendance leaderboard.",
            "`!restartattendance` - Reset attendance session."
        ],
        "Utility / Other": [
            "`!settings` - Show bot configuration panel.",
            "`!join` - Join your voice channel and start 24/7 lofi.",
            "`!play <song>` - Join voice (if needed) and start playback, or add to queue.",
            "`!queue` - Show current now-playing + queued tracks.",
            "`!skip` - Skip current track and move to the next queued item.",
            "`!pause` / `!resume` - Pause or resume active playback.",
            "`!volume <0-200>` - Set playback volume percentage.",
            "`!stop` - Stop playback and clear the queue.",
            "`!playlofi [url]` - Switch to default/custom lofi stream URL.",
            "`!status [text]` - Show 24/7 lofi + bot status, or set bot status text.",
            "`!leave` - Disconnect from voice and disable lofi mode.",
            "`!say <message>` - Make the bot send your message.",
            "`/directmessage @user <message>` - Send a direct message to a member through the bot (staff).",
            "`/directmessage @user message: ...` - Inline chat trigger variant for staff use.",
            "`/join` `/playlofi [url]` `/status` `/leave` - Slash versions of lofi controls.",
            "`!stick <message>` - Create sticky message in channel.",
            "`!unstick` / `!removestick` - Remove sticky behavior in channel.",
            "`!pingrole @role` - Set role to ping in attendance posts (admin).",
            "`!reviveping @role` - Set role used by revive chat ping (admin).",
            "`!revivechannel #channel` - Set dedicated revive ping channel (admin).",
            "`!languageassignchannel #english-channel [#dual-channel]` - Route English-only and optional dual translation outputs (admin).",
            "`/languagetoggle [on/off]` - Enable or disable auto-translation (admin).",
            "`/setuptranslator` - Show translator setup commands and examples (admin).",
            "`!revivechat` - Send revive ping in configured/fallback channel.",
            "`!deleteallmessage` / `/deleteallmessage` - Delete all messages in the current channel (admin/mod).",
            "`!setupconfession` - First-time confession setup (creates channels + role).",
            "`/setupsuggestion` - Create a suggestions channel with submit-only button flow (admin).",
            "`/setuphttyd` - Admin setup hook for HTTYD commands visibility.",
            "`!confess <message>` - Send an anonymous confession.",
            "`!reply <message>` - Send an anonymous confession reply.",
            "`/setupconfession` `/configconfession` `/confess` `/reply` - Confession setup/config + posting.",
            "`/setup247music` - Admin slash setup for always-on lofi streaming.",
            "`/say <message>` - Make the bot send your message.",
            "`/saylogs #channel` - Set channel where `!say` and `/say` usage is logged (admin).",
            "`/dmlogs #channel` - Set channel where `/directmessage` activity is logged (admin).",
            "`/draw [caption]` - Upload and share your drawing with everyone in the channel.",
            "`/pet adopt <type>` - Adopt a virtual pet.",
            "`/pet status` `/pet feed` `/pet play` `/pet clean` `/pet sleep` `/pet daily` - Core pet care loop.",
            "`/pet fetch` `/pet race` `/pet battle` `/pet shop` - Pet minigames and shop.",
            "`/annoucements [channel1] [channel2] [channel3] [channel4] [channel5] [title] [message] [timestamp] [author]` - Save 3-5 channels and/or broadcast an embed announcement (admin).",
            "`!removereport` - Delete latest attendance report message (admin).",
            "`!reset` - Reset attendance configuration (admin).",
            "`!resetpermitrole` - Clear permit role requirement (admin).",
            "`!fakeban @member [reason]` - Send a fake ban notice (fun).",
            "`!gayradar @member` - Show a playful rainbow-vibes score (for fun only)."
        ]
    }

    embeds: list[discord.Embed] = []
    current_embed = discord.Embed(
        title="📘 Bot Command List",
        description="Here are the available commands.",
        color=discord.Color.blurple()
    )
    current_chars = len(current_embed.title or "") + len(current_embed.description or "")

    for section_name, section_commands in command_sections.items():
        field_value = "\n".join(section_commands)
        if len(field_value) > 1024:
            field_value = field_value[:1021] + "..."

        projected_chars = current_chars + len(section_name) + len(field_value)
        if len(current_embed.fields) >= 25 or projected_chars > 5800:
            embeds.append(current_embed)
            current_embed = discord.Embed(
                title="📘 Bot Command List (cont.)",
                description="More commands:",
                color=discord.Color.blurple()
            )
            current_chars = len(current_embed.title or "") + len(current_embed.description or "")

        current_embed.add_field(name=section_name, value=field_value, inline=False)
        current_chars += len(section_name) + len(field_value)

    current_embed.set_footer(text="Tip: Most setup commands require administrator permissions.")
    embeds.append(current_embed)
    return embeds


@bot.tree.command(name="help", description="Show the list of available bot commands.")
async def help_slash(interaction: discord.Interaction):
    """Shows the list of available bot commands."""
    embeds = build_help_embeds()
    await interaction.response.send_message(embed=embeds[0], ephemeral=True)
    for extra_embed in embeds[1:]:
        await interaction.followup.send(embed=extra_embed, ephemeral=True)


@bot.command(name='help')
async def help_command_legacy(ctx):
    """Legacy message-command help bridge."""
    embeds = build_help_embeds()
    for embed in embeds:
        await ctx.send(embed=embed)

@bot.tree.command(name="dashboard", description="Open the web dashboard to configure the bot.")
async def dashboard_slash(interaction: discord.Interaction):
    dashboard_url = DASHBOARD_URL or "https://your-dashboard-url.example"
    embed = discord.Embed(
        title="🌐 RegistrarBot Dashboard",
        description=(
            "Configure your server settings on the web dashboard.\n"
            f"[Open Dashboard]({dashboard_url})"
        ),
        color=discord.Color.blurple()
    )
    embed.set_footer(text="Tip: Set DASHBOARD_URL in your environment to use your real site.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="setupnick", description="Show autonick setup commands.")
async def setupnick_slash(interaction: discord.Interaction):
    embed = discord.Embed(
        title="⚙️ Auto-Nickname Overview",
        description=(
            "**What it typically does**\n\n"
            "1. **Auto nickname changes**\n"
            "When someone joins a server, their nickname can be set automatically.\n"
            "Example: `John` → `John | Member`\n\n"
            "2. **Role-based nicknames**\n"
            "If a user's role changes, their nickname can update with a matching tag.\n"
            "Example: `John` → `John [Admin]`\n\n"
            "3. **Templates**\n"
            "Uses patterns like `{username}` and `{role}` so nickname formats stay customizable.\n\n"
            "Similar bots commonly:\n"
            "• Automatically rename members\n"
            "• Add prefixes/suffixes\n"
            "• Update nicknames when roles change\n\n"
            "**How it works (technical idea)**\n\n"
            "• Connect to Discord's API\n"
            "• Listen for events (member joins, role updates)\n"
            "• Run handlers such as `onMemberJoin()` and `onRoleUpdate()`\n"
            "• Apply nickname changes using Discord nickname edit APIs"
        ),
        color=discord.Color.green()
    )
    embed.set_footer(text="Tip: Use !nicksettings to review your current nickname configuration.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="setupautonickrole", description="Bulk set role -> nickname tag mappings (up to 100 pairs).")
@app_commands.describe(
    mappings="Format: @Role [TAG] @Role [TAG] ... (optional; leave blank to show usage)"
)
async def setupautonickrole_slash(interaction: discord.Interaction, mappings: str | None = None):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used inside a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_nicknames:
        await interaction.response.send_message("You need **Manage Nicknames** permission to use this command.", ephemeral=True)
        return

    if not mappings:
        await interaction.response.send_message(
            "Usage: `/setupautonickrole mappings:@Role [TAG] @Role [TAG]`\n"
            "Example: `@Admin [ADMIN] @Moderator [MODERATOR]`",
            ephemeral=True
        )
        return

    parsed_pairs, error = _parse_bulk_autonick_mappings(mappings, max_pairs=100)
    if error:
        await interaction.response.send_message(f"❌ {error}", ephemeral=True)
        return

    missing_roles = []
    applied = 0
    for role_id, tag in parsed_pairs:
        role = interaction.guild.get_role(role_id)
        if not role:
            missing_roles.append(role_id)
            continue
        database.upsert_autonick_rule(interaction.guild.id, role.id, tag)
        applied += 1

    _settings_cache.pop(interaction.guild.id, None)
    updated, skipped = await reconcile_autonicks_for_guild(interaction.guild) if applied else (0, 0)
    response_lines = [
        f"✅ Saved **{applied}** auto-nick role mapping(s).",
        "These mappings are persisted per server and survive restarts/redeploys.",
        f"Reconciled members now. Updated: **{updated}**, unchanged/skipped: **{skipped}**.",
    ]
    if missing_roles:
        sample = ", ".join(f"`{role_id}`" for role_id in missing_roles[:10])
        extra = " ..." if len(missing_roles) > 10 else ""
        response_lines.append(f"⚠️ Missing roles skipped ({len(missing_roles)}): {sample}{extra}")
    await interaction.response.send_message("\n".join(response_lines), ephemeral=True)


DRAGON_PROFILE_KEY = "dragon_rpg_v1"
DRAGON_SHOP_ITEMS = {
    "potion": {"name": "🧪 Potion", "price": 25},
    "mega_potion": {"name": "💉 Mega Potion", "price": 70},
    "dragon_snack": {"name": "🍖 Dragon Snack", "price": 40},
}


def _default_dragon_profile() -> dict:
    return {
        "gold": 100,
        "xp": 0,
        "level": 1,
        "dragons": [],
        "activeDragon": None,
        "items": {"potion": 2},
    }


def _get_dragon_profile(guild_id: int, user_id: int) -> dict:
    raw = database.get_game_progress(guild_id, user_id, DRAGON_PROFILE_KEY)
    profile = _default_dragon_profile()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                profile.update(parsed)
        except json.JSONDecodeError:
            logger.warning("Invalid dragon profile JSON for guild=%s user=%s", guild_id, user_id)
    if not isinstance(profile.get("dragons"), list):
        profile["dragons"] = []
    if not isinstance(profile.get("items"), dict):
        profile["items"] = {"potion": 2}
    profile["gold"] = int(profile.get("gold", 100))
    profile["xp"] = int(profile.get("xp", 0))
    profile["level"] = max(1, int(profile.get("level", 1)))
    if profile.get("activeDragon") is not None:
        try:
            profile["activeDragon"] = int(profile.get("activeDragon"))
        except (TypeError, ValueError):
            profile["activeDragon"] = None
    return profile


def _save_dragon_profile(guild_id: int, user_id: int, profile: dict):
    database.upsert_game_progress(guild_id, user_id, DRAGON_PROFILE_KEY, json.dumps(profile))


def _add_dragon_xp(profile: dict, amount: int) -> bool:
    profile["xp"] = int(profile.get("xp", 0)) + int(amount)
    needed = int(profile.get("level", 1)) * 100
    if profile["xp"] >= needed:
        profile["xp"] -= needed
        profile["level"] = int(profile.get("level", 1)) + 1
        return True
    return False


def _active_dragon(profile: dict) -> dict | None:
    active_index = profile.get("activeDragon")
    dragons = profile.get("dragons", [])
    if active_index is None:
        return None
    if 0 <= active_index < len(dragons):
        return dragons[active_index]
    return None


dragon_group = app_commands.Group(name="dragon", description="Dragon RPG commands (inventory, battle, PvP).")


@dragon_group.command(name="starter", description="Claim your first dragon.")
async def dragon_starter(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    profile = _get_dragon_profile(interaction.guild.id, interaction.user.id)
    if profile["dragons"]:
        await interaction.response.send_message("You already have a dragon. Use `/dragon inventory`.", ephemeral=True)
        return

    starter = {
        "name": "Emberwing",
        "level": 1,
        "xp": 0,
        "hp": 45,
        "attack": 10,
    }
    profile["dragons"].append(starter)
    profile["activeDragon"] = 0
    _save_dragon_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(
        "🐲 You claimed your starter dragon **Emberwing**! It is now your active dragon."
    )


@dragon_group.command(name="setdragon", description="Set your active dragon by number.")
@app_commands.describe(index="Dragon number from your inventory list (starts at 1)")
async def dragon_setdragon(interaction: discord.Interaction, index: app_commands.Range[int, 1, 50]):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = _get_dragon_profile(interaction.guild.id, interaction.user.id)
    selected = index - 1
    if selected >= len(profile["dragons"]):
        await interaction.response.send_message("❌ Invalid dragon number.", ephemeral=True)
        return
    profile["activeDragon"] = selected
    _save_dragon_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(f"🐲 Active dragon set to **{profile['dragons'][selected]['name']}**.")


@dragon_group.command(name="inventory", description="View your dragon inventory.")
async def dragon_inventory(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = _get_dragon_profile(interaction.guild.id, interaction.user.id)
    dragons = profile.get("dragons", [])

    dragon_lines = []
    for i, dragon in enumerate(dragons):
        marker = " ⭐ Active" if profile.get("activeDragon") == i else ""
        dragon_lines.append(
            f"{i + 1}. {dragon.get('name', 'Dragon')} (Lv {dragon.get('level', 1)} • "
            f"HP {dragon.get('hp', 0)} • ATK {dragon.get('attack', 0)}){marker}"
        )

    embed = discord.Embed(
        title=f"🎒 {interaction.user.display_name}'s Dragon Inventory",
        color=discord.Color.from_rgb(0, 255, 153)
    )
    embed.add_field(name="💰 Gold", value=str(profile.get("gold", 0)), inline=True)
    embed.add_field(name="⭐ Level", value=str(profile.get("level", 1)), inline=True)
    embed.add_field(name="🧬 XP", value=str(profile.get("xp", 0)), inline=True)
    embed.add_field(name="🐲 Dragons", value="\n".join(dragon_lines) if dragon_lines else "None", inline=False)
    await interaction.response.send_message(embed=embed)


@dragon_group.command(name="shop", description="View dragon shop items.")
async def dragon_shop(interaction: discord.Interaction):
    embed = discord.Embed(title="🏪 Dragon Shop", color=discord.Color.gold())
    for item in DRAGON_SHOP_ITEMS.values():
        embed.add_field(name=item["name"], value=f"💰 {item['price']} gold", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@dragon_group.command(name="battle", description="Fight a wild dragon for rewards.")
async def dragon_battle(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = _get_dragon_profile(interaction.guild.id, interaction.user.id)
    active = _active_dragon(profile)
    if not active:
        await interaction.response.send_message("❌ Set an active dragon first with `/dragon setdragon`.", ephemeral=True)
        return

    wild = {"name": "Wild Wyrmling", "hp": 35, "attack": 8}
    hp1 = int(active.get("hp", 0))
    hp2 = int(wild["hp"])
    while hp1 > 0 and hp2 > 0:
        hp2 -= int(active.get("attack", 0))
        if hp2 <= 0:
            break
        hp1 -= int(wild["attack"])

    if hp1 <= 0:
        await interaction.response.send_message(f"💀 {active.get('name', 'Your dragon')} lost against {wild['name']}.")
        return

    profile["gold"] = int(profile.get("gold", 0)) + 25
    leveled_up = _add_dragon_xp(profile, 50)
    _save_dragon_profile(interaction.guild.id, interaction.user.id, profile)

    embed = discord.Embed(title="⚔️ Battle Result", color=discord.Color.green())
    embed.description = f"{active.get('name', 'Your dragon')} defeated {wild['name']}!"
    embed.add_field(name="💰 Gold Earned", value="25", inline=True)
    embed.add_field(name="🧬 XP Earned", value="50", inline=True)
    embed.add_field(name="⭐ Player Level", value=str(profile.get("level", 1)), inline=True)
    await interaction.response.send_message(embed=embed)
    if leveled_up:
        await interaction.followup.send(f"🎉 You leveled up! Now level {profile.get('level', 1)}")


@dragon_group.command(name="pvp", description="Fight another player's active dragon.")
@app_commands.describe(target="Player to fight")
async def dragon_pvp(interaction: discord.Interaction, target: discord.Member):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    if target.bot:
        await interaction.response.send_message("❌ You can't challenge bots.", ephemeral=True)
        return
    if target.id == interaction.user.id:
        await interaction.response.send_message("❌ Challenge another player, not yourself.", ephemeral=True)
        return

    profile_1 = _get_dragon_profile(interaction.guild.id, interaction.user.id)
    profile_2 = _get_dragon_profile(interaction.guild.id, target.id)
    dragon_1 = _active_dragon(profile_1)
    dragon_2 = _active_dragon(profile_2)
    if not dragon_1 or not dragon_2:
        await interaction.response.send_message("❌ Both players need an active dragon.", ephemeral=True)
        return

    hp1 = int(dragon_1.get("hp", 0))
    hp2 = int(dragon_2.get("hp", 0))
    while hp1 > 0 and hp2 > 0:
        hp2 -= int(dragon_1.get("attack", 0))
        if hp2 <= 0:
            break
        hp1 -= int(dragon_2.get("attack", 0))

    winner = interaction.user.display_name if hp1 > 0 else target.display_name
    embed = discord.Embed(title="⚔️ PvP Battle", color=discord.Color.red())
    embed.description = f"{dragon_1.get('name', 'Dragon 1')} vs {dragon_2.get('name', 'Dragon 2')}"
    embed.add_field(name="Winner", value=winner, inline=False)
    await interaction.response.send_message(embed=embed)


pet_group = app_commands.Group(name="pet", description="Adopt and care for your virtual pet.")


@pet_group.command(name="adopt", description="Adopt your first pet.")
@app_commands.describe(pet_type="Choose a pet type (dog, cat, dragon, fox, rabbit)")
async def pet_adopt(interaction: discord.Interaction, pet_type: str):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    normalized_type = (pet_type or "").strip().lower()
    if normalized_type not in PET_TYPE_ALIASES:
        supported = ", ".join(sorted(PET_TYPE_ALIASES.keys()))
        await interaction.response.send_message(f"Unsupported pet type. Try one of: {supported}.", ephemeral=True)
        return

    existing = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if existing:
        await interaction.response.send_message(
            f"You already adopted **{existing.get('pet_name', 'your pet')}**. Use `/pet status` to check on them.",
            ephemeral=True
        )
        return

    profile = _default_pet_profile(normalized_type)
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(
        f"🐾 You adopted a **{PET_TYPES[normalized_type]}** named **{profile['pet_name']}**! "
        "Use `/pet name` to rename and `/pet status` to view stats."
    )


@pet_group.command(name="name", description="Rename your pet.")
@app_commands.describe(name="New pet name")
async def pet_name(interaction: discord.Interaction, name: str):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return
    cleaned = (name or "").strip()
    if not cleaned or len(cleaned) > 32:
        await interaction.response.send_message("Pet name must be between 1 and 32 characters.", ephemeral=True)
        return
    profile["pet_name"] = cleaned
    profile["updated_at"] = _iso_now()
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(f"✅ Your pet is now named **{cleaned}**.")


@pet_group.command(name="status", description="Show your pet's current stats.")
async def pet_status(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return

    profile, changed = _apply_pet_decay(profile)
    days_together = _days_between(profile.get("adopted_at"))
    pet_form = _pet_form_label(profile.get("pet_type", "pet"), profile.get("bond", 0), days_together)
    profile["evolved_stage"] = "evolved" if "Evolved" in pet_form or "Guard Dog" in pet_form or "Mystic Cat" in pet_form or "Fire Dragon" in pet_form or "Spirit Fox" in pet_form or "Moon Rabbit" in pet_form else "base"
    if changed:
        database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)

    embed = discord.Embed(
        title=f"🐾 {profile.get('pet_name', 'Your Pet')} — Status",
        color=discord.Color.teal()
    )
    embed.add_field(name="Type / Form", value=pet_form, inline=False)
    embed.add_field(name="❤️ Happiness", value=str(profile.get("happiness", 0)), inline=True)
    embed.add_field(name="🍗 Hunger", value=str(profile.get("hunger", 0)), inline=True)
    embed.add_field(name="🧼 Cleanliness", value=str(profile.get("cleanliness", 0)), inline=True)
    embed.add_field(name="⚡ Energy", value=str(profile.get("energy", 0)), inline=True)
    embed.add_field(name="🤝 Bond", value=f"{profile.get('bond', 0)} ({_bond_tier(profile.get('bond', 0))})", inline=False)
    embed.add_field(name="🪙 Coins", value=str(profile.get("coins", 0)), inline=True)
    embed.add_field(name="🔥 Daily Streak", value=str(profile.get("streak", 0)), inline=True)
    embed.set_footer(text=f"Days together: {days_together}")
    await interaction.response.send_message(embed=embed)


async def _run_pet_care_action(interaction: discord.Interaction, action_key: str, delta: dict, bond_gain: int):
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return
    profile, changed = _apply_pet_decay(profile)
    remaining = _cooldown_remaining(profile, action_key)
    if remaining > 0:
        await interaction.response.send_message(
            f"⏳ `{action_key}` is on cooldown. Try again in **{_format_remaining(remaining)}**.",
            ephemeral=True
        )
        return

    for key, amount in delta.items():
        profile[key] = _clamp_stat(profile.get(key, 0) + amount)
    profile["bond"] = _clamp_stat(profile.get("bond", 0) + bond_gain)
    timestamp_now = _iso_now()
    timestamp_field = {
        "feed": "last_fed_at",
        "play": "last_played_at",
        "clean": "last_cleaned_at",
        "sleep": "last_slept_at",
    }[action_key]
    profile[timestamp_field] = timestamp_now
    profile["updated_at"] = timestamp_now
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(
        f"✅ `{action_key}` complete for **{profile.get('pet_name', 'your pet')}**! "
        f"Bond is now **{profile.get('bond', 0)}**."
    )


@pet_group.command(name="feed", description="Feed your pet (6h cooldown).")
async def pet_feed(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    await _run_pet_care_action(interaction, "feed", {"hunger": 18, "happiness": 3}, 2)


@pet_group.command(name="play", description="Play with your pet (4h cooldown).")
async def pet_play(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    await _run_pet_care_action(interaction, "play", {"happiness": 18, "energy": -8}, 4)


@pet_group.command(name="clean", description="Clean your pet (8h cooldown).")
async def pet_clean(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    await _run_pet_care_action(interaction, "clean", {"cleanliness": 24}, 2)


@pet_group.command(name="sleep", description="Let your pet sleep (10h cooldown).")
async def pet_sleep(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    await _run_pet_care_action(interaction, "sleep", {"energy": 30, "hunger": -5}, 3)


@pet_group.command(name="daily", description="Claim your pet daily rewards and streak.")
async def pet_daily(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return
    remaining = _cooldown_remaining(profile, "daily")
    if remaining > 0:
        await interaction.response.send_message(
            f"⏳ Daily reward not ready yet. Come back in **{_format_remaining(remaining)}**.",
            ephemeral=True
        )
        return

    now = _utc_now()
    last = _parse_iso(profile.get("last_checkin_date"))
    if last and (now.date() - last.date()).days == 1:
        profile["streak"] = int(profile.get("streak", 0)) + 1
    elif last and (now.date() - last.date()).days == 0:
        await interaction.response.send_message("You already claimed daily today.", ephemeral=True)
        return
    else:
        profile["streak"] = 1

    streak = int(profile.get("streak", 1))
    bonus = 25
    if streak >= 30:
        bonus = 250
    elif streak >= 7:
        bonus = 100

    profile["coins"] = int(profile.get("coins", 0)) + bonus
    profile["happiness"] = _clamp_stat(profile.get("happiness", 0) + 8)
    profile["bond"] = _clamp_stat(profile.get("bond", 0) + 3)
    profile["total_checkins"] = int(profile.get("total_checkins", 0)) + 1
    profile["last_checkin_date"] = _iso_now()
    profile["updated_at"] = _iso_now()
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)

    extra = ""
    if streak == 7:
        extra = "\n🎁 Day 7 bonus unlocked: **Rare Item Crate**!"
    elif streak == 30:
        extra = "\n🎨 Day 30 bonus unlocked: **Exclusive Pet Skin**!"
    await interaction.response.send_message(
        f"✅ Daily claimed! +{bonus} coins\n"
        f"🔥 Streak: **{streak}** days{extra}"
    )


@pet_group.command(name="shop", description="Open the pet shop.")
async def pet_shop(interaction: discord.Interaction):
    embed = discord.Embed(title="🛍️ Pet Shop", color=discord.Color.gold())
    embed.add_field(name="Food 🍖", value="Basic Kibble — 20 coins\nDeluxe Meal — 60 coins", inline=False)
    embed.add_field(name="Toys 🎾", value="Ball — 40 coins\nPuzzle Toy — 90 coins", inline=False)
    embed.add_field(name="Skins 🎨", value="Classic Coat — 120 coins\nLegendary Aura — 300 coins", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@pet_group.command(name="fetch", description="Play fetch for bonus coins.")
async def pet_fetch(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return
    profile["coins"] = int(profile.get("coins", 0)) + 12
    profile["happiness"] = _clamp_stat(profile.get("happiness", 0) + 6)
    profile["energy"] = _clamp_stat(profile.get("energy", 0) - 4)
    profile["bond"] = _clamp_stat(profile.get("bond", 0) + 2)
    profile["updated_at"] = _iso_now()
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message("🎾 Fetch complete! You earned **12 coins** and boosted bond.")


@pet_group.command(name="race", description="Race your pet for a random reward.")
async def pet_race(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return
    reward = 20 if int(profile.get("energy", 0)) >= 40 else 8
    profile["coins"] = int(profile.get("coins", 0)) + reward
    profile["energy"] = _clamp_stat(profile.get("energy", 0) - 10)
    profile["bond"] = _clamp_stat(profile.get("bond", 0) + 2)
    profile["updated_at"] = _iso_now()
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(f"🏁 Race complete! Prize: **{reward} coins**.")


@pet_group.command(name="battle", description="Friendly pet battle (light RPG style).")
async def pet_battle(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    profile = database.get_pet_profile(interaction.guild.id, interaction.user.id)
    if not profile:
        await interaction.response.send_message("You don't have a pet yet. Use `/pet adopt <type>` first.", ephemeral=True)
        return
    won = int(profile.get("bond", 0)) >= 30
    coins = 18 if won else 5
    profile["coins"] = int(profile.get("coins", 0)) + coins
    profile["energy"] = _clamp_stat(profile.get("energy", 0) - 12)
    if won:
        profile["bond"] = _clamp_stat(profile.get("bond", 0) + 3)
    profile["updated_at"] = _iso_now()
    database.upsert_pet_profile(interaction.guild.id, interaction.user.id, profile)
    await interaction.response.send_message(
        ("⚔️ Victory!" if won else "⚔️ Tough match!") + f" You earned **{coins} coins**."
    )


bot.tree.add_command(pet_group)
bot.tree.add_command(dragon_group)


streak_group = app_commands.Group(name="streak", description="Check and track message streak progress.")


@streak_group.command(name="status", description="Show your message streak status.")
@app_commands.describe(user="Optional user to inspect")
async def streak_status(interaction: discord.Interaction, user: discord.Member | None = None):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    target = user or interaction.user
    payload = database.get_message_streak(interaction.guild.id, target.id)
    summary = _format_streak_status_message(payload)

    embed = discord.Embed(
        title="🔥 Streak Status",
        description=summary,
        color=discord.Color.orange(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_author(name=target.display_name, icon_url=target.display_avatar.url)
    embed.set_thumbnail(url=target.display_avatar.url)
    await interaction.response.send_message(embed=embed, ephemeral=True)


bot.tree.add_command(streak_group)


@bot.tree.command(name="setup247music", description="Configure always-on 24/7 lofi streaming.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    voice_channel="Voice channel for continuous lofi playback",
    stream_url="Optional custom lofi stream URL"
)
async def setup247music_slash(
    interaction: discord.Interaction,
    voice_channel: Union[discord.VoiceChannel, discord.StageChannel, None] = None,
    stream_url: str = None
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    selected_channel = voice_channel
    if not selected_channel and isinstance(interaction.user, discord.Member) and interaction.user.voice:
        if isinstance(interaction.user.voice.channel, (discord.VoiceChannel, discord.StageChannel)):
            selected_channel = interaction.user.voice.channel
    if not selected_channel:
        await interaction.response.send_message(
            "Choose a voice channel (or join one first), then run `/setup247music` again.",
            ephemeral=True
        )
        return

    state = get_lofi_state(interaction.guild.id)
    state["enabled"] = True
    state["voice_channel_id"] = selected_channel.id
    state["stream_url"] = (stream_url or state.get("stream_url") or DEFAULT_LOFI_STREAM_URL).strip()
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1
    save_lofi_data(interaction.guild.id, state)

    ok, message = await ensure_lofi_connected(interaction.guild.id)
    prefix = "✅" if ok else "⚠️"
    await interaction.response.send_message(
        f"{prefix} Saved 24/7 music setup.\n"
        f"Channel: {selected_channel.mention}\n"
        f"Stream: {state['stream_url']}\n"
        f"Status: {message}\n\n"
        "Commands: `!join`, `!playlofi [url]`, `!status`, `!leave`.",
        ephemeral=True
    )


@bot.tree.command(name="setuphttyd", description="Enable the HTTYD command set for this server.")
@app_commands.default_permissions(administrator=True)
async def setuphttyd_slash(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    embed = discord.Embed(
        title="🐉 HTTYD Setup Ready",
        description=(
            "The `/setuphttyd` command is now available in this server.\n\n"
            "✅ Setup status: **Enabled**\n"
            "Use `/help` to see the currently available gameplay and utility commands."
        ),
        color=discord.Color.green()
    )
    embed.set_footer(text="If slash commands seem outdated, run /synccommands as an admin.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="setupblinddate", description="Setup Blind Dating channels and dashboard.")
@app_commands.default_permissions(administrator=True)
async def setup_blind_dating(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("This command can only be used inside a server.", ephemeral=True)
        return

    me = interaction.guild.me
    if not me:
        await interaction.response.send_message("I can't verify my permissions right now. Try again in a moment.", ephemeral=True)
        return

    perms = interaction.guild.me.guild_permissions
    required_checks = {
        "Manage Channels": perms.manage_channels,
        "Manage Roles": perms.manage_roles,
        "Move Members": perms.move_members
    }
    missing = [name for name, has_perm in required_checks.items() if not has_perm]
    if missing:
        await interaction.response.send_message(
            "❌ Missing Permissions: " + ", ".join(missing) + ".\n"
            "Grant Administrator or these permissions, then run /setupblinddate again.",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    category = discord.utils.get(guild.categories, name="Blind Dating")
    if category is None:
        category = await guild.create_category("Blind Dating")

    lobby = discord.utils.get(guild.voice_channels, name="Lobby", category=category)
    if lobby is None:
        lobby = await guild.create_voice_channel("Lobby", category=category)

    dashboard = discord.utils.get(guild.text_channels, name="dashboard", category=category)
    if dashboard is None:
        dashboard = await guild.create_text_channel("dashboard", category=category)

    payload = load_blind_date_data(guild.id)
    payload["category_id"] = category.id
    payload["lobby_channel_id"] = lobby.id
    payload["dashboard_channel_id"] = dashboard.id

    embed = discord.Embed(
        title="Blind Dating Dashboard",
        description=(
            "1) Join **Lobby** voice channel.\n"
            "2) Click **Match Me** below.\n"
            "3) Wait for a private Date Room."
        ),
        color=discord.Color.magenta()
    )
    embed.set_footer(text="Self-matching test mode is currently enabled.")

    dashboard_message = None
    existing_message_id = payload.get("dashboard_message_id")
    if existing_message_id:
        try:
            dashboard_message = await dashboard.fetch_message(existing_message_id)
            await dashboard_message.edit(embed=embed, view=BlindDateMatchView())
        except Exception:
            dashboard_message = None

    if dashboard_message is None:
        dashboard_message = await dashboard.send(embed=embed, view=BlindDateMatchView())
    payload["dashboard_message_id"] = dashboard_message.id

    save_blind_date_data(guild.id, payload)

    await interaction.followup.send(
        f"✅ Blind Dating setup complete.\n"
        f"Category: {category.mention}\n"
        f"Lobby VC: {lobby.mention}\n"
        f"Dashboard: {dashboard.mention}",
        ephemeral=True
    )

@bot.command(name='fakeban')
@commands.has_permissions(manage_messages=True)
async def fake_ban(ctx, member: discord.Member = None, *, reason: str = "No reason provided"):
    """
    Sends a fake ban embed without actually banning anyone.
    Usage: !fakeban @Member [reason]
    """
    if member is None:
        await ctx.send("Usage: `!fakeban @Member [reason]`")
        return

    embed = discord.Embed(
        title="🚨 FAKE BAN NOTICE 🚨",
        description=f"{member.mention} has been **FAKE BANNED**.\n*(This is only for fun. No real ban was performed.)*",
        color=discord.Color.red(),
        timestamp=discord.utils.utcnow()
    )
    embed.add_field(name="Member", value=f"{member} (`{member.id}`)", inline=False)
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.set_footer(text=f"Issued by {ctx.author}", icon_url=ctx.author.display_avatar.url)

    await ctx.send(embed=embed)


@bot.command(name='reto', aliases=['ship'])
async def reto_command(ctx, member_one: discord.Member = None, member_two: discord.Member = None):
    """Ship two users together and return a deterministic compatibility score."""
    if not ctx.guild:
        await ctx.send("This command can only be used inside a server.")
        return

    if member_one is None and member_two is None:
        await ctx.send("Usage: `!reto @user` or `!reto @user @user`")
        return
    if member_one is not None and member_two is None:
        member_two = member_one
        member_one = ctx.author
    elif member_one is None and member_two is not None:
        member_one = ctx.author

    compatibility = compute_reto_percentage(ctx.guild.id, member_one.id, member_two.id)
    embed = build_reto_embed(member_one, member_two, compatibility)
    if member_one.id == member_two.id:
        embed.title = "🪞 Self-Love Reto Report"
        embed.add_field(
            name="Self Match",
            value="Being your own biggest supporter is always a perfect move 💖",
            inline=False,
        )
    await ctx.send(embed=embed)
    await maybe_send_reto_star_match(
        guild=ctx.guild,
        source_channel=ctx.channel,
        embed=embed,
        compatibility=compatibility,
    )


@bot.command(name='gayradar', aliases=['gayradaruser', 'gay_radar'])
async def gay_radar_command(ctx, member: discord.Member = None):
    """
    Playful command that returns a random rainbow-vibes reading.
    This command does not and cannot determine anyone's sexual orientation.
    Usage: !gayradar @user
    """
    if not ctx.guild:
        await ctx.send("This command can only be used inside a server.")
        return

    target = member or ctx.author
    seed = f"gayradar:{ctx.guild.id}:{target.id}"
    score = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16) % 101

    if score >= 85:
        vibe_line = "🌈 Radar says MAX rainbow energy detected."
    elif score >= 60:
        vibe_line = "✨ Radar says colorful vibes are strong today."
    elif score >= 35:
        vibe_line = "🪩 Radar says medium rainbow signal."
    else:
        vibe_line = "🛰️ Radar says low rainbow signal right now."

    embed = discord.Embed(
        title="🌈 Gay Radar (For Fun)",
        description=(
            f"{target.mention} scored **{score}%** on the Rainbow Vibes Meter.\n"
            f"{vibe_line}"
        ),
        color=discord.Color.magenta(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(
        name="Important",
        value="This is a joke command and cannot determine sexual orientation.",
        inline=False,
    )
    embed.set_footer(text=f"Requested by {ctx.author}", icon_url=ctx.author.display_avatar.url)
    await ctx.send(embed=embed)


@bot.command(name='lesbiancheck', aliases=['lesbian_check', 'lesbian'])
async def lesbiancheck_command(ctx, member: discord.Member = None):
    """
    Safety command that avoids guessing anyone's sexual orientation.
    Usage: !lesbiancheck @user
    """
    if not ctx.guild:
        await ctx.send("This command can only be used inside a server.")
        return

    target = member or ctx.author
    embed = discord.Embed(
        title="🛡️ LesbianCheck",
        description=(
            f"{target.mention} I will not guess whether someone is lesbian or not.\n"
            "Sexual orientation is personal and cannot be determined by this bot."
        ),
        color=discord.Color.blurple(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_footer(text=f"Requested by {ctx.author}", icon_url=ctx.author.display_avatar.url)
    await ctx.send(embed=embed)


def build_cutecheck_embed(target: discord.abc.User, requester: discord.abc.User) -> discord.Embed:
    score = random.randint(1, 100)
    verdict = "✅ Cute confirmed!" if score >= 50 else "❌ Not cute today (try again for better luck)."
    compliments = [
        "Certified adorable energy 💖",
        "Main character smile detected ✨",
        "Cute levels are legally dangerous 😳",
        "Serving soft vibes and charm 🌸",
    ]

    embed = discord.Embed(
        title="💘 Cute Check",
        description=f"{target.mention} got **{score}/100** on the cute scale.",
        color=discord.Color.pink(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="Result", value=verdict, inline=False)
    if score >= 50:
        embed.add_field(name="Vibe", value=random.choice(compliments), inline=False)
    embed.set_footer(text=f"Requested by {requester}", icon_url=requester.display_avatar.url)
    embed.set_thumbnail(url=target.display_avatar.url)
    return embed


@bot.command(name='cutecheck', aliases=['cute_check'])
async def cutecheck_command(ctx, member: discord.Member = None):
    """Check a user's cute score. Usage: !cutecheck @user (or without mention for yourself)."""
    if not ctx.guild:
        await ctx.send("This command can only be used inside a server.")
        return

    target = member or ctx.author
    embed = build_cutecheck_embed(target, ctx.author)
    await ctx.send(embed=embed)


@bot.tree.command(name="setnick", description="Set a nickname for yourself (or another user if you can manage nicknames).")
@app_commands.describe(member="Who to rename (defaults to yourself)", new_name="New nickname to set")
async def setnick_slash(interaction: discord.Interaction, new_name: str, member: discord.Member | None = None):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used inside a server.", ephemeral=True)
        return

    target_member = member or interaction.user
    actor = interaction.user
    if not isinstance(target_member, discord.Member) or not isinstance(actor, discord.Member):
        await interaction.response.send_message("Unable to resolve server member data for this command.", ephemeral=True)
        return

    if target_member.id != actor.id and not actor.guild_permissions.manage_nicknames:
        await interaction.response.send_message(
            "You can only use this command on yourself unless you have Manage Nicknames permission.",
            ephemeral=True,
        )
        return

    class _CtxProxy:
        def __init__(self, guild: discord.Guild):
            self.guild = guild

    allowed, message = can_manage_nick(_CtxProxy(interaction.guild), target_member)
    if not allowed:
        await interaction.response.send_message(f"Failed: {message}", ephemeral=True)
        return

    settings = load_settings(interaction.guild.id)
    suffix = settings.get("suffix_format", SUFFIX)
    if suffix and not new_name.endswith(suffix):
        max_name_length = 32 - len(suffix)
        if len(new_name) > max_name_length:
            new_nick = new_name[:max_name_length] + suffix
        else:
            new_nick = new_name + suffix
    else:
        new_nick = new_name

    try:
        await target_member.edit(nick=new_nick)
    except discord.Forbidden:
        await interaction.response.send_message(
            "Failed: I don't have permission to change that user's nickname.",
            ephemeral=True,
        )
        return
    except Exception as e:
        await interaction.response.send_message(f"An error occurred: {e}", ephemeral=True)
        return

    await interaction.response.send_message(
        f"Successfully changed nickname for {target_member.mention} to `{new_nick}`"
    )


@bot.command(name='say')
async def say_command(ctx, *, message: str = None):
    if not message:
        await ctx.send("Usage: `!say <message>`")
        return
    await ctx.send(message)
    await log_say_message(
        guild=ctx.guild,
        actor=ctx.author,
        source_channel=ctx.channel,
        content=message,
        origin="Prefix (!say)"
    )


def _extract_custom_emoji_reference(raw: str | None) -> tuple[int, bool, str] | None:
    if not raw:
        return None
    parsed = discord.PartialEmoji.from_str(raw.strip())
    if not parsed or parsed.id is None:
        return None
    emoji_name = (parsed.name or "stolen_emoji").strip("_") or "stolen_emoji"
    return parsed.id, bool(parsed.animated), emoji_name


async def _download_discord_asset(url: str) -> bytes | None:
    if not url:
        return None
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                return None
            return await response.read()


@bot.command(name='steal')
@commands.has_permissions(manage_emojis_and_stickers=True)
async def steal_asset(ctx, mode: str = None, *, source: str = None):
    """
    Steal a custom emoji or sticker from another server and add it to this server.
    Usage:
      !steal emoji <:name:id> [new_name]
      !steal sticker  (reply to a message that contains a sticker)
    """
    if ctx.guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not mode:
        await ctx.send("Usage: `!steal emoji <:name:id> [new_name]` or `!steal sticker` (as a reply).")
        return

    mode = mode.lower().strip()
    if mode == "emoji":
        if not source:
            await ctx.send("Usage: `!steal emoji <:name:id> [new_name]`")
            return

        parts = source.split()
        emoji_ref = _extract_custom_emoji_reference(parts[0])
        if emoji_ref is None:
            await ctx.send("❌ Please provide a valid custom emoji, e.g. `!steal emoji <:wave:1234567890>`.")
            return

        emoji_id, is_animated, fallback_name = emoji_ref
        desired_name = (parts[1] if len(parts) > 1 else fallback_name).strip()
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "", desired_name)[:32] or "stolen_emoji"
        extension = "gif" if is_animated else "png"
        asset_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{extension}?quality=lossless"
        emoji_bytes = await _download_discord_asset(asset_url)
        if not emoji_bytes:
            await ctx.send("❌ I couldn't download that emoji. Make sure it still exists and is accessible.")
            return

        try:
            created = await ctx.guild.create_custom_emoji(
                name=safe_name,
                image=emoji_bytes,
                reason=f"Stolen by {ctx.author} ({ctx.author.id})",
            )
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to manage emojis in this server.")
            return
        except discord.HTTPException as exc:
            await ctx.send(f"❌ Failed to add emoji: {exc}")
            return

        await ctx.send(f"✅ Emoji added: {created} (`:{created.name}:`)")
        return

    if mode == "sticker":
        if not ctx.message.reference or not isinstance(ctx.message.reference.resolved, discord.Message):
            await ctx.send("Reply to a message that contains a sticker, then run `!steal sticker`.")
            return

        referenced_message: discord.Message = ctx.message.reference.resolved
        if not referenced_message.stickers:
            await ctx.send("❌ The replied message doesn't contain a sticker.")
            return

        sticker = referenced_message.stickers[0]
        sticker_url = getattr(sticker, "url", None)
        sticker_name = re.sub(r"[^a-zA-Z0-9_]", "", sticker.name or "stolen_sticker")[:30] or "stolen_sticker"
        sticker_tags = (getattr(sticker, "emoji", None) or "✨").strip() or "✨"

        if not sticker_url:
            await ctx.send("❌ I couldn't resolve a downloadable URL for that sticker.")
            return

        sticker_bytes = await _download_discord_asset(str(sticker_url))
        if not sticker_bytes:
            await ctx.send("❌ I couldn't download that sticker asset.")
            return

        ext = "png"
        if str(sticker_url).endswith(".apng"):
            ext = "apng"
        elif str(sticker_url).endswith(".json"):
            ext = "json"

        try:
            created = await ctx.guild.create_sticker(
                name=sticker_name,
                description=f"Stolen by {ctx.author.display_name}",
                emoji=sticker_tags,
                file=discord.File(io.BytesIO(sticker_bytes), filename=f"{sticker_name}.{ext}"),
                reason=f"Sticker stolen by {ctx.author} ({ctx.author.id})",
            )
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to manage stickers in this server.")
            return
        except discord.HTTPException as exc:
            await ctx.send(f"❌ Failed to add sticker: {exc}")
            return

        await ctx.send(f"✅ Sticker added: `{created.name}`")
        return

    await ctx.send("❌ Invalid option. Use `emoji` or `sticker`.")


async def log_say_message(
    *,
    guild: discord.Guild | None,
    actor: discord.abc.User,
    source_channel: discord.abc.GuildChannel | discord.Thread | None,
    content: str,
    origin: str
):
    if guild is None:
        return

    data = load_attendance_data(guild.id)
    log_channel_id = data.get("say_log_channel_id")
    if not log_channel_id:
        return

    log_channel = guild.get_channel(log_channel_id)
    if not isinstance(log_channel, discord.TextChannel):
        return

    log_embed = discord.Embed(
        title="🗣️ Say Command Log",
        description=(content[:1800] + "…") if len(content) > 1800 else content,
        color=discord.Color.blurple(),
        timestamp=discord.utils.utcnow()
    )
    log_embed.add_field(name="Sender", value=f"{actor} (`{actor.id}`)", inline=False)
    log_embed.add_field(name="Origin", value=origin, inline=True)
    if source_channel is not None:
        log_embed.add_field(name="Source Channel", value=source_channel.mention, inline=True)

    try:
        await log_channel.send(embed=log_embed)
    except discord.HTTPException:
        logger.warning("Failed to post say log for guild %s", guild.id, exc_info=True)


async def log_direct_message_action(
    *,
    guild: discord.Guild | None,
    actor: discord.abc.User,
    source_channel: discord.abc.GuildChannel | discord.Thread | None,
    target_user: discord.abc.User,
    content: str,
    origin: str,
):
    if guild is None:
        return

    data = load_attendance_data(guild.id)
    log_channel_id = data.get("dm_log_channel_id")
    if not log_channel_id:
        return

    log_channel = guild.get_channel(log_channel_id)
    if not isinstance(log_channel, discord.TextChannel):
        return

    log_embed = discord.Embed(
        title="📩 Direct Message Log",
        description=(content[:1800] + "…") if len(content) > 1800 else content,
        color=discord.Color.dark_gold(),
        timestamp=discord.utils.utcnow(),
    )
    log_embed.add_field(name="Sender", value=f"{actor} (`{actor.id}`)", inline=False)
    log_embed.add_field(name="Recipient", value=f"{target_user} (`{target_user.id}`)", inline=False)
    log_embed.add_field(name="Origin", value=origin, inline=True)
    if source_channel is not None:
        log_embed.add_field(name="Source Channel", value=source_channel.mention, inline=True)

    try:
        await log_channel.send(embed=log_embed)
    except discord.HTTPException:
        logger.warning("Failed to post directmessage log for guild %s", guild.id, exc_info=True)


def is_reserved_command_name(command_name: str) -> bool:
    """Return True when a custom command name would collide with a built-in command or alias."""
    return bot.get_command(command_name) is not None


@bot.command(name='addcommand', aliases=['setcommand', 'customcommand'])
@commands.has_permissions(administrator=True)
async def add_custom_command(ctx, command_name: str = None, *, response_text: str = None):
    """
    Creates or updates a custom command for this server.
    Usage: !addcommand rules Be respectful and follow the server rules.
    """
    if command_name is None or response_text is None:
        await ctx.send("Usage: `!addcommand <command> <response>`")
        return

    normalized_name = normalize_custom_command_name(command_name)
    if normalized_name is None:
        await ctx.send("❌ Command names must be a single word, like `rules` or `faq`.")
        return

    if is_reserved_command_name(normalized_name):
        await ctx.send(f"❌ `!{normalized_name}` is already used by a built-in bot command or alias.")
        return

    database.upsert_custom_command(ctx.guild.id, normalized_name, response_text.strip())
    await ctx.send(f"✅ Custom command saved. Members can now use `!{normalized_name}`.")


@bot.command(name='removecommand', aliases=['deletecommand'])
@commands.has_permissions(administrator=True)
async def remove_custom_command(ctx, command_name: str = None):
    """
    Deletes a saved custom command.
    Usage: !removecommand rules
    """
    if command_name is None:
        await ctx.send("Usage: `!removecommand <command>`")
        return

    normalized_name = normalize_custom_command_name(command_name)
    if normalized_name is None:
        await ctx.send("❌ Please provide a valid one-word command name.")
        return

    if database.delete_custom_command(ctx.guild.id, normalized_name):
        await ctx.send(f"🗑️ Removed custom command `!{normalized_name}`.")
    else:
        await ctx.send(f"❌ No custom command named `!{normalized_name}` was found.")


@bot.command(name='listcommands', aliases=['customcommands'])
async def list_custom_commands(ctx):
    """Lists the custom commands configured for this server."""
    commands_map = database.get_custom_commands(ctx.guild.id)
    if not commands_map:
        await ctx.send("No custom commands are configured yet.")
        return

    command_list = ', '.join(f"`!{name}`" for name in commands_map.keys())
    embed = discord.Embed(
        title="Custom Commands",
        description=command_list,
        color=discord.Color.blurple()
    )
    embed.set_footer(text=f"Total custom commands: {len(commands_map)}")
    await ctx.send(embed=embed)

async def check_and_notify_setup_completion(ctx):
    """
    Checks if all critical configuration steps are completed and notifies the user.
    Required: Time Window, Report Channel, Present Role, Absent Role, Excused Role, Permit Role.
    """
    try:
        data = load_attendance_data(ctx.guild.id)
        settings = load_settings(ctx.guild.id)
        
        # Check required fields
        # 1. Time Window (implied by attendance_mode='window' which is set by !settime, 
        #    but we check if start/end times are set properly just in case)
        has_time = settings.get('attendance_mode') == 'window' and settings.get('window_start_time') and settings.get('window_end_time')
        
        # 2. Roles
        has_present = bool(data.get('attendance_role_id'))
        has_absent = bool(data.get('absent_role_id'))
        has_excused = bool(data.get('excused_role_id'))
        has_permit = bool(data.get('allowed_role_id'))
        
        # 3. Channel
        has_channel = bool(data.get('report_channel_id'))
        
        if has_time and has_present and has_absent and has_excused and has_permit and has_channel:
            # Check if we already notified? 
            # For now, we'll just send a nice embed message.
            # To prevent spam on every single command if they re-run them, we could add a flag,
            # but the user asked for a message "after ... then the bot will message me".
            
            embed = discord.Embed(
                title="🎉 Setup Complete!",
                description=(
                    "All systems are go! The bot is fully configured.\n\n"
                    "**Configuration Checklist:**\n"
                    "✅ Time Window Set\n"
                    "✅ Attendance Report Channel Assigned\n"
                    "✅ 'Present' Role Configured\n"
                    "✅ 'Absent' Role Configured\n"
                    "✅ 'Excused' Role Configured\n"
                    "✅ 'Permitted' Role Configured\n\n"
                    "Users with the **Permitted Role** can now use `!present` within the time window to mark their attendance!"
                ),
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
            
    except Exception as e:
        logger.error(f"Error in setup check: {e}")

@bot.command(name='settime')
@commands.has_permissions(administrator=True)
async def set_attendance_time(ctx, *, time_input: str = None):
    """
    Sets the attendance window time.
    Usage: !settime 6am to 11:59pm
    Usage: !settime 08:00 - 17:00
    """
    logger.info(f"Command execution started: settime with input '{time_input}'")
    
    # Normalize input
    if not time_input:
         await ctx.send("❌ Please provide a time range. Usage: `!settime 6am to 11:59pm`")
         return
         
    try:
        raw_input = time_input.lower()
        logger.info(f"Processing time input: '{raw_input}'")
        
        # Try different separators
        parts = []
        if " to " in raw_input:
            parts = raw_input.split(" to ")
        elif "-" in raw_input:
            parts = raw_input.split("-")
        else:
            # Fallback: Split by space
            temp_parts = raw_input.split()
            if len(temp_parts) == 2:
                parts = temp_parts
        
        if len(parts) < 2:
            logger.warning(f"Failed to split time input: {raw_input}")
            await ctx.send("Could not identify start and end time. Please separate them with `to` or `-`. \nExample: `!settime 6am to 11pm`")
            return
    
        start_str = parts[0].strip()
        end_str = parts[1].strip()
    
        # Validate
        s_parsed = parse_time_input(start_str)
        e_parsed = parse_time_input(end_str)
        
        if not s_parsed or not e_parsed:
            logger.warning(f"Failed to parse times: {start_str} -> {s_parsed}, {end_str} -> {e_parsed}")
            await ctx.send(f"Invalid time format (`{start_str}` or `{end_str}`). Please use formats like `6am`, `11:59pm`, `08:00`.")
            return
            
        settings = load_settings(ctx.guild.id)
        settings['attendance_mode'] = 'window'
        settings['window_start_time'] = s_parsed
        settings['window_end_time'] = e_parsed
        
        # Reset the last processed date so we don't accidentally skip today if re-setting
        settings['last_processed_date'] = None
        
        save_settings(ctx.guild.id, settings)
        
        # Convert to 12-hour format for confirmation message
        dt_start = datetime.datetime.strptime(s_parsed, "%H:%M")
        dt_end = datetime.datetime.strptime(e_parsed, "%H:%M")
        display_s = dt_start.strftime("%I:%M %p").lstrip('0')
        display_e = dt_end.strftime("%I:%M %p").lstrip('0')
        
        logger.info(f"Successfully saved settings: {s_parsed} - {e_parsed}")
        await ctx.send(f"✅ Attendance time set to **{display_s} - {display_e}**. Mode switched to 'Window'.")
        
        # Check if allowed_role is set for auto-absence
        data = load_attendance_data(ctx.guild.id)
        if not data.get('allowed_role_id'):
            await ctx.send("⚠️ **Note:** You haven't set a 'Permitted Role' (the role required to attend). \n"
                           "Bot cannot determine who is 'missing' without it. \n"
                           "Please run `!setpermitrole @Role` (e.g., @Student) so the bot knows who should be marked absent if they don't show up.")
        
        await ctx.send(f"Bot will now automatically mark absences and reset attendance after {display_e}.")
        
        # Check setup completion
        await check_and_notify_setup_completion(ctx)
        
    except Exception as e:
        logger.error(f"Critical error in set_attendance_time: {e}", exc_info=True)
        await ctx.send(f"❌ An internal error occurred: {e}")

# --- Attendance Logic ---

_settings_cache = {}

def has_conflicting_attendance_status(records, user_id, target_status):
    """Return the existing attendance status when a user tries to switch states."""
    record = (records or {}).get(str(user_id), {})
    current_status = record.get('status')
    if current_status in ('present', 'absent', 'excused') and current_status != target_status:
        return current_status
    return None


def parse_announcement_channel_ids(raw_value) -> list[int]:
    if not raw_value:
        return []
    if isinstance(raw_value, list):
        return [int(cid) for cid in raw_value if str(cid).isdigit()]
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, list):
                return [int(cid) for cid in parsed if str(cid).isdigit()]
        except json.JSONDecodeError:
            return []
    return []


def parse_staff_role_ids(raw_value) -> list[int]:
    if not raw_value:
        return []
    if isinstance(raw_value, list):
        return [int(rid) for rid in raw_value if str(rid).isdigit()]
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, list):
                return [int(rid) for rid in parsed if str(rid).isdigit()]
        except json.JSONDecodeError:
            return []
    return []


def parse_staff_tracker_role_ids(raw_value) -> list[int]:
    return parse_staff_role_ids(raw_value)


def _staff_tracker_reason_category(reason: str | None) -> str:
    cleaned = (reason or "").strip().lower()
    if cleaned == "":
        return "missing"
    if cleaned in {"test", "none", ".", "-", "n/a", "na"}:
        return "placeholder"
    return "provided"


def _staff_tracker_reason_invalid(reason: str | None) -> bool:
    return _staff_tracker_reason_category(reason) in {"missing", "placeholder"}


def _staff_tracker_timeout_for_strike(strike_count: int) -> datetime.timedelta | None:
    if strike_count >= 8:
        return datetime.timedelta(days=14)
    if strike_count >= 7:
        return datetime.timedelta(days=7)
    if strike_count >= 5:
        return datetime.timedelta(days=1)
    if strike_count >= 3:
        return datetime.timedelta(hours=1)
    return None


def _staff_tracker_next_penalties_text() -> str:
    return (
        "• 5 → 1 day timeout\n"
        "• 7 → 1 week timeout\n"
        "• 8 → 2 weeks mute"
    )

def load_attendance_data(guild_id):
    """Loads attendance data for a specific guild from the database."""
    config = database.get_guild_config(guild_id)
    if not config:
        # Default structure for new guilds
        return {
            "attendance_role_id": None, 
            "absent_role_id": None, 
            "excused_role_id": None, 
            "ping_role_id": None,
            "revive_ping_role_id": None,
            "revive_channel_id": None,
            "confession_channel_id": None,
            "confession_log_channel_id": None,
            "confession_review_channel_id": None,
            "confession_author_channel_id": None,
            "confession_ping_role_id": None,
            "confession_word_filter_enabled": False,
            "confession_cooldown_enabled": False,
            "confession_min_account_age_enabled": False,
            "confess_alias": None,
            "reply_alias": None,
            "confession_header_text": None,
            "reply_header_text": None,
            "confession_embed_footer_text": None,
            "confession_embed_color": None,
            "confession_submit_button_text": "Submit a confession!",
            "confession_reply_button_text": "Reply",
            "say_log_channel_id": None,
            "confession_counter": 0,
            "suggestion_channel_id": None,
            "suggestion_counter": 0,
            "quote_counter": 0,
            "welcome_channel_id": None, 
            "report_channel_id": None,
            "reto_star_channel_id": None,
            "translation_channel_id": None,
            "translation_dual_channel_id": None,
            "translation_enabled": False,
            "announcement_channel_ids": [],
            "staff_attendance_enabled": False,
            "staff_attendance_allowed_role_ids": [],
            "staff_attendance_log_channel_id": None,
            "staff_attendance_channel_id": None,
            "staff_attendance_cooldown_seconds": 300,
            "staff_tracker_enabled": False,
            "staff_tracker_role_ids": [],
            "staff_tracker_exempt_role_ids": [],
            "staff_tracker_log_channel_id": None,
            "staff_tracker_punishment_mode": "timeout",
            "last_report_message_id": None,
            "last_report_channel_id": None,
            "records": {}, 
            "settings": {}
        }
    
    # Reconstruct settings dict
    settings = {
        "attendance_mode": config.get('attendance_mode'),
        "attendance_expiry_hours": config.get('attendance_expiry_hours'),
        "window_start_time": config.get('window_start_time'),
        "window_end_time": config.get('window_end_time'),
        "last_processed_date": config.get('last_processed_date'),
        "last_opened_date": config.get('last_opened_date'),
        "allow_self_marking": bool(config.get('allow_self_marking')),
        "require_admin_excuse": bool(config.get('require_admin_excuse')),
        "auto_nick_on_join": bool(config.get('auto_nick_on_join')),
        "enforce_suffix": bool(config.get('enforce_suffix')),
        "remove_suffix_on_role_loss": bool(config.get('remove_suffix_on_role_loss')),
        "suffix_format": config.get('suffix_format')
    }
    
    records = database.get_attendance_records(guild_id)
    
    return {
        "attendance_role_id": config.get('attendance_role_id'),
        "absent_role_id": config.get('absent_role_id'),
        "excused_role_id": config.get('excused_role_id'),
        "ping_role_id": config.get('ping_role_id'),
        "revive_ping_role_id": config.get('revive_ping_role_id'),
        "revive_channel_id": config.get('revive_channel_id'),
        "confession_channel_id": config.get('confession_channel_id'),
        "confession_log_channel_id": config.get('confession_log_channel_id'),
        "confession_review_channel_id": config.get('confession_review_channel_id'),
        "confession_author_channel_id": config.get('confession_author_channel_id'),
        "confession_ping_role_id": config.get('confession_ping_role_id'),
        "confession_word_filter_enabled": bool(config.get('confession_word_filter_enabled')) if config.get('confession_word_filter_enabled') is not None else False,
        "confession_cooldown_enabled": bool(config.get('confession_cooldown_enabled')) if config.get('confession_cooldown_enabled') is not None else False,
        "confession_min_account_age_enabled": bool(config.get('confession_min_account_age_enabled')) if config.get('confession_min_account_age_enabled') is not None else False,
        "confess_alias": config.get('confess_alias'),
        "reply_alias": config.get('reply_alias'),
        "confession_header_text": config.get('confession_header_text'),
        "reply_header_text": config.get('reply_header_text'),
        "confession_embed_footer_text": config.get('confession_embed_footer_text'),
        "confession_embed_color": config.get('confession_embed_color'),
        "confession_submit_button_text": config.get('confession_submit_button_text') or "Submit a confession!",
        "confession_reply_button_text": config.get('confession_reply_button_text') or "Reply",
        "say_log_channel_id": config.get('say_log_channel_id'),
        "confession_counter": config.get('confession_counter') or 0,
        "suggestion_channel_id": config.get('suggestion_channel_id'),
        "suggestion_counter": config.get('suggestion_counter') or 0,
        "quote_counter": config.get('quote_counter') or 0,
        "welcome_channel_id": config.get('welcome_channel_id'),
        "report_channel_id": config.get('report_channel_id'),
        "reto_star_channel_id": config.get('reto_star_channel_id'),
        "translation_channel_id": config.get('translation_channel_id'),
        "translation_dual_channel_id": config.get('translation_dual_channel_id'),
        "translation_enabled": bool(config.get('translation_enabled')) if config.get('translation_enabled') is not None else False,
        "announcement_channel_ids": parse_announcement_channel_ids(config.get('announcement_channel_ids')),
        "staff_attendance_enabled": bool(config.get('staff_attendance_enabled')) if config.get('staff_attendance_enabled') is not None else False,
        "staff_attendance_allowed_role_ids": parse_staff_role_ids(config.get('staff_attendance_allowed_role_ids')),
        "staff_attendance_log_channel_id": config.get('staff_attendance_log_channel_id'),
        "staff_attendance_channel_id": config.get('staff_attendance_channel_id'),
        "staff_attendance_cooldown_seconds": int(config.get('staff_attendance_cooldown_seconds') or 300),
        "staff_tracker_enabled": bool(config.get('staff_tracker_enabled')) if config.get('staff_tracker_enabled') is not None else False,
        "staff_tracker_role_ids": parse_staff_tracker_role_ids(config.get('staff_tracker_role_ids')),
        "staff_tracker_exempt_role_ids": parse_staff_tracker_role_ids(config.get('staff_tracker_exempt_role_ids')),
        "staff_tracker_log_channel_id": config.get('staff_tracker_log_channel_id'),
        "staff_tracker_punishment_mode": (config.get('staff_tracker_punishment_mode') or "timeout").lower(),
        "last_report_message_id": config.get('last_report_message_id'),
        "last_report_channel_id": config.get('last_report_channel_id'),
        "present_channel_id": config.get('present_channel_id'),
        "records": records,
        "settings": settings
    }

def save_attendance_data(guild_id, guild_data):
    """Saves attendance data for a specific guild to the database."""
    settings = guild_data.get('settings', {})
    existing_config = database.get_guild_config(guild_id) or {}

    config_fields = [
        "attendance_role_id", "absent_role_id", "excused_role_id", "ping_role_id",
        "revive_ping_role_id", "revive_channel_id",
        "confession_channel_id", "confession_log_channel_id", "say_log_channel_id", "confession_counter",
        "confession_review_channel_id", "confession_author_channel_id", "confession_ping_role_id",
        "confession_word_filter_enabled", "confession_cooldown_enabled", "confession_min_account_age_enabled",
        "confess_alias", "reply_alias", "confession_header_text", "reply_header_text",
        "confession_embed_footer_text", "confession_embed_color", "confession_submit_button_text", "confession_reply_button_text",
        "suggestion_channel_id", "suggestion_counter", "quote_counter",
        "welcome_channel_id", "report_channel_id", "reto_star_channel_id", "last_report_message_id",
        "last_report_channel_id", "present_channel_id", "translation_channel_id", "translation_dual_channel_id",
        "translation_enabled", "announcement_channel_ids", "staff_attendance_enabled",
        "staff_attendance_allowed_role_ids", "staff_attendance_log_channel_id", "staff_attendance_channel_id",
        "staff_attendance_cooldown_seconds", "staff_tracker_enabled", "staff_tracker_role_ids",
        "staff_tracker_exempt_role_ids", "staff_tracker_log_channel_id", "staff_tracker_punishment_mode"
    ]
    setting_fields = [
        "attendance_mode", "attendance_expiry_hours", "window_start_time", "window_end_time",
        "last_processed_date", "last_opened_date", "allow_self_marking", "require_admin_excuse",
        "auto_nick_on_join", "enforce_suffix", "remove_suffix_on_role_loss", "suffix_format"
    ]

    config_update = {}

    # Preserve existing values when a key is not included in guild_data/settings,
    # so redeploy/startup writes don't accidentally reset configuration.
    for field in config_fields:
        if field in guild_data:
            config_update[field] = guild_data.get(field)
        elif field in existing_config:
            config_update[field] = existing_config.get(field)

    for field in setting_fields:
        if field in settings:
            config_update[field] = settings.get(field)
        elif field in existing_config:
            config_update[field] = existing_config.get(field)

    if "announcement_channel_ids" in config_update and isinstance(config_update["announcement_channel_ids"], list):
        config_update["announcement_channel_ids"] = json.dumps(config_update["announcement_channel_ids"])
    if "staff_attendance_allowed_role_ids" in config_update and isinstance(config_update["staff_attendance_allowed_role_ids"], list):
        config_update["staff_attendance_allowed_role_ids"] = json.dumps(config_update["staff_attendance_allowed_role_ids"])
    if "staff_tracker_role_ids" in config_update and isinstance(config_update["staff_tracker_role_ids"], list):
        config_update["staff_tracker_role_ids"] = json.dumps(config_update["staff_tracker_role_ids"])
    if "staff_tracker_exempt_role_ids" in config_update and isinstance(config_update["staff_tracker_exempt_role_ids"], list):
        config_update["staff_tracker_exempt_role_ids"] = json.dumps(config_update["staff_tracker_exempt_role_ids"])

    database.update_guild_config(guild_id, **config_update)
    database.replace_all_records(guild_id, guild_data.get('records', {}))

def load_settings(guild_id):
    """Helper to get settings with defaults for a guild (Cached)"""
    # Check cache first
    if guild_id in _settings_cache:
        return _settings_cache[guild_id]

    config = database.get_guild_config(guild_id)
    
    defaults = {
        "debug_mode": False,
        "auto_nick_on_join": False,
        "enforce_suffix": False,
        "remove_suffix_on_role_loss": False,
        "attendance_expiry_hours": 12,
        "allow_self_marking": True,
        "require_admin_excuse": True,
        "suffix_format": " [𝙼𝚂𝚄𝚊𝚗]",
        "attendance_mode": "duration", 
        "window_start_time": "00:00",
        "window_end_time": "23:59",
        "last_processed_date": None,
        "last_opened_date": None
    }
    
    if not config:
        # Cache defaults and return
        _settings_cache[guild_id] = defaults.copy()
        return defaults.copy()
        
    # Map DB fields to settings dict
    settings = {
        "auto_nick_on_join": bool(config.get('auto_nick_on_join', False)),
        "enforce_suffix": bool(config.get('enforce_suffix', False)),
        "remove_suffix_on_role_loss": bool(config.get('remove_suffix_on_role_loss', False)),
        "attendance_expiry_hours": config.get('attendance_expiry_hours', 12),
        "allow_self_marking": bool(config.get('allow_self_marking', True)),
        "require_admin_excuse": bool(config.get('require_admin_excuse', True)),
        "suffix_format": config.get('suffix_format', " [𝙼𝚂𝚄𝚊𝚗]"),
        "attendance_mode": config.get('attendance_mode', 'duration'),
        "window_start_time": config.get('window_start_time', '00:00'),
        "window_end_time": config.get('window_end_time', '23:59'),
        "last_processed_date": config.get('last_processed_date'),
        "last_opened_date": config.get('last_opened_date')
    }
    
    # Merge defaults
    for k, v in defaults.items():
        if k not in settings or settings[k] is None:
            settings[k] = v
            
    # Update cache
    _settings_cache[guild_id] = settings
    return settings

def save_settings(guild_id, settings):
    # Update cache
    _settings_cache[guild_id] = settings
    
    config_update = {
        "attendance_mode": settings.get('attendance_mode'),
        "attendance_expiry_hours": settings.get('attendance_expiry_hours'),
        "window_start_time": settings.get('window_start_time'),
        "window_end_time": settings.get('window_end_time'),
        "last_processed_date": settings.get('last_processed_date'),
        "last_opened_date": settings.get('last_opened_date'),
        "allow_self_marking": settings.get('allow_self_marking'),
        "require_admin_excuse": settings.get('require_admin_excuse'),
        "auto_nick_on_join": settings.get('auto_nick_on_join'),
        "enforce_suffix": settings.get('enforce_suffix'),
        "remove_suffix_on_role_loss": settings.get('remove_suffix_on_role_loss'),
        "suffix_format": settings.get('suffix_format')
    }
    database.update_guild_config(guild_id, **config_update)

# --- Configuration Views ---

class SettingsSelect(discord.ui.Select):
    def __init__(self, bot_instance):
        options = [
            discord.SelectOption(label="System Settings", description="Debug Mode, Sync Commands", emoji="⚙️"),
            discord.SelectOption(label="Auto-Nickname", description="Suffix, Auto-Add, Enforce", emoji="📝"),
            discord.SelectOption(label="Attendance Settings", description="Expiry, Self-Marking, Admin Only", emoji="📅"),
            discord.SelectOption(label="Presence", description="Set Bot Status", emoji="🤖")
        ]
        super().__init__(placeholder="Select a category to configure...", min_values=1, max_values=1, options=options)
        self.bot_instance = bot_instance

    async def callback(self, interaction: discord.Interaction):
        category = self.values[0]
        settings = load_settings(interaction.guild.id)
        
        if category == "System Settings":
            view = SystemSettingsView(interaction.guild.id, settings)
            embed = discord.Embed(title="System Settings", color=discord.Color.blue())
            embed.add_field(name="Debug Mode", value="Enabled" if settings['debug_mode'] else "Disabled")
            await interaction.response.edit_message(embed=embed, view=view)
            
        elif category == "Auto-Nickname":
            view = AutoNickSettingsView(interaction.guild.id, settings)
            embed = discord.Embed(title="Auto-Nickname Configuration", color=discord.Color.green())
            embed.add_field(name="Suffix Format", value=f"`{settings['suffix_format']}`", inline=False)
            embed.add_field(name="Auto-Add on Join", value=str(settings['auto_nick_on_join']))
            embed.add_field(name="Enforce Suffix", value=str(settings['enforce_suffix']))
            embed.add_field(name="Remove on Role Loss", value=str(settings['remove_suffix_on_role_loss']))
            await interaction.response.edit_message(embed=embed, view=view)
            
        elif category == "Attendance Settings":
            view = AttendanceSettingsView(interaction.guild.id, settings)
            embed = discord.Embed(title="Attendance Settings", color=discord.Color.orange())
            embed.add_field(name="Attendance Mode", value=settings['attendance_mode'].title())
            if settings['attendance_mode'] == 'window':
                 embed.add_field(name="Window", value=f"{settings['window_start_time']} - {settings['window_end_time']}")
            else:
                 embed.add_field(name="Auto-Expiry (Hours)", value=str(settings['attendance_expiry_hours']))
            
            embed.add_field(name="Allow Self-Marking", value=str(settings['allow_self_marking']))
            embed.add_field(name="Require Admin for Excuse", value=str(settings['require_admin_excuse']))
            await interaction.response.edit_message(embed=embed, view=view)
            
        elif category == "Presence":
            await interaction.response.send_modal(PresenceModal(self.bot_instance))

class PresenceModal(discord.ui.Modal, title="Set Bot Presence"):
    status_type = discord.ui.TextInput(label="Type (playing, watching, listening)", placeholder="playing")
    status_text = discord.ui.TextInput(label="Status Text", placeholder="Managing Attendance")

    def __init__(self, bot_instance):
        super().__init__()
        self.bot_instance = bot_instance

    async def on_submit(self, interaction: discord.Interaction):
        status_type = (self.status_type.value or "playing").strip().lower()
        status_text = self.status_text.value.strip()
        await set_and_persist_presence(status_type, status_text)
        await interaction.response.send_message(
            f"Presence updated to: {self.status_type.value} {self.status_text.value} (saved until you change/clear it).",
            ephemeral=True
        )

@bot.command(name='status')
async def set_or_view_status(ctx, *, text: str = None):
    """
    View or set the bot status text.
    Usage: !status
    Usage: !status I am sleeping
    Usage: !status clear
    """
    if text is None:
        if ctx.guild:
            lines = _build_lofi_status_lines(ctx.guild)
            lines.append(_build_bot_activity_line())
            await ctx.send("\n".join(lines))
        else:
            await ctx.send(_build_bot_activity_line().replace("🤖 Bot status", "Current bot status"))
        return

    cleaned = text.strip()
    if not ctx.author.guild_permissions.manage_guild:
        await ctx.send("You do not have permission to change the bot status text.")
        return

    if cleaned.lower() in ("clear", "reset", "off"):
        await set_and_persist_presence(None, None)
        await ctx.send("Bot status has been cleared.")
        return

    await set_and_persist_presence("playing", cleaned)
    await ctx.send(f"Bot status updated to: **Playing {cleaned}** (saved until you change/clear it).")


@bot.tree.command(name="status", description="Show 24/7 lofi connection state and current bot status text.")
async def status_slash(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    lines = _build_lofi_status_lines(interaction.guild)
    lines.append(_build_bot_activity_line())

    await interaction.response.send_message("\n".join(lines), ephemeral=True)


def _is_valid_stream_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


async def _resolve_audio_source(query: str) -> tuple[str | None, str | None, str | None]:
    candidate = (query or "").strip()
    if not candidate:
        return None, None, "Query is empty."

    if _is_valid_stream_url(candidate):
        try:
            def _extract():
                with yt_dlp.YoutubeDL(YTDL_FORMAT_OPTIONS) as ydl:
                    return ydl.extract_info(candidate, download=False)
            info = await asyncio.to_thread(_extract)
            if not info:
                return None, None, "Could not read stream metadata."
            if "entries" in info and info["entries"]:
                info = info["entries"][0]
            stream_url = info.get("url")
            title = info.get("title")
            if stream_url:
                return stream_url, title, None
        except Exception:
            # Fallback to direct URL streaming for non-Youtube direct streams.
            pass
        return candidate, None, None

    return DEFAULT_LOFI_STREAM_URL, candidate, None


def _build_track_entry(query: str, user_id: int | None = None, source_url: str | None = None) -> dict:
    title = (query or "").strip()
    return {
        "title": title[:120] if title else "Unknown Track",
        "requested_by": user_id,
        "source_url": (source_url or "").strip() or None,
    }


def _build_lofi_status_lines(guild: discord.Guild) -> list[str]:
    guild_lofi = get_lofi_state(guild.id)
    channel_id = guild_lofi.get("voice_channel_id")
    channel = guild.get_channel(channel_id) if channel_id else None
    voice_client = guild.voice_client
    lofi_enabled = guild_lofi.get("enabled", False)
    lofi_connected = bool(voice_client and voice_client.is_connected())
    lofi_playing = bool(voice_client and voice_client.is_playing())
    stream_url = guild_lofi.get("stream_url") or DEFAULT_LOFI_STREAM_URL
    lines = [
        f"🎵 24/7 Lofi: **{'Enabled' if lofi_enabled else 'Disabled'}**",
        f"🔌 Connected: **{'Yes' if lofi_connected else 'No'}**",
        f"▶️ Playing: **{'Yes' if lofi_playing else 'No'}**",
        f"🔊 Channel: {channel.mention if channel else 'Not configured'}",
        f"📻 Stream: {stream_url}",
    ]
    if guild_lofi.get("last_error"):
        lines.append(f"⚠️ Last lofi error: `{guild_lofi.get('last_error')}`")
    return lines


def _build_bot_activity_line() -> str:
    activity = bot.activity
    if activity and getattr(activity, "name", None):
        activity_type = getattr(activity.type, "name", "playing").replace("_", " ").title()
        return f"🤖 Bot status: **{activity_type} {activity.name}**"
    return "🤖 Bot status: **No custom status set**"


def _queue_display_lines(state: dict) -> list[str]:
    lines = []
    now_playing = state.get("now_playing")
    if isinstance(now_playing, dict):
        lines.append(f"1. {now_playing.get('title', 'Unknown Track')}")
    for index, track in enumerate(state.get("music_queue", []), start=2 if lines else 1):
        title = track.get("title", "Unknown Track") if isinstance(track, dict) else str(track)
        lines.append(f"{index}. {title}")
    return lines

@bot.command(name='join')
async def join_lofi(ctx):
    if not ctx.guild or not isinstance(ctx.author, discord.Member):
        await ctx.send("This command can only be used in a server.")
        return
    if not ctx.author.voice or not ctx.author.voice.channel:
        await ctx.send("Join a voice channel first, then run `!join`.")
        return

    state = get_lofi_state(ctx.guild.id)
    state["enabled"] = True
    state["voice_channel_id"] = ctx.author.voice.channel.id
    state["stream_url"] = state.get("stream_url") or DEFAULT_LOFI_STREAM_URL
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1
    save_lofi_data(ctx.guild.id, state)

    ok, message = await ensure_lofi_connected(ctx.guild.id)
    await ctx.send(f"🎧 {message}" if ok else f"⚠️ {message}")


@bot.tree.command(name="join", description="Join your voice channel and start 24/7 lofi.")
async def join_lofi_slash(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("Join a voice channel first, then run `/join`.", ephemeral=True)
        return

    state = get_lofi_state(interaction.guild.id)
    state["enabled"] = True
    state["voice_channel_id"] = interaction.user.voice.channel.id
    state["stream_url"] = state.get("stream_url") or DEFAULT_LOFI_STREAM_URL
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1
    save_lofi_data(interaction.guild.id, state)

    ok, message = await ensure_lofi_connected(interaction.guild.id)
    await interaction.response.send_message(f"🎧 {message}" if ok else f"⚠️ {message}")


@bot.command(name='playlofi')
async def play_lofi(ctx, *, url: str = None):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return

    state = get_lofi_state(ctx.guild.id)
    state["stream_url"] = (url or DEFAULT_LOFI_STREAM_URL).strip()
    state["enabled"] = True
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1

    if isinstance(ctx.author, discord.Member) and ctx.author.voice and ctx.author.voice.channel:
        state["voice_channel_id"] = ctx.author.voice.channel.id
    elif not state.get("voice_channel_id"):
        await ctx.send("Join a voice channel first (or run `!join`) so I know where to stream.")
        return

    save_lofi_data(ctx.guild.id, state)
    ok, message = await ensure_lofi_connected(ctx.guild.id)
    if ok:
        await ctx.send(f"🎵 Streaming lofi from: {state['stream_url']}")
    else:
        await ctx.send(f"⚠️ Stream URL updated, but I could not connect yet: {message}")


@bot.tree.command(name="playlofi", description="Set default/custom lofi stream URL and start playback.")
@app_commands.describe(url="Optional custom stream URL")
async def play_lofi_slash(interaction: discord.Interaction, url: str = None):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    state = get_lofi_state(interaction.guild.id)
    state["stream_url"] = (url or DEFAULT_LOFI_STREAM_URL).strip()
    state["enabled"] = True
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1

    if isinstance(interaction.user, discord.Member) and interaction.user.voice and interaction.user.voice.channel:
        state["voice_channel_id"] = interaction.user.voice.channel.id
    elif not state.get("voice_channel_id"):
        await interaction.response.send_message(
            "Join a voice channel first (or run `/join`) so I know where to stream.",
            ephemeral=True
        )
        return

    save_lofi_data(interaction.guild.id, state)
    ok, message = await ensure_lofi_connected(interaction.guild.id)
    if ok:
        await interaction.response.send_message(f"🎵 Streaming lofi from: {state['stream_url']}")
    else:
        await interaction.response.send_message(f"⚠️ Stream URL updated, but I could not connect yet: {message}")


@bot.command(name='leave')
async def leave_lofi(ctx):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return

    state = get_lofi_state(ctx.guild.id)
    state["enabled"] = False
    state["now_playing"] = None
    state["music_queue"] = []
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1
    save_lofi_data(ctx.guild.id, state)

    voice_client = ctx.guild.voice_client
    if voice_client and voice_client.is_connected():
        await voice_client.disconnect(force=True)
    await ctx.send("👋 Disconnected and disabled 24/7 lofi mode.")


@bot.tree.command(name="leave", description="Disconnect from voice and disable 24/7 lofi mode.")
async def leave_lofi_slash(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    state = get_lofi_state(interaction.guild.id)
    state["enabled"] = False
    state["now_playing"] = None
    state["music_queue"] = []
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1
    save_lofi_data(interaction.guild.id, state)

    voice_client = interaction.guild.voice_client
    if voice_client and voice_client.is_connected():
        await voice_client.disconnect(force=True)
    await interaction.response.send_message("👋 Disconnected and disabled 24/7 lofi mode.")


@bot.command(name='play')
async def play_music(ctx, *, query: str = ""):
    if not ctx.guild or not isinstance(ctx.author, discord.Member):
        await ctx.send("This command can only be used in a server.")
        return
    if not ctx.author.voice or not ctx.author.voice.channel:
        await ctx.send("❌ You are NOT in a voice channel. Join one first, then run `!play <song name>`.")
        return
    if not query.strip():
        await ctx.send("Use: `!play <song name or stream URL>`")
        return

    state = get_lofi_state(ctx.guild.id)
    state["enabled"] = True
    state["voice_channel_id"] = ctx.author.voice.channel.id
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1

    current_volume = max(0.0, min(2.0, float(state.get("volume", DEFAULT_LOFI_VOLUME))))
    volume_autofixed = current_volume < MIN_AUDIBLE_LOFI_VOLUME
    if volume_autofixed:
        state["volume"] = DEFAULT_LOFI_VOLUME

    query_text = query.strip()
    stream_url, resolved_title, resolve_error = await _resolve_audio_source(query_text)
    if resolve_error or not stream_url:
        await ctx.send(f"❌ Could not play that input: {resolve_error or 'Unknown stream resolution error.'}")
        return

    voice_client = ctx.guild.voice_client
    is_currently_playing = bool(voice_client and voice_client.is_connected() and voice_client.is_playing())
    has_now_playing = isinstance(state.get("now_playing"), dict)
    track_entry = _build_track_entry(resolved_title or query_text, ctx.author.id, source_url=stream_url)

    if is_currently_playing or has_now_playing:
        state.setdefault("music_queue", []).append(track_entry)
        save_lofi_data(ctx.guild.id, state)
        await ctx.send("✅ Added to queue")
        await ctx.send("\n".join(_build_lofi_status_lines(ctx.guild) + [_build_bot_activity_line()]))
        return

    state["now_playing"] = track_entry
    state["stream_url"] = stream_url
    save_lofi_data(ctx.guild.id, state)
    ok, message = await ensure_lofi_connected(ctx.guild.id)
    if not ok:
        await ctx.send(f"⚠️ {message}")
        return
    await ctx.send("🔊 Joining your voice channel...")
    if volume_autofixed:
        await ctx.send("🔊 Volume was too low to hear clearly, so I reset it to 100%.")
    await ctx.send(f"🎶 Now playing: {track_entry['title']}")
    await ctx.send("\n".join(_build_lofi_status_lines(ctx.guild) + [_build_bot_activity_line()]))


@bot.command(name='queue')
async def show_music_queue(ctx):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    state = get_lofi_state(ctx.guild.id)
    queue_lines = _queue_display_lines(state)
    if not queue_lines:
        await ctx.send("📜 Queue is currently empty.")
        return
    await ctx.send("📜 Queue\n" + "\n".join(queue_lines))


@bot.command(name='skip')
async def skip_music(ctx):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    state = get_lofi_state(ctx.guild.id)
    queue = state.get("music_queue", [])
    if queue:
        next_track = queue.pop(0)
        state["now_playing"] = next_track if isinstance(next_track, dict) else {"title": str(next_track), "requested_by": None, "source_url": None}
        next_source_url = state["now_playing"].get("source_url") if isinstance(state["now_playing"], dict) else None
        state["stream_url"] = next_source_url or state.get("stream_url") or DEFAULT_LOFI_STREAM_URL
        state["enabled"] = True
        state["next_retry_at"] = 0.0
        state["backoff_seconds"] = 1
        save_lofi_data(ctx.guild.id, state)
        ok, message = await ensure_lofi_connected(ctx.guild.id)
        if not ok:
            await ctx.send(f"⚠️ {message}")
            return
        await ctx.send("⏭️ Skipped")
        await ctx.send(f"🎶 Now playing: {state['now_playing'].get('title', 'Unknown Track')}")
        return

    state["now_playing"] = None
    state["enabled"] = False
    save_lofi_data(ctx.guild.id, state)
    voice_client = ctx.guild.voice_client
    if voice_client and voice_client.is_playing():
        voice_client.stop()
    await ctx.send("⏭️ Skipped")
    await ctx.send("📭 Queue is empty. Playback stopped.")


@bot.command(name='pause')
async def pause_music(ctx):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    voice_client = ctx.guild.voice_client
    if not voice_client or not voice_client.is_connected() or not voice_client.is_playing():
        await ctx.send("Nothing is currently playing.")
        return
    voice_client.pause()
    await ctx.send("⏸️ Paused")


@bot.command(name='resume')
async def resume_music(ctx):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    voice_client = ctx.guild.voice_client
    if not voice_client or not voice_client.is_connected() or not voice_client.is_paused():
        await ctx.send("Nothing is paused right now.")
        return
    voice_client.resume()
    await ctx.send("▶️ Resumed")


@bot.command(name='volume')
async def set_music_volume(ctx, percent: int = 100):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    clamped = max(0, min(200, int(percent)))
    state = get_lofi_state(ctx.guild.id)
    state["volume"] = clamped / 100.0
    save_lofi_data(ctx.guild.id, state)

    voice_client = ctx.guild.voice_client
    if voice_client and getattr(voice_client, "source", None) and hasattr(voice_client.source, "volume"):
        voice_client.source.volume = state["volume"]

    await ctx.send(f"🔊 Volume set to {clamped}%")


@bot.command(name='stop')
async def stop_music(ctx):
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    state = get_lofi_state(ctx.guild.id)
    state["enabled"] = False
    state["now_playing"] = None
    state["music_queue"] = []
    state["next_retry_at"] = 0.0
    state["backoff_seconds"] = 1
    save_lofi_data(ctx.guild.id, state)

    voice_client = ctx.guild.voice_client
    if voice_client and voice_client.is_playing():
        voice_client.stop()

    await ctx.send("⏹️ Stopped playback")


@bot.tree.command(name="say", description="Make the bot send your message.")
@app_commands.describe(message="Message for the bot to send")
async def say_slash(interaction: discord.Interaction, message: str):
    channel = interaction.channel
    if channel is None:
        await interaction.response.send_message("I couldn't find a channel to post that message.", ephemeral=True)
        return

    await interaction.response.send_message("✅ Sent publicly.", ephemeral=True)
    await channel.send(message)
    await log_say_message(
        guild=interaction.guild,
        actor=interaction.user,
        source_channel=channel,
        content=message,
        origin="Slash (/say)"
    )


@bot.tree.command(name="message", description="Send a rich embed message with title, message text, and optional image.")
@app_commands.describe(
    title="Embed title",
    message="Embed message body",
    image="Optional image URL to show in the embed"
)
async def message_embed_slash(
    interaction: discord.Interaction,
    title: str,
    message: str,
    image: Optional[str] = None
):
    channel = interaction.channel
    guild = interaction.guild
    if channel is None or guild is None:
        await interaction.response.send_message("This command can only be used in a server text channel.", ephemeral=True)
        return

    image_url = None
    if image:
        parsed = urlparse(image.strip())
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            await interaction.response.send_message(
                "Please provide a valid image URL that starts with http:// or https://.",
                ephemeral=True
            )
            return
        image_url = image.strip()

    embed = discord.Embed(
        title=title.strip(),
        description=message,
        color=discord.Color.blurple(),
        timestamp=discord.utils.utcnow()
    )
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    embed.add_field(name="Server", value=guild.name, inline=False)
    if image_url:
        embed.set_image(url=image_url)
    embed.set_footer(text=f"Author ID: {interaction.user.id}")

    await interaction.response.send_message("✅ Embedded message sent.", ephemeral=True)
    await channel.send(embed=embed)

    log_content = f"[EMBED] Title: {title} | Message: {message}"
    if image_url:
        log_content += f" | Image: {image_url}"
    await log_say_message(
        guild=guild,
        actor=interaction.user,
        source_channel=channel,
        content=log_content,
        origin="Slash (/message)"
    )


@bot.tree.command(name="automod", description="Toggle AutoMod (links, spam burst, and bad words) for this server.")
@app_commands.default_permissions(manage_guild=True)
@app_commands.describe(state="Turn AutoMod on or off")
@app_commands.choices(state=[
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
])
async def automod_slash(interaction: discord.Interaction, state: app_commands.Choice[str]):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    enabled = state.value.lower() == "on"
    database.update_guild_config(guild.id, automod_enabled=bool(enabled))
    await interaction.response.send_message(
        f"🛡️ AutoMod is now **{'ON' if enabled else 'OFF'}** for **{guild.name}**.",
        ephemeral=True
    )


@bot.tree.command(name="spamping", description="Spam ping a user in this channel.")
@app_commands.default_permissions(manage_messages=True)
@app_commands.describe(
    user="User to mention repeatedly",
    times="How many mentions to send (1-100)"
)
async def spamping_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    times: app_commands.Range[int, 1, 100] = 5
):
    channel = interaction.channel
    if channel is None:
        await interaction.response.send_message("I couldn't find a channel to send mentions in.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"🚨 Spam-pinging {user.mention} **{times}** time(s) in {channel.mention}.",
        ephemeral=True
    )

    for _ in range(times):
        await channel.send(
            user.mention,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False)
        )
        await asyncio.sleep(0.8)


@bot.tree.command(name="spammessage", description="Send a repeated message burst to a selected channel (admin-only, capped).")
@app_commands.default_permissions(manage_messages=True)
@app_commands.describe(
    channel="Channel where the repeated message should be sent",
    message="Message text to repeat",
    times="How many repeated messages to send (1-25)"
)
async def spammessage_slash(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    message: str,
    times: app_commands.Range[int, 1, 25] = 5
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    bot_member = guild.me or guild.get_member(bot.user.id if bot.user else 0)
    if bot_member is None:
        await interaction.response.send_message("I couldn't validate bot permissions right now. Try again.", ephemeral=True)
        return

    perms = channel.permissions_for(bot_member)
    if not perms.send_messages:
        await interaction.response.send_message(
            f"I can't send messages in {channel.mention}. Please update channel permissions first.",
            ephemeral=True
        )
        return

    if not message.strip():
        await interaction.response.send_message("Please provide a non-empty message to send.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"📣 Sending your message **{times}** time(s) in {channel.mention}.",
        ephemeral=True
    )

    for _ in range(times):
        await channel.send(message)
        await asyncio.sleep(0.7)


@bot.tree.command(name="spamreact", description="React to a user's recent messages in this channel.")
@app_commands.default_permissions(manage_messages=True)
@app_commands.describe(
    user="User whose recent messages should be reacted to",
    count="How many reactions to add (1-10000)",
    emoji="Emoji to use for each reaction"
)
async def spamreact_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    count: app_commands.Range[int, 1, 10000] = 5,
    emoji: str = "🔥"
):
    channel = interaction.channel
    if channel is None or not hasattr(channel, "history"):
        await interaction.response.send_message(
            "I couldn't access message history in this channel.",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    reactions_added = 0
    max_scan = min(max(count * 5, 100), 10000)

    try:
        async for message in channel.history(limit=max_scan):
            if message.author.id != user.id:
                continue
            try:
                await message.add_reaction(emoji)
                reactions_added += 1
            except (discord.Forbidden, discord.HTTPException):
                continue

            if reactions_added >= count:
                break
            await asyncio.sleep(0.25)
    except (discord.Forbidden, discord.HTTPException):
        await interaction.followup.send(
            "I couldn't read enough channel history to complete that request.",
            ephemeral=True
        )
        return

    if reactions_added == 0:
        await interaction.followup.send(
            f"I couldn't find recent messages from {user.mention} to react to in {channel.mention}.",
            ephemeral=True
        )
        return

    if reactions_added < count:
        await interaction.followup.send(
            f"✅ Added **{reactions_added}** reaction(s) with {emoji} to recent messages from {user.mention} "
            f"in {channel.mention}. (Requested: {count})",
            ephemeral=True
        )
        return

    await interaction.followup.send(
        f"✅ Added **{reactions_added}** reaction(s) with {emoji} to recent messages from {user.mention} "
        f"in {channel.mention}.",
        ephemeral=True
    )


@bot.tree.command(name="saylogs", description="Set the channel where /say and !say usage is logged.")
@app_commands.default_permissions(manage_channels=True)
@app_commands.describe(channel="Channel that should receive say command logs")
async def saylogs_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    data = load_attendance_data(guild.id)
    data["say_log_channel_id"] = channel.id
    save_attendance_data(guild.id, data)
    await interaction.response.send_message(
        f"✅ Say logs will now be sent to {channel.mention}.",
        ephemeral=True
    )


@bot.tree.command(name="dmlogs", description="Set the channel where /directmessage and inline direct message usage is logged.")
@app_commands.default_permissions(manage_channels=True)
@app_commands.describe(channel="Channel that should receive direct message logs")
async def dmlogs_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    data = load_attendance_data(guild.id)
    data["dm_log_channel_id"] = channel.id
    save_attendance_data(guild.id, data)
    await interaction.response.send_message(
        f"✅ Direct message logs will now be sent to {channel.mention}.",
        ephemeral=True
    )


@bot.tree.command(name="retostarchannel", description="Set the channel where 0% and 100% reto matches are posted.")
@app_commands.default_permissions(manage_channels=True)
@app_commands.describe(channel="Channel that should receive 0% and 100% reto matches")
async def retostarchannel_slash(interaction: discord.Interaction, channel: discord.TextChannel):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    data = load_attendance_data(guild.id)
    data["reto_star_channel_id"] = channel.id
    save_attendance_data(guild.id, data)
    await interaction.response.send_message(
        f"✅ Extreme reto matches (0% and 100%) will now be posted in {channel.mention}.",
        ephemeral=True
    )


@bot.tree.command(name="draw", description="Share your drawing to this channel.")
@app_commands.describe(caption="Optional short caption for your drawing")
async def draw_slash(interaction: discord.Interaction, caption: str = ""):
    channel = interaction.channel
    if channel is None:
        await interaction.response.send_message("I couldn't find a channel for this command.", ephemeral=True)
        return

    await interaction.response.send_message(
        "🎨 Upload one image in this channel within **3 minutes**. "
        "I will repost it so everyone can see your drawing!",
        ephemeral=True
    )

    def is_valid_drawing_message(message: discord.Message) -> bool:
        if message.author.id != interaction.user.id:
            return False
        if message.channel.id != channel.id:
            return False
        if not message.attachments:
            return False
        return any((attachment.content_type or "").startswith("image/") for attachment in message.attachments)

    try:
        drawing_message = await bot.wait_for("message", check=is_valid_drawing_message, timeout=180)
    except asyncio.TimeoutError:
        await interaction.followup.send("⏰ Timed out. Run `/draw` again when you're ready to share.", ephemeral=True)
        return

    image_attachment = next(
        (attachment for attachment in drawing_message.attachments if (attachment.content_type or "").startswith("image/")),
        None
    )
    if image_attachment is None:
        await interaction.followup.send("❌ I only found non-image files. Please try `/draw` again.", ephemeral=True)
        return

    try:
        uploaded_file = await image_attachment.to_file()
    except Exception:
        await interaction.followup.send("❌ I couldn't download that image. Please try again.", ephemeral=True)
        return

    clean_caption = caption.strip()
    embed = discord.Embed(
        title=f"🎨 New drawing from {interaction.user.display_name}",
        description=clean_caption if clean_caption else "Shared with `/draw`.",
        color=discord.Color.magenta()
    )
    embed.set_image(url=f"attachment://{uploaded_file.filename}")
    embed.set_footer(text=f"Artist: {interaction.user}")
    if interaction.user.display_avatar:
        embed.set_author(name=str(interaction.user), icon_url=interaction.user.display_avatar.url)

    await channel.send(
        content=f"{interaction.user.mention} shared a drawing!",
        embed=embed,
        file=uploaded_file
    )
    await interaction.followup.send("✅ Your drawing has been posted to the channel.", ephemeral=True)


async def _send_announcement_to_channels(
    interaction: discord.Interaction,
    channel_ids: list[int],
    message: str,
    title: str = "Announcement",
    author: Union[str, None] = None,
    include_timestamp: bool = False,
    ping_role: Union[discord.Role, None] = None
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    if len(channel_ids) < 3:
        await interaction.response.send_message(
            "❌ Please provide at least **3 different channels**.",
            ephemeral=True
        )
        return

    unique_channels = []
    seen_channel_ids = set()
    for channel_id in channel_ids:
        if channel_id in seen_channel_ids:
            continue
        seen_channel_ids.add(channel_id)
        channel = interaction.guild.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            unique_channels.append(channel)

    if len(unique_channels) < 3:
        await interaction.response.send_message(
            "❌ Fewer than 3 saved channels are currently available. Re-run `/annoucements` with 3-5 valid channels.",
            ephemeral=True
        )
        return

    sent_mentions = []
    failed_mentions = []
    embed = discord.Embed(
        title=(title.strip() if title and title.strip() else "Announcement"),
        description=message,
        color=discord.Color.blurple()
    )
    if author and author.strip():
        embed.set_author(name=author.strip())
    if include_timestamp:
        embed.timestamp = discord.utils.utcnow()

    for channel in unique_channels:
        try:
            await channel.send(content=ping_role.mention if ping_role else None, embed=embed)
            sent_mentions.append(channel.mention)
        except Exception as e:
            logger.warning("Failed to send announcement in guild %s channel %s: %s", interaction.guild_id, channel.id, e)
            failed_mentions.append(channel.mention)

    confirmation_lines = []
    if sent_mentions:
        confirmation_lines.append("✅ Announcement sent to: " + ", ".join(sent_mentions))
    if failed_mentions:
        confirmation_lines.append("⚠️ Could not send to: " + ", ".join(failed_mentions))

    if not confirmation_lines:
        confirmation_lines.append("❌ I couldn't send the announcement to any selected channel.")

    await interaction.response.send_message("\n".join(confirmation_lines), ephemeral=True)


@bot.tree.command(name="annoucements", description="Broadcast an announcement message to 3-5 channels.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    channel1="First channel (optional, used when saving/updating assignment)",
    channel2="Second channel (optional)",
    channel3="Third channel (optional)",
    channel4="Optional fourth channel",
    channel5="Optional fifth channel",
    title="Announcement title",
    message="Announcement message/body",
    timestamp="Include current timestamp in the embed",
    author="Author shown on the announcement embed",
    assignrole="Optional role to ping with the announcement"
)
async def annoucements_slash(
    interaction: discord.Interaction,
    channel1: Union[discord.TextChannel, None] = None,
    channel2: Union[discord.TextChannel, None] = None,
    channel3: Union[discord.TextChannel, None] = None,
    channel4: Union[discord.TextChannel, None] = None,
    channel5: Union[discord.TextChannel, None] = None,
    title: str = "Announcement",
    message: str = "📢 Announcement",
    timestamp: bool = False,
    author: Union[str, None] = None,
    assignrole: Union[discord.Role, None] = None
):
    data = load_attendance_data(interaction.guild_id)
    selected_channels = [channel1, channel2, channel3, channel4, channel5]
    selected_ids = [ch.id for ch in selected_channels if ch is not None]

    if selected_ids:
        deduped_ids = list(dict.fromkeys(selected_ids))
        if len(deduped_ids) < 3:
            await interaction.response.send_message(
                "❌ Please provide at least **3 different channels** when updating assignments.",
                ephemeral=True
            )
            return
        if len(deduped_ids) > 5:
            await interaction.response.send_message(
                "❌ You can assign at most **5 channels**.",
                ephemeral=True
            )
            return
        data["announcement_channel_ids"] = deduped_ids
        save_attendance_data(interaction.guild_id, data)
        channel_ids = deduped_ids
    else:
        channel_ids = parse_announcement_channel_ids(data.get("announcement_channel_ids"))
        if len(channel_ids) < 3:
            await interaction.response.send_message(
                "❌ No saved announcement channels found.\n"
                "Run `/annoucements #channel #channel #channel` once to save 3-5 channels.",
                ephemeral=True
            )
            return

    send_message = message.strip() if message and message.strip() else "📢 Announcement"
    send_title = title.strip() if title and title.strip() else "Announcement"
    send_author = author.strip() if author and author.strip() else None
    await _send_announcement_to_channels(
        interaction,
        channel_ids,
        send_message,
        send_title,
        send_author,
        timestamp,
        assignrole
    )

class BaseSettingsView(discord.ui.View):
    def __init__(self, guild_id, settings):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.settings = settings

    async def update_message(self, interaction, embed):
        save_settings(self.guild_id, self.settings)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Back to Main Menu", style=discord.ButtonStyle.secondary, row=4)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(embed=discord.Embed(title="Settings Dashboard", description="Select a category below."), view=MainSettingsView(interaction.client))

class SystemSettingsView(BaseSettingsView):
    @discord.ui.button(label="Toggle Debug Mode", style=discord.ButtonStyle.primary)
    async def toggle_debug(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.settings['debug_mode'] = not self.settings['debug_mode']
        
        # Apply logging change immediately
        if self.settings['debug_mode']:
            logger.setLevel(logging.DEBUG)
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            logging.getLogger().setLevel(logging.INFO)
            
        embed = interaction.message.embeds[0]
        embed.set_field_at(0, name="Debug Mode", value="Enabled" if self.settings['debug_mode'] else "Disabled")
        await self.update_message(interaction, embed)

class AutoNickSettingsView(BaseSettingsView):
    @discord.ui.button(label="Toggle Auto-Add on Join", style=discord.ButtonStyle.primary)
    async def toggle_auto_add(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.settings['auto_nick_on_join'] = not self.settings['auto_nick_on_join']
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    @discord.ui.button(label="Toggle Enforce Suffix", style=discord.ButtonStyle.primary)
    async def toggle_enforce(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.settings['enforce_suffix'] = not self.settings['enforce_suffix']
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    @discord.ui.button(label="Toggle Remove on Role Loss", style=discord.ButtonStyle.primary)
    async def toggle_remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.settings['remove_suffix_on_role_loss'] = not self.settings['remove_suffix_on_role_loss']
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    def update_embed(self, embed):
        embed.set_field_at(1, name="Auto-Add on Join", value=str(self.settings['auto_nick_on_join']))
        embed.set_field_at(2, name="Enforce Suffix", value=str(self.settings['enforce_suffix']))
        embed.set_field_at(3, name="Remove on Role Loss", value=str(self.settings['remove_suffix_on_role_loss']))

class TimeWindowModal(discord.ui.Modal, title="Set Time Window"):
    start_time = discord.ui.TextInput(label="Start Time (HH:MM 24h)", placeholder="08:00", min_length=5, max_length=5)
    end_time = discord.ui.TextInput(label="End Time (HH:MM 24h)", placeholder="17:00", min_length=5, max_length=5)

    def __init__(self, view_instance):
        super().__init__()
        self.view_instance = view_instance

    async def on_submit(self, interaction: discord.Interaction):
        # Basic validation
        try:
            datetime.datetime.strptime(self.start_time.value, "%H:%M")
            datetime.datetime.strptime(self.end_time.value, "%H:%M")
        except ValueError:
            await interaction.response.send_message("Invalid time format. Please use HH:MM (e.g., 09:00, 23:59).", ephemeral=True)
            return

        self.view_instance.settings['window_start_time'] = self.start_time.value
        self.view_instance.settings['window_end_time'] = self.end_time.value
        self.view_instance.update_embed(interaction.message.embeds[0])
        await self.view_instance.update_message(interaction, interaction.message.embeds[0])

class AttendanceSettingsView(BaseSettingsView):
    @discord.ui.button(label="Toggle Mode (Duration/Window)", style=discord.ButtonStyle.primary, row=0)
    async def toggle_mode(self, interaction: discord.Interaction, button: discord.ui.Button):
        current = self.settings.get('attendance_mode', 'duration')
        self.settings['attendance_mode'] = 'window' if current == 'duration' else 'duration'
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    @discord.ui.button(label="Set Time Window", style=discord.ButtonStyle.secondary, row=0)
    async def set_window(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.settings.get('attendance_mode') != 'window':
             await interaction.response.send_message("Enable 'Window' mode first.", ephemeral=True)
             return
        await interaction.response.send_modal(TimeWindowModal(self))

    @discord.ui.button(label="Toggle Self-Marking", style=discord.ButtonStyle.primary, row=1)
    async def toggle_self_mark(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.settings['allow_self_marking'] = not self.settings['allow_self_marking']
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    @discord.ui.button(label="Toggle Admin Only Excuse", style=discord.ButtonStyle.primary, row=1)
    async def toggle_admin_excuse(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.settings['require_admin_excuse'] = not self.settings['require_admin_excuse']
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    @discord.ui.select(placeholder="Select Expiry Time (Duration Mode)", options=[
        discord.SelectOption(label="12 Hours", value="12"),
        discord.SelectOption(label="24 Hours", value="24"),
        discord.SelectOption(label="48 Hours", value="48")
    ], row=2)
    async def select_expiry(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.settings['attendance_expiry_hours'] = int(select.values[0])
        self.update_embed(interaction.message.embeds[0])
        await self.update_message(interaction, interaction.message.embeds[0])

    def update_embed(self, embed):
        embed.clear_fields() # Rebuild fields
        
        mode = self.settings.get('attendance_mode', 'duration')
        embed.add_field(name="Attendance Mode", value=mode.title(), inline=False)
        
        if mode == 'window':
             embed.add_field(name="Window", value=f"{self.settings.get('window_start_time', '00:00')} - {self.settings.get('window_end_time', '23:59')}", inline=False)
        else:
             embed.add_field(name="Auto-Expiry (Hours)", value=str(self.settings['attendance_expiry_hours']), inline=False)

        embed.add_field(name="Allow Self-Marking", value=str(self.settings['allow_self_marking']))
        embed.add_field(name="Require Admin for Excuse", value=str(self.settings['require_admin_excuse']))

class MainSettingsView(discord.ui.View):
    def __init__(self, bot_instance):
        super().__init__()
        self.add_item(SettingsSelect(bot_instance))

@bot.command(name='settings', aliases=['panel', 'config'])
@commands.has_permissions(administrator=True)
async def settings_panel(ctx):
    """Opens the interactive settings dashboard."""
    embed = discord.Embed(title="Settings Dashboard", description="Select a category below to configure the bot.", color=discord.Color.blurple())
    await ctx.send(embed=embed, view=MainSettingsView(bot))

@bot.command(name='presentrole', aliases=['assignrole'])
@commands.has_permissions(manage_roles=True)
async def assign_attendance_role(ctx, role: discord.Role):
    """
    Sets the role that users receive when they say 'present'.
    Usage: !presentrole @Role (or !assignrole @Role)
    """
    data = load_attendance_data(ctx.guild.id)
    data['attendance_role_id'] = role.id
    save_attendance_data(ctx.guild.id, data)
    await ctx.send(f"Attendance role has been set to {role.mention}. Users who say 'present' will now receive this role for 12 hours.")
    
    # Check setup completion
    await check_and_notify_setup_completion(ctx)

@bot.command(name='absentrole')
@commands.has_permissions(manage_roles=True)
async def assign_absent_role(ctx, role: discord.Role):
    """
    Sets the role that users receive when marked as absent.
    Usage: !absentrole @Role
    """
    data = load_attendance_data(ctx.guild.id)
    data['absent_role_id'] = role.id
    save_attendance_data(ctx.guild.id, data)
    await ctx.send(f"Absent role has been set to {role.mention}.")
    
    # Check setup completion
    await check_and_notify_setup_completion(ctx)

@bot.command(name='excuserole')
@commands.has_permissions(manage_roles=True)
async def assign_excused_role(ctx, role: discord.Role):
    """
    Sets the role that users receive when marked as excused.
    Usage: !excuserole @Role
    """
    data = load_attendance_data(ctx.guild.id)
    data['excused_role_id'] = role.id
    save_attendance_data(ctx.guild.id, data)
    await ctx.send(f"Excused role has been set to {role.mention}.")
    
    # Check setup completion
    await check_and_notify_setup_completion(ctx)

@bot.command(name='pingrole')
@commands.has_permissions(manage_roles=True)
async def assign_ping_role(ctx, role: discord.Role = None):
    """
    Sets the role that gets pinged when attendance opens/closes.
    Usage: !pingrole @Role
    Usage: !pingrole (to disable ping notifications)
    """
    data = load_attendance_data(ctx.guild.id)
    if role:
        data['ping_role_id'] = role.id
        await ctx.send(f"Ping role has been set to {role.mention}. I will ping this role when attendance opens and closes.")
    else:
        data['ping_role_id'] = None
        await ctx.send("Ping role has been cleared. No role will be pinged for attendance open/close notices.")

    save_attendance_data(ctx.guild.id, data)

@bot.command(name='reviveping')
@commands.has_permissions(manage_roles=True)
async def set_revive_ping_role(ctx, role: discord.Role = None):
    """
    Sets the role used by !revivechat pings.
    Usage: !reviveping @Role
    Usage: !reviveping (to disable revive pings)
    """
    data = load_attendance_data(ctx.guild.id)
    if role:
        data['revive_ping_role_id'] = role.id
        embed = build_revive_style_embed(
            title="Revive Ping Configured",
            body_lines=[
                f"Revive ping role set to {role.mention}.",
                "I will ping this role with !revivechat."
            ],
            guild=ctx.guild,
            requested_by=ctx.author
        )
        await ctx.send(embed=embed)
    else:
        data['revive_ping_role_id'] = None
        embed = build_revive_style_embed(
            title="Revive Ping Disabled",
            body_lines=[
                "Revive ping role cleared.",
                "!revivechat will no longer ping a role."
            ],
            guild=ctx.guild,
            requested_by=ctx.author
        )
        await ctx.send(embed=embed)

    save_attendance_data(ctx.guild.id, data)

@bot.command(name='revivechannel')
@commands.has_permissions(manage_channels=True)
async def set_revive_channel(ctx, channel: discord.TextChannel = None):
    """
    Sets the dedicated channel used by !revivechat and automatic revive pings.
    Usage: !revivechannel #channel
    Usage: !revivechannel (to clear and use fallback routing)
    """
    data = load_attendance_data(ctx.guild.id)
    if channel:
        data['revive_channel_id'] = channel.id
        await ctx.send(f"Revive channel set to {channel.mention}.")
    else:
        data['revive_channel_id'] = None
        await ctx.send("Revive channel cleared. Revive pings will use fallback routing.")

    save_attendance_data(ctx.guild.id, data)

async def send_revive_ping(guild, source_channel):
    """Send revive ping to configured channel or fallback destination."""
    data = load_attendance_data(guild.id)
    revive_ping_role_id = data.get('revive_ping_role_id')
    if not revive_ping_role_id:
        return False, "⚠️ No revive ping role is set. Use `!reviveping @Role` first."

    revive_role = guild.get_role(revive_ping_role_id)
    if not revive_role:
        return False, "⚠️ The saved revive role no longer exists. Please run `!reviveping @Role` again."

    announce_channel = None
    revive_channel_id = data.get('revive_channel_id')
    if revive_channel_id:
        announce_channel = guild.get_channel(revive_channel_id)

    if not announce_channel:
        announce_channel = source_channel

    if not announce_channel and data.get('report_channel_id'):
        announce_channel = guild.get_channel(data.get('report_channel_id'))
    if not announce_channel and data.get('welcome_channel_id'):
        announce_channel = guild.get_channel(data.get('welcome_channel_id'))
    if not announce_channel:
        announce_channel = guild.system_channel

    if not announce_channel:
        return False, "⚠️ I couldn't find a valid channel for revive pings. Set one with `!revivechannel #channel`."

    embed = build_revive_style_embed(
        title="Revive Chat",
        body_lines=[
            f"{revive_role.mention}",
            "Let's revive the chat with !revivechat."
        ],
        guild=guild
    )
    await announce_channel.send(content=revive_role.mention, embed=embed)
    return True, None

@bot.command(name='revivechat', aliases=['revive'])
@commands.has_permissions(manage_messages=True)
async def revive_chat(ctx):
    """
    Sends a chat revival ping using the role configured via !reviveping.
    Usage: !revivechat
    """
    ok, response = await send_revive_ping(ctx.guild, ctx.channel)
    if not ok and response:
        await ctx.send(response)


def _build_confession_config_embed(
    data: dict,
    confession_channel: Union[discord.TextChannel, None],
    log_channel: Union[discord.TextChannel, None],
    review_channel: Union[discord.TextChannel, None],
    author_channel: Union[discord.TextChannel, None],
    moderator_role: Union[discord.Role, None]
) -> discord.Embed:
    embed = discord.Embed(
        title="✅ Confession Setup Complete",
        description="Confession channels and defaults are now configured.",
        color=discord.Color.dark_teal()
    )
    embed.add_field(
        name="📌 Channels",
        value=(
            f"💬 Confession Channel: {confession_channel.mention if confession_channel else 'Not configured'}\n"
            f"📄 Log Channel: {log_channel.mention if log_channel else 'Not configured'}\n"
            f"👀 Review Channel: {review_channel.mention if review_channel else 'Not configured'}\n"
            f"👤 Author Channel: {author_channel.mention if author_channel else 'Not configured'} (optional / system tracking channel)"
        ),
        inline=False
    )
    embed.add_field(
        name="🔔 Misc Settings",
        value=(
            f"🔔 Ping Role: {moderator_role.mention if moderator_role else 'Not configured'}\n"
            f"🔇 Word Filter: {'Enable' if data.get('confession_word_filter_enabled') else 'Disable'}\n"
            f"❄️ Cooldown: {'Enable' if data.get('confession_cooldown_enabled') else 'Disable'}\n"
            f"👶 Minimum Account Age: {'Enable' if data.get('confession_min_account_age_enabled') else 'Disable'}"
        ),
        inline=False
    )
    embed.add_field(
        name="📚 Aliases",
        value=(
            f"/confess: {data.get('confess_alias') or 'custom alias'}\n"
            f"/reply: {data.get('reply_alias') or 'custom alias'}"
        ),
        inline=False
    )
    embed.add_field(
        name="🎨 Embed Customization",
        value=(
            f"📜 Confession Header: {data.get('confession_header_text') or 'custom text'}\n"
            f"📜 Reply Header: {data.get('reply_header_text') or 'custom text'}\n"
            f"📜 Embed Footer: {data.get('confession_embed_footer_text') or 'custom text'}\n"
            f"🎨 Custom Color: {data.get('confession_embed_color') or '#HEX color or preset'}"
        ),
        inline=False
    )
    embed.add_field(
        name="⚫ In-Channel Buttons",
        value=(
            f"🟢 Submit Button Text: {data.get('confession_submit_button_text') or 'Submit a confession!'}\n"
            f"⚫ Reply Button Text: {data.get('confession_reply_button_text') or 'Reply'}"
        ),
        inline=False
    )
    embed.add_field(
        name="🧠 How this works in your bot",
        value=(
            "💬 **Confession Channel**\nWhere anonymous confessions are posted.\n\n"
            "📄 **Log Channel**\nStores confession content, timestamps, and moderation actions.\n\n"
            "👀 **Review Channel**\nUsed for moderation review and reported confessions.\n\n"
            "👤 **Author Channel**\nAdmin-only tracking for sender mapping and audits."
        ),
        inline=False
    )
    embed.set_footer(text="Users can now post with !confess, /confess, !reply, or /reply.")
    return embed

async def _apply_confession_configuration(
    guild_id: int,
    confession_channel: discord.TextChannel,
    log_channel: Union[discord.TextChannel, None] = None,
    review_channel: Union[discord.TextChannel, None] = None,
    author_channel: Union[discord.TextChannel, None] = None,
    moderator_role: Union[discord.Role, None] = None
) -> discord.Embed:
    data = load_attendance_data(guild_id)
    data['confession_channel_id'] = confession_channel.id
    data['confession_log_channel_id'] = log_channel.id if log_channel else None
    data['confession_review_channel_id'] = review_channel.id if review_channel else None
    data['confession_author_channel_id'] = author_channel.id if author_channel else None
    data['confession_ping_role_id'] = moderator_role.id if moderator_role else None
    data['confession_submit_button_text'] = data.get('confession_submit_button_text') or "Submit a confession!"
    data['confession_reply_button_text'] = data.get('confession_reply_button_text') or "Reply"
    data['confession_counter'] = data.get('confession_counter', 0)
    save_attendance_data(guild_id, data)
    return _build_confession_config_embed(
        data=data,
        confession_channel=confession_channel,
        log_channel=log_channel,
        review_channel=review_channel,
        author_channel=author_channel,
        moderator_role=moderator_role
    )


@bot.hybrid_command(name='setupconfession', with_app_command=True, description="Setup confession channels (optionally choose channels).")
@app_commands.describe(
    confession_channel="Channel where anonymous confessions are posted.",
    log_channel="Channel used for confession logs.",
    author_channel="Channel used for confession author mapping."
)
@commands.has_permissions(manage_channels=True)
async def setup_confession(
    ctx,
    confession_channel: Union[discord.TextChannel, None] = None,
    log_channel: Union[discord.TextChannel, None] = None,
    author_channel: Union[discord.TextChannel, None] = None
):
    """
    Setup anonymous confession channels.
    Usage: !setupconfession [#confession] [#confession-logs] [#confession-authors]
    Slash usage: /setupconfession (optional channel picks)
    """
    guild = ctx.guild
    if guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return

    confession_channel = confession_channel or discord.utils.get(guild.text_channels, name="confession")
    log_channel = log_channel or discord.utils.get(guild.text_channels, name="confession-logs")
    review_channel = discord.utils.get(guild.text_channels, name="confession-review")
    author_channel = author_channel or discord.utils.get(guild.text_channels, name="confession-authors")
    moderator_role = discord.utils.get(guild.roles, name="Confession Moderator")

    try:
        if confession_channel is None:
            confession_channel = await guild.create_text_channel("confession", reason=f"Created by {ctx.author} via setupconfession")
        if log_channel is None:
            log_channel = await guild.create_text_channel("confession-logs", reason=f"Created by {ctx.author} via setupconfession")
        if review_channel is None:
            review_channel = await guild.create_text_channel("confession-review", reason=f"Created by {ctx.author} via setupconfession")
        if author_channel is None:
            author_channel = await guild.create_text_channel("confession-authors", reason=f"Created by {ctx.author} via setupconfession")
        if moderator_role is None:
            moderator_role = await guild.create_role(name="Confession Moderator", mentionable=True, reason=f"Created by {ctx.author} via setupconfession")
    except discord.Forbidden:
        await ctx.send("❌ I need Manage Channels and Manage Roles permissions to run setup.")
        return

    embed = await _apply_confession_configuration(
        guild_id=guild.id,
        confession_channel=confession_channel,
        log_channel=log_channel,
        review_channel=review_channel,
        author_channel=author_channel,
        moderator_role=moderator_role
    )
    await ctx.send(embed=embed)


@bot.tree.command(name="confession", description="Setup and save confession channel settings.")
@app_commands.default_permissions(manage_channels=True)
@app_commands.describe(
    confession_channel="Channel where anonymous confessions are posted.",
    log_channel="Channel used for confession logs.",
    author_channel="Channel used for confession author mapping."
)
async def confession_setup_slash(
    interaction: discord.Interaction,
    confession_channel: Union[discord.TextChannel, None] = None,
    log_channel: Union[discord.TextChannel, None] = None,
    author_channel: Union[discord.TextChannel, None] = None
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    confession_channel = confession_channel or discord.utils.get(guild.text_channels, name="confession")
    log_channel = log_channel or discord.utils.get(guild.text_channels, name="confession-logs")
    review_channel = discord.utils.get(guild.text_channels, name="confession-review")
    author_channel = author_channel or discord.utils.get(guild.text_channels, name="confession-authors")
    moderator_role = discord.utils.get(guild.roles, name="Confession Moderator")

    try:
        if confession_channel is None:
            confession_channel = await guild.create_text_channel("confession", reason=f"Created by {interaction.user} via /confession")
        if log_channel is None:
            log_channel = await guild.create_text_channel("confession-logs", reason=f"Created by {interaction.user} via /confession")
        if review_channel is None:
            review_channel = await guild.create_text_channel("confession-review", reason=f"Created by {interaction.user} via /confession")
        if author_channel is None:
            author_channel = await guild.create_text_channel("confession-authors", reason=f"Created by {interaction.user} via /confession")
        if moderator_role is None:
            moderator_role = await guild.create_role(name="Confession Moderator", mentionable=True, reason=f"Created by {interaction.user} via /confession")
    except discord.Forbidden:
        await interaction.response.send_message("❌ I need Manage Channels and Manage Roles permissions to save confession setup.", ephemeral=True)
        return

    embed = await _apply_confession_configuration(
        guild_id=guild.id,
        confession_channel=confession_channel,
        log_channel=log_channel,
        review_channel=review_channel,
        author_channel=author_channel,
        moderator_role=moderator_role
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="configconfession", description="Show confession configuration summary.")
@app_commands.default_permissions(manage_channels=True)
async def config_confession_slash(
    interaction: discord.Interaction
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    data = load_attendance_data(guild.id)
    confession_channel = guild.get_channel(data.get('confession_channel_id'))
    log_channel = guild.get_channel(data.get('confession_log_channel_id'))
    review_channel = guild.get_channel(data.get('confession_review_channel_id'))
    author_channel = guild.get_channel(data.get('confession_author_channel_id'))
    moderator_role = guild.get_role(data.get('confession_ping_role_id'))

    embed = _build_confession_config_embed(
        data=data,
        confession_channel=confession_channel,
        log_channel=log_channel,
        review_channel=review_channel,
        author_channel=author_channel,
        moderator_role=moderator_role
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

async def _post_suggestion_panel(guild: discord.Guild, channel: discord.TextChannel) -> discord.Message:
    embed = discord.Embed(
        title="💡 Suggestions",
        description=(
            "Have an idea to improve the server?\n"
            "Click **Submit Suggestion** below.\n\n"
            "Regular messages are disabled in this channel to keep it clean."
        ),
        color=discord.Color.blurple(),
        timestamp=discord.utils.utcnow()
    )
    embed.set_footer(text="Use the button below to submit suggestions.")
    return await channel.send(embed=embed, view=SuggestionActionView())


class SetupFullServerTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Create Ticket",
        style=discord.ButtonStyle.primary,
        custom_id="setupfullserver:create_ticket"
    )
    async def create_ticket_button(self, interaction: discord.Interaction, _button: discord.ui.Button):
        guild = interaction.guild
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if guild is None or member is None:
            await interaction.response.send_message("❌ This button only works in a server.", ephemeral=True)
            return

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "", member.name.lower())[:20] or str(member.id)
        channel_name = f"ticket-{safe_name}"
        existing = discord.utils.get(guild.text_channels, name=channel_name)
        if existing:
            await interaction.response.send_message(f"🎫 You already have an open ticket: {existing.mention}", ephemeral=True)
            return

        support_role = discord.utils.get(guild.roles, name="Moderator")
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True
            ),
        }
        if support_role:
            overwrites[support_role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_messages=True
            )

        try:
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                overwrites=overwrites,
                reason=f"Ticket created by {member} via /setupfullserver panel"
            )
        except discord.Forbidden:
            await interaction.response.send_message("❌ I need **Manage Channels** to create tickets.", ephemeral=True)
            return
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to create ticket: {e}", ephemeral=True)
            return

        ticket_embed = discord.Embed(
            title="🎫 Support Ticket",
            description=f"{member.mention}, a team member will be with you shortly.",
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        await ticket_channel.send(embed=ticket_embed)
        await interaction.response.send_message(f"✅ Ticket created: {ticket_channel.mention}", ephemeral=True)


class SetupFullServerRoleView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _toggle_role(self, interaction: discord.Interaction, role_name: str):
        guild = interaction.guild
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if guild is None or member is None:
            await interaction.response.send_message("❌ This button only works in a server.", ephemeral=True)
            return

        role = discord.utils.get(guild.roles, name=role_name)
        if role is None:
            await interaction.response.send_message(f"❌ `{role_name}` role does not exist yet.", ephemeral=True)
            return

        try:
            if role in member.roles:
                await member.remove_roles(role, reason=f"Self-role toggle via /setupfullserver ({role_name})")
                await interaction.response.send_message(f"➖ Removed **{role_name}** role.", ephemeral=True)
            else:
                await member.add_roles(role, reason=f"Self-role toggle via /setupfullserver ({role_name})")
                await interaction.response.send_message(f"➕ Added **{role_name}** role.", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(
                f"❌ I need **Manage Roles** and a higher role position to toggle `{role_name}`.",
                ephemeral=True
            )

    @discord.ui.button(label="Get Member Role", style=discord.ButtonStyle.success, custom_id="setupfullserver:role_member")
    async def role_member_button(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self._toggle_role(interaction, "Member")

    @discord.ui.button(label="Gamer Role", style=discord.ButtonStyle.primary, custom_id="setupfullserver:role_gamer")
    async def role_gamer_button(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self._toggle_role(interaction, "Gamer")


@bot.tree.command(name="setupfullserver", description="Create a full modular setup: tickets, roles, logs, and core channels.")
@app_commands.default_permissions(administrator=True)
async def setupfullserver_slash(interaction: discord.Interaction):
    guild = interaction.guild
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if guild is None or member is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return
    if not member.guild_permissions.administrator:
        await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    created_channels = []
    created_roles = []
    channel_names = ["create-ticket", "roles", "audit-logs", "welcome"]

    try:
        for name in channel_names:
            existing = discord.utils.get(guild.text_channels, name=name)
            if existing is None:
                created = await guild.create_text_channel(
                    name=name,
                    reason=f"Created by {member} via /setupfullserver"
                )
                created_channels.append(created.mention)

        for role_name in ("Member", "Gamer"):
            existing = discord.utils.get(guild.roles, name=role_name)
            if existing is None:
                await guild.create_role(
                    name=role_name,
                    mentionable=True,
                    reason=f"Created by {member} via /setupfullserver"
                )
                created_roles.append(role_name)
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I need **Manage Channels** and **Manage Roles** to complete `/setupfullserver`.",
            ephemeral=True
        )
        return
    except Exception as e:
        await interaction.followup.send(f"❌ Setup failed: {e}", ephemeral=True)
        return

    ticket_channel = discord.utils.get(guild.text_channels, name="create-ticket")
    roles_channel = discord.utils.get(guild.text_channels, name="roles")

    if ticket_channel:
        panel_embed = discord.Embed(
            title="🎫 Support Tickets",
            description="Click the button below to create a private support ticket.",
            color=discord.Color.blue(),
        )
        await ticket_channel.send(embed=panel_embed, view=SetupFullServerTicketView())

    if roles_channel:
        await roles_channel.send("🎭 Choose your roles:", view=SetupFullServerRoleView())

    data = load_attendance_data(guild.id)
    data["setupfullserver_enabled"] = True
    data["setupfullserver_channels"] = channel_names
    save_attendance_data(guild.id, data)

    summary_lines = ["✅ `/setupfullserver` completed."]
    summary_lines.append(
        f"Channels created: {', '.join(created_channels) if created_channels else 'none (already existed)'}"
    )
    summary_lines.append(
        f"Roles created: {', '.join(created_roles) if created_roles else 'none (already existed)'}"
    )
    summary_lines.append("Ticket + reaction-role panels posted.")
    await interaction.followup.send("\n".join(summary_lines), ephemeral=True)


@bot.tree.command(name="setupsuggestion", description="Create/configure a suggestion channel with a submit button.")
@app_commands.default_permissions(manage_channels=True)
async def setupsuggestion_slash(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    everyone = guild.default_role
    overwrites = {
        everyone: discord.PermissionOverwrite(
            view_channel=True,
            read_message_history=True,
            send_messages=False,
            send_messages_in_threads=False,
            create_public_threads=False,
            create_private_threads=False
        ),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            read_message_history=True,
            send_messages=True,
            manage_messages=True,
            embed_links=True
        )
    }

    suggestion_channel = discord.utils.get(guild.text_channels, name="suggestions")
    created = False
    try:
        if suggestion_channel is None:
            suggestion_channel = await guild.create_text_channel(
                name="suggestions",
                overwrites=overwrites,
                reason=f"Suggestion channel created by {interaction.user} via /setupsuggestion"
            )
            created = True
        else:
            await suggestion_channel.edit(
                overwrites=overwrites,
                reason=f"Suggestion permissions updated by {interaction.user} via /setupsuggestion"
            )

        panel_message = await _post_suggestion_panel(guild, suggestion_channel)
    except discord.Forbidden:
        await interaction.response.send_message(
            "❌ I need **Manage Channels** and **Send Messages** permissions to set up suggestions.",
            ephemeral=True
        )
        return
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to set up suggestions: {e}", ephemeral=True)
        return

    data = load_attendance_data(guild.id)
    data['suggestion_channel_id'] = suggestion_channel.id
    data['suggestion_counter'] = data.get('suggestion_counter', 0)
    save_attendance_data(guild.id, data)

    action = "created" if created else "updated"
    await interaction.response.send_message(
        (
            f"✅ Suggestion channel {action}: {suggestion_channel.mention}\n"
            f"Members cannot type there, but can submit through the button.\n"
            f"Panel message: [jump to message]({panel_message.jump_url})"
        ),
        ephemeral=True
    )


@bot.tree.command(name="setuplogs", description="Create a full server logging channel system.")
@app_commands.default_permissions(administrator=True)
async def setuplogs_slash(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    saved = load_log_setup_data()
    existing_record = saved.get(str(guild.id))
    if isinstance(existing_record, dict):
        category_id = existing_record.get("categoryId")
        if category_id and guild.get_channel(int(category_id)):
            await interaction.followup.send("⚠️ Logs are already set up for this server.", ephemeral=True)
            return

    category_name = "────SV LOGS────"
    category = discord.utils.get(guild.categories, name=category_name)
    created_category = False
    if category is None:
        try:
            category = await guild.create_category_channel(
                category_name,
                reason=f"Log system setup by {interaction.user} via /setuplogs"
            )
            created_category = True
        except discord.Forbidden:
            await interaction.followup.send("❌ I need **Manage Channels** to set up logs.", ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(f"❌ Failed creating logs category: {e}", ephemeral=True)
            return

    logs = [{"name": f"🧰・{slug}", "key": key} for key, slug, _ in SERVER_LOG_TYPES]

    guild_logs = {}
    created_channels = []
    everyone = guild.default_role
    bot_member = guild.me

    for log in logs:
        channel = discord.utils.get(guild.text_channels, name=log["name"])
        if channel is None:
            overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=False, send_messages=False),
            }
            if bot_member is not None:
                overwrites[bot_member] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    manage_messages=True,
                    read_message_history=True
                )
            try:
                channel = await guild.create_text_channel(
                    name=log["name"],
                    category=category,
                    overwrites=overwrites,
                    reason=f"Log channel setup by {interaction.user} via /setuplogs"
                )
                created_channels.append(channel.mention)
            except discord.Forbidden:
                await interaction.followup.send("❌ I need **Manage Channels** and **Manage Roles** to configure log permissions.", ephemeral=True)
                return
            except Exception as e:
                await interaction.followup.send(f"❌ Failed creating `{log['name']}`: {e}", ephemeral=True)
                return
        elif channel.category_id != category.id:
            try:
                await channel.edit(
                    category=category,
                    reason=f"Log channel re-grouped by {interaction.user} via /setuplogs"
                )
            except Exception:
                pass
        guild_logs[log["key"]] = channel.id

    saved[str(guild.id)] = {
        "categoryId": category.id,
        "logs": guild_logs,
        "updatedAt": _iso_now(),
    }
    save_log_setup_data(saved)

    status_title = "✅ FULL LOG SYSTEM INSTALLED"
    description = "All logging channels have been created successfully."
    if not created_channels and not created_category:
        status_title = "✅ FULL LOG SYSTEM SYNCHRONIZED"
        description = "Existing log channels were detected, grouped, and saved to persistent storage."

    embed = discord.Embed(
        title=status_title,
        description=description,
        color=discord.Color.green(),
        timestamp=discord.utils.utcnow()
    )
    embed.add_field(name="📁 Category", value=category.mention if category else category_name, inline=False)
    embed.add_field(
        name="📌 Logs Ready",
        value="\n".join([f"#{entry['name']}" for entry in logs]),
        inline=False
    )
    if created_channels:
        embed.add_field(
            name="🆕 Newly Created",
            value="\n".join(created_channels[:10]) + ("\n..." if len(created_channels) > 10 else ""),
            inline=False
        )
    embed.set_footer(text="System is now fully active 🔒")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="serverlogs", description="Assign a specific log type to a channel.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    log_type="Which log should be routed",
    channel="Channel that should receive this log",
)
@app_commands.choices(
    log_type=[
        app_commands.Choice(name=slug, value=key)
        for key, slug, _ in SERVER_LOG_TYPES
    ]
)
async def serverlogs_slash(
    interaction: discord.Interaction,
    log_type: app_commands.Choice[str],
    channel: discord.TextChannel,
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        return

    saved = load_log_setup_data()
    guild_payload = saved.get(str(guild.id))
    if not isinstance(guild_payload, dict):
        guild_payload = {}
    logs = guild_payload.get("logs")
    if not isinstance(logs, dict):
        logs = {}

    logs[log_type.value] = channel.id
    guild_payload["logs"] = logs
    guild_payload["updatedAt"] = _iso_now()
    saved[str(guild.id)] = guild_payload
    save_log_setup_data(saved)

    await interaction.response.send_message(
        f"✅ `{log_type.name}` will now be sent to {channel.mention}.",
        ephemeral=True,
    )

class ConfessionActionView(discord.ui.View):
    def __init__(self, submit_label: str = "Submit a confession!", reply_label: str = "Reply"):
        super().__init__(timeout=None)
        self.submit_confession.label = submit_label[:80] if submit_label else "Submit a confession!"
        self.reply_confession.label = reply_label[:80] if reply_label else "Reply"

    @discord.ui.button(label="Submit a confession!", style=discord.ButtonStyle.primary, custom_id="confession_submit_button")
    async def submit_confession(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ConfessionModal())

    @discord.ui.button(label="Reply", style=discord.ButtonStyle.secondary, custom_id="confession_reply_button")
    async def reply_confession(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReplyModal())


class ConfessionModal(discord.ui.Modal, title="Anonymous Confession"):
    confession_text = discord.ui.TextInput(
        label="Your confession",
        style=discord.TextStyle.paragraph,
        max_length=1800,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        ok = await post_confession_message(
            interaction=interaction,
            message_text=str(self.confession_text),
            source_author=interaction.user,
            send_success_response=True
        )
        if not ok and not interaction.response.is_done():
            await interaction.response.send_message("❌ Something went wrong. Try again.", ephemeral=True)


class ReplyModal(discord.ui.Modal, title="Anonymous Reply"):
    reply_text = discord.ui.TextInput(
        label="Your anonymous reply",
        style=discord.TextStyle.paragraph,
        max_length=1800,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        ok = await post_confession_message(
            interaction=interaction,
            message_text=str(self.reply_text),
            source_author=interaction.user,
            is_reply=True,
            send_success_response=True
        )
        if not ok and not interaction.response.is_done():
            await interaction.response.send_message("❌ Something went wrong. Try again.", ephemeral=True)


class SuggestionActionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Submit Suggestion", style=discord.ButtonStyle.success, custom_id="suggestion_submit_button", emoji="💡")
    async def submit_suggestion(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SuggestionModal())


class SuggestionModal(discord.ui.Modal, title="Submit a Suggestion"):
    suggestion_text = discord.ui.TextInput(
        label="Your suggestion",
        style=discord.TextStyle.paragraph,
        max_length=1800,
        required=True,
        placeholder="Describe your idea..."
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Suggestions can only be submitted inside a server.", ephemeral=True)
            return

        data = load_attendance_data(guild.id)
        suggestion_channel_id = data.get('suggestion_channel_id')
        suggestion_channel = guild.get_channel(suggestion_channel_id) if suggestion_channel_id else None
        if suggestion_channel is None:
            await interaction.response.send_message(
                "⚠️ Suggestion channel is not configured yet. Ask an admin to run `/setupsuggestion`.",
                ephemeral=True
            )
            return

        suggestion_number = (data.get('suggestion_counter') or 0) + 1
        data['suggestion_counter'] = suggestion_number
        save_attendance_data(guild.id, data)

        suggestion_embed = discord.Embed(
            title=f"Suggestion #{suggestion_number}",
            description=str(self.suggestion_text).strip(),
            color=discord.Color.green(),
            timestamp=discord.utils.utcnow()
        )
        suggestion_embed.set_footer(text=f"Submitted by {interaction.user}")

        await suggestion_channel.send(embed=suggestion_embed, view=SuggestionActionView())
        await interaction.response.send_message("✅ Suggestion submitted. Thank you!", ephemeral=True)


SERVER_TEMPLATE = [
    {"category": "────STARBOARD────", "text": ["🌟・starboard", "📖・bible-verse"], "voice": []},
    {"category": "────EVENT & TOURNAMENT────", "text": ["🍟・upcoming-events", "🍟・activities", "🍟・event-convo"], "voice": []},
    {"category": "────VERIFICATION────", "text": ["✅・verify-here"], "voice": []},
    {"category": "────PINNED────", "text": ["☑️・rules", "📢・announcements", "🎂・birthdays", "👾・level-up", "🔗・invite-tracker", "🚩・server-warning", "🔒・server-banned"], "voice": []},
    {"category": "────WELCOMING────", "text": ["🎉・welcome", "🕊️・left", "🎙️・introduction", "🛂・roles", "🎂・set-bday", "🚀・bump-here"], "voice": []},
    {"category": "────SERVER INFO────", "text": ["ⓘ・server-info", "🔰・partnership-guide", "🧿・portal"], "voice": []},
    {"category": "────INQUIRIES────", "text": ["📒・create-ticket", "📩・apply-here", "💡・suggestion-box", "🖍️・nick-req"], "voice": []},
    {"category": "────STUDENT CONNECT────", "text": ["🔻・convos", "🔻・english", "🔻・tagalog", "🔻・gaming"], "voice": []},
    {"category": "────ACADEMIC HELP────", "text": ["📚・scholarship", "📚・academic-help", "📚・job-posting", "📚・thesis｜survey", "📚・discussion", "📚・notes-share"], "voice": []},
    {"category": "────RANT & CONFESS────", "text": ["❤️・confession", "❤️・yearn", "💢・rants", "⛔・spam"], "voice": []},
    {"category": "────VIP LOUNGE────", "text": ["💎・boost", "💎・sponsor", "💎・vip-chat", "💎・bot-cmnds"], "voice": ["💎｜VIP"]},
    {"category": "────SOCIALS────", "text": ["✨・memories", "👑・shop", "🌀・promote-socials", "🌀・selfies", "🌀・dump-photos", "🌀・cats-dogs", "🌀・memes", "🌀・artwork", "🌀・cosplay", "🌀・gacha", "🌀・music-cover", "🌀・gameplay"], "voice": []},
    {"category": "────ARCADE GAMES────", "text": ["🍁・sentence-of-the-day", "🍁・mafia", "🍁・truth-or-dare", "🍁・asterie", "🍁・gartic", "🍁・owo", "🍁・casino", "🍁・virtual-fisher", "🍁・mudae", "🍁・uno", "🍁・make-a-sentence", "🍁・guess-the-number"], "voice": []},
    {"category": "────CALL & MUSIC────", "text": ["🏅・leaderboard", "🟡・music-playlist", "💭・discussion", "⚙️・music-command", "🛠️・vc-control"], "voice": ["🔶｜Create VC", "📚｜Study Room", "🎤｜Karaoke", "🔥・OG ROOM", "🔶・SERVER STREAK"]},
    {"category": "────STAFF ROOM────", "text": ["📢・annoucement", "📜・staff-rules", "📄・events", "🎙️・event-staff", "🎙️・podcast-team", "🔒・image-room", "🔒・staff-room", "🔒・config-bots"], "voice": ["📢・Meeting", "📢・staff room", "❤️・Cals & Beans"]},
    {"category": "────SERVER LOGS────", "text": ["🧰・server-updates", "🧰・server-log", "🧰・staff-log", "🧰・confession-log", "🧰・permission-log", "🧰・mods-log", "🧰・automod-log", "🧰・action-log", "🧰・event-log", "🧰・invites-log", "🧰・join_leave-log", "🧰・ticket-log", "🧰・deleted-log", "🧰・post-log", "🧰・updated-log"], "voice": []},
    {"category": "────SAFESPACE────", "text": [], "voice": ["safespace"]},
    {"category": "────AFKROOM────", "text": [], "voice": ["Server Staff", "Dormitory Room"]}
]

def _collect_guild_layout(source_guild: discord.Guild) -> dict:
    """Capture a guild's category/channel layout for later recreation."""
    categories_payload = []
    for category in sorted(source_guild.categories, key=lambda c: c.position):
        channels_payload = []
        category_channels = sorted(
            category.channels,
            key=lambda ch: (ch.position, ch.id)
        )
        for channel in category_channels:
            if isinstance(channel, discord.TextChannel):
                channel_type = "text"
            elif isinstance(channel, discord.VoiceChannel):
                channel_type = "voice"
            elif isinstance(channel, discord.StageChannel):
                channel_type = "stage"
            elif isinstance(channel, discord.ForumChannel):
                channel_type = "forum"
            else:
                continue
            channels_payload.append(
                {
                    "name": channel.name,
                    "type": channel_type
                }
            )

        categories_payload.append(
            {
                "name": category.name,
                "channels": channels_payload
            }
        )

    uncategorized_payload = []
    for channel in sorted(source_guild.channels, key=lambda ch: (ch.position, ch.id)):
        if channel.category is not None:
            continue
        if isinstance(channel, discord.TextChannel):
            channel_type = "text"
        elif isinstance(channel, discord.VoiceChannel):
            channel_type = "voice"
        elif isinstance(channel, discord.StageChannel):
            channel_type = "stage"
        elif isinstance(channel, discord.ForumChannel):
            channel_type = "forum"
        else:
            continue
        uncategorized_payload.append(
            {
                "name": channel.name,
                "type": channel_type
            }
        )
    if uncategorized_payload:
        categories_payload.append(
            {
                "name": None,
                "channels": uncategorized_payload
            }
        )

    return {
        "source_guild_id": source_guild.id,
        "source_guild_name": source_guild.name,
        "categories": categories_payload
    }


async def _apply_copied_layout(
    target_guild: discord.Guild,
    layout: dict,
    actor: Union[discord.Member, discord.User]
) -> tuple[int, int, int]:
    created_categories = 0
    created_text_channels = 0
    created_voice_channels = 0

    for section in layout.get("categories", []):
        category_name = section.get("name")

        category = None
        if category_name:
            category = discord.utils.get(target_guild.categories, name=category_name)
            if category is None:
                category = await target_guild.create_category(
                    category_name,
                    reason=f"Server copy paste requested by {actor}"
                )
                created_categories += 1

        for channel_info in section.get("channels", []):
            channel_name = channel_info.get("name")
            channel_type = channel_info.get("type")
            if not channel_name or not channel_type:
                continue

            if channel_type == "text":
                if discord.utils.get(target_guild.text_channels, name=channel_name, category=category) is None:
                    try:
                        await target_guild.create_text_channel(
                            channel_name,
                            category=category,
                            reason=f"Server copy paste requested by {actor}"
                        )
                        created_text_channels += 1
                    except discord.HTTPException as e:
                        if e.code == 50024:
                            logger.warning("Skipped unsupported text channel `%s` in guild %s: %s", channel_name, target_guild.id, e)
                            continue
                        raise
            elif channel_type == "voice":
                if discord.utils.get(target_guild.voice_channels, name=channel_name, category=category) is None:
                    try:
                        await target_guild.create_voice_channel(
                            channel_name,
                            category=category,
                            reason=f"Server copy paste requested by {actor}"
                        )
                        created_voice_channels += 1
                    except discord.HTTPException as e:
                        if e.code == 50024:
                            logger.warning("Skipped unsupported voice channel `%s` in guild %s: %s", channel_name, target_guild.id, e)
                            continue
                        raise
            elif channel_type == "stage":
                if discord.utils.get(target_guild.stage_channels, name=channel_name, category=category) is None:
                    try:
                        await target_guild.create_stage_channel(
                            channel_name,
                            category=category,
                            reason=f"Server copy paste requested by {actor}"
                        )
                        created_voice_channels += 1
                    except discord.HTTPException as e:
                        if e.code == 50024:
                            logger.warning("Skipped unsupported stage channel `%s` in guild %s: %s", channel_name, target_guild.id, e)
                            continue
                        raise
            elif channel_type == "forum":
                if discord.utils.get(target_guild.forums, name=channel_name, category=category) is None:
                    try:
                        await target_guild.create_forum(
                            name=channel_name,
                            category=category,
                            reason=f"Server copy paste requested by {actor}"
                        )
                        created_text_channels += 1
                    except discord.HTTPException as e:
                        if e.code == 50024:
                            logger.warning("Skipped unsupported forum channel `%s` in guild %s: %s", channel_name, target_guild.id, e)
                            continue
                        raise

    return created_categories, created_text_channels, created_voice_channels


@bot.command(name='nukethisserver21')
@commands.has_permissions(administrator=True)
async def nukethisserver21(ctx):
    """Delete all channels/categories/roles (except @everyone and managed roles)."""
    if ctx.guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return

    server_revert_cache[ctx.guild.id] = _collect_guild_layout(ctx.guild)

    deleted_channels = 0
    deleted_categories = 0
    deleted_roles = 0

    await ctx.send("⚠️ Say goodbye to your server now!")

    for channel in list(ctx.guild.channels):
        try:
            await channel.delete(reason=f"Server nuke requested by {ctx.author}")
            deleted_channels += 1
        except discord.Forbidden:
            continue
        except Exception as e:
            logger.warning("Failed to delete channel %s in guild %s: %s", channel.id, ctx.guild.id, e)

    for category in list(ctx.guild.categories):
        try:
            await category.delete(reason=f"Server nuke requested by {ctx.author}")
            deleted_categories += 1
        except discord.Forbidden:
            continue
        except Exception as e:
            logger.warning("Failed to delete category %s in guild %s: %s", category.id, ctx.guild.id, e)

    roles_to_delete = [
        role for role in sorted(ctx.guild.roles, key=lambda r: r.position, reverse=True)
        if role != ctx.guild.default_role and not role.managed
    ]
    for role in roles_to_delete:
        try:
            await role.delete(reason=f"Server nuke requested by {ctx.author}")
            deleted_roles += 1
        except discord.Forbidden:
            continue
        except Exception as e:
            logger.warning("Failed to delete role %s in guild %s: %s", role.id, ctx.guild.id, e)

    await ctx.send(
        "☢️ Server wipe complete.\n"
        "🩸 **FINISH HIM!** The server is now finished.\n"
        f"Deleted channels: **{deleted_channels}**\n"
        f"Deleted categories: **{deleted_categories}**\n"
        f"Deleted roles: **{deleted_roles}**\n"
        "Use `/revert` if you want me to restore the previous channel/category layout."
    )


@bot.tree.command(name="revert", description="Restore the last saved channel/category layout for this server.")
@app_commands.default_permissions(administrator=True)
async def revert_slash(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    layout = server_revert_cache.get(guild.id)
    if not layout:
        await interaction.followup.send(
            "❌ I don't have a saved layout for this server yet. Run the nuke command first so I can capture a backup.",
            ephemeral=True
        )
        return

    try:
        created_categories, created_text_channels, created_voice_channels = await _apply_copied_layout(
            guild,
            layout,
            interaction.user
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I need **Manage Channels** permission to restore channels and categories.",
            ephemeral=True
        )
        return
    except Exception as e:
        await interaction.followup.send(f"❌ Failed while restoring layout: {e}", ephemeral=True)
        return

    await interaction.followup.send(
        (
            "✅ Revert complete.\n"
            f"Restored categories: **{created_categories}**\n"
            f"Restored text/forum channels: **{created_text_channels}**\n"
            f"Restored voice/stage channels: **{created_voice_channels}**"
        ),
        ephemeral=True
    )


@bot.command(name='unlinuke21')
@commands.has_permissions(administrator=True)
async def unlinuke21(ctx):
    """Attempt to create 9,999 text channels without stopping on per-channel errors."""
    if ctx.guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return

    created_channels = 0
    failed_channels = 0

    await ctx.send("⚠️ Starting unlimited channel creation sequence (target: 9,999 channels).")

    for index in range(1, 10000):
        channel_name = f"unlinuke-{index:04d}"
        try:
            await ctx.guild.create_text_channel(
                channel_name,
                reason=f"Unlimited nuke requested by {ctx.author}"
            )
            created_channels += 1
        except discord.Forbidden:
            failed_channels += 1
            logger.warning(
                "Missing permissions while creating channel `%s` in guild %s.",
                channel_name,
                ctx.guild.id
            )
        except Exception as e:
            failed_channels += 1
            logger.warning(
                "Failed to create channel `%s` in guild %s: %s",
                channel_name,
                ctx.guild.id,
                e
            )

    await ctx.send(
        "✅ Unlimited channel creation finished.\n"
        f"Created channels: **{created_channels}**\n"
        f"Failed attempts: **{failed_channels}**"
    )


ENHANCE_STYLE_TEMPLATES = {
    "gaming": [
        {
            "category": "🎮 GAMING",
            "text": ["🎮・lobby", "🏆・leaderboards", "📢・tournaments"],
            "voice": ["🔊・squad-vc", "🎧・duo-vc"],
        }
    ],
    "minimal": [
        {
            "category": "📌 INFO",
            "text": ["📌・welcome", "📖・rules", "💬・general"],
            "voice": [],
        }
    ],
    "community": [
        {
            "category": "🌍 COMMUNITY",
            "text": ["👋・introductions", "💬・general", "🎉・events"],
            "voice": ["🗣️・hangout"],
        }
    ],
    "creator": [
        {
            "category": "🎬 CREATOR HUB",
            "text": ["📣・announcements", "🧠・ideas", "🎨・showcase"],
            "voice": ["🎙️・recording-room"],
        }
    ],
    "esports": [
        {
            "category": "⚔️ ESPORTS",
            "text": ["📅・match-schedule", "🎯・scrim-chat", "📊・team-stats"],
            "voice": ["🎤・team-comms", "🧪・strategy-room"],
        }
    ],
}

ENHANCE_STYLE_MIN_CHANNELS = 50


def _normalize_enhance_channel_name(name: str, prefix: str = "✨・") -> str:
    clean_name = (name or "").strip()
    if not clean_name:
        return "✨・channel"
    if clean_name.startswith(prefix):
        return clean_name[:100]
    return f"{prefix}{clean_name}"[:100]


async def _rate_limit_wait(last_action_at: float, min_interval: float) -> float:
    now = time.monotonic()
    elapsed = now - last_action_at
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    return time.monotonic()


async def _enhance_safe_rename(guild: discord.Guild, min_interval: float) -> tuple[int, int, float]:
    renamed = 0
    skipped = 0
    last_action_at = 0.0
    channels_sorted = sorted(guild.channels, key=lambda c: (c.position, c.id))
    for channel in channels_sorted:
        target_name = _normalize_enhance_channel_name(channel.name)
        if channel.name == target_name:
            skipped += 1
            continue
        last_action_at = await _rate_limit_wait(last_action_at, min_interval)
        try:
            await channel.edit(name=target_name, reason="enhanceserver safe mode rename")
            renamed += 1
        except (discord.Forbidden, discord.HTTPException):
            skipped += 1
    return renamed, skipped, last_action_at


async def _enhance_full_reset(guild: discord.Guild, min_interval: float) -> tuple[int, int, float]:
    deleted = 0
    failed = 0
    last_action_at = 0.0
    non_category_channels = sorted(
        [channel for channel in guild.channels if not isinstance(channel, discord.CategoryChannel)],
        key=lambda c: (c.position, c.id),
    )
    categories = sorted(guild.categories, key=lambda c: (c.position, c.id))

    for channel in non_category_channels:
        last_action_at = await _rate_limit_wait(last_action_at, min_interval)
        try:
            await channel.delete(reason="enhanceserver full reset")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    for category in categories:
        last_action_at = await _rate_limit_wait(last_action_at, min_interval)
        try:
            await category.delete(reason="enhanceserver full reset")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1
    return deleted, failed, last_action_at


async def _build_style_template(
    guild: discord.Guild,
    style_key: str,
    last_action_at: float,
    min_interval: float,
) -> tuple[int, int, int, float]:
    sections = ENHANCE_STYLE_TEMPLATES.get(style_key, [])
    if not sections:
        return 0, 0, 0, last_action_at

    created_categories = 0
    created_text = 0
    created_voice = 0
    reason = f"enhanceserver style={style_key}"
    style_categories: list[discord.CategoryChannel] = []

    for section in sections:
        category_name = section.get("category", "📁 CHANNELS")
        category = discord.utils.get(guild.categories, name=category_name)
        if category is None:
            last_action_at = await _rate_limit_wait(last_action_at, min_interval)
            category = await guild.create_category(category_name, reason=reason)
            created_categories += 1
        style_categories.append(category)

        for text_name in section.get("text", []):
            if discord.utils.get(guild.text_channels, name=text_name, category=category):
                continue
            last_action_at = await _rate_limit_wait(last_action_at, min_interval)
            await guild.create_text_channel(text_name, category=category, reason=reason)
            created_text += 1

        for voice_name in section.get("voice", []):
            if discord.utils.get(guild.voice_channels, name=voice_name, category=category):
                continue
            last_action_at = await _rate_limit_wait(last_action_at, min_interval)
            await guild.create_voice_channel(voice_name, category=category, reason=reason)
            created_voice += 1

    category_ids = {category.id for category in style_categories}
    style_channel_count = sum(
        1
        for channel in guild.channels
        if channel.category_id in category_ids
        and isinstance(channel, (discord.TextChannel, discord.VoiceChannel))
    )
    overflow_category = style_categories[0]
    overflow_index = 1

    while style_channel_count < ENHANCE_STYLE_MIN_CHANNELS:
        overflow_name = f"✨・{style_key}-chat-{overflow_index:02d}"
        overflow_index += 1
        if discord.utils.get(guild.text_channels, name=overflow_name, category=overflow_category):
            continue
        last_action_at = await _rate_limit_wait(last_action_at, min_interval)
        await guild.create_text_channel(overflow_name, category=overflow_category, reason=reason)
        created_text += 1
        style_channel_count += 1

    return created_categories, created_text, created_voice, last_action_at


@bot.tree.command(name="enhanceserver", description="Enhance server layout using one or more style templates.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    mode="Safe renames existing channels, reset deletes all channels before rebuilding.",
    style_primary="Main style template to apply.",
    style_secondary="Optional second style template.",
    style_tertiary="Optional third style template.",
)
@app_commands.choices(
    mode=[
        app_commands.Choice(name="✅ Safe (rename only)", value="safe"),
        app_commands.Choice(name="💥 Full Reset", value="reset"),
    ],
    style_primary=[
        app_commands.Choice(name="🎮 Gaming", value="gaming"),
        app_commands.Choice(name="✨ Minimal", value="minimal"),
        app_commands.Choice(name="🌍 Community", value="community"),
        app_commands.Choice(name="🎬 Creator Hub", value="creator"),
        app_commands.Choice(name="⚔️ Esports", value="esports"),
    ],
    style_secondary=[
        app_commands.Choice(name="🎮 Gaming", value="gaming"),
        app_commands.Choice(name="✨ Minimal", value="minimal"),
        app_commands.Choice(name="🌍 Community", value="community"),
        app_commands.Choice(name="🎬 Creator Hub", value="creator"),
        app_commands.Choice(name="⚔️ Esports", value="esports"),
    ],
    style_tertiary=[
        app_commands.Choice(name="🎮 Gaming", value="gaming"),
        app_commands.Choice(name="✨ Minimal", value="minimal"),
        app_commands.Choice(name="🌍 Community", value="community"),
        app_commands.Choice(name="🎬 Creator Hub", value="creator"),
        app_commands.Choice(name="⚔️ Esports", value="esports"),
    ],
)
async def enhanceserver_slash(
    interaction: discord.Interaction,
    mode: app_commands.Choice[str],
    style_primary: app_commands.Choice[str],
    style_secondary: app_commands.Choice[str] | None = None,
    style_tertiary: app_commands.Choice[str] | None = None,
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    unique_styles = []
    for style_choice in (style_primary, style_secondary, style_tertiary):
        if style_choice and style_choice.value not in unique_styles:
            unique_styles.append(style_choice.value)

    await interaction.response.defer(ephemeral=True, thinking=True)

    min_interval_seconds = 0.6
    renamed_count = 0
    skipped_renames = 0
    deleted_count = 0
    failed_deletes = 0
    created_categories_total = 0
    created_text_total = 0
    created_voice_total = 0
    last_action_at = 0.0

    try:
        if mode.value == "reset":
            deleted_count, failed_deletes, last_action_at = await _enhance_full_reset(guild, min_interval_seconds)
        else:
            renamed_count, skipped_renames, last_action_at = await _enhance_safe_rename(guild, min_interval_seconds)

        for style_key in unique_styles:
            created_categories, created_text, created_voice, last_action_at = await _build_style_template(
                guild,
                style_key,
                last_action_at,
                min_interval_seconds,
            )
            created_categories_total += created_categories
            created_text_total += created_text
            created_voice_total += created_voice
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I need **Manage Channels** permission to modify your server layout.",
            ephemeral=True,
        )
        return
    except Exception as e:
        await interaction.followup.send(f"❌ Enhancement failed: {e}", ephemeral=True)
        return

    report_lines = [
        "✅ Server enhancement complete.",
        f"Mode: **{mode.value}**",
        f"Styles applied: **{', '.join(unique_styles)}**",
        "",
        f"Renamed channels: **{renamed_count}**",
        f"Rename skipped/failed: **{skipped_renames}**",
        f"Deleted channels/categories: **{deleted_count}**",
        f"Delete failed: **{failed_deletes}**",
        f"Created categories: **{created_categories_total}**",
        f"Created text channels: **{created_text_total}**",
        f"Created voice channels: **{created_voice_total}**",
        f"Rate-limit delay: **{min_interval_seconds:.1f}s** between actions",
    ]
    await interaction.followup.send("\n".join(report_lines), ephemeral=True)


@bot.tree.command(name="createserver", description="Create server categories and channels using the default template.")
@app_commands.default_permissions(administrator=True)
async def createserver_slash(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    created_categories = 0
    created_text_channels = 0
    created_voice_channels = 0

    try:
        for section in SERVER_TEMPLATE:
            category_name = section["category"]
            category = discord.utils.get(guild.categories, name=category_name)

            if category is None:
                category = await guild.create_category(
                    category_name,
                    reason=f"Server template setup by {interaction.user}"
                )
                created_categories += 1

            for text_name in section["text"]:
                if discord.utils.get(guild.text_channels, name=text_name, category=category) is None:
                    await guild.create_text_channel(
                        text_name,
                        category=category,
                        reason=f"Server template setup by {interaction.user}"
                    )
                    created_text_channels += 1

            for voice_name in section["voice"]:
                if discord.utils.get(guild.voice_channels, name=voice_name, category=category) is None:
                    await guild.create_voice_channel(
                        voice_name,
                        category=category,
                        reason=f"Server template setup by {interaction.user}"
                    )
                    created_voice_channels += 1
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I need **Manage Channels** permission to create categories/channels.",
            ephemeral=True
        )
        return
    except Exception as e:
        await interaction.followup.send(f"❌ Failed while creating server template: {e}", ephemeral=True)
        return

    await interaction.followup.send(
        (
            "✅ Server template applied.\n"
            f"Created categories: **{created_categories}**\n"
            f"Created text channels: **{created_text_channels}**\n"
            f"Created voice channels: **{created_voice_channels}**"
        ),
        ephemeral=True
    )


@bot.tree.command(name="copyserver", description="Copy category/channel layout from another server the bot is in.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(server_id="Server ID to copy from")
async def copyserver_slash(interaction: discord.Interaction, server_id: str):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    if not server_id.isdigit():
        await interaction.followup.send("❌ Please provide a valid numeric server ID.", ephemeral=True)
        return

    source_guild = bot.get_guild(int(server_id))
    if source_guild is None:
        await interaction.followup.send(
            "❌ I can't access that server. Make sure I'm in it and that the ID is correct.",
            ephemeral=True
        )
        return

    layout = _collect_guild_layout(source_guild)
    server_copy_cache[interaction.user.id] = layout

    category_count = len(layout.get("categories", []))
    channel_count = sum(len(section.get("channels", [])) for section in layout.get("categories", []))
    await interaction.followup.send(
        (
            f"✅ Copied layout from **{source_guild.name}** (`{source_guild.id}`).\n"
            f"Saved **{category_count}** categories and **{channel_count}** channels.\n"
            "Now run `/pasteserver` in the target server."
        ),
        ephemeral=True
    )


@bot.tree.command(name="pasteserver", description="Paste the last copied category/channel layout into this server.")
@app_commands.default_permissions(administrator=True)
async def pasteserver_slash(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    layout = server_copy_cache.get(interaction.user.id)
    if not layout:
        await interaction.followup.send(
            "❌ No copied server layout found. Run `/copyserver <server_id>` first.",
            ephemeral=True
        )
        return

    try:
        created_categories, created_text_channels, created_voice_channels = await _apply_copied_layout(
            guild,
            layout,
            interaction.user
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I need **Manage Channels** permission to create categories/channels.",
            ephemeral=True
        )
        return
    except Exception as e:
        await interaction.followup.send(f"❌ Failed while pasting server layout: {e}", ephemeral=True)
        return

    await interaction.followup.send(
        (
            f"✅ Pasted copied layout from **{layout.get('source_guild_name', 'Unknown Server')}**.\n"
            f"Created categories: **{created_categories}**\n"
            f"Created text/forum channels: **{created_text_channels}**\n"
            f"Created voice/stage channels: **{created_voice_channels}**"
        ),
        ephemeral=True
    )


async def post_confession_message(
    *,
    interaction: Union[discord.Interaction, None],
    ctx=None,
    message_text: str,
    source_author: Union[discord.Member, discord.User],
    attachment: Union[discord.Attachment, None] = None,
    is_reply: bool = False,
    send_success_response: bool = False
):
    guild = interaction.guild if interaction else ctx.guild
    data = load_attendance_data(guild.id)
    confession_channel_id = data.get('confession_channel_id')
    confession_channel = guild.get_channel(confession_channel_id) if confession_channel_id else None
    if not confession_channel:
        warning_message = "⚠️ Confession channel is not configured. Ask an admin to run `/setupconfession`."
        if interaction:
            await interaction.response.send_message(warning_message, ephemeral=True)
        else:
            await ctx.send(warning_message)
        return False

    clean_message = message_text.strip()
    if not clean_message:
        if interaction:
            await interaction.response.send_message("❌ Please provide a confession message.", ephemeral=True)
        else:
            await ctx.send("❌ Please provide a confession message.")
        return False

    confession_number = (data.get('confession_counter') or 0) + 1
    data['confession_counter'] = confession_number
    save_attendance_data(guild.id, data)

    entry_type = "Reply" if is_reply else "Confession"
    embed_title = f"Anonymous {entry_type} (#{confession_number})"
    description = clean_message
    if attachment:
        description = f"{clean_message}\n\n[ attached photo ]"

    embed_header = data.get('reply_header_text') if is_reply else data.get('confession_header_text')
    embed_footer = data.get('confession_embed_footer_text') or f"Anonymous {entry_type}"
    color_hex = data.get('confession_embed_color')
    embed_color = discord.Color.from_rgb(15, 18, 25)
    if isinstance(color_hex, str) and color_hex.strip().startswith("#") and len(color_hex.strip()) == 7:
        try:
            embed_color = discord.Color(int(color_hex.strip()[1:], 16))
        except ValueError:
            pass

    confession_embed = discord.Embed(
        title=embed_header or embed_title,
        description=description,
        color=embed_color,
        timestamp=discord.utils.utcnow()
    )
    confession_embed.set_footer(text=embed_footer)
    files = [await attachment.to_file()] if attachment else []
    submit_label = data.get('confession_submit_button_text') or "Submit a confession!"
    reply_label = data.get('confession_reply_button_text') or "Reply"
    try:
        await confession_channel.send(embed=confession_embed, files=files, view=ConfessionActionView(submit_label=submit_label, reply_label=reply_label))
    except Exception:
        if interaction and not interaction.response.is_done():
            await interaction.response.send_message("❌ Something went wrong, try again.", ephemeral=True)
        elif ctx:
            await ctx.send("❌ Something went wrong, try again.")
        return False

    log_channel_id = data.get('confession_log_channel_id')
    log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
    if log_channel:
        log_embed = discord.Embed(
            title=f"🧾 {entry_type} Log",
            description=clean_message,
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow()
        )
        log_embed.add_field(name="Sender", value=f"{source_author} (`{source_author.id}`)", inline=False)
        if ctx and ctx.channel:
            log_embed.add_field(name="Source", value=ctx.channel.mention, inline=False)
        elif interaction and interaction.channel:
            log_embed.add_field(name="Source", value=interaction.channel.mention, inline=False)
        await log_channel.send(embed=log_embed)
    if send_success_response and interaction:
        if interaction.response.is_done():
            await interaction.followup.send("✅ Your confession was posted anonymously.", ephemeral=True)
        else:
            await interaction.response.send_message("✅ Your confession was posted anonymously.", ephemeral=True)
    return True


@bot.hybrid_command(name='confess', with_app_command=True, description="Post an anonymous confession to the configured confession channel.")
@app_commands.describe(message_text="Your anonymous confession message")
async def confess(ctx, *, message_text: str, attachment: Union[discord.Attachment, None] = None):
    """
    Send an anonymous confession to the configured confession channel.
    Usage: !confess <message>
    Slash usage: /confess message_text:<message>
    """
    if len(message_text.strip()) > 1800:
        await ctx.send("❌ Confession is too long. Please keep it under 1800 characters.")
        return

    prefix_attachment = None
    if not ctx.interaction and getattr(ctx.message, "attachments", None):
        prefix_attachment = ctx.message.attachments[0]
    selected_attachment = attachment or prefix_attachment
    ok = await post_confession_message(
        ctx=ctx,
        interaction=ctx.interaction if ctx.interaction else None,
        message_text=message_text,
        source_author=ctx.author,
        attachment=selected_attachment
    )
    if not ok:
        return

    if ctx.interaction:
        await ctx.send("✅ Your confession was posted anonymously.", ephemeral=True)
    else:
        try:
            await ctx.message.delete()
        except Exception:
            pass
        await ctx.send("✅ Your confession was posted anonymously.", delete_after=8)


@bot.hybrid_command(name='reply', with_app_command=True, description="Post an anonymous reply to the configured confession channel.")
@app_commands.describe(message_text="Your anonymous reply message")
async def reply_confession(ctx, *, message_text: str, attachment: Union[discord.Attachment, None] = None):
    """
    Send an anonymous reply to the configured confession channel.
    Usage: !reply <message>
    Slash usage: /reply message_text:<message>
    """
    if len(message_text.strip()) > 1800:
        await ctx.send("❌ Reply is too long. Please keep it under 1800 characters.")
        return

    prefix_attachment = None
    if not ctx.interaction and getattr(ctx.message, "attachments", None):
        prefix_attachment = ctx.message.attachments[0]
    selected_attachment = attachment or prefix_attachment
    ok = await post_confession_message(
        ctx=ctx,
        interaction=ctx.interaction if ctx.interaction else None,
        message_text=message_text,
        source_author=ctx.author,
        attachment=selected_attachment,
        is_reply=True
    )
    if not ok:
        return

    if ctx.interaction:
        await ctx.send("✅ Your reply was posted anonymously.", ephemeral=True)
    else:
        try:
            await ctx.message.delete()
        except Exception:
            pass
        await ctx.send("✅ Your reply was posted anonymously.", delete_after=8)

async def update_user_status(ctx, member, status, reason=None):
    data = load_attendance_data(ctx.guild.id)
    
    # Get all role IDs
    present_role_id = data.get('attendance_role_id')
    absent_role_id = data.get('absent_role_id')
    excused_role_id = data.get('excused_role_id')
    
    target_role_id = None
    roles_to_remove = []
    
    if status == 'present':
        target_role_id = present_role_id
        if absent_role_id: roles_to_remove.append(absent_role_id)
        if excused_role_id: roles_to_remove.append(excused_role_id)
    elif status == 'absent':
        target_role_id = absent_role_id
        if present_role_id: roles_to_remove.append(present_role_id)
        if excused_role_id: roles_to_remove.append(excused_role_id)
    elif status == 'excused':
        target_role_id = excused_role_id
        if present_role_id: roles_to_remove.append(present_role_id)
        if absent_role_id: roles_to_remove.append(absent_role_id)
        
    # Remove conflicting roles
    for rid in roles_to_remove:
        role = ctx.guild.get_role(rid)
        if role and role in member.roles:
            try:
                await member.remove_roles(role)
            except discord.Forbidden:
                await ctx.send(f"Warning: Could not remove role {role.name} from {member.display_name} (Missing Permissions)")

    # Add new role
    if target_role_id:
        role = ctx.guild.get_role(target_role_id)
        if role:
            try:
                await member.add_roles(role)
                msg = f"Marked {member.mention} as **{status.upper()}** and gave them the {role.name} role."
                if reason:
                    msg += f"\nReason: {reason}"
                await ctx.send(msg, delete_after=10)
            except discord.Forbidden:
                await ctx.send(f"Failed to give {status} role to {member.display_name} (Missing Permissions)", delete_after=10)
        else:
             msg = f"Marked {member.mention} as **{status.upper()}**, but the role for this status is not configured or deleted."
             if reason:
                 msg += f"\nReason: {reason}"
             await ctx.send(msg, delete_after=10)
    else:
        msg = f"Marked {member.mention} as **{status.upper()}**. (No role configured for this status)"
        if reason:
            msg += f"\nReason: {reason}"
        await ctx.send(msg, delete_after=10)

    # Update JSON
    user_id = str(member.id)
    if 'records' not in data:
        data['records'] = {}
    
    record = {
        "status": status,
        "timestamp": datetime.datetime.now().isoformat(),
        "channel_id": ctx.channel.id
    }
    if reason:
        record["reason"] = reason
        
    data['records'][user_id] = record
    save_attendance_data(ctx.guild.id, data)
    if status in ('present', 'absent', 'excused'):
        database.increment_status_count(ctx.guild.id, member.id, status)
    
    # Philippines Time (UTC+8) for DMs
    ph_tz = datetime.timezone(datetime.timedelta(hours=8))
    now_ph = datetime.datetime.now(ph_tz)
    date_str = now_ph.strftime('%B %d, %Y')
    time_str = now_ph.strftime('%I:%M %p')

    # DM the user if excused
    if status == 'excused':
        try:
            dm_embed = discord.Embed(
                title="Attendance Status: Excused",
                description=f"You have been marked as **EXCUSED** in **{ctx.guild.name}**.",
                color=discord.Color.from_rgb(255, 255, 255),
                timestamp=now_ph
            )
            if reason:
                dm_embed.add_field(name="Reason", value=reason, inline=False)
            
            dm_embed.add_field(name="Date", value=date_str, inline=True)
            dm_embed.add_field(name="Time", value=time_str, inline=True)
            
            if ctx.guild.icon:
                dm_embed.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url)
                dm_embed.set_thumbnail(url=ctx.guild.icon.url)
            else:
                dm_embed.set_author(name=ctx.guild.name)
            
            dm_embed.set_footer(text="Registrar Bot • Attendance System")
                
            await member.send(embed=dm_embed)
        except discord.Forbidden:
            pass
            
    # DM the user if absent
    if status == 'absent':
        try:
            dm_embed = discord.Embed(
                title="Attendance Status: Absent",
                description=f"You have been marked as **ABSENT** in **{ctx.guild.name}**.",
                color=discord.Color.red(),
                timestamp=now_ph
            )
            
            dm_embed.add_field(name="Date", value=date_str, inline=True)
            dm_embed.add_field(name="Time", value=time_str, inline=True)
            
            if ctx.guild.icon:
                dm_embed.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url)
                dm_embed.set_thumbnail(url=ctx.guild.icon.url)
            else:
                dm_embed.set_author(name=ctx.guild.name)
            
            dm_embed.set_footer(text="Registrar Bot • Attendance System")
                
            await member.send(embed=dm_embed)
        except discord.Forbidden:
            pass

    # Refresh the report immediately
    await refresh_attendance_report(ctx.guild, force_update=True)

@bot.command(name='setpermitrole', aliases=['allowrole'])
@commands.has_permissions(manage_roles=True)
async def set_permit_role(ctx, role: discord.Role = None):
    """
    Sets the role required to use the 'present' command.
    Usage: !setpermitrole @Role
    Usage: !setpermitrole (to reset/allow everyone)
    """
    data = load_attendance_data(ctx.guild.id)
    if role:
        data['allowed_role_id'] = role.id
        await ctx.send(f"Permission Updated: Only users with the {role.mention} role can mark attendance.")
    else:
        data['allowed_role_id'] = None
        await ctx.send("Permission Updated: Everyone can now mark attendance.")
    
    save_attendance_data(ctx.guild.id, data)
    
    # Check setup completion
    await check_and_notify_setup_completion(ctx)

@bot.command(name='channelpresent', aliases=['setpresentchannel'])
@commands.has_permissions(manage_channels=True)
async def set_present_channel(ctx, channel: discord.TextChannel = None):
    """
    Sets the only channel where users are allowed to say 'present'.
    Usage: !channelpresent #channel
    Usage: !channelpresent (to remove the restriction)
    """
    data = load_attendance_data(ctx.guild.id)
    if channel:
        data['present_channel_id'] = channel.id
        save_attendance_data(ctx.guild.id, data)
        await ctx.send(f"Present channel updated: users can only say `present` in {channel.mention}.")
    else:
        data['present_channel_id'] = None
        save_attendance_data(ctx.guild.id, data)
        await ctx.send("Present channel restriction removed: users can say `present` in any channel.")

@bot.command(name='resetpermitrole', aliases=['resetassignrole', 'resetallowedrole'])
@commands.has_permissions(manage_roles=True)
async def reset_permit_role_users(ctx):
    """
    Removes the 'Permitted Role' (assigned via !setpermitrole) from ALL users who have it.
    This effectively resets who is allowed to say 'present'.
    Usage: !resetpermitrole
    """
    data = load_attendance_data(ctx.guild.id)
    allowed_role_id = data.get('allowed_role_id')
    
    if not allowed_role_id:
        await ctx.send("No 'Permitted Role' is currently configured. Use `!setpermitrole @Role` first.")
        return
        
    role = ctx.guild.get_role(allowed_role_id)
    if not role:
        await ctx.send("The configured 'Permitted Role' no longer exists in this server.")
        return
        
    # Get users with the role
    users_with_role = role.members
    
    if not users_with_role:
        await ctx.send(f"No users currently have the {role.mention} role.")
        return
        
    await ctx.send(f"Removing {role.mention} from {len(users_with_role)} users... This may take a moment.")
    
    count = 0
    for member in users_with_role:
        try:
            await member.remove_roles(role)
            count += 1
            # Add a small delay to avoid rate limits if many users
            if count % 5 == 0:
                await asyncio.sleep(1) 
        except discord.Forbidden:
            logger.warning(f"Failed to remove permitted role from {member.name} (Missing Permissions)")
        except Exception as e:
            logger.error(f"Error removing permitted role from {member.id}: {e}")
            
    await ctx.send(f"✅ Reset complete! Removed {role.mention} from {count} users. They will need to be re-assigned the role to say 'present'.")

@bot.command(name='reset')
@commands.has_permissions(manage_roles=True)
async def reset_specific_role(ctx, role: discord.Role):
    """
    Removes the specified role from ALL users who have it.
    Usage: !reset @Role
    """
    # Get users with the role
    users_with_role = role.members
    
    if not users_with_role:
        await ctx.send(f"No users currently have the {role.mention} role.")
        return
        
    await ctx.send(f"Removing {role.mention} from {len(users_with_role)} users... This may take a moment.")
    
    count = 0
    for member in users_with_role:
        try:
            await member.remove_roles(role)
            count += 1
            # Add a small delay to avoid rate limits if many users
            if count % 5 == 0:
                await asyncio.sleep(1) 
        except discord.Forbidden:
            logger.warning(f"Failed to remove role {role.name} from {member.name} (Missing Permissions)")
        except Exception as e:
            logger.error(f"Error removing role {role.name} from {member.id}: {e}")
            
    await ctx.send(f"✅ Reset complete! Removed {role.mention} from {count} users.")

def get_current_ph_time():
    """Returns the current Philippines time (UTC+8)."""
    ph_tz = datetime.timezone(datetime.timedelta(hours=8))
    return datetime.datetime.now(ph_tz)


def is_weekend_in_ph(now_dt=None):
    """Returns True when the current Philippines day is Saturday or Sunday."""
    current_dt = now_dt or get_current_ph_time()
    return current_dt.weekday() >= 5


def is_nstp_present_text(text):
    """Returns True when a message indicates present check-in for NSTP."""
    lowered = " ".join((text or "").lower().strip().split())
    return (
        lowered == "present nstp"
        or lowered == "present for nstp"
        or lowered == "present for the subject nstp"
        or lowered == "present subject nstp"
        or lowered == "nstp present"
    )


def is_in_attendance_window(guild_id, allow_weekend_override=False):
    settings = load_settings(guild_id)
    now_dt = get_current_ph_time()

    if is_weekend_in_ph(now_dt) and not allow_weekend_override:
        current_day = now_dt.strftime("%A")
        return False, f"Attendance is closed on weekends. Today is {current_day} in Philippines time, so attendance is only available from Monday to Friday."

    if settings.get('attendance_mode') != 'window':
        return True, None
    
    start_str = settings.get('window_start_time', '00:00')
    end_str = settings.get('window_end_time', '23:59')
    
    try:
        t_start = datetime.datetime.strptime(start_str, "%H:%M").time()
        t_end = datetime.datetime.strptime(end_str, "%H:%M").time()
        now = now_dt.time()
        
        in_window = False
        if t_start <= t_end:
            in_window = t_start <= now <= t_end
        else:
            in_window = now >= t_start or now <= t_end
            
        if not in_window:
            # Convert to 12-hour format for display
            display_start = t_start.strftime("%I:%M %p").lstrip('0')
            display_end = t_end.strftime("%I:%M %p").lstrip('0')
            current_time = now.strftime("%I:%M %p").lstrip('0')
            return False, f"Attendance is only allowed between {display_start} and {display_end}. (Current Time: {current_time})"
            
        return True, None
    except ValueError:
        return True, None

@bot.command(name='present')
async def mark_present(ctx, member: discord.Member = None):
    """
    Marks a user as present.
    Usage: !present (for yourself)
    Usage: !present @User (requires Manage Roles)
    """
    if member is None:
        member = ctx.author

    # Check for required role if marking self
    if member == ctx.author:
        settings = load_settings(ctx.guild.id)
        
        # Check Window
        allowed, msg = is_in_attendance_window(ctx.guild.id)
        if not allowed:
             await ctx.send(msg)
             return
        
        if not settings.get('allow_self_marking', True):
            await ctx.send("Self-marking is currently disabled.")
            return

        data = load_attendance_data(ctx.guild.id)
        existing_status = has_conflicting_attendance_status(data.get('records'), ctx.author.id, 'present')
        if existing_status:
            await ctx.send(
                f"You are already marked as **{existing_status}** and cannot switch to **present** this session. Reset attendance before changing it."
            )
            return

        allowed_role_id = data.get('allowed_role_id')
        if allowed_role_id:
            allowed_role = ctx.guild.get_role(allowed_role_id)
            if allowed_role and allowed_role not in ctx.author.roles:
                await ctx.send(f"You need the {allowed_role.mention} role to mark attendance.")
                return

    if member != ctx.author and not ctx.author.guild_permissions.manage_roles:
        await ctx.send("You do not have permission to mark others as present.")
        return

    await update_user_status(ctx, member, 'present')

@bot.command(name='absent')
@commands.has_permissions(manage_roles=True)
async def mark_absent(ctx, member: discord.Member):
    """
    Marks a user as absent.
    Usage: !absent @User
    """
    allowed, msg = is_in_attendance_window(ctx.guild.id)
    if not allowed:
        await ctx.send(msg)
        return

    await update_user_status(ctx, member, 'absent')

@bot.command(name='excuse')
async def mark_excuse(ctx, member: discord.Member, *, reason: str):
    """
    Marks a user as excused with a reason.
    Usage: !excuse @User I was sick
    """
    allowed, msg = is_in_attendance_window(ctx.guild.id)
    if not allowed:
        await ctx.send(f"{msg} Excuse submissions are also closed once attendance time is over.")
        return

    settings = load_settings(ctx.guild.id)
    if settings.get('require_admin_excuse', True):
        if not ctx.author.guild_permissions.manage_roles:
            await ctx.send("You do not have permission to excuse users.")
            return

    await update_user_status(ctx, member, 'excused', reason=reason)

@mark_excuse.error
async def mark_excuse_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: `!excuse @User <reason>` (e.g., `!excuse @John I was sick`)")

def create_attendance_embed(guild):
    logger.info(f"Generating report for guild: {guild.name} ({guild.id})")
    data = load_attendance_data(guild.id)
    records = data.get('records', {})
    
    now_ph = get_current_ph_time()

    embed = discord.Embed(title="Daily Attendance Report", color=discord.Color.gold())
    if guild.icon:
        embed.set_author(name=guild.name, icon_url=guild.icon.url)
        embed.set_thumbnail(url=guild.icon.url)
    else:
        embed.set_author(name=guild.name)
        
    # Check status
    allowed, _ = is_in_attendance_window(guild.id)
    status_str = "🟢 **OPEN**" if allowed else "🔴 **CLOSED**"
    
    # Get Window Info
    settings = data.get('settings', {})
    time_info = f"**⌚ Time:** `{now_ph.strftime('%I:%M %p')}`"
    
    if settings.get('attendance_mode') == 'window':
        end_str = settings.get('window_end_time', '23:59')
        try:
            t_end = datetime.datetime.strptime(end_str, "%H:%M").time()
            display_end = t_end.strftime("%I:%M %p").lstrip('0')
            time_info += f"\n**⏳ Deadline:** `{display_end}`"
        except:
            pass

    weekend_note = ""
    if is_weekend_in_ph(now_ph):
        weekend_note = "**Note:** Weekend attendance submissions are disabled except for NSTP `present` check-ins.\n"

    embed.description = (
        f"**🗓️ Date:** `{now_ph.strftime('%B %d, %Y')}`\n"
        f"{time_info}\n"
        f"**Status:** {status_str}\n"
        f"{weekend_note}"
    )
    
    # Helper to get name
    def get_name(uid):
        member = guild.get_member(int(uid))
        return member.display_name if member else f"Unknown ({uid})"

    # Sort records by name for cleaner display
    sorted_records = sorted(records.items(), key=lambda x: get_name(x[0]).lower())

    present_entries = []
    absent_entries = []
    excused_entries = []

    for uid, info in sorted_records:
        if isinstance(info, str):
            info = {"status": "present", "timestamp": info}
            
        status = info.get('status', 'present')
        reason = info.get('reason')
        name = get_name(uid)
        
        entry = f"• {name}"
        if reason:
            entry += f" (*{reason}*)"

        if status == 'present':
            present_entries.append(entry)
        elif status == 'absent':
            absent_entries.append(entry)
        elif status == 'excused':
            excused_entries.append(entry)

    # Helper to chunk list to avoid hitting Discord 1024 char limit
    def format_list(entries):
        if not entries:
            return "None"
        text = "\n".join(entries)
        if len(text) > 1000:
            return text[:950] + "\n... (truncated)"
        return text

    embed.add_field(name=f"✅  **Present**  ` {len(present_entries)} `", value=format_list(present_entries), inline=True)
    embed.add_field(name=f"❌  **Absent**  ` {len(absent_entries)} `", value=format_list(absent_entries), inline=True)
    embed.add_field(name=f"⚠️  **Excused**  ` {len(excused_entries)} `", value=format_list(excused_entries), inline=False)
    
    embed.set_footer(text=f"Calvsbot • Last Updated: {now_ph.strftime('%I:%M %p')}", icon_url=guild.icon.url if guild.icon else None)
    
    return embed

@bot.command(name='removepresent')
@commands.has_permissions(manage_roles=True)
async def remove_present(ctx, member: discord.Member):
    """
    Removes a user's present status/role so they can mark attendance again.
    Usage: !removepresent @User
    """
    data = load_attendance_data(ctx.guild.id)
    role_id = data.get('attendance_role_id')
    user_id = str(member.id)
    
    # Remove from records
    if 'records' in data and user_id in data['records']:
        del data['records'][user_id]
        save_attendance_data(ctx.guild.id, data)
    
    # Remove role
    if role_id:
        role = ctx.guild.get_role(role_id)
        if role and role in member.roles:
            try:
                await member.remove_roles(role)
            except discord.Forbidden:
                await ctx.send("Warning: Could not remove role (Missing Permissions).")
                
    await ctx.send(f"Reset attendance for {member.mention}. You can now say 'present' again.")

@bot.command(name='restartattendance', aliases=['resetattendance'])
@commands.has_permissions(administrator=True)
async def restart_attendance(ctx):
    """
    Completely resets ALL attendance data and settings for this server.
    Removes roles from present users, clears all records, and resets configuration.
    Usage: !restartattendance
    """
    embed = discord.Embed(title="⚠️ Confirm Full Reset", description="Are you sure you want to restart everything?\n\nThis will:\n1. Remove attendance roles from users.\n2. Delete ALL attendance records.\n3. Delete ALL leaderboard stats data.\n4. Reset configuration (Time Window, Roles, Channels) to default.\n\nType `confirm` to proceed.", color=discord.Color.red())
    await ctx.send(embed=embed)

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == 'confirm'

    try:
        await bot.wait_for('message', check=check, timeout=30.0)
    except asyncio.TimeoutError:
        await ctx.send("Reset cancelled (timed out).")
        return

    # Proceed with reset
    await ctx.send("🔄 Resetting attendance system... Please wait.")
    
    data = load_attendance_data(ctx.guild.id)
    
    # 1. Remove Roles
    roles_to_reset = []
    
    # Get all configured roles
    if data.get('attendance_role_id'): roles_to_reset.append(data.get('attendance_role_id'))
    if data.get('absent_role_id'): roles_to_reset.append(data.get('absent_role_id'))
    if data.get('excused_role_id'): roles_to_reset.append(data.get('excused_role_id'))
    
    for rid in roles_to_reset:
        role = ctx.guild.get_role(rid)
        if role:
            for member in role.members:
                try:
                    await member.remove_roles(role)
                    await asyncio.sleep(0.3)
                except: pass
    
    # 2. Wipe Data and Settings
    # Create a fresh default structure
    default_settings = {
        "suffix_format": " [𝙼𝚂𝚄𝚊𝚗]",
        "auto_nick_on_join": False,
        "enforce_suffix": False,
        "remove_suffix_on_role_loss": False,
        "attendance_mode": "duration",
        "attendance_expiry_hours": 12,
        "allow_self_marking": True,
        "require_admin_excuse": False,
        "window_start_time": "08:00",
        "window_end_time": "17:00",
        "last_processed_date": None
    }

    fresh_data = {
        "attendance_role_id": data.get('attendance_role_id'),
        "absent_role_id": data.get('absent_role_id'),
        "excused_role_id": data.get('excused_role_id'),
        "ping_role_id": data.get('ping_role_id'),
        "welcome_channel_id": data.get('welcome_channel_id'),
        "report_channel_id": data.get('report_channel_id'),
        "records": {},
        "settings": default_settings
    }
    
    save_attendance_data(ctx.guild.id, fresh_data)
    database.clear_attendance_records(ctx.guild.id)
    database.clear_attendance_stats(ctx.guild.id)
    
    # Attempt to post a fresh, empty report to the report channel
    report_channel_id = data.get('report_channel_id')
    if report_channel_id:
        channel = ctx.guild.get_channel(report_channel_id)
        if channel:
            try:
                # Create a temporary guild object or just call the function since it only needs ID for loading data
                # but create_attendance_embed uses guild.get_member etc.
                # Since we are in ctx, we can use ctx.guild
                embed = create_attendance_embed(ctx.guild)
                await channel.send(embed=embed)
            except:
                pass

    await ctx.send("✅ **System Reset Complete.**\nAll data has been cleared. You can now reconfigure the bot using `!settime`, `!assignchannel`, etc.")

# Store the last report state to prevent unnecessary updates
guild_report_state = {}

async def refresh_attendance_report(guild, target_channel=None, force_update=False):
    """
    Updates the existing report or sends a new one if it doesn't exist.
    """
    data = load_attendance_data(guild.id)
    
    # Calculate state to check if update is needed
    try:
        is_open, _ = is_in_attendance_window(guild.id)
        records = data.get('records', {})
        # Create a stable string representation of the data that affects the report content
        # We include: Open Status, Records (sorted), and Window Settings (in case time changes)
        settings = data.get('settings', {})
        window_info = f"{settings.get('window_start_time')}-{settings.get('window_end_time')}"
        
        # Sort records by user ID to ensure consistent ordering in the hash
        sorted_records = sorted(records.items())
        
        current_state = f"{is_open}|{window_info}|{str(sorted_records)}"
        
        last_state = guild_report_state.get(guild.id)
        
        if not force_update and last_state == current_state:
            # Content hasn't changed, skip update to prevent spam
            return None
            
        guild_report_state[guild.id] = current_state
        
    except Exception as e:
        logger.error(f"Error calculating report state: {e}")
        # If calculation fails, proceed with update just in case
    
    last_msg_id = data.get('last_report_message_id')
    last_chan_id = data.get('last_report_channel_id')
    
    # Determine Target Channel
    channel = target_channel
    if not channel:
        report_channel_id = data.get('report_channel_id')
        if report_channel_id:
            channel = guild.get_channel(report_channel_id)
            
    # Removed fallback to welcome/system channel to allow "removing" the report completely
    if not channel:
        return None # Nowhere to send

    # SAFETY CHECK: Ensure target channel belongs to the guild
    if channel.guild.id != guild.id:
        logger.error(f"Security Alert: Attempted to post report for {guild.name} to channel in {channel.guild.name}!")
        return None
        
    embed = create_attendance_embed(guild)
    
    # Try to edit existing message if channel matches
    if last_msg_id and last_chan_id and last_chan_id == channel.id:
        try:
            msg = await channel.fetch_message(last_msg_id)
            await msg.edit(embed=embed)
            return msg
        except (discord.NotFound, discord.Forbidden):
            # Message deleted or can't access, fall through to send new
            pass
        except Exception as e:
            logger.error(f"Error editing report: {e}")
            pass

    # If we are here, we need to send a new message
    # First, try to delete the old one if it was in a DIFFERENT channel (or we failed to edit)
    if last_msg_id and last_chan_id and last_chan_id != channel.id:
        try:
             old_chan = guild.get_channel(last_chan_id)
             if old_chan:
                 try:
                     old_msg = await old_chan.fetch_message(last_msg_id)
                     await old_msg.delete()
                 except: pass
        except:
            pass
            
    try:
        new_msg = await channel.send(embed=embed)
        data['last_report_message_id'] = new_msg.id
        data['last_report_channel_id'] = channel.id
        save_attendance_data(guild.id, data)
        return new_msg
    except discord.Forbidden:
        return None

@bot.command(name='attendance_leaderboard', aliases=['presentleaderboard', 'leaderboard'])
async def attendance_leaderboard(ctx, page: int = 1):
    per_page = 10
    max_pages = 200

    total_rows = database.get_attendance_leaderboard_count(ctx.guild.id)
    if total_rows == 0:
        await ctx.send("No attendance data yet.")
        return

    calculated_pages = (total_rows + per_page - 1) // per_page
    total_pages = max(1, min(max_pages, calculated_pages))
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    offset = (page - 1) * per_page
    rows = database.get_attendance_leaderboard(ctx.guild.id, per_page, offset)
    if not rows:
        await ctx.send("No attendance data yet.")
        return

    start_rank = offset + 1
    end_rank = offset + len(rows)

    embed = discord.Embed(
        title="🏆 Attendance Leaderboard",
        description=(
            f"**{ctx.guild.name}**\n"
            f"Showing ranks **{start_rank}–{end_rank}**"
        ),
        color=discord.Color.gold()
    )
    embed.timestamp = discord.utils.utcnow()

    if ctx.guild.icon:
        embed.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url)
    else:
        embed.set_author(name=ctx.guild.name)

    # Show a recognizable logo in the leaderboard embed.
    if bot.user and bot.user.display_avatar:
        embed.set_thumbnail(url=bot.user.display_avatar.url)
    elif ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)

    rank_emojis = {
        1: "🥇",
        2: "🥈",
        3: "🥉"
    }

    lines = []
    rank = start_rank
    for row in rows:
        member = ctx.guild.get_member(row["user_id"])
        mention_text = member.mention if member else f"<@{row['user_id']}>"
        tag_text = member.name if member else f"User ID: {row['user_id']}"
        present = row["present_count"] or 0
        absent = row["absent_count"] or 0
        excused = row["excused_count"] or 0

        rank_badge = rank_emojis.get(rank, f"`#{rank}`")
        lines.append(
            f"{rank_badge} {mention_text} • `{tag_text}`\n"
            f"↳ ✅ Present: **{present}** | ❌ Absent: **{absent}** | ⚠️ Excused: **{excused}**"
        )
        rank += 1

    embed.add_field(name="Top Members", value="\n\n".join(lines), inline=False)
    embed.add_field(
        name="How to Navigate",
        value=f"Use `!leaderboard <page>` • Page **{page}/{total_pages}**",
        inline=False
    )
    embed.set_footer(
        text=f"Registrar Bot • Total Members Ranked: {total_rows}",
        icon_url=bot.user.display_avatar.url if bot.user and bot.user.display_avatar else None
    )

    await ctx.send(embed=embed)

@bot.command(name='stick')
@commands.has_permissions(manage_messages=True)
async def stick_message(ctx, *, message_text: str):
    existing_sticky = sticky_channels.get(ctx.channel.id)
    if existing_sticky:
        await ctx.send(
            "This channel already has a sticky message. "
            "Use `!unstick` or `!removestick` first if you want to replace it.",
            delete_after=8
        )
        return
    msg = await ctx.send(message_text)
    sticky_channels[ctx.channel.id] = {
        "message_id": msg.id,
        "content": message_text
    }
    save_sticky_channels()

@bot.command(name='unstick', aliases=['removestick'])
@commands.has_permissions(manage_messages=True)
async def removestick_message(ctx):
    removed_sticky = sticky_channels.pop(ctx.channel.id, None)
    if removed_sticky:
        save_sticky_channels()
        await ctx.send("Sticky message removed for this channel.", delete_after=5)
    if ctx.message.reference and ctx.message.reference.resolved:
        target = ctx.message.reference.resolved
        try:
            if target.pinned:
                await target.unpin()
                return
        except discord.Forbidden:
            await ctx.send("I cannot unpin that message. Please check my permissions.", delete_after=5)
            return
    pins = await ctx.channel.pins()
    if not pins:
        await ctx.send("There are no pinned messages in this channel.", delete_after=5)
        return
    target = pins[0]
    try:
        await target.unpin()
    except discord.Forbidden:
        await ctx.send("I cannot unpin messages here. Please check my permissions.", delete_after=5)

@bot.hybrid_command(
    name='deleteallmessage',
    with_app_command=True,
    description="Delete every message in the current channel."
)
@commands.has_permissions(manage_messages=True)
@commands.guild_only()
async def delete_all_message(ctx):
    """
    Deletes all messages in the current channel (bot + user messages).
    Usage: !deleteallmessage
    Slash usage: /deleteallmessage
    """
    if not isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
        await ctx.send("❌ This command can only be used in server text channels.")
        return

    interaction_deferred = False

    # Fast path for regular text channels: clone channel and delete old one.
    # This is effectively instant, even for very large histories.
    if isinstance(ctx.channel, discord.TextChannel) and ctx.guild and ctx.guild.me.guild_permissions.manage_channels:
        if ctx.interaction:
            await ctx.defer(ephemeral=True)
            interaction_deferred = True

        old_channel = ctx.channel
        try:
            new_channel = await old_channel.clone(
                reason=f"Fast clear requested by {ctx.author} ({ctx.author.id})"
            )
            await new_channel.edit(position=old_channel.position)
            await old_channel.delete(reason=f"Fast clear requested by {ctx.author} ({ctx.author.id})")

            await new_channel.send(
                f"✅ Channel history was cleared instantly by {ctx.author.mention}."
            )

            if ctx.interaction:
                await ctx.followup.send(
                    f"✅ Done. Channel recreated as {new_channel.mention} and history cleared instantly.",
                    ephemeral=True
                )
            return
        except (discord.Forbidden, discord.HTTPException):
            # Fall back to message purge below if cloning/deletion fails.
            pass

    if ctx.interaction and not interaction_deferred:
        await ctx.defer(ephemeral=True)

    try:
        deleted = await ctx.channel.purge(limit=None)
        summary = f"✅ Deleted **{len(deleted)}** message(s) in {ctx.channel.mention}."
    except (discord.Forbidden, discord.HTTPException):
        summary = "⚠️ I couldn't clear all messages. Please check my permissions and try again."

    if ctx.interaction:
        await ctx.followup.send(summary, ephemeral=True)
        return

    try:
        await ctx.author.send(summary)
    except discord.Forbidden:
        await ctx.channel.send(summary, delete_after=8)


@bot.hybrid_command(
    name='vcmsgdelete',
    aliases=['vchatdeletemessage', 'vchanneldeletemessage'],
    with_app_command=True,
    description="Instantly delete messages from a voice/stage channel chat."
)
@app_commands.describe(
    channel="Voice or stage channel whose chat messages should be deleted (defaults to current channel).",
    amount="How many recent messages to delete (1-500, default 100)."
)
@commands.has_permissions(manage_messages=True)
@commands.guild_only()
async def vcmsgdelete(
    ctx,
    channel: discord.VoiceChannel | None = None,
    amount: app_commands.Range[int, 1, 500] = 100,
):
    target_channel = channel or ctx.channel
    if not isinstance(target_channel, (discord.VoiceChannel, discord.StageChannel)):
        await ctx.send("❌ Please select a voice/stage channel chat to clear.", ephemeral=bool(ctx.interaction))
        return

    if ctx.interaction:
        await ctx.defer(ephemeral=True)

    try:
        messages = [message async for message in target_channel.history(limit=amount, oldest_first=False)]
    except (discord.Forbidden, discord.HTTPException):
        summary = f"⚠️ I couldn't read chat history for {target_channel.mention}. Check my permissions."
        if ctx.interaction:
            await ctx.followup.send(summary, ephemeral=True)
        else:
            await ctx.send(summary)
        return

    # Delete concurrently to make clears feel instant for users.
    delete_tasks = [message.delete() for message in messages]
    results = await asyncio.gather(*delete_tasks, return_exceptions=True)
    deleted = sum(1 for result in results if not isinstance(result, Exception))
    failed = len(results) - deleted

    summary = (
        f"✅ Deleted **{deleted}** message(s) from {target_channel.mention} chat."
        + (f" ⚠️ Failed to delete **{failed}** message(s)." if failed else "")
    )
    if ctx.interaction:
        await ctx.followup.send(summary, ephemeral=True)
    else:
        await ctx.send(summary)


@bot.command(name='attendance')
async def view_attendance(ctx):
    """
    View the current attendance lists.
    Usage: !attendance
    """
    await refresh_attendance_report(ctx.guild, ctx.channel, force_update=True)

@assign_attendance_role.error
async def assign_role_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You do not have permission to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: `!assignrole @Role`")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid role. Please ping a valid role.")


async def _process_staff_attendance_reminders(guild: discord.Guild, data: dict):
    if not data.get("staff_attendance_enabled"):
        return

    open_signins = database.get_open_staff_signins(guild.id)
    if not open_signins:
        return

    now_utc = datetime.datetime.utcnow()
    now_ph = now_utc.replace(tzinfo=datetime.timezone.utc).astimezone(PH_TIMEZONE)
    log_channel_id = data.get("staff_attendance_log_channel_id")
    log_channel = guild.get_channel(log_channel_id) if log_channel_id else None

    for signin in open_signins:
        user_id = int(signin.get("user_id"))
        member = guild.get_member(user_id)
        if not member:
            continue

        sign_in_time = _parse_iso(signin.get("timestamp"))
        if not sign_in_time:
            continue

        elapsed = now_utc - sign_in_time
        elapsed_seconds = int(elapsed.total_seconds())

        if elapsed_seconds >= 11 * 3600:
            await _send_staff_duty_dm(
                member,
                "⏰ You're almost at 12 duty hours. Please make sure to **sign out** before your shift completes.",
                dedupe_key=f"11h-{sign_in_time.isoformat()}"
            )

        if elapsed_seconds >= 12 * 3600:
            sent = await _send_staff_duty_dm(
                member,
                "🚨 Your 12-hour duty window is up. Please **sign out now** and start a new duty cycle at **12:00 AM PH time**.",
                dedupe_key=f"12h-{sign_in_time.isoformat()}"
            )
            if log_channel and sent:
                await log_channel.send(
                    f"{member.mention} your 12-hour duty window is up. Please sign out before the next duty cycle.",
                    delete_after=120
                )

        if now_ph.hour == 23 and now_ph.minute >= 50:
            await _send_staff_duty_dm(
                member,
                "🌙 It's almost **11:59 PM PH time**. Please make sure to **sign out** before midnight.",
                dedupe_key=f"eod-{now_ph.date().isoformat()}"
            )

@tasks.loop(minutes=1)
async def check_attendance_expiry():
    await process_due_giveaways(bot)
    await process_due_polls(bot)
    await process_birthdays(bot)
    await process_agenda_reminders(bot)
    # Iterate over guilds first, then load data for each
    for guild in bot.guilds:
        try:
            settings = load_settings(guild.id)
            data = load_attendance_data(guild.id)
            await _process_staff_attendance_reminders(guild, data)
            await _process_anniversary_rewards(guild)

            # Hourly revive ping logic
            revive_ping_role_id = data.get('revive_ping_role_id')
            if revive_ping_role_id:
                current_hour_key = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
                if last_revive_ping_hour.get(guild.id) != current_hour_key:
                    revive_role = guild.get_role(revive_ping_role_id)
                    announce_channel = None
                    if data.get('revive_channel_id'):
                        announce_channel = guild.get_channel(data.get('revive_channel_id'))
                    if not announce_channel and data.get('report_channel_id'):
                        announce_channel = guild.get_channel(data.get('report_channel_id'))
                    if not announce_channel and data.get('welcome_channel_id'):
                        announce_channel = guild.get_channel(data.get('welcome_channel_id'))
                    if not announce_channel:
                        announce_channel = guild.system_channel

                    if revive_role and announce_channel:
                        embed = build_revive_style_embed(
                            title="Revive Chat",
                            body_lines=[
                                f"{revive_role.mention}",
                                "Let's revive the chat with !revivechat."
                            ],
                            guild=guild
                        )
                        await announce_channel.send(content=revive_role.mention, embed=embed)
                    last_revive_ping_hour[guild.id] = current_hour_key
            
            mode = settings.get('attendance_mode', 'duration')
            expiry_hours = settings.get("attendance_expiry_hours", 12)
            records = data.get('records', {})
            
            # --- End of Day / Session Logic (Window Mode) ---
            if mode == 'window':
                start_str = settings.get('window_start_time', '00:00')
                end_str = settings.get('window_end_time', '23:59')
                last_processed = settings.get('last_processed_date')
            
            try:
                now = get_current_ph_time()
                today_str = now.strftime("%Y-%m-%d")

                if mode != 'window':
                    continue

                ph_tz = now.tzinfo
                t_start = datetime.datetime.strptime(start_str, "%H:%M").time()
                t_end = datetime.datetime.strptime(end_str, "%H:%M").time()
                
                # Construct datetime objects for comparison (make them timezone-aware)
                dt_start = datetime.datetime.combine(now.date(), t_start).replace(tzinfo=ph_tz)
                dt_end = datetime.datetime.combine(now.date(), t_end).replace(tzinfo=ph_tz)
                
                if is_weekend_in_ph(now):
                    logger.debug(f"Skipping attendance automation for {guild.name} because it is a weekend in Philippines time.")
                    continue

                # --- START OF WINDOW LOGIC ---
                # Automatically post/refresh report when window opens
                last_opened = settings.get('last_opened_date')
                
                if now >= dt_start and now < dt_end:
                     if last_opened != today_str:
                         logger.info(f"Opening attendance window for {guild.name}")
                         
                         # Update state FIRST to prevent loops if refresh fails
                         settings['last_opened_date'] = today_str
                         save_settings(guild.id, settings)
                         
                         try:
                             await refresh_attendance_report(guild)
                         except Exception as e:
                             logger.error(f"Failed to refresh report on window open: {e}")

                         ping_role_id = data.get('ping_role_id')
                         if ping_role_id:
                             ping_role = guild.get_role(ping_role_id)
                             announce_channel = None
                             if data.get('report_channel_id'):
                                 announce_channel = guild.get_channel(data.get('report_channel_id'))
                             if not announce_channel and data.get('welcome_channel_id'):
                                 announce_channel = guild.get_channel(data.get('welcome_channel_id'))
                             if ping_role and announce_channel:
                                 await announce_channel.send(f"{ping_role.mention} Attendance is now **OPEN**.")
                
                target_date_to_process = None
                
                # Check 1: Post-Shift (Same Day)
                # If we are past the end time today
                if now > dt_end:
                    target_date_to_process = today_str
                    
                # Check 2: Pre-Shift (Next Day / Overnight)
                # If we are before the start time, we might need to close out yesterday
                # (Logic: If we haven't closed out yesterday, do it now)
                elif now < dt_start:
                    yesterday = now - datetime.timedelta(days=1)
                    target_date_to_process = yesterday.strftime("%Y-%m-%d")

                # Handle Cross-Midnight windows (Start > End, e.g. 22:00 to 06:00)
                # Not fully supported by this simple logic yet, but user asked for 6am-11:59pm
                
                if target_date_to_process and last_processed != target_date_to_process:
                    logger.info(f"Triggering End-of-Day for {guild.name} (Date: {target_date_to_process})")
                    
                    # 1. Auto-Absent Logic
                    allowed_role_id = data.get('allowed_role_id')
                    absent_role_id = data.get('absent_role_id')
                    
                    if allowed_role_id:
                        allowed_role = guild.get_role(allowed_role_id)
                        if allowed_role:
                            # Identify missing users
                            present_ids = set(records.keys())
                            missing_members = [m for m in allowed_role.members if str(m.id) not in present_ids and not m.bot]
                            
                            # Mark them absent
                            if missing_members:
                                absent_role = guild.get_role(absent_role_id) if absent_role_id else None
                                
                                for member in missing_members:
                                    # Add to records
                                    records[str(member.id)] = {
                                        "status": "absent",
                                        "timestamp": now.isoformat(),
                                        "reason": "Auto-marked at end of attendance window"
                                    }
                                    database.increment_status_count(guild.id, member.id, "absent")
                                    
                                    # Give absent role
                                    if absent_role:
                                        try:
                                            await member.add_roles(absent_role)
                                            await asyncio.sleep(0.3)
                                        except discord.Forbidden:
                                            pass
                                    
                                    # DM the user
                                    try:
                                        dm_embed = discord.Embed(
                                            title="Attendance Status: Absent",
                                            description=f"You have been marked **ABSENT** in **{guild.name}** because you did not check in within the attendance window.",
                                            color=discord.Color.red(),
                                            timestamp=now
                                        )
                                        
                                        date_str = now.strftime('%B %d, %Y')
                                        time_str = now.strftime('%I:%M %p')
                                        
                                        dm_embed.add_field(name="Date", value=date_str, inline=True)
                                        dm_embed.add_field(name="Time", value=time_str, inline=True)

                                        if guild.icon:
                                            dm_embed.set_author(name=guild.name, icon_url=guild.icon.url)
                                            dm_embed.set_thumbnail(url=guild.icon.url)
                                        else:
                                             dm_embed.set_author(name=guild.name)
                                        
                                        dm_embed.set_footer(text="Registrar Bot • Attendance System")

                                        await member.send(embed=dm_embed)
                                        await asyncio.sleep(0.5)
                                    except discord.Forbidden:
                                        pass # User has DMs blocked
                                            
                                logger.info(f"Marked {len(missing_members)} users as absent in {guild.name}")
                    else:
                        logger.warning(f"Cannot auto-mark absences for {guild.name}: No 'allowed_role' configured.")
                    
                    # 2. Generate and Post Report
                    # Save data first so embed is accurate
                    data['records'] = records
                    save_attendance_data(guild.id, data)
                    
                    await refresh_attendance_report(guild)

                    # 3. Reset/Clear Data ("Old attendance will be out")
                    # Remove 'present' roles
                    present_role_id = data.get('attendance_role_id')
                    if present_role_id:
                        role = guild.get_role(present_role_id)
                        if role:
                            for uid in list(records.keys()):
                                member = guild.get_member(int(uid))
                                if member and role in member.roles:
                                    try:
                                        await member.remove_roles(role)
                                        await asyncio.sleep(0.3)
                                    except: pass

                    # Clear Records
                    data['records'] = {}
                    
                    # Update Settings
                    settings['last_processed_date'] = target_date_to_process
                    save_settings(guild.id, settings)
                    save_attendance_data(guild.id, data)

                    ping_role_id = data.get('ping_role_id')
                    if ping_role_id:
                        ping_role = guild.get_role(ping_role_id)
                        announce_channel = None
                        if data.get('report_channel_id'):
                            announce_channel = guild.get_channel(data.get('report_channel_id'))
                        if not announce_channel and data.get('welcome_channel_id'):
                            announce_channel = guild.get_channel(data.get('welcome_channel_id'))
                        if ping_role and announce_channel:
                            await announce_channel.send(f"{ping_role.mention} Attendance is now **CLOSED**.")
                    
                    logger.info(f"Attendance reset complete for {guild.name}")
                    
            except ValueError as e:
                logger.error(f"Error parsing time settings for {guild.name}: {e}")
                
        # --- End of Window Logic ---
        except Exception as e:
            logger.error(f"Error in check_attendance_expiry loop for guild {guild.id}: {e}")
        
        # Determine if we should expire individual users (Duration Mode Only)
        # Window mode now handles bulk expiry/reset above.
        if mode == 'window':
            continue 

        # ... Existing Duration Mode Logic Below ...
        
        # Get all role IDs
        role_map = {
            'present': data.get('attendance_role_id'),
            'absent': data.get('absent_role_id'),
            'excused': data.get('excused_role_id')
        }
        ping_role_id = data.get('ping_role_id')
    
        now = datetime.datetime.now()
        users_to_remove = []
        users_to_update = {} 

        for user_id_str, info in records.items():
            # Handle migration/fallback
            if isinstance(info, str):
                info = {"status": "present", "timestamp": info, "channel_id": None}
            
            timestamp_str = info.get('timestamp')
            status = info.get('status', 'present')
            channel_id = info.get('channel_id')
            role_id = role_map.get(status)

            if not timestamp_str:
                users_to_remove.append(user_id_str)
                continue

            try:
                timestamp = datetime.datetime.fromisoformat(str(timestamp_str))
                should_expire = False
                
                if mode == 'window':
                    # In window mode, we expire if we are outside the window AND they are still present
                    # We assume if they are 'present', they haven't been expired yet.
                    if expire_all_present and status == 'present':
                        should_expire = True
                else:
                    # Duration mode
                    if now - timestamp > datetime.timedelta(hours=expiry_hours):
                        should_expire = True

                if should_expire:
                    user_id = int(user_id_str)
                    member = guild.get_member(user_id)
                    
                    # 1. Remove current role
                    if member and role_id:
                        role = guild.get_role(role_id)
                        if role and role in member.roles:
                            try:
                                await member.remove_roles(role)
                                logger.info(f"Removed {status} role from {member.name} (expired)")
                            except discord.Forbidden:
                                logger.warning(f"Failed to remove role from {member.name}: Missing Permissions")
                    
                    # 2. Determine Channel
                    channel = None
                    if channel_id:
                        channel = guild.get_channel(channel_id)
                    if not channel and data.get('welcome_channel_id'):
                        channel = guild.get_channel(data.get('welcome_channel_id'))

                    # 3. Handle Transitions
                    if status == 'present':
                        # Transition to ABSENT
                        absent_role_id = data.get('absent_role_id')
                        if absent_role_id:
                            absent_role = guild.get_role(absent_role_id)
                            if absent_role and member:
                                try:
                                    await member.add_roles(absent_role)
                                except: pass
                        
                        # Schedule update to 'absent'
                        users_to_update[user_id_str] = {
                            "status": "absent",
                            "timestamp": now.isoformat(), 
                            "channel_id": channel_id
                        }
                        database.increment_status_count(guild.id, user_id, "absent")

                        # Notify
                        if channel:
                            msg_content = f"{member.mention}, your attendance session has expired. You have been marked as Absent. You are now allowed to say present again."
                            if ping_role_id:
                                ping_role = guild.get_role(ping_role_id)
                                if ping_role:
                                    msg_content = f"{ping_role.mention} " + msg_content
                            await channel.send(msg_content)

                    else:
                        # For absent/excused, just remove the record
                        users_to_remove.append(user_id_str)
                                    
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing timestamp for user {user_id_str}: {e}")
                users_to_remove.append(user_id_str)

        # Apply Updates
        if users_to_update:
            for uid, new_record in users_to_update.items():
                data['records'][uid] = new_record
                
        # Apply Removals
        if users_to_remove:
            users_to_remove = list(set(users_to_remove))
            for uid in users_to_remove:
                if uid in data['records'] and uid not in users_to_update:
                    del data['records'][uid]
                    
        if users_to_update or users_to_remove:
            save_attendance_data(guild.id, data)


@tasks.loop(seconds=15)
async def lofi_watchdog():
    for guild in bot.guilds:
        try:
            state = get_lofi_state(guild.id)
            if not state.get("enabled"):
                continue
            await ensure_lofi_connected(guild.id)
        except Exception as e:
            logger.warning("Lofi watchdog error for guild %s: %s", guild.id, e)


@check_attendance_expiry.before_loop
async def before_check_attendance_expiry():
    await bot.wait_until_ready()


@lofi_watchdog.before_loop
async def before_lofi_watchdog():
    await bot.wait_until_ready()

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user.name}')
    logger.info('Bot is ready to auto-nickname users!')
    
    # Initialize Database
    try:
        database.init_db()
        await apply_persisted_presence()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        
    if not check_attendance_expiry.is_running():
        check_attendance_expiry.start()
    if not lofi_watchdog.is_running():
        lofi_watchdog.start()

    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} application commands.")
    except Exception as e:
        logger.error(f"Failed to sync application commands: {e}")
    # Also force per-guild sync so newly added commands (e.g. /agendaedit) appear immediately.
    for guild in bot.guilds:
        try:
            guild_synced = await bot.tree.sync(guild=guild)
            logger.info("Synced %s commands for guild %s (%s).", len(guild_synced), guild.name, guild.id)
        except Exception as e:
            logger.warning("Guild sync failed for %s (%s): %s", guild.name, guild.id, e)
    
    # Register persistent views
    bot.add_view(AttendanceView(bot))
    bot.add_view(BlindDateMatchView())
    bot.add_view(ConfessionActionView())
    bot.add_view(SuggestionActionView())
    bot.add_view(AgendaAttendanceView())

    global sticky_channels
    sticky_channels = load_sticky_channels()
    logger.info("Loaded %s sticky channel configuration(s) from %s", len(sticky_channels), STICKY_STATE_FILE)

    # Reconcile nicknames on startup so rules survive downtime/redeploys.
    for guild in bot.guilds:
        try:
            updated, skipped = await reconcile_autonicks_for_guild(guild)
            logger.info("Autonick startup reconcile for guild %s: updated=%s skipped=%s", guild.id, updated, skipped)
        except Exception as e:
            logger.warning("Autonick startup reconcile failed for guild %s: %s", guild.id, e)

# --- Persistent Views for Attendance ---

class ExcuseModal(discord.ui.Modal, title="Excuse Reason"):
    reason = discord.ui.TextInput(
        label="Reason for being excused",
        style=discord.TextStyle.paragraph,
        placeholder="e.g., I was sick...",
        required=True,
        max_length=200
    )

    def __init__(self, view_instance):
        super().__init__()
        self.view_instance = view_instance

    async def on_submit(self, interaction: discord.Interaction):
        await self.view_instance.handle_attendance(interaction, "excused", self.reason.value)
        # handle_attendance handles the response

class AttendanceView(discord.ui.View):
    def __init__(self, bot_instance):
        super().__init__(timeout=None) # Persistent view
        self.bot_instance = bot_instance

    @discord.ui.button(label="Mark Present", style=discord.ButtonStyle.success, custom_id="attendance_btn_present", emoji="✅")
    async def btn_present(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_attendance(interaction, "present")

    @discord.ui.button(label="Excused", style=discord.ButtonStyle.secondary, custom_id="attendance_btn_excused", emoji="⚠️")
    async def btn_excused(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check permission for admin only excuse is handled inside handle_attendance or here?
        # The modal should open first, then we check? Or check first?
        # Checking first is better UX.
        
        settings = load_settings(interaction.guild.id)
        if settings.get('require_admin_excuse', True) and not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("Only admins can mark users as excused.", ephemeral=True)
            return
            
        # Open Modal to get reason
        await interaction.response.send_modal(ExcuseModal(self))

    async def handle_attendance(self, interaction, status, reason=None):
        user = interaction.user
        
        # Check Window (present and excused are both blocked when closed)
        if status in ('present', 'excused'):
            allowed, msg = is_in_attendance_window(interaction.guild.id)
            if not allowed:
                 if status == 'excused':
                     msg = f"{msg} Excuse submissions are also closed once attendance time is over."
                 await interaction.response.send_message(msg, ephemeral=True)
                 return

        # Check self-marking setting (only for present)
        settings = load_settings(interaction.guild.id)
        if status == 'present' and not settings.get('allow_self_marking', True):
             await interaction.response.send_message("Self-marking is currently disabled.", ephemeral=True)
             return

        # Check permitted role
        data = load_attendance_data(interaction.guild.id)

        existing_status = has_conflicting_attendance_status(data.get('records'), user.id, status)
        if existing_status:
            await interaction.response.send_message(
                f"You are already marked as **{existing_status}** and cannot switch to **{status}** this session. Reset attendance before changing it.",
                ephemeral=True
            )
            return

        allowed_role_id = data.get('allowed_role_id')
        if allowed_role_id:
            allowed_role = interaction.guild.get_role(allowed_role_id)
            if allowed_role and allowed_role not in user.roles:
                await interaction.response.send_message(f"You need the {allowed_role.mention} role to use this.", ephemeral=True)
                return

        # If it's a modal submission (interaction.type == modal_submit), we don't need to defer usually if we reply quickly.
        # But process_status_update might take a moment.
        if not interaction.response.is_done():
             await interaction.response.defer(ephemeral=True)
        
        await self.process_status_update(interaction, user, status, reason)
        
        msg = f"Successfully marked as **{status.upper()}**!"
        if reason:
            msg += f"\nReason: {reason}"
        
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    async def process_status_update(self, interaction, member, status, reason=None):
        # Logic duplicated/adapted from update_user_status to avoid ctx dependency
        data = load_attendance_data(interaction.guild.id)
        present_role_id = data.get('attendance_role_id')
        absent_role_id = data.get('absent_role_id')
        excused_role_id = data.get('excused_role_id')
        
        target_role_id = None
        roles_to_remove = []
        
        if status == 'present':
            target_role_id = present_role_id
            if absent_role_id: roles_to_remove.append(absent_role_id)
            if excused_role_id: roles_to_remove.append(excused_role_id)
        elif status == 'excused':
            target_role_id = excused_role_id
            if present_role_id: roles_to_remove.append(present_role_id)
            if absent_role_id: roles_to_remove.append(absent_role_id)
            
        guild = interaction.guild
        
        # Remove roles
        for rid in roles_to_remove:
            role = guild.get_role(rid)
            if role and role in member.roles:
                try:
                    await member.remove_roles(role)
                except: pass

        # Add role
        if target_role_id:
            role = guild.get_role(target_role_id)
            if role:
                try:
                    await member.add_roles(role)
                except: pass
        
        # Save record
        user_id = str(member.id)
        if 'records' not in data:
            data['records'] = {}
        
        record = {
            "status": status,
            "timestamp": datetime.datetime.now().isoformat()
        }
        if reason:
            record["reason"] = reason
            
        data['records'][user_id] = record
        save_attendance_data(interaction.guild.id, data)
        if status in ('present', 'absent', 'excused'):
            database.increment_status_count(interaction.guild.id, member.id, status)

        # Update Report
        await refresh_attendance_report(interaction.guild, interaction.channel, force_update=True)

        # Send DM if present
        if status == 'present':
            try:
                embed = discord.Embed(
                    title="✅ Attendance Confirmed",
                    description="Your attendance has been checked successfully.",
                    color=discord.Color.gold()
                )
                if interaction.guild.icon:
                    embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
                    embed.set_thumbnail(url=interaction.guild.icon.url)
                else:
                    embed.set_author(name=interaction.guild.name)

                embed.add_field(name="Status", value="Present", inline=True)
                embed.add_field(name="Note", value="You will be notified once the 12-hour period has expired, after which you will be allowed to mark yourself as present again.", inline=False)
                embed.set_footer(text=f"Calvsbot • Server: {interaction.guild.name}")
                await member.send(embed=embed)
            except:
                pass

@bot.command(name='assignchannel')
@commands.has_permissions(administrator=True)
async def assign_report_channel(ctx, channel: Union[discord.TextChannel, str] = None):
    """
    Sets the channel where attendance reports will be sent.
    Usage: !assignchannel #channel-name
    Usage: !assignchannel remove (to disable reports)
    """
    if channel is None:
        await ctx.send("❌ Usage: `!assignchannel #channel` or `!assignchannel remove`")
        return

    try:
        data = load_attendance_data(ctx.guild.id)
        
        if isinstance(channel, str):
            if channel.lower() in ['remove', 'none', 'off', 'disable']:
                data['report_channel_id'] = None
                save_attendance_data(ctx.guild.id, data)
                await ctx.send("✅ Attendance reports have been **disabled**. No new reports will be sent.")
                return
            else:
                await ctx.send("❌ Invalid input. Please mention a channel (e.g., `#general`) or use `remove`.")
                return
                
        # If it's a TextChannel
        data['report_channel_id'] = channel.id
        save_attendance_data(ctx.guild.id, data)
        
        logger.info(f"Report channel set to {channel.name} ({channel.id}) for guild {ctx.guild.id}")
        await ctx.send(f"✅ Attendance reports will now be sent to {channel.mention}.")
        
        # Check setup completion
        await check_and_notify_setup_completion(ctx)
        
    except Exception as e:
        logger.error(f"Error assigning channel: {e}", exc_info=True)
        await ctx.send(f"❌ Failed to assign channel: {e}")


@bot.hybrid_command(
    name='languageassignchannel',
    aliases=['languangeassignchannel'],
    with_app_command=True,
    description="Set English-only and optional dual translation channels."
)
@commands.has_permissions(manage_channels=True)
async def assign_language_channel(
    ctx,
    english_channel: Union[discord.TextChannel, None] = None,
    dual_channel: Union[discord.TextChannel, None] = None
):
    """
    Routes automatic translation messages to dedicated channels.
    Usage: !languageassignchannel #english-channel #dual-channel
    Usage: !languageassignchannel #english-channel
    Usage: !languageassignchannel (to clear configured translation channels)
    Slash usage: /languageassignchannel [english_channel] [dual_channel]
    """
    data = load_attendance_data(ctx.guild.id)
    if english_channel is None and dual_channel is None:
        data['translation_channel_id'] = None
        data['translation_dual_channel_id'] = None
        save_attendance_data(ctx.guild.id, data)
        await ctx.send("✅ Translation channels cleared. Auto-translation will fallback to same-channel original + English output.")
        return

    if english_channel and dual_channel and english_channel.id == dual_channel.id:
        await ctx.send("❌ Please use two different channels for English-only and dual output.")
        return

    data['translation_channel_id'] = english_channel.id if english_channel else None
    data['translation_dual_channel_id'] = dual_channel.id if dual_channel else None
    save_attendance_data(ctx.guild.id, data)

    saved_english = english_channel.mention if english_channel else "Not set"
    saved_dual = dual_channel.mention if dual_channel else "Not set"
    await ctx.send(
        "✅ Translation routing updated.\n"
        f"• English-only channel: {saved_english}\n"
        f"• Original + English channel: {saved_dual}"
    )


@bot.hybrid_command(name='languagetoggle', with_app_command=True, description="Turn auto-translation on or off.")
@commands.has_permissions(manage_channels=True)
async def language_toggle(ctx, state: str):
    """
    Toggle the automatic translator.
    Usage: !languagetoggle on
    Usage: !languagetoggle off
    Slash usage: /languagetoggle [on/off]
    """
    normalized = (state or "").strip().lower()
    if normalized not in {"on", "off"}:
        await ctx.send("❌ Usage: `!languagetoggle on` or `!languagetoggle off`.")
        return

    data = load_attendance_data(ctx.guild.id)
    enabled = normalized == "on"
    data["translation_enabled"] = enabled
    save_attendance_data(ctx.guild.id, data)
    await ctx.send(f"✅ Auto-translation is now **{'enabled' if enabled else 'disabled'}**.")


@bot.tree.command(name="setuptranslator", description="Show auto-translator setup commands.")
@app_commands.default_permissions(manage_channels=True)
async def setuptranslator_slash(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🌐 Translator Setup",
        description=(
            "Use these commands to configure automatic translation:\n\n"
            "1. `/languageassignchannel [english_channel] [dual_channel]`\n"
            "   • Set an English-only output channel.\n"
            "   • Optionally set a dual output channel (original + English).\n"
            "   • Run with no channels to clear translation routing.\n\n"
            "2. `/languagetoggle [on/off]`\n"
            "   • Turn auto-translation on or off.\n"
            "   • Default state is **off** for new server configs."
        ),
        color=discord.Color.blurple()
    )
    embed.set_footer(text="Tip: You need Manage Channels permission to use translator setup commands.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@assign_language_channel.error
async def assign_language_channel_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You need `Manage Channels` permission to set the translation channel.")
    else:
        logger.error("Error setting translation channel in guild %s: %s", getattr(ctx.guild, "id", "unknown"), error, exc_info=True)
        await ctx.send("❌ Failed to update translation channel.")


@language_toggle.error
async def language_toggle_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You need `Manage Channels` permission to toggle auto-translation.")
    else:
        logger.error("Error toggling translation in guild %s: %s", getattr(ctx.guild, "id", "unknown"), error, exc_info=True)
        await ctx.send("❌ Failed to toggle auto-translation.")

@bot.command(name='removereport', aliases=['deletereport'])
@commands.has_permissions(administrator=True)
async def remove_last_report(ctx):
    """
    Deletes the currently active attendance report message.
    Usage: !removereport
    """
    data = load_attendance_data(ctx.guild.id)
    last_msg_id = data.get('last_report_message_id')
    last_chan_id = data.get('last_report_channel_id')
    
    if not last_msg_id or not last_chan_id:
        await ctx.send("⚠️ No active report found to remove.")
        return
        
    try:
        channel = ctx.guild.get_channel(last_chan_id)
        if channel:
            try:
                msg = await channel.fetch_message(last_msg_id)
                await msg.delete()
                await ctx.send("✅ Report removed.")
            except discord.NotFound:
                await ctx.send("⚠️ Report message not found (maybe already deleted).")
            except discord.Forbidden:
                await ctx.send("❌ I don't have permission to delete the report message.")
        else:
             await ctx.send("⚠️ Report channel no longer exists.")
             
        # Clear the record so it doesn't try to edit it later
        data['last_report_message_id'] = None
        data['last_report_channel_id'] = None
        save_attendance_data(ctx.guild.id, data)
        
    except Exception as e:
        logger.error(f"Error removing report: {e}")
        await ctx.send(f"❌ Error removing report: {e}")

@bot.command(name='setup_attendance')
@commands.has_permissions(administrator=True)
async def setup_attendance_ui(ctx):
    """Posts the persistent attendance buttons."""
    embed = discord.Embed(
        title="Attendance Check-In", 
        description="Click the button below to mark your attendance.", 
        color=discord.Color.green()
    )
    await ctx.send(embed=embed, view=AttendanceView(bot))

@bot.hybrid_command(name='setupattendance', with_app_command=True)
@commands.has_permissions(administrator=True)
@app_commands.describe(
    assignchannel="Channel where attendance reports will be posted",
    presentchannel="Channel where members are allowed to type present",
    presentrole="Role given to present members",
    absentrole="Role given to absent members",
    excuserole="Role given to excused members",
    pingrole="Optional role to ping when attendance opens/closes",
    setpermitrole="Role allowed to mark attendance",
    settime="Attendance window (example: 6am to 11:59pm)"
)
async def setup_attendance_bundle(
    ctx,
    assignchannel: discord.TextChannel,
    presentchannel: discord.TextChannel,
    presentrole: discord.Role,
    absentrole: discord.Role,
    excuserole: discord.Role,
    pingrole: discord.Role = None,
    setpermitrole: discord.Role = None,
    *,
    settime: str = None
):
    """
    One-shot attendance setup command.
    Slash usage: /setupattendance assignchannel:#... presentchannel:#... presentrole:@... absentrole:@... excuserole:@... [pingrole:@...] [setpermitrole:@...] [settime:6am to 11:59pm]
    Prefix usage: !setupattendance #assign #present @present @absent @excused [@ping] [@permit] settime="6am to 11:59pm"
    """
    data = load_attendance_data(ctx.guild.id)
    data['report_channel_id'] = assignchannel.id
    data['present_channel_id'] = presentchannel.id
    data['attendance_role_id'] = presentrole.id
    data['absent_role_id'] = absentrole.id
    data['excused_role_id'] = excuserole.id
    data['ping_role_id'] = pingrole.id if pingrole else None
    data['allowed_role_id'] = setpermitrole.id if setpermitrole else None
    save_attendance_data(ctx.guild.id, data)

    saved_window = None
    if settime:
        raw_input = settime.lower()
        parts = []
        if " to " in raw_input:
            parts = raw_input.split(" to ")
        elif "-" in raw_input:
            parts = raw_input.split("-")
        else:
            temp_parts = raw_input.split()
            if len(temp_parts) == 2:
                parts = temp_parts

        if len(parts) < 2:
            await ctx.send("❌ Invalid `settime` format. Use `6am to 11:59pm` or `08:00 - 17:00`.")
            return

        start_str = parts[0].strip()
        end_str = parts[1].strip()
        s_parsed = parse_time_input(start_str)
        e_parsed = parse_time_input(end_str)

        if not s_parsed or not e_parsed:
            await ctx.send(f"❌ Invalid `settime` values (`{start_str}` or `{end_str}`).")
            return

        settings = load_settings(ctx.guild.id)
        settings['attendance_mode'] = 'window'
        settings['window_start_time'] = s_parsed
        settings['window_end_time'] = e_parsed
        settings['last_processed_date'] = None
        save_settings(ctx.guild.id, settings)

        dt_start = datetime.datetime.strptime(s_parsed, "%H:%M")
        dt_end = datetime.datetime.strptime(e_parsed, "%H:%M")
        saved_window = f"{dt_start.strftime('%I:%M %p').lstrip('0')} - {dt_end.strftime('%I:%M %p').lstrip('0')}"

    embed = discord.Embed(
        title="✅ Attendance Setup Complete",
        description="Saved all requested attendance channels and roles.",
        color=discord.Color.green()
    )
    embed.add_field(name="Assign Channel", value=assignchannel.mention, inline=False)
    embed.add_field(name="Present Channel", value=presentchannel.mention, inline=False)
    embed.add_field(name="Present Role", value=presentrole.mention, inline=True)
    embed.add_field(name="Absent Role", value=absentrole.mention, inline=True)
    embed.add_field(name="Excuse Role", value=excuserole.mention, inline=True)
    embed.add_field(name="Ping Role", value=pingrole.mention if pingrole else "Not set", inline=False)
    embed.add_field(name="Permit Role", value=setpermitrole.mention if setpermitrole else "Not set", inline=False)
    if saved_window:
        embed.add_field(name="Attendance Window", value=saved_window, inline=False)
    embed.set_footer(text="Use /setupattendance again any time to update these values.")
    await ctx.send(embed=embed)

    await check_and_notify_setup_completion(ctx)


@bot.tree.command(name="setupanniversary", description="Configure anniversary role rewards and milestone years.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    assignrole="Role to give members on matching anniversary milestones",
    pingrole="Optional role to ping when posting anniversary announcements",
    date="Anniversary date in MM-DD-YYYY format (e.g. 05-17-2026)",
    message="Announcement text (supports {server_name}, {ordinal}, {years}, {user})",
    milestones="Milestone years list/ranges. Example: 1-50,60,75+",
    channel="Channel to post anniversary announcements"
)
async def setup_anniversary_slash(
    interaction: discord.Interaction,
    assignrole: discord.Role,
    date: str,
    channel: discord.TextChannel = None,
    pingrole: Union[discord.Role, None] = None,
    message: str = "Happy anniversary {server_name}!",
    milestones: str = "1-50",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    if not channel:
        await interaction.response.send_message("Please set `channel` so anniversary messages are published in the right place.", ephemeral=True)
        return
    try:
        anchor = datetime.datetime.strptime(date.strip(), "%m-%d-%Y")
    except ValueError:
        await interaction.response.send_message("Invalid date format. Use `MM-DD-YYYY` (example: `05-17-2026`).", ephemeral=True)
        return
    try:
        milestone_years = _parse_anniversary_milestones(milestones)
    except ValueError:
        await interaction.response.send_message("Invalid milestone format. Example: `1-50,60,75+`.", ephemeral=True)
        return
    if not milestone_years:
        await interaction.response.send_message("Please provide at least one milestone year.", ephemeral=True)
        return

    database.upsert_anniversary_config(
        interaction.guild.id,
        assignrole.id,
        anchor.strftime("%m-%d"),
        milestone_years,
        channel_id=channel.id,
        ping_role_id=pingrole.id if pingrole else None,
        message_template=message,
    )
    anniversary_daily_runs.pop(interaction.guild.id, None)
    preview = message.format(
        server_name=interaction.guild.name,
        years=1,
        ordinal="1st",
        user="@member",
    )
    embed = _anniversary_embed(
        interaction.guild,
        "✅ Anniversary setup saved",
        "Members will receive the configured role on matching join-date anniversaries.",
        color=discord.Color.green(),
    )
    embed.add_field(name="Assign Role", value=assignrole.mention, inline=False)
    embed.add_field(name="Ping Role", value=pingrole.mention if pingrole else "Not set", inline=False)
    embed.add_field(name="Anniversary Date", value=anchor.strftime("%B %d, %Y"), inline=True)
    embed.add_field(name="Announcement Channel", value=channel.mention, inline=True)
    embed.add_field(name="Milestones", value=", ".join(_format_ordinal(year) for year in milestone_years[:25]), inline=False)
    embed.add_field(name="Sample Message", value=preview[:1024], inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

    channel_embed = _anniversary_embed(
        interaction.guild,
        "📌 Anniversary Assignment Setup",
        "Anniversary reward configuration has been updated for this channel.",
        color=discord.Color.blurple(),
    )
    channel_embed.add_field(name="Milestones", value=", ".join(_format_ordinal(year) for year in milestone_years[:25]), inline=False)
    channel_embed.add_field(name="Message", value=preview[:1024], inline=False)
    channel_embed.add_field(name="Anniversary Date", value=anchor.strftime("%B %d, %Y"), inline=False)
    channel_embed.add_field(name="Reward", value=f"Assign Role: {assignrole.mention}", inline=False)
    if pingrole:
        channel_embed.add_field(name="Ping Role", value=pingrole.mention, inline=False)
    await channel.send(embed=channel_embed)


@bot.tree.command(name="setupstaffattendance", description="Configure message-based staff sign in/out attendance.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    log_channel="Channel where staff attendance logs will be posted",
    role_1="First allowed role (required)",
    role_2="Second allowed role (optional)",
    role_3="Third allowed role (optional)",
    cooldown_seconds="Cooldown per user per action in seconds (default: 300)",
    allowed_channel="Optional channel where sign in/out messages are accepted"
)
async def setup_staff_attendance_slash(
    interaction: discord.Interaction,
    log_channel: discord.TextChannel,
    role_1: discord.Role,
    role_2: Union[discord.Role, None] = None,
    role_3: Union[discord.Role, None] = None,
    cooldown_seconds: app_commands.Range[int, 0, 86400] = 300,
    allowed_channel: Union[discord.TextChannel, None] = None
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    unique_role_ids: list[int] = []
    for role in (role_1, role_2, role_3):
        if role and role.id not in unique_role_ids:
            unique_role_ids.append(role.id)

    data = load_attendance_data(interaction.guild.id)
    data["staff_attendance_enabled"] = True
    data["staff_attendance_allowed_role_ids"] = unique_role_ids
    data["staff_attendance_log_channel_id"] = log_channel.id
    data["staff_attendance_cooldown_seconds"] = int(cooldown_seconds)
    data["staff_attendance_channel_id"] = allowed_channel.id if allowed_channel else None
    save_attendance_data(interaction.guild.id, data)

    role_mentions = [f"<@&{role_id}>" for role_id in unique_role_ids]
    config_preview = {
        "enabled": True,
        "allowedRoles": role_mentions,
        "logChannelId": str(log_channel.id),
        "cooldownSeconds": int(cooldown_seconds),
    }
    if allowed_channel:
        config_preview["allowedChannelId"] = str(allowed_channel.id)

    embed = discord.Embed(
        title="✅ Staff Attendance Setup Complete",
        color=discord.Color.green(),
        description="Message commands enabled: `sign in` and `sign out` (case-insensitive)."
    )
    embed.add_field(name="Allowed Roles", value=", ".join(role_mentions), inline=False)
    embed.add_field(name="Log Channel", value=log_channel.mention, inline=True)
    embed.add_field(name="Cooldown", value=f"{cooldown_seconds}s", inline=True)
    embed.add_field(name="Allowed Channel", value=allowed_channel.mention if allowed_channel else "Any channel", inline=False)
    embed.add_field(
        name="Config Preview",
        value=f"```json\n{json.dumps(config_preview, indent=2)}\n```",
        inline=False
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="setupstafftracker", description="Configure automatic staff abuse tracking for timeout actions.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    staffrole="Primary staff role to monitor",
    logchannel="Channel where staff tracker logs are posted",
    punishment="Punishment mode (timeout, kick, or ban)",
    exempt_role_1="Optional exempt role #1",
    exempt_role_2="Optional exempt role #2",
)
@app_commands.choices(punishment=[
    app_commands.Choice(name="timeout", value="timeout"),
    app_commands.Choice(name="kick", value="kick"),
    app_commands.Choice(name="ban", value="ban"),
])
async def setup_staff_tracker_slash(
    interaction: discord.Interaction,
    staffrole: discord.Role,
    logchannel: discord.TextChannel,
    punishment: app_commands.Choice[str] | None = None,
    exempt_role_1: Union[discord.Role, None] = None,
    exempt_role_2: Union[discord.Role, None] = None,
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    monitored_role_ids = [staffrole.id]
    exempt_role_ids: list[int] = []
    for role in (exempt_role_1, exempt_role_2):
        if role and role.id not in exempt_role_ids:
            exempt_role_ids.append(role.id)

    mode = (punishment.value if punishment else "timeout").lower()
    data = load_attendance_data(interaction.guild.id)
    data["staff_tracker_enabled"] = True
    data["staff_tracker_role_ids"] = monitored_role_ids
    data["staff_tracker_exempt_role_ids"] = exempt_role_ids
    data["staff_tracker_log_channel_id"] = logchannel.id
    data["staff_tracker_punishment_mode"] = mode
    save_attendance_data(interaction.guild.id, data)

    embed = discord.Embed(
        title="✅ Staff Tracker Setup Complete",
        color=discord.Color.green(),
        description="Timeout abuse tracking is now enabled for configured staff roles."
    )
    embed.add_field(name="Monitored Role", value=staffrole.mention, inline=False)
    embed.add_field(name="Log Channel", value=logchannel.mention, inline=True)
    embed.add_field(name="Punishment Mode", value=mode, inline=True)
    embed.add_field(
        name="Exempt Roles",
        value=", ".join(f"<@&{rid}>" for rid in exempt_role_ids) if exempt_role_ids else "None",
        inline=False
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


def _parse_duration_to_timedelta(duration: str) -> datetime.timedelta | None:
    cleaned = duration.strip().lower()
    if not cleaned:
        return None

    unit_map = {
        "s": "seconds",
        "sec": "seconds",
        "secs": "seconds",
        "second": "seconds",
        "seconds": "seconds",
        "m": "minutes",
        "min": "minutes",
        "mins": "minutes",
        "minute": "minutes",
        "minutes": "minutes",
        "h": "hours",
        "hr": "hours",
        "hrs": "hours",
        "hour": "hours",
        "hours": "hours",
        "d": "days",
        "day": "days",
        "days": "days",
        "w": "weeks",
        "week": "weeks",
        "weeks": "weeks",
    }

    number = ""
    unit = ""
    for char in cleaned:
        if char.isdigit():
            number += char
            continue
        if not char.isspace():
            unit += char

    if not number or not unit:
        return None

    amount = int(number)
    resolved_unit = unit_map.get(unit)
    if amount <= 0 or not resolved_unit:
        return None

    return datetime.timedelta(**{resolved_unit: amount})


@bot.tree.command(name="timeout", description="Temporarily timeout a member.")
@app_commands.default_permissions(moderate_members=True)
@app_commands.describe(member="Member to timeout", duration="Duration (e.g. 10m, 2h, 1d)", reason="Reason for timeout")
async def timeout_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    duration: str,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    delta = _parse_duration_to_timedelta(duration)
    if not delta:
        await interaction.response.send_message("Invalid duration. Example formats: `10m`, `2h`, `1d`, `1w`.", ephemeral=True)
        return

    target_until = discord.utils.utcnow() + delta
    try:
        await member.timeout(target_until, reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't timeout that member. Check my permissions and role hierarchy.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"✅ {member.mention} has been timed out until <t:{int(target_until.timestamp())}:F>.\nReason: {reason}"
    )


@bot.tree.command(name="kick", description="Kick a member from the server.")
@app_commands.default_permissions(kick_members=True)
@app_commands.describe(member="Member to kick", reason="Reason for kick")
async def kick_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    try:
        await member.kick(reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't kick that member. Check my permissions and role hierarchy.", ephemeral=True)
        return

    await interaction.response.send_message(f"✅ Kicked {member.mention}.\nReason: {reason}")


@bot.tree.command(name="ban", description="Ban a member from the server.")
@app_commands.default_permissions(ban_members=True)
@app_commands.describe(member="Member to ban", delete_message_days="Delete message history days (0-7)", reason="Reason for ban")
async def ban_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    delete_message_days: app_commands.Range[int, 0, 7] = 0,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    try:
        await interaction.guild.ban(
            member,
            delete_message_days=int(delete_message_days),
            reason=f"{interaction.user}: {reason}",
        )
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't ban that member. Check my permissions and role hierarchy.", ephemeral=True)
        return

    await interaction.response.send_message(f"✅ Banned {member.mention}.\nReason: {reason}")


@bot.tree.command(name="warn", description="Warn a member without applying a punishment.")
@app_commands.default_permissions(moderate_members=True)
@app_commands.describe(member="Member to warn", reason="Reason for warning")
async def warn_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    dm_sent = False
    try:
        await member.send(
            f"⚠️ You were warned in **{interaction.guild.name}** by **{interaction.user}**.\n"
            f"Reason: {reason}"
        )
        dm_sent = True
    except discord.Forbidden:
        dm_sent = False

    await _send_configured_log_embed(
        interaction.guild,
        "moderation",
        title="⚠️ Member Warned",
        color=discord.Color.orange(),
        fields=[
            ("Member", f"{member.mention} (`{member.id}`)", False),
            ("Moderator", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
            ("Reason", reason, False),
            ("DM Sent", "Yes" if dm_sent else "No", True),
        ],
    )

    dm_note = "A DM was sent to the member." if dm_sent else "I couldn't DM that member."
    await interaction.response.send_message(
        f"⚠️ Warned {member.mention}.\nReason: {reason}\n{dm_note}"
    )


@bot.tree.command(name="mute", description="Mute a member using a server mute role.")
@app_commands.default_permissions(manage_roles=True)
@app_commands.describe(member="Member to mute", reason="Reason for mute")
async def mute_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False, thinking=True)

    mute_role = discord.utils.get(interaction.guild.roles, name="Muted")
    if not mute_role:
        try:
            mute_role = await interaction.guild.create_role(
                name="Muted",
                colour=discord.Color.dark_grey(),
                reason=f"Created by {interaction.user} via /mute",
                mentionable=False,
            )
        except discord.Forbidden:
            await interaction.followup.send("I couldn't create the `Muted` role. Check my permissions and role hierarchy.")
            return

    hell_channel = discord.utils.get(interaction.guild.text_channels, name="hell")
    if not hell_channel:
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            ),
            mute_role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                add_reactions=True,
                send_messages_in_threads=True,
            ),
            interaction.guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
                manage_messages=True,
            ),
        }
        try:
            hell_channel = await interaction.guild.create_text_channel(
                name="hell",
                overwrites=overwrites,
                reason=f"Created by {interaction.user} via /mute",
            )
        except discord.Forbidden:
            hell_channel = None

    for channel in interaction.guild.channels:
        if hell_channel and channel.id == hell_channel.id:
            continue
        try:
            await channel.set_permissions(
                mute_role,
                view_channel=False,
                read_message_history=False,
                send_messages=False,
                add_reactions=False,
                connect=False,
                speak=False,
                send_messages_in_threads=False,
            )
        except (discord.Forbidden, discord.HTTPException):
            continue

    if hell_channel:
        try:
            await hell_channel.set_permissions(
                mute_role,
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                add_reactions=True,
                send_messages_in_threads=True,
                connect=False,
                speak=False,
            )
        except discord.Forbidden:
            pass

    try:
        await member.add_roles(mute_role, reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.followup.send("I couldn't assign the mute role. Check my permissions and role hierarchy.")
        return

    if hell_channel:
        try:
            await hell_channel.send(f"{member.mention} You are muted and welcome to hell!")
        except (discord.Forbidden, discord.HTTPException):
            pass

    muted_channel_note = "They can only chat in #hell while muted.\n" if hell_channel else ""
    await interaction.followup.send(
        f"🔇 {member.mention} has been muted.\n"
        "They can no longer chat in normal channels, join voice calls, or view regular channel history.\n"
        f"{muted_channel_note}"
        f"Reason: {reason}",
    )


@bot.tree.command(name="unmute", description="Unmute a member by removing the server mute role.")
@app_commands.default_permissions(manage_roles=True)
@app_commands.describe(member="Member to unmute", reason="Reason for unmute")
async def unmute_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    mute_role = discord.utils.get(interaction.guild.roles, name="Muted")
    if not mute_role:
        await interaction.response.send_message("There is no `Muted` role in this server yet.", ephemeral=True)
        return

    if mute_role not in member.roles:
        await interaction.response.send_message(f"{member.mention} is not muted.", ephemeral=True)
        return

    try:
        await member.remove_roles(mute_role, reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't remove the mute role. Check my permissions and role hierarchy.", ephemeral=True)
        return

    await interaction.response.send_message(f"🔊 {member.mention} has been unmuted.\nReason: {reason}")


role_group = app_commands.Group(name="role", description="Manage member roles.")


@role_group.command(name="add", description="Add a role to a member.")
@app_commands.describe(member="Member to update", role="Role to add", reason="Reason for role update")
async def role_add_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message("You need Manage Roles permission to use this command.", ephemeral=True)
        return
    try:
        await member.add_roles(role, reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't add that role. Check my permissions and role hierarchy.", ephemeral=True)
        return
    await interaction.response.send_message(f"✅ Added {role.mention} to {member.mention}.")


@role_group.command(name="remove", description="Remove a role from a member.")
@app_commands.describe(member="Member to update", role="Role to remove", reason="Reason for role update")
async def role_remove_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message("You need Manage Roles permission to use this command.", ephemeral=True)
        return
    try:
        await member.remove_roles(role, reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't remove that role. Check my permissions and role hierarchy.", ephemeral=True)
        return
    await interaction.response.send_message(f"✅ Removed {role.mention} from {member.mention}.")


@role_group.command(name="temp", description="Temporarily add a role to a member.")
@app_commands.describe(member="Member to update", role="Role to add temporarily", duration="Duration (e.g. 30m, 12h, 2d)", reason="Reason for role update")
async def role_temp_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
    duration: str,
    reason: str = "No reason provided",
):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message("You need Manage Roles permission to use this command.", ephemeral=True)
        return

    delta = _parse_duration_to_timedelta(duration)
    if not delta:
        await interaction.response.send_message("Invalid duration. Example formats: `30m`, `12h`, `2d`, `1w`.", ephemeral=True)
        return

    try:
        await member.add_roles(role, reason=f"{interaction.user}: {reason}")
    except discord.Forbidden:
        await interaction.response.send_message("I couldn't add that role. Check my permissions and role hierarchy.", ephemeral=True)
        return

    remove_at = discord.utils.utcnow() + delta
    await interaction.response.send_message(
        f"✅ Added {role.mention} to {member.mention} until <t:{int(remove_at.timestamp())}:F>."
    )

    async def _remove_later():
        await asyncio.sleep(int(delta.total_seconds()))
        guild = bot.get_guild(interaction.guild.id) if interaction.guild else None
        if not guild:
            return
        refreshed_member = guild.get_member(member.id)
        refreshed_role = guild.get_role(role.id)
        if not refreshed_member or not refreshed_role or refreshed_role not in refreshed_member.roles:
            return
        try:
            await refreshed_member.remove_roles(refreshed_role, reason=f"Temporary role expired ({duration})")
        except discord.Forbidden:
            logger.warning("Failed to remove temporary role %s from %s due to missing permissions.", role.id, member.id)

    bot.loop.create_task(_remove_later())


def _classic_server_roles() -> list[str]:
    roles = [
        "Owner", "Co-Owner", "Founder", "Co-Founder", "Head Admin", "Admin Director", "Senior Admin", "Admin", "Junior Admin", "Trial Admin",
        "Head Moderator", "Senior Moderator", "Moderator", "Junior Moderator", "Trial Moderator", "Chat Moderator", "Voice Moderator", "Community Moderator", "Safety Moderator", "Discipline Moderator",
        "Head Support", "Senior Support", "Support", "Junior Support", "Trial Support", "Help Desk", "Ticket Support", "Customer Support", "Community Helper", "Assistant",
        "Verified Member", "Active Member", "Member", "New Member", "Beginner", "Rookie", "Learner", "Starter", "Explorer", "Visitor",
        "Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Level 6", "Level 7", "Level 8", "Level 9", "Level 10",
    ]
    roles.extend([f"Bronze Member {i}" for i in range(1, 11)])
    roles.extend([f"Silver Member {i}" for i in range(1, 11)])
    roles.extend([f"Gold Member {i}" for i in range(1, 11)])
    roles.extend([f"Platinum Member {i}" for i in range(1, 11)])
    roles.extend([f"Diamond Member {i}" for i in range(1, 11)])
    return roles


def _pro_roles() -> list[str]:
    roles = [
        "Director", "Co-Director", "Executive Chief", "Operations Head", "Strategic Lead", "Senior Manager", "Manager", "Junior Manager", "Supervisor", "Coordinator",
        "Lead Enforcer", "Senior Enforcer", "Enforcer", "Junior Enforcer", "Trial Enforcer", "Compliance Officer", "Control Officer", "Regulation Staff", "Security Staff", "Watcher",
        "Lead Specialist", "Senior Specialist", "Specialist", "Junior Specialist", "Technician", "Analyst", "Advisor", "Consultant", "Assistant Specialist", "Support Specialist",
        "Skilled", "Advanced", "Expert", "Professional", "Elite", "Master", "Grandmaster", "Champion", "Apex", "Dominator",
    ]
    roles.extend([f"Tier {i}" for i in range(1, 21)])
    roles.extend([f"Rank {chr(65 + i)}" for i in range(20)])
    roles.extend(["Alpha Tier", "Beta Tier", "Gamma Tier", "Delta Tier"])
    roles.extend(["Bronze", "Silver", "Gold", "Platinum", "Diamond", "Immortal", "Radiant", "Mythic"])
    roles.extend([f"Ascendant {i}" for i in range(1, 9)])
    return roles


def _international_roles() -> list[str]:
    roles = [
        "President", "Vice President", "Prime Minister", "Deputy Leader", "Secretary General", "Council Head", "Senior Ambassador", "Ambassador General", "Ambassador", "Junior Ambassador",
        "Envoy Chief", "Envoy", "Delegate", "Representative", "Attaché", "Cultural Officer", "Relations Officer", "Policy Officer", "Foreign Staff", "Liaison",
        "Asia Representative", "Europe Representative", "Africa Representative", "Americas Representative", "Oceania Representative",
        "Southeast Asia", "East Asia", "South Asia", "Central Asia", "Middle East",
        "Northern Europe", "Western Europe", "Eastern Europe", "Southern Europe", "North Africa",
        "West Africa", "East Africa", "Central Africa", "Southern Africa", "North America",
        "Central America", "South America", "Caribbean", "Australia Region", "Pacific Islands",
        "Scandinavia", "Baltics", "Balkans", "Caucasus", "Arctic Council",
    ]
    roles.extend([
        "Philippines", "Japan", "South Korea", "United States", "United Kingdom", "Canada", "India", "Australia", "Germany", "France",
        "Italy", "Spain", "Netherlands", "Belgium", "Sweden", "Norway", "Denmark", "Finland", "Poland", "Portugal",
        "Switzerland", "Austria", "Ireland", "Czech Republic", "Greece", "Turkey", "Saudi Arabia", "United Arab Emirates", "Qatar", "Kuwait",
        "Egypt", "South Africa", "Nigeria", "Kenya", "Morocco", "Brazil", "Argentina", "Mexico", "Chile", "Colombia",
        "New Zealand", "Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam", "Pakistan", "Bangladesh", "Nepal", "Sri Lanka",
    ])
    return roles


def _worldwide_roles() -> list[str]:
    roles = [
        "Supreme Leader", "Eternal Leader", "Global Chief", "World Architect", "Universal Overseer", "Infinite Commander", "Cosmic Director", "Prime Authority", "Absolute Ruler", "Apex Sovereign",
        "High Overseer", "Grand Overseer", "World Overseer", "Global Guardian", "Elite Guardian", "Prime Guardian", "Sentinel Prime", "Watcher Elite", "Dimension Keeper", "Reality Keeper",
        "World Class", "Global Elite", "Universal Elite", "Supreme Elite", "Infinite Elite", "Legendary", "Mythic", "Immortal", "Eternal", "Transcendent",
        "Celestial", "Astral", "Nova", "Stellar", "Nebula", "Quantum", "Paragon", "Ascendant", "Dominion", "Zenith",
        "Omniscient", "Omnipotent", "Chrono", "Voidwalker", "Skybreaker", "Starforged", "Aether", "Primal", "Exalted", "Titan",
    ]
    roles.extend([f"Planetary {r}" for r in ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]])
    roles.extend([f"Galactic {r}" for r in ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]])
    roles.extend([f"Universal {r}" for r in ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]])
    roles.extend([f"Multiversal {r}" for r in ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]])
    roles.extend([f"Infinite {r}" for r in ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]])
    return roles


ROLE_CATEGORY_BUILDERS = {
    "classic": ("Classic Server", _classic_server_roles),
    "pro": ("PRO", _pro_roles),
    "international": ("INTERNATIONAL", _international_roles),
    "worldwide": ("WORLDWIDE", _worldwide_roles),
}


def _role_style(name: str, index: int, base_member_permissions: discord.Permissions, muted_permissions: discord.Permissions, mod_permissions: discord.Permissions, admin_permissions: discord.Permissions, manager_permissions: discord.Permissions):
    lowered = name.lower()
    if index == 0 or "owner" in lowered or "founder" in lowered or "supreme" in lowered or "president" in lowered:
        return discord.Color.gold(), discord.Permissions(administrator=True), True
    if any(k in lowered for k in ["admin", "director", "chief", "prime minister", "secretary", "manager", "strategic", "global chief", "world architect"]):
        return discord.Color.red(), admin_permissions, True
    if any(k in lowered for k in ["moderator", "enforcer", "guardian", "overseer", "envoy", "ambassador", "support", "specialist", "officer", "liaison"]):
        return discord.Color.blurple(), mod_permissions, True
    if "muted" in lowered or "blacklisted" in lowered:
        return discord.Color.dark_grey(), muted_permissions, False
    if "level" in lowered or "tier" in lowered or "rank" in lowered or "planetary" in lowered or "galactic" in lowered or "universal" in lowered or "multiversal" in lowered or "infinite" in lowered:
        return discord.Color.purple(), base_member_permissions, False
    if any(k in lowered for k in ["helper", "assistant", "visitor", "member", "beginner", "rookie", "learner", "starter", "explorer"]):
        return discord.Color.green(), base_member_permissions, False
    return discord.Color.orange(), manager_permissions, True


@bot.tree.command(name="setuprole", description="Create a starter role structure for one selected category.")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(
    category=[
        app_commands.Choice(name="Classic Server", value="classic"),
        app_commands.Choice(name="PRO", value="pro"),
        app_commands.Choice(name="INTERNATIONAL", value="international"),
        app_commands.Choice(name="WORLDWIDE", value="worldwide"),
    ]
)
async def setup_role_slash(interaction: discord.Interaction, category: app_commands.Choice[str]):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)

    base_member_permissions = discord.Permissions(
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        connect=True,
        speak=True,
        use_voice_activation=True,
        add_reactions=True,
        embed_links=True,
        attach_files=True,
    )
    muted_permissions = discord.Permissions(
        view_channel=True,
        read_message_history=True,
        connect=True,
    )
    mod_permissions = discord.Permissions(
        manage_messages=True,
        kick_members=True,
        moderate_members=True,
        manage_nicknames=True,
        view_audit_log=True,
        mute_members=True,
        deafen_members=True,
        move_members=True,
    )
    admin_permissions = discord.Permissions(
        manage_guild=True,
        manage_channels=True,
        manage_roles=True,
        manage_messages=True,
        manage_nicknames=True,
        kick_members=True,
        ban_members=True,
        moderate_members=True,
        view_audit_log=True,
        mention_everyone=True,
    )
    manager_permissions = discord.Permissions(
        manage_channels=True,
        manage_roles=True,
        manage_messages=True,
        manage_threads=True,
        view_audit_log=True,
    )

    _, role_builder = ROLE_CATEGORY_BUILDERS[category.value]
    role_names = role_builder()
    role_configs = []
    for idx, role_name in enumerate(role_names):
        color, permissions, hoist = _role_style(
            role_name,
            idx,
            base_member_permissions,
            muted_permissions,
            mod_permissions,
            admin_permissions,
            manager_permissions,
        )
        role_configs.append((role_name, color, permissions, hoist))

    created = []
    existing = []
    try:
        # Create from lowest -> highest so the final hierarchy keeps top roles (e.g. Owner) above lower ranks.
        for name, color, permissions, hoist in reversed(role_configs):
            found = discord.utils.get(interaction.guild.roles, name=name)
            if found:
                existing.append(found.mention)
                continue
            role = await interaction.guild.create_role(
                name=name,
                colour=color,
                permissions=permissions,
                hoist=hoist,
                mentionable=True,
                reason=f"Created by {interaction.user} via /setuprole",
            )
            created.append(role.mention)
    except discord.Forbidden:
        await interaction.followup.send(
            "I couldn't create one or more roles. Please ensure I have **Manage Roles** and my highest role is above the roles being created.",
            ephemeral=True,
        )
        return

    # Force final hierarchy ordering so higher ranks stay above lower ranks, including pre-existing roles.
    me = interaction.guild.me or interaction.guild.get_member(bot.user.id) if bot.user else None
    bot_top_position = me.top_role.position if me else max((r.position for r in interaction.guild.roles), default=1)
    target_position = max(1, bot_top_position - 1)
    ordered_roles = [discord.utils.get(interaction.guild.roles, name=role_name) for role_name in role_names]
    ordered_roles = [role for role in ordered_roles if role is not None and role.position < bot_top_position]
    role_positions = {}
    for role in ordered_roles:
        role_positions[role] = target_position
        target_position = max(1, target_position - 1)
    if role_positions:
        try:
            await interaction.guild.edit_role_positions(positions=role_positions, reason=f"Sorted by {interaction.user} via /setuprole")
        except discord.Forbidden:
            await interaction.followup.send(
                "⚠️ Roles were created, but I couldn't reorder hierarchy. Ensure my top role is above all setup roles.",
                ephemeral=True,
            )
            return

    lines = []
    if created:
        lines.append("✅ Created roles: " + ", ".join(reversed(created)))
    if existing:
        lines.append("ℹ️ Already existed: " + ", ".join(existing))
    if not lines:
        lines.append("No role changes were required.")

    selected_label, _ = ROLE_CATEGORY_BUILDERS[category.value]
    lines.append(f"Category selected: **{selected_label}** ({len(role_names)} roles).")
    lines.append("Only this selected category was processed.")
    lines.append("These roles are set to display separately (where applicable).")
    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(name="verifyrole", description="Post a verification reaction-role message and create the verify role if needed.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(channel="Channel where the verification message will be posted (defaults to current channel)")
async def verifyrole_slash(interaction: discord.Interaction, channel: discord.TextChannel | None = None):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used inside a server.", ephemeral=True)
        return

    target_channel = channel or interaction.channel
    if not isinstance(target_channel, discord.TextChannel):
        await interaction.response.send_message(
            "❌ Please run this in a text channel or provide a text channel.",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    verify_role = discord.utils.get(guild.roles, name="verify")
    if verify_role is None:
        try:
            verify_role = await guild.create_role(
                name="verify",
                reason=f"Requested by {interaction.user} via /verifyrole"
            )
        except discord.Forbidden:
            await interaction.followup.send("❌ I don't have permission to create the `verify` role.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.followup.send("❌ Discord returned an error while creating the `verify` role.", ephemeral=True)
            return

    embed = discord.Embed(
        title="VERIFY HERE!",
        description=(
            "Hi there ! 👋\n"
            "To join the conversations in our server, please verify yourself first.\n\n"
            "Head over to #verify-here\n"
            "✅React to the message to get full access"
        ),
        color=discord.Color.red(),
    )

    try:
        message = await target_channel.send(embed=embed)
        await message.add_reaction("✅")
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I don't have permission to send messages or add reactions in that channel.",
            ephemeral=True
        )
        return
    except discord.HTTPException:
        await interaction.followup.send("❌ Failed to post the verification panel. Please try again.", ephemeral=True)
        return

    await interaction.followup.send(
        f"✅ Verification panel sent in {target_channel.mention}. Reacting with ✅ now gives {verify_role.mention}.",
        ephemeral=True
    )


async def handle_verify_reaction_role(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id == bot.user.id:
        return
    if str(payload.emoji) != "✅":
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    verify_role = discord.utils.get(guild.roles, name="verify")
    if verify_role is None:
        return

    channel = guild.get_channel(payload.channel_id)
    if not isinstance(channel, discord.TextChannel):
        return

    try:
        message = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return

    if message.author.id != bot.user.id or not message.embeds:
        return

    title = (message.embeds[0].title or "").strip()
    if title.upper() != "VERIFY HERE!":
        return

    try:
        member = guild.get_member(payload.user_id) or await guild.fetch_member(payload.user_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return

    if member.bot or verify_role in member.roles:
        return

    try:
        await member.add_roles(verify_role, reason="Verification reaction role")
    except (discord.Forbidden, discord.HTTPException):
        return


bot.tree.add_command(role_group)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Global slash-command error handler to avoid silent interaction failures."""
    if isinstance(error, app_commands.CommandInvokeError) and error.original:
        actual_error = error.original
    else:
        actual_error = error

    if isinstance(actual_error, app_commands.MissingPermissions):
        message = "❌ You do not have permission to use this slash command."
    elif isinstance(actual_error, app_commands.CheckFailure):
        message = "❌ You can't use this command in the current context."
    elif isinstance(actual_error, app_commands.CommandOnCooldown):
        message = f"⏳ This command is on cooldown. Try again in `{actual_error.retry_after:.1f}s`."
    else:
        message = "❌ Something went wrong while running this slash command. Please try again."
        logger.error(
            "Slash command error for %s by %s (%s): %s",
            interaction.command.qualified_name if interaction.command else "unknown",
            interaction.user,
            interaction.user.id if interaction.user else "unknown",
            actual_error,
            exc_info=True,
        )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.InteractionResponded:
        # A response may complete between `is_done()` and `send_message()` due to race conditions.
        try:
            await interaction.followup.send(message, ephemeral=True)
        except discord.HTTPException:
            logger.warning("Failed to send slash command error follow-up response: %s", message)
    except discord.NotFound:
        # Interaction token may have expired before we could send an error message.
        logger.warning("Failed to send slash command error response because the interaction expired.")
    except discord.HTTPException:
        logger.warning("Failed to send slash command error response: %s", message)


@bot.event
async def on_command_error(ctx, error):
    """Global error handler to catch and report errors to the user."""
    if isinstance(error, commands.CommandNotFound):
        # Ignore unknown commands
        return
        
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You do not have permission to use this command.")
        return
        
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing required argument. Usage: `{ctx.prefix}{ctx.command.name} {ctx.command.signature}`")
        return
        
    if isinstance(error, commands.BadArgument):
        await ctx.send("❌ Invalid argument provided. Please check your input.")
        return
    
    # Log the full error
    logger.error(f"Command error in {ctx.command}: {error}", exc_info=True)
    await ctx.send(f"❌ An error occurred while executing the command: `{error}`")

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    try:
        await handle_giveaway_reaction(payload, bot)
    except Exception as e:
        logger.warning("Giveaway reaction validation failed: %s", e)
    try:
        await handle_verify_reaction_role(payload)
    except Exception as e:
        logger.warning("Verify reaction role handling failed: %s", e)


@bot.event
async def on_message_delete(message: discord.Message):
    if not message.guild or message.author.bot:
        return

    attachments = [attachment.url for attachment in message.attachments if attachment.url]
    await _send_deleted_message_log(
        guild=message.guild,
        channel_id=message.channel.id if message.channel else None,
        message_id=message.id,
        author=message.author,
        content=message.content,
        attachment_urls=attachments,
    )


@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    if payload.cached_message is not None:
        return
    if payload.guild_id is None:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    await _send_deleted_message_log(
        guild=guild,
        channel_id=payload.channel_id,
        message_id=payload.message_id,
        author=None,
        content="(Message not in cache, so content/author unavailable)",
        attachment_urls=[],
    )


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if not after.guild or after.author.bot:
        return
    if before.content == after.content:
        return

    await _send_configured_log_embed(
        after.guild,
        "updated",
        title="✏️ Message Edited",
        color=discord.Color.gold(),
        fields=[
            ("Author", f"{after.author.mention} (`{after.author.id}`)", False),
            ("Channel", after.channel.mention if isinstance(after.channel, discord.TextChannel) else "Unknown", False),
            ("Before", _truncate_for_embed(before.content or "No text content"), False),
            ("After", _truncate_for_embed(after.content or "No text content"), False),
        ],
    )


@bot.event
async def on_guild_update(before: discord.Guild, after: discord.Guild):
    changes: list[tuple[str, str, bool]] = []
    if before.name != after.name:
        changes.append(("Name", f"`{before.name}` → `{after.name}`", False))

    before_icon = before.icon.url if before.icon else None
    after_icon = after.icon.url if after.icon else None
    if before_icon != after_icon:
        changes.append(("Server Icon", "[updated]", False))
        if after_icon:
            changes.append(("New Icon URL", after_icon, False))

    if not changes:
        return

    await _send_configured_log_embed(
        after,
        "server_updates",
        title="🛠️ Server Updated",
        color=discord.Color.blue(),
        fields=changes,
    )


@bot.event
async def on_message(message):
    # Don't let the bot reply to itself
    if message.author == bot.user:
        return
    if not message.guild:
        await bot.process_commands(message)
        return
    if message.author.bot:
        return

    guild_config = database.get_guild_config(message.guild.id) or {}
    automod_enabled = bool(guild_config.get("automod_enabled"))

    if automod_enabled:
        message_content = message.content or ""
        message_links = _extract_message_links(message_content)
        contains_invite_link = bool(INVITE_LINK_REGEX.search(message_content))
        contains_unknown_link = any(not _is_known_safe_link(link) for link in message_links)
        contains_bad_word = _contains_bad_word(message_content)
        contains_spam_burst = _is_spam_burst(message.guild.id, message.author.id)

        if (contains_invite_link or contains_unknown_link or contains_bad_word or contains_spam_burst) and not message.author.guild_permissions.manage_messages:
            try:
                await message.delete()
            except (discord.Forbidden, discord.HTTPException):
                pass

            if contains_invite_link:
                violation_type = "invite link"
                violation_label = "Invite link"
            elif contains_unknown_link:
                violation_type = "unknown link"
                violation_label = "Unknown link"
            elif contains_bad_word:
                violation_type = "bad words"
                violation_label = "Bad words"
            else:
                violation_type = "message spam"
                violation_label = f"Spam burst (≥{AUTOMOD_SPAM_THRESHOLD} msgs/{AUTOMOD_SPAM_WINDOW_SECONDS}s)"

            await message.channel.send(
                f"🚫 {message.author.mention}, {violation_type} are not allowed here.",
                delete_after=6
            )
            await _send_configured_log_embed(
                message.guild,
                "automod",
                title="🛡️ AutoMod Link Removal",
                color=discord.Color.orange(),
                fields=[
                    ("User", f"{message.author.mention} (`{message.author.id}`)", False),
                    ("Channel", message.channel.mention if isinstance(message.channel, discord.TextChannel) else "Unknown", False),
                    ("Violation", violation_label, False),
                    ("Content", _truncate_for_embed(message.content or "No text content"), False),
                ],
            )
            return

    if await _clear_member_afk(message.author):
        await message.channel.send(f"👋 Welcome back {message.author.mention}, I removed your AFK status.")

    if message.mentions:
        notified = set()
        afk_lines = []
        for member in message.mentions:
            if member.bot or member.id == message.author.id or member.id in notified:
                continue
            afk_data = _get_member_afk(member)
            if not afk_data:
                continue
            reason = afk_data.get("reason") or "AFK"
            afk_lines.append(f"💤 **{member.display_name}** is AFK: {reason}")
            notified.add(member.id)
        if afk_lines:
            await message.channel.send("\n".join(afk_lines))

    if message.guild:
        await _send_configured_log_embed(
            message.guild,
            "posts",
            title="📝 New Message",
            color=discord.Color.dark_teal(),
            fields=[
                ("Author", f"{message.author.mention} (`{message.author.id}`)", False),
                ("Channel", message.channel.mention if isinstance(message.channel, discord.TextChannel) else "Unknown", False),
                ("Content", _truncate_for_embed(message.content or "No text content"), False),
            ],
        )
    
    current_prefix = _get_guild_prefix(message.guild)

    # Debug log to ensure we are receiving messages
    if message.content.startswith(current_prefix):
        logger.info(f"Command-like message received from {message.author}: {message.content}")

    ctx = await bot.get_context(message)
    await bot.process_commands(message)

    if message.guild and ctx.command is None and not message.author.bot:
        direct_message_payload = parse_direct_message_request(message.content)
        if direct_message_payload:
            if not message.author.guild_permissions.manage_messages:
                await message.channel.send("❌ You need **Manage Messages** permission to use `/directmessage`.", delete_after=6)
                return

            target_user_id, dm_text = direct_message_payload
            try:
                target_user = await bot.fetch_user(target_user_id)
            except (discord.NotFound, discord.HTTPException):
                await message.channel.send("❌ I couldn't find that user.", delete_after=6)
                return

            try:
                await target_user.send(dm_text)
                await message.channel.send(f"✅ Sent a DM to **{target_user}**.", delete_after=6)
            except discord.Forbidden:
                await message.channel.send("⚠️ I couldn't send a DM to that user (DMs may be closed).", delete_after=8)
            except discord.HTTPException:
                await message.channel.send("❌ Failed to send the DM due to a Discord API error.", delete_after=8)
            return

    if ctx.command is None and not message.content.startswith(current_prefix):
        if await handle_plain_avatar_request(message, ctx):
            return
        if await handle_plain_reto_request(message):
            return
        if await handle_plain_cute_check_request(message):
            return
        if await handle_plain_gay_radar_request(message):
            return
        if await handle_plain_lesbiancheck_request(message):
            return

    if message.guild and ctx.command is None:
        command_name = extract_prefixed_command_name(message.content, current_prefix)
        if command_name:
            custom_response = database.get_custom_command(message.guild.id, command_name)
            if custom_response:
                await message.channel.send(custom_response)
                return

    await auto_translate_to_english(message, ctx)

    # Quote generator: if a user replies to a message and tags the bot,
    # generate a quote image + embed from the replied message.
    if (
        message.guild
        and ctx.command is None
        and bot.user
        and bot.user in message.mentions
        and message.reference
        and message.reference.message_id
    ):
        referenced = message.reference.resolved
        if referenced is None:
            try:
                referenced = await message.channel.fetch_message(message.reference.message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                referenced = None

        if referenced and not referenced.author.bot:
            # Avoid re-posting the same quoted source message repeatedly in one runtime.
            if referenced.id in quoted_source_message_ids:
                return

            quote_text = (referenced.content or "").strip()
            preview_text = quote_text[:380] + ("..." if len(quote_text) > 380 else "")
            poster_image_url = None
            for attachment in referenced.attachments:
                if attachment.content_type and attachment.content_type.startswith("image/"):
                    poster_image_url = attachment.url
                    break
            if not poster_image_url:
                for embed_item in referenced.embeds:
                    if embed_item.image and embed_item.image.url:
                        poster_image_url = embed_item.image.url
                        break

            if not quote_text and not poster_image_url:
                return

            data = load_attendance_data(message.guild.id)
            quote_number = (data.get("quote_counter") or 0) + 1
            data["quote_counter"] = quote_number
            save_attendance_data(message.guild.id, data)

            embed = discord.Embed(
                title="Quote",
                description=f"> {preview_text or '[ photo-only message ]'}\n— {referenced.author.display_name}",
                color=discord.Color.from_rgb(16, 16, 20),
                timestamp=referenced.created_at
            )
            timestamp_text = referenced.created_at.strftime("%b %d, %Y • %I:%M %p UTC")
            if poster_image_url:
                embed.set_image(url=poster_image_url)
            embed.set_footer(
                text=f"Quoted by {message.author.display_name} • {timestamp_text}",
                icon_url=message.author.display_avatar.url
            )
            await message.channel.send(embed=embed)
            quoted_source_message_ids.add(referenced.id)
            return

    msg_content = message.content.strip().lower()

    if message.guild and "happy anniversary" in msg_content:
        anniversary_config = database.get_anniversary_config(message.guild.id)
        if anniversary_config:
            configured_channel_id = anniversary_config.get("channel_id")
            if configured_channel_id and message.channel.id != int(configured_channel_id):
                anniversary_config = None
        if anniversary_config:
            reward_years = _get_anniversary_reward_year_if_eligible(message.guild, anniversary_config)
            if not reward_years:
                try:
                    anchor_md = (anniversary_config.get("anniversary_date_md") or "").strip()
                    if anchor_md:
                        anchor_month, anchor_day = [int(part) for part in anchor_md.split("-", 1)]
                        now_year = datetime.datetime.utcnow().year
                        next_date = datetime.date(now_year, anchor_month, anchor_day)
                        if next_date < datetime.datetime.utcnow().date():
                            next_date = datetime.date(now_year + 1, anchor_month, anchor_day)
                        when_text = next_date.strftime("%B %d, %Y")
                    else:
                        when_text = "the configured anniversary date"
                except (TypeError, ValueError):
                    when_text = "the configured anniversary date"

                await message.channel.send(
                    f"{message.author.mention}, anniversary rewards are not available yet. Please wait until **{when_text}**.",
                    delete_after=8
                )
                return

            anniversary_role = message.guild.get_role(int(anniversary_config.get("role_id")))
            if anniversary_role and anniversary_role not in message.author.roles:
                template = anniversary_config.get("message_template") or "Happy anniversary {server_name}!"
                ping_role = message.guild.get_role(int(anniversary_config.get("ping_role_id") or 0)) if anniversary_config.get("ping_role_id") else None
                try:
                    await message.author.add_roles(
                        anniversary_role,
                        reason=f"{reward_years} year server anniversary reward"
                    )
                    years = reward_years
                    channel_message = template.format(
                        server_name=message.guild.name,
                        years=years,
                        ordinal=_format_ordinal(years),
                        user=message.author.mention,
                    )
                    channel_embed = _anniversary_embed(
                        message.guild,
                        "🎉 Happy Anniversary!",
                        f"{channel_message}\n\nAwarded role: {anniversary_role.mention}",
                    )
                    await message.channel.send(
                        content=ping_role.mention if ping_role else None,
                        embed=channel_embed,
                        delete_after=15
                    )
                    await _send_anniversary_dm(message.author, message.guild, template, years)
                except discord.Forbidden:
                    error_embed = _anniversary_embed(
                        message.guild,
                        "⚠️ Anniversary Role Error",
                        "I couldn't assign the anniversary role. Please check my role permissions.",
                        color=discord.Color.red(),
                    )
                    await message.channel.send(embed=error_embed, delete_after=10)

    if msg_content == "botowner":
        await message.channel.send(
            "The owner of this bot is Calvin. It was created on February 27, 2026."
        )
        return

    is_nstp_weekend_present = is_nstp_present_text(msg_content)

    if message.guild and msg_content in ("streak", "streak!"):
        current_streak = database.get_message_streak(message.guild.id, message.author.id)
        payload, did_claim_window = _streak_status_for_claim(current_streak)
        database.upsert_message_streak(message.guild.id, message.author.id, payload)

        try:
            await message.add_reaction("🔥")
        except (discord.Forbidden, discord.HTTPException):
            pass

        if did_claim_window:
            streak_days = payload["streak_days"]
            day_word = "day" if streak_days == 1 else "days"
            await message.channel.send(
                f"{message.author.mention} you're on fire today! You got **{streak_days} {day_word}** streak! 🔥\n"
                "You can claim this every 12 hours (12:00 AM-11:59 AM and 12:00 PM-11:59 PM, Philippines time).",
                delete_after=12
            )
        else:
            await message.channel.send(
                f"{message.author.mention} you already claimed this streak window. Come back in the next 12-hour window! 🔥",
                delete_after=8
            )
        return

    if msg_content == "67":
        for emoji in ("6️⃣", "7️⃣"):
            try:
                await message.add_reaction(emoji)
            except (discord.Forbidden, discord.HTTPException):
                break
        return

    # Trigger revive ping with plain text "revive chat" (case-insensitive)
    if msg_content == "revive chat" and message.guild:
        ok, response = await send_revive_ping(message.guild, message.channel)
        if not ok and response:
            await message.channel.send(response, delete_after=6)
        return

    if message.guild and (msg_content.startswith("sign in") or msg_content.startswith("sign out")):
        data = load_attendance_data(message.guild.id)
        if data.get("staff_attendance_enabled"):
            allowed_channel_id = data.get("staff_attendance_channel_id")
            if allowed_channel_id and message.channel.id != allowed_channel_id:
                return

            allowed_role_ids = data.get("staff_attendance_allowed_role_ids") or []
            member_role_ids = {role.id for role in getattr(message.author, "roles", [])}
            if allowed_role_ids and not any(role_id in member_role_ids for role_id in allowed_role_ids):
                await message.channel.send("You are not allowed to use staff attendance.", delete_after=6)
                return

            action = "sign_in" if msg_content.startswith("sign in") else "sign_out"
            note = message.content.strip()[7:].strip() if action == "sign_in" else message.content.strip()[8:].strip()
            note = note or ("ready for duty" if action == "sign_in" else "done for today")

            cooldown_seconds = int(data.get("staff_attendance_cooldown_seconds") or 300)
            last_action = database.get_last_staff_attendance_action(message.guild.id, message.author.id)
            if last_action:
                last_action_time = _parse_iso(last_action.get("timestamp"))
                if last_action_time:
                    elapsed_seconds = int((datetime.datetime.utcnow() - last_action_time).total_seconds())
                    if elapsed_seconds < cooldown_seconds:
                        remaining = cooldown_seconds - elapsed_seconds
                        await message.channel.send(
                            f"⏳ Staff attendance cooldown active. Try again in `{_format_remaining(remaining)}`.",
                            delete_after=6
                        )
                        return

            now_utc = datetime.datetime.utcnow()
            sign_in_record = database.get_open_staff_signin(message.guild.id, message.author.id)

            if action == "sign_in" and sign_in_record:
                await message.channel.send("You are already signed in.", delete_after=6)
                return
            if action == "sign_out" and not sign_in_record:
                await message.channel.send("You are not currently signed in.", delete_after=6)
                return

            database.add_staff_attendance_log(
                message.guild.id,
                message.author.id,
                action,
                note,
                _iso_now()
            )
            duty_count = database.get_staff_duty_count(message.guild.id, message.author.id)

            avatar_url = message.author.display_avatar.url

            if action == "sign_in":
                unix_ts = int(now_utc.timestamp())
                response_embed = discord.Embed(
                    title="🟢 SIGN IN SUCCESSFUL",
                    description="You are now marked as **On Duty**.",
                    color=discord.Color.green(),
                    timestamp=now_utc,
                )
                response_embed.add_field(name="👤 User", value=message.author.mention, inline=False)
                response_embed.add_field(name="💬 Note", value=note, inline=False)
                response_embed.add_field(name="🕒 Time", value=f"{_format_ph_time(now_utc)} (PH Time)", inline=False)
                response_embed.add_field(name="🧭 Channel", value=message.channel.mention, inline=True)
                response_embed.add_field(name="🔢 Total Duty Sessions", value=str(duty_count), inline=True)
                response_embed.add_field(name="⏰ Signed In", value=f"<t:{unix_ts}:F>", inline=False)
                response_embed.set_footer(text="Remember to sign out before 11:59 PM PH time.")
                response_embed.set_thumbnail(url=avatar_url)

                log_embed = discord.Embed(
                    title="👮 STAFF ATTENDANCE LOG",
                    description="🟢 **SIGN IN**",
                    color=discord.Color.green(),
                    timestamp=now_utc,
                )
                log_embed.add_field(name="👤 Staff", value=message.author.display_name, inline=False)
                log_embed.add_field(name="💬 Note", value=note, inline=False)
                log_embed.add_field(name="🧭 Channel", value=message.channel.mention, inline=True)
                log_embed.add_field(name="🔢 Total Duty Sessions", value=str(duty_count), inline=False)
                log_embed.add_field(name="🆔 User ID", value=str(message.author.id), inline=True)
                log_embed.add_field(name="🕒 Time", value=f"{_format_ph_time(now_utc)}", inline=False)
                log_embed.add_field(name="⏰ Timestamp", value=f"<t:{unix_ts}:F>", inline=False)
                log_embed.set_footer(text=f"Guild: {message.guild.name}")
                log_embed.set_thumbnail(url=avatar_url)

                await _send_staff_duty_dm(
                    message.author,
                    "🟢 Duty started. You are now on a **12-hour duty window**. Please remember to sign out before **11:59 PM PH time**.",
                    dedupe_key=f"duty-start-{now_utc.date().isoformat()}-{now_utc.hour}"
                )
            else:
                sign_in_time = _parse_iso(sign_in_record.get("timestamp"))
                duration_text = _format_duration(now_utc - sign_in_time) if sign_in_time else "0h 0m"

                response_embed = discord.Embed(
                    title="🔴 SIGN OUT SUCCESSFUL",
                    color=discord.Color.red(),
                    timestamp=now_utc,
                )
                response_embed.add_field(name="👤 User", value=message.author.mention, inline=False)
                response_embed.add_field(name="💬 Note", value=note, inline=False)
                response_embed.add_field(name="⏱ Duration", value=duration_text, inline=False)
                response_embed.add_field(name="🕒 Time", value=f"{_format_ph_time(now_utc)} (PH Time)", inline=False)
                response_embed.set_thumbnail(url=avatar_url)

                log_embed = discord.Embed(
                    title="👮 STAFF ATTENDANCE LOG",
                    description="🔴 **SIGN OUT**",
                    color=discord.Color.red(),
                    timestamp=now_utc,
                )
                log_embed.add_field(name="👤 Staff", value=message.author.display_name, inline=False)
                log_embed.add_field(name="💬 Note", value=note, inline=False)
                log_embed.add_field(name="⏱ Duration", value=duration_text, inline=False)
                log_embed.add_field(name="🔢 Total Duty Sessions", value=str(duty_count), inline=False)
                log_embed.add_field(name="🕒 Time", value=f"{_format_ph_time(now_utc)}", inline=False)
                log_embed.set_thumbnail(url=avatar_url)

            await message.channel.send(embed=response_embed)
            log_channel_id = data.get("staff_attendance_log_channel_id")
            if log_channel_id:
                log_channel = message.guild.get_channel(log_channel_id)
                if log_channel:
                    await log_channel.send(embed=log_embed)
            return

    if msg_content in ("present", "absent") or is_nstp_weekend_present:
        if not message.guild:
            return

        settings = load_settings(message.guild.id)
        status = 'present' if is_nstp_weekend_present else msg_content

        # Check Window
        allowed, window_msg = is_in_attendance_window(message.guild.id, allow_weekend_override=is_nstp_weekend_present)
        if not allowed:
            await message.channel.send(window_msg, delete_after=5)
            return

        if not settings.get('allow_self_marking', True):
            # If self-marking is disabled, we ignore the message or warn?
            # Warn is better UX
            await message.channel.send("Self-marking is currently disabled.", delete_after=5)
            return

        data = load_attendance_data(message.guild.id)

        # Restrict to configured present channel if set
        present_channel_id = data.get('present_channel_id')
        if present_channel_id and message.channel.id != present_channel_id:
            target_channel = message.guild.get_channel(present_channel_id)
            if target_channel:
                await message.channel.send(f"You can only mark your attendance in {target_channel.mention}.", delete_after=5)
            return

        # Check permissions
        allowed_role_id = data.get('allowed_role_id')
        if allowed_role_id:
            allowed_role = message.guild.get_role(allowed_role_id)
            if allowed_role and allowed_role not in message.author.roles:
                # Silently ignore to prevent spam if they don't have perms.
                return

        attendance_role_id = data.get('attendance_role_id')
        absent_role_id = data.get('absent_role_id')
        excused_role_id = data.get('excused_role_id')
        status_role_id = attendance_role_id if status == 'present' else absent_role_id
        status_role_name = 'attendance' if status == 'present' else 'absence'
        success_emoji = '✅' if status == 'present' else '❌'

        existing_status = has_conflicting_attendance_status(data.get('records'), message.author.id, status)
        if existing_status:
            await message.channel.send(
                f"{message.author.mention}, you are already marked as **{existing_status}** and cannot switch to **{status}** this session.",
                delete_after=6
            )
            return

        if status_role_id:
            role = message.guild.get_role(status_role_id)
            if role:
                user_id = str(message.author.id)
                now = datetime.datetime.now()

                # Check if already marked today (prevent spamming status updates)
                # We check if they HAVE the role already as a proxy for already having that status.
                if role in message.author.roles:
                     await message.channel.send(f"{message.author.mention}, you have already marked your status as {status}!", delete_after=5)
                else:
                    # Give role
                    try:
                        # Remove conflicting roles first
                        roles_to_remove = []
                        if status == 'present':
                            if absent_role_id: roles_to_remove.append(absent_role_id)
                            if excused_role_id: roles_to_remove.append(excused_role_id)
                        else:
                            if attendance_role_id: roles_to_remove.append(attendance_role_id)
                            if excused_role_id: roles_to_remove.append(excused_role_id)

                        for rid in roles_to_remove:
                            r = message.guild.get_role(rid)
                            if r and r in message.author.roles:
                                await message.author.remove_roles(r)

                        await message.author.add_roles(role)
                        await message.add_reaction(success_emoji)

                        if 'records' not in data:
                            data['records'] = {}
                        data['records'][user_id] = {
                            "status": status,
                            "timestamp": now.isoformat(),
                            "channel_id": message.channel.id
                        }
                        save_attendance_data(message.guild.id, data)
                        database.increment_status_count(message.guild.id, message.author.id, status)

                        await message.channel.send(
                            f"{status.title()} marked for {message.author.mention}! You have been given the {role.name} role.",
                            delete_after=10
                        )

                        # DM the user
                        try:
                            if status == 'present':
                                embed = discord.Embed(
                                    title="✅ Attendance Confirmed",
                                    description="Your attendance has been checked successfully.",
                                    color=discord.Color.gold()
                                )
                                embed.add_field(name="Status", value="Present", inline=True)
                                embed.add_field(name="Note", value="You will be notified once the 12-hour period has expired, after which you will be allowed to mark yourself as present again.", inline=False)
                            else:
                                embed = discord.Embed(
                                    title="Attendance Status: Absent",
                                    description=f"You have been marked as **ABSENT** in **{message.guild.name}**.",
                                    color=discord.Color.red()
                                )
                                embed.add_field(name="Status", value="Absent", inline=True)
                                embed.add_field(name="Marked At", value=now.strftime("%I:%M %p"), inline=True)

                            if message.guild.icon:
                                embed.set_author(name=message.guild.name, icon_url=message.guild.icon.url)
                                embed.set_thumbnail(url=message.guild.icon.url)
                            else:
                                embed.set_author(name=message.guild.name)

                            embed.set_footer(text=f"Calvsbot • Server: {message.guild.name}")
                            await message.author.send(embed=embed)
                        except discord.Forbidden:
                            logger.warning(f"Could not DM user {message.author.name} (Closed DMs)")
                        except Exception:
                            pass

                        await refresh_attendance_report(message.guild, message.channel, force_update=True)
                    except discord.Forbidden:
                        await message.channel.send(f"I tried to give you the {status_role_name} role, but I don't have permission! Please check my role hierarchy.")
    elif msg_content == "presents":
        if message.guild:
            await refresh_attendance_report(message.guild, message.channel, force_update=True)
    elif msg_content.startswith("excuse"):
        if not message.guild:
            return

        allowed, window_msg = is_in_attendance_window(message.guild.id)
        if not allowed:
            await message.channel.send(f"{window_msg} Excuse submissions are also closed once attendance time is over.", delete_after=5)
            return
            
        settings = load_settings(message.guild.id)
        if settings.get('require_admin_excuse', True):
            # Check if user has manage_roles
            if not message.author.guild_permissions.manage_roles:
                await message.channel.send("Only admins can excuse users.", delete_after=5)
                return

        data = load_attendance_data(message.guild.id)
        attendance_role_id = data.get('attendance_role_id')
        absent_role_id = data.get('absent_role_id')
        excused_role_id = data.get('excused_role_id')

        existing_status = has_conflicting_attendance_status(data.get('records'), message.author.id, 'excused')
        if existing_status:
            await message.channel.send(
                f"{message.author.mention}, you are already marked as **{existing_status}** and cannot switch to **excused** this session.",
                delete_after=6
            )
            return
        
        # Parse reason
        # "excuse because i am sick" -> reason: "because i am sick"
        reason = message.content[6:].strip()
        if not reason:
            reason = "No reason provided"

        if excused_role_id:
            role = message.guild.get_role(excused_role_id)
            if role:
                user_id = str(message.author.id)
                now = datetime.datetime.now()
                
                # Check if already marked (prevent spamming)
                if role in message.author.roles:
                     await message.channel.send(f"{message.author.mention}, you have already marked your status as excused!", delete_after=5)
                else:
                    # Give role
                    try:
                        # Remove conflicting roles first
                        roles_to_remove = []
                        if attendance_role_id: roles_to_remove.append(attendance_role_id)
                        if absent_role_id: roles_to_remove.append(absent_role_id)
                        
                        for rid in roles_to_remove:
                            r = message.guild.get_role(rid)
                            if r and r in message.author.roles:
                                await message.author.remove_roles(r)

                        await message.author.add_roles(role)
                        await message.add_reaction("✅")
                        
                        # Update record with FULL timestamp for 24h expiry
                        if 'records' not in data:
                            data['records'] = {}
                        data['records'][user_id] = {
                            "status": "excused",
                            "timestamp": now.isoformat(),
                            "channel_id": message.channel.id,
                            "reason": reason
                        }
                        save_attendance_data(message.guild.id, data)
                        database.increment_status_count(message.guild.id, message.author.id, "excused")
                        
                        await message.channel.send(f"Excused status marked for {message.author.mention}! Reason: {reason}", delete_after=10)
                        
                        # Automatically show the attendance report
                        await refresh_attendance_report(message.guild, force_update=True)
                    except discord.Forbidden:
                        await message.channel.send("I tried to give you the role, but I don't have permission! Please check my role hierarchy.")

    if message.guild and not message.content.startswith('!'):
        sticky_info = sticky_channels.get(message.channel.id)
        if sticky_info:
            has_image_attachment = False
            if message.attachments:
                for att in message.attachments:
                    if att.content_type and att.content_type.startswith("image/"):
                        has_image_attachment = True
                        break
                    filename = att.filename.lower()
                    if filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")):
                        has_image_attachment = True
                        break
            if has_image_attachment:
                return
            try:
                await message.delete()
            except discord.Forbidden:
                pass
            except discord.HTTPException:
                pass
            channel = message.channel
            sticky_msg = None
            try:
                sticky_msg = await channel.fetch_message(sticky_info["message_id"])
            except (discord.NotFound, discord.Forbidden):
                sticky_msg = None
            if not sticky_msg:
                new_msg = await channel.send(sticky_info["content"])
                sticky_info["message_id"] = new_msg.id
                save_sticky_channels()


if __name__ == "__main__":
    keep_alive()

    def keep_process_alive(reason: str):
        logger.warning("%s", reason)
        while True:
            time.sleep(3600)

    if not TOKEN:
        keep_process_alive(
            "DISCORD_TOKEN is not set. The healthcheck server will stay online, "
            "but the Discord bot will not connect until the environment variable is configured."
        )
    else:
        try:
            bot.run(TOKEN)
        except discord.LoginFailure as e:
            keep_process_alive(
                f"Login failed: {e}. The healthcheck server remains online for diagnostics."
            )
        except Exception as e:
            logger.error(f"Bot crashed with error: {e}", exc_info=True)
            keep_process_alive(
                "Unexpected bot crash. The healthcheck server remains online for diagnostics."
            )
