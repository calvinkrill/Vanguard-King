import os
import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    value = (os.getenv(name, default) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _running_on_hosted_platform() -> bool:
    """Best-effort check for common hosted deployment environments."""
    return any(
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


def resolve_db_file():
    """Choose a database path, preferring persistent storage when available."""
    configured_path = os.getenv("DB_FILE")
    if configured_path:
        return configured_path

    configured_dir = os.getenv("DB_DIR")
    if configured_dir:
        return str(Path(configured_dir) / "attendance.db")

    # Prefer known platform volume mounts when DB_FILE/DB_DIR is not set.
    # This keeps leaderboard/stats data across bot restarts and redeploys.
    platform_volume_dir = (
        os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
        or os.getenv("RENDER_DISK_PATH")
        or os.getenv("PERSISTENT_VOLUME_DIR")
    )

    candidate_dirs = []
    if platform_volume_dir:
        candidate_dirs.append((Path(platform_volume_dir), True))

    persistent_dir = Path("/data")
    if persistent_dir.exists() and persistent_dir.is_dir():
        candidate_dirs.append((persistent_dir, True))

    candidate_dirs.append((Path("data"), False))

    require_persistent = _env_flag("REQUIRE_PERSISTENT_STORAGE", "0")
    if require_persistent and not any(is_persistent for _, is_persistent in candidate_dirs):
        raise RuntimeError(
            "REQUIRE_PERSISTENT_STORAGE=1 is set, but no persistent volume was detected. "
            "Set DB_FILE/DB_DIR to a persistent mount path (for example /data/attendance.db)."
        )
    if _running_on_hosted_platform() and not any(is_persistent for _, is_persistent in candidate_dirs):
        logger.warning(
            "No persistent storage mount detected on hosted platform; falling back to ephemeral local storage. "
            "Set DB_FILE/DB_DIR or mount a volume to persist attendance data."
        )

    for directory, _ in candidate_dirs:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            return str(directory / "attendance.db")
        except OSError:
            continue

    return "attendance.db"


DB_FILE = resolve_db_file()
SNAPSHOT_FILE = os.getenv("DB_SNAPSHOT_FILE", str(Path(DB_FILE).with_name("attendance_snapshot.json")))
SNAPSHOT_TABLES = (
    "guild_configs",
    "attendance_records",
    "attendance_stats",
    "custom_commands",
    "bot_state",
    "game_progress",
    "autonick_rules",
    "pet_profiles",
    "message_streaks",
    "fmbot_links",
    "birthday_entries",
    "birthday_channels",
    "birthday_announcements",
    "anniversary_configs",
    "anniversary_awards",
    "gartic_games",
    "giveaway_configs",
    "giveaway_entries",
    "poll_entries",
    "poll_votes",
    "ticket_panels",
    "ticket_entries",
    "tod_prompts",
    "tod_lobbies",
    "staff_attendance_logs",
)


def ensure_parent_directory(file_path):
    """Creates the parent directory for a file path when needed."""
    path = Path(file_path)
    parent = path.parent

    if str(parent) in ("", "."):
        return

    parent.mkdir(parents=True, exist_ok=True)


def write_snapshot():
    """Writes a JSON backup snapshot beside the SQLite database."""
    try:
        snapshot_path = Path(SNAPSHOT_FILE)
        ensure_parent_directory(snapshot_path)
        payload = export_all_data()
        snapshot_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8"
        )
    except Exception as e:
        logger.warning("Failed to write snapshot %s: %s", SNAPSHOT_FILE, e)


def export_all_data():
    """Exports the database contents as plain JSON-serializable structures."""
    conn = get_connection()
    c = conn.cursor()

    tables = {}
    for table_name in SNAPSHOT_TABLES:
        c.execute(f"SELECT * FROM {table_name}")
        tables[table_name] = [dict(row) for row in c.fetchall()]

    conn.close()
    return {
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "db_file": DB_FILE,
        "tables": tables
    }


def is_database_empty(conn):
    """Returns True when all persisted tables are empty."""
    c = conn.cursor()
    for table_name in SNAPSHOT_TABLES:
        c.execute(f"SELECT COUNT(*) AS count FROM {table_name}")
        row = c.fetchone()
        if row and row["count"]:
            return False
    return True


def restore_snapshot_if_needed(conn):
    """Restores the JSON snapshot into a new empty database."""
    snapshot_path = Path(SNAPSHOT_FILE)
    if not snapshot_path.exists() or not is_database_empty(conn):
        return

    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        tables = payload.get("tables", {})
        c = conn.cursor()

        guild_rows = tables.get("guild_configs", [])
        for row in guild_rows:
            row.setdefault("present_channel_id", None)
            row.setdefault("translation_channel_id", None)
            row.setdefault("translation_dual_channel_id", None)
            row.setdefault("translation_enabled", 0)
            row.setdefault("ping_role_id", None)
            row.setdefault("revive_ping_role_id", None)
            row.setdefault("revive_channel_id", None)
            row.setdefault("confession_channel_id", None)
            row.setdefault("confession_log_channel_id", None)
            row.setdefault("confession_review_channel_id", None)
            row.setdefault("confession_author_channel_id", None)
            row.setdefault("confession_ping_role_id", None)
            row.setdefault("confession_word_filter_enabled", 0)
            row.setdefault("confession_cooldown_enabled", 0)
            row.setdefault("confession_min_account_age_enabled", 0)
            row.setdefault("confess_alias", None)
            row.setdefault("reply_alias", None)
            row.setdefault("confession_header_text", None)
            row.setdefault("reply_header_text", None)
            row.setdefault("confession_embed_footer_text", None)
            row.setdefault("confession_embed_color", None)
            row.setdefault("confession_submit_button_text", None)
            row.setdefault("confession_reply_button_text", None)
            row.setdefault("confession_counter", 0)
            row.setdefault("suggestion_channel_id", None)
            row.setdefault("suggestion_counter", 0)
            row.setdefault("quote_counter", 0)
            row.setdefault("reto_star_channel_id", None)
            row.setdefault("default_nick_tag", None)
            row.setdefault("announcement_channel_ids", None)
            row.setdefault("staff_attendance_enabled", 0)
            row.setdefault("staff_attendance_allowed_role_ids", None)
            row.setdefault("staff_attendance_log_channel_id", None)
            row.setdefault("staff_attendance_channel_id", None)
            row.setdefault("staff_attendance_cooldown_seconds", 300)
            row.setdefault("bot_prefix", "!")

        c.executemany(
            '''INSERT INTO guild_configs (
                   guild_id, attendance_role_id, absent_role_id, excused_role_id,
                   welcome_channel_id, report_channel_id, last_report_message_id,
                   last_report_channel_id, attendance_mode, attendance_expiry_hours,
                   window_start_time, window_end_time, last_processed_date,
                   last_opened_date, allow_self_marking, require_admin_excuse,
                   auto_nick_on_join, enforce_suffix, remove_suffix_on_role_loss,
                   suffix_format, present_channel_id, ping_role_id, revive_ping_role_id,
                   translation_channel_id, translation_dual_channel_id, translation_enabled, revive_channel_id, confession_channel_id, confession_log_channel_id,
                   confession_review_channel_id, confession_author_channel_id, confession_ping_role_id, confession_word_filter_enabled, confession_cooldown_enabled,
                   confession_min_account_age_enabled, confess_alias, reply_alias, confession_header_text, reply_header_text, confession_embed_footer_text,
                   confession_embed_color, confession_submit_button_text, confession_reply_button_text, confession_counter, suggestion_channel_id, suggestion_counter, quote_counter,
                   reto_star_channel_id, default_nick_tag, announcement_channel_ids, staff_attendance_enabled, staff_attendance_allowed_role_ids,
                   staff_attendance_log_channel_id, staff_attendance_channel_id, staff_attendance_cooldown_seconds,
                   bot_prefix
               ) VALUES (
                   :guild_id, :attendance_role_id, :absent_role_id, :excused_role_id,
                   :welcome_channel_id, :report_channel_id, :last_report_message_id,
                   :last_report_channel_id, :attendance_mode, :attendance_expiry_hours,
                   :window_start_time, :window_end_time, :last_processed_date,
                   :last_opened_date, :allow_self_marking, :require_admin_excuse,
                   :auto_nick_on_join, :enforce_suffix, :remove_suffix_on_role_loss,
                   :suffix_format, :present_channel_id, :ping_role_id, :revive_ping_role_id,
                   :translation_channel_id, :translation_dual_channel_id, :translation_enabled, :revive_channel_id, :confession_channel_id, :confession_log_channel_id,
                   :confession_review_channel_id, :confession_author_channel_id, :confession_ping_role_id, :confession_word_filter_enabled, :confession_cooldown_enabled,
                   :confession_min_account_age_enabled, :confess_alias, :reply_alias, :confession_header_text, :reply_header_text, :confession_embed_footer_text,
                   :confession_embed_color, :confession_submit_button_text, :confession_reply_button_text, :confession_counter, :suggestion_channel_id, :suggestion_counter, :quote_counter,
                   :reto_star_channel_id, :default_nick_tag, :announcement_channel_ids, :staff_attendance_enabled, :staff_attendance_allowed_role_ids,
                   :staff_attendance_log_channel_id, :staff_attendance_channel_id, :staff_attendance_cooldown_seconds,
                   :bot_prefix
               )''',
            guild_rows
        )
        c.executemany(
            '''INSERT INTO staff_attendance_logs (
                   id, guild_id, user_id, action, note, timestamp
               ) VALUES (
                   :id, :guild_id, :user_id, :action, :note, :timestamp
               )''',
            tables.get("staff_attendance_logs", [])
        )
        c.executemany(
            '''INSERT INTO attendance_records (
                   id, guild_id, user_id, status, timestamp, channel_id, reason
               ) VALUES (
                   :id, :guild_id, :user_id, :status, :timestamp, :channel_id, :reason
               )''',
            tables.get("attendance_records", [])
        )
        c.executemany(
            '''INSERT INTO attendance_stats (
                   guild_id, user_id, present_count, absent_count, excused_count
               ) VALUES (
                   :guild_id, :user_id, :present_count, :absent_count, :excused_count
               )''',
            tables.get("attendance_stats", [])
        )
        c.executemany(
            '''INSERT INTO custom_commands (
                   guild_id, command_name, response_text
               ) VALUES (
                   :guild_id, :command_name, :response_text
               )''',
            tables.get("custom_commands", [])
        )
        c.executemany(
            '''INSERT INTO bot_state (
                   id, status_type, status_text, updated_at
               ) VALUES (
                   :id, :status_type, :status_text, :updated_at
               )''',
            tables.get("bot_state", [])
        )
        c.executemany(
            '''INSERT INTO game_progress (
                   guild_id, user_id, game_name, progress_text, updated_at
               ) VALUES (
                   :guild_id, :user_id, :game_name, :progress_text, :updated_at
               )''',
            tables.get("game_progress", [])
        )
        c.executemany(
            '''INSERT INTO autonick_rules (
                   guild_id, role_id, tag
               ) VALUES (
                   :guild_id, :role_id, :tag
               )''',
            tables.get("autonick_rules", [])
        )
        c.executemany(
            '''INSERT INTO pet_profiles (
                   guild_id, user_id, pet_name, pet_type, hunger, happiness, cleanliness,
                   energy, bond, coins, streak, total_checkins, last_checkin_date,
                   adopted_at, updated_at, last_fed_at, last_played_at, last_cleaned_at,
                   last_slept_at, evolved_stage
               ) VALUES (
                   :guild_id, :user_id, :pet_name, :pet_type, :hunger, :happiness, :cleanliness,
                   :energy, :bond, :coins, :streak, :total_checkins, :last_checkin_date,
                   :adopted_at, :updated_at, :last_fed_at, :last_played_at, :last_cleaned_at,
                   :last_slept_at, :evolved_stage
               )''',
            tables.get("pet_profiles", [])
        )
        c.executemany(
            '''INSERT INTO message_streaks (
                   guild_id, user_id, streak_days, last_claim_date, last_claim_window, updated_at
               ) VALUES (
                   :guild_id, :user_id, :streak_days, :last_claim_date, :last_claim_window, :updated_at
               )''',
            tables.get("message_streaks", [])
        )

        conn.commit()
        logger.info("Restored database contents from snapshot %s", snapshot_path)
    except Exception as e:
        conn.rollback()
        logger.warning("Failed to restore snapshot %s: %s", snapshot_path, e)

def get_connection():
    """Establishes a connection to the SQLite database."""
    ensure_parent_directory(DB_FILE)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database tables."""
    conn = get_connection()
    c = conn.cursor()
    
    # Guild Configuration Table
    c.execute('''CREATE TABLE IF NOT EXISTS guild_configs (
        guild_id INTEGER PRIMARY KEY,
        attendance_role_id INTEGER,
        absent_role_id INTEGER,
        excused_role_id INTEGER,
        welcome_channel_id INTEGER,
        report_channel_id INTEGER,
        last_report_message_id INTEGER,
        last_report_channel_id INTEGER,
        attendance_mode TEXT DEFAULT 'duration',
        attendance_expiry_hours INTEGER DEFAULT 12,
        window_start_time TEXT DEFAULT '08:00',
        window_end_time TEXT DEFAULT '17:00',
        last_processed_date TEXT,
        last_opened_date TEXT,
        allow_self_marking BOOLEAN DEFAULT 1,
        require_admin_excuse BOOLEAN DEFAULT 0,
        auto_nick_on_join BOOLEAN DEFAULT 0,
        enforce_suffix BOOLEAN DEFAULT 0,
        remove_suffix_on_role_loss BOOLEAN DEFAULT 0,
        suffix_format TEXT DEFAULT ' [𝙼𝚂𝚄𝚊𝚗]',
        present_channel_id INTEGER,
        ping_role_id INTEGER,
        revive_ping_role_id INTEGER,
        translation_channel_id INTEGER,
        translation_dual_channel_id INTEGER,
        translation_enabled BOOLEAN DEFAULT 0,
        revive_channel_id INTEGER,
        confession_channel_id INTEGER,
        confession_log_channel_id INTEGER,
        confession_review_channel_id INTEGER,
        confession_author_channel_id INTEGER,
        confession_ping_role_id INTEGER,
        confession_word_filter_enabled BOOLEAN DEFAULT 0,
        confession_cooldown_enabled BOOLEAN DEFAULT 0,
        confession_min_account_age_enabled BOOLEAN DEFAULT 0,
        confess_alias TEXT,
        reply_alias TEXT,
        confession_header_text TEXT,
        reply_header_text TEXT,
        confession_embed_footer_text TEXT,
        confession_embed_color TEXT,
        confession_submit_button_text TEXT,
        confession_reply_button_text TEXT,
        say_log_channel_id INTEGER,
        confession_counter INTEGER DEFAULT 0,
        suggestion_channel_id INTEGER,
        suggestion_counter INTEGER DEFAULT 0,
        quote_counter INTEGER DEFAULT 0,
        reto_star_channel_id INTEGER,
        default_nick_tag TEXT,
        announcement_channel_ids TEXT,
        staff_attendance_enabled BOOLEAN DEFAULT 0,
        staff_attendance_allowed_role_ids TEXT,
        staff_attendance_log_channel_id INTEGER,
        staff_attendance_channel_id INTEGER,
        staff_attendance_cooldown_seconds INTEGER DEFAULT 300,
        staff_tracker_enabled BOOLEAN DEFAULT 0,
        staff_tracker_role_ids TEXT,
        staff_tracker_exempt_role_ids TEXT,
        staff_tracker_log_channel_id INTEGER,
        staff_tracker_punishment_mode TEXT DEFAULT 'timeout',
        bot_prefix TEXT DEFAULT '!',
        automod_enabled BOOLEAN DEFAULT 0
    )''')
    
    # Ensure new columns exist on older databases
    c.execute("PRAGMA table_info('guild_configs')")
    existing_guild_columns = [row[1] for row in c.fetchall()]
    if 'present_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN present_channel_id INTEGER")
    if 'ping_role_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN ping_role_id INTEGER")
    if 'revive_ping_role_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN revive_ping_role_id INTEGER")
    if 'translation_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN translation_channel_id INTEGER")
    if 'translation_dual_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN translation_dual_channel_id INTEGER")
    if 'translation_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN translation_enabled BOOLEAN DEFAULT 0")
    if 'revive_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN revive_channel_id INTEGER")
    if 'confession_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_channel_id INTEGER")
    if 'confession_log_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_log_channel_id INTEGER")
    if 'confession_review_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_review_channel_id INTEGER")
    if 'confession_author_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_author_channel_id INTEGER")
    if 'confession_ping_role_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_ping_role_id INTEGER")
    if 'confession_word_filter_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_word_filter_enabled BOOLEAN DEFAULT 0")
    if 'confession_cooldown_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_cooldown_enabled BOOLEAN DEFAULT 0")
    if 'confession_min_account_age_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_min_account_age_enabled BOOLEAN DEFAULT 0")
    if 'confess_alias' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confess_alias TEXT")
    if 'reply_alias' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN reply_alias TEXT")
    if 'confession_header_text' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_header_text TEXT")
    if 'reply_header_text' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN reply_header_text TEXT")
    if 'confession_embed_footer_text' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_embed_footer_text TEXT")
    if 'confession_embed_color' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_embed_color TEXT")
    if 'confession_submit_button_text' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_submit_button_text TEXT")
    if 'confession_reply_button_text' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_reply_button_text TEXT")
    if 'say_log_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN say_log_channel_id INTEGER")
    if 'confession_counter' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN confession_counter INTEGER DEFAULT 0")
    if 'suggestion_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN suggestion_channel_id INTEGER")
    if 'suggestion_counter' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN suggestion_counter INTEGER DEFAULT 0")
    if 'quote_counter' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN quote_counter INTEGER DEFAULT 0")
    if 'reto_star_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN reto_star_channel_id INTEGER")
    if 'default_nick_tag' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN default_nick_tag TEXT")
    if 'announcement_channel_ids' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN announcement_channel_ids TEXT")
    if 'staff_attendance_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_attendance_enabled BOOLEAN DEFAULT 0")
    if 'staff_attendance_allowed_role_ids' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_attendance_allowed_role_ids TEXT")
    if 'staff_attendance_log_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_attendance_log_channel_id INTEGER")
    if 'staff_attendance_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_attendance_channel_id INTEGER")
    if 'staff_attendance_cooldown_seconds' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_attendance_cooldown_seconds INTEGER DEFAULT 300")
    if 'staff_tracker_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_tracker_enabled BOOLEAN DEFAULT 0")
    if 'staff_tracker_role_ids' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_tracker_role_ids TEXT")
    if 'staff_tracker_exempt_role_ids' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_tracker_exempt_role_ids TEXT")
    if 'staff_tracker_log_channel_id' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_tracker_log_channel_id INTEGER")
    if 'staff_tracker_punishment_mode' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN staff_tracker_punishment_mode TEXT DEFAULT 'timeout'")
    if 'bot_prefix' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN bot_prefix TEXT DEFAULT '!'")
    if 'automod_enabled' not in existing_guild_columns:
        c.execute("ALTER TABLE guild_configs ADD COLUMN automod_enabled BOOLEAN DEFAULT 0")

    # Attendance Records Table
    c.execute('''CREATE TABLE IF NOT EXISTS attendance_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER,
        user_id INTEGER,
        status TEXT,
        timestamp TEXT,
        channel_id INTEGER,
        reason TEXT,
        FOREIGN KEY(guild_id) REFERENCES guild_configs(guild_id)
    )''')
    
    # Index for faster lookups
    c.execute('CREATE INDEX IF NOT EXISTS idx_records_guild_user ON attendance_records (guild_id, user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_records_guild_date ON attendance_records (guild_id, timestamp)')

    c.execute('''CREATE TABLE IF NOT EXISTS attendance_stats (
        guild_id INTEGER,
        user_id INTEGER,
        present_count INTEGER DEFAULT 0,
        absent_count INTEGER DEFAULT 0,
        excused_count INTEGER DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS custom_commands (
        guild_id INTEGER,
        command_name TEXT,
        response_text TEXT NOT NULL,
        PRIMARY KEY (guild_id, command_name)
    )''')

    c.execute('CREATE INDEX IF NOT EXISTS idx_custom_commands_guild ON custom_commands (guild_id)')

    c.execute('''CREATE TABLE IF NOT EXISTS bot_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        status_type TEXT,
        status_text TEXT,
        updated_at TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS game_progress (
        guild_id INTEGER,
        user_id INTEGER,
        game_name TEXT,
        progress_text TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id, game_name)
    )''')

    c.execute('CREATE INDEX IF NOT EXISTS idx_game_progress_guild_user ON game_progress (guild_id, user_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS autonick_rules (
        guild_id INTEGER,
        role_id INTEGER,
        tag TEXT NOT NULL,
        PRIMARY KEY (guild_id, role_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_autonick_rules_guild ON autonick_rules (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS pet_profiles (
        guild_id INTEGER,
        user_id INTEGER,
        pet_name TEXT NOT NULL,
        pet_type TEXT NOT NULL,
        hunger INTEGER DEFAULT 80,
        happiness INTEGER DEFAULT 80,
        cleanliness INTEGER DEFAULT 80,
        energy INTEGER DEFAULT 80,
        bond INTEGER DEFAULT 0,
        coins INTEGER DEFAULT 0,
        streak INTEGER DEFAULT 0,
        total_checkins INTEGER DEFAULT 0,
        last_checkin_date TEXT,
        adopted_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_fed_at TEXT,
        last_played_at TEXT,
        last_cleaned_at TEXT,
        last_slept_at TEXT,
        evolved_stage TEXT DEFAULT 'base',
        PRIMARY KEY (guild_id, user_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_pet_profiles_guild ON pet_profiles (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS message_streaks (
        guild_id INTEGER,
        user_id INTEGER,
        streak_days INTEGER DEFAULT 0,
        last_claim_date TEXT,
        last_claim_window TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_message_streaks_guild ON message_streaks (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS fmbot_links (
        guild_id INTEGER,
        user_id INTEGER,
        username TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_fmbot_links_guild ON fmbot_links (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS birthday_entries (
        guild_id INTEGER,
        user_id INTEGER,
        birthday_date TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_birthday_entries_guild ON birthday_entries (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS birthday_channels (
        guild_id INTEGER PRIMARY KEY,
        channel_id INTEGER NOT NULL,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS birthday_announcements (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        sent_on_date TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id, sent_on_date)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_birthday_announcements_guild_date ON birthday_announcements (guild_id, sent_on_date)')
    c.execute('''CREATE TABLE IF NOT EXISTS anniversary_configs (
        guild_id INTEGER PRIMARY KEY,
        role_id INTEGER NOT NULL,
        anniversary_date_md TEXT NOT NULL,
        milestone_years_json TEXT NOT NULL,
        channel_id INTEGER,
        ping_role_id INTEGER,
        message_template TEXT,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS anniversary_awards (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        milestone_years INTEGER NOT NULL,
        award_date TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id, milestone_years, award_date)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_anniversary_awards_guild_date ON anniversary_awards (guild_id, award_date)')
    c.execute('''CREATE TABLE IF NOT EXISTS gartic_games (
        guild_id INTEGER PRIMARY KEY,
        host_user_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        current_word TEXT,
        scores_json TEXT NOT NULL,
        players_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS ticket_panels (
        guild_id INTEGER PRIMARY KEY,
        channel_id INTEGER NOT NULL,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS ticket_entries (
        guild_id INTEGER,
        channel_id INTEGER,
        opener_user_id INTEGER NOT NULL,
        reason TEXT,
        is_open BOOLEAN DEFAULT 1,
        created_at TEXT NOT NULL,
        closed_at TEXT,
        PRIMARY KEY (guild_id, channel_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ticket_entries_guild ON ticket_entries (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS ticket_settings (
        guild_id INTEGER PRIMARY KEY,
        settings_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS giveaway_configs (
        guild_id INTEGER PRIMARY KEY,
        channel_id INTEGER NOT NULL,
        log_channel_id INTEGER,
        required_role_id INTEGER,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS giveaway_entries (
        message_id INTEGER PRIMARY KEY,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        prize TEXT NOT NULL,
        winner_count INTEGER NOT NULL,
        required_role_id INTEGER,
        end_at TEXT NOT NULL,
        created_by_user_id INTEGER,
        ended_at TEXT,
        status TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_giveaway_entries_guild_status_end ON giveaway_entries (guild_id, status, end_at)')
    c.execute('''CREATE TABLE IF NOT EXISTS poll_entries (
        message_id INTEGER PRIMARY KEY,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        question TEXT NOT NULL,
        description TEXT,
        choices_json TEXT NOT NULL,
        created_by_user_id INTEGER NOT NULL,
        end_at TEXT NOT NULL,
        status TEXT NOT NULL,
        closed_at TEXT,
        updated_at TEXT NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_poll_entries_guild_status_end ON poll_entries (guild_id, status, end_at)')
    c.execute('''CREATE TABLE IF NOT EXISTS poll_votes (
        message_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        choice_index INTEGER NOT NULL,
        voted_at TEXT NOT NULL,
        PRIMARY KEY (message_id, user_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_poll_votes_message_choice ON poll_votes (message_id, choice_index)')
    c.execute('''CREATE TABLE IF NOT EXISTS tod_prompts (
        guild_id INTEGER,
        prompt_type TEXT NOT NULL,
        prompt_text TEXT NOT NULL,
        created_by_user_id INTEGER,
        created_at TEXT NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tod_prompts_guild_type ON tod_prompts (guild_id, prompt_type)')
    c.execute('''CREATE TABLE IF NOT EXISTS tod_lobbies (
        guild_id INTEGER PRIMARY KEY,
        host_user_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        players_json TEXT NOT NULL,
        status TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS staff_attendance_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        note TEXT,
        timestamp TEXT NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_staff_attendance_logs_guild_user_time ON staff_attendance_logs (guild_id, user_id, timestamp)')
    c.execute('''CREATE TABLE IF NOT EXISTS staff_tracker_strikes (
        guild_id INTEGER NOT NULL,
        staff_user_id INTEGER NOT NULL,
        strike_count INTEGER DEFAULT 0,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, staff_user_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_staff_tracker_strikes_guild ON staff_tracker_strikes (guild_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS staff_tracker_cases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT UNIQUE NOT NULL,
        guild_id INTEGER NOT NULL,
        staff_user_id INTEGER NOT NULL,
        target_user_id INTEGER,
        action TEXT NOT NULL,
        reason_text TEXT,
        strike_count INTEGER NOT NULL,
        punishment_applied TEXT,
        audit_entry_id INTEGER,
        created_at TEXT NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_staff_tracker_cases_guild_time ON staff_tracker_cases (guild_id, created_at)')
    c.execute('''CREATE TABLE IF NOT EXISTS meeting_attendance_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        meeting_date TEXT NOT NULL,
        message_id INTEGER,
        timestamp TEXT NOT NULL,
        UNIQUE(guild_id, user_id, meeting_date)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_meeting_attendance_guild_date ON meeting_attendance_records (guild_id, meeting_date)')
    c.execute('''CREATE TABLE IF NOT EXISTS meeting_reminder_history (
        guild_id INTEGER NOT NULL,
        meeting_date TEXT NOT NULL,
        reminder_key TEXT NOT NULL,
        sent_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, meeting_date, reminder_key)
    )''')

    c.execute("PRAGMA table_info('attendance_stats')")
    existing_columns = [row[1] for row in c.fetchall()]
    if 'present_count' not in existing_columns:
        c.execute("ALTER TABLE attendance_stats ADD COLUMN present_count INTEGER DEFAULT 0")
    if 'absent_count' not in existing_columns:
        c.execute("ALTER TABLE attendance_stats ADD COLUMN absent_count INTEGER DEFAULT 0")
    if 'excused_count' not in existing_columns:
        c.execute("ALTER TABLE attendance_stats ADD COLUMN excused_count INTEGER DEFAULT 0")
    c.execute("PRAGMA table_info('anniversary_configs')")
    anniversary_columns = [row[1] for row in c.fetchall()]
    if anniversary_columns and 'anniversary_date_md' not in anniversary_columns:
        c.execute("ALTER TABLE anniversary_configs ADD COLUMN anniversary_date_md TEXT DEFAULT '01-01'")
    if anniversary_columns and 'ping_role_id' not in anniversary_columns:
        c.execute("ALTER TABLE anniversary_configs ADD COLUMN ping_role_id INTEGER")

    conn.commit()
    restore_snapshot_if_needed(conn)
    conn.close()
    logger.info("Database initialized at %s (snapshot: %s).", DB_FILE, SNAPSHOT_FILE)

def get_guild_config(guild_id):
    """Retrieves configuration for a guild."""
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM guild_configs WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None

def update_guild_config(guild_id, **kwargs):
    """Updates specific fields in the guild configuration."""
    conn = get_connection()
    c = conn.cursor()
    
    # Check if exists
    c.execute('SELECT 1 FROM guild_configs WHERE guild_id = ?', (guild_id,))
    exists = c.fetchone()
    
    if not exists:
        # Create default entry first
        c.execute('INSERT INTO guild_configs (guild_id) VALUES (?)', (guild_id,))
    
    if kwargs:
        columns = ', '.join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values()) + [guild_id]
        c.execute(f'UPDATE guild_configs SET {columns} WHERE guild_id = ?', values)
    
    conn.commit()
    conn.close()
    write_snapshot()

def get_attendance_records(guild_id):
    """Retrieves all attendance records for a guild."""
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM attendance_records WHERE guild_id = ?', (guild_id,))
    rows = c.fetchall()
    conn.close()
    
    # Convert to dictionary format expected by bot {user_id: {status, timestamp, ...}}
    records = {}
    for row in rows:
        records[str(row['user_id'])] = {
            "status": row['status'],
            "timestamp": row['timestamp'],
            "channel_id": row['channel_id'],
            "reason": row['reason']
        }
    return records


def get_bot_presence():
    """Returns the persisted bot presence state."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT status_type, status_text, updated_at FROM bot_state WHERE id = 1")
    row = c.fetchone()
    conn.close()
    if not row:
        return {"status_type": None, "status_text": None, "updated_at": None}
    return dict(row)


def set_bot_presence(status_type, status_text):
    """Persists bot presence state."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO bot_state (id, status_type, status_text, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            status_type=excluded.status_type,
            status_text=excluded.status_text,
            updated_at=excluded.updated_at
        """,
        (status_type, status_text, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_dashboard_snapshot():
    """Returns lightweight data for the web dashboard."""
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) AS count FROM guild_configs")
        guild_count = c.fetchone()["count"]

        c.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM attendance_records
            GROUP BY status
            """
        )
        status_counts = {"present": 0, "absent": 0, "excused": 0}
        for row in c.fetchall():
            key = (row["status"] or "").lower()
            if key in status_counts:
                status_counts[key] = row["count"]

        c.execute("SELECT COUNT(*) AS count FROM custom_commands")
        custom_command_count = c.fetchone()["count"]
        conn.close()
    except sqlite3.OperationalError:
        init_db()
        return get_dashboard_snapshot()

    return {
        "guild_count": guild_count,
        "custom_command_count": custom_command_count,
        "attendance_totals": status_counts,
        "presence": get_bot_presence()
    }

def add_or_update_record(guild_id, user_id, status, timestamp, channel_id=None, reason=None):
    """Adds or updates an attendance record."""
    conn = get_connection()
    c = conn.cursor()
    
    # Upsert logic
    c.execute('''INSERT INTO attendance_records (guild_id, user_id, status, timestamp, channel_id, reason)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET
                 status=excluded.status,
                 timestamp=excluded.timestamp,
                 channel_id=excluded.channel_id,
                 reason=excluded.reason
    ''', (guild_id, user_id, status, timestamp, channel_id, reason))
    
    # Wait, SQLite UPSERT usually requires a unique constraint to conflict on.
    # We don't have a unique constraint on (guild_id, user_id) because we might want history?
    # But the current bot only stores ONE record per user per guild (current status).
    # So we should probably DELETE old record for this user or UPDATE it.
    
    # Let's clean up: Delete existing record for this user in this guild first
    # (Since we only track 'current' status in the JSON version)
    
    # Actually, let's use a unique constraint if we only want one record per user per day/session.
    # The JSON structure is `records: { "user_id": { ... } }`, so only one active record per user.
    
    conn.rollback() # Undo the insert above
    
    # Delete previous record for this user
    c.execute('DELETE FROM attendance_records WHERE guild_id = ? AND user_id = ?', (guild_id, user_id))
    
    # Insert new
    c.execute('''INSERT INTO attendance_records (guild_id, user_id, status, timestamp, channel_id, reason)
                 VALUES (?, ?, ?, ?, ?, ?)''', (guild_id, user_id, status, timestamp, channel_id, reason))
    
    conn.commit()
    conn.close()
    write_snapshot()

def replace_all_records(guild_id, records_dict):
    """Replaces all attendance records for a guild (bulk save)."""
    conn = get_connection()
    c = conn.cursor()
    
    # Transaction
    try:
        # Delete all existing
        c.execute('DELETE FROM attendance_records WHERE guild_id = ?', (guild_id,))
        
        # Insert new
        # records_dict is {user_id: {status, timestamp, channel_id, reason}}
        to_insert = []
        for uid, info in records_dict.items():
            to_insert.append((
                guild_id, 
                uid, 
                info.get('status', 'present'), 
                info.get('timestamp'), 
                info.get('channel_id'),
                info.get('reason')
            ))
            
        if to_insert:
            c.executemany('''INSERT INTO attendance_records (guild_id, user_id, status, timestamp, channel_id, reason)
                             VALUES (?, ?, ?, ?, ?, ?)''', to_insert)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to replace records for guild {guild_id}: {e}")
        raise
    finally:
        conn.close()
    write_snapshot()

def clear_attendance_records(guild_id):
    """Clears all attendance records for a guild (e.g., reset)."""
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM attendance_records WHERE guild_id = ?', (guild_id,))
    conn.commit()
    conn.close()
    write_snapshot()

def clear_attendance_stats(guild_id):
    """Clears all attendance stats (present/absent/excused counts) for a guild."""
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM attendance_stats WHERE guild_id = ?', (guild_id,))
    conn.commit()
    conn.close()
    write_snapshot()

def increment_status_count(guild_id, user_id, status, count=1):
    conn = get_connection()
    c = conn.cursor()
    present = 0
    absent = 0
    excused = 0
    if status == 'present':
        present = count
        column = 'present_count'
    elif status == 'absent':
        absent = count
        column = 'absent_count'
    elif status == 'excused':
        excused = count
        column = 'excused_count'
    else:
        conn.close()
        return
    c.execute(
        f'''INSERT INTO attendance_stats (guild_id, user_id, present_count, absent_count, excused_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET {column} = {column} + ?''',
        (guild_id, user_id, present, absent, excused, count)
    )
    conn.commit()
    conn.close()
    write_snapshot()


def add_staff_attendance_log(guild_id, user_id, action, note, timestamp):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO staff_attendance_logs (guild_id, user_id, action, note, timestamp)
           VALUES (?, ?, ?, ?, ?)''',
        (guild_id, user_id, action, note, timestamp)
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_last_staff_attendance_action(guild_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT action, note, timestamp
           FROM staff_attendance_logs
           WHERE guild_id = ? AND user_id = ?
           ORDER BY timestamp DESC, id DESC
           LIMIT 1''',
        (guild_id, user_id)
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_open_staff_signin(guild_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT action, note, timestamp
           FROM staff_attendance_logs
           WHERE guild_id = ? AND user_id = ?
           ORDER BY timestamp DESC, id DESC''',
        (guild_id, user_id)
    )
    rows = c.fetchall()
    conn.close()
    if not rows:
        return None
    for row in rows:
        action = (row["action"] or "").lower()
        if action == "sign_out":
            return None
        if action == "sign_in":
            return dict(row)
    return None


def get_open_staff_signins(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT s.user_id, s.note, s.timestamp
           FROM staff_attendance_logs s
           WHERE s.guild_id = ?
             AND LOWER(s.action) = 'sign_in'
             AND NOT EXISTS (
                 SELECT 1
                 FROM staff_attendance_logs o
                 WHERE o.guild_id = s.guild_id
                   AND o.user_id = s.user_id
                   AND LOWER(o.action) = 'sign_out'
                   AND (
                       o.timestamp > s.timestamp
                       OR (o.timestamp = s.timestamp AND o.id > s.id)
                   )
             )''',
        (guild_id,)
    )
    rows = c.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_staff_duty_count(guild_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT COUNT(*) AS duty_count
           FROM staff_attendance_logs
           WHERE guild_id = ? AND user_id = ? AND LOWER(action) = 'sign_in' ''',
        (guild_id, user_id)
    )
    row = c.fetchone()
    conn.close()
    return int(row['duty_count']) if row else 0

def get_attendance_leaderboard_count(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) AS total FROM attendance_stats WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    return row['total'] if row else 0


def get_attendance_leaderboard(guild_id, limit=10, offset=0):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT user_id, present_count, absent_count, excused_count
           FROM attendance_stats
           WHERE guild_id = ?
           ORDER BY present_count DESC, user_id ASC
           LIMIT ? OFFSET ?''',
        (guild_id, limit, offset)
    )
    rows = c.fetchall()
    conn.close()
    return rows


def get_custom_commands(guild_id):
    """Returns all custom commands for a guild keyed by normalized command name."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT command_name, response_text
           FROM custom_commands
           WHERE guild_id = ?
           ORDER BY command_name ASC''',
        (guild_id,)
    )
    rows = c.fetchall()
    conn.close()
    return {row['command_name']: row['response_text'] for row in rows}


def get_custom_command(guild_id, command_name):
    """Returns the response text for one custom command."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT response_text
           FROM custom_commands
           WHERE guild_id = ? AND command_name = ?''',
        (guild_id, command_name)
    )
    row = c.fetchone()
    conn.close()
    return row['response_text'] if row else None


def upsert_custom_command(guild_id, command_name, response_text):
    """Creates or updates a custom command for a guild."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO custom_commands (guild_id, command_name, response_text)
           VALUES (?, ?, ?)
           ON CONFLICT(guild_id, command_name) DO UPDATE SET
           response_text = excluded.response_text''',
        (guild_id, command_name, response_text)
    )
    conn.commit()
    conn.close()
    write_snapshot()


def delete_custom_command(guild_id, command_name):
    """Deletes a custom command and returns whether a row was removed."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'DELETE FROM custom_commands WHERE guild_id = ? AND command_name = ?',
        (guild_id, command_name)
    )
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def get_game_progress(guild_id, user_id, game_name):
    """Returns saved progress text for a user's game, if present."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT progress_text
           FROM game_progress
           WHERE guild_id = ? AND user_id = ? AND game_name = ?''',
        (guild_id, user_id, game_name)
    )
    row = c.fetchone()
    conn.close()
    return row['progress_text'] if row else None


def upsert_game_progress(guild_id, user_id, game_name, progress_text):
    """Creates or updates persisted game progress."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO game_progress (guild_id, user_id, game_name, progress_text, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(guild_id, user_id, game_name) DO UPDATE SET
           progress_text = excluded.progress_text,
           updated_at = excluded.updated_at''',
        (guild_id, user_id, game_name, progress_text, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def clear_game_progress(guild_id, user_id, game_name):
    """Clears saved game progress for one user/game pair."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'DELETE FROM game_progress WHERE guild_id = ? AND user_id = ? AND game_name = ?',
        (guild_id, user_id, game_name)
    )
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def get_autonick_rules(guild_id):
    """Returns all role->tag autonick rules for a guild."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT role_id, tag
           FROM autonick_rules
           WHERE guild_id = ?
           ORDER BY role_id ASC''',
        (guild_id,)
    )
    rows = c.fetchall()
    conn.close()
    return {int(row['role_id']): row['tag'] for row in rows}


def get_pet_profile(guild_id, user_id):
    """Returns one virtual pet profile row as a dict."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT *
           FROM pet_profiles
           WHERE guild_id = ? AND user_id = ?''',
        (guild_id, user_id)
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_staff_tracker_strikes(guild_id, staff_user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT strike_count
           FROM staff_tracker_strikes
           WHERE guild_id = ? AND staff_user_id = ?''',
        (guild_id, staff_user_id)
    )
    row = c.fetchone()
    conn.close()
    return int(row["strike_count"]) if row else 0


def increment_staff_tracker_strike(guild_id, staff_user_id):
    now_iso = datetime.utcnow().isoformat() + "Z"
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO staff_tracker_strikes (guild_id, staff_user_id, strike_count, updated_at)
           VALUES (?, ?, 1, ?)
           ON CONFLICT(guild_id, staff_user_id) DO UPDATE SET
               strike_count = strike_count + 1,
               updated_at = excluded.updated_at''',
        (guild_id, staff_user_id, now_iso)
    )
    c.execute(
        '''SELECT strike_count
           FROM staff_tracker_strikes
           WHERE guild_id = ? AND staff_user_id = ?''',
        (guild_id, staff_user_id)
    )
    row = c.fetchone()
    conn.commit()
    conn.close()
    write_snapshot()
    return int(row["strike_count"]) if row else 1


def add_staff_tracker_case(
    guild_id,
    staff_user_id,
    target_user_id,
    action,
    reason_text,
    strike_count,
    punishment_applied,
    audit_entry_id
):
    now_iso = datetime.utcnow().isoformat() + "Z"
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO staff_tracker_cases (
               case_id, guild_id, staff_user_id, target_user_id, action, reason_text,
               strike_count, punishment_applied, audit_entry_id, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            "__PENDING__",
            guild_id,
            staff_user_id,
            target_user_id,
            action,
            reason_text,
            strike_count,
            punishment_applied,
            audit_entry_id,
            now_iso,
        )
    )
    row_id = c.lastrowid
    case_id = f"ST-{1000 + row_id}"
    c.execute("UPDATE staff_tracker_cases SET case_id = ? WHERE id = ?", (case_id, row_id))
    conn.commit()
    conn.close()
    write_snapshot()
    return case_id


def upsert_pet_profile(guild_id, user_id, payload):
    """Creates or updates a user's pet profile."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO pet_profiles (
               guild_id, user_id, pet_name, pet_type, hunger, happiness, cleanliness,
               energy, bond, coins, streak, total_checkins, last_checkin_date,
               adopted_at, updated_at, last_fed_at, last_played_at, last_cleaned_at,
               last_slept_at, evolved_stage
           ) VALUES (
               :guild_id, :user_id, :pet_name, :pet_type, :hunger, :happiness, :cleanliness,
               :energy, :bond, :coins, :streak, :total_checkins, :last_checkin_date,
               :adopted_at, :updated_at, :last_fed_at, :last_played_at, :last_cleaned_at,
               :last_slept_at, :evolved_stage
           )
           ON CONFLICT(guild_id, user_id) DO UPDATE SET
               pet_name=excluded.pet_name,
               pet_type=excluded.pet_type,
               hunger=excluded.hunger,
               happiness=excluded.happiness,
               cleanliness=excluded.cleanliness,
               energy=excluded.energy,
               bond=excluded.bond,
               coins=excluded.coins,
               streak=excluded.streak,
               total_checkins=excluded.total_checkins,
               last_checkin_date=excluded.last_checkin_date,
               adopted_at=excluded.adopted_at,
               updated_at=excluded.updated_at,
               last_fed_at=excluded.last_fed_at,
               last_played_at=excluded.last_played_at,
               last_cleaned_at=excluded.last_cleaned_at,
               last_slept_at=excluded.last_slept_at,
               evolved_stage=excluded.evolved_stage''',
        {
            "guild_id": guild_id,
            "user_id": user_id,
            **payload
        }
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_message_streak(guild_id, user_id):
    """Returns one user's message streak row as a dict."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT guild_id, user_id, streak_days, last_claim_date, last_claim_window, updated_at
           FROM message_streaks
           WHERE guild_id = ? AND user_id = ?''',
        (guild_id, user_id)
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_message_streak(guild_id, user_id, payload):
    """Creates or updates a user's message streak state."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO message_streaks (
               guild_id, user_id, streak_days, last_claim_date, last_claim_window, updated_at
           ) VALUES (
               :guild_id, :user_id, :streak_days, :last_claim_date, :last_claim_window, :updated_at
           )
           ON CONFLICT(guild_id, user_id) DO UPDATE SET
               streak_days=excluded.streak_days,
               last_claim_date=excluded.last_claim_date,
               last_claim_window=excluded.last_claim_window,
               updated_at=excluded.updated_at''',
        {
            "guild_id": guild_id,
            "user_id": user_id,
            **payload
        }
    )
    conn.commit()
    conn.close()
    write_snapshot()


def clear_pet_profile(guild_id, user_id):
    """Deletes a user's pet profile."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'DELETE FROM pet_profiles WHERE guild_id = ? AND user_id = ?',
        (guild_id, user_id)
    )
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def upsert_autonick_rule(guild_id, role_id, tag):
    """Creates or updates one autonick role rule."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO autonick_rules (guild_id, role_id, tag)
           VALUES (?, ?, ?)
           ON CONFLICT(guild_id, role_id) DO UPDATE SET
           tag = excluded.tag''',
        (guild_id, role_id, tag)
    )
    conn.commit()
    conn.close()
    write_snapshot()


def delete_autonick_rule(guild_id, role_id):
    """Deletes one autonick role rule. Returns True if deleted."""
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'DELETE FROM autonick_rules WHERE guild_id = ? AND role_id = ?',
        (guild_id, role_id)
    )
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def upsert_fmbot_link(guild_id, user_id, username):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO fmbot_links (guild_id, user_id, username, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(guild_id, user_id) DO UPDATE SET
           username = excluded.username,
           updated_at = excluded.updated_at''',
        (guild_id, user_id, username, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_fmbot_link(guild_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT username FROM fmbot_links WHERE guild_id = ? AND user_id = ?',
        (guild_id, user_id)
    )
    row = c.fetchone()
    conn.close()
    return row["username"] if row else None


def upsert_birthday(guild_id, user_id, birthday_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO birthday_entries (guild_id, user_id, birthday_date, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(guild_id, user_id) DO UPDATE SET
           birthday_date = excluded.birthday_date,
           updated_at = excluded.updated_at''',
        (guild_id, user_id, birthday_date, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_birthday(guild_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT birthday_date FROM birthday_entries WHERE guild_id = ? AND user_id = ?',
        (guild_id, user_id)
    )
    row = c.fetchone()
    conn.close()
    return row["birthday_date"] if row else None


def list_birthdays_for_guild(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT user_id, birthday_date FROM birthday_entries WHERE guild_id = ? ORDER BY birthday_date ASC',
        (guild_id,)
    )
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows


def remove_birthday(guild_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'DELETE FROM birthday_entries WHERE guild_id = ? AND user_id = ?',
        (guild_id, user_id)
    )
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def upsert_birthday_channel(guild_id, channel_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO birthday_channels (guild_id, channel_id, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           channel_id = excluded.channel_id,
           updated_at = excluded.updated_at''',
        (guild_id, channel_id, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_birthday_channel(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT channel_id FROM birthday_channels WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    return row["channel_id"] if row else None


def has_birthday_announcement(guild_id, user_id, sent_on_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT 1 FROM birthday_announcements WHERE guild_id = ? AND user_id = ? AND sent_on_date = ?',
        (guild_id, user_id, sent_on_date)
    )
    row = c.fetchone()
    conn.close()
    return row is not None


def add_birthday_announcement(guild_id, user_id, sent_on_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT OR IGNORE INTO birthday_announcements (guild_id, user_id, sent_on_date)
           VALUES (?, ?, ?)''',
        (guild_id, user_id, sent_on_date)
    )
    inserted = c.rowcount > 0
    conn.commit()
    conn.close()
    if inserted:
        write_snapshot()
    return inserted


def upsert_giveaway_config(guild_id, channel_id, log_channel_id=None, required_role_id=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO giveaway_configs (guild_id, channel_id, log_channel_id, required_role_id, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           channel_id = excluded.channel_id,
           log_channel_id = excluded.log_channel_id,
           required_role_id = excluded.required_role_id,
           updated_at = excluded.updated_at''',
        (guild_id, channel_id, log_channel_id, required_role_id, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_giveaway_config(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT guild_id, channel_id, log_channel_id, required_role_id FROM giveaway_configs WHERE guild_id = ?',
        (guild_id,)
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_giveaway_entry(message_id, guild_id, channel_id, prize, winner_count, required_role_id, end_at, created_by_user_id, status="active"):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO giveaway_entries (message_id, guild_id, channel_id, prize, winner_count, required_role_id, end_at, created_by_user_id, ended_at, status, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
           ON CONFLICT(message_id) DO UPDATE SET
           guild_id = excluded.guild_id,
           channel_id = excluded.channel_id,
           prize = excluded.prize,
           winner_count = excluded.winner_count,
           required_role_id = excluded.required_role_id,
           end_at = excluded.end_at,
           created_by_user_id = excluded.created_by_user_id,
           status = excluded.status,
           updated_at = excluded.updated_at''',
        (
            message_id,
            guild_id,
            channel_id,
            prize,
            winner_count,
            required_role_id,
            end_at,
            created_by_user_id,
            status,
            datetime.utcnow().isoformat() + "Z",
        )
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_giveaway_entry(message_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM giveaway_entries WHERE message_id = ?', (message_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def list_due_giveaways(before_iso_utc):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT * FROM giveaway_entries
           WHERE status = 'active' AND end_at <= ?
           ORDER BY end_at ASC''',
        (before_iso_utc,)
    )
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows


def mark_giveaway_ended(message_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''UPDATE giveaway_entries
           SET status = 'ended', ended_at = ?, updated_at = ?
           WHERE message_id = ?''',
        (datetime.utcnow().isoformat() + "Z", datetime.utcnow().isoformat() + "Z", message_id)
    )
    updated = c.rowcount > 0
    conn.commit()
    conn.close()
    if updated:
        write_snapshot()
    return updated


def upsert_poll_entry(message_id, guild_id, channel_id, question, description, choices, created_by_user_id, end_at, status="active"):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO poll_entries (message_id, guild_id, channel_id, question, description, choices_json, created_by_user_id, end_at, status, closed_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
           ON CONFLICT(message_id) DO UPDATE SET
           guild_id = excluded.guild_id,
           channel_id = excluded.channel_id,
           question = excluded.question,
           description = excluded.description,
           choices_json = excluded.choices_json,
           created_by_user_id = excluded.created_by_user_id,
           end_at = excluded.end_at,
           status = excluded.status,
           updated_at = excluded.updated_at''',
        (
            message_id,
            guild_id,
            channel_id,
            question,
            description,
            json.dumps(choices),
            created_by_user_id,
            end_at,
            status,
            datetime.utcnow().isoformat() + "Z",
        )
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_poll_entry(message_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM poll_entries WHERE message_id = ?', (message_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    data = dict(row)
    data["choices"] = json.loads(data.pop("choices_json") or "[]")
    return data


def list_due_polls(before_iso_utc):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT * FROM poll_entries
           WHERE status = 'active' AND end_at <= ?
           ORDER BY end_at ASC''',
        (before_iso_utc,)
    )
    rows = []
    for row in c.fetchall():
        data = dict(row)
        data["choices"] = json.loads(data.pop("choices_json") or "[]")
        rows.append(data)
    conn.close()
    return rows


def add_or_update_poll_vote(message_id, user_id, choice_index):
    now = datetime.utcnow().isoformat() + "Z"
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO poll_votes (message_id, user_id, choice_index, voted_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(message_id, user_id) DO UPDATE SET
           choice_index = excluded.choice_index,
           voted_at = excluded.voted_at''',
        (message_id, user_id, choice_index, now)
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_poll_vote_counts(message_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT choice_index, COUNT(*) AS vote_count
           FROM poll_votes
           WHERE message_id = ?
           GROUP BY choice_index''',
        (message_id,)
    )
    counts = {int(row["choice_index"]): int(row["vote_count"]) for row in c.fetchall()}
    conn.close()
    return counts


def get_poll_total_voters(message_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) AS count FROM poll_votes WHERE message_id = ?', (message_id,))
    row = c.fetchone()
    conn.close()
    return int(row["count"]) if row else 0


def mark_poll_closed(message_id):
    now = datetime.utcnow().isoformat() + "Z"
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''UPDATE poll_entries
           SET status = 'closed', closed_at = ?, updated_at = ?
           WHERE message_id = ?''',
        (now, now, message_id)
    )
    updated = c.rowcount > 0
    conn.commit()
    conn.close()
    if updated:
        write_snapshot()
    return updated

def upsert_anniversary_config(guild_id, role_id, anniversary_date_md, milestone_years, channel_id=None, ping_role_id=None, message_template=None):
    conn = get_connection()
    c = conn.cursor()
    normalized = sorted(set(int(year) for year in milestone_years if int(year) > 0))
    c.execute(
        '''INSERT INTO anniversary_configs (guild_id, role_id, anniversary_date_md, milestone_years_json, channel_id, ping_role_id, message_template, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           role_id = excluded.role_id,
           anniversary_date_md = excluded.anniversary_date_md,
           milestone_years_json = excluded.milestone_years_json,
           channel_id = excluded.channel_id,
           ping_role_id = excluded.ping_role_id,
           message_template = excluded.message_template,
           updated_at = excluded.updated_at''',
        (
            guild_id,
            role_id,
            anniversary_date_md,
            json.dumps(normalized),
            channel_id,
            ping_role_id,
            message_template,
            datetime.utcnow().isoformat() + "Z",
        )
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_anniversary_config(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT guild_id, role_id, anniversary_date_md, milestone_years_json, channel_id, ping_role_id, message_template FROM anniversary_configs WHERE guild_id = ?',
        (guild_id,)
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    data = dict(row)
    data["milestone_years"] = json.loads(data.pop("milestone_years_json") or "[]")
    return data


def has_anniversary_award(guild_id, user_id, milestone_years, award_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT 1 FROM anniversary_awards WHERE guild_id = ? AND user_id = ? AND milestone_years = ? AND award_date = ?',
        (guild_id, user_id, milestone_years, award_date)
    )
    row = c.fetchone()
    conn.close()
    return row is not None


def add_anniversary_award(guild_id, user_id, milestone_years, award_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT OR IGNORE INTO anniversary_awards (guild_id, user_id, milestone_years, award_date)
           VALUES (?, ?, ?, ?)''',
        (guild_id, user_id, milestone_years, award_date)
    )
    inserted = c.rowcount > 0
    conn.commit()
    conn.close()
    if inserted:
        write_snapshot()
    return inserted


def upsert_gartic_game(guild_id, host_user_id, channel_id, status, current_word, players, scores):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO gartic_games (guild_id, host_user_id, channel_id, status, current_word, scores_json, players_json, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           host_user_id = excluded.host_user_id,
           channel_id = excluded.channel_id,
           status = excluded.status,
           current_word = excluded.current_word,
           scores_json = excluded.scores_json,
           players_json = excluded.players_json,
           updated_at = excluded.updated_at''',
        (
            guild_id, host_user_id, channel_id, status, current_word,
            json.dumps(scores), json.dumps(players), datetime.utcnow().isoformat() + "Z"
        )
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_gartic_game(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM gartic_games WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    payload = dict(row)
    payload["scores"] = json.loads(payload.pop("scores_json") or "{}")
    payload["players"] = json.loads(payload.pop("players_json") or "[]")
    return payload


def clear_gartic_game(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM gartic_games WHERE guild_id = ?', (guild_id,))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def upsert_ticket_panel(guild_id, channel_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO ticket_panels (guild_id, channel_id, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           channel_id = excluded.channel_id,
           updated_at = excluded.updated_at''',
        (guild_id, channel_id, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_ticket_panel(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT channel_id FROM ticket_panels WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    return row["channel_id"] if row else None


def open_ticket(guild_id, channel_id, opener_user_id, reason):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO ticket_entries (guild_id, channel_id, opener_user_id, reason, is_open, created_at, closed_at)
           VALUES (?, ?, ?, ?, 1, ?, NULL)
           ON CONFLICT(guild_id, channel_id) DO UPDATE SET
           opener_user_id = excluded.opener_user_id,
           reason = excluded.reason,
           is_open = 1,
           created_at = excluded.created_at,
           closed_at = NULL''',
        (guild_id, channel_id, opener_user_id, reason, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_ticket_entry(guild_id, channel_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT * FROM ticket_entries WHERE guild_id = ? AND channel_id = ?',
        (guild_id, channel_id)
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def close_ticket(guild_id, channel_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''UPDATE ticket_entries
           SET is_open = 0, closed_at = ?
           WHERE guild_id = ? AND channel_id = ?''',
        (datetime.utcnow().isoformat() + "Z", guild_id, channel_id)
    )
    updated = c.rowcount > 0
    conn.commit()
    conn.close()
    if updated:
        write_snapshot()
    return updated


def upsert_ticket_settings(guild_id, settings):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO ticket_settings (guild_id, settings_json, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           settings_json = excluded.settings_json,
           updated_at = excluded.updated_at''',
        (guild_id, json.dumps(settings), datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_ticket_settings(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT settings_json FROM ticket_settings WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    try:
        return json.loads(row["settings_json"] or "{}")
    except json.JSONDecodeError:
        return {}


def count_open_tickets_for_user(guild_id, opener_user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        'SELECT COUNT(*) AS count FROM ticket_entries WHERE guild_id = ? AND opener_user_id = ? AND is_open = 1',
        (guild_id, opener_user_id)
    )
    row = c.fetchone()
    conn.close()
    return int(row["count"]) if row else 0


def add_tod_prompt(guild_id, prompt_type, prompt_text, created_by_user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO tod_prompts (guild_id, prompt_type, prompt_text, created_by_user_id, created_at)
           VALUES (?, ?, ?, ?, ?)''',
        (guild_id, prompt_type, prompt_text, created_by_user_id, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()
    write_snapshot()


def list_tod_prompts(guild_id, prompt_type=None):
    conn = get_connection()
    c = conn.cursor()
    if prompt_type:
        c.execute(
            '''SELECT prompt_type, prompt_text
               FROM tod_prompts
               WHERE guild_id = ? AND prompt_type = ?
               ORDER BY created_at DESC''',
            (guild_id, prompt_type)
        )
    else:
        c.execute(
            '''SELECT prompt_type, prompt_text
               FROM tod_prompts
               WHERE guild_id = ?
               ORDER BY created_at DESC''',
            (guild_id,)
        )
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows


def upsert_tod_lobby(guild_id, host_user_id, channel_id, players, status="open"):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO tod_lobbies (guild_id, host_user_id, channel_id, players_json, status, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET
           host_user_id = excluded.host_user_id,
           channel_id = excluded.channel_id,
           players_json = excluded.players_json,
           status = excluded.status,
           updated_at = excluded.updated_at''',
        (
            guild_id, host_user_id, channel_id, json.dumps(players),
            status, datetime.utcnow().isoformat() + "Z"
        )
    )
    conn.commit()
    conn.close()
    write_snapshot()


def get_tod_lobby(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM tod_lobbies WHERE guild_id = ?', (guild_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    payload = dict(row)
    payload["players"] = json.loads(payload.pop("players_json") or "[]")
    return payload


def clear_tod_lobby(guild_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM tod_lobbies WHERE guild_id = ?', (guild_id,))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    if deleted:
        write_snapshot()
    return deleted


def add_meeting_attendance(guild_id, user_id, username, meeting_date, message_id=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT OR IGNORE INTO meeting_attendance_records
           (guild_id, user_id, username, meeting_date, message_id, timestamp)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (
            guild_id,
            user_id,
            username,
            meeting_date,
            message_id,
            datetime.utcnow().isoformat() + "Z",
        ),
    )
    inserted = c.rowcount > 0
    conn.commit()
    conn.close()
    if inserted:
        write_snapshot()
    return inserted


def has_meeting_reminder_sent(guild_id, meeting_date, reminder_key):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''SELECT 1 FROM meeting_reminder_history
           WHERE guild_id = ? AND meeting_date = ? AND reminder_key = ?''',
        (guild_id, meeting_date, reminder_key),
    )
    row = c.fetchone()
    conn.close()
    return bool(row)


def mark_meeting_reminder_sent(guild_id, meeting_date, reminder_key):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT OR IGNORE INTO meeting_reminder_history
           (guild_id, meeting_date, reminder_key, sent_at)
           VALUES (?, ?, ?, ?)''',
        (
            guild_id,
            meeting_date,
            reminder_key,
            datetime.utcnow().isoformat() + "Z",
        ),
    )
    inserted = c.rowcount > 0
    conn.commit()
    conn.close()
    if inserted:
        write_snapshot()
    return inserted
