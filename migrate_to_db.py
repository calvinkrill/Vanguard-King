import os
import json
import sqlite3
from database import init_db, update_guild_config, add_or_update_record, DB_FILE

DATA_DIR = "data"

def migrate():
    print("Initializing database...")
    init_db()
    
    if not os.path.exists(DATA_DIR):
        print("No data directory found. Skipping migration.")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    print(f"Found {len(files)} guild data files to migrate.")

    for filename in files:
        guild_id_str = filename.replace('.json', '')
        try:
            guild_id = int(guild_id_str)
        except ValueError:
            print(f"Skipping invalid filename: {filename}")
            continue
            
        filepath = os.path.join(DATA_DIR, filename)
        print(f"Migrating guild {guild_id} from {filepath}...")
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue
            
        # 1. Migrate Settings & Config
        settings = data.get('settings', {})
        
        config_update = {
            "attendance_role_id": data.get('attendance_role_id'),
            "absent_role_id": data.get('absent_role_id'),
            "excused_role_id": data.get('excused_role_id'),
            "welcome_channel_id": data.get('welcome_channel_id'),
            "report_channel_id": data.get('report_channel_id'),
            "last_report_message_id": data.get('last_report_message_id'),
            "last_report_channel_id": data.get('last_report_channel_id'),
            
            # Flatten settings
            "attendance_mode": settings.get('attendance_mode', 'duration'),
            "attendance_expiry_hours": settings.get('attendance_expiry_hours', 12),
            "window_start_time": settings.get('window_start_time', '08:00'),
            "window_end_time": settings.get('window_end_time', '17:00'),
            "last_processed_date": settings.get('last_processed_date'),
            "last_opened_date": settings.get('last_opened_date'), # Might be missing in some JSONs
            "allow_self_marking": settings.get('allow_self_marking', True),
            "require_admin_excuse": settings.get('require_admin_excuse', False),
            "auto_nick_on_join": settings.get('auto_nick_on_join', False),
            "enforce_suffix": settings.get('enforce_suffix', False),
            "remove_suffix_on_role_loss": settings.get('remove_suffix_on_role_loss', False),
            "suffix_format": settings.get('suffix_format', ' [𝙼𝚂𝚄𝚊𝚗]')
        }
        
        # Filter out None values to let DB defaults apply? 
        # Actually update_guild_config will handle it. 
        # But if key is missing in JSON, we might want to preserve DB default if it's a new row.
        # But here we are creating the row.
        
        update_guild_config(guild_id, **config_update)
        
        # 2. Migrate Records
        records = data.get('records', {})
        count = 0
        for user_id_str, record in records.items():
            try:
                user_id = int(user_id_str)
                # Handle different record formats if any (old string format vs new dict)
                if isinstance(record, str):
                     # Legacy format: just timestamp? or "present"?
                     # Code says: info = {"status": "present", "timestamp": info, "channel_id": None}
                     status = "present"
                     timestamp = record
                     channel_id = None
                     reason = None
                else:
                    status = record.get('status', 'present')
                    timestamp = record.get('timestamp')
                    channel_id = record.get('channel_id')
                    reason = record.get('reason')
                
                add_or_update_record(guild_id, user_id, status, timestamp, channel_id, reason)
                count += 1
            except Exception as e:
                print(f"Failed to migrate record for user {user_id_str}: {e}")
        
        print(f"  - Migrated {count} records.")

    print("Migration complete.")

if __name__ == "__main__":
    migrate()
