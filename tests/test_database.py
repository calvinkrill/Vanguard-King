from pathlib import Path

import database


def test_resolve_db_file_prefers_db_file_env(monkeypatch):
    monkeypatch.setenv("DB_FILE", "/tmp/custom.db")
    monkeypatch.delenv("DB_DIR", raising=False)
    assert database.resolve_db_file() == "/tmp/custom.db"


def test_resolve_db_file_uses_db_dir_env(monkeypatch):
    monkeypatch.delenv("DB_FILE", raising=False)
    monkeypatch.setenv("DB_DIR", "/tmp/data-dir")
    assert database.resolve_db_file() == "/tmp/data-dir/attendance.db"


def test_resolve_db_file_requires_persistent_storage_when_enabled(monkeypatch):
    monkeypatch.delenv("DB_FILE", raising=False)
    monkeypatch.delenv("DB_DIR", raising=False)
    monkeypatch.delenv("RAILWAY_VOLUME_MOUNT_PATH", raising=False)
    monkeypatch.delenv("RENDER_DISK_PATH", raising=False)
    monkeypatch.delenv("PERSISTENT_VOLUME_DIR", raising=False)
    monkeypatch.setenv("REQUIRE_PERSISTENT_STORAGE", "1")

    original_exists = database.Path.exists
    original_is_dir = database.Path.is_dir

    def fake_exists(path_obj):
        if str(path_obj) == "/data":
            return False
        return original_exists(path_obj)

    def fake_is_dir(path_obj):
        if str(path_obj) == "/data":
            return False
        return original_is_dir(path_obj)

    monkeypatch.setattr(database.Path, "exists", fake_exists)
    monkeypatch.setattr(database.Path, "is_dir", fake_is_dir)

    try:
        database.resolve_db_file()
    except RuntimeError as exc:
        assert "REQUIRE_PERSISTENT_STORAGE=1" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when persistent storage is required")


def test_ensure_parent_directory_creates_nested_parent(tmp_path):
    target_file = tmp_path / "nested" / "deeper" / "attendance.db"
    assert not target_file.parent.exists()
    database.ensure_parent_directory(target_file)
    assert target_file.parent.exists()
    assert Path(target_file.parent).is_dir()


def test_staff_tracker_strikes_increment_and_case_creation(tmp_path, monkeypatch):
    db_path = tmp_path / "attendance.db"
    snapshot_path = tmp_path / "snapshot.json"
    monkeypatch.setattr(database, "DB_FILE", str(db_path))
    monkeypatch.setattr(database, "SNAPSHOT_FILE", str(snapshot_path))
    database.init_db()

    guild_id = 1001
    staff_user_id = 2002
    target_user_id = 3003

    assert database.get_staff_tracker_strikes(guild_id, staff_user_id) == 0
    assert database.increment_staff_tracker_strike(guild_id, staff_user_id) == 1
    assert database.increment_staff_tracker_strike(guild_id, staff_user_id) == 2
    assert database.get_staff_tracker_strikes(guild_id, staff_user_id) == 2

    case_id = database.add_staff_tracker_case(
        guild_id=guild_id,
        staff_user_id=staff_user_id,
        target_user_id=target_user_id,
        action="timeout_without_reason",
        reason_text="",
        strike_count=2,
        punishment_applied="Logged only",
        audit_entry_id=555,
    )
    assert case_id.startswith("ST-")


def test_poll_entry_vote_and_close_flow(tmp_path, monkeypatch):
    db_path = tmp_path / "attendance.db"
    snapshot_path = tmp_path / "snapshot.json"
    monkeypatch.setattr(database, "DB_FILE", str(db_path))
    monkeypatch.setattr(database, "SNAPSHOT_FILE", str(snapshot_path))
    database.init_db()

    database.upsert_poll_entry(
        message_id=111,
        guild_id=222,
        channel_id=333,
        question="Ship the feature?",
        description="Quick vote",
        choices=["Yes", "No", "Abstain"],
        created_by_user_id=444,
        end_at="2099-01-01T00:00:00Z",
    )
    entry = database.get_poll_entry(111)
    assert entry is not None
    assert entry["choices"] == ["Yes", "No", "Abstain"]

    database.add_or_update_poll_vote(111, user_id=1001, choice_index=0)
    database.add_or_update_poll_vote(111, user_id=1002, choice_index=1)
    database.add_or_update_poll_vote(111, user_id=1002, choice_index=0)
    counts = database.get_poll_vote_counts(111)
    assert counts[0] == 2
    assert database.get_poll_total_voters(111) == 2

    assert database.mark_poll_closed(111) is True
    closed_entry = database.get_poll_entry(111)
    assert closed_entry["status"] == "closed"
