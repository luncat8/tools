#!/usr/bin/env python3

import atexit
import fnmatch
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    _HAS_WATCHDOG = True
except Exception:  # pragma: no cover - optional dependency
    _HAS_WATCHDOG = False

    class FileSystemEventHandler:  # type: ignore
        pass

    class Observer:  # type: ignore
        pass


# ===== CONFIGURATION =====

SOURCE_FOLDERS = [
    r"/home/user/fluid",
    r"/home/user/galaxy-flythrough",
]

DESTINATION_FOLDER = r"/mnt/shared/1"

EXCLUDE_FOLDERS = {
    ".git",
}

EXCLUDE_FILES = []

EXCLUDE_HIDDEN = False

BASE_DIR = Path(__file__).resolve().parent
HASH_DB_FILE = BASE_DIR / "sync_share.json"

DEBOUNCE_SECONDS = 2.0

MAX_WAIT_SECONDS = 10.0

FILE_STABILITY_SECONDS = 0.0

DELETE_GRACE_SECONDS = 0.0

HASH_CHUNK_SIZE = 1024 * 1024

# Persist the hash database to disk after each scan. Set to False to keep
# it in memory only (no sync_share.json writes).
SAVE_HASH_DB = True

# Debounce delay before flushing the in-memory hash database to disk.
HASH_DB_SAVE_DEBOUNCE_SECONDS = 2.0

_LAST_NO_NEWLINE = False

# In-memory hash database state for debounced persistence.
_DB_INSTANCE = None
_DB_DIRTY = False
_DB_SAVE_TIMER = None
_DB_SAVE_LOCK = threading.Lock()


def should_exclude(path, is_dir=False):
    """Return whether a file or directory should be excluded."""
    path = os.fspath(path)
    name = os.path.basename(path)

    if EXCLUDE_HIDDEN and name.startswith("."):
        return True

    if is_dir and name in EXCLUDE_FOLDERS:
        return True

    if not is_dir:
        return any(
            fnmatch.fnmatch(name, pattern)
            for pattern in EXCLUDE_FILES
        )

    return False


def is_excluded_path(path, is_dir=False):
    """Check every component of a path for excluded directories."""
    path = Path(path)

    if should_exclude(path, is_dir=is_dir):
        return True

    for parent in path.parents:
        if parent == parent.parent:
            break
        if should_exclude(parent, is_dir=True):
            return True

    return False


def file_signature(path):
    """Return size and nanosecond modification time, or None if unavailable."""
    try:
        stat = os.stat(path)
        return stat.st_size, stat.st_mtime_ns
    except OSError:
        return None


def calculate_file_hash(path):
    """Calculate a SHA-256 hash, returning None if the file cannot be read."""
    digest = hashlib.sha256()

    try:
        with open(path, "rb") as file:
            for chunk in iter(
                lambda: file.read(HASH_CHUNK_SIZE),
                b"",
            ):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as error:
        print(f"ERROR: Could not read {path}: {error}")
        return None


def load_hash_db():
    """Load the hash database, returning an empty database on failure."""
    try:
        with open(HASH_DB_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
            if isinstance(data, dict):
                return data
    except FileNotFoundError:
        pass
    except (OSError, json.JSONDecodeError) as error:
        print(f"WARNING: Could not load hash database: {error}")

    return {}


def save_hash_db(db):
    """Write the database to a temporary file and replace it atomically."""
    db_path = Path(HASH_DB_FILE)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=db_path.parent,
            prefix=f".{db_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp_path = Path(file.name)
            json.dump(db, file, indent=2, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temp_path, db_path)
    except OSError as error:
        print(f"ERROR: Could not save hash database: {error}")
        if temp_path:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False

    return True


def _mark_hash_db_dirty():
    """Mark the in-memory hash database as needing persistence."""
    global _DB_DIRTY
    _DB_DIRTY = True


def schedule_hash_db_save(delay=None):
    """Schedule a debounced save of the hash database to disk."""
    global _DB_SAVE_TIMER

    if not SAVE_HASH_DB:
        return

    if delay is None:
        delay = HASH_DB_SAVE_DEBOUNCE_SECONDS

    with _DB_SAVE_LOCK:
        if _DB_SAVE_TIMER is not None:
            _DB_SAVE_TIMER.cancel()
        timer = threading.Timer(delay, _flush_hash_db_save)
        timer.daemon = True
        _DB_SAVE_TIMER = timer
        timer.start()


def _flush_hash_db_save():
    """Persist the hash database if it is dirty."""
    global _DB_DIRTY, _DB_SAVE_TIMER

    with _DB_SAVE_LOCK:
        _DB_SAVE_TIMER = None
        dirty = _DB_DIRTY
        _DB_DIRTY = False

    if dirty and SAVE_HASH_DB and _DB_INSTANCE is not None:
        if not save_hash_db(_DB_INSTANCE):
            _mark_hash_db_dirty()


def flush_hash_db_save():
    """Cancel any pending debounced save and flush immediately if dirty."""
    global _DB_DIRTY, _DB_SAVE_TIMER

    with _DB_SAVE_LOCK:
        if _DB_SAVE_TIMER is not None:
            _DB_SAVE_TIMER.cancel()
            _DB_SAVE_TIMER = None
        dirty = _DB_DIRTY
        _DB_DIRTY = False

    if dirty and SAVE_HASH_DB and _DB_INSTANCE is not None:
        save_hash_db(_DB_INSTANCE)


def source_target_path(source_file, source_folder):
    """Map a source file to its corresponding target path."""
    relative_path = os.path.relpath(source_file, source_folder)
    source_name = os.path.basename(os.path.normpath(source_folder))
    return os.path.join(DESTINATION_FOLDER, source_name, relative_path)


def copy_file_atomically(source_file, target_file, expected_hash):
    """Copy to a temporary file, verify it, then atomically replace the target."""
    target = Path(target_file)
    target.parent.mkdir(parents=True, exist_ok=True)

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

        shutil.copy2(source_file, temp_path)

        copied_hash = calculate_file_hash(temp_path)
        if copied_hash is None or copied_hash != expected_hash:
            return False

        shutil.copystat(source_file, temp_path)
        os.replace(temp_path, target)
        return True

    except OSError as error:
        print(f"ERROR: Could not copy {source_file}: {error}")
        return False
    finally:
        if temp_path:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass


def wait_until_stable(path):
    """Wait until file size and modification time remain unchanged."""
    first = file_signature(path)
    if first is None:
        return None

    time.sleep(FILE_STABILITY_SECONDS)

    second = file_signature(path)
    if first != second:
        return None

    return second


def scan_sources(db, missing_since):
    """Scan available source folders, sync changed files, and process deletions."""
    global _LAST_NO_NEWLINE

    started = time.monotonic()
    counts = {"new": 0, "updated": 0, "deleted": 0}
    had_error = False
    db_changed = False
    scanned_roots = set()
    current_files = set()

    for source_folder in SOURCE_FOLDERS:
        source_root = os.path.abspath(source_folder)

        if not os.path.isdir(source_root):
            print(f"WARNING: Source folder unavailable; skipping: {source_folder}")
            had_error = True
            continue

        root_scan_ok = True

        try:
            for root, dirs, files in os.walk(source_root):
                dirs[:] = [
                    directory
                    for directory in dirs
                    if not should_exclude(
                        os.path.join(root, directory),
                        is_dir=True,
                    )
                ]

                for filename in files:
                    source_file = os.path.abspath(os.path.join(root, filename))

                    if is_excluded_path(source_file):
                        continue

                    current_files.add(source_file)
                    signature = file_signature(source_file)

                    if signature is None:
                        had_error = True
                        continue

                    size, mtime_ns = signature
                    record = db.get(source_file, {})
                    if not isinstance(record, dict):
                        record = {}

                    if (
                        record.get("size") == size
                        and record.get("mtime_ns") == mtime_ns
                        and record.get("hash")
                    ):
                        had_missing = (
                            "missing_since" in record
                            or source_file in missing_since
                        )
                        record.pop("missing_since", None)
                        db[source_file] = record
                        missing_since.pop(source_file, None)
                        if had_missing:
                            db_changed = True
                        continue

                    stable_signature = wait_until_stable(source_file)
                    if stable_signature is None:
                        continue

                    stable_size, stable_mtime_ns = stable_signature
                    current_hash = calculate_file_hash(source_file)
                    if current_hash is None:
                        had_error = True
                        continue

                    if file_signature(source_file) != stable_signature:
                        continue

                    target_file = source_target_path(source_file, source_root)
                    old_hash = record.get("hash")

                    if old_hash == current_hash and os.path.exists(target_file):
                        record.update({
                            "hash": current_hash,
                            "size": stable_size,
                            "mtime_ns": stable_mtime_ns,
                        })
                        record.pop("missing_since", None)
                        db[source_file] = record
                        missing_since.pop(source_file, None)
                        db_changed = True
                        continue

                    if not copy_file_atomically(
                        source_file,
                        target_file,
                        current_hash,
                    ):
                        had_error = True
                        continue

                    if file_signature(source_file) != stable_signature:
                        continue

                    counts["updated" if old_hash else "new"] += 1
                    db[source_file] = {
                        "hash": current_hash,
                        "size": stable_size,
                        "mtime_ns": stable_mtime_ns,
                    }
                    missing_since.pop(source_file, None)
                    db_changed = True

            scanned_roots.add(source_root)

        except OSError as error:
            print(f"ERROR: Could not scan {source_folder}: {error}")
            root_scan_ok = False
            had_error = True

        if not root_scan_ok:
            scanned_roots.discard(source_root)

    now = time.time()

    for source_file, record in list(db.items()):
        if not isinstance(record, dict):
            record = {}

        source_root = next(
            (
                os.path.abspath(folder)
                for folder in SOURCE_FOLDERS
                if source_file == os.path.abspath(folder)
                or source_file.startswith(
                    os.path.abspath(folder) + os.sep
                )
            ),
            None,
        )

        if source_root not in scanned_roots or source_file in current_files:
            had_missing = "missing_since" in record or source_file in missing_since
            record.pop("missing_since", None)
            db[source_file] = record
            missing_since.pop(source_file, None)
            if had_missing:
                db_changed = True
            continue

        absent_since = record.get("missing_since")
        if absent_since is None:
            absent_since = now
            record["missing_since"] = absent_since
            db[source_file] = record
            missing_since[source_file] = absent_since
            db_changed = True
            continue

        if now - float(absent_since) < DELETE_GRACE_SECONDS:
            continue

        target_file = source_target_path(source_file, source_root)

        try:
            if os.path.exists(target_file):
                os.remove(target_file)

                target_root = os.path.join(
                    DESTINATION_FOLDER,
                    os.path.basename(os.path.normpath(source_root)),
                )
                parent = os.path.dirname(target_file)

                while parent != target_root and parent.startswith(target_root):
                    try:
                        os.rmdir(parent)
                    except OSError:
                        break
                    parent = os.path.dirname(parent)

            del db[source_file]
            missing_since.pop(source_file, None)
            counts["deleted"] += 1
            db_changed = True

        except OSError as error:
            print(f"ERROR: Could not remove {target_file}: {error}")
            had_error = True

    if SAVE_HASH_DB and db_changed:
        global _DB_INSTANCE
        _DB_INSTANCE = db
        _mark_hash_db_dirty()
        schedule_hash_db_save()

    elapsed = time.monotonic() - started
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = sum(counts.values())

    if total or had_error:
        if _LAST_NO_NEWLINE:
            print()
        print(
            f"[{timestamp}] Sync finished in {elapsed:.2f}s | "
            f"new: {counts['new']}, "
            f"updated: {counts['updated']}, "
            f"deleted: {counts['deleted']}"
        )
        _LAST_NO_NEWLINE = False
    else:
        print(
            f"\r[{timestamp}] No changes | {elapsed:.2f}s          ",
            end="",
            flush=True,
        )
        _LAST_NO_NEWLINE = True

    return counts, had_error


if _HAS_WATCHDOG:

    class ChangeHandler(FileSystemEventHandler):
        """Signal when a relevant filesystem event occurs."""

        def __init__(self, changed_event):
            self.changed_event = changed_event

        def on_any_event(self, event):
            if is_excluded_path(event.src_path, is_dir=event.is_directory):
                return

            if event.event_type == "moved":
                if is_excluded_path(event.dest_path, is_dir=event.is_directory):
                    return

            self.changed_event.set()


def watch_and_sync():
    """Watch source folders and sync after changes settle."""
    global _DB_INSTANCE
    db = load_hash_db()
    _DB_INSTANCE = db
    missing_since = {}

    print("Initial sync...")
    scan_sources(db, missing_since)

    if not _HAS_WATCHDOG:
        print(f"Watching source folders (polling); interval: {DEBOUNCE_SECONDS}s")
        for folder in SOURCE_FOLDERS:
            print(f"  {folder}")
        print(f"Target folder: {DESTINATION_FOLDER}")
        print("Press Ctrl+C to stop.")

        try:
            while True:
                time.sleep(DEBOUNCE_SECONDS)
                scan_sources(db, missing_since)
        except KeyboardInterrupt:
            print("\nWatching stopped.")
        finally:
            flush_hash_db_save()
        return

    observer = Observer()
    changed_event = threading.Event()
    handler = ChangeHandler(changed_event)

    for folder in SOURCE_FOLDERS:
        source_root = os.path.abspath(folder)
        if not os.path.isdir(source_root):
            print(f"WARNING: Source folder unavailable: {folder}")
            continue

        observer.schedule(handler, source_root, recursive=True)

    print(f"Watching source folders; debounce: {DEBOUNCE_SECONDS}s")
    for folder in SOURCE_FOLDERS:
        print(f"  {folder}")
    print(f"Target folder: {DESTINATION_FOLDER}")
    print("Press Ctrl+C to stop.")

    observer.start()

    try:
        while True:
            changed_event.wait(timeout=MAX_WAIT_SECONDS)

            if changed_event.is_set():
                first_event_time = time.monotonic()

                while True:
                    remaining_max_wait = (
                        MAX_WAIT_SECONDS
                        - (time.monotonic() - first_event_time)
                    )
                    if remaining_max_wait <= 0:
                        break

                    wait_time = min(
                        DEBOUNCE_SECONDS,
                        remaining_max_wait,
                    )

                    changed_event.clear()
                    if not changed_event.wait(timeout=wait_time):
                        break

                    last_event_time = time.monotonic()

                    if time.monotonic() - last_event_time >= DEBOUNCE_SECONDS:
                        break

            changed_event.clear()
            scan_sources(db, missing_since)

    except KeyboardInterrupt:
        print("\nWatching stopped.")
    finally:
        observer.stop()
        observer.join()
        flush_hash_db_save()


atexit.register(flush_hash_db_save)


if __name__ == "__main__":
    watch_and_sync()
