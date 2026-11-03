"""
persistence.py - external (MongoDB) storage for the hosting bot.

Why this exists
---------------
Render's disk is wiped on every redeploy / restart, so anything kept only on local disk
(the SQLite database, customer uploads, files a customer's bot writes) is lost.
This module mirrors all of it into MongoDB (GridFS for files) and restores it at start-up.

What is stored in MongoDB (database MONGO_DB_NAME, default "hostbot"):
  * the host bot's own SQLite database (users, subscriptions, file records ...) as a snapshot
  * every customer file under upload_bots/<user_id>/ (scripts, zips, data files the bot writes)
  * a small "running" collection so bots that were running are started again after a restart

Customers' own bots can additionally get private databases (see customer_dbs.py), injected as
environment variables when their bot starts.

Every public method is safe to call: errors are logged and never raise, so a MongoDB outage can
never take the bot down. If MONGO_URI is not set the module is simply disabled.
"""
import io
import json
import logging
import os
import sqlite3
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from pathlib import Path

log = logging.getLogger("persistence")

EXCLUDED_DIRS = {'__pycache__', 'node_modules', '.git', 'venv', '.venv', '.cache', '.npm', 'site-packages', '.packages'}
EXCLUDED_SUFFIXES = {'.log', '.pyc'}
MAX_FILE_BYTES = int(os.getenv('PERSIST_MAX_FILE_MB', '25')) * 1024 * 1024
TELEGRAM_SEND_LIMIT = 49 * 1024 * 1024          # Telegram bots can send files up to 50 MB
DB_EXPORT_CAP_BYTES = 30 * 1024 * 1024


def _is_excluded(rel_parts):
    if any(p in EXCLUDED_DIRS for p in rel_parts[:-1]):
        return True
    return Path(rel_parts[-1]).suffix.lower() in EXCLUDED_SUFFIXES


def _safe_rel(rel):
    """Return a clean relative posix path, or None if it could escape the user folder."""
    p = Path(rel.replace('\\', '/'))
    if p.is_absolute() or '..' in p.parts or not p.parts:
        return None
    return p.as_posix()


class Persistence:
    def __init__(self, uri, db_name, upload_dir, sqlite_path, customer=None):
        self.uri = (uri or '').strip()
        self.db_name = (db_name or 'hostbot').strip() or 'hostbot'
        self.customer = customer     # customer_dbs.CustomerDBs or None
        self.upload_dir = Path(upload_dir)
        self.sqlite_path = Path(sqlite_path)
        self._client = None
        self._db = None
        self._fs = None
        self._lock = threading.RLock()
        self._synced = {}            # (uid, rel) -> (size, mtime_ns, gridfs_id)
        self._db_mtime = None        # mtime of sqlite file at last snapshot
        self._restore_ok = False     # snapshots/deletes are only allowed after a successful restore
        self.stats = {'users': 0, 'files': 0, 'db_restored': False, 'running': 0}

    # ------------------------------------------------------------------ connection
    @property
    def enabled(self):
        return bool(self.uri)

    @property
    def customer_db_enabled(self):
        return bool(self.customer and self.customer.enabled)

    def connect(self):
        if not self.enabled:
            return False
        if self._db is not None:
            return True
        try:
            import gridfs
            from pymongo import MongoClient
            client = MongoClient(self.uri, serverSelectionTimeoutMS=8000, connectTimeoutMS=8000)
            client.admin.command('ping')
            self._client = client
            self._db = client[self.db_name]
            self._fs = gridfs.GridFS(self._db)
            try:
                self._db['fs.files'].create_index([('kind', 1), ('uid', 1), ('rel', 1)])
            except Exception:
                pass
            return True
        except Exception as e:
            log.error(f"MongoDB connection failed: {e}")
            self._client = self._db = self._fs = None
            return False

    # ------------------------------------------------------------------ restore at start-up
    def restore_all(self):
        """Call once at start-up BEFORE the SQLite database is opened."""
        if not self.enabled:
            log.warning("MONGO_URI is not set - data is only kept on local disk and WILL be lost on redeploy!")
            return self.stats
        with self._lock:
            if not self.connect():
                log.error("Could not reach MongoDB at start-up - running without cloud storage this time. "
                          "Cloud snapshots are paused so existing cloud data is not overwritten.")
                return self.stats
            try:
                self._restore_sqlite()
                self._restore_files()
                self._restore_ok = True
            except Exception as e:
                log.error(f"Restore failed: {e}", exc_info=True)
        return self.stats

    def _restore_sqlite(self):
        if self.sqlite_path.exists():
            return
        newest = None
        for f in self._fs.find({'kind': 'host', 'name': 'bot_data.db'}):
            if newest is None or f.upload_date > newest.upload_date:
                newest = f
        if newest is None:
            return
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.sqlite_path.write_bytes(newest.read())
        self._db_mtime = self.sqlite_path.stat().st_mtime_ns
        self.stats['db_restored'] = True
        log.info("Restored bot database from MongoDB")

    def _restore_files(self):
        users = set()
        count = 0
        for f in self._fs.find({'kind': 'user'}):
            rel = _safe_rel(getattr(f, 'rel', '') or '')
            uid = getattr(f, 'uid', None)
            if rel is None or uid is None:
                continue
            target = self.upload_dir / str(uid) / rel
            users.add(uid)
            if target.exists():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(f.read())
            st = target.stat()
            self._synced[(uid, rel)] = (st.st_size, st.st_mtime_ns, f._id)
            count += 1
        self.stats['users'] = len(users)
        self.stats['files'] = count
        if count:
            log.info(f"Restored {count} customer file(s) for {len(users)} user(s) from MongoDB")

    # ------------------------------------------------------------------ continuous sync
    def _scan_user(self, uid):
        """Return {rel: (size, mtime_ns, Path)} for the files of one user that should be stored."""
        base = self.upload_dir / str(uid)
        found = {}
        if not base.is_dir():
            return found
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
            for name in files:
                p = Path(root) / name
                try:
                    rel_parts = p.relative_to(base).parts
                    if _is_excluded(rel_parts):
                        continue
                    st = p.stat()
                    if st.st_size > MAX_FILE_BYTES:
                        continue
                    found[Path(*rel_parts).as_posix()] = (st.st_size, st.st_mtime_ns, p)
                except OSError:
                    continue
        return found

    def sync_user(self, uid):
        """Upload new/changed files of one user, delete removed ones. Returns number of uploads."""
        if not (self.enabled and self._restore_ok):
            return 0
        uploaded = 0
        with self._lock:
            try:
                if not self.connect():
                    return 0
                uid = int(uid)
                local = self._scan_user(uid)
                for rel, (size, mtime, path) in local.items():
                    prev = self._synced.get((uid, rel))
                    if prev and prev[0] == size and prev[1] == mtime:
                        continue
                    with open(path, 'rb') as fh:
                        new_id = self._fs.put(fh, filename=f"u/{uid}/{rel}", kind='user', uid=uid, rel=rel,
                                              size=size, mtime_ns=mtime)
                    # remove every older version of this file
                    for old in self._fs.find({'kind': 'user', 'uid': uid, 'rel': rel}):
                        if old._id != new_id:
                            self._fs.delete(old._id)
                    self._synced[(uid, rel)] = (size, mtime, new_id)
                    uploaded += 1
                # files that were synced before but no longer exist locally -> delete in the cloud
                for (u, rel), (_s, _m, _id) in list(self._synced.items()):
                    if u == uid and rel not in local:
                        for old in self._fs.find({'kind': 'user', 'uid': uid, 'rel': rel}):
                            self._fs.delete(old._id)
                        self._synced.pop((u, rel), None)
            except Exception as e:
                log.error(f"sync_user({uid}) failed: {e}", exc_info=True)
        return uploaded

    def sync_all(self):
        if not (self.enabled and self._restore_ok):
            return 0
        total = 0
        try:
            uids = set()
            if self.upload_dir.is_dir():
                for d in self.upload_dir.iterdir():
                    if d.is_dir() and d.name.isdigit():
                        uids.add(int(d.name))
            uids.update(u for (u, _r) in list(self._synced.keys()))
            for uid in uids:
                total += self.sync_user(uid)
        except Exception as e:
            log.error(f"sync_all failed: {e}", exc_info=True)
        return total

    def snapshot_db(self, force=False):
        """Upload the SQLite database if it changed since the last snapshot."""
        if not (self.enabled and self._restore_ok):
            return False
        with self._lock:
            try:
                if not self.sqlite_path.exists() or not self.connect():
                    return False
                mtime = self.sqlite_path.stat().st_mtime_ns
                if not force and mtime == self._db_mtime:
                    return False
                fd, tmp = tempfile.mkstemp(suffix='.db')
                os.close(fd)
                try:
                    src = sqlite3.connect(self.sqlite_path)
                    dst = sqlite3.connect(tmp)
                    src.backup(dst)      # consistent copy even while the bot is writing
                    dst.close()
                    src.close()
                    with open(tmp, 'rb') as fh:
                        new_id = self._fs.put(fh, filename='host/bot_data.db', kind='host', name='bot_data.db')
                finally:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
                for old in self._fs.find({'kind': 'host', 'name': 'bot_data.db'}):
                    if old._id != new_id:
                        self._fs.delete(old._id)
                self._db_mtime = mtime
                return True
            except Exception as e:
                log.error(f"snapshot_db failed: {e}", exc_info=True)
                return False

    def flush(self):
        """Push everything now (used after uploads and on shutdown)."""
        self.snapshot_db()
        return self.sync_all()

    # ------------------------------------------------------------------ "was running" bookkeeping
    def mark_running(self, key, uid, file_name):
        if not (self.enabled and self._restore_ok):
            return
        try:
            if self.connect():
                self._db['running'].replace_one({'_id': key}, {'_id': key, 'uid': int(uid), 'file_name': file_name,
                                                               'since': datetime.utcnow().isoformat()}, upsert=True)
        except Exception as e:
            log.error(f"mark_running failed: {e}")

    def clear_running(self, key):
        if not (self.enabled and self._restore_ok):
            return
        try:
            if self.connect():
                self._db['running'].delete_one({'_id': key})
        except Exception as e:
            log.error(f"clear_running failed: {e}")

    def list_running(self):
        if not (self.enabled and self._restore_ok):
            return []
        try:
            if self.connect():
                return [(d['uid'], d['file_name']) for d in self._db['running'].find({})]
        except Exception as e:
            log.error(f"list_running failed: {e}")
        return []

    # ------------------------------------------------------------------ backup for the customer
    def build_backup(self, uid):
        """
        Build the files to send to a customer: their bot files + a JSON export of their database.
        Returns a list of (path, filename). The caller must delete the paths afterwards.
        Normally one ZIP; if it would be too large for Telegram, files and database are sent separately.
        """
        stamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        readme = (
            "BACKUP OF YOUR BOT\n"
            "==================\n"
            f"Created: {datetime.utcnow().isoformat()}Z\n\n"
            "bot_files/            your uploaded bot files and any data files your bot wrote\n"
            "database_export.json  your MongoDB data (if you used MongoDB)\n"
            "postgres_export.json  your PostgreSQL tables as JSON (columns + rows) (if you used PostgreSQL)\n"
            "mysql_backup.sql      your MySQL data (if you used MySQL) - import with: mysql -u USER -p DB < mysql_backup.sql\n\n"
            "To run it somewhere else:\n"
            "1. Upload bot_files/ to any host (Render, Railway, VPS, ...).\n"
            "2. MongoDB: create a free MongoDB (e.g. Atlas) and import database_export.json\n"
            "   (each key under \"collections\" is a collection; each item is a document in Extended JSON).\n"
            "   MySQL: create a database and import mysql_backup.sql.\n"
            "3. Set MONGO_URI / MONGO_DB_NAME or MYSQL_* for your bot, install your requirements and start it.\n"
        )
        try:
            db_files = self.customer.export_for(uid) if self.customer else []
        except Exception as e:
            log.error(f"customer export failed: {e}")
            db_files = []
        local = self._scan_user(int(uid))
        tmp_paths = []

        def make_zip(include_files, include_db):
            fd, tmp = tempfile.mkstemp(suffix='.zip')
            os.close(fd)
            with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as z:
                z.writestr('README.txt', readme)
                if include_files:
                    for rel, (_s, _m, path) in local.items():
                        try:
                            z.write(path, f"bot_files/{rel}")
                        except OSError:
                            continue
                if include_db:
                    for name, data in db_files:
                        z.writestr(name, data)
            return tmp

        full = make_zip(True, True)
        if os.path.getsize(full) <= TELEGRAM_SEND_LIMIT:
            return [(full, f"bot-backup-{stamp}.zip")]
        os.unlink(full)
        result = []
        files_zip = make_zip(True, False)
        result.append((files_zip, f"bot-files-{stamp}.zip"))
        if db_files:
            fd, tmp = tempfile.mkstemp(suffix='.zip')
            os.close(fd)
            with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as z:
                for name, data in db_files:
                    z.writestr(name, data)
            result.append((tmp, f"database-{stamp}.zip"))
        return result
