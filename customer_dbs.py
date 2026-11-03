"""
customer_dbs.py - one pool of free databases for the customers' bots (MongoDB, MySQL and PostgreSQL mixed).

Add servers with environment variables (the scheme of the URL tells the engine):

    CUSTOMER_DB_1 ... CUSTOMER_DB_20      mongodb+srv://...   |  mysql://...   |  postgresql://...
or the older, engine-specific names (also accepted):
    CUSTOMER_MONGO_URI, CUSTOMER_MONGO_URI_2 ...   CUSTOMER_MYSQL_URL_1 ...   CUSTOMER_POSTGRES_URL_1 ...

ROUND ROBIN: server order = CUSTOMER_DB_1, _2, ... (older names are added after them, interleaved
Mongo -> MySQL -> Postgres). The 1st customer gets server 1, the 2nd customer server 2, ... and after the last
server it starts again at server 1. The choice is remembered forever (table `user_pool` in the host SQLite
database, which is saved to the cloud). A server that is unreachable or over its limit is skipped.

* MongoDB   : every customer gets its own database name (cust_<id>) on the server.
* MySQL     : own database + own MySQL user restricted to it (admin login needs CREATE USER / GRANT).
* PostgreSQL: own database + own role restricted to it (admin login needs CREATEDB / CREATEROLE; Neon, Supabase,
              CockroachDB, Aiven, ... all work this way). Passwords are derived (HMAC), nothing secret is stored.

Nothing here raises into the bot: failures are logged and turned into a short message for the customer.
"""
import hmac
import hashlib
import json
import logging
import sqlite3
import threading
from datetime import datetime
from urllib.parse import urlsplit, unquote, parse_qs

log = logging.getLogger("customer_dbs")

EXPORT_CAP_BYTES = 30 * 1024 * 1024
LABELS = {'mongo': 'MongoDB', 'mysql': 'MySQL', 'postgres': 'PostgreSQL'}
EMOJI = {'mongo': '🍃', 'mysql': '🐬', 'postgres': '🐘'}


def detect_engine(url):
    scheme = url.split('://', 1)[0].lower()
    if scheme.startswith('mongodb'):
        return 'mongo'
    if scheme.startswith('mysql'):
        return 'mysql'
    if scheme.startswith(('postgres', 'cockroach')):
        return 'postgres'
    return None


def load_servers_from_env(environ):
    """All configured server URLs, in round-robin order (duplicates removed)."""
    urls = []

    def add(v):
        v = (v or '').strip()
        if v and v not in urls:
            urls.append(v)

    for i in range(1, 21):
        add(environ.get(f'CUSTOMER_DB_{i}'))
    for i in range(1, 10):
        add(environ.get('CUSTOMER_MONGO_URI') or environ.get('CUSTOMER_MONGO_URI_1') if i == 1
            else environ.get(f'CUSTOMER_MONGO_URI_{i}'))
        add(environ.get(f'CUSTOMER_MYSQL_URL_{i}'))
        add(environ.get(f'CUSTOMER_POSTGRES_URL_{i}'))
    return urls


def parse_mysql_url(url):
    u = urlsplit(url)
    q = parse_qs(u.query)
    return {
        'host': u.hostname, 'port': u.port or 3306,
        'user': unquote(u.username or ''), 'password': unquote(u.password or ''),
        'ssl': (q.get('ssl', ['false'])[0].lower() in ('1', 'true', 'yes', 'required')),
    }


def parse_pg_url(url):
    u = urlsplit(url)
    q = {k: v[0] for k, v in parse_qs(u.query).items()}
    return {
        'host': u.hostname, 'port': u.port or 5432,
        'user': unquote(u.username or ''), 'password': unquote(u.password or ''),
        'database': (u.path or '').lstrip('/') or 'postgres',
        'query': u.query, 'params': q,
    }


class PoolFull(Exception):
    """No database server can take this customer (all full / unreachable)."""
    def __init__(self, engine=None):
        super().__init__(engine or '')
        self.engine = engine


class CustomerDBs:
    def __init__(self, sqlite_path, urls, secret='', max_per_server=None):
        self.sqlite_path = str(sqlite_path)
        self.secret = secret or ''
        self.max_per_server = {'mongo': 0, 'mysql': 0, 'postgres': 0}     # 0 = unlimited
        self.max_per_server.update(max_per_server or {})
        self.slots = []
        for u in urls:
            eng = detect_engine(u)
            if not eng:
                log.warning("Ignoring a customer database URL with an unknown scheme (use mongodb://, mysql:// or postgresql://)")
                continue
            slot = {'engine': eng, 'url': u, 'key': hashlib.sha1(u.encode()).hexdigest()[:12]}
            if eng == 'mysql':
                slot.update(parse_mysql_url(u))
            elif eng == 'postgres':
                slot.update(parse_pg_url(u))
            else:
                slot['host'] = urlsplit(u).hostname or 'mongodb'
            self.slots.append(slot)
        self._lock = threading.RLock()
        self._mongo_clients = {}
        self._provisioned = set()
        self._errors = {}

    @property
    def enabled(self):
        return bool(self.slots)

    @staticmethod
    def db_name(uid):
        return f"cust_{int(uid)}"

    def counts(self):
        return {e: sum(1 for s in self.slots if s['engine'] == e) for e in LABELS}

    # ------------------------------------------------------------------ assignment (round robin, SQLite)
    def _conn(self):
        c = sqlite3.connect(self.sqlite_path)
        c.execute('CREATE TABLE IF NOT EXISTS user_pool (user_id INTEGER PRIMARY KEY, slot_key TEXT)')
        c.execute('CREATE TABLE IF NOT EXISTS pool_meta (k TEXT PRIMARY KEY, v INTEGER)')
        return c

    def _index_of(self, key):
        for i, s in enumerate(self.slots):
            if s['key'] == key:
                return i
        return None

    def _load(self, c):
        load = [0] * len(self.slots)
        for (key,) in c.execute('SELECT slot_key FROM user_pool'):
            i = self._index_of(key)
            if i is not None:
                load[i] += 1
        return load

    def assigned_index(self, uid):
        """Index of the customer's server or None (never assigns)."""
        if not self.slots:
            return None
        with self._lock:
            c = self._conn()
            try:
                row = c.execute('SELECT slot_key FROM user_pool WHERE user_id=?', (int(uid),)).fetchone()
            finally:
                c.close()
        return self._index_of(row[0]) if row else None

    def _assign(self, uid):
        """Server index for this customer. Assigns the next server in the circle the first time. Blocking."""
        n = len(self.slots)
        if not n:
            raise PoolFull()
        with self._lock:
            c = self._conn()
            try:
                row = c.execute('SELECT slot_key FROM user_pool WHERE user_id=?', (int(uid),)).fetchone()
                if row and self._index_of(row[0]) is not None:
                    return self._index_of(row[0])
                load = self._load(c)
                meta = c.execute("SELECT v FROM pool_meta WHERE k='next_slot'").fetchone()
                start = (meta[0] if meta else 0) % n
                chosen = None
                for step in range(n):
                    i = (start + step) % n
                    limit = self.max_per_server[self.slots[i]['engine']]
                    if limit and load[i] >= limit:
                        continue
                    if not self._ping(i):
                        log.warning(f"Database server #{i + 1} ({self.slots[i]['engine']}) is unreachable - skipped")
                        continue
                    chosen = i
                    break
                if chosen is None:
                    raise PoolFull()
                c.execute('INSERT OR REPLACE INTO user_pool (user_id, slot_key) VALUES (?, ?)',
                          (int(uid), self.slots[chosen]['key']))
                c.execute("INSERT OR REPLACE INTO pool_meta (k, v) VALUES ('next_slot', ?)", ((chosen + 1) % n,))
                c.commit()
                return chosen
            finally:
                c.close()

    def assign(self, uid):
        """Give the customer a server now (called when they register). Returns the index or None. Never raises."""
        if not self.slots:
            return None
        try:
            return self._assign(uid)
        except PoolFull:
            self._errors[int(uid)] = "⚠️ All database servers are full or offline right now, so your bot started without a database."
        except Exception as e:
            log.error(f"assign({uid}) failed: {e}", exc_info=True)
        return None

    def usage(self):
        """[(label, customers_on_server, max_or_0), ...] for every server, in round-robin order."""
        with self._lock:
            c = self._conn()
            try:
                load = self._load(c)
            finally:
                c.close()
        out = []
        for i, s in enumerate(self.slots):
            out.append((f"{EMOJI[s['engine']]} #{i + 1} {LABELS[s['engine']]} ({s['host']})",
                        load[i], self.max_per_server[s['engine']]))
        return out

    def info(self, uid):
        """{'engine','server_no'} of the customer's database or None."""
        i = self.assigned_index(uid)
        if i is None:
            return None
        return {'engine': self.slots[i]['engine'], 'server_no': i + 1}

    # ------------------------------------------------------------------ connectivity
    def _mongo_client(self, idx):
        if idx not in self._mongo_clients:
            from pymongo import MongoClient
            self._mongo_clients[idx] = MongoClient(self.slots[idx]['url'], serverSelectionTimeoutMS=8000,
                                                   connectTimeoutMS=8000)
        return self._mongo_clients[idx]

    def _mysql_connect(self, idx, database=None):
        import pymysql
        s = self.slots[idx]
        kw = dict(host=s['host'], port=s['port'], user=s['user'], password=s['password'],
                  connect_timeout=8, read_timeout=30, write_timeout=30, autocommit=True, charset='utf8mb4')
        if s['ssl']:
            kw['ssl'] = {'fake_flag_to_enable_tls': True}
        if database:
            kw['database'] = database
        return pymysql.connect(**kw)

    def _pg_connect(self, idx, database=None, user=None, password=None):
        import psycopg2
        s = self.slots[idx]
        kw = dict(s['params'])
        kw.update(host=s['host'], port=s['port'], user=user or s['user'], password=password or s['password'],
                  dbname=database or s['database'], connect_timeout=8)
        conn = psycopg2.connect(**kw)
        conn.autocommit = True
        return conn

    def _ping(self, idx):
        try:
            eng = self.slots[idx]['engine']
            if eng == 'mongo':
                self._mongo_client(idx).admin.command('ping')
            elif eng == 'mysql':
                self._mysql_connect(idx).close()
            else:
                self._pg_connect(idx).close()
            return True
        except Exception as e:
            log.warning(f"ping server #{idx + 1} failed: {e}")
            return False

    def _password(self, idx, uid):
        s = self.slots[idx]
        key = f"{self.secret}|{s['host']}|{s.get('user', '')}|{s.get('password', '')}".encode()
        return hmac.new(key, f"cust:{int(uid)}".encode(), hashlib.sha256).hexdigest()[:32]

    # ------------------------------------------------------------------ MongoDB
    def _mongo_env(self, uid, idx):
        uri = self.slots[idx]['url']
        db = self.db_name(uid)
        return {'MONGO_URI': uri, 'MONGODB_URI': uri, 'MONGO_URL': uri, 'MONGO_DB_NAME': db}, uri

    def _dump_mongo(self, uid, idx):
        from bson import json_util
        db = self._mongo_client(idx)[self.db_name(uid)]
        names = [n for n in db.list_collection_names() if not n.startswith('system.')]
        if not names:
            return None
        size, truncated, cols = 0, False, {}
        for name in names:
            docs = []
            for doc in db[name].find({}):
                s = json_util.dumps(doc)
                size += len(s)
                if size > EXPORT_CAP_BYTES:
                    truncated = True
                    break
                docs.append(json.loads(s))
            cols[name] = docs
            if truncated:
                break
        return json.dumps({'database': self.db_name(uid), 'exported_at': datetime.utcnow().isoformat() + 'Z',
                           'truncated': truncated, 'collections': cols}, indent=2).encode()

    # ------------------------------------------------------------------ MySQL
    def _provision_mysql(self, uid, idx):
        key = (idx, int(uid))
        if key in self._provisioned:
            return
        db, user, pw = self.db_name(uid), self.db_name(uid), self._password(idx, uid)
        conn = self._mysql_connect(idx)
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4")
                cur.execute("CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s", (user, pw))
                cur.execute("ALTER USER %s@'%%' IDENTIFIED BY %s", (user, pw))
                cur.execute(f"GRANT ALL PRIVILEGES ON `{db}`.* TO %s@'%%'", (user,))
        finally:
            conn.close()
        self._provisioned.add(key)

    def _mysql_env(self, uid, idx):
        self._provision_mysql(uid, idx)
        s = self.slots[idx]
        db, pw = self.db_name(uid), self._password(idx, uid)
        url = f"mysql://{db}:{pw}@{s['host']}:{s['port']}/{db}" + ('?ssl=true' if s['ssl'] else '')
        env = {'MYSQL_HOST': s['host'], 'MYSQL_PORT': str(s['port']), 'MYSQL_USER': db, 'MYSQL_PASSWORD': pw,
               'MYSQL_DATABASE': db, 'MYSQL_URL': url}
        if s['ssl']:
            env['MYSQL_SSL'] = 'true'
        return env, url

    def _dump_mysql(self, uid, idx):
        db = self.db_name(uid)
        conn = self._mysql_connect(idx)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s", (db,))
                if cur.fetchone() is None:
                    return None
                cur.execute(f"USE `{db}`")
                cur.execute("SHOW FULL TABLES WHERE Table_type='BASE TABLE'")
                tables = [r[0] for r in cur.fetchall()]
                if not tables:
                    return None
                out = [f"-- MySQL backup of {db}\n-- {datetime.utcnow().isoformat()}Z\nSET FOREIGN_KEY_CHECKS=0;\n"]
                size = 0
                for t in tables:
                    cur.execute(f"SHOW CREATE TABLE `{t}`")
                    out.append(f"DROP TABLE IF EXISTS `{t}`;\n{cur.fetchone()[1]};\n")
                    cur.execute(f"SELECT * FROM `{t}`")
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            break
                        for row in rows:
                            vals = ", ".join(self._sql_value(conn, v) for v in row)
                            line = f"INSERT INTO `{t}` VALUES ({vals});\n"
                            size += len(line)
                            if size > EXPORT_CAP_BYTES:
                                out.append("-- TRUNCATED: backup size limit reached\n")
                                return "".join(out).encode()
                            out.append(line)
                out.append("SET FOREIGN_KEY_CHECKS=1;\n")
                return "".join(out).encode()
        finally:
            conn.close()

    @staticmethod
    def _sql_value(conn, v):
        if v is None:
            return "NULL"
        if isinstance(v, (bytes, bytearray)):
            return "X'" + bytes(v).hex() + "'"
        if isinstance(v, (int, float)):
            return str(v)
        return conn.escape(v if isinstance(v, str) else str(v))

    # ------------------------------------------------------------------ PostgreSQL
    def _provision_pg(self, uid, idx):
        key = (idx, int(uid))
        if key in self._provisioned:
            return
        from psycopg2 import sql
        name, pw = self.db_name(uid), self._password(idx, uid)
        conn = self._pg_connect(idx)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,))
                if cur.fetchone() is None:
                    cur.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(name), sql.Literal(pw)))
                else:
                    cur.execute(sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(sql.Identifier(name), sql.Literal(pw)))
                try:      # newer PostgreSQL needs the admin to be a member of the role to make it the owner
                    cur.execute(sql.SQL("GRANT {} TO CURRENT_USER").format(sql.Identifier(name)))
                except Exception:
                    pass
                cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,))
                if cur.fetchone() is None:
                    cur.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(name), sql.Identifier(name)))
                try:      # other customers must not be able to connect to this database
                    cur.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(name)))
                except Exception:
                    pass
        finally:
            conn.close()
        self._provisioned.add(key)

    def _pg_env(self, uid, idx):
        self._provision_pg(uid, idx)
        s = self.slots[idx]
        name, pw = self.db_name(uid), self._password(idx, uid)
        url = f"postgresql://{name}:{pw}@{s['host']}:{s['port']}/{name}" + (f"?{s['query']}" if s['query'] else '')
        env = {'POSTGRES_URL': url, 'POSTGRESQL_URL': url, 'PGHOST': s['host'], 'PGPORT': str(s['port']),
               'PGUSER': name, 'PGPASSWORD': pw, 'PGDATABASE': name}
        if s['params'].get('sslmode'):
            env['PGSSLMODE'] = s['params']['sslmode']
        return env, url

    def _dump_pg(self, uid, idx):
        name, pw = self.db_name(uid), self._password(idx, uid)
        conn = self._pg_connect(idx, database=name, user=name, password=pw)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT table_schema, table_name FROM information_schema.tables "
                            "WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema')")
                tables = cur.fetchall()
                if not tables:
                    return None
                size, truncated, out = 0, False, {}
                for schema, t in tables:
                    cur.execute(f'SELECT * FROM "{schema}"."{t}"')
                    cols = [d[0] for d in cur.description]
                    rows = []
                    for row in cur:
                        size += sum(len(str(v)) for v in row) + 16
                        if size > EXPORT_CAP_BYTES:
                            truncated = True
                            break
                        rows.append(list(row))
                    out[f"{schema}.{t}"] = {'columns': cols, 'rows': rows}
                    if truncated:
                        break
                return json.dumps({'database': name, 'exported_at': datetime.utcnow().isoformat() + 'Z',
                                   'truncated': truncated, 'tables': out}, indent=2, default=str).encode()
        finally:
            conn.close()

    # ------------------------------------------------------------------ public API
    def env_for(self, uid):
        """Environment variables for a customer's bot (blocking: may create the database / user)."""
        if not self.slots:
            return {}
        try:
            idx = self._assign(uid)
        except PoolFull:
            self._errors[int(uid)] = "⚠️ All database servers are full or offline right now, so your bot started without a database."
            return {}
        except Exception as e:
            log.error(f"assign({uid}) failed: {e}", exc_info=True)
            return {}
        eng = self.slots[idx]['engine']
        try:
            env, url = {'mongo': self._mongo_env, 'mysql': self._mysql_env, 'postgres': self._pg_env}[eng](uid, idx)
        except Exception as e:
            log.error(f"{eng} database for {uid} failed: {e}", exc_info=True)
            self._errors[int(uid)] = f"⚠️ Your {LABELS[eng]} database could not be prepared. Your bot started without it."
            return {}
        env['DB_ENGINE'] = eng
        env['DATABASE_URL'] = url
        return env

    def pop_error(self, uid):
        return self._errors.pop(int(uid), None)

    def export_for(self, uid):
        """Backup files for a customer: list of (name_inside_zip, bytes). Never raises."""
        idx = self.assigned_index(uid)
        if idx is None:
            return []
        eng = self.slots[idx]['engine']
        try:
            if eng == 'mongo':
                data, name = self._dump_mongo(uid, idx), 'database_export.json'
            elif eng == 'mysql':
                data, name = self._dump_mysql(uid, idx), 'mysql_backup.sql'
            else:
                data, name = self._dump_pg(uid, idx), 'postgres_export.json'
            return [(name, data)] if data else []
        except Exception as e:
            log.error(f"{eng} export for {uid} failed: {e}")
            return []
