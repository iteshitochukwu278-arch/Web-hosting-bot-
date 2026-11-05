"""Independent Telegram bot fleet manager for the hosting bot.

This module provides persistent per-bot metadata, Telegram token verification,
per-bot file storage, isolated process handles, logs, and owner-scoped controls.
It intentionally does not execute uploaded code in a security sandbox; deploy behind
container/OS isolation before accepting untrusted public workloads.
"""
from __future__ import annotations
import asyncio, base64, hashlib, json, os, re, sqlite3, subprocess, sys, time, threading, signal, shutil, zipfile, tempfile
from pathlib import Path
from typing import Optional
import aiohttp
from cryptography.fernet import Fernet

BOT_ID_RE = re.compile(r"^[a-f0-9]{12}$")
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_. -]{1,100}$")

class FleetError(Exception): pass

class BotFleet:
    def __init__(self, root: Path, db_path: Path, secret: str, default_limit: int = 5):
        if not secret or len(secret) < 16:
            raise ValueError("FLEET_SECRET must be at least 16 characters")
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(db_path)
        self.limit = max(1, int(default_limit))
        key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
        self.fernet = Fernet(key)
        self.processes: dict[str, subprocess.Popen] = {}
        self.logs: dict[str, object] = {}
        self._lock = threading.RLock()
        self.max_memory_mb = max(64, int(os.getenv("BOT_MEMORY_MB", "512")))
        self.max_cpu_seconds = max(10, int(os.getenv("BOT_CPU_SECONDS", "3600")))
        self.max_restarts = max(0, int(os.getenv("BOT_MAX_RESTARTS", "5")))
        self.restart_window = max(60, int(os.getenv("BOT_RESTART_WINDOW_SECONDS", "300")))
        self._restart_history: dict[str, list[float]] = {}
        self._restart_counts: dict[str, int] = {}
        self._stop_event = threading.Event()
        self.backup_dir = self.root / ".backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.backup_interval = max(3600, int(os.getenv("BOT_BACKUP_INTERVAL_SECONDS", "86400")))
        self.backup_retention = max(1, int(os.getenv("BOT_BACKUP_RETENTION", "7")))
        self._last_backup = {}
        self._init_db()
        self._monitor_thread = threading.Thread(target=self._monitor, name="fleet-supervisor", daemon=True)
        self._monitor_thread.start()

    def _conn(self):
        c = sqlite3.connect(self.db_path, timeout=20)
        c.row_factory = sqlite3.Row
        return c

    def _init_db(self):
        with self._conn() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS hosted_bots (
                id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL, name TEXT NOT NULL,
                username TEXT NOT NULL, token_enc TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'stopped',
                disabled INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL,
                UNIQUE(owner_id, username))''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_hosted_bots_owner ON hosted_bots(owner_id)')
            c.execute("CREATE TABLE IF NOT EXISTS fleet_accounts (owner_id INTEGER PRIMARY KEY, suspended INTEGER NOT NULL DEFAULT 0, bot_limit INTEGER, updated_at REAL NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS bot_file_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, bot_id TEXT NOT NULL, filename TEXT NOT NULL, version INTEGER NOT NULL, backup_path TEXT NOT NULL, sha256 TEXT NOT NULL, created_at REAL NOT NULL, UNIQUE(bot_id,filename,version))")
            c.execute("CREATE INDEX IF NOT EXISTS idx_bot_file_versions ON bot_file_versions(bot_id,filename,version DESC)")
            c.execute("CREATE TABLE IF NOT EXISTS bot_environment (bot_id TEXT NOT NULL, name TEXT NOT NULL, value_enc TEXT NOT NULL, updated_at REAL NOT NULL, PRIMARY KEY(bot_id,name))")
            c.execute("CREATE TABLE IF NOT EXISTS bot_backups (id INTEGER PRIMARY KEY AUTOINCREMENT, bot_id TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, created_at REAL NOT NULL)")
            cols={r[1] for r in c.execute('PRAGMA table_info(fleet_accounts)').fetchall()}
            if 'plan_name' not in cols: c.execute("ALTER TABLE fleet_accounts ADD COLUMN plan_name TEXT NOT NULL DEFAULT 'free'")
            if 'expires_at' not in cols: c.execute('ALTER TABLE fleet_accounts ADD COLUMN expires_at REAL')
            c.execute("CREATE TABLE IF NOT EXISTS fleet_alerts (bot_id TEXT PRIMARY KEY, status TEXT NOT NULL, notified_at REAL NOT NULL)")

    def _bot_dir(self, bot_id: str) -> Path:
        if not BOT_ID_RE.fullmatch(bot_id): raise FleetError('Invalid bot ID')
        path = (self.root / bot_id).resolve()
        if self.root not in path.parents: raise FleetError('Invalid bot path')
        path.mkdir(parents=True, exist_ok=True)
        return path

    def account(self, owner_id: int):
        with self._conn() as c:
            row = c.execute('SELECT * FROM fleet_accounts WHERE owner_id=?', (int(owner_id),)).fetchone()
        return dict(row) if row else {'owner_id': int(owner_id), 'suspended': 0, 'bot_limit': None, 'plan_name':'free', 'expires_at':None}

    def set_account(self, owner_id: int, suspended=None, bot_limit=None):
        current = self.account(owner_id)
        suspended = current['suspended'] if suspended is None else int(bool(suspended))
        bot_limit = current['bot_limit'] if bot_limit is None else int(bot_limit)
        if bot_limit is not None and bot_limit < 1:
            raise FleetError('Bot limit must be at least 1.')
        with self._conn() as c:
            c.execute("INSERT INTO fleet_accounts(owner_id,suspended,bot_limit,updated_at) VALUES(?,?,?,?) "
                      "ON CONFLICT(owner_id) DO UPDATE SET suspended=excluded.suspended,bot_limit=excluded.bot_limit,updated_at=excluded.updated_at",
                      (int(owner_id), suspended, bot_limit, time.time()))
        if suspended:
            for b in self.list_bots(int(owner_id)):
                self.stop(int(owner_id), b['id'])

    def set_plan(self, owner_id: int, plan_name: str, bot_limit: int, days: int):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,32}',plan_name) or bot_limit < 1 or days < 1:
            raise FleetError('Plan name, bot limit, or duration is invalid.')
        expires=time.time()+int(days)*86400
        with self._conn() as c:
            c.execute("INSERT INTO fleet_accounts(owner_id,suspended,bot_limit,updated_at,plan_name,expires_at) VALUES(?,0,?,?,?,?) ON CONFLICT(owner_id) DO UPDATE SET suspended=0,bot_limit=excluded.bot_limit,updated_at=excluded.updated_at,plan_name=excluded.plan_name,expires_at=excluded.expires_at",(int(owner_id),int(bot_limit),time.time(),plan_name,expires))
        return expires

    def pending_crashloop_alerts(self):
        with self._conn() as c:
            rows=c.execute("SELECT b.id,b.owner_id,b.name FROM hosted_bots b LEFT JOIN fleet_alerts a ON a.bot_id=b.id AND a.status=b.status WHERE b.status='crashloop' AND a.bot_id IS NULL").fetchall()
            for r in rows: c.execute('INSERT OR REPLACE INTO fleet_alerts(bot_id,status,notified_at) VALUES(?,?,?)',(r['id'],'crashloop',time.time()))
        return [dict(r) for r in rows]

    def owner_limit(self, owner_id: int):
        account = self.account(owner_id)
        return int(account['bot_limit']) if account.get('bot_limit') else self.limit

    async def verify_token(self, token: str) -> dict:
        token = token.strip()
        if len(token) > 256 or not re.fullmatch(r"[0-9]{5,15}:[A-Za-z0-9_-]{20,}", token):
            raise FleetError('Token format is invalid')
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f'https://api.telegram.org/bot{token}/getMe') as r:
                data = await r.json(content_type=None)
        if not data.get('ok') or not isinstance(data.get('result'), dict):
            raise FleetError('Telegram rejected this token. Check it in @BotFather.')
        me = data['result']
        return {'id': int(me['id']), 'name': str(me.get('first_name') or 'Telegram bot'),
                'username': str(me.get('username') or '')}

    def add_verified(self, owner_id: int, token: str, verified: dict) -> dict:
        account = self.account(owner_id)
        if account['suspended']: raise FleetError('Your hosting account is suspended. Contact support.')
        if account.get('expires_at') and float(account['expires_at']) <= time.time(): raise FleetError('Your hosting plan has expired. Contact support to renew.')
        bots = self.list_bots(owner_id)
        limit = self.owner_limit(owner_id)
        if len(bots) >= limit: raise FleetError(f'Bot limit reached ({limit}).')
        username = verified.get('username') or str(verified['id'])
        with self._conn() as c:
            existing = c.execute('SELECT id FROM hosted_bots WHERE owner_id=? AND username=?',
                                 (owner_id, username)).fetchone()
            if existing: raise FleetError('This Telegram bot is already registered to your account.')
            bot_id = hashlib.sha256(f'{owner_id}:{verified["id"]}:{time.time_ns()}'.encode()).hexdigest()[:12]
            c.execute('INSERT INTO hosted_bots(id,owner_id,name,username,token_enc,created_at) VALUES(?,?,?,?,?,?)',
                      (bot_id, owner_id, verified['name'], username,
                       self.fernet.encrypt(token.encode()).decode(), time.time()))
        self._bot_dir(bot_id)
        return self.get_bot(owner_id, bot_id)

    def list_bots(self, owner_id: int):
        with self._conn() as c:
            rows = c.execute('SELECT id,name,username,status,disabled,created_at FROM hosted_bots WHERE owner_id=? ORDER BY created_at',
                             (owner_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_bot(self, owner_id: int, bot_id: str, admin: bool = False):
        with self._conn() as c:
            row = c.execute('SELECT * FROM hosted_bots WHERE id=?', (bot_id,)).fetchone()
        if not row or (not admin and int(row['owner_id']) != int(owner_id)):
            raise FleetError('Bot not found or access denied.')
        d = dict(row); d.pop('token_enc', None)
        return d

    def _token(self, bot_id: str):
        with self._conn() as c:
            row = c.execute('SELECT token_enc FROM hosted_bots WHERE id=?', (bot_id,)).fetchone()
        if not row: raise FleetError('Bot not found')
        return self.fernet.decrypt(row['token_enc'].encode()).decode()

    def files(self, owner_id: int, bot_id: str):
        self.get_bot(owner_id, bot_id)
        folder = self._bot_dir(bot_id)
        return sorted(p.name for p in folder.iterdir() if p.is_file() and p.name != '.token')

    def save_file(self, owner_id: int, bot_id: str, filename: str, data: bytes, max_bytes=25*1024*1024):
        bot = self.get_bot(owner_id, bot_id)
        if self.account(owner_id)['suspended']: raise FleetError('Your hosting account is suspended.')
        if bot['disabled']: raise FleetError('This bot has been disabled by an administrator.')
        if not SAFE_NAME_RE.fullmatch(filename) or filename in {'.','..'} or '/' in filename or '\\' in filename:
            raise FleetError('Unsafe filename')
        if len(data) > max_bytes: raise FleetError('File exceeds the upload size limit.')
        if not filename.lower().endswith(('.py','.js','.json','.txt','.yaml','.yml','.env','.cfg','.ini','.csv','.db')):
            raise FleetError('Unsupported file type')
        path = self._bot_dir(bot_id) / filename
        if path.exists(): self._snapshot_file(bot_id, filename, path)
        tmp = path.with_suffix(path.suffix + '.uploading')
        tmp.write_bytes(data); os.replace(tmp, path)
        self._restart_if_running(owner_id, bot_id)
        return filename

    def _safe_file(self, filename: str) -> str:
        if not SAFE_NAME_RE.fullmatch(filename) or filename in {'.','..'} or '/' in filename or '\\' in filename:
            raise FleetError('Unsafe filename')
        return filename

    def _snapshot_file(self, bot_id: str, filename: str, path: Path):
        """Keep a bounded, content-addressed version before replacing a file."""
        if not path.is_file():
            return
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        with self._conn() as c:
            row = c.execute('SELECT COALESCE(MAX(version),0) AS v FROM bot_file_versions WHERE bot_id=? AND filename=?', (bot_id, filename)).fetchone()
            version = int(row['v']) + 1
        target_dir = self.backup_dir / bot_id / 'versions'
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f'{hashlib.sha256(filename.encode()).hexdigest()[:12]}-{version}.bak'
        shutil.copy2(path, target)
        with self._conn() as c:
            c.execute('INSERT INTO bot_file_versions(bot_id,filename,version,backup_path,sha256,created_at) VALUES(?,?,?,?,?,?)',
                      (bot_id, filename, version, str(target), digest, time.time()))
            rows = c.execute('SELECT version,backup_path FROM bot_file_versions WHERE bot_id=? AND filename=? ORDER BY version DESC', (bot_id, filename)).fetchall()
            for old in rows[self.backup_retention:]:
                try: Path(old['backup_path']).unlink(missing_ok=True)
                except OSError: pass
                c.execute('DELETE FROM bot_file_versions WHERE bot_id=? AND filename=? AND version=?', (bot_id, filename, old['version']))

    def file_versions(self, owner_id: int, bot_id: str, filename: str):
        self.get_bot(owner_id, bot_id); self._safe_file(filename)
        with self._conn() as c:
            rows=c.execute('SELECT version,sha256,created_at FROM bot_file_versions WHERE bot_id=? AND filename=? ORDER BY version DESC', (bot_id,filename)).fetchall()
        return [dict(r) for r in rows]

    def restore_file_version(self, owner_id: int, bot_id: str, filename: str, version: int):
        self.get_bot(owner_id, bot_id); self._safe_file(filename)
        with self._conn() as c:
            row=c.execute('SELECT backup_path FROM bot_file_versions WHERE bot_id=? AND filename=? AND version=?', (bot_id,filename,int(version))).fetchone()
        if not row or not Path(row['backup_path']).is_file(): raise FleetError('That file version is unavailable.')
        folder=self._bot_dir(bot_id); target=folder/filename
        if target.exists(): self._snapshot_file(bot_id,filename,target)
        tmp=target.with_suffix(target.suffix+'.restoring'); shutil.copy2(row['backup_path'],tmp); os.replace(tmp,target)
        self._restart_if_running(owner_id,bot_id)
        return filename

    def delete_file(self, owner_id: int, bot_id: str, filename: str):
        self.get_bot(owner_id,bot_id); self._safe_file(filename)
        if filename in {'.token','bot.log'}: raise FleetError('This file cannot be deleted from the file manager.')
        path=self._bot_dir(bot_id)/filename
        if not path.is_file(): raise FleetError('File not found.')
        self._snapshot_file(bot_id,filename,path); path.unlink(); self._restart_if_running(owner_id,bot_id)

    def _restart_if_running(self, owner_id: int, bot_id: str):
        proc=self.processes.get(bot_id)
        if proc and proc.poll() is None:
            self.stop(owner_id,bot_id); self.start(owner_id,bot_id)

    def set_environment(self, owner_id: int, bot_id: str, name: str, value: str):
        self.get_bot(owner_id,bot_id)
        if not re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}',name) or name in {'PATH','PYTHONPATH','LD_PRELOAD','LD_LIBRARY_PATH','BOT_TOKEN','TELEGRAM_BOT_TOKEN'}:
            raise FleetError('Invalid or reserved environment variable name.')
        if len(value)>8192: raise FleetError('Environment value is too long (8 KB max).')
        if '\n' in value or '\r' in value: raise FleetError('Environment values cannot contain newline characters.')
        encrypted=self.fernet.encrypt(value.encode()).decode()
        with self._conn() as c:
            c.execute('INSERT INTO bot_environment(bot_id,name,value_enc,updated_at) VALUES(?,?,?,?) ON CONFLICT(bot_id,name) DO UPDATE SET value_enc=excluded.value_enc,updated_at=excluded.updated_at',
                      (bot_id,name,encrypted,time.time()))
        self._restart_if_running(owner_id,bot_id)

    def environment_names(self, owner_id: int, bot_id: str):
        self.get_bot(owner_id,bot_id)
        with self._conn() as c: rows=c.execute('SELECT name FROM bot_environment WHERE bot_id=? ORDER BY name',(bot_id,)).fetchall()
        return [r['name'] for r in rows]

    def delete_environment(self, owner_id: int, bot_id: str, name: str):
        self.get_bot(owner_id,bot_id)
        with self._conn() as c: c.execute('DELETE FROM bot_environment WHERE bot_id=? AND name=?',(bot_id,name))
        self._restart_if_running(owner_id,bot_id)

    def _environment(self, bot_id: str):
        with self._conn() as c: rows=c.execute('SELECT name,value_enc FROM bot_environment WHERE bot_id=?',(bot_id,)).fetchall()
        return {r['name']:self.fernet.decrypt(r['value_enc'].encode()).decode() for r in rows}

    def create_backup(self, owner_id: int, bot_id: str) -> Path:
        self.get_bot(owner_id,bot_id)
        folder=self._bot_dir(bot_id)
        target_dir=self.backup_dir/bot_id; target_dir.mkdir(parents=True,exist_ok=True)
        target=target_dir/f'backup-{int(time.time())}.zip'
        with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as z:
            for p in folder.iterdir():
                if p.is_file() and p.name not in {'bot.log','.token'} and not p.name.endswith('.uploading'):
                    z.write(p,arcname=p.name)
            z.writestr('MANIFEST.json',json.dumps({'bot_id':bot_id,'created_at':time.time(),'files':self.files(owner_id,bot_id)},indent=2))
        with self._conn() as c: c.execute('INSERT INTO bot_backups(bot_id,path,size,created_at) VALUES(?,?,?,?)',(bot_id,str(target),target.stat().st_size,time.time()))
        rows=[]
        with self._conn() as c: rows=c.execute('SELECT id,path FROM bot_backups WHERE bot_id=? ORDER BY created_at DESC',(bot_id,)).fetchall()
        for old in rows[self.backup_retention:]:
            try: Path(old['path']).unlink(missing_ok=True)
            except OSError: pass
            with self._conn() as c: c.execute('DELETE FROM bot_backups WHERE id=?',(old['id'],))
        return target

    def backup_list(self, owner_id: int, bot_id: str):
        self.get_bot(owner_id,bot_id)
        with self._conn() as c: rows=c.execute('SELECT id,size,created_at FROM bot_backups WHERE bot_id=? ORDER BY created_at DESC',(bot_id,)).fetchall()
        return [dict(r) for r in rows]

    def resource_usage(self, owner_id: int, bot_id: str):
        bot=self.get_bot(owner_id,bot_id); proc=self.processes.get(bot_id)
        if not proc or proc.poll() is not None: return {'status':bot['status'],'cpu_percent':0.0,'memory_mb':0.0,'pid':None}
        try:
            import psutil
            p=psutil.Process(proc.pid)
            return {'status':'running','cpu_percent':p.cpu_percent(interval=0.05),'memory_mb':round(p.memory_info().rss/1048576,1),'pid':proc.pid}
        except Exception: return {'status':'running','cpu_percent':0.0,'memory_mb':0.0,'pid':proc.pid}

    def _resource_limits(self, runtime: str):
        """Best-effort limits for trusted-only subprocess mode.

        Do not apply RLIMIT_NPROC (shared by all service processes) or RLIMIT_CPU
        (cumulative CPU seconds kill long-running bots). Docker/cgroups enforce
        production CPU, memory and PID quotas per customer container.
        """
        if os.name == "nt":
            return None
        def apply():
            try:
                import resource
                if runtime == 'python':
                    memory = self.max_memory_mb * 1024 * 1024
                    resource.setrlimit(resource.RLIMIT_AS, (memory, memory))
                resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))
                resource.setrlimit(resource.RLIMIT_FSIZE, (64 * 1024 * 1024, 64 * 1024 * 1024))
            except Exception:
                pass
        return apply

    def _spawn(self, bot_id: str, folder: Path, script: Path):
        mode = os.getenv('BOT_EXECUTION_MODE', 'docker').strip().lower()
        if mode == 'subprocess' and os.getenv('BOT_ALLOW_UNSAFE_SUBPROCESS', '').lower() != 'true':
            raise FleetError('Unsafe subprocess hosting is disabled. Configure a Docker-capable worker and BOT_EXECUTION_MODE=docker.')
        if mode not in ('docker', 'subprocess'):
            raise FleetError('BOT_EXECUTION_MODE must be docker or subprocess.')

        # Never inherit the host service environment. Only basic runtime variables,
        # the selected customer's own token, and their explicitly configured vars pass.
        env = {k: os.environ[k] for k in ('PATH', 'LANG', 'LC_ALL', 'TMPDIR', 'SYSTEMROOT', 'WINDIR') if k in os.environ}
        host_env = dict(env)
        # Docker client connection settings belong to the host process only.
        for key in ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_CERT_PATH', 'DOCKER_TLS_VERIFY', 'HOME'):
            if key in os.environ: host_env[key] = os.environ[key]
        token = self._token(bot_id)
        customer_env = self._environment(bot_id)
        env.update({'TELEGRAM_BOT_TOKEN': token, 'BOT_TOKEN': token, 'PYTHONUNBUFFERED': '1'})
        env.update(customer_env)
        runtime = 'python' if script.suffix.lower() == '.py' else 'node'

        if mode == 'docker':
            image = os.getenv('BOT_DOCKER_IMAGE', 'python:3.12-slim' if runtime == 'python' else 'node:22-slim')
            if runtime == 'python':
                # Install requirements inside the isolated container, not on the host.
                bootstrap = ("import os,runpy,subprocess,sys; "
                    "req='/app/requirements.txt'; target='/tmp/customer-packages'; "
                    "os.makedirs(target,exist_ok=True); "
                    "subprocess.check_call([sys.executable,'-m','pip','install','--disable-pip-version-check','--no-cache-dir','--no-warn-script-location','--target',target,'-r',req]) if os.path.isfile(req) else None; "
                    "sys.path.insert(0,target); sys.argv=['/app/" + script.name + "']; runpy.run_path('/app/" + script.name + "',run_name='__main__')")
                inner = ['python', '-u', '-c', bootstrap]
            else:
                inner = ['node', f'/app/{script.name}']
            env_file = self.backup_dir / bot_id / 'runtime.env'
            env_file.parent.mkdir(parents=True, exist_ok=True)
            env_file.write_text(''.join(f'{k}={v}\n' for k,v in env.items() if k not in ('PATH','LANG','LC_ALL','TMPDIR','SYSTEMROOT','WINDIR')), encoding='utf-8')
            try: os.chmod(env_file, 0o600)
            except OSError: pass
            try:
                subprocess.run(['docker', 'rm', '-f', f'hostbot-{bot_id}'], env=host_env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15, check=False)
            except Exception:
                pass
            cmd = ['docker', 'run', '--rm', '--name', f'hostbot-{bot_id}',
                   '--memory', f'{self.max_memory_mb}m', '--cpus', os.getenv('BOT_CPU_QUOTA', '1.0'),
                   '--pids-limit', os.getenv('BOT_PIDS_LIMIT', '64'), '--read-only',
                   '--tmpfs', '/tmp:rw,nosuid,size=256m',
                   '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                   '--user', os.getenv('BOT_CONTAINER_USER', f'{os.getuid()}:{os.getgid()}' if hasattr(os, 'getuid') else '65534:65534'),
                   '--network', os.getenv('BOT_DOCKER_NETWORK', 'bridge'),
                   '--env-file', str(env_file),
                   '-v', f'{folder}:/app:rw', '-w', '/app', image, *inner]
        else:
            if runtime == 'python':
                # No Docker (e.g. Render): install the customer's requirements into a private
                # per-bot folder (inside the child process, so the host never blocks) and run the script.
                pkg_dir = str(folder / '.packages')
                bootstrap = ("import hashlib,os,runpy,subprocess,sys; "
                    "req=os.path.join(os.getcwd(),'requirements.txt'); target=" + repr(pkg_dir) + "; "
                    "os.makedirs(target,exist_ok=True); marker=os.path.join(target,'.req-hash'); "
                    "h=hashlib.sha256(open(req,'rb').read()).hexdigest() if os.path.isfile(req) else ''; "
                    "done=open(marker).read() if os.path.isfile(marker) else None; "
                    "(subprocess.check_call([sys.executable,'-m','pip','install','--disable-pip-version-check','--no-cache-dir','--no-warn-script-location','--target',target,'-r',req]), open(marker,'w').write(h)) if (h and h!=done) else None; "
                    "sys.path.insert(0,target); sys.argv=[" + repr(str(script)) + "]; runpy.run_path(" + repr(str(script)) + ",run_name='__main__')")
                cmd = [sys.executable, '-u', '-c', bootstrap]
            else:
                cmd = ['node', str(script)]

        log_path = folder / 'bot.log'
        if log_path.exists() and log_path.stat().st_size > 5 * 1024 * 1024:
            rotated = folder / 'bot.log.1'
            try:
                rotated.unlink(missing_ok=True); os.replace(log_path, rotated)
            except OSError: pass
        log = open(log_path, 'a', encoding='utf-8')
        kwargs = {'cwd': str(folder), 'env': host_env if mode == 'docker' else env, 'stdout': log, 'stderr': subprocess.STDOUT,
                  'start_new_session': (os.name != 'nt')}
        if os.name != 'nt' and mode == 'subprocess':
            kwargs['preexec_fn'] = self._resource_limits(runtime)
        try:
            proc = subprocess.Popen(cmd, **kwargs)
        except Exception:
            log.close()
            raise
        self.processes[bot_id] = proc
        self.logs[bot_id] = log
        with self._conn() as c:
            c.execute("UPDATE hosted_bots SET status='running' WHERE id=?", (bot_id,))
        return proc

    def start(self, owner_id: int, bot_id: str, admin=False):
        bot = self.get_bot(owner_id, bot_id, admin=admin)
        account = self.account(int(bot['owner_id']))
        if account['suspended'] and not admin: raise FleetError('Your hosting account is suspended.')
        if account.get('expires_at') and float(account['expires_at']) <= time.time() and not admin: raise FleetError('Your hosting plan has expired. Contact support to renew.')
        if bot['disabled']: raise FleetError('Bot disabled by administrator.')
        with self._lock:
            old = self.processes.get(bot_id)
            if old and old.poll() is None: return {'pid': old.pid, 'status': 'running'}
            folder = self._bot_dir(bot_id)
            scripts = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in ('.py','.js')
                       and p.name not in ('__init__.py',)]
            if not scripts: raise FleetError('Upload a bot.py or bot.js file first.')
            script = next((p for p in scripts if p.name in ('bot.py','bot.js')), scripts[0])
            proc = self._spawn(bot_id, folder, script)
            self._restart_counts[bot_id] = 0
            self._restart_history.pop(bot_id, None)
            return {'pid': proc.pid, 'status': 'running'}

    def stop(self, owner_id: int, bot_id: str, admin=False):
        self.get_bot(owner_id, bot_id, admin=admin)
        with self._lock:
            p = self.processes.get(bot_id)
            if p and p.poll() is None:
                if os.getenv('BOT_EXECUTION_MODE', 'docker').strip().lower() == 'docker':
                    try:
                        subprocess.run(['docker', 'stop', '--time', '5', f'hostbot-{bot_id}'],
                                      env={k: os.environ[k] for k in os.environ if k in ('PATH','HOME','DOCKER_HOST','DOCKER_CONTEXT','DOCKER_CERT_PATH','DOCKER_TLS_VERIFY','SYSTEMROOT')},
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10, check=False)
                    except Exception:
                        pass
                try:
                    if os.name != 'nt':
                        os.killpg(p.pid, signal.SIGTERM)
                    else:
                        p.terminate()
                    p.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    try:
                        if os.name != 'nt': os.killpg(p.pid, signal.SIGKILL)
                        else: p.kill()
                    except Exception: pass
                    try: p.wait(timeout=3)
                    except Exception: pass
                except ProcessLookupError:
                    pass
            self.processes.pop(bot_id, None)
            log = self.logs.pop(bot_id, None)
            if log:
                try: log.close()
                except Exception: pass
            with self._conn() as c: c.execute("UPDATE hosted_bots SET status='stopped' WHERE id=?", (bot_id,))
        return {'status':'stopped'}

    def _monitor(self):
        while not self._stop_event.wait(5):
            to_restart = []
            with self._lock:
                for bot_id, proc in list(self.processes.items()):
                    if time.time() - self._last_backup.get(bot_id, 0) >= self.backup_interval:
                        try:
                            with self._conn() as c: row_owner=c.execute('SELECT owner_id FROM hosted_bots WHERE id=?',(bot_id,)).fetchone()
                            self._last_backup[bot_id]=time.time()
                            if row_owner: self.create_backup(int(row_owner['owner_id']),bot_id)
                        except Exception: self._last_backup[bot_id]=time.time()
                    rc = proc.poll()
                    if rc is None:
                        continue
                    self.processes.pop(bot_id, None)
                    log = self.logs.pop(bot_id, None)
                    if log:
                        try: log.close()
                        except Exception: pass
                    with self._conn() as c:
                        row = c.execute('SELECT owner_id,disabled FROM hosted_bots WHERE id=?', (bot_id,)).fetchone()
                        c.execute("UPDATE hosted_bots SET status='stopped' WHERE id=?", (bot_id,))
                    if not row or row['disabled'] or self._stop_event.is_set():
                        continue
                    now = time.time()
                    history = [t for t in self._restart_history.get(bot_id, []) if now-t < self.restart_window]
                    history.append(now); self._restart_history[bot_id] = history
                    if len(history) > self.max_restarts:
                        with self._conn() as c:
                            c.execute("UPDATE hosted_bots SET status='crashloop' WHERE id=?", (bot_id,))
                        continue
                    to_restart.append((bot_id, min(2 ** (len(history)-1), 30)))
            for bot_id, delay in to_restart:
                if self._stop_event.wait(delay):
                    break
                with self._lock:
                    try:
                        if bot_id in self.processes:
                            continue
                        with self._conn() as c:
                            row = c.execute('SELECT disabled FROM hosted_bots WHERE id=?', (bot_id,)).fetchone()
                        if not row or row['disabled']:
                            continue
                        folder = self._bot_dir(bot_id)
                        scripts = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in ('.py','.js')]
                        script = next((p for p in scripts if p.name in ('bot.py','bot.js')), scripts[0] if scripts else None)
                        if script:
                            self._spawn(bot_id, folder, script)
                    except Exception:
                        with self._conn() as c:
                            c.execute("UPDATE hosted_bots SET status='crashloop' WHERE id=?", (bot_id,))

    def restore_running(self):
        with self._conn() as c:
            rows = c.execute("SELECT owner_id,id FROM hosted_bots WHERE status='running' AND disabled=0").fetchall()
        restored = 0
        for row in rows:
            try:
                self.start(int(row['owner_id']), row['id'])
                restored += 1
            except Exception:
                with self._conn() as c:
                    c.execute("UPDATE hosted_bots SET status='stopped' WHERE id=?", (row['id'],))
        return restored

    def close(self):
        self._stop_event.set()
        for bot_id, proc in list(self.processes.items()):
            try:
                with self._conn() as c: row=c.execute('SELECT owner_id FROM hosted_bots WHERE id=?',(bot_id,)).fetchone()
                if row: self.stop(int(row['owner_id']), bot_id, admin=True)
            except Exception: pass

    def set_disabled(self, bot_id: str, disabled: bool):
        with self._conn() as c: c.execute('UPDATE hosted_bots SET disabled=? WHERE id=?', (int(disabled), bot_id))
        if disabled:
            with self._conn() as c: row=c.execute('SELECT owner_id FROM hosted_bots WHERE id=?',(bot_id,)).fetchone()
            if row: self.stop(int(row['owner_id']), bot_id, admin=True)

    def log_tail(self, owner_id: int, bot_id: str, lines=80, admin=False):
        self.get_bot(owner_id, bot_id, admin=admin)
        path=self._bot_dir(bot_id)/'bot.log'
        if not path.exists(): return ''
        return '\n'.join(path.read_text(errors='replace').splitlines()[-max(1,min(lines,300)):])
