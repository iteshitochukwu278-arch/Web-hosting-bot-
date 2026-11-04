import asyncio
import os
import sys
import logging
import subprocess
import re
import importlib.util
import psutil
import sqlite3
import hashlib
import json
import zipfile
import threading
import shlex
import time
import html
import signal
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
import aiohttp
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
from persistence import Persistence
from customer_dbs import CustomerDBs, load_servers_from_env, LABELS, EMOJI

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID_STR = os.getenv('OWNER_ID')
ADMIN_ID_STR = os.getenv('ADMIN_ID')
YOUR_USERNAME = os.getenv('YOUR_USERNAME')
UPDATE_CHANNEL = os.getenv('UPDATE_CHANNEL')

if not TOKEN:
    logger.error("BOT_TOKEN not found in environment variables!")
    raise ValueError("BOT_TOKEN is required. Please set it in .env file or environment variables.")

if not OWNER_ID_STR or not ADMIN_ID_STR:
    logger.error("OWNER_ID or ADMIN_ID not found in environment variables!")
    raise ValueError("OWNER_ID and ADMIN_ID are required. Please set them in .env file.")

try:
    OWNER_ID = int(OWNER_ID_STR)
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    logger.error("OWNER_ID or ADMIN_ID must be valid integers!")
    raise

# Optional. Set these in your environment (Render / .env). Leave empty to hide the buttons.
YOUR_USERNAME = (os.getenv('YOUR_USERNAME') or '').strip()
UPDATE_CHANNEL = (os.getenv('UPDATE_CHANNEL') or '').strip()

BASE_DIR = Path(__file__).parent.absolute()
UPLOAD_BOTS_DIR = BASE_DIR / 'upload_bots'
IROTECH_DIR = BASE_DIR / 'inf'
DATABASE_PATH = IROTECH_DIR / 'bot_data.db'

FREE_USER_LIMIT = float('inf')
SUBSCRIBED_USER_LIMIT = float('inf')
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

UPLOAD_BOTS_DIR.mkdir(parents=True, exist_ok=True)
IROTECH_DIR.mkdir(exist_ok=True)

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
BOT_START_TIME = datetime.now()

bot_scripts = {}
user_subscriptions = {}
user_files = {}
user_favorites = {}
banned_users = set()
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
bot_locked = False
bot_stats = {'total_uploads': 0, 'total_downloads': 0, 'total_runs': 0}

def migrate_db():
    logger.info("Running database migrations...")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        
        c.execute("PRAGMA table_info(user_files)")
        columns = [row[1] for row in c.fetchall()]
        if 'upload_date' not in columns:
            logger.info("Adding upload_date column to user_files table...")
            c.execute('ALTER TABLE user_files ADD COLUMN upload_date TEXT')
            logger.info("upload_date column added successfully.")
        
        c.execute("PRAGMA table_info(active_users)")
        columns = [row[1] for row in c.fetchall()]
        if 'join_date' not in columns:
            logger.info("Adding join_date column to active_users table...")
            c.execute('ALTER TABLE active_users ADD COLUMN join_date TEXT')
            logger.info("join_date column added successfully.")
        if 'last_active' not in columns:
            logger.info("Adding last_active column to active_users table...")
            c.execute('ALTER TABLE active_users ADD COLUMN last_active TEXT')
            logger.info("last_active column added successfully.")
        
        conn.commit()
        conn.close()
        logger.info("Database migrations completed successfully.")
    except Exception as e:
        logger.error(f"Database migration error: {e}", exc_info=True)

def init_db():
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT, upload_date TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY, join_date TEXT, last_active TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users
                     (user_id INTEGER PRIMARY KEY, banned_date TEXT, reason TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS favorites
                     (user_id INTEGER, file_name TEXT, PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS bot_stats
                     (stat_name TEXT PRIMARY KEY, stat_value INTEGER)''')
        
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        
        for stat in ['total_uploads', 'total_downloads', 'total_runs']:
            c.execute('INSERT OR IGNORE INTO bot_stats (stat_name, stat_value) VALUES (?, 0)', (stat,))
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization error: {e}", exc_info=True)

def load_data():
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"Invalid expiry date for user {user_id}")
        
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))
        
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())
        
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall())
        
        c.execute('SELECT user_id FROM banned_users')
        banned_users.update(user_id for (user_id,) in c.fetchall())
        
        c.execute('SELECT user_id, file_name FROM favorites')
        for user_id, file_name in c.fetchall():
            if user_id not in user_favorites:
                user_favorites[user_id] = []
            user_favorites[user_id].append(file_name)
        
        c.execute('SELECT stat_name, stat_value FROM bot_stats')
        for stat_name, stat_value in c.fetchall():
            bot_stats[stat_name] = stat_value
        
        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(banned_users)} banned, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)

# -- EXTERNAL STORAGE (MongoDB) ---------------------------------------------------------
# Restores the SQLite database + all customer files from MongoDB BEFORE the database is opened,
# so a Render redeploy/restart no longer wipes anything. Disabled (with a warning) if MONGO_URI is empty.
_pool_urls = load_servers_from_env(os.environ)
_default_max = os.getenv('DB_MAX_PER_SERVER')
customers = CustomerDBs(DATABASE_PATH, _pool_urls, secret=os.getenv('DB_SECRET', ''),
                        max_per_server={e: int(os.getenv(f'{e.upper()}_MAX_PER_SERVER', _default_max or '0') or 0)
                                        for e in ('mongo', 'mysql', 'postgres')})
persist = Persistence(
    uri=os.getenv('MONGO_URI'),
    db_name=os.getenv('MONGO_DB_NAME', 'hostbot'),
    upload_dir=UPLOAD_BOTS_DIR,
    sqlite_path=DATABASE_PATH,
    customer=customers,
)
persist.restore_all()

init_db()
migrate_db()
load_data()

# ── AUTO DEPENDENCY INSTALLER ────────────────────────
# Import name → actual pip package name (when the two differ)
IMPORT_TO_PIP = {
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'pyrogram': 'pyrogram',
    'tgcrypto': 'TgCrypto',
    'PIL': 'Pillow',
    'cv2': 'opencv-python-headless',
    'bs4': 'beautifulsoup4',
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'Crypto': 'pycryptodome',
    'Cryptodome': 'pycryptodomex',
    'jwt': 'PyJWT',
    'dateutil': 'python-dateutil',
    'discord': 'discord.py',
    'yt_dlp': 'yt-dlp',
    'youtube_dl': 'youtube-dl',
    'sklearn': 'scikit-learn',
    'websocket': 'websocket-client',
    'mysql': 'mysql-connector-python',
    'serial': 'pyserial',
}

# Python built-in modules — never try to install these
STDLIB_MODULES = set(sys.stdlib_module_names) if hasattr(sys, 'stdlib_module_names') else {
    'os', 'sys', 'time', 'json', 're', 'math', 'random', 'datetime', 'collections',
    'itertools', 'functools', 'subprocess', 'threading', 'asyncio', 'socket', 'io',
    'sqlite3', 'hashlib', 'base64', 'logging', 'pathlib', 'shutil', 'zipfile',
    'urllib', 'http', 'email', 'csv', 'string', 'copy', 'typing', 'traceback',
    'signal', 'struct', 'queue', 'multiprocessing', 'abc', 'enum', 'uuid', 'glob'
}

def extract_imports(file_path):
    """Extracts all top-level 'import X' / 'from X import Y' names from the file"""
    modules = set()
    try:
        content = Path(file_path).read_text(errors='ignore')
        for match in re.finditer(r'^\s*(?:import|from)\s+([a-zA-Z0-9_\.]+)', content, re.MULTILINE):
            top_module = match.group(1).split('.')[0]
            modules.add(top_module)
    except Exception as e:
        logger.error(f"Import extraction error: {e}")
    return modules

def install_missing_dependencies(file_path, timeout=90):
    """Checks the libraries used by the script and installs any missing ones with pip.
    Returns: (installed_list, failed_list)"""
    modules = extract_imports(file_path)
    installed, failed = [], []

    for mod in modules:
        if mod in STDLIB_MODULES or mod in ('__future__',):
            continue
        if importlib.util.find_spec(mod) is not None:
            continue  # already installed

        pip_name = IMPORT_TO_PIP.get(mod, mod)
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'pip', 'install', pip_name, '--break-system-packages', '--quiet'],
                capture_output=True, text=True, timeout=timeout
            )
            if result.returncode == 0:
                installed.append(pip_name)
            else:
                failed.append(pip_name)
        except Exception as e:
            logger.error(f"pip install error for {pip_name}: {e}")
            failed.append(pip_name)

    return installed, failed

def get_user_file_limit(user_id):
    return OWNER_LIMIT  # Unlimited for everyone — no upload limit

def format_limit(limit):
    return "Unlimited ♾️" if limit == float('inf') else f"{limit} files"

def _contact_url():
    name = (YOUR_USERNAME or '').strip().lstrip('@')
    return f"https://t.me/{name}" if name else None

def _updates_url():
    ch = (UPDATE_CHANNEL or '').strip()
    if not ch:
        return None
    return ch if ch.startswith('http') else 'https://t.me/' + ch.lstrip('@')

def _updates_row():
    url = _updates_url()
    return [[InlineKeyboardButton(text="📢 Updates", url=url)]] if url else []

def _contact_row():
    url = _contact_url()
    return [[InlineKeyboardButton(text="💬 Contact Support", url=url)]] if url else []

def get_main_keyboard(user_id):
    rows = [
        *_updates_row(),
        [InlineKeyboardButton(text="📤 Upload File", callback_data="upload_file"),
         InlineKeyboardButton(text="📁 My Files", callback_data="check_files")],
        [InlineKeyboardButton(text="🔍 Search Files", callback_data="search_files"),
         InlineKeyboardButton(text="⚡ Bot Speed", callback_data="bot_speed")],
        [InlineKeyboardButton(text="💾 My Backup", callback_data="my_backup"),
         InlineKeyboardButton(text="🗄️ My Database", callback_data="my_database")],
        [InlineKeyboardButton(text="🖥️ Terminal", callback_data="terminal_help")],
    ]
    if user_id in admin_ids:
        rows.append([InlineKeyboardButton(text="📊 My Stats", callback_data="statistics"),
                     InlineKeyboardButton(text="👑 Admin Panel", callback_data="admin_panel")])
    else:
        rows.append([InlineKeyboardButton(text="📊 My Stats", callback_data="statistics")])
    rows.extend(_contact_row())
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_admin_panel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏱️ Bot Uptime", callback_data="admin_bot_uptime")],
        [InlineKeyboardButton(text="👥 Total Users", callback_data="admin_total_users_simple")],
        [InlineKeyboardButton(text="📝 Logs", callback_data="admin_view_logs")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    return keyboard

def build_welcome_text(user_id: int, full_name: str) -> str:
    updates_line = f"\n📢 <b>Updates:</b> {_updates_url()}\n" if _updates_url() else "\n"
    return f"""╔═══════════════════════╗
    🚀 <b>HOSTING BOT</b> 🚀
    ⚡ <i>Script Runner Bot</i>
╚═══════════════════════╝

👋 <b>Welcome, {full_name}!</b>

🆔 <b>Your ID:</b> <code>{user_id}</code>
📦 <b>Upload Limit:</b> Unlimited ♾️

━━━━━━━━━━━━━━━━━━━━
<b>🔥 AVAILABLE FEATURES</b>

📤 <b>Upload Files</b> — Python, JS, ZIP
📁 <b>Manage Files</b> — View, delete, organize
🔍 <b>Search Files</b> — Find by name/type
▶️ <b>Run Scripts</b> — Execute Python/Node.js
🛑 <b>Stop Scripts</b> — Force stop running
📊 <b>View Stats</b> — Usage & storage
⚡ <b>Speed Test</b> — Bot response time
💾 <b>File Info</b> — Size, type, date
ℹ️ <b>Help &amp; Support</b> — Get assistance

━━━━━━━━━━━━━━━━━━━━{updates_line}
🔒 <b>Version:</b> 2.0"""

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    
    if user_id in banned_users:
        await message.answer("🚫 <b>You are banned from using this bot!</b>\n\nContact admin for more info.", parse_mode="HTML")
        return
    
    active_users.add(user_id)
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        c.execute('INSERT OR IGNORE INTO active_users (user_id, join_date, last_active) VALUES (?, ?, ?)', 
                  (user_id, now, now))
        c.execute('UPDATE active_users SET last_active = ? WHERE user_id = ?', (now, user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving active user: {e}")
    
    # Round robin: the 1st customer goes to database server 1, the 2nd to server 2, ... then back to server 1.
    if customers.enabled and customers.assigned_index(user_id) is None:
        idx = await asyncio.to_thread(customers.assign, user_id)
        if idx is None:
            customers.pop_error(user_id)
            try:
                await bot.send_message(OWNER_ID, "⚠️ <b>No database server available.</b> A new customer could not be given "
                                                 "a database (all servers full or offline). Check /dbstats and your "
                                                 "CUSTOMER_DB_* variables.", parse_mode="HTML")
            except Exception:
                pass
        else:
            await _persist_now()

    welcome_text = build_welcome_text(user_id, message.from_user.full_name)
    await message.answer(welcome_text, reply_markup=get_main_keyboard(user_id), parse_mode="HTML")

@dp.callback_query(F.data == "back_to_main")
async def callback_back_to_main(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    welcome_text = f"""
╔═══════════════════════╗
    🏠 <b>MAIN MENU</b> 🏠
╚═══════════════════════╝

👤 <b>User:</b> {callback.from_user.full_name}
🆔 <b>ID:</b> <code>{user_id}</code>
📦 <b>Files:</b> {len(user_files.get(user_id, []))} (Unlimited ♾️)

Use buttons below to navigate 👇
"""
    await callback.message.edit_text(welcome_text, reply_markup=get_main_keyboard(user_id), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "upload_file")
async def callback_upload_file(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if bot_locked and user_id not in admin_ids:
        await callback.answer("🔒 Bot is locked for maintenance!", show_alert=True)
        return
    
    current_files = len(user_files.get(user_id, []))
    limit = get_user_file_limit(user_id)
    
    upload_text = f"""
╔═══════════════════════╗
    📤 <b>UPLOAD FILES</b> 📤
╚═══════════════════════╝

📊 <b>Current Usage:</b> {current_files} files (Unlimited ♾️)

📝 <b>Supported Formats:</b>
🐍 Python (.py)  ·  🟨 JavaScript (.js)
📦 ZIP Archives (.zip)
📋 <code>requirements.txt</code> → packages are installed for you
📋 <code>package.json</code> → npm packages are installed for you
🔐 <code>.env</code> → your secrets/settings for your bot
🖥️ Terminal: /term (pip install, ls, python bot.py ...)

▶️ .py / .js files <b>start automatically</b> once uploaded.

━━━━━━━━━━━━━━━━━━━━
<b>💡 How to Upload:</b>

1️⃣ Send your file to the bot
2️⃣ Wait for upload confirmation
3️⃣ File will be saved automatically

⚡ <b>Upload limit:</b> {limit} files
🔥 <b>Quick & Easy!</b>
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(upload_text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "check_files")
async def callback_check_files(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    files = user_files.get(user_id, [])
    
    if not files:
        text = """
╔═══════════════════════╗
    📁 <b>MY FILES</b> 📁
╚═══════════════════════╝

📭 <b>No files found!</b>

Upload your first file to get started! 🚀
"""
        back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 Upload File", callback_data="upload_file")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
    else:
        text = f"""
╔═══════════════════════╗
    📁 <b>MY FILES ({len(files)})</b> 📁
╚═══════════════════════╝

"""
        buttons = []
        for i, (file_name, file_type) in enumerate(files, 1):
            icon = "🐍" if file_type == "py" else "🟨" if file_type == "js" else "📦"
            text += f"{i}. {icon} <code>{file_name}</code>\n"
            
            is_favorite = file_name in user_favorites.get(user_id, [])
            star = "⭐" if is_favorite else "☆"
            
            buttons.append([
                InlineKeyboardButton(text=f"▶️ Run {file_name[:15]}", callback_data=f"run_script:{file_name}"),
                InlineKeyboardButton(text=f"{star}", callback_data=f"toggle_fav:{file_name}")
            ])
            buttons.append([
                InlineKeyboardButton(text=f"ℹ️ Info {file_name[:15]}", callback_data=f"file_info:{file_name}"),
                InlineKeyboardButton(text=f"🗑️ Delete", callback_data=f"delete_file:{file_name}")
            ])
        
        buttons.append([InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")])
        back_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "my_favorites")
async def callback_my_favorites(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    favorites = user_favorites.get(user_id, [])
    
    if not favorites:
        text = """
╔═══════════════════════╗
    ⭐ <b>FAVORITES</b> ⭐
╚═══════════════════════╝

💭 No favorite files yet!

Add files to favorites for quick access! 🚀
"""
        buttons = [[InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]]
    else:
        text = f"""
╔═══════════════════════╗
    ⭐ <b>FAVORITES ({len(favorites)})</b> ⭐
╚═══════════════════════╝

"""
        buttons = []
        for i, file_name in enumerate(favorites, 1):
            text += f"{i}. ⭐ <code>{file_name}</code>\n"
            buttons.append([
                InlineKeyboardButton(text=f"▶️ {file_name[:20]}", callback_data=f"run_script:{file_name}"),
                InlineKeyboardButton(text=f"❌", callback_data=f"toggle_fav:{file_name}")
            ])
        
        buttons.append([InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")])
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "search_files")
async def callback_search_files(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    files = user_files.get(user_id, [])
    
    text = f"""
╔═══════════════════════╗
    🔍 <b>SEARCH FILES</b> 🔍
╚═══════════════════════╝

📊 <b>Total Files:</b> {len(files)}

<b>File Types:</b>
🐍 Python: {sum(1 for f in files if f[1] == 'py')}
🟨 JavaScript: {sum(1 for f in files if f[1] == 'js')}
📦 ZIP: {sum(1 for f in files if f[1] == 'zip')}

━━━━━━━━━━━━━━━━━━━━
To search, use:
<code>/search filename</code>
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 View All Files", callback_data="check_files")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "bot_speed")
async def callback_bot_speed(callback: types.CallbackQuery):
    start_time = datetime.now()
    await callback.answer("⚡ Testing...")
    end_time = datetime.now()
    speed = (end_time - start_time).total_seconds() * 1000
    
    if speed < 100:
        status = "🟢 Excellent"
        emoji = "🚀"
    elif speed < 300:
        status = "🟡 Good"
        emoji = "⚡"
    else:
        status = "🔴 Slow"
        emoji = "🐌"
    
    text = f"""
╔═══════════════════════╗
    ⚡ <b>SPEED TEST</b> ⚡
╚═══════════════════════╝

{emoji} <b>Response Time:</b> {speed:.2f}ms
📊 <b>Status:</b> {status}

🖥️ <b>Server Info:</b>
• CPU: {psutil.cpu_percent()}%
• Memory: {psutil.virtual_memory().percent}%
• Uptime: Online ✅

✨ Bot is running smoothly!
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Test Again", callback_data="bot_speed"),
         InlineKeyboardButton(text="🏠 Home", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "statistics")
async def callback_statistics(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    user_file_count = len(user_files.get(user_id, []))
    user_fav_count = len(user_favorites.get(user_id, []))
    limit = get_user_file_limit(user_id)
    is_premium = user_id in user_subscriptions
    
    text = f"""
╔═══════════════════════╗
    📊 <b>YOUR STATISTICS</b> 📊
╚═══════════════════════╝

👤 <b>User:</b> {callback.from_user.full_name}
🆔 <b>ID:</b> <code>{user_id}</code>

━━━━━━━━━━━━━━━━━━━━
📦 <b>FILE STATISTICS:</b>

📁 Total Files: {user_file_count} (Unlimited ♾️)
⭐ Favorites: {user_fav_count}
💎 Account: Free 🆓 (Unlimited ♾️)
🚀 Running: {sum(1 for k in bot_scripts if k.startswith(f"{user_id}_"))}

━━━━━━━━━━━━━━━━━━━━
📈 <b>USAGE:</b>

📤 Uploads: {bot_stats.get('total_uploads', 0)}
📥 Downloads: {bot_stats.get('total_downloads', 0)}
▶️ Script Runs: {bot_stats.get('total_runs', 0)}

{'✅ Bot Status: Active' if not bot_locked else '🔒 Bot: Maintenance'}
"""
    
    if user_id in admin_ids:
        text += f"\n━━━━━━━━━━━━━━━━━━━━\n👑 <b>ADMIN STATS:</b>\n"
        text += f"👥 Total Users: {len(active_users)}\n"
        text += f"📁 Total Files: {sum(len(files) for files in user_files.values())}\n"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "help_info")
async def callback_help_info(callback: types.CallbackQuery):
    text = """
╔═══════════════════════╗
    ℹ️ <b>HELP & INFO</b> ℹ️
╚═══════════════════════╝

<b>🎯 HOW TO USE:</b>

1️⃣ <b>Upload Files:</b>
   • Click 'Upload File'
   • Send your .py, .js, or .zip file
   • File will be saved automatically

2️⃣ <b>Run Scripts:</b>
   • Go to 'My Files'
   • Click 'Run' on any file
   • Monitor script execution

3️⃣ <b>Manage Files:</b>
   • View all files in 'My Files'
   • Add to favorites with ⭐
   • Delete unwanted files

4️⃣ <b>Search:</b>
   • Use /search [filename]
   • Quick file lookup

━━━━━━━━━━━━━━━━━━━━
<b>💡 COMMANDS:</b>

/start - Start the bot
/help - Show this help
/search - Search files
/stats - Your statistics
/premium - Free info

<b>Need help? Contact owner! 💬</b>
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Features", callback_data="all_features")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "all_features")
async def callback_all_features(callback: types.CallbackQuery):
    text = """
╔═══════════════════════╗
    🎯 <b>ALL FEATURES</b> 🎯
╚═══════════════════════╝

<b>✨ FREE USER FEATURES (12+):</b>

1. 📤 Upload Files (Python, JS, ZIP)
2. 📁 View & Manage Files
3. ⭐ Add to Favorites
4. 🔍 Search Files by Name
5. ▶️ Run Python Scripts
6. ▶️ Run JavaScript Scripts
7. 🛑 Stop Running Scripts
8. 📊 View Your Statistics
9. ⚡ Bot Speed Test
10. 📥 Download Your Files
11. 💾 View File Information
12. ℹ️ Help & Support
13. 🎯 Feature Discovery

━━━━━━━━━━━━━━━━━━━━
<b>🆓 EVERYTHING IS FREE!</b>

• Unlimited file uploads
• No premium plans
• Same features for everyone
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "get_premium")
async def callback_get_premium(callback: types.CallbackQuery):
    text = """
╔═══════════════════════╗
    🆓 <b>100% FREE</b> 🆓
╚═══════════════════════╝

This bot has <b>no premium plans</b>.
✅ Unlimited uploads
✅ All features are free for everyone

Need help? Contact the owner 💬
"""
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        *_contact_row(),
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_panel")
async def callback_admin_panel(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id not in admin_ids:
        await callback.answer("❌ Admin access required!", show_alert=True)
        return
    
    text = """
╔═══════════════════════╗
    👑 <b>ADMIN PANEL</b> 👑
╚═══════════════════════╝

Select an option below 👇
"""
    
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_bot_uptime")
async def callback_admin_bot_uptime(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in admin_ids:
        await callback.answer("❌ Admin access required!", show_alert=True)
        return

    delta = datetime.now() - BOT_START_TIME
    days = delta.days
    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    text = f"""╔═══════════════════════╗
    ⏱️ <b>BOT UPTIME</b> ⏱️
╚═══════════════════════╝

🟢 <b>Status:</b> Running
📅 <b>Started:</b> {BOT_START_TIME.strftime('%d %b %Y, %I:%M %p')}
⏳ <b>Uptime:</b> {days}d {hours}h {minutes}m {seconds}s"""

    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_total_users_simple")
async def callback_admin_total_users_simple(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in admin_ids:
        await callback.answer("❌ Admin access required!", show_alert=True)
        return

    total_users = len(active_users)
    total_active_scripts = len(bot_scripts)

    text = f"""╔═══════════════════════╗
    👥 <b>TOTAL USERS</b> 👥
╚═══════════════════════╝

👤 <b>Total Users:</b> {total_users}
▶️ <b>Currently Running Scripts:</b> {total_active_scripts}"""

    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Back", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_fav:"))
async def callback_toggle_favorite(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    file_name = callback.data.split(":", 1)[1]
    
    if user_id not in user_favorites:
        user_favorites[user_id] = []
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        
        if file_name in user_favorites[user_id]:
            user_favorites[user_id].remove(file_name)
            c.execute('DELETE FROM favorites WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            await callback.answer("❌ Removed from favorites!", show_alert=True)
        else:
            user_favorites[user_id].append(file_name)
            c.execute('INSERT OR IGNORE INTO favorites (user_id, file_name) VALUES (?, ?)', (user_id, file_name))
            await callback.answer("⭐ Added to favorites!", show_alert=True)
        
        conn.commit()
        conn.close()
        
        await callback_check_files(callback)
        
    except Exception as e:
        logger.error(f"Error toggling favorite: {e}")
        await callback.answer(f"❌ Error: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("file_info:"))
async def callback_file_info(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    file_name = callback.data.split(":", 1)[1]
    
    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    file_path = user_folder / file_name
    
    if not file_path.exists():
        await callback.answer("❌ File not found!", show_alert=True)
        return
    
    file_size = file_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    file_ext = file_path.suffix
    modified_time = datetime.fromtimestamp(file_path.stat().st_mtime)
    
    is_favorite = file_name in user_favorites.get(user_id, [])
    
    text = f"""
╔═══════════════════════╗
    ℹ️ <b>FILE INFO</b> ℹ️
╚═══════════════════════╝

📄 <b>Name:</b> <code>{file_name}</code>

📦 <b>Type:</b> {file_ext.upper()} File
💾 <b>Size:</b> {file_size_mb:.2f} MB ({file_size} bytes)
📅 <b>Modified:</b> {modified_time.strftime('%Y-%m-%d %H:%M')}
⭐ <b>Favorite:</b> {'Yes ✨' if is_favorite else 'No'}

🔐 <b>MD5:</b> <code>{hashlib.md5(file_path.read_bytes()).hexdigest()[:16]}...</code>
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Run", callback_data=f"run_script:{file_name}"),
         InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_file:{file_name}")],
        [InlineKeyboardButton(text="📁 My Files", callback_data="check_files"),
         InlineKeyboardButton(text="🏠 Home", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.message(F.document)
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    
    if user_id in banned_users:
        await message.answer("🚫 You are banned from using this bot!")
        return
    
    if bot_locked and user_id not in admin_ids:
        await message.answer("🔒 Bot is currently locked!")
        return
    
    document = message.document
    file_name = os.path.basename((document.file_name or '').replace('\\', '/')) or 'file'
    file_ext = os.path.splitext(file_name)[1].lower()

    special = _special_kind(file_name)
    if special:
        await _handle_special_upload(message, document, special)
        return
    
    if file_ext not in ['.py', '.js', '.zip']:
        await message.answer("❌ Supported files: .py, .js, .zip, requirements.txt, package.json and .env")
        return
    
    current_files = len(user_files.get(user_id, []))
    limit = get_user_file_limit(user_id)
    
    if current_files >= limit:
        await message.answer(f"❌ Upload limit reached! ({current_files}/{limit})")
        return
    
    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    user_folder.mkdir(exist_ok=True)
    
    file_path = user_folder / file_name
    
    try:
        file_size_kb = document.file_size / 1024
        
        status_msg = await message.answer(
            f"📤 <b>Preparing upload...</b>\n\n"
            f"📄 File: <code>{file_name}</code>\n"
            f"💾 Size: {file_size_kb:.2f} KB\n\n"
            f"▓░░░░░░░░░ 0%",
            parse_mode="HTML"
        )
        
        await asyncio.sleep(0.3)
        await status_msg.edit_text(
            f"📥 <b>Downloading...</b>\n\n"
            f"📄 File: <code>{file_name}</code>\n"
            f"💾 Size: {file_size_kb:.2f} KB\n\n"
            f"▓▓▓░░░░░░░ 30%",
            parse_mode="HTML"
        )
        
        await bot.download(document, destination=file_path)
        
        await status_msg.edit_text(
            f"💾 <b>Saving to database...</b>\n\n"
            f"📄 File: <code>{file_name}</code>\n"
            f"💾 Size: {file_size_kb:.2f} KB\n\n"
            f"▓▓▓▓▓▓▓░░░ 70%",
            parse_mode="HTML"
        )
        
        if user_id not in user_files:
            user_files[user_id] = []
        
        user_files[user_id].append((file_name, file_ext[1:]))
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type, upload_date) VALUES (?, ?, ?, ?)',
                  (user_id, file_name, file_ext[1:], now))
        c.execute('UPDATE bot_stats SET stat_value = stat_value + 1 WHERE stat_name = ?', ('total_uploads',))
        conn.commit()
        conn.close()
        
        bot_stats['total_uploads'] = bot_stats.get('total_uploads', 0) + 1
        await _persist_now(user_id)
        
        await status_msg.edit_text(
            f"✅ <b>Finalizing...</b>\n\n"
            f"📄 File: <code>{file_name}</code>\n"
            f"💾 Size: {file_size_kb:.2f} KB\n\n"
            f"▓▓▓▓▓▓▓▓▓▓ 100%",
            parse_mode="HTML"
        )
        
        await asyncio.sleep(0.5)
        
        if file_ext == '.zip':
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Extract ZIP", callback_data=f"extract_zip:{file_name}"),
                 InlineKeyboardButton(text="⭐ Add Favorite", callback_data=f"toggle_fav:{file_name}")],
                [InlineKeyboardButton(text="ℹ️ File Info", callback_data=f"file_info:{file_name}"),
                 InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_file:{file_name}")],
                [InlineKeyboardButton(text="📁 My Files", callback_data="check_files"),
                 InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
            ])
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="▶️ Run Now", callback_data=f"run_script:{file_name}"),
                 InlineKeyboardButton(text="⭐ Add Favorite", callback_data=f"toggle_fav:{file_name}")],
                [InlineKeyboardButton(text="ℹ️ File Info", callback_data=f"file_info:{file_name}"),
                 InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_file:{file_name}")],
                [InlineKeyboardButton(text="📁 My Files", callback_data="check_files"),
                 InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
            ])
        
        await status_msg.edit_text(
            f"""
╔═══════════════════════╗
    ✅ <b>UPLOAD SUCCESS!</b> ✅
╚═══════════════════════╝

📄 <b>File:</b> <code>{file_name}</code>
📦 <b>Type:</b> {file_ext[1:].upper()}
💾 <b>Size:</b> {document.file_size / 1024:.2f} KB
📊 <b>Usage:</b> {current_files + 1} files (Unlimited ♾️)

🎉 File uploaded successfully!
""",
            reply_markup=keyboard,
            parse_mode="HTML"
        )

        if file_ext in ('.py', '.js'):
            try:
                await _auto_run_after_upload(message, user_id, file_name)
            except Exception as auto_err:
                logger.error(f"auto-run failed: {auto_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        await message.answer(f"❌ Upload failed: {str(e)}")

_starting_scripts = set()  # script keys that are being launched right now

# Environment variables of THIS host bot that must never leak into hosted scripts.
# If a hosted bot inherited BOT_TOKEN it would poll the host bot's token ("Conflict" errors),
# and an inherited PORT would clash with the host's own web server.
_HOST_ONLY_ENV = (
    'BOT_TOKEN', 'OWNER_ID', 'ADMIN_ID', 'ADMIN_PASSWORD', 'UPDATE_CHANNEL', 'YOUR_USERNAME',
    'PORT', 'RENDER_EXTERNAL_URL', 'DATABASE_PATH', 'WEB_CONCURRENCY',
    'MONGO_URI', 'MONGO_DB_NAME', 'PERSIST_MAX_FILE_MB', 'DB_SECRET', 'DB_MAX_PER_SERVER',
    'MONGO_MAX_PER_SERVER', 'MYSQL_MAX_PER_SERVER', 'POSTGRES_MAX_PER_SERVER',
)

def _child_env(user_id=None):
    env = os.environ.copy()
    for k in _HOST_ONLY_ENV:
        env.pop(k, None)
    env['PYTHONUNBUFFERED'] = '1'
    # never leak the pool's admin logins to a customer's script
    for k in list(env):
        if k.startswith('CUSTOMER_MONGO_URI') or k.startswith('CUSTOMER_MYSQL_URL') \
                or k.startswith('CUSTOMER_POSTGRES_URL') or k.startswith('CUSTOMER_DB_'):
            env.pop(k, None)
    # Private databases for this customer's bot (MONGO_* / MYSQL_*), as chosen in "My Database".
    # Blocking (may create the MySQL user), so call it with asyncio.to_thread.
    if user_id is not None:
        env.update(customers.env_for(user_id))
        env.update(_user_dotenv(user_id))
        packages = UPLOAD_BOTS_DIR / str(user_id) / '.packages'
        if packages.is_dir():      # the customer's own pip packages (requirements.txt), isolated from the host
            env['PYTHONPATH'] = str(packages)
    return env

# -- CLOUD STORAGE HELPERS ----------------------------------------------------------------
_stopping = set()      # script keys the user/admin stopped on purpose (so they are not treated as crashes)
_last_backup = {}      # script key -> time of last crash backup (cooldown)

def _bg(fn, *args):
    threading.Thread(target=fn, args=args, daemon=True).start()

def _note_stop(script_key):
    """A script was stopped on purpose: do not restart it after a host restart, do not report a crash."""
    _stopping.add(script_key)
    _bg(persist.clear_running, script_key)

async def _persist_now(user_id=None):
    """Push the latest files + database to MongoDB right away (never raises)."""
    try:
        if user_id is not None:
            await asyncio.to_thread(persist.sync_user, user_id)
        await asyncio.to_thread(persist.snapshot_db)
    except Exception as e:
        logger.error(f"persist_now failed: {e}")

def _terminate_tree(pid, grace=3):
    """Terminate a process and all its children, then kill whatever is still alive."""
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    procs = parent.children(recursive=True) + [parent]
    for p in procs:
        try:
            p.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(procs, timeout=grace)
    for p in alive:
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass

def _reap_dead_scripts():
    """Drop entries of scripts that already exited/crashed so 'Start' works again."""
    for key in list(bot_scripts.keys()):
        info = bot_scripts[key]
        try:
            if info['process'].poll() is not None:
                lf = info.get('log_file')
                if lf and not lf.closed:
                    lf.close()
                del bot_scripts[key]
                _bg(persist.clear_running, key)
        except Exception:
            bot_scripts.pop(key, None)

def _kill_orphans(file_path):
    """Kill leftover processes still running the same file (e.g. after a host restart)."""
    tracked = {i['process'].pid for i in bot_scripts.values()}
    target = str(file_path)
    me = os.getpid()
    for p in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = p.info.get('cmdline') or []
            if p.info['pid'] in tracked or p.info['pid'] == me:
                continue
            if target in cmd:
                _terminate_tree(p.info['pid'])
        except Exception:
            continue

@dp.callback_query(F.data.startswith("run_script:"))
async def callback_run_script(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    file_name = callback.data.split(":", 1)[1]

    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    file_path = user_folder / file_name

    if not file_path.exists():
        await callback.answer("❌ File not found!", show_alert=True)
        return

    script_key = f"{user_id}_{file_name}"

    _reap_dead_scripts()
    if script_key in bot_scripts or script_key in _starting_scripts:
        await callback.answer("⚠️ Script is already running or starting!", show_alert=True)
        return

    file_ext = file_path.suffix.lower()
    if file_ext not in ('.py', '.js'):
        await callback.answer("❌ Cannot run this file type!", show_alert=True)
        return

    # Safety: never run a script that uses the host bot's own token (it would break this bot)
    try:
        if TOKEN and TOKEN in file_path.read_text(errors='ignore'):
            await callback.answer(
                "❌ This file contains the hosting bot's own token. Use a different bot token for your script.",
                show_alert=True)
            return
    except Exception:
        pass

    # Lock immediately so a second tap cannot start a duplicate copy, and answer the tap right away.
    _starting_scripts.add(script_key)
    _stopping.discard(script_key)
    log_file = None
    try:
        await callback.answer("⏳ Starting your script, please wait...")

        log_file_path = user_folder / f"{file_path.stem}.log"
        log_file = open(log_file_path, 'w')

        await asyncio.to_thread(_ensure_dependencies, user_folder)

        status_msg = None
        if file_ext == '.py':
            status_msg = await callback.message.answer("📦 <i>Checking dependencies...</i>", parse_mode="HTML")
            # pip can take a long time: run it in a thread so the whole bot does not freeze
            installed, failed = await asyncio.to_thread(install_missing_dependencies, file_path)
            if installed:
                log_file.write(f"[AUTO-INSTALLER] Installed: {', '.join(installed)}\n")
            if failed:
                log_file.write(f"[AUTO-INSTALLER] Failed to install: {', '.join(failed)}\n")
            log_file.flush()
            try:
                await status_msg.delete()
            except Exception:
                pass

        # make sure no old copy of this same file is still alive
        await asyncio.to_thread(_kill_orphans, file_path)

        cmd = [sys.executable, '-u', str(file_path)] if file_ext == '.py' else ['node', str(file_path)]
        child_env = await asyncio.to_thread(_child_env, user_id)
        process = subprocess.Popen(
            cmd,
            cwd=str(user_folder),
            stdout=log_file,
            stderr=log_file,
            env=child_env,
        )

        bot_scripts[script_key] = {
            'process': process,
            'file_name': file_name,
            'script_owner_id': user_id,
            'start_time': datetime.now(),
            'user_folder': str(user_folder),
            'type': file_ext[1:],
            'log_file': log_file
        }

        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('UPDATE bot_stats SET stat_value = stat_value + 1 WHERE stat_name = ?', ('total_runs',))
        conn.commit()
        conn.close()
        bot_stats['total_runs'] = bot_stats.get('total_runs', 0) + 1
        _bg(persist.mark_running, script_key, user_id, file_name)

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛑 Stop Script", callback_data=f"stop_script:{script_key}")],
            [InlineKeyboardButton(text="📋 View Output", callback_data=f"view_log:{file_name}")],
            [InlineKeyboardButton(text="📁 My Files", callback_data="check_files"),
             InlineKeyboardButton(text="🏠 Home", callback_data="back_to_main")]
        ])
        await callback.message.answer(
            f"✅ <b>{file_name}</b> started! (PID: {process.pid})",
            reply_markup=keyboard, parse_mode="HTML")
        db_warning = customers.pop_error(user_id)
        if db_warning:
            await callback.message.answer(db_warning)

    except Exception as e:
        logger.error(f"Error running script: {e}")
        if log_file and not log_file.closed and script_key not in bot_scripts:
            log_file.close()
        try:
            await callback.message.answer(f"❌ Error starting script: {e}")
        except Exception:
            pass
    finally:
        _starting_scripts.discard(script_key)

@dp.callback_query(F.data.startswith("view_log:"))
async def callback_view_log(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    file_name = callback.data.split(":", 1)[1]
    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    file_stem = Path(file_name).stem
    log_path = user_folder / f"{file_stem}.log"

    if not log_path.exists():
        await callback.answer("❌ No log found. Run the script first.", show_alert=True)
        return

    try:
        content = log_path.read_text(errors='replace').strip()
        script_key = f"{user_id}_{file_name}"
        running_note = "🟢 Status: Running" if script_key in bot_scripts else "🔴 Status: Stopped/Exited"

        if not content:
            content = "(No output yet — the script is running silently)"
        elif len(content) > 3500:
            content = "...(truncated)...\n" + content[-3500:]

        text = f"""╔═══════════════════════╗
    📋 <b>SCRIPT OUTPUT</b> 📋
╚═══════════════════════╝

📄 <b>File:</b> <code>{file_name}</code>
{running_note}

<pre>{content}</pre>"""

        back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data=f"view_log:{file_name}")],
            [InlineKeyboardButton(text="📁 My Files", callback_data="check_files")]
        ])
        await callback.message.answer(text, reply_markup=back_keyboard, parse_mode="HTML")
        await callback.answer()
    except Exception as e:
        await callback.answer(f"❌ Log read error: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("stop_script:"))
async def callback_stop_script(callback: types.CallbackQuery):
    script_key = callback.data.split(":", 1)[1]
    
    if script_key not in bot_scripts:
        await callback.answer("❌ Script not found or already stopped!", show_alert=True)
        return
    
    try:
        script_info = bot_scripts[script_key]
        process = script_info['process']
        log_file = script_info.get('log_file')
        
        if log_file and not log_file.closed:
            log_file.close()
        
        _note_stop(script_key)
        await asyncio.to_thread(_terminate_tree, process.pid)

        bot_scripts.pop(script_key, None)
        
        await callback.answer("✅ Script stopped successfully!", show_alert=True)
        
        if callback.from_user.id in admin_ids:
            await callback.message.edit_text("🛑 Script stopped!", parse_mode="HTML")
        else:
            await callback_back_to_main(callback)
        
    except Exception as e:
        logger.error(f"Error stopping script: {e}")
        await callback.answer(f"❌ Error: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("extract_zip:"))
async def callback_extract_zip(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    file_name = callback.data.split(":", 1)[1]
    
    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    zip_path = user_folder / file_name
    
    if not zip_path.exists():
        await callback.answer("❌ ZIP file not found!", show_alert=True)
        return
    
    if not zipfile.is_zipfile(zip_path):
        await callback.answer("❌ Invalid ZIP file!", show_alert=True)
        return
    
    try:
        status_text = f"""
╔═══════════════════════╗
    📦 <b>EXTRACTING ZIP</b> 📦
╚═══════════════════════╝

📄 File: <code>{file_name}</code>
⏳ Status: <b>Extracting...</b>

Please wait...
"""
        await callback.message.edit_text(status_text, parse_mode="HTML")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(user_folder)
            all_files = zip_ref.namelist()
        
        registered_files = []
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        
        for extracted_file in all_files:
            if extracted_file.endswith('/'):
                continue
            
            file_path = Path(extracted_file)
            file_ext = file_path.suffix.lower()
            
            if file_ext in ['.py', '.js']:
                just_name = file_path.name
                
                # If the file was extracted into a subfolder, move it to the root user_folder
                # so it can be found when running later (registration only tracks the file name)
                extracted_full_path = user_folder / extracted_file
                target_path = user_folder / just_name
                if extracted_full_path.exists() and extracted_full_path != target_path:
                    if not target_path.exists():
                        extracted_full_path.rename(target_path)
                        just_name = target_path.name
                    else:
                        # Name already exists, make it unique (to avoid overwriting)
                        stem, suf = target_path.stem, target_path.suffix
                        counter = 1
                        new_target = user_folder / f"{stem}_{counter}{suf}"
                        while new_target.exists():
                            counter += 1
                            new_target = user_folder / f"{stem}_{counter}{suf}"
                        extracted_full_path.rename(new_target)
                        just_name = new_target.name
                
                if user_id not in user_files:
                    user_files[user_id] = []
                
                user_files[user_id].append((just_name, file_ext[1:]))
                
                c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type, upload_date) VALUES (?, ?, ?, ?)',
                          (user_id, just_name, file_ext[1:], now))
                
                registered_files.append(just_name)
        
        if user_id in user_files:
            user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
        
        c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
        c.execute('DELETE FROM favorites WHERE user_id = ? AND file_name = ?', (user_id, file_name))
        conn.commit()
        conn.close()
        
        if zip_path.exists():
            zip_path.unlink()
        await _persist_now(user_id)
        
        registered_text = "\n".join([f"  • <code>{f}</code>" for f in registered_files[:10]])
        if len(registered_files) > 10:
            registered_text += f"\n  ... and {len(registered_files) - 10} more files"
        elif len(registered_files) == 0:
            registered_text = "  <i>No .py or .js files found</i>"
        
        current_count = len(user_files.get(user_id, []))
        limit = get_user_file_limit(user_id)
        
        success_text = f"""
╔═══════════════════════╗
    ✅ <b>EXTRACTION SUCCESS!</b> ✅
╚═══════════════════════╝

📄 <b>ZIP File:</b> <code>{file_name}</code>
📊 <b>Total Extracted:</b> {len(all_files)} files
✅ <b>Registered:</b> {len(registered_files)} files (.py, .js)
🗑️ <b>ZIP Deleted:</b> Automatically

<b>📋 Registered Files:</b>
{registered_text}

📦 <b>Your Files:</b> {current_count} (Unlimited ♾️)

✨ Extraction completed successfully!
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 My Files", callback_data="check_files"),
             InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
        
        await callback.message.edit_text(success_text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("✅ ZIP extracted & registered!")
        
    except zipfile.BadZipFile:
        await callback.answer("❌ Corrupted ZIP file!", show_alert=True)
    except Exception as e:
        logger.error(f"Error extracting ZIP: {e}")
        await callback.answer(f"❌ Extraction failed: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("delete_file:"))
async def callback_delete_file(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    file_name = callback.data.split(":", 1)[1]
    
    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    file_path = user_folder / file_name
    
    try:
        if file_path.exists():
            file_path.unlink()
        
        if user_id in user_files:
            user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
        
        if file_name in user_favorites.get(user_id, []):
            user_favorites[user_id].remove(file_name)
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
        c.execute('DELETE FROM favorites WHERE user_id = ? AND file_name = ?', (user_id, file_name))
        conn.commit()
        conn.close()
        await _persist_now(user_id)
        
        await callback.answer("✅ File deleted successfully!", show_alert=True)
        await callback_check_files(callback)
        
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        await callback.answer(f"❌ Error: {str(e)}", show_alert=True)

@dp.callback_query(F.data == "admin_total_users")
async def callback_admin_total_users(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    user_list = "\n".join([f"• <code>{uid}</code>" for uid in list(active_users)[:15]])
    text = f"""
╔═══════════════════════╗
    👥 <b>USER STATISTICS</b> 👥
╚═══════════════════════╝

📊 <b>Total Users:</b> {len(active_users)}
🚫 <b>Banned:</b> {len(banned_users)}
✅ <b>Active:</b> {len(active_users) - len(banned_users)}

<b>📝 Recent Users (15):</b>
{user_list}

{'...' if len(active_users) > 15 else ''}
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_total_files")
async def callback_admin_total_files(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    total_files = sum(len(files) for files in user_files.values())
    py_files = sum(1 for files in user_files.values() for f in files if f[1] == 'py')
    js_files = sum(1 for files in user_files.values() for f in files if f[1] == 'js')
    zip_files = sum(1 for files in user_files.values() for f in files if f[1] == 'zip')
    
    text = f"""
╔═══════════════════════╗
    📁 <b>FILE STATISTICS</b> 📁
╚═══════════════════════╝

📊 <b>Total Files:</b> {total_files}

<b>📦 By Type:</b>
🐍 Python: {py_files}
🟨 JavaScript: {js_files}
📦 ZIP: {zip_files}

<b>📈 Top Users:</b>
"""
    
    top_users = sorted(user_files.items(), key=lambda x: len(x[1]), reverse=True)[:5]
    for user_id, files in top_users:
        text += f"• User <code>{user_id}</code>: {len(files)} files\n"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_running_scripts")
async def callback_admin_running_scripts(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    if not bot_scripts:
        text = """
╔═══════════════════════╗
    🚀 <b>RUNNING SCRIPTS</b> 🚀
╚═══════════════════════╝

💤 No scripts running currently
"""
        buttons = []
    else:
        text = f"""
╔═══════════════════════╗
    🚀 <b>RUNNING ({len(bot_scripts)})</b> 🚀
╚═══════════════════════╝

"""
        buttons = []
        for script_key, info in bot_scripts.items():
            runtime = (datetime.now() - info['start_time']).total_seconds()
            text += f"🔸 <code>{info['file_name']}</code>\n"
            text += f"   PID: {info['process'].pid} | User: {info['script_owner_id']}\n"
            text += f"   Runtime: {int(runtime)}s\n\n"
            buttons.append([InlineKeyboardButton(
                text=f"🛑 Stop {info['file_name'][:15]}", 
                callback_data=f"stop_script:{script_key}"
            )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")])
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_premium_users")
async def callback_admin_premium_users(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    premium_users = [(u, data) for u, data in user_subscriptions.items() if data['expiry'] > datetime.now()]
    
    if not premium_users:
        text = """
╔═══════════════════════╗
    💎 <b>PREMIUM USERS</b> 💎
╚═══════════════════════╝

No active premium subscriptions.
"""
    else:
        text = f"""
╔═══════════════════════╗
    💎 <b>PREMIUM ({len(premium_users)})</b> 💎
╚═══════════════════════╝

"""
        for user_id, data in premium_users:
            expiry_date = data['expiry'].strftime('%Y-%m-%d')
            text += f"💎 User <code>{user_id}</code>\n   Expires: {expiry_date}\n\n"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Premium", callback_data="add_premium")],
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_analytics")
async def callback_admin_analytics(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = f"""
╔═══════════════════════╗
    📊 <b>BOT ANALYTICS</b> 📊
╚═══════════════════════╝

<b>📈 GLOBAL STATS:</b>

📤 Total Uploads: {bot_stats.get('total_uploads', 0)}
📥 Total Downloads: {bot_stats.get('total_downloads', 0)}
▶️ Script Runs: {bot_stats.get('total_runs', 0)}
👥 Total Users: {len(active_users)}
📁 Total Files: {sum(len(files) for files in user_files.values())}
🚀 Running Now: {len(bot_scripts)}
⭐ Total Favorites: {sum(len(favs) for favs in user_favorites.values())}

<b>💎 PREMIUM:</b>
Active: {len([u for u in user_subscriptions if user_subscriptions[u]['expiry'] > datetime.now()])}
Expired: {len([u for u in user_subscriptions if user_subscriptions[u]['expiry'] <= datetime.now()])}

<b>🛡️ SECURITY:</b>
Banned Users: {len(banned_users)}
Admins: {len(admin_ids)}
Bot Status: {'🔒 Locked' if bot_locked else '✅ Active'}
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_system_status")
async def callback_admin_system_status(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    cpu = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    text = f"""
╔═══════════════════════╗
    ⚙️ <b>SYSTEM STATUS</b> ⚙️
╚═══════════════════════╝

<b>💻 CPU:</b>
Usage: {cpu}%
{'🟢 Normal' if cpu < 70 else '🟡 High' if cpu < 90 else '🔴 Critical'}

<b>🧠 MEMORY:</b>
Used: {memory.percent}%
Free: {memory.available / (1024**3):.1f} GB
Total: {memory.total / (1024**3):.1f} GB

<b>💾 DISK:</b>
Used: {disk.percent}%
Free: {disk.free / (1024**3):.1f} GB
Total: {disk.total / (1024**3):.1f} GB

<b>🤖 BOT STATUS:</b>
Status: {'🔒 Locked' if bot_locked else '✅ Running'}
Scripts: {len(bot_scripts)} active
Uptime: ✅ Online
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_system_status")],
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_add_admin")
async def callback_admin_add_admin(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = """
╔═══════════════════════╗
    ➕ <b>ADD ADMIN</b> ➕
╚═══════════════════════╝

To add a new admin, use:
<code>/addadmin USER_ID</code>

<b>Example:</b>
<code>/addadmin 123456789</code>

The user will get full admin privileges!
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_remove_admin")
async def callback_admin_remove_admin(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = f"""
╔═══════════════════════╗
    ➖ <b>REMOVE ADMIN</b> ➖
╚═══════════════════════╝

<b>Current Admins ({len(admin_ids)}):</b>

"""
    
    for admin_id in admin_ids:
        text += f"👑 <code>{admin_id}</code>\n"
    
    text += "\n<b>To remove:</b>\n<code>/removeadmin USER_ID</code>"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_ban_user")
async def callback_admin_ban_user(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = f"""
╔═══════════════════════╗
    🚫 <b>BAN USER</b> 🚫
╚═══════════════════════╝

<b>Currently Banned:</b> {len(banned_users)} users

To ban a user, use:
<code>/ban USER_ID REASON</code>

<b>Example:</b>
<code>/ban 123456789 Spam</code>

Banned users cannot use the bot!
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_unban_user")
async def callback_admin_unban_user(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = f"""
╔═══════════════════════╗
    ✅ <b>UNBAN USER</b> ✅
╚═══════════════════════╝

<b>Banned Users:</b> {len(banned_users)}

"""
    
    if banned_users:
        text += "<b>List:</b>\n"
        for ban_id in list(banned_users)[:10]:
            text += f"🚫 <code>{ban_id}</code>\n"
    
    text += "\n<b>To unban:</b>\n<code>/unban USER_ID</code>"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "lock_bot")
async def callback_lock_bot(callback: types.CallbackQuery):
    global bot_locked
    
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    bot_locked = not bot_locked
    status = "🔒 LOCKED" if bot_locked else "🔓 UNLOCKED"
    
    await callback.answer(f"Bot is now {status}!", show_alert=True)
    await callback_admin_panel(callback)

@dp.callback_query(F.data == "broadcast")
async def callback_broadcast(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = f"""
╔═══════════════════════╗
    📢 <b>BROADCAST</b> 📢
╚═══════════════════════╝

Send a message to all users!

<b>Total Recipients:</b> {len(active_users)}

<b>Command:</b>
<code>/broadcast Your message here</code>

⚠️ Use this feature responsibly!
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "add_premium")
async def callback_add_premium(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = """
╔═══════════════════════╗
    💎 <b>ADD PREMIUM</b> 💎
╚═══════════════════════╝

Give premium access to users!

<b>Command:</b>
<code>/addpremium USER_ID DAYS</code>

<b>Examples:</b>
<code>/addpremium 123456789 30</code> (30 days)
<code>/addpremium 987654321 7</code> (7 days)

Premium benefits:
• 50 file limit (vs 20)
• Priority support
• Premium badge
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_clean_files")
async def callback_admin_clean_files(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = """
╔═══════════════════════╗
    🗑️ <b>CLEAN FILES</b> 🗑️
╚═══════════════════════╝

Clean old or unused files from the system.

<b>Options:</b>
• Delete files older than 30 days
• Remove files from banned users
• Clean temp/log files

<b>Command:</b>
<code>/clean OPTION</code>

⚠️ This action cannot be undone!
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_backup_db")
async def callback_admin_backup_db(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    try:
        backup_path = IROTECH_DIR / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        conn = sqlite3.connect(DATABASE_PATH)
        backup_conn = sqlite3.connect(backup_path)
        conn.backup(backup_conn)
        backup_conn.close()
        conn.close()
        
        await callback.answer("✅ Database backed up!", show_alert=True)
        
        await callback.message.answer_document(
            FSInputFile(backup_path),
            caption="💾 <b>Database Backup</b>\n\nCreated: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            parse_mode="HTML"
        )
        
        backup_path.unlink()
        
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await callback.answer(f"❌ Backup failed: {str(e)}", show_alert=True)

@dp.callback_query(F.data == "admin_view_logs")
async def callback_admin_view_logs(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    text = """
╔═══════════════════════╗
    📝 <b>SYSTEM LOGS</b> 📝
╚═══════════════════════╝

View bot logs and activity.

<b>Available Logs:</b>
• Error logs
• User activity
• Script executions
• Admin actions

Logs are stored in the system directory.
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_restart_bot")
async def callback_admin_restart_bot(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Owner only!", show_alert=True)
        return
    
    text = """
╔═══════════════════════╗
    🔄 <b>RESTART BOT</b> 🔄
╚═══════════════════════╝

⚠️ <b>WARNING:</b>
This will restart the entire bot!

All running scripts will be stopped.
Users may experience brief downtime.

<b>Only use if necessary!</b>

Use <code>/restart</code> to confirm.
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    
    await callback.message.edit_text(text, reply_markup=back_keyboard, parse_mode="HTML")
    await callback.answer()

@dp.message(Command("addadmin"))
async def cmd_add_admin(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
    
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /addadmin USER_ID")
            return
        
        new_admin_id = int(args[1])
        
        if new_admin_id in admin_ids:
            await message.answer(f"✅ User {new_admin_id} is already an admin!")
            return
        
        admin_ids.add(new_admin_id)
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (new_admin_id,))
        conn.commit()
        conn.close()
        
        await message.answer(f"✅ User <code>{new_admin_id}</code> added as admin!", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error adding admin: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("removeadmin"))
async def cmd_remove_admin(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.answer("❌ Only owner can remove admins!")
        return
    
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /removeadmin USER_ID")
            return
        
        remove_admin_id = int(args[1])
        
        if remove_admin_id == OWNER_ID:
            await message.answer("❌ Cannot remove owner!")
            return
        
        if remove_admin_id not in admin_ids:
            await message.answer(f"❌ User {remove_admin_id} is not an admin!")
            return
        
        admin_ids.remove(remove_admin_id)
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM admins WHERE user_id = ?', (remove_admin_id,))
        conn.commit()
        conn.close()
        
        await message.answer(f"✅ User <code>{remove_admin_id}</code> removed from admins!", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error removing admin: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("addpremium"))
async def cmd_add_premium(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
    
    try:
        args = message.text.split()
        if len(args) != 3:
            await message.answer("Usage: /addpremium USER_ID DAYS")
            return
        
        user_id = int(args[1])
        days = int(args[2])
        
        if days <= 0:
            await message.answer("❌ Days must be greater than 0!")
            return
        
        expiry = datetime.now() + timedelta(days=days)
        user_subscriptions[user_id] = {'expiry': expiry}
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)',
                  (user_id, expiry.isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(
            f"✅ <b>Premium Added!</b>\n\n"
            f"User: <code>{user_id}</code>\n"
            f"Duration: {days} days\n"
            f"Expires: {expiry.strftime('%Y-%m-%d %H:%M')}",
            parse_mode="HTML"
        )
        
    except ValueError:
        await message.answer("❌ Invalid input!")
    except Exception as e:
        logger.error(f"Error adding premium: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("ban"))
async def cmd_ban_user(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
    
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 2:
            await message.answer("Usage: /ban USER_ID [REASON]")
            return
        
        ban_user_id = int(args[1])
        reason = args[2] if len(args) > 2 else "No reason provided"
        
        if ban_user_id in admin_ids:
            await message.answer("❌ Cannot ban an admin!")
            return
        
        banned_users.add(ban_user_id)
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO banned_users (user_id, banned_date, reason) VALUES (?, ?, ?)',
                  (ban_user_id, datetime.now().isoformat(), reason))
        conn.commit()
        conn.close()
        
        await message.answer(f"🚫 User <code>{ban_user_id}</code> has been banned!\n\nReason: {reason}", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("unban"))
async def cmd_unban_user(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
    
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /unban USER_ID")
            return
        
        unban_user_id = int(args[1])
        
        if unban_user_id not in banned_users:
            await message.answer(f"❌ User {unban_user_id} is not banned!")
            return
        
        banned_users.remove(unban_user_id)
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM banned_users WHERE user_id = ?', (unban_user_id,))
        conn.commit()
        conn.close()
        
        await message.answer(f"✅ User <code>{unban_user_id}</code> has been unbanned!", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
    
    try:
        broadcast_text = message.text.replace("/broadcast", "", 1).strip()
        
        if not broadcast_text:
            await message.answer("Usage: /broadcast Your message here")
            return
        
        sent_count = 0
        failed_count = 0
        
        status_msg = await message.answer(f"📢 Broadcasting to {len(active_users)} users...")
        
        for user_id in active_users:
            if user_id in banned_users:
                continue
            
            try:
                await bot.send_message(user_id, f"📢 <b>Announcement:</b>\n\n{broadcast_text}", parse_mode="HTML")
                sent_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to {user_id}: {e}")
                failed_count += 1
        
        await status_msg.edit_text(
            f"✅ <b>Broadcast Complete!</b>\n\n"
            f"✅ Sent: {sent_count}\n"
            f"❌ Failed: {failed_count}",
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Error broadcasting: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("search"))
async def cmd_search_files(message: types.Message):
    user_id = message.from_user.id
    
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("Usage: /search filename")
            return
        
        search_term = args[1].lower()
        user_file_list = user_files.get(user_id, [])
        
        matches = [f for f in user_file_list if search_term in f[0].lower()]
        
        if not matches:
            await message.answer(f"🔍 No files found matching '<code>{search_term}</code>'", parse_mode="HTML")
            return
        
        text = f"🔍 <b>Search Results ({len(matches)}):</b>\n\n"
        
        for file_name, file_type in matches:
            icon = "🐍" if file_type == "py" else "🟨" if file_type == "js" else "📦"
            text += f"{icon} <code>{file_name}</code>\n"
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        await message.answer(f"❌ Error: {str(e)}")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    text = """
╔═══════════════════════╗
    ℹ️ <b>HELP & INFO</b> ℹ️
╚═══════════════════════╝

<b>🎯 HOW TO USE:</b>

1️⃣ <b>Upload Files:</b>
   • Click 'Upload File'
   • Send your .py, .js, or .zip file
   • File will be saved automatically

2️⃣ <b>Run Scripts:</b>
   • Go to 'My Files'
   • Click 'Run' on any file
   • Monitor script execution

3️⃣ <b>Manage Files:</b>
   • View all files in 'My Files'
   • Add to favorites with ⭐
   • Delete unwanted files

4️⃣ <b>Search:</b>
   • Use /search [filename]
   • Quick file lookup

━━━━━━━━━━━━━━━━━━━━
<b>💡 COMMANDS:</b>

/start - Start the bot
/help - Show this help
/search - Search files
/stats - Your statistics
/premium - Free info

<b>Need help? Contact owner! 💬</b>
"""
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Features", callback_data="all_features")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await message.answer(text, reply_markup=back_keyboard, parse_mode="HTML")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    user_id = message.from_user.id
    user_file_count = len(user_files.get(user_id, []))
    user_fav_count = len(user_favorites.get(user_id, []))
    is_premium = user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now()
    
    text = f"""
╔═══════════════════════╗
    📊 <b>YOUR STATISTICS</b> 📊
╚═══════════════════════╝

<b>👤 USER INFO:</b>

🆔 User ID: <code>{user_id}</code>
👤 Name: {message.from_user.full_name}
📦 Files Uploaded: {user_file_count} (Unlimited ♾️)
⭐ Favorites: {user_fav_count}
💎 Account: Free 🆓 (Unlimited ♾️)
🚀 Running: {sum(1 for k in bot_scripts if k.startswith(f"{user_id}_"))}

━━━━━━━━━━━━━━━━━━━━
📈 <b>USAGE:</b>

📤 Uploads: {bot_stats.get('total_uploads', 0)}
📥 Downloads: {bot_stats.get('total_downloads', 0)}
▶️ Script Runs: {bot_stats.get('total_runs', 0)}

{'✅ Bot Status: Active' if not bot_locked else '🔒 Bot: Maintenance'}
"""
    
    if user_id in admin_ids:
        text += f"\n━━━━━━━━━━━━━━━━━━━━\n👑 <b>ADMIN STATS:</b>\n"
        text += f"👥 Total Users: {len(active_users)}\n"
        text += f"📁 Total Files: {sum(len(files) for files in user_files.values())}\n"
    
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    
    await message.answer(text, reply_markup=back_keyboard, parse_mode="HTML")


# ── WEBSITE: LANDING PAGE + ADMIN PANEL ──────────────
import base64
import hmac
import html as html_lib

ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', '')
_BOT_USERNAME = None

def _auth_ok(request):
    if not ADMIN_PASSWORD:
        return True  # no password set, so the panel stays open
    h = request.headers.get('Authorization', '')
    if not h.startswith('Basic '):
        return False
    try:
        pwd = base64.b64decode(h[6:]).decode('utf-8').partition(':')[2]
    except Exception:
        return False
    return hmac.compare_digest(pwd.encode(), ADMIN_PASSWORD.encode())

def _deny():
    if not ADMIN_PASSWORD:
        return web.Response(text="Panel is disabled. Set ADMIN_PASSWORD in your Render environment.", status=503)
    return web.Response(status=401, headers={'WWW-Authenticate': 'Basic realm="Admin"'}, text="Login required")

def _running_list():
    out = []
    for key, info in list(bot_scripts.items()):
        try:
            if info['process'].poll() is None:
                out.append({'key': key, 'owner': info['script_owner_id'], 'file': info['file_name'],
                            'type': info['type'], 'since': info['start_time'].strftime('%d %b %H:%M')})
        except Exception:
            pass
    return out

def _collect():
    now = datetime.now()
    running = _running_list()
    users = []
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('SELECT user_id, join_date, last_active FROM active_users ORDER BY COALESCE(last_active, join_date) DESC LIMIT 300')
        for uid, jd, la in c.fetchall():
            users.append({'id': uid, 'joined': (jd or '-')[:16].replace('T', ' '), 'last': (la or '-')[:16].replace('T', ' '),
                          'files': len(user_files.get(uid, [])), 'banned': uid in banned_users, 'admin': uid in admin_ids,
                          'sub': uid in user_subscriptions and user_subscriptions[uid]['expiry'] > now,
                          'running': sum(1 for r in running if r['owner'] == uid)})
        conn.close()
    except Exception as e:
        logger.error(f"Panel user fetch error: {e}")
    extra = {'joins7': [], 'new_today': 0, 'active24': 0}
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        d6 = (now - timedelta(days=6)).strftime('%Y-%m-%d')
        c.execute("SELECT substr(join_date,1,10), COUNT(*) FROM active_users WHERE join_date >= ? GROUP BY 1", (d6,))
        cnt = dict(c.fetchall())
        days = [now - timedelta(days=i) for i in range(6, -1, -1)]
        extra['joins7'] = [[x.strftime('%d %b'), cnt.get(x.strftime('%Y-%m-%d'), 0)] for x in days]
        extra['new_today'] = cnt.get(now.strftime('%Y-%m-%d'), 0)
        c.execute("SELECT COUNT(*) FROM active_users WHERE last_active >= ?", ((now - timedelta(hours=24)).isoformat(),))
        extra['active24'] = c.fetchone()[0]
        conn.close()
    except Exception as e:
        logger.error(f"Panel stats error: {e}")
    types = {}
    for fl in user_files.values():
        for f in fl:
            types[f[1]] = types.get(f[1], 0) + 1
    top = sorted(((u, len(fl)) for u, fl in user_files.items()), key=lambda x: -x[1])[:5]
    try:
        ram = round(psutil.Process().memory_info().rss / 1024 / 1024, 1)
    except Exception:
        ram = 0
    up = now - BOT_START_TIME
    return {'total_users': len(active_users), 'banned': len(banned_users), 'admins': len(admin_ids),
            'total_files': sum(len(f) for f in user_files.values()), 'running_now': len(running),
            'uploads': bot_stats.get('total_uploads', 0), 'downloads': bot_stats.get('total_downloads', 0),
            'runs': bot_stats.get('total_runs', 0), 'locked': bot_locked,
            'uptime': f"{up.days}d {up.seconds // 3600}h {(up.seconds % 3600) // 60}m",
            'files_list': [{'owner': u, 'name': f[0], 'type': f[1], 'on': f'{u}_{f[0]}' in bot_scripts and bot_scripts[f'{u}_{f[0]}']['process'].poll() is None} for u, fl in user_files.items() for f in fl][:500], 'ram_mb': ram, 'cpu': psutil.cpu_percent(interval=None), 'running': running, 'users': users, 'types': types, 'top': top, **extra}

def _stop_script(key):
    info = bot_scripts.get(key)
    if not info:
        return False
    _note_stop(key)
    try:
        lf = info.get('log_file')
        if lf and not lf.closed:
            lf.close()
        parent = psutil.Process(info['process'].pid)
        for ch in parent.children(recursive=True):
            ch.terminate()
        parent.terminate()
    except Exception:
        pass
    bot_scripts.pop(key, None)
    return True

def _db(sql, params=()):
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute(sql, params)
    conn.commit()
    conn.close()

async def _broadcast(text):
    sent = 0
    for uid in list(active_users):
        if uid in banned_users:
            continue
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception:
            pass
        await asyncio.sleep(0.06)
    logger.info(f"Broadcast finished: {sent} users")

async def panel_action(request):
    global bot_locked
    if not _auth_ok(request):
        return _deny()
    if request.headers.get('X-Panel') != '1':
        return web.json_response({'ok': False, 'msg': 'Bad request'}, status=400)
    try:
        d = await request.json()
        act = d.get('action')
        uid = int(d['user_id']) if d.get('user_id') not in (None, '') else None
    except Exception:
        return web.json_response({'ok': False, 'msg': 'Invalid data'}, status=400)

    if act in ('ban', 'unban', 'stop_user') and uid is None:
        return web.json_response({'ok': False, 'msg': 'User ID required'})
    if act == 'ban':
        if uid in admin_ids:
            return web.json_response({'ok': False, 'msg': 'You cannot ban an admin'})
        banned_users.add(uid)
        _db('INSERT OR REPLACE INTO banned_users (user_id, banned_date, reason) VALUES (?, ?, ?)',
            (uid, datetime.now().isoformat(), 'Banned from web panel'))
        for r in _running_list():
            if r['owner'] == uid:
                _stop_script(r['key'])
        return web.json_response({'ok': True, 'msg': f'{uid} banned'})
    if act == 'unban':
        banned_users.discard(uid)
        _db('DELETE FROM banned_users WHERE user_id = ?', (uid,))
        return web.json_response({'ok': True, 'msg': f'{uid} unbanned'})
    if act == 'stop_user':
        n = sum(1 for r in _running_list() if r['owner'] == uid and _stop_script(r['key']))
        return web.json_response({'ok': True, 'msg': f'{n} bot(s) stopped'})
    if act == 'stop_script':
        return web.json_response({'ok': _stop_script(d.get('key')), 'msg': 'Script stopped'})
    if act == 'stop_all':
        n = sum(1 for r in _running_list() if _stop_script(r['key']))
        return web.json_response({'ok': True, 'msg': f'{n} bot(s) stopped'})
    if act == 'lock':
        bot_locked = not bot_locked
        return web.json_response({'ok': True, 'msg': 'Bot LOCKED' if bot_locked else 'Bot UNLOCKED'})
    if act == 'broadcast':
        text = (d.get('text') or '').strip()
        if not text or len(text) > 3500:
            return web.json_response({'ok': False, 'msg': 'Message is empty or too long'})
        asyncio.create_task(_broadcast(text))
        return web.json_response({'ok': True, 'msg': f'Broadcast started: {len(active_users)} users'})
    return web.json_response({'ok': False, 'msg': 'Unknown action'})

async def panel_api(request):
    if not _auth_ok(request):
        return _deny()
    return web.json_response(_collect())

ADMIN_HTML = """<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hosting Bot - Dashboard</title><style>
*{box-sizing:border-box}body{margin:0;font-family:system-ui,sans-serif;background:#0b0f17;color:#e6edf3;display:flex;min-height:100vh}
#side{width:220px;background:#111826;border-right:1px solid #1f2a3d;padding:16px 10px;position:fixed;top:0;bottom:0;left:0;z-index:20;transition:.2s}
.logo{font-weight:800;font-size:17px;padding:4px 10px 16px}.logo span{color:#58a6ff}
.nav{display:block;width:100%;text-align:left;background:none;border:0;color:#9aa7b5;padding:11px 12px;border-radius:8px;font-size:14px;cursor:pointer;margin-bottom:3px}
.nav:hover{background:#1a2438}.nav.on{background:#1f6feb;color:#fff}
.side-foot{position:absolute;bottom:14px;left:14px;font-size:11px;color:#6b7785}
#main{margin-left:220px;flex:1;padding:18px;min-width:0}
#top{display:none;align-items:center;gap:10px;margin-bottom:12px}
h2{margin:0 0 4px;font-size:20px}.sub{color:#8b949e;font-size:12px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(135px,1fr));gap:10px;margin-bottom:18px}
.card{background:#111826;border:1px solid #1f2a3d;border-radius:12px;padding:14px}.n{font-size:22px;font-weight:700}.l{font-size:12px;color:#8b949e;margin-top:3px}
.box{background:#111826;border:1px solid #1f2a3d;border-radius:12px;padding:14px;margin-bottom:14px}
.wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:8px;border-bottom:1px solid #1f2a3d;white-space:nowrap}th{color:#8b949e;font-weight:600}
button.a{background:#21262d;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:6px 10px;font-size:12px;cursor:pointer}
.r{background:#da3633!important;border-color:#da3633!important}.g{background:#238636!important;border-color:#238636!important}.b{background:#1f6feb!important;border-color:#1f6feb!important}
input,textarea{background:#0b0f17;color:#e6edf3;border:1px solid #30363d;border-radius:8px;padding:9px;font-size:13px;width:100%}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}.row input{flex:1;min-width:120px}
.tag{padding:2px 8px;border-radius:20px;font-size:11px}.on{}.t-on{background:#12361f;color:#3fb950}.t-off{background:#2a2f38;color:#8b949e}
.two{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px}.bars{display:flex;align-items:flex-end;gap:8px;height:130px;margin-top:12px}
.bc{flex:1;text-align:center;display:flex;flex-direction:column;justify-content:flex-end;align-items:center}.bc i{font-style:normal;font-size:11px;color:#9aa7b5}.bc small{font-size:10px;color:#6b7785}
.bar{width:100%;background:linear-gradient(#58a6ff,#1f6feb);border-radius:5px 5px 0 0}.hb{display:flex;align-items:center;gap:8px;margin-top:8px;font-size:12px}.hb span{width:56px;overflow:hidden;text-overflow:ellipsis}
.hb div{flex:1;background:#1a2438;border-radius:4px;height:8px}.hb div div{background:#1f6feb;height:8px}.hb em{font-style:normal;color:#9aa7b5;width:28px;text-align:right}
#toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);padding:10px 18px;border-radius:8px;display:none;font-size:13px;z-index:99}
@media(max-width:760px){#side{left:-230px}#side.open{left:0}#main{margin-left:0}#top{display:flex}}
</style></head><body>
<div id="side"><div class="logo">🚀 Hosting <span>Bot</span></div>
<button class="nav on" data-p="over">📊 Overview</button><button class="nav" data-p="bots">🤖 Running Bots</button>
<button class="nav" data-p="files">📁 All Files</button><button class="nav" data-p="users">👥 Users</button>
<button class="nav" data-p="cast">📢 Broadcast</button><button class="nav" data-p="set">⚙️ Settings</button>
<a class="nav" href="/" style="text-decoration:none">🌐 Website</a><div class="side-foot">Hosting Bot</div></div>
<div id="main"><div id="top"><button class="a b" onclick="$('side').classList.toggle('open')">☰ Menu</button><b>Hosting Bot</b></div>
<h2 id="ttl"></h2><div class="sub" id="st"></div><div id="view"></div></div><div id="toast"></div>
<script>
const $=id=>document.getElementById(id);let D=null,P='over',Q='';
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(m,ok=true){const t=$('toast');t.textContent=m;t.style.background=ok?'#238636':'#da3633';t.style.display='block';setTimeout(()=>t.style.display='none',2600)}
async function act(b){try{const r=await fetch('/admin/action',{method:'POST',headers:{'Content-Type':'application/json','X-Panel':'1'},body:JSON.stringify(b)});const j=await r.json();toast(j.msg||'Done',j.ok);load()}catch(e){toast('Error',false)}}
document.querySelectorAll('button.nav').forEach(b=>b.onclick=()=>{P=b.dataset.p;document.querySelectorAll('button.nav').forEach(x=>x.classList.toggle('on',x==b));$('side').classList.remove('open');render()});
const card=(l,n)=>`<div class="card"><div class="n">${esc(n)}</div><div class="l">${l}</div></div>`;
function bots(){return `<div class="box wrap"><table><tr><th>User</th><th>File</th><th>Type</th><th>Start</th><th></th></tr>`+(D.running.map(r=>`<tr><td>${r.owner}</td><td>${esc(r.file)}</td><td>${esc(r.type)}</td><td>${esc(r.since)}</td><td><button class="a r" onclick='act({action:"stop_script",key:${JSON.stringify(r.key)}})'>Stop</button></td></tr>`).join('')||'<tr><td colspan=5>No bots running</td></tr>')+`</table></div>`}
function render(){if(!D)return;const T={over:'Overview',bots:'Running Bots',files:'All Files',users:'Users',cast:'Broadcast',set:'Settings'};$('ttl').textContent=T[P];
$('st').textContent=(D.locked?'🔒 Maintenance mode':'✅ Bot active')+' • uptime '+D.uptime+' • auto-refresh 15s';let h='';
if(P=='over'){const mx=Math.max(1,...D.joins7.map(x=>x[1])),tt=Math.max(1,...Object.values(D.types));
h=`<div class="grid">${card('👥 Total Members',D.total_users)}${card('🆕 New today',D.new_today)}${card('🔥 Active 24h',D.active24)}${card('🟢 Bots running',D.running_now)}${card('📁 Files',D.total_files)}${card('🚫 Banned',D.banned)}${card('👑 Admins',D.admins)}${card('📤 Uploads',D.uploads)}${card('📥 Downloads',D.downloads)}${card('▶️ Script runs',D.runs)}${card('🧠 RAM MB',D.ram_mb)}${card('⚙️ CPU %',D.cpu)}</div>
<div class="two"><div class="box"><b>📈 Naye members (7 din)</b><div class="bars">${D.joins7.map(x=>`<div class="bc"><i>${x[1]}</i><div class="bar" style="height:${8+x[1]/mx*80}px"></div><small>${x[0]}</small></div>`).join('')}</div></div>
<div class="box"><b>🗂 File types</b>${Object.entries(D.types).map(([k,v])=>`<div class="hb"><span>${esc(k)}</span><div><div style="width:${v/tt*100}%"></div></div><em>${v}</em></div>`).join('')||'<p class="sub">No files yet</p>'}
<b style="display:block;margin-top:14px">🏆 Top users (files)</b>${D.top.map(t=>`<div class="hb"><span>${t[0]}</span><div><div style="width:${t[1]/Math.max(1,D.top[0][1])*100}%"></div></div><em>${t[1]}</em></div>`).join('')||'<p class="sub">-</p>'}</div></div>
<h3>🤖 Currently running bots</h3>`+bots()}
if(P=='bots')h=bots();
if(P=='files')h=`<input placeholder="🔍 Search file or user ID..." value="${esc(Q)}" oninput="Q=this.value;render()" style="margin-bottom:10px"><div class="box wrap"><table><tr><th>User</th><th>File</th><th>Type</th><th>Status</th></tr>`+(D.files_list.filter(f=>(f.name+f.owner).toLowerCase().includes(Q.toLowerCase())).map(f=>`<tr><td>${f.owner}</td><td>${esc(f.name)}</td><td>${esc(f.type)}</td><td><span class="tag ${f.on?'t-on':'t-off'}">${f.on?'Running':'Stopped'}</span></td></tr>`).join('')||'<tr><td colspan=4>No files</td></tr>')+`</table></div>`;
if(P=='users')h=`<input placeholder="🔍 Search user ID..." value="${esc(Q)}" oninput="Q=this.value;render()" style="margin-bottom:10px"><div class="box wrap"><table><tr><th>User ID</th><th>Joined</th><th>Last active</th><th>Files</th><th>Run</th><th>Tags</th><th></th></tr>`+(D.users.filter(u=>String(u.id).includes(Q)).map(u=>`<tr><td>${u.id}</td><td>${esc(u.joined)}</td><td>${esc(u.last)}</td><td>${u.files}</td><td>${u.running}</td><td>${u.banned?'🚫':''}${u.admin?'👑':''}</td><td>${u.banned?`<button class="a g" onclick="act({action:'unban',user_id:${u.id}})">Unban</button>`:`<button class="a r" onclick="act({action:'ban',user_id:${u.id}})">Ban</button>`} <button class="a" onclick="act({action:'stop_user',user_id:${u.id}})">Stop bots</button></td></tr>`).join('')||'<tr><td colspan=7>No users</td></tr>')+`</table></div>`;
if(P=='cast')h=`<div class="box"><b>📢 Message all users</b><textarea id="bc" rows="5" placeholder="Write your message..." style="margin:10px 0"></textarea><button class="a b" onclick="cast()">Send to all users</button></div>`;
if(P=='set')h=`<div class="box"><b>Bot control</b><div class="row" style="margin-top:10px"><button class="a b" onclick="act({action:'lock'})">🔒 Lock / Unlock bot</button><button class="a r" onclick="if(confirm('Stop all bots?'))act({action:'stop_all'})">🛑 Stop ALL bots</button></div></div><div class="box"><b>Manual ban / unban</b><div class="row" style="margin-top:10px"><input id="uid" placeholder="User ID" inputmode="numeric"><button class="a r" onclick="m('ban')">Ban</button><button class="a g" onclick="m('unban')">Unban</button></div></div>`;
$('view').innerHTML=h;const i=document.querySelector('#view input[oninput]');if(i&&Q){i.focus();i.setSelectionRange(Q.length,Q.length)}}
function m(a){const u=$('uid').value.trim();if(!u)return toast('User ID daalo',false);act({action:a,user_id:u})}
function cast(){const t=$('bc').value.trim();if(!t)return toast('Write a message',false);if(confirm('Send to everyone?')){act({action:'broadcast',text:t});$('bc').value=''}}
async function load(){try{D=await (await fetch('/admin/api')).json();const a=document.activeElement;if(a&&(a.tagName=='TEXTAREA'||a.id=='uid'))return;render()}catch(e){$('st').textContent='Load error'}}
load();setInterval(load,15000);
</script></body></html>"""

async def admin_page(request):
    if not _auth_ok(request):
        return _deny()
    return web.Response(text=ADMIN_HTML, content_type='text/html')

LANDING_HTML = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hosting Bot - Free Telegram Bot Hosting</title><meta name="description" content="Host your Python and Node.js bots straight from Telegram. Unlimited uploads, no premium plans.">
<style>
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;font-family:system-ui,sans-serif;background:#070b12;color:#e6edf3;line-height:1.55}
nav{display:flex;justify-content:space-between;align-items:center;padding:14px 22px;position:sticky;top:0;background:#070b12ee;backdrop-filter:blur(8px);border-bottom:1px solid #151d2c;z-index:5}
nav b{font-size:18px}nav b span,h1 span,h2 span{color:#58a6ff}nav a{color:#9aa7b5;text-decoration:none;margin-left:16px;font-size:14px}
.hero{text-align:center;padding:70px 20px 40px;background:radial-gradient(circle at 50% 0,#14366a 0,#070b12 62%)}
.badge{display:inline-block;background:#12361f;color:#3fb950;border-radius:20px;padding:5px 14px;font-size:13px;margin-bottom:16px}
h1{font-size:40px;margin:0 0 12px;line-height:1.15}.hero p{color:#9aa7b5;max-width:560px;margin:0 auto 26px;font-size:17px}
.btn{display:inline-block;padding:13px 24px;border-radius:10px;text-decoration:none;font-weight:600;margin:5px;font-size:15px}.p{background:#1f6feb;color:#fff}.s{border:1px solid #2b374d;color:#e6edf3}
.stats{display:flex;justify-content:center;gap:12px;flex-wrap:wrap;padding:6px 20px 30px}.st{background:#0f1624;border:1px solid #1f2a3d;border-radius:14px;padding:16px 30px;text-align:center}.st b{font-size:26px;display:block}.st small{color:#8b949e}
section{max-width:900px;margin:0 auto;padding:36px 20px}h2{text-align:center;font-size:26px;margin:0 0 22px}
.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:14px}.f{background:#0f1624;border:1px solid #1f2a3d;border-radius:14px;padding:18px}.f h3{margin:0 0 6px;font-size:16px}.f p{margin:0;color:#9aa7b5;font-size:14px}
.step{display:flex;gap:14px;align-items:flex-start;background:#0f1624;border:1px solid #1f2a3d;border-radius:14px;padding:16px;margin-bottom:10px}.num{background:#1f6feb;border-radius:50%;min-width:34px;height:34px;display:flex;align-items:center;justify-content:center;font-weight:700}
details{background:#0f1624;border:1px solid #1f2a3d;border-radius:12px;padding:14px;margin-bottom:10px}summary{cursor:pointer;font-weight:600}details p{color:#9aa7b5;margin:10px 0 0}
.cta{text-align:center;background:linear-gradient(135deg,#12315f,#0f1624);border-radius:18px;padding:34px 20px}
footer{text-align:center;color:#6b7785;padding:28px 10px;font-size:13px;border-top:1px solid #151d2c}footer a{color:#58a6ff;text-decoration:none;margin:0 6px}
</style></head><body>
<nav><b>🚀 Hosting <span>Bot</span></b><div><a href="#features">Features</a><a href="#how">How it works</a><a href="#faq">FAQ</a></div></nav>
<div class="hero"><div class="badge">🆓 100% Free &bull; Unlimited Uploads</div>
<h1>Host Your Bots <span>24/7</span><br>Straight From Telegram</h1>
<p>Send a Python (.py), Node.js (.js) or ZIP file. Dependencies install automatically and your bot starts. No premium plans, no limits.</p>
<a class="btn p" href="__BOTLINK__">🚀 Start on Telegram</a>__UPDATES_BTN__</div>
<div class="stats"><div class="st"><b>__USERS__</b><small>Users</small></div><div class="st"><b>__RUNNING__</b><small>Bots running now</small></div><div class="st"><b>__FILES__</b><small>Files hosted</small></div></div>
<section id="features"><h2>Features</h2><div class="g">
<div class="f"><h3>📤 Easy Upload</h3><p>Upload .py, .js and .zip files directly from the Telegram chat.</p></div>
<div class="f"><h3>⚙️ Auto Install</h3><p>Required Python packages are installed automatically.</p></div>
<div class="f"><h3>▶️ Run &amp; Stop</h3><p>Start, stop and manage your scripts with bot buttons.</p></div>
<div class="f"><h3>📁 File Manager</h3><p>View, search, download and delete your files.</p></div>
<div class="f"><h3>⭐ Favorites</h3><p>Keep your important scripts in your favorites.</p></div>
<div class="f"><h3>🆓 Totally Free</h3><p>Unlimited uploads, same features for everyone.</p></div></div></section>
<section id="how"><h2>How it works</h2>
<div class="step"><div class="num">1</div><div><b>Open the bot</b><br><span style="color:#9aa7b5">Open the bot on Telegram and press /start.</span></div></div>
<div class="step"><div class="num">2</div><div><b>Upload your file</b><br><span style="color:#9aa7b5">Send your .py, .js or .zip file.</span></div></div>
<div class="step"><div class="num">3</div><div><b>Press Run</b><br><span style="color:#9aa7b5">Your bot starts, and you can stop it at any time.</span></div></div></section>
<section id="faq"><h2>FAQ</h2>
<details><summary>Is it really free?</summary><p>Yes. There are no premium plans and no upload limits.</p></details>
<details><summary>Which files can I run?</summary><p>Python (.py), Node.js (.js) and ZIP projects.</p></details>
<details><summary>What if I have a problem?</summary><p>Contact the owner on Telegram using the support link below.</p></details></section>
<section><div class="cta"><h2 style="margin-bottom:8px">Ready to host? 🚀</h2><p style="color:#9aa7b5;margin:0 0 16px">Open the bot on Telegram now.</p><a class="btn p" href="__BOTLINK__">Open Bot</a></div></section>
<footer>__FOOTER_LINKS__</footer></body></html>"""

async def landing_page(request):
    global _BOT_USERNAME
    if not _BOT_USERNAME:
        try:
            _BOT_USERNAME = (await bot.get_me()).username
        except Exception:
            _BOT_USERNAME = ''
    ch = _updates_url() or ''
    contact = _contact_url() or ''
    bot_link = f'https://t.me/{_BOT_USERNAME}' if _BOT_USERNAME else (ch or '#')
    updates_btn = f'<a class="btn s" href="{html_lib.escape(ch)}">📢 Updates Channel</a>' if ch else ''
    footer_links = []
    if ch:
        footer_links.append(f'<a href="{html_lib.escape(ch)}">Telegram Channel</a>')
    if contact:
        footer_links.append(f'<a href="{html_lib.escape(contact)}">Contact</a>')
    page = (LANDING_HTML.replace('__BOTLINK__', html_lib.escape(bot_link))
            .replace('__UPDATES_BTN__', updates_btn)
            .replace('__FOOTER_LINKS__', ' &bull; '.join(footer_links) or '&nbsp;')
            .replace('__USERS__', str(len(active_users))).replace('__RUNNING__', str(len(_running_list())))
            .replace('__FILES__', str(sum(len(f) for f in user_files.values()))))
    return web.Response(text=page, content_type='text/html')

# ── CUSTOMER TOOLS: .env / requirements / package.json, auto-run, terminal ─────
SPECIAL_NAMES = {'requirements.txt': 'requirements', 'package.json': 'package'}
BLOCKED_ENV_KEYS = {'PATH', 'HOME', 'SHELL', 'USER', 'PWD', 'PYTHONPATH', 'PYTHONHOME', 'PYTHONSTARTUP',
                    'LD_PRELOAD', 'LD_LIBRARY_PATH', 'NODE_OPTIONS', 'NODE_PATH'}
_ENV_KEY_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_REQ_LINE_RE = re.compile(
    r'^[A-Za-z0-9][A-Za-z0-9._\-]*(\[[A-Za-z0-9,_\-]+\])?\s*'
    r'(((==|>=|<=|~=|!=|<|>)\s*[A-Za-z0-9.*+!_\-]+)\s*,?\s*)*(;[^#]*)?(\s*#.*)?$')
_PKG_RE = re.compile(r'^(@[A-Za-z0-9._\-]+/)?[A-Za-z0-9][A-Za-z0-9._\-]*(\[[A-Za-z0-9,_\-]+\])?'
                     r'((==|>=|<=|~=|!=|<|>|@)[A-Za-z0-9.*+!_\-^~<>=]+)?$')
INSTALL_TIMEOUT = 600
TERM_TIMEOUT = 90
TERM_MAX_CHARS = 3500
TERM_COMMANDS = ('pip', 'python', 'node', 'npm', 'ls', 'cat', 'head', 'tail', 'pwd', 'mkdir', 'rm', 'mv', 'cp')
_term_busy = set()

def _special_kind(file_name):
    low = file_name.lower()
    if low == '.env' or low.endswith('.env'):
        return 'env'
    return SPECIAL_NAMES.get(low)

def _user_dotenv(user_id):
    """Variables from the customer's own .env file, ready to be given to their bot."""
    path = UPLOAD_BOTS_DIR / str(user_id) / '.env'
    out = {}
    try:
        if not path.is_file() or path.stat().st_size > 200_000:
            return out
        for k, v in list(dotenv_values(path).items())[:200]:
            if v is None or not _ENV_KEY_RE.match(k) or k.upper() in BLOCKED_ENV_KEYS:
                continue
            if TOKEN and TOKEN in v:      # never hand the host bot's token to a customer script
                continue
            out[k] = v
    except Exception as e:
        logger.error(f"Could not read .env of {user_id}: {e}")
    return out

def _validate_requirements(path):
    """Only plain package names with optional version pins are allowed (no URLs, paths or pip options)."""
    bad = []
    for n, line in enumerate(Path(path).read_text(errors='ignore').splitlines(), 1):
        t = line.strip()
        if t and not t.startswith('#') and not _REQ_LINE_RE.match(t):
            bad.append(f"line {n}: {t[:60]}")
    return bad

def _run_install(cmd, cwd, timeout=INSTALL_TIMEOUT):
    try:
        r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, timeout=timeout, env=_child_env())
        out = (r.stdout + "\n" + r.stderr).strip()
        return r.returncode == 0, out[-1200:]
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {timeout}s"
    except Exception as e:
        return False, str(e)

def _install_requirements(folder):
    folder = Path(folder)
    bad = _validate_requirements(folder / 'requirements.txt')
    if bad:
        return False, "Not allowed in requirements.txt (use plain package names, e.g. requests==2.32.3):\n" + "\n".join(bad[:8])
    return _run_install([sys.executable, '-m', 'pip', 'install', '--no-input', '--disable-pip-version-check',
                         '--upgrade', '--target', str(folder / '.packages'), '-r', str(folder / 'requirements.txt')], folder)

def _install_node(folder):
    return _run_install(['npm', 'install', '--no-audit', '--no-fund', '--omit=dev'], folder)

def _ensure_dependencies(folder):
    """After a redeploy the packages are gone: reinstall them from the customer's requirements/package.json."""
    folder = Path(folder)
    try:
        if (folder / 'requirements.txt').is_file() and not (folder / '.packages').is_dir():
            ok, out = _install_requirements(folder)
            if not ok:
                logger.warning(f"requirements install failed in {folder.name}: {out[-200:]}")
        if (folder / 'package.json').is_file() and not (folder / 'node_modules').is_dir():
            ok, out = _install_node(folder)
            if not ok:
                logger.warning(f"npm install failed in {folder.name}: {out[-200:]}")
    except Exception as e:
        logger.error(f"_ensure_dependencies failed: {e}")

async def _stop_for_restart(script_key):
    info = bot_scripts.pop(script_key, None)
    if info:
        lf = info.get('log_file')
        if lf and not lf.closed:
            lf.close()
        await asyncio.to_thread(_terminate_tree, info['process'].pid)

async def _restart_script(user_id, file_name):
    await _stop_for_restart(f"{user_id}_{file_name}")
    return await _start_script_headless(user_id, file_name)

async def _restart_user_scripts(user_id):
    keys = [k for k, i in list(bot_scripts.items()) if i.get('script_owner_id') == user_id]
    n = 0
    for k in keys:
        if await _restart_script(user_id, bot_scripts[k]['file_name']):
            n += 1
    return n

async def _auto_run_after_upload(message, user_id, file_name):
    """Start the file right after it was uploaded (re-uploading a running file replaces it)."""
    note = await message.answer(f"🚀 Starting <code>{html.escape(file_name)}</code> ...", parse_mode="HTML")
    ok = await _restart_script(user_id, file_name)
    key = f"{user_id}_{file_name}"
    if ok:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛑 Stop Script", callback_data=f"stop_script:{key}")],
            [InlineKeyboardButton(text="📋 View Output", callback_data=f"view_log:{file_name}")]])
        await note.edit_text(f"✅ <code>{html.escape(file_name)}</code> is running (PID {bot_scripts[key]['process'].pid}).\n"
                             f"Your files are saved in the cloud and it restarts itself after a host restart.",
                             reply_markup=keyboard, parse_mode="HTML")
        warn = customers.pop_error(user_id)
        if warn:
            await message.answer(warn)
    else:
        await note.edit_text(f"❌ Could not start <code>{html.escape(file_name)}</code>. Use ▶️ Run in 📁 My Files "
                             f"and check 📋 View Output.", parse_mode="HTML")

async def _handle_special_upload(message, document, kind):
    user_id = message.from_user.id
    if document.file_size and document.file_size > 1_000_000:
        await message.answer("❌ That file is too big (max 1 MB).")
        return
    folder = UPLOAD_BOTS_DIR / str(user_id)
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / {'env': '.env', 'requirements': 'requirements.txt', 'package': 'package.json'}[kind]
    try:
        await bot.download(document, destination=target)
        await _persist_now(user_id)
        if kind == 'env':
            n = len(_user_dotenv(user_id))
            restarted = await _restart_user_scripts(user_id)
            await message.answer(f"🔐 <b>.env saved</b> ({n} variable(s)). It is given to your bot when it starts."
                                 + (f"\n♻️ Restarted {restarted} running bot(s) to use it." if restarted else ""),
                                 parse_mode="HTML")
            return
        label = 'requirements.txt' if kind == 'requirements' else 'package.json'
        status = await message.answer(f"📦 <b>{label} saved.</b> Installing packages, this can take a minute ...",
                                      parse_mode="HTML")
        if kind == 'requirements':
            ok, out = await asyncio.to_thread(_install_requirements, folder)
        else:
            ok, out = await asyncio.to_thread(_install_node, folder)
        restarted = 0
        if ok:
            await _persist_now(user_id)
            restarted = await _restart_user_scripts(user_id)
        text = (f"✅ <b>Packages installed.</b>" + (f"\n♻️ Restarted {restarted} running bot(s)." if restarted else "")
                if ok else f"❌ <b>Install failed</b>\n<pre>{html.escape(out[-900:])}</pre>")
        await status.edit_text(text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"special upload failed: {e}", exc_info=True)
        await message.answer(f"❌ Failed: {html.escape(str(e))}", parse_mode="HTML")

TERMINAL_HELP = (
    "🖥️ <b>TERMINAL</b>\n\nRun commands inside your own folder with <code>/term COMMAND</code>\n\n"
    "<b>Examples</b>\n"
    "<code>/term pip install requests</code>\n"
    "<code>/term pip install -r requirements.txt</code>\n"
    "<code>/term npm install discord.js</code>\n"
    "<code>/term python bot.py</code>  (runs up to 90 s; use ▶️ / upload to keep a bot running)\n"
    "<code>/term ls</code> · <code>/term cat .env</code> · <code>/term tail bot.log</code>\n"
    "<code>/term rm old.py</code> · <code>/term mv a.py b.py</code> · <code>/term mkdir data</code>\n\n"
    "Allowed commands: " + ", ".join(TERM_COMMANDS) + ".\n"
    "Pipes, <code>&amp;&amp;</code> and other shell tricks are not supported. Packages you install are private to your bots."
)

def _term_path(folder, arg):
    p = (folder / arg).resolve()
    if p != folder and folder not in p.parents:
        raise ValueError(f"'{arg}' is outside your folder")
    return p

def _term_build(user_id, text):
    """Turn what the customer typed into a safe argv list (no shell). Raises ValueError with a friendly message."""
    folder = (UPLOAD_BOTS_DIR / str(user_id)).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    try:
        argv = shlex.split(text)
    except ValueError:
        raise ValueError("Could not read that command (check your quotes).")
    if not argv:
        raise ValueError("Type a command, e.g. /term ls")
    cmd, args = argv[0].lower(), argv[1:]
    if cmd == 'pwd':
        return folder, ['echo', f"/{user_id}"], False
    if cmd in ('pip', 'pip3'):
        sub = args[0] if args else ''
        if sub not in ('install', 'list', 'show', 'freeze'):
            raise ValueError("pip: only install, list, show and freeze are allowed.")
        out = [sys.executable, '-m', 'pip', sub, '--disable-pip-version-check']
        rest, i = args[1:], 0
        while i < len(rest):
            a = rest[i]
            if sub == 'install' and a in ('-U', '--upgrade', '-q', '--quiet', '--no-cache-dir', '--pre'):
                out.append(a)
            elif sub == 'install' and a in ('-r', '--requirement') and i + 1 < len(rest):
                req = _term_path(folder, rest[i + 1])
                if not req.is_file():
                    raise ValueError(f"{rest[i + 1]} not found")
                bad = _validate_requirements(req)
                if bad:
                    raise ValueError("Not allowed in requirements: " + "; ".join(bad[:3]))
                out += ['-r', str(req)]
                i += 1
            elif a.startswith('-'):
                raise ValueError(f"pip option {a} is not allowed")
            elif sub in ('install', 'show') and not _PKG_RE.match(a):
                raise ValueError(f"'{a}' is not a plain package name")
            else:
                out.append(a)
            i += 1
        if sub in ('install', 'list', 'freeze', 'show'):
            if sub == 'install':
                out += ['--no-input', '--upgrade', '--target', str(folder / '.packages')]
            elif sub in ('list', 'freeze') and (folder / '.packages').is_dir():
                out += ['--path', str(folder / '.packages')]
        return folder, out, sub == 'install'
    if cmd in ('python', 'python3', 'node'):
        if not args or not args[0].lower().endswith(('.py', '.js')) or (cmd.startswith('python') != args[0].lower().endswith('.py')):
            raise ValueError(f"Usage: /term {cmd} yourfile.{'js' if cmd == 'node' else 'py'}")
        script = _term_path(folder, args[0])
        if not script.is_file():
            raise ValueError(f"{args[0]} not found")
        base = ['node'] if cmd == 'node' else [sys.executable, '-u']
        return folder, base + [str(script)] + args[1:], False
    if cmd == 'npm':
        sub = args[0] if args else ''
        if sub not in ('install', 'i', 'ci', 'ls', 'list'):
            raise ValueError("npm: only install, ci and ls are allowed.")
        for a in args[1:]:
            if a not in ('--save', '--save-dev', '-D', '-S') and not _PKG_RE.match(a):
                raise ValueError(f"'{a}' is not a plain package name")
        return folder, ['npm', *args, '--no-audit', '--no-fund'], sub in ('install', 'i', 'ci')
    if cmd in ('ls', 'cat', 'head', 'tail', 'mkdir', 'rm', 'mv', 'cp'):
        out, paths = [cmd], []
        for a in args:
            if a.startswith('-'):
                if not re.fullmatch(r'-[A-Za-z0-9]{1,4}', a) or (cmd == 'ls' and 'R' in a):
                    raise ValueError(f"option {a} is not allowed")
                out.append(a)
            else:
                p = _term_path(folder, a)
                if cmd in ('rm', 'mv') and p == folder:
                    raise ValueError("You cannot do that to your whole folder")
                paths.append(str(p))
        if cmd in ('cat', 'head', 'tail', 'mkdir', 'rm', 'mv', 'cp') and not paths:
            raise ValueError(f"Usage: /term {cmd} FILE")
        if cmd in ('mv', 'cp') and len(paths) != 2:
            raise ValueError(f"Usage: /term {cmd} FROM TO")
        return folder, out + (paths or ([str(folder)] if cmd == 'ls' else [])), cmd in ('rm', 'mv', 'cp', 'mkdir')
    raise ValueError("Command not allowed. Allowed: " + ", ".join(TERM_COMMANDS))

async def _run_terminal(user_id, text):
    folder, argv, changes_files = _term_build(user_id, text)
    env = await asyncio.to_thread(_child_env, user_id)
    proc = await asyncio.create_subprocess_exec(*argv, cwd=str(folder), env=env,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        data, _ = await asyncio.wait_for(proc.communicate(), timeout=TERM_TIMEOUT)
        out = data.decode(errors='replace')
        status = "" if proc.returncode == 0 else f"\n[exit code {proc.returncode}]"
    except asyncio.TimeoutError:
        await asyncio.to_thread(_terminate_tree, proc.pid)
        out, status = "", f"\n⏱️ Stopped after {TERM_TIMEOUT}s. To keep a bot running, upload it (it starts automatically)."
    if changes_files:
        await _persist_now(user_id)
    out = out.strip()
    if len(out) > TERM_MAX_CHARS:
        out = "...\n" + out[-TERM_MAX_CHARS:]
    return (out or "(no output)") + status

@dp.callback_query(F.data == "terminal_help")
async def callback_terminal_help(callback: types.CallbackQuery):
    await callback.message.answer(TERMINAL_HELP, parse_mode="HTML")
    await callback.answer()

@dp.message(Command("term"))
async def cmd_term(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    if user_id in banned_users:
        return
    if bot_locked and user_id not in admin_ids:
        await message.answer("🔒 Bot is currently locked!")
        return
    if not command.args:
        await message.answer(TERMINAL_HELP, parse_mode="HTML")
        return
    if user_id in _term_busy:
        await message.answer("⏳ Your previous command is still running.")
        return
    _term_busy.add(user_id)
    try:
        status = await message.answer("⏳ Running ...")
        try:
            out = await _run_terminal(user_id, command.args)
        except ValueError as e:
            out = f"❌ {e}"
        except Exception as e:
            logger.error(f"terminal error: {e}", exc_info=True)
            out = f"❌ Error: {e}"
        await status.edit_text(f"<pre>{html.escape(out)}</pre>", parse_mode="HTML")
    finally:
        _term_busy.discard(user_id)

# ── CLOUD STORAGE: backups, database info, auto-restore, crash reports ─────────
async def _send_backup(uid, caption):
    """Send the customer a ZIP with their bot files + a JSON export of their database."""
    paths = []
    try:
        paths = await asyncio.to_thread(persist.build_backup, uid)
        for path, name in paths:
            await bot.send_document(uid, FSInputFile(path, filename=name), caption=caption)
        return True
    except Exception as e:
        logger.error(f"Backup for {uid} failed: {e}", exc_info=True)
        return False
    finally:
        for path, _name in paths:
            try:
                os.unlink(path)
            except OSError:
                pass

BACKUP_CAPTION = (
    "💾 Backup of your bot\n\n"
    "• bot_files/ = your bot files and data files\n"
    "• database_export.json / mysql_backup.sql / postgres_export.json = your database (if you used one)\n\n"
    "You can upload these to any host and keep running."
)

_DB_EXAMPLES = {
    'mongo': ("Variables: <code>MONGO_URI</code>, <code>MONGO_DB_NAME</code>",
              "import os\nfrom pymongo import MongoClient\n"
              "db = MongoClient(os.getenv('MONGO_URI'))[os.getenv('MONGO_DB_NAME')]\n"
              "db.wallets.update_one({'user': 1}, {'$inc': {'balance': 100}}, upsert=True)"),
    'mysql': ("Variables: <code>MYSQL_HOST</code>, <code>MYSQL_PORT</code>, <code>MYSQL_USER</code>, "
              "<code>MYSQL_PASSWORD</code>, <code>MYSQL_DATABASE</code> (or <code>MYSQL_URL</code>)",
              "import os, pymysql\n"
              "db = pymysql.connect(host=os.getenv('MYSQL_HOST'), port=int(os.getenv('MYSQL_PORT')),\n"
              "    user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),\n"
              "    database=os.getenv('MYSQL_DATABASE'), autocommit=True)"),
    'postgres': ("Variables: <code>POSTGRES_URL</code> (same as <code>DATABASE_URL</code>), or <code>PGHOST</code>, "
                 "<code>PGPORT</code>, <code>PGUSER</code>, <code>PGPASSWORD</code>, <code>PGDATABASE</code>",
                 "import os, psycopg2\n"
                 "db = psycopg2.connect(os.getenv('POSTGRES_URL'))\n"
                 "db.autocommit = True"),
}

def _db_info_text(user_id):
    if not customers.enabled:
        return ("🗄️ <b>My Database</b>\n\nA private database is not enabled on this host yet. "
                "Your files are still saved and restored automatically after restarts.")
    info = customers.info(user_id)
    if not info:
        return ("🗄️ <b>My Database</b>\n\nYour database is being prepared. Upload and start your bot, "
                "then open this page again.")
    eng = info['engine']
    vars_line, example = _DB_EXAMPLES[eng]
    return "\n".join([
        "🗄️ <b>My Database</b>", "",
        f"Engine: <b>{LABELS[eng]}</b> {EMOJI[eng]}",
        f"Your private database name: <code>{customers.db_name(user_id)}</code>", "",
        "It is given to your bot automatically as environment variables every time it starts. "
        "Use it for wallet balances, transactions, orders, users, anything your bot must remember.",
        "", vars_line, f"<pre>{example}</pre>",
        "Also set: <code>DB_ENGINE</code> and <code>DATABASE_URL</code>.", "",
        "Data survives restarts and redeploys. 💾 My Backup (or /backup) sends you your files and database any time."])

def _usage_text():
    lines = [f"{label}: {n}/{mx if mx else '∞'} customers" for label, n, mx in customers.usage()]
    return "\n".join(lines) or "No customer database servers configured."

def _db_keyboard(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💾 My Backup", callback_data="my_backup"),
        InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]])

@dp.callback_query(F.data == "my_database")
async def callback_my_database(callback: types.CallbackQuery):
    uid = callback.from_user.id
    await callback.message.answer(_db_info_text(uid), reply_markup=_db_keyboard(uid), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "my_backup")
async def callback_my_backup(callback: types.CallbackQuery):
    if callback.from_user.id in banned_users:
        await callback.answer("🚫 Banned", show_alert=True)
        return
    await callback.answer("⏳ Preparing your backup...")
    ok = await _send_backup(callback.from_user.id, BACKUP_CAPTION)
    if not ok:
        await callback.message.answer("❌ Could not create the backup right now. Please try again.")

@dp.message(Command("dbstats"))
async def cmd_dbstats(message: types.Message):
    if message.from_user.id not in admin_ids:
        return
    await message.answer("🗄️ <b>Customer database servers</b>\n\n" + _usage_text(), parse_mode="HTML")

@dp.message(Command("backup"))
async def cmd_backup(message: types.Message):
    if message.from_user.id in banned_users:
        return
    await message.answer("⏳ Preparing your backup...")
    if not await _send_backup(message.from_user.id, BACKUP_CAPTION):
        await message.answer("❌ Could not create the backup right now. Please try again.")

@dp.message(Command("mydb"))
async def cmd_mydb(message: types.Message):
    if message.from_user.id in banned_users:
        return
    uid = message.from_user.id
    await message.answer(_db_info_text(uid), reply_markup=_db_keyboard(uid), parse_mode="HTML")

async def _start_script_headless(user_id, file_name):
    """Start a customer's script without a button press (used to restore bots after a host restart)."""
    user_folder = UPLOAD_BOTS_DIR / str(user_id)
    file_path = user_folder / file_name
    script_key = f"{user_id}_{file_name}"
    ext = file_path.suffix.lower()
    if script_key in bot_scripts or script_key in _starting_scripts or ext not in ('.py', '.js') \
            or not file_path.exists():
        return False
    try:
        if TOKEN and TOKEN in file_path.read_text(errors='ignore'):
            return False
    except Exception:
        pass
    _stopping.discard(script_key)
    _starting_scripts.add(script_key)
    log_file = open(user_folder / f"{file_path.stem}.log", 'a')
    try:
        await asyncio.to_thread(_ensure_dependencies, user_folder)
        if ext == '.py':
            installed, failed = await asyncio.to_thread(install_missing_dependencies, file_path)
            if installed:
                log_file.write(f"[AUTO-INSTALLER] Installed: {', '.join(installed)}\n")
            if failed:
                log_file.write(f"[AUTO-INSTALLER] Failed to install: {', '.join(failed)}\n")
            log_file.flush()
        await asyncio.to_thread(_kill_orphans, file_path)
        cmd = [sys.executable, '-u', str(file_path)] if ext == '.py' else ['node', str(file_path)]
        child_env = await asyncio.to_thread(_child_env, user_id)
        process = subprocess.Popen(cmd, cwd=str(user_folder), stdout=log_file, stderr=log_file,
                                   env=child_env)
        bot_scripts[script_key] = {
            'process': process, 'file_name': file_name, 'script_owner_id': user_id,
            'start_time': datetime.now(), 'user_folder': str(user_folder), 'type': ext[1:],
            'log_file': log_file,
        }
        _bg(persist.mark_running, script_key, user_id, file_name)
        return True
    except Exception as e:
        logger.error(f"Auto-start of {script_key} failed: {e}")
        if not log_file.closed:
            log_file.close()
        return False
    finally:
        _starting_scripts.discard(script_key)

async def restore_running_scripts():
    """After a restart/redeploy: start again every bot that was running before."""
    await asyncio.sleep(5)
    saved = await asyncio.to_thread(persist.list_running)
    restored = 0
    for uid, file_name in saved:
        key = f"{uid}_{file_name}"
        if uid in banned_users:
            continue
        if not (UPLOAD_BOTS_DIR / str(uid) / file_name).exists():
            await asyncio.to_thread(persist.clear_running, key)
            continue
        if await _start_script_headless(uid, file_name):
            restored += 1
            try:
                await bot.send_message(uid, f"♻️ The host restarted. Your bot <code>{html.escape(file_name)}</code> "
                                            f"was started again automatically with all your files.", parse_mode="HTML")
            except Exception:
                pass
        await asyncio.sleep(2)
    if saved:
        logger.info(f"Auto-restored {restored}/{len(saved)} running bot(s)")

def _tail_log(user_folder, file_name, lines=12):
    try:
        text = (Path(user_folder) / f"{Path(file_name).stem}.log").read_text(errors='ignore')
        return "\n".join(text.strip().splitlines()[-lines:])[-1500:]
    except Exception:
        return ""

async def monitor_scripts():
    """Detect bots that crashed and send the customer their files + database so nothing is lost."""
    await asyncio.sleep(20)
    while True:
        try:
            for key, info in list(bot_scripts.items()):
                rc = info['process'].poll()
                if rc is None or key in _stopping:
                    continue
                bot_scripts.pop(key, None)
                lf = info.get('log_file')
                if lf and not lf.closed:
                    lf.close()
                _bg(persist.clear_running, key)
                uid, file_name = info['script_owner_id'], info['file_name']
                if rc == 0:
                    continue          # the script finished normally
                if time.time() - _last_backup.get(key, 0) < 600:
                    continue
                _last_backup[key] = time.time()
                tail = html.escape(_tail_log(info['user_folder'], file_name))
                try:
                    await bot.send_message(
                        uid,
                        f"⚠️ <b>Your bot stopped unexpectedly</b>\n\n📄 <code>{html.escape(file_name)}</code>\n"
                        f"Exit code: <code>{rc}</code>\n\n"
                        + (f"<b>Last output:</b>\n<pre>{tail}</pre>\n\n" if tail else "")
                        + "Sending you a backup of your files and database now. You can upload it anywhere and keep going.",
                        parse_mode="HTML")
                except Exception:
                    pass
                await _send_backup(uid, BACKUP_CAPTION)
        except Exception as e:
            logger.error(f"monitor_scripts error: {e}", exc_info=True)
        await asyncio.sleep(15)

async def persist_loop():
    """Every 45 seconds copy changed files and the database to MongoDB."""
    await asyncio.sleep(30)
    while True:
        try:
            await asyncio.to_thread(persist.flush)
        except Exception as e:
            logger.error(f"persist_loop error: {e}")
        await asyncio.sleep(45)

async def _notify_owner_storage_status():
    await asyncio.sleep(8)
    try:
        if not persist.enabled:
            text = ("⚠️ <b>Cloud storage is OFF</b>\n\nMONGO_URI is not set, so customer files and the database "
                    "are lost on every redeploy. Add MONGO_URI in Render → Environment.")
        elif not persist._restore_ok:
            text = ("⚠️ <b>Cloud storage could not connect</b>\n\nCheck MONGO_URI and that MongoDB Atlas "
                    "Network Access allows 0.0.0.0/0. Cloud saving is paused until the next restart.")
        else:
            st = persist.stats
            text = (f"☁️ <b>Cloud storage connected</b>\n\nRestored {st['files']} file(s) for {st['users']} user(s)"
                    f"{' and the bot database' if st['db_restored'] else ''}.\n"
                    f"Customer database pool: {customers.counts()['mongo']} MongoDB, {customers.counts()['mysql']} MySQL, {customers.counts()['postgres']} PostgreSQL server(s)")
        await bot.send_message(OWNER_ID, text, parse_mode="HTML")
    except Exception:
        pass

async def keep_alive():
    """Pings itself so the Render free service does not go to sleep"""
    url = os.getenv('RENDER_EXTERNAL_URL')
    if not url:
        return
    await asyncio.sleep(60)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url + '/health', timeout=aiohttp.ClientTimeout(total=20)) as r:
                    logger.info(f"Keep-alive ping: {r.status}")
        except Exception as e:
            logger.warning(f"Keep-alive failed: {e}")
        await asyncio.sleep(600)  # every 10 minutes

async def web_server():
    app = web.Application()
    
    async def health(request):
        return web.Response(text="ok")
    
    app.router.add_get('/', landing_page)
    app.router.add_get('/health', health)
    app.router.add_get('/admin', admin_page)
    app.router.add_get('/admin/api', panel_api)
    app.router.add_post('/admin/action', panel_action)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 5000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"🌐 Web server started on port {port}")

async def main():
    logger.info("🚀 Starting Advanced File Host Bot...")
    
    background = [
        asyncio.create_task(web_server()),
        asyncio.create_task(keep_alive()),
        asyncio.create_task(persist_loop()),
        asyncio.create_task(monitor_scripts()),
        asyncio.create_task(restore_running_scripts()),
        asyncio.create_task(_notify_owner_storage_status()),
    ]

        # Render sends SIGTERM before a redeploy: save everything to MongoDB first
    async def _graceful_shutdown():
        logger.info("Shutdown signal received - saving data to cloud storage...")
        await asyncio.to_thread(persist.flush)
        await dp.stop_polling()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: background.append(asyncio.create_task(_graceful_shutdown())))
        except (NotImplementedError, RuntimeError):
            pass

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
