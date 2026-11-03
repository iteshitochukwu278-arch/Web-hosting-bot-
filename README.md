# Web Hosting Bot (with cloud storage)

Telegram bot that hosts customers' `.py` / `.js` bots.

## What the cloud storage does
* **Nothing is lost on redeploy.** The bot database (users, premium, bans, file records) and every customer file
  (scripts, zips, data files their bot writes) are copied to MongoDB every ~45 s, right after each upload/delete,
  and again when Render stops the service. On start-up everything is restored.
* **Bots come back by themselves.** Bots that were running before a restart are started again automatically
  (the customer gets a message).
* **Database pool, round robin.** Add up to 20 free database servers (MongoDB, MySQL, PostgreSQL in any mix) as
  `CUSTOMER_DB_1`, `CUSTOMER_DB_2`, ... The engine is detected from the URL. The 1st customer who registers gets server 1,
  the 2nd gets server 2, and so on; after the last server it starts again at server 1. A customer stays on their server
  forever. Servers that are offline (or over `DB_MAX_PER_SERVER`, default 0 = unlimited) are skipped.
  Each customer's bot automatically receives its database as environment variables (`DATABASE_URL`, `DB_ENGINE`, plus
  `MONGO_URI`/`MONGO_DB_NAME`, `MYSQL_*` or `POSTGRES_URL`/`PG*`) - for wallets, transactions, orders, users, etc.
  * MySQL / PostgreSQL: every customer gets their **own database and own login** that can only access that database
    (the admin login you give needs CREATE USER / CREATEDB rights).
  * MongoDB: every customer gets their own database name (`cust_<telegram id>`). Note: Atlas free clusters cannot make
    per-database logins, so a Mongo customer's bot receives the cluster login. Use Mongo servers only if you trust
    your customers, or prefer MySQL/PostgreSQL servers for isolation.
  * Admins: `/dbstats` shows customers per server.
* **Automatic backup to the customer.** If a customer's bot crashes, the bot sends them a ZIP with their files
  + their database export (`database_export.json`, `mysql_backup.sql` or `postgres_export.json`). Customers can also
  press **💾 My Backup** or send `/backup`. **🗄️ My Database** (or `/mydb`) shows how to use their database.

## Customer tools
* **Upload and it runs.** A customer sends `bot.py` / `bot.js` and it starts automatically (re-uploading replaces the running copy).
* **`requirements.txt`** -> packages are installed into the customer's own private folder (never the host's), then their bots restart.
  Only plain names with version pins are accepted (no URLs, paths or pip options). After a redeploy they are reinstalled automatically.
* **`package.json`** -> `npm install` runs for the customer. **`.env`** -> its variables are given to the customer's bot
  (`PATH`, `PYTHONPATH`, `LD_*` etc. and the host's token are never passed on).
* **Terminal:** `/term COMMAND` (or the Terminal button). Allowed: pip (install/list/show/freeze), python, node, npm (install/ci/ls),
  ls, cat, head, tail, pwd, mkdir, rm, mv, cp - always limited to the customer's own folder, 90 s limit, no shell (no pipes / `&&`).

## Setup (10 minutes)
1. **Revoke the old bot token** in @BotFather (`/revoke`) if it was ever committed to GitHub, and use the new one.
2. Create a free cluster on https://cloud.mongodb.com (MongoDB Atlas).
   * Database Access -> add a user with a password (role: *Read and write to any database*).
   * Network Access -> *Add IP address* -> `0.0.0.0/0` (Render has no fixed IP).
   * Connect -> Drivers -> copy the `mongodb+srv://...` string.
3. Customer databases (optional) - free, no card: MongoDB Atlas (new cluster/project each), Neon (PostgreSQL),
   Supabase (PostgreSQL), Aiven (MySQL/PostgreSQL), TiDB Cloud (MySQL-compatible). Put each connection string in Render ->
   Environment as `CUSTOMER_DB_1`, `CUSTOMER_DB_2`, ... (see `.env.example`; never reuse `MONGO_URI`). Also set
   `DB_SECRET` = any long random text (never change it afterwards). SQL providers must let the admin login create
   databases/users; a service that gives you only one fixed database (e.g. some free MySQL hosts) will not work.
4. Render -> Environment: `MONGO_URI` = the main Atlas string (host data), `MONGO_DB_NAME` = `hostbot` (optional).
5. Push to GitHub; Render redeploys. The owner receives a Telegram message: *"Cloud storage connected"*.

## Notes / limits
* Atlas free tier = 512 MB per cluster. Files over `PERSIST_MAX_FILE_MB` (25 MB), `node_modules`, `__pycache__`,
  `.log` files are not copied to the cloud (Python packages are re-installed automatically on start).
* Without `MONGO_URI` the bot still works exactly as before, but everything is lost on redeploy (a warning is sent to the owner).
* If MongoDB is unreachable at start-up, cloud saving is paused for that run so existing cloud data is never overwritten.
