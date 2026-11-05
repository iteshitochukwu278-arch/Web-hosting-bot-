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

## Multi-bot fleet management (Marvel)

The `/mybots` and `/addbot` commands add a separate fleet workflow alongside the legacy file-hosting workflow:

1. `/addbot` (or **Add bot** in `/mybots**) asks for a customer bot token.
2. The token is checked against Telegram's `getMe` API before a bot record is created.
3. Each verified bot receives its own 12-character ID, directory, encrypted token record, files, log, and process.
4. `/mybots` lets the owner upload/replace files, start/stop the selected bot, and view its logs.
5. Admins can use `/fleetadmin` to list bots and `/fleetadmin stop|disable|enable BOT_ID` to control them.

The default customer limit is five bots and can be changed with `MAX_BOTS_PER_USER`. Set a dedicated stable `FLEET_SECRET` of at least 32 random characters in Render. There is no fallback to `DB_SECRET` or `BOT_TOKEN`. Losing/changing this secret makes existing encrypted fleet tokens unreadable.

### Deployment and security caveats

- This fleet module keeps bots in separate folders and processes, but **it is not a security sandbox**. Do not expose it to untrusted users until customer workloads are isolated with containers or OS-level sandboxing, CPU/memory/PID quotas, network policy, and a dedicated non-root runtime.
- Current fleet files live on the application filesystem. Configure a persistent disk or extend cloud persistence to include `upload_bots/fleet` before relying on Render redeploys; ephemeral storage can be lost.
- Token verification and encryption are implemented; tokens are never displayed in the customer UI. Bot scripts receive their own token in `BOT_TOKEN` and `TELEGRAM_BOT_TOKEN` environment variables.
- Fleet bots have bounded crash recovery, automatic daily ZIP backups (7 retained by default), per-file version history, encrypted environment variables, resource usage reporting, and crash-loop notifications. Subprocess limits are best-effort and are not a security boundary.
- Existing legacy upload/run behavior remains available and has not been migrated into fleet records.

## Credential hygiene

The previous `.env.example` contained live-looking credentials. They have been removed from this archive. If any were real or previously deployed, rotate the host Telegram token with @BotFather and rotate database credentials/passwords immediately. Never commit `.env` or production secrets.


## Multi-bot fleet operations

Customer commands: `/mybots` to list bots and `/addbot` to verify/register a Telegram token.
Each bot has its own directory under `upload_bots/0/fleet/<bot-id>`, encrypted token metadata in
the host SQLite database, process, log, and controls. Uploaded files are limited to 25 MiB and
supported text/config/script formats. Replacing a file restarts only that bot if it was running.
The fleet data directory is included in MongoDB persistence because it lives below `upload_bots/0`.

Admin commands: `/fleetadmin` lists fleet bots; `/fleetadmin stop BOT_ID`,
`/fleetadmin disable BOT_ID`, and `/fleetadmin enable BOT_ID` manage a bot. The default per-user
limit is five and can be changed with `MAX_BOTS_PER_USER`.

## Important isolation and deployment notes

- Fleet execution defaults to `BOT_EXECUTION_MODE=docker`. A Docker-capable worker with the Docker CLI and access to a trusted Docker daemon is required. The Render blueprint is for the control bot/web service; Render's standard service does not provide a nested Docker daemon. Run customer bot workloads on a separate VPS/worker/orchestrator and configure the control plane to reach that worker. Do not expose the Docker socket or worker credentials to customers.
- `BOT_EXECUTION_MODE=subprocess` is refused unless `BOT_ALLOW_UNSAFE_SUBPROCESS=true` is explicitly set. It is only for trusted development: customer code runs as the host service user and can read host files/processes. Never enable it for public customer uploads.
- Docker containers receive only their own bot token and customer-defined environment variables. The host process environment is not inherited. `CUSTOMER_DB_*`, `UPDATE_CHANNEL`, host bot credentials, database credentials, and `FLEET_SECRET` are never passed to customer containers.
- Each container has memory, CPU, and PID cgroup limits, a read-only root filesystem, a per-bot writable bind mount, dropped capabilities, and `no-new-privileges`. Python `requirements.txt` packages are installed inside the bot container into `/tmp/customer-packages`, not into the host environment. Node bots use a Node image. Docker bridge networking permits outbound Telegram access; apply host firewall/network policy to limit egress where practical.
- Fleet files and ZIP backups are stored below `upload_bots/0/fleet`, which MongoDB persistence synchronizes. Plaintext Docker `runtime.env` files are excluded from cloud sync and recreated at startup. Configure persistent storage and test a redeploy/restore in staging. Logs are excluded from cloud sync.
- Set a dedicated stable `FLEET_SECRET` of at least 32 random characters. There is no fallback to `DB_SECRET` or `BOT_TOKEN`; changing or losing it makes existing encrypted customer tokens unreadable.
- Rotate any credentials previously committed or shared, keep `.env` out of Git, and use a non-root worker account. Container isolation reduces host exposure but does not prevent customer code from abusing outbound network access or its own bot token.

The authenticated `/admin/api` payload now includes `fleet_bots`. The authenticated `/admin/action`
endpoint also accepts `fleet_stop`, `fleet_disable`, `fleet_enable` (with `bot_id`),
`fleet_suspend`, `fleet_activate` (with `user_id`), and `fleet_limit` (with `user_id` and `limit`).
These use the same panel authentication and `X-Panel: 1` request guard as existing admin actions.


## Fleet file management, backups, and plans

From `/mybots` → Manage bot, customers can upload/replace files, browse and delete files, download a ZIP backup, inspect logs, and view process CPU/RAM usage. Replacing or deleting a file keeps a bounded version history; use `/botversions BOT_ID filename` and `/botrestore BOT_ID filename VERSION` to inspect and restore versions. Environment variables are stored encrypted and can be set with `/botenv BOT_ID KEY value`, listed by name in the bot menu, and removed with `/botenvdel BOT_ID KEY`. Values are never displayed in the UI.

Fleet backups run every 24 hours by default and retain 7 archives per bot. Configure `BOT_BACKUP_INTERVAL_SECONDS` (minimum 3600) and `BOT_BACKUP_RETENTION`. Fleet files and ZIP backups live below `upload_bots/0/fleet` and are included in MongoDB file synchronization. Runtime `runtime.env` files are explicitly excluded from cloud persistence because they contain plaintext bot tokens; they are regenerated on each start. Verify a restore/redeploy in staging before relying on backups. Crash-looping bots notify their owner after the bounded restart policy gives up.

Admins can assign a time-limited plan with `/fleetadmin plan USER_ID BOT_COUNT DAYS`; it updates the account's bot limit and expiry. `/fleetadmin limit USER_ID COUNT` changes only the limit. **Payment collection and automatic renewal are not connected**: integrate a payment provider and call the plan assignment only after verifying a signed payment webhook. Do not trust a client-side payment success redirect.

Security note: uploaded customer code can still exfiltrate data or abuse network access unless the worker is isolated. Use a dedicated Docker-capable worker host, non-root execution, restricted egress, persistent volumes, and monitoring. Do not expose the Docker daemon/socket to customers. The current subprocess mode is for trusted development only.
