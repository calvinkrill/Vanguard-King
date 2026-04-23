# How to Host Your Bot 24/7 on Render

To keep your bot running 24/7, you need to host it on a cloud server.

## 🚨 IMPORTANT: Data Persistence 🚨
This bot uses a local SQLite database (`attendance.db`) and now also writes a JSON snapshot backup (`attendance_snapshot.json`) beside it.
- **On Render's Free Tier**, the filesystem is **ephemeral**. This means **all attendance data will be deleted** every time the bot restarts or redeploys.
- **To save data permanently**, you must use a **Render Disk** (Paid Feature) or switch to an external database.

## Method 1: Automatic Deployment (Recommended for Persistence)
This method uses the included `render.yaml` file to set up a Persistent Disk automatically. **(Requires Render Paid Plan)**

1. **Fork/Clone this Repository** to your GitHub.
2. Go to [Render Dashboard](https://dashboard.render.com/).
3. Click **New +** -> **Blueprint**.
4. Connect your repository.
5. Render will automatically detect the `render.yaml` configuration.
6. Click **Apply**.
7. **Environment Variables**: Enter your `DISCORD_TOKEN` in the dashboard if prompted, or add it manually in the **Environment** tab after creation. Render blueprint placeholders created with `sync: false` do **not** include a value automatically.
8. If the token is missing, the service can stay online for health checks, but the bot will **not** connect to Discord until `DISCORD_TOKEN` is configured and the service is redeployed.
9. The included `render.yaml` already mounts `/data`, stores all bot state there, and enables `REQUIRE_PERSISTENT_STORAGE=1`, so deploys fail fast if persistence is misconfigured.

## Method 2: Free Tier (No Persistence)
If you only need the bot for testing and don't care about losing data on restart:

1. **Create a New Web Service** on Render.
2. Connect your GitHub repo.
3. **Settings**:
   - **Runtime**: Python 3
   - **Build Command**: `python3 -m pip install -r requirements-runtime.txt`
   - **Start Command**: `python3 bot.py`
4. **Environment Variables**:
   - `DISCORD_TOKEN`: Your bot token.
   - `PYTHON_VERSION`: `3.12.12`

## Prevent "Sleeping" (Free Tier Only)
Free servers sleep after 15 minutes. Use [UptimeRobot](https://uptimerobot.com/) to ping your Render URL every 5 minutes.


## How to Update/Redeploy
When you make changes to your code (like the recent timezone update):

1. **Save** your files.
2. **Push** your changes to GitHub.
3. **Render** will automatically detect the new code and start redeploying within a minute.
4. You can check the progress in the "Events" or "Logs" tab on your Render dashboard.

If it doesn't deploy automatically:
1. Go to your service on Render.
2. Click the **"Manual Deploy"** button.
3. Select **"Deploy latest commit"**.

## Railway + Cloudflare

This repository now includes `railway.json`, which tells Railway to start the bot with `python3 bot.py` and use `/healthz` for health checks. The HTTP server also exposes `/readyz`, so you can place the Railway app behind Cloudflare and still have a simple endpoint for checks.

1. Create a new Railway project from this repository. The included `nixpacks.toml` tells Railway to install `requirements-runtime.txt` before starting the bot.
2. Add `DISCORD_TOKEN` in Railway Variables.
3. For persistent SQLite storage, attach a Railway Volume and set `DB_FILE=/data/attendance.db`.
4. Set `DB_SNAPSHOT_FILE=/data/attendance_snapshot.json` and `BOT_DATA_DIR=/data` so all backup/state files are written to the same persistent volume.
5. Set `REQUIRE_PERSISTENT_STORAGE=1` so the bot refuses to start if Railway volume storage is missing.
6. After the first deployment succeeds, optionally add a Cloudflare-managed custom domain that points to the Railway service.
7. The built-in stdlib health server listens on `PORT`, so Railway and Cloudflare can both reach the app without extra changes.

> Note: Cloudflare is used here as the DNS/proxy layer in front of Railway. Cloudflare Workers/Pages are not suitable for running this always-on Discord bot process by themselves.


## Cloudflare Containers

This repo now includes a direct Cloudflare deployment path for Cloudflare's global network using **Cloudflare Containers**.

### Included files
- `Dockerfile`: Builds the Python bot as a `linux/amd64` image for Cloudflare Containers.
- `wrangler.jsonc`: Declares the Worker, container binding, Durable Object migration, and a 1-minute cron keepalive.
- `cloudflare-worker/index.js`: Proxies HTTP requests to the container, forwards the `DISCORD_TOKEN` Worker secret into the container environment, and pings `/healthz` every minute so the bot process stays available.

### Deploy steps
1. Install dependencies:
   ```bash
   npm install
   ```
2. Log in to Cloudflare:
   ```bash
   npx wrangler login
   ```
3. Store the bot token as a Worker secret:
   ```bash
   npx wrangler secret put DISCORD_TOKEN
   ```
4. Deploy the Worker + container:
   ```bash
   npm run cf:deploy
   ```
5. Verify rollout status:
   ```bash
   npx wrangler containers list
   npx wrangler containers images list
   ```

### Limitation
Cloudflare Containers do **not** offer persistent disk yet. That means the included SQLite database and JSON snapshot are suitable only for testing on Cloudflare unless you swap persistence to an external database or object-storage-backed design.
