# Dragon

A comprehensive Discord bot for managing attendance, user nicknames, and role-based access control. Designed for ease of use with automated reporting, professional designs, and persistent data storage.

## Features

### 📅 Advanced Attendance System
- **Time Window Mode**: Set specific hours (e.g., 8:00 AM - 5:00 PM) for attendance.
- **Time Zone Support**: Automatically uses **Philippines Time (UTC+8)** for all schedules.
- **Deadline Display**: Reports clearly show the submission deadline.
- **Role-Based Permissions**: Restrict `present` to specific roles (e.g., "Student" or "Staff") using `!setpermitrole`.
- **Automated Reports**:
  - Live-updating **Daily Attendance Report** embed in your chosen channel.
  - Shows Date, Time, Deadline, Status (OPEN/CLOSED), and Present/Absent/Excused lists.
- **Status Tracking**:
  - **Present**: Users marked present (automatically or manually).
  - **Absent**: Users who missed the window (auto-marked) or manually marked.
  - **Excused**: Users excused by admins with a reason.
- **Setup Confirmation**: Smart notification when all required configurations are complete.

### 🔔 Smart Notifications
- **Auto-Absent DMs**: Users who miss the attendance window are automatically marked Absent and receive a detailed Direct Message.
- **Status Alerts**:
  - **Present**: Users receive a gold-themed DM confirming their attendance.
  - **Excused**: Users receive a neutral white DM including the reason and time.
  - **Absent**: Users receive a red DM notification when marked absent (auto or manual).
- **Professional Design**: All DMs and Reports feature the server's icon, branded colors, and timestamps.

### 🌐 Web Dashboard
- **Built-in Dashboard**: Open `/dashboard` on the bot's health server to view live stats.
- **Live Metrics**: Shows connected guild count, custom command count, and attendance totals.
- **Status Visibility**: Displays the bot's current custom status.
- **Discord Shortcut**: Use the slash command `/dashboard` to get the website link for configuration.

### 🏆 Attendance Leaderboard
- **Per-Member Stats**: Tracks Present, Absent, and Excused counts in SQLite.
- **Leaderboard Command**: `!leaderboard` / `!attendance_leaderboard` shows:
  - A gold embed with server branding.
  - A table: `Rank | Member | Present / Absent / Excused`.
- **Daily Reset**: `!resetattendance` clears daily records **and resets all leaderboard counts back to 0** while keeping your config.

### 📌 Sticky Messages
- **Channel Stickies**: Use `!stick <text>` to keep one sticky message at the bottom of a channel.
- **No Silent Reset**: If a sticky already exists, `!stick` will not replace it; remove it first.
- **Non-Duplicating**: The sticky message is only recreated if it is deleted.
- **Restart/Redeploy Safe**: Sticky channel configuration is saved to persistent storage and restored after downtime/redeploys.
- **Smart Cleanup**: In sticky channels, plain-text messages are auto-deleted, but messages with images/photos are kept.
- **Remove Sticky**: Use `!unstick` or `!removestick` to disable the sticky for a channel.

### 💬 Custom Commands
- **Server-Specific Responses**: Admins can create reusable text commands like `!rules` or `!faq`.
- **Persistent Storage**: Custom commands are stored in SQLite and survive bot restarts.
- **Easy Management**:
  - `!addcommand <name> <response>` creates or updates a command.
  - `!removecommand <name>` deletes a command.
  - `!listcommands` shows every saved custom command for the server.

### 🔥 Chat Revival Tools
- **Revive Role Ping**: Set a role to ping for reviving engagement with `!reviveping @Role`.
- **Revive Channel Targeting**: Set a dedicated channel for revive messages with `!revivechannel #channel`.
- **Manual Trigger Options**:
  - `!revivechat` or `!revive` to send a revive ping on demand.
  - Plain text trigger: `revive chat` (case-insensitive).
- **Automatic Hourly Revive Ping**: Once configured, the bot can ping the revive role every hour.

### 🛡️ Moderation Utility
- **`!fakeban`**: Sends a realistic **Server Ban** embed for announcements/testing, without actually banning the user.

### 🎧 24/7 Lofi Voice Streaming
- **Always-On Voice Music**: Keeps a lofi stream running in a configured voice channel.
- **Auto-Reconnect Loop**: Infinite reconnect attempts with exponential backoff when Discord/network disconnects happen.
- **Auto-Restart Playback**: If the stream drops, playback is automatically restarted.
- **Quick Commands**:
  - `!join` — Join your current voice channel and start lofi.
  - `!play <song name>` — Start playback (joins automatically if needed) or add to queue.
  - `!queue` — Show now-playing + queued songs.
  - `!skip` — Skip to the next queued song.
  - `!pause` / `!resume` — Pause or resume playback.
  - `!volume <0-200>` — Set playback volume percent.
  - `!stop` — Stop playback and clear queue.
  - `!playlofi [url]` — Use the default stream or switch to a custom stream URL.
  - `!status` — Shows whether 24/7 lofi is enabled, connected, and playing.
  - `!leave` — Disconnect from voice and disable 24/7 lofi for the server.
- **Slash Command Support**: `/join`, `/playlofi [url]`, `/status`, and `/leave` are available alongside prefix commands.
- **Admin Setup Slash Command**: `/setup247music` for one-shot setup.


### 🎙️ discord.js Voice Playback Notes
If you are implementing voice features with **discord.js**, joining a channel alone is not enough to make the bot speak.

- Install and use `@discordjs/voice` for voice connections and playback.
- Create an audio player, load a resource, and subscribe the connection to the player.
- Ensure you actually provide audio (mp3/wav file, TTS, or stream URL).
- Verify bot permissions: **Connect** and **Speak**.
- Check that the bot is not server-muted/suppressed (especially in Stage channels).
- For encoded audio formats, install voice dependencies such as `ffmpeg-static` (and sometimes `@discordjs/opus`).
- Confirm the audio file path exists.
- Add logging for player errors and connection state for debugging.

Example:
```js
const {
  joinVoiceChannel,
  createAudioPlayer,
  createAudioResource,
  AudioPlayerStatus,
} = require('@discordjs/voice');

const connection = joinVoiceChannel({
  channelId: voiceChannel.id,
  guildId: guild.id,
  adapterCreator: guild.voiceAdapterCreator,
});

const player = createAudioPlayer();
const resource = createAudioResource('./voice.mp3');

player.play(resource);
connection.subscribe(player);

player.on(AudioPlayerStatus.Playing, () => {
  console.log('Bot is speaking now');
});

player.on('error', (error) => {
  console.error(error);
});

console.log(connection.state.status);
```

### 📝 Auto-Nickname
- **Role-Based Auto Nickname**: append a custom tag when a user receives a configured role.
- **Default Tag Support**: apply a fallback tag for users without any configured autonick role.
- **Multi-Server Isolation**: each server has its own role/tag mappings and default tag.
- **Membership Screening Support**: updates users after they pass Discord's membership screening (rules acceptance).
- **Restart/Re-Deploy Safe**: autonick mappings are persisted in SQLite on the writable data directory and reconciled on startup.


### 🧠 Brainrot Bot Concept (Meme Chaos Bot)
A Discord bot concept focused on loud, random, high-chaos meme energy via slash commands and automated trigger responses.

- **Core Personality**: no serious tone, random caps, intentionally broken grammar, overused meme phrases, and sudden nonsense replies.
- **Core Slash Commands**:
  - `/brainrot` → sends random chaotic one-liners (e.g., absurd fake announcements).
  - `/meme [cursed|normal|void]` → returns random meme text with an image style.
  - `/sound` → plays random meme audio stingers (vine boom, distorted laugh, etc.).
  - `/npc` → replies in robotic “NPC dialogue” style.
  - `/dripcheck @user` → nonsense roast or hype line.
  - `/mirror <message>` → repeats message in caps/corrupted/emojified format.
  - `/ratio` → fake X/Twitter-style roast response.
- **Auto Brainrot Features**:
  - Toggleable auto-chat mode that posts chaotic lines every X minutes.
  - Keyword triggers (`help` → `no.`, `hi` → `67`, `bro` → `BRO???`).
- **Chaos Controls**:
  - `/chaos level 1-10`
  - `/mute brainrot`
  - `/uncook server` (tries to restore normal mode, intentionally unreliable)



### 💾 Persistence
- **Database Storage**: SQLite database ensures data survives restarts.
- **Snapshot Backups**: The bot also writes `attendance_snapshot.json` beside the database so attendance history and custom commands can be restored if a fresh SQLite file is created on the same persistent disk.
- **Railway + Cloudflare Ready**: Includes Railway health checks, Cloudflare-friendly HTTP endpoints, and first-class Cloudflare Containers deployment files.
- **Render.com Ready**: Configured for easy deployment with persistent disk support.

---

## 🚀 Setup Guide

### 1. Installation
1. Clone the repository.
2. Install system dependency (required for voice playback):
   ```bash
   # Debian/Ubuntu
   sudo apt-get update && sudo apt-get install -y ffmpeg
   ```
3. Install Python dependencies:
   ```bash
   python3 -m pip install -r requirements-runtime.txt
   ```
4. Create a `.env` file with your bot token:
   ```
   DISCORD_TOKEN=your_token_here
   DASHBOARD_URL=https://your-dashboard.example
   ```
   Optional persistence overrides:
   ```
   DB_FILE=/data/attendance.db
   DB_SNAPSHOT_FILE=/data/attendance_snapshot.json
   BOT_DATA_DIR=/data
   BLIND_DATE_DATA_DIR=/data/blind_date_data
   LOFI_DATA_DIR=/data/lofi_data
   STICKY_STATE_FILE=/data/sticky_channels.json
   REQUIRE_PERSISTENT_STORAGE=1
   ```
5. Run the bot:
   ```bash
   python3 bot.py
   ```

If `DB_FILE` is not set, the bot now auto-detects common persistent volume environment paths (`RAILWAY_VOLUME_MOUNT_PATH`, `RENDER_DISK_PATH`, `PERSISTENT_VOLUME_DIR`), then falls back to `/data/attendance.db` when `/data` exists, and otherwise uses `data/attendance.db`.

To make settings survive redeploys, use a persistent disk/volume mounted at `/data` (or explicitly set `DB_FILE` + `DB_SNAPSHOT_FILE` to a persistent path). With persistent storage, your configured roles/channels/times remain until you run `!resetattendance`.

If you want to hard-fail startup when no persistent disk is mounted (recommended for production), set `REQUIRE_PERSISTENT_STORAGE=1`. With this enabled, the bot exits early instead of silently using ephemeral container storage.

### 2. Configuration (In Discord)
Run these commands in your server to set up the bot. **The bot will notify you when setup is complete!**

1.  **Reset (Optional)**: Start fresh if needed.
    ```
    !resetattendance
    ```
2.  **Set Time Window**:
    ```
    !settime 8:00am - 5:00pm
    ```
3.  **Assign Report Channel**:
    ```
    !assignchannel #attendance-reports
    ```
    *To disable reporting, use:* `!assignchannel remove`
4.  **Configure Roles**:
    ```
    !presentrole @Present
    !absentrole @Absent
    !excuserole @Excused
    ```
5.  **Set Permitted Role** (Who can use `present`?):
    ```
    !setpermitrole @Student
    ```
6.  **Set Present Channel** (Where they can say `present`?):
    ```
    !channelpresent #attendance-channel
    ```

🎉 **Once all steps are done, the bot will send a "Setup Complete" confirmation!**

After setup, use:

- `!attendance` to post or refresh the Daily Attendance Report.
- `present`, `absent`, or the attendance buttons in the attendance channel to update the report. Once a user is marked present, absent, or excused for the session, they cannot switch to another status until an admin resets them.
- Weekend behavior: attendance is closed on Saturdays/Sundays **except** NSTP check-ins using messages such as `present nstp` or `present for the subject nstp`.
- `!leaderboard` to show the gold Attendance Leaderboard.
- `!addcommand rules Be respectful and follow the server guide.` to create a reusable `!rules` command.
- `!reviveping @Role` and `!revivechannel #channel` to configure revive pings.
- `!revivechat` (or type `revive chat`) to send a revive ping.

---

## 📚 Command Reference

### Prefix User Commands
| Command | Description |
| :--- | :--- |
| `!help` | Show the full command help menu. |
| `!ping` | Check if the bot is online and responsive. |
| `!av [@member]` | Show a member profile card/avatar. |
| `av [@member]` | Same avatar command without the `!` prefix. |
| `present` / `absent` | Mark yourself as present or absent (requires Permitted Role & Active Window). Once submitted, you cannot switch to another status until an admin resets you. |
| `present nstp` | Weekend exception format for NSTP attendance marking. |
| `!nick <Name>` | Change your nickname (suffix added automatically). |
| `!nicksettings` | View default autonick tag + role-tag mappings for this server. |
| `!attendance` | View the current attendance status. |
| `!attendance_leaderboard` / `!leaderboard` | Show attendance ranking totals. |
| `!join` | Join your current voice channel and start continuous lofi streaming. |
| `!play <song>` | Start playback (or queue if already playing). |
| `!queue` | Show now-playing and queued tracks. |
| `!skip` | Skip the active track and play the next queued song. |
| `!pause` / `!resume` | Pause or resume current playback. |
| `!volume <0-200>` | Set playback volume percentage. |
| `!stop` | Stop playback and clear the queue. |
| `!playlofi [url]` | Switch to the default/custom 24/7 lofi stream URL. |
| `!status` | Show 24/7 lofi connection/playback state and current bot status text. |
| `!leave` | Disconnect from voice and disable 24/7 lofi mode. |
| `!say <message>` | Make the bot send your message (slash equivalent available as `/say`). |
| `!uno`, `!owo`, `!mafia`, `!gartic`, `!casino`, `!mudae`, `!asterie`, `!hangman`, `!truthordare`, `!virtualfisher`, `!guessthenumber` | View your saved progress per game. |
| `!<game> set <progress>` | Save progress for any supported game command. |
| `!<game> clear` | Clear your saved progress for that game. |
| `!listcommands` | Show all custom commands configured for the server. |
| `!revivechat` / `!revive` | Trigger a revive ping in the configured revive channel (or fallback channel). |
| `!confess <message>` | Post an anonymous confession to the configured confession channel. |
| `revive chat` | Plain text trigger that sends a revive ping (case-insensitive). |
| `!stick <message>` | Create a sticky message for the channel. |
| `!unstick` / `!removestick` | Remove sticky behavior from the channel. |
| `!fakeban @User <reason>` | Send a fake-ban embed (for fun/testing only). |

### Prefix Admin / Staff Commands
| Command | Description |
| :--- | :--- |
| **Attendance** | |
| `!present @User` | Manually mark a user as present. |
| `!absent @User` | Mark a user as absent (Sends DM). |
| `!excuse @User <Reason>` | Excuse a user with a reason (Sends DM). |
| `!removepresent @User` | Reset a user's status. |
| `!removereport` | Instantly delete the current attendance report message. |
| `!leaderboard` | Show the attendance leaderboard (Present / Absent / Excused). |
| `!restartattendance` / `!resetattendance` | Full wipe of attendance session + leaderboard stats + settings reset. |
| **Configuration** | |
| `!settings` | Open interactive settings dashboard. |
| `!setupattendance ...` | One-shot setup command (also available as slash command). |
| `!setup_attendance` | Post attendance button UI manually. |
| `!addcommand <name> <response>` | Create or update a custom text command. |
| `!removecommand <name>` | Delete a custom text command. |
| `!reviveping @Role` | Set the role that gets pinged for revive messages. |
| `!reviveping` | Disable revive role pings. |
| `!revivechannel #channel` | Set the dedicated channel used for revive pings. |
| `!revivechannel` | Clear the dedicated revive channel and use fallback routing. |
| `!setupconfession #channel [#logchannel]` | Set the confession post channel and optional private confession log channel. |
| `!fakeban @User <reason>` | Send a realistic "Server Ban" embed (no real ban action). |
| `!status` | Show the bot's current custom status text. |
| `!status <text>` | Set bot status text (example: `!status i am sleeping`). The value is saved and restored after restart. |
| `!status clear` | Clear the bot's custom status. |
| `!settime <Start> - <End>` | Set daily attendance window (PH Time). |
| `!assignchannel #channel` | Set channel for live reports. |
| `!assignchannel remove` | Disable automatic attendance reporting. |
| `!setpermitrole @Role` | Set which role is allowed to use `!present`. |
| `!resetpermitrole` | Remove the permission restriction. |
| `!channelpresent #channel` | Restrict `present` messages to a single channel. |
| `!presentrole @Role` | Set the role given for Present status. |
| `!absentrole @Role` | Set the role given for Absent status. |
| `!excuserole @Role` | Set the role given for Excused status. |
| `!setnick @member <name\|remove>` | Admin nickname override command. |
| `!autonick @Role [Tag]` | Associate a nickname tag with a role (example: `!autonick @VIP [VIP]`). |
| `!defaultnick [Tag]` | Set the fallback tag for users without configured autonick roles. |
| `!removenick @Role` | Remove a role->tag autonick mapping. |
| `!updateall [@Role]` | Re-apply autonick to everyone, or only members with one role. |
| `!removeall @Role` | Remove that role's tag from all role members and delete its mapping. |
| `!stripall @Role [TagToRemove]` | Strip specific text from nicknames of all users with a role. |
| `!reset` | Alias-style reset command used by the bot's utility group. |

### Slash Commands
| Command | Description |
| :--- | :--- |
| `/setupattendance` | Guided one-shot attendance setup (channel/roles/time in one command). |
| `/setupnick` | Shows the autonick setup command list (`!autonick`, `!defaultnick`, `!removenick`, `!updateall`, `!removeall`, `!stripall`). |
| `/setup247music` | Configure voice channel + stream URL for always-on 24/7 lofi music. |
| `/setupconfession` | Configure confession channel and optional confession log channel. |
| `/confession` | Setup and save confession channel settings (creates missing channels/role). |
| `/confess` | Submit an anonymous confession message. |
| `/join` | Join your current voice channel and start 24/7 lofi. |
| `/playlofi [url]` | Set default/custom stream URL and begin streaming. |
| `/status` | Show lofi connection/playback status and current bot activity text. |
| `/leave` | Disconnect and disable lofi mode for the server. |
| `/mute @user [reason]` | Applies the `Muted` role, blocks chat, voice join/speak, and channel visibility/history. |
| `/unmute @user [reason]` | Removes the `Muted` role so the user can chat and join channels again. |
| `/lockunlockchannels [#channel]` | Set the channel used for `/lockallchannels` and `/unlockallchannels` announcements, or clear it by leaving the field empty. |
| `/say <message>` | Send a message as the bot response (example: `/say message: hi`). |
| `/pet adopt <type>` | Adopt a new virtual pet (dog, cat, dragon, fox, rabbit). |
| `/pet status` | Show pet stats: happiness, hunger, cleanliness, energy, bond, coins, and streak. |
| `/pet name <name>` | Rename your pet. |
| `/pet feed` `/pet play` `/pet clean` `/pet sleep` | Care actions with cooldowns that improve stats and bond level. |
| `/pet daily` | Claim daily coins + stat boost with streak rewards (7-day and 30-day milestones). |
| `/pet fetch` `/pet race` `/pet battle` | Optional mini-games for coins and extra bond growth. |
| `/pet shop` | View food, toys, and skin items in the pet shop. |
| `/dashboard` | Sends the website dashboard link so users can configure there. |

---

## ☁️ Deployment

### Railway + Cloudflare

This bot now includes a production-friendly HTTP health server that works well on [Railway](https://railway.com) and when a Railway app is placed behind [Cloudflare](https://www.cloudflare.com/) DNS/proxying for a custom domain.

1.  Create a new Railway project and deploy this repository. Railway will detect Python automatically, and `railway.json` configures `python3 bot.py` plus a `/healthz` health check.
2.  In Railway variables, set:
    - `DISCORD_TOKEN` = your Discord bot token
    - `DB_FILE` = `/data/attendance.db` if you attach a Railway volume, or leave it unset and the bot will auto-select `/data/attendance.db` when that volume exists
    - `DB_SNAPSHOT_FILE` = `/data/attendance_snapshot.json` if you want the JSON backup stored explicitly on the same volume
3.  If you need persistent attendance data, attach a Railway Volume and mount it at `/data`.
4.  (Optional) In Cloudflare DNS, point a custom domain or subdomain to your Railway hostname. The built-in `/`, `/healthz`, `/healthcheck`, `/network/healthcheck`, and `/readyz` endpoints return JSON and disable caching, which makes them safe for Cloudflare proxying and uptime checks.
5.  If Cloudflare proxying causes issues during first setup, temporarily switch the DNS record to **DNS only** until SSL finishes provisioning, then re-enable proxying if desired.

### Cloudflare Containers

This repository can now deploy directly to the [Cloudflare global network](https://developers.cloudflare.com/containers/) using **Cloudflare Containers**. The repo includes a `Dockerfile`, `wrangler.jsonc`, and a Worker entrypoint that proxies requests to a named container instance, injects `DISCORD_TOKEN` into the container runtime, and uses a one-minute Cron Trigger to keep the bot container warm enough to preserve the Discord connection.

1. Install Docker, Node.js, and npm locally. Cloudflare's official flow builds the container image with Docker during `wrangler deploy`.
2. Install JavaScript dependencies:
   ```bash
   npm install
   ```
3. Authenticate Wrangler:
   ```bash
   npx wrangler login
   ```
4. Add your Discord token as a Cloudflare Worker secret:
   ```bash
   npx wrangler secret put DISCORD_TOKEN
   ```
5. Deploy to Cloudflare:
   ```bash
   npm run cf:deploy
   ```
6. Check rollout status if needed:
   ```bash
   npx wrangler containers list
   npx wrangler containers images list
   ```

**Important persistence note:** Cloudflare Containers currently provide **ephemeral disk**, so the bundled SQLite database and JSON snapshot will reset whenever the container is replaced or restarted. For production attendance history on Cloudflare, move persistence to an external database or storage service before relying on long-term data retention.

### Render.com

This bot is configured for deployment on [Render](https://render.com).

1.  Fork this repository to your GitHub.
2.  Create a new **Web Service** on Render.
3.  Connect your GitHub repository.
4.  Render should automatically detect the `render.yaml` configuration.
    *   **Runtime**: Python 3
    *   **Build Command**: `python3 -m pip install -r requirements-runtime.txt`
    *   **Start Command**: `python3 bot.py`
5.  Add your `DISCORD_TOKEN` in the **Environment Variables** section of your Render service. If you created the service from `render.yaml`, note that the placeholder secret is declared with `sync: false`, so you still need to provide the real token in Render.
6.  Until `DISCORD_TOKEN` is set, the health-check web server can still respond, but the bot itself will not log in to Discord.
7.  Add a **Persistent Disk** mounted at `/data` so both `attendance.db` and `attendance_snapshot.json` survive restarts and redeploys.

### Vercel

Vercel is optimized for web apps and serverless APIs, not long-running processes. To run this Discord bot via Vercel you typically:

1. Host the bot code in a Git repository (GitHub/GitLab).
2. Create a small web/API entry on Vercel (for status page or simple HTTP endpoint).
3. Run the actual bot process on a worker/VM provider (Render, Railway, a VPS, etc.), and connect it to the same repository.

To update the bot after making code changes:

1. Commit and push your changes to the repository connected to Vercel.
2. Trigger a new deployment from the Vercel dashboard (or let it auto-deploy on push).
3. Ensure that `DISCORD_TOKEN` and any other environment variables are set in Vercel if you expose HTTP endpoints.

For always-on Discord presence, keep the Python process running on a platform that supports long-running background workers (e.g., Render) and use Vercel only for optional web-facing parts.
