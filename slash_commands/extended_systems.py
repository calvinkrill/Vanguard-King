import datetime
import asyncio
import random
import re
import os
from typing import Iterable

import discord
from deep_translator import GoogleTranslator
from discord import app_commands

import database

BRAINROT_LINES = [
    "💀 I JUST DROPPED MY WIFI IN THE OVEN",
    "67 67 67 67 67 (DO NOT ASK)",
    "bro my cat just applied for CEO",
    "CHAT IS COOKED. SERVER TEMPERATURE: 9999°C",
    "I blinked and my keyboard started paying taxes",
]

MEME_LINES = {
    "cursed": [
        "this image stole my sleep schedule",
        "who downloaded my thoughts into a toaster",
        "this meme is banned in 14 dimensions",
    ],
    "normal": [
        "me pretending i understand the assignment",
        "POV: you said just one game",
        "average monday firmware update",
    ],
    "void": [
        "the void said brb",
        "404 braincell not found",
        "i stared into the abyss and it sent a sticker",
    ],
}

SOUND_STINGERS = [
    "🔊 *VINE BOOM*",
    "🔊 *distorted laugh.mp3*",
    "🔊 *oh no no no*",
    "🔊 *bass boosted explosion*",
]

NPC_LINES = [
    "Hello user. I am functioning normally. 67.",
    "Task: exist. Status: failed successfully.",
    "Greetings. Dialogue loop initialized.",
]

DRIP_ROASTS = [
    "BRO IS WEARING AIR FORCE -999",
    "drip so clean it reset my router",
    "fit goes hard but my GPU started crying",
]

RATIO_LINES = [
    "this got 0 aura + negative rizz 💀",
    "ratio'd by a microwave with opinions",
    "community notes: absolutely cooked",
]

CUTE_COMPLIMENTS = [
    "Certified adorable energy 💖",
    "Main character smile detected ✨",
    "Cute levels are legally dangerous 😳",
    "Serving soft vibes and charm 🌸",
]


def _parse_role_id(raw_value: str | None) -> int:
    if not raw_value:
        return 0
    value = str(raw_value).strip()
    if not value:
        return 0
    mention_match = re.fullmatch(r"<@&(\d+)>", value)
    if mention_match:
        value = mention_match.group(1)
    if value.isdigit():
        return int(value)
    return 0


def _agenda_config():
    return {
        "meeting_date": os.getenv("AGENDA_MEETING_DATE", "April 24, 2026"),
        "meeting_time": os.getenv("AGENDA_MEETING_TIME", "9:00 – 10:00 PM"),
        "meeting_location": os.getenv("AGENDA_MEETING_LOCATION", "Online Meeting Call"),
        "meeting_topics": os.getenv(
            "AGENDA_MEETING_TOPICS",
            "• Revamp of the Server\n"
            "• Hierarchy of Roles (Discord Roles)\n"
            "• Marketing for Invites\n"
            "• Server Management – Assign who will handle Aurielle and Onemsu",
        ),
        "role_id": _parse_role_id(os.getenv("AGENDA_ROLE_ID", "0")),
    }


class AgendaAttendanceView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="✔ I will attend",
        style=discord.ButtonStyle.success,
        custom_id="attend_meeting",
    )
    async def attend_meeting(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild:
            await interaction.response.send_message("This button only works in a server.", ephemeral=True)
            return

        meeting_role = discord.utils.get(interaction.guild.roles, name="Meeting")
        if meeting_role is None:
            try:
                meeting_role = await interaction.guild.create_role(
                    name="Meeting",
                    reason="Required attendance role for meeting attendees",
                )
            except discord.Forbidden:
                meeting_role = None
            except discord.HTTPException:
                meeting_role = None

        config = _agenda_config()
        inserted = database.add_meeting_attendance(
            interaction.guild.id,
            interaction.user.id,
            getattr(interaction.user, "tag", str(interaction.user)),
            config["meeting_date"],
            interaction.message.id if interaction.message else None,
        )
        if inserted:
            await interaction.response.send_message(
                "✅ You are marked as attending the meeting!",
                ephemeral=True,
            )
            return

        member = interaction.guild.get_member(interaction.user.id)
        role_note = ""
        if member and meeting_role and meeting_role not in member.roles:
            try:
                await member.add_roles(meeting_role, reason="Meeting attendance confirmation")
                role_note = f" You were also given the {meeting_role.mention} role."
            except discord.Forbidden:
                role_note = " I couldn't give you the Meeting role due to missing permissions."
            except discord.HTTPException:
                role_note = " I couldn't give you the Meeting role because Discord returned an error."

        await interaction.response.send_message(
            f"✅ You're already marked as attending this meeting.{role_note}",
            ephemeral=True,
        )


@app_commands.command(name="agenda", description="Post the meeting agenda")
async def agenda(interaction: discord.Interaction):
    cfg = _agenda_config()
    role_ping = f"<@&{cfg['role_id']}>" if cfg["role_id"] else ""
    desc = f"{role_ping} **New Meeting Announcement!**".strip()
    embed = discord.Embed(
        title="📋 Meeting Agenda",
        color=discord.Color.from_str("#2b2d31"),
        description=desc,
        timestamp=datetime.datetime.utcnow(),
    )
    embed.add_field(name="📅 Date", value=cfg["meeting_date"], inline=True)
    embed.add_field(name="⏰ Time", value=cfg["meeting_time"], inline=True)
    embed.add_field(name="📍 Location", value=cfg["meeting_location"], inline=True)
    embed.add_field(name="📌 Main Topics", value=cfg["meeting_topics"], inline=False)
    embed.add_field(
        name="🙏 Reminder",
        value="We hope everyone will attend. Thank you for your participation and patience!",
        inline=False,
    )

    await interaction.response.send_message(
        content=role_ping if role_ping else None,
        embed=embed,
        view=AgendaAttendanceView(),
        allowed_mentions=discord.AllowedMentions(roles=True),
    )


@app_commands.command(name="agendaedit", description="Edit agenda defaults used by /agenda")
@app_commands.default_permissions(manage_guild=True)
@app_commands.describe(
    meeting_date="Example: April 24, 2026",
    meeting_time="Example: 9:00 – 10:00 PM",
    meeting_location="Example: Online Meeting Call",
    meeting_topics="Use new lines or bullet points",
    role="Optional role to ping in /agenda",
)
async def agendaedit(
    interaction: discord.Interaction,
    meeting_date: str | None = None,
    meeting_time: str | None = None,
    meeting_location: str | None = None,
    meeting_topics: str | None = None,
    role: discord.Role | None = None,
):
    if not interaction.guild:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("You need Manage Server permission to use this.", ephemeral=True)
        return

    updates: list[str] = []
    if meeting_date:
        os.environ["AGENDA_MEETING_DATE"] = meeting_date
        updates.append(f"📅 Date: {meeting_date}")
    if meeting_time:
        os.environ["AGENDA_MEETING_TIME"] = meeting_time
        updates.append(f"⏰ Time: {meeting_time}")
    if meeting_location:
        os.environ["AGENDA_MEETING_LOCATION"] = meeting_location
        updates.append(f"📍 Location: {meeting_location}")
    if meeting_topics:
        os.environ["AGENDA_MEETING_TOPICS"] = meeting_topics
        updates.append("📌 Topics updated")
    if role:
        os.environ["AGENDA_ROLE_ID"] = str(role.id)
        updates.append(f"🔔 Role ping: {role.mention}")

    if not updates:
        await interaction.response.send_message(
            "No changes provided. Add at least one option to update agenda defaults.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        "✅ Agenda defaults updated for this running bot instance:\n" + "\n".join(updates),
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions(roles=True),
    )


@app_commands.command(name="donemeeting", description="Remove the Meeting role from yourself.")
async def donemeeting(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return

    meeting_role = discord.utils.get(interaction.guild.roles, name="Meeting")
    if meeting_role is None:
        await interaction.response.send_message("❌ The Meeting role does not exist in this server.", ephemeral=True)
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.response.send_message("❌ I couldn't find your member profile in this server.", ephemeral=True)
        return
    if meeting_role not in member.roles:
        await interaction.response.send_message("ℹ️ You don't currently have the Meeting role.", ephemeral=True)
        return

    try:
        await member.remove_roles(meeting_role, reason="User completed meeting attendance")
    except discord.Forbidden:
        await interaction.response.send_message(
            "❌ I can't remove the Meeting role due to missing permissions.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        await interaction.response.send_message(
            "❌ I couldn't remove the Meeting role because Discord returned an error.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message("✅ Removed the Meeting role from you.", ephemeral=True)


async def process_agenda_reminders(bot: discord.Client):
    """Optional auto-reminder; controlled by env vars and run every minute."""
    guild_id = int(os.getenv("AGENDA_REMINDER_GUILD_ID", "0") or 0)
    channel_id = int(os.getenv("AGENDA_REMINDER_CHANNEL_ID", "0") or 0)
    meeting_date = os.getenv("AGENDA_REMINDER_DATE", "")
    reminder_hour_utc = int(os.getenv("AGENDA_REMINDER_HOUR_UTC", "18") or 18)
    reminder_minute_utc = int(os.getenv("AGENDA_REMINDER_MINUTE_UTC", "0") or 0)

    if not guild_id or not channel_id or not meeting_date:
        return

    now = datetime.datetime.utcnow()
    if now.strftime("%Y-%m-%d") != meeting_date:
        return
    if now.hour != reminder_hour_utc or now.minute != reminder_minute_utc:
        return

    if database.has_meeting_reminder_sent(guild_id, meeting_date, "pre_meeting_reminder"):
        return

    channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
    if not channel:
        return

    await channel.send("📋 Reminder: Meeting starts at 9:00 PM tonight! Use /agenda for details.")
    database.mark_meeting_reminder_sent(guild_id, meeting_date, "pre_meeting_reminder")


def _require_guild(interaction: discord.Interaction) -> int | None:
    if not interaction.guild:
        return None
    return interaction.guild.id


def _today_month_day() -> str:
    now = datetime.datetime.utcnow().date()
    return now.strftime("%m-%d")


def _iso_now_utc() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"


@app_commands.command(name="brainrot", description="Drop a random chaotic brainrot line.")
async def brainrot(interaction: discord.Interaction):
    await interaction.response.send_message(random.choice(BRAINROT_LINES))


@app_commands.command(name="meme", description="Send a random meme-style brainrot line.")
@app_commands.describe(style="Choose meme style: cursed, normal, or void")
@app_commands.choices(style=[
    app_commands.Choice(name="cursed", value="cursed"),
    app_commands.Choice(name="normal", value="normal"),
    app_commands.Choice(name="void", value="void"),
])
async def meme(interaction: discord.Interaction, style: app_commands.Choice[str] | None = None):
    selected = style.value if style else "normal"
    line = random.choice(MEME_LINES[selected])
    await interaction.response.send_message(f"😂 [{selected}] {line}")


@app_commands.command(name="sound", description="Play a random brainrot sound stinger (text preview).")
async def sound(interaction: discord.Interaction):
    await interaction.response.send_message(random.choice(SOUND_STINGERS))


@app_commands.command(name="npc", description="Talk like an NPC for one message.")
async def npc(interaction: discord.Interaction):
    await interaction.response.send_message(random.choice(NPC_LINES))


@app_commands.command(name="dripcheck", description="Roast or hype a user's drip with chaos energy.")
@app_commands.describe(user="User to inspect")
async def dripcheck(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.send_message(f"{user.mention} {random.choice(DRIP_ROASTS)}")


@app_commands.command(name="cutecheck", description="Rate how cute someone is with a profile embed.")
@app_commands.describe(user="User to check (leave empty to check yourself)")
async def cutecheck(interaction: discord.Interaction, user: discord.Member | None = None):
    target = user or interaction.user
    score = random.randint(1, 100)
    verdict = "✅ Cute confirmed!" if score >= 50 else "❌ Not cute today (try again for better luck)."
    avatar_url = target.display_avatar.url if getattr(target, "display_avatar", None) else None

    embed = discord.Embed(
        title="💘 Cute Check",
        description=f"{target.mention} got **{score}/100** on the cute scale.",
        color=discord.Color.pink(),
    )
    embed.add_field(name="Result", value=verdict, inline=False)
    if score >= 50:
        embed.add_field(name="Vibe", value=random.choice(CUTE_COMPLIMENTS), inline=False)
    embed.set_footer(text=f"Requested by {interaction.user.display_name}")
    if avatar_url:
        embed.set_thumbnail(url=avatar_url)

    await interaction.response.send_message(embeds=[embed])


@app_commands.command(name="mirror", description="Mirror text with caps, glitch vibes, and emojis.")
@app_commands.describe(message="Message to mirror")
async def mirror(interaction: discord.Interaction, message: str):
    mirrored = message.upper().replace("A", "@").replace("E", "3").replace("O", "0")
    await interaction.response.send_message(f"🪞 {mirrored} 💀🔥")


@app_commands.command(name="ratio", description="Generate a fake X-style ratio roast.")
async def ratio(interaction: discord.Interaction):
    await interaction.response.send_message(random.choice(RATIO_LINES))


@app_commands.command(name="chaos", description="Set the fake chaos level for the current server.")
@app_commands.describe(level="Chaos level from 1 to 10")
async def chaos(interaction: discord.Interaction, level: app_commands.Range[int, 1, 10]):
    await interaction.response.send_message(f"⚙️ Chaos level set to **{level}/10**. Containment not guaranteed.")


@app_commands.command(name="mutebrainrot", description="Temporarily mute brainrot mode.")
async def mutebrainrot(interaction: discord.Interaction):
    await interaction.response.send_message("🧃 Brainrot muted. (for now)")


@app_commands.command(name="uncookserver", description="Attempt to normalize the server. Usually fails.")
async def uncookserver(interaction: discord.Interaction):
    await interaction.response.send_message("🧯 Running /uncook... failed successfully. server still cooked.")


fmbot_group = app_commands.Group(name="fmbot", description="Track listening stats and profile links.")


@fmbot_group.command(name="set", description="Link your FMBot username.")
@app_commands.describe(username="Your Last.fm/FMBot username")
async def fmbot_set(interaction: discord.Interaction, username: str):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    cleaned = username.strip()
    if not cleaned:
        await interaction.response.send_message("Please provide a valid username.", ephemeral=True)
        return
    database.upsert_fmbot_link(guild_id, interaction.user.id, cleaned)
    await interaction.response.send_message(f"✅ Linked your FMBot profile as **{cleaned}**.", ephemeral=True)


@fmbot_group.command(name="nowplaying", description="Show what you're listening to.")
async def fmbot_nowplaying(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    username = database.get_fmbot_link(guild_id, interaction.user.id)
    if not username:
        await interaction.response.send_message("Set your profile first with `/fmbot set <username>`.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"🎵 **{interaction.user.display_name}** (`{username}`) is now playing:\n"
        "`Unknown Track · Unknown Artist`\n"
        "(Connect a live Last.fm API integration to return real-time scrobbles.)"
    )


@fmbot_group.command(name="toptracks", description="Show your top tracks.")
async def fmbot_toptracks(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    username = database.get_fmbot_link(guild_id, interaction.user.id)
    if not username:
        await interaction.response.send_message("Set your profile first with `/fmbot set <username>`.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"📈 Top tracks for **{username}**\n"
        "1) Track A — 120 plays\n"
        "2) Track B — 98 plays\n"
        "3) Track C — 74 plays"
    )


@fmbot_group.command(name="topartists", description="Show your top artists.")
async def fmbot_topartists(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    username = database.get_fmbot_link(guild_id, interaction.user.id)
    if not username:
        await interaction.response.send_message("Set your profile first with `/fmbot set <username>`.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"🎤 Top artists for **{username}**\n"
        "1) Artist A — 402 plays\n"
        "2) Artist B — 300 plays\n"
        "3) Artist C — 210 plays"
    )


@fmbot_group.command(name="stats", description="Show your full listening profile.")
async def fmbot_stats(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    username = database.get_fmbot_link(guild_id, interaction.user.id)
    if not username:
        await interaction.response.send_message("Set your profile first with `/fmbot set <username>`.", ephemeral=True)
        return
    embed = discord.Embed(title="🎵 FMBot Listening Stats", color=discord.Color.purple())
    embed.add_field(name="Profile", value=username, inline=False)
    embed.add_field(name="Scrobbles", value="0 (placeholder)", inline=True)
    embed.add_field(name="Top Genre", value="Unknown", inline=True)
    embed.add_field(name="Most Played", value="Unknown Track", inline=False)
    await interaction.response.send_message(embed=embed)


asterie_group = app_commands.Group(name="asterie", description="AI/utility text tools.")


@asterie_group.command(name="ask", description="Get an AI-style chat response.")
@app_commands.describe(message="Your prompt")
async def asterie_ask(interaction: discord.Interaction, message: str):
    await interaction.response.send_message(
        "🤖 **Asterie**\n"
        f"You asked: {message}\n\n"
        "This is a lightweight local response. Replace with your AI provider integration for full chat quality."
    )


@asterie_group.command(name="summarize", description="Summarize text.")
@app_commands.describe(text="Text to summarize")
async def asterie_summarize(interaction: discord.Interaction, text: str):
    summary = text.strip()
    if len(summary) > 220:
        summary = summary[:220].rsplit(" ", 1)[0] + "..."
    await interaction.response.send_message(f"📝 Summary:\n{summary}")


@asterie_group.command(name="rewrite", description="Rewrite text with a chosen tone.")
@app_commands.describe(tone="e.g. professional, friendly, concise", text="Text to rewrite")
async def asterie_rewrite(interaction: discord.Interaction, tone: str, text: str):
    await interaction.response.send_message(f"✍️ Rewritten ({tone} tone):\n{text}")


@asterie_group.command(name="translate", description="Translate text to another language.")
@app_commands.describe(lang="Target language code, e.g. en, es, ja", text="Text to translate")
async def asterie_translate(interaction: discord.Interaction, lang: str, text: str):
    try:
        translated = GoogleTranslator(source="auto", target=lang).translate(text)
    except Exception:
        translated = "Translation failed. Check the language code and try again."
    await interaction.response.send_message(f"🌐 Translation ({lang}):\n{translated}")


@asterie_group.command(name="image", description="Generate an AI image prompt receipt.")
@app_commands.describe(prompt="Image prompt")
async def asterie_image(interaction: discord.Interaction, prompt: str):
    await interaction.response.send_message(
        "🎨 Image generation placeholder created.\n"
        f"Prompt: `{prompt}`\n"
        "Hook this command into your image API/provider to return actual renders."
    )


birthday_group = app_commands.Group(name="birthday", description="Manage member birthdays.")


@birthday_group.command(name="set", description="Set your birthday (YYYY-MM-DD).")
async def birthday_set(interaction: discord.Interaction, date: str):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    try:
        parsed = datetime.date.fromisoformat(date.strip())
    except ValueError:
        await interaction.response.send_message("Invalid format. Use `YYYY-MM-DD`.", ephemeral=True)
        return
    database.upsert_birthday(guild_id, interaction.user.id, parsed.isoformat())
    await interaction.response.send_message(f"🎂 Saved your birthday as **{parsed.isoformat()}**.", ephemeral=True)


@birthday_group.command(name="view", description="View your saved birthday.")
async def birthday_view(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    value = database.get_birthday(guild_id, interaction.user.id)
    if not value:
        await interaction.response.send_message("No birthday saved yet. Use `/birthday set`.", ephemeral=True)
        return
    await interaction.response.send_message(f"🎂 Your birthday: **{value}**", ephemeral=True)


@birthday_group.command(name="today", description="Show who's celebrating today.")
async def birthday_today(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    today_md = _today_month_day()
    rows = [r for r in database.list_birthdays_for_guild(guild_id) if r["birthday_date"][5:] == today_md]
    if not rows:
        await interaction.response.send_message("No birthdays today. 🎈")
        return
    mentions = "\n".join(f"• <@{r['user_id']}>" for r in rows)
    await interaction.response.send_message(f"🎉 Birthdays today:\n{mentions}")


@birthday_group.command(name="list", description="List birthdays for this server.")
async def birthday_list(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    rows = database.list_birthdays_for_guild(guild_id)
    if not rows:
        await interaction.response.send_message("No birthdays have been set yet.")
        return
    lines = [f"• <@{r['user_id']}> — {r['birthday_date']}" for r in rows[:30]]
    await interaction.response.send_message("🎂 Server Birthday List\n" + "\n".join(lines))


@birthday_group.command(name="remove", description="Remove your saved birthday.")
async def birthday_remove(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    removed = database.remove_birthday(guild_id, interaction.user.id)
    await interaction.response.send_message("✅ Birthday removed." if removed else "No birthday record found.", ephemeral=True)


@app_commands.command(name="setupbirthday", description="Set your birthday day/month and view the next birthday countdown.")
async def setupbirthday(interaction: discord.Interaction, day: app_commands.Range[int, 1, 31], month: app_commands.Range[int, 1, 12]):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    year = datetime.datetime.utcnow().year
    try:
        datetime.date(year, month, day)
    except ValueError:
        await interaction.response.send_message("❌ Invalid date. Please choose a real day/month combination.", ephemeral=True)
        return

    birthday_date = f"2000-{month:02d}-{day:02d}"
    database.upsert_birthday(guild_id, interaction.user.id, birthday_date)

    now = datetime.datetime.utcnow().date()
    next_birthday = datetime.date(now.year, month, day)
    if next_birthday < now:
        next_birthday = datetime.date(now.year + 1, month, day)

    embed = discord.Embed(
        title="🎂 Birthday Saved!",
        description=f"Your birthday is set to **{day:02d}/{month:02d}**",
        color=discord.Color.magenta(),
    )
    embed.add_field(
        name="⏳ Next Birthday",
        value=f"<t:{int(datetime.datetime(next_birthday.year, next_birthday.month, next_birthday.day).timestamp())}:R>",
        inline=False,
    )
    embed.set_footer(text="We will remind you on your special day 🎉")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@app_commands.command(name="setupbirthdaychannel", description="Set the channel used for automatic birthday announcements.")
@app_commands.checks.has_permissions(administrator=True)
async def setupbirthdaychannel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    database.upsert_birthday_channel(guild_id, channel.id)
    await interaction.response.send_message(f"✅ Birthday channel set to {channel.mention}.", ephemeral=True)


giveaway_group = app_commands.Group(name="giveaway", description="Start and manage giveaways.")


@app_commands.command(name="setupgiveaway", description="Setup giveaway system defaults for this server.")
@app_commands.checks.has_permissions(administrator=True)
async def setupgiveaway(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    log_channel: discord.TextChannel | None = None,
    required_role: discord.Role | None = None,
):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    database.upsert_giveaway_config(
        guild_id,
        channel.id,
        log_channel.id if log_channel else None,
        required_role.id if required_role else None,
    )
    await interaction.response.send_message(
        f"✅ Giveaway setup complete in {channel.mention}.",
        ephemeral=True,
    )


@giveaway_group.command(name="start", description="Start a giveaway in the configured giveaway channel.")
@app_commands.checks.has_permissions(manage_guild=True)
async def giveaway_start(
    interaction: discord.Interaction,
    prize: str,
    duration: app_commands.Range[int, 1, 10080],
    winners: app_commands.Range[int, 1, 25],
):
    guild = interaction.guild
    guild_id = _require_guild(interaction)
    if not guild or not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    config = database.get_giveaway_config(guild_id)
    if not config:
        await interaction.response.send_message("❌ Run `/setupgiveaway` first.", ephemeral=True)
        return

    channel = guild.get_channel(config["channel_id"])
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("❌ Configured giveaway channel is missing. Run `/setupgiveaway` again.", ephemeral=True)
        return

    end_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=duration)
    end_unix = int(end_at.timestamp())
    role_line = ""
    if config.get("required_role_id"):
        role_line = f"\n🎭 Required Role: <@&{config['required_role_id']}>"

    embed = discord.Embed(
        title="🎉 GIVEAWAY 🎉",
        description=(
            f"🏆 Prize: **{prize}**\n"
            f"👥 Winners: **{winners}**\n"
            f"🧑 Host: {interaction.user.mention}\n"
            f"⏳ Ends: <t:{end_unix}:R>{role_line}\n\n"
            "👉 React with 🎉 to join!"
        ),
        color=discord.Color.gold(),
    )
    message = await channel.send(content=f"🎁 Giveaway by {interaction.user.mention}", embed=embed)
    await message.add_reaction("🎉")

    database.upsert_giveaway_entry(
        message.id,
        guild_id,
        channel.id,
        prize,
        winners,
        config.get("required_role_id"),
        end_at.isoformat() + "Z",
        interaction.user.id,
        status="active",
    )
    await interaction.response.send_message("✅ Giveaway started!", ephemeral=True)


@giveaway_group.command(name="reroll", description="Reroll winners for an ended giveaway message.")
@app_commands.checks.has_permissions(manage_guild=True)
async def giveaway_reroll(interaction: discord.Interaction, message_id: str):
    try:
        mid = int(message_id.strip())
    except ValueError:
        await interaction.response.send_message("❌ Invalid message ID.", ephemeral=True)
        return

    ok, message = await end_giveaway(mid, interaction.client, reroll=True)
    await interaction.response.send_message(message if ok else f"❌ {message}", ephemeral=True)


def _pick_winners(participants: Iterable[int], count: int) -> list[int]:
    pool = list(dict.fromkeys(int(uid) for uid in participants))
    random.shuffle(pool)
    return pool[:max(1, count)]


async def end_giveaway(message_id: int, client: discord.Client, reroll: bool = False) -> tuple[bool, str]:
    entry = database.get_giveaway_entry(message_id)
    if not entry:
        return False, "Giveaway not found in the database."

    guild = client.get_guild(int(entry["guild_id"]))
    if not guild:
        return False, "Guild unavailable for this giveaway."
    channel = guild.get_channel(int(entry["channel_id"]))
    if not isinstance(channel, discord.TextChannel):
        return False, "Giveaway channel is missing."

    try:
        message = await channel.fetch_message(int(message_id))
    except discord.NotFound:
        return False, "Giveaway message no longer exists."

    reaction = discord.utils.get(message.reactions, emoji="🎉")
    if not reaction:
        return False, "No giveaway reaction found."

    users = [u async for u in reaction.users()]
    participants = []
    required_role_id = entry.get("required_role_id")
    for user in users:
        if user.bot:
            continue
        member = guild.get_member(user.id)
        if not member:
            try:
                member = await guild.fetch_member(user.id)
            except discord.HTTPException:
                member = None
        if required_role_id and (not member or int(required_role_id) not in [role.id for role in member.roles]):
            continue
        participants.append(user.id)

    host_mention = f"<@{entry['created_by_user_id']}>" if entry.get("created_by_user_id") else "Unknown"

    if not participants:
        ended_embed = discord.Embed(
            title="🎉 GIVEAWAY ENDED 🎉",
            description=(
                f"🏆 Prize: **{entry['prize']}**\n"
                f"👥 Winners: **{entry['winner_count']}**\n"
                f"🧑 Host: {host_mention}\n"
                "❌ Result: No valid participants."
            ),
            color=discord.Color.red(),
        )
        await message.edit(content=f"🎁 Giveaway by {host_mention}", embed=ended_embed)
        await message.clear_reactions()
        if not reroll:
            database.mark_giveaway_ended(message_id)
        return False, "No valid participants."

    winner_ids = _pick_winners(participants, int(entry["winner_count"]))
    mentions = ", ".join(f"<@{uid}>" for uid in winner_ids)
    result_embed = discord.Embed(
        title="🏆 Giveaway Result",
        description=(
            f"Prize: **{entry['prize']}**\n"
            f"Winners: {mentions}\n"
            f"Hosted by: {host_mention}"
        ),
        color=discord.Color.green(),
    )
    await channel.send(content=f"🎉 Congratulations {mentions}!", embed=result_embed)

    if not reroll:
        ended_embed = discord.Embed(
            title="🎉 GIVEAWAY ENDED 🎉",
            description=(
                f"🏆 Prize: **{entry['prize']}**\n"
                f"👥 Winners: **{entry['winner_count']}**\n"
                f"🧑 Host: {host_mention}\n"
                f"🏅 Result: {mentions}"
            ),
            color=discord.Color.dark_gold(),
        )
        await message.edit(content=f"🎁 Giveaway by {host_mention}", embed=ended_embed)
        await message.clear_reactions()
        database.mark_giveaway_ended(message_id)

    config = database.get_giveaway_config(int(entry["guild_id"])) or {}
    log_channel = guild.get_channel(config.get("log_channel_id") or 0)
    if isinstance(log_channel, discord.TextChannel):
        action = "Rerolled" if reroll else "Ended"
        await log_channel.send(
            f"🎁 {action} giveaway `{message_id}` | Prize: **{entry['prize']}** | Winners: {mentions}"
        )

    return True, "🔁 Giveaway rerolled!" if reroll else "✅ Giveaway ended."


async def handle_giveaway_reaction(payload: discord.RawReactionActionEvent, client: discord.Client):
    if str(payload.emoji) != "🎉" or payload.user_id == client.user.id:
        return
    if not payload.guild_id:
        return

    entry = database.get_giveaway_entry(payload.message_id)
    if not entry or entry.get("status") != "active":
        return
    required_role_id = entry.get("required_role_id")
    if not required_role_id:
        return

    guild = client.get_guild(payload.guild_id)
    if not guild:
        return
    member = guild.get_member(payload.user_id)
    if not member:
        try:
            member = await guild.fetch_member(payload.user_id)
        except discord.HTTPException:
            return
    if int(required_role_id) in [role.id for role in member.roles]:
        return

    channel = guild.get_channel(payload.channel_id)
    if not isinstance(channel, discord.TextChannel):
        return
    message = await channel.fetch_message(payload.message_id)
    reaction = discord.utils.get(message.reactions, emoji="🎉")
    if reaction:
        await reaction.remove(member)
    try:
        await member.send("❌ You don't have the required role to join this giveaway.")
    except (discord.Forbidden, discord.HTTPException):
        pass


async def process_due_giveaways(client: discord.Client):
    due = database.list_due_giveaways(_iso_now_utc())
    for entry in due:
        await end_giveaway(int(entry["message_id"]), client, reroll=False)


async def process_birthdays(client: discord.Client):
    today = datetime.datetime.utcnow().date()
    today_md = today.strftime("%m-%d")
    today_key = today.isoformat()
    for guild in client.guilds:
        channel_id = database.get_birthday_channel(guild.id)
        if not channel_id:
            continue
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            continue
        rows = database.list_birthdays_for_guild(guild.id)
        for row in rows:
            birthday_value = row.get("birthday_date") or ""
            if len(birthday_value) < 10 or birthday_value[5:] != today_md:
                continue
            user_id = int(row["user_id"])
            if database.has_birthday_announcement(guild.id, user_id, today_key):
                continue
            embed = discord.Embed(
                title="🎉 Happy Birthday!",
                description=f"Happy Birthday <@{user_id}>! 🎂🎊",
                color=discord.Color.gold(),
            )
            embed.set_footer(text="Wishing you an amazing day!")
            await channel.send(embed=embed)
            database.add_birthday_announcement(guild.id, user_id, today_key)


serverstats_group = app_commands.Group(name="serverstats", description="Server insight dashboards.")


@serverstats_group.command(name="overview", description="General server overview.")
async def serverstats_overview(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    embed = discord.Embed(title="📊 Server Overview", color=discord.Color.blurple())
    embed.add_field(name="Members", value=str(guild.member_count), inline=True)
    embed.add_field(name="Channels", value=str(len(guild.channels)), inline=True)
    embed.add_field(name="Roles", value=str(len(guild.roles)), inline=True)
    await interaction.response.send_message(embed=embed)


@serverstats_group.command(name="members", description="Member breakdown.")
async def serverstats_members(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    humans = sum(1 for m in guild.members if not m.bot)
    bots = sum(1 for m in guild.members if m.bot)
    await interaction.response.send_message(f"👥 Humans: **{humans}**\n🤖 Bots: **{bots}**")


@serverstats_group.command(name="activity", description="Approximate active user stats.")
async def serverstats_activity(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    online = sum(1 for m in guild.members if str(m.status) in {"online", "idle", "dnd"})
    await interaction.response.send_message(f"📈 Active right now: **{online}** / **{guild.member_count or 0}**")


@serverstats_group.command(name="channels", description="Channel statistics.")
async def serverstats_channels(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    text_count = len(guild.text_channels)
    voice_count = len(guild.voice_channels)
    category_count = len(guild.categories)
    await interaction.response.send_message(
        f"#️⃣ Text: **{text_count}**\n🔊 Voice: **{voice_count}**\n🗂️ Categories: **{category_count}**"
    )


@serverstats_group.command(name="roles", description="Role distribution.")
async def serverstats_roles(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    top_roles = sorted(guild.roles, key=lambda r: len(r.members), reverse=True)
    lines = [f"• {role.name}: {len(role.members)}" for role in top_roles[:10] if not role.is_default()]
    await interaction.response.send_message("🧩 Top Roles by Members\n" + ("\n".join(lines) if lines else "No roles to show."))


ticket_group = app_commands.Group(name="ticket", description="Ticket system commands.")


DEFAULT_TICKET_SETTINGS = {
    "ticket_category_id": None,
    "channel_name_format": "ticket-{user}",
    "staff_role_id": None,
    "log_channel_id": None,
    "panel_channel_id": None,
    "embed_title": "🎫 Support Tickets",
    "embed_description": "Click the button below to create a ticket.",
    "button_text": "Create Ticket",
    "button_style": "primary",
    "who_can_open": "everyone",
    "allowed_role_id": None,
    "auto_assign_role_id": None,
    "max_open_tickets": 1,
    "auto_close_minutes": 0,
    "auto_delete_after_close": False,
    "transcript_on_close": True,
    "transcript_channel_id": None,
    "ticket_types": ["Support", "Report", "Billing", "General"],
    "welcome_message": "{user} welcome! Please describe your issue.",
    "priority_enabled": False,
    "reason_required": True,
}


def _merge_ticket_settings(settings: dict | None) -> dict:
    merged = dict(DEFAULT_TICKET_SETTINGS)
    if settings:
        merged.update(settings)
    return merged


def _ticket_button_style(value: str) -> discord.ButtonStyle:
    mapping = {
        "primary": discord.ButtonStyle.primary,
        "secondary": discord.ButtonStyle.secondary,
        "success": discord.ButtonStyle.success,
        "danger": discord.ButtonStyle.danger,
    }
    return mapping.get((value or "").lower(), discord.ButtonStyle.primary)


def _ticket_channel_name(template: str, member: discord.Member, ticket_type: str | None) -> str:
    safe_user = re.sub(r"[^a-z0-9_-]", "", member.name.lower())[:30] or str(member.id)
    safe_type = re.sub(r"[^a-z0-9_-]", "", (ticket_type or "general").lower())[:20]
    try:
        built = (template or "ticket-{user}").format(user=safe_user, userid=member.id, type=safe_type)
    except Exception:
        built = f"ticket-{safe_user}"
    cleaned = re.sub(r"[^a-z0-9-_]", "-", built.lower()).strip("-")
    return cleaned[:95] or f"ticket-{member.id}"


async def _build_ticket_transcript(channel: discord.TextChannel) -> str:
    lines = []
    async for msg in channel.history(limit=200, oldest_first=True):
        content = msg.content or "[embed/attachment]"
        lines.append(f"[{msg.created_at.isoformat()}] {msg.author.display_name}: {content}")
    return "\n".join(lines) if lines else "No messages found."


async def _open_ticket_channel(
    interaction: discord.Interaction,
    reason: str | None,
    ticket_type: str | None,
    priority: str | None,
):
    guild_id = _require_guild(interaction)
    guild = interaction.guild
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not guild_id or not guild or not member:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    settings = _merge_ticket_settings(database.get_ticket_settings(guild_id))
    if settings["who_can_open"] == "specific_role":
        allowed_role_id = settings.get("allowed_role_id")
        if not allowed_role_id or not any(r.id == int(allowed_role_id) for r in member.roles):
            await interaction.response.send_message("❌ You are not allowed to open tickets.", ephemeral=True)
            return
    if settings.get("reason_required") and not (reason or "").strip():
        await interaction.response.send_message("❌ A reason is required to open a ticket.", ephemeral=True)
        return

    max_open = max(1, int(settings.get("max_open_tickets") or 1))
    current_open = database.count_open_tickets_for_user(guild_id, member.id)
    if current_open >= max_open:
        await interaction.response.send_message(
            f"❌ You already have {current_open} open ticket(s). Max allowed is {max_open}.",
            ephemeral=True,
        )
        return

    selected_type = (ticket_type or "General").strip() or "General"
    configured_types = {t.lower(): t for t in settings.get("ticket_types", [])}
    if configured_types and selected_type.lower() not in configured_types:
        selected_type = settings.get("ticket_types", ["General"])[0]
    else:
        selected_type = configured_types.get(selected_type.lower(), selected_type)

    channel_name = _ticket_channel_name(settings.get("channel_name_format", "ticket-{user}"), member, selected_type)
    support_role = guild.get_role(int(settings["staff_role_id"])) if settings.get("staff_role_id") else None
    category = guild.get_channel(int(settings["ticket_category_id"])) if settings.get("ticket_category_id") else None

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_channels=True, manage_messages=True),
    }
    if support_role:
        overwrites[support_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)

    try:
        channel = await guild.create_text_channel(channel_name, category=category, overwrites=overwrites)
    except discord.Forbidden:
        await interaction.response.send_message("❌ I need Manage Channels permission to open tickets.", ephemeral=True)
        return

    reason_text = (reason or "No reason provided").strip()
    database.open_ticket(guild_id, channel.id, member.id, reason_text)

    welcome = (settings.get("welcome_message") or "{user} welcome!").format(
        user=member.mention,
        type=selected_type,
        reason=reason_text,
        priority=(priority or "normal"),
    )
    await channel.send(
        f"{welcome}\n\n"
        f"**Type:** {selected_type}\n"
        f"**Reason:** {reason_text}"
        + (f"\n**Priority:** {(priority or 'normal').title()}" if settings.get("priority_enabled") else "")
    )
    if support_role:
        await channel.send(f"{support_role.mention} new ticket created.")
    auto_assign_role = guild.get_role(int(settings["auto_assign_role_id"])) if settings.get("auto_assign_role_id") else None
    if auto_assign_role:
        await channel.send(f"{auto_assign_role.mention} auto-assigned for this ticket.")

    if settings.get("auto_close_minutes", 0):
        timeout_minutes = max(1, int(settings["auto_close_minutes"]))

        async def _auto_close_task():
            await asyncio.sleep(timeout_minutes * 60)
            entry = database.get_ticket_entry(guild_id, channel.id)
            if not entry or not entry.get("is_open"):
                return
            database.close_ticket(guild_id, channel.id)
            await channel.send(f"⏲️ Auto-closed after {timeout_minutes} minutes.")
            if settings.get("auto_delete_after_close"):
                await asyncio.sleep(10)
                await channel.delete(reason="Ticket auto-delete after close")

        asyncio.create_task(_auto_close_task())

    await interaction.response.send_message(f"✅ Ticket created: {channel.mention}", ephemeral=True)


class SetupTicketPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @discord.ui.button(label="Create Ticket", style=discord.ButtonStyle.primary, custom_id="setupticket:create")
    async def create_ticket_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = _merge_ticket_settings(database.get_ticket_settings(self.guild_id))
        button.label = settings.get("button_text") or "Create Ticket"
        button.style = _ticket_button_style(settings.get("button_style", "primary"))
        await _open_ticket_channel(interaction, reason="Opened from panel button", ticket_type=None, priority=None)


@app_commands.command(name="setupticket", description="Configure the ticket system and post/update a ticket panel.")
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.describe(
    ticket_category="Category where ticket channels are created",
    channel_name_format="Channel format like ticket-{user} or {type}-{user}",
    staff_role="Staff role that can view and manage tickets",
    log_channel="Log channel for transcripts/events",
    panel_channel="Where the ticket panel is posted",
    embed_title="Ticket panel embed title",
    embed_description="Ticket panel embed description",
    button_text="Ticket panel button text",
    button_style="Button style: primary, secondary, success, danger",
    who_can_open="Who can open tickets",
    allowed_role="Role allowed to open tickets when specific_role is selected",
    auto_assign_role="Role to ping/assign inside new tickets",
    max_open_tickets="Max open tickets per user",
    auto_close_minutes="Auto-close timer in minutes (0 to disable)",
    auto_delete_after_close="Delete channel after closing",
    transcript_on_close="Send transcript on close",
    transcript_channel="Channel where transcripts are sent",
    ticket_types_csv="Comma-separated ticket types (Support,Report,Billing,General)",
    welcome_message="Welcome message inside each ticket ({user}, {type}, {reason}, {priority})",
    priority_enabled="Enable priority metadata for tickets",
    reason_required="Require reason when opening a ticket",
)
@app_commands.choices(
    button_style=[
        app_commands.Choice(name="primary", value="primary"),
        app_commands.Choice(name="secondary", value="secondary"),
        app_commands.Choice(name="success", value="success"),
        app_commands.Choice(name="danger", value="danger"),
    ],
    who_can_open=[
        app_commands.Choice(name="everyone", value="everyone"),
        app_commands.Choice(name="specific_role", value="specific_role"),
    ],
)
async def setupticket(
    interaction: discord.Interaction,
    ticket_category: discord.CategoryChannel | None = None,
    channel_name_format: str | None = None,
    staff_role: discord.Role | None = None,
    log_channel: discord.TextChannel | None = None,
    panel_channel: discord.TextChannel | None = None,
    embed_title: str | None = None,
    embed_description: str | None = None,
    button_text: str | None = None,
    button_style: app_commands.Choice[str] | None = None,
    who_can_open: app_commands.Choice[str] | None = None,
    allowed_role: discord.Role | None = None,
    auto_assign_role: discord.Role | None = None,
    max_open_tickets: app_commands.Range[int, 1, 5] | None = None,
    auto_close_minutes: app_commands.Range[int, 0, 10080] | None = None,
    auto_delete_after_close: bool | None = None,
    transcript_on_close: bool | None = None,
    transcript_channel: discord.TextChannel | None = None,
    ticket_types_csv: str | None = None,
    welcome_message: str | None = None,
    priority_enabled: bool | None = None,
    reason_required: bool | None = None,
):
    guild_id = _require_guild(interaction)
    guild = interaction.guild
    if not guild_id or not guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    settings = _merge_ticket_settings(database.get_ticket_settings(guild_id))
    if ticket_category is not None:
        settings["ticket_category_id"] = ticket_category.id
    if channel_name_format is not None:
        settings["channel_name_format"] = channel_name_format[:80]
    if staff_role is not None:
        settings["staff_role_id"] = staff_role.id
    if log_channel is not None:
        settings["log_channel_id"] = log_channel.id
    if panel_channel is not None:
        settings["panel_channel_id"] = panel_channel.id
    if embed_title is not None:
        settings["embed_title"] = embed_title[:256]
    if embed_description is not None:
        settings["embed_description"] = embed_description[:2000]
    if button_text is not None:
        settings["button_text"] = button_text[:80]
    if button_style is not None:
        settings["button_style"] = button_style.value
    if who_can_open is not None:
        settings["who_can_open"] = who_can_open.value
    if allowed_role is not None:
        settings["allowed_role_id"] = allowed_role.id
    if auto_assign_role is not None:
        settings["auto_assign_role_id"] = auto_assign_role.id
    if max_open_tickets is not None:
        settings["max_open_tickets"] = int(max_open_tickets)
    if auto_close_minutes is not None:
        settings["auto_close_minutes"] = int(auto_close_minutes)
    if auto_delete_after_close is not None:
        settings["auto_delete_after_close"] = bool(auto_delete_after_close)
    if transcript_on_close is not None:
        settings["transcript_on_close"] = bool(transcript_on_close)
    if transcript_channel is not None:
        settings["transcript_channel_id"] = transcript_channel.id
    if ticket_types_csv is not None:
        parsed = [part.strip() for part in ticket_types_csv.split(",") if part.strip()]
        if parsed:
            settings["ticket_types"] = parsed[:10]
    if welcome_message is not None:
        settings["welcome_message"] = welcome_message[:1500]
    if priority_enabled is not None:
        settings["priority_enabled"] = bool(priority_enabled)
    if reason_required is not None:
        settings["reason_required"] = bool(reason_required)

    database.upsert_ticket_settings(guild_id, settings)
    panel_target = guild.get_channel(int(settings["panel_channel_id"])) if settings.get("panel_channel_id") else interaction.channel
    if isinstance(panel_target, discord.TextChannel):
        database.upsert_ticket_panel(guild_id, panel_target.id)
        embed = discord.Embed(
            title=settings["embed_title"],
            description=settings["embed_description"],
            color=discord.Color.blurple(),
            timestamp=datetime.datetime.utcnow(),
        )
        await panel_target.send(embed=embed, view=SetupTicketPanelView(guild_id))

    summary = (
        "✅ Ticket setup saved.\n"
        f"• Category: <#{settings['ticket_category_id']}> \n" if settings.get("ticket_category_id") else "✅ Ticket setup saved.\n• Category: not set\n"
    )
    await interaction.response.send_message(
        summary + f"• Staff role: {('<@&' + str(settings['staff_role_id']) + '>') if settings.get('staff_role_id') else 'not set'}\n"
        f"• Max open/user: {settings['max_open_tickets']}\n"
        f"• Auto-close: {settings['auto_close_minutes']} minute(s)\n"
        f"• Types: {', '.join(settings['ticket_types'])}",
        ephemeral=True,
    )


@ticket_group.command(name="open", description="Open a new ticket.")
@app_commands.describe(reason="Reason for opening the ticket", ticket_type="Type like Support, Report, Billing", priority="Priority if enabled")
@app_commands.choices(priority=[
    app_commands.Choice(name="low", value="low"),
    app_commands.Choice(name="normal", value="normal"),
    app_commands.Choice(name="high", value="high"),
])
async def ticket_open(
    interaction: discord.Interaction,
    reason: str | None = None,
    ticket_type: str | None = None,
    priority: app_commands.Choice[str] | None = None,
):
    await _open_ticket_channel(interaction, reason=reason, ticket_type=ticket_type, priority=(priority.value if priority else None))


@ticket_group.command(name="close", description="Close the current ticket.")
async def ticket_close(interaction: discord.Interaction):
    guild_id = _require_guild(interaction)
    if not guild_id:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return
    entry = database.get_ticket_entry(guild_id, interaction.channel_id)
    if not entry:
        await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
        return
    database.close_ticket(guild_id, interaction.channel_id)
    settings = _merge_ticket_settings(database.get_ticket_settings(guild_id))

    transcript = None
    if settings.get("transcript_on_close"):
        transcript = await _build_ticket_transcript(interaction.channel)
        transcript = transcript[:3800] + ("\n..." if len(transcript) > 3800 else "")
        target_id = settings.get("transcript_channel_id") or settings.get("log_channel_id")
        target_channel = interaction.guild.get_channel(int(target_id)) if target_id and interaction.guild else None
        if isinstance(target_channel, discord.TextChannel):
            await target_channel.send(
                f"📄 Transcript for <#{interaction.channel_id}> (closed by {interaction.user.mention})\n```{transcript}```"
            )

    await interaction.response.send_message("🔒 Ticket closed.")
    if settings.get("auto_delete_after_close"):
        await asyncio.sleep(10)
        await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")


@ticket_group.command(name="adduser", description="Add a user to this ticket channel.")
async def ticket_adduser(interaction: discord.Interaction, user: discord.Member):
    if not interaction.channel:
        await interaction.response.send_message("Invalid channel.", ephemeral=True)
        return
    await interaction.channel.set_permissions(user, read_messages=True, send_messages=True)
    await interaction.response.send_message(f"✅ Added {user.mention} to this ticket.")


@ticket_group.command(name="removeuser", description="Remove a user from this ticket channel.")
async def ticket_removeuser(interaction: discord.Interaction, user: discord.Member):
    if not interaction.channel:
        await interaction.response.send_message("Invalid channel.", ephemeral=True)
        return
    await interaction.channel.set_permissions(user, overwrite=None)
    await interaction.response.send_message(f"✅ Removed {user.mention} from this ticket.")


@app_commands.command(name="close", description="Shortcut: close the current ticket.")
async def ticket_close_shortcut(interaction: discord.Interaction):
    await ticket_close(interaction)


@app_commands.command(name="add", description="Shortcut: add a user to this ticket.")
@app_commands.describe(user="User to add to ticket")
async def ticket_add_shortcut(interaction: discord.Interaction, user: discord.Member):
    await ticket_adduser(interaction, user)


@app_commands.command(name="remove", description="Shortcut: remove a user from this ticket.")
@app_commands.describe(user="User to remove from ticket")
async def ticket_remove_shortcut(interaction: discord.Interaction, user: discord.Member):
    await ticket_removeuser(interaction, user)


@ticket_group.command(name="transcript", description="Create a quick text transcript for the current ticket.")
async def ticket_transcript(interaction: discord.Interaction):
    if not interaction.channel:
        await interaction.response.send_message("Invalid channel.", ephemeral=True)
        return
    messages = []
    async for msg in interaction.channel.history(limit=50, oldest_first=True):
        messages.append(f"[{msg.created_at.isoformat()}] {msg.author.display_name}: {msg.content}")
    transcript = "\n".join(messages) if messages else "No messages found."
    if len(transcript) > 1800:
        transcript = transcript[:1800] + "\n..."
    await interaction.response.send_message(f"📄 Transcript preview:\n```\n{transcript}\n```", ephemeral=True)


def register_extended_slash_commands(bot: discord.Client):
    bot.tree.add_command(brainrot)
    bot.tree.add_command(meme)
    bot.tree.add_command(sound)
    bot.tree.add_command(npc)
    bot.tree.add_command(dripcheck)
    bot.tree.add_command(cutecheck)
    bot.tree.add_command(mirror)
    bot.tree.add_command(ratio)
    bot.tree.add_command(chaos)
    bot.tree.add_command(mutebrainrot)
    bot.tree.add_command(uncookserver)
    bot.tree.add_command(fmbot_group)
    bot.tree.add_command(asterie_group)
    bot.tree.add_command(birthday_group)
    bot.tree.add_command(setupbirthday)
    bot.tree.add_command(setupbirthdaychannel)
    bot.tree.add_command(setupgiveaway)
    bot.tree.add_command(giveaway_group)
    bot.tree.add_command(serverstats_group)
    bot.tree.add_command(setupticket)
    bot.tree.add_command(ticket_close_shortcut)
    bot.tree.add_command(ticket_add_shortcut)
    bot.tree.add_command(ticket_remove_shortcut)
    bot.tree.add_command(ticket_group)
    bot.tree.add_command(agenda)
    bot.tree.add_command(agendaedit)
    bot.tree.add_command(donemeeting)
