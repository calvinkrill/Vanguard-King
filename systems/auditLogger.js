function resolveLogChannel(guild) {
  return guild.channels.cache.find((c) => c.name === 'audit-logs');
}

async function sendLog(guild, text) {
  const channel = resolveLogChannel(guild);
  if (!channel) return;
  await channel.send(text);
}

module.exports = (client) => {
  client.on('messageDelete', (msg) => {
    if (!msg.guild) return;
    sendLog(msg.guild, `🗑 Message deleted in #${msg.channel?.name || 'unknown'}`);
  });

  client.on('messageUpdate', (oldMsg, newMsg) => {
    if (!newMsg.guild || oldMsg.content === newMsg.content) return;
    sendLog(newMsg.guild, `✏️ Message edited in #${newMsg.channel?.name || 'unknown'}`);
  });

  client.on('guildBanAdd', (ban) => {
    sendLog(ban.guild, `⛔ User banned: ${ban.user.tag}`);
  });

  client.on('guildMemberUpdate', (oldMember, newMember) => {
    if (oldMember.roles.cache.size !== newMember.roles.cache.size) {
      sendLog(oldMember.guild, `🔄 Roles updated for ${newMember.user.tag}`);
    }
  });

  client.on('channelUpdate', (oldChannel, newChannel) => {
    if (oldChannel.name !== newChannel.name) {
      sendLog(newChannel.guild, `🛠 Channel updated: ${oldChannel.name} → ${newChannel.name}`);
    }
  });
};
