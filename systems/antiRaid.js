const joinMap = new Map();

module.exports = (client, options = {}) => {
  const maxJoins = options.maxJoins || 5;
  const windowMs = options.windowMs || 10_000;

  client.on('guildMemberAdd', async (member) => {
    const { guild } = member;
    const now = Date.now();

    const joins = joinMap.get(guild.id) || [];
    const freshJoins = joins.filter((t) => now - t < windowMs);
    freshJoins.push(now);
    joinMap.set(guild.id, freshJoins);

    if (freshJoins.length <= maxJoins) return;

    for (const channel of guild.channels.cache.values()) {
      if (!channel.permissionOverwrites?.edit) continue;
      await channel.permissionOverwrites
        .edit(guild.id, { SendMessages: false })
        .catch(() => null);
    }

    const log = guild.channels.cache.find((c) => c.name === 'audit-logs');
    if (log) {
      await log.send('🛡️ Anti-raid triggered: server channels locked temporarily.');
    }
  });
};
