module.exports = async (guild, db) => {
  const names = [
    'welcome',
    'goodbye',
    'rules',
    'logs',
    'mod-chat',
    'audit-logs',
    'ticket-create',
    'create-ticket',
    'general-chat',
  ];

  for (const channel of guild.channels.cache.values()) {
    if (names.includes(channel.name)) {
      await channel.delete().catch(() => null);
    }
  }

  for (const role of guild.roles.cache.values()) {
    if (['Admin', 'Moderator', 'Muted', 'Member', 'Gamer'].includes(role.name)) {
      await role.delete().catch(() => null);
    }
  }

  if (db?.delete) {
    await db.delete(`setup_${guild.id}`);
  }

  return true;
};
