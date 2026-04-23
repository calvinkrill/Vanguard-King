const { SlashCommandBuilder, PermissionFlagsBits, ChannelType } = require('discord.js');
const ticketSystem = require('../systems/ticketSystem');
const reactionRoles = require('../systems/reactionRoles');
const auditLogger = require('../systems/auditLogger');
const antiRaid = require('../systems/antiRaid');

const DEFAULT_CHANNELS = [
  { name: 'create-ticket', type: ChannelType.GuildText },
  { name: 'roles', type: ChannelType.GuildText },
  { name: 'audit-logs', type: ChannelType.GuildText },
  { name: 'welcome', type: ChannelType.GuildText },
];

module.exports = {
  data: new SlashCommandBuilder()
    .setName('setupfullserver')
    .setDescription('Provision the full modular server setup pack')
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

  async execute(interaction) {
    const { guild, client } = interaction;

    await interaction.reply({ content: '⚙️ Building full server setup...', ephemeral: true });

    for (const config of DEFAULT_CHANNELS) {
      const exists = guild.channels.cache.find((c) => c.name === config.name);
      if (!exists) {
        await guild.channels.create(config);
      }
    }

    const roleNames = ['Member', 'Gamer'];
    for (const roleName of roleNames) {
      const exists = guild.roles.cache.find((r) => r.name === roleName);
      if (!exists) {
        await guild.roles.create({ name: roleName, mentionable: true });
      }
    }

    const ticketChannel = guild.channels.cache.find((c) => c.name === 'create-ticket');
    const rolesChannel = guild.channels.cache.find((c) => c.name === 'roles');

    await ticketSystem(client, guild);
    if (rolesChannel) {
      await reactionRoles(client, guild, rolesChannel);
    }

    if (!client.__auditLoggerBound) {
      auditLogger(client);
      client.__auditLoggerBound = true;
    }

    if (!client.__antiRaidBound) {
      antiRaid(client);
      client.__antiRaidBound = true;
    }

    await interaction.editReply(
      `✅ Full setup complete.${ticketChannel ? ` Ticket panel posted in #${ticketChannel.name}.` : ''}`
    );
  },
};
