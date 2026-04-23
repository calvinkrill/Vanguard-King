const { ActionRowBuilder, ButtonBuilder, ButtonStyle } = require('discord.js');

module.exports = async (client, guild, channel) => {
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('role_member')
      .setLabel('Get Member Role')
      .setStyle(ButtonStyle.Success),
    new ButtonBuilder()
      .setCustomId('role_gamer')
      .setLabel('Gamer Role')
      .setStyle(ButtonStyle.Primary)
  );

  await channel.send({
    content: '🎭 Choose your roles:',
    components: [row],
  });

  if (client.__reactionRolesBound) return;
  client.__reactionRolesBound = true;

  client.on('interactionCreate', async (interaction) => {
    if (!interaction.isButton()) return;

    const map = {
      role_member: 'Member',
      role_gamer: 'Gamer',
    };

    const roleName = map[interaction.customId];
    if (!roleName || !interaction.guild) return;

    const role = interaction.guild.roles.cache.find((r) => r.name === roleName);
    if (!role) {
      await interaction.reply({ content: `Role ${roleName} not found.`, ephemeral: true });
      return;
    }

    const member = await interaction.guild.members.fetch(interaction.user.id);
    if (member.roles.cache.has(role.id)) {
      await member.roles.remove(role);
      await interaction.reply({ content: `Removed ${roleName} role.`, ephemeral: true });
      return;
    }

    await member.roles.add(role);
    await interaction.reply({ content: `Assigned ${roleName} role.`, ephemeral: true });
  });
};
