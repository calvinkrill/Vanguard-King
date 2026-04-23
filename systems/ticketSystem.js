const {
  ChannelType,
  PermissionsBitField,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
} = require('discord.js');

function buildTicketPanel() {
  const embed = new EmbedBuilder()
    .setTitle('🎫 Support Tickets')
    .setDescription('Click the button below to create a support ticket.')
    .setColor('Blue');

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('create_ticket')
      .setLabel('Create Ticket')
      .setStyle(ButtonStyle.Primary)
  );

  return { embed, row };
}

module.exports = async (client, guild) => {
  const channel = guild.channels.cache.find((c) => c.name === 'create-ticket');
  if (!channel) return;

  const { embed, row } = buildTicketPanel();
  await channel.send({ embeds: [embed], components: [row] });

  if (client.__ticketSystemBound) return;
  client.__ticketSystemBound = true;

  client.on('interactionCreate', async (interaction) => {
    if (!interaction.isButton() || interaction.customId !== 'create_ticket') return;

    const ticket = await interaction.guild.channels.create({
      name: `ticket-${interaction.user.username}`,
      type: ChannelType.GuildText,
      permissionOverwrites: [
        {
          id: interaction.guild.id,
          deny: [PermissionsBitField.Flags.ViewChannel],
        },
        {
          id: interaction.user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
          ],
        },
      ],
    });

    await interaction.reply({
      content: `Ticket created: ${ticket}`,
      ephemeral: true,
    });
  });
};
