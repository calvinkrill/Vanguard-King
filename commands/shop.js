const { EmbedBuilder, SlashCommandBuilder } = require('discord.js');
const items = require('../shopItems');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('shop')
    .setDescription('View shop'),

  async execute(interaction) {
    const embed = new EmbedBuilder()
      .setTitle('🏪 Dragon Shop')
      .setColor(0xffcc00);

    for (const key in items) {
      embed.addFields({
        name: items[key].name,
        value: `💰 ${items[key].price} gold`,
        inline: true
      });
    }

    await interaction.reply({ embeds: [embed] });
  }
};
