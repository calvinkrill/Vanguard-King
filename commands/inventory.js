const { EmbedBuilder, SlashCommandBuilder } = require('discord.js');
const db = require('../db');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('inventory')
    .setDescription('View your inventory'),

  async execute(interaction) {
    const user = db.getUser(interaction.user.id);

    const embed = new EmbedBuilder()
      .setTitle(`🎒 ${interaction.user.username}'s Inventory`)
      .addFields(
        { name: '💰 Gold', value: `${user.gold}`, inline: true },
        { name: '⭐ Level', value: `${user.level}`, inline: true },
        { name: '🧬 XP', value: `${user.xp}`, inline: true },
        {
          name: '🐲 Dragons',
          value: user.dragons.map((d, i) =>
            `${i + 1}. ${d.name} (Lv ${d.level})`
          ).join('\n') || 'None'
        }
      )
      .setColor(0x00ff99);

    await interaction.reply({ embeds: [embed] });
  }
};
