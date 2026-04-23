const { SlashCommandBuilder, EmbedBuilder } = require('discord.js');
const db = require('../db');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('pvp')
    .setDescription('Fight another player')
    .addUserOption(o =>
      o.setName('target')
        .setDescription('Player to fight')
        .setRequired(true)
    ),

  async execute(interaction) {
    const opponent = interaction.options.getUser('target');

    const user1 = db.getUser(interaction.user.id);
    const user2 = db.getUser(opponent.id);

    if (user1.activeDragon === null || user2.activeDragon === null) {
      return interaction.reply('❌ Both players need an active dragon!');
    }

    const d1 = user1.dragons[user1.activeDragon];
    const d2 = user2.dragons[user2.activeDragon];

    let hp1 = d1.hp;
    let hp2 = d2.hp;

    while (hp1 > 0 && hp2 > 0) {
      hp2 -= d1.attack;
      if (hp2 <= 0) break;

      hp1 -= d2.attack;
    }

    const winner = hp1 > 0 ? interaction.user.username : opponent.username;

    const embed = new EmbedBuilder()
      .setTitle('⚔️ PvP Battle')
      .setDescription(`${d1.name} vs ${d2.name}`)
      .addFields(
        { name: 'Winner', value: winner }
      )
      .setColor(0xff0000);

    await interaction.reply({ embeds: [embed] });
  }
};
