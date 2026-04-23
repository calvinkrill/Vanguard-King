const { SlashCommandBuilder, EmbedBuilder } = require('discord.js');
const db = require('../db');
const { addXP } = require('../utils');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('battle')
    .setDescription('Fight a wild dragon'),

  async execute(interaction) {
    const user = db.getUser(interaction.user.id);

    if (user.activeDragon === null || !user.dragons[user.activeDragon]) {
      return interaction.reply('❌ Set an active dragon first with /setdragon.');
    }

    const active = user.dragons[user.activeDragon];
    const wild = { name: 'Wild Wyrmling', hp: 35, attack: 8 };

    let yourHp = active.hp;
    let wildHp = wild.hp;

    while (yourHp > 0 && wildHp > 0) {
      wildHp -= active.attack;
      if (wildHp <= 0) break;
      yourHp -= wild.attack;
    }

    if (yourHp <= 0) {
      return interaction.reply(`💀 ${active.name} lost against ${wild.name}.`);
    }

    user.gold += 25;
    const leveledUp = addXP(user, 50);
    db.updateUser(interaction.user.id, user);

    const embed = new EmbedBuilder()
      .setTitle('⚔️ Battle Result')
      .setDescription(`${active.name} defeated ${wild.name}!`)
      .addFields(
        { name: '💰 Gold Earned', value: '25', inline: true },
        { name: '🧬 XP Earned', value: '50', inline: true },
        { name: '⭐ Player Level', value: `${user.level}`, inline: true }
      )
      .setColor(0x33cc66);

    await interaction.reply({ embeds: [embed] });

    if (leveledUp) {
      await interaction.followUp(`🎉 You leveled up! Now level ${user.level}`);
    }
  }
};
