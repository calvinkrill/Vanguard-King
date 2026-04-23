const { SlashCommandBuilder } = require('discord.js');
const db = require('../db');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('setdragon')
    .setDescription('Set your active dragon')
    .addIntegerOption(o =>
      o.setName('index')
        .setDescription('Dragon number')
        .setRequired(true)
    ),

  async execute(interaction) {
    const user = db.getUser(interaction.user.id);
    const index = interaction.options.getInteger('index') - 1;

    if (!user.dragons[index]) {
      return interaction.reply('❌ Invalid dragon!');
    }

    user.activeDragon = index;
    db.updateUser(interaction.user.id, user);

    await interaction.reply(`🐲 Active dragon set to ${user.dragons[index].name}`);
  }
};
