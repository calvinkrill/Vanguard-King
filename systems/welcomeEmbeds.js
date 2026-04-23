const { AttachmentBuilder, EmbedBuilder } = require('discord.js');
const Canvas = require('canvas');

module.exports = async (member, channel) => {
  const canvas = Canvas.createCanvas(700, 250);
  const ctx = canvas.getContext('2d');

  ctx.fillStyle = '#1e1e1e';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  ctx.font = '28px sans-serif';
  ctx.fillStyle = '#ffffff';
  ctx.fillText(`Welcome ${member.user.username}`, 200, 110);
  ctx.font = '20px sans-serif';
  ctx.fillText(`Member #${member.guild.memberCount}`, 200, 155);

  const avatar = await Canvas.loadImage(
    member.user.displayAvatarURL({ extension: 'png', size: 256 })
  );
  ctx.drawImage(avatar, 20, 50, 150, 150);

  const attachment = new AttachmentBuilder(canvas.toBuffer(), {
    name: 'welcome.png',
  });

  const embed = new EmbedBuilder()
    .setTitle('👋 Welcome!')
    .setDescription(`Welcome to the server, ${member}!`)
    .setImage('attachment://welcome.png')
    .setColor('Green');

  await channel.send({ embeds: [embed], files: [attachment] });
};
