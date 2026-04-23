function addXP(user, amount) {
  user.xp += amount;

  const needed = user.level * 100;

  if (user.xp >= needed) {
    user.xp -= needed;
    user.level++;
    return true; // leveled up
  }
  return false;
}

module.exports = { addXP };
