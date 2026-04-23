const fs = require('fs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'dragon-users.json');

let data = {};

function load() {
  if (!fs.existsSync(DB_PATH)) return;
  try {
    data = JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
  } catch {
    data = {};
  }
}

function save() {
  fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2));
}

function createDefaultUser(id) {
  data[id] = {
    gold: 100,
    xp: 0,
    level: 1,
    dragons: [],
    activeDragon: null,
    items: { potion: 2 }
  };
  return data[id];
}

function getUser(id) {
  if (!data[id]) {
    createDefaultUser(id);
    save();
  }
  return data[id];
}

function updateUser(id, user) {
  data[id] = user;
  save();
}

function addDragonToUser(id, dragon) {
  const user = getUser(id);

  user.dragons.push({
    name: dragon.name,
    level: 1,
    xp: 0,
    hp: dragon.hp,
    attack: dragon.attack
  });

  updateUser(id, user);
  return user;
}

load();

module.exports = {
  getUser,
  updateUser,
  addDragonToUser,
  save
};
