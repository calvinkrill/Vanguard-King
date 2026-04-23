import fs from 'node:fs';
import path from 'node:path';

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

const root = process.cwd();
const configPath = path.join(root, 'wrangler.jsonc');
const workerPath = path.join(root, 'cloudflare-worker', 'index.js');
const dockerfilePath = path.join(root, 'Dockerfile');

assert(fs.existsSync(configPath), 'Missing wrangler.jsonc');
assert(fs.existsSync(workerPath), 'Missing cloudflare-worker/index.js');
assert(fs.existsSync(dockerfilePath), 'Missing Dockerfile');

const workerSource = fs.readFileSync(workerPath, 'utf8');
new Function(workerSource.replace(/import\s+[^;]+;/g, '').replace(/export\s+default/g, 'const __default =').replace(/export\s+class\s+/g, 'class '));

const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
assert(config.main === 'cloudflare-worker/index.js', 'wrangler.jsonc main must point to cloudflare-worker/index.js');
assert(Array.isArray(config.containers) && config.containers.length > 0, 'wrangler.jsonc must define at least one container');
assert(config.containers[0].image === './Dockerfile', 'wrangler.jsonc container image must point to ./Dockerfile');
assert(config.durable_objects?.bindings?.some((binding) => binding.name === 'REGISTRAR_BOT'), 'wrangler.jsonc must bind REGISTRAR_BOT durable object');

console.log('Cloudflare worker source and wrangler container config look valid.');
