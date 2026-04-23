import { env as workerEnv } from "cloudflare:workers";
import { Container } from "@cloudflare/containers";

const BOT_INSTANCE_NAME = "registrar-bot";
const INTERNAL_ORIGIN = "https://registrar-bot.internal";

export class RegistrarBotContainer extends Container {
  defaultPort = 8080;
  sleepAfter = "70s";

  envVars = {
    CF_DEPLOYMENT_TARGET: "cloudflare-containers",
    DB_FILE: "/tmp/attendance.db",
    DISCORD_TOKEN: workerEnv.DISCORD_TOKEN,
  };

  onStart() {
    console.log("Registrar Bot container started");
  }

  onStop() {
    console.log("Registrar Bot container stopped");
  }

  onError(error) {
    console.error("Registrar Bot container error", error);
  }
}

function getBotContainer(env) {
  return env.REGISTRAR_BOT.getByName(BOT_INSTANCE_NAME);
}

function makeInternalRequest(pathname, init = {}) {
  return new Request(new URL(pathname, INTERNAL_ORIGIN), {
    method: init.method ?? "GET",
    headers: init.headers,
    body: init.body,
  });
}

export default {
  async fetch(request, env) {
    return getBotContainer(env).fetch(request);
  },

  async scheduled(_controller, env) {
    const response = await getBotContainer(env).fetch(makeInternalRequest("/healthz"));
    console.log(`Keepalive completed with status ${response.status}`);
  },
};
