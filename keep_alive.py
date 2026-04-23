import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import unquote, urlparse
import database


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Registrar Bot Dashboard</title>
  <style>
    :root { color-scheme: dark; }
    body { font-family: Arial, sans-serif; margin: 2rem; background:#0f172a; color:#e2e8f0; }
    h1 { margin-bottom: 0.5rem; }
    .muted { color:#94a3b8; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:1rem; margin-top:1.5rem; }
    .card { background:#1e293b; border:1px solid #334155; border-radius:12px; padding:1rem; }
    .label { font-size:0.85rem; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; }
    .value { font-size:1.6rem; font-weight:bold; margin-top:0.3rem; }
  </style>
</head>
<body>
  <h1>Registrar Bot Dashboard</h1>
  <p class="muted">Live snapshot powered by the bot health server.</p>
  <div class="grid">
    <div class="card"><div class="label">Platform</div><div class="value" id="platform">-</div></div>
    <div class="card"><div class="label">Status</div><div class="value" id="statusText">-</div></div>
    <div class="card"><div class="label">Connected Guilds</div><div class="value" id="guildCount">0</div></div>
    <div class="card"><div class="label">Custom Commands</div><div class="value" id="commandCount">0</div></div>
    <div class="card"><div class="label">Present Records</div><div class="value" id="presentCount">0</div></div>
    <div class="card"><div class="label">Absent Records</div><div class="value" id="absentCount">0</div></div>
    <div class="card"><div class="label">Excused Records</div><div class="value" id="excusedCount">0</div></div>
  </div>
  <script>
    async function loadDashboard() {
      const response = await fetch('/api/dashboard', { cache: 'no-store' });
      const data = await response.json();
      document.getElementById('platform').textContent = data.platform || '-';
      document.getElementById('statusText').textContent = data.presence?.status_text || 'No custom status';
      document.getElementById('guildCount').textContent = data.guild_count || 0;
      document.getElementById('commandCount').textContent = data.custom_command_count || 0;
      document.getElementById('presentCount').textContent = data.attendance_totals?.present || 0;
      document.getElementById('absentCount').textContent = data.attendance_totals?.absent || 0;
      document.getElementById('excusedCount').textContent = data.attendance_totals?.excused || 0;
    }
    loadDashboard();
    setInterval(loadDashboard, 15000);
  </script>
</body>
</html>
"""


class _HealthHandler(BaseHTTPRequestHandler):
    _HEALTH_PATHS = {
        "/",
        "/healthz",
        "/readyz",
        "/health",
        "/healthcheck",
        "/network/healthcheck",
        "/network>healthcheck",
        "/network> healthcheck",
    }

    def _platform_name(self):
        if os.getenv("RAILWAY_ENVIRONMENT"):
            return "railway"
        if os.getenv("RENDER"):
            return "render"
        if os.getenv("CF_DEPLOYMENT_TARGET") == "cloudflare-containers" or os.getenv("CLOUDFLARE_DEPLOYMENT_ID"):
            return "cloudflare-containers"
        return "generic"

    def _health_payload(self):
        return {
            "status": "ok",
            "service": "registrar-bot",
            "platform": self._platform_name(),
            "host": self.headers.get("Host", ""),
        }

    def _send_json(self, payload, *, head_only=False):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_html(self, html, *, head_only=False):
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _handle_request(self, *, head_only=False):
        path = unquote(urlparse(self.path).path)

        if path == "/dashboard":
            self._send_html(DASHBOARD_HTML, head_only=head_only)
            return

        if path == "/api/dashboard":
            payload = database.get_dashboard_snapshot()
            payload["status"] = "ok"
            payload["service"] = "registrar-bot"
            payload["platform"] = self._platform_name()
            payload["host"] = self.headers.get("Host", "")
            self._send_json(payload, head_only=head_only)
            return

        if path not in self._HEALTH_PATHS:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        payload = self._health_payload()
        if path == "/readyz":
            payload["token_configured"] = bool(os.getenv("DISCORD_TOKEN"))

        self._send_json(payload, head_only=head_only)

    def do_GET(self):
        self._handle_request(head_only=False)

    def do_HEAD(self):
        self._handle_request(head_only=True)

    def log_message(self, format, *args):
        return


def run():
    port = int(os.environ.get("PORT", 8080))
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    server.serve_forever()


def keep_alive():
    thread = Thread(target=run, daemon=True)
    thread.start()
    return thread
