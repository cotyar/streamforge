#!/usr/bin/env python3
"""StreamForge Dapr flavor — Python trades enricher (plan 005, wave W8-A).

Pure Dapr pub/sub polyglot processor. Speaks nothing but HTTP to its own sidecar:

  1. Subscribes "sf-source-trades" (the egress copy of the seeded "trades" source that
     GeneratorActor publishes — see dapr/src/StreamForge.Dapr.Host/Actors/GeneratorActor.cs).
  2. For each SourceEventsEnvelope batch, derives 3 fields per trade: notional (price*qty),
     signedQty (a side classification: +qty on BUY, -qty on SELL), and avgPrice (a rolling
     per-symbol mean).
  3. Republishes the enriched batch into "sf-sources" (the polyglot door, decision D-D) under a
     brand-new source name, "trades-enriched" — camelCase {source, events} per dapr/POLYGLOT.md,
     the frozen external-publisher contract.
  4. Best-effort registers the "trades-enriched" SourceDefinition via REST so the SPA/console has
     a catalog entry to relay into (POST /api/sources always accepts a JSON body regardless of who
     publishes into "sf-sources" — the router itself doesn't gate on registration, but the console
     needs the registered name to have a `source:{name}` SignalR group to join). Enabled=false:
     this source is fed by THIS process, not a synthetic GeneratorActor.

No third-party dependencies (stdlib only) — see README.md for how to run it.
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_PORT = int(os.environ.get("APP_PORT", "8399"))
DAPR_HTTP_PORT = int(os.environ.get("DAPR_HTTP_PORT", "3899"))

PUBSUB = "pubsub"
EGRESS_TOPIC = "sf-source-trades"  # dapr/POLYGLOT.md: egress-only, one SourceEventsEnvelope per message
SOURCES_TOPIC = "sf-sources"  # dapr/POLYGLOT.md: the polyglot door (decision D-D)
ENRICHED_SOURCE = "trades-enriched"

SF_API = os.environ.get("SF_API", "http://localhost:5399").rstrip("/")
SF_USER = os.environ.get("SF_USER", "editor")
SF_PASS = os.environ.get("SF_PASS", "editor123!")

PUBLISH_URL = f"http://localhost:{DAPR_HTTP_PORT}/v1.0/publish/{PUBSUB}/{SOURCES_TOPIC}"

# Original "trades" fields (shared/StreamForge.AppCore/Generators/MarketDataProfiles.cs) plus the 3
# derived fields this enricher adds. FieldType values are the exact C# enum member names — the
# shared REST surface's JsonStringEnumConverter has no naming policy, so it expects "Double", not
# "double" (StreamForgeApiExtensions.cs's ConfigureHttpJsonOptions).
SOURCE_DEF = {
    "name": ENRICHED_SOURCE,
    "description": "Trades enriched by the Python polyglot processor (wave W8-A): notional, a "
    "signed-quantity side classification, and a rolling per-symbol average price.",
    "fields": [
        {"name": "symbol", "type": "String", "children": None, "isArray": False},
        {"name": "price", "type": "Double", "children": None, "isArray": False},
        {"name": "qty", "type": "Long", "children": None, "isArray": False},
        {"name": "side", "type": "String", "children": None, "isArray": False},
        {"name": "venue", "type": "String", "children": None, "isArray": False},
        {"name": "notional", "type": "Double", "children": None, "isArray": False},
        {"name": "signedQty", "type": "Long", "children": None, "isArray": False},
        {"name": "avgPrice", "type": "Double", "children": None, "isArray": False},
    ],
    "generatorProfile": "generic",
    "eventsPerSecond": 8,  # unused while Enabled=false, but POST /api/sources requires > 0
    "enabled": False,  # CRITICAL — see module docstring point 4
    "tags": [],
    "metadata": {},
}


def log(msg: str) -> None:
    print(f"[sf-enricher] {msg}", flush=True)


# ponytail: the rolling per-symbol average is a plain in-memory (count, sum) running mean — resets
# on process restart and isn't shared across replicas. A real deployment would park this in the
# Dapr state store (or a proper windowed aggregation); a sample enricher doesn't need either.
_avg_lock = threading.Lock()
_avg_state: dict[str, tuple[int, float]] = {}


def enrich(evt: dict) -> dict:
    price = float(evt.get("price") or 0)
    qty = int(evt.get("qty") or 0)
    side = evt.get("side", "")
    symbol = evt.get("symbol", "")

    with _avg_lock:
        count, total = _avg_state.get(symbol, (0, 0.0))
        count, total = count + 1, total + price
        _avg_state[symbol] = (count, total)
        avg_price = total / count

    out = dict(evt)
    out["notional"] = round(price * qty, 2)
    out["signedQty"] = qty if side == "BUY" else -qty
    out["avgPrice"] = round(avg_price, 2)
    return out


def publish_enriched(events: list) -> None:
    body = json.dumps({"source": ENRICHED_SOURCE, "events": events}).encode()
    req = urllib.request.Request(
        PUBLISH_URL, data=body, method="POST", headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # route BaseHTTPRequestHandler's access log through log()
        log(fmt % args)

    def _send_json(self, code: int, obj) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/dapr/subscribe":
            self._send_json(
                200, [{"pubsubname": PUBSUB, "topic": EGRESS_TOPIC, "route": "/sf-source-trades"}]
            )
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/sf-source-trades":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            log("malformed pub/sub payload — acking (200) anyway to avoid a redelivery loop")
            self._send_json(200, {"status": "SUCCESS"})
            return

        # Dapr's HTTP delivery to a subscribed route is always CloudEvents-shaped: unwrap "data" if
        # present, otherwise assume the body already IS the envelope (covers a raw manual test).
        envelope = payload.get("data", payload) if isinstance(payload, dict) else payload
        if isinstance(envelope, str):
            try:
                envelope = json.loads(envelope)
            except json.JSONDecodeError:
                envelope = None

        events = envelope.get("events") if isinstance(envelope, dict) else None
        if not events:
            log("empty/unrecognized envelope — acking (200) anyway")
            self._send_json(200, {"status": "SUCCESS"})
            return

        enriched = [enrich(e) for e in events]
        try:
            publish_enriched(enriched)
            log(f"enriched {len(enriched)} trade(s) -> republished as '{ENRICHED_SOURCE}'")
        except Exception as ex:  # noqa: BLE001 — never let a publish hiccup break the ack
            log(f"failed to republish enriched batch: {ex}")

        # Always ack 200, same poison-message policy as the .NET host's own topic endpoints
        # (dapr/POLYGLOT.md's "Malformed-payload handling" section) — at-least-once redelivery on a
        # non-2xx would otherwise retry a permanently-bad message forever.
        self._send_json(200, {"status": "SUCCESS"})


class _AlreadyRegistered(Exception):
    pass


def _login() -> str:
    body = json.dumps({"username": SF_USER, "password": SF_PASS}).encode()
    req = urllib.request.Request(
        f"{SF_API}/api/auth/login",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())["token"]


def _post_source(token: str) -> None:
    body = json.dumps(SOURCE_DEF).encode()
    req = urllib.request.Request(
        f"{SF_API}/api/sources/",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except urllib.error.HTTPError as ex:
        detail = ex.read().decode(errors="replace")
        if ex.code == 400 and "already exists" in detail:
            raise _AlreadyRegistered() from None
        raise


def register_source_loop() -> None:
    """Best-effort, non-fatal registration retry loop — runs in a background daemon thread so a
    down/slow host never blocks the enricher's real job (subscribing/publishing)."""
    delay = 2
    while True:
        try:
            token = _login()
            _post_source(token)
            log(f"registered source '{ENRICHED_SOURCE}' on {SF_API}")
            return
        except _AlreadyRegistered:
            log(f"source '{ENRICHED_SOURCE}' already registered on {SF_API} — treating as success")
            return
        except (urllib.error.URLError, OSError) as ex:
            log(f"{SF_API} host unreachable, will retry in {delay}s ({ex})")
        except Exception as ex:  # noqa: BLE001 — registration is best-effort, never fatal
            log(f"registration attempt failed, will retry in {delay}s: {ex}")
        time.sleep(delay)
        delay = min(delay * 2, 30)


def main() -> None:
    threading.Thread(target=register_source_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", APP_PORT), Handler)
    log(
        f"listening on :{APP_PORT} (sidecar http :{DAPR_HTTP_PORT}) — subscribing '{EGRESS_TOPIC}', "
        f"republishing enriched batches as source '{ENRICHED_SOURCE}' into '{SOURCES_TOPIC}'"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
