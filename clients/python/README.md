# streamforge-client

Sync-first Python client for [StreamForge](../../README.md) live tables: subscribe to a
materialized table, get a `pandas.DataFrame` that's always current, ad-hoc SQL, and pushing rows
into an ingest source -- all without a browser.

Design doc: `ac-co.ai-4/apps/websites/otc-terms/docs/python-client-design.md`.

## Install

```bash
uv venv
uv pip install -e '.[dev]'
```

## Quickstart

```python
import streamforge

sf = streamforge.connect(url="http://localhost:5199", grpc="localhost:5299", user="admin", password="admin123!")

t = sf.table("trigger_monitor")          # subscribes, snapshots, replays; blocks until ready
t.rows                                   # list[dict], thread-safe copy of the current state
t.df                                     # pandas.DataFrame, built fresh on every read
t.wait_for(lambda df: len(df) > 0, timeout=30)
stop = t.on_change(lambda df: print(df.shape))   # called on the reader thread, coalesced ~120ms
t.close()

with sf.table("desk_exposure") as d:
    ...                                   # unsubscribes on exit

sf.snapshot("mc_path_pnl")               # one-shot REST read, no subscription, no thread
```

## Config

Resolved in this order (first hit wins, per field): explicit `connect()` kwargs -> environment ->
`~/.config/streamforge/config.toml`.

| kwarg | env | toml key |
|---|---|---|
| `url` | `STREAMFORGE_BASE_URL` | `base_url` |
| `grpc` | `STREAMFORGE_GRPC` | `grpc` |
| `user` | `STREAMFORGE_ADMIN_USER` | `user` |
| `password` | `STREAMFORGE_ADMIN_PASS` | `password` |
| -- | `SF_INGEST_KEY` | `ingest_key` |

```toml
# ~/.config/streamforge/config.toml
base_url = "http://localhost:5199"
grpc = "localhost:5299"
user = "admin"
password = "admin123!"
```

## Transports

`transport=` on `connect()`: `"grpc" | "signalr" | "signalr:ws" | "signalr:sse" | "signalr:lp" |
"auto"` (default `"auto"`). `"signalr"` is an alias for `"signalr:ws"`.

- **grpc** -- Tier 1 (`StreamService.SubscribeTable`, batch framing, real bidi backpressure on
  ingest). Needs the host started with `--Http:Port ... --Grpc:Port ...` (never bare `--urls`,
  which binds no gRPC port at all -- see the design doc §3.2).
- **signalr:ws** -- direct WebSocket, `?access_token=` query token (skips negotiate).
- **signalr:sse** / **signalr:lp** -- negotiate + the engine's existing SignalR hub over plain
  HTTP, for networks that refuse a WebSocket upgrade. Same `\x1e`-delimited JSON protocol as `ws`,
  just a different byte pipe underneath (`_hub.py`).
- **auto** -- tries gRPC, then SignalR (ws, then sse, then lp), and always logs which one it
  picked.

Two out-of-scope wire modes from the design doc are **not implemented here**: snapshot-diff
polling, and any transport beyond the two above. `transport=` only ever accepts the values listed.

## Ad-hoc SQL

```python
q = sf.sql("SELECT counterparty_id, SUM(exposure_usd) AS e FROM strategy_exposure GROUP BY counterparty_id",
           name="by_cp", key=["counterparty_id"])
q.df

sf.adhoc()              # DataFrame of adhoc_* tables
sf.drop_adhoc("adhoc_by_cp")
```

A rejected query raises `streamforge.SqlError`; `str(err)` renders the offending line with a
caret, and `.diagnostics` carries the engine's raw `{message, line, column, severity}` list.

## Ingest

```python
sf.push("trades", [{"trade_id": "t1", "desk": "Rates", ...}])
```

Uses the gRPC bidi `IngestService.Ingest` when the client is on the gRPC transport (real HTTP/2
backpressure), REST `POST /api/sources/{name}/events` otherwise. Prefers `X-SF-Ingest-Key`
(`SF_INGEST_KEY`) over the admin JWT when set, so a notebook that only pushes to a source never
needs to hold an admin login.

## Key fields

`sf.table(name)` needs to know a table's logical key to supersede rows correctly on updates.
`key=[...]` always overrides; `key=[]` means a global aggregate (one row); omitted falls back to
a small bundled map (`_keyfields.py`, ported from the otc-terms demo's catalog) and, for a table
that map doesn't know, to whole-row identity -- never a guessed first column. Wishlist #18 (the
engine surfacing a table's own key columns) is what deletes this map for good.

## Protobuf stubs

`src/streamforge/_pb/` is generated from `orleans/src/StreamForge.Host/Protos/streamforge.proto`
and **committed**, so installing the package needs no codegen step. Regenerate after a proto
change:

```bash
uv pip install -e '.[dev]'   # grpcio-tools
scripts/gen_protos.sh
```

## Tests

```bash
uv run pytest tests/test_zset.py -v          # pure reducer unit tests, no engine needed
uv run pytest tests/test_contract.py -v      # boots an isolated engine instance, both transports
```

The contract suite never binds `5199`/`5299` (the live dev server) or `6199` (the demo
container) -- it picks `9199`/`9299`, asserts they're free first, and skips with a clear message
rather than colliding.
