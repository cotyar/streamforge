# streamsforge-client

Sync-first Python client for [StreamsForge](../../README.md) live tables: subscribe to a
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
import streamsforge

sf = streamsforge.connect(url="http://localhost:5199", grpc="localhost:5299", user="admin", password="admin123!")

t = sf.table("trigger_monitor")          # subscribes, snapshots, replays; blocks until ready
t.rows                                   # list[dict], thread-safe copy of the current state
t.df                                     # pandas.DataFrame, built fresh on every read
t.wait_for(lambda df: len(df) > 0, timeout=30)
stop = t.on_change(lambda df: print(df.shape))   # called on the reader thread -- see Change notifications below
t.close()

with sf.table("desk_exposure") as d:
    ...                                   # unsubscribes on exit

sf.snapshot("mc_path_pnl")               # one-shot REST read, no subscription, no thread
```

## Config

Resolved in this order (first hit wins, per field): explicit `connect()` kwargs -> environment ->
`~/.config/streamsforge/config.toml`.

| kwarg | env | toml key |
|---|---|---|
| `url` | `STREAMSFORGE_BASE_URL` | `base_url` |
| `grpc` | `STREAMSFORGE_GRPC` | `grpc` |
| `user` | `STREAMSFORGE_ADMIN_USER` | `user` |
| `password` | `STREAMSFORGE_ADMIN_PASS` | `password` |
| -- | `SF_INGEST_KEY` | `ingest_key` |

```toml
# ~/.config/streamsforge/config.toml
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

## TLS

An `https://` `url` (or `grpc=` target) talks TLS on every transport. `ca=` (also `STREAMSFORGE_CA`
env, `ca` in `config.toml`) is the path to a PEM certificate/CA to trust -- required against
`tools/tls/dev-cert.sh`'s self-signed dev certificate, since it is its own trust anchor and appears
in no system store:

```python
sf = streamsforge.connect(url="https://host:5199", ca="cert.pem", user="admin", password="...")
```

`verify=False` skips certificate checks for REST/SignalR only -- grpc-python has no equivalent, so
an https gRPC target with `verify=False` and no `ca=` raises `ValueError` at `connect()` rather than
failing later with an opaque handshake error. For gRPC over a self-signed cert, pass `ca=` instead.

## Ad-hoc SQL

```python
q = sf.sql("SELECT counterparty_id, SUM(exposure_usd) AS e FROM strategy_exposure GROUP BY counterparty_id",
           name="by_cp", key=["counterparty_id"])
q.df

sf.adhoc()              # DataFrame of adhoc_* tables
sf.drop_adhoc("adhoc_by_cp")
```

A rejected query raises `streamsforge.SqlError`; `str(err)` renders the offending line with a
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
`key=[...]` always overrides; omitted reads the table's own `keyFields` from `GET /api/tables`
(wishlist #18): a non-empty list is the GROUP BY/LATEST BY key, `[]` means a global aggregate (one
row), and `null` -- or an engine build old enough not to report the field at all, or a table this
engine doesn't know about -- falls back to whole-row identity, never a guessed first column.

## Change notifications and backpressure

`on_change` is the only push-style observation API this client offers -- there is no async
iterator over a `LiveTable`'s deltas (unlike the TypeScript client, which has both `onChange` and
an async-iterable side). If you need a "read the next change" pull loop instead of a callback,
poll `.df`/`.rows`/`.wait_for()` from your own loop; every read is a fresh, thread-safe projection
of the reader thread's current state, so there is nothing to miss between polls.

**The window, and why it changed.** Before this version, every `on_change` callback waited out a
fixed 120ms trailing-edge window: the reader always let the window elapse before publishing,
whether or not anything else was going to arrive in it. That is backwards for the common case -- a
lone update on an otherwise-quiet table -- where coalescing has nothing to save and the wait is
pure latency, handed straight back to the consumer for free. It matters now specifically because
the engine's push-stream transport (`--Streams:Transport push`) takes delta delivery from p50
~115ms down to p50 ~1ms; a client-side 120ms floor on top of that would have thrown the whole win
away.

The fix is a **leading-edge + trailing-coalesce** window, sized by `flush_ms` (default **16** --
one frame at 60Hz, the shortest interval any UI could even display, so it's the natural ceiling
rather than an arbitrary tuning knob):

- If at least `flush_ms` has elapsed since the last `on_change` publish (including "there hasn't
  been one yet"), the new state publishes **immediately** after the batch is applied -- no wait.
- Otherwise, the batch is merged into a single pending publish due at `last_publish + flush_ms`;
  any further batches that land before that deadline merge into the same pending publish rather
  than each scheduling their own. At most one publish is ever pending.
- `flush_ms=0` disables coalescing entirely: one `on_change` call per applied batch, whatever the
  arrival rate.

Either way, every publish -- leading or trailing -- carries the table's full current DataFrame,
not a diff: the window changes **when** a consumer is told, never **what** they're told. Pass
`flush_ms=` to `sf.table(...)` or `sf.sql(...)` to change it per table; this is a **behaviour
change** from the previous version's unconditional 120ms trailing wait, not just a smaller number.

**Why this can't be a back-pressured queue.** The reader thread's whole job is to keep draining
the transport's delta stream into the Z-set, whatever `on_change` callbacks are doing -- it must
never block waiting for a slow consumer, because the transport itself (gRPC/SignalR) has no way to
tell the *server* "pause, my client is behind" without also stalling every other consumer of that
stream. And even if it could block, a queue of pending *DataFrames* is the wrong thing to build:
each snapshot supersedes the last one entirely, so a consumer that's behind by three snapshots
wants the newest, not all three in order -- buffering the stale ones is pure memory growth for
data nobody will read. That's why the coalescing window above holds at most one pending publish
(latest-wins by construction, not by a queue that happens to be capped at one) rather than
growing without bound the way a naive producer/consumer queue would under a slow callback.

## Protobuf stubs

`src/streamsforge/_pb/` is generated from `orleans/src/StreamsForge.Host/Protos/streamsforge.proto`
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
