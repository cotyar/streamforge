"""SignalR client protocol over three wire modes, sharing one framing/dispatch layer (design doc
§3.4). `app.MapHub<StreamHub>("/hubs/stream")` restricts no transports, so the engine already
serves WebSockets, Server-Sent Events and Long Polling on that one URL -- this is not a fallback
bolted on to a WebSocket client, it is the same protocol over three different byte pipes:

  signalr:ws   direct WebSocket, skipNegotiation -- `ws(s)://…/hubs/stream?access_token=<jwt>`.
               Cheapest, and the default ("signalr" == "signalr:ws"). The query-string token
               exists ONLY because a browser's WebSocket handshake cannot send headers; this
               client could use a header here too, but keeps parity with the browser's own
               constraint deliberately -- see the two HTTP modes below for where Python actually
               gets something the browser does not.
  signalr:sse  `POST …/hubs/stream/negotiate?negotiateVersion=1` (Authorization HEADER -- httpx
               can send one, unlike EventSource) for a connectionToken, then `GET
               …/hubs/stream?id=<token>` as text/event-stream for receiving. SSE is receive-only,
               so SubscribeTable (and the ping echo) goes over a separate `POST
               …/hubs/stream?id=<token>`.
  signalr:lp   same negotiate + POST send channel; the receive side is a `GET` that returns one
               batch (or times out empty -- normal, not a disconnect) and is immediately
               re-issued.

All three then speak the identical `\x1e`-delimited JSON Hub Protocol: handshake
`{"protocol":"json","version":1}`, invocation `{"type":1,"target":"SubscribeTable","arguments":[…]}`,
inbound `tableDelta(name, deltas, seq)`, `{"type":6}` ping (echo it back on the SEND channel --
for sse/lp that is the POST, not the GET), `{"type":7}` close (caller reconnects). `_drive` below
is that layer, written once; each pipe class supplies only `send`/`recv_chunks`.

Reconnect/backoff is NOT here -- see live.py. A dropped socket, an SSE stream ending or a failed
long-poll GET all just raise or fall out of the generator; live.py's reader loop is what retries
with backoff and re-runs snapshot+replay (resuming a stream without a fresh snapshot silently
corrupts the Z-set -- design doc §3.6). One operational note that belongs in this docstring
because it is a property of the wire mode, not of this client: SSE and long-polling bind a
connection to one server instance, so a multi-instance deployment needs session affinity
(WebSockets too, for what it's worth) -- irrelevant for a single-instance engine, a Cloud Run
setting the day it matters.
"""

from __future__ import annotations

import itertools
import json
import ssl
from typing import Iterator

from ._transport import CancellableIterator
from ._zset import Delta
from .errors import StreamsForgeError

RS = "\x1e"
_SUB_ID = "sf-sub-1"  # one subscription per connection, so a constant id is enough


# ============================================================================
# One framing/dispatch layer, shared by all three pipes.
# ============================================================================


def _frame_iter(pipe) -> Iterator[str]:
    buf = ""
    for chunk in pipe.recv_chunks():
        if not chunk:
            continue
        buf += chunk
        while RS in buf:
            frame, buf = buf.split(RS, 1)
            if frame:
                yield frame


def _handshake_and_subscribe(pipe, table_name: str) -> Iterator[str]:
    """Complete the hub handshake and register the subscription, SYNCHRONOUSLY.

    This must finish before subscribe() returns, and the reason is a real lost-update race rather
    than tidiness. LiveTable's contract is subscribe -> buffer -> snapshot -> replay: the buffering
    exists precisely because deltas can land before the snapshot read. But if the SubscribeTable
    invocation is still in flight when the snapshot is taken, the server has no subscription yet,
    so deltas emitted in that window are not buffered -- they are never sent at all, and a fresh
    subscription gets no backfill. A LATEST BY table self-heals on the next tick; a row asserted
    once and never touched again just silently goes missing until something else disturbs it.

    (Found by the Kotlin client, whose cold Flow had the same shape. gRPC does not need this: the
    RPC is put on the wire when the call object is created.)

    The invocation carries an `invocationId` and we WAIT for its completion, which turns this from
    "the frame is on the wire" into a real guarantee: StreamHub.SubscribeTable returns the
    Groups.AddToGroupAsync task itself, so the server only sends the completion once this
    connection is in the `table:{name}` broadcast group. Without the id, SignalR treats the message
    as fire-and-forget and never replies -- and since the snapshot travels over a DIFFERENT
    connection, "written to the socket" would order nothing at all."""
    pipe.send(json.dumps({"protocol": "json", "version": 1}) + RS)
    frames = _frame_iter(pipe)
    next(frames, None)  # handshake ack ("{}") -- nothing to inspect
    pipe.send(json.dumps({
        "type": 1, "invocationId": _SUB_ID, "target": "SubscribeTable", "arguments": [table_name],
    }) + RS)

    # Deltas can already be arriving while we wait for the completion; hold them and put them back
    # in front of the stream rather than dropping them on the floor.
    early: list[str] = []
    for frame in frames:
        msg = json.loads(frame)
        if msg.get("type") == 3 and msg.get("invocationId") == _SUB_ID:
            if msg.get("error"):
                raise StreamsForgeError(f"SubscribeTable('{table_name}') rejected: {msg['error']}")
            break
        early.append(frame)
    else:
        raise StreamsForgeError(f"connection closed before SubscribeTable('{table_name}') completed")
    return itertools.chain(early, frames)


def _drive(frames: Iterator[str], pipe, table_name: str) -> Iterator[tuple[list[Delta], int]]:
    for frame in frames:
        msg = json.loads(frame)
        mtype = msg.get("type")
        if mtype == 6:  # ping -- echo on the same logical connection to keep it alive
            pipe.send(json.dumps({"type": 6}) + RS)
        elif mtype == 7:  # server-initiated close -- let the caller reconnect
            return
        elif mtype == 1 and msg.get("target") == "tableDelta":
            args = msg.get("arguments") or []
            if len(args) < 3:
                continue
            name, deltas, seq = args[0], args[1], args[2]
            if name != table_name:
                continue
            yield [(d["row"], d["weight"]) for d in deltas], seq


# ============================================================================
# Byte pipes. Each supplies send(text) and recv_chunks() -> Iterator[str]; _drive() above never
# sees the transport-specific parts (websocket-client, or httpx GET/POST).
# ============================================================================


class _WsPipe:
    def __init__(self, ws_base_url: str, token: str, verify: bool) -> None:
        import websocket  # websocket-client

        url = f"{ws_base_url}/hubs/stream?access_token={token}"
        sslopt = {"cert_reqs": ssl.CERT_NONE} if not verify else None
        self._ws = websocket.create_connection(url, sslopt=sslopt, timeout=30)

    def send(self, text: str) -> None:
        self._ws.send(text)

    def recv_chunks(self) -> Iterator[str]:
        while True:
            data = self._ws.recv()
            if not data:
                return
            yield data

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:
            pass


def _negotiate(http) -> str:
    resp = http.post("/hubs/stream/negotiate", params={"negotiateVersion": 1})
    resp.raise_for_status()
    body = resp.json()
    token = body.get("connectionToken")
    if not token:
        raise StreamsForgeError("SignalR negotiate returned no connectionToken")
    return token


class _SsePipe:
    """Receive-only SSE GET; SubscribeTable/ping go out over a parallel POST (design doc §3.4)."""

    def __init__(self, http, connection_token: str) -> None:
        self._http = http
        self._token = connection_token
        # The GET is opened HERE, in the constructor, not lazily on first read. SSE has no buffer
        # on the server side: anything written to the connection before its transport has started
        # is gone, so a handshake POSTed ahead of the GET is answered into a void and the client
        # then waits forever for an ack that was already discarded. (Long polling survives the same
        # ordering because a poll drains a buffer, which is why only this mode failed.) Opening the
        # receive channel first is also what the browser client does.
        self._ctx = http.stream(
            "GET", "/hubs/stream", params={"id": connection_token},
            headers={"accept": "text/event-stream"},
        )
        resp = self._ctx.__enter__()
        resp.raise_for_status()
        self._lines = resp.iter_lines()

    def send(self, text: str) -> None:
        resp = self._http.post(
            "/hubs/stream",
            params={"id": self._token},
            content=text.encode("utf-8"),
            headers={"content-type": "text/plain;charset=UTF-8"},
        )
        resp.raise_for_status()

    def recv_chunks(self) -> Iterator[str]:
        for line in self._lines:
            if line.startswith("data:"):
                payload = line[len("data:") :]
                if payload.startswith(" "):
                    payload = payload[1:]
                # Put the record separator back. httpx's iter_lines() ends up in str.splitlines(),
                # which treats \x1e as a line boundary just like \n -- so SignalR's own frame
                # terminator is eaten on the way in, and _frame_iter (which splits on it) would
                # buffer forever without ever seeing a complete frame. One SSE data line is exactly
                # one frame here, so re-appending it restores the contract the other pipes deliver.
                yield payload + RS

    def close(self) -> None:
        try:
            self._ctx.__exit__(None, None, None)
        except Exception:
            pass
        try:
            self._http.delete("/hubs/stream", params={"id": self._token})
        except Exception:
            pass


class _LongPollPipe:
    def __init__(self, http, connection_token: str) -> None:
        self._http = http
        self._token = connection_token
        self._closed = False

    def send(self, text: str) -> None:
        resp = self._http.post(
            "/hubs/stream",
            params={"id": self._token},
            content=text.encode("utf-8"),
            headers={"content-type": "text/plain;charset=UTF-8"},
        )
        resp.raise_for_status()

    def recv_chunks(self) -> Iterator[str]:
        while not self._closed:
            # Long poll: the server holds this open until data arrives or it times out. A 204
            # means the connection was closed server-side -- stop. An empty 200 body means "no
            # data yet, ask again" -- normal, not a disconnect, so we just loop.
            resp = self._http.get("/hubs/stream", params={"id": self._token}, timeout=120.0)
            if resp.status_code == 204:
                return
            resp.raise_for_status()
            if resp.content:
                yield resp.content.decode("utf-8")

    def close(self) -> None:
        self._closed = True
        try:
            self._http.delete("/hubs/stream", params={"id": self._token})
        except Exception:
            pass


# ============================================================================
# Transport
# ============================================================================

_MODES = ("ws", "sse", "lp")


class HubTransport:
    def __init__(self, http, mode: str = "ws", verify: bool = True) -> None:
        if mode not in _MODES:
            raise StreamsForgeError(f"unknown signalr mode '{mode}' -- expected one of {_MODES}")
        self._http = http
        self._mode = mode
        self._verify = verify
        self.name = f"signalr:{mode}"

    def close(self) -> None:
        pass  # no persistent connection is held outside an active subscribe()

    def snapshot(self, table_name: str, limit: int = 500) -> tuple[list[Delta], int]:
        # Snapshot is REST for every wire mode -- there is no "SSE version" of GET /rows.
        from . import tables as _tables

        table_id = _tables.resolve_table_id(self._http, table_name)
        resp = self._http.get(f"/api/tables/{table_id}/rows", params={"limit": limit})
        resp.raise_for_status()
        body = resp.json()
        rows: list[Delta] = [(r["row"], r["weight"]) for r in body.get("rows", [])]
        return rows, body.get("seq", 0)

    def subscribe(self, table_name: str) -> Iterator[tuple[list[Delta], int]]:
        # Pipe creation happens synchronously (before any yield), so the returned generator can
        # carry a thread-safe `.cancel` hook -- see _grpc.py's subscribe() for why this matters:
        # a generator's own .close() is not safe to call from a thread other than the one
        # currently iterating it. pipe.close() unblocks a ws recv() reliably (closing the
        # underlying socket cross-thread); for sse/lp it is best-effort (an in-flight httpx GET is
        # not forcibly interrupted), bounded by long-polling's own ~120s request timeout.
        pipe = self._open_pipe()
        try:
            frames = _handshake_and_subscribe(pipe, table_name)
        except BaseException:
            pipe.close()
            raise

        def _iter() -> Iterator[tuple[list[Delta], int]]:
            try:
                yield from _drive(frames, pipe, table_name)
            finally:
                pipe.close()

        return CancellableIterator(_iter(), pipe.close)

    def _open_pipe(self):
        if self._mode == "ws":
            ws_base = self._http.base_url.replace("https://", "wss://").replace("http://", "ws://")
            return _WsPipe(ws_base, self._http.token(), self._verify)
        connection_token = _negotiate(self._http)
        if self._mode == "sse":
            return _SsePipe(self._http, connection_token)
        return _LongPollPipe(self._http, connection_token)
