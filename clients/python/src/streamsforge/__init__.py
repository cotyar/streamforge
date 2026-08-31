"""streamsforge -- sync-first Python client for StreamsForge live tables.

    import streamsforge
    sf = streamsforge.connect()          # env / ~/.config/streamsforge/config.toml
    t = sf.table("trigger_monitor")     # subscribes, snapshots, replays; blocks until ready
    t.df                                # pandas.DataFrame, built fresh on every read
    t.wait_for(lambda df: len(df) > 0, timeout=30)
    stop = t.on_change(lambda df: print(df.exposure_usd.sum()))
    with sf.table("desk_exposure") as d:
        ...                            # unsubscribes on exit

See docs/python-client-design.md (ac-co.ai-4 repo) for the full design and rationale.
"""

from __future__ import annotations

import logging
from urllib.parse import urlsplit

from . import _config
from . import ingest as _ingest
from . import sql as _sql
from . import tables as _tables
from ._grpc import GrpcTransport
from ._hub import HubTransport
from ._http import AuthClient
from .errors import AuthError, IngestRejected, NotReady, SqlError, StreamsForgeError
from .live import DEFAULT_FLUSH_MS, LiveTable

__all__ = [
    "connect",
    "Client",
    "LiveTable",
    "StreamsForgeError",
    "AuthError",
    "SqlError",
    "IngestRejected",
    "NotReady",
]

logger = logging.getLogger("streamsforge")

_SIGNALR_MODES = {"signalr": "ws", "signalr:ws": "ws", "signalr:sse": "sse", "signalr:lp": "lp"}
_VALID_TRANSPORTS = {"grpc", "auto", *_SIGNALR_MODES}


class Client:
    """Returned by connect(). Holds the REST auth client, the chosen live transport (gRPC or one
    SignalR wire mode), and everything else (ingest key, verify) the per-feature modules need."""

    def __init__(
        self,
        http: AuthClient,
        grpc: GrpcTransport | None,
        live_transport,
        ingest_key: str | None,
        transport_name: str,
    ) -> None:
        self._http = http
        self._grpc = grpc
        self._live_transport = live_transport
        self._ingest_key = ingest_key
        self.transport_name = transport_name

    # ---- tables / live ----

    def table(
        self,
        name: str,
        key: list[str] | None = None,
        timeout: float = 30,
        flush_ms: float = DEFAULT_FLUSH_MS,
    ) -> LiveTable:
        key_fields = key if key is not None else _tables.resolve_key_fields(self._http, name)
        return LiveTable(self._live_transport, name, key_fields, timeout=timeout, flush_ms=flush_ms)

    def snapshot(self, name: str, limit: int = 500):
        return _tables.snapshot(self._http, name, limit)

    def tables(self) -> list[dict]:
        return _tables.list_tables(self._http)

    def search(self, name: str, query: str, limit: int = 50) -> list[dict]:
        return _tables.search(self._http, name, query, limit)

    def history(self, name: str, lookup: dict, limit: int | None = None) -> list[dict]:
        return _tables.history(self._http, name, lookup, limit)

    # ---- ad-hoc SQL ----

    def sql(
        self,
        sql_text: str,
        name: str,
        key: list[str] | None = None,
        timeout: float = 30,
        flush_ms: float = DEFAULT_FLUSH_MS,
    ) -> LiveTable:
        return _sql.run(self, name, sql_text, key, timeout, flush_ms)

    def validate(self, sql_text: str) -> dict:
        return _sql.validate(self._http, sql_text)

    def adhoc(self):
        return _sql.list_adhoc(self._http)

    def drop_adhoc(self, name: str) -> bool:
        return _sql.drop_adhoc(self._http, name)

    # ---- ingest ----

    def push(
        self, source: str, rows: list[dict], idempotency_key: str | None = None, partial: bool = False
    ) -> dict:
        return _ingest.push(self, source, rows, idempotency_key, partial)

    def close(self) -> None:
        if self._grpc is not None:
            self._grpc.close()
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _default_grpc_target(base_url: str) -> str:
    """Guess the gRPC port from the REST base_url following Program.cs's own PORT/PORT+100
    convention. Only a fallback -- pass grpc= (or STREAMSFORGE_GRPC) whenever the two ports don't
    follow that relationship, e.g. an explicit --Http:Port/--Grpc:Port pair that isn't +100 apart."""
    parts = urlsplit(base_url)
    host = parts.hostname or "localhost"
    http_port = parts.port or (443 if parts.scheme == "https" else 80)
    return f"{host}:{http_port + 100}"


def _probe_signalr_mode(http: AuthClient, verify: bool) -> str:
    """auto: try signalr:ws, then signalr:sse, then give up on signalr:lp (it needs no upgrade
    and no long-lived probe connection, so it is the mode that "always works" if REST does)."""
    from . import _hub

    ws_base = http.base_url.replace("https://", "wss://").replace("http://", "ws://")
    try:
        pipe = _hub._WsPipe(ws_base, http.token(), verify)
        pipe.close()
        return "ws"
    except Exception as exc:
        logger.warning("streamsforge: signalr:ws unavailable (%s), trying signalr:sse", exc)

    try:
        token = _hub._negotiate(http)
        with http.stream(
            "GET", "/hubs/stream", params={"id": token}, headers={"accept": "text/event-stream"}
        ) as resp:
            resp.raise_for_status()
        return "sse"
    except Exception as exc:
        logger.warning("streamsforge: signalr:sse unavailable (%s), trying signalr:lp", exc)

    return "lp"


def connect(
    url: str | None = None,
    grpc: str | None = None,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    transport: str = "auto",
    verify: bool = True,
) -> Client:
    """One-line connect. Resolution order for url/grpc/user/password: explicit kwarg -> env
    (STREAMSFORGE_BASE_URL / STREAMSFORGE_GRPC / STREAMSFORGE_ADMIN_USER / STREAMSFORGE_ADMIN_PASS) ->
    ~/.config/streamsforge/config.toml.

    `transport`: "grpc" | "signalr" | "signalr:ws" | "signalr:sse" | "signalr:lp" | "auto"
    (default). "signalr" is an alias for "signalr:ws". "auto" tries gRPC, then SignalR (ws, then
    sse, then lp), and always logs which one it got -- a client that silently degrades and lets
    someone believe they're watching a live stream is worse than one that fails loudly.
    """
    if transport not in _VALID_TRANSPORTS:
        raise StreamsForgeError(
            f"unknown transport '{transport}' -- expected grpc, signalr, signalr:ws, signalr:sse, "
            "signalr:lp or auto"
        )

    cfg = _config.resolve(url=url, grpc=grpc, user=user, password=password)
    if not cfg.base_url:
        raise StreamsForgeError(
            "no base URL: pass url=, set STREAMSFORGE_BASE_URL, or add base_url to "
            "~/.config/streamsforge/config.toml"
        )

    http = AuthClient(cfg.base_url, cfg.user, cfg.password, verify=verify, token=token)

    grpc_transport: GrpcTransport | None = None
    if transport in ("grpc", "auto"):
        target = cfg.grpc or _default_grpc_target(cfg.base_url)
        try:
            candidate = GrpcTransport(target, http)
            candidate.list_tables(timeout=3.0)  # proves the channel AND the JWT actually work
            grpc_transport = candidate
        except Exception as exc:
            if transport == "grpc":
                raise StreamsForgeError(
                    f"gRPC channel to {target} refused. If the host was started with --urls, "
                    "Program.cs's guard binds no gRPC port at all -- start it with "
                    "--Http:Port/--Grpc:Port instead (design doc §3.2)."
                ) from exc
            logger.warning(
                "streamsforge: gRPC unavailable (%s: %s), falling back to SignalR", type(exc).__name__, exc
            )

    hub_transport: HubTransport | None = None
    chosen: str
    if grpc_transport is not None:
        chosen = "grpc"
    else:
        mode = _SIGNALR_MODES[transport] if transport in _SIGNALR_MODES else _probe_signalr_mode(http, verify)
        hub_transport = HubTransport(http, mode=mode, verify=verify)
        chosen = hub_transport.name

    logger.info("streamsforge: connected via %s transport (%s)", chosen, cfg.base_url)

    return Client(
        http=http,
        grpc=grpc_transport,
        live_transport=grpc_transport if grpc_transport is not None else hub_transport,
        ingest_key=cfg.ingest_key,
        transport_name=chosen,
    )
