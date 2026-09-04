"""Tier 1 gRPC transport (design doc §3): StreamService.SubscribeTable for deltas,
TableService.Rows/Search/Validate/List for the catalog and snapshot, and the bidi
IngestService.Ingest for pushes.

`target` is `host:port` (plaintext h2c, prior knowledge -- no TLS negotiation, matching how the
engine is run from source when `--Tls:Enabled` is unset, §3.2's --urls trap), or scheme-prefixed:
`http://host:port` is the same plaintext channel, `https://host:port` opens a TLS channel with
ALPN h2 (the engine's TLS gRPC listener, plan/Server-side-TLS's two-listener branch). `ca`, when
given, is the PEM bytes of the server's certificate (or the CA that signed it) -- StreamsForge's
own `tools/tls/dev-cert.sh` mints a self-signed pair that IS its own trust anchor, so a client
dialing it passes that same cert.pem as `ca`. `ca=None` over https falls back to grpc's system
root store, which will reject a self-signed dev certificate -- that failure is deliberate, not a
bug (design doc: "over https WITHOUT ca the connect raises").

Row payloads travel as google.protobuf.Struct; MessageToDict is the whole "typing" story (design
doc §2: "Rows stay dicts... pandas re-types the column anyway"). Struct numbers are IEEE-754
doubles, so an int64 beyond 2**53 arrives as a Python str rather than losing precision -- a
documented, not fixed, edge (nothing in the reference demo crosses it).
"""

from __future__ import annotations

from typing import Iterator

import grpc
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from ._pb import streamsforge_pb2 as pb
from ._pb import streamsforge_pb2_grpc as pb_grpc
from ._transport import CancellableIterator
from ._zset import Delta, Row
from .errors import StreamsForgeError


def _to_struct(row: Row) -> Struct:
    s = Struct()
    s.update(row)
    return s


def parse_grpc_target(target: str) -> tuple[str, bool]:
    """Split a `grpc=`/`STREAMSFORGE_GRPC` target into `(host:port, use_tls)`.

    Accepts bare `host:port` (plaintext, unchanged from before TLS support), `http://host:port`
    (plaintext, explicit) and `https://host:port` (TLS, ALPN h2) -- the scheme is stripped either
    way since grpc.{insecure,secure}_channel both take a bare authority."""
    if target.startswith("https://"):
        return target[len("https://") :], True
    if target.startswith("http://"):
        return target[len("http://") :], False
    return target, False


class GrpcTransport:
    name = "grpc"

    def __init__(self, target: str, auth, ca: bytes | None = None) -> None:
        """`auth` is anything with a `.token()` method -- normally an `_http.AuthClient`, shared
        with the REST side so both transports mint/refresh the same JWT. `ca` is the PEM bytes of
        a trusted certificate/CA, used only when `target` resolves to an https (TLS) authority --
        see the module docstring."""
        host_port, use_tls = parse_grpc_target(target)
        if use_tls:
            creds = grpc.ssl_channel_credentials(root_certificates=ca)
            self._channel = grpc.secure_channel(host_port, creds)
        else:
            self._channel = grpc.insecure_channel(host_port)
        self._auth = auth
        self._tables = pb_grpc.TableServiceStub(self._channel)
        self._stream = pb_grpc.StreamServiceStub(self._channel)
        self._ingest = pb_grpc.IngestServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def _md(self) -> tuple[tuple[str, str], ...]:
        return (("authorization", f"Bearer {self._auth.token()}"),)

    # ---- catalog ----

    def list_tables(self, *, timeout: float | None = None) -> list[dict]:
        resp = self._tables.List(empty_pb2.Empty(), metadata=self._md(), timeout=timeout)
        return [MessageToDict(t, always_print_fields_with_no_presence=True) for t in resp.tables]

    def resolve_table_id(self, name: str) -> str:
        for t in self.list_tables():
            if t.get("name") == name:
                return t["id"]
        raise StreamsForgeError(f"no such table '{name}'")

    def validate(self, sql: str) -> dict:
        # `ok` (bool) and `plan_summary`'s presence both matter at their zero/default value --
        # same MessageToDict default-omission trap as the ingest ack below.
        resp = self._tables.Validate(pb.ValidateRequest(sql=sql), metadata=self._md())
        return MessageToDict(resp, always_print_fields_with_no_presence=True)

    def search(self, table_name: str, query: str, limit: int = 50) -> list[Delta]:
        table_id = self.resolve_table_id(table_name)
        resp = self._tables.Search(
            pb.SearchTableRequest(id=table_id, query=query, limit=limit), metadata=self._md()
        )
        return [(MessageToDict(r.row), r.weight) for r in resp.rows]

    # ---- Transport interface ----

    def snapshot(self, table_name: str, limit: int = 500) -> tuple[list[Delta], int]:
        table_id = self.resolve_table_id(table_name)
        resp = self._tables.Rows(pb.GetTableRowsRequest(id=table_id, limit=limit), metadata=self._md())
        rows = [(MessageToDict(r.row), r.weight) for r in resp.rows]
        return rows, resp.seq

    def subscribe(self, table_name: str) -> Iterator[tuple[list[Delta], int]]:
        # `call` is created synchronously (no blocking -- grpc-python only blocks on the first
        # iteration), which is what lets us hand back a live, thread-safe cancel hook attached to
        # the returned iterator. live.py's reader thread iterates this generator on its OWN worker
        # thread while the LiveTable's reader-orchestration thread may need to interrupt it from a
        # DIFFERENT thread on close()/reconnect -- calling Python's generator.close() cross-thread
        # would raise "generator already executing" and silently leak the subscription, but
        # grpc's Call.cancel() is documented safe from any thread, which is exactly the mismatch
        # this indirection resolves.
        call = self._stream.SubscribeTable(pb.SubscribeTableRequest(name=table_name), metadata=self._md())

        def _iter() -> Iterator[tuple[list[Delta], int]]:
            try:
                for batch in call:
                    deltas = [(MessageToDict(d.row), d.weight) for d in batch.deltas]
                    yield deltas, batch.seq
            except grpc.RpcError as exc:
                raise StreamsForgeError(f"gRPC SubscribeTable('{table_name}') stream ended: {exc}") from exc

        return CancellableIterator(_iter(), call.cancel)

    # ---- ingest ----

    def ingest(self, source_name: str, rows: list[Row], idempotency_key: str | None, partial: bool) -> dict:
        """One request, one ack, over a fresh bidi stream. This gets real backpressure semantics
        from the bidi RPC (the server does not ack until PushAsync returns) without holding a
        stream open across calls -- the simplest thing that satisfies the interface; a
        long-lived streaming session (real sustained backpressure across many pushes) is future
        work, not needed for this client's push() surface."""

        def req_iter():
            yield pb.IngestRequest(
                source_name=source_name,
                rows=[_to_struct(r) for r in rows],
                partial=partial,
                idempotency_key=idempotency_key or "",
            )

        call = self._ingest.Ingest(req_iter(), metadata=self._md())
        try:
            ack = next(iter(call))
        except StopIteration as exc:
            raise StreamsForgeError(f"gRPC Ingest('{source_name}') stream closed with no ack") from exc
        # IngestOutcome.INGEST_OUTCOME_ACCEPTED is enum value 0, proto3's default -- MessageToDict
        # omits default-valued fields unless told otherwise, which would silently drop `outcome`
        # on every successful push.
        return MessageToDict(ack, always_print_fields_with_no_presence=True)
