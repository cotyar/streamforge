"""push(source, rows, ...) -- gRPC bidi when the client's live transport is gRPC (real HTTP/2
backpressure, design doc §3.1), REST POST /api/sources/{name}/events otherwise (ported from
pushEvents in lib/streamforge/server.ts). Prefers X-SF-Ingest-Key over the admin JWT when
SF_INGEST_KEY is configured, so a notebook that only feeds a source never needs to hold one
(design doc §4) -- the REST route is AllowAnonymous with its own dual check, and the gRPC
IngestService checks the same header (lowercased: "x-sf-ingest-key") per-message.
"""

from __future__ import annotations

from .errors import IngestRejected


def push(client, source: str, rows: list[dict], idempotency_key: str | None, partial: bool) -> dict:
    if client._grpc is not None:
        return _push_grpc(client, source, rows, idempotency_key, partial)
    return _push_rest(client, source, rows, idempotency_key, partial)


def _push_grpc(client, source: str, rows: list[dict], idempotency_key: str | None, partial: bool) -> dict:
    ack = client._grpc.ingest(source, rows, idempotency_key, partial)
    if ack.get("outcome") != "INGEST_OUTCOME_ACCEPTED":
        raise IngestRejected(
            ack.get("error") or f"{source} ingest push rejected: {ack.get('outcome')}",
            ack.get("rowErrors", []),
        )
    return ack


def _push_rest(client, source: str, rows: list[dict], idempotency_key: str | None, partial: bool) -> dict:
    headers = {}
    use_ingest_key = bool(client._ingest_key)
    if use_ingest_key:
        headers["X-SF-Ingest-Key"] = client._ingest_key

    body: dict = {"events": rows, "partial": partial}
    if idempotency_key:
        body["idempotencyKey"] = idempotency_key

    # auth=False when pushing with an ingest key: the route is AllowAnonymous with its own
    # header check, and we must not force an admin login just to attach a Bearer token nobody
    # asked for (design doc §4's "never holds an admin JWT" ask).
    resp = client._http.request(
        "POST", f"/api/sources/{source}/events", json=body, headers=headers, auth=not use_ingest_key
    )
    if resp.status_code != 202:
        body_json = resp.json() if resp.content else {}
        raise IngestRejected(
            body_json.get("error") or f"{source} ingest push failed: {resp.status_code}",
            body_json.get("rowErrors", []),
        )
    return resp.json()
