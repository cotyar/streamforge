"""Ad-hoc SQL: validate (creates nothing) -> POST /api/config/import?mode=merge (create-or-update)
-> LiveTable. Two-stage flow, the `adhoc_` name slug and the drop-outside-the-namespace refusal
are ported from lib/streamsforge/adhoc.ts (otc-terms) -- see runAdhocQuery/adhocTableName/
dropAdhocTable there. Always REST: there is no gRPC RPC for config import.
"""

from __future__ import annotations

import re

import pandas as pd

from .errors import SqlError, StreamsForgeError

ADHOC_PREFIX = "adhoc_"


def adhoc_table_name(raw: str) -> str:
    """`Exposure vs Ostrava!` -> `adhoc_exposure_vs_ostrava`. Already-prefixed names pass
    through, so re-running an edited query updates the same table."""
    slug = raw.strip().lower()
    slug = re.sub(r"^adhoc_", "", slug)
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")[:48]
    return f"{ADHOC_PREFIX}{slug or 'scratch_1'}"


def validate(http, sql_text: str) -> dict:
    resp = http.post("/api/tables/validate", json={"sql": sql_text})
    body = resp.json() if resp.content else {}
    if resp.status_code >= 400 or not isinstance(body, dict):
        raise SqlError(f"validate failed: {resp.status_code}", diagnostics=[], sql=sql_text)
    return body


def _diagnostics_error(sql_text: str, diagnostics: list[dict]) -> SqlError:
    message = diagnostics[0]["message"] if diagnostics else "SQL rejected"
    return SqlError(message, diagnostics, sql=sql_text)


def run(client, name: str, sql_text: str, key: list[str] | None, timeout: float, flush_ms: float):
    table_name = adhoc_table_name(name)
    validated = validate(client._http, sql_text)
    if not validated.get("ok"):
        raise _diagnostics_error(sql_text, validated.get("diagnostics", []))

    resp = client._http.post(
        "/api/config/import",
        params={"mode": "merge"},
        json={
            "version": 1,
            "sources": [],
            "pipelines": [],
            "tables": [
                {
                    "name": table_name,
                    "description": "Ad-hoc query from the Python client",
                    "sql": sql_text,
                    "running": True,
                }
            ],
        },
    )
    body = resp.json() if resp.content else {}
    entries = body.get("entries", []) if isinstance(body, dict) else []
    errored = [e for e in entries if e.get("action") == "error"]
    if resp.status_code >= 400 or errored:
        diagnostics = []
        for e in errored:
            messages = e.get("diagnostics") or [f"import rejected '{e.get('name')}'"]
            diagnostics.extend({"message": m, "line": 0, "column": 0, "severity": "Error"} for m in messages)
        raise _diagnostics_error(sql_text, diagnostics)

    return client.table(table_name, key=key, timeout=timeout, flush_ms=flush_ms)


def list_adhoc(http) -> pd.DataFrame:
    from . import tables as _tables

    rows = [t for t in _tables.list_tables(http) if str(t.get("name", "")).startswith(ADHOC_PREFIX)]
    rows.sort(key=lambda t: t.get("updatedAtMs") or 0, reverse=True)
    return pd.DataFrame(rows)


def drop_adhoc(http, name: str) -> bool:
    from . import tables as _tables

    if not name.startswith(ADHOC_PREFIX):
        raise StreamsForgeError(f"refusing to drop non-ad-hoc table '{name}'")
    match = _tables.get_table(http, name)
    if match is None:
        return False
    resp = http.delete(f"/api/tables/{match['id']}")
    if resp.status_code == 404:
        return False
    if resp.status_code not in (200, 204):
        raise StreamsForgeError(f"drop '{name}' failed: {resp.status_code} {resp.text}")
    return True
