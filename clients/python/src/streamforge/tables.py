"""Table catalog reads: list/get/search/history, and snapshot() -- a one-shot REST read with no
subscription and no thread, dropping weight<=0 rows exactly like readTableRows in
lib/streamforge/server.ts. Always REST regardless of which live transport a Client picked --
these are cheap, infrequent calls, and keeping one code path here is simpler than duplicating
them for gRPC (design tradeoff, not a limitation: TableService.List/Rows/Search exist on the gRPC
side too, for a client that wants everything on one channel).
"""

from __future__ import annotations

import pandas as pd

from .errors import StreamForgeError


def list_tables(http) -> list[dict]:
    resp = http.get("/api/tables")
    resp.raise_for_status()
    return resp.json()


def get_table(http, name_or_id: str) -> dict | None:
    for t in list_tables(http):
        if t.get("name") == name_or_id or t.get("id") == name_or_id:
            return t
    return None


def resolve_table_id(http, name: str) -> str:
    t = get_table(http, name)
    if t is None:
        raise StreamForgeError(f"no such table '{name}'")
    return t["id"]


def resolve_key_fields(http, name: str) -> list[str] | None:
    """Wishlist #18: this table's row-identity key, read from its own definition (`GET
    /api/tables`'s `keyFields`) instead of a hand-maintained map. TableDefinition.KeyFields is a
    three-way answer -- a non-empty list is the resolved GROUP BY/LATEST BY key, `[]` is an
    unkeyed global aggregate (one row, one group), and `null` is whole-row identity -- see
    Models.cs's doc comment on the field for the full contract.

    An unknown table and an engine build that doesn't report the field at all (predates wishlist
    #18) both come back here as `None`, which `_zset.group_key_of` already treats as whole-row
    identity -- the exact fallback an unmapped name got from the old, now-deleted `KEY_FIELDS`
    map, so an old engine keeps working exactly as it did before this change. `Client.table()`'s
    `key=` argument bypasses this entirely and always wins."""
    t = get_table(http, name)
    if t is None:
        return None
    return t.get("keyFields")


def search(http, name: str, query: str, limit: int = 50) -> list[dict]:
    table_id = resolve_table_id(http, name)
    resp = http.get(f"/api/tables/{table_id}/search", params={"q": query, "limit": limit})
    resp.raise_for_status()
    body = resp.json()
    return [r["row"] for r in body.get("rows", []) if r.get("weight", 1) > 0]


def history(http, name: str, lookup: dict, limit: int | None = None) -> list[dict]:
    table_id = resolve_table_id(http, name)
    params = {"limit": limit} if limit is not None else None
    resp = http.post(f"/api/tables/{table_id}/history/lookup", json=lookup, params=params)
    resp.raise_for_status()
    return resp.json()


def snapshot(http, name: str, limit: int = 500) -> pd.DataFrame:
    table_id = resolve_table_id(http, name)
    resp = http.get(f"/api/tables/{table_id}/rows", params={"limit": limit})
    resp.raise_for_status()
    body = resp.json()
    rows = [r["row"] for r in body.get("rows", []) if r.get("weight", 1) > 0]
    return pd.DataFrame(rows)
