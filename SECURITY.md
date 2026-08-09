# Security policy

## Reporting a vulnerability

Please report security issues privately through
[GitHub's private vulnerability reporting](https://github.com/cotyar/streamforge/security/advisories/new)
rather than a public issue. Expect a first response within a few days; this is a side project, not
a vendor with an on-call rotation.

## What this project is — and is not

StreamForge is a **reference implementation and demo**, not a hardened production system. Before
pointing it at anything real, know that:

- **The seeded demo users are public knowledge** (`admin/admin123!`, `editor/editor123!`,
  `viewer/viewer123!`) and are created automatically into an empty data directory. Any deployment
  reachable by other people needs them removed or changed.
- **JWT signing key**: HS256 with a development key unless you set your own via configuration.
  Set one before exposing the API.
- **No transport security of its own** — run it behind a TLS-terminating proxy. The gRPC endpoint
  is cleartext h2c by design (`:5299`).
- **The AI control chat can mutate the catalog.** `POST /api/chat` gives the model function-calling
  access to create, edit, start and stop sources, pipelines and tables under the caller's role. A
  publicly reachable instance with `GEMINI_API_KEY` set hands those capabilities to whoever can log
  in. Don't expose it without restricting the role or the endpoint.
- **State is process-local.** Both flavors keep the working set in memory (Orleans grains / Dapr
  actors); there is no durability, replication or recovery story worth relying on.

Known-by-design limitations are documented in
[`orleans/DESIGN.md`](orleans/DESIGN.md) and the docs site's "Limits" section.
