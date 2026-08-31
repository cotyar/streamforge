# Contributing

Thanks for looking. This started as a client demo and is now a reference implementation — issues,
questions and small PRs are welcome; large redesigns are probably better as a fork.

## Ground rules

1. **Both runtimes must stay green.** `shared/` is compiled into the Orleans flavor *and* the Dapr
   flavor, so a change there has to pass both suites:
   ```bash
   dotnet test orleans/StreamsForge.sln      # 1488 tests
   dotnet test dapr/StreamsForge.Dapr.sln    # 280 tests
   ```
2. **Never edit an existing test to make a refactor pass.** Behavior-preserving changes keep the
   old assertions green, unmodified. New behavior gets new tests.
3. **Public contracts evolve additively**: `shared/StreamsForge.Engine/PublicApi.cs`, the
   `StreamsForge.Abstractions` members and `web/src/api/types.ts` take the next free `[Id(n)]` or an
   optional field — existing members never change shape or numbering. Dynamic-protobuf field
   numbers persist in the registry and are never reused.
4. **The Engine stays runtime-pure**: no Orleans, Dapr or ASP.NET types inside
   `shared/StreamsForge.Engine` — it is the semantic core both runtimes depend on.

## Local setup

.NET 10 SDK and [bun](https://bun.sh) (never npm — the lockfile is `bun.lock`):

```bash
cd web && bun install && bun run build
dotnet run --project orleans/src/StreamsForge.Host    # :5199, seeds a demo world on first run
```

Delete `orleans/src/StreamsForge.Host/data/` to reseed. The Dapr flavor needs `dapr init` once, then
`dapr/tools/run.sh` (:5399); `dapr/tools/reset.sh` is its reseed.

## Repo map

| Path | What |
|---|---|
| `shared/` | Engine (SQL + dataflow), Contracts, AppCore, Api — everything runtime-agnostic |
| `orleans/` | Orleans 10 host: grains, streams, gRPC, docs site |
| `dapr/` | Dapr host: actors, pub/sub, polyglot processors (Python, TypeScript, Java) |
| `web/` | React 19 + Tailwind 4 + shadcn console, served by both hosts |
| `deploy/` | Dockerfiles, compose stacks, Cloud Run manifests |
| `plans/` | The execution plans this system was actually built from |

`AGENTS.md` documents the conventions in more depth, including the ones that were learned the
expensive way — worth a read before a non-trivial PR.
