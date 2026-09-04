# 024 — TLS: native HTTPS/gRPC-TLS on the Orleans host, outbound trust, every client SDK

Status: **DONE on Orleans** (2026-09-04, two waves, 2 + 5 agents plus orchestrator fixes). **Dapr
deliberately out of scope** — the Dapr host is still loopback-only and has no `Tls:Enabled` wiring;
its sidecar mTLS is a separate concern. `shared/` changes leave it byte-identical in behaviour.

## Why

`SECURITY.md` said "no transport security of its own — run it behind a TLS-terminating proxy". A
survey before this plan found the gap was wider than the sentence: the two Kestrel listeners were
bound in code with no `UseHttps`; nothing set the request scheme behind a proxy, so
`/api/meta/instance` told federation peers `http://` even when the world saw `https://`; no
`ServerCertificateCustomValidationCallback` existed anywhere, so a private CA on a `url` source or
a peer meant "make the OS trust it"; NATS had no CA/client-cert fields; and all four client SDKs
hard-wired plaintext gRPC (`createInsecure()`, `$"http://{target}"`, `insecure_channel`,
`usePlaintext()`) with a scheme-less `host:port` target that had nowhere to even say "TLS". The
user asked for the full version, minus Dapr.

## Decisions

| # | Decision | Instead of |
|---|---|---|
| D1 | One flag, `Tls:Enabled`, puts TLS on BOTH listeners; the certificate is the standard `Kestrel:Certificates:Default` section Kestrel already parses, so `listenOptions.UseHttps()` takes no arguments. Startup fails fast when the flag is set with no `Path`/`Subject`. | A per-port flag (a TLS REST port next to a cleartext gRPC port is a shape nobody asked for) or a StreamsForge-specific certificate section. |
| D2 | `--urls` deployments (Docker, Cloud Run) turn TLS on through `--urls https://…` plus the same certificate section, never `Tls:Enabled` — and with real TLS that single port serves gRPC too (ALPN), closing wishlist #19's remaining gap. | Teaching `Tls:Enabled` to rewrite the URL scheme. |
| D3 | HSTS under `Tls:Enabled`, with ASP.NET's default `ExcludedHosts` (loopback) kept: HSTS is per host and ignores the port, so emitting it for `localhost` would poison every cleartext `http://localhost:<anything>` in that browser for the whole max-age. | Overriding the exclusion so the test could see the header on 127.0.0.1. |
| D4 | Behind a proxy: the built-in `ASPNETCORE_FORWARDEDHEADERS_ENABLED=true` (verified live: it registers the ForwardedHeaders filter trusting any proxy). Default off = fail-closed. | Our own middleware plus a `KnownProxies` option. |
| D5 | Outbound trust is one static class, `OutboundTls` (`shared/StreamsForge.AppCore/Net/`): `Tls:TrustedCaPath` EXTENDS system trust (`SslPolicyErrors.None` short-circuits before the custom chain, because `CustomRootTrust` REPLACES system roots), a name mismatch still fails, `Tls:AcceptAnyCertificate` is dev-only and warned at startup. The five static `HttpClient`s and the gRPC channel factory all take `OutboundTls.NewHandler()`, lazily, and the class latches on first handler creation. | A per-source `insecureSkipVerify` on every kind that dials out. |
| D6 | NATS gets a `tls` config group (`caFile`/`certFile`/`keyFile`/`insecureSkipVerify`) — additive ids 9 (`NatsSubConfig`) and 6 (`NatsPubConfig`), `Mode = Require` only when something is set so a `tls://` URL keeps deciding on its own. | Reusing `Tls:TrustedCaPath` for NATS (NATS.Net has its own TLS options, and client certificates are a per-broker thing). |
| D7 | Every SDK: the gRPC target may carry a scheme — `host:port` and `http://host:port` are plaintext (unchanged), `https://host:port` is TLS; the `PORT+100` guess preserves an https REST URL's scheme. One CA-file option and one dev-only skip-verify per SDK, applied to REST, gRPC and SignalR alike. | A separate boolean `tls` option next to a scheme-less target (two places to disagree). |
| D8 | Python's `verify=False` stays REST/SignalR-only — grpc-python cannot skip verification — and the combination `verify=False` + https gRPC target + no `ca` raises a `ValueError` up front. | Failing later with an opaque handshake error. |
| D9 | `tools/tls/dev-cert.sh` mints a self-signed cert with SAN `localhost` + `127.0.0.1` that is its own trust anchor; every test fixture in four languages uses it. | Committing a private key to the repository. |

## Waves

- **Wave 1** — H (Opus): `Program.cs`, `OutboundTls`, HSTS, five outbound clients, the dev-cert
  script, `HostProcess` additions, `TlsHostTests` / `TlsStartupFailureTests` / `TlsChainTests` /
  `ForwardedHeadersTests`, `SECURITY.md`. N (Sonnet): NATS TLS (contracts, settings, descriptors,
  tests, `TRANSPORTS.md`).
- **Wave 2** — T (TypeScript + admin CLI), C (.NET), P (Python), K (Kotlin), D (docs), all Sonnet,
  in parallel worktrees.
- **Orchestrator**: the plan-022 publish bug below, the .NET/Kotlin fixture patches, `AGENTS.md`,
  this record, final gates.

## Acceptance criteria — outcomes

- `TlsHostTests`: https healthz + login, `/api/meta/instance` reports `https://` for both endpoints,
  HSTS present for a non-loopback `Host`, absent on loopback, plain `http://` to the TLS port fails.
  `TlsStartupFailureTests`: `Tls:Enabled` without a certificate exits non-zero within 30 s and the
  log names `Kestrel:Certificates:Default:Path`, `:Subject` and the dev-cert script. **Green.**
- `TlsChainTests`: host A on TLS, host B plain with `Tls:TrustedCaPath` = A's cert and
  `Discovery:Peers` pointing at `https://` endpoints; folder source on A → `grpc` source on B by
  peer name → table on B; 200/200 rows, seq 0..199. **Green** — this is the end-to-end proof of the
  TLS gRPC listener, the peer directory over https and outbound trust together.
- `ForwardedHeadersTests`: with the flag `X-Forwarded-Proto: https` flips the reported scheme;
  without the flag the header is ignored. **Green.**
- `OutboundTlsTests` (19) and `NatsTlsSettingsTests` (5) + field-number pins. **Green.**
- One live TLS suite per SDK against a real TLS host (gRPC and SignalR/websocket transports, plus
  the negative "no CA → rejected" case): TypeScript (`tls-live.test.ts`, 4), .NET (`TlsTests`),
  Python (`test_tls.py`, 3 live + 12 unit), Kotlin (`TlsTest`). Results per suite are in the final
  report below.
- Live curl on 6599 with the dev cert: `curl -sk https://…/api/healthz` → 200; plain http → empty
  reply.

## Found and fixed on the way (not TLS)

- **Every client fixture published an unrunnable host on macOS since plan 022.** `Publish.props`
  defaulted a bare `dotnet publish` (no `-r`) to `linux-x64`, self-contained single-file — so the
  TypeScript, .NET, Python and Kotlin fixtures, which publish the host into a temp dir, produced a
  Linux executable and no `StreamsForge.Host.dll` (`Exec format error`). Nobody had run those suites
  since 022 landed. Fixed at the source: the default is now `$(NETCoreSdkRuntimeIdentifier)`
  (`tools/publish.sh` and both Dockerfiles pass `-r` explicitly, unchanged), and each fixture runs
  the native executable when there is no `.dll`. The committed Python `streamsforge_pb2.py` also
  failed to parse (stale against the proto's `key_fields`) and was regenerated.
- Four concurrent `dotnet publish`es of the same tree on an already loaded machine take longer
  than the fixtures' 5-minute timeout, which surfaced as 21 spurious "skips" in one .NET run. All
  four fixtures honour `SF_TEST_PUBLISH_DIR`; the gates below were run against ONE shared publish.

## Found and not fixed

- `Tls:Enabled` is only honoured in the two-listener branch; `--urls http://… --Tls:Enabled true`
  yields HSTS on a cleartext listener with no warning (inert — browsers ignore it — but silent).
- `DocsAuthCookie`'s hand-rolled `X-Forwarded-Proto` read is now redundant when the built-in filter
  is enabled and is still the only thing that works when it is not; left as is.
- Python: no way to skip gRPC verification; pass the server cert as `ca=` for a self-signed setup.
- TypeScript SignalR over a private CA needs `NODE_EXTRA_CA_CERTS` at process start —
  `@microsoft/signalr`'s Node transport takes no CA option, and Bun bakes that variable into the
  trust store at startup (setting it mid-process has no effect; the live test spawns a child).
- Dapr: everything here (see status line).
- The `mssql`/`postgres` kinds already had TLS flags and were not touched; `fix` sessions keep
  QuickFIX's own `SSL*` settings.
