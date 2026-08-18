/**
 * StreamForgeProvider -- owns exactly one `@streamforge/client` `Client` for the subtree below
 * it, so every `useLiveTable`/`useLiveSql`/`useTables` call in that subtree shares one
 * connection instead of each hook independently reconnecting (each `connect()` handshakes a
 * transport, per connect.ts -- doing that once per table would be wasteful and, worse, would
 * mean N independent reconnect/backoff loops instead of one).
 *
 * Two races this file exists to close, both inherent to "an async connect() driven by an effect":
 *
 *  1. StrictMode / a fast options change fires effect -> cleanup -> effect again before the first
 *     `connect()` has resolved. The first call's `Client` still lands eventually; nothing in this
 *     component's lifetime wants it by then. `cancelled` is set by the cleanup that already ran,
 *     and the `.then` handler checks it and closes the orphan instead of calling setState (a
 *     setState after unmount/after superseding effect would either warn or, worse, resurrect a
 *     connection nothing references).
 *  2. Inline props (`<StreamForgeProvider url={url} user={user}>`) build a fresh `ConnectOptions`
 *     object every render. Keying the effect on that object's identity would reconnect on every
 *     parent re-render. Keying it on the *values* of the primitive fields that actually matter to
 *     `connect()` (see `optionsKey` below) instead makes the effect re-run only when the caller's
 *     intent actually changed.
 */

import { createContext, useContext, useEffect, useMemo, useState } from "react";
import type { ReactElement, ReactNode } from "react";
import { connect } from "@streamforge/client";
import type { Client, ConnectOptions } from "@streamforge/client";

export interface StreamForgeProviderProps extends ConnectOptions {
  children: ReactNode;
  /** Optional pre-built client. When given, the provider does NOT connect or close it -- the caller owns its lifetime. */
  client?: Client;
}

export interface StreamForgeStatus {
  client: Client | null; // null until connected
  connecting: boolean;
  error: Error | null;
}

const StreamForgeContext = createContext<StreamForgeStatus | null>(null);

/** connect()'s entire behavior is determined by these primitive fields -- see ConnectOptions in
 * clients/typescript/src/index.ts. Stringifying them (rather than the ConnectOptions object
 * itself) gives the effect below a dependency that's stable across renders that don't actually
 * change what would be connected to. */
function optionsKey(opts: ConnectOptions): string {
  return JSON.stringify([opts.url, opts.grpc, opts.user, opts.password, opts.token, opts.ingestKey, opts.transport, opts.verify]);
}

export function StreamForgeProvider(props: StreamForgeProviderProps): ReactElement {
  const { children, client: providedClient, ...connectOptions } = props;
  const key = useMemo(
    () => optionsKey(connectOptions),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- optionsKey reads exactly these fields
    [connectOptions.url, connectOptions.grpc, connectOptions.user, connectOptions.password, connectOptions.token, connectOptions.ingestKey, connectOptions.transport, connectOptions.verify],
  );

  const [status, setStatus] = useState<StreamForgeStatus>(() =>
    providedClient ? { client: providedClient, connecting: false, error: null } : { client: null, connecting: true, error: null },
  );

  useEffect(() => {
    if (providedClient) {
      // Caller-owned client: reflect it, connect nothing, and there is nothing of ours to close
      // on cleanup.
      setStatus({ client: providedClient, connecting: false, error: null });
      return;
    }

    let cancelled = false;
    let connected: Client | null = null;
    setStatus({ client: null, connecting: true, error: null });

    connect(connectOptions).then(
      (c) => {
        if (cancelled) {
          void c.close(); // arrived after cleanup already ran -- see race (1) above
          return;
        }
        connected = c;
        setStatus({ client: c, connecting: false, error: null });
      },
      (err: unknown) => {
        if (cancelled) return;
        setStatus({ client: null, connecting: false, error: err instanceof Error ? err : new Error(String(err)) });
      },
    );

    return () => {
      cancelled = true;
      if (connected) void connected.close();
    };
    // key captures every field connect() reads; providedClient switches between the two modes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, providedClient]);

  return <StreamForgeContext.Provider value={status}>{children}</StreamForgeContext.Provider>;
}

function useStreamForgeContext(): StreamForgeStatus {
  const ctx = useContext(StreamForgeContext);
  if (ctx === null) {
    throw new Error("useStreamForge()/useStreamForgeStatus() called outside a <StreamForgeProvider>");
  }
  return ctx;
}

/** Throws a clear Error when used outside a provider. Returns null while still connecting. */
export function useStreamForge(): Client | null {
  return useStreamForgeContext().client;
}

export function useStreamForgeStatus(): StreamForgeStatus {
  return useStreamForgeContext();
}
