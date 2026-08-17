/**
 * push(source, rows, ...) -- gRPC bidi when the client's live transport is gRPC (real HTTP/2
 * backpressure, design doc §3.1), REST POST /api/sources/{name}/events otherwise (ported from
 * pushEvents in otc-terms' lib/streamforge/server.ts via clients/python/src/streamforge/ingest.py).
 * Prefers an ingest key over the admin JWT when one is configured, so a caller that only feeds a
 * source never needs to hold one (design doc §4) -- the REST route is anonymous with its own dual
 * check, and the gRPC IngestService checks the same header per-message.
 */

import { IngestRejected } from "./errors.js";
import type { RestClient } from "./http.js";
import type { Row } from "./zset.js";
import type { IngestAcceptedResponse, IngestErrorResponse } from "./types.js";

export interface IngestAckDto {
  outcome: string;
  accepted: number;
  dropped: number;
  invalid: number;
  error?: string;
  rowErrors?: string[];
  [key: string]: unknown;
}

export interface GrpcIngestCapable {
  ingest(sourceName: string, rows: Row[], idempotencyKey: string | undefined, partial: boolean): Promise<IngestAckDto>;
}

export interface PushDeps {
  http: RestClient;
  grpc: GrpcIngestCapable | null;
  ingestKey: string | undefined;
}

export interface PushOptions {
  idempotencyKey?: string;
  partial?: boolean;
}

export async function push(deps: PushDeps, source: string, rows: Row[], opts: PushOptions = {}): Promise<unknown> {
  const { idempotencyKey, partial = false } = opts;
  if (deps.grpc !== null) return pushGrpc(deps.grpc, source, rows, idempotencyKey, partial);
  return pushRest(deps.http, deps.ingestKey, source, rows, idempotencyKey, partial);
}

async function pushGrpc(
  grpc: GrpcIngestCapable,
  source: string,
  rows: Row[],
  idempotencyKey: string | undefined,
  partial: boolean,
): Promise<IngestAckDto> {
  const ack = await grpc.ingest(source, rows, idempotencyKey, partial);
  if (ack.outcome !== "INGEST_OUTCOME_ACCEPTED" && ack.outcome !== "ACCEPTED") {
    throw new IngestRejected(ack.error || `${source} ingest push rejected: ${ack.outcome}`, ack.rowErrors ?? []);
  }
  return ack;
}

async function pushRest(
  http: RestClient,
  ingestKey: string | undefined,
  source: string,
  rows: Row[],
  idempotencyKey: string | undefined,
  partial: boolean,
): Promise<IngestAcceptedResponse> {
  const useIngestKey = Boolean(ingestKey);
  const headers: Record<string, string> = useIngestKey ? { "X-SF-Ingest-Key": ingestKey! } : {};
  const body: Record<string, unknown> = { events: rows, partial };
  if (idempotencyKey) body.idempotencyKey = idempotencyKey;

  // auth=false when pushing with an ingest key: the route is anonymous with its own header
  // check, and we must not force an admin login just to attach a Bearer token nobody asked for
  // (design doc §4's "never holds an admin JWT" ask).
  const res = await http.request("POST", `/api/sources/${encodeURIComponent(source)}/events`, {
    body,
    headers,
    auth: !useIngestKey,
  });
  const parsed = (res.headers.get("content-length") === "0" ? {} : await res.json().catch(() => ({}))) as Partial<
    IngestAcceptedResponse & IngestErrorResponse
  >;
  if (res.status !== 202) {
    throw new IngestRejected(parsed.error ?? `${source} ingest push failed: ${res.status}`, parsed.rowErrors ?? []);
  }
  return parsed as IngestAcceptedResponse;
}
