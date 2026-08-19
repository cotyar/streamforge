#!/usr/bin/env bun
// StreamForge admin MCP server (plan 013) — stdio transport, hand-written to the MCP specification.
//
// Protocol, in full: newline-delimited JSON-RPC 2.0 on stdin/stdout, with `initialize`,
// `notifications/initialized`, `ping`, `tools/list` and `tools/call`. That is the entire server half
// of a tools-only server, which is why this has no SDK dependency — admin/ has had zero npm
// dependencies since plan 007 and this was not worth breaking that for. Conformance is pinned by
// admin/mcp.test.ts, not by trust.
//
// STDOUT IS THE TRANSPORT. Nothing may be written to it except one JSON-RPC message per line —
// a stray console.log corrupts the stream and the client disconnects. Diagnostics go to stderr.
//
//   bun admin/mcp.ts          # or: bun admin/sf.ts mcp
//
// Claude Code / Claude Desktop config:
//   { "mcpServers": { "streamforge": { "command": "bun",
//       "args": ["/abs/path/to/admin/mcp.ts"],
//       "env": { "SF_URL": "http://localhost:5199", "SF_USER": "admin", "SF_PASSWORD": "..." } } } }

import { APPROVAL_STATES, isKind, KINDS, SfClient, SfError, toApprovalState, type Kind } from "./sfclient.ts";

const SERVER_NAME = "streamforge-admin";
const SERVER_VERSION = "1.0.0";

/** Protocol revisions this server implements, newest first. The spec's rule: echo the client's
 * version when we support it, otherwise answer with our own newest and let the client decide
 * whether it can live with that. */
const SUPPORTED_PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"];

const INSTRUCTIONS = `Administers a running StreamForge instance (either runtime flavor) over its REST API:
catalog, entity lifecycle, SQL validation, rows/results, and config export/import.

Entity ids: sources are addressed by NAME, pipelines and tables by ID. list_entities returns both.
A stopped pipeline or table produces nothing — check status before concluding a query is wrong.

It also READS the plan-015 authorization surface: the access policy, one user's effective permissions,
the approval inbox and the audit log. It cannot write any of it. When an action needs a human's
sign-off, file it with request_approval and say so — there is deliberately no tool here to approve,
reject or cancel a request, because a proposer who can also approve is not a second pair of eyes. A
person decides, in the console or with \`sf approvals approve\`.

Scopes are entity NAMES, never ids. Audit \`action\` filters match by PREFIX. A non-zero \`truncated\`
on an audit page means rows were dropped and the day is incomplete — report it rather than ignoring it.`;

// --- JSON-RPC ------------------------------------------------------------------------------------

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id?: string | number | null;
  method: string;
  params?: Record<string, unknown>;
}

const ERROR_CODES = {
  invalidRequest: -32600,
  methodNotFound: -32601,
  invalidParams: -32602,
  internal: -32603,
  parse: -32700,
} as const;

function send(message: unknown): void {
  // One line, no embedded newlines: the stdio framing IS the newline (MCP stdio transport).
  process.stdout.write(JSON.stringify(message) + "\n");
}

function respond(id: string | number | null | undefined, result: unknown): void {
  send({ jsonrpc: "2.0", id: id ?? null, result });
}

function fail(id: string | number | null | undefined, code: number, message: string): void {
  send({ jsonrpc: "2.0", id: id ?? null, error: { code, message } });
}

function log(message: string): void {
  process.stderr.write(`[${SERVER_NAME}] ${message}\n`);
}

// --- tools ---------------------------------------------------------------------------------------

interface Tool {
  name: string;
  title: string;
  description: string;
  inputSchema: Record<string, unknown>;
  annotations?: Record<string, unknown>;
  run: (args: Record<string, unknown>, client: SfClient) => Promise<unknown>;
}

const kindProperty = {
  type: "string",
  enum: [...KINDS],
  description: "Catalog entity kind. Sources are addressed by name, pipelines and tables by id.",
};

/** Every tool talks to the outside world, so all of them are openWorld; the hints that carry real
 * information are readOnly (safe to call speculatively) and destructive (do not call unasked). */
const READ_ONLY = { readOnlyHint: true, openWorldHint: true };
const MUTATING = { readOnlyHint: false, openWorldHint: true };
const DESTRUCTIVE = { readOnlyHint: false, destructiveHint: true, openWorldHint: true };

function str(args: Record<string, unknown>, key: string, required = true): string {
  const value = args[key];
  if (typeof value === "string" && value.length > 0) return value;
  if (!required) return "";
  throw new SfError(`'${key}' is required`);
}

function kindOf(args: Record<string, unknown>): Kind {
  const raw = str(args, "kind");
  if (!isKind(raw)) throw new SfError(`unknown kind '${raw}' (expected ${KINDS.join(", ")})`);
  return raw;
}

function limitOf(args: Record<string, unknown>, fallback = 100): number {
  const raw = args.limit;
  return typeof raw === "number" && raw > 0 ? Math.floor(raw) : fallback;
}

export const TOOLS: Tool[] = [
  {
    name: "health",
    title: "Instance health",
    description: "Liveness of the StreamForge instance, plus the identity this server authenticates as.",
    inputSchema: { type: "object", properties: {} },
    annotations: READ_ONLY,
    run: async (_args, client) => ({
      url: client.url,
      health: await client.health(),
      identity: await client.me().catch(() => "anonymous (no credentials configured)"),
    }),
  },
  {
    name: "list_entities",
    title: "List catalog entities",
    description: "Every source, pipeline or table in the catalog with its id, name and status.",
    inputSchema: { type: "object", properties: { kind: kindProperty }, required: ["kind"] },
    annotations: READ_ONLY,
    run: (args, client) => client.list(kindOf(args)),
  },
  {
    name: "get_entity",
    title: "Get one entity",
    description: "One entity's full definition — SQL, schema, connector/sink config, status.",
    inputSchema: {
      type: "object",
      properties: { kind: kindProperty, id: { type: "string", description: "Source name, or pipeline/table id." } },
      required: ["kind", "id"],
    },
    annotations: READ_ONLY,
    run: (args, client) => client.get(kindOf(args), str(args, "id")),
  },
  {
    name: "get_metrics",
    title: "Get runtime metrics",
    description: "Live metrics for a pipeline or table; connector runtime status for a source.",
    inputSchema: {
      type: "object",
      properties: { kind: kindProperty, id: { type: "string" } },
      required: ["kind", "id"],
    },
    annotations: READ_ONLY,
    run: (args, client) => client.metrics(kindOf(args), str(args, "id")),
  },
  {
    name: "validate_sql",
    title: "Validate SQL",
    description:
      "Compiles SQL in pipeline or table mode WITHOUT creating anything, returning diagnostics and the output schema. Use before create_entity.",
    inputSchema: {
      type: "object",
      properties: {
        mode: { type: "string", enum: ["pipelines", "tables"], description: "Which execution mode to compile for." },
        sql: { type: "string" },
      },
      required: ["mode", "sql"],
    },
    annotations: READ_ONLY,
    run: (args, client) => {
      const mode = str(args, "mode");
      if (mode !== "pipelines" && mode !== "tables") throw new SfError("mode must be 'pipelines' or 'tables'");
      return client.validate(mode, str(args, "sql"));
    },
  },
  {
    name: "get_rows",
    title: "Read table rows",
    description: "Current rows of a table. Set csv:true for the CSV export instead of JSON.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Table id." },
        limit: { type: "number", description: "Default 100." },
        csv: { type: "boolean" },
      },
      required: ["id"],
    },
    annotations: READ_ONLY,
    run: (args, client) => client.rows(str(args, "id"), limitOf(args), args.csv === true),
  },
  {
    name: "get_results",
    title: "Read pipeline results",
    description: "A pipeline's recent result rows. Set csv:true for the CSV export instead of JSON.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Pipeline id." },
        limit: { type: "number", description: "Default 100." },
        csv: { type: "boolean" },
      },
      required: ["id"],
    },
    annotations: READ_ONLY,
    run: (args, client) => client.results(str(args, "id"), limitOf(args), args.csv === true),
  },
  {
    name: "create_entity",
    title: "Create an entity",
    description:
      "Creates a source, pipeline or table from the same JSON definition the REST API takes. Validate SQL first.",
    inputSchema: {
      type: "object",
      properties: {
        kind: kindProperty,
        definition: { type: "object", description: "The entity definition (name, sql/connector, …)." },
      },
      required: ["kind", "definition"],
    },
    annotations: MUTATING,
    run: (args, client) => {
      if (typeof args.definition !== "object" || args.definition === null) {
        throw new SfError("'definition' must be an object");
      }
      return client.create(kindOf(args), args.definition);
    },
  },
  {
    name: "start_entity",
    title: "Start a pipeline or table",
    description: "Starts execution. Already-running entities are unaffected.",
    inputSchema: {
      type: "object",
      properties: { kind: { ...kindProperty, enum: ["pipelines", "tables"] }, id: { type: "string" } },
      required: ["kind", "id"],
    },
    annotations: { ...MUTATING, idempotentHint: true },
    run: (args, client) => client.lifecycle(kindOf(args), str(args, "id"), "start"),
  },
  {
    name: "stop_entity",
    title: "Stop a pipeline or table",
    description: "Stops execution. A stopped table keeps its rows; a stopped pipeline stops emitting.",
    inputSchema: {
      type: "object",
      properties: { kind: { ...kindProperty, enum: ["pipelines", "tables"] }, id: { type: "string" } },
      required: ["kind", "id"],
    },
    annotations: { ...MUTATING, idempotentHint: true },
    run: (args, client) => client.lifecycle(kindOf(args), str(args, "id"), "stop"),
  },
  {
    name: "delete_entity",
    title: "Delete an entity",
    description: "Permanently removes an entity and its state. Not reversible — confirm with the user first.",
    inputSchema: {
      type: "object",
      properties: { kind: kindProperty, id: { type: "string" } },
      required: ["kind", "id"],
    },
    annotations: DESTRUCTIVE,
    run: async (args, client) => {
      await client.remove(kindOf(args), str(args, "id"));
      return { deleted: str(args, "id") };
    },
  },
  {
    name: "export_config",
    title: "Export the catalog",
    description: "The whole catalog as a portable config document (JSON or YAML). Secrets are masked.",
    inputSchema: {
      type: "object",
      properties: { format: { type: "string", enum: ["json", "yaml"], description: "Default json." } },
    },
    annotations: READ_ONLY,
    run: (args, client) => client.exportConfig(args.format === "yaml" ? "yaml" : "json", false),
  },
  {
    name: "import_config",
    title: "Import a catalog config",
    description:
      "Applies a config document. mode=validate (default) changes nothing and reports what would happen; merge upserts; replace DELETES entities absent from the document.",
    inputSchema: {
      type: "object",
      properties: {
        document: { type: "string", description: "The config document text (JSON or YAML)." },
        mode: { type: "string", enum: ["validate", "merge", "replace"], description: "Default validate." },
      },
      required: ["document"],
    },
    annotations: DESTRUCTIVE,
    run: (args, client) => {
      const mode = typeof args.mode === "string" ? args.mode : "validate";
      if (mode !== "validate" && mode !== "merge" && mode !== "replace") {
        throw new SfError("mode must be validate, merge or replace");
      }
      return client.importConfig(str(args, "document"), mode);
    },
  },

  // ================================================================================================
  // Plan 015 — access, approvals, audit. READ, plus the one write that is a REQUEST.
  //
  // ------------------------------------------------------------------------------------------------
  // Why there is no `approve_request` tool here, and why there must never be one.
  //
  // An approval exists so that a SECOND PAIR OF EYES sees a privileged action before it happens. An
  // agent that can both propose and approve is not a second pair of eyes; it is the same pair twice.
  // Shipping an approve tool would not weaken the mechanism at the margin — it would convert the whole
  // thing into a formality that logs itself, and the log would then read as if a review had occurred.
  // The server has its own half of this rule (ApprovalStateMachine refuses a requester's vote on their
  // own request), but that only stops the SAME identity voting twice; an MCP server configured with an
  // administrator's SF_USER and a human filing through the console are two identities, and the store
  // cannot tell that one of them is a model. So the line is drawn here, at the tool list, which is the
  // only place that knows the caller is an agent.
  //
  // The same reasoning bounds the rest of this block:
  //
  //   * NO access writes. `upsert_role`/`set_disabled`/... would be the entitlement store deciding
  //     what the agent may do — an agent that can edit the policy that governs it is ungoverned, and
  //     one PUT to /api/access/roles/Viewer is the whole distance from "read-only tools" to "anything".
  //     Reading the policy is fine and is genuinely useful: "why was I refused" is answerable from it.
  //   * NO cancel/reject either. They are less dangerous than approve (the state machine refuses
  //     anybody but the requester on a cancel), but an agent withdrawing a request a human is about to
  //     decide on is the same interference with the review, arriving through a politer verb.
  //   * NO `includeChanges` on the audit tool. The before/after payloads can carry stored credential
  //     fields, which is exactly why the server gates them twice; the precedent is right above — the
  //     CLI's `config export --secrets` exists for a human who asked, and export_config never passes it.
  //
  // The CLI is a different case and carries all of it: `sf approvals approve`, `sf access role set`.
  // That is a human at a terminal with their own token, which is what the mechanism is asking for.
  // ------------------------------------------------------------------------------------------------

  {
    name: "get_access_policy",
    title: "Read the access policy",
    description:
      "The whole entitlement document: roles, groups, per-user entries, approval templates and the policy version. Read-only — this server cannot edit the policy. Requires an administrator token.",
    inputSchema: { type: "object", properties: {} },
    annotations: READ_ONLY,
    run: (_args, client) => client.accessPolicy(),
  },
  {
    name: "get_effective_permissions",
    title: "What one user may do",
    description:
      "One user's flattened entitlements — roles, groups, grants — as the authorization decision itself computes them. The answer for a DISABLED user is empty across the board (the server short-circuits), so read the `disabled` flag before concluding the user is configured with nothing.",
    inputSchema: {
      type: "object",
      properties: { username: { type: "string" } },
      required: ["username"],
    },
    annotations: READ_ONLY,
    run: (args, client) => client.effectivePermissions(str(args, "username")),
  },
  {
    name: "list_approvals",
    title: "List approval requests",
    description:
      "Approval requests visible to this token: the ones it filed, the ones it may decide, or everything for an administrator. Optional state filter. Note the server applies `limit` BEFORE that visibility filter, so this is 'your requests among the most recent N', not 'your N most recent'.",
    inputSchema: {
      type: "object",
      properties: {
        state: { type: "string", enum: [...APPROVAL_STATES], description: "Optional; omit for all states." },
        limit: { type: "number", description: "Default 100, server maximum 500." },
      },
    },
    annotations: READ_ONLY,
    run: (args, client) =>
      client.listApprovals(
        typeof args.state === "string" ? toApprovalState(args.state) : undefined,
        limitOf(args),
      ),
  },
  {
    name: "get_approval",
    title: "Read one approval request",
    description: "One approval request in full, including its votes and the groups entitled to decide it.",
    inputSchema: { type: "object", properties: { id: { type: "string" } }, required: ["id"] },
    annotations: READ_ONLY,
    run: (args, client) => client.getApproval(str(args, "id")),
  },
  {
    name: "request_approval",
    title: "Ask a human to approve an action",
    description:
      "Files an approval request for a privileged action and returns its id. This is the tool to reach for when another tool was refused with 'requires approval', or when you are about to propose something a human should sign off on. It DOES NOT execute anything and there is no tool here to approve it — a person decides, in the console or with `sf approvals approve`. Scope is the entity NAME, never its id.",
    inputSchema: {
      type: "object",
      properties: {
        action: { type: "string", description: "The action being asked for, e.g. pipeline.delete." },
        scope: { type: "string", description: "Entity NAME, a `prefix-*`, `tag:x`, or `*` (default)." },
        reason: { type: "string", description: "Why. A human reads this before deciding." },
        payloadJson: {
          type: "string",
          description: "The request that would have executed, serialized. The only replay mechanism there is.",
        },
      },
      required: ["action"],
    },
    // Mutating but never destructive: it creates a pending request and changes nothing else.
    annotations: MUTATING,
    run: (args, client) =>
      client.fileApproval({
        action: str(args, "action"),
        scope: str(args, "scope", false) || "*",
        reason: str(args, "reason", false),
        payloadJson: str(args, "payloadJson", false) || undefined,
      }),
  },
  {
    name: "get_audit_days",
    title: "Which days have audit entries",
    description: "The yyyyMMdd keys that hold audit rows, newest first. The cheap first call before get_audit_day.",
    inputSchema: { type: "object", properties: {} },
    annotations: READ_ONLY,
    run: (_args, client) => client.auditDays(),
  },
  {
    name: "get_audit_day",
    title: "Read one day of the audit log",
    description:
      "One day's audit rows. `actor` is an EXACT match, `action` a PREFIX ('source' matches source.create; 'create' matches nothing). `truncated` in the response counts rows the day shard DROPPED under its cap — a non-zero value means the day is incomplete and should be reported as such, never ignored.",
    inputSchema: {
      type: "object",
      properties: {
        day: { type: "string", description: "yyyyMMdd, UTC. get_audit_days lists the valid ones." },
        actor: { type: "string", description: "Exact username." },
        action: { type: "string", description: "Action prefix." },
        limit: { type: "number", description: "Default 200, server maximum 2000." },
        offset: { type: "number" },
      },
      required: ["day"],
    },
    annotations: READ_ONLY,
    run: (args, client) =>
      client.auditDay(str(args, "day"), {
        actor: str(args, "actor", false) || undefined,
        action: str(args, "action", false) || undefined,
        limit: typeof args.limit === "number" ? Math.floor(args.limit) : undefined,
        offset: typeof args.offset === "number" ? Math.floor(args.offset) : undefined,
        // includeChanges is deliberately not settable — see the block comment above.
      }),
  },
];

/** Secrets never travel through export_config here: the REST API gates includeSecrets on the Admin
 * role, and handing a plaintext credential dump to a model is a worse idea than the role check
 * alone would suggest. The CLI's `config export --secrets` remains, for a human who asked for it. */

// --- request handling -----------------------------------------------------------------------------

export function toolDescriptors(): unknown[] {
  return TOOLS.map(({ name, title, description, inputSchema, annotations }) => ({
    name,
    title,
    description,
    inputSchema,
    annotations,
  }));
}

export async function handle(message: JsonRpcRequest, client: SfClient): Promise<void> {
  const { id, method, params } = message;
  const isNotification = id === undefined;

  switch (method) {
    case "initialize": {
      const requested = typeof params?.protocolVersion === "string" ? params.protocolVersion : "";
      respond(id, {
        protocolVersion: SUPPORTED_PROTOCOL_VERSIONS.includes(requested)
          ? requested
          : SUPPORTED_PROTOCOL_VERSIONS[0],
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: SERVER_NAME, title: "StreamForge Admin", version: SERVER_VERSION },
        instructions: INSTRUCTIONS,
      });
      return;
    }

    case "notifications/initialized":
    case "notifications/cancelled":
      return; // Notifications are never answered.

    case "ping":
      respond(id, {});
      return;

    case "tools/list":
      respond(id, { tools: toolDescriptors() });
      return;

    case "tools/call": {
      const name = typeof params?.name === "string" ? params.name : "";
      const tool = TOOLS.find((t) => t.name === name);
      if (!tool) {
        // A tool that does not exist is a protocol-level mistake by the caller, not a failed
        // execution — the spec's own example answers it with an error, not isError.
        fail(id, ERROR_CODES.invalidParams, `Unknown tool: ${name}`);
        return;
      }

      const args = (params?.arguments ?? {}) as Record<string, unknown>;
      try {
        const result = await tool.run(args, client);
        respond(id, { content: [{ type: "text", text: render(result) }] });
      } catch (err) {
        // Execution failures come back INSIDE the result with isError, so the model can see what
        // went wrong and adjust — an unreachable instance or a 404 is information, not a transport
        // fault. Only the protocol-level cases above use JSON-RPC errors.
        respond(id, {
          content: [{ type: "text", text: err instanceof Error ? err.message : String(err) }],
          isError: true,
        });
      }
      return;
    }

    default:
      if (!isNotification) fail(id, ERROR_CODES.methodNotFound, `Method not found: ${method}`);
  }
}

function render(value: unknown): string {
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

// --- stdio loop ------------------------------------------------------------------------------------

/** Reads newline-delimited JSON-RPC from a stream. Partial reads are normal on a pipe, so a chunk
 * that ends mid-message is buffered until its newline arrives. */
export async function serve(stream: AsyncIterable<Uint8Array>, client: SfClient): Promise<void> {
  const decoder = new TextDecoder();
  let buffer = "";

  for await (const chunk of stream) {
    buffer += decoder.decode(chunk, { stream: true });
    let newline: number;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      if (!line) continue;

      let message: unknown;
      try {
        message = JSON.parse(line);
      } catch {
        fail(null, ERROR_CODES.parse, "Parse error");
        continue;
      }

      if (Array.isArray(message)) {
        // JSON-RPC batching was removed from MCP in the 2025-06-18 revision; saying so beats
        // half-answering a batch.
        fail(null, ERROR_CODES.invalidRequest, "JSON-RPC batches are not supported (removed in MCP 2025-06-18)");
        continue;
      }

      const request = message as JsonRpcRequest;
      if (!request || request.jsonrpc !== "2.0" || typeof request.method !== "string") {
        fail((request as JsonRpcRequest)?.id ?? null, ERROR_CODES.invalidRequest, "Invalid Request");
        continue;
      }

      try {
        await handle(request, client);
      } catch (err) {
        // A throw out of handle() is a bug here, not a tool failure — report it as internal rather
        // than dying, so one broken request cannot take the session down.
        log(`internal error handling ${request.method}: ${err instanceof Error ? err.stack : String(err)}`);
        if (request.id !== undefined) fail(request.id, ERROR_CODES.internal, "Internal error");
      }
    }
  }
}

// Bun runs this both as an entry point and as an import from admin/sf.ts's `mcp` command; in the
// test it is imported for its exports only, which is what import.meta.main distinguishes.
if (import.meta.main || process.argv[1]?.endsWith("sf.ts")) {
  const client = new SfClient();
  log(`ready — ${client.url} (protocol ${SUPPORTED_PROTOCOL_VERSIONS[0]}, ${TOOLS.length} tools)`);
  await serve(process.stdin, client);
}
