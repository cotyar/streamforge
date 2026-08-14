#!/usr/bin/env bun
// `sf` — the StreamForge admin CLI (plan 013). Everything an operator does to a RUNNING instance
// through the console, from a terminal instead: health, catalog, lifecycle, SQL validation,
// rows/results (incl. plan 012's CSV), config export/import.
//
// Zero npm dependencies (admin/ house rule since plan 007). All API knowledge lives in sfclient.ts,
// which admin/mcp.ts shares — the CLI and the MCP server cannot drift about what a command means.
//
//   bun admin/sf.ts health
//   SF_URL=http://localhost:5399 bun admin/sf.ts ls tables     # the Dapr flavor, same commands

import { isKind, KINDS, SfClient, SfError, writeStoredToken, type Kind } from "./sfclient.ts";

const USAGE = `sf — StreamForge admin CLI

  sf health                                  instance health + who this token is
  sf login [--user U] [--password P]         store a token in ~/.streamforge/token.json (0600)
  sf ls <${KINDS.join("|")}>            one line per entity (--json for the raw array)
  sf get <kind> <id>                         one entity's full definition
  sf start|stop <pipelines|tables> <id>      lifecycle
  sf create <kind> -f <def.json>             create from the JSON the REST API takes
  sf delete <kind> <id> [--yes]              delete (asks first unless --yes)
  sf rows <table-id> [--csv] [--limit N]     a table's rows
  sf results <pipeline-id> [--csv] [--limit N]
  sf validate <pipelines|tables> "<sql>"     compile without creating anything
  sf config export [--yaml] [--secrets] [-o file]
  sf config import <file> [--mode validate|merge|replace]
  sf api <METHOD> <path> [body.json]         escape hatch for anything not above
  sf mcp                                     run the MCP server on stdio (same as bun admin/mcp.ts)

Environment: SF_URL (default http://localhost:5199), SF_TOKEN, SF_USER, SF_PASSWORD.
Global flags: --url URL, --token T, --json.
`;

interface Args {
  positional: string[];
  flags: Record<string, string | boolean>;
}

/** --flag value | --flag=value | --boolean-flag | -f file | -o file. Deliberately tiny: this CLI's
 * whole grammar is "verb noun", and a parser library for that would be the joke about dependencies. */
function parseArgs(argv: string[]): Args {
  const positional: string[] = [];
  const flags: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "-f" || arg === "-o") {
      flags[arg === "-f" ? "file" : "out"] = argv[++i] ?? "";
    } else if (arg.startsWith("--")) {
      const [name, inline] = arg.slice(2).split("=", 2);
      if (inline !== undefined) flags[name] = inline;
      else if (argv[i + 1] && !argv[i + 1].startsWith("-")) flags[name] = argv[++i];
      else flags[name] = true;
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function out(value: unknown, json: boolean): void {
  if (typeof value === "string") console.log(value);
  else console.log(JSON.stringify(value, null, json ? 0 : 2));
}

function requireKind(raw: string | undefined): Kind {
  if (!raw || !isKind(raw)) throw new SfError(`expected one of ${KINDS.join(", ")}, got '${raw ?? ""}'`);
  return raw;
}

/** A definition's addressable id — sources go by name, pipelines/tables by id (see sfclient KINDS). */
function idOf(entity: Record<string, unknown>): string {
  return String(entity.id ?? entity.name ?? "");
}

function summarize(kind: Kind, entity: Record<string, unknown>): string {
  const state = kind === "sources"
    ? (entity.enabled ? "enabled" : "disabled")
    : String(entity.status ?? "?");
  const extra = kind === "sources" ? String(entity.kind ?? "") : String(entity.error ?? "");
  return [idOf(entity).padEnd(34), String(entity.name ?? "").padEnd(24), state.padEnd(9), extra]
    .join(" ")
    .trimEnd();
}

async function confirm(question: string): Promise<boolean> {
  process.stdout.write(`${question} [y/N] `);
  for await (const line of console) return line.trim().toLowerCase() === "y";
  return false;
}

/** Reads a password without echoing it. Falls back to a plain read when stdin is not a TTY (a pipe,
 * a CI runner) — refusing there would break `echo pw | sf login` for no security gain, since a pipe
 * has no terminal echo to leak in the first place. */
async function readPassword(): Promise<string> {
  process.stdout.write("password: ");
  const tty = process.stdin.isTTY;
  if (tty) process.stdin.setRawMode?.(true);
  let secret = "";
  if (tty) {
    for await (const chunk of process.stdin) {
      const text = new TextDecoder().decode(chunk as Uint8Array);
      if (text.includes("\r") || text.includes("\n")) {
        secret += text.split(/[\r\n]/)[0];
        break;
      }
      secret += text;
    }
    process.stdin.setRawMode?.(false);
  } else {
    for await (const line of console) {
      secret = line;
      break;
    }
  }
  process.stdout.write("\n");
  return secret.trim();
}

async function main(argv: string[]): Promise<number> {
  const { positional, flags } = parseArgs(argv);
  const command = positional[0];
  if (!command || flags.help || command === "help") {
    console.log(USAGE);
    return command ? 0 : 1;
  }

  // `sf mcp` hands over to the MCP server in-process: one binary to remember, and the server's
  // stdout stays the only thing this process writes (see mcp.ts on why that matters).
  if (command === "mcp") {
    await import("./mcp.ts");
    return 0;
  }

  const json = flags.json === true;
  const client = new SfClient({
    url: typeof flags.url === "string" ? flags.url : undefined,
    token: typeof flags.token === "string" ? flags.token : undefined,
    user: typeof flags.user === "string" ? flags.user : undefined,
  });

  switch (command) {
    case "health": {
      out(await client.health(), json);
      // Anonymous is a legitimate state for /healthz, so an unauthenticated caller gets the health
      // and a note, not a failure.
      try {
        out(await client.me(), json);
      } catch {
        console.log("(not authenticated — run `sf login` for the identity line)");
      }
      return 0;
    }

    case "login": {
      const user = typeof flags.user === "string" ? flags.user : process.env.SF_USER;
      if (!user) throw new SfError("--user is required (or set SF_USER)");
      const password = typeof flags.password === "string"
        ? flags.password
        : process.env.SF_PASSWORD ?? (await readPassword());
      const stored = await client.login(user, password);
      const file = writeStoredToken(stored);
      console.log(`logged in to ${stored.url} as ${stored.username} (${stored.role}) — token in ${file}`);
      return 0;
    }

    case "ls": {
      const kind = requireKind(positional[1]);
      const entities = (await client.list(kind)) as Record<string, unknown>[];
      if (json) return out(entities, true), 0;
      if (entities.length === 0) console.log(`(no ${kind})`);
      for (const entity of entities) console.log(summarize(kind, entity));
      return 0;
    }

    case "get":
      out(await client.get(requireKind(positional[1]), required(positional[2], "id")), json);
      return 0;

    case "start":
    case "stop":
      out(await client.lifecycle(requireKind(positional[1]), required(positional[2], "id"), command), json);
      return 0;

    case "create": {
      const kind = requireKind(positional[1]);
      const file = typeof flags.file === "string" ? flags.file : undefined;
      if (!file) throw new SfError("-f <def.json> is required");
      const definition = JSON.parse(await Bun.file(file).text()) as unknown;
      out(await client.create(kind, definition), json);
      return 0;
    }

    case "delete": {
      const kind = requireKind(positional[1]);
      const id = required(positional[2], "id");
      if (flags.yes !== true && !(await confirm(`delete ${kind.slice(0, -1)} '${id}'?`))) {
        console.log("aborted");
        return 1;
      }
      await client.remove(kind, id);
      console.log(`deleted ${kind.slice(0, -1)} '${id}'`);
      return 0;
    }

    case "rows":
      out(await client.rows(required(positional[1], "table id"), limitOf(flags), flags.csv === true), json);
      return 0;

    case "results":
      out(await client.results(required(positional[1], "pipeline id"), limitOf(flags), flags.csv === true), json);
      return 0;

    case "validate": {
      const kind = requireKind(positional[1]);
      if (kind === "sources") throw new SfError("validate applies to pipelines and tables");
      out(await client.validate(kind, required(positional[2], "sql")), json);
      return 0;
    }

    case "config": {
      const sub = positional[1];
      if (sub === "export") {
        const text = await client.exportConfig(flags.yaml === true ? "yaml" : "json", flags.secrets === true);
        if (typeof flags.out === "string") {
          await Bun.write(flags.out, text);
          console.log(`wrote ${flags.out}`);
        } else {
          console.log(text);
        }
        return 0;
      }
      if (sub === "import") {
        const file = required(positional[2], "file");
        const mode = (typeof flags.mode === "string" ? flags.mode : "validate") as
          "validate" | "merge" | "replace";
        out(await client.importConfig(await Bun.file(file).text(), mode), json);
        return 0;
      }
      throw new SfError("expected `config export` or `config import <file>`");
    }

    case "api": {
      const method = required(positional[1], "method").toUpperCase();
      const path = required(positional[2], "path");
      const body = positional[3] ? JSON.parse(await Bun.file(positional[3]).text()) : undefined;
      out(await client.request(method, path.startsWith("/") ? path : `/${path}`, { body }), json);
      return 0;
    }

    default:
      console.error(`unknown command '${command}'\n`);
      console.log(USAGE);
      return 1;
  }
}

function required(value: string | undefined, what: string): string {
  if (!value) throw new SfError(`missing <${what}>`);
  return value;
}

function limitOf(flags: Record<string, string | boolean>): number {
  const raw = typeof flags.limit === "string" ? Number(flags.limit) : NaN;
  return Number.isFinite(raw) && raw > 0 ? raw : 100;
}

try {
  process.exit(await main(process.argv.slice(2)));
} catch (err) {
  // An SfError is an expected outcome (a 404, an unreachable host, a missing argument) and gets one
  // clean line; anything else is a bug here and keeps its stack.
  if (err instanceof SfError) {
    console.error(`error: ${err.message}`);
    process.exit(1);
  }
  throw err;
}
