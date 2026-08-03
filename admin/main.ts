// StreamForge — cluster admin app (plan 007 D-C).
//
// Single Bun.serve() process, ZERO npm dependencies. Fires up either flavor's containerized stack
// on command, shows live health, shuts it down. Two drivers behind the same start/status/stop/logs
// API, selected by MODE:
//   local    (default) — shells out to `docker compose -f deploy/<flavor>/compose.yaml ...`
//   cloudrun            — shells out to `gcloud run services update|describe ...`
//
// This app NEVER binds, signals, or health-probes the local dev servers on 5199/5299/5399 (see
// CLAUDE.md) — it only ever touches the containerized compose stacks (host ports 6199 orleans /
// 6399 dapr) or Cloud Run services. See README.md for the full usage/env var rundown.
//
// Style note: zero-dep bun house style mirrors dapr/processors/ts-consumer/main.ts (plain
// Bun.serve, no framework, small color helpers for terminal logging).

const ADMIN_PORT = Number(process.env.ADMIN_PORT ?? 5599);
const MODE = (process.env.MODE ?? "local") as "local" | "cloudrun";
const REGION = process.env.REGION ?? "europe-west1";

// REPO_ROOT: admin/main.ts lives at <repo>/admin/main.ts — go up one directory.
const REPO_ROOT = new URL("..", import.meta.url).pathname.replace(/\/$/, "");

// --- tiny ANSI color helpers (zero deps, matches ts-consumer house style) -----------------------
const color = {
  reset: "\x1b[0m",
  dim: (s: string) => `\x1b[2m${s}\x1b[0m`,
  bold: (s: string) => `\x1b[1m${s}\x1b[0m`,
  cyan: (s: string) => `\x1b[36m${s}\x1b[0m`,
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  red: (s: string) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s: string) => `\x1b[33m${s}\x1b[0m`,
  gray: (s: string) => `\x1b[90m${s}\x1b[0m`,
};

// --- flavor allowlist (exact two values — never shell-interpolate anything wider than this) -----
const FLAVORS = ["orleans", "dapr"] as const;
type Flavor = (typeof FLAVORS)[number];
function isFlavor(x: string | null): x is Flavor {
  return x === "orleans" || x === "dapr";
}

const COMPOSE_FILE: Record<Flavor, string> = {
  orleans: `${REPO_ROOT}/deploy/orleans/compose.yaml`,
  dapr: `${REPO_ROOT}/deploy/dapr/compose.yaml`,
};
const LOCAL_PORT: Record<Flavor, number> = { orleans: 6199, dapr: 6399 };
const CLOUD_RUN_SERVICE: Record<Flavor, string> = {
  orleans: "streamforge-orleans",
  dapr: "streamforge-dapr",
};

// PROJECT_ID resolution is deferred (cached) — only ever needed in cloudrun mode.
let cachedProjectId: string | null | undefined;
async function projectId(): Promise<string | null> {
  if (process.env.PROJECT_ID) return process.env.PROJECT_ID;
  if (cachedProjectId !== undefined) return cachedProjectId;
  const r = await run(["gcloud", "config", "get-value", "project"]);
  const v = r.stdout.trim();
  cachedProjectId = r.code === 0 && v && v !== "(unset)" ? v : null;
  return cachedProjectId;
}

// --- process + fetch helpers ---------------------------------------------------------------------
interface RunResult {
  code: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}

/** Shells out via Bun.spawn — cmd is always a fixed argv array built from constants above plus a
 * value already validated against FLAVORS; nothing here ever concatenates raw user input into a
 * shell string (Bun.spawn takes argv directly, no shell involved either way). */
async function run(cmd: string[], opts: { timeoutMs?: number } = {}): Promise<RunResult> {
  const proc = Bun.spawn(cmd, {
    cwd: REPO_ROOT,
    stdout: "pipe",
    stderr: "pipe",
    env: process.env as Record<string, string>,
  });
  let timedOut = false;
  const timer = opts.timeoutMs
    ? setTimeout(() => {
        timedOut = true;
        proc.kill();
      }, opts.timeoutMs)
    : undefined;
  const [stdout, stderr] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  const code = await proc.exited;
  if (timer) clearTimeout(timer);
  return { code, stdout, stderr, timedOut };
}

async function fetchWithTimeout(url: string, ms: number): Promise<Response> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, { signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

// --- status -----------------------------------------------------------------------------------
type Health = "ok" | "down" | "starting";
interface StatusEntry {
  mode: "local" | "cloudrun";
  running: boolean | "unknown";
  health: Health;
  url: string | null;
  detail: string;
}

async function healthzOf(url: string, timeoutMs: number): Promise<{ ok: boolean; detail: string }> {
  try {
    const res = await fetchWithTimeout(`${url}/healthz`, timeoutMs);
    const body = await res.text();
    return { ok: res.ok, detail: res.ok ? body.slice(0, 300) : `healthz returned HTTP ${res.status}` };
  } catch (e) {
    return { ok: false, detail: `healthz unreachable: ${(e as Error).message ?? e}` };
  }
}

async function localStatus(flavor: Flavor): Promise<StatusEntry> {
  const file = COMPOSE_FILE[flavor];
  const ps = await run(["docker", "compose", "-f", file, "ps", "-q"]);
  const running = ps.code === 0 && ps.stdout.trim().length > 0;
  const url = `http://localhost:${LOCAL_PORT[flavor]}`;
  const hz = await healthzOf(url, 2000);
  const health: Health = hz.ok ? "ok" : running ? "starting" : "down";
  const detail = running
    ? hz.detail
    : ps.code !== 0
      ? ps.stderr.trim().slice(0, 300) || "not running"
      : "not running (no containers up)";
  return { mode: "local", running, health, url, detail };
}

async function cloudRunStatus(flavor: Flavor): Promise<StatusEntry> {
  const project = await projectId();
  if (!project) {
    return {
      mode: "cloudrun",
      running: "unknown",
      health: "down",
      url: null,
      detail: "no PROJECT_ID configured and `gcloud config get-value project` returned nothing",
    };
  }
  const svc = CLOUD_RUN_SERVICE[flavor];
  const describe = await run([
    "gcloud",
    "run",
    "services",
    "describe",
    svc,
    "--project",
    project,
    "--region",
    REGION,
    "--format=json",
  ]);
  if (describe.code !== 0) {
    return {
      mode: "cloudrun",
      running: "unknown",
      health: "down",
      url: null,
      detail: (describe.stderr.trim() || describe.stdout.trim() || "gcloud describe failed").slice(0, 500),
    };
  }
  let parsed: any;
  try {
    parsed = JSON.parse(describe.stdout);
  } catch {
    return {
      mode: "cloudrun",
      running: "unknown",
      health: "down",
      url: null,
      detail: "failed to parse `gcloud run services describe` JSON output",
    };
  }
  const url: string | null = parsed?.status?.url ?? null;
  const minScaleRaw = parsed?.spec?.template?.metadata?.annotations?.["autoscaling.knative.dev/minScale"];
  const running: boolean | "unknown" = minScaleRaw === undefined ? "unknown" : Number(minScaleRaw) >= 1;
  if (!url) {
    return { mode: "cloudrun", running, health: "down", url: null, detail: "service has no status.url yet" };
  }
  const hz = await healthzOf(url, 5000);
  return { mode: "cloudrun", running, health: hz.ok ? "ok" : "starting", url, detail: hz.detail };
}

async function statusOf(flavor: Flavor): Promise<StatusEntry> {
  return MODE === "cloudrun" ? cloudRunStatus(flavor) : localStatus(flavor);
}

// --- start / stop -------------------------------------------------------------------------------
interface ActionResult {
  ok: boolean;
  detail: string;
}

async function startLocal(flavor: Flavor): Promise<ActionResult> {
  const file = COMPOSE_FILE[flavor];
  // GEMINI_API_KEY / ANTHROPIC_API_KEY pass through automatically: `run()` inherits the admin
  // process's own env, and docker compose reads ${VAR:-} substitutions from that same env at the
  // moment this command executes.
  const r = await run(["docker", "compose", "-f", file, "up", "-d", "--build"], { timeoutMs: 10 * 60_000 });
  const out = (r.stdout + r.stderr).trim();
  return { ok: r.code === 0 && !r.timedOut, detail: r.timedOut ? `timed out\n${out.slice(-2000)}` : out.slice(-2000) };
}

async function stopLocal(flavor: Flavor): Promise<ActionResult> {
  const file = COMPOSE_FILE[flavor];
  // 300s: an overloaded Docker daemon can take minutes to tear down the 4-container dapr stack —
  // a killed `compose down` strands half the stack (observed live in W2B verification).
  const r = await run(["docker", "compose", "-f", file, "down"], { timeoutMs: 300_000 });
  const out = (r.stdout + r.stderr).trim();
  return { ok: r.code === 0, detail: out.slice(-2000) };
}

async function scaleCloudRun(flavor: Flavor, minInstances: 0 | 1): Promise<ActionResult> {
  const project = await projectId();
  if (!project) return { ok: false, detail: "no PROJECT_ID configured" };
  const svc = CLOUD_RUN_SERVICE[flavor];
  const r = await run([
    "gcloud",
    "run",
    "services",
    "update",
    svc,
    "--project",
    project,
    "--region",
    REGION,
    `--min-instances=${minInstances}`,
  ]);
  const out = (r.stdout + r.stderr).trim();
  return { ok: r.code === 0, detail: out.slice(-2000) };
}

async function startAction(flavor: Flavor): Promise<ActionResult> {
  return MODE === "cloudrun" ? scaleCloudRun(flavor, 1) : startLocal(flavor);
}
async function stopAction(flavor: Flavor): Promise<ActionResult> {
  return MODE === "cloudrun" ? scaleCloudRun(flavor, 0) : stopLocal(flavor);
}

// --- logs -----------------------------------------------------------------------------------------
async function logsOf(flavor: Flavor): Promise<string> {
  if (MODE === "cloudrun") {
    const project = await projectId();
    if (!project) return "no PROJECT_ID configured — cannot read Cloud Run logs";
    const svc = CLOUD_RUN_SERVICE[flavor];
    const r = await run([
      "gcloud",
      "logging",
      "read",
      `resource.type=cloud_run_revision AND resource.labels.service_name=${svc}`,
      "--project",
      project,
      "--limit=100",
      "--format=value(timestamp,textPayload)",
    ]);
    return (r.stdout || r.stderr || "(no log output — best effort, service may not be deployed)").trim();
  }
  const file = COMPOSE_FILE[flavor];
  const r = await run(["docker", "compose", "-f", file, "logs", "--no-color", "--tail", "100"]);
  return (r.stdout + r.stderr).trim() || "(no log output — is the stack up?)";
}

// --- HTTP surface -----------------------------------------------------------------------------
function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

const indexHtml = Bun.file(new URL("./index.html", import.meta.url));

Bun.serve({
  port: ADMIN_PORT,
  idleTimeout: 0, // some cloudrun describe/logging calls + local `up --build` can run long
  async fetch(req) {
    const url = new URL(req.url);

    if (req.method === "GET" && url.pathname === "/") {
      return new Response(indexHtml, { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    // Interactive docs, served by the admin itself so the link works even while both stacks are
    // down (the running consoles also serve the same files at their own /docs).
    if (req.method === "GET" && (url.pathname === "/docs" || url.pathname === "/docs/comparison.html")) {
      const file = url.pathname === "/docs" ? "index.html" : "comparison.html";
      const doc = Bun.file(`${REPO_ROOT}/orleans/docs/${file}`);
      if (!(await doc.exists())) return new Response("docs not found", { status: 404 });
      return new Response(doc, { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    if (req.method === "GET" && url.pathname === "/api/status") {
      const [orleans, dapr] = await Promise.all([statusOf("orleans"), statusOf("dapr")]);
      return json({ mode: MODE, orleans, dapr });
    }

    if (req.method === "GET" && url.pathname === "/api/logs") {
      const flavor = url.searchParams.get("flavor");
      if (!isFlavor(flavor)) return json({ error: "flavor must be 'orleans' or 'dapr'" }, 400);
      const logs = await logsOf(flavor);
      return json({ flavor, logs });
    }

    if (req.method === "POST" && url.pathname === "/api/start") {
      const flavor = url.searchParams.get("flavor");
      if (!isFlavor(flavor)) return json({ error: "flavor must be 'orleans' or 'dapr'" }, 400);
      console.log(color.cyan(`[admin] start ${flavor} (mode=${MODE})`));
      const result = await startAction(flavor);
      console.log(result.ok ? color.green(`[admin] start ${flavor} ok`) : color.red(`[admin] start ${flavor} FAILED`));
      return json(result, result.ok ? 200 : 500);
    }

    if (req.method === "POST" && url.pathname === "/api/stop") {
      const flavor = url.searchParams.get("flavor");
      if (!isFlavor(flavor)) return json({ error: "flavor must be 'orleans' or 'dapr'" }, 400);
      console.log(color.cyan(`[admin] stop ${flavor} (mode=${MODE})`));
      const result = await stopAction(flavor);
      console.log(result.ok ? color.green(`[admin] stop ${flavor} ok`) : color.red(`[admin] stop ${flavor} FAILED`));
      return json(result, result.ok ? 200 : 500);
    }

    return new Response("not found", { status: 404 });
  },
});

console.log(color.bold(`StreamForge admin listening on :${ADMIN_PORT}  (mode=${MODE}${MODE === "cloudrun" ? ` region=${REGION}` : ""})`));
console.log(color.gray(`repo root: ${REPO_ROOT}`));
if (MODE !== "cloudrun") {
  console.log(color.gray(`local drivers: orleans -> ${COMPOSE_FILE.orleans} (:${LOCAL_PORT.orleans}), dapr -> ${COMPOSE_FILE.dapr} (:${LOCAL_PORT.dapr})`));
}
