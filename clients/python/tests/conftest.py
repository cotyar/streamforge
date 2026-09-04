"""Contract-test fixtures: boot an ISOLATED StreamsForge instance and tear it down after the
session. Two flavors share the publish/spawn/health-check plumbing below:

  engine      -- plaintext, ports 9199/9299 (never 5199/5299 -- the live dev server -- and never
                 6199 -- the demo container).
  tls_engine  -- HTTPS/TLS gRPC, ports 7599/7699, its own silo ports (17599/37599) so it can run
                 alongside `engine` in the same session, and a `tools/tls/dev-cert.sh`-minted
                 self-signed certificate.

Both assert their ports are free first and skip with a clear message rather than colliding, per
the task's testing requirements. The (expensive, ~2 minutes) `dotnet publish` is shared between
whichever of the two fixtures a test session actually uses, via the session-scoped `_publish_dir`
fixture -- the published DLL is identical either way, since TLS is switched on purely by config
flags at startup, not by anything baked into the build.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import threading
from collections import deque
import tempfile
import time

import httpx
import pytest

# Overridable because several client-library test suites (Python, .NET, TypeScript, Kotlin) boot
# their own engine and would otherwise fight over one pair of ports.
HTTP_PORT = int(os.environ.get("SF_TEST_HTTP_PORT", "9199"))
GRPC_PORT = int(os.environ.get("SF_TEST_GRPC_PORT", "9299"))
# Reserved for this client's TLS fixture (see CLAUDE.md's port table) -- a second host on the same
# machine also needs its own silo ports, hence 17599/37599 below.
TLS_HTTP_PORT = int(os.environ.get("SF_TEST_TLS_HTTP_PORT", "7599"))
TLS_GRPC_PORT = int(os.environ.get("SF_TEST_TLS_GRPC_PORT", "7699"))
TLS_SILO_PORT = "17599"
TLS_SILO_GATEWAY_PORT = "37599"
# The live dev server (5199/5299) and the running demo (6199) are never ours to touch, whatever
# the environment says.
_FORBIDDEN_PORTS = {5199, 5299, 6199}

DOTNET = os.path.expanduser("~/.dotnet/dotnet")
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "..", "orleans", "src", "StreamsForge.Host"))
DEV_CERT_SCRIPT = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "..", "tools", "tls", "dev-cert.sh"))

BASE_URL = f"http://localhost:{HTTP_PORT}"
GRPC_TARGET = f"localhost:{GRPC_PORT}"
TLS_BASE_URL = f"https://localhost:{TLS_HTTP_PORT}"
TLS_GRPC_TARGET = f"https://localhost:{TLS_GRPC_PORT}"
ADMIN_USER = "admin"
ADMIN_PASS = "admin123!"

SOURCE_NAME = "sf_client_trades"
LATEST_TABLE = "sf_client_latest_trade"
AGG_TABLE = "sf_client_desk_totals"
GLOBAL_AGG_TABLE = "sf_client_all_totals"

TLS_SOURCE_NAME = "sf_client_tls_trades"
TLS_LATEST_TABLE = "sf_client_tls_latest_trade"
TLS_AGG_TABLE = "sf_client_tls_desk_totals"
TLS_GLOBAL_AGG_TABLE = "sf_client_tls_all_totals"


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def _check_prereqs(http_port: int, grpc_port: int) -> None:
    if http_port in _FORBIDDEN_PORTS or grpc_port in _FORBIDDEN_PORTS:
        pytest.skip("refusing to configure a contract-test fixture onto a forbidden port")
    if not _port_free(http_port) or not _port_free(grpc_port):
        pytest.skip(f"port {http_port} or {grpc_port} is already in use -- refusing to collide with a running instance")
    if not os.path.exists(DOTNET):
        pytest.skip(f"dotnet not found at {DOTNET} -- cannot boot the contract-test engine")
    if not os.path.isdir(PROJECT_DIR):
        pytest.skip(f"StreamsForge.Host project not found at {PROJECT_DIR}")


def _login(base_url: str, verify: bool | str = True) -> str:
    resp = httpx.post(
        f"{base_url}/api/auth/login", json={"username": ADMIN_USER, "password": ADMIN_PASS}, timeout=10,
        verify=verify,
    )
    resp.raise_for_status()
    return resp.json()["token"]


class _Drain:
    """Reads the child's merged stdout on a daemon thread, keeping only the tail.

    A redirected pipe that nobody reads is a real hazard, not a tidiness point: the OS pipe buffer
    is finite (64KB on macOS), and once the engine has logged that much its next write BLOCKS. The
    process then hangs mid-run with no error anywhere -- it looks exactly like an unstable engine,
    and it is entirely our end. (Observed for real in the .NET client's fixture, whose engine died
    mid-suite for this reason; this suite only escaped it by being short enough not to fill 64KB.)

    Draining continuously also means the tail is available at any moment, so a failure report does
    not depend on the process having already exited."""

    def __init__(self, proc: subprocess.Popen, keep_lines: int = 400) -> None:
        self._lines: deque[str] = deque(maxlen=keep_lines)
        self._proc = proc
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _pump(self) -> None:
        stream = self._proc.stdout
        if stream is None:
            return
        for line in stream:  # blocking iteration, on a thread that has nothing else to do
            self._lines.append(line.rstrip("\n"))

    def tail(self, chars: int = 6000) -> str:
        return "\n".join(self._lines)[-chars:]


def _wait_healthy(proc: subprocess.Popen, drain: _Drain, base_url: str, verify: bool | str = True, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"engine process exited early (code {proc.returncode}):\n{drain.tail()}")
        try:
            resp = httpx.get(f"{base_url}/api/healthz", timeout=2, verify=verify)
            if resp.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.5)
    raise RuntimeError(
        f"engine did not become healthy within {timeout}s (last error: {last_err})\n{drain.tail()}"
    )


def _import_fixture_config(
    base_url: str,
    verify: bool | str = True,
    *,
    source_name: str = SOURCE_NAME,
    latest_table: str = LATEST_TABLE,
    agg_table: str = AGG_TABLE,
    global_agg_table: str = GLOBAL_AGG_TABLE,
) -> None:
    token = _login(base_url, verify)
    headers = {"authorization": f"Bearer {token}"}
    doc = {
        "version": 1,
        "sources": [
            {
                "name": source_name,
                "description": "python client contract test fixture",
                "kind": "ingest",
                "fields": [
                    {"name": "trade_id", "type": "String"},
                    {"name": "desk", "type": "String"},
                    {"name": "notional", "type": "Double"},
                ],
                "ingest": {},
                "enabled": True,
            }
        ],
        "pipelines": [],
        "tables": [
            {
                "name": latest_table,
                "description": "latest row per trade_id",
                "sql": f"SELECT trade_id, desk, notional FROM {source_name} LATEST BY (trade_id)",
                "running": True,
            },
            {
                "name": agg_table,
                "description": "aggregate over the derived LATEST BY (per design doc §8's fixture spec)",
                "sql": f"SELECT desk, SUM(notional) AS total FROM {latest_table} GROUP BY desk",
                "running": True,
            },
            {
                "name": global_agg_table,
                "description": "unkeyed global aggregate (no GROUP BY) -- exercises KeyFields=[] over the wire",
                "sql": f"SELECT COUNT(*) AS trade_count, SUM(notional) AS total_notional FROM {latest_table}",
                "running": True,
            },
        ],
    }
    resp = httpx.post(
        f"{base_url}/api/config/import", params={"mode": "merge"}, json=doc, headers=headers, timeout=30,
        verify=verify,
    )
    resp.raise_for_status()
    report = resp.json()
    errored = [e for e in report.get("entries", []) if e.get("action") == "error"]
    if errored:
        raise RuntimeError(f"fixture config import failed: {errored}")


def _local_rid() -> str:
    """The .NET RID for THIS machine, per `dotnet --info`'s own "RID:" line -- authoritative,
    unlike hand-mapping `platform.system()`/`platform.machine()` (which gets darwin/arm64 vs.
    osx-arm64 naming wrong in subtle ways across dotnet versions)."""
    result = subprocess.run([DOTNET, "--info"], capture_output=True, text=True, timeout=30)
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("RID:"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError(f"could not find a 'RID:' line in `dotnet --info` output:\n{result.stdout}")


def _publish(publish_dir: str) -> None:
    """`dotnet publish` into an isolated directory nothing else owns. The live demo on 6199 is
    itself a plain `dotnet run --project orleans/src/StreamsForge.Host` process, so it holds files
    open under THIS project's own `bin/`/`obj/` output -- any further `dotnet run`/`build` of the
    same project collides with it (observed: MSBuild's implicit content-copy of `data/state/*`,
    which the ASP.NET Core SDK auto-includes, hung retrying against a file the live process had
    open). Publishing to a private temp directory and then running the produced DLL directly
    sidesteps this at the root: publish's output path is explicit and shared with nothing, and
    running a DLL never re-invokes the build system at all, so the live process becomes
    irrelevant from that point on.

    `-r <local RID>` is required, not optional, since plan 022: the host's own `Publish.props`
    (`orleans/src/StreamsForge.Host/Publish.props`) defaults a bare `dotnet publish` (no `-r` at
    all) to `linux-x64` -- deliberately, since that is the container image target -- and always
    publishes self-contained + single-file once `_IsPublishing` is set, whatever RID is chosen. A
    bare `dotnet publish` on a non-Linux dev machine therefore produces a `StreamsForge.Host`
    binary this fixture cannot execute at all (`OSError: [Errno 8] Exec format error`) rather than
    one that merely fails to boot -- passing the local RID explicitly is what makes the publish
    output runnable on the machine that just built it."""
    result = subprocess.run(
        [DOTNET, "publish", PROJECT_DIR, "-c", "Debug", "-r", _local_rid(), "-o", publish_dir],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dotnet publish failed (code {result.returncode}):\n{(result.stdout + result.stderr)[-6000:]}")


def _spawn_host(publish_dir: str, data_dir: str, http_port: int, grpc_port: int, extra_args: list[str] | None = None) -> subprocess.Popen:
    # Plan 022 (already on master) makes `dotnet publish` for this host emit a self-contained,
    # single-file NATIVE executable (`StreamsForge.Host`, no `.dll` alongside it) via the host's
    # own Publish.props, gated on MSBuild's `_IsPublishing` -- so a plain `dotnet publish` with no
    # explicit RID/self-contained flags on THIS machine no longer produces the portable
    # `StreamsForge.Host.dll` this fixture originally assumed. Support both shapes so the fixture
    # keeps working across that publish-output change (and on any environment that still produces
    # the portable form).
    dll = os.path.join(publish_dir, "StreamsForge.Host.dll")
    native = os.path.join(publish_dir, "StreamsForge.Host")
    if os.path.exists(dll):
        args = [DOTNET, dll]
    elif os.path.exists(native):
        args = [native]
    else:
        raise RuntimeError(
            f"no StreamsForge.Host executable found in {publish_dir} "
            "(looked for StreamsForge.Host.dll and the single-file native StreamsForge.Host)"
        )
    args += ["--Http:Port", str(http_port), "--Grpc:Port", str(grpc_port), "--DataDir", data_dir]
    if extra_args:
        args.extend(extra_args)
    return subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        # WebApplication.CreateBuilder takes its content root from the CURRENT DIRECTORY, not from
        # the assembly's. Run the DLL from anywhere else and appsettings.json is never found, so
        # `Jwt:Key` is null and every request 500s inside the auth middleware -- including
        # /api/healthz, which makes it look like the engine never came up.
        cwd=publish_dir,
    )


def _stop_host(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="session")
def _publish_dir():
    """Session-scoped and requested lazily (via `request.getfixturevalue`, not as a normal
    parameter) by both `engine` and `tls_engine`, so whichever runs first pays the ~2 minute
    publish cost and the other reuses the same DLL -- TLS is a config-time flag here, not a build
    flag, so one publish serves both fixtures."""
    prebuilt = os.environ.get("SF_TEST_PUBLISH_DIR")
    publish_dir = prebuilt or tempfile.mkdtemp(prefix="sf-python-client-publish-")
    if not prebuilt:
        try:
            _publish(publish_dir)
        except Exception as exc:
            shutil.rmtree(publish_dir, ignore_errors=True)
            pytest.skip(f"could not publish an isolated engine build: {exc}")
    yield publish_dir
    if not prebuilt:
        shutil.rmtree(publish_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def engine(request):
    _check_prereqs(HTTP_PORT, GRPC_PORT)
    publish_dir = request.getfixturevalue("_publish_dir")

    data_dir = tempfile.mkdtemp(prefix="sf-python-client-test-")
    proc = _spawn_host(publish_dir, data_dir, HTTP_PORT, GRPC_PORT, extra_args=["--Streams:Transport", "push"])
    drain = _Drain(proc)
    try:
        _wait_healthy(proc, drain, BASE_URL)
        _import_fixture_config(BASE_URL)
        yield {
            "base_url": BASE_URL,
            "grpc": GRPC_TARGET,
            "user": ADMIN_USER,
            "password": ADMIN_PASS,
            "source": SOURCE_NAME,
            "latest_table": LATEST_TABLE,
            "agg_table": AGG_TABLE,
            "global_agg_table": GLOBAL_AGG_TABLE,
        }
    finally:
        _stop_host(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def tls_engine(request):
    """Same shape as `engine`, but the host's REST/SignalR port serves HTTPS and its gRPC port
    serves TLS (ALPN h2), via a `tools/tls/dev-cert.sh`-minted self-signed certificate -- see that
    script's own docstring for why the cert IS the trust anchor a client passes as `ca=`."""
    _check_prereqs(TLS_HTTP_PORT, TLS_GRPC_PORT)
    if shutil.which("openssl") is None:
        pytest.skip("openssl not found on PATH -- cannot generate a dev TLS certificate")
    if not os.path.exists(DEV_CERT_SCRIPT):
        pytest.skip(f"tools/tls/dev-cert.sh not found at {DEV_CERT_SCRIPT}")

    publish_dir = request.getfixturevalue("_publish_dir")

    cert_dir = tempfile.mkdtemp(prefix="sf-python-client-tls-cert-")
    result = subprocess.run([DEV_CERT_SCRIPT, cert_dir], capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        shutil.rmtree(cert_dir, ignore_errors=True)
        pytest.skip(f"tools/tls/dev-cert.sh failed (code {result.returncode}): {(result.stdout + result.stderr)[-2000:]}")
    cert_path = os.path.join(cert_dir, "cert.pem")
    key_path = os.path.join(cert_dir, "key.pem")

    data_dir = tempfile.mkdtemp(prefix="sf-python-client-tls-test-")
    proc = _spawn_host(
        publish_dir, data_dir, TLS_HTTP_PORT, TLS_GRPC_PORT,
        extra_args=[
            "--Streams:Transport", "push",
            "--Silo:Port", TLS_SILO_PORT,
            "--Silo:GatewayPort", TLS_SILO_GATEWAY_PORT,
            "--Tls:Enabled", "true",
            "--Kestrel:Certificates:Default:Path", cert_path,
            "--Kestrel:Certificates:Default:KeyPath", key_path,
        ],
    )
    drain = _Drain(proc)
    try:
        _wait_healthy(proc, drain, TLS_BASE_URL, verify=cert_path)
        _import_fixture_config(
            TLS_BASE_URL, verify=cert_path,
            source_name=TLS_SOURCE_NAME, latest_table=TLS_LATEST_TABLE,
            agg_table=TLS_AGG_TABLE, global_agg_table=TLS_GLOBAL_AGG_TABLE,
        )
        yield {
            "base_url": TLS_BASE_URL,
            "grpc": TLS_GRPC_TARGET,
            "user": ADMIN_USER,
            "password": ADMIN_PASS,
            "ca": cert_path,
            "source": TLS_SOURCE_NAME,
            "latest_table": TLS_LATEST_TABLE,
            "agg_table": TLS_AGG_TABLE,
            "global_agg_table": TLS_GLOBAL_AGG_TABLE,
        }
    finally:
        _stop_host(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree(cert_dir, ignore_errors=True)
