"""Contract-test fixture: boots an ISOLATED StreamsForge instance on 9199/9299 (never
5199/5299 -- the live dev server -- and never 6199 -- the demo container), imports a tiny config
(one ingest source, one LATEST BY table, one aggregate over that derived LATEST BY), and tears it
down after the session. Asserts the ports are free first and skips with a clear message rather
than colliding, per the task's testing requirements.
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
# The live dev server (5199/5299) and the running demo (6199) are never ours to touch, whatever
# the environment says.
_FORBIDDEN_PORTS = {5199, 5299, 6199}

DOTNET = os.path.expanduser("~/.dotnet/dotnet")
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "..", "orleans", "src", "StreamsForge.Host"))

BASE_URL = f"http://localhost:{HTTP_PORT}"
GRPC_TARGET = f"localhost:{GRPC_PORT}"
ADMIN_USER = "admin"
ADMIN_PASS = "admin123!"

SOURCE_NAME = "sf_client_trades"
LATEST_TABLE = "sf_client_latest_trade"
AGG_TABLE = "sf_client_desk_totals"
GLOBAL_AGG_TABLE = "sf_client_all_totals"


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def _login() -> str:
    resp = httpx.post(
        f"{BASE_URL}/api/auth/login", json={"username": ADMIN_USER, "password": ADMIN_PASS}, timeout=10
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


def _wait_healthy(proc: subprocess.Popen, drain: _Drain, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"engine process exited early (code {proc.returncode}):\n{drain.tail()}")
        try:
            resp = httpx.get(f"{BASE_URL}/api/healthz", timeout=2)
            if resp.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(0.5)
    raise RuntimeError(
        f"engine did not become healthy within {timeout}s (last error: {last_err})\n{drain.tail()}"
    )


def _import_fixture_config() -> None:
    token = _login()
    headers = {"authorization": f"Bearer {token}"}
    doc = {
        "version": 1,
        "sources": [
            {
                "name": SOURCE_NAME,
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
                "name": LATEST_TABLE,
                "description": "latest row per trade_id",
                "sql": f"SELECT trade_id, desk, notional FROM {SOURCE_NAME} LATEST BY (trade_id)",
                "running": True,
            },
            {
                "name": AGG_TABLE,
                "description": "aggregate over the derived LATEST BY (per design doc §8's fixture spec)",
                "sql": f"SELECT desk, SUM(notional) AS total FROM {LATEST_TABLE} GROUP BY desk",
                "running": True,
            },
            {
                "name": GLOBAL_AGG_TABLE,
                "description": "unkeyed global aggregate (no GROUP BY) -- exercises KeyFields=[] over the wire",
                "sql": f"SELECT COUNT(*) AS trade_count, SUM(notional) AS total_notional FROM {LATEST_TABLE}",
                "running": True,
            },
        ],
    }
    resp = httpx.post(
        f"{BASE_URL}/api/config/import", params={"mode": "merge"}, json=doc, headers=headers, timeout=30
    )
    resp.raise_for_status()
    report = resp.json()
    errored = [e for e in report.get("entries", []) if e.get("action") == "error"]
    if errored:
        raise RuntimeError(f"fixture config import failed: {errored}")


def _publish(publish_dir: str) -> None:
    """`dotnet publish` into an isolated directory nothing else owns. The live demo on 6199 is
    itself a plain `dotnet run --project orleans/src/StreamsForge.Host` process, so it holds files
    open under THIS project's own `bin/`/`obj/` output -- any further `dotnet run`/`build` of the
    same project collides with it (observed: MSBuild's implicit content-copy of `data/state/*`,
    which the ASP.NET Core SDK auto-includes, hung retrying against a file the live process had
    open). Publishing to a private temp directory and then running the produced DLL directly
    sidesteps this at the root: publish's output path is explicit and shared with nothing, and
    running a DLL never re-invokes the build system at all, so the live process becomes
    irrelevant from that point on."""
    result = subprocess.run(
        [DOTNET, "publish", PROJECT_DIR, "-c", "Debug", "-o", publish_dir],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dotnet publish failed (code {result.returncode}):\n{(result.stdout + result.stderr)[-6000:]}")


@pytest.fixture(scope="session")
def engine():
    if HTTP_PORT in _FORBIDDEN_PORTS or GRPC_PORT in _FORBIDDEN_PORTS:
        pytest.skip("refusing to configure the contract-test fixture onto a forbidden port")
    if not _port_free(HTTP_PORT) or not _port_free(GRPC_PORT):
        pytest.skip(f"port {HTTP_PORT} or {GRPC_PORT} is already in use -- refusing to collide with a running instance")
    if not os.path.exists(DOTNET):
        pytest.skip(f"dotnet not found at {DOTNET} -- cannot boot the contract-test engine")
    if not os.path.isdir(PROJECT_DIR):
        pytest.skip(f"StreamsForge.Host project not found at {PROJECT_DIR}")

    # A publish takes ~2 minutes; SF_TEST_PUBLISH_DIR reuses one across runs while iterating.
    prebuilt = os.environ.get("SF_TEST_PUBLISH_DIR")
    publish_dir = prebuilt or tempfile.mkdtemp(prefix="sf-python-client-publish-")
    data_dir = tempfile.mkdtemp(prefix="sf-python-client-test-")
    if not prebuilt:
        try:
            _publish(publish_dir)
        except Exception as exc:
            shutil.rmtree(publish_dir, ignore_errors=True)
            shutil.rmtree(data_dir, ignore_errors=True)
            pytest.skip(f"could not publish an isolated engine build: {exc}")

    dll = os.path.join(publish_dir, "StreamsForge.Host.dll")
    proc = subprocess.Popen(
        [
            DOTNET, dll,
            "--Http:Port", str(HTTP_PORT),
            "--Grpc:Port", str(GRPC_PORT),
            "--Streams:Transport", "push",
            "--DataDir", data_dir,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        # WebApplication.CreateBuilder takes its content root from the CURRENT DIRECTORY, not from
        # the assembly's. Run the DLL from anywhere else and appsettings.json is never found, so
        # `Jwt:Key` is null and every request 500s inside the auth middleware -- including
        # /api/healthz, which makes it look like the engine never came up.
        cwd=publish_dir,
    )
    drain = _Drain(proc)
    try:
        _wait_healthy(proc, drain)
        _import_fixture_config()
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
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(data_dir, ignore_errors=True)
        if not prebuilt:
            shutil.rmtree(publish_dir, ignore_errors=True)
