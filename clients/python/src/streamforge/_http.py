"""httpx-based REST client with cached, self-refreshing StreamForge auth.

Ported from lib/streamforge/server.ts's sfFetch: the JWT is cached in memory for ~11h (the
server issues 12h tokens) and re-minted exactly once on any 401, then the request is retried
once with the fresh token -- if THAT also 401s, we raise rather than looping forever (a
StreamForge restart invalidates every token minted before it, which is a normal event, but an
auth system that is actually broken should fail loudly, not spin).
"""

from __future__ import annotations

import threading
import time

import httpx

from .errors import AuthError

TOKEN_LIFETIME_S = 11 * 60 * 60  # server mints 12h tokens; refresh a bit early


class AuthClient:
    """Owns one httpx.Client and the login/token lifecycle for one base_url."""

    def __init__(
        self,
        base_url: str,
        user: str | None,
        password: str | None,
        *,
        verify: bool = True,
        token: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._user = user
        self._password = password
        self._client = httpx.Client(verify=verify, timeout=timeout)
        self._lock = threading.Lock()
        self._token = token
        self._token_minted_at = time.monotonic() if token else None

    def close(self) -> None:
        self._client.close()

    # ---- token lifecycle ----

    def token(self) -> str:
        with self._lock:
            if self._token is None or self._expired():
                self._login_locked()
            assert self._token is not None
            return self._token

    def _expired(self) -> bool:
        return self._token_minted_at is None or (time.monotonic() - self._token_minted_at) > TOKEN_LIFETIME_S

    def _login_locked(self) -> None:
        if not self._user or not self._password:
            raise AuthError(
                "no StreamForge credentials configured -- pass user=/password= to connect(), "
                "set STREAMFORGE_ADMIN_USER/STREAMFORGE_ADMIN_PASS, or add them to "
                "~/.config/streamforge/config.toml"
            )
        resp = self._client.post(
            f"{self.base_url}/api/auth/login",
            json={"username": self._user, "password": self._password},
        )
        if resp.status_code != 200:
            raise AuthError(f"StreamForge login failed: {resp.status_code} {resp.text}")
        body = resp.json()
        self._token = body["token"]
        self._token_minted_at = time.monotonic()

    def invalidate_token(self) -> None:
        with self._lock:
            self._token = None

    # ---- requests ----

    def request(self, method: str, path: str, *, auth: bool = True, **kwargs) -> httpx.Response:
        """`auth=False` skips minting/attaching a Bearer token entirely -- used for the ingest
        path when only SF_INGEST_KEY is configured, so a notebook that only feeds a source never
        forces an admin login (design doc §4)."""
        headers = dict(kwargs.pop("headers", None) or {})
        url = f"{self.base_url}{path}"
        if not auth:
            return self._client.request(method, url, headers=headers, **kwargs)

        headers["authorization"] = f"Bearer {self.token()}"
        resp = self._client.request(method, url, headers=headers, **kwargs)
        if resp.status_code == 401:
            self.invalidate_token()
            headers["authorization"] = f"Bearer {self.token()}"
            resp = self._client.request(method, url, headers=headers, **kwargs)
            if resp.status_code == 401:
                raise AuthError(f"StreamForge rejected the re-minted token for {method} {path}")
        return resp

    def get(self, path: str, **kwargs) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def delete(self, path: str, **kwargs) -> httpx.Response:
        return self.request("DELETE", path, **kwargs)

    def stream(self, method: str, path: str, **kwargs):
        """Streaming request (SSE receive) as a context manager, same auth as request()."""
        headers = dict(kwargs.pop("headers", None) or {})
        headers["authorization"] = f"Bearer {self.token()}"
        return self._client.stream(method, f"{self.base_url}{path}", headers=headers, **kwargs)
