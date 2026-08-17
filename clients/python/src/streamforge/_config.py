"""Config resolution: explicit kwargs -> env -> ~/.config/streamforge/config.toml.

Stdlib tomllib only (Python >= 3.11), no new dependency for the file form. First hit wins per
field, independently -- a config.toml with a base_url and an env var with just the password both
apply.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

CONFIG_PATH = Path.home() / ".config" / "streamforge" / "config.toml"


@dataclass(frozen=True)
class ResolvedConfig:
    base_url: str | None
    grpc: str | None
    user: str | None
    password: str | None
    ingest_key: str | None


def _load_toml(path: Path = CONFIG_PATH) -> dict:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def resolve(
    *,
    url: str | None = None,
    grpc: str | None = None,
    user: str | None = None,
    password: str | None = None,
    ingest_key: str | None = None,
    config_path: Path = CONFIG_PATH,
) -> ResolvedConfig:
    toml = _load_toml(config_path)

    def pick(explicit: str | None, env_name: str, toml_key: str) -> str | None:
        if explicit is not None:
            return explicit
        env_value = os.environ.get(env_name)
        if env_value is not None:
            return env_value
        value = toml.get(toml_key)
        return value if isinstance(value, str) else None

    return ResolvedConfig(
        base_url=pick(url, "STREAMFORGE_BASE_URL", "base_url"),
        grpc=pick(grpc, "STREAMFORGE_GRPC", "grpc"),
        user=pick(user, "STREAMFORGE_ADMIN_USER", "user"),
        password=pick(password, "STREAMFORGE_ADMIN_PASS", "password"),
        ingest_key=pick(ingest_key, "SF_INGEST_KEY", "ingest_key"),
    )
