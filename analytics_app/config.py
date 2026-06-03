from __future__ import annotations

from dataclasses import dataclass
from os import environ
from typing import Mapping


@dataclass(frozen=True)
class AnalyticsConfig:
    db_dsn: str
    session_secret: str
    users: dict[str, str]
    bind_host: str = "127.0.0.1"
    bind_port: int = 8090


def _parse_users(raw: str) -> dict[str, str]:
    users: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError("ANALYTICS_USERS entries must use login:hash")
        login, password_hash = item.split(":", 1)
        login = login.strip()
        password_hash = password_hash.strip()
        if not login or not password_hash:
            raise ValueError("ANALYTICS_USERS entries must use non-empty login:hash")
        users[login] = password_hash
    if not users:
        raise ValueError("ANALYTICS_USERS must contain at least one user")
    return users


def load_config(env: Mapping[str, str] | None = None) -> AnalyticsConfig:
    source = environ if env is None else env
    db_dsn = (source.get("ANALYTICS_DB_DSN") or source.get("DB_DSN") or "").strip()
    if not db_dsn:
        raise ValueError("DB_DSN or ANALYTICS_DB_DSN is required")

    session_secret = (source.get("ANALYTICS_SESSION_SECRET") or "").strip()
    if not session_secret:
        raise ValueError("ANALYTICS_SESSION_SECRET is required")

    users_raw = (source.get("ANALYTICS_USERS") or "").strip()
    if not users_raw:
        raise ValueError("ANALYTICS_USERS is required")

    bind_host = (source.get("ANALYTICS_BIND_HOST") or "127.0.0.1").strip()
    bind_port = int((source.get("ANALYTICS_BIND_PORT") or "8090").strip())
    return AnalyticsConfig(
        db_dsn=db_dsn,
        session_secret=session_secret,
        users=_parse_users(users_raw),
        bind_host=bind_host,
        bind_port=bind_port,
    )
