from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time


HASH_PREFIX = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 260_000


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("ascii"))


def make_password_hash(
    password: str,
    *,
    salt: str | None = None,
    iterations: int = DEFAULT_ITERATIONS,
) -> str:
    if not salt:
        salt = _b64encode(os.urandom(16))
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    return f"{HASH_PREFIX}${iterations}${salt}${_b64encode(digest)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        prefix, iterations_raw, salt, expected = password_hash.split("$", 3)
        if prefix != HASH_PREFIX:
            return False
        iterations = int(iterations_raw)
        actual = make_password_hash(password, salt=salt, iterations=iterations).split("$", 3)[3]
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _session_signature(login: str, issued_at: int, secret: str) -> str:
    payload = f"{login}:{issued_at}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return _b64encode(digest)


def sign_session(login: str, *, secret: str, now: int | None = None) -> str:
    issued_at = int(time.time() if now is None else now)
    signature = _session_signature(login, issued_at, secret)
    payload = f"{login}:{issued_at}:{signature}".encode("utf-8")
    return _b64encode(payload)


def unsign_session(
    cookie_value: str,
    *,
    secret: str,
    now: int | None = None,
    max_age_seconds: int = 60 * 60 * 24 * 14,
) -> str | None:
    try:
        login, issued_raw, signature = _b64decode(cookie_value).decode("utf-8").split(":", 2)
        issued_at = int(issued_raw)
    except Exception:
        return None

    current = int(time.time() if now is None else now)
    if current - issued_at > max_age_seconds:
        return None

    expected = _session_signature(login, issued_at, secret)
    if not hmac.compare_digest(signature, expected):
        return None
    return login
