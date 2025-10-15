from __future__ import annotations
import os, sys, subprocess, hashlib, time
from typing import Optional


def _git(cmd: list[str]) -> Optional[str]:
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode().strip()
        return out or None
    except Exception:
        return None


def git_commit_short() -> Optional[str]:
    return _git(["git", "rev-parse", "--short", "HEAD"])


def repo_status() -> Optional[str]:
    return _git(["git", "status", "--porcelain"])


def file_md5(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return None


def build_info() -> dict:
    return {
        "cwd": os.getcwd(),
        "main_file": os.path.abspath(sys.modules.get("__main__").__file__) if sys.modules.get("__main__") else None,
        "python": sys.executable,
        "venv": os.environ.get("VIRTUAL_ENV"),
        "aiogram": _get_aiogram_ver(),
        "git": git_commit_short(),
        "dirty": bool(repo_status()),
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _get_aiogram_ver() -> Optional[str]:
    try:
        import aiogram  # type: ignore
        return getattr(aiogram, "__version__", "unknown")
    except Exception:
        return None
