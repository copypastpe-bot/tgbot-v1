from __future__ import annotations

from aiohttp import web

from .config import load_config
from .server import create_app


def main() -> None:
    cfg = load_config()
    app = create_app(cfg)
    web.run_app(app, host=cfg.bind_host, port=cfg.bind_port)


if __name__ == "__main__":
    main()
