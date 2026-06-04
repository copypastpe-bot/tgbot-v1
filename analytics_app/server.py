from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

import asyncpg
from aiohttp import web

from .auth import sign_session, unsign_session, verify_password
from .config import AnalyticsConfig
from .dates import resolve_date_range
from .queries import build_cleaning_dashboard, build_main_cash_dashboard
from .templates import render_login, render_management_dashboard


SESSION_COOKIE = "analytics_session"
CONFIG_KEY = web.AppKey("config", AnalyticsConfig)
QUERY_SERVICE_KEY = web.AppKey("query_service", object)


@dataclass
class QueryService:
    pool: asyncpg.Pool

    def acquire(self):
        return self.pool.acquire()

    async def main(self, conn, *, start_utc, end_utc, group_by):
        return await build_main_cash_dashboard(
            conn, start_utc=start_utc, end_utc=end_utc, group_by=group_by
        )

    async def cleaning(self, conn, *, start_utc, end_utc, group_by):
        return await build_cleaning_dashboard(
            conn, start_utc=start_utc, end_utc=end_utc, group_by=group_by
        )


@web.middleware
async def auth_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
):
    if request.path in {"/login"} or request.path.startswith("/static/"):
        return await handler(request)
    cfg = request.app[CONFIG_KEY]
    login = unsign_session(request.cookies.get(SESSION_COOKIE, ""), secret=cfg.session_secret)
    if not login or login not in cfg.users:
        raise web.HTTPFound("/login")
    request["login"] = login
    return await handler(request)


async def login_get(request: web.Request) -> web.Response:
    return web.Response(text=render_login(error=False), content_type="text/html")


async def login_post(request: web.Request) -> web.Response:
    cfg = request.app[CONFIG_KEY]
    data = await request.post()
    login = str(data.get("login") or "")
    password = str(data.get("password") or "")
    password_hash = cfg.users.get(login)
    if not password_hash or not verify_password(password, password_hash):
        return web.Response(text=render_login(error=True), content_type="text/html", status=401)

    resp = web.HTTPFound("/")
    resp.set_cookie(
        SESSION_COOKIE,
        sign_session(login, secret=cfg.session_secret),
        httponly=True,
        secure=True,
        samesite="Lax",
        max_age=60 * 60 * 24 * 14,
    )
    raise resp


async def logout(request: web.Request) -> web.Response:
    resp = web.HTTPFound("/login")
    resp.del_cookie(SESSION_COOKIE)
    raise resp


def _period_label(rng) -> str:
    return f"{rng.start_date:%d.%m.%Y}-{rng.end_date:%d.%m.%Y}"


async def main_cash(request: web.Request) -> web.Response:
    rng = resolve_date_range(request.query)
    query_service = request.app[QUERY_SERVICE_KEY]
    async with query_service.acquire() as conn:
        data = await query_service.main(
            conn, start_utc=rng.start_utc, end_utc=rng.end_utc, group_by=rng.group_by
        )
    rendered = render_management_dashboard(
        title="Основная касса",
        active_section="main",
        period_label=_period_label(rng),
        date_from=f"{rng.start_date:%Y-%m-%d}",
        date_to=f"{rng.end_date:%Y-%m-%d}",
        balance=data["balance"],
        dashboard=data["management"],
        ledger=data["ledger"],
        extra_note="",
    )
    return web.Response(text=rendered, content_type="text/html")


async def cleaning(request: web.Request) -> web.Response:
    rng = resolve_date_range(request.query)
    query_service = request.app[QUERY_SERVICE_KEY]
    async with query_service.acquire() as conn:
        data = await query_service.cleaning(
            conn, start_utc=rng.start_utc, end_utc=rng.end_utc, group_by=rng.group_by
        )
    rendered = render_management_dashboard(
        title="Клининг",
        active_section="cleaning",
        period_label=_period_label(rng),
        date_from=f"{rng.start_date:%Y-%m-%d}",
        date_to=f"{rng.end_date:%Y-%m-%d}",
        balance=data["balance"],
        dashboard=data["management"],
        ledger=data["ledger"],
        extra_note=f"Подарочные сертификаты за период: {data['gift_total']} ₽. Зарплатные данные клининга пока не подключены.",
    )
    return web.Response(text=rendered, content_type="text/html")


async def _close_pool(app: web.Application) -> None:
    query_service = app[QUERY_SERVICE_KEY]
    pool = getattr(query_service, "pool", None)
    if pool is not None:
        await pool.close()


def create_app(cfg: AnalyticsConfig, *, query_service=None) -> web.Application:
    app = web.Application(middlewares=[auth_middleware])
    app[CONFIG_KEY] = cfg
    if query_service is None:

        async def init_pool(app: web.Application) -> None:
            pool = await asyncpg.create_pool(cfg.db_dsn, min_size=1, max_size=5)
            app[QUERY_SERVICE_KEY] = QueryService(pool)

        app.on_startup.append(init_pool)
    else:
        app[QUERY_SERVICE_KEY] = query_service
    app.on_cleanup.append(_close_pool)

    static_dir = Path(__file__).with_name("static")
    app.router.add_static("/static/", static_dir)
    app.router.add_get("/login", login_get)
    app.router.add_post("/login", login_post)
    app.router.add_get("/logout", logout)
    app.router.add_get("/", main_cash)
    app.router.add_get("/cleaning", cleaning)
    return app
