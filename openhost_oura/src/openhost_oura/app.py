import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timezone
from html import escape as html_escape
from pathlib import Path

from litestar import Litestar, Request, get, post
from litestar.response import Redirect, Response
from litestar.exceptions import NotFoundException

from . import db
from . import oura
from .service_api import service_routes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _base_url() -> str:
    app_name = os.environ.get("OPENHOST_APP_NAME", "health-data")
    zone = os.environ.get("OPENHOST_ZONE_DOMAIN", "")
    if zone:
        return f"https://{app_name}.{zone}"
    return "http://localhost:8080"


def _redirect_uri() -> str:
    return f"{_base_url()}/oauth/callback"


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@get("/health")
async def health_check() -> dict:
    return {"status": "ok"}


@get("/")
async def index() -> Response:
    token = await db.get_config("oura_access_token")
    if not token:
        return Redirect("/setup")
    return Response(content=DASHBOARD_HTML, media_type="text/html")


@get("/setup")
async def setup_page() -> Response:
    client_id = await db.get_config("oura_client_id") or ""
    token = await db.get_config("oura_access_token")
    last_sync = await db.get_config("last_sync") or "never"
    last_error = await db.get_config("last_sync_error")
    status = "connected" if token else "not connected"
    error_banner = ""
    if last_error:
        error_banner = (
            '<p class="status" style="color:#fca5a5;background:#7f1d1d;'
            'border:1px solid #b91c1c;border-radius:8px;padding:0.6rem 0.8rem">'
            f"{html_escape(last_error)}</p>"
        )
    html = (
        SETUP_HTML.replace("{{client_id}}", client_id)
        .replace("{{status}}", status)
        .replace("{{last_sync}}", last_sync)
        .replace("{{error_banner}}", error_banner)
    )
    return Response(content=html, media_type="text/html")


@post("/setup/oauth")
async def start_oauth(request: Request) -> Response:
    body = await request.body()
    params = dict(p.split("=", 1) for p in body.decode().split("&") if "=" in p)
    client_id = _url_decode(params.get("client_id", "")).strip()
    client_secret = _url_decode(params.get("client_secret", "")).strip()
    if not client_id or not client_secret:
        return Response(content="client_id and client_secret required", status_code=400)

    await db.set_config("oura_client_id", client_id)
    await db.set_config("oura_client_secret", client_secret)

    state = secrets.token_urlsafe(16)
    await db.set_config("oauth_state", state)

    url = oura.get_authorize_url(client_id, _redirect_uri(), state)
    return Redirect(url)


@get("/oauth/callback")
async def oauth_callback(request: Request) -> Response:
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    error = request.query_params.get("error")

    if error:
        return Response(content=f"OAuth error: {error}", status_code=400, media_type="text/plain")
    if not code:
        return Response(content="No code received", status_code=400, media_type="text/plain")

    saved_state = await db.get_config("oauth_state")
    if state != saved_state:
        return Response(content="State mismatch", status_code=400, media_type="text/plain")

    client_id = await db.get_config("oura_client_id")
    client_secret = await db.get_config("oura_client_secret")

    tokens = await oura.exchange_code(code, _redirect_uri(), client_id, client_secret)
    await db.set_config("oura_access_token", tokens["access_token"])
    if "refresh_token" in tokens:
        await db.set_config("oura_refresh_token", tokens["refresh_token"])

    await db.delete_config("oauth_state")

    asyncio.create_task(_background_sync())
    return Redirect("/")


@get("/api/status")
async def get_status() -> dict:
    last_sync = await db.get_config("last_sync")
    last_error = await db.get_config("last_sync_error") or None
    return {"last_sync": last_sync, "last_error": last_error}


@post("/sync")
async def trigger_sync() -> Response:
    token = await db.get_config("oura_access_token")
    if not token:
        return Response(
            {"status": "error", "detail": "Not connected to Oura. Open Settings to connect."},
            status_code=400,
        )
    try:
        await oura.sync_all(days=30)
        last_sync = await db.get_config("last_sync")
        return Response({"status": "ok", "last_sync": last_sync})
    except Exception as e:
        msg = oura.error_message(e)
        await db.set_config("last_sync_error", msg)
        log.exception("Sync failed")
        return Response({"status": "error", "detail": msg}, status_code=502)


@post("/backfill")
async def trigger_backfill() -> dict:
    token = await db.get_config("oura_access_token")
    if not token:
        return {"error": "Not configured"}
    global _backfill_running
    if _backfill_running or await db.get_config("backfill_state") == "running":
        return {"status": "already_running"}
    _backfill_running = True
    asyncio.create_task(_run_backfill())
    return {"status": "started"}


@get("/api/backfill-status")
async def backfill_status() -> dict:
    state = await db.get_config("backfill_state") or "idle"
    start = await db.get_config("backfill_start")
    cursor = await db.get_config("backfill_cursor")
    progress = 0.0
    if start and cursor:
        start_d = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        cursor_d = datetime.fromisoformat(cursor).replace(tzinfo=timezone.utc)
        today = datetime.now(timezone.utc)
        span = (today - start_d).total_seconds()
        if span > 0:
            progress = max(0.0, min(1.0, (cursor_d - start_d).total_seconds() / span))
    return {"state": state, "start": start, "cursor": cursor, "progress": progress}


@post("/reset-data")
async def reset_data() -> dict:
    async with db.connect() as conn:
        await conn.execute("DELETE FROM samples")
        await conn.execute("DELETE FROM sleep_session_metrics")
        await conn.execute("DELETE FROM sleep_sessions")
        await conn.execute("DELETE FROM daily_metrics")
        await conn.commit()
    return {"status": "cleared"}




# ---------------------------------------------------------------------------
# Background sync
# ---------------------------------------------------------------------------

async def _background_sync():
    try:
        await oura.sync_all(days=30)
        log.info("Background sync completed")
    except Exception as e:
        await db.set_config("last_sync_error", oura.error_message(e))
        log.exception("Background sync failed")


_backfill_running = False


async def _run_backfill():
    global _backfill_running
    try:
        await oura.backfill()
        log.info("Backfill completed")
    except Exception:
        log.exception("Backfill failed")
    finally:
        _backfill_running = False


async def _periodic_sync():
    await asyncio.sleep(10)
    while True:
        token = await db.get_config("oura_access_token")
        if token:
            try:
                await oura.sync_all()
            except Exception as e:
                await db.set_config("last_sync_error", oura.error_message(e))
                log.exception("Periodic sync failed")
        await asyncio.sleep(600)


async def on_startup() -> None:
    await db.init_db()
    asyncio.create_task(_periodic_sync())
    if await db.get_config("backfill_state") == "running":
        global _backfill_running
        _backfill_running = True
        asyncio.create_task(_run_backfill())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _url_decode(s: str) -> str:
    from urllib.parse import unquote_plus
    return unquote_plus(s)


# ---------------------------------------------------------------------------
# HTML Templates
# ---------------------------------------------------------------------------

_TEMPLATES = Path(__file__).parent / "templates"
SETUP_HTML = (_TEMPLATES / "setup.html").read_text()
DASHBOARD_HTML = (_TEMPLATES / "dashboard.html").read_text()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = Litestar(
    route_handlers=[
        health_check, index, setup_page, start_oauth,
        oauth_callback, get_status, trigger_sync, reset_data,
        trigger_backfill, backfill_status,
        *service_routes,
    ],
    on_startup=[on_startup],
)
