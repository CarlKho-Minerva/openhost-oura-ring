import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timezone
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
    status = "connected" if token else "not connected"
    html = SETUP_HTML.replace("{{client_id}}", client_id).replace(
        "{{status}}", status
    ).replace("{{last_sync}}", last_sync)
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
    return {"last_sync": last_sync}


@post("/sync")
async def trigger_sync() -> dict:
    token = await db.get_config("oura_access_token")
    if not token:
        return {"error": "Not configured"}
    try:
        await oura.sync_all(days=30)
        last_sync = await db.get_config("last_sync")
        return {"status": "ok", "last_sync": last_sync}
    except Exception as e:
        log.exception("Sync failed")
        return {"status": "error", "detail": str(e)}


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
    except Exception:
        log.exception("Background sync failed")


async def _periodic_sync():
    await asyncio.sleep(10)
    while True:
        token = await db.get_config("oura_access_token")
        if token:
            try:
                await oura.sync_all()
            except Exception:
                log.exception("Periodic sync failed")
        await asyncio.sleep(600)


async def on_startup() -> None:
    await db.init_db()
    asyncio.create_task(_periodic_sync())


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
        *service_routes,
    ],
    on_startup=[on_startup],
)
