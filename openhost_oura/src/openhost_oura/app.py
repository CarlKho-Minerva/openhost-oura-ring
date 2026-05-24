import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timezone

from litestar import Litestar, Request, get, post
from litestar.response import Redirect, Response
from litestar.exceptions import NotFoundException

from . import db
from . import oura

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


@post("/setup/pat")
async def save_pat(request: Request) -> Response:
    body = await request.body()
    params = dict(p.split("=", 1) for p in body.decode().split("&") if "=" in p)
    pat = _url_decode(params.get("pat", "")).strip()
    if not pat:
        return Response(content="Personal access token required", status_code=400)

    await db.set_config("oura_access_token", pat)
    asyncio.create_task(_background_sync())
    return Redirect("/")


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


@post("/sync")
async def trigger_sync() -> dict:
    token = await db.get_config("oura_access_token")
    if not token:
        return {"error": "Not configured"}
    try:
        await oura.sync_all(days=30)
        return {"status": "ok"}
    except Exception as e:
        log.exception("Sync failed")
        return {"status": "error", "detail": str(e)}


# ---------------------------------------------------------------------------
# Health Data API (generic, source-agnostic)
# ---------------------------------------------------------------------------

@get("/api/v1/metrics")
async def list_metrics() -> dict:
    async with db.connect() as conn:
        rows = await (await conn.execute(
            """SELECT DISTINCT metric, 'sample' as type FROM samples
               UNION SELECT DISTINCT metric, 'sleep_session' FROM sleep_session_metrics
               UNION SELECT DISTINCT metric, 'daily' FROM daily_metrics
               ORDER BY type, metric"""
        )).fetchall()
    return {"metrics": [{"name": r[0], "type": r[1]} for r in rows]}


@get("/api/v1/samples")
async def query_samples(request: Request) -> dict:
    metric = request.query_params.get("metric")
    start = request.query_params.get("start")
    end = request.query_params.get("end")
    limit = int(request.query_params.get("limit", "10000"))
    agg = request.query_params.get("agg")
    interval = request.query_params.get("interval")

    if not metric:
        return {"error": "metric parameter required"}

    if agg and interval:
        return await _aggregated_samples(metric, start, end, agg, interval)

    conditions = ["metric = ?"]
    params: list = [metric]
    if start:
        conditions.append("start_ts >= ?")
        params.append(start)
    if end:
        conditions.append("start_ts <= ?")
        params.append(end)
    params.append(limit)

    where = " AND ".join(conditions)
    async with db.connect() as conn:
        rows = await (await conn.execute(
            f"""SELECT start_ts, end_ts, value FROM samples
                WHERE {where} ORDER BY start_ts LIMIT ?""",
            params,
        )).fetchall()

    return {
        "metric": metric,
        "count": len(rows),
        "data": [{"ts": r[0], "end_ts": r[1], "value": r[2]} for r in rows],
    }


async def _aggregated_samples(metric, start, end, agg, interval):
    agg_fn = {"avg": "AVG", "min": "MIN", "max": "MAX", "sum": "SUM", "count": "COUNT"}.get(agg)
    if not agg_fn:
        return {"error": f"Unknown aggregation: {agg}"}

    trunc = {
        "5m": "%Y-%m-%dT%H:%M", "1h": "%Y-%m-%dT%H", "1d": "%Y-%m-%d",
    }.get(interval)
    if not trunc:
        return {"error": f"Unknown interval: {interval}. Use 5m, 1h, or 1d"}

    conditions = ["metric = ?"]
    params: list = [metric]
    if start:
        conditions.append("start_ts >= ?")
        params.append(start)
    if end:
        conditions.append("start_ts <= ?")
        params.append(end)
    where = " AND ".join(conditions)

    async with db.connect() as conn:
        rows = await (await conn.execute(
            f"""SELECT strftime('{trunc}', start_ts) as bucket,
                       {agg_fn}(value) as val, COUNT(*) as n
                FROM samples WHERE {where}
                GROUP BY bucket ORDER BY bucket""",
            params,
        )).fetchall()

    return {
        "metric": metric,
        "aggregation": agg,
        "interval": interval,
        "count": len(rows),
        "data": [{"ts": r[0], "value": r[1], "n": r[2]} for r in rows],
    }


@get("/api/v1/sleep-sessions")
async def query_sleep_sessions(request: Request) -> dict:
    start = request.query_params.get("start")
    end = request.query_params.get("end")
    limit = int(request.query_params.get("limit", "100"))

    conditions = []
    params: list = []
    if start:
        conditions.append("s.start_ts >= ?")
        params.append(start)
    if end:
        conditions.append("s.end_ts <= ?")
        params.append(end)
    params.append(limit)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    async with db.connect() as conn:
        sessions = await (await conn.execute(
            f"""SELECT s.id, s.source, s.source_id, s.start_ts, s.end_ts
                FROM sleep_sessions s {where}
                ORDER BY s.start_ts DESC LIMIT ?""",
            params,
        )).fetchall()

        result = []
        for s in sessions:
            metrics = await (await conn.execute(
                "SELECT metric, value FROM sleep_session_metrics WHERE sleep_session_id = ?",
                (s[0],),
            )).fetchall()
            result.append({
                "id": s[0],
                "source": s[1],
                "start_ts": s[3],
                "end_ts": s[4],
                "metrics": {m[0]: m[1] for m in metrics},
            })

    return {"count": len(result), "data": result}


@get("/api/v1/daily")
async def query_daily(request: Request) -> dict:
    metric = request.query_params.get("metric")
    start = request.query_params.get("start")
    end = request.query_params.get("end")

    conditions = []
    params: list = []
    if metric:
        conditions.append("metric = ?")
        params.append(metric)
    if start:
        conditions.append("date >= ?")
        params.append(start)
    if end:
        conditions.append("date <= ?")
        params.append(end)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    async with db.connect() as conn:
        rows = await (await conn.execute(
            f"SELECT date, metric, value FROM daily_metrics {where} ORDER BY date, metric",
            params,
        )).fetchall()

    return {
        "count": len(rows),
        "data": [{"date": r[0], "metric": r[1], "value": r[2]} for r in rows],
    }


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
                await oura.sync_all(days=7)
            except Exception:
                log.exception("Periodic sync failed")
        await asyncio.sleep(6 * 3600)


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
# HTML
# ---------------------------------------------------------------------------

SETUP_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Health Data - Setup</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
         background: #0f172a; color: #e2e8f0; min-height: 100vh;
         display: flex; align-items: center; justify-content: center; }
  .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 480px; width: 100%; }
  h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
  .status { font-size: 0.875rem; color: #94a3b8; margin-bottom: 1.5rem; }
  .divider { border: none; border-top: 1px solid #334155; margin: 1.5rem 0; }
  h2 { font-size: 1.1rem; margin-bottom: 1rem; color: #cbd5e1; }
  label { display: block; font-size: 0.875rem; color: #94a3b8; margin-bottom: 0.25rem; }
  input[type=text], input[type=password] {
    width: 100%; padding: 0.5rem 0.75rem; border-radius: 6px; border: 1px solid #334155;
    background: #0f172a; color: #e2e8f0; font-size: 0.875rem; margin-bottom: 0.75rem;
    font-family: monospace;
  }
  button {
    width: 100%; padding: 0.6rem; border-radius: 6px; border: none;
    background: #6366f1; color: white; font-size: 0.875rem; font-weight: 600;
    cursor: pointer; margin-top: 0.5rem;
  }
  button:hover { background: #4f46e5; }
  .alt-btn { background: #334155; }
  .alt-btn:hover { background: #475569; }
</style>
</head>
<body>
<div class="card">
  <h1>Health Data</h1>
  <p class="status">Status: {{status}} &middot; Last sync: {{last_sync}}</p>

  <h2>Option 1: Personal Access Token</h2>
  <form method="POST" action="/setup/pat">
    <label>Oura Personal Access Token</label>
    <input type="password" name="pat" placeholder="Paste your token here">
    <button type="submit" class="alt-btn">Save &amp; Sync</button>
  </form>

  <hr class="divider">

  <h2>Option 2: OAuth2</h2>
  <form method="POST" action="/setup/oauth">
    <label>Client ID</label>
    <input type="text" name="client_id" value="{{client_id}}" placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx">
    <label>Client Secret</label>
    <input type="password" name="client_secret" placeholder="Your client secret">
    <button type="submit">Connect to Oura</button>
  </form>
</div>
</body>
</html>"""


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Health Data</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 1.5rem; }
  .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; }
  h1 { font-size: 1.5rem; }
  .actions { display: flex; gap: 0.75rem; }
  .btn { padding: 0.4rem 1rem; border-radius: 6px; border: none; font-size: 0.8rem;
         font-weight: 600; cursor: pointer; text-decoration: none; }
  .btn-primary { background: #6366f1; color: white; }
  .btn-secondary { background: #334155; color: #e2e8f0; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 1rem; }
  .card { background: #1e293b; border-radius: 12px; padding: 1.25rem; }
  .card h2 { font-size: 1rem; color: #94a3b8; margin-bottom: 0.75rem; }
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.75rem;
           margin-bottom: 1rem; }
  .stat { background: #1e293b; border-radius: 10px; padding: 1rem; text-align: center; }
  .stat .value { font-size: 1.75rem; font-weight: 700; }
  .stat .label { font-size: 0.75rem; color: #94a3b8; margin-top: 0.25rem; }
  canvas { width: 100% !important; }
  .loading { text-align: center; padding: 3rem; color: #64748b; }
</style>
</head>
<body>
<div class="header">
  <h1>Health Data</h1>
  <div class="actions">
    <button class="btn btn-primary" onclick="doSync()">Sync Now</button>
    <a class="btn btn-secondary" href="/setup">Settings</a>
  </div>
</div>

<div class="stats" id="stats"></div>
<div class="grid">
  <div class="card"><h2>Sleep Duration</h2><canvas id="sleepDuration"></canvas></div>
  <div class="card"><h2>Readiness &amp; Sleep Score</h2><canvas id="scores"></canvas></div>
  <div class="card"><h2>Sleeping Heart Rate</h2><canvas id="hrChart"></canvas></div>
  <div class="card"><h2>Sleeping HRV</h2><canvas id="hrvChart"></canvas></div>
  <div class="card"><h2>Last Night - Heart Rate</h2><canvas id="lastHr"></canvas></div>
  <div class="card"><h2>Last Night - Sleep Stages</h2><canvas id="lastStages"></canvas></div>
</div>

<script>
const CHART_COLORS = {
  indigo: '#6366f1', cyan: '#06b6d4', emerald: '#10b981', amber: '#f59e0b',
  rose: '#f43f5e', purple: '#a855f7', slate: '#64748b',
};
const chartDefaults = {
  responsive: true,
  plugins: { legend: { labels: { color: '#94a3b8', font: { size: 11 } } } },
  scales: {
    x: { ticks: { color: '#64748b', font: { size: 10 } }, grid: { color: '#1e293b' } },
    y: { ticks: { color: '#64748b', font: { size: 10 } }, grid: { color: '#1e293b' } },
  },
};

async function fetchJSON(url) {
  const r = await fetch(url);
  return r.json();
}

function toHours(sec) { return (sec / 3600).toFixed(1); }

async function doSync() {
  const btn = document.querySelector('.btn-primary');
  btn.textContent = 'Syncing...';
  btn.disabled = true;
  try {
    await fetch('/sync', { method: 'POST' });
    location.reload();
  } catch(e) {
    btn.textContent = 'Sync Failed';
    setTimeout(() => { btn.textContent = 'Sync Now'; btn.disabled = false; }, 2000);
  }
}

async function loadDashboard() {
  const [sessions, daily] = await Promise.all([
    fetchJSON('/api/v1/sleep-sessions?limit=60'),
    fetchJSON('/api/v1/daily'),
  ]);

  const allSess = sessions.data.slice().reverse();
  // Filter out junk micro-sessions (< 30 min sleep)
  const sess = allSess.filter(s => (s.metrics.total_sleep_duration || 0) >= 1800);
  const dailyData = daily.data;

  // Find last real sleep session for "last night" charts
  const lastReal = sessions.data.find(s => (s.metrics.total_sleep_duration || 0) >= 1800);

  // Summary stats (from last 7 real sessions)
  if (sess.length > 0) {
    const recent = sess.slice(-7);
    const avgSleep = recent.reduce((s, d) => s + (d.metrics.total_sleep_duration || 0), 0) / recent.length;
    const avgHR = recent.reduce((s, d) => s + (d.metrics.average_heart_rate || 0), 0) / recent.length;
    const avgHRV = recent.reduce((s, d) => s + (d.metrics.average_hrv || 0), 0) / recent.length;
    const avgEff = recent.reduce((s, d) => s + (d.metrics.efficiency || 0), 0) / recent.length;

    document.getElementById('stats').innerHTML = [
      { value: toHours(avgSleep) + 'h', label: 'Avg Sleep (7d)' },
      { value: Math.round(avgHR) + ' bpm', label: 'Avg Sleeping HR' },
      { value: Math.round(avgHRV) + ' ms', label: 'Avg Sleeping HRV' },
      { value: Math.round(avgEff) + '%', label: 'Avg Efficiency' },
    ].map(s => `<div class="stat"><div class="value">${s.value}</div><div class="label">${s.label}</div></div>`).join('');
  }

  // Sleep duration stacked bar
  if (sess.length > 0) {
    const labels = sess.map(s => s.start_ts.slice(0, 10));
    new Chart(document.getElementById('sleepDuration'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'Deep', data: sess.map(s => toHours(s.metrics.deep_sleep_duration || 0)), backgroundColor: CHART_COLORS.indigo },
          { label: 'REM', data: sess.map(s => toHours(s.metrics.rem_sleep_duration || 0)), backgroundColor: CHART_COLORS.cyan },
          { label: 'Light', data: sess.map(s => toHours(s.metrics.light_sleep_duration || 0)), backgroundColor: CHART_COLORS.slate },
        ],
      },
      options: { ...chartDefaults, scales: { ...chartDefaults.scales, x: { ...chartDefaults.scales.x, stacked: true }, y: { ...chartDefaults.scales.y, stacked: true, title: { display: true, text: 'Hours', color: '#64748b' } } } },
    });
  }

  // Readiness + sleep scores (date-aligned)
  const readinessMap = {};
  const sleepScoreMap = {};
  dailyData.forEach(d => {
    if (d.metric === 'readiness_score') readinessMap[d.date] = d.value;
    if (d.metric === 'sleep_score') sleepScoreMap[d.date] = d.value;
  });
  const allDates = [...new Set([...Object.keys(readinessMap), ...Object.keys(sleepScoreMap)])].sort();
  if (allDates.length > 0) {
    new Chart(document.getElementById('scores'), {
      type: 'line',
      data: {
        labels: allDates,
        datasets: [
          { label: 'Readiness', data: allDates.map(d => readinessMap[d] ?? null), borderColor: CHART_COLORS.emerald, tension: 0.3, pointRadius: 2, spanGaps: true },
          { label: 'Sleep Score', data: allDates.map(d => sleepScoreMap[d] ?? null), borderColor: CHART_COLORS.purple, tension: 0.3, pointRadius: 2, spanGaps: true },
        ],
      },
      options: { ...chartDefaults, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, min: 0, max: 100 } } },
    });
  }

  // Sleeping HR trend
  if (sess.length > 0) {
    const labels = sess.map(s => s.start_ts.slice(0, 10));
    new Chart(document.getElementById('hrChart'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Avg HR', data: sess.map(s => s.metrics.average_heart_rate ?? null), borderColor: CHART_COLORS.rose, tension: 0.3, pointRadius: 2, spanGaps: true },
          { label: 'Lowest HR', data: sess.map(s => s.metrics.lowest_heart_rate ?? null), borderColor: CHART_COLORS.amber, tension: 0.3, pointRadius: 2, spanGaps: true },
        ],
      },
      options: chartDefaults,
    });
  }

  // Sleeping HRV trend
  if (sess.length > 0) {
    const labels = sess.map(s => s.start_ts.slice(0, 10));
    new Chart(document.getElementById('hrvChart'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Avg HRV', data: sess.map(s => s.metrics.average_hrv ?? null), borderColor: CHART_COLORS.cyan, tension: 0.3, pointRadius: 2, fill: true, backgroundColor: 'rgba(6,182,212,0.1)', spanGaps: true },
        ],
      },
      options: chartDefaults,
    });
  }

  // Last night HR samples
  if (lastReal) {
    const hrSamples = await fetchJSON(`/api/v1/samples?metric=heart_rate&start=${lastReal.start_ts}&end=${lastReal.end_ts}&limit=500`);
    if (hrSamples.data.length > 0) {
      new Chart(document.getElementById('lastHr'), {
        type: 'line',
        data: {
          labels: hrSamples.data.map(d => new Date(d.ts).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})),
          datasets: [{ label: 'HR (bpm)', data: hrSamples.data.map(d => d.value), borderColor: CHART_COLORS.rose, tension: 0.3, pointRadius: 0 }],
        },
        options: chartDefaults,
      });
    }
  }

  // Last night sleep stages
  if (lastReal) {
    const stages = await fetchJSON(`/api/v1/samples?metric=sleep_stage&start=${lastReal.start_ts}&end=${lastReal.end_ts}&limit=500`);
    if (stages.data.length > 0) {
      const stageLabels = { 1: 'Deep', 2: 'Light', 3: 'REM', 4: 'Awake' };
      const stageColors = { 1: CHART_COLORS.indigo, 2: CHART_COLORS.slate, 3: CHART_COLORS.cyan, 4: CHART_COLORS.amber };
      new Chart(document.getElementById('lastStages'), {
        type: 'bar',
        data: {
          labels: stages.data.map(d => new Date(d.ts).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})),
          datasets: [{
            label: 'Stage',
            data: stages.data.map(d => d.value),
            backgroundColor: stages.data.map(d => stageColors[d.value] || '#64748b'),
          }],
        },
        options: {
          ...chartDefaults,
          plugins: {
            ...chartDefaults.plugins,
            tooltip: {
              callbacks: { label: ctx => stageLabels[ctx.raw] || ctx.raw }
            }
          },
          scales: {
            ...chartDefaults.scales,
            y: { ...chartDefaults.scales.y, min: 0.5, max: 4.5,
              ticks: { ...chartDefaults.scales.y.ticks, callback: v => stageLabels[v] || '' } },
          },
        },
      });
    }
  }
}

loadDashboard();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = Litestar(
    route_handlers=[
        health_check, index, setup_page, start_oauth, save_pat,
        oauth_callback, trigger_sync,
        list_metrics, query_samples, query_sleep_sessions, query_daily,
    ],
    on_startup=[on_startup],
)
