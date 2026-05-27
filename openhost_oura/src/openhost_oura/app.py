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

    session_id = request.query_params.get("session_id")

    conditions = ["metric = ?"]
    params: list = [metric]
    if session_id:
        conditions.append("sleep_session_id = ?")
        params.append(int(session_id))
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

  <h2>Connect to Oura</h2>
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
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 1.5rem; max-width: 1100px; margin: 0 auto; }
  .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; }
  h1 { font-size: 1.5rem; }
  .actions { display: flex; gap: 0.75rem; }
  .btn { padding: 0.4rem 1rem; border-radius: 6px; border: none; font-size: 0.8rem;
         font-weight: 600; cursor: pointer; text-decoration: none; color: white; }
  .btn-primary { background: #6366f1; }
  .btn-secondary { background: #334155; color: #e2e8f0; }
  section { margin-bottom: 2rem; }
  .section-title { font-size: 1.15rem; font-weight: 600; margin-bottom: 0.75rem; display: flex; align-items: baseline; gap: 0.6rem; }
  .section-title .date { font-size: 0.85rem; color: #64748b; font-weight: 400; }
  .no-data { color: #475569; font-size: 0.9rem; padding: 1rem 0; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem; }
  .card { background: #1e293b; border-radius: 12px; padding: 1.25rem; }
  .card h3 { font-size: 0.85rem; color: #64748b; margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.03em; }
  .metrics { display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 0.5rem; }
  .metric { padding: 0.6rem; background: #0f172a; border-radius: 8px; }
  .metric .val { font-size: 1.25rem; font-weight: 700; }
  .metric .lbl { font-size: 0.7rem; color: #64748b; margin-top: 0.15rem; }
  .score-ring { display: inline-flex; align-items: center; justify-content: center;
    width: 56px; height: 56px; border-radius: 50%; font-size: 1.3rem; font-weight: 700; }
  .score-row { display: flex; align-items: center; gap: 1rem; margin-bottom: 0.75rem; }
  .score-label { font-size: 0.8rem; color: #94a3b8; }
  .contrib-bar { height: 6px; border-radius: 3px; background: #334155; margin-top: 0.25rem; }
  .contrib-fill { height: 100%; border-radius: 3px; }
  .contrib-item { margin-bottom: 0.5rem; }
  .contrib-head { display: flex; justify-content: space-between; font-size: 0.75rem; color: #94a3b8; }
  canvas { width: 100% !important; }
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
<div id="app"></div>

<script>
const C = { indigo:'#6366f1', cyan:'#06b6d4', emerald:'#10b981', amber:'#f59e0b',
            rose:'#f43f5e', purple:'#a855f7', slate:'#64748b', sky:'#38bdf8' };
const chartOpts = {
  responsive:true,
  plugins:{legend:{labels:{color:'#94a3b8',font:{size:11}}}},
  scales:{
    x:{ticks:{color:'#64748b',font:{size:10}},grid:{color:'#1e293b'}},
    y:{ticks:{color:'#64748b',font:{size:10}},grid:{color:'#1e293b'}},
  },
};
async function fetchJSON(u){return(await fetch(u)).json();}
function toH(s){return(s/3600).toFixed(1);}
function toHM(s){const h=Math.floor(s/3600),m=Math.round((s%3600)/60);return h>0?h+'h '+m+'m':m+'m';}
function scoreColor(v){return v>=85?C.emerald:v>=70?C.amber:C.rose;}

function getToday(){
  // Day boundary at 3am Pacific (UTC-7 or UTC-8 DST)
  const now=new Date();
  const pac=new Date(now.toLocaleString('en-US',{timeZone:'America/Los_Angeles'}));
  if(pac.getHours()<3) pac.setDate(pac.getDate()-1);
  return pac.toISOString().slice(0,10);
}
const DAYS=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
function fmtDate(iso){
  const d=new Date(iso+'T12:00:00');
  return DAYS[d.getDay()]+', '+d.toLocaleDateString('en-US',{month:'short',day:'numeric'});
}

async function doSync(){
  const b=document.querySelector('.btn-primary');b.textContent='Syncing...';b.disabled=true;
  try{await fetch('/sync',{method:'POST'});location.reload();}
  catch(e){b.textContent='Sync Failed';setTimeout(()=>{b.textContent='Sync Now';b.disabled=false;},2000);}
}

function contribBar(label,val,color){
  if(val==null)return'';
  return `<div class="contrib-item"><div class="contrib-head"><span>${label}</span><span>${Math.round(val)}</span></div><div class="contrib-bar"><div class="contrib-fill" style="width:${val}%;background:${color}"></div></div></div>`;
}

async function loadDashboard(){
  const [sessions,daily]=await Promise.all([
    fetchJSON('/api/v1/sleep-sessions?limit=60'),
    fetchJSON('/api/v1/daily'),
  ]);
  const allSess=sessions.data.slice().reverse();
  const sess=allSess.filter(s=>(s.metrics.total_sleep_duration||0)>=1800);
  const dailyData=daily.data;
  const today=getToday();

  // Find last real sleep session
  const lastReal=sessions.data.find(s=>(s.metrics.total_sleep_duration||0)>=1800);
  // Determine the "day" this session belongs to (end time, 3am boundary)
  let lastNightDay=null;
  if(lastReal){
    const end=new Date(lastReal.end_ts);
    const endPac=new Date(end.toLocaleString('en-US',{timeZone:'America/Los_Angeles'}));
    if(endPac.getHours()<3) endPac.setDate(endPac.getDate()-1);
    lastNightDay=endPac.toISOString().slice(0,10);
  }

  // Build daily metrics lookup by date
  const byDate={};
  dailyData.forEach(d=>{
    if(!byDate[d.date])byDate[d.date]={};
    byDate[d.date][d.metric]=d.value;
  });
  const todayMetrics=byDate[today]||null;
  // If no data for today, check yesterday
  const yesterdayISO=new Date(new Date(today+'T12:00:00').getTime()-86400000).toISOString().slice(0,10);

  let html='';

  // ---- LAST NIGHT'S SLEEP ----
  html+='<section id="last-night">';
  if(lastReal){
    const isToday=lastNightDay===today;
    html+=`<div class="section-title">Last Night's Sleep <span class="date">${fmtDate(lastNightDay)}${isToday?'':' (not current)'}</span></div>`;
    const m=lastReal.metrics;
    const sleepScore=byDate[lastNightDay]?.sleep_score;

    html+='<div class="grid">';

    // Sleep stats card
    html+='<div class="card"><h3>Sleep Summary</h3>';
    if(sleepScore!=null){
      html+=`<div class="score-row"><div class="score-ring" style="border:3px solid ${scoreColor(sleepScore)}">${Math.round(sleepScore)}</div><div><div style="font-weight:600">Sleep Score</div><div class="score-label">Total: ${toHM(m.total_sleep_duration||0)}</div></div></div>`;
    }
    html+='<div class="metrics">';
    const stats=[
      ['Total Sleep',toHM(m.total_sleep_duration||0)],
      ['Deep',toHM(m.deep_sleep_duration||0)],
      ['REM',toHM(m.rem_sleep_duration||0)],
      ['Light',toHM(m.light_sleep_duration||0)],
      ['Awake',toHM(m.awake_time||0)],
      ['Time in Bed',toHM(m.time_in_bed||0)],
      ['Avg HR',m.average_heart_rate!=null?Math.round(m.average_heart_rate)+' bpm':'--'],
      ['Lowest HR',m.lowest_heart_rate!=null?Math.round(m.lowest_heart_rate)+' bpm':'--'],
      ['Avg HRV',m.average_hrv!=null?Math.round(m.average_hrv)+' ms':'--'],
      ['Avg Breath',m.average_breath!=null?m.average_breath.toFixed(1)+'/min':'--'],
      ['Efficiency',m.efficiency!=null?Math.round(m.efficiency)+'%':'--'],
      ['Latency',m.latency!=null?toHM(m.latency):'--'],
    ];
    stats.forEach(([l,v])=>html+=`<div class="metric"><div class="val">${v}</div><div class="lbl">${l}</div></div>`);
    html+='</div>';

    // Sleep score contributors
    const sc=byDate[lastNightDay]||{};
    const contribs=[['Deep Sleep','sleep_score_deep_sleep'],['REM Sleep','sleep_score_rem_sleep'],
      ['Total Sleep','sleep_score_total_sleep'],['Efficiency','sleep_score_efficiency'],
      ['Restfulness','sleep_score_restfulness'],['Latency','sleep_score_latency'],['Timing','sleep_score_timing']];
    const hasContribs=contribs.some(([,k])=>sc[k]!=null);
    if(hasContribs){
      html+='<h3 style="margin-top:1rem">Score Breakdown</h3>';
      contribs.forEach(([label,key])=>html+=contribBar(label,sc[key],C.purple));
    }
    html+='</div>';

    // Charts card
    html+='<div class="card"><h3>Sleep Stages</h3><canvas id="lastStages"></canvas>';
    html+='<h3 style="margin-top:1rem">Heart Rate</h3><canvas id="lastHr"></canvas>';
    html+='<h3 style="margin-top:1rem">HRV</h3><canvas id="lastHrv"></canvas>';
    html+='</div>';

    html+='</div>';
  } else {
    html+='<div class="section-title">Last Night\\\'s Sleep</div><div class="no-data">No sleep data available</div>';
  }
  html+='</section>';

  // ---- TODAY ----
  html+='<section id="today">';
  if(todayMetrics){
    html+=`<div class="section-title">Today <span class="date">${fmtDate(today)}</span></div>`;
    html+='<div class="grid">';

    // Readiness card
    html+='<div class="card"><h3>Readiness</h3>';
    const rs=todayMetrics.readiness_score;
    if(rs!=null){
      html+=`<div class="score-row"><div class="score-ring" style="border:3px solid ${scoreColor(rs)}">${Math.round(rs)}</div><div style="font-weight:600">Readiness Score</div></div>`;
    }
    const rContribs=[['Resting HR','readiness_resting_heart_rate'],['HRV Balance','readiness_hrv_balance'],
      ['Body Temperature','readiness_body_temperature'],['Recovery Index','readiness_recovery_index'],
      ['Previous Night','readiness_previous_night'],['Sleep Balance','readiness_sleep_balance'],
      ['Activity Balance','readiness_activity_balance'],['Sleep Regularity','readiness_sleep_regularity']];
    rContribs.forEach(([label,key])=>html+=contribBar(label,todayMetrics[key],C.emerald));
    html+='</div>';

    // Body signals card
    html+='<div class="card"><h3>Body Signals</h3><div class="metrics">';
    const tempDev=todayMetrics.temperature_deviation;
    const tempTrend=todayMetrics.temperature_trend_deviation;
    if(tempDev!=null) html+=`<div class="metric"><div class="val">${tempDev>0?'+':''}${tempDev.toFixed(2)}&deg;</div><div class="lbl">Temp Deviation</div></div>`;
    if(tempTrend!=null) html+=`<div class="metric"><div class="val">${tempTrend>0?'+':''}${tempTrend.toFixed(2)}&deg;</div><div class="lbl">Temp Trend</div></div>`;
    html+='</div></div>';

    html+='</div>';
  } else {
    html+=`<div class="section-title">Today <span class="date">${fmtDate(today)}</span></div><div class="no-data">No data for today yet. Data usually appears after your first sync of the day.</div>`;
  }
  html+='</section>';

  // ---- HISTORY ----
  html+='<section id="history">';
  html+='<div class="section-title">History</div>';
  html+='<div class="grid">';
  html+='<div class="card"><h3>Sleep Duration</h3><canvas id="sleepDuration"></canvas></div>';
  html+='<div class="card"><h3>Readiness &amp; Sleep Score</h3><canvas id="scores"></canvas></div>';
  html+='<div class="card"><h3>Sleeping Heart Rate</h3><canvas id="hrChart"></canvas></div>';
  html+='<div class="card"><h3>Sleeping HRV</h3><canvas id="hrvChart"></canvas></div>';
  html+='</div></section>';

  document.getElementById('app').innerHTML=html;

  // ---- RENDER CHARTS ----

  // Last night charts
  if(lastReal){
    const [hrData,hrvData,stageData]=await Promise.all([
      fetchJSON(`/api/v1/samples?metric=heart_rate&session_id=${lastReal.id}&limit=500`),
      fetchJSON(`/api/v1/samples?metric=hrv&session_id=${lastReal.id}&limit=500`),
      fetchJSON(`/api/v1/samples?metric=sleep_stage&session_id=${lastReal.id}&limit=500`),
    ]);
    const timeFmt=d=>new Date(d.ts).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:'America/Los_Angeles'});

    if(stageData.data.length>0){
      const sLabels={1:'Deep',2:'Light',3:'REM',4:'Awake'};
      const sColors={1:C.indigo,2:C.slate,3:C.cyan,4:C.amber};
      new Chart(document.getElementById('lastStages'),{type:'bar',
        data:{labels:stageData.data.map(timeFmt),datasets:[{data:stageData.data.map(d=>d.value),backgroundColor:stageData.data.map(d=>sColors[d.value]||'#64748b'),barPercentage:1,categoryPercentage:1}]},
        options:{...chartOpts,plugins:{...chartOpts.plugins,legend:{display:false},tooltip:{callbacks:{label:c=>sLabels[c.raw]||c.raw}}},scales:{...chartOpts.scales,y:{...chartOpts.scales.y,min:0.5,max:4.5,ticks:{...chartOpts.scales.y.ticks,callback:v=>sLabels[v]||''}}}}});
    }
    if(hrData.data.length>0){
      new Chart(document.getElementById('lastHr'),{type:'line',
        data:{labels:hrData.data.map(timeFmt),datasets:[{label:'bpm',data:hrData.data.map(d=>d.value),borderColor:C.rose,tension:0.3,pointRadius:0,borderWidth:1.5}]},
        options:{...chartOpts,plugins:{...chartOpts.plugins,legend:{display:false}}}});
    }
    if(hrvData.data.length>0){
      new Chart(document.getElementById('lastHrv'),{type:'line',
        data:{labels:hrvData.data.map(timeFmt),datasets:[{label:'ms',data:hrvData.data.map(d=>d.value),borderColor:C.cyan,tension:0.3,pointRadius:0,borderWidth:1.5,fill:true,backgroundColor:'rgba(6,182,212,0.08)'}]},
        options:{...chartOpts,plugins:{...chartOpts.plugins,legend:{display:false}}}});
    }
  }

  // History charts
  if(sess.length>0){
    const labels=sess.map(s=>s.start_ts.slice(5,10));
    new Chart(document.getElementById('sleepDuration'),{type:'bar',
      data:{labels,datasets:[
        {label:'Deep',data:sess.map(s=>+(toH(s.metrics.deep_sleep_duration||0))),backgroundColor:C.indigo},
        {label:'REM',data:sess.map(s=>+(toH(s.metrics.rem_sleep_duration||0))),backgroundColor:C.cyan},
        {label:'Light',data:sess.map(s=>+(toH(s.metrics.light_sleep_duration||0))),backgroundColor:C.slate},
      ]},options:{...chartOpts,scales:{...chartOpts.scales,x:{...chartOpts.scales.x,stacked:true},y:{...chartOpts.scales.y,stacked:true}}}});

    new Chart(document.getElementById('hrChart'),{type:'line',
      data:{labels,datasets:[
        {label:'Avg',data:sess.map(s=>s.metrics.average_heart_rate??null),borderColor:C.rose,tension:0.3,pointRadius:2,spanGaps:true},
        {label:'Low',data:sess.map(s=>s.metrics.lowest_heart_rate??null),borderColor:C.amber,tension:0.3,pointRadius:2,spanGaps:true},
      ]},options:chartOpts});

    new Chart(document.getElementById('hrvChart'),{type:'line',
      data:{labels,datasets:[
        {label:'Avg HRV',data:sess.map(s=>s.metrics.average_hrv??null),borderColor:C.cyan,tension:0.3,pointRadius:2,spanGaps:true,fill:true,backgroundColor:'rgba(6,182,212,0.08)'},
      ]},options:chartOpts});
  }

  // Scores chart
  const readinessMap={},sleepScoreMap={};
  dailyData.forEach(d=>{
    if(d.metric==='readiness_score')readinessMap[d.date]=d.value;
    if(d.metric==='sleep_score')sleepScoreMap[d.date]=d.value;
  });
  const allDates=[...new Set([...Object.keys(readinessMap),...Object.keys(sleepScoreMap)])].sort();
  if(allDates.length>0){
    new Chart(document.getElementById('scores'),{type:'line',
      data:{labels:allDates.map(d=>d.slice(5)),datasets:[
        {label:'Readiness',data:allDates.map(d=>readinessMap[d]??null),borderColor:C.emerald,tension:0.3,pointRadius:2,spanGaps:true},
        {label:'Sleep',data:allDates.map(d=>sleepScoreMap[d]??null),borderColor:C.purple,tension:0.3,pointRadius:2,spanGaps:true},
      ]},options:{...chartOpts,scales:{...chartOpts.scales,y:{...chartOpts.scales.y,min:50,max:100}}}});
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
        health_check, index, setup_page, start_oauth,
        oauth_callback, trigger_sync, reset_data,
        list_metrics, query_samples, query_sleep_sessions, query_daily,
    ],
    on_startup=[on_startup],
)
