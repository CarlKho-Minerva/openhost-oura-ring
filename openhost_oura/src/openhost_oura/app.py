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

// Helpers for new service API format: scalar metrics are {value, ...} objects
function sv(obj){return obj?obj.value:null;}
function durMin(obj){return obj?obj.value:0;} // Duration scalars are in minutes

async function loadDashboard(){
  const [sessions,readinessTsResp,sleepScoreTsResp,tempDevResp,tempTrendResp]=await Promise.all([
    fetchJSON('/api/v1/sleep-sessions?limit=60'),
    fetchJSON('/api/v1/time-series?metric=readiness_score'),
    fetchJSON('/api/v1/time-series?metric=sleep_score'),
    fetchJSON('/api/v1/time-series?metric=temperature_deviation'),
    fetchJSON('/api/v1/time-series?metric=temperature_trend_deviation'),
  ]);

  // Also fetch readiness contributors for today
  const readinessContribMetrics=['readiness_activity_balance','readiness_body_temperature','readiness_hrv_balance',
    'readiness_previous_day_activity','readiness_previous_night','readiness_recovery_index',
    'readiness_resting_heart_rate','readiness_sleep_balance','readiness_sleep_regularity'];
  const sleepScoreContribMetrics=['sleep_score_deep_sleep','sleep_score_efficiency','sleep_score_latency',
    'sleep_score_rem_sleep','sleep_score_restfulness','sleep_score_timing','sleep_score_total_sleep'];

  const allSess=sessions.data.slice().reverse();
  // Filter out junk micro-sessions (< 30 min = 0.5 in minutes)
  const sess=allSess.filter(s=>durMin(s.total_duration)>=30);
  const today=getToday();

  // Find last real sleep session
  const lastReal=sessions.data.find(s=>durMin(s.total_duration)>=30);
  let lastNightDay=null;
  if(lastReal){
    const end=new Date(lastReal.end);
    const endPac=new Date(end.toLocaleString('en-US',{timeZone:'America/Los_Angeles'}));
    if(endPac.getHours()<3) endPac.setDate(endPac.getDate()-1);
    lastNightDay=endPac.toISOString().slice(0,10);
  }

  // Build daily lookups from time-series responses
  const readinessMap={},sleepScoreMap={},tempDevMap={},tempTrendMap={};
  (readinessTsResp.samples||[]).forEach(s=>{readinessMap[s.timestamp.slice(0,10)]=s.value;});
  (sleepScoreTsResp.samples||[]).forEach(s=>{sleepScoreMap[s.timestamp.slice(0,10)]=s.value;});
  (tempDevResp.samples||[]).forEach(s=>{tempDevMap[s.timestamp.slice(0,10)]=s.value;});
  (tempTrendResp.samples||[]).forEach(s=>{tempTrendMap[s.timestamp.slice(0,10)]=s.value;});

  // Fetch readiness contributors for today
  const todayContribs={};
  if(readinessMap[today]!=null){
    const contribResults=await Promise.all(readinessContribMetrics.map(m=>fetchJSON(`/api/v1/time-series?metric=${m}`)));
    readinessContribMetrics.forEach((m,i)=>{
      const samples=contribResults[i].samples||[];
      const todaySample=samples.find(s=>s.timestamp.slice(0,10)===today);
      if(todaySample) todayContribs[m]=todaySample.value;
    });
  }

  // Fetch sleep score contributors for last night
  const sleepContribs={};
  if(lastNightDay && sleepScoreMap[lastNightDay]!=null){
    const contribResults=await Promise.all(sleepScoreContribMetrics.map(m=>fetchJSON(`/api/v1/time-series?metric=${m}`)));
    sleepScoreContribMetrics.forEach((m,i)=>{
      const samples=contribResults[i].samples||[];
      const daySample=samples.find(s=>s.timestamp.slice(0,10)===lastNightDay);
      if(daySample) sleepContribs[m]=daySample.value;
    });
  }

  let html='';

  // ---- LAST NIGHT\\\'S SLEEP ----
  html+='<section id="last-night">';
  if(lastReal){
    const isToday=lastNightDay===today;
    html+=`<div class="section-title">Last Night\\\'s Sleep <span class="date">${fmtDate(lastNightDay)}${isToday?'':' (not current)'}</span></div>`;
    const sleepScore=sv(lastReal.sleep_score)||sleepScoreMap[lastNightDay];

    html+='<div class="grid">';

    // Sleep stats card
    html+='<div class="card"><h3>Sleep Summary</h3>';
    if(sleepScore!=null){
      html+=`<div class="score-row"><div class="score-ring" style="border:3px solid ${scoreColor(sleepScore)}">${Math.round(sleepScore)}</div><div><div style="font-weight:600">Sleep Score</div><div class="score-label">Total: ${toHM(durMin(lastReal.total_duration)*60)}</div></div></div>`;
    }
    html+='<div class="metrics">';
    const stats=[
      ['Total Sleep',toHM(durMin(lastReal.total_duration)*60)],
      ['Deep',toHM(durMin(lastReal.deep_sleep_duration)*60)],
      ['REM',toHM(durMin(lastReal.rem_sleep_duration)*60)],
      ['Light',toHM(durMin(lastReal.light_sleep_duration)*60)],
      ['Awake',toHM(durMin(lastReal.awake_time)*60)],
      ['Time in Bed',toHM(durMin(lastReal.time_in_bed)*60)],
      ['Avg HR',sv(lastReal.average_heart_rate)!=null?Math.round(sv(lastReal.average_heart_rate))+' bpm':'--'],
      ['Lowest HR',sv(lastReal.lowest_heart_rate)!=null?Math.round(sv(lastReal.lowest_heart_rate))+' bpm':'--'],
      ['Avg HRV',sv(lastReal.average_hrv)!=null?Math.round(sv(lastReal.average_hrv))+' ms':'--'],
      ['Avg Breath',sv(lastReal.average_breath)!=null?sv(lastReal.average_breath).toFixed(1)+'/min':'--'],
      ['Efficiency',sv(lastReal.efficiency)!=null?Math.round(sv(lastReal.efficiency))+'%':'--'],
      ['Latency',lastReal.latency?toHM(durMin(lastReal.latency)*60):'--'],
    ];
    stats.forEach(([l,v])=>html+=`<div class="metric"><div class="val">${v}</div><div class="lbl">${l}</div></div>`);
    html+='</div>';

    // Sleep score contributors
    const contribs=[['Deep Sleep','sleep_score_deep_sleep'],['REM Sleep','sleep_score_rem_sleep'],
      ['Total Sleep','sleep_score_total_sleep'],['Efficiency','sleep_score_efficiency'],
      ['Restfulness','sleep_score_restfulness'],['Latency','sleep_score_latency'],['Timing','sleep_score_timing']];
    const hasContribs=contribs.some(([,k])=>sleepContribs[k]!=null);
    if(hasContribs){
      html+='<h3 style="margin-top:1rem">Score Breakdown</h3>';
      contribs.forEach(([label,key])=>html+=contribBar(label,sleepContribs[key],C.purple));
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
  const todayReadiness=readinessMap[today];
  if(todayReadiness!=null){
    html+=`<div class="section-title">Today <span class="date">${fmtDate(today)}</span></div>`;
    html+='<div class="grid">';

    // Readiness card
    html+='<div class="card"><h3>Readiness</h3>';
    html+=`<div class="score-row"><div class="score-ring" style="border:3px solid ${scoreColor(todayReadiness)}">${Math.round(todayReadiness)}</div><div style="font-weight:600">Readiness Score</div></div>`;
    const rContribs=[['Resting HR','readiness_resting_heart_rate'],['HRV Balance','readiness_hrv_balance'],
      ['Body Temperature','readiness_body_temperature'],['Recovery Index','readiness_recovery_index'],
      ['Previous Night','readiness_previous_night'],['Sleep Balance','readiness_sleep_balance'],
      ['Activity Balance','readiness_activity_balance'],['Sleep Regularity','readiness_sleep_regularity']];
    rContribs.forEach(([label,key])=>html+=contribBar(label,todayContribs[key],C.emerald));
    html+='</div>';

    // Body signals card
    html+='<div class="card"><h3>Body Signals</h3><div class="metrics">';
    const tempDev=tempDevMap[today];
    const tempTrend=tempTrendMap[today];
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

  // Last night charts (data is embedded in the sleep session response)
  if(lastReal){
    const timeFmt=d=>new Date(d.timestamp).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZone:'America/Los_Angeles'});

    const stageData=lastReal.stages;
    if(stageData && stageData.samples && stageData.samples.length>0){
      const sLabels={deep:'Deep',light:'Light',rem:'REM',awake:'Awake'};
      const sColors={deep:C.indigo,light:C.slate,rem:C.cyan,awake:C.amber};
      const sNums={deep:1,light:2,rem:3,awake:4};
      new Chart(document.getElementById('lastStages'),{type:'bar',
        data:{labels:stageData.samples.map(timeFmt),datasets:[{data:stageData.samples.map(d=>sNums[d.value]||0),backgroundColor:stageData.samples.map(d=>sColors[d.value]||'#64748b'),barPercentage:1,categoryPercentage:1}]},
        options:{...chartOpts,plugins:{...chartOpts.plugins,legend:{display:false},tooltip:{callbacks:{label:c=>{const rev={1:'Deep',2:'Light',3:'REM',4:'Awake'};return rev[c.raw]||c.raw;}}}},scales:{...chartOpts.scales,y:{...chartOpts.scales.y,min:0.5,max:4.5,ticks:{...chartOpts.scales.y.ticks,callback:v=>{const m={1:'Deep',2:'Light',3:'REM',4:'Awake'};return m[v]||'';}}}}}});
    }

    const hrData=lastReal.heart_rate;
    if(hrData && hrData.samples && hrData.samples.length>0){
      new Chart(document.getElementById('lastHr'),{type:'line',
        data:{labels:hrData.samples.map(timeFmt),datasets:[{label:'bpm',data:hrData.samples.map(d=>d.value),borderColor:C.rose,tension:0.3,pointRadius:0,borderWidth:1.5}]},
        options:{...chartOpts,plugins:{...chartOpts.plugins,legend:{display:false}}}});
    }

    const hrvData=lastReal.hrv;
    if(hrvData && hrvData.samples && hrvData.samples.length>0){
      new Chart(document.getElementById('lastHrv'),{type:'line',
        data:{labels:hrvData.samples.map(timeFmt),datasets:[{label:'ms',data:hrvData.samples.map(d=>d.value),borderColor:C.cyan,tension:0.3,pointRadius:0,borderWidth:1.5,fill:true,backgroundColor:'rgba(6,182,212,0.08)'}]},
        options:{...chartOpts,plugins:{...chartOpts.plugins,legend:{display:false}}}});
    }
  }

  // History charts
  if(sess.length>0){
    const labels=sess.map(s=>s.start.slice(5,10));
    new Chart(document.getElementById('sleepDuration'),{type:'bar',
      data:{labels,datasets:[
        {label:'Deep',data:sess.map(s=>+(durMin(s.deep_sleep_duration)/60).toFixed(1)),backgroundColor:C.indigo},
        {label:'REM',data:sess.map(s=>+(durMin(s.rem_sleep_duration)/60).toFixed(1)),backgroundColor:C.cyan},
        {label:'Light',data:sess.map(s=>+(durMin(s.light_sleep_duration)/60).toFixed(1)),backgroundColor:C.slate},
      ]},options:{...chartOpts,scales:{...chartOpts.scales,x:{...chartOpts.scales.x,stacked:true},y:{...chartOpts.scales.y,stacked:true}}}});

    new Chart(document.getElementById('hrChart'),{type:'line',
      data:{labels,datasets:[
        {label:'Avg',data:sess.map(s=>sv(s.average_heart_rate)),borderColor:C.rose,tension:0.3,pointRadius:2,spanGaps:true},
        {label:'Low',data:sess.map(s=>sv(s.lowest_heart_rate)),borderColor:C.amber,tension:0.3,pointRadius:2,spanGaps:true},
      ]},options:chartOpts});

    new Chart(document.getElementById('hrvChart'),{type:'line',
      data:{labels,datasets:[
        {label:'Avg HRV',data:sess.map(s=>sv(s.average_hrv)),borderColor:C.cyan,tension:0.3,pointRadius:2,spanGaps:true,fill:true,backgroundColor:'rgba(6,182,212,0.08)'},
      ]},options:chartOpts});
  }

  // Scores chart
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
        *service_routes,
    ],
    on_startup=[on_startup],
)
