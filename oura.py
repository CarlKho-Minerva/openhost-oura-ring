import httpx
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import db

log = logging.getLogger(__name__)

OURA_API = "https://api.ouraring.com/v2"
OURA_AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
OURA_TOKEN_URL = "https://api.ouraring.com/oauth/token"
OURA_SCOPES = "daily heartrate personal session"


def get_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": OURA_SCOPES,
        "state": state,
    }
    return f"{OURA_AUTH_URL}?{urlencode(params)}"


async def exchange_code(
    code: str, redirect_uri: str, client_id: str, client_secret: str
) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            OURA_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
        resp.raise_for_status()
        return resp.json()


async def refresh_access_token() -> str | None:
    refresh_token = await db.get_config("oura_refresh_token")
    client_id = await db.get_config("oura_client_id")
    client_secret = await db.get_config("oura_client_secret")
    if not all([refresh_token, client_id, client_secret]):
        return None

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            OURA_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
        if resp.status_code != 200:
            log.error("Token refresh failed: %s", resp.text)
            return None
        tokens = resp.json()
        await db.set_config("oura_access_token", tokens["access_token"])
        if "refresh_token" in tokens:
            await db.set_config("oura_refresh_token", tokens["refresh_token"])
        return tokens["access_token"]


async def get_access_token() -> str | None:
    token = await db.get_config("oura_access_token")
    if token:
        return token
    return await refresh_access_token()


async def _fetch_paginated(
    client: httpx.AsyncClient, url: str, headers: dict, params: dict
) -> list:
    all_data = []
    p = dict(params)
    while True:
        resp = await client.get(url, headers=headers, params=p)
        resp.raise_for_status()
        body = resp.json()
        all_data.extend(body.get("data", []))
        next_token = body.get("next_token")
        if not next_token:
            break
        p["next_token"] = next_token
    return all_data


async def sync_all(days: int = 30):
    token = await get_access_token()
    if not token:
        raise RuntimeError("No Oura access token configured")

    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=60) as client:
        await _sync_sleep(client, headers, start_date, end_date)
        await _sync_heartrate(client, headers, start_date, end_date)
        await _sync_daily_readiness(client, headers, start_date, end_date)
        await _sync_daily_sleep(client, headers, start_date, end_date)

    await db.set_config("last_sync", datetime.now(timezone.utc).isoformat())
    log.info("Sync complete for %s to %s", start_date, end_date)


async def _sync_sleep(client, headers, start_date, end_date):
    records = await _fetch_paginated(
        client,
        f"{OURA_API}/usercollection/sleep",
        headers,
        {"start_date": start_date, "end_date": end_date},
    )
    log.info("Fetched %d sleep records", len(records))

    async with db.connect() as conn:
        for rec in records:
            source_id = rec.get("id")
            if not source_id:
                continue

            bedtime_start = rec.get("bedtime_start")
            bedtime_end = rec.get("bedtime_end")
            if not bedtime_start or not bedtime_end:
                continue

            cursor = await conn.execute(
                """INSERT INTO sleep_sessions (source, source_id, start_ts, end_ts)
                   VALUES ('oura', ?, ?, ?)
                   ON CONFLICT(source, source_id) DO UPDATE SET
                     start_ts = excluded.start_ts, end_ts = excluded.end_ts
                   RETURNING id""",
                (source_id, bedtime_start, bedtime_end),
            )
            row = await cursor.fetchone()
            session_id = row[0]

            await conn.execute(
                "DELETE FROM sleep_session_metrics WHERE sleep_session_id = ?",
                (session_id,),
            )
            await conn.execute(
                "DELETE FROM samples WHERE sleep_session_id = ?",
                (session_id,),
            )

            metric_fields = {
                "average_heart_rate": "average_heart_rate",
                "lowest_heart_rate": "lowest_heart_rate",
                "average_hrv": "average_hrv",
                "average_breath": "average_breath",
                "deep_sleep_duration": "deep_sleep_duration",
                "light_sleep_duration": "light_sleep_duration",
                "rem_sleep_duration": "rem_sleep_duration",
                "awake_time": "awake_time",
                "time_in_bed": "time_in_bed",
                "efficiency": "efficiency",
                "latency": "latency",
                "restless_periods": "restless_periods",
                "total_sleep_duration": "total_sleep_duration",
            }
            metrics = []
            for metric_name, field in metric_fields.items():
                val = rec.get(field)
                if val is not None:
                    metrics.append((session_id, metric_name, float(val)))

            total = sum(
                rec.get(f, 0) or 0
                for f in ["deep_sleep_duration", "light_sleep_duration", "rem_sleep_duration"]
            )
            if total > 0:
                metrics.append((session_id, "total_sleep_duration", float(total)))

            if metrics:
                await conn.executemany(
                    """INSERT INTO sleep_session_metrics (sleep_session_id, metric, value)
                       VALUES (?, ?, ?)""",
                    metrics,
                )

            hr_data = rec.get("heart_rate")
            if hr_data and hr_data.get("items"):
                _insert_interval_samples(
                    hr_data, "heart_rate", session_id, samples_buf := []
                )
                if samples_buf:
                    await conn.executemany(
                        """INSERT INTO samples (source, metric, start_ts, end_ts, value, sleep_session_id)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        samples_buf,
                    )

            hrv_data = rec.get("hrv")
            if hrv_data and hrv_data.get("items"):
                _insert_interval_samples(
                    hrv_data, "hrv", session_id, samples_buf := []
                )
                if samples_buf:
                    await conn.executemany(
                        """INSERT INTO samples (source, metric, start_ts, end_ts, value, sleep_session_id)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        samples_buf,
                    )

            phases = rec.get("sleep_phase_5_min")
            if phases and bedtime_start:
                _insert_sleep_stages(
                    phases, bedtime_start, session_id, samples_buf := []
                )
                if samples_buf:
                    await conn.executemany(
                        """INSERT INTO samples (source, metric, start_ts, end_ts, value, sleep_session_id)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        samples_buf,
                    )

        await conn.commit()


def _insert_interval_samples(
    data: dict, metric: str, session_id: int, buf: list
):
    interval = data.get("interval", 300)
    timestamp = data.get("timestamp")
    items = data.get("items", [])
    if not timestamp or not items:
        return

    base = datetime.fromisoformat(timestamp)
    for i, val in enumerate(items):
        if val is None:
            continue
        t = base + timedelta(seconds=i * interval)
        t_end = t + timedelta(seconds=interval)
        buf.append((
            "oura",
            metric,
            t.isoformat(),
            t_end.isoformat(),
            float(val),
            session_id,
        ))


STAGE_MAP = {"1": 1.0, "2": 2.0, "3": 3.0, "4": 4.0}


def _insert_sleep_stages(
    phases: str, bedtime_start: str, session_id: int, buf: list
):
    base = datetime.fromisoformat(bedtime_start)
    interval = 300
    for i, ch in enumerate(phases):
        val = STAGE_MAP.get(ch)
        if val is None:
            continue
        t = base + timedelta(seconds=i * interval)
        t_end = t + timedelta(seconds=interval)
        buf.append((
            "oura",
            "sleep_stage",
            t.isoformat(),
            t_end.isoformat(),
            val,
            session_id,
        ))


async def _sync_heartrate(client, headers, start_date, end_date):
    records = await _fetch_paginated(
        client,
        f"{OURA_API}/usercollection/heartrate",
        headers,
        {"start_date": start_date, "end_date": end_date},
    )
    log.info("Fetched %d heartrate records", len(records))

    async with db.connect() as conn:
        batch = []
        for rec in records:
            ts = rec.get("timestamp")
            bpm = rec.get("bpm")
            if ts is None or bpm is None:
                continue
            batch.append(("oura", "heart_rate", ts, None, float(bpm), None))

        if batch:
            await conn.executemany(
                """INSERT OR IGNORE INTO samples (source, metric, start_ts, end_ts, value, sleep_session_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                batch,
            )
            await conn.commit()


async def _sync_daily_readiness(client, headers, start_date, end_date):
    records = await _fetch_paginated(
        client,
        f"{OURA_API}/usercollection/daily_readiness",
        headers,
        {"start_date": start_date, "end_date": end_date},
    )
    log.info("Fetched %d readiness records", len(records))

    async with db.connect() as conn:
        for rec in records:
            day = rec.get("day")
            if not day:
                continue

            metrics = []
            score = rec.get("score")
            if score is not None:
                metrics.append(("oura", day, "readiness_score", float(score)))

            temp_dev = rec.get("temperature_deviation")
            if temp_dev is not None:
                metrics.append(("oura", day, "temperature_deviation", float(temp_dev)))

            temp_trend = rec.get("temperature_trend_deviation")
            if temp_trend is not None:
                metrics.append(("oura", day, "temperature_trend_deviation", float(temp_trend)))

            contributors = rec.get("contributors", {})
            for key, val in contributors.items():
                if val is not None:
                    metrics.append(("oura", day, f"readiness_{key}", float(val)))

            if metrics:
                await conn.executemany(
                    """INSERT OR REPLACE INTO daily_metrics (source, date, metric, value)
                       VALUES (?, ?, ?, ?)""",
                    metrics,
                )
        await conn.commit()


async def _sync_daily_sleep(client, headers, start_date, end_date):
    records = await _fetch_paginated(
        client,
        f"{OURA_API}/usercollection/daily_sleep",
        headers,
        {"start_date": start_date, "end_date": end_date},
    )
    log.info("Fetched %d daily_sleep records", len(records))

    async with db.connect() as conn:
        for rec in records:
            day = rec.get("day")
            if not day:
                continue

            metrics = []
            score = rec.get("score")
            if score is not None:
                metrics.append(("oura", day, "sleep_score", float(score)))

            contributors = rec.get("contributors", {})
            for key, val in contributors.items():
                if val is not None:
                    metrics.append(("oura", day, f"sleep_score_{key}", float(val)))

            if metrics:
                await conn.executemany(
                    """INSERT OR REPLACE INTO daily_metrics (source, date, metric, value)
                       VALUES (?, ?, ?, ?)""",
                    metrics,
                )
        await conn.commit()
