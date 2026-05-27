"""Health-data service provider API.

Implements the health-data service spec endpoints. Response JSON matches
the attrs types in health_data_service.data_types / specific_types so
that consumers can deserialize via cattrs.
"""

from __future__ import annotations

from litestar import Request, get

from . import db

SOURCE = "oura"

# Maps DB metric names to display metadata for the time-series endpoint
METRIC_INFO = {
    "heart_rate": ("Heart Rate", "bpm"),
    "hrv": ("Heart Rate Variability (RMSSD)", "ms"),
    "sleep_stage": ("Sleep Stages", None),
    # Daily metrics served as time series
    "readiness_score": ("Readiness Score", None),
    "readiness_activity_balance": ("Activity Balance", None),
    "readiness_body_temperature": ("Body Temperature", None),
    "readiness_hrv_balance": ("HRV Balance", None),
    "readiness_previous_day_activity": ("Previous Day Activity", None),
    "readiness_previous_night": ("Previous Night", None),
    "readiness_recovery_index": ("Recovery Index", None),
    "readiness_resting_heart_rate": ("Resting Heart Rate", None),
    "readiness_sleep_balance": ("Sleep Balance", None),
    "readiness_sleep_regularity": ("Sleep Regularity", None),
    "temperature_deviation": ("Temperature Deviation", "°C"),
    "temperature_trend_deviation": ("Temperature Trend Deviation", "°C"),
    "sleep_score": ("Sleep Score", None),
    "sleep_score_deep_sleep": ("Deep Sleep Score", None),
    "sleep_score_efficiency": ("Efficiency Score", None),
    "sleep_score_latency": ("Latency Score", None),
    "sleep_score_rem_sleep": ("REM Sleep Score", None),
    "sleep_score_restfulness": ("Restfulness Score", None),
    "sleep_score_timing": ("Timing Score", None),
    "sleep_score_total_sleep": ("Total Sleep Score", None),
}

# Sleep session metric names that represent durations (stored in seconds in DB,
# converted to minutes for the wire format)
DURATION_METRICS = {
    "total_sleep_duration", "deep_sleep_duration", "light_sleep_duration",
    "rem_sleep_duration", "awake_time", "time_in_bed", "latency",
}

# Map DB metric name → SleepSession field name (for scalar metrics)
SLEEP_SCALAR_FIELDS = {
    "total_sleep_duration": "total_duration",
    "deep_sleep_duration": "deep_sleep_duration",
    "light_sleep_duration": "light_sleep_duration",
    "rem_sleep_duration": "rem_sleep_duration",
    "awake_time": "awake_time",
    "time_in_bed": "time_in_bed",
    "latency": "latency",
    "average_heart_rate": "average_heart_rate",
    "lowest_heart_rate": "lowest_heart_rate",
    "average_hrv": "average_hrv",
    "average_breath": "average_breath",
    "efficiency": "efficiency",
    "restless_periods": "restless_periods",
}


def _scalar(metric_id: str, display_name: str, unit: str | None, value: float) -> dict:
    """Build a ScalarMetric-shaped dict."""
    return {
        "metric_id": metric_id,
        "display_name": display_name,
        "unit": unit,
        "value": value,
        "source": SOURCE,
    }


def _duration(field: str, seconds: float) -> dict:
    return _scalar("duration", field.replace("_", " ").title(), "min", seconds / 60.0)


# ---------------------------------------------------------------------------
# /v1/metrics
# ---------------------------------------------------------------------------

@get("/api/v1/metrics")
async def service_list_metrics() -> dict:
    metrics = []
    async with db.connect() as conn:
        sample_metrics = await (await conn.execute(
            "SELECT DISTINCT metric FROM samples ORDER BY metric"
        )).fetchall()
        for r in sample_metrics:
            name = r[0]
            display, unit = METRIC_INFO.get(name, (name, None))
            metrics.append({"metric_id": name, "display_name": display, "unit": unit})

        daily = await (await conn.execute(
            "SELECT DISTINCT metric FROM daily_metrics ORDER BY metric"
        )).fetchall()
        for r in daily:
            name = r[0]
            if not any(m["metric_id"] == name for m in metrics):
                display, unit = METRIC_INFO.get(name, (name, None))
                metrics.append({"metric_id": name, "display_name": display, "unit": unit})

    return {"metrics": metrics}


# ---------------------------------------------------------------------------
# /v1/time-series
# ---------------------------------------------------------------------------

@get("/api/v1/time-series")
async def service_time_series(request: Request) -> dict:
    metric = request.query_params.get("metric")
    start = request.query_params.get("start")
    end = request.query_params.get("end")
    limit = request.query_params.get("limit")

    if not metric:
        return {"error": "metric parameter required"}

    display, unit = METRIC_INFO.get(metric, (metric, None))

    # Check if it's a daily metric
    is_daily = metric.startswith("readiness_") or metric.startswith("sleep_score") or metric.startswith("temperature_")

    if is_daily:
        return await _daily_time_series(metric, display, unit, start, end, limit)

    conditions = ["metric = ?"]
    params: list = [metric]
    if start:
        conditions.append("start_ts >= ?")
        params.append(start)
    if end:
        conditions.append("start_ts <= ?")
        params.append(end)
    if limit:
        params.append(int(limit))

    where = " AND ".join(conditions)
    limit_clause = " LIMIT ?" if limit else ""

    async with db.connect() as conn:
        rows = await (await conn.execute(
            f"SELECT start_ts, end_ts, value FROM samples WHERE {where} ORDER BY start_ts{limit_clause}",
            params,
        )).fetchall()

    samples = []
    for r in rows:
        s: dict = {"timestamp": r[0], "value": r[2]}
        if r[1]:
            s["end_timestamp"] = r[1]
        samples.append(s)

    return {
        "metric_id": metric,
        "display_name": display,
        "unit": unit,
        "source": SOURCE,
        "samples": samples,
    }


async def _daily_time_series(metric, display, unit, start, end, limit):
    conditions = ["metric = ?"]
    params: list = [metric]
    if start:
        conditions.append("date >= ?")
        params.append(start[:10] if len(start) > 10 else start)
    if end:
        conditions.append("date <= ?")
        params.append(end[:10] if len(end) > 10 else end)
    if limit:
        params.append(int(limit))

    where = " AND ".join(conditions)
    limit_clause = " LIMIT ?" if limit else ""

    async with db.connect() as conn:
        rows = await (await conn.execute(
            f"SELECT date, value FROM daily_metrics WHERE {where} ORDER BY date{limit_clause}",
            params,
        )).fetchall()

    samples = [{"timestamp": f"{r[0]}T00:00:00+00:00", "value": r[1]} for r in rows]
    return {
        "metric_id": metric,
        "display_name": display,
        "unit": unit,
        "source": SOURCE,
        "samples": samples,
    }


# ---------------------------------------------------------------------------
# /v1/sleep-sessions
# ---------------------------------------------------------------------------

@get("/api/v1/sleep-sessions")
async def service_sleep_sessions(request: Request) -> dict:
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
            session_id = s[0]
            session: dict = {
                "start": s[3],
                "end": s[4],
                "source": s[1],
                "id": str(session_id),
            }

            # Scalar metrics
            metrics = await (await conn.execute(
                "SELECT metric, value FROM sleep_session_metrics WHERE sleep_session_id = ?",
                (session_id,),
            )).fetchall()

            for m_name, m_val in metrics:
                field = SLEEP_SCALAR_FIELDS.get(m_name)
                if not field:
                    continue
                if m_name in DURATION_METRICS:
                    session[field] = _duration(m_name, m_val)
                elif m_name == "efficiency":
                    session[field] = _scalar("efficiency", "Efficiency", "%", m_val)
                elif m_name == "restless_periods":
                    session[field] = _scalar("count", "Restless Periods", None, m_val)
                elif m_name == "average_heart_rate":
                    session[field] = _scalar("average_heart_rate", "Avg Heart Rate", "bpm", m_val)
                elif m_name == "lowest_heart_rate":
                    session[field] = _scalar("lowest_heart_rate", "Lowest Heart Rate", "bpm", m_val)
                elif m_name == "average_hrv":
                    session[field] = _scalar("average_hrv", "Avg HRV", "ms", m_val)
                elif m_name == "average_breath":
                    session[field] = _scalar("average_breath", "Avg Breath Rate", "breaths/min", m_val)

            # Sleep stages
            stage_rows = await (await conn.execute(
                """SELECT start_ts, end_ts, value FROM samples
                   WHERE sleep_session_id = ? AND metric = 'sleep_stage'
                   ORDER BY start_ts""",
                (session_id,),
            )).fetchall()

            if stage_rows:
                stage_map = {1.0: "deep", 2.0: "light", 3.0: "rem", 4.0: "awake"}
                session["stages"] = {
                    "metric_id": "sleep_stages",
                    "display_name": "Sleep Stages",
                    "unit": None,
                    "source": SOURCE,
                    "samples": [
                        {
                            "timestamp": r[0],
                            "end_timestamp": r[1],
                            "value": stage_map.get(r[2], "unknown"),
                        }
                        for r in stage_rows
                    ],
                }

            # HR time series
            hr_rows = await (await conn.execute(
                """SELECT start_ts, end_ts, value FROM samples
                   WHERE sleep_session_id = ? AND metric = 'heart_rate'
                   ORDER BY start_ts""",
                (session_id,),
            )).fetchall()

            if hr_rows:
                hr_samples = []
                for r in hr_rows:
                    s_dict: dict = {"timestamp": r[0], "value": r[2]}
                    if r[1]:
                        s_dict["end_timestamp"] = r[1]
                    hr_samples.append(s_dict)
                session["heart_rate"] = {
                    "metric_id": "heart_rate",
                    "display_name": "Heart Rate",
                    "unit": "bpm",
                    "source": SOURCE,
                    "samples": hr_samples,
                }

            # HRV time series
            hrv_rows = await (await conn.execute(
                """SELECT start_ts, end_ts, value FROM samples
                   WHERE sleep_session_id = ? AND metric = 'hrv'
                   ORDER BY start_ts""",
                (session_id,),
            )).fetchall()

            if hrv_rows:
                hrv_samples = []
                for r in hrv_rows:
                    s_dict = {"timestamp": r[0], "value": r[2]}
                    if r[1]:
                        s_dict["end_timestamp"] = r[1]
                    hrv_samples.append(s_dict)
                session["hrv"] = {
                    "metric_id": "hrv_rmssd",
                    "display_name": "Heart Rate Variability (RMSSD)",
                    "unit": None,
                    "source": SOURCE,
                    "samples": hrv_samples,
                }

            # Sleep score from daily_metrics
            date_str = s[3][:10]
            score_row = await (await conn.execute(
                "SELECT value FROM daily_metrics WHERE date = ? AND metric = 'sleep_score'",
                (date_str,),
            )).fetchone()
            if score_row:
                session["sleep_score"] = _scalar("score", "Sleep Score", None, score_row[0])

            result.append(session)

    return {"data": result}


# ---------------------------------------------------------------------------
# /v1/workouts
# ---------------------------------------------------------------------------

@get("/api/v1/workouts")
async def service_workouts(request: Request) -> dict:
    return {"data": []}


# ---------------------------------------------------------------------------
# Route list for registration
# ---------------------------------------------------------------------------

service_routes = [
    service_list_metrics,
    service_time_series,
    service_sleep_sessions,
    service_workouts,
]
