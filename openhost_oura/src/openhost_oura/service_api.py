"""Health-data service provider API.

Implements the health-data service spec endpoints using the typed
models from health_data_service.
"""

from __future__ import annotations

from datetime import datetime, timezone

import attrs
from health_data_service import (
    IntervalSample,
    MetricKind,
    MetricType,
    Sample,
    SleepSession,
    TimeSeries,
)
from health_data_service.specific_types import (
    BreathRateAvg,
    Count,
    Duration,
    Efficiency,
    HeartRate,
    HeartRateAvg,
    HeartRateMin,
    HRV_RMSSD,
    HRVAvg,
    Score,
    SleepStage,
    SleepStages,
)
from litestar import Request, get

from . import db

SOURCE = "oura"


def _dt(ms: int) -> datetime:
    """Convert a stored unix-millisecond timestamp to a UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _to_ms(value: str) -> int:
    """Parse an ISO-8601 timestamp (or bare date) into unix milliseconds."""
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# Maps DB metric names to display metadata for the time-series endpoint
METRIC_INFO = {
    "heart_rate": ("Heart Rate", "bpm"),
    "hrv": ("Heart Rate Variability (RMSSD)", "ms"),
    "sleep_stage": ("Sleep Stages", None),
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

DURATION_METRICS = {
    "total_sleep_duration", "deep_sleep_duration", "light_sleep_duration",
    "rem_sleep_duration", "awake_time", "time_in_bed", "latency",
}

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

STAGE_MAP = {1.0: SleepStage.DEEP, 2.0: SleepStage.LIGHT, 3.0: SleepStage.REM, 4.0: SleepStage.AWAKE}


def _serialize(obj):
    """Recursively convert attrs instances to dicts for JSON response."""
    if attrs.has(type(obj)):
        d = {}
        for field in attrs.fields(type(obj)):
            val = getattr(obj, field.name)
            d[field.name] = _serialize(val)
        return d
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return obj


def _parse_ts(s: str) -> datetime:
    if not s:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _make_scalar(m_name: str, m_val: float):
    """Build the appropriate scalar metric type for a sleep session field."""
    if m_name in DURATION_METRICS:
        display = m_name.replace("_", " ").title()
        return Duration(value=m_val / 60.0, source=SOURCE, display_name=display)
    if m_name == "efficiency":
        return Efficiency(value=m_val, source=SOURCE)
    if m_name == "restless_periods":
        return Count(value=int(m_val), source=SOURCE, display_name="Restless Periods")
    if m_name == "average_heart_rate":
        return HeartRateAvg(value=m_val, source=SOURCE)
    if m_name == "lowest_heart_rate":
        return HeartRateMin(value=m_val, source=SOURCE)
    if m_name == "average_hrv":
        return HRVAvg(value=m_val, source=SOURCE)
    if m_name == "average_breath":
        return BreathRateAvg(value=m_val, source=SOURCE)
    return None


# ---------------------------------------------------------------------------
# /v1/metrics
# ---------------------------------------------------------------------------

@get("/api/v1/metrics")
async def service_list_metrics() -> dict:
    metrics: list[MetricType] = []
    async with db.connect() as conn:
        sample_metrics = await (await conn.execute(
            "SELECT DISTINCT metric FROM samples ORDER BY metric"
        )).fetchall()
        for r in sample_metrics:
            name = r[0]
            display, unit = METRIC_INFO.get(name, (name, None))
            metrics.append(MetricType(
                metric_id=name, display_name=display,
                kind=MetricKind.TIME_SERIES, unit=unit,
            ))

        daily = await (await conn.execute(
            "SELECT DISTINCT metric FROM daily_metrics ORDER BY metric"
        )).fetchall()
        for r in daily:
            name = r[0]
            if not any(m.metric_id == name for m in metrics):
                display, unit = METRIC_INFO.get(name, (name, None))
                metrics.append(MetricType(
                    metric_id=name, display_name=display,
                    kind=MetricKind.TIME_SERIES, unit=unit,
                ))

    return {"metrics": [_serialize(m) for m in metrics]}


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

    is_daily = metric.startswith("readiness_") or metric.startswith("sleep_score") or metric.startswith("temperature_")

    if is_daily:
        ts = await _daily_time_series(metric, display, unit, start, end, limit)
    else:
        ts = await _sample_time_series(metric, display, unit, start, end, limit)

    return _serialize(ts)


async def _sample_time_series(metric, display, unit, start, end, limit) -> TimeSeries:
    conditions = ["metric = ?"]
    params: list = [metric]
    if start:
        conditions.append("timestamp_unix >= ?")
        params.append(_to_ms(start))
    if end:
        conditions.append("timestamp_unix <= ?")
        params.append(_to_ms(end))
    if limit:
        params.append(int(limit))

    where = " AND ".join(conditions)
    limit_clause = " LIMIT ?" if limit else ""

    async with db.connect() as conn:
        rows = await (await conn.execute(
            f"SELECT timestamp_unix, end_unix, value FROM samples WHERE {where} ORDER BY timestamp_unix{limit_clause}",
            params,
        )).fetchall()

    samples: list[Sample | IntervalSample] = []
    for r in rows:
        ts = _dt(r[0])
        if r[1] is not None:
            samples.append(IntervalSample(timestamp=ts, value=r[2], end_timestamp=_dt(r[1])))
        else:
            samples.append(Sample(timestamp=ts, value=r[2]))

    return TimeSeries(
        metric_id=metric, display_name=display,
        unit=unit, source=SOURCE, samples=samples,
    )


async def _daily_time_series(metric, display, unit, start, end, limit) -> TimeSeries:
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

    samples = [Sample(timestamp=_parse_ts(f"{r[0]}T00:00:00+00:00"), value=r[1]) for r in rows]
    return TimeSeries(
        metric_id=metric, display_name=display,
        unit=unit, source=SOURCE, samples=samples,
    )


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
            kwargs: dict = {
                "start": _parse_ts(s[3]),
                "end": _parse_ts(s[4]),
                "source": s[1],
                "id": str(session_id),
            }

            metrics = await (await conn.execute(
                "SELECT metric, value FROM sleep_session_metrics WHERE sleep_session_id = ?",
                (session_id,),
            )).fetchall()

            for m_name, m_val in metrics:
                field = SLEEP_SCALAR_FIELDS.get(m_name)
                if not field:
                    continue
                scalar = _make_scalar(m_name, m_val)
                if scalar is not None:
                    kwargs[field] = scalar

            # Sleep stages
            stage_rows = await (await conn.execute(
                """SELECT timestamp_unix, end_unix, value FROM samples
                   WHERE sleep_session_id = ? AND metric = 'sleep_stage'
                   ORDER BY timestamp_unix""",
                (session_id,),
            )).fetchall()

            if stage_rows:
                kwargs["stages"] = SleepStages(
                    source=SOURCE,
                    samples=[
                        IntervalSample(
                            timestamp=_dt(r[0]),
                            value=STAGE_MAP.get(r[2], SleepStage.UNKNOWN),
                            end_timestamp=_dt(r[1]),
                        )
                        for r in stage_rows
                    ],
                )

            # HR time series
            hr_rows = await (await conn.execute(
                """SELECT timestamp_unix, end_unix, value FROM samples
                   WHERE sleep_session_id = ? AND metric = 'heart_rate'
                   ORDER BY timestamp_unix""",
                (session_id,),
            )).fetchall()

            if hr_rows:
                hr_samples: list = []
                for r in hr_rows:
                    if r[1] is not None:
                        hr_samples.append(IntervalSample(timestamp=_dt(r[0]), value=r[2], end_timestamp=_dt(r[1])))
                    else:
                        hr_samples.append(Sample(timestamp=_dt(r[0]), value=r[2]))
                kwargs["heart_rate"] = HeartRate(source=SOURCE, samples=hr_samples)

            # HRV time series
            hrv_rows = await (await conn.execute(
                """SELECT timestamp_unix, end_unix, value FROM samples
                   WHERE sleep_session_id = ? AND metric = 'hrv'
                   ORDER BY timestamp_unix""",
                (session_id,),
            )).fetchall()

            if hrv_rows:
                hrv_samples: list = []
                for r in hrv_rows:
                    if r[1] is not None:
                        hrv_samples.append(IntervalSample(timestamp=_dt(r[0]), value=r[2], end_timestamp=_dt(r[1])))
                    else:
                        hrv_samples.append(Sample(timestamp=_dt(r[0]), value=r[2]))
                kwargs["hrv"] = HRV_RMSSD(source=SOURCE, samples=hrv_samples)

            # Sleep score from daily_metrics
            date_str = s[3][:10]
            score_row = await (await conn.execute(
                "SELECT value FROM daily_metrics WHERE date = ? AND metric = 'sleep_score'",
                (date_str,),
            )).fetchone()
            if score_row:
                kwargs["sleep_score"] = Score(value=score_row[0], source=SOURCE, display_name="Sleep Score")

            result.append(SleepSession(**kwargs))

    return {"data": [_serialize(s) for s in result]}


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
