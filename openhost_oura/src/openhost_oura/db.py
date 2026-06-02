import os
import aiosqlite
from contextlib import asynccontextmanager

DB_PATH = os.environ.get("OPENHOST_SQLITE_HEALTH", "health.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sleep_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'oura',
    source_id TEXT,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    UNIQUE(source, source_id)
);

CREATE TABLE IF NOT EXISTS sleep_session_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sleep_session_id INTEGER NOT NULL REFERENCES sleep_sessions(id) ON DELETE CASCADE,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    UNIQUE(sleep_session_id, metric)
);

CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'oura',
    metric TEXT NOT NULL,
    timestamp_unix INTEGER NOT NULL,  -- unix epoch milliseconds
    end_unix INTEGER,                 -- unix epoch milliseconds; null for instantaneous samples
    value REAL NOT NULL,
    hr_source TEXT,                   -- Oura heart-rate source: awake/rest/sleep/workout/live/session
    sleep_session_id INTEGER REFERENCES sleep_sessions(id) ON DELETE CASCADE,
    UNIQUE(metric, timestamp_unix)
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'oura',
    date TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    UNIQUE(source, date, metric)
);

CREATE INDEX IF NOT EXISTS idx_samples_session ON samples(sleep_session_id);
CREATE INDEX IF NOT EXISTS idx_daily_metric_date ON daily_metrics(metric, date);
CREATE INDEX IF NOT EXISTS idx_sleep_sessions_dates ON sleep_sessions(start_ts, end_ts);
"""


async def init_db():
    async with connect() as db:
        await db.executescript(SCHEMA)
        await db.commit()


@asynccontextmanager
async def connect():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
    finally:
        await db.close()


async def get_config(key: str) -> str | None:
    async with connect() as db:
        cursor = await db.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row[0] if row else None


async def set_config(key: str, value: str):
    async with connect() as db:
        await db.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (key, value),
        )
        await db.commit()


async def delete_config(key: str):
    async with connect() as db:
        await db.execute("DELETE FROM config WHERE key = ?", (key,))
        await db.commit()
