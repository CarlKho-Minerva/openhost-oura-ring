import asyncio

from openhost_oura import oura, db


class FakeResp:
    def __init__(self, status_code, data=None, next_token=None, headers=None):
        self.status_code = status_code
        self._body = {"data": data or [], "next_token": next_token}
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


class FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def get(self, url, headers=None, params=None):
        self.calls.append(dict(params or {}))
        return self._responses.pop(0)


def test_fetch_paginated_retries_on_429(monkeypatch):
    slept = []

    async def fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(oura.asyncio, "sleep", fake_sleep)
    client = FakeClient([
        FakeResp(429, headers={"Retry-After": "7"}),
        FakeResp(200, data=[{"a": 1}], next_token="t"),
        FakeResp(200, data=[{"a": 2}]),
    ])

    result = asyncio.run(oura._fetch_paginated(client, "url", {}, {}))

    assert result == [{"a": 1}, {"a": 2}]
    assert slept == [7]  # honored Retry-After, no exponential fallback


def test_fetch_paginated_exponential_fallback(monkeypatch):
    slept = []

    async def fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(oura.asyncio, "sleep", fake_sleep)
    client = FakeClient([
        FakeResp(429),  # no Retry-After header
        FakeResp(429),
        FakeResp(200, data=[{"a": 1}]),
    ])

    result = asyncio.run(oura._fetch_paginated(client, "url", {}, {}))

    assert result == [{"a": 1}]
    assert slept == [1, 2]  # 2**0, 2**1


def test_backfill_chunks_and_resumes(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    windows = []

    async def fake_window(client, headers, start, end):
        windows.append((start, end))

    monkeypatch.setattr(oura, "_sync_window", fake_window)

    async def run():
        await db.init_db()
        await db.set_config("oura_access_token", "tok")
        await db.set_config("backfill_cursor", "2025-01-01")
        await oura.backfill()
        return await db.get_config("backfill_state"), await db.get_config("backfill_cursor")

    state, cursor = asyncio.run(run())

    # Resumes from the persisted cursor, not the 2016 default.
    assert windows[0][0] == "2025-01-01"
    # Monthly windows tile contiguously.
    assert windows[0][1] == "2025-02-01"
    assert windows[1] == ("2025-02-01", "2025-03-01")
    assert state == "done"
    # Cursor advanced past today.
    assert cursor > "2026-01-01"


def test_backfill_marks_error(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))

    async def boom(client, headers, start, end):
        raise RuntimeError("nope")

    monkeypatch.setattr(oura, "_sync_window", boom)

    async def run():
        await db.init_db()
        await db.set_config("oura_access_token", "tok")
        try:
            await oura.backfill()
        except RuntimeError:
            pass
        return await db.get_config("backfill_state")

    assert asyncio.run(run()) == "error"
