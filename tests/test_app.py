import httpx


def test_health(stack):
    r = httpx.get(f"{stack.app_url}/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_index_redirects_to_setup(stack):
    r = httpx.get(f"{stack.url}/", follow_redirects=False)
    assert r.status_code in (301, 302, 307)
    assert "/setup" in r.headers.get("location", "")


def test_setup_page(stack):
    r = httpx.get(f"{stack.url}/setup")
    assert r.status_code == 200
    assert "Oura" in r.text


def test_api_metrics_empty(stack):
    r = httpx.get(f"{stack.url}/api/v1/metrics")
    assert r.status_code == 200
    assert "metrics" in r.json()


def test_api_time_series_requires_metric(stack):
    r = httpx.get(f"{stack.url}/api/v1/time-series")
    assert r.status_code == 200
    body = r.json()
    assert "error" in body


def test_api_sleep_sessions_empty(stack):
    r = httpx.get(f"{stack.url}/api/v1/sleep-sessions")
    assert r.status_code == 200
    assert r.json()["data"] == []


def test_api_workouts_empty(stack):
    r = httpx.get(f"{stack.url}/api/v1/workouts")
    assert r.status_code == 200
    assert r.json()["data"] == []
