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
    assert "Health Data" in r.text


def test_api_metrics_empty(stack):
    r = httpx.get(f"{stack.url}/api/v1/metrics")
    assert r.status_code == 200
    assert "metrics" in r.json()


def test_api_samples_requires_metric(stack):
    r = httpx.get(f"{stack.url}/api/v1/samples")
    assert r.status_code == 200
    body = r.json()
    assert "error" in body


def test_api_sleep_sessions_empty(stack):
    r = httpx.get(f"{stack.url}/api/v1/sleep-sessions")
    assert r.status_code == 200
    assert r.json()["count"] == 0


def test_api_daily_empty(stack):
    r = httpx.get(f"{stack.url}/api/v1/daily")
    assert r.status_code == 200
    assert r.json()["count"] == 0
