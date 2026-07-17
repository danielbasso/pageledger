"""Feature 6 correctness (the event-sourcing proof): triggering the rebuild
endpoint produces a page_state byte-identical to its state immediately beforehand.

Also checks the single-flight guard (a second trigger while one runs returns 409).

Integration test: drives the real POST /api/rebuild against the live stack, which
cancels and resubmits the actual Flink fold job. Slow (~1 min) by nature.
"""
import json
import time
import urllib.error
import urllib.request

import pytest

from conftest import pg_connect

API = "http://localhost:8000"


def _req(method, path):
    req = urllib.request.Request(f"{API}{path}", method=method, data=b"" if method == "POST" else None)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def _snapshot():
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select * from page_state order by wiki, page_title")
            return cur.fetchall()
    finally:
        conn.close()


@pytest.mark.usefixtures("stack_up")
def test_rebuild_is_deterministic():
    before = _snapshot()
    assert before, "page_state is empty — run the fixture pipeline first"

    # Trigger, retrying past any 30s cooldown from a prior run.
    deadline = time.time() + 60
    while True:
        code, body = _req("POST", "/api/rebuild")
        if code == 202:
            break
        assert code == 409, f"unexpected {code}: {body}"
        if time.time() > deadline:
            pytest.fail(f"could not start rebuild (stuck at 409): {body}")
        time.sleep(3)

    # Single-flight: a second trigger while running must be rejected.
    code2, body2 = _req("POST", "/api/rebuild")
    assert code2 == 409, f"expected 409 for concurrent rebuild, got {code2}: {body2}"

    # Wait for completion.
    deadline = time.time() + 180
    status = None
    while time.time() < deadline:
        _, s = _req("GET", "/api/rebuild/status")
        status = s.get("status")
        if status in ("succeeded", "failed"):
            break
        time.sleep(2)

    assert status == "succeeded", f"rebuild did not succeed: {status}"
    assert s.get("discrepancy_count") == 0, f"discrepancies: {s.get('discrepancy_count')}"
    assert (s.get("events_replayed") or 0) >= 1

    # The actual proof: state is byte-identical to before the rebuild.
    after = _snapshot()
    assert after == before, "page_state changed across a rebuild — not deterministic"
