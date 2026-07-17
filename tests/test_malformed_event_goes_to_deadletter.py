"""Feature 2 correctness: a malformed event must land on the dead-letter topic,
never on the valid topic or in page_events, and never crash the pipeline.

Injects one deliberately malformed event directly onto wiki.events.raw (a unique
title marker, but missing the `revision` field so stage-1 validation rejects it),
then asserts:
  1. it appears on wiki.events.deadletter with a reason,
  2. it never appears on wiki.events.valid,
  3. no page_events row exists for that marker title.
"""
import json
import time
import uuid

import pytest

from conftest import (
    DEADLETTER_TOPIC,
    RAW_TOPIC,
    VALID_TOPIC,
    drain_topic,
    pg_connect,
    wait_for,
)


@pytest.mark.usefixtures("stack_up")
def test_malformed_event_goes_to_deadletter(kafka_producer):
    marker = f"__PLTEST_MALFORMED_{uuid.uuid4().hex}__"
    # Well-formed JSON, passes the producer-style shape, but has NO `revision` field
    # -> stage-1 validation rejects it with reason "missing_revision".
    malformed = {
        "type": "edit",
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "bot": False,
        "title": marker,
        "user": "pltest",
        "length": {"old": 10, "new": 20},
        "meta": {"dt": "2026-01-01T00:00:00Z"},
        # intentionally missing "revision"
    }
    kafka_producer.produce(RAW_TOPIC, value=json.dumps(malformed).encode("utf-8"))
    kafka_producer.flush(10)

    # 1. It must reach the dead-letter topic, carrying the marker + a reason.
    #    Generous timeout: the KafkaSink flushes on the stage-1 checkpoint (~10s).
    dl = wait_for(
        DEADLETTER_TOPIC,
        lambda v: marker in v,
        timeout=90,
    )
    assert dl is not None, "malformed event never reached the dead-letter topic"
    dl_obj = json.loads(dl)
    assert dl_obj.get("reason") == "missing_revision", f"unexpected reason: {dl_obj.get('reason')}"

    # 2. It must NOT appear on the valid topic (drain a window and assert absence).
    valid_msgs = drain_topic(VALID_TOPIC, seconds=8)
    assert not any(marker in m for m in valid_msgs), "malformed event leaked onto the valid topic"

    # 3. It must NOT appear in page_events.
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select count(*) from page_events where page_title = %s", (marker,))
            (count,) = cur.fetchone()
    finally:
        conn.close()
    assert count == 0, f"malformed event was written to page_events ({count} rows)"
