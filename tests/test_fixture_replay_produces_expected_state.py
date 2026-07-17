"""Feature 3 correctness: after a full FIXTURE replay, page_state for known pages
matches values computed independently from the fixture.

Two layers of checking:
  1. Re-aggregate the fixture file here, with logic independent of the Flink job,
     and assert it equals the hand-recorded expected values below (guards against
     the fixture drifting or the test's own logic being wrong).
  2. Assert the live page_state row (produced by the Flink fold) equals those same
     values exactly — the actual event-sourcing correctness proof.

Fold spec being asserted (see stage2_fold.py):
  edit_count     = number of events for the page
  net_byte_delta = sum(byte_delta)         byte_delta = length.new - (length.old or 0)
  unique_editors = distinct editors
  last_editor    = editor of the event with max (occurred_at, revision_id)
  created_at     = min(occurred_at)        last_edited_at = max(occurred_at)
"""
import json
import os
from datetime import datetime, timezone

import pytest

from conftest import pg_connect

FIXTURE = os.path.join(
    os.path.dirname(__file__), "..", "producer", "fixtures", "wiki_events_sample.jsonl"
)

# Hand-recorded expected values, derived directly from the committed fixture.
# (wiki is always enwiki in v1.)
EXPECTED = {
    "Brenda Fricker": dict(edit_count=25, net_byte_delta=823, unique_editors=11, last_editor="ThewikiAl"),
    "Eriamel":        dict(edit_count=13, net_byte_delta=3065, unique_editors=1, last_editor="Qqdarya"),
    "Simon Haloian":  dict(edit_count=11, net_byte_delta=1704, unique_editors=2, last_editor="Iliochori2"),
}


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def _aggregate_fixture():
    """Independently fold the fixture (not importing the Flink job) into per-title aggregates."""
    agg = {}
    with open(FIXTURE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev.get("server_name") != "en.wikipedia.org" or ev.get("bot") is not False:
                continue
            if ev.get("type") not in ("new", "edit"):
                continue
            title = ev["title"]
            length = ev.get("length") or {}
            byte_delta = int(length.get("new", 0)) - int(length.get("old") or 0)
            editor = ev["user"]
            rev = int(ev["revision"]["new"])
            occurred = ev["meta"]["dt"]

            a = agg.setdefault(title, dict(
                edit_count=0, net_byte_delta=0, editors=set(),
                created_at=occurred, last_edited_at=occurred,
                last_editor=editor, last_key=(_parse_ts(occurred), rev),
            ))
            a["edit_count"] += 1
            a["net_byte_delta"] += byte_delta
            a["editors"].add(editor)
            if _parse_ts(occurred) < _parse_ts(a["created_at"]):
                a["created_at"] = occurred
            k = (_parse_ts(occurred), rev)
            if k >= a["last_key"]:
                a["last_key"] = k
                a["last_edited_at"] = occurred
                a["last_editor"] = editor
    return agg


@pytest.mark.usefixtures("stack_up")
@pytest.mark.parametrize("title", list(EXPECTED))
def test_fixture_replay_produces_expected_state(title):
    fixture_agg = _aggregate_fixture()
    assert title in fixture_agg, f"{title} not found in fixture"
    fa = fixture_agg[title]
    exp = EXPECTED[title]

    # Layer 1: the fixture itself still yields the hand-recorded values.
    assert fa["edit_count"] == exp["edit_count"]
    assert fa["net_byte_delta"] == exp["net_byte_delta"]
    assert len(fa["editors"]) == exp["unique_editors"]
    assert fa["last_editor"] == exp["last_editor"]

    # Layer 2: the Flink fold's page_state row matches exactly.
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select edit_count, net_byte_delta, unique_editors, last_editor, "
                "       created_at, last_edited_at "
                "from page_state where wiki = %s and page_title = %s",
                ("enwiki", title),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    assert row is not None, f"no page_state row for {title}"
    edit_count, net_byte_delta, unique_editors, last_editor, created_at, last_edited_at = row
    assert edit_count == exp["edit_count"], f"{title} edit_count"
    assert net_byte_delta == exp["net_byte_delta"], f"{title} net_byte_delta"
    assert unique_editors == exp["unique_editors"], f"{title} unique_editors"
    assert last_editor == exp["last_editor"], f"{title} last_editor"
    # timestamps: page_state min/max occurred_at match the fixture
    assert created_at == _parse_ts(fa["created_at"]), f"{title} created_at"
    assert last_edited_at == _parse_ts(fa["last_edited_at"]), f"{title} last_edited_at"
