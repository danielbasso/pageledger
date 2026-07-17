"""PageLedger producer — the bronze-layer ingress.

Publishes filtered Wikipedia recentchange events onto the raw Kafka topic
(wiki.events.raw), the immutable event log the whole pipeline folds over.

Two modes, identical filtering in both:
  MODE=LIVE     — reads Wikipedia's public SSE recentchange stream in real time.
  MODE=FIXTURE  — replays a recorded sample file at PLAYBACK_SPEED, then stops
                  and holds (does not loop) — so page_state stays stable, which
                  the rebuild-determinism test relies on.

Only *filtering* happens here. Normalization into the domain event shape
(PageCreated/PageEdited, byte_delta, revision_id, ...) is Flink stage 1's job —
bronze stays the raw event, verbatim.
"""
import json
import os
import signal
import sys
import time
import urllib.request

from confluent_kafka import Producer

# --- config -----------------------------------------------------------------
MODE = os.environ.get("MODE", "FIXTURE").upper()
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "wiki.events.raw")
WIKI_STREAM_URL = os.environ.get(
    "WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)
FIXTURE_FILE = os.environ.get("FIXTURE_FILE", "./fixtures/wiki_events_sample.jsonl")
PLAYBACK_SPEED = float(os.environ.get("PLAYBACK_SPEED", "1.0"))
UA = "PageLedger/1.0 (bassosrd@tcd.ie) event-pipeline"

# Cap on how long FIXTURE replay will honour a single inter-event gap, so a rare
# large jump in the recorded timeline can't stall the whole replay.
MAX_GAP_SECONDS = 10.0

_running = True


def _stop(signum, frame):  # noqa: ARG001
    global _running
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


# --- the one filter, shared by both modes -----------------------------------
def accept(ev: dict) -> bool:
    """v1 scope: English Wikipedia, human editors, page creations/edits only."""
    return (
        ev.get("server_name") == "en.wikipedia.org"
        and ev.get("bot") is False
        and ev.get("type") in ("new", "edit")
    )


def event_key(ev: dict) -> bytes:
    """Partition key = the page's aggregate identity, so a page's events keep
    their relative order on the raw topic."""
    return f'{ev.get("wiki", "")}/{ev.get("title", "")}'.encode("utf-8")


def event_epoch(ev: dict):
    """Wikipedia's own event time (unix seconds), used only to pace FIXTURE replay."""
    ts = ev.get("timestamp")
    return float(ts) if isinstance(ts, (int, float)) else None


# --- Kafka ------------------------------------------------------------------
def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "pageledger-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
        }
    )


_delivered = 0
_failed = 0


def _on_delivery(err, msg):  # noqa: ARG001
    global _delivered, _failed
    if err is not None:
        _failed += 1
        print(f"[delivery-error] {err}", flush=True)
    else:
        _delivered += 1


def publish(producer: Producer, ev: dict) -> None:
    producer.produce(
        RAW_TOPIC,
        key=event_key(ev),
        value=json.dumps(ev, separators=(",", ":")).encode("utf-8"),
        on_delivery=_on_delivery,
    )
    producer.poll(0)  # serve delivery callbacks without blocking


# --- LIVE mode: SSE, stdlib only, reconnecting ------------------------------
def run_live(producer: Producer) -> None:
    print(f"[producer] MODE=LIVE  stream={WIKI_STREAM_URL}  topic={RAW_TOPIC}", flush=True)
    last_id = None
    sent = seen = 0
    while _running:
        headers = {"User-Agent": UA, "Accept": "text/event-stream"}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            resp = urllib.request.urlopen(
                urllib.request.Request(WIKI_STREAM_URL, headers=headers), timeout=30
            )
            data_buf = []
            for raw in resp:
                if not _running:
                    break
                line = raw.decode("utf-8", "replace").rstrip("\n")
                if line == "":
                    if data_buf:
                        payload = "\n".join(data_buf)
                        data_buf = []
                        try:
                            ev = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                        seen += 1
                        if accept(ev):
                            publish(producer, ev)
                            sent += 1
                            if sent % 100 == 0:
                                print(f"[producer] live seen={seen} sent={sent}", flush=True)
                    continue
                if line.startswith("id:"):
                    last_id = line[3:].strip()
                elif line.startswith("data:"):
                    data_buf.append(line[5:].lstrip())
        except Exception as e:  # noqa: BLE001 — reconnect on any stream error
            print(f"[producer] live reconnect: {type(e).__name__}: {e}", flush=True)
            time.sleep(2)


# --- FIXTURE mode: replay recorded sample, then stop and hold ----------------
def run_fixture(producer: Producer) -> None:
    print(
        f"[producer] MODE=FIXTURE  file={FIXTURE_FILE}  speed={PLAYBACK_SPEED}  topic={RAW_TOPIC}",
        flush=True,
    )
    if not os.path.exists(FIXTURE_FILE):
        print(f"[producer] FATAL: fixture file not found: {FIXTURE_FILE}", flush=True)
        sys.exit(1)

    sent = seen = 0
    prev_ts = None
    with open(FIXTURE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not _running:
                break
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            seen += 1
            if not accept(ev):
                continue
            ts = event_epoch(ev)
            if prev_ts is not None and ts is not None and PLAYBACK_SPEED > 0:
                gap = (ts - prev_ts) / PLAYBACK_SPEED
                if gap > 0:
                    time.sleep(min(gap, MAX_GAP_SECONDS))
            prev_ts = ts if ts is not None else prev_ts
            publish(producer, ev)
            sent += 1
            if sent % 100 == 0:
                print(f"[producer] fixture seen={seen} sent={sent}", flush=True)

    producer.flush(30)
    print(
        f"[producer] fixture replay complete: {sent} events published "
        f"(delivered={_delivered} failed={_failed}). Stopping and holding "
        f"(no loop) — page_state now stable.",
        flush=True,
    )
    # Stop and hold: stay alive but idle so the container/service stays up and
    # the derived state stays put, per the chosen FIXTURE end-of-replay behaviour.
    while _running:
        time.sleep(1)


def main() -> int:
    producer = make_producer()
    try:
        if MODE == "LIVE":
            run_live(producer)
        else:
            run_fixture(producer)
    finally:
        producer.flush(30)
        print(
            f"[producer] shutdown. delivered={_delivered} failed={_failed}", flush=True
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
