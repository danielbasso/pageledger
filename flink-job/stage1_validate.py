"""Flink stage 1 — validation & normalization (bronze -> silver + page_events).

Consumes wiki.events.raw, validates and normalizes each event into the domain
shape, then:
  - sinks valid events to wiki.events.valid (Kafka, the silver topic),
  - sinks valid events to the page_events Postgres table (idempotent JdbcSink),
  - routes malformed events to wiki.events.deadletter with a reason.

Malformed events must never block the pipeline or reach the valid topic /
page_events — that resilience is Feature 2's whole point.

The validate/normalize logic is self-contained in this file (no local imports)
so it ships intact when submitted with `flink run -py`.
"""
import json
import os
from datetime import datetime, timezone

from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

# --- config -----------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "wiki.events.raw")
VALID_TOPIC = os.environ.get("KAFKA_VALID_TOPIC", "wiki.events.valid")
DEADLETTER_TOPIC = os.environ.get("KAFKA_DEADLETTER_TOPIC", "wiki.events.deadletter")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "pageledger")
PG_USER = os.environ.get("POSTGRES_USER", "pageledger")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "pageledger")

# A byte-delta beyond this is not a real Wikipedia page change -> treat as malformed.
MAX_ABS_BYTE_DELTA = 10_000_000


# --- pure validation / normalization ----------------------------------------
def validate_and_normalize(raw: str):
    """Return (normalized_dict, None) on success or (None, reason) on failure."""
    try:
        ev = json.loads(raw)
    except Exception:  # noqa: BLE001
        return None, "invalid_json"
    if not isinstance(ev, dict):
        return None, "not_object"

    ev_type = ev.get("type")
    if ev_type not in ("new", "edit"):
        return None, "bad_type"

    wiki = ev.get("wiki")
    title = ev.get("title")
    user = ev.get("user")
    if not wiki or not title or not user:
        return None, "missing_field"

    rev = (ev.get("revision") or {}).get("new")
    if not isinstance(rev, int):
        return None, "missing_revision"

    length = ev.get("length") or {}
    new_len = length.get("new")
    old_len = length.get("old")
    if not isinstance(new_len, int):
        return None, "bad_length"
    if old_len is None:
        old_len = 0
    if not isinstance(old_len, int):
        return None, "bad_length"
    byte_delta = new_len - old_len
    if abs(byte_delta) > MAX_ABS_BYTE_DELTA:
        return None, "bad_byte_delta"

    occurred = (ev.get("meta") or {}).get("dt")
    if not occurred:
        ts = ev.get("timestamp")
        if isinstance(ts, (int, float)):
            occurred = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        else:
            return None, "missing_timestamp"

    normalized = {
        "wiki": wiki,
        "page_title": title,
        "event_type": "PageCreated" if ev_type == "new" else "PageEdited",
        "revision_id": rev,
        "editor": user,
        "byte_delta": byte_delta,
        "occurred_at": occurred,
    }
    return normalized, None


# --- map helpers (module-level so they pickle cleanly for distribution) ------
def classify(raw: str) -> str:
    """Tag each raw record as ok+normalized or not-ok+reason, as a JSON envelope."""
    norm, reason = validate_and_normalize(raw)
    if norm is None:
        return json.dumps({"ok": False, "reason": reason, "raw": raw})
    return json.dumps({"ok": True, "norm": norm})


def is_ok(envelope: str) -> bool:
    return json.loads(envelope).get("ok") is True


def is_bad(envelope: str) -> bool:
    return json.loads(envelope).get("ok") is not True


def to_valid_json(envelope: str) -> str:
    return json.dumps(json.loads(envelope)["norm"], separators=(",", ":"))


def to_deadletter_json(envelope: str) -> str:
    d = json.loads(envelope)
    return json.dumps({"reason": d.get("reason"), "raw": d.get("raw")}, separators=(",", ":"))


def to_page_events_row(valid_json: str) -> Row:
    n = json.loads(valid_json)
    return Row(
        n["wiki"],
        n["page_title"],
        n["event_type"],
        int(n["revision_id"]),
        n["editor"],
        int(n["byte_delta"]),
        n["occurred_at"],
    )


PAGE_EVENTS_ROW_TYPE = Types.ROW_NAMED(
    ["wiki", "page_title", "event_type", "revision_id", "editor", "byte_delta", "occurred_at"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING(), Types.INT(), Types.STRING()],
)


def build_kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def build_page_events_jdbc_sink():
    # Idempotent by construction: ON CONFLICT (wiki, revision_id) DO NOTHING, so
    # Flink checkpoint redelivery never duplicates a row (Backend Schema design).
    sql = (
        "INSERT INTO page_events "
        "(wiki, page_title, event_type, revision_id, editor, byte_delta, occurred_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?::timestamptz) "
        "ON CONFLICT (wiki, revision_id) DO NOTHING"
    )
    conn = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name(PG_USER)
        .with_password(PG_PASSWORD)
        .build()
    )
    exec_opts = (
        JdbcExecutionOptions.builder()
        .with_batch_size(200)
        .with_batch_interval_ms(1000)
        .with_max_retries(3)
        .build()
    )
    return JdbcSink.sink(sql, PAGE_EVENTS_ROW_TYPE, conn, exec_opts)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)  # 10s; with idempotent sinks this is ample

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(RAW_TOPIC)
        .set_group_id("flink-stage1-validate")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "raw-source")

    tagged = raw.map(classify, output_type=Types.STRING())

    valid_json = (
        tagged.filter(is_ok).map(to_valid_json, output_type=Types.STRING())
    )
    dead_json = (
        tagged.filter(is_bad).map(to_deadletter_json, output_type=Types.STRING())
    )

    # silver topic
    valid_json.sink_to(build_kafka_sink(VALID_TOPIC)).name("sink-valid-kafka")
    # dead-letter topic
    dead_json.sink_to(build_kafka_sink(DEADLETTER_TOPIC)).name("sink-deadletter-kafka")
    # page_events (Postgres), idempotent upsert-by-do-nothing
    valid_json.map(to_page_events_row, output_type=PAGE_EVENTS_ROW_TYPE).add_sink(
        build_page_events_jdbc_sink()
    ).name("sink-page-events-jdbc")

    env.execute("pageledger-stage1-validate")


if __name__ == "__main__":
    main()
