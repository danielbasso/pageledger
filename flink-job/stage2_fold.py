"""Flink stage 2 — keyed-state fold (silver -> gold). The event-sourcing core.

Consumes wiki.events.valid, keys by (wiki, page_title), and folds each event
into a PageAggregate held in Flink keyed ValueState, upserting the result into
page_state (the gold read model) via the JDBC sink.

The fold is written to be **order-independent**: given the same set of events for
a key, the resulting aggregate is identical regardless of processing order. That
is what makes "Rebuild State" provably deterministic — replaying the whole log
from earliest converges to byte-identical state.

  edit_count      = number of events folded (each is a revision)
  created_at      = min(occurred_at)
  last_edited_at  = max(occurred_at)
  last_editor     = editor of the event with max (occurred_at, revision_id)
                    (revision_id breaks occurred_at ties deterministically)
  net_byte_delta  = sum(byte_delta)
  unique_editors  = |{editor}|
  is_deleted      = False (v1 has no delete/restore events)

Always started from earliest: since the fold is a
pure function of the full history and every write is an idempotent upsert,
"resume from offset" and "replay from earliest" converge to the same state.

Sink table is parameterized (--sink-table / SINK_TABLE, default page_state) so the
rebuild can retarget this same job to page_state_shadow.
"""
import argparse
import json
import os
from datetime import datetime, timezone

from pyflink.common import RestartStrategies, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import KeyedProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.state import ValueStateDescriptor

# --- config -----------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
VALID_TOPIC = os.environ.get("KAFKA_VALID_TOPIC", "wiki.events.valid")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "pageledger")
PG_USER = os.environ.get("POSTGRES_USER", "pageledger")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "pageledger")

KEY_SEP = "\x1f"  # unit separator — safe against titles containing '/'


def _parse_ts(s: str) -> datetime:
    # fixture timestamps are ISO-8601 UTC ("...Z"); normalize for fromisoformat.
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def key_of(valid_json: str) -> str:
    ev = json.loads(valid_json)
    return f'{ev["wiki"]}{KEY_SEP}{ev["page_title"]}'


PAGE_STATE_ROW_TYPE = Types.ROW_NAMED(
    ["wiki", "page_title", "created_at", "last_edited_at", "edit_count",
     "net_byte_delta", "unique_editors", "last_editor", "is_deleted"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(),
     Types.INT(), Types.INT(), Types.STRING(), Types.BOOLEAN()],
)


class FoldPage(KeyedProcessFunction):
    """Keyed fold: PageAggregate in ValueState (serialized JSON), emitting the
    current aggregate as a page_state row on every event."""

    def open(self, runtime_context):
        self.agg = runtime_context.get_state(
            ValueStateDescriptor("page_aggregate", Types.STRING())
        )

    def process_element(self, value, ctx):
        ev = json.loads(value)
        wiki = ev["wiki"]
        title = ev["page_title"]
        editor = ev["editor"]
        byte_delta = int(ev["byte_delta"])
        occurred = ev["occurred_at"]
        rev = int(ev["revision_id"])

        cur = self.agg.value()
        state = json.loads(cur) if cur else None

        if state is None:
            state = {
                "created_at": occurred,
                "last_edited_at": occurred,
                "last_editor": editor,
                "last_rev": rev,
                "edit_count": 0,
                "net_byte_delta": 0,
                "editors": [],
                "seen_revs": [],
            }

        # Idempotent application: apply each revision_id at most once per key. The
        # valid topic can carry the same revision twice (stage 1's at-least-once
        # sink re-emitting on recovery), so this is what keeps the fold correct and
        # the rebuild deterministic — the fold-side mirror of page_events'
        # unique(wiki, revision_id). (State grows with edits-per-page; fine at this
        # scale — a production hot-key stream would bound it with state TTL.)
        if rev in state["seen_revs"]:
            return
        state["seen_revs"].append(rev)

        state["edit_count"] += 1
        state["net_byte_delta"] += byte_delta

        if editor not in state["editors"]:
            state["editors"].append(editor)

        # created_at = min occurred_at
        if _parse_ts(occurred) < _parse_ts(state["created_at"]):
            state["created_at"] = occurred

        # last_edited_at / last_editor = event with max (occurred_at, revision_id)
        cur_key = (_parse_ts(state["last_edited_at"]), state["last_rev"])
        new_key = (_parse_ts(occurred), rev)
        if new_key >= cur_key:
            state["last_edited_at"] = occurred
            state["last_editor"] = editor
            state["last_rev"] = rev

        self.agg.update(json.dumps(state))

        yield Row(
            wiki,
            title,
            state["created_at"],
            state["last_edited_at"],
            int(state["edit_count"]),
            int(state["net_byte_delta"]),
            int(len(state["editors"])),
            state["last_editor"],
            False,
        )


def build_page_state_jdbc_sink(sink_table: str):
    sql = (
        f"INSERT INTO {sink_table} "
        "(wiki, page_title, created_at, last_edited_at, edit_count, net_byte_delta, "
        " unique_editors, last_editor, is_deleted) "
        "VALUES (?, ?, ?::timestamptz, ?::timestamptz, ?, ?, ?, ?, ?) "
        "ON CONFLICT (wiki, page_title) DO UPDATE SET "
        "  created_at = EXCLUDED.created_at, "
        "  last_edited_at = EXCLUDED.last_edited_at, "
        "  edit_count = EXCLUDED.edit_count, "
        "  net_byte_delta = EXCLUDED.net_byte_delta, "
        "  unique_editors = EXCLUDED.unique_editors, "
        "  last_editor = EXCLUDED.last_editor, "
        "  is_deleted = EXCLUDED.is_deleted"
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
    return JdbcSink.sink(sql, PAGE_STATE_ROW_TYPE, conn, exec_opts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sink-table", default=os.environ.get("SINK_TABLE", "page_state"))
    parser.add_argument("--group-id", default=os.environ.get("STAGE2_GROUP_ID", "flink-stage2-fold"))
    args, _ = parser.parse_known_args()
    sink_table = args.sink_table

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)
    # Keep the job alive across transient failures (e.g. a TaskManager restart):
    # retry indefinitely, and don't let a single failed checkpoint kill the job
    # (Flink tolerates zero by default).
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(2147483647, 10_000))
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(100)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(VALID_TOPIC)
        .set_group_id(args.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())  # always earliest
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    valid = env.from_source(source, WatermarkStrategy.no_watermarks(), "valid-source")

    (
        valid.key_by(key_of, key_type=Types.STRING())
        .process(FoldPage(), output_type=PAGE_STATE_ROW_TYPE)
        .add_sink(build_page_state_jdbc_sink(sink_table))
        .name(f"sink-{sink_table}-jdbc")
    )

    env.execute(f"pageledger-stage2-fold[{sink_table}]")


if __name__ == "__main__":
    main()
