"""Shared test configuration and helpers.

These are integration tests: they run on the host against the live
docker-compose stack (Kafka on localhost:29092, Postgres on localhost:5432,
the Flink cluster processing events). If the stack isn't reachable, tests
skip with a clear message rather than failing spuriously.
"""
import json
import os
import time

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP = os.environ.get("TEST_KAFKA_BOOTSTRAP", "localhost:29092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "wiki.events.raw")
VALID_TOPIC = os.environ.get("KAFKA_VALID_TOPIC", "wiki.events.valid")
DEADLETTER_TOPIC = os.environ.get("KAFKA_DEADLETTER_TOPIC", "wiki.events.deadletter")

PG_DSN = dict(
    host=os.environ.get("TEST_PG_HOST", "localhost"),
    port=int(os.environ.get("TEST_PG_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "pageledger"),
    user=os.environ.get("POSTGRES_USER", "pageledger"),
    password=os.environ.get("POSTGRES_PASSWORD", "pageledger"),
)


def pg_connect():
    return psycopg2.connect(**PG_DSN, connect_timeout=5)


@pytest.fixture(scope="session")
def stack_up():
    """Skip the whole suite cleanly if the stack isn't reachable."""
    try:
        conn = pg_connect()
        conn.close()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Postgres not reachable at {PG_DSN['host']}:{PG_DSN['port']} ({e}). "
                    f"Bring up the stack: docker compose up -d")
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 4000})
    if p.list_topics(timeout=5) is None:  # pragma: no cover
        pytest.skip("Kafka not reachable")
    return True


@pytest.fixture()
def kafka_producer(stack_up):
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def make_consumer(group_suffix: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"pltest-{group_suffix}-{int(time.time()*1000)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def drain_topic(topic: str, seconds: float):
    """Collect all messages currently on a topic (from earliest) for `seconds`."""
    c = make_consumer(topic)
    c.subscribe([topic])
    out = []
    deadline = time.time() + seconds
    try:
        while time.time() < deadline:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            out.append(msg.value().decode("utf-8", "replace"))
    finally:
        c.close()
    return out


def wait_for(topic: str, predicate, timeout: float):
    """Poll a topic from earliest until `predicate(value_str)` matches or timeout."""
    c = make_consumer(topic)
    c.subscribe([topic])
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            val = msg.value().decode("utf-8", "replace")
            if predicate(val):
                return val
    finally:
        c.close()
    return None
