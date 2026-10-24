"""Shared helpers: database connections, Pub/Sub emulator setup and result output."""

import csv
import hashlib
import os
import random
import time
import uuid

import psycopg
from google.cloud import pubsub_v1

DSN = os.environ.get("TB_DSN", "postgresql://tb:tb@localhost:5432/tb")
PROJECT = os.environ.get("PUBSUB_PROJECT_ID", "testbed")
RESULTS_DIR = os.environ.get("TB_RESULTS", "results")


class InjectedFault(Exception):
    """A transient failure injected by the harness (e.g. a lost DB connection)."""


def connect(autocommit=True):
    return psycopg.connect(DSN, autocommit=autocommit)


def rng_for(*key):
    """Deterministic RNG for one decision, independent of thread scheduling and hash seeds."""
    digest = hashlib.sha256(repr(key).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def new_topic(prefix):
    """Create a fresh topic and subscription so runs never share messages."""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"
    topic = publisher.topic_path(PROJECT, name)
    sub = subscriber.subscription_path(PROJECT, name)
    publisher.create_topic(name=topic)
    subscriber.create_subscription(name=sub, topic=topic, ack_deadline_seconds=10)
    return publisher, subscriber, topic, sub


def delete_topic(publisher, subscriber, topic, sub):
    try:
        subscriber.delete_subscription(subscription=sub)
        publisher.delete_topic(topic=topic)
    except Exception:
        pass


def write_rows(filename, rows):
    """Append result rows (list of dicts) to a CSV in the results folder."""
    if not rows:
        return
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, filename)
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def wait_until(predicate, timeout, idle=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(idle)
    return False
