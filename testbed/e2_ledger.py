"""E2: message-ID ledger under redelivery and transient errors (hazard H1; defense M3).

M messages, each carrying one unit of work, are published to the Pub/Sub emulator and
consumed by a push-style handler (streaming pull with concurrent callbacks). Each
processed message produces one side effect row.

Faults (each decided deterministically per message and delivery attempt):
  p_lost_ack   the side effect is done but the ack is lost, so the same message
               (same message ID) is delivered again
  q_ledger     a transient error while inserting into the ledger
  r_process    a transient error during processing, after the ledger insert and before
               the side effect commits; the handler returns an error (nack)

Variants:
  no_ledger      process every delivery
  catch_all      ledger insert; ANY exception is treated as "already processed" and the
                 message is acked (the behaviour observed in the studied system)
  dup_key_only   ledger insert; only a unique-key violation means "already processed";
                 other errors nack so the message is redelivered
  transactional  ledger insert and side effect in one transaction; any error rolls both
                 back and nacks (the textbook idempotent receiver)

Usage: python -m testbed.e2_ledger [--seeds 5] [--messages 300]
"""

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler

from testbed.common import InjectedFault, connect, delete_topic, new_topic, rng_for, wait_until, write_rows

SCENARIOS = {
    "redelivery":        {"p_lost_ack": 0.10, "q_ledger": 0.00, "r_process": 0.00},
    "redelivery+ledger_err": {"p_lost_ack": 0.10, "q_ledger": 0.05, "r_process": 0.00},
    "redelivery+process_err": {"p_lost_ack": 0.10, "q_ledger": 0.00, "r_process": 0.05},
    "all_faults":        {"p_lost_ack": 0.10, "q_ledger": 0.05, "r_process": 0.05},
}
VARIANTS = ("no_ledger", "catch_all", "dup_key_only", "transactional")
MAX_ATTEMPTS_BEFORE_STOP = 50  # safety valve; never reached in practice


def setup(conn):
    conn.execute("DROP TABLE IF EXISTS e2_ledger, e2_effects")
    conn.execute("CREATE TABLE e2_ledger (message_id text PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE e2_effects (id bigserial PRIMARY KEY, work_id int, message_id text)"
    )


class Handler:
    def __init__(self, variant, faults, seed):
        self.variant = variant
        self.faults = faults
        self.seed = seed
        self.lock = threading.Lock()
        self.attempts = {}  # message_id -> deliveries seen
        self.acked = set()
        self.deliveries = 0
        self.unexpected_errors = 0
        self.last_activity = time.monotonic()
        self.local = threading.local()

    def conn(self):
        if not hasattr(self.local, "conn"):
            self.local.conn = connect(autocommit=False)
        return self.local.conn

    def __call__(self, message):
        mid = message.message_id
        work_id = int(message.data.decode())
        with self.lock:
            attempt = self.attempts.get(mid, 0) + 1
            self.attempts[mid] = attempt
            self.deliveries += 1
            self.last_activity = time.monotonic()
        rng = rng_for("e2", self.seed, work_id, attempt)
        lost_ack = rng.random() < self.faults["p_lost_ack"]
        ledger_err = rng.random() < self.faults["q_ledger"]
        process_err = rng.random() < self.faults["r_process"]
        if attempt > MAX_ATTEMPTS_BEFORE_STOP:
            ledger_err = process_err = lost_ack = False

        try:
            outcome = self.handle(mid, work_id, ledger_err, process_err)
        except Exception:  # an unexpected error is a harness bug; count it and retry
            with self.lock:
                self.unexpected_errors += 1
            outcome = "nack"
        if outcome == "nack":
            message.nack()
            return
        if outcome == "ack" and lost_ack:
            message.nack()  # the ack never reached the broker: same message comes back
            return
        message.ack()
        with self.lock:
            self.acked.add(mid)

    def handle(self, mid, work_id, ledger_err, process_err):
        conn = self.conn()
        v = self.variant
        try:
            if v == "transactional":
                try:
                    with conn.transaction():
                        if ledger_err:
                            raise InjectedFault("ledger insert")
                        conn.execute("INSERT INTO e2_ledger VALUES (%s)", (mid,))
                        if process_err:
                            raise InjectedFault("processing")
                        conn.execute(
                            "INSERT INTO e2_effects (work_id, message_id) VALUES (%s, %s)", (work_id, mid)
                        )
                except psycopg.errors.UniqueViolation:
                    return "ack-duplicate"
                return "ack"

            if v != "no_ledger":
                try:
                    with conn.transaction():
                        if ledger_err:
                            raise InjectedFault("ledger insert")
                        conn.execute("INSERT INTO e2_ledger VALUES (%s)", (mid,))
                except psycopg.errors.UniqueViolation:
                    return "ack-duplicate"
                except Exception:
                    if v == "catch_all":
                        return "ack-duplicate"  # any error is taken to mean "seen before"
                    return "nack"

            with conn.transaction():
                if process_err:
                    raise InjectedFault("processing")
                conn.execute(
                    "INSERT INTO e2_effects (work_id, message_id) VALUES (%s, %s)", (work_id, mid)
                )
            return "ack"
        except InjectedFault:
            return "nack"


def run_trial(variant, scenario, seed, n_messages):
    with connect() as admin:
        setup(admin)
    publisher, subscriber, topic, sub = new_topic("e2")
    handler = Handler(variant, SCENARIOS[scenario], seed)
    try:
        futures = [publisher.publish(topic, str(i).encode()) for i in range(n_messages)]
        published = {f.result() for f in futures}

        scheduler = ThreadScheduler(ThreadPoolExecutor(max_workers=8))
        flow = pubsub_v1.types.FlowControl(max_messages=32)
        stream = subscriber.subscribe(sub, handler, flow_control=flow, scheduler=scheduler)
        started = time.monotonic()
        done = wait_until(
            lambda: handler.acked >= published and time.monotonic() - handler.last_activity > 3,
            timeout=180,
        )
        stream.cancel()
        try:
            stream.result(timeout=10)
        except Exception:
            pass
        elapsed = time.monotonic() - started
    finally:
        delete_topic(publisher, subscriber, topic, sub)

    with connect() as admin:
        effects, distinct = admin.execute(
            "SELECT count(*), count(DISTINCT work_id) FROM e2_effects"
        ).fetchone()
    return {
        "experiment": "E2",
        "variant": variant,
        "scenario": scenario,
        **SCENARIOS[scenario],
        "seed": seed,
        "messages": n_messages,
        "deliveries": handler.deliveries,
        "side_effects": effects,
        "work_done": distinct,
        "duplicate_effects": effects - distinct,
        "dropped_work": n_messages - distinct,
        "completed": done,
        "unexpected_errors": handler.unexpected_errors,
        "elapsed_s": round(elapsed, 1),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--messages", type=int, default=300)
    parser.add_argument("--variants", default=",".join(VARIANTS))
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    args = parser.parse_args()
    for scenario in args.scenarios.split(","):
        for variant in args.variants.split(","):
            for seed in range(args.seeds):
                row = run_trial(variant, scenario, seed, args.messages)
                print(row, flush=True)
                write_rows("e2_ledger.csv", [row])


if __name__ == "__main__":
    main()
