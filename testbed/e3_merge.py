"""E3: merging per-release files into one output per key (hazards H1, H2; defense M5).

Each merge key (e.g. a warehouse and delivery date) has 2-4 releases. Releases start
at random offsets, run for a random time, and on completion write their file, mark
their status row COMPLETE and publish a merge message. Downstream wants exactly one
output per key containing every release once.

Variants:
  push_naive    each message handler checks "all releases COMPLETE?" and, if so, merges
                every file of the key (the original design)
  push_lock     as push_naive, under a per-key advisory lock, merging only releases not
                yet merged
  time_window   a scheduled job merges unmerged completed releases of a key once its
                first completion is older than a fixed window; messages are ignored
  barrier_s0    scheduled pull; merge a key when all its releases are COMPLETE, mark
                them with a conditional update (merged IS NULL) and ack only the
                messages of merged keys; others are released for the next run
  barrier       as barrier_s0, plus a settle window: the newest release of the key
                must be older than SETTLE_S
  barrier_orders     scheduled pull; merge a key when none of its orders is still
                     ELIGIBLE (not released yet) and all its releases are COMPLETE. The
                     order table holds every order of the key before its release starts,
                     so a release that has not started yet still holds the merge (the
                     M5 check for normal releases)

Rush variants (time-based rules; release status is not verified, so no order holds
another). A scheduled job runs every RUSH_INTERVAL_S:
  rush_no_merge  sends one output per completed release
  rush_w0        merges every completed release not merged yet into one output per key
  rush_w5        merges, into one output per key, only the completed releases created at
  rush_w10       least 5 s / 10 s ago; newer releases wait for a later run

Faults:
  start_spread  releases of a key start up to this many seconds apart (late starters)
  p_lost_ack    an ack is lost after processing, so the message is redelivered
  p_crash       the consumer fails after writing the output but before acking

Scenarios: baseline, late_start, redelivery and crash release 2-4 orders per key, whose
orders all exist before the releases start. In rush, orders keep arriving one at a time
(8-12 per key, about 1 s apart on average) and each is released on its own as it arrives.
The rush timings are scaled so that 1 s stands for 1 minute: a 5-minute merge schedule
and waits of 5 and 10 minutes.

Usage: python -m testbed.e3_merge [--seeds 3]
"""

import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler

from testbed.common import connect, delete_topic, new_topic, rng_for, write_rows

N_KEYS = 30
INTERVAL_S = 1.0  # schedule of the pull-based variants
WINDOW_S = 3.0  # time_window variant
SETTLE_S = 3.0  # barrier variant
RUSH_ORDERS = (8, 12)  # orders per key in the rush scenario
RUSH_GAP_S = 1.0  # mean time between rush orders of a key
RUSH_INTERVAL_S = 5.0  # schedule of the rush merge job
RUSH_WAITS = {"rush_no_merge": 0.0, "rush_w0": 0.0, "rush_w5": 5.0, "rush_w10": 10.0}
DRAIN_S = 12.0
RUSH_DRAIN_S = 20.0

SCENARIOS = {
    "baseline":   {"start_spread": 1.0, "p_lost_ack": 0.0, "p_crash": 0.0, "rush": 0},
    "late_start": {"start_spread": 6.0, "p_lost_ack": 0.0, "p_crash": 0.0, "rush": 0},
    "redelivery": {"start_spread": 1.0, "p_lost_ack": 0.1, "p_crash": 0.0, "rush": 0},
    "crash":      {"start_spread": 1.0, "p_lost_ack": 0.0, "p_crash": 0.05, "rush": 0},
    "rush":       {"start_spread": 0.0, "p_lost_ack": 0.0, "p_crash": 0.0, "rush": 1},
}
VARIANTS = ("push_naive", "push_lock", "time_window", "barrier_s0", "barrier", "barrier_orders")
RUSH_VARIANTS = ("rush_no_merge", "rush_w0", "rush_w5", "rush_w10")


def setup(conn):
    conn.execute("DROP TABLE IF EXISTS e3_orders, e3_releases, e3_outputs, e3_output_lines")
    conn.execute(
        "CREATE TABLE e3_orders (key int, rid int, status text, created_at double precision,"
        " PRIMARY KEY (key, rid))"
    )
    conn.execute(
        "CREATE TABLE e3_releases (key int, rid int, status text, created_at double precision,"
        " completed_at double precision, merged_output int, PRIMARY KEY (key, rid))"
    )
    conn.execute(
        "CREATE TABLE e3_outputs (output_id serial PRIMARY KEY, key int, created_at double precision)"
    )
    conn.execute("CREATE TABLE e3_output_lines (output_id int, key int, rid int)")


def plan_releases(seed, scenario):
    """(order arrival, release start, duration) offsets for every release of every key.

    Each release carries one order. Outside the rush scenario all orders exist at the
    start; in rush each order arrives shortly before its own release starts.
    """
    s = SCENARIOS[scenario]
    plan = []
    for key in range(N_KEYS):
        rng = rng_for("e3-plan", seed, key)
        if s["rush"]:
            at = rng.uniform(0, RUSH_GAP_S)
            for rid in range(rng.randint(*RUSH_ORDERS)):
                plan.append((key, rid, at, at + rng.uniform(0.1, 0.5), rng.uniform(0.3, 1.0)))
                at += rng.expovariate(1 / RUSH_GAP_S)
        else:
            for rid in range(rng.randint(2, 4)):
                plan.append((key, rid, 0.0, rng.uniform(0, s["start_spread"]),
                             rng.uniform(0.5, 2.0)))
    return plan


def write_output(conn, key, rids):
    """Write one merged output (stands in for the merged partner file)."""
    out = conn.execute(
        "INSERT INTO e3_outputs (key, created_at) VALUES (%s, %s) RETURNING output_id",
        (key, time.time()),
    ).fetchone()[0]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO e3_output_lines VALUES (%s, %s, %s)", [(out, key, r) for r in rids]
        )
    return out


class Simulator(threading.Thread):
    """Starts and completes releases on schedule and publishes merge messages."""

    def __init__(self, plan, publisher, topic):
        super().__init__(daemon=True)
        self.plan, self.publisher, self.topic = plan, publisher, topic
        self.done = threading.Event()

    def run(self):
        events = []
        for key, rid, order_at, start, dur in self.plan:
            events.append((order_at, 0, "order", key, rid))
            events.append((start, 1, "start", key, rid))
            events.append((start + dur, 2, "complete", key, rid))
        events.sort()
        t0 = time.monotonic()
        with connect() as conn:
            for at, _, kind, key, rid in events:
                delay = at - (time.monotonic() - t0)
                if delay > 0:
                    time.sleep(delay)
                if kind == "order":
                    conn.execute(
                        "INSERT INTO e3_orders VALUES (%s, %s, 'ELIGIBLE', %s)", (key, rid, time.time())
                    )
                elif kind == "start":
                    with conn.transaction():
                        conn.execute(
                            "INSERT INTO e3_releases VALUES (%s, %s, 'RUNNING', %s, NULL, NULL)",
                            (key, rid, time.time()),
                        )
                        conn.execute(
                            "UPDATE e3_orders SET status = 'RELEASED' WHERE key = %s AND rid = %s",
                            (key, rid),
                        )
                else:
                    conn.execute(
                        "UPDATE e3_releases SET status = 'COMPLETE', completed_at = %s"
                        " WHERE key = %s AND rid = %s",
                        (time.time(), key, rid),
                    )
                    self.publisher.publish(self.topic, json.dumps({"key": key, "rid": rid}).encode())
        self.done.set()


class Faults:
    def __init__(self, scenario, seed):
        self.s, self.seed = SCENARIOS[scenario], seed
        self.counter = 0
        self.lock = threading.Lock()

    def roll(self, name):
        with self.lock:
            self.counter += 1
            n = self.counter
        return rng_for("e3-fault", self.seed, name, n).random() < self.s[name]


def all_complete(conn, key):
    pending = conn.execute(
        "SELECT count(*) FILTER (WHERE status <> 'COMPLETE') FROM e3_releases WHERE key = %s", (key,)
    ).fetchone()[0]
    return pending == 0


def no_unreleased_orders(conn, key):
    pending = conn.execute(
        "SELECT count(*) FROM e3_orders WHERE key = %s AND status = 'ELIGIBLE'", (key,)
    ).fetchone()[0]
    return pending == 0


# ---- push variants -------------------------------------------------------------

def make_push_handler(variant, faults):
    local = threading.local()

    def handler(message):
        if not hasattr(local, "conn"):
            local.conn = connect(autocommit=False)
        conn = local.conn
        key = json.loads(message.data)["key"]
        try:
            with conn.transaction():
                if variant == "push_lock":
                    conn.execute("SELECT pg_advisory_xact_lock(%s)", (key,))
                if not all_complete(conn, key):
                    rids = []
                elif variant == "push_naive":
                    rids = [r[0] for r in conn.execute(
                        "SELECT rid FROM e3_releases WHERE key = %s", (key,)).fetchall()]
                else:
                    rids = [r[0] for r in conn.execute(
                        "SELECT rid FROM e3_releases WHERE key = %s AND merged_output IS NULL",
                        (key,)).fetchall()]
                if rids:
                    out = write_output(conn, key, rids)
                    conn.execute(
                        "UPDATE e3_releases SET merged_output = %s WHERE key = %s AND rid = ANY(%s)",
                        (out, key, rids),
                    )
            if rids and faults.roll("p_crash"):
                message.nack()
                return
        except Exception:
            message.nack()
            return
        if faults.roll("p_lost_ack"):
            message.nack()
        else:
            message.ack()

    return handler


# ---- scheduled variants --------------------------------------------------------

def time_window_job(stop, faults):
    with connect(autocommit=False) as conn:
        while not stop.is_set():
            now = time.time()
            with conn.transaction():
                keys = [r[0] for r in conn.execute(
                    "SELECT key FROM e3_releases WHERE status = 'COMPLETE' AND merged_output IS NULL"
                    " GROUP BY key HAVING min(completed_at) < %s", (now - WINDOW_S,)).fetchall()]
                for key in keys:
                    rids = [r[0] for r in conn.execute(
                        "SELECT rid FROM e3_releases WHERE key = %s AND status = 'COMPLETE'"
                        " AND merged_output IS NULL", (key,)).fetchall()]
                    out = write_output(conn, key, rids)
                    conn.execute(
                        "UPDATE e3_releases SET merged_output = %s WHERE key = %s AND rid = ANY(%s)",
                        (out, key, rids),
                    )
            stop.wait(INTERVAL_S)


def barrier_job(stop, faults, subscriber, sub, settle_s, check_orders=False):
    with connect(autocommit=False) as conn:
        while not stop.is_set():
            received = []
            while True:  # one scheduled run drains what is waiting, then sleeps
                try:
                    resp = subscriber.pull(
                        request={"subscription": sub, "max_messages": 1000}, timeout=0.5
                    )
                except Exception:
                    break
                if not resp.received_messages:
                    break
                received.extend(resp.received_messages)
            by_key = {}
            for m in received:
                by_key.setdefault(json.loads(m.message.data)["key"], []).append(m.ack_id)
            ack, release = [], []
            now = time.time()
            for key, ack_ids in by_key.items():
                with conn.transaction():
                    newest = conn.execute(
                        "SELECT max(created_at) FROM e3_releases WHERE key = %s", (key,)
                    ).fetchone()[0]
                    ready = all_complete(conn, key) and newest is not None and now - newest >= settle_s
                    if ready and check_orders:
                        ready = no_unreleased_orders(conn, key)
                    merged = False
                    if ready:
                        out = conn.execute(
                            "INSERT INTO e3_outputs (key, created_at) VALUES (%s, %s) RETURNING output_id",
                            (key, time.time()),
                        ).fetchone()[0]
                        rids = [r[0] for r in conn.execute(
                            "UPDATE e3_releases SET merged_output = %s"
                            " WHERE key = %s AND merged_output IS NULL RETURNING rid",
                            (out, key)).fetchall()]
                        if rids:
                            with conn.cursor() as cur:
                                cur.executemany(
                                    "INSERT INTO e3_output_lines VALUES (%s, %s, %s)",
                                    [(out, key, r) for r in rids],
                                )
                            merged = True
                        else:
                            conn.execute("DELETE FROM e3_outputs WHERE output_id = %s", (out,))
                        ready = True  # everything for this key is merged now
                if ready and not (merged and faults.roll("p_crash")):
                    for a in ack_ids:
                        (release if faults.roll("p_lost_ack") else ack).append(a)
                else:
                    release.extend(ack_ids)
            if ack:
                subscriber.acknowledge(request={"subscription": sub, "ack_ids": ack})
            if release:
                subscriber.modify_ack_deadline(
                    request={"subscription": sub, "ack_ids": release, "ack_deadline_seconds": 0}
                )
            stop.wait(INTERVAL_S)


def rush_job(stop, faults, subscriber, sub, wait_s, merge):
    """Time-based merge without verification: take what is old enough, send it together."""
    with connect(autocommit=False) as conn:
        while not stop.is_set():
            received = []
            while True:
                try:
                    resp = subscriber.pull(
                        request={"subscription": sub, "max_messages": 1000}, timeout=0.5
                    )
                except Exception:
                    break
                if not resp.received_messages:
                    break
                received.extend(resp.received_messages)
            by_key = {}
            for m in received:
                d = json.loads(m.message.data)
                by_key.setdefault(d["key"], []).append((d["rid"], m.ack_id))
            ack, release = [], []
            cutoff = time.time() - wait_s
            for key, items in by_key.items():
                with conn.transaction():
                    due = [r[0] for r in conn.execute(
                        "SELECT rid FROM e3_releases WHERE key = %s AND status = 'COMPLETE'"
                        " AND merged_output IS NULL AND created_at <= %s AND rid = ANY(%s)",
                        (key, cutoff, [rid for rid, _ in items])).fetchall()]
                    groups = [due] if merge else [[r] for r in due]
                    for rids in groups:
                        if rids:
                            out = write_output(conn, key, rids)
                            conn.execute(
                                "UPDATE e3_releases SET merged_output = %s"
                                " WHERE key = %s AND rid = ANY(%s) AND merged_output IS NULL",
                                (out, key, rids),
                            )
                    merged = {r[0] for r in conn.execute(
                        "SELECT rid FROM e3_releases WHERE key = %s AND merged_output IS NOT NULL",
                        (key,)).fetchall()}
                for rid, ack_id in items:
                    (ack if rid in merged else release).append(ack_id)
            if ack:
                subscriber.acknowledge(request={"subscription": sub, "ack_ids": ack})
            if release:
                subscriber.modify_ack_deadline(
                    request={"subscription": sub, "ack_ids": release, "ack_deadline_seconds": 0}
                )
            stop.wait(RUSH_INTERVAL_S)


# ---- trial ---------------------------------------------------------------------

def measure(conn):
    lines = conn.execute(
        "SELECT key, rid, count(*) FROM e3_output_lines GROUP BY key, rid"
    ).fetchall()
    releases = conn.execute("SELECT key, rid, completed_at FROM e3_releases").fetchall()
    ordered = dict(((k, r), t) for k, r, t in conn.execute(
        "SELECT key, rid, created_at FROM e3_orders").fetchall())
    outputs = conn.execute("SELECT key, count(*) FROM e3_outputs GROUP BY key").fetchall()
    first_out = dict(
        ((k, r), t) for k, r, t in conn.execute(
            "SELECT l.key, l.rid, min(o.created_at) FROM e3_output_lines l"
            " JOIN e3_outputs o USING (output_id) GROUP BY l.key, l.rid").fetchall()
    )
    seen = {(k, r): n for k, r, n in lines}
    duplicate_lines = sum(n - 1 for n in seen.values() if n > 1)
    missing = sum(1 for k, r, _ in releases if (k, r) not in seen)
    split_keys = sum(1 for _, n in outputs if n > 1)
    latencies = []
    last_by_key = {}
    for k, r, done in releases:
        if done is not None and (k not in last_by_key or done > last_by_key[k][1]):
            last_by_key[k] = (r, done)
    for k, (r, done) in last_by_key.items():
        if (k, r) in first_out:
            latencies.append(first_out[(k, r)] - done)
    latencies.sort()
    p95 = latencies[int(0.95 * (len(latencies) - 1))] if latencies else None
    waits = sorted(first_out[kr] - ordered[kr] for kr in first_out if kr in ordered)
    wait_p95 = waits[int(0.95 * (len(waits) - 1))] if waits else None
    keys = len({k for k, _, _ in releases})
    return {
        "releases": len(releases),
        "outputs": sum(n for _, n in outputs),
        "duplicate_lines": duplicate_lines,
        "split_keys": split_keys,
        "missing_releases": missing,
        "latency_median_s": round(statistics.median(latencies), 2) if latencies else None,
        "latency_p95_s": round(p95, 2) if p95 is not None else None,
        "outputs_per_key": round(sum(n for _, n in outputs) / keys, 2) if keys else None,
        "order_wait_median_s": round(statistics.median(waits), 2) if waits else None,
        "order_wait_p95_s": round(wait_p95, 2) if wait_p95 is not None else None,
    }


def run_trial(variant, scenario, seed):
    with connect() as admin:
        setup(admin)
    publisher, subscriber, topic, sub = new_topic("e3")
    faults = Faults(scenario, seed)
    stop = threading.Event()
    stream = job = None
    try:
        if variant.startswith("push"):
            scheduler = ThreadScheduler(ThreadPoolExecutor(max_workers=8))
            stream = subscriber.subscribe(
                sub, make_push_handler(variant, faults),
                flow_control=pubsub_v1.types.FlowControl(max_messages=32), scheduler=scheduler,
            )
        elif variant == "time_window":
            job = threading.Thread(target=time_window_job, args=(stop, faults), daemon=True)
        elif variant in RUSH_WAITS:
            job = threading.Thread(
                target=rush_job, daemon=True,
                args=(stop, faults, subscriber, sub, RUSH_WAITS[variant], variant != "rush_no_merge"),
            )
        elif variant == "barrier_orders":
            job = threading.Thread(
                target=barrier_job, args=(stop, faults, subscriber, sub, 0.0, True), daemon=True
            )
        else:
            settle = 0.0 if variant == "barrier_s0" else SETTLE_S
            job = threading.Thread(
                target=barrier_job, args=(stop, faults, subscriber, sub, settle), daemon=True
            )
        if job:
            job.start()
        sim = Simulator(plan_releases(seed, scenario), publisher, topic)
        sim.start()
        sim.done.wait()
        time.sleep(RUSH_DRAIN_S if SCENARIOS[scenario]["rush"] else DRAIN_S)
        stop.set()
        if stream:
            stream.cancel()
            try:
                stream.result(timeout=10)
            except Exception:
                pass
        if job:
            job.join(timeout=10)
    finally:
        delete_topic(publisher, subscriber, topic, sub)
    with connect() as admin:
        result = measure(admin)
    return {"experiment": "E3", "variant": variant, "scenario": scenario,
            **SCENARIOS[scenario], "seed": seed, **result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--variants", default="", help="default: VARIANTS, or RUSH_VARIANTS for rush")
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    args = parser.parse_args()
    for scenario in args.scenarios.split(","):
        default = RUSH_VARIANTS if SCENARIOS[scenario]["rush"] else VARIANTS
        variants = args.variants.split(",") if args.variants else default
        for variant in variants:
            for seed in range(args.seeds):
                row = run_trial(variant, scenario, seed)
                print(row, flush=True)
                write_rows("e3_merge.csv", [row])


if __name__ == "__main__":
    main()
