"""E1: row-level release claim under concurrent triggers (hazards H2, H3; defense M1).

K triggers arrive at random times within a window of `jitter_ms` and each asks to
release a random half of the eligible orders, so their order sets overlap. Each release
spends `gap_ms` on validation work between selecting orders and marking them. Each released order produces one side
effect row (standing in for a line in a warehouse file).

Variants:
  read_then_update   SELECT eligible orders, do some work, then UPDATE them and emit.
  conditional_claim  UPDATE ... WHERE status = 'ELIGIBLE' RETURNING order_id; emit only
                     for the returned ids (the M1 pattern).

Usage: python -m testbed.e1_claim [--seeds 10]
"""

import argparse
import threading
import time

from testbed.common import connect, rng_for, write_rows

N_ORDERS = 200
SUBSET_FRACTION = 0.5
GAP_MS = 50  # work between the read and the write


def setup(conn):
    conn.execute("DROP TABLE IF EXISTS e1_orders, e1_effects")
    conn.execute("CREATE TABLE e1_orders (order_id int PRIMARY KEY, status text NOT NULL)")
    conn.execute("CREATE TABLE e1_effects (id bigserial PRIMARY KEY, order_id int, worker int)")
    conn.execute(
        "INSERT INTO e1_orders SELECT g, 'ELIGIBLE' FROM generate_series(1, %s) g", (N_ORDERS,)
    )


def read_then_update(conn, worker, order_ids, gap_s):
    rows = conn.execute(
        "SELECT order_id FROM e1_orders WHERE order_id = ANY(%s) AND status = 'ELIGIBLE'",
        (order_ids,),
    ).fetchall()
    selected = [r[0] for r in rows]
    time.sleep(gap_s)  # validation and allocation work between the read and the write
    conn.execute(
        "UPDATE e1_orders SET status = 'INITIATED' WHERE order_id = ANY(%s)", (selected,)
    )
    return selected


def conditional_claim(conn, worker, order_ids, gap_s):
    rows = conn.execute(
        "UPDATE e1_orders SET status = 'INITIATED' "
        "WHERE order_id = ANY(%s) AND status = 'ELIGIBLE' RETURNING order_id",
        (order_ids,),
    ).fetchall()
    time.sleep(gap_s)  # the same work, now after the claim
    return [r[0] for r in rows]


VARIANTS = {"read_then_update": read_then_update, "conditional_claim": conditional_claim}


def run_trial(variant, workers, gap_ms, jitter_ms, seed):
    with connect() as admin:
        setup(admin)
    requests = []
    for w in range(workers):
        rng = rng_for("e1", seed, w)
        requests.append(sorted(rng.sample(range(1, N_ORDERS + 1), int(N_ORDERS * SUBSET_FRACTION))))

    conns = [connect() for _ in range(workers)]
    barrier = threading.Barrier(workers)
    errors = []
    delays = [rng_for("e1-start", seed, w).uniform(0, jitter_ms / 1000) for w in range(workers)]

    def worker_main(w):
        try:
            barrier.wait()
            time.sleep(delays[w])
            released = VARIANTS[variant](conns[w], w, requests[w], gap_ms / 1000)
            with conns[w].cursor() as cur:
                cur.executemany(
                    "INSERT INTO e1_effects (order_id, worker) VALUES (%s, %s)",
                    [(o, w) for o in released],
                )
        except Exception as exc:  # report, don't hide
            errors.append(type(exc).__name__)

    threads = [threading.Thread(target=worker_main, args=(w,)) for w in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for c in conns:
        c.close()

    with connect() as admin:
        effects, distinct = admin.execute(
            "SELECT count(*), count(DISTINCT order_id) FROM e1_effects"
        ).fetchone()
    requested = len(set().union(*map(set, requests)))
    return {
        "experiment": "E1",
        "variant": variant,
        "workers": workers,
        "gap_ms": gap_ms,
        "jitter_ms": jitter_ms,
        "seed": seed,
        "orders_requested": requested,
        "side_effects": effects,
        "orders_released": distinct,
        "duplicate_effects": effects - distinct,
        "missed_orders": requested - distinct,
        "errors": len(errors),
        "deadlocks": errors.count("DeadlockDetected"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=10)
    args = parser.parse_args()
    rows = []
    for variant in VARIANTS:
        for workers in (2, 4, 8, 16):
            for jitter_ms in (0, 100, 1000):
                for seed in range(args.seeds):
                    row = run_trial(variant, workers, GAP_MS, jitter_ms, seed)
                    rows.append(row)
                    print(row, flush=True)
    write_rows("e1_claim.csv", rows)


if __name__ == "__main__":
    main()
