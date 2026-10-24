# Duplicate-side-effect defenses: a fault-injection testbed

A small, self-contained testbed for comparing ways to keep at-least-once pipelines from producing duplicate side effects. It accompanies the paper *Exactly-Once Effects on an At-Least-Once Pipeline* (in preparation).

Everything here is a clean-room re-implementation, using synthetic data. It contains no code or data from any production system.

## What it compares

| Experiment | Hazard | Variants |
|---|---|---|
| **E1** release claim | Concurrent triggers select overlapping work | read-then-update vs. conditional claim (`UPDATE … WHERE status = 'ELIGIBLE' RETURNING`) |
| **E2** message ledger | Redelivery; transient errors at the ledger insert; errors during processing | no ledger; catch-all ("any error means duplicate"); duplicate-key-only; ledger and side effect in one transaction |
| **E3** merge | Partial outputs merged into one per key; late starters, redelivery, consumer crash | merge per message; per message with an advisory lock; time window; scheduled barrier with deferred ack (with and without a settle window) |
| **E4** file claim | Workers competing for the same inbound files; a lost response on delete | no claim; copy-then-delete that moves the file to an error folder on failure; copy-then-delete that skips the file if it is already in processing; conditional create (generation precondition) |

## Running

Requires Docker.

```sh
./run_all.sh                                        # about 55 minutes; writes results/*.csv
docker compose run --rm runner testbed.analyze      # writes results/summary.md
docker compose down
```

Single experiments: `docker compose run --rm runner testbed.e2_ledger --seeds 1 --scenarios all_faults`.

## Pinned versions

- PostgreSQL 16.4 (alpine)
- Google Cloud SDK 575.0.0 Pub/Sub emulator
- fake-gcs-server 1.49.3 (Cloud Storage emulator)
- Python 3.12.7
- psycopg 3.2.3
- google-cloud-pubsub 2.26.1
- google-cloud-storage 2.18.2

## Determinism

Seeds fix the workload: E1's request sets and arrival times, E2's per-message fault decisions, E3's release plans, and E4's worker start times and fault decisions. Thread scheduling still varies between runs, so race outcomes vary. That's why each configuration runs several seeds.

## Simplifications

- **Side effects:** each is a database row, standing in for a file line or partner call.
- **E2 push delivery:** modelled with streaming pull and concurrent callbacks; a lost ack is modelled as a nack.
- **E3 merge commit:** the barrier's conditional update and its output record commit in one transaction. Production systems that write the file to object storage first have an extra crash window, which this testbed does not model.
- **E4 conditional claim:** the storage emulator ignores generation preconditions on copy, but honours them on upload. The conditional variant therefore reads the file and uploads it to `processing/` with "create only if absent". Cloud Storage supports the precondition on copy directly.
- **E4 lost response:** modelled as a delete that completes but raises an error to the caller.
- **Time scale:** seconds rather than minutes.

## License

MIT. See [LICENSE](LICENSE).
