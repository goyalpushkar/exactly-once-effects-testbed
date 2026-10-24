"""E4: file claim by competing workers on one object store (hazard H4; defense M2).

A batch of inbound files lands in `inbox/`. Several workers poll the folder at about the same
time, list it in the same (sorted) order, and try to claim each file. A claimed file is read
from `processing/`, processed (one side-effect row per file, standing in for loading it), and
archived to `archive/`.

Variants:
  no_claim          Process straight from inbox/, then archive. No claim at all.
  move_then_error   Claim by copy-then-delete with retries; if the move fails, move the file in
                    processing/ to error/.
  move_then_check   Claim by copy-then-delete with retries; if the move fails, skip the file when
                    it is in processing/, otherwise raise a failure ticket.
  conditional       Claim by creating processing/<file> only if it does not exist (generation
                    precondition 0); losers get 412 and skip. Then delete the source, treating
                    "already gone" as success.

Processing time matters: whether a losing worker's checks see the winner's file in processing/
depends on whether the winner finished before the loser's retries ran out. Both cases are run.

Fault: with probability p_lost_delete, a delete that succeeded reports an error to the caller
(a lost response), as a timeout after a completed request would.

Usage: python -m testbed.e4_file_claim [--seeds 3]
"""

import argparse
import threading
import time
import uuid

from google.api_core import exceptions as gx
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

from testbed.common import InjectedFault, connect, rng_for, write_rows

N_FILES = 40
PROC_S = (0.1, 0.3)  # per-file processing time: shorter and longer than a losing worker's retries
RETRIES = 3  # the move is retried up to a configured count...
RETRY_SLEEP_S = 0.05  # ...sleeping between attempts (scaled down from seconds)
JITTER_S = 0.05  # spread of worker start times


def client():
    return storage.Client(project="testbed", credentials=AnonymousCredentials())


class Trial:
    def __init__(self, variant, workers, proc_s, p_lost_delete, seed):
        self.variant, self.workers, self.proc_s, self.p, self.seed = variant, workers, proc_s, p_lost_delete, seed
        self.bucket = client().create_bucket(f"e4-{uuid.uuid4().hex[:12]}")
        self.lock = threading.Lock()
        self.tickets = 0  # failure tickets raised for files someone else had already claimed
        self.errors = []
        self.op = 0

    # Object-store operations, with the injected fault on delete.

    def delete(self, bucket, name, w):
        bucket.blob(name).delete()
        with self.lock:
            self.op += 1
            n = self.op
        if rng_for("e4-del", self.seed, w, name, n).random() < self.p:
            raise InjectedFault("delete succeeded but the response was lost")

    def move(self, bucket, src, dst, w):
        """Move by copy, then delete the source, retried on any error."""
        for attempt in range(RETRIES + 1):
            try:
                bucket.copy_blob(bucket.blob(src), bucket, dst)
                self.delete(bucket, src, w)
                return True
            except (gx.GoogleAPICallError, InjectedFault):
                if attempt < RETRIES:
                    time.sleep(RETRY_SLEEP_S)
        return False

    def exists(self, bucket, name):
        return bucket.blob(name).exists()

    # Claims.

    def claim(self, bucket, name, w):
        """Return True if this worker owns the file in processing/."""
        src, dst = f"inbox/{name}", f"processing/{name}"
        if self.variant == "no_claim":
            return True
        if self.variant == "conditional":
            try:
                data = bucket.blob(src).download_as_bytes()
            except gx.NotFound:
                return False
            try:
                # The emulator ignores preconditions on copy, so the conditional copy is a
                # read followed by a create-if-absent upload. Cloud Storage supports both.
                bucket.blob(dst).upload_from_string(data, if_generation_match=0)
            except gx.PreconditionFailed:
                return False
            for attempt in range(RETRIES + 1):
                try:
                    self.delete(bucket, src, w)
                    break
                except gx.NotFound:
                    break  # already gone: the claim is what matters
                except InjectedFault:
                    time.sleep(RETRY_SLEEP_S)
            return True
        if self.move(bucket, src, dst, w):
            return True
        if self.variant == "move_then_error":
            self.move(bucket, dst, f"error/{name}", w)
            return False
        # move_then_check
        if not self.exists(bucket, dst):
            with self.lock:
                self.tickets += 1
        return False

    # Processing.

    def process(self, bucket, conn, name, w):
        path = f"inbox/{name}" if self.variant == "no_claim" else f"processing/{name}"
        try:
            bucket.blob(path).download_as_bytes()
        except gx.NotFound:
            return  # the file was moved away under this worker
        time.sleep(self.proc_s)
        conn.execute("INSERT INTO e4_effects (file, worker) VALUES (%s, %s)", (name, w))
        self.move(bucket, path, f"archive/{name}", w)

    def worker(self, w, start_delay):
        bucket = client().bucket(self.bucket.name)
        try:
            with connect() as conn:
                time.sleep(start_delay)
                while True:
                    names = sorted(b.name.split("/", 1)[1] for b in bucket.list_blobs(prefix="inbox/"))
                    if not names:
                        return
                    for name in names:
                        if self.claim(bucket, name, w):
                            self.process(bucket, conn, name, w)
        except Exception as exc:  # report, don't hide
            self.errors.append(type(exc).__name__)

    def run(self):
        with connect() as admin:
            admin.execute("DROP TABLE IF EXISTS e4_effects")
            admin.execute("CREATE TABLE e4_effects (id bigserial PRIMARY KEY, file text, worker int)")
        names = [f"f{i:03d}.txt" for i in range(N_FILES)]
        for name in names:
            self.bucket.blob(f"inbox/{name}").upload_from_string(f"synthetic {name}\n".encode())

        delays = [rng_for("e4-start", self.seed, w).uniform(0, JITTER_S) for w in range(self.workers)]
        threads = [threading.Thread(target=self.worker, args=(w, delays[w])) for w in range(self.workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        with connect() as admin:
            effects, distinct = admin.execute(
                "SELECT count(*), count(DISTINCT file) FROM e4_effects"
            ).fetchone()
            processed = {r[0] for r in admin.execute("SELECT DISTINCT file FROM e4_effects")}

        def count(prefix):
            return sum(1 for _ in self.bucket.list_blobs(prefix=prefix))

        in_error, in_processing = count("error/"), count("processing/")
        stranded = sum(
            1 for b in self.bucket.list_blobs(prefix="processing/") if b.name.split("/", 1)[1] not in processed
        )
        for b in list(self.bucket.list_blobs()):
            b.delete()
        self.bucket.delete()
        return {
            "experiment": "E4",
            "variant": self.variant,
            "workers": self.workers,
            "proc_s": self.proc_s,
            "p_lost_delete": self.p,
            "seed": self.seed,
            "files": N_FILES,
            "side_effects": effects,
            "files_processed": distinct,
            "duplicate_processing": effects - distinct,
            "unprocessed": N_FILES - distinct,
            "in_error_folder": in_error,
            "stranded_in_processing": stranded,
            "left_in_processing": in_processing,
            "false_tickets": self.tickets,
            "errors": len(self.errors),
        }


VARIANTS = ("no_claim", "move_then_error", "move_then_check", "conditional")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--variants", default=",".join(VARIANTS))
    args = parser.parse_args()
    rows = []
    for variant in args.variants.split(","):
        for workers in (2, 4, 8):
            for proc_s in PROC_S:
                for p in (0.0, 0.05):
                    for seed in range(args.seeds):
                        row = Trial(variant, workers, proc_s, p, seed).run()
                        rows.append(row)
                        print(row, flush=True)
    write_rows("e4_file_claim.csv", rows)


if __name__ == "__main__":
    main()
