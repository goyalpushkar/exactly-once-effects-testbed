#!/bin/sh
# Runs every experiment and writes CSVs to results/. About 55 minutes on a laptop.
set -e
docker compose up -d postgres pubsub gcs
docker compose run --rm runner testbed.e1_claim --seeds 10
docker compose run --rm runner testbed.e2_ledger --seeds 5
docker compose run --rm runner testbed.e3_merge --seeds 3
docker compose run --rm runner testbed.e4_file_claim --seeds 3
