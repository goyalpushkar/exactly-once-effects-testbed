# Testbed results summary

## E1: release claim

| variant | workers | arrival window (ms) | trials | duplicate effects per 1,000 released (mean) | max | missed orders (total) | deadlocks (total) |
|---|---|---|---|---|---|---|---|
| conditional_claim | 2 | 0 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 2 | 100 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 2 | 1000 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 4 | 0 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 4 | 100 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 4 | 1000 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 8 | 0 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 8 | 100 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 8 | 1000 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 16 | 0 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 16 | 100 | 10 | 0 | 0 | 0 | 0 |
| conditional_claim | 16 | 1000 | 10 | 0 | 0 | 0 | 0 |
| read_then_update | 2 | 0 | 10 | 335 | 399 | 0 | 0 |
| read_then_update | 2 | 100 | 10 | 235 | 399 | 0 | 0 |
| read_then_update | 2 | 1000 | 10 | 40 | 399 | 0 | 0 |
| read_then_update | 4 | 0 | 10 | 1123 | 1174 | 0 | 0 |
| read_then_update | 4 | 100 | 10 | 838 | 1139 | 0 | 0 |
| read_then_update | 4 | 1000 | 10 | 143 | 430 | 0 | 0 |
| read_then_update | 8 | 0 | 10 | 2256 | 3040 | 41 | 16 |
| read_then_update | 8 | 100 | 10 | 1711 | 2764 | 0 | 0 |
| read_then_update | 8 | 1000 | 10 | 208 | 645 | 0 | 0 |
| read_then_update | 16 | 0 | 10 | 4865 | 7000 | 10 | 43 |
| read_then_update | 16 | 100 | 10 | 3466 | 4565 | 0 | 0 |
| read_then_update | 16 | 1000 | 10 | 558 | 875 | 0 | 0 |

## E2: message ledger

| scenario | variant | trials | duplicates per 1,000 messages | dropped per 1,000 messages | deliveries per message | incomplete trials |
|---|---|---|---|---|---|---|
| redelivery | no_ledger | 5 | 87.3 | 0.0 | 1.09 | 0 |
| redelivery | catch_all | 5 | 0.0 | 0.0 | 1.08 | 0 |
| redelivery | dup_key_only | 5 | 0.0 | 0.0 | 1.08 | 0 |
| redelivery | transactional | 5 | 0.0 | 0.0 | 1.08 | 0 |
| redelivery+ledger_err | no_ledger | 5 | 87.3 | 0.0 | 1.09 | 0 |
| redelivery+ledger_err | catch_all | 5 | 0.0 | 50.7 | 1.07 | 0 |
| redelivery+ledger_err | dup_key_only | 5 | 0.0 | 0.0 | 1.14 | 0 |
| redelivery+ledger_err | transactional | 5 | 0.0 | 0.0 | 1.14 | 0 |
| redelivery+process_err | no_ledger | 5 | 89.3 | 0.0 | 1.15 | 0 |
| redelivery+process_err | catch_all | 5 | 0.0 | 52.7 | 1.13 | 0 |
| redelivery+process_err | dup_key_only | 5 | 0.0 | 52.7 | 1.13 | 0 |
| redelivery+process_err | transactional | 5 | 0.0 | 0.0 | 1.14 | 0 |
| all_faults | no_ledger | 5 | 89.3 | 0.0 | 1.15 | 0 |
| all_faults | catch_all | 5 | 0.0 | 102.7 | 1.12 | 0 |
| all_faults | dup_key_only | 5 | 0.0 | 52.7 | 1.19 | 0 |
| all_faults | transactional | 5 | 0.0 | 0.0 | 1.20 | 0 |

## E3: merge barrier

| scenario | variant | trials | duplicate lines per 1,000 releases | split keys per trial (of 30) | missing releases (total) | merge latency median (s) | p95 (s) |
|---|---|---|---|---|---|---|---|
| baseline | push_naive | 3 | 111 | 3.3 | 0 | 0.04 | 0.06 |
| baseline | push_lock | 3 | 0 | 0.3 | 0 | 0.05 | 0.07 |
| baseline | time_window | 3 | 0 | 0.0 | 0 | 2.71 | 3.42 |
| baseline | barrier_s0 | 3 | 0 | 0.0 | 0 | 0.53 | 0.92 |
| baseline | barrier | 3 | 0 | 0.0 | 0 | 1.98 | 2.45 |
| baseline | barrier_orders | 3 | 0 | 0.0 | 0 | 0.53 | 0.91 |
| late_start | push_naive | 3 | 542 | 23.7 | 0 | 0.05 | 0.07 |
| late_start | push_lock | 3 | 0 | 23.7 | 0 | 0.05 | 0.07 |
| late_start | time_window | 3 | 0 | 14.3 | 0 | 2.80 | 3.91 |
| late_start | barrier_s0 | 3 | 0 | 19.7 | 0 | 0.53 | 0.96 |
| late_start | barrier | 3 | 0 | 5.0 | 0 | 2.16 | 2.99 |
| late_start | barrier_orders | 3 | 0 | 0.0 | 0 | 0.52 | 0.97 |
| redelivery | push_naive | 3 | 269 | 6.0 | 0 | 0.04 | 0.07 |
| redelivery | push_lock | 3 | 0 | 0.3 | 0 | 0.05 | 0.07 |
| redelivery | time_window | 3 | 0 | 0.0 | 0 | 2.72 | 3.42 |
| redelivery | barrier_s0 | 3 | 0 | 0.0 | 0 | 0.51 | 0.89 |
| redelivery | barrier | 3 | 0 | 0.0 | 0 | 1.97 | 2.44 |
| redelivery | barrier_orders | 3 | 0 | 0.0 | 0 | 0.51 | 0.89 |
| crash | push_naive | 3 | 155 | 4.3 | 0 | 0.04 | 0.07 |
| crash | push_lock | 3 | 0 | 0.3 | 0 | 0.05 | 0.07 |
| crash | time_window | 3 | 0 | 0.0 | 0 | 2.71 | 3.42 |
| crash | barrier_s0 | 3 | 0 | 0.0 | 0 | 0.52 | 0.90 |
| crash | barrier | 3 | 0 | 0.0 | 0 | 1.97 | 2.44 |
| crash | barrier_orders | 3 | 0 | 0.0 | 0 | 0.51 | 0.90 |

### E3: rush orders that keep arriving

| variant | trials | orders per key | merged outputs per key | duplicate lines per 1,000 releases | missing releases (total) | wait from order to output, median (s) | p95 (s) |
|---|---|---|---|---|---|---|---|
| rush_no_merge | 3 | 9.7 | 9.72 | 0 | 0 | 3.72 | 5.75 |
| rush_w0 | 3 | 9.7 | 2.53 | 0 | 0 | 3.70 | 5.72 |
| rush_w5 | 3 | 9.7 | 2.46 | 0 | 0 | 8.29 | 10.02 |
| rush_w10 | 3 | 9.7 | 2.46 | 0 | 0 | 13.33 | 15.05 |

## E4: file claim

| variant | workers | processing (s) | p lost delete | trials | duplicate processing per 1,000 files | unprocessed per 1,000 | in error folder per 1,000 | stranded in processing per 1,000 | false tickets per 1,000 | errors |
|---|---|---|---|---|---|---|---|---|---|---|
| conditional | 2 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 2 | 0.1 | 0.05 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 2 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 2 | 0.3 | 0.05 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 4 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 4 | 0.1 | 0.05 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 4 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 4 | 0.3 | 0.05 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 8 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 8 | 0.1 | 0.05 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 8 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| conditional | 8 | 0.3 | 0.05 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| move_then_check | 2 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 975 | 0 |
| move_then_check | 2 | 0.1 | 0.05 | 3 | 0 | 17 | 0 | 17 | 958 | 0 |
| move_then_check | 2 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 150 | 0 |
| move_then_check | 2 | 0.3 | 0.05 | 3 | 0 | 25 | 0 | 25 | 117 | 0 |
| move_then_check | 4 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 2892 | 0 |
| move_then_check | 4 | 0.1 | 0.05 | 3 | 0 | 25 | 0 | 25 | 2842 | 0 |
| move_then_check | 4 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 308 | 0 |
| move_then_check | 4 | 0.3 | 0.05 | 3 | 0 | 50 | 0 | 50 | 650 | 0 |
| move_then_check | 8 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 6558 | 0 |
| move_then_check | 8 | 0.1 | 0.05 | 3 | 0 | 67 | 0 | 67 | 6058 | 0 |
| move_then_check | 8 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 2483 | 0 |
| move_then_check | 8 | 0.3 | 0.05 | 3 | 0 | 50 | 0 | 50 | 2583 | 0 |
| move_then_error | 2 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| move_then_error | 2 | 0.1 | 0.05 | 3 | 0 | 42 | 42 | 0 | 0 | 0 |
| move_then_error | 2 | 0.3 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| move_then_error | 2 | 0.3 | 0.05 | 3 | 0 | 42 | 42 | 0 | 0 | 0 |
| move_then_error | 4 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| move_then_error | 4 | 0.1 | 0.05 | 3 | 0 | 58 | 58 | 0 | 0 | 0 |
| move_then_error | 4 | 0.3 | 0.0 | 3 | 0 | 0 | 8 | 0 | 0 | 0 |
| move_then_error | 4 | 0.3 | 0.05 | 3 | 0 | 58 | 67 | 0 | 0 | 0 |
| move_then_error | 8 | 0.1 | 0.0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| move_then_error | 8 | 0.1 | 0.05 | 3 | 0 | 50 | 50 | 0 | 0 | 0 |
| move_then_error | 8 | 0.3 | 0.0 | 3 | 0 | 0 | 50 | 0 | 0 | 0 |
| move_then_error | 8 | 0.3 | 0.05 | 3 | 0 | 50 | 83 | 0 | 0 | 0 |
| no_claim | 2 | 0.1 | 0.0 | 3 | 400 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 2 | 0.1 | 0.05 | 3 | 433 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 2 | 0.3 | 0.0 | 3 | 658 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 2 | 0.3 | 0.05 | 3 | 675 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 4 | 0.1 | 0.0 | 3 | 1200 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 4 | 0.1 | 0.05 | 3 | 1292 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 4 | 0.3 | 0.0 | 3 | 1950 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 4 | 0.3 | 0.05 | 3 | 2008 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 8 | 0.1 | 0.0 | 3 | 2800 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 8 | 0.1 | 0.05 | 3 | 2858 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 8 | 0.3 | 0.0 | 3 | 4550 | 0 | 0 | 0 | 0 | 0 |
| no_claim | 8 | 0.3 | 0.05 | 3 | 4617 | 0 | 0 | 0 | 0 | 0 |
