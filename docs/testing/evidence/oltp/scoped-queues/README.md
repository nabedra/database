# Scoped OLTP queue evidence

The scoped runtime places eligible registered point Get and Update requests on the queue for their Data range. It uses two exact RF3 payload copies and two matching acceptance markers before dispatch. A successful mutation receipt requires the durable decision, participant settlement, and two exact terminal copies. Explicit capability activation controls new scoped admission. Older UUIDs retain their original admission policy.

## Correctness

| Executed scope | Scenario references | Result |
| --- | --- | --- |
| Registered RF3, two authenticated HTTPS gateways, three instrumented workers | O01–O04, O10, O14, O23, O30, Q03, Q05, Q06 | Ordinary Python Get hit and miss, Update hit and miss, and legacy create, insert, describe, list, collect, count and duplicate-key conflict returned their expected results. A Get held after acceptance for 2.5 seconds returned its original row. The test recorded the native Timestamp decision and exact per-phase copy stamps. |
| Separate RF3 payload, marker and terminal holds | O30, Q03, Q05, Q06 | Two held followers prevented the marker phase from returning 202 or dispatching. The submitting API was killed before any follower installed the payload in a separate case. The same identity completed after one follower resumed in each case. After a durable update decision, both held terminal copies left the receipt `committing`. Releasing one follower returned the exact result. |
| Registered retries and expiry | O10, O14, O23, O30, Q05, Q06 | Both gateways obtained the same receipt. A different value and different raw bytes conflicted with the original UUID. Exact replay preserved a later update. Scoped and legacy read receipts expired explicitly, and mutation receipts remained exact. |
| Three registered workers all stopped and restarted on their original volumes | O15, O32, R04, R05, Q06 | `node.cold` published a new run and fresh enrollments. The old Timestamp commit stopped before Data/Status projection recovered its exact receipt and row. A pending pre-Prepare request was abandoned. Both old UUIDs left later writes unchanged, and neither dispatched in the new run. |
| Linux request-specific expiry regression and three copy-model assertions | O30, Q03, Q05, Q06, R01, R03 | All four passed. Expiry fenced pending payload and repair writes for the expired UUID. The unrelated request could finish, transfer still saw the change, and registration charges were released. |
| Linux Control rebase regression at `339eaf1` | O14, O16, O31, R01, R03 | The exact-owner regression and the rebase assertion passed. Publishing legacy Ready removal made the old destination plan stale. Control required the exact removed incarnation and new frozen-source, installation and fence proofs before accepting the higher term. |
| Native RocksDB and Raft assertions | O03, O06, O10, O11, O14–O16, O19, O23, Q03, Q05, Q06, R04, R05 | The native point test recovered the first Timestamp, Data and Status positions at indices 8, 3 and 5. The exact old epochs could not commit after either fence. The native RF1 process resumed four queued, prepared and committed histories with their specified outcomes. The existing three-peer TLS Raft and durable process tests passed on the integrated `4d1cca8` runtime. |

Both registered copy and full-stop fixtures passed the listed assertions with the `6d9a401` Linux workers and optimized gateway. The copy fixture then reached the owner-loss receipt deadline while the global fence was pending. Follow-up traces showed consecutive group RPCs waiting 30 seconds on the paused peer. Bounded fencing and retirement reads let a later run publish global fencing and legacy Ready removal while the scoped owner remained paused. Changing the ready set during an existing scoped transfer required the rebase added in `339eaf1`. The exact Linux rebase assertion passed. The registered rerun on `339eaf1` stopped before the owner-loss cut while waiting on the initial gated Get dispatch checkpoint. Live same-run owner takeover therefore remains unqualified.

The optimized `6d9a401` build also passed healthy registered ORM and Q05/Q06 cases. The whole-worker follower pause variants returned preaccept overload or unknown while metadata elections were in progress. The deterministic copy holds in the table isolate a slow queue replica with its metadata groups available. Whole-worker progress through concurrent metadata elections remains unverified. The 3/6/12-worker scale series, last-holder isolation and destruction, transfer under a live shard split, and scoped point archive restore have no process-level evidence in this run.

Python discovery passed 189 tests and skipped the optional Arrow case, which passed with PyArrow 19.0.1. The Python harness passed 24 tests. TypeScript passed 41 tests with three live fixture skips. The separately selected real TypeScript RF1 fixture also passed. The native OLTP library ran 267 tests on `a10fb1d`. It reported 253 passes, nine ignored tests and five failures. All five reproduced on `d330e60`. The two O19 archive storage tests passed with a canonical macOS `TMPDIR`. The inherited C02, C07/M02 and O22/C07/F03 assertions still fail in their fixtures.

### Reproduction

Use an OLTP Linux release binary and OLTP library test executable built with `--no-default-features --features oltp`. These process fixtures reserve three 1-CPU workers with 2.15 GB each, a 0.35-CPU gateway with 550 MB, and a 0.65-CPU driver with 900 MB. The secondary gateway shares the driver cgroup. The aggregate cap is 4 CPUs and 7.9 GB with swap disabled.

```sh
python3.11 -B server/tests/oltp_scoped_acceptance.py --worktree "$PWD" \
  --image "$FIXTURE_IMAGE" --binary "$NABEDRA_DB" \
  --test-executable "$NABEDRA_LIB_TEST" --output "$SCOPED_OUTPUT"
python3.11 -B server/tests/oltp_scoped_cold_acceptance.py --worktree "$PWD" \
  --image "$FIXTURE_IMAGE" --binary "$NABEDRA_DB" \
  --test-executable "$NABEDRA_LIB_TEST" --output "$COLD_OUTPUT"
cargo test --locked --manifest-path server/Cargo.toml --no-default-features \
  --features oltp --lib q03_q06_scoped_expiry_only_fences_inserts_for_its_request
```

The fixture image ID is `sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc`. The `6d9a401` executable for the copy cuts has SHA-256 `efe76cc7c7e2b4c7473d9cd74133204d5f5781ee88d77acb339aa5ed86ee9ab7`.
