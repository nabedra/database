# Testing harness wave 1 ledger
Base: bdf8d679 (origin/main). Started 17:20 Madrid 2026-11-22. Brief: common-brief.md.
Deferred this wave: phases 7-13 product seam refactor (nabedra-rt conversion, crate split, disk seam, OLTP sim, KV cluster sim, OLAP stream sim), Cache->KV rename.

| Task | Model | Branch | Phase | Depends | Status | PR | Review |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TH-CHECK-CORE | opus high | nabedra/th-check-core | 1 | - | merged 5774ef66 | #41 | sol approved (fixes 6453c2cd, 07c50ca3) |
| TH-MODELS | opus high | nabedra/th-models | 2 | CHECK-CORE | merged f03804a7 | #38 | sol approved (OLAP takeover traced: no false not_applied) |
| TH-HISTORY | opus high | nabedra/th-history | 1 | CHECK-CORE | merged 229cacfb | #46 | sol approved (3 fix commits: false passes fixed) |
| TH-MATRIX | opus high | nabedra/th-matrix | 0,18 | - | merged 6c28a40c | #37 | sol approved |
| TH-SIM-EVENTS | opus xhigh | nabedra/th-sim-events | 6 | - | merged 0dfcf828 | #42 | sol approved (500 seeds CI 12 min; nightly 4x5000) |
| TH-OLAP-STORE | opus high | nabedra/th-olap-store | 3 | CHECK-CORE | merged 60b9d3d9 | #43 | sol approved |
| TH-OLAP-LOGIC | opus high | nabedra/th-olap-logic | 3 | - | merged c7739683 | #45 | sol approved (4 explain goldens skipif: fixed-cluster footer) |
| TH-KV-CORE | opus high | nabedra/th-kv-core | 4 | CHECK-CORE | merged 2445d5f1 | #47 | sol approved (fixes 5166fca3, 8df7e083) |
| TH-REVIEW | opus high | nabedra/th-review | protocol | - | merged 77416de3 | #28 | sol approved (fix f2ba4ad1) |
| TH-INFRA | gpt-6.2-sol high | nabedra/th-infra | 0,14 | MATRIX (run_check) | merged 5d4c9b81 | #35 | opus approved (report pending) |
| TH-REGISTERS | sol high | nabedra/th-registers | 0 | - | merged f3a4005a | #29 | opus approved (fix d5c3ef46: OLAP-G01..05 R required on S3) |
| TH-BUGS | sol high | nabedra/th-bugs | 0 | - | merged 6088938e | #30 | opus approved (fix 61f31224) |
| TH-TOOLS | sol high | nabedra/th-tools | 14,17 | - | merged fbc024c1 | #33 | opus approved (4 fixes; repeat_gate bug fixed) |
| TH-EVENTS | sol high | nabedra/th-events | 5 | CHECK-CORE | merged 5f962162 | #36 | opus approved |
| TH-OLAP-ORACLE | sol high | nabedra/th-olap-oracle | 3 | CHECK-CORE | merged 27baa754 | #40 | opus approved (fix 337f888a: oracle used engine filter) |
| TH-KV-DIFF | sol high | nabedra/th-kv-diff | 4,16 | - | merged b35fe3d2 | #31 | opus approved (fixes 40c47756, 3ac3b384) |
| TH-FUZZ | sol high | nabedra/th-fuzz | 14 | - | merged c1a28665 | #39 | opus approved (fixes 1b35b55c, 084acd47) |
| TH-REALENV | sol high | nabedra/th-realenv | 15 | - | merged 4f5e399e | #32 | opus approved (fix 5926c240) |
| TH-CONFORMANCE | sol high | nabedra/th-conformance | 16 | - | merged 1fb6995b | #34 | opus approved (fixes d76336be, 66a95c68) |

Merge order plan: CHECK-CORE first (Cargo.toml/lock), then MATRIX+REGISTERS, then the rest; INFRA after MATRIX.
Reviews: cross-model (sol reviews opus PRs, opus reviews sol PRs), reviewer fixes small findings and merges after gate green.

## Forwarded changes (paths owned by others)
- TH-REVIEW → TH-INFRA (gh-actions.md): add `checks` input row; "Nothing starts on push" paragraph must name scheduled nightly pinned to main SHA; keep `#record-the-evidence` anchor.
- TH-REVIEW → TH-MATRIX: docs assume run_check.py --list --tier, matrix.py select/gate, states pass/fail/not-run/quarantined n/5/stale/n/a/missing; matrix must accept `hardware-deferred` in n/a like cloud-deferred.
- TH-BUGS → TH-INFRA: checks job checkout needs fetch-depth: 0 (bugs.py resolves historical SHAs).
- TH-KV-DIFF → TH-KV-CORE (follow-up): wrong-arity error text differs from Valkey; patch at /Users/antoniopenapena/.t3/worktrees/nabedra-database/927b667a-914f0d3c9a07fc10/artifacts/kv-wrong-arity-runtime.patch
- TH-REALENV → TH-INFRA (follow-up): kv-cluster-real needs prebuilt cache binary + redis wheel; docker-events job can run `python -m scripts.testing.kind.install --output "$RUNNER_TEMP/bin/kind"; NABEDRA_KIND=... python -m scripts.testing.kind.suite_events --image nabedra-events:ci --history events-acceptance/kind-events.jsonl` after building nabedra-events:ci.
- TH-TOOLS → TH-MATRIX: evidence schema run.date (date-time) + emit in run_info(); triage uses it for 14-day baseline.
- USER 17:25: no Cache->KV rename. Final cleanup PR after wave: module key kv->cache, kv.md->cache.md, KV-G*->CACHE-G*, check ids kv-*->cache-* (docs/testing/**, modules.toml, check tomls, skills refs).
- TH-INFRA → TH-CHECK-CORE reviewer: root Dockerfile dependency layer needs server/check manifest + empty lib stub (workspace member). fuzz-nightly toml needs pinned cargo-fuzz install → TH-FUZZ reviewer.
- TH-MATRIX → TH-CHECK-CORE reviewer: Rust evidence::Record must match docs/testing/evidence/schema.json (17 fields, kind space-separated letters, run{url,attempt,request_id,runner,date}, file <request_id>--<check>.json under yyyy/mm).
- FOLLOW-UP after TH-KV-CORE merges: cache wrong-arity error text fix (patches/kv-wrong-arity-runtime.patch) so cache-valkey-diff can pass.
- TH-REALENV follow-up: kv-cluster-real needs NABEDRA_CACHE_BINARY + NABEDRA_CACHE_ORM_ARTIFACTS in its command; full kind EV06/EV07 + K03 never run yet.
- FOLLOW-UP: ORM supervisor test test_compatible_path_binary_runs_both_api_and_worker flaky on main (predates wave) → quarantine candidate.
- QUARANTINE: PR #44 adds node::payload_owner::installation_tests::final_drain_receipt_account_publication_cleanup_and_release (owner harness-followup-olap-flake, expires 2026-12-06). Fix task to start in follow-up wave.
- FOLLOW-UP: catalog.json stale after #34 (7 ORM tests) → regenerate in final cleanup PR (with cache rename).
- SMOKE: harness-smoke dispatch on main c1a28665, run 45047452991 (11 merged checks).
- SMOKE RESULT run 45047452991: 10/11 checks pass on main c1a28665 with evidence records; kv-valkey-diff fail (194 diffs, wrong-arity text) → forwarded to TH-KV-CORE (steer). #44 merged 5763ddc0.
- NOTE for nabedra-builds skill: shared CARGO_TARGET_DIR across worktrees can link a stale path-dep rlib (nabedra-check) from another worktree; touch sources or use private target for small crates.

## Wave 2 (started 18:25)
| Task | Model | Branch | Status |
| --- | --- | --- | --- |
| W2-INGEST | sol high | nabedra/w2-ingest | merged 589dcf98 (#48); ledgers branch created; needs repo settings |
| W2-STREAM | opus high | nabedra/w2-stream | merged b1796928 (#49) |
| W2-FINAL | lead | - | after all merges: cache naming (kv→cache), catalog+matrix regen, builds-skill note, integrated review |
- W2-FINAL items: events register lists U for EVT-G02..G05 (events-state-model gives U); OLTP flake o03_stale_routes_inactive_gate_and_bad_bindings_write_nothing (NotLeader hint None, gate 45044010866) → watch/quarantine if repeats; events admin-limits.patch + leader-wait readiness hunks → product follow-ups.
- W2-FINAL: olap-oracles.toml scenarios=[U01,U02]; follow-up OLAP-G08 NULL-bearing parquet rejection test; scheduler fault hook for over-admission.
- W2-FINAL/follow-up: cache HELLO option error text, empty *0 frame (Valkey ignores), DEBUG DIGEST on cluster worker.
| W2-NAMING | sol high | nabedra/w2-naming | merged e3dea900 (#50) |
- USER DECISION NEEDED: enable repo settings 'Allow GitHub Actions to create and approve pull requests' + 'Allow auto-merge' for nabedra-evidence.yml.
| W3-WRITERS | sol high | nabedra/w3-writers | merged 5a5e4e8f (#51) |
- 19:28 #49 merged (stream sim). #50 merged e3dea900. FULL T1 run on main: 45055093094 (base + 29 T1 checks). Integrated review (opus high) running → nabedra/harness-integrated-fixes.
- 19:37 FULL T1 on main e3dea900 run 45055093094: ALL GREEN (base jobs + 29 T1 checks). Nightly launcher manual run 45055986409 success → dispatched nightly build run 45056014984 (T2 checks + fuzz 3600s).
- 20:46 Integrated review done: PR #52 merged e3d51a6a (4 blocking or important fixed, 3 minor fixed, 7 deferred). Report: integrated-review.md
- 20:50 #51 merged 5a5e4e8f; #52 integrated fixes merged e3d51a6a. Nightly 45056014984 (on e3dea90): 15 success, kind-events+cache-cluster-real fail (kind fixed in #52 after; cache-cluster-real known open), fuzz jobs still running. 0 open PRs.
