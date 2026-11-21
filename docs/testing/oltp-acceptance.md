Run the essential OLTP acceptance suite from the repository root:

```sh
python3.11 scripts/acceptance_oltp.py
```

The runner builds one cached test batch, copies the test/server executables, and reports O01–O29 as `passed`, `failed` or `not-run` in `artifacts/oltp-acceptance/results.json`. The log reports the number of executed clauses for each selected scenario. Exit zero requires all clauses of every selected scenario to pass. Exit one indicates failure. Exit two indicates incomplete execution or changing source.

| Category | Essential scenarios |
| --- | --- |
| Compatibility and packaging | O01–O02 |
| Keys, query semantics and indexes | O03–O04, O12 |
| Transactions and durable decisions | O06–O11, O29 |
| Resource limits, queue survival and cold recovery | O05, O13–O15, O22–O23 |
| Shards, volumes and unbounded table growth | O16, O21, O26, O28 |
| Deployment, autoscaling, backup and migration | O17–O20, O27 |
| Performance and scaling | O24–O25 |

Each scenario retains every assertion in [the OLTP contract](oltp-contract.md). Variations belong inside the scenario. The runner records the [scheduler identifiers and named regressions](scheduler-contract.md) mapped to each scenario. Original OLAP publication/upload assertions remain in that engine's regression checks. Existing component tests remain useful but cannot establish an entire production scenario by themselves.

For focused development use `--scenario O07` or `--category transactions`. `--list` prints the complete rubric without building. All agents use the same build/test lock:

```sh
python3.11 scripts/acceptance_oltp.py --cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --lib <affected-test-filter>
```

The runner reuses `CARGO_TARGET_DIR`, or the main checkout's existing cache. It sets two Cargo jobs, disables incremental/debug data and runs scenarios sequentially. `--target-dir` overrides the cache location. CPU/RAM enforcement for performance/provider assertions must be part of their actual fixture; these Cargo settings are not an operating-system memory limit.

Scenario library tests end in `acceptance_oNN` and use `#[ignore = "run through scripts/acceptance_oltp.py"]`. The runner explicitly executes them. Ordinary feature/regression runs do not recursively launch the acceptance suite or provision infrastructure. They use production services and deterministic barriers, and reuse existing helpers where appropriate. A missing external prerequisite must emit `NABEDRA_OLTP_ACCEPTANCE {"scenario":"O18","status":"not-run","reason":"precise missing prerequisite"}`. The runner requires exactly one executed test per scenario and a passing marker for every clause in its rubric. Tests must not call the locking wrapper recursively; any required Cargo subprocess is already protected by the suite's outer lock.

`NABEDRA_OLTP_BINARY` names the copied actual server. `--olap-binary` supplies `NABEDRA_OLAP_BINARY` for parity; `--provider-config` supplies an explicit provider test configuration. Real cloud scenarios require actual configured access and approved bounded resources. Modeled provider evidence does not replace them. Unavailable external checks stay `not-run` while local implementation and integration continue.

The runner owns the clause IDs, derived from the contract table. Use `--list` to look them up. After executing a clause's required variations, the scenario test emits one line in this format.

```text
NABEDRA_OLTP_ASSERT {"scenario":"O07","assertion":"private-before-commit","status":"passed","detail":"Prepared rows and secondary index entries stayed private."}
```

Use `failed` for an executed failure and `not-run` for a clause that could not execute. Give the reason in `detail`. The runner keeps the markers, required IDs and missing IDs in `results.json`. A test-process failure, a failed clause, an unknown ID or a duplicate marker fails the scenario. A missing clause keeps it `not-run` and appears by ID in the log.

The direct CI job runs these scenarios with direct OLTP on and off. Both CI jobs build and supply the independent OLAP executable for O01. O07 and O10 use the registered RF3 worker test executable for deterministic pauses through the real HTTP, Raft and native storage paths. O07 covers queued batch boundaries and the D08 point-write race. O10 covers the Q05, Q06 and T04 decision and receipt cases.
