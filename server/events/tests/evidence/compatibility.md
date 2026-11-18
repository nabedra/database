# Events compatibility evidence

This covers EV05 against the committed main, OLTP, and OLAP streaming tips
available during Docker acceptance. The Events source came from a temporary
Git index and a synthetic commit with tree
`cdb08c760d48b9288e218a7b48f81fd8e03e8c4e`. The branch refs and active
worktrees were left untouched.

## Merge checks

For each pair, `git merge-tree --write-tree` exited successfully in both
directions and returned the same tree in both directions.

| Base checked | Commit | Merged tree |
| --- | --- | --- |
| Main | `0d02e105183e12626e18332bfd35a23ba3266530` | `cdb08c760d48b9288e218a7b48f81fd8e03e8c4e` |
| OLTP | `6dfc7134c5da37b6f926b55b7609cb43fb7f490b` | `2224f3bcbb94aff10e9c01c3dbc17c47239a5b31` |
| OLAP streaming, Docker source | `75c4c57eb753327a11adf0f3df4e4fe5e137e8e6` | `16c66deff1b22853df6f180d24fe6889feed3838` |
| OLAP streaming, subsequent tip | `b30fc1ff4527b5c83a13b80e9b1e228c89d7d397` | `2862e581cc8186b59e1704b8bd426c0391eaba23` |
| OLAP streaming, final sample | `7e66f607b5962d62c670f727a268938ad36d7ca4` | `3734596820cba69a163c30596beaf67a845bf074` |

The checked OLTP commit is an ancestor of the OLAP streaming commit used for
Docker. Applying Events before or after merging those two branches produced
`16c66deff1b22853df6f180d24fe6889feed3838`. Each merged tree introduced
only the Events plan and files under `server/events` and the Python Events
package, docs, and tests.

## Docker dependency and packaging checks

Rust 1.94.1 `cargo tree --locked -e normal` resolved these dependency
profiles from the combined tree. The Rust runner used 0.75 CPU and 512 MiB.
The Docker image ID was
`sha256:5394100e525b2e359bd563dc5b4626ad7658e1a261e9817cd2c42bcc69618244`.

| Cargo profile | Dependencies observed |
| --- | --- |
| Default OLAP | Parquet, Arrow IPC, AWS SDK for S3 |
| OLAP without S3 | Parquet and Arrow IPC |
| OLTP without S3 | OpenRaft 0.10.0 and RocksDB 0.25.0 |
| OLTP with S3 | OpenRaft 0.10.0, RocksDB 0.25.0, AWS SDK for S3 |
| Standalone Events | OpenRaft 0.9.25 and RocksDB 0.25.0 |

The assertions checked for the expected libraries and absence of libraries
from the other profiles. The four database graphs excluded `nabedra-events`.
Events resolved without the database crate, Arrow, Parquet, or the S3 SDK.
`cargo package --locked --allow-dirty --list` also showed that the database
crate's 293-file archive excludes the Events crate.

The Python 3.12.1 Docker runner built wheels before and after the merge with
0.25 CPU and 192 MiB. The candidate's five additional wheel entries were all
under `nabedra/events/`. The 32 entries outside the generated `RECORD`,
including package metadata, had identical contents. Installing
the candidate wheel offline with `--no-deps` allowed imports of
`nabedra.Database` and `nabedra.events.Events`, and
`python -m nabedra.events.sink --help` exited successfully.

## O01 and O02 Docker execution

The locked no-S3 OLAP and OLTP builds used Rust 1.94.1, one Cargo job and
dev debug info and incremental builds disabled. The build container had
2 GiB and used 0.75 CPU while the wheel checks ran, increasing to 1 CPU
afterward. It reached its cgroup memory limit with zero OOM events. The
OLTP fixture linked the same RocksDB 11.8.1 shared build used for the Events
Docker QA. Both binary commands reported their engine.

| Binary | Version output | SHA-256 |
| --- | --- | --- |
| OLAP without S3 | `nabedra-db 0.1.1 olap` | `660ba0f7fba8ebf6a052949b2bd388865ac19ef6a5faa46299db6894be71257c` |
| OLTP without S3 | `nabedra-db 0.1.1 oltp` | `e9b5264a2d731fcb962d72b171d2df5e98d94ce67cea167334c97d2b07242a02` |

`cargo check --no-default-features` and
`cargo check --no-default-features --features olap,oltp` both failed
with the expected diagnostic `select exactly one engine: olap or oltp`.

O02 also exercised standalone OLTP startup without AWS or catalog
credentials. After a successful start and shutdown, separate local fixtures
changed the recorded pool's engine to `olap` and its format version to
`2`. Both subsequent starts exited with the pool identity mismatch
diagnostic. Each preserved the manifest and pool file inventory. The
Docker runner used 0.5 CPU and 384 MiB, with a 111,423,488-byte cgroup peak
and zero OOM events. Distributed enrollment and the two S3-enabled
binary builds remain unexecuted. The S3 rows in the dependency table
report Cargo resolution only.

For O01, the existing `server/tests/oltp_orm_contract.py` ran unchanged
against the merged OLAP binary. The real OLAP API and worker used the
SQLite conditional object store over loopback. All 13 operations passed,
including results and ordering, authentication, database scope, receipt
replay, stale table handles, and the 1, 100 and 1,000-row cases.

On the checked OLTP baseline, the committed process helper passed
`--workers 3`. Startup rejected it because the same commit requires
four threads. An isolated launcher supplied `--workers 4` without
changing the source or the ORM program. At 1 CPU and 768 MiB the OLTP
run reached scale testing and returned `UnknownOutcomeError` during
cleanup. A fresh run at 2 CPUs and 1.25 GiB traced the same visible
failure to the cleanup `table.drop` at line 112 in the 1,000-row case.
The shutdown then exceeded its 15-second watchdog and the helper killed
the process. The last runner used `--network none`, peaked at
146,128,896 cgroup bytes and recorded zero OOM events. O01 parity remains
unverified. The Events merge changes only Events paths, so the server and
shared ORM code exercised here is identical to the checked OLAP
streaming commit.
