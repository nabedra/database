# Guarantees and assurance levels

Every module states what it promises in a guarantee register, one file per module in
`docs/testing/guarantees/<module>.md`. Start the review with the guarantees the diff can affect.

## Find the affected IDs

1. Set `BASE_SHA` and `HEAD_SHA` to the commits under review and select
   the checks for the diff.

   ```sh
   python3 scripts/testing/matrix.py select --base "$BASE_SHA" --head "$HEAD_SHA"
   ```

   It maps each changed path through `docs/testing/modules.toml` to a module, each module to its
   guarantees, and each guarantee to the checks in `docs/testing/checks/*.toml`. It prints the
   module's assurance level, the guarantee IDs the review must cover and the T1 check set.
2. Read each selected guarantee's row in its register: the failure model, the scenario IDs, the
   required check kinds and any `n/a` reasons.
3. Read the cells for the base commit in `docs/testing/coverage-matrix.md`. A cell that is not
   `pass` on the base is a gap the diff inherits, and the rubric says so.
4. Add guarantees the path map misses. Typical cases are a shared type, a wire or on-disk format
   another module reads, a configuration default, or a protocol whose caller lives elsewhere.
   Give the reason in the rubric.
5. A test that maps to no guarantee fails the matrix build. If a code path
   lacks a module or a test lacks a guarantee, send the missing mapping
   to the owner of `docs/testing/modules.toml` or the corresponding check file.

The rubric then lists the selected checks, the assurance level and the affected guarantees. It
covers each guarantee or states why it does not apply, and gives a reason for every check it adds
to or drops from the selected set.

## Register format

Each register has:

- a line `Module: <module>`;
- a line `Level: L1`, `Level: L2` or `Level: L3`. A module with mixed levels uses its highest and
  states the split in prose;
- exactly one table, whose header is `| ID | Guarantee | Failure model | Scenarios | Checks | n/a |`.

In that table:

- **ID** uses the module prefix and a number, such as `OLTP-G04` or `KV-G02`. IDs are never reused.
- **Checks** is a space-separated list of the kind letters below, such as `M S H R`.
- **n/a** holds `<kind>: <reason>` entries separated by `;`, or is empty.
- For a check that needs cloud compute, put `R: cloud-deferred (<what>)` in
  the `n/a` column and keep R in Checks. For a check that needs dedicated
  hardware, use `<kind>: hardware-deferred (<what>)` and keep its kind in
  Checks. Both deferrals remain visible as `not-run` cells.

```text
| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| XX-G01 | An acknowledged write survives one node crash | crash, restart, lost reply | X01, X02 | M S H R | R: cloud-deferred (multi-host soak) |
```

| Kind | Meaning |
| --- | --- |
| M | Model check of the protocol (`nabedra_check::model::explore`) |
| U | Unit or property test (`nabedra_check::prop`) |
| S | Simulation workload with a nemesis |
| H | History check of recorded client operations (`nabedra_check::checker`) |
| F | Fuzzing of a decoder or parser (`server/fuzz`) |
| D | Differential test against a reference implementation |
| R | Real environment: processes, Docker, kind or real S3 |
| P | Performance, through the paired A/B runner |
| K | Soak |

Registers are hot files. Their owner for the wave makes every edit. A change never removes a
required check kind to turn a cell green. Moving a kind to n/a needs a reason in the row and an
approval from a reviewer that did not author the change.

## Assurance levels

The level sets the required checks and the budgets.

| Level | Typical scope | Required checks | T1 budget | Release budget |
| --- | --- | --- | --- | --- |
| L1 | Standalone engines, query semantics, scheduler internals, ORM clients | U with generators, D against a reference, F on every decoder, golden logic tests, R smoke in Docker, P paired A/B | 1,000 generated cases per property | 100,000 cases per property |
| L2 | Replicated or coordinated protocols: controllers, leases, failover, logs, commit protocols, streaming recovery | L1 plus an M model of the core protocol, S with a nemesis, H with the module's checker, R with faults in Docker and kind, an upgrade and downgrade matrix | 500 seeds per touched workload | 200,000 clean seeds on the release SHA |
| L3 | The strictly serializable transactional path | L2 plus the strict-serializability checker, the bug-replay corpus at 80%, mutation runs on all touched code | 2,000 seeds | 1,000,000 seeds |

The nightly core-hour split between modules lives in `docs/testing/modules.toml`, so the
orchestrator can move it as bug rates change.
