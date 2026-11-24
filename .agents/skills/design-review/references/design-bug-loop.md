# Design bug loop

Use this procedure for every counterexample TLC finds in a design, and for
code review findings where the code follows the design and the design is
wrong. The fix includes the property, fault or bound that catches the bug.

## Steps

1. **Classify.** Use the classes in the [bug loop](../../code-review/references/bug-loop.md#steps).
   Name the module, the guarantees and the property the trace breaks.
2. **Keep the shortest trace.** Save the TLC trace from `-dumpTrace json`
   and write it in the plan as numbered steps. See
   [read a counterexample](tla.md#read-a-counterexample).
3. **Check that it is a design bug.** Find the step the plan does not
   allow. If the plan forbids that step and the spec allows it, the spec is
   wrong. Fix the spec, note the fix in the plan, and stop here. If the
   plan allows the step, it is a design bug.
4. **Fix the plan and the spec together.** A fix that changes a public
   contract goes to the user first. Rerun the baseline and every mutant.
5. **Keep the reverted fix as a mutant.** Add `mutants/prefix_<slug>.cfg`,
   which restores the old design and expects the broken property. Show
   that it fails.
6. **Identify the coverage gap.** Check whether the property was missing,
   a fault was out of scope, the bounds were too small to reach the trace,
   or the independent read missed the scenario.
7. **Add the missing property, fault or bound.** Show that it fails on the
   `prefix_<slug>` mutant.
8. **Carry it to implementation.** Turn the trace into a scenario the
   implementation is tested against: a named scenario ID in the plan, and
   a seeded simulation case once the workload exists.
9. **Write the bug record.** One file, `docs/testing/bugs/<date>-<slug>.toml`,
   validated by `scripts/testing/bugs.py`. Set `found_by` to `review`. Set
   `lowest_regression` to the `prefix_<slug>` mutant configuration. Set
   `fix_commits` to the commit that fixed the plan and the spec. A bug that
   has no fix yet, such as one in a shipped design, gets an
   [open record](../../../../docs/testing/bugs/README.md#open-records) with
   `fix_commits = []`, an owner and an expiry date. While it is open, set
   `lowest_regression` to the configuration whose run shows the
   counterexample.

## Review findings

A finding against a design follows the same loop. It stays blocking until
its `prefix_<slug>` mutant fails and the fixed spec passes on the reviewed
SHA.
