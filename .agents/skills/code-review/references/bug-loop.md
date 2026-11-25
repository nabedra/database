# Bug loop

Use this procedure for bugs found in review, test runs or user reports. The
fix includes a check that catches the bug.

## Steps

1. **Classify.** Name the class, the module and the guarantees the bug breaks.

   | Class | Typical invariant |
   | --- | --- |
   | Uncertain outcome reported as not applied | a write reported as `not_applied` is never observed later, and every `not_applied` carries proof from the deciding authority |
   | Stale route, topology or schema | each commit uses the epoch in effect at its timestamp, and a retired leader never answers success |
   | Fence and lifecycle ordering | lifecycles move only along declared edges, and a fence takes effect only after validation |
   | Membership, replacement and trust | Raft safety across joint changes, and a wiped node never votes under its old identity |
   | Resources released in the wrong order | every acquire has exactly one release, after the work it guards, and counters return to baseline |
   | Other | name the invariant in the record |

2. **Reproduce at the lowest level.** Write a regression at the lowest level
   that reproduces the bug. Use a seed if possible so others can reproduce
   the same failure. See [simulation](simulation.md).
3. **Confirm it fails.** Run the regression on the unfixed code and keep the failing output.
4. **Fix it.** Then run the regression and the checks `matrix.py select` picks for the diff. A new
   test also passes the repetition gate.
5. **Identify the coverage gap.** Check whether the invariant was missing,
   the generator could reach the case, or the required check was skipped,
   quarantined or marked `n/a` in the register.
6. **Add the invariant.** Put it where every later run checks it: the model's `check`, the
   simulator's invariants, the history checker, or a property. Show that it fails on the reverted
   fix.
7. **Write the bug record.** One file, `docs/testing/bugs/<date>-<slug>.toml`. The validator,
   `scripts/testing/bugs.py`, defines the exact keys. The record holds:
   - the class, module, guarantees and scenarios;
   - how the bug was found, and by which check or reviewer;
   - the lowest regression, and its seed when there is one;
   - why no check caught it;
   - the invariant added;
   - the fix commit, or, for a bug that has no fix yet, the owner and expiry of an
     [open record](../../../../docs/testing/bugs/README.md#open-records).

## Review findings

A review finding follows the same loop. The finding names the check that should have caught it,
and the fix adds that check or its invariant. A finding stays blocking until its regression fails
on the old code and passes on the fix, on the reviewed SHA.
