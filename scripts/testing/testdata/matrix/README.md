# Matrix test fixtures

Used by `scripts/testing/test_matrix.py` and `test_release_gate.py`. Record files use placeholder
shas: `@HEAD`, `@HEAD~2` and `@OTHER` (a commit that is not an ancestor of HEAD). The tests build a
temporary git repository and substitute real shas before loading the records.

| Cell | Expected state |
| --- | --- |
| EVT-G01 U | pass |
| KV-G01 U | pass |
| KV-G01 S | stale 2 (pass) |
| KV-G01 R | n/a, cloud-deferred |
| KV-G02 U | fail |
| KV-G02 D | not-run: s3-unconfigured |
| KV-G03 U | quarantined 3/5 |
| KV-G03 H | missing (its only record is on a commit that is not an ancestor) |
| KV-G03 M | n/a (not required) |
