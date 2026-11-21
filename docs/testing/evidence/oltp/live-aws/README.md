# LIVE2 OLTP on AWS

## O18 and O27 provider preflight

| Check | Status | Missing evidence |
| --- | --- | --- |
| O18 EC2/EBS and EKS, A01/A02/R03/R06 | Not run | Same-AZ encrypted volume binding, verified one-writer fencing, retained PVC and data, authenticated replacement identity, native sync, quorum-aware drain and AZ loss. |
| O27 EC2/EBS growth and EKS PVC replacement, A01/A02/R03 | Not run | Real gp3/PVC expansion, a second mount, host limits, readiness-gated growth, provider failure and reply loss while queue copies and shard quorums survive. |

The supplied credentials received `UnauthorizedOperation` from EC2,
`AccessDeniedException` from EKS, and `AccessDenied` from CloudFormation.
LIVE2 made no infrastructure mutations. The acceptance runner accepts a
provider fixture path and lists both scenarios. This revision has no tests
registered as `acceptance_o18` or `acceptance_o27`. The component tests cover
modeled provider events. [Provider preflight output](../../cache/live-aws/preflight.txt)

## O19 S3 backup transport

The focused archive probe exercises the production S3 repository, encrypted
archive chunks and root publication with a fixture history. It runs on macOS
x86-64 with Rust 1.94.1 and the `oltp-backup-s3` feature. The exact probe is
`oltp_backup_repository::tests::o19_live_s3_archive_reopens_and_reconciles_exact_retries`.
The probe scope covers archive transport. The full O19 transaction-cut,
cluster-loss and point-in-time restore assertions are a separate fixture.

At `c42e1081f7d598d4074f613c85c12cd0b9170f91`, the probe passed
against real S3 in `us-east-1`. The first uploaded chunk left the archive
root absent. Publishing two chunks produced the expected size and digest. An
identical retry returned the same root digest, and an attempted replacement
with different bytes returned `Conflict`. A raw chunk carried the encrypted
archive envelope. A fresh connection reopened the archive, returned both
original chunks in order and verified the complete digest and root.

```sh
CARGO_BUILD_JOBS=2 cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp-backup-s3 --lib --no-run
"$OLTP_TEST_BINARY" --exact oltp_backup_repository::tests::o19_live_s3_archive_reopens_and_reconciles_exact_retries --ignored --nocapture
```

[Sanitized archive run output](archive.txt) · [Linux CI test lines](ci-test-results.txt)

Final cleanup verified zero S3 objects, versions, delete markers and multipart
uploads. The sweep also found zero tagged security groups and zero matches in
the Resource Groups Tagging API. The other infrastructure queries returned
permission errors. [Final sweep output](final-sweep.txt)
