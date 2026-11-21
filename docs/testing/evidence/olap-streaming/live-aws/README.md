# LIVE2 OLAP and ring TLS on S3

The native OLAP library from `c42e1081f7d598d4074f613c85c12cd0b9170f91`
used Rust 1.94.1 on macOS x86-64 and a private, unversioned S3 bucket in
`us-east-1`. The resolved OLAP dependency graph contained ring and omitted
`aws-lc-rs` and `aws-lc-sys`.

The explicit caller context sent a signed GET to the AWS endpoint and
received `NoSuchKey` for the fresh catalog key. It initialized a fenced
catalog, read back the owner and fence, advanced the fence by conditional PUT,
then tried to advance again using the original ETag. The stale attempt returned
`StorageError::Conflict`. A final GET confirmed the winning fence and original
owner. The runner tagged and deleted the one 249-byte catalog object.
[Sanitized run output](catalog-and-tls.txt)

This is evidence for the real-S3 application TLS smoke from BLD5 and the stale
catalog CAS assertion in T05. The full T05 cold-start and in-flight mutation
sequence and a real-S3 streaming process run remain open.

| Check | Status | Reason |
| --- | --- | --- |
| OLAP provider growth, provisioning failure and stream drain (A02/ST05) | Not run | EC2 discovery returned `UnauthorizedOperation` and CloudFormation discovery returned `AccessDenied`. The existing callback acceptance uses local processes. |
| GPU performance (ST06, B01/B02) | Not run | The supplied identity could not verify EC2 inventory or a clean teardown. The approved GPU spot lane depends on those checks. |

The build and test selectors were:

```sh
CARGO_BUILD_JOBS=2 cargo test --locked --manifest-path server/Cargo.toml --lib --no-run
"$OLAP_TEST_BINARY" --exact storage::s3::live_context_probe::explicit_context_first_use --ignored --nocapture
```

The test executable received only the selected AWS credentials and an explicit
fresh fixture prefix in its environment. An external teardown removed only
objects under that prefix.
