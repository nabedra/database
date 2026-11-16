# Shared storage bindings

This records the interfaces connecting request admission, replicated worker
queues, per-request storage grants and conditional catalog publication. See the
[execution ledger](../../docs/testing/cli-serverless-evidence.md) for exercised
assertions and integration limits.

## Concrete interfaces

- `storage::RuntimeStore` is Clone and implements ObjectStore. Variants are Http
  and feature-gated S3. Existing physical WorkerPool and scheduler remain shared.
- `storage::context::RequestContext::decode_header(header: &str, now: u64)` returns
  Result<Self, StorageError>. Now is Unix seconds. It enforces unpadded base64url,
  24 KiB header, 16 KiB decoded strict JSON, UTC expiry, bucket/region/prefix checks.
- RequestContext is Clone, Serialize, Deserialize, with a redacted Debug. Its
  fields are private. `validate(now)` must be called after signed peer decoding.
  Serde decoding alone does not authorize a context. No context goes to disk.
- `storage_key() -> Result<StorageKey, StorageError>`, `bucket()`, `region()`,
  `namespace() -> Result<Namespace, StorageError>` expose target metadata.
- StorageKey is Clone/Eq/Ord/Hash/Serialize/Deserialize with private fields and
  redacted Debug. It contains AWS partition, bucket, normalized trailing-slash
  prefix. Region determines partition; credentials never enter identity.
  `digest()` is SHA256 of `meshdb.storage.v1\0`, then each partition/bucket/prefix
  UTF-8 value followed by NUL. `overlaps(&other)` checks same bucket/partition and
  prefix containment. Reject overlap with the configured control StorageKey.
- `S3Store::from_context(&RequestContext)` constructs an isolated explicit static
  provider from that accepted credential snapshot. It never loads ambient
  providers, environment endpoints or compute-role credentials. No retries.
- `storage::registry::DatabaseRegistry::new(max_entries, max_bytes, control_key)`
  returns a bounded registry. `open(context: RequestContext, now: u64)` returns
  Result<DatabaseHandle, StorageError>. `DatabaseHandle::{storage,key,context}`
  return references to Arc<Storage<RuntimeStore>>, StorageKey, RequestContext.
  Each open creates an isolated grant/client. No shared authorized object/cache
  values are reused between grants. Entries remain reserved while handles exist;
  the registry releases count/byte reservations when the last SDK/store clone and handle drops. The S3Store itself retains the reservation, so extracting/cloning storage cannot release its accounting early. Registry
  exhaustion maps to pre-admission Overloaded. Context expiry/invalid syntax maps
  to pre-admission Unauthorized or ValidationError. Storage failures after
  acceptance require existing ambiguous-write reconciliation, never not_applied.

`context::from_headers(&hyper::HeaderMap, serverless: bool, now: u64)` applies
32 KiB aggregate limits, rejects identity headers, fixed-mode storage context,
missing serverless context and duplicates. Authenticate compute bearer first.

## Binding and accounting requirements

The API-to-worker signed RAM command extends Submit with
`context: Option<RequestContext>`, and Lookup likewise carries fresh context.
Fixed mode requires None; serverless requires Some and a compute bearer. The
request record retains the complete accepted context before all-ready ACK. Keep
original body bytes. Scope all identity maps by `(StorageKey, request_id)` and
hash original body independently of replaceable credentials. Carry storage-key
and accepted grant digest into signed jobs, pins, prepared outputs and decisions.
Do not log commands/records containing credentials. Peer transport already signs
and encrypts its complete body; context must be inside that signed body.

Receipt credentials authorize backing objects again. Never replace credentials
in an accepted record with fresh polling credentials. An accepted snapshot can
expire, causing work to fail or reconcile. Idle handle recreation keeps run/term.

Registry count and conservative byte reservations bound client/context creation;
binder must also charge these bytes against the ONE physical worker scheduler.
Request records charge serialized context plus metadata under replica bytes.
No pending context eviction; use handle lifetime to hold registry reservations.
Per-grant pending limits and global queue limits must both be checked before ACK.
These registry primitives alone do not establish full process RSS accounting.

## Configuration

Strict v2 adds top-level `mode: fixed|serverless`; v1 accepts fixed only.
Template carries mode, omitted when fixed to preserve v1 fingerprints. Worker
bootstrap v2 carries mode; enrollment compares mode through semantic fingerprint.
S3 storage options are bucket/namespace plus optional region. Omitted legacy
region resolves the standard region provider only for operator control storage.
Explicit region is honored. Customer context always requires explicit region.
Fixed mode rejects storage headers. Serverless admission requires the complete
validated context and its database-scoped queue/job bindings.

HTTP fixtures establish no real AWS conditional-write, IAM or region evidence.

## Local test injection

`#[cfg(test)] DatabaseRegistry::new_fixture(max_entries, max_bytes, control_key,
backends: BTreeMap<StorageKey, HttpStore>, authorize:
Arc<dyn Fn(&RequestContext) -> bool + Send + Sync>)` accepts only an in-process
trusted map, without any production endpoint/configuration toggle. Every open
validates context/expiry/overlap, reserves capacity, and invokes the checker
again. Denial returns StorageError::Unavailable and drops that new reservation.
The test-only RuntimeStore::Fixture retains the same reservation in every clone.
This supports local shared-execution assertions, not IAM/S3 acceptance.

## Queue and execution bindings

- `request_queue::RequestKey { storage: Option<StorageKey>, request_id: Uuid }`
  is Clone/Ord/serde. `From<Uuid>` selects fixed scope. Queue APIs accept
  `impl Into<RequestKey>` for identity, keeping run IDs as UUIDs. Public bodies
  and receipt IDs are unchanged.
- `RequestRecord.context: Option<RequestContext>` is serialized in RAM replicas.
  `identity()` returns RequestKey. `context_bytes()` and `binding_digests()` are
  crate-visible. `binding_digests()` returns storage and grant digest pairs as
  `Option<[u8;32]>`; storage digest is SHA256 of StorageKey.digest() ASCII bytes,
  grant digest is SHA256 of serde JSON for the complete accepted context.
  `digest` remains SHA256 of original body bytes only.
- `RequestQueue::propose_with_context(body, context, now_ms)` validates expiry
  before admission. Identical body replay at the same storage key returns the
  first record/context; rotation never renews accepted snapshots. Installation
  validates context syntax with time zero to preserve accepted expired records.
  Context bytes count against replica memory. Under the configured authenticated
  data bearer, each stable StorageKey is limited to ceil(global pending limit / 4).
  Credential/expiry rotation does not create a new quota scope. This cap is
  separate from the immutable accepted-grant digest used for signed jobs and
  from finer user quotas enforced by gateway middleware. Global count/byte limits
  remain shared.
- Signed queue Submit and Lookup now carry `context: Option<RequestContext>`.
  Fixed mode requires None. Peer decoding validates mode, context and control
  overlap. API and peer paths share strict mode-dependent validation. API
  ingress bounds total headers, rejects duplicate storage/foreign identity
  headers, and uses the configured compute data bearer.
- `job::QueueBinding` remains Copy and adds optional `storage_digest` and
  `grant_digest` `[u8;32]`, defaulting to None for fixed compatibility.
  `validate_queue_binding` compares both digests with the accepted record.
  Job lookup must select composite identity using those digests, never UUID only.
- `Storage::initialize_owned_cluster_catalog(cluster_id, fence,
  migration_allowed, cold)` creates format5 roots with `owner_cluster`, run and
  term in the same CAS. Old roots require explicit migration. Foreign cluster
  ownership always conflicts. Recreation with the same fence does no PUT.
  Takeover in the same run requires a larger term; another run requires cold.
  Publication preserves owner metadata. `prepare_fenced` requires fixed v4;
  `prepare_owned(base,tables,decision,cluster_id,fence)` requires owned v5 and
  binds the expected cluster to the same captured root/ETag.

Worker has one bounded DatabaseRegistry charged through the existing physical
scheduler. QueueRuntime retains DataGrant values by RequestKey through terminal
settlement; active/retry/first_seen maps use the same composite identity. Operator
`Worker.storage` continues serving membership/control. Coordinator receives an
explicit RequestKey and data grant. Signed jobs resolve the accepted record by
both digests before execution. `queue_data_for_record` checks accepted expiry even
for retained clients; receipt credentials never enter this map. Receipt lookup
opens its fresh grant, checks cluster owner and authorizes every current and pinned
backing object before consulting cached state/history. Accepted mutation errors
during reconciliation remain outcome_unknown when their credentials expire.
The concurrent Worker pipeline fixture exercises these bindings under the same
physical resource scheduler. Its results and live AWS boundaries are recorded
in the execution ledger.

The local queue->dispatch request carries RequestKey in request extensions,
which external HTTP cannot supply. Coordinator execution takes this key and a
retained DataGrant. `QueueRuntime::record(key)` and `record_for_job(job)` expose
validated immutable snapshots. `queue_command` and `run_queued` are pub(super)
for the internal pipeline fixture; cfg(test) `install_fixture_queue` seeds one
bounded queue without a production override. No caller-chosen storage endpoint
was introduced. Real AWS IAM/conditional-write acceptance remains separate.
