# DN-A11 configuration loading and semantic attachment

Design handoff only, 2026-11-11. No implementation or tests were run. [Shared
contracts](../../plans/distributed-contracts.md) are authoritative; use accepted
[A05](DN-A05.md), [A07](DN-A07.md), and only [A10's](DN-A10.md) startup obligations.
[A03](../../scripts/notes/DN-A03.md) supplies historical bootstrap shapes, with
one `storage` backend and autoscale minimum 1 replacing its older proposals.
[A04](../../orm/notes/DN-A04.md) and the current `_serve_args.py` /
`_serve_process.py` supply private startup records. This note designs bounded
Rust loading and comparison, without listeners, IPC implementation, worker
execution, controller loops, a new public API, or a generic config framework.

Parent adoption after independent re-check: accepted for implementation,
including Config generation 2 and required worker_semantic_fingerprint,
generation-1 refusal without migration, and the cached Unix libc prerequisite.
The shared contract and A05 schema now record this amendment. C08a/b/c below
remain unimplemented; adoption itself establishes no runtime or test result.

## 1. Exact local inputs

The API command remains `meshdb-server api --config PATH`. Its JSON object is:

```text
ApiConfig = {
 version:1, cluster_id:UUID, bind:host_port, advertise_url:URL,
 api_urls:[URL], storage:Storage, auth_file:path, tls:Tls|null,
 worker:{listen:host_port, operation_slots:u64, partition_slots:u64,
         queue_size:u64, partition_size:string},
 lifecycle?:{Timing field:u64, ...}
}
Storage = {driver:"http", options:{base_url:URL, namespace:string,
                                  token_file:path, ca_file:path|null}}
        | {driver:"s3", options:{bucket:string, namespace:string}}
Tls = {cert_file:path, key_file:path, ca_file:path}
Auth = {admin_token:string, data_token:string, enrollment_token:string}
```

Every API field except `lifecycle` is required, including `tls:null`. The five
worker fields are required; deployment authors can write the recommended values
`"0.0.0.0:8001"`, 4, 4, 64, `"64 MiB"`. These are not omission defaults.
An absent lifecycle or `{}` uses `Timing::default()`. A present lifecycle is an
object containing any subset of the eleven exact `Timing` fields. Merge each
provided positive integer onto that default, then call `Timing::validate()`.
Explicit null is invalid here. No other omission, alias, environment fallback,
inline credential, historical `control`/`objects`, or AWS field is accepted.
Unknown storage drivers and S3 without its feature fail explicitly.

The worker file command remains `meshdb-server worker --bootstrap PATH`, directly
forwarded by the Python CLI. The exact record is:

```text
Bootstrap = {
 version:1, cluster_id:UUID, node_id:UUID, request_id:UUID|null,
 api_urls:[URL], listen:host_port, advertise_url:URL,
 storage:Storage, enrollment_token:string, tls:Tls|null,
 operation_slots:u64, partition_slots:u64, queue_size:u64, partition_size:string
}
```

All fields are required. Managed callback bootstrap has the same shape with
`advertise_url:null` and a non-null persisted launch request ID. The adapter fills
the target URL and replaces credential/TLS path hints with target-local paths.
It preserves identities, backend address/namespace, API URLs and capacities.
Manual external files require `request_id:null`. The worker file loader rejects
`advertise_url:null`; the callback builder alone permits it. Managed eligibility
is established later against persisted launch records, not by JSON parsing.
Bootstrap has no `auth_file`, API bind, lifecycle, admin/data secret or bridge
token. Enrollment token is the only deployment-auth secret delivered to workers.

Use typed strict object deserialization through `protocol::deserialize_object`,
including every nested option record. Do not first parse into a JSON Value map,
which loses duplicates. Required nullable fields use explicit deserializers as
in `configuration.rs`. Reject duplicates, unknown fields, trailing JSON, bad
UTF-8, wrong shapes, booleans as numbers, negative/fractional/exponent integer
tokens, overflow and versions other than the integer token `1`. Reuse the
existing depth precheck before deserialization, with maximum depth 32 including
RawValue options. API and bootstrap input caps are each 128 KiB including
whitespace; Auth is 16 KiB. Read cap+1 and reject excess before parsing.

Construct and validate the existing `Template` from API `api_urls`, `worker`,
`storage`, and `tls`, and from the corresponding bootstrap fields. Reuse its
64 KiB bound, 1..16 API URLs, listen syntax, capacity ranges 1..256 / 0..4096,
namespace/storage guards and `PartitionSize` parser with its 64 MiB maximum.
Reuse member canonical UUID/endpoint checks and the existing name rule for IPC
init; expose narrow crate-private helpers where needed rather than copying
validators. API URLs and advertised worker/API URLs have no path except optional
`/`, no userinfo/query/fragment, and at most 2048 ASCII bytes. Bind uses the same
host:port syntax as listen, at most 256 bytes. Port zero is allowed only on a
bind/listen; advertised ports must be nonzero. TLS mode must agree with the
advertised URL and all API URLs: `tls:null` means HTTP, a TLS object means HTTPS.
Storage HTTPS has its independent CA setting. All paths are nonempty, at most
2048 UTF-8 bytes and contain no NUL. Do not resolve hosts during validation.

Python continues to validate only serve arguments/callback declarations and the
literal readable config path. `_resolve_config_path()` already makes that path
absolute relative to the caller's current directory. Rust owns all JSON and
material validation; there is no second Python parser.

## 2. Local material and backend identities

Resolve paths inside each input relative to the directory of its absolute CLI
file path, without resolving that file's symlink to change the base. A bootstrap
uses its own directory on its target host. Absolute paths stay absolute. No
tilde, environment, shell or dotenv expansion occurs, and auth has no nested
paths. Keep original Template path strings unchanged as outgoing hints; use a
separate private resolved-path view for API-local opens. Callback materialization
must make its target paths meaningful relative to the target bootstrap directory.

Load each local file once and retain the validated bytes/parsed material for
client/listener construction. Do not hash one read and reopen for use. Check for
regular files before opening and on the opened handle, refuse devices/FIFOs, and
use a nonblocking regular-file open on supported POSIX systems so a type-swap
cannot block on a FIFO. On Unix use OpenOptionsExt::custom_flags(libc::O_NONBLOCK)
and validate the opened handle before reading; ordinary File::open is not a
substitute for this type-swap protection. Check opened-handle permissions. Auth, HTTP token,
bootstrap containing enrollment, and private key files must have no group/other
permission bits on POSIX; require read access, not writability or literal 0600.
Config/cert/CA files need not be secret or owner-only. Protected readable mounted
files and symlinks to regular files are allowed; validate the opened target.
No chmod, approval step, or assumption that a path protects against a compromised
same-user process is added.

Caps are 16 KiB for auth, 4 KiB for HTTP token, 256 KiB for a private key, and
1 MiB each for a certificate chain or CA bundle. Allow at most 64 certificates
per certificate chain and 1024 per CA bundle, including system bundles. Each
DER certificate is at most 64 KiB, with 8 MiB aggregate local
material read per startup. Apply caps while reading, even if stat reports a
smaller size. The existing startup absolute deadline also bounds progress checks;
these byte limits do not promise preemption of an uninterruptible filesystem.
Missing, empty, oversized, malformed or truncated PEM and key/leaf mismatch fail
before preparation. Use existing rustls/provider parsing and cert/key validation,
with loaded bytes rather than legacy `from_pem_file` reopens. Legacy `Config`
and standalone `serve` behavior stay separate; do not call its S3/environment
loader from distributed mode. This design needs no new TLS dependency/API.

Auth has exactly three distinct strings, each 43..4096 printable ASCII bytes
33..126. Preserve exact bytes; do not trim. Operators must generate each
independently with at least 256 bits of random entropy, for example 32 random
bytes encoded as unpadded base64url. Length, alphabet and inequality checks
cannot establish entropy; repeated/predictable strings can pass syntax. HTTP
token files contain one nonempty printable ASCII token, optionally followed by
one LF, within the file cap. Remove only that LF. No whitespace trimming or
multiline tokens. Its identity binds the exact token used by `HttpStore::new`.

A04 callback/authorization tokens remain independent random per-child lifetime
values received through private init. They are not persisted Auth fields, do
not enter configuration fingerprints, and confer no deployment authority.
No raw material, private key, token, trust bytes, digests or paths enter Debug,
status or SDK diagnostics. Durable Template holds path hints, never credential
or trust bytes; digests below are private control metadata. Errors expose fixed categories with
static text, without serde/PEM/OS/provider sources or offending values. Map local
input/material failures to A04 `invalid_config`, semantic mismatch to
`incompatible_cluster`, and backend access failure to `storage_unavailable`.

Canonical backend identity follows the adapters, not a generic URL normalizer:

* HTTP address is exactly the accepted `base_url.trim_end_matches('/')` used by
  `HttpStore`, plus `Namespace::new(namespace).as_str()` including its one final
  slash. Preserve scheme/authority spelling, explicit port, host spelling and
  accepted path bytes. Do not equate DNS names, IPs, default ports, case changes,
  escaped paths or other URL rewrites. Both constructor and template syntax must
  accept the input. Backend identity includes HTTP token identity and effective
  CA identity so different credentials/trust reject attachment too.
* For HTTPS, supplied CA PEM parses to a nonempty rustls root set. If `ca_file`
  is null, select the first readable regular system bundle in HttpStore's current
  order: `/etc/ssl/cert.pem`, `/etc/ssl/certs/ca-certificates.crt`, then
  `/etc/pki/tls/certs/ca-bundle.crt`. Bound and parse it once, fingerprint the
  loaded roots, and pass those same bytes as `Some` to `HttpStore::new` to avoid
  its currently unbounded fallback read. A supplied bundle is validated and
  bound even for HTTP; HTTP with null CA has null trust identity. Matching
  explicit/system loaded roots are compatible. Failure of the selected readable
  bundle is an error, not silent fallback to a different trust policy.
* S3 remains feature-gated with exactly bucket/namespace options and the standard
  credential/region chain. Identity binds bucket verbatim, normalized namespace,
  resolved region and the standard AWS S3 service endpoint policy. Do not hash
  access/session credentials: ordinary provider-chain refresh and different
  authorized principals are compatible. No generic AWS options are introduced.
  A narrow adapter connection result must expose the effective region and
  confirm standard AWS endpoint policy without exposing credentials. Reject
  custom endpoint overrides in distributed mode; bucket/namespace alone cannot
  distinguish two S3-compatible services. Legacy S3 setup stays unchanged.
  The adapter owns provider resolution/timeouts, not the pure JSON parser.

## 3. Exact digest inputs and the persistence gap

Use existing `sha2`. Let `J` be compact UTF-8 JSON with recursively lexical object
keys, required nulls and decimal integers, using the canonical bounded serializer
in `configuration.rs`, extended narrowly for the fixed identity records below.
Do not introduce another serializer. Let `H(label,x)` mean lowercase hex
SHA256 of ASCII label, one NUL byte, then `J(x)`. Every label below ends in `.v1`;
the NUL is appended once. Strings in these inputs are exact unless a normalization
is explicitly specified here. Arrays retain order except root sets.

```text
secret(role,bytes) = H("meshdb.config-secret.v1",
                       {role:role, sha256:hex(SHA256(bytes))})
roots = H("meshdb.config-roots.v1", {certificates:[hex(DER), ...]})
chain = H("meshdb.config-chain.v1", {certificates:[hex(DER), ...]})

backend_http = {driver:"http", base_url:canonical_base, namespace:canonical_ns,
                token:secret("storage.http",effective_token), ca:roots|null}
backend_s3 = {driver:"s3", bucket:bucket, namespace:canonical_ns,
              region:resolved_region, endpoint_policy:"aws-s3-standard.v1"}
storage_fingerprint = H("meshdb.control-storage.v1", backend_http|backend_s3)
auth_fingerprint = H("meshdb.control-auth.v1",
  {cluster_id:canonical_UUID, admin:secret("admin",admin_token),
   data:secret("data",data_token), enrollment:secret("enrollment",enrollment_token)})
lifecycle_fingerprint = H("meshdb.control-lifecycle.v1", expanded_Timing)
worker_semantic_fingerprint = H("meshdb.control-worker-semantic.v1",
  {api_urls:template.api_urls, listen:template.listen,
   operation_slots:template.operation_slots, partition_slots:template.partition_slots,
   queue_size:template.queue_size, partition_bytes:parsed_PartitionSize.bytes(),
   storage_fingerprint:storage_fingerprint,
   tls:null|{certificate_chain:chain, ca:roots}})
```

For roots, sort unique DER certificate byte strings lexicographically before
encoding. CA PEM whitespace/order/duplicates do not change identity. Chain
retains certificate order, including leaf. All digest encoding is bounded while
writing, at most 16 MiB for DER hex input; do not build an unbounded JSON value.
Keys never enter a digest: successful key/leaf matching establishes the public
certificate identity and permits different equivalent private-key encodings.
Certificate renewal or trust-set change is incompatible in this version.

`template_fingerprint` remains exactly `Template::fingerprint()` with domain
`meshdb.control-template.v1\0`, hashing the complete original outgoing Template,
including every path. Do not replace it with the semantic fingerprint or normalize
the persisted template. API bind/advertise address, auth_file path and bridge
availability are local settings, excluded from worker identity. Worker listen,
ordered API failover URLs and capacities are semantic settings, not local paths.
Equivalent partition-size spelling is compatible by parsed byte count.

The single top-level TLS record supplies both API-local server material and
outgoing worker path hints. With this accepted shape, replicas may relocate the
same cert/key/CA; they cannot supply different leaf certificates and claim this
is only an API-local change, since that also changes worker template identity.
The callback may rewrite paths, not certificate/trust identity. This entails a
certificate with appropriate SANs for the deployed endpoints. A separate
API/worker TLS identity or per-node certificate issuance needs a later explicit
contract; this handoff adds no PKI or new enrollment secret. Verified TLS still
checks chains and DNS/IP SAN at connection time. HTTP still has A07's trusted
network/member/backend limits, not compromised-member isolation.

One genuine parent reconciliation is required. Current `state::Config` has
only storage/auth/template/lifecycle fingerprints and adapter_contract. After
the creator's paths become inaccessible, none records the worker cert/trust
identity separately from path strings. The exact template digest cannot recover
it. Comparing the new replica to itself or opening the old paths is unsound.
Overloading auth/storage/lifecycle digests with unrelated worker settings hides
this gap and prevents their specified independent comparisons.

Accepted explicit change: require one additional Digest field
`config.worker_semantic_fingerprint`, use `config.generation:2`, and keep root
`version:1`, Template, Timing and every other field unchanged. The parent adopted
this private record amendment in the shared contract and A05 after re-check;
assignment 3 owns its implementation. Update Config's exact fields/generation and
digest validation only. Generation-1 roots cannot be safely upgraded from their
path hints; startup must fail incompatible without writes, never reopen their
foreign paths or synthesize missing identity. No online migration is designed.
Coordinate the strict generation-2 codec and its existing synthetic root fixtures
with the state owner. At startup, map unsupported/invalid control configuration
from root decoding to sanitized `incompatible_cluster`; do not depend on
successful generation-1 decoding to refuse it. A mixed old/new binary rollout
is unsupported.

## 4. Startup comparison and bounded assignments

Private proposed boundaries are `decode_api(bytes)`, `decode_bootstrap(bytes)`,
`load_api_material(config, config_directory, deadline) -> LoadedApiConfig`,
and `check_attach(loaded_control, loaded_api_config, expected_name,
expected_provisioning) -> AttachedConfig`. Loaded material types have redacted
Debug and no general Serialize. `AttachedConfig` retains the unchanged validated
LoadedControl and local material; its constructor checks compatibility and grants
no controller authority. Do not serialize functions or use callback identity as
a fingerprint; the persisted adapter contract remains `meshdb.callbacks.v1`.

Startup builds its validated initial candidate from expanded defaults and loaded
fingerprints, then calls C04b1 `initialize_control(initial, deadline)`. Whether
that returns its candidate, an existing root, or a Create-race winner, perform
the same check before use: cluster ID, name, adopted generation, provisioning,
adapter contract, storage/auth/lifecycle fingerprints and worker semantic
fingerprint. Validate the stored exact Template fingerprint against its stored
Template only: do not compare it to the new replica's raw local-path template
digest. Recheck the Timing digest against its stored record. Recompute stored
worker non-material settings against *local loaded
identities* and compare the durable semantic digest too; this catches a stored
template/settings mismatch without opening any stored path. Compare the local
semantic digest independently. Storage address/namespace checks use the same
adapter normalization for stored strings and local config. No outgoing file
opens are part of this deterministic comparison.

On success reuse the persisted Template verbatim for every callback and insert
only the locally loaded enrollment token into the in-memory bootstrap. Preserve
policy/revision, history, scheduler/backoff/timers, nodes and intents. Ignore the
new serve initial `nodes` on attachment. A mismatch causes no root write or
ownership attempt. Creation races do not justify a second Create or resetting
anything. Existing C04b1's observed root is validated storage evidence only.

A missing bridge does not reinterpret a managed root as external or reset its
adapter binding. A correctly declared managed replica without an active bridge
is ineligible for ownership and reports degraded availability through later
runtime wiring. Current Python init declares absent callbacks as external, so
attaching that init to a managed root is a provisioning mismatch and fails;
do not infer managed provisioning from the root to bypass this check. External
mode keeps null adapter contract, rejects autoscale, and never grants provider
start/stop/autoscale capability. A10 ownership checks remain separate. Complete
validation/attachment before `prepared`; do not acquire authority or dispatch
until A04 activation. No listeners/IPC/controller implementation belongs here.

Issue these three small assignments in dependency order. The parent serializes
Cargo ownership and file ownership; no assignment changes a concurrently owned
framed transport. Before assignment 2, the Cargo owner adds the already locked
and cached libc = "=0.2.191" as a direct Unix dependency, independent of the S3
feature. Authorize only that narrow Cargo.toml/Cargo.lock delta; no dependency
fetch is needed. This design-only note itself has no Cargo ownership.
The proposed APIs are crate-private. Their tests live in private owning-module
test files; do not widen visibility or add a public facade just for integration
tests. Narrow test-support imports may reuse existing fixtures.

1. **C08a: Parse fixed input records.** New `server/src/cluster/startup_config.rs`,
   `server/src/cluster/startup_config/tests.rs`; narrow crate-private validator/serializer
   reuse in `configuration.rs` / `member.rs` and export in `cluster/mod.rs`.
   Implement the two decoders and expanded Timing only. Use a few representative
   duplicate/unknown/malformed/required-null/range/depth/byte-limit cases, including
   exponent integers and external request null. Assert errors contain no supplied
   secret/path. No I/O, CLI or Python changes.
2. **C08b: Load local material and compute identities.** New
   `server/src/cluster/startup_material.rs`, `server/src/cluster/startup_material/tests.rs`;
   reuse the fixed-record canonical writer. Add only the narrow feature-gated
   S3 effective-region/standard-endpoint connection handoff in `storage/s3.rs`
   with its owner. HTTP uses explicit loaded CA bytes with its existing constructor.
   Cover synthetic protected temporary auth/token files, equivalent PEM and
   key encodings, cert/key mismatch, and missing/truncated/oversized TLS files
   with real TLS fixtures. No deployment files, dependency fetch/addition,
   cloud requests, custom certificate generation framework or runtime wiring.
3. **C08c: Implement the adopted schema and check attachment.** New
   `server/src/cluster/attachment.rs`, `server/src/cluster/attachment/tests.rs`; coordinate
   only the exact Config amendment in `state.rs` and affected synthetic fixtures
   with its owner. Reuse C04b1 LoadedControl and store fakes without changing
   initialization semantics. Prove different local paths/same loaded identities
   attach and use the first persisted Template; changing secret, CA, cert,
   backend or worker settings rejects before ownership. Seed noninitial policy,
   admin head/sequence, timers and intents and assert they remain unchanged.
   Put deliberately nonexistent API-foreign paths in the persisted template and
   record local file opens to prove no attempt to read them. Include a competitor
   root and provisioning mismatch plus generation-1 refusal. This is comparison
   evidence, not a controller/runtime test.

The parent startup owner later wires these results into main/runtime and the
existing IPC handshake. Existing real-process E03/E04 startup acceptance remains
the final integration evidence. No repeated full codec matrix, benchmark, new
acceptance runner, implementation claim or test result is established by this note.

The first independent design review found that a 64-certificate CA cap rejected
the inspected default system bundle, and that nonblocking file opens needed a
direct platform-flags dependency. The parent separated chain/CA count limits
and assigned the narrow cached Unix libc dependency prerequisite above. The
fresh design re-check passed without essential remaining findings. The parent
then explicitly adopted the generation-2 schema and prerequisite above. The
Cargo prerequisite remains pending until assigned before C08b.
