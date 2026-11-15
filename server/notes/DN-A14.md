# DN-A14 effective identities and retained backend composition

Design handoff only, 2026-11-11. Bounds the scope of accepted [A11](DN-A11.md)
and rubric C08b2 in [distributed-nodes](../../plans/distributed-nodes.md), under
[distributed-contracts](../../plans/distributed-contracts.md). It adds no
architecture decision or public configuration fields. C08a and C08b1 are reviewed
prerequisites. Read their notes, including C08b1's corrected DER and fixture-child
evidence. Their passing results are reported evidence, not checks run here.

Adopted by the parent after independent design review and a passing re-check of
the endpoint-decision binding correction. The three implementation assignments
and optional dependency changes remain unimplemented by this note.

No applicable AGENTS.md was found in the ancestor chain or destination directories.
Only this note changed. No delegates, Cargo, builds, tests, network calls, host
AWS profiles, environment values, .env files or real secrets were used. Installed
dependency source was read locally. C04c1 retains exclusive Cargo ownership.

## Existing boundaries

`startup_config::{ApiConfig,Bootstrap}` already retain validated local Template
hints. `startup_material::files::Reader` binds the absolute original input path,
deadline, 8 MiB aggregate and 16 cached literal resolved paths. Its Auth,
HttpToken, CaMaterial and TlsMaterial retain exact secrets, original bytes,
sorted unique roots, ordered certificates and a validated CertifiedKey. Compose
these records; do not duplicate parsing, expand paths or reopen files. In
particular, no helper here accepts a persisted foreign Template as file hints.

`configuration::bounded` is the existing bounded canonical JSON writer. Its
canonical ordering comes from lexical struct field declarations and explicit
Storage serialization, not a general sorting serializer. `HttpStore::new` already
accepts explicit CA PEM and constructs locally without a request. Its base is
`base_url.trim_end_matches('/')`; Namespace::new supplies one trailing slash.
Both Template and adapter validation must succeed. Never use Uri reserialization
as the identity. The storage module is `src/storage.rs`, not storage/mod.rs.

## 1. C08b2a, pure fixed identities

Own new `src/cluster/startup_material/identity.rs` and `identity/tests.rs`, its
declaration in startup_material.rs, and only a crate-private entry into the
existing writer in configuration.rs. Keep its implementation shared with existing
Template/Timing encoders. No serializer framework, Value tree or public facade.
Use private fixed Serialize records whose fields are recursively lexical. Keep
the general bounded writer private if a fixed identity helper can live beside it.

Use exactly A11's `H(label,x) = hex(SHA256(label || NUL || J(x)))`, with one NUL,
lowercase hex, required nulls and decimal integers. Fix the following inputs:

* `meshdb.config-secret.v1`: `{role,sha256}`, where sha256 hashes exact effective
  bytes. Roles are `admin`, `data`, `enrollment`, and `storage.http`.
* `meshdb.config-roots.v1`: `{certificates:[hex(DER),...]}` sorted unique by DER
  bytes. `meshdb.config-chain.v1` uses the same record with original chain order.
* `meshdb.control-storage.v1`: HTTP `{base_url,ca,driver,namespace,token}` or S3
  `{bucket,driver,endpoint_policy,namespace,region}`. CA is roots digest or null;
  token is the storage.http secret digest. S3 policy is `aws-s3-standard.v1`.
* `meshdb.control-auth.v1`: `{admin,cluster_id,data,enrollment}`, canonical UUID
  and the corresponding secret digests.
* `meshdb.control-lifecycle.v1`: the complete expanded Timing record.
* `meshdb.control-worker-semantic.v1`:
  `{api_urls,listen,operation_slots,partition_bytes,partition_slots,queue_size,
  storage_fingerprint,tls}`. TLS is null or `{ca,certificate_chain}` with the
  roots/chain digests. PartitionSize supplies bytes; API URL order is preserved.

Use a 16 MiB cap on each complete identity JSON encoding, checked while writing.
Serialize certificate hex incrementally or one bounded certificate at a time;
do not first allocate a complete DER-hex JSON tree. The cap includes JSON syntax.
Keys and all file paths are excluded. The unchanged exact Template fingerprint
continues to include original hints under `meshdb.control-template.v1`.

Expose only crate-private concrete inputs/results needed by composition and
C08c. Bootstrap computes storage/worker identity and may compute its enrollment
secret identity; it cannot invent API auth/lifecycle identities without those
records. No input/material record gains Serialize. Keep digests and material out
of Debug, Display and error sources.

Private tests must pin literal canonical bytes and independently fixed digest
vectors for every label, including nulls, lexical nested fields and one NUL.
Exercise writer exact-cap/max+1 behavior through the actual bounded writer.
Reuse existing temporary TLS fixtures through the narrowest cfg(test) helper
visibility change if required. Compare loaded identities, not just input strings:
CA permutation/duplicates/PEM whitespace, equivalent key encoding, relocated
paths, namespace trailing slash and equal partition byte counts are equivalent.
Secrets, roots, leaf/ordered chain, accepted URL spelling/explicit ports,
ordered API URLs, listen and capacities change the appropriate identity. API
bind/advertise and local auth path changes do not change worker semantics.
Raw Template path changes still change its exact fingerprint. S3 region changes
storage identity; authorized principal/refresh changes do not.

## 2. C08b2b, local HTTP composition

Depends on C08b2a. Own startup_material.rs and new composition/tests.rs. Limit
files.rs changes to removing consumed item-level dead_code allowances and any
small shared cfg(test) fixture access. No parser rewrites, listener setup or
control-root access. HTTP adapter edits are unnecessary for construction; a
small adapter-private test module may inspect constructor state.

Add crate-private API/bootstrap preparation returning concrete redacted bundles.
Each retains its LoadedInput, optional TlsMaterial and effective backend material;
API also retains Auth, bootstrap keeps its own enrollment token. Preserve original
Template hints and input bytes. Split local preparation from backend connection
so tests can remove/replace files between these stages. One Reader performs all
local loading under the original deadline. The bundles must outlive any later
client/listener construction and supply retained roots/CertifiedKey directly.

For HTTP retain HttpToken and optional CaMaterial together with the validated
base/Namespace. Construct HttpStore with the retained effective token and
`ca.as_ref().map(|c| c.pem.as_ref())`. HTTPS must have Some, including a previously
selected system bundle. Supplied CA remains bound even for plain HTTP. Compute
identity from these same records and base normalization. Return the client with
the identity; do not independently reconstruct either from paths. A private
HTTP/S3 enum can retain concrete stores; no new dyn ObjectStore abstraction is
needed. Reserve the feature-gated S3 arm for assignment 3.

Use a private fixed startup error classification, with static text and no source.
Local parsing/material limits, unavailable S3 feature, invalid namespace, adapter
constructor validation, missing region and forbidden endpoints are invalid_config.
Provider deadline/access failures are storage_unavailable. Future control decode
or attachment mismatches are incompatible_cluster and belong to C08c. Do not map
every ConfigurationError::LimitExceeded to backend timeout: Reader already uses
it for both local limits and local progress expiry. This slice has no network
storage access whose success could establish availability.

Focused private tests prepare synthetic API and bootstrap files, then remove or
replace auth/token/CA/cert/key originals before constructing the backend. Assert
retained token/CA arguments and resulting adapter authorization/base/namespace
through private inspection, plus retained CertifiedKey/roots after deletion.
Test HTTPS explicit/system-equivalent roots using injected temporary candidates,
never the machine trust store. Reuse C08b1 fixture generation rather than adding
a certificate framework. Check fixed sanitized failure categories and redacted
bundles with sentinels. Constructor success alone does not prove token/trust use;
inspect actual adapter state or the exact constructor arguments. No listeners or
HTTP requests are needed, and C08a/C08b1 rejection matrices need no duplication.

## 3. C08b2c, feature-gated S3 connection

Depends on the first two assignments and the parent dependency decision below.
Own only the distributed connection path in storage/s3.rs, a new private
storage/s3/startup.rs plus startup/tests.rs if useful, and the reserved S3 arm in
startup_material.rs. Keep legacy Storage<S3Store>::connect and load_defaults
behavior unchanged. A small shared bucket validator/client assembly helper is
acceptable; do not hand off either whole module.

Return a private connection result containing S3Store, resolved region and a
non-forgeable standard-policy marker produced only after endpoint checks. Validate
bucket/namespace before provider work. Wrap the actual configuration/provider
resolution future in tokio::time::timeout_at using the original startup deadline,
including effective-region resolution and endpoint inspection. Checking the clock
only before/after load does not bound its await. Credential-chain construction
does not necessarily resolve credentials; retain its refreshable provider and
do not force credential retrieval or an S3 request to prove configuration. Preserve
the disabled write retries. Use the accepted standard resolver, never clear an
override and label the result standard. FIPS/dual-stack remain standard AWS
resolver choices, not custom endpoint URLs.

Installed source evidence uses registry root
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`:

* aws-config 1.13.1 `src/lib.rs:861` ConfigLoader::load resolves region and builds
  provider chains. At 986..1025 it honors ignore_configured_endpoint_urls, stores
  EnvServiceConfig and may discard a configured global URL. Its `.env`/`.fs`
  injection methods at 1133/1141 require cfg(test) in that dependency or its
  `test-util` feature; this crate's cfg(test) does not enable them.
* `src/default_provider/endpoint_url.rs` validates global env/profile URL values,
  logs invalid values and returns None. Thus even without the ignore flag,
  SdkConfig::endpoint_url()==None is insufficient evidence of no override.
* aws-sdk-s3 1.158.0 `src/config.rs:1792..1813` Builder::from checks client origin,
  otherwise calls service_config.load_config with service `S3`, env key
  `AWS_ENDPOINT_URL`, profile key `endpoint_url`, then falls back to the shared
  endpoint. This covers `AWS_ENDPOINT_URL_S3` and selected profile services blocks.
  Checking only the shared getter misses them. `set_endpoint_url(None)` only
  erases the value; it does not detect or reject it.
* aws-config `src/env_service_config.rs` suppresses that service lookup when the
  ignore flag is true. No loaded-config getter restores the discarded values.
  ProviderConfig::env/fs/profile are crate-private. aws-config re-exports
  SdkConfig/Region, but not aws-types Env, Fs or ServiceConfigKey.
* aws-types 1.7.0 exposes os_shim_internal::{Env,Fs}, synthetic from_slice inputs,
  and service_config::{ServiceConfigKey,LoadServiceConfig}. aws-config::profile::load
  accepts Fs/Env/ProfileFiles and returns ProfileSet with get, selected_profile
  and other_sections. The deprecated profile_file aliases remain available;
  ProfileFiles::builder().with_contents supports inline synthetic profiles.
  aws-runtime 1.10.3 env_config::EnvConfigValue::load implements the actual
  service precedence. ProfileSet::other_sections returns Properties, whose get
  needs aws-runtime's PropertiesKey; it has no public iterator. Thus aws-types
  alone does not supply the complete raw services-block inspection seam.

Parent dependency reconciliation: the parent authorizes the following narrow
exception after independent design review confirmed the corrected seam.
No dependency edit or feature build is authorized to run concurrently with a
different Cargo owner. Current direct dependencies cannot perform complete
raw override rejection through the loaded configuration alone. The smallest
dependency exception is optional direct `aws-types = "=1.7.0"` and
`aws-runtime = "=1.10.3"`, included only by s3. Use EnvConfigValue or PropertiesKey
instead of copying the AWS services parser. Add aws-config's existing test-util
feature through a dedicated test-support feature for isolated loader tests if
those tests exercise ConfigLoader. All versions are locked/cached; no fetch or
SDK upgrade is proposed. Do not enable test-util in normal production or pull
AWS into no-default builds. Cargo.toml/lock edits
belong to the later exclusive Cargo owner, not this note or C04c1 concurrently.

With that exception, add a narrow raw endpoint preflight using SDK profile parsing
and raw environment presence before load, rejecting even empty/malformed overrides
and overrides hidden by the ignore flag. Inspect the selected global endpoint and
its referenced S3 services endpoint; do not reject unrelated inactive profiles or
other services. Reject any loaded shared endpoint. Resolve the exact S3 endpoint
ServiceConfigKey above once and reject every Some value, including empty values.
Retain the accepted absence in a private LoadServiceConfig wrapper installed in
the shared configuration used by both client builders; forward every other key
unchanged. The builders must not repeat a live lookup for that endpoint key.
Preserve the refreshable credential provider, standard resolver, FIPS/dual-stack
settings and disabled write retries. Produce the standard-policy marker only
for clients constructed through this retained decision.
The raw preflight is necessary because the loaded getters lose rejected/ignored
input. This retains only the endpoint decision, not an atomic snapshot of all
changing AWS files or environment. Tests pass explicit synthetic sources, and
construction must consume the retained endpoint absence even if the original
provider later changes. No general AWS source-snapshot framework is assigned
here. The dependency exception
is accepted in scope for this slice; implementation still requires an exclusive
Cargo handoff. It is not a proven implementation.

Offline tests use Env::from_slice, Fs::from_slice and inline profiles, never
set_var, real credentials, IMDS, process credentials or a request. Cover global
env, S3 env, global selected-profile and services overrides, precedence, invalid/
empty values and ignore=true with each override. Include inactive/unrelated
sections, missing/effective region, pending provider timeout/cancellation and
credentials-independent identity. Inject a fixed region and synthetic credentials
through the existing loader methods for loader tests; use a pending provider at
the real timeout seam. Synthetic SdkConfig/service-config tests must exercise
Builder::from's service-specific behavior, not just a fake boolean policy marker.
Use a stateful synthetic LoadServiceConfig that first reports endpoint absence
and then an override: after validation, both actual client builders must retain
absence without consulting that key again, while other settings still forward.
This is the causal check for validation being bound to construction.

Run no AWS build until the parent grants a serialized Cargo slot. Later validation
starts with focused locked/offline/no-default identity/composition tests, then
separately schedules s3-enabled focused tests and feature checks. No cloud probe
is required. Mutable metadata Match/non-ABA work remains C01d. Generation-2 codec,
fixtures, attachment comparison and generation-1 refusal remain C08c. This note
establishes no implementation, compilation, test or runtime guarantee.
