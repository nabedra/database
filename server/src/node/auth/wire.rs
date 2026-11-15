//! Fixed A07 declaration order. Decoding proves shape only.
use super::Error;
use crate::{
    cluster::{
        configuration,
        member::{nullable_uuid, uuid},
    },
    protocol::deserialize_object,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;
const AUTH_CAP: usize = 4096;
const SMALL_CAP: usize = 16 * 1024;
fn check(ok: bool) -> Result<(), Error> {
    if ok { Ok(()) } else { Err(Error::Invalid) }
}
fn encode(value: &impl Serialize, cap: usize) -> Result<Vec<u8>, Error> {
    configuration::bounded(value, cap).map_err(|_| Error::Limit)
}
// A depth-16 lexical preflight is deliberately before any serde allocation.
// The existing configuration preflight has a different (32) contract.
fn preflight(bytes: &[u8], cap: usize) -> Result<(), Error> {
    if bytes.len() > cap {
        return Err(Error::Limit);
    }
    let (mut depth, mut string, mut escape) = (0u32, false, false);
    for &b in bytes {
        if string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                string = false;
            }
        } else {
            match b {
                b'"' => string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 16 {
                        return Err(Error::Limit);
                    }
                }
                b'}' | b']' => depth = depth.checked_sub(1).ok_or(Error::Invalid)?,
                _ => {}
            }
        }
    }
    Ok(())
}
// Unchecked construction is private to this module; exposed records deliberately
// do not implement serde::Deserialize. Callers must use their checked decoders.
trait DecodeRecord: Sized {
    fn decode_record<'de, D: Deserializer<'de>>(d: D) -> Result<Self, D::Error>;
}
#[allow(dead_code)] // Nested checked decoders gain production callers in R03a2.
fn nested_record<'de, D: Deserializer<'de>, T: DecodeRecord>(d: D) -> Result<T, D::Error> {
    T::decode_record(d)
}
fn decode<T: DecodeRecord>(bytes: &[u8], cap: usize) -> Result<T, Error> {
    preflight(bytes, cap)?;
    let mut d = serde_json::Deserializer::from_slice(bytes);
    let v = T::decode_record(&mut d).map_err(|_| Error::Invalid)?;
    d.end().map_err(|_| Error::Invalid)?;
    Ok(v)
}
fn hex_ok(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| {
            let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
            digit(p[0]) * 16 + digit(p[1])
        })
        .collect()
}
fn decode_header(s: &str) -> Result<Vec<u8>, Error> {
    if s.len() > 8192 {
        return Err(Error::Limit);
    }
    check(s.len().is_multiple_of(2) && hex_ok(s, s.len()))?;
    Ok(unhex(s))
}
pub(crate) fn decode_signature(s: &str) -> Result<[u8; 64], Error> {
    check(hex_ok(s, 128))?;
    unhex(s).try_into().map_err(|_| Error::Invalid)
}
// Referenced by the pending Auth decoder's derived Wire implementation.
#[allow(dead_code)]
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct Number(u64);
impl<'de> Deserialize<'de> for Number {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = <&RawValue>::deserialize(d)?;
        let s = raw.get();
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(serde::de::Error::custom("Invalid"));
        }
        s.parse()
            .map(Self)
            .map_err(|_| serde::de::Error::custom("Invalid"))
    }
}
macro_rules! strings {($name:ident {$($v:ident => $text:literal),* $(,)?})=>{
 #[derive(Clone,Copy,PartialEq,Eq,Serialize)] pub(crate) enum $name {$(#[serde(rename=$text)] $v),*}
 impl<'de> Deserialize<'de> for $name {fn deserialize<D:Deserializer<'de>>(d:D)->Result<Self,D::Error>{match String::deserialize(d)?.as_str() {$($text=>Ok(Self::$v),)* _=>Err(serde::de::Error::custom("Invalid"))}}}
};}
strings!(Role { Api=>"api", Worker=>"worker" });
strings!(Scope {Operation=>"operation",Job=>"job",Readiness=>"readiness",DependencyOpen=>"dependency_open",DependencyClose=>"dependency_close",Drain=>"drain"});
strings!(Route {Operations=>"/internal/v1/operations",Jobs=>"/internal/v1/jobs",Readiness=>"/internal/v1/readiness",DependencyOpen=>"/internal/v1/dependencies/open",DependencyClose=>"/internal/v1/dependencies/close",Drain=>"/internal/v1/drain"});
strings!(Registration {Registered=>"registered",Verified=>"verified"});
strings!(Method {Post=>"POST"});
impl Route {
    fn scope(self) -> Scope {
        match self {
            Self::Operations => Scope::Operation,
            Self::Jobs => Scope::Job,
            Self::Readiness => Scope::Readiness,
            Self::DependencyOpen => Scope::DependencyOpen,
            Self::DependencyClose => Scope::DependencyClose,
            Self::Drain => Scope::Drain,
        }
    }
}
impl Scope {
    fn role(self) -> Role {
        match self {
            Self::Operation | Self::Readiness | Self::Drain => Role::Api,
            _ => Role::Worker,
        }
    }
}
// All DTO fields remain private. These item allowances cover the pending R03a2
// consumers only, and should be removed when those consumers land.
macro_rules! record {($name:ident {$($(#[$a:meta])* $f:ident:$t:ty),* $(,)?})=>{
 #[allow(dead_code)] #[derive(Clone,PartialEq,Eq,Serialize)] pub(crate) struct $name {$($(#[$a])* $f:$t),*}
 impl DecodeRecord for $name {fn decode_record<'de,D:Deserializer<'de>>(d:D)->Result<Self,D::Error>{
 #[derive(Deserialize)] #[serde(deny_unknown_fields)] struct Wire {$($(#[$a])* $f:$t),*}
 let w:Wire=deserialize_object(d)?; Ok(Self{$($f:w.$f),*})
 }}
};}
record!(Sender {
 role:Role,
 #[serde(deserialize_with="uuid")] process_id:Uuid,
 #[serde(deserialize_with="nullable_uuid")] node_id:Option<Uuid>,
 #[serde(deserialize_with="nullable_uuid")] incarnation:Option<Uuid>,
 #[serde(deserialize_with="nullable_uuid")] session_id:Option<Uuid>,
 public_key:String,
});
record!(Receiver {
    #[serde(deserialize_with = "uuid")]
    node_id: Uuid,
    #[serde(deserialize_with = "uuid")]
    incarnation: Uuid,
    #[serde(deserialize_with = "uuid")]
    session_id: Uuid,
});
record!(Auth {
 version:Number,
 #[serde(deserialize_with="uuid")] cluster_id:Uuid,
 scope:Scope,
 #[serde(deserialize_with="nested_record")]
 sender:Sender,
 #[serde(deserialize_with="nested_record")]
 receiver:Receiver,
 sequence:Number,
 method:Method,
 path:Route,
 #[serde(deserialize_with="nullable_uuid")] operation_id:Option<Uuid>,
 #[serde(deserialize_with="nullable_uuid")] admission_id:Option<Uuid>,
 #[serde(deserialize_with="nullable_uuid")] job_id:Option<Uuid>,
 #[serde(deserialize_with="nullable_uuid")] attempt_id:Option<Uuid>,
 #[serde(deserialize_with="nullable")] request_digest:Option<String>,
 control_sha256:String,
 #[serde(deserialize_with="nullable_uuid")] dependency_id:Option<Uuid>,
});
record!(ReplyAuth {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    cluster_id: Uuid,
    request_sequence: Number,
    scope: Scope,
    #[serde(deserialize_with = "nested_record")]
    sender: Sender,
    #[serde(deserialize_with = "nested_record")]
    receiver: Sender,
    status: Number,
    control_sha256: String,
});
record!(EnrollmentUnsigned {
 #[serde(default, skip_serializing_if="Option::is_none")]
 resources:Option<crate::config::WorkerResources>,
 version:Number,
 #[serde(deserialize_with="uuid")] cluster_id:Uuid,
 #[serde(deserialize_with="uuid")] node_id:Uuid,
 #[serde(deserialize_with="nullable_uuid")] request_id:Option<Uuid>,
 #[serde(deserialize_with="uuid")] incarnation:Uuid,
 #[serde(deserialize_with="uuid")] session_id:Uuid,
 public_key:String,
 endpoint:String,
 operation_slots:Number,
 partition_slots:Number,
 queue_size:Number,
});
record!(EnrollmentReply {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    cluster_id: Uuid,
    #[serde(deserialize_with = "uuid")]
    node_id: Uuid,
    #[serde(deserialize_with = "uuid")]
    incarnation: Uuid,
    #[serde(deserialize_with = "uuid")]
    session_id: Uuid,
    registration: Registration,
    api_public_key: String,
});
record!(Readiness {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    challenge: Uuid,
});
record!(ReadinessReply {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    challenge: Uuid,
    initialized: bool,
});
record!(Dependency {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    permit_id: Uuid,
});
record!(DependencyCloseReply {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    permit_id: Uuid,
    closed: bool,
});
record!(Drain {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    drain_id: Uuid,
});
record!(DrainReply {
    version: Number,
    #[serde(deserialize_with = "uuid")]
    drain_id: Uuid,
    barrier: bool,
    idle: bool,
    heartbeat_sequence: Number,
});
record!(Enrollment {
 #[serde(default, skip_serializing_if="Option::is_none")]
 resources:Option<crate::config::WorkerResources>,
 version:Number,
 #[serde(deserialize_with="uuid")] cluster_id:Uuid,
 #[serde(deserialize_with="uuid")] node_id:Uuid,
 #[serde(deserialize_with="nullable_uuid")] request_id:Option<Uuid>,
 #[serde(deserialize_with="uuid")] incarnation:Uuid,
 #[serde(deserialize_with="uuid")] session_id:Uuid,
 public_key:String,
 endpoint:String,
 operation_slots:Number,
 partition_slots:Number,
 queue_size:Number,
 proof:String,
});

#[allow(dead_code)]
pub(crate) struct ActualRoute {
    route: Route,
    dependency: Option<Uuid>,
}
#[allow(dead_code)] // R03a2 request-verification caller pending.
impl ActualRoute {
    pub(crate) fn new(method: &str, path: &str, dependency: Option<&str>) -> Result<Self, Error> {
        check(method == "POST")?;
        let route = match path {
            "/internal/v1/operations" => Route::Operations,
            "/internal/v1/jobs" => Route::Jobs,
            "/internal/v1/readiness" => Route::Readiness,
            "/internal/v1/dependencies/open" => Route::DependencyOpen,
            "/internal/v1/dependencies/close" => Route::DependencyClose,
            "/internal/v1/drain" => Route::Drain,
            _ => return Err(Error::Invalid),
        };
        let dependency = dependency
            .map(|s| {
                let id = Uuid::parse_str(s).map_err(|_| Error::Invalid)?;
                check(id.to_string() == s)?;
                Ok(id)
            })
            .transpose()?;
        check(dependency.is_some() == (route == Route::Jobs))?;
        Ok(Self { route, dependency })
    }
}
impl Sender {
    pub(crate) fn api(process_id: Uuid, public_key: String) -> Result<Self, Error> {
        let value = Self {
            role: Role::Api,
            process_id,
            node_id: None,
            incarnation: None,
            session_id: None,
            public_key,
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn worker(
        node_id: Uuid,
        incarnation: Uuid,
        session_id: Uuid,
        public_key: String,
    ) -> Result<Self, Error> {
        let value = Self {
            role: Role::Worker,
            process_id: incarnation,
            node_id: Some(node_id),
            incarnation: Some(incarnation),
            session_id: Some(session_id),
            public_key,
        };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), Error> {
        check(hex_ok(&self.public_key, 64))?;
        check(match self.role {
            Role::Api => {
                self.node_id.is_none() && self.incarnation.is_none() && self.session_id.is_none()
            }
            Role::Worker => {
                self.node_id.is_some()
                    && self.incarnation == Some(self.process_id)
                    && self.session_id.is_some()
            }
        })
    }
}
impl Receiver {
    pub(crate) fn new(node_id: Uuid, incarnation: Uuid, session_id: Uuid) -> Self {
        Self {
            node_id,
            incarnation,
            session_id,
        }
    }
}
#[derive(Default)]
pub(crate) struct RequestIds {
    pub(crate) operation: Option<Uuid>,
    pub(crate) admission: Option<Uuid>,
    pub(crate) job: Option<Uuid>,
    pub(crate) attempt: Option<Uuid>,
    pub(crate) dependency: Option<Uuid>,
    pub(crate) digest: Option<String>,
}
impl Auth {
    /// Validate locally constructed claims before the boot counter stamps them.
    pub(crate) fn request(
        cluster: Uuid,
        sender: Sender,
        receiver: Receiver,
        route: Route,
        ids: RequestIds,
        digest: String,
    ) -> Result<Self, Error> {
        let value = Self {
            version: Number(1),
            cluster_id: cluster,
            scope: route.scope(),
            sender,
            receiver,
            sequence: Number(1),
            method: Method::Post,
            path: route,
            operation_id: ids.operation,
            admission_id: ids.admission,
            job_id: ids.job,
            attempt_id: ids.attempt,
            request_digest: ids.digest,
            control_sha256: digest,
            dependency_id: ids.dependency,
        };
        value.validate(&ActualRoute {
            route,
            dependency: ids.dependency,
        })?;
        value.canonical_bytes()?;
        Ok(value)
    }
    pub(super) fn with_sequence(mut self, sequence: u64) -> Result<Auth, Error> {
        if sequence == 0 {
            return Err(Error::Invalid);
        }
        self.sequence = Number(sequence);
        Ok(self)
    }

    pub(crate) fn decode(header: &str, actual: &ActualRoute) -> Result<Self, Error> {
        let bytes = decode_header(header)?;
        let v: Self = decode(&bytes, AUTH_CAP)?;
        v.validate(actual)?;
        check(v.canonical_bytes()? == bytes)?;
        Ok(v)
    }
    fn validate(&self, actual: &ActualRoute) -> Result<(), Error> {
        self.sender.validate()?;
        check(
            self.version.0 == 1
                && self.sequence.0 > 0
                && self.path == actual.route
                && self.scope == actual.route.scope()
                && (self.sender.role == self.scope.role()
                    || (self.scope == Scope::Operation
                        && self.sender.role == Role::Worker
                        && self.sender.node_id == Some(self.receiver.node_id)
                        && self.sender.incarnation == Some(self.receiver.incarnation)
                        && self.sender.session_id == Some(self.receiver.session_id)))
                && self.dependency_id == actual.dependency
                && hex_ok(&self.control_sha256, 64),
        )?;
        let operation = matches!(
            self.scope,
            Scope::Operation | Scope::Job | Scope::DependencyOpen | Scope::DependencyClose
        );
        let admission = matches!(
            self.scope,
            Scope::Job | Scope::DependencyOpen | Scope::DependencyClose
        );
        let job = self.scope == Scope::Job;
        check(
            self.operation_id.is_some() == operation
                && self.admission_id.is_some() == admission
                && self.job_id.is_some() == job
                && self.attempt_id.is_some() == job
                && self.request_digest.is_some() == operation
                && self.request_digest.as_deref().is_none_or(|s| hex_ok(s, 64)),
        )
    }
    pub(crate) fn canonical_bytes(&self) -> Result<Vec<u8>, Error> {
        encode(self, AUTH_CAP)
    }
}
impl ReplyAuth {
    pub(crate) fn response(
        request: &Auth,
        sender: Sender,
        status: u16,
        digest: String,
    ) -> Result<Self, Error> {
        let value = Self {
            version: Number(1),
            cluster_id: request.cluster_id,
            request_sequence: request.sequence,
            scope: request.scope,
            sender,
            receiver: request.sender.clone(),
            status: Number(u64::from(status)),
            control_sha256: digest,
        };
        let header: String = value
            .canonical_bytes()?
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        Self::decode(&header)
    }
    pub(crate) fn decode(header: &str) -> Result<Self, Error> {
        let bytes = decode_header(header)?;
        let v: Self = decode(&bytes, AUTH_CAP)?;
        v.sender.validate()?;
        v.receiver.validate()?;
        check(
            v.version.0 == 1
                && v.request_sequence.0 > 0
                && v.sender.role == Role::Worker
                && (v.receiver.role == v.scope.role()
                    || (v.scope == Scope::Operation
                        && v.receiver.role == Role::Worker
                        && v.receiver == v.sender))
                && (100..=599).contains(&v.status.0)
                && hex_ok(&v.control_sha256, 64),
        )?;
        check(v.canonical_bytes()? == bytes)?;
        Ok(v)
    }
    pub(crate) fn canonical_bytes(&self) -> Result<Vec<u8>, Error> {
        encode(self, AUTH_CAP)
    }
}
macro_rules! small {
    ($name:ident,$v:ident,$ok:expr) => {
        #[allow(dead_code)] // Enrollment/HTTP callers arrive after this wire-only slice.
        impl $name {
            pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
                let $v: Self = decode(bytes, SMALL_CAP)?;
                check($v.version.0 == 1 && ($ok))?;
                Ok($v)
            }
            pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
                encode(self, SMALL_CAP)
            }
        }
    };
}
small!(Readiness, v, true);
small!(ReadinessReply, v, v.initialized);
small!(Dependency, v, true);
small!(DependencyCloseReply, v, true);
small!(Drain, v, true);
small!(DrainReply, v, v.barrier && v.heartbeat_sequence.0 > 0);
small!(EnrollmentReply, v, hex_ok(&v.api_public_key, 64));
small!(EnrollmentUnsigned, v, v.valid());
small!(Enrollment, v, v.unsigned().valid() && hex_ok(&v.proof, 128));
impl EnrollmentUnsigned {
    pub(crate) fn public_key(&self) -> Result<[u8; 32], Error> {
        key_bytes(&self.public_key)
    }
    fn valid(&self) -> bool {
        hex_ok(&self.public_key, 64)
            && configuration::api_url(&self.endpoint)
            && (1..=256).contains(&self.operation_slots.0)
            && (1..=if self.resources.is_some() { 1024 } else { 256 })
                .contains(&self.partition_slots.0)
            && self.queue_size.0 <= 4096
            && self
                .resources
                .is_none_or(|r| r.validate().is_ok() && r.max_threads <= self.partition_slots.0)
    }
}
impl Enrollment {
    pub(crate) fn signed(value: EnrollmentUnsigned, proof: String) -> Result<Self, Error> {
        let value = Self {
            version: value.version,
            cluster_id: value.cluster_id,
            node_id: value.node_id,
            request_id: value.request_id,
            incarnation: value.incarnation,
            session_id: value.session_id,
            public_key: value.public_key,
            endpoint: value.endpoint,
            operation_slots: value.operation_slots,
            partition_slots: value.partition_slots,
            queue_size: value.queue_size,
            resources: value.resources,
            proof,
        };
        Self::decode(&value.encode()?)
    }
    fn unsigned(&self) -> EnrollmentUnsigned {
        EnrollmentUnsigned {
            version: self.version,
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            request_id: self.request_id,
            incarnation: self.incarnation,
            session_id: self.session_id,
            public_key: self.public_key.clone(),
            endpoint: self.endpoint.clone(),
            operation_slots: self.operation_slots,
            partition_slots: self.partition_slots,
            queue_size: self.queue_size,
            resources: self.resources,
        }
    }
    pub(crate) fn unsigned_bytes(&self) -> Result<Vec<u8>, Error> {
        self.unsigned().encode()
    }
}
#[cfg(test)]
mod tests;

// Concrete read access for R03a2 identity, content and request-association checks.
// Remove the per-impl pending-caller allowances as those checks are integrated.
macro_rules! access {
    ($name:ident {$($field:ident : $ty:ty => $value:expr),* $(,)?}) => {
        #[allow(dead_code)]
        impl $name { $(pub(crate) fn $field(&self) -> $ty { $value(self) })* }
    };
}
access!(Sender {
    role: Role => |s: &Sender| s.role,
    process_id: Uuid => |s: &Sender| s.process_id,
    node_id: Option<Uuid> => |s: &Sender| s.node_id,
    incarnation: Option<Uuid> => |s: &Sender| s.incarnation,
    session_id: Option<Uuid> => |s: &Sender| s.session_id,
});
access!(Receiver {
    node_id: Uuid => |s: &Receiver| s.node_id,
    incarnation: Uuid => |s: &Receiver| s.incarnation,
    session_id: Uuid => |s: &Receiver| s.session_id,
});
access!(Auth {
    cluster_id: Uuid => |s: &Auth| s.cluster_id,
    sequence: u64 => |s: &Auth| s.sequence.0,
    scope: Scope => |s: &Auth| s.scope,
    operation_id: Option<Uuid> => |s: &Auth| s.operation_id,
    admission_id: Option<Uuid> => |s: &Auth| s.admission_id,
    job_id: Option<Uuid> => |s: &Auth| s.job_id,
    attempt_id: Option<Uuid> => |s: &Auth| s.attempt_id,
    dependency_id: Option<Uuid> => |s: &Auth| s.dependency_id,
});
impl Auth {
    pub(crate) fn receiver(&self) -> &Receiver {
        &self.receiver
    }
    pub(crate) fn request_digest(&self) -> Option<&str> {
        self.request_digest.as_deref()
    }
    pub(crate) fn control_sha256(&self) -> &str {
        &self.control_sha256
    }
}
impl Auth {
    pub(crate) fn sender(&self) -> &Sender {
        &self.sender
    }
}
#[cfg(test)]
std::thread_local! {
    static SENDER_KEY_DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
impl Sender {
    pub(crate) fn public_key(&self) -> Result<[u8; 32], Error> {
        #[cfg(test)]
        SENDER_KEY_DECODES.with(|count| count.set(count.get().saturating_add(1)));
        key_bytes(&self.public_key)
    }
    #[cfg(test)]
    pub(in crate::node) fn key_decode_count() -> u64 {
        SENDER_KEY_DECODES.with(std::cell::Cell::get)
    }
}
fn key_bytes(s: &str) -> Result<[u8; 32], Error> {
    check(hex_ok(s, 64))?;
    unhex(s).try_into().map_err(|_| Error::Invalid)
}
access!(ReplyAuth {
    cluster_id: Uuid => |s: &ReplyAuth| s.cluster_id,
    request_sequence: u64 => |s: &ReplyAuth| s.request_sequence.0,
    scope: Scope => |s: &ReplyAuth| s.scope,
    status: u16 => |s: &ReplyAuth| s.status.0 as u16,
});
impl ReplyAuth {
    pub(crate) fn sender(&self) -> &Sender {
        &self.sender
    }
    pub(crate) fn receiver(&self) -> &Sender {
        &self.receiver
    }
    pub(crate) fn control_sha256(&self) -> &str {
        &self.control_sha256
    }
}
access!(Enrollment {
    cluster_id: Uuid => |s: &Enrollment| s.cluster_id,
    node_id: Uuid => |s: &Enrollment| s.node_id,
    request_id: Option<Uuid> => |s: &Enrollment| s.request_id,
    incarnation: Uuid => |s: &Enrollment| s.incarnation,
    session_id: Uuid => |s: &Enrollment| s.session_id,
});
impl Enrollment {
    pub(crate) fn resources(&self) -> Option<crate::config::WorkerResources> {
        self.resources
    }
    #[allow(dead_code)] // C03 enrollment callers pending.
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    #[allow(dead_code)] // C03 enrollment callers pending.
    pub(crate) fn operation_slots(&self) -> u64 {
        self.operation_slots.0
    }
    #[allow(dead_code)] // C03 enrollment callers pending.
    pub(crate) fn partition_slots(&self) -> u64 {
        self.partition_slots.0
    }
    #[allow(dead_code)] // C03 enrollment callers pending.
    pub(crate) fn queue_size(&self) -> u64 {
        self.queue_size.0
    }
    pub(crate) fn public_key(&self) -> Result<[u8; 32], Error> {
        key_bytes(&self.public_key)
    }
    pub(crate) fn proof(&self) -> Result<[u8; 64], Error> {
        decode_signature(&self.proof)
    }
}

impl Auth {
    pub(crate) fn route(&self) -> Route {
        self.path
    }
}
access!(Readiness { challenge: Uuid => |s: &Readiness| s.challenge });
access!(ReadinessReply {
 challenge: Uuid => |s: &ReadinessReply| s.challenge,
 initialized: bool => |s: &ReadinessReply| s.initialized,
});
access!(Dependency { permit_id: Uuid => |s: &Dependency| s.permit_id });
access!(DependencyCloseReply {
 permit_id: Uuid => |s: &DependencyCloseReply| s.permit_id,
 closed: bool => |s: &DependencyCloseReply| s.closed,
});
access!(Drain { drain_id: Uuid => |s: &Drain| s.drain_id });
access!(DrainReply {
 drain_id: Uuid => |s: &DrainReply| s.drain_id,
 barrier: bool => |s: &DrainReply| s.barrier,
 idle: bool => |s: &DrainReply| s.idle,
 heartbeat_sequence: u64 => |s: &DrainReply| s.heartbeat_sequence.0,
});
access!(EnrollmentReply {
 cluster_id: Uuid => |s: &EnrollmentReply| s.cluster_id,
 node_id: Uuid => |s: &EnrollmentReply| s.node_id,
 incarnation: Uuid => |s: &EnrollmentReply| s.incarnation,
 session_id: Uuid => |s: &EnrollmentReply| s.session_id,
 registration: Registration => |s: &EnrollmentReply| s.registration,
});
impl EnrollmentReply {
    #[allow(dead_code)] // Later trusted-channel enrollment association.
    pub(crate) fn api_public_key(&self) -> Result<[u8; 32], Error> {
        key_bytes(&self.api_public_key)
    }
}

impl DrainReply {
    /// Only admission's checked local observation can produce this reply.
    #[allow(dead_code)] // HTTP transport remains a later integration.
    pub(in crate::node) fn from_observation(
        observation: &crate::node::admission::DrainObservation,
    ) -> Self {
        let _original_request = observation.request_binding();
        Self {
            version: Number(1),
            drain_id: observation.drain_id(),
            barrier: true,
            idle: observation.idle(),
            heartbeat_sequence: Number(observation.sequence()),
        }
    }
}
