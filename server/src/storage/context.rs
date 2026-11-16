//! Trusted gateway context. Secrets have no Debug implementation and are never
//! included in the stable database identity or operation digest.
use super::{Namespace, StorageError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_CONTEXT_BYTES: usize = 16 * 1024;
pub const MAX_CONTEXT_HEADER: usize = 24 * 1024;
pub const MAX_HEADERS: usize = 32 * 1024;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageKey {
    partition: String,
    bucket: String,
    prefix: String,
}
impl StorageKey {
    pub fn new(bucket: &str, region: &str, prefix: &str) -> Result<Self, StorageError> {
        if !valid_bucket(bucket) || !valid_region(region) {
            return Err(StorageError::Configuration);
        }
        let prefix = Namespace::new(prefix)?.as_str().to_owned();
        let partition = if region.starts_with("cn-") {
            "aws-cn"
        } else if region.starts_with("us-gov-") {
            "aws-us-gov"
        } else {
            "aws"
        };
        Ok(Self {
            partition: partition.into(),
            bucket: bucket.into(),
            prefix,
        })
    }
    pub fn overlaps(&self, other: &Self) -> bool {
        self.partition == other.partition
            && self.bucket == other.bucket
            && (self.prefix.starts_with(&other.prefix) || other.prefix.starts_with(&self.prefix))
    }
    pub fn digest(&self) -> String {
        let mut h = Sha256::new();
        // Persisted storage identities retain their original format domain.
        h.update(b"meshdb.storage.v1\0");
        for value in [&self.partition, &self.bucket, &self.prefix] {
            h.update(value.as_bytes());
            h.update([0]);
        }
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }
}
impl std::fmt::Debug for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StorageKey { .. }")
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(deserialize_with = "required_nullable")]
    pub session_token: Option<String>,
    #[serde(deserialize_with = "required_nullable")]
    pub expires_at: Option<String>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestContext {
    version: u32,
    bucket: String,
    region: String,
    prefix: String,
    credentials: AwsCredentials,
}
impl std::fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RequestContext { .. }")
    }
}
impl RequestContext {
    pub fn decode_header(header: &str, now_epoch_seconds: u64) -> Result<Self, StorageError> {
        if header.len() > MAX_CONTEXT_HEADER {
            return Err(StorageError::LimitExceeded);
        }
        let bytes = decode_base64url(header)?;
        crate::protocol::json_input(&bytes, MAX_CONTEXT_BYTES)
            .map_err(|_| StorageError::Configuration)?;
        let value: Self =
            serde_json::from_slice(&bytes).map_err(|_| StorageError::Configuration)?;
        value.validate(now_epoch_seconds)?;
        Ok(value)
    }
    pub fn validate(&self, now: u64) -> Result<(), StorageError> {
        self.storage_key()?;
        if self.version != 1
            || self.prefix.len() > 1024
            || self.credentials.access_key_id.is_empty()
            || self.credentials.secret_access_key.is_empty()
            || [
                &self.credentials.access_key_id,
                &self.credentials.secret_access_key,
            ]
            .iter()
            .any(|s| s.len() > 4096 || s.chars().any(char::is_control))
            || self
                .credentials
                .session_token
                .as_deref()
                .is_some_and(|s| s.is_empty() || s.len() > 8192 || s.chars().any(char::is_control))
            || self
                .credentials
                .expires_at
                .as_deref()
                .is_some_and(|s| parse_expiry(s).is_none_or(|t| t <= now))
        {
            return Err(StorageError::Configuration);
        }
        Ok(())
    }
    pub fn storage_key(&self) -> Result<StorageKey, StorageError> {
        StorageKey::new(&self.bucket, &self.region, &self.prefix)
    }
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn namespace(&self) -> Result<Namespace, StorageError> {
        Namespace::new(&self.prefix)
    }
    #[cfg(any(all(feature = "olap", feature = "s3"), test))]
    pub(crate) fn credentials(&self) -> &AwsCredentials {
        &self.credentials
    }
}
pub fn valid_bucket(s: &str) -> bool {
    (3..=63).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.".contains(&b))
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
        && !s.contains("..")
        && !s.contains(".-")
        && !s.contains("-.")
        && !s.starts_with("xn--")
        && !s.ends_with("-s3alias")
        && !s.ends_with("--ol-s3")
        && !s.ends_with("--x-s3")
        && s.parse::<std::net::Ipv4Addr>().is_err()
}
pub fn valid_region(s: &str) -> bool {
    let p: Vec<_> = s.split('-').collect();
    if s.len() > 64 {
        return false;
    }
    let number =
        |s: &str| !s.is_empty() && !s.starts_with('0') && s.bytes().all(|b| b.is_ascii_digit());
    match p.as_slice() {
        ["us", "gov", direction, n] => matches!(*direction, "east" | "west") && number(n),
        [area, direction, n] => {
            matches!(
                *area,
                "af" | "ap" | "ca" | "cn" | "eu" | "il" | "me" | "mx" | "sa" | "us"
            ) && matches!(
                *direction,
                "east"
                    | "west"
                    | "north"
                    | "south"
                    | "central"
                    | "northeast"
                    | "northwest"
                    | "southeast"
                    | "southwest"
            ) && number(n)
        }
        _ => false,
    }
}

fn decode_base64url(s: &str) -> Result<Vec<u8>, StorageError> {
    if s.len() % 4 == 1 {
        return Err(StorageError::Configuration);
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut bits, mut n) = (0u32, 0u32);
    for b in s.bytes() {
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(StorageError::Configuration),
        };
        bits = (bits << 6) | u32::from(v);
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8);
            bits &= (1 << n) - 1;
        }
        if out.len() > MAX_CONTEXT_BYTES {
            return Err(StorageError::LimitExceeded);
        }
    }
    if bits != 0 {
        return Err(StorageError::Configuration);
    }
    Ok(out)
}
// Strict UTC calendar conversion, independent of local time zone and libc.
fn parse_expiry(s: &str) -> Option<u64> {
    if !s.is_ascii()
        || s.len() != 20
        || &s[4..5] != "-"
        || &s[7..8] != "-"
        || &s[10..11] != "T"
        || &s[13..14] != ":"
        || &s[16..17] != ":"
        || &s[19..] != "Z"
        || !s.is_ascii()
    {
        return None;
    }
    let y = s[0..4].parse::<i64>().ok()?;
    let m = s[5..7].parse::<i64>().ok()?;
    let d = s[8..10].parse::<i64>().ok()?;
    let h = s[11..13].parse::<u64>().ok()?;
    let min = s[14..16].parse::<u64>().ok()?;
    let sec = s[17..19].parse::<u64>().ok()?;
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1970..=9999).contains(&y)
        || !(1..=12).contains(&m)
        || d < 1
        || d > days[(m - 1) as usize]
        || h > 23
        || min > 59
        || sec > 59
    {
        return None;
    }
    let y = y - i64::from(m <= 2);
    let era = y / 400;
    let yo = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let day = era * 146097 + yo * 365 + yo / 4 - yo / 100 + (153 * mp + 2) / 5 + d - 1 - 719468;
    u64::try_from(day)
        .ok()?
        .checked_mul(86400)?
        .checked_add(h * 3600 + min * 60 + sec)
}

fn required_nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AwsCredentials { .. }")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn raw() -> String {
        r#"{"version":1,"bucket":"tenant-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"SENTINEL_ACCESS","secret_access_key":"SENTINEL_SECRET","session_token":null,"expires_at":null}}"#.into()
    }
    #[test]
    fn gateway_expiry_requires_normalized_whole_seconds() {
        let fractional = raw().replace(
            "\"expires_at\":null",
            "\"expires_at\":\"2026-11-16T20:00:00.123Z\"",
        );
        assert!(RequestContext::decode_header(&encode(fractional.as_bytes()), 0).is_err());
        let normalized = fractional.replace(".123Z", "Z");
        assert!(RequestContext::decode_header(&encode(normalized.as_bytes()), 0).is_ok());
    }
    fn encode(bytes: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let (mut bits, mut n) = (0u32, 0u32);
        let mut out = String::new();
        for b in bytes {
            bits = (bits << 8) | u32::from(*b);
            n += 8;
            while n >= 6 {
                n -= 6;
                out.push(ALPHABET[((bits >> n) & 63) as usize] as char);
            }
        }
        if n > 0 {
            out.push(ALPHABET[((bits << (6 - n)) & 63) as usize] as char);
        }
        out
    }
    #[test]
    fn sl01_stable_identity_rotation_and_prefix_isolation() {
        let a = RequestContext::decode_header(&encode(raw().as_bytes()), 0).unwrap();
        let b = RequestContext::decode_header(
            &encode(raw().replace("SENTINEL_ACCESS", "ROTATED").as_bytes()),
            0,
        )
        .unwrap();
        assert_eq!(a.storage_key().unwrap(), b.storage_key().unwrap());
        assert_eq!(
            a.storage_key().unwrap().digest(),
            b.storage_key().unwrap().digest()
        );
        let c = StorageKey::new("tenant-data", "eu-west-1", "other").unwrap();
        assert_ne!(a.storage_key().unwrap(), c);
        assert!(!a.storage_key().unwrap().overlaps(&c));
        assert!(
            a.storage_key()
                .unwrap()
                .overlaps(&StorageKey::new("tenant-data", "eu-west-1", "db/nested").unwrap())
        );
        assert!(!format!("{a:?}").contains("SENTINEL"));
        assert!(!format!("{:?}", a.credentials()).contains("SENTINEL"));
    }
    #[test]
    fn sl02_sl06_strict_context_rejects_duplicates_unknown_fields_and_expiry() {
        for s in [
            raw().replace("\"version\":1", "\"version\":1,\"version\":1"),
            raw().replace(
                "\"version\":1",
                "\"version\":1,\"endpoint\":\"http://attacker\"",
            ),
            raw().replace("\"session_token\":null,", ""),
            raw().replace(
                "\"secret_access_key\":",
                "\"extra\":0,\"secret_access_key\":",
            ),
            raw().replace("eu-west-1", "http://attacker"),
            raw().replace("tenant-data", "bucket--x-s3"),
            raw().replace("\"db/\"", "\"../control\""),
            raw().replace(
                "\"expires_at\":null",
                "\"expires_at\":\"2026-11-16T20:00:00Z\"",
            ),
        ] {
            assert!(RequestContext::decode_header(&encode(s.as_bytes()), u64::MAX).is_err());
        }
        assert!(RequestContext::decode_header(&"A".repeat(MAX_CONTEXT_HEADER + 1), 0).is_err());
        for bad in ["A", "AA=", "+AAA", "/AAA", "AB"] {
            assert!(RequestContext::decode_header(bad, 0).is_err());
        }
        assert_eq!(parse_expiry("1970-01-01T00:00:00Z"), Some(0));
        assert!(parse_expiry("2025-02-29T00:00:00Z").is_none());
        assert!(parse_expiry("2024-02-29T00:00:00Z").is_some());
        assert!(parse_expiry("éééééééééé").is_none());
        for invalid in [
            "us-iso-east-1",
            "us-isob-east-1",
            "us-isof-south-1",
            "us-iso-1",
            "eu-west-0",
            "eu-west-01",
            "eusc-de-east-1",
        ] {
            assert!(!valid_region(invalid));
        }
        for region in ["eu-west-1", "ap-southeast-7", "cn-north-1", "us-gov-east-1"] {
            assert!(valid_region(region));
        }
        assert_ne!(
            StorageKey::new("tenant-data", "cn-north-1", "db").unwrap(),
            StorageKey::new("tenant-data", "us-east-1", "db").unwrap()
        );
        assert_ne!(
            StorageKey::new("tenant-data", "us-gov-east-1", "db").unwrap(),
            StorageKey::new("tenant-data", "us-east-1", "db").unwrap()
        );
    }
}

/// Call only after authenticating the separate gateway compute bearer. This
/// parser never treats any supplied identity header as an authorization grant.
pub fn from_headers(
    headers: &hyper::HeaderMap,
    serverless: bool,
    now: u64,
) -> Result<Option<RequestContext>, StorageError> {
    let mut bytes = 0usize;
    for (name, value) in headers {
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|b| b.checked_add(value.as_bytes().len() + 4))
            .ok_or(StorageError::LimitExceeded)?;
        if bytes > MAX_HEADERS {
            return Err(StorageError::LimitExceeded);
        }
        if name.as_str().starts_with("x-nabedra-") && name.as_str() != "x-nabedra-storage" {
            return Err(StorageError::Configuration);
        }
    }
    let mut contexts = headers.get_all("x-nabedra-storage").iter();
    let context = contexts.next();
    if contexts.next().is_some() || (!serverless && context.is_some()) {
        return Err(StorageError::Configuration);
    }
    match (serverless, context) {
        (true, Some(value)) => RequestContext::decode_header(
            value.to_str().map_err(|_| StorageError::Configuration)?,
            now,
        )
        .map(Some),
        (true, None) => Err(StorageError::Configuration),
        (false, None) => Ok(None),
        _ => Err(StorageError::Configuration),
    }
}

#[cfg(test)]
mod header_tests {
    use super::*;
    #[test]
    fn sl02_missing_forged_duplicate_context_never_admits() {
        let mut headers = hyper::HeaderMap::new();
        assert!(from_headers(&headers, true, 0).is_err());
        assert!(from_headers(&headers, false, 0).unwrap().is_none());
        headers.insert(
            "x-nabedra-principal",
            hyper::header::HeaderValue::from_static("forged"),
        );
        assert!(from_headers(&headers, false, 0).is_err());
        headers.clear();
        headers.append(
            "x-nabedra-storage",
            hyper::header::HeaderValue::from_static("e30"),
        );
        headers.append(
            "x-nabedra-storage",
            hyper::header::HeaderValue::from_static("e30"),
        );
        assert!(from_headers(&headers, true, 0).is_err());
        assert!(from_headers(&headers, false, 0).is_err());
        headers.clear();
        headers.insert(
            "x-fill",
            hyper::header::HeaderValue::from_str(&"x".repeat(MAX_HEADERS)).unwrap(),
        );
        assert!(matches!(
            from_headers(&headers, false, 0),
            Err(StorageError::LimitExceeded)
        ));
    }
}
