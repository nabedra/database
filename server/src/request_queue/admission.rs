//! Shared API/peer context validation. Decoding never obtains ambient credentials.
use crate::{
    cluster::configuration::{Storage, Template},
    protocol::{ErrorCode, Outcome, ProtocolError},
    storage::context::{MAX_CONTEXT_BYTES, MAX_HEADERS, RequestContext, StorageKey},
};
use axum::http::HeaderMap;
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid storage context",
        Outcome::NotApplied,
    )
}
fn denied() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "storage context not authorized",
        Outcome::NotApplied,
    )
}
pub fn validate_context(
    template: &Template,
    context: Option<&RequestContext>,
    now: u64,
) -> Result<(), ProtocolError> {
    if template.mode.is_fixed() {
        return if context.is_none() {
            Ok(())
        } else {
            Err(denied())
        };
    }
    let context = context.ok_or_else(denied)?;
    context.validate(now).map_err(|_| invalid())?;
    if serde_json::to_vec(context).map_err(|_| invalid())?.len() > MAX_CONTEXT_BYTES {
        return Err(invalid());
    }
    if let Storage::S3(control) = &template.storage {
        // An omitted operator region cannot permit a same-bucket prefix overlap.
        // Region changes never distinguish customer identity within a partition.
        let region = control.region.as_deref().unwrap_or(context.region());
        let key =
            StorageKey::new(&control.bucket, region, &control.namespace).map_err(|_| invalid())?;
        if key.overlaps(&context.storage_key().map_err(|_| invalid())?) {
            return Err(denied());
        }
    }
    Ok(())
}
pub fn decode_context(
    template: &Template,
    headers: &HeaderMap,
    now: u64,
) -> Result<Option<RequestContext>, ProtocolError> {
    let mut total = 0usize;
    for (name, value) in headers {
        total = total
            .checked_add(name.as_str().len())
            .and_then(|n| n.checked_add(value.as_bytes().len() + 4))
            .ok_or_else(invalid)?;
        if name.as_str().starts_with("x-nabedra-") && name != "x-nabedra-storage" {
            return Err(denied());
        }
    }
    if total > MAX_HEADERS {
        return Err(invalid());
    }
    let mut values = headers.get_all("x-nabedra-storage").iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(invalid());
    }
    if template.mode.is_fixed() {
        return if value.is_none() {
            Ok(None)
        } else {
            Err(denied())
        };
    }
    let context = RequestContext::decode_header(
        value.ok_or_else(denied)?.to_str().map_err(|_| invalid())?,
        now,
    )
    .map_err(|_| invalid())?;
    validate_context(template, Some(&context), now)?;
    Ok(Some(context))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    fn template(serverless: bool) -> Template {
        Template::decode(&serde_json::to_vec(&serde_json::json!({"mode":if serverless {"serverless"}else{"fixed"},"api_urls":["http://127.0.0.1:9000/"],"listen":"127.0.0.1:0","operation_slots":1,"partition_slots":1,"queue_size":16,"partition_size":"4 KiB","storage":{"driver":"http","options":{"base_url":"http://127.0.0.1:9001/","namespace":"control/","token_file":"/fixture-token","ca_file":null}},"tls":null})).unwrap()).unwrap()
    }
    fn encode(value: &serde_json::Value) -> HeaderValue {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let bytes = serde_json::to_vec(value).unwrap();
        let mut out = String::new();
        let mut bits = 0u32;
        let mut n = 0;
        for b in bytes {
            bits = (bits << 8) | u32::from(b);
            n += 8;
            while n >= 6 {
                n -= 6;
                out.push(ALPHABET[((bits >> n) & 63) as usize] as char);
            }
        }
        if n > 0 {
            out.push(ALPHABET[((bits << (6 - n)) & 63) as usize] as char);
        }
        HeaderValue::from_str(&out).unwrap()
    }
    fn value() -> serde_json::Value {
        serde_json::json!({"version":1,"bucket":"customer-bucket","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"sentinel-access","secret_access_key":"sentinel-secret","session_token":null,"expires_at":null}})
    }
    #[test]
    fn sl02_sl06_api_and_peer_context_validation_fail_before_admission() {
        let mut headers = HeaderMap::new();
        assert!(
            decode_context(&template(false), &headers, 0)
                .unwrap()
                .is_none()
        );
        assert!(decode_context(&template(true), &headers, 0).is_err());
        headers.insert("x-nabedra-storage", encode(&value()));
        assert!(decode_context(&template(false), &headers, 0).is_err());
        let accepted = decode_context(&template(true), &headers, 0)
            .unwrap()
            .unwrap();
        assert_eq!(accepted.bucket(), "customer-bucket");
        assert!(validate_context(&template(false), Some(&accepted), 0).is_err());
        headers.append("x-nabedra-storage", encode(&value()));
        assert!(decode_context(&template(true), &headers, 0).is_err());
        headers.remove("x-nabedra-storage");
        let mut bad = value();
        bad["credentials"]["expires_at"] = serde_json::json!("2026-11-16T20:00:00Z");
        headers.insert("x-nabedra-storage", encode(&bad));
        assert!(decode_context(&template(true), &headers, u64::MAX).is_err());
        bad = value();
        bad["endpoint"] = serde_json::json!("http://attacker");
        headers.insert("x-nabedra-storage", encode(&bad));
        assert!(decode_context(&template(true), &headers, 0).is_err());
        headers.insert("x-nabedra-storage", encode(&value()));
        headers.insert("x-nabedra-principal", HeaderValue::from_static("forged"));
        assert!(decode_context(&template(true), &headers, 0).is_err());
        headers.remove("x-nabedra-principal");
        headers.insert(
            "padding",
            HeaderValue::from_str(&"x".repeat(MAX_HEADERS)).unwrap(),
        );
        assert!(decode_context(&template(true), &headers, 0).is_err());
        headers.remove("padding");
        headers.insert(
            "x-nabedra-storage",
            HeaderValue::from_str(&"x".repeat(24 * 1024 + 1)).unwrap(),
        );
        assert!(decode_context(&template(true), &headers, 0).is_err());
    }
    #[cfg(feature = "s3")]
    #[test]
    fn sl02_s3_control_overlap_is_rejected_at_api_and_peer_admission() {
        let mut config = serde_json::to_value(template(true)).unwrap();
        config["storage"] = serde_json::json!({"driver":"s3","options":{"bucket":"operator-bucket","region":"eu-west-1","namespace":"control/"}});
        let template = Template::decode(&serde_json::to_vec(&config).unwrap()).unwrap();
        for prefix in ["control", "control/customer"] {
            let mut overlapping = value();
            overlapping["bucket"] = serde_json::json!("operator-bucket");
            overlapping["prefix"] = serde_json::json!(prefix);
            let mut headers = HeaderMap::new();
            let header = encode(&overlapping);
            let context = RequestContext::decode_header(header.to_str().unwrap(), 0).unwrap();
            headers.insert("x-nabedra-storage", header);
            assert!(decode_context(&template, &headers, 0).is_err());
            assert!(validate_context(&template, Some(&context), 0).is_err());
        }
        let mut separate = value();
        separate["bucket"] = serde_json::json!("operator-bucket");
        separate["prefix"] = serde_json::json!("other/");
        let mut headers = HeaderMap::new();
        headers.insert("x-nabedra-storage", encode(&separate));
        assert!(decode_context(&template, &headers, 0).unwrap().is_some());
    }
}
