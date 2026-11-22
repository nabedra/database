use nabedra_database_server::{
    protocol::{MAX_BODY_BYTES, Predicate, parse_request},
    storage::{ClusterCatalog, StorageError},
};

/// Returns `accept` or `reject`.
pub fn fuzz_olap_operation(data: &[u8]) -> String {
    let Ok(request) = parse_request(data) else {
        return "reject".to_owned();
    };
    let encoded = serde_json::to_vec(&request).expect("decoded operation serializes");
    // Canonical UUIDs or numbers can make the encoding larger than the input.
    if encoded.len() <= MAX_BODY_BYTES {
        let round_trip = parse_request(&encoded).expect("encoded operation decodes");
        assert_eq!(round_trip, request);
    }
    "accept".to_owned()
}

/// Returns `accept` or `reject`.
pub fn fuzz_olap_predicate(data: &[u8]) -> String {
    let Ok(predicate) = serde_json::from_slice::<Predicate>(data) else {
        return "reject".to_owned();
    };
    let encoded = serde_json::to_vec(&predicate).expect("decoded predicate serializes");
    let round_trip =
        serde_json::from_slice::<Predicate>(&encoded).expect("encoded predicate decodes");
    assert_eq!(predicate, round_trip);
    "accept".to_owned()
}

/// Returns `accept`, `accept, too large to re-encode` or `reject`.
pub fn fuzz_olap_catalog(data: &[u8]) -> String {
    let Ok(catalog) = ClusterCatalog::decode(data) else {
        return "reject".to_owned();
    };
    match catalog.encode() {
        Ok(encoded) => {
            assert_eq!(
                ClusterCatalog::decode(&encoded).expect("encoded catalog decodes"),
                catalog,
            );
            "accept".to_owned()
        }
        // The canonical representation can be larger than the input.
        Err(StorageError::LimitExceeded) => "accept, too large to re-encode".to_owned(),
        Err(error) => panic!("decoded catalog failed validation on encode: {error:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reaches_olap_decoders() {
        assert!(parse_request(include_bytes!("../corpus/olap_operation/list.json")).is_ok());
        assert!(
            parse_request(include_bytes!(
                "../corpus/olap_operation/duplicate-field.json"
            ))
            .is_err()
        );

        let predicate = |bytes: &[u8]| serde_json::from_slice::<Predicate>(bytes);
        assert!(predicate(include_bytes!("../corpus/olap_predicate/comparison.json")).is_ok());
        assert!(predicate(include_bytes!("../corpus/olap_predicate/too-deep.json")).is_err());

        let catalog =
            ClusterCatalog::decode(include_bytes!("../corpus/olap_catalog/v3-empty.json")).unwrap();
        assert_eq!(catalog.format_version, 3);
        assert!(
            ClusterCatalog::decode(include_bytes!(
                "../corpus/olap_catalog/v3-partitioned-table.json"
            ))
            .is_ok()
        );
        assert!(
            ClusterCatalog::decode(include_bytes!("../corpus/olap_catalog/absent-head.json"))
                .is_err()
        );
        assert!(
            ClusterCatalog::decode(include_bytes!("../corpus/olap_catalog/partition-gap.json"))
                .is_err()
        );
    }
}
