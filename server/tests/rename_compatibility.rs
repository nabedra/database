use nabedra_database_server::{cluster::configuration::Template, storage::context::StorageKey};
use serde_json::json;

#[test]
fn existing_storage_identity_and_template_fingerprints_survive_rename() {
    // These fixed values predate the rename. Existing persisted roots must match.
    let key = StorageKey::new("tenant-data", "us-east-1", "meshdb-v0/").unwrap();
    assert_eq!(
        key.digest(),
        "a31a2977bd480b802a05931f659ab9bc99e1443a6ff65d756741b15dfca545c4"
    );
    let original = json!({
        "api_urls": ["http://api/"],
        "listen": "worker:0",
        "storage": {"driver": "http", "options": {
            "base_url": "http://store/", "namespace": "meshdb/",
            "token_file": "/token", "ca_file": null
        }},
        "tls": null,
        "operation_slots": 4,
        "partition_slots": 4,
        "queue_size": 64,
        "partition_size": "64 MiB"
    });
    let template = Template::decode(&serde_json::to_vec(&original).unwrap()).unwrap();
    assert_eq!(
        template.fingerprint().unwrap(),
        "a6f1bc932125f814fed2b0676c9f3350e6bd4eda9d570efb5abd17651c0f53d1"
    );
}
