use super::super::command::Command;
use super::*;
use bytes::Bytes;

fn credentials(name: Option<&'static [u8]>, password: &'static [u8]) -> Credentials {
    Credentials {
        username: name.map(Bytes::from_static),
        password: Bytes::from_static(password),
    }
}

fn default_user() -> UserConfig {
    UserConfig {
        username: "default".to_owned(),
        password: "密碼".to_owned(),
        key_prefixes: vec!["nb:catalog:".to_owned()],
        commands: vec![CommandKind::Get, CommandKind::Del],
        max_connections: Some(1),
    }
}

fn command(words: &[&'static [u8]]) -> DataCommand {
    let Command::Data(command) =
        Command::parse(words.iter().map(|word| Bytes::from_static(word)).collect()).unwrap()
    else {
        panic!("expected data command");
    };
    command
}

#[test]
fn k01_acl_checks_every_argument_and_distinguishes_command_and_prefix() {
    let acl = Acl::new(vec![default_user()]).unwrap();
    let user = acl
        .authenticate(&credentials(None, "密碼".as_bytes()))
        .unwrap();
    assert_eq!(user.name(), "default");
    assert!(
        acl.authenticate(&credentials(Some(b"default"), "密碼".as_bytes()))
            .is_some()
    );
    assert!(
        acl.authenticate(&credentials(Some(b"unknown"), "密碼".as_bytes()))
            .is_none()
    );
    assert!(acl.authenticate(&credentials(None, b"wrong")).is_none());
    assert!(user.allows(&command(&[b"GET", b"nb:catalog:sku"])));
    assert!(!user.allows(&command(&[b"SET", b"nb:catalog:sku", b"stock"])));
    assert!(!user.allows(&command(&[b"GET", b"nb:catalog2:sku"])));
    assert!(!user.allows(&command(&[b"DEL", b"nb:catalog:sku", b"nb:catalog2:sku"])));
}

#[test]
fn k02_identity_connection_charge_is_released_when_the_permit_drops() {
    let acl = Acl::new(vec![default_user()]).unwrap();
    let user = acl
        .authenticate(&credentials(None, "密碼".as_bytes()))
        .unwrap();
    let first = user.try_connect().unwrap();
    assert!(user.try_connect().is_none());
    drop(first);
    assert!(user.try_connect().is_some());
    assert!(matches!(
        Acl::new(vec![default_user(), default_user()]),
        Err(AclConfigError::DuplicateUser)
    ));
    assert!(matches!(
        Acl::new(vec![UserConfig {
            max_connections: Some(0),
            ..default_user()
        }]),
        Err(AclConfigError::ConnectionLimit)
    ));
}
