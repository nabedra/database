use super::*;

fn parse(words: &[&'static [u8]]) -> Result<Command, CommandError> {
    Command::parse(words.iter().map(|word| Bytes::from_static(word)).collect())
}

fn operation(words: &[&'static [u8]]) -> Operation {
    let Command::Data(command) = parse(words).unwrap() else {
        panic!("expected data command");
    };
    command.into_operation()
}

#[test]
fn k01_validates_set_forms_and_integer_boundaries() {
    assert!(matches!(
        operation(&[b"sET", b"x", b"\x00\xff", b"nx", b"ex", b"12"]),
        Operation::Set { value, ttl_ms: Some(12000), when: SetWhen::Absent, .. }
        if value.as_ref() == b"\x00\xff"
    ));
    for flags in [
        &[b"NX".as_slice(), b"XX"][..],
        &[b"PX".as_slice()][..],
        &[b"PX".as_slice(), b"1", b"EX", b"2"][..],
        &[b"GET".as_slice()][..],
    ] {
        let mut words: Vec<&[u8]> = vec![b"SET", b"k", b"v"];
        words.extend_from_slice(flags);
        assert!(matches!(parse(&words), Err(CommandError::Syntax)));
    }
    for ttl in [b"0".as_slice(), b"-1", b"9223372036854775807"] {
        assert!(matches!(
            parse(&[b"SET", b"k", b"v", b"EX", ttl]),
            Err(CommandError::InvalidExpiry)
        ));
    }
    assert!(matches!(
        operation(&[b"INCRBY", b"k", b"-9223372036854775808"]),
        Operation::Increment {
            delta: i64::MIN,
            ..
        }
    ));
    assert!(matches!(
        operation(&[b"INCRBY", b"k", b"9223372036854775807"]),
        Operation::Increment {
            delta: i64::MAX,
            ..
        }
    ));
    for bad in [
        b"+1".as_slice(),
        b"01",
        b"-0",
        b" 1",
        b"9223372036854775808",
        b"-9223372036854775809",
    ] {
        assert!(matches!(
            parse(&[b"INCRBY", b"k", bad]),
            Err(CommandError::InvalidInteger)
        ));
    }
    assert!(matches!(
        operation(&[b"EXPIRE", b"k", b"-1"]),
        Operation::Expire { ttl_ms: -1, .. }
    ));
    assert!(matches!(
        operation(&[b"PEXPIRE", b"k", b"0"]),
        Operation::Expire { ttl_ms: 0, .. }
    ));
}

#[test]
fn k01_slots_use_the_first_nonempty_hash_tag_rule() {
    assert_eq!(key_slot(b"123456789"), 12739);
    assert_eq!(key_slot(b"foo"), 12182);
    assert_eq!(
        key_slot(b"{user1000}.following"),
        key_slot(b"{user1000}.followers")
    );
    assert_eq!(key_slot(b"{x}{y}"), key_slot(b"x"));
    assert_eq!(key_slot(b"a{{x}}"), key_slot(b"{x"));
    assert_ne!(key_slot(b"foo{}{bar}"), key_slot(b"bar"));
    assert_ne!(key_slot(b"a{unfinished"), key_slot(b"unfinished"));

    let Command::Data(command) = parse(&[b"DEL", b"{x}:one", b"{y}:two"]).unwrap() else {
        panic!("data command");
    };
    assert_eq!(command.slot(), Err(CommandError::CrossSlot));

    let Command::Data(command) = parse(&[b"MGET", b"{x}:two", b"{x}:one", b"{x}:two"]).unwrap()
    else {
        panic!("data command");
    };
    assert_eq!(command.slot(), Ok(key_slot(b"x")));
    let keys: Vec<_> = command.keys().map(|key| key.as_ref()).collect();
    assert_eq!(keys, [b"{x}:two", b"{x}:one", b"{x}:two"]);
}

#[test]
fn k01_validates_driver_handshake_and_metadata() {
    assert!(matches!(
        parse(&[b"HELLO", b"2", b"AUTH", b"worker", b"secret", b"SETNAME", b"client"]),
        Ok(Command::Hello { auth: Some(Credentials { username: Some(username), password }), name: Some(name) })
        if username.as_ref() == b"worker" && password.as_ref() == b"secret" && name.as_ref() == b"client"
    ));
    assert!(matches!(
        parse(&[b"HELLO", b"3"]),
        Err(CommandError::ProtocolVersion)
    ));
    assert!(matches!(
        parse(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-py"]),
        Ok(Command::ClientSetInfo)
    ));
    assert!(matches!(
        parse(&[b"CLIENT", b"SETNAME", b"bad\r\nname"]),
        Err(CommandError::InvalidClientName)
    ));
    assert!(matches!(
        parse(&[b"CLUSTER", b"SLOTS"]),
        Ok(Command::ClusterSlots)
    ));
    assert!(matches!(
        parse(&[b"CLUSTER", b"SHARDS"]),
        Ok(Command::ClusterShards)
    ));

    let response = parse(&[b"COMMAND", b"INFO", b"get", b"MGET", b"HGET"])
        .unwrap()
        .metadata_response(1 << 20)
        .unwrap();
    let Response::Array(rows) = response else {
        panic!("metadata response");
    };
    assert_eq!(rows.len(), 3);
    assert!(
        matches!(&rows[0], Response::Array(fields) if fields[0] == Response::bulk(b"get") && fields[3..6] == [Response::Integer(1), Response::Integer(1), Response::Integer(1)])
    );
    assert!(
        matches!(&rows[1], Response::Array(fields) if fields[0] == Response::bulk(b"mget") && fields[3..6] == [Response::Integer(1), Response::Integer(-1), Response::Integer(1)])
    );
    assert_eq!(rows[2], Response::NullArray);
    assert_eq!(
        parse(&[b"COMMAND", b"GETKEYS", b"MGET", b"x", b"x", b"y"])
            .unwrap()
            .metadata_response(1 << 20),
        Some(Response::Array(vec![
            Response::bulk(b"x"),
            Response::bulk(b"x"),
            Response::bulk(b"y")
        ]))
    );
}

#[test]
fn k02_validates_item_sizes_in_every_operation() {
    let oversized_key = Bytes::from(vec![b'k'; MAX_KEY_BYTES + 1]);
    assert!(matches!(
        Command::parse(vec![
            Bytes::from_static(b"DEL"),
            Bytes::from_static(b"valid"),
            oversized_key
        ]),
        Err(CommandError::InvalidKeySize)
    ));
    assert!(matches!(
        Command::parse(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"x"),
            Bytes::from(vec![b'v'; MAX_VALUE_BYTES + 1])
        ]),
        Err(CommandError::InvalidValueSize)
    ));
}
