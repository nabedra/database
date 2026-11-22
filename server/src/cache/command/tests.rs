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

/// Release builds have no DEBUG command: every form gets the ordinary
/// unknown-command error, exactly as an unsupported command does.
#[cfg(not(feature = "test-hooks"))]
#[test]
fn test_hooks_debug_is_unknown_without_the_feature() {
    for words in [
        &[b"DEBUG".as_slice(), b"DIGEST"][..],
        &[b"debug", b"SET-ACTIVE-EXPIRE", b"0"],
        &[b"DEBUG", b"SLEEP", b"1"],
        &[b"DEBUG"],
    ] {
        let error = parse(words).err().expect("DEBUG must not parse");
        assert_eq!(error, CommandError::Unsupported);
        assert_eq!(
            error.response(),
            Response::error(b"ERR unknown or unsupported command")
        );
    }
}

/// Wrong-arity replies, byte for byte as Valkey 8.1.11 answers them
/// (captured from `valkey/valkey:8.1.11`; the differential found the
/// data-command cases in Actions run 45047452991). Valkey prints the
/// lowercase full command name, whatever case the client sent.
#[test]
fn k01_wrong_arity_errors_match_valkey() {
    let cases: &[(&[&[u8]], &[u8])] = &[
        (
            &[b"GET"],
            b"ERR wrong number of arguments for 'get' command",
        ),
        (
            &[b"get", b"a", b"b"],
            b"ERR wrong number of arguments for 'get' command",
        ),
        (
            &[b"SET", b"k"],
            b"ERR wrong number of arguments for 'set' command",
        ),
        (
            &[b"DEL"],
            b"ERR wrong number of arguments for 'del' command",
        ),
        (
            &[b"EXISTS"],
            b"ERR wrong number of arguments for 'exists' command",
        ),
        (
            &[b"MGET"],
            b"ERR wrong number of arguments for 'mget' command",
        ),
        (
            &[b"TTL"],
            b"ERR wrong number of arguments for 'ttl' command",
        ),
        (
            &[b"pttl", b"a", b"b"],
            b"ERR wrong number of arguments for 'pttl' command",
        ),
        (
            &[b"EXPIRE", b"k"],
            b"ERR wrong number of arguments for 'expire' command",
        ),
        (
            &[b"PEXPIRE", b"k"],
            b"ERR wrong number of arguments for 'pexpire' command",
        ),
        (
            &[b"PERSIST"],
            b"ERR wrong number of arguments for 'persist' command",
        ),
        (
            &[b"InCrBy", b"k"],
            b"ERR wrong number of arguments for 'incrby' command",
        ),
        (
            &[b"PING", b"a", b"b"],
            b"ERR wrong number of arguments for 'ping' command",
        ),
        (
            &[b"ECHO"],
            b"ERR wrong number of arguments for 'echo' command",
        ),
        (
            &[b"ECHO", b"a", b"b"],
            b"ERR wrong number of arguments for 'echo' command",
        ),
        (
            &[b"AUTH"],
            b"ERR wrong number of arguments for 'auth' command",
        ),
        (&[b"AUTH", b"a", b"b", b"c"], b"ERR syntax error"),
        (
            &[b"CLIENT"],
            b"ERR wrong number of arguments for 'client' command",
        ),
        (
            &[b"CLIENT", b"SETNAME"],
            b"ERR wrong number of arguments for 'client|setname' command",
        ),
        (
            &[b"client", b"setname", b"a", b"b"],
            b"ERR wrong number of arguments for 'client|setname' command",
        ),
        (
            &[b"CLIENT", b"SETINFO", b"LIB-NAME"],
            b"ERR wrong number of arguments for 'client|setinfo' command",
        ),
        (
            &[b"COMMAND", b"COUNT", b"x"],
            b"ERR wrong number of arguments for 'command|count' command",
        ),
        (
            &[b"COMMAND", b"GETKEYS"],
            b"ERR wrong number of arguments for 'command|getkeys' command",
        ),
        (
            &[b"COMMAND", b"GETKEYS", b"GET"],
            b"ERR Invalid number of arguments specified for command",
        ),
        (
            &[b"COMMAND", b"GETKEYS", b"GET", b"a", b"b"],
            b"ERR Invalid number of arguments specified for command",
        ),
        (
            &[b"CLUSTER"],
            b"ERR wrong number of arguments for 'cluster' command",
        ),
        (
            &[b"CLUSTER", b"SLOTS", b"x"],
            b"ERR wrong number of arguments for 'cluster|slots' command",
        ),
        (
            &[b"CLUSTER", b"SHARDS", b"x"],
            b"ERR wrong number of arguments for 'cluster|shards' command",
        ),
        (
            &[b"CLUSTER", b"KEYSLOT"],
            b"ERR wrong number of arguments for 'cluster|keyslot' command",
        ),
        (
            &[b"CLUSTER", b"KEYSLOT", b"a", b"b"],
            b"ERR wrong number of arguments for 'cluster|keyslot' command",
        ),
    ];
    for (words, expected) in cases {
        let error = parse(words).err().expect("wrong arity must not parse");
        assert_eq!(
            error.response(),
            Response::Error(Bytes::from_static(expected)),
            "{:?}",
            words
                .iter()
                .map(|w| String::from_utf8_lossy(w))
                .collect::<Vec<_>>()
        );
    }
    // Valkey's QUIT ignores its arguments and answers OK.
    assert!(matches!(parse(&[b"QUIT", b"x"]), Ok(Command::Quit)));
}
