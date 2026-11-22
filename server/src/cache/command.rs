//! RESP commands, key validation, and Redis Cluster slot mapping.
use bytes::Bytes;

use super::store::{Operation, SetWhen};
use super::wire::Response;

pub use super::store::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
pub const SLOT_COUNT: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandKind {
    Get,
    Set,
    Del,
    Exists,
    Mget,
    Ttl,
    Pttl,
    Expire,
    Pexpire,
    Persist,
    Incrby,
}

impl CommandKind {
    /// Valkey's lowercase command name, as its errors print it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "set",
            Self::Del => "del",
            Self::Exists => "exists",
            Self::Mget => "mget",
            Self::Ttl => "ttl",
            Self::Pttl => "pttl",
            Self::Expire => "expire",
            Self::Pexpire => "pexpire",
            Self::Persist => "persist",
            Self::Incrby => "incrby",
        }
    }

    fn parse(name: &[u8]) -> Option<Self> {
        DATA_METADATA
            .iter()
            .find_map(|(kind, metadata)| name.eq_ignore_ascii_case(metadata.name).then_some(*kind))
    }
}

pub struct Credentials {
    pub username: Option<Bytes>,
    pub password: Bytes,
}

pub enum Command {
    Auth(Credentials),
    Hello {
        auth: Option<Credentials>,
        name: Option<Bytes>,
    },
    Ping(Option<Bytes>),
    Echo(Bytes),
    ClientSetInfo,
    ClientSetName(Bytes),
    CommandInfo(Vec<Bytes>),
    CommandCount,
    CommandGetKeys(Vec<Bytes>),
    ClusterSlots,
    ClusterShards,
    ClusterKeySlot(Bytes),
    Quit,
    Data(DataCommand),
    /// Test-only hooks; see `test_hooks`.
    #[cfg(feature = "test-hooks")]
    Debug(super::test_hooks::DebugCommand),
}

pub struct DataCommand {
    kind: CommandKind,
    operation: Operation,
}

impl DataCommand {
    pub fn kind(&self) -> CommandKind {
        self.kind
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = &Bytes> {
        self.operation.keys()
    }

    /// Validates that the operation can execute on a single cluster slot.
    pub fn slot(&self) -> Result<u16, CommandError> {
        let mut keys = self.keys();
        let first = keys.next().expect("a data command always has a key");
        let slot = key_slot(first);
        if keys.any(|key| key_slot(key) != slot) {
            return Err(CommandError::CrossSlot);
        }
        Ok(slot)
    }

    pub fn into_operation(self) -> Operation {
        self.operation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandError {
    /// Wrong argument count for the command with this Valkey full name, such
    /// as `get` or `client|setname`.
    Arity(&'static str),
    /// An empty command frame.
    Empty,
    /// `COMMAND GETKEYS` embedding a command with the wrong argument count.
    GetKeysArity,
    /// An expiry that overflows, for the command with this Valkey name.
    InvalidExpireTime(&'static str),
    Syntax,
    Unsupported,
    InvalidInteger,
    InvalidExpiry,
    ProtocolVersion,
    InvalidClientName,
    InvalidKeySize,
    InvalidValueSize,
    NoKeys,
    CrossSlot,
}

impl CommandError {
    pub fn response(self) -> Response {
        Response::error(match self {
            Self::Arity(name) => {
                return Response::Error(Bytes::from(format!(
                    "ERR wrong number of arguments for '{name}' command"
                )));
            }
            Self::InvalidExpireTime(name) => {
                return Response::Error(Bytes::from(format!(
                    "ERR invalid expire time in '{name}' command"
                )));
            }
            Self::Empty => b"ERR wrong number of arguments",
            Self::GetKeysArity => b"ERR Invalid number of arguments specified for command",
            Self::Syntax => b"ERR syntax error",
            Self::Unsupported => b"ERR unknown or unsupported command",
            Self::InvalidInteger => b"ERR value is not an integer or out of range",
            Self::InvalidExpiry => b"ERR invalid expire time in 'set' command",
            Self::ProtocolVersion => b"NOPROTO unsupported protocol version",
            Self::InvalidClientName => b"ERR invalid client name or library identifier",
            Self::InvalidKeySize => b"ERR cache key exceeds 512 bytes",
            Self::InvalidValueSize => b"ERR cache value exceeds 1048576 bytes",
            Self::NoKeys => b"ERR The command has no key arguments",
            Self::CrossSlot => b"CROSSSLOT Keys in request don't hash to the same slot",
        })
    }
}

impl Command {
    pub fn parse(frame: Vec<Bytes>) -> Result<Self, CommandError> {
        let mut arguments = frame.into_iter();
        let Some(name) = arguments.next() else {
            return Err(CommandError::Empty);
        };
        if let Some(kind) = CommandKind::parse(&name) {
            return parse_data(kind, arguments).map(Self::Data);
        }
        match name.as_ref() {
            name if word(name, b"AUTH") => parse_auth(arguments).map(Self::Auth),
            name if word(name, b"HELLO") => parse_hello(arguments),
            name if word(name, b"PING") => match arguments.len() {
                0 | 1 => Ok(Self::Ping(arguments.next())),
                _ => Err(CommandError::Arity("ping")),
            },
            name if word(name, b"ECHO") => Ok(Self::Echo(exactly_one(arguments, "echo")?)),
            name if word(name, b"CLIENT") => parse_client(arguments),
            name if word(name, b"COMMAND") => parse_metadata(arguments),
            name if word(name, b"CLUSTER") => parse_cluster(arguments),
            // Valkey's QUIT takes any arguments and ignores them.
            name if word(name, b"QUIT") => Ok(Self::Quit),
            #[cfg(feature = "test-hooks")]
            name if word(name, b"DEBUG") => super::test_hooks::parse(arguments).map(Self::Debug),
            _ => Err(CommandError::Unsupported),
        }
    }

    pub fn metadata_response(&self, max_bytes: usize) -> Option<Response> {
        match self {
            Self::CommandCount => Some(Response::Integer(
                (DATA_METADATA.len() + CONTROL_METADATA.len()) as i64,
            )),
            Self::CommandInfo(names) => {
                let mut items = Vec::new();
                let mut size = 7; // Upper bound for an array header with at most 1,024 items.
                let mut push = |item: Response| {
                    size += item.encoded_len().expect("bounded command metadata");
                    if size > max_bytes {
                        return false;
                    }
                    items.push(item);
                    true
                };
                let fits = if names.is_empty() {
                    metadata().all(|metadata| push(metadata.response()))
                } else {
                    names.iter().all(|name| {
                        push(
                            metadata()
                                .find(|meta| word(name, meta.name))
                                .map_or(Response::NullArray, Metadata::response),
                        )
                    })
                };
                Some(if fits {
                    Response::Array(items)
                } else {
                    Response::error(b"ERR response exceeds cache output limit")
                })
            }
            Self::CommandGetKeys(keys) => Some(Response::Array(
                keys.iter()
                    .cloned()
                    .map(|key| Response::Bulk(Some(key)))
                    .collect(),
            )),
            _ => None,
        }
    }
}

fn exactly_one(
    mut arguments: std::vec::IntoIter<Bytes>,
    name: &'static str,
) -> Result<Bytes, CommandError> {
    if arguments.len() != 1 {
        return Err(CommandError::Arity(name));
    }
    Ok(arguments.next().unwrap())
}

fn parse_data(
    kind: CommandKind,
    mut positional: std::vec::IntoIter<Bytes>,
) -> Result<DataCommand, CommandError> {
    use CommandKind::*;
    let count = positional.len();
    let operation = match (kind, count) {
        (Get, 1) => Operation::Get(positional.next().unwrap()),
        (Set, 2..) => parse_set(positional)?,
        (Del, 1..) => Operation::Delete(positional.collect()),
        (Exists, 1..) => Operation::Exists(positional.collect()),
        (Mget, 1..) => Operation::GetMany(positional.collect()),
        (Ttl | Pttl, 1) => Operation::Ttl {
            key: positional.next().unwrap(),
            milliseconds: kind == Pttl,
        },
        (Expire | Pexpire, 2) => {
            let key = positional.next().unwrap();
            let ttl_ms = parse_integer(&positional.next().unwrap())?;
            // Valkey rejects seconds whose milliseconds overflow, either sign.
            let ttl_ms = if kind == Expire {
                ttl_ms
                    .checked_mul(1000)
                    .ok_or(CommandError::InvalidExpireTime("expire"))?
            } else {
                ttl_ms
            };
            if ttl_ms > max_relative_ttl_ms() {
                return Err(CommandError::InvalidExpireTime(kind.name()));
            }
            Operation::Expire { key, ttl_ms }
        }
        (Persist, 1) => Operation::Persist(positional.next().unwrap()),
        (Incrby, 2) => Operation::Increment {
            key: positional.next().unwrap(),
            delta: parse_integer(&positional.next().unwrap())?,
        },
        _ => return Err(CommandError::Arity(kind.name())),
    };
    let command = DataCommand { kind, operation };
    if command.keys().any(|key| key.len() > MAX_KEY_BYTES) {
        return Err(CommandError::InvalidKeySize);
    }
    Ok(command)
}

fn parse_set(mut args: std::vec::IntoIter<Bytes>) -> Result<Operation, CommandError> {
    let key = args.next().unwrap();
    let value = args.next().unwrap();
    if value.len() > MAX_VALUE_BYTES {
        return Err(CommandError::InvalidValueSize);
    }
    let mut when = SetWhen::Always;
    let mut ttl_ms = None;
    while let Some(flag) = args.next() {
        if matches!(when, SetWhen::Always) && word(&flag, b"NX") {
            when = SetWhen::Absent;
        } else if matches!(when, SetWhen::Always) && word(&flag, b"XX") {
            when = SetWhen::Present;
        } else if ttl_ms.is_none() && (word(&flag, b"EX") || word(&flag, b"PX")) {
            let value = args.next().ok_or(CommandError::Syntax)?;
            let value = parse_integer(&value)?;
            let ttl = if word(&flag, b"EX") {
                value.checked_mul(1000).ok_or(CommandError::InvalidExpiry)?
            } else {
                value
            };
            if ttl <= 0 || ttl > max_relative_ttl_ms() {
                return Err(CommandError::InvalidExpiry);
            }
            ttl_ms = Some(ttl as u64);
        } else {
            return Err(CommandError::Syntax);
        }
    }
    Ok(Operation::Set {
        key,
        value,
        ttl_ms,
        when,
    })
}

/// Valkey stores absolute Unix milliseconds and rejects a relative TTL that
/// would overflow `i64` when added to the current wall-clock time.
fn max_relative_ttl_ms() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    i64::MAX - i64::try_from(now).unwrap_or(i64::MAX)
}

fn parse_integer(input: &[u8]) -> Result<i64, CommandError> {
    let digits = input.strip_prefix(b"-").unwrap_or(input);
    if digits.is_empty()
        || digits.len() > 19
        || (digits[0] == b'0' && input != b"0")
        || !digits.iter().all(u8::is_ascii_digit)
    {
        return Err(CommandError::InvalidInteger);
    }
    std::str::from_utf8(input)
        .ok()
        .and_then(|input| input.parse().ok())
        .ok_or(CommandError::InvalidInteger)
}

fn parse_auth(mut arguments: std::vec::IntoIter<Bytes>) -> Result<Credentials, CommandError> {
    let (username, password) = match arguments.len() {
        1 => (None, arguments.next().unwrap()),
        2 => (arguments.next(), arguments.next().unwrap()),
        0 => return Err(CommandError::Arity("auth")),
        _ => return Err(CommandError::Syntax),
    };
    Ok(Credentials { username, password })
}

fn parse_hello(mut arguments: std::vec::IntoIter<Bytes>) -> Result<Command, CommandError> {
    if let Some(version) = arguments.next() {
        if parse_integer(&version)? != 2 {
            return Err(CommandError::ProtocolVersion);
        }
    }
    let mut auth = None;
    let mut name = None;
    while let Some(arg) = arguments.next() {
        if auth.is_none() && word(&arg, b"AUTH") {
            auth = Some(Credentials {
                username: Some(arguments.next().ok_or(CommandError::Syntax)?),
                password: arguments.next().ok_or(CommandError::Syntax)?,
            });
        } else if name.is_none() && word(&arg, b"SETNAME") {
            let client_name = arguments.next().ok_or(CommandError::Syntax)?;
            validate_client_name(&client_name)?;
            name = Some(client_name);
        } else {
            return Err(CommandError::Syntax);
        }
    }
    Ok(Command::Hello { auth, name })
}

fn parse_client(mut arguments: std::vec::IntoIter<Bytes>) -> Result<Command, CommandError> {
    let subcommand = arguments.next().ok_or(CommandError::Arity("client"))?;
    if word(&subcommand, b"SETINFO") {
        if arguments.len() != 2 {
            return Err(CommandError::Arity("client|setinfo"));
        }
        let field = arguments.next().unwrap();
        if !(word(&field, b"LIB-NAME") || word(&field, b"LIB-VER")) {
            return Err(CommandError::Unsupported);
        }
        validate_client_name(&arguments.next().unwrap())?;
        Ok(Command::ClientSetInfo)
    } else if word(&subcommand, b"SETNAME") {
        let name = exactly_one(arguments, "client|setname")?;
        validate_client_name(&name)?;
        Ok(Command::ClientSetName(name))
    } else {
        Err(CommandError::Unsupported)
    }
}

fn validate_client_name(name: &[u8]) -> Result<(), CommandError> {
    if name.len() > 128 || !name.iter().all(u8::is_ascii_graphic) {
        return Err(CommandError::InvalidClientName);
    }
    Ok(())
}

fn parse_metadata(mut arguments: std::vec::IntoIter<Bytes>) -> Result<Command, CommandError> {
    let Some(name) = arguments.next() else {
        return Ok(Command::CommandInfo(Vec::new()));
    };
    match name.as_ref() {
        name if word(name, b"INFO") => Ok(Command::CommandInfo(arguments.collect())),
        name if word(name, b"COUNT") && arguments.len() == 0 => Ok(Command::CommandCount),
        name if word(name, b"COUNT") => Err(CommandError::Arity("command|count")),
        name if word(name, b"GETKEYS") => {
            let embedded = arguments
                .next()
                .ok_or(CommandError::Arity("command|getkeys"))?;
            let kind = CommandKind::parse(&embedded).ok_or(CommandError::NoKeys)?;
            let command = parse_data(kind, arguments).map_err(|error| match error {
                CommandError::Arity(_) => CommandError::GetKeysArity,
                error => error,
            })?;
            Ok(Command::CommandGetKeys(command.keys().cloned().collect()))
        }
        _ => Err(CommandError::Unsupported),
    }
}

fn parse_cluster(mut arguments: std::vec::IntoIter<Bytes>) -> Result<Command, CommandError> {
    let subcommand = arguments.next().ok_or(CommandError::Arity("cluster"))?;
    match subcommand.as_ref() {
        subcommand if word(subcommand, b"SLOTS") && arguments.len() == 0 => {
            Ok(Command::ClusterSlots)
        }
        subcommand if word(subcommand, b"SHARDS") && arguments.len() == 0 => {
            Ok(Command::ClusterShards)
        }
        subcommand if word(subcommand, b"SLOTS") => Err(CommandError::Arity("cluster|slots")),
        subcommand if word(subcommand, b"SHARDS") => Err(CommandError::Arity("cluster|shards")),
        subcommand if word(subcommand, b"KEYSLOT") => {
            let key = exactly_one(arguments, "cluster|keyslot")?;
            if key.len() > MAX_KEY_BYTES {
                return Err(CommandError::InvalidKeySize);
            }
            Ok(Command::ClusterKeySlot(key))
        }
        _ => Err(CommandError::Unsupported),
    }
}

pub fn hello_response(id: i64, cluster: bool) -> Response {
    Response::Array(vec![
        Response::bulk(b"server"),
        Response::bulk(b"nabedra"),
        Response::bulk(b"version"),
        Response::bulk(env!("CARGO_PKG_VERSION").as_bytes()),
        Response::bulk(b"proto"),
        Response::Integer(2),
        Response::bulk(b"id"),
        Response::Integer(id),
        Response::bulk(b"mode"),
        Response::bulk(if cluster { b"cluster" } else { b"standalone" }),
        Response::bulk(b"role"),
        Response::bulk(b"master"),
        Response::bulk(b"modules"),
        Response::Array(Vec::new()),
    ])
}

struct Metadata {
    name: &'static [u8],
    arity: i64,
    last_key: i64,
    write: bool,
}

impl Metadata {
    const fn data(name: &'static [u8], arity: i64, last_key: i64, write: bool) -> Self {
        Self {
            name,
            arity,
            last_key,
            write,
        }
    }

    const fn control(name: &'static [u8], arity: i64) -> Self {
        Self {
            name,
            arity,
            last_key: 0,
            write: false,
        }
    }

    fn response(&self) -> Response {
        let first_key = i64::from(self.last_key != 0);
        let flags: &'static [u8] = if self.last_key == 0 {
            b"connection"
        } else if self.write {
            b"write"
        } else {
            b"readonly"
        };
        Response::Array(vec![
            Response::bulk(self.name),
            Response::Integer(self.arity),
            Response::Array(vec![Response::bulk(flags)]),
            Response::Integer(first_key),
            Response::Integer(self.last_key),
            Response::Integer(first_key),
            Response::Array(Vec::new()), // ACL categories
            Response::Array(Vec::new()), // Tips
            Response::Array(Vec::new()), // Key specifications
            Response::Array(Vec::new()), // Subcommands
        ])
    }
}

const DATA_METADATA: &[(CommandKind, Metadata)] = &[
    (CommandKind::Get, Metadata::data(b"get", 2, 1, false)),
    (CommandKind::Set, Metadata::data(b"set", -3, 1, true)),
    (CommandKind::Del, Metadata::data(b"del", -2, -1, true)),
    (
        CommandKind::Exists,
        Metadata::data(b"exists", -2, -1, false),
    ),
    (CommandKind::Mget, Metadata::data(b"mget", -2, -1, false)),
    (CommandKind::Ttl, Metadata::data(b"ttl", 2, 1, false)),
    (CommandKind::Pttl, Metadata::data(b"pttl", 2, 1, false)),
    (CommandKind::Expire, Metadata::data(b"expire", 3, 1, true)),
    (CommandKind::Pexpire, Metadata::data(b"pexpire", 3, 1, true)),
    (CommandKind::Persist, Metadata::data(b"persist", 2, 1, true)),
    (CommandKind::Incrby, Metadata::data(b"incrby", 3, 1, true)),
];

const CONTROL_METADATA: &[Metadata] = &[
    Metadata::control(b"auth", -2),
    Metadata::control(b"hello", -1),
    Metadata::control(b"ping", -1),
    Metadata::control(b"echo", 2),
    Metadata::control(b"client", -2),
    Metadata::control(b"command", -1),
    Metadata::control(b"cluster", -2),
    Metadata::control(b"quit", -1),
];

fn metadata() -> impl Iterator<Item = &'static Metadata> {
    DATA_METADATA
        .iter()
        .map(|(_, meta)| meta)
        .chain(CONTROL_METADATA)
}

fn word(input: &[u8], name: &[u8]) -> bool {
    input.eq_ignore_ascii_case(name)
}

const CRC16_TABLE: [u16; 256] = {
    let mut table = [0; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut crc = (byte as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc << 1) ^ (if crc & 0x8000 != 0 { 0x1021 } else { 0 });
            bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }
    table
};

pub fn key_slot(key: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &byte in hash_tag(key) {
        crc = (crc << 8) ^ CRC16_TABLE[usize::from((crc >> 8) as u8 ^ byte)];
    }
    crc & (SLOT_COUNT as u16 - 1)
}

fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&byte| byte == b'{') {
        let tail = &key[start + 1..];
        if let Some(end @ 1..) = tail.iter().position(|&byte| byte == b'}') {
            return &tail[..end];
        }
    }
    key
}

#[cfg(test)]
mod tests;
