//! Process-local tests that compose replication and the OLTP runtime.
mod control;
mod rpc;
mod status;
mod timestamp;

use crate::{
    oltp::pool::{ByteClass, ChargedBytes},
    oltp_replication::{
        authority::{
            control::{ControlClient, ServiceAction},
            status::*,
        },
        multiplex::*,
        owned::serialized_digest,
        service_rpc::*,
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;
