use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use clap::Subcommand;
use serde::Deserialize;
use uuid::Uuid;

use super::protocol::{Admin, CONTROL_FRAME, Member, Reply, Request, VERSION};
use super::transport::{Endpoint, PeerTls, TlsConfig};
use crate::cache::config::read_toml;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Assign empty slots after the initial workers enroll.
    Init {
        #[arg(long, default_value_t = 3)]
        groups: usize,
    },
    /// Read the committed topology.
    Status,
    /// Authorize a worker's certificate and advertised endpoints.
    Enroll {
        #[arg(long)]
        member: PathBuf,
    },
    /// Move an inclusive slot range to a member.
    Rebalance {
        #[arg(long)]
        first: u16,
        #[arg(long)]
        last: u16,
        #[arg(long)]
        target: u64,
    },
    /// Move a member's primary and replica assignments away before removal.
    Drain {
        #[arg(long)]
        worker: u64,
    },
    /// Authorize replacement endpoints or rotated certificates.
    Replace {
        #[arg(long)]
        member: PathBuf,
    },
    /// Explicitly discard a slot or all cache data after fencing prior owners.
    ColdReset {
        #[arg(long)]
        slot: Option<u16>,
        #[arg(long)]
        confirm_data_loss: bool,
    },
}

impl Command {
    fn into_request(self) -> io::Result<Admin> {
        Ok(match self {
            Self::Init { groups } => Admin::Init { groups },
            Self::Status => Admin::Status,
            Self::Enroll { member } => Admin::Enroll {
                member: read_member(&member)?,
            },
            Self::Rebalance {
                first,
                last,
                target,
            } => Admin::Rebalance {
                first,
                last,
                target,
            },
            Self::Drain { worker } => Admin::Drain { worker },
            Self::Replace { member } => Admin::Replace {
                member: read_member(&member)?,
            },
            Self::ColdReset {
                slot,
                confirm_data_loss: true,
            } => Admin::ColdReset { slot },
            Self::ColdReset {
                confirm_data_loss: false,
                ..
            } => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--confirm-data-loss is required",
                ));
            }
        })
    }
}

fn read_member(path: &Path) -> io::Result<Member> {
    read_toml(path)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    tls: TlsConfig,
    #[serde(deserialize_with = "super::transport::controller_endpoints")]
    controllers: BTreeMap<u64, Endpoint>,
}

pub fn run(path: &Path, operation: Option<Uuid>, command: Command) -> io::Result<()> {
    let id = operation.unwrap_or_else(nabedra_rt::ids::uuid_v4);
    let command = command.into_request()?;
    let config: Config = read_toml(path)?;
    if config.controllers.len() != 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "exactly three controller voters are required",
        ));
    }
    let root = path.parent().unwrap_or_else(|| Path::new("."));
    let tls = PeerTls::load(&config.tls, root)?;
    let request = Request::Admin {
        version: VERSION,
        operation: id,
        command,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            for endpoint in config.controllers.values() {
                let Ok(mut peer) = tls.connect(endpoint).await else {
                    continue;
                };
                match peer.json::<_, Reply>(&request, CONTROL_FRAME).await {
                    Ok(Reply::Refused(message)) => return Err(io::Error::other(message)),
                    Ok(reply @ Reply::Status(_)) | Ok(reply @ Reply::Applied { .. }) => {
                        serde_json::to_writer(io::stdout().lock(), &reply)?;
                        println!();
                        return Ok(());
                    }
                    _ => {}
                }
            }
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("no controller accepted operation {id}"),
            ))
        })
}
