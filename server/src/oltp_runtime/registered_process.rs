//! Native worker process for an explicitly provisioned fixed topology.
use super::{read_config, worker_runtime::WorkerRuntime};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp::pool::SharedResourceConfig,
    oltp_replication::manager::RegisteredConfig,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    bind: SocketAddr,
    auth_file: PathBuf,
    node: RegisteredConfig,
    resources: WorkerResources,
    execution: ExecutionConfig,
    #[serde(default)]
    automatic_split: Option<crate::oltp_replication::manager::shard_split::AutomaticSplitPolicy>,
    #[serde(default)]
    native_group_capacity: Option<NativeGroupCapacity>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeGroupCapacity {
    RetainedSplitHistory,
}

const GROUP_PROFILE_MIN_MEMORY: u64 = 3 << 30;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Auth {
    admin_token: String,
    data_token: String,
    enrollment_token: String,
}
pub async fn run(path: PathBuf) -> Result<(), &'static str> {
    let config: Config =
        read_config(&path).map_err(|_| "registered worker configuration invalid")?;
    if config.version != 1 {
        return Err("registered worker version unsupported");
    }
    if config.automatic_split.is_some_and(|policy| {
        config.node.mount.is_none() || policy.validate().is_err()
    }) {
        return Err("automatic shard splitting requires a mounted pool and a valid policy");
    }
    let native = native_resources(&config).map_err(report_startup_error)?;
    #[cfg(feature = "sim")]
    let native = super::sim::limits().map_or(native, |limits| SharedResourceConfig {
        data_apply_reply_bytes: limits.data_apply_reply_bytes,
        ..native
    });
    let auth: Auth = super::api_process::protected_json(&config.auth_file)
        .map_err(|_| "worker authentication unreadable")?;
    let tokens = [&auth.admin_token, &auth.data_token, &auth.enrollment_token];
    if tokens.iter().any(|token| {
        !(43..=4096).contains(&token.len()) || !token.bytes().all(|byte| (33..=126).contains(&byte))
    }) || auth.admin_token == auth.data_token
        || auth.admin_token == auth.enrollment_token
        || auth.data_token == auth.enrollment_token
    {
        return Err("worker authentication invalid");
    }
    let runtime = WorkerRuntime::open_registered(
        config.node,
        &auth.data_token,
        config.execution,
        config.resources,
        native,
    )
    .await
    .map_err(report_startup_error)?;
    // RANGES-2: the owner loop runs without an automatic policy too. It then
    // sweeps no group and only resumes the open split attempts that Timestamps
    // lists (manual ones included).
    let automatic_split = runtime.spawn_automatic_split(config.automatic_split)
        .map_err(|_| "automatic shard split startup failed")?;
    #[cfg(test)]
    if let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_ARCHIVE_SPLIT_INDEX") {
        nabedra_rt::task::spawn(super::recovery_tests::build_archive_split_index(
            runtime.clone(),
            directory.into(),
        ));
    }
    #[cfg(test)]
    if let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_INDEX_FAULT_DIR") {
        nabedra_rt::task::spawn(super::recovery_tests::index_faults::serve(
            runtime.clone(),
            directory.into(),
        ));
    }
    let result = async {
        let listener = super::api_process::bind_http(config.bind)
            .await
            .map_err(|_| "worker HTTP bind failed")?;
        let application = runtime
            .application_with_admin(auth.data_token, auth.admin_token)
            .map_err(|_| "worker application invalid")?;
        #[cfg(feature = "sim")]
        if let Some(stop) = super::sim::worker_stop() {
            return crate::network::serve_router(listener, application, None, async {
                let _ = stop.await;
                runtime.stop_accepting();
            })
            .await
            .map_err(|_| "worker HTTP server failed");
        }
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| "worker signal failed")?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|_| "worker signal failed")?;
        crate::network::serve_router(listener, application, None, async {
            tokio::select! {_=terminate.recv()=>{},_=interrupt.recv()=>{}}
            runtime.stop_accepting();
        })
        .await
        .map_err(|_| "worker HTTP server failed")
    }
    .await;
    runtime.stop_accepting();
    if let Some(actor) = automatic_split {
        actor.await.map_err(|_| "automatic shard split actor failed")?;
    }
    let drained = runtime.drain().await.map_err(|_| "worker drain failed");
    result.and(drained)
}

fn native_resources(config: &Config) -> Result<SharedResourceConfig, ProtocolError> {
    let Some(NativeGroupCapacity::RetainedSplitHistory) = config.native_group_capacity else {
        return Ok(SharedResourceConfig::default());
    };
    if config.node.mount.is_none() {
        return Err(invalid_profile(
            "native group capacity requires a registered mount",
        ));
    }
    config
        .resources
        .validate_execution(&config.execution)
        .map_err(|_| invalid_profile("invalid registered worker execution resources"))?;
    let (memory, pids) = cgroup_envelope()?;
    validate_profile_envelope(config.resources, memory, pids)?;
    Ok(SharedResourceConfig {
        max_resident_groups: 272,
        max_group_handles: 320,
        precharge_group_capacity: true,
        ..Default::default()
    })
}

fn validate_profile_envelope(
    resources: WorkerResources,
    memory: Option<u64>,
    pids: Option<u64>,
) -> Result<(), ProtocolError> {
    if resources.memory_limit_bytes < GROUP_PROFILE_MIN_MEMORY
        || memory.is_none_or(|limit| limit < resources.memory_limit_bytes)
    {
        return Err(invalid_profile(
            "native group capacity requires at least 3 GiB of worker memory within a finite cgroup v2 memory.max",
        ));
    }
    // The profile uses the existing executor and resolver thread counts. Keep
    // the fixture's process allowance and include any additional query threads.
    let minimum_pids = resources
        .max_threads
        .checked_add(95)
        .ok_or_else(|| invalid_profile("native group capacity PID allowance overflow"))?;
    if pids.is_none_or(|limit| limit < minimum_pids) {
        return Err(invalid_profile(
            "native group capacity requires finite cgroup v2 pids.max with at least 95 plus the query thread count",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn cgroup_envelope() -> Result<(Option<u64>, Option<u64>), ProtocolError> {
    use std::{
        fs,
        io::ErrorKind,
        path::{Component, Path},
    };

    let unreadable = || invalid_profile("native group capacity requires readable cgroup v2 limits");
    let membership = fs::read_to_string("/proc/self/cgroup").map_err(|_| unreadable())?;
    let group = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::/"))
        .ok_or_else(unreadable)?;
    let group = Path::new(group);
    if group
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(unreadable());
    }
    let root = Path::new("/sys/fs/cgroup");
    let current = root.join(group);
    let mut limits = [None, None];
    for ancestor in current
        .ancestors()
        .take_while(|path| path.starts_with(root))
    {
        for (file, limit) in ["memory.max", "pids.max"].into_iter().zip(&mut limits) {
            let text = match fs::read_to_string(ancestor.join(file)) {
                Ok(text) => text,
                Err(error) if ancestor == root && error.kind() == ErrorKind::NotFound => continue,
                Err(_) => return Err(unreadable()),
            };
            if text.trim() != "max" {
                let bound: u64 = text.trim().parse().map_err(|_| unreadable())?;
                *limit = Some(limit.map_or(bound, |old: u64| old.min(bound)));
            }
        }
    }
    Ok((limits[0], limits[1]))
}

#[cfg(not(target_os = "linux"))]
fn cgroup_envelope() -> Result<(Option<u64>, Option<u64>), ProtocolError> {
    Err(invalid_profile(
        "native group capacity requires Linux cgroup v2",
    ))
}

fn invalid_profile(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}

fn report_startup_error(error: ProtocolError) -> &'static str {
    eprintln!(
        "nabedra-db: registered startup {:?}: {}",
        error.code, error.message
    );
    "registered worker startup failed"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c02_registered_group_profile_requires_declared_memory_and_cgroup_limits() {
        assert!(matches!(
            serde_json::from_str(r#""retained_split_history""#).unwrap(),
            NativeGroupCapacity::RetainedSplitHistory
        ));
        assert!(serde_json::from_str::<NativeGroupCapacity>(r#""unlimited""#).is_err());
        let resources = WorkerResources {
            max_threads: 1,
            memory_limit_bytes: GROUP_PROFILE_MIN_MEMORY,
            task_memory_limit_bytes: 256 << 20,
            control_memory_reserve_bytes: 128 << 20,
        };
        validate_profile_envelope(resources, Some(GROUP_PROFILE_MIN_MEMORY), Some(96)).unwrap();
        for (memory, pids) in [
            (None, Some(96)),
            (Some(GROUP_PROFILE_MIN_MEMORY - 1), Some(96)),
            (Some(GROUP_PROFILE_MIN_MEMORY), None),
            (Some(GROUP_PROFILE_MIN_MEMORY), Some(95)),
        ] {
            let error = validate_profile_envelope(resources, memory, pids).unwrap_err();
            assert_eq!(error.code, ErrorCode::ValidationError);
            assert_eq!(error.outcome, Outcome::NotApplied);
        }
        let underdeclared = WorkerResources {
            memory_limit_bytes: GROUP_PROFILE_MIN_MEMORY - 1,
            ..resources
        };
        assert!(
            validate_profile_envelope(underdeclared, Some(GROUP_PROFILE_MIN_MEMORY), Some(96))
                .is_err()
        );
    }
}
