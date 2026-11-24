//! Gates at real run sync, index backfill and snapshot chunk acknowledgements.
use std::{
    cell::Cell,
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

thread_local! {
    static FIXTURE_FLUSH: Cell<bool> = const { Cell::new(false) };
}

pub fn directory() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    path.join("enable-maintenance-observation")
        .is_file()
        .then_some(path)
}

fn reached(directory: &Path, stage: &str, bytes: &[u8]) -> bool {
    if !directory.join(format!("hold-{stage}")).is_file() {
        return false;
    }
    let marker = directory.join(format!("reached-{stage}"));
    // Claim just one instance of each stage, even if a second compaction
    // or retry arrives while the first one is paused.
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
    {
        Ok(mut file) => {
            use std::io::Write as _;
            file.write_all(bytes).unwrap();
            true
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
        Err(error) => panic!("write maintenance checkpoint {stage}: {error}"),
    }
}

pub(super) fn hold_run_sync(directory: &Path, bytes: u64) -> io::Result<()> {
    let current = std::thread::current();
    let stage = match current.name() {
        Some("kv-compact") => "compaction",
        Some("kv-flush") => "flush",
        _ if FIXTURE_FLUSH.get() => "flush",
        _ => return Ok(()),
    };
    if reached(directory, stage, bytes.to_string().as_bytes()) {
        let release = directory.join(format!("release-{stage}"));
        let deadline = nabedra_rt::time::now_std() + Duration::from_secs(90);
        while !release.is_file() {
            if nabedra_rt::time::now_std() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "O05 run sync gate"));
            }
            nabedra_rt::time::thread_sleep(Duration::from_millis(5));
        }
    }
    Ok(())
}

pub async fn hold_snapshot(group: Uuid, transferred: u64, total: u64) {
    hold(
        format!("snapshot-{group}"),
        &serde_json::to_vec(&(transferred, total)).unwrap(),
    )
    .await;
}

pub(crate) async fn hold_index_page(index: Uuid, rows: usize) {
    hold(format!("index-{index}"), rows.to_string().as_bytes()).await;
}

async fn hold(stage: String, observed: &[u8]) {
    let Some(directory) = directory() else { return };
    if reached(&directory, &stage, observed) {
        let release = directory.join(format!("release-{stage}"));
        nabedra_rt::time::timeout(Duration::from_secs(90), async {
            while !release.is_file() {
                nabedra_rt::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("maintenance checkpoint watchdog");
    }
}

pub struct FlushMarker;

impl FlushMarker {
    pub fn new() -> Self {
        FIXTURE_FLUSH.set(true);
        Self
    }
}

impl Drop for FlushMarker {
    fn drop(&mut self) {
        FIXTURE_FLUSH.set(false);
    }
}
