use std::{fs, io::ErrorKind, path::Path};

#[test]
fn replay_corpora_and_regressions() {
    #[cfg(feature = "cache")]
    {
        replay("cache_resp", nabedra_fuzz::fuzz_cache_resp);
        replay("cache_command", nabedra_fuzz::fuzz_cache_command);
    }
    #[cfg(feature = "events")]
    {
        replay("events_publish", nabedra_fuzz::fuzz_events_publish);
        replay("events_poll", nabedra_fuzz::fuzz_events_poll);
        replay("events_ack", nabedra_fuzz::fuzz_events_ack);
        replay("events_lease", nabedra_fuzz::fuzz_events_lease);
    }
    #[cfg(feature = "olap")]
    {
        replay("olap_operation", nabedra_fuzz::fuzz_olap_operation);
        replay("olap_predicate", nabedra_fuzz::fuzz_olap_predicate);
        replay("olap_catalog", nabedra_fuzz::fuzz_olap_catalog);
    }
}

fn replay(target: &str, fuzz: fn(&[u8])) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let corpus = root.join("corpus").join(target);
    assert!(replay_dir(&corpus, fuzz) > 0, "empty corpus for {target}");

    let regressions = root.join("regressions").join(target);
    match fs::metadata(&regressions) {
        Ok(_) => {
            replay_dir(&regressions, fuzz);
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => panic!("{}: {error}", regressions.display()),
    }
}

fn replay_dir(dir: &Path, fuzz: fn(&[u8])) -> usize {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("{}: {error}", dir.display()))
        .map(|entry| entry.expect("read replay directory entry").path())
        .collect();
    paths.sort_unstable();

    paths
        .iter()
        .map(|path| {
            let metadata = fs::symlink_metadata(path)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            if metadata.is_dir() {
                return replay_dir(path, fuzz);
            }
            assert!(
                metadata.is_file(),
                "expected a regular file at {}",
                path.display()
            );
            let data = fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            eprintln!("replaying {}", path.display());
            fuzz(&data);
            1
        })
        .sum()
}
