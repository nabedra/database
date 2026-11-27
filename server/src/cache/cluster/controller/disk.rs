use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

const HEADER_BYTES: usize = 4 + 32;
const MAX_RECORD: usize = 8 << 20;

pub(super) fn write_record<W: Write>(writer: &mut W, value: &impl Serialize) -> io::Result<()> {
    let data = super::super::transport::bounded_json(value, MAX_RECORD)?;
    let len = u32::try_from(data.len()).map_err(io::Error::other)?;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(&Sha256::digest(&data))?;
    writer.write_all(&data)
}

pub(super) enum ReadRecord<T> {
    Complete(T),
    End,
    Partial,
}

pub(super) fn read_record<T: DeserializeOwned>(
    reader: &mut impl Read,
) -> io::Result<ReadRecord<T>> {
    let mut header = [0; HEADER_BYTES];
    match reader.read_exact(&mut header[..1]) {
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(ReadRecord::End),
        Err(error) => return Err(error),
        Ok(()) => {}
    }
    if !read_tail(reader, &mut header[1..])? {
        return Ok(ReadRecord::Partial);
    }
    let len = u32::from_be_bytes(header[..4].try_into().expect("four length bytes")) as usize;
    if !(1..=MAX_RECORD).contains(&len) {
        return Err(invalid("invalid metadata record length"));
    }

    let mut data = vec![0; len];
    if !read_tail(reader, &mut data)? {
        return Ok(ReadRecord::Partial);
    }
    let digest = Sha256::digest(&data);
    if digest[..] != header[4..] {
        return Err(invalid("metadata checksum mismatch"));
    }
    serde_json::from_slice(&data)
        .map(ReadRecord::Complete)
        .map_err(io::Error::other)
}

fn read_tail(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<bool> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) fn read_atomic<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let mut file = match File::open(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
        Ok(file) => file,
    };
    match read_record(&mut file)? {
        ReadRecord::Complete(value) if matches!(read_record::<T>(&mut file)?, ReadRecord::End) => {
            Ok(Some(value))
        }
        _ => Err(invalid("invalid atomic metadata file")),
    }
}

pub(super) fn replace(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<File> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("metadata directory missing"))?;
    let tmp = parent.join(format!(".metadata-{}", nabedra_rt::ids::uuid_v4()));
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&tmp)?;
    let result = (|| {
        let mut file = file;
        write(&mut file)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(file)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// Creates the controller's metadata directory and makes it durable on every
/// open: its own entries, its entry in its parent and each ancestor's entry up
/// the path. A power loss right after an earlier open's `create_dir_all`, or
/// after a first open that crashed before syncing, could otherwise drop the
/// whole directory with the WAL and the acknowledged decisions in it.
pub(super) fn durable_dir(fs: &dyn nabedra_rt::fs::Fs, dir: &Path) -> io::Result<()> {
    fs.create_dir_all(dir)?;
    let mut current = Some(dir);
    while let Some(path) = current.filter(|path| !path.as_os_str().is_empty()) {
        fs.sync_dir(path)?;
        current = path.parent();
    }
    Ok(())
}

pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn k03_distinguishes_torn_tail_from_a_checksum_failure() {
        let mut storage = vec![];
        write_record(&mut storage, &"lease").unwrap();
        let len = storage.len();
        for cut in 1..len {
            assert!(matches!(
                read_record::<String>(&mut &storage[..cut]).unwrap(),
                ReadRecord::Partial
            ));
        }
        assert!(matches!(
            read_record::<String>(&mut storage.as_slice()).unwrap(),
            ReadRecord::Complete(s) if s == "lease"
        ));
        storage[len - 1] ^= 0x12;
        assert!(read_record::<String>(&mut storage.as_slice()).is_err());
    }

    /// RV-CACHE-controller-dir-sync (from fx-lsm's LSM-10 search): the shipped
    /// open created the directory and synced only the directory itself.
    #[test]
    fn k03_controller_directory_survives_a_power_loss_after_open() {
        use nabedra_rt::fs::{FaultFs, Fs, OpenMode};

        let wal = |fs: &FaultFs, dir: &Path| {
            let file = fs
                .open(&dir.join("topology.wal"), OpenMode::CreateNew)
                .unwrap();
            file.write_all_at(b"vote", 0).unwrap();
            file.sync_all().unwrap();
        };
        let dir = Path::new("/var/lib/nabedra/controller-1");

        // The shipped steps: create_dir_all, then sync the directory only.
        let shipped = FaultFs::new();
        shipped.create_dir_all(dir).unwrap();
        wal(&shipped, dir);
        shipped.sync_dir(dir).unwrap();
        assert!(
            !shipped.crash().exists(&dir.join("topology.wal")).unwrap(),
            "the shipped open keeps its WAL across a power loss"
        );

        let fixed = FaultFs::new();
        durable_dir(&fixed, dir).unwrap();
        wal(&fixed, dir);
        fixed.sync_dir(dir).unwrap();
        assert!(fixed.crash().exists(&dir.join("topology.wal")).unwrap());
        for seed in 0..64 {
            assert!(
                fixed
                    .power_loss(seed)
                    .exists(&dir.join("topology.wal"))
                    .unwrap()
            );
        }
    }
}
