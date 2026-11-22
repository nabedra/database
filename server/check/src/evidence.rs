//! Evidence records. (Skeleton.)

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub schema: String,
    pub check: String,
    pub kind: String,
}

impl Record {
    pub fn new(check: &str, kind: &str) -> Self {
        Self { schema: "nabedra.evidence/1".into(), check: check.into(), kind: kind.into() }
    }

    pub fn write_to(&self, dir: &Path) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("local--{}.json", self.check));
        std::fs::write(&path, serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?)?;
        Ok(path)
    }
}
