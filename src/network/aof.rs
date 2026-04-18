use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{bail, Result};
use resp::Value;

use super::resp::{encode_into, resp_encoded_len};

pub struct AofWriter {
    file: Mutex<File>,
    fsync_always: bool,
}

impl AofWriter {
    /// Set up the AOF directory, active incr file, and manifest. If a manifest already
    /// exists, reuse its `type i` entry; otherwise create the default one.
    ///
    /// # Errors
    /// Returns an error if filesystem operations fail or the manifest is malformed.
    pub fn setup(
        dir: &Path,
        appenddirname: &str,
        appendfilename: &str,
        fsync_always: bool,
    ) -> Result<Self> {
        let aof_dir = dir.join(appenddirname);
        fs::create_dir_all(&aof_dir)?;

        let manifest_path = aof_dir.join(format!("{appendfilename}.manifest"));
        let incr_name = if manifest_path.exists() {
            read_active_incr(&manifest_path)?
        } else {
            let name = format!("{appendfilename}.1.incr.aof");
            fs::write(&manifest_path, format!("file {name} seq 1 type i\n"))?;
            name
        };

        let incr_path = aof_dir.join(&incr_name);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)?;

        Ok(AofWriter {
            file: Mutex::new(file),
            fsync_always,
        })
    }

    /// Encode a RESP `Value` and append it to the AOF file, fsyncing when configured.
    ///
    /// # Errors
    /// Returns an error if the write or fsync fails.
    pub fn append(&self, value: &Value) -> Result<()> {
        let mut buf = Vec::with_capacity(resp_encoded_len(value));
        encode_into(value, &mut buf);
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("AOF mutex poisoned"))?;
        file.write_all(&buf)?;
        if self.fsync_always {
            file.sync_data()?;
        }
        Ok(())
    }
}

fn read_active_incr(manifest_path: &PathBuf) -> Result<String> {
    let contents = fs::read_to_string(manifest_path)?;
    for line in contents.lines() {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        // file <name> seq <n> type <t>
        if tokens.len() >= 6 && tokens[0] == "file" && tokens[4] == "type" && tokens[5] == "i" {
            return Ok(tokens[1].to_string());
        }
    }
    bail!("no active incremental entry in manifest")
}
