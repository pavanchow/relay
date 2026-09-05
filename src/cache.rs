//! Content-hash caching. A job's key is a hand-rolled FNV-1a digest of its
//! config plus the content of its declared input paths. A matching stored entry
//! means the job is CACHED and never runs; its recorded outputs are restored.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::executor::Artifacts;
use crate::hash::Hasher;
use crate::pipeline::Job;
use crate::safepath::is_confined;

/// Deterministic key for a job in isolation, folding no upstream contribution.
/// Kept for callers that key a single job; the scheduler uses
/// [`compute_key_with_deps`] so an upstream change busts the downstream key.
pub fn compute_key(job: &Job, base: &Path) -> u64 {
    compute_key_with_deps(job, base, &Artifacts::new(), &[])
}

/// Deterministic key for a job, made upstream-aware. It folds the job's own
/// config and declared cache-path content, plus every dependency's resolved key
/// and the merged `inputs` artifacts it received. So when a non-cached upstream
/// job's output changes, this dependent's key changes and its stale cache entry
/// is not reused.
pub fn compute_key_with_deps(job: &Job, base: &Path, inputs: &Artifacts, dep_keys: &[u64]) -> u64 {
    let mut h = Hasher::new();
    h.write_str("relay-cache-v1");
    h.write_str(&job.name);
    for step in &job.steps {
        h.write_str("step");
        h.write_str(step);
    }
    for (k, v) in &job.env {
        h.write_str("env");
        h.write_str(k);
        h.write_str(v);
    }
    for need in &job.needs {
        h.write_str("needs");
        h.write_str(need);
    }
    h.write_u64(job.continue_on_error as u64);
    if let Some(timeout) = job.timeout {
        h.write_str("timeout");
        h.write_u64(timeout.as_secs());
    }

    // Upstream awareness: each dependency's resolved key, then the content of the
    // artifacts this job received from its dependencies.
    for &dk in dep_keys {
        h.write_str("dep-key");
        h.write_u64(dk);
    }
    for (name, bytes) in inputs.iter() {
        h.write_str("input");
        h.write_str(name);
        h.write_u64(bytes.len() as u64);
        h.write(bytes);
    }

    if let Some(cache) = &job.cache {
        if let Some(key) = &cache.key {
            h.write_str("label");
            h.write_str(key);
        }
        let mut paths = cache.paths.clone();
        paths.sort();
        for p in &paths {
            h.write_str("path");
            // A path that escapes base_dir must never read or hash outside it.
            // Valid pipelines reject these at parse time; this guards direct API use.
            if is_confined(p) {
                h.write_str(p);
                hash_path(&mut h, &base.join(p));
            } else {
                h.write_str("unsafe-path");
            }
        }
    }
    h.finish()
}

fn hash_path(h: &mut Hasher, path: &Path) {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => match fs::read(path) {
            Ok(bytes) => {
                h.write_str("file");
                h.write_u64(bytes.len() as u64);
                h.write(&bytes);
            }
            Err(_) => h.write_str("unreadable"),
        },
        Ok(meta) if meta.is_dir() => {
            h.write_str("dir");
            let mut entries: Vec<PathBuf> = match fs::read_dir(path) {
                Ok(rd) => rd.filter_map(|e| e.ok().map(|e| e.path())).collect(),
                Err(_) => {
                    h.write_str("unreadable");
                    return;
                }
            };
            entries.sort();
            for entry in entries {
                h.write_str(&entry.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default());
                hash_path(h, &entry);
            }
        }
        _ => h.write_str("missing"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub artifacts: Vec<(String, Vec<u8>)>,
    pub logs: String,
}

/// In-memory store, optionally backed by a directory so the CLI keeps cache
/// across invocations. Tests use the in-memory form and run twice.
pub struct CacheStore {
    entries: HashMap<u64, CacheEntry>,
    dir: Option<PathBuf>,
}

impl CacheStore {
    pub fn in_memory() -> Self {
        CacheStore { entries: HashMap::new(), dir: None }
    }

    pub fn on_disk(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(CacheStore { entries: HashMap::new(), dir: Some(dir) })
    }

    pub fn get(&mut self, key: u64) -> Option<CacheEntry> {
        if let Some(entry) = self.entries.get(&key) {
            return Some(entry.clone());
        }
        if let Some(dir) = &self.dir {
            if let Ok(entry) = read_entry(&dir.join(format!("{key:016x}.entry"))) {
                self.entries.insert(key, entry.clone());
                return Some(entry);
            }
        }
        None
    }

    pub fn put(&mut self, key: u64, entry: CacheEntry) {
        if let Some(dir) = &self.dir {
            let _ = write_entry(&dir.join(format!("{key:016x}.entry")), &entry);
        }
        self.entries.insert(key, entry);
    }
}

impl Default for CacheStore {
    fn default() -> Self {
        CacheStore::in_memory()
    }
}

// Simple length-prefixed binary format. Hand-rolled to avoid a serde dependency.
fn write_entry(path: &Path, entry: &CacheEntry) -> io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entry.artifacts.len() as u32).to_le_bytes());
    for (name, data) in &entry.artifacts {
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
        buf.extend_from_slice(data);
    }
    let logs = entry.logs.as_bytes();
    buf.extend_from_slice(&(logs.len() as u64).to_le_bytes());
    buf.extend_from_slice(logs);
    let mut f = fs::File::create(path)?;
    f.write_all(&buf)
}

fn read_entry(path: &Path) -> io::Result<CacheEntry> {
    let mut f = fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let mut r = Reader { buf: &buf, pos: 0 };
    let count = r.u32()? as usize;
    let mut artifacts = Vec::with_capacity(count);
    for _ in 0..count {
        let name_len = r.u32()? as usize;
        let name = String::from_utf8_lossy(r.take(name_len)?).into_owned();
        let data_len = r.u64()? as usize;
        let data = r.take(data_len)?.to_vec();
        artifacts.push((name, data));
    }
    let logs_len = r.u64()? as usize;
    let logs = String::from_utf8_lossy(r.take(logs_len)?).into_owned();
    Ok(CacheEntry { artifacts, logs })
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated cache entry"));
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u32(&mut self) -> io::Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> io::Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
}
