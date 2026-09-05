//! The pipeline config: data model, a parser, and a matching printer.
//!
//! Grammar (whitespace-insensitive, `#` starts a comment line):
//!
//! ```text
//! job build {
//!   needs fetch, lint
//!   env RUST_LOG = debug
//!   step cargo build
//!   produces target/app
//!   cache {
//!     key v1
//!     paths src, Cargo.toml
//!   }
//!   continue-on-error
//! }
//! ```

use std::fmt;
use std::time::Duration;

use crate::error::Error;
use crate::safepath::is_confined;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cache {
    /// Optional label folded into the key so a user can force a bust.
    pub key: Option<String>,
    /// Declared input paths whose content decides the cache key.
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub name: String,
    pub needs: Vec<String>,
    /// Ordered key/value pairs (order is significant for hashing and round-trip).
    pub env: Vec<(String, String)>,
    pub steps: Vec<String>,
    /// Output file paths the executor collects as artifacts for dependents.
    pub produces: Vec<String>,
    pub cache: Option<Cache>,
    pub continue_on_error: bool,
    /// Wall-clock cap for the whole job, across all its steps. Complements the
    /// executor's per-step timeout. `None` means no job-level limit.
    pub timeout: Option<Duration>,
}

impl Job {
    pub fn new(name: impl Into<String>) -> Self {
        Job {
            name: name.into(),
            needs: Vec::new(),
            env: Vec::new(),
            steps: Vec::new(),
            produces: Vec::new(),
            cache: None,
            continue_on_error: false,
            timeout: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Pipeline {
    pub jobs: Vec<Job>,
}

impl Pipeline {
    pub fn parse(input: &str) -> Result<Pipeline, Error> {
        parse(input)
    }

    pub fn job(&self, name: &str) -> Option<&Job> {
        self.jobs.iter().find(|j| j.name == name)
    }
}

fn split_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn parse(input: &str) -> Result<Pipeline, Error> {
    let lines: Vec<(usize, String)> = input
        .lines()
        .enumerate()
        .map(|(i, l)| (i + 1, l.trim().to_string()))
        .collect();

    let mut jobs: Vec<Job> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let (ln, line) = &lines[i];
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        let rest = line
            .strip_prefix("job ")
            .ok_or_else(|| Error::parse(*ln, format!("expected a job definition, found: {line}")))?;
        let name = rest
            .trim()
            .strip_suffix('{')
            .map(str::trim)
            .ok_or_else(|| Error::parse(*ln, "expected '{' after the job name"))?;
        if name.is_empty() {
            return Err(Error::parse(*ln, "missing job name"));
        }
        if name.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(Error::parse(
                *ln,
                format!("invalid job name '{name}': names may not contain whitespace or control characters"),
            ));
        }
        if jobs.iter().any(|j| j.name == name) {
            return Err(Error::DuplicateJob(name.to_string()));
        }
        let (job, next) = parse_job(name, &lines, i + 1)?;
        jobs.push(job);
        i = next;
    }
    Ok(Pipeline { jobs })
}

fn parse_job(name: &str, lines: &[(usize, String)], mut i: usize) -> Result<(Job, usize), Error> {
    let mut job = Job::new(name);
    while i < lines.len() {
        let (ln, line) = &lines[i];
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        if line == "}" {
            return Ok((job, i + 1));
        }
        if let Some(r) = line.strip_prefix("needs ") {
            job.needs = split_list(r);
        } else if let Some(r) = line.strip_prefix("env ") {
            let (k, v) = r
                .split_once('=')
                .ok_or_else(|| Error::parse(*ln, "env requires 'KEY = VALUE'"))?;
            job.env.push((k.trim().to_string(), v.trim().to_string()));
        } else if let Some(r) = line.strip_prefix("step ") {
            job.steps.push(r.trim().to_string());
        } else if let Some(r) = line.strip_prefix("produces ") {
            let paths = split_list(r);
            for p in &paths {
                if !is_confined(p) {
                    return Err(Error::parse(
                        *ln,
                        format!("produces path '{p}' escapes the workspace (absolute or '..' not allowed)"),
                    ));
                }
            }
            job.produces = paths;
        } else if let Some(r) = line.strip_prefix("timeout ") {
            let secs: u64 = r
                .trim()
                .parse()
                .map_err(|_| Error::parse(*ln, format!("timeout requires whole seconds, found: {}", r.trim())))?;
            job.timeout = Some(Duration::from_secs(secs));
        } else if line == "continue-on-error" {
            job.continue_on_error = true;
        } else if line == "cache {" || line == "cache{" {
            let (cache, next) = parse_cache(lines, i + 1)?;
            job.cache = Some(cache);
            i = next;
            continue;
        } else {
            return Err(Error::parse(*ln, format!("unknown directive in job '{name}': {line}")));
        }
        i += 1;
    }
    Err(Error::parse(
        lines.last().map(|(n, _)| *n).unwrap_or(0),
        format!("unterminated job '{name}' (missing '}}')"),
    ))
}

fn parse_cache(lines: &[(usize, String)], mut i: usize) -> Result<(Cache, usize), Error> {
    let mut cache = Cache::default();
    while i < lines.len() {
        let (ln, line) = &lines[i];
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        if line == "}" {
            return Ok((cache, i + 1));
        }
        if let Some(r) = line.strip_prefix("key ") {
            cache.key = Some(r.trim().to_string());
        } else if let Some(r) = line.strip_prefix("paths ") {
            let paths = split_list(r);
            for p in &paths {
                if !is_confined(p) {
                    return Err(Error::parse(
                        *ln,
                        format!("cache path '{p}' escapes the workspace (absolute or '..' not allowed)"),
                    ));
                }
            }
            cache.paths = paths;
        } else {
            return Err(Error::parse(*ln, format!("unknown cache directive: {line}")));
        }
        i += 1;
    }
    Err(Error::parse(0, "unterminated cache block (missing '}')"))
}

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, job) in self.jobs.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write_job(f, job)?;
        }
        Ok(())
    }
}

fn write_job(f: &mut fmt::Formatter<'_>, job: &Job) -> fmt::Result {
    writeln!(f, "job {} {{", job.name)?;
    if !job.needs.is_empty() {
        writeln!(f, "  needs {}", job.needs.join(", "))?;
    }
    for (k, v) in &job.env {
        writeln!(f, "  env {k} = {v}")?;
    }
    for step in &job.steps {
        writeln!(f, "  step {step}")?;
    }
    if !job.produces.is_empty() {
        writeln!(f, "  produces {}", job.produces.join(", "))?;
    }
    if let Some(timeout) = job.timeout {
        writeln!(f, "  timeout {}", timeout.as_secs())?;
    }
    if let Some(cache) = &job.cache {
        writeln!(f, "  cache {{")?;
        if let Some(key) = &cache.key {
            writeln!(f, "    key {key}")?;
        }
        if !cache.paths.is_empty() {
            writeln!(f, "    paths {}", cache.paths.join(", "))?;
        }
        writeln!(f, "  }}")?;
    }
    if job.continue_on_error {
        writeln!(f, "  continue-on-error")?;
    }
    writeln!(f, "}}")
}
