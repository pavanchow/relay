//! A scripted executor for deterministic tests. It spawns no processes; it just
//! returns the outcome you told it to and records what the scheduler did:
//! start order, and the peak number of jobs live at once (proving the
//! concurrency limit and real parallelism without any threads).

use std::collections::HashMap;
use std::time::Duration;

use crate::executor::{Artifacts, Executor, JobOutcome};
use crate::pipeline::Job;

#[derive(Clone)]
struct Script {
    success: bool,
    duration: Duration,
    artifacts: Vec<(String, Vec<u8>)>,
}

pub struct MockExecutor {
    scripts: HashMap<String, Script>,
    default_duration: Duration,
    live: usize,
    running: Vec<String>,
    /// Peak number of jobs live at the same instant.
    pub max_concurrency: usize,
    /// The set of jobs live when the peak was hit.
    pub peak_running: Vec<String>,
    /// Names in the order the scheduler started them.
    pub started: Vec<String>,
    /// Names the scheduler actually executed (never includes cached/skipped).
    pub ran: Vec<String>,
}

impl MockExecutor {
    pub fn new() -> Self {
        MockExecutor {
            scripts: HashMap::new(),
            default_duration: Duration::from_millis(1),
            live: 0,
            running: Vec::new(),
            max_concurrency: 0,
            peak_running: Vec::new(),
            started: Vec::new(),
            ran: Vec::new(),
        }
    }

    pub fn succeed(mut self, name: &str, duration_ms: u64) -> Self {
        self.scripts.insert(
            name.to_string(),
            Script { success: true, duration: Duration::from_millis(duration_ms), artifacts: Vec::new() },
        );
        self
    }

    pub fn fail(mut self, name: &str, duration_ms: u64) -> Self {
        self.scripts.insert(
            name.to_string(),
            Script { success: false, duration: Duration::from_millis(duration_ms), artifacts: Vec::new() },
        );
        self
    }

    pub fn produce(mut self, name: &str, artifact: &str, bytes: &[u8]) -> Self {
        let entry = self.scripts.entry(name.to_string()).or_insert(Script {
            success: true,
            duration: self.default_duration,
            artifacts: Vec::new(),
        });
        entry.artifacts.push((artifact.to_string(), bytes.to_vec()));
        self
    }

    pub fn ran_job(&self, name: &str) -> bool {
        self.ran.iter().any(|n| n == name)
    }
}

impl Default for MockExecutor {
    fn default() -> Self {
        MockExecutor::new()
    }
}

impl Executor for MockExecutor {
    fn run(&mut self, job: &Job, _inputs: &Artifacts) -> JobOutcome {
        self.live += 1;
        self.running.push(job.name.clone());
        if self.live > self.max_concurrency {
            self.max_concurrency = self.live;
            self.peak_running = self.running.clone();
        }
        self.started.push(job.name.clone());
        self.ran.push(job.name.clone());

        let script = self.scripts.get(&job.name).cloned().unwrap_or(Script {
            success: true,
            duration: self.default_duration,
            artifacts: Vec::new(),
        });

        let mut artifacts = Artifacts::new();
        for (k, v) in script.artifacts {
            artifacts.insert(k, v);
        }

        JobOutcome {
            success: script.success,
            exit_code: Some(if script.success { 0 } else { 1 }),
            logs: String::new(),
            artifacts,
            duration: script.duration,
        }
    }

    fn on_finish(&mut self, job: &str) {
        if self.live > 0 {
            self.live -= 1;
        }
        if let Some(pos) = self.running.iter().position(|n| n == job) {
            self.running.remove(pos);
        }
    }
}
