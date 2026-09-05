//! The boundary between the pure scheduler and the messy outside world.
//!
//! The scheduler never spawns a process. It calls `Executor::run` for each job
//! and reads back a `JobOutcome`. The real `ShellExecutor` runs shell steps; the
//! `MockExecutor` (in `crate::mock`) returns scripted outcomes so scheduling can
//! be tested with no processes at all.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::pipeline::Job;

/// A content-addressed bag of named byte blobs handed between jobs. `BTreeMap`
/// keeps iteration order stable so hashing and merging are deterministic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Artifacts {
    map: BTreeMap<String, Vec<u8>>,
}

impl Artifacts {
    pub fn new() -> Self {
        Artifacts::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, data: Vec<u8>) {
        self.map.insert(name.into(), data);
    }

    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.map.get(name).map(Vec::as_slice)
    }

    pub fn merge(&mut self, other: &Artifacts) {
        for (k, v) in &other.map {
            self.map.insert(k.clone(), v.clone());
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<u8>)> {
        self.map.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

/// What an `Executor` reports back for one job.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub logs: String,
    pub artifacts: Artifacts,
    /// How long the job took. For the mock this is virtual sim-time that the
    /// scheduler uses as a clock; for the shell it is the measured wall time.
    pub duration: Duration,
}

impl JobOutcome {
    pub fn success(duration: Duration) -> Self {
        JobOutcome {
            success: true,
            exit_code: Some(0),
            logs: String::new(),
            artifacts: Artifacts::new(),
            duration,
        }
    }

    pub fn failure(duration: Duration) -> Self {
        JobOutcome {
            success: false,
            exit_code: Some(1),
            logs: String::new(),
            artifacts: Artifacts::new(),
            duration,
        }
    }
}

/// The one seam the scheduler drives. `run` is called when a job starts;
/// `on_finish` is called when the scheduler retires it (used by the mock to
/// track live concurrency; the shell ignores it).
pub trait Executor {
    fn run(&mut self, job: &Job, inputs: &Artifacts) -> JobOutcome;

    fn on_finish(&mut self, _job: &str) {}
}

/// Runs each step as `sh -c <step>` in the job's working directory, stops at the
/// first failing step, and collects declared `produces` paths as artifacts. A
/// per-step timeout keeps a hung child from wedging the whole run.
pub struct ShellExecutor {
    pub base_dir: PathBuf,
    pub timeout: Option<Duration>,
}

impl ShellExecutor {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        ShellExecutor { base_dir: base_dir.into(), timeout: None }
    }

    pub fn with_timeout(base_dir: impl Into<PathBuf>, timeout: Duration) -> Self {
        ShellExecutor { base_dir: base_dir.into(), timeout: Some(timeout) }
    }
}

impl Executor for ShellExecutor {
    fn run(&mut self, job: &Job, _inputs: &Artifacts) -> JobOutcome {
        let start = Instant::now();
        let mut logs = String::new();
        let mut success = true;
        let mut exit_code = Some(0);

        for step in &job.steps {
            let result = run_step(step, &self.base_dir, &job.env, self.timeout);
            match result {
                Ok(step_out) => {
                    logs.push_str(&step_out.output);
                    if !step_out.success {
                        success = false;
                        exit_code = step_out.code;
                        if step_out.timed_out {
                            logs.push_str(&format!("\n[relay] step timed out: {step}\n"));
                        }
                        break;
                    }
                }
                Err(e) => {
                    logs.push_str(&format!("\n[relay] failed to launch step '{step}': {e}\n"));
                    success = false;
                    exit_code = None;
                    break;
                }
            }
        }

        let mut artifacts = Artifacts::new();
        if success {
            for path in &job.produces {
                match std::fs::read(self.base_dir.join(path)) {
                    Ok(bytes) => artifacts.insert(path.clone(), bytes),
                    Err(e) => logs.push_str(&format!("\n[relay] declared output missing: {path} ({e})\n")),
                }
            }
        }

        JobOutcome { success, exit_code, logs, artifacts, duration: start.elapsed() }
    }
}

struct StepOutput {
    success: bool,
    code: Option<i32>,
    output: String,
    timed_out: bool,
}

fn run_step(
    step: &str,
    dir: &PathBuf,
    env: &[(String, String)],
    timeout: Option<Duration>,
) -> std::io::Result<StepOutput> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(step)
        .current_dir(dir)
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Drain both pipes on threads so a chatty child cannot deadlock on a full pipe.
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let out_handle = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    let err_handle = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf);
        buf
    });

    let start = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if let Some(limit) = timeout {
            if start.elapsed() > limit {
                let _ = child.kill();
                timed_out = true;
                break child.wait()?;
            }
        }
        thread::sleep(Duration::from_millis(5));
    };

    let mut output = String::from_utf8_lossy(&out_handle.join().unwrap_or_default()).into_owned();
    output.push_str(&String::from_utf8_lossy(&err_handle.join().unwrap_or_default()));

    Ok(StepOutput {
        success: status.success() && !timed_out,
        code: status.code(),
        output,
        timed_out,
    })
}
