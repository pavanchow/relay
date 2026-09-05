//! The boundary between the pure scheduler and the messy outside world.
//!
//! The scheduler never spawns a process. It calls `Executor::run` for each job
//! and reads back a `JobOutcome`. The real `ShellExecutor` runs shell steps; the
//! `MockExecutor` (in `crate::mock`) returns scripted outcomes so scheduling can
//! be tested with no processes at all.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::pipeline::Job;
use crate::safepath::is_confined;

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

/// Runs each step as `sh -c <step>` in the job's own working directory, stops at
/// the first failing step, and collects declared `produces` paths as artifacts.
///
/// Each job gets an isolated working directory under `base_dir/.relay-work/<job>`.
/// The `inputs` bag (artifacts produced by dependencies) is materialized into that
/// directory before the steps run, so cross-job files travel through the documented
/// artifact mechanism rather than a shared cwd, and jobs cannot clobber each other.
///
/// A per-step timeout and an optional per-job wall-clock timeout both kill the
/// step's whole process group, so a backgrounded grandchild cannot outlive the
/// deadline or hold a pipe open past it.
pub struct ShellExecutor {
    pub base_dir: PathBuf,
    /// Per-step wall-clock limit.
    pub timeout: Option<Duration>,
}

impl ShellExecutor {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        ShellExecutor { base_dir: base_dir.into(), timeout: None }
    }

    pub fn with_timeout(base_dir: impl Into<PathBuf>, timeout: Duration) -> Self {
        ShellExecutor { base_dir: base_dir.into(), timeout: Some(timeout) }
    }

    /// The isolated working directory a job runs in.
    pub fn work_dir_for(&self, job: &str) -> PathBuf {
        self.base_dir.join(".relay-work").join(job)
    }
}

impl Executor for ShellExecutor {
    fn run(&mut self, job: &Job, inputs: &Artifacts) -> JobOutcome {
        let start = Instant::now();
        let mut logs = String::new();
        let mut success = true;
        let mut exit_code = Some(0);

        // Fresh, isolated working directory for this job.
        let work = self.work_dir_for(&job.name);
        let _ = std::fs::remove_dir_all(&work);
        if let Err(e) = std::fs::create_dir_all(&work) {
            logs.push_str(&format!("\n[relay] could not create work dir for {}: {e}\n", job.name));
            return JobOutcome { success: false, exit_code: None, logs, artifacts: Artifacts::new(), duration: start.elapsed() };
        }

        // Deliver dependency artifacts into the work dir.
        for (name, bytes) in inputs.iter() {
            if !is_confined(name) {
                continue;
            }
            let dest = work.join(name);
            if let Some(parent) = dest.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(&dest, bytes) {
                logs.push_str(&format!("\n[relay] could not materialize input {name}: {e}\n"));
            }
        }

        let job_deadline = job.timeout.map(|t| start + t);

        for step in &job.steps {
            // Enforce the job-level wall clock before launching the next step.
            if let Some(dl) = job_deadline {
                if Instant::now() >= dl {
                    logs.push_str(&format!("\n[relay] job timed out before step: {step}\n"));
                    success = false;
                    exit_code = None;
                    break;
                }
            }
            let step_timeout = effective_timeout(self.timeout, job_deadline);
            let result = run_step(step, &work, &job.env, step_timeout);
            match result {
                Ok(step_out) => {
                    logs.push_str(&step_out.output);
                    if !step_out.success {
                        success = false;
                        exit_code = step_out.code;
                        if step_out.timed_out {
                            let job_level = job_deadline.map(|dl| Instant::now() >= dl).unwrap_or(false);
                            if job_level {
                                logs.push_str(&format!("\n[relay] job timed out during step: {step}\n"));
                            } else {
                                logs.push_str(&format!("\n[relay] step timed out: {step}\n"));
                            }
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
                // Declared outputs are confined at parse time; guard direct API use.
                if !is_confined(path) {
                    logs.push_str(&format!("\n[relay] refusing to collect out-of-workspace output: {path}\n"));
                    continue;
                }
                match std::fs::read(work.join(path)) {
                    Ok(bytes) => artifacts.insert(path.clone(), bytes),
                    Err(e) => logs.push_str(&format!("\n[relay] declared output missing: {path} ({e})\n")),
                }
            }
        }

        JobOutcome { success, exit_code, logs, artifacts, duration: start.elapsed() }
    }
}

/// The tighter of the per-step limit and the time left on the job-level deadline.
fn effective_timeout(step_timeout: Option<Duration>, job_deadline: Option<Instant>) -> Option<Duration> {
    let remaining = job_deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
    match (step_timeout, remaining) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Send SIGKILL to an entire process group so orphaned grandchildren die too.
#[cfg(unix)]
fn kill_group(pgid: i32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    // Negative pid targets the whole group; 9 is SIGKILL.
    unsafe {
        kill(-pgid, 9);
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
    dir: &Path,
    env: &[(String, String)],
    timeout: Option<Duration>,
) -> std::io::Result<StepOutput> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(step)
        .current_dir(dir)
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Own process group so a timeout can reap the whole tree, not just `sh`.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let mut child = cmd.spawn()?;
    #[cfg(unix)]
    let pgid = child.id() as i32;

    // Drain both pipes on threads so a chatty child cannot deadlock on a full pipe.
    // Results come back over channels so we can bound how long we wait for them.
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let (otx, orx) = mpsc::channel();
    let (etx, erx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        let _ = otx.send(buf);
    });
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf);
        let _ = etx.send(buf);
    });

    // The step is done only when `sh` has exited AND both pipes have hit EOF.
    // A backgrounded grandchild keeps the pipes open after `sh` exits, so we must
    // bound the whole wait by the deadline, not just the wait on `sh`.
    let start = Instant::now();
    let deadline = timeout.map(|t| start + t);
    let mut timed_out = false;
    let mut status = None;
    let mut out: Option<Vec<u8>> = None;
    let mut err: Option<Vec<u8>> = None;
    loop {
        if status.is_none() {
            status = child.try_wait()?;
        }
        if out.is_none() {
            if let Ok(buf) = orx.try_recv() {
                out = Some(buf);
            }
        }
        if err.is_none() {
            if let Ok(buf) = erx.try_recv() {
                err = Some(buf);
            }
        }
        if status.is_some() && out.is_some() && err.is_some() {
            break;
        }
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                // Kill the whole group so a backgrounded grandchild dies too and
                // stops holding the stdout/stderr pipe open.
                #[cfg(unix)]
                kill_group(pgid);
                #[cfg(not(unix))]
                {
                    let _ = child.kill();
                }
                timed_out = true;
                break;
            }
        }
        thread::sleep(Duration::from_millis(5));
    }

    // Reap `sh` if it was still running, then collect whatever the readers have.
    // After a group kill the pipes close and the readers finish promptly, so a
    // short grace is enough and we never block past the deadline.
    let status = match status {
        Some(s) => s,
        None => child.wait()?,
    };
    let grace = Duration::from_millis(200);
    let out = out.or_else(|| orx.recv_timeout(grace).ok()).unwrap_or_default();
    let err = err.or_else(|| erx.recv_timeout(grace).ok()).unwrap_or_default();
    let mut output = String::from_utf8_lossy(&out).into_owned();
    output.push_str(&String::from_utf8_lossy(&err));

    Ok(StepOutput {
        success: status.success() && !timed_out,
        code: status.code(),
        output,
        timed_out,
    })
}
