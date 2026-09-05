//! The heart: a pure, deterministic DAG scheduler.
//!
//! It owns no processes and no threads. It drives an `Executor` in virtual time:
//! `run` is called when a job starts (returning a duration), and the job retires
//! at start + duration on a virtual clock. Up to `jobs` run concurrently; ties
//! break by config order. Failure marks transitive dependents SKIPPED unless the
//! failing job is `continue-on-error`. Cache hits skip execution entirely.

use std::path::PathBuf;
use std::time::Duration;

use crate::cache::{self, CacheEntry, CacheStore};
use crate::dag::Dag;
use crate::error::Error;
use crate::executor::{Artifacts, Executor};
use crate::pipeline::Pipeline;
use crate::report::{JobRecord, Report, Status};

#[derive(Debug, Clone)]
pub struct Limits {
    pub jobs: usize,
    pub fail_fast: bool,
    /// Directory that a job's `cache.paths` and shell `produces` resolve against.
    pub base_dir: PathBuf,
}

impl Limits {
    pub fn new(jobs: usize) -> Self {
        Limits { jobs: jobs.max(1), fail_fast: false, base_dir: PathBuf::from(".") }
    }

    pub fn base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = dir.into();
        self
    }

    pub fn fail_fast(mut self, on: bool) -> Self {
        self.fail_fast = on;
        self
    }
}

/// Convenience entry point matching the documented signature; uses a throwaway
/// in-memory cache.
pub fn run(pipeline: &Pipeline, exec: &mut dyn Executor, limits: Limits) -> Result<Report, Error> {
    let mut cache = CacheStore::in_memory();
    run_with_cache(pipeline, exec, &mut cache, limits)
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Pending,
    Running,
    Done,
}

pub fn run_with_cache(
    pipeline: &Pipeline,
    exec: &mut dyn Executor,
    cache: &mut CacheStore,
    limits: Limits,
) -> Result<Report, Error> {
    let dag = Dag::build(pipeline)?;
    let n = dag.len();
    let jobs = &pipeline.jobs;
    let concurrency = limits.jobs.max(1);

    let mut state = vec![State::Pending; n];
    let mut status: Vec<Option<Status>> = vec![None; n];
    let mut outputs: Vec<Artifacts> = (0..n).map(|_| Artifacts::new()).collect();
    let mut start_order = vec![usize::MAX; n];
    let mut start_nanos = vec![0u64; n];
    let mut duration = vec![Duration::ZERO; n];
    let mut logs = vec![String::new(); n];
    // Cache key per job, computed once when the job becomes ready (upstream-aware)
    // and reused when it retires, so get and put always agree.
    let mut keys = vec![0u64; n];
    // For a running job: Some(success) it will report when it retires.
    let mut pending_success: Vec<Option<bool>> = vec![None; n];

    let mut in_flight: Vec<(u64, usize)> = Vec::new();
    let mut clock: u64 = 0;
    let mut counter = 0usize;
    let mut aborting = false;

    loop {
        // 1. Propagate skips until stable: a pending job with any dep that is
        //    done-and-blocking becomes SKIPPED and takes no time.
        loop {
            let mut changed = false;
            for i in 0..n {
                if state[i] != State::Pending {
                    continue;
                }
                if aborting {
                    status[i] = Some(Status::Skipped);
                    state[i] = State::Done;
                    changed = true;
                    continue;
                }
                let blocked = dag.deps[i].iter().any(|&d| {
                    state[d] == State::Done && is_blocking(status[d], jobs[d].continue_on_error)
                });
                if blocked {
                    status[i] = Some(Status::Skipped);
                    state[i] = State::Done;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        // 2. Start every job we can: cache hits resolve instantly and take no
        //    slot; real starts consume a concurrency slot.
        loop {
            if aborting {
                break;
            }
            let mut started = false;
            for i in 0..n {
                if state[i] != State::Pending {
                    continue;
                }
                let ready = dag.deps[i].iter().all(|&d| state[d] == State::Done);
                if !ready {
                    continue;
                }

                // Merge dependency outputs into this job's inputs, then key the job
                // against its own config plus that upstream contribution.
                let mut inputs = Artifacts::new();
                for &d in &dag.deps[i] {
                    inputs.merge(&outputs[d]);
                }
                let dep_keys: Vec<u64> = dag.deps[i].iter().map(|&d| keys[d]).collect();
                let key = cache::compute_key_with_deps(&jobs[i], &limits.base_dir, &inputs, &dep_keys);
                keys[i] = key;

                if let Some(entry) = cache.get(key) {
                    status[i] = Some(Status::Cached);
                    state[i] = State::Done;
                    start_order[i] = counter;
                    counter += 1;
                    start_nanos[i] = clock;
                    logs[i] = entry.logs;
                    let mut arts = Artifacts::new();
                    for (k, v) in entry.artifacts {
                        arts.insert(k, v);
                    }
                    outputs[i] = arts;
                    started = true;
                    break;
                }

                if in_flight.len() >= concurrency {
                    continue;
                }

                let outcome = exec.run(&jobs[i], &inputs);
                state[i] = State::Running;
                start_order[i] = counter;
                counter += 1;
                start_nanos[i] = clock;
                duration[i] = outcome.duration;
                logs[i] = outcome.logs;
                outputs[i] = outcome.artifacts;
                pending_success[i] = Some(outcome.success);
                let finish = clock.saturating_add(outcome.duration.as_nanos() as u64);
                in_flight.push((finish, i));
                started = true;
                break;
            }
            if !started {
                break;
            }
        }

        // 3. Nothing in flight means everything reachable is resolved.
        if in_flight.is_empty() {
            break;
        }

        // 4. Advance the virtual clock to the earliest finish and retire every
        //    job finishing at that instant, in config order.
        let min_finish = in_flight.iter().map(|&(f, _)| f).min().unwrap();
        clock = min_finish;
        let mut finished: Vec<usize> = in_flight
            .iter()
            .filter(|&&(f, _)| f == min_finish)
            .map(|&(_, i)| i)
            .collect();
        finished.sort_unstable();
        in_flight.retain(|&(f, _)| f != min_finish);

        for i in finished {
            exec.on_finish(&jobs[i].name);
            state[i] = State::Done;
            let success = pending_success[i].take().unwrap_or(false);
            if success {
                status[i] = Some(Status::Success);
                if jobs[i].cache.is_some() {
                    let key = keys[i];
                    let artifacts = outputs[i]
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    cache.put(key, CacheEntry { artifacts, logs: logs[i].clone() });
                }
            } else {
                status[i] = Some(Status::Failed);
                if !jobs[i].continue_on_error && limits.fail_fast {
                    aborting = true;
                }
            }
        }
    }

    let records = (0..n)
        .map(|i| JobRecord {
            name: jobs[i].name.clone(),
            status: status[i].unwrap_or(Status::Skipped),
            wave: dag.levels[i],
            start_order: start_order[i],
            start_nanos: start_nanos[i],
            duration: duration[i],
            logs: std::mem::take(&mut logs[i]),
        })
        .collect();

    Ok(Report { jobs: records })
}

/// A dependency blocks its dependents when it was SKIPPED, or FAILED without
/// `continue-on-error`. SUCCESS, CACHED, and continue-on-error FAILED all clear.
fn is_blocking(status: Option<Status>, continue_on_error: bool) -> bool {
    match status {
        Some(Status::Skipped) => true,
        Some(Status::Failed) => !continue_on_error,
        _ => false,
    }
}
