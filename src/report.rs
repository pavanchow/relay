//! What a run produced: a status, wave, start order, timing, and captured logs
//! per job. Pure data so tests can assert against it directly.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Success,
    Failed,
    Skipped,
    Cached,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Success => "success",
            Status::Failed => "failed",
            Status::Skipped => "skipped",
            Status::Cached => "cached",
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub name: String,
    pub status: Status,
    /// Topological level (its wave in the plan).
    pub wave: usize,
    /// Order the scheduler started this job in; `usize::MAX` if it never started.
    pub start_order: usize,
    /// Virtual (mock) or real (shell) start offset, in nanoseconds.
    pub start_nanos: u64,
    pub duration: Duration,
    pub logs: String,
}

#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Records in config order.
    pub jobs: Vec<JobRecord>,
}

impl Report {
    pub fn get(&self, name: &str) -> Option<&JobRecord> {
        self.jobs.iter().find(|j| j.name == name)
    }

    pub fn status(&self, name: &str) -> Option<Status> {
        self.get(name).map(|j| j.status)
    }

    /// True when no job failed (cached, skipped, and success all count as not-failed).
    pub fn succeeded(&self) -> bool {
        !self.jobs.iter().any(|j| j.status == Status::Failed)
    }

    pub fn count(&self, status: Status) -> usize {
        self.jobs.iter().filter(|j| j.status == status).count()
    }

    /// Records ordered by the sequence they started (skipped jobs last).
    pub fn by_start(&self) -> Vec<&JobRecord> {
        let mut v: Vec<&JobRecord> = self.jobs.iter().collect();
        v.sort_by_key(|j| j.start_order);
        v
    }

    pub fn summary(&self) -> String {
        format!(
            "{} success, {} cached, {} failed, {} skipped",
            self.count(Status::Success),
            self.count(Status::Cached),
            self.count(Status::Failed),
            self.count(Status::Skipped),
        )
    }
}
