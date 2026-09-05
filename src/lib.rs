//! Relay: a small, readable CI/CD engine.
//!
//! The core is a pure, deterministic DAG scheduler ([`schedule::run`]) over an
//! abstract [`executor::Executor`]. That is the whole point: parallelism,
//! failure propagation, and content-hash caching can be unit-tested with a mock
//! executor and zero real processes. [`executor::ShellExecutor`] is the real
//! materialization.

pub mod cache;
pub mod dag;
pub mod error;
pub mod executor;
pub mod hash;
pub mod mock;
pub mod pipeline;
pub mod report;
pub mod safepath;
pub mod schedule;

pub use error::Error;
pub use executor::{Artifacts, Executor, JobOutcome, ShellExecutor};
pub use pipeline::{Cache, Job, Pipeline};
pub use report::{JobRecord, Report, Status};
pub use schedule::{run, run_with_cache, Limits};
