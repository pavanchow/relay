//! Real runs: actual shell commands, artifacts delivered between isolated job
//! working directories, timeouts that reap orphans, upstream-aware caching, and
//! path confinement. Each test uses a temp dir, cleaned up on drop.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use relay::cache::CacheStore;
use relay::executor::{Executor, ShellExecutor};
use relay::pipeline::Pipeline;
use relay::report::Status;
use relay::schedule::{self, Limits};

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("relay-it-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn real_pipeline_passes_artifacts_and_skips_after_failure() {
    let dir = TempDir::new("real");

    // produce -> consume (reads the delivered artifact and writes a result).
    // flaky -> after-flaky (flaky exits nonzero; after-flaky must be skipped).
    let pipeline = Pipeline::parse(
        "job produce {\n\
         \x20 step printf 'payload' > out.txt\n\
         \x20 produces out.txt\n\
         }\n\
         job consume {\n\
         \x20 needs produce\n\
         \x20 step sh -c 'cat out.txt out.txt > combined.txt'\n\
         \x20 produces combined.txt\n\
         }\n\
         job flaky {\n\
         \x20 step sh -c 'exit 3'\n\
         }\n\
         job after_flaky {\n\
         \x20 needs flaky\n\
         \x20 step printf 'should not run' > never.txt\n\
         }\n",
    )
    .unwrap();

    let mut exec = ShellExecutor::with_timeout(dir.path().to_path_buf(), Duration::from_secs(30));
    let produce_work = exec.work_dir_for("produce");
    let consume_work = exec.work_dir_for("consume");
    let after_work = exec.work_dir_for("after_flaky");
    let limits = Limits::new(2).base_dir(dir.path());
    let report = schedule::run(&pipeline, &mut exec, limits).unwrap();

    // Statuses.
    assert_eq!(report.status("produce"), Some(Status::Success));
    assert_eq!(report.status("consume"), Some(Status::Success));
    assert_eq!(report.status("flaky"), Some(Status::Failed));
    assert_eq!(report.status("after_flaky"), Some(Status::Skipped));

    // Each job ran in its own working directory.
    assert_eq!(fs::read_to_string(produce_work.join("out.txt")).unwrap(), "payload");
    // The upstream artifact was materialized into the DIFFERENT downstream work dir.
    assert_eq!(fs::read_to_string(consume_work.join("out.txt")).unwrap(), "payload");
    assert_eq!(fs::read_to_string(consume_work.join("combined.txt")).unwrap(), "payloadpayload");
    // A skipped job never runs, so its work dir is never created.
    assert!(!after_work.join("never.txt").exists(), "skipped job must not run its step");

    // The artifact really flowed through the report/store.
    assert_eq!(report.get("consume").unwrap().status, Status::Success);
}

#[test]
fn artifact_is_delivered_across_working_dirs() {
    let dir = TempDir::new("crossdir");
    let pipeline = Pipeline::parse(
        "job maker {\n\
         \x20 step printf 'from-maker' > data.bin\n\
         \x20 produces data.bin\n\
         }\n\
         job user {\n\
         \x20 needs maker\n\
         \x20 step sh -c 'cat data.bin > seen.txt'\n\
         \x20 produces seen.txt\n\
         }\n",
    )
    .unwrap();

    let mut exec = ShellExecutor::new(dir.path().to_path_buf());
    let maker_work = exec.work_dir_for("maker");
    let user_work = exec.work_dir_for("user");
    // The jobs must run in genuinely distinct directories.
    assert_ne!(maker_work, user_work);

    let report = schedule::run(&pipeline, &mut exec, Limits::new(1).base_dir(dir.path())).unwrap();
    assert_eq!(report.status("user"), Some(Status::Success));
    // maker's file exists only in maker's dir, yet user received a copy in its own.
    assert!(maker_work.join("data.bin").exists());
    assert!(user_work.join("data.bin").exists(), "input must be materialized into user's dir");
    assert_eq!(fs::read_to_string(user_work.join("seen.txt")).unwrap(), "from-maker");
}

#[test]
fn per_step_timeout_kills_backgrounded_child_without_hanging() {
    let dir = TempDir::new("orphan");
    // The step backgrounds a long sleep that inherits the pipe, then returns.
    // Before the fix run() blocked on the reader until the orphan exited.
    let mut job = relay::pipeline::Job::new("bg");
    job.steps.push("sleep 20 & echo started".into());
    let mut exec = ShellExecutor::with_timeout(dir.path().to_path_buf(), Duration::from_millis(400));

    let start = Instant::now();
    let outcome = exec.run(&job, &relay::executor::Artifacts::new());
    let elapsed = start.elapsed();

    assert!(!outcome.success, "a timed-out job must not report success");
    assert!(
        elapsed < Duration::from_secs(5),
        "timeout must bound wall time even with a backgrounded child, took {elapsed:?}"
    );
}

#[test]
fn job_level_timeout_is_enforced() {
    let dir = TempDir::new("jobto");
    let pipeline = Pipeline::parse(
        "job slow {\n\
         \x20 timeout 1\n\
         \x20 step sleep 20\n\
         }\n",
    )
    .unwrap();
    // No per-step timeout: the job-level wall clock is what must fire.
    let mut exec = ShellExecutor::new(dir.path().to_path_buf());
    let start = Instant::now();
    let report = schedule::run(&pipeline, &mut exec, Limits::new(1).base_dir(dir.path())).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(report.status("slow"), Some(Status::Failed));
    assert!(elapsed < Duration::from_secs(5), "job timeout must bound wall time, took {elapsed:?}");
}

#[test]
fn upstream_change_busts_downstream_cache() {
    let dir = TempDir::new("falsehit");

    let make = |producer_step: &str| -> Pipeline {
        Pipeline {
            jobs: vec![
                {
                    // Producer has no cache block, so it always runs.
                    let mut p = relay::pipeline::Job::new("producer");
                    p.steps.push(producer_step.to_string());
                    p
                },
                {
                    let mut c = relay::pipeline::Job::new("consumer");
                    c.needs = vec!["producer".into()];
                    c.steps.push("echo consuming".into());
                    c.cache = Some(relay::pipeline::Cache { key: Some("v1".into()), paths: vec![] });
                    c
                },
            ],
        }
    };

    let p1 = make("echo VERSION_A > out.txt");
    let p2 = make("echo VERSION_B_totally_different > out.txt");

    let mut cache = CacheStore::in_memory();
    let mut e1 = ShellExecutor::new(dir.path().to_path_buf());
    let r1 = schedule::run_with_cache(&p1, &mut e1, &mut cache, Limits::new(2).base_dir(dir.path())).unwrap();
    assert_eq!(r1.status("consumer"), Some(Status::Success));

    let mut e2 = ShellExecutor::new(dir.path().to_path_buf());
    let r2 = schedule::run_with_cache(&p2, &mut e2, &mut cache, Limits::new(2).base_dir(dir.path())).unwrap();
    // The upstream changed, so the downstream must re-run, not serve a stale hit.
    assert_eq!(
        r2.status("consumer"),
        Some(Status::Success),
        "consumer must re-run when a non-cached upstream changes"
    );
    assert!(e2.work_dir_for("consumer").exists(), "consumer should have executed, not been cached");
}

#[test]
fn absolute_and_parent_produces_paths_are_rejected() {
    let abs = Pipeline::parse("job a {\n  step true\n  produces /etc/hosts\n}\n");
    assert!(abs.is_err(), "absolute produces path must be rejected");
    let parent = Pipeline::parse("job a {\n  step true\n  produces ../../escape\n}\n");
    assert!(parent.is_err(), "'..' produces path must be rejected");
    let cache_escape =
        Pipeline::parse("job a {\n  step true\n  cache {\n    paths ../../secrets\n  }\n}\n");
    assert!(cache_escape.is_err(), "'..' cache path must be rejected");
}

#[test]
fn invalid_job_name_is_rejected() {
    // `job a b c {` must not silently become a job named "a b c".
    let err = Pipeline::parse("job a b c {\n  step true\n}\n");
    assert!(err.is_err(), "a job name with whitespace must be rejected");
    // A well-formed name still parses.
    assert!(Pipeline::parse("job build_1 {\n  step true\n}\n").is_ok());
}
