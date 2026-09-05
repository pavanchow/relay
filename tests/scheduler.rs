//! Pure scheduler behavior over the mock executor. No real processes.

use relay::mock::MockExecutor;
use relay::pipeline::{Job, Pipeline};
use relay::report::Status;
use relay::schedule::{self, Limits};

/// Build a pipeline from (name, needs, continue_on_error) triples. Keeps the
/// tests focused on scheduling rather than config text.
fn pipeline(specs: &[(&str, &[&str], bool)]) -> Pipeline {
    let jobs = specs
        .iter()
        .map(|(name, needs, coe)| {
            let mut job = Job::new(*name);
            job.needs = needs.iter().map(|s| s.to_string()).collect();
            job.steps = vec!["run".to_string()];
            job.continue_on_error = *coe;
            job
        })
        .collect();
    Pipeline { jobs }
}

#[test]
fn dependency_order_is_respected() {
    let p = pipeline(&[("a", &[], false), ("b", &["a"], false), ("c", &["b"], false)]);
    let mut exec = MockExecutor::new().succeed("a", 10).succeed("b", 10).succeed("c", 10);
    let report = schedule::run(&p, &mut exec, Limits::new(4)).unwrap();

    let a = report.get("a").unwrap();
    let b = report.get("b").unwrap();
    let c = report.get("c").unwrap();
    assert!(a.start_nanos + a.duration.as_nanos() as u64 <= b.start_nanos);
    assert!(b.start_nanos + b.duration.as_nanos() as u64 <= c.start_nanos);
    assert!(report.succeeded());
}

#[test]
fn independent_jobs_run_up_to_the_limit_and_not_beyond() {
    let p = pipeline(&[("a", &[], false), ("b", &[], false), ("c", &[], false), ("d", &[], false)]);
    let mut exec = MockExecutor::new()
        .succeed("a", 10)
        .succeed("b", 10)
        .succeed("c", 10)
        .succeed("d", 10);
    let report = schedule::run(&p, &mut exec, Limits::new(2)).unwrap();

    assert_eq!(exec.max_concurrency, 2, "must run exactly 2 at once, never more");
    assert_eq!(report.count(Status::Success), 4);
}

#[test]
fn full_fan_out_saturates_the_limit() {
    let p = pipeline(&[
        ("a", &[], false),
        ("b", &[], false),
        ("c", &[], false),
        ("d", &[], false),
        ("e", &[], false),
    ]);
    let mut exec = MockExecutor::new()
        .succeed("a", 5)
        .succeed("b", 5)
        .succeed("c", 5)
        .succeed("d", 5)
        .succeed("e", 5);
    schedule::run(&p, &mut exec, Limits::new(3)).unwrap();
    assert_eq!(exec.max_concurrency, 3);
}

#[test]
fn failure_skips_transitive_dependents_and_does_not_run_them() {
    // a -> b -> c, and a fails. b and c must be SKIPPED and never executed.
    let p = pipeline(&[
        ("a", &[], false),
        ("b", &["a"], false),
        ("c", &["b"], false),
        ("d", &[], false),
    ]);
    let mut exec = MockExecutor::new()
        .fail("a", 10)
        .succeed("b", 10)
        .succeed("c", 10)
        .succeed("d", 10);
    let report = schedule::run(&p, &mut exec, Limits::new(4)).unwrap();

    assert_eq!(report.status("a"), Some(Status::Failed));
    assert_eq!(report.status("b"), Some(Status::Skipped));
    assert_eq!(report.status("c"), Some(Status::Skipped));
    assert_eq!(report.status("d"), Some(Status::Success), "independent branch keeps running");
    assert!(!exec.ran_job("b"), "skipped job must not be executed");
    assert!(!exec.ran_job("c"), "skipped job must not be executed");
    assert!(!report.succeeded());
}

#[test]
fn continue_on_error_keeps_dependents_running() {
    let p = pipeline(&[("a", &[], true), ("b", &["a"], false)]);
    let mut exec = MockExecutor::new().fail("a", 10).succeed("b", 10);
    let report = schedule::run(&p, &mut exec, Limits::new(4)).unwrap();

    assert_eq!(report.status("a"), Some(Status::Failed));
    assert_eq!(report.status("b"), Some(Status::Success));
    assert!(exec.ran_job("b"), "dependent of continue-on-error job must run");
}

#[test]
fn fail_fast_skips_remaining_dependent_work() {
    let p = pipeline(&[("a", &[], false), ("b", &[], false), ("c", &["b"], false)]);
    // a fails fast; b already started concurrently finishes, but c is skipped.
    let mut exec = MockExecutor::new().fail("a", 5).succeed("b", 50).succeed("c", 5);
    let report = schedule::run(&p, &mut exec, Limits::new(4).fail_fast(true)).unwrap();

    assert_eq!(report.status("a"), Some(Status::Failed));
    assert_eq!(report.status("c"), Some(Status::Skipped));
    assert!(!exec.ran_job("c"));
}

#[test]
fn schedule_is_deterministic_across_runs() {
    let p = pipeline(&[
        ("a", &[], false),
        ("b", &["a"], false),
        ("c", &["a"], false),
        ("d", &["b", "c"], false),
    ]);

    let order = |ms: u64| {
        let mut exec = MockExecutor::new()
            .succeed("a", ms)
            .succeed("b", ms)
            .succeed("c", ms)
            .succeed("d", ms);
        let _ = schedule::run(&p, &mut exec, Limits::new(2)).unwrap();
        exec.started
    };
    assert_eq!(order(10), order(10));
    let started = order(10);
    let b = started.iter().position(|n| n == "b").unwrap();
    let c = started.iter().position(|n| n == "c").unwrap();
    assert!(b < c, "config order tie-break: b starts before c");
}

#[test]
fn diamond_waves_are_correct() {
    let p = pipeline(&[
        ("a", &[], false),
        ("b", &["a"], false),
        ("c", &["a"], false),
        ("d", &["b", "c"], false),
    ]);
    let mut exec = MockExecutor::new();
    let report = schedule::run(&p, &mut exec, Limits::new(4)).unwrap();
    assert_eq!(report.get("a").unwrap().wave, 0);
    assert_eq!(report.get("b").unwrap().wave, 1);
    assert_eq!(report.get("c").unwrap().wave, 1);
    assert_eq!(report.get("d").unwrap().wave, 2);
}
