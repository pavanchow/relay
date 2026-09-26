//! Integration tests for the pure core: parser, printer round-trip, path
//! confinement, FNV-1a hashing, the DAG builder, and the deterministic scheduler
//! driven through the mock executor. No processes, no threads, no filesystem.

use std::time::Duration;

use relay::cache::{compute_key, CacheStore};
use relay::dag::Dag;
use relay::error::Error;
use relay::hash::{fnv1a, Hasher};
use relay::mock::MockExecutor;
use relay::pipeline::Pipeline;
use relay::report::Status;
use relay::safepath::is_confined;
use relay::schedule::{run, run_with_cache, Limits};

// ---- parser ----------------------------------------------------------------

#[test]
fn parses_a_full_job() {
    let p = Pipeline::parse(
        "job build {\n  needs fetch, lint\n  env RUST_LOG = debug\n  step cargo build\n  produces target/app\n  timeout 90\n  continue-on-error\n}",
    )
    .unwrap();
    let j = p.job("build").unwrap();
    assert_eq!(j.needs, vec!["fetch", "lint"]);
    assert_eq!(j.env, vec![("RUST_LOG".to_string(), "debug".to_string())]);
    assert_eq!(j.steps, vec!["cargo build"]);
    assert_eq!(j.produces, vec!["target/app"]);
    assert_eq!(j.timeout, Some(Duration::from_secs(90)));
    assert!(j.continue_on_error);
}

#[test]
fn parses_a_cache_block() {
    let p = Pipeline::parse("job b {\n  cache {\n    key v3\n    paths src, Cargo.toml\n  }\n}").unwrap();
    let cache = p.job("b").unwrap().cache.as_ref().unwrap();
    assert_eq!(cache.key.as_deref(), Some("v3"));
    assert_eq!(cache.paths, vec!["src", "Cargo.toml"]);
}

#[test]
fn comments_and_blank_lines_are_ignored() {
    let p = Pipeline::parse("# a comment\n\njob a {\n  # inside\n  step echo hi\n}\n").unwrap();
    assert_eq!(p.jobs.len(), 1);
    assert_eq!(p.job("a").unwrap().steps, vec!["echo hi"]);
}

#[test]
fn duplicate_job_name_is_rejected() {
    let err = Pipeline::parse("job a {\n}\njob a {\n}").unwrap_err();
    assert!(matches!(err, Error::DuplicateJob(n) if n == "a"));
}

#[test]
fn unknown_directive_is_a_parse_error() {
    let err = Pipeline::parse("job a {\n  frobnicate now\n}").unwrap_err();
    assert!(matches!(err, Error::Parse { .. }));
}

#[test]
fn unterminated_job_is_a_parse_error() {
    assert!(matches!(Pipeline::parse("job a {\n  step x").unwrap_err(), Error::Parse { .. }));
}

#[test]
fn escaping_produces_path_is_rejected() {
    assert!(matches!(Pipeline::parse("job a {\n  produces ../secret\n}").unwrap_err(), Error::Parse { .. }));
    assert!(matches!(Pipeline::parse("job a {\n  produces /etc/passwd\n}").unwrap_err(), Error::Parse { .. }));
}

#[test]
fn job_name_with_whitespace_is_rejected() {
    assert!(matches!(Pipeline::parse("job bad name {\n}").unwrap_err(), Error::Parse { .. }));
}

#[test]
fn env_value_may_contain_equals() {
    let p = Pipeline::parse("job a {\n  env FLAGS = --cfg=x\n}").unwrap();
    assert_eq!(p.job("a").unwrap().env, vec![("FLAGS".to_string(), "--cfg=x".to_string())]);
}

// ---- printer round-trip ----------------------------------------------------

#[test]
fn printed_pipeline_reparses_to_the_same_model() {
    let src = "job fetch {\n  step git clone\n}\njob build {\n  needs fetch\n  env K = v\n  step make\n  produces out/bin\n  timeout 30\n  cache {\n    key v1\n    paths src\n  }\n  continue-on-error\n}";
    let first = Pipeline::parse(src).unwrap();
    let printed = first.to_string();
    let second = Pipeline::parse(&printed).unwrap();
    assert_eq!(first, second);
}

// ---- safepath --------------------------------------------------------------

#[test]
fn confinement_allows_relative_and_rejects_escapes() {
    assert!(is_confined("src"));
    assert!(is_confined("src/main.rs"));
    assert!(is_confined("a/./b"));
    assert!(!is_confined(".."));
    assert!(!is_confined("../x"));
    assert!(!is_confined("a/../b"));
    assert!(!is_confined("/etc/passwd"));
}

// ---- hash ------------------------------------------------------------------

#[test]
fn fnv1a_is_deterministic_and_distinguishes_input() {
    assert_eq!(fnv1a(b"relay"), fnv1a(b"relay"));
    assert_ne!(fnv1a(b"relay"), fnv1a(b"relax"));
    assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
}

#[test]
fn write_str_is_length_delimited() {
    let mut a = Hasher::new();
    a.write_str("ab");
    a.write_str("c");
    let mut b = Hasher::new();
    b.write_str("a");
    b.write_str("bc");
    assert_ne!(a.finish(), b.finish());
}

// ---- dag -------------------------------------------------------------------

fn dag(src: &str) -> Result<Dag, Error> {
    Dag::build(&Pipeline::parse(src).unwrap())
}

#[test]
fn linear_chain_gets_increasing_levels() {
    let d = dag("job a {\n}\njob b {\n  needs a\n}\njob c {\n  needs b\n}").unwrap();
    assert_eq!(d.len(), 3);
    assert_eq!(d.levels[d.index_of("a").unwrap()], 0);
    assert_eq!(d.levels[d.index_of("b").unwrap()], 1);
    assert_eq!(d.levels[d.index_of("c").unwrap()], 2);
}

#[test]
fn diamond_groups_the_middle_into_one_wave() {
    let d = dag("job a {\n}\njob b {\n  needs a\n}\njob c {\n  needs a\n}\njob d {\n  needs b, c\n}").unwrap();
    let waves = d.waves();
    assert_eq!(waves.len(), 3);
    assert_eq!(waves[0], vec![d.index_of("a").unwrap()]);
    assert_eq!(waves[2], vec![d.index_of("d").unwrap()]);
    assert_eq!(waves[1].len(), 2);
}

#[test]
fn missing_dependency_is_reported() {
    let err = dag("job a {\n  needs ghost\n}").unwrap_err();
    assert!(matches!(err, Error::MissingDependency { job, needs } if job == "a" && needs == "ghost"));
}

#[test]
fn cycles_are_detected() {
    assert!(matches!(dag("job a {\n  needs b\n}\njob b {\n  needs a\n}").unwrap_err(), Error::Cycle(_)));
}

#[test]
fn self_dependency_is_a_cycle() {
    assert!(matches!(dag("job a {\n  needs a\n}").unwrap_err(), Error::Cycle(_)));
}

// ---- scheduler -------------------------------------------------------------

fn pipeline(src: &str) -> Pipeline {
    Pipeline::parse(src).unwrap()
}

#[test]
fn linear_pipeline_all_succeeds_in_dependency_order() {
    let p = pipeline("job a {\n  step x\n}\njob b {\n  needs a\n  step y\n}\njob c {\n  needs b\n  step z\n}");
    let mut exec = MockExecutor::new();
    let report = run(&p, &mut exec, Limits::new(4)).unwrap();
    assert!(report.succeeded());
    assert_eq!(report.count(Status::Success), 3);
    assert_eq!(exec.started, vec!["a", "b", "c"]);
}

#[test]
fn independent_jobs_run_concurrently_up_to_the_limit() {
    let p = pipeline("job a {\n}\njob b {\n  needs a\n}\njob c {\n  needs a\n}\njob d {\n  needs b, c\n}");
    let mut exec = MockExecutor::new();
    run(&p, &mut exec, Limits::new(4)).unwrap();
    assert_eq!(exec.max_concurrency, 2);
}

#[test]
fn concurrency_of_one_serialises_everything() {
    let p = pipeline("job a {\n}\njob b {\n  needs a\n}\njob c {\n  needs a\n}");
    let mut exec = MockExecutor::new();
    run(&p, &mut exec, Limits::new(1)).unwrap();
    assert_eq!(exec.max_concurrency, 1);
}

#[test]
fn failure_skips_transitive_dependents() {
    let p = pipeline("job a {\n}\njob b {\n  needs a\n}\njob c {\n  needs b\n}");
    let mut exec = MockExecutor::new().fail("a", 1);
    let report = run(&p, &mut exec, Limits::new(4)).unwrap();
    assert_eq!(report.status("a"), Some(Status::Failed));
    assert_eq!(report.status("b"), Some(Status::Skipped));
    assert_eq!(report.status("c"), Some(Status::Skipped));
    assert!(!exec.ran_job("b"));
    assert!(!report.succeeded());
}

#[test]
fn continue_on_error_lets_dependents_run() {
    let p = pipeline("job a {\n  continue-on-error\n}\njob b {\n  needs a\n}");
    let mut exec = MockExecutor::new().fail("a", 1);
    let report = run(&p, &mut exec, Limits::new(4)).unwrap();
    assert_eq!(report.status("a"), Some(Status::Failed));
    assert_eq!(report.status("b"), Some(Status::Success));
    assert!(exec.ran_job("b"));
}

#[test]
fn cache_hit_on_the_second_run_skips_execution() {
    let p = pipeline("job a {\n  step build\n  cache {\n    key v1\n  }\n}");
    let mut store = CacheStore::in_memory();

    let mut first = MockExecutor::new();
    let r1 = run_with_cache(&p, &mut first, &mut store, Limits::new(1)).unwrap();
    assert_eq!(r1.status("a"), Some(Status::Success));
    assert!(first.ran_job("a"));

    let mut second = MockExecutor::new();
    let r2 = run_with_cache(&p, &mut second, &mut store, Limits::new(1)).unwrap();
    assert_eq!(r2.status("a"), Some(Status::Cached));
    assert!(!second.ran_job("a"));
}

#[test]
fn compute_key_is_stable_and_config_sensitive() {
    let p = pipeline("job a {\n  step one\n}");
    let q = pipeline("job a {\n  step two\n}");
    let base = std::path::Path::new(".");
    let ja = p.job("a").unwrap();
    assert_eq!(compute_key(ja, base), compute_key(ja, base));
    assert_ne!(compute_key(ja, base), compute_key(q.job("a").unwrap(), base));
}
