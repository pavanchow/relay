//! Content-hash caching over the mock executor.

use std::fs;

use relay::cache::{self, CacheStore};
use relay::mock::MockExecutor;
use relay::pipeline::Pipeline;
use relay::report::Status;
use relay::schedule::{self, Limits};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("relay-cache-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn pipeline_over(dir: &std::path::Path) -> Pipeline {
    let input = dir.join("input.txt");
    fs::write(&input, b"hello").unwrap();
    Pipeline::parse(
        "job build {\n step compile\n cache {\n key v1\n paths input.txt\n }\n }\n",
    )
    .unwrap()
}

#[test]
fn unchanged_inputs_hit_the_cache_and_do_not_execute() {
    let dir = TempDir::new("hit");
    let p = pipeline_over(dir.path());
    let mut cache = CacheStore::in_memory();
    let limits = || Limits::new(2).base_dir(dir.path());

    let mut first = MockExecutor::new().succeed("build", 5);
    let r1 = schedule::run_with_cache(&p, &mut first, &mut cache, limits()).unwrap();
    assert_eq!(r1.status("build"), Some(Status::Success));
    assert!(first.ran_job("build"));

    let mut second = MockExecutor::new().succeed("build", 5);
    let r2 = schedule::run_with_cache(&p, &mut second, &mut cache, limits()).unwrap();
    assert_eq!(r2.status("build"), Some(Status::Cached));
    assert!(!second.ran_job("build"), "cache hit must not call the executor");
}

#[test]
fn changing_an_input_busts_the_key() {
    let dir = TempDir::new("bust");
    let p = pipeline_over(dir.path());
    let mut cache = CacheStore::in_memory();
    let limits = || Limits::new(2).base_dir(dir.path());

    let mut first = MockExecutor::new().succeed("build", 5);
    schedule::run_with_cache(&p, &mut first, &mut cache, limits()).unwrap();

    fs::write(dir.path().join("input.txt"), b"changed").unwrap();

    let mut second = MockExecutor::new().succeed("build", 5);
    let r2 = schedule::run_with_cache(&p, &mut second, &mut cache, limits()).unwrap();
    assert_eq!(r2.status("build"), Some(Status::Success));
    assert!(second.ran_job("build"), "changed input must force a re-run");
}

#[test]
fn key_is_deterministic() {
    let dir = TempDir::new("det");
    let p = pipeline_over(dir.path());
    let job = &p.jobs[0];
    let k1 = cache::compute_key(job, dir.path());
    let k2 = cache::compute_key(job, dir.path());
    assert_eq!(k1, k2);
}
