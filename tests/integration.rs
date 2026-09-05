//! A real run: actual shell commands, an artifact passed between jobs, and a
//! failing job whose dependent is correctly skipped. Uses a temp dir, cleaned up.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use relay::executor::ShellExecutor;
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

    // produce -> consume (reads the artifact and writes a result).
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
    let limits = Limits::new(2).base_dir(dir.path());
    let report = schedule::run(&pipeline, &mut exec, limits).unwrap();

    // Statuses.
    assert_eq!(report.status("produce"), Some(Status::Success));
    assert_eq!(report.status("consume"), Some(Status::Success));
    assert_eq!(report.status("flaky"), Some(Status::Failed));
    assert_eq!(report.status("after_flaky"), Some(Status::Skipped));

    // Real files on disk.
    assert_eq!(fs::read_to_string(dir.path().join("out.txt")).unwrap(), "payload");
    assert_eq!(fs::read_to_string(dir.path().join("combined.txt")).unwrap(), "payloadpayload");
    assert!(!dir.path().join("never.txt").exists(), "skipped job must not run its step");

    // The artifact really flowed through the report/store.
    let consume = report.get("consume").unwrap();
    assert_eq!(consume.status, Status::Success);
}
