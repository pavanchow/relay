//! The parser and printer round-trip.

use relay::pipeline::Pipeline;

#[test]
fn parse_print_parse_is_stable() {
    let src = "\
job fetch {
  step git clone https://example.com/repo
  produces repo/src
}

job build {
  needs fetch
  env RUST_LOG = debug
  env CI = 1
  step cargo build
  step cargo doc
  produces target/app
  cache {
    key v1
    paths src, Cargo.toml
  }
  continue-on-error
}
";
    let first = Pipeline::parse(src).unwrap();
    let printed = first.to_string();
    let second = Pipeline::parse(&printed).unwrap();
    assert_eq!(first, second, "round-trip must preserve the pipeline exactly");
    // Reprinting the reparsed form is idempotent.
    assert_eq!(printed, second.to_string());
}

#[test]
fn comments_and_blank_lines_are_ignored() {
    let src = "\
# a comment
job a {
  # inner comment
  step echo hi

}
";
    let p = Pipeline::parse(src).unwrap();
    assert_eq!(p.jobs.len(), 1);
    assert_eq!(p.jobs[0].steps, vec!["echo hi".to_string()]);
}

#[test]
fn parse_errors_report_a_line() {
    let err = Pipeline::parse("job a {\n  bogus directive\n}\n").unwrap_err();
    assert!(err.to_string().contains("line 2"), "got: {err}");
}
