//! Graph validation: cycles and missing dependencies are rejected.

use relay::dag::Dag;
use relay::error::Error;
use relay::pipeline::Pipeline;

#[test]
fn cycle_is_detected() {
    let p = Pipeline::parse(
        "job a {\n  needs c\n  step x\n}\n\
         job b {\n  needs a\n  step x\n}\n\
         job c {\n  needs b\n  step x\n}\n",
    )
    .unwrap();
    match Dag::build(&p) {
        Err(Error::Cycle(nodes)) => assert_eq!(nodes.len(), 3),
        other => panic!("expected a cycle error, got {other:?}"),
    }
}

#[test]
fn self_loop_is_a_cycle() {
    let p = Pipeline::parse("job a {\n  needs a\n  step x\n}\n").unwrap();
    assert!(matches!(Dag::build(&p), Err(Error::Cycle(_))));
}

#[test]
fn missing_dependency_errors() {
    let p = Pipeline::parse("job a {\n  needs ghost\n  step x\n}\n").unwrap();
    match Dag::build(&p) {
        Err(Error::MissingDependency { job, needs }) => {
            assert_eq!(job, "a");
            assert_eq!(needs, "ghost");
        }
        other => panic!("expected a missing-dependency error, got {other:?}"),
    }
}

#[test]
fn duplicate_job_errors() {
    let err = Pipeline::parse("job a {\n  step x\n}\njob a {\n  step y\n}\n");
    assert!(matches!(err, Err(Error::DuplicateJob(_))));
}

#[test]
fn valid_graph_builds_and_layers() {
    let p = Pipeline::parse(
        "job a {\n  step x\n}\n\
         job b {\n  needs a\n  step x\n}\n\
         job c {\n  needs a, b\n  step x\n}\n",
    )
    .unwrap();
    let dag = Dag::build(&p).unwrap();
    assert_eq!(dag.levels, vec![0, 1, 2]);
    assert_eq!(dag.waves().len(), 3);
}
