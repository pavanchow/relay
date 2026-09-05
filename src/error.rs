use std::fmt;

/// Every way a pipeline can be rejected before or during a run.
#[derive(Debug)]
pub enum Error {
    Parse { line: usize, msg: String },
    DuplicateJob(String),
    MissingDependency { job: String, needs: String },
    Cycle(Vec<String>),
    Io(std::io::Error),
}

impl Error {
    pub(crate) fn parse(line: usize, msg: impl Into<String>) -> Self {
        Error::Parse { line, msg: msg.into() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Parse { line, msg } => write!(f, "parse error on line {line}: {msg}"),
            Error::DuplicateJob(name) => write!(f, "duplicate job name: {name}"),
            Error::MissingDependency { job, needs } => {
                write!(f, "job '{job}' needs '{needs}', which is not defined")
            }
            Error::Cycle(nodes) => write!(f, "dependency cycle among jobs: {}", nodes.join(", ")),
            Error::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
