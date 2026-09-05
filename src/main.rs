use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use relay::cache::CacheStore;
use relay::dag::Dag;
use relay::executor::ShellExecutor;
use relay::pipeline::Pipeline;
use relay::report::Status;
use relay::schedule::{self, Limits};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(msg) => {
            eprintln!("relay: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<ExitCode, String> {
    let Some(command) = args.first() else {
        return Err(usage());
    };

    match command.as_str() {
        "run" => cmd_run(&args[1..]),
        "plan" => cmd_plan(&args[1..]),
        "graph" => cmd_graph(&args[1..]),
        "-h" | "--help" | "help" => {
            println!("{}", usage());
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!("unknown command '{other}'\n\n{}", usage())),
    }
}

struct Options {
    path: PathBuf,
    jobs: usize,
    fail_fast: bool,
}

fn parse_options(args: &[String]) -> Result<Options, String> {
    let mut path: Option<PathBuf> = None;
    let mut jobs = 4usize;
    let mut fail_fast = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--jobs" | "-j" => {
                i += 1;
                let raw = args.get(i).ok_or("--jobs needs a number")?;
                jobs = raw.parse().map_err(|_| format!("invalid --jobs value: {raw}"))?;
                if jobs == 0 {
                    return Err("--jobs must be at least 1".into());
                }
            }
            "--fail-fast" => fail_fast = true,
            other if other.starts_with('-') => return Err(format!("unknown flag: {other}")),
            other => path = Some(PathBuf::from(other)),
        }
        i += 1;
    }

    let path = path.ok_or("missing pipeline file")?;
    Ok(Options { path, jobs, fail_fast })
}

fn load(path: &Path) -> Result<Pipeline, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    Pipeline::parse(&text).map_err(|e| e.to_string())
}

fn base_dir(path: &Path) -> PathBuf {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn cmd_run(args: &[String]) -> Result<ExitCode, String> {
    let opts = parse_options(args)?;
    let pipeline = load(&opts.path)?;
    let base = base_dir(&opts.path);

    let mut exec = ShellExecutor::with_timeout(base.clone(), Duration::from_secs(300));
    let mut cache = CacheStore::on_disk(base.join(".relay-cache")).map_err(|e| e.to_string())?;
    let limits = Limits::new(opts.jobs).base_dir(base).fail_fast(opts.fail_fast);

    let report = schedule::run_with_cache(&pipeline, &mut exec, &mut cache, limits)
        .map_err(|e| e.to_string())?;

    println!("relay run: {} (--jobs {})", opts.path.display(), opts.jobs);
    for record in report.by_start() {
        let when = if record.start_order == usize::MAX {
            "     ".to_string()
        } else {
            format!("[{:>2}] ", record.start_order)
        };
        println!("  {when}{:<20} {}", record.name, mark(record.status));
    }
    println!("summary: {}", report.summary());

    if report.succeeded() {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

fn cmd_plan(args: &[String]) -> Result<ExitCode, String> {
    let opts = parse_options(args)?;
    let pipeline = load(&opts.path)?;
    let dag = Dag::build(&pipeline).map_err(|e| e.to_string())?;

    println!("plan: {} ({} jobs, --jobs {})", opts.path.display(), dag.len(), opts.jobs);
    println!();
    println!("dependency graph:");
    print_graph(&pipeline, &dag);
    println!();
    println!("execution waves (dependency order, up to {} at once):", opts.jobs);
    for (w, wave) in dag.waves().iter().enumerate() {
        let names: Vec<&str> = wave.iter().map(|&i| dag.order[i].as_str()).collect();
        println!("  wave {w}: {}", names.join(", "));
    }
    Ok(ExitCode::SUCCESS)
}

fn cmd_graph(args: &[String]) -> Result<ExitCode, String> {
    let opts = parse_options(args)?;
    let pipeline = load(&opts.path)?;
    let dag = Dag::build(&pipeline).map_err(|e| e.to_string())?;
    println!("graph: {}", opts.path.display());
    print_graph(&pipeline, &dag);
    Ok(ExitCode::SUCCESS)
}

fn print_graph(pipeline: &Pipeline, dag: &Dag) {
    for (i, job) in pipeline.jobs.iter().enumerate() {
        if job.needs.is_empty() {
            println!("  {} (root)", job.name);
        } else {
            let needs: Vec<&str> = dag.deps[i].iter().map(|&d| dag.order[d].as_str()).collect();
            println!("  {} <- {}", job.name, needs.join(", "));
        }
    }
}

fn mark(status: Status) -> &'static str {
    match status {
        Status::Success => "ok       success",
        Status::Cached => "==       cached",
        Status::Failed => "xx       failed",
        Status::Skipped => "--       skipped",
    }
}

fn usage() -> String {
    "\
relay - a readable CI/CD engine

usage:
  relay run   <pipeline> [--jobs N] [--fail-fast]   run the pipeline
  relay plan  <pipeline> [--jobs N]                 show the DAG and wave order, run nothing
  relay graph <pipeline>                            show the dependency graph"
        .to_string()
}
