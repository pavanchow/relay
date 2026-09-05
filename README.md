# Relay

A from-scratch CI/CD engine you can actually read. The gap it fills: most CI
systems bury their scheduler behind YAML, network calls, and container runtimes,
so the one interesting part (how jobs get ordered, parallelized, skipped, and
cached) is impossible to study or unit-test in isolation. Relay pulls that part
out and makes it a pure, deterministic DAG scheduler over an abstract executor,
so parallelism, failure propagation, and content-hash caching can all be tested
without running a single real process.

It pairs with [Metronome](https://github.com/pavanchow), a from-scratch
scheduler: Metronome decides when work runs over time, Relay decides in what
order a graph of work runs and what can run at once.

Zero external dependencies. Pure Rust std.

## The point

The core is one function:

```rust
schedule::run(&pipeline, &mut executor, limits) -> Report
```

The scheduler never spawns a process or a thread. It drives an `Executor` trait
in virtual time. That single seam is what makes it testable: swap in a
`MockExecutor` that returns scripted outcomes and records what ran concurrently,
and you can assert every scheduling rule deterministically. Swap in the
`ShellExecutor` and the same scheduler runs real shell commands.

What the scheduler guarantees:

- Dependency order. A job starts only after every job in its `needs` has
  finished successfully.
- Real parallelism. Up to N jobs run at once; the mock proves the peak
  concurrency is exactly N and never more.
- Failure propagation. A failed job marks its transitive dependents SKIPPED and
  they never run. Independent branches keep going. A `continue-on-error` job that
  fails does not skip its dependents.
- Content-hash caching. A job's cache key is a hand-rolled FNV-1a digest of its
  config plus the content of its declared input paths. A stored match marks the
  job CACHED and skips execution, restoring its recorded outputs. Change an input
  and the key busts.
- Determinism. Same pipeline, same result and same order, every run. Ties break
  by config order.

## The pipeline format

A readable text format with a parser and a matching printer that round-trip.

```text
job build {
  needs fetch
  env RUST_LOG = debug
  step cargo build
  produces target/app
  cache {
    key v1
    paths src, Cargo.toml
  }
  continue-on-error
}
```

- `needs` lists dependency job names.
- `env` sets one key/value pair per line.
- `step` is a shell command; steps run in order and stop at the first failure.
- `produces` lists output files collected as artifacts for dependent jobs.
- `cache { key, paths }` declares the inputs whose content decides the key.
- `continue-on-error` lets a failure not skip dependents.

## CLI

```
relay run   <pipeline> [--jobs N] [--fail-fast]   run it, print each status and a summary
relay plan  <pipeline> [--jobs N]                 print the DAG and wave order, run nothing
relay graph <pipeline>                            print the dependency graph
```

Example:

```
$ relay plan examples/diamond.relay --jobs 2
dependency graph:
  fetch (root)
  build <- fetch
  lint <- fetch
  test <- build, lint

execution waves (dependency order, up to 2 at once):
  wave 0: fetch
  wave 1: build, lint
  wave 2: test
```

```
$ relay run examples/failure.relay
  [ 0] setup                ok       success
  [ 1] unit                 xx       failed
  [ 2] docs                 ok       success
       package              --       skipped
       deploy               --       skipped
summary: 2 success, 0 cached, 1 failed, 2 skipped
```

## Layout

- `pipeline.rs` config data model, parser, and printer
- `dag.rs` graph build, cycle detection, topological waves
- `schedule.rs` the pure deterministic scheduler
- `executor.rs` the `Executor` trait, `ShellExecutor`, and the `Artifacts` store
- `cache.rs` the content-hash cache key and store
- `hash.rs` hand-rolled FNV-1a
- `report.rs` per-job status, wave, timing, logs
- `mock.rs` the scripted executor used by tests
- `main.rs` the CLI

## Tests

```
cargo test
cargo clippy --all-targets -- -D warnings
```

The suite covers the scheduler over the mock (dependency order, concurrency
limit, skip propagation, continue-on-error, fail-fast, determinism, waves),
graph validation (cycles, missing deps, duplicates), caching (hit, bust,
determinism), config round-trip, and one real integration test that runs actual
shell commands in a temp dir, passes an artifact between jobs, and confirms a
failing job skips its dependent.

## Non-goals

Relay is a readable reference engine, not a production CI platform. It does not
do distributed or remote runners, containers or sandboxing, secrets management,
a web UI or server, log streaming to a service, retries and backoff, artifact
storage beyond the local cache, or a matrix expansion syntax. The `ShellExecutor`
runs a scheduled parallel plan sequentially through the single-threaded executor
seam; genuine multi-core execution of shell jobs is left out on purpose to keep
the executor trait simple and the scheduler pure. See DESIGN.md.

## Author

Pavan Nallamothu (pavanchow). MIT licensed.
