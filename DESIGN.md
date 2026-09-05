# Relay design

## The gap

A CI/CD system is, at its core, a scheduler over a dependency graph. Everything
else (config formats, runners, container images, dashboards) is packaging. Yet
in real systems that core is the least inspectable part. It is entangled with
process spawning, network I/O, and clocks, so you cannot unit-test "does a failed
job skip its dependents" without standing up an execution environment.

Relay's thesis is that the scheduler should be a pure function of the graph and
the job outcomes, and nothing else. Pull the side effects out behind one trait
and the interesting behavior becomes ordinary data you can assert on. That is the
whole design.

## The seam

```rust
pub trait Executor {
    fn run(&mut self, job: &Job, inputs: &Artifacts) -> JobOutcome;
    fn on_finish(&mut self, _job: &str) {}
}
```

The scheduler calls `run` when a job starts and reads back a `JobOutcome`: did it
succeed, what did it produce, how long did it take. It never touches a process, a
thread, or a real clock. Two implementations sit behind the trait:

- `ShellExecutor` runs each step as `sh -c` in the job's directory, stops at the
  first failing step, and collects declared `produces` files as artifacts.
- `MockExecutor` returns scripted outcomes and records the order jobs started and
  the peak number live at once.

`on_finish` exists so the mock can track live concurrency (increment on `run`,
decrement on `on_finish`). The shell ignores it.

## Virtual-time scheduling

The scheduler models parallelism with a virtual clock instead of OS threads,
which keeps it deterministic and testable while still expressing genuine overlap.

1. Build the DAG, reject cycles and missing dependencies.
2. Propagate skips: any pending job with a dependency that is done-and-blocking
   (SKIPPED, or FAILED without `continue-on-error`) becomes SKIPPED and takes no
   time.
3. Start jobs: scan in config order. A runnable job (all deps done and
   successful) is either a cache hit (resolved instantly, no slot consumed) or a
   real start (consumes one of N concurrency slots). On a real start we call
   `run`, record the start, and schedule its finish at `clock + duration`.
4. Advance the clock to the earliest finish and retire every job finishing at
   that instant, in config order. Retiring a job sets its final status and, on
   success with a cache block, stores its cache entry.
5. Repeat until nothing is in flight.

Because `run` returns the job's duration up front, and multiple jobs in a wave
are started before any is retired, the mock sees all of them live at once. That
is how a test asserts the peak concurrency equals the limit and never exceeds it,
with no real parallelism involved.

Determinism comes from a single rule applied everywhere: when there is a choice,
take the lowest config index. Runnable jobs, simultaneous finishes, and wave
membership all tie-break that way, so a pipeline produces the same report and the
same start order on every run.

## Failure propagation

A dependency edge is "blocking" when the upstream job was SKIPPED, or FAILED and
not marked `continue-on-error`. When a blocking outcome lands, skip propagation
marks the whole downstream subtree SKIPPED in one pass, and those jobs are never
handed to the executor. Independent subtrees are untouched, so one failing branch
does not abort the run. The `--fail-fast` flag changes this: the first
non-continue-on-error failure marks all not-yet-started work SKIPPED, while jobs
already in flight are allowed to finish.

## Caching

Each job's cache key is a hand-rolled FNV-1a digest over:

- a format version tag,
- the job's name, steps, env pairs, needs, and continue-on-error flag,
- the optional `cache.key` label,
- for each declared `cache.paths` entry (sorted), the path and a recursive
  content hash of the file or directory it points at.

All writes into the hasher are length-delimited so `"ab" + "c"` cannot collide
with `"a" + "bc"`. If the store holds an entry for the key, the job is CACHED: it
is not executed, and its recorded artifacts and logs are restored so dependents
still see its outputs. On a successful real run of a job that has a cache block,
the entry is stored under the key. Change any declared input byte and the key
changes, so the job re-runs. The key is a pure function of config plus input
content, so it is identical across machines and runs.

The store is in-memory for tests (run the scheduler twice with the same store);
the CLI backs it with a `.relay-cache` directory using a small hand-rolled
length-prefixed binary format, so cache survives across invocations.

## Artifacts

`Artifacts` is a `BTreeMap<String, Vec<u8>>` so iteration is ordered and hashing
is stable. When a job starts, the scheduler merges the outputs of its direct
dependencies into the `inputs` it passes to `run`. The `ShellExecutor` reads
declared `produces` files off disk after a successful run; the mock supplies
artifacts from its script. This is how an early job's file reaches a later job.

## Why a text config with a printer

The config is a small readable block format rather than JSON or YAML, and it
ships with both a parser and a `Display` printer that round-trip: parsing then
printing then parsing yields an identical `Pipeline`. That round-trip is a test
in itself and keeps the format honest, since a field the printer forgets or the
parser drops shows up immediately as an inequality.

## Honest limitations

- The single-threaded `Executor` seam means the reference `ShellExecutor`
  materializes the parallel schedule sequentially. The scheduler computes a real
  N-way parallel plan and the mock proves it, but genuine multi-core shell
  execution would need a different, non-pure executor and is deliberately out of
  scope.
- Cache entries are never evicted; the store grows unbounded.
- Step commands are run through `sh -c`, so the engine is Unix-oriented.
- A `step` command cannot be a lone `}` on its own line, because the line-based
  parser reserves that token for closing a block.

## Non-goals

Distributed or remote runners, containers and sandboxing, secrets management, a
web UI or server, hosted log streaming, retries and backoff, remote artifact
storage, and matrix expansion are all out of scope. Relay is meant to be read and
learned from, and to pair with Metronome as a second small, honest systems
project.
