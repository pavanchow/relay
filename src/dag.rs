//! The dependency graph: built from `needs`, validated for missing edges and
//! cycles, and layered into topological waves. Jobs keep their config index so
//! every operation has a deterministic, reproducible tie-break.

use std::collections::HashMap;

use crate::error::Error;
use crate::pipeline::Pipeline;

#[derive(Debug)]
pub struct Dag {
    /// Job names in config order; index into this is the canonical job id.
    pub order: Vec<String>,
    index: HashMap<String, usize>,
    /// deps[i] = indices of the jobs that job i needs.
    pub deps: Vec<Vec<usize>>,
    /// dependents[i] = indices of the jobs that need job i.
    pub dependents: Vec<Vec<usize>>,
    /// Topological level (longest path from any root) per job.
    pub levels: Vec<usize>,
}

impl Dag {
    pub fn build(pipeline: &Pipeline) -> Result<Dag, Error> {
        let order: Vec<String> = pipeline.jobs.iter().map(|j| j.name.clone()).collect();

        let mut index = HashMap::new();
        for (i, name) in order.iter().enumerate() {
            if index.insert(name.clone(), i).is_some() {
                return Err(Error::DuplicateJob(name.clone()));
            }
        }

        let mut deps = vec![Vec::new(); order.len()];
        for (i, job) in pipeline.jobs.iter().enumerate() {
            for need in &job.needs {
                let d = *index.get(need).ok_or_else(|| Error::MissingDependency {
                    job: job.name.clone(),
                    needs: need.clone(),
                })?;
                if d == i {
                    return Err(Error::Cycle(vec![job.name.clone()]));
                }
                if !deps[i].contains(&d) {
                    deps[i].push(d);
                }
            }
        }

        let mut dependents = vec![Vec::new(); order.len()];
        for (i, ds) in deps.iter().enumerate() {
            for &d in ds {
                dependents[d].push(i);
            }
        }

        let levels = topo_levels(&deps, &dependents, &order)?;

        Ok(Dag { order, index, deps, dependents, levels })
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Jobs grouped by topological level; each wave is in config order.
    pub fn waves(&self) -> Vec<Vec<usize>> {
        let max = self.levels.iter().copied().max().unwrap_or(0);
        let mut waves = vec![Vec::new(); if self.order.is_empty() { 0 } else { max + 1 }];
        for (i, &lvl) in self.levels.iter().enumerate() {
            waves[lvl].push(i);
        }
        waves
    }
}

/// Kahn's algorithm: detects cycles and, as a side effect, computes the
/// longest-path level of every node (its wave).
fn topo_levels(
    deps: &[Vec<usize>],
    dependents: &[Vec<usize>],
    order: &[String],
) -> Result<Vec<usize>, Error> {
    let n = deps.len();
    let mut indeg: Vec<usize> = deps.iter().map(Vec::len).collect();
    let mut level = vec![0usize; n];
    let mut queue: Vec<usize> = (0..n).filter(|&i| indeg[i] == 0).collect();

    let mut processed = 0;
    let mut qi = 0;
    while qi < queue.len() {
        let u = queue[qi];
        qi += 1;
        processed += 1;
        for &v in &dependents[u] {
            if level[v] < level[u] + 1 {
                level[v] = level[u] + 1;
            }
            indeg[v] -= 1;
            if indeg[v] == 0 {
                queue.push(v);
            }
        }
    }

    if processed != n {
        let cycle: Vec<String> = (0..n)
            .filter(|&i| indeg[i] > 0)
            .map(|i| order[i].clone())
            .collect();
        return Err(Error::Cycle(cycle));
    }

    Ok(level)
}
