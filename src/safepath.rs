//! Path confinement. A declared path (a `produces`, `consumes`, or `cache.paths`
//! entry) must stay inside the job's workspace. Absolute paths and any `..`
//! component can read or clobber files outside `base_dir`, so they are rejected.

use std::path::{Component, Path};

/// True when `p` is a relative path with no `..` component and no root/prefix, so
/// joining it onto `base_dir` cannot escape `base_dir`.
pub fn is_confined(p: &str) -> bool {
    let path = Path::new(p);
    if path.is_absolute() {
        return false;
    }
    for component in path.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return false,
            _ => {}
        }
    }
    true
}
