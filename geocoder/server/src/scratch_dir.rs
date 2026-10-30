//! A unique directory under the system temp dir for one test, removed when
//! dropped, so neither a passing nor a panicking test leaves files behind.
//! Declared by both lib.rs and main.rs because their tests build separately.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn candidate(prefix: &str, n: usize) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}_{}_{n}", std::process::id()))
}

/// Creates the first `name(n)` that didn't exist before, so a leftover from an
/// earlier run with the same pid is skipped, never reused or removed.
fn create_new(next: &AtomicUsize, name: impl Fn(usize) -> PathBuf) -> PathBuf {
    loop {
        let path = name(next.fetch_add(1, Ordering::Relaxed));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("scratch dir {}: {e}", path.display()),
        }
    }
}

pub struct ScratchDir(PathBuf);

impl ScratchDir {
    pub fn new() -> Self {
        ScratchDir(create_new(&NEXT, |n| candidate("gc_test", n)))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scratch_dir_is_new_and_leaves_existing_dirs_alone() {
        // Its own names and counter, so no concurrent test takes the same dirs.
        static TEST_NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = |n| candidate("gc_test_stale", n);
        let stale: Vec<PathBuf> = (0..64).map(name).collect();
        for p in &stale {
            std::fs::create_dir_all(p).unwrap();
            std::fs::write(p.join("stale"), b"x").unwrap();
        }

        let dir = ScratchDir(create_new(&TEST_NEXT, name));
        let reused = stale.iter().any(|p| p == dir.path());
        let empty = std::fs::read_dir(dir.path()).unwrap().next().is_none();
        drop(dir);
        let kept = stale.iter().all(|p| p.join("stale").exists());
        for p in &stale {
            let _ = std::fs::remove_dir_all(p);
        }

        assert!(!reused && empty, "ScratchDir reused a directory that already existed");
        assert!(kept, "dropping a ScratchDir removed a directory it didn't create");
    }
}
