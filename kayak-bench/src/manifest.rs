//! The environment a run happened in.
//!
//! This is half of what a baseline is, and the half that is usually left out.
//! A throughput number on its own is not comparable to anything: the same
//! commit measured on a laptop on battery, in a container with two cores, and
//! on a workstation will differ by more than most regressions anyone cares
//! about. So every recorded run carries where it came from, and baselines are
//! filed under [`Manifest::machine_id`] rather than in one shared file.
//!
//! Nothing here fails a run. A machine that won't say what its cpu is still
//! produces perfectly good numbers; it just produces ones that have to be
//! compared by hand.

use std::process::Command;

use serde::{Deserialize, Serialize};

/// Where and when a run happened, and against what.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Manifest {
    /// The commit the runtime was built from, with `-dirty` when the tree had
    /// uncommitted changes. The dirty marker earns its keep: an unexplained
    /// 30% is very often a number taken against edits that were never
    /// committed.
    pub commit: String,
    /// `rustc --version`. A compiler upgrade moves throughput on its own, and
    /// it is the first thing to check when every row moved at once.
    pub rustc: String,
    /// The cargo profile the bench binary was built with. A debug-profile
    /// number is not a slower version of a release one, it is a different
    /// measurement — see [`Manifest::is_release`].
    pub profile: String,
    pub os: String,
    pub cpu: String,
    /// Physical-ish core count as the OS reports it. Together with `cpu` it is
    /// what makes the multi-pipeline rows comparable at all.
    pub cores: usize,
    pub total_memory_bytes: u64,
    /// Seconds since the epoch, so a baseline file says how stale it is.
    pub taken_at: u64,
}

impl Manifest {
    /// Read everything about this machine and this build.
    #[must_use]
    pub fn capture() -> Self {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        system.refresh_cpu_all();
        let cpu = system
            .cpus()
            .first()
            .map_or_else(|| "unknown".to_string(), |c| c.brand().trim().to_string());
        Self {
            commit: commit(),
            rustc: shell("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string()),
            profile: if cfg!(debug_assertions) { "debug" } else { "release" }.to_string(),
            os: format!(
                "{} {}",
                sysinfo::System::name().unwrap_or_else(|| "unknown".to_string()),
                sysinfo::System::os_version().unwrap_or_default(),
            )
            .trim()
            .to_string(),
            cpu: if cpu.is_empty() { "unknown".to_string() } else { cpu },
            cores: sysinfo::System::physical_core_count().unwrap_or_else(num_cpus_fallback),
            total_memory_bytes: system.total_memory(),
            taken_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        }
    }

    /// The key a baseline is filed under: everything that has to match for two
    /// numbers to be worth subtracting. Deliberately *not* the commit — the
    /// whole point is to compare across commits — and deliberately not the
    /// rustc version either, since a toolchain bump should show up as a
    /// visible move in the numbers rather than as a silently fresh baseline.
    #[must_use]
    pub fn machine_id(&self) -> String {
        let slug: String = format!("{}-{}c-{}", self.cpu, self.cores, self.os)
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
            .collect();
        // collapse the runs of dashes the mapping above leaves behind
        let mut out = String::with_capacity(slug.len());
        for c in slug.chars() {
            if c != '-' || !out.ends_with('-') {
                out.push(c);
            }
        }
        out.trim_matches('-').to_string()
    }

    /// Whether this build is one whose numbers are worth recording.
    ///
    /// A debug build measures the optimiser's absence, and it does it
    /// inconsistently — some of the hot paths here are generic and inline away
    /// entirely under `--release`. A debug run is useful for checking the
    /// harness works and useless as a baseline, so `main` refuses to save one.
    #[must_use]
    pub fn is_release(&self) -> bool {
        self.profile == "release"
    }
}

/// `git rev-parse --short HEAD`, plus a `-dirty` marker.
fn commit() -> String {
    let Some(sha) = shell("git", &["rev-parse", "--short", "HEAD"]) else {
        return "unknown".to_string();
    };
    match shell("git", &["status", "--porcelain"]) {
        Some(s) if !s.is_empty() => format!("{sha}-dirty"),
        _ => sha,
    }
}

/// Run a command and take its trimmed stdout, or nothing if it didn't work.
/// A machine with no git and no rustc on the path still benches fine.
fn shell(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// What to say about core count when the OS won't give a physical one — the
/// logical count is wrong in a knowable direction, which beats zero.
fn num_cpus_fallback() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// This process' resident set, in bytes, if it can be read.
///
/// Reported per scenario rather than once, because "how many pipelines fit"
/// is a memory question at least as much as a throughput one and the
/// thousand-pipeline row is where it becomes visible. It is the process' whole
/// RSS, so it includes everything the earlier scenarios left behind — read the
/// column as a high-water mark, not as the cost of that row alone.
#[must_use]
pub fn resident_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    Some(system.process(pid)?.memory())
}

#[cfg(test)]
mod tests {
    use super::Manifest;

    #[test]
    fn capture_says_something_about_every_field() {
        let m = Manifest::capture();
        assert!(!m.commit.is_empty());
        assert!(!m.rustc.is_empty());
        assert!(m.cores >= 1);
        assert!(m.taken_at > 0);
    }

    /// The id becomes a file name, so anything that isn't safe in one has to
    /// have been mapped away — and two dashes in a row would make the same
    /// machine produce two names depending on how its cpu string is spelled.
    #[test]
    fn a_machine_id_is_a_safe_file_name() {
        let mut m = Manifest::capture();
        m.cpu = "Apple M3 Pro (10 cores)".to_string();
        m.os = "Darwin 25.5.0".to_string();
        m.cores = 10;
        let id = m.machine_id();
        assert_eq!(id, "apple-m3-pro-10-cores-10c-darwin-25-5-0", "{id}");
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
        assert!(!id.contains("--"));
    }

    /// Two runs on one machine have to land on one baseline file however far
    /// apart they are, which means nothing that changes between runs may be in
    /// the id.
    #[test]
    fn the_machine_id_ignores_the_commit_and_the_clock() {
        let mut a = Manifest::capture();
        let mut b = a.clone();
        b.commit = "deadbee-dirty".to_string();
        b.taken_at = a.taken_at + 86_400;
        b.rustc = "rustc 9.9.9".to_string();
        a.commit = "0000000".to_string();
        assert_eq!(a.machine_id(), b.machine_id());
    }

    #[test]
    fn a_debug_build_is_not_a_release_one() {
        let m = Manifest::capture();
        assert_eq!(m.is_release(), !cfg!(debug_assertions));
    }
}
