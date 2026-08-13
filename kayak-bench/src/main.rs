//! The throughput harness: what the run loop costs, as a table and as a file.
//!
//! It drives the runtime in process — no socket, no broker, no filesystem —
//! through the same seams the integration tests use, and measures with the
//! counters the run loop already keeps. See `docs/guide.md`'s "benchmarking"
//! section for what the numbers mean and how a baseline is meant to be used.
//!
//! Two things it deliberately is not. It is **not part of `just ci`**: a
//! minute-long sweep in the pre-push loop is a minute-long sweep people learn
//! to skip. And it is **not a gate** — `--compare` prints a delta table and
//! stops there, because a threshold needs to be set from measured run-to-run
//! noise on a real machine rather than guessed at now.

mod harness;
mod manifest;
mod report;
mod scenario;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

use manifest::Manifest;
use report::Report;

/// Where committed baselines live, relative to the workspace root.
const BASELINE_DIR: &str = "bench/baselines";

#[derive(Parser, Debug)]
#[command(about = "measure what kayak's run loop costs, and compare it to a recorded baseline")]
struct Args {
    /// seconds to measure each scenario for. Shorter runs are noisier; five is
    /// enough to be stable on a quiet machine and short enough that the whole
    /// suite is a coffee rather than a lunch.
    #[arg(long, default_value_t = 5.0)]
    duration: f64,

    /// seconds to let each graph settle before the counters are read. This is
    /// what keeps the cost of spawning a thousand tasks out of the throughput
    /// of running them.
    #[arg(long, default_value_t = 1.0)]
    warmup: f64,

    /// only run scenarios whose name contains this
    #[arg(long)]
    filter: Option<String>,

    /// print the run as JSON instead of as tables
    #[arg(long)]
    json: bool,

    /// compare against this machine's recorded baseline and print the deltas
    #[arg(long)]
    compare: bool,

    /// record this run as this machine's baseline. Refused for a debug build
    /// and for a run that skipped scenarios — both produce a file that later
    /// runs would be compared against wrongly.
    #[arg(long)]
    save: bool,

    /// where baselines are read from and written to
    #[arg(long, default_value = BASELINE_DIR)]
    baseline_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let scenarios = scenario::filtered(args.filter.as_deref());
    if scenarios.is_empty() {
        anyhow::bail!(
            "no scenario matches '{}'",
            args.filter.unwrap_or_default()
        );
    }
    let manifest = Manifest::capture();
    if !args.json {
        print_header(&manifest, &args, scenarios.len());
    }

    // A fresh runtime per process rather than per scenario: the scenarios are
    // run one after another on one multi-threaded runtime, which is the shape
    // a server has. `run` waits for every task of a scenario to finish before
    // returning, so nothing leaks into the next one.
    let runtime = tokio::runtime::Runtime::new().context("starting the tokio runtime")?;
    let warmup = Duration::from_secs_f64(args.warmup);
    let duration = Duration::from_secs_f64(args.duration);

    let mut measurements = Vec::with_capacity(scenarios.len());
    for s in &scenarios {
        if !args.json {
            println!("  {:<16} {}", s.name, s.description);
        }
        let (counted, elapsed) = runtime
            .block_on(harness::run(s, warmup, duration))
            .with_context(|| format!("running scenario '{}'", s.name))?;
        measurements.push(report::measure(
            s,
            counted,
            elapsed,
            manifest::resident_bytes(),
        ));
    }
    let current = Report {
        manifest,
        measurements,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&current)?);
    } else {
        println!("\n{}", report::table(&current));
        println!("{}", report::ratio_table(&current));
        warn_about_unsound_rows(&current);
    }

    let path = baseline_path(&args.baseline_dir, &current.manifest);
    if args.compare {
        match read_baseline(&path)? {
            Some(baseline) => println!("{}", report::comparison(&current, &baseline)),
            None => println!(
                "no baseline for this machine yet — take one with `just bench --save`\n  ({})",
                path.display()
            ),
        }
    }
    if args.save {
        save_baseline(&path, &current, args.filter.is_some())?;
    }
    Ok(())
}

fn print_header(manifest: &Manifest, args: &Args, scenarios: usize) {
    println!(
        "kayak bench — {} scenarios, {}s each after {}s of warmup",
        scenarios, args.duration, args.warmup
    );
    println!(
        "  {} on {} ({} cores), {}, {}",
        manifest.commit, manifest.cpu, manifest.cores, manifest.os, manifest.rustc
    );
    if !manifest.is_release() {
        println!(
            "  NOTE: this is a debug build. The numbers are not a baseline — \
             run `just bench` for that."
        );
    }
    if manifest.commit.ends_with("-dirty") {
        println!("  NOTE: the tree has uncommitted changes, so this run names no commit exactly.");
    }
    println!();
}

/// Say so when a row measured something other than what it claims. Printed
/// rather than returned as an error: the rest of the table is still worth
/// reading, and the ratios have already left the bad rows out.
fn warn_about_unsound_rows(report: &Report) {
    for m in &report.measurements {
        if m.errors > 0 {
            println!(
                "  '{}' counted {} errors — that row measured a broken graph and is left out of \
                 the ratios",
                m.name, m.errors
            );
        } else if m.ingested_per_sec <= 0.0 {
            println!("  '{}' moved no messages at all", m.name);
        }
    }
}

/// This machine's baseline file. Per machine because an absolute throughput
/// number is only comparable to one taken on the same hardware — see
/// [`Manifest::machine_id`].
fn baseline_path(dir: &Path, manifest: &Manifest) -> PathBuf {
    dir.join(format!("{}.json", manifest.machine_id()))
}

fn read_baseline(path: &Path) -> Result<Option<Report>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading the baseline at {}", path.display()))?;
    let report = serde_json::from_str(&text)
        .with_context(|| format!("parsing the baseline at {}", path.display()))?;
    Ok(Some(report))
}

/// Write this run as the machine's baseline, or refuse and say why.
///
/// Both refusals are about a file that would be compared against wrongly
/// later: a debug run measures the optimiser's absence, and a filtered run
/// would silently drop every scenario it didn't measure from the recorded set.
fn save_baseline(path: &Path, report: &Report, filtered: bool) -> Result<()> {
    if !report.manifest.is_release() {
        anyhow::bail!(
            "refusing to save a debug build as a baseline — it measures the optimiser's absence. \
             Run `just bench --save`, which builds with --release."
        );
    }
    if filtered {
        anyhow::bail!(
            "refusing to save a filtered run as a baseline — it would drop every scenario it \
             didn't measure. Run the whole suite, or edit the file by hand."
        );
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(report)?;
    std::fs::write(path, format!("{text}\n"))
        .with_context(|| format!("writing the baseline to {}", path.display()))?;
    println!("baseline written to {}", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{baseline_path, read_baseline, save_baseline};
    use crate::manifest::Manifest;
    use crate::report::{Measurement, Report};
    use std::path::Path;

    fn report(profile: &'static str) -> Report {
        let mut manifest = Manifest::capture();
        manifest.profile = profile.to_string();
        manifest.cpu = "Test CPU".to_string();
        manifest.os = "TestOS 1".to_string();
        manifest.cores = 4;
        Report {
            manifest,
            measurements: vec![Measurement {
                name: "batch100".to_string(),
                pipelines: 1,
                batch_size: 100,
                transforms: 0,
                depth: 1,
                watched: false,
                ingested_per_sec: 1234.0,
                passes_per_sec: 12.34,
                handled_per_sec: 1234.0,
                per_pipeline_per_sec: 1234.0,
                errors: 0,
                resident_bytes: None,
                seconds: 5.0,
            }],
        }
    }

    #[test]
    fn a_baseline_is_filed_under_the_machine_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("kayak-bench-{}", std::process::id()));
        let report = report("release");
        let path = baseline_path(&dir, &report.manifest);
        assert_eq!(
            path.file_name().and_then(std::ffi::OsStr::to_str),
            Some("test-cpu-4c-testos-1.json")
        );
        if let Err(e) = save_baseline(&path, &report, false) {
            panic!("saving the baseline: {e:#}");
        }
        let back = match read_baseline(&path) {
            Ok(Some(r)) => r,
            other => panic!("reading the baseline back: {other:?}"),
        };
        assert_eq!(back.measurements.len(), 1);
        assert!((back.measurements[0].ingested_per_sec - 1234.0).abs() < 1e-9);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A debug number saved as a baseline makes every later release run look
    /// like a several-hundred-percent improvement, forever.
    #[test]
    fn a_debug_run_is_refused_as_a_baseline() {
        let dir = std::env::temp_dir().join(format!("kayak-bench-debug-{}", std::process::id()));
        let report = report("debug");
        let path = baseline_path(&dir, &report.manifest);
        let Err(e) = save_baseline(&path, &report, false) else {
            panic!("a debug build was accepted as a baseline");
        };
        assert!(format!("{e:#}").contains("debug"), "{e:#}");
        assert!(!path.exists());
    }

    /// A filtered run has fewer rows than the suite, and saving it would drop
    /// the rest from the recorded set without saying so.
    #[test]
    fn a_filtered_run_is_refused_as_a_baseline() {
        let dir = std::env::temp_dir().join(format!("kayak-bench-filtered-{}", std::process::id()));
        let report = report("release");
        let path = baseline_path(&dir, &report.manifest);
        let Err(e) = save_baseline(&path, &report, true) else {
            panic!("a filtered run was accepted as a baseline");
        };
        assert!(format!("{e:#}").contains("filtered"), "{e:#}");
    }

    #[test]
    fn a_machine_with_no_baseline_yet_reads_as_none() {
        let missing = Path::new("/nonexistent/kayak/baseline.json");
        match read_baseline(missing) {
            Ok(None) => {}
            other => panic!("expected no baseline, got {other:?}"),
        }
    }
}
