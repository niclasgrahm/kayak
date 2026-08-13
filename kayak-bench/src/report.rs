//! What a run produced, how it is printed, and how it is filed.
//!
//! The two halves are deliberately different in kind. The **per-scenario
//! numbers** are absolute — messages a second — and are only comparable to
//! numbers taken on the same machine, which is why they are stored per machine
//! and why the manifest travels with them. The **ratios** are the numbers that
//! survive leaving the machine: a ratio divides two runs taken seconds apart on
//! one box, so the cpu, the compiler and the background load cancel. Those are
//! the ones worth quoting in a review, putting in a threshold, or comparing
//! against a number someone recorded on different hardware a year ago.
//!
//! Everything in this module is pure. It takes counted results in and produces
//! text and JSON; running a pipeline is [`crate::harness`]'s job.

use std::collections::BTreeMap;
// Writing into a `String` cannot fail, so every `writeln!` below discards its
// `Result` — the alternative is an `unwrap` per line for an error that has no
// way to happen.
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::manifest::Manifest;
use crate::scenario::{REFERENCE, Scenario};

/// One scenario's answer.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Measurement {
    pub name: String,
    /// The graph that produced it, restated so a baseline file can be read
    /// without the code that generated it.
    pub pipelines: usize,
    pub batch_size: usize,
    pub transforms: usize,
    pub depth: usize,
    pub watched: bool,
    /// Messages a second entering the root pipelines — the headline number.
    pub ingested_per_sec: f64,
    /// Messages a second entering *any* pipeline. Equal to `ingested_per_sec`
    /// unless the scenario has depth.
    pub handled_per_sec: f64,
    /// Passes through the run loop a second, across the root pipelines —
    /// `ingested_per_sec` over the batch size.
    ///
    /// The column that makes the batch sweep readable, and the one to look at
    /// first when a row has no transforms: with an empty chain and a
    /// discarding output, *nothing in the run loop ever touches an individual
    /// message* — the batch is an `Arc` that is cloned rather than walked, and
    /// the counters take its length. So the empty rows measure the cost of a
    /// pass and nothing else, their messages-a-second is exactly that times
    /// the batch size, and reading 7 GB/s off one of them as a data rate is
    /// reading the batch size back out.
    pub passes_per_sec: f64,
    /// `ingested_per_sec` divided by the number of root pipelines. This is the
    /// one to read down the `pipelines*` rows: total throughput going up while
    /// this falls is what "scaling, but not for free" looks like.
    pub per_pipeline_per_sec: f64,
    /// Failures counted during the window. Anything but zero means the row is
    /// measuring something other than what it claims.
    pub errors: u64,
    /// The process' resident set at the end of the run, when it could be read.
    pub resident_bytes: Option<u64>,
    /// The wall clock the window actually took, which is not exactly the
    /// duration asked for.
    pub seconds: f64,
}

impl Measurement {
    /// Whether this row is trustworthy enough to compare or save. A row that
    /// errored measured a broken graph, and a row that moved no messages
    /// measured nothing at all.
    #[must_use]
    pub fn is_sound(&self) -> bool {
        self.errors == 0 && self.ingested_per_sec > 0.0
    }
}

/// A whole run: the environment, every measurement, and what they say together.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Report {
    pub manifest: Manifest,
    pub measurements: Vec<Measurement>,
}

/// A derived number that survives leaving the machine it was taken on.
#[derive(Clone, Debug, PartialEq)]
pub struct Ratio {
    pub name: &'static str,
    pub description: &'static str,
    pub value: f64,
    /// Which way is good news, so the printed arrow doesn't have to be
    /// remembered. A cost ratio wants to be high (little was lost); a
    /// throughput multiplier wants to be high too — there is deliberately no
    /// ratio here where lower is better, because a mixed table is one nobody
    /// reads correctly at a glance.
    pub ideal: f64,
}

impl Report {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Measurement> {
        self.measurements.iter().find(|m| m.name == name)
    }

    /// The measurement every ratio divides by.
    fn reference(&self) -> Option<&Measurement> {
        self.get(REFERENCE).filter(|m| m.is_sound())
    }

    /// The ratios this run has the rows to compute. A partial run — `--filter`
    /// or a scenario that failed — simply produces fewer of them rather than
    /// producing wrong ones.
    #[must_use]
    pub fn ratios(&self) -> Vec<Ratio> {
        let Some(reference) = self.reference() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut against_reference = |name, description, of: fn(&Measurement) -> f64| {
            if let Some(m) = self.get(name).filter(|m| m.is_sound()) {
                out.push(Ratio {
                    name,
                    description,
                    value: of(m) / of(reference),
                    ideal: 1.0,
                });
            }
        };
        against_reference(
            "watched",
            "throughput with a browser attached to /events, against nobody watching",
            |m| m.ingested_per_sec,
        );
        against_reference(
            "filter1",
            "throughput with one filter, against an empty chain that touches no message",
            |m| m.ingested_per_sec,
        );
        against_reference(
            "map1",
            "throughput with one map, against an empty chain that touches no message",
            |m| m.ingested_per_sec,
        );
        against_reference(
            "depth3",
            "throughput ingested three pipelines deep, against one",
            |m| m.ingested_per_sec,
        );
        against_reference(
            "pipelines10",
            "per-pipeline throughput at ten, against one",
            |m| m.per_pipeline_per_sec,
        );
        against_reference(
            "pipelines100",
            "per-pipeline throughput at a hundred, against one",
            |m| m.per_pipeline_per_sec,
        );
        against_reference(
            "pipelines1000",
            "per-pipeline throughput at a thousand, against one",
            |m| m.per_pipeline_per_sec,
        );
        // The marginal cost of a transform, which is the one ratio that isn't
        // against the reference: five filters against one says what each of
        // the four extra cost, with the fixed per-pass work already paid in
        // both terms.
        if let (Some(one), Some(five)) = (
            self.get("filter1").filter(|m| m.is_sound()),
            self.get("filter5").filter(|m| m.is_sound()),
        ) {
            out.push(Ratio {
                name: "filter5/filter1",
                description: "throughput at five filters, against one",
                value: five.ingested_per_sec / one.ingested_per_sec,
                ideal: 1.0,
            });
        }
        out
    }

    /// The measurements by name, which is the shape a baseline file compares
    /// in — a run that skipped a scenario must not read as a regression to
    /// zero on it.
    #[must_use]
    pub fn by_name(&self) -> BTreeMap<&str, &Measurement> {
        self.measurements
            .iter()
            .map(|m| (m.name.as_str(), m))
            .collect()
    }
}

/// Turn a counted run into a measurement.
#[must_use]
pub fn measure(
    scenario: &Scenario,
    counted: crate::harness::Counted,
    elapsed: std::time::Duration,
    resident_bytes: Option<u64>,
) -> Measurement {
    // A window that somehow took no time would divide by zero on its way to
    // reporting an infinity, which then poisons every ratio it appears in.
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    #[allow(clippy::cast_precision_loss)]
    let ingested_per_sec = counted.ingested as f64 / seconds;
    #[allow(clippy::cast_precision_loss)]
    let batch_size = scenario.batch_size as f64;
    Measurement {
        name: scenario.name.to_string(),
        pipelines: scenario.pipelines,
        batch_size: scenario.batch_size,
        transforms: scenario.chain.count(),
        depth: scenario.depth,
        watched: scenario.watched,
        ingested_per_sec,
        passes_per_sec: ingested_per_sec / batch_size,
        #[allow(clippy::cast_precision_loss)]
        handled_per_sec: counted.handled as f64 / seconds,
        #[allow(clippy::cast_precision_loss)]
        per_pipeline_per_sec: ingested_per_sec / scenario.pipelines as f64,
        errors: counted.errors,
        resident_bytes,
        seconds,
    }
}

/// A number of messages a second, at a width a column can rely on.
#[must_use]
pub fn rate(value: f64) -> String {
    match value {
        v if v >= 1e9 => format!("{:.2}G", v / 1e9),
        v if v >= 1e6 => format!("{:.2}M", v / 1e6),
        v if v >= 1e3 => format!("{:.1}k", v / 1e3),
        v => format!("{v:.0}"),
    }
}

/// Bytes, at the same width.
#[must_use]
pub fn bytes(value: Option<u64>) -> String {
    let Some(v) = value else {
        return "-".to_string();
    };
    #[allow(clippy::cast_precision_loss)]
    let v = v as f64;
    match v {
        v if v >= 1e9 => format!("{:.2}G", v / 1e9),
        v if v >= 1e6 => format!("{:.0}M", v / 1e6),
        v => format!("{:.0}k", v / 1e3),
    }
}

/// The measurements as a table.
#[must_use]
pub fn table(report: &Report) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<16} {:>6} {:>6} {:>4} {:>10} {:>9} {:>10} {:>7} {:>6}",
        "scenario", "pipes", "batch", "tf", "msgs/s", "passes/s", "per pipe", "rss", "errors"
    );
    let _ = writeln!(out, "{}", "-".repeat(80));
    for m in &report.measurements {
        let _ = writeln!(
            out,
            "{:<16} {:>6} {:>6} {:>4} {:>10} {:>9} {:>10} {:>7} {:>6}",
            m.name,
            m.pipelines * m.depth,
            m.batch_size,
            m.transforms,
            rate(m.ingested_per_sec),
            rate(m.passes_per_sec),
            rate(m.per_pipeline_per_sec),
            bytes(m.resident_bytes),
            m.errors,
        );
    }
    out
}

/// The ratios as a table, with what each of them means.
#[must_use]
pub fn ratio_table(report: &Report) -> String {
    let ratios = report.ratios();
    if ratios.is_empty() {
        return format!(
            "no ratios: they are all taken against '{REFERENCE}', which this run did not \
             measure\n"
        );
    }
    let mut out = String::new();
    let _ = writeln!(out, "{:<18} {:>7}   meaning", "ratio", "value");
    let _ = writeln!(out, "{}", "-".repeat(72));
    for r in ratios {
        let _ = writeln!(out, "{:<18} {:>6.2}x   {}", r.name, r.value, r.description);
    }
    out
}

/// What changed against a stored baseline, as a table.
///
/// Print-only and deliberately without a threshold or an exit code. A gate
/// needs to know how much run-to-run noise this suite actually has on this
/// machine, and that is a question a few weeks of recorded runs answer and
/// a guess does not.
#[must_use]
pub fn comparison(current: &Report, baseline: &Report) -> String {
    let old = baseline.by_name();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "against the baseline taken at commit {} ({} scenarios)",
        baseline.manifest.commit,
        baseline.measurements.len()
    );
    let _ = writeln!(
        out,
        "{:<16} {:>10} {:>10} {:>9}",
        "scenario", "baseline", "now", "change"
    );
    let _ = writeln!(out, "{}", "-".repeat(72));
    for m in &current.measurements {
        let Some(was) = old.get(m.name.as_str()) else {
            let _ = writeln!(
                out,
                "{:<16} {:>10} {:>10} {:>9}",
                m.name,
                "-",
                rate(m.ingested_per_sec),
                "new"
            );
            continue;
        };
        let change = if was.ingested_per_sec > 0.0 {
            format!(
                "{:+.1}%",
                (m.ingested_per_sec / was.ingested_per_sec - 1.0) * 100.0
            )
        } else {
            "-".to_string()
        };
        let _ = writeln!(
            out,
            "{:<16} {:>10} {:>10} {:>9}",
            m.name,
            rate(was.ingested_per_sec),
            rate(m.ingested_per_sec),
            change
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{Measurement, Report, bytes, comparison, rate, ratio_table};
    use crate::manifest::Manifest;

    fn measurement(name: &str, per_sec: f64) -> Measurement {
        Measurement {
            name: name.to_string(),
            pipelines: 1,
            batch_size: 100,
            transforms: 0,
            depth: 1,
            watched: false,
            ingested_per_sec: per_sec,
            passes_per_sec: per_sec / 100.0,
            handled_per_sec: per_sec,
            per_pipeline_per_sec: per_sec,
            errors: 0,
            resident_bytes: Some(50_000_000),
            seconds: 5.0,
        }
    }

    fn report(measurements: Vec<Measurement>) -> Report {
        Report {
            manifest: Manifest::capture(),
            measurements,
        }
    }

    #[test]
    fn a_ratio_divides_the_scenario_by_the_reference() {
        let r = report(vec![
            measurement("batch100", 1000.0),
            measurement("watched", 600.0),
        ]);
        let ratios = r.ratios();
        let watched = ratios
            .iter()
            .find(|x| x.name == "watched")
            .unwrap_or_else(|| panic!("no 'watched' ratio in {ratios:?}"));
        assert!((watched.value - 0.6).abs() < 1e-9, "{}", watched.value);
    }

    /// Every ratio is against the reference, so a run that skipped it has to
    /// produce none rather than dividing by whatever happens to be first.
    #[test]
    fn without_the_reference_there_are_no_ratios() {
        let r = report(vec![measurement("watched", 600.0)]);
        assert!(r.ratios().is_empty());
        assert!(ratio_table(&r).contains("no ratios"));
    }

    /// A row that failed measured a broken graph. Letting it into a ratio
    /// would turn a pipeline that errored on every batch into a headline
    /// performance win.
    #[test]
    fn a_row_that_errored_is_left_out_of_the_ratios() {
        let mut broken = measurement("watched", 90_000.0);
        broken.errors = 12;
        let r = report(vec![measurement("batch100", 1000.0), broken]);
        assert!(r.ratios().iter().all(|x| x.name != "watched"));
    }

    #[test]
    fn a_reference_that_errored_disqualifies_every_ratio() {
        let mut reference = measurement("batch100", 1000.0);
        reference.errors = 1;
        let r = report(vec![reference, measurement("watched", 600.0)]);
        assert!(r.ratios().is_empty());
    }

    /// The marginal-transform ratio is the one taken against something other
    /// than the reference.
    #[test]
    fn the_transform_ratio_is_five_filters_against_one() {
        let r = report(vec![
            measurement("batch100", 1000.0),
            measurement("filter1", 800.0),
            measurement("filter5", 400.0),
        ]);
        let ratios = r.ratios();
        let marginal = ratios
            .iter()
            .find(|x| x.name == "filter5/filter1")
            .unwrap_or_else(|| panic!("no marginal ratio in {ratios:?}"));
        assert!((marginal.value - 0.5).abs() < 1e-9, "{}", marginal.value);
    }

    /// A scenario the baseline never measured is new, not a regression — the
    /// suite is expected to grow.
    #[test]
    fn a_scenario_missing_from_the_baseline_reads_as_new() {
        let now = report(vec![measurement("batch100", 1000.0), measurement("map1", 900.0)]);
        let then = report(vec![measurement("batch100", 1000.0)]);
        let text = comparison(&now, &then);
        assert!(text.contains("new"), "{text}");
        assert!(text.contains("+0.0%"), "{text}");
    }

    #[test]
    fn a_slower_run_reads_as_a_negative_change() {
        let now = report(vec![measurement("batch100", 750.0)]);
        let then = report(vec![measurement("batch100", 1000.0)]);
        assert!(comparison(&now, &then).contains("-25.0%"));
    }

    #[test]
    fn rates_and_sizes_are_scaled_to_a_readable_unit() {
        assert_eq!(rate(12.0), "12");
        assert_eq!(rate(1_500.0), "1.5k");
        assert_eq!(rate(2_400_000.0), "2.40M");
        assert_eq!(bytes(None), "-");
        assert_eq!(bytes(Some(1_500_000_000)), "1.50G");
    }
}
