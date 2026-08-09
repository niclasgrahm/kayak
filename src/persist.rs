//! Writing the running pipelines out as a config file.
//!
//! The `--config` file is a load source and a save target, never a mirror:
//! editing the graph changes what the server is *running*, and only an explicit
//! save writes anything to disk. The workflow that buys is the one worth
//! having — build the graph in the UI, save it, commit the file — without the
//! server quietly rewriting a file nobody asked it to touch.
//!
//! That goal is what dictates the two rules here:
//!
//! - **The order is derived, not remembered.** A `pipeline` input can only be
//!   built once its upstream exists, so the file has to declare parents before
//!   children; [`ordered`] topologically sorts them and breaks every tie by id.
//!   Nothing depends on the order pipelines happened to be created in, and the
//!   same graph always renders byte-for-byte the same file — a diff means the
//!   graph changed, not that a `HashMap` was iterated twice.
//! - **The write is atomic.** A crash halfway through must not leave a
//!   truncated config where a working one was, so [`write`] renders the whole
//!   file first and renames it into place.
//!
//! Secrets are safe here for the reason they're safe on the wire: a `Config`
//! only ever holds the unresolved `${NAME}` template.
//!
//! A file can be JSON or YAML — see [`kayak_core::ConfigFormat`]. That is a
//! property of the file and nothing else: the format is decided by the name at
//! the two edges ([`read`] and [`write`]), and every other function here works
//! on `Config`s that no longer remember which one they came from.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use anyhow::Context;
use kayak_core::config::Config;
use kayak_core::state::ConfigFile;
use kayak_core::{ConfigFormat, PipelineId};

/// The pipelines in an order the config file can be replayed in: every pipeline
/// after all of the ones it names as upstream, and alphabetical among the ones
/// that are equally free to go.
///
/// Configs without an `id` sort as if their id were empty. In practice they
/// don't occur — the writer fills the generated id in first, precisely so that
/// a downstream's `upstream` reference still resolves on the next start.
///
/// A cycle can't be built at runtime (an upstream has to exist already), but a
/// hand-edited file could describe one. Rather than dropping those pipelines,
/// whatever is left over is appended in id order: the file stays a complete
/// record of the graph, and the next start reports the unbuildable pipeline.
#[must_use]
pub fn ordered(configs: Vec<Config>) -> Vec<Config> {
    let empty = String::new();
    let id_of = |c: &Config| c.id.clone().unwrap_or_else(|| empty.clone());

    let mut remaining: BTreeMap<PipelineId, Config> =
        configs.into_iter().map(|c| (id_of(&c), c)).collect();
    let mut placed: HashSet<PipelineId> = HashSet::new();
    let mut out: Vec<Config> = Vec::with_capacity(remaining.len());

    // Kahn's algorithm over a BTreeMap, so "the ones that are ready" is always
    // considered in id order and the output is fully determined by the graph.
    loop {
        let ready: Vec<PipelineId> = remaining
            .iter()
            .filter(|(_, config)| {
                config.upstreams().into_iter().all(|up| {
                    // an upstream that isn't in this set is dangling, not
                    // pending — it can never become ready, so don't wait for it
                    placed.contains(up) || !remaining.contains_key(up)
                })
            })
            .map(|(id, _)| id.clone())
            .collect();
        if ready.is_empty() {
            break;
        }
        for id in ready {
            if let Some(config) = remaining.remove(&id) {
                placed.insert(id);
                out.push(config);
            }
        }
    }

    // only reachable through a cycle; see the doc comment
    out.extend(remaining.into_values());
    out
}

/// The file's contents: the pipelines as a sequence, in [`ordered`] order,
/// written in `format`.
///
/// Pretty-printed with a trailing newline, because the file is meant to be
/// read and diffed by a human rather than only by `serde`. Both formats are
/// deterministic in the same way, which is what [`crate::state`] leans on to
/// answer "are there unsaved changes" by comparing rendered bytes.
pub fn render(file: ConfigFile, format: ConfigFormat) -> anyhow::Result<String> {
    // the pipelines are ordered; the buckets order themselves, being a BTreeMap
    let file = ConfigFile::new(file.state, ordered(file.pipelines));
    match format {
        ConfigFormat::Json => {
            let mut json =
                serde_json::to_string_pretty(&file).context("failed to serialize the config")?;
            json.push('\n');
            Ok(json)
        }
        // serde_norway already ends its output in a newline
        ConfigFormat::Yaml => {
            serde_norway::to_string(&file).context("failed to serialize the config")
        }
    }
}

/// A config file's contents — the buckets and the pipelines.
///
/// The counterpart of [`render`], and the only place either format is parsed —
/// everything past here has a [`ConfigFile`] and no idea how it was spelled.
/// It is also the only place the two *spellings* of a config file are told
/// apart; see [`ConfigFile`].
pub fn parse(contents: &str, format: ConfigFormat) -> anyhow::Result<ConfigFile> {
    match format {
        ConfigFormat::Json => serde_json::from_str(contents).map_err(Into::into),
        ConfigFormat::Yaml => serde_norway::from_str(contents).map_err(Into::into),
    }
}

/// Read and parse the config file at `path`, in the format its name implies.
pub fn read(path: &Path) -> anyhow::Result<ConfigFile> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to open config file {}", path.display()))?;
    let format = format_of(path);
    parse(&contents, format)
        .with_context(|| format!("failed to parse config file {} as {format}", path.display()))
}

/// The format a path's extension names. A path with no file name is JSON, the
/// same as any other name that doesn't say otherwise.
#[must_use]
pub fn format_of(path: &Path) -> ConfigFormat {
    path.file_name()
        .map(|name| ConfigFormat::of_file_name(&name.to_string_lossy()))
        .unwrap_or_default()
}

/// Replace `path` with the given pipelines, written in `format`. Callers taking
/// a path from a request must put it through [`save_path`] first.
///
/// Rendered in full and renamed into place, so a failure partway through leaves
/// the previous file untouched rather than a half-written one. The temporary
/// sits next to the target because a rename across filesystems isn't atomic
/// (and on some platforms isn't allowed at all).
pub fn write(path: &Path, file: ConfigFile, format: ConfigFormat) -> anyhow::Result<()> {
    let contents = render(file, format)?;
    let temporary = temporary_path(path);
    std::fs::write(&temporary, contents)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    std::fs::rename(&temporary, path).with_context(|| {
        format!(
            "failed to move {} into place at {}",
            temporary.display(),
            path.display()
        )
    })
}

/// A sibling of the target to stage the new contents in. The name is derived
/// from the target's, so two servers writing two different config files in one
/// directory don't collide.
pub(crate) fn temporary_path(path: &Path) -> std::path::PathBuf {
    let name = path
        .file_name()
        .map_or_else(|| std::ffi::OsString::from("config"), ToOwned::to_owned);
    let mut name = name;
    name.push(".tmp");
    path.with_file_name(name)
}

/// Where a "save as" of `name` is allowed to land, given the one directory the
/// server may write configs to.
///
/// **This is a security boundary, not a convenience.** The browser cannot write
/// to the server's disk — the server does, on request — so an unconstrained
/// path here would let anyone who can reach the UI write a file anywhere the
/// process can. Confining saves to the one directory the operator already
/// pointed at is what keeps a config editor from being an arbitrary-write
/// primitive. `dir` is the `--config` file's directory, or — for a server
/// started without one, which can still be asked to *create* a config — the
/// directory the process was started in. Either way it is chosen by whoever ran
/// the server, never by the request.
///
/// So `name` must be a bare file name. That is enough for everything the
/// feature is for, including overwriting the loaded file — pass its own name.
/// Anything with a separator, a `..`, or a root in it is refused rather than
/// normalised, because normalising is where these checks go wrong: the rule is
/// easier to trust when nothing is rewritten to satisfy it.
pub fn save_path(dir: &Path, name: &str) -> anyhow::Result<std::path::PathBuf> {
    let trimmed = name.trim();
    anyhow::ensure!(!trimmed.is_empty(), "a file name is required");
    anyhow::ensure!(
        !trimmed.contains(['/', '\\']),
        "'{trimmed}' is not a file name: a config can only be saved in the directory the server writes to, so it cannot contain a path"
    );
    // `.` and `..` pass the separator check but name directories, not files
    anyhow::ensure!(
        !trimmed.chars().all(|c| c == '.'),
        "'{trimmed}' is not a file name"
    );
    // belt and braces: whatever the string looked like, it has to come out of
    // `Path` as exactly one ordinary component
    let mut components = Path::new(trimmed).components();
    let (Some(std::path::Component::Normal(only)), None) = (components.next(), components.next())
    else {
        anyhow::bail!("'{trimmed}' is not a file name");
    };
    Ok(dir.join(only))
}

/// The ids named as upstream by any of these pipelines but not declared by any
/// of them. A file like that can't be started, so the caller can refuse to
/// write one rather than persisting a graph that won't come back.
#[must_use]
pub fn dangling_upstreams(configs: &[Config]) -> Vec<PipelineId> {
    let known: HashSet<&str> = configs.iter().filter_map(|c| c.id.as_deref()).collect();
    let missing: BTreeSet<&PipelineId> = configs
        .iter()
        .flat_map(Config::upstreams)
        .filter(|up| !known.contains(up.as_str()))
        .collect();
    missing.into_iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::{
        DummyConfig, InputConfig, InputKind, PipelineConfig, TransformConfig,
    };

    /// Most of these tests are about the *pipelines* — the order they come out
    /// in, the bytes they render to — and predate buckets entirely. Wrapping
    /// here rather than at every call site keeps them saying what they are
    /// about; the bucket half of a config file has its own tests below.
    fn render(configs: Vec<Config>, format: ConfigFormat) -> anyhow::Result<String> {
        super::render(ConfigFile::of_pipelines(configs), format)
    }

    fn write(path: &Path, configs: Vec<Config>, format: ConfigFormat) -> anyhow::Result<()> {
        super::write(path, ConfigFile::of_pipelines(configs), format)
    }

    fn pipeline(id: &str, upstreams: &[&str]) -> Config {
        let mut inputs: Vec<InputConfig> = upstreams
            .iter()
            .map(|up| InputConfig {
                kind: InputKind::Pipeline(PipelineConfig {
                    upstream: (*up).to_string(),
                }),
                buffer: None,
                envelope: None,
            })
            .collect();
        if inputs.is_empty() {
            inputs.push(InputConfig {
                kind: InputKind::Dummy(DummyConfig { duration: 1, payload: None, amplitude: None, period: None }),
                buffer: None,
                envelope: None,
            });
        }
        Config {
            id: Some(id.to_string()),
            inputs,
            transforms: Vec::<TransformConfig>::new(),
            outputs: Vec::new(),
            state: None,
        }
    }

    fn ids(configs: &[Config]) -> Vec<String> {
        configs
            .iter()
            .map(|c| c.id.clone().unwrap_or_default())
            .collect()
    }

    /// The whole point of the ordering: a `pipeline` input can't be built
    /// before its upstream exists, so a file that lists them the other way
    /// round fails to start.
    #[test]
    fn a_pipeline_is_written_after_the_ones_it_reads_from() {
        let out = ordered(vec![
            pipeline("z-child", &["a-root"]),
            pipeline("a-root", &[]),
        ]);
        assert_eq!(ids(&out), ["a-root", "z-child"]);
    }

    /// Two hops deep, and the middle one only becomes writable once the root is.
    #[test]
    fn a_chain_is_written_root_first() {
        let out = ordered(vec![
            pipeline("c", &["b"]),
            pipeline("b", &["a"]),
            pipeline("a", &[]),
        ]);
        assert_eq!(ids(&out), ["a", "b", "c"]);
    }

    /// A pipeline with several parents goes after the last of them, not the first.
    #[test]
    fn a_pipeline_with_several_upstreams_waits_for_all_of_them() {
        let out = ordered(vec![
            pipeline("everything", &["a", "deep"]),
            pipeline("deep", &["a"]),
            pipeline("a", &[]),
        ]);
        assert_eq!(ids(&out), ["a", "deep", "everything"]);
    }

    /// The file is version controlled, so the same graph has to render the same
    /// bytes every time — otherwise every save is a diff.
    #[test]
    fn independent_pipelines_are_written_in_id_order() {
        let out = ordered(vec![
            pipeline("c", &[]),
            pipeline("a", &[]),
            pipeline("b", &[]),
        ]);
        assert_eq!(ids(&out), ["a", "b", "c"]);
    }

    /// ...including when the input order differs, which it will: the pipelines
    /// come out of a `HashMap`.
    #[test]
    fn the_same_graph_renders_the_same_file_whatever_order_it_arrives_in() -> anyhow::Result<()> {
        for format in [ConfigFormat::Json, ConfigFormat::Yaml] {
            let one = render(
                vec![
                    pipeline("child", &["root"]),
                    pipeline("other", &["root"]),
                    pipeline("root", &[]),
                ],
                format,
            )?;
            let two = render(
                vec![
                    pipeline("other", &["root"]),
                    pipeline("root", &[]),
                    pipeline("child", &["root"]),
                ],
                format,
            )?;
            assert_eq!(one, two, "{format}");
        }
        Ok(())
    }

    /// An upstream that was deleted leaves a pipeline that can't be built, but
    /// dropping it from the file would lose the user's work silently. It is
    /// written, and reported separately.
    #[test]
    fn a_pipeline_naming_an_unknown_upstream_is_still_written() {
        let configs = vec![pipeline("orphan", &["was-deleted"]), pipeline("a", &[])];
        assert_eq!(dangling_upstreams(&configs), ["was-deleted"]);
        assert_eq!(ids(&ordered(configs)), ["a", "orphan"]);
    }

    #[test]
    fn a_graph_with_every_upstream_present_has_no_dangling_references() {
        assert!(
            dangling_upstreams(&[pipeline("root", &[]), pipeline("child", &["root"])]).is_empty()
        );
    }

    /// A cycle can't be created through the API, but a hand-edited file can
    /// describe one. Losing those pipelines on the next save would be worse
    /// than writing a file that reports itself as broken on the next start.
    #[test]
    fn a_cycle_is_written_rather_than_dropped() {
        let out = ordered(vec![pipeline("a", &["b"]), pipeline("b", &["a"])]);
        assert_eq!(ids(&out), ["a", "b"]);
    }

    #[test]
    fn the_rendered_file_is_a_json_array_ending_in_a_newline() -> anyhow::Result<()> {
        let rendered = render(vec![pipeline("a", &[])], ConfigFormat::Json)?;
        assert!(rendered.ends_with("]\n"), "got: {rendered}");
        let parsed: Vec<Config> = serde_json::from_str(&rendered)?;
        assert_eq!(ids(&parsed), ["a"]);
        Ok(())
    }

    /// The point of the YAML support: a file a human would rather write, that
    /// still describes the same pipelines. The tagged `type` field survives,
    /// which is the part `#[serde(flatten)]` could plausibly break.
    #[test]
    fn a_yaml_file_is_a_block_sequence_that_parses_back() -> anyhow::Result<()> {
        let rendered = render(
            vec![pipeline("child", &["root"]), pipeline("root", &[])],
            ConfigFormat::Yaml,
        )?;
        assert!(rendered.starts_with("- id: root"), "got: {rendered}");
        assert!(rendered.contains("type: dummy"), "got: {rendered}");
        assert!(rendered.ends_with('\n'), "got: {rendered}");

        let parsed = parse(&rendered, ConfigFormat::Yaml)?;
        assert_eq!(ids(&parsed.pipelines), ["root", "child"]);
        Ok(())
    }

    /// Both formats have to describe the same graph, or "save as yaml" would
    /// quietly change what the next start builds.
    #[test]
    fn the_two_formats_round_trip_to_the_same_pipelines() -> anyhow::Result<()> {
        let graph = || vec![pipeline("child", &["root"]), pipeline("root", &[])];
        let json = parse(&render(graph(), ConfigFormat::Json)?, ConfigFormat::Json)?;
        let yaml = parse(&render(graph(), ConfigFormat::Yaml)?, ConfigFormat::Yaml)?;
        assert_eq!(serde_json::to_value(&json)?, serde_json::to_value(&yaml)?,);
        Ok(())
    }

    /// A `.yaml` file is parsed as YAML and a `.json` file as JSON, whatever is
    /// actually in them — the extension is the whole rule, so an unparseable
    /// file gets an error rather than a second guess.
    #[test]
    fn a_file_is_read_in_the_format_its_name_implies() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        for (name, format) in [
            ("config.json", ConfigFormat::Json),
            ("config.yaml", ConfigFormat::Yaml),
            ("config.yml", ConfigFormat::Yaml),
        ] {
            let path = dir.path().join(name);
            assert_eq!(format_of(&path), format, "{name}");
            std::fs::write(&path, render(vec![pipeline("a", &[])], format)?)?;
            assert_eq!(ids(&read(&path)?.pipelines), ["a"], "{name}");
        }

        let broken = dir.path().join("broken.yaml");
        std::fs::write(&broken, "not: [a, sequence, of, pipelines")?;
        let Err(err) = read(&broken) else {
            panic!("a file that is not yaml was read as one");
        };
        assert!(
            format!("{err:#}").contains("as yaml"),
            "the error should name the format tried: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn a_save_lands_in_the_directory_the_server_writes_configs_to() -> anyhow::Result<()> {
        let dir = Path::new("/srv/kayak");
        assert_eq!(
            save_path(dir, "other.json")?,
            Path::new("/srv/kayak/other.json")
        );
        // saving over the loaded file is the ordinary "save" case
        assert_eq!(
            save_path(dir, "config.json")?,
            Path::new("/srv/kayak/config.json")
        );
        // surrounding whitespace is a typing artefact, not part of a name
        assert_eq!(
            save_path(dir, "  spaced.json  ")?,
            Path::new("/srv/kayak/spaced.json")
        );
        Ok(())
    }

    /// The one that matters: this path comes from an HTTP request, so anything
    /// that escapes the config directory is an arbitrary file write. Each of
    /// these is a way out, and none of them may be normalised into something
    /// acceptable — they are refused.
    #[test]
    fn a_save_cannot_escape_the_config_directory() {
        let dir = Path::new("/srv/kayak");
        for escape in [
            "../outside.json",
            "../../etc/cron.d/kayak",
            "sub/dir.json",
            "/etc/passwd",
            "/srv/kayak/config.json", // absolute, even to the right place
            "..",
            ".",
            "",
            "   ",
            r"..\windows.json",
            r"C:\windows\system32\x.json",
        ] {
            let result = save_path(dir, escape);
            assert!(
                result.is_err(),
                "'{escape}' was accepted as {:?}",
                result.ok()
            );
        }
    }

    /// The staging file has to be a sibling: renaming across filesystems isn't
    /// atomic, which is the only reason the temporary exists.
    #[test]
    fn the_temporary_is_written_next_to_the_target() {
        let path = Path::new("/etc/kayak/config.json");
        let temporary = temporary_path(path);
        assert_eq!(temporary.parent(), path.parent());
        assert_ne!(temporary, path);
    }

    #[test]
    fn writing_replaces_the_file_and_leaves_no_temporary_behind() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("config.json");
        std::fs::write(&path, "[]")?;

        write(
            &path,
            vec![pipeline("child", &["root"]), pipeline("root", &[])],
            ConfigFormat::Json,
        )?;

        let reloaded: Vec<Config> = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
        assert_eq!(ids(&reloaded), ["root", "child"]);
        assert!(
            !temporary_path(&path).exists(),
            "the staging file was left behind"
        );
        Ok(())
    }
}
