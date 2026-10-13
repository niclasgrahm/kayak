//! `kayak check`: the server's own build, with nothing started, every problem
//! listed, and the consequences of one problem not listed as more of them.

use std::path::{Path, PathBuf};

use kayak::check::{Options, Report, check};
use serde_json::{Value, json};

fn idle(id: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    })
}

fn downstream_of(id: &str, upstream: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "pipeline", "upstream": upstream }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    })
}

/// Refused at build time, not at parse time: a reducer with no aggregations.
fn unbuildable(id: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [{ "type": "reducer", "aggregations": [] }],
        "outputs": [{ "type": "stdout" }]
    })
}

fn write(dir: &Path, name: &str, contents: &Value) -> anyhow::Result<PathBuf> {
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string_pretty(contents)?)?;
    Ok(path)
}

fn check_pipelines(pipelines: &[Value]) -> anyhow::Result<Report> {
    let dir = tempfile::tempdir()?;
    let config = write(dir.path(), "config.json", &json!(pipelines))?;
    Ok(check(&Options {
        config: Some(config),
        ..Options::default()
    }))
}

fn subjects(report: &Report) -> Vec<&str> {
    report.problems.iter().map(|p| p.subject.as_str()).collect()
}

/// The sample is what every component is tried against, and it must check
/// clean with the flags `just dev` passes — or the check disagrees with a
/// server that starts.
#[test]
fn the_sample_config_checks_clean() {
    let report = check(&Options {
        config: Some("example_config/config.json".into()),
        secrets: Some("example_config/secrets.example.json".into()),
        data_dir: Some("dev_data".into()),
        server_config: Some("example_config/server.yaml".into()),
        ..Options::default()
    });
    assert!(report.is_ok(), "{}", report.render());
    assert!(report.pipelines > 20, "{}", report.render());
    assert!(report.connections > 0);
}

#[test]
fn a_clean_config_is_ok_and_says_what_it_built() -> anyhow::Result<()> {
    let report = check_pipelines(&[idle("a"), downstream_of("b", "a")])?;
    assert!(report.is_ok(), "{}", report.render());
    assert_eq!(report.pipelines, 2);
    let rendered = report.render();
    assert!(rendered.contains("2 pipelines built"), "{rendered}");
    assert!(rendered.ends_with("ok: no problems found\n"), "{rendered}");
    Ok(())
}

/// Startup stops at the first; a checker that did would be fixed one problem
/// per run.
#[test]
fn every_broken_pipeline_is_reported_and_the_rest_are_built() -> anyhow::Result<()> {
    let report = check_pipelines(&[unbuildable("x"), idle("a"), unbuildable("y")])?;
    assert_eq!(subjects(&report), vec!["pipeline 'x'", "pipeline 'y'"]);
    assert_eq!(report.pipelines, 1);
    assert!(
        report.problems[0].detail.contains("aggregation"),
        "{}",
        report.problems[0].detail
    );
    assert!(!report.is_ok());
    assert!(
        report
            .render()
            .contains("2 problems: the server would not start")
    );
    Ok(())
}

#[test]
fn a_pipeline_under_a_broken_one_is_skipped_rather_than_reported() -> anyhow::Result<()> {
    let report = check_pipelines(&[
        unbuildable("x"),
        downstream_of("b", "x"),
        downstream_of("c", "b"),
    ])?;
    assert_eq!(subjects(&report), vec!["pipeline 'x'"]);
    let skipped: Vec<(&str, &str)> = report
        .skipped
        .iter()
        .map(|s| (s.subject.as_str(), s.upstream.as_str()))
        .collect();
    assert_eq!(
        skipped,
        vec![("pipeline 'b'", "x"), ("pipeline 'c'", "b")]
    );
    Ok(())
}

/// The server says "not found", which is true and points the wrong way.
#[test]
fn an_upstream_declared_further_down_is_named_as_the_problem() -> anyhow::Result<()> {
    let report = check_pipelines(&[downstream_of("b", "a"), idle("a")])?;
    assert_eq!(subjects(&report), vec!["pipeline 'b'"]);
    assert!(
        report.problems[0].detail.contains("declared after it"),
        "{}",
        report.problems[0].detail
    );
    Ok(())
}

#[test]
fn an_upstream_that_does_not_exist_is_reported_by_the_build() -> anyhow::Result<()> {
    let report = check_pipelines(&[downstream_of("b", "nowhere")])?;
    assert_eq!(subjects(&report), vec!["pipeline 'b'"]);
    assert!(report.problems[0].detail.contains("'nowhere' not found"));
    Ok(())
}

#[test]
fn a_pipeline_that_feeds_itself_is_reported() -> anyhow::Result<()> {
    let report = check_pipelines(&[downstream_of("a", "a")])?;
    assert!(report.problems[0].detail.contains("names itself"));
    Ok(())
}

#[test]
fn a_repeated_id_is_reported() -> anyhow::Result<()> {
    let report = check_pipelines(&[idle("a"), idle("a")])?;
    assert_eq!(subjects(&report), vec!["pipeline 'a'"]);
    assert!(report.problems[0].detail.contains("already used"));
    assert_eq!(report.pipelines, 1);
    Ok(())
}

#[test]
fn a_config_that_does_not_parse_is_one_problem() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(&config, r#"[{"inputs": [{"type": "no_such_kind"}]}]"#)?;
    let report = check(&Options {
        config: Some(config),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["config file"]);
    assert_eq!(report.pipelines, 0);
    Ok(())
}

#[test]
fn a_secret_that_does_not_resolve_is_reported_against_its_pipeline() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    write(
        dir.path(),
        "config.connections.json",
        &json!({ "bus": { "type": "nats", "urls": "${KAYAK_CHECK_TEST_UNSET_SECRET}" } }),
    )?;
    let config = write(
        dir.path(),
        "config.json",
        &json!([{
            "id": "reads",
            "inputs": [{ "type": "nats", "connection": "bus", "subject": "s" }],
            "outputs": [{ "type": "stdout" }]
        }]),
    )?;
    let report = check(&Options {
        config: Some(config),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["pipeline 'reads'"]);
    assert!(
        report.problems[0]
            .detail
            .contains("KAYAK_CHECK_TEST_UNSET_SECRET"),
        "{}",
        report.problems[0].detail
    );
    assert_eq!(report.connections, 1);
    Ok(())
}

/// One missing file, one problem: building every pipeline without it would
/// report each of its consequences as well.
#[test]
fn a_missing_secrets_file_stops_the_builds_and_says_so() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = write(dir.path(), "config.json", &json!([idle("a")]))?;
    let report = check(&Options {
        config: Some(config),
        secrets: Some(dir.path().join("missing.json")),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["secrets file"]);
    assert_eq!(report.pipelines, 0);
    assert!(report.notes.iter().any(|n| n.contains("were not built")));
    Ok(())
}

#[test]
fn a_named_connections_file_has_to_exist() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = write(dir.path(), "config.json", &json!([idle("a")]))?;
    let report = check(&Options {
        config: Some(config),
        connections: Some(dir.path().join("missing.connections.json")),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["connections file"]);
    Ok(())
}

#[test]
fn a_broken_layout_file_is_reported_because_startup_refuses_it() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = write(dir.path(), "config.json", &json!([idle("a")]))?;
    std::fs::write(dir.path().join("config.layout.json"), "{ not json")?;
    let report = check(&Options {
        config: Some(config),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["layout file"]);
    assert_eq!(report.pipelines, 1);
    Ok(())
}

#[test]
fn the_server_config_and_its_accounts_are_checked() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let unparseable = dir.path().join("server.yaml");
    std::fs::write(&unparseable, "auth: [")?;
    let report = check(&Options {
        server_config: Some(unparseable),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["server config file"]);

    let unresolved = dir.path().join("accounts.yaml");
    std::fs::write(
        &unresolved,
        "auth:\n  type: basic\n  users:\n    someone:\n      password: ${KAYAK_CHECK_TEST_UNSET_PASSWORD}\n      role: admin\n",
    )?;
    let report = check(&Options {
        server_config: Some(unresolved),
        ..Options::default()
    });
    assert_eq!(subjects(&report), vec!["server config file"]);
    assert!(
        report.problems[0]
            .detail
            .contains("KAYAK_CHECK_TEST_UNSET_PASSWORD"),
        "{}",
        report.problems[0].detail
    );
    Ok(())
}

#[test]
fn the_state_dir_is_not_opened() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let state_dir = dir.path().join("state");
    let report = check(&Options {
        state_dir: Some(state_dir.clone()),
        ..Options::default()
    });
    assert!(report.is_ok(), "{}", report.render());
    assert!(!state_dir.exists());
    assert!(report.render().contains("note: --state-dir"));
    Ok(())
}

/// The property the whole command rests on: a check builds what a start
/// builds and runs none of it. An `http_poll` input reads at once when it
/// runs, and the input `buffer` around it spawns its pump at build time — so
/// if anything a build spawned were ever polled, this listener would hear it.
#[test]
fn nothing_a_check_builds_is_run() -> anyhow::Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let url = format!("http://{}/items", listener.local_addr()?);

    let report = check_pipelines(&[json!({
        "id": "polls",
        "inputs": [{
            "type": "http_poll",
            "url": url,
            "interval_secs": 1,
            "buffer": { "type": "static", "size": 10 }
        }],
        "outputs": [{ "type": "stdout" }]
    })])?;
    assert!(report.is_ok(), "{}", report.render());
    assert_eq!(report.pipelines, 1);

    std::thread::sleep(std::time::Duration::from_millis(200));
    match listener.accept() {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
        Ok((_, peer)) => anyhow::bail!("the check connected to the endpoint from {peer}"),
        Err(e) => Err(e.into()),
    }
}
