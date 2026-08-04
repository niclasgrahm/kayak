//! Secret resolution: the `${NAME}` syntax, the stores behind it, and the
//! guarantee that a resolved value never travels back out of the server.
//!
//! The last of those is the point of the whole design, so it's tested from two
//! directions: `Resolved` refuses to print its value, and a `Config` that
//! referenced a secret still serialises to the reference after the pipeline has
//! been built from it.

use std::sync::Arc;

use streamer::secrets::{ChainStore, EnvStore, FileStore, SecretStore, resolve};
use streamer::state::{AppState, StreamerError};
use streamer::testing::MapSecretStore;
use streamer_core::config::{Config, Secret};

fn store(values: &[(&str, &str)]) -> MapSecretStore {
    MapSecretStore::new("the test store", values)
}

/// A config whose nats input url references `${NATS_PASSWORD}`.
fn config_referencing_a_secret(id: &str) -> anyhow::Result<Config> {
    Ok(serde_json::from_value(serde_json::json!({
        "id": id,
        "input": {
            "type": "nats",
            "urls": "nats://app:${NATS_PASSWORD}@broker:4222",
            "subject": "test.subject"
        },
        "transforms": [],
        "output": {"type": "stdout"}
    }))?)
}

/// The common case: most fields hold nothing sensitive and must be untouched.
#[test]
fn a_value_with_no_references_is_passed_through_unchanged() -> anyhow::Result<()> {
    let resolved = resolve(&Secret::new("nats://localhost:4222"), &store(&[]))?;
    assert_eq!(resolved.expose(), "nats://localhost:4222");
    Ok(())
}

/// Credentials live *inside* connection strings, which is why references
/// interpolate rather than replace the whole value.
#[test]
fn references_are_replaced_where_they_appear() -> anyhow::Result<()> {
    let resolved = resolve(
        &Secret::new("nats://${NATS_USER}:${NATS_PASSWORD}@broker:4222"),
        &store(&[("NATS_USER", "app"), ("NATS_PASSWORD", "hunter2")]),
    )?;
    assert_eq!(resolved.expose(), "nats://app:hunter2@broker:4222");
    Ok(())
}

/// Substituting an empty string would let a pipeline connect without the
/// credentials it was configured with — worse than not starting at all.
#[test]
fn a_missing_secret_is_an_error_that_names_it() {
    let err = match resolve(&Secret::new("nats://${NATS_PASSWORD}@b"), &store(&[])) {
        Err(e) => format!("{e:#}"),
        Ok(r) => panic!("a missing secret resolved to '{}'", r.expose()),
    };
    assert!(
        err.contains("NATS_PASSWORD") && err.contains("the test store"),
        "the error should name the secret and where it was looked for, got: {err}"
    );
}

#[test]
fn an_unterminated_reference_is_rejected() {
    assert!(
        resolve(&Secret::new("nats://${NATS_PASSWORD@b"), &store(&[])).is_err(),
        "'${{' with no closing brace should not be treated as literal text"
    );
}

#[test]
fn a_reference_with_an_unusable_name_is_rejected() {
    for template in ["${}", "${a b}", "${pass$word}"] {
        assert!(
            resolve(&Secret::new(template), &store(&[("", "x"), ("a b", "x")])).is_err(),
            "'{template}' should be rejected as a secret reference"
        );
    }
}

/// Chaining is what lets one secret be overridden for a single run without
/// editing the file the rest of them live in.
#[test]
fn the_first_store_in_the_chain_wins() {
    let chain = ChainStore::new(vec![
        Box::new(MapSecretStore::new("the override", &[("PW", "from-env")])),
        Box::new(MapSecretStore::new(
            "the file",
            &[("PW", "from-file"), ("OTHER", "file-only")],
        )),
    ]);

    assert_eq!(chain.get("PW"), Some("from-env".to_string()));
    // a name only the later store has still resolves
    assert_eq!(chain.get("OTHER"), Some("file-only".to_string()));
    assert_eq!(chain.get("MISSING"), None);
}

/// Reads the environment rather than a copy of it taken at startup — checked
/// without mutating the environment, which isn't safe to do from a test thread.
#[test]
fn the_env_store_reads_the_process_environment() -> anyhow::Result<()> {
    let expected = std::env::var("PATH")?;
    assert_eq!(EnvStore.get("PATH"), Some(expected.clone()));
    assert_eq!(EnvStore.get("KAYAK_SECRET_THAT_IS_NEVER_SET"), None);

    // and end to end, through the reference syntax
    let resolved = resolve(&Secret::new("path=${PATH}"), &EnvStore)?;
    assert_eq!(resolved.expose(), format!("path={expected}"));
    Ok(())
}

#[test]
fn the_file_store_loads_name_value_pairs() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("kayak-secrets-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("ok.json");
    std::fs::write(&path, br#"{"NATS_PASSWORD": "hunter2"}"#)?;

    let loaded = FileStore::from_path(&path);
    std::fs::remove_file(&path)?;

    assert_eq!(loaded?.get("NATS_PASSWORD"), Some("hunter2".to_string()));
    Ok(())
}

/// A secrets file is small and hand-written; a wrong shape should be loud at
/// startup rather than a confusing connection failure later.
#[test]
fn the_file_store_rejects_a_value_that_is_not_a_string() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("kayak-secrets-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("nested.json");
    std::fs::write(&path, br#"{"NATS": {"PASSWORD": "hunter2"}}"#)?;

    let loaded = FileStore::from_path(&path);
    std::fs::remove_file(&path)?;

    assert!(loaded.is_err(), "a nested object should be rejected");
    Ok(())
}

/// `Resolved` is passed to `format!` in connection errors, so its `Display` and
/// `Debug` are the last line of defence against a password in the logs.
#[test]
fn a_resolved_secret_prints_its_reference_and_not_its_value() -> anyhow::Result<()> {
    let resolved = resolve(
        &Secret::new("nats://app:${NATS_PASSWORD}@broker:4222"),
        &store(&[("NATS_PASSWORD", "hunter2")]),
    )?;

    for rendered in [format!("{resolved}"), format!("{resolved:?}")] {
        assert!(
            !rendered.contains("hunter2"),
            "a resolved secret leaked its value: {rendered}"
        );
        assert_eq!(rendered, "nats://app:${NATS_PASSWORD}@broker:4222");
    }
    Ok(())
}

/// The config is the version-controlled artefact, so the reference — not the
/// value — is what has to survive a parse/serialise round trip.
#[test]
fn a_config_that_references_a_secret_round_trips_unchanged() -> anyhow::Result<()> {
    let config = config_referencing_a_secret("x")?;
    let json = serde_json::to_value(&config)?;
    assert_eq!(
        json["input"]["urls"],
        serde_json::json!("nats://app:${NATS_PASSWORD}@broker:4222")
    );
    // and a Secret is a plain JSON string on the wire, not a wrapper object
    assert!(json["input"]["urls"].is_string());
    Ok(())
}

/// Building the pipeline is where resolution happens. The nats input connects
/// lazily, so this exercises the build without needing a broker.
#[tokio::test]
async fn a_streamer_builds_from_a_config_that_references_a_secret() -> anyhow::Result<()> {
    let state = AppState::with_secrets(Arc::new(store(&[("NATS_PASSWORD", "hunter2")])));
    state.create_streamer(config_referencing_a_secret("with-secret")?)?;
    assert_eq!(state.get_streamer_ids(), vec!["with-secret".to_string()]);
    Ok(())
}

/// The whole point: a pipeline that has been built with a real secret still
/// hands the *reference* back to `GET /api/streams` and the UI.
#[tokio::test]
async fn the_api_view_of_a_streamer_never_shows_a_resolved_secret() -> anyhow::Result<()> {
    let state = AppState::with_secrets(Arc::new(store(&[("NATS_PASSWORD", "hunter2")])));
    state.create_streamer(config_referencing_a_secret("with-secret")?)?;

    let view = serde_json::to_string(&state.get_streamers()?)?;
    assert!(
        !view.contains("hunter2"),
        "the resolved secret leaked into the API view: {view}"
    );
    assert!(
        view.contains("${NATS_PASSWORD}"),
        "the API view should still show the reference: {view}"
    );
    Ok(())
}

/// A config naming a secret the server doesn't have is the caller's mistake, so
/// it must fail the build rather than start a pipeline that can't authenticate.
#[tokio::test]
async fn a_streamer_whose_secret_is_missing_fails_to_start() -> anyhow::Result<()> {
    let state = AppState::with_secrets(Arc::new(MapSecretStore::empty()));
    // InvalidConfig is what the HTTP layer turns into a 4xx — a config naming an
    // unknown secret is the caller's mistake, not a server fault
    let err = match state.create_streamer(config_referencing_a_secret("no-secret")?) {
        Err(StreamerError::InvalidConfig(e)) => format!("{e:#}"),
        Err(e) => panic!("expected InvalidConfig, got: {e}"),
        Ok(_) => panic!("a streamer built despite its secret being missing"),
    };

    assert!(
        err.contains("NATS_PASSWORD"),
        "the failure should name the missing secret, got: {err}"
    );
    assert!(
        state.get_streamer_ids().is_empty(),
        "a streamer that failed to build should not be registered"
    );
    Ok(())
}
