//! Which address the server binds, and whether that address is one worth
//! warning about.
//!
//! Two decisions, both pure, both here rather than in `main.rs` so they can be
//! tested — `main.rs` is clap args, tracing setup and router wiring, and a
//! precedence rule that nothing can assert is exactly the kind of thing that
//! silently stops holding.
//!
//! # Precedence
//!
//! The bind address has four possible sources and they are ordered
//! `--listen` > `LEPTOS_SITE_ADDR` > `Cargo.toml` > [`DEFAULT_ADDR`]. The
//! middle two are already collapsed into one [`SocketAddr`] by the time leptos
//! hands over its options, so [`resolve`] is the flag and the fallback against
//! that answer.
//!
//! # Why there is a fallback at all
//!
//! `Cargo.toml`'s `site-addr` is read by **cargo-leptos**, not by this binary:
//! `cargo leptos watch` reads it and exports `LEPTOS_SITE_ADDR` before
//! starting the server. A binary that has been installed somewhere has neither
//! cargo-leptos nor a `Cargo.toml` beside it, so nothing sets that variable and
//! leptos falls back to its own `127.0.0.1:3000` — which is the one address
//! nothing in this repository names. `just dev`, the container image, the docs
//! and every example say 6767.
//!
//! So the fallback applies **only when nothing said anything**: an unset
//! `LEPTOS_SITE_ADDR` is the whole of the condition, which is why [`resolve`]
//! is told whether it was set rather than comparing the address against 3000.
//! Someone who genuinely wants 3000 sets the variable to it and is obeyed.
//!
//! **The flag is an `Option` and must stay one.** A clap `default_value_t`
//! would win over the environment on every run, which is not a preference —
//! it breaks `cargo leptos watch` (which sets `LEPTOS_SITE_ADDR` and expects
//! the server to appear there) and the container image (`Dockerfile` sets the
//! same var to `0.0.0.0:6767`, since the `Cargo.toml` default of loopback
//! reaches nothing from outside a network namespace). Absent means "whatever
//! was already decided", which is what leaves every existing invocation
//! unchanged.
//!
//! # One flag rather than a host and a port
//!
//! `--listen` takes a whole `SocketAddr` because that is what
//! `TcpListener::bind` takes. A `--host` beside a `--port` has to be
//! reassembled, and `format!("{host}:{port}")` produces `::1:6767` for IPv6 —
//! which does not parse. `--listen [::]:6767` needs no rule.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kayak_core::server_config::ServerConfig;

/// Where the server listens when nothing says otherwise.
///
/// The same address as `site-addr` in `[[workspace.metadata.leptos]]`, and
/// `the_default_is_the_address_cargo_toml_names` reads that file to make sure
/// it stays that way — two spellings of one number is exactly the pair that
/// drifts.
pub const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6767);

/// The address to bind: the flag if one was given, otherwise whatever the
/// leptos options decided — unless nothing decided anything, in which case
/// [`DEFAULT_ADDR`].
///
/// `site_addr_was_set` is whether `LEPTOS_SITE_ADDR` was present in the
/// environment. It is a parameter rather than a `std::env::var` call so this
/// stays pure and testable; `main.rs` is the one place that reads it.
#[must_use]
pub fn resolve(
    flag: Option<SocketAddr>,
    configured: SocketAddr,
    site_addr_was_set: bool,
) -> SocketAddr {
    match (flag, site_addr_was_set) {
        (Some(wanted), _) => wanted,
        (None, true) => configured,
        (None, false) => DEFAULT_ADDR,
    }
}

/// Whether this is an unauthenticated server reachable from off the machine.
///
/// Not a refusal, and deliberately not one: the open default is what makes a
/// first run and a local `just dev` work, and turning it into an error would
/// break every deployment that predates authentication. But an open *control
/// plane* — one where anyone who can reach the port can delete a pipeline or
/// rewrite the config — on an address other than loopback is worth one loud
/// line in the log.
///
/// Loopback is the whole test. `0.0.0.0` binds every interface the machine
/// happens to have, including ones nobody was thinking about — a VPN tunnel, a
/// bridge, a cloud instance's public address — so it is not treated as any
/// safer than a specific public address, and it is also the correct and
/// necessary choice inside a container. Which of those two it is, this cannot
/// know.
#[must_use]
pub fn is_open_to_the_network(config: &ServerConfig, addr: SocketAddr) -> bool {
    !config.requires_auth() && !addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};

    use kayak_core::config::Secret;
    use kayak_core::server_config::{AuthConfig, UserConfig};

    use super::*;

    /// The jwt scheme counts as authentication the way basic does: the
    /// warning is about an open control plane, and a server that wants a
    /// token is not open.
    #[test]
    fn a_jwt_server_is_not_warned_about() {
        let auth: AuthConfig = serde_json::from_value(serde_json::json!({
            "type": "jwt",
            "jwks_url": "https://issuer.example/jwks.json",
            "issuer": "https://issuer.example",
        }))
        .unwrap_or_else(|error| panic!("the jwt sample parses: {error}"));
        let config = ServerConfig {
            auth,
            ..ServerConfig::default()
        };
        assert!(!is_open_to_the_network(&config, v4([0, 0, 0, 0], 6767)));
    }

    /// Built rather than parsed: `.parse()` on a literal is a `Result` these
    /// lints will not let a test unwrap, and the constructors say the same
    /// thing without one.
    fn v4(octets: [u8; 4], port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::from(octets), port))
    }

    fn v6(ip: Ipv6Addr, port: u16) -> SocketAddr {
        SocketAddr::from((ip, port))
    }

    fn loopback() -> SocketAddr {
        v4([127, 0, 0, 1], 6767)
    }

    fn unspecified() -> SocketAddr {
        v4([0, 0, 0, 0], 6767)
    }

    fn authenticated() -> ServerConfig {
        let mut users = BTreeMap::new();
        users.insert(
            "sam".to_string(),
            UserConfig {
                password: Secret::from("hunter2"),
                role: kayak_core::server_config::Role::Admin,
            },
        );
        ServerConfig {
            history: kayak_core::server_config::HistoryConfig::default(),
            auth: AuthConfig::Basic { users },
        }
    }

    /// The property the whole flag rests on: without it, nothing changes. This
    /// is what keeps `cargo leptos watch` and the container image — both of
    /// which speak through `LEPTOS_SITE_ADDR` — working.
    #[test]
    fn no_flag_keeps_the_configured_address() {
        assert_eq!(resolve(None, loopback(), true), loopback());
        assert_eq!(resolve(None, unspecified(), true), unspecified());
    }

    #[test]
    fn the_flag_wins_over_the_configured_address() {
        let wanted = v4([0, 0, 0, 0], 8080);
        assert_eq!(resolve(Some(wanted), loopback(), true), wanted);
    }

    /// An installed binary: no cargo-leptos, no `Cargo.toml`, so leptos hands
    /// over its own `127.0.0.1:3000` and nothing in this repository names that
    /// address. The fallback is what makes `kayak` on its own agree with
    /// `just dev`, the image and the docs.
    #[test]
    fn nothing_set_at_all_lands_on_the_projects_own_address() {
        let leptos_default = v4([127, 0, 0, 1], 3000);
        assert_eq!(resolve(None, leptos_default, false), DEFAULT_ADDR);
        assert_eq!(DEFAULT_ADDR, loopback());
    }

    /// The fallback is about *silence*, not about the number 3000: someone who
    /// asks for it gets it, which is what keeps this a default rather than a
    /// refusal to bind where you said.
    #[test]
    fn an_address_that_was_asked_for_is_obeyed_even_when_it_is_the_leptos_one() {
        let three_thousand = v4([127, 0, 0, 1], 3000);
        assert_eq!(resolve(None, three_thousand, true), three_thousand);
        assert_eq!(
            resolve(Some(three_thousand), loopback(), false),
            three_thousand
        );
    }

    /// The flag still wins when nothing else was set — otherwise installing the
    /// binary would take `--listen` away from it.
    #[test]
    fn the_flag_wins_over_the_fallback() {
        let wanted = v4([0, 0, 0, 0], 8080);
        assert_eq!(resolve(Some(wanted), v4([127, 0, 0, 1], 3000), false), wanted);
    }

    /// Two spellings of one number, so this reads the other one. `site-addr`
    /// under `[[workspace.metadata.leptos]]` is what `cargo leptos watch` and
    /// the docs use; [`DEFAULT_ADDR`] is what an installed binary uses, and a
    /// change to either that left the other behind would be invisible until
    /// someone wondered why two ways of starting the same server disagreed.
    #[test]
    fn the_default_is_the_address_cargo_toml_names() {
        let Ok(manifest) = std::fs::read_to_string("Cargo.toml") else {
            panic!("the workspace manifest is not where the test is run from");
        };
        let quoted = manifest
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with("site-addr"))
            .and_then(|line| line.split('"').nth(1));
        let Some(quoted) = quoted else {
            panic!("no quoted site-addr under [[workspace.metadata.leptos]]");
        };
        assert_eq!(
            quoted,
            DEFAULT_ADDR.to_string(),
            "Cargo.toml's site-addr and listen::DEFAULT_ADDR have drifted apart"
        );
    }

    /// One `SocketAddr` rather than a host beside a port, so the case
    /// `format!("{host}:{port}")` gets wrong is an ordinary one here.
    #[test]
    fn an_ipv6_address_is_expressible() {
        let any = v6(Ipv6Addr::UNSPECIFIED, 6767);
        assert_eq!(resolve(Some(any), loopback(), true), any);
        assert!(!any.ip().is_loopback());
        assert!(v6(Ipv6Addr::LOCALHOST, 6767).ip().is_loopback());
    }

    #[test]
    fn an_unauthenticated_server_on_loopback_is_not_warned_about() {
        let open = ServerConfig::default();
        assert!(!is_open_to_the_network(&open, loopback()));
        assert!(!is_open_to_the_network(
            &open,
            v6(Ipv6Addr::LOCALHOST, 6767)
        ));
    }

    #[test]
    fn an_unauthenticated_server_off_loopback_is_warned_about() {
        let open = ServerConfig::default();
        assert!(is_open_to_the_network(&open, unspecified()));
        assert!(is_open_to_the_network(&open, v4([192, 168, 1, 10], 6767)));
        assert!(is_open_to_the_network(
            &open,
            v6(Ipv6Addr::UNSPECIFIED, 6767)
        ));
    }

    /// Authentication is the thing the warning is about the absence of, so it
    /// silences it on any address.
    #[test]
    fn an_authenticated_server_is_never_warned_about() {
        let closed = authenticated();
        assert!(!is_open_to_the_network(&closed, unspecified()));
        assert!(!is_open_to_the_network(&closed, loopback()));
    }
}
