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
//! The bind address has three possible sources and they are ordered
//! `--listen` > `LEPTOS_SITE_ADDR` > `Cargo.toml`. Only the first is this
//! module's business: the other two are already collapsed into one
//! [`SocketAddr`] by the time leptos hands over its options, so [`resolve`] is
//! the flag against that answer.
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

use std::net::SocketAddr;

use kayak_core::server_config::ServerConfig;

/// The address to bind: the flag if one was given, otherwise whatever the
/// leptos options already decided.
#[must_use]
pub fn resolve(flag: Option<SocketAddr>, configured: SocketAddr) -> SocketAddr {
    flag.unwrap_or(configured)
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
            auth: AuthConfig::Basic { users },
        }
    }

    /// The property the whole flag rests on: without it, nothing changes. This
    /// is what keeps `cargo leptos watch` and the container image — both of
    /// which speak through `LEPTOS_SITE_ADDR` — working.
    #[test]
    fn no_flag_keeps_the_configured_address() {
        assert_eq!(resolve(None, loopback()), loopback());
        assert_eq!(resolve(None, unspecified()), unspecified());
    }

    #[test]
    fn the_flag_wins_over_the_configured_address() {
        let wanted = v4([0, 0, 0, 0], 8080);
        assert_eq!(resolve(Some(wanted), loopback()), wanted);
    }

    /// One `SocketAddr` rather than a host beside a port, so the case
    /// `format!("{host}:{port}")` gets wrong is an ordinary one here.
    #[test]
    fn an_ipv6_address_is_expressible() {
        let any = v6(Ipv6Addr::UNSPECIFIED, 6767);
        assert_eq!(resolve(Some(any), loopback()), any);
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
