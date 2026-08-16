//! The embedding handshake's pure half: finding `auth_token` in a query
//! string, and what the query looks like with it removed.
//!
//! The token arrives on the URL because an `<iframe src>` can set nothing
//! else — no header, no cookie. The app reads it once, exchanges it at
//! `POST /api/auth/token` for the session cookie, and immediately rewrites
//! the URL without it, so the token lives in the address bar for as short a
//! time as a page can manage.
//!
//! No percent-decoding happens here, deliberately: a JWT is base64url plus
//! dots, all of which `encodeURIComponent` leaves alone, so a decoded and an
//! undecoded read are the same string — and a decoder would be code that
//! never runs except when something upstream double-encoded, which is a bug
//! better surfaced as a failed exchange than papered over.

/// The name the token travels under — the same one Grafana's `url_login`
/// reads, so a host application embedding both passes both the same way.
pub const TOKEN_PARAM: &str = "auth_token";

/// The token out of a `location.search` string (with or without the leading
/// `?`). An empty value reads as absent: `?auth_token=` is nothing to
/// exchange.
#[must_use]
pub fn token_in_query(search: &str) -> Option<String> {
    search
        .trim_start_matches('?')
        .split('&')
        .find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == TOKEN_PARAM && !value.is_empty()).then(|| value.to_string())
        })
}

/// The same query with the token taken out — `?a=1&auth_token=x&b=2` becomes
/// `?a=1&b=2`, and a query that held only the token becomes nothing at all
/// rather than a dangling `?`.
#[must_use]
pub fn query_without_token(search: &str) -> String {
    let kept: Vec<&str> = search
        .trim_start_matches('?')
        .split('&')
        .filter(|pair| {
            !pair.is_empty()
                && pair
                    .split_once('=')
                    .is_none_or(|(key, _)| key != TOKEN_PARAM)
        })
        .collect();
    if kept.is_empty() {
        String::new()
    } else {
        format!("?{}", kept.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_token_is_found_among_other_parameters() {
        assert_eq!(
            token_in_query("?var-device=press_3&auth_token=abc.def.ghi&kiosk=1"),
            Some("abc.def.ghi".to_string())
        );
        assert_eq!(token_in_query("auth_token=abc"), Some("abc".to_string()));
    }

    #[test]
    fn no_token_and_an_empty_token_are_both_absent() {
        assert_eq!(token_in_query(""), None);
        assert_eq!(token_in_query("?a=1&b=2"), None);
        assert_eq!(token_in_query("?auth_token="), None);
        // a key that merely starts the same way is not the key
        assert_eq!(token_in_query("?auth_token_extra=x"), None);
    }

    /// The rewrite keeps everything else and never leaves a dangling `?` —
    /// what lands in the address bar is a URL someone could bookmark.
    #[test]
    fn stripping_the_token_keeps_the_rest_of_the_query() {
        assert_eq!(
            query_without_token("?var-device=press_3&auth_token=abc&kiosk=1"),
            "?var-device=press_3&kiosk=1"
        );
        assert_eq!(query_without_token("?auth_token=abc"), "");
        assert_eq!(query_without_token(""), "");
        assert_eq!(query_without_token("?a=1"), "?a=1");
    }
}
