//! Validating tokens somebody else minted.
//!
//! The declaration is [`kayak_core::server_config::JwtConfig`]; this is the
//! live half — the issuer's published keys, fetched and cached, and the check
//! a token goes through on its way to becoming an [`Identity`].
//!
//! # The keys are fetched, and when
//!
//! Twice, and only twice. Once at startup, through [`Validator::prime`], which
//! **fails the server** if the JWKS can't be fetched or holds nothing usable —
//! the same rule a `${NAME}` that isn't set follows, because a jwt server with
//! no keys is a server nobody can enter, and 02:00 in a crash loop beats 09:00
//! in a "why is every token invalid". And again whenever a token names a key
//! id the cache doesn't hold, which is how rotation is followed: the issuer
//! signs with the new key, the first such token misses, the set is re-fetched
//! and that same call retries. A refresh that fails is a warning and the
//! request a 401, never a crash — the cached keys keep working.
//!
//! Refreshes are rate-limited ([`MIN_REFRESH_INTERVAL`]) so a stream of junk
//! tokens with made-up key ids can't be used to hose the issuer — an
//! unauthenticated caller choosing when we make outbound requests is a lever
//! worth not handing out.
//!
//! # What a token must carry
//!
//! A `kid` header naming a published key, a signature that key verifies under
//! the algorithm *the key* declares (the token's own `alg` claim is never
//! trusted with the choice — that is the alg-confusion hole), the configured
//! `iss`, an unexpired `exp`, the configured `aud` when one is set, and a
//! string under the username claim. Everything else is somebody's business but
//! not ours; the role mapping reads one more claim and the rest are dropped.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use kayak_core::server_config::{JwtConfig, Role, RoleMapping};
use serde_json::Value;

use super::Identity;

/// The least time between two JWKS fetches, however many unknown key ids
/// arrive in between. Long enough that junk tokens can't turn kayak into a
/// load generator against the issuer, short enough that a real rotation is
/// picked up on the next request after it.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// How long a JWKS fetch may take before it is a failure. Applies to the
/// startup fetch and to refreshes alike.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// An identity plus when the token that proved it stops being true.
///
/// The expiry travels with the identity because the session minted from a
/// token must not outlive it — a token is the host application's "this person
/// is signed in with us", and that claim has an end written into it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenIdentity {
    pub identity: Identity,
    /// The token's `exp`, as a wall-clock time. `None` only for a token the
    /// issuer minted without one, which validation refuses by default.
    pub expires_at: Option<SystemTime>,
}

/// One cached signing key: the key itself and the algorithm *it* declares.
#[derive(Clone)]
struct CachedKey {
    key: DecodingKey,
    algorithm: Algorithm,
}

/// The live half of a `jwt` auth section.
pub struct Validator {
    config: JwtConfig,
    http: reqwest::Client,
    /// The published keys, by `kid`. Replaced wholesale on every fetch, so a
    /// key the issuer withdrew stops verifying at the next refresh.
    keys: Mutex<HashMap<String, CachedKey>>,
    /// When the set was last fetched, for the rate limit. `None` until
    /// [`Validator::prime`] runs.
    last_fetch: Mutex<Option<tokio::time::Instant>>,
    refresh_interval: Duration,
}

impl Validator {
    /// A validator with no keys yet — [`Validator::prime`] loads them.
    #[must_use]
    pub fn new(config: JwtConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::builder()
                .timeout(FETCH_TIMEOUT)
                .build()
                .unwrap_or_default(),
            keys: Mutex::new(HashMap::new()),
            last_fetch: Mutex::new(None),
            refresh_interval: MIN_REFRESH_INTERVAL,
        }
    }

    /// Shorten the refresh rate limit. For tests that exercise rotation;
    /// production has no reason to touch it.
    #[must_use]
    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    /// The startup fetch, and the fail-fast half of the contract: a jwt
    /// server that can't load any keys refuses to start.
    pub async fn prime(&self) -> anyhow::Result<()> {
        let set = self.fetch().await?;
        let installed = self.install_keys(&set);
        anyhow::ensure!(
            installed > 0,
            "the JWKS at {} holds no usable signing keys (every entry is missing a kid, \
             an algorithm, or is a key type this build can't verify with)",
            self.config.jwks_url
        );
        tracing::info!(
            "loaded {installed} signing key(s) from {} for issuer {}",
            self.config.jwks_url,
            self.config.issuer
        );
        self.mark_fetched();
        Ok(())
    }

    /// Put a key set in place without fetching anything.
    ///
    /// Public for tests and for anything that already holds the set; the
    /// server itself only ever loads keys through [`Validator::prime`] and the
    /// refresh. Returns how many keys were usable.
    pub fn install_keys(&self, set: &JwkSet) -> usize {
        let mut usable = HashMap::new();
        for jwk in &set.keys {
            match cache_entry(jwk) {
                Ok((kid, key)) => {
                    usable.insert(kid, key);
                }
                Err(reason) => {
                    tracing::warn!(
                        "skipping a key in the JWKS from {}: {reason}",
                        self.config.jwks_url
                    );
                }
            }
        }
        let count = usable.len();
        if let Ok(mut keys) = self.keys.lock() {
            *keys = usable;
        }
        count
    }

    /// Who this token says the caller is, if it checks out. `None` is the only
    /// failure shape on purpose: the middleware turns every flavour of bad
    /// token into the same 401, and the *why* goes to the debug log rather
    /// than to whoever is guessing.
    pub async fn identify(&self, token: &str) -> Option<TokenIdentity> {
        let header = decode_header(token)
            .map_err(|error| tracing::debug!("refused a token with an unreadable header: {error}"))
            .ok()?;
        let Some(kid) = header.kid else {
            tracing::debug!("refused a token with no kid header");
            return None;
        };
        let key = if let Some(key) = self.key(&kid) {
            key
        } else {
            // an unknown kid is what rotation looks like from here, so this
            // is the one place a refresh is worth an outbound request
            self.refresh().await;
            let key = self.key(&kid);
            if key.is_none() {
                tracing::debug!("refused a token naming unknown key id '{kid}'");
            }
            key?
        };
        let claims = self.check(token, &key)?;
        let username = match claims.get(&self.config.username_claim) {
            Some(Value::String(name)) if !name.trim().is_empty() => name.clone(),
            _ => {
                tracing::debug!(
                    "refused a valid token with no usable '{}' claim",
                    self.config.username_claim
                );
                return None;
            }
        };
        Some(TokenIdentity {
            identity: Identity {
                username,
                role: role_of(&claims, self.config.roles.as_ref()),
            },
            expires_at: expiry_of(&claims),
        })
    }

    /// The signature and the registered claims, against one cached key.
    fn check(&self, token: &str, key: &CachedKey) -> Option<serde_json::Map<String, Value>> {
        // the algorithm comes from the *key*, never from the token's own
        // header — trusting the token with that choice is the classic
        // confusion attack (an HS256 token "signed" with the public key)
        let mut validation = Validation::new(key.algorithm);
        validation.set_issuer(&[&self.config.issuer]);
        match &self.config.audience {
            Some(audience) => {
                validation.set_audience(&[audience]);
                // required, or a token that simply omits `aud` sails past the
                // check the config asked for
                validation.required_spec_claims.insert("aud".to_string());
            }
            None => validation.validate_aud = false,
        }
        decode::<serde_json::Map<String, Value>>(token, &key.key, &validation)
            .map_err(|error| tracing::debug!("refused a token: {error}"))
            .ok()
            .map(|data| data.claims)
    }

    fn key(&self, kid: &str) -> Option<CachedKey> {
        self.keys.lock().ok()?.get(kid).cloned()
    }

    /// Re-fetch the key set, unless one was fetched too recently. Failures are
    /// warned about and swallowed: the cached keys keep working, and the
    /// request that prompted this becomes an ordinary 401.
    async fn refresh(&self) {
        if !self.take_refresh_slot() {
            return;
        }
        match self.fetch().await {
            Ok(set) => {
                let installed = self.install_keys(&set);
                tracing::info!(
                    "refreshed the JWKS from {}: {installed} usable key(s)",
                    self.config.jwks_url
                );
            }
            Err(error) => {
                tracing::warn!("failed to refresh the JWKS (keeping the cached keys): {error:#}");
            }
        }
    }

    /// Whether a refresh may run now. Claims the slot *before* the fetch, so
    /// a burst of unknown-kid requests costs one outbound request rather than
    /// one each.
    fn take_refresh_slot(&self) -> bool {
        let Ok(mut last) = self.last_fetch.lock() else {
            return false;
        };
        if let Some(at) = *last
            && at.elapsed() < self.refresh_interval
        {
            return false;
        }
        *last = Some(tokio::time::Instant::now());
        true
    }

    fn mark_fetched(&self) {
        if let Ok(mut last) = self.last_fetch.lock() {
            *last = Some(tokio::time::Instant::now());
        }
    }

    async fn fetch(&self) -> anyhow::Result<JwkSet> {
        let url = &self.config.jwks_url;
        let response = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to fetch the JWKS from {url}"))?
            .error_for_status()
            .with_context(|| format!("the JWKS endpoint {url} refused the request"))?;
        response
            .json::<JwkSet>()
            .await
            .with_context(|| format!("the response from {url} is not a JWKS document"))
    }
}

/// One JWKS entry as a cache entry, or the reason it can't be one.
fn cache_entry(jwk: &Jwk) -> Result<(String, CachedKey), String> {
    let kid = jwk
        .common
        .key_id
        .clone()
        .ok_or_else(|| "it has no kid, so no token could name it".to_string())?;
    let algorithm = jwk
        .common
        .key_algorithm
        .ok_or_else(|| format!("key '{kid}' declares no algorithm"))?;
    let algorithm = Algorithm::try_from(algorithm)
        .map_err(|_| format!("key '{kid}' declares an algorithm tokens can't be signed with"))?;
    let key = DecodingKey::from_jwk(jwk)
        .map_err(|error| format!("key '{kid}' could not be parsed: {error}"))?;
    Ok((kid, CachedKey { key, algorithm }))
}

/// The claim → role rule, spelled out in
/// [`RoleMapping`](kayak_core::server_config::RoleMapping)'s docs: a string
/// claim matches by equality, an array claim if any element is listed, and
/// everything else — including no mapping at all — is a reader.
fn role_of(claims: &serde_json::Map<String, Value>, roles: Option<&RoleMapping>) -> Role {
    let Some(roles) = roles else {
        return Role::Read;
    };
    let is_admin = match claims.get(&roles.claim) {
        Some(Value::String(value)) => roles.admin.iter().any(|admin| admin == value),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .any(|value| roles.admin.iter().any(|admin| admin == value)),
        _ => false,
    };
    if is_admin { Role::Admin } else { Role::Read }
}

/// The `exp` claim as a wall-clock time.
fn expiry_of(claims: &serde_json::Map<String, Value>) -> Option<SystemTime> {
    let seconds = claims.get("exp")?.as_u64()?;
    Some(UNIX_EPOCH + Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    /// The fixture pair: a throwaway RSA key checked into the repo, and the
    /// JWKS document describing its public half under kid `test-key-1`.
    /// Generated once with openssl; nothing outside tests reads either.
    const PRIVATE_KEY_PEM: &str =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/jwt/test_jwt_key.pem"));
    pub(crate) const JWKS_JSON: &str =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/jwt/test_jwks.json"));
    const KID: &str = "test-key-1";
    const ISSUER: &str = "https://issuer.example";

    fn config() -> JwtConfig {
        let parsed = serde_json::from_value(serde_json::json!({
            "jwks_url": "http://127.0.0.1:9/jwks.json",
            "issuer": ISSUER,
            "username_claim": "cognito:username",
            "roles": {"claim": "cognito:groups", "admin": ["Admin"]},
        }));
        parsed.unwrap_or_else(|error| panic!("the test config parses: {error}"))
    }

    fn validator(config: JwtConfig) -> Validator {
        let validator = Validator::new(config);
        let set: JwkSet = serde_json::from_str(JWKS_JSON)
            .unwrap_or_else(|error| panic!("the fixture JWKS parses: {error}"));
        assert_eq!(validator.install_keys(&set), 1);
        validator
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default()
    }

    fn token_with(kid: Option<&str>, claims: &Value) -> String {
        let key = EncodingKey::from_rsa_pem(PRIVATE_KEY_PEM.as_bytes())
            .unwrap_or_else(|error| panic!("the fixture key parses: {error}"));
        let mut header = Header::new(Algorithm::RS256);
        header.kid = kid.map(str::to_string);
        encode(&header, claims, &key).unwrap_or_else(|error| panic!("signing works: {error}"))
    }

    fn good_claims() -> Value {
        serde_json::json!({
            "iss": ISSUER,
            "exp": now_secs() + 300,
            "cognito:username": "niclas",
            "cognito:groups": ["Admin", "Operators"],
        })
    }

    #[tokio::test]
    async fn a_valid_token_is_an_identity_with_its_expiry() {
        let validator = validator(config());
        let exp = now_secs() + 300;
        let token = token_with(Some(KID), &good_claims());
        let found = validator.identify(&token).await;
        let Some(found) = found else {
            panic!("a valid token should identify");
        };
        assert_eq!(found.identity.username, "niclas");
        assert_eq!(found.identity.role, Role::Admin);
        assert_eq!(found.expires_at, Some(UNIX_EPOCH + Duration::from_secs(exp)));
    }

    /// The mapping's two shapes: an array claim matches by membership, a
    /// string claim by equality, and a valid token matching neither reads.
    #[tokio::test]
    async fn the_role_mapping_reads_strings_and_arrays() {
        let validator = validator(config());
        let mut claims = good_claims();
        claims["cognito:groups"] = serde_json::json!("Admin");
        let token = validator.identify(&token_with(Some(KID), &claims)).await;
        assert_eq!(token.map(|t| t.identity.role), Some(Role::Admin));

        claims["cognito:groups"] = serde_json::json!(["Operators"]);
        let token = validator.identify(&token_with(Some(KID), &claims)).await;
        assert_eq!(token.map(|t| t.identity.role), Some(Role::Read));
    }

    /// No roles section means every valid token is a reader — the safe
    /// reading of a section someone didn't write.
    #[tokio::test]
    async fn without_a_mapping_every_token_reads() {
        let mut config = config();
        config.roles = None;
        let validator = validator(config);
        let token = validator.identify(&token_with(Some(KID), &good_claims())).await;
        assert_eq!(token.map(|t| t.identity.role), Some(Role::Read));
    }

    #[tokio::test]
    async fn an_expired_token_is_refused() {
        let validator = validator(config());
        let mut claims = good_claims();
        // past the default 60s leeway, so no clock generosity saves it
        claims["exp"] = serde_json::json!(now_secs() - 300);
        assert!(validator.identify(&token_with(Some(KID), &claims)).await.is_none());
    }

    #[tokio::test]
    async fn a_token_from_another_issuer_is_refused() {
        let validator = validator(config());
        let mut claims = good_claims();
        claims["iss"] = serde_json::json!("https://somewhere.else");
        assert!(validator.identify(&token_with(Some(KID), &claims)).await.is_none());
    }

    /// `aud` is checked when configured and only then — a Cognito access
    /// token carries none, and requiring one would refuse every one of them.
    #[tokio::test]
    async fn the_audience_is_checked_exactly_when_configured() {
        let mut with_audience = config();
        with_audience.audience = Some("client-id".to_string());
        let validator = validator(with_audience);

        let no_aud = token_with(Some(KID), &good_claims());
        assert!(validator.identify(&no_aud).await.is_none());

        let mut claims = good_claims();
        claims["aud"] = serde_json::json!("client-id");
        assert!(validator.identify(&token_with(Some(KID), &claims)).await.is_some());

        claims["aud"] = serde_json::json!("someone-else");
        assert!(validator.identify(&token_with(Some(KID), &claims)).await.is_none());
    }

    /// A token with no kid, or naming a key that isn't published, is refused —
    /// and the unknown kid tries a refresh that fails quietly offline, which
    /// is itself the property under test: no panic, no hang, a plain refusal.
    #[tokio::test]
    async fn unknown_and_missing_key_ids_are_refused() {
        let validator = validator(config());
        assert!(validator.identify(&token_with(None, &good_claims())).await.is_none());
        assert!(
            validator
                .identify(&token_with(Some("no-such-key"), &good_claims()))
                .await
                .is_none()
        );
    }

    /// The alg-confusion attack: a token whose header claims HS256, "signed"
    /// with a shared-secret MAC. The key cache pins RS256 for this kid, so
    /// the token's own claim about its algorithm is never consulted.
    #[tokio::test]
    async fn a_token_claiming_a_different_algorithm_is_refused() {
        let validator = validator(config());
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(KID.to_string());
        let secret = EncodingKey::from_secret(b"guessable");
        let Ok(forged) = encode(&header, &good_claims(), &secret) else {
            panic!("encoding the forgery works");
        };
        assert!(validator.identify(&forged).await.is_none());
    }

    /// A valid token that doesn't say who it is for is not an identity.
    #[tokio::test]
    async fn a_token_without_the_username_claim_is_refused() {
        let validator = validator(config());
        let mut claims = good_claims();
        let Some(map) = claims.as_object_mut() else {
            panic!("claims are an object");
        };
        map.remove("cognito:username");
        assert!(validator.identify(&token_with(Some(KID), &claims)).await.is_none());
    }
}
