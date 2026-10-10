# authentication

Authentication is off by default. A server without `--server-config` asks
nobody for credentials. kayak logs a warning at startup when the server has no
authentication and listens on an address that is not loopback.

To turn authentication on, write a server config file in JSON or YAML:

```yaml
# kayak.server.yaml
auth:
  type: basic
  users:
    niclas:
      password: ${KAYAK_NICLAS_PASSWORD}
      role: admin
    grafana:
      password: ${KAYAK_DASHBOARD_PASSWORD}
      role: read
```

Give its path to the server:

```bash
kayak --config config.yaml --server-config kayak.server.yaml
```

The server config describes the process, not the graph. Thus kayak does not
derive its path from the config. Two configs on one server share it. kayak
reads the file and never writes it. No HTTP request can change who has access
to the server.

`auth` has three types: `none`, `basic` and `jwt`. A `basic` server with no
users does not start, because nobody can log in.

Passwords are secrets. Write them as `${NAME}` references, so that you can
commit the file. kayak resolves them against the environment first and then
against the `--secrets` file. A literal password also works. Do not commit a
literal password. See [secrets](/io/secrets).

## jwt: tokens from an identity provider

Use `jwt` when kayak is embedded in a host application, for example as an
iframe. The users of the host sign in with an identity provider that publishes
a JWKS: Cognito, Keycloak or a similar service. kayak accepts the tokens of
that provider and keeps no accounts of its own.

```yaml
# kayak.server.yaml
auth:
  type: jwt
  jwks_url: https://cognito-idp.eu-central-1.amazonaws.com/POOL/.well-known/jwks.json
  issuer: https://cognito-idp.eu-central-1.amazonaws.com/POOL
  username_claim: cognito:username     # default: sub
  roles:
    claim: cognito:groups
    admin: [Admin]                     # all other valid tokens: read
  service_accounts:                    # optional; checked as HTTP Basic
    provisioner:
      password: ${KAYAK_PROVISIONER_PASSWORD}
      role: admin
```

`jwks_url`, `issuer` and `audience` are plain strings. They are addresses, not
credentials. Only the passwords of the service accounts are secrets.

**The keys load at startup.** kayak gets the signing keys from `jwks_url` when
it starts. If it cannot get them, or the set has no usable key, the server does
not start. After startup, kayak follows a key rotation. A token with an unknown
key id causes one new fetch of the key set, and the request then tries again.
These fetches have a rate limit, so invalid tokens cannot overload the issuer.

**A valid token must have these properties:**

- a `kid` that names a published key;
- a signature that this key verifies, with the algorithm of the key (kayak
  ignores the algorithm that the token names);
- the configured `iss`;
- an `exp` in the future;
- the configured `aud`, if you set `audience`;
- a non-empty string in the `username_claim`.

Omit `audience` for Cognito access tokens. These tokens carry `client_id`
instead of `aud`.

The role comes from `roles`. A string claim matches when it is equal to a value
in `admin`. An array claim, such as `cognito:groups`, matches when one element
is in `admin`. All other tokens get `read`. Without `roles`, every token gets
`read`.

**A client can send a token in two ways:**

```bash
# an API client sends it on every request
curl -H "Authorization: Bearer $TOKEN" localhost:6767/api/pipelines
```

```html
<!-- the host page puts it on the iframe URL, one time -->
<iframe src="https://kayak.example/?auth_token=JWT"></iframe>
```

For the iframe, the UI reads `auth_token` from the URL when it loads. It sends
the token to `POST /api/auth/token` and gets an `HttpOnly` session cookie. Then
it removes the token from the address bar. Thus the token is in one request
only, and not in bookmarks, shared links or access logs. The session expires at
the `exp` of the token or before. Grafana's `url_login` uses the same parameter
name.

Use `service_accounts` for scripts and CI. A machine cannot do a login with an
identity provider. A service account is a Basic credential, with the same shape
as a user of the `basic` type. A token, a cookie and a Basic credential all give
the same identity and the same two roles.

## two roles

- **`admin`** can do all operations. It can create and delete pipelines and
  connections, save and revert the config file, and change the layout.
- **`read`** can see everything and change nothing. An account without a
  `role` gets `read`.

The endpoint table in `kayak-core/src/api_docs.rs` declares the role that each
endpoint needs. The router, the `/docs` page and the `security` of the OpenAPI
document all use that table. Thus the three cannot disagree. See the
[http api reference](/reference/api) for the access of each endpoint.

Two endpoints do not follow the rule "GET is read, all other methods are
admin":

- `POST /api/pipelines/{id}/messages` is public. See the limits below.
- `PUT /api/layout` is admin, because it writes a file that you commit.

In the web UI, a `read` user sees no edit button. The server refuses the
requests in all cases.

## two ways in

All ways in give the same identity, so a role has the same meaning for each.

```bash
curl -u niclas localhost:6767/api/pipelines   # any client that is not a browser; curl asks for the password

# the browser: a login that sets a session cookie
curl -c jar -X POST localhost:6767/api/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"niclas","password":"hunter2"}'
```

The browser needs the cookie. The UI reads `GET /events` with `EventSource`,
and `EventSource` cannot set request headers. A token in the query string goes
into access logs, so kayak uses a cookie.

Sessions are in memory. `POST /api/auth/logout` deletes the session, so the
cookie stops working on every copy of it. A restart deletes all sessions, so a
deploy logs out all users. kayak has no signing key to store or rotate.

A `401` has no `WWW-Authenticate` header. With that header, the browser shows
its own login dialog over the UI. `curl -u` sends its credentials without the
header.

## what this does not do

- **kayak does not terminate TLS.** Put a TLS proxy in front of the server.
  Basic credentials over plain HTTP are visible on the network. kayak marks the
  session cookie `Secure` only when the proxy sends `X-Forwarded-Proto: https`.
- **The ingest endpoint is not covered.** `POST /api/pipelines/{id}/messages`
  has no account check, also on a server with accounts. A device that posts
  readings is not an operator. To protect the endpoint, set `auth` on the
  `http` input. See
  [protecting the endpoint](/io/posting-into-a-pipeline#protecting-the-endpoint).
  An `http` input without `auth` accepts posts from any client.
- **kayak has no rate limit.** A wrong password costs an attacker one round
  trip. Use long passwords.
- **kayak does not hash passwords.** It compares each password with the value
  from the secret store, in constant time.
