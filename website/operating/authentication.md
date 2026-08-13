# authentication

**Off by default, and that is deliberate.** A server started without
`--server-config` asks nobody for anything and behaves exactly as kayak did
before this existed — which is what makes `just dev` on a laptop a single
command, and what keeps an upgrade from locking an existing deployment out of
its own server. Turning a security control off by default is a real cost; the
thing that takes the edge off it is that the server logs a warning at startup
when it is unauthenticated *and* bound to something other than loopback.

Turning it on is one file, named with `--server-config` and written as JSON or
YAML:

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

```bash
kayak --config config.json --secrets secrets.json --server-config kayak.server.yaml
```

The file describes **how the server is run**, as against what the graph is —
which is why, unlike the connections file, its path is not derived from the
config's. It belongs to the process, so two configs served by one server share
it. It is read and never written: nothing reachable over HTTP may change who is
allowed to reach the server.

`auth` is a tagged enum rather than a boolean beside a map, and that is the
point of the shape. `auth: false` sitting above a populated `users:` — an
operator believing they are protected and not being — is not expressible here,
because there is nowhere to write it. The two states are the two variants,
`none` and `basic`, and `basic` with no users refuses to start rather than
serving a server nobody can log into.

Passwords are `Secret`s, so they hold `${NAME}` references resolved against the
same store everything else uses (the environment, then `--secrets`). The
settings file stays committable. A literal password works and is what a
throwaway deployment will write, but it puts a real credential in a file that
gets committed, which is the habit the reference syntax exists to replace — see
[secrets](/io/secrets).

## two roles

- **`admin`** — everything: create and delete pipelines and connections, save
  and revert the config file, rearrange the canvas.
- **`read`** — see everything, change nothing. The default for an account whose
  `role` is left out, because the field someone forgets should be the harmless
  one.

Two rather than more because the split that matters first is "can change what
the server is running" against "can watch it". Anything finer — per pipeline,
per connection — needs a model of *which* resources, which is a much larger
feature than a third role.

Which role an endpoint needs is declared in `kayak-core/src/api_docs.rs`
alongside everything else about it, and `api_router` is folded over that same
table — so the access shown on `/docs`, the `security` in the OpenAPI spec and
the check the middleware makes are one fact rather than three that agree today.
Note the two entries that a "GET is read, everything else is admin" rule would
get wrong: `POST /api/pipelines/{id}/messages` is public (see below), and
`PUT /api/layout` is admin, because it writes a file that gets committed. A
reader can look at the canvas; they just can't rearrange it.

In the UI a reader gets no edit toggle at all — hidden rather than disabled,
which is the rule the rest of the canvas follows. The server refuses the calls
regardless; the UI only avoids offering what it would refuse.

## two ways in

Both resolve to the same identity, so a role means the same thing however you
got in.

```bash
curl -u niclas:hunter2 localhost:6767/api/pipelines            # anything that is not a browser
curl -c jar -X POST localhost:6767/api/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"niclas","password":"hunter2"}'              # the browser's path
```

The session cookie is not a convenience. `EventSource` — which the canvas
consumes `GET /events` with — cannot set request headers at all, so a browser
has no way to present Basic credentials on the one endpoint it needs most. The
alternatives were a token in the query string, which ends up in every access log
the request passes through, or a cookie.

Sessions live in memory and are dropped on `POST /api/auth/logout`, which means
signing out genuinely signs you out everywhere that cookie was copied to — the
thing a signed, stateless cookie could not do. The cost is that they do not
survive a restart, so a deploy logs everyone out. That trade is the right way
round for a dev tool, and it means there is no signing key to invent, store or
rotate.

A 401 deliberately carries **no `WWW-Authenticate` header**. Sending one makes
the browser throw its own credential dialog over the app, which is exactly what
the login page exists to replace, and there is no way to send it to `curl` and
not to a browser. Nothing is lost: `curl -u` sends its credentials preemptively.

## what this does not do

Three limits, stated rather than implied:

- **Terminate TLS in front of it.** Basic credentials over plain HTTP are
  credentials on the wire, and a session cookie is only marked `Secure` when the
  request arrived over TLS (from the proxy's `X-Forwarded-Proto`). Nothing here
  refuses to run without it, but nothing here makes it safe either.
- **`POST /api/pipelines/{id}/messages` is not covered, and never will be.** The
  ingest endpoint is a data plane, not a control plane: a device posting
  readings is not an operator, and sharing the operators' credentials with every
  publisher is wrong the moment there is more than one. It stays reachable
  without an account even on a server with accounts, and has its own mechanism —
  the per-pipeline `auth` on the `http` input, under "protecting the endpoint"
  above. That one is opt-in, so an input with no `auth` is still open to anyone
  who can reach it.
- **Nothing is rate limited.** A wrong password costs an attacker nothing but
  the round trip, so an account is only as good as its password. Password
  *hashing* is likewise not here yet — passwords are compared against the value
  from the secret store, in constant time, and hashes need a
  `kayak hash-password` helper to be usable at all.
