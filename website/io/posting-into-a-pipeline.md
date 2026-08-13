# posting into a pipeline

Every other input reaches out to something — a broker, a timer, another
pipeline. The `http` input is the one that gets reached: give a pipeline one,
and it serves its own endpoint.

```json
{ "id": "ingest", "inputs": [{ "type": "http" }], "transforms": [], "outputs": [{ "type": "stdout" }] }
```

```bash
curl -X POST localhost:6767/api/pipelines/ingest/messages \
     -H 'content-type: application/json' \
     -d '[{"sensor": "a", "value": 91}, {"sensor": "b", "value": 12}]'
# {"accepted":2}
```

The path is **derived from the pipeline's id** rather than configured, so there
is nothing to keep in step and no second name for the same pipeline. It exists
for as long as the pipeline runs: deleting the pipeline takes the endpoint down
with it, in the same request, and a post that arrives afterwards is a 404.

Three rules worth knowing:

- **An array is one batch.** Posting ten messages is one pass through the
  transforms, not ten — which is what makes a reducer or a buffer downstream
  mean anything. A bare object is a batch of one.
- **Accepted is not processed.** The batch is queued for the run loop and the
  202 is sent without waiting for the outputs. `capacity` (default 1024) is how
  many batches may queue; past that the post is refused with a 503 rather than
  held open, because a request blocked on a pipeline catching up is a request
  that eventually times out somewhere less visible.
- **One pipeline is one endpoint.** Two `http` inputs on one pipeline would
  share a path with no way to say which a request meant, so the second one fails
  to build.

There is no envelope and no schema — whatever is posted is what the transforms
see, same as every other input.

## protecting the endpoint

By default the endpoint takes anything that reaches it, which is what every
pipeline with an `http` input has always done. `auth` on the input changes that:

```json
{
  "id": "ingest",
  "inputs": [
    { "type": "http", "auth": { "type": "bearer", "token": "${INGEST_TOKEN}" } }
  ],
  "transforms": [],
  "outputs": [{ "type": "stdout" }]
}
```

```bash
curl -X POST localhost:6767/api/pipelines/ingest/messages \
     -H "authorization: Bearer $INGEST_TOKEN" \
     -H 'content-type: application/json' \
     -d '{"sensor": "a", "value": 91}'
```

A post without the token is a 401. There is a second spelling for senders that
can't set `Authorization` — which is most webhook sources — where the header is
yours to choose:

```json
{ "type": "http", "auth": { "type": "header", "name": "x-api-key", "value": "${INGEST_TOKEN}" } }
```

Five things about it are deliberate:

- **It is the data plane's own credential, and has nothing to do with the
  accounts in the settings file.** A machine posting readings should not need an
  account that can rewrite the graph, and an operator with such an account
  should not thereby be able to post readings. The two never meet: this endpoint
  stays `Public` in the API table whether or not the server has sign-in.
- **The token is per pipeline**, so revoking one publisher doesn't touch the
  others. That is the whole argument against a single server-wide ingest key.
- **It is only as private as the transport.** The token is a fixed string
  repeated on every request, so on plain HTTP it is readable by anything on the
  path. Terminate TLS in front of kayak. This is the same trade every log-ingest
  API makes; it is worth making on purpose.
- **The token lives in the secret store**, like every other credential — the
  config file holds `${INGEST_TOKEN}` and can be committed. A reference nobody
  set stops the pipeline at build time rather than turning into a token nobody
  can guess, and a token that resolves to *empty* is refused too, since an empty
  header would satisfy it.
- **`auth` may not use a header an `envelope` copies.** Refused at build time,
  not filtered afterwards: filtering is the step that gets forgotten, and a
  credential written into an object store outlives the request by years. The
  comparison is constant-time, and a refused post never reaches the queue — so
  someone without the token can't fill it and turn the holder's 202 into a 503.

What the status codes give away, since it is worth knowing rather than
discovering: a guarded pipeline 401s, an unguarded one 202s and a missing one
404s, so a caller with no token can learn which pipelines exist and which are
protected. Unavoidable while the credential is per-pipeline — the pipeline has
to be found before its requirement can be read — and a fair trade here, where
the ids are on a canvas anyway.

`POST /api/pipelines/{id}/messages` is in the generated reference like the rest
of the API. The registry the handler finds the input through is
`src/inputs/http.rs`: the input claims its pipeline's id when it is built and
gives it up when it is dropped, which is why the endpoint's lifetime is exactly
the run loop's.
