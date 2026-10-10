# posting into a pipeline

The `http` input gives a pipeline its own HTTP endpoint. Other systems post
messages to it.

```json
{ "id": "ingest", "inputs": [{ "type": "http" }], "transforms": [], "outputs": [{ "type": "stdout" }] }
```

```bash
curl -X POST localhost:6767/api/pipelines/ingest/messages \
     -H 'content-type: application/json' \
     -d '[{"sensor": "a", "value": 91}, {"sensor": "b", "value": 12}]'
# {"accepted":2}
```

kayak derives the path from the pipeline id: `POST /api/pipelines/{id}/messages`.
You do not configure it. The endpoint exists while the pipeline runs. When you
delete the pipeline, the same request removes the endpoint. A later post gets a
404.

## rules

- **An array is one batch.** Ten messages in one post go through the transforms
  in one pass. A single object is a batch of one message.
- **The server accepts before it processes.** It puts the batch in a queue for
  the run loop and returns 202. It does not wait for the outputs.
- **The queue has a limit.** `capacity` sets how many batches can wait. The
  default is 1024. When the queue is full, the server returns 503 at once. Retry
  the post later.
- **A pipeline has one endpoint.** A second `http` input on the same pipeline
  fails to build.

kayak does not apply a schema. Without an
[envelope](/pipelines/message-metadata), the transforms get the messages as the
sender wrote them.

## protecting the endpoint

Without `auth`, the endpoint accepts every post that reaches it. Add `auth` to
the input to require a credential:

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

A post without the correct token gets 401. Many webhook senders cannot set the
`Authorization` header. For them, use the `header` type and choose the header
name:

```json
{ "type": "http", "auth": { "type": "header", "name": "x-api-key", "value": "${INGEST_TOKEN}" } }
```

Rules for `auth`:

- **It is separate from the server sign-in.** The accounts in the settings file
  are for operators. This credential is for one sender and one pipeline. The
  ingest endpoint stays public in the API reference, with or without sign-in.
- **Each pipeline has its own token.** You can revoke one sender and keep the
  others.
- **Use TLS.** The token is a fixed string in every request. On plain HTTP,
  anyone on the network path can read it. Put a TLS proxy in front of kayak.
- **Keep the token in the secret store.** Write `${INGEST_TOKEN}` in the config
  file. An unknown reference stops the pipeline at build time. A token that
  resolves to an empty string also fails the build.
- **Do not use a header that an `envelope` copies into the messages.** kayak
  refuses that config at build time. Otherwise the credential goes into the
  messages and into the outputs.

kayak compares the token in constant time. It checks the token before it
queues the batch, so a sender without the token cannot fill the queue. An empty
post (`[]`) also needs the token.

The status codes show some information to a caller without a token. A
protected pipeline returns 401, an open one returns 202, and a missing one
returns 404. Thus a caller can find out which pipelines exist and which are
protected. kayak cannot prevent this, because each pipeline has its own
credential.

`POST /api/pipelines/{id}/messages` is in the generated API reference.
