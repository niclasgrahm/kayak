# sending over http

The `http` output sends the results of a pipeline to a webhook or to an ingest
API.

```json
{ "type": "http", "url": "https://example.com/hooks/readings" }
```

`url` is the only required field. With the defaults, the output sends a `POST`
with the batch as one JSON array and no credential.

## fields

- **`verb`**: `POST` is the default. `PUT` and `PATCH` are also accepted.
  `GET` and `DELETE` fail at build time, because a request without a body
  cannot carry the messages.
- **`body`**: `batch` is the default. It sends the whole batch as one JSON
  array in one request. `message` sends one request for each message. Choose
  the value that the receiving API expects.
- **`auth`**: the same block as on the [`http` input](/io/posting-into-a-pipeline#protecting-the-endpoint).
  Use a `bearer` token or a header that you name. Write the value as a
  `${NAME}` reference.
- **`timeout_seconds`**: the longest time for one request. The default is 30.
  A request that times out fails the batch. Thus this value is also the longest
  time that a slow endpoint can stop the pipeline.

With `body: message`, the output sends the requests in order. The first failure
fails the batch, and the output does not send the messages after it.

For performance, use `body: batch` when the receiver accepts an array. The
output then makes one request for each batch, however many messages the batch
holds.

## errors and retries

- **A status that is not 2xx fails the batch.** The error quotes the first
  300 bytes of the response body.
- **The output ignores the response body of a 2xx reply.** To use the reply in
  the pipeline, use the [`http` transform](/pipelines/model-round-trip).
- **After a failure, the output waits before the next request.** During the
  backoff, the next batches fail at once, and the output sends nothing. A
  webhook that is down gets one attempt every few seconds.
- **The output does not connect at startup.** kayak checks the url and its
  scheme at build time. An endpoint that is not reachable fails the first batch.

There is no connection for this output. The `url` and the `auth` are on the
component.

## the sample

`heartbeat_to_webhook` in `example_config/` sends to the ingest endpoint of the
same server, on `127.0.0.1:6767`. It works under `just dev` without other
services. If you change the port of the server, change this url too.
