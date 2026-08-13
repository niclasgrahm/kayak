# outputs

Every output on a pipeline gets every batch. An output is initialised when the
pipeline is built — which is what makes a wrong password fail at startup rather
than on the first message — and finished once when the run loop ends, however
it ended, so the ones that hold a *part* (`file` has a JSON array to close, `s3`
has an object that has not been uploaded at all) close it.

Where the destination is a system rather than a place, the settings that
describe the *system* live on a [connection](/io/connections) and only what this
pipeline wants from it — a topic, a table, a path — is on the component. The
two exceptions are deliberate: `stdout` has nothing to connect to, and the
[`http` output](/io/sending-over-http) takes a `url`, because for a webhook the
url is the whole of what a connection would have held.

Two of them map messages onto real columns rather than writing JSON whole;
`postgres` and `clickhouse` share that mapping and differ only in DDL and wire
format. See [database outputs](/io/database-outputs).

<!--@include: ./generated/components/outputs.md-->
