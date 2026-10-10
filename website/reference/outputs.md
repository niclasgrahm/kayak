# outputs

Every output of a pipeline gets every batch. The run loop initializes each
output before the first batch. If an output cannot initialize, for example
because the database is not available, the run loop tries again with a backoff.
The pipeline reads no input until all outputs are ready.

When the run loop ends, it calls `finish` on each output one time. Two outputs
need this step. A `file` output with `json_array` closes the array. An `s3`
output uploads the part that it holds in memory.

When the destination is a system, the settings of the system are on a
[connection](/io/connections). The component has only what this pipeline wants
from the system: a topic, a table, a path. Two outputs have no connection.
`stdout` has nothing to connect to. The [`http` output](/io/sending-over-http)
takes a `url`, because for a webhook the url is all that a connection holds.

`postgres` and `clickhouse` map messages onto real columns. They share the
same column mapping and differ only in DDL and wire format. See
[database outputs](/io/database-outputs).

<!--@include: ./generated/components/outputs.md-->
