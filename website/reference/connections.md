# connections

A connection is a system that you declare one time, under a name, in a file
beside the config. Components refer to it by that name. The connection holds
what the system is: brokers, urls, credentials. The component holds what the
pipeline wants from the system: a topic, a group, a subject, a table. There is
no inline form. A component names a connection, or it does not build.

One kind serves both directions. For example, a `kafka` connection serves a
kafka input and a kafka output. kayak checks the kind as well as the name.
kayak refuses to delete a connection that a pipeline uses.

Credentials are secrets. A connection holds the `${NAME}` template, never the
value. See [secrets](/io/secrets). [Connections](/io/connections) tells how
kayak finds the file, what an edit does to a pipeline that runs, and why `file`
is a connection.

<!--@include: ./generated/components/connections.md-->
