# connections

A connection is a system, declared once under a name in a file beside the config
and referred to by the components that use it. The split is **what the system is
(brokers, urls, credentials) against what this pipeline wants from it (topic,
group, subject, table)** — there is no inline form: a component names a
connection or it does not build.

One kind serves both directions, so a `kafka` connection feeds a kafka input and
a kafka output. The kind is checked as well as the name, and deleting one a
running pipeline names is refused rather than breaking it later.

Credentials are typed as secrets and hold the *unresolved* `${NAME}` template,
never the value — see [secrets](/io/secrets). How the file is found, what
happens when you edit one under a running pipeline, and why `file` is a
connection at all are covered in [connections](/io/connections).

<!--@include: ./generated/components/connections.md-->
