# transforms

Transforms run in the sequence of the list. Each transform takes one batch and
returns zero or more batches. Thus `splitter` and `reducer` can change the
number of messages in a stream. All messages are untyped JSON. A transform
addresses a field by its [field path](/pipelines/message-metadata#field-paths),
so `_meta.subject` works in every place where `value` works.

`remember` and `recall` share a [state bucket](/pipelines/state). They are two
transforms because their position in the chain sets what they do. `remember`
passes its batch on unchanged. `recall` writes the remembered values onto the
messages that come after it.

Eight transforms keep state per `group_by` key in the state bucket of the
pipeline: `deadband`, `throttle`, `pivot`, `derive`, `rolling`, `smooth`,
`detect` and `resample`. They do not build in a pipeline without a `state`. See
[streaming statistics](/pipelines/streaming-statistics).

kayak refuses a contradictory transform when it builds the pipeline. Examples
are a reducer with no aggregations, an `as` that overwrites a group field, and
a `map` that writes a path through a scalar.

<!--@include: ./generated/components/transforms.md-->
