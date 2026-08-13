# transforms

Transforms run in the order they are listed, each taking a batch and returning
zero or more batches — which is how `splitter` and `reduce` change a stream's
cardinality. Everything is untyped JSON, and a transform addresses fields by
[field path](/pipelines/message-metadata#field-paths), so `_meta.subject` is
reachable wherever `value` is.

Two of these are a pair rather than two components: `remember` and `recall`
share a [state bucket](/pipelines/state), and they are separate because **chain
order is the semantics** — `remember` is a tap that passes its batch on
unchanged, and `recall` writes what was remembered onto the messages that come
after.

Anything contradictory is refused when the pipeline is built rather than
producing a strange message once per batch forever: a reducer with no
aggregations, an `as` that would overwrite a group field, a `map` writing a
path that runs through a scalar.

<!--@include: ./generated/components/transforms.md-->
