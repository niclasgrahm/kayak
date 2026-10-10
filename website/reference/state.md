# state buckets

You declare buckets at the top of the config file. The pipelines that use a
bucket refer to it by name, as with connections. Buckets are global. Thus one
pipeline can remember the current recipe per machine, and six other pipelines
can add it to their output.

::: warning the rule that kayak does not enforce
Do not share a bucket between pipelines for data where order matters. Two
pipelines are two run loops with no order between them. Put correlation that
depends on order in one pipeline. Share a bucket only for state that changes
slowly compared with the message rate.
:::

Every bucket has a limit, and there is no bucket without a limit. kayak applies
expiry when a pipeline touches a bucket. There is no background task. The
contents stay after a config reload, unless the declaration of that bucket
changed. [State](/pipelines/state) tells what `remember` and `recall` do with
buckets.

<!--@include: ./generated/state.md-->
