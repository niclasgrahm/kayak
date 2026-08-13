# state buckets

Buckets are declared at the top of the config file and referred to by the
pipelines that use them — deliberately the shape connections already have, and
global for the same reason: one pipeline remembers the current recipe per
machine, and six unrelated ones stamp it onto their output.

::: warning the rule that isn't enforced
Two pipelines sharing a bucket are two run loops with no ordering between them.
Ordering-sensitive correlation has to live in *one* pipeline; sharing is for
state that doesn't change on the timescale of a message. Nothing prevents this
— it is a property of what you are computing, not of the config.
:::

Every bucket is bounded and there is no unbounded spelling, expiry is applied
when a bucket is touched rather than by a sweeper, and contents survive a config
reload unless that bucket's own declaration changed. The narrative, and what
`remember` / `recall` do with all this, is in [state](/pipelines/state).

<!--@include: ./generated/state.md-->
