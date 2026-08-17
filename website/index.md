---
layout: home
hero:
  name: kayak
  text: stream processing you can watch running
  tagline: >-
    describe pipelines as input → transforms → output, and kayak runs them on a
    live canvas — a card per pipeline, edges for the data, a log and a
    throughput chart on each one.
  actions:
    - theme: brand
      text: getting started
      link: /getting-started
    - theme: alt
      text: component reference
      link: /reference/inputs
    - theme: alt
      text: github
      link: https://github.com/niclasgrahm/kayak
features:
  - title: configured, not coded
    details: >-
      nats, kafka, mqtt, redis, opc ua and http in; postgres, clickhouse, s3, files and
      webhooks out; filter, reduce, map, split and remember in between. plain
      JSON the whole way through — no schema to define up front.
  - title: a graph, not a list of jobs
    details: >-
      a pipeline can feed another pipeline, so what you configure is a DAG. the
      canvas is a real view onto the running server: watch it, edit it, or drive
      the same JSON API by hand.
  - title: the reference cannot rot
    details: >-
      every table under reference/ is reflected out of the config types kayak
      actually deserializes, and the HTTP tables come from the table the router
      is built from. no component is documented by hand.
---
