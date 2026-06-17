# kayak - graph-based stream processing

## currently working on

- [ ] add filter transform
- [ ] add some kind of component plugin registry which can be used to generate docs

## todo

- [ ] add time based buffer for the transform buffer
- [ ] make outputs optional (for example, when a parent node is only used to push data to children)
- [ ] think about necessary metadata to add to each message
- [ ] deal with all unwraps -- this will bite us in the ass soon otherwise
- [ ] show config in the "cards" in the web ui
- [ ] give streamer ability to have multiple inputs
- [ ] new transform (i guess?): wait_for_condition (should it be called buffer_until_condition? or perhaps both are needed?)
      for example, we need to wait for x: a and z: b. for this, we also need the multiple input thing
