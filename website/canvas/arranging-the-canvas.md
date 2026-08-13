# arranging the canvas

Where the cards sit is **not configuration**, and it is deliberately kept out of
the config file. A config file with pixel coordinates in it stops being
reviewable, and nothing about a position changes what the server runs. So it
lives in its own file beside the config: `config.json` is arranged by
`config.layout.json`, `pipelines.yaml` by `pipelines.layout.json`. Derived from
the config path rather than configured, so the pair travels together.

It is generated and maintained by the program, and meant to be committed —
which is why it is written deterministically (ids in one order, no `null`s for
things nobody set) and atomically, the same as the config file. It holds only
the cards someone has actually moved:

```json
{
  "version": 1,
  "pipelines": {
    "everything": { "x": 760, "y": 1180, "width": 360, "height": 320 }
  },
  "edges": [
    {
      "from": "sensors",
      "to": "hot_readings",
      "offset": -60,
      "from_port": { "side": "bottom", "along": 260 }
    }
  ]
}
```

An absent id is the normal case, not a gap: that card is laid out
automatically. `height` is absent unless the card was resized, because the two
are different things — normally a card is as tall as its content, and only an
explicit resize pins it. `edges` is absent entirely unless a line
has been adjusted, each of the three adjustments is absent unless it was made,
and an entry disappears once *all* of them are back to automatic — an undone
adjustment shouldn't leave a no-op behind in a committed file. An entry naming a
pipeline that no longer exists is kept rather than pruned; it costs nothing and
it is still there if the pipeline comes back.

Here the write-through rule is the *opposite* of the config file's, and for the
same reason: moving a card changes nothing the server runs, so `PUT
/api/layout` writes immediately (on release, not per frame) and arranging the
canvas never counts as an unsaved change. There is no save step because there is
nothing worth reviewing before it lands. It is a full replacement rather than a
patch, which is what makes "put everything back to automatic" an ordinary send
of a smaller map. Without a config file there is nowhere to put it, so the
arrangement lives in memory — until a save creates one, which writes it out on
the spot rather than losing the tidying that came before it.

**The grid is the unit.** `GRID` in `frontend/src/graph.rs` is 20px, the card
width is 18 cells, positions and sizes snap to it, ports sit on its lines, and
edge channels run along them — the background grid you can see is the same one
things land on. A route only reads as "along the grid" if the things it connects
are on the grid too, which is also why measured card heights are rounded *up* to
the next line (up, so content still fits; and idempotent, so the measure → lay
out → render loop doesn't oscillate between two heights).

Pinning a card does *not* take it out of the automatic flow: the row it came
from keeps its slot, so dragging one card doesn't rearrange every other card on
the canvas. Two cards can then be dragged on top of each other, which is the
user's business in the same way it is in any other editor with a canvas.
