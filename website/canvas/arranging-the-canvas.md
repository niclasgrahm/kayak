# arranging the canvas

In edit mode, you can move cards, resize them and adjust the lines between
them. The positions do not change what the server runs.

## gestures (edit mode)

| gesture | what it does |
| --- | --- |
| drag the title bar of a card | move the card and all other selected cards |
| drag the bottom-right corner of a card | resize the card |
| double-click the title bar of a card | give the card back to the automatic layout |
| shift-click a card or a sidebar row | add the pipeline to the selection, or remove it |
| `⋯` on a sidebar row, then `select children` | add the pipeline and all pipelines downstream of it to the selection |
| click the empty canvas | clear the selection |
| drag the middle of a line | move the middle segment nearer to one card or the other |
| drag an end of a line | move the point where the line connects, along the face of the card |
| double-click the middle or an end of a line | give that part back to the automatic routing |

Cards snap to the 20 px grid. A moved card keeps its slot in the automatic
layout, so the other cards do not move. Cards can overlap.

A plain click on a card that is not selected selects only that card. A press on
a selected card keeps the selection, so you can drag a group by any card in it.
`select children` adds to the selection. A resize applies to one card only.

The face that a line uses is always automatic. kayak ignores a stored end
position when the line moves to a different face. A straight line or an L-shaped
line has no middle handle.

## the layout file

kayak writes the positions to a layout file beside the config. The name comes
from the config: `config.json` has `config.layout.json`, and `pipelines.yaml`
has `pipelines.layout.json`. The layout file is always JSON.

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

- `pipelines` contains only the cards that you moved. The other cards use the
  automatic layout.
- `height` is present only when you resized the card.
- `edges` contains only the lines that you adjusted. An entry goes away when
  all its adjustments are back to automatic.
- An entry for a deleted pipeline stays in the file.

The UI sends `PUT /api/layout` when you release the mouse button. The server
writes the file immediately. A layout change is never an unsaved change. The
request replaces the complete layout.

Without a config file, the layout stays in memory. When you save a new config
file, kayak also writes the layout file.

kayak writes the file in a fixed sequence and replaces it atomically. Thus you
can commit it beside the config.
