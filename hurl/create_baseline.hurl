POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "dummy", "duration": 1, "buffer": 10 },
    "transforms": [
    { "type": "splitter", "out_size": 2}
    ],
    "output": { "type": "stdout" }
}

HTTP 201
[Captures]
dummy_streamer_id: jsonpath "$.id"

