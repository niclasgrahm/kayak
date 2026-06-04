POST http://localhost:6767
Content-Type: application/json
{
    "input": { 
      "type": "nats", 
      "urls": "nats://localhost:4222",
      "subject": "test.subject"
    },
    "transforms": [
      { "type": "buffer", "size": 2},
      { "type": "reducer",  "function": "sum", "field": "value" }
    ],
    "output": { "type": "stdout" }
}

HTTP 201
[Captures]
dummy_streamer_id: jsonpath "$.id"

