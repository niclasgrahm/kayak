POST http://localhost:6767
Content-Type: application/json
{
    "inputs": [{ 
      "type": "nats", 
      "urls": "nats://localhost:4222",
      "subject": "test.subject"
    }],
    "transforms": [
      { "type": "buffer", "size": 2},
      { "type": "reducer",  "function": "sum", "field": "value" }
    ],
    "outputs": [{ "type": "stdout" }]
}

HTTP 201
[Captures]
dummy_pipeline_id: jsonpath "$.id"

