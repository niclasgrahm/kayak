# the connection this pipeline reads over. `local-nats` is already in
# config.connections.json, so this only matters against a bare server; a name
# that is already there comes back 409 and the run stops, which is why it is
# spelled out rather than assumed.
POST http://localhost:6767/api/connections
Content-Type: application/json
{
    "id": "baseline-nats",
    "type": "nats",
    "urls": "nats://localhost:4222"
}

HTTP 201


POST http://localhost:6767/api/pipelines
Content-Type: application/json
{
    "inputs": [{
      "type": "nats",
      "connection": "baseline-nats",
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

