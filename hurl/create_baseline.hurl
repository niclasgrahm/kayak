POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "dummy", "duration": 1 },
    "transforms": [],
    "output": "Stdout"
}

HTTP 201
[Captures]
dummy_streamer_id: jsonpath "$.id"

POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "streamer", "upstream": "{{ dummy_streamer_id }}" },
    "transforms": [],
    "output": "Stdout"
}


POST http://localhost:6767
Content-Type: application/json
{
    "input": {
        "type": "nats",
        "urls": "localhost:4222",
        "subject": "test.subject"
        
    },
    "transforms": [],
    "output": "Stdout"
}
HTTP 201
[Captures]
nats_streamer_id : jsonpath "$.id"

POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "streamer", "upstream": "{{ nats_streamer_id }}" },
    "transforms": [],
    "output": "Stdout"
}
