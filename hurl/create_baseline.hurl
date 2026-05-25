POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "dummy", "duration": 1, "buffer": 10 },
    "transforms": [],
    "output": { "type": "stdout" }
}

HTTP 201
[Captures]
dummy_streamer_id: jsonpath "$.id"

POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "streamer", "upstream": "{{ dummy_streamer_id }}" },
    "transforms": [],
    "output": { 
      "type": "nats",
      "urls": "localhost:4222",
      "subject": "test.output_11"

    }
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
    "output": { "type": "stdout" }
}
HTTP 201
[Captures]
nats_streamer_id : jsonpath "$.id"

POST http://localhost:6767
Content-Type: application/json
{
    "input": {
        "type": "nats",
        "urls": "localhost:4222",
        "subject": "test.subject"
        
    },
    "transforms": [],
    "output": { "type": "file", "file_type": "csv" }
}
HTTP 201

POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "streamer", "upstream": "{{ nats_streamer_id }}" },
    "transforms": [],
    "output": { "type": "stdout" }
}
HTTP 201

GET http://localhost:6767
