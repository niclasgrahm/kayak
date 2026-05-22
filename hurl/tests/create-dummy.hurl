POST http://localhost:6767
Content-Type: application/json
{
    "input": { "type": "dummy", "duration": 1 },
    "transforms": [],
    "output": {"type": "file", "file_type": "csv"}
}

HTTP 201
[Captures]
streamer_id: jsonpath "$.id"

[Asserts]
jsonpath "$.id" exists


# list should now contain it
GET http://localhost:6767
HTTP 200
[Asserts]
jsonpath "$['{{streamer_id}}']" exists

# cleanup
DELETE http://localhost:6767/{{streamer_id}}
HTTP 200
