# Smoke test against a *running* server (`just test-http`).
#
# The behaviour here is already covered in-process by tests/api.rs; what this
# adds is proof that the real binary, with the real router and the Leptos routes
# merged in, still serves the API. Keep it small.

# a pipeline that stays idle: the dummy input only ticks once an hour
POST http://localhost:6767/api/streams
Content-Type: application/json
{
    "id": "hurl-smoke-test",
    "input": { "type": "dummy", "duration": 3600 },
    "transforms": [{ "type": "buffer", "size": 10 }],
    "output": { "type": "stdout" }
}

HTTP 201
[Captures]
streamer_id: jsonpath "$.id"
[Asserts]
jsonpath "$.id" == "hurl-smoke-test"
jsonpath "$.config.input.type" == "dummy"


# it shows up in the listing
GET http://localhost:6767/api/streams
HTTP 200
[Asserts]
jsonpath "$[?(@.id=='{{streamer_id}}')]" count == 1


# ids are unique
POST http://localhost:6767/api/streams
Content-Type: application/json
{
    "id": "hurl-smoke-test",
    "input": { "type": "dummy", "duration": 3600 },
    "transforms": [],
    "output": { "type": "stdout" }
}
HTTP 409


# an unknown upstream is the caller's mistake, not a server error
POST http://localhost:6767/api/streams
Content-Type: application/json
{
    "id": "hurl-orphan",
    "input": { "type": "streamer", "upstream": "does-not-exist" },
    "transforms": [],
    "output": { "type": "stdout" }
}
HTTP 422


# cleanup
DELETE http://localhost:6767/api/streams/{{streamer_id}}
HTTP 204


DELETE http://localhost:6767/api/streams/{{streamer_id}}
HTTP 404
