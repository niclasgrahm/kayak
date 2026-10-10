# the config file

You write the pipelines of a kayak server in one config file. Keep the file in
version control. The server reads the file when it starts.

## format

The config file is JSON or YAML. The extension of the file sets the format:

- `.yaml` and `.yml` are YAML.
- All other extensions are JSON.

If the content does not agree with the extension, the server does not start.
kayak does not try the other format.

The file has two spellings. Both are permanent.

**A bare array of pipelines.** Use it when the graph has no state buckets:

```yaml
- id: heartbeat
  inputs:
    - { type: dummy, duration: 1 }
  transforms: []
  outputs:
    - { type: stdout }
```

**A document with `state` and `pipelines`.** Use it to declare
[state buckets](/pipelines/state):

```yaml
state:
  machine_state:
    max_keys: 5000
pipelines:
  - id: machine_cycles
    state: { bucket: machine_state, key: machine_id }
    inputs: [...]
    transforms: [...]
    outputs: [...]
```

kayak builds the pipelines in the order of the file. A pipeline that has a
`pipeline` input must come after its upstream pipeline. If one pipeline does not
build, the server does not start. The error names the pipeline.

A pipeline without an `id` gets a random name. Give every pipeline an `id`, so
that other pipelines can refer to it.

## the files beside it

| file | flag | content |
| --- | --- | --- |
| `config.yaml` | `--config` | the pipelines and the state buckets |
| `config.connections.yaml` | `--connections` | the systems that the pipelines connect to |
| `secrets.json` | `--secrets` | the values of the `${NAME}` references |
| `config.layout.json` | none | the positions of the cards in the web UI |

### connections

A component names a [connection](/io/connections). The connections file declares
each connection one time. Without `--connections`, kayak derives the path from
the config file. The connections file uses the same name and the same format:

- `config.json` → `config.connections.json`
- `pipelines.yaml` → `pipelines.connections.yaml`

If the derived file does not exist, the server starts with no connections. If
you name a file with `--connections`, the file must exist. Use `--connections`
to share one connections file between many config files.

### secrets

Do not write a credential in the config file or in the connections file. Write a
reference, for example `${POSTGRES_PASSWORD}`. kayak reads the value from an
environment variable first. Then it reads the JSON file that `--secrets` names.
Do not commit the secrets file. See [secrets](/io/secrets).

### layout

The web UI keeps the positions of its cards in `config.layout.json`. This file
is always JSON. It has no effect on the pipelines.

## the workflow

1. Write `config.yaml` and `config.connections.yaml`.
2. Commit the two files.
3. Run the container image with the files mounted:

   ```bash
   docker run -p 6767:6767 -v "$PWD:/kayak" ghcr.io/niclasgrahm/kayak \
     --config /kayak/config.yaml --secrets /kayak/secrets.json
   ```

4. Change the file and review the diff.
5. Deploy again.

On Kubernetes, put the config and the connections in a ConfigMap. Put the
credentials in a Secret, and give them to the container as environment
variables. See [deployment](/operating/deployment#kubernetes).

## when the server reads and writes the file

The server reads the config file at startup. After that, it reads the file only
on a `revert`. It writes the file only on an explicit save. A change to the
running graph through the HTTP API does not change the file.

| action | direction |
| --- | --- |
| startup | file → server |
| `POST /api/config/revert` | file → server |
| `POST /api/config/save` | server → file |

### revert

`POST /api/config/revert` stops every pipeline and builds the graph again from
the file. Use it to load a file that you changed in an editor.

- kayak parses the file before it stops the pipelines. If the file has an error,
  the running graph does not change.
- kayak loads the connections file first, because the pipelines name the
  connections.
- kayak waits for the old pipelines to stop before it builds the new ones.
- The [state buckets](/pipelines/state#lifetime) keep their contents.

### save

`POST /api/config/save` writes the running graph to a file:

```bash
curl -s localhost:6767/api/config/save -H 'content-type: application/json' \
  -d '{"name": "config.yaml"}'
```

- `name` must be a bare file name. A name with a path separator, `..` or a root
  is refused with status 422.
- The file goes into the directory of the config file. Without `--config`, it
  goes into the working directory of the server.
- `format` is `"json"` or `"yaml"`. Without `format`, the extension of `name`
  sets the format.
- `overwrite: false` refuses to replace a file that exists, with status 409.
  The default is `true`.
- The same save writes the connections file and the layout file beside the
  config file.
- If the server started without `--config`, the saved file becomes its config
  file. A later `revert` reads that file.

kayak writes the file to a temporary file first and then renames it. Thus a
failure does not leave half a file.

`GET /api/settings` tells which file the server uses, where a save goes, and if
the running graph is different from the file.

## the output is deterministic

A save writes the same graph as the same bytes. Thus a diff shows only real
changes.

- Pipelines are in topological order. An upstream pipeline comes before the
  pipelines that read from it. Pipelines at the same level are in order of `id`.
- Connections are in order of name.
- A config with no state buckets is written as a bare array.
- A pipeline with a generated name is written with that name. Thus a downstream
  `upstream` field still resolves at the next start.
