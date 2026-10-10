# editing the graph

The canvas starts in read-only mode. In read-only mode, the edit controls are
not shown. Click `edit` in the navbar to show them. A `read` user does not see
the `edit` button. See [authentication](/operating/authentication).

The mode is a property of the browser tab. The server does not know about it.
The server checks the role of the user on each request.

## add and delete a pipeline

The `+` in the sidebar header opens the "add pipeline" form. Give an id, then
add inputs, transforms and outputs. Select each component from a list and fill
in its fields. The form sends `POST /api/pipelines`.

The `×` on a sidebar row deletes that pipeline. The first click arms the
button, and the second click sends `DELETE /api/pipelines/{id}`.

The `+` at the bottom edge of a card opens the same form. Its first input is a
`pipeline` input that reads from that card.

**Changes apply immediately.** A new pipeline starts at once. A delete stops
the pipeline at once. There is no draft. The undo is `revert`, which loads the
config file again.

The connections tab has the same `+` and `×` for connections. kayak refuses to
delete a connection that a pipeline uses.

## the form

kayak generates the form from the same schema as the
[reference](/reference/). Thus a new component has a form without a change to
the frontend. The doc comment of each field is the tooltip of its label.

- A field with a fixed set of values is a dropdown. It starts empty.
- A field with fields of its own, such as `rotate`, shows those fields below it.
- A field that is a choice of shapes, such as `buffer`, shows the choice first.
  Then it shows the fields of the selected shape. kayak sends only the fields
  of the selected shape.
- A list, such as the `conditions` of a `filter`, has rows that you add and
  remove.

No field needs raw JSON. A test fails if a new field needs it.

The form checks every field before it sends the request. It shows all problems
at once, at the field. The rules are the same as the rules of the server. The
server also checks the request. The form shows the error text of the server at
the bottom.

## see the data while you build {#seeing-the-data-while-you-build}

The form can read sample messages from an input. Use them to find the field
names before you configure the transforms and the outputs.

**`fetch messages`** on an input builds that input and reads some messages from
it. kayak does not create a pipeline and does not acknowledge the messages. The
form shows the messages in a panel beside it.

The sample stops at 5 messages or after 5 s, at the first limit. An input that
sends one message per second thus gives 4 messages. A quiet input gives an
empty sample. No input can replay messages from before the sample started.

Some inputs change their behavior for a sample. The panel shows a note for each
change:

- `kafka` reads with a temporary consumer group. It does not affect the group
  of the pipeline and starts where `start_at` says.
- `mqtt` connects with its own client id.
- An input `buffer` has no effect.
- An `http` input cannot give a sample. Create the pipeline and post a message
  to its endpoint.

**The form then runs the transforms over the sample.** kayak builds each
transform with the production code and shows what each stage gives. A
`splitter` gives several batches. A `filter` that matches nothing gives none. A
`buffer` holds the messages and gives nothing. No output receives a message.

**The field boxes suggest field names.** A box that names a message field
suggests the fields that reach that point of the chain. Each suggestion shows
the type and an example value. You can also type a name that the sample did not
contain.

**`fill from sample`** fills a mapping, such as the `columns` of a database
output. It adds one row per field: the path, a name from the path, and the
suggested type.

- A field with different types in the sample gets no type.
- Every row is nullable. Five messages cannot prove that a field is always
  present. Make a column not-null yourself when you know that it is.
- A second click adds nothing. kayak skips fields that are already mapped.

**The sample also feeds the script editor.** See
[writing one in the ui](/pipelines/scripting#writing-one-in-the-ui).

The two halves are endpoints: `POST /api/inputs/sample` and
`POST /api/pipelines/dry-run`. Other clients can use them too.

## save, save as and revert {#the-config-file}

Changes in the UI do not go to the config file. Write them to the file
yourself:

- **`save as…`** writes the running graph to a file in the directory of the
  config. You can choose JSON or YAML.
- **`revert`** loads the config file again and rebuilds every pipeline.
- **`unsaved changes`** in the navbar shows that the running graph is different
  from the file.
- **`create config file`** replaces `save as…` when the server started without
  `--config`. The new file becomes the config file of the server.

The file is the source of truth. Commit it, and use the UI only to try
changes. See [the config file](/pipelines/the-config-file) for its format and
for what save and revert do.
