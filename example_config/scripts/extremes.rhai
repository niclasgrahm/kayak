// One message out of a whole batch: the extremes of whatever the buffer
// gathered, and the spread between them.
//
// `batch` scope, which is why the input in front of this has a `buffer` on it —
// without one the heartbeat arrives a message at a time and every "batch" here
// would hold exactly one reading. That is the general rule: batch scope is only
// interesting when something upstream has made the batches worth looking at.
//
// A reducer can do `min` and `max`; what it cannot do is `spread`, because that
// is arithmetic *between* two aggregates.
//
// The arithmetic itself lives in `scripts/shared/readings.rhai` — the swings
// script classifies with the same module, and a helper written twice drifts.
// The import path is relative to the config file's directory.

import "scripts/shared/readings" as readings;

let lowest = batch[0].value;
let highest = batch[0].value;

for reading in batch {
    if reading.value < lowest {
        lowest = reading.value;
    }
    if reading.value > highest {
        highest = reading.value;
    }
}

// One emit, so one batch of one message leaves.
emit([#{
    readings: batch.len,
    lowest: lowest,
    highest: highest,
    spread: readings::spread(lowest, highest),
    at: now(),
}]);
