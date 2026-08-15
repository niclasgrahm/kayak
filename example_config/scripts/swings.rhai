// Change detection: which way the heartbeat is travelling, and by how much
// since the last reading.
//
// This is the combination that a script is actually for. `map` has no memory,
// and `remember`/`recall` have memory but nowhere to put the *comparison* —
// the "is this bigger than last time" is the bit with no declarative spelling.

let previous = recall("heartbeat");
remember("heartbeat", #{ value: msg.value });

if previous == () {
    // The warm-up. Every stateful pipeline has one, and saying so is better
    // than emitting a delta against nothing.
    msg.direction = "unknown";
    msg.delta = 0.0;
} else {
    let delta = msg.value - previous.value;
    msg.delta = delta;
    msg.direction = if delta > 0.0 {
        "rising"
    } else if delta < 0.0 {
        "falling"
    } else {
        "flat"
    };
}

msg
