// Helpers both heartbeat scripts share. This is what a module is for: the
// same three-way classification written twice drifts, and a module is the
// spelling that keeps it one.
//
// A module holds functions (and exported constants). Its top level runs once,
// when the pipeline is built — not per message — so anything per-message
// belongs in the importing script.
//
// Imported as `import "scripts/shared/readings" as readings;` — the path is
// relative to the directory the config file is in, the same place every
// script path resolves, and the `.rhai` extension is implied.

// Which way a change is travelling, as the UI words it.
fn direction(delta) {
    if delta > 0.0 {
        "rising"
    } else if delta < 0.0 {
        "falling"
    } else {
        "flat"
    }
}

// The distance between the extremes of a window of readings.
fn spread(lowest, highest) {
    highest - lowest
}
