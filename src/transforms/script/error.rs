//! What a script got wrong, and where.
//!
//! A script is the one component whose configuration has a *position* in it, so
//! it is the one whose errors have to carry one. A flattened string would be
//! enough for the log line and useless for everything else: the dry-run
//! endpoint hands the line and column to whatever called it, and the editor
//! puts a marker on that line. Both need the number, not a sentence containing
//! the number.

use std::fmt;

/// A script that would not compile, or a run of one that would not finish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptError {
    /// What went wrong, in rhai's own words. Not prefixed with the position —
    /// see [`ScriptError::located`] for why that is kept apart.
    pub message: String,
    /// Where in the script, when rhai knew. One-based, as an editor counts.
    ///
    /// `None` is not unusual: an error raised by a host function or by the
    /// operation budget belongs to the run rather than to a line.
    pub position: Option<Position>,
    /// Whether this stopped the script from compiling at all, as against
    /// stopping one run of it. The distinction is what decides whether a
    /// pipeline refuses to start or fails a batch.
    pub kind: ScriptErrorKind,
}

/// A one-based line and column, as an editor counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptErrorKind {
    /// The script does not parse. Caught when the pipeline is built, so the
    /// pipeline refuses to start.
    Compile,
    /// The script parsed but this run of it failed — a missing field, a type
    /// that would not convert, a `throw`, or the operation budget running out.
    Runtime,
}

impl ScriptError {
    pub fn compile(message: impl Into<String>, position: Option<Position>) -> Self {
        Self {
            message: message.into(),
            position,
            kind: ScriptErrorKind::Compile,
        }
    }

    pub fn runtime(message: impl Into<String>, position: Option<Position>) -> Self {
        Self {
            message: message.into(),
            position,
            kind: ScriptErrorKind::Runtime,
        }
    }

    /// The message with its position appended, for somewhere that can only
    /// carry one string — a log line, or the `history` error signature.
    ///
    /// Kept as a method rather than baked into `message` because the two
    /// consumers that *can* use the position want it as a number.
    #[must_use]
    pub fn located(&self) -> String {
        match self.position {
            Some(Position { line, column }) => {
                format!("{} (line {line}, column {column})", self.message)
            }
            None => self.message.clone(),
        }
    }
}

impl fmt::Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.located())
    }
}

impl std::error::Error for ScriptError {}

/// rhai's `Display` for a runtime error appends its own position — "Too many
/// operations (line 1, position 6)" — and this type carries the position as
/// numbers beside the message. Left in, it would be said twice: once in rhai's
/// spelling and once in [`ScriptError::located`]'s, and the dry run would hand a
/// client a `message` with a position baked into it that no editor can use.
///
/// So the tail is taken off, and only when it is the position rhai just told us
/// about. Anything else in trailing parentheses is part of what the error says
/// and stays.
#[must_use]
pub fn strip_position(message: &str) -> &str {
    let trimmed = message.trim_end();
    let Some(open) = trimmed.rfind(" (") else {
        return message;
    };
    let inside = &trimmed[open + 2..];
    let is_position = inside.ends_with(')')
        && (inside.starts_with("line ") || inside.starts_with("@ "))
        && !inside[..inside.len() - 1].contains(')');
    if is_position {
        &trimmed[..open]
    } else {
        message
    }
}

/// rhai reports "no position" as line zero, which would render as a marker on a
/// line that does not exist. Reading it back as `None` is what keeps the
/// editor's gutter honest.
#[must_use]
pub fn position_of(position: rhai::Position) -> Option<Position> {
    match (position.line(), position.position()) {
        (Some(line), Some(column)) => Some(Position { line, column }),
        // A position rhai knows the line of but not the column within it —
        // the start of the line is the honest answer, and it is still a
        // useful marker.
        (Some(line), None) => Some(Position { line, column: 1 }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// rhai says the position in its message and this type says it in numbers.
    /// Saying it twice is what `located()` would otherwise do — and the numbers
    /// are the half a client can act on.
    #[test]
    fn a_runtime_message_does_not_carry_the_position_twice() {
        assert_eq!(
            strip_position("Too many operations (line 1, position 6)"),
            "Too many operations"
        );
        assert_eq!(
            strip_position("Runtime error: nope (@ position 12)"),
            "Runtime error: nope"
        );
    }

    /// Only rhai's own position tail comes off. Parentheses are ordinary
    /// punctuation in an error message, and eating a real one would cut the
    /// message short.
    #[test]
    fn other_trailing_parentheses_are_left_alone() {
        for message in [
            "expected a map (got an array)",
            "no such field 'a' (did you mean 'b'?)",
            "unbalanced (line 1, position 6",
            "plain message",
        ] {
            assert_eq!(strip_position(message), message, "{message:?} was altered");
        }
    }

    #[test]
    fn a_located_error_reads_as_one_line() {
        let err = ScriptError::runtime("something went wrong", Some(Position { line: 3, column: 7 }));
        assert_eq!(err.located(), "something went wrong (line 3, column 7)");
    }

    /// A failure that belongs to the run rather than to a line has nothing to
    /// append, and must not grow an empty pair of brackets.
    #[test]
    fn an_error_without_a_position_is_just_its_message() {
        let err = ScriptError::runtime("no position for this", None);
        assert_eq!(err.located(), "no position for this");
    }
}
