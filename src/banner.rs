//! The startup banner.
//!
//! Pure on purpose — `banner` builds the string and nothing else, so the shape
//! of it is testable and `main` is left with one `println!`. It is printed
//! *before* `tracing_subscriber` is initialised, which is what keeps a
//! timestamp and a level off the front of every line of it: the banner is not a
//! log record, and a structured log shipper should never see one.
//!
//! Two properties are worth keeping. The art is **six lines, none of them
//! wider than 80 columns**, so it does not wrap in a default terminal — a
//! wrapped banner reads as a broken one. And no line carries trailing
//! whitespace, since the art's own right edge is ragged and padding it would
//! only be invisible bytes in the output. Both are pinned below.

/// The word, in the figlet font usually called "ANSI Shadow". Box-drawing
/// characters, so it needs a UTF-8 terminal — which is every terminal this is
/// run in, and the fallback if it isn't is mojibake for six lines rather than
/// anything that fails.
const WORDMARK: &str = "  ██╗  ██╗ █████╗ ██╗   ██╗ █████╗ ██╗  ██╗
  ██║ ██╔╝██╔══██╗╚██╗ ██╔╝██╔══██╗██║ ██╔╝
  █████╔╝ ███████║ ╚████╔╝ ███████║█████╔╝
  ██╔═██╗ ██╔══██║  ╚██╔╝  ██╔══██║██╔═██╗
  ██║  ██╗██║  ██║   ██║   ██║  ██║██║  ██╗
  ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝";

/// What the thing is, under the word — the one line someone who has just
/// inherited a running server gets for free.
const TAGLINE: &str = "graph-based stream processing";

/// The banner as it is printed, blank line above and below so it stands clear
/// of the shell prompt and of the first log line.
#[must_use]
pub fn banner(version: &str) -> String {
    format!("\n{WORDMARK}\n\n  {TAGLINE} · v{version}\n")
}

/// The version this binary was built as. `CARGO_PKG_VERSION` rather than
/// anything read at runtime — the banner is a property of the build.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::{TAGLINE, banner, version};

    /// The rows of the wordmark: the non-blank lines that are drawing rather
    /// than prose. Note the bottom row is all shadow and carries no `█`, which
    /// is what a filter on that character alone misses.
    fn art_lines(banner: &str) -> Vec<&str> {
        banner
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.contains(char::is_alphanumeric))
            .collect::<Vec<_>>()
    }

    #[test]
    fn the_wordmark_is_six_rows() {
        assert_eq!(art_lines(&banner("1.2.3")).len(), 6);
    }

    #[test]
    fn nothing_wraps_in_an_eighty_column_terminal() {
        for line in banner("1.2.3").lines() {
            assert!(
                line.chars().count() <= 80,
                "banner line is {} columns: {line}",
                line.chars().count()
            );
        }
    }

    #[test]
    fn no_line_carries_trailing_whitespace() {
        for line in banner("1.2.3").lines() {
            assert_eq!(line.trim_end(), line, "trailing whitespace on {line:?}");
        }
    }

    #[test]
    fn the_tagline_carries_the_version_it_was_given() {
        let rendered = banner("9.9.9");
        assert!(rendered.contains(&format!("{TAGLINE} · v9.9.9")), "{rendered}");
    }

    #[test]
    fn the_version_is_the_crate_s_own() {
        assert_eq!(version(), env!("CARGO_PKG_VERSION"));
    }
}
