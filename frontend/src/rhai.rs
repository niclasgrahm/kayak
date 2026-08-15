//! Highlighting rhai source for the script editor.
//!
//! Pure so it can be tested without a DOM, the same convention [`crate::pretty`]
//! and [`crate::log`] follow: source in, lines of spans out, and the component
//! in `app.rs` renders them.
//!
//! ## Why a scanner and not a parser
//!
//! The same reason `pretty.rs` re-indents instead of round-tripping through a
//! `Value`: **what is on screen has to be exactly what is in the box.** This
//! runs over source somebody is part-way through typing, so at almost every
//! keystroke it is looking at something that does not parse — an unclosed
//! string, a half-written `if`. A parser would have nothing to say about those,
//! which is precisely when the highlighting is being watched.
//!
//! So every character of the input appears in the output, in order, including
//! whitespace. That is not a nicety either: the rendered spans sit *behind* a
//! transparent `<textarea>`, and the two only line up if they hold the same
//! text. A scanner that skipped a space would show the code sliding out from
//! under the caret.
//!
//! Nothing here knows what the script means. An unterminated string runs to the
//! end of the line and is coloured as a string, which is what an editor should
//! do while someone is typing one.

/// What a run of characters is, which is the only thing the colouring goes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Whitespace, and anything the scanner has no better word for.
    Plain,
    /// A language keyword — `let`, `if`, `for`, `fn`.
    Keyword,
    /// `true`, `false`, and the unit `()`.
    Literal,
    /// A string or a character, quotes included.
    Str,
    /// A number, in any of the spellings rhai accepts.
    Num,
    /// A `//` line comment or a `/* */` block one.
    Comment,
    /// One of the functions kayak itself puts in scope — `emit`, `recall`,
    /// `remember`, `field`, `now`, `warn`.
    ///
    /// Coloured apart from ordinary identifiers on purpose: these are the whole
    /// interface between a script and the pipeline around it, and seeing at a
    /// glance which calls reach out of the script is most of reading one.
    Host,
    /// Operators, braces, commas.
    Punct,
    /// Anything else — a variable, a property, a call to something else.
    Ident,
}

impl Kind {
    /// The class the span is rendered with. Named here rather than in the view
    /// so the mapping is testable and there is one list of them.
    #[must_use]
    pub fn class(self) -> &'static str {
        match self {
            Self::Plain => "rhai-plain",
            Self::Keyword => "rhai-keyword",
            Self::Literal => "rhai-literal",
            Self::Str => "rhai-str",
            Self::Num => "rhai-num",
            Self::Comment => "rhai-comment",
            Self::Host => "rhai-host",
            Self::Punct => "rhai-punct",
            Self::Ident => "rhai-ident",
        }
    }
}

/// One run of characters of a single kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span {
    pub kind: Kind,
    pub text: String,
}

/// rhai's keywords, as far as the colouring is concerned.
///
/// `import` and `export` are in the list even though the engine refuses them —
/// a script that uses one should look like it is using a keyword, and then fail
/// with the sandbox's own message. Colouring it as an unknown identifier would
/// suggest the problem was a typo.
const KEYWORDS: &[&str] = &[
    "let", "const", "if", "else", "switch", "while", "loop", "for", "in", "do", "until", "break",
    "continue", "return", "fn", "private", "throw", "try", "catch", "import", "export", "as",
    "global", "this",
];

const LITERALS: &[&str] = &["true", "false"];

/// What kayak puts in a script's scope. Kept beside the keywords because to
/// somebody writing a script the difference is invisible — both are simply
/// there — and the editor should show both as given rather than as things the
/// script defined.
const HOST: &[&str] = &[
    "emit",
    "recall",
    "remember",
    "field",
    "now",
    "now_millis",
    "warn",
    "msg",
    "batch",
];

/// Scan `source` into lines of spans.
///
/// The result holds every character of the input: joining the spans of every
/// line with a newline between lines reproduces the source exactly. That is
/// what `reproduces_the_source_exactly` pins, and it is what makes the overlay
/// line up with the textarea behind it.
#[must_use]
pub fn highlight(source: &str) -> Vec<Vec<Span>> {
    let mut lines: Vec<Vec<Span>> = Vec::new();
    let mut line: Vec<Span> = Vec::new();
    let chars: Vec<char> = source.chars().collect();
    let mut i = 0;

    // Only block comments survive a line ending, so that is the only state
    // carried across one.
    let mut in_block_comment = false;

    while i < chars.len() {
        if chars[i] == '\n' {
            lines.push(std::mem::take(&mut line));
            i += 1;
            continue;
        }

        if in_block_comment {
            let (text, len, ended) = block_comment_body(&chars[i..]);
            push(&mut line, Kind::Comment, text);
            in_block_comment = !ended;
            i += len;
            continue;
        }

        let c = chars[i];
        let (kind, len) = if c == '/' && chars.get(i + 1) == Some(&'/') {
            (Kind::Comment, to_end_of_line(&chars[i..]))
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            let (_, len, ended) = block_comment_body(&chars[i + 2..]);
            in_block_comment = !ended;
            (Kind::Comment, 2 + len)
        } else if c == '"' || c == '\'' || c == '`' {
            (Kind::Str, quoted(&chars[i..], c))
        } else if c.is_ascii_digit() {
            (Kind::Num, number(&chars[i..]))
        } else if is_word_start(c) {
            let len = word(&chars[i..]);
            let text: String = chars[i..i + len].iter().collect();
            (word_kind(&text), len)
        } else if c.is_whitespace() {
            (Kind::Plain, run(&chars[i..], |c| c.is_whitespace() && c != '\n'))
        } else {
            (Kind::Punct, run(&chars[i..], is_punct))
        };

        push(&mut line, kind, chars[i..i + len].iter().collect());
        i += len;
    }
    lines.push(line);
    lines
}

/// Append a span, merging it into the previous one when they are the same kind.
///
/// Fewer, longer spans means fewer DOM nodes for the same text, and the editor
/// re-renders the whole overlay on every keystroke.
fn push(line: &mut Vec<Span>, kind: Kind, text: String) {
    if text.is_empty() {
        return;
    }
    match line.last_mut() {
        Some(last) if last.kind == kind => last.text.push_str(&text),
        _ => line.push(Span { kind, text }),
    }
}

fn is_word_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn word_kind(text: &str) -> Kind {
    if KEYWORDS.contains(&text) {
        Kind::Keyword
    } else if LITERALS.contains(&text) {
        Kind::Literal
    } else if HOST.contains(&text) {
        Kind::Host
    } else {
        Kind::Ident
    }
}

fn is_punct(c: char) -> bool {
    !c.is_alphanumeric() && c != '_' && !c.is_whitespace() && c != '"' && c != '\'' && c != '`'
}

fn run(chars: &[char], accept: impl Fn(char) -> bool) -> usize {
    chars.iter().take_while(|c| accept(**c)).count().max(1)
}

fn word(chars: &[char]) -> usize {
    run(chars, |c| c.is_alphanumeric() || c == '_')
}

/// A number, including the spellings that carry a `.`, an exponent, a `0x`
/// prefix or `_` separators. Deliberately loose: this is colouring, not
/// validation, and a half-typed `1.` should stay a number rather than becoming
/// a number and a stray dot.
fn number(chars: &[char]) -> usize {
    run(chars, |c| {
        c.is_ascii_alphanumeric() || c == '.' || c == '_'
    })
}

fn to_end_of_line(chars: &[char]) -> usize {
    chars.iter().take_while(|c| **c != '\n').count()
}

/// A quoted run, ending at the matching quote or at the end of the line.
///
/// Stopping at the line end is what keeps a string someone is half way through
/// typing from colouring the rest of the file. rhai's strings can span lines,
/// so this is a deliberate trade — the common case is the unclosed one.
fn quoted(chars: &[char], quote: char) -> usize {
    let mut i = 1;
    while i < chars.len() {
        match chars[i] {
            '\n' => return i,
            '\\' => i += 2,
            c if c == quote => return i + 1,
            _ => i += 1,
        }
    }
    chars.len()
}

/// The body of a block comment up to `*/`, and whether it found one.
///
/// Returns the text on this line only; the caller carries the "still inside
/// one" flag across the line ending, which is the only state this scanner has.
fn block_comment_body(chars: &[char]) -> (String, usize, bool) {
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\n' {
            return (chars[..i].iter().collect(), i, false);
        }
        if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
            return (chars[..=i + 1].iter().collect(), i + 2, true);
        }
        i += 1;
    }
    (chars.iter().collect(), chars.len(), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(source: &str) -> Vec<Vec<(Kind, String)>> {
        highlight(source)
            .into_iter()
            .map(|line| line.into_iter().map(|s| (s.kind, s.text)).collect())
            .collect()
    }

    fn rejoin(source: &str) -> String {
        highlight(source)
            .into_iter()
            .map(|line| line.into_iter().map(|s| s.text).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// **The property everything else rests on.** The spans are rendered behind
    /// a transparent textarea holding the same text, so a single character
    /// dropped or reordered slides the code out from under the caret. Every
    /// shape the scanner has a branch for is in here.
    #[test]
    fn reproduces_the_source_exactly() {
        for source in [
            "",
            "\n",
            "emit(msg);",
            "let x = 1;\n\nlet y = 2;\n",
            "  indented(); \t tabbed();",
            "// a comment\nlet a = 1; // trailing\n",
            "/* block\n   spanning\n   lines */ let a = 1;",
            "let s = \"with \\\" an escape\";",
            "let unterminated = \"oh no\nlet next = 1;",
            "let n = 1_000.5e3; let h = 0xff;",
            "msg.lines[0].qty * 2 >= 4 && !done",
            "let emoji = \"a — ü 🎉\";",
            "/* never closed",
        ] {
            assert_eq!(rejoin(source), source, "source was not reproduced: {source:?}");
        }
    }

    #[test]
    fn keywords_literals_and_host_functions_are_told_apart() {
        let line = &kinds("let ok = true; emit(other);")[0];
        let of = |name: &str| {
            line.iter()
                .find(|(_, text)| text == name)
                .map(|(kind, _)| *kind)
        };
        assert_eq!(of("let"), Some(Kind::Keyword));
        assert_eq!(of("true"), Some(Kind::Literal));
        assert_eq!(of("emit"), Some(Kind::Host));
        assert_eq!(of("other"), Some(Kind::Ident));
        assert_eq!(of("ok"), Some(Kind::Ident));
    }

    /// `msg` and `batch` are given to the script rather than declared by it, so
    /// they read as part of the interface — the same reason `emit` does.
    #[test]
    fn the_bindings_a_script_is_given_read_as_host_names() {
        let line = &kinds("emit(#{ n: msg.n, total: batch.len });")[0];
        for name in ["msg", "batch"] {
            let kind = line
                .iter()
                .find(|(_, text)| text == name)
                .map(|(kind, _)| *kind);
            assert_eq!(kind, Some(Kind::Host), "{name} should read as a given name");
        }
    }

    /// A block comment is the only thing that carries across a line ending, so
    /// it is the only state the scanner has and the only thing that can get it
    /// wrong.
    #[test]
    fn a_block_comment_spans_lines_and_stops_where_it_closes() {
        let lines = kinds("let a = 1; /* one\ntwo */ let b = 2;");
        assert_eq!(lines.len(), 2);
        assert!(
            lines[1].iter().any(|(kind, text)| *kind == Kind::Comment && text.contains("two")),
            "the second line's comment part should still be a comment: {lines:?}"
        );
        assert!(
            lines[1].iter().any(|(kind, text)| *kind == Kind::Keyword && text == "let"),
            "and the code after it should not be: {lines:?}"
        );
    }

    /// While someone is typing a string there is no closing quote yet. Running
    /// to the end of the file would colour everything below it, which is the
    /// worst moment for the editor to give up.
    #[test]
    fn an_unterminated_string_stops_at_the_line_end() {
        let lines = kinds("let s = \"oh no\nlet next = 1;");
        assert!(
            lines[1].iter().any(|(kind, text)| *kind == Kind::Keyword && text == "let"),
            "the next line should be code again: {lines:?}"
        );
    }

    /// Colouring, not validation: a half-typed number is still a number.
    #[test]
    fn a_half_typed_number_is_still_one() {
        let line = &kinds("let x = 1.")[0];
        assert!(
            line.iter().any(|(kind, text)| *kind == Kind::Num && text == "1."),
            "{line:?}"
        );
    }

    /// Multi-byte characters are why the scanner works on `char`s rather than
    /// bytes — slicing a `&str` at a byte offset inside one would panic.
    #[test]
    fn multi_byte_characters_do_not_split() {
        assert_eq!(rejoin("let s = \"ü — 🎉\";"), "let s = \"ü — 🎉\";");
    }
}
