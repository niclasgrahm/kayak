//! Pretty-printing one log message for an expanded row.
//!
//! Pure so it can be tested without a DOM, the same convention `log.rs` and
//! `stats.rs` follow: the component in `app.rs` hands a string in and renders
//! the lines that come out.
//!
//! **It re-indents the text rather than parsing it into a `Value` and printing
//! that back.** The reason is that a `serde_json::Map` is a `BTreeMap` here —
//! `preserve_order` isn't on, and turning it on is a workspace-wide feature
//! that would reach the config renderer — so a round trip through `Value` would
//! silently sort the keys of every message on screen. A payload is shown to be
//! *read*, and a reordered one is a quiet lie about what arrived. Scanning the
//! text also keeps a number's own digits (`1.50`, `1e3`) exactly as the source
//! wrote them, and hands the highlighter its token kinds for free.
//!
//! Anything that isn't well-formed JSON comes back as [`Rendered::Plain`] and
//! is shown as it is. That is not an edge case: the feed cuts a message to
//! `kayak_core::MAX_MESSAGE_BYTES` and marks the cut with an ellipsis, so a fat
//! message *arrives* truncated. The box still shows all of what there is,
//! wrapped, which is more than the row it came from does.

/// What a piece of a line is, which is the only thing the colouring goes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Braces, brackets, commas, the colon after a key.
    Punct,
    /// A string in key position.
    Key,
    /// A string in value position.
    Str,
    /// A number.
    Num,
    /// `true`, `false` or `null`.
    Literal,
}

impl Kind {
    /// The class the span is rendered with. Named here rather than in the view
    /// so the mapping is testable and there is one list of them.
    #[must_use]
    pub fn class(self) -> &'static str {
        match self {
            Self::Punct => "json-punct",
            Self::Key => "json-key",
            Self::Str => "json-str",
            Self::Num => "json-num",
            Self::Literal => "json-literal",
        }
    }
}

/// One run of characters of a single kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span {
    pub kind: Kind,
    pub text: String,
}

impl Span {
    fn new(kind: Kind, text: impl Into<String>) -> Self {
        Self {
            kind,
            text: text.into(),
        }
    }
}

/// One rendered line: how deep it is nested, and what is on it.
///
/// The indent is a depth rather than the spaces themselves so the view can
/// decide what a level is worth; [`Line::indent`] is what turns it into text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Line {
    pub depth: usize,
    pub spans: Vec<Span>,
}

impl Line {
    /// The leading whitespace of this line: two spaces a level, which is what
    /// every JSON pretty-printer does and what the clipboard text uses.
    #[must_use]
    pub fn indent(&self) -> String {
        "  ".repeat(self.depth)
    }

    /// The line as plain text, indent included.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut text = self.indent();
        for span in &self.spans {
            text.push_str(&span.text);
        }
        text
    }
}

/// A message as the expanded box shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rendered {
    /// Well-formed JSON, laid out and coloured.
    Json(Vec<Line>),
    /// Anything else — a truncated payload, a non-JSON one, an error message.
    Plain(String),
}

impl Rendered {
    /// What the copy button puts on the clipboard: the text that is on screen,
    /// not the compact form it arrived as. The box is what was copied.
    #[must_use]
    pub fn to_text(&self) -> String {
        match self {
            Self::Json(lines) => lines
                .iter()
                .map(Line::to_text)
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Plain(text) => text.clone(),
        }
    }
}

/// Lay one message out for display.
#[must_use]
pub fn render(message: &str) -> Rendered {
    match tokens(message) {
        Some(tokens) => Rendered::Json(lay_out(&tokens)),
        None => Rendered::Plain(message.to_string()),
    }
}

/// Every message of a batch, in order.
#[must_use]
pub fn render_all(messages: &[String]) -> Vec<Rendered> {
    messages.iter().map(|message| render(message)).collect()
}

/// The whole batch as one blob for the clipboard: one message after another,
/// each laid out, blank line between. A batch copied out of a card is a batch,
/// so this is not the compact per-line form the log bar's copy produces.
#[must_use]
pub fn all_to_text(messages: &[Rendered]) -> String {
    messages
        .iter()
        .map(Rendered::to_text)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// What the scanner produced. `Open`/`Close` carry the character so the layout
/// pass never has to remember which bracket it is closing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Raw {
    Open(char),
    Close(char),
    Comma,
    Colon,
    Str,
    Bare,
}

struct Token {
    raw: Raw,
    text: String,
}

/// Scan `text` into tokens, or `None` if it isn't well-formed JSON.
///
/// Well-formed enough to lay out, which is a slightly weaker claim than a
/// parser's: brackets have to match and every token has to be one of the four
/// shapes, but the grammar between them isn't checked. That is deliberate — the
/// failure this actually has to catch is a message cut in half, and everything
/// it lets through still renders as itself.
fn tokens(text: &str) -> Option<Vec<Token>> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut open: Vec<char> = Vec::new();
    let mut chars = text.char_indices().peekable();

    while let Some((start, c)) = chars.next() {
        match c {
            c if c.is_whitespace() => {}
            '{' | '[' => {
                open.push(c);
                tokens.push(Token {
                    raw: Raw::Open(c),
                    text: c.to_string(),
                });
            }
            '}' | ']' => {
                let wanted = if c == '}' { '{' } else { '[' };
                if open.pop() != Some(wanted) {
                    return None;
                }
                tokens.push(Token {
                    raw: Raw::Close(c),
                    text: c.to_string(),
                });
            }
            ',' => tokens.push(Token {
                raw: Raw::Comma,
                text: ",".to_string(),
            }),
            ':' => tokens.push(Token {
                raw: Raw::Colon,
                text: ":".to_string(),
            }),
            '"' => {
                let mut end = None;
                let mut escaped = false;
                for (i, c) in chars.by_ref() {
                    if escaped {
                        escaped = false;
                    } else if c == '\\' {
                        escaped = true;
                    } else if c == '"' {
                        end = Some(i + 1);
                        break;
                    }
                }
                // an unterminated string is the shape a cut-off message
                // usually has, and the whole reason there is a fallback
                let end = end?;
                tokens.push(Token {
                    raw: Raw::Str,
                    text: text.get(start..end)?.to_string(),
                });
            }
            _ => {
                let mut end = text.len();
                while let Some(&(i, c)) = chars.peek() {
                    if c.is_whitespace() || matches!(c, ',' | ':' | '}' | ']' | '{' | '[') {
                        end = i;
                        break;
                    }
                    chars.next();
                }
                let word = text.get(start..end)?;
                if !is_bare_value(word) {
                    return None;
                }
                tokens.push(Token {
                    raw: Raw::Bare,
                    text: word.to_string(),
                });
            }
        }
    }

    if open.is_empty() && !tokens.is_empty() {
        Some(tokens)
    } else {
        None
    }
}

/// Whether an unquoted run of characters is a value JSON could have written.
/// The ellipsis a truncated message ends with fails here when it lands outside
/// a string, which is the other half of the cut-off case.
fn is_bare_value(word: &str) -> bool {
    matches!(word, "true" | "false" | "null") || word.parse::<f64>().is_ok()
}

/// Turn the token stream into indented lines.
///
/// A container opens a line and closes one; an empty container stays on the
/// line it started, because `{}` split over two lines reads as something with
/// contents that failed to render.
fn lay_out(tokens: &[Token]) -> Vec<Line> {
    let mut lines: Vec<Line> = Vec::new();
    let mut spans: Vec<Span> = Vec::new();
    let mut depth = 0usize;
    // which container each level is, so a comma knows whether the next string
    // is a key or a value
    let mut stack: Vec<bool> = Vec::new();
    let mut expect_key = false;

    let mut flush = |spans: &mut Vec<Span>, depth: usize| {
        if !spans.is_empty() {
            lines.push(Line {
                depth,
                spans: std::mem::take(spans),
            });
        }
    };

    let mut i = 0;
    while let Some(token) = tokens.get(i) {
        match token.raw {
            Raw::Open(c) => {
                spans.push(Span::new(Kind::Punct, &token.text));
                if let Some(next) = tokens.get(i + 1)
                    && matches!(next.raw, Raw::Close(_))
                {
                    spans.push(Span::new(Kind::Punct, &next.text));
                    i += 1;
                } else {
                    flush(&mut spans, depth);
                    stack.push(c == '{');
                    expect_key = c == '{';
                    depth += 1;
                }
            }
            Raw::Close(_) => {
                flush(&mut spans, depth);
                depth = depth.saturating_sub(1);
                stack.pop();
                expect_key = false;
                spans.push(Span::new(Kind::Punct, &token.text));
            }
            Raw::Comma => {
                spans.push(Span::new(Kind::Punct, ","));
                flush(&mut spans, depth);
                expect_key = stack.last().copied().unwrap_or(false);
            }
            Raw::Colon => {
                spans.push(Span::new(Kind::Punct, ": "));
                expect_key = false;
            }
            Raw::Str => {
                let kind = if expect_key { Kind::Key } else { Kind::Str };
                spans.push(Span::new(kind, &token.text));
            }
            Raw::Bare => {
                let kind = if is_number(&token.text) {
                    Kind::Num
                } else {
                    Kind::Literal
                };
                spans.push(Span::new(kind, &token.text));
            }
        }
        i += 1;
    }
    flush(&mut spans, depth);
    lines
}

fn is_number(word: &str) -> bool {
    !matches!(word, "true" | "false" | "null")
}

#[cfg(test)]
mod tests {
    use super::{Kind, Rendered, render, render_all};

    fn text_of(message: &str) -> String {
        render(message).to_text()
    }

    fn kinds(message: &str) -> Vec<(Kind, String)> {
        match render(message) {
            Rendered::Json(lines) => lines
                .into_iter()
                .flat_map(|line| line.spans)
                .map(|span| (span.kind, span.text))
                .collect(),
            Rendered::Plain(text) => panic!("expected json, got plain: {text}"),
        }
    }

    #[test]
    fn an_object_is_indented_one_field_a_line() {
        assert_eq!(
            text_of(r#"{"id":7,"temp":21.5}"#),
            "{\n  \"id\": 7,\n  \"temp\": 21.5\n}"
        );
    }

    #[test]
    fn nesting_indents_two_spaces_a_level() {
        assert_eq!(
            text_of(r#"{"a":{"b":[1,2]}}"#),
            "{\n  \"a\": {\n    \"b\": [\n      1,\n      2\n    ]\n  }\n}"
        );
    }

    /// A round trip through `serde_json::Value` would sort these. The whole
    /// reason this scans the text is that a payload is shown to be read.
    #[test]
    fn key_order_is_the_message_s_own() {
        assert_eq!(
            text_of(r#"{"zeta":1,"alpha":2}"#),
            "{\n  \"zeta\": 1,\n  \"alpha\": 2\n}"
        );
    }

    /// Same argument, for the value side: `1.50` is what arrived and `1.5` is
    /// what a re-serialised f64 would say.
    #[test]
    fn a_number_keeps_its_own_spelling() {
        assert_eq!(text_of(r#"{"v":1.50}"#), "{\n  \"v\": 1.50\n}");
        assert_eq!(text_of(r#"{"v":1e3}"#), "{\n  \"v\": 1e3\n}");
    }

    #[test]
    fn an_empty_container_stays_on_its_line() {
        assert_eq!(text_of(r#"{"a":{},"b":[]}"#), "{\n  \"a\": {},\n  \"b\": []\n}");
        assert_eq!(text_of("{}"), "{}");
    }

    #[test]
    fn a_key_is_told_from_a_string_value() {
        assert_eq!(
            kinds(r#"{"name":"kayak"}"#),
            vec![
                (Kind::Punct, "{".to_string()),
                (Kind::Key, "\"name\"".to_string()),
                (Kind::Punct, ": ".to_string()),
                (Kind::Str, "\"kayak\"".to_string()),
                (Kind::Punct, "}".to_string()),
            ]
        );
    }

    /// A string inside an array is a value, and the comma before it must not
    /// put the next one back into key position.
    #[test]
    fn strings_in_an_array_are_values() {
        let kinds = kinds(r#"["a","b"]"#);
        assert!(
            kinds
                .iter()
                .filter(|(kind, _)| *kind != Kind::Punct)
                .all(|(kind, _)| *kind == Kind::Str),
            "{kinds:?}"
        );
    }

    #[test]
    fn numbers_and_literals_are_told_apart() {
        let kinds = kinds(r#"{"a":1,"b":true,"c":null}"#);
        assert!(kinds.contains(&(Kind::Num, "1".to_string())), "{kinds:?}");
        assert!(
            kinds.contains(&(Kind::Literal, "true".to_string())),
            "{kinds:?}"
        );
        assert!(
            kinds.contains(&(Kind::Literal, "null".to_string())),
            "{kinds:?}"
        );
    }

    /// The braces and colons a payload happens to contain are text, not
    /// structure — reading them as structure is how a highlighter loses a line.
    #[test]
    fn punctuation_inside_a_string_is_not_structure() {
        assert_eq!(
            text_of(r#"{"a":"{\"b\": 1}"}"#),
            "{\n  \"a\": \"{\\\"b\\\": 1}\"\n}"
        );
    }

    /// The case the feed actually produces: `MAX_MESSAGE_BYTES` cuts a message
    /// and marks it, so the box has to show it rather than a parse failure.
    #[test]
    fn a_truncated_message_falls_back_to_plain_text() {
        let cut = r#"{"id":7,"payload":"aaaa…"#;
        assert_eq!(render(cut), Rendered::Plain(cut.to_string()));
        let cut_outside_a_string = r#"{"id":7,"temp":21…"#;
        assert_eq!(
            render(cut_outside_a_string),
            Rendered::Plain(cut_outside_a_string.to_string())
        );
    }

    #[test]
    fn anything_that_isnt_json_is_shown_as_it_is() {
        for message in ["", "nats: connection refused", "{", "]", "{\"a\":1"] {
            assert_eq!(render(message), Rendered::Plain(message.to_string()));
        }
    }

    /// A message that is a bare scalar is still a message.
    #[test]
    fn a_top_level_scalar_renders() {
        assert_eq!(text_of("42"), "42");
        assert_eq!(text_of(r#""hello""#), "\"hello\"");
    }

    #[test]
    fn a_batch_renders_message_by_message() {
        let rendered = render_all(&[r#"{"a":1}"#.to_string(), "nope".to_string()]);
        assert_eq!(rendered.len(), 2);
        assert_eq!(rendered[0].to_text(), "{\n  \"a\": 1\n}");
        assert_eq!(rendered[1], Rendered::Plain("nope".to_string()));
    }

    /// The clipboard gets what is on screen, laid out — the compact form is
    /// what the row already showed.
    #[test]
    fn copying_a_batch_separates_its_messages() {
        let rendered = render_all(&[r#"{"a":1}"#.to_string(), r#"{"b":2}"#.to_string()]);
        assert_eq!(
            super::all_to_text(&rendered),
            "{\n  \"a\": 1\n}\n\n{\n  \"b\": 2\n}"
        );
    }
}
