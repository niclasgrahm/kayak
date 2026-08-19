//! The script editor's pure half: highlighting rhai source, and working out
//! what is under the caret so the editor can answer for it.
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

/// Whether a name is one kayak puts in a script's scope.
///
/// Read out of [`kayak_core::script::builtins`] rather than listed again here,
/// which is what makes the colour, the completion list, the reference panel and
/// the engine's registrations one fact. There used to be a `const HOST` beside
/// the keywords, and it drifted the moment a function was added.
///
/// **Deliberately not scope-aware**, unlike the completion list: `batch` in a
/// `message`-scoped script is still a kayak name rather than a variable
/// somebody declared, and colouring it as an ordinary identifier would suggest
/// the fix is to define it. What being out of scope changes is whether the
/// editor *offers* it — see [`completions`] — and what the hint under it says.
fn is_host(name: &str) -> bool {
    kayak_core::script::builtin(name).is_some()
}

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
    } else if is_host(text) {
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

// ── what is under the caret ─────────────────────────────────────────────────
//
// Everything below answers one of two questions the editor asks about a
// position in the source: *what is being typed here* (so it can offer
// completions) and *what word is here* (so it can describe it). Both work in
// `char`s, which is the unit the rest of this module and the editor's own
// geometry use — the box is monospaced, so a column is a character is a `ch`.
//
// One known limit, shared with the tab handler in `app.rs`: a textarea reports
// its selection in UTF-16 code units, so a character outside the basic plane
// (an emoji in a string literal) counts as two there and one here. Everything
// on the line after it is then out by one until the next keystroke. Fixing it
// means threading UTF-16 offsets all the way through, and the cost — an emoji
// in a script, before the caret, on the line being completed — did not earn
// that.

/// A position in the source, zero-based, as the editor lays it out: a line and
/// a character within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caret {
    pub line: usize,
    pub column: usize,
}

/// Where a character offset falls in the text.
#[must_use]
pub fn caret_at(source: &str, offset: usize) -> Caret {
    let mut caret = Caret { line: 0, column: 0 };
    for c in source.chars().take(offset) {
        if c == '\n' {
            caret.line += 1;
            caret.column = 0;
        } else {
            caret.column += 1;
        }
    }
    caret
}

/// What somebody is part-way through typing at an offset.
///
/// The `chain` is what a completion needs beyond the word itself: `msg.sensor.i`
/// is a chain of `["msg", "sensor"]` and a prefix of `"i"`, which is what turns
/// a list of every name there is into the two fields that could come next.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Typed {
    /// The dotted segments before the word being typed, outermost first.
    pub chain: Vec<String>,
    /// The word being typed, which may be empty — `msg.` with nothing after it
    /// is the moment the list is most useful.
    pub prefix: String,
    /// The character offset the prefix starts at, which is what accepting a
    /// completion replaces from.
    pub start: usize,
}

/// Read back from `offset` to see what is being typed there.
#[must_use]
pub fn typing_at(source: &str, offset: usize) -> Typed {
    let chars: Vec<char> = source.chars().collect();
    let offset = offset.min(chars.len());

    let mut start = offset;
    while start > 0 && is_word_char(chars[start - 1]) {
        start -= 1;
    }
    let prefix: String = chars[start..offset].iter().collect();

    // Then the dotted chain in front of it, walked outwards: `a.b.` yields
    // ["a", "b"]. A segment that is not a word ends the walk, so `f().` and
    // `"text".` offer nothing rather than guessing.
    let mut chain: Vec<String> = Vec::new();
    let mut at = start;
    while at > 0 && chars[at - 1] == '.' {
        let mut segment_start = at - 1;
        while segment_start > 0 && is_word_char(chars[segment_start - 1]) {
            segment_start -= 1;
        }
        if segment_start == at - 1 {
            break;
        }
        chain.push(chars[segment_start..at - 1].iter().collect());
        at = segment_start;
    }
    chain.reverse();

    Typed {
        chain,
        prefix,
        start,
    }
}

/// The word at a position, if there is one — what the hint under the pointer
/// or the caret is about.
#[must_use]
pub fn word_at(source: &str, caret: Caret) -> Option<String> {
    let line: Vec<char> = source.lines().nth(caret.line)?.chars().collect();
    if caret.column >= line.len() || !is_word_char(line[caret.column]) {
        return None;
    }
    let mut start = caret.column;
    while start > 0 && is_word_char(line[start - 1]) {
        start -= 1;
    }
    let mut end = caret.column;
    while end < line.len() && is_word_char(line[end]) {
        end += 1;
    }
    Some(line[start..end].iter().collect())
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// What a completion row offers, which decides how it is drawn and where it is
/// sorted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompletionKind {
    /// A function or binding kayak provides.
    Builtin,
    /// A field the sample showed at this point in the chain.
    Field,
    /// A rhai keyword.
    Keyword,
}

impl CompletionKind {
    #[must_use]
    pub fn class(self) -> &'static str {
        match self {
            Self::Builtin => "completion-builtin",
            Self::Field => "completion-field",
            Self::Keyword => "completion-keyword",
        }
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Builtin => "kayak",
            Self::Field => "field",
            Self::Keyword => "rhai",
        }
    }
}

/// One row of the completion popup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    /// What the row reads as — the signature for a builtin, the name for a
    /// field.
    pub label: String,
    /// The one-liner beside it.
    pub detail: String,
    /// What replaces the prefix when it is accepted.
    pub insert: String,
    /// Where the caret goes afterwards, as an offset into `insert`. This is
    /// what puts it inside a function's brackets.
    pub caret: usize,
    pub kind: CompletionKind,
}

/// What could come next at a position.
///
/// Three sources, and the order is deliberate: what kayak gives you, then what
/// your own data carries, then the language. Someone reaching for a completion
/// in a five-line transform is nearly always after the first, and a list led by
/// `const` and `continue` is one nobody reads to the end of.
///
/// `fields` is the sample's answer at this point in the chain — a dotted path
/// and what type it held — so an empty slice is simply a form nobody has
/// sampled yet, and the builtins still come back.
#[must_use]
pub fn completions(
    typed: &Typed,
    scope: kayak_core::script::ScriptScope,
    fields: &[(String, String)],
) -> Vec<Completion> {
    let mut out: Vec<Completion> = Vec::new();

    if typed.chain.is_empty() {
        for builtin in kayak_core::script::builtins()
            .iter()
            .filter(|builtin| builtin.in_scope(scope))
            .filter(|builtin| builtin.name.starts_with(&typed.prefix))
        {
            let (insert, caret) = builtin.completion();
            out.push(Completion {
                label: builtin.signature.to_string(),
                detail: builtin.summary.to_string(),
                insert,
                caret,
                kind: CompletionKind::Builtin,
            });
        }
        for keyword in KEYWORDS.iter().chain(LITERALS) {
            if keyword.starts_with(&typed.prefix) {
                out.push(Completion {
                    label: (*keyword).to_string(),
                    detail: String::new(),
                    insert: (*keyword).to_string(),
                    caret: keyword.len(),
                    kind: CompletionKind::Keyword,
                });
            }
        }
        return out;
    }

    // A dotted chain, so this is a walk into the message. Only a binding can
    // be the root of one: the sample says what `msg` carries and nothing says
    // what a variable holds, and a list of the message's fields under an
    // unrelated name would be worse than no list.
    let Some(root) = typed.chain.first() else {
        return out;
    };
    if !kayak_core::script::builtin(root).is_some_and(|builtin| {
        builtin.kind == kayak_core::script::BuiltinKind::Binding && builtin.in_scope(scope)
    }) {
        return out;
    }
    // `batch` is an array, so `batch.` is rhai's own methods rather than the
    // message's fields — nothing here knows those, and offering the fields of
    // a message under it would be wrong rather than incomplete.
    if root == "batch" {
        return out;
    }

    let base = typed.chain[1..].join(".");
    let under = if base.is_empty() {
        String::new()
    } else {
        format!("{base}.")
    };
    for (path, types) in fields {
        let Some(rest) = path.strip_prefix(under.as_str()) else {
            continue;
        };
        // Immediate children only: `sensor.id` is not a completion for `msg.`,
        // it is one for `msg.sensor.`.
        if rest.is_empty() || rest.contains('.') || !rest.starts_with(&typed.prefix) {
            continue;
        }
        out.push(Completion {
            label: rest.to_string(),
            detail: types.clone(),
            insert: rest.to_string(),
            caret: rest.chars().count(),
            kind: CompletionKind::Field,
        });
    }
    out
}

/// Take a completion: the source with the half-typed word replaced, and where
/// the caret goes in it.
///
/// Pure because it is the one piece of the popup that can be wrong in a way
/// nobody notices until they have lost a line of code — it rewrites the whole
/// box, and the editor then pushes that straight into the `<textarea>`.
#[must_use]
pub fn apply_completion(source: &str, typed: &Typed, completion: &Completion) -> (String, usize) {
    let chars: Vec<char> = source.chars().collect();
    let start = typed.start.min(chars.len());
    let end = (start + typed.prefix.chars().count()).min(chars.len());
    let mut out: String = chars[..start].iter().collect();
    out.push_str(&completion.insert);
    out.extend(chars[end..].iter());
    (out, start + completion.caret)
}

/// Where a point inside the code area falls, given the size of one character
/// cell.
///
/// The editor is monospaced, so this is arithmetic rather than a hit test —
/// which is what makes a hint under the pointer affordable at all: the
/// highlighted spans sit *behind* a transparent textarea, so nothing can be
/// hovered directly and the alternative is a DOM node per token with the
/// pointer events routed around the caret.
#[must_use]
pub fn caret_from_pixels(x: f64, y: f64, char_width: f64, line_height: f64) -> Option<Caret> {
    if char_width <= 0.0 || line_height <= 0.0 || x < 0.0 || y < 0.0 {
        return None;
    }
    Some(Caret {
        line: (y / line_height) as usize,
        column: (x / char_width) as usize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::script::ScriptScope;

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

    // ── what is under the caret ─────────────────────────────────────────────

    fn fields() -> Vec<(String, String)> {
        [
            ("temperature", "float"),
            ("sensor", "object"),
            ("sensor.id", "string"),
            ("sensor.site", "string"),
            ("readings", "array"),
        ]
        .into_iter()
        .map(|(path, types)| (path.to_string(), types.to_string()))
        .collect()
    }

    fn complete(source: &str, scope: ScriptScope) -> Vec<String> {
        let typed = typing_at(source, source.chars().count());
        completions(&typed, scope, &fields())
            .into_iter()
            .map(|c| c.label)
            .collect()
    }

    #[test]
    fn a_caret_offset_becomes_a_line_and_a_column() {
        let source = "emit(msg);\nlet x = 1;";
        assert_eq!(caret_at(source, 0), Caret { line: 0, column: 0 });
        assert_eq!(caret_at(source, 4), Caret { line: 0, column: 4 });
        // the offset of the newline itself is still the end of the first line
        assert_eq!(caret_at(source, 10), Caret { line: 0, column: 10 });
        assert_eq!(caret_at(source, 11), Caret { line: 1, column: 0 });
        // past the end clamps rather than panicking, which is what a stale
        // selection from a textarea that has just been rewritten looks like
        assert_eq!(caret_at(source, 999).line, 1);
    }

    #[test]
    fn typing_a_bare_word_has_no_chain() {
        assert_eq!(
            typing_at("emi", 3),
            Typed { chain: Vec::new(), prefix: "emi".to_string(), start: 0 }
        );
    }

    /// The moment the list is most useful is the one where there is nothing to
    /// filter by, so an empty prefix after a dot is a state rather than a
    /// no-op.
    #[test]
    fn a_dot_with_nothing_after_it_is_a_chain_and_an_empty_prefix() {
        let typed = typing_at("emit(msg.", 9);
        assert_eq!(typed.chain, vec!["msg".to_string()]);
        assert_eq!(typed.prefix, "");
        assert_eq!(typed.start, 9);
    }

    #[test]
    fn a_deeper_walk_keeps_the_segments_in_order() {
        let typed = typing_at("let x = msg.sensor.i", 20);
        assert_eq!(typed.chain, vec!["msg".to_string(), "sensor".to_string()]);
        assert_eq!(typed.prefix, "i");
    }

    /// A chain is only followed through words. `f().` and `"text".` are dots
    /// after something this has nothing to say about, and guessing there would
    /// offer the message's fields on an expression that is not the message.
    #[test]
    fn a_dot_after_something_that_is_not_a_word_ends_the_walk() {
        assert!(typing_at("f().", 4).chain.is_empty());
        assert!(typing_at("\"text\".", 7).chain.is_empty());
    }

    #[test]
    fn the_builtins_are_offered_and_filtered_by_what_is_typed() {
        let all = complete("", ScriptScope::Message);
        assert!(all.iter().any(|label| label == "emit(value)"), "{all:?}");
        assert!(all.iter().any(|label| label == "msg"), "{all:?}");

        let filtered = complete("emi", ScriptScope::Message);
        assert_eq!(
            filtered.iter().filter(|label| label.starts_with("emit")).count(),
            1,
            "{filtered:?}"
        );
        assert!(!filtered.iter().any(|label| label == "warn(text)"), "{filtered:?}");
    }

    /// A name that belongs to the other scope is not offered, because it is
    /// not there: completing `batch` into a per-message script writes a call
    /// that fails at runtime, which is the editor being actively wrong rather
    /// than merely unhelpful.
    #[test]
    fn a_binding_from_the_other_scope_is_not_offered() {
        let message = complete("", ScriptScope::Message);
        assert!(message.iter().any(|label| label == "msg"));
        assert!(!message.iter().any(|label| label == "batch"));

        let batch = complete("", ScriptScope::Batch);
        assert!(batch.iter().any(|label| label == "batch"));
        assert!(!batch.iter().any(|label| label == "msg"));
    }

    /// It is still *coloured* as a kayak name, though — see [`is_host`].
    #[test]
    fn an_out_of_scope_binding_is_still_a_host_name() {
        let line = &kinds("emit(batch);")[0];
        let kind = line.iter().find(|(_, text)| text == "batch").map(|(kind, _)| *kind);
        assert_eq!(kind, Some(Kind::Host));
    }

    #[test]
    fn a_walk_into_the_message_offers_the_sampled_fields() {
        let top = complete("emit(msg.", ScriptScope::Message);
        assert!(top.contains(&"temperature".to_string()), "{top:?}");
        assert!(top.contains(&"sensor".to_string()), "{top:?}");
        // a child is a completion for its parent, not for the root
        assert!(!top.contains(&"sensor.id".to_string()), "{top:?}");
        assert!(!top.contains(&"id".to_string()), "{top:?}");

        let nested = complete("emit(msg.sensor.", ScriptScope::Message);
        assert_eq!(nested, vec!["id".to_string(), "site".to_string()]);

        let filtered = complete("emit(msg.sensor.i", ScriptScope::Message);
        assert_eq!(filtered, vec!["id".to_string()]);
    }

    /// Nothing knows what a variable holds, so a dotted walk under one offers
    /// nothing at all — the message's fields under an unrelated name would be
    /// a confident wrong answer.
    #[test]
    fn a_walk_under_a_variable_offers_nothing() {
        assert!(complete("let row = 1; row.", ScriptScope::Message).is_empty());
    }

    /// `batch` is an array of messages, so its members are rhai's array
    /// methods rather than a message's fields.
    #[test]
    fn a_walk_under_batch_offers_no_message_fields() {
        assert!(complete("emit(batch.", ScriptScope::Batch).is_empty());
    }

    /// With nothing sampled the field half is simply empty, which is the state
    /// every form starts in.
    #[test]
    fn completions_work_with_no_sample_at_all() {
        let typed = typing_at("em", 2);
        let out = completions(&typed, ScriptScope::Message, &[]);
        assert!(out.iter().any(|c| c.label == "emit(value)"), "{out:?}");

        let typed = typing_at("msg.", 4);
        assert!(completions(&typed, ScriptScope::Message, &[]).is_empty());
    }

    #[test]
    fn a_word_is_found_from_anywhere_inside_it() {
        let source = "let total = 0;\nemit(msg);";
        for column in 4..=8 {
            assert_eq!(
                word_at(source, Caret { line: 0, column }),
                Some("total".to_string()),
                "column {column}"
            );
        }
        assert_eq!(word_at(source, Caret { line: 1, column: 0 }), Some("emit".to_string()));
        // on the bracket, not on a word
        assert_eq!(word_at(source, Caret { line: 1, column: 4 }), None);
        // past the end of the line, and past the end of the source
        assert_eq!(word_at(source, Caret { line: 1, column: 99 }), None);
        assert_eq!(word_at(source, Caret { line: 9, column: 0 }), None);
    }

    /// Accepting a completion rewrites the whole box, so what it does to the
    /// text *around* the word matters as much as the word itself.
    #[test]
    fn accepting_a_completion_replaces_only_the_word_being_typed() {
        let source = "let x = emi\nemit(x);";
        let typed = typing_at(source, 11);
        let completion = completions(&typed, ScriptScope::Message, &[])
            .into_iter()
            .find(|c| c.label == "emit(value)")
            .expect("emit should be offered");

        let (out, caret) = apply_completion(source, &typed, &completion);
        assert_eq!(out, "let x = emit()\nemit(x);");
        // inside the brackets, which is where the argument goes
        assert_eq!(out.chars().take(caret).collect::<String>(), "let x = emit(");
    }

    #[test]
    fn accepting_a_field_completion_after_a_dot_keeps_the_dot() {
        let source = "emit(msg.te);";
        let typed = typing_at(source, 11);
        let fields = fields();
        let completion = completions(&typed, ScriptScope::Message, &fields)
            .into_iter()
            .next()
            .expect("a field should be offered");
        let (out, caret) = apply_completion(source, &typed, &completion);
        assert_eq!(out, "emit(msg.temperature);");
        assert_eq!(caret, "emit(msg.temperature".chars().count());
    }

    /// The multi-byte case, which is the one that would panic rather than
    /// merely look wrong: the surgery works in `char`s throughout.
    #[test]
    fn accepting_a_completion_next_to_a_multi_byte_character() {
        let source = "let s = \"ü🎉\"; em";
        let typed = typing_at(source, source.chars().count());
        let completion = completions(&typed, ScriptScope::Message, &[])
            .into_iter()
            .find(|c| c.label == "emit(value)")
            .expect("emit");
        let (out, _) = apply_completion(source, &typed, &completion);
        assert_eq!(out, "let s = \"ü🎉\"; emit()");
    }

    #[test]
    fn a_point_in_the_code_area_becomes_a_cell() {
        assert_eq!(
            caret_from_pixels(21.0, 40.0, 7.0, 18.0),
            Some(Caret { line: 2, column: 3 })
        );
        // inside the first cell, not past it
        assert_eq!(
            caret_from_pixels(0.0, 0.0, 7.0, 18.0),
            Some(Caret { line: 0, column: 0 })
        );
        // a pointer in the padding, and a box that has not been measured yet
        assert_eq!(caret_from_pixels(-2.0, 5.0, 7.0, 18.0), None);
        assert_eq!(caret_from_pixels(10.0, 10.0, 0.0, 0.0), None);
    }
}
