//! A pipeline's config as text, for a card's source view.
//!
//! The text comes from the server (`GET /api/pipelines/{id}/config`), which
//! renders it with the same code a save writes the file with — so this module
//! never *produces* a format, it only reads what arrived: colours it, and finds
//! where a component is in it. Pure, like `pretty.rs`, whose spans and colour
//! classes it shares, so a config and a log payload are coloured as one
//! language.
//!
//! JSON goes through [`pretty::render`]. YAML has a highlighter of its own
//! here, and it is deliberately a line scanner rather than a parser: it only
//! has to read what `serde_norway` writes — block mappings and sequences, plain
//! or quoted scalars, `|-` blocks for multi-line text such as an inline script
//! — and a line it misreads is a wrong colour, not a wrong config.

use std::ops::Range;

use kayak_core::ConfigFormat;

use crate::pretty::{self, Kind, Span};

/// The text as coloured lines, one `Vec<Span>` per line, the indent included as
/// a leading span — so concatenating a line's spans gives the line back.
#[must_use]
pub fn highlight(text: &str, format: ConfigFormat) -> Vec<Vec<Span>> {
    match format {
        ConfigFormat::Yaml => yaml(text),
        ConfigFormat::Json => match pretty::render(text) {
            pretty::Rendered::Json(lines) => lines
                .into_iter()
                .map(|line| {
                    let mut spans = vec![span(Kind::Punct, line.indent())];
                    spans.extend(line.spans);
                    spans
                })
                .collect(),
            // the server rendered it, so this is not expected; shown as it
            // stands if it happens
            pretty::Rendered::Plain(text) => text.lines().map(|l| vec![span(Kind::Str, l)]).collect(),
        },
    }
}

/// The lines (as indices into [`highlight`]'s output) holding item `index` of
/// the top-level list `key` — `transforms`, `inputs`, `outputs` — or `None` if
/// there is no such item.
///
/// This is what lets a section's heading open the source at that component:
/// the lines are found in the text rather than worked out from the config,
/// because the text is the server's and only it knows how it was spelled.
#[must_use]
pub fn item_lines(
    lines: &[Vec<Span>],
    format: ConfigFormat,
    key: &str,
    index: usize,
) -> Option<Range<usize>> {
    let texts: Vec<String> = lines
        .iter()
        .map(|spans| spans.iter().map(|s| s.text.as_str()).collect())
        .collect();
    let items = match format {
        ConfigFormat::Yaml => yaml_items(&texts, key),
        ConfigFormat::Json => json_items(&texts, key),
    };
    items.into_iter().nth(index)
}

fn span(kind: Kind, text: impl Into<String>) -> Span {
    Span {
        kind,
        text: text.into(),
    }
}

fn indent_of(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// The items of a block sequence under a top-level `key:`. `serde_norway`
/// writes them at the key's own indentation (`- type: …`), and an item runs
/// until the next `- ` at that indentation or the first line that is less
/// indented than its contents.
fn yaml_items(lines: &[String], key: &str) -> Vec<Range<usize>> {
    let header = format!("{key}:");
    let Some(start) = lines.iter().position(|l| l.trim_end() == header) else {
        return Vec::new();
    };
    let mut items = Vec::new();
    let mut current: Option<usize> = None;
    let mut seq_indent: Option<usize> = None;
    let mut end = start + 1;
    for (i, line) in lines.iter().enumerate().skip(start + 1) {
        let indent = indent_of(line);
        let marker = line[indent..].starts_with("- ") || line[indent..] == *"-";
        let seq = *seq_indent.get_or_insert(indent);
        if marker && indent == seq {
            if let Some(from) = current.replace(i) {
                items.push(from..i);
            }
        } else if line.trim().is_empty() || indent > seq {
            // inside the current item — including a block scalar's blank lines
        } else {
            break;
        }
        end = i + 1;
    }
    if let Some(from) = current {
        items.push(from..end);
    }
    items
}

/// The items of an array under a top-level `"key": [`, as pretty-printed: each
/// item opens and closes at one level deeper than the key.
fn json_items(lines: &[String], key: &str) -> Vec<Range<usize>> {
    let header = format!("\"{key}\": [");
    let Some(start) = lines.iter().position(|l| l.trim_start() == header) else {
        return Vec::new();
    };
    let item_indent = indent_of(&lines[start]) + 2;
    let mut items = Vec::new();
    let mut open: Option<usize> = None;
    for (i, line) in lines.iter().enumerate().skip(start + 1) {
        let indent = indent_of(line);
        if indent < item_indent {
            break;
        }
        if indent != item_indent {
            continue;
        }
        let rest = &line[indent..];
        if rest.starts_with('}') || rest.starts_with(']') {
            if let Some(from) = open.take() {
                items.push(from..i + 1);
            }
        } else if rest.ends_with('{') || rest.ends_with('[') {
            open = Some(i);
        } else {
            // a scalar item, or a container written on one line (`{}`)
            items.push(i..i + 1);
        }
    }
    items
}

fn yaml(text: &str) -> Vec<Vec<Span>> {
    // the column of the key that opened a `|` / `>` block, while inside one
    let mut block: Option<usize> = None;
    text.lines()
        .map(|line| {
            let indent = indent_of(line);
            if let Some(column) = block {
                if line.trim().is_empty() || indent > column {
                    return vec![span(Kind::Punct, &line[..indent]), span(Kind::Str, &line[indent..])];
                }
                block = None;
            }
            let mut spans = vec![span(Kind::Punct, &line[..indent])];
            let mut rest = &line[indent..];
            let mut column = indent;
            while let Some(after) = rest.strip_prefix("- ") {
                spans.push(span(Kind::Punct, "- "));
                rest = after;
                column += 2;
            }
            if rest == "-" {
                spans.push(span(Kind::Punct, "-"));
                return spans;
            }
            match split_key(rest) {
                Some((key, value)) => {
                    spans.push(span(Kind::Key, key));
                    spans.push(span(Kind::Punct, ":"));
                    if !value.is_empty() {
                        spans.push(span(Kind::Punct, " "));
                        if value.starts_with('|') || value.starts_with('>') {
                            block = Some(column);
                        }
                        spans.push(scalar(value));
                    }
                }
                None => {
                    if rest.starts_with('|') || rest.starts_with('>') {
                        block = Some(column.saturating_sub(2));
                    }
                    spans.push(scalar(rest));
                }
            }
            spans
        })
        .collect()
}

/// `key: value` or `key:` into the key and what follows the colon. `None` for
/// a line that is a bare value (a sequence item that is a scalar).
fn split_key(rest: &str) -> Option<(&str, &str)> {
    // a quoted key: up to its closing quote, which must be followed by a colon
    if let Some(quote) = rest.chars().next().filter(|c| *c == '"' || *c == '\'') {
        let close = rest[1..].find(quote)? + 1;
        let after = &rest[close + 1..];
        let value = after.strip_prefix(':')?;
        return (value.is_empty() || value.starts_with(' '))
            .then(|| (&rest[..=close], value.trim_start()));
    }
    if let Some(key) = rest.strip_suffix(':') {
        return (!key.contains(": ")).then_some((key, ""));
    }
    rest.split_once(": ")
}

fn scalar(value: &str) -> Span {
    let kind = if value.starts_with('|') || value.starts_with('>') || value == "[]" || value == "{}"
    {
        Kind::Punct
    } else if value.starts_with('"') || value.starts_with('\'') {
        Kind::Str
    } else if matches!(value, "true" | "false" | "null" | "~") {
        Kind::Literal
    } else if value
        .trim_start_matches('-')
        .starts_with(|c: char| c.is_ascii_digit())
        && value.parse::<f64>().is_ok()
    {
        Kind::Num
    } else {
        Kind::Str
    };
    span(kind, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What `serde_norway` writes for a pipeline, more or less verbatim: the
    /// shapes the highlighter has to read.
    const YAML: &str = "\
id: trend
inputs:
- type: pipeline
  upstream: heartbeat
transforms:
- type: smooth
  field: value
  method:
    type: ewma
    half_life: 5.0
- type: script
  source:
    type: inline
    code: |-
      let x = msg.value;

      emit(x);
  scope: message
- type: buffer
  size: 2
outputs:
- type: stdout
state: null";

    fn text(line: &[Span]) -> String {
        line.iter().map(|s| s.text.as_str()).collect()
    }

    fn kinds(line: &[Span]) -> Vec<(Kind, &str)> {
        line.iter()
            .filter(|s| !s.text.trim().is_empty())
            .map(|s| (s.kind, s.text.as_str()))
            .collect()
    }

    #[test]
    fn a_highlighted_line_reads_back_as_the_line() {
        let lines = highlight(YAML, ConfigFormat::Yaml);
        let back: Vec<String> = lines.iter().map(|l| text(l)).collect();
        assert_eq!(back, YAML.lines().collect::<Vec<_>>());
    }

    #[test]
    fn yaml_keys_and_values_are_told_apart() {
        let lines = highlight(YAML, ConfigFormat::Yaml);
        assert_eq!(
            kinds(&lines[0]),
            [(Kind::Key, "id"), (Kind::Punct, ":"), (Kind::Str, "trend")]
        );
        assert_eq!(
            kinds(&lines[2]),
            [
                (Kind::Punct, "- "),
                (Kind::Key, "type"),
                (Kind::Punct, ":"),
                (Kind::Str, "pipeline")
            ]
        );
        assert_eq!(kinds(&lines[9])[2], (Kind::Num, "5.0"));
        assert_eq!(kinds(&lines[22])[2], (Kind::Literal, "null"));
    }

    /// An inline script is a `|-` block, and every line of it is the string —
    /// including a line that looks like `key: value` and a blank one.
    #[test]
    fn a_block_scalar_is_one_string_until_it_ends() {
        let lines = highlight(YAML, ConfigFormat::Yaml);
        assert_eq!(kinds(&lines[14]), [(Kind::Str, "let x = msg.value;")]);
        assert_eq!(kinds(&lines[16]), [(Kind::Str, "emit(x);")]);
        // and the block ends where the indentation does
        assert_eq!(kinds(&lines[17])[0], (Kind::Key, "scope"));
    }

    #[test]
    fn a_yaml_items_lines_are_found_by_position() {
        let lines = highlight(YAML, ConfigFormat::Yaml);
        let at = |key, i| item_lines(&lines, ConfigFormat::Yaml, key, i);
        assert_eq!(at("inputs", 0), Some(2..4));
        assert_eq!(at("transforms", 0), Some(5..10));
        // the script's blank line is inside it, not the end of it
        assert_eq!(at("transforms", 1), Some(10..18));
        assert_eq!(at("transforms", 2), Some(18..20));
        assert_eq!(at("outputs", 0), Some(21..22));
        assert_eq!(at("transforms", 3), None);
        assert_eq!(at("nothing", 0), None);
    }

    #[test]
    fn an_empty_yaml_list_has_no_items() {
        let yaml = "id: a\ntransforms: []\noutputs:\n- type: stdout";
        let lines = highlight(yaml, ConfigFormat::Yaml);
        assert_eq!(item_lines(&lines, ConfigFormat::Yaml, "transforms", 0), None);
        assert_eq!(item_lines(&lines, ConfigFormat::Yaml, "outputs", 0), Some(3..4));
    }

    #[test]
    fn a_json_items_lines_are_found_by_position() -> anyhow::Result<()> {
        let config = serde_json::json!({
            "id": "trend",
            "inputs": [{ "type": "pipeline", "upstream": "heartbeat" }],
            "transforms": [
                { "type": "smooth", "method": { "type": "ewma" } },
                { "type": "buffer", "size": 2 }
            ],
            "outputs": [{ "type": "stdout" }]
        });
        let rendered = serde_json::to_string_pretty(&config)?;
        let lines = highlight(&rendered, ConfigFormat::Json);
        let at = |key, i| {
            item_lines(&lines, ConfigFormat::Json, key, i)
                .map(|range| lines[range].iter().map(|l| text(l)).collect::<Vec<_>>().join("\n"))
        };
        let second = at("transforms", 1).unwrap_or_default();
        assert!(second.contains("\"buffer\""), "{second}");
        assert!(!second.contains("smooth"), "{second}");
        let first = at("transforms", 0).unwrap_or_default();
        assert!(first.contains("ewma") && first.trim_end().ends_with("},"), "{first}");
        assert!(at("outputs", 0).unwrap_or_default().contains("stdout"));
        assert_eq!(at("transforms", 2), None);
        Ok(())
    }
}
