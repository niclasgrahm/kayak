//! Writes the doc site's generated reference. `just docs` runs it.
//!
//! Takes the site root as its one argument (default `website`), and writes
//! every file [`kayak_docsgen::files`] describes. Files are written whole
//! rather than patched, and a file that already holds what it should is left
//! alone — so the mtimes a dev server watches only move when something has
//! actually changed.

use std::path::{Path, PathBuf};
use std::{fs, process::ExitCode};

fn main() -> ExitCode {
    let root: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "website".to_string())
        .into();

    match write_all(&root) {
        Ok(written) => {
            println!("{written} file(s) written under {}", root.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("docsgen: {error}");
            ExitCode::FAILURE
        }
    }
}

fn write_all(root: &Path) -> std::io::Result<usize> {
    let mut written = 0;
    for file in kayak_docsgen::files() {
        let path = root.join(&file.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if fs::read_to_string(&path).is_ok_and(|existing| existing == file.contents) {
            continue;
        }
        fs::write(&path, &file.contents)?;
        written += 1;
    }
    Ok(written)
}
