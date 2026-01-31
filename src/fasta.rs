use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use glob::glob;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Loads a FASTA file into a map of accession -> sequence.
///
/// Supports both plain `.fasta` and gzipped `.fasta.gz` files.
///
/// Header parsing:
/// - If header is like `>sp|P04637-2|...`, uses `P04637-2`.
/// - Otherwise uses the first token after `>` up to whitespace.
pub fn load_fasta_map(path: &Path) -> Result<HashMap<String, String>> {
    let file =
        File::open(path).with_context(|| format!("Failed to open FASTA: {}", path.display()))?;

    // Detect gzipped files by extension and wrap appropriately
    let reader: Box<dyn BufRead> = if path
        .extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("gz"))
    {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut map: HashMap<String, String> = HashMap::new();

    let mut current_key: Option<String> = None;
    let mut current_seq = String::new();

    for line in reader.lines() {
        let line = line?;
        if line.starts_with('>') {
            if let Some(key) = current_key.take() {
                if !current_seq.is_empty() {
                    map.insert(key, std::mem::take(&mut current_seq));
                } else {
                    map.insert(key, String::new());
                }
            }

            let header = line.trim_start_matches('>').trim();
            let key = parse_fasta_key(header);
            current_key = Some(key);
        } else {
            let part = line.trim();
            if !part.is_empty() {
                current_seq.push_str(part);
            }
        }
    }

    if let Some(key) = current_key.take() {
        map.insert(key, current_seq);
    }

    Ok(map)
}

/// Attempts to auto-detect a FASTA sidecar file in the given directory.
///
/// Search strategy (in order of priority):
/// 1. Files matching `*varsplic*.fasta` or `*varsplic*.fasta.gz`
/// 2. Files matching `*.fasta` or `*.fasta.gz` (fallback)
///
/// Returns the most specific match, or None if no candidates found.
///
/// # Arguments
/// * `input_dir` - Directory to search (or file, in which case parent directory is used)
///
/// # Example
/// ```ignore
/// let sidecar = detect_sidecar(Path::new("/data/uniprot/"))?;
/// if let Some(path) = sidecar {
///     println!("Found sidecar: {}", path.display());
/// }
/// ```
pub fn detect_sidecar(input_dir: &Path) -> Result<Option<PathBuf>> {
    // Normalize to directory (handle both dir and file inputs)
    let search_dir = if input_dir.is_file() {
        input_dir.parent().ok_or_else(|| {
            anyhow!(
                "Cannot determine parent directory of {}",
                input_dir.display()
            )
        })?
    } else if input_dir.is_dir() {
        input_dir
    } else {
        return Err(anyhow!("Path does not exist: {}", input_dir.display()));
    };

    // Priority 1: varsplic pattern (most specific)
    let varsplic_patterns = [
        format!("{}/*varsplic*.fasta.gz", search_dir.display()),
        format!("{}/*varsplic*.fasta", search_dir.display()),
    ];

    for pattern in &varsplic_patterns {
        let matches: Vec<PathBuf> = glob(pattern)
            .map_err(|e| anyhow!("Invalid glob pattern: {}", e))?
            .filter_map(Result::ok)
            .collect();

        if !matches.is_empty() {
            return Ok(disambiguate_candidates(matches, "varsplic"));
        }
    }

    // Priority 2: generic .fasta (fallback)
    let generic_patterns = [
        format!("{}/*.fasta.gz", search_dir.display()),
        format!("{}/*.fasta", search_dir.display()),
    ];

    for pattern in &generic_patterns {
        let matches: Vec<PathBuf> = glob(pattern)
            .map_err(|e| anyhow!("Invalid glob pattern: {}", e))?
            .filter_map(Result::ok)
            .collect();

        if !matches.is_empty() {
            return Ok(disambiguate_candidates(matches, "generic"));
        }
    }

    Ok(None)
}

/// Disambiguate multiple FASTA candidates using heuristics.
fn disambiguate_candidates(mut candidates: Vec<PathBuf>, category: &str) -> Option<PathBuf> {
    if candidates.is_empty() {
        return None;
    }

    // Single match - return immediately
    if candidates.len() == 1 {
        let selected = candidates.pop().unwrap();
        eprintln!(
            "[INFO] Auto-detected FASTA sidecar ({}): {}",
            category,
            selected.display()
        );
        return Some(selected);
    }

    // Multiple matches - apply heuristics:

    // 1. Prefer uncompressed over compressed (faster to read)
    let uncompressed: Vec<_> = candidates
        .iter()
        .filter(|p| !p.to_string_lossy().ends_with(".gz"))
        .cloned()
        .collect();

    if uncompressed.len() == 1 {
        eprintln!(
            "[INFO] Auto-detected FASTA sidecar ({}): {} (preferred uncompressed)",
            category,
            uncompressed[0].display()
        );
        return Some(uncompressed[0].clone());
    }

    // 2. Prefer standard UniProt naming
    let standard_uniprot: Vec<_> = candidates
        .iter()
        .filter(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            name.contains("uniprot_sprot_varsplic") || name.contains("uniprot_trembl_varsplic")
        })
        .cloned()
        .collect();

    if standard_uniprot.len() == 1 {
        eprintln!(
            "[INFO] Auto-detected FASTA sidecar ({}): {} (standard UniProt naming)",
            category,
            standard_uniprot[0].display()
        );
        return Some(standard_uniprot[0].clone());
    }

    // 3. Largest file (likely most complete)
    candidates.sort_by_key(|p| std::fs::metadata(p).ok().map(|m| m.len()).unwrap_or(0));
    candidates.reverse();

    let selected = candidates[0].clone();
    eprintln!(
        "[INFO] Auto-detected FASTA sidecar ({}): {} (largest among {} candidates)",
        category,
        selected.display(),
        candidates.len()
    );

    Some(selected)
}

fn parse_fasta_key(header: &str) -> String {
    // Prefer UniProt pipe format.
    // Examples: `sp|P04637-2|...`, `tr|Q9TEST-1|...`
    let first_token = header.split_whitespace().next().unwrap_or(header);
    let mut parts = first_token.split('|');
    let p0 = parts.next();
    let p1 = parts.next();
    let p2 = parts.next();

    match (p0, p1, p2) {
        (Some(_db), Some(acc), Some(_rest)) if !acc.is_empty() => acc.to_string(),
        _ => first_token.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_uniprot_pipe_header() {
        assert_eq!(parse_fasta_key("sp|P04637-2|TP53_HUMAN"), "P04637-2");
        assert_eq!(parse_fasta_key("tr|Q9TEST-1|SOME"), "Q9TEST-1");
    }

    #[test]
    fn parses_simple_header() {
        assert_eq!(parse_fasta_key("Q9TEST-1 some desc"), "Q9TEST-1");
    }
}
