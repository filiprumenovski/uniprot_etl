use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Loads a FASTA file into a map of accession -> sequence.
///
/// Header parsing:
/// - If header is like `>sp|P04637-2|...`, uses `P04637-2`.
/// - Otherwise uses the first token after `>` up to whitespace.
pub fn load_fasta_map(path: &Path) -> Result<HashMap<String, String>> {
    let file =
        File::open(path).with_context(|| format!("Failed to open FASTA: {}", path.display()))?;
    let reader = BufReader::new(file);

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
