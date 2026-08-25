//! Glob-lite matching: `*` matches any run of characters, everything else
//! matches literally. One shared implementation so the catalog table-name
//! filter and the backend bucket-set gate (`verglas-backend`, #235) match
//! patterns identically — a bucket pattern and a table pattern must never
//! disagree on what `*` means.

/// Whether `pattern` matches `text`, with `*` matching any run of characters
/// (the empty run included) and every other character matching literally.
///
/// Iterative two-pointer scan with backtracking to the last `*` — no recursion,
/// no allocation beyond the char buffers, so a pathological pattern cannot blow
/// the stack.
pub fn matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    // (pattern pos after the last `*`, text pos that `*` has consumed to).
    let mut backtrack: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() && pattern[p] == '*' {
            backtrack = Some((p + 1, t));
            p += 1;
        } else if p < pattern.len() && pattern[p] == text[t] {
            p += 1;
            t += 1;
        } else if let Some((bp, bt)) = backtrack {
            // Let the last `*` swallow one more character and retry.
            backtrack = Some((bp, bt + 1));
            p = bp;
            t = bt + 1;
        } else {
            return false;
        }
    }
    // Only trailing `*`s may remain unconsumed.
    pattern[p..].iter().all(|c| *c == '*')
}

#[cfg(test)]
mod tests {
    use super::matches;

    /// A literal pattern matches only the exact string.
    #[test]
    fn literal_matches_exactly() {
        assert!(matches("bucket", "bucket"));
        assert!(!matches("bucket", "bucket-2"));
        assert!(!matches("bucket", "buck"));
    }

    /// A trailing `*` matches any suffix, including the empty one; the S3 Tables
    /// `*--table-s3` case is a leading `*`.
    #[test]
    fn star_matches_any_run() {
        assert!(matches("*--table-s3", "abc--table-s3"));
        assert!(matches("*--table-s3", "909ef3f6-a7df-4967--table-s3"));
        assert!(matches("*--table-s3", "--table-s3")); // empty run before the suffix
        assert!(!matches("*--table-s3", "abc--table-s3x"));
        assert!(!matches("*--table-s3", "plain-bucket"));
    }

    /// A `*` in the middle and multiple `*`s both work, and `*` spans any
    /// characters (hyphens and dots included).
    #[test]
    fn star_in_middle_and_multiple() {
        assert!(matches("a*z", "a-b.c-z"));
        assert!(matches("*table*", "my--table-s3"));
        assert!(matches("*", ""));
        assert!(matches("*", "anything"));
        assert!(!matches("a*z", "a-b-c"));
    }
}
