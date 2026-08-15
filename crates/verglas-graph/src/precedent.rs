//! Hand-rolled BM25 lexical ranking over decision objectives (#148).
//!
//! No embedding model and no external ranking crate: this is the entire
//! lexical-scoring path for precedent search. Tokenization is
//! lowercase-alphanumeric splitting; scoring is textbook Robertson/Sparck
//! Jones BM25 with the standard `k1 = 1.2`, `b = 0.75` constants. Everything
//! here is a pure function of its inputs, so identical documents and query
//! text always produce identical scores — the caller (the semantic REST-JSON
//! adapter) is what turns that into a deterministic wire response.

/// BM25 term-frequency saturation parameter.
pub const BM25_K1: f64 = 1.2;
/// BM25 document-length normalization parameter.
pub const BM25_B: f64 = 0.75;

/// Splits `text` into lowercase alphanumeric tokens. Any run of non-ASCII-
/// alphanumeric characters (punctuation, whitespace, symbols) is a separator;
/// empty runs are dropped. This is the one tokenizer both indexing and
/// querying use, so a document and a query are always comparable.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Scores every document in `documents` against `query_tokens` with BM25 and
/// returns one score per document, aligned by index (never negative).
///
/// Uses the `+1` idf floor (`ln(1 + (N - n + 0.5) / (n + 0.5))`) so a term
/// that appears in most documents contributes a small positive weight instead
/// of a negative one. Duplicate query tokens are deduplicated first: a
/// repeated word in the query does not multiply its own contribution, since
/// callers pass free-text search phrases, not weighted term vectors. An empty
/// corpus or an empty (post-tokenization) query returns all-zero scores
/// rather than dividing by zero.
pub fn bm25_scores(documents: &[Vec<String>], query_tokens: &[String]) -> Vec<f64> {
    let doc_count = documents.len();
    if doc_count == 0 {
        return Vec::new();
    }
    let mut query_terms: Vec<&str> = Vec::new();
    for token in query_tokens {
        if !query_terms.contains(&token.as_str()) {
            query_terms.push(token.as_str());
        }
    }
    if query_terms.is_empty() {
        return vec![0.0; doc_count];
    }

    let total_len: f64 = documents.iter().map(|doc| doc.len() as f64).sum();
    // Every document is empty: there is nothing to rank against, and dividing
    // by an average of zero would produce NaN. Fall back to 1.0 so the length
    // normalization term degenerates to a no-op rather than poisoning scores.
    let avg_doc_len = if total_len == 0.0 {
        1.0
    } else {
        total_len / doc_count as f64
    };

    let idfs: Vec<f64> = query_terms
        .iter()
        .map(|term| {
            let doc_freq = documents
                .iter()
                .filter(|doc| doc.iter().any(|token| token == term))
                .count();
            idf(doc_count, doc_freq)
        })
        .collect();

    documents
        .iter()
        .map(|doc| {
            let doc_len = doc.len() as f64;
            query_terms
                .iter()
                .zip(&idfs)
                .map(|(term, term_idf)| {
                    let term_freq =
                        doc.iter().filter(|token| token.as_str() == *term).count() as f64;
                    if term_freq == 0.0 {
                        return 0.0;
                    }
                    let numerator = term_freq * (BM25_K1 + 1.0);
                    let denominator =
                        term_freq + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_len / avg_doc_len);
                    term_idf * numerator / denominator
                })
                .sum()
        })
        .collect()
}

/// The `+1` idf variant: `ln(1 + (N - n + 0.5) / (n + 0.5))`, always positive.
fn idf(doc_count: usize, doc_freq: usize) -> f64 {
    let n = doc_count as f64;
    let df = doc_freq as f64;
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_lowercases_and_splits_on_non_alphanumeric() {
        assert_eq!(
            tokenize("Roll-back the Cache, Eviction Policy!"),
            vec!["roll", "back", "the", "cache", "eviction", "policy"]
        );
        assert_eq!(tokenize("  "), Vec::<String>::new());
        assert_eq!(tokenize("Ω vector Ω"), vec!["vector"]);
    }

    #[test]
    fn a_document_with_more_query_term_occurrences_scores_higher() {
        let documents = vec![
            tokenize("vector search vector search retrieval"),
            tokenize("vector search retrieval"),
            tokenize("cache eviction policy"),
        ];
        let query = tokenize("vector search");
        let scores = bm25_scores(&documents, &query);
        assert!(
            scores[0] > scores[1],
            "more occurrences should score higher: {scores:?}"
        );
        assert!(
            scores[1] > scores[2],
            "any match should outscore no match: {scores:?}"
        );
        assert_eq!(
            scores[2], 0.0,
            "no query terms present must score exactly 0: {scores:?}"
        );
    }

    #[test]
    fn identical_inputs_always_produce_identical_scores() {
        let documents = vec![
            tokenize("adopt vector search for retrieval"),
            tokenize("roll back the cache eviction policy"),
        ];
        let query = tokenize("vector search retrieval");
        let first = bm25_scores(&documents, &query);
        let second = bm25_scores(&documents, &query);
        assert_eq!(first, second);
    }

    #[test]
    fn duplicate_query_terms_do_not_multiply_their_contribution() {
        let documents = vec![tokenize("vector search retrieval")];
        let single = bm25_scores(&documents, &tokenize("vector"));
        let repeated = bm25_scores(&documents, &tokenize("vector vector vector"));
        assert_eq!(single, repeated);
    }

    #[test]
    fn empty_corpus_and_empty_query_never_divide_by_zero() {
        assert_eq!(bm25_scores(&[], &tokenize("anything")), Vec::<f64>::new());
        let documents = vec![tokenize("some objective text")];
        assert_eq!(bm25_scores(&documents, &tokenize("")), vec![0.0]);
    }
}
