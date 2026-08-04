//! Distance metrics. Cosine and L2 are the v0 pair (issue #91: "cosine + L2
//! v0; metric recorded in blob metadata — never inferred"). The metric is
//! persisted in the blob and drives both index construction and query scoring,
//! so an index built under one metric is always searched under the same one.

use serde::{Deserialize, Serialize};

/// A distance metric over embedding vectors. Both are framed so that *smaller is
/// closer*, which is the ordering every Vamana routine (GreedySearch,
/// RobustPrune) relies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Metric {
    /// Squared Euclidean distance. Vectors are stored as-is. The public distance
    /// is the true Euclidean norm (the square root); internal comparisons use
    /// the square, which is monotonic and avoids the `sqrt` on the hot path.
    L2,
    /// Cosine distance, `1 - cos(a, b)`. Vectors are L2-normalized at insert
    /// time, so the cosine similarity is a plain dot product and the distance is
    /// `1 - dot`.
    Cosine,
}

impl Metric {
    /// The wire/label name, matching the JSON `metric` field on the declare API.
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::L2 => "l2",
            Metric::Cosine => "cosine",
        }
    }

    /// Parses a metric label; `None` for anything unrecognized.
    pub fn parse(s: &str) -> Option<Metric> {
        match s.to_ascii_lowercase().as_str() {
            "l2" | "euclidean" => Some(Metric::L2),
            "cosine" | "cos" => Some(Metric::Cosine),
            _ => None,
        }
    }

    /// Prepares a raw vector for storage under this metric. L2 stores it
    /// verbatim; Cosine returns an L2-normalized copy (a zero vector is left
    /// as-is — its cosine distance to everything is then `1`).
    pub fn prepare(self, v: &[f32]) -> Vec<f32> {
        match self {
            Metric::L2 => v.to_vec(),
            Metric::Cosine => normalize(v),
        }
    }

    /// The order-comparable distance between two *prepared* vectors: squared L2,
    /// or `1 - dot` for (normalized) cosine. Smaller is closer. This is the
    /// value every internal candidate comparison uses.
    #[inline]
    pub fn compare_distance(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => {
                let mut acc = 0.0f32;
                for i in 0..a.len() {
                    let d = a[i] - b[i];
                    acc += d * d;
                }
                acc
            }
            Metric::Cosine => {
                let mut dot = 0.0f32;
                for i in 0..a.len() {
                    dot += a[i] * b[i];
                }
                1.0 - dot
            }
        }
    }

    /// The user-facing distance between two prepared vectors: the true Euclidean
    /// norm for L2 (square root of [`Metric::compare_distance`]), or the same
    /// `1 - dot` for cosine. Returned in query results.
    #[inline]
    pub fn report_distance(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => self.compare_distance(a, b).max(0.0).sqrt(),
            Metric::Cosine => self.compare_distance(a, b),
        }
    }
}

/// Returns an L2-normalized copy of `v`; a zero (or near-zero) vector is
/// returned unchanged.
pub fn normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm <= f32::MIN_POSITIVE {
        return v.to_vec();
    }
    v.iter().map(|x| x / norm).collect()
}
