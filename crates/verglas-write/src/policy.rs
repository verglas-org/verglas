//! Per-prefix write-back opt-in (#180).
//!
//! Write-back is off by default and enabled only for configured key prefixes.
//! A write to a key that matches no rule takes the ordinary write-through path
//! with no added work beyond this prefix check. The longest matching prefix
//! wins, so a table can override a broader bucket-wide rule.

use verglas_core::CacheKey;

/// One opt-in rule: a key prefix and the erasure geometry to use for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixRule {
    /// Keys whose `key` starts with this prefix are write-back eligible. An
    /// empty prefix matches every key in the pod.
    pub prefix: String,
    /// Reed-Solomon data fragments.
    pub k: usize,
    /// Reed-Solomon parity fragments.
    pub m: usize,
    /// Fragments that must be durable on distinct live nodes to ack.
    pub w: usize,
}

/// The set of opt-in rules, matched longest-prefix-first.
#[derive(Debug, Clone, Default)]
pub struct WritebackPolicy {
    /// Rules sorted by descending prefix length so the first match is longest.
    rules: Vec<PrefixRule>,
}

impl WritebackPolicy {
    /// Builds a policy from rules, ordering them longest-prefix-first.
    pub fn new(mut rules: Vec<PrefixRule>) -> Self {
        rules.sort_by_key(|r| std::cmp::Reverse(r.prefix.len()));
        Self { rules }
    }

    /// A policy with no rules: write-back is disabled for every key.
    pub fn disabled() -> Self {
        Self { rules: Vec::new() }
    }

    /// True when no prefix opts in, so the write path never buffers or encodes.
    pub fn is_disabled(&self) -> bool {
        self.rules.is_empty()
    }

    /// Returns the geometry `(k, m, w)` for `key`, or `None` when no prefix
    /// opts it in. The longest matching prefix wins.
    pub fn geometry_for(&self, key: &CacheKey) -> Option<(usize, usize, usize)> {
        self.rules
            .iter()
            .find(|rule| key.key.starts_with(&rule.prefix))
            .map(|rule| (rule.k, rule.m, rule.w))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key for `(bucket, key)`.
    fn ck(key: &str) -> CacheKey {
        CacheKey {
            bucket: "b".to_owned(),
            key: key.to_owned(),
        }
    }

    /// Longest matching prefix wins; unmatched keys opt out.
    #[test]
    fn longest_prefix_wins() {
        let policy = WritebackPolicy::new(vec![
            PrefixRule {
                prefix: "data/".to_owned(),
                k: 4,
                m: 2,
                w: 5,
            },
            PrefixRule {
                prefix: "data/hot/".to_owned(),
                k: 8,
                m: 3,
                w: 9,
            },
        ]);
        assert_eq!(policy.geometry_for(&ck("data/hot/x")), Some((8, 3, 9)));
        assert_eq!(policy.geometry_for(&ck("data/cold/x")), Some((4, 2, 5)));
        assert_eq!(policy.geometry_for(&ck("other/x")), None);
    }

    /// A disabled policy opts nothing in.
    #[test]
    fn disabled_opts_nothing_in() {
        let policy = WritebackPolicy::disabled();
        assert!(policy.is_disabled());
        assert_eq!(policy.geometry_for(&ck("data/x")), None);
    }
}
