//! A self-verifying cache of statement analyses.
//!
//! Parsing is about 95% of what it costs to route a statement, so caching
//! the analysis is the difference between a proxy that adds microseconds and
//! one that adds hundreds of nanoseconds.
//!
//! The key is the statement's *shape*: the text with literals lifted out
//! into placeholders (see [`crate::shape`]). That groups every
//! `WHERE tenant_id = '<something>'` into one entry, which is what makes the
//! cache work for applications that inline their literals rather than
//! preparing statements.
//!
//! # The check
//!
//! Lifting a literal can change what a statement means — `ORDER BY 1` sorts
//! by the first column and `ORDER BY $1` does not — and the resulting bug is
//! silent and data-dependent. Rather than try to enumerate the exceptions,
//! this cache verifies its own key on every miss:
//!
//! 1. Analyse the original statement.
//! 2. Analyse the normalised statement.
//! 3. Compare them. Cache the shape **only if they agree**.
//!
//! A shape that fails the check is recorded as unusable and its statements
//! are always analysed in full. So the normaliser is allowed to be
//! imperfect: a mistake costs a cache miss, never a wrong answer.
//!
//! The check runs once per shape, not once per statement, so its cost
//! vanishes immediately under any repeating workload.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::shape::normalize;
use crate::{SqlError, StatementAnalysis};

/// Default number of shapes to keep.
///
/// Applications issue a small, stable set of statement shapes; a few
/// thousand covers an ORM-heavy service with room to spare, and the entries
/// are small.
pub const DEFAULT_CAPACITY: usize = 4_096;

/// An analysed statement, ready to route.
#[derive(Debug, Clone)]
pub struct Prepared {
    /// One analysis per statement in the text.
    pub analyses: Arc<Vec<StatementAnalysis>>,
    /// Literals lifted out of the statement, in placeholder order.
    ///
    /// Empty when nothing was lifted, which is the case for an already
    /// parameterised statement and for one with no literals at all.
    pub literals: Vec<Option<String>>,
    /// The placeholder number given to `literals[0]`.
    pub first_placeholder: u16,
    /// Whether this came from the cache.
    pub hit: bool,
}

/// How the cache is doing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Shapes served from the cache.
    pub hits: u64,
    /// Shapes analysed because they were not cached.
    pub misses: u64,
    /// Shapes that failed verification and are always analysed in full.
    pub unverifiable: u64,
    /// Entries dropped to stay within capacity.
    pub evictions: u64,
}

impl CacheStats {
    /// Hit rate in the range `0.0..=1.0`, or `None` before any lookup.
    #[must_use]
    pub fn hit_rate(&self) -> Option<f64> {
        let total = self.hits.checked_add(self.misses)?;
        if total == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some(self.hits as f64 / total as f64)
    }
}

#[derive(Debug, Clone)]
enum Entry {
    /// The shape was verified; this analysis applies to every statement
    /// sharing it.
    Verified(Arc<Vec<StatementAnalysis>>),
    /// Normalising this shape changed its meaning. Always analyse in full.
    Unverifiable,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    /// Insertion order, for eviction.
    ///
    /// First-in-first-out rather than least-recently-used: it needs no
    /// bookkeeping on the read path, which is the path that has to be fast,
    /// and the working set of statement shapes barely changes once an
    /// application is warm.
    order: VecDeque<String>,
    stats: CacheStats,
}

/// A cache of statement analyses, keyed on statement shape.
#[derive(Debug)]
pub struct AnalysisCache {
    inner: RwLock<Inner>,
    capacity: usize,
}

impl Default for AnalysisCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl AnalysisCache {
    /// A cache holding at most `capacity` shapes.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(Inner::default()),
            capacity: capacity.max(1),
        }
    }

    /// Current statistics.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.inner
            .read()
            .map(|inner| inner.stats)
            .unwrap_or_default()
    }

    /// How many shapes are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().map_or(0, |inner| inner.entries.len())
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Analyses a statement, reusing earlier work where the shape matches.
    pub fn analyze(&self, sql: &str) -> Result<Prepared, SqlError> {
        let shape = normalize(sql);

        if let Some(entry) = self.lookup(&shape.normalized) {
            return match entry {
                Entry::Verified(analyses) => Ok(Prepared {
                    analyses,
                    literals: shape.literal_values(),
                    first_placeholder: shape.first_placeholder,
                    hit: true,
                }),
                Entry::Unverifiable => Ok(Prepared {
                    analyses: Arc::new(crate::analyze(sql)?),
                    literals: Vec::new(),
                    first_placeholder: 1,
                    hit: true,
                }),
            };
        }

        // A miss. Analyse the original, because that is the answer we are
        // obliged to return whatever happens to the shape.
        let original = crate::analyze(sql)?;

        // Nothing was lifted, so the shape is the statement and no check is
        // needed.
        if shape.is_empty() {
            let analyses = Arc::new(original);
            let first_placeholder = shape.first_placeholder;
            self.insert(shape.normalized, Entry::Verified(Arc::clone(&analyses)));
            return Ok(Prepared {
                analyses,
                literals: Vec::new(),
                first_placeholder,
                hit: false,
            });
        }

        match crate::analyze(&shape.normalized) {
            Ok(normalized) if equivalent(&original, &normalized) => {
                let analyses = Arc::new(normalized);
                let literals = shape.literal_values();
                let first_placeholder = shape.first_placeholder;
                self.insert(shape.normalized, Entry::Verified(Arc::clone(&analyses)));
                Ok(Prepared {
                    analyses,
                    literals,
                    first_placeholder,
                    hit: false,
                })
            }
            // Either the normalised text no longer parses, or it parses into
            // something different. Both mean the shape is not a safe key.
            _ => {
                self.insert(shape.normalized, Entry::Unverifiable);
                if let Ok(mut inner) = self.inner.write() {
                    inner.stats.unverifiable = inner.stats.unverifiable.saturating_add(1);
                }
                Ok(Prepared {
                    analyses: Arc::new(original),
                    literals: Vec::new(),
                    first_placeholder: 1,
                    hit: false,
                })
            }
        }
    }

    fn lookup(&self, key: &str) -> Option<Entry> {
        let mut inner = self.inner.write().ok()?;
        let found = inner.entries.get(key).cloned();
        if found.is_some() {
            inner.stats.hits = inner.stats.hits.saturating_add(1);
        } else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
        }
        found
    }

    fn insert(&self, key: String, entry: Entry) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        if inner.entries.insert(key.clone(), entry).is_none() {
            inner.order.push_back(key);
        }
        while inner.entries.len() > self.capacity {
            let Some(oldest) = inner.order.pop_front() else {
                break;
            };
            inner.entries.remove(&oldest);
            inner.stats.evictions = inner.stats.evictions.saturating_add(1);
        }
    }
}

/// Whether analysing the normalised text gave the same answer as analysing
/// the original.
///
/// Values are expected to differ — that is the point of lifting them — so
/// what is compared is everything that decides *where* a statement goes and
/// *how* its results combine, plus the structure of the key constraints.
fn equivalent(original: &[StatementAnalysis], normalized: &[StatementAnalysis]) -> bool {
    if original.len() != normalized.len() {
        return false;
    }
    original.iter().zip(normalized).all(|(left, right)| {
        left.kind == right.kind
            && left.access == right.access
            && left.tables == right.tables
            && left.assigned_columns == right.assigned_columns
            // Catches `ORDER BY 1` becoming `ORDER BY $1`: the sort key text
            // changes, so the merge order would have changed with it.
            && left.post_processing == right.post_processing
            && same_constraint_structure(left, right)
    })
}

/// Whether the same columns are constrained, to the same number of values.
fn same_constraint_structure(left: &StatementAnalysis, right: &StatementAnalysis) -> bool {
    let shape_of = |analysis: &StatementAnalysis| {
        analysis
            .constraints
            .columns()
            .map(|(column, values)| (column.clone(), values.len()))
            .collect::<Vec<_>>()
    };
    shape_of(left) == shape_of(right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeyValue;

    fn key_values(prepared: &Prepared, table: &str, column: &str) -> Vec<KeyValue> {
        prepared
            .analyses
            .first()
            .and_then(|analysis| analysis.shard_key_values(table, column))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[test]
    fn a_repeated_shape_is_analysed_once() {
        let cache = AnalysisCache::default();
        let first = cache
            .analyze("SELECT * FROM users WHERE tenant_id = 'alice'")
            .expect("should analyse");
        let second = cache
            .analyze("SELECT * FROM users WHERE tenant_id = 'bob'")
            .expect("should analyse");

        assert!(!first.hit);
        assert!(second.hit);
        assert_eq!(cache.len(), 1);
        // Both statements share one analysis, allocated once.
        assert!(Arc::ptr_eq(&first.analyses, &second.analyses));
    }

    #[test]
    fn the_lifted_literal_comes_back_as_the_value_to_route_on() {
        let cache = AnalysisCache::default();
        let prepared = cache
            .analyze("SELECT * FROM users WHERE tenant_id = 'alice'")
            .expect("should analyse");
        assert_eq!(prepared.literals, vec![Some("alice".to_owned())]);
        assert_eq!(prepared.first_placeholder, 1);
        // The cached analysis sees a parameter where the literal was.
        assert_eq!(
            key_values(&prepared, "users", "tenant_id"),
            vec![KeyValue::Parameter(1)]
        );
    }

    #[test]
    fn a_negative_literal_keeps_its_sign_through_the_cache() {
        // The failure this guards against: `id = -1` cached under the same
        // shape as `id = 1` and then routed on the absolute value.
        let cache = AnalysisCache::default();
        let negative = cache
            .analyze("SELECT * FROM t WHERE id = -1")
            .expect("should analyse");
        let positive = cache
            .analyze("SELECT * FROM t WHERE id = 1")
            .expect("should analyse");
        assert!(positive.hit, "both should share one shape");
        assert_eq!(negative.literals, vec![Some("-1".to_owned())]);
        assert_eq!(positive.literals, vec![Some("1".to_owned())]);
    }

    #[test]
    fn order_by_an_ordinal_is_detected_and_never_cached_as_a_shape() {
        // `ORDER BY 1` sorts by the first column; `ORDER BY $1` does not.
        // The check has to catch this, or a scattered read merges in the
        // wrong order.
        let cache = AnalysisCache::default();
        let prepared = cache
            .analyze("SELECT a, b FROM users ORDER BY 1")
            .expect("should analyse");
        assert_eq!(cache.stats().unverifiable, 1);

        // The answer returned is still the correct one, taken from the
        // original text.
        let order = prepared
            .analyses
            .first()
            .map(|analysis| analysis.post_processing.order_by.clone())
            .unwrap_or_default();
        assert_eq!(order.first().map(|key| key.expression.as_str()), Some("1"));
    }

    #[test]
    fn an_unverifiable_shape_stays_correct_on_every_later_call() {
        let cache = AnalysisCache::default();
        let _ = cache.analyze("SELECT a, b FROM users ORDER BY 1");
        let again = cache
            .analyze("SELECT a, b FROM users ORDER BY 2")
            .expect("should analyse");
        let order = again
            .analyses
            .first()
            .map(|analysis| analysis.post_processing.order_by.clone())
            .unwrap_or_default();
        assert_eq!(order.first().map(|key| key.expression.as_str()), Some("2"));
        assert_eq!(again.literals, Vec::<Option<String>>::new());
    }

    #[test]
    fn an_already_parameterised_statement_caches_with_no_literals() {
        let cache = AnalysisCache::default();
        let first = cache
            .analyze("SELECT * FROM users WHERE tenant_id = $1")
            .expect("should analyse");
        let second = cache
            .analyze("SELECT * FROM users WHERE tenant_id = $1")
            .expect("should analyse");
        assert!(!first.hit);
        assert!(second.hit);
        assert_eq!(first.literals, Vec::<Option<String>>::new());
    }

    #[test]
    fn different_statements_do_not_share_an_entry() {
        let cache = AnalysisCache::default();
        let _ = cache.analyze("SELECT * FROM users WHERE tenant_id = 'a'");
        let _ = cache.analyze("SELECT * FROM orders WHERE tenant_id = 'a'");
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.stats().hits, 0);
        assert_eq!(cache.stats().misses, 2);
    }

    #[test]
    fn a_syntax_error_is_still_an_error_and_is_not_cached() {
        let cache = AnalysisCache::default();
        assert!(cache.analyze("SELEKT * FRM").is_err());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn capacity_is_respected_and_evictions_are_counted() {
        let cache = AnalysisCache::new(2);
        for table in ["a", "b", "c", "d"] {
            let _ = cache.analyze(&format!("SELECT * FROM {table} WHERE x = 1"));
        }
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.stats().evictions, 2);
    }

    #[test]
    fn hit_rate_is_reported_once_there_is_something_to_report() {
        let cache = AnalysisCache::default();
        assert_eq!(cache.stats().hit_rate(), None);
        let _ = cache.analyze("SELECT * FROM users WHERE tenant_id = 'a'");
        let _ = cache.analyze("SELECT * FROM users WHERE tenant_id = 'b'");
        assert_eq!(cache.stats().hit_rate(), Some(0.5));
    }

    #[test]
    fn a_multi_statement_batch_caches_as_one_shape() {
        let cache = AnalysisCache::default();
        let first = cache
            .analyze("SELECT * FROM users WHERE tenant_id = 'a'; SELECT 1")
            .expect("should analyse");
        assert_eq!(first.analyses.len(), 2);
        let second = cache
            .analyze("SELECT * FROM users WHERE tenant_id = 'b'; SELECT 1")
            .expect("should analyse");
        assert!(second.hit);
    }
}
