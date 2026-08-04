//! The Vamana graph engine in its streaming (FreshDiskANN) variant.
//!
//! This is a pure, hermetic implementation of the DiskANN line:
//!
//! - **GreedySearch** and **RobustPrune** from Subramanya et al., *DiskANN:
//!   Fast Accurate Billion-point Nearest Neighbor Search on a Single Node*
//!   (NeurIPS 2019). The `alpha`/`R`/`L` parameters and the medoid entry point
//!   are theirs.
//! - **Streaming insert**, **lazy delete + delete-consolidation** from Singh et
//!   al., *FreshDiskANN* (arXiv:2105.09613, 2021). An insert runs the same
//!   GreedySearch+RobustPrune step the batch build runs per point, plus the
//!   backward-edge repair; a delete only tombstones (the point stays wired into
//!   the graph so traversal is unbroken), and `consolidate` later rewires and
//!   physically removes tombstoned points.
//!
//! Everything here is in-memory and single-threaded: correctness first, at
//! framework level. Vectors are keyed by an external `i64` id (the source
//! table's identity column); the graph is over dense internal indices.

use std::collections::HashMap;

use crate::error::{Result, VectorError};
use crate::metric::Metric;

/// The default maximum out-degree `R`. DiskANN uses graph degrees in the 32–128
/// range; 64 is the common default and the FreshDiskANN streaming default.
pub const DEFAULT_R: usize = 64;
/// The default build/search candidate-list size `L`. Must be `>= R`; 100 is the
/// paper's typical build list.
pub const DEFAULT_L: usize = 100;
/// The default pruning parameter `alpha`. DiskANN's second construction pass and
/// FreshDiskANN's inserts use `alpha > 1` (1.2 is the paper default) to admit
/// longer-range "shortcut" edges that shrink the graph diameter.
pub const DEFAULT_ALPHA: f32 = 1.2;

/// Construction/insert parameters. Shared by the batch build and every streaming
/// insert so a streamed-in point is wired exactly like a batch-built one.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct VamanaParams {
    /// Maximum out-degree `R`.
    pub r: usize,
    /// Candidate-list size `L` used during construction/insert search.
    pub l_build: usize,
    /// Pruning parameter `alpha` (`>= 1`).
    pub alpha: f32,
}

impl Default for VamanaParams {
    fn default() -> Self {
        VamanaParams {
            r: DEFAULT_R,
            l_build: DEFAULT_L,
            alpha: DEFAULT_ALPHA,
        }
    }
}

impl VamanaParams {
    /// Clamps nonsensical parameters into a working range rather than failing:
    /// `r >= 1`, `l_build >= r`, `alpha >= 1`. (Not a tuning surface — these are
    /// guards against a malformed declare body, not knobs.)
    pub fn sane(self) -> Self {
        let r = self.r.max(1);
        VamanaParams {
            r,
            l_build: self.l_build.max(r),
            alpha: if self.alpha >= 1.0 { self.alpha } else { 1.0 },
        }
    }
}

/// One nearest-neighbor query result: the external id and its distance under the
/// index metric (Euclidean norm for L2, `1 - cos` for cosine).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Neighbor {
    /// The external id of the neighbor (the source row's identity value).
    pub id: i64,
    /// The distance under the index metric; smaller is closer.
    pub distance: f32,
}

/// A streaming Vamana index over `dim`-dimensional vectors keyed by `i64` id.
#[derive(Debug, Clone)]
pub struct VamanaIndex {
    metric: Metric,
    dim: usize,
    params: VamanaParams,
    /// The source snapshot/watermark this index reflects (an Iceberg snapshot
    /// id). Carried so a loaded blob knows what it indexes.
    reflected_snapshot: i64,

    /// Prepared vectors (normalized for cosine), flat, `dim`-strided by internal
    /// index. Tombstoned slots keep their vectors until `consolidate`.
    vectors: Vec<f32>,
    /// Internal index → external id.
    ids: Vec<i64>,
    /// External id → internal index (live and tombstoned alike, until
    /// consolidation compacts them out).
    id_to_idx: HashMap<i64, u32>,
    /// Out-adjacency per internal index.
    adj: Vec<Vec<u32>>,
    /// Tombstone flags per internal index (the FreshDiskANN DeleteList).
    deleted: Vec<bool>,
    /// Count of tombstoned-but-not-consolidated points.
    tombstones: usize,
    /// The current entry point (medoid), an internal index, or `None` when the
    /// index is empty or the medoid was tombstoned and not yet recomputed.
    medoid: Option<u32>,
    /// Deterministic RNG state for random init and permutation.
    rng: u64,
}

impl VamanaIndex {
    /// Creates an empty index. `seed` makes construction deterministic (tests
    /// and reproducible builds).
    pub fn new(metric: Metric, dim: usize, params: VamanaParams, seed: u64) -> Result<Self> {
        if dim == 0 {
            return Err(VectorError::InvalidDim(0));
        }
        Ok(VamanaIndex {
            metric,
            dim,
            params: params.sane(),
            reflected_snapshot: -1,
            vectors: Vec::new(),
            ids: Vec::new(),
            id_to_idx: HashMap::new(),
            adj: Vec::new(),
            deleted: Vec::new(),
            tombstones: 0,
            medoid: None,
            rng: seed | 1,
        })
    }

    /// The index metric.
    pub fn metric(&self) -> Metric {
        self.metric
    }
    /// The vector dimensionality.
    pub fn dim(&self) -> usize {
        self.dim
    }
    /// The construction parameters.
    pub fn params(&self) -> VamanaParams {
        self.params
    }
    /// The source snapshot id this index reflects.
    pub fn reflected_snapshot(&self) -> i64 {
        self.reflected_snapshot
    }
    /// Sets the reflected snapshot (called by the maintenance MV after applying a
    /// delta up to that snapshot).
    pub fn set_reflected_snapshot(&mut self, snapshot: i64) {
        self.reflected_snapshot = snapshot;
    }
    /// The number of live (non-tombstoned) vectors.
    pub fn len(&self) -> usize {
        self.ids.len() - self.tombstones
    }
    /// Whether the index has no live vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The number of tombstoned-but-not-consolidated points.
    pub fn tombstone_count(&self) -> usize {
        self.tombstones
    }
    /// Whether `id` is present and live.
    pub fn contains(&self, id: i64) -> bool {
        self.id_to_idx
            .get(&id)
            .is_some_and(|&i| !self.deleted[i as usize])
    }

    fn next_rng(&mut self) -> u64 {
        // SplitMix64 — small, fast, dependency-free, good enough for graph init.
        self.rng = self.rng.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    #[inline]
    fn vec_at(&self, idx: u32) -> &[f32] {
        let s = idx as usize * self.dim;
        &self.vectors[s..s + self.dim]
    }

    /// Batch-builds the index from `(id, vector)` pairs, replacing any current
    /// contents. Two passes (`alpha = 1` then the configured `alpha`) over a
    /// random permutation, the DiskANN construction. Duplicate ids keep the last
    /// vector seen.
    pub fn build(&mut self, items: &[(i64, Vec<f32>)]) -> Result<()> {
        self.vectors.clear();
        self.ids.clear();
        self.id_to_idx.clear();
        self.adj.clear();
        self.deleted.clear();
        self.tombstones = 0;
        self.medoid = None;

        // De-duplicate by id (last wins), preserving discovery order.
        let mut order: Vec<i64> = Vec::new();
        let mut latest: HashMap<i64, &Vec<f32>> = HashMap::new();
        for (id, v) in items {
            if latest.insert(*id, v).is_none() {
                order.push(*id);
            }
        }
        for id in &order {
            let v = latest[id];
            if v.len() != self.dim {
                return Err(VectorError::DimMismatch {
                    expected: self.dim,
                    got: v.len(),
                });
            }
            let prepared = self.metric.prepare(v);
            let idx = self.ids.len() as u32;
            self.vectors.extend_from_slice(&prepared);
            self.ids.push(*id);
            self.id_to_idx.insert(*id, idx);
            self.adj.push(Vec::new());
            self.deleted.push(false);
        }
        let n = self.ids.len();
        if n == 0 {
            return Ok(());
        }

        self.medoid = Some(self.compute_medoid());

        // Random initialization: each node gets up to R random out-edges.
        for i in 0..n {
            let mut nbrs = Vec::with_capacity(self.params.r.min(n - 1));
            let mut seen = vec![false; n];
            seen[i] = true;
            let want = self.params.r.min(n - 1);
            while nbrs.len() < want {
                let j = (self.next_rng() % n as u64) as usize;
                if !seen[j] {
                    seen[j] = true;
                    nbrs.push(j as u32);
                }
            }
            self.adj[i] = nbrs;
        }

        // A random visit order for the construction passes.
        let mut perm: Vec<u32> = (0..n as u32).collect();
        for i in (1..n).rev() {
            let j = (self.next_rng() % (i as u64 + 1)) as usize;
            perm.swap(i, j);
        }

        // Pass 1 at alpha=1, pass 2 at the configured alpha (DiskANN's two-pass
        // construction: the second, longer-range pass is what makes Vamana's
        // diameter small).
        for &alpha in &[1.0f32, self.params.alpha] {
            for &p in &perm {
                self.insert_edges_for(p, alpha);
            }
        }
        Ok(())
    }

    /// Runs GreedySearch from the medoid toward `p`'s vector, then RobustPrune to
    /// set `p`'s out-edges, then repairs backward edges. The shared per-point
    /// construction/insert step.
    fn insert_edges_for(&mut self, p: u32, alpha: f32) {
        let query = self.vec_at(p).to_vec();
        let visited = self.greedy_search_visited(&query, self.params.l_build, Some(p));
        let pruned = self.robust_prune(p, visited, alpha);
        self.adj[p as usize] = pruned.clone();
        // Backward edges: add p to each neighbor, prune the neighbor if it
        // overflows R.
        for &j in &pruned {
            let jn = j as usize;
            if !self.adj[jn].contains(&p) {
                self.adj[jn].push(p);
            }
            if self.adj[jn].len() > self.params.r {
                let cand = std::mem::take(&mut self.adj[jn]);
                self.adj[jn] = self.robust_prune(j, cand, alpha);
            }
        }
    }

    /// GreedySearch returning the full set of visited internal indices (the
    /// candidate pool RobustPrune consumes). Traversal passes *through*
    /// tombstoned nodes (their edges are intact) but does not itself filter;
    /// filtering deleted ids is a query-result concern. `exclude` is the point
    /// being (re)inserted, kept out of its own candidate set.
    fn greedy_search_visited(&self, query: &[f32], l: usize, exclude: Option<u32>) -> Vec<u32> {
        let Some(medoid) = self.entry_point() else {
            return Vec::new();
        };
        // Candidate frontier as (distance, idx), kept sorted, capped at L.
        let mut frontier: Vec<(f32, u32)> = Vec::with_capacity(l + 1);
        let mut in_frontier: HashMap<u32, ()> = HashMap::new();
        let mut visited: Vec<u32> = Vec::new();
        let mut is_visited: HashMap<u32, ()> = HashMap::new();

        let d0 = self.metric.compare_distance(query, self.vec_at(medoid));
        frontier.push((d0, medoid));
        in_frontier.insert(medoid, ());

        // Expand the closest unvisited frontier node until the frontier is
        // fully explored.
        while let Some(&(_, cur)) = frontier
            .iter()
            .filter(|(_, idx)| !is_visited.contains_key(idx))
            .min_by(|a, b| a.0.total_cmp(&b.0))
        {
            is_visited.insert(cur, ());
            if Some(cur) != exclude {
                visited.push(cur);
            }
            // Expand.
            let neighbors = self.adj[cur as usize].clone();
            for nb in neighbors {
                if in_frontier.contains_key(&nb) {
                    continue;
                }
                let d = self.metric.compare_distance(query, self.vec_at(nb));
                frontier.push((d, nb));
                in_frontier.insert(nb, ());
            }
            // Keep the L closest in the frontier.
            frontier.sort_by(|a, b| a.0.total_cmp(&b.0));
            if frontier.len() > l {
                for &(_, dropped) in &frontier[l..] {
                    in_frontier.remove(&dropped);
                }
                frontier.truncate(l);
            }
            // Done when every frontier node is visited.
            if frontier.iter().all(|(_, idx)| is_visited.contains_key(idx)) {
                break;
            }
        }
        visited
    }

    /// RobustPrune (DiskANN Algorithm 2): from the candidate pool (visited set
    /// plus `p`'s current out-neighbors), greedily keep the closest candidate
    /// `p*` and drop every remaining `p'` for which `alpha * d(p*, p') <=
    /// d(p, p')`, until the pool empties or `R` edges are kept. Never keeps `p`
    /// itself or tombstoned points.
    fn robust_prune(&self, p: u32, candidates: Vec<u32>, alpha: f32) -> Vec<u32> {
        let pv = self.vec_at(p).to_vec();
        // Build the candidate pool: visited ∪ current neighbors, minus p and
        // tombstones, de-duplicated, each tagged with distance to p.
        let mut pool: Vec<(f32, u32)> = Vec::new();
        let mut seen: HashMap<u32, ()> = HashMap::new();
        for c in candidates
            .into_iter()
            .chain(self.adj[p as usize].iter().copied())
        {
            if c == p || self.deleted[c as usize] || seen.contains_key(&c) {
                continue;
            }
            seen.insert(c, ());
            let d = self.metric.compare_distance(&pv, self.vec_at(c));
            pool.push((d, c));
        }
        pool.sort_by(|a, b| a.0.total_cmp(&b.0));

        let mut result: Vec<u32> = Vec::with_capacity(self.params.r);
        let mut i = 0;
        while i < pool.len() && result.len() < self.params.r {
            let (_, pstar) = pool[i];
            i += 1;
            // pstar may have been pruned already.
            if pstar == u32::MAX {
                continue;
            }
            result.push(pstar);
            let sv = self.vec_at(pstar).to_vec();
            // Occlude the rest.
            for entry in pool.iter_mut().skip(i) {
                if entry.1 == u32::MAX {
                    continue;
                }
                let d_pstar = self.metric.compare_distance(&sv, self.vec_at(entry.1));
                if alpha * d_pstar <= entry.0 {
                    entry.1 = u32::MAX; // tombstone within this prune pass
                }
            }
        }
        result
    }

    /// The live entry point. Prefers the medoid; if it is missing or tombstoned,
    /// falls back to any live node (and does not mutate — recomputation happens
    /// in `consolidate`).
    fn entry_point(&self) -> Option<u32> {
        if let Some(m) = self.medoid
            && !self.deleted[m as usize]
        {
            return Some(m);
        }
        (0..self.ids.len() as u32).find(|&i| !self.deleted[i as usize])
    }

    /// The medoid: the live point closest to the centroid (mean) of all live
    /// vectors. DiskANN's `O(n)` medoid approximation — the true medoid is
    /// `O(n^2)` and unnecessary as an entry point.
    fn compute_medoid(&self) -> u32 {
        let n = self.ids.len();
        let mut centroid = vec![0.0f32; self.dim];
        let mut live = 0usize;
        for i in 0..n {
            if self.deleted[i] {
                continue;
            }
            live += 1;
            let v = self.vec_at(i as u32);
            for d in 0..self.dim {
                centroid[d] += v[d];
            }
        }
        if live > 0 {
            for c in &mut centroid {
                *c /= live as f32;
            }
        }
        let mut best = 0u32;
        let mut best_d = f32::INFINITY;
        for i in 0..n {
            if self.deleted[i] {
                continue;
            }
            let d = self
                .metric
                .compare_distance(&centroid, self.vec_at(i as u32));
            if d < best_d {
                best_d = d;
                best = i as u32;
            }
        }
        best
    }

    /// Streaming insert (FreshDiskANN): adds or replaces the vector for `id`.
    /// A replace tombstones the old slot and inserts a fresh one, so stale edges
    /// retire at the next `consolidate`. New points are wired with the same
    /// GreedySearch+RobustPrune step the batch build uses.
    pub fn insert(&mut self, id: i64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }
        // Replace: tombstone the existing live slot first.
        if let Some(&old) = self.id_to_idx.get(&id)
            && !self.deleted[old as usize]
        {
            self.deleted[old as usize] = true;
            self.tombstones += 1;
        }
        let prepared = self.metric.prepare(vector);
        let idx = self.ids.len() as u32;
        self.vectors.extend_from_slice(&prepared);
        self.ids.push(id);
        self.id_to_idx.insert(id, idx);
        self.adj.push(Vec::new());
        self.deleted.push(false);

        if self.medoid.is_none() {
            self.medoid = Some(idx);
            return Ok(());
        }
        self.insert_edges_for(idx, self.params.alpha);
        Ok(())
    }

    /// Lazy delete (FreshDiskANN): tombstones `id`. The point stays wired into
    /// the graph so in-flight traversals are unbroken; it is filtered from query
    /// results and physically removed at the next `consolidate`. Returns whether
    /// a live point was tombstoned.
    pub fn delete(&mut self, id: i64) -> bool {
        if let Some(&idx) = self.id_to_idx.get(&id)
            && !self.deleted[idx as usize]
        {
            self.deleted[idx as usize] = true;
            self.tombstones += 1;
            return true;
        }
        false
    }

    /// Delete-consolidation (FreshDiskANN StreamingMerge): for every live point
    /// `p` with an out-edge into the DeleteList, replace those edges with the
    /// deleted neighbors' own live out-neighbors, then RobustPrune back to `R`;
    /// finally physically drop all tombstoned points and rebuild compacted
    /// storage. Recomputes the medoid. A no-op when there are no tombstones.
    pub fn consolidate(&mut self) {
        if self.tombstones == 0 {
            return;
        }
        let n = self.ids.len();

        // Phase 1 — edge repair (still on the old index space).
        for p in 0..n {
            if self.deleted[p] {
                continue;
            }
            let has_deleted_edge = self.adj[p].iter().any(|&j| self.deleted[j as usize]);
            if !has_deleted_edge {
                continue;
            }
            let mut cand: Vec<u32> = Vec::new();
            let mut seen: HashMap<u32, ()> = HashMap::new();
            for &j in &self.adj[p] {
                if self.deleted[j as usize] {
                    // Splice in the deleted neighbor's live out-neighbors.
                    for &k in &self.adj[j as usize] {
                        if !self.deleted[k as usize]
                            && k as usize != p
                            && seen.insert(k, ()).is_none()
                        {
                            cand.push(k);
                        }
                    }
                } else if seen.insert(j, ()).is_none() {
                    cand.push(j);
                }
            }
            self.adj[p] = self.robust_prune(p as u32, cand, self.params.alpha);
        }

        // Phase 2 — physically compact out the tombstoned points.
        let mut remap: Vec<Option<u32>> = vec![None; n];
        let mut new_next = 0u32;
        for (i, slot) in remap.iter_mut().enumerate() {
            if !self.deleted[i] {
                *slot = Some(new_next);
                new_next += 1;
            }
        }
        let new_n = new_next as usize;

        let mut new_vectors = Vec::with_capacity(new_n * self.dim);
        let mut new_ids = Vec::with_capacity(new_n);
        let mut new_adj: Vec<Vec<u32>> = Vec::with_capacity(new_n);
        let mut new_id_to_idx = HashMap::with_capacity(new_n);
        for i in 0..n {
            let Some(ni) = remap[i] else { continue };
            new_vectors.extend_from_slice(self.vec_at(i as u32));
            new_ids.push(self.ids[i]);
            new_id_to_idx.insert(self.ids[i], ni);
            let mut nbrs: Vec<u32> = self.adj[i]
                .iter()
                .filter_map(|&j| remap[j as usize])
                .collect();
            nbrs.truncate(self.params.r);
            new_adj.push(nbrs);
        }

        self.vectors = new_vectors;
        self.ids = new_ids;
        self.id_to_idx = new_id_to_idx;
        self.adj = new_adj;
        self.deleted = vec![false; new_n];
        self.tombstones = 0;
        self.medoid = if new_n == 0 {
            None
        } else {
            Some(self.compute_medoid())
        };
    }

    /// Searches for the `k` nearest live neighbors of `query`, exploring with a
    /// candidate list of size `l` (`l` is clamped up to `k`). Tombstoned points
    /// are traversed but never returned. Results are sorted nearest-first.
    pub fn search(&self, query: &[f32], k: usize, l: usize) -> Result<Vec<Neighbor>> {
        if query.len() != self.dim {
            return Err(VectorError::DimMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }
        if k == 0 || self.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = self.metric.prepare(query);
        let l = l.max(k).max(self.params.r);
        let visited = self.greedy_search_visited(&prepared, l, None);
        let mut hits: Vec<Neighbor> = visited
            .into_iter()
            .filter(|&idx| !self.deleted[idx as usize])
            .map(|idx| Neighbor {
                id: self.ids[idx as usize],
                distance: self.metric.report_distance(&prepared, self.vec_at(idx)),
            })
            .collect();
        hits.sort_by(|a, b| a.distance.total_cmp(&b.distance));
        hits.truncate(k);
        Ok(hits)
    }

    // --- serialization support (used by codec.rs) ---

    #[allow(clippy::type_complexity)]
    pub(crate) fn parts(
        &self,
    ) -> (
        Metric,
        usize,
        VamanaParams,
        i64,
        &[f32],
        &[i64],
        &[Vec<u32>],
        &[bool],
    ) {
        (
            self.metric,
            self.dim,
            self.params,
            self.reflected_snapshot,
            &self.vectors,
            &self.ids,
            &self.adj,
            &self.deleted,
        )
    }

    /// Reconstructs an index from decoded parts, including the persisted
    /// tombstone bitmap (FreshDiskANN's on-disk DeleteList — deletes survive
    /// across maintenance runs until a consolidation rewrites the graph). The
    /// medoid is recomputed over live points; vectors are already prepared.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        metric: Metric,
        dim: usize,
        params: VamanaParams,
        reflected_snapshot: i64,
        vectors: Vec<f32>,
        ids: Vec<i64>,
        adj: Vec<Vec<u32>>,
        deleted: Vec<bool>,
    ) -> Result<Self> {
        if dim == 0 {
            return Err(VectorError::InvalidDim(0));
        }
        let n = ids.len();
        if vectors.len() != n * dim || adj.len() != n || deleted.len() != n {
            return Err(VectorError::CorruptBlob(
                "vector/adjacency/tombstone length mismatch".to_owned(),
            ));
        }
        let mut id_to_idx = HashMap::with_capacity(n);
        for (i, &id) in ids.iter().enumerate() {
            id_to_idx.insert(id, i as u32);
        }
        let tombstones = deleted.iter().filter(|&&d| d).count();
        let mut idx = VamanaIndex {
            metric,
            dim,
            params: params.sane(),
            reflected_snapshot,
            vectors,
            ids,
            id_to_idx,
            adj,
            deleted,
            tombstones,
            medoid: None,
            rng: 0x1234_5678_9ABC_DEF0,
        };
        idx.medoid = if idx.is_empty() {
            None
        } else {
            Some(idx.compute_medoid())
        };
        Ok(idx)
    }
}
