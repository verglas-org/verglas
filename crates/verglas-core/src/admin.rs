//! Shared admin HTTP API types used by the CLI and server.
//!
//! The admin API is a private control surface on a separate port from the S3
//! data-plane endpoint. Request and response bodies defined here are the wire
//! contract both sides must agree on.

use serde::{Deserialize, Serialize};

/// Path for the server version probe.
pub const VERSION_PATH: &str = "/admin/version";

/// Path for the server liveness probe.
pub const HEALTHZ_PATH: &str = "/admin/healthz";

/// Path for the cache purge control operation (`POST`). Drops every key→ETag
/// mapping and clears both cache tiers so the next reads are cold misses —
/// an honest repeat-cold benchmark leg (or operator reset) without a server
/// restart (issue #138).
pub const PURGE_PATH: &str = "/cache/purge";

/// Path for the server cache-config + read-path counters probe (issue #141).
///
/// The single source of truth for a bench run's tier context: reports read the
/// server's actual `dram_bytes`/`capacity_bytes` from here rather than guessing,
/// and the nvme-resident/constrained profiles read the counters to prove where
/// warm reads were served from (DRAM vs disk) and whether eviction happened.
pub const STATS_PATH: &str = "/admin/stats";

/// Path for the gossip membership snapshot probe (`GET`, issue #27).
///
/// Reports this node's live view of the pod — every member's identity,
/// addresses, capacity/weight, and state — for `verglas status` (#62) and
/// debugging. Loopback-only like the rest of the admin surface; served only
/// when the server runs with a `[cluster]` config.
pub const MEMBERS_PATH: &str = "/admin/members";

/// Path for the Prometheus metrics scrape (`GET`, issue #46).
///
/// Standard text exposition (`text/plain; version=0.0.4`) of the node-level
/// metric families: request latency, bytes served by tier, cache hit/miss and
/// admission counters, cache size/capacity gauges, fill inflight, and backend
/// health. Loopback-only like the rest of the admin surface; served only when a
/// cache engine exists (the config-less smoke mode has nothing to report).
pub const METRICS_PATH: &str = "/metrics";

/// Path for the per-table telemetry report (`GET`, issue #60).
///
/// Returns the [`telemetry::TablesReport`](crate::telemetry::TablesReport): one
/// row per table with recorded traffic (hit rate, cached/served bytes, requests
/// avoided, latency saved) plus fleet totals. This is what `verglas tables`
/// reads. Loopback-only like the rest of the admin surface; served only when the
/// mapper exists to attribute reads to tables.
pub const TABLE_METRICS_PATH: &str = "/v1/metering/tables";

/// Path for the graceful-drain control operation (`POST`, issue #31).
///
/// Marks the target node `draining`: it gossips the state (peers shed its
/// ownership to successors), keeps serving what it holds as a donor its
/// successors warm from, and exits after its keys are re-owned warm or the
/// drain timeout elapses — so the pod never sees an error spike on a scale-in.
/// Loopback-only like the rest of the admin surface, and served only on a node
/// running with a `[cluster]` config (a single node has nothing to drain to).
pub const DRAIN_PATH: &str = "/admin/drain";

/// Path for the runtime log-level control (`POST`, issue #61). Hot-reloads the
/// server's `tracing` filter without a restart, so an operator can turn on
/// `DEBUG` span detail for a live investigation and turn it back down after.
/// Loopback-only like the rest of the admin surface.
pub const LOG_PATH: &str = "/admin/log";

/// Path for the local-access probe (`GET`, issue #287). Returns the non-secret
/// connection details clients need: the server's S3 cache endpoint, query API,
/// and shallow on-prem Iceberg REST proxy. The paired secret access key is
/// deliberately NOT served here: the admin listener is unauthenticated and
/// binds loopback, which is host-scoped, so any local process could otherwise
/// read a lakehouse read/write key without opening the server's 0600 credentials
/// file. The CLI reads the secret from that user-scoped credentials file
/// instead. Loopback-only like the rest of the admin surface.
pub const ACCESS_PATH: &str = "/admin/access";

/// Default admin API listen address when no override is configured. Must
/// agree with the `[listen]` config default (`Listen::default().admin_port`).
pub const DEFAULT_ADMIN_ADDR: &str = "127.0.0.1:8334";

/// Environment variable holding the admin API base URL for CLI clients.
pub const ENDPOINT_ENV: &str = "VERGLAS_ENDPOINT";

/// Default admin API base URL used by CLI clients. Must agree with
/// `DEFAULT_ADMIN_ADDR` so a default CLI reaches a default-config server.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8334";

/// Version metadata returned by `GET /admin/version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionInfo {
    /// Server binary name.
    pub name: String,
    /// Server release version.
    pub version: String,
}

/// Readiness marker reported by `/admin/healthz` once the cache engine's disk
/// recovery has completed and the node can serve warm reads.
pub const HEALTH_STATUS_OK: &str = "ok";

/// Readiness marker reported by `/admin/healthz` while the server is still
/// rebuilding its cache index from the on-disk tiers (issue #16). A load
/// balancer must not route data-plane traffic to a node reporting this — it
/// would cold-miss everything until recovery finishes.
pub const HEALTH_STATUS_STARTING: &str = "starting";

/// Liveness/readiness metadata returned by `GET /admin/healthz`.
///
/// The probe reports [`HEALTH_STATUS_STARTING`] (with a `503 Service
/// Unavailable` status) until disk recovery completes, then
/// [`HEALTH_STATUS_OK`] (with `200 OK`) — serve-gating so an NLB never routes
/// to a node that would cold-miss everything (issue #16).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthzInfo {
    /// `ok` once recovery is complete, `starting` while it runs.
    pub status: String,
}

/// Counts returned by `POST /cache/purge` (issue #138): what the purge
/// dropped, for the operator's log line and the CLI/bench caller.
///
/// Purge is a generation-epoch bump (memcached `flush_all` / Redis lazyfree
/// convergent pattern): it is O(1) and returns immediately. The block and
/// metadata stores are **not** physically cleared — the engine's global cache
/// generation is bumped, so every entry written under an earlier generation is
/// instantly unreachable (a lookup at the new generation misses) and is
/// reclaimed lazily by LRU aging. The byte figures below split what that means:
///
/// - `mapping_bytes_freed` is *actually freed* now — the tiny DRAM-only
///   key→ETag mapping cache is cleared synchronously (it is fence-serialized,
///   not the racy foyer store, so a clear there is safe; see the engine's purge
///   doc).
/// - `reclaimable_bytes` was *live* an instant ago and is now logically freed
///   but still physically resident until background reclaim — the honest T=0
///   "live bytes now reclaimable" figure a repeat-cold benchmark reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeReport {
    /// The cache generation *after* this purge. Reads now resolve at this
    /// generation; entries under earlier generations are unreachable garbage.
    pub generation: u64,
    /// Bytes freed synchronously from the key→ETag mapping cache (DRAM-only).
    pub mapping_bytes_freed: u64,
    /// DRAM bytes (block tier + metadata store) that were live before this
    /// purge and are now reclaimable: logically freed instantly, physically
    /// resident until the background LRU reclaim ages them out.
    pub reclaimable_bytes: u64,
}

impl VersionInfo {
    /// Builds the version payload served by `verglas-server`.
    pub fn for_server(version: &str) -> Self {
        Self {
            name: "verglas-server".to_owned(),
            version: version.to_owned(),
        }
    }
}

impl HealthzInfo {
    /// The ready response: recovery is complete, warm reads are being served.
    pub fn ok() -> Self {
        Self {
            status: HEALTH_STATUS_OK.to_owned(),
        }
    }

    /// The starting response: disk recovery is still rebuilding the cache index,
    /// so the node is not yet ready to take data-plane traffic (issue #16).
    pub fn starting() -> Self {
        Self {
            status: HEALTH_STATUS_STARTING.to_owned(),
        }
    }
}

/// Non-secret connection details returned by `GET /admin/access` (issue #287) so
/// the agent-facing CLI verbs run with zero configuration against a running
/// server.
///
/// Everything here is resolved from the server's own config and is safe to hand
/// out over the unauthenticated, host-scoped loopback admin socket: the S3
/// endpoint, query API, Iceberg REST catalog coordinates, signing region, served
/// bucket, and endpoint access key id.
/// `access_key_id` is present only when the server has an endpoint auth keypair.
///
/// The paired **secret access key is intentionally absent**. Serving it here
/// would leak a lakehouse read/write credential to any local process that can
/// open the admin port, bypassing the server's 0600 credentials file. The CLI
/// reads the secret from that user-scoped credentials file instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalAccess {
    /// Base URL of the server's S3 data endpoint (e.g. `http://127.0.0.1:8333`).
    /// Data files are written and queries are read through it so a server in the
    /// path gives cache residency and write-back.
    pub s3_endpoint: String,
    /// Base URL of the query and write API served by this deployment.
    pub query_uri: String,
    /// Iceberg REST catalog URI clients should use, when present.
    pub catalog_uri: Option<String>,
    /// Configured Iceberg warehouse identifier, when present.
    pub warehouse: Option<String>,
    /// SigV4 signing region the endpoint expects (e.g. `us-east-1`).
    pub region: String,
    /// The one bucket the server serves, when configured.
    pub bucket: Option<String>,
    /// Endpoint access key id, or `None` when the server has no auth keypair.
    /// The paired secret is never carried here; the CLI reads it from the local
    /// 0600 credentials file the server also reads.
    pub access_key_id: Option<String>,
}

/// The server's resolved cache budgets, as configured (hard ceilings). Reports
/// stamp these next to every latency number so no speedup is ever published
/// without its tier context (issue #141).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfigInfo {
    /// The on-disk cache directory.
    pub dir: String,
    /// On-disk (NVMe) cache size ceiling, in bytes.
    pub capacity_bytes: u64,
    /// In-memory (DRAM) cache size ceiling, in bytes.
    pub dram_bytes: u64,
}

/// The read-path counters, mirroring `verglas-cache`'s `CountersSnapshot` on the
/// admin wire. These are what the nvme-resident and constrained profiles read to
/// prove *where* warm reads came from (DRAM vs disk) and whether eviction forced
/// backend refills — evidence the acceptance criteria demand rather than assume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountersInfo {
    /// Block lookups answered by the DRAM tier.
    pub dram_hits: u64,
    /// Block lookups that missed the DRAM tier.
    pub dram_misses: u64,
    /// Block lookups answered by the disk tier (after a DRAM miss).
    pub disk_hits: u64,
    /// Block lookups that missed the disk tier too.
    pub disk_misses: u64,
    /// Block lookups answered by a peer node (always 0 while N=1).
    pub peer_hits: u64,
    /// Peer fetches that came back a clean miss (owner lacked the block).
    pub peer_misses: u64,
    /// Peer fetches that failed at the transport (timeout, refused, auth).
    pub peer_errors: u64,
    /// Blocks this node served to peers out of its own cache (donor side).
    pub peer_served_blocks: u64,
    /// Bytes this node served to peers (#46 intra-pod NIC amplification).
    pub peer_served_bytes: u64,
    /// Bytes served to clients out of DRAM-tier blocks.
    pub dram_bytes_served: u64,
    /// Bytes served to clients out of disk-tier blocks.
    pub disk_bytes_served: u64,
    /// Bytes served to clients out of peer-fetched blocks (0 while N=1).
    pub peer_bytes_served: u64,
    /// Bytes served to clients straight out of backend-filled blocks.
    pub backend_bytes_served: u64,
    /// Backend GETs issued to fill blocks (a warm-leg increase over the cold
    /// leg means eviction forced refills — the constrained-profile evidence).
    pub backend_fills: u64,
    /// Bytes fetched from the backend by successful block fills.
    pub backend_fill_bytes: u64,
    /// Backend HEADs issued to resolve object metadata.
    pub backend_heads: u64,
    /// Requests served passthrough because the origin reported no ETag.
    pub non_cacheable_passthroughs: u64,
    /// Metadata-store (#50) lookups answered from the pinned store — the
    /// planning-stays-warm evidence: this stays ~100% of metadata reads even as
    /// data blocks churn.
    pub meta_hits: u64,
    /// Metadata-store lookups that missed and filled from the backend.
    pub meta_misses: u64,
    /// Bytes served to clients out of the pinned metadata store.
    pub meta_bytes_served: u64,
    /// Bytes currently held by demoted (retired, grace-pending) objects
    /// (#305): the dead share of the resident cache. A gauge — demotion adds,
    /// the hard-eviction sweep subtracts.
    pub retired_bytes_pending: u64,
    /// Bytes physically reclaimed by the grace-window hard-eviction sweep
    /// (#305). Monotonic since boot.
    pub retired_bytes_reclaimed: u64,
    /// Retired files physically reclaimed by the sweep (#305). Monotonic.
    pub retired_files_reclaimed: u64,
}

/// Cache config and live read-path counters returned by `GET /admin/stats`.
///
/// `dram_usage_bytes` is the live DRAM-tier occupancy: when it is a small
/// fraction of the working set yet warm reads still land, the reads were served
/// from disk, not DRAM (the nvme-resident proof).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsInfo {
    /// The server's configured cache budgets.
    pub cache: CacheConfigInfo,
    /// The live read-path counters.
    pub counters: CountersInfo,
    /// Bytes currently resident in the DRAM tiers (block tier + metadata),
    /// live and reclaimable together — the hard-ceiling gauge foyer enforces.
    pub dram_usage_bytes: u64,
    /// Of `dram_usage_bytes`, the bytes resident under the *current* cache
    /// generation — the usable working set. After a purge this reads ~0 even
    /// though `dram_usage_bytes` still reflects the physically resident
    /// (now-reclaimable) bytes, so a repeat-cold benchmark reads truth at T=0.
    #[serde(default)]
    pub dram_live_bytes: u64,
    /// Of `dram_usage_bytes`, the bytes resident under a *superseded*
    /// generation — logically purged, physically resident until LRU reclaim.
    #[serde(default)]
    pub dram_reclaimable_bytes: u64,
    /// Eager metadata warming progress (#168); `None` when no catalog is
    /// watched (nothing to warm). Absent-field-tolerant so an older client can
    /// still read a newer server's stats.
    #[serde(default)]
    pub warming: Option<WarmingInfo>,
    /// Write-back tier counters (#180); `None` when the tier is disabled. Makes
    /// the quorum-vs-write-through ack split and the propagation lag observable.
    #[serde(default)]
    pub writeback: Option<WritebackStatsInfo>,
}

/// Write-back tier counters (#180), surfaced by `GET /admin/stats`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WritebackStatsInfo {
    /// Writes acked after a fragment quorum landed on distinct live nodes.
    pub acked_via_quorum: u64,
    /// Writes acked by the synchronous write-through fallback (degraded mode).
    pub acked_via_write_through: u64,
    /// Transitions between write-back and write-through mode.
    pub mode_transitions: u64,
    /// Objects fully propagated to the origin and marked clean.
    pub propagated: u64,
    /// Propagation attempts that failed and will be retried.
    pub propagation_failures: u64,
    /// Fragments re-encoded and re-placed by node-loss repair or scrubbing.
    pub fragments_repaired: u64,
    /// Fragments checked by the background scrubber.
    #[serde(default)]
    pub fragments_scrubbed: u64,
    /// Fragments the scrubber found corrupt (present but failing their checksum).
    #[serde(default)]
    pub corrupt_fragments_found: u64,
}

/// Eager metadata warming progress (#168), surfaced by `GET /admin/stats`.
///
/// A watched table's `metadata.json`, manifest list, manifests, and Parquet
/// footers are pulled into the pinned store on onboarding and on every commit.
/// These counters mirror the warmer's `WarmProgress`; once `footers_warmed`
/// covers a table's files, a cold planning walk hits the store for zero backend
/// GETs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmingInfo {
    /// Tables a warming walk has begun.
    pub tables_started: u64,
    /// Tables a warming walk has finished.
    pub tables_completed: u64,
    /// Data files inspected (of any format).
    pub files_seen: u64,
    /// Data files that were Parquet (footer candidates).
    pub parquet_files: u64,
    /// Block-metadata objects warmed (metadata.json, manifest lists, manifests).
    pub block_objects_warmed: u64,
    /// Parquet footers pinned.
    pub footers_warmed: u64,
    /// Total footer bytes pinned.
    pub footer_bytes_warmed: u64,
    /// Backend GETs issued warming footers (speculative + refetches).
    pub footer_gets: u64,
    /// Footers that needed a second read (footer larger than the window).
    pub footer_refetches: u64,
    /// Non-Parquet data files skipped (never suffix-read — the Avro rule).
    pub skipped_non_parquet: u64,
    /// Footers skipped because a run hit its byte budget.
    pub skipped_over_budget: u64,
    /// Times a table's footprint exceeded the metadata budget.
    pub budget_alerts: u64,
}

/// One pod member as reported by `GET /admin/members` (issue #27). The wire
/// mirror of the cluster agent's `NodeMeta`, with addresses rendered as strings
/// so the admin surface carries no socket types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberInfo {
    /// The member's pod-wide identity.
    pub node_id: String,
    /// The member's restart generation (a restarted node outranks its stale
    /// record).
    pub generation: u64,
    /// The address the member gossips on.
    pub gossip_addr: String,
    /// The address peers fetch owned bytes from (#29), if advertised.
    pub advertise_addr: Option<String>,
    /// The member's loopback admin API address, if advertised.
    pub admin_addr: Option<String>,
    /// Advertised capacity in bytes — the member's rendezvous weight.
    pub capacity_bytes: u64,
    /// Lifecycle state (`active`/`donor`/`takeover`/`draining`/`dead`).
    pub state: String,
}

/// The gossip membership snapshot returned by `GET /admin/members` (issue #27):
/// this node's identity and pod, the current ring epoch, and its live view of
/// every pod member. Agreement across nodes (same members, same owners) is what
/// the multi-node acceptance test checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembersInfo {
    /// The identity of the node answering the probe.
    pub node_id: String,
    /// The pod (gossip cluster) this node belongs to.
    pub pod_id: String,
    /// The ring epoch this snapshot corresponds to (bumps on membership change).
    pub epoch: u64,
    /// The live member set from this node's point of view.
    pub members: Vec<MemberInfo>,
}

/// The `POST /admin/drain` request body (issue #31). Empty JSON (`{}`) is valid
/// — every field is optional and defaults — so `verglas drain` with no flags
/// takes the server's configured drain timeout.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainRequest {
    /// Maximum seconds to keep serving as a donor before exiting, regardless of
    /// whether the shed keys have been re-owned warm (`verglas drain
    /// --timeout`). `None` takes the server's configured default.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// The `POST /admin/drain` acknowledgement (issue #31): the drain was initiated
/// and the node is now `draining`. The node exits asynchronously once its keys
/// are re-owned warm or `timeout_secs` elapses — the caller polls
/// `GET /admin/members` to watch it leave and the ring rebalance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainAck {
    /// The node that entered the draining state.
    pub node_id: String,
    /// The lifecycle state now gossiped (`"draining"`).
    pub state: String,
    /// The effective donor window in seconds before the node exits.
    pub timeout_secs: u64,
}

/// The `POST /admin/log` request body (issue #61): the new verbosity filter, in
/// `RUST_LOG` syntax (e.g. `debug`, or `verglas_cache=debug,info`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLevelRequest {
    /// The filter to apply from now on.
    pub level: String,
}

/// The `POST /admin/log` acknowledgement (issue #61): the filter now in effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLevelInfo {
    /// The active verbosity filter after the reload.
    pub level: String,
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_ADMIN_ADDR, DEFAULT_ENDPOINT};
    use crate::config::Listen;

    /// The compiled-in admin defaults must agree with the documented config
    /// default (`[listen] admin_port`), or a default-config server and a
    /// default CLI talk past each other.
    #[test]
    fn admin_defaults_match_listen_default_admin_port() {
        let expected_addr = format!("127.0.0.1:{}", Listen::default().admin_port);
        assert_eq!(DEFAULT_ADMIN_ADDR, expected_addr);
        assert_eq!(DEFAULT_ENDPOINT, format!("http://{expected_addr}"));
    }
}
