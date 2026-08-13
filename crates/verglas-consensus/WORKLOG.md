# Worklog

- #135: Defined the executable acceptance contract for coded-entry quorum
  intersection, complete-entry degraded operation, elections, retry identity,
  writer fencing, linearizable reads, and joint membership changes before the
  consensus implementation exists.

- #135: Implemented the deterministic coded-Raft safety model used by the
  protocol contract. It records immutable entry decisions, proves coded writes
  against successor-quorum intersection, and refuses unsafe retries, writer
  epochs, reads, and membership transitions.

- #135: Tightened group geometry validation so every configured Reed-Solomon
  threshold can be reconstructed by a legal future Raft majority. Added a
  non-unanimous seven-voter case that executes the general intersection bound.

- #135: Defined restart acceptance tests for fsynced per-voter headers and
  coded payloads. Recovery must reconstruct committed data and preserve exact
  retry identity using only a legal successor election quorum.

- #135: Added durable per-voter entry files with canonical checksummed headers,
  fragment checksums, atomic replacement, and fsync boundaries. Recovery now
  requires quorum header agreement, reconstructs only verified fragments, and
  rebuilds retry identities before accepting a new leader.

- #135: Defined the remaining durable universal API for complete-entry commits
  and writer-lease recovery. These tests ensure degraded operation and Neon
  fencing use the same persisted consensus log rather than a second protocol.

- #135: Added the real Raft command boundary: OpenRaft replicates only immutable
  entry headers and validated payload certificates. Large WAL and object bytes
  remain in the staged EC plane and become visible only at a Raft commit index.

- #135: Added OpenRaft's complete storage conformance suite as the acceptance
  gate for persistent terms, votes, logs, commit indexes, membership, state
  application, truncation, purge, and snapshots.

- #135: Added a real five-node Raft partition test with persistent stores. A
  majority elects a replacement and commits while the isolated former leader
  cannot assign or commit a conflicting log index.

- #135: Added fsynced payload staging separate from the small Raft header log.
  Coded and complete representations now yield a commit certificate only after
  the configured successor-quorum intersection is durably satisfied.

- #135: Made durable payload staging continue through the full committed voter
  allocation when an ingress-preferred holder is unavailable. The resulting
  immutable certificate records only successful fsynced holders and still
  requires the coded intersection threshold or a complete-entry majority.

- #135: Moved exact retry identity and writer-epoch validation into the durable
  Raft state machine. Duplicate requests return their original index, while
  conflicting retries and stale writers apply as closed failures.

- #135: Added the sole leader-facing group API that stages payload durability
  before Raft submission and fences reads through the current leader quorum.
  Callers can no longer manufacture a committed result from fragment placement.

- #135: Added committed WAL range, writer-lease, and verified archive-watermark
  transitions to the real Raft state machine. Timeline leaders now reject stale
  epochs, gaps, and unsafe archive advancement before returning acknowledgement.

- #135: Added typed authoritative catalog batches with atomic optimistic
  requirements, namespace changes, and table-pointer changes. Linearizable
  catalog reads now fence through Raft without a PostgreSQL transaction owner.

- #135: Added committed WAL range reads over reconstructed coded entries. Reads
  require a current-term quorum fence and a minimum applied-index watermark, so
  isolated or lagging ingresses cannot expose uncommitted timeline bytes.

- #135: Added an explicit committed writer release transition. A released epoch
  cannot append again, and only a later committed acquisition can reactivate the
  timeline under a strictly greater fence.

- #135: Abstracted durable payload staging and reconstruction behind an async
  transport-neutral contract. Production groups can now use cache-ring peer
  storage instead of the filesystem-only conformance implementation.
- #135: Added a universal routed command envelope for catalog and WAL groups.
  Any ingress can forward the same typed request to the observed leader while
  exact request identity keeps leadership-change retries idempotent.
- #135: Added fenced namespace and table listings to the authoritative catalog
  state machine. The native Iceberg REST adapter no longer needs a SQL database
  to answer discovery or current metadata-pointer reads.
- #135: Writer acquisition now returns the consensus-owned committed WAL tail
  with the new fence epoch. A replacement Neon writer therefore starts from
  authoritative timeline state instead of inferring the append position from
  local WAL or a former safekeeper donor.
- #135: Added a universal immutable-object publication command. Ring write-back
  commits its durable coded-placement certificate through an independent object
  group before returning an S3 acknowledgment.
- #135: Exposed the timeline's applied index with its linearizable WAL/archive
  boundaries. Range readers first fence on status and then request only exact
  committed bytes.

- #135: Exposed a linearizable WAL archive state and checkpoint-gated release
  boundary from the consensus state machine. Local WAL can now be released only
  through a committed archive watermark, never merely because an upload started.

- #135: Added a deterministic, current-term-fenced catalog export carrying its
  applied index. Lifecycle code can upload and verify this exact snapshot before
  recording its future checkpoint, without reading mutable catalog state locally.

- #135: Added tenant-root warehouse registrations as durable replicated state.
  A warehouse route is registered idempotently and survives state-machine
  restart, so catalog groups can only be selected through their tenant root.

- #135: Persisted the exact OpenRaft `LogId` beside each committed header. This
  gives post-commit representation sealing the authoritative term and index,
  rather than deriving or guessing them before Raft assigns the entry.
- #135: Added restart coverage for tenant-root warehouse routing and rejected conflicting registrations. The test proves that warehouse ownership survives state-machine recovery and cannot silently move between groups.
- #135: Added a committed timeline-open command that establishes the immutable initial PostgreSQL LSN before writer acquisition. Nonzero real-world WAL positions now survive restart and conflicting attempts to reopen a timeline at another LSN fail closed.

- #135: Added typed durable catalog collections for the complete hosted Lakekeeper
  domain. Canonical JSON records are transactionally guarded and applied with
  namespace/table pointers, so the future Lakekeeper adapter has one CRaft state
  image rather than a SQL fallback.

- #135: Exposed the voter set and its committed Raft-log generation from durable
  state-machine state. The membership test now catches up a learner before a
  joint and uniform voter transition, so placement code cannot use a desired or
  process-local configuration as authority.

- #135: Bound each payload certificate to its exact ordered voter allocation.
  Reconstruction validates fragment slots against the committed allocation, so
  a future membership repair has the information required to re-encode safely.

- #135: Added replicated payload repair certificates. A leader reconstructs and
  re-encodes every committed body before recording the target allocation, and
  reads resolve that committed repair map rather than transient staging.

- #135: Made an open consensus group advance its header generation from the
  committed uniform Raft membership. Historical reads retain their recorded
  generation while new writes use the replacement configuration.

- #135: Made distributed payload allocation live across a committed uniform
  voter change. Existing group handles now publish the new voter ordering before
  subsequent staging, instead of retaining startup-ring placement forever.
- #135: Replaced panic-only unwraps in the coded consensus and durable recovery acceptance tests with contextual assertions. This keeps the full workspace lint gate strict while preserving the same protocol coverage.
- #135: Bound every local durable representation to its group, configuration,
  mode, hash, request, slot, length, and committed Raft term/index. Equal
  payload bodies from distinct requests now occupy separate fsynced records,
  and reconstruction rejects any allocation or seal mismatch.
- #135: Gave repaired allocations their own committed configuration generation
  and repair-command log identity. Recovery resumes partially sealed committed
  allocations idempotently, while failed repair proposals leave the source
  generation readable and untouched.
- #135: Added a fail-first acceptance test for catalog checkpoint compaction.
  The frozen test requires a snapshot to discard checkpointed catalog headers
  while retaining the materialized catalog state and exact retry identity.

- #135: Made catalog checkpoint compaction part of durable snapshot creation.
  Snapshots now remove only checkpointed Catalog headers and their log and repair
  records, while retaining materialized state and request identity for retries.
- #135: Added a fail-first cache synchronization test for catalog compaction.
  The frozen evaluator requires snapshots to release checkpoint-covered internal
  payload representations while retaining unrelated cached consensus bodies.

- #135: Snapshot compaction now releases every exact catalog payload allocation
  before pruning its header metadata. Local durable replicas and distributed
  transports validate the group, generation, request, hash, certificate slot,
  length, and committed log identity, while missing holder files remain an
  idempotent release. A failed release keeps the catalog history intact for a
  later retry.

- #135: Added a fail-first follower snapshot-install reclamation test. It
  requires replicas receiving an already-compacted leader snapshot to release
  their own superseded catalog payloads before installing the state image.

- #135: Made follower snapshot installation reclaim local catalog payload
  representations that the authoritative compacted image has pruned. The local
  headers and repair records supply exact release identities, and any release
  failure now aborts before either persistent state image changes.
- #135: Added a real Raft regression for persisting typed Lakekeeper catalog records. The test reproduces the JSON state-machine failure seen by a four-node Docker cluster before exercising a linearizable read.

- #135: Replaced compound hosted-record map keys with a typed collection of
  entity-keyed record maps. This keeps catalog record lookups and deterministic
  ordering intact while making every persisted state-machine image valid JSON.

- #135: Made distributed payload reconstruction continue past unavailable and
  absent certified holders, then require one valid complete copy or `k` valid
  coded shards. Returned representations still require their exact committed
  allocation identity, so a mismatched peer response fails closed.

- #135: Validated certified payload holders in parallel and retained committed
  prefixes with a bounded sixteen-entry concurrency during leader readiness.
  Every completed holder response is still identity-checked before service, so
  a dead former leader no longer makes catalog recovery exceed its request
  deadline without weakening corruption checks or resource ceilings.
- #135: Stage every committed voter concurrently and acknowledge only after
  the existing coded or complete durability threshold fsyncs. This keeps the
  committed configuration and certificate unchanged while a dead preferred
  holder can no longer exhaust the foreground command deadline.
- #135: Persist each committed WAL response boundary with its retry identity.
  Exact retries now return the original index and WAL end, while conflicting
  retry identities remain closed without exposing another command's result.
- #135: Moved durable Raft log, state-machine, and snapshot image writes to
  Tokio's blocking pool. Per-replica persistence ordering remains serialized,
  and every OpenRaft callback still waits for the temp-file, rename, and
  directory-fsync barrier before it reports success.
