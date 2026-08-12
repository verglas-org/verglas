# Engineering objective

RIME separates eligibility from ranking:

- Hard gates and numeric constraints decide whether a candidate may win.
- Named measured metrics produce normalized utilities used for scalar AIDE ranking.

Every objective includes the software gates for task correctness, no dead code, no unrequested fallbacks, architectural fit, and semantic rigor. Add domain gates such as held-out retrieval quality or output equivalence when needed. A failed gate or constraint makes the candidate buggy and eligible for bounded debugging; a high scalar score never overrides it.

## Metrics

Declare every metric with:

- A stable name and unit.
- `maximize` or `minimize` direction.
- A non-negative weight. Use zero for a constraint-only measurement.
- An explicit worst-to-best normalization scale.
- Accepted independent evidence kinds.

Linear utility maps `worst` to 0 and `best` to 1 and clamps values outside that interval. Use a positive logarithmic scale when equal ratios should have equal utility across orders of magnitude, such as cost or latency spanning 1–1000. The scalar reward is the weighted mean of metric utilities. Choose scales from requirements, capacity limits, or a frozen baseline and target—not from the current candidate batch. Changing scales during search makes scores incomparable.

Do not add milliseconds, dollars, recall ratios, and bytes directly. Preserve raw values and units beside normalized utility so results remain interpretable.

## Constraints

Use numeric constraints for service-level and resource limits, such as:

- `p95Latency <= 500 ms`
- `peakMemory <= 2 GiB`
- `costPerQuery <= 0.02 USD`
- `retrievalRecallAt10 >= 0.85`

Do not hide a required threshold inside a low metric weight. A requirement is a constraint; a trade-off is a weighted metric.

## Benchmark protocol

Freeze the evaluator before search:

1. Establish the baseline on representative inputs and target hardware.
2. Use external ground truth, upstream tests, or frozen baseline outputs for correctness. Never compare a candidate to its own output.
3. Separate warmup from timed iterations. Run enough repetitions to expose variance and report the aggregation used, such as median or p95.
4. Reset caches and state when the production workload does not reuse them. Preserve realistic caches when it does.
5. Reserve held-out cases for retrieval, model, prompt, or heuristic systems. Do not select candidates on the same examples used to author them.
6. Keep hardware, concurrency, dataset, dependency versions, and measurement boundaries fixed across candidates.
7. Store raw benchmark artifacts outside the graph and persist their identities and summaries with each measurement.

Reject objectives that can be gamed by skipping work, returning cached constants, shrinking the evaluated dataset, weakening correctness tolerance, or moving work outside the measured boundary.

## Examples

For database query performance, keep result equivalence and software-quality gates hard. Rank p95 latency and throughput, optionally include CPU or cost, and constrain peak memory and error rate.

For graph RAG, gate on held-out correctness and provenance. Rank retrieval recall, answer faithfulness, and coverage while constraining p95 latency and cost per query. Keep ingestion quality and online query quality as separate metrics rather than one opaque judge score.

The default coding profile is itself a general objective: maximize task and architectural utility while minimizing change-surface utility, after every software gate passes.
