# VectorStateServerV2 Claim Boundary

This memo is the canonical public claim boundary for the `deploy_to_github` Branch 33 package.

## Public Packaging Status

VectorStateServerV2 is **v1.0 controlled-deployment ready**.

That means:

1. The runtime is test-backed and replay-safe.
2. The approved public vector story is narrow and artifact-backed.
3. Public copy should freeze here until a stronger higher-scale packet is reproduced and reviewed.

## Approved Public Claims

1. Approved selective vector lane: `10K` corpus, `M=12`, `ef_construction=32`, `ef_search=24`, filtered 8-bucket query, pipeline depth `2`
2. Frozen selective result: `46.20 ms` p99 with fallback count `0`
3. Round 10 confirmation: `46.97 ms` p99 with fallback count `0`, which confirms the lane but does not widen the claim
4. Bounded mixed append/query story: about `33x+` maintenance relief on the approved maintenance lane
5. April 9 artifacts remain valid for insert and ingest packaging when stated narrowly:
   - `82K vectors/s` insert throughput
   - `32,036 docs/s` RAG ingest
   - `100%` integrity verification on the benchmark packet

## Blocked Public Claims

1. Broad fast-ANN marketing
2. General concurrent network ANN latency claims beyond the approved selective lane
3. Any statement that April 9 insert or ingest numbers prove fast ANN query latency
4. Any statement that Round 10 produced a new stronger approval-safe vector packet
5. Any statement that ACP/WAL produced a packaged vector-performance win on this branch; ACP/WAL remains observational only in the Round 10 packet

## Canonical Evidence Files

1. `reports/vector_hotspots_10k_round9_client_pipelining_summary.json`
2. `reports/vector_hotspots_10k_round10_line_drain_summary.json`
3. `reports/ahanaflow_benchmarks_2026_04_09.ipynb`

## Public Wording Guidance

Use language like:

1. "sub-50 ms selective vector lane"
2. "fallback 0 on the approved bounded lane"
3. "integrated vector operations"
4. "controlled-deployment ready"
5. "insert and ingest proof kept separate from concurrent ANN latency claims"

Avoid language like:

1. "fast ANN"
2. "broad vector performance lead"
3. "perfect recall at production scale"
4. "replaces Pinecone/Qdrant on speed"
5. "sub-3 ms vector latency"