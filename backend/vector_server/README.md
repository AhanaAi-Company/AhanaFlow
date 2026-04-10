# AhanaFlow Vector V2

This package is a separate vector-capable runtime for Branch 33. It does not modify the existing compressed state engine or the existing universal server command surface.

Current scope:

1. Named vector collections with fixed dimensions
2. Exact search using cosine or dot-product scoring over contiguous NumPy float32 matrices
3. Packed float32 persistence inside a replay-safe append-only compressed WAL
4. Per-item metadata, payload, and optional TTL
5. Separate TCP command surface so current benchmarks remain clean

Current commands:

1. `PING`
2. `VECTOR_CREATE`
3. `VECTOR_LIST`
4. `VECTOR_UPSERT`
5. `VECTOR_GET`
6. `VECTOR_DELETE`
7. `VECTOR_BUILD_ANN`
8. `VECTOR_QUERY`
9. `VECTOR_COMPACT`
10. `VECTOR_STATS`

Query API notes:

1. `VECTOR_QUERY` now accepts `strategy="exact"` or `strategy="ann_rerank"`
2. `candidate_multiplier` controls how many ANN candidates are gathered before exact rerank
3. `ann_probe_count` controls how many centroid buckets are probed during ANN candidate selection
4. `VECTOR_BUILD_ANN` builds or refreshes the in-memory ANN routing index for a collection
5. `VECTOR_COMPACT` rewrites the WAL from live records only and reports reclaimed bytes

Benchmark entrypoint:

1. `vector_server.benchmark.run_vector_benchmark()` for a single formulation experiment
2. `vector_server.benchmark.run_vector_formulation_matrix()` for multi-dimension experiments
3. `vector_server.benchmark.run_vector_acp_compression_matrix()` for lossless ACP codec testing on packed vector segments
4. `vector_server.benchmark.run_vector_acp_compression_suite()` for parallel ACP codec testing across multiple vector sizes and dimensions
5. Current runnable formulations and baselines: legacy JSON exact scan, packed float32 exact scan, NumPy exact scan, and `hnswlib` when installed

Current recommendation:

1. Treat packed float32 plus exact rerankable search as the canonical tier for local AI memory
2. Treat any later ANN or quantized path as an accelerator, not the only stored truth
3. For compacted warm or cold vector segments, use `float32_shuffle + zstd-22` as the default ACP compression profile unless a narrower corpus proves a better lossless winner

This branch is still an MVP, but it now uses the first Ahana-aligned vector formulation: exact float32 storage without JSON float-array overhead, plus vectorized exact scoring suitable for local RAG memory and semantic retrieval experiments.

## Current Measured Boundary

Latest validated live results:

1. Heavy insert pass: `4,000` expected vectors inserted, `4,000` actual vectors stored, `0` insert errors.
2. Insert throughput: `3,344.5 vec/sec` on the live threaded TCP path.
3. HNSW build time: `14.813 s` for the 4K-vector operational rebuild.
4. Concurrent ANN query load: `654.46 qps`, `23.81 ms` p50, `37.094 ms` p95, `38.047 ms` p99, `0` query errors.
5. Benchmark recall reporting now uses brute-force ground truth rather than placeholder recall values.

Current guidance:

1. Treat exact search plus measured ANN recall as the truthful product surface.
2. Keep broad ANN marketing claims gated on the higher-scale marketable summary report.
3. Do not describe the current networked concurrent query path as fast ANN production performance yet; correctness is strong, but latency and rebuild cost are still active work.
