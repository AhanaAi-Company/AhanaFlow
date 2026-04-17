# Production Readiness Report — Branch 33 Event Streams

**Date:** April 16, 2026  
**Databases:** UniversalStateServer (port 9633), VectorStateServerV2 (port 9644)  
**Result:** Approved for controlled deployment

## Executive Summary

Branch 33 is ready for controlled deployment with a narrow, honest public performance boundary.

1. UniversalStateServer is strongest on compressed durable state, compact fast-mode pipelined KV, and WAL storage efficiency.
2. VectorStateServerV2 is packaged as a bounded controlled-deployment vector lane, not as a broad fast-ANN product claim.
3. The Branch 33 validation sweep is green in the source branch, and the public wording in this repo is aligned to that approved evidence packet.

## Buyer-Safe Performance Boundary

### UniversalStateServer

1. Compact fast-mode mixed load: `47,642.31 req/sec` at `1.884 ms` p99
2. Compact fast-mode pipelined KV: `295,183.83 ops/sec`, `1.157× Redis` versus managed no-persistence Redis
3. Compact fast-mode WAL footprint: `3.42× smaller` than Redis AOF
4. Async RESP pipelined KV: `155,096.15 ops/sec`, `0.695× Redis`; this is the compatibility-first lane, not the performance lead lane

### VectorStateServerV2

1. Approved selective vector lane: `10K` corpus, `M=12`, `ef_construction=32`, `ef_search=24`, filtered 8-bucket query, pipeline depth `2`
2. Frozen selective result: `46.20 ms` p99 with fallback count `0`
3. Round 10 confirmation result: `46.97 ms` p99 with fallback count `0`; this confirms the lane but does not widen the claim
4. Mixed append/query maintenance story: about `33x+` relief on the bounded maintenance lane
5. April 9 proof remains valid only when stated narrowly:
   - `82K vectors/s` insert throughput
   - `32,036 docs/s` RAG ingest
   - integrity verification on the benchmark packet

## Approved Claims

1. AhanaFlow provides durable compressed state and integrated vector operations in one runtime.
2. UniversalStateServer is controlled-deployment ready on the compact fast-mode packet.
3. VectorStateServerV2 v1.0 is controlled-deployment ready on the approved bounded vector lane.
4. Insert and ingest proof are public, but they are not substitutes for concurrent network ANN latency claims.

## Blocked Claims

1. Blanket Redis replacement messaging across all modes
2. Blanket Redis parity on every wire and durability lane
3. Broad fast-ANN marketing
4. Any statement that April 9 insert or ingest numbers prove fast ANN query latency
5. Any statement that Round 10 created a broader new vector-performance packet

## Deployment Guidance

Use AhanaFlow when:

1. One runtime for retained state plus vector retrieval matters more than absolute peak throughput on every lane
2. WAL storage efficiency and simpler control-plane packaging matter
3. The current controlled-deployment latency envelope is acceptable

Do not oversell AhanaFlow when:

1. Redis-class pipelined throughput is mandatory on every wire mode
2. Low-latency concurrent network ANN is mandatory across broader scales
3. Frequent live ANN rebuilds are central to the workload

## Canonical Supporting Docs

1. `docs/VECTOR_STATE_SERVER_V2_CLAIM_BOUNDARY.md`
2. `website/index.html`
3. `README.md`