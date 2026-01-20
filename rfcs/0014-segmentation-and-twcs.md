# SlateDB RFC: Segmentation & Sealing

Status: Draft

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes introducing a *segmentation* abstraction into SlateDB. Segmentation allows users to define logical groupings of data and communicate expected access patterns to the engine — enabling optimizations for compaction and query execution. Segments can optionally be *sealed* to indicate immutability, which enables further optimizations such as reduced write amplification. Use cases include time-window compaction and multi-tenant isolation.

## Motivation

LSM compaction continuously rewrites data to maintain sorted structure. For append-only workloads, this creates unnecessary write amplification — old data that will never be updated is repeatedly rewritten alongside newer data.

**Time-window compaction (TWCS)** addresses this for time-series workloads. Data is partitioned into time windows (e.g., hourly or daily). Once a window closes, it is *sealed* — no new writes will arrive. The engine can compact sealed windows to a stable form and stop rewriting them, bounding write amplification for cold data.

Not all workloads align naturally with time boundaries. Log systems, for example, may organize data by size or offset ranges rather than time. The [OpenData log](https://github.com/opendata-oss/opendata/tree/main/log) segments data this way — sealing a segment when it reaches a target size. The common pattern is that the application knows when a logical group of data is complete, and the engine can use this information to bound compaction.

This suggests a generalization: if the engine supported *logical segmentation*, it could be used as the basis for time or size-based strategies. It might also open the path to new use cases. One speculative example is multi-tenant isolation. Queries typically target a single tenant, so segment metadata could help the engine skip irrelevant sorted runs — even if tenant segments are never sealed. More broadly, segmentation provides a way for applications to communicate access patterns and lifecycle hints that the engine can exploit.

## Goals

- Provide a general *segmentation* abstraction that supports a variety of access patterns and lifecycle policies.
- Enable segment-directed query execution for workloads that can identify relevant segments.
- Allow segments to be optionally *sealed*, signaling immutability and enabling reduced write amplification.
- Support monotonic sealing (a watermark) as an optimization for ordered segmentation policies (e.g., time-based, offset-based).
- Enable TWCS-style behavior (time-window segmentation + sealing + compact-to-final) as one policy built on top of segmentation.

## Design

The design is TBD. At a high level, the following functionality is needed:

1. **Segment assignment**: An API to identify the segment for each new write. The caller (or a configured policy) provides the segment ID based on record metadata (timestamp, tenant, offset, etc.).

2. **Segment-directed queries**: Hooks into the query path to limit the scope of a scan by segment, skipping sorted runs that don't intersect the target segment.

3. **Segment-aware compaction**: Hooks into compaction to avoid mixing data across segment boundaries. For sealed segments, compact to a stable form and stop rewriting.

4. **Sealing**: An API to mark a segment as immutable. Supports both per-segment sealing and watermark-based sealing for ordered policies.

5. **Segment-based retention**: Drop entire segments when they fall out of a retention window.

## Alternatives

- **Engine-defined time windows**: rejected as too prescriptive for non-time segmentation.

## Open Questions

- What is the API shape for segmentation (config-only, traits, extension points)?
- Do we require SSTables to be single-segment?
- For policies that use sealing: how should writes to sealed segments be handled (reject, redirect, lateness horizon)?
- What is the compaction endpoint for a sealed segment (single SSTable, single per level)?
- How is segment metadata tracked in the manifest? Is there a scalability concern for policies with many segments?

## References

- [CASSANDRA-9666](https://issues.apache.org/jira/browse/CASSANDRA-9666) — Cassandra TWCS design context
