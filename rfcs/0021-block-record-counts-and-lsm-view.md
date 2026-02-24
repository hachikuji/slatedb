# Block Record Counts and LSM Level Access

Table of Contents:

<!-- TOC start -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  * [SST Format Changes](#sst-format-changes)
    + [Block Record Counts in `BlockMeta`](#block-record-counts-in-blockmeta)
    + [Total Record Count in `SsTableInfo`](#total-record-count-in-sstableinfo)
  * [LSM Level Access API](#lsm-level-access-api)
    + [`LsmView`](#lsmview)
    + [`LsmLevel`](#lsmlevel)
    + [Accessing the `LsmView`](#accessing-the-lsmview)
  * [Level-Scoped Counting](#level-scoped-counting)
  * [Level-Scoped Scanning](#level-scoped-scanning)
  * [Implementation Phases](#implementation-phases)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
  * [Performance & Cost](#performance--cost)
  * [Observability](#observability)
  * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes two related changes:

1. Adding cumulative record counts to `BlockMeta` entries in the SST block index and a total record count to `SsTableInfo`, enabling efficient record counting within key ranges at the index level.
2. Introducing an `LsmView` API that exposes the LSM level structure from `Db` and `DbReader`, allowing users to perform per-level counting and scanning without cross-level merging.

## Motivation

SlateDB's current public API treats the LSM tree as an opaque structure. Users can scan key ranges and get merged results, but cannot reason about individual levels or count records without reading every entry. RocksDB, Pebble, and the systems built on them have all added APIs to address this — SlateDB needs similar capabilities.

### Record Counting

[OpenData-Log](https://github.com/opendata-dev/log) stores append-only log entries in SlateDB using composite keys (`key + sequence_number`). Computing "lag" (the number of unread records for a key) requires counting records in a sequence range. Today this requires a full scan. With cumulative block counts in the SST block index, a count can be computed with at most two block reads per SST (for the boundary blocks), and the rest derived from index arithmetic.

Record counting is useful beyond log-structured workloads. MyRocks uses RocksDB's `GetApproximateSizes` to implement MySQL's `records_in_range()`, which the query optimizer uses to choose between index scans and full table scans. TiKV uses per-SST table properties for region split decisions. CockroachDB uses Pebble's `EstimateDiskUsage` for range split/merge decisions.

These systems estimate at SST granularity — boundary SSTs are counted in full. Cumulative block counts offer finer granularity, narrowing the estimate to within two blocks of the true count even for ranges that span a small portion of a large SST.

### Compaction Scheduling

Custom `CompactionScheduler` implementations receive `SsTableHandle` metadata via `CompactorStateView`, but `SsTableInfo` currently has no record count — only a rough size estimate derived from byte offsets. Adding a total record count to `SsTableInfo` gives schedulers a direct measure of SST "weight" without additional I/O. TiKV solves a similar problem via RocksDB's `TablePropertiesCollector` callback; including record counts in SlateDB's standard SST metadata avoids such custom machinery.

### Level-Scoped Iteration

For append-only workloads, newer entries are always in higher LSM levels. A consumer reading the "tail" of a log can iterate only the newest levels and stop early. SlateDB's current scan API always merges across all levels, which is unnecessarily expensive for these access patterns.

Level-scoped access has precedent. CockroachDB uses Pebble's `ScanInternal` to walk the LSM level-by-level during disaggregated storage replication, and `ScanStatistics` for per-level MVCC garbage estimation. Neither system exposes per-level iteration as a user-facing read-path primitive, which is what this RFC proposes.

## Goals

- Add cumulative record counts to `BlockMeta` in the SST block index
- Add a total record count to `SsTableInfo`
- Expose an `LsmView` API from `Db` and `DbReader` that provides per-level counting and scanning
- Ensure backward compatibility with existing SSTs (missing counts treated as unavailable)

## Non-Goals

- Cross-level merged counting (i.e. deduplicating keys across levels) — this requires merge iteration and is not cheaper than a scan
- Modifying the compaction scheduler trait — record counts in `SsTableInfo` are automatically available to schedulers through the existing `CompactorStateView`
- Exposing memtable-level counts through `LsmView` — memtables are bounded in size and already covered by `StatRegistry` metrics (see RFC 0020)
- Bloom filter changes (e.g. keying on a user-defined prefix) — left for future work

## Design

### SST Format Changes

#### Block Record Counts in `BlockMeta`

Add a `cumulative_record_count` field to `BlockMeta` in `sst.fbs`:

```flatbuffers
table BlockMeta {
    // Offset of the block within the SST file.
    offset: ulong;

    // First key contained in the block.
    first_key: [ubyte] (required);

    // Cumulative record count: total number of records in this block
    // and all preceding blocks in the SST. The first block's cumulative
    // count equals its own record count.
    cumulative_record_count: ulong;
}
```

The cumulative count for block `i` is the sum of record counts for blocks `0..=i`. This enables computing the record count for any contiguous range of blocks with a single subtraction:

```
count(blocks[i..j]) = cumulative[j] - cumulative[i - 1]
```

The builder tracks a running total as it writes blocks. Since blocks are written sequentially, this adds negligible overhead to SST construction.

#### Total Record Count in `SsTableInfo`

Add a `record_count` field to `SsTableInfo` in `sst.fbs`:

```flatbuffers
table SsTableInfo {
    first_entry: [ubyte];
    index_offset: ulong;
    index_len: ulong;
    filter_offset: ulong;
    filter_len: ulong;
    compression_format: CompressionFormat;

    // Total number of records in the SST.
    record_count: ulong;
}
```

This is the cumulative count of the last block — no additional tracking is needed. It is immediately available through `SsTableHandle.info` in the manifest, giving compaction schedulers a direct record count without any I/O.

Both fields are new optional FlatBuffers fields, defaulting to `0`. The `LsmView` API returns `None` for counts when the underlying SST lacks record count data.

### LSM Level Access API

#### `LsmView`

An `LsmView` is a snapshot of the LSM level structure at a point in time. It holds the resources needed to perform I/O on the SSTs it references.

```rust
/// A snapshot of the LSM level structure, providing per-level
/// access to record counts and iteration.
pub struct LsmView {
    levels: Vec<LsmLevel>,
}

impl LsmView {
    /// Returns the levels ordered from newest to oldest.
    ///
    /// Each L0 SST is represented as its own level. Sorted runs
    /// follow L0, ordered from newest to oldest.
    pub fn levels(&self) -> &[LsmLevel] {
        &self.levels
    }
}
```

#### `LsmLevel`

Each `LsmLevel` represents either a single L0 SST or a sorted run.

```rust
/// A single level in the LSM tree: either one L0 SST or a sorted run.
pub struct LsmLevel {
    table_store: Arc<TableStore>,
    kind: LsmLevelKind,
}

enum LsmLevelKind {
    L0(SsTableHandle),
    SortedRun(SortedRun),
}

/// Metadata about a level.
pub struct LsmLevelMetadata {
    /// Total record count across all SSTs in this level.
    /// `None` if any SST in the level lacks record count data.
    pub record_count: Option<u64>,

    /// Number of SSTs in this level.
    pub sst_count: usize,
}

impl LsmLevel {
    /// Returns metadata about this level.
    pub fn metadata(&self) -> LsmLevelMetadata;

    /// Counts the number of records in the given key range within
    /// this level, using the block index. At most two blocks per
    /// SST are read (the boundary blocks); interior blocks are
    /// counted from the index alone.
    ///
    /// Returns `None` if any overlapping SST lacks record count data.
    pub async fn count(
        &self,
        range: impl RangeBounds<Bytes>,
    ) -> Result<Option<u64>, Error>;

    /// Like `count`, but uses only the block index without reading
    /// boundary blocks. The result may overcount by up to two
    /// blocks' worth of records per overlapping SST.
    pub async fn approximate_count(
        &self,
        range: impl RangeBounds<Bytes>,
    ) -> Result<Option<u64>, Error>;

    /// Returns an iterator over the entries in this level within
    /// the given key range. No cross-level merging is performed.
    ///
    /// For a sorted run, this merges across the SSTs within the run
    /// (which have non-overlapping key ranges, so this is a
    /// concatenation rather than a true merge).
    pub async fn scan(
        &self,
        range: impl RangeBounds<Bytes>,
    ) -> Result<DbIterator, Error>;
}
```

#### Accessing the `LsmView`

`LsmView` is available from both `Db` and `DbReader`:

```rust
impl Db {
    /// Returns a snapshot of the current LSM level structure.
    pub fn lsm_view(&self) -> LsmView;
}

impl DbReader {
    /// Returns a snapshot of the current LSM level structure.
    pub fn lsm_view(&self) -> LsmView;
}
```

The `LsmView` captures the current manifest state and holds `Arc<TableStore>` for I/O — the same ownership pattern used by `DbIterator`. SSTs referenced by the view are protected from garbage collection as long as the view is held.

### Level-Scoped Counting

For each overlapping SST within a level, the block index is read and binary-searched for the range start and end keys. Interior blocks (fully contained within the range) are counted from cumulative counts alone: `cumulative[last_interior] - cumulative[first_interior - 1]`. This requires one index read per SST (cacheable).

For exact counts, the two boundary blocks (first and last, which may be partially within the range) are read and their matching entries counted. This adds at most 2 block reads per overlapping SST. For approximate counts, boundary blocks are included in full without reading them, overcounting by at most 2 blocks' worth of records per SST.

### Level-Scoped Scanning

`LsmLevel::scan` returns a `DbIterator` over entries within a single level, without cross-level merging. For a sorted run, the SSTs have non-overlapping key ranges, so the iterator concatenates them in key order. Entries may include multiple versions of the same key or tombstones.

This enables the append-only scan optimization described in OpenData-Log's RFC: iterate levels newest-to-oldest, stopping when entries are older than the target sequence range.

### Implementation Phases

**Phase 1: SST format changes.**
- Add `cumulative_record_count` to `BlockMeta` in `sst.fbs`
- Add `record_count` to `SsTableInfo` in `sst.fbs`
- Update `EncodedSsTableBuilder` to track and write cumulative counts
- Update `SsTableFormat` to read cumulative counts from the index

**Phase 2: `LsmView` API.**
- Add `LsmView`, `LsmLevel`, `LsmLevelMetadata` types
- Implement `Db::lsm_view()` and `DbReader::lsm_view()`
- Implement `LsmLevel::count`, `approximate_count`, and `scan`

## Impact Analysis

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics — `LsmLevel::scan` returns a `DbIterator` for per-level iteration
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [x] Snapshots — `LsmView` is a point-in-time snapshot of the LSM structure
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection — `LsmView` holds references that prevent GC of referenced SSTs
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [x] Compaction strategies — `SsTableInfo.record_count` gives schedulers direct access to SST record counts
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [x] Block cache — `LsmLevel::count` and `scan` read index and data blocks through the cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata) — cumulative record counts added to block index
- [x] SST format or block format — new fields in `BlockMeta` and `SsTableInfo`

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc) — new `LsmView` API to expose in bindings
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

**SST format overhead:**
- `BlockMeta` grows by 8 bytes per block (one `ulong`). For a 256 MB SST with 4 KB blocks, this adds ~512 KB to the index — roughly doubling the index size. This is a meaningful increase and should be validated with benchmarks, but the index is typically a small fraction of the SST.
- `SsTableInfo` grows by 8 bytes per SST (one `ulong`). Negligible.

**Counting cost:**
- `LsmLevel::approximate_count`: one index read per overlapping SST (cacheable). No data block reads. O(log N) binary search per SST where N is the number of blocks.
- `LsmLevel::count`: one index read + at most 2 data block reads per overlapping SST.

**Scanning cost:**
- `LsmLevel::scan` has the same per-SST cost as a normal scan, but avoids the cross-level merge. For workloads that only need recent data, this can be significantly cheaper.

**Write path:**
- Negligible overhead. The builder maintains one running counter incremented per record.

### Observability

No new metrics or configuration. The `record_count` field in `SsTableInfo` is visible through existing manifest inspection tools (`Admin::read_manifest`).

### Compatibility

- New FlatBuffers fields default to `0`, so existing SSTs are readable without changes. Rolling upgrades are safe: the `LsmView` API handles a mix of old and new SSTs by returning `None` for counts when data is missing.
- No changes to existing public APIs.

## Testing

- Unit tests:
  - `EncodedSsTableBuilder` correctly writes cumulative counts and total count
  - `SsTableFormat` correctly reads cumulative counts from the index
  - Backward compatibility: SSTs without counts return `0` / `None`
  - `LsmLevel::count` returns exact counts for key ranges within a single SST
  - `LsmLevel::count` handles ranges spanning multiple SSTs in a sorted run
  - `LsmLevel::approximate_count` returns counts within expected bounds (exact count <= approximate <= exact + 2 blocks per SST)
  - `LsmLevel::scan` returns entries in key order within a level
  - `LsmView` ordering: L0 levels before sorted runs, newest first
  - Edge cases: empty range, range beyond SST bounds, single-block SSTs
- Integration tests:
  - End-to-end: write data, flush, compact, then verify counts match expected values through `LsmView`
  - `DbReader::lsm_view` returns consistent results
- Performance tests:
  - Benchmark `LsmLevel::count` vs full scan for varying range sizes
  - Measure index size overhead from cumulative counts

## Rollout

- Phase 1 (SST format changes) can be merged independently and has no API impact beyond `SsTableInfo` gaining a new field.
- Phase 2 (`LsmView` API) depends on Phase 1 and introduces new public types.
- Docs update with usage examples for counting and level-scoped scanning.

## Alternatives

**Per-block counts instead of cumulative counts.** Requires summing across all blocks in a range rather than a single subtraction. Cumulative counts are strictly more efficient for range queries.

**Stats block approach (RFC 0020).** RFC 0020 adds a separate stats block with `num_puts`, `num_deletes`, `num_merges`, and byte sizes. This is complementary — the stats block provides per-SST aggregates, while cumulative block counts enable sub-SST range counting.

**Expose counting through `SstReader`/`SstFile` (RFC 0020) instead of `LsmView`.** RFC 0020's `SstReader` is detached from a live `Db` — it takes an `ObjectStore` and opens SSTs independently. This works for offline inspection but is awkward for live counting: the caller must coordinate between `Db::manifest()` (to discover SSTs) and a separately-constructed `SstReader` (to read them), with GC races in between. `LsmView` avoids this by capturing a consistent snapshot from the live `Db`/`DbReader` with GC protection built in.

**`Db`-level `count` method.** A high-level `db.count(range)` could aggregate across levels internally. But the semantics are ambiguous — does it return raw records or deduplicated keys? For append-only workloads the answer is clear, but for general KV it's misleading. Exposing per-level counts lets callers decide how to aggregate.

## Open Questions

- Should `LsmLevel::scan` apply sequence number filtering (like `max_seq` in the normal scan path), or return all versions? Returning all versions is more flexible for append-only use cases, but diverges from the normal scan behavior.
- Should `LsmView` include memtable levels? This would give a complete picture but complicates the API since memtables are not SSTs and don't have block indexes for counting.
- What is the GC protection mechanism for `LsmView`? Options include incrementing a reference count on the manifest snapshot, creating an implicit checkpoint, or relying on the existing sequence tracker.
- How does `LsmView` interact with RFC 0020's `SstReader`/`SstFile`? Should `LsmLevel` expose the underlying `SsTableHandle`s so callers can use `SstFile::stats()` for additional metadata?

## References

- [RFC 0020: Range Metadata and Size Estimation](./0020-range-metadata.md) — complementary RFC adding per-SST stats and `SstReader`/`SstFile`
- [OpenData-Log RFC 0001: Log Storage](https://github.com/opendata-dev/log/blob/main/rfcs/0001-storage.md) — motivating use case for record counting and level-scoped scanning
- [RocksDB `GetColumnFamilyMetaData`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/metadata.h) — per-level SST metadata including `num_entries`
- [RocksDB `GetApproximateSizes`](https://github.com/facebook/rocksdb/wiki/Approximate-Size) — index-level size estimation
- [Pebble `ScanInternal`](https://pkg.go.dev/github.com/cockroachdb/pebble) — level-aware internal scanning with per-level callbacks
- [Pebble `ScanStatistics`](https://pkg.go.dev/github.com/cockroachdb/pebble) — per-level key statistics for a key range

## Updates

| Date       | Description |
|------------|-------------|
| 2026-02-24 | Initial draft |
