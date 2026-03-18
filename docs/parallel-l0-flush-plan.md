# Parallel L0 Flush Pipeline

## Background

Currently, the memtable flush pipeline is single-threaded. When an immutable
memtable is ready to flush, a single task serially:

1. builds the SST
2. writes the SST to object storage
3. updates in-memory state
4. writes the manifest

In high-throughput workloads this becomes a bottleneck. WAL flush can outpace
L0 flush, immutable memtables pile up, and writers block on backpressure.

The target design is a pipelined flush subsystem:

- an **orchestrator** that owns flush request semantics and dispatch
- a pool of **SST uploaders** that only build and upload SSTs
- a **manifest sequencer** that restores order, batches contiguous completions,
  and commits manifest updates

The critical point is that manifest updates remain single-flight, but they no
longer block new SST build/upload work from starting.

## Current flush path (as of upstream/main)

The codebase uses a `MessageHandler`/`MessageDispatcher` framework. The
memtable flusher implements `MessageHandler<MemtableFlushMsg>` and receives:

- `FlushImmutableMemtables { sender }`
- `PollManifest`
- `CreateCheckpoint { options, sender }`

The flush-to-L0 logic in `flush_imm_memtables_to_l0` does, per memtable:

1. **WAL gating**: if WAL is enabled, ensure the WAL is persisted up to the
   memtable's last sequence before flushing to L0.
2. **SST build + write**: `flush_imm_table(&id, table, true)`.
3. **State modification** under the DB state write lock:
   - pop from `imm_memtable` deque
   - push SST handle to L0
   - update `replay_after_wal_id`
   - validate and update `last_l0_clock_tick`
   - update `last_l0_seq`
   - update `recent_snapshot_min_seq`
   - extend `sequence_tracker`
4. **`notify_flush_to_l0(Ok(()))`**
5. **`write_manifest_safely()`**
6. **`notify_durable(Ok(()))`** and **`oracle.advance_durable_seq()`**
7. **Fencing handling**: delete SST and reload manifest on `Fenced`
8. **`notify_durable(Err(...))`** on manifest write failure

Key observations:

- The ordered state transition in step 3 must remain serialized.
- `notify_flush_to_l0` and `notify_durable` are distinct signals today.
- `FlushImmutableMemtables` currently returns `Ok(())` even if `l0_max_ssts`
  prevents some immutable memtables from being flushed.
- `CreateCheckpoint(CheckpointScope::All)` currently depends on
  `flush_memtables()`, so its real guarantee is also ambiguous when L0 is full.

## Goals

- Parallelize SST build and upload across multiple immutable memtables.
- Pipeline manifest writes so uploader work can continue while a manifest write
  is in progress.
- Preserve the current ordering invariants for deque pop, clock ticks,
  sequence monotonicity, and durability notifications.
- Make flush and checkpoint API guarantees explicit.

## Non-goals

- Multiple concurrent manifest updates.
- Changing the logical ordering of L0 flush retirement.
- Making the SST uploader responsible for epoch tracking or API semantics.

## API Contracts

The first part of the work is to define explicit contracts for the flush API.
Today a periodic best-effort flush attempt and a user-visible `Db::flush()` use
the same message path but need different guarantees.

### `FlushImmutableMemtables`

`FlushImmutableMemtables` should carry an explicit guarantee:

```rust
enum FlushImmGuarantee {
    BestEffort,
    ThroughWalId(u64),
}

struct FlushCompletion {
    durable_through_seq: Option<u64>,
}
```

Semantics:

- `BestEffort`
  - used by periodic polling and opportunistic progress
  - may dispatch whatever work is currently possible
  - may return without making all current immutable memtables durable
- `ThroughWalId(target)`
  - used by `Db::flush()` and `CheckpointScope::All`
  - must not report success until every immutable memtable at or before the
    captured flush frontier whose `recent_flushed_wal_id <= target` is durable
  - if L0 is saturated, the request remains pending until compaction frees
    space or an error occurs

`FlushCompletion.durable_through_seq` should report the latest sequence made
durable by the request. `None` means there was nothing to flush.

### `Db::flush()`

`Db::flush()` should:

1. freeze the current memtable if needed
2. capture the latest immutable frontier as a `wal_id` target
3. send `FlushImmutableMemtables { guarantee: ThroughWalId(target), ... }`
4. wait for the orchestrator to report that target as durable

This fixes the current false-success behavior when `l0_max_ssts` is reached.

### `notify_flush_to_l0` vs `notify_durable`

These remain distinct:

- `notify_flush_to_l0(Ok(()))`
  - means the memtable has been retired from the in-memory immutable deque and
    replaced by an L0 SST in local state
  - backpressure may be released
- `notify_durable(Ok(()))`
  - means the manifest commit has completed successfully

### `CheckpointScope::All`

`CheckpointScope::All` should mean:

- all writes issued at the time `create_checkpoint()` is called are included in
  the checkpoint

To satisfy this, checkpoint creation must first wait for a
`FlushImmutableMemtables::ThroughWalId(target)` barrier to complete. This is
especially important in no-WAL mode, where a false-success flush would create a
checkpoint that misses data.

## Architecture

```
maybe_freeze_memtable()
       |
       v
  [MemtableFlushMsg channel]
       |
       v
  Orchestrator (MessageHandler)
       |
       |-- UploadJob ---------> [channel] --> SST Uploaders (N)
       |                                         |
       |<-- UploadResult ------------------------+
       |
       |-- SequencerMsg::Uploaded ------> [channel] --> Manifest Sequencer (1)
       |                                                  |
       |<-- durable epoch / seq ---------- [watch] -------+
       |<-- checkpoint response ----------- [oneshot] -----+
```

### Component boundaries

#### Orchestrator

The orchestrator is the control plane. It owns:

- `MemtableFlushMsg` handling
- epoch assignment
- flush waiter tracking
- dispatch policy
- WAL gating before upload dispatch
- retry policy for uploader failures
- watching sequencer durability progress

The orchestrator does not build SSTs and does not mutate the manifest.

#### SST uploader pool

The uploader pool is intentionally simple. It owns:

- build SST from immutable memtable
- upload SST to object storage
- return success or failure

It does not own:

- flush API semantics
- epoch assignment policy
- manifest mutation
- checkpoint handling
- retry decisions

#### Manifest sequencer

The sequencer owns all ordered retirement and all writer-side manifest mutation:

- reorder buffer keyed by flush epoch
- restoring original immutable-memtable order
- batched state transition for contiguous uploaded results
- single-flight manifest writes
- `notify_flush_to_l0`
- `notify_durable`
- `oracle.advance_durable_seq`
- checkpoint creation

This keeps all manifest mutation in one place.

## Ordering model

The uploader pool is unordered. The sequencer restores order.

### Why keep epochs

Use a dedicated flush epoch rather than `last_seq` or `wal_id` as the ordering
key.

Reasons:

- The correctness invariant is the immutable deque order, not simply sequence
  order.
- `wal_id` is a durability watermark, not a flush pipeline token.
- The existing code validates retirement using `pop_back()` plus
  `Arc::ptr_eq`, so the pipeline needs an explicit ordering identity.

Each dispatched immutable memtable gets:

```rust
struct FlushEpoch(u64);
```

Epochs start at 1 so that a durable epoch of 0 means "nothing durable yet".

## Concrete APIs

These types are intended as planning-level APIs, not final exact Rust.

### Messages

```rust
enum MemtableFlushMsg {
    FlushImmutableMemtables {
        guarantee: FlushImmGuarantee,
        sender: Option<tokio::sync::oneshot::Sender<Result<FlushCompletion, SlateDBError>>>,
    },
    PollManifest,
}
```

Checkpoints should no longer flow through `MemtableFlushMsg`. They should be
sent directly to the sequencer:

```rust
struct CheckpointRequest {
    options: CheckpointOptions,
    sender: tokio::sync::oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
}
```

### Orchestrator-owned types

```rust
struct UploadJob {
    epoch: FlushEpoch,
    imm_memtable: Arc<ImmutableMemtable>,
    sst_id: SsTableId,
    not_before: tokio::time::Instant,
}

struct PendingFlushResponse {
    target_wal_id: u64,
    sender: tokio::sync::oneshot::Sender<Result<FlushCompletion, SlateDBError>>,
}
```

### Uploader result types

```rust
struct UploadSuccess {
    epoch: FlushEpoch,
    imm_memtable: Arc<ImmutableMemtable>,
    sst_id: SsTableId,
    sst_handle: SsTableHandle,
    last_seq: u64,
}

struct UploadFailure {
    epoch: FlushEpoch,
    imm_memtable: Arc<ImmutableMemtable>,
    sst_id: SsTableId,
    error: SlateDBError,
}
```

### Sequencer input/output

```rust
enum SequencerMsg {
    Uploaded(UploadSuccess),
    CreateCheckpoint(CheckpointRequest),
    Shutdown,
}

struct DurableProgress {
    durable_epoch: FlushEpoch,
    durable_seq: u64,
}
```

The orchestrator watches `DurableProgress` and resolves pending
`FlushImmutableMemtables` waiters when their target frontier has become
durable.

## Dispatch and durability tracking

The orchestrator tracks:

- `next_epoch`: next epoch to assign
- `durable_epoch_seen`: latest sequencer-reported durable epoch processed by
  the orchestrator
- per-epoch metadata for dispatched but not-yet-durable work
- pending flush responses keyed by target `wal_id`

The sequencer tracks:

- `durable_epoch`: latest epoch durably committed
- reorder buffer `BTreeMap<FlushEpoch, UploadSuccess>`

`in_flight()` is derived from dispatched epochs minus durable epochs.

## Flush flow

### Best effort

1. `PollManifest` or other background trigger arrives.
2. Orchestrator refreshes manifest-related state if needed and calls
   `try_dispatch()`.
3. Orchestrator dispatches whatever immutable memtables are eligible.
4. Uploaders build and upload SSTs in parallel.
5. Sequencer retires the maximal contiguous uploaded prefix in order.

### `Db::flush()`

1. Freeze current memtable if needed.
2. Determine flush target `wal_id`.
3. Send `FlushImmutableMemtables::ThroughWalId(target)`.
4. Orchestrator dispatches work as L0 capacity allows.
5. Sequencer retires uploaded results in order and updates durability.
6. Orchestrator resolves the oneshot when the target frontier is durable.

## Sequencer batching model

The sequencer should retire the maximal contiguous ready prefix and commit it
with one manifest write.

Example:

- if epochs `7, 8, 9` are uploaded and `10` is not, retire `7..=9` as one
  batch
- if `8` is missing, stop after `7` even if `9` and `10` are already uploaded

For one contiguous batch, the sequencer does:

1. apply the ordered in-memory state transition for each uploaded memtable
2. call `notify_flush_to_l0(Ok(()))` for each
3. perform one `write_manifest_safely()`
4. on success:
   - call `notify_durable(Ok(()))` for each
   - call `oracle.advance_durable_seq(last_seq)` in order
   - publish updated `DurableProgress`
5. on failure:
   - report the error
   - delete any uploaded SSTs that were not durably referenced if required
   - refresh manifest/state on fencing

## WAL gating

WAL gating should happen in the orchestrator before dispatch to the uploader
pool.

Reason:

- if WAL is not yet durable through the immutable memtable's `last_seq`, there
  is no point spending CPU and object-store bandwidth building an L0 SST yet

## Checkpoints

Checkpoint creation should be sequencer-owned because the sequencer owns all
writer-side manifest mutation.

### `CheckpointScope::Durable`

- send checkpoint request directly to the sequencer
- checkpoint is created against the current durable manifest state

### `CheckpointScope::All`

1. flush WALs if enabled
2. issue `FlushImmutableMemtables::ThroughWalId(target)`
3. wait for success
4. send checkpoint request to the sequencer

This keeps checkpoint semantics clear and avoids routing manifest mutation
through the orchestrator.

## Shutdown

1. Orchestrator stops dispatching new work.
2. Orchestrator waits for in-flight work to become durable, or exits early on
   terminal error.
3. Orchestrator closes uploader input so uploaders exit.
4. Orchestrator sends `SequencerMsg::Shutdown`.
5. Sequencer drains any remaining contiguous uploaded results, writes a final
   manifest if needed, and exits.
6. `cleanup` drains pending flush requests and resolves them with
   `Err(SlateDBError::Closed)`.
7. Remaining in-memory memtables are notified of error as today.

The sequencing constraint is important: the sequencer must finish applying any
final ordered state transitions before cleanup notifies remaining memtables.

## Error handling

### Uploader failure

When an uploader returns `UploadFailure`:

1. orchestrator logs the error
2. orchestrator may retry by re-enqueueing the same `UploadJob` with
   `not_before = now + manifest_poll_interval`
3. if the error is terminal, orchestrator fails any pending flush responses
   whose targets depend on that epoch

Later uploaded epochs remain blocked in the sequencer until the missing epoch
is retried successfully or the subsystem fails.

### Sequencer failure

If manifest write or ordered retirement fails:

1. sequencer reports terminal failure
2. orchestrator fails all pending flush responses
3. cleanup notifies in-memory memtables of error

## Known issues from prototyping

### `watch` initial value

If the durable epoch watch is initialized to 0 and epochs start at 1, `send(0)`
does not trigger `changed()`. Tests must wait for `>= 1`, not `>= 0`.

### `Arc::ptr_eq`

The ordered state transition still depends on using the exact same
`Arc<ImmutableMemtable>` instance that lives in the immutable deque.

### `Db::close()` and L0 saturation

`Db::close()` currently shuts compaction down before memtable flush. If L0 is at
`l0_max_ssts`, remaining immutable memtables may still be unflushable. This is
a pre-existing issue.

### `CheckpointScope::All` in no-WAL mode

This is affected by the current false-success flush contract. Fixing the flush
contract fixes checkpoint correctness in no-WAL mode.

## Implementation stages

### Stage 1: API cleanup

- Define `FlushImmGuarantee` and `FlushCompletion`.
- Update `FlushImmutableMemtables` semantics in code comments and tests.
- Add tests for the `l0_max_ssts` false-success case.
- Add tests for `CheckpointScope::All` under L0 saturation, especially
  no-WAL mode.

### Stage 2: SST uploader pool

- Add `UploadJob`, `UploadSuccess`, `UploadFailure`.
- Implement stateless uploader workers.
- Tests:
  - single upload produces valid SST
  - multiple uploads complete out of order
  - retries remain orchestrator-owned

### Stage 3: Manifest sequencer

- Add `SequencerMsg`, `DurableProgress`, reorder buffer.
- Move checkpoint creation into the sequencer.
- Implement contiguous-prefix batching and single-flight manifest commit.
- Tests:
  - out-of-order uploaded results retire in order
  - contiguous batches produce one manifest write
  - notifications fire at the correct times
  - checkpoint creation is ordered against flush retirement

### Stage 4: Orchestrator

- Replace current `MemtableFlusher` handler with an orchestrator.
- Spawn uploader workers and the sequencer.
- Implement:
  - epoch assignment
  - WAL gating
  - dispatch under `l0_max_ssts`
  - pending flush waiters keyed by target `wal_id`
  - durability watch processing
  - uploader retry handling

### Stage 5: Integration testing

1. End-to-end correctness with uploader parallelism > 1.
2. Backpressure: writers block and resume.
3. L0 saturation: `Db::flush()` remains pending and later succeeds after
   compaction frees space.
4. Error + retry: uploader fails, retries, eventually succeeds.
5. Error + oneshot: `Db::flush()` gets `Err` on terminal failure.
6. `CheckpointScope::All` includes all writes present at call time.
7. Shutdown with in-flight work completes cleanly.
8. All existing relevant tests pass after contract updates.
