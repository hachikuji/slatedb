//! Parallel L0 memtable flusher.
//!
//! The memtable flusher is the control plane for the parallel L0 flush pipeline.
//! It owns:
//! - flush request semantics
//! - immutable-memtable frontier capture
//! - upload dispatch policy
//! - waiter tracking
//! - coordination between uploader and sequencer
//!
//! It does not own:
//! - SST build/upload execution
//! - manifest mutation
//! - manifest durability sequencing

use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::manifest::store::StoredManifest;
use crate::memtable_flusher::sequencer::{Sequencer, SequencerEvent, UploadedMemtable};
use crate::memtable_flusher::uploader::{UploadJob, Uploader, UploaderEvent};
use crate::oracle::Oracle;
use crate::utils::{IdGenerator, SendSafely, WatchableOnceCell};
use fail_parallel::fail_point;
use log::debug;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time;

use super::FlushEpoch;

/// Flush request target exposed by the memtable flusher.
pub(crate) enum FlushTarget {
    /// Attempt to make progress without waiting for a specific durability frontier.
    BestEffort,
    /// Operate against the currently durable frontier without requiring new flush work.
    CurrentDurable,
    /// Wait until all immutable memtables through the captured WAL frontier are durable.
    ThroughWalId(u64),
    /// Wait until the currently observed immutable memtable frontier is durable.
    ThroughCurrentImm,
}

/// Result reported for a completed flush request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushResult {
    /// Highest durable WAL id covered by the completed flush.
    pub(crate) durable_through_wal_id: Option<u64>,
    /// Highest durable sequence number covered by the completed flush.
    pub(crate) durable_through_seq: Option<u64>,
}

/// Narrow dependency bundle for the parallel memtable flusher.
struct MemtableFlusherDb {
    inner: Arc<DbInner>,
    manifest_reader: Option<AsyncMutex<StoredManifest>>,
}

impl MemtableFlusherDb {
    fn from_db_inner(db_inner: &Arc<DbInner>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(db_inner),
            manifest_reader: None,
        })
    }

    fn new(inner: Arc<DbInner>, manifest_reader: Option<StoredManifest>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            manifest_reader: manifest_reader.map(AsyncMutex::new),
        })
    }
}

/// Parallel L0 memtable flusher subsystem.
pub(crate) struct MemtableFlusher {
    commands: Mutex<Option<OrchestratorCommandSender>>,
    poisoned: Arc<Mutex<Option<SlateDBError>>>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    task: Mutex<Option<JoinHandle<Result<(), SlateDBError>>>>,
}

enum OrchestratorCommand {
    Flush {
        target: FlushTarget,
        sender: Option<oneshot::Sender<Result<FlushResult, SlateDBError>>>,
    },
    CreateCheckpoint {
        target: FlushTarget,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
}

type OrchestratorCommandSender = mpsc::UnboundedSender<OrchestratorCommand>;
type OrchestratorCommandReceiver = mpsc::UnboundedReceiver<OrchestratorCommand>;

impl MemtableFlusher {
    /// Starts the memtable flusher subsystem.
    pub(crate) fn start(
        inner: Arc<DbInner>,
        manifest_reader: Option<StoredManifest>,
        uploader: Uploader,
        sequencer: Sequencer,
        handle: &Handle,
    ) -> Self {
        Self::start_with_db(
            MemtableFlusherDb::new(inner, manifest_reader),
            uploader,
            sequencer,
            handle,
        )
    }

    fn start_with_db(
        db: Arc<MemtableFlusherDb>,
        uploader: Uploader,
        sequencer: Sequencer,
        handle: &Handle,
    ) -> Self {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let poisoned = Arc::new(Mutex::new(None));
        let closed_result = WatchableOnceCell::new();
        let task = handle.spawn(
            OrchestratorTask::new(
                db,
                uploader,
                sequencer,
                Arc::clone(&poisoned),
                closed_result.clone(),
                commands_rx,
            )
            .run(),
        );

        Self {
            commands: Mutex::new(Some(commands_tx)),
            poisoned,
            closed_result,
            task: Mutex::new(Some(task)),
        }
    }

    /// Processes one flush request using the requested target.
    pub(crate) async fn flush(&self, target: FlushTarget) -> Result<FlushResult, SlateDBError> {
        if let Some(err) = self.poisoned.lock().clone() {
            return Err(err);
        }
        let (tx, rx) = oneshot::channel();
        self.commands
            .lock()
            .as_ref()
            .ok_or(SlateDBError::Closed)?
            .send_safely(
                self.closed_result.reader(),
                OrchestratorCommand::Flush {
                    target,
                    sender: Some(tx),
                },
            )?;
        rx.await.map_err(SlateDBError::ReadChannelError)?
    }

    /// Schedules a flush request without awaiting its result.
    pub(crate) fn schedule_flush(&self, target: FlushTarget) -> Result<(), SlateDBError> {
        if let Some(err) = self.poisoned.lock().clone() {
            return Err(err);
        }
        let Some(commands) = self.commands.lock().as_ref().cloned() else {
            return Err(SlateDBError::Closed);
        };
        #[allow(clippy::disallowed_methods)]
        match commands.send(OrchestratorCommand::Flush {
            target,
            sender: None,
        }) {
            Ok(()) => Ok(()),
            Err(_) => {
                if let Some(result) = self.closed_result.reader().read() {
                    match result {
                        Ok(()) => Err(SlateDBError::Closed),
                        Err(err) => Err(err),
                    }
                } else {
                    Err(SlateDBError::Closed)
                }
            }
        }
    }

    /// Creates a checkpoint using the memtable flusher's flush semantics.
    pub(crate) async fn create_checkpoint(
        &self,
        target: FlushTarget,
        options: CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        if let Some(err) = self.poisoned.lock().clone() {
            return Err(err);
        }
        let (tx, rx) = oneshot::channel();
        self.commands
            .lock()
            .as_ref()
            .ok_or(SlateDBError::Closed)?
            .send_safely(
                self.closed_result.reader(),
                OrchestratorCommand::CreateCheckpoint {
                    target,
                    options,
                    sender: tx,
                },
            )?;
        rx.await.map_err(SlateDBError::ReadChannelError)?
    }

    /// Closes the flusher and any owned subsystems.
    pub(crate) async fn close(&self) -> Result<(), SlateDBError> {
        self.commands.lock().take();
        let task = self.task.lock().take();
        let result = if let Some(task) = task {
            match task.await {
                Ok(result) => result,
                Err(join_err) if join_err.is_cancelled() => Ok(()),
                Err(join_err) if join_err.is_panic() => {
                    Err(SlateDBError::BackgroundTaskPanic("memtable_flusher".into()))
                }
                Err(_) => Err(SlateDBError::BackgroundTaskCancelled(
                    "memtable_flusher".into(),
                )),
            }
        } else {
            Ok(())
        };
        self.closed_result.write(result.clone().map(|_| ()));
        result
    }
}

struct OrchestratorTask {
    db: Arc<MemtableFlusherDb>,
    uploader: Uploader,
    sequencer: Sequencer,
    poisoned: Arc<Mutex<Option<SlateDBError>>>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    commands: OrchestratorCommandReceiver,
    next_epoch: FlushEpoch,
    tracked_imms: VecDeque<TrackedImm>,
    durable_state: DurableState,
    pending_flushes: Vec<PendingFlush>,
    pending_checkpoints: Vec<PendingCheckpoint>,
    durable_l0_len: usize,
}

impl OrchestratorTask {
    fn new(
        db: Arc<MemtableFlusherDb>,
        uploader: Uploader,
        sequencer: Sequencer,
        poisoned: Arc<Mutex<Option<SlateDBError>>>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        commands: OrchestratorCommandReceiver,
    ) -> Self {
        Self {
            db,
            uploader,
            sequencer,
            poisoned,
            closed_result,
            commands,
            next_epoch: FlushEpoch(1),
            tracked_imms: VecDeque::new(),
            durable_state: DurableState::default(),
            pending_flushes: Vec::new(),
            pending_checkpoints: Vec::new(),
            durable_l0_len: 0,
        }
    }

    #[allow(clippy::disallowed_methods)]
    async fn run(mut self) -> Result<(), SlateDBError> {
        let mut poll = time::interval(self.db.inner.settings.manifest_poll_interval);
        loop {
            tokio::select! {
                _ = poll.tick() => {
                    self.reconcile_and_dispatch().await?;
                }
                maybe_command = self.commands.recv() => {
                    let Some(command) = maybe_command else {
                        let uploader_result = self.uploader.close().await;
                        let sequencer_result = self.sequencer.close().await;
                        uploader_result?;
                        sequencer_result?;
                        return Ok(());
                    };
                    self.handle_command(command).await?;
                }
                maybe_event = self.uploader.events().recv() => {
                    let Some(event) = maybe_event else {
                        return self.handle_fatal_error(SlateDBError::Closed).await;
                    };
                    self.handle_uploader_event(event).await?;
                }
                maybe_event = self.sequencer.events().recv() => {
                    let Some(event) = maybe_event else {
                        return self.handle_fatal_error(SlateDBError::Closed).await;
                    };
                    self.handle_sequencer_event(event).await?;
                }
            }
        }
    }

    async fn handle_command(&mut self, command: OrchestratorCommand) -> Result<(), SlateDBError> {
        match command {
            OrchestratorCommand::Flush { target, sender } => {
                fail_point!(
                    Arc::clone(&self.db.inner.fp_registry),
                    "flush-memtable-to-l0"
                );
                self.register_new_imm_memtables();
                let result = self.handle_flush_request(target, sender).await;
                self.reconcile_and_dispatch().await?;
                result
            }
            OrchestratorCommand::CreateCheckpoint {
                target,
                options,
                sender,
            } => {
                let result = self
                    .handle_checkpoint_request(target, options, sender)
                    .await;
                self.reconcile_and_dispatch().await?;
                result
            }
        }
    }

    async fn handle_flush_request(
        &mut self,
        target: FlushTarget,
        sender: Option<oneshot::Sender<Result<FlushResult, SlateDBError>>>,
    ) -> Result<(), SlateDBError> {
        match target {
            FlushTarget::BestEffort => {
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(self.flush_result()));
                }
                Ok(())
            }
            FlushTarget::CurrentDurable => {
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(self.flush_result()));
                }
                Ok(())
            }
            FlushTarget::ThroughWalId(required_wal_id) => {
                if self
                    .durable_state
                    .wal_id
                    .is_some_and(|durable_wal_id| durable_wal_id >= required_wal_id)
                {
                    if let Some(sender) = sender {
                        let _ = sender.send(Ok(self.flush_result()));
                    }
                    return Ok(());
                }
                if let Some(sender) = sender {
                    self.pending_flushes.push(PendingFlush {
                        requirement: FlushRequirement::WalId(required_wal_id),
                        sender,
                    });
                }
                Ok(())
            }
            FlushTarget::ThroughCurrentImm => {
                self.register_new_imm_memtables();
                let Some(required_epoch) = self.tracked_imms.back().map(|tracked| tracked.epoch)
                else {
                    if let Some(sender) = sender {
                        let _ = sender.send(Ok(self.flush_result()));
                    }
                    return Ok(());
                };
                if self
                    .durable_state
                    .epoch
                    .is_some_and(|durable_epoch| durable_epoch >= required_epoch)
                {
                    if let Some(sender) = sender {
                        let _ = sender.send(Ok(self.flush_result()));
                    }
                    return Ok(());
                }
                if let Some(sender) = sender {
                    self.pending_flushes.push(PendingFlush {
                        requirement: FlushRequirement::Epoch(required_epoch),
                        sender,
                    });
                }
                Ok(())
            }
        }
    }

    async fn handle_checkpoint_request(
        &mut self,
        target: FlushTarget,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        match target {
            FlushTarget::BestEffort => {
                let result = self.sequencer.create_checkpoint(None, options).await;
                let _ = sender.send(result);
                Ok(())
            }
            FlushTarget::CurrentDurable => {
                let result = self.sequencer.create_checkpoint(None, options).await;
                let _ = sender.send(result);
                Ok(())
            }
            FlushTarget::ThroughWalId(required_wal_id) => {
                self.register_new_imm_memtables();
                if self
                    .durable_state
                    .wal_id
                    .is_some_and(|durable| durable >= required_wal_id)
                {
                    let result = self.sequencer.create_checkpoint(None, options).await;
                    let _ = sender.send(result);
                    return Ok(());
                }
                self.pending_checkpoints.push(PendingCheckpoint {
                    requirement: FlushRequirement::WalId(required_wal_id),
                    options,
                    sender,
                });
                Ok(())
            }
            FlushTarget::ThroughCurrentImm => {
                self.register_new_imm_memtables();
                let Some(required_epoch) = self.tracked_imms.back().map(|tracked| tracked.epoch)
                else {
                    let result = self.sequencer.create_checkpoint(None, options).await;
                    let _ = sender.send(result);
                    return Ok(());
                };
                if self
                    .durable_state
                    .epoch
                    .is_some_and(|durable_epoch| durable_epoch >= required_epoch)
                {
                    let result = self.sequencer.create_checkpoint(None, options).await;
                    let _ = sender.send(result);
                    return Ok(());
                }
                self.pending_checkpoints.push(PendingCheckpoint {
                    requirement: FlushRequirement::Epoch(required_epoch),
                    options,
                    sender,
                });
                Ok(())
            }
        }
    }

    async fn handle_uploader_event(&mut self, event: UploaderEvent) -> Result<(), SlateDBError> {
        match event {
            UploaderEvent::Uploaded(success) => {
                if let Some(tracked) = self
                    .tracked_imms
                    .iter_mut()
                    .find(|tracked| tracked.epoch == success.epoch)
                {
                    tracked.state = TrackedImmState::Sequencing;
                }
                self.sequencer
                    .notify_uploaded(UploadedMemtable::new(
                        success.epoch,
                        Arc::clone(&success.imm_memtable),
                        success.sst_id,
                        success.sst_handle.clone(),
                        success.last_seq,
                    ))
                    .await?;
                Ok(())
            }
            UploaderEvent::Fatal(err) => Err(err),
        }
    }

    async fn handle_sequencer_event(&mut self, event: SequencerEvent) -> Result<(), SlateDBError> {
        match event {
            SequencerEvent::Flushed {
                through_epoch,
                through_seq,
            } => {
                let mut through_wal_id = None;
                while self
                    .tracked_imms
                    .front()
                    .is_some_and(|tracked| tracked.epoch <= through_epoch)
                {
                    let tracked = self.tracked_imms.pop_front().expect("checked above");
                    through_wal_id = Some(tracked.wal_id);
                }
                self.durable_state = DurableState {
                    epoch: Some(through_epoch),
                    wal_id: through_wal_id.or(self.durable_state.wal_id),
                    seq: Some(through_seq),
                };
                self.resolve_flush_waiters();
                self.resolve_checkpoint_waiters().await?;
                self.reconcile_and_dispatch().await
            }
            SequencerEvent::Fatal(err) => Err(err),
        }
    }

    async fn reconcile_and_dispatch(&mut self) -> Result<(), SlateDBError> {
        self.refresh_manifest_progress().await?;
        self.register_new_imm_memtables();
        self.dispatch_ready_memtables().await?;
        self.resolve_flush_waiters();
        self.resolve_checkpoint_waiters().await
    }

    async fn refresh_manifest_progress(&mut self) -> Result<(), SlateDBError> {
        if let Some(manifest_reader) = &self.db.manifest_reader {
            let mut manifest_reader = manifest_reader.lock().await;
            manifest_reader.refresh().await?;
            let remote_dirty = manifest_reader.prepare_dirty()?;
            debug!(
                "flusher refreshing manifest progress [remote_l0_len={}, remote_last_l0_seq={}, remote_replay_after_wal_id={}, remote_tracker_len={}, remote_tracker_first_seq={:?}, remote_tracker_last_seq={:?}, remote_tracker_first_ts={:?}, remote_tracker_last_ts={:?}]",
                remote_dirty.value.core.l0.len(),
                remote_dirty.value.core.last_l0_seq,
                remote_dirty.value.core.replay_after_wal_id,
                remote_dirty.value.core.sequence_tracker.len(),
                remote_dirty.value.core.sequence_tracker.first_seq(),
                remote_dirty.value.core.sequence_tracker.last_seq(),
                remote_dirty.value.core.sequence_tracker.first_ts(),
                remote_dirty.value.core.sequence_tracker.last_ts(),
            );
            self.durable_l0_len = manifest_reader.db_state().l0.len();
            let mut wguard_state = self.db.inner.state.write();
            wguard_state.merge_remote_manifest(remote_dirty);
            let merged_state = wguard_state.state();
            let merged = merged_state.core();
            debug!(
                "flusher merged manifest progress [merged_l0_len={}, merged_last_l0_seq={}, merged_replay_after_wal_id={}, merged_tracker_len={}, merged_tracker_first_seq={:?}, merged_tracker_last_seq={:?}, merged_tracker_first_ts={:?}, merged_tracker_last_ts={:?}]",
                merged.l0.len(),
                merged.last_l0_seq,
                merged.replay_after_wal_id,
                merged.sequence_tracker.len(),
                merged.sequence_tracker.first_seq(),
                merged.sequence_tracker.last_seq(),
                merged.sequence_tracker.first_ts(),
                merged.sequence_tracker.last_ts(),
            );
            self.db
                .inner
                .db_stats
                .l0_sst_count
                .set(wguard_state.state().core().l0.len() as i64);
        } else {
            self.durable_l0_len = self.db.inner.state.read().state().core().l0.len();
        }
        Ok(())
    }

    fn register_new_imm_memtables(&mut self) {
        let guard = self.db.inner.state.read();
        for imm_memtable in guard.state().imm_memtable.iter().rev() {
            let ptr = Arc::as_ptr(imm_memtable) as usize;
            if self.tracked_imms.iter().any(|tracked| tracked.ptr == ptr) {
                continue;
            }
            self.tracked_imms.push_back(TrackedImm {
                epoch: self.next_epoch,
                ptr,
                wal_id: imm_memtable.recent_flushed_wal_id(),
                imm_memtable: Arc::clone(imm_memtable),
                state: TrackedImmState::PendingDispatch,
            });
            self.next_epoch = FlushEpoch(self.next_epoch.0 + 1);
        }
    }

    async fn dispatch_ready_memtables(&mut self) -> Result<(), SlateDBError> {
        loop {
            let reserved_l0_slots = self
                .tracked_imms
                .iter()
                .filter(|tracked| {
                    matches!(
                        tracked.state,
                        TrackedImmState::Uploading | TrackedImmState::Sequencing
                    )
                })
                .count();
            if self.durable_l0_len + reserved_l0_slots >= self.db.inner.settings.l0_max_ssts {
                return Ok(());
            }

            let Some(next_index) = self
                .tracked_imms
                .iter()
                .position(|tracked| matches!(tracked.state, TrackedImmState::PendingDispatch))
            else {
                return Ok(());
            };
            let tracked = &mut self.tracked_imms[next_index];
            if self.db.inner.wal_enabled {
                let last_seq = tracked.imm_memtable.table().last_seq().unwrap_or(0);
                if self.db.inner.oracle.last_remote_persisted_seq() < last_seq {
                    self.db.inner.flush_wals().await?;
                }
            }
            let sst_id = crate::db_state::SsTableId::Compacted(
                self.db
                    .inner
                    .rand
                    .rng()
                    .gen_ulid(self.db.inner.system_clock.as_ref()),
            );
            self.uploader
                .submit(UploadJob::new(
                    tracked.epoch,
                    Arc::clone(&tracked.imm_memtable),
                    sst_id,
                ))
                .await?;
            tracked.state = TrackedImmState::Uploading;
        }
    }

    fn resolve_flush_waiters(&mut self) {
        let flush_result = self.flush_result();
        let pending_flushes = std::mem::take(&mut self.pending_flushes);
        let mut pending = Vec::with_capacity(pending_flushes.len());
        for flush in pending_flushes {
            if self.requirement_satisfied(flush.requirement) {
                let _ = flush.sender.send(Ok(FlushResult {
                    durable_through_wal_id: flush_result.durable_through_wal_id,
                    durable_through_seq: flush_result.durable_through_seq,
                }));
            } else {
                pending.push(flush);
            }
        }
        self.pending_flushes = pending;
    }

    async fn resolve_checkpoint_waiters(&mut self) -> Result<(), SlateDBError> {
        let pending_checkpoints = std::mem::take(&mut self.pending_checkpoints);
        let mut pending = Vec::with_capacity(pending_checkpoints.len());
        for checkpoint in pending_checkpoints {
            if self.requirement_satisfied(checkpoint.requirement) {
                let result = self
                    .sequencer
                    .create_checkpoint(None, checkpoint.options)
                    .await;
                let _ = checkpoint.sender.send(result);
            } else {
                pending.push(checkpoint);
            }
        }
        self.pending_checkpoints = pending;
        Ok(())
    }

    fn requirement_satisfied(&self, requirement: FlushRequirement) -> bool {
        match requirement {
            FlushRequirement::WalId(required_wal_id) => self
                .durable_state
                .wal_id
                .is_some_and(|durable_wal_id| durable_wal_id >= required_wal_id),
            FlushRequirement::Epoch(required_epoch) => self
                .durable_state
                .epoch
                .is_some_and(|durable_epoch| durable_epoch >= required_epoch),
        }
    }

    fn flush_result(&self) -> FlushResult {
        FlushResult {
            durable_through_wal_id: self.durable_state.wal_id,
            durable_through_seq: self.durable_state.seq,
        }
    }

    async fn handle_fatal_error(&mut self, err: SlateDBError) -> Result<(), SlateDBError> {
        *self.poisoned.lock() = Some(err.clone());
        self.closed_result.write(Err(err.clone()));
        for flush in self.pending_flushes.drain(..) {
            let _ = flush.sender.send(Err(err.clone()));
        }
        for checkpoint in self.pending_checkpoints.drain(..) {
            let _ = checkpoint.sender.send(Err(err.clone()));
        }
        let _ = self.uploader.close().await;
        let _ = self.sequencer.close().await;
        Err(err)
    }
}

#[derive(Clone, Copy, Default)]
struct DurableState {
    epoch: Option<FlushEpoch>,
    wal_id: Option<u64>,
    seq: Option<u64>,
}

struct TrackedImm {
    epoch: FlushEpoch,
    ptr: usize,
    wal_id: u64,
    imm_memtable: Arc<crate::mem_table::ImmutableMemtable>,
    state: TrackedImmState,
}

enum TrackedImmState {
    PendingDispatch,
    Uploading,
    Sequencing,
}

struct PendingFlush {
    requirement: FlushRequirement,
    sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
}

struct PendingCheckpoint {
    requirement: FlushRequirement,
    options: CheckpointOptions,
    sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
}

#[derive(Clone, Copy)]
enum FlushRequirement {
    WalId(u64),
    Epoch(FlushEpoch),
}

#[cfg(test)]
mod tests {
    use super::{FlushTarget, MemtableFlusher, MemtableFlusherDb};
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::{ManifestCore, SsTableHandle, SsTableId, SsTableInfo, SstType};
    use crate::error::SlateDBError;
    use crate::format::sst::{SsTableFormat, SST_FORMAT_VERSION_LATEST};
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::memtable_flusher::sequencer::{Sequencer, SequencerDb};
    use crate::memtable_flusher::uploader::{Uploader, UploaderDb};
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::time::timeout;

    struct TestHarness {
        inner: Arc<DbInner>,
        orchestrator_db: Arc<MemtableFlusherDb>,
        uploader_db: Arc<UploaderDb>,
        sequencer_db: Arc<SequencerDb>,
        manifest: FenceableManifest,
        object_store: Arc<dyn ObjectStore>,
        path: String,
    }

    async fn setup_harness(
        path: &str,
        settings: Settings,
        fp_registry: Arc<FailPointRegistry>,
    ) -> TestHarness {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = path.to_string();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::new(42));
        let stat_registry = Arc::new(StatRegistry::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let stored_manifest = StoredManifest::create_new_db(
            Arc::clone(&manifest_store),
            ManifestCore::new_with_wal_object_store(None),
            Arc::clone(&system_clock),
        )
        .await
        .unwrap();
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            PathResolver::new(Path::from(path.clone())),
            Arc::clone(&fp_registry),
            None,
        ));
        let (write_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(
            DbInner::new(
                settings,
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                stored_manifest.prepare_dirty().unwrap(),
                write_tx,
                Arc::clone(&stat_registry),
                fp_registry,
                None,
            )
            .await
            .unwrap(),
        );
        // These tests freeze immutable memtables directly instead of flowing through the WAL
        // pipeline, so treat all sequences as already remotely durable.
        inner.oracle.advance_durable_seq(u64::MAX);
        let manifest =
            FenceableManifest::init_writer(stored_manifest, Duration::from_secs(300), system_clock)
                .await
                .unwrap();
        let reader = StoredManifest::load(
            Arc::new(ManifestStore::new(
                &Path::from(path.clone()),
                Arc::clone(&object_store),
            )),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        TestHarness {
            orchestrator_db: MemtableFlusherDb::new(Arc::clone(&inner), Some(reader)),
            uploader_db: UploaderDb::from_db_inner(&inner),
            sequencer_db: SequencerDb::from_db_inner(&inner),
            inner,
            manifest,
            object_store,
            path,
        }
    }

    fn freeze_value_imm(
        harness: &TestHarness,
        key: &[u8],
        value: &[u8],
        seq: u64,
        recent_flushed_wal_id: u64,
    ) {
        let mut guard = harness.inner.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        guard.freeze_memtable(recent_flushed_wal_id).unwrap();
    }

    fn freeze_merge_imm(
        harness: &TestHarness,
        key: &[u8],
        value: &[u8],
        seq: u64,
        recent_flushed_wal_id: u64,
    ) {
        let mut guard = harness.inner.state.write();
        guard.memtable().put(RowEntry::new_merge(key, value, seq));
        guard.freeze_memtable(recent_flushed_wal_id).unwrap();
    }

    async fn latest_manifest_checkpoint_count(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> usize {
        let manifest_store = ManifestStore::new(&Path::from(path), object_store);
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        manifest.core.checkpoints.len()
    }

    fn seeded_l0_handle(first_key: &[u8]) -> SsTableHandle {
        SsTableHandle::new(
            SsTableId::Compacted(ulid::Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::copy_from_slice(first_key)),
                last_entry: None,
                index_offset: 0,
                index_len: 0,
                filter_offset: 0,
                filter_len: 0,
                compression_codec: None,
                sst_type: SstType::Compacted,
                stats_offset: 0,
                stats_len: 0,
            },
        )
    }

    async fn set_remote_l0_len(path: &str, object_store: Arc<dyn ObjectStore>, l0_len: usize) {
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.l0.clear();
        for idx in 0..l0_len {
            dirty
                .value
                .core
                .l0
                .push_back(seeded_l0_handle(format!("seed-{idx}").as_bytes()));
        }
        stored_manifest.update(dirty).await.unwrap();
    }

    fn set_local_l0_len(harness: &TestHarness, l0_len: usize) {
        let mut guard = harness.inner.state.write();
        guard.modify(|modifier| {
            modifier.state.manifest.value.core.l0.clear();
            for idx in 0..l0_len {
                modifier
                    .state
                    .manifest
                    .value
                    .core
                    .l0
                    .push_back(seeded_l0_handle(format!("local-seed-{idx}").as_bytes()));
            }
        });
    }

    fn start_orchestrator(harness: TestHarness) -> MemtableFlusher {
        let uploader = Uploader::start(
            Arc::clone(&harness.uploader_db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
        );
        let sequencer = Sequencer::start(
            Arc::clone(&harness.sequencer_db),
            harness.manifest,
            &Handle::current(),
        );
        MemtableFlusher::start_with_db(
            Arc::clone(&harness.orchestrator_db),
            uploader,
            sequencer,
            &Handle::current(),
        )
    }

    fn start_orchestrator_without_reader(harness: TestHarness) -> MemtableFlusher {
        let uploader = Uploader::start(
            Arc::clone(&harness.uploader_db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
        );
        let sequencer = Sequencer::start(
            Arc::clone(&harness.sequencer_db),
            harness.manifest,
            &Handle::current(),
        );
        MemtableFlusher::start_with_db(
            MemtableFlusherDb::from_db_inner(&harness.inner),
            uploader,
            sequencer,
            &Handle::current(),
        )
    }

    #[tokio::test]
    async fn best_effort_returns_immediately() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_best_effort",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 11);
        let orchestrator = start_orchestrator(harness);

        let result = timeout(
            Duration::from_secs(5),
            orchestrator.flush(FlushTarget::BestEffort),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(result.durable_through_wal_id, None);
        assert_eq!(result.durable_through_seq, None);

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn through_wal_id_waits_for_durable_upload() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_through_wal_id",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 11);
        let orchestrator = start_orchestrator(harness);

        let result = timeout(
            Duration::from_secs(5),
            orchestrator.flush(FlushTarget::ThroughWalId(11)),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(result.durable_through_wal_id, Some(11));
        assert_eq!(result.durable_through_seq, Some(1));

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn through_current_imm_waits_for_durable_upload() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_through_current_imm",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 0);
        freeze_value_imm(&harness, b"k2", b"v2", 2, 0);
        let orchestrator = start_orchestrator(harness);

        let result = timeout(
            Duration::from_secs(5),
            orchestrator.flush(FlushTarget::ThroughCurrentImm),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(result.durable_through_wal_id, Some(0));
        assert_eq!(result.durable_through_seq, Some(2));

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_resolve_multiple_flush_waiters_on_one_durable_advance() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_multiple_waiters",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 15);
        let orchestrator = start_orchestrator(harness);

        let (first, second) = tokio::join!(
            timeout(
                Duration::from_secs(5),
                orchestrator.flush(FlushTarget::ThroughWalId(15))
            ),
            timeout(
                Duration::from_secs(5),
                orchestrator.flush(FlushTarget::ThroughWalId(15))
            )
        );

        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_eq!(first.durable_through_wal_id, Some(15));
        assert_eq!(first.durable_through_seq, Some(1));
        assert_eq!(second, first);

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_current_imm_waits_for_flush_barrier() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_checkpoint_current_imm",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 0);
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let orchestrator = start_orchestrator(harness);

        let checkpoint = timeout(
            Duration::from_secs(5),
            orchestrator
                .create_checkpoint(FlushTarget::ThroughCurrentImm, CheckpointOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();

        let after = latest_manifest_checkpoint_count(&path, object_store).await;
        assert!(checkpoint.manifest_id > 0);
        assert_eq!(after, before + 1);

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_all_waits_for_flush_barrier() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_checkpoint_all",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 21);
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let orchestrator = start_orchestrator(harness);

        let checkpoint = timeout(
            Duration::from_secs(5),
            orchestrator
                .create_checkpoint(FlushTarget::ThroughWalId(21), CheckpointOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();

        let after = latest_manifest_checkpoint_count(&path, object_store).await;
        assert!(checkpoint.manifest_id > 0);
        assert_eq!(after, before + 1);

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_all_waits_for_manifest_refresh_when_l0_is_full() {
        let settings = Settings {
            l0_max_ssts: 1,
            manifest_poll_interval: Duration::from_millis(10),
            ..Settings::default()
        };
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_checkpoint_l0_backpressure",
            settings,
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        set_remote_l0_len(&harness.path, Arc::clone(&harness.object_store), 1).await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 61);
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let orchestrator = start_orchestrator(harness);

        {
            let checkpoint = orchestrator
                .create_checkpoint(FlushTarget::ThroughWalId(61), CheckpointOptions::default());
            tokio::pin!(checkpoint);
            assert!(timeout(Duration::from_millis(100), &mut checkpoint)
                .await
                .is_err());
            let still_before =
                latest_manifest_checkpoint_count(&path, Arc::clone(&object_store)).await;
            assert_eq!(still_before, before);

            set_remote_l0_len(&path, Arc::clone(&object_store), 0).await;

            let checkpoint = timeout(Duration::from_secs(5), &mut checkpoint)
                .await
                .unwrap()
                .unwrap();
            let after = latest_manifest_checkpoint_count(&path, object_store).await;
            assert!(checkpoint.manifest_id > 0);
            assert_eq!(after, before + 1);
        }

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn fatal_upload_failure_propagates_to_flush_waiter() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_build_failure",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_merge_imm(&harness, b"k1", b"merge", 1, 31);
        let orchestrator = start_orchestrator(harness);

        let err = timeout(
            Duration::from_secs(5),
            orchestrator.flush(FlushTarget::ThroughWalId(31)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(!matches!(err, SlateDBError::Closed));

        let close_result = orchestrator.close().await;
        assert!(close_result.is_err());
    }

    #[tokio::test]
    async fn should_wait_for_manifest_refresh_before_dispatching_when_l0_is_full() {
        let settings = Settings {
            l0_max_ssts: 1,
            manifest_poll_interval: Duration::from_millis(10),
            ..Settings::default()
        };
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_l0_backpressure",
            settings,
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        set_remote_l0_len(&harness.path, Arc::clone(&harness.object_store), 1).await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 41);
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let orchestrator = start_orchestrator(harness);

        {
            let flush = orchestrator.flush(FlushTarget::ThroughWalId(41));
            tokio::pin!(flush);
            assert!(timeout(Duration::from_millis(100), &mut flush)
                .await
                .is_err());

            set_remote_l0_len(&path, object_store, 0).await;

            let result = timeout(Duration::from_secs(5), &mut flush)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.durable_through_wal_id, Some(41));
            assert_eq!(result.durable_through_seq, Some(1));
        }

        orchestrator.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_fallback_to_local_l0_tracking_when_manifest_reader_is_absent() {
        let settings = Settings {
            l0_max_ssts: 1,
            manifest_poll_interval: Duration::from_millis(10),
            ..Settings::default()
        };
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_orchestrator_local_l0_fallback",
            settings,
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        set_local_l0_len(&harness, 1);
        freeze_value_imm(&harness, b"k1", b"v1", 1, 71);
        let inner = Arc::clone(&harness.inner);
        let orchestrator = start_orchestrator_without_reader(harness);

        {
            let flush = orchestrator.flush(FlushTarget::ThroughWalId(71));
            tokio::pin!(flush);
            assert!(timeout(Duration::from_millis(100), &mut flush)
                .await
                .is_err());

            {
                let mut guard = inner.state.write();
                guard.modify(|modifier| modifier.state.manifest.value.core.l0.clear());
            }

            let result = timeout(Duration::from_secs(5), &mut flush)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.durable_through_wal_id, Some(71));
            assert_eq!(result.durable_through_seq, Some(1));
        }
        orchestrator.close().await.unwrap();
    }
}
