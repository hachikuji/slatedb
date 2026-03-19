//! Parallel L0 flush manifest sequencer.
//!
//! The sequencer owns ordered retirement of uploaded L0 tables:
//! - restore flush order using [`FlushEpoch`]
//! - apply ordered in-memory manifest state transitions
//! - persist manifest updates
//! - report durable progress
//! - create checkpoints against manifest-owned state
//!
//! It does not own:
//! - upload execution
//! - flush request semantics
//! - flush waiter bookkeeping

use super::FlushEpoch;
use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::{DbState, SsTableHandle, SsTableId};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::mem_table::ImmutableMemtable;
use crate::oracle::DbOracle;
use crate::rand::DbRand;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::transaction_manager::TransactionManager;
use crate::utils::IdGenerator;
use crate::utils::{SendSafely, WatchableOnceCell};
use log::debug;
use parking_lot::Mutex;
use slatedb_common::clock::SystemClock;
use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Successful upload result handed off to the sequencer for ordered retirement.
#[derive(Clone)]
pub(crate) struct UploadedMemtable {
    /// Ordering token assigned by the memtable flusher.
    pub(crate) epoch: FlushEpoch,
    /// Same immutable memtable that was uploaded by the uploader.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    /// SST id used for the uploaded table.
    pub(crate) sst_id: SsTableId,
    /// Handle for the uploaded SST in object storage.
    pub(crate) sst_handle: SsTableHandle,
    /// Highest sequence number present in the immutable memtable.
    pub(crate) last_seq: u64,
}

impl UploadedMemtable {
    /// Creates a new uploaded-memtable command payload.
    pub(crate) fn new(
        epoch: FlushEpoch,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_id: SsTableId,
        sst_handle: SsTableHandle,
        last_seq: u64,
    ) -> Self {
        Self {
            epoch,
            imm_memtable,
            sst_id,
            sst_handle,
            last_seq,
        }
    }
}

/// Command submitted to the sequencer.
enum SequencerCommand {
    /// One uploaded table is ready for ordered retirement.
    Uploaded(Box<UploadedMemtable>),
    /// Create a checkpoint against the current durable manifest state.
    CreateCheckpoint {
        through_epoch: Option<FlushEpoch>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
}

/// Event emitted by the sequencer.
///
/// The event stream is the shared progress/supervision surface exposed to the
/// caller. Durable frontier advances and fatal subsystem failures are reported
/// on the same channel.
#[derive(Clone, Debug)]
pub(crate) enum SequencerEvent {
    /// Durable progress advanced through a new contiguous flush frontier.
    Flushed {
        /// Highest contiguous flush epoch durably reflected in the manifest.
        through_epoch: FlushEpoch,
        /// Highest durable sequence number covered by `through_epoch`.
        through_seq: u64,
    },
    /// The sequencer encountered a fatal error and is now poisoned.
    Fatal(SlateDBError),
}

type SequencerCommandSender = mpsc::UnboundedSender<SequencerCommand>;
type SequencerCommandReceiver = mpsc::UnboundedReceiver<SequencerCommand>;
type SequencerEventSender = mpsc::UnboundedSender<SequencerEvent>;
type SequencerEventReceiver = mpsc::UnboundedReceiver<SequencerEvent>;

/// Narrow dependency bundle for the manifest sequencer.
pub(crate) struct SequencerDb {
    state: Arc<parking_lot::RwLock<DbState>>,
    db_stats: DbStats,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
    oracle: Arc<DbOracle>,
    txn_manager: Arc<TransactionManager>,
    table_store: Arc<TableStore>,
    #[allow(dead_code)]
    stat_registry: Arc<StatRegistry>,
}

impl SequencerDb {
    pub(crate) fn from_db_inner(db_inner: &Arc<DbInner>) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::clone(&db_inner.state),
            db_stats: db_inner.db_stats.clone(),
            system_clock: Arc::clone(&db_inner.system_clock),
            rand: Arc::clone(&db_inner.rand),
            oracle: Arc::clone(&db_inner.oracle),
            txn_manager: Arc::clone(&db_inner.txn_manager),
            table_store: Arc::clone(&db_inner.table_store),
            stat_registry: Arc::clone(&db_inner.stat_registry),
        })
    }
}

/// Ordered L0 retirement and manifest update subsystem.
pub(crate) struct Sequencer {
    commands: Option<SequencerCommandSender>,
    events: SequencerEventReceiver,
    poisoned: Arc<Mutex<Option<SlateDBError>>>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    task: Option<JoinHandle<Result<(), SlateDBError>>>,
}

impl Sequencer {
    /// Starts the sequencer subsystem.
    pub(crate) fn start(
        db: Arc<SequencerDb>,
        manifest: FenceableManifest,
        handle: &Handle,
    ) -> Self {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let poisoned = Arc::new(Mutex::new(None));
        let closed_result = WatchableOnceCell::new();
        let task = handle.spawn(
            SequencerTask::new(
                db,
                manifest,
                Arc::clone(&poisoned),
                closed_result.clone(),
                commands_rx,
                events_tx,
            )
            .run(),
        );

        Self {
            commands: Some(commands_tx),
            events: events_rx,
            poisoned,
            closed_result,
            task: Some(task),
        }
    }

    /// Notifies the sequencer that one uploaded table is ready for ordered retirement.
    pub(crate) async fn notify_uploaded(
        &self,
        uploaded_memtable: UploadedMemtable,
    ) -> Result<(), SlateDBError> {
        if let Some(err) = self.poisoned.lock().clone() {
            return Err(err);
        }

        self.commands
            .as_ref()
            .ok_or(SlateDBError::Closed)?
            .send_safely(
                self.closed_result.reader(),
                SequencerCommand::Uploaded(Box::new(uploaded_memtable)),
            )
    }

    /// Creates a checkpoint against the current durable manifest state.
    pub(crate) async fn create_checkpoint(
        &self,
        through_epoch: Option<FlushEpoch>,
        options: CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        if let Some(err) = self.poisoned.lock().clone() {
            return Err(err);
        }

        let (tx, rx) = oneshot::channel();
        self.commands
            .as_ref()
            .ok_or(SlateDBError::Closed)?
            .send_safely(
                self.closed_result.reader(),
                SequencerCommand::CreateCheckpoint {
                    through_epoch,
                    options,
                    sender: tx,
                },
            )?;
        rx.await.map_err(SlateDBError::ReadChannelError)?
    }

    /// Returns the shared sequencer event receiver.
    pub(crate) fn events(&mut self) -> &mut SequencerEventReceiver {
        &mut self.events
    }

    /// Closes the sequencer.
    pub(crate) async fn close(&mut self) -> Result<(), SlateDBError> {
        self.commands.take();
        let result = if let Some(task) = self.task.take() {
            match task.await {
                Ok(result) => result,
                Err(join_err) if join_err.is_cancelled() => Ok(()),
                Err(join_err) if join_err.is_panic() => Err(SlateDBError::BackgroundTaskPanic(
                    "parallel_l0_flush_sequencer".into(),
                )),
                Err(_) => Err(SlateDBError::BackgroundTaskCancelled(
                    "parallel_l0_flush_sequencer".into(),
                )),
            }
        } else {
            Ok(())
        };

        self.closed_result.write(result.clone().map(|_| ()));
        result
    }
}

struct SequencerTask {
    db: Arc<SequencerDb>,
    manifest: FenceableManifest,
    poisoned: Arc<Mutex<Option<SlateDBError>>>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    commands: SequencerCommandReceiver,
    events: SequencerEventSender,
    ready: BTreeMap<FlushEpoch, UploadedMemtable>,
    next_epoch: FlushEpoch,
    durable_through: Option<(FlushEpoch, u64)>,
    pending_checkpoints: Vec<PendingCheckpoint>,
}

impl SequencerTask {
    fn new(
        db: Arc<SequencerDb>,
        manifest: FenceableManifest,
        poisoned: Arc<Mutex<Option<SlateDBError>>>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        commands: SequencerCommandReceiver,
        events: SequencerEventSender,
    ) -> Self {
        Self {
            db,
            manifest,
            poisoned,
            closed_result,
            commands,
            events,
            ready: BTreeMap::new(),
            next_epoch: FlushEpoch(1),
            durable_through: None,
            pending_checkpoints: Vec::new(),
        }
    }

    async fn run(mut self) -> Result<(), SlateDBError> {
        loop {
            let Some(command) = self.commands.recv().await else {
                return self.write_current_manifest_safely().await;
            };

            let commands = self.drain_ready_commands(command);
            if let Err(err) = self.handle_commands(commands).await {
                return self.handle_fatal_error(err).await;
            }
        }
    }

    fn drain_ready_commands(&mut self, first_command: SequencerCommand) -> Vec<SequencerCommand> {
        let mut commands = vec![first_command];
        while let Ok(command) = self.commands.try_recv() {
            commands.push(command);
        }
        commands
    }

    async fn handle_commands(
        &mut self,
        commands: Vec<SequencerCommand>,
    ) -> Result<(), SlateDBError> {
        for command in commands {
            match command {
                SequencerCommand::Uploaded(uploaded_memtable) => {
                    self.handle_uploaded(*uploaded_memtable).await?;
                }
                SequencerCommand::CreateCheckpoint {
                    through_epoch,
                    options,
                    sender,
                } => {
                    self.handle_create_checkpoint(through_epoch, options, sender)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_uploaded(
        &mut self,
        uploaded_memtable: UploadedMemtable,
    ) -> Result<(), SlateDBError> {
        if self
            .ready
            .insert(uploaded_memtable.epoch, uploaded_memtable)
            .is_some()
        {
            return Err(SlateDBError::InvalidDBState);
        }
        self.process_ready_work().await
    }

    async fn handle_create_checkpoint(
        &mut self,
        through_epoch: Option<FlushEpoch>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        if self.checkpoint_requirement_satisfied(through_epoch) {
            let result = self.write_checkpoint_safely(&options).await;
            let _ = sender.send(result.clone());
            return result.map(|_| ());
        }

        self.pending_checkpoints.push(PendingCheckpoint {
            through_epoch,
            options,
            sender,
        });
        self.process_ready_work().await
    }

    async fn process_ready_work(&mut self) -> Result<(), SlateDBError> {
        loop {
            let Some(staged_batch) = self.take_next_ready_batch() else {
                return Ok(());
            };
            let (through_epoch, through_seq) = staged_batch
                .last()
                .map(|uploaded| (uploaded.epoch, uploaded.last_seq))
                .expect("staged batch should not be empty");
            let attached_checkpoints = self.take_satisfied_pending_checkpoints(through_epoch);
            self.apply_ready_batch(
                staged_batch,
                attached_checkpoints,
                through_epoch,
                through_seq,
            )
            .await?;
        }
    }

    fn take_next_ready_batch(&mut self) -> Option<Vec<UploadedMemtable>> {
        let mut epoch = self.next_epoch;
        let mut batch = Vec::new();
        while let Some(uploaded) = self.ready.remove(&epoch) {
            batch.push(uploaded);
            epoch = FlushEpoch(epoch.0 + 1);
        }

        if batch.is_empty() {
            None
        } else {
            self.next_epoch = epoch;
            Some(batch)
        }
    }

    fn take_satisfied_pending_checkpoints(
        &mut self,
        through_epoch: FlushEpoch,
    ) -> Vec<PendingCheckpoint> {
        let mut satisfied = Vec::new();
        let mut pending = Vec::with_capacity(self.pending_checkpoints.len());
        for checkpoint in self.pending_checkpoints.drain(..) {
            if checkpoint
                .through_epoch
                .is_none_or(|required_epoch| required_epoch <= through_epoch)
            {
                satisfied.push(checkpoint);
            } else {
                pending.push(checkpoint);
            }
        }
        self.pending_checkpoints = pending;
        satisfied
    }

    fn checkpoint_requirement_satisfied(&self, through_epoch: Option<FlushEpoch>) -> bool {
        match through_epoch {
            None => true,
            Some(required_epoch) => self
                .durable_through
                .is_some_and(|(durable_epoch, _)| durable_epoch >= required_epoch),
        }
    }

    async fn apply_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        through_epoch: FlushEpoch,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        self.apply_uploaded_state(&staged_batch)?;

        for uploaded in &staged_batch {
            uploaded.imm_memtable.notify_flush_to_l0(Ok(()));
            self.db.db_stats.immutable_memtable_flushes.inc();
        }

        match self
            .write_manifest_update_safely(
                &attached_checkpoints
                    .iter()
                    .map(|c| &c.options)
                    .collect::<Vec<_>>(),
            )
            .await
        {
            Ok(checkpoint_results) => {
                self.finish_ready_batch(
                    staged_batch,
                    attached_checkpoints,
                    checkpoint_results,
                    through_epoch,
                    through_seq,
                )
                .await
            }
            Err(err) => {
                self.fail_ready_batch(staged_batch, attached_checkpoints, err.clone())
                    .await?;
                Err(err)
            }
        }
    }

    fn apply_uploaded_state(&self, staged_batch: &[UploadedMemtable]) -> Result<(), SlateDBError> {
        let min_active_snapshot_seq = self.db.txn_manager.min_active_seq();
        let mut guard = self.db.state.write();
        guard.modify(|modifier| {
            for uploaded in staged_batch {
                let uploaded_tracker = uploaded.imm_memtable.sequence_tracker();
                let popped = modifier
                    .state
                    .imm_memtable
                    .pop_back()
                    .expect("expected imm memtable");
                assert!(Arc::ptr_eq(&popped, &uploaded.imm_memtable));
                modifier
                    .state
                    .manifest
                    .value
                    .core
                    .l0
                    .push_front(uploaded.sst_handle.clone());
                modifier.state.manifest.value.core.replay_after_wal_id =
                    uploaded.imm_memtable.recent_flushed_wal_id();

                let memtable_tick = uploaded.imm_memtable.table().last_tick();
                modifier.state.manifest.value.core.last_l0_clock_tick = cmp::max(
                    modifier.state.manifest.value.core.last_l0_clock_tick,
                    memtable_tick,
                );
                if modifier.state.manifest.value.core.last_l0_clock_tick != memtable_tick {
                    return Err(SlateDBError::InvalidClockTick {
                        last_tick: modifier.state.manifest.value.core.last_l0_clock_tick,
                        next_tick: memtable_tick,
                    });
                }

                assert!(uploaded.last_seq >= modifier.state.manifest.value.core.last_l0_seq);
                modifier.state.manifest.value.core.last_l0_seq = uploaded.last_seq;
                modifier.state.manifest.value.core.recent_snapshot_min_seq =
                    min_active_snapshot_seq.unwrap_or(uploaded.last_seq);

                modifier
                    .state
                    .manifest
                    .value
                    .core
                    .sequence_tracker
                    .extend_from(uploaded_tracker);
                let tracker = &modifier.state.manifest.value.core.sequence_tracker;
                debug!(
                    "sequencer applied uploaded state [epoch={}, last_seq={}, uploaded_tracker_len={}, manifest_tracker_len={}, manifest_tracker_first_seq={:?}, manifest_tracker_last_seq={:?}, manifest_tracker_first_ts={:?}, manifest_tracker_last_ts={:?}]",
                    uploaded.epoch.0,
                    uploaded.last_seq,
                    uploaded_tracker.len(),
                    tracker.len(),
                    tracker.first_seq(),
                    tracker.last_seq(),
                    tracker.first_ts(),
                    tracker.last_ts(),
                );
            }
            Ok(())
        })
    }

    async fn write_manifest_update_safely(
        &mut self,
        checkpoint_options: &[&CheckpointOptions],
    ) -> Result<Vec<CheckpointCreateResult>, SlateDBError> {
        loop {
            let result = self.write_manifest_update(checkpoint_options).await;
            if matches!(result, Err(SlateDBError::TransactionalObjectVersionExists)) {
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn write_manifest_update(
        &mut self,
        checkpoint_options: &[&CheckpointOptions],
    ) -> Result<Vec<CheckpointCreateResult>, SlateDBError> {
        let mut dirty = self.clone_local_manifest_for_write("manifest update");
        let mut checkpoint_results = Vec::new();
        for options in checkpoint_options {
            let id = self.db.rand.rng().gen_uuid();
            let checkpoint = self.manifest.new_checkpoint(id, options)?;
            let manifest_id = checkpoint.manifest_id;
            dirty.value.core.checkpoints.push(checkpoint);
            checkpoint_results.push(CheckpointCreateResult { id, manifest_id });
        }
        self.manifest.update(dirty).await?;
        Ok(checkpoint_results)
    }

    async fn write_current_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            let result = self.write_current_manifest().await;
            if matches!(result, Err(SlateDBError::TransactionalObjectVersionExists)) {
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn write_current_manifest(&mut self) -> Result<(), SlateDBError> {
        let dirty = self.clone_local_manifest_for_write("current manifest");
        self.manifest.update(dirty).await
    }

    fn clone_local_manifest_for_write(
        &self,
        reason: &str,
    ) -> slatedb_txn_obj::DirtyObject<crate::manifest::Manifest> {
        let dirty = {
            let rguard_state = self.db.state.read();
            rguard_state.state().manifest.clone()
        };
        debug!(
            "sequencer writing {} [l0_len={}, last_l0_seq={}, replay_after_wal_id={}, tracker_len={}, tracker_first_seq={:?}, tracker_last_seq={:?}, tracker_first_ts={:?}, tracker_last_ts={:?}]",
            reason,
            dirty.value.core.l0.len(),
            dirty.value.core.last_l0_seq,
            dirty.value.core.replay_after_wal_id,
            dirty.value.core.sequence_tracker.len(),
            dirty.value.core.sequence_tracker.first_seq(),
            dirty.value.core.sequence_tracker.last_seq(),
            dirty.value.core.sequence_tracker.first_ts(),
            dirty.value.core.sequence_tracker.last_ts(),
        );
        dirty
    }

    async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        let remote_dirty = self.manifest.prepare_dirty()?;
        debug!(
            "sequencer loading manifest [remote_l0_len={}, remote_last_l0_seq={}, remote_replay_after_wal_id={}, remote_tracker_len={}, remote_tracker_first_seq={:?}, remote_tracker_last_seq={:?}, remote_tracker_first_ts={:?}, remote_tracker_last_ts={:?}]",
            remote_dirty.value.core.l0.len(),
            remote_dirty.value.core.last_l0_seq,
            remote_dirty.value.core.replay_after_wal_id,
            remote_dirty.value.core.sequence_tracker.len(),
            remote_dirty.value.core.sequence_tracker.first_seq(),
            remote_dirty.value.core.sequence_tracker.last_seq(),
            remote_dirty.value.core.sequence_tracker.first_ts(),
            remote_dirty.value.core.sequence_tracker.last_ts(),
        );
        let mut wguard_state = self.db.state.write();
        wguard_state.merge_remote_manifest(remote_dirty);
        let merged_state = wguard_state.state();
        let merged = merged_state.core();
        debug!(
            "sequencer merged manifest [merged_l0_len={}, merged_last_l0_seq={}, merged_replay_after_wal_id={}, merged_tracker_len={}, merged_tracker_first_seq={:?}, merged_tracker_last_seq={:?}, merged_tracker_first_ts={:?}, merged_tracker_last_ts={:?}]",
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
            .db_stats
            .l0_sst_count
            .set(wguard_state.state().core().l0.len() as i64);
        Ok(())
    }

    async fn write_checkpoint_safely(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        self.load_manifest().await?;
        let mut results = self.write_manifest_update_safely(&[options]).await?;
        Ok(results
            .pop()
            .expect("checkpoint write should return exactly one result"))
    }

    async fn finish_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        checkpoint_results: Vec<CheckpointCreateResult>,
        through_epoch: FlushEpoch,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        self.durable_through = Some((through_epoch, through_seq));
        for uploaded in staged_batch {
            uploaded.imm_memtable.table().notify_durable(Ok(()));
            self.db.oracle.advance_durable_seq(uploaded.last_seq);
        }
        for (checkpoint, result) in attached_checkpoints
            .into_iter()
            .zip(checkpoint_results.into_iter())
        {
            let _ = checkpoint.sender.send(Ok(result));
        }
        self.events.send_safely(
            self.closed_result.reader(),
            SequencerEvent::Flushed {
                through_epoch,
                through_seq,
            },
        )?;
        Ok(())
    }

    async fn fail_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        err: SlateDBError,
    ) -> Result<(), SlateDBError> {
        if matches!(err, SlateDBError::Fenced) {
            for uploaded in &staged_batch {
                if let Err(delete_err) = self.db.table_store.delete_sst(&uploaded.sst_id).await {
                    log::warn!(
                        "failed to delete fenced SST [id={:?}, error={:?}]",
                        uploaded.sst_id,
                        delete_err
                    );
                }
            }
            self.load_manifest().await?;
        }

        for uploaded in staged_batch {
            uploaded
                .imm_memtable
                .table()
                .notify_durable(Err(err.clone()));
        }
        for checkpoint in attached_checkpoints {
            let _ = checkpoint.sender.send(Err(err.clone()));
        }
        Ok(())
    }

    async fn handle_fatal_error(&mut self, err: SlateDBError) -> Result<(), SlateDBError> {
        *self.poisoned.lock() = Some(err.clone());
        self.closed_result.write(Err(err.clone()));
        let _ = self.events.send_safely(
            self.closed_result.reader(),
            SequencerEvent::Fatal(err.clone()),
        );
        Err(err)
    }
}

struct PendingCheckpoint {
    through_epoch: Option<FlushEpoch>,
    options: CheckpointOptions,
    sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
}

#[cfg(test)]
mod tests {
    use super::{
        FlushEpoch, Sequencer, SequencerCommand, SequencerDb, SequencerEvent, UploadedMemtable,
    };
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::{ManifestCore, SsTableId};
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::IdGenerator;
    use crate::utils::SendSafely;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::clock::SystemClock;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    struct TestHarness {
        inner: Arc<DbInner>,
        db: Arc<SequencerDb>,
        manifest: FenceableManifest,
        object_store: Arc<dyn ObjectStore>,
        path: String,
    }

    async fn setup_harness(path: &str, fp_registry: Arc<FailPointRegistry>) -> TestHarness {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = path.to_string();
        let settings = Settings::default();
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
        let manifest_dirty = stored_manifest.prepare_dirty().unwrap();
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            PathResolver::new(Path::from(path.clone())),
            Arc::clone(&fp_registry),
            None,
        ));
        let (write_tx, _) = mpsc::unbounded_channel();
        let inner = Arc::new(
            DbInner::new(
                settings.clone(),
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                manifest_dirty,
                write_tx,
                Arc::clone(&stat_registry),
                fp_registry,
                None,
            )
            .await
            .unwrap(),
        );
        let db = SequencerDb::from_db_inner(&inner);
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let stored_manifest =
            StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let manifest = FenceableManifest::init_writer(
            stored_manifest,
            Duration::from_secs(300),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        TestHarness {
            inner,
            db,
            manifest,
            object_store,
            path,
        }
    }

    async fn load_writer_manifest(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> FenceableManifest {
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store));
        let stored_manifest =
            StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        FenceableManifest::init_writer(
            stored_manifest,
            Duration::from_secs(300),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap()
    }

    async fn latest_manifest_checkpoint_count(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> usize {
        let manifest_store = ManifestStore::new(&Path::from(path), object_store);
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        manifest.core.checkpoints.len()
    }

    fn freeze_imm(
        harness: &TestHarness,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> Arc<crate::mem_table::ImmutableMemtable> {
        let mut guard = harness.inner.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        guard.freeze_memtable(0).unwrap();
        guard.state().imm_memtable.front().cloned().unwrap()
    }

    async fn next_uploaded_memtable(
        harness: &TestHarness,
        epoch: u64,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> UploadedMemtable {
        let imm_memtable = freeze_imm(harness, key, value, seq);
        let sst_id = SsTableId::Compacted(
            harness
                .db
                .rand
                .rng()
                .gen_ulid(harness.db.system_clock.as_ref()),
        );
        let sst_handle = harness
            .inner
            .flush_imm_table(&sst_id, imm_memtable.table(), true)
            .await
            .unwrap();
        let last_seq = imm_memtable.table().last_seq().unwrap();
        UploadedMemtable::new(
            FlushEpoch(epoch),
            imm_memtable,
            sst_id,
            sst_handle,
            last_seq,
        )
    }

    #[tokio::test]
    async fn should_emit_flushed_event_for_contiguous_uploads() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_sequencer_flush_event",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;

        let mut sequencer = Sequencer::start(
            Arc::clone(&harness.db),
            harness.manifest,
            &Handle::current(),
        );
        sequencer.notify_uploaded(uploaded).await.unwrap();

        let event = timeout(Duration::from_secs(5), sequencer.events().recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            SequencerEvent::Flushed {
                through_epoch,
                through_seq,
            } => {
                assert_eq!(through_epoch, FlushEpoch(1));
                assert_eq!(through_seq, 1);
            }
            SequencerEvent::Fatal(err) => panic!("unexpected fatal sequencer event: {err:?}"),
        }

        sequencer.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_wait_for_missing_epoch_before_flushing() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_sequencer_gap",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded1 = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;
        let uploaded2 = next_uploaded_memtable(&harness, 2, b"k2", b"v2", 2).await;

        let mut sequencer = Sequencer::start(
            Arc::clone(&harness.db),
            harness.manifest,
            &Handle::current(),
        );
        sequencer.notify_uploaded(uploaded2).await.unwrap();
        assert!(
            timeout(Duration::from_millis(100), sequencer.events().recv())
                .await
                .is_err()
        );

        sequencer.notify_uploaded(uploaded1).await.unwrap();
        let event = timeout(Duration::from_secs(5), sequencer.events().recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            SequencerEvent::Flushed {
                through_epoch,
                through_seq,
            } => {
                assert_eq!(through_epoch, FlushEpoch(2));
                assert_eq!(through_seq, 2);
            }
            SequencerEvent::Fatal(err) => panic!("unexpected fatal sequencer event: {err:?}"),
        }

        sequencer.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_create_checkpoint_immediately_when_no_barrier_is_required() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_sequencer_checkpoint_immediate",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let mut sequencer = Sequencer::start(
            Arc::clone(&harness.db),
            harness.manifest,
            &Handle::current(),
        );

        let checkpoint = timeout(
            Duration::from_secs(5),
            sequencer.create_checkpoint(None, CheckpointOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();

        let after =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        assert_eq!(after, before + 1);
        assert!(checkpoint.manifest_id > 0);

        sequencer.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_wait_for_checkpoint_barrier_and_attach_to_flush_batch() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_sequencer_checkpoint_barrier",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;

        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let mut sequencer = Sequencer::start(
            Arc::clone(&harness.db),
            harness.manifest,
            &Handle::current(),
        );

        let (tx, rx) = oneshot::channel();
        sequencer
            .commands
            .as_ref()
            .unwrap()
            .send_safely(
                sequencer.closed_result.reader(),
                SequencerCommand::CreateCheckpoint {
                    through_epoch: Some(FlushEpoch(1)),
                    options: CheckpointOptions::default(),
                    sender: tx,
                },
            )
            .unwrap();

        tokio::task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), sequencer.events().recv())
                .await
                .is_err()
        );

        sequencer.notify_uploaded(uploaded).await.unwrap();

        let event = timeout(Duration::from_secs(5), sequencer.events().recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            SequencerEvent::Flushed {
                through_epoch,
                through_seq,
            } => {
                assert_eq!(through_epoch, FlushEpoch(1));
                assert_eq!(through_seq, 1);
            }
            SequencerEvent::Fatal(err) => panic!("unexpected fatal sequencer event: {err:?}"),
        }

        let checkpoint = rx.await.unwrap().unwrap();
        let after =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        assert_eq!(after, before + 1);
        assert!(checkpoint.manifest_id > 0);

        sequencer.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_emit_fatal_event_when_sequencer_is_fenced() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_sequencer_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;

        let mut sequencer = Sequencer::start(
            Arc::clone(&harness.db),
            harness.manifest,
            &Handle::current(),
        );

        let _fence = load_writer_manifest(&harness.path, Arc::clone(&harness.object_store)).await;
        sequencer.notify_uploaded(uploaded).await.unwrap();

        let event = timeout(Duration::from_secs(5), sequencer.events().recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            SequencerEvent::Fatal(SlateDBError::Fenced) => {}
            SequencerEvent::Fatal(err) => panic!("unexpected fatal sequencer error: {err:?}"),
            SequencerEvent::Flushed { .. } => panic!("unexpected flush event after fence"),
        }

        let err = sequencer
            .create_checkpoint(None, CheckpointOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, SlateDBError::Fenced));
    }
}
