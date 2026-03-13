use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::SsTableId;
use crate::dispatcher::{MessageFactory, MessageHandler};
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::oracle::Oracle;
use crate::utils::IdGenerator;
use async_trait::async_trait;
use fail_parallel::fail_point;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinSet;
use tracing::instrument;

pub(crate) const MEMTABLE_FLUSHER_TASK_NAME: &str = "memtable_writer";

#[derive(Debug)]
pub(crate) enum MemtableFlushMsg {
    FlushImmutableMemtables {
        sender: Option<Sender<Result<(), SlateDBError>>>,
    },
    CreateCheckpoint {
        options: CheckpointOptions,
        sender: Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
    PollManifest,
}

pub(crate) struct MemtableFlusher {
    db_inner: Arc<DbInner>,
    manifest: FenceableManifest,
}

const MAX_BUILD_IN_FLIGHT: usize = 2;
const MAX_UPLOAD_IN_FLIGHT: usize = 1;

struct PendingBuiltFlush {
    flush_seq: u64,
    imm_memtable: Arc<crate::mem_table::ImmutableMemtable>,
    sst_id: SsTableId,
    flush_start: Instant,
    wal_wait_elapsed_ms: u64,
    built: crate::flush::BuiltImmTable,
}

struct ReadyForCommit {
    flush_seq: u64,
    imm_memtable: Arc<crate::mem_table::ImmutableMemtable>,
    sst_id: SsTableId,
    sst_handle: crate::db_state::SsTableHandle,
    output_bytes: u64,
    flush_stats: crate::flush::FlushImmTableStats,
    flush_start: Instant,
    wal_wait_elapsed_ms: u64,
    last_seq: u64,
}

impl MemtableFlusher {
    pub(crate) fn new(db_inner: Arc<DbInner>, manifest: FenceableManifest) -> Self {
        Self { db_inner, manifest }
    }

    pub(crate) async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        let mut wguard_state = self.db_inner.state.write();
        wguard_state.merge_remote_manifest(self.manifest.prepare_dirty()?);
        self.db_inner
            .db_stats
            .l0_sst_count
            .set(wguard_state.state().core().l0.len() as i64);
        Ok(())
    }

    async fn write_checkpoint(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let mut dirty = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().manifest.clone()
        };
        let id = self.db_inner.rand.rng().gen_uuid();
        let checkpoint = self.manifest.new_checkpoint(id, options)?;
        let manifest_id = checkpoint.manifest_id;
        dirty.value.core.checkpoints.push(checkpoint);
        self.manifest.update(dirty).await?;
        Ok(CheckpointCreateResult { id, manifest_id })
    }

    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        let dirty = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().manifest.clone()
        };
        self.manifest.update(dirty).await
    }

    pub(crate) async fn write_checkpoint_safely(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        loop {
            self.load_manifest().await?;
            let result = self.write_checkpoint(options).await;
            if matches!(result, Err(SlateDBError::TransactionalObjectVersionExists)) {
                debug!("conflicting manifest version. updating and retrying write again.");
            } else {
                return result;
            }
        }
    }

    pub(crate) async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            let result = self.write_manifest().await;
            if matches!(result, Err(SlateDBError::TransactionalObjectVersionExists)) {
                debug!("conflicting manifest version. updating and retrying write again.");
                self.db_inner.db_stats.l0_flush_manifest_retries.inc();
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn flush_imm_memtables_to_l0(&mut self) -> Result<(), SlateDBError> {
        let mut next_flush_seq = 0u64;
        let mut next_commit_seq = 0u64;
        let mut build_tasks = JoinSet::new();
        let mut upload_tasks = JoinSet::new();
        let mut built_ready: BTreeMap<u64, PendingBuiltFlush> = BTreeMap::new();
        let mut uploaded_ready: BTreeMap<u64, ReadyForCommit> = BTreeMap::new();

        loop {
            self.db_inner
                .db_stats
                .l0_flush_build_inflight
                .set(build_tasks.len() as i64);
            self.db_inner
                .db_stats
                .l0_flush_upload_inflight
                .set(upload_tasks.len() as i64);
            self.db_inner
                .db_stats
                .l0_flush_built_ready
                .set(built_ready.len() as i64);
            self.db_inner
                .db_stats
                .l0_flush_uploaded_ready
                .set(uploaded_ready.len() as i64);
            while build_tasks.len() < MAX_BUILD_IN_FLIGHT {
                let scheduled_count = build_tasks.len()
                    + upload_tasks.len()
                    + built_ready.len()
                    + uploaded_ready.len();
                let maybe_imm_memtable = {
                    let rguard = self.db_inner.state.read();
                    if rguard.state().core().l0.len() + scheduled_count
                        >= self.db_inner.settings.l0_max_ssts
                    {
                        warn!(
                            "won't flush imm to l0 because too many l0 files [l0_len={}, in_flight={}, l0_max_ssts={}]",
                            rguard.state().core().l0.len(),
                            scheduled_count,
                            self.db_inner.settings.l0_max_ssts
                        );
                        rguard.state().core().log_db_runs();
                        None
                    } else {
                        rguard
                            .state()
                            .imm_memtable
                            .iter()
                            .rev()
                            .nth(scheduled_count)
                            .cloned()
                    }
                };
                let Some(imm_memtable) = maybe_imm_memtable else {
                    break;
                };

                let flush_start = Instant::now();
                let metadata = imm_memtable.table().metadata();
                self.db_inner
                    .db_stats
                    .l0_flush_input_rows_last
                    .set(metadata.entry_num as u64);
                self.db_inner
                    .db_stats
                    .l0_flush_input_bytes
                    .add(metadata.entries_size_in_bytes as u64);
                self.db_inner
                    .db_stats
                    .l0_flush_input_bytes_last
                    .set(metadata.entries_size_in_bytes as u64);

                let wal_wait_start = Instant::now();
                if self.db_inner.wal_enabled {
                    let last_seq = imm_memtable.table().last_seq().unwrap_or(0);
                    if self.db_inner.oracle.last_remote_persisted_seq() < last_seq {
                        self.db_inner.flush_wals().await?;
                        assert!(
                            self.db_inner.oracle.last_remote_persisted_seq() >= last_seq,
                            "flush_wals did not flush up to the last seq in the imm memtable"
                        );
                    }
                }
                let wal_wait_elapsed_ms = wal_wait_start.elapsed().as_millis() as u64;
                self.db_inner
                    .db_stats
                    .l0_flush_wal_wait_ms
                    .add(wal_wait_elapsed_ms);
                self.db_inner
                    .db_stats
                    .l0_flush_wal_wait_ms_last
                    .set(wal_wait_elapsed_ms);

                let sst_id = SsTableId::Compacted(
                    self.db_inner
                        .rand
                        .rng()
                        .gen_ulid(self.db_inner.system_clock.as_ref()),
                );
                let flush_seq = next_flush_seq;
                next_flush_seq += 1;
                let db_inner = Arc::clone(&self.db_inner);
                let imm_for_task = Arc::clone(&imm_memtable);
                build_tasks.spawn(async move {
                    let built = db_inner
                        .build_imm_table_with_stats(imm_for_task.table())
                        .await?;
                    Ok::<_, SlateDBError>(PendingBuiltFlush {
                        flush_seq,
                        imm_memtable,
                        sst_id,
                        flush_start,
                        wal_wait_elapsed_ms,
                        built,
                    })
                });
            }

            if upload_tasks.len() < MAX_UPLOAD_IN_FLIGHT {
                if let Some((&flush_seq, _pending)) = built_ready.iter().next() {
                    let pending = built_ready
                        .remove(&flush_seq)
                        .expect("pending build exists");
                    let db_inner = Arc::clone(&self.db_inner);
                    upload_tasks.spawn(async move {
                        let last_seq = pending
                            .imm_memtable
                            .table()
                            .last_seq()
                            .expect("flush of l0 with no entries");
                        let (sst_handle, flush_stats, output_bytes) = db_inner
                            .upload_built_imm_table_with_stats(&pending.sst_id, pending.built, true)
                            .await?;
                        Ok::<_, SlateDBError>(ReadyForCommit {
                            flush_seq: pending.flush_seq,
                            imm_memtable: pending.imm_memtable,
                            sst_id: pending.sst_id,
                            sst_handle,
                            output_bytes,
                            flush_stats,
                            flush_start: pending.flush_start,
                            wal_wait_elapsed_ms: pending.wal_wait_elapsed_ms,
                            last_seq,
                        })
                    });
                }
            }

            let mut batch = Vec::new();
            while let Some(ready) = uploaded_ready.remove(&next_commit_seq) {
                batch.push(ready);
                next_commit_seq += 1;
            }
            if !batch.is_empty() {
                self.commit_uploaded_flushes(batch).await?;
                continue;
            }

            let no_more_schedulable = {
                let scheduled_count = build_tasks.len()
                    + upload_tasks.len()
                    + built_ready.len()
                    + uploaded_ready.len();
                let rguard = self.db_inner.state.read();
                rguard
                    .state()
                    .imm_memtable
                    .iter()
                    .rev()
                    .nth(scheduled_count)
                    .is_none()
            };
            if no_more_schedulable
                && build_tasks.is_empty()
                && upload_tasks.is_empty()
                && built_ready.is_empty()
                && uploaded_ready.is_empty()
            {
                break;
            }

            if build_tasks.is_empty()
                && upload_tasks.is_empty()
                && built_ready.is_empty()
                && uploaded_ready.is_empty()
            {
                break;
            }

            tokio::select! {
                Some(result) = build_tasks.join_next(), if !build_tasks.is_empty() => {
                    let pending = result.expect("build task panicked")?;
                    built_ready.insert(pending.flush_seq, pending);
                }
                Some(result) = upload_tasks.join_next(), if !upload_tasks.is_empty() => {
                    let uploaded = result.expect("upload task panicked")?;
                    uploaded_ready.insert(uploaded.flush_seq, uploaded);
                }
            }
        }

        Ok(())
    }

    async fn commit_uploaded_flushes(
        &mut self,
        batch: Vec<ReadyForCommit>,
    ) -> Result<(), SlateDBError> {
        let publish_start = Instant::now();
        let publish_share_count = batch.len() as u64;
        self.db_inner
            .db_stats
            .l0_flush_commit_batch_size_last
            .set(publish_share_count);
        let min_active_snapshot_seq = self.db_inner.txn_manager.min_active_seq();
        {
            let mut guard = self.db_inner.state.write();
            guard.modify(|modifier| {
                for ready in &batch {
                    let popped = modifier
                        .state
                        .imm_memtable
                        .pop_back()
                        .expect("expected imm memtable");
                    assert!(Arc::ptr_eq(&popped, &ready.imm_memtable));
                    modifier
                        .state
                        .manifest
                        .value
                        .core
                        .l0
                        .push_front(ready.sst_handle.clone());
                    modifier.state.manifest.value.core.replay_after_wal_id =
                        ready.imm_memtable.recent_flushed_wal_id();

                    let memtable_tick = ready.imm_memtable.table().last_tick();
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

                    assert!(ready.last_seq >= modifier.state.manifest.value.core.last_l0_seq);
                    modifier.state.manifest.value.core.last_l0_seq = ready.last_seq;
                    modifier.state.manifest.value.core.recent_snapshot_min_seq =
                        min_active_snapshot_seq.unwrap_or(ready.last_seq);
                    modifier
                        .state
                        .manifest
                        .value
                        .core
                        .sequence_tracker
                        .extend_from(ready.imm_memtable.sequence_tracker());
                }

                Ok(())
            })?;
        }
        let publish_elapsed_ms = publish_start.elapsed().as_millis() as u64;
        let publish_share_ms = publish_elapsed_ms / publish_share_count.max(1);
        for ready in &batch {
            self.record_uploaded_flush_metrics(ready, publish_share_ms);
            ready.imm_memtable.notify_flush_to_l0(Ok(()));
            self.db_inner.db_stats.immutable_memtable_flushes.inc();
        }

        let manifest_start = Instant::now();
        match self.write_manifest_safely().await {
            Ok(_) => {
                let manifest_elapsed_ms = manifest_start.elapsed().as_millis() as u64;
                let manifest_share_ms = manifest_elapsed_ms / publish_share_count.max(1);
                for ready in &batch {
                    self.db_inner
                        .db_stats
                        .l0_flush_manifest_ms
                        .add(manifest_share_ms);
                    self.db_inner
                        .db_stats
                        .l0_flush_manifest_ms_last
                        .set(manifest_share_ms);
                    let total_elapsed_ms = ready.flush_start.elapsed().as_millis() as u64;
                    self.db_inner
                        .db_stats
                        .l0_flush_total_ms
                        .add(total_elapsed_ms);
                    self.db_inner
                        .db_stats
                        .l0_flush_total_ms_last
                        .set(total_elapsed_ms);
                    debug!(
                        "flushed imm memtable to l0 [flush_seq={}, input_bytes={}, output_bytes={}, wal_wait_ms={}, iter_setup_ms={}, row_loop_ms={}, finish_block_ms={}, footer_ms={}, encode_ms={}, put_ms={}, cache_ms={}, write_ms={}, publish_ms={}, manifest_ms={}, total_ms={}, sst_id={:?}]",
                        ready.flush_seq,
                        ready.imm_memtable.table().metadata().entries_size_in_bytes,
                        ready.output_bytes,
                        ready.wal_wait_elapsed_ms,
                        ready.flush_stats.iter_setup_ms,
                        ready.flush_stats.row_loop_ms,
                        ready.flush_stats.finish_block_ms,
                        ready.flush_stats.footer_ms,
                        ready.flush_stats.encode_ms,
                        ready.flush_stats.put_ms,
                        ready.flush_stats.cache_ms,
                        ready.flush_stats.write_ms,
                        publish_share_ms,
                        manifest_share_ms,
                        total_elapsed_ms,
                        ready.sst_id,
                    );
                    ready.imm_memtable.table().notify_durable(Ok(()));
                    self.db_inner.oracle.advance_durable_seq(ready.last_seq);
                }
            }
            Err(err) => {
                for ready in &batch {
                    if matches!(err, SlateDBError::Fenced) {
                        if let Err(delete_err) =
                            self.db_inner.table_store.delete_sst(&ready.sst_id).await
                        {
                            warn!(
                                "failed to delete fenced SST [id={:?}, error={:?}]",
                                ready.sst_id, delete_err
                            );
                        }
                    }
                    ready.imm_memtable.table().notify_durable(Err(err.clone()));
                }
                if matches!(err, SlateDBError::Fenced) {
                    self.load_manifest().await?;
                }
                return Err(err);
            }
        }

        Ok(())
    }

    fn record_uploaded_flush_metrics(&self, ready: &ReadyForCommit, publish_elapsed_ms: u64) {
        self.db_inner
            .db_stats
            .l0_flush_iter_setup_ms
            .add(ready.flush_stats.iter_setup_ms);
        self.db_inner
            .db_stats
            .l0_flush_iter_setup_ms_last
            .set(ready.flush_stats.iter_setup_ms);
        self.db_inner
            .db_stats
            .l0_flush_row_loop_ms
            .add(ready.flush_stats.row_loop_ms);
        self.db_inner
            .db_stats
            .l0_flush_row_loop_ms_last
            .set(ready.flush_stats.row_loop_ms);
        self.db_inner
            .db_stats
            .l0_flush_finish_block_ms
            .add(ready.flush_stats.finish_block_ms);
        self.db_inner
            .db_stats
            .l0_flush_finish_block_ms_last
            .set(ready.flush_stats.finish_block_ms);
        self.db_inner
            .db_stats
            .l0_flush_footer_ms
            .add(ready.flush_stats.footer_ms);
        self.db_inner
            .db_stats
            .l0_flush_footer_ms_last
            .set(ready.flush_stats.footer_ms);
        self.db_inner
            .db_stats
            .l0_flush_put_ms
            .add(ready.flush_stats.put_ms);
        self.db_inner
            .db_stats
            .l0_flush_put_ms_last
            .set(ready.flush_stats.put_ms);
        self.db_inner
            .db_stats
            .l0_flush_cache_ms
            .add(ready.flush_stats.cache_ms);
        self.db_inner
            .db_stats
            .l0_flush_cache_ms_last
            .set(ready.flush_stats.cache_ms);
        self.db_inner
            .db_stats
            .l0_flush_output_bytes
            .add(ready.output_bytes);
        self.db_inner
            .db_stats
            .l0_flush_output_bytes_last
            .set(ready.output_bytes);
        self.db_inner
            .db_stats
            .l0_flush_publish_ms
            .add(publish_elapsed_ms);
        self.db_inner
            .db_stats
            .l0_flush_publish_ms_last
            .set(publish_elapsed_ms);
    }

    async fn flush_and_record(&mut self) -> Result<(), SlateDBError> {
        fail_point!(
            Arc::clone(&self.db_inner.fp_registry),
            "flush-memtable-to-l0"
        );
        let result = self.flush_imm_memtables_to_l0().await;
        if let Err(err) = &result {
            error!("error from memtable flush [error={:?}]", err);
        }
        result
    }
}

#[async_trait]
impl MessageHandler<MemtableFlushMsg> for MemtableFlusher {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<MemtableFlushMsg>>)> {
        vec![(
            self.db_inner.settings.manifest_poll_interval,
            Box::new(|| MemtableFlushMsg::PollManifest),
        )]
    }

    async fn handle(&mut self, message: MemtableFlushMsg) -> Result<(), SlateDBError> {
        match message {
            MemtableFlushMsg::PollManifest => {
                self.load_manifest().await?;
                self.flush_and_record().await
            }
            MemtableFlushMsg::FlushImmutableMemtables { sender } => {
                let result = self.flush_and_record().await;
                if let Some(rsp_sender) = sender {
                    let res = rsp_sender.send(result.clone());
                    if let Err(Err(err)) = res {
                        error!("error sending flush response [error={:?}]", err);
                    }
                }
                result
            }
            MemtableFlushMsg::CreateCheckpoint { options, sender } => {
                let write_result = self.write_checkpoint_safely(&options).await;
                if let Err(Err(e)) = sender.send(write_result.clone()) {
                    error!("Failed to send checkpoint error [error={:?}]", e);
                }
                write_result.map(|_| ())
            }
        }
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, MemtableFlushMsg>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let error = result.clone().err().unwrap_or(SlateDBError::Closed);
        // drain remaining messages
        while let Some(message) = messages.next().await {
            match message {
                MemtableFlushMsg::CreateCheckpoint { options: _, sender } => {
                    let _ = sender.send(Err(error.clone()));
                }
                MemtableFlushMsg::FlushImmutableMemtables {
                    sender: Some(sender),
                } => {
                    let _ = sender.send(Err(error.clone()));
                }
                _ => (),
            }
        }
        if let Err(err) = self.write_manifest_safely().await {
            error!("error writing manifest on shutdown [err={}]", err);
        }
        info!("memtable flush thread exiting [result={:?}]", result);

        // notify in-memory memtables of error
        let state = self.db_inner.state.read();
        debug!(
            "notifying in-memory memtable of shutdown [result={:?}]",
            result
        );
        state.memtable().table().notify_durable(Err(error.clone()));
        for imm_table in state.state().imm_memtable.iter() {
            debug!(
                "notifying imm memtable of shutdown [last_wal_id={}, error={:?}]",
                imm_table.recent_flushed_wal_id(),
                error,
            );
            imm_table.notify_flush_to_l0(Err(error.clone()));
            imm_table.table().notify_durable(Err(error.clone()));
        }
        Ok(())
    }
}
