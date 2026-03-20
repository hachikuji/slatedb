//! Parallel L0 flush SST uploader.
//!
//! The uploader is intentionally narrow in scope:
//! - build an SST from an immutable memtable
//! - upload the SST to object storage
//! - report successful completion
//!
//! It does not own:
//! - manifest mutation
//! - checkpoint creation
//! - flush waiter bookkeeping
//! - timeout enforcement

use super::FlushEpoch;
use crate::db::DbInner;
use crate::db_state::{DbState, SsTableHandle, SsTableId};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::format::sst::EncodedSsTable;
use crate::iter::RowEntryIterator;
use crate::mem_table::{ImmutableMemtable, KVTable};
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorRequiredIterator};
use crate::oracle::{DbOracle, Oracle};
use crate::rand::DbRand;
use crate::reader::DbStateReader;
use crate::retention_iterator::RetentionIterator;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::transaction_manager::TransactionManager;
use crate::utils::{SendSafely, WatchableOnceCell, WatchableOnceCellReader};
use log::{error, info};
use parking_lot::Mutex;
use slatedb_common::clock::SystemClock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

/// One immutable-memtable upload request submitted to the uploader.
pub(crate) struct UploadJob {
    /// Ordering token assigned by the memtable flusher.
    pub(crate) epoch: FlushEpoch,
    /// Immutable memtable to build into an SST.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    /// Preallocated SST id to use for the uploaded table.
    pub(crate) sst_id: SsTableId,
}

impl UploadJob {
    /// Creates a new upload job.
    pub(crate) fn new(
        epoch: FlushEpoch,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_id: SsTableId,
    ) -> Self {
        Self {
            epoch,
            imm_memtable,
            sst_id,
        }
    }
}

#[derive(Clone)]
/// Successful completion of one upload job.
pub(crate) struct UploadSuccess {
    /// Ordering token copied from the submitted job.
    pub(crate) epoch: FlushEpoch,
    /// Same immutable memtable that was supplied in the job.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    /// SST id used for the uploaded table.
    pub(crate) sst_id: SsTableId,
    /// Handle for the uploaded SST in object storage.
    pub(crate) sst_handle: SsTableHandle,
    /// Highest sequence number present in the immutable memtable.
    pub(crate) last_seq: u64,
}

#[derive(Clone)]
/// Event emitted by the uploader.
///
/// The event stream is the only completion/supervision surface exposed to the
/// caller. Successful uploads and fatal subsystem failures are reported on the
/// same channel.
pub(crate) enum UploaderEvent {
    /// One upload job completed successfully.
    Uploaded(Box<UploadSuccess>),
    /// The uploader encountered a fatal error and is now poisoned.
    Fatal(SlateDBError),
}

type UploadJobSender = mpsc::UnboundedSender<UploadJob>;
type UploadJobReceiver = Arc<AsyncMutex<mpsc::UnboundedReceiver<UploadJob>>>;
type UploaderEventSender = mpsc::UnboundedSender<UploaderEvent>;
type UploaderEventReceiver = mpsc::UnboundedReceiver<UploaderEvent>;

/// Narrow dependency bundle for the SST uploader.
pub(crate) struct UploaderDb {
    state: Arc<parking_lot::RwLock<DbState>>,
    settings: crate::config::Settings,
    table_store: Arc<TableStore>,
    db_stats: DbStats,
    system_clock: Arc<dyn SystemClock>,
    mono_clock: Arc<crate::clock::MonotonicClock>,
    rand: Arc<DbRand>,
    oracle: Arc<DbOracle>,
    txn_manager: Arc<TransactionManager>,
    #[allow(dead_code)]
    stat_registry: Arc<StatRegistry>,
}

impl UploaderDb {
    pub(crate) fn from_db_inner(db_inner: &Arc<DbInner>) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::clone(&db_inner.state),
            settings: db_inner.settings.clone(),
            table_store: Arc::clone(&db_inner.table_store),
            db_stats: db_inner.db_stats.clone(),
            system_clock: Arc::clone(&db_inner.system_clock),
            mono_clock: Arc::clone(&db_inner.mono_clock),
            rand: Arc::clone(&db_inner.rand),
            oracle: Arc::clone(&db_inner.oracle),
            txn_manager: Arc::clone(&db_inner.txn_manager),
            stat_registry: Arc::clone(&db_inner.stat_registry),
        })
    }

    async fn build_imm_sst(&self, imm_table: Arc<KVTable>) -> Result<EncodedSsTable, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = self.iter_imm_table(imm_table).await?;
        while let Some(entry) = iter.next().await? {
            sst_builder.add(entry).await?;
        }

        sst_builder.build().await
    }

    async fn upload_compacted_sst(
        &self,
        id: &SsTableId,
        imm_table: Arc<KVTable>,
        encoded_sst: EncodedSsTable,
        write_cache: bool,
    ) -> Result<SsTableHandle, SlateDBError> {
        let handle = self
            .table_store
            .write_sst(id, encoded_sst, write_cache)
            .await?;
        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());
        Ok(handle)
    }

    async fn iter_imm_table(
        &self,
        imm_table: Arc<KVTable>,
    ) -> Result<RetentionIterator<Box<dyn RowEntryIterator>>, SlateDBError> {
        let state = self.state.read().view();
        let durable_seq = self.oracle.last_remote_persisted_seq();
        let min_retention_seq = match self.txn_manager.min_active_seq() {
            Some(active_seq) => Some(active_seq.min(durable_seq)),
            None => Some(durable_seq),
        };

        let merge_iter = if let Some(merge_operator) = self.settings.merge_operator.clone() {
            Box::new(MergeOperatorIterator::new(
                merge_operator,
                imm_table.iter(),
                false,
                imm_table.last_tick(),
                min_retention_seq,
            ))
        } else {
            Box::new(MergeOperatorRequiredIterator::new(imm_table.iter()))
                as Box<dyn RowEntryIterator>
        };
        let mut iter = RetentionIterator::new(
            merge_iter,
            None,
            min_retention_seq,
            false,
            imm_table.last_tick(),
            Arc::clone(&self.system_clock),
            Arc::new(state.core().sequence_tracker.clone()),
        )
        .await?;
        iter.init().await?;
        Ok(iter)
    }
}

pub(crate) struct Uploader {
    jobs: Option<UploadJobSender>,
    events: UploaderEventReceiver,
    poisoned: Arc<Mutex<Option<SlateDBError>>>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    shutdown: CancellationToken,
    supervisor: Option<JoinHandle<Result<(), SlateDBError>>>,
}

impl Uploader {
    /// Starts the uploader subsystem and spawns `worker_count` worker tasks plus
    /// a supervisor task on the provided runtime handle.
    ///
    /// Upload attempts use a fixed retry backoff. Fatal worker failures poison
    /// the uploader and are emitted as [`UploaderEvent::Fatal`].
    pub(crate) fn start(
        db: Arc<UploaderDb>,
        worker_count: usize,
        retry_backoff: Duration,
        handle: &Handle,
    ) -> Self {
        let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let poisoned = Arc::new(Mutex::new(None));
        let closed_result = WatchableOnceCell::new();
        let shutdown = CancellationToken::new();
        let mut workers = JoinSet::new();
        let jobs_rx = Arc::new(AsyncMutex::new(jobs_rx));

        for worker_id in 0..worker_count {
            let worker = UploadWorker::new(
                Arc::clone(&db),
                shutdown.clone(),
                retry_backoff,
                closed_result.reader(),
            );
            let jobs = Arc::clone(&jobs_rx);
            let events = events_tx.clone();
            workers.spawn(async move { worker.run(worker_id, jobs, events).await });
        }

        let supervisor = handle.spawn(Self::run_supervisor(
            workers,
            Arc::clone(&poisoned),
            events_tx,
            closed_result.clone(),
            shutdown.clone(),
        ));

        Self {
            jobs: Some(jobs_tx),
            events: events_rx,
            poisoned,
            closed_result,
            shutdown,
            supervisor: Some(supervisor),
        }
    }

    /// Submits a new upload job.
    ///
    /// Returns an error if the uploader has already been poisoned or if the job
    /// channel has been closed.
    pub(crate) async fn submit(&self, job: UploadJob) -> Result<(), SlateDBError> {
        if let Some(err) = self.poisoned.lock().clone() {
            return Err(err);
        }

        self.jobs
            .as_ref()
            .ok_or(SlateDBError::Closed)?
            .send_safely(self.closed_result.reader(), job)
    }

    /// Returns the shared uploader event receiver.
    ///
    /// The caller is expected to drive progress by consuming events until
    /// shutdown. This returns a mutable receiver because Tokio's unbounded
    /// receiver is single-consumer.
    pub(crate) fn events(&mut self) -> &mut UploaderEventReceiver {
        &mut self.events
    }

    /// Closes the uploader.
    ///
    /// On the healthy path this is a graceful drain:
    /// - stop accepting new jobs
    /// - let queued jobs finish
    /// - wait for all worker results to be emitted
    ///
    /// If the uploader is already poisoned, the remaining workers are cancelled
    /// and the fatal error is returned.
    pub(crate) async fn close(&mut self) -> Result<(), SlateDBError> {
        self.jobs.take();
        if self.poisoned.lock().is_some() {
            self.shutdown.cancel();
        }
        let result = if let Some(supervisor) = self.supervisor.take() {
            match supervisor.await {
                Ok(result) => result,
                Err(join_err) if join_err.is_cancelled() => Ok(()),
                Err(join_err) if join_err.is_panic() => Err(SlateDBError::BackgroundTaskPanic(
                    "parallel_l0_flush_uploader".into(),
                )),
                Err(_) => Err(SlateDBError::BackgroundTaskCancelled(
                    "parallel_l0_flush_uploader".into(),
                )),
            }
        } else {
            Ok(())
        };

        self.closed_result.write(result.clone().map(|_| ()));
        result
    }

    async fn run_supervisor(
        mut workers: JoinSet<Result<(), SlateDBError>>,
        poisoned: Arc<Mutex<Option<SlateDBError>>>,
        events: UploaderEventSender,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        shutdown: CancellationToken,
    ) -> Result<(), SlateDBError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    return Ok(());
                }
                maybe_result = workers.join_next() => {
                    let Some(result) = maybe_result else {
                        return Ok(());
                    };
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Self::handle_fatal_worker_error(
                                &mut workers,
                                poisoned,
                                events,
                                closed_result,
                                err,
                            )
                            .await;
                        }
                        Err(join_err) if join_err.is_panic() => {
                            return Self::handle_fatal_worker_error(
                                &mut workers,
                                poisoned,
                                events,
                                closed_result,
                                SlateDBError::BackgroundTaskPanic(
                                "parallel_l0_flush_uploader_worker".into(),
                                ),
                            )
                            .await;
                        }
                        Err(_) => {
                            return Self::handle_fatal_worker_error(
                                &mut workers,
                                poisoned,
                                events,
                                closed_result,
                                SlateDBError::BackgroundTaskCancelled(
                                    "parallel_l0_flush_uploader_worker".into(),
                                ),
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_fatal_worker_error(
        workers: &mut JoinSet<Result<(), SlateDBError>>,
        poisoned: Arc<Mutex<Option<SlateDBError>>>,
        events: UploaderEventSender,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        err: SlateDBError,
    ) -> Result<(), SlateDBError> {
        *poisoned.lock() = Some(err.clone());
        closed_result.write(Err(err.clone()));
        let _ = events.send_safely(closed_result.reader(), UploaderEvent::Fatal(err.clone()));
        workers.abort_all();
        while workers.join_next().await.is_some() {}
        Err(err)
    }
}

struct UploadWorker {
    db: Arc<UploaderDb>,
    shutdown: CancellationToken,
    retry_backoff: Duration,
    closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
}

impl UploadWorker {
    fn new(
        db: Arc<UploaderDb>,
        shutdown: CancellationToken,
        retry_backoff: Duration,
        closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
    ) -> Self {
        Self {
            db,
            shutdown,
            retry_backoff,
            closed_result_reader,
        }
    }

    async fn run(
        self,
        worker_id: usize,
        jobs: UploadJobReceiver,
        events: UploaderEventSender,
    ) -> Result<(), SlateDBError> {
        info!("l0 uploader worker started [worker_id={}]", worker_id);
        let result = self.recv_loop(jobs, events).await;
        match &result {
            Ok(()) => info!("l0 uploader worker stopped [worker_id={}]", worker_id),
            Err(err) => error!(
                "l0 uploader worker failed [worker_id={}, error={:?}]",
                worker_id, err
            ),
        }
        result
    }

    async fn recv_loop(
        &self,
        jobs: UploadJobReceiver,
        events: UploaderEventSender,
    ) -> Result<(), SlateDBError> {
        loop {
            let idle_start = Instant::now();
            tokio::select! {
                _ = self.shutdown.cancelled() => return Ok(()),
                recv_result = async {
                    let mut jobs = jobs.lock().await;
                    jobs.recv().await
                } => {
                    self.db
                        .db_stats
                        .l0_upload_idle_millis
                        .add(idle_start.elapsed().as_millis() as u64);
                    let Some(job) = recv_result else {
                        return Ok(());
                    };

                    let busy_start = Instant::now();
                    let success = self.upload_with_retry(job).await?;
                    self.db
                        .db_stats
                        .l0_upload_busy_millis
                        .add(busy_start.elapsed().as_millis() as u64);
                    #[allow(clippy::disallowed_methods)]
                    if events.send(UploaderEvent::Uploaded(Box::new(success))).is_err() {
                        // The flusher is no longer listening for uploader events, so
                        // treat this as normal shutdown rather than panicking.
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn build_sst(&self, job: &UploadJob) -> Result<EncodedSsTable, SlateDBError> {
        self.db.build_imm_sst(job.imm_memtable.table()).await
    }

    async fn upload_with_retry(&self, job: UploadJob) -> Result<UploadSuccess, SlateDBError> {
        loop {
            let encoded_sst = self.build_sst(&job).await?;
            tokio::select! {
                _ = self.shutdown.cancelled() => return Err(SlateDBError::Closed),
                result = self.try_upload_once(&job, encoded_sst) => match result {
                    Ok(success) => return Ok(success),
                    Err(_) => {
                        tokio::select! {
                            _ = self.shutdown.cancelled() => return Err(SlateDBError::Closed),
                            _ = self.db.system_clock.sleep(self.retry_backoff) => {}
                        }
                    }
                }
            }
        }
    }

    async fn try_upload_once(
        &self,
        job: &UploadJob,
        encoded_sst: EncodedSsTable,
    ) -> Result<UploadSuccess, SlateDBError> {
        // TODO: consider changing the low-level upload path so failed uploads
        // return ownership of the built SST. That would let the worker build
        // once and retry uploads without rebuilding.
        let last_seq = job
            .imm_memtable
            .table()
            .last_seq()
            .expect("flush of l0 with no entries");
        let written_bytes = encoded_sst.remaining_len() as u64;
        let start = self.db.system_clock.now();
        let sst_handle = self
            .db
            .upload_compacted_sst(&job.sst_id, job.imm_memtable.table(), encoded_sst, true)
            .await?;
        self.db.db_stats.l0_flush_bytes.add(written_bytes);
        let elapsed = self
            .db
            .system_clock
            .now()
            .signed_duration_since(start)
            .to_std()
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0);
        let throughput = if elapsed > 0.0 {
            (written_bytes as f64 / elapsed) as u64
        } else {
            written_bytes
        };
        self.db.db_stats.l0_flush_throughput.set(throughput);
        Ok(UploadSuccess {
            epoch: job.epoch,
            imm_memtable: Arc::clone(&job.imm_memtable),
            sst_id: job.sst_id,
            sst_handle,
            last_seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FlushEpoch, UploadJob, Uploader, UploaderDb, UploaderEvent};
    use crate::config::Settings;
    use crate::db::DbInner;
    use crate::db_state::{ManifestCore, SsTableId};
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::stats::{ReadableStat, StatRegistry};
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::IdGenerator;
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
        db: Arc<UploaderDb>,
    }

    async fn setup_harness(path: &str, fp_registry: Arc<FailPointRegistry>) -> TestHarness {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let settings = Settings::default();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::new(42));
        let stat_registry = Arc::new(StatRegistry::new());
        let manifest_store = Arc::new(crate::manifest::store::ManifestStore::new(
            &Path::from(path),
            Arc::clone(&object_store),
        ));
        let stored_manifest = crate::manifest::store::StoredManifest::create_new_db(
            manifest_store,
            ManifestCore::new_with_wal_object_store(None),
            Arc::clone(&system_clock),
        )
        .await
        .unwrap();
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            PathResolver::new(Path::from(path)),
            fp_registry.clone(),
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
        let db = UploaderDb::from_db_inner(&inner);
        TestHarness { inner, db }
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

    fn next_upload_job(
        harness: &TestHarness,
        epoch: u64,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> UploadJob {
        let imm_memtable = freeze_imm(harness, key, value, seq);
        let sst_id = SsTableId::Compacted(
            harness
                .db
                .rand
                .rng()
                .gen_ulid(harness.db.system_clock.as_ref()),
        );
        UploadJob::new(FlushEpoch(epoch), imm_memtable, sst_id)
    }

    #[tokio::test]
    async fn should_emit_uploaded_event_for_successful_job() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_uploader_success",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let job = next_upload_job(&harness, 1, b"key", b"value", 1);

        let mut uploader = Uploader::start(
            Arc::clone(&harness.db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
        );
        uploader.submit(job).await.unwrap();

        let event = timeout(Duration::from_secs(5), uploader.events().recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            UploaderEvent::Uploaded(success) => {
                assert_eq!(success.epoch, FlushEpoch(1));
                assert_eq!(success.last_seq, 1);
            }
            UploaderEvent::Fatal(err) => panic!("unexpected fatal uploader event: {err:?}"),
        }
        assert!(harness.inner.db_stats.l0_flush_bytes.get() > 0);
        assert!(harness.inner.db_stats.l0_flush_throughput.get() >= 0);

        uploader.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_retry_upload_failures_until_success() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "1*off->return",
        )
        .unwrap();
        let harness =
            setup_harness("/tmp/test_parallel_l0_flush_uploader_retry", fp_registry).await;
        let job = next_upload_job(&harness, 7, b"key", b"value", 1);

        let mut uploader = Uploader::start(
            Arc::clone(&harness.db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
        );
        uploader.submit(job).await.unwrap();

        let event = timeout(Duration::from_secs(5), uploader.events().recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            UploaderEvent::Uploaded(success) => {
                assert_eq!(success.epoch, FlushEpoch(7));
            }
            UploaderEvent::Fatal(err) => panic!("unexpected fatal uploader event: {err:?}"),
        }

        uploader.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_emit_fatal_event_for_build_failure() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_uploader_build_failure",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        {
            let mut guard = harness.inner.state.write();
            guard.memtable().put(crate::types::RowEntry::new_merge(
                b"key",
                b"merge_operand",
                1,
            ));
            guard.freeze_memtable(0).unwrap();
        }
        let imm_memtable = harness
            .inner
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let sst_id = SsTableId::Compacted(
            harness
                .db
                .rand
                .rng()
                .gen_ulid(harness.db.system_clock.as_ref()),
        );
        let job = UploadJob::new(FlushEpoch(3), imm_memtable, sst_id);

        let mut uploader = Uploader::start(
            Arc::clone(&harness.db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
        );
        uploader.submit(job).await.unwrap();

        let event = timeout(Duration::from_secs(5), uploader.events().recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, UploaderEvent::Fatal(_)));

        let err = uploader
            .submit(next_upload_job(&harness, 4, b"key2", b"value2", 2))
            .await
            .unwrap_err();
        assert!(!matches!(err, SlateDBError::Closed));

        let close_result = uploader.close().await;
        assert!(close_result.is_err());
    }

    #[tokio::test]
    async fn close_should_drain_queued_jobs() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_uploader_close_drains",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let job1 = next_upload_job(&harness, 1, b"key1", b"value1", 1);
        let job2 = next_upload_job(&harness, 2, b"key2", b"value2", 2);

        let mut uploader = Uploader::start(
            Arc::clone(&harness.db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
        );
        uploader.submit(job1).await.unwrap();
        uploader.submit(job2).await.unwrap();

        uploader.close().await.unwrap();

        let mut uploaded_epochs = Vec::new();
        while let Some(event) = uploader.events().recv().await {
            match event {
                UploaderEvent::Uploaded(success) => uploaded_epochs.push(success.epoch),
                UploaderEvent::Fatal(err) => panic!("unexpected fatal uploader event: {err:?}"),
            }
        }

        assert_eq!(uploaded_epochs, vec![FlushEpoch(1), FlushEpoch(2)]);
    }
}
