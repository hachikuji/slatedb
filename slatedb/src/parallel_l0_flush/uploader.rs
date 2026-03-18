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
use crate::db_state::{SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::format::sst::EncodedSsTable;
use crate::mem_table::ImmutableMemtable;
use crate::utils::{SendSafely, WatchableOnceCell, WatchableOnceCellReader};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

/// One immutable-memtable upload request submitted to the uploader.
pub(crate) struct UploadJob {
    /// Ordering token assigned by the orchestrator.
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
        db_inner: Arc<DbInner>,
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
                Arc::clone(&db_inner),
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
    db_inner: Arc<DbInner>,
    shutdown: CancellationToken,
    retry_backoff: Duration,
    closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
}

impl UploadWorker {
    fn new(
        db_inner: Arc<DbInner>,
        shutdown: CancellationToken,
        retry_backoff: Duration,
        closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
    ) -> Self {
        Self {
            db_inner,
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
        let _ = worker_id;
        self.recv_loop(jobs, events).await
    }

    async fn recv_loop(
        &self,
        jobs: UploadJobReceiver,
        events: UploaderEventSender,
    ) -> Result<(), SlateDBError> {
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return Ok(()),
                recv_result = async {
                    let mut jobs = jobs.lock().await;
                    jobs.recv().await
                } => {
                    let Some(job) = recv_result else {
                        return Ok(());
                    };

                    let success = self.upload_with_retry(job).await?;
                    if events
                        .send_safely(
                            self.closed_result_reader.clone(),
                            UploaderEvent::Uploaded(Box::new(success)),
                        )
                        .is_err()
                    {
                        return Err(SlateDBError::Closed);
                    }
                }
            }
        }
    }

    async fn build_sst(&self, job: &UploadJob) -> Result<EncodedSsTable, SlateDBError> {
        self.db_inner.build_imm_sst(job.imm_memtable.table()).await
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
                            _ = self.db_inner.system_clock.sleep(self.retry_backoff) => {}
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
        let sst_handle = self
            .db_inner
            .upload_compacted_sst(&job.sst_id, job.imm_memtable.table(), encoded_sst, true)
            .await?;

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
    use super::{FlushEpoch, UploadJob, Uploader, UploaderEvent};
    use crate::config::Settings;
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::error::SlateDBError;
    use crate::utils::IdGenerator;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::time::timeout;

    async fn setup_db(path: &str, fp_registry: Arc<FailPointRegistry>) -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Db::builder(path, object_store)
            .with_settings(Settings::default())
            .with_fp_registry(fp_registry)
            .build()
            .await
            .unwrap()
    }

    fn freeze_imm(db: &Db) -> Arc<crate::mem_table::ImmutableMemtable> {
        let recent_flushed_wal_id = db.inner.wal_buffer.recent_flushed_wal_id();
        let mut guard = db.inner.state.write();
        guard.freeze_memtable(recent_flushed_wal_id).unwrap();
        guard.state().imm_memtable.back().cloned().unwrap()
    }

    fn next_upload_job(db: &Db, epoch: u64) -> UploadJob {
        let imm_memtable = freeze_imm(db);
        let sst_id =
            SsTableId::Compacted(db.inner.rand.rng().gen_ulid(db.inner.system_clock.as_ref()));
        UploadJob::new(FlushEpoch(epoch), imm_memtable, sst_id)
    }

    #[tokio::test]
    async fn should_emit_uploaded_event_for_successful_job() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_success",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        db.put(b"key", b"value").await.unwrap();
        let job = next_upload_job(&db, 1);

        let mut uploader = Uploader::start(
            Arc::clone(&db.inner),
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

        uploader.close().await.unwrap();
        db.close().await.unwrap();
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
        let db = setup_db("/tmp/test_parallel_l0_flush_uploader_retry", fp_registry).await;
        db.put(b"key", b"value").await.unwrap();
        let job = next_upload_job(&db, 7);

        let mut uploader = Uploader::start(
            Arc::clone(&db.inner),
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
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_emit_fatal_event_for_build_failure() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_build_failure",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        db.merge(b"key", b"merge_operand").await.unwrap();
        let job = next_upload_job(&db, 3);

        let mut uploader = Uploader::start(
            Arc::clone(&db.inner),
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

        let err = uploader.submit(next_upload_job(&db, 4)).await.unwrap_err();
        assert!(!matches!(err, SlateDBError::Closed));

        let close_result = uploader.close().await;
        assert!(close_result.is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_should_drain_queued_jobs() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_close_drains",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        db.put(b"key1", b"value1").await.unwrap();
        let job1 = next_upload_job(&db, 1);
        db.put(b"key2", b"value2").await.unwrap();
        let job2 = next_upload_job(&db, 2);

        let mut uploader = Uploader::start(
            Arc::clone(&db.inner),
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
        db.close().await.unwrap();
    }
}
