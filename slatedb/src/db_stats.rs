use crate::stats::{Counter, Gauge, StatRegistry};
use std::sync::Arc;

macro_rules! db_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("db", $suffix)
    };
}

pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");
pub const WAL_FLUSH_TOTAL_MS: &str = db_stat_name!("wal_flush_total_ms");
pub const WAL_FLUSH_TOTAL_MS_LAST: &str = db_stat_name!("wal_flush_total_ms_last");
pub const WAL_FLUSH_ROW_LOOP_MS: &str = db_stat_name!("wal_flush_row_loop_ms");
pub const WAL_FLUSH_ROW_LOOP_MS_LAST: &str = db_stat_name!("wal_flush_row_loop_ms_last");
pub const WAL_FLUSH_BUILD_MS: &str = db_stat_name!("wal_flush_build_ms");
pub const WAL_FLUSH_BUILD_MS_LAST: &str = db_stat_name!("wal_flush_build_ms_last");
pub const WAL_FLUSH_PUT_MS: &str = db_stat_name!("wal_flush_put_ms");
pub const WAL_FLUSH_PUT_MS_LAST: &str = db_stat_name!("wal_flush_put_ms_last");
pub const WAL_FLUSH_CACHE_MS: &str = db_stat_name!("wal_flush_cache_ms");
pub const WAL_FLUSH_CACHE_MS_LAST: &str = db_stat_name!("wal_flush_cache_ms_last");
pub const WAL_FLUSH_INPUT_ROWS_LAST: &str = db_stat_name!("wal_flush_input_rows_last");
pub const WAL_FLUSH_INPUT_BYTES: &str = db_stat_name!("wal_flush_input_bytes");
pub const WAL_FLUSH_INPUT_BYTES_LAST: &str = db_stat_name!("wal_flush_input_bytes_last");
pub const WAL_FLUSH_OUTPUT_BYTES: &str = db_stat_name!("wal_flush_output_bytes");
pub const WAL_FLUSH_OUTPUT_BYTES_LAST: &str = db_stat_name!("wal_flush_output_bytes_last");
pub const SST_FILTER_FALSE_POSITIVES: &str = db_stat_name!("sst_filter_false_positives");
pub const SST_FILTER_POSITIVES: &str = db_stat_name!("sst_filter_positives");
pub const SST_FILTER_NEGATIVES: &str = db_stat_name!("sst_filter_negatives");
pub const BACKPRESSURE_COUNT: &str = db_stat_name!("backpressure_count");
pub const WAL_BUFFER_ESTIMATED_BYTES: &str = db_stat_name!("wal_buffer_estimated_bytes");
pub const WAL_BUFFER_FLUSHES: &str = db_stat_name!("wal_buffer_flushes");
pub const GET_REQUESTS: &str = db_stat_name!("get_requests");
pub const SCAN_REQUESTS: &str = db_stat_name!("scan_requests");
pub const FLUSH_REQUESTS: &str = db_stat_name!("flush_requests");
pub const WRITE_BATCH_COUNT: &str = db_stat_name!("write_batch_count");
pub const WRITE_OPS: &str = db_stat_name!("write_ops");
pub const TOTAL_MEM_SIZE_BYTES: &str = db_stat_name!("total_mem_size_bytes");
pub const L0_SST_COUNT: &str = db_stat_name!("l0_sst_count");
pub const ACTIVE_MEMTABLE_BYTES: &str = db_stat_name!("active_memtable_bytes");
pub const IMM_MEMTABLE_COUNT: &str = db_stat_name!("imm_memtable_count");
pub const IMM_MEMTABLE_BYTES: &str = db_stat_name!("imm_memtable_bytes");
pub const L0_FLUSH_TOTAL_MS: &str = db_stat_name!("l0_flush_total_ms");
pub const L0_FLUSH_TOTAL_MS_LAST: &str = db_stat_name!("l0_flush_total_ms_last");
pub const L0_FLUSH_WAL_WAIT_MS: &str = db_stat_name!("l0_flush_wal_wait_ms");
pub const L0_FLUSH_WAL_WAIT_MS_LAST: &str = db_stat_name!("l0_flush_wal_wait_ms_last");
pub const L0_FLUSH_ENCODE_MS: &str = db_stat_name!("l0_flush_encode_ms");
pub const L0_FLUSH_ENCODE_MS_LAST: &str = db_stat_name!("l0_flush_encode_ms_last");
pub const L0_FLUSH_ITER_SETUP_MS: &str = db_stat_name!("l0_flush_iter_setup_ms");
pub const L0_FLUSH_ITER_SETUP_MS_LAST: &str = db_stat_name!("l0_flush_iter_setup_ms_last");
pub const L0_FLUSH_ROW_LOOP_MS: &str = db_stat_name!("l0_flush_row_loop_ms");
pub const L0_FLUSH_ROW_LOOP_MS_LAST: &str = db_stat_name!("l0_flush_row_loop_ms_last");
pub const L0_FLUSH_FINISH_BLOCK_MS: &str = db_stat_name!("l0_flush_finish_block_ms");
pub const L0_FLUSH_FINISH_BLOCK_MS_LAST: &str = db_stat_name!("l0_flush_finish_block_ms_last");
pub const L0_FLUSH_FOOTER_MS: &str = db_stat_name!("l0_flush_footer_ms");
pub const L0_FLUSH_FOOTER_MS_LAST: &str = db_stat_name!("l0_flush_footer_ms_last");
pub const L0_FLUSH_WRITE_MS: &str = db_stat_name!("l0_flush_write_ms");
pub const L0_FLUSH_WRITE_MS_LAST: &str = db_stat_name!("l0_flush_write_ms_last");
pub const L0_FLUSH_PUT_MS: &str = db_stat_name!("l0_flush_put_ms");
pub const L0_FLUSH_PUT_MS_LAST: &str = db_stat_name!("l0_flush_put_ms_last");
pub const L0_FLUSH_CACHE_MS: &str = db_stat_name!("l0_flush_cache_ms");
pub const L0_FLUSH_CACHE_MS_LAST: &str = db_stat_name!("l0_flush_cache_ms_last");
pub const L0_FLUSH_PUBLISH_MS: &str = db_stat_name!("l0_flush_publish_ms");
pub const L0_FLUSH_PUBLISH_MS_LAST: &str = db_stat_name!("l0_flush_publish_ms_last");
pub const L0_FLUSH_MANIFEST_MS: &str = db_stat_name!("l0_flush_manifest_ms");
pub const L0_FLUSH_MANIFEST_MS_LAST: &str = db_stat_name!("l0_flush_manifest_ms_last");
pub const L0_FLUSH_INPUT_ROWS_LAST: &str = db_stat_name!("l0_flush_input_rows_last");
pub const L0_FLUSH_INPUT_BYTES_LAST: &str = db_stat_name!("l0_flush_input_bytes_last");
pub const L0_FLUSH_INPUT_BYTES: &str = db_stat_name!("l0_flush_input_bytes");
pub const L0_FLUSH_OUTPUT_BYTES_LAST: &str = db_stat_name!("l0_flush_output_bytes_last");
pub const L0_FLUSH_OUTPUT_BYTES: &str = db_stat_name!("l0_flush_output_bytes");
pub const L0_FLUSH_MANIFEST_RETRIES: &str = db_stat_name!("l0_flush_manifest_retries");
pub const L0_FLUSH_COMMIT_BATCH_SIZE_LAST: &str = db_stat_name!("l0_flush_commit_batch_size_last");
pub const L0_FLUSH_BUILD_INFLIGHT: &str = db_stat_name!("l0_flush_build_inflight");
pub const L0_FLUSH_UPLOAD_INFLIGHT: &str = db_stat_name!("l0_flush_upload_inflight");
pub const L0_FLUSH_BUILT_READY: &str = db_stat_name!("l0_flush_built_ready");
pub const L0_FLUSH_UPLOADED_READY: &str = db_stat_name!("l0_flush_uploaded_ready");

#[non_exhaustive]
#[derive(Clone, Debug)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<Counter>,
    pub(crate) wal_flush_total_ms: Arc<Counter>,
    pub(crate) wal_flush_total_ms_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_row_loop_ms: Arc<Counter>,
    pub(crate) wal_flush_row_loop_ms_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_build_ms: Arc<Counter>,
    pub(crate) wal_flush_build_ms_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_put_ms: Arc<Counter>,
    pub(crate) wal_flush_put_ms_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_cache_ms: Arc<Counter>,
    pub(crate) wal_flush_cache_ms_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_input_rows_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_input_bytes: Arc<Counter>,
    pub(crate) wal_flush_input_bytes_last: Arc<Gauge<u64>>,
    pub(crate) wal_flush_output_bytes: Arc<Counter>,
    pub(crate) wal_flush_output_bytes_last: Arc<Gauge<u64>>,
    pub(crate) wal_buffer_estimated_bytes: Arc<Gauge<i64>>,
    pub(crate) wal_buffer_flushes: Arc<Counter>,
    pub(crate) sst_filter_false_positives: Arc<Counter>,
    pub(crate) sst_filter_positives: Arc<Counter>,
    pub(crate) sst_filter_negatives: Arc<Counter>,
    pub(crate) backpressure_count: Arc<Counter>,
    pub(crate) get_requests: Arc<Counter>,
    pub(crate) scan_requests: Arc<Counter>,
    pub(crate) flush_requests: Arc<Counter>,
    pub(crate) write_batch_count: Arc<Counter>,
    pub(crate) write_ops: Arc<Counter>,
    pub(crate) total_mem_size_bytes: Arc<Gauge<i64>>,
    pub(crate) l0_sst_count: Arc<Gauge<i64>>,
    pub(crate) active_memtable_bytes: Arc<Gauge<i64>>,
    pub(crate) imm_memtable_count: Arc<Gauge<i64>>,
    pub(crate) imm_memtable_bytes: Arc<Gauge<i64>>,
    pub(crate) l0_flush_total_ms: Arc<Counter>,
    pub(crate) l0_flush_total_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_wal_wait_ms: Arc<Counter>,
    pub(crate) l0_flush_wal_wait_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_encode_ms: Arc<Counter>,
    pub(crate) l0_flush_encode_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_iter_setup_ms: Arc<Counter>,
    pub(crate) l0_flush_iter_setup_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_row_loop_ms: Arc<Counter>,
    pub(crate) l0_flush_row_loop_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_finish_block_ms: Arc<Counter>,
    pub(crate) l0_flush_finish_block_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_footer_ms: Arc<Counter>,
    pub(crate) l0_flush_footer_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_write_ms: Arc<Counter>,
    pub(crate) l0_flush_write_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_put_ms: Arc<Counter>,
    pub(crate) l0_flush_put_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_cache_ms: Arc<Counter>,
    pub(crate) l0_flush_cache_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_publish_ms: Arc<Counter>,
    pub(crate) l0_flush_publish_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_manifest_ms: Arc<Counter>,
    pub(crate) l0_flush_manifest_ms_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_input_rows_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_input_bytes: Arc<Counter>,
    pub(crate) l0_flush_input_bytes_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_output_bytes: Arc<Counter>,
    pub(crate) l0_flush_output_bytes_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_manifest_retries: Arc<Counter>,
    pub(crate) l0_flush_commit_batch_size_last: Arc<Gauge<u64>>,
    pub(crate) l0_flush_build_inflight: Arc<Gauge<i64>>,
    pub(crate) l0_flush_upload_inflight: Arc<Gauge<i64>>,
    pub(crate) l0_flush_built_ready: Arc<Gauge<i64>>,
    pub(crate) l0_flush_uploaded_ready: Arc<Gauge<i64>>,
}

impl DbStats {
    pub(crate) fn new(registry: &StatRegistry) -> DbStats {
        let stats = Self {
            immutable_memtable_flushes: Arc::new(Counter::default()),
            wal_flush_total_ms: Arc::new(Counter::default()),
            wal_flush_total_ms_last: Arc::new(Gauge::default()),
            wal_flush_row_loop_ms: Arc::new(Counter::default()),
            wal_flush_row_loop_ms_last: Arc::new(Gauge::default()),
            wal_flush_build_ms: Arc::new(Counter::default()),
            wal_flush_build_ms_last: Arc::new(Gauge::default()),
            wal_flush_put_ms: Arc::new(Counter::default()),
            wal_flush_put_ms_last: Arc::new(Gauge::default()),
            wal_flush_cache_ms: Arc::new(Counter::default()),
            wal_flush_cache_ms_last: Arc::new(Gauge::default()),
            wal_flush_input_rows_last: Arc::new(Gauge::default()),
            wal_flush_input_bytes: Arc::new(Counter::default()),
            wal_flush_input_bytes_last: Arc::new(Gauge::default()),
            wal_flush_output_bytes: Arc::new(Counter::default()),
            wal_flush_output_bytes_last: Arc::new(Gauge::default()),
            wal_buffer_estimated_bytes: Arc::new(Gauge::default()),
            wal_buffer_flushes: Arc::new(Counter::default()),
            sst_filter_false_positives: Arc::new(Counter::default()),
            sst_filter_positives: Arc::new(Counter::default()),
            sst_filter_negatives: Arc::new(Counter::default()),
            backpressure_count: Arc::new(Counter::default()),
            get_requests: Arc::new(Counter::default()),
            scan_requests: Arc::new(Counter::default()),
            flush_requests: Arc::new(Counter::default()),
            write_batch_count: Arc::new(Counter::default()),
            write_ops: Arc::new(Counter::default()),
            total_mem_size_bytes: Arc::new(Gauge::default()),
            l0_sst_count: Arc::new(Gauge::default()),
            active_memtable_bytes: Arc::new(Gauge::default()),
            imm_memtable_count: Arc::new(Gauge::default()),
            imm_memtable_bytes: Arc::new(Gauge::default()),
            l0_flush_total_ms: Arc::new(Counter::default()),
            l0_flush_total_ms_last: Arc::new(Gauge::default()),
            l0_flush_wal_wait_ms: Arc::new(Counter::default()),
            l0_flush_wal_wait_ms_last: Arc::new(Gauge::default()),
            l0_flush_encode_ms: Arc::new(Counter::default()),
            l0_flush_encode_ms_last: Arc::new(Gauge::default()),
            l0_flush_iter_setup_ms: Arc::new(Counter::default()),
            l0_flush_iter_setup_ms_last: Arc::new(Gauge::default()),
            l0_flush_row_loop_ms: Arc::new(Counter::default()),
            l0_flush_row_loop_ms_last: Arc::new(Gauge::default()),
            l0_flush_finish_block_ms: Arc::new(Counter::default()),
            l0_flush_finish_block_ms_last: Arc::new(Gauge::default()),
            l0_flush_footer_ms: Arc::new(Counter::default()),
            l0_flush_footer_ms_last: Arc::new(Gauge::default()),
            l0_flush_write_ms: Arc::new(Counter::default()),
            l0_flush_write_ms_last: Arc::new(Gauge::default()),
            l0_flush_put_ms: Arc::new(Counter::default()),
            l0_flush_put_ms_last: Arc::new(Gauge::default()),
            l0_flush_cache_ms: Arc::new(Counter::default()),
            l0_flush_cache_ms_last: Arc::new(Gauge::default()),
            l0_flush_publish_ms: Arc::new(Counter::default()),
            l0_flush_publish_ms_last: Arc::new(Gauge::default()),
            l0_flush_manifest_ms: Arc::new(Counter::default()),
            l0_flush_manifest_ms_last: Arc::new(Gauge::default()),
            l0_flush_input_rows_last: Arc::new(Gauge::default()),
            l0_flush_input_bytes: Arc::new(Counter::default()),
            l0_flush_input_bytes_last: Arc::new(Gauge::default()),
            l0_flush_output_bytes: Arc::new(Counter::default()),
            l0_flush_output_bytes_last: Arc::new(Gauge::default()),
            l0_flush_manifest_retries: Arc::new(Counter::default()),
            l0_flush_commit_batch_size_last: Arc::new(Gauge::default()),
            l0_flush_build_inflight: Arc::new(Gauge::default()),
            l0_flush_upload_inflight: Arc::new(Gauge::default()),
            l0_flush_built_ready: Arc::new(Gauge::default()),
            l0_flush_uploaded_ready: Arc::new(Gauge::default()),
        };
        registry.register(
            IMMUTABLE_MEMTABLE_FLUSHES,
            stats.immutable_memtable_flushes.clone(),
        );
        registry.register(WAL_FLUSH_TOTAL_MS, stats.wal_flush_total_ms.clone());
        registry.register(
            WAL_FLUSH_TOTAL_MS_LAST,
            stats.wal_flush_total_ms_last.clone(),
        );
        registry.register(WAL_FLUSH_ROW_LOOP_MS, stats.wal_flush_row_loop_ms.clone());
        registry.register(
            WAL_FLUSH_ROW_LOOP_MS_LAST,
            stats.wal_flush_row_loop_ms_last.clone(),
        );
        registry.register(WAL_FLUSH_BUILD_MS, stats.wal_flush_build_ms.clone());
        registry.register(
            WAL_FLUSH_BUILD_MS_LAST,
            stats.wal_flush_build_ms_last.clone(),
        );
        registry.register(WAL_FLUSH_PUT_MS, stats.wal_flush_put_ms.clone());
        registry.register(WAL_FLUSH_PUT_MS_LAST, stats.wal_flush_put_ms_last.clone());
        registry.register(WAL_FLUSH_CACHE_MS, stats.wal_flush_cache_ms.clone());
        registry.register(
            WAL_FLUSH_CACHE_MS_LAST,
            stats.wal_flush_cache_ms_last.clone(),
        );
        registry.register(
            WAL_FLUSH_INPUT_ROWS_LAST,
            stats.wal_flush_input_rows_last.clone(),
        );
        registry.register(WAL_FLUSH_INPUT_BYTES, stats.wal_flush_input_bytes.clone());
        registry.register(
            WAL_FLUSH_INPUT_BYTES_LAST,
            stats.wal_flush_input_bytes_last.clone(),
        );
        registry.register(WAL_FLUSH_OUTPUT_BYTES, stats.wal_flush_output_bytes.clone());
        registry.register(
            WAL_FLUSH_OUTPUT_BYTES_LAST,
            stats.wal_flush_output_bytes_last.clone(),
        );
        registry.register(
            WAL_BUFFER_ESTIMATED_BYTES,
            stats.wal_buffer_estimated_bytes.clone(),
        );
        registry.register(WAL_BUFFER_FLUSHES, stats.wal_buffer_flushes.clone());
        registry.register(
            SST_FILTER_FALSE_POSITIVES,
            stats.sst_filter_false_positives.clone(),
        );
        registry.register(SST_FILTER_POSITIVES, stats.sst_filter_positives.clone());
        registry.register(SST_FILTER_NEGATIVES, stats.sst_filter_negatives.clone());
        registry.register(BACKPRESSURE_COUNT, stats.backpressure_count.clone());
        registry.register(GET_REQUESTS, stats.get_requests.clone());
        registry.register(SCAN_REQUESTS, stats.scan_requests.clone());
        registry.register(FLUSH_REQUESTS, stats.flush_requests.clone());
        registry.register(WRITE_BATCH_COUNT, stats.write_batch_count.clone());
        registry.register(WRITE_OPS, stats.write_ops.clone());
        registry.register(TOTAL_MEM_SIZE_BYTES, stats.total_mem_size_bytes.clone());
        registry.register(L0_SST_COUNT, stats.l0_sst_count.clone());
        registry.register(ACTIVE_MEMTABLE_BYTES, stats.active_memtable_bytes.clone());
        registry.register(IMM_MEMTABLE_COUNT, stats.imm_memtable_count.clone());
        registry.register(IMM_MEMTABLE_BYTES, stats.imm_memtable_bytes.clone());
        registry.register(L0_FLUSH_TOTAL_MS, stats.l0_flush_total_ms.clone());
        registry.register(L0_FLUSH_TOTAL_MS_LAST, stats.l0_flush_total_ms_last.clone());
        registry.register(L0_FLUSH_WAL_WAIT_MS, stats.l0_flush_wal_wait_ms.clone());
        registry.register(
            L0_FLUSH_WAL_WAIT_MS_LAST,
            stats.l0_flush_wal_wait_ms_last.clone(),
        );
        registry.register(L0_FLUSH_ENCODE_MS, stats.l0_flush_encode_ms.clone());
        registry.register(
            L0_FLUSH_ENCODE_MS_LAST,
            stats.l0_flush_encode_ms_last.clone(),
        );
        registry.register(L0_FLUSH_ITER_SETUP_MS, stats.l0_flush_iter_setup_ms.clone());
        registry.register(
            L0_FLUSH_ITER_SETUP_MS_LAST,
            stats.l0_flush_iter_setup_ms_last.clone(),
        );
        registry.register(L0_FLUSH_ROW_LOOP_MS, stats.l0_flush_row_loop_ms.clone());
        registry.register(
            L0_FLUSH_ROW_LOOP_MS_LAST,
            stats.l0_flush_row_loop_ms_last.clone(),
        );
        registry.register(
            L0_FLUSH_FINISH_BLOCK_MS,
            stats.l0_flush_finish_block_ms.clone(),
        );
        registry.register(
            L0_FLUSH_FINISH_BLOCK_MS_LAST,
            stats.l0_flush_finish_block_ms_last.clone(),
        );
        registry.register(L0_FLUSH_FOOTER_MS, stats.l0_flush_footer_ms.clone());
        registry.register(
            L0_FLUSH_FOOTER_MS_LAST,
            stats.l0_flush_footer_ms_last.clone(),
        );
        registry.register(L0_FLUSH_WRITE_MS, stats.l0_flush_write_ms.clone());
        registry.register(L0_FLUSH_WRITE_MS_LAST, stats.l0_flush_write_ms_last.clone());
        registry.register(L0_FLUSH_PUT_MS, stats.l0_flush_put_ms.clone());
        registry.register(L0_FLUSH_PUT_MS_LAST, stats.l0_flush_put_ms_last.clone());
        registry.register(L0_FLUSH_CACHE_MS, stats.l0_flush_cache_ms.clone());
        registry.register(L0_FLUSH_CACHE_MS_LAST, stats.l0_flush_cache_ms_last.clone());
        registry.register(L0_FLUSH_PUBLISH_MS, stats.l0_flush_publish_ms.clone());
        registry.register(
            L0_FLUSH_PUBLISH_MS_LAST,
            stats.l0_flush_publish_ms_last.clone(),
        );
        registry.register(L0_FLUSH_MANIFEST_MS, stats.l0_flush_manifest_ms.clone());
        registry.register(
            L0_FLUSH_MANIFEST_MS_LAST,
            stats.l0_flush_manifest_ms_last.clone(),
        );
        registry.register(
            L0_FLUSH_INPUT_ROWS_LAST,
            stats.l0_flush_input_rows_last.clone(),
        );
        registry.register(L0_FLUSH_INPUT_BYTES, stats.l0_flush_input_bytes.clone());
        registry.register(
            L0_FLUSH_INPUT_BYTES_LAST,
            stats.l0_flush_input_bytes_last.clone(),
        );
        registry.register(L0_FLUSH_OUTPUT_BYTES, stats.l0_flush_output_bytes.clone());
        registry.register(
            L0_FLUSH_OUTPUT_BYTES_LAST,
            stats.l0_flush_output_bytes_last.clone(),
        );
        registry.register(
            L0_FLUSH_MANIFEST_RETRIES,
            stats.l0_flush_manifest_retries.clone(),
        );
        registry.register(
            L0_FLUSH_COMMIT_BATCH_SIZE_LAST,
            stats.l0_flush_commit_batch_size_last.clone(),
        );
        registry.register(
            L0_FLUSH_BUILD_INFLIGHT,
            stats.l0_flush_build_inflight.clone(),
        );
        registry.register(
            L0_FLUSH_UPLOAD_INFLIGHT,
            stats.l0_flush_upload_inflight.clone(),
        );
        registry.register(L0_FLUSH_BUILT_READY, stats.l0_flush_built_ready.clone());
        registry.register(
            L0_FLUSH_UPLOADED_READY,
            stats.l0_flush_uploaded_ready.clone(),
        );
        stats
    }
}
