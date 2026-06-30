//! # TSDB segmentation benchmark (RFC-0024)
//!
//! This benchmark demonstrates the effect of segment-oriented compaction
//! ([RFC-0024](../../rfcs/0024-segment-oriented-compaction.md)) on read and
//! write stability for an append-oriented, time-series-shaped workload.
//!
//! ## Workload, in one sentence
//!
//! A single writer continuously appends samples into an advancing time bucket,
//! readers issue Zipf-weighted fixed-width range scans over the most recent
//! sealed buckets, and we compare scan tail-latency and compaction throughput
//! between a segmented and an unsegmented database as the database grows.
//!
//! ## Key encoding
//!
//! `<bucket: u64 BE><metric: u32 BE><ts: u64 BE>` (20 bytes). Big-endian so
//! lexicographic order equals numeric order: the 8-byte bucket is a clean
//! segment prefix (see [`BucketPrefixExtractor`]) and `<bucket><metric>` scans
//! are contiguous.
//!
//! ## Roles
//!
//! - **Writer** ([`run_writer`]): pure append. A shared atomic slot counter
//!   ([`Frontier`]) advances the bucket automatically once `bucket_bytes` of
//!   data have been written, and lets readers derive the current frontier.
//! - **Reader** ([`run_reader`]): each scan picks a metric via Zipf (so the
//!   hot read set is the small head of the distribution and stays cache
//!   resident) and a bucket uniformly from the trailing window of *sealed*
//!   buckets `[A-W, A-1]`. Each scan covers one bucket for one metric — a fixed
//!   range of time that routes to exactly one segment in the segmented arm.
//! - **Metrics dump** ([`dump_metrics`]): snapshots SlateDB's internal metric
//!   registry every window, diffs cumulative counters, and writes a CSV row
//!   keyed by bytes-written (the x-axis is DB size).
//!
//! The only difference between the two arms is whether the database is built
//! with [`BucketPrefixExtractor`] as its segment extractor; everything else
//! (config, cache, workload) is identical.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use hdrhistogram::Histogram;
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::{Distribution, Zipf};
use rand_xorshift::XorShiftRng;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::prefix_extractor::{PrefixExtractor, PrefixTarget};
use slatedb::Db;
use slatedb_common::metrics::{DefaultMetricsRecorder, Metric, MetricValue, Metrics};
use tokio::time::Instant;
use tracing::{info, warn};

/// The 8-byte big-endian time bucket that prefixes every key is the segment
/// prefix.
pub const BUCKET_PREFIX_LEN: usize = 8;

/// How often the metrics dump task snapshots the registry and emits a row.
const WINDOW: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Segment extractor
// ---------------------------------------------------------------------------

/// Segment extractor used in the segmented arm: the segment of a key is its
/// first [`BUCKET_PREFIX_LEN`] bytes (the time bucket). Modeled on the
/// fixed-length test extractors in `slatedb::test_utils`.
#[derive(Debug)]
pub struct BucketPrefixExtractor;

impl PrefixExtractor for BucketPrefixExtractor {
    fn name(&self) -> &str {
        "tsdb-bucket-8"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let len = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b.len(),
        };
        (len >= BUCKET_PREFIX_LEN).then_some(BUCKET_PREFIX_LEN)
    }
}

// ---------------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------------

/// `<bucket: u64 BE><metric: u32 BE><ts: u64 BE>`.
fn encode_key(bucket: u64, metric: u32, ts: u64) -> Bytes {
    let mut b = BytesMut::with_capacity(20);
    b.put_u64(bucket);
    b.put_u32(metric);
    b.put_u64(ts);
    b.freeze()
}

/// `<bucket: u64 BE><metric: u32 BE>` — a scan bound covering all timestamps of
/// one metric in one bucket.
fn encode_prefix(bucket: u64, metric: u32) -> Bytes {
    let mut b = BytesMut::with_capacity(12);
    b.put_u64(bucket);
    b.put_u32(metric);
    b.freeze()
}

// ---------------------------------------------------------------------------
// Frontier: shared slot allocator + bucket clock
// ---------------------------------------------------------------------------

/// Shared append state. A single monotonic slot counter is fetched-and-added by
/// the writer; the bucket, metric, and timestamp of each sample derive from the
/// slot, so the bucket advances on its own once a bucket fills. Readers read the
/// counter to learn the current frontier.
struct Frontier {
    next_slot: AtomicU64,
    samples_per_bucket: u64,
    num_metrics: u32,
    val_len: usize,
}

impl Frontier {
    /// Claim the next sample slot, returning its `(bucket, metric, ts)`.
    fn claim(&self) -> (u64, u32, u64) {
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let bucket = slot / self.samples_per_bucket;
        let idx = slot % self.samples_per_bucket;
        let metric = (idx % self.num_metrics as u64) as u32;
        let ts = idx / self.num_metrics as u64;
        (bucket, metric, ts)
    }

    /// The current (newest) bucket receiving writes — the frontier `A`.
    fn active_bucket(&self) -> u64 {
        self.next_slot.load(Ordering::Relaxed) / self.samples_per_bucket
    }

    /// Total user bytes written so far — the experiment's x-axis (DB size).
    fn bytes_written(&self) -> u64 {
        self.next_slot.load(Ordering::Relaxed) * self.val_len as u64
    }
}

// ---------------------------------------------------------------------------
// In-bencher stats (latency histograms + counters)
// ---------------------------------------------------------------------------

/// Per-window latency distributions and operation counters recorded by the
/// bencher itself (SlateDB-internal metrics are pulled separately from the
/// registry by [`dump_metrics`]).
struct TsdbStats {
    /// Scan latency in microseconds; cleared each window.
    scan_hist: Mutex<Histogram<u64>>,
    /// Put latency in microseconds; cleared each window.
    put_hist: Mutex<Histogram<u64>>,
    scans: AtomicU64,
    scan_rows: AtomicU64,
    scan_empty: AtomicU64,
    puts: AtomicU64,
}

impl TsdbStats {
    fn new() -> Self {
        Self {
            scan_hist: Mutex::new(Histogram::new(3).expect("histogram")),
            put_hist: Mutex::new(Histogram::new(3).expect("histogram")),
            scans: AtomicU64::new(0),
            scan_rows: AtomicU64::new(0),
            scan_empty: AtomicU64::new(0),
            puts: AtomicU64::new(0),
        }
    }

    fn record_scan(&self, elapsed: Duration, rows: u64) {
        let _ = self
            .scan_hist
            .lock()
            .expect("lock")
            .record(elapsed.as_micros() as u64);
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.scan_rows.fetch_add(rows, Ordering::Relaxed);
        if rows == 0 {
            self.scan_empty.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_put(&self, elapsed: Duration) {
        let _ = self
            .put_hist
            .lock()
            .expect("lock")
            .record(elapsed.as_micros() as u64);
        self.puts.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

/// Append-only writer. Reuses a single value buffer to avoid per-put
/// allocation.
async fn run_writer(
    frontier: Arc<Frontier>,
    db: Arc<Db>,
    val_len: usize,
    write_options: WriteOptions,
    stats: Arc<TsdbStats>,
    stop: Arc<AtomicBool>,
) {
    let mut rng = XorShiftRng::from_os_rng();
    let mut value = vec![0u8; val_len];
    while !stop.load(Ordering::Relaxed) {
        let (bucket, metric, ts) = frontier.claim();
        rng.fill_bytes(value.as_mut_slice());
        let key = encode_key(bucket, metric, ts);
        let start = Instant::now();
        match db
            .put_with_options(
                key,
                value.as_slice(),
                &PutOptions::default(),
                &write_options,
            )
            .await
        {
            Ok(_) => stats.record_put(start.elapsed()),
            Err(e) => warn!("put failed [error={}]", e),
        }
    }
}

/// Reader: Zipf metric × sliding window of sealed buckets, one scan each.
async fn run_reader(
    frontier: Arc<Frontier>,
    db: Arc<Db>,
    window_buckets: u64,
    zipf_s: f64,
    num_metrics: u32,
    stats: Arc<TsdbStats>,
    stop: Arc<AtomicBool>,
) {
    let mut rng = XorShiftRng::from_os_rng();
    // Zipf over [1, num_metrics]; the head (low indices) is read most often.
    let zipf = Zipf::new(num_metrics as f64, zipf_s).expect("valid zipf parameters");
    while !stop.load(Ordering::Relaxed) {
        let active = frontier.active_bucket();
        // Need at least one sealed bucket behind the frontier.
        if active < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        let hi = active - 1; // newest sealed bucket
        let lo = hi.saturating_sub(window_buckets - 1);
        let bucket = rng.random_range(lo..=hi);
        let metric = (zipf.sample(&mut rng) as u32)
            .saturating_sub(1)
            .min(num_metrics - 1);

        let start = Instant::now();
        match db
            .scan(encode_prefix(bucket, metric)..encode_prefix(bucket, metric + 1))
            .await
        {
            Ok(mut iter) => {
                let mut rows = 0u64;
                loop {
                    match iter.next().await {
                        Ok(Some(_kv)) => rows += 1,
                        Ok(None) => break,
                        Err(e) => {
                            warn!("scan next failed [error={}]", e);
                            break;
                        }
                    }
                }
                stats.record_scan(start.elapsed(), rows);
            }
            Err(e) => warn!("scan failed [error={}]", e),
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics registry sampling
// ---------------------------------------------------------------------------

fn metric_u64(m: &Metric) -> u64 {
    match m.value {
        MetricValue::Counter(v) => v,
        MetricValue::Gauge(v) => v.max(0) as u64,
        MetricValue::UpDownCounter(v) => v.max(0) as u64,
        _ => 0,
    }
}

/// A counter/gauge with no labels, looked up by name; 0 if absent.
fn scalar(snap: &Metrics, name: &str) -> u64 {
    snap.by_name_and_labels(name, &[])
        .map(metric_u64)
        .unwrap_or(0)
}

/// Sum of object-store `get_range`/`get_ranges` request counts for a given
/// component (`"db"` for read-path block fetches, `"compactor"` for compaction
/// input reads). Read-path `get_range` calls are our cache-miss proxy.
fn os_get_range(snap: &Metrics, component: &str) -> u64 {
    snap.by_name("slatedb.object_store.request_count")
        .iter()
        .filter(|m| {
            let has = |k: &str, v: &str| m.labels.iter().any(|(lk, lv)| lk == k && lv == v);
            has("component", component)
                && has("op", "get")
                && (has("api", "get_range") || has("api", "get_ranges"))
        })
        .map(|m| metric_u64(m))
        .sum()
}

/// Periodically snapshots the SlateDB metrics registry, diffs cumulative
/// counters against the previous window, and emits a CSV row keyed by
/// bytes-written.
async fn dump_metrics(
    recorder: Arc<DefaultMetricsRecorder>,
    stats: Arc<TsdbStats>,
    frontier: Arc<Frontier>,
    csv_path: Option<String>,
    stop: Arc<AtomicBool>,
) {
    use std::io::Write;

    let mut csv = csv_path.map(|p| std::fs::File::create(&p).expect("create metrics csv"));
    if let Some(file) = csv.as_mut() {
        writeln!(
            file,
            "elapsed_s,bytes_written,compaction_bytes_delta,cum_write_amp,l0_sst_count,\
segment_max_l0,backpressure_delta,read_get_range_delta,compactor_get_delta,\
scan_p50_us,scan_p99_us,scan_p999_us,put_p99_us,scans_delta,puts_delta"
        )
        .expect("write csv header");
    }

    let start = Instant::now();
    let mut prev = recorder.snapshot();
    let mut prev_scans = 0u64;
    let mut prev_puts = 0u64;

    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(WINDOW).await;
        let snap = recorder.snapshot();

        let compaction = scalar(&snap, "slatedb.compactor.bytes_compacted");
        let memtable = scalar(&snap, "slatedb.db.memtable_write_bytes");
        let wal = scalar(&snap, "slatedb.db.wal_flush_bytes");
        let l0 = scalar(&snap, "slatedb.db.l0_flush_bytes");
        let cum_write_amp = if memtable > 0 {
            (wal + l0 + compaction) as f64 / memtable as f64
        } else {
            0.0
        };
        let l0_sst = scalar(&snap, "slatedb.db.l0_sst_count");
        let seg_max_l0 = scalar(&snap, "slatedb.db.segment_max_l0_sst_count");
        let backpressure = scalar(&snap, "slatedb.db.backpressure_count");
        let read_gr = os_get_range(&snap, "db");
        let comp_gr = os_get_range(&snap, "compactor");

        // Per-window deltas of cumulative counters.
        let d_compaction =
            compaction.saturating_sub(scalar(&prev, "slatedb.compactor.bytes_compacted"));
        let d_backpressure =
            backpressure.saturating_sub(scalar(&prev, "slatedb.db.backpressure_count"));
        let d_read = read_gr.saturating_sub(os_get_range(&prev, "db"));
        let d_comp_gr = comp_gr.saturating_sub(os_get_range(&prev, "compactor"));

        let (scan_p50, scan_p99, scan_p999) = {
            let mut h = stats.scan_hist.lock().expect("lock");
            let r = (
                h.value_at_quantile(0.5),
                h.value_at_quantile(0.99),
                h.value_at_quantile(0.999),
            );
            h.clear();
            r
        };
        let put_p99 = {
            let mut h = stats.put_hist.lock().expect("lock");
            let r = h.value_at_quantile(0.99);
            h.clear();
            r
        };

        let scans = stats.scans.load(Ordering::Relaxed);
        let puts = stats.puts.load(Ordering::Relaxed);
        let d_scans = scans - prev_scans;
        let d_puts = puts - prev_puts;
        prev_scans = scans;
        prev_puts = puts;

        let bytes_written = frontier.bytes_written();
        let elapsed = start.elapsed().as_secs_f64();

        info!(
            "tsdb [t={:.0}s bytes_written={} compaction_delta={} write_amp={:.2} l0={} \
seg_max_l0={} bp_delta={} read_get_range_delta={} compactor_get_delta={} \
scan_p99={}us scan_p999={}us put_p99={}us scans_delta={} puts_delta={}]",
            elapsed,
            bytes_written,
            d_compaction,
            cum_write_amp,
            l0_sst,
            seg_max_l0,
            d_backpressure,
            d_read,
            d_comp_gr,
            scan_p99,
            scan_p999,
            put_p99,
            d_scans,
            d_puts,
        );

        if let Some(file) = csv.as_mut() {
            writeln!(
                file,
                "{:.0},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{}",
                elapsed,
                bytes_written,
                d_compaction,
                cum_write_amp,
                l0_sst,
                seg_max_l0,
                d_backpressure,
                d_read,
                d_comp_gr,
                scan_p50,
                scan_p99,
                scan_p999,
                put_p99,
                d_scans,
                d_puts,
            )
            .expect("write csv row");
            file.flush().ok();
        }

        prev = snap;
    }
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

/// Run the TSDB segmentation benchmark: one writer, `readers` readers, and a
/// metrics dump task, for `duration` (or one hour if unset).
#[allow(clippy::too_many_arguments)]
pub async fn run(
    db: Arc<Db>,
    recorder: Arc<DefaultMetricsRecorder>,
    write_options: WriteOptions,
    num_metrics: u32,
    bucket_bytes: u64,
    val_len: usize,
    window_buckets: u64,
    zipf_s: f64,
    readers: u32,
    duration: Option<Duration>,
    metrics_csv: Option<String>,
) {
    let samples_per_bucket = (bucket_bytes / val_len as u64).max(1);
    info!(
        "starting tsdb bench [num_metrics={} bucket_bytes={} val_len={} samples_per_bucket={} \
window_buckets={} zipf_s={} readers={}]",
        num_metrics, bucket_bytes, val_len, samples_per_bucket, window_buckets, zipf_s, readers
    );

    let frontier = Arc::new(Frontier {
        next_slot: AtomicU64::new(0),
        samples_per_bucket,
        num_metrics,
        val_len,
    });
    let stats = Arc::new(TsdbStats::new());
    let stop = Arc::new(AtomicBool::new(false));

    let mut tasks = Vec::new();

    // Single writer (append stream into the advancing bucket).
    {
        let frontier = frontier.clone();
        let db = db.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let write_options = write_options.clone();
        tasks.push(tokio::spawn(async move {
            run_writer(frontier, db, val_len, write_options, stats, stop).await
        }));
    }

    // Readers.
    for _ in 0..readers {
        let frontier = frontier.clone();
        let db = db.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            run_reader(
                frontier,
                db,
                window_buckets,
                zipf_s,
                num_metrics,
                stats,
                stop,
            )
            .await
        }));
    }

    // Metrics dump.
    {
        let recorder = recorder.clone();
        let stats = stats.clone();
        let frontier = frontier.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            dump_metrics(recorder, stats, frontier, metrics_csv, stop).await
        }));
    }

    let run_for = duration.unwrap_or_else(|| {
        warn!("no --duration set; running for 1 hour");
        Duration::from_secs(3600)
    });
    tokio::time::sleep(run_for).await;
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = task.await;
    }
}
