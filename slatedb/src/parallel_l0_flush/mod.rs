//! Experimental parallel L0 flush pipeline.
//!
//! This module is intentionally isolated from the existing `mem_table_flush`
//! implementation so the new pipeline can be developed and validated without
//! changing current production behavior.

/// Monotonic ordering token assigned by the parallel L0 flush orchestrator.
///
/// Workers carry this through upload completion so the sequencer can restore
/// the original immutable-memtable retirement order.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FlushEpoch(pub(crate) u64);

pub(crate) mod orchestrator;
pub(crate) mod sequencer;
pub(crate) mod uploader;
