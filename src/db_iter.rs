use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;
use crate::config::DbRecord;
use crate::db_state::{DbStateSnapshot, SsTableHandle};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::MaterializedIterator;
use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
use crate::range_util::BytesRange;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::SstIterator;
use crate::types::ValueDeletable;

type ScanIterator<'a> = TwoMergeIterator<
    MaterializedIterator,
    TwoMergeIterator<MergeIterator<SstIterator<'a, Arc<SsTableHandle>>>, MergeIterator<SortedRunIterator<'a, Arc<SsTableHandle>>>>,
>;

pub struct DbIterator<'a> {
    #[allow(dead_code)]
    snapshot: Arc<DbStateSnapshot>,
    #[allow(dead_code)]
    range: &'a BytesRange,
    iter: ScanIterator<'a>,
    invalidated: bool,
}

impl<'a> DbIterator<'a> {
    pub(crate) async fn new(
        snapshot: Arc<DbStateSnapshot>,
        range: &'a BytesRange,
        mem_iter: MaterializedIterator,
        l0_iters: VecDeque<SstIterator<'a, Arc<SsTableHandle>>>,
        sr_iters: VecDeque<SortedRunIterator<'a, Arc<SsTableHandle>>>,
    ) -> Result<Self, SlateDBError> {
        let l0_iter = MergeIterator::new(l0_iters).await?;
        let sr_iter = MergeIterator::new(sr_iters).await?;
        let sst_iter = TwoMergeIterator::new(l0_iter, sr_iter).await?;
        let iter = TwoMergeIterator::new(mem_iter, sst_iter).await?;
        Ok(DbIterator { snapshot, range, iter, invalidated: false })
    }

    /// Get the next record in the scan.
    ///
    /// returns Ok(None) when the scan is complete
    /// returns Err(InvalidatedIterator) if the iterator has been invalidated
    ///  due to an underlying error
    pub async fn next(&mut self) -> Result<Option<DbRecord>, SlateDBError> {
        if self.invalidated {
            Err(SlateDBError::InvalidatedIterator)
        } else {
            loop {
                let next_opt = self.iter.next_entry().await?;
                if let Some(kv) = next_opt {
                    match kv.value {
                        ValueDeletable::Value(value) => {
                            let record = DbRecord { key: kv.key, value };
                            return Ok(Some(record));
                        }
                        ValueDeletable::Tombstone => continue
                    }
                } else {
                    return Ok(None)
                }
            }
        }
    }

    /// Seek to a key ahead of the last key returned from the iterator or
    /// the lower range bound if no records have yet been returned.
    ///
    /// returns Ok(()) if the position is successfully advanced
    /// returns SlateDbError::InvalidArgument if `lower_bound` is `Unbounded`
    /// returns SlateDbError::InvalidArgument if the key is comes before the
    ///  current iterator position.
    /// returns Err(InvalidatedIterator) if the iterator has been invalidated
    ///  due to an underlying error
    #[allow(dead_code)]
    pub async fn seek(&mut self, _lower_bound: Bound<&[u8]>) -> Result<(), SlateDBError> {
        unimplemented!()
    }
}