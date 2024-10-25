use std::ops::Bound;
use std::ops::Bound::{Unbounded, Excluded, Included};
use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct BytesRange {
    pub start_bound: Bound<Bytes>,
    pub end_bound: Bound<Bytes>,
}

impl BytesRange {

    pub fn new(start_bound: Bound<Bytes>, end_bound: Bound<Bytes>) -> Self {
        Self { start_bound, end_bound }
    }

    pub fn unbounded() -> Self {
        Self::new(Unbounded, Unbounded)
    }

    pub fn with_end_bound(end_bound: Bound<Bytes>) -> Self {
        Self::new(Unbounded, end_bound)
    }

    pub fn with_start_bound(start_bound: Bound<Bytes>) -> Self {
        Self::new(start_bound, Unbounded)
    }


    /// Checks whether this range has an end bound which is strictly lower
    /// than the provided start bound, which would indicate an empty
    /// intersection
    fn has_lower_end_bound(
        &self,
        start_bound: &Bound<Bytes>,
    ) -> bool {
        match start_bound {
            Unbounded => false,
            Included(start_bound) => match &self.end_bound {
                Unbounded => false,
                Included(end_bound) => end_bound < start_bound,
                Excluded(end_bound) => end_bound <= start_bound,
            },
            Excluded(start_bound) => match &self.end_bound {
                Unbounded => false,
                Included(end_bound) => end_bound <= start_bound,
                Excluded(end_bound) => end_bound <= start_bound,
            },
        }
    }


    fn has_higher_start_bound(
        &self,
        end_bound: &Bound<Bytes>,
    ) -> bool {
        match end_bound {
            Unbounded => false,
            Included(end_bound) => match &self.start_bound {
                Unbounded => false,
                Included(start_bound) => start_bound > end_bound,
                Excluded(start_bound) => start_bound >= end_bound,
            },
            Excluded(end_bound) => match &self.start_bound {
                Unbounded => false,
                Included(start_bound) => start_bound >= end_bound,
                Excluded(start_bound) => start_bound >= end_bound,
            },
        }
    }

    pub(crate) fn has_nonempty_intersection(
        &self,
        range: &BytesRange,
    ) -> bool {
        !self.has_lower_end_bound(&range.start_bound) &&
            !self.has_higher_start_bound(&range.end_bound)
    }
}

pub(crate) fn start_bound(
    range: &BytesRange,
) -> Option<&[u8]> {
    as_option(&range.start_bound)
}

fn as_option(
    bound: &Bound<Bytes>
) -> Option<&[u8]> {
    match bound {
        Unbounded => None,
        Included(bytes) => Some(bytes.as_ref()),
        Excluded(bytes) => Some(bytes.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;
    use std::ops::Bound::{Excluded, Included};
    use bytes::Bytes;
    use crate::range_util::BytesRange;

    #[test]
    fn test_basic_range_ops() {
        let unbounded_range = BytesRange::unbounded();
        assert_eq!(Bound::Unbounded, unbounded_range.start_bound);
        assert_eq!(Bound::Unbounded, unbounded_range.end_bound);

        let bound_key = Bytes::copy_from_slice("foo".as_bytes());
        let inclusive_bound_key = Bound::Included(bound_key.clone());
        let exclusive_bound_key = Bound::Excluded(bound_key.clone());

        let upper_bounded_inclusive_range = BytesRange::with_end_bound(inclusive_bound_key.clone());
        assert_eq!(Bound::Unbounded, upper_bounded_inclusive_range.start_bound);
        assert_eq!(inclusive_bound_key.clone(), upper_bounded_inclusive_range.end_bound);

        let upper_bounded_exclusive_range = BytesRange::with_end_bound(exclusive_bound_key.clone());
        assert_eq!(Bound::Unbounded, upper_bounded_exclusive_range.start_bound);
        assert_eq!(exclusive_bound_key.clone(), upper_bounded_exclusive_range.end_bound);

        let lower_bounded_inclusive_range = BytesRange::with_start_bound(inclusive_bound_key.clone());
        assert_eq!(inclusive_bound_key.clone(), lower_bounded_inclusive_range.start_bound);
        assert_eq!(Bound::Unbounded, lower_bounded_inclusive_range.end_bound);

        let lower_bounded_exclusive_range = BytesRange::with_start_bound(exclusive_bound_key.clone());
        assert_eq!(exclusive_bound_key.clone(), lower_bounded_exclusive_range.start_bound);
        assert_eq!(Bound::Unbounded, lower_bounded_exclusive_range.end_bound);
    }

    #[test]
    fn test_may_overlap_with_unbounded_range() {
        let unbounded_range = BytesRange::unbounded();
        let bound_key = Bytes::copy_from_slice("abc".as_bytes());

        let lower_bounded_inclusive_range = BytesRange::with_start_bound(Included(bound_key.clone()));
        assert_eq!(true, unbounded_range.has_nonempty_intersection(&lower_bounded_inclusive_range));
        assert_eq!(true, lower_bounded_inclusive_range.has_nonempty_intersection(&unbounded_range));

        let lower_bounded_exclusive_range = BytesRange::with_start_bound(Excluded(bound_key.clone()));
        assert_eq!(true, unbounded_range.has_nonempty_intersection(&lower_bounded_exclusive_range));
        assert_eq!(true, lower_bounded_exclusive_range.has_nonempty_intersection(&unbounded_range));

        let upper_bounded_inclusive_range = BytesRange::with_end_bound(Included(bound_key.clone()));
        assert_eq!(true, unbounded_range.has_nonempty_intersection(&upper_bounded_inclusive_range));
        assert_eq!(true, upper_bounded_inclusive_range.has_nonempty_intersection(&unbounded_range));

        let upper_bounded_exclusive_range = BytesRange::with_end_bound(Excluded(bound_key.clone()));
        assert_eq!(true, unbounded_range.has_nonempty_intersection(&upper_bounded_exclusive_range));
        assert_eq!(true, upper_bounded_exclusive_range.has_nonempty_intersection(&unbounded_range));
    }

}