//!
//! Common traits and structs for layers
//!

use crate::relish::{RelTag, RelishTag};
use crate::repository::{BlockNumber, ZenithWalRecord};
use crate::{ZTenantId, ZTimelineId};
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use zenith_utils::lsn::Lsn;

// Size of one segment in pages (10 MB)
pub const RELISH_SEG_SIZE: u32 = 10 * 1024 * 1024 / 8192;

///
/// Each relish stored in the repository is divided into fixed-sized "segments",
/// with 10 MB of key-space, or 1280 8k pages each.
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct SegmentTag {
    pub rel: RelishTag,
    pub segno: u32,
}

/// SegmentBlk represents a block number within a segment, or the size of segment.
///
/// This is separate from BlockNumber, which is used for block number within the
/// whole relish. Since this is just a type alias, the compiler will let you mix
/// them freely, but we use the type alias as documentation to make it clear
/// which one we're dealing with.
///
/// (We could turn this into "struct SegmentBlk(u32)" to forbid accidentally
/// assigning a BlockNumber to SegmentBlk or vice versa, but that makes
/// operations more verbose).
pub type SegmentBlk = u32;

impl fmt::Display for SegmentTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.rel, self.segno)
    }
}

impl SegmentTag {
    /// Given a relish and block number, calculate the corresponding segment and
    /// block number within the segment.
    pub const fn from_blknum(rel: RelishTag, blknum: BlockNumber) -> (SegmentTag, SegmentBlk) {
        (
            SegmentTag {
                rel,
                segno: blknum / RELISH_SEG_SIZE,
            },
            blknum % RELISH_SEG_SIZE,
        )
    }
}

pub struct SegmentRange(pub Range<SegmentTag>);

// FIXME: these constants should probably be in relish.rs. Or we should have
// another representation for ranges like these.
pub const MIN_SEG_TAG: SegmentTag = SegmentTag {
    rel: RelishTag::Relation(RelTag {
        forknum: 0,
        spcnode: 0,
        dbnode: 0,
        relnode: 0,
    }),
    segno: 0,
};
pub const MAX_SEG_TAG: SegmentTag = SegmentTag {
    rel: RelishTag::Checkpoint,
    segno: u32::MAX,
};
pub const ALL_SEG_RANGE: SegmentRange = SegmentRange(MIN_SEG_TAG..MAX_SEG_TAG);

impl SegmentRange {
    pub fn get_singleton(&self) -> Option<SegmentTag> {
        if self.0.end.rel == self.0.start.rel && self.0.end.segno == self.0.start.segno + 1 {
            Some(self.0.start)
        } else {
            None
        }
    }

    pub const fn singleton(seg: SegmentTag) -> Self {
        Self(
            seg..SegmentTag {
                rel: seg.rel,
                segno: seg.segno + 1,
            },
        )
    }
}

///
/// Represents a version of a page at a specific LSN. The LSN is the key of the
/// entry in the 'page_versions' hash, it is not duplicated here.
///
/// A page version can be stored as a full page image, or as WAL record that needs
/// to be applied over the previous page version to reconstruct this version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PageVersion {
    Page(Bytes),
    Wal(ZenithWalRecord),
}

///
/// Data needed to reconstruct a page version
///
/// 'page_img' is the old base image of the page to start the WAL replay with.
/// It can be None, if the first WAL record initializes the page (will_init)
/// 'records' contains the records to apply over the base image.
///
/// FIXME: caller can fill in page_img
pub struct PageReconstructData {
    pub records: Vec<(Lsn, ZenithWalRecord)>,
    pub page_img: Option<(Lsn, Bytes)>,
}

/// Return value from Layer::get_page_reconstruct_data
pub enum PageReconstructResult {
    /// Got all the data needed to reconstruct the requested page
    Complete,
    /// This layer didn't contain all the required data, the caller should look up
    /// the predecessor layer at the returned LSN and collect more data from there.
    Continue(Lsn),
    /// This layer didn't contain data needed to reconstruct the page version at
    /// the returned LSN. This is usually considered an error, but might be OK
    /// in some circumstances.
    Missing(Lsn),
}

///
/// A Layer corresponds to one RELISH_SEG_SIZE slice of a relish in a range of LSNs.
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access
/// to the recent page versions. On-disk layers are stored as files on disk, and
/// are immutable. This trait presents the common functionality of
/// in-memory and on-disk layers.
///
pub trait Layer: Send + Sync {
    fn get_tenant_id(&self) -> ZTenantId;

    /// Identify the timeline this relish belongs to
    fn get_timeline_id(&self) -> ZTimelineId;

    /// Range of segments that this layer covers
    fn get_seg_range(&self) -> SegmentRange;

    fn covers_seg(&self, seg: SegmentTag) -> bool;

    /// Inclusive start bound of the LSN range that this layer holds
    fn get_start_lsn(&self) -> Lsn;

    /// Exclusive end bound of the LSN range that this layer holds.
    ///
    /// - For an open in-memory layer, this is MAX_LSN.
    /// - For a frozen in-memory layer or a delta layer, this is a valid end bound.
    /// - An image layer represents snapshot at one LSN, so end_lsn is always the snapshot LSN + 1
    fn get_end_lsn(&self) -> Lsn;

    /// Filename used to store this layer on disk. (Even in-memory layers
    /// implement this, to print a handy unique identifier for the layer for
    /// log messages, even though they're never not on disk.)
    fn filename(&self) -> PathBuf;

    ///
    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// `cached_img_lsn` should be set to a cached page image's lsn < `lsn`.
    /// This function will only return data after `cached_img_lsn`.
    /// FIXME
    ///
    /// See PageReconstructResult for possible return values. The collected data
    /// is appended to reconstruct_data; the caller should pass an empty struct
    /// on first call. If this returns PageReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data'
    /// to collect more data.
    fn get_page_reconstruct_data(
        &self,
        seg: SegmentTag,
        blknum: SegmentBlk,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult>;

    /// Return size of the segment at given LSN. (Only for blocky relations.)
    fn get_seg_size(&self, seg: SegmentTag, lsn: Lsn) -> Result<Option<SegmentBlk>>;

    /// Does the segment exist at given LSN? Or was it dropped before it.
    fn get_seg_exists(&self, seg: SegmentTag, lsn: Lsn) -> Result<bool>;

    /// Does this layer only contain some data for the segment (incremental),
    /// or does it contain a version of every page? This is important to know
    /// for garbage collecting old layers: an incremental layer depends on
    /// the previous non-incremental layer.
    fn is_incremental(&self) -> bool;

    /// Returns true for layers that are represented in memory.
    fn is_in_memory(&self) -> bool;

    /// Release memory used by this layer. There is no corresponding 'load'
    /// function, that's done implicitly when you call one of the get-functions.
    fn unload(&self) -> Result<()>;

    /// Permanently remove this layer from disk.
    fn delete(&self) -> Result<()>;

    /// Dump summary of the contents of the layer to stdout
    fn dump(&self) -> Result<()>;
}
