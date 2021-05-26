use crate::repository::{BufferTag, RelTag};
use crate::ZTimelineId;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use zenith_utils::lsn::Lsn;
use std::collections::HashSet;
use std::iter::Iterator;

// For outer postgres Tenant and Timeline should look like uuid or hash,
// but inside the pageserver we can store mapping to int which would enumerate
// them and that int will be used in actual keys that would be written to disk.
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct StorageKey {
    //TODO: tenant: Tenant,
    pub timeline: ZTimelineId,
    pub buf_tag: BufferTag,
    pub lsn: Lsn,
}

///
/// Low-level storage abstraction.
///
/// All the data in the repository is stored in a key-value store. This trait
/// abstracts the details of the key-value store.
///
/// A simple key-value store would support just GET and PUT operations with
/// a key, but the upper layer needs slightly complicated read operations
///
/// The most frequently used function is 'block_iter_backwards'. It is used
/// to look up a page version. It's LSN aware, in that the caller specifies an
/// LSN, and the function returns all values for that block with the same
/// or older LSN.
///
/// Store contains all the pageserver data of all tenants and don't know
/// anything about our more high-level concepts like timelines and tenants.
/// It just provides abstraction of persistent ordered collection.
pub trait Storage: Send + Sync  {

    ///
    /// Store a value with given key.
    ///
    fn put(&self, key: &StorageKey, value: &[u8]) -> Result<()>;

    /// Read entry with the exact given key.
    ///
    /// This is used for retrieving metadata with special key that doesn't
    /// correspond to any real relation.
    fn get(&self, key: &StorageKey) -> Result<Vec<u8>>;

    /// Iterate through all page versions of given block, in descending LSN order.
    fn block_iter_backwards<'a>(&'a self, timeline: ZTimelineId, buf: BufferTag, lsn: Lsn) -> Result<Box<dyn StorageIterator + 'a>>;

    /// Iterate through all keys with given tablespace and database ID, and LSN <= 'lsn'.
    ///
    /// This is used to implement 'create database'
    fn list_rels<'a>(&'a self, timelineid: ZTimelineId, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>>;

    // we don't really delete any data in storage we only can throw away some kv
    // pairs during lsm-like merge. (that may be records with old lsn or whole
    // database -- callback would receive key and return bool indicating whether
    // to keep that kv pair or not).
    //fn set_merge_filter()
}

pub trait StorageIterator: Iterator<Item = (StorageKey, Vec<u8>)> {

}
