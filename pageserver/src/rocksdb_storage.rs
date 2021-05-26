//
// A Repository holds all the different page versions and WAL records
//
// This implementation uses RocksDB to store WAL wal records and
// full page images, keyed by the RelFileNode, blocknumber, and the
// LSN.

use crate::repository::{BufferTag, RelTag};
use crate::storage::{Storage, StorageKey, StorageIterator};
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use std::collections::HashSet;
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

pub struct RocksStorage {
    _conf: &'static PageServerConf,

    // RocksDB handle
    db: rocksdb::DB,
}

impl Storage for RocksStorage {
    fn get(&self, key: &StorageKey) -> Result<Vec<u8>> {
        let val = self.db.get(StorageKey::ser(key)?)?;
        if let Some(val) = val {
            Ok(val)
        } else {
            bail!("could not find page {:?}", key);
        }
    }

    fn put(&self, key: &StorageKey, value: &[u8]) -> Result<()> {
        self.db.put(
            StorageKey::ser(key)?,
            value)?;
        Ok(())
    }

    /// Iterate through page versions of given page, starting from the given LSN.
    /// The versions are walked in descending LSN order.
    fn block_iter_backwards<'a>(&'a self, timeline: ZTimelineId, buf: BufferTag, lsn: Lsn) -> Result<Box<dyn StorageIterator + 'a>> {
        let iter = BlockBackwardsIter::new(&self.db, timeline, buf, lsn)?;
        Ok(Box::new(iter))
    }

    fn list_rels<'a>(&'a self, timelineid: ZTimelineId, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {
        // FIXME: This scans everything. Very slow

        let mut rels: HashSet<RelTag> = HashSet::new();

        let searchkey = StorageKey {
            timeline: timelineid,
            buf_tag: BufferTag {
                rel: RelTag {
                    spcnode: spcnode,
                    dbnode: dbnode,
                    relnode: 0,
                    forknum: 0u8,
                },
                blknum: 0,
            },
            lsn: Lsn(0),
        };
        let mut iter = self.db.raw_iterator();
        iter.seek(searchkey.ser()?);
        while iter.valid() {
            let key = StorageKey::des(iter.key().unwrap())?;
            if key.buf_tag.rel.spcnode != spcnode || key.buf_tag.rel.dbnode != dbnode {
                break;
            }

            if key.lsn < lsn {
                rels.insert(key.buf_tag.rel);
            }
            iter.next();
        }

        Ok(rels)
    }
}

impl RocksStorage {
    /// Open a RocksDB database, and load the last valid and record LSNs into memory.
    pub fn open(conf: &'static PageServerConf) -> Result<RocksStorage> {
        let path = conf.workdir.join("rocksdb-storage");
        let db = rocksdb::DB::open(&Self::get_rocksdb_opts(), path)?;

        let storage = RocksStorage {
            _conf: conf,
            db,
        };
        Ok(storage)
    }

    /// Create a new, empty RocksDB database.
    pub fn create(conf: &'static PageServerConf) -> Result<RocksStorage> {
        let path = conf.workdir.join("rocksdb-storage");
        std::fs::create_dir(&path)?;

        let mut opts = Self::get_rocksdb_opts();
        opts.create_if_missing(true);
        opts.set_error_if_exists(true);
        let db = rocksdb::DB::open(&opts, &path)?;

        let storage = RocksStorage {
            _conf: conf,
            db,
        };
        Ok(storage)
    }

    /// common options used by `open` and `create`
    fn get_rocksdb_opts() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_use_fsync(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // FIXME
/*
        opts.set_compaction_filter("ttl", move |_level: u32, _key: &[u8], val: &[u8]| {
            if (val[0] & UNUSED_VERSION_FLAG) != 0 {
                rocksdb::compaction_filter::Decision::Remove
            } else {
                rocksdb::compaction_filter::Decision::Keep
            }
        });
*/
        opts
    }
}



///
/// Iterator for `block_iter_backwards. Returns all page versions of a given block, in
/// reverse LSN order.
///
struct BlockBackwardsIter<'a> {
    buf_tag: BufferTag,
    dbiter: rocksdb::DBRawIterator<'a>,
    first_call: bool,
}
impl<'a> BlockBackwardsIter<'a> {
    fn new(db: &'a rocksdb::DB, timeline: ZTimelineId, buf_tag: BufferTag, lsn: Lsn) -> Result<BlockBackwardsIter<'a>>{
        let key = StorageKey {
            timeline,
            buf_tag,
            lsn,
        };
        let mut dbiter = db.raw_iterator();
        dbiter.seek_for_prev(StorageKey::ser(&key)?); // locate last entry
        Ok(BlockBackwardsIter {
            first_call: true,
            buf_tag,
            dbiter
        })
    }
}
impl<'a> Iterator for BlockBackwardsIter<'a>
{
    type Item = (StorageKey, Vec<u8>);

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {

        if self.first_call {
            self.first_call = false;
        } else {
            self.dbiter.prev(); // walk backwards
        }

        if !self.dbiter.valid() {
            return None;
        }
        let key = StorageKey::des(self.dbiter.key().unwrap()).unwrap();
        if key.buf_tag != self.buf_tag {
            return None;
        }
        let val = self.dbiter.value().unwrap();
        let result = val.to_vec();

        return Some((key, result));
    }
}
impl StorageIterator for BlockBackwardsIter<'_> {

}
