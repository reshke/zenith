use crate::{PageServerConf, ZTimelineId};
use crate::storage::{Storage, StorageKey, StorageIterator};
use crate::walredo::WalRedoManager;
use crate::restore_local_repo::import_timeline_wal;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::forknumber_to_name;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;
use log::*;
use zenith_utils::bin_ser::BeSer;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
///
pub struct Repository {
    storage: Arc<dyn Storage>,
    conf: &'static PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<Timeline>>>,
    walredo_mgr: Arc<dyn WalRedoManager>,
}

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(600);

impl Repository {
    pub fn new(
        conf: &'static PageServerConf,
        storage: Arc<dyn Storage>,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> Repository {
        Repository {
            conf,
            storage,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }

    /// Get Timeline handle for given zenith timeline ID.
    pub fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline =
                    Timeline::open(Arc::clone(&self.storage), timelineid, self.walredo_mgr.clone())?;

                // Load any new WAL after the last checkpoint into the repository.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self.conf.timeline_path(timelineid).join("wal");
                import_timeline_wal(&wal_dir, &timeline, timeline.get_last_record_lsn())?;

                let timeline_rc = Arc::new(timeline);

                if self.conf.gc_horizon != 0 {
                    Timeline::launch_gc_thread(self.conf, timeline_rc.clone());
                }

                timelines.insert(timelineid, timeline_rc.clone());

                Ok(timeline_rc)
            }
        }
    }

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    pub fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Write metadata key
        let metadata = MetadataEntry {
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            ancestor_timeline: None,
            ancestor_lsn: start_lsn,
        };
        self.storage.put(&timeline_metadata_key(timelineid), &MetadataEntry::ser(&metadata)?)?;

        info!("Created empty timeline {}", timelineid);

        let timeline =
            Timeline::open(Arc::clone(&self.storage), timelineid, self.walredo_mgr.clone())?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());

        // don't start the garbage collector for unit tests, either.

        Ok(timeline_rc)
    }

    /// Branch a timeline
    pub fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, at_lsn: Lsn) -> Result<()> {

        // just to check the source timeline exists
        let _ = self.get_timeline(src)?;

        // Write a metadata key, noting the ancestor of th new timeline. There is initially
        // no data in it, but all the read-calls know to look into the ancestor.
        let metadata = MetadataEntry {
            last_valid_lsn: at_lsn,
            last_record_lsn: at_lsn,
            ancestor_timeline: Some(src),
            ancestor_lsn: at_lsn,
        };
        self.storage.put(&timeline_metadata_key(dst), &MetadataEntry::ser(&metadata)?)?;

        Ok(())
    }
}

///
/// A handle to a specific timeline in the repository. This is the API that's exposed
/// to the rest of the system.
///
/// The API can be divided into two main part: GET and PUT functions. The GET functions
/// are used to serve the requests from compute node, to read a relation. The GET main
/// functions are:
///
/// get_page_at_lsn: Read a page at a given LSN
/// get_rel_size: Get relation size at given LSN
/// get_rel_exists: Does relation exist at given LSN?
///
/// The entry point to the PUT side is 'save_decoded_record'. It looks at a WAL record,
/// figures out which pages it modifies, and stores a copy of the WAL record for all
/// the modified pages. It also handles a few special WAL record types like CREATE
/// DATABASE and relation truncation, which affect relation data.
///
pub struct Timeline {
    timelineid: ZTimelineId,

    // Backing key-value store
    storage: Arc<dyn Storage>,

    // WAL redo manager, for reconstructing page versions from WAL records.
    walredo_mgr: Arc<dyn WalRedoManager>,

    // What page versions do we hold in the cache? If we get a request > last_valid_lsn,
    // we need to wait until we receive all the WAL up to the request. The SeqWait
    // provides functions for that. TODO: If we get a request for an old LSN, such that
    // the versions have already been garbage collected away, we should throw an error,
    // but we don't track that currently.
    //
    // last_record_lsn points to the end of last processed WAL record.
    // It can lag behind last_valid_lsn, if the WAL receiver has received some WAL
    // after the end of last record, but not the whole next record yet. In the
    // page cache, we care about last_valid_lsn, but if the WAL receiver needs to
    // restart the streaming, it needs to restart at the end of last record, so
    // we track them separately. last_record_lsn should perhaps be in
    // walreceiver.rs instead of here, but it seems convenient to keep all three
    // values together.
    //
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,

    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

impl Timeline {

    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory.
    fn open(
        storage: Arc<dyn Storage>,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> Result<Timeline> {
        // Load metadata into memory
        let v = storage
            .get(&timeline_metadata_key(timelineid))
            .with_context(|| "timeline not found in repository")?;
        let metadata = MetadataEntry::des(&v)?;

        let timeline = Timeline {
            timelineid,
            storage,
            walredo_mgr,
            last_valid_lsn: SeqWait::new(metadata.last_valid_lsn),
            last_record_lsn: AtomicLsn::new(metadata.last_record_lsn.0),
            ancestor_timeline: metadata.ancestor_timeline,
            ancestor_lsn: metadata.ancestor_lsn,
        };
        Ok(timeline)
    }

    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page in the cache.
    pub fn get_page_at_lsn(&self, tag: BufferTag, req_lsn: Lsn) -> Result<Bytes> {
        let lsn = self.wait_lsn(req_lsn)?;

        self.get_page_at_lsn_nowait(tag, lsn)
    }
    fn get_page_at_lsn_nowait(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes> {
        // Look up the page entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let mut iter = self.block_iter_backwards(&*self.storage, tag, lsn)?;

        if let Some((key, value)) = iter.next().transpose()? {
            let page_img: Bytes;

            match PageEntry::des(&value)? {
                PageEntry::Page(img) => {
                    page_img = img;
                },
                PageEntry::WALRecord(_rec) => {
                    // Request the WAL redo manager to apply the WAL records for us.
                    let (base_img, records) = self.collect_records_for_apply(tag, lsn)?;
                    page_img = self.walredo_mgr.request_redo(tag, lsn, base_img, records)?;

                    self.put_page_image(tag, lsn, page_img.clone())?;
                }
            }
            // FIXME: assumes little-endian. Only used for the debugging log though
            let page_lsn_hi =
                u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
            let page_lsn_lo =
                u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
            info!(
                "Returning page with LSN {:X}/{:X} for {} blk {} from {} (request {})",
                page_lsn_hi, page_lsn_lo, tag.rel, tag.blknum, key.lsn, lsn
            );
            return Ok(page_img);
        }
        static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
        info!(
            "Page {} blk {} at {} not found",
            tag.rel, tag.blknum, lsn
        );
        Ok(Bytes::from_static(&ZERO_PAGE))
        /* return Err("could not find page image")?; */
    }

    /// Get size of relation
    pub fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;

        match self.relsize_get_nowait(rel, lsn)? {
            Some(nblocks) => Ok(nblocks),
            None => bail!("relation {} not found at {}", rel, lsn)
        }
    }

    ///
    /// Does relation exist at given LSN?
    ///
    /// FIXME: this actually returns true, if the relation exists at *any* LSN
    /// Does relation exist?
    pub fn get_rel_exists(&self, rel: RelTag, req_lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(req_lsn)?;
        let key = relation_size_key(self.timelineid, rel, lsn);
        let mut iter = self.block_iter_backwards(&*self.storage, key.buf_tag, lsn)?;
        if let Some((_key, _val)) = iter.next().transpose()? {
            debug!("Relation {} exists at {}", rel, lsn);
            return Ok(true);
        }
        debug!("Relation {} doesn't exist at {}", rel, lsn);
        Ok(false)
    }

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    pub fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) -> Result<()> {
        let lsn = rec.lsn;
        let key = StorageKey { timeline: self.timelineid, buf_tag: tag, lsn };
        let val = PageEntry::WALRecord(rec);

        self.storage.put(&key, &PageEntry::ser(&val)?)?;
        debug!(
            "put_wal_record rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );

        // Also check if this created or extended the file
        let old_nblocks = self.relsize_get_nowait(tag.rel, lsn)?.unwrap_or(0);

        if tag.blknum >= old_nblocks {
            let new_nblocks = tag.blknum + 1;
            let key = relation_size_key(self.timelineid, tag.rel, lsn);
            let val = RelationSizeEntry::Size(new_nblocks);

            info!("Extended relation {} from {} to {} blocks at {}", tag.rel, old_nblocks, new_nblocks, lsn);

            self.storage.put(&key, &RelationSizeEntry::ser(&val)?)?;
        }

        Ok(())
    }

    ///
    /// Memorize a full image of a page version
    ///
    pub fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) -> Result<()> {
        let key = StorageKey { timeline: self.timelineid, buf_tag: tag, lsn };
        let val = PageEntry::Page(img);

        self.storage.put(&key, &PageEntry::ser(&val)?)?;

        debug!(
            "put_page_image rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );

        // Also check if this created or extended the file
        let old_nblocks = self.relsize_get_nowait(tag.rel, lsn)?.unwrap_or(0);

        if tag.blknum >= old_nblocks {
            let new_nblocks = tag.blknum + 1;
            let key = relation_size_key(self.timelineid, tag.rel, lsn);
            let val = RelationSizeEntry::Size(new_nblocks);

            info!("Extended relation {} from {} to {} blocks at {}", tag.rel, old_nblocks, new_nblocks, lsn);

            self.storage.put(&key, &RelationSizeEntry::ser(&val)?)?;
        }

        Ok(())
    }

    ///
    /// Adds a relation-wide WAL record (like truncate) to the repository,
    /// associating it with all pages started with specified block number
    ///
    pub fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> Result<()> {

        let key = relation_size_key(self.timelineid, rel, lsn);
        let val = RelationSizeEntry::Size(nblocks);

        info!("Truncate relation {} to {} blocks at {}", rel, nblocks, lsn);

        self.storage.put(&key, &RelationSizeEntry::ser(&val)?)?;

        Ok(())
    }

    /// Remember the all WAL before the given LSN has been processed.
    ///
    /// The WAL receiver calls this after the put_* functions, to indicate that
    /// all WAL before this point has been digested. Before that, if you call
    /// GET on an earlier LSN, it will block.
    pub fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);

        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last valid LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    pub fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    pub fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
    }

    /// Like `advance_last_valid_lsn`, but this always points to the end of
    /// a WAL record, not in the middle of one.
    ///
    /// This must be <= last valid LSN. This is tracked separately from last
    /// valid LSN, so that the WAL receiver knows where to restart streaming.
    ///
    /// NOTE: this updates last_valid_lsn as well.
    pub fn advance_last_record_lsn(&self, lsn: Lsn) {
        // Can't move backwards.
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old <= lsn);

        // Also advance last_valid_lsn
        let old = self.last_valid_lsn.advance(lsn);
        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last record LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }
    pub fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load()
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.

    // Flush all the changes written so far with PUT functions to disk.
    // RocksDB writes out things as we go (?), so we don't need to do much here. We just
    // write out the last valid and record LSNs.
    pub fn checkpoint(&self) -> Result<()> {

        let metadata = MetadataEntry {
            last_valid_lsn: self.last_valid_lsn.load(),
            last_record_lsn: self.last_record_lsn.load(),
            ancestor_timeline: self.ancestor_timeline,
            ancestor_lsn: self.ancestor_lsn,
        };
        self.storage.put(&timeline_metadata_key(self.timelineid), &MetadataEntry::ser(&metadata)?)?;

        trace!("checkpoint at {}", metadata.last_valid_lsn);

        Ok(())
    }

    //
    // Internal function to get relation size at given LSN.
    //
    // The caller must ensure that WAL has been received up to 'lsn'.
    //
    fn relsize_get_nowait(&self, rel: RelTag, lsn: Lsn) -> Result<Option<u32>> {
        let key = relation_size_key(self.timelineid, rel, lsn);
        let mut iter = self.block_iter_backwards(&*self.storage, key.buf_tag, lsn)?;

        if let Some((key, value)) = iter.next().transpose()? {
            match RelationSizeEntry::des(&value)? {
                RelationSizeEntry::Size(nblocks) => {
                    info!("relation {} has size {} at {} (request {})", rel, nblocks, key.lsn, lsn);
                    Ok(Some(nblocks))
                }
                RelationSizeEntry::Unlink => {
                    info!("relation {} not found; it was dropped at lsn {}", rel, key.lsn);
                    Ok(None)
                }
            }
        } else {
            info!("relation {} not found at {}", rel, lsn);
            Ok(None)
        }
    }

    ///
    /// Collect all the WAL records that are needed to reconstruct a page
    /// image for the given cache entry.
    ///
    /// Returns an old page image (if any), and a vector of WAL records to apply
    /// over it.
    ///
    fn collect_records_for_apply(
        &self,
        tag: BufferTag,
        lsn: Lsn,
    ) -> Result<(Option<Bytes>, Vec<WALRecord>)> {
        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        let mut iter = self.block_iter_backwards(&*self.storage, tag, lsn)?;
        while let Some((_key, value)) = iter.next().transpose()? {
            match PageEntry::des(&value)? {
                PageEntry::Page(img) => {
                    // We have a base image. No need to dig deeper into the list of
                    // records
                    base_img = Some(img);
                    break;
                }
                PageEntry::WALRecord(rec) => {
                    records.push(rec.clone());
                    // If this WAL record initializes the page, no need to dig deeper.
                    if rec.will_init {
                        break;
                    }
                }
            }
        }
        records.reverse();
        Ok((base_img, records))
    }


    fn launch_gc_thread(conf: &'static PageServerConf, timeline_rc: Arc<Timeline>) {
        let timeline_rc_copy = timeline_rc.clone();
        let _gc_thread = thread::Builder::new()
            .name("Garbage collection thread".into())
            .spawn(move || {
                // FIXME
                timeline_rc_copy.do_gc(conf).expect("GC thread died");
            })
            .unwrap();
    }

    fn do_gc(&self, conf: &'static PageServerConf) -> Result<()> {
        loop {
            thread::sleep(conf.gc_period);

            // FIXME: broken
            /*
            let last_lsn = self.get_last_valid_lsn();

            // checked_sub() returns None on overflow.
            if let Some(horizon) = last_lsn.checked_sub(conf.gc_horizon) {
                let mut maxkey = StorageKey {
                    tag: BufferTag {
                        rel: RelTag {
                            spcnode: u32::MAX,
                            dbnode: u32::MAX,
                            relnode: u32::MAX,
                            forknum: u8::MAX,
                        },
                        blknum: u32::MAX,
                    },
                    lsn: Lsn::MAX,
                };
                let now = Instant::now();
                let mut reconstructed = 0u64;
                let mut truncated = 0u64;
                let mut inspected = 0u64;
                let mut deleted = 0u64;
                loop {
                    let mut iter = self.db.raw_iterator();
                    iter.seek_for_prev(maxkey.to_bytes());
                    if iter.valid() {
                        let key = StorageKey::des(iter.key().unwrap());
                        let val = StorageValue::des(iter.value().unwrap());

                        inspected += 1;

                        // Construct boundaries for old records cleanup
                        maxkey.tag = key.tag;
                        let last_lsn = key.lsn;
                        maxkey.lsn = min(horizon, last_lsn); // do not remove last version

                        let mut minkey = maxkey.clone();
                        minkey.lsn = Lsn(0); // first version

                        // reconstruct most recent page version
                        if let StorageValueContent::Image(_) = val.content {
                            // force reconstruction of most recent page version
                            let (base_img, records) =
                                self.collect_records_for_apply(key.tag, key.lsn);

                            trace!(
                                "Reconstruct most recent page {} blk {} at {} from {} records",
                                key.tag.rel,
                                key.tag.blknum,
                                key.lsn,
                                records.len()
                            );

                            let new_img = self
                                .walredo_mgr
                                .request_redo(key.tag, key.lsn, base_img, records)?;
                            self.put_page_image(key.tag, key.lsn, new_img.clone());

                            reconstructed += 1;
                        }

                        iter.seek_for_prev(maxkey.to_bytes());
                        if iter.valid() {
                            // do not remove last version
                            if last_lsn > horizon {
                                // locate most recent record before horizon
                                let key = StorageKey::des(iter.key().unwrap());
                                if key.tag == maxkey.tag {
                                    let val = StorageValue::des(iter.value().unwrap());
                                    if let StorageValueContent::Image(_) = val.content {
                                        let (base_img, records) =
                                            self.collect_records_for_apply(key.tag, key.lsn);
                                        trace!("Reconstruct horizon page {} blk {} at {} from {} records",
                                              key.tag.rel, key.tag.blknum, key.lsn, records.len());
                                        let new_img = self
                                            .walredo_mgr
                                            .request_redo(key.tag, key.lsn, base_img, records)?;
                                        self.put_page_image(key.tag, key.lsn, new_img.clone());

                                        truncated += 1;
                                    } else {
                                        trace!(
                                            "Keeping horizon page {} blk {} at {}",
                                            key.tag.rel,
                                            key.tag.blknum,
                                            key.lsn
                                        );
                                    }
                                }
                            } else {
                                trace!(
                                    "Last page {} blk {} at {}, horizon {}",
                                    key.tag.rel,
                                    key.tag.blknum,
                                    key.lsn,
                                    horizon
                                );
                            }
                            // remove records prior to horizon
                            loop {
                                iter.prev();
                                if !iter.valid() {
                                    break;
                                }
                                let key = StorageKey::des(iter.key().unwrap());
                                if key.tag != maxkey.tag {
                                    break;
                                }
                                let mut val = StorageValue::des(iter.value().unwrap());
                                if val.alive {
                                    val.alive = false;
                                    self.storage.put(key, val)?;
                                    deleted += 1;
                                    trace!(
                                        "deleted: {} blk {} at {}",
                                        key.tag.rel,
                                        key.tag.blknum,
                                        key.lsn
                                    );
                                } else {
                                    break;
                                }
                            }
                        }
                        maxkey = minkey;
                    } else {
                        break;
                    }
                }
                info!("Garbage collection completed in {:?}:\n{} version chains inspected, {} pages reconstructed, {} version histories truncated, {} versions deleted",
					  now.elapsed(), inspected, reconstructed, truncated, deleted);
            }
             */
        }
    }

    //
    // Wait until WAL has been received up to the given LSN.
    //
    fn wait_lsn(&self, mut lsn: Lsn) -> Result<Lsn> {
        // When invalid LSN is requested, it means "don't wait, return latest version of the page"
        // This is necessary for bootstrap.
        if lsn == Lsn(0) {
            let last_valid_lsn = self.last_valid_lsn.load();
            trace!(
                "walreceiver doesn't work yet last_valid_lsn {}, requested {}",
                last_valid_lsn,
                lsn
            );
            lsn = last_valid_lsn;
        }
        trace!(
            "Start waiting for LSN {}, valid LSN is {}",
            lsn,
            self.last_valid_lsn.load()
        );
        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive. valid LSN in {}",
                    lsn,
                    self.last_valid_lsn.load(),
                )
            })?;
        //trace!("Stop waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());

        Ok(lsn)
    }

    //
    // Functions matching the ones in Storage, but ancestor-aware
    //

    fn block_iter_backwards<'a>(&self, storage: &'a dyn Storage, buf_tag: BufferTag, lsn: Lsn) -> Result<BlockBackwardsIter<'a>>{
        let current_iter = storage.block_iter_backwards(self.timelineid, buf_tag, lsn)?;

        Ok(BlockBackwardsIter {
            storage,
            buf_tag: buf_tag,
            current_iter: current_iter,
            ancestor_timeline: self.ancestor_timeline,
            ancestor_lsn: self.ancestor_lsn,
        })
    }

    pub fn list_rels<'a>(&'a self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {

        let mut all_rels = self.storage.list_rels(self.timelineid, spcnode, dbnode, lsn)?;

        let mut prev_timeline: Option<ZTimelineId> = self.ancestor_timeline;
        let mut lsn = self.ancestor_lsn;
        while let Some(timeline) = prev_timeline {
            let this_rels = self.storage.list_rels(timeline, spcnode, dbnode, lsn)?;

            for rel in this_rels {
                all_rels.insert(rel);
            }

            // Load ancestor metadata.
            let v = self.storage
                .get(&timeline_metadata_key(timeline))
                .with_context(|| "timeline not found in repository")?;
            let metadata = MetadataEntry::des(&v)?;

            prev_timeline = metadata.ancestor_timeline;
            lsn = metadata.ancestor_lsn;
        }

        Ok(all_rels)
    }
}



//
// We store two kinds of entries in the repository:
//
// 1. Ready-made images of the block
// 2. WAL records, to be applied on top of the "previous" entry
//
// Some WAL records will initialize the page from scratch. For such records,
// the 'will_init' flag is set. They don't need the previous page image before
// applying. The 'will_init' flag is set for records containing a full-page image,
// and for records with the BKPBLOCK_WILL_INIT flag. These differ from PageImages
// stored directly in the cache entry in that you still need to run the WAL redo
// routine to generate the page image.
//
#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
enum PageEntry {
    Page(Bytes),
    WALRecord(WALRecord),
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub enum RelationSizeEntry {
    Size(u32),

    // Tombstone for a dropped relation.
    // TODO: Currently, we never drop relations. The parsing of relation drops in
    // COMMIT/ABORT records has not been implemented. We should also have a mechanism
    // to remove "orphaned" relfiles, if the compute node crashes before writing the
    // COMMIT/ABORT record.
    Unlink,
}

const fn relation_size_key(timelineid: ZTimelineId, rel: RelTag, lsn: Lsn) -> StorageKey {
    StorageKey {
        timeline: timelineid,
        buf_tag: BufferTag {
            rel: rel,
            blknum: u32::MAX,
        },
        lsn,
    }
}

//
// In addition to those per-page entries, the 'last_valid_lsn' and 'last_record_lsn'
// values are also persisted in the key-value store. They are stored with StorageKeys
// with STORAGE_SPECIAL_FORKNUM, and 'blknum' indicates which value it is. The
// rest of the key fields are zero. We use a StorageKey as the key for these too,
// so that whenever we iterate through keys in the repository, we can safely parse
// the key blob as StorageKey without checking for these special values first.
//
const fn timeline_metadata_key(timelineid: ZTimelineId) -> StorageKey {
    StorageKey {
        timeline: timelineid,
        buf_tag: BufferTag {
            rel: RelTag {
                forknum: pg_constants::ROCKSDB_SPECIAL_FORKNUM,
                spcnode: 0,
                dbnode: 0,
                relnode: 0,
            },
            blknum: 0,
        },
        lsn: Lsn(0),
    }
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct MetadataEntry {
    last_valid_lsn: Lsn,
    last_record_lsn: Lsn,
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}


///
/// Relation data file segment id throughout the Postgres cluster.
///
/// Every data file in Postgres is uniquely identified by 4 numbers:
/// - relation id / node (`relnode`)
/// - database id (`dbnode`)
/// - tablespace id (`spcnode`), in short this is a unique id of a separate
///   directory to store data files.
/// - forknumber (`forknum`) is used to split different kinds of data of the same relation
///   between some set of files (`relnode`, `relnode_fsm`, `relnode_vm`).
///
/// In native Postgres code `RelFileNode` structure and individual `ForkNumber` value
/// are used for the same purpose.
/// [See more related comments here](https:///github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/relfilenode.h#L57).
///
/// We use additional fork numbers to logically separate relational and
/// non-relational data inside pageserver key-value storage.
/// See, e.g., `ROCKSDB_SPECIAL_FORKNUM`.
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct RelTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
}

/// Display RelTag in the same format that's used in most PostgreSQL debug messages:
///
/// <spcnode>/<dbnode>/<relnode>[_fsm|_vm|_init]
///
impl fmt::Display for RelTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(forkname) = forknumber_to_name(self.forknum) {
            write!(
                f,
                "{}/{}/{}_{}",
                self.spcnode, self.dbnode, self.relnode, forkname
            )
        } else {
            write!(f, "{}/{}/{}", self.spcnode, self.dbnode, self.relnode)
        }
    }
}

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
/// This is used as a part of the key inside key-value storage (RocksDB currently).
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct WALRecord {
    pub lsn: Lsn, // LSN at the *end* of the record
    pub will_init: bool,
    pub rec: Bytes,
    // Remember the offset of main_data in rec,
    // so that we don't have to parse the record again.
    // If record has no main_data, this offset equals rec.len().
    pub main_data_offset: u32,
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///
#[cfg(test)]
mod tests {
    use super::*;
    use crate::walredo::{WalRedoError, WalRedoManager};
    use crate::PageServerConf;
    use bytes::BytesMut;
    use postgres_ffi::pg_constants;
    use std::fs;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelTag = RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    };

    /// Convenience function to create a BufferTag for testing.
    /// Helps to keeps the tests shorter.
    #[allow(non_snake_case)]
    fn TEST_BUF(blknum: u32) -> BufferTag {
        BufferTag {
            rel: TESTREL_A,
            blknum,
        }
    }

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(8192, 0);

        buf.freeze()
    }

    fn get_test_repo(test_name: &str) -> Result<Box<Repository>> {
        let repo_dir = PathBuf::from(format!("../tmp_check/test_{}", test_name));
        let _ = fs::remove_dir_all(&repo_dir);
        fs::create_dir_all(&repo_dir)?;

        let conf = PageServerConf {
            daemonize: false,
            interactive: false,
            gc_horizon: 64 * 1024 * 1024,
            gc_period: Duration::from_secs(10),
            listen_addr: "127.0.0.1:5430".parse().unwrap(),
            workdir: repo_dir,
            pg_distrib_dir: "".into(),
        };
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let storage = crate::rocksdb_storage::RocksStorage::create(conf)?;

        let walredo_mgr = TestRedoManager {};

        let repo = Repository::new(conf, Arc::new(storage), Arc::new(walredo_mgr));

        Ok(Box::new(repo))
    }

    /// Test get_relsize() and truncation.
    #[test]
    fn test_relsize() -> Result<()> {
        // get_timeline() with non-existent timeline id should fail
        //repo.get_timeline("11223344556677881122334455667788");

        // Create timeline to work on
        let repo = get_test_repo("test_relsize")?;
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        tline.init_valid_lsn(Lsn(1));
        tline.put_page_image(TEST_BUF(0), Lsn(2), TEST_IMG("foo blk 0 at 2"))?;
        tline.put_page_image(TEST_BUF(0), Lsn(2), TEST_IMG("foo blk 0 at 2"))?;
        tline.put_page_image(TEST_BUF(0), Lsn(3), TEST_IMG("foo blk 0 at 3"))?;
        tline.put_page_image(TEST_BUF(1), Lsn(4), TEST_IMG("foo blk 1 at 4"))?;
        tline.put_page_image(TEST_BUF(2), Lsn(5), TEST_IMG("foo blk 2 at 5"))?;

        tline.advance_last_valid_lsn(Lsn(5));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(1))?, false);
        assert!(tline.get_rel_size(TESTREL_A, Lsn(1)).is_err());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(2))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(2))?, 1);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(5))?, 3);

        // Check page contents at each LSN
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(2))?,
            TEST_IMG("foo blk 0 at 2")
        );

        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(3))?,
            TEST_IMG("foo blk 0 at 3")
        );

        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(4))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(1), Lsn(4))?,
            TEST_IMG("foo blk 1 at 4")
        );

        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(5))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(1), Lsn(5))?,
            TEST_IMG("foo blk 1 at 4")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(2), Lsn(5))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate last block
        tline.put_truncation(TESTREL_A, Lsn(6), 2)?;
        tline.advance_last_valid_lsn(Lsn(6));

        // Check reported size and contents after truncation
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(6))?, 2);
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(6))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(1), Lsn(6))?,
            TEST_IMG("foo blk 1 at 4")
        );

        // should still see the truncated block with older LSN
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(5))?, 3);
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(2), Lsn(5))?,
            TEST_IMG("foo blk 2 at 5")
        );

        Ok(())
    }

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    ///
    /// This isn't very interesting with the RocksDb implementation, as we don't pay
    /// any attention to Postgres segment boundaries there.
    #[test]
    fn test_large_rel() -> Result<()> {
        let repo = get_test_repo("test_large_rel")?;
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        tline.init_valid_lsn(Lsn(1));

        let mut lsn = 0;
        for i in 0..pg_constants::RELSEG_SIZE + 1 {
            let img = TEST_IMG(&format!("foo blk {} at {}", i, Lsn(lsn)));
            lsn += 1;
            tline.put_page_image(TEST_BUF(i as u32), Lsn(lsn), img)?;
        }
        tline.advance_last_valid_lsn(Lsn(lsn));

        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 1;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE)?;
        tline.advance_last_valid_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE
        );

        // Truncate another block
        lsn += 1;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE - 1)?;
        tline.advance_last_valid_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE - 1
        );

        Ok(())
    }

    // Mock WAL redo manager that doesn't do much
    struct TestRedoManager {}

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            tag: BufferTag,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<WALRecord>,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for rel {} blk {} to get to {}, with {} and {} records",
                tag.rel,
                tag.blknum,
                lsn,
                if base_img.is_some() {
                    "base image"
                } else {
                    "no base image"
                },
                records.len()
            );
            println!("{}", s);
            Ok(TEST_IMG(&s))
        }
    }
}


///
/// Iterator for `block_iter_backwards. Returns all page versions of a given block, in
/// reverse LSN order.
///
struct BlockBackwardsIter<'a> {
    storage: &'a dyn Storage,

    buf_tag: BufferTag,

    current_iter: Box<dyn StorageIterator + 'a>,
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}


impl<'a> Iterator for BlockBackwardsIter<'a>
{
    type Item = Result<(StorageKey, Vec<u8>)>;

    fn next(&mut self) -> Option<<Self as std::iter::Iterator>::Item> {
        self.next_result().transpose()
    }
}

impl<'a> BlockBackwardsIter<'a> {
    fn next_result(&mut self) -> Result<Option<(StorageKey, Vec<u8>)>> {
        loop {
            if let Some(result) = self.current_iter.next() {
                return Ok(Some(result));
            }

            // Out of entries. Check ancestor, if any.
            if let Some(ancestor_timeline) = self.ancestor_timeline {
                let ancestor_iter = self.storage.block_iter_backwards(
                    ancestor_timeline,
                    self.buf_tag,
                    self.ancestor_lsn
                )?;

                // Load ancestor metadata. (We don't actually need it yet, only if
                // we need to follow to the grandparent timeline)
                let v = self.storage
                    .get(&timeline_metadata_key(ancestor_timeline))
                    .with_context(|| "timeline not found in repository")?;
                let ancestor_metadata = MetadataEntry::des(&v)?;

                self.ancestor_timeline = ancestor_metadata.ancestor_timeline;
                self.ancestor_lsn = ancestor_metadata.ancestor_lsn;
                self.current_iter = ancestor_iter;
            } else {
                return Ok(None);
            }
        }
    }
}
