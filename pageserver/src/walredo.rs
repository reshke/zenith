//!
//! WAL redo. This service runs PostgreSQL in a special wal_redo mode
//! to apply given WAL records over an old page image and return new
//! page image.
//!
//! We rely on Postgres to perform WAL redo for us. We launch a
//! postgres process in special "wal redo" mode that's similar to
//! single-user mode. We then pass the previous page image, if any,
//! and all the WAL records we want to apply, to the postgres
//! process. Then we get the page image back. Communication with the
//! postgres process happens via stdin/stdout
//!
//! See src/backend/tcop/zenith_wal_redo.c for the other side of
//! this communication.
//!
//! The Postgres process is assumed to be secure against malicious WAL
//! records. It achieves it by dropping privileges before replaying
//! any WAL records, so that even if an attacker hijacks the Postgres
//! process, he cannot escape out of it.
//!
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use log::*;
use nix::poll::*;
use serde::Serialize;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::Stdio;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use zenith_metrics::{register_histogram, register_int_counter, Histogram, IntCounter};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::nonblock::set_nonblock;
use zenith_utils::zid::ZTenantId;

use crate::config::PageServerConf;
use crate::relish::*;
use crate::repository::ZenithWalRecord;
use postgres_ffi::nonrelfile_utils::mx_offset_to_flags_bitshift;
use postgres_ffi::nonrelfile_utils::mx_offset_to_flags_offset;
use postgres_ffi::nonrelfile_utils::mx_offset_to_member_offset;
use postgres_ffi::nonrelfile_utils::transaction_id_set_status;
use postgres_ffi::pg_constants;

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

///
/// WAL Redo Manager is responsible for replaying WAL records.
///
/// Callers use the WAL redo manager through this abstract interface,
/// which makes it easy to mock it in tests.
pub trait WalRedoManager: Send + Sync {
    /// Apply some WAL records.
    ///
    /// The caller passes an old page image, and WAL records that should be
    /// applied over it. The return value is a new page image, after applying
    /// the reords.
    fn request_redo(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<(Lsn, ZenithWalRecord)>,
    ) -> Result<Bytes, WalRedoError>;
}

///
/// A dummy WAL Redo Manager implementation that doesn't allow replaying
/// anything. Currently used during bootstrapping (zenith init), to create
/// a Repository object without launching the real WAL redo process.
///
pub struct DummyRedoManager {}
impl crate::walredo::WalRedoManager for DummyRedoManager {
    fn request_redo(
        &self,
        _rel: RelishTag,
        _blknum: u32,
        _lsn: Lsn,
        _base_img: Option<Bytes>,
        _records: Vec<(Lsn, ZenithWalRecord)>,
    ) -> Result<Bytes, WalRedoError> {
        Err(WalRedoError::InvalidState)
    }
}

static TIMEOUT: Duration = Duration::from_secs(20);

// Metrics collected on WAL redo operations
//
// We collect the time spent in actual WAL redo ('redo'), and time waiting
// for access to the postgres process ('wait') since there is only one for
// each tenant.
lazy_static! {
    static ref WAL_REDO_TIME: Histogram =
        register_histogram!("pageserver_wal_redo_time", "Time spent on WAL redo")
            .expect("failed to define a metric");
    static ref WAL_REDO_WAIT_TIME: Histogram = register_histogram!(
        "pageserver_wal_redo_wait_time",
        "Time spent waiting for access to the WAL redo process"
    )
    .expect("failed to define a metric");
    static ref WAL_REDO_RECORD_COUNTER: IntCounter = register_int_counter!(
        "pageserver_wal_records_replayed",
        "Number of WAL records replayed"
    )
    .unwrap();
}

///
/// This is the real implementation that uses a Postgres process to
/// perform WAL replay. Only one thread can use the processs at a time,
/// that is controlled by the Mutex. In the future, we might want to
/// launch a pool of processes to allow concurrent replay of multiple
/// records.
///
pub struct PostgresRedoManager {
    tenantid: ZTenantId,
    conf: &'static PageServerConf,

    process: Mutex<Option<PostgresRedoProcess>>,
}

/// Can this request be served by zenith redo funcitons
/// or we need to pass it to wal-redo postgres process?
fn can_apply_in_zenith(rec: &ZenithWalRecord) -> bool {
    // Currently, we don't have bespoken Rust code to replay any
    // Postgres WAL records. But everything else is handled in zenith.
    #[allow(clippy::match_like_matches_macro)]
    match rec {
        ZenithWalRecord::Postgres {
            will_init: _,
            rec: _,
        } => false,
        _ => true,
    }
}

fn check_forknum(rel: &RelishTag, expected_forknum: u8) -> bool {
    if let RelishTag::Relation(RelTag {
        forknum,
        spcnode: _,
        dbnode: _,
        relnode: _,
    }) = rel
    {
        *forknum == expected_forknum
    } else {
        false
    }
}

fn check_slru_segno(rel: &RelishTag, expected_slru: SlruKind, expected_segno: u32) -> bool {
    if let RelishTag::Slru { slru, segno } = rel {
        *slru == expected_slru && *segno == expected_segno
    } else {
        false
    }
}

/// An error happened in WAL redo
#[derive(Debug, thiserror::Error)]
pub enum WalRedoError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("cannot perform WAL redo now")]
    InvalidState,
    #[error("cannot perform WAL redo for this request")]
    InvalidRequest,
}

///
/// Public interface of WAL redo manager
///
impl WalRedoManager for PostgresRedoManager {
    ///
    /// Request the WAL redo manager to apply some WAL records
    ///
    /// The WAL redo is handled by a separate thread, so this just sends a request
    /// to the thread and waits for response.
    ///
    fn request_redo(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<(Lsn, ZenithWalRecord)>,
    ) -> Result<Bytes, WalRedoError> {
        if records.is_empty() {
            error!("invalid WAL redo request with no records");
            return Err(WalRedoError::InvalidRequest);
        }

        let mut img: Option<Bytes> = base_img;
        let mut batch_zenith = can_apply_in_zenith(&records[0].1);
        let mut batch_start = 0;
        for i in 1..records.len() {
            let rec_zenith = can_apply_in_zenith(&records[i].1);

            if rec_zenith != batch_zenith {
                let result = if batch_zenith {
                    self.apply_batch_zenith(rel, blknum, lsn, img, &records[batch_start..i])
                } else {
                    self.apply_batch_postgres(rel, blknum, lsn, img, &records[batch_start..i])
                };
                img = Some(result?);

                batch_zenith = rec_zenith;
                batch_start = i;
            }
        }
        // last batch
        if batch_zenith {
            self.apply_batch_zenith(rel, blknum, lsn, img, &records[batch_start..])
        } else {
            self.apply_batch_postgres(rel, blknum, lsn, img, &records[batch_start..])
        }
    }
}

impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    pub fn new(conf: &'static PageServerConf, tenantid: ZTenantId) -> PostgresRedoManager {
        // The actual process is launched lazily, on first request.
        PostgresRedoManager {
            tenantid,
            conf,
            process: Mutex::new(None),
        }
    }

    ///
    /// Process one request for WAL redo using wal-redo postgres
    ///
    fn apply_batch_postgres(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: &[(Lsn, ZenithWalRecord)],
    ) -> Result<Bytes, WalRedoError> {
        let start_time = Instant::now();

        let apply_result: Result<Bytes, Error>;

        let mut process_guard = self.process.lock().unwrap();
        let lock_time = Instant::now();

        // launch the WAL redo process on first use
        if process_guard.is_none() {
            let p = PostgresRedoProcess::launch(self.conf, &self.tenantid)?;
            *process_guard = Some(p);
        }
        let process = process_guard.as_mut().unwrap();

        WAL_REDO_WAIT_TIME.observe(lock_time.duration_since(start_time).as_secs_f64());

        let result = if let RelishTag::Relation(rel) = rel {
            // Relational WAL records are applied using wal-redo-postgres
            let buf_tag = BufferTag { rel, blknum };
            apply_result = process.apply_wal_records(buf_tag, base_img, records);

            apply_result.map_err(WalRedoError::IoError)
        } else {
            error!("unexpected non-relation relish: {:?}", rel);
            Err(WalRedoError::InvalidRequest)
        };

        let end_time = Instant::now();
        let duration = end_time.duration_since(lock_time);
        WAL_REDO_TIME.observe(duration.as_secs_f64());
        debug!(
            "postgres applied {} WAL records in {} us to reconstruct page image at LSN {}",
            records.len(),
            duration.as_micros(),
            lsn
        );

        // If something went wrong, don't try to reuse the process. Kill it, and
        // next request will launch a new one.
        if result.is_err() {
            let process = process_guard.take().unwrap();
            process.kill();
        }
        result
    }

    ///
    /// Process a batch of WAL records using bespoken Zenith code.
    ///
    fn apply_batch_zenith(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: &[(Lsn, ZenithWalRecord)],
    ) -> Result<Bytes, WalRedoError> {
        let start_time = Instant::now();

        let mut page = BytesMut::new();
        if let Some(fpi) = base_img {
            // If full-page image is provided, then use it...
            page.extend_from_slice(&fpi[..]);
        } else {
            // All the current WAL record types that we can handle require a base image.
            error!("invalid zenith WAL redo request with no base image");
            return Err(WalRedoError::InvalidRequest);
        }

        // Apply all the WAL records in the batch
        for (record_lsn, record) in records.iter() {
            self.apply_record_zenith(rel, blknum, &mut page, *record_lsn, record)?;
        }
        // Success!
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        WAL_REDO_TIME.observe(duration.as_secs_f64());

        debug!(
            "zenith applied {} WAL records in {} ms to reconstruct page image at LSN {}",
            records.len(),
            duration.as_micros(),
            lsn
        );

        Ok(page.freeze())
    }

    fn apply_record_zenith(
        &self,
        rel: RelishTag,
        blknum: u32,
        page: &mut BytesMut,
        _record_lsn: Lsn,
        record: &ZenithWalRecord,
    ) -> Result<(), WalRedoError> {
        match record {
            ZenithWalRecord::Postgres {
                will_init: _,
                rec: _,
            } => panic!("tried to pass postgres wal record to zenith WAL redo"),
            ZenithWalRecord::ClearVisibilityMapFlags {
                new_heap_blkno,
                old_heap_blkno,
                flags,
            } => {
                // sanity check that this is modifying the correct relish
                assert!(
                    check_forknum(&rel, pg_constants::VISIBILITYMAP_FORKNUM),
                    "ClearVisibilityMapFlags record on unexpected rel {:?}",
                    rel
                );
                if let Some(heap_blkno) = *new_heap_blkno {
                    // Calculate the VM block and offset that corresponds to the heap block.
                    let map_block = pg_constants::HEAPBLK_TO_MAPBLOCK(heap_blkno);
                    let map_byte = pg_constants::HEAPBLK_TO_MAPBYTE(heap_blkno);
                    let map_offset = pg_constants::HEAPBLK_TO_OFFSET(heap_blkno);

                    // Check that we're modifying the correct VM block.
                    assert!(map_block == blknum);

                    // equivalent to PageGetContents(page)
                    let map = &mut page[pg_constants::MAXALIGN_SIZE_OF_PAGE_HEADER_DATA..];

                    map[map_byte as usize] &= !(flags << map_offset);
                }

                // Repeat for 'old_heap_blkno', if any
                if let Some(heap_blkno) = *old_heap_blkno {
                    let map_block = pg_constants::HEAPBLK_TO_MAPBLOCK(heap_blkno);
                    let map_byte = pg_constants::HEAPBLK_TO_MAPBYTE(heap_blkno);
                    let map_offset = pg_constants::HEAPBLK_TO_OFFSET(heap_blkno);

                    assert!(map_block == blknum);

                    let map = &mut page[pg_constants::MAXALIGN_SIZE_OF_PAGE_HEADER_DATA..];

                    map[map_byte as usize] &= !(flags << map_offset);
                }
            }
            // Non-relational WAL records are handled here, with custom code that has the
            // same effects as the corresponding Postgres WAL redo function.
            ZenithWalRecord::ClogSetCommitted { xids } => {
                for &xid in xids {
                    let pageno = xid as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                    let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

                    // Check that we're modifying the correct CLOG block.
                    assert!(
                        check_slru_segno(&rel, SlruKind::Clog, expected_segno),
                        "ClogSetCommitted record for XID {} with unexpected rel {:?}",
                        xid,
                        rel
                    );
                    assert!(blknum == expected_blknum);

                    transaction_id_set_status(
                        xid,
                        pg_constants::TRANSACTION_STATUS_COMMITTED,
                        page,
                    );
                }
            }
            ZenithWalRecord::ClogSetAborted { xids } => {
                for &xid in xids {
                    let pageno = xid as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                    let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

                    // Check that we're modifying the correct CLOG block.
                    assert!(
                        check_slru_segno(&rel, SlruKind::Clog, expected_segno),
                        "ClogSetCommitted record for XID {} with unexpected rel {:?}",
                        xid,
                        rel
                    );
                    assert!(blknum == expected_blknum);

                    transaction_id_set_status(xid, pg_constants::TRANSACTION_STATUS_ABORTED, page);
                }
            }
            ZenithWalRecord::MultixactOffsetCreate { mid, moff } => {
                // Compute the block and offset to modify.
                // See RecordNewMultiXact in PostgreSQL sources.
                let pageno = mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
                let entryno = mid % pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
                let offset = (entryno * 4) as usize;

                // Check that we're modifying the correct multixact-offsets block.
                let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                assert!(
                    check_slru_segno(&rel, SlruKind::MultiXactOffsets, expected_segno),
                    "MultiXactOffsetsCreate record for multi-xid {} with unexpected rel {:?}",
                    mid,
                    rel
                );
                assert!(blknum == expected_blknum);

                LittleEndian::write_u32(&mut page[offset..offset + 4], *moff);
            }
            ZenithWalRecord::MultixactMembersCreate { moff, members } => {
                for (i, member) in members.iter().enumerate() {
                    let offset = moff + i as u32;

                    // Compute the block and offset to modify.
                    // See RecordNewMultiXact in PostgreSQL sources.
                    let pageno = offset / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
                    let memberoff = mx_offset_to_member_offset(offset);
                    let flagsoff = mx_offset_to_flags_offset(offset);
                    let bshift = mx_offset_to_flags_bitshift(offset);

                    // Check that we're modifying the correct multixact-members block.
                    let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    assert!(
                        check_slru_segno(&rel, SlruKind::MultiXactMembers, expected_segno),
                        "MultiXactMembersCreate record at offset {} with unexpected rel {:?}",
                        moff,
                        rel
                    );
                    assert!(blknum == expected_blknum);

                    let mut flagsval = LittleEndian::read_u32(&page[flagsoff..flagsoff + 4]);
                    flagsval &= !(((1 << pg_constants::MXACT_MEMBER_BITS_PER_XACT) - 1) << bshift);
                    flagsval |= member.status << bshift;
                    LittleEndian::write_u32(&mut page[flagsoff..flagsoff + 4], flagsval);
                    LittleEndian::write_u32(&mut page[memberoff..memberoff + 4], member.xid);
                }
            }
        }

        Ok(())
    }
}

///
/// Handle to the Postgres WAL redo process
///
struct PostgresRedoProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

impl PostgresRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    fn launch(conf: &PageServerConf, tenantid: &ZTenantId) -> Result<PostgresRedoProcess, Error> {
        // FIXME: We need a dummy Postgres cluster to run the process in. Currently, we
        // just create one with constant name. That fails if you try to launch more than
        // one WAL redo manager concurrently.
        let datadir = conf.tenant_path(tenantid).join("wal-redo-datadir");

        // Create empty data directory for wal-redo postgres, deleting old one first.
        if datadir.exists() {
            info!("directory {:?} exists, removing", &datadir);
            if let Err(e) = fs::remove_dir_all(&datadir) {
                error!("could not remove old wal-redo-datadir: {:#}", e);
            }
        }
        info!("running initdb in {:?}", datadir.display());
        let initdb = Command::new(conf.pg_bin_dir().join("initdb"))
            .args(&["-D", datadir.to_str().unwrap()])
            .arg("--data-checksums")
            .arg("-N")
            .env_clear()
            .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .output()
            .expect("failed to execute initdb");

        if !initdb.status.success() {
            panic!(
                "initdb failed: {}\nstderr:\n{}",
                std::str::from_utf8(&initdb.stdout).unwrap(),
                std::str::from_utf8(&initdb.stderr).unwrap()
            );
        } else {
            // Limit shared cache for wal-redo-postres
            let mut config = OpenOptions::new()
                .append(true)
                .open(PathBuf::from(&datadir).join("postgresql.conf"))?;
            config.write_all(b"shared_buffers=128kB\n")?;
            config.write_all(b"fsync=off\n")?;
            config.write_all(b"shared_preload_libraries=zenith\n")?;
            config.write_all(b"zenith.wal_redo=on\n")?;
        }
        // Start postgres itself
        let mut child = Command::new(conf.pg_bin_dir().join("postgres"))
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env_clear()
            .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("PGDATA", &datadir)
            .spawn()
            .expect("postgres --wal-redo command failed to start");

        info!(
            "launched WAL redo postgres process on {:?}",
            datadir.display()
        );

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        set_nonblock(stdin.as_raw_fd())?;
        set_nonblock(stdout.as_raw_fd())?;
        set_nonblock(stderr.as_raw_fd())?;

        Ok(PostgresRedoProcess {
            child,
            stdin,
            stdout,
            stderr,
        })
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        if let Ok(exit_status) = self.child.wait() {
            error!("wal-redo-postgres exited with code {}", exit_status);
        }
        drop(self);
    }

    //
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    //
    fn apply_wal_records(
        &mut self,
        tag: BufferTag,
        base_img: Option<Bytes>,
        records: &[(Lsn, ZenithWalRecord)],
    ) -> Result<Bytes, std::io::Error> {
        // Serialize all the messages to send the WAL redo process first.
        //
        // This could be problematic if there are millions of records to replay,
        // but in practice the number of records is usually so small that it doesn't
        // matter, and it's better to keep this code simple.
        let mut writebuf: Vec<u8> = Vec::new();
        build_begin_redo_for_block_msg(tag, &mut writebuf);
        if let Some(img) = base_img {
            build_push_page_msg(tag, &img, &mut writebuf);
        }
        for (lsn, rec) in records.iter() {
            if let ZenithWalRecord::Postgres {
                will_init: _,
                rec: postgres_rec,
            } = rec
            {
                build_apply_record_msg(*lsn, postgres_rec, &mut writebuf);
            } else {
                panic!("tried to pass zenith wal record to postgres WAL redo");
            }
        }
        build_get_page_msg(tag, &mut writebuf);
        WAL_REDO_RECORD_COUNTER.inc_by(records.len() as u64);

        // The input is now in 'writebuf'. Do a blind write first, writing as much as
        // we can, before calling poll(). That skips one call to poll() if the stdin is
        // already available for writing, which it almost certainly is because the
        // process is idle.
        let mut nwrite = self.stdin.write(&writebuf)?;

        // We expect the WAL redo process to respond with an 8k page image. We read it
        // into this buffer.
        let mut resultbuf = vec![0; pg_constants::BLCKSZ.into()];
        let mut nresult: usize = 0; // # of bytes read into 'resultbuf' so far

        // Prepare for calling poll()
        let mut pollfds = [
            PollFd::new(self.stdout.as_raw_fd(), PollFlags::POLLIN),
            PollFd::new(self.stderr.as_raw_fd(), PollFlags::POLLIN),
            PollFd::new(self.stdin.as_raw_fd(), PollFlags::POLLOUT),
        ];

        // We do three things simultaneously: send the old base image and WAL records to
        // the child process's stdin, read the result from child's stdout, and forward any logging
        // information that the child writes to its stderr to the page server's log.
        while nresult < pg_constants::BLCKSZ.into() {
            // If we have more data to write, wake up if 'stdin' becomes writeable or
            // we have data to read. Otherwise only wake up if there's data to read.
            let nfds = if nwrite < writebuf.len() { 3 } else { 2 };
            let n = nix::poll::poll(&mut pollfds[0..nfds], TIMEOUT.as_millis() as i32)?;

            if n == 0 {
                return Err(Error::new(ErrorKind::Other, "WAL redo timed out"));
            }

            // If we have some messages in stderr, forward them to the log.
            let err_revents = pollfds[1].revents().unwrap();
            if err_revents & (PollFlags::POLLERR | PollFlags::POLLIN) != PollFlags::empty() {
                let mut errbuf: [u8; 16384] = [0; 16384];
                let n = self.stderr.read(&mut errbuf)?;

                // The message might not be split correctly into lines here. But this is
                // good enough, the important thing is to get the message to the log.
                if n > 0 {
                    error!(
                        "wal-redo-postgres: {}",
                        String::from_utf8_lossy(&errbuf[0..n])
                    );

                    // To make sure we capture all log from the process if it fails, keep
                    // reading from the stderr, before checking the stdout.
                    continue;
                }
            } else if err_revents.contains(PollFlags::POLLHUP) {
                return Err(Error::new(
                    ErrorKind::BrokenPipe,
                    "WAL redo process closed its stderr unexpectedly",
                ));
            }

            // If we have more data to write and 'stdin' is writeable, do write.
            if nwrite < writebuf.len() {
                let in_revents = pollfds[2].revents().unwrap();
                if in_revents & (PollFlags::POLLERR | PollFlags::POLLOUT) != PollFlags::empty() {
                    nwrite += self.stdin.write(&writebuf[nwrite..])?;
                } else if in_revents.contains(PollFlags::POLLHUP) {
                    // We still have more data to write, but the process closed the pipe.
                    return Err(Error::new(
                        ErrorKind::BrokenPipe,
                        "WAL redo process closed its stdin unexpectedly",
                    ));
                }
            }

            // If we have some data in stdout, read it to the result buffer.
            let out_revents = pollfds[0].revents().unwrap();
            if out_revents & (PollFlags::POLLERR | PollFlags::POLLIN) != PollFlags::empty() {
                nresult += self.stdout.read(&mut resultbuf[nresult..])?;
            } else if out_revents.contains(PollFlags::POLLHUP) {
                return Err(Error::new(
                    ErrorKind::BrokenPipe,
                    "WAL redo process closed its stdout unexpectedly",
                ));
            }
        }

        Ok(Bytes::from(resultbuf))
    }
}

// Functions for constructing messages to send to the postgres WAL redo
// process. See vendor/postgres/src/backend/tcop/zenith_wal_redo.c for
// explanation of the protocol.

fn build_begin_redo_for_block_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'B');
    buf.put_u32(len as u32);

    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}

fn build_push_page_msg(tag: BufferTag, base_img: &[u8], buf: &mut Vec<u8>) {
    assert!(base_img.len() == 8192);

    let len = 4 + 1 + 4 * 4 + base_img.len();

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
    buf.put(base_img);
}

fn build_apply_record_msg(endlsn: Lsn, rec: &[u8], buf: &mut Vec<u8>) {
    let len = 4 + 8 + rec.len();

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn.0);
    buf.put(rec);
}

fn build_get_page_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}
