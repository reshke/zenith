//! Everything to deal with WAL -- reading and writing to disk.

use anyhow::{anyhow, Result};

use lazy_static::lazy_static;
use postgres_ffi::xlog_utils::{find_end_of_wal, XLogSegNo, PG_TLI};
use std::cmp::min;

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use tracing::*;

use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTenantTimelineId;

use crate::safekeeper::SafeKeeperState;

use crate::SafeKeeperConf;
use postgres_ffi::xlog_utils::{XLogFileName, XLOG_BLCKSZ};

use postgres_ffi::waldecoder::WalStreamDecoder;

use zenith_metrics::{
    register_gauge_vec, register_histogram_vec, Gauge, GaugeVec, Histogram, HistogramVec,
    DISK_WRITE_SECONDS_BUCKETS,
};
use zenith_utils::zid::ZTimelineId;

lazy_static! {
    // The prometheus crate does not support u64 yet, i64 only (see `IntGauge`).
    // i64 is faster than f64, so update to u64 when available.
    static ref FLUSH_LSN_GAUGE: GaugeVec = register_gauge_vec!(
        "safekeeper_flush_lsn",
        "Current flush_lsn, grouped by timeline",
        &["ztli"]
    )
    .expect("Failed to register safekeeper_flush_lsn gauge vec");
    static ref COMMIT_LSN_GAUGE: GaugeVec = register_gauge_vec!(
        "safekeeper_commit_lsn",
        "Current commit_lsn (not necessarily persisted to disk), grouped by timeline",
        &["ztli"]
    )
    .expect("Failed to register safekeeper_commit_lsn gauge vec");
    static ref WRITE_WAL_BYTES: HistogramVec = register_histogram_vec!(
        "safekeeper_write_wal_bytes",
        "Bytes written to WAL in a single request, grouped by timeline",
        &["timeline_id"],
        vec![1.0, 10.0, 100.0, 1024.0, 8192.0, 128.0 * 1024.0, 1024.0 * 1024.0, 10.0 * 1024.0 * 1024.0]
    )
    .expect("Failed to register safekeeper_write_wal_bytes histogram vec");
    static ref WRITE_WAL_SECONDS: HistogramVec = register_histogram_vec!(
        "safekeeper_write_wal_seconds",
        "Seconds spent writing and syncing WAL to a disk in a single request, grouped by timeline",
        &["timeline_id"],
        DISK_WRITE_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_write_wal_seconds histogram vec");
}

struct WalStorageMetrics {
    flush_lsn: Gauge,
    write_wal_bytes: Histogram,
    write_wal_seconds: Histogram,
}

impl WalStorageMetrics {
    fn new(ztli: ZTimelineId) -> Self {
        let ztli_str = format!("{}", ztli);
        Self {
            flush_lsn: FLUSH_LSN_GAUGE.with_label_values(&[&ztli_str]),
            write_wal_bytes: WRITE_WAL_BYTES.with_label_values(&[&ztli_str]),
            write_wal_seconds: WRITE_WAL_SECONDS.with_label_values(&[&ztli_str]),
        }
    }
}

// pub trait Storage {
//     fn flush_lsn(&self) -> Result<Lsn>;

//     /// Write piece of wal in buf to disk and sync it.
//     fn write_wal(&mut self, server: &ServerInfo, startpos: Lsn, buf: &[u8]) -> Result<()>;
//     // Truncate WAL at specified LSN
//     fn truncate_wal(&mut self, s: &ServerInfo, endpos: Lsn) -> Result<()>;
// }

pub struct PhysicalStorage {
    metrics: WalStorageMetrics,
    zttid: ZTenantTimelineId,
    // save timeline dir to avoid reconstructing it every time
    dir: PathBuf,
    conf: SafeKeeperConf,

    /// filled upon initialization
    wal_seg_size: Option<usize>,

    // Relationship of lsns:
    // `write_lsn` >= `record_lsn` >= `flush_lsn`

    // Written to disk, but possibly still in the cache and not fully persisted.
    // Also can be ahead of record_lsn, if happen to be in the middle of a WAL record.
    write_lsn: Lsn,
    // The LSN of the last WAL record written to disk. Still can be not fully flushed.
    record_lsn: Lsn,
    // The LSN of the last WAL record flushed to disk.
    flush_lsn: Lsn,

    decoder: WalStreamDecoder,
}

impl PhysicalStorage {
    pub fn new(zttid: &ZTenantTimelineId, conf: &SafeKeeperConf) -> PhysicalStorage {
        let dir = conf.timeline_dir(zttid);
        PhysicalStorage {
            metrics: WalStorageMetrics::new(zttid.timeline_id),
            zttid: *zttid,
            dir,
            conf: conf.clone(),
            wal_seg_size: None,
            write_lsn: Lsn(0),
            record_lsn: Lsn(0),
            flush_lsn: Lsn(0),
            decoder: WalStreamDecoder::new(Lsn(0)),
        }
    }

    pub fn flush_lsn(&self) -> Lsn {
        self.flush_lsn
    }

    pub fn init_storage(&mut self, state: &SafeKeeperState) -> Result<()> {
        if state.server.wal_seg_size == 0 {
            // state is not initialized yet
            return Ok(());
        }

        if let Some(wal_seg_size) = self.wal_seg_size {
            // physical storage is already initialized
            assert_eq!(wal_seg_size, state.server.wal_seg_size as usize);
            return Ok(());
        }

        // initialize physical storage
        let wal_seg_size = state.server.wal_seg_size as usize;
        self.wal_seg_size = Some(wal_seg_size);

        self.write_lsn =
            Lsn(find_end_of_wal(&self.dir, wal_seg_size, true, state.wal_start_lsn)?.0);

        // TODO: call fsync() here to be 100% sure?
        self.flush_lsn = self.write_lsn;

        info!(
            "initialized storage for timeline {}, flush_lsn={}, commit_lsn={}, truncate_lsn={}",
            self.zttid.timeline_id, self.flush_lsn, state.commit_lsn, state.truncate_lsn,
        );
        if self.flush_lsn < state.commit_lsn || self.flush_lsn < state.truncate_lsn {
            warn!("timeline {} potential data loss: flush_lsn by find_end_of_wal is less than either commit_lsn or truncate_lsn from control file", self.zttid.timeline_id);
        }

        Ok(())
    }

    /// Helper returning full path to WAL segment file and its .partial brother.
    pub fn wal_file_paths(&self, segno: XLogSegNo) -> Result<(PathBuf, PathBuf)> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
        let wal_file_path = self.dir.join(wal_file_name.clone());
        let wal_file_partial_path = self.dir.join(wal_file_name + ".partial");
        Ok((wal_file_path, wal_file_partial_path))
    }

    // write and flush WAL to disk
    // TODO: don't flush
    fn write_to_disk(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        let mut bytes_left: usize = buf.len();
        let mut bytes_written: usize = 0;
        let mut partial;
        let mut start_pos = startpos;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

        /* Extract WAL location for this block */
        let mut xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

        while bytes_left != 0 {
            let bytes_to_write;

            /*
             * If crossing a WAL boundary, only write up until we reach wal
             * segment size.
             */
            if xlogoff + bytes_left > wal_seg_size {
                bytes_to_write = wal_seg_size - xlogoff;
            } else {
                bytes_to_write = bytes_left;
            }

            /* Open file */
            let segno = start_pos.segment_number(wal_seg_size);
            let (wal_file_path, wal_file_partial_path) = self.wal_file_paths(segno)?;
            {
                let mut wal_file: File;
                /* Try to open already completed segment */
                if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                    wal_file = file;
                    partial = false;
                } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path)
                {
                    /* Try to open existed partial file */
                    wal_file = file;
                    partial = true;
                } else {
                    /* Create and fill new partial file */
                    partial = true;
                    match OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&wal_file_partial_path)
                    {
                        Ok(mut file) => {
                            for _ in 0..(wal_seg_size / XLOG_BLCKSZ) {
                                file.write_all(ZERO_BLOCK)?;
                            }
                            wal_file = file;
                        }
                        Err(e) => {
                            error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                            return Err(e.into());
                        }
                    }
                }
                wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
                wal_file.write_all(&buf[bytes_written..(bytes_written + bytes_to_write)])?;

                // Flush file, if not said otherwise
                if !self.conf.no_sync {
                    wal_file.sync_all()?;
                }
            }
            /* Write was successful, advance our position */
            bytes_written += bytes_to_write;
            bytes_left -= bytes_to_write;
            start_pos += bytes_to_write as u64;
            xlogoff += bytes_to_write;

            /* Did we reach the end of a WAL segment? */
            if start_pos.segment_offset(wal_seg_size) == 0 {
                xlogoff = 0;
                if partial {
                    fs::rename(&wal_file_partial_path, &wal_file_path)?;
                }
            }
        }
        Ok(())
    }

    pub fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        {
            let _timer = self.metrics.write_wal_seconds.start_timer();
            self.write_to_disk(startpos, buf)?;
        }

        self.write_lsn = startpos + buf.len() as u64;

        self.metrics.write_wal_bytes.observe(buf.len() as f64);

        // figure out last record's end lsn for reporting (if we got the
        // whole record)
        if self.decoder.available() != startpos {
            info!(
                "restart decoder from {} to {}",
                self.decoder.available(),
                startpos,
            );
            self.decoder = WalStreamDecoder::new(startpos);
        }
        self.decoder.feed_bytes(buf);
        loop {
            match self.decoder.poll_decode()? {
                None => break, // no full record yet
                Some((lsn, _rec)) => {
                    self.record_lsn = lsn;
                }
            }
        }

        self.flush_lsn = self.record_lsn;

        Ok(())
    }

    pub fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        // TODO: cross check divergence point

        // streaming must not create a hole
        assert!(self.flush_lsn == Lsn(0) || self.flush_lsn >= end_pos);

        // truncate obsolete part of WAL
        if self.flush_lsn == Lsn(0) {
            // TODO: truncate something?
            return Ok(());
        }

        self.flush_lsn = end_pos;
        self.metrics.flush_lsn.set(u64::from(self.flush_lsn) as f64);

        let partial;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

        /* Extract WAL location for this block */
        let mut xlogoff = end_pos.segment_offset(wal_seg_size) as usize;

        /* Open file */
        let mut segno = end_pos.segment_number(wal_seg_size);
        let (wal_file_path, wal_file_partial_path) = self.wal_file_paths(segno)?;
        {
            let mut wal_file: File;
            /* Try to open already completed segment */
            if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                wal_file = file;
                partial = false;
            } else {
                wal_file = OpenOptions::new()
                    .write(true)
                    .open(&wal_file_partial_path)?;
                partial = true;
            }
            wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
            while xlogoff < wal_seg_size {
                let bytes_to_write = min(XLOG_BLCKSZ, wal_seg_size - xlogoff);
                wal_file.write_all(&ZERO_BLOCK[0..bytes_to_write])?;
                xlogoff += bytes_to_write;
            }
            // Flush file, if not said otherwise
            if !self.conf.no_sync {
                wal_file.sync_all()?;
            }
        }
        if !partial {
            // Make segment partial once again
            fs::rename(&wal_file_path, &wal_file_partial_path)?;
        }
        // Remove all subsequent segments
        loop {
            segno += 1;
            let (wal_file_path, wal_file_partial_path) = self.wal_file_paths(segno)?;
            // TODO: better use fs::try_exists which is currenty avaialble only in nightly build
            if wal_file_path.exists() {
                fs::remove_file(&wal_file_path)?;
            } else if wal_file_partial_path.exists() {
                fs::remove_file(&wal_file_partial_path)?;
            } else {
                break;
            }
        }
        Ok(())
    }
}
