//! An ImageLayer represents an image or a snapshot of a segment at one particular LSN.
//! It is stored in a file on disk.
//!
//! On disk, the image files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each image layer file is named like this:
//!
//! Note that segno is
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_5_000000000169C348
//!
//! An image file is constructed using the 'bookfile' crate.
//!
//! Only metadata is loaded into memory by the load function.
//! When images are needed, they are read directly from disk.
//!
//! For blocky relishes, the images are stored in BLOCKY_IMAGES_CHAPTER.
//! All the images are required to be BLOCK_SIZE, which allows for random access.
//!
//! For non-blocky relishes, the image can be found in NONBLOCKY_IMAGE_CHAPTER.
//!
use crate::config::PageServerConf;
use crate::layered_repository::filename::{ImageFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, SegmentBlk, SegmentRange, SegmentTag,
};
use crate::layered_repository::RELISH_SEG_SIZE;
use crate::virtual_file::VirtualFile;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter, ChapterWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith segment image file
pub const IMAGE_FILE_MAGIC: u32 = 0x5A616E01 + 1;

/// Contains each block in block # order
const BLOCKY_IMAGES_CHAPTER: u64 = 1;
const NONBLOCKY_IMAGE_CHAPTER: u64 = 2;

/// Contains the [`Summary`] struct
const SUMMARY_CHAPTER: u64 = 3;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    seg: SegmentTag,

    lsn: Lsn,
}

impl From<&ImageLayer> for Summary {
    fn from(layer: &ImageLayer) -> Self {
        Self {
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            seg: layer.seg,

            lsn: layer.lsn,
        }
    }
}

const BLOCK_SIZE: usize = 8192;

///
/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.  We keep an ImageLayer in memory for each file, in the LayerMap. If a
/// layer is in "loaded" state, we have a copy of the file in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
///
pub struct ImageLayer {
    path_or_conf: PathOrConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    // This entry contains an image of all pages as of this LSN
    pub lsn: Lsn,

    inner: Mutex<ImageLayerInner>,
}

#[derive(Clone)]
enum ImageType {
    Blocky { num_blocks: SegmentBlk },
    NonBlocky,
}

pub struct ImageLayerInner {
    /// If None, the 'image_type' has not been loaded into memory yet.
    book: Option<Book<VirtualFile>>,

    /// Derived from filename and bookfile chapter metadata
    image_type: ImageType,
}

impl Layer for ImageLayer {
    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
    }

    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_range(&self) -> SegmentRange {
        SegmentRange::singleton(self.seg)
    }

    fn covers_seg(&self, seg: SegmentTag) -> bool {
        seg == self.seg
    }

    fn get_start_lsn(&self) -> Lsn {
        self.lsn
    }

    fn get_end_lsn(&self) -> Lsn {
        // End-bound is exclusive
        self.lsn + 1
    }

    /// Look up given page in the file
    fn get_page_reconstruct_data(
        &self,
        seg: SegmentTag,
        blknum: SegmentBlk,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        assert!(seg == self.seg);
        assert!((0..RELISH_SEG_SIZE).contains(&blknum));
        assert!(lsn >= self.lsn);

        match reconstruct_data.page_img {
            Some((cached_lsn, _)) if self.lsn <= cached_lsn => {
                return Ok(PageReconstructResult::Complete)
            }
            _ => {}
        }

        let inner = self.load()?;

        let buf = match &inner.image_type {
            ImageType::Blocky { num_blocks } => {
                // Check if the request is beyond EOF
                if blknum >= *num_blocks {
                    return Ok(PageReconstructResult::Missing(lsn));
                }

                let mut buf = vec![0u8; BLOCK_SIZE];
                let offset = BLOCK_SIZE as u64 * blknum as u64;

                let chapter = inner
                    .book
                    .as_ref()
                    .unwrap()
                    .chapter_reader(BLOCKY_IMAGES_CHAPTER)?;

                chapter.read_exact_at(&mut buf, offset).with_context(|| {
                    format!(
                        "failed to read page from data file {} at offset {}",
                        self.filename().display(),
                        offset
                    )
                })?;

                buf
            }
            ImageType::NonBlocky => {
                ensure!(blknum == 0);
                inner
                    .book
                    .as_ref()
                    .unwrap()
                    .read_chapter(NONBLOCKY_IMAGE_CHAPTER)?
                    .into_vec()
            }
        };

        reconstruct_data.page_img = Some((self.lsn, Bytes::from(buf)));
        Ok(PageReconstructResult::Complete)
    }

    /// Get size of the segment
    fn get_seg_size(&self, seg: SegmentTag, _lsn: Lsn) -> Result<Option<SegmentBlk>> {
        assert!(seg == self.seg);
        let inner = self.load()?;
        match inner.image_type {
            ImageType::Blocky { num_blocks } => Ok(Some(num_blocks)),
            ImageType::NonBlocky => Ok(Some(1)),
        }
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, seg: SegmentTag, lsn: Lsn) -> Result<bool> {
        assert!(lsn >= self.lsn);
        assert!(seg == self.seg);
        Ok(true)
    }

    fn unload(&self) -> Result<()> {
        Ok(())
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        false
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- image layer for ten {} tli {} seg {} at {} ----",
            self.tenantid, self.timelineid, self.seg, self.lsn
        );

        let inner = self.load()?;

        match inner.image_type {
            ImageType::Blocky { num_blocks } => println!("({}) blocks ", num_blocks),
            ImageType::NonBlocky => {
                let chapter = inner
                    .book
                    .as_ref()
                    .unwrap()
                    .read_chapter(NONBLOCKY_IMAGE_CHAPTER)?;
                println!("non-blocky ({} bytes)", chapter.len());
            }
        }

        Ok(())
    }
}

impl ImageLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.to_path_buf(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<ImageLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.book.is_some() {
            return Ok(inner);
        }

        let path = self.path();
        let file = VirtualFile::open(&path)
            .with_context(|| format!("Failed to open virtual file '{}'", path.display()))?;
        let book = Book::new(file).with_context(|| {
            format!(
                "Failed to open virtual file '{}' as a bookfile",
                path.display()
            )
        })?;

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
                let actual_summary = Summary::des(&chapter)?;

                let expected_summary = Summary::from(self);

                if actual_summary != expected_summary {
                    bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                }
            }
            PathOrConf::Path(path) => {
                let actual_filename = Path::new(path.file_name().unwrap());
                let expected_filename = self.filename();

                if actual_filename != expected_filename {
                    println!(
                        "warning: filename does not match what is expected from in-file summary"
                    );
                    println!("actual: {:?}", actual_filename);
                    println!("expected: {:?}", expected_filename);
                }
            }
        }

        let image_type = if self.seg.rel.is_blocky() {
            let chapter = book.chapter_reader(BLOCKY_IMAGES_CHAPTER)?;
            let images_len = chapter.len();
            ensure!(images_len % BLOCK_SIZE as u64 == 0);
            let num_blocks: SegmentBlk = (images_len / BLOCK_SIZE as u64).try_into()?;
            ImageType::Blocky { num_blocks }
        } else {
            let _chapter = book.chapter_reader(NONBLOCKY_IMAGE_CHAPTER)?;
            ImageType::NonBlocky
        };

        debug!("loaded from {}", &path.display());

        *inner = ImageLayerInner {
            book: Some(book),
            image_type,
        };

        Ok(inner)
    }

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> ImageLayer {
        ImageLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            seg: filename.seg,
            lsn: filename.lsn,
            inner: Mutex::new(ImageLayerInner {
                book: None,
                image_type: ImageType::Blocky { num_blocks: 0 },
            }),
        }
    }

    /// Create an ImageLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path<F>(path: &Path, book: &Book<F>) -> Result<ImageLayer>
    where
        F: std::os::unix::prelude::FileExt,
    {
        let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
        let summary = Summary::des(&chapter)?;

        Ok(ImageLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            seg: summary.seg,
            lsn: summary.lsn,
            inner: Mutex::new(ImageLayerInner {
                book: None,
                image_type: ImageType::Blocky { num_blocks: 0 },
            }),
        })
    }

    fn layer_name(&self) -> ImageFileName {
        ImageFileName {
            seg: self.seg,
            lsn: self.lsn,
        }
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timelineid,
            self.tenantid,
            &self.layer_name(),
        )
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every page
///    in the segment.
///
/// 3. Call `finish`.
///
pub struct ImageLayerWriter {
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
    seg: SegmentTag,
    lsn: Lsn,

    num_blocks: SegmentBlk,

    page_image_writer: ChapterWriter<BufWriter<VirtualFile>>,
    num_blocks_written: SegmentBlk,
}

impl ImageLayerWriter {
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        lsn: Lsn,
        num_blocks: SegmentBlk,
    ) -> Result<ImageLayerWriter> {
        // Create the file
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = ImageLayer::path_for(
            &PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            &ImageFileName { seg, lsn },
        );
        let file = VirtualFile::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let book = BookWriter::new(buf_writer, IMAGE_FILE_MAGIC)?;

        // Open the page-images chapter for writing. The calls to
        // `put_page_image` will use this to write the contents.
        let chapter = if seg.rel.is_blocky() {
            book.new_chapter(BLOCKY_IMAGES_CHAPTER)
        } else {
            assert_eq!(num_blocks, 1);
            book.new_chapter(NONBLOCKY_IMAGE_CHAPTER)
        };

        let writer = ImageLayerWriter {
            conf,
            timelineid,
            tenantid,
            seg,
            lsn,
            num_blocks,
            page_image_writer: chapter,
            num_blocks_written: 0,
        };

        Ok(writer)
    }

    ///
    /// Write next page image to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    pub fn put_page_image(&mut self, block_bytes: &[u8]) -> Result<()> {
        assert!(self.num_blocks_written < self.num_blocks);
        if self.seg.rel.is_blocky() {
            assert_eq!(block_bytes.len(), BLOCK_SIZE);
        }
        self.page_image_writer.write_all(block_bytes)?;
        self.num_blocks_written += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<ImageLayer> {
        // Check that the `put_page_image' was called for every block.
        assert!(self.num_blocks_written == self.num_blocks);

        // Close the page-images chapter
        let book = self.page_image_writer.close()?;

        // Write out the summary chapter
        let image_type = if self.seg.rel.is_blocky() {
            ImageType::Blocky {
                num_blocks: self.num_blocks,
            }
        } else {
            ImageType::NonBlocky
        };
        let mut chapter = book.new_chapter(SUMMARY_CHAPTER);
        let summary = Summary {
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            seg: self.seg,
            lsn: self.lsn,
        };
        Summary::ser_into(&summary, &mut chapter)?;
        let book = chapter.close()?;

        // This flushes the underlying 'buf_writer'.
        book.close()?;

        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        let layer = ImageLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            timelineid: self.timelineid,
            tenantid: self.tenantid,
            seg: self.seg,
            lsn: self.lsn,
            inner: Mutex::new(ImageLayerInner {
                book: None,
                image_type,
            }),
        };
        trace!("created image layer {}", layer.path().display());

        Ok(layer)
    }
}
