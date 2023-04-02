use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, Write};
use std::ops::Deref;
use std::path::Path;
use std::sync::Weak;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use memmap2::Mmap;

use ownedbytes::OwnedBytes;
use stable_deref_trait::StableDeref;

use super::{AntiCallToken, Directory, TerminatingWrite, WritePtr};

pub type ArcBytes = Arc<dyn Deref<Target = [u8]> + Send + Sync + 'static>;
pub type WeakArcBytes = Weak<dyn Deref<Target = [u8]> + Send + Sync + 'static>;

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct CacheCounters {
    /// Number of time the cache prevents to call `mmap`
    pub hit: usize,
    /// Number of time tantivy had to call `mmap`
    /// as no entry was in the cache.
    pub miss: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheInfo {
    pub counters: CacheCounters,
    pub mmapped: Vec<PathBuf>,
}

#[derive(Default)]
struct MmapCache {
    counters: CacheCounters,
    cache: HashMap<PathBuf, WeakArcBytes>,
}

impl MmapCache {
    fn get_info(&self) -> CacheInfo {
        let paths: Vec<PathBuf> = self.cache.keys().cloned().collect();
        CacheInfo {
            counters: self.counters.clone(),
            mmapped: paths,
        }
    }

    // Returns None if the file exists but as a len of 0 (and hence is not mmappable).
    fn get_mmap(&mut self, full_path: &Path) -> Result<Option<ArcBytes>, io::Error> {
        if let Some(mmap_weak) = self.cache.get(full_path) {
            if let Some(mmap_arc) = mmap_weak.upgrade() {
                self.counters.hit += 1;
                return Ok(Some(mmap_arc));
            }
        }
        self.cache.remove(full_path);
        self.counters.miss += 1;
        let mmap_opt = open_mmap(full_path)?;
        Ok(mmap_opt.map(|mmap| {
            let mmap_arc: ArcBytes = Arc::new(mmap);
            let mmap_weak = Arc::downgrade(&mmap_arc);
            self.cache.insert(full_path.to_owned(), mmap_weak);
            mmap_arc
        }))
    }
}
/// Returns `None` iff the file exists, can be read, but is empty (and hence
/// cannot be mmapped)
fn open_mmap(full_path: &Path) -> Result<Option<Mmap>, io::Error> {
    let file = File::open(full_path)?;

    let meta_data = file.metadata()?;
    if meta_data.len() == 0 {
        // if the file size is 0, it will not be possible
        // to mmap the file, so we return None
        // instead.
        return Ok(None);
    }
    unsafe { memmap2::Mmap::map(&file).map(Some) }
}

struct MmapDirectoryInner {
    root_path: PathBuf,
    mmap_cache: RwLock<MmapCache>,
}

impl MmapDirectoryInner {
    fn new(root_path: PathBuf) -> MmapDirectoryInner {
        MmapDirectoryInner {
            mmap_cache: Default::default(),
            root_path,
        }
    }
}

#[derive(Clone)]
struct MmapArc(Arc<dyn Deref<Target = [u8]> + Send + Sync>);

impl Deref for MmapArc {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}
unsafe impl StableDeref for MmapArc {}

#[derive(Clone)]
pub struct MmapDirectory {
    inner: Arc<MmapDirectoryInner>,
}

impl Debug for MmapDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapDirectory").finish()
    }
}

impl Directory for MmapDirectory {
    fn open_write(&self, path: &Path) -> Result<WritePtr, io::Error> {
        debug!("Open Write {:?}", path);
        let full_path = self.resolve_path(path);

        let mut file = OpenOptions::new().write(true).create_new(true).open(full_path)?;

        // making sure the file is created.
        file.flush()?;

        // Note we actually do not sync the parent directory here.
        //
        // A newly created file, may, in some case, be created and even flushed to disk.
        // and then lost...
        //
        // The file will only be durably written after we terminate AND
        // sync_directory() is called.

        let writer = SafeFileWriter::new(file);
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn get_file_bytes(&self, path: &Path) -> Result<OwnedBytes, io::Error> {
        debug!("Open Read {:?}", path);
        let full_path = self.resolve_path(path);

        let mut mmap_cache = self.inner.mmap_cache.write().map_err(|_| {
            let msg = format!("Failed to acquired write lock on mmap cache while reading {:?}", path);
            make_io_err(msg)
        })?;

        let owned_bytes = mmap_cache
            .get_mmap(&full_path)?
            .map(|mmap_arc| {
                let mmap_arc_obj = MmapArc(mmap_arc);
                OwnedBytes::new(mmap_arc_obj)
            })
            .unwrap_or_else(OwnedBytes::empty);

        Ok(owned_bytes)
    }

    #[cfg(windows)]
    fn sync_directory(&self) -> Result<(), io::Error> {
        // On Windows, it is not necessary to fsync the parent directory to
        // ensure that the directory entry containing the file has also reached
        // disk, and calling sync_data on a handle to directory is a no-op on
        // local disks, but will return an error on virtual drives.
        Ok(())
    }

    #[cfg(not(windows))]
    fn sync_directory(&self) -> Result<(), io::Error> {
        let mut open_opts = OpenOptions::new();

        // Linux needs read to be set, otherwise returns EINVAL
        // write must not be set, or it fails with EISDIR
        open_opts.read(true);

        let fd = open_opts.open(&self.inner.root_path)?;
        fd.sync_data()?;
        Ok(())
    }
}

impl MmapDirectory {
    // Creates a new directory under the path
    pub fn create(path: &Path) -> Result<Self, io::Error> {
        fs::create_dir_all(&path).unwrap();

        fs::create_dir(path.join("temp")).unwrap(); // for temporary index creation
        Ok(MmapDirectory {
            inner: Arc::new(MmapDirectoryInner::new(path.to_path_buf())),
        })
    }

    /// Opens an existing directory in the path
    pub fn open(path: &Path) -> Result<Self, io::Error> {
        Ok(MmapDirectory {
            inner: Arc::new(MmapDirectoryInner::new(path.to_path_buf())),
        })
    }

    /// Joins a relative_path to the directory `root_path`
    /// to create a proper complete `filepath`.
    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.inner.root_path.join(relative_path)
    }

    /// Creates a new file
    fn create_file(&self, relative_path: &Path) -> Result<File, io::Error> {
        File::create(self.inner.root_path.join(relative_path))
    }
}

/// Create a default io error given a string.
pub(crate) fn make_io_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

/// This Write wraps a File, but has the specificity of
/// call `sync_all` on flush.
struct SafeFileWriter(File);

impl SafeFileWriter {
    fn new(file: File) -> SafeFileWriter {
        SafeFileWriter(file)
    }
}

impl Write for SafeFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for SafeFileWriter {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

impl TerminatingWrite for SafeFileWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.0.flush()?;
        self.0.sync_data()?;
        Ok(())
    }
}
