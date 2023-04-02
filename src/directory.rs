use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::ops::Deref;
use std::path::Path;
use std::sync::Weak;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use memmap2::Mmap;

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

pub struct MmapDirectory {
    inner: Arc<MmapDirectoryInner>,
}

impl Debug for MmapDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapDirectory").finish()
    }
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
