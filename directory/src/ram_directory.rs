use log::debug;
use log::warn;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use ownedbytes::OwnedBytes;
use stable_deref_trait::StableDeref;

use super::{AntiCallToken, Directory, TerminatingWrite, WritePtr};

/// Writer associated with the [`RamDirectory`].
///
/// The Writer just writes a buffer.
struct VecWriter {
    path: PathBuf,
    shared_directory: RamDirectory,
    data: Vec<u8>,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf, shared_directory: RamDirectory) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: Vec::new(),
            shared_directory,
            is_flushed: true,
        }
    }
}

impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            warn!(
                "You forgot to flush {:?} before its writer got Drop. Do not rely on drop. This \
                 also occurs when the indexer crashed, so you may want to check the logs for the \
                 root cause.",
                self.path
            )
        }
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        let mut fs = self.shared_directory.fs.write().unwrap();
        fs.append(self.path.clone(), &self.data);
        self.data.clear();
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

#[derive(Default)]
struct InnerDirectory {
    fs: HashMap<PathBuf, Arc<Vec<u8>>>,
}

#[derive(Clone)]
struct ArcVec(Arc<dyn Deref<Target = [u8]> + Send + Sync>);

impl Deref for ArcVec {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}
unsafe impl StableDeref for ArcVec {}

impl InnerDirectory {
    fn append(&mut self, path: PathBuf, data: &[u8]) -> bool {
        let path_buf = path;
        if let Some(existing_data) = self.fs.get_mut(&path_buf) {
            Arc::make_mut(existing_data).extend_from_slice(data);
            true
        } else {
            self.fs.insert(path_buf, Arc::new(data.to_vec()));
            false
        }
    }

    fn write(&mut self, path: PathBuf, data: &[u8]) -> bool {
        let data = Arc::from(data.to_vec());
        self.fs.insert(path, data).is_some()
    }

    fn get_file_bytes(&self, path: &Path) -> Result<OwnedBytes, io::Error> {
        self.fs
            .get(path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("File {:?} does not exist", path)))
            .map(|el| OwnedBytes::new(ArcVec(el.clone())))
    }

    fn delete(&mut self, path: &Path) -> Result<(), io::Error> {
        match self.fs.remove(path) {
            Some(_) => Ok(()),
            None => Err(io::Error::new(io::ErrorKind::NotFound, path.to_str().unwrap().to_string())),
        }
    }

    #[allow(unused)]
    fn exists(&self, path: &Path) -> bool {
        self.fs.contains_key(path)
    }

    fn total_mem_usage(&self) -> usize {
        self.fs.values().map(|f| f.len()).sum()
    }
}

impl fmt::Debug for RamDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fs = self.fs.read().unwrap();
        write!(f, "RamDirectory")?;
        write!(f, "{:?}", fs.fs.keys())?;
        Ok(())
    }
}

/// A Directory storing everything in anonymous memory.
///
/// It is mainly meant for unit testing.
/// Writes are only made visible upon flushing.
#[derive(Clone, Default)]
pub struct RamDirectory {
    fs: Arc<RwLock<InnerDirectory>>,
}

impl RamDirectory {
    /// Constructor
    pub fn create() -> RamDirectory {
        Self::default()
    }

    /// Returns the sum of the size of the different files
    /// in the [`RamDirectory`].
    pub fn total_mem_usage(&self) -> usize {
        self.fs.read().unwrap().total_mem_usage()
    }

    /// Write a copy of all of the files saved in the [`RamDirectory`] in the target [`Directory`].
    ///
    /// Files are all written using the [`Directory::open_write()`] meaning, even if they were
    /// written using the [`Directory::atomic_write()`] api.
    ///
    /// If an error is encountered, files may be persisted partially.
    pub fn persist(&self, dest: &dyn Directory) -> Result<(), io::Error> {
        let wlock = self.fs.write().unwrap();
        for (path, file) in wlock.fs.iter() {
            let mut dest_wrt = dest.open_append(path)?;
            dest_wrt.write_all(file.as_slice())?;
            dest_wrt.terminate()?;
        }
        Ok(())
    }
}

impl Directory for RamDirectory {
    fn get_file_bytes(&self, path: &Path) -> Result<OwnedBytes, io::Error> {
        let fs = self.fs.read().unwrap();
        fs.get_file_bytes(path)
    }

    fn open_append(&self, path: &Path) -> Result<WritePtr, io::Error> {
        debug!("Append Write {:?}", path);
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf, self.clone());
        Ok(BufWriter::new(Box::new(vec_writer)))
    }

    fn write(&self, path: &Path, data: &[u8]) -> Result<(), io::Error> {
        debug!("Write {:?}", path);
        let path_buf = PathBuf::from(path);
        let mut fs = self.fs.write().unwrap();
        fs.write(path_buf, data);
        Ok(())
    }

    fn delete(&self, path: &Path) -> Result<(), io::Error> {
        self.fs.write().unwrap().delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, io::Error> {
        Ok(self.fs.read().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?.exists(path))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append() {
        let path = Path::new("testfile");
        let directory: Box<dyn Directory> = Box::<RamDirectory>::default();
        {
            let mut wrt = directory.open_append(path).unwrap();
            wrt.write_all(&[1, 2, 3]).unwrap();
            wrt.flush().unwrap();
            wrt.write_all(&[5, 6, 7]).unwrap();
            wrt.flush().unwrap();
        }
        assert_eq!(directory.get_file_bytes(path).unwrap().as_ref(), &[1, 2, 3, 5, 6, 7]);

        {
            let mut wrt = directory.open_append(path).unwrap();
            wrt.write_all(&[5, 6, 7]).unwrap();
            wrt.flush().unwrap();
        }
        assert_eq!(directory.get_file_bytes(path).unwrap().as_ref(), &[1, 2, 3, 5, 6, 7, 5, 6, 7]);
    }

    #[test]
    fn test_ram_dir() {
        let path = Path::new("testfile");
        let directory: Box<dyn Directory> = Box::<RamDirectory>::default();
        {
            let mut wrt = directory.open_append(path).unwrap();
            wrt.write_all(&[1, 2, 3]).unwrap();
            wrt.flush().unwrap();
        }
        assert_eq!(directory.get_file_bytes(path).unwrap().as_ref(), &[1, 2, 3]);
    }
}
