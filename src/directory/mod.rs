mod mmap_directory;
mod ram_directory;

use std::io;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::Weak;

pub use mmap_directory::MmapDirectory;

/// Write object for Directory.
///
/// `WritePtr` are required to implement both Write
/// and Seek.
pub type WritePtr = std::io::BufWriter<Box<dyn TerminatingWrite>>;

use ownedbytes::OwnedBytes;

pub type ArcBytes = Arc<dyn Deref<Target = [u8]> + Send + Sync + 'static>;
pub type WeakArcBytes = Weak<dyn Deref<Target = [u8]> + Send + Sync + 'static>;

pub trait Directory: DirectoryClone + std::fmt::Debug + Send + Sync + 'static {
    /// Opens a file and returns a the file contents as OwnedBytes
    ///
    /// Users of `Directory` should typically call `Directory::open_read(...)`,
    /// while `Directory` implementor should implement `get_file_handle()`.
    fn get_file_bytes(&self, path: &Path) -> Result<OwnedBytes, io::Error>;

    /// Opens a writer for the *virtual file* associated with
    /// a [`Path`].
    ///
    /// Right after this call, for the span of the execution of the program
    /// the file should be created and any subsequent call to
    /// [`Directory::open_read()`] for the same path should return
    /// a [`FileSlice`].
    ///
    /// However, depending on the directory implementation,
    /// it might be required to call [`Directory::sync_directory()`] to ensure
    /// that the file is durably created.
    /// (The semantics here are the same when dealing with
    /// a POSIX filesystem.)
    ///
    /// Write operations may be aggressively buffered.
    /// The client of this trait is responsible for calling flush
    /// to ensure that subsequent `read` operations
    /// will take into account preceding `write` operations.
    ///
    /// Flush operation should also be persistent.
    ///
    /// The user shall not rely on [`Drop`] triggering `flush`.
    /// Note that [`RamDirectory`][crate::directory::RamDirectory] will
    /// panic! if `flush` was not called.
    ///
    /// The file may not previously exist.
    fn open_write(&self, path: &Path) -> Result<WritePtr, io::Error>;

    /// Sync the directory.
    ///
    /// This call is required to ensure that newly created files are
    /// effectively stored durably.
    fn sync_directory(&self) -> io::Result<()>;
}

/// DirectoryClone
pub trait DirectoryClone {
    /// Clones the directory and boxes the clone
    fn box_clone(&self) -> Box<dyn Directory>;
}

impl<T> DirectoryClone for T
where
    T: 'static + Directory + Clone,
{
    fn box_clone(&self) -> Box<dyn Directory> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Directory> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

impl<T: Directory + 'static> From<T> for Box<dyn Directory> {
    fn from(t: T) -> Self {
        Box::new(t)
    }
}

/// Struct used to prevent from calling
/// [`terminate_ref`](TerminatingWrite::terminate_ref) directly
///
/// The point is that while the type is public, it cannot be built by anyone
/// outside of this module.
#[derive(Debug)]
pub struct AntiCallToken(());

/// Trait used to indicate when no more write need to be done on a writer
pub trait TerminatingWrite: std::io::Write + Send + Sync {
    /// Indicate that the writer will no longer be used. Internally call terminate_ref.
    fn terminate(mut self) -> io::Result<()>
    where
        Self: Sized,
    {
        self.terminate_ref(AntiCallToken(()))
    }

    /// You should implement this function to define custom behavior.
    /// This function should flush any buffer it may hold.
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()>;
}

impl<W: TerminatingWrite + ?Sized> TerminatingWrite for Box<W> {
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.as_mut().terminate_ref(token)
    }
}

impl<W: TerminatingWrite> TerminatingWrite for std::io::BufWriter<W> {
    fn terminate_ref(&mut self, a: AntiCallToken) -> io::Result<()> {
        self.flush()?;
        self.get_mut().terminate_ref(a)
    }
}

impl<'a> TerminatingWrite for &'a mut Vec<u8> {
    fn terminate_ref(&mut self, _a: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}
