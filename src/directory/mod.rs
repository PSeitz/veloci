use std::{io, path::Path};

pub use directory::Directory;
pub use directory::MmapDirectory;
pub use directory::RamDirectory;
use ownedbytes::OwnedBytes;

use crate::util::{Ext, SetExt};

pub fn load_data_pair(directory: &Box<dyn Directory>, path: &Path) -> Result<(OwnedBytes, OwnedBytes), io::Error> {
    let data_path = path.set_ext(Ext::Data);
    let indirect_path = path.set_ext(Ext::Indirect);
    Ok((directory.get_file_bytes(&indirect_path)?, directory.get_file_bytes(&data_path)?))
}
