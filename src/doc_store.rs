use std::io::prelude::*;
use std::io::SeekFrom;

use search;

#[derive(Debug)]
pub struct DocLoader {}

use persistence::Persistence;

impl DocLoader {

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_doc(persistence: &Persistence, pos: usize) -> Result<String, search::SearchError> {
        let (start, end) = {
            debug!("now loading document offsets for id {:?}", pos);
            let offsets = persistence.get_offsets("data").unwrap();
            (offsets.get_value(pos as u64).unwrap() as usize, offsets.get_value(pos as u64 + 1).unwrap() as usize) // @Temporary array access by get - option
        };

        let mut f = persistence.get_file_handle("data")?;
        let mut buffer: Vec<u8> = vec![0; (end - start) as usize];

        f.seek(SeekFrom::Start(start as u64))?;
        f.read_exact(&mut buffer)?;

        let s = unsafe { String::from_utf8_unchecked(buffer) };

        Ok(s)
    }
}
