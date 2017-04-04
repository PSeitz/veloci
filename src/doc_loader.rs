use std::fs::File;
use std::io::prelude::*;
use std::io;
use std::io::SeekFrom;
use std::str;

use util::get_file_path;
use persistence;

#[derive(Debug)]
pub struct DocLoader {
    folder: String,
    filename: String
    // offsets: Vec<u64>
}

impl DocLoader {
    pub fn new(folder:&str, filename:&str) -> DocLoader {
        persistence::load_index_64(&get_file_path(folder, filename, ".offsets")).unwrap();
        DocLoader{folder : folder.to_string(), filename: filename.to_string()}
    }

    pub fn get_doc(&self, pos: usize) -> Result<String, io::Error> {

        let (start, end) = {
            let cache_lock = persistence::INDEX_64_CACHE.read().unwrap();
            let offsets = cache_lock.get(&get_file_path(&self.folder, &self.filename, ".offsets")).unwrap();
            (offsets[pos] as usize, offsets[pos as usize + 1] as usize)
        };

        let mut f = File::open(&get_file_path(&self.folder, &self.filename, ""))?;
        // println!("OPen Time: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
        // let start = offsets[pos] as usize;
        // let end = offsets[pos as usize + 1] as usize;
        let mut buffer:Vec<u8> = Vec::with_capacity((end - start) as usize);
        unsafe { buffer.set_len(end - start ); }
        // println!("Buffer Create Time: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

        f.seek(SeekFrom::Start(start as u64))?;
        f.read_exact(&mut buffer)?;
        // println!("Load Buffer: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

        let s = unsafe {str::from_utf8_unchecked(&buffer).to_string()};
        // println!("To String: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

        Ok(s)
    }

}

