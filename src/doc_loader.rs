use std::fs::File;
use std::io::prelude::*;
use std::fs;
use std::io;
use std::io::SeekFrom;
use std::str;
use std::time::Instant;

use util;
use util::get_file_path;


#[derive(Debug)]
pub struct DocLoader {
    folder: String,
    filename: String,
    offsets: Vec<u64>
}

impl DocLoader {
    pub fn new(folder:&str, filename:&str) -> DocLoader {
        DocLoader{folder : folder.to_string(), filename: filename.to_string(), offsets: util::load_index64(&get_file_path(folder, filename, ".offsets")).unwrap()}
    }

    pub fn get_doc(&self, pos: usize) -> Result<String, io::Error> {
        let now = Instant::now();
        let mut f = File::open(&get_file_path(&self.folder, &self.filename, ""))?;
        // println!("OPen Time: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
        let start = self.offsets[pos] as usize;
        let end = self.offsets[pos as usize + 1] as usize;
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

