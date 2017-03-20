
use regex::Regex;
use std::io::prelude::*;
use std::io;
use std::mem;
use std::fs::File;
use std;

pub fn normalize_text(text:&str) -> String {

    lazy_static! {
        static ref REGEXES:Vec<(Regex, & 'static str)> = vec![
            (Regex::new(r"\([fmn\d]\)").unwrap(), " "),
            (Regex::new(r"[\(\)]").unwrap(), " "),  // remove braces
            (Regex::new(r#"[{}'"“]"#).unwrap(), ""), // remove ' " {}
            (Regex::new(r"\s\s+").unwrap(), " "), // replace tabs, newlines, double spaces with single spaces
            (Regex::new(r"[,.…]").unwrap(), ""),  // remove , .
            (Regex::new(r"[;・’-]").unwrap(), "") // remove ;・’-
        ];
    }
    let mut new_str = text.to_owned();
    for ref tupl in &*REGEXES {
        new_str = (tupl.0).replace_all(&new_str, tupl.1).into_owned();
    }

    new_str.trim().to_owned()

}

pub fn get_file_path(folder: &str, path:&str, suffix:&str) -> String {
    folder.to_string()+"/"+path+suffix
}

pub fn get_path_name(path_to_anchor: &str, is_text_index_part:bool) -> String{
    let suffix = if is_text_index_part {".textindex"}else{""};
    path_to_anchor.to_owned() + suffix
}

pub fn load_index64(s1: &str) -> Result<(Vec<u64>), io::Error> {
    let mut f = File::open(s1)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    buffer.shrink_to_fit();
    let buf_len = buffer.len();

    let mut read: Vec<u64> = unsafe { mem::transmute(buffer) };
    unsafe { read.set_len(buf_len/8); }
    info!("Loaded Index64 With size {:?}", read.len());
    Ok(read)

}

pub fn load_index(s1: &str) -> Result<(Vec<u32>), io::Error> {
    let mut f = File::open(s1)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    buffer.shrink_to_fit();
    let buf_len = buffer.len();

    let mut read: Vec<u32> = unsafe { mem::transmute(buffer) };
    unsafe { read.set_len(buf_len/4); }
    // info!("100: {}", data[100]);
    info!("Loaded Index32 With size {:?}", read.len());
    Ok(read)
    // let v_from_raw = unsafe {
    // Vec::from_raw_parts(buffer.as_mut_ptr(),
    //                     buffer.len(),
    //                     buffer.capacity())
    // };
    // info!("100: {}", v_from_raw[100]);

    // >   native_search-429325dbe2259521.exe!alloc::raw_vec::{{impl}}::drop<u32>(alloc::raw_vec::RawVec<u32> * self) Line 546 Unknown

}

unsafe fn typed_to_bytes<T>(slice: &[T]) -> &[u8] {
    std::slice::from_raw_parts(slice.as_ptr() as *const u8,
                               slice.len() * mem::size_of::<T>())
}

pub fn write_index(data:&Vec<u32>, path:&str) -> Result<(), io::Error> {
    unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
    info!("Wrote Index32 With size {:?}", data.len());
    Ok(())
}

pub fn write_index64(data:&Vec<u64>, path:&str) -> Result<(), io::Error> {

    unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
    info!("Wrote Index64 With size {:?}", data.len());
    // let read: Vec<u8> = unsafe { mem::transmute(data) };
    // File::create(path)?.write_all(&read);
    // let v_from_raw:Vec<u8> = unsafe {
    //     // let x_ptr:*mut u8 = data.as_mut_ptr() as *mut u8;
    //     Vec::from_raw_parts(mem::transmute::<*const u64, *mut u8>(data.as_ptr()),
    //                         data.len() * 8,
    //                         data.capacity())
    // };
    // File::create(path)?.write_all(v_from_raw.as_slice())?;
    Ok(())
}

pub fn get_level(path:&str) -> u32{
    path.matches("[]").count() as u32
}

pub fn remove_array_marker(path:&str) -> String{
    path.split(".").collect::<Vec<_>>()
    .iter().map(|el| {
        if el.ends_with("[]") {
            &el[0..el.len()-2]
        } 
        else {el}
    }).collect::<Vec<_>>()
    .join(".")
}


pub fn get_steps_to_anchor(path:&str) -> Vec<String> {
    
    let mut paths = vec![];
    let mut current = vec![];
    // let parts = path.split('.')
    let parts = path.split(".");

    for part in parts {
        current.push(part.to_string());
        if part.ends_with("[]"){
            let joined = current.join(".");
            paths.push(joined);
        }
    }

    paths.push(path.to_string()); // add complete path
    return paths


}


// assert_eq!(re.replace("1078910", ""), " ");

//     text = text.replace(/ *\([^)]*\) */g, ' ') // remove everything in braces
//     text = text.replace(/[{}'"]/g, '') // remove ' " {}
//     text = text.replace(/\s\s+/g, ' ') // replace tabs, newlines, double spaces with single spaces
//     text = text.replace(/[,.]/g, '') // remove , .
//     text = text.replace(/[;・’-]/g, '') // remove ;・’-
//     text = text.toLowerCase()
//     return text.trim()
// }

//     text = text.replace(/ *\([fmn\d)]*\) */g, ' ') // remove (f)(n)(m)(1)...(9)
//     text = text.replace(/[\(\)]/g, ' ') // remove braces
//     text = text.replace(/[{}'"“]/g, '') // remove ' " {}
//     text = text.replace(/\s\s+/g, ' ') // replace tabs, newlines, double spaces with single spaces
//     text = text.replace(/[,.…]/g, '') // remove , .
//     text = text.replace(/[;・’-]/g, '') // remove ;・’-
//     text = text.toLowerCase()
//     return text.trim()
// }