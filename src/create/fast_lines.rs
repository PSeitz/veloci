use std::io::BufRead;
use std::io::BufReader;
// use rayon::prelude::*;
use std::thread;
use std::fs::File;

use crossbeam_channel as channel;
type PlanDataReceiver = crossbeam_channel::Receiver<Result<serde_json::Value, serde_json::Error>>;

pub trait FastLinesTrait {
    fn fast_lines(self) -> FastLinesJson;
}

impl FastLinesTrait for BufReader<File> {
    fn fast_lines(mut self) -> FastLinesJson
    {
        let (s, r) = channel::bounded(5);
        thread::spawn(move || {

            loop {
                let mut cache = vec![];
                match self.read_until(b'\n', &mut cache) {
                    Ok(0) => break,
                    Ok(_n) => {
                        if cache.ends_with(b"\n") {
                            cache.pop();
                            if cache.ends_with(b"\r") {
                                cache.pop();
                            }
                        }
                        let line = unsafe { String::from_utf8_unchecked(cache) };
                        s.send(serde_json::from_str(&line))
                    }
                    Err(_e) => break,
                }
            }
        });

        FastLinesJson {
            receiver: r,
        }
    }
}
#[derive(Debug)]
pub struct FastLinesJson {
    receiver: PlanDataReceiver,
}

impl Iterator for FastLinesJson {
    type Item = Result<serde_json::Value, serde_json::Error>;

    #[inline(always)]
    fn next(&mut self) -> Option<Result<serde_json::Value, serde_json::Error>> {
        self.receiver.recv()
    }
}

// pub trait FastLinesTrait<T> {
//     fn fast_lines(self) -> FastLinesJson<Self>
//     where
//         Self: Sized,
//     {
//         FastLinesJson {
//             reader: self,
//             prepared_jsons: vec![],
//         }
//     }
// }

// impl<T> FastLinesTrait<T> for BufReader<T> {
//     fn fast_lines(self) -> FastLinesJson<Self>
//     where
//         Self: Sized,
//     {
//         FastLinesJson {
//             reader: self,
//             prepared_jsons: vec![],
//         }
//     }
// }
// #[derive(Debug)]
// pub struct FastLinesJson<T> {
//     reader: T,
//     // cache: Vec<u8>,
//     prepared_jsons: Vec<Result<serde_json::Value, serde_json::Error>>,
// }
// impl<T: BufRead> FastLinesJson<T> {
//     #[inline]
//     fn load_lines_into_cache(&mut self) {
//         let mut lines = vec![];
//         for _ in 0..256 {
//             if let Some(line) = self.load_line() {
//                 lines.push(line);
//             } else {
//                 break;
//             }
//         }

//         self.prepared_jsons = lines
//             .par_iter()
//             .map(|line| serde_json::from_str(line))
//             .collect();

//     }

//     #[inline]
//     fn load_line(&mut self) -> Option<String> {
//         let mut cache = vec![];
//         match self.reader.read_until(b'\n', &mut cache) {
//             Ok(0) => None,
//             Ok(_n) => {
//                 if cache.ends_with(b"\n") {
//                     cache.pop();
//                     if cache.ends_with(b"\r") {
//                         cache.pop();
//                     }
//                 }
//                 Some(unsafe { String::from_utf8_unchecked(cache) })
//             }
//             Err(_e) => None,
//         }
//     }
// }

// impl<B: BufRead> Iterator for FastLinesJson<B> {
//     type Item = Result<serde_json::Value, serde_json::Error>;

//     #[inline(always)]
//     fn next(&mut self) -> Option<Result<serde_json::Value, serde_json::Error>> {
//         if let Some(next) = self.prepared_jsons.pop() {
//             Some(next)
//         } else {
//             self.load_lines_into_cache();
//             if let Some(next) = self.prepared_jsons.pop() {
//                 Some(next)
//             } else {
//                 None
//             }
//         }
//     }
//     // fn next(&mut self) -> Option<Result<serde_json::Value, serde_json::Error>> {
//     //     self.cache.clear();
//     //     match self.reader.read_until(b'\n', &mut self.cache) {
//     //         Ok(0) => None,
//     //         Ok(_n) => {
//     //             if self.cache.ends_with(b"\n") {
//     //                 self.cache.pop();
//     //                 if self.cache.ends_with(b"\r") {
//     //                     self.cache.pop();
//     //                 }
//     //             }
//     //             let json = serde_json::from_str(unsafe { std::str::from_utf8_unchecked(&self.cache) });
//     //             Some(json)
//     //         }
//     //         Err(_e) => None,
//     //     }
//     // }
// }