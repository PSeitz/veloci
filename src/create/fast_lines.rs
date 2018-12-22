use std::io::BufRead;
use std::io::BufReader;
// use rayon::prelude::*;
use std::fs::File;
use std::thread;

use crossbeam_channel as channel;
type PlanDataReceiver = crossbeam_channel::Receiver<Result<serde_json::Value, serde_json::Error>>;

pub trait FastLinesTrait {
    fn fast_lines(self) -> FastLinesJson;
}

impl FastLinesTrait for BufReader<File> {
    fn fast_lines(mut self) -> FastLinesJson {
        let (s, r) = channel::bounded(5);
        thread::spawn(move || loop {
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
                    s.send(serde_json::from_str(&line)).expect("could not send json to channel while indexing");
                }
                Err(_e) => break,
            }
        });

        FastLinesJson { receiver: r }
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
        self.receiver.recv().ok()
    }
}
