
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufRead};
// use std::io::Error;
use std::path::Path;
use std::char;
use std::cmp;
use std::mem;
use std::time::Duration;

use futures_cpupool::CpuPool;
use tokio_timer::Timer;

use futures::{Poll, Future, Sink};
use futures::executor;
use futures::future::{ok, err};
use futures::stream::{iter, Peekable, BoxStream, channel, Stream};
use futures::sync::oneshot;
use futures::sync::mpsc;
use std::str;
use std::thread;
use std::fmt;
use std::sync::mpsc::sync_channel;
use std::fs;

// use std::os::windows::fs::FileExt;
use std::io::SeekFrom;
use std::collections::HashMap;
use util;
use std::collections::hash_map::Entry;
use fnv::FnvHashMap;


//-----

use serde_json;
use serde_json::Value;

pub struct CreateIndexOptions {
    tokenize: bool,
    firstCharExactMatch: bool
}


pub fn getAllterms(data:String,  path:&str, options:CreateIndexOptions){


}


pub fn createFulltextIndex(data:String, path:&str, options:CreateIndexOptions){


	let dat2 = r#" { "name": "John Doe", "age": 43, ... } "#;
	let v: Value = serde_json::from_str(dat2).unwrap();


	let allTerms = getAllterms(data, path, options);

	let paths = util::getStepsToAnchor(path);

	let lastPath = (paths.iter().last().unwrap()).clone();

	for pathToAnchor in paths {
	    let level = util::getLevel(&pathToAnchor);
	    // let tuples = vec![];

	    let isLast = pathToAnchor == lastPath;

	}

}