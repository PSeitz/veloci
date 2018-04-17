use std::mem::transmute;
use std::ptr;
use std::fs::File;
use std::io::prelude::*;
use std::io;
use std::fs;

use persistence::*;
use type_info::TypeInfo;
use half::f16;
use std;

use heapsize::HeapSizeOf;

use memmap::Mmap;
use memmap::MmapOptions;
use util::*;

mod compressor;
pub mod token_to_anchor_score_vint;

const U31_MAX: u32 = (1 << 31) - 1;

const SIZE_OF_ANCHOR_SCORE: usize = std::mem::size_of::<AnchorScore>();
const SIZE_OF_NUM_ELEM: usize = std::mem::size_of::<(u32)>();



impl_type_info!(TokenToAnchorScoreBinary, TokenToAnchorScoreMmap);

struct CompactHit {
    id: [u8; 3],
    score: u8,
}
impl CompactHit {
    fn new(id: u32, score: u8) -> Self {
        let bytes: [u8; 4] = unsafe { transmute(id) };
        let id: [u8; 3] = [bytes[0], bytes[1], bytes[2]];
        CompactHit { id, score }
    }
    fn get_id(&self) -> u32 {
        let bytes: [u8; 4] = [self.id[0], self.id[1], self.id[2], 0];
        unsafe { transmute(bytes) }
    }
}
#[test]
fn test_compact_hit() {
    let hit = CompactHit::new(100, 1);
    assert_eq!(hit.get_id(), 100);
    assert_eq!(hit.score, 1);
}

#[repr(packed)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnchorScore {
    pub id: u32,
    pub score: f16,
}

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// pub struct AnchorScore {
//     pub id: u32,
//     pub score: f16,
// }

#[repr(packed)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnchorScoreSerialize {
    pub id: u32,
    pub score: u16,
}

impl AnchorScoreSerialize {
    pub fn new(id: u32, score: u16) -> AnchorScoreSerialize {
        AnchorScoreSerialize { id: id, score: score }
    }
}
impl AnchorScore {
    pub fn new(id: u32, score: f16) -> AnchorScore {
        AnchorScore { id: id, score: score }
    }
}



#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct TokenToAnchorScoreBinary {
    pub start_pos: Vec<u32>,
    pub data: Vec<u8>,
    pub num_values: u32,
    pub num_anchor_data: u32,
}

impl TokenToAnchorScoreBinary {
    pub fn set_scores(&mut self, id: u32, add_data: Vec<AnchorScore>) {
        //TODO INVALIDATE OLD DATA IF SET TWICE?

        let pos: usize = id as usize;
        let required_size = pos + 1;
        if self.start_pos.len() < required_size {
            self.start_pos.resize(required_size, U31_MAX);
        }

        let add_data: Vec<AnchorScoreSerialize> = add_data.iter().map(|el| AnchorScoreSerialize::new(el.id, el.score.as_bits())).collect(); //TODO CHECK WHY as_bits is needed, else deserialization fails

        let byte_offset = self.data.len() as u32;
        self.start_pos[pos] = byte_offset;

        let num_elements_as_bytes: [u8; 4] = unsafe { transmute(add_data.len() as u32) };
        self.data.extend(num_elements_as_bytes.iter());

        // let encoded: Vec<u8> = serialize(&add_data).unwrap();
        // self.data.extend(encoded.iter());

        let p = add_data.as_ptr();
        // let ptr = unsafe{std::mem::transmute::<*const (u32, u16), *const u8>(p)};
        let ptr = unsafe { std::mem::transmute::<*const AnchorScoreSerialize, *const u8>(p) };

        let add_bytes = add_data.len() * SIZE_OF_ANCHOR_SCORE;
        unsafe {
            self.data.reserve(add_bytes);
            let end_of_vec = self.data.as_mut_ptr().offset(self.data.len() as isize);
            let new_len = self.data.len() + add_bytes;
            self.data.set_len(new_len);
            ptr::copy(ptr, end_of_vec, add_bytes);
        }
    }

    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    pub fn write(&self, path_indirect: &str, path_data: &str) -> Result<(), io::Error> {
        File::create(path_indirect)?.write_all(&vec_to_bytes_u32(&self.start_pos))?;
        File::create(path_data)?.write_all(&self.data)?;
        Ok(())
    }
    pub fn read(&mut self, path_indirect: &str, path_data: &str) -> Result<(), io::Error> {
        self.start_pos = load_index_u32(&path_indirect)?;
        self.data = file_to_bytes(&path_data)?;
        Ok(())
    }
}

impl TokenToAnchorScore for TokenToAnchorScoreBinary {
    fn get_scores(&self, id: u32) -> Option<Vec<AnchorScore>> {
        if id as usize >= self.get_size() {
            return None;
        }

        let pos = self.start_pos[id as usize];
        if pos == U31_MAX {
            return None;
        }

        Some(get_achor_score_data_from_bytes(&self.data, pos))
    }

    fn get_max_id(&self) -> usize {
        self.get_size()
    }
}

#[derive(Debug)]
pub struct TokenToAnchorScoreMmap {
    pub start_pos: Mmap,
    pub data: Mmap,
    pub max_value_id: u32,
}

impl TokenToAnchorScoreMmap {
    pub fn new(start_and_end_file: &fs::File, data_file: &fs::File) -> Self {
        let start_and_end_file = unsafe { MmapOptions::new().map(&start_and_end_file).unwrap() };
        let data_file = unsafe { MmapOptions::new().map(&data_file).unwrap() };
        TokenToAnchorScoreMmap {
            start_pos: start_and_end_file,
            data: data_file,
            max_value_id: 0,
        }
    }
}

impl HeapSizeOf for TokenToAnchorScoreMmap {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl TokenToAnchorScore for TokenToAnchorScoreMmap {
    fn get_scores(&self, id: u32) -> Option<Vec<AnchorScore>> {
        if id as usize >= self.start_pos.len() / 4 {
            return None;
        }
        let pos = get_u32_from_bytes(&self.start_pos, id as usize * 4);
        if pos == U31_MAX {
            return None;
        }
        Some(get_achor_score_data_from_bytes(&self.data, pos))
    }
    fn get_max_id(&self) -> usize {
        self.start_pos.len() / 4
    }
}



fn get_achor_score_data_from_bytes(data: &[u8], pos: u32) -> Vec<AnchorScore> {
    let mut ret_data = vec![];
    unsafe {
        let num_elements: u32 = get_u32_from_bytes(&data, pos as usize);
        let num_bytes = num_elements as usize * SIZE_OF_ANCHOR_SCORE;
        ret_data.reserve(num_elements as usize);
        ret_data.set_len(num_elements as usize);
        let data_ptr_start = data.as_ptr().offset(pos as isize + SIZE_OF_NUM_ELEM as isize);

        let p = ret_data.as_mut_ptr();
        let return_data_ptr = std::mem::transmute::<*mut AnchorScore, *mut u8>(p);
        ptr::copy(data_ptr_start, return_data_ptr, num_bytes);
    }
    ret_data
}

#[test]
fn test_token_to_anchor_score_binary() {
    let mut yeps = TokenToAnchorScoreBinary::default();

    yeps.set_scores(1, vec![AnchorScore::new(1, f16::from_f32(1.0))]);

    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    yeps.set_scores(5, vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))]);
    assert_eq!(yeps.get_scores(4), None);
    assert_eq!(
        yeps.get_scores(5),
        Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    );
    assert_eq!(yeps.get_scores(6), None);

    let data = "TokenToAnchorScoreBinaryTestData";
    let indirect = "TokenToAnchorScoreBinaryTestIndirect";
    yeps.write(indirect, data).unwrap();

    // IM loaded from File
    let mut yeps = TokenToAnchorScoreBinary::default();
    yeps.read(indirect, data).unwrap();
    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    assert_eq!(yeps.get_scores(4), None);
    assert_eq!(
        yeps.get_scores(5),
        Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    );
    assert_eq!(yeps.get_scores(6), None);

    // Mmap from File
    let start_and_end_file = File::open(indirect).unwrap();
    let data_file = File::open(data).unwrap();
    let yeps = TokenToAnchorScoreMmap::new(&start_and_end_file, &data_file);
    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    assert_eq!(yeps.get_scores(4), None);
    assert_eq!(
        yeps.get_scores(5),
        Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    );
    assert_eq!(yeps.get_scores(6), None);
}
