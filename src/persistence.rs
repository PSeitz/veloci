pub use crate::metadata::*;
use crate::{
    directory::{load_data_pair, Directory, MmapDirectory, RamDirectory},
    error::VelociError,
    indices::*,
    search::*,
    type_info,
    util::*,
};
use colored::*;
use fnv::FnvHashMap;
use fst::Map;
use ownedbytes::OwnedBytes;
use vint32::iterator::VintArrayIterator;

use lru_time_cache::LruCache;
use num::{self, cast::ToPrimitive, Integer};
use parking_lot::RwLock;
use prettytable::{format, Table};

use std::{self, collections::HashMap, fmt, fmt::Debug, io, marker::Sync, path::Path, str, time::Duration, u32};

pub const TOKENS_TO_TEXT_ID: &str = ".tokens_to_text_id";
pub const TEXT_ID_TO_TOKEN_IDS: &str = ".text_id_to_token_ids";
pub const TO_ANCHOR_ID_SCORE: &str = ".to_anchor_id_score";
pub const PHRASE_PAIR_TO_ANCHOR: &str = ".phrase_pair_to_anchor";
pub const VALUE_ID_TO_PARENT: &str = ".value_id_to_parent";
pub const PARENT_TO_VALUE_ID: &str = ".parent_to_value_id";
pub const TEXT_ID_TO_ANCHOR: &str = ".text_id_to_anchor";
// pub const PARENT_TO_TEXT_ID: &str = ".parent_to_text_id";
pub const ANCHOR_TO_TEXT_ID: &str = ".anchor_to_text_id";
pub const BOOST_VALID_TO_VALUE: &str = ".boost_valid_to_value";
pub const VALUE_ID_TO_ANCHOR: &str = ".value_id_to_anchor";
pub const TOKEN_VALUES: &str = ".token_values";

pub const TEXTINDEX: &str = ".textindex";

pub static INDEX_FILE_ENDINGS: &[&str] = &[
    TOKENS_TO_TEXT_ID,
    TEXT_ID_TO_TOKEN_IDS,
    TO_ANCHOR_ID_SCORE,
    PHRASE_PAIR_TO_ANCHOR,
    VALUE_ID_TO_PARENT,
    PARENT_TO_VALUE_ID,
    TEXT_ID_TO_ANCHOR,
    ANCHOR_TO_TEXT_ID,
    BOOST_VALID_TO_VALUE,
    VALUE_ID_TO_ANCHOR,
    TOKEN_VALUES,
];

#[derive(Debug, Default)]
pub struct PersistenceIndices {
    pub key_value_stores: HashMap<String, Box<dyn IndexIdToParent<Output = u32>>>,
    pub token_to_anchor_score: HashMap<String, Box<dyn TokenToAnchorScore>>,
    pub phrase_pair_to_anchor: HashMap<String, Box<dyn PhrasePairToAnchor<Input = (u32, u32)>>>,
    pub boost_valueid_to_value: HashMap<String, Box<dyn IndexIdToParent<Output = u32>>>,
    // index_64: HashMap<String, Box<IndexIdToParent<Output = u64>>>,
    pub fst: HashMap<String, Map<OwnedBytes>>,
}

pub struct Persistence {
    pub directory: Box<dyn Directory>, // folder
    pub metadata: PeristenceMetaData,
    pub indices: PersistenceIndices,
    pub lru_cache: HashMap<String, LruCache<RequestSearchPart, SearchResult>>,
    pub term_boost_cache: RwLock<LruCache<Vec<RequestSearchPart>, Vec<SearchFieldResult>>>,
}

impl fmt::Debug for Persistence {
    #[cfg(not(tarpaulin_include))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Persistence").field("metadata", &self.metadata).field("indices", &self.indices).finish()
    }
}

pub trait IndexIdToParentData: Integer + Clone + num::NumCast + Debug + Sync + Send + Copy + ToPrimitive + std::hash::Hash + 'static {}
impl<T> IndexIdToParentData for T where T: Integer + Clone + num::NumCast + Debug + Sync + Send + Copy + ToPrimitive + std::hash::Hash + 'static {}

pub trait TokenToAnchorScore: Debug + Sync + Send + type_info::TypeInfo {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter<'_>;
}

pub trait PhrasePairToAnchor: Debug + 'static + Sync + Send {
    type Input: Debug;
    fn get_values(&self, id: Self::Input) -> Option<Vec<u32>>;
}

#[derive(Debug, Clone)]
pub struct VintArrayIteratorOpt<'a> {
    pub(crate) single_value: i64,
    pub(crate) iter: std::boxed::Box<VintArrayIterator<'a>>,
}

impl<'a> VintArrayIteratorOpt<'a> {
    pub fn from_single_val(val: u32) -> Self {
        VintArrayIteratorOpt {
            single_value: i64::from(val),
            iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
        }
    }

    pub fn empty() -> Self {
        VintArrayIteratorOpt {
            single_value: -2,
            iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
        }
    }

    pub fn from_slice(data: &'a [u8]) -> Self {
        VintArrayIteratorOpt {
            single_value: -1,
            iter: Box::new(VintArrayIterator::from_serialized_vint_array(data)),
        }
    }
}

impl<'a> Iterator for VintArrayIteratorOpt<'a> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.single_value == -2 {
            None
        } else if self.single_value == -1 {
            self.iter.next()
        } else {
            let tmp = self.single_value;
            self.single_value = -2;
            Some(tmp as u32)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

// impl<'a> FusedIterator for VintArrayIteratorOpt<'a> {}

pub trait IndexIdToParent: Debug + Sync + Send + type_info::TypeInfo {
    type Output: IndexIdToParentData;

    fn get_values_iter(&self, _id: u64) -> VintArrayIteratorOpt<'_>;

    fn get_index_meta_data(&self) -> &IndexValuesMetadata;

    fn get_values(&self, id: u64) -> Option<Vec<Self::Output>>;

    #[inline]
    fn append_values_for_ids(&self, ids: &[u32], vec: &mut Vec<Self::Output>) {
        for id in ids {
            if let Some(vals) = self.get_values(u64::from(*id)) {
                vec.reserve(vals.len());
                for id in vals {
                    vec.push(id);
                }
            }
        }
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], _top: Option<u32>) -> FnvHashMap<Self::Output, usize> {
        let mut hits = FnvHashMap::default();
        for id in ids {
            if let Some(vals) = self.get_values(u64::from(*id)) {
                for id in vals {
                    let stat = hits.entry(id).or_insert(0);
                    *stat += 1;
                }
            }
        }
        hits
    }

    #[inline]
    fn get_value(&self, id: u64) -> Option<Self::Output> {
        self.get_values(id).map(|el| el[0])
    }
}

#[cfg(not(tarpaulin_include))]
pub fn trace_index_id_to_parent<T: IndexIdToParentData>(val: &dyn IndexIdToParent<Output = T>) {
    if log_enabled!(log::Level::Trace) {
        let meta = val.get_index_meta_data();
        // let keys = val.get_keys();
        for key in [0; 100].iter().enumerate().map(|(i, _el)| i).take(meta.num_ids as usize) {
            if let Some(vals) = val.get_values(num::cast(key).unwrap()) {
                let to = std::cmp::min(vals.len(), 100);
                trace!("key {:?} to {:?}", key, &vals[0..to]);
            }
        }
    }
}

pub fn get_readable_size(value: usize) -> ColoredString {
    match value {
        0..=1_000 => format!("{:?} b", value).blue(),
        1_001..=1_000_000 => format!("{:?} kb", value / 1_000).green(),
        _ => format!("{:?} mb", value / 1_000_000).red(),
    }
}

impl Persistence {
    fn load_indices(&mut self) -> Result<(), VelociError> {
        info_time!("loaded persistence");

        //ANCHOR TO SCORE
        for el in self.metadata.columns.iter().flat_map(|col| col.1.indices.iter()) {
            match el.index_category {
                IndexCategory::Phrase => {
                    //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
                    if el.is_empty {
                        let store = IndirectIMBinarySearchIM::<(u32, u32)> {
                            start_pos: vec![],
                            data: vec![],
                            metadata: el.metadata,
                        };
                        self.indices.phrase_pair_to_anchor.insert(el.path.to_string(), Box::new(store));
                        continue;
                    }

                    let (ind, data) = load_data_pair(&self.directory, Path::new(&el.path))?;
                    let store = Box::new(IndirectIMBinarySearch::from_data(ind, data, el.metadata)?);

                    self.indices.phrase_pair_to_anchor.insert(el.path.to_string(), store);
                }
                IndexCategory::AnchorScore => {
                    let (indirect_data, data) = load_data_pair(&self.directory, Path::new(&el.path))?;
                    let store: Box<dyn TokenToAnchorScore> = {
                        match el.data_type {
                            DataType::U32 => Box::new(TokenToAnchorScoreVint::<u32>::from_data(indirect_data, data)?),
                            DataType::U64 => Box::new(TokenToAnchorScoreVint::<u64>::from_data(indirect_data, data)?),
                        }
                    };
                    self.indices.token_to_anchor_score.insert(el.path.to_string(), store);
                }
                IndexCategory::Boost => {
                    match el.index_cardinality {
                        IndexCardinality::MultiValue => {
                            // let meta = IndexValuesMetadata{max_value_id: el.metadata.max_value_id, avg_join_size:el.avg_join_size, ..Default::default()};
                            let (ind, data) = load_data_pair(&self.directory, Path::new(&el.path))?;
                            let store = Indirect::from_data(ind, data, el.metadata)?;
                            self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
                        }
                        IndexCardinality::SingleValue => {
                            let data = self.directory.get_file_bytes(Path::new(&el.path))?;
                            let store = SingleArrayPacked::<u32>::from_data(data, el.metadata);
                            self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
                        }
                    }
                }
                IndexCategory::KeyValue => {
                    info_time!("loaded key_value_store {:?}", &el.path);

                    //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
                    if el.is_empty {
                        let store = SingleArrayPacked::from_vec(Vec::new(), el.metadata);
                        self.indices.key_value_stores.insert(el.path.to_string(), Box::new(store));
                        continue;
                    }

                    let store = {
                        match el.index_cardinality {
                            IndexCardinality::MultiValue => {
                                let meta = IndexValuesMetadata {
                                    max_value_id: el.metadata.max_value_id,
                                    avg_join_size: el.metadata.avg_join_size,
                                    ..Default::default()
                                };
                                let (ind, data) = load_data_pair(&self.directory, Path::new(&el.path))?;
                                let store: Box<dyn IndexIdToParent<Output = u32>> = Box::new(Indirect::from_data(ind, data, meta)?);
                                store
                            }
                            IndexCardinality::SingleValue => {
                                let data = self.directory.get_file_bytes(Path::new(&el.path))?;
                                let store: Box<dyn IndexIdToParent<Output = u32>> = Box::new(SingleArrayPacked::<u32>::from_data(data, el.metadata));
                                store
                            }
                        }
                    };

                    self.indices.key_value_stores.insert(el.path.to_string(), store);
                }
            }
        }

        self.load_all_fst()?;
        Ok(())
    }

    pub fn load_all_fst(&mut self) -> Result<(), VelociError> {
        for (column_name, _) in self.metadata.columns.iter().filter(|(_, info)| info.has_fst) {
            let path = column_name.add(TEXTINDEX);
            let map = self.load_fst(&path)?;
            self.indices.fst.insert(path, map);
        }
        Ok(())
    }

    pub fn load_fst(&self, path: &str) -> Result<Map<OwnedBytes>, VelociError> {
        let bytes = self.directory.get_file_bytes(&PathBuf::from(path.to_string()).set_ext(Ext::Fst))?;
        Map::new(bytes).map_err(|err| VelociError::StringError(format!("Could not load fst {} {:?}", path, err)))
    }

    pub fn get_file_bytes(&self, path: &str) -> Result<OwnedBytes, VelociError> {
        let bytes = self.directory.get_file_bytes(Path::new(path))?;
        Ok(bytes)
    }

    pub fn get_boost(&self, path: &str) -> Result<&dyn IndexIdToParent<Output = u32>, VelociError> {
        self.indices.boost_valueid_to_value.get(path).map(|el| el.as_ref()).ok_or_else(|| path_not_found(path))
    }

    pub fn has_index(&self, path: &str) -> bool {
        self.indices.key_value_stores.contains_key(path)
    }

    pub fn get_token_to_anchor<S: AsRef<str>>(&self, path: S) -> Result<&dyn TokenToAnchorScore, VelociError> {
        let path = path.as_ref().add(TO_ANCHOR_ID_SCORE);
        self.indices
            .token_to_anchor_score
            .get(&path)
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    pub fn has_token_to_anchor<S: AsRef<str>>(&self, path: S) -> bool {
        let path = path.as_ref().add(TO_ANCHOR_ID_SCORE);
        self.indices.token_to_anchor_score.contains_key(&path)
    }

    pub fn get_phrase_pair_to_anchor<S: AsRef<str>>(&self, path: S) -> Result<&dyn PhrasePairToAnchor<Input = (u32, u32)>, VelociError> {
        self.indices
            .phrase_pair_to_anchor
            .get(path.as_ref())
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    pub fn get_valueid_to_parent<S: AsRef<str>>(&self, path: S) -> Result<&dyn IndexIdToParent<Output = u32>, VelociError> {
        self.indices
            .key_value_stores
            .get(path.as_ref())
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    pub fn get_number_of_documents(&self) -> u64 {
        self.metadata.num_docs
    }

    pub fn get_bytes_indexed(&self) -> u64 {
        self.metadata.bytes_indexed
    }

    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        self.directory.write(Path::new(path), data)?;
        Ok(())
    }

    pub fn write_metadata(&self) -> Result<(), VelociError> {
        self.write_data("metaData.ron", ron::ser::to_string_pretty(&self.metadata, Default::default())?.as_bytes())?;
        self.write_data("metaData.json", serde_json::to_string_pretty(&self.metadata)?.as_bytes())?;
        Ok(())
    }

    /// Creates a new persistence instance with provided directory.
    /// The persistence is empty and can be used to index data
    pub fn create(directory: Box<dyn Directory>) -> Result<Self, io::Error> {
        let metadata = PeristenceMetaData { ..Default::default() };
        Ok(Persistence {
            directory,
            metadata,
            lru_cache: HashMap::default(),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        })
    }

    /// Creates a new persistence instance in-memory
    /// The persistence is empty and can be used to index data
    pub fn create_im() -> Result<Self, io::Error> {
        Self::create(Box::new(RamDirectory::create()))
    }

    /// Creates a new persistence instance mmaped to the folder.
    /// The persistence is empty and can be used to index data
    pub fn create_mmap(db_folder: String) -> Result<Self, io::Error> {
        Self::create(Box::new(MmapDirectory::create(db_folder.as_ref())?))
    }

    pub fn load<P: AsRef<Path>>(db: P) -> Result<Self, VelociError> {
        let directory: Box<dyn Directory> = Box::new(MmapDirectory::open(db.as_ref())?);
        Self::open(directory)
    }

    pub fn open(directory: Box<dyn Directory>) -> Result<Self, VelociError> {
        let metadata = PeristenceMetaData::new(&directory)?;
        let mut pers = Persistence {
            directory,
            metadata,
            lru_cache: HashMap::default(),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        };
        pers.load_indices()?;
        pers.print_heap_sizes();
        Ok(pers)
    }

    pub fn print_heap_sizes(&self) {
        // info!(
        //     "indices.key_value_stores {}",
        //     get_readable_size(self.indices.key_value_stores.heap_size_of_children()) // get_readable_size_for_children(&self.indices.key_value_stores)
        // );
        // info!("indices.boost_valueid_to_value {}", get_readable_size_for_children(&self.indices.boost_valueid_to_value));
        // info!("indices.token_to_anchor_score {}", get_readable_size_for_children(&self.indices.token_to_anchor_score));
        info!("indices.fst {}", get_readable_size(self.get_fst_sizes()));
        info!("------");
        // let total_size = self.get_fst_sizes()
        //     + self.indices.key_value_stores.heap_size_of_children()
        //     + self.indices.boost_valueid_to_value.heap_size_of_children()
        //     + self.indices.token_to_anchor_score.heap_size_of_children();

        // info!("totale size {}", get_readable_size(total_size));

        let mut print_and_size = vec![];
        // for (k, v) in &self.indices.key_value_stores {
        //     print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        // }
        for (k, v) in &self.indices.fst {
            print_and_size.push((v.as_fst().size(), "FST".to_string(), k));
        }
        // Sort by size
        print_and_size.sort_by_key(|row| row.0);

        // Create the table
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.add_row(row!["Type", "Path", "Size"]);
        for row in print_and_size {
            table.add_row(row![row.1, row.2, get_readable_size(row.0)]);
        }

        info!("{}", table);
    }

    fn get_fst_sizes(&self) -> usize {
        self.indices.fst.values().map(|v| v.as_fst().size()).sum()
    }
}

fn path_not_found(path: &str) -> VelociError {
    let error = format!("Did not found path in indices {}", path);
    error!("{:?}", error);
    VelociError::StringError(error)
}

//TODO Only LittleEndian supported currently
pub(crate) fn vec_to_bytes<T>(data: &[T]) -> Vec<u8> {
    let mut out_dat: Vec<u8> = vec_with_size_uninitialized(std::mem::size_of_val(data));
    unsafe {
        let ptr = data.as_ptr() as *const u8;
        ptr.copy_to_nonoverlapping(out_dat.as_mut_ptr(), std::mem::size_of_val(data));
    }
    // LittleEndian::write_u32_into(data, &mut wtr);
    out_dat
}
