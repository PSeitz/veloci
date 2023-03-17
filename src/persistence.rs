pub use crate::metadata::*;
use crate::{
    error::VelociError,
    indices::*,
    search::*,
    type_info,
    util::{self, get_file_path, *},
};
use colored::*;
use fnv::FnvHashMap;
use fst::Map;
use vint32::iterator::VintArrayIterator;

use lru_time_cache::LruCache;
use memmap::{Mmap, MmapOptions};
use num::{self, cast::ToPrimitive, Integer};
use parking_lot::RwLock;
use prettytable::{format, Table};

use std::{
    self,
    collections::HashMap,
    env, fmt,
    fmt::Debug,
    fs::{self, File},
    io::{self, prelude::*},
    marker::Sync,
    path::Path,
    str,
    str::FromStr,
    time::Duration,
    u32,
};

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
    pub doc_offsets: Option<Mmap>,
    pub key_value_stores: HashMap<String, Box<dyn IndexIdToParent<Output = u32>>>,
    pub token_to_anchor_score: HashMap<String, Box<dyn TokenToAnchorScore>>,
    pub phrase_pair_to_anchor: HashMap<String, Box<dyn PhrasePairToAnchor<Input = (u32, u32)>>>,
    pub boost_valueid_to_value: HashMap<String, Box<dyn IndexIdToParent<Output = u32>>>,
    // index_64: HashMap<String, Box<IndexIdToParent<Output = u64>>>,
    pub fst: HashMap<String, Map<memmap::Mmap>>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PersistenceType {
    /// Transient Doesn't write indices, just holds them in memory. Good for small indices with incremental updates.
    Transient,
    Persistent,
}

pub struct Persistence {
    pub db: String, // folder
    pub metadata: PeristenceMetaData,
    pub persistence_type: PersistenceType,
    pub indices: PersistenceIndices,
    pub lru_cache: HashMap<String, LruCache<RequestSearchPart, SearchResult>>,
    pub term_boost_cache: RwLock<LruCache<Vec<RequestSearchPart>, Vec<SearchFieldResult>>>,
}

impl fmt::Debug for Persistence {
    #[cfg(not(tarpaulin_include))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Persistence")
            .field("db", &self.db)
            .field("metadata", &self.metadata)
            .field("persistence_type", &self.persistence_type)
            .field("indices", &self.indices)
            .finish()
    }
}

impl FromStr for LoadingType {
    type Err = ();

    fn from_str(s: &str) -> Result<LoadingType, ()> {
        match s {
            "InMemory" => Ok(LoadingType::InMemory),
            "Disk" => Ok(LoadingType::Disk),
            _ => Err(()),
        }
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
    fn load_types_index_to_one<T: IndexIdToParentData, P: AsRef<Path> + std::fmt::Debug>(
        data_direct_path: P,
        metadata: IndexValuesMetadata,
    ) -> Result<Box<dyn IndexIdToParent<Output = u32>>, VelociError> {
        let store = SingleArrayIM::<u32, T> {
            data: decode_bit_packed_vals(&file_path_to_bytes(data_direct_path)?, get_bytes_required(metadata.max_value_id)),
            metadata,
            ok: std::marker::PhantomData,
        };
        Ok(Box::new(store))
    }

    fn load_indices(&mut self) -> Result<(), VelociError> {
        info_time!("loaded persistence {:?}", &self.db);

        let doc_offsets_file = self.get_file_handle("data.offsets")?;
        let doc_offsets_mmap = unsafe { MmapOptions::new().map(&doc_offsets_file)? };
        self.indices.doc_offsets = Some(doc_offsets_mmap);

        //ANCHOR TO SCORE
        for el in self.metadata.columns.iter().flat_map(|col| col.1.indices.iter()) {
            let indirect_path = get_file_path(&self.db, &el.path).set_ext(Ext::Indirect);
            let indirect_data_path = get_file_path(&self.db, &el.path).set_ext(Ext::Data);
            let loading_type = get_loading_type(el.loading_type)?;
            match el.index_category {
                IndexCategory::Phrase => {
                    //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
                    if el.is_empty {
                        let store = IndirectIMBinarySearch::<(u32, u32)> {
                            start_pos: vec![],
                            data: vec![],
                            metadata: el.metadata,
                        };
                        self.indices.phrase_pair_to_anchor.insert(el.path.to_string(), Box::new(store));
                        continue;
                    }

                    let store: Box<dyn PhrasePairToAnchor<Input = (u32, u32)>> = match loading_type {
                        LoadingType::Disk => Box::new(IndirectIMBinarySearchMMAP::from_path(&get_file_path(&self.db, &el.path), el.metadata)?),
                        LoadingType::InMemory => Box::new(IndirectIMBinarySearchMMAP::from_path(&get_file_path(&self.db, &el.path), el.metadata)?),
                    };
                    self.indices.phrase_pair_to_anchor.insert(el.path.to_string(), store);
                }
                IndexCategory::AnchorScore => {
                    let store: Box<dyn TokenToAnchorScore> = match loading_type {
                        LoadingType::Disk => match el.data_type {
                            DataType::U32 => Box::new(TokenToAnchorScoreVintMmap::<u32>::from_path(&indirect_path, &indirect_data_path)?),
                            DataType::U64 => Box::new(TokenToAnchorScoreVintMmap::<u64>::from_path(&indirect_path, &indirect_data_path)?),
                        },
                        LoadingType::InMemory => match el.data_type {
                            DataType::U32 => {
                                let store = TokenToAnchorScoreVintIM::<u32>::from_path(&indirect_path, &indirect_data_path)?;
                                Box::new(store)
                            }
                            DataType::U64 => {
                                let store = TokenToAnchorScoreVintIM::<u64>::from_path(&indirect_path, &indirect_data_path)?;
                                Box::new(store)
                            }
                        },
                    };
                    self.indices.token_to_anchor_score.insert(el.path.to_string(), store);
                }
                IndexCategory::Boost => {
                    match el.index_cardinality {
                        IndexCardinality::IndirectIM => {
                            // let meta = IndexValuesMetadata{max_value_id: el.metadata.max_value_id, avg_join_size:el.avg_join_size, ..Default::default()};
                            let store = IndirectMMap::from_path(&get_file_path(&self.db, &el.path), el.metadata)?;
                            self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
                        }
                        IndexCardinality::IndexIdToOneParent => {
                            let store = SingleArrayMMAPPacked::<u32>::from_file(&self.get_file_handle(&el.path)?, el.metadata)?;
                            self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
                        }
                    }
                }
                IndexCategory::KeyValue => {
                    info_time!("loaded key_value_store {:?}", &el.path);
                    let data_direct_path = get_file_path(&self.db, &el.path);

                    //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
                    if el.is_empty {
                        let store = SingleArrayIM::<u32, u32> {
                            data: vec![],
                            metadata: el.metadata,
                            ok: std::marker::PhantomData,
                        };
                        self.indices.key_value_stores.insert(el.path.to_string(), Box::new(store));
                        continue;
                    }

                    let store = match loading_type {
                        LoadingType::InMemory => match el.index_cardinality {
                            IndexCardinality::IndirectIM => {
                                let indirect_u32 = bytes_to_vec_u32(&file_path_to_bytes(&indirect_path)?);
                                let store = IndirectIM {
                                    start_pos: indirect_u32,
                                    data: file_path_to_bytes(&indirect_data_path)?,
                                    cache: LruCache::with_capacity(0),
                                    metadata: IndexValuesMetadata {
                                        max_value_id: el.metadata.max_value_id,
                                        avg_join_size: el.metadata.avg_join_size,
                                        num_values: 0,
                                        num_ids: 0,
                                    },
                                };
                                Box::new(store)
                            }
                            IndexCardinality::IndexIdToOneParent => {
                                let bytes_required = get_bytes_required(el.metadata.max_value_id) as u8;
                                if bytes_required == 1 {
                                    Self::load_types_index_to_one::<u8, _>(&data_direct_path, el.metadata)?
                                } else if bytes_required == 2 {
                                    Self::load_types_index_to_one::<u16, _>(&data_direct_path, el.metadata)?
                                } else {
                                    Self::load_types_index_to_one::<u32, _>(&data_direct_path, el.metadata)?
                                }
                            }
                        },
                        LoadingType::Disk => match el.index_cardinality {
                            IndexCardinality::IndirectIM => {
                                let meta = IndexValuesMetadata {
                                    max_value_id: el.metadata.max_value_id,
                                    avg_join_size: el.metadata.avg_join_size,
                                    ..Default::default()
                                };
                                let store: Box<dyn IndexIdToParent<Output = u32>> = Box::new(IndirectMMap::from_path(&get_file_path(&self.db, &el.path), meta)?);
                                store
                            }
                            IndexCardinality::IndexIdToOneParent => {
                                let store: Box<dyn IndexIdToParent<Output = u32>> =
                                    Box::new(SingleArrayMMAPPacked::<u32>::from_file(&self.get_file_handle(&el.path)?, el.metadata)?);
                                store
                            }
                        },
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

    pub fn load_fst(&self, path: &str) -> Result<Map<memmap::Mmap>, VelociError> {
        Map::new(self.get_mmap_handle(&(path.to_string() + ".fst"))?).map_err(|err| VelociError::StringError(format!("Could not load fst {} {:?}", path, err)))

        // In memory version
        // let mut f = self.get_file_handle(&(path.to_string() + ".fst"))?;
        // let mut buffer: Vec<u8> = Vec::new();
        // f.read_to_end(&mut buffer)?;
        // buffer.shrink_to_fit();
        // Ok(Map::from_bytes(buffer)?)
    }

    pub fn get_mmap_handle(&self, path: &str) -> Result<memmap::Mmap, VelociError> {
        let file = self.get_file_handle(path)?;
        unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|err| VelociError::StringError(format!("Could not load fst {} {:?}", path, err)))
        }
    }

    pub fn get_file_handle(&self, path: &str) -> Result<File, VelociError> {
        File::open(get_file_path(&self.db, path)).map_err(|err| VelociError::StringError(format!("Could not open {} {:?}", path, err)))
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

    pub fn get_buffered_writer(&self, path: &str) -> Result<io::BufWriter<fs::File>, io::Error> {
        use std::fs::OpenOptions;
        let file = OpenOptions::new().read(true).append(true).create(true).open(get_file_path(&self.db, path))?;
        Ok(io::BufWriter::new(file))
    }

    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        File::create(get_file_path(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    pub fn write_metadata(&self) -> Result<(), VelociError> {
        self.write_data("metaData.ron", ron::ser::to_string_pretty(&self.metadata, Default::default())?.as_bytes())?;
        self.write_data("metaData.json", serde_json::to_string_pretty(&self.metadata)?.as_bytes())?;
        Ok(())
    }

    pub fn write_data_offset<T: Clone + Copy + Debug>(&self, bytes: &[u8], data: &[T]) -> Result<(), VelociError> {
        debug_time!("Wrote data offsets with size {:?}", data.len());
        File::create(util::get_file_path(&self.db, "data.offsets"))?.write_all(bytes)?;
        info!("Wrote data offsets with size {:?}", data.len());
        trace!("{:?}", data);
        Ok(())
    }

    pub fn create(db: String) -> Result<Self, io::Error> {
        Self::create_type(db, PersistenceType::Persistent)
    }

    pub fn create_type(db: String, persistence_type: PersistenceType) -> Result<Self, io::Error> {
        if Path::new(&db).exists() {
            fs::remove_dir_all(&db)?;
        }
        fs::create_dir_all(&db)?;
        fs::create_dir(db.to_string() + "/temp")?; // for temporary index creation
        let metadata = PeristenceMetaData { ..Default::default() };
        Ok(Persistence {
            persistence_type,
            metadata,
            db,
            lru_cache: HashMap::default(),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        })
    }

    pub fn load<P: AsRef<Path>>(db: P) -> Result<Self, VelociError> {
        let metadata = PeristenceMetaData::new(db.as_ref().to_str().unwrap())?;
        let mut pers = Persistence {
            persistence_type: PersistenceType::Persistent,
            metadata,
            db: db.as_ref().to_str().unwrap().to_string(),
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
        // for (k, v) in &self.indices.token_to_anchor_score {
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

    pub fn temp_dir(&self) -> String {
        self.db.to_string() + "/temp"
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

fn load_type_from_env() -> Result<Option<LoadingType>, VelociError> {
    if let Some(val) = env::var_os("LoadingType") {
        let conv_env = val
            .clone()
            .into_string()
            .map_err(|_err| VelociError::StringError(format!("Could not convert LoadingType environment variable to utf-8: {:?}", val)))?;
        let loading_type =
            LoadingType::from_str(&conv_env).map_err(|_err| VelociError::StringError("only InMemory or Disk allowed for LoadingType environment variable".to_string()))?;
        Ok(Some(loading_type))
    } else {
        Ok(None)
    }
}

fn get_loading_type(loading_type: LoadingType) -> Result<LoadingType, VelociError> {
    let mut loading_type = loading_type;
    if let Some(val) = load_type_from_env()? {
        // Overrule Loadingtype from env
        loading_type = val;
    }
    Ok(loading_type)
}

//TODO Only LittleEndian supported currently
pub(crate) fn vec_to_bytes<T>(data: &[T]) -> Vec<u8> {
    let mut out_dat: Vec<u8> = vec_with_size_uninitialized(data.len() * std::mem::size_of::<T>());
    unsafe {
        let ptr = data.as_ptr() as *const u8;
        ptr.copy_to_nonoverlapping(out_dat.as_mut_ptr(), data.len() * std::mem::size_of::<T>());
    }
    // LittleEndian::write_u32_into(data, &mut wtr);
    out_dat
}

pub(crate) fn bytes_to_vec_u32(data: &[u8]) -> Vec<u32> {
    bytes_to_vec::<u32>(data)
}
pub(crate) fn bytes_to_vec_u64(data: &[u8]) -> Vec<u64> {
    bytes_to_vec::<u64>(data)
}
pub(crate) fn bytes_to_vec<T>(data: &[u8]) -> Vec<T> {
    let mut out_dat = vec_with_size_uninitialized(data.len() / std::mem::size_of::<T>());
    // LittleEndian::read_u64_into(&data, &mut out_dat);
    unsafe {
        let ptr = data.as_ptr() as *const T;
        ptr.copy_to_nonoverlapping(out_dat.as_mut_ptr(), data.len() / std::mem::size_of::<T>());
    }
    out_dat
}

pub(crate) fn file_path_to_bytes<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u8>, VelociError> {
    let f = get_file_handle_complete_path(s1)?;
    file_handle_to_bytes(&f)
}

pub(crate) fn file_handle_to_bytes(f: &File) -> Result<Vec<u8>, VelociError> {
    let file_size = { f.metadata()?.len() as usize };
    let mut reader = std::io::BufReader::new(f);
    let mut buffer: Vec<u8> = Vec::with_capacity(file_size + 1);
    reader.read_to_end(&mut buffer)?;
    // buffer.shrink_to_fit();
    Ok(buffer)
}

pub(crate) fn load_index_u32<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u32>, VelociError> {
    info!("Loading Index32 {:?} ", s1);
    Ok(bytes_to_vec_u32(&file_path_to_bytes(s1)?))
}

pub(crate) fn load_index_u64<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u64>, VelociError> {
    info!("Loading Index64 {:?} ", s1);
    Ok(bytes_to_vec_u64(&file_path_to_bytes(s1)?))
}

pub(crate) fn get_file_handle_complete_path<P: AsRef<Path> + std::fmt::Debug>(path: P) -> Result<File, VelociError> {
    File::open(&path).map_err(|err| VelociError::StringError(format!("Could not open {:?} {:?}", path, err)))
}
