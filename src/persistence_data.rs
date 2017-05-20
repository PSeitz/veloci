
use persistence::Persistence;

#[derive(Debug)]
pub struct IndexKeyValueStoreFile<'a> {
    pub path1: String,
    pub path2: String,
    pub persistence:&'a Persistence
}

impl<'a> IndexKeyValueStoreFile<'a> {
    fn new(key:&(String, String), persistence:&'a Persistence) -> Self {
        IndexKeyValueStoreFile { path1: key.0.clone(), path2: key.1.clone(), persistence }
    }
    fn get_values(&self, find: u32) -> Vec<u32> {
        let mut data:Vec<u8> = Vec::with_capacity(8);
        let mut file = self.persistence.get_file_handle(&self.path1).unwrap();// -> Result<File, io::Error>
        load_bytes(&mut data, &mut file, find as u64 *8);


        let mut result = Vec::new();
        // match self.values1.binary_search(&find) {
        //     Ok(mut pos) => {
        //         //this is not a lower_bounds search so we MUST move to the first hit
        //         while pos != 0 && self.values1[pos - 1] == find {pos-=1;}
        //         let val_len = self.values1.len();
        //         while pos < val_len && self.values1[pos] == find{
        //             result.push(self.values2[pos]);
        //             pos+=1;
        //         }
        //     },Err(_) => {},
        // }
        result
    }
}
impl<'a> HeapSizeOf for IndexKeyValueStoreFile<'a> {
    fn heap_size_of_children(&self) -> usize{self.path1.heap_size_of_children() + self.path2.heap_size_of_children() }
}


#[derive(Debug, Clone)]
struct PointingArrays {
    arr1: Vec<u64>, // offset
    arr2: Vec<u8>
}

fn to_pointing_array(keys: Vec<u32>, values: Vec<u32>) -> PointingArrays {
    let mut valids = keys.clone();
    valids.dedup();
    let mut arr1 = vec![];
    let mut arr2 = vec![];
    if valids.len() == 0 { return PointingArrays{arr1, arr2}; }

    let store = IndexKeyValueStore { values1: keys.clone(), values2: values.clone() };
    let mut offset = 0;
    for valid in valids {
        let mut vals = store.get_values(valid);
        vals.sort();
        let data = vec_to_bytes_u32(&vals); // @Temporary Add Compression
        arr1.push(offset);
        arr2.extend(data.iter().cloned());
        offset += data.len() as u64;
    }
    arr1.push(offset);
    PointingArrays{arr1, arr2}
}