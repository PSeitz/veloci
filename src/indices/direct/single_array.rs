use crate::{indices::metadata::IndexValuesMetadata, persistence::*, type_info::TypeInfo};
use std::{self, io, marker::PhantomData, ptr::copy_nonoverlapping, u32};

use ownedbytes::OwnedBytes;

impl_type_info_single_templ!(SingleArrayPacked);

#[derive(Debug, Clone, Copy)]
pub(crate) enum BytesRequired {
    One = 1,
    Two,
    Three,
    Four,
}

#[inline]
pub(crate) fn get_bytes_required(mut val: u32) -> BytesRequired {
    val += val; //+1 because EMPTY_BUCKET = 0 is already reserved
    if val < 1 << 8 {
        BytesRequired::One
    } else if val < 1 << 16 {
        BytesRequired::Two
    } else if val < 1 << 24 {
        BytesRequired::Three
    } else {
        BytesRequired::Four
    }
}

#[inline]
pub(crate) fn encode_vals<O: std::io::Write>(vals: &[u32], bytes_required: BytesRequired, out: &mut O) -> Result<(), io::Error> {
    //Maximum speed, Maximum unsafe
    use std::slice;
    unsafe {
        let slice = slice::from_raw_parts(vals.as_ptr() as *const u8, std::mem::size_of_val(vals));
        let mut pos = 0;
        while pos != slice.len() {
            out.write_all(&slice[pos..pos + bytes_required as usize])?;
            pos += 4;
        }
    }
    Ok(())
}

#[inline]
#[allow(trivial_casts)]
pub(crate) fn decode_bit_packed_val<T: IndexIdToParentData>(data: &[u8], bytes_required: BytesRequired, index: usize) -> Option<T> {
    let bit_pos_start = index * bytes_required as usize;
    if bit_pos_start >= data.len() {
        None
    } else {
        let mut out = T::zero();
        unsafe {
            copy_nonoverlapping(data.as_ptr().add(bit_pos_start), &mut out as *mut T as *mut u8, bytes_required as usize);
        }
        if out == T::zero() {
            // == EMPTY_BUCKET
            None
        } else {
            Some(out - T::one())
        }
    }
}

#[test]
fn test_encodsing_and_decoding_bitpacking() {
    let vals: Vec<u32> = vec![123, 33, 545, 99];

    let bytes_required = get_bytes_required(*vals.iter().max().unwrap());

    let mut bytes = vec![];

    encode_vals(&vals, bytes_required, &mut bytes).unwrap();

    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 0), Some(122));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 1), Some(32));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 2), Some(544));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 3), Some(98));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 4), None);
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 5), None);

    let vals: Vec<u32> = vec![50001, 33];
    let bytes_required = get_bytes_required(*vals.iter().max().unwrap());
    let mut bytes = vec![];

    encode_vals(&vals, bytes_required, &mut bytes).unwrap();

    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 0), Some(50_000));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 1), Some(32));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 2), None);
}

#[derive(Debug)]
// Loads integer with flexibel widths 1, 2 or 4 byte
pub(crate) struct SingleArrayPacked<T: IndexIdToParentData> {
    pub(crate) data_file: OwnedBytes,
    #[allow(dead_code)]
    pub(crate) size: usize,
    pub(crate) metadata: IndexValuesMetadata,
    pub(crate) ok: PhantomData<T>,
    pub(crate) bytes_required: BytesRequired,
}

impl<T: IndexIdToParentData> SingleArrayPacked<T> {
    pub fn from_vec(data_file: Vec<u8>, metadata: IndexValuesMetadata) -> Self {
        Self::from_data(OwnedBytes::new(data_file), metadata)
    }

    pub fn from_data(data_file: OwnedBytes, metadata: IndexValuesMetadata) -> Self {
        let size = data_file.len() / get_bytes_required(metadata.max_value_id) as usize;

        SingleArrayPacked {
            data_file,
            size,
            metadata,
            ok: PhantomData,
            bytes_required: get_bytes_required(metadata.max_value_id),
        }
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for SingleArrayPacked<T> {
    type Output = T;

    fn get_index_meta_data(&self) -> &IndexValuesMetadata {
        &self.metadata
    }

    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    #[inline]
    fn get_value(&self, id: u64) -> Option<T> {
        decode_bit_packed_val::<T>(&self.data_file, self.bytes_required, id as usize)
    }

    #[inline]
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
    }
}
