// use config::Options;
use std::io::Read;

// use self::read::CompactRead;
use byteorder::ReadBytesExt;
// use internal::SizeLimit;
use super::error::{Error, Result};
use byteorder::LittleEndian;
use serde;
use serde::de::Error as DeError;
use serde::de::IntoDeserializer;
use std::io;
use vint::vint::*;

/// A Deserializer that reads bytes from a buffer.
///
/// This struct should rarely be used.
/// In most cases, prefer the `deserialize_from` function.
///
/// The ByteOrder that is chosen will impact the endianness that
/// is used to read integers out of the reader.
///
/// ```rust,ignore
/// let d = Deserializer::new(&mut some_reader, SizeLimit::new());
/// serde::Deserialize::deserialize(&mut deserializer);
/// let bytes_read = d.bytes_read();
/// ```
pub(crate) struct Deserializer<R> {
    reader: R,
}

impl<'de, R: CompactRead<'de>> Deserializer<R> {
    /// Creates a new Deserializer with a given `Read`er and a size_limit.
    pub(crate) fn new(r: R) -> Deserializer<R> {
        Deserializer { reader: r }
    }

    fn read_bytes(&mut self, _count: u64) -> Result<()> {
        unimplemented!()
        // self.options.limit().add(count)
    }

    fn read_type<T>(&mut self) -> Result<()> {
        use std::mem::size_of;
        self.read_bytes(size_of::<T>() as u64)
    }
}

macro_rules! impl_nums {
    ($ty:ty, $dser_method:ident, $visitor_method:ident, $reader_method:ident) => {
        #[inline]
        fn $dser_method<V>(self, visitor: V) -> Result<V::Value>
            where V: serde::de::Visitor<'de>,
        {
            self.read_type::<$ty>()?;
            let value = self.reader.$reader_method::<LittleEndian>()?;
            visitor.$visitor_method(value)
        }
    }
}

impl<'de, 'a, R> serde::Deserializer<'de> for &'a mut Deserializer<R>
where
    R: CompactRead<'de>,
{
    type Error = Error;

    impl_nums!(u16, deserialize_u16, visit_u16, read_u16);

    // impl_nums!(u32, deserialize_u32, visit_u32, read_u32);
    // impl_nums!(u64, deserialize_u64, visit_u64, read_u64);
    impl_nums!(i16, deserialize_i16, visit_i16, read_i16);

    impl_nums!(i32, deserialize_i32, visit_i32, read_i32);

    impl_nums!(i64, deserialize_i64, visit_i64, read_i64);

    impl_nums!(f32, deserialize_f32, visit_f32, read_f32);

    impl_nums!(f64, deserialize_f64, visit_f64, read_f64);

    #[inline]
    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::DeserializeAnyNotSupported)
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    //TODO FIXME
    #[inline]
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        // self.read_type::<u64>()?;
        visitor.visit_u64(decode_from_reader(&mut self.reader).ok_or_else(|| Error::Eof)? as u64)
    }

    #[inline]
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        // self.read_type::<u32>()?;
        visitor.visit_u32(decode_from_reader(&mut self.reader).ok_or_else(|| Error::Eof)?)
        // visitor.visit_u32(decode_from_reader_and_count(&mut self.reader).ok_or_else(||Error::Eof)?)
    }

    #[inline]
    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
        // try!(self.read_type::<u8>());
        // visitor.visit_u8(try!(self.reader.read_u8()))
    }

    #[inline]
    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_enum<V>(self, _enum: &'static str, _variants: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        impl<'de, 'a, R: 'a> serde::de::EnumAccess<'de> for &'a mut Deserializer<R>
        where
            R: CompactRead<'de>,
        {
            type Error = Error;
            type Variant = Self;

            fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
            where
                V: serde::de::DeserializeSeed<'de>,
            {
                let idx: u32 = serde::de::Deserialize::deserialize(&mut *self)?;
                let val: Result<_> = seed.deserialize(idx.into_deserializer());
                Ok((val?, self))
            }
        }

        visitor.visit_enum(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        struct Access<'a, R: Read + 'a> {
            deserializer: &'a mut Deserializer<R>,
            len: usize,
        }

        impl<'de, 'a, 'b: 'a, R: CompactRead<'de> + 'b> serde::de::SeqAccess<'de> for Access<'a, R> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: serde::de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = serde::de::DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access { deserializer: self, len })
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
        // let value: u8 = try!(serde::de::Deserialize::deserialize(&mut *self));
        // match value {
        //     0 => visitor.visit_none(),
        //     1 => visitor.visit_some(&mut *self),
        //     v => Err(ErrorKind::InvalidTagEncoding(v as usize).into()),
        // }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let len = serde::Deserialize::deserialize(&mut *self)?;

        // let len = visitor.visit_u32(decode_from_reader(&mut self.reader).ok_or_else(||Error::Eof)?).unwrap();

        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        struct Access<'a, R: Read + 'a> {
            deserializer: &'a mut Deserializer<R>,
            len: usize,
        }

        impl<'de, 'a, 'b: 'a, R: CompactRead<'de> + 'b> serde::de::MapAccess<'de> for Access<'a, R> {
            type Error = Error;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
            where
                K: serde::de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let key = serde::de::DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                    Ok(Some(key))
                } else {
                    Ok(None)
                }
            }

            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
            where
                V: serde::de::DeserializeSeed<'de>,
            {
                let value = serde::de::DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                Ok(value)
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        let len = serde::Deserialize::deserialize(&mut *self)?;

        visitor.visit_map(Access { deserializer: self, len })
    }

    fn deserialize_struct<V>(self, _name: &str, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let message = "CompactCode does not support Deserializer::deserialize_identifier";
        Err(Error::custom(message))
    }

    fn deserialize_newtype_struct<V>(self, _name: &str, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_tuple_struct<V>(self, _name: &'static str, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        let message = "CompactCode does not support Deserializer::deserialize_ignored_any";
        Err(Error::custom(message))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'de, 'a, R> serde::de::VariantAccess<'de> for &'a mut Deserializer<R>
where
    R: CompactRead<'de>,
{
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        serde::de::DeserializeSeed::deserialize(seed, self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

/// An optional Read trait for advanced CompactCode usage.
///
/// It is highly recommended to use CompactCode with `io::Read` or `&[u8]` before
/// implementing a custom `CompactRead`.
pub trait CompactRead<'storage>: io::Read {
    // /// Forwards reading `length` bytes of a string on to the serde reader.
    // fn forward_read_str<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    // where
    //     V: serde::de::Visitor<'storage>;

    /// Return the first `length` bytes of the internal byte buffer.
    fn get_byte_buffer(&mut self, length: usize) -> Result<Vec<u8>>;

    /// Forwards reading `length` bytes on to the serde reader.
    fn forward_read_bytes<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'storage>;
}

/// A CompactRead implementation for byte slices
/// NOT A PART OF THE STABLE PUBLIC API
#[doc(hidden)]
pub struct SliceReader<'storage> {
    slice: &'storage [u8],
}

/// A CompactRead implementation for io::Readers
/// NOT A PART OF THE STABLE PUBLIC API
#[doc(hidden)]
pub struct IoReader<R> {
    reader: R,
    temp_buffer: Vec<u8>,
}

impl<'storage> SliceReader<'storage> {
    /// Constructs a slice reader
    pub fn new(bytes: &'storage [u8]) -> SliceReader<'storage> {
        SliceReader { slice: bytes }
    }
}

impl<R> IoReader<R> {
    /// Constructs an IoReadReader
    pub fn new(r: R) -> IoReader<R> {
        IoReader { reader: r, temp_buffer: vec![] }
    }
}

impl<'storage> io::Read for SliceReader<'storage> {
    #[inline(always)]
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        (&mut self.slice).read(out)
    }

    #[inline(always)]
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        (&mut self.slice).read_exact(out)
    }
}

impl<R: io::Read> io::Read for IoReader<R> {
    #[inline(always)]
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.reader.read(out)
    }

    #[inline(always)]
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(out)
    }
}

impl<'storage> SliceReader<'storage> {
    #[inline(always)]
    fn unexpected_eof() -> Error {
        Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, ""))
    }
}

impl<'storage> CompactRead<'storage> for SliceReader<'storage> {
    #[inline(always)]
    fn get_byte_buffer(&mut self, length: usize) -> Result<Vec<u8>> {
        if length > self.slice.len() {
            return Err(SliceReader::unexpected_eof());
        }

        let r = &self.slice[..length];
        self.slice = &self.slice[length..];
        Ok(r.to_vec())
    }

    #[inline(always)]
    fn forward_read_bytes<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'storage>,
    {
        if length > self.slice.len() {
            return Err(SliceReader::unexpected_eof());
        }

        let r = visitor.visit_borrowed_bytes(&self.slice[..length]);
        self.slice = &self.slice[length..];
        r
    }
}

impl<R> IoReader<R>
where
    R: io::Read,
{
    fn fill_buffer(&mut self, length: usize) -> Result<()> {
        let current_length = self.temp_buffer.len();
        if length > current_length {
            self.temp_buffer.reserve_exact(length - current_length);
        }

        unsafe {
            self.temp_buffer.set_len(length);
        }

        self.reader.read_exact(&mut self.temp_buffer)?;
        Ok(())
    }
}

impl<R> CompactRead<'static> for IoReader<R>
where
    R: io::Read,
{
    // fn forward_read_str<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    // where
    //     V: serde::de::Visitor<'static>,
    // {
    //     self.fill_buffer(length)?;

    //     let string = match ::std::str::from_utf8(&self.temp_buffer[..]) {
    //         Ok(s) => s,
    //         Err(e) => return Err(::ErrorKind::InvalidUtf8Encoding(e).into()),
    //     };

    //     let r = visitor.visit_str(string);
    //     r
    // }

    fn get_byte_buffer(&mut self, length: usize) -> Result<Vec<u8>> {
        self.fill_buffer(length)?;
        Ok(::std::mem::replace(&mut self.temp_buffer, Vec::new()))
    }

    fn forward_read_bytes<V>(&mut self, length: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'static>,
    {
        self.fill_buffer(length)?;
        let r = visitor.visit_bytes(&self.temp_buffer[..]);
        r
    }
}
