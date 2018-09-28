use std::io::Write;
use std::u32;

use serde;

use byteorder::WriteBytesExt;
use byteorder::LittleEndian;
use vint::vint::encode_varint_into_writer;
// use vint::vint::*;
// use super::internal::SizeLimit;
use super::error::{Error, Result};
// use config::Options;


// pub fn to_string<T>(value: &T) -> Result<String>
// where
//     T: Serialize,
// {
//     let mut serializer = Serializer {
//         output: String::new(),
//     };
//     value.serialize(&mut serializer)?;
//     Ok(serializer.output)
// }


pub fn to_writer<W, T: ?Sized>(writer: W, value: &T) -> Result<()> where
    W: Write,
    T: Serialize, 
{
    let mut serializer = Serializer::new(writer);
    value.serialize(&mut serializer)?;
    Ok(())
}

use std::fs::File;
use std::io::BufWriter;
#[test]
fn testo() {
    let yop = BufWriter::new(File::create("yop").unwrap());
    to_writer(yop, &50_u32).unwrap();
    // yop
}

/// An Serializer that encodes values directly into a Writer.
///
/// The specified byte-order will impact the endianness that is
/// used during the encoding.
///
/// This struct should not be used often.
/// For most cases, prefer the `encode_into` function.
pub(crate) struct Serializer<W> {
    writer: W,
    // _options: O,
}

impl<W: Write> Serializer<W> {
    /// Creates a new Serializer with the given `Write`r.
    pub fn new(w: W) -> Serializer<W> {
        Serializer {
            writer: w,
            // _options: options,
        }
    }
}

use serde::ser::Serialize;
use serde::ser;



impl<'a, W: Write> serde::Serializer for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;
    // type SerializeSeq = Compound<'a, W>;
    // type SerializeTuple = Compound<'a, W>;
    // type SerializeTupleStruct = Compound<'a, W>;
    // type SerializeTupleVariant = Compound<'a, W>;
    // type SerializeMap = Compound<'a, W>;
    // type SerializeStruct = Compound<'a, W>;
    // type SerializeStructVariant = Compound<'a, W>;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> Result<()> {
        unimplemented!()
        // self.writer
        //     .write_u8(if v { 1 } else { 0 })
        //     .map_err(Into::into)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.writer.write_u8(v).map_err(Into::into)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.writer.write_u16::<LittleEndian>(v).map_err(Into::into)
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        encode_varint_into_writer(&mut self.writer, v).map_err(Into::into)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.writer.write_u64::<LittleEndian>(v).map_err(Into::into)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.writer.write_i8(v).map_err(Into::into)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.writer.write_i16::<LittleEndian>(v).map_err(Into::into)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.writer.write_i32::<LittleEndian>(v).map_err(Into::into)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.writer.write_i64::<LittleEndian>(v).map_err(Into::into)
    }


    fn serialize_f32(self, v: f32) -> Result<()> {
        self.writer.write_f32::<LittleEndian>(v).map_err(Into::into)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.writer.write_f64::<LittleEndian>(v).map_err(Into::into)
    }


    fn serialize_str(self, _v: &str) -> Result<()> {
        unimplemented!()
    }

    fn serialize_char(self, _c: char) -> Result<()> {
        unimplemented!()
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn serialize_none(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_some<T: ?Sized>(self, _v: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        unimplemented!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.ok_or(Error::SequenceMustHaveLength)?;
        self.serialize_u32(len as u32)?;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        unimplemented!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        // Ok(Self::SerializeStruct { ser: self })
        Ok(self)
        // unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        unimplemented!()
    }

    fn serialize_newtype_struct<T: ?Sized>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        unimplemented!()
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        unimplemented!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        unimplemented!()
    }


    fn serialize_unit(self) -> Result<()> {
        // self.output += "null";
        Ok(())
    }

    // Unit struct means a named value containing no data. Again, since there is
    // no data, map this to JSON as `null`. There is no need to serialize the
    // name in most formats.
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Ok(())
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}






impl<'a, W: Write> serde::ser::SerializeSeq for &'a mut Serializer<W> {
    // Must match the `Ok` type of the serializer.
    type Ok = ();
    // Must match the `Error` type of the serializer.
    type Error = Error;

    // Serialize a single element of the sequence.
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + serde::ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    // Close the sequence.
    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Same thing but for tuples.
impl<'a, W: Write> ser::SerializeTuple for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Same thing but for tuple structs.
impl<'a, W: Write> ser::SerializeTupleStruct for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Tuple variants are a little different. Refer back to the
// `serialize_tuple_variant` method above:
//
//    self.output += "{";
//    variant.serialize(&mut *self)?;
//    self.output += ":[";
//
// So the `end` method in this impl is responsible for closing both the `]` and
// the `}`.
impl<'a, W: Write> ser::SerializeTupleVariant for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Some `Serialize` types are not able to hold a key and value in memory at the
// same time so `SerializeMap` implementations are required to support
// `serialize_key` and `serialize_value` individually.
//
// There is a third optional method on the `SerializeMap` trait. The
// `serialize_entry` method allows serializers to optimize for the case where
// key and value are both available simultaneously. In JSON it doesn't make a
// difference so the default behavior for `serialize_entry` is fine.
impl<'a, W: Write> ser::SerializeMap for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    // The Serde data model allows map keys to be any serializable type. JSON
    // only allows string keys so the implementation below will produce invalid
    // JSON if the key serializes as something other than a string.
    //
    // A real JSON serializer would need to validate that map keys are strings.
    // This can be done by using a different Serializer to serialize the key
    // (instead of `&mut **self`) and having that other serializer only
    // implement `serialize_str` and return an error on any other data type.
    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    // It doesn't make a difference whether the colon is printed at the end of
    // `serialize_key` or at the beginning of `serialize_value`. In this case
    // the code is a bit simpler having it here.
    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a, W: Write> ser::SerializeStruct for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Similar to `SerializeTupleVariant`, here the `end` method is responsible for
// closing both of the curly braces opened by `serialize_struct_variant`.
impl<'a, W: Write> ser::SerializeStructVariant for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

