use fst;
use serde_json;
use std::io;

#[derive(Debug, Fail)]
pub enum VelociError {
    /// Placeholder error
    #[fail(display = "{:?}", _0)]
    StringError(String),
    /// The Data is corrupted
    #[fail(display = "{:?}", _0)]
    JsonError(serde_json::Error),
    #[fail(display = "Utf8Error: '{}'", _0)]
    Utf8Error(std::str::Utf8Error),
    #[fail(display = "FstError: '{:?}'", _0)]
    FstError(fst::Error),
    #[fail(display = "IoError: '{:?}'", _0)]
    Io(io::Error),
}

impl From<io::Error> for VelociError {
    fn from(error: io::Error) -> VelociError {
        VelociError::Io(error)
    }
}
impl From<fst::Error> for VelociError {
    fn from(error: fst::Error) -> VelociError {
        VelociError::FstError(error)
    }
}
impl From<std::str::Utf8Error> for VelociError {
    fn from(error: std::str::Utf8Error) -> VelociError {
        VelociError::Utf8Error(error)
    }
}
impl From<serde_json::Error> for VelociError {
    fn from(error: serde_json::Error) -> VelociError {
        VelociError::JsonError(error)
    }
}
