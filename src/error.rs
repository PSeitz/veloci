use fst;
use serde_json;
use std::io;

#[derive(Debug, Fail)]
pub enum VelociError {
    /// Placeholder error
    #[fail(display = "{:?}", _0)]
    StringError(String),
    /// Ron Sir Error
    #[fail(display = "{:?}", _0)]
    RonSerError(ron::ser::Error),
    /// The Data is corrupted
    #[fail(display = "{:?}", _0)]
    JsonError(serde_json::Error),
    #[fail(display = "{:?}", _0)]
    TomlError(toml::de::Error),
    #[fail(display = "Utf8Error: '{}'", _0)]
    Utf8Error(std::str::Utf8Error),
    #[fail(display = "FromUtf8Error: '{}'", _0)]
    FromUtf8Error(std::string::FromUtf8Error),
    #[fail(display = "FstError: '{:?}'", _0)]
    FstError(fst::Error),
    #[fail(display = "IoError: '{:?}'", _0)]
    Io(io::Error),
    #[fail(display = "Invalid Config: '{:?}'", _0)]
    InvalidConfig(String),
    #[fail(display = "Missing text_id {:?} in index {}, therefore could not load text", text_value_id, field_name)]
    MissingTextId { text_value_id: u32, field_name: String },
    #[fail(display = "field does not exist {} (fst not found)", _0)]
    FstNotFound(String),
    #[fail(display = "Plan Execution Failed, receive channel was closed or empty ")]
    PlanExecutionRecvFailed,
    #[fail(display = "Plan Execution Failed, could not send to channel ")]
    PlanExecutionSendFailed,
    #[fail(display = "Plan Execution Failed, filter channel was closed or empty ")]
    PlanExecutionRecvFailedFilter,
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
impl From<std::string::FromUtf8Error> for VelociError {
    fn from(error: std::string::FromUtf8Error) -> VelociError {
        VelociError::FromUtf8Error(error)
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
impl From<toml::de::Error> for VelociError {
    fn from(error: toml::de::Error) -> VelociError {
        VelociError::TomlError(error)
    }
}
impl From<ron::ser::Error> for VelociError {
    fn from(error: ron::ser::Error) -> VelociError {
        VelociError::RonSerError(error)
    }
}
