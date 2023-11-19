use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VelociError {
    /// Placeholder error
    #[error("{0}")]
    StringError(String),

    #[error("InvalidRequest: {message:?} ")]
    InvalidRequest { message: String },

    #[error("Field {field} not found in {all_fields:?}")]
    FieldNotFound { field: String, all_fields: Vec<String> },

    #[error("All fields filtered all_fields: {all_fields:?} filter: {filter:?}")]
    AllFieldsFiltered { all_fields: Vec<String>, filter: Option<Vec<String>> },
    /// The Data is corrupted
    #[error("{:?}", _0)]
    JsonError(serde_json::Error),
    #[error("{:?}", _0)]
    TomlError(toml::de::Error),
    #[error("Utf8Error: '{}'", _0)]
    Utf8Error(std::str::Utf8Error),
    #[error("FromUtf8Error: '{}'", _0)]
    FromUtf8Error(std::string::FromUtf8Error),
    #[error("FstError: '{:?}'", _0)]
    FstError(fst::Error),
    #[error("IoError: '{:?}'", _0)]
    Io(io::Error),
    #[error("Invalid Config: '{:?}'", _0)]
    InvalidConfig(String),
    #[error("Missing text_id {:?} in index {}, therefore could not load text", text_value_id, field_name)]
    MissingTextId { text_value_id: u32, field_name: String },
    #[error("field does not exist {} (fst not found)", _0)]
    FstNotFound(String),
    #[error("Plan Execution Failed, receive channel was closed or empty ")]
    PlanExecutionRecvFailed,
    #[error("Plan Execution Failed, could not send to channel ")]
    PlanExecutionSendFailed,
    #[error("Plan Execution Failed, filter channel was closed or empty ")]
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
