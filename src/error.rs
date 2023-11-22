use std::{io::Error as IoError, sync::Arc};

use csv::Error as CsvError;
use reqwest::Error as ReqwestError;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct LemmyError {
    pub error: String,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Error {
    Reqwest(Arc<ReqwestError>),
    LoginFailed,
    InvalidRateLimit,
    GetInstancesFailed,
    InvalidUrl(String),
    InvalidParams,
    SendFailed,
    RecvFailed,
    Lemmy(LemmyError),
    LemmyBug,
    TooManyRetries,
    InstanceNotFound,
    InvalidCommentPath(String),
    ClientNotReady,
    Csv(Arc<CsvError>),
    CsvWriteFailed,
    Io(Arc<IoError>),
}

impl From<ReqwestError> for Error {
    fn from(error: ReqwestError) -> Self {
        Error::Reqwest(Arc::new(error))
    }
}

impl From<LemmyError> for Error {
    fn from(error: LemmyError) -> Self {
        Error::Lemmy(error)
    }
}

impl From<CsvError> for Error {
    fn from(error: CsvError) -> Self {
        Error::Csv(Arc::new(error))
    }
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Self {
        Error::Io(Arc::new(error))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
