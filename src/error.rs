use std::{io::Error as IoError, sync::Arc, fmt::Display};

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
    ClientNotReady,
    Csv(Arc<CsvError>),
    CsvWriteFailed,
    Io(Arc<IoError>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Error::Reqwest(error) => write!(f, "Reqwest error: {}", error),
            Error::LoginFailed => write!(f, "Login failed"),
            Error::InvalidRateLimit => write!(f, "Invalid rate limit"),
            Error::GetInstancesFailed => write!(f, "Get instances failed"),
            Error::InvalidUrl(url) => write!(f, "Invalid URL: {}", url),
            Error::InvalidParams => write!(f, "Invalid params"),
            Error::SendFailed => write!(f, "Send failed"),
            Error::RecvFailed => write!(f, "Recv failed"),
            Error::Lemmy(error) => write!(f, "Lemmy error: {}", error.error),
            Error::LemmyBug => write!(f, "Lemmy bug"),
            Error::TooManyRetries => write!(f, "Too many retries"),
            Error::InstanceNotFound => write!(f, "Instance not found"),
            Error::ClientNotReady => write!(f, "Client not ready"),
            Error::Csv(error) => write!(f, "CSV error: {}", error),
            Error::CsvWriteFailed => write!(f, "CSV write failed"),
            Error::Io(error) => write!(f, "IO error: {}", error),
        }
    }
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
