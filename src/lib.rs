#![deny(warnings)]
#![deny(clippy::all)]

//! We have a seperate tree created for each combination of hostname and log level. While this is slightly less efficient when wanting to
//! view all logs between a time range for all log levels, it is beneficial from the loading side. This is because by having a seperate tree
//! we can ensure that we always insert ascending values of the index. It also ensures that we can send through different log levels
//! at different frequencies (eg per message for Errors, per n messages for Info).

use once_cell::sync as once_cell;
use std::{collections, fmt, iter, result};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "subscriber")]
pub mod subscriber;

cfg_if::cfg_if! {
    if #[cfg(not(any(feature = "bincode", feature = "json")))] {
        compile_error!("eigenlog: must select at least one of `json` and `bincode`")
    }
}

cfg_if::cfg_if! {
    if #[cfg(not(any(feature = "client", feature = "server", feature = "subscriber")))] {
        compile_error!("eigenlog: must select at least one of `client`, `server` or `subscriber`")
    }
}

cfg_if::cfg_if! {
    if #[cfg(any(feature = "client", feature = "subscriber"))] {
        use reqwest::header;
    } else if #[cfg(feature = "server")] {
        use warp::header;
    }
}

const API_KEY_HEADER: &str = "X-API-KEY";
const APPLICATION_JSON: &str = "application/json";
const OCTET_STREAM: &str = "application/octet-stream";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct LogData {
    pub message: String,
    pub code_module: Option<String>,
    pub code_line: Option<u32>,
    pub code_file: Option<String>,
    pub tags: collections::HashMap<String, String>,
}

impl<'a> From<&log::Record<'a>> for LogData {
    fn from(log: &log::Record<'a>) -> LogData {
        LogData {
            code_file: log.file().map(ToString::to_string),
            code_line: log.line(),
            code_module: log.module_path().map(ToString::to_string),
            message: log.args().to_string(),
            tags: iter::once(("target".to_string(), log.target().to_string())).collect(),
        }
    }
}

type LogBatch = collections::BTreeMap<ulid::Ulid, LogData>;

#[derive(Clone, Debug, Copy)]
pub enum SerializationFormat {
    #[cfg(feautre = "bincode")]
    Bincode,
    #[cfg(feature = "json")]
    Json,
}

impl SerializationFormat {
    fn header_value(&self) -> header::HeaderValue {
        match self {
            #[cfg(feautre = "bincode")]
            SerializationFormat::Bincode => header::HeaderValue::from_static(OCTET_STREAM),
            #[cfg(feature = "json")]
            SerializationFormat::Json => header::HeaderValue::from_static(APPLICATION_JSON), 
            _ => unreachable!(),
        }
    }
}

#[cfg(any(feature = "client", feature = "subscriber"))]
pub struct ApiConfig {
    pub base_url: String,
    pub api_key: String,
    pub serialization_format: SerializationFormat,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TreeName {
    host: Host,
    app: App,
    level: Level,
}

#[cfg(any(feature = "client", feature = "subscriber"))]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct QueryParams {
    /// will only return logs equal to or more significant than this level,
    /// where `Trace` is the least significant and `Error` is the most significant
    max_log_level: Option<Level>,

    start_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    end_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    host_contains: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct QueryResponse {
    host: Host,
    app: App,
    level: Level,
    id: ulid::Ulid,
    data: LogData,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Host {
    name: String,
}

static HOST_REGEX: once_cell::Lazy<regex::Regex> =
    once_cell::Lazy::new(|| regex::Regex::new("[A-Za-z0-9]+").unwrap());

impl std::str::FromStr for Host {
    type Err = String;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        if HOST_REGEX.is_match(s) {
            Ok(Host {
                name: s.to_string(),
            })
        } else {
            Err(format!(
                "Host name `{}` contains characters outisde the range of `[A-Za-z0-9]`",
                s
            ))
        }
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct App {
    name: String,
}

impl std::str::FromStr for App {
    type Err = String;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        if HOST_REGEX.is_match(s) {
            Ok(App {
                name: s.to_string(),
            })
        } else {
            Err(format!(
                "App name `{}` contains characters outisde the range of `[A-Za-z0-9]`",
                s
            ))
        }
    }
}

impl fmt::Display for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Trace = 4,
    Debug = 3,
    Info = 2,
    Warn = 1,
    Error = 0,
}

impl Level {
    fn get_tree_name(&self, hostname: &Host) -> String {
        format!("{}-{}", hostname.name, self)
    }
    fn get_levels(max_level: Level) -> collections::BTreeSet<Level> {
        Level::all()
            .filter(|l| *l <= max_level)
            .collect()
    }
    fn all() -> impl Iterator<Item = Level> {
        IntoIterator::into_iter([
            Level::Trace,
            Level::Debug,
            Level::Info,
            Level::Warn,
            Level::Error,    
        ])
    }
}

impl From<log::Level> for Level {
    fn from(lvl: log::Level) -> Self {
        match lvl {
            log::Level::Trace => Level::Trace,
            log::Level::Debug => Level::Debug,
            log::Level::Info => Level::Info,
            log::Level::Warn => Level::Warn,
            log::Level::Error => Level::Error,
        }
    }
}

impl std::str::FromStr for Level {
    type Err = String;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_ref() {
            "trace" => Level::Trace,
            "debug" => Level::Debug,
            "info" => Level::Info,
            "warn" => Level::Warn,
            "error" => Level::Error,
            otherwise => return Err(format!("Unexpected logging level `{}`", otherwise)),
        })
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Level::Trace => write!(f, "trace"),
            Level::Debug => write!(f, "debug"),
            Level::Info => write!(f, "info"),
            Level::Warn => write!(f, "warn"),
            Level::Error => write!(f, "error"),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[cfg(feature = "bincode")]
    #[error("Bincode: {0}")]
    Bincode(#[from] bincode_crate::Error),

    #[cfg(feature = "json")]
    #[error("Serde Json: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Error sending flush response")]
    FlushResponse(#[from] std::sync::mpsc::SendError<()>),

    #[cfg(any(feature = "client", feature = "subscriber"))]
    #[error("Header: {0}")]
    Header(#[from] reqwest::header::InvalidHeaderValue),

    #[error("API key `{0}` is not valid")]
    InvalidApiKey(String),

    #[error("Submission of content type `{0}` is not valid")]
    InvalidSubmissionContentType(String),

    #[error("Missing entity with id: {0}")]
    MissingEntity(ulid::Ulid),

    #[cfg(any(feature = "client", feature = "subscriber"))]
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[cfg(feature = "server")]
    #[error("Sled: {0}")]
    Sled(#[from] sled::Error),

    #[error("Ulid: {0}")]
    Ulid(#[from] ulid::MonotonicError),

    #[error("Uuid: {0}")]
    Uuid(#[from] uuid::Error),

    #[cfg(feature = "server")]
    #[error("Warp: {0}")]
    Warp(#[from] warp::Error),
}
