#![deny(warnings)]
#![deny(clippy::all)]
// otherwise the feature combinations would be too messy
#![allow(dead_code)]

//! We have a seperate tree created for each combination of hostname and log level. While this is slightly less efficient when wanting to
//! view all logs between a time range for all log levels, it is beneficial from the loading side. This is because by having a seperate tree
//! we can ensure that we always insert ascending values of the index. It also ensures that we can send through different log levels
//! at different frequencies (eg per message for Errors, per n messages for Info).

use http::header;
use once_cell::sync as once_cell;
use std::{collections, error, fmt, iter, result, str, sync};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(any(feature = "remote-subscriber", feature = "local-subscriber"))]
pub mod subscriber;

#[cfg(any(feature = "server", feature = "local-subscriber", feature = "bincode"))]
pub mod db;

cfg_if::cfg_if! {
    if #[cfg(not(any(feature = "bincode", feature = "json")))] {
        compile_error!("eigenlog: must select at least one of `json` and `bincode`");
    }
}

cfg_if::cfg_if! {
    if #[cfg(not(any(feature = "client", feature = "server", feature = "remote-subscriber", feature = "local-subscriber")))] {
        compile_error!("eigenlog: must select at least one of `client`, `server` or `subscriber`");
    }
}

const API_KEY_HEADER: &str = "x-api-key";

#[cfg(feature = "json")]
const APPLICATION_JSON: &str = "application/json";

#[cfg(feature = "bincode")]
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
    #[cfg(any(feautre = "bincode", feature = "client", feature = "server"))]
    Bincode,
    #[cfg(feature = "json")]
    Json,
}

impl str::FromStr for SerializationFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            #[cfg(feature = "json")]
            APPLICATION_JSON => Ok(SerializationFormat::Json),
            #[cfg(any(feautre = "bincode", feature = "client", feature = "server"))]
            OCTET_STREAM => Ok(SerializationFormat::Bincode),
            otherwise => Err(Error::UnsupportedSerializationMimeType(
                otherwise.to_string(),
            )),
        }
    }
}

impl SerializationFormat {
    fn header_value(&self) -> header::HeaderValue {
        match self {
            #[cfg(any(feautre = "bincode", feature = "client", feature = "server"))]
            SerializationFormat::Bincode => header::HeaderValue::from_static(OCTET_STREAM),
            #[cfg(feature = "json")]
            SerializationFormat::Json => header::HeaderValue::from_static(APPLICATION_JSON),
        }
    }
    fn serialize<T>(&self, t: T) -> Result<Vec<u8>>
    where
        T: serde::Serialize,
    {
        match self {
            #[cfg(any(feautre = "bincode", feature = "client", feature = "server"))]
            SerializationFormat::Bincode => Ok(bincode_crate::serialize(&t)?),
            #[cfg(feature = "json")]
            SerializationFormat::Json => Ok(serde_json::to_vec(&t)?),
        }
    }
}

#[cfg(any(feature = "client", feature = "remote-subscriber"))]
/// This allows the user of the library to interject in each request that is made to
/// the server and add any headers, client auth, certificate auth, etc.
#[async_trait::async_trait]
pub trait ConnectionProxy: Send + Sync + Unpin {
    async fn proxy(
        self: sync::Arc<Self>,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder>;
}

#[cfg(any(feature = "client", feature = "remote-subscriber"))]
#[derive(Clone)]
pub struct BasicProxy {
    pub api_key: String,
}
impl BasicProxy {
    pub fn init(api_key: String) -> sync::Arc<BasicProxy> {
        sync::Arc::new(BasicProxy { api_key })
    }
}

#[cfg(any(feature = "client", feature = "remote-subscriber"))]
/// Example of interjecting an API key into a request
#[async_trait::async_trait]
impl ConnectionProxy for BasicProxy {
    async fn proxy(
        self: sync::Arc<Self>,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder> {
        Ok(request.header(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&self.api_key)?,
        ))
    }
}

#[cfg(any(feature = "client", feature = "remote-subscriber"))]
pub struct ApiConfig<T>
where
    T: ConnectionProxy,
{
    pub client: reqwest::Client,
    pub base_url: reqwest::Url,
    pub proxy: sync::Arc<T>,
    pub serialization_format: SerializationFormat,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TreeName {
    host: Host,
    app: App,
    level: Level,
}

impl TreeName {
    fn from_bytes(bytes: &[u8]) -> Result<TreeName> {
        let indicies = bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'-')
            .map(|t| t.0)
            .collect::<Vec<usize>>();
        if indicies.len() != 2 {
            return Err(Error::ParseTreeNameFromBytes(bytes.to_owned()));
        }
        Ok(TreeName {
            host: String::from_utf8_lossy(&bytes[..indicies[0]])
                .parse()
                .map_err(|_| Error::ParseTreeNameFromBytes(bytes.to_owned()))?,
            app: String::from_utf8_lossy(&bytes[(indicies[0] + 1)..indicies[1]])
                .parse()
                .map_err(|_| Error::ParseTreeNameFromBytes(bytes.to_owned()))?,
            level: String::from_utf8_lossy(&bytes[(indicies[1] + 1)..])
                .parse()
                .map_err(|_| Error::ParseTreeNameFromBytes(bytes.to_owned()))?,
        })
    }
}

// this is because the display impl is inefficient
impl ToString for TreeName {
    fn to_string(&self) -> String {
        self.level.get_tree_name(&self.host, &self.app)
    }
}

// empty trees will be ignored
// so we can be sure we will always have a min and max date
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct LogTreeInfo {
    pub host: Host,
    pub app: App,
    pub level: Level,
    pub min: chrono::DateTime<chrono::Utc>,
    pub max: chrono::DateTime<chrono::Utc>,
}

// empty trees will be ignored
// so we can be sure we will always have a min and max date
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct LogTreeDetail {
    pub host: Host,
    pub app: App,
    pub level: Level,
    pub rows: usize,
    pub row_detail: collections::BTreeMap<chrono::NaiveDate, usize>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct QueryParams {
    /// will only return logs equal to or more significant than this level,
    /// where `Trace` is the least significant and `Error` is the most significant
    pub max_log_level: Option<Level>,
    pub start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub end_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub host_contains: Option<Host>,
    pub app_contains: Option<App>,
    pub message_matches: Option<String>,
    pub message_not_matches: Option<String>,
    pub max_results: Option<usize>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct LogTreeDetailParams {
    /// will only return logs equal to or more significant than this level,
    /// where `Trace` is the least significant and `Error` is the most significant
    pub level: Level,
    pub host: Host,
    pub app: App,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct QueryResponse {
    pub host: Host,
    pub app: App,
    pub level: Level,
    pub id: ulid::Ulid,
    pub data: LogData,
}

#[derive(Clone, Debug)]
pub struct Host {
    name: String,
}

#[derive(Clone, Debug)]
pub struct HostParseError {
    msg: String,
}

impl serde::Serialize for Host {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.name)
    }
}

impl<'de> serde::Deserialize<'de> for Host {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let st = String::deserialize(deserializer)?;

        let host = st.parse().map_err(serde::de::Error::custom)?;

        Ok(host)
    }
}

impl AsRef<str> for Host {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

static HOST_REGEX: once_cell::Lazy<regex::Regex> =
    once_cell::Lazy::new(|| regex::Regex::new("^[A-Za-z0-9]+$").unwrap());

impl std::str::FromStr for Host {
    type Err = HostParseError;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        if HOST_REGEX.is_match(s) {
            Ok(Host {
                name: s.to_string(),
            })
        } else {
            Err(HostParseError {
                msg: format!(
                    "Host name `{}` contains characters outisde the range of `[A-Za-z0-9]`",
                    s
                ),
            })
        }
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for HostParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "host parse error: {}", self.msg)
    }
}

impl error::Error for HostParseError {}

#[derive(Clone, Debug)]
pub struct App {
    name: String,
}

#[derive(Clone, Debug)]
pub struct AppParseError {
    msg: String,
}

impl serde::Serialize for App {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.name)
    }
}

impl<'de> serde::Deserialize<'de> for App {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let st = String::deserialize(deserializer)?;

        let app = st.parse().map_err(serde::de::Error::custom)?;

        Ok(app)
    }
}

impl AsRef<str> for App {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

impl std::str::FromStr for App {
    type Err = AppParseError;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        if HOST_REGEX.is_match(s) {
            Ok(App {
                name: s.to_string(),
            })
        } else {
            Err(AppParseError {
                msg: format!(
                    "App name `{}` contains characters outisde the range of `[A-Za-z0-9]`",
                    s
                ),
            })
        }
    }
}

impl fmt::Display for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for AppParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "app parse error: {}", self.msg)
    }
}

impl error::Error for AppParseError {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Trace = 4,
    Debug = 3,
    Info = 2,
    Warn = 1,
    Error = 0,
}

impl Level {
    fn get_tree_name(&self, hostname: &Host, application: &App) -> String {
        format!("{}-{}-{}", hostname.name, application.name, self)
    }
    fn get_levels(max_level: Level) -> collections::BTreeSet<Level> {
        Level::all().filter(|l| *l <= max_level).collect()
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

    #[cfg(any(feature = "client", feature = "remote-subscriber"))]
    #[error("Header: {0}")]
    Header(#[from] reqwest::header::InvalidHeaderValue),

    #[cfg(any(feature = "client", feature = "remote-subscriber"))]
    #[error("Parse url: {0}")]
    Url(#[from] url::ParseError),

    #[error("API key `{0}` is not valid")]
    InvalidApiKey(String),

    #[error("Submission of content type `{0}` is not valid")]
    InvalidSubmissionContentType(String),

    #[error("Bytes of length {0} cannot be converted to a Ulid (16 required")]
    InvalidLengthBytesForUlid(usize),

    #[error("Missing entity with id: {0}")]
    MissingEntity(ulid::Ulid),

    #[error("Error parsing tree name from bytes: {}", String::from_utf8_lossy(.0))]
    ParseTreeNameFromBytes(Vec<u8>),

    #[cfg(feature = "reqwest")]
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[cfg(feature = "sled")]
    #[error("Sled: {0}")]
    Sled(#[from] sled::Error),

    #[error("Ulid: {0}")]
    Ulid(#[from] ulid::MonotonicError),

    #[error("Uuid: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("Unsupported serialization mime type of: {0}")]
    UnsupportedSerializationMimeType(String),

    #[cfg(feature = "server")]
    #[error("Warp: {0}")]
    Warp(#[from] warp::Error),

    #[error("Setting logger: {0}")]
    Log(#[from] log::SetLoggerError),

    #[error("Parsing regex: {0}")]
    Regex(#[from] regex::Error),

    #[error("Parse log tree info: {0}")]
    ParseLogTreeInfo(String),

    #[error("Log subscriber was closed")]
    LogSubscriberClosed,

    #[error("Custom: {0}")]
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host() {
        assert!("abc123".parse::<Host>().is_ok());
        assert!("abc-123".parse::<Host>().is_err());
    }

    #[test]
    fn test_app() {
        assert!("abc123".parse::<App>().is_ok());
        assert!("abc-123".parse::<App>().is_err());
    }
}
