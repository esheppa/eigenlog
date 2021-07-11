// #![deny(warnings)]
#![deny(clippy::all)]
use std::{result, collections, iter};
use once_cell::sync as once_cell;

const API_KEY_HEADER: &str = "X-API-KEY";

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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Host {
    name: String,
}

static HOST_REGEX: once_cell::Lazy<regex::Regex> = once_cell::Lazy::new(|| regex::Regex::new("[A-Za-z0-9]+").unwrap());

impl std::str::FromStr for Host {
    type Err = String;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        if HOST_REGEX.is_match(s) {
            Ok(Host {
                name: s.to_string(),
            })
        } else {
            Err(format!("Hostname `{}` contains characters outisde the range of `[A-Za-z0-9]`", s))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Bincode: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Error sending flush response")]
    FlushResponse(#[from] std::sync::mpsc::SendError<()>),
    
    #[cfg(feature = "client")]
    #[error("Header: {0}")]
    Header(#[from] reqwest::header::InvalidHeaderValue),

    #[error("Missing entity with id: {0}")]
    MissingEntity(ulid::Ulid),

    #[cfg(feature = "client")]
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


#[cfg(feature = "client")]
pub use client::*;

#[cfg(feature = "client")]
mod client {

    use tokio::sync::mpsc;
    use futures::stream::StreamExt;
    use reqwest::header;
    use super::*;

    pub struct Client {
        // send a new log message
        sender: mpsc::UnboundedSender<(log::Level, LogData)>,

        // request a flush, will recieve () back when done
        // innser sender is sync as this has to be handled from sync context
        flush_requester: mpsc::UnboundedSender<std::sync::mpsc::SyncSender<()>>,

        on_result: Box<dyn Fn(result::Result<(),()>) + Sync + Send>,

        level: log::Level,
    }

    pub struct ApiConfig {
        pub base_url: String,
        pub api_key: String,
    }

    /// How many messages of a given level
    /// that we will stack up in the cache
    /// before sending a batch to the server
    pub struct CacheLimit {
        pub error: usize,
        pub warn: usize,
        pub info: usize,
        pub debug: usize,
        pub trace: usize,
    }

    impl CacheLimit {
        fn get_limit(&self, level: log::Level) -> usize {
            match level {
                log::Level::Trace => self.trace,
                log::Level::Debug => self.debug,
                log::Level::Info => self.info,
                log::Level::Warn => self.warn,
                log::Level::Error => self.error,
            }
        }
        fn should_send(&self, level: log::Level, cache: &collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>) -> bool {
            let current_len = cache.get(&level).map(|d| d.len()).unwrap_or(0);
            self.get_limit(level) <= current_len + 1
        }
    }

    impl Default for CacheLimit {
        fn default() -> CacheLimit {
            CacheLimit {
                error: 1,
                warn: 1,
                info: 10,
                debug: 100,
                trace: 100,
            }
        }
    }

    impl Client {
        fn on_result<T, E>(&self, result: result::Result<T, E>) 
        {
            let func = &self.on_result;
            func(result.map_err(|_| ()).map(|_| ()))
        }
        pub fn new(on_result: Box<dyn Fn(result::Result<(), ()>) + Sync + Send>, api_config: ApiConfig, host: Host, level: log::Level, cache_limit: CacheLimit) -> (Client, DataSender) {
            let (tx1, rx1) = mpsc::unbounded_channel();
            let (tx2, rx2) = mpsc::unbounded_channel();

            (
                Client {
                    sender: tx1,
                    flush_requester: tx2,
                    on_result,
                    level,
                },
                DataSender {
                    receiver: rx1,
                    flush_request: rx2,
                    api_config,
                    host,
                    cache_limit,
                    cache: Default::default(),
                },
            )
        }
    }

    pub struct DataSender {
        receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

        flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

        api_config: ApiConfig,

        host: Host,

        cache_limit: CacheLimit,

        cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,
    }

    async fn send_batch(client: &reqwest::Client, config: &ApiConfig, host: &Host, level: log::Level, batch: LogBatch) -> result::Result<(), Error> {
        let url = format!("{base}/submit/{host}/{level}", base = config.base_url, host = host.name, level = level.to_string().to_lowercase());
        let batch = bincode::serialize(&batch)?;
        client.post(url)
            .body(batch)
            .send().await?
            .error_for_status()?;
        Ok(())
    }

    impl DataSender {

        pub async fn run_forever<OnError>(mut self, mut func: OnError) 
        where OnError: FnMut(Error),
        {
            loop {
                if let Err(e) = self.run().await {
                    func(e)
                }
            }
        }

        pub async fn run(&mut self) -> result::Result<(), Error> {
            // each time we recieve a new log message, check the size of the cache and send if required

            let DataSender { receiver, flush_request, api_config, cache_limit, cache, host } = self;
            
            let mut headers = header::HeaderMap::new();
            headers.insert(header::HeaderName::from_static("X-API-KEY"), header::HeaderValue::from_str(&api_config.api_key)?);
            let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()?;
            
            let mut tasks = futures::stream::FuturesUnordered::new();

            loop {
                let log_data = receiver.recv();
                let flush_req = flush_request.recv();

                tokio::select! {
                    Some((level, data)) = log_data => {
                        if cache_limit.should_send(level, &cache) {

                            let mut generator = ulid::Generator::new();

                            
                            let mut batch = cache.remove(&level).unwrap_or_default();
                            batch.insert(generator.generate()?, data);

                            tasks.push(send_batch(&client, &api_config, &host, level, batch))
                        }


                    }
                    Some(sender) = flush_req => {
                        for (level, batch) in cache.drain() {
                            tasks.push(send_batch(&client, &api_config, &host, level, batch))
                        }
                        sender.send(())?;
                    }
                    Some(_) = tasks.next() => {
                        // Do nothing. We must have this so that we drive the FuturesUnordered to completion, however.
                    }
                }
            }
        } 
    }


    impl log::Log for Client {
        fn enabled(&self, metadata: &log::Metadata) -> bool {
            self.level >= metadata.level()
        }
        fn log(&self, record: &log::Record) {
            let res = self.sender.send((record.level(), record.into()));
            self.on_result(res);
        }
        fn flush(&self) {
            let (tx, rx) = std::sync::mpsc::sync_channel(0);
            let res = self.flush_requester.send(tx);
            self.on_result(res);
            let res = rx.recv();
            self.on_result(res);
        }
    } 

}

#[cfg(feature = "server")]
pub use server::*;

#[cfg(feature = "server")]
mod server {
    use std::{collections, result, convert};
    use warp::Filter;
    use super::*;
    use futures::FutureExt;

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
    pub struct QueryResponse {
        host: Host,
        level: Level,
        id: ulid::Ulid,
        data: LogData,
    }

    fn add<C: Clone + Send>(
        c: C,
    ) -> impl warp::Filter<Extract = (C,), Error = convert::Infallible> + Clone {
        warp::any().map(move || c.clone())
    }
    

    
    pub enum AppReply {
        Json(warp::reply::Json),
        Empty,
    }

    impl<T: serde::Serialize> From<T> for AppReply {
        fn from(t: T) -> AppReply {
            AppReply::Json(warp::reply::json(&t))
        }
    }

    impl warp::Reply for AppReply {
        fn into_response(self) -> warp::reply::Response {
            match self {
                AppReply::Json(j) => j.into_response(),
                AppReply::Empty => warp::http::Response::default(),
            }
        }
    }

    // make a new version for each return type?
    // need a new into_reply as well
    pub fn error_to_reply<R: Into<AppReply>>(
        maybe_err: Result<R>,
    ) -> result::Result<AppReply, convert::Infallible> {
        match maybe_err {
            Ok(r) => Ok(r.into()),
            Err(e) => Ok(e.to_reply()),
        }
    }

    impl Error {
        pub fn to_reply<'a>(self) -> AppReply {
            todo!()
        }
    }

    pub type Result<T> = result::Result<T, Error>;

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
    pub enum Level {
        Trace,
        Debug,
        Info,
        Warn,
        Error,
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

   

    async fn submit(
            host: Host,
            level: Level,
            batch: LogBatch,
            db: sled::Db,
    ) -> Result<()> {
        todo!()
    }

    async fn query(
            params: collections::HashMap<String, String>,
            db: sled::Db,
    // vec queryresponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
    ) -> Result<Vec<QueryResponse>> {
        todo!()
    }

    fn create_server(db: sled::Db, api_keys: Vec<String>) -> warp::Server<impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection>> {
    // endpoint is /logbatch/<host>/<level>
    //
    // new tree for each host/level combination
    //  
    // host must be a-zAZ09 and -_
    //
    //
    //
        let submit = 
            warp::path("submit")
            .and(warp::post())
            .and(warp::path::param()) // Host 
            .and(warp::path::param()) // Level
            .and(warp::path::end())
            .and(warp::body::json()) // LogBatch payload
            .and(add(db.clone()))
            .and_then(|host, level, batch, db| submit(host, level, batch, db).map(error_to_reply));

        let query =
            warp::path("query")
            .and(warp::get())
            .and(warp::path::end())
            .and(warp::query())
            .and(add(db.clone()))
            .and_then(|params, db| query(params, db).map(error_to_reply));

        warp::serve(submit.or(query))
    }


}
