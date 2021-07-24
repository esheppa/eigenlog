use super::*;
#[cfg(feature = "remote-subscriber")]
use futures::stream::StreamExt;

#[cfg(feature = "remote-subscriber")]
use reqwest::header;

use tokio::sync::mpsc;

pub struct Subscriber {
    // send a new log message
    sender: mpsc::UnboundedSender<(log::Level, LogData)>,

    // request a flush, will recieve () back when done
    // innser sender is sync as this has to be handled from sync context
    flush_requester: mpsc::UnboundedSender<std::sync::mpsc::SyncSender<()>>,

    on_result: Box<dyn Fn(result::Result<(), ()>) + Sync + Send>,

    level: log::Level,
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
    fn should_send(
        &self,
        level: log::Level,
        cache: &collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,
    ) -> bool {
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

impl Subscriber {
    fn on_result<T, E>(&self, result: result::Result<T, E>) {
        let func = &self.on_result;
        func(result.map_err(|_| ()).map(|_| ()))
    }
    #[cfg(feature = "remote-subscriber")]
    pub fn new_remote(
        on_result: Box<dyn Fn(result::Result<(), ()>) + Sync + Send>,
        api_config: ApiConfig,
        host: Host,
        app: App,
        level: log::Level,
        cache_limit: CacheLimit,
    ) -> (Subscriber, DataSender) {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();

        (
            Subscriber {
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
                app,
                cache_limit,
                cache: Default::default(),
            },
        )
    }
    #[cfg(feature = "local-subscriber")]
    pub fn new_local(
        on_result: Box<dyn Fn(result::Result<(), ()>) + Sync + Send>,
        host: Host,
        app: App,
        level: log::Level,
        db: sled::Db,
    ) -> (Subscriber, DataSaver) {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();

        (
            Subscriber {
                sender: tx1,
                flush_requester: tx2,
                on_result,
                level,
            },
            DataSaver {
                receiver: rx1,
                flush_request: rx2,
                host,
                app,
                db,
            },
        )
    }
}

#[cfg(feature = "local-subscriber")]
pub struct DataSaver {
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    host: Host,

    app: App,

    db: sled::Db,
}

#[cfg(feature = "remote-subscriber")]
pub struct DataSender {
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    api_config: ApiConfig,

    host: Host,

    app: App,

    cache_limit: CacheLimit,

    cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,
}

#[cfg(feature = "remote-subscriber")]
async fn send_batch(
    client: &reqwest::Client,
    config: &ApiConfig,
    host: &Host,
    app: &App,
    level: log::Level,
    batch: LogBatch,
) -> result::Result<(), Error> {
    let url = format!(
        "{base}/submit/{host}/{app}/{level}",
        base = config.base_url,
        host = host,
        app = app,
        level = level.to_string().to_lowercase()
    );
    let batch = config.serialization_format.serialize(&batch)?;

    client
        .post(url)
        .body(batch)
        .header(
            header::CONTENT_TYPE,
            config.serialization_format.header_value(),
        )
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[cfg(feature = "remote-subscriber")]
impl DataSender {
    pub async fn run_forever<OnError>(mut self, mut func: OnError)
    where
        OnError: FnMut(Error),
    {
        loop {
            if let Err(e) = self.run().await {
                func(e)
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        // each time we recieve a new log message, check the size of the cache and send if required

        let DataSender {
            receiver,
            flush_request,
            api_config,
            cache_limit,
            cache,
            host,
            app,
        } = self;

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&api_config.api_key)?,
        );
        let subscriber = reqwest::ClientBuilder::new()
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

                    tasks.push(send_batch(&subscriber, &api_config, &host, &app, level, batch))
                }


                }
                Some(sender) = flush_req => {
                    for (level, batch) in cache.drain() {
                        tasks.push(send_batch(&subscriber, &api_config, &host, &app, level, batch))
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

#[cfg(feature = "local-subscriber")]
impl DataSaver {
    pub async fn run_forever<OnError>(mut self, mut func: OnError)
    where
        OnError: FnMut(Error),
    {
        loop {
            if let Err(e) = self.run().await {
                func(e)
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let DataSaver {
            receiver,
            flush_request,
            ref db,
            ref host,
            app,
        } = self;

        loop {
            let log_data = receiver.recv();
            let flush_req = flush_request.recv();

            tokio::select! {
                Some((level, data)) = log_data => {
                    let mut generator = ulid::Generator::new();
                    let tree = db.open_tree(Level::from(level).get_tree_name(host, app))?;
                    tree.insert(
                        u128::from(generator.generate()?).to_be_bytes(),
                        bincode_crate::serialize(&data)?,
                    )?;
                }
                Some(sender) = flush_req => {
                    for level in Level::all() {
                        let tree = db.open_tree(level.get_tree_name(host, app))?;
                        tree.flush()?;
                    }
                    sender.send(())?;
                }
            }
        }
    }
}

impl log::Log for Subscriber {
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
