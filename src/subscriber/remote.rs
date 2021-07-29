use super::*;
use futures::stream::StreamExt;
use reqwest::header;

impl Subscriber {
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
}

pub struct DataSender {
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    api_config: ApiConfig,

    host: Host,

    app: App,

    cache_limit: CacheLimit,

    cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,
}

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