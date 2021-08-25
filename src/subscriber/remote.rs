use super::*;
use futures::stream::StreamExt;
use reqwest::header;

impl Subscriber {
    pub fn new_remote(
        on_result: Box<dyn Fn(&'static str) + Sync + Send>,
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
                send_cache: Default::default(),
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

    send_cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,

}

async fn send_batch_err(
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

async fn send_batch(
    client: &reqwest::Client,
    config: &ApiConfig,
    host: &Host,
    app: &App,
    level: log::Level,
    batch: LogBatch,
) {
    if let Err(e) = send_batch_err(client, config, host, app, level, batch).await {
        eprintln!("Error sending batch: {:?}", e);
    }
}

impl DataSender {
    pub async fn run_forever<OnError>(&mut self, mut func: OnError)
    where
        OnError: FnMut(Error),
    {
        loop {
            if let Err(e) = self.run().await {
                func(e)
            }
        }
    }

    pub async fn flush(self) -> Result<()> {
        let DataSender {
            api_config,
            send_cache,
            host,
            app,
            ..
        } = self;

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&api_config.api_key)?,
        );
        let subscriber = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()?;

        let tasks = futures::stream::FuturesUnordered::new();

        for (level, batch) in send_cache {
            println!("Flushing {}: {}", level, batch.len());
            tasks.push(send_batch(&subscriber, &api_config, &host, &app, level, batch));
        }
        tasks.for_each(|_| async {}).await;
        Ok(())
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
            send_cache,
        } = self;

        let _ = flush_request; // we can ignore as we have our own flush?

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&api_config.api_key)?,
        );
        let subscriber = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()?;


        let mut generator = ulid::Generator::new();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let sub_recv = async {
            while let Some((level, data)) = receiver.recv().await {
                // eprintln!("Recieved log msg: {:?} - {:?}", level, data);

                let mut batch = cache.remove(&level).unwrap_or_default();

                batch.insert(generator.generate().unwrap(), data);

                if cache_limit.should_send(level, &batch) {
                    let _ =tx.send((level, batch));
                }
            }
        };

        let send_do = async {
            loop {
                for (level, batch) in send_cache.iter_mut() {
                    if cache_limit.should_send(*level, batch) {
                        println!("Sending batch for {}", level);
                        let batch = std::mem::take(batch);
                        send_batch(&subscriber, &api_config, &host, &app, *level, batch).await;
                    }
                }

                let recv_batch = rx.recv();

                tokio::select! {
                    Some((level, batch)) = recv_batch => {
                        send_cache.insert(level, batch);
                    }
                }
            }
        };

        futures::future::join(sub_recv, send_do).await;

        Ok(())

        // loop {
        //     let log_data = ;
        //     let flush_req = flush_request.recv();


        //     tokio::select! {
        //         Some((level, data)) = log_data => {
        //             eprintln!("Recieved log msg: {:?} - {:?}", level, data);

        //             let mut batch = cache.remove(&level).unwrap_or_default();

        //             batch.insert(generator.generate()?, data);

        //             if cache_limit.should_send(level, &batch) && tasks.is_empty() {
        //                 tasks.push(send_batch(&subscriber, &api_config, &host, &app, level, batch));
        //             }
        //         }
        //         Some(sender) = flush_req => {
        //             eprintln!("Fush req");

        //             for (level, batch) in cache.drain() {
        //                 send_batch(&subscriber, &api_config, &host, &app, level, batch).await;
        //             }
        //             sender.send(())?;
        //         }
        //         _ = tasks.next() => {
        //             println!("Tasks::next");

        //             // Do nothing. We must have this so that we drive the FuturesUnordered to completion, however.
        //         }
        //         _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
        //             println!("Wakeup");
        //             // check if tasks is back to 0 and we have batches to send

        //             for (level, batch) in cache.iter_mut() {
        //                 if cache_limit.should_send(*level, batch) && tasks.is_empty() {
        //                     let batch = std::mem::take(batch);
        //                     tasks.push(send_batch(&subscriber, &api_config, &host, &app, *level, batch));
        //                     // no point checking the rest
        //                     break;
        //                 }
        //             }
        //         }
        //     };
        // }
    }
}
