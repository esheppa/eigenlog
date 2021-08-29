use super::*;
use futures::FutureExt;
use reqwest::header;

use std::{pin, task};

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
                sender: None,
                generator: ulid::Generator::new(),
            },
        )
    }
}

#[must_use]
pub struct DataSender {
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    api_config: ApiConfig,

    host: Host,

    app: App,

    cache_limit: CacheLimit,

    cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,

    sender: Option<pin::Pin<Box<dyn futures::Future<Output = ()>>>>,

    generator: ulid::Generator,
}

async fn send_batch_err(
    config: ApiConfig,
    host: Host,
    app: App,
    level: log::Level,
    batch: LogBatch,
) -> result::Result<(), Error> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::HeaderName::from_static(API_KEY_HEADER),
        header::HeaderValue::from_str(&config.api_key).expect("Valid header key"),
    );
    let client = reqwest::ClientBuilder::new()
        .default_headers(headers)
        .build()
        .expect("Should succeed");

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

async fn send_batch(config: ApiConfig, host: Host, app: App, level: log::Level, batch: LogBatch) {
    if let Err(e) = send_batch_err(config, host, app, level, batch).await {
        eprintln!("Error sending batch: {:?}", e);
    }
}

impl Drop for DataSender {
    fn drop(&mut self) {
        let cache = std::mem::take(&mut self.cache);
        let api_config = self.api_config.clone();
        let host = self.host.clone();
        let app = self.app.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                while let Ok(msg) = rx.recv() {
                    if let Some((level, batch)) = msg {
                        send_batch(api_config.clone(), host.clone(), app.clone(), level, batch)
                            .await;
                    } else {
                        break;
                    }
                }
            });
        });

        for (level, batch) in cache {
            let _ = tx.send(Some((level, batch)));
        }

        let _ = tx.send(None);

        let _ = handle.join();
    }
}

// We impl Future rather than using async fn's here as this allows more granular control
// on sending the batches to the remote server but still recieving new batches as fast as possible
impl futures::Future for DataSender {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.receiver.poll_recv(cx) {
            // do nothing, will will return a pending later if needed
            task::Poll::Pending => (),

            // recieved a new log message, add it to the cache
            task::Poll::Ready(Some((level, data))) => {
                let mut batch = self.cache.remove(&level).unwrap_or_default();

                batch.insert(self.generator.generate().unwrap(), data);

                self.cache.insert(level, batch);
            }
            // if channel is hung up on, complete the future
            task::Poll::Ready(None) => return task::Poll::Ready(()),
        }

        if let Some(mut fut) = self.sender.take() {
            match fut.poll_unpin(cx) {
                // current send isn't yet finished
                task::Poll::Pending => {
                    self.sender = Some(fut);
                }
                // previous send has finished, ready for the next one
                task::Poll::Ready(()) => {
                    // due to Error having the smallest associated value
                    // Error's will be sent in preference
                    let mut detached_cache = std::mem::take(&mut self.cache);
                    for (level, batch) in detached_cache.iter_mut() {
                        if self.cache_limit.should_send(*level, batch) {
                            let batch = std::mem::take(batch);
                            self.sender = Some(Box::pin(send_batch(
                                self.api_config.clone(),
                                self.host.clone(),
                                self.app.clone(),
                                *level,
                                batch,
                            )));
                            break;
                        }
                    }
                    self.cache = detached_cache;
                }
            }
        } else {
            let mut detached_cache = std::mem::take(&mut self.cache);
            for (level, batch) in detached_cache.iter_mut() {
                if self.cache_limit.should_send(*level, batch) {
                    let batch = std::mem::take(batch);
                    self.sender = Some(Box::pin(send_batch(
                        self.api_config.clone(),
                        self.host.clone(),
                        self.app.clone(),
                        *level,
                        batch,
                    )));
                    break;
                }
            }
            self.cache = detached_cache;
        }

        task::Poll::Pending
    }
}
