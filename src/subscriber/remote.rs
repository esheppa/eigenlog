use super::*;
use futures::FutureExt;
use futures::TryFutureExt;
use reqwest::header;

use std::pin;

impl Subscriber {
    pub fn new_remote<T>(
        on_result: Box<dyn Fn(&'static str) + Sync + Send>,
        api_config: ApiConfig<T>,
        host: Host,
        app: App,
        level: log::LevelFilter,
        cache_limit: CacheLimit,
    ) -> (Subscriber, DataSender<T>)
    where
        T: ConnectionProxy,
    {
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
pub struct DataSender<T>
where
    T: ConnectionProxy + Sync + Send + 'static,
{
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    api_config: ApiConfig<T>,

    host: Host,

    app: App,

    cache_limit: CacheLimit,

    cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,

    sender: Option<pin::Pin<Box<dyn futures::Future<Output = Result<()>>>>>,

    generator: ulid::Generator,
}

impl<T> Drop for DataSender<T>
where
    T: ConnectionProxy + Sync + Send + 'static,
{
    fn drop(&mut self) {
        eprintln!("Dropping DataSender");
        let cache = std::mem::take(&mut self.cache);
        let (tx, rx) = std::sync::mpsc::channel();

        let trace_req =
            prepare_without_batch(&self.api_config, &self.host, &self.app, log::Level::Trace);
        let debug_req =
            prepare_without_batch(&self.api_config, &self.host, &self.app, log::Level::Debug);
        let info_req =
            prepare_without_batch(&self.api_config, &self.host, &self.app, log::Level::Info);
        let warn_req =
            prepare_without_batch(&self.api_config, &self.host, &self.app, log::Level::Warn);
        let error_req =
            prepare_without_batch(&self.api_config, &self.host, &self.app, log::Level::Error);

        let serialization_format = self.api_config.serialization_format;
        let proxy = self.api_config.proxy.clone();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                while let Ok(msg) = rx.recv() {
                    if let Some((level, batch)) = msg {
                        let batch = serialization_format.serialize(batch);

                        let req = match level {
                            log::Level::Trace => trace_req.try_clone(),
                            log::Level::Debug => debug_req.try_clone(),
                            log::Level::Info => info_req.try_clone(),
                            log::Level::Warn => warn_req.try_clone(),
                            log::Level::Error => error_req.try_clone(),
                        };

                        match batch {
                            Ok(batch) => {
                                if let Some(req) = req {
                                    send_batch(req.body(batch), proxy.clone()).await;
                                }
                            }
                            Err(_) => break,
                        }
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

// // We impl Future rather than using async fn's here as this allows more granular control
// // on sending the batches to the remote server but still recieving new batches as fast as possible
impl<T> futures::Future for DataSender<T>
where
    T: ConnectionProxy + Sync + Send + 'static,
{
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = std::pin::Pin::into_inner(self);

        match this.receiver.poll_recv(cx) {
            // do nothing, will will return a pending later if needed
            task::Poll::Pending => (),

            // recieved a new log message, add it to the cache
            task::Poll::Ready(Some((level, data))) => {
                let mut batch = this.cache.remove(&level).unwrap_or_default();

                batch.insert(this.generator.generate().unwrap(), data);

                this.cache.insert(level, batch);
            }
            // if channel is hung up on, complete the future
            task::Poll::Ready(None) => return task::Poll::Ready(()),
        }

        if let Some(mut fut) = this.sender.take() {
            match fut.poll_unpin(cx) {
                // current send isn't yet finished
                task::Poll::Pending => {
                    this.sender = Some(fut);
                    return task::Poll::Pending;
                }
                // previous send has finished, ready for the next one
                task::Poll::Ready(prev_res) => {
                    if let Err(e) = prev_res {
                        eprintln!("Error during sending of log message: {}", e);
                    }
                }
            }
        }

        // due to Error having the smallest associated value
        // Error's will be sent in preference
        let mut detached_cache = std::mem::take(&mut this.cache);
        for (level, batch) in detached_cache.iter_mut() {
            if this.cache_limit.should_send(*level, batch) {
                let batch = std::mem::take(batch);
                let req = prepare_without_batch(&this.api_config, &this.host, &this.app, *level);
                let local_proxy = this.api_config.proxy.clone();
                let local_format = this.api_config.serialization_format;
                this.sender = Some(Box::pin(local_proxy.proxy(req).and_then(
                    move |r| async move {
                        r.body(local_format.serialize(&batch)?)
                            .send()
                            .await?
                            .error_for_status()?;
                        Ok(())
                    },
                )));

                break;
            }
        }
        this.cache = detached_cache;

        task::Poll::Pending
    }
}

fn prepare_without_batch<T>(
    config: &ApiConfig<T>,
    host: &Host,
    app: &App,
    level: log::Level,
) -> reqwest::RequestBuilder
where
    T: ConnectionProxy,
{
    let url = format!(
        "{base}/submit/{host}/{app}/{level}",
        base = config.base_url,
        host = host,
        app = app,
        level = level.to_string().to_lowercase()
    );

    config.client.post(url).header(
        header::CONTENT_TYPE,
        config.serialization_format.header_value(),
    )
}

async fn send_batch<T>(request: reqwest::RequestBuilder, proxy: sync::Arc<T>)
where
    T: ConnectionProxy + Sync + Send + 'static,
{
    if let Err(e) = proxy
        .proxy(request)
        .map_err(|e| e.to_string())
        .and_then(|req| req.send().map_err(|e| e.to_string()))
        .await
        .and_then(|res| res.error_for_status().map_err(|e| e.to_string()))
    {
        eprintln!("Error sending batch: {}", e);
    }
}
