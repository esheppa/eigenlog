use super::*;
use futures::{channel::mpsc, future, StreamExt, TryFutureExt};
use reqwest::header;
use std::{ops, pin};

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
        let (tx1, rx1) = mpsc::unbounded();
        let (tx2, rx2) = mpsc::unbounded();

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

        for (level, logs) in cache {
            for (id, data) in logs {
                eprintln!("[{} {}]: {}", id.datetime(), level, data.message)
            }
        }
    }
}

impl<T> DataSender<T>
where
    T: ConnectionProxy + Sync + Send + 'static,
{
    pub async fn run(mut self) {
        // get message and try to send
        // always select on both and do the next
        loop {
            if let ops::ControlFlow::Break(()) = self.run_once().await {
                eprintln!("DataSender<T>'s channel has been hung up, exiting...");
                break;
            }
        }
    }

    fn add_to_cache(&mut self, msg: Option<(log::Level, LogData)>) -> Option<()> {
        if let Some((level, data)) = msg {
            let mut batch = self.cache.remove(&level).unwrap_or_default();

            batch.insert(self.generator.generate().unwrap(), data);

            self.cache.insert(level, batch);
            None
        } else {
            Some(())
        }
    }

    async fn run_once(&mut self) -> ops::ControlFlow<()> {
        // check if we have an existing send future that needs polling
        match self.sender.take() {
            // if we have an existing send future, poll it while also polling the receiver
            // this ensures that the send is progressed, while also receiving new log messages
            Some(fut) => match future::select(Box::pin(self.receiver.next()), fut).await {
                future::Either::Left((msg, send_fut)) => {
                    // sender is still in progress, so place it back to be polled again
                    self.sender = Some(send_fut);

                    if let Some((level, data)) = msg {
                        let mut batch = self.cache.remove(&level).unwrap_or_default();

                        batch.insert(self.generator.generate().unwrap(), data);

                        self.cache.insert(level, batch);
                        // once we have added to the cache we want to exit the fn
                        // and wait again on ether the log message or send completing.
                        return ops::ControlFlow::Continue(());
                    } else {
                        return ops::ControlFlow::Break(());
                    }
                }
                future::Either::Right((completed_send, _)) => {
                    if let Err(e) = completed_send {
                        eprintln!("Error during sending of log message: {}", e);
                    }
                }
            },
            None => {
                if let Some((level, data)) = self.receiver.next().await {
                    let mut batch = self.cache.remove(&level).unwrap_or_default();

                    batch.insert(self.generator.generate().unwrap(), data);

                    self.cache.insert(level, batch);
                } else {
                    return ops::ControlFlow::Break(());
                }
            }
        }

        let mut detached_cache = std::mem::take(&mut self.cache);
        for (level, batch) in detached_cache.iter_mut() {
            if self.cache_limit.should_send(*level, batch) {
                let batch = std::mem::take(batch);
                let req = prepare_without_batch(&self.api_config, &self.host, &self.app, *level);
                let local_proxy = self.api_config.proxy.clone();
                let local_format = self.api_config.serialization_format;
                self.sender = Some(Box::pin(local_proxy.proxy(req).and_then(
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
        self.cache = detached_cache;

        ops::ControlFlow::Continue(())
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
