use super::*;
use futures_util::{future, StreamExt};

impl Subscriber {
    pub fn new_local<S>(
        on_result: Box<dyn Fn(&'static str) + Sync + Send>,
        host: Host,
        app: App,
        level: log::LevelFilter,
        cache_limit: CacheLimit,
        cache_timeout: time::Duration,
        storage: S,
    ) -> (Subscriber, DataSaver<S>)
    where
        S: storage::Storage,
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
            DataSaver {
                receiver: rx1,
                flush_request: rx2,
                host,
                app,
                cache_limit,
                cache_timeout,
                cache: Default::default(),
                storage,
                generator: ulid::Generator::new(),
            },
        )
    }
}

pub struct DataSaver<S>
where
    S: storage::Storage,
{
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    host: Host,

    app: App,

    cache_limit: CacheLimit,

    cache_timeout: time::Duration,

    cache: collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,

    generator: ulid::Generator,

    storage: S,
}

impl<S> DataSaver<S>
where
    S: storage::Storage,
{
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
        loop {
            match future::select(self.receiver.next(), self.flush_request.next()).await {
                future::Either::Left((Some((level, data)), _)) => {
                    let mut batch = self.cache.remove(&level).unwrap_or_default();

                    batch.insert(self.generator.generate().unwrap(), data);

                    self.cache.insert(level, batch);

                    let mut detached_cache = std::mem::take(&mut self.cache);
                    for (level, batch) in detached_cache.iter_mut() {
                        let now = time::SystemTime::now();
                        if self
                            .cache_limit
                            .should_send(*level, batch, now, self.cache_timeout)
                        {
                            let batch = std::mem::take(batch);
                            self.storage
                                .submit(&self.host, &self.app, (*level).into(), batch)
                                .await?;
                        }
                    }
                    self.cache = detached_cache;
                }
                future::Either::Right((Some(sender), _)) => {
                    self.storage.flush(&self.host, &self.app).await?;
                    sender.send(())?;
                }
                future::Either::Left((None, _)) | future::Either::Right((None, _)) => {
                    return Err(Error::LogSubscriberClosed)
                }
            }
        }
    }
}
