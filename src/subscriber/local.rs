use super::*;
use futures_util::{future, StreamExt};

impl Subscriber {
    pub fn new_local<S>(
        on_result: Box<dyn Fn(&'static str) + Sync + Send>,
        host: Host,
        app: App,
        level: log::LevelFilter,
        storage: S,
    ) -> (Subscriber, DataSaver<S>)
    where
        S: db::Storage,
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
                storage,
            },
        )
    }
}

pub struct DataSaver<S>
where
    S: db::Storage,
{
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    host: Host,

    app: App,

    storage: S,
}

impl<S> DataSaver<S>
where
    S: db::Storage,
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
        let DataSaver {
            receiver,
            flush_request,
            ref storage,
            ref host,
            app,
        } = self;

        loop {
            match future::select(receiver.next(), flush_request.next()).await {
                future::Either::Left((Some((level, data)), _)) => {
                    let mut generator = ulid::Generator::new();
                    let mut batch = collections::BTreeMap::new();
                    batch.insert(generator.generate()?, data);
                    storage.submit(host.clone(), app.clone(), level.into(), batch).await?;
                }
                future::Either::Right((Some(sender), _)) => {
                    storage.flush(host.clone(), app.clone()).await?;
                    sender.send(())?;
                }
                future::Either::Left((None, _)) | future::Either::Right((None, _)) => {
                    return Err(Error::LogSubscriberClosed)
                }
            }
        }
    }
}
