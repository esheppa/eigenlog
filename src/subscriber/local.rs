use super::*;
use futures_util::{future, StreamExt};

impl Subscriber {
    pub fn new_local(
        on_result: Box<dyn Fn(&'static str) + Sync + Send>,
        host: Host,
        app: App,
        level: log::LevelFilter,
        db: sled::Db,
    ) -> (Subscriber, DataSaver) {
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
                db,
            },
        )
    }
}

pub struct DataSaver {
    receiver: mpsc::UnboundedReceiver<(log::Level, LogData)>,

    flush_request: mpsc::UnboundedReceiver<std::sync::mpsc::SyncSender<()>>,

    host: Host,

    app: App,

    db: sled::Db,
}

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
            match future::select(receiver.next(), flush_request.next()).await {
                future::Either::Left((Some((level, data)), _)) => {
                    let mut generator = ulid::Generator::new();
                    let mut batch = collections::BTreeMap::new();
                    batch.insert(generator.generate()?, data);
                    db::submit(host, app, level.into(), batch, db)?;
                }
                future::Either::Right((Some(sender), _)) => {
                    for level in Level::all() {
                        let tree = db.open_tree(level.get_tree_name(host, app))?;
                        tree.flush()?;
                    }
                    sender.send(())?;
                }
                future::Either::Left((None, _)) | future::Either::Right((None, _)) => {
                    return Err(Error::LogSubscriberClosed)
                }
            }
        }
    }
}
