use super::*;

impl Subscriber {
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
