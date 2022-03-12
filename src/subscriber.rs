use super::*;
// use std::task;
use futures::channel::mpsc;

#[cfg(feature = "local-subscriber")]
pub mod local;
#[cfg(feature = "remote-subscriber")]
pub mod remote;

pub struct Subscriber {
    // send a new log message
    sender: mpsc::UnboundedSender<(log::Level, LogData)>,

    // request a flush, will recieve () back when done
    // inner sender is sync as this has to be handled from sync context
    flush_requester: mpsc::UnboundedSender<std::sync::mpsc::SyncSender<()>>,

    on_result: Box<dyn Fn(&'static str) + Sync + Send>,

    level: log::LevelFilter,
}
impl Subscriber {
    pub fn set_logger(self) -> Result<()> {
        let lf = self.level;
        log::set_boxed_logger(Box::new(self))?;
        log::set_max_level(lf);
        Ok(())
    }
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
        batch: &collections::BTreeMap<ulid::Ulid, LogData>,
    ) -> bool {
        batch.len() >= self.get_limit(level)
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

impl log::Log for Subscriber {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.level >= metadata.level()
    }
    fn log(&self, record: &log::Record) {
        let res = self.sender.unbounded_send((record.level(), record.into()));
        if res.is_err() {
            (self.on_result)("log_record_send_failure");
        }
    }
    fn flush(&self) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let res = self.flush_requester.unbounded_send(tx);
        if res.is_err() {
            (self.on_result)("flush_request_failure");
        }
        let res = rx.recv();
        if res.is_err() {
            (self.on_result)("flush_response_failure");
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        eprintln!("Dropping log subscriber - sleeing the thread to give the sender time to empty the cache");
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_cache_limit() {
        let mut gen = ulid::Generator::new();
        let log_data = LogData {
            message: "abc".to_string(),
            code_module: None,
            code_file: None,
            code_line: None,
            tags: collections::HashMap::new(),
        };

        let batch_1 = iter::repeat(log_data.clone())
            .take(1)
            .map(|ld| (gen.generate().unwrap(), ld))
            .collect::<collections::BTreeMap<_, _>>();
        let batch_5 = iter::repeat(log_data.clone())
            .take(5)
            .map(|ld| (gen.generate().unwrap(), ld))
            .collect::<collections::BTreeMap<_, _>>();
        let batch_9 = iter::repeat(log_data.clone())
            .take(9)
            .map(|ld| (gen.generate().unwrap(), ld))
            .collect::<collections::BTreeMap<_, _>>();
        let batch_10 = iter::repeat(log_data.clone())
            .take(10)
            .map(|ld| (gen.generate().unwrap(), ld))
            .collect::<collections::BTreeMap<_, _>>();
        let batch_99 = iter::repeat(log_data.clone())
            .take(99)
            .map(|ld| (gen.generate().unwrap(), ld))
            .collect::<collections::BTreeMap<_, _>>();
        let batch_100 = iter::repeat(log_data)
            .take(100)
            .map(|ld| (gen.generate().unwrap(), ld))
            .collect::<collections::BTreeMap<_, _>>();

        let limit = CacheLimit::default();

        assert!(limit.should_send(log::Level::Error, &batch_1));
        assert!(limit.should_send(log::Level::Error, &batch_5));
        assert!(limit.should_send(log::Level::Error, &batch_9));
        assert!(limit.should_send(log::Level::Error, &batch_10));
        assert!(limit.should_send(log::Level::Error, &batch_99));
        assert!(limit.should_send(log::Level::Error, &batch_100));

        assert!(limit.should_send(log::Level::Warn, &batch_1));
        assert!(limit.should_send(log::Level::Warn, &batch_5));
        assert!(limit.should_send(log::Level::Warn, &batch_9));
        assert!(limit.should_send(log::Level::Warn, &batch_10));
        assert!(limit.should_send(log::Level::Warn, &batch_99));
        assert!(limit.should_send(log::Level::Warn, &batch_100));

        assert!(!limit.should_send(log::Level::Info, &batch_1));
        assert!(!limit.should_send(log::Level::Info, &batch_5));
        assert!(!limit.should_send(log::Level::Info, &batch_9));
        assert!(limit.should_send(log::Level::Info, &batch_10));
        assert!(limit.should_send(log::Level::Info, &batch_99));
        assert!(limit.should_send(log::Level::Info, &batch_100));

        assert!(!limit.should_send(log::Level::Debug, &batch_1));
        assert!(!limit.should_send(log::Level::Debug, &batch_5));
        assert!(!limit.should_send(log::Level::Debug, &batch_9));
        assert!(!limit.should_send(log::Level::Debug, &batch_10));
        assert!(!limit.should_send(log::Level::Debug, &batch_99));
        assert!(limit.should_send(log::Level::Debug, &batch_100));

        assert!(!limit.should_send(log::Level::Trace, &batch_1));
        assert!(!limit.should_send(log::Level::Trace, &batch_5));
        assert!(!limit.should_send(log::Level::Trace, &batch_9));
        assert!(!limit.should_send(log::Level::Trace, &batch_10));
        assert!(!limit.should_send(log::Level::Trace, &batch_99));
        assert!(limit.should_send(log::Level::Trace, &batch_100));
    }
}
