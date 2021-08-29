use super::*;
use tokio::sync::mpsc;

#[cfg(feature = "local-subscriber")]
mod local;
#[cfg(feature = "remote-subscriber")]
mod remote;

pub struct Subscriber {
    // send a new log message
    sender: mpsc::UnboundedSender<(log::Level, LogData)>,

    // request a flush, will recieve () back when done
    // inner sender is sync as this has to be handled from sync context
    flush_requester: mpsc::UnboundedSender<std::sync::mpsc::SyncSender<()>>,

    on_result: Box<dyn Fn(&'static str) + Sync + Send>,

    level: log::Level,
}
impl Subscriber {
    pub fn set_logger(self, max_level: log::LevelFilter) -> Result<()> {
        log::set_boxed_logger(Box::new(self))?;
        log::set_max_level(max_level);
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
        batch.len() < self.get_limit(level)
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
        let res = self.sender.send((record.level(), record.into()));
        if res.is_err() {
            (self.on_result)("log_record_send_failure");
        }
    }
    fn flush(&self) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let res = self.flush_requester.send(tx);
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