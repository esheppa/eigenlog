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
    // innser sender is sync as this has to be handled from sync context
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
        cache: &collections::HashMap<log::Level, collections::BTreeMap<ulid::Ulid, LogData>>,
    ) -> bool {
        let current_len = cache.get(&level).map(|d| d.len()).unwrap_or(0);
        self.get_limit(level) <= current_len + 1
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

// impl Subscriber {
//     fn on_result<T, E>(&self, result: result::Result<T, E>) {
//         let func = &self.on_result;
//         func(result.map_err(|_| ()).map(|_| ()))
//     }
// }

impl log::Log for Subscriber {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.level >= metadata.level()
    }
    fn log(&self, record: &log::Record) {
        let res = self.sender.send((record.level(), record.into()));
        if let Err(_) = res {
            (self.on_result)("log_record_send_failure");
        }    
    }
    fn flush(&self) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let res = self.flush_requester.send(tx);
        if let Err(_) = res {
            (self.on_result)("flush_request_failure");
        }
        let res = rx.recv();
        if let Err(_) = res {
            (self.on_result)("flush_response_failure");
        }
    }
}
