use async_trait::async_trait;

use super::*;

#[async_trait]
pub trait Storage: Clone + Send + Sync {
    async fn submit(&self, host: &Host, app: &App, level: Level, log_batch: LogBatch)
        -> Result<()>;

    async fn query(&self, params: QueryParams) -> Result<Vec<QueryResponse>>;

    async fn detail(&self, host: &Host, app: &App, level: Level) -> Result<LogTreeDetail>;

    async fn info(&self) -> Result<Vec<result::Result<LogTreeInfo, ParseLogTreeInfoError>>>;

    async fn flush(&self, host: &Host, app: &App) -> Result<()>;
}

pub fn filter_with_option<T: AsRef<str>>(input: &T, filter: &Option<T>) -> bool {
    filter
        .as_ref()
        .map(|f| input.as_ref().contains(f.as_ref()))
        .unwrap_or(true)
}
