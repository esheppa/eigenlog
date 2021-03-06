// this should be run simultaneously with the `remote_subscriber` or `client` examples

use eigenlog::server;
use std::{collections, sync};
use warp::Filter;

const BASE_URL: &str = "log";
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = sled::open("db.sled")?;
    env_logger::init();

    let api_keys = sync::Arc::new(
        IntoIterator::into_iter(["123".to_string()]).collect::<collections::BTreeSet<String>>(),
    );

    let info = server::create_info_endpoint(db.clone(), api_keys.clone());
    let submit = server::create_submission_endpoint(db.clone(), api_keys.clone());
    let query = server::create_query_endpoint(db.clone(), api_keys.clone());
    let detail = server::create_detail_endpoint(db.clone(), api_keys.clone());
    warp::serve(
        warp::path(BASE_URL)
            .and(info.or(query).or(submit).or(detail))
            .with(warp::log("server")),
    )
    .bind(([127u8, 0, 0, 1], 8080u16))
    .await;
    Ok(())
}
