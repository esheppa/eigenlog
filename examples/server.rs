// this should be run simultaneously with the `remote_subscriber` example
// and this demonstrates the use of a server with remote subscriber

use warp::Filter;
use eigenlog::server;
use sled;
use std::{sync, collections};

const BASE_URL: &str = "log";
#[tokio::main]
async fn main() -> anyhow::Result<()> {    
    let  db = sled::open("db.sled")?; 

    let api_keys = sync::Arc::new(IntoIterator::into_iter(["123".to_string()]).collect::<collections::BTreeSet<String>>());

    let info = server::create_info_endpoint(db.clone(), api_keys.clone());
    let submit = server::create_submission_endpoint(db.clone(), api_keys.clone());
    let query = server::create_query_endpoint(db.clone(), api_keys.clone());
	warp::serve(
        warp::path(BASE_URL)
        .and(
            info
            .or(query)
            .or(submit)
        )
    )
    .bind(([127u8, 0, 0, 1], 8080u16))
    .await;
	Ok(())
}
