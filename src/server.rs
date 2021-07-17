use super::*;
use futures::FutureExt;
use std::{collections, convert, result, sync};
use warp::{http::{self, header}, hyper::{self, body}, Filter};

use bincode_crate as bincode;

fn add<C: Clone + Send>(
    c: C,
) -> impl warp::Filter<Extract = (C,), Error = convert::Infallible> + Clone {
    warp::any().map(move || c.clone())
}

pub enum AppReply {
    Json(warp::reply::Json),
    Bincode(http::Response<hyper::Body>),
    Empty,
}

impl<T: serde::Serialize> From<T> for AppReply {
    fn from(t: T) -> AppReply {
        AppReply::Json(warp::reply::json(&t))
    }
}

impl warp::Reply for AppReply {
    fn into_response(self) -> warp::reply::Response {
        match self {
            AppReply::Json(j) => j.into_response(),
            AppReply::Bincode(i) => i.into_response(),
            AppReply::Empty => warp::http::Response::default(),
        }
    }
}

// make a new version for each return type?
// need a new into_reply as well
pub fn error_to_reply<R: Into<AppReply>>(
    maybe_err: Result<R>,
) -> result::Result<AppReply, convert::Infallible> {
    match maybe_err {
        Ok(r) => Ok(r.into()),
        Err(e) => Ok(e.to_reply()),
    }
}

impl Error {
    pub fn to_reply<'a>(self) -> AppReply {
        todo!()
    }
}

async fn submit(host: Host, app: App, level: Level, content_type: String, bytes: body::Bytes, db: sled::Db) -> Result<()> {
    let batch: LogBatch = match content_type.as_str() {
        OCTET_STREAM => {
            bincode::deserialize(&bytes)?
        }
        #[cfg(feature = "json")]
        APPLICATION_JSON => {
            serde_json::from_slice(&bytes)?
        }
        _ => {
            return Err(Error::InvalidSubmissionContentType(content_type));
        }
    };

    // this will create the tree if it doesn't already exist
    let tree = db.open_tree(level.get_tree_name(&host))?;

    // insert all items from the batch into the tree.
    // while we could use `apply_batch` here, we don't have any need
    // for all the rows to be atomically applied, and it should be faster
    // to add them one by one.
    for (key, item) in batch {
        tree.insert(u128::from(key).to_be_bytes(), bincode::serialize(&item)?)?;
    }

    Ok(())
}

async fn query(
    key: String,
    accept: String,
    params: QueryParams,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
    // vec queryresponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<Vec<QueryResponse>> {
    // ensure the requests API key is allowed
    if !api_keys.contains(&key) {
        return Err(Error::InvalidApiKey(key));
    }

    // let hosts = db
    //     .tree_names()
    //     .into_iter()
    //     .filter_map(|n| n.split(b"-").next())

    todo!()
}

pub fn create_server(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> warp::Server<impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection>> {
    // endpoint is /logbatch/<host>/<level>
    //
    // new tree for each host/level combination
    //
    // host must be a-zAZ09 and -_
    //
    //
    //
    let submit = warp::path("submit")
        .and(warp::post())
        .and(warp::path::param()) // Host
        .and(warp::path::param()) // App
        .and(warp::path::param()) // Level
        .and(warp::path::end())
        .and(warp::header(header::CONTENT_TYPE.as_str()))
        .and(warp::body::bytes()) // LogBatch payload
        .and(add(db.clone()))
        .and_then(|host, app, level, content_type, batch, db| submit(host, app, level, content_type, batch, db).map(error_to_reply));

    let query = warp::path("query")
        .and(warp::get())
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::ACCEPT.as_str()))
        .and(warp::query())
        .and(add(db.clone()))
        .and(add(api_keys.clone()))
        .and_then(|key, accept, params, db, keys| query(key, accept, params, db, keys).map(error_to_reply));

    warp::serve(submit.or(query))
}
