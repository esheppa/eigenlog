use super::*;
use futures::FutureExt;
use std::{collections, convert, result, sync};
use warp::{
    http::{self, header},
    hyper::{self, body},
    Filter,
};

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

async fn submit(
    host: Host,
    app: App,
    level: Level,
    api_key: String,
    content_type: String,
    bytes: body::Bytes,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> Result<()> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    let batch: LogBatch = match content_type.as_str() {
        OCTET_STREAM => bincode::deserialize(&bytes)?,
        #[cfg(feature = "json")]
        APPLICATION_JSON => serde_json::from_slice(&bytes)?,
        _ => {
            return Err(Error::InvalidSubmissionContentType(content_type));
        }
    };

    // this will create the tree if it doesn't already exist
    let tree = db.open_tree(level.get_tree_name(&host, &app))?;

    // insert all items from the batch into the tree.
    // while we could use `apply_batch` here, we don't have any need
    // for all the rows to be atomically applied, and it should be faster
    // to add them one by one.
    for (key, item) in batch {
        // use to_be_bytes to ensure that the ulid is sorted as expected
        tree.insert(u128::from(key).to_be_bytes(), bincode::serialize(&item)?)?;
    }

    Ok(())
}

async fn query(
    api_key: String,
    accept: String,
    params: QueryParams,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
    // vec queryresponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<Vec<QueryResponse>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    // let hosts = db
    //     .tree_names()
    //     .into_iter()
    //     .filter_map(|n| n.split(b"-").next())

    todo!()
}

// empty trees will be ignored
// so we can be sure we will always have a min and max date
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct LogTreeInfo {
    host: Host,
    app: App,
    level: Level,
    min: chrono::DateTime<chrono::Utc>,
    max: chrono::DateTime<chrono::Utc>,
}

async fn info(
    api_key: String,
    accept: String,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
    // vec queryresponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<Vec<LogTreeInfo>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    // let hosts = db
    //     .tree_names()
    //     .into_iter()
    //     .filter_map(|n| n.split(b"-").next())

    todo!()
}

fn ivec_be_to_u128(vec: sled::IVec) -> crate::Result<u128> {
    let mut bytes = [0; 16];
    if vec.len() != 16 {
        return Err(todo!());
    }

    for (i, b) in vec.into_iter().enumerate() {
        bytes[i] = *b;
    }

    Ok(u128::from_be_bytes(bytes))
}

fn tree_name_to_info(db: &sled::Db, name: sled::IVec) -> crate::Result<Option<LogTreeInfo>> {
    let parsed = TreeName::from_bytes(&name)?;
    let tree = db.open_tree(&name)?;

    if tree.is_empty() {
        return Ok(None);
    }

    let first = if let Some((k, _)) = tree.first()? {
        ulid::Ulid::from(ivec_be_to_u128(k)?).datetime()
    } else {
        return Ok(None);
    };

    let last = if let Some((k, _)) = tree.last()? {
        ulid::Ulid::from(ivec_be_to_u128(k)?).datetime()
    } else {
        return Ok(None);
    };

    Ok(Some(LogTreeInfo {
        host: parsed.host,
        app: parsed.app,
        level: parsed.level,
        min: first,
        max: last,
    }))
}

// These endpoints are kept seperate as sometimes only one may be needed
// for example if using local-subscriber, people may want query to add to their own app
// if providing a submission endpoint, the app may not necessarily need to provider query as well.
pub fn create_submission_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> {
    warp::path("submit")
        .and(warp::post())
        .and(warp::path::param()) // Host
        .and(warp::path::param()) // App
        .and(warp::path::param()) // Level
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::CONTENT_TYPE.as_str()))
        .and(warp::body::bytes()) // LogBatch payload
        .and(add(db.clone()))
        .and(add(api_keys.clone()))
        .and_then(|host, app, level, key, content_type, batch, db, keys| {
            submit(host, app, level, key, content_type, batch, db, keys).map(error_to_reply)
        })
}

pub fn create_query_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> {
    warp::path("query")
        .and(warp::get())
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::ACCEPT.as_str()))
        .and(warp::query())
        .and(add(db.clone()))
        .and(add(api_keys.clone()))
        .and_then(|key, accept, params, db, keys| {
            query(key, accept, params, db, keys).map(error_to_reply)
        })
}

pub fn create_info_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> {
    warp::path("info")
        .and(warp::get())
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::ACCEPT.as_str()))
        .and(add(db.clone()))
        .and(add(api_keys.clone()))
        .and_then(|key, accept, db, keys| info(key, accept, db, keys).map(error_to_reply))
}
