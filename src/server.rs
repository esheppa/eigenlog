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

pub enum AppReply<T: serde::Serialize> {
    Json(T),
    Bincode(T),
    Empty,
}

// impl<T: serde::Serialize> From<T> for AppReply {
//     fn from(t: T) -> AppReply {
//         AppReply::Json(warp::reply::json(&t))
//     }
// }

impl<T: serde::Serialize + Send> warp::Reply for AppReply<T> {
    fn into_response(self) -> warp::reply::Response {
        match self {
            #[cfg(feature = "json")]
            AppReply::Json(j) => warp::reply::json(&j).into_response(),
            AppReply::Bincode(i) => http::Response::new(hyper::Body::from(
                bincode::serialize(&i).expect("Bincode Serialize should succeed"),
            )),
            AppReply::Empty => http::Response::default(),
        }
    }
}

// make a new version for each return type?
// need a new into_reply as well
pub fn error_to_reply<T: serde::Serialize>(
    maybe_err: Result<AppReply<T>>,
) -> result::Result<AppReply<T>, convert::Infallible> {
    match maybe_err {
        Ok(r) => Ok(r.into()),
        Err(e) => Ok(e.to_reply()),
    }
}

impl Error {
    pub fn to_reply<'a, T: serde::Serialize>(self) -> AppReply<T> {
        todo!()
    }
}

async fn submit(
    host: Host,
    app: App,
    level: Level,
    api_key: String,
    content_type: SerializationFormat,
    bytes: body::Bytes,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> Result<AppReply<()>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    let batch: LogBatch = match content_type {
        SerializationFormat::Bincode => bincode::deserialize(&bytes)?,
        #[cfg(feature = "json")]
        SerializationFormat::Json => serde_json::from_slice(&bytes)?,
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

    Ok(AppReply::Empty)
}

fn filter_with_option<T: AsRef<str>>(input: &T, filter: &Option<T>) -> bool {
    filter
        .as_ref()
        .map(|f| input.as_ref().contains(f.as_ref()))
        .unwrap_or(true)
}

// later this function should access the db via a channel to bridge sync and async
// this will avoid blocking the runtime
// in the short term we will leave it like this
async fn query(
    api_key: String,
    accept: SerializationFormat,
    params: QueryParams,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
    // vec queryresponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<AppReply<Vec<QueryResponse>>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    let relevant_trees = db
        .tree_names()
        .iter()
        .filter_map(|t| TreeName::from_bytes(&t).ok())
        .filter(|t| filter_with_option(&t.host, &params.host_contains))
        .filter(|t| filter_with_option(&t.app, &params.app_contains))
        .filter(|t| t.level >= params.max_log_level.clone().unwrap_or(Level::Info))
        .collect::<Vec<_>>();

    let start = params
        .start_timestamp
        .map(ulid::Ulid::from_datetime)
        .map(ulid_floor)
        .unwrap_or(u128::MIN)
        .to_be_bytes();

    let end = params
        .start_timestamp
        .map(ulid::Ulid::from_datetime)
        .map(ulid_ceiling)
        .unwrap_or(u128::MAX)
        .to_be_bytes();

    let mut response = Vec::new();
    for tree_name in relevant_trees {
        let tree = db.open_tree(tree_name.to_string())?;
        for item in tree.range(start..=end) {
            let (key, value) = item?;
            response.push(QueryResponse {
                host: tree_name.host.clone(),
                app: tree_name.app.clone(),
                level: tree_name.level.clone(),
                id: ivec_be_to_u128(key)?.into(),
                data: bincode::deserialize(&value)?,
            })
        }
    }

    match accept {
        SerializationFormat::Bincode => Ok(AppReply::Bincode(response)),
        #[cfg(feature = "json")]
        SerializationFormat::Json => Ok(AppReply::Json(response)),
    }
}

async fn info(
    api_key: String,
    accept: SerializationFormat,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
    // vec queryresponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<AppReply<Vec<LogTreeInfo>>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    let mut db_info = Vec::new();

    for name in db.tree_names() {
        if let Some(info) = tree_name_to_info(&db, name)? {
            db_info.push(info);
        }
    }

    match accept {
        SerializationFormat::Bincode => Ok(AppReply::Bincode(db_info)),
        #[cfg(feature = "json")]
        SerializationFormat::Json => Ok(AppReply::Json(db_info)),
    }
}

fn ulid_floor(input: ulid::Ulid) -> u128 {
    let mut base = u128::from(input).to_be_bytes();

    for i in 6..16 {
        base[i] = u8::MIN;
    }

    u128::from_be_bytes(base)
}

fn ulid_ceiling(input: ulid::Ulid) -> u128 {
    let mut base = u128::from(input).to_be_bytes();

    for i in 6..16 {
        base[i] = u8::MAX;
    }

    u128::from_be_bytes(base)
}

fn ivec_be_to_u128(vec: sled::IVec) -> crate::Result<u128> {
    let mut bytes = [0; 16];

    if vec.len() != 16 {
        return Err(crate::Error::InvalidLengthBytesForUlid(vec.len()));
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
