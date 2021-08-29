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
    Error(String),
}

impl<T: serde::Serialize + Send> warp::Reply for AppReply<T> {
    fn into_response(self) -> warp::reply::Response {
        match self {
            #[cfg(feature = "json")]
            AppReply::Json(j) => warp::reply::json(&j).into_response(),
            AppReply::Bincode(i) => http::Response::new(hyper::Body::from(
                bincode::serialize(&i).expect("Bincode Serialize should succeed"),
            )),
            AppReply::Empty => http::Response::default(),
            AppReply::Error(e) => e.into_response(),
        }
    }
}

// make a new version for each return type?
// need a new into_reply as well
pub fn error_to_reply<T: serde::Serialize>(
    maybe_err: Result<AppReply<T>>,
) -> result::Result<AppReply<T>, convert::Infallible> {
    match maybe_err {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_reply()),
    }
}

impl Error {
    pub fn into_reply<T: serde::Serialize>(self) -> AppReply<T> {
        AppReply::Error(self.to_string())
    }
}

// this is ok as it is an internal function
#[allow(clippy::too_many_arguments)]
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

    db::submit(&host, &app, level, batch, &db)?;

    Ok(AppReply::Empty)
}

async fn query(
    api_key: String,
    accept: SerializationFormat,
    params: QueryParams,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
    // vec QueryResponse isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<AppReply<Vec<QueryResponse>>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    // later this function should access the db via a channel to bridge sync and async
    // this will avoid blocking the runtime
    // in the short term we will leave it like this
    let response = db::query(params, &db)?;

    match accept {
        SerializationFormat::Bincode => Ok(AppReply::Bincode(response)),
        #[cfg(feature = "json")]
        SerializationFormat::Json => Ok(AppReply::Json(response)),
    }
}

async fn detail(
    host: Host,
    app: App,
    level: Level,
    api_key: String,
    accept: SerializationFormat,
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> Result<AppReply<LogTreeDetail>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    let response = db::detail(&host, &app, level, &db)?;

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
    // vec LogTreeInfo isn't that nice, but
    // it is the best option when using JSON serialization.
    // for Bincode or RON there could be another endpoint.
) -> Result<AppReply<Vec<result::Result<LogTreeInfo, db::ParseLogTreeInfoError>>>> {
    // ensure the request's API key is allowed
    if !api_keys.contains(&api_key) {
        return Err(Error::InvalidApiKey(api_key));
    }

    let db_info = db::info(&db)?;

    match accept {
        SerializationFormat::Bincode => Ok(AppReply::Bincode(db_info)),
        #[cfg(feature = "json")]
        SerializationFormat::Json => Ok(AppReply::Json(db_info)),
    }
}

// These endpoints are kept seperate as sometimes only one may be needed
// for example if using local-subscriber, people may want query to add to their own app
// if providing a submission endpoint, the app may not necessarily need to provider query as well.
pub fn create_submission_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<Extract = (AppReply<()>,), Error = warp::Rejection> + Clone {
    warp::path("submit")
        .and(warp::post())
        .and(warp::path::param()) // Host
        .and(warp::path::param()) // App
        .and(warp::path::param()) // Level
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::CONTENT_TYPE.as_str()))
        .and(warp::body::bytes()) // LogBatch payload
        .and(add(db))
        .and(add(api_keys))
        .and_then(|host, app, level, key, content_type, batch, db, keys| {
            submit(host, app, level, key, content_type, batch, db, keys).map(error_to_reply)
        })
}

pub fn create_query_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<Extract = (AppReply<Vec<QueryResponse>>,), Error = warp::Rejection> + Clone {
    warp::path("query")
        .and(warp::get())
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::ACCEPT.as_str()))
        .and(warp::query())
        .and(add(db))
        .and(add(api_keys))
        .and_then(|key, accept, params, db, keys| {
            query(key, accept, params, db, keys).map(error_to_reply)
        })
}

pub fn create_detail_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<Extract = (AppReply<LogTreeDetail>,), Error = warp::Rejection> + Clone {
    warp::path("detail")
        .and(warp::get())
        .and(warp::path::param()) // Host
        .and(warp::path::param()) // App
        .and(warp::path::param()) // Level
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::ACCEPT.as_str()))
        .and(add(db))
        .and(add(api_keys))
        .and_then(|host, app, level, key, accept, db, keys| {
            detail(host, app, level, key, accept, db, keys).map(error_to_reply)
        })
}

pub fn create_info_endpoint(
    db: sled::Db,
    api_keys: sync::Arc<collections::BTreeSet<String>>,
) -> impl warp::Filter<
    Extract = (AppReply<Vec<result::Result<LogTreeInfo, db::ParseLogTreeInfoError>>>,),
    Error = warp::Rejection,
> + Clone {
    warp::path("info")
        .and(warp::get())
        .and(warp::path::end())
        .and(warp::header(API_KEY_HEADER))
        .and(warp::header(header::ACCEPT.as_str()))
        .and(add(db))
        .and(add(api_keys))
        .and_then(|key, accept, db, keys| info(key, accept, db, keys).map(error_to_reply))
}
