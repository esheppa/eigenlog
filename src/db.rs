use super::*;
use bincode_crate as bincode;

pub fn submit(
    host: &Host,
    app: &App,
    level: Level,
    log_batch: LogBatch,
    db: &sled::Db,
) -> Result<()> {
    // this will create the tree if it doesn't already exist
    let tree = db.open_tree(level.get_tree_name(host, app))?;

    // insert all items from the batch into the tree.
    // while we could use `apply_batch` here, we don't have any need
    // for all the rows to be atomically applied, and it should be faster
    // to add them one by one.
    for (key, item) in log_batch {
        // use to_be_bytes to ensure that the ulid is sorted as expected
        tree.insert(u128::from(key).to_be_bytes(), bincode::serialize(&item)?)?;
    }

    Ok(())
}

fn filter_with_option<T: AsRef<str>>(input: &T, filter: &Option<T>) -> bool {
    filter
        .as_ref()
        .map(|f| input.as_ref().contains(f.as_ref()))
        .unwrap_or(true)
}

pub fn query(params: QueryParams, db: &sled::Db) -> Result<Vec<QueryResponse>> {
    let relevant_trees = db
        .tree_names()
        .iter()
        .filter_map(|t| TreeName::from_bytes(t).ok())
        .filter(|t| filter_with_option(&t.host, &params.host_contains))
        .filter(|t| filter_with_option(&t.app, &params.app_contains))
        .filter(|t| t.level <= params.max_log_level.clone().unwrap_or(Level::Info))
        .collect::<Vec<_>>();

    let start = params
        .start_timestamp
        .map(ulid::Ulid::from_datetime)
        .map(ulid_floor)
        .unwrap_or(u128::MIN)
        .to_be_bytes();

    let end = params
        .end_timestamp
        .map(ulid::Ulid::from_datetime)
        .map(ulid_ceiling)
        .unwrap_or(u128::MAX)
        .to_be_bytes();

    let mut response = Vec::new();

    let must_match = params
        .message_matches
        .map(|s| regex::Regex::new(&s).map_err(crate::Error::from))
        .transpose()?;
    let must_not_match = params
        .message_not_matches
        .map(|s| regex::Regex::new(&s).map_err(crate::Error::from))
        .transpose()?;

    let mut rows = 0;
    for tree_name in relevant_trees {
        let tree = db.open_tree(tree_name.to_string())?;
        for item in tree.range(start..=end) {
            match params.max_results {
                Some(max_results) if max_results < rows => {
                    break;
                }
                _ => (),
            }
            let (key, value) = item?;
            let data = bincode::deserialize::<LogData>(&value)?;

            // exit early if we want to filter out
            let any_not_matches = must_not_match
                .as_ref()
                .map(|n| n.is_match(&data.message))
                .unwrap_or(false); // if empty this should have no effect

            // exit early if we want to filter it out
            if any_not_matches {
                continue;
            }

            let any_matches = must_match
                .as_ref()
                .map(|m| m.is_match(&data.message))
                .unwrap_or(true); // if empty this should have no effect

            if !any_matches {
                continue;
            }

            response.push(QueryResponse {
                host: tree_name.host.clone(),
                app: tree_name.app.clone(),
                level: tree_name.level.clone(),
                id: ivec_be_to_u128(key)?.into(),
                data: bincode::deserialize(&value)?,
            });
            rows += 1;
        }
    }

    Ok(response)
}

pub fn detail(host: &Host, app: &App, level: Level, db: &sled::Db) -> Result<LogTreeDetail> {
    let tree = db.open_tree(level.get_tree_name(host, app))?;

    let mut row_detail = collections::BTreeMap::new();

    for row in tree.iter() {
        let (key, _) = row?;
        let ulid_key = ulid::Ulid::from(ivec_be_to_u128(key)?);
        row_detail
            .entry(ulid_key.datetime().naive_local().date())
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    Ok(LogTreeDetail {
        app: app.clone(),
        host: host.clone(),
        level,
        rows: row_detail.values().sum(),
        row_detail,
    })
}



pub fn info(db: &sled::Db) -> Result<Vec<result::Result<LogTreeInfo, ParseLogTreeInfoError>>> {
    let mut db_info = Vec::new();

    for name in db
        .tree_names()
        .into_iter()
        .filter(|n| n != b"__sled__default")
    {
        match tree_name_to_info(db, name.clone()) {
            Ok(Some(info)) => {
                db_info.push(Ok(info));
            }
            // consider what to do here - prehaps an enum within LogTreeInfo.
            Ok(None) => {
                let msg = format!("Tree {} is empty", String::from_utf8_lossy(&name));
                db_info.push(Err(ParseLogTreeInfoError(msg)));
            }
            Err(e) => {
                let msg = format!(
                    "Skipping invalid tree name {}, due to: {}",
                    String::from_utf8_lossy(&name),
                    e
                );
                db_info.push(Err(ParseLogTreeInfoError(msg)));
                continue;
            }
        }
    }

    Ok(db_info)
}

fn ulid_floor(input: ulid::Ulid) -> u128 {
    let mut base = u128::from(input).to_be_bytes();

    for i in base.iter_mut().skip(6) {
        *i = u8::MIN;
    }

    u128::from_be_bytes(base)
}

fn ulid_ceiling(input: ulid::Ulid) -> u128 {
    let mut base = u128::from(input).to_be_bytes();

    for i in base.iter_mut().skip(6) {
        *i = u8::MAX;
    }

    u128::from_be_bytes(base)
}

fn ivec_be_to_u128(vec: sled::IVec) -> crate::Result<u128> {
    let mut bytes = [0; 16];

    if vec.len() != 16 {
        return Err(crate::Error::InvalidLengthBytesForUlid(vec.len()));
    }

    for (i, b) in vec.iter().enumerate() {
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
