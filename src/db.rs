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

    Ok(response)
}

pub fn info(db: &sled::Db) -> Result<Vec<LogTreeInfo>> {
    let mut db_info = Vec::new();

    for name in db.tree_names() {
        if let Some(info) = tree_name_to_info(&db, name)? {
            db_info.push(info);
        }
    }

    Ok(db_info)
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
